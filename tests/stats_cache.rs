//! The stats/predict response cache through the public API only, the way a
//! host's handlers use it. Needs no backing service.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use craftainer_crud::async_trait::async_trait;
use craftainer_crud::error::CrudError;
use craftainer_crud::stats_cache::{
    CacheError, CacheScope, CacheState, CachedStats, MemoryStatsCache, StatsCache, cache_key,
};
use serde_json::{Value, json};

/// A stats handler's shape: derive the key, then build the body on a miss.
async fn widget_stats(
    cached: &CachedStats,
    scope: &CacheScope,
    bucket: &str,
    reads: &AtomicUsize,
) -> Result<(Value, CacheState), CrudError> {
    let params = BTreeMap::from([("bucket".to_owned(), json!(bucket))]);
    let key = cache_key("stats", "WidgetStats", scope, &params);
    cached
        .get_or_build(&key, || async {
            let n = reads.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(json!({ "reads": n }))
        })
        .await
}

#[tokio::test]
async fn a_handler_hits_misses_and_invalidates_per_scope_and_params() {
    let memory = Arc::new(MemoryStatsCache::with_ttl(Duration::from_secs(30)));
    let cached = CachedStats::new(memory, "widgets");
    let reads = AtomicUsize::new(0);
    let alice = CacheScope::Owner {
        field: "owner_id".into(),
        value: "alice".into(),
    };
    let bob = CacheScope::Owner {
        field: "owner_id".into(),
        value: "bob".into(),
    };

    let (body, state) = widget_stats(&cached, &alice, "day", &reads).await.unwrap();
    assert_eq!((body, state), (json!({"reads": 1}), CacheState::Miss));
    let (body, state) = widget_stats(&cached, &alice, "day", &reads).await.unwrap();
    assert_eq!((body, state), (json!({"reads": 1}), CacheState::Hit));

    // Another owner, and another query, each get their own entry.
    let (_, state) = widget_stats(&cached, &bob, "day", &reads).await.unwrap();
    assert_eq!(state, CacheState::Miss);
    let (_, state) = widget_stats(&cached, &alice, "week", &reads).await.unwrap();
    assert_eq!(state, CacheState::Miss);
    assert_eq!(reads.load(Ordering::SeqCst), 3);

    // A write invalidates the resource's entries.
    cached.invalidate().await.unwrap();
    let (_, state) = widget_stats(&cached, &alice, "day", &reads).await.unwrap();
    assert_eq!(state, CacheState::Miss);
}

#[tokio::test]
async fn without_a_cache_every_call_builds() {
    let cached = CachedStats::uncached("widgets");
    assert_eq!(cached.resource(), "widgets");
    let reads = AtomicUsize::new(0);
    for expected in 1..=2 {
        let (body, state) = widget_stats(&cached, &CacheScope::Shared, "day", &reads)
            .await
            .unwrap();
        assert_eq!(
            (body, state),
            (json!({"reads": expected}), CacheState::Bypass)
        );
    }
    cached.invalidate().await.unwrap();
    assert_eq!(CacheState::Bypass.as_str(), "bypass");
    assert!(format!("{cached:?}").contains("widgets"));
}

#[tokio::test]
async fn entries_expire_after_the_ttl() {
    let memory = MemoryStatsCache::with_ttl(Duration::from_millis(20));
    memory.set("widgets", "k", json!(1)).await.unwrap();
    assert_eq!(memory.get("widgets", "k").await.unwrap(), Some(json!(1)));
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(memory.get("widgets", "k").await.unwrap(), None);
    assert_eq!(
        MemoryStatsCache::default()
            .get("widgets", "k")
            .await
            .unwrap(),
        None
    );
}

/// A cache that is always down.
struct Down;

#[async_trait]
impl StatsCache for Down {
    async fn get(&self, _: &str, _: &str) -> Result<Option<Value>, CacheError> {
        Err(CacheError::new("connection refused"))
    }
    async fn set(&self, _: &str, _: &str, _: Value) -> Result<(), CacheError> {
        Err(CacheError::new("connection refused"))
    }
    async fn invalidate(&self, _: &str) -> Result<(), CacheError> {
        Err(CacheError::new("connection refused"))
    }
}

#[tokio::test]
async fn a_failing_cache_reaches_the_handler_without_running_the_query() {
    let cached = CachedStats::new(Arc::new(Down), "widgets");
    let reads = AtomicUsize::new(0);
    let error = widget_stats(&cached, &CacheScope::Shared, "day", &reads)
        .await
        .unwrap_err();
    assert!(matches!(&error, CrudError::Internal(m) if m.contains("connection refused")));
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert!(cached.invalidate().await.is_err());
    assert_eq!(CacheState::Hit.as_str(), "hit");
    assert_eq!(CacheState::Miss.as_str(), "miss");
}
