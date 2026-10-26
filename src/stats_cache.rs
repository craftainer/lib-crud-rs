//! An opt-in response cache for the `/stats` and `/predict` routes, at parity
//! with `lib-crud-py`'s `StatsCache`.
//!
//! The crate has no CRUD routers, so this is a building block a host's
//! handlers call, in the spirit of [`controllers::crud_stats`]: the host
//! implements [`StatsCache`] over its own store (Redis, for example) or uses
//! [`MemoryStatsCache`], wraps it in a [`CachedStats`] per resource and runs
//! each handler's query through [`CachedStats::get_or_build`]. Nothing is
//! cached unless the host does so; a [`CachedStats::uncached`] passes every
//! call straight to the builder.
//!
//! Only non-per-user aggregates should be cached. [`cache_key`] covers that
//! by taking the caller's [`CacheScope`]: a resource whose reads are scoped
//! to an owner never shares an entry between owners, and the owner value
//! appears only as a hash.
//!
//! A cache failure is a [`CacheError`] that propagates to the handler (as in
//! `lib-crud-py`), never a silent miss. [`CacheError`] converts into
//! [`CrudError::Internal`], so a handler that returns [`CrudError`] can use
//! `?` directly.
//!
//! ```
//! use std::collections::BTreeMap;
//! use std::sync::Arc;
//! use craftainer_crud::error::CrudError;
//! use craftainer_crud::stats_cache::{
//!     CacheScope, CacheState, CachedStats, MemoryStatsCache, cache_key,
//! };
//! use serde_json::json;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), CrudError> {
//! let cache = CachedStats::new(Arc::new(MemoryStatsCache::new()), "widgets");
//! let key = cache_key("stats", "WidgetStats", &CacheScope::Shared, &BTreeMap::new());
//!
//! let (_, state) = cache
//!     .get_or_build(&key, || async { Ok::<_, CrudError>(json!({"count": 3})) })
//!     .await?;
//! assert_eq!(state, CacheState::Miss);
//! let (body, state) = cache
//!     .get_or_build(&key, || async { Ok::<_, CrudError>(json!({"count": 4})) })
//!     .await?;
//! assert_eq!((body, state), (json!({"count": 3}), CacheState::Hit));
//! # Ok(())
//! # }
//! ```
//!
//! [`controllers::crud_stats`]: crate::controllers::crud_stats

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::error::CrudError;

/// A cache backend failed (a connection dropped, a value could not be
/// stored, ...). It propagates to the handler instead of being treated as a
/// miss; see the [module docs](self).
#[derive(Debug, thiserror::Error)]
#[error("stats cache error: {0}")]
pub struct CacheError(String);

impl CacheError {
    /// A cache error carrying a description of what failed.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl From<CacheError> for CrudError {
    fn from(error: CacheError) -> Self {
        Self::Internal(error.to_string())
    }
}

/// A shared cache of `/stats` and `/predict` response bodies, kept per
/// resource.
///
/// The key is opaque (see [`cache_key`]). An implementation owns everything
/// else: the TTL, any namespacing (an app version, say, so a response whose
/// shape changed between releases is never served across them) and how
/// [`invalidate`](Self::invalidate) finds a resource's entries.
#[async_trait]
pub trait StatsCache: Send + Sync {
    /// The body stored under `resource`/`key`, or `None` on a miss or an
    /// expired entry.
    async fn get(&self, resource: &str, key: &str) -> Result<Option<Value>, CacheError>;

    /// Store `value` under `resource`/`key`, overwriting any previous entry.
    async fn set(&self, resource: &str, key: &str, value: Value) -> Result<(), CacheError>;

    /// Drop every entry stored for `resource` (call it after any of the
    /// resource's writes).
    async fn invalidate(&self, resource: &str) -> Result<(), CacheError>;
}

/// The default [`MemoryStatsCache`] time to live.
pub const DEFAULT_TTL: Duration = Duration::from_secs(60);

/// A process-local [`StatsCache`] whose entries expire after a time to live.
///
/// There is no size bound: expired entries are dropped on any `set`, when
/// read, and on `invalidate`.
///
/// Meant for a single instance, tests and development; replicas behind a load
/// balancer need a shared store, which the host supplies by implementing
/// [`StatsCache`].
#[derive(Debug)]
pub struct MemoryStatsCache {
    ttl: Duration,
    entries: Mutex<HashMap<(String, String), (Instant, Value)>>,
}

impl MemoryStatsCache {
    /// An empty cache with the [`DEFAULT_TTL`] of 60 seconds.
    #[must_use]
    pub fn new() -> Self {
        Self::with_ttl(DEFAULT_TTL)
    }

    /// An empty cache whose entries expire `ttl` after they are stored.
    #[must_use]
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), (Instant, Value)>> {
        // A panic while holding the lock cannot leave the map half-updated.
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for MemoryStatsCache {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StatsCache for MemoryStatsCache {
    async fn get(&self, resource: &str, key: &str) -> Result<Option<Value>, CacheError> {
        let mut entries = self.entries();
        let slot = (resource.to_owned(), key.to_owned());
        match entries.get(&slot) {
            Some((stored_at, value)) if stored_at.elapsed() < self.ttl => Ok(Some(value.clone())),
            Some(_) => {
                entries.remove(&slot);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn set(&self, resource: &str, key: &str, value: Value) -> Result<(), CacheError> {
        let mut entries = self.entries();
        // Drop every expired entry first, so keys that are never read again
        // (distinct query params) cannot pile up.
        entries.retain(|_, (stored_at, _)| stored_at.elapsed() < self.ttl);
        entries.insert(
            (resource.to_owned(), key.to_owned()),
            (Instant::now(), value),
        );
        Ok(())
    }

    async fn invalidate(&self, resource: &str) -> Result<(), CacheError> {
        self.entries().retain(|(stored, _), _| stored != resource);
        Ok(())
    }
}

/// Whose rows a cached aggregate summarises.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheScope {
    /// Every caller sees the same rows, so they share one entry.
    Shared,
    /// Reads are scoped to one owner: each `value` of `field` gets its own
    /// entry.
    Owner {
        /// The owner column.
        field: String,
        /// This caller's owner value. It is hashed into the key, never
        /// stored in clear text.
        value: String,
    },
}

/// Lower-case hex SHA-256 of `bytes`.
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

/// The opaque [`StatsCache`] key for one `/stats` or `/predict` request.
///
/// A SHA-256 (hex) over the sorted JSON of the route `kind` (`"stats"` or
/// `"predict"`), the response `schema_id` (so a deprecated API version with a
/// different shape never shares an entry with the current one), the caller's
/// `scope` and the already-normalised query `params`. An owner value is
/// hashed inside the material, so it never appears in a key's source in clear
/// text. The app version is the cache implementation's concern.
///
/// Equal inputs give equal keys regardless of the order `params` were built
/// in; the key is behaviour-equivalent to `lib-crud-py`'s, not byte-identical.
#[must_use]
pub fn cache_key(
    kind: &str,
    schema_id: &str,
    scope: &CacheScope,
    params: &BTreeMap<String, Value>,
) -> String {
    let scope = match scope {
        CacheScope::Shared => json!("shared"),
        CacheScope::Owner { field, value } => {
            json!({ "owner": { "field": field, "value_sha256": sha256_hex(value.as_bytes()) } })
        }
    };
    // `serde_json` maps are ordered (no `preserve_order`), so this is sorted JSON.
    let material = json!({ "kind": kind, "schema": schema_id, "scope": scope, "params": params });
    sha256_hex(material.to_string().as_bytes())
}

/// What happened to a [`CachedStats::get_or_build`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheState {
    /// The body came from the cache.
    Hit,
    /// The body was built and stored.
    Miss,
    /// No cache is configured; the body was built and not stored.
    Bypass,
}

impl CacheState {
    /// The lower-case name (`hit`, `miss`, `bypass`), for a response header
    /// such as `X-Stats-Cache`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Bypass => "bypass",
        }
    }
}

/// One resource's view of a [`StatsCache`] (or of none).
#[derive(Clone)]
pub struct CachedStats {
    cache: Option<Arc<dyn StatsCache>>,
    resource: String,
}

impl std::fmt::Debug for CachedStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedStats")
            .field("resource", &self.resource)
            .field("cached", &self.cache.is_some())
            .finish()
    }
}

impl CachedStats {
    /// Cache `resource`'s bodies in `cache`.
    #[must_use]
    pub fn new(cache: Arc<dyn StatsCache>, resource: impl Into<String>) -> Self {
        Self {
            cache: Some(cache),
            resource: resource.into(),
        }
    }

    /// No cache: [`get_or_build`](Self::get_or_build) always builds and
    /// reports [`CacheState::Bypass`].
    #[must_use]
    pub fn uncached(resource: impl Into<String>) -> Self {
        Self {
            cache: None,
            resource: resource.into(),
        }
    }

    /// The resource name entries are kept under.
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }

    /// Return the body cached under `key` ([`CacheState::Hit`]), or run
    /// `build` and store its body ([`CacheState::Miss`]). Without a cache,
    /// run `build` only ([`CacheState::Bypass`]).
    ///
    /// # Errors
    ///
    /// `build`'s own error, or a [`CacheError`] (converted into `E`) when the
    /// cache fails; a failing cache is not treated as a miss.
    pub async fn get_or_build<E, F, Fut>(
        &self,
        key: &str,
        build: F,
    ) -> Result<(Value, CacheState), E>
    where
        E: From<CacheError>,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Value, E>>,
    {
        let Some(cache) = &self.cache else {
            return Ok((build().await?, CacheState::Bypass));
        };
        if let Some(body) = cache.get(&self.resource, key).await? {
            return Ok((body, CacheState::Hit));
        }
        let body = build().await?;
        cache.set(&self.resource, key, body.clone()).await?;
        Ok((body, CacheState::Miss))
    }

    /// Drop every cached body of this resource. Call it after any of the
    /// resource's writes. A no-op without a cache.
    ///
    /// # Errors
    ///
    /// A [`CacheError`] when the cache fails.
    pub async fn invalidate(&self) -> Result<(), CacheError> {
        match &self.cache {
            Some(cache) => cache.invalidate(&self.resource).await,
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn params(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    fn owner(value: &str) -> CacheScope {
        CacheScope::Owner {
            field: "owner_id".into(),
            value: value.into(),
        }
    }

    #[test]
    fn sha256_hex_is_lowercase_hex() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn key_ignores_param_insertion_order() {
        let a = params(&[("bucket", json!("day")), ("field", json!("price"))]);
        let b = params(&[("field", json!("price")), ("bucket", json!("day"))]);
        assert_eq!(
            cache_key("stats", "S", &CacheScope::Shared, &a),
            cache_key("stats", "S", &CacheScope::Shared, &b)
        );
    }

    #[test]
    fn key_changes_with_every_component() {
        let p = params(&[("bucket", json!("day"))]);
        let base = cache_key("stats", "S", &CacheScope::Shared, &p);
        assert_eq!(base.len(), 64);
        assert_ne!(base, cache_key("predict", "S", &CacheScope::Shared, &p));
        assert_ne!(base, cache_key("stats", "T", &CacheScope::Shared, &p));
        assert_ne!(base, cache_key("stats", "S", &owner("a"), &p));
        assert_ne!(
            cache_key("stats", "S", &owner("a"), &p),
            cache_key("stats", "S", &owner("b"), &p)
        );
        assert_ne!(
            base,
            cache_key(
                "stats",
                "S",
                &CacheScope::Shared,
                &params(&[("bucket", json!("week"))])
            )
        );
    }

    #[test]
    fn key_is_the_hash_of_sorted_json_with_the_owner_value_hashed() {
        let p = params(&[("z", json!(1)), ("a", json!(2))]);
        let material = format!(
            concat!(
                r#"{{"kind":"stats","params":{{"a":2,"z":1}},"schema":"S","#,
                r#""scope":{{"owner":{{"field":"owner_id","value_sha256":"{}"}}}}}}"#
            ),
            sha256_hex(b"alice")
        );
        assert_eq!(
            cache_key("stats", "S", &owner("alice"), &p),
            sha256_hex(material.as_bytes())
        );
        assert!(
            !material.contains("\"alice\""),
            "the owner value is not in clear text"
        );
    }

    #[test]
    fn cache_state_names() {
        assert_eq!(CacheState::Hit.as_str(), "hit");
        assert_eq!(CacheState::Miss.as_str(), "miss");
        assert_eq!(CacheState::Bypass.as_str(), "bypass");
    }

    #[test]
    fn cache_error_becomes_an_internal_crud_error() {
        let error: CrudError = CacheError::new("down").into();
        assert!(matches!(&error, CrudError::Internal(m) if m == "stats cache error: down"));
    }

    #[tokio::test]
    async fn memory_cache_stores_and_isolates_resources() {
        let cache = MemoryStatsCache::default();
        assert_eq!(cache.get("widgets", "k").await.unwrap(), None);
        cache.set("widgets", "k", json!(1)).await.unwrap();
        cache.set("gadgets", "k", json!(2)).await.unwrap();
        assert_eq!(cache.get("widgets", "k").await.unwrap(), Some(json!(1)));
        cache.invalidate("widgets").await.unwrap();
        assert_eq!(cache.get("widgets", "k").await.unwrap(), None);
        assert_eq!(cache.get("gadgets", "k").await.unwrap(), Some(json!(2)));
    }

    #[tokio::test]
    async fn memory_cache_entries_expire_after_the_ttl() {
        let cache = MemoryStatsCache::with_ttl(Duration::from_millis(20));
        cache.set("widgets", "k", json!(1)).await.unwrap();
        assert_eq!(cache.get("widgets", "k").await.unwrap(), Some(json!(1)));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(cache.get("widgets", "k").await.unwrap(), None);
        assert!(cache.entries().is_empty(), "the expired entry is dropped");
    }

    #[tokio::test]
    async fn set_drops_expired_entries_that_are_never_read_again() {
        let cache = MemoryStatsCache::with_ttl(Duration::from_millis(20));
        cache.set("widgets", "k1", json!(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        cache.set("widgets", "k2", json!(2)).await.unwrap();
        let entries = cache.entries();
        assert_eq!(entries.len(), 1);
        assert!(entries.contains_key(&("widgets".to_owned(), "k2".to_owned())));
    }

    #[test]
    fn memory_cache_default_ttl_is_sixty_seconds() {
        assert_eq!(MemoryStatsCache::new().ttl, Duration::from_secs(60));
    }

    #[test]
    fn memory_cache_survives_a_poisoned_lock() {
        let cache = Arc::new(MemoryStatsCache::new());
        let poisoner = Arc::clone(&cache);
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.entries.lock().unwrap();
            panic!("poison the lock");
        })
        .join();
        assert!(cache.entries().is_empty());
    }

    fn build_counted(
        calls: &AtomicUsize,
        body: Value,
    ) -> std::future::Ready<Result<Value, CrudError>> {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(body))
    }

    #[tokio::test]
    async fn get_or_build_misses_then_hits_then_rebuilds_after_invalidate() {
        let stats = CachedStats::new(Arc::new(MemoryStatsCache::new()), "widgets");
        assert_eq!(stats.resource(), "widgets");
        let calls = AtomicUsize::new(0);

        // One closure for every call: it runs on a miss and never on a hit.
        let build = |n: u32| {
            let calls = &calls;
            move || build_counted(calls, json!({ "n": n }))
        };

        let first = stats.get_or_build("k", build(1)).await;
        assert_eq!(first.unwrap(), (json!({"n": 1}), CacheState::Miss));
        let second = stats.get_or_build("k", build(2)).await;
        assert_eq!(second.unwrap(), (json!({"n": 1}), CacheState::Hit));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        stats.invalidate().await.unwrap();
        let third = stats.get_or_build("k", build(3)).await;
        assert_eq!(third.unwrap(), (json!({"n": 3}), CacheState::Miss));
    }

    #[tokio::test]
    async fn get_or_build_bypasses_without_a_cache() {
        let stats = CachedStats::uncached("widgets");
        let calls = AtomicUsize::new(0);
        for _ in 0..2 {
            let out = stats
                .get_or_build("k", || build_counted(&calls, json!(7)))
                .await;
            assert_eq!(out.unwrap(), (json!(7), CacheState::Bypass));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        stats.invalidate().await.unwrap();
        assert!(format!("{stats:?}").contains("cached: false"));
    }

    #[tokio::test]
    async fn a_build_error_is_returned_and_not_cached() {
        let cache = Arc::new(MemoryStatsCache::new());
        let stats = CachedStats::new(cache.clone(), "widgets");
        let out = stats
            .get_or_build("k", || async {
                Err::<Value, _>(CrudError::NotFound("x".into()))
            })
            .await;
        assert!(matches!(out, Err(CrudError::NotFound(_))));
        assert_eq!(cache.get("widgets", "k").await.unwrap(), None);
    }

    /// A cache whose operations fail, selected by `fail_on`.
    struct Broken {
        fail_on: &'static str,
    }

    #[async_trait]
    impl StatsCache for Broken {
        async fn get(&self, _: &str, _: &str) -> Result<Option<Value>, CacheError> {
            if self.fail_on == "get" {
                Err(CacheError::new("get"))
            } else {
                Ok(None)
            }
        }
        async fn set(&self, _: &str, _: &str, _: Value) -> Result<(), CacheError> {
            if self.fail_on == "set" {
                Err(CacheError::new("set"))
            } else {
                Ok(())
            }
        }
        async fn invalidate(&self, _: &str) -> Result<(), CacheError> {
            Err(CacheError::new("invalidate"))
        }
    }

    #[tokio::test]
    async fn cache_errors_propagate_instead_of_counting_as_misses() {
        for op in ["get", "set"] {
            let stats = CachedStats::new(Arc::new(Broken { fail_on: op }), "widgets");
            let calls = AtomicUsize::new(0);
            let out = stats
                .get_or_build("k", || build_counted(&calls, json!(1)))
                .await;
            assert!(
                matches!(&out, Err(CrudError::Internal(m)) if m.contains(op)),
                "{op}: {out:?}"
            );
            // A failing `get` never reaches the builder; a failing `set` follows it.
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(op == "set"));
        }
        let stats = CachedStats::new(Arc::new(Broken { fail_on: "" }), "widgets");
        assert!(stats.invalidate().await.is_err());
    }
}
