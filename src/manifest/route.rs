//! `GET <prefix>/manifest`: the manifest of the resources the caller may
//! read, with a weak `ETag` and `Cache-Control: private, no-cache`.
//!
//! The crate knows no authentication scheme, so the host supplies a
//! [`CallerResolver`] that turns the request headers into a [`Caller`]. A
//! resource is listed when the caller may read it ([`Access::may_read`]):
//! anonymous callers see only public resources, and a signed-in caller whose
//! roles match nothing is treated the same way. The body's `ETag` is a digest
//! of the caller-filtered document, so two callers who see different
//! resources get different tags, and a matching `If-None-Match` gets a
//! bodiless `304`.
//!
//! [`Access::may_read`]: crate::manifest::Access::may_read

use std::fmt::Write as _;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_NONE_MATCH, VARY};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use sha2::{Digest, Sha256};

use crate::error::CrudError;
use crate::manifest::build::build_manifest;
use crate::manifest::descriptor::{Caller, ManifestConfig, ManifestRegistry};

/// `Cache-Control` of the manifest: stored by the client only, revalidated
/// before every reuse.
pub const REVALIDATE_CACHE_CONTROL: &str = "private, no-cache";

/// Decides who is calling.
///
/// Return [`Caller::Anonymous`] for a request without valid credentials
/// (the manifest then lists public resources only) and an error for a
/// failure that is not the caller's, such as an identity provider outage:
/// it is rendered as its problem response rather than hiding resources.
#[async_trait]
pub trait CallerResolver: Send + Sync + 'static {
    /// Resolve the caller of a request with these headers.
    async fn resolve(&self, headers: &HeaderMap) -> Result<Caller, CrudError>;
}

#[async_trait]
impl<F> CallerResolver for F
where
    F: Fn(&HeaderMap) -> Caller + Send + Sync + 'static,
{
    async fn resolve(&self, headers: &HeaderMap) -> Result<Caller, CrudError> {
        Ok(self(headers))
    }
}

struct RouteState {
    registry: ManifestRegistry,
    config: ManifestConfig,
    resolver: Box<dyn CallerResolver>,
}

/// A short stable hex digest of `source`, for an entity-tag's opaque value
/// (SHA-256, first 32 hex digits, as `lib-crud-py` does).
fn digest(source: &[u8]) -> String {
    let hash = Sha256::digest(source);
    let mut hex = String::with_capacity(32);
    for byte in &hash[..16] {
        write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

/// Whether an `If-None-Match` header value matches `etag`: weak comparison
/// (RFC 9110 8.8.3.2), so the `W/` prefix is ignored on both sides, and `*`
/// matches anything.
fn if_none_match(headers: &HeaderMap, etag: &str) -> bool {
    let opaque = etag.trim_start_matches("W/");
    headers
        .get_all(IF_NONE_MATCH)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .any(|tag| tag == "*" || tag.trim_start_matches("W/") == opaque)
}

async fn get_manifest(
    State(state): State<Arc<RouteState>>,
    headers: HeaderMap,
) -> Result<Response, CrudError> {
    let caller = state.resolver.resolve(&headers).await?;
    let visible = state.registry.visible_to(&caller);
    let manifest = build_manifest(visible.iter().map(|resource| &**resource), &state.config);
    let body = serde_json::to_vec(&manifest).expect("a manifest always serializes");
    let etag = format!("W/\"{}\"", digest(&body));
    let validators = [
        (ETAG, etag.as_str()),
        (CACHE_CONTROL, REVALIDATE_CACHE_CONTROL),
        // The document depends on who is calling, whatever the resolver reads.
        (VARY, AUTHORIZATION.as_str()),
    ];
    if if_none_match(&headers, &etag) {
        return Ok((StatusCode::NOT_MODIFIED, validators).into_response());
    }
    Ok((validators, [(CONTENT_TYPE, "application/json")], body).into_response())
}

/// A router serving `GET <prefix>/manifest` (`config.prefix`, by default
/// `/api/_crud`; slashes at either end are optional) for the resources in `registry`, filtered to what
/// `resolver` says the caller may read. The registry is read on every
/// request, so a resource registered after this call still appears.
pub fn manifest_router<R: CallerResolver>(
    registry: ManifestRegistry,
    config: ManifestConfig,
    resolver: R,
) -> Router {
    let prefix = config.prefix.trim_matches('/');
    let path = if prefix.is_empty() {
        "/manifest".to_owned()
    } else {
        format!("/{prefix}/manifest")
    };
    let state = Arc::new(RouteState {
        registry,
        config,
        resolver: Box::new(resolver),
    });
    Router::new()
        .route(&path, get(get_manifest))
        .with_state(state)
}
