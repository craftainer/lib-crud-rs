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

use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::extract::State;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, VARY};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use crate::conditional::{digest, if_none_match};
use crate::error::CrudError;
use crate::manifest::build::build_manifest;
use crate::manifest::descriptor::{Caller, ManifestConfig, ManifestRegistry};

pub use crate::conditional::REVALIDATE_CACHE_CONTROL;

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
        // The document depends on who is calling. A resolver that reads other
        // headers should be fronted by a cache that keys on them too.
        (VARY, "authorization, cookie"),
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
