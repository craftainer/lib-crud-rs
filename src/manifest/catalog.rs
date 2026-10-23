//! `GET /.well-known/api-catalog` (RFC 9727, an RFC 9264 linkset): points a
//! client at the `OpenAPI` document and the manifest. Mount it on the app
//! root. It needs no authentication.

use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::header::{CONTENT_TYPE, HOST};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Serialize;
use std::sync::Arc;

use crate::manifest::model::MANIFEST_PROFILE;

/// Where the catalog is served.
pub const API_CATALOG_PATH: &str = "/.well-known/api-catalog";
const LINKSET_MEDIA_TYPE: &str = "application/linkset+json";
const OPENAPI_MEDIA_TYPE: &str = "application/vnd.oai.openapi+json";

#[derive(Serialize)]
struct Link<'a> {
    href: &'a str,
    #[serde(rename = "type")]
    media_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<&'a str>,
}

#[derive(Serialize)]
struct Entry<'a> {
    anchor: &'a str,
    #[serde(rename = "service-desc", skip_serializing_if = "Vec::is_empty")]
    service_desc: Vec<Link<'a>>,
    describedby: Vec<Link<'a>>,
}

#[derive(Serialize)]
struct Linkset<'a> {
    linkset: [Entry<'a>; 1],
}

struct Catalog {
    manifest_url: String,
    openapi_url: Option<String>,
    anchor: Option<String>,
}

/// The base URL the request arrived on: `https` when a proxy says so in
/// `X-Forwarded-Proto`, else `http`, and the `Host` header (`localhost`
/// when it is missing or not a plain host). Behind a proxy that does not
/// forward them, pass an explicit `anchor` instead.
fn request_base(headers: &HeaderMap) -> String {
    let https = headers
        .get("x-forwarded-proto")
        .is_some_and(|proto| proto.as_bytes() == b"https");
    let host = headers
        .get(HOST)
        .and_then(|host| host.to_str().ok())
        .filter(|host| {
            !host.is_empty()
                && host
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':' | '[' | ']'))
        })
        .unwrap_or("localhost");
    format!("{}://{host}", if https { "https" } else { "http" })
}

async fn get_catalog(State(catalog): State<Arc<Catalog>>, headers: HeaderMap) -> Response {
    let anchor = catalog
        .anchor
        .clone()
        .unwrap_or_else(|| request_base(&headers));
    let service_desc = catalog
        .openapi_url
        .iter()
        .map(|href| Link {
            href,
            media_type: OPENAPI_MEDIA_TYPE,
            profile: None,
        })
        .collect();
    let linkset = Linkset {
        linkset: [Entry {
            anchor: &anchor,
            service_desc,
            describedby: vec![Link {
                href: &catalog.manifest_url,
                media_type: "application/json",
                profile: Some(MANIFEST_PROFILE),
            }],
        }],
    };
    let body = serde_json::to_vec(&linkset).expect("a linkset always serializes");
    ([(CONTENT_TYPE, LINKSET_MEDIA_TYPE)], body).into_response()
}

/// A router serving `GET /.well-known/api-catalog`; merge it into the app
/// root.
///
/// `manifest_url` is the manifest's URL as a client reaches it (for example
/// `/api/_crud/manifest`). `openapi_url` is the `OpenAPI` document's URL,
/// advertised as the optional `service-desc` link; `None` omits the link, so
/// an app that serves no `OpenAPI` document (a production mode, say) leaves no
/// dangling one. Axum has no app-wide `OpenAPI` setting to follow, so the host
/// passes what it serves. `anchor` is the API's base URL; by default it is
/// the base URL the request arrived on.
pub fn api_catalog_router(
    manifest_url: impl Into<String>,
    openapi_url: Option<String>,
    anchor: Option<String>,
) -> Router {
    let catalog = Arc::new(Catalog {
        manifest_url: manifest_url.into(),
        openapi_url,
        anchor,
    });
    Router::new()
        .route(API_CATALOG_PATH, get(get_catalog))
        .with_state(catalog)
}
