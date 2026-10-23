//! Unit tests of the manifest builder, registry, route and catalog.

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_NONE_MATCH};
use axum::http::{HeaderMap, Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use super::*;
use crate::controllers::crud_query::FieldSpec;
use crate::error::CrudError;

fn widget(name: &str, version: &str) -> ResourceDescriptor {
    let mut d = ResourceDescriptor::new(
        name,
        "Widgets",
        version,
        format!("/api/{name}/{version}"),
        json!({"type": "object"}),
        json!({"type": "object"}),
        json!({"type": "object"}),
    );
    d.access = Access {
        public_read: true,
        ..Access::default()
    };
    d
}

fn anyone(_: &HeaderMap) -> Caller {
    Caller::Anonymous
}

async fn get(
    router: &Router,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut request = Request::builder().uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    (
        parts.status,
        parts.headers,
        to_bytes(body, usize::MAX).await.unwrap().to_vec(),
    )
}

fn manifest_of(resource: &ResourceDescriptor, config: &ManifestConfig) -> Value {
    serde_json::to_value(build_manifest([resource], config)).unwrap()
}

#[test]
fn fields_derive_from_the_parsers_own_table() {
    let specs = [
        FieldSpec::number("id"),
        FieldSpec::float("price"),
        FieldSpec::datetime("created_at"),
        FieldSpec::string("name"),
        FieldSpec::boolean("active"),
    ];
    let mut d = widget("widgets", "v1");
    d.fields = specs.iter().map(ManifestField::from_spec).collect();
    d.fields[0] = ManifestField::from_spec(&specs[0]).read_only();
    d.fields[3] = ManifestField::from_spec(&specs[3])
        .required()
        .not_sortable();
    let m = manifest_of(&d, &ManifestConfig::new("svc", "1"));
    let fields = &m["resources"][0]["fields"];
    let summary: Vec<(&str, &str, bool, bool)> = fields
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                f["name"].as_str().unwrap(),
                f["value_type"].as_str().unwrap(),
                f["numeric"].as_bool().unwrap(),
                f["categorical"].as_bool().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            ("id", "integer", true, false),
            ("price", "number", true, false),
            ("created_at", "datetime", false, false),
            ("name", "string", false, false),
            ("active", "boolean", false, true),
        ]
    );
    assert_eq!(fields[0]["filter_ops"], json!(["eq", "min", "max", "in"]));
    assert_eq!(
        fields[3]["filter_ops"],
        json!(["eq", "contains", "icontains"])
    );
    assert_eq!(fields[4]["filter_ops"], json!(["eq", "in"]));
    assert_eq!(
        (
            fields[0]["read_only"].clone(),
            fields[3]["required"].clone(),
            fields[3]["sortable"].clone()
        ),
        (json!(true), json!(true), json!(false))
    );
}

#[test]
fn enum_and_date_fields_get_their_operators_and_choices() {
    let status = ManifestField::new("status", ValueType::String).with_choices(&["open", "done"]);
    assert_eq!(status.value_type, ValueType::Enum);
    assert_eq!(status.filter_ops, ["eq", "in"]);
    assert_eq!(
        status.choices,
        Some(vec!["open".to_owned(), "done".to_owned()])
    );
    assert_eq!(default_ops(ValueType::Date), ["eq", "min", "max", "in"]);
    let mut d = widget("widgets", "v1");
    d.fields = vec![status];
    let field = &manifest_of(&d, &ManifestConfig::new("s", "1"))["resources"][0]["fields"][0];
    assert_eq!(
        (field["categorical"].clone(), field["choices"].clone()),
        (json!(true), json!(["open", "done"]))
    );
}

#[test]
fn paths_and_cache_follow_the_features() {
    let mut config = ManifestConfig::new("svc", "2");
    config.stats_ttl = Some(30);
    config.bulk_max_matched = 50;
    config.oidc_discovery_url = Some("https://idp/.well-known".into());
    let mut d = widget("widgets", "v1");
    let plain = manifest_of(&d, &config);
    let r = &plain["resources"][0];
    assert_eq!(
        r["paths"],
        json!({"filters": "/api/widgets/v1/filters", "clone": "/api/widgets/v1/clone"})
    );
    assert_eq!(
        r["cache"],
        json!({"etag": false, "max_age": 0, "stats_ttl": null, "stale_while_revalidate": 0, "list_etag": null, "record_etag": null, "revalidate_only": false})
    );
    assert_eq!(r["bulk"], json!({"max_matched": 50}));
    assert_eq!(r["auth"]["oidc_discovery_url"], "https://idp/.well-known");
    assert_eq!(r["capabilities"]["events"], Value::Null);

    d.features.predict = true;
    d.features.conditional_requests = true;
    d.features.revisions = true;
    d.features.draft_publish = true;
    d.features.archive_restore = true;
    d.events = Some(EventsDescriptor::new("widget", Replay::None));
    d.access = Access {
        read_roles: vec!["r".into()],
        write_roles: vec!["w".into()],
        ..Access::default()
    };
    let full = manifest_of(&d, &config);
    let r = &full["resources"][0];
    let keys: Vec<&str> = r["paths"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        [
            "clone",
            "draft",
            "events",
            "filters",
            "predict",
            "publish",
            "restore",
            "revisions"
        ]
    );
    assert_eq!(r["cache"]["stats_ttl"], 30);
    assert_eq!(r["cache"]["list_etag"], "weak");
    assert_eq!(r["auth"]["read_roles"], json!(["r"]));
    assert_eq!(r["capabilities"]["events"]["replay"], "none");
    assert_eq!(
        r["capabilities"]["events"]["path"],
        "/api/widgets/v1/events"
    );
    assert_eq!(
        r["capabilities"]["events"]["actions"],
        json!(["create", "update", "delete"])
    );
}

#[test]
fn resources_are_ordered_by_name_then_version() {
    let (b2, b1, a1) = (widget("b", "v2"), widget("b", "v1"), widget("a", "v1"));
    let m = serde_json::to_value(build_manifest(
        [&b2, &b1, &a1],
        &ManifestConfig::new("s", "1"),
    ))
    .unwrap();
    let order: Vec<String> = m["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            format!(
                "{}{}",
                r["name"].as_str().unwrap(),
                r["api_version"].as_str().unwrap()
            )
        })
        .collect();
    assert_eq!(order, ["av1", "bv1", "bv2"]);
}

#[test]
fn registry_rejects_duplicates_and_filters_by_read_access() {
    let registry = ManifestRegistry::new();
    registry.register(widget("pub", "v1")).unwrap();
    let mut private = widget("priv", "v1");
    private.access = Access {
        read_roles: vec!["reader".into()],
        ..Access::default()
    };
    registry.register(private).unwrap();
    let mut closed = widget("closed", "v1");
    closed.access = Access::default();
    registry.register(closed).unwrap();
    let err = registry.register(widget("pub", "v1")).unwrap_err();
    assert_eq!(err.to_string(), "resource \"pub\" v1 is already registered");
    registry.register(widget("pub", "v2")).unwrap();

    let names = |caller: &Caller| -> Vec<String> {
        registry
            .visible_to(caller)
            .iter()
            .map(|r| format!("{}:{}", r.name, r.api_version))
            .collect()
    };
    assert_eq!(names(&Caller::Anonymous), ["pub:v1", "pub:v2"]);
    let stranger = Caller::Authenticated {
        roles: vec!["other".into()],
    };
    assert_eq!(names(&stranger), ["pub:v1", "pub:v2"]);
    let reader = Caller::Authenticated {
        roles: vec!["reader".into()],
    };
    assert_eq!(names(&reader), ["pub:v1", "priv:v1", "pub:v2"]);
}

fn router_for(registry: &ManifestRegistry, prefix: &str) -> Router {
    let mut config = ManifestConfig::new("svc", "1");
    config.prefix = prefix.to_owned();
    manifest_router(registry.clone(), config, anyone)
}

#[tokio::test]
async fn route_honours_prefix_and_sees_later_registrations() {
    let registry = ManifestRegistry::new();
    let router = router_for(&registry, "/x/");
    let (status, headers, body) = get(&router, "/x/manifest", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[CONTENT_TYPE], "application/json");
    assert_eq!(headers[CACHE_CONTROL], "private, no-cache");
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["resources"],
        json!([])
    );
    let first_etag = headers[ETAG].clone();

    registry.register(widget("widgets", "v1")).unwrap();
    let (_, headers, body) = get(&router, "/x/manifest", &[]).await;
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["resources"][0]["name"],
        "widgets"
    );
    assert_ne!(headers[ETAG], first_etag);
    assert_eq!(
        get(&router, "/api/_crud/manifest", &[]).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(headers[axum::http::header::VARY], "authorization, cookie");
    for (prefix, path) in [("", "/manifest"), ("api", "/api/manifest")] {
        let router = router_for(&registry, prefix);
        assert_eq!(get(&router, path, &[]).await.0, StatusCode::OK, "{prefix}");
    }
}

#[tokio::test]
async fn if_none_match_uses_weak_comparison() {
    let registry = ManifestRegistry::new();
    let router = router_for(&registry, DEFAULT_PREFIX);
    let uri = "/api/_crud/manifest";
    let (_, headers, _) = get(&router, uri, &[]).await;
    let weak = headers[ETAG].to_str().unwrap().to_owned();
    let strong = weak.trim_start_matches("W/").to_owned();
    let other = "W/\"nope\"";
    for matching in [
        weak.clone(),
        strong,
        "*".to_owned(),
        format!("{other}, {weak}"),
    ] {
        let (status, h, body) = get(&router, uri, &[(IF_NONE_MATCH.as_str(), &matching)]).await;
        assert_eq!(
            (status, body.is_empty()),
            (StatusCode::NOT_MODIFIED, true),
            "{matching}"
        );
        assert_eq!(h[ETAG].to_str().unwrap(), weak);
        assert_eq!(h[CACHE_CONTROL], "private, no-cache");
    }
    let (status, _, _) = get(&router, uri, &[(IF_NONE_MATCH.as_str(), other)]).await;
    assert_eq!(status, StatusCode::OK);
}

struct Down;

#[async_trait]
impl CallerResolver for Down {
    async fn resolve(&self, _: &HeaderMap) -> Result<Caller, CrudError> {
        Err(CrudError::ServiceUnavailable("idp down".into()))
    }
}

#[tokio::test]
async fn a_resolver_failure_is_its_problem_response_not_a_hidden_resource() {
    let registry = ManifestRegistry::new();
    registry.register(widget("widgets", "v1")).unwrap();
    let router = manifest_router(registry, ManifestConfig::new("svc", "1"), Down);
    let (status, headers, _) = get(&router, "/api/_crud/manifest", &[]).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers[CONTENT_TYPE], "application/problem+json");
}

async fn catalog(router: &Router, headers: &[(&str, &str)]) -> Value {
    let (status, h, body) = get(router, "/.well-known/api-catalog", headers).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h[CONTENT_TYPE], "application/linkset+json");
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn api_catalog_links_the_manifest_and_optionally_the_openapi_document() {
    let router = api_catalog_router(
        "/api/_crud/manifest",
        Some("/openapi.json".into()),
        Some("https://h/api".into()),
    );
    assert_eq!(
        catalog(&router, &[]).await,
        json!({"linkset": [{
            "anchor": "https://h/api",
            "service-desc": [{"href": "/openapi.json", "type": "application/vnd.oai.openapi+json"}],
            "describedby": [{"href": "/api/_crud/manifest", "type": "application/json", "profile": MANIFEST_PROFILE}],
        }]})
    );
    let bare = api_catalog_router("/m", None, Some("https://h".into()));
    let entry = &catalog(&bare, &[]).await["linkset"][0];
    assert!(entry.get("service-desc").is_none());
    assert_eq!(entry["describedby"][0]["href"], "/m");
}

#[tokio::test]
async fn api_catalog_anchor_defaults_to_the_request_base() {
    let router = api_catalog_router("/m", None, None);
    let anchor = |headers: &[(&str, &str)]| {
        let router = router.clone();
        let headers: Vec<(String, String)> = headers
            .iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect();
        async move {
            let pairs: Vec<(&str, &str)> = headers
                .iter()
                .map(|(a, b)| (a.as_str(), b.as_str()))
                .collect();
            catalog(&router, &pairs).await["linkset"][0]["anchor"]
                .as_str()
                .unwrap()
                .to_owned()
        }
    };
    assert_eq!(
        anchor(&[("host", "api.example.com")]).await,
        "http://api.example.com"
    );
    assert_eq!(
        anchor(&[
            ("host", "api.example.com:8443"),
            ("x-forwarded-proto", "https")
        ])
        .await,
        "https://api.example.com:8443"
    );
    assert_eq!(
        anchor(&[("host", "h"), ("x-forwarded-proto", "HTTPS, http")]).await,
        "https://h"
    );
    assert_eq!(anchor(&[("host", "a.b/evil\"")]).await, "http://localhost");
    assert_eq!(anchor(&[]).await, "http://localhost");
}
