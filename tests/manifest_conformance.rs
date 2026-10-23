//! The shared CRUD manifest v1 conformance fixtures (vendored from
//! `lib-crud-py`, see `tests/fixtures/README.md`) run through the Rust
//! builder and the manifest route: each case's caller must get exactly the
//! fixture's manifest, and every manifest must validate against the I1 JSON
//! Schema.
//!
//! The fixtures describe the models (schemas, fields, capabilities) in
//! `admin.manifest.json` and the roles gating them in `server.json`; the
//! descriptors below are built from those, so what is under test is the
//! builder's own derivation (paths, auth, cache, pagination, ordering) and
//! the caller filtering. Rust does not have `regex` filters (ADR 0004), so
//! the fields take the fixture's operators explicitly.

use std::path::PathBuf;

use axum::body::{Body, to_bytes};
use axum::http::header::{CACHE_CONTROL, ETAG, IF_NONE_MATCH};
use axum::http::{HeaderMap, Request, StatusCode};
use craftainer_crud::manifest::{
    Access, Caller, EventsDescriptor, Features, ManifestConfig, ManifestEventAction, ManifestField,
    ManifestRegistry, Replay, ResourceDescriptor, ValueType, build_manifest, manifest_router,
};
use serde_json::Value;
use tower::ServiceExt;

const CASES: &[&str] = &[
    "admin",
    "anonymous",
    "stranger",
    "hero-reader",
    "legacy-reader",
];

fn fixture(relative: &str) -> Value {
    let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "fixtures", relative]
        .iter()
        .collect();
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_str(&text).unwrap()
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect()
}

fn value_type(name: &str) -> ValueType {
    match name {
        "integer" => ValueType::Integer,
        "number" => ValueType::Number,
        "date" => ValueType::Date,
        "datetime" => ValueType::Datetime,
        "string" => ValueType::String,
        "boolean" => ValueType::Boolean,
        "enum" => ValueType::Enum,
        other => panic!("unknown value_type {other}"),
    }
}

fn action(name: &str) -> ManifestEventAction {
    match name {
        "create" => ManifestEventAction::Create,
        "update" => ManifestEventAction::Update,
        "delete" => ManifestEventAction::Delete,
        "restore" => ManifestEventAction::Restore,
        other => panic!("unknown action {other}"),
    }
}

fn flag(object: &Value, key: &str) -> bool {
    object[key].as_bool().unwrap()
}

fn field(info: &Value) -> ManifestField {
    let ops = strings(&info["filter_ops"]);
    let ops: Vec<&str> = ops.iter().map(String::as_str).collect();
    let mut field = ManifestField::new(
        info["name"].as_str().unwrap(),
        value_type(info["value_type"].as_str().unwrap()),
    )
    .with_filter_ops(&ops);
    field.choices = info["choices"]
        .as_array()
        .map(|_| strings(&info["choices"]));
    field.sortable = flag(info, "sortable");
    field.read_only = flag(info, "read_only");
    field.required = flag(info, "required");
    field
}

fn descriptor(model: &Value, server: &Value) -> ResourceDescriptor {
    let schemas = &model["schemas"];
    let mut d = ResourceDescriptor::new(
        model["name"].as_str().unwrap(),
        model["title"].as_str().unwrap(),
        model["api_version"].as_str().unwrap(),
        model["base_path"].as_str().unwrap(),
        schemas["read"].clone(),
        schemas["create"].clone(),
        schemas["update"].clone(),
    );
    d.schemas.draft = Some(schemas["draft"].clone()).filter(|v| !v.is_null());
    d.formats = model["formats"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
        .collect();
    model["id_field"]
        .as_str()
        .unwrap()
        .clone_into(&mut d.id_field);
    d.fields = model["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(field)
        .collect();
    let caps = &model["capabilities"];
    d.features = Features {
        bulk_update: flag(caps, "bulk_update"),
        bulk_delete: flag(caps, "bulk_delete"),
        clone: flag(caps, "clone"),
        draft_publish: flag(caps, "draft_publish"),
        archive_restore: flag(caps, "archive_restore"),
        revisions: flag(caps, "revisions"),
        stats: flag(caps, "stats"),
        predict: flag(caps, "predict"),
        conditional_requests: flag(caps, "conditional_requests"),
        owner_scoped: flag(caps, "owner_scoped"),
        schedulable: flag(caps, "schedulable"),
    };
    if let Some(events) = caps["events"].as_object() {
        let replay = if events["replay"] == "persistent" {
            Replay::Persistent
        } else {
            Replay::None
        };
        let mut descriptor = EventsDescriptor::new(events["resource"].as_str().unwrap(), replay);
        descriptor.keepalive_seconds = events["keepalive_seconds"].as_f64().unwrap();
        descriptor.actions = events["actions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| action(a.as_str().unwrap()))
            .collect();
        d.events = Some(descriptor);
    }
    d.access = Access {
        public_read: server["public_read"].as_bool().unwrap_or(false),
        read_roles: server.get("read_roles").map(strings).unwrap_or_default(),
        write_roles: strings(&server["write_roles"]),
        delete_roles: strings(&server["delete_roles"]),
    };
    d
}

fn setup() -> (ManifestRegistry, ManifestConfig) {
    let server = fixture("manifest/server.json");
    let admin = fixture("manifest/admin.manifest.json");
    let registry = ManifestRegistry::new();
    for entry in server["resources"].as_array().unwrap() {
        let model = admin["resources"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["name"] == entry["name"] && m["api_version"] == entry["api_version"])
            .unwrap();
        registry.register(descriptor(model, entry)).unwrap();
    }
    let mut config = ManifestConfig::new(
        server["service"]["name"].as_str().unwrap(),
        server["service"]["version"].as_str().unwrap(),
    );
    config.oidc_discovery_url = server["oidc_discovery_url"].as_str().map(str::to_owned);
    config.stats_ttl = server["stats_ttl_seconds"].as_u64();
    (registry, config)
}

fn caller(request: &Value) -> Caller {
    if flag(request, "authenticated") {
        Caller::Authenticated {
            roles: strings(&request["roles"]),
        }
    } else {
        Caller::Anonymous
    }
}

fn schema_errors(manifest: &Value) -> Vec<String> {
    let schema = fixture("schemas/crud-manifest/v1.json");
    let validator = jsonschema::validator_for(&schema).unwrap();
    validator
        .iter_errors(manifest)
        .map(|e| e.to_string())
        .collect()
}

#[test]
fn builder_output_equals_every_fixture_and_validates() {
    let (registry, config) = setup();
    for case in CASES {
        let request = fixture(&format!("manifest/{case}.request.json"));
        let expected = fixture(&format!("manifest/{case}.manifest.json"));
        let visible = registry.visible_to(&caller(&request));
        let manifest = build_manifest(visible.iter().map(|r| &**r), &config);
        let actual = serde_json::to_value(&manifest).unwrap();
        assert_eq!(actual, expected, "case {case}");
        assert_eq!(schema_errors(&actual), Vec::<String>::new(), "case {case}");
    }
}

#[tokio::test]
async fn route_serves_every_fixture_with_validators_and_a_304() {
    let (registry, config) = setup();
    let resolver = |headers: &HeaderMap| match headers.get("x-test-roles") {
        None => Caller::Anonymous,
        Some(roles) => Caller::Authenticated {
            roles: roles
                .to_str()
                .unwrap()
                .split(',')
                .filter(|r| !r.is_empty())
                .map(str::to_owned)
                .collect(),
        },
    };
    let router = manifest_router(registry, config, resolver);
    for case in CASES {
        let request = fixture(&format!("manifest/{case}.request.json"));
        let expected = fixture(&format!("manifest/{case}.manifest.json"));
        let build = |etag: Option<&str>| {
            let mut builder = Request::builder().uri(request["path"].as_str().unwrap());
            if flag(&request, "authenticated") {
                builder = builder.header("x-test-roles", strings(&request["roles"]).join(","));
            }
            if let Some(etag) = etag {
                builder = builder.header(IF_NONE_MATCH, etag);
            }
            builder.body(Body::empty()).unwrap()
        };
        let response = router.clone().oneshot(build(None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "case {case}");
        assert_eq!(response.headers()[CACHE_CONTROL], "private, no-cache");
        let etag = response.headers()[ETAG].to_str().unwrap().to_owned();
        assert!(etag.starts_with("W/\""), "{etag}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let actual: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(actual, expected, "case {case}");

        let again = router.clone().oneshot(build(Some(&etag))).await.unwrap();
        assert_eq!(again.status(), StatusCode::NOT_MODIFIED, "case {case}");
        assert_eq!(again.headers()[ETAG].to_str().unwrap(), etag);
        assert!(
            to_bytes(again.into_body(), usize::MAX)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
