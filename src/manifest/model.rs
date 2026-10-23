//! The wire types of the CRUD manifest v1 document
//! (`schemas/crud-manifest/v1.json` in `lib-crud-py`). Field order is the
//! order `lib-crud-py` emits, so a serialized manifest reads the same.

use serde::Serialize;
use serde_json::Value;

/// `manifest_version` of every document this module builds.
pub const MANIFEST_VERSION: &str = "1";
/// The `profile` of the api-catalog's `describedby` link: the manifest's
/// JSON Schema `$id`.
pub const MANIFEST_PROFILE: &str = "https://craftainer.github.io/schemas/crud-manifest/v1";

/// The value type of a filterable field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ValueType {
    /// A whole number.
    Integer,
    /// A floating-point number.
    Number,
    /// A calendar date.
    Date,
    /// A timestamp.
    Datetime,
    /// Free text.
    String,
    /// A boolean.
    Boolean,
    /// One of a fixed set of strings (`choices`).
    Enum,
}

/// An action an event stream can carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ManifestEventAction {
    /// A record was created.
    Create,
    /// A record was updated.
    Update,
    /// A record was deleted.
    Delete,
    /// An archived record was restored.
    Restore,
}

/// Whether a resumed event stream replays what it missed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Replay {
    /// Events missed while disconnected are replayed on resume (a
    /// broker-backed bus).
    Persistent,
    /// A reconnect starts fresh (the in-memory bus).
    None,
}

/// How strong a response's `ETag` is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EtagStrength {
    /// Weak: a page of records.
    Weak,
    /// Strong: one record.
    Strong,
}

/// The serving application.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServiceInfo {
    /// Application name.
    pub name: String,
    /// Application version.
    pub version: String,
}

/// The JSON Schemas of a resource's records and request bodies.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SchemaSet {
    /// The record as read.
    pub read: Value,
    /// The create body.
    pub create: Value,
    /// The update body.
    pub update: Value,
    /// The draft body, or `null` for a resource without drafts.
    pub draft: Option<Value>,
}

/// One filterable and sortable field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the manifest's field object is a flat set of booleans"
)]
pub struct FieldInfo {
    /// Property name; also the query key.
    pub name: String,
    /// The value type.
    pub value_type: ValueType,
    /// Operators the server accepts for the field.
    pub filter_ops: Vec<String>,
    /// Whether `sort=` accepts the field.
    pub sortable: bool,
    /// Allowed values of an `enum` field, otherwise `null`.
    pub choices: Option<Vec<String>>,
    /// Plain integer or number: eligible for numeric stats and `predict`.
    pub numeric: bool,
    /// Boolean or enum: eligible for a value distribution.
    pub categorical: bool,
    /// In neither the create nor the update schema.
    pub read_only: bool,
    /// Required by the create schema.
    pub required: bool,
}

/// Offset pagination as the list route implements it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Pagination {
    /// Always `offset`.
    pub style: &'static str,
    /// Query key of the offset.
    pub skip_param: &'static str,
    /// Query key of the page size.
    pub limit_param: &'static str,
    /// Query key of the sort order.
    pub sort_param: &'static str,
    /// Page size when `limit` is absent.
    pub default_limit: u64,
    /// Largest accepted `limit`.
    pub max_limit: u64,
    /// Whether a total count is sent (it is not).
    pub total: bool,
}

/// The event stream of a resource.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Events {
    /// Always `sse`.
    pub transport: &'static str,
    /// The stream's path; equals `paths.events`.
    pub path: String,
    /// The label inside each event's `resource` member.
    pub resource: String,
    /// Query key that resumes a subscription.
    pub resume_param: &'static str,
    /// Whether a resumed stream replays what it missed.
    pub replay: Replay,
    /// Idle seconds before a keep-alive comment frame.
    pub keepalive_seconds: f64,
    /// Actions the stream can carry.
    pub actions: Vec<ManifestEventAction>,
}

/// What a resource supports.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the manifest's capabilities object is a flat set of booleans"
)]
pub struct Capabilities {
    /// Bulk `PATCH` by filters.
    pub bulk_update: bool,
    /// Bulk `DELETE` by filters.
    pub bulk_delete: bool,
    /// The `clone` route.
    pub clone: bool,
    /// The `draft` and `publish` routes.
    pub draft_publish: bool,
    /// The `restore` route.
    pub archive_restore: bool,
    /// The `revisions` route.
    pub revisions: bool,
    /// The event stream, or `null`.
    pub events: Option<Events>,
    /// The `stats` route.
    pub stats: bool,
    /// The `predict` route.
    pub predict: bool,
    /// `ETag`, `If-None-Match` and `If-Match` are honoured.
    pub conditional_requests: bool,
    /// Callers see only records they own.
    pub owner_scoped: bool,
    /// Records have a publication window.
    pub schedulable: bool,
}

/// Absolute paths of a resource's sub-routes. A key is present exactly when
/// its capability is on; `filters` and `clone` always are.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Paths {
    /// The filter-vocabulary route.
    pub filters: String,
    /// The stats route.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,
    /// The predict route.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predict: Option<String>,
    /// The revisions route.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revisions: Option<String>,
    /// The clone route.
    pub clone: String,
    /// The draft route.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub draft: Option<String>,
    /// The publish route.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish: Option<String>,
    /// The restore route.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore: Option<String>,
    /// The event stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events: Option<String>,
}

/// Limits of bulk `PATCH`/`DELETE`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Bulk {
    /// A bulk action matching more records is refused with `400`.
    pub max_matched: u64,
}

/// How a resource is protected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Auth {
    /// Always `bearer`.
    pub scheme: &'static str,
    /// The OIDC discovery document, when the service has one.
    pub oidc_discovery_url: Option<String>,
    /// Anyone may read the resource.
    pub public_read: bool,
    /// Roles that may read (empty when `public_read`).
    pub read_roles: Vec<String>,
    /// Roles that may write.
    pub write_roles: Vec<String>,
    /// Roles that may delete.
    pub delete_roles: Vec<String>,
}

/// Caching hints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Cache {
    /// Responses carry an `ETag`.
    pub etag: bool,
    /// Seconds a response is fresh without revalidation.
    pub max_age: u64,
    /// Server-side TTL of cached stats bodies, when known.
    pub stats_ttl: Option<u64>,
    /// Seconds a stale response may be served while revalidating.
    pub stale_while_revalidate: u64,
    /// Strength of a list response's `ETag`.
    pub list_etag: Option<EtagStrength>,
    /// Strength of a single record's `ETag`.
    pub record_etag: Option<EtagStrength>,
    /// Responses must be revalidated before every reuse.
    pub revalidate_only: bool,
}

/// One resource version.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResourceManifest {
    /// Machine name.
    pub name: String,
    /// Display name.
    pub title: String,
    /// The resource version (`v1`).
    pub api_version: String,
    /// Collection URL of the JSON API.
    pub base_path: String,
    /// Format to root path.
    pub formats: std::collections::BTreeMap<String, String>,
    /// Name of the identifier property.
    pub id_field: String,
    /// The JSON Schemas.
    pub schemas: SchemaSet,
    /// Filterable and sortable fields.
    pub fields: Vec<FieldInfo>,
    /// Pagination.
    pub pagination: Pagination,
    /// Capabilities.
    pub capabilities: Capabilities,
    /// Sub-routes.
    pub paths: Paths,
    /// Bulk limits.
    pub bulk: Bulk,
    /// Protection.
    pub auth: Auth,
    /// Caching hints.
    pub cache: Cache,
}

/// The whole document.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Manifest {
    /// Always [`MANIFEST_VERSION`].
    pub manifest_version: &'static str,
    /// The serving application.
    pub service: ServiceInfo,
    /// Readable resources, ordered by name then `api_version`.
    pub resources: Vec<ResourceManifest>,
}
