//! What a host tells the manifest about each resource: [`ResourceDescriptor`]
//! (one per resource version, kept in a [`ManifestRegistry`]), plus the
//! service-wide [`ManifestConfig`].
//!
//! The crate has no router of its own to introspect, so the host registers
//! what it mounted. [`ManifestField::from_spec`] derives a field's value type
//! and operators from the same [`FieldSpec`] table the query parser runs on,
//! which is what keeps the manifest from drifting from behaviour.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};

use serde_json::Value;

use crate::controllers::crud_query::{FieldKind, FieldSpec, ops_for};
use crate::manifest::model::{ManifestEventAction, Replay, SchemaSet, ValueType};

/// `bulk.max_matched` when the host does not set one: the cap
/// `lib-crud-py` defaults to (`bulk_action_max_matched`).
pub const DEFAULT_BULK_MAX_MATCHED: u64 = 1000;
/// `events.keepalive_seconds` when the host does not set one: the interval
/// of the keep-alive comment frame in [`crate::controllers::crud_events`].
pub const DEFAULT_KEEPALIVE_SECONDS: f64 = 15.0;
/// Where the manifest route is mounted unless the host says otherwise.
pub const DEFAULT_PREFIX: &str = "/api/_crud";

/// One filterable and sortable field of a resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestField {
    /// Property name; also the query key.
    pub name: String,
    /// The value type.
    pub value_type: ValueType,
    /// Operators the server accepts for the field.
    pub filter_ops: Vec<String>,
    /// Whether `sort=` accepts the field.
    pub sortable: bool,
    /// Allowed values of an `enum` field.
    pub choices: Option<Vec<String>>,
    /// In neither the create nor the update schema.
    pub read_only: bool,
    /// Required by the create schema.
    pub required: bool,
}

impl ManifestField {
    /// A field of `value_type` with that type's default operators (see
    /// [`default_ops`]), sortable, writable and optional. Refine it with the
    /// `with_*` methods.
    #[must_use]
    pub fn new(name: impl Into<String>, value_type: ValueType) -> Self {
        Self {
            name: name.into(),
            value_type,
            filter_ops: default_ops(value_type)
                .iter()
                .map(|op| (*op).to_owned())
                .collect(),
            sortable: true,
            choices: None,
            read_only: false,
            required: false,
        }
    }

    /// The field a [`FieldSpec`] describes. Its operators come from the
    /// parser's own table, so they are exactly what the server accepts.
    #[must_use]
    pub fn from_spec(spec: &FieldSpec) -> Self {
        let value_type = match spec.kind {
            FieldKind::Number => ValueType::Integer,
            FieldKind::Float => ValueType::Number,
            FieldKind::DateTime => ValueType::Datetime,
            FieldKind::String => ValueType::String,
            FieldKind::Boolean => ValueType::Boolean,
        };
        Self::new(spec.name, value_type).with_filter_ops(ops_for(spec.kind))
    }

    /// Replace the operators.
    #[must_use]
    pub fn with_filter_ops(mut self, ops: &[&str]) -> Self {
        self.filter_ops = ops.iter().map(|op| (*op).to_owned()).collect();
        self
    }

    /// Mark the field as an enum with these allowed values.
    #[must_use]
    pub fn with_choices(mut self, choices: &[&str]) -> Self {
        self.value_type = ValueType::Enum;
        self.filter_ops = default_ops(ValueType::Enum)
            .iter()
            .map(|op| (*op).to_owned())
            .collect();
        self.choices = Some(choices.iter().map(|c| (*c).to_owned()).collect());
        self
    }

    /// Mark the field as not accepted by the create and update bodies.
    #[must_use]
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    /// Mark the field as required by the create body.
    #[must_use]
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Mark the field as not sortable.
    #[must_use]
    pub fn not_sortable(mut self) -> Self {
        self.sortable = false;
        self
    }
}

/// The operators this crate's query parser accepts for a value type:
/// numbers and timestamps take `eq min max in`, text takes
/// `eq contains icontains` (no `regex`; see ADR 0004), booleans and enums
/// take `eq in`.
#[must_use]
pub fn default_ops(value_type: ValueType) -> &'static [&'static str] {
    match value_type {
        ValueType::Integer | ValueType::Number | ValueType::Date | ValueType::Datetime => {
            ops_for(FieldKind::Number)
        }
        ValueType::String => ops_for(FieldKind::String),
        ValueType::Boolean | ValueType::Enum => ops_for(FieldKind::Boolean),
    }
}

/// The optional routes and behaviours a resource has. The host sets what it
/// actually mounted; `Default` is bulk update and delete on, the rest off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the manifest's capabilities object is a flat set of booleans"
)]
pub struct Features {
    /// Bulk `PATCH` by filters.
    pub bulk_update: bool,
    /// Bulk `DELETE` by filters.
    pub bulk_delete: bool,
    /// A `clone` route is mounted. `paths.clone` is emitted either way (the
    /// schema requires it), so mount the route or leave this off knowingly.
    pub clone: bool,
    /// `draft` and `publish` routes are mounted.
    pub draft_publish: bool,
    /// A `restore` route is mounted.
    pub archive_restore: bool,
    /// A `revisions` route is mounted.
    pub revisions: bool,
    /// A `stats` route is mounted.
    pub stats: bool,
    /// A `predict` route is mounted.
    pub predict: bool,
    /// `ETag`, `If-None-Match` and `If-Match` are honoured. The crate's
    /// routes do not apply [`crate::conditional`] for the host, so set this
    /// only when the host's handlers call it.
    pub conditional_requests: bool,
    /// Callers see only records they own.
    pub owner_scoped: bool,
    /// Records have a publication window.
    pub schedulable: bool,
}

impl Default for Features {
    fn default() -> Self {
        Self {
            bulk_update: true,
            bulk_delete: true,
            clone: false,
            draft_publish: false,
            archive_restore: false,
            revisions: false,
            stats: false,
            predict: false,
            conditional_requests: false,
            owner_scoped: false,
            schedulable: false,
        }
    }
}

/// A resource's event stream. The stream's path is `<base_path>/events`.
#[derive(Debug, Clone, PartialEq)]
pub struct EventsDescriptor {
    /// The label inside each event's `resource` member (the topic segment
    /// the host publishes under).
    pub resource: String,
    /// Whether a resumed stream replays what it missed: `Persistent` for the
    /// MQTT-backed bus, `None` for the in-memory one.
    pub replay: Replay,
    /// Idle seconds before a keep-alive comment frame.
    pub keepalive_seconds: f64,
    /// Actions the stream can carry.
    pub actions: Vec<ManifestEventAction>,
}

impl EventsDescriptor {
    /// A stream under `resource` that carries `create`, `update` and
    /// `delete`, with the default keep-alive. A resource with a `restore`
    /// route also publishes restores: push [`ManifestEventAction::Restore`]
    /// onto `actions`.
    #[must_use]
    pub fn new(resource: impl Into<String>, replay: Replay) -> Self {
        Self {
            resource: resource.into(),
            replay,
            keepalive_seconds: DEFAULT_KEEPALIVE_SECONDS,
            actions: vec![
                ManifestEventAction::Create,
                ManifestEventAction::Update,
                ManifestEventAction::Delete,
            ],
        }
    }
}

/// Who may do what with a resource.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Access {
    /// Anyone, signed in or not, may read the resource.
    pub public_read: bool,
    /// Roles that may read; a caller needs any one. Ignored when
    /// `public_read`.
    pub read_roles: Vec<String>,
    /// Roles that may write.
    pub write_roles: Vec<String>,
    /// Roles that may delete.
    pub delete_roles: Vec<String>,
}

/// Who is calling, as the host's [`CallerResolver`](crate::manifest::CallerResolver)
/// decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    /// No valid credentials.
    Anonymous,
    /// Signed in, with these roles.
    Authenticated {
        /// The caller's roles.
        roles: Vec<String>,
    },
}

impl Access {
    /// Whether `caller` may read the resource, which is what decides
    /// whether the manifest lists it: a public resource is listed for
    /// everyone; otherwise the caller must be signed in and hold one of
    /// `read_roles`. A resource with no read roles and not public is listed
    /// for no one.
    #[must_use]
    pub fn may_read(&self, caller: &Caller) -> bool {
        if self.public_read {
            return true;
        }
        match caller {
            Caller::Anonymous => false,
            Caller::Authenticated { roles } => {
                roles.iter().any(|role| self.read_roles.contains(role))
            }
        }
    }
}

/// Everything the manifest says about one resource version.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ResourceDescriptor {
    /// Machine name (`widgets`).
    pub name: String,
    /// Display name.
    pub title: String,
    /// The resource version (`v1`).
    pub api_version: String,
    /// Collection URL of the JSON API; sub-routes hang off it.
    pub base_path: String,
    /// Format to root path; `json` (equal to `base_path`) is always there.
    pub formats: BTreeMap<String, String>,
    /// Name of the identifier property.
    pub id_field: String,
    /// The JSON Schemas of records and bodies.
    pub schemas: SchemaSet,
    /// The filterable and sortable fields.
    pub fields: Vec<ManifestField>,
    /// Optional routes and behaviours.
    pub features: Features,
    /// The event stream, when there is one.
    pub events: Option<EventsDescriptor>,
    /// Who may read, write and delete.
    pub access: Access,
}

impl ResourceDescriptor {
    /// A resource with the given identity and schemas, no fields and no
    /// optional routes, identified by `id` and readable by no one until
    /// `access` is set.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        title: impl Into<String>,
        api_version: impl Into<String>,
        base_path: impl Into<String>,
        read_schema: Value,
        create_schema: Value,
        update_schema: Value,
    ) -> Self {
        let base_path = base_path.into();
        Self {
            name: name.into(),
            title: title.into(),
            api_version: api_version.into(),
            formats: BTreeMap::from([("json".to_owned(), base_path.clone())]),
            base_path,
            id_field: "id".to_owned(),
            schemas: SchemaSet {
                read: read_schema,
                create: create_schema,
                update: update_schema,
                draft: None,
            },
            fields: Vec::new(),
            features: Features::default(),
            events: None,
            access: Access::default(),
        }
    }

    /// `(name, api_version)`: what makes two descriptors the same resource.
    fn identity(&self) -> (&str, &str) {
        (&self.name, &self.api_version)
    }
}

/// Service-wide settings of the manifest and its route.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ManifestConfig {
    /// The application's name.
    pub service_name: String,
    /// The application's version.
    pub service_version: String,
    /// The OIDC discovery document, advertised as `auth.oidc_discovery_url`.
    pub oidc_discovery_url: Option<String>,
    /// TTL in seconds of the host's cached stats bodies, which the crate
    /// cannot see; `None` reports "unknown or no cache".
    pub stats_ttl: Option<u64>,
    /// `bulk.max_matched`: the cap the host passes to the bulk actions.
    pub bulk_max_matched: u64,
    /// Where the route is mounted; it serves `<prefix>/manifest`.
    pub prefix: String,
}

impl ManifestConfig {
    /// Defaults for everything but the service's identity.
    #[must_use]
    pub fn new(service_name: impl Into<String>, service_version: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            service_version: service_version.into(),
            oidc_discovery_url: None,
            stats_ttl: None,
            bulk_max_matched: DEFAULT_BULK_MAX_MATCHED,
            prefix: DEFAULT_PREFIX.to_owned(),
        }
    }
}

/// Registering a resource failed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("resource {name:?} {api_version} is already registered")]
pub struct DuplicateResource {
    /// The resource's name.
    pub name: String,
    /// The resource's version.
    pub api_version: String,
}

/// The resources a host has registered, shared between the host's startup
/// code and the manifest route (cloning shares the same list).
#[derive(Debug, Clone, Default)]
pub struct ManifestRegistry {
    resources: Arc<RwLock<Vec<Arc<ResourceDescriptor>>>>,
}

impl ManifestRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `resource`.
    ///
    /// # Errors
    ///
    /// [`DuplicateResource`] when its `(name, api_version)` is already
    /// registered.
    pub fn register(&self, resource: ResourceDescriptor) -> Result<(), DuplicateResource> {
        let mut resources = self
            .resources
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if resources
            .iter()
            .any(|known| known.identity() == resource.identity())
        {
            return Err(DuplicateResource {
                name: resource.name,
                api_version: resource.api_version,
            });
        }
        resources.push(Arc::new(resource));
        Ok(())
    }

    /// The registered resources `caller` may read, in registration order.
    #[must_use]
    pub fn visible_to(&self, caller: &Caller) -> Vec<Arc<ResourceDescriptor>> {
        self.resources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|resource| resource.access.may_read(caller))
            .cloned()
            .collect()
    }
}
