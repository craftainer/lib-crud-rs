//! The CRUD manifest (contract I1, version 1): one JSON document that tells a
//! client which resources a server has, the shape of their records, which
//! filters and sorts it accepts, which optional routes are on and how
//! responses may be cached. The contract is specified by `lib-crud-py`
//! (`schemas/crud-manifest/v1.json`, `docs/manifest.md`); this module builds
//! the same document and the crate's tests run the shared conformance
//! fixtures through it.
//!
//! - [`ResourceDescriptor`] and [`ManifestRegistry`]: what a host registers
//!   per resource version; [`ManifestField::from_spec`] derives a field from
//!   the [`FieldSpec`](crate::controllers::crud_query::FieldSpec) table the
//!   query parser runs on.
//! - [`build_manifest`]: descriptors to the [`Manifest`] document.
//! - [`manifest_router`]: `GET <prefix>/manifest`, filtered to the caller,
//!   with a weak `ETag`.
//! - [`api_catalog_router`]: `GET /.well-known/api-catalog` (RFC 9727).

mod build;
mod catalog;
mod descriptor;
pub mod model;
mod route;

pub use build::build_manifest;
pub use catalog::{API_CATALOG_PATH, api_catalog_router};
pub use descriptor::{
    Access, Caller, DEFAULT_BULK_MAX_MATCHED, DEFAULT_KEEPALIVE_SECONDS, DEFAULT_PREFIX,
    DuplicateResource, EventsDescriptor, Features, ManifestConfig, ManifestField, ManifestRegistry,
    ResourceDescriptor, default_ops,
};
pub use model::{
    MANIFEST_PROFILE, MANIFEST_VERSION, Manifest, ManifestEventAction, Replay, SchemaSet, ValueType,
};
pub use route::{CallerResolver, REVALIDATE_CACHE_CONTROL, manifest_router};

#[cfg(test)]
mod tests;
