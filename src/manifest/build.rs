//! Build the manifest document from registered resources.
//!
//! Nothing here restates a rule the routes enforce: operators come from the
//! query parser's table ([`ManifestField::from_spec`]), the limits from
//! [`crate::crud`], the optional routes from what the host registered.

use crate::crud::{DEFAULT_LIMIT, MAX_LIMIT};
use crate::manifest::descriptor::{ManifestConfig, ManifestField, ResourceDescriptor};
use crate::manifest::model::{
    Auth, Bulk, Cache, Capabilities, EtagStrength, Events, FieldInfo, MANIFEST_VERSION, Manifest,
    Pagination, Paths, ResourceManifest, ServiceInfo, ValueType,
};

fn field_info(field: &ManifestField) -> FieldInfo {
    FieldInfo {
        name: field.name.clone(),
        value_type: field.value_type,
        filter_ops: field.filter_ops.clone(),
        sortable: field.sortable,
        choices: field.choices.clone(),
        numeric: matches!(field.value_type, ValueType::Integer | ValueType::Number),
        categorical: matches!(field.value_type, ValueType::Boolean | ValueType::Enum),
        read_only: field.read_only,
        required: field.required,
    }
}

fn paths(resource: &ResourceDescriptor) -> Paths {
    let sub = |segment: &str| format!("{}/{segment}", resource.base_path);
    let features = &resource.features;
    Paths {
        filters: sub("filters"),
        stats: features.stats.then(|| sub("stats")),
        predict: features.predict.then(|| sub("predict")),
        revisions: features.revisions.then(|| sub("revisions")),
        clone: sub("clone"),
        draft: features.draft_publish.then(|| sub("draft")),
        publish: features.draft_publish.then(|| sub("publish")),
        restore: features.archive_restore.then(|| sub("restore")),
        events: resource.events.as_ref().map(|_| sub("events")),
    }
}

fn cache(resource: &ResourceDescriptor, config: &ManifestConfig) -> Cache {
    let features = &resource.features;
    let conditional = features.conditional_requests;
    Cache {
        etag: conditional,
        max_age: 0,
        stats_ttl: config
            .stats_ttl
            .filter(|_| features.stats || features.predict),
        stale_while_revalidate: 0,
        list_etag: conditional.then_some(EtagStrength::Weak),
        record_etag: conditional.then_some(EtagStrength::Strong),
        revalidate_only: conditional,
    }
}

fn resource_manifest(resource: &ResourceDescriptor, config: &ManifestConfig) -> ResourceManifest {
    let paths = paths(resource);
    let features = &resource.features;
    let events = resource.events.as_ref().map(|events| Events {
        transport: "sse",
        path: format!("{}/events", resource.base_path),
        resource: events.resource.clone(),
        resume_param: "subscriber_id",
        replay: events.replay,
        keepalive_seconds: events.keepalive_seconds,
        actions: events.actions.clone(),
    });
    let access = &resource.access;
    ResourceManifest {
        name: resource.name.clone(),
        title: resource.title.clone(),
        api_version: resource.api_version.clone(),
        base_path: resource.base_path.clone(),
        formats: resource.formats.clone(),
        id_field: resource.id_field.clone(),
        schemas: resource.schemas.clone(),
        fields: resource.fields.iter().map(field_info).collect(),
        pagination: Pagination {
            style: "offset",
            skip_param: "skip",
            limit_param: "limit",
            sort_param: "sort",
            default_limit: DEFAULT_LIMIT,
            max_limit: MAX_LIMIT,
            total: false,
        },
        capabilities: Capabilities {
            bulk_update: features.bulk_update,
            bulk_delete: features.bulk_delete,
            clone: features.clone,
            draft_publish: features.draft_publish,
            archive_restore: features.archive_restore,
            revisions: features.revisions,
            events,
            stats: features.stats,
            predict: features.predict,
            conditional_requests: features.conditional_requests,
            owner_scoped: features.owner_scoped,
            schedulable: features.schedulable,
        },
        cache: cache(resource, config),
        paths,
        bulk: Bulk {
            max_matched: config.bulk_max_matched,
        },
        auth: Auth {
            scheme: "bearer",
            oidc_discovery_url: config.oidc_discovery_url.clone(),
            public_read: access.public_read,
            read_roles: if access.public_read {
                Vec::new()
            } else {
                access.read_roles.clone()
            },
            write_roles: access.write_roles.clone(),
            delete_roles: access.delete_roles.clone(),
        },
    }
}

/// Build the manifest of `resources`, ordered by name then `api_version`.
///
/// The caller has already been filtered out of `resources`
/// ([`ManifestRegistry::visible_to`](crate::manifest::ManifestRegistry::visible_to));
/// this renders what it is given.
pub fn build_manifest<'a>(
    resources: impl IntoIterator<Item = &'a ResourceDescriptor>,
    config: &ManifestConfig,
) -> Manifest {
    let mut ordered: Vec<&ResourceDescriptor> = resources.into_iter().collect();
    ordered.sort_by(|a, b| (&a.name, &a.api_version).cmp(&(&b.name, &b.api_version)));
    Manifest {
        manifest_version: MANIFEST_VERSION,
        service: ServiceInfo {
            name: config.service_name.clone(),
            version: config.service_version.clone(),
        },
        resources: ordered
            .into_iter()
            .map(|resource| resource_manifest(resource, config))
            .collect(),
    }
}
