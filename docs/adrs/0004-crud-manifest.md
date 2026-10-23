# 0004. Serve the CRUD manifest v1 and forecast on bucket time

## Status

Accepted

## Context

The templates, SDKs and UI plan (item AX3) has every CRUD server describe its
resources in one JSON document, the CRUD manifest (contract I1), so that
clients configure themselves instead of hard-coding resources. `lib-crud-py`
(ADR 0002 there) owns the contract: a JSON Schema, a prose spec and shared
conformance fixtures, which this repository vendors under `tests/fixtures/`
with their provenance commit. `lib-crud-py` builds the manifest from the router
factories' registrations and `field_specs`. This crate has no router factory:
a host composes the plain functions in `controllers/` into its own routes, so
there is nothing to introspect.

Work item MF4 also changed how both servers forecast (ADR 0003 had recorded
the Rust forecast regressing on series position, as Python did then, and left
the change for both servers together).

## Decision

**The host registers what it mounted.** `manifest::ResourceDescriptor` (one
per `(name, api_version)`, held in a `ManifestRegistry`) carries the identity,
paths root, JSON Schemas (supplied by the host, for example from `schemars`),
fields, `Features` (the optional routes), the event stream, and `Access`
(public or role lists). A field is derived from the same `FieldSpec` table the
query parser runs on (`ManifestField::from_spec`), so its value type and
operators cannot drift from what `parse_filters` accepts. `build_manifest`
renders descriptors to the `Manifest` document; it derives `paths`, `auth`,
`cache`, `pagination` (from `crud::DEFAULT_LIMIT`/`MAX_LIMIT`), `bulk` and the
`events` object from the descriptor and `ManifestConfig`, as Python does. The
registry is read on every request.

**Caller filtering is the host's decision, applied by the crate.** The crate
knows no authentication scheme. `manifest_router` takes a `CallerResolver`
(`Fn(&HeaderMap) -> Caller`, or an async trait impl that may fail) and lists a
resource when the caller may read it: public resources for everyone, otherwise
a signed-in caller holding one of `read_roles`. Anonymous callers and callers
whose roles match nothing see the same thing; a resource with no read roles
that is not public is listed for no one. A resolver error (an identity
provider outage) is rendered as its problem response, not as a hidden
resource, as Python does for a 503.

**Validators as in Python.** The body is the compact JSON of the filtered
document; the `ETag` is `W/"<first 32 hex digits of SHA-256(body)>"`, the same
construction as `lib-crud-py`; `Cache-Control: private, no-cache`; a matching
`If-None-Match` (weak comparison, `*` included) gets a bodiless `304` with the
same validators.

**`api_catalog_router`** serves `/.well-known/api-catalog` (RFC 9727,
`application/linkset+json`) with a `describedby` link to the manifest and the
optional `service-desc` link. Python follows the serving app's `openapi_url`
(MF3); axum has no such app-wide setting, so the host passes
`Option<String>`: `None` omits the link, so an app that serves no OpenAPI
document leaves no dangling one. The anchor defaults to the request's base URL
(`Host`, `X-Forwarded-Proto`), and can be fixed explicitly.

**Cache hints tell the truth about this crate.** Python always reports
conditional requests. This crate does not implement them yet, so the
`conditional_requests` feature defaults to off, and while it is off `cache`
reports `etag: false`, no `list_etag`/`record_etag` and `revalidate_only:
false` (the schema allows exactly that). A host that sets the feature owns
making it true.

**Forecast on bucket time (MF4 (b)).** Superseding ADR 0003's "documented, not
changed" note: `forecast` now regresses on `x_i`, the whole buckets between
the first bucket and bucket `i` (`Day`: days, `Week`: days divided by 7,
floored, `Month`: `12 * year difference + month difference`), so an empty
bucket leaves a gap instead of shifting later points. Sums are accumulated
left to right with `+=` (no iterator `sum`, which is not guaranteed to
associate the same way), `slope` is `0` when `sxx == 0`, and
`value_k = slope * (x_{n-1} + k) + intercept`. It is the formula in
`lib-crud-py`'s `forecast` docstring and `docs/manifest.md`, and a test pins
x = 0, 1, 4, 5 with y = 3, 1, 7, 4 to `104/17` at `k = 1`, as Python's does.

## Consequences

Where Rust cannot match Python, and why (the conformance test feeds the
fixture's own operators and values for these, and says so):

- **`regex` filter operator.** There is no `field__regex=` in this crate's
  query layer, so a string field derived from a `FieldSpec` reports
  `eq contains icontains`, without `regex`. A manifest from this crate
  therefore differs from the fixtures' string fields unless the host overrides
  the operators.
- **`enum` and `date` value types.** `FieldKind` has no enum or date kind, so
  `from_spec` never produces them. The manifest layer supports both
  (`ManifestField::with_choices`, `ValueType::Date`), but the query parser does
  not cast them; a host that lists one must handle it in its own filter
  parsing.
- **Event actions.** The manifest enum is `create`, `update`, `delete`,
  `restore`. This crate's stream also carries `update_many` and `delete_many`
  frames (one per bulk action, with the ids) where Python emits one event per
  record, and has no `restore`. The descriptor lists only the manifest's four;
  a client must tolerate the two bulk names. Aligning the stream is a change to
  `events`, not to the manifest.
- **`schedulable`, `owner_scoped`, `draft_publish`, `archive_restore`,
  `revisions`, `clone`, `filters` route.** These are host-declared. `paths.filters`
  and `paths.clone` are always emitted because the schema requires them, so a
  host must mount those two routes, which this crate does not provide.
- **Schemas** are the host's JSON Schemas, not generated here; the
  conformance contract compares them structurally, and the test passes the
  fixture's through.

The crate gains `manifest/` (public API: the types above, `build_manifest`,
`manifest_router`, `api_catalog_router`) and an `axum::Router`-returning
helper, the first router in the crate. `controllers::crud_query::ops_for` is
now `pub(crate)` so the manifest and the parser share one operator table.
Dev-dependencies `jsonschema` (validates the manifest against the I1 schema)
and `tower` (drives the routers in tests). The forecast of a series with gaps
changes: callers that zero-filled to work around the old behaviour no longer
need to.
