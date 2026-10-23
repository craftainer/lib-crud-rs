# manifest/

The CRUD manifest v1 (contract I1, specified in `lib-crud-py`'s
`docs/manifest.md` and `schemas/crud-manifest/v1.json`) and the routes that
serve it. See [ADR 0004](../../docs/adrs/0004-crud-manifest.md).

- `model.rs` — the wire types, in the field order `lib-crud-py` emits.
- `descriptor.rs` — what a host registers (`ResourceDescriptor`,
  `ManifestField`, `Features`, `Access`), `ManifestConfig`, `ManifestRegistry`.
- `build.rs` — `build_manifest`: descriptors to the document.
- `route.rs` — `manifest_router`, `CallerResolver`, the weak `ETag`.
- `catalog.rs` — `api_catalog_router` (RFC 9727).
- `tests.rs` — unit tests; the shared fixtures run in
  `tests/manifest_conformance.rs`.

## Do

- Derive anything the parser or a route already decides from that code
  (`ManifestField::from_spec`), not from a second list.
- Keep the output identical to `lib-crud-py`'s where the fixtures define it;
  record any difference in the ADR.

## Don't

- Reference a concrete resource or an authentication scheme: the host's
  `CallerResolver` decides who is calling.
