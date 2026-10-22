# views/

Resource-agnostic DTO building blocks.

- `mod.rs` — `FieldError`, the field-level validation failure rendered into
  an RFC 9457 `detail` list.
- `bulk.rs` — `BulkUpdateResult`/`BulkDeleteResult`.
- `stats.rs` — `ResourceStats`/`Prediction` and their parts, the `/stats` and
  `/predict` response bodies.

A host's own DTOs (create, update and read shapes per resource) live in the
host.

## Don't

- Add a resource-specific DTO.
