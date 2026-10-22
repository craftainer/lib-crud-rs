# generic/views/

The generic half of the View layer (see `src/README.md`'s "Generic vs.
Hero-specific split"). May import from `generic/models` (for
`From<Model> for ReadDto` conversions), never the reverse.

- `mod.rs` — `FieldError`, the shared field-level validation-failure
  shape every resource's `*Create`/`*Update::validate()` returns.
- `bulk.rs` — `BulkUpdateResult`/`BulkDeleteResult`, the resource-
  agnostic result shapes a bulk update/delete returns (`docs/adrs/0013`).
- `stats.rs` — `ResourceStats`/`Prediction` and their sub-shapes, the
  `GET /stats`/`GET /predict` response bodies (`docs/adrs/0015`).

Hero's own DTOs (`HeroCreate`/`HeroUpdate`/`HeroRead`/...) live in
`crate::hero::views`, not here. Convention there: three DTOs per
resource — a `*Create` (required fields), a `*Update` (all-optional,
PATCH semantics per `FR-0004`), and a plain read DTO with `id` and every
stored field; `*Create`/`*Update` never include server-managed fields
(`id`, `created_at`, `updated_at`, `owner_id`, `archived_at`).

## Don't

- Add a Hero-specific (or any other resource's) DTO here — see
  `docs/nfrs/NFR-0004-generic-crud-excludes-resource-logic.md`.
