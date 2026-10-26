# src/

The `craftainer-crud` library crate (imported as `craftainer_crud`): the
resource-agnostic half of an Axum CRUD service. It knows nothing about a
concrete resource, an authentication scheme or a database; a host supplies
a `repositories::Repository` per resource and composes the helpers here in
its own routers.

Modules, lowest layer first (a module imports only from those above it in
this list, except `error`, which `conditional`, `controllers`, `events` and
`stats_cache` use):

- `models/` — `HasId`, the one model-layer contract.
- `repositories/` — the `Repository` trait, `ListOptions`, `RepoError`,
  `dyn_repository!` and the filter/sort vocabulary.
- `views/` — `FieldError`, bulk-result and stats/predict response shapes.
- `crud/` — `CrudService<R>`, generic over any repository.
- `conditional.rs` — `Versioned`, entity-tags, `If-None-Match` and `If-Match`
  helpers (imports `models` and `error`).
- `stats_cache.rs` — the opt-in `/stats` and `/predict` response cache:
  `StatsCache`, `MemoryStatsCache`, `CachedStats`, `cache_key` (imports `error`
  only).
- `events.rs` — the publish/subscribe bus (in-memory; MQTT behind the `mqtt`
  feature).
- `error.rs` — `CrudError` and its `application/problem+json` rendering.
- `controllers/` — plain functions a resource's router composes.
- `manifest/` — the CRUD manifest (contract I1) built from descriptors a
  host registers, its `GET <prefix>/manifest` router and the `api-catalog`
  router. Imports `conditional`, `controllers::crud_query`, `crud` and `error`.

## Do

- Keep every public item documented: `missing_docs` is a warning and clippy
  runs with `-D warnings`.
- Keep the code free of resource-specific names, fields and sample data, in
  tests too; the fixtures are a neutral `widgets` resource.
- Gate anything that needs a broker behind the `mqtt` feature.

## Don't

- Depend on an ORM, an auth library or an application's state type. A host's
  error type converts from `CrudError` (`From<CrudError>`), not the other way
  round.
