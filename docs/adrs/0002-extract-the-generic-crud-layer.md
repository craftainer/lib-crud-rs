# 0002. Extract the generic CRUD layer from template-axum with its own error type

## Status

Accepted

## Context

`template-axum` keeps a generic CRUD layer under `src/generic/` (plus
`src/crud/`, `src/events.rs` and `src/problem_details.rs`) beside the
Hero-specific code, separated by a layering rule that `check_layering.py`
enforces. A static read of the code (work item RS4 of the templates, SDKs and
UI plan) found the boundary clean: `generic::*` imports only from `config`,
`oidc`, `problem_details`, `rate_limit`, `events`, `health`, `crud` and
itself, and the `crate::hero` hits inside `generic/` are comments. The layer is
wanted by more than one service, and `template-fastapi` already ships the
equivalent as the `craftainer-crud` package, so the Rust side follows the same
shape: a crate published as `craftainer-crud` (repository `lib-crud-rs`), built
from `template-rust-crate`.

Three questions needed an answer.

1. **What moves.** The generic layer itself, and the three modules it cannot
   work without: `crud::CrudService`, `events` (the CRUD event bus) and the
   error type it returns. The health checks, the `/live` and `/ready` router,
   OIDC, rate limiting, the `audit` and `mock` routers and the accessor traits
   `HasSettings`/`HasRateLimiter` stay in `template-axum`, because they need
   its configuration and authentication. `HasHealthRegistry` and the health
   checks belong to `lib-observability-rs`.
2. **Which error type.** `crud_actions`, `crud_stats` and `crud_events` return
   `AppError`, which also has `Unauthorized`, `Forbidden` and `TooManyRequests`
   variants the generic layer never produces, and whose redaction switch takes
   the application's `Mode`. Options were to move `AppError` into the crate
   (smallest change, but every dependent inherits an error type it may not
   want), or to give the crate its own error type and let the application
   convert.
3. **Is the MQTT bus core?** `events.rs` pulls in `rumqttc` for the broker-backed
   bus. `template-fastapi` puts MQTT behind an extra.

## Decision

We extract with history, with `git filter-repo` run on a throwaway clone of
`template-axum`'s `main`, keeping `src/generic/{models,views,repositories}`,
`src/generic/controllers/crud_*.rs`, `src/crud/`, `src/events.rs`,
`src/problem_details.rs` and `tests/mqtt_events.rs`, and renaming
`src/generic/` to `src/`. That history is merged into this repository with
`--allow-unrelated-histories`, so `git log` and `git blame` follow the code
back to its origin.

The crate defines its own `CrudError` (`NotFound`, `UnprocessableEntity`,
`ServiceUnavailable`, `Internal`; non-exhaustive), which implements
`IntoResponse` as `application/problem+json`. `template-axum` keeps `AppError`
and gains `From<CrudError>`. `configure_detail_redaction` takes a `bool`
instead of the application's `Mode`. `http_headers::Sunset` stays in the
application.

The in-memory event bus is core. The MQTT bus is the `mqtt` Cargo feature, off
by default, so a dependent that does not run a broker does not compile
`rumqttc`.

`dyn_repository!` expands inside the host crate. Its paths therefore start at
`$crate::repositories` and it uses `$crate::async_trait`, which the crate
re-exports, so a host needs no direct `async-trait` dependency.

The Hero-specific words in comments, doc strings and test data become a
neutral `widgets` resource. The generic layer's tests move with it; those that
needed Hero's SeaORM repository stay in `template-axum`.

The Postgres tests use a plain-`sqlx` `widgets` repository in `tests/support/`
(a dev-dependency only) rather than SeaORM, to keep the crate's build small:
it implements `Repository` and so exercises `CrudService` and every `resolve_*`
helper end to end against a real database.

## Consequences

- A dependent gets the generic layer without Hero, SeaORM, OIDC or Redis, and
  its error type is its own. The price is a conversion in every handler that
  calls a `resolve_*` helper: `?` does it through `From<CrudError>`, but a
  handler that matched on `AppError` variants must match on `CrudError`'s.
- `CrudError` is `#[non_exhaustive]`, so adding a variant is not a breaking
  change, but a dependent's `match` needs a wildcard arm.
- The crate has no Hero-shaped regression test of its own any more; the
  application's end-to-end tests, which stay in `template-axum`, are what
  prove the two still fit together.
- The 100% line-coverage gate uses `llvm-cov`'s best single instantiation of
  a generic function, not the union of several, so each generic helper's
  failure-path tests have to use the same repository type as its
  success-path tests (`tests/README.md`). Tests that need a database or a
  broker mean the devcontainer starts both, and CI needs the same services.
- Reversing the error-type choice later (moving `AppError` in) would be a
  breaking change to every dependent's handlers, which is why it is decided
  now and recorded here.
