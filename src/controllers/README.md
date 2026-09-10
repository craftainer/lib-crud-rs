# generic/controllers/

The generic half of the Controller layer (see `src/README.md`'s
"Generic vs. Hero-specific split"). Two kinds of file live here:

- Pure, resource-agnostic helper functions generic over any
  `R: generic::repositories::Repository`, with no axum `State` of their
  own: `crud_actions.rs` (shared id/filter/bulk list-or-get/update/delete
  decision logic), `crud_query.rs` (query-string-to-`FilterClause`/
  `SortClause` parsing, driven by a resource's own field table),
  `crud_stats.rs` (resource-agnostic parts of `GET /stats`/`GET
  /predict`: `TimeBucket`, field-kind narrowing, the OLS `forecast`),
  `crud_events.rs` (resource-agnostic parts of `GET <prefix>/events`:
  subscriber-id resolution, the SSE frame shape, the `publish` helper).
- Full axum routers that need nothing resource-specific at all:
  `health.rs` (`GET /health/live`, `GET /health/ready`), `audit.rs`
  (`GET /audit`), `mock.rs` (`POST /mock/token`). Each is generic over
  its state type `S`, bound only by the small accessor traits this
  package's `mod.rs` defines (`HasSettings`/`HasHealthRegistry`/
  `HasRateLimiter` — mirrors `oidc::HasOidcVerifier`'s own pattern) —
  never the concrete, Hero-bearing `crate::hero::controllers::AppState`.
  `lib.rs::build_router` instantiates each at the concrete state type
  and merges the result with Hero's own routers into one
  `Router<AppState>`.

Hero's own routers (`heroes.rs` and siblings) live in
`crate::hero::controllers`, not here — see that package's own `README.md`.
A resource's router is the one place allowed to reference both this
package and its own resource-specific types concretely; this package
never references `crate::hero` (or any future resource) at all, enforced
by `.github/scripts/check_layering.py` (see `docs/adrs/0009`).

## Do

- Keep a new resource-agnostic helper generic over `R: Repository` (or
  whatever trait bound it actually needs) rather than hardcoding a
  concrete resource type, so a second resource gets it for free.
- Give a new router here that needs some piece of shared state its own
  narrow `HasX` accessor trait, the same way `HasSettings`/
  `HasHealthRegistry`/`HasRateLimiter` do — never a direct dependency on
  `crate::hero::controllers::AppState`.
- Use a small `FakeState` implementing only the trait(s) a router's own
  tests need (see `audit.rs`/`health.rs`/`mock.rs`'s test modules),
  never a real resource's repository/state, even in `#[cfg(test)]` code.

## Don't

- Reference `crate::hero` (or any other resource-specific module) from
  this package, in production code or tests — see
  `docs/nfrs/NFR-0004-generic-crud-excludes-resource-logic.md`.
