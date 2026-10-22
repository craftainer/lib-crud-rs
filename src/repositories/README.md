# repositories/

Storage-agnostic CRUD access, backing `crud/`.

- `mod.rs` — the `Repository` trait (associated types `Model`/`Create`/
  `Update`; methods `list`/`count`/`get`/`create`/`update`/`update_many`/
  `delete`/`delete_many`), `ListOptions`, `RepoError` and the
  `dyn_repository!` macro, which generates a concrete, boxed-trait-object
  wrapper per resource (a blanket `impl` trips an async-trait/HRTB limit).
  The macro expands inside the host crate and reaches this crate through
  `$crate`, including `async_trait`, which this crate re-exports.
- `filtering.rs` — `FilterOp`/`FilterClause`/`FilterValue`/`SortClause`, the
  vocabulary each repository interprets itself.

A host's SQL and in-memory repositories live in the host. `tests/support`
holds a plain-`sqlx` example used by the integration tests.

## Don't

- Add a resource-specific `impl Repository`.
