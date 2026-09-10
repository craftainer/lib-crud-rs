# generic/repositories/

The generic half of storage-agnostic CRUD access (see `src/README.md`'s
"Generic vs. Hero-specific split"), backing `crud/`. See
`docs/adrs/0001-mvc-layering-with-a-generic-crud-interface.md` for why
this exists as a trait with two implementations per resource, and for
the Rust-specific deviation from template-fastapi's fully dynamic
`SQLAlchemyRepository`.

- `mod.rs` — the `Repository` trait (associated types `Model`/
  `Create`/`Update`; methods `list`/`count`/`get`/`create`/`update`/
  `update_many`/`delete`/`delete_many`), `ListOptions`, `RepoError`, and
  the `dyn_repository!` macro (generates a concrete, boxed-trait-object
  wrapper per resource — see its doc comment for why a generic blanket
  `impl` doesn't work here).
- `filtering.rs` — `FilterOp`/`FilterClause`/`FilterValue`/
  `SortClause`, the storage-agnostic filter/sort vocabulary each
  `Repository` impl interprets itself (`docs/adrs/0013`).

Hero's own `HeroSeaOrmRepository`/`HeroMemoryRepository` implementations
live in `crate::hero::repositories`, not here — see that package's own
`README.md`. A new resource adds its own sibling package the same way,
plus a `crate::dyn_repository!(...)` invocation in its own controllers'
`mod.rs` — no changes to this package.

## Don't

- Add a resource-specific `impl Repository` or any Hero-aware logic
  here — see `docs/nfrs/NFR-0004-generic-crud-excludes-resource-logic.md`.
