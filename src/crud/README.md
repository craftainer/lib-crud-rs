# crud/

The generic CRUD interface: `CrudService<R>`, built from any `R:
Repository`. Contains zero resource-specific code
(`docs/nfrs/0004-generic-crud-excludes-resource-logic.md`) — a resource
that needs bespoke query logic (a search endpoint, a bulk operation)
adds it directly in its own controller against its own `Repository`
impl, not by extending `CrudService`.

`DEFAULT_LIMIT`/`MAX_LIMIT` are the only resource-agnostic policy this
module owns (a list request's default/maximum page size); everything
else is a pass-through to `R`'s methods.
