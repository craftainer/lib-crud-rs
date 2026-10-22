# crud/

The generic CRUD interface: `CrudService<R>`, built from any `R:
Repository`. Contains zero resource-specific code
(`docs/nfrs/0004-generic-crud-excludes-resource-logic.md`) — a resource
that needs genuinely bespoke query logic (a business-rule search
endpoint, a multi-step workflow) adds it directly in its own controller
against its own `Repository` impl, not by extending `CrudService`.

`docs/adrs/0013` narrows that guidance the same way template-fastapi's
own `docs/adrs/0008` narrows its Python equivalent: filter/sort/bulk
support that's *mechanically derivable* from a resource's own DTO (a
numeric field always gets `eq`/`min`/`max`/`in`, a string field always
gets `eq`/`contains`/`icontains`, and so on) belongs here instead,
since a per-resource implementation would just mean re-deriving the
same logic in every controller. `CrudService::list` takes
`filters`/`sort` (`repositories::filtering::FilterClause`/
`SortClause`), and `count`/`update_many`/`delete_many` are new —
`update_many`/`delete_many` inject an `owner_id` equality clause before
delegating to the repository, the same ownership scoping `update`/
`delete` already apply (ADR 0007/0011).

`DEFAULT_LIMIT`/`MAX_LIMIT` are the only other resource-agnostic policy
this module owns (a list request's default/maximum page size);
everything else is a pass-through to `R`'s methods.
