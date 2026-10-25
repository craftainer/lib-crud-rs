# controllers/

The resource-agnostic half of the controller layer: plain functions a
resource's own router composes. There is no router and no axum `State` here.

- `crud_actions.rs` — `resolve_list_or_get`, `resolve_update`,
  `resolve_update_by_id`, `resolve_delete`: the `?id=` versus filter
  branching, the bulk size cap and the error mapping, generic over any
  `R: Repository`.
- `crud_query.rs` — query string to `FilterClause`/`SortClause`, driven by a
  resource's own `FieldSpec` table, plus the `include_archived` /
  `only_archived` flag parsers (`parse_archived_scope`, `parse_include_archived`,
  `parse_only_archived`, `reject_only_archived`) a host calls on the raw query.
- `crud_stats.rs` — the resource-agnostic parts of `/stats` and `/predict`:
  `TimeBucket`, field-kind narrowing, the OLS `forecast` (regressed on
  bucket time, as `lib-crud-py`'s).
- `crud_events.rs` — the resource-agnostic parts of `GET <prefix>/events`:
  subscriber-id resolution, the SSE frame shape, `publish`.

## Do

- Keep a new helper generic over `R: Repository` (or the narrowest trait it
  needs) rather than a concrete resource type.
- Return `CrudError`, never an application's own error type.

## Don't

- Reference a concrete resource or an application's state.
