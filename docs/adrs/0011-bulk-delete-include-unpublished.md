# 0011. Bulk delete honours `include_unpublished`

## Status

Accepted

## Context

`include_unpublished` is a reserved query key of the CRUD manifest, but a bulk `DELETE`
ignored it: it reached every live row, including rows outside their publish window (not
yet published, or no longer). `lib-crud-py` changed this in its ADR 0008 (merge commit
a0ce9a4): a bulk delete reads the flag, defaults it to true so existing callers keep
deleting scheduled rows, and `false` spares them. This crate mirrors it. It has no publish
window of its own: the host's `Repository` implementation applies it, as it does for
`archived_at`.

## Decision

`Repository::delete_many` and `Repository::count` take an `include_unpublished: bool`.
`CrudService::delete_many` and `CrudService::count` pass it on, and `resolve_delete`
takes it too, so the size cap counts the rows the delete will reach and the count and the
delete agree. `crud_query` gains `parse_bulk_delete_include_unpublished`, which reads the
key with the shared flag vocabulary (`true`/`1`, `false`/`0`, case-insensitive; anything
else is `invalid filter value`) and defaults to `true` when absent. The `dyn_repository!`
macro forwards both. A delete by id ignores the flag. `update_many` and `resolve_update`
are unchanged and size their cap with `true` (a known gap, same as `lib-crud-py`).

This is the breaking shape, as ADR 0006 was for `only_archived`: no default method hides
the flag, so a host that does not handle it fails to compile rather than silently
deleting rows a caller asked to spare. Additive `*_with` trait methods with defaults were
rejected for that reason.

## Consequences

- A breaking change to the `Repository` trait, `CrudService::count`/`delete_many` and
  `resolve_delete`: a breaking release (0.3.0), with the compiler pointing at each call.
  The semver-checks hook runs with `SEMVER_RELEASE_TYPE=major`.
- A host with a publish window adds `AND <published>` to `count` and `delete_many` when
  the flag is false; a host without one ignores the argument.
- A host passes `parse_bulk_delete_include_unpublished(params)` to `resolve_delete`.
