# 0006. Add an `only_archived` list query parameter

## Status

Accepted

## Context

An archivable resource hides archived rows from a list, and `?include_archived=true`
returns live and archived rows together, so a trash view had to page through every row.
`lib-crud-py` added a reserved `only_archived` key for this (its ADR 0007); the reserved
keys are part of the cross-implementation contract, so this crate mirrors it.

## Decision

`only_archived` is a reserved query key (never a filter, not an OpenAPI parameter) with
the boolean vocabulary `true`/`1`, `false`/`0`, case-insensitive. `true` returns only
rows with `archived_at` set, implies `include_archived`, and combines with filters, sort,
`skip` and `limit`. `ListOptions` gains `only_archived`, `Repository::count` and
`CrudService::list`/`count` gain an `only_archived` argument, and
`resolve_list_or_get` takes an `ArchivedScope` (`Live`, `Include`, `Only`; `from_flags`
builds it) in place of the `include_archived` bool. A repository gives it precedence over `include_archived`.

`crud_query` gains `parse_archived_scope` (the one call for a list read),
`parse_only_archived`, `parse_include_archived` and
`reject_only_archived`, returning `FieldError`s the host wraps in `UnprocessableEntity`:
a malformed value is `invalid filter value`; `true` next to an explicit
`include_archived=false` is `cannot be combined with include_archived=false`; `true` on
`/stats`, a write, restore or a single-record `?id=` read is `applies to list reads only`.
`only_archived=false` is accepted everywhere. The message texts match `lib-crud-py`; the
`field` is the key (`only_archived`, or `include_archived` for its malformed value),
which is this crate's `FieldError` shape for `lib-crud-py`'s `loc ["query", key]`.

## Consequences

- A breaking change to `Repository::count`, `ListOptions`, `CrudService::list`/`count`
  and `resolve_list_or_get`: a breaking release for the crate, with the compiler pointing at each call.
- A model field named `only_archived` can no longer be filtered on.
- The write resolvers take no flag, so a host must call `reject_only_archived` on its
  write, restore and stats routes.
