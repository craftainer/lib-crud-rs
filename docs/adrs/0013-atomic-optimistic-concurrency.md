# 0013. Atomic optimistic concurrency: `expected_version` on `update` and `delete`

## Status

Proposed. Amends [0008](0008-conditional-requests.md). Mirrors the same change in
`lib-crud-py`. A breaking release (0.x minor).

## Context

ADR 0008 left a gap: `check_if_match` and the write were separate statements, so two
writers holding the same stale `ETag` could both pass the check and both write (OW-lib-crud-rs
L-3; the same shape at template-axum's `heroes.rs:183-185`). `Repository::update` and
`delete` had no "write if the version is X" form, so a host could not close it. User
decision (2026-11-04): make it a breaking repository-contract change, in both CRUD libraries.

The crate's only notion of a record's version is `Versioned::updated_at`; the `ETag` is a
one-way digest of `<id>:<updated_at>`, so it cannot itself be a SQL predicate.

## Decision

- The version is `updated_at` (`chrono::NaiveDateTime`).
- `Repository::update(id, owner_id, expected_version, data)` and
  `Repository::delete(id, owner_id, expected_version)` take `expected_version:
  Option<NaiveDateTime>`. `Some(v)` writes only if the row is still at `v`, atomically with
  the write (SQL: `... AND updated_at = $n` beside the id, owner and archived predicates in
  the one statement). `None` is the explicit unconditional write. There is no default
  method: a host that does not handle the argument fails to compile.
- When a conditional write matches no row, the implementation tells the two causes apart:
  record missing, not owned or archived stays `Ok(None)` / `Ok(false)` (404, so ownership
  is not revealed by a different status); the record exists at another version is the new
  `RepoError::VersionConflict` (now `#[non_exhaustive]`), which converts to the existing
  `CrudError::PreconditionFailed` (412). Telling them apart may use a second read after the
  failed write; it only classifies, it does not guard, and a row changed between the two
  statements can be reported as the other outcome (best effort, never a lost update). A
  concurrent delete during a conditional write is a 404, not a 412, as in `lib-crud-py`.
- An implementation moves `updated_at` on every successful write, soft delete and bulk
  writes included, strictly forward: the host's responsibility, as the crate has no backend.
  For Postgres use `updated_at = GREATEST(now(), updated_at + interval '1 microsecond')`
  (as `lib-crud-py` does and the test fixture's `PgWidgets` now does), so the version
  increases even inside one transaction or when the clock has not moved or stepped back.
- `CrudService::update`/`delete`, `resolve_update`, `resolve_update_by_id` and
  `resolve_delete` pass it through. It applies to the `?id=` path; bulk actions ignore it,
  as they ignored `If-Match`.
- `conditional::expected_version(headers, &record, resource)` turns `If-Match` into the
  argument: the same strong comparison as `check_if_match`; a matching tag gives
  `Some(record.updated_at())`; an absent header or `*` gives `None`; anything else is the
  same 412 as before. It takes the resource name (as `check_if_match` does) so the 412
  `detail` text is unchanged when the stale tag is caught up front. A conflict only found
  at write time (`RepoError::VersionConflict`) becomes a 412 with the generic detail
  "record was modified since the supplied If-Match", because the conversion has no resource
  name: same status and problem type, different `detail`.
- HTTP behaviour is otherwise unchanged: a stale or weak `If-Match` is 412, a missing
  `If-Match` passes (no 428), `*` matches any existing record.
- `dyn_repository!` forwards the new argument and the crate re-exports `chrono` for it.
- The crate still has no routers and no backends, so the host's handlers and repositories
  make the change real; `check_if_match` stays for a host that wants the 0008 behaviour.

## Consequences

- Breaking: every `Repository` impl, every `update`/`delete`/`resolve_*` caller and every
  `dyn_repository!` use changes (the compiler points at each). Released as a 0.x minor.
- A handler still reads the record first (it needs the current tag to compare and the
  `updated_at` to pass), but the write is conditional, so the read-compare-write race is
  gone: of two writers with the same version exactly one succeeds, the other gets 412.
- The version is only as good as `updated_at`: a store with coarse timestamps, or a write
  path that does not bump it, can let two writes share a version (OW L-2). The trait
  documents the requirement.
- Migration, per host: (1) add `expected_version: Option<NaiveDateTime>` to the repository's
  `update` and `delete`, add `AND updated_at = $n` (or the equivalent) to the write, set
  `updated_at` strictly forward on every write, and return `RepoError::VersionConflict` when the row exists
  at another version; (2) in each single-record `PATCH`/`DELETE` handler replace the
  read-`check_if_match`-write sequence with read, `expected_version(&headers, &record,
  "Name")?`, then the write with that value; (3) pass `None` wherever an unconditional write
  was intended, and update `dyn_repository!` users (nothing else changes).
