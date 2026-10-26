# 0008. Conditional requests: entity-tags, If-None-Match and If-Match

## Status

Accepted. Follows [0004](0004-crud-manifest.md), which left `conditional_requests`
off "until this crate implements them", and mirrors `lib-crud-py`'s `crud_router.py`.

## Context

`lib-crud-py` gives every `ETag`-bearing `GET` a `Cache-Control: private, no-cache`,
answers a matching `If-None-Match` with a bodiless `304`, and makes a single-record
`PATCH`/`DELETE` honour `If-Match` (`412` problem details on a mismatch). The SDKs cache
the `ETag` of what they read and send it back as `If-Match`, so a Rust server has to
produce the same tags and the same status codes. This crate has no CRUD routers (a host
composes `controllers::*` in its own), so the feature is a set of helpers, not routes.

## Decision

A new module `conditional` (no new dependency):

- `trait Versioned: HasId { fn updated_at(&self) -> NaiveDateTime }`. `NaiveDateTime` is
  the timestamp type the rest of the crate uses (filters, stats, events).
- `record_etag(&T) -> String`: strong, `"<hex>"`; `list_etag(&[T]) -> String`: weak,
  `W/"<hex>"`. `<hex>` is the first 32 hex digits of SHA-256 over `<id>:<updated_at>`
  (a list: those strings joined with `|`, in order). `updated_at` is written as Python's
  `datetime.isoformat()` writes a naive datetime (`2026-01-02T03:04:05`, plus `.ffffff`
  only when the microseconds are not zero), so both servers tag the same row alike.
  Python falls back to hashing the serialized view when a view has no `updated_at`;
  `Versioned` requires one, so there is no fallback.
- `not_modified(&HeaderMap, &str) -> Option<Response>`: `If-None-Match`, weak comparison
  (RFC 9110 8.8.3.2: `W/` ignored on both sides), `*`, comma-separated lists and repeated
  header lines. `Some` is a bodiless `304` with the same `ETag` and `Cache-Control`.
- `check_if_match(&HeaderMap, current_etag, resource) -> Result<(), CrudError>`:
  `If-Match`, strong comparison (a `W/` tag never matches, nor does any tag against a weak
  current tag; `*` matches any existing record). No header passes. A failure is
  `CrudError::PreconditionFailed("<resource> was modified since the supplied If-Match")`,
  Python's message. The caller reads the record first and calls this only if it exists
  (Python falls through to the write's `404` for a missing record) and only for a
  single-record write (bulk ignores `If-Match`, as in Python).
- `revalidation_headers(etag) -> HeaderMap` and `REVALIDATE_CACHE_CONTROL`
  (`private, no-cache`, moved here from `manifest`, which re-exports it).
- Tag parsing is `lib-crud-py`'s `(?:W/)?"[^"]*"|\*` scan: tags are found anywhere in the
  value and junk between them is skipped, so a malformed `If-Match` is a `412`, not a `400`.
- `CrudError::PreconditionFailed(String)`: HTTP 412, title `Precondition Failed`, rendered
  as `application/problem+json` like the others. `CrudError` is already
  `#[non_exhaustive]`, so the variant is not a breaking change.
- The manifest route now uses the shared digest and `If-None-Match` matcher instead of its
  own copies. Matching quoted tags, `W/`, `*` and lists is as before; the difference is that
  only quoted tags are recognised (an unquoted token no longer matches) and a comma inside
  a quoted tag no longer splits it.

## Consequences

- Additive; a minor release.
- Nothing is applied automatically, so `Features::conditional_requests` keeps defaulting to
  `false` and stays the host's switch: ADR 0004's "a host that sets the feature owns
  making it true" still holds, now with the helpers to make it true. When it is set the
  manifest reports `etag`, strong `record_etag` and weak `list_etag`, which is what these
  functions produce.
- The check and the write are separate statements, so two writers racing inside that gap
  can both pass, as in Python. A host that needs atomicity adds a `WHERE updated_at = ?`
  to its update.
- `not_modified` is for `GET`/`HEAD`. A failed `If-None-Match` on a write is not turned
  into `412` (Python does not either).
- Differences from Python: the typed `Versioned` bound instead of duck-typing, and no
  serialized-view fallback.
