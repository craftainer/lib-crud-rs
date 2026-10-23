# 0003. Match lib-crud-py's query behaviour and harden the event stream before release

## Status

Accepted

## Context

A review of the extracted crate (work item AX1, 2026-10-22) found places where
it behaved differently from `lib-crud-py`, which is the source of truth for
the server contract the TypeScript and Python SDKs are written against, and
places where the event stream was unsafe to expose. The crate had not been
released, so public API could still change. The rule applied to every item:
match `lib-crud-py`'s behaviour, and where the transport differs on purpose
(MQTT behind SSE here, MQTT behind SSE there) keep its semantics:
server-minted subscriber ids, a resume parameter, per-resource scoping.

Two items of the work order described Python behaviour that Python does not
have (`?id=` with filters, and the forecast's x-axis). They are recorded under
Decision so the next reader knows which way each was resolved and why.

## Decision

**Event stream.**

- Subscriber ids are minted by the server and verified, never chosen by the
  client: 128 random bits plus a 128-bit HMAC-SHA256 tag over the resource and
  those bits, keyed by a per-bus secret (`EventBus::with_subscriber_secret`, at least 32 bytes, to
  share it across restarts and instances). `EventBus::subscribe` resumes an id
  only if its tag verifies for that resource, otherwise it mints a new one.
  This closes session takeover through a guessed or low-entropy id; Python
  accepts any client string. We chose a MAC over a server-side registry of
  issued ids so that the bus holds no per-subscriber state and ids survive a
  restart when the host supplies the secret.
- Ids are scoped per resource: the tag covers the resource, and the MQTT client
  id is `crud-events-<resource>-<id>` (as in Python).
- Connections that need a freshly minted id ("anonymous") are capped
  (`with_max_anonymous_connections`, default 256 open at once) and fail with
  `EventError::TooManyAnonymousConnections`, rendered as `429`. The cap bounds
  open streams (connections and tasks); it does not bound the persistent
  sessions they leave on the broker, which the broker's own
  `persistent_client_expiration` does, and it is one budget for the bus, so
  per-client limits stay with the host's rate limiter. Resuming with an issued
  id is not counted.
- `subscribe` waits for the broker's CONNACK and SUBACK. A broker that is down
  or refuses the subscription is a `503` at subscribe time rather than an
  open stream that never delivers. Events replayed by a resumed session before
  the SUBACK are kept.
- The broker's error text is shown in the `503` body only when
  `configure_detail_redaction(false)`; it is always logged.
- A reconnect (CONNACK) no longer resets the consecutive-error count; the count
  resets only after 30 s without an error, so a flapping broker ends the
  stream and the client reconnects with its id.
- The "no missed events" claim is corrected: the guarantee covers events that
  reached the broker. `publish` is fire-and-forget into a bounded in-process
  queue, and drops (logged) when the broker has been unreachable long enough
  to fill it or the process stops with the queue non-empty.

**Queries.**

- `?id=` together with filters is a `422` on a read, update and delete. The work
  order asked for this "as Python does", but Python does not refuse it: it
  returns before it parses filters, so the filters are ignored and
  `DELETE ?id=5&status=draft` deletes record 5 whatever its status. A guard the
  caller wrote that silently does nothing is the more dangerous behaviour for a
  mutation, and the SDKs send an id or filters, never both, so refusing costs
  no compatible client anything. This is a deliberate difference from Python.

- `parse_filters` and `parse_sort` take `&[(K, V)]` pairs in wire order instead
  of a `HashMap`. Clauses and errors then come out in request order, which is
  what Python does (`QueryParams.multi_items()`), a repeated key is a repeated
  clause, and the last `sort` wins (`QueryParams.get`). A hash map made
  clause order, error order and which duplicate won depend on hashing.
- `FieldKind::Float`, `FieldSpec::float` and `FilterValue::Float`: Python casts
  with the field's own `int`/`float` type, so a decimal is valid for a float
  field and a 422 for an integer one. Unlike Python, `nan` and `inf` are a 422:
  a NaN bound into SQL sorts above every number and would turn a range filter
  or a bulk target into the wrong set. `Number` keeps meaning integer. Float
  fields take part in stats and predict like integers.
- `RESERVED_PARAMS` is public and complete per the manifest:
  `skip`, `limit`, `sort`, `id`, `include_archived`, `include_unpublished`.
- Categorical counts come back fields-in-order, values in first-seen order
  (Python's in-memory repository), instead of hash order.

**Documented, not changed** (the forecast item has since been changed).

- Forecast regresses on the position of each point in the series, as Python
  did at the time, not on bucket time. The work order said Python regresses on
  bucket time; it did not (`xs = range(len(series))`). Changing it in Rust
  alone would have made the same data forecast differently per server, so it
  was left for a change to both. **Superseded** by work item MF4: both
  servers now regress on bucket time (see
  [ADR 0004](0004-crud-manifest.md), "Forecast"), and the position-based
  behaviour is gone from this crate.
- The bulk cap is check-then-act (`count`, then `update_many`/`delete_many`).
  `Repository` has no "write at most N" operation, so the crate cannot make it
  atomic; Python has the same shape. Documented on `crud_actions` and
  `Repository`.
- The `owner_id` filter clause that `CrudService` appends to bulk actions is now
  a documented contract of `Repository`.

**Mechanical.** `BulkUpdateResult` and `BulkDeleteResult` are aliases of one
`BulkResult`, and the no-target and cap checks of bulk update and delete are
one function. The names and the JSON shape are unchanged.

## Consequences

Public API changes (the crate is unreleased, so no deprecation path):

- `EventBus::subscribe(resource, requested: Option<&str>)` returns
  `(String, EventStream)`; it no longer takes a caller-chosen id.
- `events::sanitize_subscriber_id` and `events::new_subscriber_id` are removed.
  `EventBus::mint_subscriber_id` and `EventBus::verify_subscriber_id` replace
  them. `events::client_id_for` takes the resource too.
- New: `EventBus::{with_subscriber_secret, with_max_anonymous_connections}`,
  `events::DEFAULT_MAX_ANONYMOUS_CONNECTIONS`, `EventError::TooManyAnonymousConnections`
  (`EventError` is now `#[non_exhaustive]`), `CrudError::TooManyRequests` (429),
  `error::redacted_detail`.
- `EventStream` is a struct, not a tuple struct (it was never constructible
  outside the crate).
- `crud_events::resolve_subscriber_id(bus, resource, requested, headers)` returns
  `Option<String>`.
- `crud_query::parse_filters` and `parse_sort` take `&[(K, V)]`. A host extracts
  `Query<Vec<(String, String)>>` instead of `Query<HashMap<..>>`;
  `RESERVED_PARAMS` is public; `FieldKind::Float`, `FieldSpec::float` and
  `FilterValue::Float` are new, and a host's `match` on `FilterValue` needs a
  `Float` arm.
- `views::bulk::BulkResult` is new; the two old names are type aliases.
- New dependencies `hmac` and `sha2` (RustCrypto), already in the lock file
  through `sqlx`.

A client that chose its own `subscriber_id` before now gets a new one and no
replay; ids minted under a random secret do not survive a restart.
A host behind a load balancer or that wants resume across restarts must pass
the same secret to every instance.
