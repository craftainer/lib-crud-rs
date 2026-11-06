# lib-crud-rs

> [!NOTE]
> This repository is vibe coded with [Claude](https://claude.com/product/claude-code).

`craftainer-crud` (imported as `craftainer_crud`): the generic async CRUD
layer for [Axum](https://github.com/tokio-rs/axum) services, extracted from
[`template-axum`](https://github.com/craftainer/template-axum). It is an
instance of
[`template-rust-crate`](https://github.com/craftainer/template-rust-crate)
(so it gets the devcontainer, CI, release workflow and template sync from
there); see [`docs/TEMPLATE.md`](docs/TEMPLATE.md) for the conventions shared
by every instance and
[`docs/adrs/0002-extract-the-generic-crud-layer.md`](docs/adrs/0002-extract-the-generic-crud-layer.md)
for why the crate exists and where its boundary is.

The crate is the resource-agnostic half of a CRUD service. A host application
supplies one `Repository` implementation per resource (any storage) and the
crate supplies the rest:

- `crud::CrudService`, a thin service over any repository;
- `repositories::filtering` and `controllers::crud_query`, the filter and
  sort vocabulary and its query-string parser;
- `controllers::crud_actions`, list-or-get, update and delete with bulk
  actions and a size cap;
- `controllers::crud_stats` and `views::stats`, building blocks for stats and
  prediction endpoints;
- `conditional`, strong and weak `ETag`s for records and pages, `304` on
  `If-None-Match` and `412` on `If-Match` (see below);
- `stats_cache`, an opt-in cache for stats and prediction responses:
  `StatsCache` (async, fallible; `MemoryStatsCache` with a TTL and an entry
  cap ships), the
  `CachedStats::get_or_build` wrapper and `cache_key` (see
  [ADR 0010](docs/adrs/0010-stats-cache.md));
- `events` and `controllers::crud_events`, a publish/subscribe bus and its
  Server-Sent Events stream;
- `manifest`, the CRUD manifest (contract I1): register a
  `ResourceDescriptor` per resource, mount `manifest_router` (caller-filtered,
  weak `ETag`) and `api_catalog_router` (`/.well-known/api-catalog`);
- `error::CrudError`, the error every one of them returns, rendered as
  `application/problem+json`.

A host keeps its own error type and converts with `From<CrudError>`.

The query and bulk behaviour follows
[`lib-crud-py`](https://github.com/craftainer/lib-crud-py), the server source
of truth for the SDKs; [ADR 0003](docs/adrs/0003-match-lib-crud-py-and-harden-events.md)
lists where and why. Things a host needs to know:

- Extract query params as `Query<Vec<(String, String)>>` and pass the slice to
  `parse_filters`/`parse_sort`: clauses and errors keep the request's order.
- `?id=` together with filters is a `422` (Python silently ignores the
  filters; see the ADR).
- The bulk cap is a check followed by a write, not one atomic step.
- Archived rows are the host's to gate. Reads are open, so `include_archived`
  and `only_archived` return every owner's soft-deleted rows to any caller who
  passes them: role-check (or ignore) the flags before calling
  `parse_archived_scope`/`parse_only_archived`, and do not offer them to a
  reader who should not see rows an owner deleted.
- `only_archived=true` (a reserved key, see
  [ADR 0006](docs/adrs/0006-only-archived-list-param.md)) narrows a list to the
  archived rows. Call `parse_archived_scope` on the raw query for list reads and `reject_only_archived` for writes, restore and
  `/stats`; a `Repository` implements `ListOptions::only_archived` and the
  `only_archived` argument of `count`.
- Event streams use server-minted, per-resource subscriber ids and answer
  `503` when the broker is down. The route (`event_stream`) takes a `caller`
  (pass the owner scope's value) and holds at most 5 streams per caller
  (`with_max_streams_per_caller`) and 2 per subscriber id
  (`with_max_streams_per_subscriber`), `429` beyond, counted per process and
  released when the stream is dropped; the streams that start without an id
  are still capped bus-wide. Streams are **transient** (clean session, no
  replay, `subscriber_id`/`Last-Event-ID` ignored) unless the host builds the
  bus `with_durable_streams()`; then pass `EventBus::with_subscriber_secret`
  the same secret on every instance (and across restarts) if clients should
  resume, and advertise replay in the manifest only in that case. Delivery
  covers events that reached the broker; a publish can still be dropped before
  that (see `events`). See
  [ADR 0012](docs/adrs/0012-hardened-mqtt-and-event-streams.md).
- The MQTT connection is secured with `with_mqtt_credentials(user, password)`
  and, with the `mqtt-tls` feature, `with_mqtt_tls(MqttTls::new(ca_file)
  [.with_client_auth(cert, key)])`: TLS 1.2 or later, the broker verified
  against the CA bundle and its host name (always), the files re-read for
  each connection. Credentials without TLS are refused unless
  `with_mqtt_allow_insecure()` (a local broker); call `EventBus::validate()`
  at startup to fail fast. Secrets are redacted from `Debug`. The default port
  stays 1883: pass 8883 with TLS.
- Events are published after the commit, best effort, over one shared
  connection: record them in a `PendingEvents` while the transaction runs
  (mirroring SQL savepoints with `savepoint`/`rollback_to`/`release`), and
  call `commit(&bus)` after the database commit; dropping it, or
  `rollback()`, publishes nothing. A broker failure is logged and counted
  (`delivery_stats()`), never a `500`; a publish that times out or is
  cancelled discards the shared connection. Call `EventBus::shutdown()` when
  the host stops so in-flight events drain.
- `EventBus::subscribe_transient` connects with a clean session, so the broker
  drops the session when the connection ends (cleanly or by crash) and a
  restarting in-process consumer (a cache invalidator) orphans nothing; it is
  never replayed to. `subscribe` stays durable. Use `None` for the id: a
  clean-session connect under an id a durable subscriber uses discards that
  subscriber's session.
- Durable subscribers connect over MQTT 5.0 with Clean Start off and a Session
  Expiry Interval, one hour by default (`with_session_ttl`, 1 to 4294967295
  seconds; `without_session_ttl` returns to MQTT 3.1.1 and leaves expiry to the
  broker, for a broker that does not speak 5.0). A browser SSE client that never
  returns has its session and queued events dropped by the broker after the
  TTL; within it a returning id is replayed. The broker may cap the value
  (Mosquitto `max_session_expiry_interval`); a broker-side expiry
  (`persistent_client_expiration 1h`) stays advisable as a backstop. See
  [ADR 0007](docs/adrs/0007-durable-session-ttl.md) and ADR 0012.
- The forecast regresses on bucket time (a gap in the series counts), the
  same formula as `lib-crud-py`.
- The manifest is built from what the host registers; see
  [ADR 0004](docs/adrs/0004-crud-manifest.md) for where it can differ from
  `lib-crud-py` (no `regex` operator, no enum or date field kinds, bulk
  event names).
- Conditional requests are building blocks, not routes (the crate has no CRUD
  routers): a handler computes `record_etag`/`list_etag` from `Versioned`
  records, answers `not_modified(headers, etag)` when it returns `Some`, and
  otherwise attaches `revalidation_headers(etag)`; a single-record `PATCH` or
  `DELETE` calls `check_if_match` first. The tags are `lib-crud-py`'s, so the
  SDKs behave the same. Nothing is applied automatically, so
  `Features::conditional_requests` in the manifest stays the host's switch: set
  it once the handlers do this. See
  [ADR 0008](docs/adrs/0008-conditional-requests.md).
- A repository must honour the `owner_id` equality clause that bulk actions
  append (see `Repository`).

## Features

- `mqtt` (off by default): `EventBus::connect`, an MQTT-backed bus with
  persistent subscriber sessions. Without it only the in-memory bus
  (`EventBus::mock`) exists.
- `mqtt-tls` (off by default; implies `mqtt`): `EventBus::with_mqtt_tls` and
  `MqttTls`, TLS over rumqttc's `use-rustls` (rustls 0.23 with the aws-lc-rs
  provider, so the build needs a C toolchain and cmake).

## Development

Open the repository in the devcontainer: it starts Postgres and Mosquitto,
which the integration tests need (`tests/README.md`). Then run
`prek run --all-files --hook-stage manual`; that runs `rustfmt`,
`clippy -D warnings`, the tests with a 100% line-coverage gate, `cargo-deny`
and the template-sync manifest check.

## Publishing to crates.io

`make publish-packages` is a no-op (with a `::warning::` in the log) until you opt in, per repository, by setting the
repository variable `CARGO_PUBLISH` to any non-empty value (Settings,
Secrets and variables, Actions, Variables). The version lives only in the
git tag `release.yml` computes; `Cargo.toml` stays at `0.0.0` and
`make build` stamps `RELEASE_VERSION` into the build without committing it.

crates.io has no "pending publisher": a trusted publisher can only be added
to a crate that already exists. So the first release of each crate uses a
short-lived bootstrap token, and trusted publishing takes over after it:

1. Create a crates.io API token scoped to the new crate name
   (`publish-new` only for the first release) with a short expiry, and store
   it as the repository secret `CARGO_REGISTRY_TOKEN`.
2. Set `CARGO_PUBLISH=1` and run the release workflow. `make publish-packages` sees
   the token and publishes with it.
3. On crates.io, open the crate's Settings, Trusted Publishing, and add a
   GitHub publisher: the owner, the repository, the workflow `release.yml`,
   and the environment `release`. The environment field is the binding
   control: it limits trusted publishing to jobs that declare
   `environment: release`, which template-base's `release.yml` does for its
   privileged jobs (with a deployment-branch rule allowing only the default
   branch).
4. Delete the `CARGO_REGISTRY_TOKEN` secret and revoke the token. A present
   token is preferred over OIDC (`make publish-packages` publishes with it and
   never tries trusted publishing), so the token must be gone for OIDC to
   engage; if it is an organization secret, restrict its repository access so
   this repository no longer sees it. From the next release,
   `make publish-packages` finds no token and exchanges the job's GitHub OIDC
   identity for a 30-minute crates.io token, publishes with it and revokes
   it.

## License

Licensed under the [GNU General Public License v3.0](LICENSE).
