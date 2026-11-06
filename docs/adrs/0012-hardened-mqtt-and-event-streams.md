# 0012. Hardened MQTT connection, after-commit events and bounded event streams

## Status

Accepted. Mirrors `lib-crud-py` ADR 0010. Amends [0003](0003-match-lib-crud-py-and-harden-events.md)
(the per-bus anonymous cap is now only a backstop),
[0005](0005-transient-event-subscribers.md) (the events route subscribes transient unless the
host opts into durable sessions), [0007](0007-durable-session-ttl.md) (the session TTL now
defaults to one hour) and [0010](0010-stats-cache.md) (`MemoryStatsCache` is now bounded).

## Context

An OWASP review of the crate (OW-lib-crud-rs) found four gaps in the event path and one in the
stats cache:

- The MQTT bus had no credentials and no TLS option, and TLS was not even compiled in
  (`rumqttc` with default features off). Every `EventBus::connect(host, port)` was a plaintext,
  anonymous client, so anyone on the path could read every event and forge `update`/`delete`
  events that every SSE client acts on.
- The anonymous-stream cap (default 256) was one budget for the whole bus, and a caller who
  presented any valid `subscriber_id` took no permit, so one caller could open unlimited streams
  under one id. Every stream was durable, and no session expiry was set by default, so each
  connect-disconnect cycle left a persistent broker session behind until the broker (never, on
  Mosquitto's default) expired it.
- The bus is not threaded through `CrudService`, so a host's handler published wherever it chose,
  usually inside the request's transaction: an event could announce a change that then rolled
  back. The publisher connection could not be reached or discarded from outside.
- `MemoryStatsCache` had no entry cap, and every `set` scanned the whole map.
- `include_archived`/`only_archived` give any reader every owner's soft-deleted rows; the crate
  did not say that gating them is the host's job.

## Decision

**TLS and credentials.** `EventBus::with_mqtt_credentials(user, password)` and, behind the new
`mqtt-tls` feature (`rumqttc/use-rustls`), `EventBus::with_mqtt_tls(MqttTls)`. `MqttTls::new(ca_file)`
takes a required PEM CA bundle and, optionally, `with_client_auth(cert, key)` for mutual TLS (a
pair, so "a key without a certificate" cannot be built). TLS 1.2 is rustls's floor and the broker's
host name is verified against its certificate on every connection; unlike the Python
`allow_insecure`, there is no way to switch verification off. The files are read for every
connection, so rotation needs no restart. Credentials without TLS are refused unless
`with_mqtt_allow_insecure()`, which relaxes only that rule. The rule runs when a connection is
opened (so builder order does not matter and a refused configuration fails closed: a subscribe is
`EventError::Config` and a `503`, a publish is logged and counted), and `EventBus::validate()`
runs it at startup. The password is redacted from `Debug` (`EventBus`, and the subscriber secret).
The default port stays 1883; the host passes 8883 with TLS. Trusting the system store needs the
host to point `ca_file` at the system bundle: rumqttc's `Simple` TLS configuration requires a CA.

**Event delivery.** The crate cannot see a host's transaction, so the after-commit rule is a type
the host drives: `PendingEvents` records events while the transaction runs, mirrors SQL
savepoints (`savepoint`/`release`/`rollback_to`), and `commit(&bus)` publishes them in order once the
outermost database commit succeeded. A savepoint release keeps its events, a rollback to a
savepoint drops only the events recorded after it, and dropping the value (or `rollback()`)
publishes nothing, so a transaction that ends without a commit leaves no queue behind. Delivery is
best effort: `EventBus::publish` queues the event on one connection shared by every publish
(opened lazily on the first one, so `connect` no longer needs a runtime), the wait for room in its
request queue is bounded to 10 s, and a failure is logged and counted
(`delivery_stats()`), never returned, so a broker outage is not a `500`. A publish that fails, times
out or is cancelled discards the shared connection (a guard does it on every path, including a
dropped future) and the next publish opens a new one. `EventBus::shutdown()` stops accepting
events, waits for the publishes in flight, then disconnects and gives the event loop a moment to
flush.

**Event streams.** `subscribe_for_caller`, which the `event_stream_for_caller` and
`event_stream_until_for_caller` handlers use, counts
open streams per caller (the owner scope's value the host passes, across resources; default 5) and
per subscriber id (default 2: a reconnect can overlap a connection the server has not noticed is
gone), and answers `EventError::TooManyStreams` (a `429`) beyond either. A slot is a guard owned by
the `EventStream`, so it is released when the stream is dropped, including a failed open and a
dropped response body. The limits are per process; `None` removes one. The per-subscriber limit is
the Rust crate's addition to the Python design, because here a presented id took no permit.
Callers a host cannot tell apart pass one shared key and share one budget. The route's streams are
transient (clean session, no replay, a presented `subscriber_id` ignored) unless the host builds the
bus `with_durable_streams()`. `connect` now defaults durable subscribers to a one-hour session over
MQTT 5.0 (`DEFAULT_SESSION_TTL`); `without_session_ttl()` returns to MQTT 3.1.1.

**Stats cache.** `MemoryStatsCache` keeps at most `DEFAULT_MAX_ENTRIES` (10 000) entries
(`with_max_entries`); a `set` of a new key at the cap evicts the entry stored longest ago. The scan
runs only at the cap.

**Archived rows.** Documented where the flags are parsed and in the README: reads are open, and
whether a caller may pass `include_archived`/`only_archived` is the host's to gate.

## Consequences

A minor release with behaviour changes a host must know: the route no longer resumes unless the
host calls `with_durable_streams()` (and then its manifest should advertise replay); the existing
`event_stream`/`event_stream_until` keep their signatures but put every caller in one shared
budget (the `_for_caller` pair takes the caller, so the release stays source compatible); durable sessions use MQTT 5.0 by default, so the broker
must speak it (or call `without_session_ttl()`); events can be lost on a broker failure or a crash
between commit and publish (a client that needs completeness refetches on reconnect); and a
host that publishes inside its transaction today should move to `PendingEvents`. The stream limits
are per process, so with N workers the effective cap is N times larger, and callers a host cannot
tell apart compete for one budget. `mqtt-tls` builds rustls with aws-lc-rs, which needs a C
toolchain and cmake, and pulls `rustls-webpki` 0.102 and the unmaintained `rustls-pemfile` through
`rumqttc` 0.25.1 (see the dependency note in the pull request). The dev broker has no TLS
listener or credentials, so those paths are unit-tested rather than run against Mosquitto.
Optimistic concurrency stays a separate item.
