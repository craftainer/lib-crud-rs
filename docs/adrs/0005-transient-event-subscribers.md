# 0005. Transient event subscribers

## Status

Accepted

## Context

Subscriber ids are server-minted and signed (ADR 0003). A long-lived in-process
subscriber, such as an application's cache-invalidation task, gets a new id on every
restart, so its previous persistent MQTT session (`set_clean_session(false)`) is never
resumed and stays on the broker with whatever it queued. `lib-crud-py` has the same gap
and gets the same fix.

## Decision

Add `EventBus::subscribe_transient`, the same as `subscribe` except that the MQTT
connection uses a clean session (MQTT 3.1.1 section 3.1.2.4): the broker discards the
session when the connection ends, cleanly or by crash. `subscribe` stays durable and the
HTTP events route never uses the new method, because SSE clients rely on replay. The mock
bus treats both alike. The README recommends a finite persistent-session expiry on
production brokers (Mosquitto never expires by default; EMQX defaults to 2h) as a
backstop for abandoned durable sessions. An MQTT v5 session expiry interval follows
separately.

## Consequences

- Restarting a transient consumer orphans nothing; it also gets no replay.
- New public method; a minor release.
