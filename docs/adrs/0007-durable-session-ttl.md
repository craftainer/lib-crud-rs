# 0007. Session TTL for durable event subscribers

## Status

Accepted. Completes the follow-up that [0005](0005-transient-event-subscribers.md) left
open, and mirrors `lib-crud-py` ADR 0005.

## Context

Browser SSE clients that drop their subscriber id leave a durable (persistent) MQTT
session behind. Under MQTT 3.1.1 only the broker's own non-standard expiry bounds it
(Mosquitto `persistent_client_expiration`, off by default). MQTT 5.0 lets the client set
the bound itself: a Session Expiry Interval in CONNECT (section 3.1.2.11.2) together with
Clean Start 0 (section 3.1.2.4).

## Decision

Add `EventBus::with_session_ttl(self, ttl: Duration) -> Self` (builder, like
`with_subscriber_secret`), whole seconds from 1 to `u32::MAX`; anything else panics, as
`with_subscriber_secret` does for a short secret. There is no config struct in this crate,
so the builder is the whole surface (Python also has `MQTTConfig.session_ttl_seconds`).

- Unset (default): unchanged. MQTT 3.1.1, `set_clean_session(false)`.
- Set, `subscribe` (durable): `rumqttc::v5` client, `set_clean_start(false)` and
  `set_session_expiry_interval(Some(ttl))`; same client id, topic and `QoS` 1. A client
  that comes back within the TTL resumes and is replayed; later it gets a fresh session.
  The stream's resubscribe-on-lost-session logic applies unchanged.
- `subscribe_transient`: stays on 3.1.1 with a clean session, whatever the TTL.
- The publisher stays on 3.1.1. Verified against Mosquitto 2.0.22
  (`tests/mqtt_events.rs`): its `QoS` 1 publishes reach a v5 subscriber.
- The mock bus ignores the setting.
- Internally the stream polls through a small version-neutral `Polled` type, so one
  `EventStream` serves both protocol versions.

## Consequences

- Additive: a new method; nothing changes unless it is called. A minor release.
- Needs a broker with MQTT 5.0 support; Mosquitto may cap the interval with
  `max_session_expiry_interval`.
- Removing the TTL later makes the same id resume over 3.1.1. What the broker does with
  the expiry of a session created over v5 is broker-specific; keep the broker-side
  backstop from the README.
- Rust-side difference from Python: the TTL is a `Duration` (sub-second part dropped)
  and out-of-range values panic instead of raising `ValueError`.
