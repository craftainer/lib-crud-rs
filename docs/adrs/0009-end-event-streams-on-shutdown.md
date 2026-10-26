# 0009. End `/events` streams on a shutdown token

## Status

Accepted. User decision of 2026-10-26 (AX11 of the templates/SDKs/UI plan).

## Context

An SSE response body only completes when the bus is dropped or the broker
flaps. A host using `axum::serve(..).with_graceful_shutdown(..)` therefore
waits on every open `/events` client until its own timeout kills the process.

## Decision

`controllers::crud_events::event_stream_until(bus, resource, requested,
headers, shutdown: CancellationToken)` is `event_stream` plus
`take_until(shutdown.cancelled_owned())` on the body. Cancelling the token
completes every open stream; a stream opened after cancellation completes
with an empty body. `event_stream` is unchanged (no token, same behaviour).
New dependency: `tokio-util` (`=0.7.20`, no features; `CancellationToken` is
always available), already in the lock through other crates.

## Consequences

- Additive: no existing signature changes. There is no `EventBus::close()`.
- A host must cancel the same token (or a clone) it passed in; cancelling
  stops event delivery to clients, not publishing.
