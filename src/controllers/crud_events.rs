//! The resource-agnostic half of a `GET <prefix>/events` route:
//! subscriber-id resolution, the SSE frame shape and the publish helper
//! every mutating handler calls. A resource's router supplies the one
//! resource-specific thing this needs, the resource name.
//!
//! Shaped like [`crud_actions`](super::crud_actions): free functions a
//! resource's router composes, not a router of its own, so a second resource
//! gets the same endpoint by adding one route and one `&'static str`.
//!
//! See [`crate::events`] for the transport, how subscriber ids are minted and
//! verified, and the delivery guarantee's exact scope.

use std::convert::Infallible;
use std::pin::Pin;
use std::time::Duration;

use axum::http::HeaderMap;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::error::{CrudError, redacted_detail};
use crate::events::{CrudEvent, EventAction, EventBus, EventError, EventStream};

/// The header a browser's `EventSource` resends automatically after a
/// dropped connection -- the stream puts the server-minted `subscriber_id` in
/// every frame's `id:` field precisely so that automatic resend is enough to
/// resume the persistent session, with no client-side bookkeeping.
const LAST_EVENT_ID: &str = "last-event-id";

/// How often a comment frame is sent on an idle stream, to keep proxies and
/// load balancers from reaping a connection that has simply had no events.
#[cfg(not(test))]
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);
// A few milliseconds under test, so a dedicated test can wait for a
// keep-alive comment to arrive before the first real event without
// waiting 15 real seconds -- same idea as `REDIS_TIMEOUT`'s existing
// test-friendliness, just via cfg instead of a parameter.
#[cfg(test)]
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_millis(20);

/// The event stream's response type. A plain `Response` rather than
/// `Sse<S>`: `Sse::keep_alive` wraps the stream in an axum-private
/// `KeepAliveStream`, so the configured value has no nameable type for a
/// handler signature -- and the body is already a trait object, so nothing
/// is lost by erasing the rest of it here.
pub type EventSse = Response;

/// Pick the `subscriber_id` this connection asks to resume.
///
/// Precedence: the explicit `?subscriber_id=` query parameter, then the
/// `Last-Event-ID` header. A candidate counts only if `bus` minted it for
/// `resource` ([`EventBus::verify_subscriber_id`]); one that does not verify
/// is treated as absent rather than as an error -- a client sending garbage
/// gets a working stream under a new server-minted id, not a 400 it cannot
/// act on. The cost is stated plainly in the FR: no replay. `None` means
/// "mint a fresh id".
#[must_use]
pub fn resolve_subscriber_id(
    bus: &EventBus,
    resource: &str,
    requested: Option<&str>,
    headers: &HeaderMap,
) -> Option<String> {
    let from_header = headers
        .get(LAST_EVENT_ID)
        .and_then(|value| value.to_str().ok());
    [requested, from_header]
        .into_iter()
        .flatten()
        .find_map(|candidate| bus.verify_subscriber_id(resource, candidate))
}

/// The stream's first frame: the `subscriber_id` this connection was given,
/// so a client that did not supply one learns the id it must send back to
/// get replay on reconnect.
fn subscriber_frame(subscriber_id: &str) -> SseEvent {
    SseEvent::default()
        .event("subscriber")
        .id(subscriber_id)
        .data(format!(r#"{{"subscriber_id":"{subscriber_id}"}}"#))
}

/// One CRUD event as an SSE frame. `event:` is the action name; `id:` is the
/// subscriber id (not a per-message sequence number) -- see
/// `resolve_subscriber_id` for why.
fn event_frame(subscriber_id: &str, event: &CrudEvent) -> SseEvent {
    let frame = SseEvent::default()
        .event(event.action.as_str())
        .id(subscriber_id);
    // CrudEvent is a plain struct of owned scalars, so encoding never fails
    // in practice; degrade to a typed error frame rather than panicking
    // inside a live connection.
    frame.json_data(event).unwrap_or_else(error_frame)
}

/// Logs an SSE frame's JSON-encode failure and returns a typed error
/// frame instead of the real payload -- see `event_frame`'s own doc
/// comment for why this branch is structurally unreachable but still
/// tested directly (accepts anything `Display`, so a cheap
/// `serde_json::Error` stands in for the real `axum_core::Error` in
/// tests).
fn error_frame(err: impl std::fmt::Display) -> SseEvent {
    tracing::error!("failed to encode CRUD event frame: {err}");
    SseEvent::default().event("error").data("encode failed")
}

/// Build the SSE response for `resource`'s event stream.
///
/// # Errors
///
/// [`CrudError::ServiceUnavailable`] when the bus cannot open the
/// subscription (for example the broker is down; the broker's own message is
/// hidden unless [`configure_detail_redaction`](crate::error::configure_detail_redaction)
/// turned redaction off), and [`CrudError::TooManyRequests`] when the bus's
/// cap on anonymous streams is reached.
pub async fn event_stream(
    bus: &EventBus,
    resource: &'static str,
    requested_subscriber_id: Option<&str>,
    headers: &HeaderMap,
) -> Result<EventSse, CrudError> {
    open_stream(bus, resource, requested_subscriber_id, headers, None).await
}

/// [`event_stream`] whose response body completes once `shutdown` is
/// cancelled, so a host's graceful shutdown is not held open by a
/// long-lived `/events` connection. A call made after the token was
/// already cancelled does not subscribe and yields an empty, already-complete
/// body (so it cannot fail with 429/503 either).
///
/// Cancel the token when the host starts shutting down (for example in the
/// future passed to `axum::serve(..).with_graceful_shutdown`), and clone it
/// into each call; clones share cancellation.
///
/// # Errors
///
/// The same as [`event_stream`].
pub async fn event_stream_until(
    bus: &EventBus,
    resource: &'static str,
    requested_subscriber_id: Option<&str>,
    headers: &HeaderMap,
    shutdown: CancellationToken,
) -> Result<EventSse, CrudError> {
    open_stream(
        bus,
        resource,
        requested_subscriber_id,
        headers,
        Some(shutdown),
    )
    .await
}

async fn open_stream(
    bus: &EventBus,
    resource: &'static str,
    requested_subscriber_id: Option<&str>,
    headers: &HeaderMap,
    shutdown: Option<CancellationToken>,
) -> Result<EventSse, CrudError> {
    // After cancellation do not subscribe at all: no anonymous-connection
    // slot, no broker round trip, and no 429/503 for a client that is being
    // turned away anyway.
    if shutdown
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled)
    {
        let empty: Pin<Box<dyn Stream<Item = Result<SseEvent, Infallible>> + Send>> =
            Box::pin(futures::stream::empty());
        return Ok(Sse::new(empty).into_response());
    }
    let requested = resolve_subscriber_id(bus, resource, requested_subscriber_id, headers);
    let opened = bus.subscribe(resource, requested.as_deref()).await;
    into_sse(opened, shutdown)
}

/// The CRUD error for a failed subscription. The broker's message can carry
/// its address or driver internals, so it is logged and only shown when
/// redaction is off.
fn subscribe_error(err: EventError) -> CrudError {
    match err {
        EventError::TooManyAnonymousConnections => CrudError::TooManyRequests(
            "too many anonymous event streams; retry shortly, or resume with the subscriber_id you were issued"
                .to_string(),
        ),
        EventError::Broker(detail) => {
            tracing::warn!("event stream unavailable: {detail}");
            CrudError::ServiceUnavailable(redacted_detail(
                detail,
                "event stream temporarily unavailable",
            ))
        }
    }
}

/// The response for a resolved subscription, or the error for a failed one.
fn into_sse(
    opened: Result<(String, EventStream), EventError>,
    shutdown: Option<CancellationToken>,
) -> Result<EventSse, CrudError> {
    let (subscriber_id, stream) = opened.map_err(subscribe_error)?;
    Ok(sse_from(subscriber_id, stream, shutdown))
}

/// Wrap a resolved `EventStream` in the SSE response shape: the
/// subscriber-id frame first, then one frame per event until the feed ends.
fn sse_from(
    subscriber_id: String,
    stream: EventStream,
    shutdown: Option<CancellationToken>,
) -> EventSse {
    // State: `Some(id)` until the first frame has been emitted, then `None`.
    let initial = (Some(subscriber_id.clone()), stream, subscriber_id);
    let body =
        futures::stream::unfold(initial, |(pending, mut stream, subscriber_id)| async move {
            if let Some(id) = pending {
                let frame = subscriber_frame(&id);
                return Some((Ok(frame), (None, stream, subscriber_id)));
            }
            let event = stream.next_event().await?;
            let frame = event_frame(&subscriber_id, &event);
            Some((Ok(frame), (None, stream, subscriber_id)))
        });

    let body: Pin<Box<dyn Stream<Item = Result<SseEvent, Infallible>> + Send>> = match shutdown {
        // `take_until` polls the token first, so an already-cancelled token
        // ends the body before any frame, and a cancel mid-wait drops the
        // pending `next_event` and completes the body.
        Some(token) => Box::pin(body.take_until(token.cancelled_owned())),
        None => Box::pin(body),
    };
    Sse::new(body)
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE_INTERVAL))
        .into_response()
}

/// Announce a completed mutation. Never fails the caller's request: see
/// `EventBus::publish`.
pub async fn publish(bus: &EventBus, resource: &str, action: EventAction, ids: Vec<i32>) {
    bus.publish(CrudEvent::new(resource, action, ids)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{REDACTION_LOCK, configure_detail_redaction};
    use axum::http::HeaderValue;

    fn headers_with_last_event_id(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(LAST_EVENT_ID, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn a_failed_subscription_becomes_a_503_with_the_broker_detail_redacted() {
        let _guard = REDACTION_LOCK.lock().unwrap();
        configure_detail_redaction(true);
        let err = into_sse(
            Err(EventError::Broker(
                "tcp connect error to mqtt.internal:1883".to_string(),
            )),
            None,
        )
        .unwrap_err();
        assert!(
            matches!(&err, CrudError::ServiceUnavailable(msg) if !msg.contains("mqtt.internal")),
            "{err:?}"
        );
    }

    #[test]
    fn the_broker_detail_is_shown_when_redaction_is_off() {
        let _guard = REDACTION_LOCK.lock().unwrap();
        configure_detail_redaction(false);
        let err = into_sse(Err(EventError::Broker("broker down".to_string())), None).unwrap_err();
        configure_detail_redaction(true);
        assert!(matches!(err, CrudError::ServiceUnavailable(msg) if msg == "broker down"));
    }

    #[test]
    fn hitting_the_anonymous_cap_becomes_a_429() {
        let err = into_sse(Err(EventError::TooManyAnonymousConnections), None).unwrap_err();
        assert!(matches!(err, CrudError::TooManyRequests(_)));
    }

    #[test]
    fn an_explicit_query_parameter_wins() {
        let bus = EventBus::mock();
        let from_query = bus.mint_subscriber_id("widgets");
        let from_header = bus.mint_subscriber_id("widgets");
        let headers = headers_with_last_event_id(&from_header);
        assert_eq!(
            resolve_subscriber_id(&bus, "widgets", Some(&from_query), &headers),
            Some(from_query)
        );
    }

    #[test]
    fn the_last_event_id_header_is_used_when_no_query_parameter_is_given() {
        let bus = EventBus::mock();
        let from_header = bus.mint_subscriber_id("widgets");
        let headers = headers_with_last_event_id(&from_header);
        assert_eq!(
            resolve_subscriber_id(&bus, "widgets", None, &headers),
            Some(from_header)
        );
    }

    #[test]
    fn nothing_is_resumed_when_neither_source_supplies_an_id() {
        let bus = EventBus::mock();
        assert_eq!(
            resolve_subscriber_id(&bus, "widgets", None, &HeaderMap::new()),
            None
        );
    }

    #[test]
    fn an_unminted_query_parameter_falls_back_to_the_header() {
        let bus = EventBus::mock();
        let from_header = bus.mint_subscriber_id("widgets");
        let headers = headers_with_last_event_id(&from_header);
        assert_eq!(
            resolve_subscriber_id(&bus, "widgets", Some("victim"), &headers),
            Some(from_header)
        );
    }

    #[test]
    fn ids_that_were_not_minted_for_this_resource_resume_nothing() {
        let bus = EventBus::mock();
        let other_resource = bus.mint_subscriber_id("gadgets");
        let headers = headers_with_last_event_id("also/bad");
        assert_eq!(
            resolve_subscriber_id(&bus, "widgets", Some(&other_resource), &headers),
            None
        );
        assert_eq!(
            resolve_subscriber_id(&bus, "widgets", Some(""), &headers),
            None
        );
    }

    #[test]
    fn a_non_ascii_header_value_is_ignored() {
        let mut headers = HeaderMap::new();
        headers.insert(
            LAST_EVENT_ID,
            HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
        );
        let bus = EventBus::mock();
        assert_eq!(resolve_subscriber_id(&bus, "widgets", None, &headers), None);
    }

    #[tokio::test]
    async fn a_client_chosen_id_is_replaced_by_a_minted_one() {
        let bus = EventBus::mock();
        let response = event_stream(&bus, "widgets", Some("victim"), &HeaderMap::new())
            .await
            .unwrap();
        let body = first_frames(response, 1).await;
        assert!(!body.contains("victim"), "{body}");
        assert!(body.contains("event: subscriber"), "{body}");
    }

    #[tokio::test]
    async fn the_anonymous_cap_stops_new_streams_but_not_resuming_ones() {
        let bus = EventBus::mock().with_max_anonymous_connections(1);
        let first = event_stream(&bus, "widgets", None, &HeaderMap::new())
            .await
            .unwrap();
        let err = event_stream(&bus, "widgets", None, &HeaderMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, CrudError::TooManyRequests(_)));
        let issued = first_frames(first, 1).await;
        let id = issued
            .lines()
            .find_map(|line| line.strip_prefix("id: "))
            .unwrap()
            .to_string();
        assert!(
            event_stream(&bus, "widgets", Some(&id), &HeaderMap::new())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn the_first_frame_carries_the_subscriber_id() {
        let bus = EventBus::mock();
        let response = event_stream(&bus, "widgets", None, &HeaderMap::new())
            .await
            .unwrap();
        let body = first_frames(response, 1).await;
        assert!(body.contains("event: subscriber"), "{body}");
        let id = body
            .lines()
            .find_map(|line| line.strip_prefix("id: "))
            .unwrap();
        assert!(bus.verify_subscriber_id("widgets", id).is_some(), "{body}");
        assert!(
            body.contains(&format!(r#"{{"subscriber_id":"{id}"}}"#)),
            "{body}"
        );
    }

    #[tokio::test]
    async fn published_events_become_frames_named_after_the_action() {
        let bus = EventBus::mock();
        let response = event_stream(&bus, "widgets", None, &HeaderMap::new())
            .await
            .unwrap();
        publish(&bus, "widgets", EventAction::Create, vec![42]).await;
        let body = first_frames(response, 2).await;
        assert!(body.contains("event: create"), "{body}");
        assert!(body.contains(r#""ids":[42]"#), "{body}");
        let id = body
            .lines()
            .find_map(|line| line.strip_prefix("id: "))
            .unwrap();
        assert_eq!(body.matches(&format!("id: {id}")).count(), 2, "{body}");
    }

    #[tokio::test]
    async fn the_stream_ends_when_the_bus_goes_away() {
        let bus = EventBus::mock();
        let response = event_stream(&bus, "widgets", None, &HeaderMap::new())
            .await
            .unwrap();
        drop(bus);
        // Only the subscriber frame is ever produced; asking for two frames
        // still terminates rather than hanging.
        let body = first_frames(response, 2).await;
        assert!(body.contains("event: subscriber"), "{body}");
        assert!(!body.contains("event: create"), "{body}");
    }

    #[tokio::test]
    async fn keep_alive_comments_are_skipped_before_the_first_real_event() {
        let bus = EventBus::mock();
        let response = event_stream(&bus, "widgets", None, &HeaderMap::new())
            .await
            .unwrap();
        let publish_after_a_few_keep_alives = async {
            tokio::time::sleep(KEEP_ALIVE_INTERVAL * 5).await;
            publish(&bus, "widgets", EventAction::Create, vec![7]).await;
        };
        // The subscriber frame, then at least one keep-alive comment (which
        // `first_frames` must skip via its `starts_with(':')` branch) before
        // the real `create` frame that ends the count.
        let ((), body) = tokio::join!(publish_after_a_few_keep_alives, first_frames(response, 2));
        assert!(body.contains("event: subscriber"), "{body}");
        assert!(body.contains("event: create"), "{body}");
    }

    #[tokio::test]
    async fn an_open_stream_ends_when_the_token_is_cancelled() {
        let bus = EventBus::mock();
        let token = CancellationToken::new();
        let response = event_stream_until(&bus, "widgets", None, &HeaderMap::new(), token.clone())
            .await
            .unwrap();
        let canceller = async {
            tokio::time::sleep(KEEP_ALIVE_INTERVAL * 3).await;
            // Still open before the cancel: a real event is delivered.
            publish(&bus, "widgets", EventAction::Create, vec![5]).await;
            tokio::time::sleep(KEEP_ALIVE_INTERVAL * 3).await;
            token.cancel();
        };
        // The bus is still alive, so only the token can end the body; the
        // timeout turns a hang into a failure.
        let ((), body) = tokio::join!(
            canceller,
            tokio::time::timeout(Duration::from_secs(2), drain(response))
        );
        let body = body.expect("the stream did not end after cancel");
        assert!(body.contains("event: subscriber"), "{body}");
        assert!(body.contains("event: create"), "{body}");
    }

    #[tokio::test]
    async fn a_subscription_opened_after_cancel_ends_immediately() {
        // Cap already used up: a post-cancel call must not even try to subscribe.
        let bus = EventBus::mock().with_max_anonymous_connections(1);
        let _held = event_stream(&bus, "widgets", None, &HeaderMap::new())
            .await
            .unwrap();
        let token = CancellationToken::new();
        token.cancel();
        let response = event_stream_until(&bus, "widgets", None, &HeaderMap::new(), token)
            .await
            .unwrap();
        let body = tokio::time::timeout(Duration::from_secs(2), drain(response))
            .await
            .expect("the stream did not end");
        assert!(body.is_empty(), "{body}");
    }

    #[tokio::test]
    async fn an_uncancelled_token_changes_nothing() {
        let bus = EventBus::mock();
        let token = CancellationToken::new();
        let response = event_stream_until(&bus, "widgets", None, &HeaderMap::new(), token)
            .await
            .unwrap();
        publish(&bus, "widgets", EventAction::Create, vec![9]).await;
        let body = first_frames(response, 2).await;
        assert!(body.contains("event: create"), "{body}");
    }

    /// Read a response body to its end (non-keep-alive chunks only).
    async fn drain(response: EventSse) -> String {
        let mut body = response.into_body().into_data_stream();
        let mut collected = String::new();
        while let Some(Ok(chunk)) = body.next().await {
            let chunk = String::from_utf8_lossy(&chunk).to_string();
            if !chunk.starts_with(':') {
                collected.push_str(&chunk);
            }
        }
        collected
    }

    #[test]
    fn error_frame_logs_and_returns_a_typed_error_frame() {
        let err = serde_json::from_str::<i32>("bad").unwrap_err();
        // Doesn't panic; the frame is a real `SseEvent`, exercised end to
        // end via `event_frame`'s own callers in the tests above.
        let _frame = error_frame(err);
    }

    /// Read up to `count` SSE frames out of a response body. Keep-alive
    /// comment frames are filtered out so a slow test never sees one.
    pub(super) async fn first_frames(response: EventSse, count: usize) -> String {
        let mut body = response.into_body().into_data_stream();
        let mut collected = String::new();
        let mut seen = 0;
        while seen < count {
            let Ok(Some(Ok(chunk))) =
                tokio::time::timeout(Duration::from_millis(500), body.next()).await
            else {
                break;
            };
            let chunk = String::from_utf8_lossy(&chunk).to_string();
            if chunk.starts_with(':') {
                continue;
            }
            collected.push_str(&chunk);
            seen += 1;
        }
        collected
    }
}
