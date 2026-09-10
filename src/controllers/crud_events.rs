//! The resource-agnostic half of `GET <prefix>/events` -- subscriber-id
//! resolution, the SSE frame shape, and the publish helper every mutating
//! handler calls. `controllers::heroes` supplies the one resource-specific
//! thing this needs: the resource name (`"heroes"`).
//!
//! Shaped like `crud_actions`/`crud_query`: free functions a resource's
//! router composes, not a router of its own -- so a second resource gets
//! the same endpoint by adding one route and one `&'static str`.
//!
//! See `src/events.rs` for the transport and the delivery guarantee's exact
//! scope, `docs/adrs/0016-mqtt-backed-sse-crud-event-stream.md` for why
//! MQTT, and `docs/frs/FR-0030-crud-event-stream.md` for the contract.

use std::convert::Infallible;
use std::pin::Pin;
use std::time::Duration;

use axum::http::HeaderMap;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures::Stream;

use crate::events::{
    new_subscriber_id, sanitize_subscriber_id, CrudEvent, EventAction, EventBus, EventStream,
};
use crate::problem_details::AppError;

/// The header a browser's `EventSource` resends automatically after a
/// dropped connection -- this app puts the `subscriber_id` in every frame's
/// `id:` field precisely so that automatic resend is enough to resume the
/// persistent session, with no client-side bookkeeping.
const LAST_EVENT_ID: &str = "last-event-id";

/// How often a comment frame is sent on an idle stream, to keep proxies and
/// load balancers from reaping a connection that has simply had no events.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// The event stream's response type. A plain `Response` rather than
/// `Sse<S>`: `Sse::keep_alive` wraps the stream in an axum-private
/// `KeepAliveStream`, so the configured value has no nameable type for a
/// handler signature -- and the body is already a trait object, so nothing
/// is lost by erasing the rest of it here.
pub type EventSse = Response;

/// Pick the `subscriber_id` this connection resumes (or starts).
///
/// Precedence: the explicit `?subscriber_id=` query parameter, then the
/// `Last-Event-ID` header, then a freshly issued id. An id that fails
/// `sanitize_subscriber_id` is treated as absent rather than as an error --
/// a client sending garbage gets a working stream with a new id, not a 400
/// it cannot act on. The cost is stated plainly in the FR: no replay.
pub fn resolve_subscriber_id(requested: Option<&str>, headers: &HeaderMap) -> String {
    requested
        .and_then(sanitize_subscriber_id)
        .or_else(|| {
            headers
                .get(LAST_EVENT_ID)
                .and_then(|value| value.to_str().ok())
                .and_then(sanitize_subscriber_id)
        })
        .unwrap_or_else(new_subscriber_id)
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
    match frame.json_data(event) {
        Ok(frame) => frame,
        Err(err) => {
            // CrudEvent is a plain struct of owned scalars, so this is
            // unreachable in practice; degrade to a typed error frame
            // rather than panicking inside a live connection.
            tracing::error!("failed to encode CRUD event frame: {err}");
            SseEvent::default().event("error").data("encode failed")
        }
    }
}

/// Build the SSE response for `resource`'s event stream.
pub async fn event_stream(
    bus: &EventBus,
    resource: &'static str,
    requested_subscriber_id: Option<&str>,
    headers: &HeaderMap,
) -> Result<EventSse, AppError> {
    let subscriber_id = resolve_subscriber_id(requested_subscriber_id, headers);
    let stream = bus
        .subscribe(resource, &subscriber_id)
        .await
        .map_err(|err| AppError::ServiceUnavailable(err.to_string()))?;
    Ok(sse_from(subscriber_id, stream))
}

/// Wrap a resolved `EventStream` in the SSE response shape: the
/// subscriber-id frame first, then one frame per event until the feed ends.
fn sse_from(subscriber_id: String, stream: EventStream) -> EventSse {
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

    let body: Pin<Box<dyn Stream<Item = Result<SseEvent, Infallible>> + Send>> = Box::pin(body);
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
    use axum::http::HeaderValue;

    fn headers_with_last_event_id(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(LAST_EVENT_ID, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn an_explicit_query_parameter_wins() {
        let headers = headers_with_last_event_id("from-header");
        assert_eq!(
            resolve_subscriber_id(Some("from-query"), &headers),
            "from-query"
        );
    }

    #[test]
    fn the_last_event_id_header_is_used_when_no_query_parameter_is_given() {
        let headers = headers_with_last_event_id("from-header");
        assert_eq!(resolve_subscriber_id(None, &headers), "from-header");
    }

    #[test]
    fn a_fresh_id_is_issued_when_neither_source_supplies_one() {
        let issued = resolve_subscriber_id(None, &HeaderMap::new());
        assert_eq!(
            sanitize_subscriber_id(&issued).as_deref(),
            Some(issued.as_str())
        );
    }

    #[test]
    fn an_invalid_query_parameter_falls_back_to_the_header() {
        let headers = headers_with_last_event_id("from-header");
        assert_eq!(
            resolve_subscriber_id(Some("bad/id"), &headers),
            "from-header"
        );
    }

    #[test]
    fn an_invalid_id_from_both_sources_yields_a_fresh_one() {
        let headers = headers_with_last_event_id("also/bad");
        let issued = resolve_subscriber_id(Some(""), &headers);
        assert!(sanitize_subscriber_id(&issued).is_some());
        assert_ne!(issued, "also/bad");
    }

    #[test]
    fn a_non_ascii_header_value_is_ignored() {
        let mut headers = HeaderMap::new();
        headers.insert(
            LAST_EVENT_ID,
            HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
        );
        let issued = resolve_subscriber_id(None, &headers);
        assert!(sanitize_subscriber_id(&issued).is_some());
    }

    #[tokio::test]
    async fn the_first_frame_carries_the_subscriber_id() {
        let bus = EventBus::mock();
        let response = event_stream(&bus, "heroes", Some("sub-1"), &HeaderMap::new())
            .await
            .unwrap();
        let body = first_frames(response, 1).await;
        assert!(body.contains("event: subscriber"), "{body}");
        assert!(body.contains("id: sub-1"), "{body}");
        assert!(body.contains(r#"{"subscriber_id":"sub-1"}"#), "{body}");
    }

    #[tokio::test]
    async fn published_events_become_frames_named_after_the_action() {
        let bus = EventBus::mock();
        let response = event_stream(&bus, "heroes", Some("sub-2"), &HeaderMap::new())
            .await
            .unwrap();
        publish(&bus, "heroes", EventAction::Create, vec![42]).await;
        let body = first_frames(response, 2).await;
        assert!(body.contains("event: create"), "{body}");
        assert!(body.contains(r#""ids":[42]"#), "{body}");
        assert!(body.contains("id: sub-2"), "{body}");
    }

    #[tokio::test]
    async fn the_stream_ends_when_the_bus_goes_away() {
        let bus = EventBus::mock();
        let response = event_stream(&bus, "heroes", Some("sub-3"), &HeaderMap::new())
            .await
            .unwrap();
        drop(bus);
        // Only the subscriber frame is ever produced; asking for two frames
        // still terminates rather than hanging.
        let body = first_frames(response, 2).await;
        assert!(body.contains("event: subscriber"), "{body}");
        assert!(!body.contains("event: create"), "{body}");
    }

    /// Read up to `count` SSE frames out of a response body. Keep-alive
    /// comment frames are filtered out so a slow test never sees one.
    pub(super) async fn first_frames(response: EventSse, count: usize) -> String {
        use futures::StreamExt;

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
