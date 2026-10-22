//! MQTT-backed CRUD event publish/subscribe -- the transport under
//! `controllers::crud_events`'s Server-Sent Events stream. See
//! `docs/adrs/0016-mqtt-backed-sse-crud-event-stream.md` for the design and
//! `docs/frs/FR-0030-crud-event-stream.md` for the endpoint contract.
//!
//! Flat (outside any `src/` submodule) for the same reason `rate_limit.rs`
//! is: no resource-specific code and no state of its own beyond what it is
//! handed at construction. `controllers` reaches for it inline the way it
//! reaches for `RateLimiter` -- deliberately *not* a hook threaded through
//! `crud::CrudService`, which stays free of infrastructure concerns
//! (`docs/nfrs/NFR-0004-generic-crud-excludes-resource-logic.md`).
//!
//! ## Delivery guarantee (and its exact scope)
//!
//! A subscriber is issued a `subscriber_id` on first connect. That id --
//! and nothing else -- is what the guarantee hangs on: it becomes the MQTT
//! **client id** (`client_id_for`) of a **persistent session**
//! (`set_clean_session(false)`) subscribed at **QoS 1** to
//! `crud-events/<resource>`. Because the session is persistent, Mosquitto
//! queues messages published while that client id is disconnected and
//! delivers them when the same client id reconnects. That is what makes
//! "briefly offline, no missed events" true.
//!
//! It is true *only* for a client that preserves its `subscriber_id` and
//! sends it back (`?subscriber_id=` or `Last-Event-ID`). A client that
//! reconnects with no id gets a fresh session and no replay, silently --
//! by design, since there is no infinite backlog to replay from. The
//! broker-side queue is bounded by `.devcontainer/stack/mqtt/
//! mosquitto.conf` (`max_queued_messages 1000`,
//! `persistent_client_expiration 1h`).
//!
//! `Mode::Mock` swaps the broker for a `tokio::sync::broadcast` fan-out
//! (`EventBus::mock`), the same shape `RateLimiter::mock`/
//! `HeroMemoryRepository` take so the app still boots with zero containers
//! (`NFR-0022`). That path is explicitly best-effort: no broker, no
//! persistent session, no replay. A test run against it cannot be used to
//! validate the delivery guarantee above -- only `tests/mqtt_events.rs`,
//! against the real broker, does that.

use std::time::Duration;

use rumqttc::{AsyncClient, ConnectionError, Event, EventLoop, MqttOptions, Packet, QoS};
use tokio::sync::broadcast;

/// Abstraction over `rumqttc::EventLoop::poll`, so `EventStream::Mqtt`
/// can be driven by a test-only fake that returns a scripted sequence of
/// poll errors -- the only way to deterministically exercise
/// `next_event`'s backoff/reconnect-limit branches without actually
/// breaking the shared Mosquitto container mid-suite, which would risk
/// flaking every other MQTT test running concurrently.
#[async_trait::async_trait]
pub trait MqttPoll: Send {
    async fn poll(&mut self) -> Result<Event, ConnectionError>;
}

#[async_trait::async_trait]
impl MqttPoll for EventLoop {
    async fn poll(&mut self) -> Result<Event, ConnectionError> {
        EventLoop::poll(self).await
    }
}

/// Keeps the publisher's own MQTT event loop turning so queued publishes
/// actually leave the process (`EventBus::connect`'s own doc comment) --
/// runs forever in production; a test drives it against a fake that
/// answers one scripted error and then pends forever, bounded by a
/// `tokio::time::timeout`.
async fn drive_publisher_eventloop(eventloop: &mut dyn MqttPoll) {
    loop {
        if let Err(err) = eventloop.poll().await {
            tracing::warn!("mqtt publisher connection error: {err}");
            tokio::time::sleep(POLL_ERROR_BACKOFF).await;
        }
    }
}

/// Root of the topic tree every resource's events publish under, matching
/// the reference implementation's `crud-events/<resource>`.
const TOPIC_ROOT: &str = "crud-events";

/// Bound on a client-supplied `subscriber_id`. The id is interpolated into
/// an MQTT client id, so it is length- and charset-checked before use
/// rather than trusted (`sanitize_subscriber_id`) -- MQTT client ids have
/// their own length limits and a `/`-bearing id would be confusing at best.
const MAX_SUBSCRIBER_ID_LEN: usize = 64;

/// Fan-out queue depth for the `Mode::Mock` broadcast channel, and the
/// per-connection request-channel capacity for the real `AsyncClient`.
const CHANNEL_CAPACITY: usize = 64;

/// How long a subscriber's event loop tolerates consecutive broker errors
/// (reconnect attempts) before ending the SSE stream and letting the client
/// reconnect from scratch. Without a cap, an unreachable broker would spin
/// the poll loop; without any tolerance, a single blip would drop every
/// open stream.
const MAX_CONSECUTIVE_POLL_ERRORS: u32 = 5;
#[cfg(not(test))]
const POLL_ERROR_BACKOFF: Duration = Duration::from_millis(500);
// A few milliseconds under test, so the backoff/reconnect-limit tests
// below don't spend real seconds sleeping -- same idea as
// `crud_events.rs`'s `KEEP_ALIVE_INTERVAL` cfg override.
#[cfg(test)]
const POLL_ERROR_BACKOFF: Duration = Duration::from_millis(1);

const KEEP_ALIVE: Duration = Duration::from_secs(30);

/// What happened to a record. Broader than a revision log's scope (the
/// reference ADR's `RevisionSink` comparison): a subscriber watching a
/// resource in real time cares about bulk actions as their own kind, not
/// as N single-record events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventAction {
    Create,
    Update,
    UpdateMany,
    Delete,
    DeleteMany,
}

impl EventAction {
    /// The SSE `event:` name for this action.
    pub fn as_str(self) -> &'static str {
        match self {
            EventAction::Create => "create",
            EventAction::Update => "update",
            EventAction::UpdateMany => "update_many",
            EventAction::Delete => "delete",
            EventAction::DeleteMany => "delete_many",
        }
    }
}

/// One published CRUD event -- the JSON payload of an MQTT message and, on
/// the way out, of an SSE frame's `data:`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CrudEvent {
    /// The resource segment of the topic (`heroes`), so a client
    /// multiplexing several streams can tell them apart.
    pub resource: String,
    pub action: EventAction,
    /// Every record the action touched -- one id for the single-record
    /// actions, zero or more for the bulk ones.
    pub ids: Vec<i32>,
    pub occurred_at: chrono::NaiveDateTime,
}

impl CrudEvent {
    pub fn new(resource: &str, action: EventAction, ids: Vec<i32>) -> Self {
        Self {
            resource: resource.to_string(),
            action,
            ids,
            occurred_at: chrono::Utc::now().naive_utc(),
        }
    }
}

/// The MQTT topic one resource's events publish to and are subscribed from.
pub fn topic(resource: &str) -> String {
    format!("{TOPIC_ROOT}/{resource}")
}

/// The MQTT client id a `subscriber_id` maps to. Stable for the life of
/// the subscriber id -- that stability *is* the persistent session.
pub fn client_id_for(subscriber_id: &str) -> String {
    format!("crud-events-{subscriber_id}")
}

/// Accept a client-supplied `subscriber_id` only if it is safe to use as an
/// MQTT client id: non-empty (`set_clean_session(false)` panics on an empty
/// client id), within `MAX_SUBSCRIBER_ID_LEN`, and restricted to
/// ASCII alphanumerics plus `-`/`_`. Anything else is rejected rather than
/// escaped, and the caller issues a fresh id instead.
pub fn sanitize_subscriber_id(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() || raw.len() > MAX_SUBSCRIBER_ID_LEN {
        return None;
    }
    if !raw
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return None;
    }
    Some(raw.to_string())
}

/// Number of random bytes behind a freshly issued subscriber id -- 128
/// bits, the same entropy a v4 UUID carries.
const SUBSCRIBER_ID_BYTES: usize = 16;

/// Issue a fresh subscriber id: 128 bits from the OS CSPRNG, hex-encoded.
/// Unguessable by construction, which matters because a subscriber id is a
/// capability -- whoever presents it resumes that persistent session and
/// its queued events.
///
/// Panics only if the OS entropy source itself is unavailable, which is
/// unrecoverable and exactly what `uuid::Uuid::new_v4`/`rand`'s `OsRng`
/// do in the same situation.
pub fn new_subscriber_id() -> String {
    let mut bytes = [0u8; SUBSCRIBER_ID_BYTES];
    getrandom::fill(&mut bytes).expect("OS entropy source unavailable");
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Logs a CRUD event's serialization failure and drops the publish --
/// see `EventBus::publish`'s own doc comment for why this never
/// propagates to the caller. `CrudEvent` is owned scalars, so this branch
/// never actually fires in practice; it's still tested directly rather
/// than left unreachable.
fn log_and_skip_publish(err: serde_json::Error) {
    tracing::error!("failed to serialize CRUD event: {err}");
}

/// Something went wrong opening a subscriber's stream.
#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("event broker error: {0}")]
    Broker(String),
}

enum Backend {
    /// A long-lived publisher client plus the broker address every
    /// subscriber opens its *own* persistent session against (each
    /// subscriber needs a distinct client id, so they cannot share one
    /// connection).
    Mqtt {
        host: String,
        port: u16,
        publisher: AsyncClient,
    },
    Mock(broadcast::Sender<CrudEvent>),
}

/// Shared event bus -- one instance built at startup (`lib.rs`'s
/// `build_event_bus`), held in `AppState` behind an `Arc`.
pub struct EventBus {
    backend: Backend,
}

impl EventBus {
    /// Connect the publisher side to the broker. The returned bus is usable
    /// immediately: rumqttc queues publishes on the `AsyncClient`'s request
    /// channel and flushes them once its event loop connects, so startup
    /// never blocks on broker availability.
    ///
    /// The publisher deliberately uses a *clean* session with its own
    /// process-scoped client id -- only subscribers need persistence, and a
    /// persistent publisher session would accumulate broker state for a
    /// client that never subscribes to anything.
    pub fn connect(host: &str, port: u16) -> Self {
        let client_id = format!("crud-events-publisher-{}", new_subscriber_id());
        let mut options = MqttOptions::new(client_id, host, port);
        options.set_keep_alive(KEEP_ALIVE);
        options.set_clean_session(true);
        let (publisher, mut eventloop) = AsyncClient::new(options, CHANNEL_CAPACITY);

        // The event loop is what actually drives the socket; a publish only
        // leaves the process while something polls it. Nothing consumes
        // incoming packets on the publisher connection, so this task just
        // keeps it turning and logs failures.
        tokio::spawn(async move { drive_publisher_eventloop(&mut eventloop).await });

        Self {
            backend: Backend::Mqtt {
                host: host.to_string(),
                port,
                publisher,
            },
        }
    }

    /// The `Mode::Mock` in-memory fan-out -- see the module doc for what it
    /// deliberately does not provide.
    pub fn mock() -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            backend: Backend::Mock(sender),
        }
    }

    /// Publish one event at QoS 1, non-retained. Failures are logged, never
    /// propagated: a CRUD write that already succeeded must not be reported
    /// to the caller as failed because a side-channel notification could
    /// not be delivered.
    pub async fn publish(&self, event: CrudEvent) {
        match &self.backend {
            Backend::Mqtt { publisher, .. } => {
                let payload = match serde_json::to_vec(&event) {
                    Ok(payload) => payload,
                    Err(err) => return log_and_skip_publish(err),
                };
                if let Err(err) = publisher
                    .publish(topic(&event.resource), QoS::AtLeastOnce, false, payload)
                    .await
                {
                    tracing::warn!("failed to publish CRUD event: {err}");
                }
            }
            Backend::Mock(sender) => {
                // `send` errors only when there are no receivers -- an idle
                // stream with nobody watching, not a failure.
                let _ = sender.send(event);
            }
        }
    }

    /// Open `subscriber_id`'s stream of `resource` events.
    ///
    /// On the MQTT path this opens (or *resumes*) that subscriber's own
    /// persistent session and subscribes at QoS 1 -- everything the broker
    /// queued for the same client id while it was away is delivered before
    /// anything new.
    pub async fn subscribe(
        &self,
        resource: &str,
        subscriber_id: &str,
    ) -> Result<EventStream, EventError> {
        match &self.backend {
            Backend::Mqtt { host, port, .. } => {
                let mut options = MqttOptions::new(client_id_for(subscriber_id), host, *port);
                options.set_keep_alive(KEEP_ALIVE);
                // The entire delivery guarantee: a persistent session keyed
                // by this subscriber's own client id. `subscriber_id` is
                // guaranteed non-empty by `sanitize_subscriber_id`/
                // `new_subscriber_id`, which is what keeps this call (which
                // panics on an empty client id) safe.
                options.set_clean_session(false);
                let (client, eventloop) = AsyncClient::new(options, CHANNEL_CAPACITY);
                client
                    .subscribe(topic(resource), QoS::AtLeastOnce)
                    .await
                    .map_err(|err| EventError::Broker(err.to_string()))?;
                Ok(EventStream::Mqtt {
                    // Kept alive alongside the event loop: dropping the
                    // client closes the request channel and ends the loop.
                    _client: client,
                    eventloop: Box::new(eventloop),
                    consecutive_errors: 0,
                })
            }
            Backend::Mock(sender) => Ok(EventStream::Mock {
                receiver: sender.subscribe(),
                resource: resource.to_string(),
            }),
        }
    }
}

/// One subscriber's live event feed. Dropping it disconnects that
/// subscriber's MQTT session -- which the broker keeps (and queues into)
/// because the session is persistent.
pub enum EventStream {
    Mqtt {
        _client: AsyncClient,
        eventloop: Box<dyn MqttPoll>,
        consecutive_errors: u32,
    },
    Mock {
        receiver: broadcast::Receiver<CrudEvent>,
        resource: String,
    },
}

impl EventStream {
    /// The next event, or `None` once the feed is finished (the broker went
    /// away for `MAX_CONSECUTIVE_POLL_ERRORS` consecutive attempts, or --
    /// under `Mode::Mock` -- the bus itself was dropped). Ending the stream
    /// is safe: the client reconnects with the same `subscriber_id` and
    /// resumes the same persistent session.
    pub async fn next_event(&mut self) -> Option<CrudEvent> {
        match self {
            EventStream::Mqtt {
                eventloop,
                consecutive_errors,
                ..
            } => loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        *consecutive_errors = 0;
                        match serde_json::from_slice::<CrudEvent>(&publish.payload) {
                            Ok(event) => return Some(event),
                            Err(err) => {
                                // Someone else published to this topic in a
                                // shape this app doesn't understand. Skip
                                // it rather than tearing down the stream.
                                tracing::warn!("skipping undecodable CRUD event: {err}");
                            }
                        }
                    }
                    Ok(_) => {
                        // ConnAck/SubAck/PingResp/outgoing acks -- the
                        // protocol turning over, not an event.
                        *consecutive_errors = 0;
                    }
                    Err(err) => {
                        *consecutive_errors += 1;
                        tracing::warn!(
                            "mqtt subscriber connection error ({consecutive_errors}): {err}"
                        );
                        if *consecutive_errors >= MAX_CONSECUTIVE_POLL_ERRORS {
                            return None;
                        }
                        tokio::time::sleep(POLL_ERROR_BACKOFF).await;
                    }
                }
            },
            EventStream::Mock {
                receiver, resource, ..
            } => loop {
                match receiver.recv().await {
                    // One broadcast channel carries every resource, unlike
                    // MQTT's per-resource topic, so filter here.
                    Ok(event) if event.resource == *resource => return Some(event),
                    Ok(_) => continue,
                    // Lagged: this subscriber fell behind the fan-out
                    // buffer. Best-effort by design under Mode::Mock (see
                    // the module doc) -- keep going from wherever the
                    // channel resumes.
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!("mock event subscriber lagged, dropped {skipped} events");
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_is_namespaced_under_the_crud_events_root() {
        assert_eq!(topic("heroes"), "crud-events/heroes");
    }

    #[test]
    fn client_id_is_derived_from_the_subscriber_id() {
        assert_eq!(client_id_for("abc123"), "crud-events-abc123");
    }

    #[test]
    fn a_new_subscriber_id_is_a_valid_subscriber_id() {
        let id = new_subscriber_id();
        assert!(!id.is_empty());
        assert_eq!(sanitize_subscriber_id(&id).as_deref(), Some(id.as_str()));
    }

    #[test]
    fn new_subscriber_ids_are_unique() {
        assert_ne!(new_subscriber_id(), new_subscriber_id());
    }

    #[test]
    fn log_and_skip_publish_does_not_panic() {
        let err = serde_json::from_str::<i32>("bad").unwrap_err();
        log_and_skip_publish(err);
    }

    #[test]
    fn sanitize_rejects_an_empty_or_whitespace_only_id() {
        // An empty client id would panic set_clean_session(false) -- this
        // rejection is what makes that call safe.
        assert_eq!(sanitize_subscriber_id(""), None);
        assert_eq!(sanitize_subscriber_id("   "), None);
    }

    #[test]
    fn sanitize_rejects_an_over_long_id() {
        let too_long = "a".repeat(MAX_SUBSCRIBER_ID_LEN + 1);
        assert_eq!(sanitize_subscriber_id(&too_long), None);
        let exactly_max = "a".repeat(MAX_SUBSCRIBER_ID_LEN);
        assert!(sanitize_subscriber_id(&exactly_max).is_some());
    }

    #[test]
    fn sanitize_rejects_topic_and_wildcard_characters() {
        for raw in ["a/b", "a+b", "a#b", "a b", "a.b", "héro"] {
            assert_eq!(sanitize_subscriber_id(raw), None, "{raw} must be rejected");
        }
    }

    #[test]
    fn sanitize_trims_surrounding_whitespace() {
        assert_eq!(
            sanitize_subscriber_id("  abc-1_2  ").as_deref(),
            Some("abc-1_2")
        );
    }

    #[test]
    fn event_actions_serialize_as_snake_case_names() {
        for (action, expected) in [
            (EventAction::Create, "create"),
            (EventAction::Update, "update"),
            (EventAction::UpdateMany, "update_many"),
            (EventAction::Delete, "delete"),
            (EventAction::DeleteMany, "delete_many"),
        ] {
            assert_eq!(action.as_str(), expected);
            assert_eq!(
                serde_json::to_value(action).unwrap(),
                serde_json::Value::String(expected.to_string())
            );
        }
    }

    #[test]
    fn a_crud_event_round_trips_through_json() {
        let event = CrudEvent::new("heroes", EventAction::DeleteMany, vec![1, 2, 3]);
        let encoded = serde_json::to_vec(&event).unwrap();
        let decoded: CrudEvent = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, event);
        assert_eq!(decoded.resource, "heroes");
        assert_eq!(decoded.ids, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn mock_bus_fans_an_event_out_to_a_subscriber() {
        let bus = EventBus::mock();
        let mut stream = bus.subscribe("heroes", "sub-1").await.unwrap();
        bus.publish(CrudEvent::new("heroes", EventAction::Create, vec![7]))
            .await;
        let event = stream.next_event().await.unwrap();
        assert_eq!(event.action, EventAction::Create);
        assert_eq!(event.ids, vec![7]);
    }

    #[tokio::test]
    async fn mock_bus_delivers_to_every_open_subscriber() {
        let bus = EventBus::mock();
        let mut a = bus.subscribe("heroes", "sub-a").await.unwrap();
        let mut b = bus.subscribe("heroes", "sub-b").await.unwrap();
        bus.publish(CrudEvent::new("heroes", EventAction::Update, vec![1]))
            .await;
        assert_eq!(a.next_event().await.unwrap().ids, vec![1]);
        assert_eq!(b.next_event().await.unwrap().ids, vec![1]);
    }

    #[tokio::test]
    async fn mock_bus_filters_events_for_other_resources() {
        let bus = EventBus::mock();
        let mut stream = bus.subscribe("heroes", "sub-1").await.unwrap();
        bus.publish(CrudEvent::new("villains", EventAction::Create, vec![1]))
            .await;
        bus.publish(CrudEvent::new("heroes", EventAction::Create, vec![2]))
            .await;
        let event = stream.next_event().await.unwrap();
        assert_eq!(event.resource, "heroes");
        assert_eq!(event.ids, vec![2]);
    }

    #[tokio::test]
    async fn mock_bus_publish_with_no_subscribers_is_a_no_op() {
        let bus = EventBus::mock();
        bus.publish(CrudEvent::new("heroes", EventAction::Create, vec![1]))
            .await;
    }

    #[tokio::test]
    async fn a_mock_stream_ends_once_the_bus_is_dropped() {
        let bus = EventBus::mock();
        let mut stream = bus.subscribe("heroes", "sub-1").await.unwrap();
        drop(bus);
        assert!(stream.next_event().await.is_none());
    }

    #[tokio::test]
    async fn a_lagging_mock_subscriber_keeps_going_after_dropping_events() {
        let bus = EventBus::mock();
        let mut stream = bus.subscribe("heroes", "sub-1").await.unwrap();
        // Overrun the fan-out buffer without reading, then confirm the
        // stream resumes rather than ending.
        for id in 0..(CHANNEL_CAPACITY as i32 + 5) {
            bus.publish(CrudEvent::new("heroes", EventAction::Create, vec![id]))
                .await;
        }
        let event = stream.next_event().await.unwrap();
        assert_eq!(event.action, EventAction::Create);
    }

    /// Returns a scripted sequence of poll results -- the only
    /// deterministic way to drive `EventStream::next_event`'s MQTT
    /// backoff/reconnect-limit branches, which only run against a broken
    /// broker connection (see `MqttPoll`'s own doc comment).
    struct ScriptedEventLoop {
        responses: std::collections::VecDeque<Result<Event, ConnectionError>>,
    }

    impl ScriptedEventLoop {
        fn new(responses: Vec<Result<Event, ConnectionError>>) -> Self {
            Self {
                responses: responses.into(),
            }
        }
    }

    #[async_trait::async_trait]
    impl MqttPoll for ScriptedEventLoop {
        async fn poll(&mut self) -> Result<Event, ConnectionError> {
            self.responses
                .pop_front()
                .expect("ScriptedEventLoop's scripted responses were exhausted")
        }
    }

    fn mqtt_stream_with(responses: Vec<Result<Event, ConnectionError>>) -> EventStream {
        let options = MqttOptions::new("test-client", "localhost", 1883);
        let (client, _real_eventloop) = AsyncClient::new(options, CHANNEL_CAPACITY);
        EventStream::Mqtt {
            _client: client,
            eventloop: Box::new(ScriptedEventLoop::new(responses)),
            consecutive_errors: 0,
        }
    }

    fn publish_event(event: &CrudEvent) -> Event {
        Event::Incoming(Packet::Publish(rumqttc::Publish::new(
            topic(&event.resource),
            QoS::AtLeastOnce,
            serde_json::to_vec(event).unwrap(),
        )))
    }

    #[tokio::test]
    async fn mqtt_stream_backs_off_and_recovers_from_fewer_than_max_consecutive_errors() {
        let event = CrudEvent::new("heroes", EventAction::Create, vec![9]);
        let mut stream = mqtt_stream_with(vec![
            Err(ConnectionError::NetworkTimeout),
            Err(ConnectionError::NetworkTimeout),
            Ok(publish_event(&event)),
        ]);
        let received = stream.next_event().await.unwrap();
        assert_eq!(received.ids, vec![9]);
    }

    #[tokio::test]
    async fn mqtt_stream_ends_after_max_consecutive_poll_errors() {
        let mut responses = Vec::with_capacity(MAX_CONSECUTIVE_POLL_ERRORS as usize);
        for _ in 0..MAX_CONSECUTIVE_POLL_ERRORS {
            responses.push(Err(ConnectionError::NetworkTimeout));
        }
        let mut stream = mqtt_stream_with(responses);
        assert!(stream.next_event().await.is_none());
    }

    /// One scripted error, then pends forever -- for
    /// `drive_publisher_eventloop`'s infinite loop, which a test can only
    /// ever observe a bounded prefix of.
    struct FirstErrorThenPendingForever(Option<ConnectionError>);

    #[async_trait::async_trait]
    impl MqttPoll for FirstErrorThenPendingForever {
        async fn poll(&mut self) -> Result<Event, ConnectionError> {
            match self.0.take() {
                Some(err) => Err(err),
                None => std::future::pending().await,
            }
        }
    }

    #[tokio::test]
    async fn drive_publisher_eventloop_logs_and_backs_off_on_a_poll_error() {
        let mut eventloop = FirstErrorThenPendingForever(Some(ConnectionError::NetworkTimeout));
        // The loop never returns on its own; give it just long enough to
        // take the error branch once before cutting it off.
        let _ = tokio::time::timeout(
            Duration::from_millis(50),
            drive_publisher_eventloop(&mut eventloop),
        )
        .await;
    }
}
