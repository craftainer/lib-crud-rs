//! CRUD event publish/subscribe: the transport under
//! [`controllers::crud_events`](crate::controllers::crud_events)'s
//! Server-Sent Events stream.
//!
//! Two buses share one [`EventBus`] type. [`EventBus::mock`] is an in-memory
//! `tokio::sync::broadcast` fan-out: no broker, no persistence and no replay,
//! so a host can boot with zero containers. [`EventBus::connect`] (feature
//! `mqtt`) publishes to and subscribes from an MQTT broker. The bus is not a
//! hook threaded through [`CrudService`](crate::crud::CrudService), which stays
//! free of infrastructure: a resource's router calls
//! [`publish`](crate::controllers::crud_events::publish) itself.
//!
//! ## Subscriber ids
//!
//! A subscriber id is a capability: whoever presents it resumes that
//! subscriber's persistent session and the events queued in it. So the
//! server mints every id ([`EventBus::mint_subscriber_id`]) and a client can
//! only hand one back, never pick one. An id is 128 random bits plus a
//! 128-bit HMAC-SHA256 tag over the resource and those bits, keyed with a
//! secret the bus holds ([`EventBus::with_subscriber_secret`], random per
//! bus otherwise). [`EventBus::subscribe`] resumes an id only when its tag
//! verifies for the resource being subscribed; anything else (absent, forged,
//! low-entropy, from another resource, minted before a restart under a random
//! secret) gets a fresh id and a fresh session, so a made-up id can never take
//! over somebody else's session. The id is also scoped per resource: the tag
//! covers the resource and the MQTT client id is
//! `crud-events-<resource>-<id>`.
//!
//! Every connection that has to be given a fresh id is *anonymous*. With
//! [`EventBus::subscribe`] it also leaves a persistent session on the broker
//! behind it. The number of anonymous streams open at once is capped
//! ([`EventBus::with_max_anonymous_connections`]): over the cap
//! [`EventBus::subscribe`] fails with
//! [`EventError::TooManyAnonymousConnections`]. A client that resumes with its
//! id is not counted. That cap is one budget for the whole bus, so it is only
//! a backstop; the per-caller limits below are what bound one caller.
//!
//! ## Stream limits (the HTTP events route)
//!
//! [`EventBus::subscribe_for_caller`], which the events route uses, also
//! bounds the streams open at once per *caller* (the host passes the owner
//! scope's value; default [`DEFAULT_MAX_STREAMS_PER_CALLER`], counted across
//! resources) and per *subscriber id* ([`DEFAULT_MAX_STREAMS_PER_SUBSCRIBER`],
//! so one issued id cannot be opened over and over). Over either limit it
//! fails with [`EventError::TooManyStreams`] (a `429`). A slot is released when
//! the [`EventStream`] is dropped, on every exit path. The limits are per
//! process: with N instances the effective cap is N times larger. Callers a
//! host cannot tell apart should pass one shared key and so share one budget.
//!
//! The route's streams are *transient* (clean session, no replay, a client's
//! `subscriber_id` is ignored) unless the host opts into durable sessions with
//! [`EventBus::with_durable_streams`]; a durable session is then kept by the
//! broker for [`DEFAULT_SESSION_TTL`] after the connection ends unless
//! [`EventBus::with_session_ttl`] or [`EventBus::without_session_ttl`] says
//! otherwise. See `docs/adrs/0012-hardened-mqtt-and-event-streams.md`.
//!
//! ## Securing the MQTT connection
//!
//! [`EventBus::with_mqtt_credentials`] sets a user name and password and
//! [`EventBus::with_mqtt_tls`] (feature `mqtt-tls`, rumqttc over the system
//! OpenSSL through native-tls) a [`MqttTls`]: a CA bundle that alone is
//! trusted to verify the broker and its host name, and optionally a client
//! certificate and key for mutual TLS. TLS 1.2 is the floor and host name
//! verification cannot be switched off. Credentials without TLS are refused unless
//! [`EventBus::with_mqtt_allow_insecure`] says otherwise; the check runs when
//! a connection is opened, and [`EventBus::validate`] runs it at startup. The
//! key files are read for every connection, so rotating them needs no restart.
//! Secrets are redacted from `Debug` output.
//!
//! ## Delivery (MQTT bus)
//!
//! Events are best effort. [`EventBus::publish`] queues each one on a single
//! shared publisher connection, opened on first use. A failure is logged and
//! counted ([`EventBus::delivery_stats`]), never returned, so a broker outage
//! is not a `500`. A publish that times out or is cancelled discards the shared
//! connection (the next one opens a new one). To announce a change only once it
//! is durable, record it in a [`PendingEvents`] while the transaction runs and
//! call [`PendingEvents::commit`] after the database commit.
//! [`EventBus::shutdown`] waits for publishes in flight and then closes the
//! connection.
//!
//! ## Transient subscribers
//!
//! A long-lived in-process consumer that always starts from scratch (a cache
//! invalidator) has no use for replay, and every restart would otherwise
//! orphan its previous persistent session. [`EventBus::subscribe_transient`]
//! connects with a *clean* session instead (MQTT 3.1.1 section 3.1.2.4): the
//! broker discards the session when the connection ends, whether it closes
//! cleanly or crashes, so nothing is left behind and nothing is replayed.
//! Pass `None` for the id (a fresh one is minted); an id a durable subscriber
//! uses would have its stored session discarded by the clean-session connect.
//! The HTTP events route never uses it, because browser clients rely on
//! replay. Production brokers should also set a finite persistent-session
//! expiry sized to the longest replay window clients need (Mosquitto's
//! `persistent_client_expiration`, never by default; EMQX defaults to 2h), as
//! a backstop for abandoned durable sessions.
//!
//! ## Bounding abandoned durable sessions
//!
//! [`EventBus::with_session_ttl`] makes durable subscribers connect over MQTT
//! 5.0 with Clean Start off and a Session Expiry Interval of that many seconds
//! (MQTT 5.0 sections 3.1.2.4 and 3.1.2.11.2), so a browser SSE client that
//! never returns has its session and queued events dropped by the broker after
//! the TTL, with no broker-wide setting needed. Within the TTL a returning id
//! is replayed as before. The default is [`DEFAULT_SESSION_TTL`] (one hour);
//! [`EventBus::without_session_ttl`] keeps MQTT 3.1.1 and leaves expiry to the
//! broker. Transient subscribers and the publisher stay on 3.1.1; the broker
//! delivers between the two versions. The broker must speak MQTT 5.0 and may
//! cap the value (Mosquitto's `max_session_expiry_interval`).
//! See `docs/adrs/0007-durable-session-ttl.md`.
//!
//! ## Delivery guarantee (MQTT bus, and its exact scope)
//!
//! A subscriber is issued a `subscriber_id` on first connect. That id, and
//! nothing else, is what the guarantee hangs on: it becomes part of the MQTT
//! **client id** of a **persistent session** (`set_clean_session(false)`; see
//! [`EventBus::subscribe_transient`] for the clean-session alternative)
//! subscribed at **`QoS` 1** to `crud-events/<resource>`. Because the session
//! is persistent, the broker queues messages published while that client id
//! is disconnected and delivers them when the same client id reconnects.
//!
//! It holds *only* for a client that keeps its `subscriber_id` and sends it
//! back (`?subscriber_id=` or `Last-Event-ID`), and only for events that
//! reached the broker. It is **not** "no missed events":
//!
//! - A client that reconnects with no (or an unverifiable) id gets a fresh
//!   session and no replay, by design: there is no infinite backlog.
//! - The broker-side queue is bounded by the broker's own configuration
//!   (`max_queued_messages`, `persistent_client_expiration`).
//! - The publisher side is fire-and-forget. [`EventBus::publish`] queues the
//!   message on a bounded in-process channel without waiting; when the
//!   broker is unreachable long enough for that queue to fill, or the process
//!   stops with messages still queued, those events are dropped (logged,
//!   never retried) and no subscriber, resuming or not, ever sees them. A
//!   CRUD write that succeeded is never failed for it. A client that needs
//!   certainty re-reads the resource after reconnecting.
//!
//! The in-memory bus cannot validate the guarantee; only a test against a real
//! broker (`tests/mqtt_events.rs`) does.

use std::collections::HashMap;
#[cfg(feature = "mqtt")]
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
#[cfg(feature = "mqtt-tls")]
use std::path::{Path, PathBuf};
#[cfg(feature = "mqtt")]
use std::sync::MutexGuard;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
#[cfg(feature = "mqtt")]
use std::time::Instant;

use hmac::{Hmac, KeyInit, Mac};
#[cfg(feature = "mqtt-tls")]
use rumqttc::TlsConfiguration;
#[cfg(feature = "mqtt")]
use rumqttc::{
    AsyncClient, ConnectionError, Event, EventLoop, MqttOptions, Packet, QoS, SubscribeReasonCode,
    Transport, v5 as mqtt5,
};
use sha2::Sha256;
use tokio::sync::{broadcast, mpsc};
#[cfg(feature = "mqtt")]
use tokio::task::JoinHandle;

/// What an event loop's poll produced, reduced to what this module acts on,
/// so one stream type serves both MQTT 3.1.1 and MQTT 5.0 connections.
#[cfg(feature = "mqtt")]
#[derive(Debug)]
enum Polled {
    /// An incoming PUBLISH, with its payload.
    Publish(Vec<u8>),
    /// A CONNACK: whether the broker resumed a stored session.
    ConnAck { session_present: bool },
    /// A SUBACK: whether any requested subscription was refused.
    SubAck { refused: bool },
    /// Anything else -- the protocol turning over.
    Other,
}

/// Reduce an MQTT 3.1.1 poll result to a [`Polled`].
#[cfg(feature = "mqtt")]
fn polled_v3(result: Result<Event, ConnectionError>) -> Result<Polled, String> {
    Ok(match result.map_err(|err| err.to_string())? {
        Event::Incoming(Packet::Publish(publish)) => Polled::Publish(publish.payload.to_vec()),
        Event::Incoming(Packet::ConnAck(ack)) => Polled::ConnAck {
            session_present: ack.session_present,
        },
        Event::Incoming(Packet::SubAck(ack)) => Polled::SubAck {
            refused: ack
                .return_codes
                .iter()
                .any(|code| matches!(code, SubscribeReasonCode::Failure)),
        },
        _ => Polled::Other,
    })
}

/// Reduce an MQTT 5.0 poll result to a [`Polled`]. Any SUBACK reason other
/// than a granted `QoS` is a refusal.
#[cfg(feature = "mqtt")]
fn polled_v5(result: Result<mqtt5::Event, mqtt5::ConnectionError>) -> Result<Polled, String> {
    use mqtt5::mqttbytes::v5::{Packet as Packet5, SubscribeReasonCode as Reason5};
    Ok(match result.map_err(|err| err.to_string())? {
        mqtt5::Event::Incoming(Packet5::Publish(publish)) => {
            Polled::Publish(publish.payload.to_vec())
        }
        mqtt5::Event::Incoming(Packet5::ConnAck(ack)) => Polled::ConnAck {
            session_present: ack.session_present,
        },
        mqtt5::Event::Incoming(Packet5::SubAck(ack)) => Polled::SubAck {
            refused: ack
                .return_codes
                .iter()
                .any(|code| !matches!(code, Reason5::Success(_))),
        },
        _ => Polled::Other,
    })
}

#[cfg(feature = "mqtt")]
/// Abstraction over `rumqttc::EventLoop::poll` (3.1.1 or 5.0), so an MQTT
/// `EventStream` can be driven by a test-only fake that returns a scripted
/// sequence of poll errors -- the only way to deterministically exercise
/// `next_event`'s backoff/reconnect-limit branches without actually
/// breaking the shared Mosquitto container mid-suite, which would risk
/// flaking every other MQTT test running concurrently.
#[async_trait::async_trait]
trait MqttPoll: Send {
    async fn poll(&mut self) -> Result<Polled, String>;
}

#[cfg(feature = "mqtt")]
#[async_trait::async_trait]
impl MqttPoll for EventLoop {
    async fn poll(&mut self) -> Result<Polled, String> {
        polled_v3(EventLoop::poll(self).await)
    }
}

#[cfg(feature = "mqtt")]
#[async_trait::async_trait]
impl MqttPoll for mqtt5::EventLoop {
    async fn poll(&mut self) -> Result<Polled, String> {
        polled_v5(mqtt5::EventLoop::poll(self).await)
    }
}

/// A subscriber's client handle, of whichever protocol version its
/// connection speaks.
#[cfg(feature = "mqtt")]
enum SubscriberClient {
    V3(AsyncClient),
    V5(mqtt5::AsyncClient),
}

#[cfg(feature = "mqtt")]
impl SubscriberClient {
    /// Queue a `QoS` 1 subscription to `topic`.
    async fn subscribe(&self, topic: String) -> Result<(), String> {
        match self {
            Self::V3(client) => client
                .subscribe(topic, QoS::AtLeastOnce)
                .await
                .map_err(|err| err.to_string()),
            Self::V5(client) => client
                .subscribe(topic, mqtt5::mqttbytes::QoS::AtLeastOnce)
                .await
                .map_err(|err| err.to_string()),
        }
    }

    /// Like [`subscribe`](Self::subscribe), without waiting for room in the
    /// request queue.
    fn try_subscribe(&self, topic: String) -> Result<(), String> {
        match self {
            Self::V3(client) => client
                .try_subscribe(topic, QoS::AtLeastOnce)
                .map_err(|err| err.to_string()),
            Self::V5(client) => client
                .try_subscribe(topic, mqtt5::mqttbytes::QoS::AtLeastOnce)
                .map_err(|err| err.to_string()),
        }
    }
}

#[cfg(feature = "mqtt")]
/// Keeps the publisher's own MQTT event loop turning so queued publishes
/// actually leave the process (`EventBus::connect`'s own doc comment) --
/// runs forever in production; a test drives it against a fake that
/// answers one scripted error and then pends forever, bounded by a
/// `tokio::time::timeout`.
async fn drive_publisher_eventloop<P: MqttPoll>(mut eventloop: P) {
    loop {
        if let Err(err) = eventloop.poll().await {
            tracing::warn!("mqtt publisher connection error: {err}");
            tokio::time::sleep(POLL_ERROR_BACKOFF).await;
        }
    }
}

/// Queue a `QoS` 1 subscription to `topic` on `client`.
#[cfg(feature = "mqtt")]
async fn subscribe_to(client: &SubscriberClient, topic: String) -> Result<(), EventError> {
    client.subscribe(topic).await.map_err(EventError::Broker)
}

/// Decode one broker payload, skipping (with a warning) anything this crate
/// did not publish: someone else wrote to this topic in a shape the app does
/// not understand, which must not tear down the stream.
#[cfg(feature = "mqtt")]
fn decode_event(payload: &[u8]) -> Option<CrudEvent> {
    match serde_json::from_slice::<CrudEvent>(payload) {
        Ok(event) => Some(event),
        Err(err) => {
            tracing::warn!("skipping undecodable CRUD event: {err}");
            None
        }
    }
}

/// Drive a freshly built subscriber connection until the broker has accepted
/// both the session (CONNECT) and the subscription (SUBSCRIBE), so a broker
/// that is down, refuses the client or rejects the topic fails
/// [`EventBus::subscribe`] instead of producing a stream that silently never
/// delivers. Events the broker replays from a resumed session may arrive
/// before the SUBACK; they are returned so none is lost.
#[cfg(feature = "mqtt")]
async fn await_subscription<P: MqttPoll + ?Sized>(
    eventloop: &mut P,
    timeout: Duration,
) -> Result<VecDeque<CrudEvent>, EventError> {
    let handshake = async {
        let mut backlog = VecDeque::new();
        loop {
            match eventloop.poll().await {
                Ok(Polled::SubAck { refused: true }) => {
                    return Err(EventError::Broker("subscription refused".to_string()));
                }
                Ok(Polled::SubAck { refused: false }) => return Ok(backlog),
                Ok(Polled::Publish(payload)) => backlog.extend(decode_event(&payload)),
                Ok(_) => {}
                Err(err) => return Err(EventError::Broker(err)),
            }
        }
    };
    tokio::time::timeout(timeout, handshake)
        .await
        .unwrap_or_else(|_| {
            Err(EventError::Broker(
                "timed out waiting for the broker".to_string(),
            ))
        })
}

/// Root of the topic tree every resource's events publish under, matching
/// `crud-events/<resource>`.
const TOPIC_ROOT: &str = "crud-events";

/// Random bytes behind a subscriber id: 128 bits, the entropy of a v4 UUID.
const NONCE_BYTES: usize = 16;

/// Bytes of the HMAC tag kept in a subscriber id (128 bits).
const TAG_BYTES: usize = 16;

/// Length of a subscriber id: the hex of the nonce followed by the hex of
/// its tag.
const SUBSCRIBER_ID_LEN: usize = 2 * (NONCE_BYTES + TAG_BYTES);

/// Bytes of the random per-bus secret used when the host supplies none.
const SECRET_BYTES: usize = 32;

/// The shortest secret [`EventBus::with_subscriber_secret`] accepts.
pub const MIN_SUBSCRIBER_SECRET_LEN: usize = 32;

/// How many anonymous connections a bus allows at once unless the host
/// says otherwise.
pub const DEFAULT_MAX_ANONYMOUS_CONNECTIONS: usize = 256;

/// How many `/events` streams one caller may hold open at once unless the
/// host says otherwise.
pub const DEFAULT_MAX_STREAMS_PER_CALLER: usize = 5;

/// How many streams may run under one subscriber id at once unless the host
/// says otherwise. Two, not one: a browser that reconnects can overlap the
/// connection the server has not yet noticed is gone.
pub const DEFAULT_MAX_STREAMS_PER_SUBSCRIBER: usize = 2;

/// How long the broker keeps a durable subscriber's session after its
/// connection ends, unless the host says otherwise. One hour.
pub const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(3600);

/// [`DEFAULT_SESSION_TTL`] as a Session Expiry Interval, in seconds.
#[cfg(feature = "mqtt")]
const DEFAULT_SESSION_TTL_SECONDS: u32 = 3600;

/// How long one publish may wait for room in the publisher's request queue
/// before the shared connection is discarded.
#[cfg(all(feature = "mqtt", not(test)))]
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(all(feature = "mqtt", test))]
const PUBLISH_TIMEOUT: Duration = Duration::from_millis(100);

/// How long [`EventBus::shutdown`] waits for publishes in flight.
#[cfg(not(test))]
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const DRAIN_TIMEOUT: Duration = Duration::from_millis(100);

/// How long [`EventBus::shutdown`] lets the publisher connection flush its
/// queue after the disconnect request.
#[cfg(all(feature = "mqtt", not(test)))]
const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);
#[cfg(all(feature = "mqtt", test))]
const SHUTDOWN_GRACE: Duration = Duration::from_millis(5);

/// Fan-out queue depth for the the in-memory bus broadcast channel, and the
/// per-connection request-channel capacity for the real `AsyncClient`.
const CHANNEL_CAPACITY: usize = 64;

/// How long a subscriber's event loop tolerates consecutive broker errors
/// (reconnect attempts) before ending the SSE stream and letting the client
/// reconnect from scratch. Without a cap, an unreachable broker would spin
/// the poll loop; without any tolerance, a single blip would drop every
/// open stream.
#[cfg(feature = "mqtt")]
const MAX_CONSECUTIVE_POLL_ERRORS: u32 = 5;
#[cfg(all(feature = "mqtt", not(test)))]
const POLL_ERROR_BACKOFF: Duration = Duration::from_millis(500);
// A few milliseconds under test, so the backoff/reconnect-limit tests
// below don't spend real seconds sleeping -- same idea as
// `crud_events.rs`'s `KEEP_ALIVE_INTERVAL` cfg override.
#[cfg(all(feature = "mqtt", test))]
const POLL_ERROR_BACKOFF: Duration = Duration::from_millis(1);

/// How long a connection must go without a poll error before an earlier run
/// of errors stops counting against it. A reconnect (CONNACK) alone proves
/// nothing: a flapping broker accepts every connection and drops it again.
#[cfg(all(feature = "mqtt", not(test)))]
const HEALTHY_AFTER: Duration = Duration::from_secs(30);
#[cfg(all(feature = "mqtt", test))]
const HEALTHY_AFTER: Duration = Duration::from_millis(60);

/// How long [`EventBus::subscribe`] waits for the broker to accept the
/// session and the subscription before reporting it unavailable.
#[cfg(feature = "mqtt")]
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(feature = "mqtt")]
const KEEP_ALIVE: Duration = Duration::from_secs(30);

/// What happened to a record. Broader than a revision log's scope: a
/// subscriber watching a resource in real time cares about bulk actions as
/// their own kind, not as N single-record events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventAction {
    /// A record was created.
    Create,
    /// A record was updated.
    Update,
    /// A bulk update touched several records.
    UpdateMany,
    /// A record was deleted.
    Delete,
    /// A bulk delete touched several records.
    DeleteMany,
}

impl EventAction {
    /// The SSE `event:` name for this action.
    #[must_use]
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
    /// The resource segment of the topic (`widgets`), so a client
    /// multiplexing several streams can tell them apart.
    pub resource: String,
    /// What happened.
    pub action: EventAction,
    /// Every record the action touched -- one id for the single-record
    /// actions, zero or more for the bulk ones.
    pub ids: Vec<i32>,
    /// When it happened (UTC).
    pub occurred_at: chrono::NaiveDateTime,
}

impl CrudEvent {
    /// An event for `resource`, stamped with the current time.
    #[must_use]
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
#[must_use]
pub fn topic(resource: &str) -> String {
    format!("{TOPIC_ROOT}/{resource}")
}

#[cfg(feature = "mqtt")]
/// The `MQTT` client id a resource's `subscriber_id` maps to. Stable for the
/// life of the subscriber id -- that stability *is* the persistent session --
/// and different for the same id under another resource.
#[must_use]
pub fn client_id_for(resource: &str, subscriber_id: &str) -> String {
    format!("crud-events-{resource}-{subscriber_id}")
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|&byte| {
            [
                char::from(HEX[usize::from(byte >> 4)]),
                char::from(HEX[usize::from(byte & 0x0f)]),
            ]
        })
        .collect()
}

/// `N` bytes from the OS CSPRNG.
///
/// # Panics
///
/// Only if the OS entropy source itself is unavailable, which is
/// unrecoverable and exactly what `uuid::Uuid::new_v4`/`rand`'s `OsRng`
/// do in the same situation.
fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).expect("OS entropy source unavailable");
    bytes
}

/// A fresh random token, hex-encoded: 128 bits from the OS CSPRNG. For
/// publisher client ids and the like, never for a subscriber id (those carry
/// a tag, see [`EventBus::mint_subscriber_id`]).
fn random_token() -> String {
    hex(&random_bytes::<NONCE_BYTES>())
}

/// The hex of the first [`TAG_BYTES`] of HMAC-SHA256 over `resource` and
/// `nonce`, keyed with `secret`.
fn subscriber_tag(secret: &[u8], resource: &str, nonce: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts a key of any length");
    mac.update(resource.as_bytes());
    // A separator no resource name can be confused across: the nonce is hex.
    mac.update(&[0]);
    mac.update(nonce.as_bytes());
    hex(&mac.finalize().into_bytes()[..TAG_BYTES])
}

/// Compare two equal-length strings without stopping at the first
/// difference.
fn constant_time_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |diff, (x, y)| diff | (x ^ y))
            == 0
}

/// Something went wrong opening a subscriber's stream.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EventError {
    #[error("event broker error: {0}")]
    /// The broker is unreachable, rejected or dropped the subscription.
    Broker(String),
    #[error("too many anonymous event connections")]
    /// The bus already serves its maximum number of connections that had to
    /// be given a fresh subscriber id; the caller should retry with the id
    /// it was issued.
    TooManyAnonymousConnections,
    #[error("too many concurrent event streams")]
    /// The caller, or the subscriber id it presented, already holds its
    /// maximum number of open streams.
    TooManyStreams,
    #[error("invalid event bus configuration: {0}")]
    /// The MQTT settings are refused (credentials without TLS) or unusable
    /// (a TLS file cannot be read).
    Config(String),
}

/// One slot of the anonymous-connection budget, returned when the stream
/// that holds it is dropped.
struct AnonymousPermit(Arc<AtomicUsize>);

impl Drop for AnonymousPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Open-stream counts by key (caller and subscriber id).
type StreamCounts = Arc<Mutex<HashMap<String, usize>>>;

/// One caller's and one subscriber id's share of the stream limits, returned
/// when the stream that holds it is dropped (on every exit path).
struct StreamSlot {
    counts: StreamCounts,
    keys: [String; 2],
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        let mut counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        for key in &self.keys {
            counts.entry(key.clone()).and_modify(|open| *open -= 1);
            if counts.get(key) == Some(&0) {
                counts.remove(key);
            }
        }
    }
}

/// What happened to the events handed to a bus since it was built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeliveryStats {
    /// Events queued for the broker (MQTT) or fanned out (in-memory).
    pub published: u64,
    /// Events dropped: the broker connection failed or timed out, or the bus
    /// was shutting down.
    pub failed: u64,
}

/// A user name and password for the broker. The password never appears in
/// `Debug` output.
#[cfg(feature = "mqtt")]
#[derive(Clone)]
struct Credentials {
    username: String,
    password: String,
}

#[cfg(feature = "mqtt")]
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Where the broker's certificate is verified against, and optionally the
/// client certificate to present: the options of
/// [`EventBus::with_mqtt_tls`].
///
/// The files are PEM and are read for every new connection, so a rotated
/// certificate takes effect without a restart. The CA bundle (one or more
/// certificates) is required and is the only trust anchor: the system store
/// is not consulted (to trust it, point the bundle at the system one, for
/// example `/etc/ssl/certs/ca-certificates.crt`). The client key is PKCS#8
/// PEM (`BEGIN PRIVATE KEY`). TLS 1.2 is the minimum and the broker's host
/// name is always verified against its certificate. The feature links the
/// system OpenSSL: building needs `pkg-config` and `libssl-dev`, running
/// needs `libssl3`.
#[cfg(feature = "mqtt-tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttTls {
    ca_file: PathBuf,
    client_auth: Option<(PathBuf, PathBuf)>,
}

#[cfg(feature = "mqtt-tls")]
impl MqttTls {
    /// Verify the broker against the PEM bundle at `ca_file`.
    #[must_use]
    pub fn new(ca_file: impl Into<PathBuf>) -> Self {
        Self {
            ca_file: ca_file.into(),
            client_auth: None,
        }
    }

    /// Present the PEM certificate chain at `cert_file` with its private key
    /// at `key_file` (mutual TLS).
    #[must_use]
    pub fn with_client_auth(
        mut self,
        cert_file: impl Into<PathBuf>,
        key_file: impl Into<PathBuf>,
    ) -> Self {
        self.client_auth = Some((cert_file.into(), key_file.into()));
        self
    }
}

/// The security options of the MQTT connections, and the rules about them.
#[cfg(feature = "mqtt")]
#[derive(Debug, Default)]
struct MqttSettings {
    credentials: Option<Credentials>,
    allow_insecure: bool,
    #[cfg(feature = "mqtt-tls")]
    tls: Option<MqttTls>,
}

/// The security options resolved for one connection attempt: the credentials
/// and, with TLS, the transport built from the files as they are now.
#[cfg(feature = "mqtt")]
struct Security {
    credentials: Option<Credentials>,
    transport: Option<Transport>,
}

#[cfg(feature = "mqtt")]
impl MqttSettings {
    fn has_tls(&self) -> bool {
        #[cfg(feature = "mqtt-tls")]
        {
            self.tls.is_some()
        }
        #[cfg(not(feature = "mqtt-tls"))]
        {
            false
        }
    }

    /// Refuse credentials that would cross the network in clear text.
    fn validate(&self) -> Result<(), String> {
        if self.credentials.is_some() && !self.allow_insecure && !self.has_tls() {
            return Err(
                "MQTT credentials are refused without TLS: configure with_mqtt_tls, \
                        or opt in with with_mqtt_allow_insecure for a local broker"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Validate, then read the TLS files for a new connection.
    #[cfg_attr(
        not(feature = "mqtt-tls"),
        expect(clippy::unused_async, reason = "only the TLS build reads files")
    )]
    async fn security(&self) -> Result<Security, String> {
        self.validate()?;
        #[cfg(feature = "mqtt-tls")]
        let transport = self.load_transport().await?;
        #[cfg(not(feature = "mqtt-tls"))]
        let transport = None;
        Ok(Security {
            credentials: self.credentials.clone(),
            transport,
        })
    }

    #[cfg(feature = "mqtt-tls")]
    async fn load_transport(&self) -> Result<Option<Transport>, String> {
        let Some(tls) = &self.tls else {
            return Ok(None);
        };
        let ca = read_pem(&tls.ca_file).await?;
        let client_auth = match &tls.client_auth {
            Some((cert, key)) => Some((read_pem(cert).await?, read_pem(key).await?)),
            None => None,
        };
        let connector = build_connector(&ca, client_auth.as_ref())
            .map_err(|err| format!("invalid TLS configuration: {err}"))?;
        Ok(Some(Transport::tls_with_config(
            TlsConfiguration::NativeConnector(connector),
        )))
    }
}

/// The TLS connector for one connection: TLS 1.2 or later, the host name
/// always verified, and only the certificates of `ca` (a PEM bundle) trusted.
/// `client` is the PEM certificate chain and PKCS#8 PEM key to present.
#[cfg(feature = "mqtt-tls")]
fn build_connector(
    ca: &[u8],
    client: Option<&(Vec<u8>, Vec<u8>)>,
) -> Result<native_tls::TlsConnector, Box<dyn std::error::Error + Send + Sync>> {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let mut builder = native_tls::TlsConnector::builder();
    builder
        .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
        .disable_built_in_roots(true);
    let text = String::from_utf8_lossy(ca);
    let mut trusted = 0;
    // `Certificate::from_pem` reads one certificate, so split the bundle.
    for block in text.split_inclusive(END) {
        if block.contains(BEGIN) {
            builder.add_root_certificate(native_tls::Certificate::from_pem(block.as_bytes())?);
            trusted += 1;
        }
    }
    if trusted == 0 {
        return Err("the CA file holds no PEM certificate".into());
    }
    if let Some((cert, key)) = client {
        builder.identity(native_tls::Identity::from_pkcs8(cert, key)?);
    }
    Ok(builder.build()?)
}

/// One PEM file, or why it could not be read.
#[cfg(feature = "mqtt-tls")]
async fn read_pem(path: &Path) -> Result<Vec<u8>, String> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(bytes),
        Err(err) => Err(format!("cannot read {}: {err}", path.display())),
    }
}

/// Apply the credentials and transport to an MQTT 3.1.1 connection.
#[cfg(feature = "mqtt")]
fn secure_v3(options: &mut MqttOptions, security: &Security) {
    if let Some(login) = &security.credentials {
        options.set_credentials(login.username.clone(), login.password.clone());
    }
    if let Some(transport) = &security.transport {
        options.set_transport(transport.clone());
    }
}

/// Apply the credentials and transport to an MQTT 5.0 connection.
#[cfg(feature = "mqtt")]
fn secure_v5(options: &mut mqtt5::MqttOptions, security: &Security) {
    if let Some(login) = &security.credentials {
        options.set_credentials(login.username.clone(), login.password.clone());
    }
    if let Some(transport) = &security.transport {
        options.set_transport(transport.clone());
    }
}

/// The shared publisher connection: the client and the task that drives its
/// event loop. Dropping it stops the task and closes the connection.
#[cfg(feature = "mqtt")]
struct Publisher {
    client: AsyncClient,
    generation: u64,
    task: JoinHandle<()>,
}

#[cfg(feature = "mqtt")]
impl Drop for Publisher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Discards the shared publisher on drop unless disarmed, so a publish that
/// fails, times out or is cancelled mid-flight never leaves a connection in an
/// unknown state behind it.
#[cfg(feature = "mqtt")]
struct DiscardGuard<'a> {
    backend: &'a MqttBackend,
    generation: u64,
    armed: bool,
}

#[cfg(feature = "mqtt")]
impl Drop for DiscardGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.backend.discard(self.generation);
        }
    }
}

/// The MQTT side of an [`EventBus`]: the broker address, how to secure the
/// connections, and the one lazily opened publisher every publish shares.
/// Every subscriber needs a distinct client id, so each opens its *own*
/// persistent session against the same address.
#[cfg(feature = "mqtt")]
struct MqttBackend {
    host: String,
    port: u16,
    /// Session Expiry Interval, in seconds, for durable subscribers;
    /// `None` keeps MQTT 3.1.1 sessions (see [`EventBus::with_session_ttl`]).
    session_ttl: Option<u32>,
    settings: MqttSettings,
    publisher: Mutex<Option<Publisher>>,
    /// Serialises opening the publisher so concurrent first publishes open
    /// one connection. Behind `AssertUnwindSafe` because tokio's `Mutex` is
    /// not `RefUnwindSafe` and `EventBus` was; it guards no data.
    opening: AssertUnwindSafe<tokio::sync::Mutex<()>>,
    generations: AtomicU64,
}

#[cfg(feature = "mqtt")]
impl MqttBackend {
    fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
            session_ttl: Some(DEFAULT_SESSION_TTL_SECONDS),
            settings: MqttSettings::default(),
            publisher: Mutex::new(None),
            opening: AssertUnwindSafe(tokio::sync::Mutex::new(())),
            generations: AtomicU64::new(0),
        }
    }

    fn publisher_slot(&self) -> MutexGuard<'_, Option<Publisher>> {
        self.publisher
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Drop the shared publisher if it is still the one of `generation`.
    fn discard(&self, generation: u64) {
        let mut slot = self.publisher_slot();
        if slot
            .as_ref()
            .is_some_and(|open| open.generation == generation)
        {
            *slot = None;
        }
    }

    /// The shared publisher's client, opening the connection on first use.
    /// Nothing waits for the broker: rumqttc queues publishes and flushes
    /// them once the event loop connects.
    ///
    /// The publisher deliberately uses a *clean* session with its own
    /// process-scoped client id -- only subscribers need persistence, and a
    /// persistent publisher session would accumulate broker state for a
    /// client that never subscribes to anything.
    async fn shared_client(&self) -> Result<(AsyncClient, u64), String> {
        let _opening = self.opening.lock().await;
        if let Some(open) = self.publisher_slot().as_ref() {
            return Ok((open.client.clone(), open.generation));
        }
        let security = self.settings.security().await?;
        let client_id = format!("crud-events-publisher-{}", random_token());
        let mut options = MqttOptions::new(client_id, &self.host, self.port);
        options.set_keep_alive(KEEP_ALIVE);
        options.set_clean_session(true);
        secure_v3(&mut options, &security);
        let (client, eventloop) = AsyncClient::new(options, CHANNEL_CAPACITY);
        let generation = self.generations.fetch_add(1, Ordering::Relaxed);
        // The event loop is what actually drives the socket; a publish only
        // leaves the process while something polls it. Nothing consumes
        // incoming packets on the publisher connection, so this task just
        // keeps it turning and logs failures.
        let task = tokio::spawn(drive_publisher_eventloop(eventloop));
        *self.publisher_slot() = Some(Publisher {
            client: client.clone(),
            generation,
            task,
        });
        Ok((client, generation))
    }

    /// Queue `event` as a JSON `QoS` 1, non-retained message on its topic.
    /// Waiting for room in the request queue is bounded by
    /// [`PUBLISH_TIMEOUT`]; a failure, a timeout or a cancelled call discards
    /// the shared connection.
    async fn send(
        &self,
        event: &CrudEvent,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::to_vec(event)?;
        let (client, generation) = self.shared_client().await?;
        let mut guard = DiscardGuard {
            backend: self,
            generation,
            armed: true,
        };
        let publish = client.publish(topic(&event.resource), QoS::AtLeastOnce, false, payload);
        match tokio::time::timeout(PUBLISH_TIMEOUT, publish).await {
            Ok(Ok(())) => {
                guard.armed = false;
                Ok(())
            }
            Ok(Err(err)) => Err(err.into()),
            Err(_) => Err("timed out queueing the event for the broker".into()),
        }
    }

    /// Disconnect the shared publisher, giving its event loop a moment to
    /// flush what is queued.
    async fn close(&self) {
        let open = self.publisher_slot().take();
        if let Some(open) = open {
            let _ = tokio::time::timeout(SHUTDOWN_GRACE, open.client.disconnect()).await;
            tokio::time::sleep(SHUTDOWN_GRACE).await;
        }
    }
}

enum Backend {
    #[cfg(feature = "mqtt")]
    Mqtt(Box<MqttBackend>),
    Mock(broadcast::Sender<CrudEvent>),
}

/// Shared event bus: build one at startup and share it (for example behind an
/// `Arc` in the application state).
pub struct EventBus {
    backend: Backend,
    secret: Vec<u8>,
    anonymous: Arc<AtomicUsize>,
    max_anonymous: usize,
    // `stream_counts`, `inflight` and `drained` sit behind `AssertUnwindSafe`
    // to keep `EventBus` `RefUnwindSafe`, as it was before they existed: a
    // panic inside one of their short critical sections leaves nothing
    // half-updated, and every lock recovers from poisoning.
    stream_counts: AssertUnwindSafe<StreamCounts>,
    max_streams_per_caller: Option<usize>,
    max_streams_per_subscriber: Option<usize>,
    durable_streams: bool,
    /// A clone of the sender is held by every publish in flight; shutdown
    /// removes this one and waits for the rest to drop.
    inflight: AssertUnwindSafe<Mutex<Option<mpsc::Sender<()>>>>,
    drained: AssertUnwindSafe<Mutex<Option<mpsc::Receiver<()>>>>,
    published: AtomicU64,
    failed: AtomicU64,
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("EventBus");
        match &self.backend {
            #[cfg(feature = "mqtt")]
            Backend::Mqtt(mqtt) => {
                debug
                    .field("backend", &"mqtt")
                    .field("host", &mqtt.host)
                    .field("port", &mqtt.port)
                    .field("settings", &mqtt.settings);
            }
            Backend::Mock(_) => {
                debug.field("backend", &"mock");
            }
        }
        debug
            .field("subscriber_secret", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl EventBus {
    fn with_backend(backend: Backend) -> Self {
        let (inflight, drained) = mpsc::channel(1);
        Self {
            backend,
            secret: random_bytes::<SECRET_BYTES>().to_vec(),
            anonymous: Arc::new(AtomicUsize::new(0)),
            max_anonymous: DEFAULT_MAX_ANONYMOUS_CONNECTIONS,
            stream_counts: AssertUnwindSafe(Arc::new(Mutex::new(HashMap::new()))),
            max_streams_per_caller: Some(DEFAULT_MAX_STREAMS_PER_CALLER),
            max_streams_per_subscriber: Some(DEFAULT_MAX_STREAMS_PER_SUBSCRIBER),
            durable_streams: false,
            inflight: AssertUnwindSafe(Mutex::new(Some(inflight))),
            drained: AssertUnwindSafe(Mutex::new(Some(drained))),
            published: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        }
    }

    /// A bus on the MQTT broker at `host`:`port`. It is usable immediately
    /// and nothing connects yet: the shared publisher opens on the first
    /// [`publish`](Self::publish), and rumqttc queues publishes and flushes
    /// them once its event loop connects, so startup never blocks on broker
    /// availability. (Subscribing does wait for the broker: see
    /// [`subscribe`](Self::subscribe).)
    ///
    /// The connection is plaintext and anonymous until
    /// [`with_mqtt_tls`](Self::with_mqtt_tls) and
    /// [`with_mqtt_credentials`](Self::with_mqtt_credentials) say otherwise.
    /// Durable subscribers keep their session for [`DEFAULT_SESSION_TTL`]
    /// (over MQTT 5.0, so the broker must speak it; see
    /// [`without_session_ttl`](Self::without_session_ttl)).
    #[cfg(feature = "mqtt")]
    #[must_use]
    pub fn connect(host: &str, port: u16) -> Self {
        Self::with_backend(Backend::Mqtt(Box::new(MqttBackend::new(host, port))))
    }

    /// The MQTT side, if this is an MQTT bus.
    #[cfg(feature = "mqtt")]
    fn mqtt(&self) -> Option<&MqttBackend> {
        match &self.backend {
            Backend::Mqtt(mqtt) => Some(mqtt.as_ref()),
            Backend::Mock(_) => None,
        }
    }

    /// Change the MQTT settings; a no-op on the in-memory bus.
    #[cfg(feature = "mqtt")]
    fn with_mqtt(mut self, change: impl FnOnce(&mut MqttBackend)) -> Self {
        if let Backend::Mqtt(mqtt) = &mut self.backend {
            change(mqtt.as_mut());
        }
        self
    }

    /// Log in to the broker as `username` with `password`, on every MQTT
    /// connection. Refused (see [`validate`](Self::validate)) unless TLS is
    /// configured with [`with_mqtt_tls`](Self::with_mqtt_tls) or
    /// [`with_mqtt_allow_insecure`](Self::with_mqtt_allow_insecure) says a
    /// plaintext link is acceptable. The password is redacted from `Debug`.
    /// The in-memory bus ignores it.
    #[cfg(feature = "mqtt")]
    #[must_use]
    pub fn with_mqtt_credentials(
        self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        let login = Credentials {
            username: username.into(),
            password: password.into(),
        };
        self.with_mqtt(|mqtt| mqtt.settings.credentials = Some(login))
    }

    /// Connect to the broker over TLS (1.2 or later), verifying its
    /// certificate and host name against `tls`. The in-memory bus ignores it.
    #[cfg(feature = "mqtt-tls")]
    #[must_use]
    pub fn with_mqtt_tls(self, tls: MqttTls) -> Self {
        self.with_mqtt(|mqtt| mqtt.settings.tls = Some(tls))
    }

    /// Allow credentials over a connection without TLS, for a broker on a
    /// trusted local network or a development stack. It changes nothing else:
    /// when TLS is configured the broker is still verified.
    #[cfg(feature = "mqtt")]
    #[must_use]
    pub fn with_mqtt_allow_insecure(self) -> Self {
        self.with_mqtt(|mqtt| mqtt.settings.allow_insecure = true)
    }

    /// Check the MQTT settings without connecting, so a host can fail at
    /// startup. Every connection runs the same check and fails closed, but
    /// only when it is opened (a publish then logs and counts the failure).
    ///
    /// # Errors
    ///
    /// [`EventError::Config`] when credentials are set without TLS and
    /// without [`with_mqtt_allow_insecure`](Self::with_mqtt_allow_insecure).
    pub fn validate(&self) -> Result<(), EventError> {
        #[cfg(feature = "mqtt")]
        {
            self.mqtt().map_or(Ok(()), |mqtt| {
                mqtt.settings.validate().map_err(EventError::Config)
            })
        }
        #[cfg(not(feature = "mqtt"))]
        {
            Ok(())
        }
    }

    /// The the in-memory bus in-memory fan-out -- see the module doc for what it
    /// deliberately does not provide.
    #[must_use]
    pub fn mock() -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self::with_backend(Backend::Mock(sender))
    }

    /// Key the subscriber ids with `secret` instead of a random per-bus one.
    ///
    /// With the random default an id stops verifying when the process
    /// restarts, and is unknown to every other instance behind a load
    /// balancer, so a reconnecting client lands on a fresh session. A host
    /// that wants resume across restarts and instances gives every instance
    /// the same secret, kept like any signing key.
    ///
    /// # Panics
    ///
    /// If `secret` is shorter than [`MIN_SUBSCRIBER_SECRET_LEN`] bytes: a
    /// short or empty key (an unset environment variable read with a default,
    /// say) would make every id forgeable.
    #[must_use]
    pub fn with_subscriber_secret(mut self, secret: impl Into<Vec<u8>>) -> Self {
        let secret = secret.into();
        assert!(
            secret.len() >= MIN_SUBSCRIBER_SECRET_LEN,
            "the subscriber secret must be at least {MIN_SUBSCRIBER_SECRET_LEN} bytes"
        );
        self.secret = secret;
        self
    }

    /// Allow at most `max` concurrent connections that had to be issued a
    /// fresh subscriber id (default [`DEFAULT_MAX_ANONYMOUS_CONNECTIONS`]).
    #[must_use]
    pub fn with_max_anonymous_connections(mut self, max: usize) -> Self {
        self.max_anonymous = max;
        self
    }

    /// Bound abandoned durable sessions: durable subscribers
    /// ([`subscribe`](Self::subscribe)) connect over MQTT 5.0 with Clean Start
    /// off and a Session Expiry Interval of `ttl`, so the broker deletes a
    /// session, and the events queued in it, that long after its connection
    /// ends. A client that returns within `ttl` with its `subscriber_id` is
    /// replayed exactly as without a TTL; one that returns later gets a fresh
    /// session. The default is [`DEFAULT_SESSION_TTL`] (one hour);
    /// [`without_session_ttl`](Self::without_session_ttl) goes back to MQTT
    /// 3.1.1 and leaves expiry to the broker.
    ///
    /// Transient subscribers ([`subscribe_transient`](Self::subscribe_transient))
    /// and the publisher stay on MQTT 3.1.1, which the broker bridges to
    /// MQTT 5.0 subscribers. The broker must speak MQTT 5.0 and may cap the
    /// interval (Mosquitto's `max_session_expiry_interval`). The mock bus
    /// ignores the setting.
    ///
    /// # Panics
    ///
    /// If `ttl` is shorter than one second or longer than `u32::MAX` seconds,
    /// which a Session Expiry Interval cannot carry (zero would mean "expire
    /// on disconnect", which is what [`subscribe_transient`](Self::subscribe_transient)
    /// is for).
    #[must_use]
    pub fn with_session_ttl(self, ttl: Duration) -> Self {
        let seconds = u32::try_from(ttl.as_secs())
            .ok()
            .filter(|&seconds| seconds >= 1)
            .expect("the session TTL must be between 1 and 4294967295 seconds");
        #[cfg(feature = "mqtt")]
        let this = self.with_mqtt(|mqtt| mqtt.session_ttl = Some(seconds));
        #[cfg(not(feature = "mqtt"))]
        let this = {
            let _ = seconds;
            self
        };
        this
    }

    /// Leave the expiry of abandoned durable sessions to the broker: durable
    /// subscribers go back to MQTT 3.1.1 instead of the
    /// [`DEFAULT_SESSION_TTL`] over MQTT 5.0, for a broker that does not speak
    /// 5.0. Set the broker's own persistent-session expiry then. The in-memory
    /// bus ignores it.
    #[must_use]
    pub fn without_session_ttl(self) -> Self {
        #[cfg(feature = "mqtt")]
        let this = self.with_mqtt(|mqtt| mqtt.session_ttl = None);
        #[cfg(not(feature = "mqtt"))]
        let this = self;
        this
    }

    /// Allow at most `max` concurrent streams per caller on
    /// [`subscribe_for_caller`](Self::subscribe_for_caller) (default
    /// [`DEFAULT_MAX_STREAMS_PER_CALLER`]); `None` removes the limit.
    #[must_use]
    pub fn with_max_streams_per_caller(mut self, max: Option<usize>) -> Self {
        self.max_streams_per_caller = max;
        self
    }

    /// Allow at most `max` concurrent streams under one subscriber id on
    /// [`subscribe_for_caller`](Self::subscribe_for_caller) (default
    /// [`DEFAULT_MAX_STREAMS_PER_SUBSCRIBER`]); `None` removes the limit.
    #[must_use]
    pub fn with_max_streams_per_subscriber(mut self, max: Option<usize>) -> Self {
        self.max_streams_per_subscriber = max;
        self
    }

    /// Let [`subscribe_for_caller`](Self::subscribe_for_caller) (the HTTP
    /// events route) open *durable* sessions that a client resumes with its
    /// `subscriber_id` or `Last-Event-ID`. Without this call (the default) the
    /// route's streams are transient: a clean session, no replay, and a
    /// client-supplied id is ignored.
    #[must_use]
    pub fn with_durable_streams(mut self) -> Self {
        self.durable_streams = true;
        self
    }

    /// Whether [`with_durable_streams`](Self::with_durable_streams) was set.
    /// A host advertises replay in its manifest only when it is.
    #[must_use]
    pub fn durable_streams(&self) -> bool {
        self.durable_streams
    }

    /// Mint a subscriber id for `resource`: 128 random bits plus an HMAC tag
    /// binding them to the resource, 64 lowercase hex characters.
    #[must_use]
    pub fn mint_subscriber_id(&self, resource: &str) -> String {
        let nonce = random_token();
        let tag = subscriber_tag(&self.secret, resource, &nonce);
        format!("{nonce}{tag}")
    }

    /// `raw` (trimmed), if this bus minted it for `resource`.
    #[must_use]
    pub fn verify_subscriber_id(&self, resource: &str, raw: &str) -> Option<String> {
        let raw = raw.trim();
        if raw.len() != SUBSCRIBER_ID_LEN
            || !raw
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return None;
        }
        let (nonce, tag) = raw.split_at(2 * NONCE_BYTES);
        constant_time_eq(tag, &subscriber_tag(&self.secret, resource, nonce))
            .then(|| raw.to_string())
    }

    fn acquire_anonymous(&self) -> Result<AnonymousPermit, EventError> {
        self.anonymous
            .try_update(Ordering::AcqRel, Ordering::Acquire, |open| {
                (open < self.max_anonymous).then_some(open + 1)
            })
            .map_err(|_| EventError::TooManyAnonymousConnections)?;
        Ok(AnonymousPermit(Arc::clone(&self.anonymous)))
    }

    fn acquire_stream_slot(
        &self,
        caller: &str,
        subscriber_id: &str,
    ) -> Result<StreamSlot, EventError> {
        let keys = [
            format!("caller:{caller}"),
            format!("subscriber:{subscriber_id}"),
        ];
        let limits = [self.max_streams_per_caller, self.max_streams_per_subscriber];
        let mut counts = self
            .stream_counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let full = keys.iter().zip(limits).any(|(key, limit)| {
            limit.is_some_and(|max| counts.get(key).copied().unwrap_or(0) >= max)
        });
        if full {
            return Err(EventError::TooManyStreams);
        }
        for key in &keys {
            *counts.entry(key.clone()).or_insert(0) += 1;
        }
        drop(counts);
        Ok(StreamSlot {
            counts: Arc::clone(&self.stream_counts.0),
            keys,
        })
    }

    /// A handle that keeps [`shutdown`](Self::shutdown) waiting, or `None`
    /// once shutdown has begun.
    fn track(&self) -> Option<mpsc::Sender<()>> {
        self.inflight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Publish one event at `QoS` 1, non-retained. Failures are logged and
    /// counted ([`delivery_stats`](Self::delivery_stats)), never propagated:
    /// a CRUD write that already succeeded must not be reported to the
    /// caller as failed because a side-channel notification could not be
    /// delivered. That also means an event can be lost before it reaches the
    /// broker (see the module doc).
    ///
    /// On MQTT the event goes out over one connection shared by every
    /// publish, opened on first use. Waiting for room in its request queue is
    /// bounded; a publish that fails, times out or is cancelled discards that
    /// connection and the next publish opens a new one. After
    /// [`shutdown`](Self::shutdown) an event is dropped and counted.
    ///
    /// To announce a change only once its transaction has committed, record it
    /// in a [`PendingEvents`] and commit that instead of calling this
    /// directly.
    #[cfg_attr(
        not(feature = "mqtt"),
        expect(
            clippy::unused_async,
            clippy::unused_async_trait_impl,
            reason = "kept async so the signature is stable and the same with and without the `mqtt` feature"
        )
    )]
    pub async fn publish(&self, event: CrudEvent) {
        let Some(_inflight) = self.track() else {
            self.failed.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("dropped CRUD event, the event bus is shut down");
            return;
        };
        match &self.backend {
            #[cfg(feature = "mqtt")]
            Backend::Mqtt(mqtt) => match mqtt.send(&event).await {
                Ok(()) => {
                    self.published.fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => {
                    self.failed.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!("dropped CRUD event, not queued for the broker: {err}");
                }
            },
            Backend::Mock(sender) => {
                // `send` errors only when there are no receivers -- an idle
                // stream with nobody watching, not a failure.
                let _ = sender.send(event);
                self.published.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// [`publish`](Self::publish) each of `events`, in order.
    pub async fn publish_all(&self, events: Vec<CrudEvent>) {
        for event in events {
            self.publish(event).await;
        }
    }

    /// How many events [`publish`](Self::publish) has queued and dropped so
    /// far.
    #[must_use]
    pub fn delivery_stats(&self) -> DeliveryStats {
        DeliveryStats {
            published: self.published.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }

    /// Stop accepting events, wait (bounded) for the publishes in flight, then
    /// disconnect the publisher, giving it a moment to flush its queue. Call it
    /// when the host shuts down; open [`EventStream`]s are unaffected (end them
    /// with `event_stream_until`). Later publishes are dropped and counted.
    /// Calling it again is harmless.
    pub async fn shutdown(&self) {
        // Dropping the bus's own sender is what lets the receiver see the end
        // once every publish in flight has dropped its clone.
        drop(
            self.inflight
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        );
        let receiver = self
            .drained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let drained = match receiver {
            Some(mut receiver) => tokio::time::timeout(DRAIN_TIMEOUT, receiver.recv())
                .await
                .is_ok(),
            None => true,
        };
        if !drained {
            tracing::warn!("event bus shutdown: publishes still in flight were not awaited");
        }
        match &self.backend {
            #[cfg(feature = "mqtt")]
            Backend::Mqtt(mqtt) => mqtt.close().await,
            Backend::Mock(_) => {}
        }
    }

    /// Open a subscriber's stream of `resource` events, returning the
    /// subscriber id it runs under.
    ///
    /// `requested` is the id a client handed back. It is resumed only if
    /// [`verify_subscriber_id`](Self::verify_subscriber_id) accepts it for
    /// `resource`; otherwise (absent or not minted here for this resource) a
    /// fresh one is minted, counting against the anonymous-connection cap.
    ///
    /// On the MQTT path this opens (or *resumes*) that subscriber's own
    /// persistent session and subscribes at `QoS` 1 -- everything the broker
    /// queued for the same client id while it was away is delivered before
    /// anything new. It returns only once the broker has accepted both, so a
    /// broker that is down is an error here and not a stream that never
    /// delivers.
    ///
    /// # Errors
    ///
    /// [`EventError::Broker`] when the broker is unreachable or refuses the
    /// session or subscription; [`EventError::TooManyAnonymousConnections`]
    /// when a fresh id is needed and the cap is reached.
    pub async fn subscribe(
        &self,
        resource: &str,
        requested: Option<&str>,
    ) -> Result<(String, EventStream), EventError> {
        self.open(None, resource, requested, true).await
    }

    /// [`subscribe`](Self::subscribe) as the HTTP events route uses it, for a
    /// `caller` the host identifies (the owner scope's value; callers it cannot
    /// tell apart share one key and so one budget).
    ///
    /// The stream is *transient* unless the bus was built
    /// [`with_durable_streams`](Self::with_durable_streams), and then
    /// `requested` is ignored (a clean-session connect under a durable id
    /// would discard that session). The stream holds one of the caller's
    /// [`DEFAULT_MAX_STREAMS_PER_CALLER`] slots and one of its subscriber id's
    /// until it is dropped.
    ///
    /// # Errors
    ///
    /// As [`subscribe`](Self::subscribe), plus
    /// [`EventError::TooManyStreams`] when the caller or the subscriber id is
    /// at its limit, and [`EventError::Config`] for refused MQTT settings.
    pub async fn subscribe_for_caller(
        &self,
        caller: &str,
        resource: &str,
        requested: Option<&str>,
    ) -> Result<(String, EventStream), EventError> {
        let requested = requested.filter(|_| self.durable_streams);
        self.open(Some(caller), resource, requested, self.durable_streams)
            .await
    }

    /// Like [`subscribe`](Self::subscribe), but *transient*: on the MQTT path
    /// the connection uses a clean session, so the broker drops the session
    /// when the connection ends (cleanly or by crash) and a restarting
    /// consumer orphans nothing. Nothing queued while it is away is
    /// replayed. On the mock bus it behaves like [`subscribe`](Self::subscribe).
    ///
    /// Pass `None` for `requested` unless the id belongs to a transient
    /// subscriber: a clean-session connect under an id a durable subscriber
    /// uses discards that subscriber's stored session.
    ///
    /// # Errors
    ///
    /// As [`subscribe`](Self::subscribe).
    pub async fn subscribe_transient(
        &self,
        resource: &str,
        requested: Option<&str>,
    ) -> Result<(String, EventStream), EventError> {
        self.open(None, resource, requested, false).await
    }

    #[cfg_attr(
        not(feature = "mqtt"),
        expect(
            clippy::unused_async,
            clippy::unused_async_trait_impl,
            reason = "the signature is the same with and without the `mqtt` feature"
        )
    )]
    async fn open(
        &self,
        caller: Option<&str>,
        resource: &str,
        requested: Option<&str>,
        durable: bool,
    ) -> Result<(String, EventStream), EventError> {
        #[cfg(not(feature = "mqtt"))]
        let _ = durable;
        let (subscriber_id, permit) =
            match requested.and_then(|raw| self.verify_subscriber_id(resource, raw)) {
                Some(resumed) => (resumed, None),
                None => (
                    self.mint_subscriber_id(resource),
                    Some(self.acquire_anonymous()?),
                ),
            };
        let slot = caller
            .map(|caller| self.acquire_stream_slot(caller, &subscriber_id))
            .transpose()?;
        let inner = match &self.backend {
            #[cfg(feature = "mqtt")]
            Backend::Mqtt(mqtt) => open_mqtt(mqtt, resource, &subscriber_id, durable).await?,
            Backend::Mock(sender) => Inner::Mock {
                receiver: sender.subscribe(),
                resource: resource.to_string(),
            },
        };
        Ok((
            subscriber_id,
            EventStream {
                inner,
                _permit: permit,
                _slot: slot,
            },
        ))
    }
}

/// Open one subscriber's MQTT session (persistent when `durable`, over MQTT
/// 5.0 with an expiry when `session_ttl` is set too) and wait for the broker
/// to accept it and the subscription.
#[cfg(feature = "mqtt")]
async fn open_mqtt(
    mqtt: &MqttBackend,
    resource: &str,
    subscriber_id: &str,
    durable: bool,
) -> Result<Inner, EventError> {
    let security = mqtt.settings.security().await.map_err(EventError::Config)?;
    let (host, port) = (mqtt.host.as_str(), mqtt.port);
    let client_id = client_id_for(resource, subscriber_id);
    let (client, mut eventloop): (SubscriberClient, Box<dyn MqttPoll>) =
        if let Some(ttl) = mqtt.session_ttl.filter(|_| durable) {
            let mut options = mqtt5::MqttOptions::new(client_id, host, port);
            secure_v5(&mut options, &security);
            options.set_keep_alive(KEEP_ALIVE);
            // Clean Start 0 keeps the stored session; the interval is what
            // makes the broker drop it after `ttl` once the connection ends.
            options.set_clean_start(false);
            options.set_session_expiry_interval(Some(ttl));
            let (client, eventloop) = mqtt5::AsyncClient::new(options, CHANNEL_CAPACITY);
            (SubscriberClient::V5(client), Box::new(eventloop))
        } else {
            let mut options = MqttOptions::new(client_id, host, port);
            secure_v3(&mut options, &security);
            options.set_keep_alive(KEEP_ALIVE);
            // The entire delivery guarantee: a persistent session keyed by
            // this subscriber's own client id. The client id is never empty
            // (it always carries the minted id), which is what keeps this
            // call (which panics on an empty client id) safe. A transient
            // subscriber asks for a clean session instead, which the broker
            // discards when the connection ends.
            options.set_clean_session(!durable);
            let (client, eventloop) = AsyncClient::new(options, CHANNEL_CAPACITY);
            (SubscriberClient::V3(client), Box::new(eventloop))
        };
    subscribe_to(&client, topic(resource)).await?;
    let backlog = await_subscription(&mut *eventloop, SUBSCRIBE_TIMEOUT).await?;
    Ok(Inner::Mqtt {
        // Kept alive alongside the event loop: dropping the client closes
        // the request channel and ends the loop.
        client,
        topic: topic(resource),
        eventloop,
        errors: ErrorRun::default(),
        backlog,
    })
}

/// Events recorded while a database transaction runs, announced only if it
/// commits.
///
/// Publishing inside the transaction announces changes that may still roll
/// back, and holds the request on the broker while the transaction is open.
/// Instead, a handler records each event here ([`record`](Self::record)) as
/// its write succeeds, mirrors its SQL savepoints with
/// [`savepoint`](Self::savepoint) / [`rollback_to`](Self::rollback_to) /
/// [`release`](Self::release), and calls [`commit`](Self::commit) once, after
/// the *outermost* database commit succeeded.
///
/// - A savepoint release keeps its events (they belong to the enclosing
///   transaction); a rollback to a savepoint drops only the events recorded
///   after it.
/// - Dropping the value, or calling [`rollback`](Self::rollback), without
///   committing discards everything, which is what a transaction that ends
///   without a commit must do.
/// - Delivery is best effort ([`EventBus::publish`]): a crash between the
///   database commit and [`commit`](Self::commit) loses the events, and a
///   client that needs completeness refetches on reconnect.
#[derive(Debug, Default)]
pub struct PendingEvents {
    events: Vec<CrudEvent>,
    /// The number of events recorded when each open savepoint was taken.
    marks: Vec<usize>,
}

/// A position in a [`PendingEvents`], returned by
/// [`savepoint`](PendingEvents::savepoint).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Savepoint {
    depth: usize,
}

impl PendingEvents {
    /// An empty queue for a new transaction.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an event of the current transaction.
    pub fn record(&mut self, event: CrudEvent) {
        self.events.push(event);
    }

    /// The events recorded so far, oldest first.
    #[must_use]
    pub fn events(&self) -> &[CrudEvent] {
        &self.events
    }

    /// Mirror `SAVEPOINT`: remember where the queue stands.
    pub fn savepoint(&mut self) -> Savepoint {
        self.marks.push(self.events.len());
        Savepoint {
            depth: self.marks.len() - 1,
        }
    }

    /// Mirror `RELEASE SAVEPOINT`: the savepoint and any taken after it are
    /// gone, and their events stay in the transaction. A savepoint that is
    /// already gone changes nothing.
    pub fn release(&mut self, savepoint: Savepoint) {
        self.marks.truncate(savepoint.depth);
    }

    /// Mirror `ROLLBACK TO SAVEPOINT`: drop the events recorded since the
    /// savepoint. Savepoints taken after it are gone; it stays usable, as in
    /// SQL. A savepoint that is already gone changes nothing.
    pub fn rollback_to(&mut self, savepoint: Savepoint) {
        if let Some(&mark) = self.marks.get(savepoint.depth) {
            self.events.truncate(mark);
            self.marks.truncate(savepoint.depth + 1);
        }
    }

    /// The transaction rolled back: discard every event. (Dropping the value
    /// does the same.)
    pub fn rollback(self) {
        drop(self);
    }

    /// The outermost commit succeeded: publish every event, in order, with
    /// [`EventBus::publish_all`]. Open savepoints are part of the commit.
    pub async fn commit(self, bus: &EventBus) {
        bus.publish_all(self.events).await;
    }
}

/// One subscriber's live event feed. Dropping it disconnects that
/// subscriber's MQTT session -- which the broker keeps (and queues into)
/// when the session is persistent -- and frees its anonymous-connection and
/// stream-limit slots.
pub struct EventStream {
    inner: Inner,
    _permit: Option<AnonymousPermit>,
    _slot: Option<StreamSlot>,
}

enum Inner {
    #[cfg(feature = "mqtt")]
    Mqtt {
        client: SubscriberClient,
        /// Subscribed again whenever the broker reports a reconnect without
        /// the session (a clean-session stream never has one to resume).
        topic: String,
        eventloop: Box<dyn MqttPoll>,
        errors: ErrorRun,
        /// Events decoded while the subscription was being set up.
        backlog: VecDeque<CrudEvent>,
    },
    Mock {
        receiver: broadcast::Receiver<CrudEvent>,
        resource: String,
    },
}

/// The current run of consecutive broker errors on one subscriber
/// connection.
#[cfg(feature = "mqtt")]
#[derive(Default)]
struct ErrorRun {
    count: u32,
    last: Option<Instant>,
}

#[cfg(feature = "mqtt")]
impl ErrorRun {
    /// A poll succeeded. The run ends only once the connection has stayed
    /// error-free for [`HEALTHY_AFTER`]: a reconnect that is dropped again at
    /// once (a flapping broker) must keep counting.
    fn succeeded(&mut self) {
        if self.last.is_some_and(|at| at.elapsed() >= HEALTHY_AFTER) {
            *self = Self::default();
        }
    }

    /// A poll failed; the run's length so far.
    fn failed(&mut self) -> u32 {
        self.count += 1;
        self.last = Some(Instant::now());
        self.count
    }
}

impl EventStream {
    /// The next event, or `None` once the feed is finished (the broker kept
    /// failing for `MAX_CONSECUTIVE_POLL_ERRORS` attempts without a
    /// `HEALTHY_AFTER` stretch of quiet in between, or -- under the
    /// in-memory bus -- the bus itself was dropped). After a durable
    /// [`subscribe`](EventBus::subscribe) stream ends, the client reconnects
    /// with the same `subscriber_id` and resumes the same persistent session;
    /// a transient stream has no session to resume, so its owner just
    /// subscribes again.
    ///
    /// When the broker reconnects without a session, the stream subscribes
    /// again by itself (and ends if it cannot); events published during the
    /// outage are lost.
    pub async fn next_event(&mut self) -> Option<CrudEvent> {
        match &mut self.inner {
            #[cfg(feature = "mqtt")]
            Inner::Mqtt {
                client,
                topic,
                eventloop,
                errors,
                backlog,
            } => {
                if let Some(event) = backlog.pop_front() {
                    return Some(event);
                }
                loop {
                    match eventloop.poll().await {
                        Ok(Polled::Publish(payload)) => {
                            errors.succeeded();
                            if let Some(event) = decode_event(&payload) {
                                return Some(event);
                            }
                        }
                        // A reconnect the broker has no session for (a
                        // transient stream, or a durable session that
                        // expired) lost the subscription with it, so ask for
                        // it again or the stream would stay quiet for good.
                        Ok(Polled::ConnAck {
                            session_present: false,
                        }) => {
                            errors.succeeded();
                            if let Err(err) = client.try_subscribe(topic.clone()) {
                                // Without the subscription the stream would stay
                                // quiet for good; ending it tells the owner.
                                tracing::warn!("mqtt resubscribe failed: {err}");
                                return None;
                            }
                        }
                        // ConnAck/SubAck/PingResp/outgoing acks -- the
                        // protocol turning over, not an event.
                        Ok(_) => errors.succeeded(),
                        Err(err) => {
                            let run = errors.failed();
                            tracing::warn!("mqtt subscriber connection error ({run}): {err}");
                            if run >= MAX_CONSECUTIVE_POLL_ERRORS {
                                return None;
                            }
                            tokio::time::sleep(POLL_ERROR_BACKOFF).await;
                        }
                    }
                }
            }
            Inner::Mock {
                receiver, resource, ..
            } => loop {
                match receiver.recv().await {
                    // One broadcast channel carries every resource, unlike
                    // MQTT's per-resource topic, so filter here.
                    Ok(event) if event.resource == *resource => return Some(event),
                    Ok(_) => {}
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
        assert_eq!(topic("widgets"), "crud-events/widgets");
    }

    #[cfg(feature = "mqtt")]
    #[test]
    fn client_id_is_derived_from_the_resource_and_the_subscriber_id() {
        assert_eq!(
            client_id_for("widgets", "abc123"),
            "crud-events-widgets-abc123"
        );
        assert_ne!(
            client_id_for("widgets", "abc123"),
            client_id_for("gadgets", "abc123")
        );
    }

    #[test]
    fn a_minted_subscriber_id_verifies_for_its_own_resource_only() {
        let bus = EventBus::mock();
        let id = bus.mint_subscriber_id("widgets");
        assert_eq!(id.len(), SUBSCRIBER_ID_LEN);
        assert_eq!(
            bus.verify_subscriber_id("widgets", &id).as_deref(),
            Some(id.as_str())
        );
        // Scoped per resource: the same id is worthless under another one.
        assert_eq!(bus.verify_subscriber_id("gadgets", &id), None);
    }

    #[test]
    fn minted_subscriber_ids_are_unique() {
        let bus = EventBus::mock();
        assert_ne!(
            bus.mint_subscriber_id("widgets"),
            bus.mint_subscriber_id("widgets")
        );
    }

    #[test]
    fn a_client_chosen_or_low_entropy_id_is_never_accepted() {
        let bus = EventBus::mock();
        for raw in [
            "",
            "   ",
            "victim",
            "sub-1",
            "a/b",
            "héro",
            &"0".repeat(SUBSCRIBER_ID_LEN),
            &"a".repeat(SUBSCRIBER_ID_LEN),
            &"A".repeat(SUBSCRIBER_ID_LEN),
            &"a".repeat(SUBSCRIBER_ID_LEN + 1),
        ] {
            assert_eq!(
                bus.verify_subscriber_id("widgets", raw),
                None,
                "{raw:?} must be rejected"
            );
        }
    }

    #[test]
    fn a_tampered_subscriber_id_is_rejected() {
        let bus = EventBus::mock();
        let id = bus.mint_subscriber_id("widgets");
        // Same nonce, tag of another resource.
        let other = bus.mint_subscriber_id("gadgets");
        let spliced = format!("{}{}", &id[..2 * NONCE_BYTES], &other[2 * NONCE_BYTES..]);
        assert_eq!(bus.verify_subscriber_id("widgets", &spliced), None);
        // One flipped character in the tag.
        let mut flipped = id.clone().into_bytes();
        let last = flipped.len() - 1;
        flipped[last] = if flipped[last] == b'0' { b'1' } else { b'0' };
        let flipped = String::from_utf8(flipped).unwrap();
        assert_eq!(bus.verify_subscriber_id("widgets", &flipped), None);
    }

    #[test]
    fn a_subscriber_id_from_another_bus_secret_is_rejected() {
        let ours = EventBus::mock();
        let theirs = EventBus::mock();
        let id = theirs.mint_subscriber_id("widgets");
        assert_eq!(ours.verify_subscriber_id("widgets", &id), None);
    }

    #[test]
    fn a_shared_secret_makes_ids_portable_between_buses() {
        let a = EventBus::mock().with_subscriber_secret(b"k".repeat(32));
        let b = EventBus::mock().with_subscriber_secret(b"k".repeat(32));
        let id = a.mint_subscriber_id("widgets");
        assert!(b.verify_subscriber_id("widgets", &id).is_some());
    }

    #[test]
    #[should_panic(expected = "at least 32 bytes")]
    fn a_short_subscriber_secret_is_refused() {
        let _ = EventBus::mock().with_subscriber_secret(Vec::new());
    }

    #[test]
    fn verify_trims_surrounding_whitespace() {
        let bus = EventBus::mock();
        let id = bus.mint_subscriber_id("widgets");
        assert_eq!(
            bus.verify_subscriber_id("widgets", &format!("  {id} "))
                .as_deref(),
            Some(id.as_str())
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
        let event = CrudEvent::new("widgets", EventAction::DeleteMany, vec![1, 2, 3]);
        let encoded = serde_json::to_vec(&event).unwrap();
        let decoded: CrudEvent = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, event);
        assert_eq!(decoded.resource, "widgets");
        assert_eq!(decoded.ids, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn mock_bus_fans_an_event_out_to_a_subscriber() {
        let bus = EventBus::mock();
        let (_, mut stream) = bus.subscribe("widgets", None).await.unwrap();
        bus.publish(CrudEvent::new("widgets", EventAction::Create, vec![7]))
            .await;
        let event = stream.next_event().await.unwrap();
        assert_eq!(event.action, EventAction::Create);
        assert_eq!(event.ids, vec![7]);
    }

    #[tokio::test]
    async fn mock_bus_delivers_to_every_open_subscriber() {
        let bus = EventBus::mock();
        let (_, mut a) = bus.subscribe("widgets", None).await.unwrap();
        let (_, mut b) = bus.subscribe("widgets", None).await.unwrap();
        bus.publish(CrudEvent::new("widgets", EventAction::Update, vec![1]))
            .await;
        assert_eq!(a.next_event().await.unwrap().ids, vec![1]);
        assert_eq!(b.next_event().await.unwrap().ids, vec![1]);
    }

    #[tokio::test]
    async fn mock_bus_filters_events_for_other_resources() {
        let bus = EventBus::mock();
        let (_, mut stream) = bus.subscribe("widgets", None).await.unwrap();
        bus.publish(CrudEvent::new("gadgets", EventAction::Create, vec![1]))
            .await;
        bus.publish(CrudEvent::new("widgets", EventAction::Create, vec![2]))
            .await;
        let event = stream.next_event().await.unwrap();
        assert_eq!(event.resource, "widgets");
        assert_eq!(event.ids, vec![2]);
    }

    #[tokio::test]
    async fn mock_bus_publish_with_no_subscribers_is_a_no_op() {
        let bus = EventBus::mock();
        bus.publish(CrudEvent::new("widgets", EventAction::Create, vec![1]))
            .await;
    }

    #[tokio::test]
    async fn a_mock_stream_ends_once_the_bus_is_dropped() {
        let bus = EventBus::mock();
        let (_, mut stream) = bus.subscribe("widgets", None).await.unwrap();
        drop(bus);
        assert!(stream.next_event().await.is_none());
    }

    #[tokio::test]
    async fn a_lagging_mock_subscriber_keeps_going_after_dropping_events() {
        let bus = EventBus::mock();
        let (_, mut stream) = bus.subscribe("widgets", None).await.unwrap();
        // Overrun the fan-out buffer without reading, then confirm the
        // stream resumes rather than ending.
        for id in 0..(i32::try_from(CHANNEL_CAPACITY).unwrap() + 5) {
            bus.publish(CrudEvent::new("widgets", EventAction::Create, vec![id]))
                .await;
        }
        let event = stream.next_event().await.unwrap();
        assert_eq!(event.action, EventAction::Create);
    }

    /// Returns a scripted sequence of poll results -- the only
    /// deterministic way to drive `EventStream::next_event`'s MQTT
    /// backoff/reconnect-limit branches, which only run against a broken
    /// broker connection (see `MqttPoll`'s own doc comment).
    #[cfg(feature = "mqtt")]
    struct ScriptedEventLoop {
        responses: std::collections::VecDeque<Result<Event, ConnectionError>>,
    }

    #[cfg(feature = "mqtt")]
    impl ScriptedEventLoop {
        fn new(responses: Vec<Result<Event, ConnectionError>>) -> Self {
            Self {
                responses: responses.into(),
            }
        }
    }

    #[cfg(feature = "mqtt")]
    #[async_trait::async_trait]
    impl MqttPoll for ScriptedEventLoop {
        async fn poll(&mut self) -> Result<Polled, String> {
            polled_v3(
                self.responses
                    .pop_front()
                    .expect("ScriptedEventLoop's scripted responses were exhausted"),
            )
        }
    }

    #[cfg(feature = "mqtt")]
    fn mqtt_stream_with(responses: Vec<Result<Event, ConnectionError>>) -> EventStream {
        let options = MqttOptions::new("test-client", "localhost", 1883);
        let (client, _real_eventloop) = AsyncClient::new(options, CHANNEL_CAPACITY);
        EventStream {
            inner: Inner::Mqtt {
                client: SubscriberClient::V3(client),
                topic: topic("widgets"),
                eventloop: Box::new(ScriptedEventLoop::new(responses)),
                errors: ErrorRun::default(),
                backlog: VecDeque::new(),
            },
            _permit: None,
            _slot: None,
        }
    }

    #[cfg(feature = "mqtt")]
    fn publish_event(event: &CrudEvent) -> Event {
        Event::Incoming(Packet::Publish(rumqttc::Publish::new(
            topic(&event.resource),
            QoS::AtLeastOnce,
            serde_json::to_vec(event).unwrap(),
        )))
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn mqtt_stream_backs_off_and_recovers_from_fewer_than_max_consecutive_errors() {
        let event = CrudEvent::new("widgets", EventAction::Create, vec![9]);
        let mut stream = mqtt_stream_with(vec![
            Err(ConnectionError::NetworkTimeout),
            Err(ConnectionError::NetworkTimeout),
            Ok(publish_event(&event)),
        ]);
        let received = stream.next_event().await.unwrap();
        assert_eq!(received.ids, vec![9]);
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn mqtt_stream_skips_an_undecodable_payload_and_protocol_acks() {
        let event = CrudEvent::new("widgets", EventAction::Create, vec![3]);
        let garbage = Event::Incoming(Packet::Publish(rumqttc::Publish::new(
            topic("widgets"),
            QoS::AtLeastOnce,
            b"not json".to_vec(),
        )));
        let mut stream = mqtt_stream_with(vec![
            Ok(Event::Incoming(Packet::PingResp)),
            Ok(garbage),
            Ok(publish_event(&event)),
        ]);
        assert_eq!(stream.next_event().await.unwrap().ids, vec![3]);
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn mqtt_stream_ends_after_max_consecutive_poll_errors() {
        let mut responses = Vec::with_capacity(MAX_CONSECUTIVE_POLL_ERRORS as usize);
        for _ in 0..MAX_CONSECUTIVE_POLL_ERRORS {
            responses.push(Err(ConnectionError::NetworkTimeout));
        }
        let mut stream = mqtt_stream_with(responses);
        assert!(stream.next_event().await.is_none());
    }

    /// A scripted error and a healthy poll, then pends forever -- for
    /// `drive_publisher_eventloop`'s infinite loop, which a test can only
    /// ever observe a bounded prefix of.
    #[cfg(feature = "mqtt")]
    struct FirstErrorThenPendingForever(std::collections::VecDeque<Result<Event, ConnectionError>>);

    #[cfg(feature = "mqtt")]
    #[async_trait::async_trait]
    impl MqttPoll for FirstErrorThenPendingForever {
        async fn poll(&mut self) -> Result<Polled, String> {
            match self.0.pop_front() {
                Some(result) => polled_v3(result),
                None => std::future::pending().await,
            }
        }
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn subscribing_while_the_broker_is_down_is_an_error() {
        // Nothing listens on port 1: the connection is refused, which must
        // surface here (-> 503) and not as a stream that never delivers.
        let bus = EventBus::connect("127.0.0.1", 1).without_session_ttl();
        let err = bus.subscribe("widgets", None).await.err().unwrap();
        assert!(matches!(err, EventError::Broker(_)), "{err}");
        // The failed attempt gave its anonymous slot back.
        assert_eq!(bus.anonymous.load(Ordering::Acquire), 0);
    }

    #[cfg(feature = "mqtt")]
    fn suback(code: SubscribeReasonCode) -> Event {
        Event::Incoming(Packet::SubAck(rumqttc::SubAck::new(1, vec![code])))
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn the_handshake_waits_for_the_suback_and_keeps_replayed_events() {
        let replayed = CrudEvent::new("widgets", EventAction::Update, vec![5]);
        let garbage = Event::Incoming(Packet::Publish(rumqttc::Publish::new(
            topic("widgets"),
            QoS::AtLeastOnce,
            b"not json".to_vec(),
        )));
        let mut eventloop = ScriptedEventLoop::new(vec![
            Ok(Event::Incoming(Packet::PingResp)),
            // A resumed session delivers its queue before the SUBACK.
            Ok(publish_event(&replayed)),
            Ok(garbage),
            Ok(suback(SubscribeReasonCode::Success(QoS::AtLeastOnce))),
        ]);
        let backlog = await_subscription(&mut eventloop, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(backlog, VecDeque::from([replayed]));
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn the_handshake_fails_on_a_refused_subscription() {
        let mut eventloop = ScriptedEventLoop::new(vec![Ok(suback(SubscribeReasonCode::Failure))]);
        let err = await_subscription(&mut eventloop, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(err, EventError::Broker(msg) if msg.contains("refused")));
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn the_handshake_fails_on_a_connection_error() {
        let mut eventloop = ScriptedEventLoop::new(vec![Err(ConnectionError::NetworkTimeout)]);
        let err = await_subscription(&mut eventloop, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(err, EventError::Broker(_)));
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn the_handshake_times_out_on_a_silent_broker() {
        let mut eventloop = FirstErrorThenPendingForever(VecDeque::new());
        let err = await_subscription(&mut eventloop, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert!(matches!(err, EventError::Broker(msg) if msg.contains("timed out")));
    }

    /// A fake whose every answer arrives after a scripted pause.
    #[cfg(feature = "mqtt")]
    struct SlowEventLoop(VecDeque<(Duration, Result<Event, ConnectionError>)>);

    #[cfg(feature = "mqtt")]
    #[async_trait::async_trait]
    impl MqttPoll for SlowEventLoop {
        async fn poll(&mut self) -> Result<Polled, String> {
            let (pause, result) = self.0.pop_front().expect("SlowEventLoop script exhausted");
            tokio::time::sleep(pause).await;
            polled_v3(result)
        }
    }

    #[cfg(feature = "mqtt")]
    fn conn_ack() -> Event {
        conn_ack_with(false)
    }

    #[cfg(feature = "mqtt")]
    fn conn_ack_with(session_present: bool) -> Event {
        Event::Incoming(Packet::ConnAck(rumqttc::ConnAck::new(
            rumqttc::ConnectReturnCode::Success,
            session_present,
        )))
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn a_flapping_broker_ends_the_stream_even_though_it_keeps_acking_connections() {
        // error, CONNACK, error, CONNACK, ...: every reconnect succeeds and
        // is dropped again. A CONNACK must not reset the error count.
        let mut script = Vec::new();
        for _ in 0..MAX_CONSECUTIVE_POLL_ERRORS {
            script.push(Err(ConnectionError::NetworkTimeout));
            script.push(Ok(conn_ack()));
        }
        let mut stream = mqtt_stream_with(script);
        assert!(stream.next_event().await.is_none());
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn errors_stop_counting_after_a_healthy_stretch() {
        let event = CrudEvent::new("widgets", EventAction::Create, vec![4]);
        let none = Duration::ZERO;
        let mut script = VecDeque::new();
        for _ in 0..(MAX_CONSECUTIVE_POLL_ERRORS - 1) {
            script.push_back((none, Err(ConnectionError::NetworkTimeout)));
        }
        // Quiet for longer than HEALTHY_AFTER, then a healthy poll: the run
        // is over, so another MAX - 1 errors do not end the stream.
        script.push_back((HEALTHY_AFTER * 2, Ok(Event::Incoming(Packet::PingResp))));
        for _ in 0..(MAX_CONSECUTIVE_POLL_ERRORS - 1) {
            script.push_back((none, Err(ConnectionError::NetworkTimeout)));
        }
        script.push_back((none, Ok(publish_event(&event))));
        let (client, _real) = AsyncClient::new(MqttOptions::new("t", "localhost", 1883), 1);
        let mut stream = EventStream {
            inner: Inner::Mqtt {
                client: SubscriberClient::V3(client),
                topic: topic("widgets"),
                eventloop: Box::new(SlowEventLoop(script)),
                errors: ErrorRun::default(),
                backlog: VecDeque::new(),
            },
            _permit: None,
            _slot: None,
        };
        assert_eq!(stream.next_event().await.unwrap().ids, vec![4]);
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn events_buffered_during_the_handshake_come_out_first() {
        let buffered = CrudEvent::new("widgets", EventAction::Create, vec![1]);
        let live = CrudEvent::new("widgets", EventAction::Create, vec![2]);
        let (client, _real) = AsyncClient::new(MqttOptions::new("t", "localhost", 1883), 1);
        let mut stream = EventStream {
            inner: Inner::Mqtt {
                client: SubscriberClient::V3(client),
                topic: topic("widgets"),
                eventloop: Box::new(ScriptedEventLoop::new(vec![Ok(publish_event(&live))])),
                errors: ErrorRun::default(),
                backlog: VecDeque::from([buffered]),
            },
            _permit: None,
            _slot: None,
        };
        assert_eq!(stream.next_event().await.unwrap().ids, vec![1]);
        assert_eq!(stream.next_event().await.unwrap().ids, vec![2]);
    }

    #[tokio::test]
    async fn a_valid_id_is_resumed_and_anything_else_gets_a_fresh_one() {
        let bus = EventBus::mock();
        let issued = bus.mint_subscriber_id("widgets");
        let (resumed, _stream) = bus.subscribe("widgets", Some(&issued)).await.unwrap();
        assert_eq!(resumed, issued);

        // A client-chosen id is not taken over; a fresh one is minted.
        let (fresh, _stream) = bus.subscribe("widgets", Some("victim")).await.unwrap();
        assert_ne!(fresh, "victim");
        assert!(bus.verify_subscriber_id("widgets", &fresh).is_some());

        // An id minted for another resource is not valid here either.
        let other = bus.mint_subscriber_id("gadgets");
        let (fresh, _stream) = bus.subscribe("widgets", Some(&other)).await.unwrap();
        assert_ne!(fresh, other);
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn a_reconnect_without_a_session_subscribes_again_and_keeps_streaming() {
        let event = CrudEvent::new("widgets", EventAction::Create, vec![4]);
        let script = || {
            vec![
                Ok(conn_ack_with(true)),
                Ok(conn_ack_with(false)),
                Ok(publish_event(&event)),
            ]
        };

        // Request queue open: the resubscribe is queued.
        let options = MqttOptions::new("test-client", "localhost", 1883);
        let (client, _requests) = AsyncClient::new(options, CHANNEL_CAPACITY);
        let mut stream = EventStream {
            inner: Inner::Mqtt {
                client: SubscriberClient::V3(client),
                topic: topic("widgets"),
                eventloop: Box::new(ScriptedEventLoop::new(script())),
                errors: ErrorRun::default(),
                backlog: VecDeque::new(),
            },
            _permit: None,
            _slot: None,
        };
        assert_eq!(stream.next_event().await.unwrap().ids, vec![4]);

        // Request queue closed: the failed resubscribe ends the stream.
        let mut stream = mqtt_stream_with(script());
        assert!(stream.next_event().await.is_none());
    }

    #[tokio::test]
    async fn a_transient_mock_subscriber_receives_events_like_a_durable_one() {
        let bus = EventBus::mock();
        let (id, mut stream) = bus.subscribe_transient("widgets", None).await.unwrap();
        assert!(bus.verify_subscriber_id("widgets", &id).is_some());
        bus.publish(CrudEvent::new("widgets", EventAction::Create, vec![3]))
            .await;
        assert_eq!(stream.next_event().await.unwrap().ids, vec![3]);
    }

    #[tokio::test]
    async fn anonymous_connections_are_capped_and_resuming_ones_are_not() {
        let bus = EventBus::mock().with_max_anonymous_connections(2);
        let (id, first) = bus.subscribe("widgets", None).await.unwrap();
        let (_, _second) = bus.subscribe("widgets", None).await.unwrap();
        let err = bus.subscribe("widgets", None).await.err().unwrap();
        assert!(matches!(err, EventError::TooManyAnonymousConnections));
        // A forged id is anonymous too.
        assert!(bus.subscribe("widgets", Some("victim")).await.is_err());
        // Resuming with the issued id is free.
        assert!(bus.subscribe("widgets", Some(&id)).await.is_ok());
        // Dropping an anonymous stream frees its slot.
        drop(first);
        assert!(bus.subscribe("widgets", None).await.is_ok());
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn drive_publisher_eventloop_logs_and_backs_off_on_a_poll_error() {
        let eventloop = FirstErrorThenPendingForever(
            [
                Err(ConnectionError::NetworkTimeout),
                Ok(Event::Incoming(Packet::PingResp)),
            ]
            .into(),
        );
        // The loop never returns on its own; give it just long enough to
        // take the error branch and then a healthy poll before cutting it off.
        let _ = tokio::time::timeout(
            Duration::from_millis(50),
            drive_publisher_eventloop(eventloop),
        )
        .await;
    }

    #[cfg(feature = "mqtt")]
    fn client_with_closed_request_channel() -> AsyncClient {
        let options = MqttOptions::new("test-closed", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options, CHANNEL_CAPACITY);
        drop(eventloop);
        client
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn subscribe_to_reports_a_closed_request_channel_as_a_broker_error() {
        let client = client_with_closed_request_channel();
        let err = subscribe_to(&SubscriberClient::V3(client), "t".to_string())
            .await
            .unwrap_err();
        assert!(matches!(err, EventError::Broker(_)));
    }

    #[test]
    #[should_panic(expected = "between 1 and 4294967295 seconds")]
    fn a_session_ttl_under_one_second_is_refused() {
        let _ = EventBus::mock().with_session_ttl(Duration::from_millis(999));
    }

    #[test]
    #[should_panic(expected = "between 1 and 4294967295 seconds")]
    fn a_session_ttl_beyond_the_expiry_interval_range_is_refused() {
        let _ = EventBus::mock().with_session_ttl(Duration::from_secs(u64::from(u32::MAX) + 1));
    }

    #[tokio::test]
    async fn the_mock_bus_ignores_a_session_ttl() {
        let bus = EventBus::mock().with_session_ttl(Duration::from_secs(u64::from(u32::MAX)));
        let (_, mut stream) = bus.subscribe("widgets", None).await.unwrap();
        bus.publish(CrudEvent::new("widgets", EventAction::Create, vec![1]))
            .await;
        assert_eq!(stream.next_event().await.unwrap().ids, vec![1]);
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn a_session_ttl_is_kept_whole_seconds_on_the_mqtt_bus() {
        let bus = EventBus::connect("localhost", 1).without_session_ttl();
        assert_eq!(bus.mqtt().unwrap().session_ttl, None);
        let bus = bus.with_session_ttl(Duration::from_millis(3_600_999));
        assert_eq!(bus.mqtt().unwrap().session_ttl, Some(3600));
        // The default is the documented hour.
        let bus = EventBus::connect("localhost", 1);
        assert_eq!(bus.mqtt().unwrap().session_ttl, Some(3600));
        assert_eq!(DEFAULT_SESSION_TTL, Duration::from_secs(3600));
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn a_v5_durable_subscribe_while_the_broker_is_down_is_an_error() {
        let bus = EventBus::connect("127.0.0.1", 1).with_session_ttl(Duration::from_secs(60));
        let err = bus.subscribe("widgets", None).await.err().unwrap();
        assert!(matches!(err, EventError::Broker(_)), "{err}");
    }

    #[cfg(feature = "mqtt")]
    #[test]
    fn mqtt5_poll_results_reduce_to_what_the_stream_acts_on() {
        use mqtt5::mqttbytes::QoS as Qos5;
        use mqtt5::mqttbytes::v5::{
            ConnAck as ConnAck5, ConnectReturnCode as Code5, Packet as Packet5,
            Publish as Publish5, SubAck as SubAck5, SubscribeReasonCode as Reason5,
        };
        let incoming = |packet| Ok(mqtt5::Event::Incoming(packet));

        let publish = Publish5::new("t", Qos5::AtLeastOnce, b"payload".to_vec(), None);
        assert!(matches!(
            polled_v5(incoming(Packet5::Publish(publish))),
            Ok(Polled::Publish(payload)) if payload == b"payload"
        ));
        let conn_ack = ConnAck5 {
            session_present: true,
            code: Code5::Success,
            properties: None,
        };
        assert!(matches!(
            polled_v5(incoming(Packet5::ConnAck(conn_ack))),
            Ok(Polled::ConnAck {
                session_present: true
            })
        ));
        let sub_ack = |code| {
            incoming(Packet5::SubAck(SubAck5 {
                pkid: 1,
                return_codes: vec![code],
                properties: None,
            }))
        };
        assert!(matches!(
            polled_v5(sub_ack(Reason5::Success(Qos5::AtLeastOnce))),
            Ok(Polled::SubAck { refused: false })
        ));
        assert!(matches!(
            polled_v5(sub_ack(Reason5::NotAuthorized)),
            Ok(Polled::SubAck { refused: true })
        ));
        assert!(matches!(
            polled_v5(incoming(Packet5::PingResp(mqtt5::mqttbytes::v5::PingResp))),
            Ok(Polled::Other)
        ));
        assert!(polled_v5(Err(mqtt5::ConnectionError::RequestsDone)).is_err());
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn a_v5_subscriber_client_queues_subscriptions_and_reports_a_closed_channel() {
        let open = || {
            mqtt5::AsyncClient::new(
                mqtt5::MqttOptions::new("test-v5", "localhost", 1883),
                CHANNEL_CAPACITY,
            )
        };
        let (client, _eventloop) = open();
        let client = SubscriberClient::V5(client);
        client.subscribe("t".to_string()).await.unwrap();
        client.try_subscribe("t".to_string()).unwrap();

        let (client, eventloop) = open();
        drop(eventloop);
        let client = SubscriberClient::V5(client);
        assert!(client.subscribe("t".to_string()).await.is_err());
        assert!(client.try_subscribe("t".to_string()).is_err());
    }

    fn widget_event(id: i32) -> CrudEvent {
        CrudEvent::new("widgets", EventAction::Create, vec![id])
    }

    #[tokio::test]
    async fn streams_are_capped_per_caller_across_resources_and_freed_on_drop() {
        let bus = EventBus::mock().with_max_streams_per_caller(Some(2));
        let (_, first) = bus
            .subscribe_for_caller("alice", "widgets", None)
            .await
            .unwrap();
        let (_, _second) = bus
            .subscribe_for_caller("alice", "gadgets", None)
            .await
            .unwrap();
        let err = bus
            .subscribe_for_caller("alice", "widgets", None)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, EventError::TooManyStreams), "{err}");
        // Another caller has its own budget.
        let (_, _bob) = bus
            .subscribe_for_caller("bob", "widgets", None)
            .await
            .unwrap();
        // Dropping a stream frees its slot.
        drop(first);
        assert!(
            bus.subscribe_for_caller("alice", "widgets", None)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn an_anonymous_cap_refusal_takes_no_stream_slot() {
        let bus = EventBus::mock()
            .with_max_streams_per_caller(Some(1))
            .with_max_anonymous_connections(0);
        // The anonymous cap refuses first; the caller's slot must not leak.
        assert!(
            bus.subscribe_for_caller("alice", "widgets", None)
                .await
                .is_err()
        );
        assert!(bus.stream_counts.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn streams_are_capped_per_subscriber_id() {
        let bus = EventBus::mock()
            .with_durable_streams()
            .with_max_streams_per_subscriber(Some(1));
        let (id, _held) = bus
            .subscribe_for_caller("alice", "widgets", None)
            .await
            .unwrap();
        // Another caller presenting the same id is over the id's own limit.
        let err = bus
            .subscribe_for_caller("bob", "widgets", Some(&id))
            .await
            .err()
            .unwrap();
        assert!(matches!(err, EventError::TooManyStreams), "{err}");
        // The refused attempt left nothing behind for bob.
        assert!(
            bus.subscribe_for_caller("bob", "widgets", None)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn the_stream_limits_can_be_removed() {
        let bus = EventBus::mock()
            .with_max_streams_per_caller(None)
            .with_max_streams_per_subscriber(None);
        let mut held = Vec::new();
        for _ in 0..(DEFAULT_MAX_STREAMS_PER_CALLER + 2) {
            held.push(
                bus.subscribe_for_caller("alice", "widgets", None)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(held.len(), DEFAULT_MAX_STREAMS_PER_CALLER + 2);
    }

    #[tokio::test]
    async fn the_default_limits_are_five_per_caller_and_two_per_subscriber() {
        let bus = EventBus::mock().with_durable_streams();
        assert!(bus.durable_streams());
        assert!(!EventBus::mock().durable_streams());
        assert_eq!(bus.max_streams_per_caller, Some(5));
        assert_eq!(bus.max_streams_per_subscriber, Some(2));
        let mut held = Vec::new();
        for _ in 0..5 {
            held.push(
                bus.subscribe_for_caller("alice", "widgets", None)
                    .await
                    .unwrap(),
            );
        }
        assert!(
            bus.subscribe_for_caller("alice", "widgets", None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn route_streams_are_transient_unless_the_host_opts_into_durable_ones() {
        let transient = EventBus::mock();
        let issued = transient.mint_subscriber_id("widgets");
        let (id, _stream) = transient
            .subscribe_for_caller("alice", "widgets", Some(&issued))
            .await
            .unwrap();
        assert_ne!(id, issued, "a transient stream ignores the presented id");

        let durable = EventBus::mock().with_durable_streams();
        let issued = durable.mint_subscriber_id("widgets");
        let (id, _stream) = durable
            .subscribe_for_caller("alice", "widgets", Some(&issued))
            .await
            .unwrap();
        assert_eq!(id, issued);
    }

    #[tokio::test]
    async fn pending_events_are_published_in_order_only_on_commit() {
        let bus = EventBus::mock();
        let (_, mut stream) = bus.subscribe("widgets", None).await.unwrap();
        let mut pending = PendingEvents::new();
        pending.record(widget_event(1));
        pending.record(widget_event(2));
        assert_eq!(pending.events().len(), 2);
        assert_eq!(bus.delivery_stats(), DeliveryStats::default());
        pending.commit(&bus).await;
        assert_eq!(stream.next_event().await.unwrap().ids, vec![1]);
        assert_eq!(stream.next_event().await.unwrap().ids, vec![2]);
        assert_eq!(bus.delivery_stats().published, 2);
    }

    #[tokio::test]
    async fn a_transaction_that_ends_without_a_commit_publishes_nothing() {
        let bus = EventBus::mock();
        let mut dropped = PendingEvents::new();
        dropped.record(widget_event(1));
        drop(dropped);
        let mut rolled_back = PendingEvents::new();
        rolled_back.record(widget_event(2));
        rolled_back.rollback();
        assert_eq!(bus.delivery_stats(), DeliveryStats::default());
    }

    #[tokio::test]
    async fn a_savepoint_rollback_drops_only_its_own_events() {
        let bus = EventBus::mock();
        let (_, mut stream) = bus.subscribe("widgets", None).await.unwrap();
        let mut pending = PendingEvents::new();
        pending.record(widget_event(1));
        let outer = pending.savepoint();
        pending.record(widget_event(2));
        let inner = pending.savepoint();
        pending.record(widget_event(3));
        // Releasing the inner savepoint keeps its event in the outer one.
        pending.release(inner);
        pending.record(widget_event(4));
        // Rolling back the outer savepoint drops everything since it.
        pending.rollback_to(outer);
        pending.record(widget_event(5));
        // A stale savepoint changes nothing.
        pending.rollback_to(inner);
        pending.release(inner);
        assert_eq!(
            pending
                .events()
                .iter()
                .map(|event| event.ids[0])
                .collect::<Vec<_>>(),
            vec![1, 5]
        );
        // The rolled-back savepoint is still usable, as in SQL.
        pending.rollback_to(outer);
        assert_eq!(pending.events().len(), 1);
        pending.commit(&bus).await;
        assert_eq!(stream.next_event().await.unwrap().ids, vec![1]);
        assert_eq!(bus.delivery_stats().published, 1);
    }

    #[tokio::test]
    async fn shutdown_drops_later_events_and_counts_them() {
        let bus = EventBus::mock();
        bus.publish(widget_event(1)).await;
        bus.shutdown().await;
        bus.publish(widget_event(2)).await;
        bus.shutdown().await;
        assert_eq!(
            bus.delivery_stats(),
            DeliveryStats {
                published: 1,
                failed: 1
            }
        );
    }

    #[tokio::test]
    async fn shutdown_waits_for_a_publish_in_flight_but_only_so_long() {
        let bus = EventBus::mock();
        let held = bus.track().unwrap();
        // The publish never finishes, so the bounded wait gives up.
        bus.shutdown().await;
        drop(held);
        assert!(bus.track().is_none());
    }

    #[tokio::test]
    async fn the_debug_output_hides_the_secrets() {
        let bus = EventBus::mock().with_subscriber_secret(b"s".repeat(40));
        let shown = format!("{bus:?}");
        assert!(
            shown.contains("mock") && shown.contains("<redacted>"),
            "{shown}"
        );
        assert!(!shown.contains(&"s".repeat(40)), "{shown}");
        assert!(bus.validate().is_ok());
        #[cfg(feature = "mqtt")]
        assert!(bus.mqtt().is_none());
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn mqtt_credentials_are_refused_without_tls_unless_allowed() {
        let bus = EventBus::connect("127.0.0.1", 1).with_mqtt_credentials("alice", "hunter2");
        let err = bus.validate().unwrap_err();
        assert!(
            matches!(&err, EventError::Config(msg) if msg.contains("without TLS")),
            "{err}"
        );
        // The password is redacted from Debug, the user name is not.
        let shown = format!("{bus:?}");
        assert!(
            shown.contains("alice") && shown.contains("<redacted>"),
            "{shown}"
        );
        assert!(!shown.contains("hunter2"), "{shown}");
        // Both ways to open a connection fail closed, and a publish is counted.
        bus.publish(widget_event(1)).await;
        assert_eq!(bus.delivery_stats().failed, 1);
        let err = bus.subscribe("widgets", None).await.err().unwrap();
        assert!(matches!(err, EventError::Config(_)), "{err}");

        let insecure = bus.with_mqtt_allow_insecure();
        assert!(insecure.validate().is_ok());
        // Allowed: the connection is attempted (and refused by nothing listening).
        let err = insecure.subscribe("widgets", None).await.err().unwrap();
        assert!(matches!(err, EventError::Broker(_)), "{err}");
        let err = insecure
            .without_session_ttl()
            .subscribe("widgets", None)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, EventError::Broker(_)), "{err}");
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn the_mqtt_settings_are_ignored_by_the_in_memory_bus() {
        let bus = EventBus::mock()
            .with_mqtt_credentials("alice", "hunter2")
            .with_mqtt_allow_insecure()
            .without_session_ttl();
        assert!(bus.validate().is_ok());
        bus.publish(widget_event(1)).await;
        assert_eq!(bus.delivery_stats().published, 1);
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn the_mqtt_publisher_is_opened_once_and_shared() {
        let bus = EventBus::connect("127.0.0.1", 1);
        // Shutting down before anything was published has nothing to close.
        bus.shutdown().await;
        let bus = EventBus::connect("127.0.0.1", 1);
        let mqtt = bus.mqtt().unwrap();
        let (_, first) = mqtt.shared_client().await.unwrap();
        let (_, again) = mqtt.shared_client().await.unwrap();
        assert_eq!(first, again);
        bus.publish(widget_event(1)).await;
        bus.publish_all(vec![widget_event(2), widget_event(3)])
            .await;
        // Queued for a broker that is not there: counted as published.
        assert_eq!(bus.delivery_stats().published, 3);
        bus.shutdown().await;
        assert!(mqtt.publisher_slot().is_none());
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn a_publish_that_times_out_discards_the_shared_connection() {
        // Nothing listens on port 1, so the request queue is never drained and
        // fills up; the publish that finds it full times out.
        let bus = EventBus::connect("127.0.0.1", 1);
        let mqtt = bus.mqtt().unwrap();
        let (_, before) = mqtt.shared_client().await.unwrap();
        for id in 0..500 {
            bus.publish(widget_event(id)).await;
            if bus.delivery_stats().failed > 0 {
                break;
            }
        }
        assert_eq!(bus.delivery_stats().failed, 1);
        assert!(
            mqtt.publisher_slot().is_none(),
            "the connection is discarded"
        );
        // The next publish opens a fresh connection.
        bus.publish(widget_event(1)).await;
        let (_, after) = mqtt.shared_client().await.unwrap();
        assert!(after > before);
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn a_cancelled_publish_discards_the_shared_connection() {
        let bus = EventBus::connect("127.0.0.1", 1);
        let mqtt = bus.mqtt().unwrap();
        let (_, generation) = mqtt.shared_client().await.unwrap();
        let guard = DiscardGuard {
            backend: mqtt,
            generation,
            armed: true,
        };
        drop(guard);
        assert!(mqtt.publisher_slot().is_none());
        // A guard for a connection that is already replaced leaves the new one.
        let (_, current) = mqtt.shared_client().await.unwrap();
        mqtt.discard(current + 100);
        assert!(mqtt.publisher_slot().is_some());
        // A disarmed guard does nothing.
        let guard = DiscardGuard {
            backend: mqtt,
            generation: current,
            armed: false,
        };
        drop(guard);
        assert!(mqtt.publisher_slot().is_some());
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn a_publish_on_a_dead_connection_is_counted_and_replaces_it() {
        let bus = EventBus::connect("127.0.0.1", 1);
        let mqtt = bus.mqtt().unwrap();
        mqtt.shared_client().await.unwrap();
        // Stopping the driver drops the event loop, which closes the queue.
        mqtt.publisher_slot().as_ref().unwrap().task.abort();
        tokio::time::sleep(Duration::from_millis(50)).await;
        bus.publish(widget_event(1)).await;
        assert_eq!(bus.delivery_stats().failed, 1);
        assert!(mqtt.publisher_slot().is_none());
        bus.publish(widget_event(2)).await;
        assert_eq!(bus.delivery_stats().published, 1);
    }

    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn stream_slots_are_freed_when_the_broker_refuses_the_connection() {
        let bus = EventBus::connect("127.0.0.1", 1).with_max_streams_per_caller(Some(1));
        for _ in 0..2 {
            let err = bus
                .subscribe_for_caller("alice", "widgets", None)
                .await
                .err()
                .unwrap();
            assert!(matches!(err, EventError::Broker(_)), "{err}");
        }
        assert!(bus.stream_counts.lock().unwrap().is_empty());
    }

    #[cfg(feature = "mqtt-tls")]
    fn pem_file(name: &str, contents: &[u8]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("craftainer-crud-events-{}", random_token()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    /// A throwaway self-signed certificate and its PKCS#8 PEM key, made when
    /// the test runs so no key material lives in the repository.
    #[cfg(feature = "mqtt-tls")]
    fn self_signed() -> (Vec<u8>, Vec<u8>) {
        use openssl::asn1::Asn1Time;
        use openssl::ec::{EcGroup, EcKey};
        use openssl::hash::MessageDigest;
        use openssl::nid::Nid;
        use openssl::pkey::PKey;
        use openssl::x509::{X509, X509NameBuilder};

        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", "test").unwrap();
        let name = name.build();
        let mut builder = X509::builder().unwrap();
        builder.set_version(2).unwrap();
        builder.set_subject_name(&name).unwrap();
        builder.set_issuer_name(&name).unwrap();
        builder.set_pubkey(&key).unwrap();
        builder
            .set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        builder
            .set_not_after(&Asn1Time::days_from_now(1).unwrap())
            .unwrap();
        builder.sign(&key, MessageDigest::sha256()).unwrap();
        (
            builder.build().to_pem().unwrap(),
            key.private_key_to_pem_pkcs8().unwrap(),
        )
    }

    #[cfg(feature = "mqtt-tls")]
    #[tokio::test]
    async fn tls_credentials_and_client_auth_reach_every_connection() {
        let (cert_pem, key_pem) = self_signed();
        // A bundle of two certificates, with a trailing newline.
        let bundle = [cert_pem.clone(), cert_pem.clone()].concat();
        let ca = pem_file("ca.pem", &bundle);
        let cert = pem_file("client.pem", &cert_pem);
        let key = pem_file("client.key", &key_pem);
        let tls = MqttTls::new(&ca).with_client_auth(&cert, &key);
        assert_eq!(tls, tls.clone());
        let bus = EventBus::connect("127.0.0.1", 1)
            .with_mqtt_tls(tls)
            .with_mqtt_credentials("alice", "hunter2");
        // TLS satisfies the credentials rule.
        assert!(bus.validate().is_ok());
        // The shared publisher is built with the transport and login.
        bus.publish(widget_event(1)).await;
        assert_eq!(bus.delivery_stats().published, 1);
        // So is a v5 durable subscriber (the default) and a v3 one; the
        // refused connection is a broker error, not a configuration one.
        let err = bus.subscribe("widgets", None).await.err().unwrap();
        assert!(matches!(err, EventError::Broker(_)), "{err}");
        let err = bus
            .without_session_ttl()
            .subscribe("widgets", None)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, EventError::Broker(_)), "{err}");
        assert!(
            format!("{:?}", EventBus::mock().with_mqtt_tls(MqttTls::new(&ca))).contains("mock")
        );
    }

    #[cfg(feature = "mqtt-tls")]
    #[tokio::test]
    async fn tls_without_client_auth_builds_a_transport_from_the_ca_alone() {
        let (cert_pem, _) = self_signed();
        let ca = pem_file("ca.pem", &cert_pem);
        let bus = EventBus::connect("127.0.0.1", 1).with_mqtt_tls(MqttTls::new(&ca));
        bus.publish(widget_event(1)).await;
        assert_eq!(bus.delivery_stats().published, 1);
    }

    #[cfg(feature = "mqtt-tls")]
    #[tokio::test]
    async fn an_unreadable_tls_file_fails_closed_with_a_config_error() {
        let (cert_pem, key_pem) = self_signed();
        let ca = pem_file("ca.pem", &cert_pem);
        let cert = pem_file("client.pem", &cert_pem);
        let key = pem_file("client.key", &key_pem);
        let missing = ca.with_file_name("missing.pem");
        for tls in [
            MqttTls::new(&missing),
            MqttTls::new(&ca).with_client_auth(&missing, &key),
            MqttTls::new(&ca).with_client_auth(&cert, &missing),
        ] {
            let bus = EventBus::connect("127.0.0.1", 1).with_mqtt_tls(tls);
            let err = bus.subscribe("widgets", None).await.err().unwrap();
            assert!(
                matches!(&err, EventError::Config(msg) if msg.contains("missing.pem")),
                "{err}"
            );
            bus.publish(widget_event(1)).await;
            assert_eq!(bus.delivery_stats().failed, 1);
        }
    }

    #[cfg(feature = "mqtt-tls")]
    #[tokio::test]
    async fn malformed_tls_material_fails_closed_with_a_config_error() {
        let (cert_pem, key_pem) = self_signed();
        let good_ca = pem_file("ca.pem", &cert_pem);
        let junk = pem_file("junk.pem", b"not a certificate");
        let truncated = pem_file(
            "truncated.pem",
            b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        );
        let key = pem_file("client.key", &key_pem);
        for tls in [
            // No certificate at all, a certificate that does not parse, and
            // a client certificate or key that does not parse.
            MqttTls::new(&junk),
            MqttTls::new(&truncated),
            MqttTls::new(&good_ca).with_client_auth(&junk, &key),
            MqttTls::new(&good_ca).with_client_auth(&good_ca, &junk),
        ] {
            let bus = EventBus::connect("127.0.0.1", 1).with_mqtt_tls(tls);
            let err = bus.subscribe("widgets", None).await.err().unwrap();
            assert!(
                matches!(&err, EventError::Config(msg) if msg.contains("invalid TLS configuration")),
                "{err}"
            );
        }
    }
}
