//! Integration tier: the CRUD event stream against the devcontainer
//! stack's real Mosquitto broker (`MQTT_HOST`/`MQTT_PORT`, default
//! `mqtt:1883`). Needs the `mqtt` feature.
//!
//! This is the only place the delivery guarantee can actually be
//! verified. `EventBus::mock` has no broker, no persistent
//! session and no queue, so a test against it proves nothing about
//! replay -- it can only show that the fan-out works. What is proved here:
//!
//! - a QoS-1 publish reaches a live subscriber;
//! - an event published while a subscriber is *disconnected* is delivered
//!   when the same `subscriber_id` reconnects (the persistent session);
//! - a *transient* subscriber (clean session) leaves nothing to replay;
//! - reconnecting with a *different* id gets no replay, which is the
//!   guarantee's stated limit rather than a bug.
//!
//! Every test uses a resource name (and therefore a topic) unique to the
//! run, so a leftover persistent session from an earlier run can never
//! feed one of these assertions.

use std::time::Duration;

use craftainer_crud::events::{CrudEvent, EventAction, EventBus, EventError, EventStream, topic};
use rumqttc::{AsyncClient, MqttOptions, QoS};

/// Long enough for a local broker round trip, short enough that a hung
/// expectation fails the test rather than the harness timeout.
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);

fn mqtt_endpoint() -> (String, u16) {
    let host = std::env::var("MQTT_HOST").unwrap_or_else(|_| "mqtt".to_string());
    let port = std::env::var("MQTT_PORT")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(1883);
    (host, port)
}

/// A suffix unique to this process and call, so topics never collide with a
/// leftover persistent session from an earlier run.
fn unique_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CALLS: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    format!(
        "{}-{nanos}-{}",
        std::process::id(),
        CALLS.fetch_add(1, Ordering::Relaxed)
    )
}

fn broker() -> EventBus {
    let (host, port) = mqtt_endpoint();
    EventBus::connect(&host, port)
}

async fn next_within(stream: &mut EventStream, timeout: Duration) -> Option<CrudEvent> {
    tokio::time::timeout(timeout, stream.next_event())
        .await
        .ok()
        .flatten()
}

/// Drive a freshly opened stream until the broker has actually accepted
/// the subscription. `next_event` only yields on a PUBLISH, so the way to
/// know the CONNECT/SUBSCRIBE handshake has completed is to poll it for a
/// moment and see nothing -- which is also what establishes the persistent
/// session the replay assertions depend on.
async fn settle(stream: &mut EventStream) {
    assert!(
        next_within(stream, Duration::from_secs(2)).await.is_none(),
        "nothing has been published yet, so nothing should arrive"
    );
}

#[tokio::test]
async fn a_published_event_reaches_a_live_subscriber() {
    let resource = format!("widgets-it-{}", unique_suffix());
    let bus = broker();
    let mut stream = bus
        .subscribe(&resource, None)
        .await
        .expect("the devcontainer stack's Mosquitto must be running")
        .1;
    settle(&mut stream).await;

    bus.publish(CrudEvent::new(&resource, EventAction::Create, vec![42]))
        .await;

    let event = next_within(&mut stream, RECEIVE_TIMEOUT)
        .await
        .expect("a QoS-1 publish must reach a subscribed client");
    assert_eq!(event.resource, resource);
    assert_eq!(event.action, EventAction::Create);
    assert_eq!(event.ids, vec![42]);
}

#[tokio::test]
async fn a_transient_subscriber_leaves_no_session_to_replay() {
    let resource = format!("widgets-it-{}", unique_suffix());
    let bus = broker();

    let (subscriber_id, mut first) = bus.subscribe_transient(&resource, None).await.unwrap();
    settle(&mut first).await;
    bus.publish(CrudEvent::new(&resource, EventAction::Create, vec![1]))
        .await;
    assert!(
        next_within(&mut first, RECEIVE_TIMEOUT).await.is_some(),
        "a transient subscriber still receives events while connected"
    );
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    bus.publish(CrudEvent::new(&resource, EventAction::Update, vec![2]))
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Resuming the same client id durably finds no session, so nothing was
    // queued for it while it was away.
    let (resumed_id, mut second) = bus
        .subscribe(&resource, Some(&subscriber_id))
        .await
        .unwrap();
    assert_eq!(resumed_id, subscriber_id);
    assert!(
        next_within(&mut second, Duration::from_secs(2))
            .await
            .is_none(),
        "the broker must hold no session for a transient subscriber"
    );
}

#[tokio::test]
async fn events_published_while_a_subscriber_is_away_are_replayed_on_reconnect() {
    let resource = format!("widgets-it-{}", unique_suffix());
    let bus = broker();

    // First connect: establishes the persistent session (clean_session
    // false, client id derived from subscriber_id) and its QoS-1
    // subscription. Without this the broker has nothing to queue *into*.
    let (subscriber_id, mut first) = bus.subscribe(&resource, None).await.unwrap();
    settle(&mut first).await;
    drop(first);
    // Give the broker a moment to notice the disconnect before publishing.
    tokio::time::sleep(Duration::from_millis(500)).await;

    bus.publish(CrudEvent::new(&resource, EventAction::Update, vec![7]))
        .await;
    bus.publish(CrudEvent::new(
        &resource,
        EventAction::DeleteMany,
        vec![8, 9],
    ))
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Same subscriber_id -> same client id -> same session -> the queue.
    let (resumed_id, mut second) = bus
        .subscribe(&resource, Some(&subscriber_id))
        .await
        .unwrap();
    assert_eq!(resumed_id, subscriber_id, "a minted id is resumed as given");

    let first_replayed = next_within(&mut second, RECEIVE_TIMEOUT)
        .await
        .expect("the first event published while away must be replayed");
    let second_replayed = next_within(&mut second, RECEIVE_TIMEOUT)
        .await
        .expect("the second event published while away must be replayed");

    let mut actions = [first_replayed.action, second_replayed.action];
    actions.sort_by_key(|action| action.as_str());
    assert_eq!(actions, [EventAction::DeleteMany, EventAction::Update]);
}

#[tokio::test]
async fn a_subscriber_that_reconnects_with_a_new_id_gets_no_replay() {
    let resource = format!("widgets-it-{}", unique_suffix());
    let bus = broker();

    let mut first = bus.subscribe(&resource, None).await.unwrap().1;
    settle(&mut first).await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    bus.publish(CrudEvent::new(&resource, EventAction::Create, vec![1]))
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // A fresh subscriber_id is a fresh client id, so a fresh session with
    // an empty queue -- the guarantee's stated limit, asserted rather than
    // merely documented.
    let mut fresh = bus.subscribe(&resource, None).await.unwrap().1;
    assert!(
        next_within(&mut fresh, Duration::from_secs(3))
            .await
            .is_none(),
        "a client that discarded its subscriber_id must not receive replay"
    );
}

#[tokio::test]
async fn two_subscribers_each_receive_the_same_event() {
    let resource = format!("widgets-it-{}", unique_suffix());
    let bus = broker();
    let mut a = bus.subscribe(&resource, None).await.unwrap().1;
    let mut b = bus.subscribe(&resource, None).await.unwrap().1;
    settle(&mut a).await;
    settle(&mut b).await;

    bus.publish(CrudEvent::new(&resource, EventAction::Delete, vec![3]))
        .await;

    assert_eq!(
        next_within(&mut a, RECEIVE_TIMEOUT).await.unwrap().ids,
        vec![3]
    );
    assert_eq!(
        next_within(&mut b, RECEIVE_TIMEOUT).await.unwrap().ids,
        vec![3]
    );
}

#[tokio::test]
async fn a_subscriber_only_receives_its_own_resources_events() {
    let suffix = unique_suffix();
    let watched = format!("widgets-it-{suffix}");
    let other = format!("villains-it-{suffix}");
    let bus = broker();
    let mut stream = bus.subscribe(&watched, None).await.unwrap().1;
    settle(&mut stream).await;

    bus.publish(CrudEvent::new(&other, EventAction::Create, vec![1]))
        .await;
    bus.publish(CrudEvent::new(&watched, EventAction::Create, vec![2]))
        .await;

    let event = next_within(&mut stream, RECEIVE_TIMEOUT).await.unwrap();
    assert_eq!(
        event.resource, watched,
        "topics are per-resource, so the other resource's event never arrives here"
    );
    assert_eq!(event.ids, vec![2]);
}

#[tokio::test]
async fn a_subscriber_skips_an_undecodable_payload_and_keeps_streaming() {
    let resource = format!("widgets-it-{}", unique_suffix());
    let bus = broker();
    let mut stream = bus
        .subscribe(&resource, None)
        .await
        .expect("the devcontainer stack's Mosquitto must be running")
        .1;
    settle(&mut stream).await;

    // A raw client, bypassing EventBus::publish entirely, so it can put
    // a payload on the topic that isn't valid `CrudEvent` JSON at all --
    // something no caller going through the public API could ever send.
    let (host, port) = mqtt_endpoint();
    let mut options = MqttOptions::new(
        format!("crud-events-garbage-publisher-{}", unique_suffix()),
        host,
        port,
    );
    options.set_clean_session(true);
    let (raw_publisher, mut raw_eventloop) = AsyncClient::new(options, 8);
    tokio::spawn(async move {
        loop {
            if raw_eventloop.poll().await.is_err() {
                return;
            }
        }
    });
    // Let the eventloop task actually connect before publishing, and again
    // after, so the garbage payload is on the wire (and, being QoS 1,
    // acknowledged) before the legitimate event below -- two publishes
    // from different client connections have no ordering guarantee
    // otherwise, and this test only means anything if the garbage one
    // arrives first.
    tokio::time::sleep(Duration::from_millis(300)).await;
    raw_publisher
        .publish(
            topic(&resource),
            QoS::AtLeastOnce,
            false,
            b"not json".to_vec(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    bus.publish(CrudEvent::new(&resource, EventAction::Create, vec![7]))
        .await;

    let event = next_within(&mut stream, RECEIVE_TIMEOUT)
        .await
        .expect("the undecodable payload must be skipped, not tear down the stream");
    assert_eq!(event.ids, vec![7]);
}

#[tokio::test]
async fn subscribing_while_the_broker_is_down_is_an_error_not_a_dead_stream() {
    // Nothing listens on port 1.
    let bus = EventBus::connect("127.0.0.1", 1);
    let err = bus.subscribe("widgets", None).await.err().unwrap();
    assert!(matches!(err, EventError::Broker(_)), "{err}");
}

#[tokio::test]
async fn a_client_chosen_id_cannot_take_over_a_live_subscribers_session() {
    let resource = format!("widgets-it-{}", unique_suffix());
    let bus = broker();
    let (victim_id, mut victim) = bus.subscribe(&resource, None).await.unwrap();
    settle(&mut victim).await;

    // The attacker guesses the id (here: the victim's id with a character
    // changed, and the raw client id the victim's session runs under).
    let mut guessed = victim_id.clone();
    guessed.replace_range(0..1, if victim_id.starts_with('0') { "1" } else { "0" });
    for attempt in [guessed, format!("crud-events-{resource}-{victim_id}")] {
        let (given, _stream) = bus.subscribe(&resource, Some(&attempt)).await.unwrap();
        assert_ne!(given, victim_id);
        assert_ne!(given, attempt);
    }

    // The victim's session was not kicked off the broker.
    bus.publish(CrudEvent::new(&resource, EventAction::Create, vec![11]))
        .await;
    let event = next_within(&mut victim, RECEIVE_TIMEOUT)
        .await
        .expect("the victim's session must survive the takeover attempt");
    assert_eq!(event.ids, vec![11]);
}

#[tokio::test]
async fn a_subscriber_id_is_scoped_to_its_resource() {
    let suffix = unique_suffix();
    let first = format!("widgets-it-{suffix}");
    let second = format!("gadgets-it-{suffix}");
    let bus = broker();
    let (id, _stream) = bus.subscribe(&first, None).await.unwrap();
    let (issued, _other) = bus.subscribe(&second, Some(&id)).await.unwrap();
    assert_ne!(
        issued, id,
        "an id minted for one resource is not valid for another"
    );
}
