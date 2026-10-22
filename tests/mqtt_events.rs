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
//! - reconnecting with a *different* id gets no replay, which is the
//!   guarantee's stated limit rather than a bug.
//!
//! Every test uses a resource name (and therefore a topic) unique to the
//! run, so a leftover persistent session from an earlier run can never
//! feed one of these assertions.

use std::time::Duration;

use craftainer_crud::events::{
    CrudEvent, EventAction, EventBus, EventStream, new_subscriber_id, topic,
};
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
    new_subscriber_id()
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
        .subscribe(&resource, &new_subscriber_id())
        .await
        .expect("the devcontainer stack's Mosquitto must be running");
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
async fn events_published_while_a_subscriber_is_away_are_replayed_on_reconnect() {
    let resource = format!("widgets-it-{}", unique_suffix());
    let subscriber_id = new_subscriber_id();
    let bus = broker();

    // First connect: establishes the persistent session (clean_session
    // false, client id derived from subscriber_id) and its QoS-1
    // subscription. Without this the broker has nothing to queue *into*.
    let mut first = bus.subscribe(&resource, &subscriber_id).await.unwrap();
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
    let mut second = bus.subscribe(&resource, &subscriber_id).await.unwrap();

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

    let mut first = bus
        .subscribe(&resource, &new_subscriber_id())
        .await
        .unwrap();
    settle(&mut first).await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    bus.publish(CrudEvent::new(&resource, EventAction::Create, vec![1]))
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // A fresh subscriber_id is a fresh client id, so a fresh session with
    // an empty queue -- the guarantee's stated limit, asserted rather than
    // merely documented.
    let mut fresh = bus
        .subscribe(&resource, &new_subscriber_id())
        .await
        .unwrap();
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
    let mut a = bus
        .subscribe(&resource, &new_subscriber_id())
        .await
        .unwrap();
    let mut b = bus
        .subscribe(&resource, &new_subscriber_id())
        .await
        .unwrap();
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
    let mut stream = bus.subscribe(&watched, &new_subscriber_id()).await.unwrap();
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
        .subscribe(&resource, &new_subscriber_id())
        .await
        .expect("the devcontainer stack's Mosquitto must be running");
    settle(&mut stream).await;

    // A raw client, bypassing EventBus::publish entirely, so it can put
    // a payload on the topic that isn't valid `CrudEvent` JSON at all --
    // something no caller going through the public API could ever send.
    let (host, port) = mqtt_endpoint();
    let mut options = MqttOptions::new(
        format!("crud-events-garbage-publisher-{}", new_subscriber_id()),
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
