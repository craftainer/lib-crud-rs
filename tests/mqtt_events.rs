//! Integration tier: the CRUD event stream against the devcontainer
//! stack's real Mosquitto broker.
//!
//! This is the only place NFR-0029's delivery guarantee can actually be
//! verified. `Mode::Mock`'s `EventBus::mock` has no broker, no persistent
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

mod common;

use std::time::Duration;

use common::{integration_settings, unique_suffix};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use template_axum::events::{
    new_subscriber_id, topic, CrudEvent, EventAction, EventBus, EventStream,
};

/// Long enough for a local broker round trip, short enough that a hung
/// expectation fails the test rather than the harness timeout.
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);

fn broker() -> EventBus {
    let settings = integration_settings();
    EventBus::connect(&settings.mqtt_host, settings.mqtt_port)
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
    let resource = format!("heroes-it-{}", unique_suffix());
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
    let resource = format!("heroes-it-{}", unique_suffix());
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
    let resource = format!("heroes-it-{}", unique_suffix());
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
    // an empty queue -- NFR-0029's stated limit, asserted rather than
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
    let resource = format!("heroes-it-{}", unique_suffix());
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
    let watched = format!("heroes-it-{suffix}");
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
async fn build_event_bus_connects_to_the_real_broker_outside_mode_mock() {
    // `lib::build_event_bus`'s `Mode::Mock` branch has its own unit test
    // (`src/lib.rs`'s colocated `mod tests`); only the real-broker branch
    // needs live Mosquitto.
    let mut settings = integration_settings();
    settings.mode = template_axum::config::Mode::Dev;
    let bus = template_axum::build_event_bus(&settings);

    let resource = format!("heroes-it-{}", unique_suffix());
    let mut stream = bus
        .subscribe(&resource, &new_subscriber_id())
        .await
        .unwrap();
    settle(&mut stream).await;
    bus.publish(CrudEvent::new(&resource, EventAction::Create, vec![42]))
        .await;
    assert_eq!(
        next_within(&mut stream, RECEIVE_TIMEOUT).await.unwrap().ids,
        vec![42]
    );
}

#[tokio::test]
async fn a_subscriber_skips_an_undecodable_payload_and_keeps_streaming() {
    let resource = format!("heroes-it-{}", unique_suffix());
    let bus = broker();
    let mut stream = bus
        .subscribe(&resource, &new_subscriber_id())
        .await
        .expect("the devcontainer stack's Mosquitto must be running");
    settle(&mut stream).await;

    // A raw client, bypassing EventBus::publish entirely, so it can put
    // a payload on the topic that isn't valid `CrudEvent` JSON at all --
    // something no caller going through the public API could ever send.
    let settings = integration_settings();
    let mut options = MqttOptions::new(
        format!("crud-events-garbage-publisher-{}", new_subscriber_id()),
        settings.mqtt_host,
        settings.mqtt_port,
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
