use continuo::{Event, ProcessEventBus};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};

#[derive(Clone, Debug, PartialEq)]
struct Message(u8);
impl Event for Message {}

#[test]
fn filtered_queue_ignores_unrelated_traffic_and_preserves_typed_broadcast() {
    let bus = ProcessEventBus::with_capacity(2);
    let mut all = bus.subscribe::<Message>();
    bus.emit(Message(1));
    let mut filtered = bus.subscribe_filtered::<Message>(|event| event.0 == 1);
    assert_eq!(filtered.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(all.try_recv(), Ok(Message(1)));
    for _ in 0..16 {
        bus.emit(Message(0));
    }
    assert_eq!(filtered.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(all.try_recv(), Err(TryRecvError::Lagged(14)));
    bus.emit(Message(1));
    assert_eq!(filtered.try_recv(), Ok(Message(1)));
    assert_eq!(filtered.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn matching_overflow_still_reports_lag() {
    let bus = ProcessEventBus::with_capacity(2);
    let mut first = bus.subscribe_filtered::<Message>(|_| true);
    let mut second = bus.subscribe_filtered::<Message>(|_| true);
    for value in 0..4 {
        bus.emit(Message(value));
        assert_eq!(second.try_recv(), Ok(Message(value)));
    }
    assert_eq!(first.try_recv(), Err(TryRecvError::Lagged(2)));
    assert_eq!(first.try_recv(), Ok(Message(2)));
    assert_eq!(first.try_recv(), Ok(Message(3)));
}

#[test]
fn filtered_capacity_belongs_to_the_consumer_not_the_typed_channel() {
    let bus = ProcessEventBus::with_capacity(1);
    let mut receiver =
        bus.subscribe_filtered_with_capacity::<Message>(4, |_| true);
    let mut ordinary = bus.subscribe::<Message>();
    for value in 0..4 {
        bus.emit(Message(value));
    }
    assert_eq!(ordinary.try_recv(), Err(TryRecvError::Lagged(3)));
    for value in 0..4 {
        assert_eq!(receiver.try_recv(), Ok(Message(value)));
    }
}

#[tokio::test]
async fn filtered_wait_uses_native_wake_and_closes_with_the_bus() {
    let bus = ProcessEventBus::with_capacity(2);
    let peer = bus.clone();
    let mut receiver = bus.subscribe_filtered::<Message>(|event| event.0 == 1);
    {
        let receive = receiver.recv();
        tokio::pin!(receive);
        assert!(futures_util::poll!(&mut receive).is_pending());
        bus.emit(Message(0));
        assert!(futures_util::poll!(&mut receive).is_pending());
        peer.emit(Message(1));
        assert_eq!(receive.await, Ok(Message(1)));
    }
    drop(bus);
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    drop(peer);
    assert_eq!(receiver.recv().await, Err(RecvError::Closed));
}

#[test]
fn dropped_receiver_releases_its_filter_on_the_next_publication() {
    let bus = ProcessEventBus::new();
    let selection = std::sync::Arc::new(());
    let captured = selection.clone();
    let receiver = bus.subscribe_filtered::<Message>(move |_| {
        let _ = &captured;
        true
    });
    drop(receiver);
    bus.emit(Message(1));
    assert_eq!(std::sync::Arc::strong_count(&selection), 1);
}
