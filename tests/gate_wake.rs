use continuo::Gate;
use std::sync::Mutex;

struct ReleaseOnCapacity(Mutex<Option<continuo::Permit>>);
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S>
    for ReleaseOnCapacity
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() == "continuo::gate" {
            drop(self.0.lock().unwrap().take());
        }
    }
}
#[tokio::test]
async fn enter_does_not_lose_capacity_release() {
    use tracing_subscriber::layer::SubscriberExt;
    let gate = Gate::new(Some(1), continuo::gate::default_acquire_timeout());
    let held = gate.try_enter().unwrap();
    let subscriber = tracing_subscriber::registry()
        .with(ReleaseOnCapacity(Mutex::new(Some(held))));
    let _default = tracing::subscriber::set_default(subscriber);
    // The synchronous capacity debug event releases the permit after the
    // failed capacity check but before enter creates its Notified future.
    let result = gate.enter().await;
    assert_eq!(gate.count(), 1);
    assert!(
        result.is_ok(),
        "capacity was released before sleeping, but enter returned {:?}",
        result.err()
    );
}

#[tokio::test]
async fn queued_entrants_recheck_capacity_and_shutdown_interrupts() {
    use std::future::Future;
    use std::task::{Context, Waker};
    let gate = Gate::new(Some(1), continuo::gate::default_acquire_timeout());
    let held = gate.try_enter().unwrap();
    let mut first = Box::pin(gate.enter());
    let mut second = Box::pin(gate.enter());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    drop(held);
    let held = first.await.unwrap();
    assert!(second.as_mut().poll(&mut cx).is_pending());
    drop(held);
    let held = second.await.unwrap();
    let mut waiting = Box::pin(gate.enter());
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    gate.graceful_shutdown(None);
    assert!(matches!(waiting.await, Err(continuo::gate::Error::ShuttingDown)));
    drop(held);
    assert_eq!(gate.wait_all_done().await, continuo::GateDrainOutcome::Drained);
}
