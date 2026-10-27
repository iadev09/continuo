use std::time::Duration;

use super::{Error, Gate, GateDrainOutcome};

#[test]
fn try_enter_counts_one_slot_per_permit() {
    let gate = Gate::new(Some(2), Duration::from_millis(10));

    let first = gate.try_enter().unwrap();
    assert_eq!(gate.count(), 1);

    let second = gate.try_enter().unwrap();
    assert_eq!(gate.count(), 2);

    assert!(matches!(gate.try_enter(), Err(Error::AtCapacity)));

    drop(first);
    assert_eq!(gate.count(), 1);

    drop(second);
    assert_eq!(gate.count(), 0);
}

#[tokio::test]
async fn empty_gate_reports_clean_drain() {
    let gate = Gate::new(None, Duration::from_millis(10));

    assert_eq!(gate.wait_all_done().await, GateDrainOutcome::Drained);
}

#[tokio::test]
async fn elapsed_grace_period_reports_forced_drain() {
    let gate = Gate::new(None, Duration::from_millis(10));
    let _permit = gate.try_enter().expect("permit should be admitted");
    gate.graceful_shutdown(Some(Duration::from_millis(1)));

    assert_eq!(
        gate.wait_all_done().await,
        GateDrainOutcome::Forced { remaining: 1 }
    );
}

/// `enter` under contention, which is the only way its limit can be wrong.
///
/// Single-threaded tests cannot see this: the fault was reading the count
/// and then taking a permit, so every task that observed `max - 1` in the
/// window between the two got one. Against the load-then-increment version
/// this reaches 5 live permits against a max of 4 within a few rounds.
///
/// Count only accepted permits; rejected entrants never affect drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn enter_never_admits_more_than_max() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    for _ in 0..200 {
        let gate = Gate::new(Some(4), Duration::from_secs(5));
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let entrants: Vec<_> = (0..32)
            .map(|_| {
                let gate = gate.clone();
                let live = Arc::clone(&live);
                let peak = Arc::clone(&peak);
                tokio::spawn(async move {
                    let Ok(permit) = gate.enter().await else {
                        return;
                    };
                    let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    live.fetch_sub(1, Ordering::SeqCst);
                    drop(permit);
                })
            })
            .collect();

        for entrant in entrants {
            entrant.await.unwrap();
        }

        let observed = peak.load(Ordering::SeqCst);
        assert!(
            observed <= 4,
            "gate held {observed} permits at once against max 4"
        );
    }
}

#[tokio::test]
async fn shutdown_drains_accepted_work_and_rejects_waiting_threads() {
    use std::future::Future;
    use std::sync::{Arc, Barrier};
    use std::task::{Context, Waker};

    let gate = Gate::new(Some(1), Duration::ZERO);
    let permit = gate.try_enter().unwrap();
    // Refusal cannot contribute a temporary permit to the drain count.
    assert!(matches!(gate.try_enter(), Err(Error::AtCapacity)));
    assert_eq!(gate.count(), 1);
    let ready = Arc::new(Barrier::new(2));
    let proceed = Arc::new(Barrier::new(2));
    let entrant = {
        let gate = gate.clone();
        let ready = ready.clone();
        let proceed = proceed.clone();
        std::thread::spawn(move || {
            ready.wait();
            proceed.wait();
            assert!(matches!(gate.try_enter(), Err(Error::ShuttingDown)));
        })
    };
    ready.wait();
    gate.graceful_shutdown(None);
    let mut drain = Box::pin(gate.wait_all_done());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(drain.as_mut().poll(&mut cx).is_pending());
    drop(permit);
    assert_eq!(drain.await, GateDrainOutcome::Drained);
    // Even an entrant already scheduled before closure cannot create work
    // after the completed drain boundary.
    proceed.wait();
    entrant.join().unwrap();
    assert_eq!(gate.count(), 0);
}

#[test]
fn grace_period_preserves_full_duration_range() {
    let gate = Gate::new(None, Duration::ZERO);
    assert_eq!(gate.grace_period(), None);
    for duration in [
        Duration::ZERO,
        super::default_acquire_timeout(),
        Duration::from_nanos(u64::MAX - 1),
        Duration::from_nanos(u64::MAX),
        Duration::from_nanos(u64::MAX) + Duration::from_nanos(1),
        Duration::MAX,
    ] {
        gate.graceful_shutdown(Some(duration));
        assert_eq!(gate.grace_period(), Some(duration));
    }
    gate.graceful_shutdown(None);
    assert_eq!(gate.grace_period(), None);
}
