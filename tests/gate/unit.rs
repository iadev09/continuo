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
/// It counts live permits rather than `count()`, because `count()` is
/// briefly above the limit by design — a rejected task increments before it
/// learns there was no room.
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
