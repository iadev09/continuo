use super::*;
use std::sync::atomic::AtomicUsize;

#[tokio::test]
async fn unused_scope_never_constructs_a_tracker_and_late_handles_are_closed() {
    let context = RunContext::new(Default::default());
    let retained = context.child();
    assert!(context.tasks.tracker.get().is_none());
    run_owned(context.clone(), async { Ok(()) }).await.unwrap();
    assert!(context.tasks.tracker.get().is_none());
    assert!(retained.spawn_tracked(|_| async { Ok(()) }).is_err());
    assert!(context.tasks.tracker.get().is_none());
}

#[tokio::test]
async fn handles_preserve_child_results_and_panics() {
    let context = RunContext::new(Default::default());
    let error = context
        .spawn_tracked(|_| async { Err(Error::msg("child failure")) })
        .unwrap();
    assert_eq!(error.await.unwrap().unwrap_err().to_string(), "child failure");
    let panic =
        context.spawn_tracked(|_| async { panic!("child panic") }).unwrap();
    assert!(panic.await.unwrap_err().is_panic());
    run_owned(context, async { Ok(()) }).await.unwrap();
}

#[tokio::test]
async fn returning_owner_drains_nested_children_without_retained_join_handles()
{
    let context = RunContext::new(Default::default());
    let entered = Arc::new(tokio::sync::Notify::new());
    let draining = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let child_entered = entered.clone();
    let child_draining = draining.clone();
    let child_release = release.clone();
    drop(
        context
            .spawn_tracked(move |child| async move {
                drop(child.spawn_tracked(move |nested| async move {
                    child_entered.notify_one();
                    nested.cancelled().await;
                    child_draining.notify_one();
                    child_release.notified().await;
                    Ok(())
                })?);
                child.cancelled().await;
                Ok(())
            })
            .unwrap(),
    );
    entered.notified().await;
    let finished = tokio::spawn(run_owned(context, async { Ok(()) }));
    draining.notified().await;
    assert!(!finished.is_finished());
    release.notify_one();
    finished.await.unwrap().unwrap();
}

#[tokio::test]
async fn admission_and_close_share_one_completion_boundary() {
    let context = RunContext::new(Default::default());
    let accepted = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let first_done = completed.clone();
    drop(
        context
            .spawn_tracked(move |child| async move {
                child.cancelled().await;
                first_done.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap(),
    );
    accepted.fetch_add(1, Ordering::SeqCst);
    let barrier = Arc::new(std::sync::Barrier::new(3));
    std::thread::scope(|threads| {
        let mut jobs = Vec::new();
        for _ in 0..2 {
            let context = context.clone();
            let barrier = barrier.clone();
            let accepted = accepted.clone();
            let completed = completed.clone();
            jobs.push(threads.spawn(move || {
                barrier.wait();
                if let Ok(task) =
                    context.spawn_tracked(move |child| async move {
                        child.cancelled().await;
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    drop(task);
                }
            }));
        }
        barrier.wait();
        context.tasks.close();
        for job in jobs {
            job.join().unwrap();
        }
    });
    context.cancellation.cancel();
    context.tasks.wait().await;
    assert_eq!(
        completed.load(Ordering::SeqCst),
        accepted.load(Ordering::SeqCst)
    );
    assert!(context.spawn_tracked(|_| async { Ok(()) }).is_err());
}
