use async_trait::async_trait;
use continuo::{
    Component, Finalizable, HasRegistry, Registry, Result, RunContext,
    Runnable, Runtime, SharedState,
};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct State(CancellationToken, Arc<Registry<State>>);
impl HasRegistry for State {
    fn registry_ref(&self) -> &Registry<Self> {
        &self.1
    }
}
impl SharedState for State {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.clone()
    }
}
struct Fails(Mutex<Option<tokio::sync::oneshot::Sender<()>>>);
impl Component<State> for Fails {
    type Deps = ();
    fn name(&self) -> &'static str {
        "fails"
    }
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<State>>> {
        Some(self)
    }
}
#[async_trait]
impl Runnable<State> for Fails {
    async fn run(
        self: Arc<Self>,
        _: (),
        state: State,
        _: RunContext,
    ) -> Result<()> {
        state.0.cancelled().await;
        self.0.lock().unwrap().take().unwrap().send(()).unwrap();
        Err(continuo::Error::msg("first failure"))
    }
}
struct Pending(CancellationToken, AtomicBool, Arc<AtomicBool>);
#[async_trait]
impl Finalizable<State> for Pending {
    async fn finalize(
        &self,
        _: &State,
    ) -> Result<()> {
        assert!(
            self.1.load(Ordering::SeqCst),
            "finalized before runnable completion"
        );
        self.2.store(true, Ordering::SeqCst);
        Ok(())
    }
}
impl Component<State> for Pending {
    type Deps = ();
    fn name(&self) -> &'static str {
        "pending"
    }
    fn as_finalizable(&self) -> Option<&dyn Finalizable<State>> {
        Some(self)
    }
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<State>>> {
        Some(self)
    }
}
#[async_trait]
impl Runnable<State> for Pending {
    async fn run(
        self: Arc<Self>,
        _: (),
        _: State,
        _: RunContext,
    ) -> Result<()> {
        self.0.cancelled().await;
        self.1.store(true, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn drain_waits_for_all_tasks_after_one_fails() {
    let registry = Arc::new(Registry::new());
    let (tx, rx) = tokio::sync::oneshot::channel();
    registry.register(Arc::new(Fails(Mutex::new(Some(tx))))).unwrap();
    let release = CancellationToken::new();
    registry
        .register(Arc::new(Pending(
            release.clone(),
            AtomicBool::new(false),
            Arc::new(AtomicBool::new(false)),
        )))
        .unwrap();
    let shutdown = CancellationToken::new();
    let mut runtime = Runtime::default();
    runtime
        .spawn_all(&registry, State(shutdown.clone(), registry.clone()))
        .unwrap();
    shutdown.cancel();
    rx.await.unwrap(); // The failing task returns in this same uninterrupted turn.
    let mut drain = Box::pin(runtime.drain());
    let mut cx = Context::from_waker(Waker::noop());
    let observed = drain.as_mut().poll(&mut cx);
    release.cancel();
    // The failed join has been consumed, but its error must survive this await.
    // Keep the same drain future so its retained failure is not discarded.
    assert!(
        observed.is_pending(),
        "drain returned while another owned task was pending: {observed:?}"
    );
    assert!(drain.await.unwrap_err().to_string().contains("first failure"));
}

struct LateFailure<const PANIC: bool>(
    CancellationToken,
    Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
);
impl<const PANIC: bool> Component<State> for LateFailure<PANIC> {
    type Deps = ();
    fn name(&self) -> &'static str {
        if PANIC { "panics" } else { "fails-later" }
    }
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<State>>> {
        Some(self)
    }
}
#[async_trait]
impl<const PANIC: bool> Runnable<State> for LateFailure<PANIC> {
    async fn run(
        self: Arc<Self>,
        _: (),
        _: State,
        _: RunContext,
    ) -> Result<()> {
        self.0.cancelled().await;
        self.1.lock().unwrap().take().unwrap().send(()).unwrap();
        assert!(!PANIC, "fixture panic");
        Err(continuo::Error::msg("later failure"))
    }
}

#[tokio::test]
async fn finalization_waits_for_sibling_after_multiple_failures_and_panic() {
    let registry = Arc::new(Registry::new());
    let shutdown = CancellationToken::new();
    let state = State(shutdown.clone(), registry.clone());
    let (tx, first_failed) = tokio::sync::oneshot::channel();
    registry.register(Arc::new(Fails(Mutex::new(Some(tx))))).unwrap();
    let fail_later = CancellationToken::new();
    let (tx, second_failed) = tokio::sync::oneshot::channel();
    registry
        .register(Arc::new(LateFailure::<false>(
            fail_later.clone(),
            Mutex::new(Some(tx)),
        )))
        .unwrap();
    let (tx, panicked) = tokio::sync::oneshot::channel();
    registry
        .register(Arc::new(LateFailure::<true>(
            fail_later.clone(),
            Mutex::new(Some(tx)),
        )))
        .unwrap();
    let release = CancellationToken::new();
    let finalized = Arc::new(AtomicBool::new(false));
    registry
        .register(Arc::new(Pending(
            release.clone(),
            AtomicBool::new(false),
            finalized.clone(),
        )))
        .unwrap();
    let mut run = Box::pin(Runtime::default().run_prepared(state));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(run.as_mut().poll(&mut cx).is_pending());
    shutdown.cancel();
    assert!(run.as_mut().poll(&mut cx).is_pending());
    first_failed.await.unwrap();
    assert!(run.as_mut().poll(&mut cx).is_pending());
    fail_later.cancel();
    second_failed.await.unwrap();
    panicked.await.unwrap();
    assert!(run.as_mut().poll(&mut cx).is_pending());
    assert!(!finalized.load(Ordering::SeqCst));
    release.cancel();
    assert!(run.await.unwrap_err().to_string().contains("first failure"));
    assert!(finalized.load(Ordering::SeqCst));
}
