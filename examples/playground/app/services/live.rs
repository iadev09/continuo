//! Resource handoff to a running service, not file/config notification.
//! The reload hook prepares a replacement; its run loop drains/switches and
//! acknowledges completion. Stopping drops pending work and wakes its caller.
use continuo::{Error, Result};
use std::sync::Mutex;
use tokio::sync::{Notify, oneshot};

pub struct Live<T> {
    state: Mutex<State<T>>,
    ready: Notify,
}

struct State<T> {
    running: bool,
    pending: Option<Replacement<T>>,
}

pub struct Replacement<T> {
    pub value: T,
    pub completed: oneshot::Sender<Result<()>>,
}

pub struct Generation<'a, T>(&'a Live<T>);

impl<T> Live<T> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State { running: false, pending: None }),
            ready: Notify::new(),
        }
    }

    pub fn start(&self) -> Generation<'_, T> {
        self.state.lock().expect("live resource poisoned").running = true;
        Generation(self)
    }

    pub fn is_running(&self) -> bool {
        self.state.lock().expect("live resource poisoned").running
    }

    /// The provider serializes reloads; at most one handoff is outstanding.
    pub async fn replace(
        &self,
        value: T,
    ) -> Result<()> {
        let (completed, reply) = oneshot::channel();
        {
            let mut state = self.state.lock().expect("live resource poisoned");
            if !state.running {
                return Err(Error::msg("service stopped during reload"));
            }
            if state.pending.is_some() {
                return Err(Error::msg("resource replacement already pending"));
            }
            state.pending = Some(Replacement { value, completed });
        }
        self.ready.notify_one();
        reply.await.map_err(|_| {
            Error::msg("service stopped before replacement completed")
        })?
    }

    pub async fn replacement(&self) -> Replacement<T> {
        loop {
            if let Some(value) = self
                .state
                .lock()
                .expect("live resource poisoned")
                .pending
                .take()
            {
                return value;
            }
            self.ready.notified().await;
        }
    }
}

impl<T> Drop for Generation<'_, T> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("live resource poisoned");
        state.running = false;
        state.pending = None;
    }
}
