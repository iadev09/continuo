//! A number, independent of Playground and Continuo.
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn value(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
    pub fn set(
        &self,
        value: u64,
    ) {
        self.0.store(value, Ordering::Relaxed);
    }
    pub fn add(
        &self,
        amount: u64,
    ) -> u64 {
        let mut current = self.value();
        loop {
            let next = current.saturating_add(amount);
            match self.0.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(value) => current = value,
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/playground/counter.rs"]
mod tests;
