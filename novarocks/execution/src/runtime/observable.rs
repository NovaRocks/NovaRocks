//! Scheduler-neutral observable callbacks used by execution queues and ports.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Callback invoked after an observable state transition.
pub type Observer = Arc<dyn Fn() + Send + Sync + 'static>;

/// Thread-safe callback registry for execution readiness transitions.
pub struct Observable {
    observers: Mutex<Vec<Observer>>,
    generation: AtomicU64,
}

impl Observable {
    pub fn new() -> Self {
        Self {
            observers: Mutex::new(Vec::new()),
            generation: AtomicU64::new(0),
        }
    }

    /// Returns the monotonic transition generation observed by waiters.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn add_observer(&self, observer: Observer) {
        self.observers
            .lock()
            .expect("observable lock")
            .push(observer);
    }

    pub fn defer_notify(self: &Arc<Self>) -> DeferNotify {
        DeferNotify::new(Arc::clone(self))
    }

    pub fn notify_observers(&self) {
        // Publish the transition before invoking callbacks. A waiter that has
        // not registered yet can therefore detect the transition by comparing
        // the generation it froze before deciding to block.
        self.generation.fetch_add(1, Ordering::Release);
        let observers = self.observers.lock().expect("observable lock").clone();
        for observer in observers {
            observer();
        }
    }

    pub fn num_observers(&self) -> usize {
        self.observers.lock().expect("observable lock").len()
    }
}

impl Default for Observable {
    fn default() -> Self {
        Self::new()
    }
}

/// Defers observable callbacks until the surrounding state transition ends.
#[must_use]
pub struct DeferNotify {
    observable: Arc<Observable>,
    armed: AtomicBool,
}

impl DeferNotify {
    pub fn new(observable: Arc<Observable>) -> Self {
        Self {
            observable,
            armed: AtomicBool::new(false),
        }
    }

    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
}

impl Drop for DeferNotify {
    fn drop(&mut self) {
        if self.armed.load(Ordering::Acquire) {
            self.observable.notify_observers();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_is_published_before_callbacks() {
        let observable = Arc::new(Observable::new());
        let callback_generation = Arc::new(AtomicU64::new(u64::MAX));
        let observed = Arc::clone(&observable);
        let callback_generation_clone = Arc::clone(&callback_generation);
        observable.add_observer(Arc::new(move || {
            callback_generation_clone.store(observed.generation(), Ordering::Release);
        }));

        observable.notify_observers();

        assert_eq!(observable.generation(), 1);
        assert_eq!(callback_generation.load(Ordering::Acquire), 1);
    }

    #[test]
    fn generation_advances_without_registered_observers() {
        let observable = Observable::new();

        observable.notify_observers();
        observable.notify_observers();

        assert_eq!(observable.generation(), 2);
    }
}
