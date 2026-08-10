//! Scheduler-neutral observable callbacks used by execution queues and ports.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Callback invoked after an observable state transition.
pub type Observer = Arc<dyn Fn() + Send + Sync + 'static>;

/// Thread-safe callback registry for execution readiness transitions.
pub struct Observable {
    observers: Mutex<Vec<Observer>>,
}

impl Observable {
    pub fn new() -> Self {
        Self {
            observers: Mutex::new(Vec::new()),
        }
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
