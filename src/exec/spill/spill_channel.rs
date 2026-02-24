// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread;

use crate::common::config::{spill_io_queue_size, spill_io_threads};
use crate::exec::pipeline::schedule::observer::Observable;
use crate::novarocks_logging::error;

pub type SpillTask = Box<dyn FnOnce() -> Result<(), String> + Send + 'static>;

pub struct SpillIoExecutor {
    inner: Arc<SpillIoExecutorInner>,
    #[allow(dead_code)]
    workers: Vec<thread::JoinHandle<()>>,
}

impl SpillIoExecutor {
    pub fn new(num_threads: usize, queue_capacity: usize) -> Self {
        let capacity = queue_capacity.max(1);
        let inner = Arc::new(SpillIoExecutorInner::new(capacity));
        let threads = num_threads.max(1);
        let mut workers = Vec::with_capacity(threads);
        for _ in 0..threads {
            let inner_clone = Arc::clone(&inner);
            workers.push(thread::spawn(move || worker_loop(inner_clone)));
        }
        Self { inner, workers }
    }

    pub fn submit(&self, task: SpillTask) -> bool {
        self.inner.submit(task)
    }

    pub fn force_submit(&self, task: SpillTask) {
        self.inner.force_submit(task);
    }

    pub fn num_tasks(&self) -> usize {
        self.inner.num_tasks()
    }

    pub fn register_capacity_waiter(&self, observable: &Arc<Observable>) {
        self.inner.register_capacity_waiter(observable);
    }
}

struct SpillIoExecutorInner {
    queue: Mutex<VecDeque<SpillTask>>,
    cv: Condvar,
    waiters: Mutex<Vec<Weak<Observable>>>,
    capacity: usize,
    shutdown: AtomicBool,
}

impl SpillIoExecutorInner {
    fn new(capacity: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            waiters: Mutex::new(Vec::new()),
            capacity: capacity.max(1),
            shutdown: AtomicBool::new(false),
        }
    }

    fn submit(&self, task: SpillTask) -> bool {
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        let mut queue = self.queue.lock().expect("spill io executor queue lock");
        if queue.len() >= self.capacity {
            return false;
        }
        queue.push_back(task);
        self.cv.notify_one();
        true
    }

    fn force_submit(&self, task: SpillTask) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }
        let mut queue = self.queue.lock().expect("spill io executor queue lock");
        queue.push_back(task);
        self.cv.notify_one();
    }

    fn num_tasks(&self) -> usize {
        let queue = self.queue.lock().expect("spill io executor queue lock");
        queue.len()
    }

    fn take(&self) -> Option<SpillTask> {
        let mut queue = self.queue.lock().expect("spill io executor queue lock");
        while queue.is_empty() && !self.shutdown.load(Ordering::Acquire) {
            queue = self
                .cv
                .wait(queue)
                .expect("spill io executor queue condvar wait");
        }
        if self.shutdown.load(Ordering::Acquire) {
            return None;
        }
        let task = queue.pop_front();
        drop(queue);
        if task.is_some() {
            self.notify_one_capacity_waiter();
        }
        task
    }

    fn register_capacity_waiter(&self, observable: &Arc<Observable>) {
        let queue = self.queue.lock().expect("spill io executor queue lock");
        if queue.len() < self.capacity || self.shutdown.load(Ordering::Acquire) {
            drop(queue);
            let notify = observable.defer_notify();
            notify.arm();
            return;
        }
        let mut waiters = self.waiters.lock().expect("spill io executor waiters lock");
        waiters.push(Arc::downgrade(observable));
        drop(waiters);
        drop(queue);
    }

    fn notify_one_capacity_waiter(&self) {
        let mut waiters = self.waiters.lock().expect("spill io executor waiters lock");
        let mut notify = None;
        while let Some(weak) = waiters.pop() {
            if let Some(obs) = weak.upgrade() {
                notify = Some(obs);
                break;
            }
        }
        drop(waiters);
        if let Some(obs) = notify {
            let notify = obs.defer_notify();
            notify.arm();
        }
    }
}

fn worker_loop(inner: Arc<SpillIoExecutorInner>) {
    while let Some(task) = inner.take() {
        if let Err(err) = task() {
            error!("spill task failed: {}", err);
        }
    }
}

static SPILL_IO_EXECUTOR: OnceLock<SpillIoExecutor> = OnceLock::new();

pub fn spill_io_executor() -> &'static SpillIoExecutor {
    SPILL_IO_EXECUTOR.get_or_init(|| {
        let threads = spill_io_threads();
        let queue_capacity = spill_io_queue_size();
        SpillIoExecutor::new(threads, queue_capacity)
    })
}

struct SpillChannel {
    executor: &'static SpillIoExecutor,
    inflight_tasks: AtomicUsize,
    capacity_observable: Arc<Observable>,
}

#[derive(Clone)]
pub struct SpillChannelHandle {
    inner: Arc<SpillChannel>,
}

impl SpillChannelHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SpillChannel {
                executor: spill_io_executor(),
                inflight_tasks: AtomicUsize::new(0),
                capacity_observable: Arc::new(Observable::new()),
            }),
        }
    }

    pub fn submit(&self, task: SpillTask) -> Result<(), String> {
        self.inner.inflight_tasks.fetch_add(1, Ordering::AcqRel);
        let channel = Arc::clone(&self.inner);
        let wrapped: SpillTask = Box::new(move || {
            let _guard = InflightGuard {
                channel: Arc::clone(&channel),
            };
            task()
        });
        if !self.inner.executor.submit(wrapped) {
            self.inner.inflight_tasks.fetch_sub(1, Ordering::AcqRel);
            return Err("spill io queue is full".to_string());
        }
        Ok(())
    }

    pub fn force_submit(&self, task: SpillTask) {
        self.inner.inflight_tasks.fetch_add(1, Ordering::AcqRel);
        let channel = Arc::clone(&self.inner);
        let wrapped: SpillTask = Box::new(move || {
            let _guard = InflightGuard {
                channel: Arc::clone(&channel),
            };
            task()
        });
        self.inner.executor.force_submit(wrapped);
    }

    pub fn has_pending(&self) -> bool {
        self.inner.inflight_tasks.load(Ordering::Acquire) > 0
    }

    pub fn capacity_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.inner.capacity_observable)
    }

    pub fn register_capacity_waiter(&self) {
        self.inner
            .executor
            .register_capacity_waiter(&self.inner.capacity_observable);
    }
}

struct InflightGuard {
    channel: Arc<SpillChannel>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.channel.inflight_tasks.fetch_sub(1, Ordering::AcqRel);
    }
}

impl fmt::Debug for SpillChannelHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillChannelHandle")
            .field(
                "inflight_tasks",
                &self.inner.inflight_tasks.load(Ordering::Acquire),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    impl SpillIoExecutor {
        fn new_for_test(queue_capacity: usize) -> Self {
            let inner = Arc::new(SpillIoExecutorInner::new(queue_capacity));
            Self {
                inner,
                workers: Vec::new(),
            }
        }

        fn pop_one_for_test(&self) -> bool {
            let task = {
                let mut queue = self
                    .inner
                    .queue
                    .lock()
                    .expect("spill io executor queue lock");
                queue.pop_front()
            };
            if let Some(task) = task {
                self.inner.notify_one_capacity_waiter();
                let _ = task();
                true
            } else {
                false
            }
        }
    }

    #[test]
    fn spill_executor_submit_respects_capacity() {
        let exec = SpillIoExecutor::new_for_test(1);
        assert!(exec.submit(Box::new(|| Ok(()))));
        assert!(!exec.submit(Box::new(|| Ok(()))));
    }

    #[test]
    fn spill_executor_capacity_waiter_notified() {
        let exec = SpillIoExecutor::new_for_test(1);
        let observable = Arc::new(Observable::new());
        let (tx, rx) = mpsc::channel::<()>();
        observable.add_observer(Arc::new(move || {
            let _ = tx.send(());
        }));

        assert!(exec.submit(Box::new(|| Ok(()))));
        exec.register_capacity_waiter(&observable);
        assert!(exec.pop_one_for_test());

        let notified = rx.recv_timeout(Duration::from_millis(200)).is_ok();
        assert!(notified);
    }

    #[test]
    fn spill_executor_capacity_waiter_single_notify() {
        let exec = SpillIoExecutor::new_for_test(1);
        let observable = Arc::new(Observable::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        observable.add_observer(Arc::new(move || {
            counter_clone.fetch_add(1, Ordering::AcqRel);
        }));

        assert!(exec.submit(Box::new(|| Ok(()))));
        exec.register_capacity_waiter(&observable);
        exec.register_capacity_waiter(&observable);
        assert!(exec.pop_one_for_test());

        let count = counter.load(Ordering::Acquire);
        assert_eq!(count, 1);
    }
}
