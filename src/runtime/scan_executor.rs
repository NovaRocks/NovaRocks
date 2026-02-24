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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread;

use crate::common::config::{
    pipeline_scan_thread_pool_queue_size, pipeline_scan_thread_pool_thread_num,
};
use crate::exec::pipeline::schedule::observer::Observable;

type ScanTask = Box<dyn FnOnce() + Send + 'static>;

pub struct ScanExecutor {
    inner: Arc<ScanExecutorInner>,
    #[allow(dead_code)]
    workers: Vec<thread::JoinHandle<()>>,
}

impl ScanExecutor {
    pub fn new(num_threads: usize, queue_capacity: usize) -> Self {
        let capacity = queue_capacity.max(1);
        let inner = Arc::new(ScanExecutorInner::new(capacity));
        let threads = num_threads.max(1);
        let mut workers = Vec::with_capacity(threads);
        for _ in 0..threads {
            let inner_clone = Arc::clone(&inner);
            workers.push(thread::spawn(move || worker_loop(inner_clone)));
        }
        Self { inner, workers }
    }

    pub fn submit<F>(&self, task: F) -> bool
    where
        F: FnOnce() + Send + 'static,
    {
        self.inner.submit(Box::new(task))
    }

    pub fn force_submit<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.inner.force_submit(Box::new(task));
    }

    pub fn num_tasks(&self) -> usize {
        self.inner.num_tasks()
    }

    // Register a waiter to be notified when executor capacity becomes available.
    // This function guarantees the waiter is registered before any capacity release
    // can happen, avoiding missed wakeups.
    pub fn register_capacity_waiter(&self, observable: &Arc<Observable>) {
        self.inner.register_capacity_waiter(observable);
    }
}

struct ScanExecutorInner {
    queue: Mutex<VecDeque<ScanTask>>,
    cv: Condvar,
    waiters: Mutex<Vec<Weak<Observable>>>,
    capacity: usize,
    shutdown: AtomicBool,
}

impl ScanExecutorInner {
    fn new(capacity: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            waiters: Mutex::new(Vec::new()),
            capacity: capacity.max(1),
            shutdown: AtomicBool::new(false),
        }
    }

    fn submit(&self, task: ScanTask) -> bool {
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        let mut queue = self.queue.lock().expect("scan executor queue lock");
        if queue.len() >= self.capacity {
            return false;
        }
        queue.push_back(task);
        self.cv.notify_one();
        true
    }

    fn force_submit(&self, task: ScanTask) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }
        let mut queue = self.queue.lock().expect("scan executor queue lock");
        queue.push_back(task);
        self.cv.notify_one();
    }

    fn num_tasks(&self) -> usize {
        let queue = self.queue.lock().expect("scan executor queue lock");
        queue.len()
    }

    fn take(&self) -> Option<ScanTask> {
        let mut queue = self.queue.lock().expect("scan executor queue lock");
        while queue.is_empty() && !self.shutdown.load(Ordering::Acquire) {
            queue = self
                .cv
                .wait(queue)
                .expect("scan executor queue condvar wait");
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
        // Hold the queue lock to avoid a race with workers taking tasks.
        let queue = self.queue.lock().expect("scan executor queue lock");
        if queue.len() < self.capacity || self.shutdown.load(Ordering::Acquire) {
            drop(queue);
            let notify = observable.defer_notify();
            notify.arm();
            return;
        }
        let mut waiters = self.waiters.lock().expect("scan executor waiters lock");
        waiters.push(Arc::downgrade(observable));
        drop(waiters);
        drop(queue);
    }

    fn notify_one_capacity_waiter(&self) {
        let mut waiters = self.waiters.lock().expect("scan executor waiters lock");
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

fn worker_loop(inner: Arc<ScanExecutorInner>) {
    while let Some(task) = inner.take() {
        task();
    }
}

static SCAN_EXECUTOR: OnceLock<ScanExecutor> = OnceLock::new();

pub fn scan_executor() -> &'static ScanExecutor {
    SCAN_EXECUTOR.get_or_init(|| {
        let threads = pipeline_scan_thread_pool_thread_num();
        let queue_capacity = pipeline_scan_thread_pool_queue_size();
        ScanExecutor::new(threads, queue_capacity)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    impl ScanExecutor {
        // Test-only constructor: no worker threads, controlled by test code.
        fn new_for_test(queue_capacity: usize) -> Self {
            let inner = Arc::new(ScanExecutorInner::new(queue_capacity));
            Self {
                inner,
                workers: Vec::new(),
            }
        }

        fn pop_one_for_test(&self) -> bool {
            let task = {
                let mut queue = self.inner.queue.lock().expect("scan executor queue lock");
                queue.pop_front()
            };
            if let Some(task) = task {
                self.inner.notify_one_capacity_waiter();
                task();
                true
            } else {
                false
            }
        }
    }

    #[test]
    fn scan_executor_submit_respects_capacity() {
        let exec = ScanExecutor::new_for_test(1);
        assert!(exec.submit(|| {}));
        assert!(!exec.submit(|| {}));
    }

    #[test]
    fn scan_executor_capacity_waiter_notified() {
        let exec = ScanExecutor::new_for_test(1);
        let observable = Arc::new(Observable::new());
        let (tx, rx) = mpsc::channel::<()>();
        observable.add_observer(Arc::new(move || {
            let _ = tx.send(());
        }));

        assert!(exec.submit(|| {}));
        exec.register_capacity_waiter(&observable);
        assert!(exec.pop_one_for_test());

        let notified = rx.recv_timeout(Duration::from_millis(200)).is_ok();
        assert!(notified);
    }

    #[test]
    fn scan_executor_capacity_waiter_single_notify() {
        let exec = ScanExecutor::new_for_test(1);
        let observable = Arc::new(Observable::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        observable.add_observer(Arc::new(move || {
            counter_clone.fetch_add(1, Ordering::AcqRel);
        }));

        assert!(exec.submit(|| {}));
        exec.register_capacity_waiter(&observable);
        exec.register_capacity_waiter(&observable);
        assert!(exec.pop_one_for_test());

        let count = counter.load(Ordering::Acquire);
        assert_eq!(count, 1);
    }
}
