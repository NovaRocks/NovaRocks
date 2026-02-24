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
//! Blocked-driver poller for event-driven wake-up.
//!
//! Responsibilities:
//! - Tracks blocked drivers and re-schedules them when dependencies become ready.
//! - Reduces active-spin scheduling by polling readiness queues in batches.
//!
//! Key exported interfaces:
//! - Types: `BlockedDriverPoller`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use super::global_driver_executor::{DriverTask, ExecutorShared};
use crate::novarocks_logging::debug;

const DEFAULT_POLL_INTERVAL_MS: u64 = 10;

struct BlockedTask {
    task: DriverTask,
    next_poll_at: Instant,
}

struct PollerState {
    shared: Arc<ExecutorShared>,
    poll_interval: Duration,
    blocked: Mutex<VecDeque<BlockedTask>>,
    cv: Condvar,
    cv_mutex: Mutex<()>,
    shutdown: AtomicBool,
    started: AtomicBool,
}

#[derive(Clone)]
/// Poller that tracks blocked drivers and re-queues them once blocking dependencies are satisfied.
pub(crate) struct BlockedDriverPoller {
    state: Arc<PollerState>,
}

impl BlockedDriverPoller {
    pub(crate) fn new(shared: Arc<ExecutorShared>) -> Self {
        let state = PollerState {
            shared,
            poll_interval: Duration::from_millis(DEFAULT_POLL_INTERVAL_MS),
            blocked: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            cv_mutex: Mutex::new(()),
            shutdown: AtomicBool::new(false),
            started: AtomicBool::new(false),
        };
        Self {
            state: Arc::new(state),
        }
    }

    pub(crate) fn start(&self) {
        if self.state.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let state = Arc::clone(&self.state);
        thread::Builder::new()
            .name("blocked_driver_poller".to_string())
            .spawn(move || run_poller(state))
            .expect("blocked driver poller thread");
    }

    pub(crate) fn add_pending_finish(&self, task: DriverTask) {
        debug!(
            "Driver pending finish; using poll scheduling: driver_id={}",
            task.driver_id()
        );
        let next_poll_at = Instant::now() + self.state.poll_interval;
        let mut blocked = self.state.blocked.lock().expect("blocked poller lock");
        blocked.push_back(BlockedTask { task, next_poll_at });
        self.state.cv.notify_one();
    }
}

fn run_poller(state: Arc<PollerState>) {
    debug!(
        "BlockedDriverPoller started with poll_interval={:?}",
        state.poll_interval
    );
    loop {
        if state.shutdown.load(Ordering::Acquire) {
            break;
        }

        let mut ready_tasks = Vec::new();
        let mut aborted_tasks = Vec::new();
        drain_blocked(&state, &mut ready_tasks, &mut aborted_tasks);

        for task in aborted_tasks {
            task.finish_due_to_abort();
        }
        for task in ready_tasks {
            enqueue_one(&state.shared, task);
        }

        let guard = state.cv_mutex.lock().expect("blocked poller cv lock");
        let _ = state
            .cv
            .wait_timeout(guard, state.poll_interval)
            .expect("blocked poller cv wait");
    }
}

fn drain_blocked(
    state: &PollerState,
    ready_tasks: &mut Vec<DriverTask>,
    aborted_tasks: &mut Vec<DriverTask>,
) {
    let now = Instant::now();
    let mut blocked = state.blocked.lock().expect("blocked poller lock");
    let mut pending = VecDeque::new();
    while let Some(mut entry) = blocked.pop_front() {
        if entry.task.should_abort_immediately() {
            aborted_tasks.push(entry.task);
            continue;
        }
        if entry.next_poll_at > now {
            pending.push_back(entry);
            continue;
        }
        if entry.task.check_is_ready() {
            entry.task.set_ready();
            ready_tasks.push(entry.task);
        } else {
            entry.next_poll_at = now + state.poll_interval;
            pending.push_back(entry);
        }
    }
    *blocked = pending;
}

fn enqueue_one(shared: &ExecutorShared, task: DriverTask) {
    let mut queue = shared.queue.lock().expect("global executor queue lock");
    queue.push_back(task);
    shared.cv.notify_one();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;
    use std::time::Duration;

    use crate::exec::chunk::Chunk;
    use crate::exec::pipeline::driver::PipelineDriver;
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::FragmentCompletion;
    use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
    use crate::runtime::runtime_state::RuntimeState;

    struct PollerGuard {
        state: Arc<PollerState>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl Drop for PollerGuard {
        fn drop(&mut self) {
            self.state.shutdown.store(true, Ordering::Release);
            self.state.cv.notify_all();
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    struct DummySource;

    impl Operator for DummySource {
        fn name(&self) -> &str {
            "dummy_source"
        }
    }

    impl ProcessorOperator for DummySource {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }
    }

    struct DummySink;

    impl Operator for DummySink {
        fn name(&self) -> &str {
            "dummy_sink"
        }
    }

    impl ProcessorOperator for DummySink {
        fn need_input(&self) -> bool {
            true
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn pending_finish_driver_finishes_on_abort() {
        let shared = Arc::new(ExecutorShared {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
        });
        let poller = BlockedDriverPoller::new(Arc::clone(&shared));
        let state = Arc::clone(&poller.state);
        let poller_thread = thread::spawn(move || run_poller(state));
        let _guard = PollerGuard {
            state: Arc::clone(&poller.state),
            handle: Some(poller_thread),
        };

        let runtime_state = Arc::new(RuntimeState::default());
        let driver = PipelineDriver::new(
            1,
            vec![Box::new(DummySource), Box::new(DummySink)],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            None,
        );
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&runtime_state),
            None,
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1, Arc::clone(&fragment_ctx));
        let task = DriverTask::new(driver, Arc::clone(&completion), Duration::from_millis(10));

        poller.add_pending_finish(task);
        completion.fail("forced abort".to_string());

        let (tx, rx) = mpsc::channel();
        let completion_clone: Arc<FragmentCompletion> = Arc::clone(&completion);
        thread::spawn(move || {
            let _ = tx.send(completion_clone.wait());
        });

        let wait_res = rx.recv_timeout(Duration::from_millis(200));
        assert!(
            wait_res.is_ok(),
            "completion wait should finish after abort"
        );
    }
}
