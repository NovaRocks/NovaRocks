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

//! Process-wide completion ownership for native tasks.
//!
//! Pipeline workers publish only an immutable stopped fact. One fixed async
//! owner consumes those facts and performs the task's ordered completion work,
//! so a running task does not retain a dedicated supervising OS thread.

use std::collections::HashMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks_execution::runtime::fragment::FragmentTerminalFact;
use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use tokio::sync::{Notify, mpsc, watch};

use crate::BackendDataRuntime;

const COMPLETION_STATE_LOCK: &str = "task completion supervisor state lock";
const COMPLETION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

type CompletionAction = Box<dyn FnOnce(FragmentTerminalFact) + Send + 'static>;

struct CompletionSlot {
    expected_fragment: novarocks_types::UniqueId,
    action: Option<CompletionAction>,
    fact: Option<FragmentTerminalFact>,
    creation_committed: bool,
    queued: bool,
}

struct CompletionState {
    accepting: bool,
    slots: HashMap<TaskIdentity, CompletionSlot>,
    failure: Option<String>,
}

struct CompletionShared {
    state: Mutex<CompletionState>,
    ready: mpsc::Sender<TaskIdentity>,
    changed: Notify,
}

impl CompletionShared {
    fn fail_locked(state: &mut CompletionState, detail: impl Into<String>) {
        if state.failure.is_none() {
            state.failure = Some(detail.into());
        }
    }

    fn publish(&self, identity: TaskIdentity, fact: FragmentTerminalFact) {
        let enqueue = {
            let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
            let Some(slot) = state.slots.get_mut(&identity) else {
                // A stopped observer is one-shot. Reaching a missing exact
                // slot means the owner has already processed it, or process
                // shutdown has withdrawn it; neither can create new work.
                return;
            };
            if fact.query_id() != identity.query_execution_id().query_id()
                || fact.fragment_instance_id() != slot.expected_fragment
            {
                Self::fail_locked(
                    &mut state,
                    format!(
                        "task {identity} published a stopped fact for query {:?} fragment {}",
                        fact.query_id(),
                        fact.fragment_instance_id()
                    ),
                );
                self.changed.notify_one();
                return;
            }
            if slot.fact.is_some() {
                // The kernel contract is one-shot. Keep the first immutable
                // fact if an implementation wakes the same slot twice.
                return;
            }
            slot.fact = Some(fact);
            if !slot.creation_committed || slot.queued {
                false
            } else {
                slot.queued = true;
                true
            }
        };
        if enqueue {
            self.enqueue(identity);
        }
    }

    fn commit_creation(&self, identity: TaskIdentity) {
        let enqueue = {
            let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
            let Some(slot) = state.slots.get_mut(&identity) else {
                Self::fail_locked(
                    &mut state,
                    format!("task {identity} committed without a completion slot"),
                );
                self.changed.notify_one();
                return;
            };
            if slot.creation_committed {
                return;
            }
            slot.creation_committed = true;
            if slot.fact.is_none() || slot.queued {
                false
            } else {
                slot.queued = true;
                true
            }
        };
        if enqueue {
            self.enqueue(identity);
        }
    }

    fn enqueue(&self, identity: TaskIdentity) {
        if let Err(error) = self.ready.try_send(identity) {
            let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
            Self::fail_locked(
                &mut state,
                format!("task {identity} completion ready queue rejected its exact slot: {error}"),
            );
            self.changed.notify_one();
        }
    }

    fn take_ready(
        &self,
        identity: TaskIdentity,
    ) -> Option<(CompletionAction, FragmentTerminalFact)> {
        let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
        let Some(mut slot) = state.slots.remove(&identity) else {
            Self::fail_locked(
                &mut state,
                format!("task {identity} completion ready queue named no exact slot"),
            );
            self.changed.notify_one();
            return None;
        };
        let Some(action) = slot.action.take() else {
            Self::fail_locked(
                &mut state,
                format!("task {identity} completion slot had no action"),
            );
            self.changed.notify_one();
            return None;
        };
        let Some(fact) = slot.fact.take() else {
            Self::fail_locked(
                &mut state,
                format!("task {identity} completion slot was ready without a stopped fact"),
            );
            self.changed.notify_one();
            return None;
        };
        self.changed.notify_one();
        Some((action, fact))
    }

    fn note_owner_panic(&self, identity: TaskIdentity) {
        let mut state = self.state.lock().expect(COMPLETION_STATE_LOCK);
        Self::fail_locked(
            &mut state,
            format!("task {identity} completion owner panicked"),
        );
        self.changed.notify_one();
    }
}

/// The exact one-shot publisher attached to a running fragment.
#[derive(Clone)]
pub(super) struct TaskCompletionSignal {
    identity: TaskIdentity,
    shared: Arc<CompletionShared>,
}

impl TaskCompletionSignal {
    pub(super) fn publish(&self, fact: FragmentTerminalFact) {
        self.shared.publish(self.identity, fact);
    }

    pub(super) fn commit_creation(&self) {
        self.shared.commit_creation(self.identity);
    }
}

/// One fixed async owner and its bounded set of exact task slots.
pub(crate) struct TaskCompletionSupervisor {
    runtime: BackendDataRuntime,
    capacity: usize,
    shared: Arc<CompletionShared>,
    stop: watch::Sender<bool>,
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl fmt::Debug for TaskCompletionSupervisor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.shared.state.lock().expect(COMPLETION_STATE_LOCK);
        formatter
            .debug_struct("TaskCompletionSupervisor")
            .field("capacity", &self.capacity)
            .field("slots", &state.slots.len())
            .field("failed", &state.failure.is_some())
            .finish()
    }
}

impl TaskCompletionSupervisor {
    pub(crate) fn start(runtime: BackendDataRuntime, capacity: usize) -> Arc<Self> {
        assert!(capacity > 0, "task completion capacity must be positive");
        let (ready, mut receiver) = mpsc::channel(capacity);
        let shared = Arc::new(CompletionShared {
            state: Mutex::new(CompletionState {
                accepting: true,
                slots: HashMap::new(),
                failure: None,
            }),
            ready,
            changed: Notify::new(),
        });
        let (stop, mut stopped) = watch::channel(false);
        let owner_shared = Arc::clone(&shared);
        let join = runtime.handle().spawn(async move {
            loop {
                let identity = tokio::select! {
                    biased;
                    changed = stopped.changed() => {
                        if changed.is_err() || *stopped.borrow() {
                            return;
                        }
                        continue;
                    }
                    ready = receiver.recv() => match ready {
                        Some(identity) => identity,
                        None => return,
                    },
                };
                let Some((action, fact)) = owner_shared.take_ready(identity) else {
                    return;
                };
                if catch_unwind(AssertUnwindSafe(|| action(fact))).is_err() {
                    // No later convergence fact is fabricated. The task stays
                    // live and charged, and application supervision observes
                    // this owner failure.
                    owner_shared.note_owner_panic(identity);
                    return;
                }
            }
        });
        Arc::new(Self {
            runtime,
            capacity,
            shared,
            stop,
            join: Mutex::new(Some(join)),
        })
    }

    pub(super) fn reserve(
        &self,
        identity: TaskIdentity,
        expected_fragment: novarocks_types::UniqueId,
        action: CompletionAction,
    ) -> Result<TaskCompletionSignal, String> {
        let mut state = self.shared.state.lock().expect(COMPLETION_STATE_LOCK);
        if !state.accepting {
            return Err("task completion owner is shutting down".to_string());
        }
        if state.failure.is_some() {
            return Err("task completion owner is unavailable".to_string());
        }
        if state.slots.contains_key(&identity) {
            return Err(format!("task {identity} already has a completion slot"));
        }
        if state.slots.len() >= self.capacity {
            return Err(format!(
                "task completion owner reached its {} exact-slot bound",
                self.capacity
            ));
        }
        state.slots.insert(
            identity,
            CompletionSlot {
                expected_fragment,
                action: Some(action),
                fact: None,
                creation_committed: false,
                queued: false,
            },
        );
        Ok(TaskCompletionSignal {
            identity,
            shared: Arc::clone(&self.shared),
        })
    }

    pub(crate) fn poll_failure(&self) -> Option<String> {
        self.shared
            .state
            .lock()
            .expect(COMPLETION_STATE_LOCK)
            .failure
            .clone()
    }

    pub(crate) fn shutdown(&self) -> Result<(), String> {
        {
            let mut state = self.shared.state.lock().expect(COMPLETION_STATE_LOCK);
            state.accepting = false;
        }
        let shared = Arc::clone(&self.shared);
        let drain_result = self.runtime.block_on(async move {
            tokio::time::timeout(COMPLETION_SHUTDOWN_TIMEOUT, async {
                loop {
                    let changed = shared.changed.notified();
                    {
                        let state = shared.state.lock().expect(COMPLETION_STATE_LOCK);
                        if let Some(failure) = state.failure.as_ref() {
                            return Err(failure.clone());
                        }
                        if state.slots.is_empty() {
                            return Ok(());
                        }
                    }
                    changed.await;
                }
            })
            .await
            .map_err(|_| {
                "task completion owner did not drain its exact slots within 5000 ms".to_string()
            })?
        });
        let _ = self.stop.send(true);
        let join = self.join.lock().expect(COMPLETION_STATE_LOCK).take();
        let join_result = if let Some(join) = join {
            self.runtime
                .block_on(async move { join.await })
                .map_err(|error| format!("task completion owner stopped unexpectedly: {error}"))
        } else {
            Ok(())
        };
        match (drain_result, join_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(error), Err(join_error)) => Err(format!("{error}; {join_error}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use novarocks_execution::runtime::fragment::{FragmentOutcome, FragmentTerminalFact};
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    use super::TaskCompletionSupervisor;

    fn identity(query: i64, task: u32) -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(
                QueryId::new(query, query + 1),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("nonzero query"),
            StageId::new(1).expect("nonzero stage"),
            TaskId::new(task).expect("nonzero task"),
            BackendProcessId::new_v7(),
        )
    }

    fn terminal(identity: TaskIdentity, fragment: UniqueId) -> FragmentTerminalFact {
        FragmentTerminalFact::new(
            identity.query_execution_id().query_id(),
            fragment,
            FragmentOutcome::Succeeded,
            None,
        )
    }

    fn wait_for_count(count: &AtomicUsize, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while count.load(Ordering::Acquire) != expected && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(count.load(Ordering::Acquire), expected);
    }

    fn injected_owner_failure() {
        panic!("injected completion owner panic");
    }

    #[test]
    fn completion_before_creation_commit_is_delivered_once_after_commit() {
        let supervisor =
            TaskCompletionSupervisor::start(crate::rpc::runtime::test_backend_data_runtime(), 2);
        let identity = identity(91_001, 1);
        let fragment = UniqueId::new(91_003, 91_004);
        let completions = Arc::new(AtomicUsize::new(0));
        let action_completions = Arc::clone(&completions);
        let signal = supervisor
            .reserve(
                identity,
                fragment,
                Box::new(move |_| {
                    action_completions.fetch_add(1, Ordering::Release);
                }),
            )
            .expect("reserve exact slot");

        signal.publish(terminal(identity, fragment));
        signal.publish(terminal(identity, fragment));
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(completions.load(Ordering::Acquire), 0);

        signal.commit_creation();
        signal.commit_creation();
        wait_for_count(&completions, 1);
        signal.publish(terminal(identity, fragment));
        assert_eq!(completions.load(Ordering::Acquire), 1);
        supervisor.shutdown().expect("completion owner drains");
    }

    #[test]
    fn exact_slot_bound_rejects_excess_work_before_start() {
        let supervisor =
            TaskCompletionSupervisor::start(crate::rpc::runtime::test_backend_data_runtime(), 1);
        let first = identity(92_001, 1);
        let second = identity(92_011, 1);
        let first_fragment = UniqueId::new(92_003, 92_004);
        supervisor
            .reserve(first, first_fragment, Box::new(|_| {}))
            .expect("first slot fits");
        assert!(
            supervisor
                .reserve(second, UniqueId::new(92_013, 92_014), Box::new(|_| {}))
                .is_err()
        );
        // This fixture deliberately leaves an uncommitted slot; Drop stops
        // the owner without claiming that its action converged.
    }

    #[test]
    fn owner_panic_is_supervision_failure_and_does_not_run_later_steps() {
        let supervisor =
            TaskCompletionSupervisor::start(crate::rpc::runtime::test_backend_data_runtime(), 1);
        let identity = identity(93_001, 1);
        let fragment = UniqueId::new(93_003, 93_004);
        let actual_stop = Arc::new(AtomicUsize::new(0));
        let converged = Arc::new(AtomicUsize::new(0));
        let action_actual_stop = Arc::clone(&actual_stop);
        let action_converged = Arc::clone(&converged);
        let signal = supervisor
            .reserve(
                identity,
                fragment,
                Box::new(move |_| {
                    action_actual_stop.fetch_add(1, Ordering::Release);
                    injected_owner_failure();
                    action_converged.fetch_add(1, Ordering::Release);
                }),
            )
            .expect("reserve exact slot");
        signal.commit_creation();
        signal.publish(terminal(identity, fragment));

        let deadline = Instant::now() + Duration::from_secs(1);
        while supervisor.poll_failure().is_none() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(supervisor.poll_failure().is_some());
        assert_eq!(actual_stop.load(Ordering::Acquire), 1);
        assert_eq!(converged.load(Ordering::Acquire), 0);
        assert!(supervisor.shutdown().is_err());
    }
}

impl Drop for TaskCompletionSupervisor {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(join) = self.join.get_mut().expect(COMPLETION_STATE_LOCK).take() {
            join.abort();
        }
    }
}
