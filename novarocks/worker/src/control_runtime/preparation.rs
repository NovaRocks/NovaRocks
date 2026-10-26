// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Actual Accepted input and preparation custody. The fair queue owns the
//! original jobs; body slots, whole preparation and retained inbound capacity
//! have separate release conditions. No host call or opaque Drop runs locked.

use super::actors::{WorkerAction, WorkerActor};
use crate::{control_admission::Dimension, task_port::TaskPendingNotice};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{ActorRuntime, DriverError, EffectTicket},
    lease::{CapacityBundle, StepGuard, WorkLease},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::ConsumerKind,
    fair_queue::{FairQueueError, FairWorkQueue},
    identity::{ConsumerId, ScopeId, WorkId},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::{
    TaskCreationInput, TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact},
};
use std::{
    any::Any,
    collections::BTreeMap,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

type Key = (ScopeId, WorkId);
type PanicPayload = ManuallyDrop<Box<dyn Any + Send>>;

struct State {
    work: Option<WorkLease>,
    capacity: Option<CapacityBundle>,
    body_exited: bool,
    captures_exited: bool,
    exit_published: bool,
    body_report_retired: bool,
    exit_report_retired: bool,
    transferred: bool,
    unresolved: bool,
    // One original body and one independent input-disposal boundary. Panic
    // graphs are retained without invoking their destructor a second time.
    panics: [Option<PanicPayload>; 2],
}
struct Custody {
    task: TaskIdentity,
    parent: PreparationParent,
    input: Mutex<Option<ManuallyDrop<TaskCreationInput>>>,
    state: Mutex<State>,
    body_report: ReportSlot,
    exit_report: ReportSlot,
    preparing: ResourceVector,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
    retained: Mutex<Option<Arc<Self>>>,
}

/// This loan can derive only from the actual still-counted original work.
/// Closing external admission does not revoke existing drain authority;
/// stop/failure still rejects business and permits necessary cleanup.
#[derive(Clone)]
pub(crate) struct PreparationParent {
    scope: ScopeHandle,
    work: WorkId,
}
impl PreparationParent {
    pub(crate) fn derive(&self, kind: WorkKind) -> Result<StepGuard, RuntimeError> {
        let generation = self.scope.gates()?.generation;
        self.scope.reserve_next(
            generation,
            ResourceVector::default(),
            kind,
            WorkAdmission::Derived(self.work),
        )
    }
}

/// Only references to the original work/reports live here. No copied active,
/// preparing, byte or child counters constitute another admission authority.
#[derive(Clone)]
pub(crate) struct PreparationCustody(Arc<Custody>);

#[must_use = "The actual preparation body or unused input must exit explicitly"]
pub(crate) struct PreparationJob {
    custody: PreparationCustody,
    active: bool,
}
pub(crate) enum PreparationOutcome<T> {
    Returned(T),
    StartRefused(RuntimeError),
    Unresolved,
}
#[derive(Debug)]
pub(crate) enum PreparationAdoptionError {
    WrongEffect,
    Driver(DriverError),
    Runtime(RuntimeError),
}
impl std::fmt::Display for PreparationAdoptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongEffect => {
                f.write_str("preparation adoption requires the exact EnqueuePreparation effect")
            }
            Self::Driver(error) => write!(f, "preparation responsibility transfer failed: {error}"),
            Self::Runtime(error) => write!(f, "preparation report admission failed: {error}"),
        }
    }
}
impl std::error::Error for PreparationAdoptionError {}

pub(crate) const PREPARATION_CORE_CONSUMER: ConsumerId = ConsumerId::new(1).unwrap();
pub(crate) const PREPARATION_OBSERVER_CONSUMER: ConsumerId = ConsumerId::new(2).unwrap();
fn revoke_unbound_report(report: &ReportSlot) {
    report.revoke_unused().expect("unused preparation report");
    report
        .body_exited()
        .expect("unused report has no producer body");
    report
        .captures_exited()
        .expect("unused report has no captures");
    assert!(report.try_retire().expect("unused report retirement"));
}
impl PreparationCustody {
    /// Adopt the original, still-unbound Accepted guard directly from its
    /// committed batch. Report headers are installed before binding a payload;
    /// an admission/failure race rolls back only unused headers and that guard.
    /// It never allocates a second quota bundle or invents an exit observation.
    pub(crate) fn adopt(
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
        input: TaskCreationInput,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> Result<(Self, PreparationJob), (PreparationAdoptionError, TaskCreationInput)> {
        let (task, reservation) = match &ticket.effect {
            WorkerAction::Context(
                novarocks_worker_protocol::context::ContextEffect::EnqueuePreparation {
                    task,
                    reservation,
                    ..
                },
            ) => (*task, *reservation),
            _ => return Err((PreparationAdoptionError::WrongEffect, input)),
        };
        let guard = match runtime.take_step_reservation(ticket, reservation) {
            Ok(guard) => guard,
            Err(error) => return Err((PreparationAdoptionError::Driver(error), input)),
        };
        let scope = guard.scope();
        let generation = ticket.generation;
        let report = || {
            ReportSlot::reserve_next(
                scope,
                generation,
                guard.work_id(),
                2,
                [
                    (
                        PREPARATION_CORE_CONSUMER,
                        ConsumerKind::Owner {
                            scope: scope.id(),
                            generation,
                        },
                    ),
                    (PREPARATION_OBSERVER_CONSUMER, ConsumerKind::Observer),
                ],
            )
        };
        let body = match report() {
            Ok(report) => report,
            Err(error) => return Err((PreparationAdoptionError::Runtime(error), input)),
        };
        let exit = match report() {
            Ok(report) => report,
            Err(error) => {
                revoke_unbound_report(&body);
                return Err((PreparationAdoptionError::Runtime(error), input));
            }
        };
        let (work, capacity) = match guard.bind(WorkKind::Business) {
            Ok(bound) => bound,
            Err(error) => {
                revoke_unbound_report(&body);
                revoke_unbound_report(&exit);
                return Err((PreparationAdoptionError::Runtime(error), input));
            }
        };
        Ok(Self::from_admitted(
            task,
            work,
            capacity,
            [body, exit],
            input,
            pending,
            health,
        ))
    }
    /// All work, storage and both reports were admitted before attaching this
    /// input. The capacity is the same Accepted reservation from the Core step.
    /// The Task owner receives it back, with preparation charges removed, only
    /// after real whole exit and report retirement. Inbound charges stay in it.
    pub(crate) fn from_admitted(
        task: TaskIdentity,
        work: WorkLease,
        capacity: CapacityBundle,
        reports: [ReportSlot; 2],
        input: TaskCreationInput,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> (Self, PreparationJob) {
        let resources = capacity
            .resources()
            .expect("live original Accepted reservation");
        assert_eq!(resources.get(Dimension::PreparingTasks.id()), 1);
        let preparing = ResourceVector::new([
            (Dimension::PreparingTasks.id(), 1),
            (
                Dimension::PreparingBytes.id(),
                resources.get(Dimension::PreparingBytes.id()),
            ),
        ])
        .expect("fixed preparation release dimensions");
        let parent = PreparationParent {
            scope: work.scope().clone(),
            work: work.id(),
        };
        let [body_report, exit_report] = reports;
        let custody = Arc::new(Custody {
            task,
            parent,
            input: Mutex::new(Some(ManuallyDrop::new(input))),
            state: Mutex::new(State {
                work: Some(work),
                capacity: Some(capacity),
                body_exited: false,
                captures_exited: false,
                exit_published: false,
                body_report_retired: false,
                exit_report_retired: false,
                transferred: false,
                unresolved: false,
                panics: [None, None],
            }),
            body_report,
            exit_report,
            preparing,
            pending,
            health,
            retained: Mutex::new(None),
        });
        *custody.retained.lock().expect("preparation retention lock") = Some(custody.clone());
        let owner = Self(custody);
        let job = PreparationJob {
            custody: owner.clone(),
            active: true,
        };
        (owner, job)
    }
    fn key(&self) -> Key {
        (self.0.parent.scope.id(), self.0.parent.work)
    }
    pub(crate) fn body_report(&self) -> &ReportSlot {
        &self.0.body_report
    }
    pub(crate) fn exit_report(&self) -> &ReportSlot {
        &self.0.exit_report
    }
    fn take_input(&self) -> Option<TaskCreationInput> {
        self.0
            .input
            .lock()
            .expect("preparation input transfer lock")
            .take()
            .map(ManuallyDrop::into_inner)
    }
    fn unresolved(&self, panic: Option<(usize, Box<dyn Any + Send>)>) {
        {
            let mut state = self.0.state.lock().expect("preparation custody lock");
            state.unresolved = true;
            state
                .work
                .as_ref()
                .expect("preparation work")
                .mark_unresolved();
            if let Some((slot, payload)) = panic {
                assert!(
                    state.panics[slot].is_none(),
                    "one original panic per boundary"
                );
                state.panics[slot] = Some(ManuallyDrop::new(payload));
            }
        }
        self.0.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.0.task,
                phase: TaskFailurePhase::Preparation,
            },
            FailureFact::new(
                FailureClass::Invariant,
                crate::failure_reason::WorkerFailureReason::PreparationChildUnresolved.reason(),
            ),
        );
        self.0.pending.pending();
    }
    fn observe_body(&self, captures_exited: bool) {
        let mut state = self.0.state.lock().expect("preparation custody lock");
        assert!(!state.body_exited, "one original preparation body exit");
        state
            .work
            .as_mut()
            .expect("preparation work")
            .body_exited_with_report(&self.0.body_report)
            .expect("prepaid preparation body report");
        state.body_exited = true;
        self.0
            .body_report
            .body_exited()
            .expect("body report producer exit");
        if captures_exited {
            state
                .work
                .as_mut()
                .expect("preparation work")
                .captures_exited()
                .expect("actual preparation capture exit");
            state.captures_exited = true;
            self.0
                .body_report
                .captures_exited()
                .expect("body report capture exit");
        }
        drop(state);
        self.0.pending.pending();
    }
    /// The physical worker can already be reused. Children, input unwind and
    /// independent report consumers can still prevent whole exit/charge reuse.
    pub(crate) fn publish_whole_exit(&self) -> Result<bool, RuntimeError> {
        let mut state = self.0.state.lock().expect("preparation custody lock");
        if state.exit_published {
            return Ok(true);
        }
        if state.unresolved || !state.body_exited || !state.captures_exited {
            return Ok(false);
        }
        if !state
            .work
            .as_ref()
            .expect("preparation work")
            .actual_exit_ready()?
        {
            return Ok(false);
        }
        self.0.exit_report.fill()?;
        self.0.exit_report.body_exited()?;
        self.0.exit_report.captures_exited()?;
        state.exit_published = true;
        drop(state);
        self.0.pending.pending();
        Ok(true)
    }
    /// Transfer the original bundle once. Only P/count/bytes leave the ledger;
    /// Task storage and retained normal-close charges remain in the same bundle.
    pub(crate) fn take_after_exit(&self) -> Result<Option<CapacityBundle>, RuntimeError> {
        let mut state = self.0.state.lock().expect("preparation custody lock");
        if state.transferred || !state.exit_published || state.unresolved {
            return Ok(None);
        }
        if !state.body_report_retired {
            state.body_report_retired = self.0.body_report.try_retire()?;
        }
        if !state.exit_report_retired {
            state.exit_report_retired = self.0.exit_report.try_retire()?;
        }
        if !state.body_report_retired || !state.exit_report_retired {
            return Ok(None);
        }
        let capacity = state.capacity.as_mut().expect("original Accepted bundle");
        capacity.release(&self.0.preparing)?;
        let capacity = state.capacity.take();
        let work = state.work.take();
        state.transferred = true;
        drop(state);
        drop(work);
        let retained = self
            .0
            .retained
            .lock()
            .expect("preparation retention lock")
            .take();
        drop(retained);
        Ok(capacity)
    }
}
impl PreparationJob {
    fn key(&self) -> Key {
        self.custody.key()
    }
    /// Invoked by an actual host worker, never an actor poller. Start checks
    /// the current scope stop/failure gate; an old Accepted fact cannot grant
    /// execution. A refused job still really disposes its original input.
    pub(crate) fn run<T>(
        mut self,
        host: impl FnOnce(TaskCreationInput, PreparationParent) -> T,
    ) -> PreparationOutcome<T> {
        let start = self
            .custody
            .0
            .state
            .lock()
            .expect("preparation custody lock")
            .work
            .as_mut()
            .expect("preparation work")
            .start();
        let result = catch_unwind(AssertUnwindSafe(|| match start {
            Ok(()) => PreparationOutcome::Returned(host(
                self.custody
                    .take_input()
                    .expect("one actual preparation input"),
                self.custody.0.parent.clone(),
            )),
            Err(error) => {
                drop(host);
                drop(self.custody.take_input());
                PreparationOutcome::StartRefused(error)
            }
        }));
        let outcome = match result {
            Ok(outcome) => {
                self.custody.observe_body(true);
                outcome
            }
            Err(payload) => {
                self.custody.unresolved(Some((0, payload)));
                // A rejected start can unwind while disposing the callback,
                // leaving its input in original custody. Dispose separately.
                let input = self.custody.take_input();
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(input))) {
                    self.custody.unresolved(Some((1, payload)));
                }
                self.custody.observe_body(false);
                PreparationOutcome::Unresolved
            }
        };
        self.active = false;
        outcome
    }
    /// Cancellation removes a queued job; this host-side disposal is the
    /// positive exit observation. Queue removal/Drop alone proves no exit.
    pub(crate) fn dispose(mut self) -> bool {
        let input = self.custody.take_input();
        let result = catch_unwind(AssertUnwindSafe(|| drop(input)));
        let exited = match result {
            Ok(()) => true,
            Err(payload) => {
                self.custody.unresolved(Some((1, payload)));
                false
            }
        };
        self.custody.observe_body(exited);
        self.active = false;
        exited
    }
}
impl Drop for PreparationJob {
    fn drop(&mut self) {
        if self.active {
            self.custody.unresolved(None);
        }
    }
}

struct QueueState {
    fair: FairWorkQueue,
    jobs: BTreeMap<Key, ManuallyDrop<PreparationJob>>,
    running: usize,
}
struct Queue {
    state: Mutex<QueueState>,
    workers: usize,
}
#[derive(Clone)]
pub(crate) struct PreparationQueue(Arc<Queue>);
pub(crate) struct RunningPreparation {
    queue: PreparationQueue,
    key: Key,
    job: Option<PreparationJob>,
}
impl PreparationQueue {
    pub(crate) fn new(contexts: usize, jobs: usize, per_context: usize, workers: usize) -> Self {
        assert!(contexts > 0 && jobs > 0 && per_context > 0 && workers > 0);
        Self(Arc::new(Queue {
            state: Mutex::new(QueueState {
                fair: FairWorkQueue::new(contexts, jobs, per_context),
                jobs: BTreeMap::new(),
                running: 0,
            }),
            workers,
        }))
    }
    pub(crate) fn enqueue(
        &self,
        job: PreparationJob,
    ) -> Result<(), (FairQueueError, PreparationJob)> {
        let key = job.key();
        let mut state = self.0.state.lock().expect("preparation queue lock");
        if let Err(error) = state.fair.enqueue(key.0, key.1) {
            drop(state);
            return Err((error, job));
        }
        assert!(state.jobs.insert(key, ManuallyDrop::new(job)).is_none());
        Ok(())
    }
    pub(crate) fn pop(&self) -> Option<RunningPreparation> {
        let mut state = self.0.state.lock().expect("preparation queue lock");
        if state.running == self.0.workers {
            return None;
        }
        let key = state.fair.pop()?;
        let job = state
            .jobs
            .remove(&key)
            .expect("original queued preparation");
        state.running += 1;
        drop(state);
        Some(RunningPreparation {
            queue: self.clone(),
            key,
            job: Some(ManuallyDrop::into_inner(job)),
        })
    }
    /// The caller disposes the returned original on a host cleanup executor.
    pub(crate) fn cancel(
        &self,
        scope: ScopeId,
        work: WorkId,
    ) -> Result<PreparationJob, FairQueueError> {
        let mut state = self.0.state.lock().expect("preparation queue lock");
        state.fair.remove_queued(scope, work)?;
        let job = state
            .jobs
            .remove(&(scope, work))
            .expect("original canceled job");
        drop(state);
        Ok(ManuallyDrop::into_inner(job))
    }
}
impl RunningPreparation {
    pub(crate) fn run<T>(
        mut self,
        host: impl FnOnce(TaskCreationInput, PreparationParent) -> T,
    ) -> PreparationOutcome<T> {
        let result = self.job.take().expect("one actual running body").run(host);
        // Drop returns only the physical body slot. Original work, whole exit
        // reports, child obligations and P/bytes remain in PreparationCustody.
        drop(self);
        result
    }
}
impl Drop for RunningPreparation {
    fn drop(&mut self) {
        // Dropping an uninvoked body leaves its original work unresolved.
        let job = self.job.take();
        {
            let mut state = self.queue.0.state.lock().expect("preparation queue lock");
            state
                .fair
                .finish(self.key.0, self.key.1)
                .expect("actual running preparation body slot");
            state.running -= 1;
        }
        drop(job);
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        let jobs =
            std::mem::take(&mut self.state.get_mut().expect("exclusive queue teardown").jobs);
        for job in jobs.into_values() {
            drop(ManuallyDrop::into_inner(job));
        }
    }
}

#[cfg(test)]
mod tests;
