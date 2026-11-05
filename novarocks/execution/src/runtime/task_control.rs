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
//! Scoped failure publication and stop observation of one exact Task owner.
//! This view never chooses a first cause, turns a notification into a decision,
//! or keeps an independent logical conclusion.
use crate::runtime::{
    observable::Observable,
    task::{ExecutionFailure, ExecutionReason, ExecutionResult},
};
#[cfg(test)]
use novarocks_execution_contract::{AbortCause, CancelReason};
use novarocks_execution_contract::{
    AcceptedTaskConclusion, TaskConclusion, TaskConclusionPort, TaskConclusionProposal,
    TaskFailurePhase, TaskIdentity, TerminationDetail,
};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
use std::{
    error::Error,
    fmt,
    sync::{Arc, OnceLock},
    time::Duration,
};

/// Worker implements this on the same acceptance domain used by control and
/// lifecycle. `is_stopped` is the owner's cheap presence view; waits register
/// against that same domain. Notifications are readiness, not an arbiter.
pub trait TaskExecutionPort: TaskConclusionPort<ExecutionFailure> {
    /// The exact original runtime health, with no Task acceptance authority.
    fn execution_health(&self) -> Arc<crate::runtime::execution_health::ExecutionHealth>;
    /// Secondary cleanup never submits or refines a Task conclusion. The
    /// original rich source stays borrowed, outside the acceptance fence.
    fn report_cleanup(&self, cause: &ExecutionFailure, phase: TaskFailurePhase);
    /// Physical exit is unproved. The actual owner retains work; this reports
    /// only neutral facts to the process supervisor, without accepting a Task.
    fn report_unresolved_exit(&self, fact: FailureFact, phase: TaskFailurePhase);
    fn is_stopped(&self) -> bool;
    fn stop_observable(&self) -> Arc<Observable>;
    fn wait_until_stopped(&self, duration: Duration) -> bool;
}

#[derive(Debug)]
pub struct TaskControlIdentityMismatch;
impl fmt::Display for TaskControlIdentityMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Task conclusion port identity does not match its execution binding")
    }
}
impl Error for TaskControlIdentityMismatch {}
impl FailureProjection for TaskControlIdentityMismatch {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::TaskControlIdentityMismatch.reason(),
        )
    }
}

#[derive(Debug)]
pub struct MissingTaskExecutionControl;
impl fmt::Display for MissingTaskExecutionControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Task execution requires its accepted conclusion owner")
    }
}
impl Error for MissingTaskExecutionControl {}
impl FailureProjection for MissingTaskExecutionControl {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::MissingTaskExecutionControl.reason(),
        )
    }
}

/// A detached control observation, never a replacement Task conclusion or a
/// reconstructed copy of a retired owner's originating failure graph.
#[derive(Debug)]
pub struct TaskExecutionOwnerRetired {
    identity: TaskIdentity,
}
impl TaskExecutionOwnerRetired {
    pub const fn new(identity: TaskIdentity) -> Self {
        Self { identity }
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
}
impl fmt::Display for TaskExecutionOwnerRetired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Task execution control owner has retired")
    }
}
impl Error for TaskExecutionOwnerRetired {}
impl FailureProjection for TaskExecutionOwnerRetired {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::TaskExecutionOwnerRetired.reason(),
        )
    }
}

/// A detached neutral metadata reporter. It cannot retain a Task port, a
/// source graph, RuntimeState, input or work resource after control retirement.
#[derive(Clone)]
pub(crate) struct ExecutionExitReporter {
    health: Arc<crate::runtime::execution_health::ExecutionHealth>,
    origin: crate::runtime::execution_health::ExecutionExitOrigin,
}
impl ExecutionExitReporter {
    pub(crate) fn new(
        health: Arc<crate::runtime::execution_health::ExecutionHealth>,
        origin: crate::runtime::execution_health::ExecutionExitOrigin,
    ) -> Self {
        Self { health, origin }
    }
    pub(crate) fn report(&self, fact: FailureFact) {
        self.health.report_unresolved(self.origin, fact);
    }
    #[cfg(test)]
    pub(crate) fn failure(&self) -> Option<crate::runtime::execution_health::ExecutionExitFault> {
        self.health.failure()
    }
}

/// A preparation or execution scope borrows the same authority. Keeping this
/// narrow owned capability in a job does not retain a RuntimeState, driver,
/// expression arena, input, construction factory or thread work lease.
pub struct TaskExecutionControl {
    identity: TaskIdentity,
    owner: Arc<dyn TaskExecutionPort>,
    phase: TaskFailurePhase,
    /// A local control observation is separate from an originating failure.
    /// Only control receipts use this diagnostic cache. A later legal derived
    /// refinement reads its actual originating source directly from the owner.
    observed_control: OnceLock<ExecutionFailure>,
    exit_reporter: ExecutionExitReporter,
}
impl fmt::Debug for TaskExecutionControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskExecutionControl")
            .field("identity", &self.identity)
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}
impl TaskExecutionControl {
    pub fn new(
        identity: TaskIdentity,
        owner: Arc<dyn TaskExecutionPort>,
        phase: TaskFailurePhase,
    ) -> Result<Self, TaskControlIdentityMismatch> {
        if identity != owner.identity() {
            return Err(TaskControlIdentityMismatch);
        }
        let exit_reporter = ExecutionExitReporter::new(
            owner.execution_health(),
            crate::runtime::execution_health::ExecutionExitOrigin::Task { identity, phase },
        );
        Ok(Self {
            identity,
            owner,
            exit_reporter,
            phase,
            observed_control: OnceLock::new(),
        })
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub fn in_phase(&self, phase: TaskFailurePhase) -> Self {
        Self {
            identity: self.identity,
            owner: Arc::clone(&self.owner),
            phase,
            observed_control: OnceLock::new(),
            exit_reporter: ExecutionExitReporter::new(
                self.exit_reporter.health.clone(),
                crate::runtime::execution_health::ExecutionExitOrigin::Task {
                    identity: self.identity,
                    phase,
                },
            ),
        }
    }
    pub(crate) fn exit_health_reporter(&self) -> ExecutionExitReporter {
        self.exit_reporter.clone()
    }
    pub fn report_failure(&self, cause: ExecutionFailure) {
        self.owner.submit(TaskConclusionProposal::Failed {
            cause,
            phase: self.phase,
        })
    }
    pub fn report_cleanup(&self, cause: &ExecutionFailure) {
        self.owner.report_cleanup(cause, self.phase);
    }
    pub fn report_unresolved_exit(&self, cause: &ExecutionFailure) {
        let fact = cause.failure_fact();
        self.owner.report_unresolved_exit(fact, self.phase);
    }
    pub fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>> {
        self.owner.accepted()
    }
    pub fn is_stopped(&self) -> bool {
        self.owner.is_stopped()
    }
    pub fn stop_observable(&self) -> Arc<Observable> {
        self.owner.stop_observable()
    }
    pub fn wait_until_stopped(&self, duration: Duration) -> bool {
        self.owner.wait_until_stopped(duration)
    }
    /// Original failures remain the exact accepted source. Control commands
    /// have no originating failure: this is their typed local observation,
    /// never a reconstructed failure standing in for a source held elsewhere.
    pub fn error(&self) -> Option<ExecutionFailure> {
        if !self.is_stopped() {
            return None;
        }
        let Some(accepted) = self.owner.accepted() else {
            let observation = TaskExecutionOwnerRetired::new(self.identity);
            self.exit_reporter.report(observation.failure_fact());
            return Some(ExecutionFailure::from_domain(observation));
        };
        if let Some(original) = accepted.originating_failure() {
            return Some(original.clone());
        }
        match accepted.neutral().conclusion() {
            TaskConclusion::Terminated(
                TerminationDetail::Canceled(_) | TerminationDetail::Aborted(_),
            ) => Some(
                self.observed_control
                    .get_or_init(|| ExecutionFailure::from_domain(TaskStopObservation(accepted)))
                    .clone(),
            ),
            TaskConclusion::Terminated(TerminationDetail::Failed(_)) => {
                unreachable!("accepted failure receipt requires its original cause")
            }
            TaskConclusion::Succeeded => None,
        }
    }
    pub fn wait_interruptibly(&self, duration: Duration) -> ExecutionResult<()> {
        if !self.wait_until_stopped(duration) {
            return Ok(());
        }
        match self.error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
impl crate::runtime::task::ExecutionFailureReporter for TaskExecutionControl {
    fn report_cleanup(&self, cause: &ExecutionFailure) {
        TaskExecutionControl::report_cleanup(self, cause);
    }
    fn report_unresolved_exit(&self, cause: &ExecutionFailure) {
        TaskExecutionControl::report_unresolved_exit(self, cause);
    }
    fn report_failure(&self, error: ExecutionFailure) {
        TaskExecutionControl::report_failure(self, error);
    }
}

#[derive(Debug)]
struct TaskStopObservation(Arc<AcceptedTaskConclusion<ExecutionFailure>>);
impl fmt::Display for TaskStopObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.neutral().conclusion() {
            TaskConclusion::Terminated(TerminationDetail::Canceled(reason)) => {
                write!(f, "Task execution observed accepted cancellation: {reason}")
            }
            TaskConclusion::Terminated(TerminationDetail::Aborted(reason)) => {
                write!(f, "Task execution observed accepted abort: {reason}")
            }
            _ => unreachable!("only accepted control conclusions create a stop observation"),
        }
    }
}
impl Error for TaskStopObservation {}
impl FailureProjection for TaskStopObservation {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::stop_observation(ExecutionReason::TaskControlObservedStop.reason())
    }
}

#[cfg(any(test, novarocks_dev_controls))]
pub(crate) mod test_support {
    use super::*;
    #[cfg(test)]
    use novarocks_execution_contract::SettledContinuation;
    use novarocks_execution_contract::{
        AcceptedConclusion, ConclusionDecision, TaskFailure, decide_conclusion,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::{
        num::{NonZeroU16, NonZeroU64},
        sync::{
            Barrier, Condvar, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    type Snapshot = Arc<AcceptedTaskConclusion<ExecutionFailure>>;
    #[derive(Debug)]
    struct UnownedStop(ExecutionFailure);
    impl fmt::Display for UnownedStop {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Task observed stop without an accepted conclusion")
        }
    }
    impl Error for UnownedStop {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.0)
        }
    }
    impl FailureProjection for UnownedStop {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Unexpected,
                novarocks_failure_contract::FailureReason::new(
                    novarocks_failure_contract::domain::EXECUTION,
                    NonZeroU16::new(65000).unwrap(),
                ),
            )
        }
    }
    #[cfg(test)]
    mod finite_fixture {
        use super::*;
        // Each fixture prepays exactly one fragment (three named originals plus
        // one settlement child) and two independent observation roles. Issuing a
        // permit consumes its original position; registration never adds credit.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(super) enum FixtureCompletionPhase {
            Reserved,
            Issued,
            Started,
            Exited,
            Unresolved,
        }
        /// Finite inventory of original completion positions. Individual
        /// capabilities author transitions; sharing this inventory creates no
        /// additional completion capability.
        pub(super) struct FixtureCompletionInventory(Mutex<[FixtureCompletionPhase; 6]>);
        impl FixtureCompletionInventory {
            pub(super) fn new() -> Arc<Self> {
                Arc::new(Self(Mutex::new([FixtureCompletionPhase::Reserved; 6])))
            }
            pub(super) fn issue(&self, indexes: &[usize]) {
                let mut phases = self.0.lock().unwrap();
                assert!(
                    indexes
                        .iter()
                        .all(|i| phases[*i] == FixtureCompletionPhase::Reserved),
                    "fixture role admission is finite and cannot be repeated"
                );
                for i in indexes {
                    phases[*i] = FixtureCompletionPhase::Issued;
                }
            }
            fn transition(
                &self,
                index: usize,
                expected: FixtureCompletionPhase,
                next: FixtureCompletionPhase,
            ) {
                let mut phases = self.0.lock().unwrap();
                assert_eq!(
                    phases[index], expected,
                    "the original fixture role owns its exit"
                );
                phases[index] = next;
            }
            pub(super) fn take_observation(&self) -> Option<usize> {
                let mut phases = self.0.lock().unwrap();
                let index = (4..6).find(|i| phases[*i] == FixtureCompletionPhase::Reserved)?;
                phases[index] = FixtureCompletionPhase::Issued;
                Some(index)
            }
            pub(super) fn phases(&self) -> [FixtureCompletionPhase; 6] {
                *self.0.lock().unwrap()
            }
        }
        struct FixtureOriginal {
            pool: Arc<FixtureCompletionInventory>,
            index: usize,
            started: bool,
        }
        impl novarocks_execution_contract::CompletionWorkCapability for FixtureOriginal {
            fn started(&mut self) {
                self.pool.transition(
                    self.index,
                    FixtureCompletionPhase::Issued,
                    FixtureCompletionPhase::Started,
                );
                self.started = true;
            }
            fn completed(self: Box<Self>) {
                self.pool.transition(
                    self.index,
                    if self.started {
                        FixtureCompletionPhase::Started
                    } else {
                        FixtureCompletionPhase::Issued
                    },
                    FixtureCompletionPhase::Exited,
                );
            }
            fn unused_exited(self: Box<Self>) {
                self.pool.transition(
                    self.index,
                    FixtureCompletionPhase::Issued,
                    FixtureCompletionPhase::Exited,
                );
            }
            fn unresolved(self: Box<Self>) {
                self.pool.transition(
                    self.index,
                    if self.started {
                        FixtureCompletionPhase::Started
                    } else {
                        FixtureCompletionPhase::Issued
                    },
                    FixtureCompletionPhase::Unresolved,
                );
            }
        }
        type RegistrationRoute = Box<dyn FnOnce(SettledContinuation<ExecutionFailure>) + Send>;
        struct FixturePermit {
            pool: Arc<FixtureCompletionInventory>,
            index: usize,
            route: RegistrationRoute,
        }
        struct FixtureCall {
            pool: Arc<FixtureCompletionInventory>,
            index: usize,
            exited: bool,
        }
        impl Drop for FixtureCall {
            fn drop(&mut self) {
                self.pool.transition(
                    self.index,
                    FixtureCompletionPhase::Started,
                    if self.exited {
                        FixtureCompletionPhase::Exited
                    } else {
                        FixtureCompletionPhase::Unresolved
                    },
                );
            }
        }
        impl novarocks_execution_contract::SettlementCapability<ExecutionFailure> for FixturePermit {
            fn register(self: Box<Self>, continuation: SettledContinuation<ExecutionFailure>) {
                let Self { pool, index, route } = *self;
                route(Box::new(move |snapshot| {
                    pool.transition(
                        index,
                        FixtureCompletionPhase::Issued,
                        FixtureCompletionPhase::Started,
                    );
                    let mut call = FixtureCall {
                        pool,
                        index,
                        exited: false,
                    };
                    continuation(snapshot);
                    call.exited = true;
                }));
            }
            fn revoke_unused(self: Box<Self>) {
                self.pool.transition(
                    self.index,
                    FixtureCompletionPhase::Issued,
                    FixtureCompletionPhase::Exited,
                );
            }
        }
        pub(super) fn fixture_permit(
            identity: TaskIdentity,
            pool: Arc<FixtureCompletionInventory>,
            index: usize,
            route: RegistrationRoute,
        ) -> novarocks_execution_contract::SettlementPermit<ExecutionFailure> {
            novarocks_execution_contract::SettlementPermit::from_admitted_reservation(
                identity,
                Box::new(FixturePermit { pool, index, route }),
            )
        }
        pub(super) fn fixture_completion_work(
            identity: TaskIdentity,
            pool: Arc<FixtureCompletionInventory>,
            route: RegistrationRoute,
        ) -> novarocks_execution_contract::AdmittedWorkParts<ExecutionFailure> {
            pool.issue(&[0, 1, 2, 3]);
            let permit = fixture_permit(identity, pool.clone(), 3, route);
            let work = |index| {
                Box::new(FixtureOriginal {
                    pool: pool.clone(),
                    index,
                    started: false,
                })
                    as Box<dyn novarocks_execution_contract::CompletionWorkCapability>
            };
            novarocks_execution_contract::AdmittedWorkParts::from_admitted_parts(
                novarocks_execution_contract::AdmittedFinisher::from_admitted_parts(
                    permit,
                    work(0),
                ),
                work(1),
                work(2),
            )
        }
    }
    #[cfg(test)]
    use finite_fixture::{FixtureCompletionInventory, fixture_completion_work, fixture_permit};
    // Project a source on its reporting thread before either queue or owner lock.
    struct PreparedConclusion {
        reported_evidence: Option<novarocks_failure_contract::FailureEvidence>,
        conclusion: TaskConclusion,
        origin: Option<ExecutionFailure>,
    }
    pub(crate) struct Owner {
        health: Arc<crate::runtime::execution_health::ExecutionHealth>,
        cleanup: Mutex<[Option<FailureFact>; 4]>,
        identity: TaskIdentity,
        #[cfg(test)]
        completion_inventory: Arc<FixtureCompletionInventory>,
        accepted: Mutex<Option<Snapshot>>,
        stopped: AtomicBool,
        cv: Condvar,
        observable: Arc<Observable>,
        before_notify: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
    }
    impl Owner {
        pub(crate) fn new() -> Arc<Self> {
            static NEXT: AtomicUsize = AtomicUsize::new(1);
            let sequence = i64::try_from(NEXT.fetch_add(1, Ordering::Relaxed)).unwrap();
            let identity = TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(3901, sequence), AttemptId::new(1).unwrap())
                    .unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            );
            Arc::new(Self {
                health: Arc::new(crate::runtime::execution_health::ExecutionHealth::default()),
                cleanup: Mutex::new([None; 4]),
                identity,
                #[cfg(test)]
                completion_inventory: FixtureCompletionInventory::new(),
                accepted: Mutex::new(None),
                stopped: AtomicBool::new(false),
                cv: Condvar::new(),
                observable: Arc::new(Observable::new()),
                before_notify: Mutex::new(None),
            })
        }
        pub(crate) fn view(self: &Arc<Self>, phase: TaskFailurePhase) -> TaskExecutionControl {
            TaskExecutionControl::new(self.identity, self.clone(), phase).unwrap()
        }
        #[cfg(test)]
        pub(crate) fn issue_fixture_completion_work(
            self: &Arc<Self>,
        ) -> novarocks_execution_contract::AdmittedWorkParts<ExecutionFailure> {
            let owner = self.clone();
            fixture_completion_work(
                self.identity,
                self.completion_inventory.clone(),
                Box::new(move |callback| callback(owner.accepted())),
            )
        }
        #[cfg(test)]
        pub(crate) fn admit_barrier(
            self: &Arc<Self>,
        ) -> novarocks_execution_contract::SettlementPermit<ExecutionFailure> {
            let index = self
                .completion_inventory
                .take_observation()
                .expect("both prepaid observation roles were consumed");
            let owner = self.clone();
            fixture_permit(
                self.identity,
                self.completion_inventory.clone(),
                index,
                Box::new(move |callback| callback(owner.accepted())),
            )
        }
    }
    impl Owner {
        fn prepare(proposed: TaskConclusionProposal<ExecutionFailure>) -> PreparedConclusion {
            // This fixture owns one Task domain. The real Worker owner, its
            // status publication and terminal seal are integrated separately.
            let mut reported_evidence = None;
            let (conclusion, origin) = match proposed {
                TaskConclusionProposal::Succeeded => (TaskConclusion::Succeeded, None),
                TaskConclusionProposal::Canceled(reason) => (
                    TaskConclusion::Terminated(TerminationDetail::Canceled(reason)),
                    None,
                ),
                TaskConclusionProposal::Aborted(reason) => (
                    TaskConclusion::Terminated(TerminationDetail::Aborted(reason)),
                    None,
                ),
                TaskConclusionProposal::Failed { cause, phase } => {
                    let fact = cause.failure_fact();
                    let evidence = fact.evidence();
                    reported_evidence = Some(evidence);
                    let (cause, fact) = if evidence
                        == novarocks_failure_contract::FailureEvidence::StopObservation
                    {
                        let cause = ExecutionFailure::from_domain(UnownedStop(cause));
                        let fact = cause.failure_fact();
                        (cause, fact)
                    } else {
                        (cause, fact)
                    };
                    (
                        TaskConclusion::Terminated(TerminationDetail::Failed(
                            TaskFailure::new_in_phase(
                                fact,
                                cause.safe_detail_for_fact(fact),
                                phase,
                            ),
                        )),
                        Some(cause),
                    )
                }
            };

            PreparedConclusion {
                reported_evidence,
                conclusion,
                origin,
            }
        }
        fn adjudicate(&self, proposed: TaskConclusionProposal<ExecutionFailure>) {
            self.adjudicate_prepared(Self::prepare(proposed));
        }
        fn adjudicate_prepared(&self, prepared: PreparedConclusion) {
            let PreparedConclusion {
                reported_evidence,
                conclusion,
                origin,
            } = prepared;
            let mut current = self.accepted.lock().unwrap();
            let held = current
                .as_ref()
                .map(|snapshot| snapshot.neutral().conclusion());
            let decision = if let Some(evidence) = reported_evidence {
                novarocks_execution_contract::decide_reported_failure(held, &conclusion, evidence)
            } else {
                decide_conclusion(held, &conclusion)
            };
            if decision != ConclusionDecision::Keep {
                let generation = current.as_ref().map_or(1, |snapshot| {
                    snapshot
                        .neutral()
                        .generation()
                        .get()
                        .checked_add(1)
                        .unwrap()
                });
                let receipt = AcceptedConclusion::new(
                    self.identity,
                    NonZeroU64::new(generation).unwrap(),
                    conclusion,
                );
                *current = Some(Arc::new(
                    AcceptedTaskConclusion::new(receipt, origin).unwrap(),
                ));
                self.stopped.store(current.is_some(), Ordering::Release);
            }
            drop(current);
            if decision != ConclusionDecision::Keep {
                let gap = self.before_notify.lock().unwrap().take();
                if let Some((entered, release)) = gap {
                    entered.wait();
                    release.wait();
                }
                self.cv.notify_all();
                self.observable.notify_observers();
            }
        }
    }
    impl TaskConclusionPort<ExecutionFailure> for Owner {
        fn identity(&self) -> TaskIdentity {
            self.identity
        }
        fn accepted(&self) -> Option<Snapshot> {
            self.accepted.lock().unwrap().clone()
        }
        fn submit(&self, proposed: TaskConclusionProposal<ExecutionFailure>) {
            self.adjudicate(proposed);
        }
    }
    impl TaskExecutionPort for Owner {
        fn execution_health(&self) -> Arc<crate::runtime::execution_health::ExecutionHealth> {
            self.health.clone()
        }
        fn report_cleanup(&self, cause: &ExecutionFailure, _: TaskFailurePhase) {
            let fact = cause.failure_fact();
            let mut cleanup = self.cleanup.lock().unwrap();
            if let Some(slot) = cleanup.iter_mut().find(|slot| slot.is_none()) {
                *slot = Some(fact);
            }
        }
        fn report_unresolved_exit(&self, fact: FailureFact, phase: TaskFailurePhase) {
            self.health.report_unresolved(
                crate::runtime::execution_health::ExecutionExitOrigin::Task {
                    identity: self.identity,
                    phase,
                },
                fact,
            );
        }
        fn is_stopped(&self) -> bool {
            self.stopped.load(Ordering::Acquire)
        }
        fn stop_observable(&self) -> Arc<Observable> {
            self.observable.clone()
        }
        fn wait_until_stopped(&self, duration: Duration) -> bool {
            let guard = self.accepted.lock().unwrap();
            let (_guard, _) = self
                .cv
                .wait_timeout_while(guard, duration, |_| !self.stopped.load(Ordering::Acquire))
                .unwrap();
            self.stopped.load(Ordering::Acquire)
        }
    }
    /// A manually driven owner for proposal-order and settlement contracts.
    /// Only one test pump advances it; submissions may come from any thread.
    #[cfg(test)]
    pub(crate) struct DeferredOwner {
        owner: Arc<Owner>,
        queue: Mutex<DeferredQueue>,
    }
    #[cfg(test)]
    #[derive(Default)]
    struct DeferredQueue {
        inputs: std::collections::VecDeque<DeferredInput>,
        failed: bool,
        failure_published: bool,
        dropped: usize,
    }
    #[cfg(test)]
    enum DeferredInput {
        Proposal(Box<PreparedConclusion>),
        Settle(SettledContinuation<ExecutionFailure>),
    }
    #[cfg(test)]
    impl DeferredOwner {
        pub(crate) fn new() -> Arc<Self> {
            Arc::new(Self {
                owner: Owner::new(),
                queue: Mutex::new(DeferredQueue::default()),
            })
        }
        pub(crate) fn view(self: &Arc<Self>, phase: TaskFailurePhase) -> TaskExecutionControl {
            TaskExecutionControl::new(self.identity(), self.clone(), phase).unwrap()
        }
        #[cfg(test)]
        pub(crate) fn issue_fixture_completion_work(
            self: &Arc<Self>,
        ) -> novarocks_execution_contract::AdmittedWorkParts<ExecutionFailure> {
            let owner = self.clone();
            fixture_completion_work(
                self.identity(),
                self.owner.completion_inventory.clone(),
                Box::new(move |callback| owner.route_admitted(callback)),
            )
        }
        #[cfg(test)]
        pub(crate) fn admit_barrier(
            self: &Arc<Self>,
        ) -> novarocks_execution_contract::SettlementPermit<ExecutionFailure> {
            let index = self
                .owner
                .completion_inventory
                .take_observation()
                .expect("both prepaid observation roles were consumed");
            let owner = self.clone();
            fixture_permit(
                self.identity(),
                self.owner.completion_inventory.clone(),
                index,
                Box::new(move |callback| owner.route_admitted(callback)),
            )
        }
        pub(crate) fn pending_len(&self) -> usize {
            self.queue.lock().unwrap().inputs.len()
        }
        pub(crate) fn pending_settlements(&self) -> usize {
            self.queue
                .lock()
                .unwrap()
                .inputs
                .iter()
                .filter(|input| matches!(input, DeferredInput::Settle(_)))
                .count()
        }
        pub(crate) fn dropped_proposals(&self) -> usize {
            self.queue.lock().unwrap().dropped
        }
        pub(crate) fn advance_one(&self) -> bool {
            // Callbacks may submit or register another continuation. No queue
            // lock covers adjudication, notification, destruction or callbacks.
            let input = self.queue.lock().unwrap().inputs.pop_front();
            match input {
                Some(DeferredInput::Proposal(prepared)) => {
                    self.owner.adjudicate_prepared(*prepared)
                }
                Some(DeferredInput::Settle(continuation)) => continuation(self.accepted()),
                None => return false,
            }
            true
        }
        pub(crate) fn advance_all(&self) {
            while self.advance_one() {}
        }
        pub(crate) fn fail_owner(&self) {
            {
                let mut queue = self.queue.lock().unwrap();
                if queue.failed {
                    return;
                }
                queue.failed = true;
            }
            if self.accepted().is_none() {
                self.owner.adjudicate(TaskConclusionProposal::Failed {
                    cause: ExecutionFailure::from_domain(DeferredOwnerFailure),
                    phase: TaskFailurePhase::Execution,
                });
            }
            let inputs = {
                let mut queue = self.queue.lock().unwrap();
                queue.failure_published = true;
                let inputs = std::mem::take(&mut queue.inputs);
                queue.dropped += inputs
                    .iter()
                    .filter(|input| matches!(input, DeferredInput::Proposal(_)))
                    .count();
                inputs
            };
            for input in inputs {
                match input {
                    DeferredInput::Proposal(proposal) => drop(proposal),
                    DeferredInput::Settle(continuation) => continuation(self.accepted()),
                }
            }
        }
    }
    #[cfg(test)]
    impl TaskConclusionPort<ExecutionFailure> for DeferredOwner {
        fn identity(&self) -> TaskIdentity {
            self.owner.identity()
        }
        fn accepted(&self) -> Option<Snapshot> {
            self.owner.accepted()
        }
        fn submit(&self, proposal: TaskConclusionProposal<ExecutionFailure>) {
            let prepared = Box::new(Owner::prepare(proposal));
            let rejected = {
                let mut queue = self.queue.lock().unwrap();
                if queue.failed {
                    queue.dropped += 1;
                    Some(prepared)
                } else {
                    queue.inputs.push_back(DeferredInput::Proposal(prepared));
                    None
                }
            };
            drop(rejected);
        }
    }
    #[cfg(test)]
    impl DeferredOwner {
        fn route_admitted(&self, continuation: SettledContinuation<ExecutionFailure>) {
            let immediate = {
                let mut queue = self.queue.lock().unwrap();
                if queue.failure_published {
                    Some(continuation)
                } else {
                    queue.inputs.push_back(DeferredInput::Settle(continuation));
                    None
                }
            };
            if let Some(continuation) = immediate {
                continuation(self.accepted());
            }
        }
    }
    #[cfg(test)]
    impl TaskExecutionPort for DeferredOwner {
        fn execution_health(&self) -> Arc<crate::runtime::execution_health::ExecutionHealth> {
            self.owner.execution_health()
        }
        fn report_cleanup(&self, cause: &ExecutionFailure, phase: TaskFailurePhase) {
            self.owner.report_cleanup(cause, phase);
        }
        fn report_unresolved_exit(&self, fact: FailureFact, phase: TaskFailurePhase) {
            self.owner.report_unresolved_exit(fact, phase);
        }
        fn is_stopped(&self) -> bool {
            self.owner.is_stopped()
        }
        fn stop_observable(&self) -> Arc<Observable> {
            self.owner.stop_observable()
        }
        fn wait_until_stopped(&self, duration: Duration) -> bool {
            self.owner.wait_until_stopped(duration)
        }
    }
    #[cfg(test)]
    #[derive(Debug)]
    struct DeferredOwnerFailure;
    #[cfg(test)]
    impl fmt::Display for DeferredOwnerFailure {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Task conclusion owner failed")
        }
    }
    #[cfg(test)]
    impl Error for DeferredOwnerFailure {}
    #[cfg(test)]
    impl FailureProjection for DeferredOwnerFailure {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Invariant,
                novarocks_failure_contract::FailureReason::new(
                    novarocks_failure_contract::domain::EXECUTION,
                    NonZeroU16::new(65004).unwrap(),
                ),
            )
        }
    }
    #[cfg(test)]
    pub(crate) fn deferred_owner_for(identity: TaskIdentity) -> Arc<DeferredOwner> {
        Arc::new(DeferredOwner {
            owner: owner_for(identity),
            queue: Mutex::new(DeferredQueue::default()),
        })
    }
    #[test]
    fn a_stopped_control_without_its_owner_receipt_reports_a_typed_retirement_observation() {
        let owner = Owner::new();
        let control = owner.view(TaskFailurePhase::Preparation);
        owner.stopped.store(true, Ordering::Release);
        let error = control
            .error()
            .expect("retired owner is not permission for new work");
        let expected = TaskExecutionOwnerRetired::new(owner.identity()).failure_fact();
        assert_eq!(error.failure_fact(), expected);
        assert_eq!(
            error.to_string(),
            "Task execution control owner has retired"
        );
        assert!(
            owner.accepted().is_none(),
            "an observation cannot reconstruct or accept a conclusion"
        );
        assert_eq!(owner.health.failure().unwrap().fact(), expected);
        assert_eq!(
            owner.health.failure().unwrap().origin(),
            crate::runtime::execution_health::ExecutionExitOrigin::Task {
                identity: owner.identity(),
                phase: TaskFailurePhase::Preparation,
            }
        );
        assert_eq!(
            control
                .wait_interruptibly(Duration::ZERO)
                .unwrap_err()
                .failure_fact(),
            expected
        );
    }
    #[test]
    fn fixture_admission_prepays_original_task_and_finite_observer_work_once() {
        use finite_fixture::FixtureCompletionPhase as Phase;
        let owner = Owner::new();
        assert_eq!(owner.completion_inventory.phases(), [Phase::Reserved; 6]);
        let originals = owner.issue_fixture_completion_work();
        let one = owner.admit_barrier();
        let two = owner.admit_barrier();
        assert_eq!(owner.completion_inventory.phases(), [Phase::Issued; 6]);
        assert!(
            owner.completion_inventory.take_observation().is_none(),
            "clone count cannot create another role"
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        one.register(Box::new(move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(owner.completion_inventory.phases()[4], Phase::Exited);
        assert_eq!(owner.completion_inventory.phases()[5], Phase::Issued);
        drop(two);
        assert_eq!(owner.completion_inventory.phases()[5], Phase::Exited);
        drop(originals);
        assert_eq!(owner.completion_inventory.phases(), [Phase::Exited; 6]);
        assert!(
            owner.completion_inventory.take_observation().is_none(),
            "exited positions are not newly signed credentials"
        );
    }
    // The oracle is the proposal contract: handoff order, monotonic published
    // stop and a barrier after all earlier inputs. No production async owner
    // is required to exercise execution-side callers at this lowest layer.
    #[test]
    fn deferred_owner_adjudicates_in_handoff_order_before_settlement() {
        let owner = DeferredOwner::new();
        let control = owner.view(TaskFailurePhase::Execution);
        let original = cause();
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        control.report_failure(original.clone());
        let observed = Arc::new(Mutex::new(None));
        let seen = observed.clone();
        owner
            .admit_barrier()
            .register(Box::new(move |snapshot| *seen.lock().unwrap() = snapshot));
        assert_eq!(owner.pending_len(), 3);
        assert!(owner.accepted().is_none());
        assert!(owner.advance_one());
        let peer = owner.accepted().unwrap();
        assert_eq!(
            peer.neutral().conclusion(),
            &TaskConclusion::Terminated(TerminationDetail::Aborted(AbortCause::PeerTaskFailed))
        );
        assert_eq!(peer.neutral().generation().get(), 1);
        assert!(owner.advance_one());
        assert!(
            observed.lock().unwrap().is_none(),
            "settlement is ordered after both proposals"
        );
        assert!(owner.advance_one());
        let snapshot = observed.lock().unwrap().take().unwrap();
        assert_eq!(snapshot.neutral().generation().get(), 2);
        assert!(
            snapshot
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        assert!(!owner.advance_one());
    }
    #[test]
    fn deferred_stop_follows_the_published_snapshot_and_never_resets() {
        let owner = DeferredOwner::new();
        let notifications = Arc::new(AtomicUsize::new(0));
        let seen = notifications.clone();
        let weak = Arc::downgrade(&owner);
        owner.stop_observable().add_observer(Arc::new(move || {
            let owner = weak.upgrade().unwrap();
            assert!(owner.is_stopped());
            assert!(
                owner.accepted().is_some(),
                "snapshot precedes stop and notification"
            );
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        let control = owner.view(TaskFailurePhase::Execution);
        control.report_failure(cause());
        assert!(!control.is_stopped());
        assert!(!control.wait_until_stopped(Duration::ZERO));
        assert!(control.accepted().is_none());
        owner.advance_all();
        let accepted = owner.accepted().unwrap();
        assert!(control.wait_until_stopped(Duration::ZERO));
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        owner.advance_all();
        assert!(control.is_stopped());
        assert!(Arc::ptr_eq(&accepted, &owner.accepted().unwrap()));
        assert_eq!(notifications.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn settlement_runs_exactly_once_including_after_terminal() {
        let owner = DeferredOwner::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        owner.admit_barrier().register(Box::new(move |snapshot| {
            assert!(
                snapshot.is_none(),
                "no proposal is a legitimate success-side barrier"
            );
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        owner.advance_all();
        owner.advance_all();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        owner.submit(TaskConclusionProposal::Succeeded);
        owner.advance_all();
        let seen = calls.clone();
        owner.admit_barrier().register(Box::new(move |snapshot| {
            assert_eq!(
                snapshot.unwrap().neutral().conclusion(),
                &TaskConclusion::Succeeded
            );
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        owner.advance_all();
        owner.advance_all();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn owner_failure_publishes_before_stop_and_settles_once() {
        for keep_existing in [false, true] {
            let owner = DeferredOwner::new();
            if keep_existing {
                owner.submit(TaskConclusionProposal::Canceled(
                    CancelReason::UpstreamNoLongerNeeded,
                ));
                owner.advance_all();
            }
            let existing = owner.accepted();
            let weak = Arc::downgrade(&owner);
            owner.stop_observable().add_observer(Arc::new(move || {
                let owner = weak.upgrade().unwrap();
                assert!(owner.is_stopped());
                assert!(owner.accepted().is_some());
            }));
            owner
                .view(TaskFailurePhase::Execution)
                .report_failure(cause());
            let snapshots = Arc::new(Mutex::new(Vec::new()));
            let seen = snapshots.clone();
            owner.admit_barrier().register(Box::new(move |snapshot| {
                seen.lock().unwrap().push(snapshot.unwrap())
            }));
            owner
                .view(TaskFailurePhase::Execution)
                .report_failure(cause());
            owner.fail_owner();
            owner.fail_owner();
            owner.advance_all();
            assert_eq!(owner.dropped_proposals(), 2);
            assert_eq!(owner.pending_len(), 0);
            let accepted = owner.accepted().unwrap();
            if let Some(existing) = existing {
                assert!(Arc::ptr_eq(&existing, &accepted));
            } else {
                let source = accepted.originating_failure().unwrap();
                assert_eq!(source.failure_fact().class(), FailureClass::Invariant);
                assert_eq!(source.to_string(), "Task conclusion owner failed");
            }
            assert!(owner.is_stopped());
            assert_eq!(snapshots.lock().unwrap().len(), 1);
            assert!(Arc::ptr_eq(&snapshots.lock().unwrap()[0], &accepted));
            owner
                .view(TaskFailurePhase::Execution)
                .report_failure(cause());
            assert_eq!(owner.dropped_proposals(), 3);
            let seen = snapshots.clone();
            owner.admit_barrier().register(Box::new(move |snapshot| {
                seen.lock().unwrap().push(snapshot.unwrap())
            }));
            assert_eq!(snapshots.lock().unwrap().len(), 2);
            assert!(Arc::ptr_eq(&snapshots.lock().unwrap()[1], &accepted));
            owner.advance_all();
            assert_eq!(snapshots.lock().unwrap().len(), 2);
        }
    }
    #[test]
    fn owner_failure_queues_concurrent_settlement_until_publication_finishes() {
        let owner = DeferredOwner::new();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        *owner.owner.before_notify.lock().unwrap() = Some((entered.clone(), release.clone()));
        let pump = owner.clone();
        let failed = std::thread::spawn(move || pump.fail_owner());
        entered.wait();
        let settled = Arc::new(Mutex::new(None));
        let seen = settled.clone();
        owner.admit_barrier().register(Box::new(move |snapshot| {
            *seen.lock().unwrap() = Some(snapshot)
        }));
        let called_before_publication_finished = settled.lock().unwrap().is_some();
        release.wait();
        failed.join().unwrap();
        assert!(!called_before_publication_finished);
        let snapshot = settled.lock().unwrap().take().unwrap().unwrap();
        assert_eq!(
            snapshot
                .originating_failure()
                .unwrap()
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
        assert!(Arc::ptr_eq(&snapshot, &owner.accepted().unwrap()));
        assert!(!owner.advance_one());
    }
    #[test]
    fn cleanup_and_unknown_exit_leave_the_exact_accepted_task_receipt_unchanged() {
        use novarocks_execution_contract::AbortCause;
        let owner = Owner::new();
        let control = owner.view(TaskFailurePhase::Execution);
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let accepted = owner.accepted().unwrap();
        let original = accepted;
        let cleanup = ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted);
        control.report_cleanup(&cleanup);
        control.report_unresolved_exit(&cleanup);
        let current = control.accepted().unwrap();
        assert!(Arc::ptr_eq(&current, &original));
        assert!(current.originating_failure().is_none());
        assert_eq!(
            owner.cleanup.lock().unwrap()[0],
            Some(cleanup.failure_fact())
        );
        let fault = owner.health.failure().unwrap();
        assert_eq!(fault.fact(), cleanup.failure_fact());
        assert_eq!(
            fault.origin(),
            crate::runtime::execution_health::ExecutionExitOrigin::Task {
                identity: owner.identity,
                phase: TaskFailurePhase::Execution,
            }
        );
        // An independently authored original failure still has its own legal
        // acceptance path; cleanup did not consume or refine that opportunity.
        let before_refined = control.accepted();
        control.report_failure(cleanup);
        let refined = control.accepted().unwrap();
        assert_eq!(
            refined.neutral().generation().get(),
            before_refined.unwrap().neutral().generation().get() + 1
        );
    }

    pub(crate) fn owner_for(identity: TaskIdentity) -> Arc<Owner> {
        let mut owner = Owner::new();
        Arc::get_mut(&mut owner).unwrap().identity = identity;
        owner
    }
    pub(crate) fn control_for(identity: TaskIdentity) -> Arc<TaskExecutionControl> {
        Arc::new(owner_for(identity).view(TaskFailurePhase::Execution))
    }
    #[test]
    fn fragment_group_has_only_physical_completion_and_the_same_task_acceptance() {
        use crate::exec::pipeline::{
            global_driver_executor::DriverGroupCompletion, task_context::TaskContext,
        };
        use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
        let owner = Owner::new();
        let state = Arc::new(RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Arc::new(RuntimeErrorState::for_task(Arc::new(
                owner.view(TaskFailurePhase::Execution),
            ))),
        ));
        let fragment = TaskContext::new(None, state.clone(), None, None, None, None);
        let completion = DriverGroupCompletion::new(2, state.error_state());
        let reentered = Arc::new(AtomicUsize::new(0));
        let callback_count = reentered.clone();
        let callback_completion = Arc::downgrade(&completion);
        owner.observable.add_observer(Arc::new(move || {
            let completion = callback_completion.upgrade().unwrap();
            assert!(completion.conclusion().unwrap().is_err());
            assert!(completion.stopped_fact().is_none());
            callback_count.fetch_add(1, Ordering::Release);
        }));
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let observation = state.error_state().error().unwrap();
        let before_observation = owner.accepted().unwrap();
        fragment.set_final_status(observation);
        assert!(Arc::ptr_eq(&before_observation, &owner.accepted().unwrap()));
        let original = cause();
        fragment.set_final_status(original.clone());
        assert!(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        assert!(completion.should_abort());
        assert_eq!(
            reentered.load(Ordering::Acquire),
            2,
            "both Task acceptance and refinement may reenter physical completion"
        );
        assert!(
            completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&original)
        );
        assert!(completion.stopped_fact().is_none());
        assert!(!completion.driver_finished());
        assert!(completion.stopped_fact().is_none());
        assert!(completion.driver_finished());
        assert!(completion.wait().unwrap_err().same_occurrence(&original));
        let owner = Owner::new();
        let completion = DriverGroupCompletion::new(
            0,
            Arc::new(RuntimeErrorState::for_task(Arc::new(
                owner.view(TaskFailurePhase::Execution),
            ))),
        );
        assert_eq!(completion.wait(), Ok(()));
        assert!(
            owner.accepted().is_none(),
            "zero drivers do not prove root/finalization work finished"
        );
        let original = cause();
        completion.fail(original.clone());
        assert!(
            completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&original),
            "physical driver exit cannot suppress another work owner's failure"
        );
        assert!(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
    }

    #[test]
    fn task_runtime_requires_exact_actual_control_and_local_execution_stays_taskless() {
        use crate::runtime::task::runtime_state::{RuntimeStateInputs, build_runtime_state};
        let owner = Owner::new();
        let other = Owner::new();
        let inputs = |task_identity, task_control| RuntimeStateInputs {
            query_options: None,
            query_id: None,
            task_key: None,
            backend_num: None,
            mem_tracker: None,
            runtime_filter_session: None,
            execution_runtime: None,
            query_memory: None,
            task_identity,
            task_control,
            #[cfg(novarocks_dev_controls)]
            exchange_test_observer: None,
        };
        let absent = build_runtime_state(inputs(Some(owner.identity), None)).unwrap_err();
        assert_eq!(
            absent.failure_fact(),
            FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::MissingTaskExecutionControl.reason()
            )
        );
        for identity in [None, Some(other.identity)] {
            let mismatch = build_runtime_state(inputs(
                identity,
                Some(Arc::new(owner.view(TaskFailurePhase::Execution))),
            ))
            .unwrap_err();
            assert_eq!(
                mismatch.failure_fact(),
                TaskControlIdentityMismatch.failure_fact()
            );
        }
        let runtime = build_runtime_state(inputs(
            Some(owner.identity),
            Some(Arc::new(owner.view(TaskFailurePhase::Execution))),
        ))
        .unwrap();
        let original = cause();
        runtime.error_state().set_error(original.clone());
        assert!(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        let local = build_runtime_state(inputs(None, None)).unwrap();
        assert!(!local.error_state().is_stopped());
        local.error_state().set_error(cause());
        assert!(
            runtime
                .error_state()
                .error()
                .unwrap()
                .same_occurrence(&original)
        );
    }

    #[test]
    fn runtime_facade_observes_task_acceptance_and_legal_original_refinement() {
        use crate::runtime::runtime_state::RuntimeErrorState;
        let owner = Owner::new();
        let runtime =
            RuntimeErrorState::for_task(Arc::new(owner.view(TaskFailurePhase::Execution)));
        assert!(Arc::ptr_eq(&runtime.stop_observable(), &owner.observable));
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        assert!(runtime.is_stopped());
        assert!(runtime.wait_until_stopped(Duration::ZERO));
        let observed = runtime.error().unwrap();
        assert_eq!(
            observed.failure_fact().evidence(),
            novarocks_failure_contract::FailureEvidence::StopObservation
        );
        let before_observation = owner.accepted().unwrap();
        runtime.set_error(observed);
        assert!(Arc::ptr_eq(&before_observation, &owner.accepted().unwrap()));
        let original = cause();
        runtime.set_error(original.clone());
        assert!(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        assert!(runtime.error().unwrap().same_occurrence(&original));
        assert!(
            runtime
                .wait_interruptibly(Duration::ZERO)
                .unwrap_err()
                .same_occurrence(&original)
        );
        assert!(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        let canceled = Owner::new();
        let runtime =
            RuntimeErrorState::for_task(Arc::new(canceled.view(TaskFailurePhase::Execution)));
        canceled.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let receipt = canceled.accepted().unwrap();
        runtime.set_error(cause());
        assert!(Arc::ptr_eq(&receipt, &canceled.accepted().unwrap()));
        assert_eq!(
            runtime.error().unwrap().failure_fact().evidence(),
            novarocks_failure_contract::FailureEvidence::StopObservation
        );
    }

    #[cfg(test)]
    fn cause() -> ExecutionFailure {
        novarocks_functions::KernelFailure::ResourceExhausted.into()
    }
    #[test]
    fn an_acceptance_before_notification_is_visible_and_a_later_proposal_keeps_its_source() {
        let owner = Owner::new();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        *owner.before_notify.lock().unwrap() = Some((entered.clone(), release.clone()));
        let publisher = Arc::new(owner.view(TaskFailurePhase::Execution));
        let reader = owner.view(TaskFailurePhase::Execution);
        let original = cause();
        let source = original.clone();
        let job = std::thread::spawn(move || {
            publisher.report_failure(source);
            publisher.accepted().unwrap()
        });
        entered.wait();
        assert!(reader.is_stopped());
        assert!(reader.wait_until_stopped(Duration::ZERO));
        assert!(reader.error().unwrap().same_occurrence(&original));
        let before_later = reader.accepted();
        reader.report_failure(cause());
        let later = reader.accepted().unwrap();
        assert!(Arc::ptr_eq(&before_later.unwrap(), &later));
        assert!(
            later
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        release.wait();
        let first = job.join().unwrap();
        assert!(Arc::ptr_eq(&first, &later));
    }
    #[test]
    fn accepted_control_has_no_fabricated_origin_and_refinement_reads_the_actual_failure() {
        let owner = Owner::new();
        let view = owner.view(TaskFailurePhase::Execution);
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let first = owner.accepted().unwrap();
        let observation = view.error().unwrap();
        assert_eq!(observation.failure_fact().class(), FailureClass::Control);
        assert!(first.originating_failure().is_none());
        let original = cause();
        let before_refined = view.accepted();
        view.report_failure(original.clone());
        let refined = view.accepted().unwrap();
        assert_eq!(
            refined.neutral().generation().get(),
            before_refined.unwrap().neutral().generation().get() + 1
        );
        assert_eq!(refined.neutral().generation().get(), 2);
        assert!(view.error().unwrap().same_occurrence(&original));
        let owner = Owner::new();
        let view = owner.view(TaskFailurePhase::Execution);
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let canceled = owner.accepted().unwrap();
        let before_later = view.accepted();
        view.report_failure(cause());
        let later = view.accepted().unwrap();
        assert!(Arc::ptr_eq(&before_later.unwrap(), &later));
        assert!(Arc::ptr_eq(&canceled, &later));
        assert!(view.accepted().unwrap().originating_failure().is_none());
    }
    #[test]
    fn scoped_phase_keeps_one_authority_and_success_has_no_late_failure_origin() {
        let owner = Owner::new();
        let prep = owner.view(TaskFailurePhase::Preparation);
        let execution = prep.in_phase(TaskFailurePhase::Execution);
        let original = cause();
        prep.report_failure(original.clone());
        let accepted = prep.accepted().unwrap();
        execution.report_failure(cause());
        let later = execution.accepted().unwrap();
        assert!(Arc::ptr_eq(&accepted, &later));
        let TaskConclusion::Terminated(TerminationDetail::Failed(failure)) =
            accepted.neutral().conclusion()
        else {
            panic!("accepted original failure");
        };
        assert_eq!(failure.phase(), TaskFailurePhase::Preparation);
        assert!(execution.error().unwrap().same_occurrence(&original));
        let owner = Owner::new();
        let view = owner.view(TaskFailurePhase::Execution);
        owner.submit(TaskConclusionProposal::Succeeded);
        let successful = owner.accepted().unwrap();
        assert!(
            view.is_stopped(),
            "any accepted Task conclusion closes ordinary production"
        );
        view.report_failure(cause());
        let later = view.accepted().unwrap();
        assert!(Arc::ptr_eq(&successful, &later));
        assert!(view.error().is_none());
    }
    #[test]
    fn observed_stop_cannot_refine_peer_placeholder_but_a_genuine_control_failure_can() {
        let owner = Owner::new();
        let view = owner.view(TaskFailurePhase::Execution);
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let first = owner.accepted().unwrap();
        let observed = view.error().unwrap();
        assert_eq!(
            observed.failure_fact().evidence(),
            novarocks_failure_contract::FailureEvidence::StopObservation
        );
        for observation in [
            observed,
            novarocks_functions::KernelFailure::Cancelled.into(),
            novarocks_functions::KernelFailure::DeadlineExceeded.into(),
        ] {
            let before_later = view.accepted();
            view.report_failure(observation);
            let later = view.accepted().unwrap();
            assert!(Arc::ptr_eq(&before_later.unwrap(), &later));
            assert!(Arc::ptr_eq(&first, &later));
            assert!(later.originating_failure().is_none());
        }
        #[derive(Debug)]
        struct LocalTimeout;
        impl fmt::Display for LocalTimeout {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("local timeout")
            }
        }
        impl Error for LocalTimeout {}
        impl FailureProjection for LocalTimeout {
            fn failure_fact(&self) -> FailureFact {
                FailureFact::new(
                    FailureClass::Control,
                    novarocks_failure_contract::FailureReason::new(
                        novarocks_failure_contract::domain::EXECUTION,
                        NonZeroU16::new(65001).unwrap(),
                    ),
                )
            }
        }
        let original = ExecutionFailure::from_domain(LocalTimeout);
        let before_refined = view.accepted();
        view.report_failure(original.clone());
        let refined = view.accepted().unwrap();
        assert_eq!(
            refined.neutral().generation().get(),
            before_refined.unwrap().neutral().generation().get() + 1
        );
        assert!(view.error().unwrap().same_occurrence(&original));
    }
    #[test]
    fn unowned_stop_is_an_owner_failure_with_the_actual_observation_as_source() {
        let owner = Owner::new();
        let view = owner.view(TaskFailurePhase::Execution);
        let observation: ExecutionFailure = novarocks_functions::KernelFailure::Cancelled.into();
        let before_accepted = view.accepted();
        view.report_failure(observation.clone());
        let accepted = view.accepted().unwrap();
        assert!(before_accepted.is_none());
        assert_eq!(accepted.neutral().generation().get(), 1);
        let primary = accepted.originating_failure().unwrap();
        assert_eq!(primary.failure_fact().class(), FailureClass::Unexpected);
        let mut source = primary.source();
        let mut seen = false;
        while let Some(current) = source {
            if let Some(actual) = current.downcast_ref::<ExecutionFailure>() {
                seen |= actual.same_occurrence(&observation);
            }
            source = current.source();
        }
        assert!(seen);
    }

    #[test]
    fn another_task_port_is_rejected_before_work_is_bound() {
        let owner = Owner::new();
        let other = Owner::new();
        let error = TaskExecutionControl::new(other.identity, owner, TaskFailurePhase::Execution)
            .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    }
}
