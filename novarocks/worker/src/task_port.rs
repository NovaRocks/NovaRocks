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

//! Runtime work sees only published core facts and finite proposal custody.
//! This port has no conclusion latch, host action, or settlement issuer.

use std::{
    num::NonZeroU64,
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use novarocks_control_runtime::{publication::PublishedSnapshot, transaction::ScopeHandle};
use novarocks_execution::runtime::{
    execution_health::{ExecutionExitOrigin, ExecutionHealth},
    fragment::ExecutionFailure,
    observable::Observable,
};
use novarocks_execution_contract::{
    AcceptedConclusion, AcceptedTaskConclusion, ConclusionSourceError, RetainedTaskSource,
    SafeDetail, TaskConclusion, TaskConclusionProposal, TaskFailurePhase, TaskIdentity, TaskStatus,
    failure::FailureFact,
};
use novarocks_worker_protocol::{
    convergence::TaskConvergenceSnapshot,
    proposal_summary::{Diagnostic, Diagnostics, SourceId},
};

use crate::{
    task_diagnostics::TaskSecondaryDiagnosticsSnapshot, task_proposal_ingress::TaskProposalIngress,
};

/// One prepaid/coalesced ready position. Implementations only mark this exact
/// Task pending; they never wait for its actor, run an observer, or execute I/O.
/// An admitted role keeps waking its live host custodian after Core failure;
/// that custodian routes work to failure cleanup instead of the dead Core.
/// Ignoring late registration/revocation/exit would strand original duties.
pub(crate) trait TaskPendingNotice: Send + Sync {
    fn pending(&self);
}

struct ProposalReadyAfterHandoff<'a>(&'a dyn TaskPendingNotice);
impl Drop for ProposalReadyAfterHandoff<'_> {
    fn drop(&mut self) {
        // The ingress's local guards have already unlocked, including on a
        // failed bind. Its original custodian must observe Pending/Unknown.
        self.0.pending();
    }
}

/// Read-only metadata for the exact Task's published producer-stop intent.
/// Clones cannot clear it, notify observers, or claim any physical exit.
#[derive(Clone)]
pub(crate) struct TaskStopSignal {
    identity: TaskIdentity,
    stopped: Arc<AtomicBool>,
}
impl TaskStopSignal {
    /// Initial read-only metadata. This carries no Task execution authority,
    /// source position, admission credit or lifecycle registration right.
    pub(crate) fn initial(identity: TaskIdentity) -> Self {
        Self {
            identity,
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }
    pub(crate) fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub(crate) fn is_set(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

/// An immutable projection, authored only by ordered core publication effects.
/// Rich sources are immutable shared references, never inputs to adjudication.
#[derive(Clone)]
pub(crate) struct TaskProjection {
    status: TaskStatus,
    accepted: Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>>,
    // This is the same accepted receipt's neutral identity and watermark, not
    // another adjudicator. It outlives disposal of its original rich source.
    neutral_accepted: Option<AcceptedConclusion>,
    convergence: TaskConvergenceSnapshot,
    diagnostics: TaskSecondaryDiagnosticsSnapshot,
}
/// Immutable metadata only. Copying this observation never borrows a rich
/// conclusion/source or transfers responsibility to its reading thread.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TaskNeutralObservation {
    pub status: TaskStatus,
    pub convergence: TaskConvergenceSnapshot,
    pub diagnostics: TaskSecondaryDiagnosticsSnapshot,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProjectionError {
    WrongTask,
    StatusRegressed,
    SameVersionDifferentStatus,
    ConclusionGeneration,
    MissingSource(SourceId),
    SourcePosition(crate::control_runtime::source_positions::SourcePositionError),
    InvalidSource(ConclusionSourceError),
    StatusVersionExhausted,
    ConvergenceRegressed,
    SameVersionDifferentConvergence,
    DiagnosticsCapacity,
    DiagnosticsRegressed,
    DiagnosticsPrefixChanged,
    SourceStillBorrowed,
}

/// A host publication attempt keeps the same original source on refusal.
/// The executor disposes it only in its original supervised teardown lane.
pub(crate) struct TaskConclusionRefusal {
    pub error: ProjectionError,
    pub source: Option<RetainedTaskSource<ExecutionFailure>>,
}
impl std::fmt::Debug for TaskConclusionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskConclusionRefusal")
            .field("error", &self.error)
            .field("retains_source", &self.source.is_some())
            .finish()
    }
}
impl TaskConclusionRefusal {
    fn metadata(error: ProjectionError) -> Self {
        Self {
            error,
            source: None,
        }
    }
}
impl TaskProjection {
    fn bootstrap(identity: TaskIdentity) -> Self {
        Self {
            status: TaskStatus::created(identity),
            accepted: None,
            neutral_accepted: None,
            convergence: novarocks_worker_protocol::convergence::TaskConvergence::new().snapshot(),
            diagnostics: TaskSecondaryDiagnosticsSnapshot::from_protocol(&Diagnostics::default())
                .expect("the initial diagnostic history is empty"),
        }
    }
    pub(crate) fn status(&self) -> &TaskStatus {
        &self.status
    }
    pub(crate) fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>> {
        self.accepted.clone()
    }
    pub(crate) fn has_rich_source(&self) -> bool {
        self.accepted
            .as_ref()
            .is_some_and(|accepted| accepted.originating_failure().is_some())
    }
    pub(crate) fn neutral_accepted(&self) -> Option<&AcceptedConclusion> {
        self.neutral_accepted.as_ref()
    }
    fn with_detached_source(&self) -> Self {
        Self {
            accepted: None,
            ..self.clone()
        }
    }
    pub(crate) fn convergence(&self) -> TaskConvergenceSnapshot {
        self.convergence
    }
    pub(crate) fn secondary_diagnostics(&self) -> &TaskSecondaryDiagnosticsSnapshot {
        &self.diagnostics
    }
    pub(crate) fn neutral_observation(&self) -> TaskNeutralObservation {
        TaskNeutralObservation {
            status: self.status.clone(),
            convergence: self.convergence,
            diagnostics: self.diagnostics.clone(),
        }
    }
    /// Copy a real Core-authored observation without replaying convergence
    /// transitions or inferring physical exit from the Task status.
    pub(crate) fn with_convergence(
        &self,
        next: TaskConvergenceSnapshot,
    ) -> Result<Self, ProjectionError> {
        if next.version() < self.convergence.version() {
            return Err(ProjectionError::ConvergenceRegressed);
        }
        if next.version() == self.convergence.version() && next != self.convergence {
            return Err(ProjectionError::SameVersionDifferentConvergence);
        }
        if (self.convergence.conclusion_stable() && !next.conclusion_stable())
            || (self.convergence.actual_stopped() && !next.actual_stopped())
            || (self.convergence.output_released() && !next.output_released())
            || (self.convergence.resources_converged() && !next.resources_converged())
            || (self.convergence.retired() && !next.retired())
        {
            return Err(ProjectionError::ConvergenceRegressed);
        }
        Ok(Self {
            convergence: next,
            ..self.clone()
        })
    }
    /// Diagnostics have no conclusion-generation clock: cleanup can extend
    /// their history while the accepted conclusion remains unchanged. The
    /// actual retained prefix and saturated count are their ordering facts.
    pub(crate) fn with_diagnostics(&self, next: &Diagnostics) -> Result<Self, ProjectionError> {
        let next = TaskSecondaryDiagnosticsSnapshot::from_protocol(next)
            .ok_or(ProjectionError::DiagnosticsCapacity)?;
        if next.entries().len() < self.diagnostics.entries().len()
            || next.dropped() < self.diagnostics.dropped()
        {
            return Err(ProjectionError::DiagnosticsRegressed);
        }
        if self
            .diagnostics
            .entries()
            .zip(next.entries())
            .any(|(previous, next)| previous != next)
        {
            return Err(ProjectionError::DiagnosticsPrefixChanged);
        }
        Ok(Self {
            diagnostics: next,
            ..self.clone()
        })
    }
    /// This verifies publication ordering, not lifecycle/conclusion policy.
    /// The host must serialize this Task's publications across actor batches.
    pub(crate) fn with_status(&self, next: TaskStatus) -> Result<Self, ProjectionError> {
        if next.identity() != self.status.identity() {
            return Err(ProjectionError::WrongTask);
        }
        if next.version().get() < self.status.version().get() {
            return Err(ProjectionError::StatusRegressed);
        }
        if next.version() == self.status.version() && next != self.status {
            return Err(ProjectionError::SameVersionDifferentStatus);
        }
        Ok(Self {
            status: next,
            ..self.clone()
        })
    }
    pub(crate) fn with_conclusion(
        &self,
        next: Arc<AcceptedTaskConclusion<ExecutionFailure>>,
    ) -> Result<Self, ProjectionError> {
        if next.neutral().identity() != self.status.identity() {
            return Err(ProjectionError::WrongTask);
        }
        let previous = self
            .neutral_accepted
            .as_ref()
            .map_or(0, |accepted| accepted.generation().get());
        // One exact compressed summary may accept a peer placeholder and
        // refine it before its single final publication, advancing by two.
        if next.neutral().generation().get() <= previous {
            return Err(ProjectionError::ConclusionGeneration);
        }
        Ok(Self {
            neutral_accepted: Some(next.neutral().clone()),
            accepted: Some(next),
            ..self.clone()
        })
    }
}

pub(crate) struct TaskRuntimePort {
    identity: TaskIdentity,
    proposals: Arc<TaskProposalIngress>,
    published: PublishedSnapshot<TaskProjection>,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
    stop: Arc<Observable>,
    // Presence hint installed only by the post-publication host effect. It
    // selects no conclusion and keeps rich Arc destruction out of wait locks.
    stop_published: Arc<AtomicBool>,
    wait: Mutex<()>,
    changed: Condvar,
    execution_retired: Arc<AtomicBool>,
}

/// The actual Task host/scope retains the strong port. Executing work may
/// retain this detached view in an error source without forming a cycle back
/// from the published accepted source to its own owner.
pub(crate) struct TaskExecutionView {
    owner: Weak<TaskRuntimePort>,
    identity: TaskIdentity,
    health: Arc<ExecutionHealth>,
    stop: Arc<Observable>,
    retired: Arc<AtomicBool>,
}
impl TaskExecutionView {
    fn owner(&self) -> Option<Arc<TaskRuntimePort>> {
        if self.retired.load(Ordering::Acquire) {
            return None;
        }
        let owner = self.owner.upgrade()?;
        // Retirement can close admission while the temporary metadata loan
        // upgrades. It grants no new Work, callback or capacity authority.
        if self.retired.load(Ordering::Acquire) {
            None
        } else {
            Some(owner)
        }
    }
    fn observe_retired(&self, phase: TaskFailurePhase) {
        use novarocks_execution_contract::failure::FailureProjection;
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.identity,
                phase,
            },
            novarocks_execution::runtime::task_control::TaskExecutionOwnerRetired::new(
                self.identity,
            )
            .failure_fact(),
        );
    }
}
#[derive(Debug)]
struct ProtocolOwnerFailed;
impl std::fmt::Display for ProtocolOwnerFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Task protocol owner failed")
    }
}
impl std::error::Error for ProtocolOwnerFailed {}
impl novarocks_execution_contract::failure::FailureProjection for ProtocolOwnerFailed {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            novarocks_execution_contract::failure::FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::ProtocolOwnerFailed.reason(),
        )
    }
}
impl TaskRuntimePort {
    /// The admission owner retains the role and its storage reservation. This
    /// constructor grants no authority to admit another role or callback.
    pub(crate) fn from_admitted(
        identity: TaskIdentity,
        scope: &ScopeHandle,
        proposals: Arc<TaskProposalIngress>,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> Self {
        let stop = TaskStopSignal::initial(identity);
        Self {
            identity,
            proposals,
            pending,
            health,
            published: PublishedSnapshot::new(scope, Arc::new(TaskProjection::bootstrap(identity))),
            stop: Arc::new(Observable::new()),
            stop_published: stop.stopped,
            wait: Mutex::new(()),
            changed: Condvar::new(),
            execution_retired: Arc::new(AtomicBool::new(false)),
        }
    }
    pub(crate) fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub(crate) fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>> {
        self.published.read().accepted()
    }
    pub(crate) fn current(&self) -> TaskStatus {
        self.published.borrow().status().clone()
    }
    pub(crate) fn observe_neutral(&self) -> TaskNeutralObservation {
        self.published.borrow().neutral_observation()
    }
    pub(crate) fn publication(&self) -> &PublishedSnapshot<TaskProjection> {
        &self.published
    }
    pub(crate) fn ingress(&self) -> &Arc<TaskProposalIngress> {
        &self.proposals
    }
    pub(crate) fn submit(&self, proposal: TaskConclusionProposal<ExecutionFailure>) {
        let _ready = ProposalReadyAfterHandoff(self.pending.as_ref());
        let handoff = self.proposals.submit(proposal);
        // Any rejected source destructor runs on the submitting host after
        // both handoff and wake metadata locks have been released.
        drop(handoff.discarded);
    }
    pub(crate) fn report_cleanup(&self, cause: &ExecutionFailure, phase: TaskFailurePhase) {
        let mut prepared = crate::PreparedCleanupDiagnostic::from_failure(cause, phase);
        let record = prepared.record_mut().take().expect("prepared cleanup fact");
        let transferred = self.proposals.cleanup(Diagnostic {
            phase,
            fact: record.fact(),
            detail: SafeDetail::truncating(record.detail()),
            cleanup: true,
        });
        if transferred {
            self.pending.pending();
        }
    }
    pub(crate) fn report_unresolved(&self, fact: FailureFact, phase: TaskFailurePhase) {
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.identity,
                phase,
            },
            fact,
        );
    }
    pub(crate) fn health(&self) -> Arc<ExecutionHealth> {
        self.health.clone()
    }
    /// This is the only execution trait projection. The concrete owning port
    /// deliberately does not implement either public execution trait.
    pub(crate) fn execution_port(
        self: &Arc<Self>,
    ) -> Arc<dyn novarocks_execution::runtime::task_control::TaskExecutionPort> {
        self.execution_view()
    }
    pub(crate) fn execution_view(self: &Arc<Self>) -> Arc<TaskExecutionView> {
        Arc::new(TaskExecutionView {
            owner: Arc::downgrade(self),
            identity: self.identity,
            health: self.health.clone(),
            stop: self.stop.clone(),
            retired: self.execution_retired.clone(),
        })
    }
    /// The real host closes new execution/control borrowing after its actual
    /// borrowers finish, before disposing rich sources if necessary. It keeps
    /// owning this port and its original inventory throughout cleanup. This
    /// gate grants no conclusion, physical-exit or capacity-release evidence.
    /// In particular source cleanup need not precede this close operation.
    pub(crate) fn retire_execution_port(&self) {
        let first = {
            let _wait = self.wait.lock().expect("Task stop wait metadata");
            let first = !self.execution_retired.swap(true, Ordering::AcqRel);
            self.changed.notify_all();
            first
        };
        if first {
            self.stop.notify_observers();
        }
    }

    /// Prepare source detachment only after the execution borrowing gate has
    /// closed. That gate is necessary, not a proof that every existing borrower
    /// or registered settlement has exited: the real service verifies those
    /// independent obligations before running its original host cleanup Work.
    ///
    /// This changes no publication, stop/exit fact or capacity. The host must
    /// commit this candidate through its actual runtime failure fence, then
    /// dispose the returned old projection on its cleanup executor outside
    /// every runtime, publication and ingress lock. Neutral accepted identity,
    /// generation, status, convergence and diagnostics stay in the same cell.
    pub(crate) fn prepare_source_retirement(&self) -> Result<TaskProjection, ProjectionError> {
        if !self.execution_retired.load(Ordering::Acquire) {
            return Err(ProjectionError::SourceStillBorrowed);
        }
        Ok(self.published.read().with_detached_source())
    }

    /// Fixed failure projection from the actual published inventory. This
    /// cannot revoke an existing result or inspect the failed protocol Core.
    /// The host still commits through its original failure publication session
    /// before publishing producer stop or running registered continuations.
    pub(crate) fn prepare_owner_failure(&self) -> Result<TaskProjection, ProjectionError> {
        let current = self.published.read();
        if current.neutral_accepted.is_some() {
            return Ok((*current).clone());
        }
        let version = current
            .status
            .version()
            .next()
            .ok_or(ProjectionError::StatusVersionExhausted)?;
        let cause = ExecutionFailure::from_domain(ProtocolOwnerFailed);
        let (proposal, original) =
            crate::task_conclusion::PreparedTaskConclusion::new(TaskConclusionProposal::Failed {
                cause,
                phase: if current.status.installed() {
                    TaskFailurePhase::Execution
                } else {
                    TaskFailurePhase::Preparation
                },
            })
            .into_protocol();
        let accepted = Arc::new(
            AcceptedTaskConclusion::new(
                AcceptedConclusion::new(
                    self.identity,
                    NonZeroU64::new(1).unwrap(),
                    proposal.conclusion,
                ),
                original,
            )
            .expect("fixed owner failure has its original cause"),
        );
        let TaskConclusion::Terminated(termination) = accepted.neutral().conclusion() else {
            unreachable!("fixed owner failure is a termination");
        };
        let mut status = TaskStatus::try_new_with_installed(
            self.identity,
            version,
            novarocks_execution_contract::TaskState::Failing,
            Some(termination.clone()),
            current.status.output(),
            current.status.installed(),
        )
        .expect("fixed owner failure preserves the installed and output facts")
        .with_resources(current.status.resources());
        if let Some(filters) = current.status.dynamic_filters() {
            status = status.with_dynamic_filters(filters);
        }
        if let Some(writer) = current.status.writer() {
            status = status.with_writer(writer);
        }
        current.with_conclusion(accepted)?.with_status(status)
    }

    /// Only a host publication job takes a rich source. It must commit the
    /// resulting snapshot through the runtime generation fence, then dispose
    /// its old/rejected Arc outside every actor, inbox and ledger lock.
    pub(crate) fn prepare_conclusion(
        &self,
        generation: NonZeroU64,
        conclusion: TaskConclusion,
        source: Option<SourceId>,
    ) -> Result<Arc<AcceptedTaskConclusion<ExecutionFailure>>, TaskConclusionRefusal> {
        let neutral = AcceptedConclusion::new(self.identity, generation, conclusion);
        let Some(id) = source else {
            return AcceptedTaskConclusion::new(neutral, None)
                .map(Arc::new)
                .map_err(|error| {
                    TaskConclusionRefusal::metadata(ProjectionError::InvalidSource(error))
                });
        };
        let retained = self.proposals.take_source(id).map_err(|error| {
            use crate::control_runtime::source_positions::SourcePositionError;
            TaskConclusionRefusal::metadata(match error {
                SourcePositionError::UnknownSource => ProjectionError::MissingSource(id),
                other => ProjectionError::SourcePosition(other),
            })
        })?;
        AcceptedTaskConclusion::from_retained_source(neutral, retained)
            .map(Arc::new)
            .map_err(|(error, source)| TaskConclusionRefusal {
                error: ProjectionError::InvalidSource(error),
                source: Some(source),
            })
    }
    pub(crate) fn is_stopped(&self) -> bool {
        self.stop_published.load(Ordering::Acquire)
    }
    pub(crate) fn stop_signal(&self) -> TaskStopSignal {
        TaskStopSignal {
            identity: self.identity,
            stopped: self.stop_published.clone(),
        }
    }
    pub(crate) fn stop_observable(&self) -> Arc<Observable> {
        self.stop.clone()
    }
    /// This is a host effect after the actual conclusion/status commits. The
    /// poller only hands off its pending role, never invokes these observers.
    pub(crate) fn notify_stopped(&self) {
        assert!(
            self.published.read().neutral_accepted().is_some(),
            "stop notification follows actual conclusion publication"
        );
        {
            let _wait = self.wait.lock().expect("Task stop wait metadata");
            self.stop_published.store(true, Ordering::Release);
            self.changed.notify_all();
        }
        self.stop.notify_observers();
    }
    pub(crate) fn wait_until_stopped(&self, duration: Duration) -> bool {
        let wait = self.wait.lock().expect("Task stop wait metadata");
        let (_wait, _) = self
            .changed
            .wait_timeout_while(wait, duration, |()| {
                !self.is_stopped() && !self.execution_retired.load(Ordering::Acquire)
            })
            .expect("Task stop wait metadata");
        self.is_stopped() || self.execution_retired.load(Ordering::Acquire)
    }
}
impl novarocks_execution_contract::TaskConclusionPort<ExecutionFailure> for TaskExecutionView {
    fn identity(&self) -> TaskIdentity {
        self.identity
    }
    fn submit(&self, proposal: TaskConclusionProposal<ExecutionFailure>) {
        if let Some(owner) = self.owner() {
            owner.submit(proposal);
        } else {
            let phase = match &proposal {
                TaskConclusionProposal::Failed { phase, .. } => *phase,
                _ => TaskFailurePhase::Execution,
            };
            self.observe_retired(phase);
            // The caller's original supervised stack owns this rejected
            // source, including its destructor. No port/ingress lock is held.
            drop(proposal);
        }
    }
    fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>> {
        self.owner().and_then(|owner| owner.accepted())
    }
}
impl novarocks_execution::runtime::task_control::TaskExecutionPort for TaskExecutionView {
    fn execution_health(&self) -> Arc<ExecutionHealth> {
        self.health.clone()
    }
    fn report_cleanup(&self, cause: &ExecutionFailure, phase: TaskFailurePhase) {
        if let Some(owner) = self.owner() {
            owner.report_cleanup(cause, phase);
        } else {
            self.observe_retired(phase);
        }
    }
    fn report_unresolved_exit(&self, fact: FailureFact, phase: TaskFailurePhase) {
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.identity,
                phase,
            },
            fact,
        );
    }
    fn is_stopped(&self) -> bool {
        self.owner().is_none_or(|owner| owner.is_stopped())
    }
    fn stop_observable(&self) -> Arc<Observable> {
        self.stop.clone()
    }
    fn wait_until_stopped(&self, duration: Duration) -> bool {
        self.owner()
            .is_none_or(|owner| owner.wait_until_stopped(duration))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_control_runtime::{
        actor::{Actor, ActorConfig, StepContext},
        completion::ReportSlot,
        driver::ActorRuntime,
        lease::{CapacityBundle, WorkLease},
        transaction::RuntimeLedger,
    };
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        completion::{CompletionError, ConsumerKind},
        effect::{EffectDescriptor, EffectKind},
        identity::{ActionId, ActorId, ConsumerId, DimensionId, Generation, ScopeId, Tick, WorkId},
        mailbox::{InputClass, InputEnvelope},
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_execution_contract::{
        AbortCause, TaskOutputFacts, TaskState, TaskStatusVersion,
        failure::{FailureClass, FailureProjection},
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_worker_protocol::proposal_summary::{ConclusionState, Diagnostics};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::{
        control_runtime::source_positions::{ReapedPosition, SourcePositionPhase},
        task_completion_roles::AdmittedCompletionRole,
        task_proposal_ingress::PairRoute,
    };

    struct Notice(Arc<AtomicUsize>);
    impl TaskPendingNotice for Notice {
        fn pending(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    // This actor grants only a neutral publication ticket. Protocol decisions
    // in these tests come from the real pure summary author, never this driver.
    #[derive(Debug)]
    struct Publisher;
    impl Actor for Publisher {
        type Input = WorkId;
        type Effect = ();
        fn timer_input(_: Tick) -> WorkId {
            unreachable!("no timer")
        }
        fn handle(
            &mut self,
            _: Tick,
            _: InputEnvelope,
            work: WorkId,
            step: &mut StepContext<'_, ()>,
        ) {
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(1).unwrap(),
                    work,
                    kind: EffectKind::Publication,
                    dependencies: vec![],
                },
                (),
            )
            .unwrap();
        }
        fn next_deadline(&self) -> Option<Tick> {
            None
        }
        fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut StepContext<'_, ()>) {}
    }
    struct Fixture {
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        runtime: ActorRuntime<Publisher>,
        work: WorkLease,
        capacity: CapacityBundle,
        port: Arc<TaskRuntimePort>,
        wakes: Arc<AtomicUsize>,
        open_consuming: AtomicBool,
    }
    fn actor() -> ActorId {
        ActorId::new(1).unwrap()
    }
    fn fixture() -> Fixture {
        fixture_with_source_notice(None, false)
    }
    fn fixture_with_source_notice(
        source_notice: Option<Arc<dyn TaskPendingNotice>>,
        prestart_first_source: bool,
    ) -> Fixture {
        let ledger = RuntimeLedger::new(
            ResourceVector::new([(DimensionId::new(1).unwrap(), 1)]).unwrap(),
            32,
            1,
        );
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap(), 32, 32)
            .unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ResourceVector::new([(DimensionId::new(1).unwrap(), 1)]).unwrap(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new(1);
        runtime
            .register_next(
                actor(),
                scope.clone(),
                Publisher,
                ActorConfig {
                    mailbox: [4; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap();
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(502, 503), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let wakes = Arc::new(AtomicUsize::new(0));
        let pending = source_notice.unwrap_or_else(|| Arc::new(Notice(wakes.clone())));
        // Publisher is not WorkerCore. These actual paid source roles declare
        // only the host observer, never a fictitious Core InputId consumer.
        let roles = std::array::from_fn(|index| {
            let role = admitted_source(&scope, capacity.id());
            let mut role = role;
            if prestart_first_source && index == 0 {
                // Deliberate custody fault: the original binder must reject
                // this already-started header and preserve Unknown custody.
                role.work.start().unwrap();
            }
            crate::control_runtime::source_positions::SourceSlotAdmission::protocol_model(
                crate::control_runtime::admitted_role::adopt_reported_work(
                    role.work,
                    role.capacity,
                    role.report,
                ),
            )
        });
        let health = Arc::new(ExecutionHealth::default());
        let ingress = Arc::new(
            TaskProposalIngress::from_admitted(identity, 2, roles, pending.clone(), health.clone())
                .unwrap_or_else(|_| panic!("the original paid source roles fit")),
        );
        let port = Arc::new(TaskRuntimePort::from_admitted(
            identity, &scope, ingress, pending, health,
        ));
        Fixture {
            ledger,
            scope,
            runtime,
            work,
            capacity,
            port,
            wakes,
            open_consuming: AtomicBool::new(false),
        }
    }
    fn observer() -> ConsumerId {
        ConsumerId::new(2).unwrap()
    }
    fn admitted_source(
        scope: &ScopeHandle,
        parent: novarocks_control_runtime_core::identity::ReservationId,
    ) -> AdmittedCompletionRole {
        let generation = scope.gates().unwrap().generation;
        let guard = scope
            .reserve_child_next(
                generation,
                parent,
                ResourceVector::default(),
                WorkKind::Cleanup,
            )
            .unwrap();
        let report = ReportSlot::reserve_next(
            scope,
            generation,
            guard.work_id(),
            1,
            [(observer(), ConsumerKind::Observer)],
        )
        .unwrap();
        let (work, capacity) = guard.bind(WorkKind::Cleanup).unwrap();
        AdmittedCompletionRole {
            work,
            capacity,
            report,
        }
    }
    impl Fixture {
        fn take_summary(&self) -> crate::task_proposal_ingress::SummaryInput {
            let input = self.port.ingress().take_next_summary().unwrap();
            assert!(
                input.cut.is_none(),
                "this publisher consumes the open prefix"
            );
            assert!(!self.open_consuming.swap(true, Ordering::SeqCst));
            input
        }
        fn publish(&self, next: TaskProjection) {
            let input = self
                .runtime
                .reserve_input(actor(), ActorId::new(2).unwrap(), InputClass::Lifecycle)
                .unwrap()
                .fill(self.work.id())
                .unwrap();
            self.runtime.poll_one(Tick(1)).unwrap();
            let ticket = self
                .runtime
                .take_effect(actor(), input, ActionId::new(1).unwrap())
                .unwrap();
            let old = self
                .runtime
                .commit_snapshot(&ticket, self.port.publication(), Arc::new(next))
                .unwrap_or_else(|_| panic!("current generation publication"));
            drop(old);
            self.runtime.effect_completed(ticket).unwrap();
            self.runtime.retire_input(actor(), input).unwrap();
        }
        fn reap_pair(&self, route: PairRoute) -> [ReapedPosition; 2] {
            for holder in self
                .port
                .ingress()
                .take_unused_pair(route)
                .unwrap()
                .into_iter()
                .flatten()
            {
                holder.exit();
            }
            for observation in self.port.ingress().pair_inventory(route).unwrap() {
                assert!(observation.custody.actual_exit_ready().unwrap());
                if observation.source.is_some() {
                    observation.custody.report().consume(observer()).unwrap();
                } else {
                    assert_eq!(
                        observation.custody.report().consume(observer()),
                        Err(
                            novarocks_control_runtime::transaction::RuntimeError::Completion(
                                CompletionError::NotFilled
                            )
                        )
                    );
                }
                assert!(observation.custody.try_retire().unwrap());
            }
            // The common reaper can retire another already-exited source in
            // this same real scope first. Its bounded ordering is independent
            // of the bank's pair order; exact witnesses below decide reuse.
            let admitted_headers = self.scope.snapshot().unwrap().work;
            self.ledger
                .reap_scope_ready(self.scope.id(), admitted_headers)
                .unwrap();
            self.port
                .ingress()
                .prepare_pair_reap(route)
                .unwrap()
                .map(|probe| probe.observe_reaped().unwrap())
        }
        fn finish(mut self) {
            self.port.retire_execution_port();
            let detached = self.port.prepare_source_retirement().unwrap();
            self.publish(detached);
            self.port.ingress().seal_source_admission();
            let _open = self.reap_pair(PairRoute::Open);
            let consuming = self.reap_pair(PairRoute::ConsumingOpen);
            // A summary already transferred to this test's actual pure
            // consumer keeps its separate acknowledgement. No report receipt
            // is fabricated for a WorkerCore which this fixture does not run.
            if self.open_consuming.load(Ordering::SeqCst) {
                self.port
                    .ingress()
                    .finish_open_after_reap(consuming)
                    .unwrap_or_else(|_| panic!("the actual consumed prefix must retire"));
            } else {
                drop(consuming);
            }
            assert_eq!(self.port.ingress().retained_sources(), 0);
            self.work.body_exited().unwrap();
            self.work.captures_exited().unwrap();
            self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
            self.capacity
                .release(&self.capacity.resources().unwrap())
                .unwrap();
            self.scope.close_external().unwrap();
            while let Some(id) = self.runtime.claim_effect_actor() {
                self.runtime.release_effect_actor(id, false).unwrap();
            }
            self.runtime.remove_actor(actor()).unwrap();
            self.ledger.remove_scope(self.scope.id()).unwrap();
            assert!(self.ledger.is_empty());
        }
    }
    #[test]
    fn source_retirement_preserves_neutral_receipt_and_requires_a_real_host_commit() {
        struct Source {
            port: Weak<TaskRuntimePort>,
            preparation: Arc<crate::task_preparation::TaskPreparationScope>,
            drops: Arc<AtomicUsize>,
            source: Arc<Mutex<Option<SourceId>>>,
        }
        impl std::fmt::Debug for Source {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("source retirement lock probe")
            }
        }
        impl std::fmt::Display for Source {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("source retirement lock probe")
            }
        }
        impl std::error::Error for Source {}
        impl FailureProjection for Source {
            fn failure_fact(&self) -> FailureFact {
                FailureFact::new(
                    FailureClass::Data,
                    crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
                )
            }
        }
        impl Drop for Source {
            fn drop(&mut self) {
                let port = self
                    .port
                    .upgrade()
                    .expect("the host retains original port custody");
                // Reenter both real metadata locks. A source destructor cannot
                // run inside the snapshot swap or the proposal handoff lock.
                assert!(port.publication().read().neutral_accepted().is_some());
                assert!(port.accepted().is_none());
                let id = self
                    .source
                    .lock()
                    .unwrap()
                    .expect("actual summary source id");
                let original = port.ingress().source_inventory(id).unwrap();
                assert_eq!(original.source, Some(id));
                assert_eq!(original.phase, SourcePositionPhase::Taken);
                assert!(!original.custody.actual_exit_ready().unwrap());
                assert!(!original.custody.reusable());
                assert_eq!(port.ingress().retained_sources(), 1);
                assert!(!self.preparation.physical_bodies_exited());
                self.preparation.admission_exited();
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let f = fixture();
        let preparation = Arc::new(crate::task_preparation::TaskPreparationScope::new(
            f.port.identity(),
            f.port.health(),
        ));
        preparation.begin_admission();
        let drops = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(Mutex::new(None));
        f.port
            .submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        f.port.submit(TaskConclusionProposal::Failed {
            cause: ExecutionFailure::from_domain(Source {
                port: Arc::downgrade(&f.port),
                preparation: preparation.clone(),
                drops: drops.clone(),
                source: source.clone(),
            }),
            phase: TaskFailurePhase::Preparation,
        });
        let input = f.take_summary();
        let result = input
            .summary
            .apply(ConclusionState::default(), Diagnostics::default());
        *source.lock().unwrap() = result.state.source;
        assert_eq!(
            result.state.generation, 2,
            "retain the actual compressed Core watermark"
        );
        let accepted = f
            .port
            .prepare_conclusion(
                NonZeroU64::new(result.state.generation).unwrap(),
                result.state.accepted.unwrap(),
                result.state.source,
            )
            .unwrap();
        let expected = accepted.neutral().clone();
        f.publish(
            f.port
                .publication()
                .read()
                .with_conclusion(accepted)
                .unwrap()
                .with_diagnostics(&result.diagnostics)
                .unwrap(),
        );
        f.port.ingress().summary_consumed(input.cut).unwrap();
        assert!(matches!(
            f.port.prepare_source_retirement(),
            Err(ProjectionError::SourceStillBorrowed)
        ));
        let before = f.port.publication().read();
        let status = before.status().clone();
        let convergence = before.convergence();
        let diagnostics = before.secondary_diagnostics().clone();
        drop(before);
        let work_before = f.scope.snapshot().unwrap().work;
        f.port.retire_execution_port();
        let candidate = f.port.prepare_source_retirement().unwrap();
        assert!(candidate.accepted().is_none());
        assert_eq!(candidate.neutral_accepted(), Some(&expected));
        assert!(
            f.port.accepted().is_some(),
            "preparing is not a snapshot commit"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        f.publish(candidate);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let actual = f.port.publication().read();
        assert!(actual.accepted().is_none());
        assert_eq!(actual.neutral_accepted(), Some(&expected));
        assert_eq!(actual.status(), &status);
        assert_eq!(actual.convergence(), convergence);
        assert_eq!(actual.secondary_diagnostics(), &diagnostics);
        let owner_failure = f.port.prepare_owner_failure().unwrap();
        assert!(
            owner_failure.accepted().is_none(),
            "OwnerFailed cannot recreate a retired rich source"
        );
        assert_eq!(owner_failure.neutral_accepted(), Some(&expected));
        assert_eq!(owner_failure.status(), &status);
        assert_eq!(owner_failure.convergence(), convergence);
        assert_eq!(owner_failure.secondary_diagnostics(), &diagnostics);
        f.port.notify_stopped();
        assert!(
            f.port.is_stopped(),
            "a published neutral receipt remains valid after source disposal"
        );
        assert_eq!(f.scope.snapshot().unwrap().work, work_before);
        assert!(
            !preparation.physical_bodies_exited(),
            "source disposal is not preparation whole exit"
        );
        assert!(f.port.health().is_healthy());
        for generation in [1, 2] {
            let older = Arc::new(
                AcceptedTaskConclusion::new(
                    AcceptedConclusion::new(
                        f.port.identity(),
                        NonZeroU64::new(generation).unwrap(),
                        TaskConclusion::Terminated(
                            novarocks_execution_contract::TerminationDetail::Aborted(
                                AbortCause::PeerTaskFailed,
                            ),
                        ),
                    ),
                    None::<ExecutionFailure>,
                )
                .unwrap(),
            );
            assert!(
                matches!(
                    actual.with_conclusion(older),
                    Err(ProjectionError::ConclusionGeneration)
                ),
                "rich-source retirement must never reset accepted generation to zero"
            );
        }
        let repeated = f.port.prepare_source_retirement().unwrap();
        assert_eq!(repeated.neutral_accepted(), Some(&expected));
        assert!(repeated.accepted().is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        preparation.body_exited();
        assert!(preparation.physical_bodies_exited());
        f.finish();
    }
    #[test]
    fn neutral_lifecycle_facts_commit_with_the_existing_task_projection() {
        use novarocks_worker_protocol::convergence::TaskConvergence;
        let f = fixture();
        let initial = f.port.publication().read();
        assert_eq!(initial.convergence(), TaskConvergence::new().snapshot());
        assert_eq!(initial.secondary_diagnostics().entries().len(), 0);
        let mut convergence = TaskConvergence::new();
        convergence.note_actual_stopped().unwrap();
        convergence.note_resources_converged().unwrap();
        let diagnostic = Diagnostic {
            phase: TaskFailurePhase::Execution,
            fact: FailureFact::new(
                FailureClass::Resource,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
            ),
            detail: SafeDetail::truncating("cleanup fact after actual runtime exit"),
            cleanup: true,
        };
        let mut diagnostics = Diagnostics::default();
        diagnostics.record(diagnostic.clone());
        let prepared = initial
            .with_convergence(convergence.snapshot())
            .unwrap()
            .with_diagnostics(&diagnostics)
            .unwrap();
        assert_eq!(f.port.publication().read().convergence().version(), 0);
        assert_eq!(
            f.port
                .publication()
                .read()
                .secondary_diagnostics()
                .entries()
                .len(),
            0
        );
        f.publish(prepared);
        let actual = f.port.publication().read();
        assert_eq!(actual.convergence(), convergence.snapshot());
        assert_eq!(actual.status(), initial.status());
        assert!(actual.accepted().is_none());
        let entry = actual.secondary_diagnostics().entries().next().unwrap();
        assert_eq!(entry.role(), crate::TaskDiagnosticRole::Cleanup);
        assert_eq!(entry.phase(), diagnostic.phase);
        assert_eq!(entry.fact(), diagnostic.fact);
        assert_eq!(entry.detail(), diagnostic.detail.as_str());
        let accepted = f
            .port
            .prepare_conclusion(NonZeroU64::new(1).unwrap(), TaskConclusion::Succeeded, None)
            .unwrap();
        let preserved = actual
            .with_status(actual.status().clone())
            .unwrap()
            .with_conclusion(accepted)
            .unwrap();
        assert_eq!(preserved.convergence(), actual.convergence());
        assert_eq!(
            preserved.secondary_diagnostics(),
            actual.secondary_diagnostics()
        );
        f.publish(preserved);
        assert_eq!(
            f.port.publication().read().convergence(),
            convergence.snapshot()
        );
        f.finish();
    }
    #[test]
    fn neutral_projection_checks_real_versions_and_retained_diagnostic_history() {
        use novarocks_worker_protocol::convergence::TaskConvergence;
        let f = fixture();
        let initial = f.port.publication().read();
        let mut stopped = TaskConvergence::new();
        stopped.note_actual_stopped().unwrap();
        let mut released = TaskConvergence::new();
        released.note_output_released().unwrap();
        let projection = initial.with_convergence(stopped.snapshot()).unwrap();
        assert!(matches!(
            projection.with_convergence(released.snapshot()),
            Err(ProjectionError::SameVersionDifferentConvergence)
        ));
        assert!(matches!(
            projection.with_convergence(initial.convergence()),
            Err(ProjectionError::ConvergenceRegressed)
        ));
        released.note_conclusion_stable().unwrap();
        assert!(
            matches!(
                projection.with_convergence(released.snapshot()),
                Err(ProjectionError::ConvergenceRegressed)
            ),
            "a newer unrelated observation cannot clear positive stop evidence"
        );
        assert_eq!(
            projection
                .with_convergence(stopped.snapshot())
                .unwrap()
                .convergence(),
            stopped.snapshot()
        );
        let mut diagnostics = Diagnostics::default();
        for cleanup in [false, true, false, true] {
            diagnostics.record(Diagnostic {
                phase: TaskFailurePhase::Execution,
                fact: FailureFact::new(
                    FailureClass::Invariant,
                    crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
                ),
                detail: SafeDetail::truncating("ordered neutral diagnostic"),
                cleanup,
            });
        }
        let projection = projection.with_diagnostics(&diagnostics).unwrap();
        diagnostics.dropped = u64::MAX;
        let projection = projection.with_diagnostics(&diagnostics).unwrap();
        assert_eq!(projection.secondary_diagnostics().dropped(), u64::MAX);
        assert!(matches!(
            projection.with_diagnostics(&Diagnostics::default()),
            Err(ProjectionError::DiagnosticsRegressed)
        ));
        let mut regressed = diagnostics.clone();
        regressed.dropped -= 1;
        assert!(matches!(
            projection.with_diagnostics(&regressed),
            Err(ProjectionError::DiagnosticsRegressed)
        ));
        let mut changed = diagnostics.clone();
        changed.entries[0].cleanup = true;
        assert!(matches!(
            projection.with_diagnostics(&changed),
            Err(ProjectionError::DiagnosticsPrefixChanged)
        ));
        let mut oversized = diagnostics.clone();
        oversized.entries.push(diagnostics.entries[0].clone());
        assert!(matches!(
            projection.with_diagnostics(&oversized),
            Err(ProjectionError::DiagnosticsCapacity)
        ));
        assert_eq!(
            projection
                .with_diagnostics(&diagnostics)
                .unwrap()
                .secondary_diagnostics(),
            projection.secondary_diagnostics()
        );
        f.finish();
    }
    #[test]
    fn execution_view_cannot_keep_its_own_accepted_source_and_task_owner_alive() {
        use novarocks_execution::runtime::task_control::TaskExecutionControl;
        #[derive(Debug)]
        struct CapturedControl {
            control: Arc<TaskExecutionControl>,
            dropped: Arc<AtomicUsize>,
        }
        impl std::fmt::Display for CapturedControl {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("captured Task control probe")
            }
        }
        impl std::error::Error for CapturedControl {}
        impl FailureProjection for CapturedControl {
            fn failure_fact(&self) -> FailureFact {
                FailureFact::new(
                    FailureClass::Data,
                    crate::failure_reason::WorkerFailureReason::ProtocolOwnerFailed.reason(),
                )
            }
        }
        impl Drop for CapturedControl {
            fn drop(&mut self) {
                assert!(self.control.is_stopped());
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        let f = fixture();
        let view = f.port.execution_port();
        let control = Arc::new(
            TaskExecutionControl::new(f.port.identity(), view.clone(), TaskFailurePhase::Execution)
                .unwrap(),
        );
        let dropped = Arc::new(AtomicUsize::new(0));
        control.report_failure(ExecutionFailure::from_domain(CapturedControl {
            control: control.clone(),
            dropped: dropped.clone(),
        }));
        let summary = f.take_summary();
        let result = summary
            .summary
            .apply(ConclusionState::default(), Diagnostics::default());
        let accepted = f
            .port
            .prepare_conclusion(
                NonZeroU64::new(result.state.generation).unwrap(),
                result.state.accepted.unwrap(),
                result.state.source,
            )
            .unwrap();
        let next = f
            .port
            .publication()
            .read()
            .with_conclusion(accepted)
            .unwrap();
        f.publish(next);
        f.port.ingress().summary_consumed(summary.cut).unwrap();
        f.port.notify_stopped();
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert_eq!(
            Arc::strong_count(&f.port),
            1,
            "source -> captured control -> execution view must not strongly retain the owning port"
        );
        let weak = Arc::downgrade(&f.port);
        let health = f.port.health();
        f.port.retire_execution_port();
        assert!(view.is_stopped());
        assert!(
            view.accepted().is_none(),
            "closed execution views cannot borrow rich projections"
        );
        assert!(
            health.is_healthy(),
            "closing the borrow gate alone is not unresolved exit"
        );
        f.finish();
        assert!(weak.upgrade().is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(control.is_stopped());
        assert_eq!(
            control.error().unwrap().failure_fact().class(),
            FailureClass::Invariant
        );
        assert!(health.failure().is_some());
    }
    #[test]
    fn closing_execution_borrowing_wakes_existing_observers_and_refuses_late_sources() {
        use novarocks_execution::runtime::task_control::{
            TaskExecutionControl, TaskExecutionOwnerRetired,
        };
        struct LateSource {
            view: Arc<TaskExecutionControl>,
            dropped: Arc<AtomicUsize>,
            thread: std::thread::ThreadId,
        }
        impl std::fmt::Debug for LateSource {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("late source probe")
            }
        }
        impl std::fmt::Display for LateSource {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("late source probe")
            }
        }
        impl std::error::Error for LateSource {}
        impl FailureProjection for LateSource {
            fn failure_fact(&self) -> FailureFact {
                FailureFact::new(
                    FailureClass::Data,
                    crate::failure_reason::WorkerFailureReason::ProtocolOwnerFailed.reason(),
                )
            }
        }
        impl Drop for LateSource {
            fn drop(&mut self) {
                assert_eq!(
                    std::thread::current().id(),
                    self.thread,
                    "late source disposal belongs to its original submitting stack"
                );
                assert!(self.view.is_stopped());
                assert!(self.view.accepted().is_none());
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        let f = fixture();
        let view = f.port.execution_port();
        let control = Arc::new(
            TaskExecutionControl::new(
                f.port.identity(),
                view.clone(),
                TaskFailurePhase::Preparation,
            )
            .unwrap(),
        );
        let observable = control.stop_observable();
        let weak = Arc::downgrade(&control);
        let notifications = Arc::new(AtomicUsize::new(0));
        let seen = notifications.clone();
        observable.add_observer(Arc::new(move || {
            let control = weak.upgrade().unwrap();
            assert!(control.is_stopped());
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        assert!(Arc::ptr_eq(&observable, &f.port.stop_observable()));
        assert!(!control.is_stopped());
        f.port.retire_execution_port();
        f.port.retire_execution_port();
        assert_eq!(notifications.load(Ordering::SeqCst), 1);
        assert!(control.wait_until_stopped(Duration::ZERO));
        assert!(
            !f.port.is_stopped(),
            "closing borrowing is not a producer-stop publication"
        );
        assert!(f.port.accepted().is_none());
        assert!(f.port.health().is_healthy());
        let dropped = Arc::new(AtomicUsize::new(0));
        control.report_failure(ExecutionFailure::from_domain(LateSource {
            view: control.clone(),
            dropped: dropped.clone(),
            thread: std::thread::current().id(),
        }));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(f.wakes.load(Ordering::SeqCst), 0);
        assert_eq!(f.port.ingress().retained_sources(), 0);
        let expected = TaskExecutionOwnerRetired::new(f.port.identity()).failure_fact();
        assert_eq!(f.port.health().failure().unwrap().fact(), expected);
        assert_eq!(
            f.port.health().failure().unwrap().origin(),
            ExecutionExitOrigin::Task {
                identity: f.port.identity(),
                phase: TaskFailurePhase::Preparation,
            }
        );
        assert_eq!(control.error().unwrap().failure_fact(), expected);
        f.finish();
    }
    #[test]
    fn owner_failure_projection_publishes_a_nonterminal_invariant_and_preserves_prior_promises() {
        let f = fixture();
        let projection = f.port.prepare_owner_failure().unwrap();
        assert!(f.port.accepted().is_none(), "preparing is not publication");
        assert!(!f.port.is_stopped());
        assert_eq!(projection.status().state(), TaskState::Failing);
        assert!(!projection.status().installed());
        assert!(!projection.status().is_terminal());
        let failure = projection.accepted().unwrap();
        let TaskConclusion::Terminated(novarocks_execution_contract::TerminationDetail::Failed(
            detail,
        )) = failure.neutral().conclusion()
        else {
            panic!("an unpublished Task gets the fixed owner failure");
        };
        assert_eq!(detail.fact().class(), FailureClass::Invariant);
        assert_eq!(detail.phase(), TaskFailurePhase::Preparation);
        f.publish(projection);
        f.port.notify_stopped();
        let again = f.port.prepare_owner_failure().unwrap();
        assert!(Arc::ptr_eq(&failure, &again.accepted().unwrap()));
        assert_eq!(&f.port.current(), again.status());
        f.finish();

        for conclusion in [
            TaskConclusion::Succeeded,
            TaskConclusion::Terminated(novarocks_execution_contract::TerminationDetail::Aborted(
                AbortCause::PeerTaskFailed,
            )),
        ] {
            let f = fixture();
            let accepted = f
                .port
                .prepare_conclusion(NonZeroU64::new(1).unwrap(), conclusion, None)
                .unwrap();
            f.publish(
                f.port
                    .publication()
                    .read()
                    .with_conclusion(accepted.clone())
                    .unwrap(),
            );
            let failure = f.port.prepare_owner_failure().unwrap();
            assert!(Arc::ptr_eq(&accepted, &failure.accepted().unwrap()));
            assert_eq!(failure.status(), &f.port.current());
            f.finish();
        }
    }
    #[test]
    fn submitting_only_hands_off_and_a_compressed_core_generation_publishes_through_the_runtime() {
        let f = fixture();
        let view = f.port.execution_port();
        assert!(Arc::ptr_eq(&view.execution_health(), &f.port.health()));
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        view.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        view.submit(TaskConclusionProposal::Failed {
            cause: original.clone(),
            phase: TaskFailurePhase::Execution,
        });
        assert!(f.port.accepted().is_none());
        assert_eq!(f.wakes.load(Ordering::SeqCst), 2);
        let input = f.take_summary();
        let result = input
            .summary
            .apply(ConclusionState::default(), Diagnostics::default());
        assert_eq!(result.state.generation, 2);
        let accepted = f
            .port
            .prepare_conclusion(
                NonZeroU64::new(result.state.generation).unwrap(),
                result.state.accepted.unwrap(),
                result.state.source,
            )
            .unwrap();
        assert!(std::ptr::eq(
            accepted.originating_failure().unwrap().cause(),
            original.cause()
        ));
        assert!(
            f.port.accepted().is_none(),
            "preparing an effect is not publication"
        );
        f.publish(
            f.port
                .publication()
                .read()
                .with_conclusion(accepted)
                .unwrap(),
        );
        f.port.ingress().summary_consumed(input.cut).unwrap();
        assert!(
            !f.port.is_stopped(),
            "the post-publication stop effect installs the presence hint"
        );
        f.port.notify_stopped();
        assert!(view.wait_until_stopped(Duration::ZERO));
        assert!(Arc::ptr_eq(
            &view.accepted().unwrap(),
            &f.port.accepted().unwrap()
        ));
        assert_eq!(f.port.accepted().unwrap().neutral().generation().get(), 2);
        assert!(std::ptr::eq(
            f.port
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .cause(),
            original.cause()
        ));
        drop(original);
        f.finish();
    }
    #[test]
    fn a_failed_handoff_wakes_its_live_custodian_without_accepting_late_sources() {
        let f = fixture();
        f.port.ingress().fail();
        f.port.submit(TaskConclusionProposal::Succeeded);
        let cause: ExecutionFailure = novarocks_functions::KernelFailure::ResourceExhausted.into();
        f.port.submit(TaskConclusionProposal::Failed {
            cause: cause.clone(),
            phase: TaskFailurePhase::Execution,
        });
        f.port.report_cleanup(&cause, TaskFailurePhase::Execution);
        assert_eq!(f.wakes.load(Ordering::SeqCst), 2);
        assert_eq!(f.port.ingress().retained_sources(), 0);
        assert!(f.port.accepted().is_none());
        f.finish();
    }
    #[test]
    fn cleanup_and_unresolved_exit_never_acquire_task_conclusion_authority() {
        let f = fixture();
        let cause: ExecutionFailure = novarocks_functions::KernelFailure::ResourceExhausted.into();
        f.port.report_cleanup(&cause, TaskFailurePhase::Execution);
        f.port
            .report_unresolved(cause.failure_fact(), TaskFailurePhase::Execution);
        let input = f.take_summary();
        let result = input
            .summary
            .apply(ConclusionState::default(), Diagnostics::default());
        assert!(result.state.accepted.is_none());
        assert_eq!(result.diagnostics.entries.len(), 1);
        assert!(result.diagnostics.entries[0].cleanup);
        assert!(f.port.accepted().is_none());
        assert!(f.port.health().failure().is_some());
        assert_eq!(
            f.port.health().failure().unwrap().fact().class(),
            FailureClass::Resource
        );
        f.port.ingress().summary_consumed(input.cut).unwrap();
        f.finish();
    }
    #[test]
    fn projection_rejects_host_reordering_without_readjudicating_core_policy() {
        let f = fixture();
        let status = |version, state, installed| {
            TaskStatus::try_new_with_installed(
                f.port.identity(),
                TaskStatusVersion::new(version).unwrap(),
                state,
                None,
                TaskOutputFacts::new(false),
                installed,
            )
            .unwrap()
        };
        let current = f
            .port
            .publication()
            .read()
            .with_status(status(3, TaskState::Running, true))
            .unwrap();
        assert!(matches!(
            current.with_status(status(2, TaskState::Planned, false)),
            Err(ProjectionError::StatusRegressed)
        ));
        assert!(matches!(
            current.with_status(status(3, TaskState::Flushing, true)),
            Err(ProjectionError::SameVersionDifferentStatus)
        ));
        assert!(matches!(
            f.port.prepare_conclusion(
                NonZeroU64::new(1).unwrap(),
                TaskConclusion::Succeeded,
                Some(SourceId(999))
            ),
            Err(TaskConclusionRefusal {
                error: ProjectionError::MissingSource(SourceId(999)),
                source: None,
            })
        ));
        assert_eq!(f.port.current().version(), TaskStatusVersion::FIRST);
        f.finish();
    }

    #[test]
    fn conclusion_refusal_keeps_the_same_originating_carrier_and_paid_source_guard() {
        let f = fixture();
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        f.port.submit(TaskConclusionProposal::Failed {
            cause: original.clone(),
            phase: TaskFailurePhase::Execution,
        });
        let input = f.take_summary();
        let result = input
            .summary
            .apply(ConclusionState::default(), Diagnostics::default());
        let id = result.state.source.expect("actual pure summary source id");
        let refusal = f
            .port
            .prepare_conclusion(
                NonZeroU64::new(result.state.generation).unwrap(),
                TaskConclusion::Succeeded,
                Some(id),
            )
            .unwrap_err();
        assert_eq!(
            refusal.error,
            ProjectionError::InvalidSource(ConclusionSourceError::UnexpectedOriginatingFailure)
        );
        let retained = refusal.source.expect("the unchanged original source guard");
        assert!(std::ptr::eq(retained.cause().cause(), original.cause()));
        let observation = f.port.ingress().source_inventory(id).unwrap();
        assert_eq!(observation.phase, SourcePositionPhase::Taken);
        assert_eq!(observation.source, Some(id));
        assert!(!observation.custody.actual_exit_ready().unwrap());
        assert!(!observation.custody.try_retire().unwrap());
        assert!(!observation.custody.reusable());
        f.port.ingress().summary_consumed(input.cut).unwrap();
        drop(retained);
        assert!(
            !observation.custody.actual_exit_ready().unwrap(),
            "the real early ExecutionFailure alias still retains the source node"
        );
        drop(original);
        assert!(observation.custody.actual_exit_ready().unwrap());
        assert!(!observation.custody.reusable());
        assert_eq!(f.port.ingress().retained_sources(), 1);
        f.finish();
    }

    #[test]
    fn submit_bind_unwind_still_wakes_the_live_custodian_after_the_cut_unlocks() {
        struct BindNotice {
            ingress: Mutex<Weak<TaskProposalIngress>>,
            calls: AtomicUsize,
            observed_after_unwind: AtomicBool,
        }
        impl TaskPendingNotice for BindNotice {
            fn pending(&self) {
                let ingress = self.ingress.lock().unwrap().upgrade().unwrap();
                // This reenters the actual cut fence, rather than a test lock.
                let observed = ingress.pair_inventory(PairRoute::Open).unwrap();
                let source = observed
                    .into_iter()
                    .find(|position| position.source.is_some())
                    .expect("the original claim has its actual source identity");
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    assert_eq!(source.phase, SourcePositionPhase::Pending);
                    panic!("controlled failure of the binder's original pending notice");
                }
                assert_eq!(source.phase, SourcePositionPhase::Abandoned);
                assert_eq!(
                    source.custody.phase(),
                    novarocks_control_runtime::exit_role::ExitRolePhase::Unresolved
                );
                assert!(!source.custody.actual_exit_ready().unwrap());
                self.observed_after_unwind.store(true, Ordering::SeqCst);
            }
        }
        let notice = Arc::new(BindNotice {
            ingress: Mutex::new(Weak::new()),
            calls: AtomicUsize::new(0),
            observed_after_unwind: AtomicBool::new(false),
        });
        let f = fixture_with_source_notice(Some(notice.clone()), true);
        *notice.ingress.lock().unwrap() = Arc::downgrade(f.port.ingress());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            f.port.submit(TaskConclusionProposal::Failed {
                cause: novarocks_functions::KernelFailure::ResourceExhausted.into(),
                phase: TaskFailurePhase::Execution,
            });
        }));
        assert!(result.is_err());
        assert_eq!(notice.calls.load(Ordering::SeqCst), 2);
        assert!(notice.observed_after_unwind.load(Ordering::SeqCst));
        let original = f
            .port
            .ingress()
            .pair_inventory(PairRoute::Open)
            .unwrap()
            .into_iter()
            .find(|position| position.source.is_some())
            .unwrap();
        assert_eq!(original.phase, SourcePositionPhase::Abandoned);
        assert!(!original.custody.reusable());
        assert!(!f.ledger.unresolved_work().is_empty());
        assert_eq!(f.scope.snapshot().unwrap().work, 5);
        assert!(f.port.health().failure().is_some());
        // The deliberately damaged original Work stays Unknown. This test
        // cannot finish it, acknowledge its report or claim an empty ledger.
    }
}
