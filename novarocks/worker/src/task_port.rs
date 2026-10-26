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
        Arc, Condvar, Mutex,
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
    AcceptedConclusion, AcceptedTaskConclusion, ConclusionSourceError, SafeDetail, TaskConclusion,
    TaskConclusionProposal, TaskFailurePhase, TaskIdentity, TaskStatus, failure::FailureFact,
};
use novarocks_worker_protocol::proposal_summary::{Diagnostic, SourceId};

use crate::task_proposal_ingress::TaskProposalIngress;

/// One prepaid/coalesced ready position. Implementations only mark this exact
/// Task pending; they never wait for its actor, run an observer, or execute I/O.
/// An admitted role keeps waking its live host custodian after Core failure;
/// that custodian routes work to failure cleanup instead of the dead Core.
/// Ignoring late registration/revocation/exit would strand original duties.
pub(crate) trait TaskPendingNotice: Send + Sync {
    fn pending(&self);
}

/// Read-only metadata for the exact Task's published producer-stop intent.
/// Clones cannot clear it, notify observers, or claim any physical exit.
#[derive(Clone)]
pub(crate) struct TaskStopSignal {
    identity: TaskIdentity,
    stopped: Arc<AtomicBool>,
}
impl TaskStopSignal {
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
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProjectionError {
    WrongTask,
    StatusRegressed,
    SameVersionDifferentStatus,
    ConclusionGeneration,
    MissingSource(SourceId),
    InvalidSource(ConclusionSourceError),
    StatusVersionExhausted,
}
impl TaskProjection {
    fn bootstrap(identity: TaskIdentity) -> Self {
        Self {
            status: TaskStatus::created(identity),
            accepted: None,
        }
    }
    pub(crate) fn status(&self) -> &TaskStatus {
        &self.status
    }
    pub(crate) fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>> {
        self.accepted.clone()
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
            accepted: self.accepted.clone(),
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
            .accepted
            .as_ref()
            .map_or(0, |accepted| accepted.neutral().generation().get());
        // One exact compressed summary may accept a peer placeholder and
        // refine it before its single final publication, advancing by two.
        if next.neutral().generation().get() <= previous {
            return Err(ProjectionError::ConclusionGeneration);
        }
        Ok(Self {
            status: self.status.clone(),
            accepted: Some(next),
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
        Self {
            identity,
            proposals,
            pending,
            health,
            published: PublishedSnapshot::new(scope, Arc::new(TaskProjection::bootstrap(identity))),
            stop: Arc::new(Observable::new()),
            stop_published: Arc::new(AtomicBool::new(false)),
            wait: Mutex::new(()),
            changed: Condvar::new(),
        }
    }
    pub(crate) fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub(crate) fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>> {
        self.published.read().accepted()
    }
    pub(crate) fn current(&self) -> TaskStatus {
        self.published.read().status().clone()
    }
    pub(crate) fn publication(&self) -> &PublishedSnapshot<TaskProjection> {
        &self.published
    }
    pub(crate) fn ingress(&self) -> &Arc<TaskProposalIngress> {
        &self.proposals
    }
    pub(crate) fn submit(&self, proposal: TaskConclusionProposal<ExecutionFailure>) {
        let handoff = self.proposals.submit(proposal);
        if handoff.transferred {
            self.pending.pending();
        }
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

    /// Fixed failure projection from the actual published inventory. This
    /// cannot revoke an existing result or inspect the failed protocol Core.
    /// The host still commits through its original failure publication session
    /// before publishing producer stop or running registered continuations.
    pub(crate) fn prepare_owner_failure(&self) -> Result<TaskProjection, ProjectionError> {
        let current = self.published.read();
        if current.accepted.is_some() {
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
    ) -> Result<Arc<AcceptedTaskConclusion<ExecutionFailure>>, ProjectionError> {
        let cause = source
            .map(|id| {
                self.proposals
                    .take_source(id)
                    .ok_or(ProjectionError::MissingSource(id))
            })
            .transpose()?;
        AcceptedTaskConclusion::new(
            AcceptedConclusion::new(self.identity, generation, conclusion),
            cause,
        )
        .map(Arc::new)
        .map_err(ProjectionError::InvalidSource)
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
            self.accepted().is_some(),
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
            .wait_timeout_while(wait, duration, |()| !self.is_stopped())
            .expect("Task stop wait metadata");
        self.is_stopped()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_control_runtime::{
        actor::{Actor, ActorConfig, StepContext},
        driver::ActorRuntime,
        lease::{CapacityBundle, WorkLease},
        transaction::RuntimeLedger,
    };
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        effect::{EffectDescriptor, EffectKind},
        identity::{ActionId, ActorId, DimensionId, Generation, ScopeId, Tick, WorkId},
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
    }
    fn actor() -> ActorId {
        ActorId::new(1).unwrap()
    }
    fn fixture() -> Fixture {
        let ledger = RuntimeLedger::new(
            ResourceVector::new([(DimensionId::new(1).unwrap(), 1)]).unwrap(),
            1,
            1,
        );
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap(), 1, 1)
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
        let port = Arc::new(TaskRuntimePort::from_admitted(
            identity,
            &scope,
            Arc::new(TaskProposalIngress::new(2).unwrap()),
            Arc::new(Notice(wakes.clone())),
            Arc::new(ExecutionHealth::default()),
        ));
        Fixture {
            ledger,
            scope,
            runtime,
            work,
            capacity,
            port,
            wakes,
        }
    }
    impl Fixture {
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
        fn finish(mut self) {
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
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        f.port
            .submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        f.port.submit(TaskConclusionProposal::Failed {
            cause: original.clone(),
            phase: TaskFailurePhase::Execution,
        });
        assert!(f.port.accepted().is_none());
        assert_eq!(f.wakes.load(Ordering::SeqCst), 2);
        let input = f.port.ingress().take_next_summary().unwrap();
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
        assert!(f.port.wait_until_stopped(Duration::ZERO));
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
        f.finish();
    }
    #[test]
    fn a_failed_handoff_does_not_wake_a_dead_core_or_accept_a_late_source() {
        let f = fixture();
        f.port.ingress().fail();
        f.port.submit(TaskConclusionProposal::Succeeded);
        let cause: ExecutionFailure = novarocks_functions::KernelFailure::ResourceExhausted.into();
        f.port.submit(TaskConclusionProposal::Failed {
            cause: cause.clone(),
            phase: TaskFailurePhase::Execution,
        });
        f.port.report_cleanup(&cause, TaskFailurePhase::Execution);
        assert_eq!(f.wakes.load(Ordering::SeqCst), 0);
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
        let input = f.port.ingress().take_next_summary().unwrap();
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
            Err(ProjectionError::MissingSource(SourceId(999)))
        ));
        assert_eq!(f.port.current().version(), TaskStatusVersion::FIRST);
        f.finish();
    }
}
