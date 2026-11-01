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

//! One admitted holder and its retained source node owe independent exits.
//! Both markers retain the same Work/report; neither issues another obligation
//! or acknowledges consumers. Shared domain handles use their canonical node.
//! Other typed shared owners still need their author-owned retention boundary;
//! this custodian is not an arbitrary error-graph or resource-alias tracer.

mod retirement_notice;

use retirement_notice::RetirementNotices;

use super::{
    actors::WorkerAction, adopted_cleanup::AdoptedCleanupEffect, failure::ScopePublicationsDrained,
};
#[cfg(test)]
use crate::task_completion_roles::AdmittedCompletionRole;
use crate::task_port::TaskPendingNotice;
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::DriverError,
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::WorkRetirementObserver,
    reported_work::ReportedWork,
};
use novarocks_execution::runtime::{
    execution_health::{ExecutionExitOrigin, ExecutionHealth},
    fragment::ExecutionFailure,
};
use novarocks_execution_contract::{
    RetainedTaskSource, SourceRetentionCapability, TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact},
};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Leg {
    Holder,
    SourceNode,
}
#[derive(Default)]
struct Exits {
    authority: Option<ExitAuthority>,
    bound: bool,
    source_phase: Option<TaskFailurePhase>,
    holder: bool,
    source_node: bool,
    unresolved: bool,
    failure_pending: bool,
}
struct Custody {
    task: TaskIdentity,
    scope: novarocks_control_runtime::transaction::ScopeHandle,
    role: ExitRole,
    work: novarocks_control_runtime_core::identity::WorkId,
    report: ReportSlot,
    retirement: WorkRetirementObserver,
    exits: Mutex<Exits>,
    pending: Arc<dyn TaskPendingNotice>,
    retirement_notices: Mutex<RetirementNotices>,
    health: Arc<ExecutionHealth>,
    retained: Mutex<Option<Arc<Self>>>,
    cleanup: Mutex<Option<AdoptedCleanupEffect>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceCleanupError {
    Exit(ExitRoleError),
    Driver(DriverError),
}
#[derive(Clone)]
#[must_use = "The original source Work/report remain until actual retirement"]
pub(crate) struct SourceCustody(Arc<Custody>);
#[must_use = "Consume the original source position or positively exit its unused holder"]
pub(crate) struct SourcePermit {
    custody: SourceCustody,
    active: bool,
}
struct Marker {
    custody: SourceCustody,
    leg: Leg,
}
impl std::fmt::Debug for Marker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Formatting a source must not acquire its own exit-state lock.
        f.debug_struct("OriginalSourceCapture")
            .field("task", &self.custody.0.task)
            .field("leg", &self.leg)
            .finish()
    }
}
impl SourceRetentionCapability for Marker {}
impl SourceCustody {
    /// The actual financial owner; actor consumer identity is independent.
    pub(crate) fn financial_scope(&self) -> &novarocks_control_runtime::transaction::ScopeHandle {
        &self.0.scope
    }
    /// All authority already belongs to the real admission owner. This binds
    /// no source yet, acquires no capacity and creates no new Work or report.
    #[cfg(test)]
    pub(crate) fn from_admitted(
        task: TaskIdentity,
        admitted: AdmittedCompletionRole,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> (Self, SourcePermit) {
        Self::from_reported(
            task,
            super::admitted_role::adopt_reported_work(
                admitted.work,
                admitted.capacity,
                admitted.report,
            ),
            pending,
            health,
        )
    }
    /// Moves an already validated original into custody without revalidating
    /// its binding or acquiring the shared admission ledger lock.
    pub(crate) fn from_reported(
        task: TaskIdentity,
        reported: ReportedWork,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> (Self, SourcePermit) {
        let scope = reported.work().scope().clone();
        let work = reported.work().id();
        let report = reported.report().clone();
        let retirement = reported.work().retirement_observer();
        let (role, authority) = ExitRole::from_admitted(reported);
        let shared = Arc::new(Custody {
            task,
            scope,
            role,
            work,
            report,
            retirement,
            exits: Mutex::new(Exits {
                authority: Some(authority),
                ..Exits::default()
            }),
            pending,
            retirement_notices: Mutex::new(RetirementNotices::default()),
            health,
            retained: Mutex::new(None),
            cleanup: Mutex::new(None),
        });
        *shared
            .retained
            .lock()
            .expect("source original custody retention") = Some(shared.clone());
        let custody = Self(shared);
        let permit = SourcePermit {
            custody: custody.clone(),
            active: true,
        };
        (custody, permit)
    }
    /// Immutable scheduling identity of this actual admitted Source role.
    /// It neither signs exit nor grants another payload/Work permission.
    pub(crate) fn queue_key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        (self.0.scope.id(), self.0.work)
    }
    pub(crate) fn report(&self) -> &ReportSlot {
        &self.0.report
    }
    pub(crate) fn phase(&self) -> ExitRolePhase {
        self.0.role.phase()
    }
    pub(crate) fn actual_exit_ready(&self) -> Result<bool, ExitRoleError> {
        self.0.role.actual_exit_ready()
    }
    /// The exact bank lookup selected this source. Its original role supplies
    /// physical exit; the cleanup effect cannot supply that evidence itself.
    pub(crate) fn adopt_cleanup(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        cleanup: AdoptedCleanupEffect,
    ) -> Result<(), (DriverError, AdoptedCleanupEffect)> {
        if cleanup.is_completed()
            || !matches!(cleanup.effect(), WorkerAction::Context(
                novarocks_worker_protocol::context::ContextEffect::DisposeSource { task, .. }
            ) if *task == self.0.task)
        {
            return Err((DriverError::PublicationTargetMismatch, cleanup));
        }
        if let Err(error) = cleanup.validate_scope(stage, &self.0.scope) {
            return Err((error, cleanup));
        }
        let mut slot = self
            .0
            .cleanup
            .lock()
            .expect("original source cleanup effect");
        if slot.is_some() {
            return Err((DriverError::RoleBusy, cleanup));
        }
        *slot = Some(cleanup);
        drop(slot);
        // The physical exit may already have happened before this transfer.
        // Revisit the original owner once; waiting alone does not self-wake.
        self.notify_pending();
        Ok(())
    }
    /// Run before waiting for the old input's effect retirement. The same
    /// source holder AND retained graph must have positively exited first.
    pub(crate) fn advance_cleanup(&self) -> Result<bool, SourceCleanupError> {
        if !self.actual_exit_ready().map_err(SourceCleanupError::Exit)? {
            return Ok(false);
        }
        let progressed = self
            .0
            .cleanup
            .lock()
            .expect("original source cleanup effect")
            .as_mut()
            .map_or(Ok(false), |cleanup| cleanup.complete())
            .map_err(SourceCleanupError::Driver)?;
        if self.phase() == ExitRolePhase::Retired && self.cleanup_completed() {
            let retained = self
                .0
                .retained
                .lock()
                .expect("source original custody retention")
                .take();
            drop(retained);
        }
        Ok(progressed)
    }
    fn cleanup_completed(&self) -> bool {
        self.0
            .cleanup
            .lock()
            .expect("original source cleanup effect")
            .as_ref()
            .is_none_or(AdoptedCleanupEffect::is_completed)
    }
    /// Body/report/capacity retirement alone does not free the original pure
    /// Work header. Only the common reaper's exact observation permits reuse.
    /// This query signs no new permit and acknowledges no consumer.
    pub(crate) fn reusable(&self) -> bool {
        let unresolved = self
            .0
            .exits
            .lock()
            .expect("source capture exits")
            .unresolved;
        !unresolved
            && self.phase() == ExitRolePhase::Retired
            && self.0.retirement.is_retired()
            && self.cleanup_completed()
    }
    /// Read only original capture/role metadata. A claimed position with a
    /// pending inventory fault cannot be published Ready before its host has
    /// drained that fault into the common runtime outside the cut lock.
    pub(crate) fn retained_source_ready(&self) -> bool {
        let unresolved = self
            .0
            .exits
            .lock()
            .expect("source capture exits")
            .unresolved;
        !unresolved && self.phase() == ExitRolePhase::Started
    }
    /// An unclaimed paid holder can move to another segment without issuing
    /// new authority. An inventory fault makes that original holder unusable
    /// immediately, even before its host drains the fault into the ledger.
    pub(crate) fn unused_source_ready(&self) -> bool {
        let ready = {
            let exits = self.0.exits.lock().expect("source capture exits");
            !exits.bound && !exits.unresolved && exits.authority.is_some()
        };
        ready && self.phase() == ExitRolePhase::Admitted
    }
    /// Unexpected inventory loss only records metadata here. A slot may be
    /// returned on an unwinding short-lock path, so its actual authority stays
    /// in the original custodian until the host drains the failure outside it.
    pub(crate) fn note_inventory_abandoned(&self) {
        self.abandon_claimed();
    }
    /// The real host independently acknowledges every report consumer. The
    /// issuing owner keeps this custodian until `reusable`, even after this
    /// method has retired the original role/report/capacity custody.
    pub(crate) fn try_retire(&self) -> Result<bool, ExitRoleError> {
        self.observe_pending_failure();
        if self
            .0
            .exits
            .lock()
            .expect("source capture exits")
            .unresolved
        {
            return Ok(false);
        }
        let retired = self.phase() == ExitRolePhase::Retired || self.0.role.try_retire()?;
        let complete = retired && self.cleanup_completed();
        if complete {
            let retained = self
                .0
                .retained
                .lock()
                .expect("source original custody retention")
                .take();
            drop(retained);
        }
        Ok(complete)
    }
    /// Classifies only the original Core report leg. Source holders, aliases
    /// and their unique authority continue to supply physical exit themselves.
    pub(crate) fn observe_failed_consumers(
        &self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        runtime: &novarocks_control_runtime::driver::ActorRuntime<super::actors::WorkerActor>,
        actor: novarocks_control_runtime_core::identity::ActorId,
        original_generation: novarocks_control_runtime_core::identity::Generation,
        core_receipt: novarocks_control_runtime_core::completion::Consumption,
    ) -> Result<bool, ExitRoleError> {
        use novarocks_control_runtime_core::{completion::Consumption, identity::ConsumerId};
        stage.validate(runtime, actor).map_err(|_| {
            ExitRoleError::Runtime(
                novarocks_control_runtime::transaction::RuntimeError::UnknownScope,
            )
        })?;
        if self.phase() == ExitRolePhase::Retired {
            return Ok(false);
        }
        let mut progressed = false;
        let core = ConsumerId::new(1).unwrap();
        if core_receipt == Consumption::AbandonedByOwnerFailure
            && self.report().consumption(core)? == Consumption::Pending
        {
            let original = stage.registration();
            if original.initial_epoch() != original_generation {
                return Err(ExitRoleError::Runtime(
                    novarocks_control_runtime::transaction::RuntimeError::UnknownScope,
                ));
            }
            let failure = runtime.failure_proof_for(original).map_err(|_| {
                ExitRoleError::Runtime(
                    novarocks_control_runtime::transaction::RuntimeError::UnknownScope,
                )
            })?;
            let consumer = self
                .report()
                .bind_actor_consumer(runtime, original, core)
                .map_err(|_| {
                    ExitRoleError::Runtime(
                        novarocks_control_runtime::transaction::RuntimeError::UnknownScope,
                    )
                })?;
            consumer.abandon(&failure).map_err(|_| {
                ExitRoleError::Runtime(
                    novarocks_control_runtime::transaction::RuntimeError::UnknownScope,
                )
            })?;
            progressed = true;
        }
        if self.actual_exit_ready()? {
            for consumer in [ConsumerId::new(2).unwrap(), core] {
                if consumer == core && core_receipt != Consumption::Consumed {
                    continue;
                }
                if self.report().consumption(consumer)? == Consumption::Pending {
                    self.report().consume(consumer)?;
                    progressed = true;
                }
            }
        }
        let before = self.phase();
        self.try_retire()?;
        Ok(progressed || before != self.phase())
    }
    fn report_unresolved(&self) {
        let source_phase = self
            .0
            .exits
            .lock()
            .expect("source capture exits")
            .source_phase;
        let origin = source_phase.map_or(ExecutionExitOrigin::Process, |phase| {
            ExecutionExitOrigin::Task {
                identity: self.0.task,
                phase,
            }
        });
        self.0.health.report_unresolved(
            origin,
            FailureFact::new(
                FailureClass::Invariant,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
            ),
        );
        self.notify_pending();
    }
    fn finish_authority(&self, authority: ExitAuthority) {
        self.observe_authority_exit(authority.exited());
    }
    fn observe_authority_exit(&self, result: Result<(), ExitRoleError>) {
        if result.is_err() {
            self.0
                .exits
                .lock()
                .expect("source capture exits")
                .unresolved = true;
            self.report_unresolved();
        } else {
            self.notify_pending();
        }
    }
    /// Drain only on the host side after releasing its cut/ingress lock. A
    /// claimed permit can be abandoned while that lock is held, so its Drop
    /// records local metadata only and retains the original authority intact.
    /// This transfers no source and acknowledges no report consumer.
    pub(crate) fn observe_pending_failure(&self) -> bool {
        let authority = {
            let mut exits = self.0.exits.lock().expect("source capture exits");
            if !exits.failure_pending {
                return false;
            }
            exits.failure_pending = false;
            exits.authority.take()
        };
        drop(authority);
        self.report_unresolved();
        true
    }
    fn claim(&self, phase: TaskFailurePhase) {
        let valid = {
            let mut exits = self.0.exits.lock().expect("source capture exits");
            if exits.unresolved || (exits.bound && exits.source_phase != Some(phase)) {
                exits.unresolved = true;
                exits.failure_pending = true;
                false
            } else {
                exits.bound = true;
                exits.source_phase = Some(phase);
                true
            }
        };
        // Do not poison our own capture mutex on a caller contract breach:
        // the original failed-owner inventory must still observe its duty.
        assert!(
            valid,
            "one original source claim preserves its reporting phase"
        );
    }
    fn abandon_claimed(&self) {
        let mut exits = self.0.exits.lock().expect("source capture exits");
        exits.unresolved = true;
        exits.failure_pending = true;
        // Keep authority here. Dropping it would touch the common ledger,
        // which is forbidden while a caller may hold its short cut lock.
    }
    fn start_claimed(&self) {
        let mut authority = {
            let mut exits = self.0.exits.lock().expect("source capture exits");
            assert!(
                exits.bound,
                "source start follows its original metadata claim"
            );
            exits.authority.take().expect("original source authority")
        };
        // Actual source capture starts here, not during the short-lock claim.
        // The caller must have released every ingress/cut metadata lock.
        if authority.start().is_err() {
            self.0
                .exits
                .lock()
                .expect("source capture exits")
                .unresolved = true;
            drop(authority);
            self.report_unresolved();
        } else {
            self.0.exits.lock().expect("source capture exits").authority = Some(authority);
        }
    }
    fn capture_exited(&self, leg: Leg, unwinding: bool) {
        let (authority, unresolved) = {
            let mut exits = self.0.exits.lock().expect("source capture exits");
            assert!(exits.bound, "only a bound source has capture markers");
            if unwinding {
                exits.unresolved = true;
            } else {
                match leg {
                    Leg::Holder => exits.holder = true,
                    Leg::SourceNode => exits.source_node = true,
                }
            }
            let authority = if exits.unresolved || (exits.holder && exits.source_node) {
                exits.authority.take()
            } else {
                None
            };
            (authority, exits.unresolved)
        };
        if unresolved {
            // Drop marks the same original Work unresolved. Neither the other
            // marker nor a later normal drop may turn it into positive exit.
            drop(authority);
            self.report_unresolved();
        } else if let Some(authority) = authority {
            self.finish_authority(authority);
        } else {
            self.notify_pending();
        }
    }
    fn unused_exited(&self) {
        let (authority, unresolved) = {
            let mut exits = self.0.exits.lock().expect("source capture exits");
            assert!(!exits.bound, "a captured source is not unused");
            // An inventory which already became unknown cannot later turn
            // into positive unused exit. Its authority may have been drained
            // by the real host; an empty field is not a new exit receipt.
            (exits.authority.take(), exits.unresolved)
        };
        if unresolved {
            drop(authority);
            self.report_unresolved();
        } else {
            self.observe_authority_exit(
                authority
                    .expect("one original unused source holder")
                    .revoke_unused_and_exit(),
            );
        }
    }
}
impl SourcePermit {
    /// A short-lock handoff claims this same prepaid position. Only capture
    /// metadata changes: no ledger, foreign call, health or ready notification.
    /// Repeating the same claim grants no additional authority; a different
    /// phase is a sticky failure of this original position.
    pub(crate) fn claim(&mut self, phase: TaskFailurePhase) {
        self.custody.claim(phase);
    }
    /// Preserve the source's actual reporting boundary as exit metadata. This
    /// phase never reprojects or changes its original cause or failure facts.
    pub(crate) fn retain(
        mut self,
        original: ExecutionFailure,
        phase: TaskFailurePhase,
    ) -> RetainedTaskSource<ExecutionFailure> {
        self.claim(phase);
        self.active = false;
        self.custody.start_claimed();
        // Attach to the existing shared occurrence, including clones created
        // before retain. The original cause identity and typed facts stay intact.
        original.retain_source_lifetime(Box::new(Marker {
            custody: self.custody.clone(),
            leg: Leg::SourceNode,
        }));
        RetainedTaskSource::new(
            original,
            Box::new(Marker {
                custody: self.custody.clone(),
                leg: Leg::Holder,
            }),
        )
    }
}
impl Drop for SourcePermit {
    fn drop(&mut self) {
        if self.active {
            let claimed = self
                .custody
                .0
                .exits
                .lock()
                .expect("source capture exits")
                .bound;
            if claimed {
                self.custody.abandon_claimed();
            } else {
                self.custody.unused_exited();
            }
        }
    }
}
impl Drop for Marker {
    fn drop(&mut self) {
        self.custody
            .capture_exited(self.leg, std::thread::panicking());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_control_runtime::{
        lease::{ReservationBundle, WorkLease},
        transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
    };
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        completion::{CompletionError, ConsumerKind},
        identity::{ConsumerId, DimensionId, Generation},
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_execution_contract::{
        AcceptedConclusion, AcceptedTaskConclusion, TaskConclusion, TaskFailure, TerminationDetail,
        failure::FailureProjection,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::{
        mem::ManuallyDrop,
        num::NonZeroU64,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Weak,
            atomic::{AtomicUsize, Ordering},
        },
    };

    struct Pending(AtomicUsize);
    impl TaskPendingNotice for Pending {
        fn pending(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Fixture {
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        parent: WorkLease,
        funding: ReservationBundle,
        custody: SourceCustody,
        permit: Option<SourcePermit>,
        health: Arc<ExecutionHealth>,
        pending: Arc<Pending>,
    }
    fn observer() -> ConsumerId {
        ConsumerId::new(2).unwrap()
    }
    fn dimension() -> DimensionId {
        DimensionId::new(1).unwrap()
    }
    fn fixture() -> Fixture {
        let generation = Generation::new(1).unwrap();
        let resources = ResourceVector::new([(dimension(), 1)]).unwrap();
        let ledger = RuntimeLedger::new(resources.clone());
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut parent, funding) = scope
            .reserve_next(
                generation,
                novarocks_control_runtime_core::capacity::ReservationResources::for_work(resources),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        parent.start().unwrap();
        let mut guard = scope
            .reserve_child_next(
                generation,
                funding.id(),
                novarocks_control_runtime_core::capacity::ReservationResources::new(
                    ResourceVector::default(),
                    novarocks_control_runtime_core::owned_storage::OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..novarocks_control_runtime_core::owned_storage::OwnedSlotNeed::default()
                    },
                ),
                WorkKind::Cleanup,
            )
            .unwrap();
        let report = ReportSlot::reserve_next(
            &mut guard,
            generation,
            1,
            [(observer(), ConsumerKind::Observer)],
        )
        .unwrap();
        let (work, capacity) = guard.bind(WorkKind::Cleanup).unwrap();
        let task = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(931, 932), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let health = Arc::new(ExecutionHealth::default());
        let pending = Arc::new(Pending(AtomicUsize::new(0)));
        let (custody, permit) = SourceCustody::from_admitted(
            task,
            AdmittedCompletionRole {
                work,
                capacity,
                report,
            },
            pending.clone(),
            health.clone(),
        );
        // A real preparation body can finish while its original source child
        // remains alive. Source exit must not be inferred from parent return.
        parent.body_exited().unwrap();
        parent.captures_exited().unwrap();
        Fixture {
            ledger,
            scope,
            parent,
            funding,
            custody,
            permit: Some(permit),
            health,
            pending,
        }
    }
    impl Fixture {
        fn accept(
            &mut self,
            source: ExecutionFailure,
            phase: TaskFailurePhase,
        ) -> Arc<AcceptedTaskConclusion<ExecutionFailure>> {
            let fact = source.failure_fact();
            let detail = source.safe_detail();
            let retained = self.permit.take().unwrap().retain(source, phase);
            Arc::new(
                AcceptedTaskConclusion::from_retained_source(
                    AcceptedConclusion::new(
                        self.custody.0.task,
                        NonZeroU64::new(1).unwrap(),
                        TaskConclusion::Terminated(TerminationDetail::Failed(
                            TaskFailure::new_in_phase(fact, detail, phase),
                        )),
                    ),
                    retained,
                )
                .unwrap(),
            )
        }
        fn finish(mut self) {
            assert!(self.permit.is_none());
            assert_eq!(self.custody.phase(), ExitRolePhase::Exited);
            assert!(self.custody.actual_exit_ready().unwrap());
            assert!(!self.custody.reusable());
            assert!(
                !self.parent.actual_exit_ready().unwrap(),
                "a positive source report does not retire the original child header"
            );
            let bound = self.custody.0.exits.lock().unwrap().bound;
            if bound {
                assert!(
                    !self.custody.try_retire().unwrap(),
                    "actual host observer has not consumed its report"
                );
                self.custody.report().consume(observer()).unwrap();
            } else {
                assert_eq!(
                    self.custody.report().consume(observer()),
                    Err(RuntimeError::Completion(CompletionError::NotFilled)),
                    "an unused source revokes its report without inventing consumption"
                );
            }
            assert!(self.custody.try_retire().unwrap());
            assert_eq!(self.custody.phase(), ExitRolePhase::Retired);
            assert!(
                !self.custody.reusable(),
                "ExitRole/capacity retirement is not common Work header retirement"
            );
            assert_eq!(self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap(), 1);
            assert!(self.custody.reusable());
            assert!(self.parent.actual_exit_ready().unwrap());
            assert_eq!(
                self.ledger.used(dimension()),
                1,
                "parent funding remains until its own release"
            );
            assert_eq!(self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap(), 1);
            self.funding
                .release_quota(&self.funding.resources().unwrap().quota)
                .unwrap();
            self.funding.finish().unwrap();
            self.scope.close_external().unwrap();
            self.ledger.remove_scope(self.scope.id()).unwrap();
            assert!(self.ledger.is_empty());
            assert!(self.health.failure().is_none());
        }
    }
    #[derive(Debug)]
    struct DomainSource {
        dropped: Arc<AtomicUsize>,
        custody: Weak<Custody>,
        panic: bool,
    }
    impl std::fmt::Display for DomainSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original supervised source")
        }
    }
    impl std::error::Error for DomainSource {}
    impl FailureProjection for DomainSource {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Data,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
            )
        }
    }
    impl Drop for DomainSource {
        fn drop(&mut self) {
            let custody = self
                .custody
                .upgrade()
                .expect("the original source role remains held");
            assert!(
                !custody.exits.lock().unwrap().source_node,
                "source-node marker cannot precede the real domain source destructor"
            );
            assert_ne!(custody.role.phase(), ExitRolePhase::Exited);
            self.dropped.fetch_add(1, Ordering::SeqCst);
            assert!(
                !self.panic,
                "injected original domain source destructor panic"
            );
        }
    }
    fn source(f: &Fixture, panic: bool) -> (ExecutionFailure, Arc<AtomicUsize>) {
        let dropped = Arc::new(AtomicUsize::new(0));
        (
            ExecutionFailure::from_domain(DomainSource {
                dropped: dropped.clone(),
                custody: Arc::downgrade(&f.custody.0),
                panic,
            }),
            dropped,
        )
    }
    #[test]
    fn the_last_accepted_arc_exits_both_real_capture_legs_without_consuming_its_report() {
        let mut f = fixture();
        let (source, dropped) = source(&f, false);
        let original_cause = source.cause() as *const _;
        let original_fact = source.failure_fact();
        let accepted = f.accept(source, TaskFailurePhase::Execution);
        assert_eq!(
            accepted.originating_failure().unwrap().cause() as *const _,
            original_cause
        );
        assert_eq!(
            accepted.originating_failure().unwrap().failure_fact(),
            original_fact
        );
        let last = accepted.clone();
        drop(accepted);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert_eq!(f.custody.phase(), ExitRolePhase::Started);
        assert!(!f.custody.actual_exit_ready().unwrap());
        drop(last);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        let exits = f.custody.0.exits.lock().unwrap();
        assert!(exits.holder && exits.source_node && !exits.unresolved);
        drop(exits);
        assert!(f.pending.0.load(Ordering::SeqCst) >= 2);
        f.finish();
    }
    #[test]
    fn an_alias_created_before_retain_keeps_the_original_source_node_and_preparation_child_counted()
    {
        let mut f = fixture();
        let (original, dropped) = source(&f, false);
        let old_alias = original.clone();
        let accepted = f.accept(original, TaskFailurePhase::Execution);
        drop(accepted);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert!(f.custody.0.exits.lock().unwrap().holder);
        assert!(!f.custody.0.exits.lock().unwrap().source_node);
        assert_eq!(f.custody.phase(), ExitRolePhase::Started);
        assert!(!f.custody.actual_exit_ready().unwrap());
        assert!(!f.parent.actual_exit_ready().unwrap());
        assert!(!f.custody.try_retire().unwrap());
        assert!(!f.custody.reusable());
        assert!(
            f.custody.report().consume(observer()).is_err(),
            "holder-only exit has not filled the source report"
        );
        assert_eq!(f.ledger.used(dimension()), 1);
        drop(old_alias);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        f.finish();
    }
    #[test]
    fn a_shared_inner_domain_alias_keeps_the_original_source_and_work_header_counted() {
        use novarocks_execution::runtime::fragment::ExecutionFailureCause;

        for alias_before_retain in [false, true] {
            let mut f = fixture();
            let (original, dropped) = source(&f, false);
            let original_fact = original.failure_fact();
            let early_alias = alias_before_retain.then(|| match original.cause() {
                ExecutionFailureCause::Domain(inner) => inner.clone(),
                _ => panic!("the original fixture is a domain source"),
            });
            let accepted = f.accept(original, TaskFailurePhase::Preparation);
            let inner = early_alias.unwrap_or_else(|| {
                match accepted.originating_failure().unwrap().cause() {
                    ExecutionFailureCause::Domain(inner) => inner.clone(),
                    _ => panic!("accepted retains the original domain source"),
                }
            });
            let original_pointer = inner.downcast_ref::<DomainSource>().unwrap() as *const _;
            assert_eq!(inner.failure_fact(), original_fact);
            drop(accepted);
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
            let exits = f.custody.0.exits.lock().unwrap();
            assert!(exits.holder && !exits.source_node && !exits.unresolved);
            drop(exits);
            assert_eq!(f.custody.phase(), ExitRolePhase::Started);
            assert!(!f.custody.actual_exit_ready().unwrap());
            assert!(f.custody.report().consume(observer()).is_err());
            assert!(!f.custody.try_retire().unwrap());
            assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 0);
            assert!(!f.custody.reusable());
            assert!(!f.parent.actual_exit_ready().unwrap());
            assert_eq!(
                inner.downcast_ref::<DomainSource>().unwrap() as *const _,
                original_pointer
            );
            drop(inner);
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            f.finish();
        }
    }
    #[test]
    fn a_bound_source_is_not_ready_after_inventory_loss_before_ledger_observation() {
        let mut f = fixture();
        let (original, dropped) = source(&f, false);
        let retained = f
            .permit
            .take()
            .unwrap()
            .retain(original, TaskFailurePhase::Preparation);
        assert!(f.custody.retained_source_ready());
        let cut = Mutex::new(());
        let version = f.ledger.change_version();
        {
            let _cut = cut.lock().unwrap();
            f.custody.note_inventory_abandoned();
            assert_eq!(f.custody.phase(), ExitRolePhase::Started);
            assert!(!f.custody.retained_source_ready());
            assert_eq!(f.ledger.change_version(), version);
            assert!(f.health.failure().is_none());
        }
        assert!(f.custody.observe_pending_failure());
        assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
        drop(retained);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(!f.custody.try_retire().unwrap());
        assert!(!f.custody.reusable());
    }

    #[test]
    fn an_abandoned_unused_inventory_cannot_later_become_positive_exit() {
        for drain_before_holder_drop in [false, true] {
            let mut f = fixture();
            assert!(f.custody.unused_source_ready());
            let cut = Mutex::new(());
            {
                let _cut = cut.lock().unwrap();
                f.custody.note_inventory_abandoned();
                // Only local capture metadata changes under the real short
                // lock. Work/report and notification authority stay original.
                assert_eq!(f.custody.phase(), ExitRolePhase::Admitted);
                assert!(!f.custody.unused_source_ready());
                assert!(f.health.failure().is_none());
                assert_eq!(f.pending.0.load(Ordering::SeqCst), 0);
            }
            if drain_before_holder_drop {
                assert!(f.custody.observe_pending_failure());
            }
            drop(f.permit.take());
            f.custody.observe_pending_failure();
            assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
            assert!(!f.custody.actual_exit_ready().unwrap());
            assert!(!f.custody.try_retire().unwrap());
            assert!(!f.custody.reusable());
            assert!(f.health.failure().is_some());
        }
    }

    #[test]
    fn an_unused_permit_exits_only_its_original_unbound_holder_without_inventing_source_node_exit()
    {
        let mut f = fixture();
        drop(f.permit.take());
        let exits = f.custody.0.exits.lock().unwrap();
        assert!(!exits.bound && !exits.holder && !exits.source_node && !exits.unresolved);
        assert_eq!(
            exits.source_phase, None,
            "an unused role has no source phase"
        );
        drop(exits);
        f.finish();
    }
    #[test]
    fn a_source_destructor_panic_never_fills_its_exit_report_or_releases_the_parent() {
        // Expected fail-stop custody keeps the same original parent/funding,
        // child/report, and this one actual panic payload. Nothing is retried.
        let mut f = ManuallyDrop::new(fixture());
        let (source, dropped) = source(&f, true);
        let accepted = f.accept(source, TaskFailurePhase::Execution);
        let payload = catch_unwind(AssertUnwindSafe(|| drop(accepted))).unwrap_err();
        let _retained_panic = ManuallyDrop::new(payload);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
        assert!(!f.custody.actual_exit_ready().unwrap());
        assert!(!f.custody.try_retire().unwrap());
        assert!(!f.custody.reusable());
        assert!(f.custody.report().consume(observer()).is_err());
        assert!(!f.parent.actual_exit_ready().unwrap());
        assert_eq!(f.ledger.used(dimension()), 1);
        let failure = f.health.failure().unwrap();
        assert_eq!(
            failure.origin(),
            ExecutionExitOrigin::Task {
                identity: f.custody.0.task,
                phase: TaskFailurePhase::Execution
            }
        );
    }
    #[test]
    fn a_preparation_source_unknown_keeps_its_original_phase_identity_and_facts() {
        let mut f = ManuallyDrop::new(fixture());
        let (original, dropped) = source(&f, true);
        let original_cause = original.cause() as *const _;
        let original_fact = original.failure_fact();
        let accepted = f.accept(original, TaskFailurePhase::Preparation);
        assert_eq!(
            accepted.originating_failure().unwrap().cause() as *const _,
            original_cause
        );
        assert_eq!(
            accepted.originating_failure().unwrap().failure_fact(),
            original_fact
        );
        assert_eq!(
            f.custody.0.exits.lock().unwrap().source_phase,
            Some(TaskFailurePhase::Preparation)
        );
        let payload = catch_unwind(AssertUnwindSafe(|| drop(accepted))).unwrap_err();
        let _retained_panic = ManuallyDrop::new(payload);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
        assert!(!f.custody.try_retire().unwrap());
        assert!(f.custody.report().consume(observer()).is_err());
        assert!(!f.parent.actual_exit_ready().unwrap());
        assert_eq!(f.ledger.used(dimension()), 1);
        assert_eq!(
            f.health.failure().unwrap().origin(),
            ExecutionExitOrigin::Task {
                identity: f.custody.0.task,
                phase: TaskFailurePhase::Preparation,
            }
        );
    }
    #[test]
    fn a_later_graph_panic_cannot_turn_an_already_exited_holder_into_positive_source_exit() {
        let mut f = ManuallyDrop::new(fixture());
        let (original, dropped) = source(&f, true);
        let old_alias = original.clone();
        drop(f.accept(original, TaskFailurePhase::Execution));
        assert!(f.custody.0.exits.lock().unwrap().holder);
        assert!(!f.custody.0.exits.lock().unwrap().source_node);
        let payload = catch_unwind(AssertUnwindSafe(|| drop(old_alias))).unwrap_err();
        let _retained_panic = ManuallyDrop::new(payload);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
        assert!(!f.custody.try_retire().unwrap());
        assert!(f.custody.report().consume(observer()).is_err());
        assert_eq!(f.ledger.used(dimension()), 1);
    }
    #[test]
    fn unwinding_holder_exit_remains_unknown_even_if_the_graph_later_exits_normally() {
        let mut f = ManuallyDrop::new(fixture());
        let (original, dropped) = source(&f, false);
        let old_alias = original.clone();
        let accepted = f.accept(original, TaskFailurePhase::Execution);
        let payload = catch_unwind(AssertUnwindSafe(move || {
            let _held = accepted;
            panic!("injected source holder unwind");
        }))
        .unwrap_err();
        let _retained_panic = ManuallyDrop::new(payload);
        assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
        drop(old_alias);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(!f.custody.0.exits.lock().unwrap().holder);
        assert!(f.custody.0.exits.lock().unwrap().source_node);
        assert!(!f.custody.try_retire().unwrap());
        assert!(!f.custody.reusable());
        assert!(f.custody.report().consume(observer()).is_err());
    }
    #[test]
    fn a_short_lock_claim_defers_runtime_start_until_the_original_source_is_retained() {
        let mut f = fixture();
        let (original, dropped) = source(&f, false);
        let cut = Mutex::new(());
        let version = f.ledger.change_version();
        {
            let _cut = cut.lock().unwrap();
            f.permit
                .as_mut()
                .unwrap()
                .claim(TaskFailurePhase::Preparation);
        }
        assert_eq!(f.ledger.change_version(), version);
        assert_eq!(f.custody.phase(), ExitRolePhase::Admitted);
        assert!(f.health.failure().is_none());
        assert_eq!(f.pending.0.load(Ordering::SeqCst), 0);
        let accepted = f.accept(original, TaskFailurePhase::Preparation);
        assert_eq!(f.custody.phase(), ExitRolePhase::Started);
        drop(accepted);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        f.finish();
    }
    #[test]
    fn dropping_a_claimed_permit_under_a_cut_lock_only_records_original_pending_failure() {
        let mut f = ManuallyDrop::new(fixture());
        // The real PendingSource inventory keeps this original, not a made-up
        // replacement source. No graph marker was attached before abandonment.
        let (original, dropped) = source(&f, false);
        let _pending_original = ManuallyDrop::new(original);
        let cut = Mutex::new(());
        let version = f.ledger.change_version();
        {
            let _cut = cut.lock().unwrap();
            let mut permit = f.permit.take().unwrap();
            permit.claim(TaskFailurePhase::Preparation);
            drop(permit);
        }
        assert_eq!(
            f.ledger.change_version(),
            version,
            "claim/Drop must not touch the common ledger"
        );
        assert_eq!(f.custody.phase(), ExitRolePhase::Admitted);
        assert!(f.ledger.unresolved_work().is_empty());
        assert!(f.custody.0.exits.lock().unwrap().unresolved);
        assert!(f.custody.0.exits.lock().unwrap().authority.is_some());
        assert!(f.health.failure().is_none());
        assert_eq!(f.pending.0.load(Ordering::SeqCst), 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert!(!f.custody.reusable());
        assert!(f.custody.observe_pending_failure());
        assert!(f.ledger.change_version() > version);
        assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
        assert_eq!(f.ledger.unresolved_work().len(), 1);
        assert!(f.custody.report().consume(observer()).is_err());
        assert!(!f.parent.actual_exit_ready().unwrap());
        assert_eq!(f.ledger.used(dimension()), 1);
        assert_eq!(
            f.health.failure().unwrap().origin(),
            ExecutionExitOrigin::Task {
                identity: f.custody.0.task,
                phase: TaskFailurePhase::Preparation,
            }
        );
        let observed_version = f.ledger.change_version();
        let wakes = f.pending.0.load(Ordering::SeqCst);
        assert!(!f.custody.observe_pending_failure());
        assert!(!f.custody.try_retire().unwrap());
        assert_eq!(f.ledger.change_version(), observed_version);
        assert_eq!(f.pending.0.load(Ordering::SeqCst), wakes);
    }
    #[test]
    fn a_claimed_permit_unwind_keeps_its_own_metadata_readable_until_lock_free_observation() {
        let mut f = ManuallyDrop::new(fixture());
        let (original, dropped) = source(&f, false);
        let _pending_original = ManuallyDrop::new(original);
        let cut = Mutex::new(());
        let version = f.ledger.change_version();
        let permit = f.permit.take().unwrap();
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _cut = cut.lock().unwrap();
            let mut permit = permit;
            permit.claim(TaskFailurePhase::Preparation);
            panic!("injected caller unwind before lock-free source binding");
        }))
        .unwrap_err();
        let _retained_panic = ManuallyDrop::new(payload);
        assert!(
            cut.is_poisoned(),
            "the original claim really unwound while holding its cut lock"
        );
        assert_eq!(f.ledger.change_version(), version);
        assert!(f.ledger.unresolved_work().is_empty());
        assert_eq!(f.custody.phase(), ExitRolePhase::Admitted);
        assert!(f.custody.0.exits.lock().unwrap().failure_pending);
        assert!(f.health.failure().is_none());
        assert_eq!(f.pending.0.load(Ordering::SeqCst), 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        // try_retire may drain the same pending metadata only after unlocking;
        // it cannot turn the unknown source into a positive completion.
        assert!(!f.custody.try_retire().unwrap());
        assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
        assert_eq!(f.ledger.unresolved_work().len(), 1);
        assert!(f.custody.report().consume(observer()).is_err());
        assert!(!f.custody.reusable());
        assert_eq!(f.ledger.used(dimension()), 1);
        assert_eq!(
            f.health.failure().unwrap().origin(),
            ExecutionExitOrigin::Task {
                identity: f.custody.0.task,
                phase: TaskFailurePhase::Preparation,
            }
        );
    }
    #[test]
    fn a_claimed_phase_cannot_be_replaced_by_a_different_retain_phase() {
        let mut f = ManuallyDrop::new(fixture());
        let (original, dropped) = source(&f, false);
        let mut permit = f.permit.take().unwrap();
        permit.claim(TaskFailurePhase::Preparation);
        let version = f.ledger.change_version();
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _retained = permit.retain(original, TaskFailurePhase::Execution);
        }))
        .unwrap_err();
        let _retained_panic = ManuallyDrop::new(payload);
        assert_eq!(f.ledger.change_version(), version);
        assert_eq!(
            f.custody.0.exits.lock().unwrap().source_phase,
            Some(TaskFailurePhase::Preparation)
        );
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(f.custody.observe_pending_failure());
        assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
        assert!(f.custody.report().consume(observer()).is_err());
        assert!(!f.custody.reusable());
        assert_eq!(
            f.health.failure().unwrap().origin(),
            ExecutionExitOrigin::Task {
                identity: f.custody.0.task,
                phase: TaskFailurePhase::Preparation,
            }
        );
    }
}
