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

//! Original fragment roles consume their actual pre-admitted work. A Task
//! port clone cannot issue these roles or manufacture settlement authority.

use crate::{
    control_runtime_policy::TaskLifecycleInputKind,
    settlement::{RegistrationNotice, SettlementCustody, SettlementPhase},
    task_port::TaskPendingNotice,
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyPhase, ExitObligation},
    obligation::{ReservationObligation, WorkObligation},
};
use novarocks_execution::runtime::{
    execution_health::{ExecutionExitOrigin, ExecutionHealth},
    task::ExecutionFailure,
};
use novarocks_execution_contract::{
    AdmittedFinisher, AdmittedWorkParts, CompletionWorkCapability, TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact},
};
use std::sync::{Arc, Mutex};

/// Supplied once by the actual Task admission/preparation owner. Capacity may
/// be a role's own portion or a zero-resource child of an aggregate prepaid
/// Task bundle, whose owner retains funding until every child/report retires.
pub(crate) struct AdmittedCompletionTransfer {
    pub work: WorkObligation,
    pub capacity: ReservationObligation,
    pub report: ReportSlot,
}
struct Original {
    exit_custody: ExitCustody,
    report: ReportSlot,
}
struct Work {
    identity: TaskIdentity,
    authority: Option<ExitObligation>,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
}
impl Work {
    fn exited(mut self) {
        self.authority
            .take()
            .expect("unique original role exit authority")
            .exited()
            .expect("original fragment role exit metadata");
        self.pending.pending();
    }
}
impl CompletionWorkCapability for Work {
    fn started(&mut self) {
        self.authority
            .as_mut()
            .expect("original role authority")
            .start()
            .expect("pre-admitted original fragment role starts");
    }
    fn completed(self: Box<Self>) {
        self.exited();
    }
    fn unused_exited(self: Box<Self>) {
        self.exited();
    }
    fn unresolved(mut self: Box<Self>) {
        drop(self.authority.take());
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.identity,
                phase: TaskFailurePhase::Execution,
            },
            FailureFact::new(
                FailureClass::Invariant,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
            ),
        );
        self.pending.pending();
    }
}

/// The scope's real role inventory. These are references to actual runtime
/// custody, not a second work ledger, conclusion owner, or inferred exit table.
pub(crate) struct TaskCompletionSet {
    originals: [Original; 3],
    settlement: SettlementCustody,
    report: ReportSlot,
    pending: Arc<dyn TaskPendingNotice>,
    unused_observation: Mutex<()>,
}
impl TaskCompletionSet {
    /// All four roles and report positions already exist. The settlement work
    /// is a same-scope derived child of the original finisher; the caller keeps
    /// the aggregate Task funding and transfers this single set before building
    /// the dormant fragment. No capacity acquisition happens on registration.
    pub(crate) fn from_admitted(
        identity: TaskIdentity,
        originals: [AdmittedCompletionTransfer; 3],
        child: AdmittedCompletionTransfer,
        registration: Box<dyn RegistrationNotice>,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> (Self, AdmittedWorkParts<ExecutionFailure>) {
        let scope = child.work.scope().id();
        assert!(
            originals
                .iter()
                .all(|completion_transfer| completion_transfer.work.scope().id() == scope),
            "one context scope owns original completion roles"
        );
        let report = child.report.clone();
        let (settlement, permit) = SettlementCustody::from_admitted(
            identity,
            child.work,
            child.capacity,
            child.report,
            health.clone(),
            registration,
        );
        let bind = |input: AdmittedCompletionTransfer| {
            let report = input.report.clone();
            let (exit_custody, authority) = ExitCustody::from_admitted(
                crate::control_runtime::reported_work_admission::adopt_reported_work(
                    input.work,
                    input.capacity,
                    input.report,
                ),
            );
            let capability = Box::new(Work {
                identity,
                authority: Some(authority),
                pending: pending.clone(),
                health: health.clone(),
            }) as Box<dyn CompletionWorkCapability>;
            (
                Original {
                    exit_custody,
                    report,
                },
                capability,
            )
        };
        let [finisher, wall_time, publisher] = originals;
        let (finisher, finisher_work) = bind(finisher);
        let (wall_time, wall_work) = bind(wall_time);
        let (publisher, publisher_work) = bind(publisher);
        let admitted = AdmittedWorkParts::from_admitted_parts(
            AdmittedFinisher::from_admitted_parts(permit, finisher_work),
            wall_work,
            publisher_work,
        );
        (
            Self {
                originals: [finisher, wall_time, publisher],
                settlement,
                report,
                pending,
                unused_observation: Mutex::new(()),
            },
            admitted,
        )
    }
    fn original(&self, input_kind: TaskLifecycleInputKind) -> &Original {
        &self.originals[match input_kind {
            TaskLifecycleInputKind::Finisher => 0,
            TaskLifecycleInputKind::WallTime => 1,
            TaskLifecycleInputKind::TerminalPublisher => 2,
            _ => panic!("not an original fragment completion role"),
        }]
    }
    pub(crate) fn report(&self, input_kind: TaskLifecycleInputKind) -> &ReportSlot {
        if matches!(input_kind, TaskLifecycleInputKind::Settlement) {
            &self.report
        } else {
            &self.original(input_kind).report
        }
    }
    pub(crate) fn phase(&self, input_kind: TaskLifecycleInputKind) -> ExitCustodyPhase {
        self.original(input_kind).exit_custody.phase()
    }
    pub(crate) fn settlement_phase(&self) -> SettlementPhase {
        self.settlement.phase()
    }
    pub(crate) fn settlement_work(
        &self,
    ) -> novarocks_control_runtime_core::identity::ScopedWorkRef {
        self.settlement.work_id()
    }
    #[cfg(test)]
    pub(crate) fn commit_settlement_barrier(
        &self,
        runtime: &novarocks_control_runtime::actor_turn::ActorRuntime<
            crate::control_runtime::actors::WorkerActor,
        >,
        ticket: &novarocks_control_runtime::actor_turn::EffectTicket<
            crate::control_runtime::actors::WorkerAction,
        >,
        port: &crate::task_port::TaskRuntimePort,
    ) -> Result<(), crate::settlement::SettlementDispatchError> {
        self.settlement.commit_barrier(runtime, ticket, port)
    }
    pub(crate) fn handoff_settlement_effect(
        &self,
        runtime: &novarocks_control_runtime::actor_turn::ActorRuntime<
            crate::control_runtime::actors::WorkerActor,
        >,
        dispatch: &mut crate::control_runtime::effect_dispatch::EffectDispatch,
        ticket: novarocks_control_runtime::actor_turn::EffectTicket<
            crate::control_runtime::actors::WorkerAction,
        >,
        port: &crate::task_port::TaskRuntimePort,
    ) -> Result<
        (),
        (
            crate::settlement::SettlementDispatchError,
            novarocks_control_runtime::actor_turn::EffectTicket<
                crate::control_runtime::actors::WorkerAction,
            >,
        ),
    > {
        self.settlement
            .handoff_original_barrier(runtime, dispatch, ticket, port)
    }
    pub(crate) fn finish_settlement_effect(
        &self,
    ) -> Result<(), crate::settlement::SettlementDispatchError> {
        self.settlement.complete_original_barrier()
    }
    pub(crate) fn settlement_barrier_committed(&self) -> bool {
        self.settlement.barrier_committed()
    }
    pub(crate) fn commit_failure_settlement_barrier(
        &self,
        session: &mut novarocks_control_runtime::actor_turn::FailurePublicationSession<
            crate::control_runtime::actors::WorkerActor,
        >,
        port: &crate::task_port::TaskRuntimePort,
    ) -> Result<(), crate::settlement::SettlementDispatchError> {
        self.settlement.commit_failure_barrier(session, port)
    }
    pub(crate) fn dispatch_settlement(
        &self,
        port: &crate::task_port::TaskRuntimePort,
    ) -> Result<crate::settlement::SettlementRun, crate::settlement::SettlementDispatchError> {
        let result = self.settlement.run(port)?;
        if result != crate::settlement::SettlementRun::Deferred {
            self.pending.pending();
        }
        Ok(result)
    }
    /// Claim this same admitted continuation before the metadata owner offers
    /// a physical job. Failure publication is not callback or capture exit.
    pub(crate) fn take_failed_settlement_dispatch(
        &self,
        stage: &crate::control_runtime::failure::ScopePublicationsDrained<'_>,
        runtime: &novarocks_control_runtime::actor_turn::ActorRuntime<
            crate::control_runtime::actors::WorkerActor,
        >,
        actor: novarocks_control_runtime_core::identity::ActorId,
        port: Arc<crate::task_port::TaskRuntimePort>,
    ) -> Result<
        Option<crate::settlement::FailedSettlementDispatch>,
        crate::settlement::SettlementDispatchError,
    > {
        self.settlement
            .take_failed_dispatch(stage, runtime, actor, port, self.pending.clone())
    }
    #[cfg(test)]
    pub(crate) fn publish_settlement_barrier(
        &self,
        snapshot: Option<
            Arc<novarocks_execution_contract::AcceptedTaskConclusion<ExecutionFailure>>,
        >,
    ) {
        self.settlement.publish_barrier(snapshot);
    }
    #[cfg(test)]
    pub(crate) fn run_settlement(&self) {
        self.settlement.invoke();
        self.pending.pending();
    }
    /// Revocation is not enough. Only original holder exit can account for a
    /// continuation that never registered; this creates no fictional call.
    pub(crate) fn observe_unused_settlement(&self) {
        let _observation = self
            .unused_observation
            .lock()
            .expect("unused completion holder observation");
        let finisher = &self.originals[0].exit_custody;
        if self.settlement.phase() == SettlementPhase::Unused
            && finisher.body_exited()
            && finisher.captures_exited()
        {
            self.settlement.unused_holder_exited();
        }
    }
    /// The same original settlement callback and captures returned; report
    /// consumers and header reaping are independent of this positive fact.
    pub(crate) fn settlement_call_exited(&self) -> bool {
        self.settlement.actual_call_exited()
    }
    /// Read the real fixed execution-call holders, without consuming reports
    /// or waiting for causal inputs, sources or child-header reaping. In
    /// particular the finisher's settlement child has its own actual-exit
    /// evidence below; its still-live report must not delay this narrow gate.
    pub(crate) fn execution_calls_exited(&self) -> bool {
        self.originals.iter().all(|original| {
            matches!(
                original.exit_custody.phase(),
                ExitCustodyPhase::Exited | ExitCustodyPhase::Retired
            ) && original.exit_custody.body_exited()
                && original.exit_custody.captures_exited()
        }) && self.settlement.actual_call_exited()
    }
    /// This covers these four roles only. Whole Task convergence also requires
    /// its other actual subsystem/source/cut/report facts. The context's bounded
    /// reaper separately advances child-to-parent work retirement.
    pub(crate) fn try_retire(&self) -> bool {
        if self.settlement.phase() != SettlementPhase::Retired {
            self.settlement.try_retire();
        }
        for original in &self.originals {
            if original.exit_custody.phase() != ExitCustodyPhase::Retired {
                original
                    .exit_custody
                    .try_retire()
                    .expect("original completion custody retirement");
            }
        }
        self.is_retired()
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.settlement.phase() == SettlementPhase::Retired
            && self
                .originals
                .iter()
                .all(|original| original.exit_custody.phase() == ExitCustodyPhase::Retired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_control_runtime::transaction::{RuntimeLedger, ScopeHandle};
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        completion::ConsumerKind,
        identity::{ConsumerId, DimensionId, Generation},
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
    };

    struct Pending(Arc<AtomicUsize>);
    impl TaskPendingNotice for Pending {
        fn pending(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Registered(Arc<AtomicUsize>);
    impl RegistrationNotice for Registered {
        fn bind_work(&mut self, _: novarocks_control_runtime_core::identity::ScopedWorkRef) {}
        fn registered(self: Box<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn unused(self: Box<Self>) {
            self.0.fetch_add(2, Ordering::SeqCst);
        }
    }
    struct Fixture {
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        completion_set: Arc<TaskCompletionSet>,
        admitted: Option<AdmittedWorkParts<ExecutionFailure>>,
        health: Arc<ExecutionHealth>,
        registration: Arc<AtomicUsize>,
    }
    fn generation() -> Generation {
        Generation::new(1).unwrap()
    }
    fn dimension() -> DimensionId {
        DimensionId::new(1).unwrap()
    }
    fn core() -> ConsumerId {
        ConsumerId::new(1).unwrap()
    }
    fn observer() -> ConsumerId {
        ConsumerId::new(2).unwrap()
    }
    fn named() -> [TaskLifecycleInputKind; 4] {
        [
            TaskLifecycleInputKind::Finisher,
            TaskLifecycleInputKind::Settlement,
            TaskLifecycleInputKind::WallTime,
            TaskLifecycleInputKind::TerminalPublisher,
        ]
    }
    fn reserve_completion_transfer(
        scope: &ScopeHandle,
        kind: WorkKind,
        admission: WorkAdmission,
    ) -> AdmittedCompletionTransfer {
        let (work, mut capacity) = scope
            .reserve_next(
                generation(),
                novarocks_control_runtime_core::capacity::ReservationResources::new(
                    ResourceVector::new([(dimension(), 1)]).unwrap(),
                    novarocks_control_runtime_core::owned_storage::OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..novarocks_control_runtime_core::owned_storage::OwnedSlotNeed::default()
                    },
                ),
                kind,
                admission,
            )
            .unwrap()
            .bind(kind)
            .unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            2,
            [
                (
                    core(),
                    ConsumerKind::Owner {
                        scope: scope.id(),
                        generation: generation(),
                    },
                ),
                (observer(), ConsumerKind::Observer),
            ],
        )
        .unwrap();
        AdmittedCompletionTransfer {
            work,
            capacity,
            report,
        }
    }
    fn fixture() -> Fixture {
        let ledger = RuntimeLedger::new(ResourceVector::new([(dimension(), 4)]).unwrap());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let finisher =
            reserve_completion_transfer(&scope, WorkKind::Cleanup, WorkAdmission::External);
        let child = reserve_completion_transfer(
            &scope,
            WorkKind::Settlement,
            WorkAdmission::Derived(finisher.work.id()),
        );
        let wall_time =
            reserve_completion_transfer(&scope, WorkKind::Cleanup, WorkAdmission::External);
        let publisher =
            reserve_completion_transfer(&scope, WorkKind::Cleanup, WorkAdmission::External);
        let health = Arc::new(ExecutionHealth::default());
        let registration = Arc::new(AtomicUsize::new(0));
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(531, 532), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let (roles, admitted) = TaskCompletionSet::from_admitted(
            identity,
            [finisher, wall_time, publisher],
            child,
            Box::new(Registered(registration.clone())),
            Arc::new(Pending(Arc::new(AtomicUsize::new(0)))),
            health.clone(),
        );
        Fixture {
            ledger,
            scope,
            completion_set: Arc::new(roles),
            admitted: Some(admitted),
            health,
            registration,
        }
    }
    #[test]
    fn unused_registration_waits_for_original_holder_exit_and_all_report_consumers() {
        let mut f = fixture();
        assert!(!f.completion_set.try_retire());
        drop(f.admitted.take());
        assert_eq!(f.registration.load(Ordering::SeqCst), 2);
        assert_eq!(f.completion_set.settlement_phase(), SettlementPhase::Unused);
        assert_eq!(f.ledger.used(dimension()), 4);
        f.completion_set.observe_unused_settlement();
        assert_eq!(f.completion_set.settlement_phase(), SettlementPhase::Exited);
        for input_kind in named() {
            f.completion_set.report(input_kind).consume(core()).unwrap();
        }
        assert!(!f.completion_set.try_retire());
        assert_eq!(f.ledger.used(dimension()), 4);
        for input_kind in named() {
            f.completion_set
                .report(input_kind)
                .consume(observer())
                .unwrap();
        }
        f.completion_set.try_retire();
        f.ledger.reap_scope_ready(f.scope.id(), 4).unwrap();
        assert!(f.completion_set.try_retire());
        f.scope.close_external().unwrap();
        f.ledger.remove_scope(f.scope.id()).unwrap();
        assert!(f.ledger.is_empty());
    }
    struct Capture {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }
    impl Drop for Capture {
        fn drop(&mut self) {
            self.entered.wait();
            self.release.wait();
        }
    }
    struct Invocation {
        release: Arc<Barrier>,
        thread: Option<std::thread::JoinHandle<()>>,
    }
    impl Invocation {
        fn join(&mut self) {
            if let Some(thread) = self.thread.take() {
                self.release.wait();
                thread.join().expect("original settlement invocation exits");
            }
        }
    }
    impl Drop for Invocation {
        fn drop(&mut self) {
            self.join();
        }
    }
    #[test]
    fn failed_core_cannot_free_the_original_finisher_before_child_capture_and_observer_exit() {
        let mut f = fixture();
        let (finisher, wall_time, publisher) = f.admitted.take().unwrap().into_parts();
        let (permit, mut work) = finisher.into_parts();
        work.bind();
        work.started();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let capture = Capture {
            entered: entered.clone(),
            release: release.clone(),
        };
        permit.register(Box::new(move |_| {
            std::hint::black_box(&capture);
        }));
        assert_eq!(
            f.completion_set.settlement_phase(),
            SettlementPhase::Registered
        );
        assert_eq!(f.registration.load(Ordering::SeqCst), 1);
        f.completion_set.publish_settlement_barrier(None);
        let roles = f.completion_set.clone();
        let thread = std::thread::spawn(move || roles.run_settlement());
        entered.wait();
        // After the original destructor reaches its controlled gate, even an
        // assertion failure releases it and joins its actual thread.
        let mut invocation = Invocation {
            release,
            thread: Some(thread),
        };
        work.completed();
        drop(wall_time);
        drop(publisher);
        f.scope.fail().unwrap();
        for input_kind in named() {
            f.completion_set
                .report(input_kind)
                .abandon_failed_owner(core(), &f.scope, generation())
                .unwrap();
        }
        for input_kind in [
            TaskLifecycleInputKind::Finisher,
            TaskLifecycleInputKind::WallTime,
            TaskLifecycleInputKind::TerminalPublisher,
        ] {
            f.completion_set
                .report(input_kind)
                .consume(observer())
                .unwrap();
        }
        assert!(!f.completion_set.try_retire());
        f.ledger.reap_scope_ready(f.scope.id(), 4).unwrap();
        assert_eq!(f.scope.snapshot().unwrap().work, 2);
        assert_eq!(f.ledger.used(dimension()), 2);
        assert_eq!(
            f.completion_set.phase(TaskLifecycleInputKind::Finisher),
            ExitCustodyPhase::Exited
        );
        invocation.join();
        assert!(
            !f.completion_set.try_retire(),
            "observer still owns the child report leg"
        );
        f.completion_set
            .report(TaskLifecycleInputKind::Settlement)
            .consume(observer())
            .unwrap();
        f.completion_set.try_retire();
        f.ledger.reap_scope_ready(f.scope.id(), 4).unwrap();
        assert!(f.completion_set.try_retire());
        assert!(f.health.failure().is_none());
        f.ledger.remove_scope(f.scope.id()).unwrap();
        assert!(f.ledger.is_empty());
    }
    #[test]
    fn losing_a_bound_original_authority_cannot_revoke_its_child_as_unused_exit() {
        let mut f = fixture();
        let (finisher, wall_time, publisher) = f.admitted.take().unwrap().into_parts();
        let (permit, mut work) = finisher.into_parts();
        work.bind();
        drop(work);
        drop(permit);
        drop(wall_time);
        drop(publisher);
        f.completion_set.observe_unused_settlement();
        assert_eq!(
            f.completion_set.phase(TaskLifecycleInputKind::Finisher),
            ExitCustodyPhase::Unresolved
        );
        assert_eq!(f.completion_set.settlement_phase(), SettlementPhase::Unused);
        assert!(f.health.failure().is_some());
        assert_eq!(f.scope.snapshot().unwrap().unresolved, 1);
        assert!(!f.completion_set.try_retire());
        assert_eq!(f.ledger.used(dimension()), 4);
    }
    mod execution_calls_exit;
}
