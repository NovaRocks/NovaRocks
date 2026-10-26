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
    control_runtime_policy::TaskLifecycleRole,
    settlement::{RegistrationNotice, SettlementPhase, SettlementRole},
    task_port::TaskPendingNotice,
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_role::{ExitAuthority, ExitRole, ExitRolePhase},
    lease::{CapacityBundle, WorkLease},
};
use novarocks_execution::runtime::{
    execution_health::{ExecutionExitOrigin, ExecutionHealth},
    fragment::ExecutionFailure,
};
use novarocks_execution_contract::{
    AcceptedTaskConclusion, AdmittedFinisher, AdmittedFragmentRoles, CompletionWorkCapability,
    TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact},
};
use std::sync::{Arc, Mutex};

/// Supplied once by the actual Task admission/preparation owner. Capacity may
/// be a role's own portion or a zero-resource child of an aggregate prepaid
/// Task bundle, whose owner retains funding until every child/report retires.
pub(crate) struct AdmittedCompletionRole {
    pub work: WorkLease,
    pub capacity: CapacityBundle,
    pub report: ReportSlot,
}
struct Original {
    role: ExitRole,
    report: ReportSlot,
}
struct Work {
    identity: TaskIdentity,
    authority: Option<ExitAuthority>,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
}
impl Work {
    fn exited(mut self: Box<Self>) {
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
pub(crate) struct TaskCompletionRoles {
    originals: [Original; 3],
    settlement: SettlementRole,
    report: ReportSlot,
    pending: Arc<dyn TaskPendingNotice>,
    unused_observation: Mutex<()>,
}
impl TaskCompletionRoles {
    /// All four roles and report positions already exist. The settlement work
    /// is a same-scope derived child of the original finisher; the caller keeps
    /// the aggregate Task funding and transfers this single set before building
    /// the dormant fragment. No capacity acquisition happens on registration.
    pub(crate) fn from_admitted(
        identity: TaskIdentity,
        originals: [AdmittedCompletionRole; 3],
        child: AdmittedCompletionRole,
        registration: Box<dyn RegistrationNotice>,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> (Self, AdmittedFragmentRoles<ExecutionFailure>) {
        let scope = child.work.scope().id();
        assert!(
            originals.iter().all(|role| role.work.scope().id() == scope),
            "one context scope owns original completion roles"
        );
        let report = child.report.clone();
        let (settlement, permit) = SettlementRole::from_admitted(
            identity,
            child.work,
            child.capacity,
            child.report,
            health.clone(),
            registration,
        );
        let bind = |input: AdmittedCompletionRole| {
            let report = input.report.clone();
            let (role, authority) =
                ExitRole::from_admitted(input.work, input.capacity, input.report);
            let capability = Box::new(Work {
                identity,
                authority: Some(authority),
                pending: pending.clone(),
                health: health.clone(),
            }) as Box<dyn CompletionWorkCapability>;
            (Original { role, report }, capability)
        };
        let [finisher, wall_time, publisher] = originals;
        let (finisher, finisher_work) = bind(finisher);
        let (wall_time, wall_work) = bind(wall_time);
        let (publisher, publisher_work) = bind(publisher);
        let admitted = AdmittedFragmentRoles::from_admitted_roles(
            AdmittedFinisher::from_admitted_role(permit, finisher_work),
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
    fn original(&self, role: TaskLifecycleRole) -> &Original {
        &self.originals[match role {
            TaskLifecycleRole::Finisher => 0,
            TaskLifecycleRole::WallTime => 1,
            TaskLifecycleRole::TerminalPublisher => 2,
            _ => panic!("not an original fragment completion role"),
        }]
    }
    pub(crate) fn report(&self, role: TaskLifecycleRole) -> &ReportSlot {
        if matches!(role, TaskLifecycleRole::Settlement) {
            &self.report
        } else {
            &self.original(role).report
        }
    }
    pub(crate) fn phase(&self, role: TaskLifecycleRole) -> ExitRolePhase {
        self.original(role).role.phase()
    }
    pub(crate) fn settlement_phase(&self) -> SettlementPhase {
        self.settlement.phase()
    }
    pub(crate) fn publish_settlement_barrier(
        &self,
        snapshot: Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>>,
    ) {
        self.settlement.publish_barrier(snapshot);
    }
    /// Called by the host executor after the actual predecessor publications.
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
        let finisher = &self.originals[0].role;
        if self.settlement.phase() == SettlementPhase::Unused
            && finisher.body_exited()
            && finisher.captures_exited()
        {
            self.settlement.unused_role_exited();
        }
    }
    /// This covers these four roles only. Whole Task convergence also requires
    /// its other actual subsystem/source/cut/report facts. The context's bounded
    /// reaper separately advances child-to-parent work retirement.
    pub(crate) fn try_retire(&self) -> bool {
        if self.settlement.phase() != SettlementPhase::Retired {
            self.settlement.try_retire();
        }
        for original in &self.originals {
            if original.role.phase() != ExitRolePhase::Retired {
                original
                    .role
                    .try_retire()
                    .expect("original completion custody retirement");
            }
        }
        self.settlement.phase() == SettlementPhase::Retired
            && self
                .originals
                .iter()
                .all(|original| original.role.phase() == ExitRolePhase::Retired)
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
        roles: Arc<TaskCompletionRoles>,
        admitted: Option<AdmittedFragmentRoles<ExecutionFailure>>,
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
    fn named() -> [TaskLifecycleRole; 4] {
        [
            TaskLifecycleRole::Finisher,
            TaskLifecycleRole::Settlement,
            TaskLifecycleRole::WallTime,
            TaskLifecycleRole::TerminalPublisher,
        ]
    }
    fn role(
        scope: &ScopeHandle,
        kind: WorkKind,
        admission: WorkAdmission,
    ) -> AdmittedCompletionRole {
        let (work, capacity) = scope
            .reserve_next(
                generation(),
                ResourceVector::new([(dimension(), 1)]).unwrap(),
                kind,
                admission,
            )
            .unwrap()
            .bind(kind)
            .unwrap();
        let report = ReportSlot::reserve_next(
            scope,
            generation(),
            work.id(),
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
        AdmittedCompletionRole {
            work,
            capacity,
            report,
        }
    }
    fn fixture() -> Fixture {
        let ledger = RuntimeLedger::new(ResourceVector::new([(dimension(), 4)]).unwrap(), 4, 1);
        let scope = ledger.open_scope_next(None, generation(), 4, 4).unwrap();
        let finisher = role(&scope, WorkKind::Cleanup, WorkAdmission::External);
        let child = role(
            &scope,
            WorkKind::Settlement,
            WorkAdmission::Derived(finisher.work.id()),
        );
        let wall_time = role(&scope, WorkKind::Cleanup, WorkAdmission::External);
        let publisher = role(&scope, WorkKind::Cleanup, WorkAdmission::External);
        let health = Arc::new(ExecutionHealth::default());
        let registration = Arc::new(AtomicUsize::new(0));
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(531, 532), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let (roles, admitted) = TaskCompletionRoles::from_admitted(
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
            roles: Arc::new(roles),
            admitted: Some(admitted),
            health,
            registration,
        }
    }
    #[test]
    fn unused_registration_waits_for_original_holder_exit_and_all_report_consumers() {
        let mut f = fixture();
        assert!(!f.roles.try_retire());
        drop(f.admitted.take());
        assert_eq!(f.registration.load(Ordering::SeqCst), 2);
        assert_eq!(f.roles.settlement_phase(), SettlementPhase::Unused);
        assert_eq!(f.ledger.used(dimension()), 4);
        f.roles.observe_unused_settlement();
        assert_eq!(f.roles.settlement_phase(), SettlementPhase::Exited);
        for role in named() {
            f.roles.report(role).consume(core()).unwrap();
        }
        assert!(!f.roles.try_retire());
        assert_eq!(f.ledger.used(dimension()), 4);
        for role in named() {
            f.roles.report(role).consume(observer()).unwrap();
        }
        f.roles.try_retire();
        f.ledger.reap_scope_ready(f.scope.id(), 4).unwrap();
        assert!(f.roles.try_retire());
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
        assert_eq!(f.roles.settlement_phase(), SettlementPhase::Registered);
        assert_eq!(f.registration.load(Ordering::SeqCst), 1);
        f.roles.publish_settlement_barrier(None);
        let roles = f.roles.clone();
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
        for role in named() {
            f.roles
                .report(role)
                .abandon_failed_owner(core(), &f.scope, generation())
                .unwrap();
        }
        for role in [
            TaskLifecycleRole::Finisher,
            TaskLifecycleRole::WallTime,
            TaskLifecycleRole::TerminalPublisher,
        ] {
            f.roles.report(role).consume(observer()).unwrap();
        }
        assert!(!f.roles.try_retire());
        f.ledger.reap_scope_ready(f.scope.id(), 4).unwrap();
        assert_eq!(f.scope.snapshot().unwrap().work, 2);
        assert_eq!(f.ledger.used(dimension()), 2);
        assert_eq!(
            f.roles.phase(TaskLifecycleRole::Finisher),
            ExitRolePhase::Exited
        );
        invocation.join();
        assert!(
            !f.roles.try_retire(),
            "observer still owns the child report leg"
        );
        f.roles
            .report(TaskLifecycleRole::Settlement)
            .consume(observer())
            .unwrap();
        f.roles.try_retire();
        f.ledger.reap_scope_ready(f.scope.id(), 4).unwrap();
        assert!(f.roles.try_retire());
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
        f.roles.observe_unused_settlement();
        assert_eq!(
            f.roles.phase(TaskLifecycleRole::Finisher),
            ExitRolePhase::Unresolved
        );
        assert_eq!(f.roles.settlement_phase(), SettlementPhase::Unused);
        assert!(f.health.failure().is_some());
        assert_eq!(f.scope.snapshot().unwrap().unresolved, 1);
        assert!(!f.roles.try_retire());
        assert_eq!(f.ledger.used(dimension()), 4);
    }
}
