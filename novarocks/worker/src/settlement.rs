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

//! Task identity, registration notification and failure projection over the
//! shared runtime's single continuation-custody implementation.

pub(crate) use novarocks_control_runtime::continuation::ContinuationPhase as SettlementPhase;
use novarocks_control_runtime::{
    completion::ReportSlot,
    continuation::{ContinuationSlot, InvocationResult, RegistrationToken},
    lease::{CapacityBundle, WorkLease},
};
use novarocks_execution::{
    runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth},
    runtime::fragment::ExecutionFailure,
};
use novarocks_execution_contract::{
    AcceptedTaskConclusion, SettledContinuation, SettlementCapability, SettlementPermit,
    TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact},
};
use std::{mem::ManuallyDrop, sync::Arc};
type Snapshot = Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>>;

/// A role's already reserved neutral mailbox position. It cannot wait for its
/// actor or invoke the continuation; it only transfers the admitted role ID.
pub(crate) trait RegistrationNotice: Send {
    fn registered(self: Box<Self>);
    fn unused(self: Box<Self>);
}
struct Registration {
    token: RegistrationToken<Snapshot>,
    notice: Box<dyn RegistrationNotice>,
}
impl SettlementCapability<ExecutionFailure> for Registration {
    fn register(self: Box<Self>, continuation: SettledContinuation<ExecutionFailure>) {
        if let Err((error, callback)) = self.token.register(continuation) {
            let _callback = ManuallyDrop::new(callback);
            tracing::error!(
                ?error,
                "admitted Task settlement lost its unique registration"
            );
            std::process::abort();
        }
        self.notice.registered();
    }
    fn revoke_unused(self: Box<Self>) {
        drop(self.token);
        self.notice.unused();
    }
}

#[derive(Clone)]
pub(crate) struct SettlementRole {
    identity: TaskIdentity,
    slot: ContinuationSlot<Snapshot>,
    health: Arc<ExecutionHealth>,
}
impl SettlementRole {
    pub(crate) fn from_admitted(
        identity: TaskIdentity,
        work: WorkLease,
        capacity: CapacityBundle,
        completion: ReportSlot,
        health: Arc<ExecutionHealth>,
        notice: Box<dyn RegistrationNotice>,
    ) -> (Self, SettlementPermit<ExecutionFailure>) {
        let (slot, token) = ContinuationSlot::from_admitted(work, capacity, completion);
        let permit = SettlementPermit::from_admitted_role(
            identity,
            Box::new(Registration { token, notice }),
        );
        (
            Self {
                identity,
                slot,
                health,
            },
            permit,
        )
    }
    pub(crate) fn phase(&self) -> SettlementPhase {
        self.slot.phase()
    }
    /// Only the ordered, actually published barrier effect grants this input.
    pub(crate) fn publish_barrier(&self, snapshot: Snapshot) {
        if let Some(snapshot) = snapshot.as_ref() {
            assert_eq!(snapshot.neutral().identity(), self.identity);
        }
        if let Err((error, snapshot)) = self.slot.publish_barrier(snapshot) {
            drop(snapshot);
            panic!("Task settlement barrier failed: {error}");
        }
    }
    pub(crate) fn invoke(&self) {
        match self
            .slot
            .invoke()
            .expect("admitted Task settlement invocation")
        {
            InvocationResult::Exited => {}
            InvocationResult::Unresolved => {
                self.health.report_unresolved(
                    ExecutionExitOrigin::Task {
                        identity: self.identity,
                        phase: TaskFailurePhase::Execution,
                    },
                    FailureFact::new(
                        FailureClass::Unexpected,
                        crate::failure_reason::WorkerFailureReason::SettlementCall.reason(),
                    ),
                );
            }
        }
    }
    pub(crate) fn unused_role_exited(&self) {
        self.slot
            .unused_role_exited()
            .expect("original unused Task settlement exits");
    }
    pub(crate) fn try_retire(&self) -> bool {
        self.slot
            .try_retire()
            .expect("Task settlement completion custody")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_control_runtime::transaction::{RuntimeLedger, ScopeHandle};
    use novarocks_control_runtime_core::{
        capacity::{ReservationRequest, ResourceVector},
        completion::ConsumerKind,
        identity::{ConsumerId, DimensionId, Generation, ReservationId, ScopeId, SlotId, WorkId},
        scope::{WorkAdmission, WorkKind},
    };
    use std::sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
    };

    struct Fixture {
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        report: ReportSlot,
        owner: ConsumerId,
        observer: ConsumerId,
        role: SettlementRole,
        permit: Option<SettlementPermit<ExecutionFailure>>,
        health: Arc<ExecutionHealth>,
        notifications: Arc<AtomicUsize>,
    }

    struct Notice(Arc<AtomicUsize>);
    impl RegistrationNotice for Notice {
        fn registered(self: Box<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn unused(self: Box<Self>) {
            self.0.fetch_add(2, Ordering::SeqCst);
        }
    }

    fn fixture() -> Fixture {
        let dimension = DimensionId::new(1).unwrap();
        let resources = ResourceVector::new([(dimension, 1)]).unwrap();
        let ledger = RuntimeLedger::new(resources.clone(), 1, 1);
        let scope = ledger
            .open_scope(
                ScopeId::new(1).unwrap(),
                None,
                Generation::new(1).unwrap(),
                1,
                1,
            )
            .unwrap();
        let guard = scope
            .reserve(ReservationRequest {
                id: ReservationId::new(1).unwrap(),
                generation: Generation::new(1).unwrap(),
                guard: WorkId::new(1).unwrap(),
                resources,
                admission: WorkAdmission::External,
                kind: WorkKind::Settlement,
            })
            .unwrap();
        let owner = ConsumerId::new(1).unwrap();
        let observer = ConsumerId::new(2).unwrap();
        let report = ReportSlot::reserve(
            &scope,
            SlotId::new(1).unwrap(),
            Generation::new(1).unwrap(),
            guard.work_id(),
            2,
            [
                (
                    owner,
                    ConsumerKind::Owner {
                        scope: scope.id(),
                        generation: Generation::new(1).unwrap(),
                    },
                ),
                (observer, ConsumerKind::Observer),
            ],
        )
        .unwrap();
        let (work, capacity) = guard.bind(WorkKind::Settlement).unwrap();
        let health = Arc::new(ExecutionHealth::default());
        let notifications = Arc::new(AtomicUsize::new(0));
        let identity = TaskIdentity::new(
            novarocks_types::identity::QueryExecutionId::new(
                novarocks_types::QueryId::new(91, 92),
                novarocks_types::identity::AttemptId::new(1).unwrap(),
            )
            .unwrap(),
            novarocks_types::identity::StageId::new(1).unwrap(),
            novarocks_types::identity::TaskId::new(1).unwrap(),
            novarocks_types::identity::BackendProcessId::new_v7(),
        );
        let (role, permit) = SettlementRole::from_admitted(
            identity,
            work,
            capacity,
            report.clone(),
            health.clone(),
            Box::new(Notice(notifications.clone())),
        );
        Fixture {
            ledger,
            scope,
            report,
            owner,
            observer,
            role,
            permit: Some(permit),
            health,
            notifications,
        }
    }

    fn drain(f: &Fixture) {
        f.report.consume(f.owner).unwrap();
        f.report.consume(f.observer).unwrap();
        assert!(f.role.try_retire());
        assert_eq!(f.ledger.reap_ready(1).unwrap(), 1);
        f.scope.close_external().unwrap();
        assert!(f.scope.snapshot().unwrap().retirement_ready);
        f.ledger.remove_scope(f.scope.id()).unwrap();
    }

    #[test]
    fn one_prepaid_registration_waits_for_the_publication_barrier_and_all_consumers() {
        let mut f = fixture();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        // Move the unique permit without replacing it by another authority.
        let permit = f.permit.take().unwrap();
        permit.register(Box::new(move |snapshot| {
            assert!(snapshot.is_none());
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        assert_eq!(f.role.phase(), SettlementPhase::Registered);
        assert_eq!(f.notifications.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!f.role.try_retire());
        f.role.publish_barrier(None);
        f.role.invoke();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.role.phase(), SettlementPhase::Exited);
        assert!(!f.role.try_retire());
        assert_eq!(f.ledger.used(DimensionId::new(1).unwrap()), 1);
        drain(&f);
    }

    #[test]
    fn unused_authority_revocation_does_not_claim_actual_role_exit() {
        let mut f = fixture();
        drop(f.permit.take().unwrap());
        assert_eq!(f.role.phase(), SettlementPhase::Unused);
        assert_eq!(f.notifications.load(Ordering::SeqCst), 2);
        f.scope.close_external().unwrap();
        assert!(!f.scope.snapshot().unwrap().join_ready);
        assert!(!f.role.try_retire());
        f.role.unused_role_exited();
        assert!(!f.role.try_retire());
        drain(&f);
    }

    struct HeldCapture {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }
    impl Drop for HeldCapture {
        fn drop(&mut self) {
            self.entered.wait();
            self.release.wait();
        }
    }
    #[test]
    fn failed_owner_cannot_release_a_running_continuations_capture_or_other_consumer() {
        let mut f = fixture();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let capture = HeldCapture {
            entered: entered.clone(),
            release: release.clone(),
        };
        f.permit.take().unwrap().register(Box::new(move |_| {
            let _ = &capture;
        }));
        f.role.publish_barrier(None);
        let role = f.role.clone();
        let invocation = std::thread::spawn(move || role.invoke());
        entered.wait();
        f.scope.fail().unwrap();
        f.report
            .abandon_failed_owner(f.owner, &f.scope, Generation::new(1).unwrap())
            .unwrap();
        assert!(!f.scope.snapshot().unwrap().join_ready);
        assert!(!f.role.try_retire());
        release.wait();
        invocation.join().unwrap();
        assert!(
            !f.role.try_retire(),
            "another consumer still owes the report"
        );
        f.report.consume(f.observer).unwrap();
        assert!(f.role.try_retire());
        f.ledger.reap_ready(1).unwrap();
        assert!(f.scope.snapshot().unwrap().retirement_ready);
    }

    #[test]
    fn a_callback_panic_keeps_original_work_storage_and_payload_in_custody() {
        let mut f = fixture();
        f.permit.take().unwrap().register(Box::new(|_| {
            panic!("injected settlement call failure");
        }));
        f.role.publish_barrier(None);
        f.role.invoke();
        assert_eq!(f.role.phase(), SettlementPhase::Unresolved);
        assert!(f.health.failure().is_some());
        assert!(!f.scope.snapshot().unwrap().join_ready);
        assert!(!f.role.try_retire());
        assert_eq!(f.ledger.used(DimensionId::new(1).unwrap()), 1);
    }
}
