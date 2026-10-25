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

//! Custody for one prepaid settlement role. The protocol publishes permission
//! to call it; a host executor owns invocation and every foreign destructor.

use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

use novarocks_control_runtime::{
    completion::ReportSlot,
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

type Snapshot = Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>>;
type Payload = ManuallyDrop<Box<dyn Any + Send>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SettlementPhase {
    AwaitingRegistration,
    Registered,
    Ready,
    Running,
    Unused,
    Exited,
    Unresolved,
    Retired,
}

struct State {
    phase: SettlementPhase,
    callback: Option<ManuallyDrop<SettledContinuation<ExecutionFailure>>>,
    snapshot: Snapshot,
    work: Option<WorkLease>,
    capacity: Option<CapacityBundle>,
    panic: Option<Payload>,
}

struct Custody {
    identity: TaskIdentity,
    completion: ReportSlot,
    health: Arc<ExecutionHealth>,
    state: Mutex<State>,
    // Actual unresolved work stays owned even if its external handle exits.
    // Successful retirement explicitly breaks this original custody cycle.
    retained: Mutex<Option<Arc<Self>>>,
}

/// The host's handle for an already admitted role. Clones refer to the same
/// role and cannot issue another registration authority.
#[derive(Clone)]
pub(crate) struct SettlementRole(Arc<Custody>);

/// The role's already reserved mailbox position. Implementations only fill
/// a neutral event; they cannot wait for its actor or invoke the continuation.
pub(crate) trait RegistrationNotice: Send {
    fn registered(self: Box<Self>);
    fn unused(self: Box<Self>);
}

struct Registration {
    custody: Arc<Custody>,
    notice: Box<dyn RegistrationNotice>,
}
impl SettlementCapability<ExecutionFailure> for Registration {
    fn register(self: Box<Self>, continuation: SettledContinuation<ExecutionFailure>) {
        // Taking an EC permit already consumed the only registration right.
        // Neither locking nor this handoff calls or drops foreign captures.
        let continuation = ManuallyDrop::new(continuation);
        let mut state = self.custody.state.lock().expect("settlement custody lock");
        assert_eq!(state.phase, SettlementPhase::AwaitingRegistration);
        state.callback = Some(continuation);
        state.phase = SettlementPhase::Registered;
        drop(state);
        self.notice.registered();
    }

    fn revoke_unused(self: Box<Self>) {
        let mut state = self.custody.state.lock().expect("settlement custody lock");
        assert_eq!(state.phase, SettlementPhase::AwaitingRegistration);
        state.phase = SettlementPhase::Unused;
        // This only revokes authority. The role's original host still owes
        // its actual exit, and its report still owes core consumption.
        drop(state);
        self.notice.unused();
    }
}

impl SettlementRole {
    /// Consumes a role's existing work, capacity and reliable exit storage.
    /// This constructor does not reserve capacity or mint work after admission.
    pub(crate) fn from_admitted(
        identity: TaskIdentity,
        work: WorkLease,
        capacity: CapacityBundle,
        completion: ReportSlot,
        health: Arc<ExecutionHealth>,
        notice: Box<dyn RegistrationNotice>,
    ) -> (Self, SettlementPermit<ExecutionFailure>) {
        let custody = Arc::new(Custody {
            identity,
            completion,
            health,
            state: Mutex::new(State {
                phase: SettlementPhase::AwaitingRegistration,
                callback: None,
                snapshot: None,
                work: Some(work),
                capacity: Some(capacity),
                panic: None,
            }),
            retained: Mutex::new(None),
        });
        *custody.retained.lock().expect("settlement retention lock") = Some(custody.clone());
        let permit = SettlementPermit::from_admitted_role(
            identity,
            Box::new(Registration {
                custody: custody.clone(),
                notice,
            }),
        );
        (Self(custody), permit)
    }

    pub(crate) fn phase(&self) -> SettlementPhase {
        self.0.state.lock().expect("settlement custody lock").phase
    }

    /// Called by the barrier's ordered publication effect, after every earlier
    /// proposal has been adjudicated and its required publications committed.
    /// This grants execution; it never invokes the continuation on a poller.
    pub(crate) fn publish_barrier(&self, snapshot: Snapshot) {
        if let Some(snapshot) = snapshot.as_ref() {
            assert_eq!(snapshot.neutral().identity(), self.0.identity);
        }
        let mut state = self.0.state.lock().expect("settlement custody lock");
        assert_eq!(state.phase, SettlementPhase::Registered);
        state.snapshot = snapshot;
        state.phase = SettlementPhase::Ready;
    }

    /// Runs on the role's host executor. Failure retains the original payload
    /// and work; a panic is never positive evidence of capture disposal.
    pub(crate) fn invoke(&self) {
        let (callback, snapshot, mut work) = {
            let mut state = self.0.state.lock().expect("settlement custody lock");
            assert_eq!(state.phase, SettlementPhase::Ready);
            let mut work = state.work.take().expect("original settlement work");
            work.start().expect("admitted settlement starts after stop");
            state.phase = SettlementPhase::Running;
            (
                state.callback.take().expect("registered continuation"),
                state.snapshot.take(),
                work,
            )
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            ManuallyDrop::into_inner(callback)(snapshot);
        }));
        match result {
            Ok(()) => {
                // FnOnce's call and capture destruction have really returned.
                work.body_exited().expect("settlement body exits once");
                work.captures_exited_with_report(&self.0.completion)
                    .expect("settlement capture exit and report are atomic");
                self.0.completion.body_exited().expect("exit report body");
                self.0
                    .completion
                    .captures_exited()
                    .expect("exit report captures");
                let mut state = self.0.state.lock().expect("settlement custody lock");
                state.work = Some(work);
                state.phase = SettlementPhase::Exited;
            }
            Err(payload) => {
                let payload = ManuallyDrop::new(payload);
                let mut state = self.0.state.lock().expect("settlement custody lock");
                state.panic = Some(payload);
                state.work = Some(work);
                state.phase = SettlementPhase::Unresolved;
                drop(state);
                self.0.health.report_unresolved(
                    ExecutionExitOrigin::Task {
                        identity: self.0.identity,
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

    /// The original unused role has exited without constructing a callback.
    /// Dropping its permit alone cannot call this method or supply this fact.
    pub(crate) fn unused_role_exited(&self) {
        let mut state = self.0.state.lock().expect("settlement custody lock");
        assert_eq!(state.phase, SettlementPhase::Unused);
        let work = state.work.as_mut().expect("original settlement work");
        work.body_exited().expect("unused role body exits once");
        work.captures_exited_with_report(&self.0.completion)
            .expect("unused role capture exit and report are atomic");
        self.0.completion.body_exited().expect("exit report body");
        self.0
            .completion
            .captures_exited()
            .expect("exit report captures");
        state.phase = SettlementPhase::Exited;
    }

    /// Core/other consumers acknowledge the actual reliable report separately.
    /// Storage and its capacity remain owned until every such leg is retired.
    pub(crate) fn try_retire(&self) -> bool {
        let mut state = self.0.state.lock().expect("settlement custody lock");
        if state.phase != SettlementPhase::Exited {
            return false;
        }
        if !self
            .0
            .completion
            .try_retire()
            .expect("settlement completion slot")
        {
            return false;
        }
        let mut capacity = state.capacity.take().expect("original settlement capacity");
        let resources = capacity.resources().expect("live settlement capacity");
        capacity
            .release(&resources)
            .expect("retired settlement capacity");
        let work = state.work.take();
        state.phase = SettlementPhase::Retired;
        drop(state);
        drop(work);
        drop(capacity);
        let retained = self
            .0
            .retained
            .lock()
            .expect("settlement retention lock")
            .take();
        drop(retained);
        true
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
