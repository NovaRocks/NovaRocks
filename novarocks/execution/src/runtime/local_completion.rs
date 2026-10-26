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

//! One taskless fragment's finite completion roles. Registration only stores
//! its one callback; the physical completion driver invokes it after the
//! original finisher call returns. No Task identity or protocol is invented.

use std::sync::{Arc, Mutex};

use novarocks_control_runtime::{
    completion::ReportSlot,
    continuation::{ContinuationPhase, ContinuationSlot, InvocationResult, RegistrationToken},
    exit_role::{ExitAuthority, ExitRole, ExitRolePhase as Phase},
    lease::{CapacityBundle, WorkLease},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::ConsumerKind,
    identity::{ConsumerId, DimensionId, Generation},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{
    AcceptedTaskConclusion, AdmittedFinisher, AdmittedFragmentRoles, CompletionWorkCapability,
    SettledContinuation, SettlementCapability, SettlementPermit,
    failure::{FailureClass, FailureFact},
};

use crate::runtime::{
    fragment::{ExecutionFailure, ExecutionReason},
    task_control::ExecutionExitReporter,
};

type Snapshot = Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>>;
const ROLES: usize = 3;
const RESPONSIBILITIES: usize = ROLES + 1;
const FINISHER: usize = 0;

struct Role {
    slot: ExitRole,
    report: ReportSlot,
    consumed: bool,
}
struct State {
    roles: [Role; ROLES],
    settlement_consumed: bool,
    settlement_retired: bool,
    removed: bool,
    run_claimed: bool,
}
struct Owner {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    state: Mutex<State>,
    settlement: ContinuationSlot<Snapshot>,
    report: ReportSlot,
    health: ExecutionExitReporter,
}

/// The original executor holds this driver, independently of settlement
/// registration authority. It runs on its host thread, outside all role locks.
#[derive(Clone)]
pub(crate) struct LocalCompletionDriver(Arc<Owner>);

struct Work {
    owner: Arc<Owner>,
    authority: Option<ExitAuthority>,
}
impl Work {
    fn exited(mut self: Box<Self>) {
        self.authority
            .take()
            .expect("original local role authority")
            .exited()
            .expect("original local body and captures actually exited");
        self.owner.drain();
    }
}
impl CompletionWorkCapability for Work {
    fn started(&mut self) {
        self.authority
            .as_mut()
            .expect("original local role authority")
            .start()
            .expect("admitted local work starts");
    }
    fn completed(self: Box<Self>) {
        self.exited();
    }
    fn unused_exited(self: Box<Self>) {
        self.exited();
    }
    fn unresolved(mut self: Box<Self>) {
        // Drop records unknown exit; it is never positive body/capture proof.
        drop(self.authority.take());
        self.owner.health.report(FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::CompletionWorkUnresolved.reason(),
        ));
    }
}
struct Registration {
    token: RegistrationToken<Snapshot>,
    owner: Arc<Owner>,
}
impl SettlementCapability<ExecutionFailure> for Registration {
    fn register(self: Box<Self>, callback: SettledContinuation<ExecutionFailure>) {
        self.token
            .register(callback)
            .unwrap_or_else(|_| panic!("one admitted local settlement registration"));
        // No callback, destructor or driver runs on the registration stack.
    }
    fn revoke_unused(self: Box<Self>) {
        drop(self.token);
        self.owner.drain();
    }
}

fn consumer() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}
fn resources() -> ResourceVector {
    ResourceVector::new([(DimensionId::new(1).unwrap(), 1)]).unwrap()
}
fn report(scope: &ScopeHandle, work: &WorkLease) -> ReportSlot {
    ReportSlot::reserve_next(
        scope,
        Generation::new(1).unwrap(),
        work.id(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    )
    .expect("one prepaid report per local completion role")
}
fn admit(
    scope: &ScopeHandle,
    admission: WorkAdmission,
    kind: WorkKind,
) -> (WorkLease, CapacityBundle) {
    scope
        .reserve_next(Generation::new(1).unwrap(), resources(), kind, admission)
        .expect("finite original local completion roles")
        .bind(kind)
        .expect("original local role holder")
}
impl LocalCompletionDriver {
    pub(crate) fn admit(
        health: ExecutionExitReporter,
    ) -> (AdmittedFragmentRoles<ExecutionFailure>, Self) {
        // This geometry follows the three named OnceRoles and the sole child
        // continuation. It is unrelated to SQL data size or backend topology.
        let ledger = RuntimeLedger::new(
            ResourceVector::new([(DimensionId::new(1).unwrap(), RESPONSIBILITIES as u64)]).unwrap(),
            RESPONSIBILITIES,
            1,
        );
        let scope = ledger
            .open_scope_next(
                None,
                Generation::new(1).unwrap(),
                RESPONSIBILITIES,
                RESPONSIBILITIES,
            )
            .unwrap();
        let (finisher, finisher_capacity) =
            admit(&scope, WorkAdmission::External, WorkKind::Cleanup);
        let (child, child_capacity) = admit(
            &scope,
            WorkAdmission::Derived(finisher.id()),
            WorkKind::Settlement,
        );
        let child_report = report(&scope, &child);
        let (settlement, token) =
            ContinuationSlot::from_admitted(child, child_capacity, child_report.clone());
        let finisher_report = report(&scope, &finisher);
        let make_role = |work, capacity, report: ReportSlot| {
            let (slot, authority) = ExitRole::from_admitted(work, capacity, report.clone());
            (
                Role {
                    slot,
                    report,
                    consumed: false,
                },
                authority,
            )
        };
        let (finisher, authority) = make_role(finisher, finisher_capacity, finisher_report);
        let mut roles = vec![finisher];
        let mut authorities = vec![authority];
        for _ in 1..ROLES {
            let (work, capacity) = admit(&scope, WorkAdmission::External, WorkKind::Cleanup);
            let slot = report(&scope, &work);
            let (role, authority) = make_role(work, capacity, slot);
            roles.push(role);
            authorities.push(authority);
        }
        scope.close_external().unwrap();
        ledger.close_admission();
        let owner = Arc::new(Owner {
            ledger,
            scope,
            state: Mutex::new(State {
                roles: roles
                    .try_into()
                    .unwrap_or_else(|_| unreachable!("three named roles")),
                settlement_consumed: false,
                settlement_retired: false,
                removed: false,
                run_claimed: false,
            }),
            settlement,
            report: child_report,
            health,
        });
        let work = |authority| {
            Box::new(Work {
                owner: owner.clone(),
                authority: Some(authority),
            }) as Box<dyn CompletionWorkCapability>
        };
        let permit = SettlementPermit::from_admitted_local_role(Box::new(Registration {
            token,
            owner: owner.clone(),
        }));
        let [finisher, wall_time, publisher]: [ExitAuthority; ROLES] = authorities
            .try_into()
            .unwrap_or_else(|_| unreachable!("three original role authorities"));
        let roles = AdmittedFragmentRoles::from_admitted_roles(
            AdmittedFinisher::from_admitted_role(permit, work(finisher)),
            work(wall_time),
            work(publisher),
        );
        (roles, Self(owner))
    }
    pub(crate) fn run(&self) {
        let invoke = {
            let mut state = self.0.state.lock().expect("local completion custody");
            if !state.run_claimed && self.0.settlement.phase() == ContinuationPhase::Registered {
                state.run_claimed = true;
                true
            } else {
                false
            }
        };
        if invoke {
            self.0
                .settlement
                .publish_barrier(None)
                .expect("taskless local barrier");
            if self
                .0
                .settlement
                .invoke()
                .expect("admitted local settlement invocation")
                == InvocationResult::Unresolved
            {
                self.0.health.report(FailureFact::new(
                    FailureClass::Unexpected,
                    ExecutionReason::SettlementContinuationPanicked.reason(),
                ));
            }
        }
        self.0.drain();
    }
}
impl Owner {
    fn drain(&self) {
        let mut state = self.state.lock().expect("local completion custody");
        if state.removed {
            return;
        }
        if self.settlement.phase() == ContinuationPhase::Unused
            && state.roles[FINISHER].slot.body_exited()
            && state.roles[FINISHER].slot.captures_exited()
        {
            // Both facts are necessary: revocation alone is not original exit.
            self.settlement.unused_role_exited().unwrap();
        }
        if self.settlement.phase() == ContinuationPhase::Exited {
            if !state.settlement_consumed {
                self.report.consume(consumer()).unwrap();
                state.settlement_consumed = true;
            }
            state.settlement_retired = self.settlement.try_retire().unwrap();
        }
        self.ledger
            .reap_scope_ready(self.scope.id(), RESPONSIBILITIES)
            .unwrap();
        for role in &mut state.roles {
            if role.slot.phase() != Phase::Exited {
                continue;
            }
            if !role.consumed {
                role.report.consume(consumer()).unwrap();
                role.consumed = true;
            }
            role.slot
                .try_retire()
                .expect("local original completion custody");
        }
        if state.settlement_retired
            && state
                .roles
                .iter()
                .all(|role| role.slot.phase() == Phase::Retired)
        {
            self.ledger.remove_scope(self.scope.id()).unwrap();
            state.removed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{execution_health::ExecutionHealth, task_control::ExecutionExitReporter};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn fixture() -> (
        AdmittedFragmentRoles<ExecutionFailure>,
        LocalCompletionDriver,
        Arc<ExecutionHealth>,
    ) {
        let health = Arc::new(ExecutionHealth::default());
        let reporter = ExecutionExitReporter::new(
            health.clone(),
            crate::runtime::execution_health::ExecutionExitOrigin::Local,
        );
        let (roles, driver) = LocalCompletionDriver::admit(reporter);
        (roles, driver, health)
    }
    #[test]
    fn registration_returns_before_the_local_driver_and_keeps_the_original_finisher_counted() {
        let (roles, driver, health) = fixture();
        let (finisher, wall_time, publisher) = roles.into_parts();
        let (permit, mut work) = finisher.into_parts();
        work.bind();
        work.started();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        permit.register(Box::new(move |snapshot| {
            assert!(snapshot.is_none(), "no synthetic Task receipt");
            observed.fetch_add(1, Ordering::SeqCst);
        }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(driver.0.scope.snapshot().unwrap().work, RESPONSIBILITIES);
        driver.run();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(driver.0.scope.snapshot().unwrap().work, ROLES);
        assert_eq!(
            driver.0.ledger.used(DimensionId::new(1).unwrap()),
            ROLES as u64
        );
        work.completed();
        drop(wall_time);
        drop(publisher);
        assert!(driver.0.ledger.is_empty());
        assert!(health.failure().is_none());
    }
    #[test]
    fn an_unused_local_fragment_retires_only_after_all_original_holders_exit() {
        let (roles, driver, _) = fixture();
        drop(roles);
        assert!(driver.0.ledger.is_empty());
        driver.run();
        assert!(driver.0.ledger.is_empty());
    }
    #[test]
    fn a_local_continuation_panic_never_releases_its_original_capacity() {
        let (roles, driver, health) = fixture();
        let (finisher, wall_time, publisher) = roles.into_parts();
        let (permit, mut work) = finisher.into_parts();
        work.bind();
        work.started();
        permit.register(Box::new(|_| panic!("local settlement call exit unknown")));
        driver.run();
        assert!(health.failure().is_some());
        assert!(driver.0.settlement.phase() == ContinuationPhase::Unresolved);
        work.completed();
        drop(wall_time);
        drop(publisher);
        assert_eq!(driver.0.scope.snapshot().unwrap().work, 2);
        assert_eq!(driver.0.ledger.used(DimensionId::new(1).unwrap()), 2);
        assert!(!driver.0.scope.snapshot().unwrap().join_ready);
    }
}
