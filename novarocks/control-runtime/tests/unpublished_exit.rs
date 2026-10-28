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

#![cfg(not(loom))]

use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::StepGuard,
    owned_return::{OwnedReturnReader, OwnedReturnSender},
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::{CompletionError, ConsumerKind, Consumption, ReturnState},
    identity::{ConsumerId, DimensionId, Generation},
    scope::{WorkAdmission, WorkKind},
};
use std::mem::ManuallyDrop;

fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn resources() -> ResourceVector {
    ResourceVector::new([(dimension(), 3)]).unwrap()
}
fn consumers() -> [ConsumerId; 2] {
    [ConsumerId::new(1).unwrap(), ConsumerId::new(2).unwrap()]
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    role: ExitRole,
    authority: Option<ExitAuthority>,
    report: ReportSlot,
    sender: Option<OwnedReturnSender<usize>>,
    reader: OwnedReturnReader<usize>,
}
impl Fixture {
    fn new() -> Self {
        let ledger = RuntimeLedger::new(resources(), 2, 2);
        let scope = ledger.open_scope_next(None, generation(), 2, 1).unwrap();
        let (work, capacity) = scope
            .reserve_next(
                generation(),
                resources(),
                WorkKind::Step,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        let [owner, observer] = consumers();
        let report = ReportSlot::reserve_next(
            &scope,
            generation(),
            work.id(),
            2,
            [
                (
                    owner,
                    ConsumerKind::Owner {
                        scope: scope.id(),
                        generation: generation(),
                    },
                ),
                (observer, ConsumerKind::Observer),
            ],
        )
        .unwrap();
        let (sender, reader) = report.claim_owned_return().unwrap();
        let (role, authority) =
            ExitRole::from_admitted(ReportedWork::try_new(work, capacity, report.clone()).unwrap());
        Self {
            ledger,
            scope,
            role,
            authority: Some(authority),
            report,
            sender: Some(sender),
            reader,
        }
    }
    fn pending_consumers(&self) -> [Consumption; 2] {
        consumers().map(|consumer| self.report.consumption(consumer).unwrap())
    }
    fn start(&mut self) {
        self.authority.as_mut().unwrap().start().unwrap();
    }
    fn withdraw(&mut self) -> Result<(), ExitRoleError> {
        self.authority
            .take()
            .unwrap()
            .withdraw_unpublished_and_exit()
    }
    fn finish_child(&self, child: ScopeHandle, root: StepGuard) {
        // This actual neutral child never captured or invoked a foreign body.
        let (mut work, mut capacity) = root.bind(WorkKind::Cleanup).unwrap();
        child.close_external().unwrap();
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        assert_eq!(self.ledger.reap_scope_ready(child.id(), 1).unwrap(), 1);
        capacity.release(&ResourceVector::default()).unwrap();
        self.ledger.remove_scope(child.id()).unwrap();
        assert_eq!(self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap(), 1);
    }
    fn finish(self) {
        assert!(self.role.try_retire().unwrap());
        assert_eq!(self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap(), 1);
        self.scope.close_external().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}

#[test]
fn started_unpublished_exit_keeps_the_original_return_and_real_child_retirement_gates() {
    let mut f = Fixture::new();
    let (child, root) = f
        .authority
        .as_mut()
        .unwrap()
        .open_child_scope_with_root(generation(), 1, 0, WorkKind::Cleanup)
        .unwrap();
    f.start();
    f.sender.take().unwrap().publish(29).unwrap();
    f.withdraw().unwrap();
    let phase = f.role.phase();
    let body = f.role.body_exited();
    let captures = f.role.captures_exited();
    let return_before_take = f.reader.state().unwrap();
    let consumers = f.pending_consumers();
    let report_blocked_by_payload = !f.report.try_retire().unwrap();
    let parent_blocked_before_take = !f.role.try_retire().unwrap();
    let held_before_take = f.ledger.used(dimension());
    let returned = f.reader.try_take().unwrap();
    let parent_blocked_by_child =
        !f.role.actual_exit_ready().unwrap() && !f.role.try_retire().unwrap();
    let held_before_child = f.ledger.used(dimension());
    f.finish_child(child, root);
    f.finish();
    assert_eq!(phase, ExitRolePhase::Exited);
    assert!(body && captures);
    assert_eq!(return_before_take, ReturnState::Filled);
    assert_eq!(consumers, [Consumption::Pending; 2]);
    assert!(report_blocked_by_payload && parent_blocked_before_take && parent_blocked_by_child);
    assert_eq!(held_before_take, 3);
    assert_eq!(held_before_child, 3);
    assert_eq!(returned, Some(29));
}

#[test]
fn taken_or_unused_return_allows_unpublished_exit_without_acknowledging_consumers() {
    for started in [false, true] {
        for taken in [false, true] {
            let mut f = Fixture::new();
            if started {
                f.start();
            }
            if taken {
                f.sender.take().unwrap().publish(31).unwrap();
                assert_eq!(f.reader.try_take().unwrap(), Some(31));
            } else {
                f.sender.take().unwrap().revoke_unused().unwrap();
                assert!(f.reader.try_take().unwrap().is_none());
            }
            f.withdraw().unwrap();
            let phase = f.role.phase();
            let consumers = f.pending_consumers();
            let ordinary_report_is_revoked =
                f.report.fill() == Err(RuntimeError::Completion(CompletionError::AlreadyFilled));
            f.finish();
            assert_eq!(phase, ExitRolePhase::Exited);
            assert_eq!(consumers, [Consumption::Pending; 2]);
            assert!(ordinary_report_is_revoked);
        }
    }
}

#[test]
fn published_plain_report_refuses_withdrawal_without_inventing_actual_exit() {
    let mut f = Fixture::new();
    f.start();
    f.sender.take().unwrap().publish(37).unwrap();
    assert_eq!(f.reader.try_take().unwrap(), Some(37));
    f.report.fill().unwrap();
    let refusal = f.withdraw();
    assert_eq!(
        refusal,
        Err(ExitRoleError::Runtime(RuntimeError::Completion(
            CompletionError::InvalidRevocation
        )))
    );
    assert_eq!(f.role.phase(), ExitRolePhase::Unresolved);
    assert!(!f.role.body_exited());
    assert!(!f.role.captures_exited());
    assert_eq!(f.pending_consumers(), [Consumption::Pending; 2]);
    assert!(!f.role.actual_exit_ready().unwrap());
    assert!(!f.role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 3);
    // The consumed authority failed, so its exact paid custody stays Unknown.
    // No cleanup helper is allowed to manufacture the missing exit witness.
    let _original = ManuallyDrop::new(f);
}

#[test]
fn the_never_started_unused_api_still_refuses_a_started_holder() {
    let mut f = Fixture::new();
    f.start();
    f.sender.take().unwrap().revoke_unused().unwrap();
    assert!(f.reader.try_take().unwrap().is_none());
    let refusal = f.authority.take().unwrap().revoke_unused_and_exit();
    assert_eq!(
        refusal,
        Err(ExitRoleError::InvalidPhase(ExitRolePhase::Started))
    );
    assert_eq!(f.role.phase(), ExitRolePhase::Unresolved);
    assert!(!f.role.body_exited());
    assert!(!f.role.captures_exited());
    assert_eq!(f.pending_consumers(), [Consumption::Pending; 2]);
    assert!(!f.report.try_retire().unwrap());
    assert!(!f.role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 3);
    let _original = ManuallyDrop::new(f);
}
