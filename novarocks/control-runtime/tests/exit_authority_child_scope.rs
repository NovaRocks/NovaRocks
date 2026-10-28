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
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ResourceVector},
    completion::{CompletionError, ConsumerKind},
    identity::{ConsumerId, DimensionId, Generation},
    scope::{ScopeError, WorkAdmission, WorkKind},
};
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn paid() -> ResourceVector {
    ResourceVector::new([(dimension(), 3)]).unwrap()
}
struct Parent {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    role: ExitRole,
    authority: Option<ExitAuthority>,
    report: ReportSlot,
    consumer: ConsumerId,
}
impl Parent {
    fn new(scopes: usize, reservations: usize, parent_work: usize) -> Self {
        let ledger = RuntimeLedger::new(paid(), reservations, scopes);
        let scope = ledger
            .open_scope_next(None, generation(), parent_work, 1)
            .unwrap();
        let (work, capacity) = scope
            .reserve_next(
                generation(),
                paid(),
                WorkKind::Step,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &scope,
            generation(),
            work.id(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (role, authority) =
            ExitRole::from_admitted(ReportedWork::try_new(work, capacity, report.clone()).unwrap());
        Self {
            ledger,
            scope,
            role,
            authority: Some(authority),
            report,
            consumer,
        }
    }
    fn finish_child(&self, child: ScopeHandle, root: StepGuard, kind: WorkKind) {
        let (mut work, mut capacity) = root.bind(kind).unwrap();
        // This actual neutral test holder never captured a foreign body.
        child.close_external().unwrap();
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        self.ledger.reap_scope_ready(child.id(), 1).unwrap();
        capacity.release(&ResourceVector::default()).unwrap();
        self.ledger.remove_scope(child.id()).unwrap();
        // The parent link is its own real Work and retires separately.
        self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
    }
    fn finish(mut self) {
        if let Some(authority) = self.authority.take() {
            authority.exited().unwrap();
        }
        if self.report.consumption(self.consumer).unwrap()
            == novarocks_control_runtime_core::completion::Consumption::Pending
        {
            self.report.consume(self.consumer).unwrap();
        }
        assert!(self.role.try_retire().unwrap());
        self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
        self.scope.close_external().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}

#[test]
fn original_parent_exit_and_report_ack_cannot_return_fee_before_its_real_child_scope() {
    for started in [false, true] {
        let mut f = Parent::new(2, 2, 2);
        if started {
            f.authority.as_mut().unwrap().start().unwrap();
        }
        let (child, root) = f
            .authority
            .as_mut()
            .unwrap()
            .open_child_scope_with_root(generation(), 1, 0, WorkKind::Business)
            .unwrap();
        let held_before = f.ledger.used(dimension());
        f.authority.take().unwrap().exited().unwrap();
        f.report.consume(f.consumer).unwrap();
        let phase = f.role.phase();
        let physical_parent = f.role.body_exited() && f.role.captures_exited();
        let ready_while_child = f.role.actual_exit_ready().unwrap();
        let retired_while_child = f.role.try_retire().unwrap();
        let held_after = f.ledger.used(dimension());
        f.finish_child(child, root, WorkKind::Business);
        f.finish();
        assert_eq!(phase, ExitRolePhase::Exited);
        assert!(physical_parent);
        assert!(!ready_while_child);
        assert!(!retired_while_child);
        assert_eq!(held_before, 3);
        assert_eq!(held_after, 3);
    }
}

#[test]
fn ordinary_child_bootstrap_capacity_refusals_preserve_the_original_parent_inventory() {
    // Each refusal is a real current metadata bound, not a fabricated scope ID.
    for (scopes, reservations, parent_work, child_work, expected) in [
        (1, 2, 2, 1, RuntimeError::ScopeCapacity),
        (
            2,
            1,
            2,
            1,
            RuntimeError::Capacity(CapacityError::Scope(ScopeError::SlotCapacity)),
        ),
        (2, 2, 1, 1, RuntimeError::Scope(ScopeError::WorkCapacity)),
        (
            2,
            2,
            2,
            0,
            RuntimeError::Capacity(CapacityError::Scope(ScopeError::WorkCapacity)),
        ),
    ] {
        let mut f = Parent::new(scopes, reservations, parent_work);
        let before = f.scope.snapshot().unwrap();
        let outcome = f.authority.as_mut().unwrap().open_child_scope_with_root(
            generation(),
            child_work,
            0,
            WorkKind::Business,
        );
        let (error, unexpected) = match outcome {
            Err(error) => (Some(error), None),
            Ok(child) => (None, Some(child)),
        };
        let after = f.scope.snapshot().unwrap();
        let fee = f.ledger.used(dimension());
        let report_unfilled = f.report.consume(f.consumer)
            == Err(RuntimeError::Completion(CompletionError::NotFilled));
        if let Some((scope, root)) = unexpected {
            f.finish_child(scope, root, WorkKind::Business);
        }
        f.finish();
        assert_eq!(error, Some(ExitRoleError::Runtime(expected)));
        assert_eq!(after, before);
        assert_eq!(fee, 3);
        assert!(report_unfilled);
    }
}

#[test]
fn original_authority_derives_after_external_close_but_not_new_business_after_stop() {
    let mut f = Parent::new(3, 3, 3);
    f.authority.as_mut().unwrap().start().unwrap();
    f.ledger.close_admission();
    f.scope.close_external().unwrap();
    let (child, root) = f
        .authority
        .as_mut()
        .unwrap()
        .open_child_scope_with_root(generation(), 1, 0, WorkKind::Step)
        .unwrap();
    let inherited_closed = !child.gates().unwrap().external_open;
    f.scope.request_stop().unwrap();
    let before = f.scope.snapshot().unwrap();
    let rejected = f.authority.as_mut().unwrap().open_child_scope_with_root(
        generation(),
        1,
        0,
        WorkKind::Business,
    );
    let error = match rejected {
        Err(error) => Some(error),
        Ok((scope, guard)) => {
            f.finish_child(scope, guard, WorkKind::Business);
            None
        }
    };
    let unchanged = f.scope.snapshot().unwrap() == before;
    f.finish_child(child, root, WorkKind::Step);
    f.finish();
    assert!(inherited_closed);
    assert_eq!(
        error,
        Some(ExitRoleError::Runtime(RuntimeError::Scope(
            ScopeError::Stopped
        )))
    );
    assert!(unchanged);
}
