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
    exit_custody::{ExitCustody, ExitCustodyError, ExitCustodyPhase, ExitObligation},
    obligation::StepRegistration,
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::{CompletionError, ConsumerKind},
    identity::{ConsumerId, DimensionId, Generation},
    owned_storage::OwnedSlotNeed,
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
    exit_custody: ExitCustody,
    authority: Option<ExitObligation>,
    report: ReportSlot,
    consumer: ConsumerId,
}
impl Parent {
    fn new() -> Self {
        let ledger = RuntimeLedger::new(paid());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (work, mut capacity) = scope
            .reserve_next(
                generation(),
                ReservationResources::new(
                    paid(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..OwnedSlotNeed::default()
                    },
                ),
                WorkKind::Step,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (exit_custody, authority) = ExitCustody::from_admitted(
            ReportedWork::try_new(work, capacity, report.clone()).unwrap(),
        );
        Self {
            ledger,
            scope,
            exit_custody,
            authority: Some(authority),
            report,
            consumer,
        }
    }
    fn finish_child(&self, child: ScopeHandle, root: StepRegistration, kind: WorkKind) {
        let (mut work, mut capacity) = root.bind(kind).unwrap();
        // This actual neutral test holder never captured a foreign body.
        child.close_external().unwrap();
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        assert_eq!(self.ledger.reap_scope_ready(child.id(), 1).unwrap(), 1);
        capacity.finish().unwrap();
        assert!(child.snapshot().unwrap().retirement_ready);
        let parent_work = self.scope.snapshot().unwrap().work;
        self.ledger.remove_scope(child.id()).unwrap();
        // Actual child removal retires its intrinsic parent link in the same
        // transaction. Only the original parent's own exit remains for finish.
        assert_eq!(self.scope.snapshot().unwrap().work, parent_work - 1);
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
        assert!(self.exit_custody.try_retire().unwrap());
        self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
        self.scope.close_external().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}

#[test]
fn original_parent_exit_and_report_ack_cannot_return_fee_before_its_real_child_scope() {
    for started in [false, true] {
        let mut f = Parent::new();
        if started {
            f.authority.as_mut().unwrap().start().unwrap();
        }
        let (child, root) = f
            .authority
            .as_mut()
            .unwrap()
            .open_child_scope_with_root(
                generation(),
                WorkKind::Business,
                ReservationResources::work_only(),
            )
            .unwrap();
        let held_before = f.ledger.used(dimension());
        f.authority.take().unwrap().exited().unwrap();
        f.report.consume(f.consumer).unwrap();
        let phase = f.exit_custody.phase();
        let physical_parent = f.exit_custody.body_exited() && f.exit_custody.captures_exited();
        let ready_while_child = f.exit_custody.actual_exit_ready().unwrap();
        let retired_while_child = f.exit_custody.try_retire().unwrap();
        let held_after = f.ledger.used(dimension());
        f.finish_child(child, root, WorkKind::Business);
        f.finish();
        assert_eq!(phase, ExitCustodyPhase::Exited);
        assert!(physical_parent);
        assert!(!ready_while_child);
        assert!(!retired_while_child);
        assert_eq!(held_before, 3);
        assert_eq!(held_after, 3);
    }
}

#[test]
fn original_authority_keeps_dynamic_children_under_the_same_report_and_funding() {
    let mut f = Parent::new();
    let mut children = Vec::new();
    for _ in 0..16 {
        children.push(
            f.authority
                .as_mut()
                .unwrap()
                .open_child_scope_with_root(
                    generation(),
                    WorkKind::Business,
                    ReservationResources::work_only(),
                )
                .unwrap(),
        );
    }
    assert_eq!(f.scope.snapshot().unwrap().work, 17);
    assert_eq!(f.ledger.used(dimension()), 3);
    assert_eq!(
        f.report.consume(f.consumer),
        Err(RuntimeError::Completion(CompletionError::NotFilled)),
    );
    for (scope, root) in children {
        f.finish_child(scope, root, WorkKind::Business);
    }
    assert_eq!(f.scope.snapshot().unwrap().work, 1);
    f.finish();
}

#[test]
fn original_authority_derives_after_external_close_but_not_new_business_after_stop() {
    let mut f = Parent::new();
    f.authority.as_mut().unwrap().start().unwrap();
    f.ledger.close_admission();
    f.scope.close_external().unwrap();
    let (child, root) = f
        .authority
        .as_mut()
        .unwrap()
        .open_child_scope_with_root(
            generation(),
            WorkKind::Step,
            ReservationResources::work_only(),
        )
        .unwrap();
    let inherited_closed = !child.gates().unwrap().external_open;
    f.scope.request_stop().unwrap();
    let before = f.scope.snapshot().unwrap();
    let rejected = f.authority.as_mut().unwrap().open_child_scope_with_root(
        generation(),
        WorkKind::Business,
        ReservationResources::work_only(),
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
        Some(ExitCustodyError::Runtime(RuntimeError::Scope(
            ScopeError::Stopped
        )))
    );
    assert!(unchanged);
}
