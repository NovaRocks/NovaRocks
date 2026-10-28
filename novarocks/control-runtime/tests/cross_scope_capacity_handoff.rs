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

//! A funded handoff crosses scopes without making the receiver a child of the
//! original RPC. Actual Work exit, report consumption and header reaping remain
//! distinct facts in the same ledger.
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_role::{ExitAuthority, ExitRole, ExitRolePhase},
    lease::{CapacityBundle, WorkLease, WorkRetirementObserver},
    reported_work::{ReportedWork, ReportedWorkBindingError},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::{ConsumerKind, Consumption},
    identity::{ConsumerId, DimensionId, Generation},
    scope::{WorkAdmission, WorkKind},
};

fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn resources(units: u64) -> ResourceVector {
    ResourceVector::new([(dimension(), units)]).unwrap()
}
fn consumer(n: u64) -> ConsumerId {
    ConsumerId::new(n).unwrap()
}
fn release(mut capacity: CapacityBundle) {
    let held = capacity.resources().unwrap();
    capacity.release(&held).unwrap();
}
struct Original {
    work: WorkLease,
    capacity: CapacityBundle,
    report: ReportSlot,
}
struct Fixture {
    ledger: RuntimeLedger,
    root: ScopeHandle,
    context: ScopeHandle,
    rpc: Option<Original>,
    admission_work: WorkLease,
    admission_capacity: CapacityBundle,
    context_work: WorkLease,
    context_capacity: CapacityBundle,
    context_report: ReportSlot,
    route_empty_capacity: CapacityBundle,
    route: Option<Original>,
}
impl Fixture {
    fn new() -> Self {
        // Exact finite inventory: RPC 3 = local 1 + transferred 2; independent
        // Context admission 1. Two scopes, five reservations, five Work headers.
        let ledger = RuntimeLedger::new(resources(4), 5, 2);
        let root = ledger.open_scope_next(None, generation(), 3, 1).unwrap();
        let (rpc_work, mut rpc_capacity) = root
            .reserve_next(
                generation(),
                resources(3),
                WorkKind::Step,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        let rpc_report =
            ReportSlot::reserve_next(&root, generation(), rpc_work.id(), 0, []).unwrap();
        let (mut admission_work, admission_capacity) = root
            .reserve_next(
                generation(),
                resources(1),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        admission_work.start().unwrap();
        // The receiving scope is derived from this independent admission,
        // never from the RPC that happens to pay its handoff storage.
        let (context, initial) = ledger
            .open_child_scope_with_root(&admission_work, generation(), 2, 2, WorkKind::Business)
            .unwrap();
        let (mut context_work, context_capacity) = initial.bind(WorkKind::Business).unwrap();
        context_work.start().unwrap();
        let context_report =
            ReportSlot::reserve_next(&context, generation(), context_work.id(), 0, []).unwrap();
        let (route_work, route_empty_capacity) = context
            .reserve_child_next(
                generation(),
                context_capacity.id(),
                ResourceVector::default(),
                WorkKind::Settlement,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        let route_report = ReportSlot::reserve_next(
            &context,
            generation(),
            route_work.id(),
            2,
            [
                (consumer(1), ConsumerKind::Observer),
                (consumer(2), ConsumerKind::Observer),
            ],
        )
        .unwrap();
        let mut route_capacity = rpc_capacity.split_next(&resources(2)).unwrap();
        route_capacity.transfer(&route_work).unwrap();
        assert_eq!(ledger.used(dimension()), 4);
        assert_eq!(rpc_capacity.resources().unwrap(), resources(1));
        assert_eq!(route_capacity.resources().unwrap(), resources(2));
        Self {
            ledger,
            root,
            context,
            rpc: Some(Original {
                work: rpc_work,
                capacity: rpc_capacity,
                report: rpc_report,
            }),
            admission_work,
            admission_capacity,
            context_work,
            context_capacity,
            context_report,
            route_empty_capacity,
            route: Some(Original {
                work: route_work,
                capacity: route_capacity,
                report: route_report,
            }),
        }
    }
    fn take_route(&mut self) -> (ExitRole, ExitAuthority, ReportSlot, WorkRetirementObserver) {
        let Original {
            work,
            capacity,
            report,
        } = self.route.take().unwrap();
        let retired = work.retirement_observer();
        let observed_report = report.clone();
        let (role, mut authority) =
            ExitRole::from_admitted(ReportedWork::try_new(work, capacity, report).unwrap());
        authority.start().unwrap();
        (role, authority, observed_report, retired)
    }
    fn finish_rpc(&mut self) -> WorkRetirementObserver {
        let Original {
            work,
            capacity,
            report,
        } = self.rpc.take().unwrap();
        let retired = work.retirement_observer();
        let (role, mut authority) =
            ExitRole::from_admitted(ReportedWork::try_new(work, capacity, report).unwrap());
        authority.start().unwrap();
        authority.exited().unwrap();
        assert!(
            role.actual_exit_ready().unwrap(),
            "the receiving scope is not an RPC child"
        );
        assert!(role.try_retire().unwrap());
        assert_eq!(self.ledger.reap_scope_ready(self.root.id(), 1).unwrap(), 1);
        assert!(retired.is_retired());
        assert_eq!(
            self.root.snapshot().unwrap().work,
            2,
            "only the independent admission and its child link remain"
        );
        retired
    }
    fn finish(mut self) {
        assert!(self.rpc.is_none());
        assert!(self.route.is_none());
        // Even after both invocation legs finish, the real parent admission
        // cannot exit until this separate child scope is removed.
        self.admission_work.body_exited().unwrap();
        self.admission_work.captures_exited().unwrap();
        assert!(!self.admission_work.actual_exit_ready().unwrap());
        self.context_work.body_exited().unwrap();
        self.context_work
            .captures_exited_with_report(&self.context_report)
            .unwrap();
        self.context_report.body_exited().unwrap();
        self.context_report.captures_exited().unwrap();
        assert!(self.context_report.try_retire().unwrap());
        self.context.close_external().unwrap();
        self.ledger.reap_scope_ready(self.context.id(), 2).unwrap();
        assert_eq!(self.context.snapshot().unwrap().work, 0);
        assert!(self.context_work.actual_exit_ready().unwrap());
        release(self.route_empty_capacity);
        release(self.context_capacity);
        self.ledger.remove_scope(self.context.id()).unwrap();
        assert_eq!(self.ledger.reap_scope_ready(self.root.id(), 2).unwrap(), 2);
        assert!(self.admission_work.actual_exit_ready().unwrap());
        release(self.admission_capacity);
        self.root.close_external().unwrap();
        self.ledger.remove_scope(self.root.id()).unwrap();
        assert_eq!(self.ledger.used(dimension()), 0);
        assert!(self.ledger.unresolved_work().is_empty());
        assert!(self.ledger.is_empty());
    }
}

#[test]
fn the_rpc_retires_independently_while_the_transferred_capacity_waits_for_the_receivers_report() {
    let mut f = Fixture::new();
    let (role, authority, report, retired) = f.take_route();
    let rpc_retired = f.finish_rpc();
    assert!(rpc_retired.is_retired());
    assert_eq!(role.phase(), ExitRolePhase::Started);
    assert!(!role.actual_exit_ready().unwrap());
    assert!(!role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 3);
    authority.exited().unwrap();
    assert!(role.actual_exit_ready().unwrap());
    assert_eq!(
        report.consumption(consumer(1)).unwrap(),
        Consumption::Pending
    );
    assert_eq!(
        report.consumption(consumer(2)).unwrap(),
        Consumption::Pending
    );
    // Reaping this actual exited header is still not permission to reclaim
    // its report's funding while either independent consumer is outstanding.
    assert_eq!(f.ledger.reap_scope_ready(f.context.id(), 1).unwrap(), 1);
    assert!(retired.is_retired());
    assert!(!role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 3);
    report.consume(consumer(1)).unwrap();
    assert_eq!(
        report.consumption(consumer(1)).unwrap(),
        Consumption::Consumed
    );
    assert!(!role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 3);
    report.consume(consumer(2)).unwrap();
    assert!(role.try_retire().unwrap());
    assert_eq!(role.phase(), ExitRolePhase::Retired);
    assert_eq!(
        f.ledger.used(dimension()),
        1,
        "only the actual independent admission remains funded"
    );
    f.finish();
}

#[test]
fn refused_binding_after_transfer_preserves_each_original_owner_until_the_correct_handoff() {
    let mut f = Fixture::new();
    let Original {
        work: route_work,
        capacity: route_capacity,
        report: route_report,
    } = f.route.take().unwrap();
    let Original {
        work: rpc_work,
        capacity: rpc_capacity,
        report: rpc_report,
    } = f.rpc.take().unwrap();
    let route_id = route_work.id();
    let reservation = route_capacity.id();
    let rpc_id = rpc_work.id();
    let rpc_report_observed = rpc_report.clone();
    let root_before = f.root.snapshot().unwrap();
    let context_before = f.context.snapshot().unwrap();
    let wrong_scope = ReportedWork::try_new(rpc_work, route_capacity, rpc_report).unwrap_err();
    assert_eq!(wrong_scope.error, ReportedWorkBindingError::WrongScope);
    assert_eq!(wrong_scope.work.id(), rpc_id);
    assert_eq!(wrong_scope.capacity.id(), reservation);
    assert!(wrong_scope.report.same_slot(&rpc_report_observed));
    f.rpc = Some(Original {
        work: wrong_scope.work,
        capacity: rpc_capacity,
        report: wrong_scope.report,
    });
    let wrong_report =
        ReportedWork::try_new(route_work, wrong_scope.capacity, f.context_report.clone())
            .unwrap_err();
    assert_eq!(
        wrong_report.error,
        ReportedWorkBindingError::ReportProducerMismatch
    );
    assert_eq!(wrong_report.work.id(), route_id);
    assert_eq!(wrong_report.capacity.id(), reservation);
    assert!(wrong_report.report.same_slot(&f.context_report));
    assert_eq!(wrong_report.capacity.resources().unwrap(), resources(2));
    assert_eq!(f.ledger.used(dimension()), 4);
    assert_eq!(f.root.snapshot().unwrap(), root_before);
    assert_eq!(f.context.snapshot().unwrap(), context_before);
    assert!(f.ledger.unresolved_work().is_empty());
    f.route = Some(Original {
        work: wrong_report.work,
        capacity: wrong_report.capacity,
        report: route_report,
    });
    // This returned handle still identifies the lifetime's original report;
    // dropping the redundant observation neither fills nor retires that slot.
    drop(wrong_report.report);
    let (role, authority, report, _) = f.take_route();
    f.finish_rpc();
    authority.exited().unwrap();
    assert!(!role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 3);
    report.consume(consumer(1)).unwrap();
    assert!(!role.try_retire().unwrap());
    report.consume(consumer(2)).unwrap();
    assert!(role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    f.finish();
}
