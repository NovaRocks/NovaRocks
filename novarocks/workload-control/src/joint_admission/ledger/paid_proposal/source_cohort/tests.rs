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

use super::*;
use crate::{CancellationReason, RootWork, WorkClass};

fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..10000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("cohort proof and original reaper must reach finite idle");
}
fn receipt(admission: &mut JointAdmission) -> JointAdjustmentReceipt {
    let Poll::Ready(Ok(batch)) = Pin::new(admission).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("original proof must grant this cohort");
    };
    assert!(batch.new_claims.is_empty());
    batch.adjustment.unwrap()
}
fn fixture(children: usize) -> (WorkloadControl, JointProgressDriver, RootWork) {
    let (control, _, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(8, children, 1, 3);
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let mut proposal = root.owner.scope().begin_control_binding().unwrap();
    proposal
        .push_dependency(&SharedDependency::LocalHost)
        .unwrap();
    proposal.end_change().unwrap();
    let mut bind = proposal.submit().unwrap();
    drive(&mut driver);
    receipt(&mut bind)
        .install_control_scope_in(&root.owner.scope())
        .unwrap()
        .notify();
    drop(bind);
    drive(&mut driver);
    (control, driver, root)
}
fn cohort(scope: &WorkScope, driver: &mut JointProgressDriver, count: usize) -> SourceCohort {
    let mut admission = scope
        .request_source_cohort(NonZeroUsize::new(count).unwrap())
        .unwrap();
    drive(driver);
    let mut paid = receipt(&mut admission);
    let mut installed = None;
    paid.install_source_cohort_in(scope, &mut installed)
        .unwrap();
    assert!(paid.id.is_none());
    installed.unwrap()
}
fn finish(control: WorkloadControl, mut driver: JointProgressDriver, root: RootWork) {
    root.business.release();
    root.owner.complete_after_terminal_cancel_settled();
    control.close_admission();
    drive(&mut driver);
    assert!(
        control.shutdown().is_ok(),
        "all original custody must really drain"
    );
}

#[test]
fn complete_cohort_is_paid_before_effect_and_children_keep_independent_actual_lifetimes() {
    let (control, mut driver, root) = fixture(3);
    let scope = root.owner.scope();
    let mut paid = cohort(&scope, &mut driver, 3);
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.nodes.len(), 1, "reservation opens no child or Source");
        let ledger = state.joint.as_ref().unwrap();
        let request = ledger.requests.get(&paid.receipt.id.unwrap()).unwrap();
        assert_eq!(request.changes.len(), 3);
        for change in request.changes.iter() {
            let source = change.source.as_ref().unwrap();
            let claim = &ledger.claims[&source.id];
            assert_eq!(
                claim.active_facts.dependencies[0].fact,
                SharedDependency::LocalHost
            );
            assert_eq!(claim.active_amounts.get(JointResource::ScopeRecords), 1);
            assert_eq!(claim.pending, paid.receipt.id);
        }
    }
    let mut first = None;
    paid.take_child_in(&scope, &mut first)
        .unwrap()
        .unwrap()
        .notify();
    let first = first.unwrap();
    let first_view = first.scope().cancellation().unwrap();
    first.cancel(CancellationReason::Requested);
    let mut second = None;
    paid.take_child_in(&scope, &mut second)
        .unwrap()
        .unwrap()
        .notify();
    let second = second.unwrap();
    assert!(second.scope().cancellation().unwrap().reason().is_none());
    assert_eq!(paid.remaining(), 1);
    drop(paid); // Exactly one never-used slot/edge returns, not either live child.
    first.complete_after_terminal_cancel_settled();
    second.complete();
    drive(&mut driver);
    assert!(
        !control.inner.state.lock().unwrap().cancellations.drained(),
        "a retained child view keeps the actual original backing"
    );
    drop(first_view);
    finish(control, driver, root);
}

#[test]
fn queued_cancellation_returns_every_uncommitted_slot_and_edge_once() {
    let (control, mut driver, root) = fixture(3);
    let scope = root.owner.scope();
    let mut pending = scope
        .request_source_cohort(NonZeroUsize::new(3).unwrap())
        .unwrap();
    root.owner.cancel(CancellationReason::Requested);
    drive(&mut driver);
    assert!(matches!(
        Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Cancelled(_)
        }))
    ));
    drop(pending);
    finish(control, driver, root);
}

#[test]
fn granted_cancellation_keeps_the_same_positive_liability_until_receipt_exit() {
    let (control, mut driver, root) = fixture(3);
    let scope = root.owner.scope();
    let mut paid = cohort(&scope, &mut driver, 3);
    let original = paid.receipt.id.unwrap();
    root.owner.cancel(CancellationReason::Requested);
    drive(&mut driver);
    let mut destination = None;
    assert!(matches!(
        paid.take_child_in(&scope, &mut destination),
        Err(WorkError::Cancelled(_))
    ));
    assert!(destination.is_none());
    assert_eq!(paid.receipt.id, Some(original));
    {
        let state = control.inner.state.lock().unwrap();
        let request = &state.joint.as_ref().unwrap().requests[&original];
        assert!(request.changes.iter().all(|change| {
            let ClaimSource::PoolFree { slot, .. } = change.source.as_ref().unwrap().source else {
                return false;
            };
            matches!(
                state.cancellations.slots[slot.0].phase,
                SlotPhase::ChildGranted { .. }
            )
        }));
    }
    drop(paid);
    finish(control, driver, root);
}

#[test]
fn construction_failure_rolls_back_partial_slot_and_parent_edge_reservations() {
    let (control, mut driver, root) = fixture(2);
    let scope = root.owner.scope();
    // Eight original scope records: Root already holds one, so the eighth
    // child fails at the real global inventory after seven exact reservations.
    assert!(matches!(
        scope.request_source_cohort(NonZeroUsize::new(8).unwrap()),
        Err(JointAdmissionFailure {
            error: WorkError::Capacity("fixed control slots")
        })
    ));
    {
        let state = control.inner.state.lock().unwrap();
        assert!(state.cancellations.free_inventory_valid());
        assert!(state.joint.as_ref().unwrap().requests.is_empty());
    }
    drive(&mut driver);
    let paid = cohort(&scope, &mut driver, 7);
    assert_eq!(paid.remaining(), 7);
    drop(paid);
    finish(control, driver, root);
}

#[test]
fn cohort_exact_reservations_are_not_stolen_by_an_independent_ordinary_child() {
    let (control, mut driver, root) = fixture(2);
    let scope = root.owner.scope();
    let mut paid = cohort(&scope, &mut driver, 2);
    let reserved: Vec<_> = {
        let state = control.inner.state.lock().unwrap();
        state.joint.as_ref().unwrap().requests[&paid.receipt.id.unwrap()]
            .changes
            .iter()
            .map(|c| match c.source.as_ref().unwrap().source {
                ClaimSource::PoolFree { slot, .. } => slot,
                _ => panic!("exact original child"),
            })
            .collect()
    };
    let mut ordinary = scope.request_joint_child().unwrap();
    drive(&mut driver);
    let mut ordinary = receipt(&mut ordinary);
    let mut destination = None;
    ordinary
        .install_child_in(&scope, &mut destination)
        .unwrap()
        .notify();
    let ordinary = destination.unwrap();
    let ordinary_slot =
        control.inner.state.lock().unwrap().nodes[&ordinary.scope().id()].cancellation;
    assert!(!reserved.contains(&ordinary_slot));
    assert_eq!(paid.remaining(), 2);
    let mut first = None;
    paid.take_child_in(&scope, &mut first)
        .unwrap()
        .unwrap()
        .notify();
    let first = first.unwrap();
    assert!(
        reserved
            .contains(&control.inner.state.lock().unwrap().nodes[&first.scope().id()].cancellation)
    );
    assert_eq!(paid.remaining(), 1);
    ordinary.complete();
    first.complete();
    drop(paid);
    finish(control, driver, root);
}

#[test]
fn failed_install_keeps_original_slot_edge_and_receipt_armed_for_retry() {
    let (control, mut driver, root) = fixture(1);
    let scope = root.owner.scope();
    let mut paid = cohort(&scope, &mut driver, 1);
    let original_id = control.inner.state.lock().unwrap().next_id;
    control.inner.state.lock().unwrap().next_id = u64::MAX;
    let mut destination = None;
    assert!(matches!(
        paid.take_child_in(&scope, &mut destination),
        Err(WorkError::ArithmeticOverflow)
    ));
    assert!(destination.is_none());
    assert_eq!(paid.remaining(), 1);
    control.inner.state.lock().unwrap().next_id = original_id;
    paid.take_child_in(&scope, &mut destination)
        .unwrap()
        .unwrap()
        .notify();
    destination.unwrap().complete();
    assert_eq!(paid.remaining(), 0);
    assert!(paid.take_child_in(&scope, &mut None).unwrap().is_none());
    drop(paid);
    finish(control, driver, root);
}

#[test]
fn complete_cohort_beyond_former_change_bound_keeps_all_original_reservations() {
    let (control, mut driver, root) = fixture(4);
    let scope = root.owner.scope();
    let mut paid = cohort(&scope, &mut driver, 4);
    assert_eq!(paid.remaining(), 4);
    let mut children = Vec::new();
    for remaining in (0..4).rev() {
        let mut child = None;
        paid.take_child_in(&scope, &mut child)
            .unwrap()
            .unwrap()
            .notify();
        children.push(child.unwrap());
        assert_eq!(paid.remaining(), remaining);
    }
    let ids: std::collections::BTreeSet<_> =
        children.iter().map(|child| child.scope().id()).collect();
    assert_eq!(ids.len(), 4);
    drop(paid);
    for child in children {
        child.complete();
    }
    finish(control, driver, root);
}

#[test]
fn cohort_has_no_q_gate_and_cannot_use_the_ordinary_child_installer() {
    let (control, mut driver, root) = fixture(3);
    let scope = root.owner.scope();
    let first = scope.begin_control_binding().unwrap();
    let second = scope.begin_control_binding().unwrap();
    let unstarted = match scope
        .try_request_source_cohort(NonZeroUsize::new(3).unwrap())
        .unwrap()
    {
        JointTryBegin::Started(value) => value,
        JointTryBegin::Full(_) => panic!("there is no Q inventory gate"),
    };
    drop(unstarted);
    drop(first);
    drop(second);
    drive(&mut driver);
    let mut admission = scope
        .request_source_cohort(NonZeroUsize::new(1).unwrap())
        .unwrap();
    drive(&mut driver);
    let mut receipt = receipt(&mut admission);
    assert!(matches!(
        receipt.install_child_in(&scope, &mut None),
        Err(WorkError::Conflict)
    ));
    assert!(receipt.id.is_some());
    let mut installed = None;
    receipt
        .install_source_cohort_in(&scope, &mut installed)
        .unwrap();
    drop(installed);
    finish(control, driver, root);
}

#[test]
fn queued_drop_returns_all_original_reservations_without_starting_an_effect() {
    let (control, mut driver, root) = fixture(3);
    let scope = root.owner.scope();
    let pending = scope
        .request_source_cohort(NonZeroUsize::new(3).unwrap())
        .unwrap();
    drop(pending);
    drive(&mut driver);
    let paid = cohort(&scope, &mut driver, 3);
    drop(paid);
    finish(control, driver, root);
}

#[test]
fn partial_cohort_cancellation_keeps_taken_child_and_returns_only_unused_reservations() {
    let (control, mut driver, root) = fixture(3);
    let scope = root.owner.scope();
    let mut paid = cohort(&scope, &mut driver, 3);
    let mut child = None;
    paid.take_child_in(&scope, &mut child)
        .unwrap()
        .unwrap()
        .notify();
    let child = child.unwrap();
    let view = child.scope().cancellation().unwrap();
    root.owner.cancel(CancellationReason::Requested);
    drive(&mut driver);
    assert!(matches!(
        paid.take_child_in(&scope, &mut None),
        Err(WorkError::Cancelled(_))
    ));
    drop(paid);
    assert_eq!(view.reason(), Some(CancellationReason::Requested));
    child.complete_after_terminal_cancel_settled();
    drop(view);
    finish(control, driver, root);
}

#[test]
fn cancelled_cohort_returns_only_uncommitted_edges_before_failed_ticket_is_received() {
    let (control, mut driver, root) = fixture(3);
    let scope = root.owner.scope();
    let mut roots = Vec::new();
    for _ in 0..4 {
        let extra = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let mut p = extra.owner.scope().begin_control_binding().unwrap();
        p.push_dependency(&SharedDependency::LocalHost).unwrap();
        p.end_change().unwrap();
        let mut bind = p.submit().unwrap();
        drive(&mut driver);
        receipt(&mut bind)
            .install_control_scope_in(&extra.owner.scope())
            .unwrap()
            .notify();
        roots.push(extra);
    }
    let mut cancelled = scope
        .request_source_cohort(NonZeroUsize::new(2).unwrap())
        .unwrap();
    root.owner.cancel(crate::CancellationReason::Requested);
    drive(&mut driver);
    {
        let state = control.inner.state.lock().unwrap();
        assert!(matches!(
            state.joint.as_ref().unwrap().requests[&cancelled.id.unwrap()].phase,
            RequestPhase::Failed(WorkError::Cancelled(crate::CancellationReason::Requested))
        ));
    }
    let same_returned_inventory = cohort(&roots[0].owner.scope(), &mut driver, 2);
    drop(same_returned_inventory);
    assert!(matches!(
        Pin::new(&mut cancelled).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Cancelled(crate::CancellationReason::Requested)
        }))
    ));
    drop(cancelled);
    for extra in roots {
        extra.business.release();
        extra.owner.complete();
    }
    finish(control, driver, root);
}
