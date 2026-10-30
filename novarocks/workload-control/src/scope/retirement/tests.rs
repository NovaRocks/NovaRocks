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

//! Observe real reset, retained aliases, reuse, and exhausted generations.

use super::*;
use crate::joint_admission::{JointAdmission, JointProgress, JointProgressDriver, JointResources};
use novarocks_resource_contract::{SelectedNetworkOrigin, SharedDependency};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..10000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original storage reaper did not reach finite idle");
}
fn root(control: &WorkloadControl) -> RootWork {
    control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap()
}
fn install(request: &mut JointAdmission, owner: &WorkOwner) {
    let Poll::Ready(Ok(mut batch)) =
        Pin::new(request).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("original control adjustment reply");
    };
    assert!(batch.new_claims.is_empty());
    batch
        .adjustment
        .take()
        .unwrap()
        .install_control(owner, &[])
        .unwrap();
}

#[test]
fn an_observer_does_not_pin_backing_and_does_not_confuse_node_exit_with_reset() {
    let (control, _, mut driver) = fixed_control_storage_tests::prototype_profile(2, 1, 1, 1);
    let work = root(&control);
    let observer = work.owner.retirement_observer().unwrap();
    let view = work.owner.scope().cancellation().unwrap();
    let raw = Arc::clone(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .cancellations
            .cell(observer.slot),
    );
    let id = work.owner.scope().id();
    assert_eq!(observer.is_reclaimed(), Ok(false));
    work.business.release();
    work.owner.complete();
    assert!(!control.inner.state.lock().unwrap().nodes.contains_key(&id));
    drive(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(false));
    drop(view);
    drive(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(false));
    drop(raw);
    // Raw Arc is a private fixture edge; production reference exits use the
    // original typed last-exit notification, not a new observer channel.
    control.inner.notify_reclaim_progress();
    drive(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(true));
    assert!(control.inner.state.lock().unwrap().cancellations.drained());
}

#[test]
fn an_old_observer_recognizes_reset_even_after_the_same_slot_has_been_reused() {
    let (control, _, mut driver) = fixed_control_storage_tests::prototype_profile(2, 1, 1, 1);
    let first = root(&control);
    let old = first.owner.retirement_observer().unwrap();
    first.business.release();
    first.owner.complete();
    drive(&mut driver);
    // Deliberately miss the Free interval before the old observer's next read.
    let second = root(&control);
    let new = second.owner.retirement_observer().unwrap();
    assert_eq!(old.slot, new.slot);
    assert_ne!(old.generation, new.generation);
    assert_eq!(old.is_reclaimed(), Ok(true));
    assert_eq!(new.is_reclaimed(), Ok(false));
    second.business.release();
    second.owner.complete();
    drive(&mut driver);
    assert_eq!(new.is_reclaimed(), Ok(true));
}

#[test]
fn exhausted_generation_still_reports_real_free_without_wrapping() {
    let (control, _, mut driver) = fixed_control_storage_tests::prototype_profile(2, 1, 1, 1);
    let work = root(&control);
    let slot = control.inner.state.lock().unwrap().nodes[&work.owner.scope().id()].cancellation;
    // Reach the supported exhaustion boundary before creating the observer;
    // changing generation after mint would not model a production transition.
    control.inner.state.lock().unwrap().cancellations.slots[slot.0].generation = u64::MAX;
    let observer = work.owner.retirement_observer().unwrap();
    work.business.release();
    work.owner.complete();
    assert_eq!(observer.is_reclaimed(), Ok(false));
    drive(&mut driver);
    let state = control.inner.state.lock().unwrap();
    assert_eq!(state.cancellations.slots[slot.0].generation, u64::MAX);
    assert_eq!(state.cancellations.slots[slot.0].phase, SlotPhase::Free);
    drop(state);
    assert_eq!(observer.is_reclaimed(), Ok(true));
    assert!(matches!(
        control.try_begin_root(WorkRequest::new(WorkClass::Query)),
        Err(WorkError::ArithmeticOverflow)
    ));
    assert_eq!(observer.is_reclaimed(), Ok(true));
    assert!(control.inner.state.lock().unwrap().cancellations.drained());
}

#[test]
fn first_bind_grow_and_cancellation_do_not_masquerade_as_control_reset() {
    let (control, handle, mut driver) = fixed_control_storage_tests::prototype_profile(3, 1, 2, 1);
    let work = root(&control);
    let observer = work.owner.retirement_observer().unwrap();
    let mut first = handle.begin_paid_proposal(&work.owner).unwrap();
    first
        .begin_first_bind(work.owner.control_claim_view().unwrap())
        .unwrap();
    first.push_dependency(&SharedDependency::LocalHost).unwrap();
    first.end_change().unwrap();
    let mut first = first.submit().unwrap();
    drive(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(false));
    install(&mut first, &work.owner);
    drop(first);
    drive(&mut driver);
    let selected = url::Url::parse("https://retirement.example/actual/selected/route").unwrap();
    let mut grow = handle.begin_paid_proposal(&work.owner).unwrap();
    grow.begin_grow(
        work.owner.control_claim_view().unwrap(),
        JointResources::ZERO,
    )
    .unwrap();
    grow.push_selected_network(SelectedNetworkOrigin::from_selected_url(&selected).unwrap())
        .unwrap();
    grow.end_change().unwrap();
    let mut grow = grow.submit().unwrap();
    drive(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(false));
    install(&mut grow, &work.owner);
    drop(grow);
    drive(&mut driver);
    work.owner.cancel(CancellationReason::Requested);
    let cancelled = work.owner.retirement_observer().unwrap();
    let marker = control.progress_revision();
    assert_eq!(observer.is_reclaimed(), Ok(false));
    assert_eq!(cancelled.is_reclaimed(), Ok(false));
    assert_eq!(
        marker,
        control.progress_revision(),
        "pure observation cannot author progress"
    );
    // This fixture has no Native or external action: acknowledge the actual
    // local cancellation control intent before ordinary owner completion.
    while let Some(control) = control.next_control() {
        control.acknowledge();
    }
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(true));
    assert_eq!(cancelled.is_reclaimed(), Ok(true));
}

#[test]
fn leaf_and_parent_observers_do_not_prevent_the_original_leaf_first_reset() {
    let (control, _, mut driver) = fixed_control_storage_tests::prototype_profile(2, 1, 1, 1);
    let parent = root(&control);
    let child = parent
        .owner
        .scope()
        .child(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let root_observer = parent.owner.retirement_observer().unwrap();
    let child_observer = child.retirement_observer().unwrap();
    let view = child.scope().cancellation().unwrap();
    parent.business.release();
    parent.owner.complete();
    child.complete();
    drive(&mut driver);
    assert_eq!(root_observer.is_reclaimed(), Ok(false));
    assert_eq!(child_observer.is_reclaimed(), Ok(false));
    drop(view);
    drive(&mut driver);
    assert_eq!(root_observer.is_reclaimed(), Ok(true));
    assert_eq!(child_observer.is_reclaimed(), Ok(true));
}

#[test]
fn legacy_or_missing_authority_never_provides_a_physical_reset_proof() {
    let legacy = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    legacy.mark_ready().unwrap();
    let work = root(&legacy);
    assert!(matches!(
        work.owner.retirement_observer(),
        Err(WorkError::NotReady)
    ));
    work.business.release();
    work.owner.complete();
    let (observer, retained) = {
        let (control, handle, driver) = fixed_control_storage_tests::prototype_profile(2, 1, 1, 1);
        let work = root(&control);
        let observer = work.owner.retirement_observer().unwrap();
        let retained = work.owner.scope().cancellation().unwrap();
        work.business.release();
        work.owner.complete();
        assert_eq!(observer.is_reclaimed(), Ok(false));
        drop((handle, driver, control));
        (observer, retained)
    };
    assert_eq!(retained.reason(), None);
    assert_eq!(observer.is_reclaimed(), Err(WorkError::Released));
    drop(retained);
    assert_eq!(observer.is_reclaimed(), Err(WorkError::Released));
}
