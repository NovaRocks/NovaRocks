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

//! Real original paid claims/driver; no Native profile qualification.
use super::*;
use crate::joint_admission::*;
use crate::{
    ControlStorageConfig, WorkClass, WorkError, WorkOwner, WorkRequest, WorkScope, WorkloadConfig,
    WorkloadControl,
};
use novarocks_resource_contract::SharedDependency;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};
fn fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    fixture_with_positions(4)
}
fn fixture_with_positions(
    positions: usize,
) -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let count = 8;
    let storage = ControlStorageConfig::new(count, &[]);
    let config = ResponsibilityConfig {
        work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
    };
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    control
        .configure_result_capacity(ResultCapacityConfig {
            all_objects_bytes: [8 * 1024 * 1024, 96 * 1024 * 1024, 64, 8 * 1024 * 1024],
            positions: [320, 16, positions, 64],
            ..ResultCapacityConfig::V1
        })
        .unwrap();
    let (handle, driver) = control.configure_responsibility(config).unwrap();
    control.configure_control_storage(storage).unwrap();
    control.mark_ready().unwrap();
    (control, handle, driver)
}
fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..10000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original finite driver failed to reach idle");
}
fn receive(admission: &mut JointAdmission) -> JointGrantedBatch {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(batch)) => batch,
        _ => panic!("expected original paid reply"),
    }
}
fn fact(_name: &str) -> SharedDependency {
    SharedDependency::LocalHost
}
fn root(control: &WorkloadControl) -> crate::RootWork {
    control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap()
}
fn bind(scope: &WorkScope, facts: &[SharedDependency], driver: &mut JointProgressDriver) {
    let mut proposal = scope.begin_control_binding().unwrap();
    for fact in facts {
        proposal.push_dependency(fact).unwrap();
    }
    proposal.end_change().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(driver);
    let mut reply = receive(&mut pending);
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(scope)
        .unwrap()
        .notify();
    drop(reply);
    drive(driver);
}
fn amounts(resource: JointResource, amount: u64) -> JointResources {
    JointResources::ZERO
        .with(JointResource::ClaimRecords, 1)
        .with(JointResource::MetadataBytes, 0)
        .with(resource, amount)
}
fn request(
    handle: &JointAdmissionHandle,
    work: &WorkOwner,
    facts: &[SharedDependency],
    lane: JointLane,
    amount: JointResources,
) -> JointAdmission {
    let mut proposal = handle.begin_paid_proposal(work).unwrap();
    proposal.begin_new(lane, amount).unwrap();
    for fact in facts {
        proposal.push_dependency(fact).unwrap();
    }
    proposal.end_change().unwrap();
    proposal.submit().unwrap()
}
fn claim(
    handle: &JointAdmissionHandle,
    work: &WorkOwner,
    facts: &[SharedDependency],
    lane: JointLane,
    amount: JointResources,
    driver: &mut JointProgressDriver,
) -> JointClaim {
    let mut pending = request(handle, work, facts, lane, amount);
    drive(driver);
    let mut reply = receive(&mut pending);
    let result = reply.new_claims.pop().unwrap();
    drop(reply);
    drive(driver);
    result
}
fn finish(work: crate::RootWork, driver: &mut JointProgressDriver) {
    work.business.release();
    work.owner.complete();
    drive(driver);
}

#[test]
fn typed_root_alias_keeps_payload_but_not_the_delegated_stage_node() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let root_scope = work.owner.scope();
    let facts = [fact("host")];
    bind(&root_scope, &facts, &mut driver);
    let mut raw = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&root_scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    let original = ResultWindowAlias::from_internal_payload(
        &root_scope,
        &root_scope,
        payload.as_ref().unwrap(),
    )
    .unwrap();
    let stage = joint_child(&root_scope, &mut driver);
    let stage_scope = stage.scope();
    let retired = stage.retirement_observer().unwrap();
    let before = control.inner.state.lock().unwrap().nodes[&stage_scope.id()].resource_holders;
    let view = original.for_child(&stage_scope).unwrap();
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&stage_scope.id()].resource_holders,
        before
    );
    assert!(view.is_for_scope(&stage_scope));
    assert_eq!(view.root_scope().id(), root_scope.id());
    assert!(view.shares_capacity_with(&original));
    stage.complete();
    drive(&mut driver);
    assert!(
        retired.is_reclaimed().unwrap(),
        "payload-only attribution cannot pin the old stage control"
    );
    view.check_backing_total(8).unwrap();
    assert_eq!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .result_windows
            .held_positions,
        [0; 4]
    );
    let next = joint_child(&root_scope, &mut driver);
    next.complete();
    drive(&mut driver);
    let neutral = view.into_internal_retention().ok().unwrap();
    drop(original);
    drop(payload);
    finish(work, &mut driver);
    assert_eq!(neutral.capacity_bytes().unwrap(), 8);
    drop(neutral);
    drive(&mut driver);
    assert!(control.inner.state.lock().unwrap().nodes.is_empty());
}

#[test]
fn a_stage_owned_payload_cannot_be_relabelled_as_a_statement_root() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let facts = [fact("host")];
    bind(&work.owner.scope(), &facts, &mut driver);
    let child = joint_child(&work.owner.scope(), &mut driver);
    let scope = child.scope();
    bind(&scope, &facts, &mut driver);
    let mut raw = Some(claim(
        &handle,
        &child,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    assert!(matches!(
        ResultWindowAlias::from_internal_payload(&scope, &scope, payload.as_ref().unwrap()),
        Err(WorkError::Conflict)
    ));
    drop(payload);
    child.complete();
    finish(work, &mut driver);
}

fn joint_child(scope: &WorkScope, driver: &mut JointProgressDriver) -> WorkOwner {
    let mut pending = scope.request_joint_child().unwrap();
    drive(driver);
    let mut ready = receive(&mut pending);
    let mut target = None;
    ready
        .adjustment
        .as_mut()
        .unwrap()
        .install_child_in(scope, &mut target)
        .unwrap()
        .notify();
    drop(ready);
    drop(pending);
    drive(driver);
    target.unwrap()
}

#[test]
fn actual_root_buffer_receipts_share_the_original_budget_independently_of_recorded_quantities() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("local")], &mut driver);
    let mut raw = Some(claim(
        &handle,
        &work.owner,
        &[fact("local")],
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    let first = ResultWindowAlias::from_internal_payload(&scope, &scope, payload.as_ref().unwrap())
        .unwrap();
    let second =
        ResultWindowAlias::from_internal_payload(&scope, &scope, payload.as_ref().unwrap())
            .unwrap();
    struct ActualBuffer {
        _data: Vec<u8>,
        _receipt: ResultPayloadBytes,
    }
    let receipt = first.reserve_payload_bytes(40).unwrap();
    let original = Arc::new(ActualBuffer {
        _data: vec![0; 40],
        _receipt: receipt,
    });
    let retained = Arc::clone(&original);
    assert_eq!(second.retained_payload_bytes(), 40);
    assert!(matches!(
        second.reserve_payload_bytes(25),
        Err(WorkError::Capacity(_))
    ));
    let transient = second.reserve_payload_bytes(24).unwrap();
    assert_eq!(first.retained_payload_bytes(), 64);
    drop(transient);
    drop(original);
    assert_eq!(
        second.retained_payload_bytes(),
        40,
        "a handoff is not actual backing exit"
    );
    drop(retained);
    assert_eq!(first.retained_payload_bytes(), 0);
    // Neither the old responsibility quantity eight nor a fresh alias mints a
    // new budget. Both views share the original configured sixty-four bytes.
    let mut full = first.reserve_payload_bytes(64).unwrap();
    assert!(matches!(full.try_grow(1), Err(WorkError::Capacity(_))));
    assert_eq!(full.bytes(), 64);
    assert!(second.reserve_payload_bytes(1).is_err());
    drop(full);
    assert_eq!(second.retained_payload_bytes(), 0);
    drop(second);
    drop(first);
    drop(payload);
    finish(work, &mut driver);
    drop(driver);
    control.close_admission();
    assert!(control.shutdown().is_ok());
}

fn stage_request(
    payload: &RetainedPayloadGrant,
    root: &WorkScope,
    stage: &WorkOwner,
) -> RootStageAdmission {
    let mut proposal = payload
        .root_stage_source(root, stage)
        .unwrap()
        .begin(0, JointLane::Foreground)
        .unwrap();
    proposal.end_payload().unwrap();
    proposal.end_stage().unwrap();
    proposal.submit().unwrap()
}
fn install_stage(
    admission: &mut RootStageAdmission,
    root: &WorkScope,
    payload: &RetainedPayloadGrant,
    stage: &WorkOwner,
) -> InternalComputationGrant {
    let mut ready = match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("original RootStage must have its actual position"),
    };
    let mut compute = None;
    ready
        .install_in(root, payload, stage, &mut compute)
        .unwrap()
        .notify();
    compute.unwrap()
}

#[test]
fn root_owner_data_does_not_occupy_single_compute_and_last_buffer_keeps_root() {
    let (control, handle, mut driver) = fixture_with_positions(1);
    let work = root(&control);
    let scope = work.owner.scope();
    let original = ResultWindowAlias::from_root_owner(&work.owner).unwrap();
    let peer = ResultWindowAlias::from_root_owner(&work.owner).unwrap();
    assert!(original.shares_capacity_with(&peer));
    assert_eq!(control.snapshot().result_windows.held_positions, [0; 4]);
    assert!(original.internal_manifest_binding().unwrap().is_none());
    assert!(matches!(
        original.native_action_authority(),
        Err(WorkError::Conflict)
    ));
    assert!(
        original
            .try_clone()
            .unwrap()
            .into_internal_retention()
            .is_err()
    );
    struct ActualBuffer {
        _data: Vec<u8>,
        _receipt: ResultPayloadBytes,
    }
    let receipt = original.reserve_payload_bytes(40).unwrap();
    let buffer = Arc::new(ActualBuffer {
        _data: vec![0; 40],
        _receipt: receipt,
    });
    let tail = Arc::clone(&buffer);
    bind(&scope, &[fact("local")], &mut driver);
    let mut raw = Some(claim(
        &handle,
        &work.owner,
        &[fact("local")],
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    let payload = payload.unwrap();
    let old = ResultWindowAlias::from_internal_payload(&scope, &scope, &payload).unwrap();
    assert!(old.shares_capacity_with(&original));
    assert_eq!(old.retained_payload_bytes(), 40);
    assert!(matches!(
        old.reserve_payload_bytes(25),
        Err(WorkError::Capacity(_))
    ));
    let transient = old.reserve_payload_bytes(24).unwrap();
    assert_eq!(peer.retained_payload_bytes(), 64);
    drop(transient);

    let first = joint_child(&scope, &mut driver);
    let first_scope = first.scope();
    let first_exit = first.retirement_observer().unwrap();
    let attributed = original.for_child(&first_scope).unwrap();
    let mut admission = stage_request(&payload, &scope, &first);
    drive(&mut driver);
    let computation = install_stage(&mut admission, &scope, &payload, &first);
    drop(admission);
    let live_compute = computation.retain_alias().unwrap();
    drop(computation);
    first.complete();
    drive(&mut driver);
    assert!(!first_exit.is_reclaimed().unwrap());
    assert_eq!(control.snapshot().result_windows.held_positions[2], 1);
    let second = joint_child(&scope, &mut driver);
    let mut waiting = stage_request(&payload, &scope, &second);
    drive(&mut driver);
    assert!(
        Pin::new(&mut waiting)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    // The payload tail cannot release a still-used computation position.
    drop(attributed);
    assert_eq!(control.snapshot().result_windows.held_positions[2], 1);
    drop(live_compute);
    drive(&mut driver);
    assert!(first_exit.is_reclaimed().unwrap());
    let next_compute = install_stage(&mut waiting, &scope, &payload, &second);
    drop(waiting);
    assert_eq!(peer.retained_payload_bytes(), 40);
    drop(next_compute);
    second.complete();
    drive(&mut driver);
    assert_eq!(control.snapshot().result_windows.held_positions, [0; 4]);
    assert_eq!(old.retained_payload_bytes(), 40);
    drop(old);
    drop(payload);
    drop(original);
    drop(peer);
    finish(work, &mut driver);
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .nodes
            .contains_key(&scope.id())
    );
    drop(buffer);
    drive(&mut driver);
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .nodes
            .contains_key(&scope.id())
    );
    drop(tail);
    drive(&mut driver);
    assert!(control.inner.state.lock().unwrap().nodes.is_empty());
}

#[test]
fn root_owner_retention_rejects_child_cancel_and_missing_result_author() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("local")], &mut driver);
    let child = joint_child(&scope, &mut driver);
    let child_scope = child.scope();
    let held = control.inner.state.lock().unwrap().nodes[&child_scope.id()].resource_holders;
    assert!(matches!(
        ResultWindowAlias::from_root_owner(&child),
        Err(WorkError::Conflict)
    ));
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&child_scope.id()].resource_holders,
        held
    );
    assert!(
        control.inner.state.lock().unwrap().nodes[&scope.id()]
            .internal_result_bytes
            .upgrade()
            .is_none()
    );
    child.complete();
    work.owner.cancel(crate::CancellationReason::Requested);
    assert!(matches!(
        ResultWindowAlias::from_root_owner(&work.owner),
        Err(WorkError::Cancelled(_))
    ));
    work.business.release();
    work.owner.complete_after_terminal_cancel_settled();
    drive(&mut driver);
    let unconfigured = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    unconfigured.mark_ready().unwrap();
    let work = root(&unconfigured);
    assert!(matches!(
        ResultWindowAlias::from_root_owner(&work.owner),
        Err(WorkError::NotReady)
    ));
    work.business.release();
    work.owner.complete();
}
