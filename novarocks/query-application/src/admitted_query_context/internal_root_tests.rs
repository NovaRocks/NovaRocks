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
use novarocks_resource_contract::SharedDependency;
use novarocks_workload_control::joint_admission::*;
use novarocks_workload_control::{
    ControlStorageConfig, WorkClass, WorkError, WorkOwner, WorkRequest, WorkScope, WorkloadConfig,
    WorkloadControl,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};
const INTERNAL_RESULT_BYTES: u64 = 16;

fn fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    fixture_with_client(false)
}
fn fixture_with_client(
    client: bool,
) -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let count = 8;
    let storage = ControlStorageConfig::new(count, &[]);
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (handle, driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    control.configure_control_storage(storage).unwrap();
    let mut results = novarocks_workload_control::ResultCapacityConfig::V1;
    results.all_objects_bytes[2] = INTERNAL_RESULT_BYTES;
    if client {
        results.all_objects_bytes[0] = 8;
    }
    control.configure_result_capacity(results).unwrap();
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
fn root(control: &WorkloadControl) -> novarocks_workload_control::RootWork {
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
fn finish(work: novarocks_workload_control::RootWork, driver: &mut JointProgressDriver) {
    work.business.release();
    work.owner.complete();
    drive(driver);
}

#[test]
fn actual_job_activity_keeps_only_its_stage_compute_while_root_payload_remains_live() {
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
    let stage = joint_child(&root_scope, &mut driver);
    let stage_scope = stage.scope();
    bind(&stage_scope, &facts, &mut driver);
    let observer = stage.retirement_observer().unwrap();
    let mut raw_compute = Some(claim(
        &handle,
        &stage,
        &facts,
        JointLane::Foreground,
        amounts(JointResource::Computations, 1),
        &mut driver,
    ));
    let mut compute = None;
    InternalComputationGrant::adopt_in(&stage_scope, &mut raw_compute, &mut compute)
        .unwrap()
        .notify();
    let binding = QueryResultCapacityBinding::from_internal_payload(
        &root_scope,
        &stage_scope,
        payload.as_ref().unwrap(),
    )
    .unwrap();
    assert!(
        matches!(binding.begin_result_activity(), Err(WorkError::Conflict)),
        "payload-only binding cannot mint an activity"
    );
    let lease = binding
        .begin_internal_result_activity(compute.as_ref().unwrap().retain_alias().unwrap())
        .unwrap();
    let mut exit = Box::pin(binding.result_activities_exited());
    assert!(
        exit.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(compute);
    stage.complete();
    drive(&mut driver);
    assert!(!observer.is_reclaimed().unwrap());
    drop(lease);
    drive(&mut driver);
    assert!(
        exit.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert!(
        observer.is_reclaimed().unwrap(),
        "Root bytes must not pin old stage control after actual activity exit"
    );
    drop(exit);
    binding
        .try_window_alias()
        .unwrap()
        .check_backing_total(8)
        .unwrap();
    let next = joint_child(&root_scope, &mut driver);
    next.complete();
    drive(&mut driver);
    drop(binding);
    drop(payload);
    finish(work, &mut driver);
}

#[test]
fn successive_real_closed_batches_reuse_the_root_payload_after_each_actual_activity_exit() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let root_scope = work.owner.scope();
    let facts = [SharedDependency::LocalHost];
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
    let grant = payload.as_ref().unwrap();
    let original =
        QueryResultCapacityBinding::from_internal_payload(&root_scope, &root_scope, grant).unwrap();
    let old = original.try_window_alias().unwrap();
    let mut execution = QueryExecutionContext::new(
        novarocks_types::ClusterRole::Fe,
        crate::api::BackendTopologySnapshot::try_new(1, Vec::new()).unwrap(),
        Some(std::time::Instant::now() + std::time::Duration::from_secs(30)),
        crate::cancellation::QueryCancellationSource::new().view(),
        novarocks_sql::compiler::SessionOptimizerSettings::default(),
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
    )
    .with_result_capacity(original.try_clone().unwrap())
    .unwrap();
    let original_deadline = execution.deadline();
    for expected_bytes in [12, 16] {
        let stage = joint_child(&root_scope, &mut driver);
        let scope = stage.scope();
        let observer = stage.retirement_observer().unwrap();
        let mut proposal = grant
            .root_stage_source(&root_scope, &stage)
            .unwrap()
            .begin(4, JointLane::Foreground)
            .unwrap();
        proposal.end_payload().unwrap();
        proposal.end_stage().unwrap();
        let mut admission = proposal.submit().unwrap();
        drive(&mut driver);
        let mut ready = match Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(Ok(ready)) => ready,
            _ => panic!("the same Root batch must receive only after the previous activity exits"),
        };
        let mut computation = None;
        ready
            .install_in(&root_scope, grant, &stage, &mut computation)
            .unwrap()
            .notify();
        drop(ready);
        drop(admission);
        drive(&mut driver);
        assert!(matches!(
            execution.for_internal_computation(original.for_child(&scope).unwrap()),
            Err(WorkError::Conflict)
        ));
        let binding = QueryResultCapacityBinding::from_internal_computation(
            &root_scope,
            &stage,
            grant,
            computation.as_ref().unwrap(),
        )
        .unwrap();
        execution = execution
            .for_internal_computation(binding.try_clone().unwrap())
            .unwrap();
        assert_eq!(execution.deadline(), original_deadline);
        assert!(
            execution
                .result_capacity()
                .unwrap()
                .window
                .is_for_scope(&scope)
        );
        let retained = binding
            .try_clone()
            .unwrap()
            .into_statement_retention()
            .unwrap();
        assert!(
            retained
                .window
                .internal_manifest_binding()
                .unwrap()
                .is_none()
        );
        let view = binding.try_window_alias().unwrap();
        assert!(old.shares_capacity_with(&view));
        // Responsibility growth does not mint bytes. Both stages share the
        // same independently installed result owner's retained-data budget.
        old.check_backing_total(INTERNAL_RESULT_BYTES).unwrap();
        assert!(matches!(
            old.check_backing_total(INTERNAL_RESULT_BYTES + 1),
            Err(WorkError::Capacity(_))
        ));
        let activity = binding
            .begin_internal_result_activity(computation.as_ref().unwrap().retain_alias().unwrap())
            .unwrap();
        drop(computation);
        stage.complete();
        drive(&mut driver);
        assert!(!observer.is_reclaimed().unwrap());
        drop(activity);
        drive(&mut driver);
        assert!(observer.is_reclaimed().unwrap());
        assert!(matches!(binding.scope().check(), Err(WorkError::Released)));
        execution = execution.for_statement_retention(retained).unwrap();
        let continued = execution.result_capacity().unwrap();
        continued.scope().check().unwrap();
        assert!(continued.window.is_for_scope(&root_scope));
        assert!(old.shares_capacity_with(&continued.window));
        continued
            .window
            .check_backing_total(expected_bytes)
            .unwrap();
        assert!(matches!(
            execution.for_internal_computation(continued.try_clone().unwrap()),
            Err(WorkError::Conflict)
        ));
        // Both the old Root bytes and this stage's payload-only projection
        // remain alive while the original slot becomes reusable.
        assert!(old.shares_capacity_with(&view));
        drop(view);
        drop(binding);
    }
    drop(execution);
    drop(old);
    drop(original);
    drop(payload);
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

mod client_computation_tests;

#[test]
fn root_owner_binding_is_retention_only_and_preserves_same_root_budget() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let original = QueryResultCapacityBinding::from_root_owner(&work.owner).unwrap();
    let peer = QueryResultCapacityBinding::from_root_owner(&work.owner).unwrap();
    assert!(original.is_root_backed_internal());
    assert_eq!(original.class(), ResultWindowClass::Internal);
    assert!(original.native_action_authority().is_err());
    assert!(original.begin_result_activity().is_err());
    let alias = original.try_window_alias().unwrap();
    assert!(alias.internal_manifest_binding().unwrap().is_none());
    let receipt = alias.reserve_payload_bytes(8).unwrap();
    let bytes = vec![0u8; 8];
    assert_eq!(peer.window.retained_payload_bytes(), 8);
    assert_eq!(control.snapshot().result_windows.held_positions, [0; 4]);
    let execution = QueryExecutionContext::new(
        novarocks_types::ClusterRole::Fe,
        crate::api::BackendTopologySnapshot::try_new(1, Vec::new()).unwrap(),
        Some(std::time::Instant::now() + std::time::Duration::from_secs(30)),
        crate::cancellation::QueryCancellationSource::new().view(),
        novarocks_sql::compiler::SessionOptimizerSettings::default(),
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
    )
    .with_result_capacity(original.try_clone().unwrap())
    .unwrap();
    bind(&scope, &[fact("local")], &mut driver);
    let child = joint_child(&scope, &mut driver);
    assert!(matches!(
        execution.for_internal_computation(original.for_child(&child.scope()).unwrap()),
        Err(WorkError::Conflict)
    ));
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
    let admitted = QueryResultCapacityBinding::from_internal_payload(
        &scope,
        &scope,
        payload.as_ref().unwrap(),
    )
    .unwrap();
    assert!(admitted.window.shares_capacity_with(&alias));
    let retained = peer.into_statement_retention().unwrap();
    let continued = execution.for_statement_retention(retained).unwrap();
    assert_eq!(
        continued
            .result_capacity()
            .unwrap()
            .window
            .retained_payload_bytes(),
        8
    );
    let (foreign, _, mut foreign_driver) = fixture();
    let foreign_work = root(&foreign);
    assert!(matches!(
        alias.for_child(&foreign_work.owner.scope()),
        Err(WorkError::ForeignAuthority)
    ));
    assert!(matches!(
        continued.for_statement_retention(
            QueryResultCapacityBinding::from_root_owner(&foreign_work.owner).unwrap()
        ),
        Err(WorkError::Conflict)
    ));
    finish(foreign_work, &mut foreign_driver);
    drop(bytes);
    drop(receipt);
    assert_eq!(admitted.window.retained_payload_bytes(), 0);
    drop(continued);
    drop(execution);
    drop(admitted);
    drop(payload);
    drop(alias);
    drop(original);
    child.complete();
    finish(work, &mut driver);
}
