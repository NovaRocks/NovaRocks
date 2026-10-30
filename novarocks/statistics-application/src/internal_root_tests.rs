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
    ControlStorageConfig, WorkClass, WorkOwner, WorkRequest, WorkScope, WorkloadConfig,
    WorkloadControl,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};
fn fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
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
    results.all_objects_bytes[2] = 1024 * 1024;
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

async fn internal_admission(
    control: &WorkloadControl,
    handle: &JointAdmissionHandle,
    driver: &mut JointProgressDriver,
) -> (Option<StatisticsJobAdmission>, WorkScope) {
    let work = control
        .root_admission()
        .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
        .unwrap();
    let scope = work.owner.scope();
    let facts = [SharedDependency::LocalHost];
    bind(&scope, &facts, driver);
    let mut raw = Some(claim(
        handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    let mut query = Some(scope.admit_query().unwrap().await.unwrap());
    let mut owner = Some(work.owner);
    let mut admission = None;
    StatisticsJobAdmission::install_internal_in(
        &mut owner,
        &mut query,
        &mut payload,
        &mut admission,
    )
    .unwrap();
    assert!(owner.is_none() && query.is_none() && payload.is_none());
    (admission, scope)
}

#[tokio::test]
async fn terminal_history_does_not_consume_job_admission_but_active_jobs_still_do() {
    let repository = StatisticsJobRepository::new();
    let retained =
        tests::retain_unknown_terminal_jobs(&repository, MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS)
            .await;
    let ordinary = repository
        .create_admitted(tests::create(10), tests::root().await)
        .await
        .expect("unconverged terminal history is not active job admission");
    let (control, handle, mut driver) = fixture();
    let (mut admission, scope) = internal_admission(&control, &handle, &mut driver).await;
    let (installed, notification) = repository
        .install_internal_job_in(tests::create(11), &mut admission)
        .expect("the original stable Root receiver has an active position");
    assert!(admission.is_none());
    assert_eq!(
        repository.lock().unwrap().active[&installed.id]
            .owner
            .as_ref()
            .unwrap()
            .scope()
            .id(),
        scope.id()
    );
    notification.notify();
    let mut active = vec![ordinary.id, installed.id];
    for index in active.len()..MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS {
        active.push(
            repository
                .create_admitted(tests::create(index as i64 + 20), tests::root().await)
                .await
                .unwrap()
                .id,
        );
    }
    let (mut refused, refused_scope) = internal_admission(&control, &handle, &mut driver).await;
    let error = match repository.install_internal_job_in(tests::create(30), &mut refused) {
        Err(error) => error,
        Ok(_) => panic!("the original active job limit must still refuse another Root"),
    };
    assert_eq!(error.kind(), StatisticsRepositoryErrorKind::Capacity);
    assert_eq!(refused.as_ref().unwrap().scope().id(), refused_scope.id());
    assert!(refused.as_ref().unwrap().root_payload.is_some());
    assert_eq!(
        repository.lock().unwrap().active.len(),
        MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS
    );
    for (original, scope) in &retained {
        assert_eq!(
            repository.get(original.id).await.unwrap().as_ref(),
            Some(original)
        );
        scope.check().unwrap();
    }
    // Rejected installation did not consume the original source or its permit.
    let mut original = refused.take().unwrap().into_originals();
    assert!(original.query_concurrency.is_some());
    let owner = original.owner.take().unwrap();
    drop(original);
    owner.complete_after_terminal_cancel_settled();
    for id in active {
        assert!(
            repository
                .request_cancel(id, 40)
                .await
                .unwrap()
                .convergence
                .is_complete()
        );
    }
    drive(&mut driver);
    assert!(matches!(
        scope.check(),
        Err(novarocks_workload_control::WorkError::Released)
    ));
    assert!(matches!(
        refused_scope.check(),
        Err(novarocks_workload_control::WorkError::Released)
    ));
    tests::close_local_terminal_fixtures(&repository, retained).await;
}

#[tokio::test]
async fn original_live_job_receives_the_root_grant_and_transfers_only_exact_stage_compute() {
    let (control, handle, mut driver) = fixture();
    let (mut admission, root_scope) = internal_admission(&control, &handle, &mut driver).await;
    let repository = StatisticsJobRepository::new();
    let (job, notification) = repository
        .install_internal_job_in(
            StatisticsJobCreate {
                target: StatisticsTarget {
                    catalog: Arc::from("iceberg"),
                    namespace: Arc::from("db"),
                    table: Arc::from("orders"),
                    object_id: Arc::from(&b"orders-v1"[..]),
                },
                columns: StatisticsColumns::All,
                submitted_at_ms: 1,
            },
            &mut admission,
        )
        .unwrap();
    assert!(admission.is_none());
    notification.notify();
    repository.claim_next(2).await.unwrap().unwrap();
    let stage = joint_child(&root_scope, &mut driver);
    let scope = stage.scope();
    let observer = stage.retirement_observer().unwrap();
    // This source is a pure capture under the original LiveJob lock. Body
    // allocation and proof polling happen only after that lock has exited.
    let mut proposal = repository
        .root_stage_source(job.id, &stage)
        .unwrap()
        .begin(4, JointLane::Foreground)
        .unwrap();
    proposal.end_payload().unwrap();
    proposal.end_stage().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(&mut driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("expected the original closed batch"),
    };
    repository
        .install_root_stage_in(job.id, &stage, &mut ready)
        .unwrap()
        .notify();
    drop(ready);
    drop(pending);
    drive(&mut driver);
    let root_binding = repository.lock().unwrap().active[&job.id]
        .result_capacity
        .as_ref()
        .unwrap()
        .try_clone()
        .unwrap();
    let binding = root_binding.for_child(&scope).unwrap();
    let mut computation = None;
    let foreign = control
        .root_admission()
        .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
        .unwrap();
    assert!(
        repository
            .transfer_stage_computation_in(job.id, &foreign.owner, &mut computation)
            .is_err()
    );
    assert!(computation.is_none());
    assert!(
        repository.lock().unwrap().active[&job.id]
            .stage_computation
            .is_some()
    );
    foreign.owner.complete();
    drive(&mut driver);
    repository
        .transfer_stage_computation_in(job.id, &stage, &mut computation)
        .unwrap();
    let old = computation.as_ref().unwrap().retain_alias().unwrap();
    assert!(
        repository
            .transfer_stage_computation_in(job.id, &stage, &mut computation)
            .is_err()
    );
    assert!(old.shares_computation_with(&computation.as_ref().unwrap().retain_alias().unwrap()));
    let activity = binding.begin_internal_result_activity(old).unwrap();
    drop(computation);
    stage.complete();
    drive(&mut driver);
    assert!(!observer.is_reclaimed().unwrap());
    drop(activity);
    drive(&mut driver);
    assert!(observer.is_reclaimed().unwrap());
    // This test has no Native/provider operation. These are actual local
    // fixture exits, not a production terminal-to-convergence projection.
    repository
        .conclude(
            job.id,
            StatisticsJobPhase::Preparing,
            StatisticsJobConclusion::Failed,
            StatisticsPublicationFact::NotStarted,
            None,
            3,
        )
        .await
        .unwrap();
    repository
        .record_convergence(
            job.id,
            StatisticsConvergence {
                collection_stopped: true,
                execution_resources_released: true,
                provider_session_closed: true,
            },
            4,
        )
        .await
        .unwrap();
    drive(&mut driver);
    root_binding
        .try_window_alias()
        .unwrap()
        .check_backing_total(12)
        .unwrap();
    drop(binding);
    drop(root_binding);
    drive(&mut driver);
    assert!(root_scope.check().is_err());
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
