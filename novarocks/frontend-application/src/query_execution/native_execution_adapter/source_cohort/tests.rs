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

//! Real Source admission cells and original close jobs; no production certificate.
use super::*;
use novarocks_workload_control::joint_admission::*;
use novarocks_workload_control::{
    ControlStorageConfig, PollRoleSourceBacking, ResultCapacityConfig, ResultWindowClass,
    WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::{task::Waker, time::Duration};

pub(crate) fn fixture() -> (
    WorkloadControl,
    JointProgressDriver,
    novarocks_workload_control::ResultCapacityHandle,
) {
    let (control, driver, capacity) = fixture_before_ready();
    control.mark_ready().unwrap();
    (control, driver, capacity)
}

pub(crate) fn fixture_before_ready() -> (
    WorkloadControl,
    JointProgressDriver,
    novarocks_workload_control::ResultCapacityHandle,
) {
    let count = 8;
    let storage = ControlStorageConfig::new(count, &[]).with_process_source_backing(
        PollRoleSourceBacking::from_layouts(
            &[std::alloc::Layout::from_size_align(
                crate::native::NativeSubscriptionOwner::resident_storage_bytes(count).unwrap(),
                8,
            )
            .unwrap()],
            &[],
        )
        .unwrap(),
    );
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_, driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    control.configure_control_storage(storage).unwrap();
    let capacity = control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    (control, driver, capacity)
}
pub(crate) fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..100_000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original finite driver did not become idle");
}

pub(crate) fn bind(scope: &WorkScope, driver: &mut JointProgressDriver) {
    let endpoint =
        novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 19010).unwrap();
    let transport = novarocks_native_adapter::FrontendNativeTransport::plaintext();
    let mut proposal = scope.begin_control_binding().unwrap();
    proposal
        .push_selected_network(
            transport
                .selected_network_origin(endpoint.native_endpoint())
                .unwrap(),
        )
        .unwrap();
    proposal.end_change().unwrap();
    let mut admission = proposal.submit().unwrap();
    drive(driver);
    let Poll::Ready(Ok(mut batch)) =
        Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("original exact parent binding must grant");
    };
    batch
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(scope)
        .unwrap()
        .notify();
    drop((batch, admission));
    drive(driver);
}

pub(crate) fn cohort(
    scope: &WorkScope,
    count: usize,
    driver: &mut JointProgressDriver,
) -> NativeSourceCohort {
    let mut original = NativeSourceCohortAdmission::default();
    for _ in 0..100_000 {
        match original.poll(
            scope,
            NonZeroUsize::new(count).unwrap(),
            &mut Context::from_waker(Waker::noop()),
        ) {
            Poll::Pending => drive(driver),
            Poll::Ready(Err(error)) => panic!("actual Source cohort refused: {error:?}"),
            Poll::Ready(Ok(())) => return original.borrowed().unwrap(),
        }
    }
    panic!("actual Source cohort did not finish");
}

pub(crate) fn finish(
    control: WorkloadControl,
    root: novarocks_workload_control::RootWork,
    mut driver: JointProgressDriver,
) {
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    control.close_admission();
    while let Some(original) = control.next_control() {
        original.acknowledge();
    }
    drive(&mut driver);
    assert!(
        control.shutdown().is_ok(),
        "original Source children and cohort actually drain"
    );
}

#[test]
fn source_count_is_the_shared_checked_scan_and_context_recipe() {
    assert_eq!(source_count(1, 3).unwrap().get(), 4);
    assert_eq!(source_count(0, 3).unwrap().get(), 3);
    assert!(matches!(
        source_count(0, 0),
        Err(WorkError::InvalidConfig(_))
    ));
    assert_eq!(
        source_count(usize::MAX, 1),
        Err(WorkError::ArithmeticOverflow)
    );
}

#[test]
fn original_cohort_borrowed_poll_keeps_same_request_and_count_with_other_builders_live() {
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    bind(&scope, &mut driver);
    let unsubmitted = [
        scope.begin_control_binding().unwrap(),
        scope.begin_control_binding().unwrap(),
    ];
    let mut original = NativeSourceCohortAdmission::default();
    let count = source_count(1, 1).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(original.poll(&scope, count, &mut cx).is_pending());
    assert!(original.wait.is_none() && original.admission.is_some() && original.receipt.is_none());
    assert!(!original.installed);
    assert_eq!(
        control.snapshot().scopes.len(),
        1,
        "a queued request has not installed any live Source"
    );
    // Dropping the borrowed poll F leaves the same admission in this Source;
    // unrelated unsubmitted builders do not consume request quota.
    let mut borrowed = Box::pin(std::future::poll_fn(|cx| original.poll(&scope, count, cx)));
    assert!(borrowed.as_mut().poll(&mut cx).is_pending());
    drop(borrowed);
    assert!(original.wait.is_none() && original.admission.is_some());
    drive(&mut driver);
    loop {
        match original.poll(&scope, count, &mut cx) {
            Poll::Pending => drive(&mut driver),
            Poll::Ready(result) => {
                result.unwrap();
                break;
            }
        }
    }
    assert_eq!(original.borrowed().unwrap().remaining(), 2);
    assert_eq!(
        control.snapshot().scopes.len(),
        1,
        "joint grant creates no live source"
    );
    assert!(matches!(
        original.poll(&scope, source_count(1, 2).unwrap(), &mut cx),
        Poll::Ready(Err(WorkError::Conflict))
    ));
    drop(unsubmitted);
    original.close_after_sources();
    drop((original, capacity, scope));
    finish(control, root, driver);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_scan_receiving_cell_holds_child_through_original_close_job() {
    use crate::task_execution::blocking_io::{
        ConnectorBlockingIoResponsibility, ConnectorBlockingIoSupervisor,
    };
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    bind(&scope, &mut driver);
    let grant = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let alias = grant.retain_alias();
    let cohort = cohort(&scope, 1, &mut driver);
    let mut admission =
        ConnectorBlockingIoResponsibility::request_from_cohort(&scope, &alias, cohort.clone())
            .unwrap();
    assert_eq!(cohort.remaining(), 1, "original factory is metadata-only");
    let responsibility = admission.ready().await.unwrap();
    let child_id = responsibility.scope().unwrap().id();
    assert_eq!(cohort.remaining(), 0);
    let backing = responsibility.retain_backing();
    let (release, blocked) = std::sync::mpsc::channel();
    let entered = Arc::new(tokio::sync::Notify::new());
    let probe = Arc::clone(&entered);
    let supervisor = ConnectorBlockingIoSupervisor::new(tokio::runtime::Handle::current());
    let mut physical =
        crate::task_execution::blocking_io::tests::OriginalExecutorsFixture::install(&supervisor);
    let mut job = supervisor.prepare_cpu_retirement(
        vec![responsibility.join_pin()],
        (probe, blocked),
        |input, _| {
            input.0.notify_one();
            input.1.recv().unwrap();
            Some(())
        },
    );
    std::future::poll_fn(|cx| {
        let _ = job.poll_progress(cx);
        std::task::Poll::Ready(())
    })
    .await;
    entered.notified().await;
    drop(responsibility);
    assert!(
        control.snapshot().scopes.iter().any(|s| s.id == child_id),
        "source close has not actually exited"
    );
    release.send(()).unwrap();
    let mut outcome = None;
    tokio::time::timeout(Duration::from_secs(3), job.finish_in(&mut outcome))
        .await
        .unwrap();
    assert!(matches!(outcome, Some(Ok(()))));
    drop(job);
    drop(backing);
    assert!(
        !control.snapshot().scopes.iter().any(|s| s.id == child_id),
        "actual original close and final backing are the exit cut"
    );
    drop((admission, cohort, alias, grant, capacity, scope));
    physical.shutdown().await;
    finish(control, root, driver);
}

#[tokio::test]
async fn exhausted_scan_cohort_never_uses_a_newly_free_child_position() {
    use crate::task_execution::blocking_io::ConnectorBlockingIoResponsibility;
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    bind(&scope, &mut driver);
    let grant = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let alias = grant.retain_alias();
    let cohort = cohort(&scope, 1, &mut driver);
    let mut first =
        ConnectorBlockingIoResponsibility::request_from_cohort(&scope, &alias, cohort.clone())
            .unwrap();
    let source = first.ready().await.unwrap();
    drop(source);
    drive(&mut driver);
    assert_eq!(control.snapshot().scopes.len(), 1);
    let mut second =
        ConnectorBlockingIoResponsibility::request_from_cohort(&scope, &alias, cohort.clone())
            .unwrap();
    assert!(matches!(
        second.ready().await,
        Err(WorkError::Capacity("Native Source cohort exhausted"))
    ));
    assert_eq!(
        control.snapshot().scopes.len(),
        1,
        "a freed slot does not replenish the original cohort"
    );
    // A later actual attempt needs its own new exact receipt, even with the same parent.
    let next = self::cohort(&scope, 1, &mut driver);
    assert_eq!(next.remaining(), 1);
    drop((first, second, next, cohort, alias, grant, capacity, scope));
    finish(control, root, driver);
}
