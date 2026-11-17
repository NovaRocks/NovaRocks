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

//! Fixed statistics consumers over the original current QA/LP/AP Sources.

use crate::fixtures::native_recipe::{
    StatisticsRecipeCase, TypedStatisticsCustody, admit_statistics_originals, drive_typed,
    original_statistics_body, run_actual_statistics_case,
};
use crate::query_execution::internal_result_cpu::InternalResultCpuOwner;
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding, cancellation::QueryCancellationView,
};
use novarocks_spi::connector::ConnectorInstanceId;
use novarocks_workload_control::joint_admission as joint;
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

async fn watchdog<T>(stage: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("Statistics component watchdog: {stage}"))
}

#[derive(Clone, Copy)]
enum Case {
    Success,
    TypedRootSuccess,
    MissingArtifact,
    ProtocolFailureFirst,
    OriginalFailureStreamAfterEnd,
}

// The same original driver progresses alongside the consumer future. More
// yields after a finite turn; Idle waits on its original revision notification.
// This introduces no second task, polling timer or duplicate driver authority.
async fn watchdog_joint<T>(
    typed: Option<&mut TypedStatisticsCustody>,
    stage: &'static str,
    future: impl Future<Output = T>,
) -> T {
    let Some(typed) = typed else {
        return watchdog(stage, future).await;
    };
    watchdog(stage, async {
        tokio::pin!(future);
        loop {
            match typed.driver.step() {
                joint::JointProgress::More => {
                    tokio::select! {
                        result = future.as_mut() => return result,
                        _ = tokio::task::yield_now() => {}
                    }
                }
                joint::JointProgress::Idle(revision) => {
                    tokio::select! {
                        result = future.as_mut() => return result,
                        _ = typed.progress.wait_state_change(revision) => {}
                    }
                }
            }
        }
    })
    .await
}

fn typed_statistics_custody(
    cpu_owner: &mut InternalResultCpuOwner,
) -> (
    TypedStatisticsCustody,
    (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::WorkOwner,
        novarocks_workload_control::ResultCapacityHandle,
    ),
) {
    use novarocks_workload_control::{
        ControlStorageConfig, ResponsibilityConfig, ResultCapacityConfig, WorkloadConfig,
        WorkloadControl,
    };
    let count = 8;
    let mut control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let mut result = ResultCapacityConfig::V1;
    result.positions[2] = 1;
    result.all_objects_bytes[2] = 1024 * 1024;
    assert!(
        original_statistics_body().len() < 16 * 1024,
        "the actual component body fits its explicit small payload budget"
    );
    let capacity = control.configure_result_capacity(result).unwrap();
    let (_, mut driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    control
        .configure_control_storage(ControlStorageConfig::new(count, &[]))
        .unwrap();
    cpu_owner.bind_for_test_before_ready(&mut control).unwrap();
    control.mark_ready().unwrap();
    let (root, payload, stage, computation, retired) =
        admit_statistics_originals(&control, &mut driver);
    let progress = control.deadline_expiry_handle();
    (
        TypedStatisticsCustody {
            root,
            payload,
            computation: Some(computation),
            retired,
            driver,
            progress,
        },
        (control, stage, capacity),
    )
}

async fn run_case(case: Case) {
    run_actual_statistics_case(match case {
        Case::Success => StatisticsRecipeCase::Success,
        Case::TypedRootSuccess => StatisticsRecipeCase::TypedRootSuccess,
        Case::MissingArtifact => StatisticsRecipeCase::MissingArtifact,
        Case::ProtocolFailureFirst => StatisticsRecipeCase::ProtocolFailureFirst,
        Case::OriginalFailureStreamAfterEnd => StatisticsRecipeCase::OriginalFailureStreamAfterEnd,
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_end_precedes_all_success_then_second_receipt_reaches_real_ready() {
    run_case(Case::Success).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_nonempty_program_missing_artifact_refuses_second_receipt_and_ready() {
    run_case(Case::MissingArtifact).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_original_te_failure_before_qa_watch_preserves_exact_task_cause() {
    run_case(Case::ProtocolFailureFirst).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_original_failure_stream_wakes_after_end_without_waiting_for_ready() {
    run_case(Case::OriginalFailureStreamAfterEnd).await;
}

#[tokio::test(flavor = "current_thread")]
async fn typed_root_statistics_real_cpu_ready_releases_stage_while_ready_payload_remains_live() {
    run_case(Case::TypedRootSuccess).await;
}

// A bound fixed-inventory Root creates its actual child through joint admission.
#[derive(Debug)]
struct StatisticsCpuFixtureWake;
impl crate::wake::StatusIntakeWake for StatisticsCpuFixtureWake {
    fn wake(&self) {}
}
struct OriginalStatisticsCpuCause(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for OriginalStatisticsCpuCause {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
// A finite typed phase Source, not an arbitrary Fn callback. The fixture uses
// the same positive factory/offer/receipt and explicitly limits its Provider
// substitution to this component; it proves no Native/Provider IO profile.
struct OriginalStatisticsCpuPhase {
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    epoch: novarocks_spi::connector::ProviderBindingEpoch,
    calls: std::sync::atomic::AtomicUsize,
    panic: bool,
    cause_drops: Arc<std::sync::atomic::AtomicUsize>,
    source_drops: Arc<std::sync::atomic::AtomicUsize>,
}
impl Drop for OriginalStatisticsCpuPhase {
    fn drop(&mut self) {
        self.source_drops
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
impl novarocks_spi::connector::StatisticsSchemaCpuSource for OriginalStatisticsCpuPhase {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.epoch
    }
    fn run_schema(&self) {
        assert_eq!(
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            0
        );
        if self.panic {
            std::panic::panic_any(OriginalStatisticsCpuCause(self.cause_drops.clone()));
        }
    }
}

// Copying the lease's public identity cannot qualify another concrete factory.
struct ForeignStatisticsCpuPhase(Arc<OriginalStatisticsCpuPhase>);
impl novarocks_spi::connector::StatisticsSchemaCpuSource for ForeignStatisticsCpuPhase {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.0.descriptor
    }
    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.0.epoch
    }
    fn run_schema(&self) {
        panic!("foreign factory must never be dispatched");
    }
}

async fn original_statistics_scoped_cpu_case(panic: bool) {
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorProviderId, ProviderBindingEpoch, StatisticsCpuInput,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
    let (mut typed, (control, stage, capacity)) = typed_statistics_custody(&mut owner);
    let scope = stage.scope();
    let binding = QueryResultCapacityBinding::from_internal_payload(
        &typed.root.owner.scope(),
        &scope,
        &typed.payload,
    )
    .unwrap();
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("test").unwrap(),
        instance_id: ConnectorInstanceId::parse("statistics_cpu_component").unwrap(),
    };
    let epoch = ProviderBindingEpoch::new();
    let cause_drops = Arc::new(AtomicUsize::new(0));
    let source_drops = Arc::new(AtomicUsize::new(0));
    let phase = Arc::new(OriginalStatisticsCpuPhase {
        descriptor: descriptor.clone(),
        epoch,
        calls: AtomicUsize::new(0),
        panic,
        cause_drops: cause_drops.clone(),
        source_drops: source_drops.clone(),
    });
    let (port, fixture) = owner
        .runtime()
        .statistics_cpu_fixture(
            &scope,
            binding.try_window_alias().unwrap(),
            descriptor,
            epoch,
            std::any::TypeId::of::<OriginalStatisticsCpuPhase>(),
            QueryCancellationView::governed(scope.cancellation().unwrap(), None),
            tokio::runtime::Handle::current(),
            Arc::new(StatisticsCpuFixtureWake),
        )
        .unwrap();
    let counterfeit = Arc::new(ForeignStatisticsCpuPhase(phase.clone()));
    let mut counterfeit_input = Some(StatisticsCpuInput::Schema(counterfeit.clone()));
    assert!(port.submit_in(&mut counterfeit_input).is_err());
    assert!(
        matches!(counterfeit_input.as_ref(),Some(StatisticsCpuInput::Schema(s))
        if std::ptr::addr_eq(s.as_ref(), counterfeit.as_ref()))
    );
    assert_eq!(phase.calls.load(Ordering::SeqCst), 0);
    drop(counterfeit_input);
    drop(counterfeit);
    // A real generation refusal leaves the exact input at its original caller.
    let foreign = Arc::new(OriginalStatisticsCpuPhase {
        descriptor: phase.descriptor.clone(),
        epoch: ProviderBindingEpoch::new(),
        calls: AtomicUsize::new(0),
        panic: false,
        cause_drops: cause_drops.clone(),
        source_drops: Arc::new(AtomicUsize::new(0)),
    });
    let mut refused = Some(StatisticsCpuInput::Schema(foreign.clone()));
    assert!(port.submit_in(&mut refused).is_err());
    assert!(
        matches!(refused.as_ref(),Some(StatisticsCpuInput::Schema(s))
        if std::ptr::addr_eq(s.as_ref(), foreign.as_ref()))
    );
    assert_eq!(foreign.calls.load(Ordering::SeqCst), 0);
    drop(refused);
    drop(foreign);
    let mut input = Some(StatisticsCpuInput::Schema(phase.clone()));
    port.submit_in(&mut input).unwrap();
    assert!(input.is_none());
    let outcome = watchdog_joint(
        Some(&mut typed),
        "same scoped Statistics physical receipt",
        std::future::poll_fn(|cx| port.poll_finished(cx)),
    )
    .await;
    assert_eq!(phase.calls.load(Ordering::SeqCst), 1);
    assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
    if panic {
        assert!(outcome.is_err());
        let observed =
            novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                "fixture received original Statistics CPU health",
                port.local_failure().unwrap(),
            );
        assert!(fixture.worker_failure_is::<OriginalStatisticsCpuCause>());
        // Cancelling a borrowed close observation cannot replace the original
        // job, cause or window. A later observer joins that same actual source.
        let mut close = Box::pin(std::future::poll_fn(|cx| port.poll_closed(cx)));
        let _ = close
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
        drop(close);
        assert!(
            watchdog_joint(
                Some(&mut typed),
                "same joined Statistics failed close",
                std::future::poll_fn(|cx| port.poll_closed(cx))
            )
            .await
            .is_err()
        );
        assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
        drop(phase);
        assert_eq!(source_drops.load(Ordering::SeqCst), 0);
        assert!(!typed.retired.is_reclaimed().unwrap());
        // Test-only explicit disposal after actual join and same-health intake.
        // This does not set finished or issue a normal exit receipt.
        let disposition = fixture.take_joined_for_disposition(&observed).unwrap();
        drop(disposition);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
        drop(observed);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
    } else {
        outcome.unwrap();
        assert!(port.local_failure().is_none());
        assert!(
            watchdog_joint(
                Some(&mut typed),
                "empty healthy Statistics source close",
                std::future::poll_fn(|cx| port.poll_closed(cx))
            )
            .await
            .is_ok()
        );
        drop(phase);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
    }
    drop(fixture);
    drop(port);
    drop(binding);
    drop(scope);
    drop(typed.computation.take());
    stage.complete();
    drive_typed(&mut typed.driver);
    assert!(typed.retired.is_reclaimed().unwrap());
    drop(typed.payload);
    typed.root.owner.complete();
    typed.root.business.release();
    drive_typed(&mut typed.driver);
    drop(capacity);
    control.close_admission();
    assert!(
        control.shutdown().is_ok(),
        "same original WC must actually drain"
    );
    watchdog(
        "same original scoped CPU owner exits",
        owner.shutdown_until(Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn original_statistics_scoped_cpu_positive_receipt_and_refused_input_preservation() {
    original_statistics_scoped_cpu_case(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn original_statistics_scoped_cpu_panic_keeps_original_source_cause_and_window_until_disposition()
 {
    original_statistics_scoped_cpu_case(true).await;
}
