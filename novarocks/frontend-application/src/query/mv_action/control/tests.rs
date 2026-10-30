// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::query_execution::native_execution_adapter::source_cohort::tests as cohort;
use novarocks_mv_application::{
    process_runtime::ProcessRuntime,
    readiness::{MvKeyedProjectionInstallOutcome, MvKeyedReadinessService},
};
use novarocks_query_application::cpu::{QueryBlockingExecutorConfig, QueryBlockingExecutorOwner};
use novarocks_spi::connector::{
    ConnectorIoAdmissionPort, ConnectorRequestContext, ConnectorStopOwner,
};
use novarocks_state_store_api::{
    StateStoreExecutionRequests, StateStoreOpenRequest, StateStoreProviderFactory,
    StateStoreReadExecution,
};
use novarocks_state_store_sqlite::SqliteStateStoreProviderFactory;
use novarocks_workload_control::{CancellationReason, WorkClass, WorkRequest};
use source::call::tests::Fixture;
use std::{
    future::Future,
    num::NonZeroUsize,
    task::Waker,
    time::{Duration, Instant},
};

async fn drive<F: Future>(
    owner: &StateStoreOriginalOwner,
    io: &Arc<MetadataIoAdmission>,
    driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    future: F,
) -> F::Output {
    tokio::pin!(future);
    tokio::time::timeout(
        Duration::from_secs(10),
        std::future::poll_fn(|cx| {
            owner.poll_progress(cx).unwrap();
            io.poll_continuing(cx).unwrap();
            let result = future.as_mut().poll(cx);
            owner.poll_progress(cx).unwrap();
            io.poll_continuing(cx).unwrap();
            if matches!(
                driver.step(),
                novarocks_workload_control::joint_admission::JointProgress::More
            ) {
                cx.waker().wake_by_ref()
            }
            result
        }),
    )
    .await
    .expect("actual original SQLite and QB footer progress")
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_nonclone_drop_guard_uses_only_applied_finite_requests_after_root_cancel() {
    // Open is fixture setup only. Every repository operation below uses the
    // actual SQLite original API and the same governed QB=1 Root inventory.
    let dir = tempfile::tempdir().unwrap();
    let mut instance = Box::new(SqliteStateStoreProviderFactory::new(
        dir.path().join("control.sqlite"),
    ))
    .open(StateStoreOpenRequest {
        cluster_id: "mv-control-original".into(),
        limits: Default::default(),
        deadline: Instant::now() + Duration::from_secs(10),
    })
    .await
    .unwrap();
    let store = instance.state_store().unwrap();
    let mut workers = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let (control, mut driver, _) = cohort::fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let io = MetadataIoAdmission::for_original_source(
        &root.owner,
        instance.blocking_dependency_facts().unwrap(),
    )
    .unwrap();
    loop {
        if io
            .poll_progress(&root.owner, &mut Context::from_waker(Waker::noop()))
            .unwrap()
        {
            break;
        }
        cohort::drive(&mut driver)
    }
    let mut original = StateStoreOriginalOwner::for_root(
        &workers.executor(),
        instance.blocking_source_vocabulary().unwrap(),
        instance.blocking_instance_scope().unwrap(),
        root.owner.scope(),
        Default::default(),
    )
    .unwrap();
    let requests = Arc::new(requests::DropProjectionRequests::new(Arc::new(
        original
            .execution_requests(store.limits().transaction_deadline, None)
            .unwrap(),
    )));
    let repository =
        novarocks_mv_application::state_store_repository::StateStoreMvRepository::open(
            store.clone(),
            Default::default(),
        )
        .await
        .unwrap();
    let service =
        MvKeyedReadinessService::new(repository.clone(), Arc::new(ProcessRuntime::default()));
    let view = service.in_requests(requests.clone());
    let target = MvTarget::from_parts(Some("c"), "db", "mv");
    let facts =
        crate::mv::domain::test_admission::sample_keyed_projection(target.clone(), Some(10));
    drive(
        &original,
        &io,
        &mut driver,
        view.seed_projection(uuid::Uuid::now_v7(), facts),
    )
    .await
    .unwrap();
    let MvDropReadiness::ReadyToDrop(guard) = drive(
        &original,
        &io,
        &mut driver,
        view.prepare_drop(&target, false),
    )
    .await
    .unwrap() else {
        panic!("real seeded projection must supply its unique guard")
    };
    assert!(guard.has_projection());
    let stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        stop.view(),
        1024,
        4096,
    )
    .unwrap()
    .with_io_admission(io.clone());
    // These fixtures publish through the real catalog lease/receiving author.
    // They exercise selection, not Iceberg's journal budget construction.
    let no_op = Fixture::new(false, context.clone());
    let no_op_call = no_op.call();
    no_op_call.poll_progress(&mut Context::from_waker(Waker::noop()));
    assert!(
        requests
            .select_applied(
                &no_op_call.receiving,
                &io,
                &root.owner.scope(),
                &original,
                store.limits().transaction_deadline
            )
            .is_err()
    );
    assert!(!requests.selected());
    root.owner.cancel(CancellationReason::Requested);
    assert!(requests.read_transaction_context().is_err());
    assert!(requests.write_operation_context().is_err());
    io.publication_obligation_started();
    let applied = Fixture::new(true, context.clone());
    let call = applied.call();
    call.poll_progress(&mut Context::from_waker(Waker::noop()));
    assert!(call.receiving.observation().needed_committed().is_some());
    assert!(!call.receiving.observation().provider_exited());
    let selected = requests
        .select_applied(
            &call.receiving,
            &io,
            &root.owner.scope(),
            &original,
            store.limits().transaction_deadline,
        )
        .unwrap();
    assert_eq!(selected.deadline(), context.deadline());
    assert!(requests.read_transaction_context().is_ok());
    assert!(
        requests
            .select_applied(
                &call.receiving,
                &io,
                &root.owner.scope(),
                &original,
                store.limits().transaction_deadline
            )
            .is_err()
    );
    let removed = drive(
        &original,
        &io,
        &mut driver,
        view.delete_after_provider_drop(uuid::Uuid::now_v7(), guard),
    )
    .await
    .unwrap();
    assert!(matches!(removed, MvKeyedProjectionInstallOutcome::Removed));
    assert!(
        drive(
            &original,
            &io,
            &mut driver,
            view.load_ready_settled(&target)
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(
        applied.drops.load(std::sync::atomic::Ordering::Acquire),
        0,
        "business deletion does not retire Provider GC"
    );
    applied.release();
    no_op.release();
    call.poll_progress(&mut Context::from_waker(Waker::noop()));
    no_op_call.poll_progress(&mut Context::from_waker(Waker::noop()));
    assert!(call.receiving.observation().provider_exited());
    drop((
        call, no_op_call, applied, no_op, view, service, repository, requests,
    ));
    tokio::time::timeout(
        Duration::from_secs(10),
        std::future::poll_fn(|cx| original.poll_closed(cx)),
    )
    .await
    .unwrap()
    .unwrap();
    drop(original);
    drop(store);
    instance
        .shutdown(Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();
    drop(instance);
    drop(context);
    drop(io);
    cohort::finish(control, root, driver);
    workers
        .shutdown_until(Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();
}
#[test]
fn shared_configuration_transition_preserves_policy_pause_and_duration_rules() {
    use crate::mv::domain::{
        application::{MvAlterAction, MvCreateRefreshPolicy},
        flow::configuration_after_alter,
        iceberg_refresh::configuration_document,
    };
    use novarocks_mv_application::persistence::codec::{ConfigurationDocument, RefreshPolicy};
    let initial = ConfigurationDocument {
        refresh_policy: RefreshPolicy::AsyncInterval,
        paused: false,
        refresh_interval_ms: Some(5000),
        max_staleness_ms: Some(7000),
    };
    let paused = configuration_document(
        configuration_after_alter(&initial, &MvAlterAction::PauseRefresh).unwrap(),
    )
    .unwrap();
    assert!(paused.paused);
    assert_eq!(paused.refresh_interval_ms, Some(5000));
    assert_eq!(paused.max_staleness_ms, Some(7000));
    let resumed = configuration_document(
        configuration_after_alter(&paused, &MvAlterAction::ResumeRefresh).unwrap(),
    )
    .unwrap();
    assert!(!resumed.paused);
    let manual = configuration_document(
        configuration_after_alter(
            &paused,
            &MvAlterAction::SetRefresh(MvCreateRefreshPolicy::Manual),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(manual.refresh_policy, RefreshPolicy::Manual);
    assert!(manual.paused);
    assert_eq!(manual.refresh_interval_ms, None);
    assert!(
        configuration_after_alter(
            &initial,
            &MvAlterAction::SetRefresh(MvCreateRefreshPolicy::AsyncInterval { interval_ms: -1 })
        )
        .is_err()
    );
}
