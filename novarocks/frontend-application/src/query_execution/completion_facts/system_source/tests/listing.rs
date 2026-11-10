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

//! These tests migrate the real view-listing cancellation and CPU-footer
//! contracts to the system-table Source. Only generation selection is seeded:
//! the original typed pin, provider API, I/O admission, metadata CPU and close
//! are production implementations. Statement verdict selection is tested by
//! the completion owner, independently of these raw Source outcomes.
use super::*;
use crate::catalog_application::statement::external_listing_tests::{
    FailurePoint, ListingFixture, ListingMetadataGate,
};
use crate::query_execution::internal_result_cpu::InternalResultCpuOwner;
use novarocks_failure_contract::FailureProjection;
use novarocks_query_application::{
    cancellation::QueryCancellationView, cpu::QueryBlockingExecutorConfig,
};
use novarocks_workload_control::{
    CancellationReason, ControlStorageConfig, ResultCapacityConfig, ResultWindowClass,
    ResultWindowGrant, RootWork, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    joint_admission::{JointProgress, JointProgressDriver, ResponsibilityConfig},
};
use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

async fn driven<F: Future>(
    control: &WorkloadControl,
    driver: &mut JointProgressDriver,
    future: F,
) -> F::Output {
    tokio::pin!(future);
    loop {
        match driver.step() {
            JointProgress::More => tokio::task::yield_now().await,
            JointProgress::Idle(revision) => tokio::select! {
                biased;
                value = &mut future => return value,
                _ = control.wait_progress(revision) => tokio::task::yield_now().await,
            },
        }
    }
}
struct Fixture {
    source: Option<Arc<FrontendSystemTableFacts>>,
    cpu: InternalResultCpuOwner,
    physical: crate::task_execution::blocking_io::tests::OriginalExecutorsFixture,
    control: WorkloadControl,
    driver: JointProgressDriver,
    root: RootWork,
    window: Option<ResultWindowGrant>,
}
impl Fixture {
    async fn new(provider: &ListingFixture, class: ResultWindowClass) -> Self {
        Self::new_in(
            provider,
            class,
            Arc::new(
                novarocks_query_application::system_tables::SystemTableRegistry::try_new([])
                    .unwrap(),
            ),
            ResultCapacityConfig::V1,
            Duration::from_secs(5),
        )
        .await
    }
    async fn new_in(
        provider: &ListingFixture,
        class: ResultWindowClass,
        registry: Arc<novarocks_query_application::system_tables::SystemTableRegistry>,
        capacity_config: ResultCapacityConfig,
        compile_duration: Duration,
    ) -> Self {
        let mut control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: 8,
            business_limit: 8,
            query_concurrency_limit: 8,
            scope_records_limit: 8,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (_, mut driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control
            .configure_control_storage(ControlStorageConfig::new(8, &[]))
            .unwrap();
        control.configure_result_capacity(capacity_config).unwrap();
        let cpu = InternalResultCpuOwner::try_new(
            QueryBlockingExecutorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ),
            &mut control,
        )
        .unwrap();
        let (physical, blocking) = crate::task_execution::blocking_io::tests::original_executors(
            tokio::runtime::Handle::current(),
        );
        blocking
            .install_provider_creator_cpu(cpu.runtime())
            .unwrap();
        control.mark_ready().unwrap();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest {
                    class: WorkClass::Management,
                    deadline: None,
                },
                class,
            )
            .unwrap();
        let scope = root.owner.scope();
        let io = crate::query_execution::maintenance::io_admission::MetadataIoAdmission::for_original_source(&root.owner, Vec::new()).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            driven(
                &control,
                &mut driver,
                std::future::poll_fn(|cx| {
                    if io.poll_progress(&root.owner, cx).unwrap() {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }),
            ),
        )
        .await
        .unwrap();
        let context = crate::connector::test_request_context().with_io_admission(io.clone());
        let cancellation =
            QueryCancellationView::governed(scope.cancellation().unwrap(), Some(1000));
        let compile = novarocks_sql::compiler::SqlCompileControl::new(
            Some(Instant::now() + compile_duration),
            crate::query_execution::planning::sql_cancellation_observation(cancellation.clone()),
        );
        let capacity = QueryResultCapacityBinding::try_new(&scope, window.retain_alias()).unwrap();
        let scope = StatementFactScope::binding_only(
            Arc::new(QueryTableBindingStore::try_new().unwrap()),
            context.clone(),
            Some("catalog"),
            &scope,
            capacity,
        )
        .unwrap()
        .with_snapshot_planning(
            compile,
            cancellation,
            None,
            FrontendPlanningCpuSource::new(blocking.clone()),
        );
        let source = super::source_in_registry(blocking, scope, registry).await;
        let pin = crate::connector::metadata_binding_typed(provider.registry.as_ref(), "catalog")
            .unwrap();
        let pin = Arc::new(PinnedCatalogListing::new(pin, context).unwrap());
        source.io.lock().unwrap().pins.insert("catalog".into(), pin);
        Self {
            source: Some(source),
            cpu,
            physical,
            control,
            driver,
            root,
            window: Some(window),
        }
    }
    fn source(&self) -> Arc<FrontendSystemTableFacts> {
        self.source.as_ref().unwrap().clone()
    }
    async fn finish(mut self, cancelled: bool) {
        let source = self.source.take().unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            driven(&self.control, &mut self.driver, source.close()),
        )
        .await
        .unwrap()
        .unwrap();
        if let Some(cpu) = source
            .scope
            .planning
            .as_ref()
            .and_then(|planning| planning.cpu.as_ref())
        {
            tokio::time::timeout(Duration::from_secs(5), cpu.close())
                .await
                .unwrap()
                .unwrap();
        }
        drop(source);
        drop(self.window.take());
        assert_eq!(
            self.control.snapshot().result_windows.held_positions,
            [0; 4],
            "all original aliases retired after actual close"
        );
        if cancelled {
            self.root.owner.complete_after_terminal_cancel_settled();
        } else {
            self.root.owner.complete();
        }
        self.root.business.release();
        self.cpu
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        self.physical.shutdown().await;
        drop(self.driver);
        drop(self.control);
    }
}
fn assert_original_error(response: Response, kind: ConnectorErrorKind, provider: &ListingFixture) {
    let Response::Names(Err(SystemTableError::Provider(error))) = response else {
        panic!("same original listing error")
    };
    assert_eq!(error.kind(), kind);
    assert_eq!(error.to_string(), provider.expected_error());
    let SqlCompileError::CompilationSource(cause) = sql_error(error.into()) else {
        panic!("typed compile cause")
    };
    assert_eq!(
        cause.failure_fact(),
        novarocks_failure_contract::FailureProjection::failure_fact(&kind)
    );
}

#[tokio::test]
async fn cancelled_listing_borrow_and_close_retain_the_original_raw_outcome() {
    for class in [ResultWindowClass::Client, ResultWindowClass::Local] {
        let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::Cancelled);
        let gate = Arc::new(tokio::sync::Notify::new());
        provider.hold_views(gate.clone());
        let fixture = Fixture::new(&provider, class).await;
        let source = fixture.source();
        let mut borrowed =
            Box::pin(source.view_names("catalog", "db", &source.scope.connector_context));
        assert!(futures::poll!(borrowed.as_mut()).is_pending());
        assert_eq!(provider.calls(), ["views-start"]);
        drop(borrowed);
        fixture.root.owner.cancel(CancellationReason::ExplicitKill {
            requester_session: 23,
        });
        let mut close = Box::pin(source.close());
        assert!(
            futures::poll!(close.as_mut()).is_pending(),
            "Root stop cannot manufacture provider exit"
        );
        drop(close);
        assert!(source.io.lock().unwrap().future.is_some());
        assert!(!source.io.lock().unwrap().joined);
        assert!(source.local_health_failure().is_none());
        gate.notify_one();
        assert_original_error(
            source.receive().await,
            ConnectorErrorKind::Cancelled,
            &provider,
        );
        assert_eq!(provider.calls(), ["views-start", "views"]);
        drop(source);
        fixture.finish(true).await;
    }
}

#[tokio::test]
async fn same_poll_root_stop_does_not_reclassify_the_raw_listing_error() {
    let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::CorruptData);
    let gate = Arc::new(tokio::sync::Notify::new());
    provider.hold_views(gate.clone());
    let fixture = Fixture::new(&provider, ResultWindowClass::Client).await;
    provider.stop_inside_view_poll(fixture.root.owner.cancellation_requester());
    let source = fixture.source();
    let mut borrowed =
        Box::pin(source.view_names("catalog", "db", &source.scope.connector_context));
    assert!(futures::poll!(borrowed.as_mut()).is_pending());
    drop(borrowed);
    gate.notify_one();
    assert_original_error(
        source.receive().await,
        ConnectorErrorKind::CorruptData,
        &provider,
    );
    assert!(
        fixture
            .root
            .owner
            .scope()
            .cancellation()
            .unwrap()
            .reason()
            .is_some()
    );
    drop(source);
    fixture.finish(true).await;
}

#[tokio::test]
async fn original_metadata_cpu_footer_precedes_source_close_and_window_retirement() {
    struct Release(Arc<ListingMetadataGate>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::CorruptData);
    let gate = Arc::new(ListingMetadataGate::default());
    provider.hold_metadata_cpu(gate.clone());
    let mut fixture = Fixture::new(&provider, ResultWindowClass::Client).await;
    let _release_on_panic = Release(gate.clone());
    let source = fixture.source();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        driven(
            &fixture.control,
            &mut fixture.driver,
            source.view_names("catalog", "db", &source.scope.connector_context),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        matches!(error, SystemTableError::Provider(ref error) if error.kind() == ConnectorErrorKind::CorruptData)
    );
    assert!(provider.calls().iter().any(|call| call == "metadata-start"));
    let mut close = Box::pin(source.close());
    assert!(
        futures::poll!(close.as_mut()).is_pending(),
        "returned Provider error does not prove actual CPU exit"
    );
    drop(close);
    assert!(!source.io.lock().unwrap().ports.is_empty());
    assert!(!source.io.lock().unwrap().pins.is_empty());
    assert_eq!(
        fixture
            .control
            .snapshot()
            .result_windows
            .held_positions
            .iter()
            .sum::<usize>(),
        1
    );
    fixture.root.owner.cancel(CancellationReason::ExplicitKill {
        requester_session: 29,
    });
    let mut close = Box::pin(source.close());
    assert!(futures::poll!(close.as_mut()).is_pending());
    drop(close);
    assert!(source.local_health_failure().is_none());
    gate.release();
    tokio::time::timeout(
        Duration::from_secs(5),
        driven(&fixture.control, &mut fixture.driver, source.close()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(source.io.lock().unwrap().ports.is_empty());
    assert!(source.io.lock().unwrap().pins.is_empty());
    drop(source);
    fixture.finish(true).await;
}

mod completion;

mod scale;
