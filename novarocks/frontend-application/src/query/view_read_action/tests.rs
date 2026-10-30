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

//! Actual original Local/closed-CPU composition; no Native or MV owner.
use super::*;
use crate::catalog_application::statement::external_listing_tests::{FailurePoint, ListingFixture};
use crate::query_execution::internal_result_cpu::InternalResultCpuOwner;
use novarocks_failure_contract::FailureProjection;
use novarocks_query_application::{
    admitted_query_context::{QueryResultCapacityBinding, RequestAdmission},
    api::BackendTopologySnapshot,
    cpu::QueryBlockingExecutorConfig,
    session_control::{ClientSessionKey, SessionToken, StatementToken},
    view::QueryViewService,
};
use novarocks_workload_control::{
    CancellationReason, ControlStorageConfig, ResultCapacityConfig, ResultWindowGrant, RootWork,
    WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    joint_admission::{JointProgress, JointProgressDriver, ResponsibilityConfig},
};
use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

async fn with_original_driver<F: Future>(
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
                result = &mut future => return result,
                _ = control.wait_progress(revision) => tokio::task::yield_now().await,
            },
        }
    }
}
struct Fixture {
    cpu: InternalResultCpuOwner,
    control: WorkloadControl,
    driver: JointProgressDriver,
    root: RootWork,
    local: Option<ResultWindowGrant>,
    state: Option<State>,
}
impl Fixture {
    async fn new() -> Self {
        Self::for_input(None, false).await
    }
    async fn for_input(provider: Option<&ListingFixture>, root_deadline: bool) -> Self {
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
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        let cpu = InternalResultCpuOwner::try_new(
            QueryBlockingExecutorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ),
            &mut control,
        )
        .unwrap();
        control.mark_ready().unwrap();
        let kernel = ViewExecutionKernel::new(
            Arc::new(novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap()),
            provider
                .map(|p| p.catalog_service.clone())
                .unwrap_or_else(|| {
                    Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service())
                }),
            None,
            provider
                .map(|p| {
                    p.registry.clone()
                        as Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>
                })
                .unwrap_or_else(|| {
                    Arc::new(novarocks_catalog_application::ConnectorControlHost::new())
                }),
            Arc::new(QueryViewService::new()),
            crate::application::test_constant_policy(),
        );
        let deadline = root_deadline.then(|| Instant::now() + Duration::from_secs(1));
        let (root, local) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest {
                    class: WorkClass::Management,
                    deadline: deadline.map(Into::into),
                },
                ResultWindowClass::Local,
            )
            .unwrap();
        let scope = root.owner.scope();
        let origin = local.retain_alias();
        let cancellation =
            QueryCancellationView::governed(scope.cancellation().unwrap(), Some(1000));
        let request = RequestContext::admit(RequestAdmission::new(
            provider.map(|_| "catalog".to_string()),
            "db".into(),
            novarocks_types::ClusterRole::Fe,
            BackendTopologySnapshot::empty(0),
            deadline.or_else(|| Some(Instant::now() + Duration::from_secs(5))),
            cancellation.clone(),
            Default::default(),
            Default::default(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        ))
        .with_result_capacity(QueryResultCapacityBinding::try_new(&scope, origin.clone()).unwrap())
        .unwrap();
        let command = CommandContext::new(
            scope.clone(),
            crate::connector::test_request_context(),
            StatementToken::new(SessionToken::new(ClientSessionKey::new(17).unwrap()), 1),
            "test-principal",
        )
        .with_result_window(origin.clone())
        .unwrap();
        let statements = novarocks_parser::parse("SHOW VIEWS").unwrap();
        let [
            novarocks_parser::ast::Statement::View(novarocks_parser::ast::ViewStatement::Show(
                show,
            )),
        ] = statements.as_slice()
        else {
            panic!("SHOW fixture")
        };
        let io = MetadataIoAdmission::for_original_source(&root.owner, Vec::new()).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            with_original_driver(
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
        let context = command
            .connector_context()
            .clone()
            .with_io_admission(io.clone());
        let original = Arc::new(ViewShowOriginal {
            body: Mutex::new(Some(Body {
                input: Some(Input {
                    kernel,
                    statement: show.clone(),
                    request,
                    command,
                }),
                context: Some(context),
                turn: Turn::Prepare,
                prepared: None,
                lease: None,
                names: None,
                raw: None,
                producer: Some(
                    LocalResultProducer::try_new(
                        &scope,
                        origin.clone(),
                        novarocks_result_contract::ClientPresentationFamily::MysqlText,
                    )
                    .unwrap(),
                ),
                result: None,
            })),
            cpu_unreturned: AtomicBool::new(false),
        });
        let state = State {
            runtime: tokio::runtime::Handle::current(),
            cancellation,
            scope,
            origin: Some(origin),
            producer: None,
            input: None,
            io: Some(io),
            original: Some(original),
            future: None,
            job: None,
            provider_cpu: None,
            marker: None,
            cpu: cpu.runtime(),
            raw_health: None,
            ordinary_io: None,
            wake: Arc::new(Wake::default()),
            in_foreign: false,
            selected: false,
            closed: false,
        };
        Self {
            cpu,
            control,
            driver,
            root,
            local: Some(local),
            state: Some(state),
        }
    }
    async fn until_held(&mut self, provider: &ListingFixture) {
        let state = self.state.as_mut().unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            with_original_driver(
                &self.control,
                &mut self.driver,
                std::future::poll_fn(|cx| {
                    state.wake.0.register(cx.waker());
                    let result = state.poll_prepared(cx);
                    assert!(
                        result.is_pending(),
                        "held Provider cannot complete the original Action"
                    );
                    if provider.calls() == ["views-start"] {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }),
            ),
        )
        .await
        .unwrap();
        assert!(self.state.as_ref().unwrap().future.is_some());
    }
    async fn receive_original_io(&mut self) {
        let state = self.state.as_mut().unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            with_original_driver(
                &self.control,
                &mut self.driver,
                std::future::poll_fn(|cx| {
                    state.progress(cx);
                    let ready = state.poll_io(cx);
                    state.progress(cx);
                    ready
                }),
            ),
        )
        .await
        .unwrap();
        assert!(
            !state.selected,
            "receiving a raw outcome is earlier than ActionReady"
        );
    }
    async fn business(&mut self) -> Result<StatementActionValue, QueryServiceError> {
        let state = self.state.as_mut().unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            with_original_driver(
                &self.control,
                &mut self.driver,
                std::future::poll_fn(|cx| {
                    state.wake.0.register(cx.waker());
                    state.poll_prepared(cx)
                }),
            ),
        )
        .await
        .expect("same original CPU and Local source progress")
    }
    async fn close(mut self, cancelled: bool) {
        let state = self.state.as_mut().unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            with_original_driver(
                &self.control,
                &mut self.driver,
                std::future::poll_fn(|cx| state.poll_close(cx)),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        drop(self.state.take());
        drop(self.local.take());
        if cancelled {
            self.root.owner.complete_after_terminal_cancel_settled();
        } else {
            self.root.owner.complete();
        }
        self.root.business.release();
        drop(self.driver);
        drop(self.control);
        self.cpu
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}
#[tokio::test(flavor = "current_thread")]
async fn original_show_qb1_returns_its_local_graph_after_cpu_actual_exit() {
    let mut fixture = Fixture::new().await;
    let original = fixture
        .state
        .as_ref()
        .unwrap()
        .original
        .as_ref()
        .unwrap()
        .clone();
    let StatementActionValue::Local(result) = fixture.business().await.unwrap() else {
        panic!("SHOW Local result")
    };
    assert_eq!(result.row_count(), 0);
    assert_eq!(
        result
            .columns()
            .map(|c| c.name.to_owned())
            .collect::<Vec<_>>(),
        ["Views_in_db"]
    );
    assert!(fixture.state.as_ref().unwrap().job.is_none());
    assert!(!original.cpu_unreturned.load(Ordering::Acquire));
    assert!(fixture.state.as_ref().unwrap().local_health().is_none());
    drop(fixture.state.as_mut().unwrap().origin.take());
    drop(fixture.local.take());
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
    drop(result);
    assert_eq!(
        fixture.control.snapshot().result_windows.held_positions,
        [0; 4]
    );
    drop(original);
    fixture.close(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn cancelled_show_borrow_keeps_the_same_child_and_original_typed_receipt() {
    let mut fixture = Fixture::new().await;
    let original = fixture
        .state
        .as_ref()
        .unwrap()
        .original
        .as_ref()
        .unwrap()
        .clone();
    let weak = Arc::downgrade(&original);
    let gate = Arc::new(tokio::sync::Notify::new());
    let gate_child = gate.clone();
    let source = original.clone();
    let injected = ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        "original SHOW provider refusal",
    );
    let expected = connector(injected.clone())
        .failure_cause()
        .unwrap()
        .failure_fact();
    fixture.state.as_mut().unwrap().future = Some(Box::pin(async move {
        let mut loan = BodyLoan::take(&source);
        gate_child.notified().await;
        let body = loan.body.as_mut().unwrap();
        body.raw = Some(Err(injected.clone()));
        body.refuse(connector(injected));
    }));
    {
        let state = fixture.state.as_mut().unwrap();
        let mut borrowed = Box::pin(std::future::poll_fn(|cx| state.poll_io(cx)));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(borrowed.as_mut().poll(&mut cx).is_pending());
    }
    assert!(
        original.body.lock().unwrap().is_none(),
        "same actual child holds the body loan"
    );
    assert!(fixture.state.as_ref().unwrap().future.is_some());
    fixture.root.owner.cancel(CancellationReason::Requested);
    gate.notify_one();
    let error = match fixture.business().await {
        Err(e) => e,
        Ok(_) => panic!("cancelled read cannot succeed"),
    };
    assert_eq!(error.execution_failure().unwrap().fact(), expected);
    assert!(fixture.state.as_ref().unwrap().future.is_none());
    assert!(fixture.state.as_ref().unwrap().local_health().is_none());
    assert!(weak.upgrade().is_some());
    drop(original);
    fixture.close(true).await;
    assert!(weak.upgrade().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn held_provider_root_deadline_and_kill_keep_query_verdict_and_raw_outcome() {
    for deadline in [true, false] {
        let kind = if deadline {
            ConnectorErrorKind::DeadlineExceeded
        } else {
            ConnectorErrorKind::Cancelled
        };
        let provider = ListingFixture::new(FailurePoint::Views, kind);
        let gate = Arc::new(tokio::sync::Notify::new());
        provider.hold_views(gate.clone());
        let mut fixture = Fixture::for_input(Some(&provider), deadline).await;
        fixture.until_held(&provider).await;
        let original = fixture
            .state
            .as_ref()
            .unwrap()
            .original
            .as_ref()
            .unwrap()
            .clone();
        assert!(
            fixture
                .state
                .as_ref()
                .unwrap()
                .cancellation
                .reason()
                .is_none()
        );
        if deadline {
            let deadline = fixture
                .root
                .owner
                .scope()
                .cancellation()
                .unwrap()
                .deadline()
                .unwrap();
            tokio::time::sleep_until(deadline).await;
        } else {
            fixture.root.owner.cancel(CancellationReason::ExplicitKill {
                requester_session: 23,
            });
        }
        {
            let state = fixture.state.as_mut().unwrap();
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(
                state.poll_io(&mut cx).is_pending(),
                "a chosen Root verdict cannot release the held child"
            );
        }
        let reason = fixture
            .state
            .as_ref()
            .unwrap()
            .cancellation
            .reason()
            .expect("same Root admitted its actual stop");
        let expected = super::super::cancellation_error(reason);
        assert!(original.body.lock().unwrap().is_none());
        assert!(fixture.state.as_ref().unwrap().future.is_some());
        assert!(!fixture.state.as_ref().unwrap().selected);
        gate.notify_one();
        let error = match fixture.business().await {
            Err(e) => e,
            Ok(_) => panic!("Root stop cannot succeed"),
        };
        assert_eq!(
            error, expected,
            "the canonical verdict comes from the original Query author"
        );
        {
            let body = original.body.lock().unwrap();
            let raw = body
                .as_ref()
                .unwrap()
                .raw
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err();
            assert_eq!(raw.kind(), kind);
            assert_eq!(raw.failure_fact(), kind.failure_fact());
            assert_eq!(raw.to_string(), provider.expected_error());
            assert!(
                body.as_ref().unwrap().input.is_none(),
                "completed Local aliases retired after actual child close"
            );
        }
        assert_eq!(provider.calls(), ["views-start", "views"]);
        assert!(fixture.state.as_ref().unwrap().future.is_none());
        assert!(fixture.state.as_ref().unwrap().job.is_none());
        assert!(fixture.state.as_ref().unwrap().local_health().is_none());
        drop(fixture.state.as_mut().unwrap().origin.take());
        drop(fixture.local.take());
        assert_eq!(
            fixture.control.snapshot().result_windows.held_positions,
            [0; 4]
        );
        drop(original);
        fixture.close(true).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn received_provider_errors_win_late_root_and_independent_control_stays_provider_owned() {
    for kind in [
        ConnectorErrorKind::CorruptData,
        ConnectorErrorKind::ResourceExhausted,
        ConnectorErrorKind::Cancelled,
    ] {
        for late_root in [false, true] {
            let provider = ListingFixture::new(FailurePoint::Views, kind);
            let gate = Arc::new(tokio::sync::Notify::new());
            provider.hold_views(gate.clone());
            let mut fixture = Fixture::for_input(Some(&provider), false).await;
            fixture.until_held(&provider).await;
            gate.notify_one();
            fixture.receive_original_io().await;
            let original = fixture
                .state
                .as_ref()
                .unwrap()
                .original
                .as_ref()
                .unwrap()
                .clone();
            let selected = original
                .body
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .as_ref()
                .err()
                .unwrap()
                .clone();
            assert_eq!(
                selected.execution_failure().unwrap().fact(),
                kind.failure_fact()
            );
            if late_root {
                fixture
                    .root
                    .owner
                    .cancel(CancellationReason::DeadlineExceeded);
            }
            let error = match fixture.business().await {
                Err(e) => e,
                Ok(_) => panic!("selected Provider error cannot succeed"),
            };
            assert_eq!(
                error, selected,
                "a raw Err selected before this stop cannot be reclassified"
            );
            assert!(error.failure_cause().is_some());
            {
                let body = original.body.lock().unwrap();
                assert_eq!(
                    body.as_ref()
                        .unwrap()
                        .raw
                        .as_ref()
                        .unwrap()
                        .as_ref()
                        .unwrap_err()
                        .kind(),
                    kind
                );
            }
            assert_eq!(provider.calls(), ["views-start", "views"]);
            drop(original);
            fixture.close(late_root).await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn original_cpu_receipt_closes_before_cancelled_action_can_release_local() {
    let mut fixture = Fixture::new().await;
    {
        let state = fixture.state.as_mut().unwrap();
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(state.poll_prepared(&mut cx).is_pending());
        assert!(
            state.job.is_some(),
            "same closed creator packet owns its original CPU receipt"
        );
    }
    fixture.root.owner.cancel(CancellationReason::ExplicitKill {
        requester_session: 23,
    });
    let error = match fixture.business().await {
        Err(e) => e,
        Ok(_) => panic!("stopped preparation cannot succeed"),
    };
    assert!(error.execution_failure().is_some());
    let state = fixture.state.as_ref().unwrap();
    assert!(
        state.job.is_none(),
        "real worker and forwarder footer preceded business transfer"
    );
    assert!(
        !state
            .original
            .as_ref()
            .unwrap()
            .cpu_unreturned
            .load(Ordering::Acquire)
    );
    assert!(
        state.local_health().is_none(),
        "ordinary stop is not unproven physical exit"
    );
    drop(fixture.state.as_mut().unwrap().origin.take());
    drop(fixture.local.take());
    assert_eq!(
        fixture.control.snapshot().result_windows.held_positions,
        [0; 4]
    );
    fixture.close(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn selected_action_ready_success_is_not_revisited_by_residual_cancellation() {
    let mut fixture = Fixture::new().await;
    let StatementActionValue::Local(result) = fixture.business().await.unwrap() else {
        panic!("Local result")
    };
    assert!(fixture.state.as_ref().unwrap().selected);
    fixture.root.owner.cancel(CancellationReason::ExplicitKill {
        requester_session: 23,
    });
    assert_eq!(
        result.row_count(),
        0,
        "the already-selected graph is the same successful graph"
    );
    drop(result);
    fixture.close(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn raw_error_ready_inside_the_poll_that_admits_root_stop_is_not_reclassified() {
    let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::CorruptData);
    let gate = Arc::new(tokio::sync::Notify::new());
    provider.hold_views(gate.clone());
    let mut fixture = Fixture::for_input(Some(&provider), false).await;
    // The existing fixture receives the ORIGINAL Root's explicit test write
    // capability. The read-only cancel view never manufactures that right.
    provider.stop_inside_view_poll(fixture.root.owner.cancellation_requester());
    fixture.until_held(&provider).await;
    assert!(
        fixture
            .state
            .as_ref()
            .unwrap()
            .cancellation
            .reason()
            .is_none()
    );
    gate.notify_one();
    fixture.receive_original_io().await;
    assert!(
        fixture
            .state
            .as_ref()
            .unwrap()
            .cancellation
            .reason()
            .is_some()
    );
    let error = match fixture.business().await {
        Err(e) => e,
        Ok(_) => panic!("same-poll raw error cannot succeed"),
    };
    assert_eq!(
        error.execution_failure().unwrap().fact(),
        ConnectorErrorKind::CorruptData.failure_fact()
    );
    assert_eq!(
        error.failure_cause().unwrap().failure_fact(),
        ConnectorErrorKind::CorruptData.failure_fact()
    );
    fixture.close(true).await;
}

/// The existing adapter-error regression uses this same positive Root,
/// planning lease, exact author and closed factory rather than a dummy port.
pub(crate) async fn assert_listing_error_text(kind: ConnectorErrorKind) {
    let provider = ListingFixture::new(FailurePoint::Views, kind);
    let mut fixture = Fixture::for_input(Some(&provider), false).await;
    let error = match fixture.business().await {
        Err(error) => error,
        Ok(_) => panic!("fault fixture cannot succeed"),
    };
    let original = fixture
        .state
        .as_ref()
        .unwrap()
        .original
        .as_ref()
        .unwrap()
        .clone();
    {
        let body = original.body.lock().unwrap();
        let raw = body
            .as_ref()
            .unwrap()
            .raw
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err();
        assert_eq!(raw.kind(), kind);
        assert_eq!(raw.to_string(), provider.expected_error());
        assert_eq!(
            error.failure_cause().unwrap().failure_fact(),
            raw.failure_fact()
        );
        assert_eq!(
            error.execution_failure().unwrap().detail().as_str(),
            provider.expected_error()
        );
        assert_eq!(provider.calls(), ["views"]);
        assert!(
            fixture.state.as_ref().unwrap().provider_cpu.is_none(),
            "same exact port closed before alias retirement"
        );
    }
    drop(original);
    fixture.close(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn exact_provider_metadata_factory_exits_before_view_input_aliases_retire() {
    let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::CorruptData);
    provider.form_metadata_in_views();
    let mut fixture = Fixture::for_input(Some(&provider), false).await;
    let error = match fixture.business().await {
        Err(error) => error,
        Ok(_) => panic!("fault fixture cannot succeed"),
    };
    assert_eq!(
        error.failure_cause().unwrap().failure_fact(),
        ConnectorErrorKind::CorruptData.failure_fact()
    );
    let calls = provider.calls();
    assert_eq!(calls.len(), 3);
    for phase in ["metadata-submit", "metadata-cpu", "views"] {
        assert_eq!(
            calls.iter().filter(|call| call.as_str() == phase).count(),
            1
        );
    }
    assert_eq!(calls.last().unwrap(), "views");
    let state = fixture.state.as_ref().unwrap();
    assert!(
        state.provider_cpu.is_none(),
        "the original port actually closed before input retirement"
    );
    assert!(state.job.is_none());
    assert!(state.local_health().is_none());
    {
        let body = state.original.as_ref().unwrap().body.lock().unwrap();
        assert!(body.as_ref().unwrap().input.is_none());
        assert!(body.as_ref().unwrap().lease.is_none());
    }
    drop(fixture.state.as_mut().unwrap().origin.take());
    drop(fixture.local.take());
    assert_eq!(
        fixture.control.snapshot().result_windows.held_positions,
        [0; 4]
    );
    fixture.close(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn held_actual_provider_cpu_keeps_the_action_and_local_until_its_footer() {
    use crate::catalog_application::statement::external_listing_tests::ListingMetadataGate;
    struct Release(Arc<ListingMetadataGate>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::CorruptData);
    let gate = Arc::new(ListingMetadataGate::default());
    provider.hold_metadata_cpu(gate.clone());
    let mut fixture = Fixture::for_input(Some(&provider), false).await;
    // Declared AFTER the CPU owner: unwinding releases the same gate before
    // Fixture/worker Drop can join it. No worker is started in construction.
    let _release_on_panic = Release(gate.clone());
    {
        let state = fixture.state.as_mut().unwrap();
        let business = with_original_driver(
            &fixture.control,
            &mut fixture.driver,
            std::future::poll_fn(|cx| {
                state.wake.0.register(cx.waker());
                state.poll_prepared(cx)
            }),
        );
        tokio::pin!(business);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut business => panic!("held real metadata worker cannot finish: {}", result.is_ok()),
                _ = gate.wait_started() => {}
            }
        }).await.unwrap();
    }
    let original = fixture
        .state
        .as_ref()
        .unwrap()
        .original
        .as_ref()
        .unwrap()
        .clone();
    {
        let state = fixture.state.as_mut().unwrap();
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(state.poll_prepared(&mut cx).is_pending());
        assert!(
            state.future.is_none(),
            "Provider child already returned its original error"
        );
        assert!(
            state.provider_cpu.is_some(),
            "actual CPU packet still belongs to the same port"
        );
        assert!(
            !state.selected,
            "an ended child cannot substitute for its CPU footer"
        );
    }
    {
        let body = original.body.lock().unwrap();
        let body = body.as_ref().expect("same child returned its View loan");
        assert_eq!(
            body.raw.as_ref().unwrap().as_ref().unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
        assert!(body.input.is_some());
        assert!(
            body.lease.is_some(),
            "exact generation retained until the actual CPU footer"
        );
    }
    fixture.root.owner.cancel(CancellationReason::ExplicitKill {
        requester_session: 29,
    });
    {
        let state = fixture.state.as_mut().unwrap();
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            state.poll_prepared(&mut cx).is_pending(),
            "late Root stop cannot manufacture CPU exit"
        );
        assert!(
            state.local_health().is_none(),
            "a held ordinary worker is not a fabricated health failure"
        );
        assert!(!state.selected);
        assert!(
            state
                .provider_cpu
                .as_ref()
                .unwrap()
                .local_failure()
                .is_none()
        );
    }
    assert_eq!(fixture.control.snapshot().root_responsibilities, 1);
    gate.release();
    let error = match fixture.business().await {
        Err(error) => error,
        Ok(_) => panic!("stopped Action cannot succeed"),
    };
    assert_eq!(
        error.failure_cause().unwrap().failure_fact(),
        ConnectorErrorKind::CorruptData.failure_fact(),
        "the error received before this stop stays canonical while CPU close continues"
    );
    let state = fixture.state.as_ref().unwrap();
    assert!(
        state.provider_cpu.is_none(),
        "same Provider packet/forwarder closed before alias retirement"
    );
    assert!(state.future.is_none());
    assert!(state.local_health().is_none());
    {
        let body = original.body.lock().unwrap();
        assert!(
            matches!(body.as_ref().unwrap().raw, Some(Err(_))),
            "original post-CPU raw outcome is still retained"
        );
        assert!(body.as_ref().unwrap().input.is_none());
        assert!(body.as_ref().unwrap().lease.is_none());
    }
    let calls = provider.calls();
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.as_str() == "metadata-start")
            .count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.as_str() == "metadata-cpu")
            .count(),
        1
    );
    drop(fixture.state.as_mut().unwrap().origin.take());
    drop(fixture.local.take());
    assert_eq!(
        fixture.control.snapshot().result_windows.held_positions,
        [0; 4]
    );
    drop(original);
    fixture.close(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn close_preserves_known_raw_health_even_when_original_child_is_pending() {
    let mut fixture = Fixture::new().await;
    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let child_polls = polls.clone();
    let cause: Arc<dyn std::error::Error + Send + Sync> = Arc::new(Unreturned);
    let state = fixture.state.as_mut().unwrap();
    state.raw_health = Some(health(cause.clone()));
    // Only this contract test's never-dispatched pending child is injected.
    // It holds no CPU packet, Provider call or loan requiring disposition.
    state.future = Some(Box::pin(std::future::poll_fn(move |_| {
        child_polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    })));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let Poll::Ready(Err(observed)) = state.poll_close(&mut cx) else {
        panic!("pending cannot hide the already-known physical health fact");
    };
    assert!(std::ptr::eq(observed.cause(), cause.as_ref()));
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "original child still receives its one progress turn"
    );
    assert!(state.future.is_some());
    assert!(
        !state.closed,
        "reported health does not fabricate actual exit"
    );
    // Restore only the two test-injected fields. No production failed job or
    // opaque owner is disposed by this fixture cleanup.
    state.raw_health = None;
    drop(state.future.take());
    fixture.close(false).await;
}
