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

//! Actual SQLite/ordinary-QB/closed-CPU composition of the prepared SHOW Source.
use super::*;
use crate::query_execution::internal_result_cpu::InternalResultCpuOwner;
use novarocks_query_application::{
    cancellation::QueryCancellationView,
    cpu::{QueryBlockingExecutorConfig, QueryBlockingExecutorOwner},
};
use novarocks_state_store_api::{StateStoreLimits, StateStoreProviderFactory};
use novarocks_state_store_runtime::{
    StateStoreHost, StateStoreHostInput, StateStoreProviderRegistration,
    StateStoreProviderRegistry, StateStoreRunPolicy,
};
use novarocks_state_store_sqlite::{
    SQLITE_STATE_STORE_PROVIDER_ID, SqliteStateStoreProviderFactory,
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

async fn with_original_driver<F: std::future::Future>(
    control: &WorkloadControl,
    driver: &mut JointProgressDriver,
    future: F,
) -> F::Output {
    tokio::pin!(future);
    loop {
        match driver.step() {
            JointProgress::More => tokio::task::yield_now().await,
            JointProgress::Idle(revision) => {
                tokio::select! {
                    biased;
                    result = &mut future => return result,
                    // A pending original can itself record progress. Give its
                    // forwarder and the original timeout a scheduler turn.
                    _ = control.wait_progress(revision) => tokio::task::yield_now().await,
                }
            }
        }
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    host: StateStoreHost,
    readiness_service: Option<novarocks_mv_application::readiness::MvKeyedReadinessService>,
    ordinary: QueryBlockingExecutorOwner,
    cpu: InternalResultCpuOwner,
    control: WorkloadControl,
    driver: JointProgressDriver,
    root: RootWork,
    local: Option<ResultWindowGrant>,
    state: Option<State>,
}
impl Fixture {
    async fn new() -> Self {
        Self::for_explain(None).await
    }
    async fn for_explain(level: Option<novarocks_sql::compiler::ExplainLevel>) -> Self {
        Self::with_source_explain(level, None).await
    }
    async fn with_source_explain(
        level: Option<novarocks_sql::compiler::ExplainLevel>,
        source: Option<Arc<source_projection_tests::SourceFixture>>,
    ) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("show-original.sqlite");
        let mut registry = StateStoreProviderRegistry::new();
        registry
            .register(StateStoreProviderRegistration::new(
                SqliteStateStoreProviderFactory::new(path.clone())
                    .descriptor()
                    .to_owned(),
                move |_| Ok(Box::new(SqliteStateStoreProviderFactory::new(path.clone()))),
            ))
            .unwrap();
        let ordinary = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .unwrap();
        let executor = ordinary.executor();
        let mut prepared = StateStoreHost::prepare_original_open(
            &registry,
            StateStoreHostInput {
                cluster_id: "show-original-component".into(),
                provider_id: SQLITE_STATE_STORE_PROVIDER_ID,
                limits: StateStoreLimits::default(),
                run_policy: StateStoreRunPolicy::default(),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        let mut opening = StateStoreOriginalOwner::opening(
            &executor,
            prepared.vocabulary(),
            prepared.run_policy(),
        )
        .unwrap();
        let opening_context = opening.context(prepared.deadline());
        prepared.start_in(&opening_context).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            std::future::poll_fn(|cx| {
                opening.poll_progress(cx).unwrap();
                prepared.poll_ready(cx)
            }),
        )
        .await
        .unwrap()
        .unwrap();
        let mut ready = None;
        prepared.install_ready_in(&mut ready).unwrap();
        std::future::poll_fn(|cx| opening.poll_closed(cx))
            .await
            .unwrap();
        drop(opening);
        drop(prepared);
        let host = ready.unwrap();
        let (vocabulary, instance) = host.original_blocking_sources().unwrap();
        let binding = StateStoreOriginalBinding {
            transaction_deadline: host.state_store().unwrap().limits().transaction_deadline,
            dependencies: host.original_blocking_dependency_facts().unwrap().to_vec(),
            vocabulary,
            instance,
            executor,
            policy: host.run_policy(),
        };
        let repository =
            novarocks_mv_application::state_store_repository::StateStoreMvRepository::open(
                host.state_store().unwrap(),
                host.run_policy(),
            )
            .await
            .unwrap(); // This constructor stores its two inputs and performs no I/O.
        let entrance = crate::mv::domain::test_admission::management_entrance();
        let readiness_service = novarocks_mv_application::readiness::MvKeyedReadinessService::new(
            repository,
            Arc::default(),
        );
        let readiness = Arc::new(MvReadinessPort::from_product(
            readiness_service.clone(),
            tokio::runtime::Handle::current(),
            entrance.clone(),
        ));
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
        let (root, local) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let scope = root.owner.scope();
        let origin = local.retain_alias();
        let producer = LocalResultProducer::try_new(
            &scope,
            origin.clone(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        )
        .unwrap();
        let io = MetadataIoAdmission::for_original_source(
            &root.owner,
            binding.blocking_dependency_facts().to_vec(),
        )
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            with_original_driver(
                &control,
                &mut driver,
                std::future::poll_fn(|cx| {
                    let ready = io.poll_progress(&root.owner, cx).unwrap();
                    if ready {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }),
            ),
        )
        .await
        .unwrap();
        let input =
            if let Some(level) = level {
                let ports =
                    if let Some(source) = &source {
                        source.ports(readiness.clone(), entrance.clone())
                    } else {
                        let host =
                            Arc::new(novarocks_catalog_application::ConnectorControlHost::new());
                        IcebergMvCorePorts::new_with_management_entrance(
                Arc::new(novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap()),
                Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
                None,
                host.clone(),
                host,
                readiness.clone(),
                Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
                entrance.clone(),
                crate::application::test_constant_policy(),
            )
                    };
                ReadRequest::Explain(Box::new(MvExplainPreparationInput {
                    ports,
                    current_catalog: Some("ice".into()),
                    current_database: if source.is_some() { "sales" } else { "db" }.into(),
                    statement: crate::mv::domain::application::MvRefreshRequest {
                        name_parts: vec![if source.is_some() { "mv" } else { "missing_mv" }.into()],
                        full: false,
                    },
                    level,
                    context: novarocks_spi::connector::ConnectorRequestContext::try_new(
                        Instant::now() + Duration::from_secs(5),
                        novarocks_spi::connector::ConnectorStopOwner::new().view(),
                        4096,
                        64 * 1024,
                    )
                    .unwrap(),
                }))
            } else {
                ReadRequest::Show {
                    catalog: Some("ice".into()),
                    statement: MvShowStatement { database: None },
                }
            };
        let mut state = State {
            family: novarocks_result_contract::ClientPresentationFamily::MysqlText,
            readiness,
            entrance,
            binding,
            runtime: tokio::runtime::Handle::current(),
            cancellation: QueryCancellationView::governed(scope.cancellation().unwrap(), None),
            deadline: None,
            scope,
            origin: Some(origin),
            producer: Some(producer),
            input: Some(input),
            explain: None,
            io: Some(io),
            store: None,
            original: None,
            future: None,
            job: None,
            marker: None,
            cpu: cpu.runtime(),
            refusal: None,
            raw_health: None,
            ordinary_io: None,
            wake: Arc::new(Wake::default()),
            in_foreign: false,
            selected: false,
            closed: false,
        };
        // The same production constructor requires MIO positive Root data and
        // installs the strong SS owner before the first weak request or F.
        state.install_original().unwrap();
        Self {
            _directory: directory,
            host,
            readiness_service: Some(readiness_service),
            ordinary,
            cpu,
            control,
            driver,
            root,
            local: Some(local),
            state: Some(state),
        }
    }
    async fn business(&mut self) -> Result<StatementActionValue, QueryServiceError> {
        let state = self.state.as_mut().unwrap();
        let driver = &mut self.driver;
        tokio::time::timeout(
            Duration::from_secs(5),
            with_original_driver(
                &self.control,
                driver,
                std::future::poll_fn(|cx| {
                    state.wake.0.register(cx.waker());
                    state.poll_prepared(cx)
                }),
            ),
        )
        .await
        .expect("actual QB1 SS and closed CPU authors must make progress")
    }
    async fn close(mut self, cancelled: bool) {
        let state = self.state.as_mut().unwrap();
        let driver = &mut self.driver;
        tokio::time::timeout(
            Duration::from_secs(5),
            with_original_driver(
                &self.control,
                driver,
                std::future::poll_fn(|cx| state.poll_close(cx)),
            ),
        )
        .await
        .expect("same original SS/CPU/MIO close")
        .unwrap();
        drop(self.state.take());
        // The fixture's seed-only alias is not a live SQLite consumer after
        // the original State has closed. Release it before the host's exact
        // single-owner close checks its repository/store strong references.
        drop(self.readiness_service.take());
        drop(self.local.take());
        if cancelled {
            self.root.owner.complete_after_terminal_cancel_settled();
        } else {
            self.root.owner.complete();
        }
        self.root.business.release();
        let (vocabulary, instance) = self.host.original_blocking_sources().unwrap();
        let mut closing = StateStoreOriginalOwner::closing_instance(
            &self.ordinary.executor(),
            vocabulary,
            instance,
            self.host.run_policy(),
        )
        .unwrap();
        let mut original = None;
        self.host
            .prepare_original_close_in(
                &closing.context(Instant::now() + Duration::from_secs(5)),
                &mut original,
            )
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            std::future::poll_fn(|cx| {
                closing.poll_progress(cx).unwrap();
                self.host
                    .poll_original_closed(original.as_deref_mut().unwrap(), cx)
            }),
        )
        .await
        .expect("exact SQLite instance close uses ordinary QB")
        .unwrap();
        std::future::poll_fn(|cx| closing.poll_closed(cx))
            .await
            .unwrap();
        drop(original);
        drop(closing);
        drop(self.host);
        drop(self.driver);
        drop(self.control);
        self.cpu
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        self.ordinary
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn show_original_sqlite_qb1_returns_local_rows_without_holding_an_internal_position() {
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
        panic!("SHOW must return the original Local result")
    };
    assert_eq!(result.row_count(), 0);
    assert_eq!(
        result
            .columns()
            .map(|column| column.name.to_owned())
            .collect::<Vec<_>>(),
        [
            "Name",
            "Database",
            "StorageEngine",
            "RefreshMode",
            "LastRefreshTime",
            "LastRefreshRows",
            "BaseTables",
            "SelectText",
            "Dependencies",
            "RefreshPaused",
            "NextRefreshTime",
            "LastSchedulerError",
            "MaxStalenessMs",
            "RefreshState",
            "RetryAfterTime",
            "Manageability",
        ]
    );
    assert!(fixture.state.as_ref().unwrap().local_health().is_none());
    assert!(!original.cpu_unreturned.load(Ordering::Acquire));
    assert!(
        fixture.state.as_ref().unwrap().job.is_none(),
        "actual worker/forwarder footer preceded result transfer"
    );
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
        1,
        "only the actual returned Local graph owns its delivery position"
    );
    drop(result);
    assert_eq!(
        fixture.control.snapshot().result_windows.held_positions,
        [0; 4],
        "the same SS/MIO residual cannot pin delivery or an Internal computation position"
    );
    assert_eq!(fixture.control.snapshot().root_responsibilities, 1);
    drop(original);
    fixture.close(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn show_original_cancelled_borrow_keeps_the_same_pending_sqlite_input_and_closes_it() {
    let mut fixture = Fixture::new().await;
    let reservation = fixture.ordinary.executor().try_reserve().unwrap();
    let state = fixture.state.as_mut().unwrap();
    let original = state.original.as_ref().unwrap().clone();
    let weak = Arc::downgrade(&original);
    state.future = Some(Box::pin(run_io(original.clone())));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    {
        let mut borrower = Box::pin(std::future::poll_fn(|cx| {
            state.progress(cx);
            let result = state.poll_io(cx);
            state.progress(cx);
            result
        }));
        assert!(borrower.as_mut().poll(&mut cx).is_pending());
    }
    assert!(state.future.is_some());
    assert!(Arc::ptr_eq(state.original.as_ref().unwrap(), &original));
    assert!(weak.upgrade().is_some());
    fixture.root.owner.cancel(CancellationReason::Requested);
    drop(reservation);
    {
        let state = fixture.state.as_mut().unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            with_original_driver(
                &fixture.control,
                &mut fixture.driver,
                std::future::poll_fn(|cx| {
                    state.progress(cx);
                    let result = state.poll_io(cx);
                    state.progress(cx);
                    result
                }),
            ),
        )
        .await
        .expect("same cancelled read receives its original output and child exit");
    }
    let original_cause = {
        let body = original.body.lock().unwrap();
        let error = body
            .as_ref()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .as_ref()
            .err()
            .expect("actual repository cancellation is stable before public selection");
        error
            .failure_cause()
            .expect("actual repository source retained")
            .clone()
    };
    let error = match fixture.business().await {
        Err(error) => error,
        Ok(_) => panic!("the original cancelled read cannot become an empty legacy inventory"),
    };
    assert_eq!(
        error.failure_cause(),
        Some(&original_cause),
        "late MIO/root stop cannot replace the original repository cause"
    );
    use novarocks_failure_contract::FailureProjection;
    assert_eq!(
        error.execution_failure().unwrap().fact(),
        original_cause.failure_fact()
    );
    assert!(
        fixture.state.as_ref().unwrap().local_health().is_none(),
        "ordinary cancellation is not physical health failure"
    );
    assert!(fixture.state.as_ref().unwrap().future.is_none());
    assert!(
        weak.upgrade().is_some(),
        "same original remains for residual close"
    );
    drop(original);
    fixture.close(true).await;
    assert!(weak.upgrade().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn explain_original_uses_the_same_sqlite_qb1_reader_and_keeps_its_typed_failure() {
    let mut fixture =
        Fixture::for_explain(Some(novarocks_sql::compiler::ExplainLevel::Costs)).await;
    let original = fixture
        .state
        .as_ref()
        .unwrap()
        .explain
        .as_ref()
        .unwrap()
        .reader
        .clone();
    let error = match fixture.business().await {
        Err(error) => error,
        Ok(_) => panic!("the real empty repository has no target definition to explain"),
    };
    assert!(
        error
            .to_string()
            .contains("ice.db.missing_mv has no MV definition")
    );
    let state = fixture.state.as_ref().unwrap();
    let explain = state.explain.as_ref().unwrap();
    assert!(Arc::ptr_eq(&original, &explain.reader));
    let Err(domain) = explain.result.as_ref().unwrap() else {
        panic!("the original reader failure remains in its receiving slot");
    };
    assert_eq!(
        domain.kind(),
        crate::mv::domain::application::MvApplicationErrorKind::InvalidRequest
    );
    assert_eq!(error, domain_error(domain.clone()));
    assert!(state.local_health().is_none());
    assert!(state.job.is_none());
    drop(fixture.state.as_mut().unwrap().origin.take());
    drop(fixture.local.take());
    assert_eq!(
        fixture.control.snapshot().result_windows.held_positions,
        [0; 4]
    );
    assert_eq!(fixture.control.snapshot().root_responsibilities, 1);
    let weak = Arc::downgrade(&original);
    drop(original);
    fixture.close(false).await;
    assert!(
        weak.upgrade().is_none(),
        "the same reader/gate closes with its original owner"
    );
}

#[test]
fn explain_domain_projection_preserves_actual_connector_cause_and_missing_pure_identity() {
    use crate::mv::domain::application::MvApplicationErrorKind;
    use novarocks_failure_contract::FailureProjection;
    let domain = MvApplicationError::from_connector(
        MvApplicationErrorKind::Engine,
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
            "actual metadata admission refusal",
        ),
    );
    let cause = domain.original_failure().unwrap().clone();
    let selected = domain_error(domain);
    assert_eq!(selected.failure_cause(), Some(&cause));
    assert_eq!(
        selected.execution_failure().unwrap().fact(),
        cause.failure_fact()
    );
    assert!(
        selected.compile_control_error().is_none(),
        "a Connector refusal is not a compiler source"
    );

    let compile = novarocks_sql::compiler::SqlCompileError::from(
        novarocks_functions::FunctionBindingError::UnavailableImplementation(
            novarocks_functions::FunctionOverloadId::try_new("fixture/mv-explain/missing-pure-v1")
                .unwrap(),
        ),
    );
    let novarocks_sql::compiler::SqlCompileError::Analyze(ref analyze) = compile else {
        panic!("the actual SQL author provides the missing-pure identity");
    };
    let expected = analyze.code();
    let selected = domain_error(MvApplicationError::from_compile(compile));
    assert_eq!(
        selected.user_error().unwrap().code().as_str(),
        expected.as_str()
    );
    assert!(selected.failure_cause().is_none());
    assert!(selected.compile_control_error().is_none());

    let selected = domain_error(MvApplicationError::from_compile(
        novarocks_sql::compiler::SqlCompileError::ResourceExhausted,
    ));
    assert_eq!(
        selected.compile_control_error(),
        Some(novarocks_type_contract::CompileControlError::ResourceExhausted)
    );
}

#[path = "tests/source_projection_tests.rs"]
mod source_projection_tests;
