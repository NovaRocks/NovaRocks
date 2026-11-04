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
use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::catalog_application::query_materializer::{
    CatalogServiceMaterializer, iceberg_table_binding_loader,
};
use crate::query_execution::mv_assembly::refresh_preparation::io_gate::{
    MvRefreshPreparationIo, MvRefreshPreparationServices,
};
use crate::query_execution::{
    internal_result_cpu::InternalResultCpuOwner, maintenance::io_admission::MetadataIoAdmission,
};
use crate::task_execution::status_intake::StatusIntakeWake;
use novarocks_query_application::{
    cancellation::QueryCancellationView,
    cpu::QueryBlockingExecutorConfig,
    view::{QueryViewService, ViewService},
};
use novarocks_state_store_api::{
    StateStoreExecutionContext, StateStoreExecutionFailure, StateStoreExecutionRequests,
    StateStoreReadExecution,
};
use novarocks_workload_control::{
    ControlStorageConfig, ResultCapacityConfig, WorkClass, WorkRequest, WorkloadConfig,
    WorkloadControl,
    joint_admission::{JointProgress, ResponsibilityConfig},
};
use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::Poll,
    time::{Duration, Instant},
};

struct NoRepositoryIo;
impl StateStoreReadExecution for NoRepositoryIo {
    fn read_transaction_context(
        &self,
    ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
        panic!("catalog gate must not enter repository I/O")
    }
}
impl StateStoreExecutionRequests for NoRepositoryIo {
    fn write_operation_context(
        &self,
    ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
        panic!("catalog gate must not publish repository effects")
    }
}
#[derive(Debug)]
struct Wake;
impl StatusIntakeWake for Wake {
    fn wake(&self) {}
}

async fn with_driver<F: Future>(
    control: &WorkloadControl,
    driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    future: F,
) -> F::Output {
    tokio::pin!(future);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match driver.step() {
                JointProgress::More => tokio::task::yield_now().await,
                JointProgress::Idle(revision) => tokio::select! {
                    biased;
                    result = &mut future => return result,
                    _ = control.wait_progress(revision) => {},
                },
            }
        }
    })
    .await
    .expect("catalog gate fixture watchdog")
}

// This exercises the real named gate and its exact Provider preparation. It
// is a gate-boundary fixture; original physical CPU custody is independently
// covered by cancelled_create_keeps_the_original_typed_result_until_the_real_cpu_footer.
async fn with_gate(
    run: impl for<'a> FnOnce(
        &'a Arc<MvRefreshPreparationIo>,
        &'a ConnectorRequestContext,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
) {
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
    let mut cpu = InternalResultCpuOwner::try_new(
        QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ),
        &mut control,
    )
    .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .root_admission()
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let admission = MetadataIoAdmission::for_original_source(&root.owner, Vec::new()).unwrap();
    with_driver(
        &control,
        &mut driver,
        std::future::poll_fn(|cx| {
            if admission.poll_progress(&root.owner, cx).unwrap() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await;
    let capacity = admission.root_capacity().unwrap();
    let window = capacity.try_window_alias().unwrap();
    let stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(5),
        stop.view(),
        4096,
        64 * 1024,
    )
    .unwrap()
    .with_io_admission(admission.clone());
    let gate = Arc::new(MvRefreshPreparationIo::new(
        MvRefreshPreparationServices {
            cpu: cpu.runtime(),
            scope: scope.clone(),
            window: window.clone(),
            cancellation: QueryCancellationView::governed(scope.cancellation().unwrap(), None),
            runtime: tokio::runtime::Handle::current(),
            wake: Arc::new(Wake),
            requests: Arc::new(NoRepositoryIo),
        },
        admission.original_result_scope(&context).unwrap(),
    ));
    run(&gate, &context).await;
    std::future::poll_fn(|cx| gate.poll_closed(cx))
        .await
        .unwrap();
    drop(gate);
    drop(context);
    with_driver(
        &control,
        &mut driver,
        std::future::poll_fn(|cx| admission.poll_actual_quiescent_continuing(cx)),
    )
    .await
    .unwrap();
    drop(window);
    drop(capacity);
    drop(admission);
    drop(scope);
    root.owner.complete();
    root.business.release();
    drop(driver);
    drop(control);
    cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}
async fn drive<F: Future>(gate: &Arc<MvRefreshPreparationIo>, future: F) -> F::Output {
    tokio::pin!(future);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            loop {
                if let Poll::Ready(value) = future.as_mut().poll(cx) {
                    return Poll::Ready(value);
                }
                assert!(
                    gate.has_request(),
                    "reader must suspend only at its named gate"
                );
                if gate.poll_io(cx).is_pending() {
                    return Poll::Pending;
                }
            }
        }),
    )
    .await
    .expect("named gate/reader watchdog")
}
fn preparation(sql: &str) -> novarocks_sql::compiler::SqlCapturedCatalogPreparation {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query fixture")
    };
    novarocks_sql::compiler::SqlCapturedCatalogPreparation::try_new(
        query.clone(),
        Some("ice"),
        "db",
        novarocks_sql::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn original_gate_keeps_one_table_to_view_lease_and_statement_memo() {
    with_gate(|gate, context| {
        Box::pin(async move {
            let resolver = fixture(Some(absent()), Some(None));
            let service = crate::catalog_application::query_catalog::new_query_catalog_service();
            let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
            let materializer = CatalogServiceMaterializer::new(
                Some("ice"),
                &service,
                bindings.clone(),
                iceberg_table_binding_loader(&resolver, context.clone()),
            )
            .with_local_views(QueryViewService::new().catalog_snapshot().unwrap());
            let sql = "SELECT x FROM ice.db.orders";
            for _ in 0..2 {
                let source = preparation(sql);
                let [request] = source.needs() else {
                    panic!("one need")
                };
                let fact = drive(
                    gate,
                    materializer.resolve_original_relation_fact(
                        request,
                        &resolver,
                        context.clone(),
                        gate,
                    ),
                )
                .await
                .unwrap();
                let source = source
                    .continue_catalog(vec![fact].into_boxed_slice())
                    .unwrap();
                assert!(source.needs().is_empty());
                let captured = source.capture(&[]).unwrap();
                let view = captured
                    .provider()
                    .resolve_view_for_analysis(Some("ice"), "db", "orders")
                    .unwrap()
                    .unwrap();
                assert_eq!(view.version.as_ref(), "observed-v1");
                assert_eq!(view.columns[0].name.as_ref(), "x");
            }
            assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
            assert_eq!(resolver.counts.table_prepare.load(Ordering::Relaxed), 1);
            assert_eq!(resolver.counts.view_prepare.load(Ordering::Relaxed), 1);
            assert_eq!(resolver.counts.view_create.load(Ordering::Relaxed), 1);
            assert_eq!(resolver.counts.table_load.load(Ordering::Relaxed), 0);
            assert_eq!(resolver.counts.view_load.load(Ordering::Relaxed), 0);
            assert!(bindings.captured_bindings().is_empty());
        })
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_gate_resolves_reference_before_historical_schema_and_keeps_its_input() {
    with_gate(|gate, context| {
        Box::pin(async move {
            let resolver = fixture(None, Some(None));
            let service = crate::catalog_application::query_catalog::new_query_catalog_service();
            let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
            let materializer = CatalogServiceMaterializer::new(
                Some("ice"),
                &service,
                bindings.clone(),
                iceberg_table_binding_loader(&resolver, context.clone()),
            );
            let source = preparation("SELECT x FROM ice.db.orders FOR VERSION AS OF 'backup'");
            let [request] = source.needs() else {
                panic!("one need")
            };
            let fact = drive(
                gate,
                materializer.resolve_original_relation_fact(
                    request,
                    &resolver,
                    context.clone(),
                    gate,
                ),
            )
            .await
            .unwrap();
            source.capture(&[fact]).unwrap();
            let values = bindings.captured_bindings();
            let [(binding, physical)] = values.as_slice() else {
                panic!("one admitted table")
            };
            assert_eq!(
                novarocks_sql::planning::catalog::frozen_input_snapshot_id(&physical.resolved),
                Some(10)
            );
            assert_eq!(
                novarocks_sql::planning::catalog::catalog_table(&physical.resolved).columns[0]
                    .data_type,
                arrow::datatypes::DataType::Int32
            );
            let id = novarocks_sql::planning::catalog::table_binding_id(&physical.resolved);
            assert_eq!(id, *binding);
            assert_eq!(
                *resolver.counts.resolutions.lock().unwrap(),
                vec![ConnectorTableResolution::HistoricalSnapshot(10)]
            );
            assert_eq!(
                *resolver.counts.events.lock().unwrap(),
                vec!["reference", "table-prepare"]
            );
            assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
            assert_eq!(resolver.counts.view_prepare.load(Ordering::Relaxed), 0);
        })
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_gate_keeps_reference_refusals_as_facts_without_loading_a_table() {
    with_gate(|gate, context| Box::pin(async move {
        let resolver = fixture(None, Some(None));
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        let materializer = CatalogServiceMaterializer::new(Some("ice"), &service, bindings.clone(), iceberg_table_binding_loader(&resolver, context.clone()));
        for sql in [
            "SELECT x FROM ice.db.orders FOR VERSION AS OF 'missing'",
            "SELECT b.x FROM ice.db.other a JOIN ice.db.orders FOR VERSION AS OF 'missing' b ON 1=1",
        ] {
            let request = need(sql);
            let fact = drive(gate, materializer.resolve_original_relation_fact(&request, &resolver, context.clone(), gate)).await.unwrap();
            assert_eq!(fact.id(), request.id());
            assert!(matches!(fact.outcome(), novarocks_sql::compiler::CatalogRelationOutcome::SelectionRefused(_)));
        }
        assert_eq!(resolver.counts.reference.load(Ordering::Relaxed), 1);
        assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
        assert_eq!(resolver.counts.table_prepare.load(Ordering::Relaxed), 0);
        assert_eq!(resolver.counts.view_prepare.load(Ordering::Relaxed), 0);
        assert!(bindings.captured_bindings().is_empty());
    })).await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_gate_preserves_provider_failures_without_entering_a_view_or_reloading() {
    with_gate(|gate, context| {
        Box::pin(async move {
            for kind in [
                ConnectorErrorKind::Unavailable,
                ConnectorErrorKind::Unsupported,
                ConnectorErrorKind::CorruptData,
            ] {
                let original = ConnectorError::new(kind, "original provider refusal");
                let resolver = fixture(Some(original.clone()), Some(None));
                let service =
                    crate::catalog_application::query_catalog::new_query_catalog_service();
                let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
                let materializer = CatalogServiceMaterializer::new(
                    Some("ice"),
                    &service,
                    bindings.clone(),
                    iceberg_table_binding_loader(&resolver, context.clone()),
                );
                let mut original_cause = None;
                for _ in 0..2 {
                    let request = need("SELECT x FROM ice.db.orders");
                    let error = drive(
                        gate,
                        materializer.resolve_original_relation_fact(
                            &request,
                            &resolver,
                            context.clone(),
                            gate,
                        ),
                    )
                    .await
                    .unwrap_err();
                    if let Some(original) = &original_cause {
                        assert_eq!(error.original_failure(), Some(original));
                    } else {
                        original_cause = error.original_failure().cloned();
                    }
                    assert_eq!(
                        error.kind(),
                        if kind == ConnectorErrorKind::Unavailable {
                            crate::mv::domain::application::MvApplicationErrorKind::Unavailable
                        } else {
                            crate::mv::domain::application::MvApplicationErrorKind::Engine
                        }
                    );
                    assert_eq!(
                        error
                            .original_failure()
                            .unwrap()
                            .downcast_ref::<ConnectorError>()
                            .unwrap()
                            .kind(),
                        kind
                    );
                }
                assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 1);
                assert_eq!(resolver.counts.table_prepare.load(Ordering::Relaxed), 1);
                assert_eq!(resolver.counts.view_prepare.load(Ordering::Relaxed), 0);
                assert!(bindings.captured_bindings().is_empty());
            }
        })
    })
    .await;
}

struct DefinitionSystemFacts {
    calls: AtomicUsize,
    deadline: Instant,
    result: Result<
        novarocks_query_application::system_catalog_rewrite::SystemCatalogFacts,
        novarocks_sql::compiler::SqlCompileError,
    >,
}
impl novarocks_query_application::system_catalog_rewrite::SystemCatalogFactsPort
    for DefinitionSystemFacts
{
    fn definition_system_catalog_facts_async<'a>(
        &'a self,
        request: &'a ConnectorRequestContext,
        catalog: &'a str,
        include_tables: bool,
    ) -> futures::future::BoxFuture<
        'a,
        Result<
            Option<novarocks_query_application::system_catalog_rewrite::SystemCatalogFacts>,
            novarocks_sql::compiler::SqlCompileError,
        >,
    > {
        Box::pin(async move {
            assert_eq!(catalog, "default_catalog");
            assert!(!include_tables, "schemata needs namespace facts only");
            assert_eq!(request.deadline(), self.deadline);
            assert!(
                request.io_admission().is_some(),
                "the original admission must reach the typed source"
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.result.clone().map(Some)
        })
    }
    fn definition_system_catalog_facts(
        &self,
        _: &ConnectorRequestContext,
        _: &str,
        _: bool,
    ) -> Result<
        Option<novarocks_query_application::system_catalog_rewrite::SystemCatalogFacts>,
        novarocks_sql::compiler::SqlCompileError,
    > {
        panic!("the named MV system gate cannot use a synchronous fallback")
    }
    fn local_system_catalog_facts(
        &self,
    ) -> Result<novarocks_query_application::system_catalog_rewrite::SystemCatalogFacts, String>
    {
        panic!("the named MV system gate cannot use legacy String facts")
    }
    fn external_system_catalog_facts(
        &self,
        _: &ConnectorRequestContext,
        _: &str,
        _: bool,
    ) -> Result<
        Option<novarocks_query_application::system_catalog_rewrite::SystemCatalogFacts>,
        String,
    > {
        panic!("the named MV system gate cannot use legacy String facts")
    }
}
fn system_definition(sql: &str) -> novarocks_sql::compiler::SqlCapturedCatalogPreparation {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("definition query fixture")
    };
    novarocks_sql::compiler::SqlCapturedCatalogPreparation::try_new_definition(
        query.clone(),
        "default_catalog",
        "db",
        None,
        novarocks_sql::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap()
}
fn another_system_need() -> CatalogRelationNeed {
    system_definition("SELECT s.* FROM db.unused t CROSS JOIN information_schema.schemata s")
        .needs()
        .iter()
        .find(|need| need.relation().table == "schemata")
        .unwrap()
        .clone()
}
fn assert_no_system_table_probe(resolver: &CountedResolver) {
    assert_eq!(resolver.counts.acquired.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.table_prepare.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.table_create.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.table_load.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.view_prepare.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.view_create.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.view_load.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.reference.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn original_system_gate_keeps_one_typed_observation_and_the_same_inline_memo() {
    with_gate(|gate, context| Box::pin(async move {
        let resolver = fixture(Some(absent()), Some(None));
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        let typed = Arc::new(DefinitionSystemFacts {
            calls: AtomicUsize::new(0),
            deadline: context.deadline(),
            result: Ok(novarocks_query_application::system_catalog_rewrite::SystemCatalogFacts {
                catalog_name: "default_catalog".into(),
                schema_names: vec!["db".into(), "other_db".into()],
                table_names: Vec::new(),
            }),
        });
        let materializer = CatalogServiceMaterializer::new(
            Some("default_catalog"), &service, bindings.clone(),
            iceberg_table_binding_loader(&resolver, context.clone()),
        ).with_local_views(QueryViewService::new().catalog_snapshot().unwrap())
            .with_system_relations(novarocks_query_application::system_catalog::SystemCatalogRelations::new(
                typed.clone(),
                Arc::new(novarocks_query_application::system_catalog::SystemCatalogService::with_defaults()),
            ))
            .with_definition_inputs(context.clone(), crate::application::test_constant_policy(), novarocks_sql::compiler::SqlCompileControl::unbounded());
        let preparation = system_definition("SELECT * FROM information_schema.schemata");
        let [need] = preparation.needs() else { panic!("one definition system need") };
        assert!(need.is_definition_context());
        let first = drive(gate, materializer.resolve_original_relation_fact(need, &resolver, context.clone(), gate)).await.unwrap();
        let second_need = another_system_need();
        assert_ne!(need.id(), second_need.id());
        let second = drive(gate, materializer.resolve_original_relation_fact(&second_need, &resolver, context.clone(), gate)).await.unwrap();
        assert_eq!(second.id(), second_need.id());
        let (novarocks_sql::compiler::CatalogRelationOutcome::Inline(first_data), novarocks_sql::compiler::CatalogRelationOutcome::Inline(second_data)) = (first.outcome(), second.outcome()) else { panic!("actual system registry must return checked Inline facts") };
        assert!(Arc::ptr_eq(first_data, second_data));
        assert_eq!(typed.calls.load(Ordering::SeqCst), 1);
        let columns = preparation.continue_catalog(vec![first].into_boxed_slice()).unwrap()
            .analyze_view_definition(&[], novarocks_sql::compiler::builtin_sql_function_catalog(), crate::application::test_constant_policy()).unwrap();
        assert_eq!(columns.len(), 5);
        assert_eq!(columns[0].name.as_ref(), "catalog_name");
        assert_eq!(columns[4].name.as_ref(), "sql_path");
        assert_eq!(columns[4].value_type.data_type(), &arrow::datatypes::DataType::Utf8);
        assert!(columns[4].value_type.nullable());
        assert!(!columns[0].value_type.nullable());
        assert_no_system_table_probe(&resolver);
        assert!(bindings.captured_bindings().is_empty());
    })).await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_system_gate_memo_keeps_the_same_typed_source_without_table_or_string_fallback() {
    with_gate(|gate, context| Box::pin(async move {
        let resolver = fixture(Some(absent()), Some(None));
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        let original = novarocks_failure_contract::SharedFailureCause::new(ConnectorError::new(
            ConnectorErrorKind::Unavailable, "original typed system namespace source",
        ));
        let typed = Arc::new(DefinitionSystemFacts {
            calls: AtomicUsize::new(0),
            deadline: context.deadline(),
            result: Err(novarocks_sql::compiler::SqlCompileError::CompilationSource(original.clone())),
        });
        let materializer = CatalogServiceMaterializer::new(
            Some("default_catalog"), &service, bindings.clone(),
            iceberg_table_binding_loader(&resolver, context.clone()),
        ).with_local_views(QueryViewService::new().catalog_snapshot().unwrap())
            .with_system_relations(novarocks_query_application::system_catalog::SystemCatalogRelations::new(
                typed.clone(),
                Arc::new(novarocks_query_application::system_catalog::SystemCatalogService::with_defaults()),
            ))
            .with_definition_inputs(context.clone(), crate::application::test_constant_policy(), novarocks_sql::compiler::SqlCompileControl::unbounded());
        let preparation = system_definition("SELECT * FROM information_schema.schemata");
        let [need] = preparation.needs() else { panic!("one definition system need") };
        for request in [need.clone(), another_system_need()] {
            let error = drive(gate, materializer.resolve_original_relation_fact(&request, &resolver, context.clone(), gate)).await.unwrap_err();
            // This typed SQL boundary uses the existing from_compile policy.
            // Its source is not reclassified through the table Provider adapter.
            assert_eq!(error.original_failure(), Some(&original));
            assert_eq!(error.original_failure().unwrap().downcast_ref::<ConnectorError>().unwrap().kind(), ConnectorErrorKind::Unavailable);
            assert!(matches!(error.compile_control_error(), Some(novarocks_type_contract::CompileControlError::Source(cause)) if cause == original));
        }
        assert_eq!(typed.calls.load(Ordering::SeqCst), 1);
        assert_no_system_table_probe(&resolver);
        assert!(bindings.captured_bindings().is_empty());
    })).await;
}
