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
use novarocks_failure_contract::SharedFailureCause;
use novarocks_query_application::system_catalog::{
    SystemCatalog, SystemCatalogInputs, SystemCatalogService,
};
use novarocks_query_application::system_catalog_rewrite::{
    SystemCatalogFacts, SystemCatalogFactsPort,
};
use novarocks_query_application::view::{QueryViewService, ViewService};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorRequestContext, ConnectorStopOwner,
};
use novarocks_sql::compiler::{CatalogRelationOutcome, SqlCapturedCatalogPreparation};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

struct CountedFacts {
    synchronous: AtomicUsize,
    asynchronous: AtomicUsize,
    result: Result<SystemCatalogFacts, SqlCompileError>,
}
impl CountedFacts {
    fn new(result: Result<SystemCatalogFacts, SqlCompileError>) -> Arc<Self> {
        Arc::new(Self {
            synchronous: AtomicUsize::new(0),
            asynchronous: AtomicUsize::new(0),
            result,
        })
    }
    fn answer(
        &self,
        catalog: &str,
        include_tables: bool,
    ) -> Result<Option<SystemCatalogFacts>, SqlCompileError> {
        assert_eq!(catalog, "default_catalog");
        assert!(!include_tables, "schemata requires namespace facts only");
        self.result.clone().map(Some)
    }
}
impl SystemCatalogFactsPort for CountedFacts {
    fn definition_system_catalog_facts(
        &self,
        _: &ConnectorRequestContext,
        catalog: &str,
        include_tables: bool,
    ) -> Result<Option<SystemCatalogFacts>, SqlCompileError> {
        self.synchronous.fetch_add(1, Ordering::SeqCst);
        self.answer(catalog, include_tables)
    }
    fn definition_system_catalog_facts_async<'a>(
        &'a self,
        _: &'a ConnectorRequestContext,
        catalog: &'a str,
        include_tables: bool,
    ) -> futures::future::BoxFuture<'a, Result<Option<SystemCatalogFacts>, SqlCompileError>> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            self.asynchronous.fetch_add(1, Ordering::SeqCst);
            self.answer(catalog, include_tables)
        })
    }
    fn local_system_catalog_facts(&self) -> Result<SystemCatalogFacts, String> {
        panic!("definition capture must use its typed facts source")
    }
    fn external_system_catalog_facts(
        &self,
        _: &ConnectorRequestContext,
        _: &str,
        _: bool,
    ) -> Result<Option<SystemCatalogFacts>, String> {
        panic!("definition capture must not fall back to String facts")
    }
}
struct NoTableLoader;
impl QueryTableBindingLoader for NoTableLoader {
    fn load_strict_base_table(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: SqlTableBindingId,
    ) -> CatalogResolutionResult<QueryTableBinding> {
        panic!("an installed definition system relation does not load a connector table")
    }
    fn load_metadata_table(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: novarocks_sql::planning::catalog::MetadataTableKind,
        _: SqlTableBindingId,
    ) -> CatalogResolutionResult<QueryTableBinding> {
        panic!("system relations are not connector metadata aliases")
    }
}
fn facts() -> SystemCatalogFacts {
    SystemCatalogFacts {
        catalog_name: "default_catalog".into(),
        schema_names: vec!["db".into(), "other_db".into()],
        table_names: vec![],
    }
}
fn query(sql: &str) -> novarocks_parser::ast::Query {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query fixture")
    };
    query.clone()
}
fn preparation(sql: &str) -> SqlCapturedCatalogPreparation {
    SqlCapturedCatalogPreparation::try_new_definition(
        query(sql),
        "default_catalog",
        "db",
        None,
        SqlCompileControl::unbounded(),
    )
    .unwrap()
}
fn materializer<'a>(
    service: &'a QueryCatalogService,
    bindings: Arc<QueryTableBindingStore>,
    source: Arc<CountedFacts>,
) -> CatalogServiceMaterializer<'a> {
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(5),
        ConnectorStopOwner::new().view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    CatalogServiceMaterializer::new(
        Some("default_catalog"),
        service,
        bindings,
        Box::new(NoTableLoader),
    )
    .with_local_views(QueryViewService::new().catalog_snapshot().unwrap())
    .with_system_relations(SystemCatalogRelations::new(
        source,
        Arc::new(SystemCatalogService::with_defaults()),
    ))
    .with_definition_inputs(
        context,
        crate::application::test_constant_policy(),
        SqlCompileControl::unbounded(),
    )
}
fn inline(fact: &CatalogRelationFact) -> &Arc<SqlInlineRelation> {
    let CatalogRelationOutcome::Inline(data) = fact.outcome() else {
        panic!("system relation must retain the original checked pools")
    };
    data
}
fn assert_declaration(preparation: SqlCapturedCatalogPreparation, fact: CatalogRelationFact) {
    let columns = preparation
        .continue_catalog(vec![fact].into_boxed_slice())
        .unwrap()
        .analyze_view_definition(
            &[],
            novarocks_sql::compiler::builtin_sql_function_catalog(),
            crate::application::test_constant_policy(),
        )
        .unwrap();
    let original = facts();
    let data = SystemCatalogService::with_defaults()
        .resolve(
            "information_schema",
            "schemata",
            &SystemCatalogInputs {
                catalog_name: &original.catalog_name,
                schema_names: &original.schema_names,
                table_names: &original.table_names,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(columns.len(), data.columns.len());
    for (actual, expected) in columns.iter().zip(&data.columns) {
        let expected = novarocks_sql::planning::catalog::SqlInlineColumn::from_catalog_column(
            expected,
            &SqlCompileControl::unbounded(),
        )
        .unwrap();
        assert_eq!(actual.name, expected.name);
        assert_eq!(actual.value_type, expected.value_type);
    }
}
const SQL: &str = "SELECT * FROM information_schema.schemata";
fn another_need() -> CatalogRelationNeed {
    let preparation =
        preparation("SELECT s.* FROM db.unused AS t CROSS JOIN information_schema.schemata AS s");
    preparation
        .needs()
        .iter()
        .find(|need| need.relation().table == "schemata")
        .unwrap()
        .clone()
}

#[test]
fn definition_system_materializes_once_and_reanswers_another_need_across_materializers() {
    let service = crate::catalog_application::query_catalog::new_query_catalog_service();
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let source = CountedFacts::new(Ok(facts()));
    let preparation = preparation(SQL);
    let [need] = preparation.needs() else {
        panic!("one need")
    };
    let first = materializer(&service, bindings.clone(), source.clone())
        .resolve_relation_fact(need)
        .unwrap();
    let next = another_need();
    assert_ne!(need.id(), next.id());
    let second = materializer(&service, bindings, source.clone())
        .resolve_relation_fact(&next)
        .unwrap();
    assert_eq!(second.id(), next.id());
    assert!(Arc::ptr_eq(inline(&first), inline(&second)));
    assert_eq!(source.synchronous.load(Ordering::SeqCst), 1);
    assert_eq!(source.asynchronous.load(Ordering::SeqCst), 0);
    assert_declaration(preparation, first);
}

#[test]
fn definition_system_source_failure_keeps_the_same_cause_across_materializers() {
    let service = crate::catalog_application::query_catalog::new_query_catalog_service();
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let original = SharedFailureCause::new(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "original definition source",
    ));
    let source = CountedFacts::new(Err(SqlCompileError::CompilationSource(original.clone())));
    let preparation = preparation(SQL);
    let [need] = preparation.needs() else {
        panic!("one need")
    };
    for need in [need.clone(), another_need()] {
        let error = materializer(&service, bindings.clone(), source.clone())
            .resolve_relation_fact(&need)
            .unwrap_err();
        let SqlCompileError::CompilationSource(cause) = error else {
            panic!("typed source retained")
        };
        assert_eq!(cause, original);
        assert!(cause.downcast_ref::<ConnectorError>().is_some());
    }
    assert_eq!(source.synchronous.load(Ordering::SeqCst), 1);
    assert_eq!(source.asynchronous.load(Ordering::SeqCst), 0);
}

#[test]
fn current_system_need_is_left_to_its_existing_owner_even_after_definition_is_memoized() {
    let service = crate::catalog_application::query_catalog::new_query_catalog_service();
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let source = CountedFacts::new(Ok(facts()));
    let definition = preparation(SQL);
    let provider = materializer(&service, bindings, source.clone());
    provider
        .resolve_relation_fact(&definition.needs()[0])
        .unwrap();
    let current = SqlCapturedCatalogPreparation::try_new(
        query(SQL),
        Some("default_catalog"),
        "db",
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    assert!(provider.system_relation_fact(&current.needs()[0]).is_none());
    assert_eq!(source.synchronous.load(Ordering::SeqCst), 1);
    assert_eq!(source.asynchronous.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn asynchronous_definition_uses_original_cpu_and_its_own_statement_memo() {
    let service = crate::catalog_application::query_catalog::new_query_catalog_service();
    let source = CountedFacts::new(Ok(facts()));
    // A preceding synchronous statement cannot supply this statement's memo.
    let sync_preparation = preparation(SQL);
    materializer(
        &service,
        Arc::new(QueryTableBindingStore::try_new().unwrap()),
        source.clone(),
    )
    .resolve_relation_fact(&sync_preparation.needs()[0])
    .unwrap();
    let (mut executors, supervisor) = crate::task_execution::blocking_io::tests::original_executors(
        tokio::runtime::Handle::current(),
    );
    let cpu = crate::query_execution::completion_facts::FrontendPlanningCpuSource::new(supervisor);
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let preparation = preparation(SQL);
    let [need] = preparation.needs() else {
        panic!("one need")
    };
    let first = tokio::time::timeout(Duration::from_secs(5), async {
        materializer(&service, bindings.clone(), source.clone())
            .system_relation_fact_on_cpu(need, &cpu)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    })
    .await
    .unwrap();
    let next = another_need();
    let second = materializer(&service, bindings, source.clone())
        .system_relation_fact_on_cpu(&next, &cpu)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let current = SqlCapturedCatalogPreparation::try_new(
        query(SQL),
        Some("default_catalog"),
        "db",
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    assert!(
        materializer(
            &service,
            Arc::new(QueryTableBindingStore::try_new().unwrap()),
            source.clone()
        )
        .system_relation_fact_on_cpu(&current.needs()[0], &cpu)
        .await
        .is_none()
    );
    let original = SharedFailureCause::new(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "original asynchronous definition source",
    ));
    let failing = CountedFacts::new(Err(SqlCompileError::CompilationSource(original.clone())));
    let failed_bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    for need in [need.clone(), next.clone()] {
        let error = materializer(&service, failed_bindings.clone(), failing.clone())
            .system_relation_fact_on_cpu(&need, &cpu)
            .await
            .unwrap()
            .unwrap_err();
        let SqlCompileError::CompilationSource(cause) = error else {
            panic!("typed asynchronous source retained")
        };
        assert_eq!(cause, original);
    }
    assert_eq!(failing.asynchronous.load(Ordering::SeqCst), 1);
    assert_eq!(failing.synchronous.load(Ordering::SeqCst), 0);
    cpu.close().await.unwrap();
    assert!(cpu.local_health_failure().is_none());
    drop(cpu);
    executors.shutdown().await;
    assert_eq!(second.id(), next.id());
    assert!(Arc::ptr_eq(inline(&first), inline(&second)));
    assert_eq!(source.synchronous.load(Ordering::SeqCst), 1);
    assert_eq!(source.asynchronous.load(Ordering::SeqCst), 1);
    assert_declaration(preparation, first);
}

struct AbsentCatalogAdmission {
    calls: AtomicUsize,
}
impl novarocks_catalog_application::CatalogApplicationPort for AbsentCatalogAdmission {
    fn create_catalog(
        &self,
        _: novarocks_catalog_application::CatalogCreateCommand,
    ) -> Result<
        novarocks_catalog_application::CatalogRuntimeObservation,
        novarocks_catalog_application::CatalogApplicationError,
    > {
        unreachable!("definition lookup must not create a catalog")
    }
    fn drop_catalog(
        &self,
        _: novarocks_catalog_application::CatalogDropCommand,
    ) -> Result<(), novarocks_catalog_application::CatalogApplicationError> {
        unreachable!("definition lookup must not drop a catalog")
    }
    fn admit_catalog(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> novarocks_catalog_application::CatalogAdmission {
        assert_eq!(
            instance_id,
            &novarocks_spi::connector::ConnectorInstanceId::parse("catalog").unwrap()
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        novarocks_catalog_application::CatalogAdmission::Absent
    }
}

#[tokio::test(flavor = "current_thread")]
async fn absent_external_definition_system_catalog_answers_the_exact_need_before_listing() {
    use crate::catalog_application::statement::external_listing_tests::{
        FailurePoint, ListingFixture,
    };
    use crate::catalog_application::system_catalog_facts::FrontendSystemCatalogFacts;

    let (mut executors, supervisor) = crate::task_execution::blocking_io::tests::original_executors(
        tokio::runtime::Handle::current(),
    );
    let cpu = crate::query_execution::completion_facts::FrontendPlanningCpuSource::new(supervisor);
    for on_cpu in [false, true] {
        let fixture =
            ListingFixture::new(FailurePoint::Namespaces, ConnectorErrorKind::Unavailable);
        let application = AbsentCatalogAdmission {
            calls: AtomicUsize::new(0),
        };
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        let control = SqlCompileControl::unbounded();
        let preparation = SqlCapturedCatalogPreparation::try_new_definition(
            query("SELECT * FROM catalog.information_schema.schemata"),
            "catalog",
            "db",
            None,
            control.clone(),
        )
        .unwrap();
        let [need] = preparation.needs() else {
            panic!("one original definition need")
        };
        let materializer = CatalogServiceMaterializer::new(
            Some("catalog"),
            fixture.catalog_service.as_ref(),
            bindings.clone(),
            Box::new(NoTableLoader),
        )
        .with_catalog_application(Some(&application))
        .with_local_views(QueryViewService::new().catalog_snapshot().unwrap())
        .with_system_relations(SystemCatalogRelations::new(
            Arc::new(FrontendSystemCatalogFacts::new(
                fixture.catalog_service.clone(),
                fixture.registry.clone(),
            )),
            Arc::new(SystemCatalogService::with_defaults()),
        ))
        .with_definition_inputs(
            crate::connector::test_request_context(),
            crate::application::test_constant_policy(),
            control,
        );
        let fact = if on_cpu {
            materializer.system_relation_fact_on_cpu(need, &cpu).await
        } else {
            materializer.system_relation_fact(need)
        }
        .expect("the installed definition system source owns the need")
        .expect("absence is an exact fact, not a compile or source failure")
        .expect("absence must not decline to another provider");
        assert_eq!(fact.id(), need.id());
        assert_eq!(fact.relation(), need.relation());
        assert_eq!(fact.target(), need.target());
        assert!(fact.is_definition_context());
        assert!(matches!(
            fact.outcome(),
            CatalogRelationOutcome::Missing { .. }
        ));
        let completed = preparation
            .continue_catalog(vec![fact].into_boxed_slice())
            .unwrap();
        assert!(completed.needs().is_empty());
        let result = completed.analyze_view_definition(
            &[],
            novarocks_sql::compiler::builtin_sql_function_catalog(),
            crate::application::test_constant_policy(),
        );
        let Err(SqlCompileError::Analyze(error)) = result else {
            panic!("the original SQL analyzer must classify the missing system relation")
        };
        assert_eq!(
            error.kind(),
            novarocks_sql::analyze_error::AnalyzeErrorKind::UnknownTable
        );
        assert_eq!(error.code().as_str(), "sql.analyze.unknown_table");
        assert!(error.span().is_some());
        assert_eq!(application.calls.load(Ordering::SeqCst), 1);
        assert!(fixture.calls().is_empty());
        assert!(bindings.captured_bindings().is_empty());
    }
    cpu.close().await.unwrap();
    assert!(cpu.local_health_failure().is_none());
    drop(cpu);
    executors.shutdown().await;
}
