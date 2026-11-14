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

//! Full FE fact-round integration with the original planning CPU and snapshot
//! Source. Native execution owns result semantics; these tests prove the
//! compiler handoff, projection and explain observation boundary.
use super::*;
use novarocks_query_application::preparation::{
    FinalPlanCompletionDriver, ProviderReadFactPort, ReadAccessSink, SystemTableSnapshotFactPort,
};
use novarocks_sql::compiler::*;
use std::sync::atomic::AtomicUsize;
pub(super) struct NoExternalScans;
#[async_trait]
impl ProviderReadFactPort for NoExternalScans {
    type Access = ();
    async fn resolve_provider_reads(
        &self,
        _: &[ProviderReadNeed],
        _: &ReadAccessSink<()>,
    ) -> Result<Vec<ProviderReadFact>, String> {
        panic!("a system relation cannot ask for an external scan")
    }
}
struct CountSnapshots {
    source: Arc<FrontendSystemTableFacts>,
    calls: AtomicUsize,
}
#[async_trait]
impl SystemTableSnapshotFactPort for CountSnapshots {
    async fn resolve_system_tables(
        &self,
        needs: &[SystemTableSnapshotNeed],
    ) -> Result<Vec<SystemTableSnapshotFact>, SqlCompileError> {
        self.calls.fetch_add(needs.len(), Ordering::Relaxed);
        self.source.resolve_system_tables(needs).await
    }
}
pub(super) fn request(
    sql: &str,
    intent: SqlCompileIntent,
    control: SqlCompileControl,
) -> SqlFinalPlanCompileRequest {
    let [novarocks_parser::ast::Statement::Query(query)]: [_; 1] =
        novarocks_parser::parse(sql).unwrap().try_into().unwrap()
    else {
        panic!("query")
    };
    SqlFinalPlanCompileRequest::new(
        novarocks_physical_plan::PlanVersionId::try_new([53; 16]).unwrap(),
        SqlStatementInput::parsed_query(Box::new(query)),
        intent,
        SqlSessionContext {
            current_catalog: Some("system".into()),
            current_database: "metadata".into(),
            sql_semantics: Default::default(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                ..Default::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        crate::query_execution::constant_eval::constant_evaluator(),
        crate::application::test_constant_policy(),
        control,
        novarocks_physical_plan::PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        novarocks_physical_plan::ScanReadBudget {
            max_batch_rows: novarocks_physical_plan::MAX_SCAN_BATCH_ROWS,
            max_batch_bytes: novarocks_physical_plan::MAX_SCAN_BATCH_BYTES,
        },
        DEFAULT_COMPLETION_LIMITS,
    )
}

#[tokio::test]
async fn original_fe_completion_materializes_reads_joins_aggregates_and_analyze_but_not_plain_explain()
 {
    let plain = SqlCompileIntent::Explain {
        level: ExplainLevel::Normal,
        analyze: false,
    };
    let analyze = SqlCompileIntent::Explain {
        level: ExplainLevel::Normal,
        analyze: true,
    };
    for (sql, intent, expected_snapshots) in [
        (
            "SELECT table_name FROM system.information_schema.tables",
            SqlCompileIntent::Query,
            1,
        ),
        (
            "SELECT count(*) FROM system.information_schema.tables a JOIN system.information_schema.tables b ON a.table_name = b.table_name",
            SqlCompileIntent::Query,
            2,
        ),
        (
            "SELECT table_name FROM system.information_schema.tables",
            plain,
            0,
        ),
        (
            "SELECT table_name FROM system.information_schema.tables",
            analyze,
            1,
        ),
    ] {
        let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::Unavailable);
        let mut fixture = Fixture::new(&provider, ResultWindowClass::Client).await;
        let source = fixture.source();
        let counted = Arc::new(CountSnapshots {
            source: source.clone(),
            calls: AtomicUsize::new(0),
        });
        let facts = Arc::new(statement_fact_source(
            source.owners.clone(),
            source.scope.clone(),
            Arc::new(NoExternalScans),
            counted.clone(),
        ));
        let cpu = source
            .scope
            .planning
            .as_ref()
            .unwrap()
            .cpu
            .as_ref()
            .unwrap()
            .clone();
        let compile = source.scope.planning.as_ref().unwrap().control.clone();
        let taken = ReadAccessSink::new();
        let driver = FinalPlanCompletionDriver::new(facts.clone());
        let completed = tokio::time::timeout(
            Duration::from_secs(5),
            driven(
                &fixture.control,
                &mut fixture.driver,
                driver.complete(
                    request(sql, intent, compile),
                    source.scope.capacity.scope(),
                    &taken,
                    cpu.as_ref(),
                ),
            ),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert!(!completed.plan().fragments().is_empty());
        assert_eq!(
            counted.calls.load(Ordering::Relaxed),
            expected_snapshots,
            "{sql}"
        );
        assert!(
            provider.calls().is_empty(),
            "static system declarations require no external I/O"
        );
        assert!(taken.into_taken().is_empty());
        drop(completed);
        // The driver retains its fact ports. Retire that real borrower before
        // asserting that the statement's last window alias has left.
        drop(driver);
        drop(facts);
        drop(counted);
        drop(cpu);
        drop(source);
        fixture.finish(false).await;
    }
}

struct WrongCarrierSnapshot {
    source: Arc<FrontendSystemTableFacts>,
    calls: AtomicUsize,
}
#[async_trait]
impl SystemTableSnapshotFactPort for WrongCarrierSnapshot {
    async fn resolve_system_tables(
        &self,
        needs: &[SystemTableSnapshotNeed],
    ) -> Result<Vec<SystemTableSnapshotFact>, SqlCompileError> {
        use arrow::{
            array::Array,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        let [need] = needs else {
            panic!("one original system occurrence")
        };
        self.calls.fetch_add(1, Ordering::Relaxed);
        let provider = FrontendSystemTableProvider::new(
            self.source.owners.system_relations.clone(),
            self.source.clone(),
        );
        let request = SystemTableRequest::try_new(
            need.declaration().clone(),
            need.required_columns().to_vec(),
            need.key_constraint().clone(),
            &self.source.scope.connector_context,
            self.source.planning().unwrap().batch_target_rows,
        )
        .unwrap();
        let snapshot = provider.snapshot(&request).await.map_err(sql_error)?;
        let batch = &snapshot.batches[0];
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.column(0).data_type(), &DataType::Utf8);
        // Deliberately violate the provider contract by changing only the
        // carrier header. The original buffers and storage stay unchanged.
        let binary = arrow::array::make_array(
            batch
                .column(0)
                .to_data()
                .into_builder()
                .data_type(DataType::Binary)
                .build()
                .unwrap(),
        );
        let field = batch.schema().field(0).clone();
        let malformed = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                field.name(),
                DataType::Binary,
                field.is_nullable(),
            )])),
            vec![binary],
        )
        .unwrap();
        SystemTableSnapshotFact::try_new(need, vec![malformed].into(), snapshot.storage)
            .map(|fact| vec![fact])
            .map_err(|error| SqlCompileError::Compilation(error.to_string()))
    }
}
#[tokio::test]
async fn original_fe_completion_rejects_a_provider_carrier_type_mismatch_as_internal() {
    let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::Unavailable);
    let mut fixture = Fixture::new(&provider, ResultWindowClass::Client).await;
    let source = fixture.source();
    let faulty = Arc::new(WrongCarrierSnapshot {
        source: source.clone(),
        calls: AtomicUsize::new(0),
    });
    let facts = Arc::new(statement_fact_source(
        source.owners.clone(),
        source.scope.clone(),
        Arc::new(NoExternalScans),
        faulty.clone(),
    ));
    let driver = FinalPlanCompletionDriver::new(facts.clone());
    let cpu = source
        .scope
        .planning
        .as_ref()
        .unwrap()
        .cpu
        .as_ref()
        .unwrap()
        .clone();
    let taken = ReadAccessSink::new();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        driven(
            &fixture.control,
            &mut fixture.driver,
            driver.complete(
                request(
                    "SELECT table_name FROM system.information_schema.tables",
                    SqlCompileIntent::Query,
                    source.planning().unwrap().control.clone(),
                ),
                source.scope.capacity.scope(),
                &taken,
                cpu.as_ref(),
            ),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let Err(error) = result else {
        panic!("malformed carrier cannot become a completed plan")
    };
    assert!(matches!(
        &error,
        novarocks_query_application::preparation::FinalPlanCompletionError::Compiler { .. }
    ));
    // Ordinary Read uses this exact completion projector. A malformed
    // provider carrier stays an internal compiler contract failure.
    let error = crate::query::compiler::FrontendQueryCompilerError::from_completion(&error);
    assert!(matches!(
        error,
        crate::query::compiler::FrontendQueryCompilerError::Engine(_)
    ));
    assert_eq!(faulty.calls.load(Ordering::Relaxed), 1);
    assert!(provider.calls().is_empty());
    assert!(taken.into_taken().is_empty());
    drop(driver);
    drop(facts);
    drop(faulty);
    drop(cpu);
    drop(source);
    fixture.finish(false).await;
}

#[tokio::test]
async fn original_fe_completion_carries_typed_state_table_declaration_into_compiled_plan() {
    let listing = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::Unavailable);
    let mut fixture = Fixture::new(&listing, ResultWindowClass::Client).await;
    let old = fixture.source.take().unwrap();
    let mut owners = old.owners.clone();
    owners.system_relations = Arc::new(
        novarocks_query_application::system_tables::SystemTableRegistry::try_new([
            crate::system_tables::materialized_view_states_registration(),
        ])
        .unwrap(),
    );
    let scope = old.scope.clone();
    old.close().await.unwrap();
    drop(old);
    let source = FrontendSystemTableFacts::new(owners, scope).unwrap();
    fixture.source = Some(source.clone());
    // Plain EXPLAIN compiles the complete typed declaration without opening an
    // inventory. Runtime production reads are exercised against SQLite.
    let counted = Arc::new(CountSnapshots {
        source: source.clone(),
        calls: AtomicUsize::new(0),
    });
    let facts = Arc::new(statement_fact_source(
        source.owners.clone(),
        source.scope.clone(),
        Arc::new(NoExternalScans),
        counted.clone(),
    ));
    let cpu = source
        .scope
        .planning
        .as_ref()
        .unwrap()
        .cpu
        .as_ref()
        .unwrap()
        .clone();
    let control = source.scope.planning.as_ref().unwrap().control.clone();
    let taken = ReadAccessSink::new();
    let driver = FinalPlanCompletionDriver::new(facts.clone());
    let completed=driven(&fixture.control,&mut fixture.driver,driver.complete(
        request("SELECT refresh_paused, last_refresh_time, base_tables FROM system.runtime.materialized_view_states",
            SqlCompileIntent::Explain { level:ExplainLevel::Normal,analyze:false },control),
        source.scope.capacity.scope(),&taken,cpu.as_ref(),
    )).await.unwrap().unwrap();
    assert!(!completed.plan().fragments().is_empty());
    assert_eq!(counted.calls.load(Ordering::Relaxed), 0);
    assert!(taken.into_taken().is_empty());
    drop(completed);
    drop(driver);
    drop(facts);
    drop(counted);
    drop(cpu);
    drop(source);
    fixture.finish(false).await;
}
