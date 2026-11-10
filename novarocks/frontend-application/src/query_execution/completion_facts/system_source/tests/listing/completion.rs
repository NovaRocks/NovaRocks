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
struct NoExternalScans;
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
fn request(
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
                    &source.scope.capacity.scope(),
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
