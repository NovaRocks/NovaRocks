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

use novarocks_sql::plan_read::{DataSink, DistributedPlan, FragmentEdge, FragmentId, PlanFragment};

fn read_sealed_plan(plan: &DistributedPlan) {
    let _: &[PlanFragment] = plan.fragments();
    let _: FragmentId = plan.root_fragment_id();
    let _: &[FragmentEdge] = plan.edges();
    let _: &DataSink = &plan.fragments()[0].sink;
    let _ = plan.node_outputs();
    let _ = plan.fragment_edge_outputs();
    let _ = plan.write_contracts();
}

#[test]
fn external_consumers_can_read_but_not_construct_a_sealed_plan() {
    let _ = read_sealed_plan as fn(&DistributedPlan);
}

use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::compiler::{
    SessionOptimizerSettings, SqlAnalyzeRequest, SqlAnalyzedQuery, SqlCatalogSnapshot,
    SqlCompileControl, SqlCompileIntent, SqlCompiler, SqlFunctionCatalog, SqlOptimizeRequest,
    SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog,
};
use novarocks_sql::planning::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
use novarocks_sql::planning::dml::DmlStatisticsSnapshot;

struct NegativeImplMarker;

trait AmbiguousIfClone<Marker> {
    const TOKEN: ();
}

impl<T: ?Sized> AmbiguousIfClone<()> for T {
    const TOKEN: () = ();
}

impl<T: Clone> AmbiguousIfClone<NegativeImplMarker> for T {
    const TOKEN: () = ();
}

trait AmbiguousIfSerialize<Marker> {
    const TOKEN: ();
}

impl<T: ?Sized> AmbiguousIfSerialize<()> for T {
    const TOKEN: () = ();
}

impl<T: ?Sized + serde::Serialize> AmbiguousIfSerialize<NegativeImplMarker> for T {
    const TOKEN: () = ();
}

trait AmbiguousIfDeserializeOwned<Marker> {
    const TOKEN: ();
}

impl<T: ?Sized> AmbiguousIfDeserializeOwned<()> for T {
    const TOKEN: () = ();
}

impl<T: serde::de::DeserializeOwned> AmbiguousIfDeserializeOwned<NegativeImplMarker> for T {
    const TOKEN: () = ();
}

#[allow(
    path_statements,
    reason = "These associated-constant references are compile-time negative trait-contract assertions."
)]
const _: () = {
    <SqlAnalyzedQuery as AmbiguousIfClone<_>>::TOKEN;
    <SqlAnalyzedQuery as AmbiguousIfSerialize<_>>::TOKEN;
    <SqlAnalyzedQuery as AmbiguousIfDeserializeOwned<_>>::TOKEN;
};

struct EmptyCatalog;

impl PlannerTableProvider for EmptyCatalog {
    fn resolve_table_for_analysis(
        &self,
        _catalog: Option<&str>,
        _database: &str,
        _table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        Err("the public-contract query has no tables".to_string())
    }
}

impl SqlCatalogSnapshot for EmptyCatalog {
    fn planner_table_provider(&self) -> &dyn PlannerTableProvider {
        self
    }
}

fn scoped_builtin_functions(scope: &mut ()) -> &dyn SqlFunctionCatalog {
    let _ = scope;
    builtin_sql_function_catalog()
}

#[test]
fn external_sql_contract_analyzes_freezes_and_reads_a_sealed_plan() {
    let analyzed = {
        let catalog = EmptyCatalog;
        let mut function_scope = ();
        let functions = scoped_builtin_functions(&mut function_scope);
        let request = SqlAnalyzeRequest::new(
            SqlStatementInput::sql("SELECT 1"),
            SqlCompileIntent::Query,
            SqlSessionContext {
                current_catalog: None,
                current_database: "default".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            &catalog,
            functions,
            novarocks_sql::compiler::noop_constant_evaluator(),
            None,
            SqlCompileControl::unbounded(),
        );

        SqlCompiler::analyze(request)
            .expect("public analysis request materializes")
            .into_pending()
            .expect("query analysis waits for frozen statistics")
    };

    // The catalog and function-capability bindings have left scope. Phase two
    // can receive only the move-only analyzed handle and immutable statistics.
    let statistics = DmlStatisticsSnapshot::empty();
    let plan = SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics))
        .expect("public optimize request consumes frozen statistics")
        .into_distributed_plan()
        .expect("query intent produces a sealed distributed plan");
    assert!(!plan.fragments().is_empty());

    let _: Option<SqlTableBindingId> = None;
}
