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
use novarocks_coordination_contract::{ExecutionEffect, RecoveryMode};
use novarocks_functions::ConstantPolicy;
use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId, ScanReadBudget};
use novarocks_query_application::api::QueryExecutionKind;
use novarocks_query_application::preparation::{
    CompletedPhysicalPlanCandidate, ExecutionResourceRequirements, FrozenCostEstimate,
    FrozenEstimateUnknownReason, FrozenExecutionDescription, OutputContract,
};
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
    SqlCompileProgress, SqlCompiler, SqlFinalPlanCompileRequest, SqlPlanningEnvironment,
    SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog, noop_constant_evaluator,
};
pub(super) fn table_free_sql(sql: &str) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    let control = SqlCompileControl::unbounded();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([91; 16]).unwrap(),
        SqlStatementInput::parsed_query(novarocks_sql_testkit::parse_query(sql)),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                enable_common_subexpr_reuse: Some(false),
                ..SessionOptimizerSettings::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        noop_constant_evaluator(),
        // Explicit fixture admission; neither capacity grant nor defaults.
        ConstantPolicy {
            max_rows: 64,
            max_array_nodes: 1024,
            max_logical_elements: 4096,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
        control.clone(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        ScanReadBudget {
            max_batch_rows: 64,
            max_batch_bytes: 1 << 20,
        },
        DEFAULT_COMPLETION_LIMITS,
    );
    match SqlCompiler::start(request.try_into_completion().unwrap(), &control).unwrap() {
        SqlCompileProgress::Complete(completed) => completed.into_plan(),
        SqlCompileProgress::Incomplete(need) => {
            panic!("table-free original fixture unexpectedly asks for facts: {need:?}")
        }
    }
}

pub(super) fn freeze_root_purpose(
    candidate: CompletedPhysicalPlanCandidate,
) -> CompletedPhysicalPlanCandidate {
    // Borrow the same SQL-owned public declaration before publishing any
    // candidate aliases. This is the application completion's real purpose
    // handoff; neither the caller's possibly wrong output nor Arrow authors it.
    let render = novarocks_sql::compiler::client_render_schema_from_port(
        candidate.original_public_result_port(),
        novarocks_sql::compiler::RenderFamilyRequest::TrinoJson(
            &novarocks_result_contract::TrinoJsonFacts {
                parametric_datetime: true,
                variant: true,
                variant_binary: true,
            },
        ),
    )
    .expect("original public client-row declaration");
    candidate
        .freeze_root_output(novarocks_result_contract::FrozenRootOutput::ClientRows(
            render,
        ))
        .expect("same completed source freezes its root purpose before aliases")
}

pub(super) fn freeze(
    candidate: CompletedPhysicalPlanCandidate,
    output: OutputContract,
) -> Result<FrozenExecutionDescription, String> {
    let version = candidate.plan().version();
    let scans = candidate
        .plan()
        .fragments()
        .values()
        .flat_map(|fragment| {
            fragment
                .nodes()
                .values()
                .filter(|&node| {
                    matches!(&node.kind, novarocks_physical_plan::NodeKind::Scan { .. })
                })
                .map(|node| {
                    novarocks_query_application::api::PlanScanIdentity::new(
                        novarocks_query_application::api::PlanSeal::Version(version),
                        fragment.id().get(),
                        i32::try_from(node.id.get()).unwrap(),
                    )
                })
        })
        .collect();
    FrozenExecutionDescription::for_completed_plan(
        QueryExecutionKind::Read,
        candidate,
        scans,
        output,
        ExecutionEffect::None,
        RecoveryMode::NoRecovery,
        Vec::new(),
        FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
        ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
    )
}
