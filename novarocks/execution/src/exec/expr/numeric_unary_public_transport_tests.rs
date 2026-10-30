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
//! Actual SQL emission/public-declaration producer and downstream publication consumers.
//! Frozen v12/v10 VALUES probes stay unchanged as diagnostic evidence.
//! Required source columns use the sole actual ConnectorReadTableFacts producer.
//! Public NULL guarantees are the completed expression owner's guarantees.
use super::sql_test_source::sql_source as nonnull_source;
use arrow::datatypes::DataType;
use novarocks_coordination_contract::{ExecutionEffect, RecoveryMode};
use novarocks_functions::ConstantPolicy;
use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId, ScanReadBudget};
use novarocks_query_application::api::QueryExecutionKind;
use novarocks_query_application::preparation::{
    CompletedPhysicalPlanCandidate, ExecutionResourceRequirements, FrozenCostEstimate,
    FrozenEstimateUnknownReason, FrozenExecutionDescription, OutputContract,
};
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, ResultDeclarationError, SessionOptimizerSettings, SqlCompileControl,
    SqlCompileIntent, SqlCompileProgress, SqlCompiler, SqlFinalPlanCompileRequest,
    SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog,
    noop_constant_evaluator,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{ptr, sync::Mutex};
fn table_free_sql(sql: &str) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    let control = SqlCompileControl::unbounded();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([91; 16]).unwrap(),
        SqlStatementInput::sql(sql),
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

fn freeze_root_purpose(
    candidate: CompletedPhysicalPlanCandidate,
) -> CompletedPhysicalPlanCandidate {
    // Borrow the same SQL-owned public declaration before publishing any
    // candidate aliases. This is the application completion's real purpose
    // handoff; neither the caller's possibly wrong output nor Arrow authors it.
    let render = novarocks_sql::compiler::client_render_schema_from_port(
        candidate.original_public_result_port(),
        novarocks_sql::compiler::RenderFamilyRequest::MysqlText {
            timezone_offset_seconds: 0,
        },
    )
    .expect("original public client-row declaration");
    candidate
        .freeze_root_output(novarocks_result_contract::FrozenRootOutput::ClientRows(
            render,
        ))
        .expect("same completed source freezes its root purpose before aliases")
}

fn freeze(
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
#[test]
fn numeric_unary_public_transport_same_original_source_and_all_consumers() {
    for dt in [DataType::Int8, DataType::Int16, DataType::Int32] {
        let sql = "SELECT -k AS original_negated, k AS original_source, -k AS repeated_alias FROM fixture";
        let original = nonnull_source(sql, dt.clone());
        let source = nonnull_source(sql, dt.clone());
        let borrowed = source
            .checked_original_result_declaration_observed(&SqlCompileControl::unbounded())
            .unwrap()
            .unwrap();
        let original_port = original.plan().result_port().unwrap();
        let computed = source.plan().result_port().unwrap();
        assert_eq!(borrowed.original_port().fragment, computed.fragment);
        assert_eq!(borrowed.original_port().output, computed.output);
        assert_eq!(borrowed.fields().len(), 3);
        for (n, (before, public)) in original_port
            .fields
            .iter()
            .zip(borrowed.fields())
            .enumerate()
        {
            assert_eq!(before, public, "same original ordered declaration at {n}");
            assert_eq!(public.ty.data_type().clone(), dt);
            assert_eq!(public.ty.nullable(), n != 1);
            assert_eq!(public.ty.nullable(), computed.fields[n].ty.nullable());
            assert_eq!(public.value, computed.fields[n].value);
            assert_eq!(public.alias, computed.fields[n].alias);
            assert_eq!(public.name, computed.fields[n].name);
            assert_eq!(
                public.ty.logical_type(),
                computed.fields[n].ty.logical_type()
            );
            assert_eq!(public.ty.data_type(), computed.fields[n].ty.data_type());
        }
        assert!(computed.fields[0].ty.nullable());
        assert!(!computed.fields[1].ty.nullable());
        assert!(computed.fields[2].ty.nullable());
        let original_candidate = freeze_root_purpose(
            CompletedPhysicalPlanCandidate::for_sql_program(
                original,
                &SqlCompileControl::unbounded(),
            )
            .unwrap(),
        );
        let original_output =
            OutputContract::from_completed_candidate(QueryExecutionKind::Read, &original_candidate)
                .unwrap();
        // Clone shares the publication-proved original port, never copies its fields.
        let cloned = source.clone();
        assert!(ptr::eq(
            borrowed.original_port(),
            cloned
                .original_public_result_declaration()
                .unwrap()
                .original_port()
        ));
        drop(cloned);
        let candidate = CompletedPhysicalPlanCandidate::for_sql_program(
            source,
            &SqlCompileControl::unbounded(),
        )
        .unwrap();
        // Preserve the original false-public-obligation rejection at the
        // exact root boundary. Only the two computed negations are forged.
        let wrong_candidate = CompletedPhysicalPlanCandidate::for_sql_program(
            nonnull_source(sql, dt.clone()),
            &SqlCompileControl::unbounded(),
        )
        .unwrap();
        let render = novarocks_sql::compiler::client_render_schema_from_port(
            wrong_candidate.original_public_result_port(),
            novarocks_sql::compiler::RenderFamilyRequest::MysqlText {
                timezone_offset_seconds: 0,
            },
        )
        .unwrap();
        let mut wrong_columns = render.columns().to_vec();
        wrong_columns[0].field.nullable = false;
        wrong_columns[2].field.nullable = false;
        let wrong_render = novarocks_result_contract::ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
            wrong_columns,
            3,
        )
        .unwrap();
        let refusal = wrong_candidate
            .freeze_root_output(novarocks_result_contract::FrozenRootOutput::ClientRows(
                wrong_render,
            ))
            .unwrap_err()
            .to_string();
        assert!(refusal.contains("result_port.render_schema"), "{refusal}");
        assert!(
            refusal.contains("render occurrence differs at ordinal 0"),
            "{refusal}"
        );
        assert!(
            refusal.contains("render occurrence differs at ordinal 2"),
            "{refusal}"
        );
        let candidate = freeze_root_purpose(candidate);
        let output =
            OutputContract::from_completed_candidate(QueryExecutionKind::Read, &candidate).unwrap();
        assert_eq!(output.fields().len(), 3);
        assert_eq!(output.fields(), original_output.fields());
        for (ordinal, (field, label)) in output
            .fields()
            .iter()
            .zip(["original_negated", "original_source", "repeated_alias"])
            .enumerate()
        {
            assert_eq!(field.name(), label);
            assert_eq!(field.data_type(), &dt);
            assert_eq!(field.nullable(), ordinal != 1);
        }
        let from_plan =
            OutputContract::from_completed_plan(QueryExecutionKind::Read, candidate.plan())
                .unwrap();
        assert_eq!(from_plan.fields(), output.fields());
        let wrong = OutputContract::Rows(
            output
                .fields()
                .iter()
                .map(|field| {
                    novarocks_query_application::api::ResultField::new(
                        field.name(),
                        field.value_type().with_nullable(false).unwrap(),
                    )
                })
                .collect::<Vec<_>>()
                .into(),
        );
        assert_eq!(
            freeze(candidate.clone(), wrong).unwrap_err(),
            "completed plan output differs from its frozen execution contract"
        );
        let published = freeze(candidate.clone(), output.clone()).unwrap();
        assert_eq!(published.output().fields(), output.fields());
        assert!(ptr::eq(
            candidate.original_public_result_port().unwrap(),
            candidate.clone().original_public_result_port().unwrap()
        ));
    }
}
#[test]
fn numeric_unary_public_transport_original_and_unchanged_nested_types_use_actual_source() {
    for sql in [
        "SELECT 1 AS a, CAST(NULL AS VARCHAR) AS b",
        "SELECT CAST([CAST(1 AS INT)] AS ARRAY<INT>) AS nested",
    ] {
        {
            let source = table_free_sql(sql);
            let port = source.plan().result_port().unwrap();
            let original = source.original_public_result_declaration().unwrap();
            assert_eq!(original.fields(), port.fields.as_ref());

            let candidate = CompletedPhysicalPlanCandidate::for_sql_program(
                source,
                &SqlCompileControl::unbounded(),
            )
            .unwrap();
            let candidate = freeze_root_purpose(candidate);
            let out =
                OutputContract::from_completed_candidate(QueryExecutionKind::Read, &candidate)
                    .unwrap();
            assert!(freeze(candidate, out).is_ok());
        }
    }
}
struct Trace {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    fail: Option<usize>,
    cause: CompileControlError,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, p: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut calls = self.calls.lock().unwrap();
        calls.push((p, n));
        if self.fail == Some(calls.len() - 1) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}
#[test]
fn numeric_unary_public_transport_observed_receipt_preserves_first_control_cause() {
    let label = "x".repeat(700);
    let source = nonnull_source(
        &format!("SELECT -k AS `{label}` FROM fixture"),
        DataType::Int8,
    );
    let ok = Trace {
        calls: Mutex::new(Vec::new()),
        fail: None,
        cause: CompileControlError::Cancelled,
    };
    source
        .checked_original_result_declaration_observed(&ok)
        .unwrap();
    let prefix = ok.calls.lock().unwrap().clone();
    assert!(prefix.iter().any(|(_, n)| *n == 256));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..prefix.len() {
            let fail = Trace {
                calls: Mutex::new(Vec::new()),
                fail: Some(at),
                cause: cause.clone(),
            };
            match source.checked_original_result_declaration_observed(&fail) {
                Err(ResultDeclarationError::Control(got)) => assert_eq!(got, cause),
                _ => panic!("receipt must retain the actual compile control failure"),
            };
            assert_eq!(
                &*fail.calls.lock().unwrap(),
                &prefix[..=at],
                "no secondary footer/checkpoint after refusal"
            );
        }
    }
}
