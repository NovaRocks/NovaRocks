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

//! Real completion transitions for stored-view dependency closure.

use super::*;
use crate::catalog::{SqlViewColumn, SqlViewDefinition};
use crate::compiler::{
    CatalogRelationFact, CatalogRelationNeed, CatalogTimeTravelResolution, CompletionLimits,
    CompletionProtocolError, CompletionResource, DEFAULT_COMPLETION_LIMITS,
    SqlCapturedCatalogPreparation, SqlCompilation, SqlCompileProgress, SqlCompileProgressError,
    SqlFactBatch, SqlNeedBatch,
};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;

fn definition(need: &crate::compiler::CatalogRelationNeed, sql: &str) -> Arc<SqlViewDefinition> {
    Arc::new(SqlViewDefinition {
        identity: need.relation().clone(),
        version: "immutable-version-1".into(),
        raw_sql: sql.into(),
        default_catalog: need.relation().catalog.as_str().into(),
        default_database: need.relation().namespace.as_str().into(),
        columns: vec![SqlViewColumn {
            source_field_name: Some("x".into()),
            name: "x".into(),
            value_type: FunctionValueType::new(arrow::datatypes::DataType::Int64, false),
        }]
        .into(),
    })
}
fn start(limits: CompletionLimits) -> SqlCompilation {
    let mut request = super::tests::request("SELECT x FROM v0", SqlCompileIntent::Query);
    request.limits = limits;
    match SqlCompiler::start(
        request.try_into_completion().unwrap(),
        &SqlCompileControl::unbounded(),
    )
    .unwrap()
    {
        SqlCompileProgress::Incomplete(compilation) => compilation,
        _ => panic!("original relation requires a catalog fact"),
    }
}
fn one_need(compilation: &SqlCompilation) -> crate::compiler::CatalogRelationNeed {
    let SqlNeedBatch::CatalogRelations(needs) = compilation.needs() else {
        panic!("catalog round");
    };
    assert_eq!(needs.len(), 1);
    needs[0].clone()
}
fn answer(
    compilation: SqlCompilation,
    sql: &str,
) -> Result<SqlCompileProgress, SqlCompileProgressError> {
    let need = one_need(&compilation);
    let fact = CatalogRelationFact::view(&need, definition(&need, sql)).unwrap();
    SqlCompiler::finish(
        compilation,
        SqlFactBatch::CatalogRelations(vec![fact].into_boxed_slice()),
        &SqlCompileControl::unbounded(),
    )
}

#[test]
fn real_completion_closes_eight_view_rounds_under_the_original_four_phase_limit() {
    let mut compilation = start(DEFAULT_COMPLETION_LIMITS);
    for ordinal in 0..8 {
        let need = one_need(&compilation);
        assert_eq!(need.relation().table, format!("v{ordinal}"));
        assert_eq!(need.allows_local_view_shadow(), ordinal == 0);
        assert_eq!(compilation.usage().rounds, ordinal + 1);
        let sql = if ordinal == 7 {
            "SELECT 1 AS x".into()
        } else {
            format!("SELECT x FROM v{}", ordinal + 1)
        };
        let progress = answer(compilation, &sql).unwrap();
        if ordinal == 7 {
            let completed = progress.into_complete().unwrap();
            assert_eq!(
                completed.plan().result_port().unwrap().fields[0]
                    .alias
                    .as_deref(),
                Some("x")
            );
            return;
        }
        let SqlCompileProgress::Incomplete(next) = progress else {
            panic!("unanswered child cannot complete");
        };
        compilation = next;
    }
    panic!("view closure did not complete");
}

#[test]
fn discovery_credit_never_bypasses_distinct_needs_or_current_exchange_bytes() {
    let compilation = start(CompletionLimits::try_new(1, 2, 32 * 1024 * 1024).unwrap());
    let SqlCompileProgress::Incomplete(compilation) =
        answer(compilation, "SELECT x FROM v1").unwrap()
    else {
        panic!("child need");
    };
    assert!(matches!(
        answer(compilation, "SELECT x FROM v2"),
        Err(SqlCompileProgressError::Protocol(
            CompletionProtocolError::BudgetExceeded {
                resource: CompletionResource::DistinctNeeds,
                ..
            }
        ))
    ));
    let compilation = start(CompletionLimits::try_new(1, 10, 2048).unwrap());
    let body = format!("SELECT 1 AS x /* {} */", "retained definition".repeat(512));
    assert!(matches!(
        answer(compilation, &body),
        Err(SqlCompileProgressError::Protocol(
            CompletionProtocolError::BudgetExceeded {
                resource: CompletionResource::ExchangeBytes,
                ..
            }
        ))
    ));
}

#[test]
fn answered_view_need_cannot_be_replayed_to_create_discovery_progress() {
    let compilation = start(DEFAULT_COMPLETION_LIMITS);
    let original = one_need(&compilation);
    let SqlCompileProgress::Incomplete(next) = answer(compilation, "SELECT x FROM v1").unwrap()
    else {
        panic!("child need");
    };
    let replay =
        CatalogRelationFact::view(&original, definition(&original, "SELECT x FROM v1")).unwrap();
    assert!(matches!(
        SqlCompiler::finish(
            next,
            SqlFactBatch::CatalogRelations(vec![replay].into_boxed_slice()),
            &SqlCompileControl::unbounded()
        ),
        Err(SqlCompileProgressError::Protocol(
            CompletionProtocolError::UnexpectedFact(_)
        ))
    ));
}

#[test]
fn immutable_version_cannot_change_body_when_revisited_by_exact_lookup() {
    let compilation = start(DEFAULT_COMPLETION_LIMITS);
    let SqlCompileProgress::Incomplete(next) =
        answer(compilation, "SELECT x FROM iceberg.db.v0").unwrap()
    else {
        panic!("saved exact lookup is distinct");
    };
    let result = answer(next, "SELECT 1 AS x");
    assert!(
        matches!(result, Err(SqlCompileProgressError::Compile(SqlCompileError::Compilation(message))) if message.contains("conflicting definition facts"))
    );
}

#[test]
fn expression_subquery_dependencies_use_saved_context_without_caller_ctes() {
    let request = super::tests::request(
        "WITH leaf AS (SELECT 'caller' AS x) SELECT x FROM v0",
        SqlCompileIntent::Query,
    );
    let SqlCompileProgress::Incomplete(compilation) = SqlCompiler::start(
        request.try_into_completion().unwrap(),
        &SqlCompileControl::unbounded(),
    )
    .unwrap() else {
        panic!("view need");
    };
    let need = one_need(&compilation);
    let mut view = (*definition(&need, "SELECT (SELECT x FROM leaf) AS x")).clone();
    view.default_database = "saved".into();
    view.columns = vec![SqlViewColumn {
        source_field_name: Some("x".into()),
        name: "x".into(),
        value_type: FunctionValueType::new(arrow::datatypes::DataType::Int64, true),
    }]
    .into();
    let fact = CatalogRelationFact::view(&need, Arc::new(view)).unwrap();
    let SqlCompileProgress::Incomplete(next) = SqlCompiler::finish(
        compilation,
        SqlFactBatch::CatalogRelations(vec![fact].into_boxed_slice()),
        &SqlCompileControl::unbounded(),
    )
    .unwrap() else {
        panic!("saved leaf need");
    };
    let need = one_need(&next);
    assert_eq!(need.relation().fqn(), "iceberg.saved.leaf");
    assert!(!need.allows_local_view_shadow());
    answer(next, "SELECT 1 AS x")
        .unwrap()
        .into_complete()
        .unwrap();
}

fn query(sql: &str) -> novarocks_parser::ast::Query {
    let mut statements = novarocks_parser::parse(sql).unwrap();
    let novarocks_parser::ast::Statement::Query(query) = statements.remove(0) else {
        panic!("query fixture")
    };
    query
}

#[test]
fn implicit_local_and_explicit_external_view_facts_keep_distinct_actual_identities() {
    let request = super::tests::request(
        "SELECT a.x, b.x FROM v0 a CROSS JOIN iceberg.db.v0 b",
        SqlCompileIntent::Query,
    );
    let SqlCompileProgress::Incomplete(compilation) = SqlCompiler::start(
        request.try_into_completion().unwrap(),
        &SqlCompileControl::unbounded(),
    )
    .unwrap() else {
        panic!("view needs")
    };
    let SqlNeedBatch::CatalogRelations(needs) = compilation.needs() else {
        panic!("catalog")
    };
    assert_eq!(needs.len(), 2);
    let mut local = (*definition(&needs[0], "SELECT 1 AS x")).clone();
    local.identity.catalog = novarocks_types::naming::DEFAULT_CATALOG.into();
    local.default_catalog = local.identity.catalog.as_str().into();
    assert!(needs[0].allows_local_view_shadow());
    assert!(!needs[1].allows_local_view_shadow());
    assert!(CatalogRelationFact::view(&needs[1], Arc::new(local.clone())).is_err());
    let facts = vec![
        CatalogRelationFact::view(&needs[0], Arc::new(local)).unwrap(),
        CatalogRelationFact::view(&needs[1], definition(&needs[1], "SELECT 2 AS x")).unwrap(),
    ];
    SqlCompiler::finish(
        compilation,
        SqlFactBatch::CatalogRelations(facts.into()),
        &SqlCompileControl::unbounded(),
    )
    .unwrap()
    .into_complete()
    .unwrap();
}

#[test]
fn one_statement_cannot_mix_versions_of_one_actual_view_through_different_qualifications() {
    let request = super::tests::request(
        "SELECT a.x, b.x FROM v0 a CROSS JOIN iceberg.db.v0 b",
        SqlCompileIntent::Query,
    );
    let SqlCompileProgress::Incomplete(compilation) = SqlCompiler::start(
        request.try_into_completion().unwrap(),
        &SqlCompileControl::unbounded(),
    )
    .unwrap() else {
        panic!("view needs")
    };
    let SqlNeedBatch::CatalogRelations(needs) = compilation.needs() else {
        panic!("catalog")
    };
    let mut second = (*definition(&needs[1], "SELECT 2 AS x")).clone();
    second.version = "immutable-version-2".into();
    let facts = vec![
        CatalogRelationFact::view(&needs[0], definition(&needs[0], "SELECT 1 AS x")).unwrap(),
        CatalogRelationFact::view(&needs[1], Arc::new(second)).unwrap(),
    ];
    assert!(
        matches!(SqlCompiler::finish(compilation, SqlFactBatch::CatalogRelations(facts.into()),
        &SqlCompileControl::unbounded()),
        Err(SqlCompileProgressError::Compile(SqlCompileError::Compilation(message)))
        if message.contains("different versions"))
    );
}

#[test]
fn stored_collector_semantic_refusals_reach_the_actual_view_analysis_boundary() {
    let compilation = start(DEFAULT_COMPLETION_LIMITS);
    let result = answer(
        compilation,
        "WITH a AS (SELECT x FROM b), b AS (SELECT 1 AS x) SELECT x FROM a",
    );
    assert!(matches!(
        result,
        Err(SqlCompileProgressError::Compile(SqlCompileError::Analyze(
            _
        )))
    ));
}

#[test]
fn schema_only_closure_uses_current_exchange_bytes_instead_of_accumulating_old_rounds() {
    use super::super::completion_catalog::CatalogCompletionState;
    let control = SqlCompileControl::unbounded();
    let mut state = CatalogCompletionState::try_new(
        query("SELECT x FROM v0"),
        Some("iceberg"),
        "db",
        crate::catalog::TableLookupMode::SchemaOnly,
        0,
    )
    .unwrap()
    .with_exchange_budget(CompletionLimits::try_new(1, 20, 8192).unwrap())
    .unwrap();
    for ordinal in 0..8 {
        let [need] = state.needs() else {
            panic!("one child")
        };
        let body = if ordinal == 7 {
            "SELECT 1 AS x".into()
        } else {
            format!("SELECT x FROM v{}", ordinal + 1)
        };
        let body = format!("{body} /* {} */", "retained declaration".repeat(150));
        let fact = CatalogRelationFact::view(need, definition(need, &body)).unwrap();
        state.accept_facts(vec![fact].into(), &control).unwrap();
    }
    assert!(state.needs().is_empty());
    state.completed_catalog().unwrap();
}

#[test]
fn legacy_schema_only_terminal_cannot_bypass_incoming_fact_bytes() {
    let source = super::super::SqlCapturedCatalogPreparation::try_new(
        query("SELECT x FROM missing"),
        Some("iceberg"),
        "db",
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    let fact =
        CatalogRelationFact::missing(&source.needs()[0], "x".repeat(33 * 1024 * 1024)).unwrap();
    assert!(
        matches!(source.capture(&[fact]), Err(SqlCompileError::Compilation(message))
        if message.contains("ExchangeBytes"))
    );
}

#[test]
fn definition_capture_freezes_exact_context_and_recognizes_replacement_before_old_body_io() {
    use super::super::SqlCapturedCatalogPreparation;
    let target = novarocks_types::naming::TableIdentity {
        catalog: "iceberg".into(),
        namespace: "db".into(),
        table: "v0".into(),
    };
    let source = SqlCapturedCatalogPreparation::try_new_definition(
        query("SELECT x FROM v0"),
        "iceberg",
        "db",
        Some(target),
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    assert!(!source.needs()[0].allows_local_view_shadow());
    let fact = CatalogRelationFact::view(
        &source.needs()[0],
        definition(
            &source.needs()[0],
            "SELECT x FROM never_observe_this_old_body",
        ),
    )
    .unwrap();
    let source = source.continue_catalog(vec![fact].into()).unwrap();
    assert!(source.needs().is_empty());
    let result = source.analyze_view_definition(
        &[],
        crate::functions::builtin_sql_function_catalog(),
        crate::constant::test_constant_policy(),
    );
    assert!(matches!(result, Err(SqlCompileError::Analyze(error))
        if error.kind() == crate::analyze_error::AnalyzeErrorKind::ViewIsRecursive));
}

#[test]
fn semantic_refusal_does_not_lose_other_valid_saved_dependencies() {
    let compilation = start(DEFAULT_COMPLETION_LIMITS);
    let result = answer(compilation,
        "WITH a AS (SELECT x FROM real_table), b AS (SELECT x FROM later), later AS (SELECT 1 AS x) SELECT x FROM a").unwrap();
    let SqlCompileProgress::Incomplete(compilation) = result else {
        panic!("real dependency")
    };
    assert_eq!(one_need(&compilation).relation().table, "real_table");
    let result = answer(compilation, "SELECT 1 AS x");
    let Err(SqlCompileProgressError::Compile(SqlCompileError::Analyze(error))) = result else {
        panic!("stored semantic refusal");
    };
    assert_eq!(
        error.kind(),
        crate::analyze_error::AnalyzeErrorKind::InvalidView
    );
}

#[test]
fn unsupported_saved_quoted_identifier_is_not_an_unrequested_fact_protocol_error() {
    let result = answer(
        start(DEFAULT_COMPLETION_LIMITS),
        "SELECT 1 AS x FROM `bad-name`",
    );
    let Err(SqlCompileProgressError::Compile(SqlCompileError::Analyze(error))) = result else {
        panic!("unsupported saved identifier");
    };
    assert_eq!(
        error.kind(),
        crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression
    );
    assert!(error.stored_view_definition_source().is_some());
}

#[test]
fn dml_and_logical_explain_analyze_the_same_closed_saved_view_facts() {
    let session = || super::super::SqlSessionContext {
        current_catalog: Some("iceberg".into()),
        current_database: "db".into(),
        optimizer_settings: super::super::SessionOptimizerSettings::default(),
        sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
    };
    let control = SqlCompileControl::unbounded();
    let mut dml = super::super::SqlDmlCatalogPreparation::try_new(
        super::super::SqlStatementInput::parsed_query(Box::new(query("SELECT x FROM v0"))),
        session(),
        control.clone(),
        super::super::RootDistributionRequirement::Any,
    )
    .unwrap();
    let mut logical = super::super::SqlLogicalExplainCatalog::try_new(
        query("SELECT x FROM v0"),
        session(),
        control,
    )
    .unwrap();
    for sql in ["SELECT x FROM child", "SELECT 1 AS x"] {
        let fact =
            CatalogRelationFact::view(&dml.needs()[0], definition(&dml.needs()[0], sql)).unwrap();
        dml = dml.continue_catalog(vec![fact].into()).unwrap();
        let fact =
            CatalogRelationFact::view(&logical.needs()[0], definition(&logical.needs()[0], sql))
                .unwrap();
        logical = logical.continue_catalog(vec![fact].into()).unwrap();
    }
    assert!(dml.needs().is_empty());
    assert!(logical.needs().is_empty());
    dml.analyze(
        &[],
        crate::functions::builtin_sql_function_catalog(),
        super::super::noop_constant_evaluator(),
        crate::constant::test_constant_policy(),
    )
    .unwrap();
    assert!(
        !logical
            .finish(
                &[],
                crate::functions::builtin_sql_function_catalog(),
                super::super::noop_constant_evaluator(),
                crate::constant::test_constant_policy(),
                super::super::SqlPlanningEnvironment::Distributed,
                crate::explain::ExplainLevel::Normal,
            )
            .unwrap()
            .is_empty()
    );
}

#[test]
fn schema_only_continuation_rejects_an_empty_round_without_outstanding_needs() {
    let source = super::super::SqlCapturedCatalogPreparation::try_new(
        query("SELECT 1 AS x"),
        Some("iceberg"),
        "db",
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    assert!(matches!(
        source.continue_catalog(Box::default()),
        Err(SqlCompileError::InvalidRequest(_))
    ));
}

fn selection_reference_facts() -> crate::planning::catalog::SqlTimeTravelReferenceMetadataFacts {
    use crate::planning::catalog::*;
    SqlTimeTravelReferenceMetadataFacts::try_new(
        vec![10, 20],
        vec![
            SqlTimeTravelSnapshotLogFacts::new(10, 1000),
            SqlTimeTravelSnapshotLogFacts::new(20, 2000),
        ],
        vec![
            SqlTimeTravelNamedReferenceFacts::try_new(
                "backup".into(),
                SqlTimeTravelReferenceKind::Tag,
                10,
            )
            .unwrap(),
        ],
        Some(20),
    )
    .unwrap()
}

fn selection_table(
    need: &CatalogRelationNeed,
    ordinal: u32,
    snapshot: Option<i64>,
    data_type: arrow::datatypes::DataType,
) -> crate::catalog::ResolvedAnalyzerTable {
    use crate::planner::table::*;
    crate::catalog::ResolvedAnalyzerTable::from_planner(
        Some(&need.relation().catalog),
        &need.relation().namespace,
        TableDef {
            name: need.relation().table.clone(),
            columns: vec![novarocks_types::schema::ColumnDef {
                name: "x".into(),
                data_type,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::Sql(SqlScanSource::new(
                crate::binding::SqlTableBindingId::new_for_test(ordinal),
                SqlTableIdentity::try_new(
                    need.relation().catalog.clone(),
                    need.relation().namespace.clone(),
                    need.relation().table.clone(),
                )
                .unwrap(),
                SqlScanKind::Data {
                    version: snapshot.map_or(
                        SqlTableVersionSelector::Current,
                        SqlTableVersionSelector::Snapshot,
                    ),
                },
            )),
        },
    )
}

fn selected_answer(need: &CatalogRelationNeed, ordinal: u32) -> CatalogRelationFact {
    let CatalogTimeTravelResolution::Selected(binding) = need
        .resolve_time_travel(&selection_reference_facts())
        .unwrap()
    else {
        panic!("known reference")
    };
    let data_type = if binding.snapshot_id() == 10 {
        arrow::datatypes::DataType::Int32
    } else {
        arrow::datatypes::DataType::Int64
    };
    CatalogRelationFact::resolved_selected(
        need,
        selection_table(need, ordinal, Some(binding.snapshot_id()), data_type),
        &binding,
    )
    .unwrap()
}

#[test]
fn exact_selection_requests_deduplicate_aliases_without_merging_current_or_other_requests() {
    let source = SqlCapturedCatalogPreparation::try_new(
        query("SELECT a.x FROM ice.db.orders a JOIN ice.db.orders FOR VERSION AS OF 10 b ON 1=1 JOIN ice.db.orders FOR VERSION AS OF 20 c ON 1=1 JOIN ice.db.orders FOR VERSION AS OF 'backup' d ON 1=1 JOIN ice.db.orders FOR SYSTEM_TIME AS OF 1000 e ON 1=1 JOIN ice.db.orders FOR VERSION AS OF 10 f ON 1=1"),
        Some("ice"), "db", SqlCompileControl::unbounded(),
    ).unwrap();
    assert_eq!(source.needs().len(), 5);
    assert_eq!(
        source
            .needs()
            .iter()
            .filter(|need| need.version().is_some())
            .count(),
        4
    );
    assert!(
        source
            .needs()
            .iter()
            .all(|need| !need.allows_local_view_shadow())
    );
}

#[test]
fn selected_fact_requires_the_same_sql_request_and_actual_historical_selector() {
    let source = SqlCapturedCatalogPreparation::try_new(
        query("SELECT a.x FROM ice.db.orders FOR VERSION AS OF 10 a JOIN ice.db.orders FOR VERSION AS OF 20 b ON 1=1"),
        Some("ice"), "db", SqlCompileControl::unbounded(),
    ).unwrap();
    let [first, second] = source.needs() else {
        panic!("two snapshots")
    };
    let CatalogTimeTravelResolution::Selected(binding) = first
        .resolve_time_travel(&selection_reference_facts())
        .unwrap()
    else {
        panic!("known snapshot")
    };
    for snapshot in [None, Some(20)] {
        assert!(
            CatalogRelationFact::resolved_selected(
                first,
                selection_table(first, 1, snapshot, arrow::datatypes::DataType::Int32),
                &binding
            )
            .is_err()
        );
    }
    assert!(
        CatalogRelationFact::resolved_selected(
            second,
            selection_table(second, 2, Some(10), arrow::datatypes::DataType::Int32),
            &binding
        )
        .is_err()
    );
    assert!(
        CatalogRelationFact::resolved(
            first,
            selection_table(first, 1, Some(10), arrow::datatypes::DataType::Int32)
        )
        .is_err()
    );
    assert!(CatalogRelationFact::view(first, definition(first, "SELECT 1 AS x")).is_err());
    let another_relation = SqlCapturedCatalogPreparation::try_new(
        query("SELECT x FROM ice.other.orders FOR VERSION AS OF 10"),
        Some("ice"),
        "db",
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    let other_need = &another_relation.needs()[0];
    assert!(
        CatalogRelationFact::resolved_selected(
            other_need,
            selection_table(other_need, 3, Some(10), arrow::datatypes::DataType::Int32),
            &binding
        )
        .is_err()
    );
    let fact = selected_answer(first, 1);
    let Err(captured) = source.capture(&[fact]) else {
        panic!("one snapshot cannot answer two exact needs")
    };
    assert!(
        captured
            .to_string()
            .contains("did not answer every exact request")
    );
    let wrong_source = SqlCapturedCatalogPreparation::try_new(
        query("SELECT x FROM ice.db.orders FOR VERSION AS OF 20"),
        Some("ice"),
        "db",
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    // Another capture can have the same need ordinal and relation. Its
    // snapshot request still cannot answer this capture's first need.
    let wrong = selected_answer(&wrong_source.needs()[0], 2);
    let Err(error) = source.capture(&[wrong]) else {
        panic!("another selected request cannot answer this capture")
    };
    assert!(
        error
            .to_string()
            .contains("does not match its exact request")
    );
}

#[test]
fn historical_schemas_keep_physical_names_aliases_and_separate_occurrence_ids() {
    use arrow::datatypes::DataType as D;
    let sql = "SELECT a.x AS older, b.x AS newer, c.x AS now, d.x AS repeated FROM ice.db.orders FOR VERSION AS OF 10 a JOIN ice.db.orders FOR VERSION AS OF 20 b ON 1=1 JOIN ice.db.orders c ON 1=1 JOIN ice.db.orders FOR VERSION AS OF 10 d ON 1=1";
    let source = SqlCapturedCatalogPreparation::try_new(
        query(sql),
        Some("ice"),
        "db",
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    assert_eq!(source.needs().len(), 3);
    let facts = source
        .needs()
        .iter()
        .enumerate()
        .map(|(ordinal, need)| {
            if need.version().is_some() {
                selected_answer(need, ordinal as u32 + 1)
            } else {
                CatalogRelationFact::resolved(
                    need,
                    selection_table(need, ordinal as u32 + 1, None, D::Utf8),
                )
                .unwrap()
            }
        })
        .collect::<Vec<_>>();
    let source = source.continue_catalog(facts.into_boxed_slice()).unwrap();
    let captured = source.capture(&[]).unwrap();
    let (analyzed, _, _) =
        crate::analyzer::analyze(&query(sql), captured.provider(), "db").unwrap();
    assert_eq!(
        analyzed
            .output_columns
            .iter()
            .map(|field| field.value_type.data_type())
            .collect::<Vec<_>>(),
        vec![&D::Int32, &D::Int64, &D::Utf8, &D::Int32]
    );
    assert_ne!(
        analyzed.output_columns[0].column_id,
        analyzed.output_columns[3].column_id
    );
    for sql in [
        "SELECT orders.x FROM ice.db.orders FOR VERSION AS OF 10",
        "SELECT x FROM ice.db.orders FOR VERSION AS OF 10",
        "SELECT orders.* FROM ice.db.orders FOR VERSION AS OF 10",
    ] {
        let source = SqlCapturedCatalogPreparation::try_new(
            query(sql),
            Some("ice"),
            "db",
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        let fact = selected_answer(&source.needs()[0], 1);
        let source = source.continue_catalog(vec![fact].into()).unwrap();
        let captured = source.capture(&[]).unwrap();
        let (analyzed, _, _) =
            crate::analyzer::analyze(&query(sql), captured.provider(), "db").unwrap();
        assert_eq!(analyzed.output_columns[0].value_type.data_type(), &D::Int32);
    }
}

#[test]
fn saved_view_selection_uses_saved_context_and_preserves_semantic_refusal_layers() {
    use std::error::Error;
    for body in [
        "SELECT x FROM orders FOR VERSION AS OF 'backup'",
        "SELECT x FROM orders FOR VERSION AS OF 'missing'",
    ] {
        let sql = "SELECT x FROM v0";
        let source = SqlCapturedCatalogPreparation::try_new(
            query(sql),
            Some("caller"),
            "outside",
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        let mut view = definition(&source.needs()[0], body);
        Arc::make_mut(&mut view).default_catalog = "ice".into();
        Arc::make_mut(&mut view).default_database = "history".into();
        Arc::make_mut(&mut Arc::make_mut(&mut view).columns)[0].value_type =
            FunctionValueType::new(arrow::datatypes::DataType::Int32, false);
        let first = CatalogRelationFact::view(&source.needs()[0], view).unwrap();
        let source = source.continue_catalog(vec![first].into()).unwrap();
        let [need] = source.needs() else {
            panic!("selected saved dependency")
        };
        assert_eq!(need.relation().fqn(), "ice.history.orders");
        assert!(!need.allows_local_view_shadow());
        let fact = match need
            .resolve_time_travel(&selection_reference_facts())
            .unwrap()
        {
            CatalogTimeTravelResolution::Selected(binding) => {
                CatalogRelationFact::resolved_selected(
                    need,
                    selection_table(
                        need,
                        1,
                        Some(binding.snapshot_id()),
                        arrow::datatypes::DataType::Int32,
                    ),
                    &binding,
                )
                .unwrap()
            }
            CatalogTimeTravelResolution::Rejected(fact) => *fact,
        };
        let source = source.continue_catalog(vec![fact].into()).unwrap();
        let captured = source.capture(&[]).unwrap();
        let result = crate::analyzer::analyze(&query(sql), captured.provider(), "outside");
        if body.contains("'backup'") {
            assert_eq!(
                result.unwrap().0.output_columns[0].value_type.data_type(),
                &arrow::datatypes::DataType::Int32
            );
        } else {
            let error = result.unwrap_err();
            assert_eq!(
                error.kind(),
                crate::analyze_error::AnalyzeErrorKind::InvalidView
            );
            let inner = error
                .source()
                .unwrap()
                .downcast_ref::<crate::analyze_error::AnalyzeError>()
                .unwrap();
            assert_eq!(
                inner.kind(),
                crate::analyze_error::AnalyzeErrorKind::InvalidArgument
            );
            assert_eq!(error.stored_view_definition_source(), Some(body));
        }
    }
}

#[test]
fn invalid_saved_selection_is_diagnosed_before_unrequested_fact_lookup() {
    use std::error::Error;
    let result = answer(
        start(DEFAULT_COMPLETION_LIMITS),
        "SELECT x FROM orders FOR SYSTEM_TIME AS OF 'not-a-time'",
    );
    let Err(SqlCompileProgressError::Compile(SqlCompileError::Analyze(error))) = result else {
        panic!("invalid saved selection")
    };
    assert_eq!(
        error.kind(),
        crate::analyze_error::AnalyzeErrorKind::InvalidView
    );
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<crate::analyze_error::AnalyzeError>()
            .unwrap()
            .kind(),
        crate::analyze_error::AnalyzeErrorKind::InvalidLiteral
    );
}

#[test]
fn reused_selection_refusal_keeps_the_current_saved_body_coordinates() {
    use std::error::Error;
    let sql = "SELECT v0.x FROM v0 JOIN ice.history.orders FOR VERSION AS OF 'missing' z ON 1=1";
    let body = "SELECT x FROM orders FOR VERSION AS OF 'missing'";
    let source = SqlCapturedCatalogPreparation::try_new(
        query(sql),
        Some("caller"),
        "outside",
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    assert_eq!(source.needs().len(), 2);
    let mut view = definition(&source.needs()[0], body);
    Arc::make_mut(&mut view).default_catalog = "ice".into();
    Arc::make_mut(&mut view).default_database = "history".into();
    let first = CatalogRelationFact::view(&source.needs()[0], view).unwrap();
    let CatalogTimeTravelResolution::Rejected(refusal) = source.needs()[1]
        .resolve_time_travel(&selection_reference_facts())
        .unwrap()
    else {
        panic!("missing ref")
    };
    let source = source
        .continue_catalog(vec![first, *refusal].into())
        .unwrap();
    assert!(source.needs().is_empty());
    let captured = source.capture(&[]).unwrap();
    let error = crate::analyzer::analyze(&query(sql), captured.provider(), "outside").unwrap_err();
    assert_eq!(
        error.kind(),
        crate::analyze_error::AnalyzeErrorKind::InvalidView
    );
    assert_eq!(error.stored_view_definition_source(), Some(body));
    let inner = error
        .source()
        .unwrap()
        .downcast_ref::<crate::analyze_error::AnalyzeError>()
        .unwrap();
    let start = body.find("'missing'").unwrap();
    assert_eq!(
        inner.span(),
        Some(novarocks_parser::Span::new(
            start,
            start + "'missing'".len()
        ))
    );
}

#[test]
fn unsupported_version_consumers_never_silently_use_current_or_cte_facts() {
    struct CurrentOnly(std::cell::Cell<usize>);
    impl crate::catalog::PlannerTableProvider for CurrentOnly {
        fn resolve_table_for_analysis(
            &self,
            _: Option<&str>,
            _: &str,
            _: &str,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            self.0.set(self.0.get() + 1);
            Err("a Current lookup must not answer an explicit version".into())
        }
    }
    let catalog = CurrentOnly(std::cell::Cell::new(0));
    for sql in [
        "SELECT x FROM orders FOR VERSION AS OF 10",
        "WITH orders AS (SELECT 1 AS x) SELECT x FROM orders FOR VERSION AS OF 10",
    ] {
        let error = crate::analyzer::analyze(&query(sql), &catalog, "db").unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::UnsupportedQueryShape
        );
    }
    assert_eq!(catalog.0.get(), 0);
    let result = SqlCapturedCatalogPreparation::try_new(
        query("SELECT * FROM ice.db.orders$snapshots FOR VERSION AS OF 10"),
        Some("ice"),
        "db",
        SqlCompileControl::unbounded(),
    );
    assert!(
        matches!(result, Err(SqlCompileError::Analyze(error)) if error.kind() == crate::analyze_error::AnalyzeErrorKind::UnsupportedQueryShape)
    );
}
