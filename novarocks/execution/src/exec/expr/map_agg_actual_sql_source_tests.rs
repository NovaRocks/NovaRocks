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
//! Actual corpus CREATE TABLE facts and the original MAP_AGG call source.
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Schema};
use novarocks_spi::connector::{ConnectorReadSelector, ConnectorTablePlanningFacts};
use novarocks_sql::binding::SqlTableBindingAllocator;
use novarocks_sql::compiler::{
    CatalogRelationFact, ProviderReadFact, SqlFactBatch, SqlNeedBatch, StatisticsFact,
};
use novarocks_sql::planning::catalog::{
    ConnectorReadTableFacts, catalog_table, materialize_connector_read_table,
};
use novarocks_sql::planning::dml::DmlStatisticsEvidence;
use novarocks_types::schema::ColumnDef;
use std::sync::Arc;

use novarocks_functions::ConstantPolicy;
use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId, ScanReadBudget};
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
    SqlCompileProgress, SqlCompiler, SqlFinalPlanCompileRequest, SqlPlanningEnvironment,
    SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog, noop_constant_evaluator,
};

pub(super) fn map_agg_sql_source(sql: &str) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    map_agg_sql_source_with_semantics(sql, novarocks_sql::sql_mode::SqlSemanticSettings::default())
}
fn map_agg_sql_source_with_semantics(
    sql: &str,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    let control = SqlCompileControl::unbounded();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([91; 16]).unwrap(),
        SqlStatementInput::parsed_query(crate::exec::expr::test_support::parse_query(sql)),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics,
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
    let mut progress =
        SqlCompiler::start(request.try_into_completion().unwrap(), &control).unwrap();
    let mut bindings = SqlTableBindingAllocator::new_unique().unwrap();
    loop {
        let compilation = match progress {
            SqlCompileProgress::Complete(completed) => return completed.into_plan(),
            SqlCompileProgress::Incomplete(compilation) => compilation,
        };
        let facts = match compilation.needs() {
            SqlNeedBatch::CatalogRelations(needs) => SqlFactBatch::CatalogRelations(
                needs
                    .iter()
                    .map(|need| {
                        let relation = need.relation();
                        // These are the original CREATE TABLE facts, not inferred
                        // from the query projection or manufactured physical nodes.
                        let columns = [
                            ("g", DataType::Int32),
                            ("k", DataType::Utf8),
                            ("v", DataType::Int32),
                        ];
                        let schema = Arc::new(Schema::new(
                            columns
                                .iter()
                                .map(|(name, ty)| Field::new(*name, ty.clone(), true))
                                .collect::<Vec<_>>(),
                        ));
                        let resolved = materialize_connector_read_table(ConnectorReadTableFacts {
                            catalog: relation.catalog.clone(),
                            namespace: relation.namespace.clone(),
                            table: relation.table.clone(),
                            columns: columns
                                .iter()
                                .map(|(name, ty)| ColumnDef {
                                    name: (*name).into(),
                                    data_type: ty.clone(),
                                    nullable: true,
                                    write_default: None,
                                    logical_type: None,
                                })
                                .collect(),
                            iceberg_row_lineage_metadata_columns: Vec::new(),
                            schema,
                            binding: bindings.allocate().unwrap(),
                            selector: ConnectorReadSelector::Current,
                            planning_facts: ConnectorTablePlanningFacts::empty(),
                        })
                        .unwrap()
                        .into_resolved_table();
                        assert_eq!(catalog_table(&resolved).columns.len(), 3);
                        for (actual, (name, ty)) in
                            catalog_table(&resolved).columns.iter().zip(&columns)
                        {
                            assert_eq!(actual.name, *name);
                            assert_eq!(&actual.data_type, ty);
                            assert!(actual.nullable);
                        }
                        CatalogRelationFact::resolved(need, resolved).unwrap()
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            SqlNeedBatch::Statistics(needs) => SqlFactBatch::Statistics(
                needs
                    .iter()
                    .map(|need| {
                        StatisticsFact::try_new(
                            need,
                            need.metrics().to_vec(),
                            DmlStatisticsEvidence::Missing {
                                binding: need.binding(),
                                label: "map_agg_original_nullable_fixture".into(),
                                reason: "fixture provides no statistics".into(),
                            },
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            SqlNeedBatch::ProviderReads(needs) => SqlFactBatch::ProviderReads(
                needs
                    .iter()
                    .map(|need| {
                        ProviderReadFact::negotiated(
                            need,
                            super::ndv_filter_actual_sql_source_tests::provider_contract(need),
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            other => panic!("unexpected original MAP_AGG fixture needs: {other:?}"),
        };
        progress = SqlCompiler::finish(compilation, facts, &control).unwrap();
    }
}

const SQL: &str = "SELECT g, ELEMENT_AT(MAP_AGG(k, v), 'a') AS v_a, ELEMENT_AT(MAP_AGG(k, v), 'b') AS v_b, CARDINALITY(MAP_AGG(k, v)) AS map_size FROM fixture.t_agg_map_agg_extract GROUP BY g ORDER BY g";
#[test]
fn map_agg_actual_original_required_sql_full_signature_source() {
    use novarocks_physical_plan::{
        NodeKind, PhysicalCallDefinition, PhysicalCallSite, StaticFunctionArgument,
    };
    let source = map_agg_sql_source(SQL);
    let mut calls_seen = 0;
    for fragment in source.plan().fragments().values() {
        for (node_id, node) in fragment.nodes() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, call) in calls.iter().enumerate() {
                if call.binding.function.function_id.as_str() != "builtin.aggregate/map_agg/v1" {
                    continue;
                }
                let request = fragment
                    .call_requests()
                    .get(PhysicalCallDefinition::Relational(
                        PhysicalCallSite::Aggregate {
                            node: *node_id,
                            call: ordinal as u32,
                        },
                    ))
                    .unwrap();
                assert_eq!(
                    call.binding.function.overload.as_str(),
                    "builtin.aggregate/map_agg/derived-v1"
                );
                assert_eq!(call.binding.logical_argument_count, 2);
                assert_eq!(request.logical_argument_count, 2);
                assert_eq!(request.arguments.len(), 2);
                let types = request
                    .arguments
                    .iter()
                    .map(|argument| match argument {
                        StaticFunctionArgument::Value { value_type, .. } => value_type,
                        _ => panic!("actual MAP_AGG logical arguments are values"),
                    })
                    .collect::<Vec<_>>();
                assert_eq!(types[0].data_type().clone(), DataType::Utf8);
                assert_eq!(types[1].data_type().clone(), DataType::Int32);
                assert!(types.iter().all(|ty| ty.nullable()));
                assert!(matches!(
                    call.binding.function.result_type.data_type(),
                    DataType::Map(_, false)
                ));
                assert_eq!(
                    call.binding.intermediate_type,
                    call.binding.function.result_type
                );
                eprintln!(
                    "MAP_AGG actual original fragment={:?} node={node_id:?} call={ordinal} binding={:?} request={request:?}",
                    fragment.id(),
                    call.binding
                );
                calls_seen += 1;
            }
        }
    }
    assert!(
        calls_seen > 0,
        "actual original aggregate roots remain published"
    );
}
#[test]
fn map_agg_actual_original_required_sql_complete_compilation() {
    // Permanent intended equality: actual Missing before, whole-source success after.
    let source = map_agg_sql_source(SQL);
    let results = super::filter_conjunction_actual_sql_compiler_tests::compiler_results(
        &source,
        &super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue(),
    );
    assert!(!results.is_empty());
    for (fragment, program) in results {
        program.unwrap_or_else(|error| {
            panic!("complete actual MAP_AGG source fragment {fragment:?}: {error:?}")
        });
    }
}
