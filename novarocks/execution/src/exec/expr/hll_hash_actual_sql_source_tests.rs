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
//! Actual corpus CREATE TABLE facts and the original HLL_HASH call source.
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

pub(super) fn hll_sql_source(
    sql: &str,
    columns: &[(&str, DataType)],
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    hll_sql_source_with_semantics(
        sql,
        columns,
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
    )
}
fn hll_sql_source_with_semantics(
    sql: &str,
    columns: &[(&str, DataType)],
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
                        assert_eq!(catalog_table(&resolved).columns.len(), columns.len());
                        for (actual, (name, ty)) in
                            catalog_table(&resolved).columns.iter().zip(columns)
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
                                label: "hll_original_nullable_fixture".into(),
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
            other => panic!("unexpected original HLL fixture needs: {other:?}"),
        };
        progress = SqlCompiler::finish(compilation, facts, &control).unwrap();
    }
}

const TYPEDESC_SQL: &str = "SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_preaggregation') */ grp, CAST(avg(v) AS DECIMAL(18,4)) AS avg_v, ndv(k) AS ndv_k, hll_union_agg(hll_hash(k)) AS hll_k, CAST(percentile_approx(d,0.5) AS DECIMAL(18,4)) AS p50_d FROM fixture.agg_state_typedesc_contract GROUP BY grp ORDER BY grp";
const HLL_SQL: &str = "select hll_union(hll_hash(c1)) from fixture.t1";
fn sources() -> Vec<novarocks_sql::compiler::SqlAuthoredPhysicalPlan> {
    vec![
        hll_sql_source(
            TYPEDESC_SQL,
            &[
                ("grp", DataType::Int32),
                ("k", DataType::Int32),
                ("v", DataType::Int64),
                ("d", DataType::Float64),
            ],
        ),
        hll_sql_source(HLL_SQL, &[("c1", DataType::Int32), ("c2", DataType::Int32)]),
    ]
}
#[test]
fn hll_hash_actual_original_required_sql_full_signature_source() {
    use novarocks_physical_plan::{
        ExprKind, FunctionArgumentType, PhysicalCallDefinition, StaticFunctionArgument,
    };
    for source in sources() {
        let mut count = 0;
        for fragment in source.plan().fragments().values() {
            for (id, node) in fragment.expressions().iter() {
                let ExprKind::FunctionCall { function, args } = &node.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/hll_hash/v1" {
                    continue;
                }
                assert_eq!(args.len(), 1);
                assert_eq!(function.argument_types.len(), 1);
                let request = fragment
                    .call_requests()
                    .get(PhysicalCallDefinition::Expression(*id))
                    .unwrap();
                assert_eq!(request.logical_argument_count, 1);
                assert_eq!(request.arguments.len(), 1);
                let FunctionArgumentType::Value(selected) = &function.argument_types[0] else {
                    panic!("original value channel")
                };
                let StaticFunctionArgument::Value {
                    value_type: actual, ..
                } = &request.arguments[0]
                else {
                    panic!("original source channel")
                };
                assert_eq!(actual, selected);
                // The original analyzer inserts this Cast before binding HLL_HASH.
                assert_eq!(selected.data_type().clone(), DataType::Utf8);
                let argument = fragment
                    .expressions()
                    .get(args[0])
                    .expect("actual HLL argument");
                let ExprKind::Cast { expr, target, .. } = &argument.kind else {
                    panic!("original HLL analyzer-authored VARCHAR cast must remain present")
                };
                assert_eq!(target, &DataType::Utf8);
                assert_eq!(argument.ty, *selected);
                let input = fragment
                    .expressions()
                    .get(*expr)
                    .expect("actual HLL cast source");
                assert_eq!(input.ty.data_type().clone(), DataType::Int32);
                assert!(selected.nullable());
                assert_eq!(
                    selected.logical_type(),
                    novarocks_type_contract::ValueLogicalType::Physical
                );
                assert_eq!(node.ty.data_type().clone(), DataType::Binary);
                eprintln!(
                    "HLL_HASH actual original source fragment={:?} id={id:?} function={function:?} request={request:?} argument={:?} output={:?}",
                    fragment.id(),
                    fragment.expressions().get(args[0]),
                    node.ty
                );
                count += 1;
            }
        }
        assert!(
            count > 0,
            "actual corpus authored HLL_HASH root must remain present"
        );
    }
}
#[test]
fn hll_hash_actual_original_required_sql_complete_compilation() {
    for source in sources() {
        let results = super::filter_conjunction_actual_sql_compiler_tests::compiler_results(
            &source,
            &super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue(),
        );
        assert!(!results.is_empty());
        for (fragment, program) in results {
            program.unwrap_or_else(|error| {
                panic!("complete actual HLL source fragment {fragment:?}: {error:?}")
            });
        }
    }
}
