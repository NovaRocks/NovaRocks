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
//! Actual original CREATE TABLE facts and approximate percentile call authors.
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

pub(super) fn approx_sql_source(sql: &str) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    approx_sql_source_with_semantics(sql, novarocks_sql::sql_mode::SqlSemanticSettings::default())
}
fn approx_sql_source_with_semantics(
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
                            ("id", DataType::Int32),
                            ("v", DataType::Int32),
                            ("w", DataType::Int32),
                            ("d", DataType::Date32),
                            (
                                "dt",
                                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                            ),
                            ("dbl", DataType::Float64),
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
                        assert_eq!(catalog_table(&resolved).columns.len(), 6);
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
                                label: "ndv_original_nullable_fixture".into(),
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
            other => panic!("unexpected original approximate percentile fixture needs: {other:?}"),
        };
        progress = SqlCompiler::finish(compilation, facts, &control).unwrap();
    }
}

fn check_actual_approx() {
    use novarocks_physical_plan::{
        FunctionArgumentType, NodeKind, PhysicalCallDefinition, PhysicalCallSite,
        StaticFunctionArgument,
    };
    use novarocks_type_contract::FunctionValueType;
    let statements = [
        "SELECT CAST(percentile_approx(v, 0.5) AS INT) AS approx50, CAST(percentile_approx(v, 0.9, 2048) AS INT) AS approx90, percentile_approx(v, array<double>[0.25, 0.5, 0.75], 2048) AS approx_quartiles FROM fixture.t_agg_percentile_semantics",
        "SELECT CAST(percentile_approx_weighted(v, w, 0.5, 2048) AS INT) AS weighted50, CAST(percentile_approx_weighted(v, w, 0.9, 2048) AS INT) AS weighted90, percentile_approx_weighted(v, w, array<double>[0.25, 0.5, 0.75], 2048) AS weighted_quartiles FROM fixture.t_agg_percentile_semantics",
    ];
    for sql in statements {
        let source = approx_sql_source(sql);
        let mut seen = 0;
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                let NodeKind::Aggregate { calls, .. } = &node.kind else {
                    continue;
                };
                for (ordinal, call) in calls.iter().enumerate() {
                    let name = call.binding.function.function_id.as_str();
                    if !matches!(
                        name,
                        "builtin.aggregate/percentile_approx/v1"
                            | "builtin.aggregate/percentile_approx_weighted/v1"
                    ) {
                        continue;
                    }
                    let site = PhysicalCallSite::Aggregate {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    };
                    let request = fragment
                        .call_requests()
                        .get(PhysicalCallDefinition::Relational(site))
                        .unwrap();
                    assert_eq!(
                        request.logical_argument_count,
                        call.binding.function.argument_types.len()
                    );
                    for (actual, selected) in request
                        .arguments
                        .iter()
                        .zip(call.binding.function.argument_types.iter())
                    {
                        let (
                            StaticFunctionArgument::Value {
                                value_type: actual, ..
                            },
                            FunctionArgumentType::Value(selected),
                        ) = (actual, selected)
                        else {
                            panic!("actual Value channel")
                        };
                        assert_eq!(actual, selected);
                    }
                    let role = if name == "builtin.aggregate/percentile_approx/v1" {
                        1
                    } else {
                        2
                    };
                    let FunctionArgumentType::Value(rate) =
                        &call.binding.function.argument_types[role]
                    else {
                        panic!("actual rate")
                    };
                    let expected = if matches!(rate.data_type(), DataType::List(_)) {
                        DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
                    } else {
                        DataType::Float64
                    };
                    assert_eq!(
                        call.binding.function.result_type,
                        FunctionValueType::new(expected, true)
                    );
                    assert_eq!(
                        call.binding.intermediate_type,
                        FunctionValueType::new(DataType::Binary, true)
                    );
                    assert!(
                        matches!(
                            rate.data_type(),
                            DataType::Decimal128(..) | DataType::List(_)
                        ),
                        "native statements original authored rates"
                    );
                    eprintln!(
                        "approximate actual SQL mode=compiled source={site:?} phase={:?} logical={:?} rate={rate:?} output={:?} state={:?}",
                        call.binding.phase,
                        request.arguments,
                        call.binding.function.result_type,
                        call.binding.intermediate_type
                    );
                    seen += 1;
                }
            }
        }
        assert!(
            seen >= 3,
            "all original required three quantile projections"
        );
    }
}

#[test]
fn approximate_percentile_actual_candidate_sql_source() {
    check_actual_approx()
}
