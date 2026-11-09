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
//! Actual SQL author probe for the native NDV HAVING first blocker.
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Schema};
use novarocks_physical_plan::{
    ExactInputVersion, PredicateGuaranteeKind, ProviderColumnReference, ProviderReadReference,
};
use novarocks_spi::connector::read_stack::ConnectorReadBinding;
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadRelationPayload, ConnectorReadSelector,
    ConnectorTablePlanningFacts,
};
use novarocks_sql::binding::SqlTableBindingAllocator;
use novarocks_sql::compiler::{
    CatalogRelationFact, ProviderReadColumnFact, ProviderReadFact, ProviderReadLimitFact,
    ProviderReadNeed, ProviderReadPredicateFact, ProviderReadProperties,
    ProviderReadRequestBinding, ProviderReadStaticContract, SqlFactBatch, SqlNeedBatch,
    StatisticsFact,
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

pub(super) fn ndv_sql_source(sql: &str) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    ndv_sql_source_with_semantics(sql, novarocks_sql::sql_mode::SqlSemanticSettings::default())
}
fn ndv_sql_source_with_semantics(
    sql: &str,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    sql_source_with_columns_and_semantics(
        sql,
        sql_semantics,
        &[
            ("k", DataType::Int32),
            ("v", DataType::Int32),
            ("s", DataType::Utf8),
        ],
    )
}

pub(super) fn sql_source_with_columns(
    sql: &str,
    columns: &[(&str, DataType)],
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    sql_source_with_columns_and_semantics(
        sql,
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        columns,
    )
}

fn sql_source_with_columns_and_semantics(
    sql: &str,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    columns: &[(&str, DataType)],
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
                        ProviderReadFact::negotiated(need, provider_contract(need)).unwrap()
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            other => panic!("unexpected original NDV fixture needs: {other:?}"),
        };
        progress = SqlCompiler::finish(compilation, facts, &control).unwrap();
    }
}

const HAVING: &str = "SELECT k FROM fixture.ndv_null_contract GROUP BY k\nHAVING ndv(v) = 0 AND approx_count_distinct(v) = 0 ORDER BY k;";
const EMPTY_WHERE: &str = "SELECT ndv(v) AS ndv_empty, approx_count_distinct(v) AS approx_empty,\n       count(DISTINCT v) AS exact_empty\nFROM fixture.ndv_null_contract WHERE k = 999;";
const NULL_WHERE: &str = "SELECT ndv(v) AS ndv_null, approx_count_distinct(v) AS approx_null,\n       count(DISTINCT v) AS exact_null\nFROM fixture.ndv_null_contract WHERE k = 1;";

fn source_probe() {
    use novarocks_physical_plan::NodeKind;
    for (query, sql) in [(3, EMPTY_WHERE), (4, NULL_WHERE), (7, HAVING)] {
        let source = ndv_sql_source(sql);
        let mut filters = 0;
        let mut multi_predicate_filters = 0;
        let mut aggregate_calls = 0;
        for (fragment_id, fragment) in source.plan().fragments() {
            for node in fragment.nodes().values() {
                match &node.kind {
                    NodeKind::Filter { predicates } => {
                        filters += 1;
                        eprintln!(
                            "NDV actual SQL query={query} mode=compiled fragment={:?} filter={:?} inputs={:?} predicate_count={} output={:?}",
                            fragment_id,
                            node.id,
                            node.inputs,
                            predicates.len(),
                            node.output
                        );
                        assert_eq!(node.inputs.len(), 1);
                        assert!(!predicates.is_empty());
                        let input = fragment.nodes().get(&node.inputs[0]).unwrap();
                        assert_eq!(node.output.columns, input.output.columns);
                        for (ordinal, id) in predicates.iter().enumerate() {
                            let expression = fragment.expressions().get(*id).unwrap();
                            assert_eq!(expression.ty.data_type().clone(), DataType::Boolean);
                            eprintln!(
                                "NDV actual FilterPredicate ordinal={ordinal} id={id:?} expression={expression:?}"
                            );
                        }
                        if predicates.len() > 1 {
                            multi_predicate_filters += 1;
                            assert_eq!(query, 7);
                            assert_eq!(predicates.len(), 2);
                        }
                    }
                    NodeKind::Aggregate { calls, .. } => {
                        aggregate_calls += calls.len();
                        for (ordinal, call) in calls.iter().enumerate() {
                            eprintln!(
                                "NDV actual aggregate query={query} mode=compiled node={:?} call={ordinal} authored={call:?}",
                                node.id
                            );
                        }
                    }
                    NodeKind::Scan {
                        relation,
                        residuals,
                        ..
                    } => {
                        eprintln!(
                            "NDV actual scan query={query} mode=compiled node={:?} relation={relation:?} residuals={residuals:?}",
                            node.id
                        );
                    }
                    _ => {}
                }
            }
            // This is diagnostic observation of the already-emitted original
            // arena. It performs no lowering, decoder, rewriter or evaluation.
            for (id, expression) in fragment.expressions().iter() {
                eprintln!(
                    "NDV actual original definition query={query} mode=compiled id={id:?} expression={expression:?}"
                );
            }
            for (id, value) in fragment.values() {
                eprintln!(
                    "NDV actual original value query={query} mode=compiled id={id:?} value={value:?}"
                );
            }
        }
        assert!(aggregate_calls > 0);
        if query == 7 {
            assert!(filters > 0);
            assert_eq!(
                multi_predicate_filters, 1,
                "the original HAVING author retains two conjunct roots on one Filter"
            );
        } else {
            assert_eq!(multi_predicate_filters, 0);
        }
    }
}

#[test]
fn ndv_actual_filter_candidate_sql_source() {
    source_probe();
}
fn connector_binding() -> ConnectorReadBinding {
    let provider_id = ConnectorProviderId::parse("iceberg").expect("provider id");
    let instance_id = ConnectorInstanceId::parse("lakehouse").expect("instance id");
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id,
            instance_id: instance_id.clone(),
        },
        CatalogHandle::new(instance_id, CatalogVersion::from_bytes([3; 32])),
    )
}

fn encoded(
    binding: &ConnectorReadBinding,
    category: ConnectorCodecCategory,
) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            category,
            ConnectorCodecRevision::try_new(1).expect("codec revision"),
        ),
        vec![category as u8 + 1].into(),
    )
}

pub(super) fn provider_contract(need: &ProviderReadNeed) -> ProviderReadStaticContract {
    let binding = connector_binding();
    ProviderReadStaticContract {
        sql_binding: need.binding(),
        request: ProviderReadRequestBinding::from_need(need),
        read: ProviderReadReference {
            binding: binding.clone(),
            input_version: ExactInputVersion::try_new([9]).expect("input version"),
            relation: ConnectorReadRelationPayload::new(
                need.relation().relation_kind(),
                encoded(&binding, ConnectorCodecCategory::ReadTable),
                encoded(&binding, ConnectorCodecCategory::ReadView),
            ),
        },
        metadata_kind: matches!(
            need.relation(),
            novarocks_sql::compiler::ProviderReadRelationNeed::Metadata { .. }
        )
        .then(|| {
            novarocks_spi::connector::read_stack::ConnectorReadMetadataKind::try_new(
                "$fixture-metadata",
            )
            .expect("fixture metadata kind")
        }),

        selection_digest: [8; 32],
        schema: need
            .columns()
            .iter()
            .map(|column| {
                ProviderReadColumnFact::new(
                    column.ordinal(),
                    ProviderColumnReference {
                        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn),
                    },
                    column.engine_type().clone(),
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        predicates: need
            .predicates()
            .iter()
            .map(|predicate| {
                ProviderReadPredicateFact::new(
                    predicate.occurrence(),
                    PredicateGuaranteeKind::Exact,
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        limit: need.limit().map_or(
            ProviderReadLimitFact::NotRequested,
            ProviderReadLimitFact::Exact,
        ),
        provided_properties: ProviderReadProperties::unconstrained(),
        coverage_evidence: Box::default(),
    }
}
