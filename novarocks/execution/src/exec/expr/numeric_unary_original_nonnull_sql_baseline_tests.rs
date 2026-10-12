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
//! Independent original SQL completion metadata. This does not call a new
//! nullable helper, clone/re-lower a physical graph, or claim runtime execution.
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Schema};
use novarocks_physical_plan::{
    ExactInputVersion, PredicateGuaranteeKind, ProviderColumnReference, ProviderReadReference,
};
use novarocks_spi::connector::read_stack::{ConnectorReadBinding, ConnectorReadWorkSource};
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
use novarocks_physical_plan::{
    ExprKind, PipelineDopDomain, PlanVersionId, ScanReadBudget, UnaryOperator,
};
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
    SqlCompileProgress, SqlCompiler, SqlFinalPlanCompileRequest, SqlPlanningEnvironment,
    SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog, noop_constant_evaluator,
};

pub(super) fn sql_source(
    sql: &str,
    dtype: DataType,
    emission_mode: novarocks_sql::compiler::SqlPhysicalEmissionMode,
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    sql_source_with_semantics(
        sql,
        dtype,
        emission_mode,
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
    )
}
pub(super) fn sql_source_with_semantics(
    sql: &str,
    dtype: DataType,
    emission_mode: novarocks_sql::compiler::SqlPhysicalEmissionMode,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    sql_source_with_explicit_single_field(
        sql,
        Field::new("k", dtype, false),
        emission_mode,
        sql_semantics,
        None,
    )
}
/// A fixture source fact is explicit; it is never inferred from test data.
pub(super) fn sql_source_with_single_field(
    sql: &str,
    field: Field,
    emission_mode: novarocks_sql::compiler::SqlPhysicalEmissionMode,
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    sql_source_with_explicit_single_field(
        sql,
        field,
        emission_mode,
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        None,
    )
}
/// Same SQL semantics, with a real admitted optimizer setting preserving a
/// Core ScanResidual. This is not the original native default plan/site.
pub(super) fn sql_source_with_single_field_and_core_residuals(
    sql: &str,
    field: Field,
    emission_mode: novarocks_sql::compiler::SqlPhysicalEmissionMode,
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    sql_source_with_explicit_single_field(
        sql,
        field,
        emission_mode,
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        Some(false),
    )
}
fn sql_source_with_explicit_single_field(
    sql: &str,
    field: Field,
    emission_mode: novarocks_sql::compiler::SqlPhysicalEmissionMode,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    static_predicate_offer: Option<bool>,
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    sql_source_with_explicit_fields(
        sql,
        &[field],
        emission_mode,
        sql_semantics,
        static_predicate_offer,
    )
}
/// Explicit original relation declarations for multi-column SQL fixtures.
/// This shares the one original fact/resume/provider author, never a second plan.
pub(super) fn sql_source_with_declared_fields(
    sql: &str,
    fields: &[Field],
    emission_mode: novarocks_sql::compiler::SqlPhysicalEmissionMode,
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    sql_source_with_explicit_fields(
        sql,
        fields,
        emission_mode,
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        None,
    )
}
fn sql_source_with_explicit_fields(
    sql: &str,
    fields: &[Field],
    emission_mode: novarocks_sql::compiler::SqlPhysicalEmissionMode,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    static_predicate_offer: Option<bool>,
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    try_sql_source_with_explicit_fields(
        sql,
        fields,
        emission_mode,
        sql_semantics,
        static_predicate_offer,
    )
    .unwrap()
}

pub(super) fn try_sql_source_with_declared_fields(
    sql: &str,
    fields: &[Field],
    emission_mode: novarocks_sql::compiler::SqlPhysicalEmissionMode,
) -> Result<
    novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
    novarocks_sql::compiler::SqlCompileProgressError,
> {
    try_sql_source_with_explicit_fields(
        sql,
        fields,
        emission_mode,
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        None,
    )
}

fn try_sql_source_with_explicit_fields(
    sql: &str,
    fields: &[Field],
    emission_mode: novarocks_sql::compiler::SqlPhysicalEmissionMode,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    static_predicate_offer: Option<bool>,
) -> Result<
    novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
    novarocks_sql::compiler::SqlCompileProgressError,
> {
    let control = SqlCompileControl::unbounded();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([91; 16]).unwrap(),
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics,
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                enable_common_subexpr_reuse: Some(false),
                enable_connector_static_predicate_pushdown: static_predicate_offer,
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
        emission_mode,
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
    let mut progress = SqlCompiler::start(request.try_into_completion().unwrap(), &control)?;
    let mut bindings = SqlTableBindingAllocator::new_unique().unwrap();
    loop {
        let compilation = match progress {
            SqlCompileProgress::Complete(completed) => return Ok(completed.into_plan()),
            SqlCompileProgress::Incomplete(compilation) => compilation,
        };
        let facts = match compilation.needs() {
            SqlNeedBatch::CatalogRelations(needs) => SqlFactBatch::CatalogRelations(
                needs
                    .iter()
                    .map(|need| {
                        let relation = need.relation();
                        let schema = Arc::new(Schema::new(fields.to_vec()));
                        let resolved = materialize_connector_read_table(ConnectorReadTableFacts {
                            catalog: relation.catalog.clone(),
                            namespace: relation.namespace.clone(),
                            table: relation.table.clone(),
                            columns: fields
                                .iter()
                                .map(|field| ColumnDef {
                                    name: field.name().clone(),
                                    data_type: field.data_type().clone(),
                                    nullable: field.is_nullable(),
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
                        for (ordinal, field) in fields.iter().enumerate() {
                            assert_eq!(
                                catalog_table(&resolved).columns[ordinal].nullable,
                                field.is_nullable()
                            );
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
                                label: "original_nonnull_fixture".into(),
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
            other => panic!("unexpected original nonnull fixture needs: {other:?}"),
        };
        progress = SqlCompiler::finish(compilation, facts, &control)?;
    }
}

fn assert_original_same_owner(
    port: &novarocks_physical_plan::ResultPort,
    source: &novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
) {
    let fragment = &source.plan().fragments()[&port.fragment];
    let node = &fragment.nodes()[&port.output.node];
    assert_eq!(node.output, port.output);
    assert_eq!(port.fields.len(), port.output.columns.len());
    for (ordinal, field) in port.fields.iter().enumerate() {
        assert_eq!(field.value, port.output.columns[ordinal]);
        assert_eq!(field.ty, fragment.values()[&field.value].ty);
    }
}
#[test]
fn numeric_unary_original_nonnull_sql_baseline_three_narrow_widths_remain_nonnullable() {
    for (ty, min, dtype) in [
        ("TINYINT", "-128", DataType::Int8),
        ("SMALLINT", "-32768", DataType::Int16),
        ("INT", "-2147483648", DataType::Int32),
    ] {
        let _ = (ty, min); // Values are runtime inputs, never used to manufacture plan types.
        let source = sql_source(
            "SELECT -k AS original_negated FROM fixture",
            dtype.clone(),
            novarocks_sql::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
        );
        let port = source.plan().result_port().unwrap();
        assert_original_same_owner(port, &source);
        assert_eq!(port.fields.len(), 1);
        assert_eq!(port.fields[0].alias.as_deref(), Some("original_negated"));
        assert_eq!(port.fields[0].ty.data_type, dtype);
        assert!(!port.fields[0].ty.nullable);
        let mut actual_minus = 0;
        for fragment in source.plan().fragments().values() {
            for (_, expr) in fragment.expressions().iter() {
                if matches!(
                    expr.kind,
                    ExprKind::Unary {
                        op: UnaryOperator::Minus,
                        ..
                    }
                ) {
                    actual_minus += 1;
                    assert_eq!(expr.ty.data_type, dtype);
                    assert!(!expr.ty.nullable);
                }
            }
        }
        assert!(
            actual_minus > 0,
            "fixture must retain the actual original Minus definition"
        );
    }
}
#[test]
fn numeric_unary_original_nonnull_sql_baseline_ordered_labels_and_values_are_actual_root() {
    let source = sql_source(
        "SELECT -k AS first_alias, k AS original_source, -k AS repeated_alias FROM fixture",
        DataType::Int8,
        novarocks_sql::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
    );
    let port = source.plan().result_port().unwrap();
    assert_original_same_owner(port, &source);
    assert_eq!(port.fields.len(), 3);
    for (field, alias) in
        port.fields
            .iter()
            .zip(["first_alias", "original_source", "repeated_alias"])
    {
        assert_eq!(field.alias.as_deref(), Some(alias));
        assert_eq!(field.ty.data_type, DataType::Int8);
        assert!(!field.ty.nullable);
    }
    // Whether optimization reused an expression/ValueId is the actual plan's
    // choice. Each ordered output occurrence above is checked individually.
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

fn provider_contract(need: &ProviderReadNeed) -> ProviderReadStaticContract {
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
        work_source: ConnectorReadWorkSource::RuntimeSplits,
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
