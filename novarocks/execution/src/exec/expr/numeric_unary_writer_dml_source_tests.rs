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
//! Actual public SQL DML optimization, Writer statistics and final publication.
//! This fixture never constructs a replacement PhysicalPlan or state route.
use arrow::datatypes::DataType;
use novarocks_connector_iceberg_functions::{
    ICEBERG_THETA_AGGREGATE_NAME, iceberg_theta_registration,
};
use novarocks_functions::{
    AggregateBindingSelection, EngineFunctionCatalog, EngineFunctionCatalogBuilder,
    FunctionArgument, FunctionBindingRequest, FunctionBindingSelection, FunctionKind,
    FunctionResultType,
};
use novarocks_physical_plan::{EdgeKind, NodeKind, PipelineDopDomain, PlanVersionId};
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorProviderId,
    ConnectorWriteFieldToken, StatisticsArtifactIdentity, StatisticsRequiredAggregation,
    StatisticsScanColumn,
};
use novarocks_sql::binding::SqlTableBindingAllocator;
use novarocks_sql::compiler::{
    RootDistributionRequirement, SessionOptimizerSettings, SqlAnalyzeRequest, SqlCompileControl,
    SqlCompileIntent, SqlCompiler, SqlOptimizeRequest, SqlPlannerTableSnapshot,
    SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
};
use novarocks_sql::planning::catalog::PlannerMemoryCatalog;
use novarocks_sql::planning::dml::{
    ConnectorWriteInputBinding, DmlFinalPlanContext, DmlFinalWritePlanContext,
    DmlFinalizedProviderReadSet, DmlFinalizedWriteTarget, DmlFinalizedWriteTargetSet,
    DmlStatisticsSnapshot, DmlWritePlanInput, DmlWriteSinkMode, DmlWriteTarget,
    DmlWriteTargetField, compile_final_connector_write_plan,
};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use novarocks_types::schema::ColumnDef;

fn source(
    with_statistics: bool,
    dtype: DataType,
    sql_type: &str,
) -> (
    novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
    EngineFunctionCatalog,
) {
    let mut functions = EngineFunctionCatalogBuilder::new();
    novarocks_functions::builtin::catalogue::contribute_builtin_functions(&mut functions).unwrap();
    functions
        .register(iceberg_theta_registration().unwrap().definition().clone())
        .unwrap();
    let functions = functions.seal_bound().unwrap();
    let catalog = PlannerMemoryCatalog::default();
    let catalog = SqlPlannerTableSnapshot::new(&catalog);
    let settings = SessionOptimizerSettings {
        enable_materialized_view_rewrite: Some(false),
        ..SessionOptimizerSettings::default()
    };
    let control = SqlCompileControl::unbounded();
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(crate::exec::expr::test_support::parse_query(&format!(
            "SELECT CAST(7 AS {sql_type}) AS order_id"
        ))),
        SqlCompileIntent::IcebergWrite {
            root_distribution: RootDistributionRequirement::Any,
        },
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: settings.clone(),
        },
        SqlPlanningEnvironment::Distributed,
        &catalog,
        &functions,
        novarocks_sql::compiler::noop_constant_evaluator(),
        None,
        super::test_support::constant_policy(),
        control.clone(),
    ))
    .unwrap()
    .into_pending()
    .unwrap();
    let column = ColumnDef {
        name: "order_id".into(),
        data_type: dtype.clone(),
        nullable: false,
        write_default: None,
        logical_type: None,
    };
    let mut allocator = SqlTableBindingAllocator::new_unique().unwrap();
    let sink = DmlWritePlanInput::try_new(
        DmlWriteSinkMode::Data,
        DmlWriteTarget {
            binding: allocator.allocate().unwrap(),
            catalog: "iceberg".into(),
            namespace: "fixture".into(),
            table: "target".into(),
            fields: vec![DmlWriteTargetField {
                token: ConnectorWriteFieldToken::from_bytes([1; 32]),
                column: column.clone(),
                is_hidden: false,
            }],
        },
        vec![column],
        ConnectorWriteInputBinding::RootOutputByOrdinal,
    )
    .unwrap();
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let requirements = if with_statistics {
        vec![
            StatisticsRequiredAggregation::try_new(
                StatisticsScanColumn::try_new(0, "order_id", FunctionValueType::new(dtype, false))
                    .unwrap(),
                ICEBERG_THETA_AGGREGATE_NAME,
                StatisticsArtifactIdentity::try_new(vec![1], "writer-fixture-v1").unwrap(),
            )
            .unwrap(),
        ]
    } else {
        Vec::new()
    };
    let handle = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::parse("warehouse").unwrap(),
                CatalogVersion::from_bytes([9; 32]),
            ),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7].into(),
    );
    let statistics = DmlStatisticsSnapshot::empty();
    let source = compile_final_connector_write_plan(
        SqlOptimizeRequest::new(analyzed, &statistics, control),
        sink,
        ordinal,
        &requirements,
        &settings,
        DmlFinalWritePlanContext::new(
            DmlFinalPlanContext::new(
                PlanVersionId::try_new([91; 16]).unwrap(),
                PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                DmlFinalizedProviderReadSet::empty(),
            ),
            DmlFinalizedWriteTargetSet::try_new([DmlFinalizedWriteTarget { ordinal, handle }])
                .unwrap(),
        ),
        DecimalOverflowPolicy::OutputNull,
    )
    .unwrap();
    (source, functions)
}

#[test]
fn numeric_unary_writer_dml_actual_statistics_publishes_for_compiled() {
    {
        let (source, functions) = source(true, DataType::Int64, "BIGINT");
        assert_writer_selections(&source, &functions);
        let mut partials = 0;
        let mut finals = 0;
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                match &node.kind {
                    NodeKind::TableWriter { target } => {
                        assert_eq!(target.partial_aggregates.len(), 1);
                        assert_eq!(target.target_fields.len(), 1);
                        partials += 1;
                    }
                    NodeKind::TableFinish(finish) => {
                        assert_eq!(finish.final_aggregates.len(), 1);
                        finals += 1;
                    }
                    _ => {}
                }
            }
        }
        assert_eq!((partials, finals), (1, 1));
        assert!(
            source
                .plan()
                .edges()
                .values()
                .any(|edge| matches!(edge.kind, EdgeKind::Stream))
        );
    }
}

#[test]
fn numeric_unary_writer_dml_actual_no_statistics_publishes_for_compiled() {
    {
        let (source, _) = source(false, DataType::Int64, "BIGINT");
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                match &node.kind {
                    NodeKind::TableWriter { target } => {
                        assert!(target.partial_aggregates.is_empty())
                    }
                    NodeKind::TableFinish(finish) => assert!(finish.final_aggregates.is_empty()),
                    _ => {}
                }
            }
        }
    }
}

fn assert_writer_selections(
    source: &novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
    functions: &EngineFunctionCatalog,
) {
    let control = SqlCompileControl::unbounded();
    let semantics = novarocks_sql::compiler::author_fragment_package_semantics(
        source,
        super::test_support::constant_policy(),
        &control,
    )
    .unwrap();
    assert_eq!(semantics.len(), source.plan().fragments().len());
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let (partial, calls) = match &node.kind {
                NodeKind::TableWriter { target } => (true, &target.partial_aggregates),
                NodeKind::TableFinish(finish) => (false, &finish.final_aggregates),
                _ => continue,
            };
            for (ordinal, call) in calls.iter().enumerate() {
                let site = if partial {
                    novarocks_physical_plan::PhysicalCallSite::WriterPartial {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    }
                } else {
                    novarocks_physical_plan::PhysicalCallSite::WriterFinal {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    }
                };
                let request = fragment
                    .call_requests()
                    .get(novarocks_physical_plan::PhysicalCallDefinition::Relational(
                        site,
                    ))
                    .unwrap();
                let arguments = request
                    .arguments
                    .iter()
                    .map(|arg| -> FunctionArgument {
                        match arg {
                            novarocks_physical_plan::StaticFunctionArgument::Value {
                                value_type,
                                constant: None,
                            } => FunctionArgument::Value {
                                value_type: value_type.clone(),
                                constant: None,
                            },
                            _ => {
                                panic!("actual Writer statistics request is one nonconstant Value")
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                let materialized_request = || FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: request.logical_argument_count,
                    expected_result_type: request.expected_result_type.as_ref(),
                };
                let binding = &call.binding;
                let actual = FunctionBindingSelection {
                    overload: binding.function.overload.clone(),
                    argument_types: binding.function.argument_types.clone(),
                    result_type: FunctionResultType::Scalar(binding.function.result_type.clone()),
                    aggregate: Some(AggregateBindingSelection {
                        state_argument_contract: binding.state_argument_contract,
                        intermediate_type: binding.intermediate_type.clone(),
                        state_format: binding.state_format.clone(),
                    }),
                };
                let expected = functions
                    .select_exact_overload_observed(
                        &binding.function.function_id,
                        FunctionKind::Aggregate,
                        &binding.function.overload,
                        materialized_request(),
                        &control,
                    )
                    .unwrap();
                assert_eq!(
                    expected.as_ref(),
                    &actual,
                    "actual DML mode=compiled fragment={:?} site={site:?} original request={:?}",
                    fragment.id(),
                    materialized_request()
                );
                functions
                    .validate_frozen_selection(
                        &binding.function.function_id,
                        FunctionKind::Aggregate,
                        &actual,
                        materialized_request(),
                        &control,
                    )
                    .unwrap();
            }
        }
    }
}
#[test]
fn numeric_unary_writer_dml_actual_narrow_statistics_preserve_exact_selected_contract() {
    for (dtype, sql_type) in [
        (DataType::Int8, "TINYINT"),
        (DataType::Int16, "SMALLINT"),
        (DataType::Int32, "INT"),
    ] {
        {
            let (source, functions) = source(true, dtype.clone(), sql_type);
            assert_writer_selections(&source, &functions);
        }
    }
}

#[path = "sql_call_dependency_writer_loan_tests.rs"]
mod sql_call_dependency_writer_loan_tests;

// Isolate the real 1024-channel statistics tail from the native provider scan.
// NULL literals keep the native nullable BIGINT declarations without needing
// a catalog observation. This fixture does not execute rows or assert NDV.
fn wide_collect_true_source() -> (
    novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
    novarocks_connector_contract::ConnectorWriteRecipeDraft,
) {
    use arrow::datatypes::Field;
    use novarocks_connector_contract::{
        ConnectorInstanceDescriptor, ConnectorWriteBinding, ConnectorWriteFieldBinding,
        ConnectorWriteInputShape, ConnectorWriteRecipeDraft,
    };
    use novarocks_plan_codec::physical_package_v2::FROZEN_CONSTANT_POLICY;
    use novarocks_sql::planning::dml::begin_final_connector_write_plan;

    let mut builder = EngineFunctionCatalogBuilder::new();
    novarocks_functions::builtin::catalogue::contribute_builtin_functions(&mut builder).unwrap();
    builder
        .register(iceberg_theta_registration().unwrap().definition().clone())
        .unwrap();
    let functions = builder.seal_bound().unwrap();
    let catalog = PlannerMemoryCatalog::default();
    let catalog = SqlPlannerTableSnapshot::new(&catalog);
    let settings = SessionOptimizerSettings::default();
    let control = SqlCompileControl::unbounded();
    let columns = (0..1024)
        .map(|i| ColumnDef {
            name: format!("v{i}"),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        })
        .collect::<Vec<_>>();
    let sql = format!(
        "SELECT {}",
        (0..1024)
            .map(|i| format!("CAST(NULL AS BIGINT) AS v{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let semantics = novarocks_sql::sql_mode::SqlSemanticSettings::default();
    let decimal = semantics.sql_mode().decimal_overflow_policy();
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(crate::exec::expr::test_support::parse_query(&sql)),
        SqlCompileIntent::IcebergWrite {
            root_distribution: RootDistributionRequirement::Any,
        },
        SqlSessionContext {
            sql_semantics: semantics,
            current_catalog: Some("native_statistics".into()),
            current_database: "native_statistics_db".into(),
            optimizer_settings: settings.clone(),
        },
        SqlPlanningEnvironment::Distributed,
        &catalog,
        &functions,
        novarocks_sql::compiler::noop_constant_evaluator(),
        None,
        FROZEN_CONSTANT_POLICY,
        control.clone(),
    ))
    .unwrap_or_else(|e| panic!("collect1024 phase=analyze: {e:#?}"))
    .into_pending()
    .expect("collect1024 pending writer source");
    let token = |i: usize| {
        let mut bytes = [0; 32];
        bytes[..4].copy_from_slice(&u32::try_from(i + 1).unwrap().to_be_bytes());
        ConnectorWriteFieldToken::from_bytes(bytes)
    };
    let mut allocator = SqlTableBindingAllocator::new_unique().unwrap();
    let sink = DmlWritePlanInput::try_new(
        DmlWriteSinkMode::Data,
        DmlWriteTarget {
            binding: allocator.allocate().unwrap(),
            catalog: "native_statistics".into(),
            namespace: "native_statistics_db".into(),
            table: "native_statistics_wide_target".into(),
            fields: columns
                .iter()
                .enumerate()
                .map(|(i, column)| DmlWriteTargetField {
                    token: token(i),
                    column: column.clone(),
                    is_hidden: false,
                })
                .collect(),
        },
        columns.clone(),
        ConnectorWriteInputBinding::RootOutputByOrdinal,
    )
    .expect("collect1024 exact nullable BIGINT target");
    let requirements = columns
        .iter()
        .enumerate()
        .map(|(i, column)| {
            StatisticsRequiredAggregation::try_new(
                StatisticsScanColumn::try_new(
                    i,
                    column.name.as_str(),
                    FunctionValueType::new(DataType::Int64, true),
                )
                .unwrap(),
                ICEBERG_THETA_AGGREGATE_NAME,
                StatisticsArtifactIdentity::try_new(
                    vec![i32::try_from(i + 1).unwrap()],
                    "apache-datasketches-theta-v1",
                )
                .unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(requirements.len(), 1024);
    let provider = ConnectorProviderId::parse("statistics_test").unwrap();
    let instance = ConnectorInstanceId::parse("native_statistics").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([31; 32]));
    let binding = ConnectorWriteBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let handle = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![31].into(),
    );
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let statistics = DmlStatisticsSnapshot::empty();
    let (completion, needs) = begin_final_connector_write_plan(
        SqlOptimizeRequest::new(analyzed, &statistics, control.clone()),
        sink,
        ordinal,
        &requirements,
        &settings,
        decimal,
    )
    .unwrap_or_else(|e| panic!("collect1024 phase=optimize-and-auxiliary: {e:#?}"));
    assert!(needs.is_empty(), "literal isolation has no provider scan");
    let owner = completion
        .finish(
            PlanVersionId::try_new([31; 16]).unwrap(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            DmlFinalizedProviderReadSet::empty(),
            DmlFinalizedWriteTargetSet::try_new([DmlFinalizedWriteTarget {
                ordinal,
                handle: handle.clone(),
            }])
            .unwrap(),
            &control,
        )
        .unwrap_or_else(|e| panic!("collect1024 phase=final-lowering: {e:#?}"));
    let recipe = ConnectorWriteRecipeDraft::try_new(
        binding,
        handle,
        ConnectorWriteInputShape::Data {
            fields: columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    ConnectorWriteFieldBinding::try_new_typed(
                        token(i),
                        Field::new(&c.name, DataType::Int64, true),
                        FunctionValueType::new(DataType::Int64, true),
                    )
                    .unwrap()
                })
                .collect(),
        },
    )
    .unwrap_or_else(|e| panic!("collect1024 phase=core-writer-recipe: {e:#?}"));
    (owner, recipe)
}

#[test]
fn wide_collect_true_actual_theta_plan_freezes_with_production_limits() {
    use novarocks_physical_plan::{UnpivotConstant, extract_fragment_packages};
    use novarocks_plan_codec::physical_package_v2::{
        FROZEN_CONSTANT_POLICY, FROZEN_PACKAGE_ADMISSION, PackageDecodeLimits, PackageEncodeLimits,
        decode_fragment_package, encode_fragment_package,
    };
    use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
    use prost::Message;
    use std::collections::BTreeMap;

    let (owner, recipe) = wide_collect_true_source();
    let control = SqlCompileControl::unbounded();
    let mut writers = 0;
    let mut finishes = 0;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            match &node.kind {
                NodeKind::TableWriter { target } => {
                    writers += 1;
                    assert_eq!(target.target_fields.len(), 1024);
                    assert_eq!(target.partial_aggregates.len(), 1024);
                }
                NodeKind::TableFinish(finish) => {
                    finishes += 1;
                    assert_eq!(finish.final_aggregates.len(), 1024);
                    let unpivot = finish
                        .grouped_unpivot
                        .as_ref()
                        .expect("real statistics tail");
                    assert_eq!(unpivot.mappings.len(), 1024);
                    assert_eq!(
                        unpivot.statistics_target_ordinals.as_ref(),
                        &[WriteTargetOrdinal::try_new(0).unwrap()]
                    );
                    let mut work =
                        CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
                    for (i, mapping) in unpivot.mappings.iter().enumerate() {
                        assert_eq!(mapping.write_target_ordinal.get(), 0);
                        assert_eq!(mapping.constants.len(), 3);
                        let UnpivotConstant::Int32List(reference) = &mapping.constants[0] else {
                            panic!(
                                "collect1024 artifact field IDs lost their typed collection source"
                            );
                        };
                        let source = owner
                            .plan()
                            .constants()
                            .resolve_source_observed(*reference, &mut work)
                            .unwrap();
                        let values = source
                            .int32_list_observed(CompilePhase::Validate, &control)
                            .unwrap()
                            .unwrap();
                        assert_eq!(values.len(), 1);
                        assert_eq!(
                            values.item_observed(0, &mut work).unwrap(),
                            Some(i32::try_from(i + 1).unwrap())
                        );
                    }
                    work.finish().unwrap();
                }
                _ => {}
            }
        }
    }
    assert_eq!((writers, finishes), (1, 1));
    let semantics = novarocks_sql::compiler::author_fragment_package_semantics(
        &owner,
        FROZEN_CONSTANT_POLICY,
        &control,
    )
    .unwrap_or_else(|e| panic!("collect1024 phase=package-semantics: {e:#?}"));
    assert_eq!(semantics.len(), owner.plan().fragments().len());
    let uses = semantics
        .iter()
        .map(|(&id, s)| (id, s.expression_uses.clone()))
        .collect();
    let calls = semantics
        .iter()
        .map(|(&id, s)| (id, s.calls.clone()))
        .collect();
    let pruning = semantics
        .iter()
        .map(|(&id, s)| (id, s.pruning.clone()))
        .collect();
    let admissions = owner
        .plan()
        .fragments()
        .keys()
        .map(|&id| (id, FROZEN_PACKAGE_ADMISSION))
        .collect();
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let packages = extract_fragment_packages(
        owner.plan(),
        &BTreeMap::new(),
        &BTreeMap::from([(ordinal, recipe.clone())]),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &control,
    )
    .unwrap_or_else(|e| panic!("collect1024 phase=checked-package: {e:#?}"));
    assert_eq!(packages.len(), owner.plan().fragments().len());
    let model = FragmentDecodeResourceModel::try_new(&control).unwrap();
    let mut decoded_writers = 0;
    let mut decoded_finishes = 0;
    for (id, package) in &packages {
        let bytes = encode_fragment_package(package, &PackageEncodeLimits::FROZEN, &control)
            .unwrap_or_else(|e| panic!("collect1024 fragment={id:?} phase=encode: {e:#?}"))
            .encode_to_vec();
        let decoded =
            decode_fragment_package(&bytes, &model, &PackageDecodeLimits::FROZEN, &control)
                .unwrap_or_else(|e| panic!("collect1024 fragment={id:?} phase=decode: {e:#?}"));
        assert_eq!(decoded.fragment().id(), *id);
        assert_eq!(decoded.writes().len(), package.writes().len());
        for (node, actual) in decoded.writes() {
            assert_eq!(Some(actual), package.writes().get(node));
            assert_eq!(actual.input(), recipe.input());
        }
        for node in decoded.fragment().nodes().values() {
            match &node.kind {
                NodeKind::TableWriter { target } => {
                    decoded_writers += 1;
                    assert_eq!(target.target_fields.len(), 1024);
                    assert_eq!(target.partial_aggregates.len(), 1024);
                }
                NodeKind::TableFinish(finish) => {
                    decoded_finishes += 1;
                    assert_eq!(finish.final_aggregates.len(), 1024);
                    assert_eq!(
                        finish.grouped_unpivot.as_ref().unwrap().mappings.len(),
                        1024
                    );
                }
                _ => {}
            }
        }
    }
    assert_eq!((decoded_writers, decoded_finishes), (1, 1));
}
