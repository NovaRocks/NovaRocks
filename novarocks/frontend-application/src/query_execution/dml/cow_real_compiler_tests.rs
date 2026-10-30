// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This leaf crosses the real COW AST -> SQL analyze/optimize -> final writer
// publication boundary. Printing/reparsing the query is not a success oracle.
use novarocks_sql::compiler::{
    RootDistributionRequirement, SessionOptimizerSettings, SqlAnalyzeRequest, SqlCompileControl,
    SqlCompileIntent, SqlCompiler, SqlOptimizeRequest, SqlPlannerTableSnapshot,
    SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
};
use novarocks_sql::planning::dml::{
    ConnectorWriteInputBinding, DmlFinalizedProviderReadSet, DmlFinalizedWriteTarget,
    DmlFinalizedWriteTargetSet, DmlStatisticsSnapshot, DmlWritePlanInput, DmlWriteSinkMode,
    DmlWriteTarget, DmlWriteTargetField,
};

fn compile_signed_append(
    payload: ArrayRef,
    carrier: DataType,
    declaration: novarocks_types::schema::SqlType,
) {
    compile_signed_cow(payload, carrier, declaration, false);
}

fn compile_signed_cow(
    payload: ArrayRef,
    carrier: DataType,
    declaration: novarocks_types::schema::SqlType,
    rewrite: bool,
) {
    let fixture = cow_rewrite_query_fixture(vec![7], vec![2], payload, carrier, declaration);
    // The same signed after-image fields used by the existing append-route
    // oracle, with provider source ordinals deliberately reversed.
    let writer_fields = fixture.input.fields()[..if rewrite { 4 } else { 2 }]
        .iter()
        .map(|field| (*field).clone())
        .collect::<Vec<_>>();
    let input = if rewrite {
        fixture.input.clone()
    } else {
        novarocks_spi::connector::ConnectorWriteInputShape::Data {
            fields: writer_fields.clone(),
        }
    };
    let route = if rewrite {
        fixture.route_facts.clone()
    } else {
        novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts::try_new(
            fixture.route_facts.route_id(),
            fixture.route_facts.accepted_effects().to_vec(),
            vec![
                novarocks_spi::connector::ConnectorMutationRouteInput::new(
                    writer_fields[0].token(),
                    1,
                ),
                novarocks_spi::connector::ConnectorMutationRouteInput::new(
                    writer_fields[1].token(),
                    0,
                ),
            ],
            novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: novarocks_spi::connector::write_stack::ConnectorWriteColocation::None,
                layout: novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            fixture.route_facts.selection_bindings()[..2].to_vec(),
        )
        .expect("original append route")
    };
    let (_work, _root, binding, _capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let context = connector_context_for_test();
    let selection = super::admitted_cow_consumer_tests::collect_admitted_cow_selection(
        &fixture,
        &binding,
        context.clone(),
    );
    let original = crate::query_execution::original_cow_result_scope::from_original_binding(
        &binding, &context,
    )
    .unwrap();
    let check = || {
        original
            .check_active()
            .map_err(crate::query_execution::dml::cow_closed_ast::BuildError::Original)
    };
    let compact_columns = crate::query_execution::dml::cow_closed_route::materialized_columns(
        &input,
        &route,
        fixture.preparation.match_contract(),
        rewrite.then_some(&fixture.rewrite_source),
        &check,
    )
    .unwrap();
    let control = SqlCompileControl::unbounded();
    let materialized = crate::query_execution::dml::cow_materialized_values::build(
        &selection,
        &fixture.rows,
        &compact_columns,
        rewrite,
        rewrite.then_some(
            fixture
                .preparation
                .match_contract()
                .effect_field()
                .target_ordinal(),
        ),
        &original,
        crate::application::test_constant_policy(),
        &control,
    )
    .unwrap();
    let query = if rewrite {
        crate::query_execution::dml::cow_closed_route::build_cow_rewrite_query_materialized(
            &selection,
            &fixture.rows,
            &input,
            &route,
            &fixture.rewrite_source,
            fixture.preparation.match_contract(),
            &fixture.identity,
            &check,
        )
        .expect("original signed COW rewrite AST")
    } else {
        crate::query_execution::dml::cow_closed_route::build_cow_append_query_materialized(
            &selection,
            &fixture.rows,
            &input,
            &route,
            fixture.preparation.match_contract(),
            &check,
        )
        .expect("same product topology from signed compact constants")
    };
    let columns = writer_fields
        .iter()
        .map(|field| {
            ColumnDef::from_value_type(
                field.field().name().clone(),
                field
                    .execution_type()
                    .expect("signed complete writer declaration")
                    .clone(),
                None,
            )
            .expect("original typed writer column")
        })
        .collect::<Vec<_>>();
    // SQL's own allocator mints the request-local target token; no guessed
    // scope, provider-derived token reinterpretation or test-only constructor.
    let binding = novarocks_sql::binding::SqlTableBindingAllocator::new_unique()
        .unwrap()
        .allocate()
        .unwrap();
    let sink = DmlWritePlanInput::try_new(
        if rewrite {
            DmlWriteSinkMode::RowLineageData
        } else {
            DmlWriteSinkMode::Data
        },
        DmlWriteTarget {
            binding,
            catalog: "iceberg".into(),
            namespace: "db".into(),
            table: "t".into(),
            fields: writer_fields
                .iter()
                .zip(&columns)
                .map(|(field, column)| DmlWriteTargetField {
                    token: field.token(),
                    column: column.clone(),
                    is_hidden: false,
                })
                .collect(),
        },
        columns,
        ConnectorWriteInputBinding::RootOutputByOrdinal,
    )
    .unwrap()
    .with_external_count_owner();
    let scan_columns = crate::query_execution::distributed_rewrite::connector_execution_columns(
        fixture.rewrite_source.scan_execution_schema(),
    )
    .expect("original frozen source complete declarations");
    let scan_binding = novarocks_sql::binding::SqlTableBindingAllocator::new_unique()
        .unwrap()
        .allocate()
        .unwrap();
    struct SourceCatalog {
        identity: FrozenConnectorScanIdentity,
        table: novarocks_sql::planning::catalog::ResolvedAnalyzerTable,
    }
    impl novarocks_sql::planning::catalog::PlannerTableProvider for SourceCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<novarocks_sql::planning::catalog::ResolvedAnalyzerTable, String> {
            if catalog != Some(self.identity.catalog())
                || database != self.identity.namespace()
                || table != self.identity.table()
            {
                return Err(format!(
                    "unexpected COW source {catalog:?}.{database}.{table}"
                ));
            }
            Ok(self.table.clone())
        }
    }
    let catalog = SourceCatalog {
        identity: fixture.identity.clone(),
        table: novarocks_sql::planning::query_execution::pinned_file_set_resolved_analyzer_table(
            &fixture.identity,
            scan_columns.clone(),
            scan_binding,
        ),
    };
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    let functions = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let settings = SessionOptimizerSettings::default();
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::materialized_values_query(
            Box::new(query),
            if rewrite {
                novarocks_sql::compiler::SqlMaterializedValuesSite::SelectJoinDerived {
                    from: 0,
                    join: 0,
                }
            } else {
                novarocks_sql::compiler::SqlMaterializedValuesSite::SelectFromDerived { from: 0 }
            },
            materialized,
            &control,
        )
        .unwrap(),
        SqlCompileIntent::IcebergWrite {
            root_distribution: RootDistributionRequirement::Any,
        },
        SqlSessionContext {
            current_catalog: Some("iceberg".into()),
            current_database: "db".into(),
            optimizer_settings: settings.clone(),
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        },
        SqlPlanningEnvironment::Distributed,
        &snapshot,
        &functions,
        crate::query_execution::constant_eval::constant_evaluator(),
        None,
        crate::application::test_constant_policy(),
        control.clone(),
    ))
    .expect("real SQL analysis of signed COW values")
    .into_pending()
    .unwrap();
    let statistics = if rewrite {
        use novarocks_spi::connector::{
            StatisticsBasisRelation, StatisticsDataVersion, StatisticsEvidence,
            StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricObservation,
            StatisticsMetricSource, StatisticsMetricState, StatisticsMetricValue,
            StatisticsNumericNature, StatisticsRowCoverage,
        };
        let version =
            StatisticsDataVersion::try_new(bytes::Bytes::from_static(b"frozen-rewrite-source"))
                .unwrap();
        let evidence = StatisticsEvidence::try_new(
            version.clone(),
            StatisticsEvidenceRevision::try_new(bytes::Bytes::from_static(b"frozen-rewrite-stats"))
                .unwrap(),
            StatisticsRowCoverage::AllVisibleRows,
            std::collections::BTreeMap::from([(
                StatisticsMetric::RowCount,
                StatisticsMetricState::Available(StatisticsMetricObservation::new(
                    StatisticsMetricValue::U64(1),
                    version,
                    StatisticsMetricSource::CurrentManifest,
                    StatisticsNumericNature::Exact,
                    StatisticsBasisRelation::Identical,
                )),
            )]),
        )
        .unwrap();
        DmlStatisticsSnapshot::from_evidence([
            novarocks_sql::planning::dml::DmlStatisticsEvidence::Available {
                binding: scan_binding,
                label: format!(
                    "{}.{}.{}",
                    fixture.identity.catalog(),
                    fixture.identity.namespace(),
                    fixture.identity.table()
                ),
                columns: scan_columns,
                evidence,
            },
        ])
    } else {
        DmlStatisticsSnapshot::empty()
    };
    let ordinal = novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0).unwrap();
    let (completion, needs) = novarocks_sql::planning::dml::begin_final_connector_write_plan(
        SqlOptimizeRequest::new(analyzed, &statistics, control.clone()),
        sink,
        ordinal,
        &[],
        &settings,
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    )
    .expect("real optimizer and typed writer preparation");
    assert_eq!(
        needs.len(),
        usize::from(rewrite),
        "one exact frozen rewrite source or no append scan"
    );
    let reads = DmlFinalizedProviderReadSet::try_new(needs.iter().map(|need| {
        use novarocks_physical_plan::{
            ExactInputVersion, PredicateGuaranteeKind, ProviderColumnReference,
            ProviderReadReference,
        };
        use novarocks_spi::connector::read_stack::ConnectorReadBinding;
        use novarocks_spi::connector::{
            ConnectorCodecCategory, ConnectorInstanceDescriptor, ConnectorInstanceId,
            ConnectorProviderId, ConnectorReadRelationPayload,
        };
        use novarocks_sql::compiler::*;
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake").unwrap(),
                instance_id: ConnectorInstanceId::parse("mor_session_unit").unwrap(),
            },
            row_mutation_catalog_handle(),
        );
        let encoded =
            |kind| row_mutation_payload(kind, bytes::Bytes::from_static(b"original-cow-read"));
        let contract = ProviderReadStaticContract {
            sql_binding: need.binding(),
            request: ProviderReadRequestBinding::from_need(need),
            read: ProviderReadReference {
                binding,
                input_version: ExactInputVersion::try_new([9]).unwrap(),
                relation: ConnectorReadRelationPayload::new(
                    need.relation().relation_kind(),
                    encoded(ConnectorCodecCategory::ReadTable),
                    encoded(ConnectorCodecCategory::ReadView),
                ),
            },
            metadata_kind: matches!(need.relation(), ProviderReadRelationNeed::Metadata { .. })
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
                            column_payload: encoded(ConnectorCodecCategory::ReadColumn),
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
        };
        novarocks_sql::planning::dml::DmlFinalizedProviderRead {
            fact: ProviderReadFact::negotiated(need, contract)
                .expect("same original complete read binding"),
            read_budget: novarocks_physical_plan::ScanReadBudget {
                max_batch_rows: novarocks_physical_plan::MAX_SCAN_BATCH_ROWS,
                max_batch_bytes: novarocks_physical_plan::MAX_SCAN_BATCH_BYTES,
            },
        }
    }))
    .unwrap();
    let owner = completion
        .finish(
            novarocks_physical_plan::PlanVersionId::try_new([43; 16]).unwrap(),
            novarocks_physical_plan::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: true,
            },
            reads,
            DmlFinalizedWriteTargetSet::try_new([DmlFinalizedWriteTarget {
                ordinal,
                // This is the existing opaque provider wire fixture. The compiler
                // validates and preserves its header, and never interprets it.
                handle: row_mutation_payload(
                    novarocks_spi::connector::ConnectorCodecCategory::WriteHandle,
                    bytes::Bytes::from_static(b"cow-compiler-writer"),
                ),
            }])
            .unwrap(),
            &control,
        )
        .expect("actual COW compiler must publish the complete signed writer binding");
    let mut seen = 0;
    let mut finish_seen = 0;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let novarocks_physical_plan::NodeKind::TableFinish(finish) = &node.kind {
                finish_seen += 1;
                assert_eq!(
                    finish.count_mode,
                    novarocks_physical_plan::FinishCountMode::ExternalOwner
                );
                assert!(finish.inputs.logical_summary.is_none());
            }
            let novarocks_physical_plan::NodeKind::TableWriter { target } = &node.kind else {
                continue;
            };
            seen += 1;
            assert_eq!(target.target_fields.len(), writer_fields.len());
            for (actual, signed) in target.target_fields.iter().zip(&writer_fields) {
                let expected = signed.execution_type().unwrap();
                assert_eq!(actual.token, signed.token());
                assert_eq!(actual.provider_name.as_ref(), signed.field().name());
                assert_eq!(&actual.ty, expected);
                assert!(
                    fragment.values()[&actual.input]
                        .ty
                        .same_physical_binding_ignoring_root_nullability_observed(
                            expected,
                            || Ok::<_, String>(())
                        )
                        .unwrap(),
                    "actual writer input must preserve every nominal child, name, exact layout and child-NULL contract"
                );
            }
        }
    }
    assert_eq!(seen, 1, "one actual positional writer");
    assert_eq!(finish_seen, 1, "one external-count-owner finish");
}

#[test]
fn signed_nonnull_json_reaches_actual_compiler_writer_binding() {
    compile_signed_append(
        Arc::new(StringArray::from(vec!["{\"a\":1}"])),
        DataType::Utf8,
        novarocks_types::schema::SqlType::Json,
    );
}
#[test]
fn signed_nonnull_variant_reaches_actual_compiler_writer_binding() {
    // Identical payload to the original COW Variant route oracle.
    let payload = [0x0c_u8, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03];
    compile_signed_append(
        Arc::new(arrow::array::LargeBinaryArray::from_iter_values([
            payload.as_slice()
        ])),
        DataType::LargeBinary,
        novarocks_types::schema::SqlType::Variant,
    );
}
#[test]
fn signed_nested_required_child_reaches_actual_compiler_writer_binding() {
    let child = Arc::new(arrow::datatypes::Field::new("na`me", DataType::Utf8, false));
    let array = arrow::array::StructArray::from(vec![(
        child,
        Arc::new(StringArray::from(vec!["required"])) as ArrayRef,
    )]);
    let carrier = arrow::array::Array::data_type(&array).clone();
    compile_signed_append(
        Arc::new(array),
        carrier,
        novarocks_types::schema::SqlType::Struct(vec![(
            "na`me".into(),
            novarocks_types::schema::SqlType::String,
        )]),
    );
}

#[test]
fn signed_variant_rewrite_reaches_actual_compiler_writer_binding() {
    let payload = [0x0c_u8, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03];
    compile_signed_cow(
        Arc::new(arrow::array::LargeBinaryArray::from_iter_values([
            payload.as_slice()
        ])),
        DataType::LargeBinary,
        novarocks_types::schema::SqlType::Variant,
        true,
    );
}
