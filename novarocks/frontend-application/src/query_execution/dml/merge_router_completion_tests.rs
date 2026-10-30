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

// This witness keeps the production MERGE producer and completion/encoding
// path. Literal relations replace external reads; scripted sealed recipes
// exercise package/manifest structure, not Iceberg execution or publication.
#[test]
fn mor_merge_completion_freezes_original_routes_and_exact_summary_sources() {
    use novarocks_query_application::preparation::{
        CompletedPhysicalPlanCandidate, CompletedPlanWithAccess, ReadAccessSink,
    };
    use novarocks_spi::connector::{
        ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteFieldToken,
        ConnectorWriteRouteId, write_stack::WriteTargetOrdinal,
    };
    use novarocks_sql::compiler::{
        SqlAnalyzeRequest, SqlCompileControl, SqlCompileIntent, SqlCompiler, SqlOptimizeRequest,
        SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    };
    use novarocks_sql::planning::dml::{
        ConnectorWriteInputBinding, DmlChangeStreamStatisticsTarget, DmlFinalPlanContext,
        DmlFinalWritePlanContext, DmlFinalizedProviderReadSet, DmlFinalizedWriteTarget,
        DmlFinalizedWriteTargetSet, DmlStatisticsSnapshot, DmlWritePlanInput, DmlWritePlanShape,
        DmlWriteTarget, DmlWriteTargetField,
    };

    let columns = vec![col("id"), col("v")];
    // Two source rows exercise the same matched UPDATE and unmatched INSERT
    // branches as the native row-level scenario. The target supplies the exact
    // locator and v3 lineage columns consumed by the production SQL builder.
    let match_sql = build_merge_match_query_sql(
        "(SELECT CAST(2 AS BIGINT) AS id, CAST(2 AS BIGINT) AS v, \
         'fixture.parquet' AS _file, CAST(0 AS BIGINT) AS _pos, \
         CAST(20 AS BIGINT) AS _row_id, \
         CAST(1 AS BIGINT) AS _last_updated_sequence_number) AS t",
        "t",
        "(SELECT CAST(2 AS BIGINT) AS id, CAST(2000 AS BIGINT) AS v \
         UNION ALL SELECT CAST(99 AS BIGINT), CAST(9900 AS BIGINT)) AS s",
        "t.id = s.id",
        None,
        None,
        &columns,
        &[("v", "CAST(s.v AS BIGINT)")],
        &[("id", "CAST(s.id AS BIGINT)"), ("v", "CAST(s.v AS BIGINT)")],
        Some(MERGE_ACTION_MATCHED_UPDATE),
        true,
    );
    let query = parse_generated_query(&match_sql, "MOR MERGE router witness").unwrap();
    let control = SqlCompileControl::unbounded();
    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    let functions = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(Box::new(query)),
        SqlCompileIntent::ChangeStreamWrite,
        SqlSessionContext {
            current_catalog: None,
            current_database: "default_database".into(),
            optimizer_settings: dml_change_stream_optimizer_settings(),
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
    .expect("real analysis of the production MERGE match query")
    .into_pending()
    .unwrap();

    let mut allocator = novarocks_sql::binding::SqlTableBindingAllocator::new_unique().unwrap();
    let mut routes = Vec::new();
    let mut sealed_recipes = MergeSealedRecipes::new();
    for ordinal in 0_u32..2 {
        let route_columns = if ordinal == 0 {
            vec![
                col("id"),
                col("v"),
                col("_row_id"),
                col("_last_updated_sequence_number"),
            ]
        } else {
            vec![
                ColumnDef {
                    data_type: DataType::Utf8,
                    ..col("_file")
                },
                col("_pos"),
            ]
        };
        let input_ordinals: &[u32] = if ordinal == 0 { &[0, 1, 2, 3] } else { &[4, 5] };
        let fields = route_columns
            .iter()
            .enumerate()
            .map(|(index, column)| DmlWriteTargetField {
                token: ConnectorWriteFieldToken::from_bytes(
                    [1 + ordinal as u8 * 8 + index as u8; 32],
                ),
                column: column.clone(),
                is_hidden: column.name.starts_with('_'),
            })
            .collect::<Vec<_>>();
        // The provider signs the same explicit target declaration supplied to
        // SQL, including its role split, tokens, types and nullability.
        sealed_recipes.seal_target(ordinal, &fields);
        let inputs = fields
            .iter()
            .zip(input_ordinals)
            .map(|(field, input)| ConnectorMutationRouteInput::new(field.token, *input))
            .collect();
        routes.push(DmlChangeStreamRoute {
            requirements: novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: novarocks_spi::connector::write_stack::ConnectorWriteColocation::None,
                layout: novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            route_id: ConnectorWriteRouteId::from_bytes([ordinal as u8 + 1; 32]),
            write_target_ordinal: WriteTargetOrdinal::try_new(ordinal).unwrap(),
            accepted_effects: if ordinal == 0 {
                vec![
                    ConnectorRowMutationEffect::Replace,
                    ConnectorRowMutationEffect::Insert,
                ]
            } else {
                vec![ConnectorRowMutationEffect::Replace]
            },
            input_ordinals: inputs,
            sink: DmlWritePlanInput::try_new(
                if ordinal == 0 {
                    DmlWriteSinkMode::RowLineageData
                } else {
                    DmlWriteSinkMode::DeletionVectors
                },
                DmlWriteTarget {
                    binding: allocator.allocate().unwrap(),
                    catalog: "iceberg".into(),
                    namespace: "db".into(),
                    table: "target".into(),
                    fields,
                },
                route_columns,
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            )
            .unwrap(),
        });
    }
    let statistics = DmlStatisticsSnapshot::empty();
    let (completion, needs) = novarocks_sql::planning::dml::begin_final_dml_change_stream(
        DmlChangeStreamCompileRequest {
            optimize_request: SqlOptimizeRequest::new(analyzed, &statistics, control.clone()),
            kind: DmlChangeStreamKind::Merge {
                target_columns: columns,
                matched_update: true,
                matched_delete: false,
                not_matched_insert: true,
            },
            routes,
            statistics_targets: (0..2)
                .map(|ordinal| DmlChangeStreamStatisticsTarget {
                    write_target_ordinal: WriteTargetOrdinal::try_new(ordinal).unwrap(),
                    requirements: Vec::new(),
                })
                .collect(),
            pre_expand_keyed_assert: mor_merge_pre_expand_assertion(true),
            shape: DmlWritePlanShape::Dataflow,
        },
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    )
    .expect("real optimizer and MERGE change-stream preparation");
    assert!(
        needs.is_empty(),
        "literal relations acquire no provider capabilities"
    );
    let finalized = completion
        .finish(
            DmlFinalWritePlanContext::new(
                DmlFinalPlanContext::new(
                    novarocks_physical_plan::PlanVersionId::try_new([61; 16]).unwrap(),
                    novarocks_physical_plan::PipelineDopDomain {
                        min: 1,
                        max: 2,
                        requires_power_of_two: true,
                    },
                    DmlFinalizedProviderReadSet::try_new([]).unwrap(),
                ),
                DmlFinalizedWriteTargetSet::try_new((0..2).map(|ordinal| {
                    DmlFinalizedWriteTarget {
                        ordinal: WriteTargetOrdinal::try_new(ordinal).unwrap(),
                        handle: sealed_recipes.payload(ordinal),
                    }
                }))
                .unwrap(),
            ),
            &control,
        )
        .expect("actual production DML completion must precede encoding");
    assert_eq!(
        finalized
            .physical_plan()
            .fragments()
            .values()
            .filter(|fragment| {
                matches!(
                    fragment.sink(),
                    novarocks_physical_plan::FragmentSink::Router { .. }
                )
            })
            .count(),
        1
    );
    assert_eq!(
        finalized
            .physical_plan()
            .fragments()
            .values()
            .flat_map(|fragment| { fragment.nodes().values() })
            .filter(|node| matches!(
                &node.kind,
                novarocks_physical_plan::NodeKind::TableWriter { .. }
            ))
            .count(),
        2
    );
    // A generated source must not force one global window to invent identities.
    // Exact target identity remains nullable for unmatched source rows; only
    // selected matched actions enter the real keyed assertion.
    let mut assertions = 0;
    for fragment in finalized.physical_plan().fragments().values() {
        for node in fragment.nodes().values() {
            assert!(
                !matches!(node.kind, novarocks_physical_plan::NodeKind::Window(_)),
                "MERGE action selection must preserve independent source placement"
            );
            if let novarocks_physical_plan::NodeKind::AssertOneRow(
                novarocks_physical_plan::RowCountAssertionSpec::PerKeyAtMostOne {
                    applicability,
                    keys,
                    labels,
                    ..
                },
            ) = &node.kind
            {
                assertions += 1;
                assert_eq!(labels.as_ref(), [Box::<str>::from("_row_id")]);
                assert_eq!(keys.len(), 1);
                assert_eq!(fragment.values()[&keys[0]].ty.data_type(), &DataType::Int64);
                assert!(fragment.values()[&keys[0]].ty.nullable());
                let novarocks_physical_plan::AssertionApplicability::WhenTrue(mask) = applicability
                else {
                    panic!("only selected matched actions own a target-key obligation")
                };
                assert_eq!(fragment.values()[mask].ty.data_type(), &DataType::Boolean);
                // WhenTrue admits the compiler's actual nullable Boolean
                // declaration; FALSE and NULL both bypass target identity.
            }
        }
    }
    assert_eq!(
        assertions, 1,
        "the frozen graph has one scoped target assertion"
    );
    // The production sink-fact author retains the independent summary role
    // and edge from this completed graph before the normal encoder freezes it.
    let physical = finalized.physical_plan();
    let router = physical
        .fragments()
        .values()
        .find(|fragment| {
            matches!(
                fragment.sink(),
                novarocks_physical_plan::FragmentSink::Router { .. }
            )
        })
        .unwrap();
    let novarocks_physical_plan::FragmentSink::Router {
        routes, summary, ..
    } = router.sink()
    else {
        unreachable!()
    };
    let sink_facts =
        crate::native::fragment_encoder::frozen::tests::sink_targets_for_completed_fragment(
            router,
            physical.edges(),
        )
        .expect("the exact completed Router output facts");
    assert_eq!(sink_facts.len(), routes.len() + 1);
    for (route, target) in routes.iter().zip(sink_facts.iter()) {
        assert_eq!(target.edge(), route.edge);
        assert_eq!(
            target.role(),
            novarocks_physical_plan::OutputRole::ProviderRoute {
                target: route.write_target_ordinal,
            }
        );
    }
    let summary_fact = sink_facts.last().unwrap();
    let summary_edge = &physical.edges()[&summary.edge];
    assert_eq!(summary_fact.edge(), summary.edge);
    assert_eq!(
        summary_fact.role(),
        novarocks_physical_plan::OutputRole::LogicalSummary
    );
    assert_eq!(
        summary_fact.target_fragment_id(),
        summary_edge.destination.fragment.get()
    );
    assert_eq!(
        summary_fact.target_exchange_node_id(),
        i32::try_from(summary_edge.destination.node.get()).unwrap()
    );
    assert_eq!(
        summary_fact.delivery(),
        novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
    );
    let mut wrong_role = physical.edges().clone();
    wrong_role.get_mut(&summary.edge).unwrap().role =
        novarocks_physical_plan::OutputRole::WriterResults;
    assert!(
        crate::native::fragment_encoder::frozen::tests::sink_targets_for_completed_fragment(
            router,
            &wrong_role
        )
        .is_err()
    );
    let mut wrong_port = physical.edges().clone();
    wrong_port.get_mut(&summary.edge).unwrap().source.port =
        novarocks_physical_plan::FragmentOutputPort::Root;
    assert!(
        crate::native::fragment_encoder::frozen::tests::sink_targets_for_completed_fragment(
            router,
            &wrong_port
        )
        .is_err()
    );
    let mut missing_summary = physical.edges().clone();
    missing_summary.remove(&summary.edge);
    assert!(
        crate::native::fragment_encoder::frozen::tests::sink_targets_for_completed_fragment(
            router,
            &missing_summary
        )
        .is_err()
    );

    let router_fragment = router.id().get();
    let expected_summary = *summary_fact;
    let expected_fragments = physical
        .fragments()
        .keys()
        .map(|id| id.get())
        .collect::<std::collections::BTreeSet<_>>();
    let recipes = &sealed_recipes;
    let expected_writers = physical
        .fragments()
        .values()
        .flat_map(|fragment| {
            fragment.nodes().values().filter_map(move |node| {
                if let novarocks_physical_plan::NodeKind::TableWriter { target } = &node.kind {
                    assert_eq!(
                        target.handle,
                        recipes.payload(target.write_target_ordinal.get())
                    );
                    Some((fragment.id().get(), node.id, target.write_target_ordinal))
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>();
    let (plan, writer_routes) = finalized.into_parts();
    assert_eq!(writer_routes.len(), 2);
    let candidate = CompletedPhysicalPlanCandidate::for_sql_program(plan, &control)
        .unwrap()
        .freeze_root_output(novarocks_result_contract::FrozenRootOutput::InternalFacts(
            novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1,
        ))
        .unwrap();
    let sink =
        ReadAccessSink::<crate::query_execution::provider_read_facts::FrozenProviderRead>::new();
    let access = sink
        .try_into_access()
        .unwrap_or_else(|(error, _)| panic!("empty access: {error}"));
    let paired = CompletedPlanWithAccess::try_pair(candidate, access)
        .unwrap_or_else(|(error, _)| panic!("pair original completed plan: {error}"));
    let freezes = crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread();
    let encoded = crate::query_execution::physical_encoding::encode_completed_plan(
        paired,
        &crate::query_execution::package_freeze::CompiledPackageCarrier::for_test(
            novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
            novarocks_plan_codec::physical_package_v2::PackageEncodeLimits::FROZEN,
        ),
        crate::application::test_constant_policy(),
        Some(
            &crate::query_execution::physical_encoding::WriteTargetFacts {
                session: &sealed_recipes,
            },
        ),
        &control,
    )
    .expect("the normal encoder freezes this complete Router with its original sealed recipes");
    assert_eq!(
        crate::native::fragment_encoder::frozen::tests::freezes_on_this_thread() - freezes,
        expected_fragments.len(),
        "each real completed fragment freezes once",
    );
    assert_eq!(
        encoded
            .native
            .fragment_ids()
            .collect::<std::collections::BTreeSet<_>>(),
        expected_fragments
    );
    assert!(
        encoded.access.iter().next().is_none(),
        "literal source has no external read capability"
    );
    let router_artifact = encoded.native.get(router_fragment).unwrap();
    assert_eq!(
        router_artifact.facts().logical_summary_output(),
        Some(expected_summary)
    );
    assert_eq!(router_artifact.facts().sink_targets(), sink_facts.as_ref());
    assert!(
        router_artifact.facts().table_writers().is_empty(),
        "summary source is not a provider writer"
    );

    // Decode the exact normal Native artifact; no alternate package author or
    // test bytes are substituted. The session's two recipes must survive intact.
    let model =
        novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel::try_new(&control)
            .unwrap();
    let mut actual_writers = std::collections::BTreeSet::new();
    let mut compiled_finishes = 0;
    for fragment in encoded.native.fragment_ids() {
        let artifact = encoded.native.get(fragment).unwrap();
        let carried = novarocks_task_codec::creation::decode_static_package(
            artifact.content(),
            novarocks_proto_codec::FieldPath::root("merge_freeze_component"),
        )
        .unwrap();
        let package = novarocks_plan_codec::physical_package_v2::decode_fragment_package(
            carried.package(),
            &model,
            &novarocks_plan_codec::physical_package_v2::test_support::decode_limits(),
            &control,
        )
        .unwrap();
        assert_eq!(package.fragment().id().get(), fragment);
        for (&node, recipe) in package.writes() {
            let novarocks_physical_plan::NodeKind::TableWriter { target } =
                &package.fragment().nodes()[&node].kind
            else {
                panic!("recipe must address its original writer")
            };
            let sealed = sealed_recipes.target(target.write_target_ordinal.get());
            assert_eq!(recipe.binding(), sealed.handle().binding());
            assert_eq!(recipe.payload(), &target.handle);
            assert_eq!(
                recipe.payload(),
                &sealed_recipes.payload(target.write_target_ordinal.get())
            );
            assert_eq!(recipe.input(), sealed.input());
            actual_writers.insert((fragment, node, target.write_target_ordinal));
        }
        let root = &package.fragment().nodes()[&package.fragment().root()];
        if let novarocks_physical_plan::NodeKind::TableFinish(spec) = &root.kind {
            use novarocks_functions::{
                EngineFunctionCatalogBuilder, FunctionKind, InstalledPureKernel,
            };
            use novarocks_local_program::{ProgramNodeKind, ProgramOutputRole};
            let expected_roles = spec.inputs;
            let expected_targets = spec.expected_target_ordinals.clone();
            assert!(
                package.writes().is_empty(),
                "finish carries no provider recipe"
            );

            // The finish binds no function. Retain a real sealed nonempty
            // subset, using the original catalog's installed declarations.
            let definition = functions.definition("rand", FunctionKind::Scalar).unwrap();
            let declaration = definition.binding_declaration().unwrap();
            let installed = declaration
                .overloads()
                .iter()
                .map(|overload| {
                    let pure = functions
                        .pure_overload_declaration_observed(
                            declaration.function_id(),
                            declaration.kind(),
                            &overload.identity,
                            &control,
                        )
                        .unwrap();
                    InstalledPureKernel {
                        function: declaration.function_id().clone(),
                        kind: declaration.kind(),
                        implementation: pure.implementation().clone(),
                        aggregate_state_format: overload
                            .aggregate
                            .as_ref()
                            .map(|aggregate| aggregate.state_format.clone()),
                    }
                })
                .collect::<Vec<_>>();
            let mut subset = EngineFunctionCatalogBuilder::new();
            subset.register(definition.clone()).unwrap();
            let pure_functions = subset.seal_pure(installed).unwrap();
            let providers = novarocks_connector_contract::PureProviderProgramCatalog::<
                std::io::Error,
            >::try_new(&[], vec![], &control)
            .unwrap();
            let validated = novarocks_local_compiler::validate_fragment_providers(
                std::sync::Arc::new(package),
                &providers,
                &control,
            )
            .expect("the real decoded finish requires no provider capability");
            let program = novarocks_local_compiler::compile_fragment(
                validated,
                &pure_functions,
                novarocks_local_compiler::LocalCompileOptions {
                    pipeline_dop: std::num::NonZeroUsize::new(1).unwrap(),
                    root_sink_dop: std::num::NonZeroUsize::new(1),
                    kernel_abi: novarocks_local_program::KernelAbiVersion::CURRENT,
                    constants: crate::application::test_constant_policy(),
                    exchange_wait: std::time::Duration::from_secs(60),
                },
                &control,
            )
            .expect("the production SQL finish compiles from its normal Native package");
            let ProgramNodeKind::TableFinish {
                inputs,
                input_roles,
                expected_targets: actual_targets,
                writer_multiplex_layout,
                ..
            } = program.graph().nodes()[program.graph().root().index()].kind()
            else {
                panic!("the compiled finish retains its root purpose")
            };
            assert_eq!(*input_roles, expected_roles);
            assert_eq!(actual_targets.as_slice(), expected_targets.as_ref());
            assert_eq!(actual_targets.len(), 2);
            let writer =
                &program.graph().nodes()[inputs[input_roles.writer_results as usize].index()];
            assert!(matches!(writer.kind(), ProgramNodeKind::UnionAll { .. }));
            let expected_relation =
                novarocks_spi::connector::write_stack::WriterMultiplexSchema::try_new(vec![])
                    .expect("the original statement requests no auxiliary statistics");
            expected_relation
                .validate_exact_arrow_schema(writer.output_layout().schema())
                .expect("the real SQL merger publishes the exact writer protocol purpose");
            expected_relation
                .validate_exact_arrow_schema(writer_multiplex_layout.schema())
                .expect("the real SQL finish consumes the exact writer protocol purpose");
            assert_eq!(
                writer.output_layout().slots(),
                writer_multiplex_layout.slots()
            );
            assert_eq!(
                writer.output_layout().schema(),
                writer_multiplex_layout.schema()
            );
            let summary_port = input_roles.logical_summary.unwrap();
            assert_ne!(summary_port, input_roles.writer_results);
            let summary = inputs[summary_port as usize];
            assert!(matches!(
                program.graph().nodes()[summary.index()].kind(),
                ProgramNodeKind::ExchangeSource { .. }
            ));
            assert_eq!(
                program.exchange_inputs()[&summary].role,
                ProgramOutputRole::LogicalSummary
            );
            compiled_finishes += 1;
        }
    }
    assert_eq!(
        compiled_finishes, 1,
        "the actual SQL author emits one finish"
    );
    assert_eq!(actual_writers, expected_writers.iter().copied().collect());
    crate::query_execution::artifact::TaskManifestBinding::assert_completed_router_placements_for_test(
        encoded, router_fragment, expected_summary, &expected_writers,
    );
}

/// A scripted structural provider: it supplies only sealed recipe authority.
/// No writer, connector execution runtime, catalog I/O or publication is installed.
struct MergeRecipeProvider {
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    catalog: novarocks_spi::connector::CatalogHandle,
}
impl novarocks_spi::connector::write_stack::ProviderWriteRuntime for MergeRecipeProvider {
    type CommitHandle = ();
    type WriterHandle = u32;
    type CommitFragment = ();
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn catalog_handle(&self) -> &novarocks_spi::connector::CatalogHandle {
        &self.catalog
    }
}
struct MergeSealedRecipes {
    adapter: novarocks_spi::connector::write_stack::WriteRuntimeAdapter<MergeRecipeProvider>,
    targets: Vec<novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan>,
}
impl MergeSealedRecipes {
    fn new() -> Self {
        let instance =
            novarocks_spi::connector::ConnectorInstanceId::parse("merge_freeze_component").unwrap();
        let provider = MergeRecipeProvider {
            descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse(
                    "merge_freeze_scripted",
                )
                .unwrap(),
                instance_id: instance.clone(),
            },
            catalog: novarocks_spi::connector::CatalogHandle::new(
                instance,
                novarocks_spi::connector::CatalogVersion::from_bytes([61; 32]),
            ),
        };
        Self {
            adapter: novarocks_spi::connector::write_stack::WriteRuntimeAdapter::new(
                std::sync::Arc::new(provider),
            ),
            targets: Vec::new(),
        }
    }
    fn seal_target(
        &mut self,
        ordinal: u32,
        fields: &[novarocks_sql::planning::dml::DmlWriteTargetField],
    ) {
        let fields = fields
            .iter()
            .map(|field| {
                novarocks_spi::connector::ConnectorWriteFieldBinding::try_new_typed(
                    field.token,
                    arrow::datatypes::Field::new(
                        &field.column.name,
                        field.column.data_type.clone(),
                        field.column.nullable,
                    ),
                    novarocks_type_contract::FunctionValueType::new(
                        field.column.data_type.clone(),
                        field.column.nullable,
                    ),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let input = match ordinal {
            0 => {
                assert_eq!(fields.len(), 4);
                novarocks_spi::connector::ConnectorWriteInputShape::RowLineage {
                    data_fields: fields[..2].to_vec(),
                    row_identity_fields: fields[2..].to_vec(),
                }
            }
            1 => {
                assert_eq!(fields.len(), 2);
                novarocks_spi::connector::ConnectorWriteInputShape::DeletionVector {
                    identity_fields: fields,
                    partition_source_fields: Vec::new(),
                }
            }
            _ => panic!("this scripted provider seals exactly two declared routes"),
        };
        input.validate().unwrap();
        self.targets.push(
            novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan::new(
                novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(ordinal)
                    .unwrap(),
                self.adapter.wrap_writer_handle(ordinal),
                input,
            ),
        );
    }
    fn target(
        &self,
        ordinal: u32,
    ) -> &novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan {
        self.targets
            .iter()
            .find(|target| target.ordinal().get() == ordinal)
            .expect("the original scripted target was sealed")
    }
    fn payload(&self, ordinal: u32) -> novarocks_spi::connector::ConnectorEncodedPayload {
        crate::query_execution::package_writes::WriteRecipeSession::writer_handle_payload(
            self,
            self.target(ordinal).handle(),
        )
        .unwrap()
    }
}
impl crate::query_execution::package_writes::WriteRecipeSession for MergeSealedRecipes {
    fn write_targets(&self) -> &[novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan] {
        &self.targets
    }
    fn writer_handle_payload(
        &self,
        handle: &novarocks_spi::connector::write_stack::ConnectorWriterHandle,
    ) -> Result<novarocks_spi::connector::ConnectorEncodedPayload, String> {
        let ordinal = *self
            .adapter
            .writer_handle(handle)
            .map_err(|error| error.to_string())?;
        Ok(novarocks_spi::connector::ConnectorEncodedPayload::new(
            novarocks_spi::connector::ConnectorEnvelopeHeader::new(
                handle.binding().descriptor().provider_id.clone(),
                handle.binding().catalog_handle().clone(),
                novarocks_spi::connector::ConnectorCodecCategory::WriteHandle,
                novarocks_spi::connector::ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            bytes::Bytes::copy_from_slice(&ordinal.to_le_bytes()),
        ))
    }
}
