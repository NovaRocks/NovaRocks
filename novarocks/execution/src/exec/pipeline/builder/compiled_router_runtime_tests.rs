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

// Included inside compiled_exchange_tests: reuse its actual compiler,
// function subset, runtime, Loopback transport and receiver creation owners.
mod router_runtime_tests {
    use super::*;
    use crate::exec::pipeline::binding::ScanBindings;
    use crate::exec::pipeline::executor::prepare_compiled_program_with_sink_materializer;
    use crate::runtime::exchange::{ExchangeColumnBinding, ExchangePopResult};
    use crate::runtime::fragment::io::ExchangeReceiverRegistration;
    use crate::runtime::fragment::sink::materialize_compiled_sink_for_pipeline;
    use novarocks_physical_plan as p;
    use novarocks_spi::connector::{
        ConnectorRowMutationEffect as Effect, ConnectorWriteFieldToken, ConnectorWriteRouteId,
    };

    const SOURCE: UniqueId = UniqueId::new(0x761, 1);
    const UPSTREAM: UniqueId = UniqueId::new(0x760, 1);
    const INPUT_EDGE: p::EdgeId = p::EdgeId::new(70);
    const UPSTREAM_FRAGMENT: p::FragmentId = p::FragmentId::new(60);

    /// A complete independently checked source package. Peer identities and
    /// the exact provider/summary payload proofs are in outbound cuts; no
    /// provider writer, LocalProgram or package bytes are manufactured.
    fn source_package(rows: &[i64]) -> p::FragmentPackage {
        source_package_with_input(rows, None).0
    }

    /// The exchange alternative keeps the same original expand/Router author;
    /// only its physical source and precise checked inbound cut differ.
    fn source_package_with_input(
        rows: &[i64],
        upstream_value: Option<p::ValueId>,
    ) -> (p::FragmentPackage, p::NodeId, p::ValueId) {
        let fragment_id = p::FragmentId::new(61);
        let mut builder = p::FragmentBuilder::new(fragment_id);
        let values = builder.reserve_node_id().unwrap();
        let input = builder
            .add_value(
                int64(),
                upstream_value.map_or(
                    p::ValueOrigin::NodeOutput {
                        node: values,
                        output_ordinal: 0,
                    },
                    |source_value| p::ValueOrigin::ExchangeImport {
                        edge: INPUT_EDGE,
                        source_value,
                    },
                ),
            )
            .unwrap();
        let mut inbound = Vec::new();
        if let Some(upstream_value) = upstream_value {
            builder
                .add_exchange_source(
                    values,
                    INPUT_EDGE,
                    Box::from([(upstream_value, input)]),
                    Box::from([input]),
                    p::Distribution::RoundRobin,
                    p::RowMultiplicity::SingleCopy,
                )
                .unwrap();
            inbound.push(p::InboundFragmentCut {
                edge: INPUT_EDGE,
                kind: p::EdgeKind::Stream,
                source_fragment: UPSTREAM_FRAGMENT,
                destination_node: values,
                imports: Box::from([p::CutImport {
                    source: p::CutValue {
                        value: upstream_value,
                        ty: int64(),
                    },
                    destination: input,
                }]),
                partitioning: input_partitioning(),
                source_port: p::FragmentOutputPort::Root,
                payload: p::ExchangePayloadContract::Relational,
                delivery: p::DeliveryRequirement::MustDrain,
            });
        } else {
            let cells = rows
                .iter()
                .map(|&value| -> Box<[p::ExprId]> {
                    Box::from([builder
                        .add_expression(
                            values,
                            int64(),
                            p::ExprKind::Literal(p::LiteralValue::Int64(value)),
                        )
                        .unwrap()])
                })
                .collect::<Box<[_]>>();
            builder
                .add_values(values, cells, Box::from([input]))
                .unwrap();
        }
        let expand = builder.reserve_node_id().unwrap();
        let effect = builder
            .add_value(
                p::ValueType::new(DataType::Int8, false),
                p::ValueOrigin::NodeOutput {
                    node: expand,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        let data = builder
            .add_value(
                int64(),
                p::ValueOrigin::NodeOutput {
                    node: expand,
                    output_ordinal: 1,
                },
            )
            .unwrap();
        let assignment = builder
            .add_expression(expand, int64(), p::ExprKind::Value(input))
            .unwrap();
        builder
            .add_row_consuming(
                expand,
                Box::from([values]),
                p::RequiredInputs::AsProduced,
                p::Distribution::Unconstrained,
                Box::from([effect, data]),
                p::NodeKind::ChangeEventExpand {
                    effect_output: effect,
                    events: [Effect::Insert, Effect::Replace, Effect::Delete]
                        .map(|effect| p::ChangeEventSpec {
                            predicate: None,
                            effect,
                            assignments: Box::from([(data, Some(assignment))]),
                        })
                        .into(),
                },
            )
            .unwrap();
        let mut counter = |field| {
            builder
                .add_value(int64(), p::ValueOrigin::LogicalSummary { field })
                .unwrap()
        };
        let summary_columns = p::LogicalSummaryColumns {
            inserted: counter(p::LogicalChangeCountField::Inserted),
            replaced: counter(p::LogicalChangeCountField::Replaced),
            deleted: counter(p::LogicalChangeCountField::Deleted),
        };
        let summary_edge = p::EdgeId::new(73);
        let ordinals = [
            p::WriteTargetOrdinal::try_new(0).unwrap(),
            p::WriteTargetOrdinal::try_new(1).unwrap(),
        ];
        let routes = (0..2)
            .map(|index| p::ChangeStreamRoute {
                route_id: ConnectorWriteRouteId::from_bytes([index as u8 + 1; 32]),
                write_target_ordinal: ordinals[index],
                accepted_effects: if index == 0 {
                    Box::from([Effect::Insert, Effect::Replace])
                } else {
                    Box::from([Effect::Replace, Effect::Delete])
                },
                input_mapping: Box::from([(
                    ConnectorWriteFieldToken::from_bytes([index as u8 + 1; 32]),
                    data,
                )]),
                requirements: p::WriteRouteRequirements {
                    colocation: p::WriteColocation::None,
                    layout: p::WriteLayoutPreference::None,
                },
                edge: p::EdgeId::new(71 + index as u32),
            })
            .collect::<Vec<_>>();
        let multiplicity = p::RowMultiplicity::SingleCopy;
        let mut outbound = routes
            .iter()
            .enumerate()
            .map(|(index, route)| {
                let destination = p::ValueId::new(101 + index as u32);
                let projected = p::CutValue {
                    value: data,
                    ty: int64(),
                };
                p::OutboundFragmentCut {
                    edge: route.edge,
                    kind: p::EdgeKind::ChangeStreamRouter,
                    destination_fragment: p::FragmentId::new(62 + index as u32),
                    destination_node: p::NodeId::new(101 + index as u32),
                    projection: Box::from([projected.clone()]),
                    destination_imports: Box::from([p::CutImport {
                        source: projected,
                        destination,
                    }]),
                    partitioning: p::EdgePartitioning {
                        source: p::Distribution::RoundRobin,
                        destination: p::Distribution::RoundRobin,
                        source_multiplicity: multiplicity,
                        destination_multiplicity: multiplicity,
                    },
                    source_port: p::FragmentOutputPort::Root,
                    payload: p::ExchangePayloadContract::ProviderRoute(p::ChangeStreamWriterCut {
                        route_id: route.route_id,
                        write_target_ordinal: route.write_target_ordinal,
                        fields: Box::from([p::ChangeStreamWriterCutField {
                            token: route.input_mapping[0].0,
                            source: data,
                            destination,
                        }]),
                    }),
                    delivery: p::DeliveryRequirement::MustDrain,
                }
            })
            .collect::<Vec<_>>();
        let destinations = p::LogicalSummaryColumns {
            inserted: p::ValueId::new(103),
            replaced: p::ValueId::new(104),
            deleted: p::ValueId::new(105),
        };
        let projection = summary_columns
            .values()
            .map(|value| p::CutValue { value, ty: int64() });
        outbound.push(p::OutboundFragmentCut {
            edge: summary_edge,
            kind: p::EdgeKind::Stream,
            destination_fragment: p::FragmentId::new(64),
            destination_node: p::NodeId::new(103),
            projection: projection.clone().into(),
            destination_imports: projection
                .into_iter()
                .zip(destinations.values())
                .map(|(source, destination)| p::CutImport {
                    source,
                    destination,
                })
                .collect(),
            partitioning: p::EdgePartitioning {
                source: p::Distribution::Singleton,
                destination: p::Distribution::Singleton,
                source_multiplicity: multiplicity,
                destination_multiplicity: multiplicity,
            },
            source_port: p::FragmentOutputPort::LogicalSummary,
            payload: p::ExchangePayloadContract::LogicalSummary(p::LogicalSummaryCut {
                source: summary_columns,
                destination: destinations,
            }),
            delivery: p::DeliveryRequirement::MustDrain,
        });
        let fragment = builder
            .finish_definition(
                expand,
                p::FragmentSink::Router {
                    effect,
                    routes: routes.into(),
                    summary: p::LogicalSummaryOutput {
                        edge: summary_edge,
                        columns: summary_columns,
                    },
                },
                p::PipelineDopDomain {
                    min: 1,
                    max: 4,
                    requires_power_of_two: false,
                },
                p::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        let uses = root_uses(&fragment);
        let calls =
            p::FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &FixtureControl).unwrap();
        let package = p::FragmentPackage::try_new(
            p::FragmentPackageInput {
                constants: p::ConstantPools::empty(),
                version: p::PlanVersionId::try_new([61; 16]).unwrap(),
                required: p::RequiredContracts::default(),
                fragment,
                expression_uses: uses,
                calls,
                pruning: p::FrozenFragmentPruning::try_new(fragment_id, vec![], &FixtureControl)
                    .unwrap(),
                cuts: p::FragmentCuts {
                    inbound: inbound.into(),
                    outbound: outbound.into(),
                    ..p::FragmentCuts::default()
                },
                result: None,
                parameters: novarocks_type_contract::SemanticParameters::default(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            admission(),
            &FixtureControl,
        )
        .expect("the exact Router source package and inbound/outbound proofs validate");
        (package, values, input)
    }

    fn compile_source(rows: &[i64]) -> Arc<LocalProgram> {
        compile_source_package(source_package(rows))
    }

    fn compile_source_package(package: p::FragmentPackage) -> Arc<LocalProgram> {
        let providers =
            PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
                .unwrap();
        let validated =
            validate_fragment_providers(Arc::new(package), &providers, &FixtureControl).unwrap();
        let mut config = options(None);
        config.pipeline_dop = NonZeroUsize::new(4).unwrap();
        Arc::new(
            compile_fragment(
                validated,
                &crate::exec::expr::compiled_program::tests::rng_subset(),
                config,
                &FixtureControl,
            )
            .unwrap(),
        )
    }

    fn input_partitioning() -> p::EdgePartitioning {
        p::EdgePartitioning {
            source: p::Distribution::RoundRobin,
            destination: p::Distribution::RoundRobin,
            source_multiplicity: p::RowMultiplicity::SingleCopy,
            destination_multiplicity: p::RowMultiplicity::SingleCopy,
        }
    }

    /// Create the real sender fragment first and return its actual value id.
    /// Its cut is sealed after the Router constructor returns the exact import.
    fn upstream_fragment(rows: &[i64]) -> (p::Fragment, p::ValueId) {
        let mut builder = p::FragmentBuilder::new(UPSTREAM_FRAGMENT);
        let values = builder.reserve_node_id().unwrap();
        let value = builder
            .add_value(
                int64(),
                p::ValueOrigin::NodeOutput {
                    node: values,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        let cells = rows
            .iter()
            .map(|value| -> Box<[p::ExprId]> {
                Box::from([builder
                    .add_expression(
                        values,
                        int64(),
                        p::ExprKind::Literal(p::LiteralValue::Int64(*value)),
                    )
                    .unwrap()])
            })
            .collect::<Box<[_]>>();
        builder
            .add_values(values, cells, Box::from([value]))
            .unwrap();
        let fragment = builder
            .finish_definition(
                values,
                p::FragmentSink::Stream { edge: INPUT_EDGE },
                dop(),
                p::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        (fragment, value)
    }

    fn upstream_package(
        fragment: p::Fragment,
        source: p::ValueId,
        receiver: p::NodeId,
        destination: p::ValueId,
    ) -> p::FragmentPackage {
        let fragment_id = fragment.id();
        let uses = root_uses(&fragment);
        let calls =
            p::FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &FixtureControl).unwrap();
        let projected = p::CutValue {
            value: source,
            ty: int64(),
        };
        p::FragmentPackage::try_new(
            p::FragmentPackageInput {
                constants: p::ConstantPools::empty(),
                version: p::PlanVersionId::try_new([60; 16]).unwrap(),
                required: p::RequiredContracts::default(),
                fragment,
                expression_uses: uses,
                calls,
                pruning: p::FrozenFragmentPruning::try_new(fragment_id, vec![], &FixtureControl)
                    .unwrap(),
                cuts: p::FragmentCuts {
                    outbound: Box::from([p::OutboundFragmentCut {
                        edge: INPUT_EDGE,
                        kind: p::EdgeKind::Stream,
                        destination_fragment: p::FragmentId::new(61),
                        destination_node: receiver,
                        projection: Box::from([projected.clone()]),
                        destination_imports: Box::from([p::CutImport {
                            source: projected,
                            destination,
                        }]),
                        partitioning: input_partitioning(),
                        source_port: p::FragmentOutputPort::Root,
                        payload: p::ExchangePayloadContract::Relational,
                        delivery: p::DeliveryRequirement::MustDrain,
                    }]),
                    ..p::FragmentCuts::default()
                },
                result: None,
                parameters: novarocks_type_contract::SemanticParameters::default(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            admission(),
            &FixtureControl,
        )
        .expect("the original upstream Values->Stream package validates")
    }

    /// Pure fixture metadata scaffolds use the constructor's actual request
    /// callback and retain the source's original value bindings/storage. This
    /// numerical invoice is not a runtime capacity grant or a schema rebuild.
    fn receiver_projection(
        source: &novarocks_local_program::StaticLayout,
        slots: &[novarocks_types::SlotId],
    ) -> novarocks_local_program::StaticLayout {
        let mut requested_bytes = 0_usize;
        let projected = source
            .project_by_slots_typed_for_compile(
                slots,
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                |layout| {
                    requested_bytes = requested_bytes
                        .checked_add(layout.size())
                        .ok_or(CompileControlError::ResourceExhausted)?;
                    Ok(())
                },
                &FixtureControl,
            )
            .unwrap();
        for slot in slots {
            assert!(
                projected
                    .value_binding(*slot)
                    .unwrap()
                    .value_type()
                    .shares_storage(source.value_binding(*slot).unwrap().value_type())
            );
        }
        projected
    }

    fn source_runtime_case(rows: &[i64]) {
        source_runtime_case_with_input(rows, false);
    }

    fn source_runtime_case_with_input(rows: &[i64], from_exchange: bool) {
        let (program, upstream) = if from_exchange {
            let (fragment, source_value) = upstream_fragment(rows);
            let (package, receiver, imported) = source_package_with_input(&[], Some(source_value));
            let upstream = compile(
                upstream_package(fragment, source_value, receiver, imported),
                None,
            );
            (compile_source_package(package), Some((upstream, receiver)))
        } else {
            (compile_source(rows), None)
        };
        let actual_width = if from_exchange { 4 } else { 1 };
        assert_eq!(program.graph().profile().pipeline_dop().get(), 4);
        assert!(program.graph().profile().root_sink_dop().is_none());
        if from_exchange {
            let (id, input) = program.exchange_inputs().iter().next().unwrap();
            assert_eq!(
                (input.edge, input.source_fragment),
                (INPUT_EDGE.get(), UPSTREAM_FRAGMENT.get())
            );
            assert_eq!(
                input.role,
                novarocks_local_program::ProgramOutputRole::Relational
            );
            assert_eq!(
                input.delivery,
                novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
            );
            assert_eq!(
                input.routing,
                novarocks_local_program::CompiledExchangeRouting::RoundRobin
            );
            assert!(matches!(
                program.graph().nodes()[id.index()].kind(),
                ProgramNodeKind::ExchangeSource { .. }
            ));
        }
        let Some(StaticSinkProgram::ChangeStreamRouter {
            branches,
            summary_branch,
            summary_layout,
            ..
        }) = program.graph().sink()
        else {
            panic!("the original compiler must lower the checked Router")
        };
        assert_eq!(branches.len(), 3);
        assert_eq!(
            *summary_branch, 2,
            "this compiler author keeps its independent summary last"
        );
        let port = in_process_test_exchange_receiver_port();
        let creation = crate::runtime::fragment::exchange::test_receiver_creation_factory();
        let mut keys = Vec::new();
        let mut groups = Vec::new();
        for (index, branch) in branches.iter().enumerate() {
            let instance = UniqueId::new(0x762, index as i64 + 1);
            let key = ExchangeReceiverKey {
                fragment_instance_id: instance,
                node_id: branch.dest_node_id(),
            };
            let layout = if index == *summary_branch as usize {
                summary_layout.clone()
            } else {
                receiver_projection(
                    program.graph().nodes()[program.graph().root().index()].output_layout(),
                    branch.output_columns(),
                )
            };
            port.register(ExchangeReceiverRegistration {
                key,
                expected_senders: 1,
                expected_chunk_schema: creation.create_compiled_chunk_schema(&layout).unwrap(),
                column_binding: ExchangeColumnBinding::Positional,
                creation_factory: Arc::clone(&creation),
            })
            .unwrap();
            groups.push(vec![
                FragmentDestination::new(
                    instance,
                    RuntimeEndpoint::new("127.0.0.1", 9030).unwrap(),
                    SOURCE,
                    0,
                    1,
                )
                .unwrap(),
            ]);
            keys.push(key);
        }
        let assignment = FragmentSinkAssignment::DestinationGroups {
            groups,
            sender_id: None,
        };
        let baseline = runtime_state();
        let state = Arc::new(RuntimeState::for_local_execution(
            Some(crate::runtime::query_options::QueryOptions {
                query_timeout: Some(5),
                ..Default::default()
            }),
            None,
            None,
            None,
            None,
            baseline.mem_tracker(),
            Some(crate::runtime::execution_runtime::test_execution_runtime()),
        ));
        let (bindings, input_key) = if let Some((upstream, receiver)) = upstream {
            assert_eq!(program.exchange_inputs().len(), 1);
            let materialized = materialize_compiled_exchange_receivers(
                &program,
                SOURCE,
                &assignments(receiver),
                Arc::clone(&port),
                Some(&creation),
            )
            .expect("the Router binds its original checked ExchangeSource input");
            let input_key = ExchangeReceiverKey {
                fragment_instance_id: SOURCE,
                node_id: i32::try_from(receiver.get()).unwrap(),
            };
            for registration in materialized.registrations {
                port.register(registration).unwrap();
            }
            let assignment = FragmentSinkAssignment::StreamDestinations {
                destinations: vec![
                    FragmentDestination::new(
                        SOURCE,
                        RuntimeEndpoint::new("127.0.0.1", 9030).unwrap(),
                        UPSTREAM,
                        0,
                        1,
                    )
                    .unwrap(),
                ],
                sender_id: None,
            };
            let sink = materialize_compiled_sink(
                &upstream,
                &assignment,
                UPSTREAM,
                Arc::new(LoopbackTransmitter {
                    port: Arc::clone(&port),
                }),
                None,
                None,
            )
            .unwrap();
            // The sender encodes and delivers real DATA/EOS before the source
            // runs. Merely accepting that End cannot author Consumed.
            run(&upstream, sink, ExchangeBindings::default(), UPSTREAM);
            let stream = crate::runtime::fragment::io::exchange::fixture_stream(
                UPSTREAM,
                SOURCE,
                input_key.node_id,
                0,
            );
            assert!(
                matches!(
                    port.stream_close_facts(input_key, &[stream]).unwrap()[0].disposition,
                    novarocks_execution_contract::ExchangeCloseDisposition::Withdrawn,
                ),
                "admitted DATA/EOS without the real source is not Consumed"
            );
            (materialized.bindings, Some((input_key, stream)))
        } else {
            (ExchangeBindings::default(), None)
        };
        let observed_width = std::cell::Cell::new(None);
        let transmitter: Arc<dyn ExchangeFrameTransmitter> = Arc::new(LoopbackTransmitter {
            port: Arc::clone(&port),
        });
        let prepared = prepare_compiled_program_with_sink_materializer(
            Arc::clone(&program),
            Duration::from_millis(10),
            |actual| {
                assert_eq!(
                    observed_width.replace(Some(actual.get())),
                    None,
                    "one synchronous materialization"
                );
                materialize_compiled_sink_for_pipeline(
                    &program,
                    &assignment,
                    SOURCE,
                    transmitter,
                    None,
                    None,
                    actual,
                    state.mem_tracker(),
                )
            },
            bindings,
            ScanBindings::default(),
            crate::runtime::fragment::CompiledWriterBindings::default(),
            finst(SOURCE),
            None,
            4,
            Arc::clone(&state),
            Arc::new(NoopFragmentEventSink),
            None,
        )
        .expect("the actual computing graph binds its Router and independent summary together");
        assert_eq!(
            observed_width.get(),
            Some(actual_width),
            "the graph's original source owns the actual contributor width"
        );
        assert_eq!(
            prepared.driver_count(),
            actual_width + 1,
            "every real computing driver and exactly one summary driver share this batch"
        );
        assert_eq!(prepared.submitted_driver_count(), 0);
        let running = prepared.start();
        assert_eq!(running.submitted_driver_count(), actual_width + 1);
        running
            .join()
            .expect("the original batch must finish all computing and summary drivers");
        assert!(running.stopped_fact().is_some());
        if let Some((key, stream)) = input_key {
            // No explicit test-side completion: ExchangeSource itself must
            // have consumed DATA and End and authored its original fact.
            assert!(matches!(
                port.stream_close_facts(key, &[stream]).unwrap()[0].disposition,
                novarocks_execution_contract::ExchangeCloseDisposition::Consumed { .. },
            ));
        }
        let mut outputs = Vec::new();
        for key in &keys {
            let stream = crate::runtime::fragment::io::exchange::fixture_stream(
                SOURCE,
                key.fragment_instance_id,
                key.node_id,
                0,
            );
            assert!(
                matches!(
                    port.stream_close_facts(*key, &[stream]).unwrap()[0].disposition,
                    novarocks_execution_contract::ExchangeCloseDisposition::Withdrawn,
                ),
                "the source Task join does not consume downstream output"
            );
            let handle = port.receiver_handle(*key, 1).unwrap();
            let mut chunks = Vec::new();
            while let ExchangePopResult::Chunk(chunk) = handle
                .try_pop_next_with_stats(1)
                .unwrap()
                .expect("joined sender must have delivered data and EOS")
            {
                chunks.push(chunk);
            }
            assert!(
                matches!(
                    port.stream_close_facts(*key, &[stream]).unwrap()[0].disposition,
                    novarocks_execution_contract::ExchangeCloseDisposition::Withdrawn,
                ),
                "reading DATA and End alone does not forge the input owner's completion"
            );
            handle.complete_input_consumption(1).unwrap();
            let close = port.stream_close_facts(*key, &[stream]).unwrap();
            assert!(matches!(
                close[0].disposition,
                novarocks_execution_contract::ExchangeCloseDisposition::Consumed { .. }
            ));
            outputs.push(chunks);
        }
        let mut expected = rows
            .iter()
            .flat_map(|value| [*value, *value])
            .collect::<Vec<_>>();
        expected.sort();
        for provider in &outputs[..2] {
            let mut actual = provider
                .iter()
                .flat_map(|chunk| int64_column(chunk, 0))
                .collect::<Vec<_>>();
            actual.sort();
            assert_eq!(actual, expected);
        }
        let summary = &outputs[2];
        assert_eq!(
            summary.iter().map(Chunk::len).sum::<usize>(),
            1,
            "each Task sends one summary, including an empty source"
        );
        let actual = summary
            .iter()
            .flat_map(|chunk| {
                (0..chunk.len()).map(move |row| {
                    (0..3)
                        .map(|column| {
                            chunk.columns()[column]
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(row)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![vec![rows.len() as i64; 3]]);
        drop(running);
        for key in keys {
            port.remove(key);
        }
        if let Some((key, _)) = input_key {
            port.remove(key);
        }
    }

    #[test]
    fn compiled_router_values_profile_four_uses_actual_root_one_and_one_summary_driver() {
        source_runtime_case(&[7, 11]);
    }
    #[test]
    fn compiled_router_empty_values_emits_one_zero_summary_through_the_original_batch() {
        source_runtime_case(&[]);
    }

    #[test]
    fn compiled_router_exchange_uses_four_real_contributors_and_one_summary_driver() {
        source_runtime_case_with_input(&[7, 11, 19, 23, 31, 41, 47, 53], true);
    }

    #[test]
    fn compiled_router_empty_exchange_finishes_all_four_contributors_with_one_zero_summary() {
        source_runtime_case_with_input(&[], true);
    }
}
