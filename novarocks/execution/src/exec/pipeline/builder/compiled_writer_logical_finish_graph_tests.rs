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

// Included in the existing statistics module. Reuse its original physical
// writer, exact Theta requests, compiler, recorder and independent wire oracle.
mod logical_finish_graph {
    use super::*;
    use crate::runtime::fragment::io::exchange::fixture_stream;
    use novarocks_physical_plan as p;
    use novarocks_spi::connector::write_stack::RootRowKind;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const SUMMARY_EDGE: p::EdgeId = p::EdgeId::new(77);
    const SUMMARY_FINST: UniqueId = UniqueId::new(0x83, 0x03);

    /// Re-author only the physical consumer before freezing it again. Every
    /// original value, expression, call request, pool and result declaration
    /// retains its original owner; no LocalProgram is edited or reconstructed.
    fn logical_package(original: p::FragmentPackage) -> p::FragmentPackage {
        let fragment = original.fragment();
        let summary_node = p::NodeId::new(
            fragment
                .nodes()
                .keys()
                .map(|id| id.get())
                .max()
                .unwrap()
                .checked_add(1)
                .unwrap(),
        );
        let source = p::LogicalSummaryColumns {
            inserted: p::ValueId::new(901),
            replaced: p::ValueId::new(902),
            deleted: p::ValueId::new(903),
        };
        let mut builder = p::FragmentBuilder::new(fragment.id());
        for value in fragment.values().values() {
            builder.insert_value(value.clone()).unwrap();
        }
        for (_, expression) in fragment.expressions().iter() {
            builder.insert_expression(expression.clone()).unwrap();
        }
        let imports = source.values().map(|source_value| {
            builder
                .add_value(
                    ty(DataType::Int64, false),
                    p::ValueOrigin::ExchangeImport {
                        edge: SUMMARY_EDGE,
                        source_value,
                    },
                )
                .unwrap()
        });
        let destination = p::LogicalSummaryColumns {
            inserted: imports[0],
            replaced: imports[1],
            deleted: imports[2],
        };
        for node in fragment.nodes().values() {
            let mut node = node.clone();
            if node.id == fragment.root() {
                let p::NodeKind::TableFinish(spec) = &mut node.kind else {
                    panic!("the original statistics consumer root is TableFinish");
                };
                spec.count_mode = p::FinishCountMode::LogicalChanges;
                spec.inputs.logical_summary = Some(node.inputs.len() as u32);
                spec.logical_summary = Some(destination);
                let mut inputs = node.inputs.into_vec();
                inputs.push(summary_node);
                node.inputs = inputs.into();
                let mut required = node.required_inputs.into_vec();
                required.push(property(Distribution::Singleton));
                node.required_inputs = required.into();
            }
            builder.insert_node_unchecked(node).unwrap();
        }
        builder
            .add_exchange_source(
                summary_node,
                SUMMARY_EDGE,
                source
                    .values()
                    .into_iter()
                    .zip(destination.values())
                    .collect(),
                destination.values().into(),
                Distribution::Singleton,
                RowMultiplicity::SingleCopy,
            )
            .unwrap();
        for filter in fragment.runtime_filters() {
            builder.attach_runtime_filter(*filter).unwrap();
        }
        let fragment = builder
            .finish_definition(
                fragment.root(),
                fragment.sink().clone(),
                fragment.dop_domain(),
                fragment.stage_placement().clone(),
            )
            .unwrap()
            .with_call_requests_observed(
                fragment
                    .call_requests()
                    .entries()
                    .iter()
                    .map(|(definition, request)| (*definition, request.clone()))
                    .collect(),
                &FixtureControl,
            )
            .unwrap();
        let (uses, calls) = freeze(&fragment, &theta_catalog());
        let mut cuts = original.cuts().clone();
        let mut inbound = cuts.inbound.into_vec();
        inbound.push(p::InboundFragmentCut {
            edge: SUMMARY_EDGE,
            kind: p::EdgeKind::Stream,
            source_fragment: p::FragmentId::new(77),
            destination_node: summary_node,
            imports: source
                .values()
                .into_iter()
                .zip(destination.values())
                .map(|(source, destination)| p::CutImport {
                    source: p::CutValue {
                        value: source,
                        ty: ty(DataType::Int64, false),
                    },
                    destination,
                })
                .collect(),
            partitioning: p::EdgePartitioning {
                source: Distribution::Singleton,
                destination: Distribution::Singleton,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
            source_port: p::FragmentOutputPort::LogicalSummary,
            payload: p::ExchangePayloadContract::LogicalSummary(p::LogicalSummaryCut {
                source,
                destination,
            }),
            delivery: p::DeliveryRequirement::MustDrain,
        });
        cuts.inbound = inbound.into();
        p::FragmentPackage::try_new(
            p::FragmentPackageInput {
                constants: original.constants().clone(),
                version: original.version(),
                required: original.required(),
                fragment,
                expression_uses: uses,
                calls,
                pruning: original.pruning().clone(),
                cuts,
                result: original.result().cloned(),
                parameters: original.parameters().clone(),
                scans: original.scans().clone(),
                writes: original.writes().clone(),
                annotations: original.annotations().to_vec().into(),
            },
            admission(),
            &FixtureControl,
        )
        .expect("the independently checked logical finish package validates")
    }

    fn tiny_state() -> Arc<RuntimeState> {
        let runtime = Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
                    exchange_wait_ms: 5_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 64 * 1024 * 1024,
                    // Keep real partial/count/commit outputs in separate data frames.
                    exchange_max_transmit_batched_bytes: 1,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1,
                    local_exchange_max_buffered_rows: 1,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                theta_function_set(),
                crate::runtime::execution_runtime::test_memory_authority(),
            )
            .unwrap(),
        );
        Arc::new(RuntimeState::for_local_execution(
            Some(crate::runtime::query_options::QueryOptions {
                query_timeout: Some(5),
                ..Default::default()
            }),
            None,
            None,
            None,
            None,
            Some(crate::runtime::mem_tracker::MemTracker::new_root(
                "logical finish graph",
            )),
            Some(runtime),
        ))
    }

    /// Read-only transport observation; actual encoding/admission is still
    /// the original DataStreamSink -> Loopback -> receiver implementation.
    struct CountFrames {
        loopback: LoopbackTransmitter,
        data: Arc<AtomicUsize>,
    }
    impl ExchangeFrameTransmitter for CountFrames {
        fn transmit(&self, frame: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
            let data = !frame.eos && !frame.payload.is_empty();
            self.loopback.transmit(frame)?;
            if data {
                self.data.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        }
    }

    fn graph_case(writer_dop: usize, finish_dop: usize, summary_first: bool) {
        let rows = [(1, Some("a")), (2, None), (2, Some("b"))];
        let compiled =
            compiled_with_finish_package(0, &rows, writer_dop, finish_dop, logical_package);
        let (summary_id, summary_input) = compiled
            .finish
            .exchange_inputs()
            .iter()
            .find(|(_, input)| {
                input.role == novarocks_local_program::ProgramOutputRole::LogicalSummary
            })
            .unwrap();
        let summary_node = i32::try_from(summary_input.receiver_node).unwrap();
        assert_eq!(compiled.finish.exchange_inputs().len(), 2);
        assert_eq!(
            summary_input.delivery,
            novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
        );
        let summary_layout = compiled.finish.graph().nodes()[summary_id.index()].output_layout();
        let port = in_process_test_exchange_receiver_port();
        let creation = crate::runtime::fragment::exchange::test_receiver_creation_factory();
        let CompiledExchangeReceivers {
            registrations,
            bindings,
        } = materialize_compiled_exchange_receivers(
            &compiled.finish,
            FINISH_FINST,
            &ExchangeInputAssignments::new(BTreeMap::from([
                (
                    FragmentNodeId::new(i32::try_from(compiled.receiver.get()).unwrap()),
                    ExchangeInputAssignment::new(NonZeroUsize::MIN),
                ),
                (
                    FragmentNodeId::new(summary_node),
                    ExchangeInputAssignment::new(NonZeroUsize::MIN),
                ),
            ])),
            Arc::clone(&port),
            Some(&creation),
        )
        .unwrap();
        for registration in registrations {
            port.register(registration).unwrap();
        }
        let summary_key = ExchangeReceiverKey {
            fragment_instance_id: FINISH_FINST,
            node_id: summary_node,
        };
        let writer_key = ExchangeReceiverKey {
            fragment_instance_id: FINISH_FINST,
            node_id: i32::try_from(compiled.receiver.get()).unwrap(),
        };
        let summary_stream = fixture_stream(SUMMARY_FINST, FINISH_FINST, summary_node, 0);
        let writer_stream = fixture_stream(WRITER_FINST, FINISH_FINST, writer_key.node_id, 0);
        let state = tiny_state();
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let data_frames = Arc::new(AtomicUsize::new(0));
        let writer_sink = materialize_compiled_sink(
            &compiled.writer,
            &FragmentSinkAssignment::StreamDestinations {
                destinations: vec![
                    FragmentDestination::new(
                        FINISH_FINST,
                        RuntimeEndpoint::new("127.0.0.1", 9030).unwrap(),
                        WRITER_FINST,
                        0,
                        1,
                    )
                    .unwrap(),
                ],
                sender_id: None,
            },
            WRITER_FINST,
            Arc::new(CountFrames {
                loopback: LoopbackTransmitter {
                    port: Arc::clone(&port),
                },
                data: Arc::clone(&data_frames),
            }),
            None,
            None,
        )
        .unwrap();
        let writer = prepare_compiled_program_pipeline_execution_with_profiler(
            Arc::clone(&compiled.writer),
            Duration::from_millis(2),
            writer_sink,
            ExchangeBindings::default(),
            crate::exec::pipeline::binding::ScanBindings::default(),
            writer_bindings(&compiled.writer, &recorded),
            Some((WRITER_FINST.high(), WRITER_FINST.low())),
            None,
            writer_dop as i32,
            Arc::clone(&state),
            Arc::new(NoopFragmentEventSink),
        )
        .unwrap()
        .start();
        writer
            .join()
            .expect("the real provider writer and statistics outputs complete");
        assert!(writer.stopped_fact().is_some());
        assert!(
            data_frames.load(Ordering::Relaxed) >= 3,
            "the real aggregate partial must precede separate count and commit data frames"
        );
        assert_eq!(
            recorded
                .lock()
                .unwrap()
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            3
        );
        assert!(matches!(
            port.stream_close_facts(writer_key, &[writer_stream])
                .unwrap()[0]
                .disposition,
            novarocks_execution_contract::ExchangeCloseDisposition::Withdrawn
        ));

        let summary_schema = creation
            .create_compiled_chunk_schema(summary_layout)
            .unwrap();
        let send_summary = || {
            let counts = novarocks_local_program::LogicalChangeCounts::try_new(2, 3, 4).unwrap();
            let chunk = crate::exec::operators::logical_change_summary::summary_chunk(
                counts,
                &summary_schema,
                creation.as_ref(),
            )
            .unwrap();
            let payload = crate::runtime::exchange::encode_chunks(&[chunk], true).unwrap();
            port.push(
                summary_key,
                ExchangeReceiverFrame {
                    stream: summary_stream,
                    source_fragment_instance_id: SUMMARY_FINST,
                    sender_ordinal: 0,
                    sender_count: 1,
                    sender_id: 0,
                    backend_number: 0,
                    sequence: 0,
                    eos: true,
                    payload,
                },
            )
            .unwrap();
        };
        if summary_first {
            send_summary();
        }
        let mut finish_bindings = CompiledWriterBindings::default();
        finish_bindings
            .bind_finish(
                root_of(&compiled.finish),
                TableFinishRuntimeBinding::new(Arc::new(AcceptCarriers))
                    .with_logical_summary(summary_node, Arc::from([summary_stream.source()]))
                    .unwrap(),
            )
            .unwrap();
        let output = ResultSinkHandle::new();
        let prepared = prepare_compiled_program_pipeline_execution_with_profiler(
            Arc::clone(&compiled.finish),
            Duration::from_millis(2),
            Box::new(ResultSinkFactory::new(output.clone())),
            bindings,
            crate::exec::pipeline::binding::ScanBindings::default(),
            finish_bindings,
            Some((FINISH_FINST.high(), FINISH_FINST.low())),
            None,
            finish_dop as i32,
            Arc::clone(&state),
            Arc::new(NoopFragmentEventSink),
        )
        .unwrap();
        assert_eq!(
            prepared.driver_count(),
            finish_dop + 1,
            "writer ExchangeSource drivers plus the one logical finish; no second summary ExchangeSource pipeline"
        );
        assert_eq!(prepared.submitted_driver_count(), 0);
        let running = prepared.start();
        assert_eq!(running.submitted_driver_count(), finish_dop + 1);
        std::thread::scope(|scope| {
            let sender = scope.spawn(|| {
                if !summary_first {
                    let deadline = Instant::now() + Duration::from_secs(5);
                    loop {
                        if matches!(port.stream_close_facts(writer_key, &[writer_stream]).unwrap()[0].disposition,
                            novarocks_execution_contract::ExchangeCloseDisposition::Consumed { .. }) {
                            break;
                        }
                        assert!(Instant::now() < deadline,
                            "writer input must drain under its tiny runtime waterline before summary exists");
                        std::thread::yield_now();
                    }
                    send_summary();
                }
            });
            running
                .join()
                .expect("the original batch fairly consumes both finish inputs");
            sender.join().unwrap();
        });
        assert!(running.stopped_fact().is_some());
        for (key, stream) in [(writer_key, writer_stream), (summary_key, summary_stream)] {
            assert!(
                matches!(
                    port.stream_close_facts(key, &[stream]).unwrap()[0].disposition,
                    novarocks_execution_contract::ExchangeCloseDisposition::Consumed { .. }
                ),
                "only the actual input owners author normal consumption"
            );
        }
        let chunks = output.take_chunks();
        let mut counts = Vec::new();
        let mut prepared_fragments = 0;
        for chunk in &chunks {
            let kind = chunk.columns()[0]
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            let count = chunk.columns()[2]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..chunk.len() {
                if kind.value(row) == RootRowKind::Summary.to_wire() {
                    counts.push(count.value(row));
                }
                if kind.value(row) == RootRowKind::PreparedFragment.to_wire() {
                    prepared_fragments += 1;
                }
            }
        }
        assert_eq!(
            counts,
            [9],
            "semantic 2+3+4 is independent of the real writer's three rows"
        );
        assert_eq!(prepared_fragments, 1);
        let artifacts = bodies(&chunks);
        assert_eq!(artifacts.len(), 2);
        assert_eq!(estimate_compact_theta(&artifacts[0].2).unwrap(), 2.0);
        assert_eq!(estimate_compact_theta(&artifacts[1].2).unwrap(), 2.0);
        drop(running);
        drop(writer);
        port.remove(writer_key);
        port.remove(summary_key);
    }

    #[test]
    fn compiled_logical_finish_graph_consumes_early_real_statistics_before_summary() {
        for (writer_dop, finish_dop) in [(1, 1), (1, 4), (4, 1)] {
            graph_case(writer_dop, finish_dop, false);
        }
    }

    #[test]
    fn compiled_logical_finish_graph_accepts_summary_first_without_skipping_real_writer_results() {
        graph_case(1, 4, true);
    }
}
