// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Complete endpoints run on actual checked pipelines after their original
//! local lifecycle decisions. The Partial fixture cannot prove final placement
//! or partition width. Literal group results and recorded public port requests
//! are independent oracles. This component owns no external service/process.

use super::super::super::super::{Event, session_for};
use super::*;
use arrow::array::ArrayRef;

#[derive(Clone, Copy, Debug)]
enum CompleteMode {
    Single,
    ZeroCalls,
    FinalMerge,
}

struct CompleteFixture {
    program: Arc<LocalProgram>,
    producer: ProgramNodeId,
    input: Chunk,
}

fn complete_fixture(mode: CompleteMode, dop: usize) -> CompleteFixture {
    complete_fixture_with_payload(mode, dop, int64(true))
}

fn complete_fixture_with_payload(
    mode: CompleteMode,
    dop: usize,
    payload: FunctionValueType,
) -> CompleteFixture {
    complete_fixture_with_types(
        mode,
        dop,
        FunctionValueType::new(DataType::Utf8, true),
        payload,
    )
}

fn complete_fixture_with_types(
    mode: CompleteMode,
    dop: usize,
    key_type: FunctionValueType,
    payload: FunctionValueType,
) -> CompleteFixture {
    assert!(matches!(mode, CompleteMode::ZeroCalls) || payload == int64(true));
    let catalog = aggregate_catalog(&[
        ("count", PureKernelAbi::AggregateWindowV1),
        ("sum", PureKernelAbi::AggregateWindowV1),
        ("min", PureKernelAbi::AggregateWindowV1),
        ("max", PureKernelAbi::AggregateWindowV1),
    ]);
    let bounds = ["count", "sum", "min", "max"]
        .iter()
        .map(|name| {
            let arguments = if *name == "count" {
                vec![]
            } else {
                vec![int64(true)]
            };
            bind(&catalog, name, &arguments)
        })
        .collect::<Vec<_>>();
    let types = [key_type, payload.clone()];
    let fragment = FragmentId::new(1);
    let mut builder = FragmentBuilder::new(fragment);
    let source = builder.reserve_node_id().unwrap();
    let (columns, read) = crate::runtime::task::scan::compiled_fixture::add_typed_scan(
        &mut builder,
        source,
        &types,
        vec![novarocks_connector_contract::StaticScanDynamicFilter::new(
            FILTER.get(),
            Arc::from("v0"),
        )],
    );
    let sequence = |ordinal: usize| AggregateSequenceId::new(41 + ordinal as u32);
    let (input, calls) = match mode {
        CompleteMode::ZeroCalls => (source, vec![]),
        CompleteMode::Single => (
            source,
            bounds
                .iter()
                .enumerate()
                .map(|(ordinal, bound)| CallSpec {
                    bound,
                    phase: AggregatePhase::Single,
                    id: AggregateCallId::new(10 + ordinal as u32),
                    arguments: if ordinal == 0 {
                        vec![]
                    } else {
                        vec![columns[1]]
                    },
                    distinct: false,
                })
                .collect(),
        ),
        CompleteMode::FinalMerge => {
            let calls = bounds
                .iter()
                .enumerate()
                .map(|(ordinal, bound)| CallSpec {
                    bound,
                    phase: AggregatePhase::Partial {
                        sequence: sequence(ordinal),
                    },
                    id: AggregateCallId::new(10 + ordinal as u32),
                    arguments: if ordinal == 0 {
                        vec![]
                    } else {
                        vec![columns[1]]
                    },
                    distinct: false,
                })
                .collect::<Vec<_>>();
            let (node, states) = add_aggregate(
                &mut builder,
                source,
                &columns[..1],
                &calls,
                AggregateGrouping::Partial,
            );
            (
                node,
                bounds
                    .iter()
                    .enumerate()
                    .map(|(ordinal, bound)| CallSpec {
                        bound,
                        phase: AggregatePhase::Final {
                            sequence: sequence(ordinal),
                        },
                        id: AggregateCallId::new(20 + ordinal as u32),
                        arguments: vec![states[ordinal + 1]],
                        distinct: false,
                    })
                    .collect(),
            )
        }
    };
    let (producer, _) = add_aggregate(
        &mut builder,
        input,
        &columns[..1],
        &calls,
        AggregateGrouping::Complete,
    );
    let topn = builder.reserve_node_id().unwrap();
    let key = builder
        .add_expression(topn, types[0].clone(), ExprKind::Value(columns[0]))
        .unwrap();
    builder
        .add_top_n(
            topn,
            producer,
            Box::from([SortExpr {
                expr: key,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::First,
            }]),
            2,
            0,
            TopNPhase::Single,
        )
        .unwrap();
    builder.attach_runtime_filter(FILTER).unwrap();
    let definition = finish(builder, topn, FragmentSink::Result, 2);
    let output = definition.nodes()[&topn].output.clone();
    let fields = output
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, value)| ResultField {
            domain: crate::test_result_domain::result_value_domain(&definition.values()[value].ty),
            name: format!("field-{ordinal}").into(),
            alias: None,
            value: *value,
            ty: definition.values()[value].ty.clone(),
        })
        .collect();
    let mut filter = physical_filter(
        fragment,
        producer,
        source,
        topn,
        TopNPhase::Single,
        columns[0],
        types[0].clone(),
        SortDirection::Ascending,
        NullOrdering::First,
        2,
    );
    if matches!(mode, CompleteMode::FinalMerge) {
        filter.consumers[0].target = RuntimeFilterConsumerTarget::AggregateTopNScanField {
            producer: filter.producers[0].witness,
            lineage: Box::from([RuntimeFilterLineageStep::AggregateGroupKey {
                fragment,
                node: input,
                group_key_ordinal: 0,
            }]),
        };
    }
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([122; 16]).unwrap());
    plan.add_fragment(definition).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment,
        output,
        fields,
    })
    .unwrap();
    plan.add_runtime_filter(filter).unwrap();
    let plan = plan.finish_observed(&FixtureControl).unwrap();
    let package = packages_with_reads(
        &plan,
        &catalog,
        &BTreeMap::from([(ProviderReadOccurrenceId::new(0), read)]),
    )
    .remove(&fragment)
    .unwrap();
    let program = compile_with_providers(
        package,
        &catalog,
        dop,
        true,
        &crate::runtime::task::scan::compiled_fixture::providers(),
    );
    let producer = program.graph().nodes().iter().enumerate().find_map(|(id, node)| {
        matches!(node.kind(), ProgramNodeKind::Aggregate { topn_filters, .. } if !topn_filters.is_empty())
            .then_some(ProgramNodeId::new(id))
    }).unwrap();
    let (scan, _) = program.scan_inputs().iter().next().unwrap();
    let node = &program.graph().nodes()[scan.index()];
    let numbers = [30_i64, 10, 3, 7, 20, 2, 40, 1];
    let payload: ArrayRef = if payload.logical_type() == ValueLogicalType::LargeInt {
        Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                numbers.iter().map(|v| i128::from(*v).to_be_bytes()),
            )
            .unwrap(),
        )
    } else {
        Arc::new(Int64Array::from(numbers.to_vec()))
    };
    let batch = RecordBatch::try_new(
        node.output_layout().schema().clone(),
        vec![numeric::key_array(&types[0]), payload],
    )
    .unwrap();
    let input =
        crate::exec::chunk::typed_fixture::chunk(batch, node.output_layout().slots(), &types);
    CompleteFixture {
        program,
        producer,
        input,
    }
}

fn run_complete(mode: CompleteMode, dop: usize, live_bound: bool) {
    run_complete_typed(
        mode,
        dop,
        live_bound,
        FunctionValueType::new(DataType::Utf8, true),
    );
}
fn run_complete_typed(
    mode: CompleteMode,
    dop: usize,
    live_bound: bool,
    key_type: FunctionValueType,
) {
    let mut f = complete_fixture_with_types(mode, dop, key_type.clone(), int64(true));
    let tracker = MemTracker::new_root("complete-ordered-pipeline");
    tracker.install_limit_once(64 << 20).unwrap();
    f.input.try_transfer_to(&tracker).unwrap();
    let producer = session_for(&f.program, f.producer);
    let (scan, input) = f.program.scan_inputs().iter().next().unwrap();
    let ProgramNodeKind::Scan {
        runtime_filters, ..
    } = f.program.graph().nodes()[scan.index()].kind()
    else {
        panic!("actual Scan")
    };
    let spec = &runtime_filters[0].consumer;
    let expected = rf::RuntimeFilterConsumerContract::ordered_live(
        rf::RuntimeFilterBindingId::new(spec.binding_id()),
        rf::RuntimeFilterChannelId::new(spec.channel_id()),
        rf::RuntimeFilterLateApplyGranularity::Batch,
        producer.expected.contract().clone(),
    )
    .unwrap();
    let query = numeric::LiteralOrderQuery::new(key_type.data_type().clone());
    let script = if live_bound {
        let rf::RuntimeFilterExecutionContract::Ordered(order) = expected.contract() else {
            panic!("ordered")
        };
        VecDeque::from([rf::LivePollOutcome::Updated {
            snapshot: Arc::new(rf::RuntimeFilterSnapshot::new(
                expected.binding_id(),
                rf::LogicalVersion::new(1),
                order.digest(),
                query.clone(),
            )),
            terminal: Some(rf::LiveTerminal::Completed),
        }])
    } else {
        (0..3)
            .map(|_| rf::LivePollOutcome::Idle {
                latest_version: None,
                terminal: None,
            })
            .collect()
    };
    let consumer = Arc::new(ScanSession {
        expected,
        mode: Bind::Live,
        requests: Mutex::new(vec![]),
        cursor: Arc::new(Cursor {
            script: Mutex::new(script),
            observed: Mutex::new(vec![]),
        }),
    });
    let session = Arc::new(BothPorts {
        producer: producer.clone(),
        consumer: consumer.clone(),
    });
    let runtime =
        Arc::new(state(tracker.clone()).with_runtime_filter_session(Some(session.clone())));
    let creation = construction(&tracker, &runtime);
    let batches = [(0, 3), (3, 3), (6, 2)]
        .map(|(offset, rows)| slice(&f.input, offset, rows, creation.as_ref()))
        .to_vec();
    let source = crate::runtime::task::scan::compiled_fixture::FixtureScanOp::new(batches, false);
    let mut scans = ScanBindings::default();
    scans.insert(i32::try_from(input.scan_node).unwrap(), source.clone());
    let output = ResultSinkBuffer::new();
    let effects = Arc::new(Effects::default());
    let prepared = prepare_compiled_program_pipeline_execution_with_profiler(
        f.program.clone(),
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(output.clone())),
        ExchangeBindings::default(),
        scans,
        CompiledWriterBindings::default(),
        None,
        None,
        i32::try_from(dop).unwrap(),
        runtime.clone(),
        effects.clone(),
    )
    .unwrap_or_else(|e| panic!("{mode:?}/dop{dop}/bound{live_bound}: {e:?}"));
    let running = prepared.start();
    running
        .join()
        .unwrap_or_else(|e| panic!("{mode:?}/dop{dop}/bound{live_bound}: {e:?}"));
    assert!(running.stopped_fact().is_some());
    assert_eq!(source.claims(), 1);
    let expected_partitions = if matches!(mode, CompleteMode::FinalMerge) {
        1
    } else {
        dop as u32
    };
    let opened = producer.opened.lock().unwrap();
    assert_eq!(
        opened.len(),
        expected_partitions as usize,
        "{mode:?}/dop{dop}"
    );
    assert!(
        opened.iter().all(|r| r.contract() == &producer.expected
            && r.local_partition_count() == expected_partitions)
    );
    drop(opened);
    let events = producer.recorder.events.lock().unwrap();
    for partition in 0..expected_partitions {
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::Close(p, _) if *p == partition))
                .count(),
            1
        );
    }
    assert!(!events.iter().any(|e| matches!(e, Event::Fail(_))));
    if !live_bound {
        assert!(
            events.iter().any(|e| matches!(e, Event::Submit(_, _, _))),
            "enough actual distinct groups publish a bound"
        );
    }
    drop(events);
    assert_eq!(consumer.requests.lock().unwrap().len(), 1);
    assert_eq!(
        query.calls.load(Ordering::SeqCst),
        if live_bound { 8 } else { 0 }
    );
    let chunks = output.take_chunks();
    let mut actual = Vec::new();
    for chunk in &chunks {
        assert_eq!(
            chunk.chunk_schema().slots()[0].typed_binding(),
            key_type.typed_binding()
        );
        for row in 0..chunk.len() {
            let key = numeric::key_at(&chunk.columns()[0], row);
            let values = chunk.columns()[1..]
                .iter()
                .map(|column| {
                    let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert!(!values.is_null(row));
                    values.value(row)
                })
                .collect::<Vec<_>>();
            actual.push((key, values));
        }
    }
    let expected_values = if matches!(mode, CompleteMode::ZeroCalls) {
        vec![(None, vec![]), (Some(1_i128), vec![])]
    } else {
        vec![(None, vec![2, 9, 2, 7]), (Some(1_i128), vec![2, 11, 1, 10])]
    };
    assert_eq!(
        actual, expected_values,
        "{mode:?}/dop{dop}/bound{live_bound}"
    );
    // Aggregate/Sort create fresh arrays. Their typed construction lifetime
    // belongs to the complete output projection, unlike ordered source loans
    // whose backing follows an individual Buffer alias below.
    let last_projection = chunks
        .iter()
        .find(|chunk| !chunk.is_empty())
        .unwrap()
        .batch_projection();
    drop(chunks);
    drop(output);
    drop(running);
    drop(source);
    drop(f);
    drop(runtime);
    drop(session);
    drop(producer);
    drop(consumer);
    drop(creation);
    drop(effects);
    drop(query);
    assert!(
        tracker.current() > 0,
        "the final typed output projection retains its actual construction stock"
    );
    assert!(!last_projection.is_empty());
    std::thread::spawn(move || drop(last_projection))
        .join()
        .unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_ordered_complete_actual_final_processors_preserve_results_partitions_and_last_projection()
 {
    for mode in [
        CompleteMode::Single,
        CompleteMode::ZeroCalls,
        CompleteMode::FinalMerge,
    ] {
        for dop in [1, 2] {
            for live in [false, true] {
                run_complete(mode, dop, live);
            }
        }
    }
}

#[test]
fn compiled_ordered_complete_preparation_releases_refused_final_stage_before_binding() {
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let f = complete_fixture(CompleteMode::Single, 2);
        let tracker = MemTracker::new_root("complete-ordered-final-refusal");
        tracker
            .install_limit_once(if cause == KernelFailure::ResourceExhausted {
                1
            } else {
                64 << 20
            })
            .unwrap();
        let runtime = state(tracker.clone());
        let factory = CompiledAggregateProcessorFactory::prepare_ordered(
            f.program.clone(),
            f.producer,
            runtime.error_state(),
        )
        .unwrap();
        assert!(factory.requires_local_update_stages());
        let (partial, mut last) = factory.into_local_update_stages().unwrap();
        let session = session_for(&f.program, f.producer);
        let host = rf::OrderedTopNAllocationHost::new(tracker.clone());
        let mut control = RuntimeKernelControl::new(runtime.error_state());
        control.bind_mem_tracker(tracker.clone());
        if cause != KernelFailure::ResourceExhausted {
            runtime.error_state().set_error(cause.clone());
        }
        let error = last
            .install_ordered(session.clone(), 2, &host, &control)
            .unwrap_err();
        assert!(
            matches!(error.cause(), ExecutionFailureCause::Kernel(actual) if *actual == cause),
            "{error:?}"
        );
        assert!(session.opened.lock().unwrap().is_empty());
        assert!(session.recorder.events.lock().unwrap().is_empty());
        drop(last);
        drop(partial);
        drop(session);
        drop(f);
        drop(control);
        drop(host);
        drop(runtime);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn compiled_ordered_complete_largeint_payload_keeps_real_source_and_last_buffer_owners() {
    let ty = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let mut f = complete_fixture_with_payload(CompleteMode::ZeroCalls, 1, ty.clone());
    let tracker = MemTracker::new_root("complete-ordered-largeint-source");
    tracker.install_limit_once(64 << 20).unwrap();
    f.input.try_transfer_to(&tracker).unwrap();
    let runtime = state(tracker.clone());
    let creation = construction(&tracker, &runtime);
    let host = rf::OrderedTopNAllocationHost::new(tracker.clone());
    let mut control = RuntimeKernelControl::new(runtime.error_state());
    control.bind_mem_tracker(tracker.clone());
    let producer = session_for(&f.program, f.producer);
    let (&scan_id, _) = f.program.scan_inputs().iter().next().unwrap();
    let ProgramNodeKind::Scan {
        runtime_filters, ..
    } = f.program.graph().nodes()[scan_id.index()].kind()
    else {
        panic!("Scan")
    };
    let spec = &runtime_filters[0].consumer;
    let expected = rf::RuntimeFilterConsumerContract::ordered_live(
        rf::RuntimeFilterBindingId::new(spec.binding_id()),
        rf::RuntimeFilterChannelId::new(spec.channel_id()),
        rf::RuntimeFilterLateApplyGranularity::Batch,
        producer.expected.contract().clone(),
    )
    .unwrap();
    let rf::RuntimeFilterExecutionContract::Ordered(order) = expected.contract() else {
        panic!("ordered")
    };
    let query = LiteralQuery::new();
    let snapshot = Arc::new(rf::RuntimeFilterSnapshot::new(
        expected.binding_id(),
        rf::LogicalVersion::new(1),
        order.digest(),
        query.clone(),
    ));
    let session = Arc::new(ScanSession {
        expected,
        mode: Bind::Live,
        requests: Mutex::new(vec![]),
        cursor: Arc::new(Cursor {
            script: Mutex::new(VecDeque::from([rf::LivePollOutcome::Updated {
                snapshot,
                terminal: Some(rf::LiveTerminal::Completed),
            }])),
            observed: Mutex::new(vec![]),
        }),
    });
    let ordered = CompiledOrderedScanFactory::try_new(
        f.program.clone(),
        scan_id,
        session.clone(),
        &host,
        &control,
    )
    .unwrap();
    let source = Arc::new(Source {
        pages: Pages::new(),
        claims: AtomicUsize::new(0),
        construction: creation.clone(),
    });
    source.pages.push(f.input.clone());
    source.pages.end();
    let factory = StreamScanSourceFactory::new_compiled_ordered(
        7,
        Arc::new(BoundOp(source.clone())),
        None,
        ordered,
        None,
        None,
        host.clone(),
        control.clone(),
    );
    let effects = Arc::new(Effects::default());
    let mut operator = factory.create(1, 0);
    operator.set_mem_tracker(tracker.clone());
    operator.set_construction_factory(creation.clone());
    operator.set_task_event_sink(effects.clone());
    operator.prepare().unwrap();
    operator.bind_runtime_state(&runtime).unwrap();
    let mut processor =
        crate::exec::pipeline::test_driver::TestDriver::new(operator.as_mut(), &runtime);
    let filtered = processor
        .advance_to_boundary()
        .unwrap()
        .output
        .expect("the original source output");
    assert_eq!(filtered.len(), 4);
    let expected_values = [10_i128, 7, 2, 1];
    let actual = filtered.columns()[1]
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    for (row, value) in expected_values.into_iter().enumerate() {
        assert_eq!(actual.value(row), value.to_be_bytes());
    }
    assert_eq!(
        filtered.chunk_schema().slots()[1].typed_binding(),
        ty.typed_binding()
    );
    let terminal = processor.advance_to_boundary().unwrap();
    assert!(terminal.output.is_none());
    assert!(matches!(
        terminal.next,
        crate::exec::pipeline::step::NextStep::Finished
    ));
    drop(processor);
    operator.close().unwrap();
    assert!(source.pages.operations.is_exited());
    let exit = operator.observe_exit();
    assert!(matches!(
        exit.progress,
        crate::exec::pipeline::operator::ExitProgress::Exited
    ));
    assert!(exit.failure.is_none());
    assert!(exit.unresolved_exit.is_none());
    assert_eq!(source.claims.load(Ordering::SeqCst), 1);
    assert_eq!(source.pages.closes.load(Ordering::SeqCst), 1);
    assert_eq!(source.pages.consumed.load(Ordering::SeqCst), 1);
    assert_eq!(query.calls.load(Ordering::SeqCst), 8);
    let (receiver_projection, receiver_buffer, receiver_tracker) = exchange::retain_received_buffer(
        &filtered,
        f.program.graph().nodes()[scan_id.index()].output_layout(),
        creation.as_ref(),
        &control,
    );
    let last_buffer = filtered.columns()[1].to_data().buffers()[0].clone();
    drop(filtered);
    drop(operator);
    drop(factory);
    drop(source);
    drop(f);
    drop(session);
    drop(producer);
    drop(effects);
    drop(query);
    drop(host);
    drop(control);
    drop(creation);
    drop(runtime);
    assert!(tracker.current() > 0);
    std::thread::spawn(move || drop(last_buffer))
        .join()
        .unwrap();
    assert_eq!(tracker.current(), 0);
    assert!(receiver_tracker.current() > 0);
    drop(receiver_projection);
    assert!(
        receiver_tracker.current() > 0,
        "the decoded bare Buffer retains its original receiver construction stock"
    );
    // NRU1 decode attaches the receiver's original storage custody to every
    // actual Buffer alias. Dropping the typed projection cannot release the
    // budget while this bare payload Buffer remains readable.
    let first: [u8; 16] = receiver_buffer.as_slice()[..16].try_into().unwrap();
    assert!(
        [1_i128, 2, 7, 10]
            .into_iter()
            .any(|v| first == v.to_be_bytes())
    );
    std::thread::spawn(move || drop(receiver_buffer))
        .join()
        .unwrap();
    assert_eq!(receiver_tracker.current(), 0);
}

#[test]
fn compiled_ordered_complete_numeric_keys_use_the_actual_host_scan_and_final_processor() {
    let mut types = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Date32,
        DataType::Decimal128(20, 3),
    ];
    // Connector assignments declare these three units. Second is protected by
    // the shared core/type tests; this provider has no seconds assignment ABI.
    for unit in [
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Timestamp(unit, None));
    }
    let mut keys = types
        .into_iter()
        .map(|ty| FunctionValueType::new(ty, true))
        .collect::<Vec<_>>();
    keys.push(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
    );
    for key in keys {
        for bound in [false, true] {
            run_complete_typed(CompleteMode::Single, 2, bound, key.clone());
        }
    }
}

#[test]
fn compiled_ordered_complete_pending_endpoint_cannot_run_or_bind_on_the_local_partial() {
    let f = complete_fixture(CompleteMode::Single, 2);
    let tracker = MemTracker::new_root("complete-ordered-pending-endpoint");
    tracker.install_limit_once(64 << 20).unwrap();
    let runtime = state(tracker.clone());
    let creation = construction(&tracker, &runtime);
    let factory = CompiledAggregateProcessorFactory::prepare_ordered(
        f.program.clone(),
        f.producer,
        runtime.error_state(),
    )
    .unwrap();
    let mut pending = factory.create(2, 0);
    pending.set_construction_factory(creation.clone());
    assert!(
        matches!(pending.prepare().unwrap_err().cause(), ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(detail))
        if detail.message() == "compiled aggregate ordered producer endpoint is not installed")
    );
    drop(pending);
    let (mut partial, last) = factory.into_local_update_stages().unwrap();
    let mut pending = last.create(2, 0);
    pending.set_construction_factory(creation.clone());
    assert!(
        matches!(pending.prepare().unwrap_err().cause(), ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(detail))
        if detail.message() == "compiled aggregate ordered producer endpoint is not installed")
    );
    drop(pending);
    let mut local = partial.create(2, 0);
    initialize(local.as_mut(), &tracker, &creation, &runtime);
    local.close().unwrap();
    let producer = session_for(&f.program, f.producer);
    let host = rf::OrderedTopNAllocationHost::new(tracker.clone());
    let control = RuntimeKernelControl::new(runtime.error_state());
    let error = partial
        .install_ordered(producer.clone(), 2, &host, &control)
        .unwrap_err();
    assert!(
        matches!(error.cause(), ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(detail))
        if detail.message() == "compiled aggregate does not own an ordered producer endpoint")
    );
    assert!(producer.opened.lock().unwrap().is_empty());
    drop(local);
    drop(last);
    drop(partial);
    drop(f);
    drop(producer);
    drop(host);
    drop(control);
    drop(creation);
    drop(runtime);
    assert_eq!(tracker.current(), 0);
}

#[path = "compiled_complete_ordered_numeric_tests.rs"]
mod numeric;

#[path = "compiled_complete_ordered_exchange_tests.rs"]
mod exchange;
