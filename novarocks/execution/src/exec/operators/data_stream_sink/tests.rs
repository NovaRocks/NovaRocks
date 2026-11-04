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

use super::*;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use arrow::array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, Int32Array, LargeStringArray, StringArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::runtime::endpoint::RuntimeEndpoint;

#[path = "test_support.rs"]
mod test_support;
use test_support::*;

fn make_test_operator(fixture: &Fixture) -> DataStreamSinkOperator {
    let send_group = ExchangeSenderGroup::new();
    DataStreamSinkOperator {
        name: "DataStreamSink(test)".to_string(),
        input: DataStreamSinkFactoryInput::try_new(
            1,
            DataStreamPartitionType::Unpartitioned,
            novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            Vec::new(),
            Vec::new(),
        )
        .expect("test sink input"),
        transmitter: fixture.transport(),
        fragment_instance_id: UniqueId::new(1, 2),
        init_error: None,
        driver_id: 0,
        sender_id: 11,
        be_number: 0,
        send_group: send_group.clone(),
        random_next: 0,
        pending_per_dest: Vec::new(),
        pending_bytes_per_dest: Vec::new(),
        pending_payloads_per_dest: Vec::new(),
        max_transmit_batched_bytes: 1,
        finished: AtomicBool::new(false),
        aborting: AtomicBool::new(false),
        finishing: AtomicBool::new(false),
        finish_counted: AtomicBool::new(false),
        finish_completed_set: AtomicBool::new(false),
        end_of_stream_sent: AtomicBool::new(false),
        send_observable: send_group.observable().clone(),
        error_state: Some(fixture.state().error_state()),
        #[cfg(novarocks_dev_controls)]
        writer_diagnostics: None,
        finish_state: Arc::new(DataStreamSinkFinishState::default()),
        profile_initialized: false,
        profiles: None,
        pending_chunks_mem_tracker: None,
        pending_payload_mem_tracker: None,
        retained_payload_mem_tracker: None,
        edge_gates: None,
        compiled_partition: None,
        construction_factory: None,
        frozen_output: Some(frozen_test_output(typed_int64_chunk(0).chunk_schema())),
        recipe: None,
        prepared: false,
        exiting: false,
        compiled_control: None,
    }
}

fn frozen_test_output(schema: &ChunkSchema) -> novarocks_local_program::StaticLayout {
    struct Control;
    impl novarocks_type_contract::PureCompileControl for Control {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }
    // Static output is the canonical projection of the frozen complete
    // binding. Runtime Field carriers (for example Dictionary) stay on the
    // actual input Chunk and cannot become ValueLayout projection authority.
    let fields = schema
        .slots()
        .iter()
        .map(|slot| {
            let binding = slot.typed_binding();
            Arc::new(Field::new(
                slot.name(),
                binding.layout().data_type().clone(),
                binding.nullable(),
            ))
        })
        .collect::<Vec<_>>();
    let frozen_schema = Arc::new(Schema::new(fields));
    novarocks_local_program::StaticLayout::try_new_typed_for_compile(
        frozen_schema,
        schema
            .slots()
            .iter()
            .map(|slot| slot.slot_id())
            .collect::<Vec<_>>()
            .into(),
        schema
            .slots()
            .iter()
            .map(|slot| (slot.typed_binding().clone(), None))
            .collect(),
        schema.storage_lifetime(),
        |_| Ok(()),
        &Control,
    )
    .unwrap()
}

fn prepare_test_operator(fixture: &Fixture, op: &mut DataStreamSinkOperator) {
    bind_test_operator(fixture, op);
    let error = op
        .error_state
        .as_ref()
        .expect("original Task control")
        .clone();
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        crate::runtime::mem_tracker::MemTracker::new_root("prepared exchange sink fixture"),
        error,
    ).expect("actual sink creator");
    let mut control = RuntimeKernelControl::new(op.error_state.as_ref().unwrap().clone());
    control.bind_runtime_memory(&fixture.state());
    op.compiled_control = Some(control);
    op.set_construction_factory(factory);
    op.prepare().expect("actual sink preparation");
}

fn typed_int64_chunk(rows: usize) -> Chunk {
    use novarocks_type_contract::{
        TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let pure = ValueTypeStorageLifetime::pure();
    let value =
        ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int64), false, pure.clone()).unwrap();
    let binding = TypedValueBinding::try_new(
        value,
        ValueLayout::try_new(&DataType::Int64, pure.clone()).unwrap(),
    )
    .unwrap();
    let slot = ChunkSlotSchema::try_new_typed(
        SlotId::new(1),
        Arc::new(Field::new("v", DataType::Int64, false)),
        binding,
        None,
        pure.clone(),
    )
    .unwrap();
    let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
    // Explicit pure frozen source author; the runtime sender derives new
    // headers only through its genuinely installed creator above.
    Chunk::try_new_typed_with_columns_observed::<ExecutionFailure, _>(
        schema,
        vec![Arc::new(arrow::array::Int64Array::from_iter_values(
            0..rows as i64,
        ))],
        rows,
        |_| Ok(()),
    )
    .unwrap()
}

#[test]
fn hash_placement_refuses_missing_destination_and_misaligned_key_rows() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let chunk = typed_int64_chunk(2);
    for crc32 in [false, true] {
        let error = partition_chunk_by_hash_arrays(&chunk, &[], 0, crc32).unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(error.failure_fact().reason().code(), 358);
        let key: arrow::array::ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1]));
        let error = partition_chunk_by_hash_arrays(&chunk, &[key], 2, crc32).unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(error.failure_fact().reason().code(), 358);
        assert!(
            error
                .detail()
                .contains("key rows 1 differ from input rows 2")
        );
    }
}

fn blocked_eos_sink(
    fixture: &Fixture,
) -> (
    DataStreamSinkOperator,
    Arc<BlockingEosTransmitter>,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
) {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let transmitter = Arc::new(BlockingEosTransmitter {
        entered: entered_tx,
        release: Arc::new(Mutex::new(release_rx)),
        transport: fixture.transport(),
        calls: AtomicUsize::new(0),
    });
    let mut sink = make_test_operator(fixture);
    sink.transmitter = Arc::clone(&transmitter) as Arc<dyn ExchangeFrameTransmitter>;
    sink.input.destinations = vec![make_test_destination(fixture)];
    prepare_test_operator(fixture, &mut sink);
    (sink, transmitter, entered_rx, release_tx)
}

#[test]
fn normal_eos_does_not_finish_until_blocked_transmit_returns() {
    let fixture = Fixture::new();
    let (mut sink, transmitter, entered, release) = blocked_eos_sink(&fixture);
    let data = SinkKernel::finish_for_test(&mut sink, &fixture.state()).expect("enqueue EOS");
    assert!(
        matches!(data.next, NextStep::Finished),
        "the marker is enqueued, not yet transmitted"
    );
    entered
        .recv_timeout(Duration::from_secs(2))
        .expect("EOS transmit started");
    sink.begin_exit().expect("observe the real send owner");
    assert!(matches!(
        sink.observe_exit().progress,
        ExitProgress::Pending(_)
    ));
    release.send(()).expect("release real EOS transmit");
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match sink.observe_exit().progress {
            ExitProgress::Exited => break,
            ExitProgress::Pending(_) | ExitProgress::Unresolved(_) => {
                assert!(Instant::now() < deadline, "EOS transmit exit deadline");
                std::thread::yield_now();
            }
        }
    }
    assert_eq!(transmitter.calls.load(Ordering::Acquire), 1);
}

#[test]
fn cancellation_revokes_eos_debt_but_waits_for_blocked_transmit() {
    let fixture = Fixture::new();
    let (mut sink, transmitter, entered, release) = blocked_eos_sink(&fixture);
    let data = SinkKernel::finish_for_test(&mut sink, &fixture.state()).expect("enqueue EOS");
    assert!(matches!(data.next, NextStep::Finished));
    entered
        .recv_timeout(Duration::from_secs(2))
        .expect("EOS transmit started");
    sink.cancel();
    sink.begin_exit()
        .expect("retain the inflight send after cancellation");
    assert!(matches!(
        sink.observe_exit().progress,
        ExitProgress::Pending(_)
    ));
    assert!(!sink.input_admissible());
    release.send(()).expect("release real EOS transmit");
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match sink.observe_exit().progress {
            ExitProgress::Exited => break,
            ExitProgress::Pending(_) | ExitProgress::Unresolved(_) => {
                assert!(Instant::now() < deadline, "cancelled send exit deadline");
                std::thread::yield_now();
            }
        }
    }
    assert_eq!(transmitter.calls.load(Ordering::Acquire), 1);
}

#[test]
fn task_shutdown_waits_for_the_inflight_send_before_publishing_stopped() {
    let fixture = Fixture::new();
    use crate::exec::pipeline::driver::PipelineDriver;
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::{DriverTask, FragmentCompletion};

    let (mut sink, transmitter, entered, release) = blocked_eos_sink(&fixture);
    SinkKernel::finish_for_test(&mut sink, &fixture.state()).expect("enqueue EOS");
    entered
        .recv_timeout(Duration::from_secs(2))
        .expect("EOS transmit started");

    let state = Arc::new(fixture.state());
    let driver = PipelineDriver::new(
        1,
        vec![Box::new(
            crate::exec::pipeline::shape::Transform::hydrating(sink),
        )],
        None,
        Vec::new(),
        Arc::clone(&state),
        None,
    );
    let completion = FragmentCompletion::new(1, state.error_state());
    let submitted_failure: crate::runtime::fragment::ExecutionFailure =
        (crate::exec::pipeline::driver_failure::DriverFailure::ExecutorShutdown).into();
    completion.fail(submitted_failure.clone());
    assert!(
        completion
            .conclusion()
            .unwrap()
            .unwrap_err()
            .same_occurrence(&submitted_failure)
    );
    let context = Arc::new(FragmentContext::new(None, state, None, None, None, None));
    let task = DriverTask::new(
        driver,
        Arc::clone(&completion),
        context,
        Duration::from_millis(10),
    );
    let mut task = task
        .finish_due_to_abort()
        .expect("blocked transmit keeps task pending");
    assert!(completion.stopped_fact().is_none());

    release.send(()).expect("release real EOS transmit");
    let deadline = Instant::now() + Duration::from_secs(2);
    while task.has_pending_finish() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(!task.has_pending_finish());
    assert!(task.finish_due_to_abort().is_none());
    assert!(completion.stopped_fact().is_some());
    assert_eq!(transmitter.calls.load(Ordering::Acquire), 1);
}

fn make_test_destination(fixture: &Fixture) -> FragmentDestination {
    FragmentDestination::new(
        UniqueId::new(9, 9),
        RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
        UniqueId::new(1, 2),
        0,
        1,
    )
    .expect("destination")
    .with_task_identity(fixture.destination_task(9))
}

fn destination_with_finst(fixture: &Fixture, low: i64) -> FragmentDestination {
    FragmentDestination::new(
        UniqueId::new(9, low),
        RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
        UniqueId::new(1, 2),
        0,
        1,
    )
    .expect("destination")
    .with_task_identity(fixture.destination_task(u32::try_from(low).unwrap()))
}

fn edge_id(value: u32) -> ExchangeEdgeId {
    ExchangeEdgeId::new(value).expect("nonzero edge")
}

/// Gates the sink's destinations one edge per destination, all closed, so
/// each edge's decision is observable in isolation.
fn gate_destinations(op: &mut DataStreamSinkOperator, dests: Vec<FragmentDestination>) {
    let gates = ExchangeEdgeGates::try_new(dests.iter().enumerate().map(|(idx, dest)| {
        (
            edge_id(idx as u32 + 1),
            vec![ExchangeDestinationKey::new(
                *dest.finst_id(),
                op.input.dest_node_id,
            )],
        )
    }))
    .expect("legal gate set");
    op.input.destinations = dests;
    op.edge_gates = Some(gates);
}

/// One version opens one exact edge set, so every edge a test needs open
/// is named in a single request.
fn open_edges(op: &DataStreamSinkOperator, edges: &[ExchangeEdgeId]) {
    op.edge_gates
        .as_ref()
        .expect("gated sink")
        .open(crate::task_execution::domain::EdgeOpenVersion::FIRST, edges)
        .expect("open edges");
}

fn cancel_edge(op: &DataStreamSinkOperator, edge: ExchangeEdgeId) {
    assert!(
        op.edge_gates
            .as_ref()
            .expect("gated sink")
            .gate(edge)
            .expect("gated edge")
            .close_for_normal_cancellation()
    );
}

fn test_payload(bytes: usize) -> PendingPayload {
    PendingPayload {
        be_number: 0,
        payload: ExchangeRetainedPayload::new(vec![7; bytes], None),
        payload_bytes: bytes,
        eos: false,
    }
}

fn zero_column_chunk_with_rows(row_count: usize) -> Chunk {
    let schema = Arc::new(
        ChunkSchema::try_new_typed(
            Vec::new(),
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    );
    Chunk::try_new_typed_with_columns_observed::<crate::runtime::fragment::ExecutionFailure, _>(
        schema,
        Vec::new(),
        row_count,
        |_| Ok(()),
    )
    .expect("explicit count-only source")
}

#[test]
fn data_stream_sink_accepts_low_cardinality_dictionary_carriers() {
    let fixture = Fixture::new();
    let op = make_test_operator(&fixture);
    let utf8_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let large_utf8_dict =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8));

    assert!(op.retains_encoded_column(SlotId::new(91), &utf8_dict));
    assert!(op.retains_encoded_column(SlotId::new(91), &large_utf8_dict));
}

#[test]
fn data_stream_sink_rejects_non_string_or_non_int32_dictionaries() {
    let fixture = Fixture::new();
    let op = make_test_operator(&fixture);
    let wrong_key = DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8));
    let wrong_value = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));

    assert!(!op.retains_encoded_column(SlotId::new(91), &wrong_key));
    assert!(!op.retains_encoded_column(SlotId::new(91), &wrong_value));
    assert!(!op.retains_encoded_column(SlotId::new(91), &DataType::Utf8));
}

#[test]
fn data_stream_sink_accepts_dictionary_for_hash_partition_after_hash_support() {
    let fixture = Fixture::new();
    let mut op = make_test_operator(&fixture);
    op.input.output_partition_type = DataStreamPartitionType::HashPartitioned;
    let utf8_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    assert!(op.retains_encoded_column(SlotId::new(91), &utf8_dict));
}

#[test]
fn bind_runtime_state_uses_backend_num_as_be_number() {
    let fixture = Fixture::new();
    let mut op = make_test_operator(&fixture);
    let state = fixture.state_with_backend(7);
    gate_destinations(&mut op, vec![make_test_destination(&fixture)]);

    OperatorLifecycle::bind_runtime_state(&mut op, &state).expect("bind runtime state");

    assert_eq!(op.sender_id, 11);
    assert_eq!(op.be_number, 7);
}

#[test]
fn zero_byte_pending_chunks_are_tracked_for_force_flush() {
    let fixture = Fixture::new();
    let mut op = make_test_operator(&fixture);
    op.input.destinations = vec![make_test_destination(&fixture)];

    prepare_test_operator(&fixture, &mut op);
    op.buffer_chunk(zero_column_chunk_with_rows(1))
        .expect("buffer chunk");

    assert_eq!(op.pending_chunk_bytes_total(), 0);
    assert_eq!(op.pending_chunk_count_total(), 1);
    assert!(op.has_pending_chunks());
    assert!(op.has_pending_data());
    assert!(!op.should_flush_pending_dest(0, false));
    assert!(op.should_flush_pending_dest(0, true));
}

#[test]
fn queued_payload_preserves_encoded_backend_number() {
    let fixture = Fixture::new();
    let mut op = make_test_operator(&fixture);
    op.be_number = 7;
    gate_destinations(&mut op, vec![make_test_destination(&fixture)]);
    prepare_test_operator(&fixture, &mut op);
    op.buffer_chunk(typed_int64_chunk(2)).unwrap();
    op.flush_pending(true).unwrap();
    let payload = op.pending_payloads_per_dest[0].as_ref().unwrap();
    let original = payload.payload.bytes().clone();
    assert_eq!(
        payload.be_number, 7,
        "pending payload must keep its encoded backend number, not current operator state"
    );
    op.be_number = 8;
    open_edges(&op, &[edge_id(1)]);
    op.flush_pending(true).unwrap();
    let frames = fixture.transport().frames();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].receiver.backend_number, 7);
    assert_eq!(frames[0].receiver.payload.as_ptr(), original.as_ptr());
}

#[test]
fn closed_edge_keeps_original_encoded_bytes_without_consuming_a_unit_sequence() {
    let fixture = Fixture::new();
    let mut sink = make_test_operator(&fixture);
    gate_destinations(&mut sink, vec![make_test_destination(&fixture)]);
    prepare_test_operator(&fixture, &mut sink);
    sink.buffer_chunk(typed_int64_chunk(8)).unwrap();
    sink.flush_pending(true).unwrap();
    let original = sink.pending_payloads_per_dest[0]
        .as_ref()
        .unwrap()
        .payload
        .bytes()
        .clone();
    for _ in 0..3 {
        sink.flush_pending(true).unwrap();
        let pending = sink.pending_payloads_per_dest[0].as_ref().unwrap();
        assert_eq!(pending.payload.bytes().as_ptr(), original.as_ptr());
        assert_eq!(pending.payload.bytes(), &original);
    }
    assert_eq!(fixture.transport().call_count(), 0);
    assert!(fixture.transport().demands().is_empty());
    open_edges(&sink, &[edge_id(1)]);
    sink.flush_pending(true).unwrap();
    let frames = fixture.transport().frames();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].receiver.item.sequence().get(), 1);
    assert_eq!(frames[0].receiver.payload.as_ptr(), original.as_ptr());
}

#[test]
fn pending_grant_keeps_the_next_payload_and_retries_without_a_sequence_gap() {
    let fixture = Fixture::new();
    let transport = fixture.transport();
    transport.withhold_grants();
    let mut sink = make_test_operator(&fixture);
    gate_destinations(&mut sink, vec![make_test_destination(&fixture)]);
    prepare_test_operator(&fixture, &mut sink);
    open_edges(&sink, &[edge_id(1)]);
    let destination = sink.destinations()[0].clone();
    assert!(matches!(
        sink.try_enqueue_payload(&destination, test_payload(3)),
        Ok(PayloadEnqueue::Enqueued)
    ));
    let mut pending = test_payload(4);
    let original = pending.payload.bytes().clone();
    for _ in 0..3 {
        pending = match sink.try_enqueue_payload(&destination, pending).unwrap() {
            PayloadEnqueue::NoCapacity(pending) => pending,
            _ => panic!("a second unit must wait for the outstanding grant"),
        };
        assert_eq!(pending.payload.bytes().as_ptr(), original.as_ptr());
    }
    assert_eq!(transport.call_count(), 0);
    transport.grant_pending();
    assert!(matches!(
        sink.try_enqueue_payload(&destination, pending),
        Ok(PayloadEnqueue::Enqueued)
    ));
    let frames = transport.frames();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].receiver.item.sequence().get(), 1);
    assert_eq!(frames[1].receiver.item.sequence().get(), 2);
    assert_eq!(frames[1].receiver.payload.as_ptr(), original.as_ptr());
}

#[test]
fn exchange_unit_carries_distinct_dest_and_sender_finsts() {
    let fixture = Fixture::new();
    let mut sink = make_test_operator(&fixture);
    sink.fragment_instance_id = UniqueId::new(81, 82);
    let destination = FragmentDestination::new(
        UniqueId::new(91, 92),
        RuntimeEndpoint::new("127.0.0.1", 9030).unwrap(),
        sink.fragment_instance_id,
        0,
        1,
    )
    .unwrap()
    .with_task_identity(fixture.destination_task(1));
    gate_destinations(&mut sink, vec![destination]);
    prepare_test_operator(&fixture, &mut sink);
    open_edges(&sink, &[edge_id(1)]);
    let destination = sink.destinations()[0].clone();
    assert!(matches!(
        sink.try_enqueue_payload(&destination, test_payload(3)),
        Ok(PayloadEnqueue::Enqueued)
    ));
    let frames = fixture.transport().frames();
    let frame = &frames[0].receiver;
    assert_eq!(
        frame.stream.destination_fragment_instance_id(),
        UniqueId::new(91, 92)
    );
    assert_eq!(
        frame.stream.source().fragment_instance_id(),
        UniqueId::new(81, 82)
    );
    assert_eq!(frame.stream.source().sender_ordinal(), 0);
    assert_eq!(frame.sender_count, 1);
    assert_eq!(frame.item.sequence().get(), 1);
}

#[test]
fn every_nonempty_payload_is_an_independent_prepared_unit() {
    let fixture = Fixture::new();
    let mut sink = make_test_operator(&fixture);
    gate_destinations(&mut sink, vec![make_test_destination(&fixture)]);
    prepare_test_operator(&fixture, &mut sink);
    sink.buffer_chunk(typed_int64_chunk(2)).unwrap();
    sink.flush_pending(true).unwrap();
    let payload = sink.pending_payloads_per_dest[0].as_ref().unwrap();
    assert_eq!(&payload.payload.bytes()[..4], b"NRU1");
    let schema = typed_int64_chunk(0).chunk_schema_ref();
    let recipe = sink.recipe.as_ref().unwrap();
    let factory = sink.construction_factory.as_ref().unwrap();
    let decoded = crate::runtime::exchange::online_unit::decode(
        recipe,
        payload.payload.bytes(),
        &schema,
        factory.as_ref(),
    )
    .unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].len(), 2);
}

#[test]
fn a_closed_edge_enqueues_no_frame_and_reserves_no_bytes() {
    let fixture = Fixture::new();
    let mut op = make_test_operator(&fixture);
    let dest = make_test_destination(&fixture);
    gate_destinations(&mut op, vec![dest.clone()]);
    prepare_test_operator(&fixture, &mut op);

    assert!(matches!(
        op.try_enqueue_payload(&dest, test_payload(4)),
        Ok(PayloadEnqueue::NoCapacity(_))
    ));
    assert!(
        op.send_group.is_idle(),
        "a closed edge must hand nothing to the send queue"
    );
    assert_eq!(
        op.send_group.retained_payload_bytes(),
        0,
        "a closed edge must not hold any of the shared byte budget"
    );
}

#[test]
fn opening_an_edge_lets_the_sinks_frames_through() {
    let fixture = Fixture::new();
    let mut op = make_test_operator(&fixture);
    let dest = make_test_destination(&fixture);
    gate_destinations(&mut op, vec![dest.clone()]);
    prepare_test_operator(&fixture, &mut op);

    open_edges(&op, &[edge_id(1)]);
    assert!(matches!(
        op.try_enqueue_payload(&dest, test_payload(4)),
        Ok(PayloadEnqueue::Enqueued)
    ));

    // Opening is monotonic and idempotent, so a replay changes nothing.
    open_edges(&op, &[edge_id(1)]);
    assert!(matches!(
        op.try_enqueue_payload(&dest, test_payload(4)),
        Ok(PayloadEnqueue::Enqueued)
    ));
}

#[test]
fn a_normally_cancelled_edge_discards_its_payload_and_its_pending_chunks() {
    let fixture = Fixture::new();
    let mut op = make_test_operator(&fixture);
    let dest = make_test_destination(&fixture);
    gate_destinations(&mut op, vec![dest.clone()]);
    prepare_test_operator(&fixture, &mut op);
    open_edges(&op, &[edge_id(1)]);

    op.buffer_chunk(typed_int64_chunk(8)).expect("buffer chunk");
    assert!(op.has_pending_chunks());

    cancel_edge(&op, edge_id(1));
    assert!(matches!(
        op.try_enqueue_payload(&dest, test_payload(4)),
        Ok(PayloadEnqueue::Discarded)
    ));

    op.flush_pending(true).expect("flush pending");
    assert_eq!(op.pending_chunk_count_total(), 0);
    assert_eq!(op.pending_payload_count(), 0);
    assert!(
        !op.has_pending_data(),
        "abandoned data must not hold the sink open"
    );
    assert!(op.send_group.is_idle(), "no frame reaches the send queue");
    assert_eq!(op.send_group.retained_payload_bytes(), 0);
    assert_eq!(
        op.current_error(),
        None,
        "a destination's normal departure is not this producer's failure"
    );
}

#[test]
fn a_normally_cancelled_edge_leaves_the_other_destinations_sending() {
    let fixture = Fixture::new();
    let mut op = make_test_operator(&fixture);
    let abandoned = destination_with_finst(&fixture, 1);
    let healthy = destination_with_finst(&fixture, 2);
    gate_destinations(&mut op, vec![abandoned.clone(), healthy.clone()]);
    prepare_test_operator(&fixture, &mut op);
    open_edges(&op, &[edge_id(1), edge_id(2)]);

    op.buffer_chunk(typed_int64_chunk(8)).expect("buffer chunk");
    assert_eq!(op.pending_per_dest.len(), 2);
    assert!(!op.pending_per_dest[0].is_empty() && !op.pending_per_dest[1].is_empty());

    cancel_edge(&op, edge_id(1));
    op.flush_pending(true).expect("flush pending");

    assert!(
        op.pending_per_dest[0].is_empty(),
        "the abandoned edge's chunks are dropped"
    );
    assert!(
        op.pending_per_dest[1].is_empty() && op.pending_payloads_per_dest[1].is_none(),
        "the healthy edge's chunks are handed to the send queue, not dropped or parked"
    );
    assert!(matches!(
        op.try_enqueue_payload(&abandoned, test_payload(4)),
        Ok(PayloadEnqueue::Discarded)
    ));
    assert!(
        op.edge_gates
            .as_ref()
            .expect("gated sink")
            .gate(edge_id(2))
            .expect("edge two")
            .may_send(),
        "the healthy edge keeps its send permission"
    );
    assert_eq!(op.current_error(), None);
}

#[test]
fn a_closed_edge_does_not_stall_a_destination_that_can_send() {
    let fixture = Fixture::new();
    let mut op = make_test_operator(&fixture);
    let closed = destination_with_finst(&fixture, 1);
    let open = destination_with_finst(&fixture, 2);
    gate_destinations(&mut op, vec![closed, open]);
    prepare_test_operator(&fixture, &mut op);
    open_edges(&op, &[edge_id(2)]);

    op.buffer_chunk(typed_int64_chunk(8)).expect("buffer chunk");
    op.flush_pending(true).expect("flush pending");

    assert!(
        op.pending_payloads_per_dest[0].is_some(),
        "the closed edge parks its own payload"
    );
    assert!(
        op.pending_payloads_per_dest[1].is_none() && op.pending_per_dest[1].is_empty(),
        "one edge waiting for permission must not block another edge's flush"
    );
}

#[test]
fn a_destination_no_edge_claims_fails_closed() {
    let fixture = Fixture::new();
    let mut op = make_test_operator(&fixture);
    gate_destinations(&mut op, vec![destination_with_finst(&fixture, 1)]);
    prepare_test_operator(&fixture, &mut op);

    let Err(error) = op.try_enqueue_payload(&destination_with_finst(&fixture, 7), test_payload(4))
    else {
        panic!("an unattributed destination must not send");
    };
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(error.failure_fact().reason().code(), 358);
    // The factory's exact stream roster rejects this destination before a
    // per-driver gate lookup; retain that owner's nominal source.
    assert!(matches!(
        error.cause(),
        crate::runtime::fragment::ExecutionFailureCause::Domain(_)
    ));
    assert_eq!(fixture.transport().call_count(), 0);
}

#[test]
fn a_closed_edge_holds_at_most_one_payload_and_stops_taking_input() {
    let fixture = Fixture::new();
    let mut op = make_test_operator(&fixture);
    let dest = make_test_destination(&fixture);
    gate_destinations(&mut op, vec![dest]);
    // One byte batches every buffered chunk, so each push offers the
    // closed edge a payload.
    op.max_transmit_batched_bytes = 1;
    let state = fixture.state();

    prepare_test_operator(&fixture, &mut op);
    op.max_transmit_batched_bytes = 1;
    let mut pushed = 0;
    while op.input_admissible() && pushed < 8 {
        SinkKernel::consume_for_test(&mut op, &state, typed_int64_chunk(8)).expect("push chunk");
        pushed += 1;
    }

    assert!(
        pushed < 8,
        "a closed edge must stop the sink from taking more input"
    );
    assert!(
        op.pending_payload_count() <= op.destinations().len(),
        "the single per-destination payload slot is the whole bound"
    );
    assert_eq!(
        op.send_group.retained_payload_bytes(),
        0,
        "a closed edge takes nothing from the global or the per-destination ceiling"
    );
    assert!(op.send_group.is_idle(), "nothing was handed to the queue");
    assert!(
        op.pending_payload_bytes_total() > 0,
        "the original encoded payload remains on its driver"
    );
    assert_eq!(fixture.transport().call_count(), 0);

    open_edges(&op, &[edge_id(1)]);
    assert!(
        op.input_admissible(),
        "opening the edge releases the backpressure"
    );
}

/// A producer whose input drains before its edge opens is a normal race,
/// not a failure: the open follows a round trip after every destination
/// acknowledges creation, and a small fragment can finish reading sooner.
/// So the finishing drain waits, and the driver is told to come back.
#[test]
fn finishing_before_an_edge_opens_waits_instead_of_failing() {
    let fixture = Fixture::new();
    let runtime = fixture.state();
    let mut op = make_test_operator(&fixture);
    gate_destinations(&mut op, vec![make_test_destination(&fixture)]);
    prepare_test_operator(&fixture, &mut op);
    op.buffer_chunk(typed_int64_chunk(8)).expect("buffer chunk");
    let parked =
        SinkKernel::finish_for_test(&mut op, &runtime).expect("closed edge parks finalization");
    let NextStep::Wait(wait) = parked.next else {
        panic!("closed edge must name its work wait")
    };
    assert!(matches!(wait.reason, WaitReason::Finish));
    assert!(op.current_error().is_none(), "permission is not a failure");
    assert!(op.awaits_edge_permission());
    assert_eq!(
        op.send_group.retained_payload_bytes(),
        0,
        "closed edge reserved nothing"
    );
    assert!(
        !op.maybe_mark_finished(),
        "buffered output still belongs to this driver"
    );
    open_edges(&op, &[edge_id(1)]);
    assert!(
        wait.is_ready(),
        "edge publication invalidates the exact reported wait"
    );
    assert!(
        matches!(
            op.next_work(op.send_observable.generation()),
            NextStep::Runnable
        ),
        "only the worker can drain the now-admitted payload"
    );
    let completed =
        SinkKernel::poll_for_test(&mut op, &runtime).expect("resume the same finalization");
    assert!(matches!(completed.next, NextStep::Finished));
    assert!(!op.has_pending_data());
    assert!(!op.owes_end_of_stream());
}

/// The same race with nothing buffered. Only an end-of-stream is owed, and
/// forgetting it would leave the destination counting senders forever.
#[test]
fn a_closed_edge_owed_only_an_end_of_stream_still_blocks_finishing() {
    let fixture = Fixture::new();
    let runtime = fixture.state();
    let mut op = make_test_operator(&fixture);
    gate_destinations(&mut op, vec![make_test_destination(&fixture)]);
    prepare_test_operator(&fixture, &mut op);
    assert!(!op.has_pending_data(), "only EOS is owed");
    let parked = SinkKernel::finish_for_test(&mut op, &runtime).expect("signal EOS once");
    let NextStep::Wait(wait) = parked.next else {
        panic!("closed edge must defer its marker")
    };
    assert!(matches!(wait.reason, WaitReason::Finish));
    assert!(!op.maybe_mark_finished());
    assert_eq!(fixture.transport().call_count(), 0);
    open_edges(&op, &[edge_id(1)]);
    assert!(wait.is_ready());
    assert_eq!(
        fixture.transport().call_count(),
        0,
        "notification cannot send EOS"
    );
    let completed = SinkKernel::poll_for_test(&mut op, &runtime).expect("resume EOS");
    assert!(matches!(completed.next, NextStep::Finished));
    assert_eq!(fixture.transport().call_count(), 1);
    assert!(!op.owes_end_of_stream());
}

/// The defect this catches: a sink that deferred its end-of-stream while
/// its edge was closed declared itself *finished* the moment that edge
/// opened, because "nothing parked and no closed edge" was the whole of
/// "I owe no more output". The scheduler reads that verdict through
/// `is_finished` and `need_input` between driver turns, and `set_finishing`
/// returns early once `finished` is latched -- so the marker was never
/// sent, by no race: the driver's own turn re-checks `is_finished` before
/// it re-checks the operator's pending finish.
///
/// The consequence is a hang with no failure anywhere. The receiving
/// exchange keeps counting senders, so its scan never reaches end of
/// stream, the root fragment never fills its result buffer, and the
/// frontend's read completion -- which needs the root FINISHED and its own
/// end of stream, neither of which fails on absence -- waits out the whole
/// statement budget. Measured on a real 1FE+3BE cluster as a distributed
/// `SELECT ... ORDER BY` timing out after 120 s with two minutes of total
/// silence in every process's log.
#[test]
fn an_edge_opening_must_not_finish_a_sink_before_its_end_of_stream_is_sent() {
    let fixture = Fixture::new();
    let runtime = fixture.state();
    let mut op = make_test_operator(&fixture);
    gate_destinations(&mut op, vec![make_test_destination(&fixture)]);
    prepare_test_operator(&fixture, &mut op);
    let parked =
        SinkKernel::finish_for_test(&mut op, &runtime).expect("closed edge parks the seal");
    let NextStep::Wait(wait) = parked.next else {
        panic!("seal is waiting for its destination")
    };
    assert!(!op.has_pending_data());
    assert_eq!(fixture.transport().call_count(), 0);
    open_edges(&op, &[edge_id(1)]);
    assert!(wait.is_ready());
    assert!(matches!(
        op.next_work(op.send_observable.generation()),
        NextStep::Runnable
    ));
    assert_eq!(
        fixture.transport().call_count(),
        0,
        "an opened gate alone cannot satisfy the mandatory marker"
    );
    let completed = SinkKernel::poll_for_test(&mut op, &runtime).expect("worker sends the seal");
    assert!(matches!(completed.next, NextStep::Finished));
    assert_eq!(fixture.transport().call_count(), 1, "one EOS frame");
    assert!(!op.owes_end_of_stream());
    let repeated =
        SinkKernel::poll_for_test(&mut op, &runtime).expect("completed continuation is stable");
    assert!(matches!(repeated.next, NextStep::Finished));
    assert_eq!(fixture.transport().call_count(), 1, "EOS is never replayed");
}

/// A sink can enter finishing while it still has buffered rows. Once the
/// asynchronous drain clears them, the scheduler inspects `is_finished`
/// before giving `set_finishing` another turn. This driver must remain
/// active until it decrements the shared count and seals its destination.
#[test]
fn a_drained_sink_must_count_its_driver_before_finishing() {
    let fixture = Fixture::new();
    let runtime = fixture.state();
    let mut op = make_test_operator(&fixture);
    gate_destinations(&mut op, vec![make_test_destination(&fixture)]);
    prepare_test_operator(&fixture, &mut op);
    open_edges(&op, &[edge_id(1)]);
    // The continuation after the last local payload drain must still count
    // this driver and publish the one sender-wide marker.
    op.finishing.store(true, Ordering::Release);
    assert!(!op.finish_counted.load(Ordering::SeqCst));
    assert!(!op.has_pending_data());
    assert!(!op.end_of_stream_sent.load(Ordering::SeqCst));
    let completed =
        SinkKernel::poll_for_test(&mut op, &runtime).expect("count and seal in the work step");
    assert!(matches!(completed.next, NextStep::Finished));
    assert!(op.finish_counted.load(Ordering::SeqCst));
    assert!(op.end_of_stream_sent.load(Ordering::SeqCst));
    assert_eq!(op.finish_state.remaining_drivers.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.transport().call_count(), 1);
}

/// The defect this catches: `set_finishing` counted this driver against
/// the sink's driver set on every call, while `finishing_wait` had
/// the driver retry that same call for as long as the edge stayed closed.
/// One driver's retries therefore consumed the whole set and latched the
/// one-shot end-of-stream guard, so once the edge finally opened no driver
/// was the last one any more and end-of-stream was never sent. The
/// destination then counted senders forever: observed in a real 1FE+3BE
/// run as a distributed SELECT hanging until its statement deadline.
#[test]
fn retrying_set_finishing_while_an_edge_is_closed_does_not_consume_the_driver_set() {
    let fixture = Fixture::new();
    let state = Arc::new(DataStreamSinkFinishState::default());
    let runtime = fixture.state();
    let mut first = make_test_operator(&fixture);
    first.finish_state = state.clone();
    gate_destinations(&mut first, vec![make_test_destination(&fixture)]);
    let mut second = make_test_operator(&fixture);
    second.driver_id = 1;
    second.finish_state = state.clone();
    gate_destinations(&mut second, vec![make_test_destination(&fixture)]);
    share_factory_binding(&first, &mut second);
    prepare_test_operator(&fixture, &mut first);
    prepare_test_operator(&fixture, &mut second);
    let completed = SinkKernel::finish_for_test(&mut first, &runtime).expect("first driver drains");
    assert!(
        matches!(completed.next, NextStep::Finished),
        "a drained non-last driver owes no sender EOS"
    );
    for _ in 0..7 {
        let repeated =
            SinkKernel::poll_for_test(&mut first, &runtime).expect("completed continuation");
        assert!(matches!(repeated.next, NextStep::Finished));
    }
    assert_eq!(
        state.remaining_drivers.load(Ordering::SeqCst),
        1,
        "eight observations count once"
    );
    let parked =
        SinkKernel::finish_for_test(&mut second, &runtime).expect("last driver owes the marker");
    let NextStep::Wait(wait) = parked.next else {
        panic!("last driver must wait for the exact edge")
    };
    assert_eq!(state.remaining_drivers.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.transport().call_count(), 0);
    open_edges(&second, &[edge_id(1)]);
    assert!(wait.is_ready());
    let completed =
        SinkKernel::poll_for_test(&mut second, &runtime).expect("resume the last driver's seal");
    assert!(matches!(completed.next, NextStep::Finished));
    assert!(!second.owes_end_of_stream());
    assert_eq!(fixture.transport().call_count(), 1);
    assert!(
        !state.force_eos_sent.load(Ordering::SeqCst),
        "no false driver-set accounting failure"
    );
}

/// The defect this catches: the driver that completes a sink's driver set
/// sends one end-of-stream for the whole set, but the pending buffers are
/// per driver. That driver checked only its own, so a sibling that had
/// finished while still holding rows had them dropped -- the receiver's
/// sender count closed on the marker and reported the exchange complete,
/// silently short, with no error anywhere. Observed on a real 1FE+3BE run
/// as `SELECT ... UNION SELECT ...` returning one row of two and as
/// `COUNT(*)` over a union returning 0; it reproduced in 3 of 8 suite
/// runs and in none on the commit before the cutover.
#[test]
fn a_driver_still_holding_rows_is_not_counted_against_the_driver_set() {
    let fixture = Fixture::new();
    let state = Arc::new(DataStreamSinkFinishState::default());
    // Two drivers on one sink, both sharing the frozen finish state.
    let runtime = fixture.state();

    let mut holder = make_test_operator(&fixture);
    holder.finish_state = Arc::clone(&state);
    gate_destinations(&mut holder, vec![make_test_destination(&fixture)]);

    let mut sibling = make_test_operator(&fixture);
    sibling.driver_id = 1;
    sibling.finish_state = Arc::clone(&state);
    gate_destinations(&mut sibling, vec![make_test_destination(&fixture)]);
    share_factory_binding(&holder, &mut sibling);
    prepare_test_operator(&fixture, &mut holder);
    prepare_test_operator(&fixture, &mut sibling);

    // One driver finishes holding rows it has not been able to hand over.
    holder
        .buffer_chunk(typed_int64_chunk(8))
        .expect("buffer chunk");
    assert!(holder.has_pending_chunks());
    SinkKernel::finish_for_test(&mut holder, &runtime)
        .expect("a closed edge parks the drain rather than failing");
    assert!(
        holder.has_pending_data(),
        "the closed edge leaves the rows on this driver"
    );
    assert_eq!(
        state.remaining_drivers.load(Ordering::SeqCst),
        2,
        "a driver holding rows has not finished, so it must not count"
    );

    // Its sibling finishing must therefore not complete the set, and must
    // not seal the sender over rows the first driver still holds.
    SinkKernel::finish_for_test(&mut sibling, &runtime)
        .expect("the sibling drains with nothing to hand over");
    assert_eq!(state.remaining_drivers.load(Ordering::SeqCst), 1);
    assert!(
        !sibling.end_of_stream_sent.load(Ordering::SeqCst),
        "no end-of-stream may go out while a sibling still holds rows"
    );

    // The edge opens: the holder hands its rows over on its next turn,
    // counts, completes the set, and only then does the marker follow the
    // frames it seals.
    open_edges(&holder, &[edge_id(1)]);
    SinkKernel::finish_for_test(&mut holder, &runtime)
        .expect("the parked rows and then the seal go out");
    assert!(!holder.has_pending_data());
    assert_eq!(state.remaining_drivers.load(Ordering::SeqCst), 0);
    assert!(
        holder.end_of_stream_sent.load(Ordering::SeqCst),
        "the driver that counted last seals, and it seals after its rows"
    );
    assert!(
        !state.force_eos_sent.load(Ordering::SeqCst),
        "the one-shot guard is for a genuine accounting error, not for this"
    );
}

#[test]
fn hash_partition_accepts_binary_arrays() {
    let array = Arc::new(BinaryArray::from(vec![
        Some(b"a".as_slice()),
        Some(b"b".as_slice()),
        None,
    ]));
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Binary, true)]));
    let batch = arrow::array::RecordBatch::try_new(schema, vec![array.clone()]).unwrap();
    let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[novarocks_types::SlotId::new(1)],
    )
    .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);

    let partitions = partition_chunk_by_hash_arrays(&chunk, &[array], 2, false).expect("partition");

    assert_eq!(
        partitions
            .iter()
            .map(|chunk| chunk.record_batch().num_rows())
            .sum::<usize>(),
        3
    );
}

fn status_chunk_from_array(array: ArrayRef) -> Chunk {
    let schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(91),
            Field::new("status", DataType::Utf8, true),
            None,
            None,
        )])
        .expect("chunk schema"),
    );
    Chunk::try_new_with_columns(schema, vec![array]).expect("status chunk")
}

fn large_status_chunk_from_array(array: ArrayRef) -> Chunk {
    let schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(92),
            Field::new("status_l", DataType::LargeUtf8, true),
            None,
            None,
        )])
        .expect("chunk schema"),
    );
    Chunk::try_new_with_columns(schema, vec![array]).expect("large status chunk")
}

fn status_values(chunk: &Chunk) -> Vec<Option<String>> {
    if chunk.is_empty() {
        return Vec::new();
    }
    let flat = arrow::compute::cast(chunk.columns()[0].as_ref(), &DataType::Utf8)
        .expect("cast status to utf8");
    let strings = flat
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 values");
    (0..strings.len())
        .map(|idx| {
            if strings.is_null(idx) {
                None
            } else {
                Some(strings.value(idx).to_string())
            }
        })
        .collect()
}

fn partition_status_values(chunks: &[Chunk]) -> Vec<Vec<Option<String>>> {
    chunks.iter().map(status_values).collect()
}

fn flat_and_dict_status_chunks() -> (Chunk, ArrayRef, Chunk, ArrayRef) {
    let flat: ArrayRef = Arc::new(StringArray::from(vec![
        Some("PAID"),
        Some("NEW"),
        None,
        Some("PAID"),
        Some("CANCELLED"),
        Some("NEW"),
    ]));
    let dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![Some(0), Some(1), None, Some(0), Some(2), Some(1)]),
        Arc::new(StringArray::from(vec!["PAID", "NEW", "CANCELLED"])),
    ));
    (
        status_chunk_from_array(flat.clone()),
        flat,
        status_chunk_from_array(dict.clone()),
        dict,
    )
}

fn flat_and_dict_large_status_chunks() -> (Chunk, ArrayRef, Chunk, ArrayRef) {
    let flat: ArrayRef = Arc::new(LargeStringArray::from(vec![
        Some("PAID"),
        Some("NEW"),
        None,
        Some("PAID"),
        Some("CANCELLED"),
        Some("NEW"),
    ]));
    let dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![Some(2), Some(0), None, Some(2), Some(1), Some(0)]),
        Arc::new(LargeStringArray::from(vec!["NEW", "CANCELLED", "PAID"])),
    ));
    (
        large_status_chunk_from_array(flat.clone()),
        flat,
        large_status_chunk_from_array(dict.clone()),
        dict,
    )
}

#[test]
fn hash_partition_dictionary_utf8_matches_flat_utf8_fnv() {
    let (flat_chunk, flat_array, dict_chunk, dict_array) = flat_and_dict_status_chunks();

    let flat_parts =
        partition_chunk_by_hash_arrays(&flat_chunk, &[flat_array], 4, false).expect("flat fnv");
    let dict_parts =
        partition_chunk_by_hash_arrays(&dict_chunk, &[dict_array], 4, false).expect("dict fnv");

    assert_eq!(
        partition_status_values(&dict_parts),
        partition_status_values(&flat_parts)
    );
}

#[test]
fn hash_partition_dictionary_utf8_matches_flat_utf8_crc32() {
    let (flat_chunk, flat_array, dict_chunk, dict_array) = flat_and_dict_status_chunks();

    let flat_parts =
        partition_chunk_by_hash_arrays(&flat_chunk, &[flat_array], 4, true).expect("flat crc");
    let dict_parts =
        partition_chunk_by_hash_arrays(&dict_chunk, &[dict_array], 4, true).expect("dict crc");

    assert_eq!(
        partition_status_values(&dict_parts),
        partition_status_values(&flat_parts)
    );
}

#[test]
fn hash_partition_dictionary_large_utf8_matches_flat_large_utf8_fnv() {
    let (flat_chunk, flat_array, dict_chunk, dict_array) = flat_and_dict_large_status_chunks();

    let flat_parts =
        partition_chunk_by_hash_arrays(&flat_chunk, &[flat_array], 4, false).expect("flat fnv");
    let dict_parts =
        partition_chunk_by_hash_arrays(&dict_chunk, &[dict_array], 4, false).expect("dict fnv");

    assert_eq!(
        partition_status_values(&dict_parts),
        partition_status_values(&flat_parts)
    );
}

#[test]
fn hash_partition_dictionary_large_utf8_matches_flat_large_utf8_crc32() {
    let (flat_chunk, flat_array, dict_chunk, dict_array) = flat_and_dict_large_status_chunks();

    let flat_parts =
        partition_chunk_by_hash_arrays(&flat_chunk, &[flat_array], 4, true).expect("flat crc");
    let dict_parts =
        partition_chunk_by_hash_arrays(&dict_chunk, &[dict_array], 4, true).expect("dict crc");

    assert_eq!(
        partition_status_values(&dict_parts),
        partition_status_values(&flat_parts)
    );
}

fn int64_chunk(rows: usize) -> Chunk {
    let array = Arc::new(arrow::array::Int64Array::from_iter_values(0..rows as i64))
        as arrow::array::ArrayRef;
    let field = Arc::new(Field::new("v", DataType::Int64, false));
    let schema = Arc::new(Schema::new(vec![field.as_ref().clone()]));
    let batch = arrow::array::RecordBatch::try_new(schema, vec![array]).expect("batch");
    let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId(1)],
    )
    .expect("chunk schema");
    Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
}

#[test]
fn split_oversized_chunks_passes_small_chunks_through_unchanged() {
    let chunks = vec![int64_chunk(8)];
    let out = split_oversized_chunks(chunks, 1024 * 1024);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].record_batch().num_rows(), 8);
}

#[test]
fn split_oversized_chunks_slices_large_chunk_into_pieces_with_row_count_preserved() {
    // 10_000 int64 rows ≈ 80 KiB in-memory. Slice at a 8 KiB target. The
    // sliced chunks share the underlying Arrow buffers and therefore
    // still report the same `get_array_memory_size`, but the encoded IPC
    // payload (which is the wire size we actually care about) is sized
    // to the logical row range, so the slice count and row preservation
    // are what matters here.
    let rows = 10_000;
    let target_bytes = 8 * 1024;
    let chunks = vec![int64_chunk(rows)];
    let out = split_oversized_chunks(chunks, target_bytes);
    assert!(out.len() > 1, "expected multiple slices, got {}", out.len());
    let total_rows: usize = out.iter().map(|c| c.record_batch().num_rows()).sum();
    assert_eq!(total_rows, rows);
    for piece in &out {
        assert!(
            piece.record_batch().num_rows() > 0,
            "slice produced zero-row chunk"
        );
    }
}

#[test]
fn split_oversized_chunks_handles_single_row_oversized_chunk() {
    // A chunk with a single row that nonetheless exceeds the target
    // (e.g. a very wide row) should be returned untouched rather than
    // split into zero-row slices.
    let chunks = vec![int64_chunk(1)];
    let out = split_oversized_chunks(chunks, 1);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].record_batch().num_rows(), 1);
}

mod typed_sender_header_tests {
    use super::*;
    use crate::exec::chunk::{ArrayNormalizationStep, ArrayPathStep};
    use crate::exec::pipeline::operator::{
        ConstructionFailure, InputConstructionFactory, InputConstructionOwner,
    };
    use crate::runtime::fragment::ExecutionFailureCause;
    use crate::runtime::mem_tracker::MemTracker;
    use arrow::array::ListArray;
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use novarocks_type_contract::{
        TypedValueBinding, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };

    fn actual_factory(tracker: Arc<MemTracker>) -> Arc<dyn InputConstructionFactory> {
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker,
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap()
    }
    fn source(
        array: ArrayRef,
        value: ValueTypeRef,
        rows: usize,
        factory: &dyn InputConstructionFactory,
    ) -> Chunk {
        let pure = ValueTypeStorageLifetime::pure();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(array.data_type(), pure.clone()).unwrap(),
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(91),
            Arc::new(Field::new("value", array.data_type().clone(), false)),
            binding,
            None,
            pure.clone(),
        )
        .unwrap();
        let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
        let owner = factory.begin_typed_construction(&schema).unwrap();
        owner
            .observe(
                0,
                &[],
                ArrayNormalizationStep::CreateChunkColumns { channels: 1 },
            )
            .unwrap();
        Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            schema,
            vec![array],
            rows,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
        .unwrap()
    }
    fn plain(factory: &dyn InputConstructionFactory) -> Chunk {
        let value = ValueTypeRef::try_new(
            ValueKind::Scalar(DataType::Int32),
            false,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        source(
            Arc::new(Int32Array::from((0..8).collect::<Vec<_>>())),
            value,
            8,
            factory,
        )
    }
    fn nested_dictionary(factory: &dyn InputConstructionFactory) -> Chunk {
        // The unused NULL value is legal: only valid keys 0 and 2 are selected.
        let values = Arc::new(StringArray::from(vec![Some("{}"), None, Some("[]")]));
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 2, 0, 2]), values)
                .unwrap(),
        );
        #[allow(deprecated)]
        let item = Arc::new(Field::new_dict(
            "item",
            dictionary.data_type().clone(),
            true,
            291,
            false,
        ));
        let list: ArrayRef = Arc::new(
            ListArray::try_new(
                item,
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2, 3, 4])),
                dictionary,
                None,
            )
            .unwrap(),
        );
        let pure = ValueTypeStorageLifetime::pure();
        let json = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            true,
            pure.clone(),
        )
        .unwrap();
        let value = ValueTypeRef::try_new(ValueKind::List(json), false, pure).unwrap();
        source(list, value, 4, factory)
    }
    fn install_and_prepare(
        fixture: &Fixture,
        factory: Arc<dyn InputConstructionFactory>,
    ) -> DataStreamSinkOperator {
        let mut sink = make_test_operator(fixture);
        bind_test_operator(fixture, &mut sink);
        sink.set_construction_factory(factory);
        sink.prepare().unwrap();
        sink
    }
    #[test]
    fn oversized_typed_plain_keeps_original_values_and_last_header_hold() {
        let fixture = Fixture::new();
        let tracker = MemTracker::new_root("sender typed plain slice");
        let factory = actual_factory(tracker.clone());
        let source = plain(factory.as_ref());
        let sink = install_and_prepare(&fixture, factory.clone());
        let out = split_oversized_chunks_with_creation(
            vec![source.clone()],
            1,
            sink.construction_factory.as_ref().unwrap().as_ref(),
        )
        .unwrap();
        assert_eq!(out.chunks.len(), 8);
        for (row, piece) in out.chunks.iter().enumerate() {
            assert_eq!(piece.len(), 1);
            assert!(std::ptr::eq(piece.chunk_schema(), source.chunk_schema()));
            let values = piece.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(values.value(0), row as i32);
            assert_eq!(values.null_count(), 0);
        }
        let retained = out.chunks[3].clone();
        drop(out);
        drop(source);
        drop(sink);
        drop(factory);
        assert!(
            tracker.current() > 0,
            "the actual independent slice header/source holds remain"
        );
        assert_eq!(retained.len(), 1);
        drop(retained);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn oversized_typed_nested_dictionary_keeps_json_null_and_encoding_contract() {
        let fixture = Fixture::new();
        let tracker = MemTracker::new_root("sender typed nested dictionary slice");
        let factory = actual_factory(tracker.clone());
        let source = nested_dictionary(factory.as_ref());
        let sink = install_and_prepare(&fixture, factory.clone());
        let out = split_oversized_chunks_with_creation(
            vec![source.clone()],
            1,
            sink.construction_factory.as_ref().unwrap().as_ref(),
        )
        .unwrap();
        assert_eq!(out.chunks.len(), 4);
        for (row, piece) in out.chunks.iter().enumerate() {
            assert_eq!(piece.len(), 1);
            let value = piece.chunk_schema().slots()[0].typed_binding().value_type();
            assert!(!value.nullable());
            let ValueKind::List(item) = value.kind() else {
                panic!("complete List declaration")
            };
            assert!(matches!(
                item.kind(),
                ValueKind::Nominal(ValueLogicalType::Json)
            ));
            assert!(
                item.nullable(),
                "the genuine Arrow child declaration permits the retained dictionary NULL"
            );
            let list = piece.columns()[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let dictionary = list
                .values()
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap();
            let child = dictionary
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let index = list.value_offsets()[0] as usize;
            assert_eq!(
                child.value(dictionary.keys().value(index) as usize),
                if row % 2 == 0 { "{}" } else { "[]" }
            );
            assert_eq!(
                child.null_count(),
                1,
                "the genuine unreferenced dictionary NULL is retained"
            );
            let DataType::List(field) = piece.columns()[0].data_type() else {
                panic!("actual List carrier")
            };
            #[allow(deprecated)]
            {
                assert_eq!(field.dict_id(), Some(291));
                assert_eq!(field.dict_is_ordered(), Some(false));
            }
        }
        drop(out);
        drop(source);
        drop(sink);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    // Test instrumentation delegates every request and lifetime to the real
    // installed tracker factory; it supplies no grant or replacement model.
    struct FirstHeaderRefusalFactory {
        inner: Arc<dyn InputConstructionFactory>,
        tracker: Arc<MemTracker>,
        requests: Arc<AtomicUsize>,
        armed: Arc<AtomicBool>,
        later: Arc<AtomicUsize>,
    }
    struct FirstHeaderRefusalOwner {
        inner: Box<dyn InputConstructionOwner>,
        tracker: Arc<MemTracker>,
        requests: Arc<AtomicUsize>,
        armed: Arc<AtomicBool>,
        later: Arc<AtomicUsize>,
    }
    impl InputConstructionOwner for FirstHeaderRefusalOwner {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            self.inner.storage_lifetime()
        }
        fn request(&self, layout: std::alloc::Layout) -> Result<(), ConstructionFailure> {
            if self.armed.load(Ordering::SeqCst)
                && self.requests.fetch_add(1, Ordering::SeqCst) == 0
            {
                self.tracker
                    .install_limit_once(self.tracker.current() + 1)
                    .unwrap();
            }
            self.inner.request(layout)
        }
        fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
            self.later.fetch_add(1, Ordering::SeqCst);
            self.inner.observe_opaque_upper_bound(bytes)
        }
        fn observe(
            &self,
            ordinal: usize,
            path: &[ArrayPathStep],
            step: ArrayNormalizationStep<'_>,
        ) -> Result<(), ConstructionFailure> {
            self.later.fetch_add(1, Ordering::SeqCst);
            self.inner.observe(ordinal, path, step)
        }
    }
    impl InputConstructionFactory for FirstHeaderRefusalFactory {
        fn observe_input_work(&self, units: usize) -> Result<(), ConstructionFailure> {
            self.later.fetch_add(1, Ordering::SeqCst);
            self.inner.observe_input_work(units)
        }
        fn begin_typed_construction(
            &self,
            schema: &ChunkSchema,
        ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
            Ok(Box::new(FirstHeaderRefusalOwner {
                inner: self.inner.begin_typed_construction(schema)?,
                tracker: self.tracker.clone(),
                requests: self.requests.clone(),
                armed: self.armed.clone(),
                later: self.later.clone(),
            }))
        }
        fn begin_compiled_layout_construction(
            &self,
            layout: &novarocks_local_program::StaticLayout,
        ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
            self.inner.begin_compiled_layout_construction(layout)
        }
    }
    #[test]
    fn oversized_typed_first_header_refusal_is_original_memory_and_no_tail() {
        let fixture = Fixture::new();
        let tracker = MemTracker::new_root("sender first split header refusal");
        let factory = actual_factory(tracker.clone());
        let source = plain(factory.as_ref());
        let requests = Arc::new(AtomicUsize::new(0));
        let armed = Arc::new(AtomicBool::new(false));
        let later = Arc::new(AtomicUsize::new(0));
        let refusing = Arc::new(FirstHeaderRefusalFactory {
            inner: factory.clone(),
            tracker: tracker.clone(),
            requests: requests.clone(),
            armed: armed.clone(),
            later: later.clone(),
        });
        let sink = install_and_prepare(&fixture, refusing);
        // Preparation now constructs its real frozen recipe. This oracle is
        // specifically the later first split-header refusal, on the same host.
        later.store(0, Ordering::SeqCst);
        armed.store(true, Ordering::SeqCst);
        let baseline = tracker.current();
        let refusal = split_oversized_chunks_with_creation(
            vec![source.clone()],
            1,
            sink.construction_factory.as_ref().unwrap().as_ref(),
        )
        .err()
        .expect("original first Vec header request refused");
        assert!(matches!(
            refusal.cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
        ));
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(
            later.load(Ordering::SeqCst),
            0,
            "no measurement, slicing, completion or opaque tail after refusal"
        );
        assert_eq!(tracker.current(), baseline);
        assert_eq!(source.len(), 8);
        assert_eq!(
            source.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(7),
            7
        );
        drop(source);
        drop(sink);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn sender_missing_actual_factory_refuses_before_input_consumption() {
        let fixture = Fixture::new();
        let tracker = MemTracker::new_root("sender missing factory source");
        let factory = actual_factory(tracker.clone());
        let source = plain(factory.as_ref());
        let mut sink = make_test_operator(&fixture);
        bind_test_operator(&fixture, &mut sink);
        let refusal = sink.prepare().unwrap_err();
        assert!(matches!(
            refusal.cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
        ));
        assert!(!sink.prepared);
        let state = fixture.state();
        let refusal = sink.consume_for_test(&state, source.clone()).unwrap_err();
        assert!(matches!(
            refusal.cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
        ));
        assert_eq!(sink.pending_chunk_count_total(), 0);
        assert_eq!(fixture.transport().call_count(), 0);
        assert!(sink.send_group.is_idle());
        assert_eq!(source.len(), 8);
        drop(source);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
}

// This fixture enters through the real frozen-package/local-compiler chain.
// Its roots are explicit test-only Project outputs: it opens no production
// write router or Task binding constructor.
mod conditional_compiled_selection {
    use super::*;
    use arrow::{array::Int64Array, record_batch::RecordBatch};
    use novarocks_connector_contract::PureProviderProgramCatalog;
    use novarocks_functions::{
        ConstantPolicy, EngineFunctionCatalogBuilder, FunctionKind, InstalledPureKernel,
        PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    };
    use novarocks_local_compiler::{
        LocalCompileOptions, compile_fragment, validate_fragment_providers,
    };
    use novarocks_local_program::{KernelAbiVersion, ProgramNodeExpressionRole, ProgramNodeId};
    use novarocks_physical_plan::{
        BinaryOperator, ConstantPools, ExprId, ExprKind, Fragment, FragmentBuilder, FragmentCuts,
        FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentPackageInput, FragmentSink,
        FrozenFragmentCalls, FrozenFragmentPruning, LiteralValue, NodeId, PhysicalExpressionRoots,
        PhysicalRootUses, PipelineDopDomain, PlanLimits, PlanVersionId,
        PropertyProofProjectionLimits, RequiredContracts, ResultField, ResultPort,
        ResultValueDomain, StagePlacementPolicy, ValueId, ValueOrigin,
    };
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand,
        EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
        ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId, FunctionValueType,
        PureCompileControl, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
        SemanticParameterValue, SemanticParameters,
    };
    use std::{collections::BTreeMap, num::NonZeroUsize};

    struct FixtureControl;
    impl PureCompileControl for FixtureControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }

    fn root(expression: u32) -> ProgramExpressionRootSite {
        ProgramExpressionRootSite::Node {
            node: ProgramNodeId::new(2),
            role: ProgramNodeExpressionRole::ProjectOutput { expression },
        }
    }

    fn visit(
        fragment: &Fragment,
        definition: ExprId,
        demand: EvaluationDemand,
        ordered: &mut Vec<ExpressionInvocation<ExprId>>,
    ) -> ExpressionUseId {
        let arguments = match &fragment.expressions().get(definition).unwrap().kind {
            ExprKind::Binary { left, right, .. } => [*left, *right]
                .into_iter()
                .map(|child| visit(fragment, child, EvaluationDemand::Value, ordered))
                .collect::<Vec<_>>(),
            ExprKind::Value(_) | ExprKind::Literal(LiteralValue::Null) => Vec::new(),
            other => panic!("unexpected conditional exchange fixture expression: {other:?}"),
        };
        let use_id = ExpressionUseId::new(u32::try_from(ordered.len()).unwrap());
        ordered.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id,
                domain: EvaluationDomainId::new(0),
                demand,
            },
            definition,
            control: ControlShape::Eager,
            arguments: arguments.into_boxed_slice(),
        });
        use_id
    }

    fn program() -> Arc<LocalProgram> {
        let fragment_id = FragmentId::new(219);
        let source = NodeId::new(10);
        let input = NodeId::new(20);
        let output = NodeId::new(30);
        let integer = FunctionValueType::new(DataType::Int64, true);
        let boolean = FunctionValueType::new(DataType::Boolean, true);
        let mut builder = FragmentBuilder::new(fragment_id);
        builder
            .add_values(source, Box::from([Box::default()]), Box::default())
            .unwrap();
        let mut inputs = Vec::new();
        for ty in [&integer, &integer, &boolean] {
            let expr = builder
                .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
                .unwrap();
            let value = builder
                .add_value(ty.clone(), ValueOrigin::Expr { node: input, expr })
                .unwrap();
            inputs.push((expr, value));
        }
        let input_ids = inputs.iter().map(|(_, id)| *id).collect::<Vec<ValueId>>();
        builder
            .add_project(
                input,
                source,
                inputs.into_boxed_slice(),
                input_ids.clone().into_boxed_slice(),
            )
            .unwrap();
        let left = builder
            .add_expression(output, integer.clone(), ExprKind::Value(input_ids[0]))
            .unwrap();
        let right = builder
            .add_expression(output, integer.clone(), ExprKind::Value(input_ids[1]))
            .unwrap();
        let parameter = SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::AllowThrowException,
        };
        let key = builder
            .add_expression(
                output,
                integer.clone(),
                ExprKind::Binary {
                    op: BinaryOperator::Add,
                    left,
                    right,
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    allow_throw_exception: Some(parameter),
                },
            )
            .unwrap();
        let applies = builder
            .add_expression(output, boolean.clone(), ExprKind::Value(input_ids[2]))
            .unwrap();
        let mut outputs = Vec::new();
        let mut fields = Vec::new();
        for (expr, ty, name) in [(key, integer, "key"), (applies, boolean, "applies")] {
            let value = builder
                .add_value(ty.clone(), ValueOrigin::Expr { node: output, expr })
                .unwrap();
            outputs.push((expr, value));
            fields.push(ResultField {
                domain: ResultValueDomain::Plain,
                name: name.into(),
                alias: None,
                value,
                ty,
            });
        }
        let output_ids = outputs.iter().map(|(_, id)| *id).collect::<Vec<_>>();
        builder
            .add_project(
                output,
                input,
                outputs.into_boxed_slice(),
                output_ids.into_boxed_slice(),
            )
            .unwrap();
        let fragment = builder
            .finish_definition(
                output,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                StagePlacementPolicy::Singleton,
            )
            .unwrap();
        let roots = PhysicalExpressionRoots::try_new(&fragment, &FixtureControl).unwrap();
        let mut ordered = Vec::new();
        let bindings = roots
            .sites()
            .iter()
            .map(|(site, expression)| {
                (
                    *site,
                    visit(&fragment, expression.expr, expression.demand, &mut ordered),
                )
            })
            .collect();
        let flow = ExpressionControlFlow::try_new(
            vec![ExpressionEvaluationDomain {
                id: EvaluationDomainId::new(0),
                parent: None,
                guard: None,
            }],
            ordered,
            fragment.expressions(),
            CompilePhase::Validate,
            &FixtureControl,
        )
        .unwrap();
        let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &FixtureControl).unwrap();
        let calls =
            FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &FixtureControl).unwrap();
        let result = ResultPort {
            scalar_schema: None,
            fragment: fragment_id,
            output: fragment.nodes()[&output].output.clone(),
            fields: fields.into_boxed_slice(),
        };
        let package = Arc::new(
            FragmentPackage::try_new(
                FragmentPackageInput {
                    version: PlanVersionId::try_new([219; 16]).unwrap(),
                    required: RequiredContracts::default(),
                    constants: ConstantPools::empty(),
                    fragment,
                    expression_uses: uses,
                    calls,
                    pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &FixtureControl)
                        .unwrap(),
                    cuts: FragmentCuts::default(),
                    result: Some(result),
                    parameters: SemanticParameters::try_new([(
                        parameter.id,
                        SemanticParameterValue::AllowThrowException(false),
                    )])
                    .unwrap(),
                    scans: BTreeMap::new(),
                    writes: BTreeMap::new(),
                    annotations: Box::default(),
                },
                // Explicit bounded fixture invoices; no production defaults.
                FragmentPackageAdmission {
                    plan_limits: PlanLimits::FROZEN,
                    source_retained_bytes: 64 * 1024 * 1024,
                    property_projection_limits: PropertyProofProjectionLimits {
                        max_request_bytes: 16 * 1024 * 1024,
                        max_coexisting_bytes: 256 * 1024 * 1024,
                        max_projection_work: 16 * 1024 * 1024,
                    },
                },
                &FixtureControl,
            )
            .unwrap(),
        );
        compile_checked(
            package,
            ConstantPolicy {
                max_rows: 16,
                max_array_nodes: 128,
                max_logical_elements: 1024,
                max_retained_buffer_bytes: 1 << 20,
                max_type_depth: 64,
                max_type_nodes: 4096,
                max_dictionary_depth: 64,
                max_metadata_bytes: 1 << 20,
                max_library_validation_work: 1 << 20,
                max_library_validation_bytes: 1 << 20,
            },
        )
    }

    pub(super) fn compile_checked(
        package: Arc<FragmentPackage>,
        constants: ConstantPolicy,
    ) -> Arc<LocalProgram> {
        let providers =
            PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
                .unwrap();
        // The compiler requires a nonempty sealed catalogue even when all
        // roots are intrinsic. Seal the real IF owner, never a mock evaluator.
        let actual = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
        let mut catalogue = EngineFunctionCatalogBuilder::new();
        catalogue
            .register(
                actual
                    .definition("if", FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        let functions = catalogue
            .seal_pure([InstalledPureKernel {
                function: novarocks_type_contract::FunctionId::try_new("builtin.scalar/if/v1")
                    .unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: novarocks_type_contract::FunctionOverloadId::try_new(
                        "builtin.scalar/if/(bool,any<T>,any<T>)->any<T>;widen;legacy",
                    )
                    .unwrap(),
                    implementation: PureImplementationId::try_new("builtin.scalar/if/selected-v1")
                        .unwrap(),
                    abi: PureKernelAbi::ControlIntrinsicV1,
                },
                aggregate_state_format: None,
            }])
            .unwrap();
        Arc::new(
            compile_fragment(
                validate_fragment_providers(package, &providers, &FixtureControl).unwrap(),
                &functions,
                LocalCompileOptions {
                    pipeline_dop: NonZeroUsize::MIN,
                    root_sink_dop: Some(NonZeroUsize::MIN),
                    kernel_abi: KernelAbiVersion::CURRENT,
                    constants,
                },
                &FixtureControl,
            )
            .unwrap(),
        )
    }

    fn input(program: &LocalProgram, applies: Vec<Option<bool>>, left: Vec<i64>) -> Chunk {
        let rows = applies.len();
        assert_eq!(left.len(), rows);
        let layout = program.graph().nodes()[1].output_layout();
        let batch = RecordBatch::try_new(
            Arc::clone(layout.schema()),
            vec![
                Arc::new(Int64Array::from(left)),
                Arc::new(Int64Array::from(vec![1; rows])),
                Arc::new(BooleanArray::from(applies)),
            ],
        )
        .unwrap();
        Chunk::try_new_with_chunk_schema(batch, ChunkSchema::from_compiled_layout(layout).unwrap())
            .unwrap()
    }

    fn sink(fixture: &Fixture, program: Arc<LocalProgram>) -> DataStreamSinkOperator {
        let mut sink = make_test_operator(fixture);
        sink.compiled_partition = Some(CompiledPartitionState {
            keys: CompiledPartitionKeys::conditional(program, vec![root(0)], root(1)),
            instances: None,
            applicability_instances: None,
        });
        prepare_test_operator(fixture, &mut sink);
        sink
    }

    fn evaluate(
        sink: &mut DataStreamSinkOperator,
        chunk: &Chunk,
    ) -> ExecutionResult<CompiledPartitionBatch> {
        let control = RuntimeErrorState::default();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let ctx = StepContext::new(&control, &mut work, &mut conversions);
        sink.compiled_partition_keys(&ctx, chunk)
            .map(|batch| batch.expect("compiled partition"))
    }

    #[test]
    fn conditional_compiled_keys_skip_false_and_null_overflow_rows() {
        let fixture = Fixture::new();
        let program = program();
        let chunk = input(
            &program,
            vec![Some(false), Some(true), None],
            vec![i64::MAX, 6, i64::MAX],
        );
        // Positive control: this real compiled key fails if any excluded
        // payload row is accidentally evaluated across the original batch.
        let mut full_sink = sink(&fixture, program.clone());
        let mut instances = None;
        let control = full_sink.compiled_control.as_ref().unwrap();
        compiled_instances(&mut instances, &program, &[root(0)], control).unwrap();
        let error = evaluate_all(
            &mut instances.as_mut().unwrap()[0],
            root(0),
            chunk.record_batch(),
            control,
        )
        .expect_err("signed addition must overflow on excluded row 0");
        assert!(error.to_string().contains("batch row 0"), "{error}");
        assert!(
            error.to_string().to_ascii_lowercase().contains("overflow"),
            "{error}"
        );
        // Applicability and key instances are owned by the real sink path.
        let batch = evaluate(&mut full_sink, &chunk).unwrap();
        assert_eq!(batch.selected_rows.as_deref(), Some([1].as_slice()));
        assert_eq!(batch.route, CompiledPartitionRoute::ConditionalKeyV1);
        assert_eq!(batch.arrays.len(), 1);
        let keys = batch.arrays[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys.value(0), 7);
        assert_eq!(keys.null_count(), 0);
        let compiled = full_sink.compiled_partition.as_ref().unwrap();
        assert_eq!(compiled.applicability_instances.as_ref().unwrap().len(), 1);
        assert_eq!(compiled.instances.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn conditional_compiled_all_false_or_null_skips_key_instantiation() {
        let fixture = Fixture::new();
        let program = program();
        let chunk = input(
            &program,
            vec![Some(false), None, Some(false)],
            vec![i64::MAX; 3],
        );
        let mut sink = sink(&fixture, program);
        let batch = evaluate(&mut sink, &chunk).unwrap();
        assert_eq!(batch.selected_rows.as_deref(), Some([].as_slice()));
        assert!(batch.arrays.is_empty());
        assert!(batch.types.is_empty());
        let compiled = sink.compiled_partition.as_ref().unwrap();
        assert!(compiled.applicability_instances.is_some());
        assert!(
            compiled.instances.is_none(),
            "inactive key must not even be instantiated"
        );
    }

    #[test]
    fn conditional_compiled_true_overflow_remains_a_required_batch_row_error() {
        let fixture = Fixture::new();
        let program = program();
        let chunk = input(
            &program,
            vec![Some(false), None, Some(true)],
            vec![i64::MAX; 3],
        );
        let mut sink = sink(&fixture, program);
        let error = match evaluate(&mut sink, &chunk) {
            Ok(_) => panic!("selected overflow must fail"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(
            message.contains("batch row 2 (selected ordinal 0)"),
            "{message}"
        );
        assert!(
            message.to_ascii_lowercase().contains("overflow"),
            "{message}"
        );
    }
}

fn routing_batch(
    fixture: &Fixture,
    source: &Chunk,
    route: CompiledPartitionRoute,
    selected_rows: Option<Vec<usize>>,
    values: Vec<Option<i64>>,
) -> CompiledPartitionBatch {
    let mut sink = make_test_operator(fixture);
    prepare_test_operator(fixture, &mut sink);
    let owner = sink
        .construction_factory
        .as_ref()
        .unwrap()
        .begin_input_construction(source)
        .unwrap();
    let (arrays, types) = if values.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        (
            vec![Arc::new(arrow::array::Int64Array::from(values)) as ArrayRef],
            vec![novarocks_type_contract::FunctionValueType::new(
                DataType::Int64,
                true,
            )],
        )
    };
    CompiledPartitionBatch {
        arrays,
        types,
        selected_rows,
        route,
        owner,
    }
}

#[test]
fn conditional_network_routes_true_keys_and_round_robins_false_or_null_rows() {
    let fixture = Fixture::new();
    let source = typed_int64_chunk(6);
    let state = fixture.state();
    let control = state.error_state();
    let mut work =
        crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
    let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = StepContext::new(&control, &mut work, &mut conversions);
    let mask = BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        None,
    ]);
    let batch = routing_batch(
        &fixture,
        &source,
        CompiledPartitionRoute::ConditionalKeyV1,
        Some(vec![0, 3]),
        vec![Some(7), Some(7)],
    );
    assert_eq!(
        selected_partition_rows(&mask, 6, batch.owner.as_ref(), &ctx).unwrap(),
        vec![0, 3]
    );
    let mut cursor = 0;
    let rows = instance_partition_rows(&batch, &source, 6, 3, &mut cursor, &mut ctx).unwrap();
    // Frozen native tuple(7) = 0xba4003266beeb280; InstanceV1 modulo 3 = 0.
    // TRUE 0/3 share that owner; FALSE/NULL 1/2/4/5 use 0/1/2/0.
    assert_eq!(rows, vec![vec![0, 1, 3, 5], vec![2], vec![4]]);
    assert_eq!(cursor, 1);
    let next = routing_batch(
        &fixture,
        &source,
        CompiledPartitionRoute::ConditionalKeyV1,
        Some(Vec::new()),
        Vec::new(),
    );
    assert_eq!(
        instance_partition_rows(&next, &source, 2, 3, &mut cursor, &mut ctx).unwrap(),
        vec![vec![], vec![0], vec![1]]
    );
    assert_eq!(cursor, 0, "per-producer bypass continues across batches");
}

#[test]
fn all_bypass_needs_no_key_column_or_key_decode_and_wraps_its_cursor() {
    let fixture = Fixture::new();
    let source = typed_int64_chunk(5);
    let batch = routing_batch(
        &fixture,
        &source,
        CompiledPartitionRoute::ConditionalKeyV1,
        Some(Vec::new()),
        Vec::new(),
    );
    let state = fixture.state();
    let control = state.error_state();
    let mut work =
        crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
    let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = StepContext::new(&control, &mut work, &mut conversions);
    let mut cursor = 2;
    assert_eq!(
        instance_partition_rows(&batch, &source, 5, 3, &mut cursor, &mut ctx).unwrap(),
        vec![vec![1, 4], vec![2], vec![0, 3]]
    );
    assert_eq!(cursor, 1);
}

#[test]
fn selected_network_target_key_cannot_acquire_the_null_identity() {
    let fixture = Fixture::new();
    use novarocks_failure_contract::FailureProjection;
    let source = typed_int64_chunk(3);
    let batch = routing_batch(
        &fixture,
        &source,
        CompiledPartitionRoute::ConditionalKeyV1,
        Some(vec![1]),
        vec![None],
    );
    let state = fixture.state();
    let control = state.error_state();
    let mut work =
        crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
    let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = StepContext::new(&control, &mut work, &mut conversions);
    let failure = instance_partition_rows(&batch, &source, 3, 4, &mut 0, &mut ctx).unwrap_err();
    let expected = ExecutionFailure::from_domain(novarocks_value_arrays::ValueError::Invariant(
        novarocks_value_arrays::ValueViolation::RequiredNull,
    ));
    assert_eq!(failure.failure_fact(), expected.failure_fact());
}

#[test]
fn hierarchical_network_uses_frozen_instance_domain_and_keeps_ordinary_mapping() {
    let fixture = Fixture::new();
    let source = typed_int64_chunk(3);
    let batch = routing_batch(
        &fixture,
        &source,
        CompiledPartitionRoute::HierarchicalV1,
        None,
        vec![Some(1), Some(2), Some(3)],
    );
    let state = fixture.state();
    let control = state.error_state();
    let mut work =
        crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
    let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = StepContext::new(&control, &mut work, &mut conversions);
    assert_eq!(
        instance_partition_rows(&batch, &source, 3, 4, &mut 0, &mut ctx).unwrap(),
        vec![vec![], vec![0], vec![1], vec![2]]
    );
    // Ordinary FNV modulo 4 remains [2, 1, 0], with no level mixing.
    let ordinary = partition_chunk_by_hash_arrays(&source, &batch.arrays, 4, false).unwrap();
    let values = |chunk: &Chunk| {
        if chunk.is_empty() {
            vec![]
        } else {
            chunk.columns()[0]
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        }
    };
    assert_eq!(
        ordinary.iter().map(values).collect::<Vec<_>>(),
        vec![vec![2], vec![1], vec![0], vec![]]
    );
}

#[test]
fn network_partition_scope_preserves_the_exact_originating_stop() {
    let fixture = Fixture::new();
    let source = typed_int64_chunk(2);
    let batch = routing_batch(
        &fixture,
        &source,
        CompiledPartitionRoute::HierarchicalV1,
        None,
        vec![Some(1), Some(2)],
    );
    let state = fixture.state();
    let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Operational(
        novarocks_functions::KernelDiagnostic::new("network partition original failure"),
    ));
    let control = state.error_state();
    control.set_error(original.clone());
    let mut work =
        crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
    let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = StepContext::new(&control, &mut work, &mut conversions);
    assert_eq!(
        instance_partition_rows(&batch, &source, 2, 3, &mut 0, &mut ctx).unwrap_err(),
        original
    );
}

#[test]
fn async_send_failure_remains_visible_while_task_conclusion_owner_is_deferred() {
    use crate::runtime::task_control::test_support::DeferredOwner;
    use novarocks_execution_contract::{TaskConclusionPort, TaskFailurePhase};
    use novarocks_failure_contract::FailureProjection;
    let owner = DeferredOwner::new();
    let state = Arc::new(RuntimeErrorState::for_task(Arc::new(
        owner.view(TaskFailurePhase::Execution),
    )));
    let fixture = Fixture::with_error(state.clone());
    let transport = fixture.transport();
    let failure = crate::runtime::fragment::io::FragmentIoError::new(
        crate::runtime::fragment::io::FragmentIoOperation::ExchangeTransmit,
        crate::runtime::fragment::io::FragmentIoErrorKind::Unavailable,
        "actual receiver transport failure",
    )
    .failure_fact();
    transport.fail_delivery(failure);
    let mut sink = make_test_operator(&fixture);
    sink.error_state = Some(state.clone());
    sink.transmitter = transport;
    gate_destinations(&mut sink, vec![make_test_destination(&fixture)]);
    prepare_test_operator(&fixture, &mut sink);
    open_edges(&sink, &[edge_id(1)]);
    let destination = sink.destinations()[0].clone();
    sink.end_of_stream_sent.store(true, Ordering::Release);
    sink.finishing.store(true, Ordering::Release);
    assert!(matches!(
        sink.try_enqueue_payload(&destination, test_payload(3)),
        Ok(PayloadEnqueue::Enqueued)
    ));
    assert!(sink.send_group.is_idle());
    assert!(owner.accepted().is_none());
    assert!(state.error().is_none());
    let original = sink
        .send_group
        .failure()
        .expect("actual transport refusal survives deferred acceptance");
    assert_eq!(original.failure_fact(), failure);
    assert!(sink.current_error().unwrap().same_occurrence(&original));
    assert!(!sink.maybe_mark_finished());
    owner.advance_all();
    assert!(state.error().unwrap().same_occurrence(&original));
    assert!(!sink.maybe_mark_finished());
}

#[test]
fn required_withdrawal_cannot_skip_parked_payload_or_end_even_after_earlier_poll() {
    let fixture = Fixture::new();
    for pending in [false, true] {
        let mut sink = make_test_operator(&fixture);
        sink.input.delivery = novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain;
        let destination = FragmentDestination::new(
            UniqueId::new(701, 702),
            RuntimeEndpoint::new("127.0.0.1", 9030).unwrap(),
            sink.fragment_instance_id,
            0,
            1,
        )
        .unwrap()
        .with_task_identity(fixture.destination_task(1));
        gate_destinations(&mut sink, vec![destination]);
        prepare_test_operator(&fixture, &mut sink);
        let gates = sink.edge_gates.as_ref().unwrap().clone();
        sink.ensure_pending_buffers_initialized();
        if pending {
            sink.pending_payloads_per_dest[0] = Some(test_payload(3));
        }
        let gate = gates
            .gate_for_destination(ExchangeDestinationKey::new(
                UniqueId::new(701, 702),
                sink.input.dest_node_id,
            ))
            .unwrap();
        gate.close_for_normal_cancellation();
        assert!(
            if pending {
                sink.flush_pending(true)
            } else {
                sink.send_eos()
            }
            .is_err()
        );
        assert!(!sink.end_of_stream_sent.load(Ordering::Acquire));
        if pending {
            assert!(sink.pending_payloads_per_dest[0].is_some());
        }
    }
}

#[path = "worker_stack_tests.rs"]
mod worker_stack_tests;
