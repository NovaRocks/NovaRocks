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

//! Actual executor-worker sender oracle. Source and transport are mechanical;
//! Pipeline, Runtime creator, sink, encoder, cancellation and joins are real.

use super::*;
use crate::exec::pipeline::{
    fragment_context::FragmentContext,
    global_driver_executor::{DriverTask, FragmentCompletion, GlobalDriverExecutor},
    operator::{Operator, OperatorLifecycle},
    operator_factory::OperatorFactory,
    pipeline::Pipeline,
    schedule::observer::Observable,
    shape::{Source, SourceKernel},
    step::{StepResult, WaitFact, WaitReason},
    step_context::StepContext,
};
use crate::runtime::fragment::io::exchange_unit::ExchangeUnitItem;
use crate::runtime::{
    execution_runtime::{ExecutionRuntime, ExecutionRuntimeConfig},
    mem_tracker::MemTracker,
};
use arrow::array::{Int64Array, ListArray, MapArray, StructArray};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use novarocks_execution_contract::{
    CancelReason, TaskConclusion, TaskConclusionPort, TaskConclusionProposal, TerminationDetail,
};
use novarocks_failure_contract::FailureProjection;
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use std::ptr::NonNull;

#[path = "worker_stack_hash.rs"]
mod hash_program;

type Trace = Arc<Mutex<Vec<(&'static str, String)>>>;
fn event(trace: &Trace, kind: &'static str) {
    trace.lock().unwrap().push((
        kind,
        std::thread::current()
            .name()
            .unwrap_or("unnamed")
            .to_owned(),
    ));
}
#[derive(Clone, Copy, Debug)]
enum Shape {
    Struct,
    List,
    Map,
    Dictionary,
    CountOnly,
}

struct LeafOwner {
    values: Box<[i64]>,
    trace: Trace,
}
impl Drop for LeafOwner {
    fn drop(&mut self) {
        event(&self.trace, "leaf.drop");
    }
}
fn leaf(trace: Trace) -> ArrayRef {
    let source = Arc::new(LeafOwner {
        values: Box::from([37i64]),
        trace,
    });
    let pointer = NonNull::new(source.values.as_ptr().cast_mut().cast::<u8>()).unwrap();
    let length = std::mem::size_of_val(source.values.as_ref());
    // SAFETY: the immutable Box<i64> retains its aligned allocation for every
    // Buffer alias, and the pointer is never written through by this test.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, length, source) };
    Arc::new(Int64Array::new(ScalarBuffer::new(buffer, 0, 1), None))
}
fn source_chunk(shape: Shape, trace: Trace) -> Chunk {
    if matches!(shape, Shape::CountOnly) {
        return zero_column_chunk_with_rows(3);
    }
    let array: ArrayRef = if matches!(shape, Shape::Dictionary) {
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 1, 0]),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
            )
            .unwrap(),
        )
    } else {
        let mut array = leaf(trace);
        for _ in 1..64 {
            array = match shape {
                Shape::Struct => Arc::new(StructArray::new(
                    vec![Arc::new(Field::new(
                        "child",
                        array.data_type().clone(),
                        false,
                    ))]
                    .into(),
                    vec![array],
                    None,
                )),
                Shape::List => Arc::new(ListArray::new(
                    Arc::new(Field::new("item", array.data_type().clone(), false)),
                    OffsetBuffer::new(vec![0i32, 1].into()),
                    array,
                    None,
                )),
                Shape::Map => {
                    let entries = StructArray::new(
                        vec![
                            Arc::new(Field::new("key", DataType::Int32, false)),
                            Arc::new(Field::new("value", array.data_type().clone(), false)),
                        ]
                        .into(),
                        vec![Arc::new(Int32Array::from(vec![7])), array],
                        None,
                    );
                    Arc::new(MapArray::new(
                        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                        OffsetBuffer::new(vec![0i32, 1].into()),
                        entries,
                        None,
                        false,
                    ))
                }
                _ => unreachable!(),
            };
        }
        array
    };
    let value = FunctionValueType::new(array.data_type().clone(), false);
    if matches!(shape, Shape::Struct | Shape::List | Shape::Map) {
        assert_eq!(value.value_type().depth(), 64);
        let mut nodes = 0;
        novarocks_type_contract::validate_arrow_layout_structure_with_profile_observed::<
            novarocks_type_contract::ValueTypeError,
        >(
            value.data_type(),
            novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
            |visit| {
                if matches!(visit, novarocks_type_contract::ValueTypeVisit::TypeNode(_)) {
                    nodes += 1;
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(nodes, if matches!(shape, Shape::Map) { 190 } else { 64 });
        // Each of the 63 Map edges adds its entries Struct: physical127.
    }
    let pure = ValueTypeStorageLifetime::pure();
    let slot = ChunkSlotSchema::try_new_typed(
        SlotId::new(1),
        // The actual Field retains its runtime Dictionary carrier. Its complete
        // plain value binding above remains independently frozen.
        Arc::new(Field::new("v", array.data_type().clone(), false)),
        value.typed_binding().clone(),
        None,
        pure.clone(),
    )
    .unwrap();
    let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
    let rows = array.len();
    Chunk::try_new_typed_with_columns_observed::<ExecutionFailure, _>(
        schema,
        vec![array],
        rows,
        |_| Ok(()),
    )
    .unwrap()
}

struct OneSource {
    chunk: Option<Chunk>,
    trace: Trace,
    wait: Arc<Observable>,
    finish_after_chunk: bool,
}
impl Drop for OneSource {
    fn drop(&mut self) {
        event(&self.trace, "source.drop");
    }
}
impl OperatorLifecycle for OneSource {
    fn name(&self) -> &str {
        "A04 real worker source"
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        if let Some(chunk) = &mut self.chunk {
            chunk.transfer_to(&tracker);
        }
    }
}
impl SourceKernel for OneSource {
    fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        event(&self.trace, "source.poll");
        if let Some(chunk) = self.chunk.take() {
            let rows = chunk.len() as u64;
            return Ok(StepResult::new(Some(chunk), NextStep::Runnable, rows));
        }
        if self.finish_after_chunk {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        Ok(StepResult::new(
            None,
            NextStep::Wait(WaitFact {
                reason: WaitReason::Input,
                observable: self.wait.clone(),
                observed_generation: self.wait.generation(),
                deadline: None,
            }),
            0,
        ))
    }
}
struct OneSourceFactory {
    chunk: Mutex<Option<Chunk>>,
    trace: Trace,
    finish_after_chunk: bool,
}
impl OperatorFactory for OneSourceFactory {
    fn name(&self) -> &str {
        "A04 real worker source"
    }
    fn is_source(&self) -> bool {
        true
    }
    fn create(&self, dop: i32, _: i32) -> Box<dyn Operator> {
        assert_eq!(dop, 1);
        Box::new(Source::new(OneSource {
            chunk: self.chunk.lock().unwrap().take(),
            trace: self.trace.clone(),
            wait: Arc::new(Observable::new()),
            finish_after_chunk: self.finish_after_chunk,
        }))
    }
}

fn runtime() -> Arc<ExecutionRuntime> {
    Arc::new(
        ExecutionRuntime::new(
            ExecutionRuntimeConfig {
                driver_threads: 1,
                exchange_io_max_inflight_bytes: 64 * 1024 * 1024,
                // Flush the actual one-row batch during push, before the source waits.
                exchange_max_transmit_batched_bytes: 1,
                operator_buffer_chunks: 1,
                local_exchange_buffer_mem_limit_per_driver: 1024,
                local_exchange_max_buffered_rows: 1024,
                runtime_filter_scan_wait_time_ms_override: None,
                runtime_filter_wait_timeout_ms_override: None,
                sink_io_worker_threads: 1,
                sink_io_max_blocking_threads: 1,
            },
            crate::runtime::execution_runtime::test_execution_function_set(),
            crate::runtime::execution_runtime::test_memory_authority(),
        )
        .unwrap(),
    )
}
struct Running {
    executor: Arc<GlobalDriverExecutor>,
    state: Arc<RuntimeState>,
    completion: Arc<FragmentCompletion>,
    task_owner: Arc<crate::runtime::task_control::test_support::Owner>,
    joined: bool,
}
impl Running {
    fn cancel_and_join(&mut self) {
        assert!(
            self.state.error().is_none(),
            "unexpected original failure before cancellation: {:?}",
            self.state.error()
        );
        // A StopObservation is not an originating failure proposal. Only the
        // original Task owner may accept this cancellation before drivers see it.
        self.task_owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let accepted = self
            .task_owner
            .accepted()
            .expect("original accepted cancellation");
        assert!(matches!(
            accepted.neutral().conclusion(),
            TaskConclusion::Terminated(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded
            ))
        ));
        assert!(accepted.originating_failure().is_none());
        let cancellation = self.state.error().expect("original Task stop observation");
        assert_eq!(
            cancellation.failure_fact(),
            novarocks_failure_contract::FailureFact::stop_observation(
                crate::runtime::fragment::ExecutionReason::TaskControlObservedStop.reason(),
            ),
        );
        self.executor
            .shutdown()
            .expect("actual worker scheduler join");
        self.joined = true;
        let stopped = self
            .completion
            .stopped_fact()
            .expect("real stopped fact after join");
        assert_eq!(
            stopped.conclusion().unwrap_err().failure_fact(),
            cancellation.failure_fact()
        );
        assert!(
            Arc::ptr_eq(&accepted, &self.task_owner.accepted().unwrap()),
            "shutdown preserves the exact original Task receipt"
        );
    }
    fn finish_and_join(&mut self) {
        // A count-only source reaches normal EOS so the real sink performs
        // its existing forced flush. This proves local fragment exit only.
        let stopped = self
            .completion
            .stopped_fact()
            .expect("source EOS and all real sink work exited");
        assert_eq!(stopped.conclusion(), Ok(()));
        assert!(self.state.error().is_none(), "{:?}", self.state.error());
        self.executor
            .shutdown()
            .expect("actual completed worker scheduler join");
        self.joined = true;
        assert_eq!(self.completion.stopped_fact().unwrap().conclusion(), Ok(()));
        assert!(
            self.task_owner.accepted().is_none(),
            "local fragment exit grants no whole-Task success"
        );
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        if !self.joined {
            self.task_owner.submit(TaskConclusionProposal::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            ));
            let _ = self.executor.shutdown();
        }
    }
}
fn verify_values(shape: Shape, chunk: &Chunk, deep_column: usize) {
    assert_eq!(
        chunk.len(),
        if matches!(shape, Shape::Dictionary | Shape::CountOnly) {
            3
        } else {
            1
        }
    );
    if matches!(shape, Shape::CountOnly) {
        assert!(chunk.columns().is_empty());
        return;
    }
    let mut current = chunk.columns()[deep_column].clone();
    if matches!(shape, Shape::Dictionary) {
        let dict = current
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("real DictionaryBatch representation");
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (row, expected) in ["alpha", "beta", "alpha"].into_iter().enumerate() {
            assert!(!dict.is_null(row));
            assert_eq!(values.value(dict.keys().value(row) as usize), expected);
        }
        return;
    }
    for _ in 1..64 {
        assert_eq!(current.null_count(), 0);
        current = match shape {
            Shape::Struct => current
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(0)
                .clone(),
            Shape::List => {
                let list = current.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(list.value_offsets(), &[0, 1]);
                list.values().clone()
            }
            Shape::Map => {
                let map = current.as_any().downcast_ref::<MapArray>().unwrap();
                assert_eq!(map.value_offsets(), &[0, 1]);
                assert_eq!(
                    map.keys()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(0),
                    7
                );
                map.values().clone()
            }
            _ => unreachable!(),
        };
    }
    assert_eq!(current.null_count(), 0);
    assert_eq!(
        current
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        37
    );
}

fn assert_permissive_map64(source: &Chunk, column: usize) {
    let mut actual = source.columns()[column].as_ref();
    let mut logical = source.chunk_schema().slots()[column]
        .typed_binding()
        .value_type();
    assert_eq!(logical.depth(), 64);
    assert!(!source.chunk_schema().slots()[column].nullable());
    assert_ne!(
        actual.data_type(),
        source.chunk_schema().slots()[column]
            .typed_binding()
            .layout()
            .data_type()
    );
    for _ in 1..64 {
        assert!(
            !logical.nullable(),
            "physical nullable metadata never widens the frozen logical value"
        );
        assert_eq!(actual.null_count(), 0);
        let map = actual.as_any().downcast_ref::<MapArray>().unwrap();
        let DataType::Map(entries, _) = map.data_type() else {
            unreachable!()
        };
        assert!(
            !entries.is_nullable(),
            "Map entries remain required Arrow scaffolding"
        );
        let DataType::Struct(fields) = entries.data_type() else {
            unreachable!()
        };
        assert!(
            !fields[0].is_nullable(),
            "Map keys remain required Arrow scaffolding"
        );
        assert!(
            fields[1].is_nullable(),
            "the actual decoded Unit keeps its permissive value carrier"
        );
        logical = logical.child_type_at(1).unwrap();
        actual = map.values().as_ref();
    }
    assert!(!logical.nullable());
    assert_eq!(actual.null_count(), 0);
    assert_eq!(
        actual
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        37
    );
}

fn run_worker(
    shape: Shape,
    partition: DataStreamPartitionType,
    destinations: usize,
    expect_key_rejection: bool,
) {
    run_worker_with_key(shape, partition, destinations, expect_key_rejection, false)
}

fn run_worker_with_key(
    shape: Shape,
    partition: DataStreamPartitionType,
    destinations: usize,
    expect_key_rejection: bool,
    compiled_hash: bool,
) {
    run_worker_with_input(
        shape,
        partition,
        destinations,
        expect_key_rejection,
        compiled_hash,
        false,
    )
}

fn run_worker_with_input(
    shape: Shape,
    partition: DataStreamPartitionType,
    destinations: usize,
    expect_key_rejection: bool,
    compiled_hash: bool,
    decoded_input: bool,
) {
    let trace: Trace = Arc::new(Mutex::new(Vec::new()));
    // The upstream codec fixture's original source is not the decoded Driver
    // input. Keep its destructor evidence distinct from the Driver's custody.
    let upstream_trace: Trace = Arc::new(Mutex::new(Vec::new()));
    let mut source = source_chunk(
        shape,
        if decoded_input {
            upstream_trace.clone()
        } else {
            trace.clone()
        },
    );
    let mut deep_column = 0;
    let compiled_keys = if compiled_hash {
        let (program, site) = if matches!(shape, Shape::CountOnly) {
            hash_program::constant_key()
        } else {
            let deep = FunctionValueType::try_from_typed_binding(
                source.chunk_schema().slots()[0].typed_binding().clone(),
            )
            .unwrap();
            let compiled = hash_program::id_key(&deep);
            let layout = compiled.0.graph().nodes()[0].output_layout();
            // The independent required id is an actual input/output slot.
            // It never changes the nested value or becomes a marker field.
            let id: ArrayRef = Arc::new(Int64Array::from(vec![41i64; source.len()]));
            let batch = arrow::record_batch::RecordBatch::try_new(
                layout.schema().clone(),
                vec![id, source.columns()[0].clone()],
            )
            .unwrap();
            source = Chunk::try_new_with_chunk_schema(
                batch,
                ChunkSchema::from_compiled_layout(layout).unwrap(),
            )
            .unwrap();
            deep_column = 1;
            compiled
        };
        assert_eq!(
            program.graph().nodes()[0]
                .output_layout()
                .schema()
                .fields()
                .len(),
            source.columns().len()
        );
        Some(CompiledPartitionKeys::new(program, vec![site]))
    } else {
        None
    };
    let expected = source.chunk_schema_ref();
    let frozen = frozen_test_output(&expected);
    if matches!(shape, Shape::Dictionary) {
        assert!(matches!(
            expected.slots()[0].data_type(),
            DataType::Dictionary(..)
        ));
        assert_eq!(frozen.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(
            frozen.value_binding_at(0).unwrap(),
            expected.slots()[0].typed_binding()
        );
    }
    let runtime = runtime();
    let (fixture, task_owner) = Fixture::with_runtime(runtime.clone());
    let state = Arc::new(fixture.state());
    let tracker = state.mem_tracker().expect("real runtime query tracker");
    let (decoded_input_tracker, decoded_source) = if decoded_input {
        assert!(matches!(shape, Shape::Map) && compiled_hash && deep_column == 1);
        let decoded_tracker = MemTracker::new_root("A02 actual permissive Driver input");
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            decoded_tracker.clone(), state.error_state(),
        ).unwrap();
        let receiver = factory.create_compiled_chunk_schema(&frozen).unwrap();
        let recipe =
            crate::runtime::exchange::online_unit::prepare_recipe(&receiver, &[], factory.as_ref())
                .unwrap();
        let mut control = RuntimeKernelControl::new(state.error_state());
        control.bind_runtime_memory(&state);
        control.bind_construction_factory(factory.clone());
        let unit = crate::runtime::exchange::online_unit::encode(
            &recipe,
            &source,
            factory.as_ref(),
            &control,
        )
        .unwrap();
        assert_eq!(&unit[..4], b"NRU1");
        let mut chunks = crate::runtime::exchange::online_unit::decode(
            &recipe,
            &unit,
            &receiver,
            factory.as_ref(),
        )
        .unwrap();
        assert_eq!(chunks.len(), 1);
        // Move the actual decoded Chunk unchanged. No metadata projection back
        // to required fields may hide this A02 -> compiled Driver boundary.
        source = chunks.pop().unwrap();
        assert_eq!(source.len(), 1);
        assert_eq!(source.chunk_schema().slots().len(), expected.slots().len());
        for (actual, expected) in source.chunk_schema().slots().iter().zip(expected.slots()) {
            assert_eq!(
                actual.typed_binding(),
                expected.typed_binding(),
                "the original complete required FVT stays exact"
            );
        }
        assert_permissive_map64(&source, deep_column);
        verify_values(shape, &source, deep_column);
        let actual = Arc::downgrade(&source.columns()[deep_column]);
        drop((chunks, unit, recipe, receiver, control, factory));
        assert!(actual.upgrade().is_some());
        assert!(
            decoded_tracker.current() > 0,
            "the actual decoded input keeps its original allocation receipts"
        );
        (Some(decoded_tracker), Some(actual))
    } else {
        (None, None)
    };
    let transport = fixture.transport();
    let dests = (0..destinations)
        .map(|i| destination_with_finst(&fixture, 90 + i as i64))
        .collect::<Vec<_>>();
    let gates = ExchangeEdgeGates::try_new(dests.iter().enumerate().map(|(i, d)| {
        (
            edge_id(i as u32 + 1),
            vec![ExchangeDestinationKey::new(*d.finst_id(), 1)],
        )
    }))
    .unwrap();
    gates
        .open(
            crate::task_execution::domain::EdgeOpenVersion::FIRST,
            &(0..destinations)
                .map(|i| edge_id(i as u32 + 1))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let input = DataStreamSinkFactoryInput::try_from_static_program(
        1,
        partition,
        novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
        Vec::new(),
        dests,
    )
    .unwrap();
    let mut sink =
        DataStreamSinkFactory::new(input, UniqueId::new(1, 2), Some(11), 1, transport.clone())
            .with_frozen_output(&frozen)
            .with_edge_gates(gates);
    if let Some(keys) = compiled_keys {
        sink = sink
            .with_compiled_partition_keys(keys)
            .expect("nonempty compiled Hash root");
    }
    let sender_group = sink.send_group.clone();
    let pipeline = Pipeline::new(
        1,
        vec![
            Box::new(OneSourceFactory {
                chunk: Mutex::new(Some(source)),
                trace: trace.clone(),
                finish_after_chunk: matches!(shape, Shape::CountOnly),
            }),
            Box::new(sink),
        ],
        1,
    );
    let context = Arc::new(FragmentContext::new(
        None,
        state.clone(),
        Some((1, 2)),
        None,
        None,
        Some(0),
    ));
    let drivers = pipeline
        .instantiate_drivers(&context)
        .expect("real runtime-created sink preparation");
    let completion = FragmentCompletion::new(drivers.len(), state.error_state());
    let executor = runtime.driver_executor();
    let tasks = drivers
        .into_iter()
        .map(|driver| {
            DriverTask::new(
                driver,
                completion.clone(),
                context.clone(),
                Duration::from_millis(1),
            )
        })
        .collect();
    let mut running = Running {
        executor: executor.clone(),
        state: state.clone(),
        completion: completion.clone(),
        task_owner,
        joined: false,
    };
    assert!(executor.submit(tasks));
    let deadline = Instant::now() + Duration::from_secs(5);
    let expected_units = if compiled_hash { 1 } else { destinations };
    let normal_eos = matches!(shape, Shape::CountOnly) && !expect_key_rejection;
    while state.error().is_none() && Instant::now() < deadline {
        let ready = if normal_eos {
            completion.stopped_fact().is_some()
        } else {
            transport.call_count() >= expected_units
        };
        if ready {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    if expect_key_rejection {
        let failure = state
            .error()
            .expect("keyless HASH has no compiled key contract");
        assert!(
            failure.detail().contains("missing compiled partition keys"),
            "{failure}"
        );
        assert_eq!(transport.call_count(), 0);
        // The original invariant failure wins; shutdown must still really join.
        executor.shutdown().unwrap();
        assert!(completion.stopped_fact().is_some());
        running.joined = true;
    } else {
        assert!(state.error().is_none(), "{:?}", state.error());
        let (frames, ends): (Vec<_>, Vec<_>) = transport
            .take_frames()
            .into_iter()
            .partition(|frame| matches!(frame.receiver.item, ExchangeUnitItem::Unit { .. }));
        assert_eq!(
            frames.len(),
            expected_units,
            "one immutable Unit per nonempty real destination"
        );
        if normal_eos {
            assert_eq!(
                ends.len(),
                destinations,
                "one real End for every frozen destination"
            );
            let ended = ends
                .iter()
                .map(|frame| {
                    frame
                        .receiver
                        .stream
                        .destination_fragment_instance_id()
                        .low()
                })
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(ended, (0..destinations).map(|i| 90 + i as i64).collect());
            for end in &ends {
                assert!(matches!(end.receiver.item, ExchangeUnitItem::End(_)));
                assert!(end.receiver.payload.is_empty());
                let nonempty = frames
                    .iter()
                    .any(|frame| frame.receiver.stream == end.receiver.stream);
                assert_eq!(
                    end.receiver.item.sequence().get(),
                    if nonempty { 2 } else { 1 }
                );
            }
            assert_eq!(
                completion
                    .stopped_fact()
                    .expect("normal EOS fully exited")
                    .conclusion(),
                Ok(())
            );
        } else {
            assert!(
                ends.is_empty(),
                "input-waiting sources have not authored End"
            );
            assert!(
                completion.stopped_fact().is_none(),
                "the source is still input-waiting before cancellation"
            );
        }
        assert!(
            tracker.current() > 0,
            "real retained payload custody remains charged"
        );
        let decode_tracker = MemTracker::new_root("A04 actual receiver creator");
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            decode_tracker.clone(),state.error_state()).unwrap();
        // Sender and receiver prepare from the same canonical static output.
        // The original actual input still carries Dictionary; the frozen plain
        // root authorizes its existing CanonicalOrDictionary wire representation.
        let receiver = factory.create_compiled_chunk_schema(&frozen).unwrap();
        let recipe =
            crate::runtime::exchange::online_unit::prepare_recipe(&receiver, &[], factory.as_ref())
                .unwrap();
        for frame in &frames {
            if compiled_hash {
                assert_eq!(
                    frame
                        .receiver
                        .stream
                        .destination_fragment_instance_id()
                        .low(),
                    90,
                    "the frozen odd Int64 key takes FNV partition0; an empty-key seed would take1"
                );
            }
            assert!(matches!(frame.receiver.item, ExchangeUnitItem::Unit { .. }));
            let bytes = &frame.receiver.payload;
            let pointer = bytes.as_ptr();
            assert_eq!(&bytes[..4], b"NRU1");
            let decoded = crate::runtime::exchange::online_unit::decode(
                &recipe,
                bytes,
                &receiver,
                factory.as_ref(),
            )
            .unwrap();
            assert_eq!(
                bytes.as_ptr(),
                pointer,
                "decoder borrows the immutable delivered payload"
            );
            assert_eq!(decoded.len(), 1);
            assert_eq!(
                decoded[0].chunk_schema().slots().len(),
                expected.slots().len()
            );
            for (actual, expected) in decoded[0]
                .chunk_schema()
                .slots()
                .iter()
                .zip(expected.slots())
            {
                assert_eq!(
                    actual.typed_binding(),
                    expected.typed_binding(),
                    "complete recursive value binding"
                );
            }
            if deep_column == 1 {
                let id = decoded[0].columns()[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(id.null_count(), 0);
                assert_eq!(id.value(0), 41);
            }
            verify_values(shape, &decoded[0], deep_column);
            drop(decoded);
        }
        drop(recipe);
        drop(receiver);
        drop(factory);
        assert_eq!(
            decode_tracker.current(),
            0,
            "actual receiver allocations fully exited"
        );
        if normal_eos {
            running.finish_and_join();
        } else {
            running.cancel_and_join();
        }
        drop((frames, ends));
    }
    drop(transport.take_frames());
    assert!(sender_group.is_idle());
    assert_eq!(sender_group.retained_payload_bytes(), 0);
    drop(pipeline);
    drop(context);
    drop(completion);
    drop(frozen);
    drop(expected);
    if let Some(decoded_source) = decoded_source {
        assert!(
            decoded_source.upgrade().is_none(),
            "the unchanged decoded input actually exited on the Driver path"
        );
        assert_eq!(
            decoded_input_tracker.as_ref().unwrap().current(),
            0,
            "all original decoded-input allocation receipts were returned after the same Driver join"
        );
        let upstream = upstream_trace.lock().unwrap();
        let current = std::thread::current();
        assert_eq!(upstream.len(), 1);
        assert_eq!(
            upstream[0],
            ("leaf.drop", current.name().unwrap_or("unnamed").to_owned()),
            "the upstream codec fixture's source retired separately before Driver submission"
        );
    }
    assert_eq!(
        tracker.current(),
        0,
        "sender source/schema/payload actual exits refund tracker"
    );
    let trace = trace.lock().unwrap();
    assert!(trace.iter().any(|(kind, _)| *kind == "source.poll"));
    assert!(trace.iter().any(|(kind, _)| *kind == "source.drop"));
    if matches!(shape, Shape::Struct | Shape::List | Shape::Map) && !decoded_input {
        assert_eq!(
            trace
                .iter()
                .filter(|(kind, _)| *kind == "leaf.drop")
                .count(),
            1
        );
    }
    assert!(
        trace
            .iter()
            .all(|(_, name)| name.starts_with("novarocks-driver-")),
        "{trace:?}"
    );
}

#[test]
fn actual_driver_2mib_nonnull_struct64_encodes_and_destroys_on_worker() {
    run_worker(
        Shape::Struct,
        DataStreamPartitionType::Unpartitioned,
        1,
        false,
    );
}
#[test]
fn actual_driver_2mib_nonnull_list64_encodes_and_destroys_on_worker() {
    run_worker(
        Shape::List,
        DataStreamPartitionType::Unpartitioned,
        1,
        false,
    );
}
#[test]
fn actual_driver_2mib_nonnull_map64_physical127_encodes_and_destroys_on_worker() {
    run_worker(Shape::Map, DataStreamPartitionType::Unpartitioned, 1, false);
}
#[test]
fn actual_driver_zero_columns_three_rows_broadcasts_to_two_destinations() {
    run_worker(
        Shape::CountOnly,
        DataStreamPartitionType::Unpartitioned,
        2,
        false,
    );
}
#[test]
fn actual_driver_zero_columns_does_not_invent_empty_hash_key_contract() {
    run_worker(
        Shape::CountOnly,
        DataStreamPartitionType::HashPartitioned,
        2,
        true,
    );
}
#[test]
fn actual_driver_dictionary_unit_uses_original_decoder_and_complete_binding() {
    run_worker(
        Shape::Dictionary,
        DataStreamPartitionType::Unpartitioned,
        1,
        false,
    );
}

#[test]
fn actual_driver_zero_columns_three_rows_hashes_by_admitted_constant_key() {
    run_worker_with_key(
        Shape::CountOnly,
        DataStreamPartitionType::HashPartitioned,
        2,
        false,
        true,
    );
}
#[test]
fn actual_driver_struct64_hashes_by_independent_required_id_slot() {
    run_worker_with_key(
        Shape::Struct,
        DataStreamPartitionType::HashPartitioned,
        2,
        false,
        true,
    );
}
#[test]
fn actual_driver_list64_hashes_by_independent_required_id_slot() {
    run_worker_with_key(
        Shape::List,
        DataStreamPartitionType::HashPartitioned,
        2,
        false,
        true,
    );
}
#[test]
fn actual_driver_map64_physical127_hashes_by_independent_required_id_slot() {
    run_worker_with_key(
        Shape::Map,
        DataStreamPartitionType::HashPartitioned,
        2,
        false,
        true,
    );
}

#[test]
fn actual_driver_map64_decoded_permissive_unit_hashes_by_original_required_id() {
    run_worker_with_input(
        Shape::Map,
        DataStreamPartitionType::HashPartitioned,
        2,
        false,
        true,
        true,
    );
}
