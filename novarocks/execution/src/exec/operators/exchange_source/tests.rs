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
use super::test_support::runtime_with_transport_timeout;
use crate::exec::chunk::Chunk;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::*;
use crate::exec::pipeline::binding::ExchangeBinding;
use crate::exec::pipeline::driver::PipelineDriver;
use crate::exec::pipeline::global_driver_executor::{
    DriverGroupCompletion, DriverRunner, GlobalDriverExecutor,
};
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::task_context::TaskContext;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::io::ExchangeReceiverPort;
use crate::runtime::task::io::exchange::test_support::Fixture;
use novarocks_types::SlotId;

fn int32_chunk(values: Vec<i32>) -> Chunk {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let array = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![array]).expect("test batch");
    crate::exec::chunk::typed_fixture::chunk(
        batch,
        &[SlotId::new(1)],
        &[novarocks_type_contract::FunctionValueType::new(
            DataType::Int32,
            false,
        )],
    )
}

fn int32_wire_layout() -> novarocks_local_program::StaticLayout {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    crate::exec::chunk::typed_fixture::layout(
        schema,
        &[SlotId::new(1)],
        &[novarocks_type_contract::FunctionValueType::new(
            DataType::Int32,
            false,
        )],
    )
}

fn register_int32_receiver(binding: &ExchangeBinding, fixture: &Fixture, sources: &[UniqueId]) {
    let layout = int32_wire_layout();
    let creation_factory = fixture.creation(receiver_key(binding.key).task_key);
    let expected_chunk_schema = creation_factory
        .create_compiled_chunk_schema(&layout)
        .unwrap();
    binding
        .receiver_port
        .register(crate::runtime::task::io::ExchangeReceiverRegistration {
            key: receiver_key(binding.key),
            expected_senders: binding.expected_senders,
            wire_chunk_schema: expected_chunk_schema.clone(),
            expected_chunk_schema,
            column_binding: exchange::ExchangeColumnBinding::Positional,
            creation_factory,
        })
        .expect("register typed receiver");
    fixture.bind(receiver_key(binding.key), sources);
}

fn int32_values(chunk: &Chunk) -> Vec<i32> {
    let array = chunk
        .column_by_slot_id(SlotId::new(1))
        .expect("slot column");
    let ints = array
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int32 array");
    (0..ints.len()).map(|row| ints.value(row)).collect()
}

fn deliver_fixture_unit(
    fixture: &Fixture,
    binding: &ExchangeBinding,
    source: UniqueId,
    ordinal: u32,
    chunk: Option<Chunk>,
    end: bool,
) {
    deliver_fixture_item(fixture, binding, source, ordinal, 1, chunk, end);
}

fn deliver_fixture_item(
    fixture: &Fixture,
    binding: &ExchangeBinding,
    source: UniqueId,
    ordinal: u32,
    sequence: u64,
    chunk: Option<Chunk>,
    end: bool,
) {
    use crate::runtime::task::io::exchange_unit::{
        ExchangeDemandEntry, ExchangeDemandOperation, ExchangeUnitDemand, ExchangeUnitItem,
        ExchangeUnitReceiverFrame, ExchangeUnitSequence,
    };
    let key = receiver_key(binding.key);
    let stream = fixture.stream(source, key.task_key, key.node_id, ordinal);
    let (item, payload) = if let Some(chunk) = chunk {
        let creation = fixture.creation(source);
        let schema = creation
            .create_compiled_chunk_schema(&int32_wire_layout())
            .unwrap();
        let recipe =
            exchange::online_unit::prepare_recipe(&schema, &[], creation.as_ref()).unwrap();
        let control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(
            fixture.state(source).error_state(),
        );
        let payload =
            exchange::online_unit::encode(&recipe, &chunk, creation.as_ref(), &control).unwrap();
        let demand = ExchangeUnitDemand {
            sequence: ExchangeUnitSequence::try_new(sequence).unwrap(),
            wire_bytes: payload.len() as u64,
        };
        binding
            .receiver_port
            .register_unit_demand(
                key,
                ExchangeDemandEntry {
                    stream,
                    operation: ExchangeDemandOperation::Register(demand),
                },
            )
            .unwrap();
        (
            ExchangeUnitItem::Unit {
                demand,
                end_after_unit: end,
            },
            bytes::Bytes::from(payload),
        )
    } else {
        assert!(end, "the empty fixture message is an explicit End");
        (
            ExchangeUnitItem::End(ExchangeUnitSequence::try_new(sequence).unwrap()),
            bytes::Bytes::new(),
        )
    };
    binding
        .receiver_port
        .receive_unit(
            key,
            ExchangeUnitReceiverFrame {
                stream,
                sender_count: u32::try_from(binding.expected_senders).unwrap(),
                sender_id: i32::try_from(ordinal).unwrap(),
                backend_number: 0,
                item,
                payload,
                next_demand: None,
            },
        )
        .unwrap();
}

struct CountingSink {
    rows: Arc<AtomicUsize>,
}
impl OperatorLifecycle for CountingSink {
    fn name(&self) -> &str {
        "COUNTING_SINK"
    }
}
impl crate::exec::pipeline::operator::InputEncodingPolicy for CountingSink {}
impl crate::exec::pipeline::shape::SinkKernel for CountingSink {
    fn consume(
        &mut self,
        _: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
        input: crate::exec::pipeline::input::PreparedInput,
    ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
        let rows = input.into_uncovered_chunk()?.len();
        self.rows.fetch_add(rows, Ordering::AcqRel);
        Ok(crate::exec::pipeline::shape::SinkStep::new(
            crate::exec::pipeline::step::NextStep::NeedInput,
            rows as u64,
        ))
    }
}

fn source_step(source: &mut Box<dyn Operator>, state: &RuntimeState) -> StepResult {
    use crate::exec::pipeline::driver_step::{DEFAULT_TURN_WORK_UNITS, DriverStep, TurnWork};
    let control = state.error_state();
    let mut work = TurnWork::new(std::num::NonZeroU64::MIN, DEFAULT_TURN_WORK_UNITS);
    source
        .as_mut()
        .drive(&mut DriverStep::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
        .expect("source step")
}
fn input_wait(source: &mut Box<dyn Operator>, state: &RuntimeState) -> WaitFact {
    let step = source_step(source, state);
    assert!(step.output.is_none());
    let NextStep::Wait(wait) = step.next else {
        panic!("an actual empty input poll must report its wait");
    };
    assert!(matches!(wait.reason, WaitReason::Input));
    assert!(
        wait.deadline.is_none(),
        "receive waits have no Source deadline"
    );
    wait
}

fn wait_until_parked(parked: &crate::exec::pipeline::driver::DriverScheduleState) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !parked.is_in_blocked() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(
        parked.is_in_blocked(),
        "the real exchange driver did not park"
    );
}

fn assert_quiet_beyond_transport_deadline(
    fixture: &Fixture,
    state: &RuntimeState,
    completion: &DriverGroupCompletion,
    parked: &crate::exec::pipeline::driver::DriverScheduleState,
    observable: &Observable,
) {
    let generation = observable.generation();
    let transport_timeout = fixture.runtime.exchange_flow_limits().transport_timeout();
    std::thread::sleep(transport_timeout.saturating_mul(20));
    assert!(
        state.error().is_none(),
        "an idle receive is not a Task failure"
    );
    assert!(completion.conclusion().is_none());
    assert!(completion.stopped_fact().is_none());
    assert!(parked.is_in_blocked());
    assert_eq!(
        observable.generation(),
        generation,
        "time is not receiver progress"
    );
}

fn test_guard_failure(message: &str) -> crate::runtime::task::ExecutionFailure {
    novarocks_functions::KernelFailure::Internal(novarocks_functions::KernelDiagnostic::new(
        message,
    ))
    .into()
}

fn assert_input_consumed(fixture: &Fixture, binding: &ExchangeBinding, sources: &[UniqueId]) {
    let streams = sources
        .iter()
        .enumerate()
        .map(|(ordinal, source)| {
            fixture.stream(
                *source,
                receiver_key(binding.key).task_key,
                binding.key.node_id,
                u32::try_from(ordinal).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let facts = binding
        .receiver_port
        .stream_close_facts(receiver_key(binding.key), &streams)
        .unwrap();
    assert_eq!(facts.len(), sources.len());
    assert!(facts.iter().all(|fact| matches!(
        fact.disposition,
        novarocks_execution_contract::ExchangeCloseDisposition::Consumed { .. }
    )));
}

struct WaitingPipeline {
    fixture: Arc<Fixture>,
    state: Arc<RuntimeState>,
    observable: Arc<Observable>,
    rows: Arc<AtomicUsize>,
    completion: Arc<DriverGroupCompletion>,
    fragment: Arc<TaskContext>,
    source: Box<dyn Operator>,
    binding: ExchangeBinding,
}

fn waiting_pipeline(key: exchange::ExchangeKey) -> WaitingPipeline {
    let fixture = Fixture::with_runtime(runtime_with_transport_timeout(Duration::from_millis(10)));
    let binding = ExchangeBinding {
        key,
        expected_senders: 1,
        receiver_port: fixture.port.clone(),
    };
    register_int32_receiver(&binding, &fixture, &[UniqueId::new(92_000, 1)]);
    let factory = ExchangeSourceFactory::new_compiled(key.node_id, binding.clone());
    let state = fixture.state(receiver_key(key).task_key);
    let (mut source, observable) = factory.create_source(0);
    source.prepare().expect("prepare exchange source");
    source
        .bind_runtime_state(&state)
        .expect("bind exchange source runtime");
    let rows = Arc::new(AtomicUsize::new(0));
    let completion = DriverGroupCompletion::new(1, state.error_state());
    let fragment = Arc::new(TaskContext::new(
        None,
        Arc::clone(&state),
        Some((key.task_key_hi, key.task_key_lo)),
        None,
        None,
        None,
    ));
    WaitingPipeline {
        fixture,
        state,
        observable,
        rows,
        completion,
        fragment,
        source,
        binding,
    }
}

fn submit_waiting_pipeline(
    executor: &GlobalDriverExecutor,
    state: Arc<RuntimeState>,
    source: Box<dyn Operator>,
    rows: Arc<AtomicUsize>,
    completion: Arc<DriverGroupCompletion>,
    fragment: Arc<TaskContext>,
) -> Arc<crate::exec::pipeline::driver::DriverScheduleState> {
    let driver = PipelineDriver::new(
        0,
        vec![
            source,
            Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                CountingSink { rows },
            )),
        ],
        None,
        Vec::new(),
        state,
        fragment.task_key(),
    );
    let parked = driver.schedule_state();
    let task = DriverRunner::new(driver, completion, fragment, Duration::from_millis(5));
    executor.submit(vec![task]);
    parked
}

#[test]
fn no_first_packet_wait_outlives_transport_deadline_and_real_data_wakes_the_pipeline() {
    let key = exchange::ExchangeKey {
        task_key_hi: 92_001,
        task_key_lo: 92_002,
        node_id: 92_003,
    };
    let WaitingPipeline {
        fixture,
        state,
        observable,
        rows,
        completion,
        fragment,
        mut source,
        binding,
    } = waiting_pipeline(key);
    let wait = input_wait(&mut source, &state);
    assert!(Arc::ptr_eq(&wait.observable, &observable));
    let executor = GlobalDriverExecutor::new(1);
    let parked = submit_waiting_pipeline(
        &executor,
        state.clone(),
        source,
        rows.clone(),
        completion.clone(),
        fragment,
    );
    wait_until_parked(&parked);
    assert_quiet_beyond_transport_deadline(&fixture, &state, &completion, &parked, &observable);
    let before_data = observable.generation();
    deliver_fixture_unit(
        &fixture,
        &binding,
        UniqueId::new(92_000, 1),
        0,
        Some(int32_chunk(vec![7])),
        true,
    );
    completion
        .wait_timeout(
            Duration::from_secs(2),
            test_guard_failure("real data and End did not finish the parked exchange driver"),
        )
        .unwrap();
    assert_eq!(rows.load(Ordering::Acquire), 1);
    assert!(
        observable.generation() > before_data,
        "real input must wake the parked driver"
    );
    assert!(completion.stopped_fact().unwrap().conclusion().is_ok());
    assert!(state.error().is_none());
    assert_input_consumed(&fixture, &binding, &[UniqueId::new(92_000, 1)]);
    binding
        .receiver_port
        .cancel_task(receiver_key(key).task_key);
}

#[test]
fn partial_data_stall_outlives_transport_deadline_and_real_end_rewakes_the_pipeline() {
    let key = exchange::ExchangeKey {
        task_key_hi: 92_011,
        task_key_lo: 92_012,
        node_id: 92_013,
    };
    let WaitingPipeline {
        fixture,
        state,
        observable,
        rows,
        completion,
        fragment,
        mut source,
        binding,
    } = waiting_pipeline(key);
    input_wait(&mut source, &state);
    let executor = GlobalDriverExecutor::new(1);
    let parked = submit_waiting_pipeline(
        &executor,
        state.clone(),
        source,
        rows.clone(),
        completion.clone(),
        fragment,
    );
    wait_until_parked(&parked);
    let before_data = observable.generation();
    deliver_fixture_unit(
        &fixture,
        &binding,
        UniqueId::new(92_000, 1),
        0,
        Some(int32_chunk(vec![7])),
        false,
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while rows.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(rows.load(Ordering::Acquire), 1);
    wait_until_parked(&parked);
    assert!(observable.generation() > before_data);
    assert_quiet_beyond_transport_deadline(&fixture, &state, &completion, &parked, &observable);
    let before_end = observable.generation();
    deliver_fixture_item(
        &fixture,
        &binding,
        UniqueId::new(92_000, 1),
        0,
        2,
        None,
        true,
    );
    completion
        .wait_timeout(
            Duration::from_secs(2),
            test_guard_failure("real End did not finish the partial-data exchange driver"),
        )
        .unwrap();
    assert_eq!(rows.load(Ordering::Acquire), 1);
    assert!(
        observable.generation() > before_end,
        "real End must wake the same parked driver"
    );
    assert!(completion.stopped_fact().unwrap().conclusion().is_ok());
    assert!(state.error().is_none());
    assert_input_consumed(&fixture, &binding, &[UniqueId::new(92_000, 1)]);
    binding
        .receiver_port
        .cancel_task(receiver_key(key).task_key);
}

#[test]
fn sibling_chunk_consumption_preserves_every_driver_receiver_wake_without_deadlines() {
    let key = exchange::ExchangeKey {
        task_key_hi: 92_021,
        task_key_lo: 92_022,
        node_id: 92_023,
    };
    let fixture = Fixture::with_runtime(runtime_with_transport_timeout(Duration::from_millis(10)));
    let binding = ExchangeBinding {
        key,
        expected_senders: 1,
        receiver_port: fixture.port.clone(),
    };
    let sources = [UniqueId::new(92_020, 1)];
    register_int32_receiver(&binding, &fixture, &sources);
    let factory = ExchangeSourceFactory::new_compiled(key.node_id, binding.clone());
    let (mut first, first_signal) = factory.create_source(0);
    let (mut second, second_signal) = factory.create_source(1);
    first.prepare().unwrap();
    second.prepare().unwrap();
    let state = fixture.state(receiver_key(key).task_key);
    first.bind_runtime_state(&state).unwrap();
    second.bind_runtime_state(&state).unwrap();
    let first_wait = input_wait(&mut first, &state);
    let second_wait = input_wait(&mut second, &state);
    assert!(Arc::ptr_eq(&first_wait.observable, &first_signal));
    assert!(Arc::ptr_eq(&second_wait.observable, &second_signal));
    let first_before = first_signal.generation();
    let second_before = second_signal.generation();
    deliver_fixture_unit(
        &fixture,
        &binding,
        sources[0],
        0,
        Some(int32_chunk(vec![11])),
        false,
    );
    let output = source_step(&mut first, &state)
        .output
        .expect("first source chunk");
    assert_eq!(int32_values(&output), vec![11]);
    assert!(first_signal.generation() > first_before);
    assert!(
        second_signal.generation() > second_before,
        "a sibling consuming the chunk cannot swallow another driver's wake"
    );
    let after_packet = input_wait(&mut second, &state);
    assert!(Arc::ptr_eq(&after_packet.observable, &second_signal));
    assert!(after_packet.observed_generation > second_wait.observed_generation);
    let first_after = first_signal.generation();
    let second_after = second_signal.generation();
    std::thread::sleep(
        fixture
            .runtime
            .exchange_flow_limits()
            .transport_timeout()
            .saturating_mul(20),
    );
    input_wait(&mut first, &state);
    input_wait(&mut second, &state);
    assert_eq!(first_signal.generation(), first_after);
    assert_eq!(second_signal.generation(), second_after);
    assert!(state.error().is_none());
    deliver_fixture_item(&fixture, &binding, sources[0], 0, 2, None, true);
    assert!(first_signal.generation() > first_after);
    assert!(second_signal.generation() > second_after);
    assert!(matches!(
        source_step(&mut first, &state).next,
        NextStep::Finished
    ));
    assert!(matches!(
        source_step(&mut second, &state).next,
        NextStep::Finished
    ));
    assert_input_consumed(&fixture, &binding, &sources);
    binding
        .receiver_port
        .cancel_task(receiver_key(key).task_key);
}

#[test]
fn original_task_failure_wakes_a_parked_source_without_input_or_idle_deadline() {
    let key = exchange::ExchangeKey {
        task_key_hi: 92_041,
        task_key_lo: 92_042,
        node_id: 92_043,
    };
    let WaitingPipeline {
        fixture,
        state,
        observable: _,
        rows,
        completion,
        fragment,
        mut source,
        binding,
    } = waiting_pipeline(key);
    input_wait(&mut source, &state);
    let executor = GlobalDriverExecutor::new(1);
    let parked = submit_waiting_pipeline(
        &executor,
        state.clone(),
        source,
        rows.clone(),
        completion.clone(),
        fragment,
    );
    wait_until_parked(&parked);
    let original =
        test_guard_failure("original Task failure while the exchange input is still pending");
    state.error_state().set_error(original.clone());
    let failure = completion
        .wait_timeout(
            Duration::from_secs(2),
            test_guard_failure("original Task failure did not wake the parked exchange driver"),
        )
        .unwrap_err();
    assert!(failure.same_occurrence(&original));
    assert!(state.error().unwrap().same_occurrence(&original));
    assert!(
        completion
            .stopped_fact()
            .unwrap()
            .conclusion()
            .unwrap_err()
            .same_occurrence(&original)
    );
    assert_eq!(rows.load(Ordering::Acquire), 0);
    assert!(
        binding
            .receiver_port
            .stream_close_facts(
                receiver_key(key),
                &[fixture.stream(
                    UniqueId::new(92_000, 1),
                    receiver_key(key).task_key,
                    key.node_id,
                    0
                )]
            )
            .unwrap()
            .iter()
            .all(|fact| !matches!(
                fact.disposition,
                novarocks_execution_contract::ExchangeCloseDisposition::Consumed { .. }
            ))
    );
    binding
        .receiver_port
        .cancel_task(receiver_key(key).task_key);
}

#[test]
fn every_parked_driver_observes_the_shared_three_sender_eos() {
    let key = exchange::ExchangeKey {
        task_key_hi: 92_031,
        task_key_lo: 92_032,
        node_id: 92_033,
    };
    let fixture = Fixture::new();
    let sources = [
        UniqueId::new(92_030, 1),
        UniqueId::new(92_030, 2),
        UniqueId::new(92_030, 3),
    ];
    let binding = ExchangeBinding {
        key,
        expected_senders: 3,
        receiver_port: fixture.port.clone(),
    };
    register_int32_receiver(&binding, &fixture, &sources);
    let factory = ExchangeSourceFactory::new_compiled(key.node_id, binding.clone());
    let state = fixture.state(receiver_key(key).task_key);
    let fragment = Arc::new(TaskContext::new(
        None,
        Arc::clone(&state),
        Some((key.task_key_hi, key.task_key_lo)),
        None,
        None,
        None,
    ));
    let completion = DriverGroupCompletion::new(5, state.error_state());
    let rows = Arc::new(AtomicUsize::new(0));
    let mut parked = Vec::new();
    let mut tasks = Vec::new();
    for driver_id in 0..5 {
        let (mut source, _) = factory.create_source(driver_id);
        source.prepare().expect("prepare exchange source");
        source
            .bind_runtime_state(&state)
            .expect("bind exchange source runtime");
        input_wait(&mut source, &state);
        let driver = PipelineDriver::new(
            driver_id,
            vec![
                source,
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    CountingSink {
                        rows: Arc::clone(&rows),
                    },
                )),
            ],
            None,
            Vec::new(),
            Arc::clone(&state),
            fragment.task_key(),
        );
        parked.push(driver.schedule_state());
        tasks.push(DriverRunner::new(
            driver,
            Arc::clone(&completion),
            Arc::clone(&fragment),
            Duration::from_millis(5),
        ));
    }

    let executor = GlobalDriverExecutor::new(2);
    executor.submit(tasks);
    let park_deadline = Instant::now() + Duration::from_secs(1);
    while parked.iter().any(|state| !state.is_in_blocked()) && Instant::now() < park_deadline {
        std::thread::yield_now();
    }
    assert!(
        parked.iter().all(|state| state.is_in_blocked()),
        "every high-DOP exchange driver must be parked before EOS"
    );

    for (ordinal, source) in sources.into_iter().enumerate() {
        deliver_fixture_unit(
            &fixture,
            &binding,
            source,
            u32::try_from(ordinal).unwrap(),
            None,
            true,
        );
    }

    completion
        .wait_timeout(
            Duration::from_secs(1),
            novarocks_functions::KernelFailure::Internal(
                novarocks_functions::KernelDiagnostic::new(
                    "shared exchange EOS did not finish every parked driver",
                ),
            )
            .into(),
        )
        .expect("all exchange drivers finish after the third sender EOS");
    assert_eq!(rows.load(Ordering::Acquire), 0);
    assert_input_consumed(&fixture, &binding, &sources);
    binding
        .receiver_port
        .cancel_task(UniqueId::new(key.task_key_hi, key.task_key_lo));
}

#[test]
fn compiled_factory_leaves_receiver_registration_to_the_fragment_resource_lease() {
    // Receiver registration is acquired exactly once by LocalTaskResources during
    // prepare. The factory only consumes that registration through its binding.
    let key = exchange::ExchangeKey {
        task_key_hi: 77_001,
        task_key_lo: 88_002,
        node_id: 5,
    };
    let fixture = Fixture::new();
    let binding = ExchangeBinding {
        key,
        expected_senders: 4,
        receiver_port: fixture.port.clone(),
    };
    let factory = ExchangeSourceFactory::new_compiled(5, binding);
    assert!(
        binding_receiver_port(&factory)
            .snapshot(receiver_key(key))
            .is_none(),
        "factory must not create a second exchange receiver registration"
    );
}

fn binding_receiver_port(factory: &ExchangeSourceFactory) -> &Arc<dyn ExchangeReceiverPort> {
    &factory.binding.receiver_port
}
