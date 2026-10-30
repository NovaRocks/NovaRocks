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
//! Exchange source for receiving distributed upstream data.
//!
//! Responsibilities:
//! - Fetches remote stream pages from exchange service and reconstructs chunks for local pipeline processing.
//! - Handles end-of-stream coordination, sender completion tracking, and error propagation.
//!
//! Key exported interfaces:
//! - Types: `ExchangeSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

#[cfg(test)]
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::exec::pipeline::binding::ExchangeBinding;
use crate::exec::pipeline::operator::{DriverBlockDeadline, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::pipeline::shape::{Source, SourceKernel};
use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};
use crate::runtime::exchange;
use crate::runtime::fragment::io::ExchangeReceiverKey;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::UniqueId;
use tracing::debug;

struct ExchangeIdleDeadlineState {
    current: Option<DriverBlockDeadline>,
    next_token: u64,
    canceled: bool,
}

struct ExchangeIdleDeadline {
    state: Mutex<ExchangeIdleDeadlineState>,
}

struct ExchangeIdleProgressListener {
    deadline: std::sync::Weak<ExchangeIdleDeadline>,
    observable: std::sync::Weak<Observable>,
}

struct ExchangeIdleProgressState {
    receiver_observable: Option<std::sync::Weak<Observable>>,
    listeners: Vec<ExchangeIdleProgressListener>,
}

struct ExchangeIdleProgress {
    state: Mutex<ExchangeIdleProgressState>,
}

impl ExchangeIdleProgress {
    fn new() -> Self {
        Self {
            state: Mutex::new(ExchangeIdleProgressState {
                receiver_observable: None,
                listeners: Vec::new(),
            }),
        }
    }

    fn register(&self, deadline: &Arc<ExchangeIdleDeadline>, observable: &Arc<Observable>) {
        self.state
            .lock()
            .expect("exchange idle progress lock")
            .listeners
            .push(ExchangeIdleProgressListener {
                deadline: Arc::downgrade(deadline),
                observable: Arc::downgrade(observable),
            });
    }

    fn attach_receiver(self: &Arc<Self>, receiver_observable: &Arc<Observable>) {
        let should_attach = {
            let mut state = self.state.lock().expect("exchange idle progress lock");
            match state.receiver_observable.as_ref() {
                Some(current) if current.ptr_eq(&Arc::downgrade(receiver_observable)) => false,
                Some(_) => {
                    debug_assert!(false, "exchange source factory changed receiver observable");
                    false
                }
                None => {
                    state.receiver_observable = Some(Arc::downgrade(receiver_observable));
                    true
                }
            }
        };
        if !should_attach {
            return;
        }

        let progress = Arc::downgrade(self);
        receiver_observable.add_observer(Arc::new(move || {
            if let Some(progress) = progress.upgrade() {
                progress.publish();
            }
        }));
    }

    fn publish(&self) {
        let listeners = {
            let mut state = self.state.lock().expect("exchange idle progress lock");
            let mut listeners = Vec::with_capacity(state.listeners.len());
            state.listeners.retain(|listener| {
                let Some(deadline) = listener.deadline.upgrade() else {
                    return false;
                };
                let Some(observable) = listener.observable.upgrade() else {
                    return false;
                };
                listeners.push((deadline, observable));
                true
            });
            listeners
        };

        // A receiver packet belongs to the shared exchange key, not to the
        // driver that happens to win the queue pop. Reset every live driver's
        // idle period before publishing any wake-up, so a fast sibling cannot
        // consume the packet before another driver observes the progress.
        for (deadline, _) in &listeners {
            deadline.clear();
        }
        for (_, observable) in listeners {
            observable.notify_observers();
        }
    }
}

impl ExchangeIdleDeadline {
    fn new() -> Self {
        Self {
            state: Mutex::new(ExchangeIdleDeadlineState {
                current: None,
                next_token: 0,
                canceled: false,
            }),
        }
    }

    fn arm(&self, timeout: std::time::Duration) -> Option<DriverBlockDeadline> {
        let mut state = self.state.lock().expect("exchange deadline state lock");
        if state.canceled {
            return None;
        }
        if let Some(current) = state.current {
            return Some(current);
        }
        state.next_token = state.next_token.wrapping_add(1).max(1);
        let deadline = DriverBlockDeadline::new(Instant::now() + timeout, state.next_token);
        state.current = Some(deadline);
        Some(deadline)
    }

    fn is_expired(&self) -> bool {
        self.state
            .lock()
            .expect("exchange deadline state lock")
            .current
            .is_some_and(|deadline| deadline.at() <= Instant::now())
    }

    fn clear(&self) {
        self.state
            .lock()
            .expect("exchange deadline state lock")
            .current = None;
    }

    fn cancel(&self) {
        let mut state = self.state.lock().expect("exchange deadline state lock");
        state.canceled = true;
        state.current = None;
    }
}

/// Factory for exchange source operators that fetch and decode remote stream pages.
pub struct ExchangeSourceFactory {
    name: String,
    node: ExchangeSourceIdentity,
    binding: ExchangeBinding,
    idle_progress: Arc<ExchangeIdleProgress>,
}

/// The receiver registry owns the exact schema and decoded chunks. A source
/// driver needs only routing and timeout facts, never another schema owner.
#[derive(Clone, Copy)]
struct ExchangeSourceIdentity {
    node_id: i32,
    timeout: std::time::Duration,
}

impl ExchangeSourceFactory {
    /// Build the source for a compiled ExchangeSource (local-compiler
    /// output). Its builder validates the frozen local node's supported shape.
    /// The receiver registry supplies exact typed chunks; no expression arena
    /// or duplicate expected schema is constructed for this driver.
    pub(crate) fn new_compiled(
        node_id: i32,
        timeout: std::time::Duration,
        binding: ExchangeBinding,
    ) -> Self {
        Self {
            name: format!("EXCHANGE_SOURCE (id={node_id})"),
            node: ExchangeSourceIdentity { node_id, timeout },
            binding,
            idle_progress: Arc::new(ExchangeIdleProgress::new()),
        }
    }
}

impl ExchangeSourceFactory {
    // The signal is construction metadata, not a readiness predicate. Tests
    // observe this same signal without polling work or starting an idle timer.
    pub(super) fn create_source(&self, driver_id: i32) -> (Box<dyn Operator>, Arc<Observable>) {
        let source_observable = Arc::new(Observable::new());
        let idle_deadline = Arc::new(ExchangeIdleDeadline::new());
        self.idle_progress
            .register(&idle_deadline, &source_observable);
        let operator: Box<dyn Operator> = Box::new(Source::new(ExchangeSourceOperator {
            name: self.name.clone(),
            node: self.node,
            binding: self.binding.clone(),
            driver_id,
            receiver: None,
            idle_deadline,
            idle_progress: Arc::clone(&self.idle_progress),
            source_observable: Arc::clone(&source_observable),
            logged_first_pull: false,
            logged_first_none: false,
            #[cfg(novarocks_dev_controls)]
            last_partial_eos_marker_count: 0,
            receiver_mem_tracker_ready: false,
            receiver_mem_tracker_root: None,
            #[cfg(novarocks_dev_controls)]
            test_observer: None,
        }));
        (operator, source_observable)
    }
}

impl OperatorFactory for ExchangeSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        self.create_source(driver_id).0
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct ExchangeSourceOperator {
    name: String,
    node: ExchangeSourceIdentity,
    binding: ExchangeBinding,
    driver_id: i32,
    receiver: Option<exchange::ExchangeReceiverHandle>,
    source_observable: Arc<Observable>,
    idle_deadline: Arc<ExchangeIdleDeadline>,
    idle_progress: Arc<ExchangeIdleProgress>,
    logged_first_pull: bool,
    logged_first_none: bool,
    #[cfg(novarocks_dev_controls)]
    last_partial_eos_marker_count: usize,
    receiver_mem_tracker_ready: bool,
    receiver_mem_tracker_root: Option<Arc<crate::runtime::mem_tracker::MemTracker>>,
    #[cfg(novarocks_dev_controls)]
    test_observer: Option<Arc<dyn exchange::TestEventObserver>>,
}

impl ExchangeSourceOperator {
    fn waiting(&self, observed_generation: u64) -> StepResult {
        StepResult::new(
            None,
            NextStep::Wait(WaitFact {
                reason: WaitReason::Input,
                observable: Arc::clone(&self.source_observable),
                observed_generation,
                deadline: self.idle_deadline.arm(self.node.timeout),
            }),
            0,
        )
    }
}

#[derive(Debug)]
enum ExchangeSourceFailure {
    NotPrepared,
    IdleTimeout {
        key: exchange::ExchangeKey,
        expected_senders: usize,
        timeout: std::time::Duration,
    },
}
impl std::fmt::Display for ExchangeSourceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPrepared => f.write_str("exchange source operator not prepared"),
            Self::IdleTimeout {
                key,
                expected_senders,
                timeout,
            } => write!(
                f,
                "exchange timeout waiting for senders: finst_id={} node_id={} expected_senders={} timeout_ms={}",
                key.finst_uuid(),
                key.node_id,
                expected_senders,
                timeout.as_millis()
            ),
        }
    }
}
impl std::error::Error for ExchangeSourceFailure {}
impl novarocks_failure_contract::FailureProjection for ExchangeSourceFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, reason) = match self {
            Self::IdleTimeout { .. } => (FailureClass::Dependency, 1),
            Self::NotPrepared => (FailureClass::Invariant, 2),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::EXCHANGE, std::num::NonZeroU16::new(reason).unwrap()),
        )
    }
}

impl OperatorLifecycle for ExchangeSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        if self.receiver.is_some() {
            return Ok(());
        }
        let receiver = self.binding.receiver_port.receiver_handle(
            receiver_key(self.binding.key),
            self.binding.expected_senders,
        )?;
        self.idle_progress.attach_receiver(&receiver.observable());
        self.receiver = Some(receiver);
        debug!(
            "ExchangeSource prepared: finst={} node_id={} expected_senders={} timeout={:?}",
            self.binding.key.finst_uuid(),
            self.node.node_id,
            self.binding.expected_senders,
            self.node.timeout
        );
        Ok(())
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.receiver_mem_tracker_root = state.mem_tracker();
        #[cfg(novarocks_dev_controls)]
        {
            self.test_observer = state.exchange_test_observer();
        }
        Ok(())
    }

    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.idle_deadline.cancel();
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }

    fn cancel(&mut self) {
        self.idle_deadline.cancel();
    }
}

impl SourceKernel for ExchangeSourceOperator {
    fn poll_output(
        &mut self,
        ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
    ) -> ExecutionResult<StepResult> {
        if self.receiver.is_none() {
            return Err(crate::runtime::fragment::ExecutionFailure::from_domain(
                ExchangeSourceFailure::NotPrepared,
            ));
        }
        if !self.receiver_mem_tracker_ready {
            self.receiver_mem_tracker_ready = true;
            if let Some(root) = self.receiver_mem_tracker_root.as_ref() {
                self.binding
                    .receiver_port
                    .ensure_mem_tracker(receiver_key(self.binding.key), root)?;
            }
        }
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let observed_generation = self.source_observable.generation();
        if !self.logged_first_pull {
            self.logged_first_pull = true;
            debug!(
                "ExchangeSource first pull: node_id={} driver_id={}",
                self.node.node_id, self.driver_id
            );
        }

        let receiver = self.receiver.as_ref().expect("receiver");
        let out = receiver.try_pop_next_with_stats(self.binding.expected_senders)?;

        match out {
            Some(exchange::ExchangePopResult::Chunk(chunk)) => {
                self.idle_deadline.clear();
                let input_rows = chunk.len();
                if chunk.is_empty() {
                    debug!(
                        "ExchangeSource filtered empty chunk: node_id={} driver_id={} input_rows={}",
                        self.node.node_id, self.driver_id, input_rows
                    );
                    return Ok(StepResult::new(None, NextStep::Runnable, input_rows as u64));
                }
                debug!(
                    "ExchangeSource output chunk: node_id={} driver_id={} input_rows={} output_rows={}",
                    self.node.node_id,
                    self.driver_id,
                    input_rows,
                    chunk.len()
                );
                Ok(StepResult::new(
                    Some(chunk),
                    NextStep::Runnable,
                    input_rows as u64,
                ))
            }
            Some(exchange::ExchangePopResult::Finished(stats)) => {
                receiver.complete_input_consumption(self.binding.expected_senders)?;
                self.idle_deadline.cancel();
                #[cfg(novarocks_dev_controls)]
                if let Some(observer) = self
                    .test_observer
                    .as_ref()
                    .filter(|observer| observer.is_active())
                {
                    let snapshot = self
                        .binding
                        .receiver_port
                        .snapshot(receiver_key(self.binding.key));
                    exchange::emit_exchange_snapshot_marker(Some(observer.as_ref()), || {
                        format!(
                            "event=source_finished finst={} node_id={} driver_id={} expected_senders={} snapshot={:?} source_generation={}",
                            self.binding.key.finst_uuid(),
                            self.node.node_id,
                            self.driver_id,
                            self.binding.expected_senders,
                            snapshot,
                            self.source_observable.generation(),
                        )
                    });
                }
                debug!(
                    "ExchangeSource finished: finst={} node_id={} driver_id={} request_received={} bytes_received={} deserialize_ns={} chunks_received={} rows_received={}",
                    self.binding.key.finst_uuid(),
                    self.node.node_id,
                    self.driver_id,
                    stats.request_received,
                    stats.bytes_received,
                    stats.deserialize_ns,
                    stats.chunks_received,
                    stats.rows_received
                );
                Ok(StepResult::new(None, NextStep::Finished, 0))
            }
            None => {
                if self.idle_deadline.is_expired() {
                    let error = crate::runtime::fragment::ExecutionFailure::from_domain(
                        ExchangeSourceFailure::IdleTimeout {
                            key: self.binding.key,
                            expected_senders: self.binding.expected_senders,
                            timeout: self.node.timeout,
                        },
                    );
                    ctx.report_failure(error.clone());
                    let snapshot = self
                        .binding
                        .receiver_port
                        .snapshot(receiver_key(self.binding.key));
                    debug!(
                        "ExchangeSource timeout waiting for senders: finst_id={} node_id={} timeout={:?} expected_senders={} snapshot={:?}",
                        self.binding.key.finst_uuid(),
                        self.node.node_id,
                        self.node.timeout,
                        self.binding.expected_senders,
                        snapshot,
                    );
                    return Err(error);
                }
                let first_none = !self.logged_first_none;
                if first_none {
                    self.logged_first_none = true;
                }
                #[cfg(novarocks_dev_controls)]
                if let Some(observer) = self
                    .test_observer
                    .as_ref()
                    .filter(|observer| observer.is_active())
                {
                    let snapshot = self
                        .binding
                        .receiver_port
                        .snapshot(receiver_key(self.binding.key));
                    if first_none {
                        let first_snapshot = snapshot.clone();
                        exchange::emit_exchange_snapshot_marker(Some(observer.as_ref()), || {
                            format!(
                                "event=source_first_block finst={} node_id={} driver_id={} expected_senders={} snapshot={:?} source_generation={}",
                                self.binding.key.finst_uuid(),
                                self.node.node_id,
                                self.driver_id,
                                self.binding.expected_senders,
                                first_snapshot,
                                self.source_observable.generation(),
                            )
                        });
                    }
                    if let Some(snapshot) = snapshot
                        && snapshot.finished_senders > self.last_partial_eos_marker_count
                        && snapshot.finished_senders < self.binding.expected_senders
                    {
                        self.last_partial_eos_marker_count = snapshot.finished_senders;
                        exchange::emit_exchange_snapshot_marker(Some(observer.as_ref()), || {
                            format!(
                                "event=source_partial_eos_block finst={} node_id={} driver_id={} expected_senders={} snapshot={:?} source_generation={}",
                                self.binding.key.finst_uuid(),
                                self.node.node_id,
                                self.driver_id,
                                self.binding.expected_senders,
                                snapshot,
                                self.source_observable.generation(),
                            )
                        });
                    }
                }
                if first_none {
                    debug!(
                        "ExchangeSource no output yet: node_id={} driver_id={}",
                        self.node.node_id, self.driver_id
                    );
                }
                Ok(self.waiting(observed_generation))
            }
        }
    }
}

impl Drop for ExchangeSourceOperator {
    fn drop(&mut self) {
        self.idle_deadline.cancel();
    }
}

fn receiver_key(key: exchange::ExchangeKey) -> ExchangeReceiverKey {
    ExchangeReceiverKey {
        fragment_instance_id: UniqueId::new(key.finst_id_hi, key.finst_id_lo),
        node_id: key.node_id,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::exec::pipeline::binding::ExchangeBinding;
    use crate::exec::pipeline::driver::PipelineDriver;
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::{
        DriverTask, FragmentCompletion, GlobalDriverExecutor,
    };
    use crate::exec::pipeline::operator::Operator;
    use crate::runtime::fragment::io::ExchangeReceiverPort;
    use crate::runtime::fragment::io::exchange::test_support::Fixture;
    use crate::runtime::runtime_state::RuntimeState;
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

    fn register_int32_receiver(binding: &ExchangeBinding, fixture: &Fixture, sources: &[UniqueId]) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let layout = crate::exec::chunk::typed_fixture::layout(
            schema,
            &[SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                DataType::Int32,
                false,
            )],
        );
        let creation_factory = fixture.creation(receiver_key(binding.key).fragment_instance_id);
        let expected_chunk_schema = creation_factory
            .create_compiled_chunk_schema(&layout)
            .unwrap();
        binding
            .receiver_port
            .register(crate::runtime::fragment::io::ExchangeReceiverRegistration {
                key: receiver_key(binding.key),
                expected_senders: binding.expected_senders,
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
        use crate::runtime::fragment::io::exchange_unit::{
            ExchangeDemandEntry, ExchangeDemandOperation, ExchangeUnitDemand, ExchangeUnitItem,
            ExchangeUnitReceiverFrame, ExchangeUnitSequence,
        };
        let key = receiver_key(binding.key);
        let stream = fixture.stream(source, key.fragment_instance_id, key.node_id, ordinal);
        let (item, payload) = if let Some(chunk) = chunk {
            let payload = exchange::encode_chunks(&[chunk], true).unwrap();
            let demand = ExchangeUnitDemand {
                sequence: ExchangeUnitSequence::FIRST,
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
                ExchangeUnitItem::End(ExchangeUnitSequence::FIRST),
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
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
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
        use crate::exec::pipeline::step_context::{DEFAULT_TURN_WORK_UNITS, StepContext, TurnWork};
        let control = state.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, DEFAULT_TURN_WORK_UNITS);
        source
            .as_mut()
            .drive(&mut StepContext::new(
                &control,
                &mut work,
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            ))
            .expect("source step")
    }
    fn idle_deadline(source: &mut Box<dyn Operator>, state: &RuntimeState) -> DriverBlockDeadline {
        let step = source_step(source, state);
        assert!(step.output.is_none());
        let NextStep::Wait(wait) = step.next else {
            panic!("an actual idle poll must report its wait");
        };
        wait.deadline.expect("idle deadline")
    }

    struct TimeoutPipeline {
        fixture: Arc<Fixture>,
        state: Arc<RuntimeState>,
        observable: Arc<Observable>,
        rows: Arc<AtomicUsize>,
        completion: Arc<FragmentCompletion>,
        fragment: Arc<FragmentContext>,
        source: Box<dyn Operator>,
        binding: ExchangeBinding,
    }

    fn timeout_pipeline(key: exchange::ExchangeKey, timeout: Duration) -> TimeoutPipeline {
        let fixture = Fixture::new();
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
            receiver_port: fixture.port.clone(),
        };
        register_int32_receiver(&binding, &fixture, &[UniqueId::new(92_000, 1)]);
        let factory = ExchangeSourceFactory::new_compiled(key.node_id, timeout, binding.clone());
        let state = fixture.state(receiver_key(key).fragment_instance_id);
        let (mut source, observable) = factory.create_source(0);
        source.prepare().expect("prepare exchange source");
        source
            .bind_runtime_state(&state)
            .expect("bind exchange source runtime");
        let rows = Arc::new(AtomicUsize::new(0));
        let completion = FragmentCompletion::new(1, state.error_state());
        let fragment = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&state),
            Some((key.finst_id_hi, key.finst_id_lo)),
            None,
            None,
            None,
        ));
        TimeoutPipeline {
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

    fn submit_timeout_pipeline(
        executor: &GlobalDriverExecutor,
        state: Arc<RuntimeState>,
        source: Box<dyn Operator>,
        rows: Arc<AtomicUsize>,
        completion: Arc<FragmentCompletion>,
        fragment: Arc<FragmentContext>,
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
            fragment.fragment_instance_id(),
        );
        let parked = driver.schedule_state();
        let task = DriverTask::new(driver, completion, fragment, Duration::from_millis(5));
        executor.submit(vec![task]);
        parked
    }

    fn wait_for_exchange_timeout(
        completion: &Arc<FragmentCompletion>,
        fragment: &Arc<FragmentContext>,
    ) -> crate::runtime::fragment::ExecutionFailure {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !completion.should_abort() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        if !completion.should_abort() {
            let error = crate::runtime::fragment::ExecutionFailure::from(
                novarocks_functions::KernelFailure::Internal(
                    novarocks_functions::KernelDiagnostic::new(
                        "exchange timeout test did not complete",
                    ),
                ),
            );
            completion.fail(error.clone());
            fragment.set_final_status(error);
        }
        completion
            .wait()
            .expect_err("exchange timeout must fail the pipeline")
    }

    #[test]
    fn no_first_packet_timeout_wakes_a_parked_pipeline() {
        let key = exchange::ExchangeKey {
            finst_id_hi: 92_001,
            finst_id_lo: 92_002,
            node_id: 92_003,
        };
        let TimeoutPipeline {
            fixture: _fixture,
            state,
            observable,
            rows,
            completion,
            fragment,
            source,
            binding: _,
        } = timeout_pipeline(key, Duration::from_millis(30));
        let before = observable.generation();
        let executor = GlobalDriverExecutor::new(1);
        submit_timeout_pipeline(
            &executor,
            state,
            source,
            rows,
            Arc::clone(&completion),
            Arc::clone(&fragment),
        );

        let error = wait_for_exchange_timeout(&completion, &fragment);

        assert!(
            error
                .detail()
                .contains("exchange timeout waiting for senders")
        );
        assert_eq!(
            observable.generation(),
            before,
            "deadline scheduling must not synthesize a receiver event"
        );
    }

    #[test]
    fn partial_data_stall_timeout_rewakes_the_same_parked_pipeline() {
        let key = exchange::ExchangeKey {
            finst_id_hi: 92_011,
            finst_id_lo: 92_012,
            node_id: 92_013,
        };
        let TimeoutPipeline {
            fixture,
            state,
            observable,
            rows,
            completion,
            fragment,
            source,
            binding,
        } = timeout_pipeline(key, Duration::from_millis(100));
        let executor = GlobalDriverExecutor::new(1);
        let parked = submit_timeout_pipeline(
            &executor,
            state,
            source,
            Arc::clone(&rows),
            Arc::clone(&completion),
            Arc::clone(&fragment),
        );

        let registration_deadline = Instant::now() + Duration::from_secs(1);
        while !parked.is_in_blocked() && Instant::now() < registration_deadline {
            std::thread::yield_now();
        }
        assert!(parked.is_in_blocked(), "source did not park");
        let after_park = observable.generation();
        deliver_fixture_unit(
            &fixture,
            &binding,
            UniqueId::new(92_000, 1),
            0,
            Some(int32_chunk(vec![7])),
            false,
        );

        let consume_deadline = Instant::now() + Duration::from_secs(1);
        while rows.load(Ordering::Acquire) == 0 && Instant::now() < consume_deadline {
            std::thread::yield_now();
        }
        assert_eq!(rows.load(Ordering::Acquire), 1);
        let after_receiver = observable.generation();
        assert!(
            after_receiver > after_park,
            "receiver event was not forwarded"
        );

        let error = wait_for_exchange_timeout(&completion, &fragment);
        assert!(
            error
                .detail()
                .contains("exchange timeout waiting for senders")
        );
        assert_eq!(
            observable.generation(),
            after_receiver,
            "deadline scheduling must remain separate from receiver event generations"
        );
    }

    #[test]
    fn sibling_chunk_consumption_resets_every_driver_idle_deadline() {
        let key = exchange::ExchangeKey {
            finst_id_hi: 92_021,
            finst_id_lo: 92_022,
            node_id: 92_023,
        };
        let timeout = Duration::from_secs(1);
        let fixture = Fixture::new();
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
            receiver_port: fixture.port.clone(),
        };
        register_int32_receiver(&binding, &fixture, &[UniqueId::new(92_020, 1)]);
        let factory = ExchangeSourceFactory::new_compiled(key.node_id, timeout, binding.clone());
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);
        first.prepare().expect("prepare first exchange source");
        second.prepare().expect("prepare second exchange source");

        let state = fixture.state(receiver_key(key).fragment_instance_id);
        first.bind_runtime_state(&state).unwrap();
        second.bind_runtime_state(&state).unwrap();
        let first_before = idle_deadline(&mut first, &state);
        let second_before = idle_deadline(&mut second, &state);
        assert_eq!(first_before.token(), 1);
        assert_eq!(second_before.token(), 1);

        deliver_fixture_unit(
            &fixture,
            &binding,
            UniqueId::new(92_020, 1),
            0,
            Some(int32_chunk(vec![11])),
            false,
        );
        let output = source_step(&mut first, &state)
            .output
            .expect("first source chunk");
        assert_eq!(int32_values(&output), vec![11]);
        let second_after = idle_deadline(&mut second, &state);
        assert_ne!(
            second_after.token(),
            second_before.token(),
            "receiver progress must rotate an idle deadline even when a sibling consumes the chunk"
        );
        assert!(second_after.at() >= second_before.at());
        binding
            .receiver_port
            .cancel_fragment(UniqueId::new(key.finst_id_hi, key.finst_id_lo));
    }

    #[test]
    fn every_parked_driver_observes_the_shared_three_sender_eos() {
        let key = exchange::ExchangeKey {
            finst_id_hi: 92_031,
            finst_id_lo: 92_032,
            node_id: 92_033,
        };
        let timeout = Duration::from_secs(1);
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
        let factory = ExchangeSourceFactory::new_compiled(key.node_id, timeout, binding.clone());
        let state = fixture.state(receiver_key(key).fragment_instance_id);
        let fragment = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&state),
            Some((key.finst_id_hi, key.finst_id_lo)),
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(5, state.error_state());
        let rows = Arc::new(AtomicUsize::new(0));
        let mut parked = Vec::new();
        let mut tasks = Vec::new();
        for driver_id in 0..5 {
            let (mut source, _) = factory.create_source(driver_id);
            source.prepare().expect("prepare exchange source");
            source
                .bind_runtime_state(&state)
                .expect("bind exchange source runtime");
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
                fragment.fragment_instance_id(),
            );
            parked.push(driver.schedule_state());
            tasks.push(DriverTask::new(
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
        binding
            .receiver_port
            .cancel_fragment(UniqueId::new(key.finst_id_hi, key.finst_id_lo));
    }

    #[test]
    fn compiled_factory_leaves_receiver_registration_to_the_fragment_resource_lease() {
        // Receiver registration is acquired exactly once by FragmentResources during
        // prepare. The factory only consumes that registration through its binding.
        let key = exchange::ExchangeKey {
            finst_id_hi: 77_001,
            finst_id_lo: 88_002,
            node_id: 5,
        };
        let fixture = Fixture::new();
        let binding = ExchangeBinding {
            key,
            expected_senders: 4,
            receiver_port: fixture.port.clone(),
        };
        let factory = ExchangeSourceFactory::new_compiled(5, Duration::from_secs(60), binding);
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
}
