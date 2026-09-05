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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::operators::runtime_filter::RuntimeFilterConsumerSet;
use crate::exec::pipeline::binding::ExchangeBinding;
use crate::exec::pipeline::operator::{DriverBlockDeadline, Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::exchange;
use crate::runtime::fragment::io::{ExchangeReceiverKey, FragmentEventSink, NoopFragmentEventSink};
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::UniqueId;
use tracing::debug;

static EXCHANGE_SOURCE_READY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_log_exchange_source_ready() -> bool {
    let count = EXCHANGE_SOURCE_READY_LOG_COUNT.fetch_add(1, Ordering::Relaxed);
    count.is_multiple_of(1024)
}

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
    node: ExchangeSourceNode,
    binding: ExchangeBinding,
    runtime_filter_execution: ExchangeSourceRuntimeFilterExecution,
    arena: Arc<ExprArena>,
    idle_progress: Arc<ExchangeIdleProgress>,
}

struct ExchangeSourceRuntimeFilterExecution {
    consumers: RuntimeFilterConsumerSet,
}

impl ExchangeSourceFactory {
    pub(crate) fn new_native(
        node: ExchangeSourceNode,
        binding: ExchangeBinding,
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        let name = node.profile_name();
        let consumers = RuntimeFilterConsumerSet::from_plan(
            node.native_runtime_filter_specs(),
            Arc::clone(&arena),
        )?;
        Ok(Self {
            name,
            node,
            binding,
            runtime_filter_execution: ExchangeSourceRuntimeFilterExecution { consumers },
            arena,
            idle_progress: Arc::new(ExchangeIdleProgress::new()),
        })
    }
}

impl OperatorFactory for ExchangeSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let source_observable = Arc::new(Observable::new());
        let idle_deadline = Arc::new(ExchangeIdleDeadline::new());
        self.idle_progress
            .register(&idle_deadline, &source_observable);
        Box::new(ExchangeSourceOperator {
            name: self.name.clone(),
            node: self.node.clone(),
            binding: self.binding.clone(),
            driver_id,
            receiver: None,
            idle_deadline,
            idle_progress: Arc::clone(&self.idle_progress),
            source_observable,
            finished: false,
            logged_first_pull: false,
            logged_first_none: false,
            arena: Arc::clone(&self.arena),
            native_runtime_filter_consumers: Some(self.runtime_filter_execution.consumers.clone()),
            event_sink: Arc::new(NoopFragmentEventSink),
            receiver_mem_tracker_ready: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

#[allow(
    dead_code,
    reason = "The source retains its expression arena for exchange execution contract compatibility."
)]
struct ExchangeSourceOperator {
    name: String,
    node: ExchangeSourceNode,
    binding: ExchangeBinding,
    driver_id: i32,
    receiver: Option<exchange::ExchangeReceiverHandle>,
    source_observable: Arc<Observable>,
    idle_deadline: Arc<ExchangeIdleDeadline>,
    idle_progress: Arc<ExchangeIdleProgress>,
    finished: bool,
    logged_first_pull: bool,
    logged_first_none: bool,
    arena: Arc<ExprArena>,
    native_runtime_filter_consumers: Option<RuntimeFilterConsumerSet>,
    event_sink: Arc<dyn FragmentEventSink>,
    receiver_mem_tracker_ready: bool,
}

impl Operator for ExchangeSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_fragment_event_sink(&mut self, event_sink: Arc<dyn FragmentEventSink>) {
        self.event_sink = event_sink;
    }

    fn prepare(&mut self) -> Result<(), String> {
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

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        if let Some(consumers) = self.native_runtime_filter_consumers.as_ref() {
            consumers.bind(state)?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.idle_deadline.cancel();
        Ok(())
    }

    fn cancel(&mut self) {
        self.idle_deadline.cancel();
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for ExchangeSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        if self.finished {
            return false;
        }
        let Some(receiver) = self.receiver.as_ref() else {
            return false;
        };
        let ready = receiver.has_output_or_finished(self.binding.expected_senders);
        if ready {
            self.idle_deadline.clear();
        } else if self.idle_deadline.is_expired() {
            if should_log_exchange_source_ready() {
                debug!(
                    "ExchangeSource has_output due to idle timeout: finst={} node_id={} timeout={:?}",
                    self.binding.key.finst_uuid(),
                    self.node.node_id,
                    self.node.timeout
                );
            }
            return true;
        }
        if ready && should_log_exchange_source_ready() {
            debug!(
                "ExchangeSource has_output due to receiver: finst={} node_id={} expected_senders={}",
                self.binding.key.finst_uuid(),
                self.node.node_id,
                self.binding.expected_senders
            );
        }
        ready
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("exchange source operator does not accept input".to_string())
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if self.finished {
            return Ok(None);
        }

        if self.receiver.is_none() {
            return Err("exchange source operator not prepared".to_string());
        }

        if !self.receiver_mem_tracker_ready {
            self.receiver_mem_tracker_ready = true;
            if let Some(root) = state.mem_tracker() {
                let _ = self
                    .binding
                    .receiver_port
                    .ensure_mem_tracker(receiver_key(self.binding.key), &root)?;
            }
        }

        if !self.logged_first_pull {
            self.logged_first_pull = true;
            debug!(
                "ExchangeSource first pull: node_id={} driver_id={}",
                self.node.node_id, self.driver_id
            );
        }

        loop {
            let out = {
                let receiver = self.receiver.as_ref().expect("receiver");
                receiver
                    .try_pop_next_with_stats(self.binding.expected_senders)
                    .map_err(|e| e.to_string())?
            };

            match out {
                Some(exchange::ExchangePopResult::Chunk(chunk)) => {
                    self.idle_deadline.clear();
                    let input_rows = chunk.len();
                    let chunk =
                        if let Some(consumers) = self.native_runtime_filter_consumers.as_ref() {
                            consumers.acquire_configured()?;
                            let Some(chunk) =
                                consumers.apply_chunk_observed(chunk, Some(&self.event_sink))?
                            else {
                                continue;
                            };
                            chunk
                        } else {
                            chunk
                        };
                    if chunk.is_empty() {
                        debug!(
                            "ExchangeSource filtered empty chunk: node_id={} driver_id={} input_rows={}",
                            self.node.node_id, self.driver_id, input_rows
                        );
                        continue;
                    }
                    debug!(
                        "ExchangeSource output chunk: node_id={} driver_id={} input_rows={} output_rows={}",
                        self.node.node_id,
                        self.driver_id,
                        input_rows,
                        chunk.len()
                    );
                    return Ok(Some(chunk));
                }
                Some(exchange::ExchangePopResult::Finished(stats)) => {
                    self.idle_deadline.cancel();
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
                    self.finished = true;
                    return Ok(None);
                }
                None => {
                    if self.idle_deadline.is_expired() {
                        debug!(
                            "ExchangeSource timeout waiting for senders: finst_id={} node_id={} timeout={:?}",
                            self.binding.key.finst_uuid(),
                            self.node.node_id,
                            self.node.timeout
                        );
                        return Err(format!(
                            "exchange timeout waiting for senders: finst_id={} node_id={}",
                            self.binding.key.finst_uuid(),
                            self.node.node_id
                        ));
                    }
                    if !self.logged_first_none {
                        self.logged_first_none = true;
                        debug!(
                            "ExchangeSource no output yet: node_id={} driver_id={}",
                            self.node.node_id, self.driver_id
                        );
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn precondition_dependency(
        &self,
    ) -> Option<crate::exec::pipeline::dependency::DependencyHandle> {
        None
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        Some(Arc::clone(&self.source_observable))
    }

    fn source_block_deadline(&self) -> Option<DriverBlockDeadline> {
        self.idle_deadline.arm(self.node.timeout)
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

    use crate::runtime_filter::{
        self as execution, RuntimeFilterArtifactQueryError, RuntimeFilterBindingId,
        RuntimeFilterChannelId, RuntimeFilterConsumerContract, RuntimeFilterExecutionContract,
        RuntimeFilterMembershipSchema, RuntimeFilterNullSemantics, RuntimeFilterScalarRef,
    };
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_spi::connector::ConnectorScalarValue;

    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding;
    use crate::exec::pipeline::binding::ExchangeBinding;
    use crate::exec::pipeline::driver::PipelineDriver;
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::{
        DriverTask, FragmentCompletion, GlobalDriverExecutor,
    };
    use crate::exec::pipeline::operator::Operator;
    use crate::runtime::fragment::io::ExchangeReceiverPort;
    use crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port;
    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_types::SlotId;

    fn int32_chunk(values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).expect("test batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
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

    struct Int32MembershipQuery {
        accepted: Vec<i32>,
    }

    impl execution::evaluator::RuntimeFilterArtifactQuery for Int32MembershipQuery {
        fn data_type(&self) -> &DataType {
            &DataType::Int32
        }

        fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(false)
        }

        fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(!self.accepted.is_empty())
        }

        fn non_null_value_may_match(
            &self,
            value: RuntimeFilterScalarRef<'_>,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            match value {
                RuntimeFilterScalarRef::Int32(value) => Ok(self.accepted.contains(&value)),
                _ => Err(RuntimeFilterArtifactQueryError::ContractViolation),
            }
        }

        fn non_null_range_may_match(
            &self,
            _: &ConnectorScalarValue,
            _: &ConnectorScalarValue,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(true)
        }
    }

    struct PublishedSubscription(Arc<execution::RuntimeFilterSnapshot>);

    impl execution::BlockingSnapshotSubscription for PublishedSubscription {
        fn acquire(&self, _: Duration) -> execution::SnapshotAcquireOutcome {
            execution::SnapshotAcquireOutcome::Published(Arc::clone(&self.0))
        }

        fn snapshot(&self) -> Option<Arc<execution::RuntimeFilterSnapshot>> {
            Some(Arc::clone(&self.0))
        }
    }

    struct PublishedSession {
        subscription: Arc<dyn execution::BlockingSnapshotSubscription>,
    }

    impl execution::RuntimeFilterSession for PublishedSession {
        fn open_producer(
            &self,
            _: execution::RuntimeFilterProducerOpenRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::RuntimeFilterProducerHandle>,
            execution::RuntimeFilterContractViolation,
        > {
            Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "consumer-only test session",
            ))
        }

        fn subscribe(
            &self,
            _: execution::RuntimeFilterSubscriptionRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionHandle>,
            execution::RuntimeFilterContractViolation,
        > {
            Ok(execution::RuntimeFilterBindOutcome::Bound(
                execution::RuntimeFilterSubscriptionHandle::Blocking(Arc::clone(
                    &self.subscription,
                )),
            ))
        }

        fn open_final_domain_completion(
            &self,
            _: execution::RuntimeFilterFinalDomainOpenRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<
                execution::RuntimeFilterFinalDomainCompletionHandle,
            >,
            execution::RuntimeFilterContractViolation,
        > {
            Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "consumer-only test session",
            ))
        }
    }

    fn membership_binding(expr_id: crate::exec::expr::ExprId) -> RuntimeFilterConsumerBinding {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int32,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .expect("membership schema");
        let contract = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(1),
            RuntimeFilterChannelId::new(2),
            RuntimeFilterExecutionContract::Membership(schema),
        )
        .expect("membership consumer contract");
        RuntimeFilterConsumerBinding::new(expr_id, contract, None)
    }

    fn published_runtime_state(accepted: Vec<i32>) -> RuntimeState {
        let snapshot = Arc::new(execution::RuntimeFilterSnapshot::new(
            RuntimeFilterBindingId::new(1),
            execution::LogicalVersion::FIRST,
            [0; 32],
            Arc::new(Int32MembershipQuery { accepted }),
        ));
        let session: execution::RuntimeFilterSessionRef = Arc::new(PublishedSession {
            subscription: Arc::new(PublishedSubscription(snapshot)),
        });
        runtime_state().with_runtime_filter_session(Some(session))
    }

    fn runtime_state() -> RuntimeState {
        RuntimeState::default()
    }

    struct CountingSink {
        rows: Arc<AtomicUsize>,
        finishing: bool,
    }

    impl Operator for CountingSink {
        fn name(&self) -> &str {
            "COUNTING_SINK"
        }

        fn is_finished(&self) -> bool {
            self.finishing
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for CountingSink {
        fn need_input(&self) -> bool {
            !self.finishing
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
            self.rows.fetch_add(chunk.len(), Ordering::AcqRel);
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finishing = true;
            Ok(())
        }
    }

    struct TimeoutPipeline {
        state: Arc<RuntimeState>,
        observable: Arc<Observable>,
        rows: Arc<AtomicUsize>,
        completion: Arc<FragmentCompletion>,
        fragment: Arc<FragmentContext>,
        source: Box<dyn Operator>,
        binding: ExchangeBinding,
    }

    fn timeout_pipeline(key: exchange::ExchangeKey, timeout: Duration) -> TimeoutPipeline {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let node = ExchangeSourceNode::new(
            key.node_id,
            timeout,
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema"),
        );
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
            receiver_port: in_process_test_exchange_receiver_port(),
        };
        let factory = ExchangeSourceFactory::new_native(
            node,
            binding.clone(),
            Arc::new(ExprArena::default()),
        )
        .expect("exchange source factory");
        let state = Arc::new(runtime_state());
        let mut source = factory.create(1, 0);
        source.prepare().expect("prepare exchange source");
        source
            .bind_runtime_state(&state)
            .expect("bind exchange source runtime");
        let observable = source
            .as_processor_ref()
            .and_then(ProcessorOperator::source_observable)
            .expect("exchange source observable");
        let rows = Arc::new(AtomicUsize::new(0));
        let completion = FragmentCompletion::new(1);
        let fragment = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&state),
            Some((key.finst_id_hi, key.finst_id_lo)),
            None,
            None,
            None,
        ));
        TimeoutPipeline {
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
    ) {
        let driver = PipelineDriver::new(
            0,
            vec![
                source,
                Box::new(CountingSink {
                    rows,
                    finishing: false,
                }),
            ],
            None,
            Vec::new(),
            state,
            fragment.fragment_instance_id(),
        );
        let task = DriverTask::new(driver, completion, fragment, Duration::from_millis(5));
        executor.submit(vec![task]);
    }

    fn wait_for_exchange_timeout(
        completion: &Arc<FragmentCompletion>,
        fragment: &Arc<FragmentContext>,
    ) -> String {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !completion.should_abort() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        if !completion.should_abort() {
            let error = "exchange timeout test did not complete".to_string();
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

        assert!(error.contains("exchange timeout waiting for senders"));
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
            state,
            observable,
            rows,
            completion,
            fragment,
            source,
            binding,
        } = timeout_pipeline(key, Duration::from_millis(100));
        let executor = GlobalDriverExecutor::new(1);
        submit_timeout_pipeline(
            &executor,
            state,
            source,
            Arc::clone(&rows),
            Arc::clone(&completion),
            Arc::clone(&fragment),
        );

        let registration_deadline = Instant::now() + Duration::from_secs(1);
        while observable.num_observers() == 0 && Instant::now() < registration_deadline {
            std::thread::yield_now();
        }
        assert!(observable.num_observers() > 0, "source did not park");
        let after_park = observable.generation();
        binding.receiver_port.push_local(
            receiver_key(key),
            0,
            0,
            vec![int32_chunk(vec![7])],
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
        assert!(error.contains("exchange timeout waiting for senders"));
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
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let node = ExchangeSourceNode::new(
            key.node_id,
            Duration::from_secs(1),
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema"),
        );
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
            receiver_port: in_process_test_exchange_receiver_port(),
        };
        let factory = ExchangeSourceFactory::new_native(
            node,
            binding.clone(),
            Arc::new(ExprArena::default()),
        )
        .expect("exchange source factory");
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);
        first.prepare().expect("prepare first exchange source");
        second.prepare().expect("prepare second exchange source");

        let first_before = first
            .as_processor_ref()
            .and_then(ProcessorOperator::source_block_deadline)
            .expect("first source deadline");
        let second_before = second
            .as_processor_ref()
            .and_then(ProcessorOperator::source_block_deadline)
            .expect("second source deadline");
        assert_eq!(first_before.token(), 1);
        assert_eq!(second_before.token(), 1);

        binding.receiver_port.push_local(
            receiver_key(key),
            0,
            0,
            vec![int32_chunk(vec![11])],
            false,
        );
        let output = first
            .as_processor_mut()
            .expect("first processor")
            .pull_chunk(&runtime_state())
            .expect("first source pull")
            .expect("first source chunk");
        assert_eq!(int32_values(&output), vec![11]);
        assert!(
            !second
                .as_processor_ref()
                .expect("second processor")
                .has_output(),
            "the sibling consumed the only queued chunk"
        );

        let second_after = second
            .as_processor_ref()
            .and_then(ProcessorOperator::source_block_deadline)
            .expect("reset second source deadline");
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
    fn native_exchange_source_applies_the_shared_membership_mask() {
        let key = exchange::ExchangeKey {
            finst_id_hi: 91_001,
            finst_id_lo: 91_002,
            node_id: 91_003,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let node = ExchangeSourceNode::new(
            key.node_id,
            Duration::from_secs(2),
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .unwrap(),
        )
        .with_runtime_filter_consumers(vec![membership_binding(expr_id)]);
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
            receiver_port: in_process_test_exchange_receiver_port(),
        };
        let factory = ExchangeSourceFactory::new_native(node, binding, Arc::new(arena)).unwrap();
        let state = published_runtime_state(vec![2, 4]);
        let mut source = factory.create(1, 0);
        source.prepare().unwrap();
        source.bind_runtime_state(&state).unwrap();
        binding_receiver_port(&factory).push_local(
            receiver_key(key),
            0,
            0,
            vec![int32_chunk(vec![1, 2, 3, 4])],
            true,
        );

        let output = source
            .as_processor_mut()
            .unwrap()
            .pull_chunk(&state)
            .unwrap()
            .unwrap();
        assert_eq!(int32_values(&output), vec![2, 4]);
        binding_receiver_port(&factory)
            .cancel_fragment(UniqueId::new(key.finst_id_hi, key.finst_id_lo));
    }

    #[test]
    fn native_exchange_source_continues_after_first_chunk_is_fully_filtered() {
        let key = exchange::ExchangeKey {
            finst_id_hi: 91_011,
            finst_id_lo: 91_012,
            node_id: 91_013,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let node = ExchangeSourceNode::new(
            key.node_id,
            Duration::from_secs(2),
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .unwrap(),
        )
        .with_runtime_filter_consumers(vec![membership_binding(expr_id)]);
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
            receiver_port: in_process_test_exchange_receiver_port(),
        };
        let factory = ExchangeSourceFactory::new_native(node, binding, Arc::new(arena)).unwrap();
        let state = published_runtime_state(vec![2, 4]);
        let mut source = factory.create(1, 0);
        source.prepare().unwrap();
        source.bind_runtime_state(&state).unwrap();
        binding_receiver_port(&factory).push_local(
            receiver_key(key),
            0,
            0,
            vec![int32_chunk(vec![1, 3]), int32_chunk(vec![2, 4])],
            true,
        );

        let output = source
            .as_processor_mut()
            .unwrap()
            .pull_chunk(&state)
            .unwrap()
            .unwrap();
        assert_eq!(int32_values(&output), vec![2, 4]);
        binding_receiver_port(&factory)
            .cancel_fragment(UniqueId::new(key.finst_id_hi, key.finst_id_lo));
    }

    #[test]
    fn native_factory_leaves_receiver_registration_to_the_fragment_resource_lease() {
        // Receiver registration is acquired exactly once by FragmentResources during
        // prepare. The factory only consumes that registration through its binding.
        let key = exchange::ExchangeKey {
            finst_id_hi: 77_001,
            finst_id_lo: 88_002,
            node_id: 5,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        let node = ExchangeSourceNode::new(5, Duration::from_secs(60), chunk_schema);
        let binding = ExchangeBinding {
            key,
            expected_senders: 4,
            receiver_port: in_process_test_exchange_receiver_port(),
        };
        let factory =
            ExchangeSourceFactory::new_native(node, binding, Arc::new(ExprArena::default()))
                .expect("native exchange factory");
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
