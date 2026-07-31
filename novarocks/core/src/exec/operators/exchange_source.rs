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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::operators::runtime_filter::RuntimeFilterConsumerSet;
use crate::exec::pipeline::binding::ExchangeBinding;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::novarocks_logging::debug;
use crate::runtime::exchange;
use crate::runtime::runtime_state::RuntimeState;

static EXCHANGE_SOURCE_READY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_log_exchange_source_ready() -> bool {
    let count = EXCHANGE_SOURCE_READY_LOG_COUNT.fetch_add(1, Ordering::Relaxed);
    count.is_multiple_of(1024)
}

/// Factory for exchange source operators that fetch and decode remote stream pages.
pub struct ExchangeSourceFactory {
    name: String,
    node: ExchangeSourceNode,
    binding: ExchangeBinding,
    runtime_filter_execution: ExchangeSourceRuntimeFilterExecution,
    arena: Arc<ExprArena>,
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
        exchange::register_expected_chunk_schema(
            binding.key,
            binding.expected_senders,
            node.expected_chunk_schema(),
        )?;
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
        })
    }

    #[cfg(test)]
    fn new_native_with_consumers_for_test(
        node: ExchangeSourceNode,
        binding: ExchangeBinding,
        arena: Arc<ExprArena>,
        consumers: RuntimeFilterConsumerSet,
    ) -> Result<Self, String> {
        let name = node.profile_name();
        exchange::register_expected_chunk_schema(
            binding.key,
            binding.expected_senders,
            node.expected_chunk_schema(),
        )?;
        Ok(Self {
            name,
            node,
            binding,
            runtime_filter_execution: ExchangeSourceRuntimeFilterExecution { consumers },
            arena,
        })
    }
}

impl OperatorFactory for ExchangeSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(ExchangeSourceOperator {
            name: self.name.clone(),
            node: self.node.clone(),
            binding: self.binding,
            driver_id,
            receiver: None,
            start: None,
            finished: false,
            logged_first_pull: false,
            logged_first_none: false,
            arena: Arc::clone(&self.arena),
            native_runtime_filter_consumers: Some(self.runtime_filter_execution.consumers.clone()),
            profiles: None,
            receiver_mem_tracker_ready: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct ExchangeSourceOperator {
    name: String,
    node: ExchangeSourceNode,
    binding: ExchangeBinding,
    driver_id: i32,
    receiver: Option<exchange::ExchangeReceiverHandle>,
    start: Option<Instant>,
    finished: bool,
    logged_first_pull: bool,
    logged_first_none: bool,
    arena: Arc<ExprArena>,
    native_runtime_filter_consumers: Option<RuntimeFilterConsumerSet>,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    receiver_mem_tracker_ready: bool,
}

impl Operator for ExchangeSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn prepare(&mut self) -> Result<(), String> {
        if self.receiver.is_some() {
            return Ok(());
        }
        let receiver =
            exchange::get_receiver_handle(self.binding.key, self.binding.expected_senders)?;
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
        if let Some(start) = self.start
            && start.elapsed() >= self.node.timeout
        {
            if should_log_exchange_source_ready() {
                debug!(
                    "ExchangeSource has_output due to timeout: finst={} node_id={} elapsed={:?} timeout={:?}",
                    self.binding.key.finst_uuid(),
                    self.node.node_id,
                    start.elapsed(),
                    self.node.timeout
                );
            }
            return true;
        }
        let Some(receiver) = self.receiver.as_ref() else {
            return false;
        };
        let ready = receiver.has_output_or_finished(self.binding.expected_senders);
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
                let _ = exchange::ensure_receiver_mem_tracker(self.binding.key, &root)?;
            }
        }

        if !self.logged_first_pull {
            self.logged_first_pull = true;
            debug!(
                "ExchangeSource first pull: node_id={} driver_id={}",
                self.node.node_id, self.driver_id
            );
        }

        let start = self.start.get_or_insert_with(Instant::now);
        if start.elapsed() >= self.node.timeout {
            debug!(
                "ExchangeSource timeout waiting for senders: finst_id={} node_id={} elapsed={:?} timeout={:?}",
                self.binding.key.finst_uuid(),
                self.node.node_id,
                start.elapsed(),
                self.node.timeout
            );
            return Err(format!(
                "exchange timeout waiting for senders: finst_id={} node_id={}",
                self.binding.key.finst_uuid(),
                self.node.node_id
            ));
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
                    let input_rows = chunk.len();
                    let chunk =
                        if let Some(consumers) = self.native_runtime_filter_consumers.as_ref() {
                            consumers.acquire_configured()?;
                            let Some(chunk) =
                                consumers.apply_chunk_profiled(chunk, self.profiles.as_ref())?
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
        self.receiver.as_ref().map(|r| r.observable())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprArena;
    use crate::exec::pipeline::binding::ExchangeBinding;
    use crate::runtime::runtime_state::RuntimeState;

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

    #[test]
    fn native_exchange_source_applies_the_shared_membership_mask() {
        let (consumers, arena) =
            crate::exec::operators::runtime_filter::tests_support::published_consumer_set(
                crate::exec::operators::runtime_filter::tests_support::membership_bundle(&[2, 4]),
            );
        let key = exchange::ExchangeKey {
            finst_id_hi: 91_001,
            finst_id_lo: 91_002,
            node_id: 91_003,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let node = ExchangeSourceNode::new(
            key.node_id,
            Duration::from_secs(2),
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .unwrap(),
        );
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
        };
        let factory = ExchangeSourceFactory::new_native_with_consumers_for_test(
            node, binding, arena, consumers,
        )
        .unwrap();
        let state = RuntimeState::default();
        let mut source = factory.create(1, 0);
        source.prepare().unwrap();
        source.bind_runtime_state(&state).unwrap();
        exchange::push_chunks(key, 0, 0, vec![int32_chunk(vec![1, 2, 3, 4])], true);

        let output = source
            .as_processor_mut()
            .unwrap()
            .pull_chunk(&state)
            .unwrap()
            .unwrap();
        assert_eq!(int32_values(&output), vec![2, 4]);
        exchange::remove_fragment(key.finst_id_hi, key.finst_id_lo);
    }

    #[test]
    fn native_exchange_source_continues_after_first_chunk_is_fully_filtered() {
        let (consumers, arena) =
            crate::exec::operators::runtime_filter::tests_support::published_consumer_set(
                crate::exec::operators::runtime_filter::tests_support::membership_bundle(&[2, 4]),
            );
        let key = exchange::ExchangeKey {
            finst_id_hi: 91_011,
            finst_id_lo: 91_012,
            node_id: 91_013,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let node = ExchangeSourceNode::new(
            key.node_id,
            Duration::from_secs(2),
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .unwrap(),
        );
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
        };
        let factory = ExchangeSourceFactory::new_native_with_consumers_for_test(
            node, binding, arena, consumers,
        )
        .unwrap();
        let state = RuntimeState::default();
        let mut source = factory.create(1, 0);
        source.prepare().unwrap();
        source.bind_runtime_state(&state).unwrap();
        exchange::push_chunks(
            key,
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
        exchange::remove_fragment(key.finst_id_hi, key.finst_id_lo);
    }

    #[test]
    fn native_factory_registers_receiver_from_binding_not_node() {
        // node_id (5) deliberately differs from the binding's expected_senders (4):
        // a factory that read the sender count from the static node instead of the
        // instance binding would register the wrong receiver state (or use the wrong
        // ExchangeKey), so this pins the cutover contract.
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
        };
        ExchangeSourceFactory::new_native(node, binding, Arc::new(ExprArena::default()))
            .expect("native exchange factory");
        let snapshot =
            exchange::snapshot_receiver_state(key).expect("factory registered the receiver state");
        assert_eq!(snapshot.expected_senders, 4);
        exchange::cancel_exchange_key(key);
    }
}
