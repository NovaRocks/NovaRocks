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
//! Source side of local in-process exchange.
//!
//! Responsibilities:
//! - Pulls buffered chunks from local exchange channels for downstream operators.
//! - Handles producer completion, empty-queue blocking, and terminal drain semantics.
//!
//! Key exported interfaces:
//! - Types: `LocalExchangeSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use std::sync::Arc;

use crate::exec::operators::local_exchanger::LocalExchanger;
use crate::runtime::runtime_state::RuntimeState;
use crate::novarocks_logging::debug;

/// Factory for local-exchange source operators that drain local partition queues.
pub struct LocalExchangeSourceFactory {
    name: String,
    owner_node_id: i32,
    partition_count: usize,
    exchanger: Arc<LocalExchanger>,
}

impl LocalExchangeSourceFactory {
    pub(crate) fn new(
        owner_node_id: i32,
        partition_count: usize,
        exchanger: Arc<LocalExchanger>,
    ) -> Self {
        let name = if owner_node_id >= 0 {
            format!("LOCAL_EXCHANGE_SOURCE (id={owner_node_id})")
        } else {
            "LOCAL_EXCHANGE_SOURCE".to_string()
        };
        debug!(
            "LocalExchangeSourceFactory created: exchange_id={} owner_node_id={} partitions={}",
            exchanger.exchange_id(),
            owner_node_id,
            partition_count.max(1)
        );
        Self {
            name,
            owner_node_id,
            partition_count: partition_count.max(1),
            exchanger,
        }
    }
}

impl OperatorFactory for LocalExchangeSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let partition = (driver_id as usize) % self.partition_count;
        Box::new(LocalExchangeSourceOperator {
            name: self.name.clone(),
            owner_node_id: self.owner_node_id,
            driver_id,
            partition,
            exchanger: Arc::clone(&self.exchanger),
            finished: false,
            blocked_empty: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct LocalExchangeSourceOperator {
    name: String,
    owner_node_id: i32,
    driver_id: i32,
    partition: usize,
    exchanger: Arc<LocalExchanger>,
    finished: bool,
    blocked_empty: bool,
}

impl Operator for LocalExchangeSourceOperator {
    fn name(&self) -> &str {
        &self.name
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

impl ProcessorOperator for LocalExchangeSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        if self.finished {
            return false;
        }
        if let Some((buffered, remaining)) =
            self.exchanger.partition_buffered_chunks(self.partition)
        {
            if buffered > 0 {
                return true;
            }
            if self.exchanger.has_spill_pending(self.partition) {
                if self.exchanger.restore_blocked() || self.exchanger.restore_inflight() {
                    return false;
                }
                return true;
            }
            return remaining == 0;
        }
        if self.exchanger.has_spill_pending(self.partition) {
            return !(self.exchanger.restore_blocked() || self.exchanger.restore_inflight());
        }
        !self.exchanger.is_done(self.partition)
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("local exchange source operator does not accept input".to_string())
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if self.finished {
            return Ok(None);
        }
        if let Some(chunk) = self.exchanger.pop_chunk(state, self.partition) {
            if self.blocked_empty {
                if let Some((buffered, remaining)) =
                    self.exchanger.partition_buffered_chunks(self.partition)
                {
                    debug!(
                        "LocalExchangeSource resumed: exchange_id={} owner_node_id={} driver_id={} partition={} buffered_chunks={} remaining_producers={}",
                        self.exchanger.exchange_id(),
                        self.owner_node_id,
                        self.driver_id,
                        self.partition,
                        buffered,
                        remaining
                    );
                }
                self.blocked_empty = false;
            }
            return Ok(Some(chunk));
        }
        if self.exchanger.is_done(self.partition) {
            self.finished = true;
            self.exchanger.finish_source();
            let stats = self.exchanger.stats_snapshot();
            if let Some(part) = stats
                .partitions
                .iter()
                .find(|p| p.partition == self.partition)
            {
                debug!(
                    "LocalExchangeSource finished: exchange_id={} owner_node_id={} driver_id={} partition={} pushed_rows={} popped_rows={} pushed_chunks={} popped_chunks={} buffered_chunks={} remaining_producers={}",
                    stats.exchange_id,
                    self.owner_node_id,
                    self.driver_id,
                    part.partition,
                    part.pushed_rows,
                    part.popped_rows,
                    part.pushed_chunks,
                    part.popped_chunks,
                    part.buffered_chunks,
                    stats.remaining_producers
                );
            }
            return Ok(None);
        }
        if !self.blocked_empty {
            if let Some((buffered, remaining)) =
                self.exchanger.partition_buffered_chunks(self.partition)
            {
                debug!(
                    "LocalExchangeSource blocked (empty): exchange_id={} owner_node_id={} driver_id={} partition={} buffered_chunks={} remaining_producers={}",
                    self.exchanger.exchange_id(),
                    self.owner_node_id,
                    self.driver_id,
                    self.partition,
                    buffered,
                    remaining
                );
            }
            self.blocked_empty = true;
        }
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        if self.finished {
            return None;
        }
        Some(self.exchanger.source_observable())
    }
}
