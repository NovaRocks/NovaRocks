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

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::task::ExecutionResult;

use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{Source, SourceKernel, SourceRead};
use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};
use std::sync::Arc;

use crate::exec::operators::local_exchanger::LocalExchanger;
use tracing::debug;

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
        // `driver_id` is the driver's index inside this pipeline, which is
        // also its consumer identity in the exchanger.
        let consumer = driver_id.max(0) as usize;
        let partition = consumer % self.partition_count;
        Box::new(Source::new(LocalExchangeSourceOperator {
            name: self.name.clone(),
            owner_node_id: self.owner_node_id,
            driver_id,
            consumer,
            partition,
            exchanger: Arc::clone(&self.exchanger),
            finished: false,
            blocked_empty: false,
        }))
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct LocalExchangeSourceOperator {
    name: String,
    owner_node_id: i32,
    driver_id: i32,
    consumer: usize,
    partition: usize,
    exchanger: Arc<LocalExchanger>,
    finished: bool,
    blocked_empty: bool,
}

impl LocalExchangeSourceOperator {
    fn finish(&mut self) {
        if !self.finished {
            self.finished = true;
            self.exchanger.close_consumer(self.consumer);
        }
    }
}

impl OperatorLifecycle for LocalExchangeSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.finish();
        Ok(())
    }

    fn close(&mut self) -> ExecutionResult<()> {
        self.finish();
        Ok(())
    }

    fn cancel(&mut self) {
        self.finish();
    }
}

impl SourceKernel for LocalExchangeSourceOperator {
    fn poll_output(
        &mut self,
        _ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
    ) -> ExecutionResult<StepResult> {
        if self.finished {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        let observable = self.exchanger.source_observable();
        let observed_generation = observable.generation();
        let read = self.exchanger.next_chunk(self.partition);
        if let SourceRead::Chunk(chunk) = read {
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
            return Ok(StepResult::new(Some(chunk), NextStep::Runnable, 0));
        }
        if matches!(read, SourceRead::Finished) {
            self.finish();
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
            return Ok(StepResult::new(None, NextStep::Finished, 0));
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
        Ok(StepResult::new(
            None,
            NextStep::Wait(WaitFact {
                reason: WaitReason::Input,
                observable,
                observed_generation,
                deadline: None,
            }),
            0,
        ))
    }
}
