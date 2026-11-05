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
//! UNION ALL sink branch operator.
//!
//! Responsibilities:
//! - Pushes branch chunks into shared UNION ALL queues without deduplication.
//! - Marks branch completion for source-side termination detection.
//!
//! Key exported interfaces:
//! - Types: `UnionAllSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::task::ExecutionResult;

use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::{PreparedInput, RawInput},
    shape::{ControlStep, SinkKernel, SinkStep, Transform},
    step::{AdmissionNext, NextStep, WaitFact, WaitReason},
};
use crate::runtime::mem_tracker::MemTracker;
use std::sync::Arc;

use super::union_all_shared::UnionAllSharedState;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for UNION ALL sink branches that enqueue branch chunks into shared state.
pub struct UnionAllSinkFactory {
    name: String,
    state: UnionAllSharedState,
}

impl UnionAllSinkFactory {
    pub(crate) fn new(state: UnionAllSharedState, node_id: i32) -> Self {
        let name = if node_id >= 0 {
            format!("UnionAllSink (id={node_id})")
        } else {
            "UnionAllSink".to_string()
        };
        Self { name, state }
    }
}

impl OperatorFactory for UnionAllSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(UnionAllSinkOperator {
            name: self.name.clone(),
            state: self.state.clone(),
            memory: None,
            producer_finished: false,
        }))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct UnionAllSinkOperator {
    name: String,
    state: UnionAllSharedState,
    memory: Option<QueueInputMemory>,
    producer_finished: bool,
}
struct QueueInputMemory {
    tracker: Option<Arc<MemTracker>>,
}

impl UnionAllSinkOperator {
    fn finish_producer(&mut self, normal: bool) {
        if !self.producer_finished {
            self.producer_finished = true;
            if normal {
                self.state.producer_finished();
            } else {
                self.state.producer_abandoned();
            }
        }
    }
}
impl OperatorLifecycle for UnionAllSinkOperator {
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.finish_producer(false);
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.finish_producer(false);
        Ok(())
    }
    fn cancel(&mut self) {
        self.finish_producer(false);
    }

    fn name(&self) -> &str {
        &self.name
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.state.bind_runtime(state)?;
        self.memory = Some(QueueInputMemory {
            tracker: self.state.queue_mem_tracker(state),
        });
        Ok(())
    }
}
impl InputEncodingPolicy for UnionAllSinkOperator {}
impl SinkKernel for UnionAllSinkOperator {
    fn poll_control(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        if self.state.consumer_closed() {
            self.finish_producer(false);
            Ok(ControlStep::Withdrawn)
        } else {
            Ok(ControlStep::Active)
        }
    }
    fn control_observable(
        &self,
    ) -> Option<Arc<crate::exec::pipeline::schedule::observer::Observable>> {
        Some(self.state.observable())
    }
    fn admission(
        &self,
        _: &mut DriverStep<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        let observable = self.state.observable();
        let observed_generation = observable.generation();
        if !self.state.need_input() && !self.state.consumer_closed() {
            Ok(Some(AdmissionNext::Wait(WaitFact {
                reason: WaitReason::Output,
                observable,
                observed_generation,
                deadline: None,
            })))
        } else {
            Ok(None)
        }
    }

    fn consume(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let memory = self.memory.as_ref().ok_or_else(|| {
            novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "union input runtime binding is missing",
                ),
            )
        })?;
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        if !chunk.is_empty() {
            self.state.push_chunk(memory.tracker.as_ref(), chunk)?;
        }
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<SinkStep> {
        self.finish_producer(true);
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
}
