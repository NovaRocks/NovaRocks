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
//! Sink side of analytic-window execution.
//!
//! Responsibilities:
//! - Consumes input chunks and appends partition/order-aware data into shared analytic state.
//! - Marks sink completion so analytic source operators can emit finalized window outputs.
//!
//! Key exported interfaces:
//! - Types: `AnalyticSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::chunk::Chunk;
use crate::exec::operators::analytic_shared::AnalyticSharedState;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Factory that builds analytic sink operators which materialize partition/order inputs into shared analytic state.
pub struct AnalyticSinkFactory {
    name: String,
    state: AnalyticSharedState,
}

impl AnalyticSinkFactory {
    pub(crate) fn new(state: AnalyticSharedState) -> Self {
        Self {
            name: "AnalyticSink".to_string(),
            state,
        }
    }
}

impl OperatorFactory for AnalyticSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(AnalyticSinkOperator {
            name: self.name.clone(),
            state: self.state.clone(),
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct AnalyticSinkOperator {
    name: String,
    state: AnalyticSharedState,
    finished: bool,
}

impl Operator for AnalyticSinkOperator {
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

impl ProcessorOperator for AnalyticSinkOperator {
    fn need_input(&self) -> bool {
        !self.is_finished()
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.state.push_input(state, chunk);
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.state.finish(state)?;
        self.finished = true;
        Ok(())
    }
}
