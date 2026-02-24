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
//! Source side of analytic-window execution.
//!
//! Responsibilities:
//! - Pulls finalized windowed rows from shared analytic state and emits them downstream.
//! - Respects sink completion and shared error propagation before producing output chunks.
//!
//! Key exported interfaces:
//! - Types: `AnalyticSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::chunk::Chunk;
use crate::exec::operators::analytic_shared::AnalyticSharedState;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::runtime_state::RuntimeState;
use std::sync::Arc;

/// Factory that builds analytic source operators which emit finalized window results from shared analytic state.
pub struct AnalyticSourceFactory {
    name: String,
    state: AnalyticSharedState,
}

impl AnalyticSourceFactory {
    pub(crate) fn new(state: AnalyticSharedState) -> Self {
        Self {
            name: "AnalyticSource".to_string(),
            state,
        }
    }
}

impl OperatorFactory for AnalyticSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(AnalyticSourceOperator {
            name: self.name.clone(),
            state: self.state.clone(),
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct AnalyticSourceOperator {
    name: String,
    state: AnalyticSharedState,
}

impl Operator for AnalyticSourceOperator {
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
        self.state.is_done()
    }
}

impl ProcessorOperator for AnalyticSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        self.state.has_output()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("analytic source operator does not accept input".to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if let Some(chunk) = self.state.pop_output() {
            return Ok(Some(chunk));
        }
        if self.state.is_done() {
            return Ok(None);
        }
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        Some(self.state.observable())
    }
}
