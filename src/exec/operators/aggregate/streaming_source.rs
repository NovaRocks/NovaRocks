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
//! Streaming aggregate source operator for Phase 2 — reads pre-aggregated chunks.
//!
//! Responsibilities:
//! - Reads intermediate chunks from the shared `AggregateStreamingState` buffer filled by
//!   the paired `AggregateStreamingSinkOperator`.
//! - Signals the pipeline driver when new chunks are available via an `Observable`.
//! - Reports `is_finished()` once the sink is done and the buffer is fully drained.
//!
//! Key exported interfaces:
//! - Types: `AggregateStreamingSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use crate::exec::chunk::Chunk;
use crate::exec::operators::aggregate::streaming_state::AggregateStreamingState;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::runtime_state::RuntimeState;

/// Factory that constructs streaming aggregate source operators for Phase 2 pre-aggregation.
pub struct AggregateStreamingSourceFactory {
    name: String,
    state: AggregateStreamingState,
}

impl AggregateStreamingSourceFactory {
    pub fn new(node_id: i32, state: AggregateStreamingState) -> Self {
        let name = if node_id >= 0 {
            format!("AGG_STREAMING_SOURCE (id={node_id})")
        } else {
            "AGG_STREAMING_SOURCE".to_string()
        };
        Self { name, state }
    }
}

impl OperatorFactory for AggregateStreamingSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(AggregateStreamingSourceOperator {
            name: self.name.clone(),
            state: self.state.clone(),
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct AggregateStreamingSourceOperator {
    name: String,
    state: AggregateStreamingState,
}

impl Operator for AggregateStreamingSourceOperator {
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

impl ProcessorOperator for AggregateStreamingSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        self.state.has_chunks()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("aggregate streaming source does not accept input".to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(self.state.poll_chunk())
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        Some(self.state.observable())
    }
}
