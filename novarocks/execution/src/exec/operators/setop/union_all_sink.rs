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

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;

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
        Box::new(UnionAllSinkOperator {
            name: self.name.clone(),
            state: self.state.clone(),
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct UnionAllSinkOperator {
    name: String,
    state: UnionAllSharedState,
    finished: bool,
}

impl Operator for UnionAllSinkOperator {
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

impl ProcessorOperator for UnionAllSinkOperator {
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
        if chunk.is_empty() {
            return Ok(());
        }
        self.state.push_chunk(state, chunk);
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.state.producer_finished();
        Ok(())
    }
}
