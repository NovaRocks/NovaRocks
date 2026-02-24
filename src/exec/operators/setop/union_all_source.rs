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
//! UNION ALL source operator.
//!
//! Responsibilities:
//! - Pulls buffered chunks from shared UNION ALL state and emits merged output stream.
//! - Terminates when all sink branches are finished and queues are drained.
//!
//! Key exported interfaces:
//! - Types: `UnionAllSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;

use super::union_all_shared::UnionAllSharedState;
use crate::runtime::runtime_state::RuntimeState;
use std::sync::Arc;

/// Factory for UNION ALL source operators that emit merged branch chunk streams.
pub struct UnionAllSourceFactory {
    name: String,
    state: UnionAllSharedState,
}

impl UnionAllSourceFactory {
    pub(crate) fn new(state: UnionAllSharedState, node_id: i32) -> Self {
        let name = if node_id >= 0 {
            format!("UnionAllSource (id={node_id})")
        } else {
            "UnionAllSource".to_string()
        };
        Self { name, state }
    }
}

impl OperatorFactory for UnionAllSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(UnionAllSourceOperator {
            name: self.name.clone(),
            state: self.state.clone(),
            finished: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct UnionAllSourceOperator {
    name: String,
    state: UnionAllSharedState,
    finished: bool,
}

impl Operator for UnionAllSourceOperator {
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

impl ProcessorOperator for UnionAllSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        if self.finished {
            return false;
        }
        self.state.has_buffered() || self.state.remaining_producers() == 0
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("union all source operator does not accept input".to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if self.finished {
            return Ok(None);
        }

        let chunk = self.state.pop_chunk();
        if let Some(chunk) = chunk {
            return Ok(Some(chunk));
        }
        if self.state.remaining_producers() == 0 {
            self.finished = true;
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
        Some(self.state.observable())
    }
}
