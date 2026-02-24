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
//! VALUES source operator.
//!
//! Responsibilities:
//! - Produces literal rows from VALUES plan nodes as source chunks.
//! - Acts as a finite source with deterministic row materialization order.
//!
//! Key exported interfaces:
//! - Types: `ValuesSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::chunk::Chunk;

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for VALUES source operators that emit literal plan rows.
pub struct ValuesSourceFactory {
    name: String,
    chunk: Chunk,
}

impl ValuesSourceFactory {
    pub fn new(chunk: Chunk, node_id: i32) -> Self {
        let name = if node_id >= 0 {
            format!("ValuesSource (id={node_id})")
        } else {
            "ValuesSource".to_string()
        };
        Self { name, chunk }
    }
}

impl OperatorFactory for ValuesSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(ValuesSourceOperator {
            name: self.name.clone(),
            chunk: self.chunk.clone(),
            dop: dop.max(1),
            driver_id: driver_id.max(0),
            emitted: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct ValuesSourceOperator {
    name: String,
    chunk: Chunk,
    dop: i32,
    driver_id: i32,
    emitted: bool,
}

impl Operator for ValuesSourceOperator {
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
        self.emitted
    }
}

impl ProcessorOperator for ValuesSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        !self.is_finished()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("values source operator does not accept input".to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let len = self.chunk.len();
        if len == 0 {
            return Ok(None);
        }
        let dop = self.dop.max(1) as usize;
        let driver_id = self.driver_id.max(0) as usize;
        if driver_id >= dop {
            return Ok(None);
        }
        let start = (len * driver_id) / dop;
        let end = (len * (driver_id + 1)) / dop;
        if start >= end {
            return Ok(None);
        }
        Ok(Some(self.chunk.slice(start, end - start)))
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }
}
