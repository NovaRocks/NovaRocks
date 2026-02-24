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
//! Expression filter processor for row-level predicate evaluation.
//!
//! Responsibilities:
//! - Evaluates boolean predicates over incoming chunks and applies selection masks.
//! - Produces filtered chunks while preserving schema and nullability semantics.
//!
//! Key exported interfaces:
//! - Types: `FilterProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for predicate processors that apply row-level filter masks to input chunks.
pub struct FilterProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    predicate: ExprId,
}

impl FilterProcessorFactory {
    pub fn new(node_id: i32, arena: Arc<ExprArena>, predicate: ExprId) -> Self {
        let name = if node_id >= 0 {
            format!("FILTER (id={node_id})")
        } else {
            "FILTER".to_string()
        };
        Self {
            name,
            arena,
            predicate,
        }
    }
}

impl OperatorFactory for FilterProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(FilterProcessorOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            predicate: self.predicate,
            pending_output: None,
            finishing: false,
            finished: false,
        })
    }
}

struct FilterProcessorOperator {
    name: String,
    arena: Arc<ExprArena>,
    predicate: ExprId,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

impl Operator for FilterProcessorOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for FilterProcessorOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending_output.is_none()
    }

    fn has_output(&self) -> bool {
        self.pending_output.is_some()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        if self.pending_output.is_some() {
            return Err("filter received input while output buffer is full".to_string());
        }
        if chunk.is_empty() {
            self.pending_output = Some(Chunk::default());
            return Ok(());
        }

        // Vectorized implementation: use eval to compute predicate on entire chunk
        let predicate_array = self
            .arena
            .eval(self.predicate, &chunk)
            .map_err(|e| e.to_string())?;

        // Downcast to BooleanArray
        let filter_mask = predicate_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "Filter predicate must return boolean array".to_string())?;

        // Use Arrow filter kernel to filter the RecordBatch
        let filtered_batch = filter_record_batch(&chunk.batch, filter_mask)
            .map_err(|e| format!("Filter failed: {}", e))?;

        self.pending_output = Some(Chunk::new(filtered_batch));
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if out.is_some() && self.finishing && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        if self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}
