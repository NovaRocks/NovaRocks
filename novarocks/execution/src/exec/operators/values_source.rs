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

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::exec::pipeline::shape::{Source, SourceKernel, SourceStart};
use crate::exec::pipeline::step::{NextStep, StepResult};
use crate::runtime::task::ExecutionResult;

use crate::exec::chunk::Chunk;

use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;

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
        Box::new(Source::new(ValuesSourceOperator {
            name: self.name.clone(),
            chunk: Some(self.chunk.clone()),
            _dop: dop.max(1),
            driver_id: driver_id.max(0),
        }))
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct ValuesSourceOperator {
    name: String,
    chunk: Option<Chunk>,
    _dop: i32,
    driver_id: i32,
}

impl OperatorLifecycle for ValuesSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }
}

impl SourceKernel for ValuesSourceOperator {
    fn start(&self) -> SourceStart {
        if self.driver_id != 0 {
            SourceStart::Finished
        } else {
            SourceStart::Ready
        }
    }
    fn poll_output(
        &mut self,
        _ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
    ) -> ExecutionResult<StepResult> {
        let chunk = self.chunk.take().ok_or_else(|| {
            novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "literal Values output was already transferred",
                ),
            )
        })?;
        let len = chunk.len();
        if len == 0 {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        // Emit all rows from driver 0 only. VALUES chunks are small literal
        // data and do not benefit from parallelism.  Concentrating all rows in
        // a single driver preserves the original INSERT row order, which is
        // critical for deterministic AUTO_INCREMENT ID assignment.
        let driver_id = self.driver_id.max(0) as usize;
        if driver_id != 0 {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        Ok(StepResult::new(Some(chunk), NextStep::Finished, 0))
    }
}

#[cfg(test)]
#[path = "typed_values_source.rs"]
mod typed_values_source;
