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
//! No-op sink operator.
//!
//! Responsibilities:
//! - Consumes and discards upstream chunks while preserving pipeline completion semantics.
//! - Used as terminal sink when no materialized output is required.
//!
//! Key exported interfaces:
//! - Types: `NoopSinkFactory`.
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
    input::PreparedInput,
    shape::{SinkKernel, SinkStep, Transform},
    step::NextStep,
};

/// Factory for terminal sinks that consume input without producing downstream output.
pub struct NoopSinkFactory {
    name: String,
}

impl NoopSinkFactory {
    pub fn new() -> Self {
        Self {
            name: "NOOP_SINK".to_string(),
        }
    }
}

impl Default for NoopSinkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl OperatorFactory for NoopSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(NoopSinkOperator {
            name: self.name.clone(),
        }))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct NoopSinkOperator {
    name: String,
}

impl OperatorLifecycle for NoopSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }
}
impl InputEncodingPolicy for NoopSinkOperator {}
impl SinkKernel for NoopSinkOperator {
    fn consume(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        drop(chunk);
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
}
