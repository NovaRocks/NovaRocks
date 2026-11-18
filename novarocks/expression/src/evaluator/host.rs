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

use super::ScalarCallFailure;
use novarocks_functions::AggregateStateAllocator;
use std::sync::Arc;

/// The evaluator owns the order of preparation and execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarCallPhase {
    Preparation,
    Body,
}

/// Requests authored by the evaluator or by the original callback host.
/// The host applies its own accounting to these facts during admission.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScalarCallRequestFacts {
    bytes: usize,
    requests: usize,
}

impl ScalarCallRequestFacts {
    pub const fn new(bytes: usize, requests: usize) -> Self {
        Self { bytes, requests }
    }

    pub const fn bytes(self) -> usize {
        self.bytes
    }

    pub const fn requests(self) -> usize {
        self.requests
    }

    pub fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.bytes.checked_add(other.bytes)?,
            self.requests.checked_add(other.requests)?,
        ))
    }
}

// Design: ADR-0239 (docs/adr/ADR-0239-expression-evaluation-and-host-admission.md)
/// Synchronous admission capability supplied by the original evaluator host.
pub trait ScalarCallHost {
    type Refusal;

    /// Observe the beginning of a leaf call.
    fn begin_call(&mut self);

    /// Admit this phase, then invoke the admitted body exactly once on success.
    /// Return the body's failure without replacing its cause.
    fn run_admitted(
        &mut self,
        phase: ScalarCallPhase,
        facts: ScalarCallRequestFacts,
        body: &mut dyn FnMut() -> Result<(), ScalarCallFailure<Self::Refusal>>,
    ) -> Result<(), ScalarCallFailure<Self::Refusal>>;

    /// Return requests for the original whole-invocation callback.
    fn callback_request_facts(
        &mut self,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    ) -> Result<ScalarCallRequestFacts, ScalarCallFailure<Self::Refusal>>;
}
