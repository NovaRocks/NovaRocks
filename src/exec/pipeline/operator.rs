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
//! Core operator traits and blocking semantics.
//!
//! Responsibilities:
//! - Defines source/processor/sink execution contracts and blocked-reason signaling.
//! - Used by drivers to orchestrate cooperative operator execution steps.
//!
//! Key exported interfaces:
//! - Types: `BlockedReason`, `Operator`, `ProcessorOperator`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::RuntimeState;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
/// The execution engine uses cooperative scheduling.
///
/// Operators are driven by a [`PipelineDriver`](crate::exec::pipeline::driver::PipelineDriver)
/// which repeatedly tries to move data from upstream to downstream.
/// When a driver cannot make progress without blocking, it records a [`BlockedReason`]
/// and yields.
pub enum BlockedReason {
    /// Upstream currently has no data available.
    InputEmpty,
    /// Downstream cannot accept more output at the moment.
    OutputFull,
    /// Blocked on a dependency object (e.g. build-side ready).
    Dependency(DependencyHandle),
}

/// Base operator contract implemented by source/processor/sink operator implementations.
pub trait Operator: Send {
    fn name(&self) -> &str;

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let _ = tracker;
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        let _ = profiles;
    }

    fn prepare(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn cancel(&mut self) {
        // Default: nothing to cancel.
    }

    fn is_finished(&self) -> bool {
        false
    }

    fn pending_finish(&self) -> bool {
        false
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        None
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        None
    }
}

/// Extended operator contract for processor stages with push/pull semantics.
pub trait ProcessorOperator: Operator {
    fn need_input(&self) -> bool;

    fn has_output(&self) -> bool;

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String>;

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String>;

    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String>;

    /// Dependency that must be ready before the operator can make progress.
    /// This is used for build-side readiness (join, runtime filters, etc.).
    fn precondition_dependency(&self) -> Option<DependencyHandle> {
        None
    }

    /// Observable for source-side readiness (has_output becomes true).
    fn source_observable(&self) -> Option<Arc<Observable>> {
        None
    }

    /// Observable for sink-side readiness (need_input becomes true).
    fn sink_observable(&self) -> Option<Arc<Observable>> {
        None
    }
}
