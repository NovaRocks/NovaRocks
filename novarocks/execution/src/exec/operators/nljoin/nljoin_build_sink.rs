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
//! Nested-loop join build sink.
//!
//! Responsibilities:
//! - Collects build-side chunks for later nested-loop probing.
//! - Publishes build artifacts and completion state to NL-join probe operators.
//!
//! Key exported interfaces:
//! - Types: `NlJoinBuildSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;

use super::nljoin_shared::{NlJoinBuildArtifact, NlJoinSharedState};

/// Factory for nested-loop join build sinks that materialize build-side chunks.
pub struct NlJoinBuildSinkFactory {
    name: String,
    state: Arc<NlJoinSharedState>,
}

impl NlJoinBuildSinkFactory {
    pub(crate) fn new(state: Arc<NlJoinSharedState>) -> Self {
        let node_id = state.node_id();
        let name = if node_id >= 0 {
            format!("NlJoinBuildSink (id={node_id})")
        } else {
            "NlJoinBuildSink".to_string()
        };
        Self { name, state }
    }
}

impl OperatorFactory for NlJoinBuildSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(NlJoinBuildSinkOperator {
            name: self.name.clone(),
            state: Arc::clone(&self.state),
            build_batches: Vec::new(),
            finished: false,
            build_batches_mem_tracker: None,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct NlJoinBuildSinkOperator {
    name: String,
    state: Arc<NlJoinSharedState>,
    build_batches: Vec<Chunk>,
    finished: bool,
    build_batches_mem_tracker: Option<Arc<MemTracker>>,
}

impl Operator for NlJoinBuildSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let batches = MemTracker::new_child("BuildBatches", &tracker);
        self.build_batches_mem_tracker = Some(Arc::clone(&batches));
        for chunk in self.build_batches.iter_mut() {
            chunk.transfer_to(&batches);
        }
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

impl ProcessorOperator for NlJoinBuildSinkOperator {
    fn need_input(&self) -> bool {
        !self.is_finished()
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, mut chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        if !chunk.is_empty() {
            if let Some(tracker) = self.build_batches_mem_tracker.as_ref() {
                chunk.transfer_to(tracker);
            }
            self.build_batches.push(chunk);
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        let mut batches = std::mem::take(&mut self.build_batches);

        if let Some(root) = state.mem_tracker() {
            let label = format!("JoinBuildArtifact: {}", self.state.build_dep().name());
            let artifact = MemTracker::new_child(label, &root);
            let artifact_batches = MemTracker::new_child("BuildBatches", &artifact);
            for chunk in batches.iter_mut() {
                chunk.transfer_to(&artifact_batches);
            }
        }

        let artifact = Arc::new(NlJoinBuildArtifact::new(batches));
        self.state.set_build(artifact).map_err(|e| e.to_string())?;
        Ok(())
    }
}
