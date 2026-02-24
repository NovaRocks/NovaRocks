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
//! Result sink for buffering query output rows.
//!
//! Responsibilities:
//! - Collects output chunks into a shared result handle consumed by fetch paths.
//! - Tracks completion and terminal error state for client-side result retrieval.
//!
//! Key exported interfaces:
//! - Types: `ResultSinkHandle`, `ResultSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::{Arc, Mutex};

use crate::exec::chunk::Chunk;

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

#[derive(Clone)]
/// Shared handle that stores buffered result chunks and terminal status for client fetch.
pub struct ResultSinkHandle {
    inner: Arc<Mutex<Vec<Chunk>>>,
}

impl ResultSinkHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn take_chunks(&self) -> Vec<Chunk> {
        let mut guard = self.inner.lock().expect("result_sink lock");
        guard.drain(..).collect()
    }
}

/// Factory for result sinks that append output chunks to query result buffers.
pub struct ResultSinkFactory {
    name: String,
    handle: ResultSinkHandle,
}

impl ResultSinkFactory {
    pub fn new(handle: ResultSinkHandle) -> Self {
        Self::new_with_plan_node_id(handle, None)
    }

    pub fn new_with_plan_node_id(handle: ResultSinkHandle, plan_node_id: Option<i32>) -> Self {
        let plan_node_id = match plan_node_id {
            Some(id) if id >= 0 => id,
            _ => -1,
        };
        let name = format!("RESULT_SINK (plan_node_id={plan_node_id})");
        Self { name, handle }
    }
}

impl OperatorFactory for ResultSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(ResultSinkOperator {
            name: self.name.clone(),
            handle: self.handle.clone(),
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct ResultSinkOperator {
    name: String,
    handle: ResultSinkHandle,
    finished: bool,
}

impl Operator for ResultSinkOperator {
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

impl ProcessorOperator for ResultSinkOperator {
    fn need_input(&self) -> bool {
        !self.is_finished()
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        let mut guard = self.handle.inner.lock().expect("result_sink lock");
        guard.push(chunk);
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finished = true;
        Ok(())
    }
}
