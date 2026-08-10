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

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::io::FragmentResultSession;
use crate::runtime::runtime_state::RuntimeState;

#[derive(Debug)]
struct ResultBufferSinkShared {
    remaining_drivers: AtomicI32,
}

impl ResultBufferSinkShared {
    fn new() -> Self {
        Self {
            remaining_drivers: AtomicI32::new(-1),
        }
    }

    fn init_driver_count(&self, dop: i32) {
        let dop = dop.max(1);
        let _ =
            self.remaining_drivers
                .compare_exchange(-1, dop, Ordering::AcqRel, Ordering::Acquire);
    }
}

/// Factory for result sinks that stream encoded batches directly into the fetch result buffer.
pub struct ResultBufferSinkFactory {
    name: String,
    session: Arc<dyn FragmentResultSession>,
    shared: Arc<ResultBufferSinkShared>,
}

impl ResultBufferSinkFactory {
    pub fn new(session: Arc<dyn FragmentResultSession>, plan_node_id: Option<i32>) -> Self {
        let plan_node_id = match plan_node_id {
            Some(id) if id >= 0 => id,
            _ => -1,
        };
        Self {
            name: format!("RESULT_BUFFER_SINK (plan_node_id={plan_node_id})"),
            session,
            shared: Arc::new(ResultBufferSinkShared::new()),
        }
    }
}

impl OperatorFactory for ResultBufferSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        self.shared.init_driver_count(dop);
        Box::new(ResultBufferSinkOperator {
            name: self.name.clone(),
            session: Arc::clone(&self.session),
            shared: Arc::clone(&self.shared),
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct ResultBufferSinkOperator {
    name: String,
    session: Arc<dyn FragmentResultSession>,
    shared: Arc<ResultBufferSinkShared>,
    finished: bool,
}

impl ResultBufferSinkOperator {}

impl Operator for ResultBufferSinkOperator {
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

impl ProcessorOperator for ResultBufferSinkOperator {
    fn need_input(&self) -> bool {
        !self.finished
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished || chunk.is_empty() {
            return Ok(());
        }

        self.session.write(chunk).map_err(|error| error.to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;

        let prev = self.shared.remaining_drivers.fetch_sub(1, Ordering::AcqRel);
        if prev <= 0 {
            return Err("RESULT_SINK driver count underflow".to_string());
        }
        if prev == 1 {
            self.session.finish().map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}
