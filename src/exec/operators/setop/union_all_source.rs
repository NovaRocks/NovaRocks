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

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        // UNION ALL fan-in owns a single-consumer shared queue: only the
        // pipeline-local driver 0 drains it.
        Box::new(UnionAllSourceOperator {
            name: self.name.clone(),
            state: self.state.clone(),
            finished: driver_id != 0,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};

    fn one_row_chunk() -> Chunk {
        let field = Field::new("v", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .expect("record batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(SlotId(1), &field, None).expect("chunk slot"),
            ])
            .expect("chunk schema"),
        );
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
    }

    #[test]
    fn non_zero_source_driver_does_not_drain_shared_queue() {
        let state = RuntimeState::default();
        let shared = UnionAllSharedState::new(1, 7);
        shared.push_chunk(&state, one_row_chunk());

        let factory = UnionAllSourceFactory::new(shared.clone(), 7);
        let mut inactive = factory.create(2, 1);
        let inactive_source = inactive
            .as_processor_mut()
            .expect("inactive source processor");
        assert!(!inactive_source.has_output());
        assert!(
            inactive_source
                .pull_chunk(&state)
                .expect("inactive source pull")
                .is_none()
        );

        let mut active = factory.create(2, 0);
        let active_source = active.as_processor_mut().expect("active source processor");
        assert!(active_source.has_output());
        assert_eq!(
            active_source
                .pull_chunk(&state)
                .expect("active source pull")
                .expect("active source chunk")
                .batch
                .num_rows(),
            1
        );
    }
}
