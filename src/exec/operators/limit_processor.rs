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
//! Row-limit processor for LIMIT/OFFSET semantics.
//!
//! Responsibilities:
//! - Tracks consumed and emitted row counts against configured limit and offset bounds.
//! - Truncates or drops chunks deterministically once the row budget is exhausted.
//!
//! Key exported interfaces:
//! - Types: `LimitProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::chunk::Chunk;

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;
use std::sync::Arc;
use std::sync::Mutex;

/// Factory for limit processors that enforce LIMIT/OFFSET row budgets.
pub struct LimitProcessorFactory {
    name: String,
    state: Arc<Mutex<LimitState>>,
}

#[derive(Debug)]
struct LimitState {
    remaining_offset: usize,
    remaining_limit: Option<usize>,
}

impl LimitProcessorFactory {
    pub fn new(node_id: i32, limit: Option<usize>, offset: usize) -> Self {
        let name = if node_id >= 0 {
            format!("LIMIT (id={node_id})")
        } else {
            "LIMIT".to_string()
        };
        Self {
            name,
            state: Arc::new(Mutex::new(LimitState {
                remaining_offset: offset,
                remaining_limit: limit,
            })),
        }
    }
}

impl OperatorFactory for LimitProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(LimitProcessorOperator {
            name: self.name.clone(),
            state: Arc::clone(&self.state),
            pending_output: None,
            finishing: false,
            finished: false,
        })
    }
}

struct LimitProcessorOperator {
    name: String,
    state: Arc<Mutex<LimitState>>,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

impl Operator for LimitProcessorOperator {
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

impl ProcessorOperator for LimitProcessorOperator {
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
            return Err("limit received input while output buffer is full".to_string());
        }
        let chunk_rows = chunk.len();
        if chunk_rows == 0 {
            return Ok(());
        }

        let (skip_rows, take_rows, exhausted_limit) = {
            let mut state = self.state.lock().expect("limit state lock");
            if state.remaining_limit == Some(0) {
                (0usize, 0usize, true)
            } else {
                let skip_rows = state.remaining_offset.min(chunk_rows);
                state.remaining_offset -= skip_rows;

                let available_rows = chunk_rows - skip_rows;
                let take_rows = match state.remaining_limit {
                    Some(remaining) => {
                        let take = remaining.min(available_rows);
                        state.remaining_limit = Some(remaining - take);
                        take
                    }
                    None => available_rows,
                };
                let exhausted_limit = state.remaining_limit == Some(0);
                (skip_rows, take_rows, exhausted_limit)
            }
        };

        if take_rows > 0 {
            self.pending_output = Some(chunk.slice(skip_rows, take_rows));
        }

        if exhausted_limit && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if self.pending_output.is_none() {
            let exhausted_limit = {
                let state = self.state.lock().expect("limit state lock");
                state.remaining_limit == Some(0)
            };
            if self.finishing || exhausted_limit {
                self.finished = true;
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_chunk(start: i32, rows: usize) -> Chunk {
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Utf8, false), SlotId::new(2)),
        ]));
        let mut keys = Vec::with_capacity(rows);
        let mut vals = Vec::with_capacity(rows);
        for i in 0..rows {
            keys.push(start + i as i32);
            vals.push(format!("v{}", start + i as i32));
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(StringArray::from(vals)),
            ],
        )
        .expect("build record batch");
        Chunk::try_new(batch).expect("build chunk")
    }

    fn chunk_values(chunk: &Chunk) -> Vec<i32> {
        let arr = chunk
            .columns()
            .first()
            .expect("first column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    #[test]
    fn limit_processor_applies_offset_then_limit() {
        let state = RuntimeState::default();
        let mut op = LimitProcessorFactory::new(1, Some(4), 3).create(1, 0);
        let p = op.as_processor_mut().expect("processor op");

        p.push_chunk(&state, make_chunk(0, 5))
            .expect("push chunk #1");
        let out1 = p
            .pull_chunk(&state)
            .expect("pull chunk #1")
            .expect("chunk #1 exists");
        assert_eq!(chunk_values(&out1), vec![3, 4]);

        p.push_chunk(&state, make_chunk(5, 5))
            .expect("push chunk #2");
        let out2 = p
            .pull_chunk(&state)
            .expect("pull chunk #2")
            .expect("chunk #2 exists");
        assert_eq!(chunk_values(&out2), vec![5, 6]);

        assert!(op.is_finished());
    }

    #[test]
    fn limit_processor_supports_offset_only() {
        let state = RuntimeState::default();
        let mut op = LimitProcessorFactory::new(2, None, 2).create(1, 0);
        let p = op.as_processor_mut().expect("processor op");

        p.push_chunk(&state, make_chunk(0, 3))
            .expect("push chunk #1");
        let out1 = p
            .pull_chunk(&state)
            .expect("pull chunk #1")
            .expect("chunk #1 exists");
        assert_eq!(chunk_values(&out1), vec![2]);

        p.push_chunk(&state, make_chunk(3, 2))
            .expect("push chunk #2");
        let out2 = p
            .pull_chunk(&state)
            .expect("pull chunk #2")
            .expect("chunk #2 exists");
        assert_eq!(chunk_values(&out2), vec![3, 4]);
        assert!(!op.is_finished());
    }
}
