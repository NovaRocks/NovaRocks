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
//! Repeat processor for grouping-set style row expansion.
//!
//! Responsibilities:
//! - Repeats input rows with per-repeat nulling masks to emulate grouping-set semantics.
//! - Maintains repeat-id aware output layout required by downstream aggregation.
//!
//! Key exported interfaces:
//! - Types: `RepeatProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Int64Array, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_slot_id, field_with_slot_id};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for repeat processors that expand rows for grouping-set style execution.
pub struct RepeatProcessorFactory {
    name: String,
    null_slot_ids: Vec<Vec<SlotId>>,
    grouping_slot_ids: Vec<SlotId>,
    grouping_list: Vec<Vec<i64>>,
    repeat_times: usize,
}

impl RepeatProcessorFactory {
    pub fn new(
        node_id: i32,
        null_slot_ids: Vec<Vec<SlotId>>,
        grouping_slot_ids: Vec<SlotId>,
        grouping_list: Vec<Vec<i64>>,
        repeat_times: usize,
    ) -> Self {
        let name = if node_id >= 0 {
            format!("Repeat (id={node_id})")
        } else {
            "Repeat".to_string()
        };
        Self {
            name,
            null_slot_ids,
            grouping_slot_ids,
            grouping_list,
            repeat_times,
        }
    }
}

impl OperatorFactory for RepeatProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(RepeatProcessorOperator {
            name: self.name.clone(),
            null_slot_ids: self.null_slot_ids.clone(),
            grouping_slot_ids: self.grouping_slot_ids.clone(),
            grouping_list: self.grouping_list.clone(),
            repeat_times: self.repeat_times,

            input: None,
            repeat_idx: 0,
            finishing: false,
            finished: false,
            emit_empty_once: false,
            output_schema: None,
        })
    }
}

struct RepeatProcessorOperator {
    name: String,
    null_slot_ids: Vec<Vec<SlotId>>,
    grouping_slot_ids: Vec<SlotId>,
    grouping_list: Vec<Vec<i64>>,
    repeat_times: usize,

    input: Option<Chunk>,
    repeat_idx: usize,
    finishing: bool,
    finished: bool,
    emit_empty_once: bool,
    output_schema: Option<Arc<Schema>>,
}

impl Operator for RepeatProcessorOperator {
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

impl ProcessorOperator for RepeatProcessorOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.input.is_none()
    }

    fn has_output(&self) -> bool {
        if self.finished {
            return false;
        }
        if self.emit_empty_once {
            return true;
        }
        self.input.is_some() && self.repeat_idx < self.repeat_times
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if !self.need_input() {
            return Err("repeat push_chunk called when operator does not need input".to_string());
        }
        self.validate_config()?;

        if chunk.is_empty() {
            self.input = Some(chunk);
            self.repeat_idx = 0;
            self.emit_empty_once = true;
            self.output_schema = None;
            return Ok(());
        }

        let input_schema = chunk.batch.schema();
        let nullable_slot_ids: HashSet<SlotId> = self
            .null_slot_ids
            .iter()
            .flat_map(|slots| slots.iter().copied())
            .collect();

        let mut fields =
            Vec::with_capacity(input_schema.fields().len() + self.grouping_slot_ids.len());
        for input_field in input_schema.fields() {
            let mut field = input_field.as_ref().clone();
            if let Some(slot_id) = field_slot_id(input_field.as_ref())? {
                if nullable_slot_ids.contains(&slot_id) && !field.is_nullable() {
                    field = field.with_nullable(true);
                }
                field = field_with_slot_id(field, slot_id);
            }
            fields.push(Arc::new(field));
        }
        for (i, slot_id) in self.grouping_slot_ids.iter().enumerate() {
            if chunk.slot_id_to_index().contains_key(slot_id) {
                return Err(format!("repeat cannot append existing slot id {}", slot_id));
            }
            let field = Field::new(format!("repeat_grouping_{i}"), DataType::Int64, true);
            fields.push(Arc::new(field_with_slot_id(field, *slot_id)));
        }
        self.output_schema = Some(Arc::new(Schema::new(fields)));

        self.input = Some(chunk);
        self.repeat_idx = 0;
        self.emit_empty_once = false;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if !self.has_output() {
            return Ok(None);
        }
        if self.emit_empty_once {
            self.emit_empty_once = false;
            self.input = None;
            if self.finishing {
                self.finished = true;
            }
            return Ok(Some(Chunk::default()));
        }

        self.validate_config()?;
        let Some(chunk) = self.input.as_ref() else {
            return Ok(None);
        };
        if self.repeat_idx >= self.repeat_times {
            return Ok(None);
        }

        let schema = self
            .output_schema
            .as_ref()
            .ok_or_else(|| "repeat missing output schema; push_chunk was not called".to_string())?;
        let num_rows = chunk.len();
        let mut columns = chunk.batch.columns().to_vec();

        for slot_id in &self.null_slot_ids[self.repeat_idx] {
            let col_idx = chunk
                .slot_id_to_index()
                .get(slot_id)
                .copied()
                .ok_or_else(|| {
                    format!("repeat null slot id {} not found in input chunk", slot_id)
                })?;
            let dt = columns[col_idx].data_type().clone();
            columns[col_idx] = new_null_array(&dt, num_rows);
        }

        for (gidx, values) in self.grouping_list.iter().enumerate() {
            let v = values.get(self.repeat_idx).copied().ok_or_else(|| {
                format!(
                    "repeat grouping_list[{}] missing value at repeat_idx={}",
                    gidx, self.repeat_idx
                )
            })?;
            let arr = Int64Array::from_iter(std::iter::repeat(Some(v)).take(num_rows));
            columns.push(Arc::new(arr) as _);
        }

        let batch = RecordBatch::try_new(Arc::clone(schema), columns)
            .map_err(|e| format!("repeat build batch failed: {e}"))?;

        self.repeat_idx += 1;
        if self.repeat_idx >= self.repeat_times {
            self.input = None;
            if self.finishing {
                self.finished = true;
            }
        }
        Ok(Some(Chunk::new(batch)))
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        if self.input.is_none() && !self.emit_empty_once {
            self.finished = true;
        }
        Ok(())
    }
}

impl RepeatProcessorOperator {
    fn validate_config(&self) -> Result<(), String> {
        if self.repeat_times == 0 {
            return Err("repeat operator requires repeat_times > 0".to_string());
        }
        if self.null_slot_ids.len() != self.repeat_times {
            return Err(format!(
                "repeat null_slot_ids size mismatch: expected={} actual={}",
                self.repeat_times,
                self.null_slot_ids.len()
            ));
        }
        if self.grouping_slot_ids.len() != self.grouping_list.len() {
            return Err(format!(
                "repeat grouping slot ids mismatch: slot_ids={} grouping_list={}",
                self.grouping_slot_ids.len(),
                self.grouping_list.len()
            ));
        }
        for (idx, row) in self.grouping_list.iter().enumerate() {
            if row.len() != self.repeat_times {
                return Err(format!(
                    "repeat grouping_list[{}] size mismatch: expected={} actual={}",
                    idx,
                    self.repeat_times,
                    row.len()
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::runtime_state::RuntimeState;

    use arrow::array::Array;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::SchemaRef;

    fn schema_for(slot_ids: &[SlotId]) -> SchemaRef {
        let mut fields = Vec::new();
        for (idx, slot_id) in slot_ids.iter().enumerate() {
            let field = Field::new(format!("c{idx}"), DataType::Int32, true);
            fields.push(Arc::new(field_with_slot_id(field, *slot_id)));
        }
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn repeat_expands_and_nulls_slots() {
        let schema = schema_for(&[SlotId::new(1), SlotId::new(2)]);
        let a = Arc::new(Int32Array::from(vec![Some(10), Some(20)])) as _;
        let b = Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as _;
        let batch = RecordBatch::try_new(schema, vec![a, b]).expect("record batch");
        let chunk = Chunk::new(batch);

        let mut op = RepeatProcessorOperator {
            name: "Repeat".to_string(),
            null_slot_ids: vec![vec![SlotId::new(2)], vec![SlotId::new(1)]],
            grouping_slot_ids: vec![SlotId::new(10)],
            grouping_list: vec![vec![0, 1]],
            repeat_times: 2,

            input: None,
            repeat_idx: 0,
            finishing: false,
            finished: false,
            emit_empty_once: false,
            output_schema: None,
        };

        let state = RuntimeState::default();
        op.push_chunk(&state, chunk).expect("push_chunk");

        let out1 = op.pull_chunk(&state).expect("pull1").expect("chunk1");
        let out2 = op.pull_chunk(&state).expect("pull2").expect("chunk2");
        assert!(op.pull_chunk(&state).expect("pull3").is_none());

        assert_eq!(out1.len(), 2);
        assert_eq!(out2.len(), 2);

        let a1 = out1.column_by_slot_id(SlotId::new(1)).expect("slot 1");
        let b1 = out1.column_by_slot_id(SlotId::new(2)).expect("slot 2");
        let g1 = out1.column_by_slot_id(SlotId::new(10)).expect("slot 10");

        let a1 = a1.as_any().downcast_ref::<Int32Array>().expect("Int32");
        let b1 = b1.as_any().downcast_ref::<Int32Array>().expect("Int32");
        let g1 = g1.as_any().downcast_ref::<Int64Array>().expect("Int64");

        // First repeat: slot 2 is NULL, grouping=0
        assert_eq!(a1.value(0), 10);
        assert_eq!(a1.value(1), 20);
        assert!(b1.is_null(0));
        assert!(b1.is_null(1));
        assert_eq!(g1.value(0), 0);
        assert_eq!(g1.value(1), 0);

        let a2 = out2.column_by_slot_id(SlotId::new(1)).expect("slot 1");
        let b2 = out2.column_by_slot_id(SlotId::new(2)).expect("slot 2");
        let g2 = out2.column_by_slot_id(SlotId::new(10)).expect("slot 10");

        let a2 = a2.as_any().downcast_ref::<Int32Array>().expect("Int32");
        let b2 = b2.as_any().downcast_ref::<Int32Array>().expect("Int32");
        let g2 = g2.as_any().downcast_ref::<Int64Array>().expect("Int64");

        // Second repeat: slot 1 is NULL, grouping=1
        assert!(a2.is_null(0));
        assert!(a2.is_null(1));
        assert_eq!(b2.value(0), 1);
        assert_eq!(b2.value(1), 2);
        assert_eq!(g2.value(0), 1);
        assert_eq!(g2.value(1), 1);
    }
}
