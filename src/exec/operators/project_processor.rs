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
//! Projection processor for expression column materialization.
//!
//! Responsibilities:
//! - Evaluates projection expression lists and constructs output chunks with projected slots.
//! - Preserves output schema ordering and nullability contracts from plan lowering.
//!
//! Key exported interfaces:
//! - Types: `ProjectProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use arrow::array::{ArrayRef, new_empty_array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for projection processors that evaluate expression lists into projected chunks.
pub struct ProjectProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    exprs: Vec<ExprId>,
    expr_slot_ids: Vec<SlotId>,
    output_indices: Option<Vec<usize>>,
    output_slots: Vec<SlotId>,
}

impl ProjectProcessorFactory {
    pub fn new(
        node_id: i32,
        is_subordinate: bool,
        arena: Arc<ExprArena>,
        exprs: Vec<ExprId>,
        expr_slot_ids: Vec<SlotId>,
        output_indices: Option<Vec<usize>>,
        output_slots: Vec<SlotId>,
    ) -> Self {
        let mut name = if node_id >= 0 {
            format!("PROJECT (id={node_id})")
        } else {
            "PROJECT".to_string()
        };
        // Mark pipeline-internal Project as subordinate so FE won't use it as the plan node's
        // primary operator when deriving `OutputRows` from `CommonMetrics.PullRowNum`.
        if is_subordinate {
            name.push_str(" (subordinate)");
        }
        Self {
            name,
            arena,
            exprs,
            expr_slot_ids,
            output_indices,
            output_slots,
        }
    }
}

impl OperatorFactory for ProjectProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(ProjectProcessorOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            exprs: self.exprs.clone(),
            expr_slot_ids: self.expr_slot_ids.clone(),
            output_indices: self.output_indices.clone(),
            output_slots: self.output_slots.clone(),
            pending_output: None,
            finishing: false,
            finished: false,
        })
    }
}

struct ProjectProcessorOperator {
    name: String,
    arena: Arc<ExprArena>,
    exprs: Vec<ExprId>,
    expr_slot_ids: Vec<SlotId>,
    output_indices: Option<Vec<usize>>,
    output_slots: Vec<SlotId>,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

impl Operator for ProjectProcessorOperator {
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

impl ProcessorOperator for ProjectProcessorOperator {
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
            return Err("project received input while output buffer is full".to_string());
        }
        let out = self.process_one(chunk)?;
        self.pending_output = out;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if self.finishing && self.pending_output.is_none() {
            self.finished = true;
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

impl ProjectProcessorOperator {
    fn process_one(&mut self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        if chunk.is_empty() {
            return Ok(Some(self.empty_output_chunk()?));
        }

        if self.expr_slot_ids.len() != self.exprs.len() {
            return Err(format!(
                "project expr slot ids mismatch: exprs={} slot_ids={}",
                self.exprs.len(),
                self.expr_slot_ids.len()
            ));
        }

        // Vectorized implementation: compute all expressions on the chunk
        // Handle CSE by appending intermediate results to a working chunk
        let mut working_chunk = chunk.clone();
        let mut computed_columns: Vec<ArrayRef> = Vec::with_capacity(self.exprs.len());

        for (expr_id, slot_id) in self.exprs.iter().zip(self.expr_slot_ids.iter()) {
            // Evaluate expression on the current working chunk (which includes previously computed columns)
            let array = self
                .arena
                .eval(*expr_id, &working_chunk)
                .map_err(|e| e.to_string())?;

            computed_columns.push(array.clone());

            // Append this computed column to working_chunk for CSE support
            // This allows subsequent expressions to reference this result via SlotId.
            if let Some(existing_idx) = working_chunk.slot_id_to_index().get(slot_id).copied() {
                let is_identity = matches!(
                    self.arena.node(*expr_id),
                    Some(ExprNode::SlotId(existing)) if *existing == *slot_id
                );
                if is_identity {
                    continue;
                }

                // Some FE plans intentionally project into an existing slot id.
                // Replace the existing column so subsequent expressions read the updated value.
                let mut columns = working_chunk.batch.columns().to_vec();
                columns[existing_idx] = array;

                let working_schema = working_chunk.batch.schema();
                let mut fields = working_schema.fields().to_vec();
                let old_field = working_schema.field(existing_idx);
                let replaced = field_with_slot_id(
                    Field::new(
                        old_field.name(),
                        computed_columns.last().unwrap().data_type().clone(),
                        true,
                    ),
                    *slot_id,
                );
                fields[existing_idx] = Arc::new(replaced);
                let new_schema = Arc::new(Schema::new_with_metadata(
                    fields,
                    working_schema.metadata().clone(),
                ));
                working_chunk = Chunk::new(
                    RecordBatch::try_new(new_schema, columns)
                        .map_err(|e| format!("Failed to replace chunk column: {}", e))?,
                );
                continue;
            }

            let mut columns = working_chunk.batch.columns().to_vec();
            columns.push(array);

            // Create new schema with appended field
            let mut fields = working_chunk.batch.schema().fields().to_vec();
            let field = Field::new(
                format!("_cse_{}", computed_columns.len() - 1),
                computed_columns.last().unwrap().data_type().clone(),
                true,
            );
            fields.push(Arc::new(field_with_slot_id(field, *slot_id)));
            let new_schema = Arc::new(Schema::new(fields));

            working_chunk = Chunk::new(
                RecordBatch::try_new(new_schema, columns)
                    .map_err(|e| format!("Failed to extend chunk: {}", e))?,
            );
        }

        // Apply output_indices if specified (column pruning)
        let final_columns = if let Some(indices) = &self.output_indices {
            indices
                .iter()
                .map(|&idx| {
                    computed_columns.get(idx).cloned().ok_or_else(|| {
                        format!(
                            "project output index {} out of bounds (exprs={})",
                            idx,
                            computed_columns.len()
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            computed_columns
        };

        // Infer schema from final columns
        if self.output_slots.len() != final_columns.len() {
            return Err(format!(
                "project output slots mismatch: slots={} columns={}",
                self.output_slots.len(),
                final_columns.len()
            ));
        }

        let mut fields: Vec<Arc<Field>> = Vec::with_capacity(final_columns.len());
        for (idx, (array, slot_id)) in final_columns
            .iter()
            .zip(self.output_slots.iter())
            .enumerate()
        {
            let base = Field::new(format!("col_{}", idx), array.data_type().clone(), true);
            fields.push(Arc::new(field_with_slot_id(base, *slot_id)));
        }
        let schema = Arc::new(Schema::new(fields));

        let output_batch = RecordBatch::try_new(schema, final_columns)
            .map_err(|e| format!("Failed to create output batch: {}", e))?;

        Ok(Some(Chunk::new(output_batch)))
    }

    fn empty_output_chunk(&self) -> Result<Chunk, String> {
        let selected_exprs = if let Some(indices) = &self.output_indices {
            indices
                .iter()
                .map(|&idx| {
                    self.exprs.get(idx).copied().ok_or_else(|| {
                        format!(
                            "project output index {} out of bounds (exprs={})",
                            idx,
                            self.exprs.len()
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            self.exprs.clone()
        };

        if self.output_slots.len() != selected_exprs.len() {
            return Err(format!(
                "project output slots mismatch on empty input: slots={} exprs={}",
                self.output_slots.len(),
                selected_exprs.len()
            ));
        }

        let mut fields: Vec<Arc<Field>> = Vec::with_capacity(selected_exprs.len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(selected_exprs.len());
        for (idx, (expr_id, slot_id)) in selected_exprs
            .iter()
            .zip(self.output_slots.iter())
            .enumerate()
        {
            let data_type = self
                .arena
                .data_type(*expr_id)
                .ok_or_else(|| format!("project expr {} type missing on empty input", idx))?
                .clone();
            fields.push(Arc::new(field_with_slot_id(
                Field::new(format!("col_{}", idx), data_type.clone(), true),
                *slot_id,
            )));
            columns.push(new_empty_array(&data_type));
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| format!("Failed to create empty output batch: {}", e))?;
        Ok(Chunk::new(batch))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::ProjectProcessorOperator;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode};

    #[test]
    fn project_allows_overwriting_existing_slot_for_follow_up_exprs() {
        let mut arena = ExprArena::default();
        let expr_write_slot17 =
            arena.push_typed(ExprNode::SlotId(SlotId::new(18)), DataType::Int32);
        let expr_read_slot17 = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Int32);
        let mut op = ProjectProcessorOperator {
            name: "PROJECT".to_string(),
            arena: Arc::new(arena),
            exprs: vec![expr_write_slot17, expr_read_slot17],
            expr_slot_ids: vec![SlotId::new(17), SlotId::new(19)],
            output_indices: None,
            output_slots: vec![SlotId::new(17), SlotId::new(19)],
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                field_with_slot_id(Field::new("s17", DataType::Int32, true), SlotId::new(17)),
                field_with_slot_id(Field::new("s18", DataType::Int32, true), SlotId::new(18)),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![11, 12])),
            ],
        )
        .expect("build input batch");
        let output = op
            .process_one(Chunk::new(batch))
            .expect("project should succeed")
            .expect("project output");

        let c0 = output
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("output column 0");
        let c1 = output
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("output column 1");
        assert_eq!(c0.value(0), 11);
        assert_eq!(c0.value(1), 12);
        assert_eq!(c1.value(0), 11);
        assert_eq!(c1.value(1), 12);
    }
}
