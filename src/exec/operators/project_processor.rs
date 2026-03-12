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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, new_empty_array};
use arrow::datatypes::{DataType, Field, Schema};

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

fn projected_field_from_existing(
    existing: &Field,
    data_type: &arrow::datatypes::DataType,
) -> Field {
    Field::new(existing.name(), data_type.clone(), existing.is_nullable())
}

fn field_from_slot_schema(slot_schema: &ChunkSlotSchema, data_type: &DataType) -> Field {
    Field::new(
        slot_schema.name(),
        data_type.clone(),
        slot_schema.nullable(),
    )
}

fn projected_slot_schema_from_existing(
    existing: &ChunkSlotSchema,
    slot_id: SlotId,
    field: &Field,
) -> ChunkSlotSchema {
    ChunkSlotSchema::new(
        slot_id,
        field.name().to_string(),
        field.is_nullable(),
        existing.type_desc().cloned(),
        existing.unique_id(),
    )
}

fn synthetic_slot_schema(slot_id: SlotId, field: &Field) -> ChunkSlotSchema {
    ChunkSlotSchema::new(
        slot_id,
        field.name().to_string(),
        field.is_nullable(),
        None,
        None,
    )
}

/// Factory for projection processors that evaluate expression lists into projected chunks.
pub struct ProjectProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    exprs: Vec<ExprId>,
    expr_slot_ids: Vec<SlotId>,
    expr_slot_schemas: Option<Vec<ChunkSlotSchema>>,
    output_indices: Option<Vec<usize>>,
    output_slots: Vec<SlotId>,
    output_chunk_schema: Option<ChunkSchemaRef>,
}

impl ProjectProcessorFactory {
    pub fn new(
        node_id: i32,
        is_subordinate: bool,
        arena: Arc<ExprArena>,
        exprs: Vec<ExprId>,
        expr_slot_ids: Vec<SlotId>,
        expr_slot_schemas: Option<Vec<ChunkSlotSchema>>,
        output_indices: Option<Vec<usize>>,
        output_slots: Vec<SlotId>,
        output_chunk_schema: Option<ChunkSchemaRef>,
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
            expr_slot_schemas,
            output_indices,
            output_slots,
            output_chunk_schema,
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
            expr_slot_schemas: self
                .expr_slot_schemas
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|schema| (schema.slot_id(), schema))
                .collect(),
            output_indices: self.output_indices.clone(),
            output_slots: self.output_slots.clone(),
            output_chunk_schema: self.output_chunk_schema.clone(),
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
    expr_slot_schemas: HashMap<SlotId, ChunkSlotSchema>,
    output_indices: Option<Vec<usize>>,
    output_slots: Vec<SlotId>,
    output_chunk_schema: Option<ChunkSchemaRef>,
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
    fn declared_slot_schema(&self, slot_id: SlotId) -> Option<ChunkSlotSchema> {
        self.expr_slot_schemas.get(&slot_id).cloned()
    }

    fn output_slot_schema(
        &self,
        working_chunk: &Chunk,
        slot_id: SlotId,
        field: &Field,
    ) -> ChunkSlotSchema {
        self.output_chunk_schema
            .as_ref()
            .and_then(|schema| schema.slot(slot_id).cloned())
            .or_else(|| self.declared_slot_schema(slot_id))
            .or_else(|| working_chunk.chunk_schema().slot(slot_id).cloned())
            .map(|schema| projected_slot_schema_from_existing(&schema, slot_id, field))
            .unwrap_or_else(|| synthetic_slot_schema(slot_id, field))
    }

    fn build_chunk_schema(slot_schemas: Vec<ChunkSlotSchema>) -> Result<ChunkSchemaRef, String> {
        Ok(Arc::new(ChunkSchema::try_new(slot_schemas)?))
    }

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
                let data_type = computed_columns.last().unwrap().data_type();
                let preferred_slot_schema = self
                    .declared_slot_schema(*slot_id)
                    .or_else(|| working_chunk.chunk_schema().slot(*slot_id).cloned());
                let replaced = preferred_slot_schema
                    .as_ref()
                    .map(|schema| field_from_slot_schema(schema, data_type))
                    .unwrap_or_else(|| projected_field_from_existing(old_field, data_type));
                fields[existing_idx] = Arc::new(replaced.clone());
                let new_schema = Arc::new(Schema::new(fields));
                let mut slot_schemas = working_chunk.chunk_schema().slots().to_vec();
                slot_schemas[existing_idx] = preferred_slot_schema
                    .map(|schema| projected_slot_schema_from_existing(&schema, *slot_id, &replaced))
                    .unwrap_or_else(|| synthetic_slot_schema(*slot_id, &replaced));
                working_chunk = Chunk::try_new_with_schema_and_chunk_schema(
                    new_schema,
                    columns,
                    Self::build_chunk_schema(slot_schemas)?,
                )
                .map_err(|e| format!("Failed to replace chunk column: {}", e))?;
                continue;
            }

            let mut columns = working_chunk.batch.columns().to_vec();
            columns.push(array);

            // Create new schema with appended field
            let mut fields = working_chunk.batch.schema().fields().to_vec();
            let data_type = computed_columns.last().unwrap().data_type();
            let declared_slot_schema = self.declared_slot_schema(*slot_id);
            let field = if let Some(slot_schema) = declared_slot_schema.as_ref() {
                field_from_slot_schema(slot_schema, data_type)
            } else if let Some(ExprNode::SlotId(source_slot)) = self.arena.node(*expr_id) {
                if let Some(source_idx) = working_chunk.slot_id_to_index().get(source_slot) {
                    projected_field_from_existing(
                        working_chunk.batch.schema().field(*source_idx),
                        data_type,
                    )
                } else {
                    Field::new(
                        format!("_cse_{}", computed_columns.len() - 1),
                        data_type.clone(),
                        true,
                    )
                }
            } else {
                Field::new(
                    format!("_cse_{}", computed_columns.len() - 1),
                    data_type.clone(),
                    true,
                )
            };
            fields.push(Arc::new(field.clone()));
            let new_schema = Arc::new(Schema::new(fields));
            let slot_schema = declared_slot_schema
                .map(|schema| projected_slot_schema_from_existing(&schema, *slot_id, &field))
                .or_else(|| {
                    if let Some(ExprNode::SlotId(source_slot)) = self.arena.node(*expr_id) {
                        working_chunk
                            .chunk_schema()
                            .slot(*source_slot)
                            .map(|schema| {
                                projected_slot_schema_from_existing(schema, *slot_id, &field)
                            })
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| synthetic_slot_schema(*slot_id, &field));
            let mut slot_schemas = working_chunk.chunk_schema().slots().to_vec();
            slot_schemas.push(slot_schema);

            working_chunk = Chunk::try_new_with_schema_and_chunk_schema(
                new_schema,
                columns,
                Self::build_chunk_schema(slot_schemas)?,
            )
            .map_err(|e| format!("Failed to extend chunk: {}", e))?;
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
        let working_schema = working_chunk.batch.schema();
        for (idx, (array, slot_id)) in final_columns
            .iter()
            .zip(self.output_slots.iter())
            .enumerate()
        {
            let declared_output_slot_schema = self
                .output_chunk_schema
                .as_ref()
                .and_then(|schema| schema.slot(*slot_id).cloned())
                .or_else(|| self.declared_slot_schema(*slot_id));
            let runtime_nullable = array.null_count() > 0;
            let base = declared_output_slot_schema
                .as_ref()
                .map(|schema| {
                    Field::new(
                        schema.name(),
                        array.data_type().clone(),
                        schema.nullable() || runtime_nullable,
                    )
                })
                .or_else(|| {
                    working_chunk
                        .slot_id_to_index()
                        .get(slot_id)
                        .map(|field_idx| {
                            projected_field_from_existing(
                                working_schema.field(*field_idx),
                                array.data_type(),
                            )
                        })
                })
                .unwrap_or_else(|| {
                    Field::new(format!("col_{}", idx), array.data_type().clone(), true)
                });
            fields.push(Arc::new(base));
        }
        let output_slot_schemas = fields
            .iter()
            .zip(self.output_slots.iter())
            .map(|(field, slot_id)| {
                self.output_slot_schema(&working_chunk, *slot_id, field.as_ref())
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let output_chunk_schema = if let Some(explicit) = self.output_chunk_schema.as_ref() {
            let slot_schemas = schema
                .fields()
                .iter()
                .zip(self.output_slots.iter())
                .map(|(field, slot_id)| {
                    let slot_schema = explicit.slot(*slot_id).ok_or_else(|| {
                        format!(
                            "project explicit output chunk schema missing slot {}",
                            slot_id
                        )
                    })?;
                    ChunkSlotSchema::try_new(
                        *slot_id,
                        field.name(),
                        field.is_nullable(),
                        slot_schema.type_desc().cloned(),
                        slot_schema.unique_id(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Self::build_chunk_schema(slot_schemas)?
        } else {
            Self::build_chunk_schema(output_slot_schemas)?
        };

        Ok(Some(
            Chunk::try_new_with_schema_and_chunk_schema(schema, final_columns, output_chunk_schema)
                .map_err(|e| format!("Failed to create output batch: {}", e))?,
        ))
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
        let mut output_slot_schemas = Vec::with_capacity(selected_exprs.len());
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
            let declared_slot_schema = self
                .output_chunk_schema
                .as_ref()
                .and_then(|schema| schema.slot(*slot_id).cloned())
                .or_else(|| self.declared_slot_schema(*slot_id));
            let field = declared_slot_schema
                .as_ref()
                .map(|schema| field_from_slot_schema(schema, &data_type))
                .unwrap_or_else(|| Field::new(format!("col_{}", idx), data_type.clone(), true));
            output_slot_schemas.push(
                declared_slot_schema
                    .map(|schema| projected_slot_schema_from_existing(&schema, *slot_id, &field))
                    .unwrap_or_else(|| synthetic_slot_schema(*slot_id, &field)),
            );
            fields.push(Arc::new(field));
            columns.push(new_empty_array(&data_type));
        }

        let schema = Arc::new(Schema::new(fields));
        Chunk::try_new_with_schema_and_chunk_schema(
            schema,
            columns,
            self.output_chunk_schema
                .clone()
                .unwrap_or(Self::build_chunk_schema(output_slot_schemas)?),
        )
        .map_err(|e| format!("Failed to create empty output batch: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{BinaryArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::ProjectProcessorOperator;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::lower::type_lowering::scalar_type_desc;
    use crate::types::TPrimitiveType;

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
            expr_slot_schemas: HashMap::new(),
            output_indices: None,
            output_slots: vec![SlotId::new(17), SlotId::new(19)],
            output_chunk_schema: None,
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
            .process_one(Chunk::new_with_slot_ids(
                batch,
                &[SlotId::new(17), SlotId::new(18)],
            ))
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

    #[test]
    fn project_preserves_explicit_output_chunk_schema() {
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Binary);
        let output_slot = SlotId::new(2);
        let output_desc = scalar_type_desc(TPrimitiveType::HLL);
        let output_slot_schema =
            ChunkSlotSchema::new(output_slot, "out", false, Some(output_desc.clone()), None);
        let output_chunk_schema =
            Arc::new(ChunkSchema::try_new(vec![output_slot_schema.clone()]).expect("schema"));
        let mut op = ProjectProcessorOperator {
            name: "PROJECT".to_string(),
            arena: Arc::new(arena),
            exprs: vec![expr],
            expr_slot_ids: vec![output_slot],
            expr_slot_schemas: HashMap::from([(output_slot, output_slot_schema.clone())]),
            output_indices: None,
            output_slots: vec![output_slot],
            output_chunk_schema: Some(Arc::clone(&output_chunk_schema)),
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field_with_slot_id(
                Field::new("in", DataType::Binary, true),
                SlotId::new(1),
            )])),
            vec![Arc::new(BinaryArray::from(vec![Some(b"x".as_slice())]))],
        )
        .expect("input batch");
        let input_chunk = Chunk::new_with_chunk_schema(
            input_batch,
            Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new(
                    SlotId::new(1),
                    "in",
                    true,
                    None,
                    None,
                )])
                .expect("input schema"),
            ),
        );

        let output = op
            .process_one(input_chunk)
            .expect("project should succeed")
            .expect("project output");
        let slot = output
            .chunk_schema()
            .slot(output_slot)
            .expect("output slot schema");
        assert_eq!(slot.type_desc(), Some(&output_desc));
        assert_eq!(slot.primitive_type(), Some(TPrimitiveType::HLL));
        assert_eq!(output.batch.schema().field(0).name(), "out");
        assert!(!output.batch.schema().field(0).is_nullable());
    }
}
