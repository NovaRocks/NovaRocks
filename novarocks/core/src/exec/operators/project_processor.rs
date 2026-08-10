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
use arrow::datatypes::{DataType, Field};

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::expr::dict_peel::{
    expr_can_peel_from_slot, is_supported_i32_string_dictionary, referenced_slots,
    try_peel_dict_expr,
};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, cast_array_to_target};
use novarocks_types::SlotId;

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

fn projected_field_from_existing(
    existing: &Field,
    data_type: &arrow::datatypes::DataType,
) -> Field {
    Field::new(existing.name(), data_type.clone(), existing.is_nullable())
        .with_metadata(existing.metadata().clone())
}

fn field_from_slot_schema(slot_schema: &ChunkSlotSchema, data_type: &DataType) -> Field {
    Field::new(
        slot_schema.name(),
        data_type.clone(),
        slot_schema.nullable(),
    )
    .with_metadata(slot_schema.field().metadata().clone())
}

fn with_nullable_preserving_metadata(field: &Field, nullable: bool) -> Field {
    Field::new(field.name(), field.data_type().clone(), nullable)
        .with_metadata(field.metadata().clone())
}

fn projected_slot_schema_from_existing(
    existing: &ChunkSlotSchema,
    slot_id: SlotId,
    field: &Field,
) -> ChunkSlotSchema {
    existing
        .with_field_and_slot_id(slot_id, field.clone())
        .unwrap_or_else(|e| panic!("{e}"))
}

fn synthetic_slot_schema(slot_id: SlotId, field: &Field) -> ChunkSlotSchema {
    ChunkSlotSchema::new_with_field(slot_id, field.clone(), None, None)
}

fn cast_project_output_to_slot(
    array: ArrayRef,
    slot_schema: Option<&ChunkSlotSchema>,
) -> Result<ArrayRef, String> {
    let Some(slot_schema) = slot_schema else {
        return Ok(array);
    };
    if array.data_type() == slot_schema.data_type()
        || (slot_schema.data_type() == &DataType::Utf8
            && is_supported_i32_string_dictionary(array.data_type()))
    {
        return Ok(array);
    }
    cast_array_to_target(&array, slot_schema.data_type()).map_err(|e| {
        format!(
            "project output slot {} (`{}`) cast from {:?} to {:?} failed: {}",
            slot_schema.slot_id(),
            slot_schema.name(),
            array.data_type(),
            slot_schema.data_type(),
            e
        )
    })
}

/// Upgrade slot schemas to nullable where the actual array data contains null values.
///
/// The FE may declare a column as non-nullable (e.g., key columns in duplicate-key tables)
/// but still send rows with null values (e.g., when a cast overflows). The scan operator
/// tolerates this via `validate_chunk_schema_against_batch`, allowing nullable arrays to
/// satisfy a non-nullable schema contract. However, Arrow's `RecordBatch::try_new` validates
/// strictly: a non-nullable field cannot have an array with null_count > 0. This function
/// upgrades affected slot schemas so that the rebuilt RecordBatch passes Arrow validation.
fn slots_adjusted_for_actual_nullability(
    slot_schemas: &[ChunkSlotSchema],
    columns: &[ArrayRef],
) -> Vec<ChunkSlotSchema> {
    slot_schemas
        .iter()
        .zip(columns.iter())
        .map(|(schema, array)| {
            if !schema.nullable() && array.null_count() > 0 {
                let nullable_field = with_nullable_preserving_metadata(schema.field(), true);
                schema
                    .with_field(nullable_field)
                    .unwrap_or_else(|_| schema.clone())
            } else {
                schema.clone()
            }
        })
        .collect()
}

/// Factory for projection processors that evaluate expression lists into projected chunks.
pub struct ProjectProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    exprs: Vec<ExprId>,
    expr_slot_ids: Vec<SlotId>,
    expr_slot_schemas: Option<Vec<ChunkSlotSchema>>,
    output_indices: Option<Vec<usize>>,
    output_chunk_schema: ChunkSchemaRef,
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
        output_chunk_schema: ChunkSchemaRef,
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
            output_chunk_schema: Arc::clone(&self.output_chunk_schema),
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
    output_chunk_schema: ChunkSchemaRef,
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
    fn accepts_encoded_column(&self, slot_id: SlotId, data_type: &DataType) -> bool {
        is_supported_i32_string_dictionary(data_type)
            && self.exprs.iter().any(|expr_id| {
                matches!(
                    self.arena.node(*expr_id),
                    Some(ExprNode::SlotId(source_slot)) if *source_slot == slot_id
                ) || expr_can_peel_from_slot(&self.arena, *expr_id, slot_id)
            })
    }

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

    fn build_chunk_schema(slot_schemas: Vec<ChunkSlotSchema>) -> Result<ChunkSchemaRef, String> {
        Ok(Arc::new(ChunkSchema::try_new(slot_schemas)?))
    }

    fn eval_project_expr(&self, expr_id: ExprId, chunk: &Chunk) -> Result<ArrayRef, String> {
        if let Some(array) = try_peel_dict_expr(&self.arena, expr_id, chunk)? {
            return Ok(array);
        }
        if self.expr_references_dictionary_slot(expr_id, chunk) {
            let referenced = referenced_slots(&self.arena, expr_id).unwrap_or_default();
            let hydrated =
                crate::exec::chunk::hydrate_dictionary_columns_except(chunk, |slot, _| {
                    !referenced.contains(&slot)
                })?;
            return self
                .arena
                .eval(expr_id, &hydrated)
                .map_err(|e| e.to_string());
        }
        self.arena.eval(expr_id, chunk).map_err(|e| e.to_string())
    }

    fn expr_references_dictionary_slot(&self, expr_id: ExprId, chunk: &Chunk) -> bool {
        let Some(referenced) = referenced_slots(&self.arena, expr_id) else {
            return false;
        };
        referenced.iter().any(|slot| {
            chunk
                .slot_id_to_index()
                .get(slot)
                .and_then(|idx| chunk.columns().get(*idx))
                .is_some_and(|column| is_supported_i32_string_dictionary(column.data_type()))
        })
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
            let array = self.eval_project_expr(*expr_id, &working_chunk)?;

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
                let array_has_nulls = array.null_count() > 0;
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
                    .map(|schema| {
                        let f = field_from_slot_schema(schema, data_type);
                        if array_has_nulls && !f.is_nullable() {
                            with_nullable_preserving_metadata(&f, true)
                        } else {
                            f
                        }
                    })
                    .unwrap_or_else(|| projected_field_from_existing(old_field, data_type));
                fields[existing_idx] = Arc::new(replaced.clone());
                let mut slot_schemas = slots_adjusted_for_actual_nullability(
                    working_chunk.chunk_schema().slots(),
                    &columns,
                );
                slot_schemas[existing_idx] = preferred_slot_schema
                    .map(|schema| projected_slot_schema_from_existing(&schema, *slot_id, &replaced))
                    .unwrap_or_else(|| synthetic_slot_schema(*slot_id, &replaced));
                working_chunk =
                    Chunk::try_new_with_columns(Self::build_chunk_schema(slot_schemas)?, columns)
                        .map_err(|e| format!("Failed to replace chunk column: {}", e))?;
                continue;
            }

            let array_has_nulls = array.null_count() > 0;
            let mut columns = working_chunk.batch.columns().to_vec();
            columns.push(array);

            // Create new schema with appended field
            let mut fields = working_chunk.batch.schema().fields().to_vec();
            let data_type = computed_columns.last().unwrap().data_type();
            let declared_slot_schema = self.declared_slot_schema(*slot_id);
            let field = if let Some(slot_schema) = declared_slot_schema.as_ref() {
                let f = field_from_slot_schema(slot_schema, data_type);
                if array_has_nulls && !f.is_nullable() {
                    with_nullable_preserving_metadata(&f, true)
                } else {
                    f
                }
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
            let mut slot_schemas = slots_adjusted_for_actual_nullability(
                working_chunk.chunk_schema().slots(),
                working_chunk.batch.columns(),
            );
            slot_schemas.push(slot_schema);

            working_chunk =
                Chunk::try_new_with_columns(Self::build_chunk_schema(slot_schemas)?, columns)
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
        if self.output_chunk_schema.slot_ids().len() != final_columns.len() {
            return Err(format!(
                "project output slots mismatch: slots={} columns={}",
                self.output_chunk_schema.slot_ids().len(),
                final_columns.len()
            ));
        }

        let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(final_columns.len());
        let mut fields: Vec<Field> = Vec::with_capacity(final_columns.len());
        let working_schema = working_chunk.batch.schema();
        for (idx, (array, slot_id)) in final_columns
            .iter()
            .zip(self.output_chunk_schema.slot_ids().iter())
            .enumerate()
        {
            let declared_output_slot_schema = self
                .output_chunk_schema
                .slot(*slot_id)
                .cloned()
                .or_else(|| self.declared_slot_schema(*slot_id));
            let array =
                cast_project_output_to_slot(array.clone(), declared_output_slot_schema.as_ref())?;
            let runtime_nullable = array.null_count() > 0;
            let base = declared_output_slot_schema
                .as_ref()
                .map(|schema| {
                    let field_data_type = if schema.data_type() == &DataType::Utf8
                        && is_supported_i32_string_dictionary(array.data_type())
                    {
                        array.data_type()
                    } else {
                        schema.data_type()
                    };
                    let field = field_from_slot_schema(schema, field_data_type);
                    if runtime_nullable && !field.is_nullable() {
                        with_nullable_preserving_metadata(&field, true)
                    } else {
                        field
                    }
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
            fields.push(base);
            output_columns.push(array);
        }
        let output_chunk_schema = Arc::new(self.output_chunk_schema.with_fields_in_order(fields)?);

        Ok(Some(
            Chunk::try_new_with_columns(output_chunk_schema, output_columns)
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

        if self.output_chunk_schema.slot_ids().len() != selected_exprs.len() {
            return Err(format!(
                "project output slots mismatch on empty input: slots={} exprs={}",
                self.output_chunk_schema.slot_ids().len(),
                selected_exprs.len()
            ));
        }

        let mut fields: Vec<Field> = Vec::with_capacity(selected_exprs.len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(selected_exprs.len());
        for (idx, (expr_id, slot_id)) in selected_exprs
            .iter()
            .zip(self.output_chunk_schema.slot_ids().iter())
            .enumerate()
        {
            let data_type = self
                .arena
                .data_type(*expr_id)
                .ok_or_else(|| format!("project expr {} type missing on empty input", idx))?
                .clone();
            let declared_slot_schema = self
                .output_chunk_schema
                .slot(*slot_id)
                .cloned()
                .or_else(|| self.declared_slot_schema(*slot_id));
            let output_data_type = declared_slot_schema
                .as_ref()
                .map(|schema| schema.data_type().clone())
                .unwrap_or(data_type);
            let field = declared_slot_schema
                .as_ref()
                .map(|schema| field_from_slot_schema(schema, &output_data_type))
                .unwrap_or_else(|| {
                    Field::new(format!("col_{}", idx), output_data_type.clone(), true)
                });
            fields.push(field);
            columns.push(new_empty_array(&output_data_type));
        }

        let output_chunk_schema = Arc::new(self.output_chunk_schema.with_fields_in_order(fields)?);
        Chunk::try_new_with_columns(output_chunk_schema, columns)
            .map_err(|e| format!("Failed to create empty output batch: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryArray, DictionaryArray, Int32Array, Int64Array, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;

    use super::ProjectProcessorOperator;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::LiteralValue;
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::pipeline::operator::ProcessorOperator;
    use novarocks_types::SlotId;
    use novarocks_types::logical::{LogicalType, field_with_logical_type, logical_type_of_field};

    fn chunk_schema_of(schema: &Arc<Schema>, slot_ids: &[SlotId]) -> Arc<ChunkSchema> {
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)
            .expect("chunk schema")
    }

    fn dict_string_chunk(slot_id: SlotId) -> Chunk {
        let column: ArrayRef = Arc::new(
            vec![Some("PAID"), None, Some("New"), Some(" shipped ")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let slot = ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("status", column.data_type().clone(), true),
            None,
            None,
        );
        Chunk::try_new_with_columns(
            Arc::new(ChunkSchema::try_new(vec![slot]).unwrap()),
            vec![column],
        )
        .unwrap()
    }

    fn utf8_output_schema(slot_id: SlotId, name: &str) -> Arc<ChunkSchema> {
        Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                slot_id,
                Field::new(name, DataType::Utf8, true),
                None,
                None,
            )])
            .expect("output schema"),
        )
    }

    #[test]
    fn project_accepts_encoded_column_for_peelable_lower_expression() {
        let input_slot = SlotId::new(1);
        let output_slot = SlotId::new(2);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(input_slot), DataType::Utf8);
        let lower = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("lower"),
                args: vec![source],
            },
            DataType::Utf8,
        );
        let op = ProjectProcessorOperator {
            name: "PROJECT".to_string(),
            arena: Arc::new(arena),
            exprs: vec![lower],
            expr_slot_ids: vec![output_slot],
            expr_slot_schemas: HashMap::new(),
            output_indices: None,
            output_chunk_schema: utf8_output_schema(output_slot, "lower_status"),
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert!(op.accepts_encoded_column(input_slot, &dictionary_type));
        assert!(!op.accepts_encoded_column(input_slot, &DataType::Utf8));
    }

    #[test]
    fn project_peels_lower_dictionary_output_without_flattening_final_slot() {
        let input_slot = SlotId::new(3);
        let output_slot = SlotId::new(4);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(input_slot), DataType::Utf8);
        let lower = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("lower"),
                args: vec![source],
            },
            DataType::Utf8,
        );
        let mut op = ProjectProcessorOperator {
            name: "PROJECT".to_string(),
            arena: Arc::new(arena),
            exprs: vec![lower],
            expr_slot_ids: vec![output_slot],
            expr_slot_schemas: HashMap::new(),
            output_indices: None,
            output_chunk_schema: utf8_output_schema(output_slot, "lower_status"),
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let output = op
            .process_one(dict_string_chunk(input_slot))
            .expect("project should succeed")
            .expect("project output");
        let column = output.batch.column(0);
        assert_eq!(
            column.data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        let dict = column
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("dictionary output");
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string values");
        assert_eq!(values.value(0), "paid");
        assert_eq!(values.value(1), "new");
        assert_eq!(values.value(2), " shipped ");
        assert!(dict.is_null(1));
    }

    #[test]
    fn project_locally_hydrates_unsafe_mixed_expression_when_slot_is_kept_for_peeling() {
        let input_slot = SlotId::new(5);
        let lower_slot = SlotId::new(6);
        let coalesce_slot = SlotId::new(7);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(input_slot), DataType::Utf8);
        let lower = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("lower"),
                args: vec![source],
            },
            DataType::Utf8,
        );
        let fallback = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("missing".to_string())),
            DataType::Utf8,
        );
        let coalesce = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Coalesce,
                args: vec![source, fallback],
            },
            DataType::Utf8,
        );
        let output_chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::new_with_field(
                    lower_slot,
                    Field::new("lower_status", DataType::Utf8, true),
                    None,
                    None,
                ),
                ChunkSlotSchema::new_with_field(
                    coalesce_slot,
                    Field::new("status_or_missing", DataType::Utf8, true),
                    None,
                    None,
                ),
            ])
            .expect("output schema"),
        );
        let mut op = ProjectProcessorOperator {
            name: "PROJECT".to_string(),
            arena: Arc::new(arena),
            exprs: vec![lower, coalesce],
            expr_slot_ids: vec![lower_slot, coalesce_slot],
            expr_slot_schemas: HashMap::new(),
            output_indices: None,
            output_chunk_schema,
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let output = op
            .process_one(dict_string_chunk(input_slot))
            .expect("project should succeed")
            .expect("project output");
        assert_eq!(
            output.batch.column(0).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(output.batch.column(1).data_type(), &DataType::Utf8);
        let status_or_missing = output
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("flat utf8 output");
        assert_eq!(status_or_missing.value(0), "PAID");
        assert_eq!(status_or_missing.value(1), "missing");
        assert_eq!(status_or_missing.value(2), "New");
    }

    #[test]
    fn project_reuses_overwritten_dictionary_carrier_for_follow_up_expr() {
        let input_slot = SlotId::new(8);
        let output_slot = SlotId::new(9);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(input_slot), DataType::Utf8);
        let lower = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("lower"),
                args: vec![source],
            },
            DataType::Utf8,
        );
        let read_overwritten = arena.push_typed(ExprNode::SlotId(input_slot), DataType::Utf8);
        let output_chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::new_with_field(
                    input_slot,
                    Field::new("status", DataType::Utf8, true),
                    None,
                    None,
                ),
                ChunkSlotSchema::new_with_field(
                    output_slot,
                    Field::new("status_copy", DataType::Utf8, true),
                    None,
                    None,
                ),
            ])
            .expect("output schema"),
        );
        let mut op = ProjectProcessorOperator {
            name: "PROJECT".to_string(),
            arena: Arc::new(arena),
            exprs: vec![lower, read_overwritten],
            expr_slot_ids: vec![input_slot, output_slot],
            expr_slot_schemas: HashMap::new(),
            output_indices: None,
            output_chunk_schema,
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let output = op
            .process_one(dict_string_chunk(input_slot))
            .expect("project should succeed")
            .expect("project output");
        let lower_dict = output
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("lower dictionary output");
        let copy_dict = output
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("follow-up dictionary output");
        assert_eq!(copy_dict.keys(), lower_dict.keys());
        let lower_values = lower_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("lower values");
        let copy_values = copy_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("copy values");
        assert_eq!(copy_values, lower_values);
        assert_eq!(copy_values.value(0), "paid");
        assert_eq!(copy_values.value(1), "new");
        assert_eq!(copy_values.value(2), " shipped ");
        assert!(copy_dict.is_null(1));
    }

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
            output_chunk_schema: Arc::new(
                ChunkSchema::try_new(vec![
                    ChunkSlotSchema::new_with_field(
                        SlotId::new(17),
                        Field::new("s17", DataType::Int32, true),
                        None,
                        None,
                    ),
                    ChunkSlotSchema::new_with_field(
                        SlotId::new(19),
                        Field::new("s19", DataType::Int32, true),
                        None,
                        None,
                    ),
                ])
                .expect("output chunk schema"),
            ),
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("s17", DataType::Int32, true),
                Field::new("s18", DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![11, 12])),
            ],
        )
        .expect("build input batch");
        let output = op
            .process_one({
                let batch = batch;
                let chunk_schema =
                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                        batch.schema().as_ref(),
                        &[SlotId::new(17), SlotId::new(18)],
                    )
                    .expect("chunk schema");
                Chunk::new_with_chunk_schema(batch, chunk_schema)
            })
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
        let output_slot_schema = ChunkSlotSchema::new_with_field(
            output_slot,
            field_with_logical_type(Field::new("out", DataType::Binary, false), LogicalType::Hll),
            None,
            None,
        );
        let output_chunk_schema =
            Arc::new(ChunkSchema::try_new(vec![output_slot_schema.clone()]).expect("schema"));
        let mut op = ProjectProcessorOperator {
            name: "PROJECT".to_string(),
            arena: Arc::new(arena),
            exprs: vec![expr],
            expr_slot_ids: vec![output_slot],
            expr_slot_schemas: HashMap::from([(output_slot, output_slot_schema.clone())]),
            output_indices: None,
            output_chunk_schema: Arc::clone(&output_chunk_schema),
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("in", DataType::Binary, true)])),
            vec![Arc::new(BinaryArray::from(vec![Some(b"x".as_slice())]))],
        )
        .expect("input batch");
        let input_chunk = Chunk::new_with_chunk_schema(
            input_batch,
            chunk_schema_of(
                &Arc::new(Schema::new(vec![Field::new("in", DataType::Binary, true)])),
                &[SlotId::new(1)],
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
        assert_eq!(slot.data_type(), &DataType::Binary);
        assert_eq!(slot.field_schema().logical_type(), Some(LogicalType::Hll));
        assert_eq!(logical_type_of_field(slot.field()), Some(LogicalType::Hll));
        assert_eq!(output.batch.schema().field(0).name(), "out");
        assert!(!output.batch.schema().field(0).is_nullable());
    }

    #[test]
    fn project_casts_final_output_to_declared_slot_type() {
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let output_slot = SlotId::new(2);
        let output_chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                output_slot,
                Field::new("out", DataType::Int32, false),
                None,
                None,
            )])
            .expect("schema"),
        );
        let mut op = ProjectProcessorOperator {
            name: "PROJECT".to_string(),
            arena: Arc::new(arena),
            exprs: vec![expr],
            expr_slot_ids: vec![output_slot],
            expr_slot_schemas: HashMap::new(),
            output_indices: None,
            output_chunk_schema: Arc::clone(&output_chunk_schema),
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let input_schema = Arc::new(Schema::new(vec![Field::new("in", DataType::Int64, false)]));
        let input_batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2_i64]))],
        )
        .expect("input batch");
        let input_chunk = Chunk::new_with_chunk_schema(
            input_batch,
            chunk_schema_of(&input_schema, &[SlotId::new(1)]),
        );

        let output = op
            .process_one(input_chunk)
            .expect("project should succeed")
            .expect("project output");
        assert_eq!(output.batch.schema().field(0).data_type(), &DataType::Int32);
        let values = output
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("output column");
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
    }
}
