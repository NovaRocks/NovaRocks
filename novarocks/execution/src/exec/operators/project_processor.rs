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

use crate::runtime::fragment::ExecutionResult;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, new_empty_array};
use arrow::datatypes::DataType;

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

fn projected_slot_schema(
    existing: &ChunkSlotSchema,
    slot_id: SlotId,
    data_type: &DataType,
    actual_nullable: bool,
) -> Result<ChunkSlotSchema, String> {
    let derived = if existing.metadata_origins().is_some()
        && crate::exec::chunk::type_compatibility::check_exact(existing.data_type(), data_type)
            .is_ok()
    {
        existing.reconcile_to_carrier(data_type, actual_nullable)?
    } else {
        existing
            .with_type_and_nullable(data_type.clone(), existing.nullable() || actual_nullable)?
    };
    derived.with_slot_id(slot_id)
}

fn synthetic_slot_schema(
    slot_id: SlotId,
    name: String,
    data_type: &DataType,
) -> Result<ChunkSlotSchema, String> {
    use novarocks_types::arrow_metadata_owner::{
        ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
    };
    let owner = ArrowMetadataOwner::try_new(
        Vec::new(),
        MetadataOwnerLimits {
            entries: 0,
            construction_bytes: 0,
        },
    )
    .map_err(|_| "project synthetic field metadata construction failed")?
    .into_field(name, data_type.clone(), true);
    let field = Arc::clone(owner.field());
    let origins = FieldMetadataOrigins::try_new(vec![owner], 1)
        .map_err(|_| "project synthetic metadata origin construction failed")?;
    if let Ok(scoped) = origins.for_field_tree(&field, 65536, 64) {
        ChunkSlotSchema::try_new_with_metadata_origins(slot_id, field, scoped, None, None)
    } else {
        ChunkSlotSchema::try_new_with_field_ref(slot_id, field, None, None)
    }
}

fn projected_chunk_schema(
    slots: Vec<ChunkSlotSchema>,
    declared: &ChunkSchema,
) -> Result<ChunkSchemaRef, String> {
    let schema = if let Some(source) = declared.schema_metadata_origin() {
        ChunkSchema::try_new_with_derived_schema_metadata(slots, source)?
    } else if let Some(source) = declared.metadata_materializations() {
        ChunkSchema::try_new_with_original_schema_metadata(slots, source)?
    } else {
        ChunkSchema::try_new_with_schema_metadata(
            slots,
            declared.arrow_schema_ref().metadata().clone(),
        )?
    };
    Ok(Arc::new(schema))
}

// Final result identity layouts retain both supported Native string carriers.
// Generic expression projects keep their existing dictionary peel contract.
fn is_final_result_string_dictionary(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Dictionary(key, value)
        if key.as_ref() == &DataType::Int32
            && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8))
}

fn cast_project_output_to_slot(
    array: ArrayRef,
    slot_schema: Option<&ChunkSlotSchema>,
    validate_final_result_input: bool,
) -> Result<ArrayRef, String> {
    let Some(slot_schema) = slot_schema else {
        return Ok(array);
    };
    if array.data_type() == slot_schema.data_type()
        || (slot_schema.data_type() == &DataType::Utf8
            && (is_supported_i32_string_dictionary(array.data_type())
                || (validate_final_result_input
                    && is_final_result_string_dictionary(array.data_type()))))
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
) -> Result<Vec<ChunkSlotSchema>, String> {
    slot_schemas
        .iter()
        .zip(columns.iter())
        .map(|(schema, array)| {
            schema.with_type_and_nullable(
                schema.data_type().clone(),
                schema.nullable() || array.null_count() > 0,
            )
        })
        .collect()
}

/// Factory for projection processors that evaluate expression lists into projected chunks.
pub(crate) fn validate_final_result_identity_input_source(
    chunk: &Chunk,
    output: &ChunkSchema,
    identity_layout: bool,
    pair_count: usize,
    pairs: impl Iterator<Item = Result<(SlotId, SlotId), String>>,
) -> Result<(), String> {
    use novarocks_type_contract::ValueLogicalType;
    use novarocks_types::logical::NR_LOGICAL_TYPE_KEY;

    fn marker(field: &arrow::datatypes::Field) -> Result<Option<ValueLogicalType>, String> {
        let Some(value) = field.metadata().get(NR_LOGICAL_TYPE_KEY) else {
            return Ok(None);
        };
        let logical =
            ValueLogicalType::from_legacy_compatible_metadata_value(value).map_err(|_| {
                "final result input contains an unknown logical domain marker".to_owned()
            })?;
        if matches!(
            logical,
            ValueLogicalType::LargeInt | ValueLogicalType::Variant | ValueLogicalType::Uuid
        ) {
            logical
                .validate_carrier(field.data_type())
                .map_err(|_| "final result input carrier differs from output carrier".to_owned())?;
        }
        Ok(Some(logical))
    }

    fn legacy_projection(domain: Option<ValueLogicalType>) -> Option<&'static str> {
        domain
            .filter(|domain| {
                !matches!(
                    domain,
                    ValueLogicalType::LargeInt | ValueLogicalType::Variant | ValueLogicalType::Uuid
                )
            })
            .and_then(ValueLogicalType::metadata_value)
    }

    fn check_field(
        source: &arrow::datatypes::Field,
        target: &arrow::datatypes::Field,
    ) -> Result<(), String> {
        let actual = marker(source)?;
        let expected = marker(target)?;
        if actual.is_some() && actual != expected {
            return Err("final result input logical domain differs from output domain".into());
        }
        check_type(source.data_type(), target.data_type())
    }

    fn check_type(source: &DataType, target: &DataType) -> Result<(), String> {
        use DataType as D;
        match (source, target) {
            (D::Dictionary(_, _), D::Utf8) if is_final_result_string_dictionary(source) => Ok(()),
            (D::List(source), D::List(target)) | (D::LargeList(source), D::LargeList(target)) => {
                check_field(source, target)
            }
            (D::FixedSizeList(source, sw), D::FixedSizeList(target, tw)) if sw == tw => {
                check_field(source, target)
            }
            (D::Map(source, so), D::Map(target, to)) if so == to => check_field(source, target),
            (D::Struct(source), D::Struct(target)) if source.len() == target.len() => {
                for (source, target) in source.iter().zip(target) {
                    check_field(source, target)?;
                }
                Ok(())
            }
            (
                D::List(_) | D::LargeList(_) | D::FixedSizeList(_, _) | D::Map(_, _) | D::Struct(_),
                _,
            )
            | (
                _,
                D::List(_) | D::LargeList(_) | D::FixedSizeList(_, _) | D::Map(_, _) | D::Struct(_),
            ) => Err("final result input carrier differs from output carrier".into()),
            // Native result descriptors do not admit these opaque recursive carriers.
            (D::Union(_, _) | D::RunEndEncoded(_, _), _)
            | (_, D::Union(_, _) | D::RunEndEncoded(_, _)) => {
                Err("final result input carrier differs from output carrier".into())
            }
            _ if source == target => Ok(()),
            _ => Err("final result input carrier differs from output carrier".into()),
        }
    }

    fn check_cached(
        source: &crate::exec::chunk::ChunkFieldSchema,
        target: &arrow::datatypes::Field,
        exact_projection: bool,
    ) -> Result<(), String> {
        let actual = source.logical_type();
        if (exact_projection || actual.is_some())
            && actual.map(|domain| domain.metadata_value()) != legacy_projection(marker(target)?)
        {
            return Err(if exact_projection {
                "final result output contains conflicting logical domain facts"
            } else {
                "final result input logical domain differs from output domain"
            }
            .into());
        }
        if source.children().is_empty() {
            return Ok(());
        }
        match target.data_type() {
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _)
                if source.children().len() == 1 =>
            {
                check_cached(&source.children()[0], child, exact_projection)
            }
            DataType::Struct(children) if children.len() == source.children().len() => {
                for (source, target) in source.children().iter().zip(children) {
                    check_cached(source, target, exact_projection)?;
                }
                Ok(())
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(children) = entries.data_type() else {
                    return Err(
                        "final result input cached field shape differs from output shape".into(),
                    );
                };
                if children.len() != 2 || source.children().len() != 2 {
                    return Err(
                        "final result input cached field shape differs from output shape".into(),
                    );
                }
                for (source, target) in source.children().iter().zip(children) {
                    check_cached(source, target, exact_projection)?;
                }
                Ok(())
            }
            _ => Err("final result input cached field shape differs from output shape".into()),
        }
    }

    if !identity_layout || pair_count != output.slots().len() {
        return Err("final result input validation requires an identity output layout".into());
    }
    let batch_schema = chunk.batch.schema();
    for (pair, target) in pairs.zip(output.slots()) {
        let (source_slot, output_slot) = pair?;
        if output_slot != target.slot_id() || source_slot != output_slot {
            return Err("final result input validation requires exact identity slots".into());
        }
        check_cached(target.field_schema(), target.field(), true)?;
        let index = chunk
            .slot_id_to_index()
            .get(&source_slot)
            .copied()
            .ok_or("final result input source slot is missing")?;
        let actual_field = batch_schema
            .fields()
            .get(index)
            .ok_or("final result input batch field is missing")?;
        let actual_slot = chunk
            .chunk_schema()
            .slot(source_slot)
            .ok_or("final result input slot field is missing")?;
        check_field(actual_field, target.field())?;
        check_field(actual_slot.field(), target.field())?;
        let array = chunk
            .columns()
            .get(index)
            .ok_or("final result input array is missing")?;
        check_type(array.data_type(), target.data_type())?;
        check_cached(actual_slot.field_schema(), target.field(), false)?;
    }
    Ok(())
}

pub(crate) fn materialize_project_output(
    final_columns: Vec<ArrayRef>,
    output_chunk_schema: &ChunkSchema,
    validate_final_result_input: bool,
) -> Result<Chunk, String> {
    // Infer schema from final columns
    if output_chunk_schema.slot_ids().len() != final_columns.len() {
        return Err(format!(
            "project output slots mismatch: slots={} columns={}",
            output_chunk_schema.slot_ids().len(),
            final_columns.len()
        ));
    }

    let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(final_columns.len());
    let mut slots = Vec::with_capacity(final_columns.len());
    for (array, slot_id) in final_columns.iter().zip(output_chunk_schema.slot_ids()) {
        let declared = output_chunk_schema
            .slot(*slot_id)
            .ok_or_else(|| format!("project output slot {slot_id} is missing"))?;
        let array = cast_project_output_to_slot(
            Arc::clone(array),
            Some(declared),
            validate_final_result_input,
        )?;
        let field_type = if declared.data_type() == &DataType::Utf8
            && (is_supported_i32_string_dictionary(array.data_type())
                || (validate_final_result_input
                    && is_final_result_string_dictionary(array.data_type())))
        {
            array.data_type()
        } else {
            declared.data_type()
        };
        slots.push(projected_slot_schema(
            declared,
            *slot_id,
            field_type,
            array.null_count() > 0,
        )?);
        output_columns.push(array);
    }
    let output_chunk_schema = projected_chunk_schema(slots, &output_chunk_schema)?;

    Chunk::try_new_with_columns(output_chunk_schema, output_columns)
        .map_err(|e| format!("Failed to create output batch: {}", e))
}

pub struct ProjectProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    exprs: Vec<ExprId>,
    expr_slot_ids: Vec<SlotId>,
    expr_slot_schemas: Option<Vec<ChunkSlotSchema>>,
    output_indices: Option<Vec<usize>>,
    output_chunk_schema: ChunkSchemaRef,
    validate_final_result_input: bool,
}

impl ProjectProcessorFactory {
    #[expect(
        clippy::too_many_arguments,
        reason = "The projection factory receives the complete lowered node contract."
    )]
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
            validate_final_result_input: false,
        }
    }

    /// Check the original input's logical domains before final identity retagging.
    /// Ordinary expression projections retain their existing behavior by default.
    pub fn with_final_result_input_validation(mut self, enabled: bool) -> Self {
        self.validate_final_result_input = enabled;
        self
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
            validate_final_result_input: self.validate_final_result_input,
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
    validate_final_result_input: bool,
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
        if self.validate_final_result_input {
            return is_final_result_string_dictionary(data_type)
                && self.exprs.iter().any(|expr_id| {
                    matches!(
                        self.arena.node(*expr_id),
                        Some(ExprNode::SlotId(source_slot)) if *source_slot == slot_id
                    )
                });
        }
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

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finished {
            return Ok(());
        }
        if self.pending_output.is_some() {
            return Err("project received input while output buffer is full"
                .to_string()
                .into());
        }
        let out = self.process_one(chunk)?;
        self.pending_output = out;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let out = self.pending_output.take();
        if self.finishing && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
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
        if self.validate_final_result_input {
            // Validation precedes evaluation and permits only exact identity expressions.
            // Preserve the original dictionary, including null dictionary values.
            let Some(ExprNode::SlotId(slot)) = self.arena.node(expr_id) else {
                return Err("final result input validation requires identity expressions".into());
            };
            return chunk
                .slot_id_to_index()
                .get(slot)
                .and_then(|index| chunk.columns().get(*index))
                .cloned()
                .ok_or_else(|| "final result input source slot is missing".into());
        }
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

    fn validate_final_result_identity_input(&self, chunk: &Chunk) -> Result<(), String> {
        validate_final_result_identity_input_source(
            chunk,
            &self.output_chunk_schema,
            self.output_indices.is_none() && self.exprs.len() == self.expr_slot_ids.len(),
            self.exprs.len(),
            self.exprs
                .iter()
                .zip(&self.expr_slot_ids)
                .map(|(expr, output)| match self.arena.node(*expr) {
                    Some(ExprNode::SlotId(source)) => Ok((*source, *output)),
                    _ => Err("final result input validation requires identity expressions".into()),
                }),
        )
    }

    fn process_one(&mut self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        if self.validate_final_result_input {
            self.validate_final_result_identity_input(&chunk)?;
        }
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

                let data_type = computed_columns.last().unwrap().data_type();
                let preferred_slot_schema = self
                    .declared_slot_schema(*slot_id)
                    .or_else(|| working_chunk.chunk_schema().slot(*slot_id).cloned());
                let replacement = if let Some(schema) = preferred_slot_schema {
                    projected_slot_schema(&schema, *slot_id, data_type, array_has_nulls)?
                } else {
                    synthetic_slot_schema(
                        *slot_id,
                        format!("_cse_{}", computed_columns.len() - 1),
                        data_type,
                    )?
                };
                let mut slot_schemas = slots_adjusted_for_actual_nullability(
                    working_chunk.chunk_schema().slots(),
                    &columns,
                )?;
                slot_schemas[existing_idx] = replacement;
                working_chunk =
                    Chunk::try_new_with_columns(Self::build_chunk_schema(slot_schemas)?, columns)
                        .map_err(|e| format!("Failed to replace chunk column: {}", e))?;
                continue;
            }

            let array_has_nulls = array.null_count() > 0;
            let mut columns = working_chunk.batch.columns().to_vec();
            columns.push(array);

            let data_type = computed_columns.last().unwrap().data_type();
            let source_schema = self.declared_slot_schema(*slot_id).or_else(|| {
                if let Some(ExprNode::SlotId(source_slot)) = self.arena.node(*expr_id) {
                    working_chunk.chunk_schema().slot(*source_slot).cloned()
                } else {
                    None
                }
            });
            let slot_schema = if let Some(schema) = source_schema {
                projected_slot_schema(&schema, *slot_id, data_type, array_has_nulls)?
            } else {
                synthetic_slot_schema(
                    *slot_id,
                    format!("_cse_{}", computed_columns.len() - 1),
                    data_type,
                )?
            };
            let mut slot_schemas = slots_adjusted_for_actual_nullability(
                working_chunk.chunk_schema().slots(),
                working_chunk.batch.columns(),
            )?;
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

        materialize_project_output(
            final_columns,
            &self.output_chunk_schema,
            self.validate_final_result_input,
        )
        .map(Some)
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

        let mut slots = Vec::with_capacity(selected_exprs.len());
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
            let slot = if let Some(schema) = declared_slot_schema {
                projected_slot_schema(&schema, *slot_id, &output_data_type, false)?
            } else {
                synthetic_slot_schema(*slot_id, format!("col_{idx}"), &output_data_type)?
            };
            slots.push(slot);
            columns.push(new_empty_array(&output_data_type));
        }

        let output_chunk_schema = projected_chunk_schema(slots, &self.output_chunk_schema)?;
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
    fn generic_project_type_overwrite_has_same_values_with_known_and_unknown_sources() {
        use novarocks_types::arrow_metadata_owner::{
            ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
        };
        let owner = ArrowMetadataOwner::try_new(
            vec![],
            MetadataOwnerLimits {
                entries: 0,
                construction_bytes: 0,
            },
        )
        .unwrap()
        .into_field("s17".into(), DataType::Int32, true);
        let known = ChunkSlotSchema::try_new_with_metadata_origins(
            SlotId::new(17),
            Arc::clone(owner.field()),
            FieldMetadataOrigins::try_new(vec![owner], 1).unwrap(),
            None,
            None,
        )
        .unwrap();
        let unknown = ChunkSlotSchema::try_new_with_field_ref(
            SlotId::new(17),
            Arc::clone(known.field_ref()),
            None,
            None,
        )
        .unwrap();
        for original in [known, unknown] {
            let mut arena = ExprArena::default();
            let write = arena.push_typed(ExprNode::SlotId(SlotId::new(18)), DataType::Int64);
            let read = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Int64);
            let schema = Arc::new(
                ChunkSchema::try_new(vec![
                    original,
                    ChunkSlotSchema::new_with_field(
                        SlotId::new(18),
                        Field::new("s18", DataType::Int64, true),
                        None,
                        None,
                    ),
                ])
                .unwrap(),
            );
            let input = Chunk::try_new_with_columns(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(Int64Array::from(vec![11, 12])),
                ],
            )
            .unwrap();
            let mut op = ProjectProcessorOperator {
                name: "PROJECT".into(),
                arena: Arc::new(arena),
                exprs: vec![write, read],
                expr_slot_ids: vec![SlotId::new(17), SlotId::new(19)],
                expr_slot_schemas: HashMap::new(),
                output_indices: None,
                output_chunk_schema: Arc::new(
                    ChunkSchema::try_new(vec![
                        ChunkSlotSchema::new_with_field(
                            SlotId::new(17),
                            Field::new("s17", DataType::Int64, true),
                            None,
                            None,
                        ),
                        ChunkSlotSchema::new_with_field(
                            SlotId::new(19),
                            Field::new("s19", DataType::Int64, true),
                            None,
                            None,
                        ),
                    ])
                    .unwrap(),
                ),
                validate_final_result_input: false,
                pending_output: None,
                finishing: false,
                finished: false,
            };
            let output = op.process_one(input).unwrap().unwrap();
            for column in output.columns() {
                let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(column.values().as_ref(), &[11, 12]);
            }
        }
    }

    #[test]
    fn synthetic_nested_project_source_stays_unknown_without_changing_values() {
        // A fresh root map does not prove a foreign nested child map.
        let mut map = HashMap::with_capacity(8192);
        map.insert("source".into(), "unknown".into());
        let child = Arc::new(Field::new("child", DataType::Int32, false).with_metadata(map));
        let data_type = DataType::Struct(vec![Arc::clone(&child)].into());
        let slot =
            super::synthetic_slot_schema(SlotId::new(1), "computed".into(), &data_type).unwrap();
        assert!(slot.metadata_origins().is_none());
        let array = Arc::new(arrow::array::StructArray::new(
            vec![Arc::clone(&child)].into(),
            vec![Arc::new(Int32Array::from(vec![7]))],
            None,
        )) as ArrayRef;
        let chunk = Chunk::try_new_with_columns(
            Arc::new(ChunkSchema::try_new(vec![slot]).unwrap()),
            vec![array],
        )
        .unwrap();
        assert!(chunk.chunk_schema().field_metadata_origins().is_none());
        let array = chunk.columns()[0]
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        assert_eq!(
            array
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            7
        );
        let DataType::Struct(fields) = array.data_type() else {
            panic!("struct")
        };
        assert!(Arc::ptr_eq(&child, &fields[0]));
    }

    #[test]
    fn project_runtime_keeps_exact_occurrence_metadata_and_derived_schema_owners() {
        use novarocks_types::arrow_metadata_owner::{
            ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
        };
        let limits = MetadataOwnerLimits {
            entries: 1,
            construction_bytes: 4096,
        };
        let field =
            ArrowMetadataOwner::try_new(vec![("nr_logical_type".into(), "JSON".into())], limits)
                .unwrap()
                .into_field("payload".into(), DataType::Utf8, true);
        let source_slot = ChunkSlotSchema::try_new_with_metadata_origins(
            SlotId::new(1),
            Arc::clone(field.field()),
            FieldMetadataOrigins::try_new(vec![field], 1).unwrap(),
            None,
            Some(17),
        )
        .unwrap();
        let compute = source_slot.with_slot_id(SlotId::new(3)).unwrap();
        let duplicate = source_slot.with_slot_id(SlotId::new(4)).unwrap();
        let top =
            ArrowMetadataOwner::try_new(vec![("schema".into(), "project".into())], limits).unwrap();
        let output_schema = Arc::new(
            ChunkSchema::try_new_with_owned_schema_metadata(vec![compute.clone(), duplicate], top)
                .unwrap(),
        );
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let mut op = ProjectProcessorOperator {
            name: "PROJECT".into(),
            arena: Arc::new(arena),
            exprs: vec![expr],
            expr_slot_ids: vec![SlotId::new(3)],
            expr_slot_schemas: HashMap::from([(SlotId::new(3), compute)]),
            output_indices: Some(vec![0, 0]),
            output_chunk_schema: Arc::clone(&output_schema),
            validate_final_result_input: false,
            pending_output: None,
            finishing: false,
            finished: false,
        };
        let input_schema = Arc::new(ChunkSchema::try_new(vec![source_slot.clone()]).unwrap());
        let input = Chunk::try_new_with_columns(
            input_schema,
            vec![Arc::new(StringArray::from(vec![Some("{}")]))],
        )
        .unwrap();
        let output = op.process_one(input).unwrap().unwrap();
        assert_eq!(
            output.chunk_schema().slot_ids(),
            &[SlotId::new(3), SlotId::new(4)]
        );
        for slot in output.chunk_schema().slots() {
            assert!(Arc::ptr_eq(slot.field_ref(), source_slot.field_ref()));
            assert_eq!(slot.unique_id(), Some(17));
            assert!(
                slot.metadata_origins()
                    .unwrap()
                    .metadata_bytes_for(slot.field_ref())
                    .is_some()
            );
        }
        let first = output.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let second = output.columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(first.values().data_ptr(), second.values().data_ptr());
        assert_eq!(
            output.schema().metadata(),
            output_schema.arrow_schema_ref().metadata()
        );
        assert!(
            output
                .chunk_schema()
                .schema_metadata_origin()
                .unwrap()
                .backing_bytes_for(&output.schema())
                .is_some()
        );
        let empty = op.empty_output_chunk().unwrap();
        assert_eq!(empty.len(), 0);
        for slot in empty.chunk_schema().slots() {
            assert!(Arc::ptr_eq(slot.field_ref(), source_slot.field_ref()));
            assert!(
                slot.metadata_origins()
                    .unwrap()
                    .metadata_bytes_for(slot.field_ref())
                    .is_some()
            );
        }
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
            validate_final_result_input: false,
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
            validate_final_result_input: false,
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
            validate_final_result_input: false,
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
            validate_final_result_input: false,
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
            validate_final_result_input: false,
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
            validate_final_result_input: false,
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
            validate_final_result_input: false,
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

    fn final_identity_operator(
        data_type: DataType,
        domain: LogicalType,
    ) -> ProjectProcessorOperator {
        final_identity_operator_with_field(field_with_logical_type(
            Field::new("result", data_type, true),
            domain,
        ))
    }

    fn final_identity_operator_with_field(field: Field) -> ProjectProcessorOperator {
        let slot = SlotId::new(1);
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(slot), field.data_type().clone());
        ProjectProcessorOperator {
            name: "FINAL RESULT".into(),
            arena: Arc::new(arena),
            exprs: vec![expr],
            expr_slot_ids: vec![slot],
            expr_slot_schemas: HashMap::new(),
            output_indices: None,
            output_chunk_schema: Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    slot, field, None, None,
                )])
                .unwrap(),
            ),
            validate_final_result_input: true,
            pending_output: None,
            finishing: false,
            finished: false,
        }
    }

    fn final_input_with_fields(
        batch_field: Field,
        slot_field: Field,
        cached: Option<crate::exec::chunk::ChunkFieldSchema>,
        array: ArrayRef,
    ) -> Chunk {
        let schema = ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(1),
            slot_field,
            cached,
            None,
        )])
        .unwrap();
        let mut chunk =
            Chunk::try_new_with_columns(Arc::new(schema), vec![Arc::clone(&array)]).unwrap();
        // Arrow requires a nested array's embedded Field facts to match the batch.
        // Retag this fixture's independent actual batch, preserving its payload buffers.
        let actual = arrow::array::make_array(
            array
                .to_data()
                .into_builder()
                .data_type(batch_field.data_type().clone())
                .build()
                .unwrap(),
        );
        chunk.batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![batch_field])), vec![actual]).unwrap();
        chunk
    }

    fn final_binary_input(
        batch_marker: Option<&str>,
        slot_marker: Option<&str>,
        cached: Option<LogicalType>,
        empty: bool,
    ) -> Chunk {
        let field = |marker: Option<&str>| {
            let field = Field::new("source", DataType::Binary, true);
            match marker {
                Some(marker) => field.with_metadata(
                    [(
                        novarocks_types::logical::NR_LOGICAL_TYPE_KEY.to_owned(),
                        marker.to_owned(),
                    )]
                    .into(),
                ),
                None => field,
            }
        };
        let slot = ChunkSlotSchema::new_with_field(
            SlotId::new(1),
            field(slot_marker),
            Some(crate::exec::chunk::ChunkFieldSchema::new(
                cached,
                Vec::new(),
            )),
            None,
        );
        let array: ArrayRef = if empty {
            Arc::new(BinaryArray::from(Vec::<&[u8]>::new()))
        } else {
            Arc::new(BinaryArray::from(vec![&b"unchanged state"[..]]))
        };
        let mut chunk = Chunk::try_new_with_columns(
            Arc::new(ChunkSchema::try_new(vec![slot]).unwrap()),
            vec![Arc::clone(&array)],
        )
        .unwrap();
        // Real inputs can carry independent batch, slot and cached facts.
        // Mutate only the public batch to keep those three oracles distinct.
        chunk.batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field(batch_marker)])),
            vec![array],
        )
        .unwrap();
        chunk
    }

    #[test]
    fn final_identity_complete_domains_keep_raw_facts_with_legacy_cache_projection() {
        use arrow::array::new_null_array;
        use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
        for (marker, carrier) in [
            ("json", DataType::Utf8),
            ("variant", DataType::LargeBinary),
            ("hll", DataType::Binary),
            ("bitmap", DataType::Binary),
            ("object", DataType::Binary),
            ("percentile", DataType::Binary),
            ("largeint", DataType::FixedSizeBinary(16)),
            ("uuid", DataType::FixedSizeBinary(16)),
        ] {
            let marked = Field::new("source", carrier.clone(), true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), marker.into())].into());
            for empty in [false, true] {
                let mut operator = final_identity_operator_with_field(marked.clone());
                let input = final_input_with_fields(
                    marked.clone(),
                    marked.clone(),
                    None,
                    new_null_array(&carrier, usize::from(!empty)),
                );
                assert!(
                    operator.process_one(input).is_ok(),
                    "{marker}, empty={empty}"
                );
                let plain = Field::new("source", carrier.clone(), true);
                let mut operator = final_identity_operator_with_field(plain);
                let input = final_input_with_fields(
                    marked.clone(),
                    marked.clone(),
                    None,
                    new_null_array(&carrier, usize::from(!empty)),
                );
                let error = operator.process_one(input).unwrap_err();
                assert!(
                    error.contains("logical domain differs"),
                    "{marker}: {error}"
                );
                assert!(operator.pending_output.is_none());
            }
        }
    }

    #[test]
    fn final_identity_complete_nested_domains_survive_each_original_container() {
        use arrow::array::new_null_array;
        use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
        fn nested(kind: usize, marker: &str) -> Field {
            let carrier = if marker == "variant" {
                DataType::LargeBinary
            } else {
                DataType::FixedSizeBinary(16)
            };
            let leaf = Arc::new(
                Field::new("value", carrier, true)
                    .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), marker.into())].into()),
            );
            let carrier = match kind {
                0 => DataType::Struct(vec![leaf].into()),
                1 => DataType::List(leaf),
                2 => DataType::LargeList(leaf),
                3 => DataType::FixedSizeList(leaf, 2),
                4 => DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![Arc::new(Field::new("key", DataType::Int32, false)), leaf].into(),
                        ),
                        false,
                    )),
                    false,
                ),
                _ => unreachable!(),
            };
            Field::new("source", carrier, true)
        }
        for kind in 0..5 {
            for marker in ["largeint", "uuid", "variant"] {
                let source = nested(kind, marker);
                let input = final_input_with_fields(
                    source.clone(),
                    source.clone(),
                    None,
                    new_null_array(source.data_type(), 1),
                );
                let mut operator = final_identity_operator_with_field(source);
                assert!(
                    operator.process_one(input).is_ok(),
                    "kind={kind}, marker={marker}"
                );
            }
            for (source_marker, target_marker) in [("largeint", "uuid"), ("uuid", "largeint")] {
                let source = nested(kind, source_marker);
                let input = final_input_with_fields(
                    source.clone(),
                    source.clone(),
                    None,
                    new_null_array(source.data_type(), 1),
                );
                let mut operator = final_identity_operator_with_field(nested(kind, target_marker));
                let error = operator.process_one(input).unwrap_err();
                assert!(
                    error.contains("logical domain differs"),
                    "kind={kind}: {error}"
                );
            }
        }
    }

    #[test]
    fn final_identity_rejects_only_target_nested_cache_conflict_before_materializing() {
        use arrow::array::new_null_array;
        use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
        let leaf = Arc::new(
            Field::new("value", DataType::FixedSizeBinary(16), true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "largeint".into())].into()),
        );
        let field = Field::new("source", DataType::Struct(vec![leaf].into()), true);
        let source = final_input_with_fields(
            field.clone(),
            field.clone(),
            None,
            new_null_array(field.data_type(), 1),
        );
        let bitmap = crate::exec::chunk::ChunkFieldSchema::from_field(&field_with_logical_type(
            Field::new("cached", DataType::Binary, true),
            LogicalType::Bitmap,
        ))
        .unwrap();
        let cache = crate::exec::chunk::ChunkFieldSchema::new(None, vec![bitmap]);
        let mut operator = final_identity_operator_with_field(field.clone());
        operator.output_chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                field,
                Some(cache),
                None,
            )])
            .unwrap(),
        );
        let error = operator.process_one(source).unwrap_err();
        assert!(
            error.contains("output contains conflicting logical domain facts"),
            "{error}"
        );
        assert!(operator.pending_output.is_none());
    }

    #[test]
    fn final_identity_complete_fixed_width_checks_batch_slot_and_cache_separately() {
        use arrow::array::new_null_array;
        use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
        let field = |marker: &str| {
            Field::new("source", DataType::FixedSizeBinary(16), true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), marker.into())].into())
        };
        for empty in [false, true] {
            for (batch, slot, cache) in [
                (field("uuid"), field("largeint"), None),
                (field("largeint"), field("uuid"), None),
                (
                    field("largeint"),
                    field("largeint"),
                    Some(LogicalType::Bitmap),
                ),
            ] {
                let cached = cache.map(|logical| {
                    crate::exec::chunk::ChunkFieldSchema::from_field(&field_with_logical_type(
                        Field::new("cached", DataType::Binary, true),
                        logical,
                    ))
                    .unwrap()
                });
                let input = final_input_with_fields(
                    batch,
                    slot,
                    cached,
                    new_null_array(&DataType::FixedSizeBinary(16), usize::from(!empty)),
                );
                let mut operator = final_identity_operator_with_field(field("largeint"));
                let error = operator.process_one(input).unwrap_err();
                assert!(error.contains("logical domain differs"), "{error}");
                assert!(operator.pending_output.is_none());
            }
        }
    }

    #[test]
    fn final_identity_rejects_actual_batch_slot_and_cache_conflicts_independently() {
        for (batch, slot, cached) in [
            (Some("hll"), None, None),
            (None, Some("hll"), None),
            (None, None, Some(LogicalType::Hll)),
            (Some("bitmap"), Some("bitmap"), Some(LogicalType::Hll)),
        ] {
            let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
            let error = op
                .process_one(final_binary_input(batch, slot, cached, false))
                .unwrap_err();
            assert!(error.contains("logical domain differs"), "{error}");
            assert!(op.pending_output.is_none());
        }
    }

    #[test]
    fn final_identity_rejects_unknown_batch_and_slot_markers_before_empty_output() {
        for empty in [false, true] {
            for marker in ["unknown", "", "   "] {
                for (batch, slot) in [(Some(marker), None), (None, Some(marker))] {
                    let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
                    let error = op
                        .process_one(final_binary_input(batch, slot, None, empty))
                        .unwrap_err();
                    assert!(error.contains("unknown logical domain"), "{error}");
                }
            }
            let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
            assert!(
                op.process_one(final_binary_input(Some("hll"), None, None, empty))
                    .is_err()
            );
        }
    }

    #[test]
    fn final_identity_fills_absent_and_preserves_equal_domains_without_payload_copy() {
        for (batch, slot, cached) in [
            (None, None, None),
            (Some("bitmap"), Some("bitmap"), Some(LogicalType::Bitmap)),
            (Some(" BiTmAp "), None, None),
        ] {
            let input = final_binary_input(batch, slot, cached, false);
            let original = input.columns()[0].to_data();
            let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
            let output = op.process_one(input).unwrap().unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(
                logical_type_of_field(output.batch.schema().field(0)),
                Some(LogicalType::Bitmap)
            );
            assert_eq!(
                output.chunk_schema().slots()[0]
                    .field_schema()
                    .logical_type(),
                Some(LogicalType::Bitmap)
            );
            assert_eq!(original, output.columns()[0].to_data());
            for (source, target) in original
                .buffers()
                .iter()
                .zip(output.columns()[0].to_data().buffers())
            {
                assert!(source.ptr_eq(target));
            }
        }
        let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
        let output = op
            .process_one(final_binary_input(None, None, None, true))
            .unwrap()
            .unwrap();
        assert!(output.is_empty());
        assert_eq!(
            logical_type_of_field(output.batch.schema().field(0)),
            Some(LogicalType::Bitmap)
        );
    }

    #[test]
    fn final_identity_keeps_supported_dictionary_carriers() {
        let array: ArrayRef = Arc::new(
            vec![Some("{}"), None, Some("[]"), Some("null")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let field = Field::new("source", array.data_type().clone(), true);
        let input = final_input_with_fields(field.clone(), field, None, array);
        let original = input.columns()[0].to_data();
        let mut op = final_identity_operator(DataType::Utf8, LogicalType::Json);
        let output = op.process_one(input).unwrap().unwrap();
        assert_eq!(output.len(), 4);
        assert_eq!(
            output.columns()[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(
            logical_type_of_field(output.batch.schema().field(0)),
            Some(LogicalType::Json)
        );
        assert_eq!(original, output.columns()[0].to_data());
        for (source, target) in original
            .buffers()
            .iter()
            .zip(output.columns()[0].to_data().buffers())
        {
            assert!(source.ptr_eq(target));
        }
    }

    #[test]
    fn final_identity_refuses_runtime_carrier_cast_before_evaluation() {
        let field = Field::new("source", DataType::Utf8, true);
        for array in [
            Arc::new(StringArray::from(vec!["state"])) as ArrayRef,
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
        ] {
            let input = final_input_with_fields(field.clone(), field.clone(), None, array);
            let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
            let error = op.process_one(input).unwrap_err();
            assert!(error.contains("carrier differs"), "{error}");
            assert!(op.pending_output.is_none());
        }
    }

    fn final_struct_field(marker: Option<&str>) -> Field {
        let child = Field::new("state", DataType::Binary, true);
        let child = match marker {
            Some(value) => child.with_metadata(
                [(
                    novarocks_types::logical::NR_LOGICAL_TYPE_KEY.to_owned(),
                    value.to_owned(),
                )]
                .into(),
            ),
            None => child,
        };
        Field::new(
            "record",
            DataType::Struct(vec![Arc::new(child)].into()),
            true,
        )
    }

    fn final_struct_array(field: &Field, empty: bool) -> ArrayRef {
        let DataType::Struct(children) = field.data_type() else {
            panic!("struct fixture");
        };
        let value: ArrayRef = if empty {
            Arc::new(BinaryArray::from(Vec::<&[u8]>::new()))
        } else {
            Arc::new(BinaryArray::from(vec![&b"state"[..]]))
        };
        Arc::new(arrow::array::StructArray::new(
            children.clone(),
            vec![value],
            None,
        ))
    }

    #[test]
    fn final_identity_checks_nested_batch_slot_and_cached_domains_independently() {
        use crate::exec::chunk::ChunkFieldSchema;
        let target = final_struct_field(Some("bitmap"));
        for empty in [false, true] {
            for marker in ["hll", "unknown", ""] {
                let plain = final_struct_field(None);
                let conflicting = final_struct_field(Some(marker));
                for (batch, slot) in [
                    (conflicting.clone(), plain.clone()),
                    (plain.clone(), conflicting.clone()),
                ] {
                    // Keep actual ArrayData plain; batch and slot Field facts are separate.
                    let input = final_input_with_fields(
                        batch,
                        slot,
                        Some(ChunkFieldSchema::empty()),
                        final_struct_array(&plain, empty),
                    );
                    let mut op = final_identity_operator_with_field(target.clone());
                    let error = op.process_one(input).unwrap_err();
                    assert!(error.contains("logical domain"), "{error}");
                }
            }
            let plain = final_struct_field(None);
            let cache = ChunkFieldSchema::new(
                None,
                vec![ChunkFieldSchema::new(Some(LogicalType::Hll), Vec::new())],
            );
            let input = final_input_with_fields(
                plain.clone(),
                plain.clone(),
                Some(cache),
                final_struct_array(&plain, empty),
            );
            let mut op = final_identity_operator_with_field(target.clone());
            assert!(
                op.process_one(input)
                    .unwrap_err()
                    .contains("logical domain differs")
            );

            let cache = ChunkFieldSchema::new(
                None,
                vec![ChunkFieldSchema::empty(), ChunkFieldSchema::empty()],
            );
            let input = final_input_with_fields(
                plain.clone(),
                plain.clone(),
                Some(cache),
                final_struct_array(&plain, empty),
            );
            let mut op = final_identity_operator_with_field(target.clone());
            assert!(
                op.process_one(input)
                    .unwrap_err()
                    .contains("cached field shape differs")
            );
        }
    }

    #[test]
    fn final_identity_supplements_absent_nested_metadata_and_keeps_same_metadata() {
        for empty in [false, true] {
            for marker in [None, Some("bitmap")] {
                let source = final_struct_field(marker);
                let input = final_input_with_fields(
                    source.clone(),
                    source.clone(),
                    None,
                    final_struct_array(&source, empty),
                );
                let mut op = final_identity_operator_with_field(final_struct_field(Some("bitmap")));
                let output = op.process_one(input).unwrap().unwrap();
                assert_eq!(output.len(), usize::from(!empty));
                let schema = output.batch.schema();
                let DataType::Struct(children) = schema.field(0).data_type() else {
                    panic!("struct output");
                };
                assert_eq!(
                    logical_type_of_field(&children[0]),
                    Some(LogicalType::Bitmap)
                );
                assert_eq!(
                    output.chunk_schema().slots()[0].field_schema().children()[0].logical_type(),
                    Some(LogicalType::Bitmap)
                );
                if !empty {
                    let value = output.columns()[0]
                        .as_any()
                        .downcast_ref::<arrow::array::StructArray>()
                        .unwrap()
                        .column(0)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap()
                        .value(0);
                    assert_eq!(value, b"state");
                }
            }
        }
    }

    #[test]
    fn final_identity_refuses_exact_units_decimal_width_and_container_shape_drift() {
        use arrow::datatypes::TimeUnit;
        let child = Arc::new(Field::new("item", DataType::Int32, true));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", DataType::Int32, true)),
                ]
                .into(),
            ),
            false,
        ));
        for (source, target) in [
            (
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
            (DataType::Decimal128(12, 2), DataType::Decimal128(12, 3)),
            (DataType::Decimal128(12, 2), DataType::Decimal256(12, 2)),
            (
                DataType::List(Arc::clone(&child)),
                DataType::LargeList(Arc::clone(&child)),
            ),
            (
                DataType::FixedSizeList(Arc::clone(&child), 1),
                DataType::FixedSizeList(Arc::clone(&child), 2),
            ),
            (
                DataType::Map(Arc::clone(&entries), false),
                DataType::Map(Arc::clone(&entries), true),
            ),
            (
                DataType::Struct(vec![Arc::clone(&child)].into()),
                DataType::Struct(vec![Arc::clone(&child), Arc::clone(&child)].into()),
            ),
        ] {
            let source_field = Field::new("source", source.clone(), true);
            let input = final_input_with_fields(
                source_field.clone(),
                source_field,
                None,
                arrow::array::new_empty_array(&source),
            );
            let mut op = final_identity_operator_with_field(Field::new("target", target, true));
            assert!(
                op.process_one(input)
                    .unwrap_err()
                    .contains("carrier differs")
            );
        }
    }

    #[test]
    fn final_identity_accepts_large_utf8_int32_dictionary_without_hydration() {
        let values = Arc::new(arrow::array::LargeStringArray::from(vec!["{}", "[]"])) as ArrayRef;
        let array = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(1), None, Some(0)]),
                values,
            )
            .unwrap(),
        ) as ArrayRef;
        let field = Field::new("source", array.data_type().clone(), true);
        let input = final_input_with_fields(field.clone(), field, None, Arc::clone(&array));
        let original = array.to_data();
        let mut op = final_identity_operator(DataType::Utf8, LogicalType::Json);
        assert!(op.accepts_encoded_column(SlotId::new(1), array.data_type()));
        op.validate_final_result_input = false;
        assert!(!op.accepts_encoded_column(SlotId::new(1), array.data_type()));
        op.validate_final_result_input = true;
        let output = op.process_one(input).unwrap().unwrap();
        assert_eq!(output.columns()[0].data_type(), array.data_type());
        assert_eq!(original, output.columns()[0].to_data());
        assert_eq!(
            logical_type_of_field(output.batch.schema().field(0)),
            Some(LogicalType::Json)
        );
        assert_eq!(output.columns()[0].null_count(), 1);
        let actual = output.columns()[0].to_data();
        for (source, target) in original.child_data()[0]
            .buffers()
            .iter()
            .zip(actual.child_data()[0].buffers())
        {
            assert!(source.ptr_eq(target));
        }
        for (source, target) in original
            .buffers()
            .iter()
            .zip(output.columns()[0].to_data().buffers())
        {
            assert!(source.ptr_eq(target));
        }
    }

    #[test]
    fn final_identity_refuses_nonidentity_or_indexed_layout_but_default_project_is_unchanged() {
        let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
        op.output_indices = Some(vec![0]);
        assert!(
            op.process_one(final_binary_input(None, None, None, false))
                .unwrap_err()
                .contains("identity output layout")
        );
        let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
        let mut arena = ExprArena::default();
        op.exprs[0] = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Binary);
        op.arena = Arc::new(arena);
        assert!(
            op.process_one(final_binary_input(None, None, None, false))
                .unwrap_err()
                .contains("identity expressions")
        );

        // The opt-in does not impose final result semantics on generic projects.
        let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
        op.validate_final_result_input = false;
        assert!(
            op.process_one(final_binary_input(Some("hll"), None, None, false))
                .is_ok()
        );

        use crate::exec::pipeline::operator_factory::OperatorFactory;
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Binary);
        let schema =
            final_identity_operator(DataType::Binary, LogicalType::Bitmap).output_chunk_schema;
        let factory = super::ProjectProcessorFactory::new(
            37,
            true,
            Arc::new(arena),
            vec![expr],
            vec![SlotId::new(1)],
            None,
            None,
            schema,
        );
        assert!(!factory.validate_final_result_input);
        let mut operator = factory
            .with_final_result_input_validation(true)
            .create(1, 0);
        let error = operator
            .as_processor_mut()
            .unwrap()
            .push_chunk(
                &crate::runtime::runtime_state::RuntimeState::default(),
                final_binary_input(Some("hll"), None, None, false),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("logical domain differs"),
            "{error}"
        );
    }
}
