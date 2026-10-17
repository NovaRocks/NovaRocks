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
#[cfg(test)]
use crate::exec::expr::cast_array_to_target;
use crate::exec::expr::dict_peel::{
    expr_can_peel_from_slot, expression_uses_slot, is_supported_i32_string_dictionary,
    try_peel_dict_expr_owned,
};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks_types::SlotId;

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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
#[cfg(test)]
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
        let value = value.trim();
        for domain in [
            ValueLogicalType::Json,
            ValueLogicalType::Hll,
            ValueLogicalType::Bitmap,
            ValueLogicalType::Object,
            ValueLogicalType::Percentile,
        ] {
            if value.eq_ignore_ascii_case(domain.metadata_value().expect("nominal domain")) {
                return Ok(Some(domain));
            }
        }
        Err("final result input contains an unknown logical domain marker".into())
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
            (D::Dictionary(_, values), D::Utf8 | D::LargeUtf8)
                if is_final_result_string_dictionary(source) && values.as_ref() == target =>
            {
                Ok(())
            }
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
        source: &novarocks_type_contract::ValueTypeRef,
        target: &arrow::datatypes::Field,
    ) -> Result<(), String> {
        let actual = source.domain();
        if actual.is_some() && actual != marker(target)? {
            return Err("final result input logical domain differs from output domain".into());
        }
        if source.child_count() == 0 {
            return Ok(());
        }
        let children: &[std::sync::Arc<arrow::datatypes::Field>] = match target.data_type() {
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _)
                if source.child_count() == 1 =>
            {
                return check_cached(source.child_type_at(0).expect("checked child"), child);
            }
            DataType::Struct(children) if children.len() == source.child_count() => children,
            DataType::Map(entries, _) => {
                let DataType::Struct(children) = entries.data_type() else {
                    return Err(
                        "final result input semantic shape differs from output shape".into(),
                    );
                };
                if children.len() != 2 || source.child_count() != 2 {
                    return Err(
                        "final result input semantic shape differs from output shape".into(),
                    );
                }
                children
            }
            _ => return Err("final result input semantic shape differs from output shape".into()),
        };
        for (index, target) in children.iter().enumerate() {
            check_cached(source.child_type_at(index).expect("checked child"), target)?;
        }
        Ok(())
    }

    if !identity_layout || pair_count != output.slots().len() {
        return Err("final result input validation requires an identity output layout".into());
    }
    let batch_schema = chunk.record_batch().schema();
    for (pair, target) in pairs.zip(output.slots()) {
        let (source_slot, output_slot) = pair?;
        if output_slot != target.slot_id() || source_slot != output_slot {
            return Err("final result input validation requires exact identity slots".into());
        }
        let expected = if target.is_typed() {
            target.field_schema().logical_type()
        } else {
            marker(target.field())?
        };
        if target.field_schema().logical_type() != expected {
            return Err("final result output contains conflicting logical domain facts".into());
        }
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
        if target.is_typed() {
            if !actual_slot.is_typed() {
                return Err(
                    "typed final result input requires its explicit typed source binding".into(),
                );
            }
            let actual = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                actual_slot.typed_binding().clone(),
            )
            .map_err(|e| e.to_string())?;
            let expected = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                target.typed_binding().clone(),
            )
            .map_err(|e| e.to_string())?;
            if !actual.fits_value_type(&expected) {
                return Err(
                    "typed final result input semantics differ from output semantics".into(),
                );
            }
            check_type(actual_field.data_type(), target.data_type())?;
            check_type(actual_slot.data_type(), target.data_type())?;
        } else {
            check_field(actual_field, target.field())?;
            check_field(actual_slot.field(), target.field())?;
            check_cached(actual_slot.field_schema().value_type(), target.field())?;
        }
        let array = chunk
            .columns()
            .get(index)
            .ok_or("final result input array is missing")?;
        check_type(array.data_type(), target.data_type())?;
    }
    Ok(())
}

#[cfg(test)]
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

/// Encoding provenance is signed by the actual producing operation. An array
/// alone cannot establish an original dictionary's occurrence facts.
pub(crate) enum ProjectEncodingSource<'a> {
    Plain,
    OriginalField(&'a arrow::datatypes::Field),
    FreshUnorderedDictionary(&'a arrow::datatypes::Field),
}

pub(crate) fn project_reserve<T>(
    count: usize,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
) -> Result<Vec<T>, crate::exec::pipeline::operator::ConstructionFailure> {
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    if count != 0 {
        owner.request(
            std::alloc::Layout::array::<T>(count).map_err(|_| LayoutResourceError::Arithmetic)?,
        )?;
    }
    Ok(Vec::with_capacity(count))
}

pub(crate) fn project_field_owned(
    actor: &arrow::datatypes::Field,
    carrier: &DataType,
    name: &str,
    nullable: bool,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
) -> Result<arrow::datatypes::FieldRef, crate::exec::pipeline::operator::ConstructionFailure> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    owner.observe(
        0,
        &[],
        crate::exec::chunk::ArrayNormalizationStep::RebuildField {
            source: actor,
            target: carrier,
            name,
        },
    )?;
    #[allow(deprecated)]
    let field = if matches!(carrier, DataType::Dictionary(..)) {
        arrow::datatypes::Field::new_dict(
            name,
            carrier.clone(),
            nullable,
            actor.dict_id().ok_or_else(|| {
                ConstructionFailure::InvalidSource(
                    "project dictionary lacks its actual occurrence ID".into(),
                )
            })?,
            actor.dict_is_ordered().ok_or_else(|| {
                ConstructionFailure::InvalidSource(
                    "project dictionary lacks its actual order declaration".into(),
                )
            })?,
        )
    } else {
        arrow::datatypes::Field::new(name, carrier.clone(), nullable)
    };
    Ok(Arc::new(field))
}

pub(crate) fn project_schema_owned(
    slots: Vec<ChunkSlotSchema>,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
) -> Result<ChunkSchemaRef, crate::exec::pipeline::operator::ConstructionFailure> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    let schema = ChunkSchema::try_new_typed_observed::<ConstructionFailure>(
        slots,
        storage,
        |layout| owner.request(layout),
        |units| {
            owner.observe(
                0,
                &[],
                crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
            )
        },
    )?;
    owner.request(arc_layout(std::alloc::Layout::new::<ChunkSchema>())?)?;
    Ok(Arc::new(schema))
}

fn project_binding_fits(
    source: &novarocks_type_contract::TypedValueBinding,
    target: &novarocks_type_contract::TypedValueBinding,
    ordinal: usize,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
) -> Result<bool, crate::exec::pipeline::operator::ConstructionFailure> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    let work = || {
        owner.observe(
            ordinal,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
        )
    };
    work()?;
    // Reusing both already validated immutable handles needs no shared-tree
    // walk. Independent owners still receive the complete structural check.
    if source.value_type().shares_storage(target.value_type())
        && source.layout().shares_storage(target.layout())
    {
        return Ok(true);
    }
    let source = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
        source.clone_observed(work)?,
    )
    .map_err(|error| ConstructionFailure::InvalidSource(error.to_string()))?;
    let target = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
        target.clone_observed(work)?,
    )
    .map_err(|error| ConstructionFailure::InvalidSource(error.to_string()))?;
    source.fits_physical_binding_observed(&target, work)
}

/// Construct only an explicitly declared port. NULL values never widen its
/// declaration, and representation changes cannot erase nominal children.
pub(crate) fn materialize_project_output_owned<'a>(
    mut columns: Vec<ArrayRef>,
    row_count: usize,
    declared: ChunkSchemaRef,
    mut source_type_at: impl FnMut(
        usize,
    ) -> Result<
        &'a novarocks_type_contract::TypedValueBinding,
        crate::exec::pipeline::operator::ConstructionFailure,
    >,
    mut encoding_at: impl FnMut(
        usize,
    ) -> Result<
        ProjectEncodingSource<'a>,
        crate::exec::pipeline::operator::ConstructionFailure,
    >,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
) -> ExecutionResult<Chunk> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    if !declared.is_typed() || columns.len() != declared.slots().len() {
        return Err(ConstructionFailure::InvalidSource(
            "project requires its complete declared output port".into(),
        )
        .into());
    }
    // Normalization shares clean immutable representations and observes every
    // actual wrapper rebuild. The input Vec itself was covered by its author.
    let mut changed = false;
    for (ordinal, column) in columns.iter_mut().enumerate() {
        if !project_binding_fits(
            source_type_at(ordinal)?,
            declared.slots()[ordinal].typed_binding(),
            ordinal,
            owner,
        )? {
            return Err(ConstructionFailure::InvalidSource(
                "project expression's complete binding differs from its declared output".into(),
            )
            .into());
        }
        *column = crate::exec::chunk::normalize_execution_array_observed::<ConstructionFailure, _>(
            column,
            |step| owner.observe(ordinal, &[], step),
        )?;
        if column.data_type() != declared.slots()[ordinal].data_type()
            && matches!(encoding_at(ordinal)?, ProjectEncodingSource::Plain)
        {
            *column = novarocks_functions::array_projection::rebuild_array_mode::<
                ConstructionFailure,
                _,
            >(
                column,
                declared.slots()[ordinal].data_type(),
                &mut |step| owner.observe(ordinal, &[], step),
                false,
            )?;
        }
        changed |= column.data_type() != declared.slots()[ordinal].data_type();
    }
    let output = if changed {
        let mut slots = project_reserve(columns.len(), owner)?;
        for (ordinal, column) in columns.iter().enumerate() {
            let target = &declared.slots()[ordinal];
            if column.data_type() == target.data_type() {
                slots.push(target.with_slot_id_typed_observed(target.slot_id(), || {
                    owner.observe(
                        ordinal,
                        &[],
                        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                            units: 1,
                        },
                    )
                })?);
                continue;
            }
            let actor = match encoding_at(ordinal)? {
                ProjectEncodingSource::OriginalField(field)
                | ProjectEncodingSource::FreshUnorderedDictionary(field) => field,
                ProjectEncodingSource::Plain => {
                    return Err(ConstructionFailure::InvalidSource(
                        "project representation change has no explicit encoding producer".into(),
                    )
                    .into());
                }
            };
            if actor.data_type() != column.data_type() {
                return Err(ConstructionFailure::InvalidSource(
                    "project encoding producer differs from the declared actual field".into(),
                )
                .into());
            }
            let field = project_field_owned(
                actor,
                column.data_type(),
                target.name(),
                target.nullable(),
                owner,
            )?;
            slots.push(ChunkSlotSchema::try_new_typed_observed::<
                ConstructionFailure,
                _,
            >(
                target.slot_id(),
                field,
                target.typed_binding().clone(),
                target.unique_id(),
                storage.clone(),
                |step| owner.observe(ordinal, &[], step),
            )?);
        }
        project_schema_owned(slots, storage.clone(), owner)?
    } else {
        declared
    };
    Ok(Chunk::try_new_typed_with_columns_owned_observed::<
        ConstructionFailure,
        _,
    >(output, columns, row_count, storage, |step| {
        owner.observe(0, &[], step)
    })?)
}

#[derive(Clone)]
enum ProjectOutputDeclaration {
    Frozen(novarocks_local_program::StaticLayout),
    #[cfg(test)]
    Fixture(ChunkSchemaRef),
}
#[derive(Clone)]
enum ProjectPurposeDeclarations {
    Frozen(novarocks_local_program::ProjectExpressionSlots),
    #[cfg(test)]
    Fixture(Arc<[ChunkSlotSchema]>),
}
impl ProjectPurposeDeclarations {
    fn len(&self) -> usize {
        match self {
            Self::Frozen(slots) => slots.len(),
            #[cfg(test)]
            Self::Fixture(slots) => slots.len(),
        }
    }
}

pub struct ProjectProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    exprs: Vec<ExprId>,
    expr_slot_ids: Vec<SlotId>,
    purposes: Option<ProjectPurposeDeclarations>,
    output_indices: Option<Vec<usize>>,
    output: ProjectOutputDeclaration,
    validate_final_result_input: bool,
}
impl ProjectProcessorFactory {
    #[expect(
        clippy::too_many_arguments,
        reason = "Complete frozen Project declarations"
    )]
    pub fn new(
        node_id: i32,
        is_subordinate: bool,
        arena: Arc<ExprArena>,
        exprs: Vec<ExprId>,
        expr_slot_ids: Vec<SlotId>,
        purposes: Option<novarocks_local_program::ProjectExpressionSlots>,
        output_indices: Option<Vec<usize>>,
        output: novarocks_local_program::StaticLayout,
    ) -> Self {
        Self::from_declarations(
            node_id,
            is_subordinate,
            arena,
            exprs,
            expr_slot_ids,
            purposes.map(ProjectPurposeDeclarations::Frozen),
            output_indices,
            ProjectOutputDeclaration::Frozen(output),
        )
    }
    /// Component fixtures provide an explicit complete source schema. They
    /// still use the same installed owner and deferred runtime constructor.
    #[cfg(test)]
    #[expect(
        clippy::too_many_arguments,
        reason = "Explicit component source declarations"
    )]
    pub fn from_fixture_schema(
        node_id: i32,
        is_subordinate: bool,
        arena: Arc<ExprArena>,
        exprs: Vec<ExprId>,
        expr_slot_ids: Vec<SlotId>,
        purposes: Option<Vec<ChunkSlotSchema>>,
        output_indices: Option<Vec<usize>>,
        output: ChunkSchemaRef,
    ) -> Self {
        Self::from_declarations(
            node_id,
            is_subordinate,
            arena,
            exprs,
            expr_slot_ids,
            purposes.map(|slots| ProjectPurposeDeclarations::Fixture(slots.into())),
            output_indices,
            ProjectOutputDeclaration::Fixture(output),
        )
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "One owner-neutral declaration body"
    )]
    fn from_declarations(
        node_id: i32,
        is_subordinate: bool,
        arena: Arc<ExprArena>,
        exprs: Vec<ExprId>,
        expr_slot_ids: Vec<SlotId>,
        purposes: Option<ProjectPurposeDeclarations>,
        output_indices: Option<Vec<usize>>,
        output: ProjectOutputDeclaration,
    ) -> Self {
        let mut name = if node_id >= 0 {
            format!("PROJECT (id={node_id})")
        } else {
            "PROJECT".into()
        };
        if is_subordinate {
            name.push_str(" (subordinate)");
        }
        Self {
            name,
            arena,
            exprs,
            expr_slot_ids,
            purposes,
            output_indices,
            output,
            validate_final_result_input: false,
        }
    }
    pub fn with_final_result_input_validation(mut self, enabled: bool) -> Self {
        self.validate_final_result_input = enabled;
        self
    }
}
impl ProjectProcessorFactory {
    fn create_operator(&self) -> ProjectProcessorOperator {
        ProjectProcessorOperator {
            name: self.name.clone(),
            arena: self.arena.clone(),
            exprs: self.exprs.clone(),
            expr_slot_ids: self.expr_slot_ids.clone(),
            declared_output: Some(self.output.clone()),
            purpose_sources: self.purposes.clone(),
            expr_slot_schemas: HashMap::new(),
            purpose_storage: None,
            output_indices: self.output_indices.clone(),
            output_chunk_schema: None,
            prepare_failure: None,
            validate_final_result_input: self.validate_final_result_input,
            construction: None,
            control: None,
            tracker: None,
            pending_output: None,
            finishing: false,
            finished: false,
        }
    }
}

impl OperatorFactory for ProjectProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(self.create_operator())
    }
}

struct ProjectProcessorOperator {
    name: String,
    arena: Arc<ExprArena>,
    exprs: Vec<ExprId>,
    expr_slot_ids: Vec<SlotId>,
    expr_slot_schemas: HashMap<SlotId, ChunkSlotSchema>,
    output_indices: Option<Vec<usize>>,
    declared_output: Option<ProjectOutputDeclaration>,
    purpose_sources: Option<ProjectPurposeDeclarations>,
    output_chunk_schema: Option<ChunkSchemaRef>,
    // Containers/data are dropped before their finite map creation hold.
    purpose_storage: Option<novarocks_type_contract::ValueTypeStorageLifetime>,
    prepare_failure: Option<crate::runtime::fragment::ExecutionFailure>,
    validate_final_result_input: bool,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    control: Option<super::compiled_expression::RuntimeKernelControl>,
    tracker: Option<Arc<crate::runtime::mem_tracker::MemTracker>>,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

impl Operator for ProjectProcessorOperator {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        if let Some(failure) = &self.prepare_failure {
            return Err(failure.clone());
        }
        if self.output_chunk_schema.is_some() {
            return Ok(());
        }
        let result: ExecutionResult<()> = (|| {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let declaration = self
                .declared_output
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let output = match declaration {
                ProjectOutputDeclaration::Frozen(layout) => {
                    factory.create_compiled_chunk_schema(layout)?
                }
                #[cfg(test)]
                ProjectOutputDeclaration::Fixture(source) => {
                    let owner = factory.begin_typed_construction(source)?;
                    if !source.is_typed() {
                        return Err(ConstructionFailure::InvalidSource(
                            "Project fixture requires its complete declared schema".into(),
                        )
                        .into());
                    }
                    let storage = owner
                        .storage_lifetime()
                        .retaining_observed(&source.storage_lifetime(), |layout| {
                            owner.request(layout)
                        })?;
                    let mut slots = project_reserve(source.slots().len(), owner.as_ref())?;
                    for slot in source.slots() {
                        slots.push(slot.with_slot_id_typed_observed(slot.slot_id(), || {
                            owner.observe(0, &[], crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 })
                        })?);
                    }
                    project_schema_owned(slots, storage, owner.as_ref())?
                }
            };
            let mut purposes = HashMap::new();
            let mut purpose_storage = None;
            if let Some(source) = &self.purpose_sources {
                let owner = factory.begin_typed_construction(&output)?;
                let table =
                    novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
                        SlotId,
                        ChunkSlotSchema,
                    >(source.len())
                    .map_err(|error| ConstructionFailure::Resource(match error {
                        novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::Arithmetic(_) =>
                            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
                        novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::SourceModel(_) =>
                            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
                    }))?;
                if let Some(layout) = table.layout {
                    owner.request(layout)?;
                }
                purposes = HashMap::with_capacity(source.len());
                match source {
                    ProjectPurposeDeclarations::Frozen(slots) => {
                        for slot in slots.iter() {
                            let storage = owner
                                .storage_lifetime()
                                .retaining_observed(&slot.storage, |layout| {
                                    owner.request(layout)
                                })?;
                            let value =
                                ChunkSlotSchema::try_new_typed_observed::<ConstructionFailure, _>(
                                    slot.slot_id,
                                    slot.field.clone(),
                                    slot.binding.clone(),
                                    slot.unique_id,
                                    storage,
                                    |step| owner.observe(0, &[], step),
                                )?;
                            owner.observe(0, &[], crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 })?;
                            purposes.insert(value.slot_id(), value);
                        }
                    }
                    #[cfg(test)]
                    ProjectPurposeDeclarations::Fixture(slots) => {
                        for slot in slots.iter() {
                            if !slot.is_typed() {
                                return Err(ConstructionFailure::InvalidSource(
                                    "Project fixture purpose lacks its complete declaration".into(),
                                )
                                .into());
                            }
                            let value = slot.with_slot_id_typed_observed(slot.slot_id(), || {
                                owner.observe(0, &[], crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 })
                            })?;
                            purposes.insert(value.slot_id(), value);
                        }
                    }
                }
                purpose_storage = Some(owner.storage_lifetime());
            }
            self.expr_slot_schemas = purposes;
            self.purpose_storage = purpose_storage;
            self.output_chunk_schema = Some(output);
            self.declared_output = None;
            self.purpose_sources = None;
            Ok(())
        })();
        if let Err(failure) = &result {
            self.prepare_failure = Some(failure.clone());
        }
        result
    }
    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.tracker = Some(tracker.clone());
        if let Some(control) = &mut self.control {
            control.bind_mem_tracker(tracker);
        }
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        let mut control =
            super::compiled_expression::RuntimeKernelControl::new(state.error_state());
        control.bind_runtime_memory(state);
        control.bind_construction_factory(
            self.construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?
                .clone(),
        );
        if let Some(tracker) = &self.tracker {
            control.bind_mem_tracker(tracker.clone());
        }
        self.control = Some(control);
        Ok(())
    }
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
        let supported_carrier = if self.validate_final_result_input {
            is_final_result_string_dictionary(data_type)
        } else {
            is_supported_i32_string_dictionary(data_type)
        };
        if !supported_carrier {
            return false;
        }
        self.all_uses_accept_dictionary(slot_id, self.validate_final_result_input)
    }

    fn accepts_encoded_path(
        &self,
        occurrence: crate::exec::pipeline::operator::EncodedValueOccurrence<'_>,
    ) -> bool {
        if occurrence.path.is_empty() {
            return self.accepts_encoded_column(occurrence.slot, occurrence.carrier);
        }
        // Nested occurrences have only the actual identity path. Root PEEL
        // cannot establish a capability for independently encoded children.
        is_final_result_string_dictionary(occurrence.carrier)
            && self.all_uses_accept_dictionary(occurrence.slot, true)
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
    fn prepared_output(&self) -> ExecutionResult<&ChunkSchemaRef> {
        if let Some(failure) = &self.prepare_failure {
            return Err(failure.clone());
        }
        self.output_chunk_schema.as_ref().ok_or_else(|| {
            crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into()
        })
    }

    /// Every use needs this installed operator's identity or explicit peel path.
    fn all_uses_accept_dictionary(&self, slot_id: SlotId, identity_only: bool) -> bool {
        let mut used = false;
        for expr_id in &self.exprs {
            match expression_uses_slot(&self.arena, *expr_id, slot_id) {
                Some(false) => continue,
                None => return false,
                Some(true) => used = true,
            }
            let identity = matches!(
                self.arena.node(*expr_id),
                Some(ExprNode::SlotId(source_slot)) if *source_slot == slot_id
            );
            if !identity
                && (identity_only || !expr_can_peel_from_slot(&self.arena, *expr_id, slot_id))
            {
                return false;
            }
        }
        used
    }
    fn eval_project_expr(
        &self,
        expr_id: ExprId,
        chunk: &Chunk,
        output_name: &str,
        owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    ) -> ExecutionResult<(
        ArrayRef,
        Option<arrow::datatypes::FieldRef>,
        novarocks_type_contract::ValueTypeStorageLifetime,
    )> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        let control = self
            .control
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        if let Some(ExprNode::SlotId(source)) = self.arena.node(expr_id) {
            let slot = chunk.chunk_schema().slot(*source).ok_or_else(|| {
                ConstructionFailure::InvalidSource("project identity source is absent".into())
            })?;
            if !slot.is_typed() {
                return Err(ConstructionFailure::InvalidSource(
                    "project identity source lacks its complete binding".into(),
                )
                .into());
            }
            let declared = self.arena.value_type(expr_id).ok_or_else(|| {
                ConstructionFailure::InvalidSource(
                    "project identity lacks its complete expression declaration".into(),
                )
            })?;
            if !project_binding_fits(slot.typed_binding(), declared.typed_binding(), 0, owner)? {
                return Err(ConstructionFailure::InvalidSource(
                    "project identity source differs from its complete expression declaration"
                        .into(),
                )
                .into());
            }
            let storage = chunk
                .source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                    owner.request(layout)
                })?;
            return Ok((
                chunk.column_by_slot_id(*source)?,
                Some(slot.field_ref().clone()),
                storage,
            ));
        }
        if self.validate_final_result_input {
            return Err("final result requires identity expressions".into());
        }
        if let Some(peeled) =
            try_peel_dict_expr_owned(&self.arena, expr_id, chunk, output_name, owner, control)?
        {
            return Ok((peeled.array, peeled.fresh_field, peeled.storage));
        }
        let mut needs_hydration = false;
        for source in chunk.chunk_schema().slots() {
            if is_supported_i32_string_dictionary(source.data_type()) {
                let used = expression_uses_slot(&self.arena, expr_id, source.slot_id())
                    .ok_or_else(|| {
                        ConstructionFailure::InvalidSource(
                            "project lexical references are invalid".into(),
                        )
                    })?;
                needs_hydration |= used;
            }
        }
        let (array, storage) = if needs_hydration {
            let hydrated = chunk.hydrate_for_evaluation(owner, |slot, _| {
                expression_uses_slot(&self.arena, expr_id, slot) == Some(false)
            })?;
            let storage = hydrated
                .source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                    owner.request(layout)
                })?;
            let array = crate::exec::expr::ExprEvaluationContext::new(&self.arena, owner, control)
                .eval(expr_id, &hydrated)?;
            (array, storage)
        } else {
            let storage = chunk
                .source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                    owner.request(layout)
                })?;
            let array = crate::exec::expr::ExprEvaluationContext::new(&self.arena, owner, control)
                .eval(expr_id, chunk)?;
            (array, storage)
        };
        let array = if matches!(
            self.arena.node(expr_id),
            Some(ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::HourFromUnixtime,
                ..
            })
        ) {
            let target = self.arena.value_type(expr_id).ok_or_else(|| {
                ConstructionFailure::InvalidSource(
                    "HOUR public result lacks its complete expression declaration".into(),
                )
            })?;
            let mut request = |layout| owner.request(layout);
            let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
            let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
                owner.storage_lifetime(),
                &mut request,
                &mut opaque,
            );
            novarocks_functions::builtin::calendar_unixtime::realize_legacy_hour_public_result(
                &array,
                target,
                &mut creation,
                &mut |_| {
                    owner.observe(
                        0,
                        &[],
                        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                            units: 1,
                        },
                    )
                },
            )
            .map_err(|error| -> crate::runtime::fragment::ExecutionFailure {
                use novarocks_functions::selected_copy::{CopyConstructionError, CopyError};
                match error {
                    CopyConstructionError::Creator(cause) => cause.into(),
                    CopyConstructionError::Resource(cause) => {
                        ConstructionFailure::Resource(cause).into()
                    }
                    CopyConstructionError::Copy(CopyError::Control(cause)) => cause.into(),
                    CopyConstructionError::Copy(CopyError::Extent) => {
                        novarocks_functions::KernelFailure::ResourceExhausted.into()
                    }
                    CopyConstructionError::Copy(CopyError::Invalid(message)) => {
                        ConstructionFailure::InvalidSource(message.into()).into()
                    }
                    CopyConstructionError::Copy(CopyError::Unsupported(ty)) => {
                        ConstructionFailure::InvalidSource(format!(
                            "HOUR public result does not support {ty:?}"
                        ))
                        .into()
                    }
                }
            })?
        } else {
            array
        };
        Ok((array, None, storage))
    }

    fn expression_slot_owned(
        &self,
        expr_id: ExprId,
        slot_id: SlotId,
        actual: &ArrayRef,
        encoding: Option<&arrow::datatypes::Field>,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    ) -> ExecutionResult<ChunkSlotSchema> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        let author = self.arena.value_type(expr_id).ok_or_else(|| {
            ConstructionFailure::InvalidSource(
                "project expression lacks its complete declared value type".into(),
            )
        })?;
        let purpose = self.expr_slot_schemas.get(&slot_id);
        if let Some(purpose) = purpose {
            if !purpose.is_typed() {
                return Err(ConstructionFailure::InvalidSource(
                    "project CSE purpose lacks its complete declaration".into(),
                )
                .into());
            }
            if !project_binding_fits(author.typed_binding(), purpose.typed_binding(), 0, owner)? {
                return Err(ConstructionFailure::InvalidSource(
                    "project CSE differs from its complete declared purpose".into(),
                )
                .into());
            }
        }
        let name = purpose.map(ChunkSlotSchema::name).unwrap_or("_cse");
        let binding = purpose
            .map(|p| p.typed_binding())
            .unwrap_or(author.typed_binding());
        let field = if let Some(encoding) = encoding {
            project_field_owned(
                encoding,
                actual.data_type(),
                name,
                binding.nullable(),
                owner,
            )?
        } else {
            if actual.data_type() != binding.data_type() {
                return Err(ConstructionFailure::InvalidSource(
                    "project expression carrier differs from its explicit author".into(),
                )
                .into());
            }
            let anchor = arrow::datatypes::Field::new("", DataType::Null, binding.nullable());
            project_field_owned(&anchor, actual.data_type(), name, binding.nullable(), owner)?
        };
        Ok(ChunkSlotSchema::try_new_typed_observed::<
            ConstructionFailure,
            _,
        >(
            slot_id,
            field,
            binding.clone(),
            purpose.and_then(ChunkSlotSchema::unique_id),
            storage,
            |step| owner.observe(0, &[], step),
        )?)
    }

    fn validate_final_result_identity_input(&self, chunk: &Chunk) -> ExecutionResult<()> {
        validate_final_result_identity_input_source(
            chunk,
            self.prepared_output()?,
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
        .map_err(Into::into)
    }

    #[cfg(test)]
    fn legacy_fixture_empty_output_chunk(&self) -> Result<Chunk, String> {
        let output = self.prepared_output().map_err(|error| error.to_string())?;
        let mut columns = Vec::with_capacity(output.slots().len());
        let mut slots = Vec::with_capacity(output.slots().len());
        for slot in output.slots() {
            slots.push(projected_slot_schema(
                slot,
                slot.slot_id(),
                slot.data_type(),
                false,
            )?);
            columns.push(new_empty_array(slot.data_type()));
        }
        Chunk::try_new_with_columns(projected_chunk_schema(slots, &output)?, columns)
    }

    fn process_one(&mut self, chunk: Chunk) -> ExecutionResult<Option<Chunk>> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        let output = self.prepared_output()?.clone();
        if self.validate_final_result_input {
            self.validate_final_result_identity_input(&chunk)?;
        }
        if self.expr_slot_ids.len() != self.exprs.len() {
            return Err("project expressions and slots have different widths".into());
        }
        let control = self
            .control
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = control.construction_owner(&output)?;
        if chunk.is_empty() {
            let mut columns = project_reserve(output.slots().len(), owner.as_ref())?;
            let mut request = |layout| owner.request(layout);
            let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
            let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
                owner.storage_lifetime(),
                &mut request,
                &mut opaque,
            );
            for target in output.slots() {
                crate::exec::expr::ExprEvaluationControl::checkpoint(control)?;
                novarocks_functions::selected_copy::preflight_empty_construction_observed(
                    target.typed_binding().layout(),
                    &mut creation,
                    &mut |_| {
                        crate::exec::expr::ExprEvaluationControl::checkpoint(control)
                            .map_err(ConstructionFailure::from)
                    },
                )
                .map_err(|error| match error {
                    novarocks_functions::selected_copy::CopyConstructionError::Creator(error) => {
                        crate::runtime::fragment::ExecutionFailure::from(error)
                    }
                    novarocks_functions::selected_copy::CopyConstructionError::Resource(error) => {
                        crate::runtime::fragment::ExecutionFailure::from(
                            ConstructionFailure::Resource(error),
                        )
                    }
                    novarocks_functions::selected_copy::CopyConstructionError::Copy(
                        novarocks_functions::selected_copy::CopyError::Control(error),
                    ) => crate::runtime::fragment::ExecutionFailure::from(error),
                    other => crate::runtime::fragment::ExecutionFailure::from(format!(
                        "project empty representation: {other:?}"
                    )),
                })?;
                columns.push(new_empty_array(target.data_type()));
                crate::exec::expr::ExprEvaluationControl::checkpoint(control)?;
            }
            return Ok(Some(materialize_project_output_owned(
                columns,
                0,
                output.clone(),
                |ordinal| {
                    let selected = self
                        .output_indices
                        .as_ref()
                        .map_or(ordinal, |indices| indices[ordinal]);
                    let expr = self.exprs.get(selected).ok_or_else(|| {
                        ConstructionFailure::InvalidSource(
                            "empty project output ordinal is out of bounds".into(),
                        )
                    })?;
                    Ok(self
                        .arena
                        .value_type(*expr)
                        .ok_or_else(|| {
                            ConstructionFailure::InvalidSource(
                                "empty project lacks its complete expression author".into(),
                            )
                        })?
                        .typed_binding())
                },
                |_| Ok(ProjectEncodingSource::Plain),
                owner.storage_lifetime(),
                owner.as_ref(),
            )?));
        }
        let mut working = chunk.clone();
        let mut computed =
            project_reserve::<(ArrayRef, ChunkSlotSchema, bool)>(self.exprs.len(), owner.as_ref())?;
        for (expr, slot_id) in self.exprs.iter().zip(&self.expr_slot_ids) {
            let name = self
                .expr_slot_schemas
                .get(slot_id)
                .map(ChunkSlotSchema::name)
                .unwrap_or("_cse");
            let (array, encoding, storage) =
                self.eval_project_expr(*expr, &working, name, owner.as_ref())?;
            let slot = self.expression_slot_owned(
                *expr,
                *slot_id,
                &array,
                encoding.as_deref(),
                storage.clone(),
                owner.as_ref(),
            )?;
            let fresh =
                encoding.is_some() && !matches!(self.arena.node(*expr), Some(ExprNode::SlotId(_)));
            computed.push((array.clone(), slot.clone(), fresh));
            if matches!(self.arena.node(*expr), Some(ExprNode::SlotId(source)) if source == slot_id)
            {
                continue;
            }
            let existing = working.slot_id_to_index().get(slot_id).copied();
            let count = working.columns().len() + usize::from(existing.is_none());
            let mut columns = project_reserve(count, owner.as_ref())?;
            columns.extend(working.columns().iter().cloned());
            let mut slots = project_reserve(count, owner.as_ref())?;
            for source in working.chunk_schema().slots() {
                slots.push(source.with_slot_id_typed_observed(source.slot_id(), || {
                    owner.observe(
                        0,
                        &[],
                        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                            units: 1,
                        },
                    )
                })?);
            }
            if let Some(index) = existing {
                columns[index] = array;
                slots[index] = slot;
            } else {
                columns.push(array);
                slots.push(slot);
            }
            let schema = project_schema_owned(slots, storage.clone(), owner.as_ref())?;
            working = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
                schema,
                columns,
                chunk.record_batch().num_rows(),
                storage,
                |step| owner.observe(0, &[], step),
            )?;
        }
        let count = self
            .output_indices
            .as_ref()
            .map_or(computed.len(), Vec::len);
        let index_at = |ordinal: usize| -> Result<usize, ConstructionFailure> {
            let index = self
                .output_indices
                .as_ref()
                .map_or(ordinal, |indices| indices[ordinal]);
            if index >= computed.len() {
                return Err(ConstructionFailure::InvalidSource(
                    "project output ordinal is out of bounds".into(),
                ));
            }
            Ok(index)
        };
        let mut columns = project_reserve(count, owner.as_ref())?;
        for ordinal in 0..count {
            columns.push(computed[index_at(ordinal)?].0.clone());
        }
        let storage = if count == 0 {
            owner.storage_lifetime()
        } else {
            working.source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                owner.request(layout)
            })?
        };
        Ok(Some(materialize_project_output_owned(
            columns,
            chunk.record_batch().num_rows(),
            output.clone(),
            |ordinal| Ok(computed[index_at(ordinal)?].1.typed_binding()),
            |ordinal| {
                let entry = &computed[index_at(ordinal)?];
                Ok(if entry.2 {
                    ProjectEncodingSource::FreshUnorderedDictionary(entry.1.field())
                } else {
                    ProjectEncodingSource::OriginalField(entry.1.field())
                })
            },
            storage,
            owner.as_ref(),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
    };
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

    struct FixtureConstructionHost;
    impl crate::exec::pipeline::operator::input_construction::InputConstructionRequestHost
        for FixtureConstructionHost
    {
        fn request(
            &self,
            _: std::alloc::Layout,
        ) -> Result<(), crate::exec::pipeline::operator::ConstructionFailure> {
            Ok(())
        }
        fn observe_opaque_bound(
            &self,
            _: usize,
        ) -> Result<(), crate::exec::pipeline::operator::ConstructionFailure> {
            Ok(())
        }
        fn release_requested_bytes(&self, _: usize) {}
    }
    impl crate::exec::pipeline::operator::input_construction::InputConstructionWorkControl
        for FixtureConstructionHost
    {
        fn checkpoint(
            &self,
            _: usize,
        ) -> Result<(), crate::exec::pipeline::operator::ConstructionFailure> {
            Ok(())
        }
    }
    fn install_fixture_creator(op: &mut ProjectProcessorOperator) {
        use crate::exec::pipeline::operator::Operator;
        use crate::exec::pipeline::operator::input_construction::*;
        let factory = ObservedInputConstructionFactory::try_new(
            InputConstructionProfile {
                max_requests: 4096,
                max_request_bytes: 1 << 20,
                max_opaque_requests: 4096,
                max_opaque_bytes: 1 << 20,
                max_work: 1 << 20,
                max_depth: 64,
            },
            Arc::new(FixtureConstructionHost),
            Arc::new(FixtureConstructionHost),
            Arc::new(LockedArrowInputConstructionModel),
        )
        .unwrap();
        op.declared_output = Some(super::ProjectOutputDeclaration::Fixture(
            op.output_chunk_schema
                .take()
                .expect("explicit fixture output declaration"),
        ));
        if !op.expr_slot_schemas.is_empty() {
            op.purpose_sources = Some(super::ProjectPurposeDeclarations::Fixture(
                op.expr_slot_schemas
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
                    .into(),
            ));
        }
        op.expr_slot_schemas.clear();
        op.set_construction_factory(factory.clone());
        op.prepare().unwrap();
        let mut control = super::super::compiled_expression::RuntimeKernelControl::new(Arc::new(
            crate::runtime::runtime_state::RuntimeErrorState::default(),
        ));
        control.bind_construction_factory(factory);
        op.control = Some(control);
    }
    fn fixture_slot(
        slot: &ChunkSlotSchema,
        declared: &novarocks_type_contract::FunctionValueType,
    ) -> ChunkSlotSchema {
        // The caller supplies the complete semantic declaration. The original
        // field supplies only its name/encoding facts, with NULL authored by
        // that same declaration. Generic attributes stay outside execution.
        let actual = crate::exec::chunk::normalize_execution_field_observed::<String, _>(
            slot.field_ref(),
            |_| Ok(()),
        )
        .unwrap();
        let actual = Arc::new(actual.as_ref().clone().with_nullable(declared.nullable()));
        ChunkSlotSchema::try_new_typed(
            slot.slot_id(),
            actual,
            declared.typed_binding().clone(),
            slot.unique_id(),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap()
    }
    fn fixture_schema(
        schema: &ChunkSchema,
        declarations: &[novarocks_type_contract::FunctionValueType],
    ) -> Arc<ChunkSchema> {
        assert_eq!(schema.slots().len(), declarations.len());
        Arc::new(
            ChunkSchema::try_new_typed(
                schema
                    .slots()
                    .iter()
                    .zip(declarations)
                    .map(|(slot, ty)| fixture_slot(slot, ty))
                    .collect(),
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        )
    }
    fn fixture_input(
        input: Chunk,
        declarations: &[novarocks_type_contract::FunctionValueType],
    ) -> Chunk {
        let schema = fixture_schema(input.chunk_schema(), declarations);
        Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            schema,
            input.columns().to_vec(),
            input.len(),
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap()
    }
    fn run_declared_physical_fixture(
        op: &mut ProjectProcessorOperator,
        input: Chunk,
    ) -> crate::runtime::fragment::ExecutionResult<Option<Chunk>> {
        use novarocks_type_contract::FunctionValueType;
        // These scalar fixtures explicitly declare Physical semantics for all
        // values; no field marker participates in their authored declaration.
        let input_types = input
            .chunk_schema()
            .slots()
            .iter()
            .map(|slot| FunctionValueType::new(slot.data_type().clone(), slot.nullable()))
            .collect::<Vec<_>>();
        let input = fixture_input(input, &input_types);
        let output_types = op
            .output_chunk_schema
            .as_ref()
            .unwrap()
            .slots()
            .iter()
            .map(|slot| FunctionValueType::new(slot.data_type().clone(), slot.nullable()))
            .collect::<Vec<_>>();
        op.output_chunk_schema = Some(fixture_schema(
            op.output_chunk_schema.as_ref().unwrap(),
            &output_types,
        ));
        for slot in op.expr_slot_schemas.values_mut() {
            let ty = FunctionValueType::new(slot.data_type().clone(), slot.nullable());
            *slot = fixture_slot(slot, &ty);
        }
        let arena = Arc::make_mut(&mut op.arena);
        let mut ordinal = 0;
        while let Some(node) = arena.node(crate::exec::expr::ExprId(ordinal)) {
            let id = crate::exec::expr::ExprId(ordinal);
            let nullable = match node {
                ExprNode::SlotId(slot) => input
                    .chunk_schema()
                    .slot(*slot)
                    .or_else(|| op.output_chunk_schema.as_ref().unwrap().slot(*slot))
                    .map_or(true, ChunkSlotSchema::nullable),
                ExprNode::Literal(LiteralValue::Null) => true,
                ExprNode::Literal(_) => false,
                ExprNode::Cast(
                    child,
                    novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                ) => arena
                    .value_type(*child)
                    .map_or(true, novarocks_type_contract::FunctionValueType::nullable),
                _ => true,
            };
            let ty = FunctionValueType::new(arena.data_type(id).unwrap().clone(), nullable);
            arena.set_value_type(id, ty).unwrap();
            ordinal += 1;
        }
        install_fixture_creator(op);
        op.process_one(input)
    }

    fn typed_fixture_input_direct(
        field: Field,
        declaration: &novarocks_type_contract::FunctionValueType,
        array: ArrayRef,
        slot: SlotId,
    ) -> Chunk {
        let slot = ChunkSlotSchema::try_new_typed(
            slot,
            Arc::new(field.with_nullable(declaration.nullable())),
            declaration.typed_binding().clone(),
            None,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let rows = array.len();
        Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            Arc::new(
                ChunkSchema::try_new_typed(vec![slot], ValueTypeStorageLifetime::pure()).unwrap(),
            ),
            vec![array],
            rows,
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap()
    }
    fn configure_typed_identity(
        op: &mut ProjectProcessorOperator,
        declaration: &novarocks_type_contract::FunctionValueType,
    ) {
        let source = match op.arena.node(op.exprs[0]) {
            Some(ExprNode::SlotId(slot)) => *slot,
            _ => panic!("fixture requires a genuine identity"),
        };
        let mut arena = ExprArena::default();
        op.exprs = vec![arena.push_value_typed(ExprNode::SlotId(source), declaration.clone())];
        op.arena = Arc::new(arena);
        let purpose = &op.output_chunk_schema.as_ref().unwrap().slots()[0];
        op.output_chunk_schema = Some(Arc::new(
            ChunkSchema::try_new_typed(
                vec![
                    ChunkSlotSchema::try_new_typed(
                        purpose.slot_id(),
                        Arc::new(Field::new(
                            purpose.name(),
                            declaration.data_type().clone(),
                            declaration.nullable(),
                        )),
                        declaration.typed_binding().clone(),
                        purpose.unique_id(),
                        ValueTypeStorageLifetime::pure(),
                    )
                    .unwrap(),
                ],
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        ));
        install_fixture_creator(op);
    }

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
                output_chunk_schema: Some(Arc::new(
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
                )),
                validate_final_result_input: false,
                declared_output: None,
                purpose_sources: None,
                purpose_storage: None,
                prepare_failure: None,
                construction: None,
                control: None,
                tracker: None,
                pending_output: None,
                finishing: false,
                finished: false,
            };
            let output = run_declared_physical_fixture(&mut op, input)
                .unwrap()
                .unwrap();
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
    fn project_runtime_keeps_full_json_declaration_and_separate_source_attributes() {
        use novarocks_type_contract::FunctionValueType;
        let declaration =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let source_format_field = Field::new("payload", DataType::Utf8, true)
            .with_metadata([("nr_logical_type".into(), "unknown format attribute".into())].into());
        let source_slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(source_format_field.clone()),
            declaration.typed_binding().clone(),
            Some(17),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let compute = source_slot
            .with_slot_id_typed_observed::<String>(SlotId::new(3), || Ok(()))
            .unwrap();
        let duplicate = source_slot
            .with_slot_id_typed_observed::<String>(SlotId::new(4), || Ok(()))
            .unwrap();
        let mut arena = ExprArena::default();
        let expr = arena.push_value_typed(ExprNode::SlotId(SlotId::new(1)), declaration.clone());
        let mut op = ProjectProcessorOperator {
            name: "PROJECT".into(),
            arena: Arc::new(arena),
            exprs: vec![expr],
            expr_slot_ids: vec![SlotId::new(3)],
            expr_slot_schemas: HashMap::from([(SlotId::new(3), compute.clone())]),
            output_indices: Some(vec![0, 0]),
            output_chunk_schema: Some(Arc::new(
                ChunkSchema::try_new_typed(
                    vec![compute, duplicate],
                    ValueTypeStorageLifetime::pure(),
                )
                .unwrap(),
            )),
            validate_final_result_input: false,
            declared_output: None,
            purpose_sources: None,
            purpose_storage: None,
            prepare_failure: None,
            construction: None,
            control: None,
            tracker: None,
            pending_output: None,
            finishing: false,
            finished: false,
        };
        install_fixture_creator(&mut op);
        for values in [vec![Some("{}")], Vec::new()] {
            let array: ArrayRef = Arc::new(StringArray::from(values));
            let rows = array.len();
            let input = Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
                Arc::new(
                    ChunkSchema::try_new_typed(
                        vec![source_slot.clone()],
                        ValueTypeStorageLifetime::pure(),
                    )
                    .unwrap(),
                ),
                vec![array],
                rows,
                ValueTypeStorageLifetime::pure(),
                |_| Ok(()),
            )
            .unwrap();
            let output = op.process_one(input).unwrap().unwrap();
            assert_eq!(output.len(), rows);
            assert_eq!(
                output.chunk_schema().slot_ids(),
                &[SlotId::new(3), SlotId::new(4)]
            );
            for slot in output.chunk_schema().slots() {
                assert_eq!(slot.unique_id(), Some(17));
                assert_eq!(
                    slot.typed_binding().value_type().domain(),
                    Some(ValueLogicalType::Json)
                );
                assert!(slot.field().metadata().is_empty());
            }
            assert!(output.schema().metadata().is_empty());
            if rows > 0 {
                assert!(Arc::ptr_eq(&output.columns()[0], &output.columns()[1]));
            }
        }
        assert_eq!(
            source_format_field.metadata()["nr_logical_type"],
            "unknown format attribute"
        );
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
            output_chunk_schema: Some(utf8_output_schema(output_slot, "lower_status")),
            validate_final_result_input: false,
            declared_output: None,
            purpose_sources: None,
            purpose_storage: None,
            prepare_failure: None,
            construction: None,
            control: None,
            tracker: None,
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
    fn project_dictionary_input_requires_all_uses_to_accept_the_encoding() {
        use crate::exec::pipeline::operator_factory::OperatorFactory;
        let input_slot = SlotId::new(1);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(input_slot), DataType::Utf8);
        let lower = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("lower"),
                args: vec![source],
            },
            DataType::Utf8,
        );
        let unused = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("constant".into())),
            DataType::Utf8,
        );
        let unsupported = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Coalesce,
                args: vec![source, unused],
            },
            DataType::Utf8,
        );
        let arena = Arc::new(arena);
        let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        for (expressions, final_input, expected) in [
            (vec![source, lower], false, true),
            (vec![source, unsupported], false, false),
            (vec![lower, unsupported], false, false),
            (vec![source, unused], false, true),
            (vec![source, unsupported], true, false),
        ] {
            let slots: Vec<_> = (0..expressions.len())
                .map(|ordinal| SlotId::new(100 + ordinal as u32))
                .collect();
            let binding = novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true);
            let fields = slots
                .iter()
                .map(|slot| {
                    ChunkSlotSchema::try_new_typed(
                        *slot,
                        Arc::new(Field::new(format!("output_{slot}"), DataType::Utf8, true)),
                        binding.typed_binding().clone(),
                        None,
                        ValueTypeStorageLifetime::pure(),
                    )
                    .unwrap()
                })
                .collect();
            let output = Arc::new(
                ChunkSchema::try_new_typed(fields, ValueTypeStorageLifetime::pure()).unwrap(),
            );
            let factory = super::ProjectProcessorFactory::from_fixture_schema(
                1,
                false,
                arena.clone(),
                expressions,
                slots,
                None,
                None,
                output,
            )
            .with_final_result_input_validation(final_input);
            let installed = factory.create(1, 0);
            let processor = installed.as_processor_ref().unwrap();
            assert_eq!(
                processor.accepts_encoded_column(input_slot, &dictionary),
                expected
            );
        }
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
            output_chunk_schema: Some(utf8_output_schema(output_slot, "lower_status")),
            validate_final_result_input: false,
            declared_output: None,
            purpose_sources: None,
            purpose_storage: None,
            prepare_failure: None,
            construction: None,
            control: None,
            tracker: None,
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let output = run_declared_physical_fixture(&mut op, dict_string_chunk(input_slot))
            .expect("project should succeed")
            .expect("project output");
        let column = output.record_batch().column(0);
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
            output_chunk_schema: Some(output_chunk_schema),
            validate_final_result_input: false,
            declared_output: None,
            purpose_sources: None,
            purpose_storage: None,
            prepare_failure: None,
            construction: None,
            control: None,
            tracker: None,
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let output = run_declared_physical_fixture(&mut op, dict_string_chunk(input_slot))
            .expect("project should succeed")
            .expect("project output");
        assert_eq!(
            output.record_batch().column(0).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(output.record_batch().column(1).data_type(), &DataType::Utf8);
        let status_or_missing = output
            .record_batch()
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
            output_chunk_schema: Some(output_chunk_schema),
            validate_final_result_input: false,
            declared_output: None,
            purpose_sources: None,
            purpose_storage: None,
            prepare_failure: None,
            construction: None,
            control: None,
            tracker: None,
            pending_output: None,
            finishing: false,
            finished: false,
        };

        let output = run_declared_physical_fixture(&mut op, dict_string_chunk(input_slot))
            .expect("project should succeed")
            .expect("project output");
        let lower_dict = output
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("lower dictionary output");
        let copy_dict = output
            .record_batch()
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
            output_chunk_schema: Some(Arc::new(
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
            )),
            validate_final_result_input: false,
            declared_output: None,
            purpose_sources: None,
            purpose_storage: None,
            prepare_failure: None,
            construction: None,
            control: None,
            tracker: None,
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
        let output = run_declared_physical_fixture(&mut op, {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(17), SlotId::new(18)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        })
        .expect("project should succeed")
        .expect("project output");

        let c0 = output
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("output column 0");
        let c1 = output
            .record_batch()
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
        use novarocks_type_contract::FunctionValueType;
        let declaration = FunctionValueType::try_with_logical_type(
            DataType::Binary,
            false,
            ValueLogicalType::Hll,
        )
        .unwrap();
        let output_slot = SlotId::new(2);
        let mut arena = ExprArena::default();
        let expr = arena.push_value_typed(ExprNode::SlotId(SlotId::new(1)), declaration.clone());
        let slot = ChunkSlotSchema::try_new_typed(
            output_slot,
            Arc::new(Field::new("out", DataType::Binary, false)),
            declaration.typed_binding().clone(),
            None,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let mut op = ProjectProcessorOperator {
            name: "PROJECT".into(),
            arena: Arc::new(arena),
            exprs: vec![expr],
            expr_slot_ids: vec![output_slot],
            expr_slot_schemas: HashMap::from([(output_slot, slot.clone())]),
            output_indices: None,
            output_chunk_schema: Some(Arc::new(
                ChunkSchema::try_new_typed(vec![slot], ValueTypeStorageLifetime::pure()).unwrap(),
            )),
            validate_final_result_input: false,
            declared_output: None,
            purpose_sources: None,
            purpose_storage: None,
            prepare_failure: None,
            construction: None,
            control: None,
            tracker: None,
            pending_output: None,
            finishing: false,
            finished: false,
        };
        install_fixture_creator(&mut op);
        let input = typed_fixture_input_direct(
            Field::new("in", DataType::Binary, false),
            &declaration,
            Arc::new(BinaryArray::from(vec![&b"x"[..]])),
            SlotId::new(1),
        );
        let original = input.columns()[0].clone();
        let output = op.process_one(input).unwrap().unwrap();
        let slot = &output.chunk_schema().slots()[0];
        assert_eq!(slot.name(), "out");
        assert!(!slot.nullable());
        assert_eq!(
            slot.typed_binding().value_type().domain(),
            Some(ValueLogicalType::Hll)
        );
        assert!(slot.field().metadata().is_empty());
        assert!(Arc::ptr_eq(&original, &output.columns()[0]));
    }

    #[test]
    fn project_casts_final_output_to_declared_slot_type() {
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let expr = arena.push_typed(
            ExprNode::Cast(
                source,
                novarocks_type_contract::DecimalOverflowPolicy::ReportError,
            ),
            DataType::Int32,
        );
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
            output_chunk_schema: Some(Arc::clone(&output_chunk_schema)),
            validate_final_result_input: false,
            declared_output: None,
            purpose_sources: None,
            purpose_storage: None,
            prepare_failure: None,
            construction: None,
            control: None,
            tracker: None,
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

        let output = run_declared_physical_fixture(&mut op, input_chunk)
            .expect("project should succeed")
            .expect("project output");
        assert_eq!(
            output.record_batch().schema().field(0).data_type(),
            &DataType::Int32
        );
        let values = output
            .record_batch()
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
            output_chunk_schema: Some(Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    slot, field, None, None,
                )])
                .unwrap(),
            )),
            validate_final_result_input: true,
            declared_output: None,
            purpose_sources: None,
            purpose_storage: None,
            prepare_failure: None,
            construction: None,
            control: None,
            tracker: None,
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
        chunk.replace_record_batch_for_test(
            RecordBatch::try_new(Arc::new(Schema::new(vec![batch_field])), vec![actual]).unwrap(),
        );
        chunk
    }

    fn final_binary_input(
        batch_marker: Option<&str>,
        slot_marker: Option<&str>,
        cached: Option<ValueLogicalType>,
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
            Some(crate::exec::chunk::ChunkFieldSchema::from_value_type(
                ValueTypeRef::try_new(
                    cached.map_or(ValueKind::Binary, ValueKind::Nominal),
                    true,
                    ValueTypeStorageLifetime::pure(),
                )
                .unwrap(),
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
        // Corrupt only this explicit legacy fixture's actual batch.
        chunk.replace_record_batch_for_test(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![field(batch_marker)])),
                vec![array],
            )
            .unwrap(),
        );
        chunk
    }

    #[test]
    fn final_identity_rejects_actual_batch_slot_and_cache_conflicts_independently() {
        for (batch, slot, cached) in [
            (Some("hll"), None, None),
            (None, Some("hll"), None),
            (None, None, Some(ValueLogicalType::Hll)),
            (Some("bitmap"), Some("bitmap"), Some(ValueLogicalType::Hll)),
        ] {
            let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
            let error = op
                .process_one(final_binary_input(batch, slot, cached, false))
                .unwrap_err();
            assert!(error.detail().contains("logical domain differs"), "{error}");
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
                    assert!(error.detail().contains("unknown logical domain"), "{error}");
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
    fn final_identity_preserves_complete_domains_and_ignores_source_attributes_without_payload_copy()
     {
        use novarocks_type_contract::FunctionValueType;
        let declaration = FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            ValueLogicalType::Bitmap,
        )
        .unwrap();
        for attribute in [None, Some("bitmap"), Some("unknown provider attribute")] {
            for empty in [false, true] {
                let mut source_field = Field::new("source", DataType::Binary, true);
                if let Some(attribute) = attribute {
                    source_field = source_field
                        .with_metadata([("nr_logical_type".into(), attribute.into())].into());
                }
                let format_field = source_field.clone();
                let array: ArrayRef = if empty {
                    Arc::new(BinaryArray::from(Vec::<&[u8]>::new()))
                } else {
                    Arc::new(BinaryArray::from(vec![&b"unchanged state"[..]]))
                };
                let input = typed_fixture_input_direct(
                    source_field,
                    &declaration,
                    array.clone(),
                    SlotId::new(1),
                );
                let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
                configure_typed_identity(&mut op, &declaration);
                let output = op.process_one(input).unwrap().unwrap();
                assert_eq!(output.len(), usize::from(!empty));
                assert_eq!(
                    output.chunk_schema().slots()[0]
                        .typed_binding()
                        .value_type()
                        .domain(),
                    Some(ValueLogicalType::Bitmap)
                );
                assert!(output.schema().field(0).metadata().is_empty());
                if !empty {
                    assert!(Arc::ptr_eq(&array, &output.columns()[0]));
                }
                assert_eq!(
                    format_field
                        .metadata()
                        .get("nr_logical_type")
                        .map(String::as_str),
                    attribute
                );
            }
        }
    }

    #[test]
    fn final_identity_keeps_supported_dictionary_carriers() {
        use novarocks_type_contract::FunctionValueType;
        let declaration =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let array: ArrayRef = Arc::new(
            vec![Some("{}"), None, Some("[]"), Some("null")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        let input = typed_fixture_input_direct(
            Field::new("source", array.data_type().clone(), true),
            &declaration,
            array.clone(),
            SlotId::new(1),
        );
        let mut op = final_identity_operator(DataType::Utf8, LogicalType::Json);
        configure_typed_identity(&mut op, &declaration);
        let output = op.process_one(input).unwrap().unwrap();
        assert_eq!(output.len(), 4);
        assert!(Arc::ptr_eq(&array, &output.columns()[0]));
        assert_eq!(
            output.chunk_schema().slots()[0]
                .typed_binding()
                .value_type()
                .domain(),
            Some(ValueLogicalType::Json)
        );
        assert!(output.schema().field(0).metadata().is_empty());
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
            assert!(error.detail().contains("carrier differs"), "{error}");
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
                        Some(ChunkFieldSchema::from_field(&plain).unwrap()),
                        final_struct_array(&plain, empty),
                    );
                    let mut op = final_identity_operator_with_field(target.clone());
                    let error = op.process_one(input).unwrap_err();
                    assert!(error.detail().contains("logical domain"), "{error}");
                }
            }
            let plain = final_struct_field(None);
            let pure = ValueTypeStorageLifetime::pure();
            let cache = ChunkFieldSchema::from_value_type(
                ValueTypeRef::try_new(
                    ValueKind::Struct(
                        vec![ValueField::new(
                            "state",
                            ValueTypeRef::try_new(
                                ValueKind::Nominal(ValueLogicalType::Hll),
                                true,
                                pure.clone(),
                            )
                            .unwrap(),
                            pure.clone(),
                        )]
                        .into_boxed_slice(),
                    ),
                    true,
                    pure.clone(),
                )
                .unwrap(),
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
                    .detail()
                    .contains("logical domain differs")
            );

            let cache = ChunkFieldSchema::from_value_type(
                ValueTypeRef::try_new(
                    ValueKind::Struct(
                        ["state", "extra"]
                            .into_iter()
                            .map(|name| {
                                ValueField::new(
                                    name,
                                    ValueTypeRef::try_new(ValueKind::Binary, true, pure.clone())
                                        .unwrap(),
                                    pure.clone(),
                                )
                            })
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                    ),
                    true,
                    pure.clone(),
                )
                .unwrap(),
            );
            // A complete immutable semantic tree cannot hide a stale child
            // count inside an execution slot; reject it at checked binding.
            assert!(
                ChunkSlotSchema::try_new_with_field(
                    SlotId::new(1),
                    plain.clone(),
                    Some(cache),
                    None
                )
                .is_err()
            );
        }
    }

    #[test]
    fn final_identity_preserves_complete_nested_declaration_without_marker_hydration() {
        use novarocks_type_contract::{FunctionValueType, ValueLayout};
        let pure = ValueTypeStorageLifetime::pure();
        let declaration = FunctionValueType::try_from_value(
            ValueTypeRef::try_new(
                ValueKind::Struct(
                    vec![ValueField::new(
                        "state",
                        ValueTypeRef::try_new(
                            ValueKind::Nominal(ValueLogicalType::Bitmap),
                            true,
                            pure.clone(),
                        )
                        .unwrap(),
                        pure.clone(),
                    )]
                    .into_boxed_slice(),
                ),
                true,
                pure.clone(),
            )
            .unwrap(),
            ValueLayout::try_new(final_struct_field(None).data_type(), pure).unwrap(),
        )
        .unwrap();
        for empty in [false, true] {
            for marker in [None, Some("bitmap"), Some("unknown source attribute")] {
                let source_field = final_struct_field(marker);
                let array = final_struct_array(&source_field, empty);
                let input =
                    typed_fixture_input_direct(source_field, &declaration, array, SlotId::new(1));
                let original = input.columns()[0].clone();
                let mut op = final_identity_operator_with_field(final_struct_field(Some("bitmap")));
                configure_typed_identity(&mut op, &declaration);
                let output = op.process_one(input).unwrap().unwrap();
                assert_eq!(output.len(), usize::from(!empty));
                assert_eq!(
                    output.chunk_schema().slots()[0]
                        .typed_binding()
                        .value_type()
                        .child_type_at(0)
                        .unwrap()
                        .domain(),
                    Some(ValueLogicalType::Bitmap)
                );
                let DataType::Struct(children) = output.columns()[0].data_type() else {
                    panic!("struct");
                };
                assert!(children[0].metadata().is_empty());
                if !empty {
                    assert!(Arc::ptr_eq(&original, &output.columns()[0]));
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
                    .detail()
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
        let declaration =
            novarocks_type_contract::FunctionValueType::new(DataType::LargeUtf8, true);
        let input =
            typed_fixture_input_direct(field, &declaration, Arc::clone(&array), SlotId::new(1));
        let original = array.to_data();
        let mut op =
            final_identity_operator_with_field(Field::new("result", DataType::LargeUtf8, true));
        configure_typed_identity(&mut op, &declaration);
        assert!(op.accepts_encoded_column(SlotId::new(1), array.data_type()));
        op.validate_final_result_input = false;
        assert!(!op.accepts_encoded_column(SlotId::new(1), array.data_type()));
        op.validate_final_result_input = true;
        let output = op.process_one(input).unwrap().unwrap();
        assert_eq!(output.columns()[0].data_type(), array.data_type());
        assert_eq!(original, output.columns()[0].to_data());
        assert_eq!(
            output.chunk_schema().slots()[0]
                .typed_binding()
                .value_type()
                .domain(),
            None
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
    fn final_identity_refuses_nonidentity_or_indexed_layout_and_generic_domain_drift() {
        let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
        op.output_indices = Some(vec![0]);
        assert!(
            op.process_one(final_binary_input(None, None, None, false))
                .unwrap_err()
                .detail()
                .contains("identity output layout")
        );
        let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
        let mut arena = ExprArena::default();
        op.exprs[0] = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Binary);
        op.arena = Arc::new(arena);
        assert!(
            op.process_one(final_binary_input(None, None, None, false))
                .unwrap_err()
                .detail()
                .contains("identity expressions")
        );

        // Generic projections also require their actual complete source declaration.
        let mut op = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
        op.validate_final_result_input = false;
        let declaration = novarocks_type_contract::FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            ValueLogicalType::Bitmap,
        )
        .unwrap();
        configure_typed_identity(&mut op, &declaration);
        let source_declaration = novarocks_type_contract::FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            ValueLogicalType::Hll,
        )
        .unwrap();
        let input = typed_fixture_input_direct(
            Field::new("source", DataType::Binary, true),
            &source_declaration,
            Arc::new(BinaryArray::from(vec![&b"state"[..]])),
            SlotId::new(1),
        );
        assert!(op.process_one(input).is_err());

        use crate::exec::pipeline::operator_factory::OperatorFactory;
        let mut arena = ExprArena::default();
        let expr = arena.push_value_typed(ExprNode::SlotId(SlotId::new(1)), declaration.clone());
        let mut declared_operator = final_identity_operator(DataType::Binary, LogicalType::Bitmap);
        configure_typed_identity(&mut declared_operator, &declaration);
        let schema = declared_operator.output_chunk_schema.take().unwrap();
        let factory = super::ProjectProcessorFactory::from_fixture_schema(
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
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("project final-domain fixture");
        let runtime_factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        operator.set_construction_factory(runtime_factory);
        operator.prepare().unwrap();
        operator
            .bind_runtime_state(&crate::runtime::runtime_state::RuntimeState::default())
            .unwrap();
        let input = typed_fixture_input_direct(
            Field::new("source", DataType::Binary, true),
            &source_declaration,
            Arc::new(BinaryArray::from(vec![&b"state"[..]])),
            SlotId::new(1),
        );
        let error = operator
            .as_processor_mut()
            .unwrap()
            .push_chunk(
                &crate::runtime::runtime_state::RuntimeState::default(),
                input,
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("typed final result input semantics differ from output semantics"),
            "{error}"
        );
    }
    #[test]
    fn numeric_arithmetic_project_preserves_concrete_carriers_through_the_root_boundary() {
        use crate::exec::chunk::{
            RootArrayStorageError, RootArrayStorageLimits, borrowed_root_array_storage,
            borrowed_root_chunk_storage,
        };
        use arrow::array::Float64Array;
        use novarocks_type_contract::DecimalOverflowPolicy;

        let limits = RootArrayStorageLimits {
            bytes: 1024 * 1024,
            nodes: 256,
            depth: 64,
        };
        // The first subtraction is the original count(*) - count(DISTINCT
        // _row_id) shape. Other rows retain both left and right NULL behavior.
        let cases = [
            ("add", 12, 12, 12.0, 12.0),
            ("subtract", 0, 2, 0.0, 2.0),
            ("multiply", 36, 35, 36.0, 35.0),
            ("divide", 1, 1, 1.0, 1.4),
            ("modulo", 0, 2, 0.0, 2.0),
        ];
        // Exercise integer and floating kernels, plus SQL's integral inputs
        // with a declared floating division result. No cast hides an outer Arc.
        for mode in 0..3 {
            for (operation, first_int, last_int, first_float, last_float) in cases {
                if mode == 2 && operation != "divide" {
                    continue;
                }
                let left = SlotId::new(2);
                let right = SlotId::new(3);
                let input_type = if mode == 1 {
                    DataType::Float64
                } else {
                    DataType::Int64
                };
                let output_type = if mode == 0 {
                    DataType::Int64
                } else {
                    DataType::Float64
                };
                let mut arena = ExprArena::default();
                let a = arena.push_typed(ExprNode::SlotId(left), input_type.clone());
                let b = arena.push_typed(ExprNode::SlotId(right), input_type.clone());
                let policy = DecimalOverflowPolicy::ReportError;
                let expression = match operation {
                    "add" => ExprNode::Add(a, b, policy),
                    "subtract" => ExprNode::Sub(a, b, policy),
                    "multiply" => ExprNode::Mul(a, b, policy),
                    "divide" => ExprNode::Div(a, b, policy),
                    "modulo" => ExprNode::Mod(a, b, policy),
                    _ => unreachable!("closed arithmetic fixture"),
                };
                let expression = arena.push_typed(expression, output_type.clone());
                // This fixture installs the same construction owner and actual
                // Project processing as the existing component tests.
                let mut project = final_identity_operator_with_field(Field::new(
                    "arithmetic_result",
                    output_type.clone(),
                    true,
                ));
                project.arena = Arc::new(arena);
                project.exprs = vec![expression];
                project.validate_final_result_input = false;
                let schema = Arc::new(Schema::new(vec![
                    Field::new("count_all", input_type.clone(), true),
                    Field::new("count_distinct", input_type, true),
                ]));
                let columns: Vec<ArrayRef> = if mode == 1 {
                    vec![
                        Arc::new(Float64Array::from(vec![
                            Some(6.0),
                            None,
                            Some(7.0),
                            Some(6.0),
                        ])),
                        Arc::new(Float64Array::from(vec![
                            Some(6.0),
                            Some(2.0),
                            Some(5.0),
                            None,
                        ])),
                    ]
                } else {
                    vec![
                        Arc::new(Int64Array::from(vec![Some(6), None, Some(7), Some(6)])),
                        Arc::new(Int64Array::from(vec![Some(6), Some(2), Some(5), None])),
                    ]
                };
                let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
                let input =
                    Chunk::new_with_chunk_schema(batch, chunk_schema_of(&schema, &[left, right]));
                let output = run_declared_physical_fixture(&mut project, input)
                    .unwrap()
                    .unwrap();
                if mode == 0 {
                    let values = output.columns()[0]
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert_eq!(
                        values,
                        &Int64Array::from(vec![Some(first_int), None, Some(last_int), None])
                    );
                } else {
                    let values = output.columns()[0]
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    assert_eq!(
                        values,
                        &Float64Array::from(vec![Some(first_float), None, Some(last_float), None])
                    );
                }
                borrowed_root_chunk_storage(&output, limits).expect(
                    "actual arithmetic Project output must expose a concrete standard carrier",
                );
                let mut final_identity = final_identity_operator_with_field(Field::new(
                    "arithmetic_result",
                    output_type,
                    true,
                ));
                let output = run_declared_physical_fixture(&mut final_identity, output)
                    .unwrap()
                    .unwrap();
                borrowed_root_chunk_storage(&output, limits)
                    .expect("the final identity preserves the actual arithmetic array owner");
                let wrapped: ArrayRef = Arc::new(output.columns()[0].clone());
                assert_eq!(
                    borrowed_root_array_storage(wrapped.as_ref(), limits),
                    Err(RootArrayStorageError::CarrierOuterOwnerMismatch),
                    "producer correctness must not weaken root outer-owner rejection",
                );
            }
        }
    }
}

#[cfg(test)]
mod owned_project_tests {
    use super::*;
    use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
    use arrow::array::{Array, DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_type_contract::{
        FunctionValueType, TypedValueBinding, ValueField, ValueKind, ValueLayout, ValueTypeRef,
        ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CompileControl;
    impl novarocks_type_contract::PureCompileControl for CompileControl {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }
    fn frozen_project_fixture() -> (ProjectProcessorFactory, Chunk) {
        use novarocks_local_program::{
            ProjectExpressionSlot, ProjectExpressionSlots, StaticLayout,
        };
        let ty = FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            true,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap();
        let binding = ty.typed_binding().clone();
        let field = Arc::new(Field::new("alias", DataType::Utf8, true));
        let output = StaticLayout::try_new_typed_for_compile(
            Arc::new(arrow::datatypes::Schema::new(vec![field.clone()])),
            Arc::from([SlotId::new(9)]),
            vec![(binding.clone(), None)],
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &CompileControl,
        )
        .unwrap();
        let purposes = ProjectExpressionSlots::try_from_boxed_observed::<
            novarocks_type_contract::owned_resources::layout::LayoutResourceError,
        >(
            vec![ProjectExpressionSlot {
                slot_id: SlotId::new(9),
                field,
                binding: binding.clone(),
                storage: ValueTypeStorageLifetime::pure(),
                unique_id: None,
            }]
            .into_boxed_slice(),
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap();
        let mut arena = ExprArena::default();
        let expr = arena.push_value_typed(ExprNode::SlotId(SlotId::new(7)), ty);
        let factory = ProjectProcessorFactory::new(
            41,
            false,
            Arc::new(arena),
            vec![expr],
            vec![SlotId::new(9)],
            Some(purposes),
            None,
            output,
        );
        let input_slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(7),
            Arc::new(Field::new("source", DataType::Utf8, true)),
            binding,
            None,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let input = Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            Arc::new(
                ChunkSchema::try_new_typed(vec![input_slot], ValueTypeStorageLifetime::pure())
                    .unwrap(),
            ),
            vec![Arc::new(StringArray::from(vec!["{}", "[]"]))],
            2,
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap();
        (factory, input)
    }
    fn frozen_hour_project_fixture() -> (ProjectProcessorFactory, Chunk, Vec<Option<i32>>) {
        use crate::exec::expr::function::{FUNCTION_REGISTRY, FunctionKind};
        use novarocks_local_program::{
            ProjectExpressionSlot, ProjectExpressionSlots, StaticExprKind, StaticFunctionKind,
            StaticLayout,
        };
        let source = FunctionValueType::new(DataType::Int64, true);
        let result = FunctionValueType::new(DataType::Int32, true);
        let field = Arc::new(Field::new("hour", DataType::Int32, true));
        let output = StaticLayout::try_new_typed_for_compile(
            Arc::new(arrow::datatypes::Schema::new(vec![field.clone()])),
            Arc::from([SlotId::new(10)]),
            vec![(result.typed_binding().clone(), None)],
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &CompileControl,
        )
        .unwrap();
        let purposes = ProjectExpressionSlots::try_from_boxed_observed::<
            novarocks_type_contract::owned_resources::layout::LayoutResourceError,
        >(
            [9, 10]
                .into_iter()
                .map(|id| ProjectExpressionSlot {
                    slot_id: SlotId::new(id),
                    field: field.clone(),
                    binding: result.typed_binding().clone(),
                    storage: ValueTypeStorageLifetime::pure(),
                    unique_id: None,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap();
        let input_schema = Arc::new(
            ChunkSchema::try_new_typed(
                vec![
                    ChunkSlotSchema::try_new_typed(
                        SlotId::new(7),
                        Arc::new(Field::new("epoch", DataType::Int64, true)),
                        source.typed_binding().clone(),
                        None,
                        ValueTypeStorageLifetime::pure(),
                    )
                    .unwrap(),
                ],
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        );
        let input = Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            input_schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![
                Some(-1),
                None,
                Some(0),
                Some(3600),
            ]))],
            4,
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap();
        let mut arena = ExprArena::default();
        let child = arena.push_value_typed(ExprNode::SlotId(SlotId::new(7)), source);
        let kind = *FUNCTION_REGISTRY.get("hour_from_unixtime").unwrap();
        assert_eq!(kind, FunctionKind::HourFromUnixtime);
        let hour = arena.push_value_typed(
            ExprNode::FunctionCall {
                kind,
                args: vec![child],
            },
            result.clone(),
        );
        let cse = arena.push_value_typed(ExprNode::SlotId(SlotId::new(9)), result);
        // Preserve the pre-existing Local contract and original internal Int64
        // oracle; this test changes only the fixed public realization.
        let internal = arena.eval(hour, &input).unwrap();
        assert_eq!(internal.data_type(), &DataType::Int64);
        let expected = internal
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .iter()
            .map(|value| value.map(|hour| i32::try_from(hour).unwrap()))
            .collect();
        let frozen = arena.into_immutable().unwrap();
        assert!(matches!(
            frozen.nodes()[hour.0].kind(),
            StaticExprKind::FunctionCall {
                kind: StaticFunctionKind::HourFromUnixtime,
                ..
            }
        ));
        let arena = ExprArena::from_immutable(&frozen).unwrap();
        let factory = ProjectProcessorFactory::new(
            41,
            false,
            Arc::new(arena),
            vec![hour, cse],
            vec![SlotId::new(9), SlotId::new(10)],
            Some(purposes),
            Some(vec![1]),
            output,
        );
        (factory, input, expected)
    }

    #[test]
    fn project_hour_public_int32_realization_precedes_cse_and_retains_last_output() {
        let (factory, input, expected) = frozen_hour_project_fixture();
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("Project HOUR public result");
        let creation = real_project_creator(tracker.clone());
        let mut operator = factory.create_operator();
        operator.set_construction_factory(creation.clone());
        operator.prepare().unwrap();
        operator
            .bind_runtime_state(&RuntimeState::default())
            .unwrap();
        operator
            .push_chunk(&RuntimeState::default(), input)
            .unwrap();
        let output = operator
            .pull_chunk(&RuntimeState::default())
            .unwrap()
            .unwrap();
        let values = output.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), expected);
        assert_eq!(&expected[..2], &[None, None]);
        assert_eq!(
            output.chunk_schema().slots()[0].data_type(),
            &DataType::Int32
        );
        let projection = output.into_batch_projection();
        drop(operator);
        drop(factory);
        drop(creation);
        assert!(tracker.current() > 0);
        assert_eq!(projection.record_batch().num_rows(), 4);
        drop(projection);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn project_hour_public_realization_retains_original_creator_refusal_and_control() {
        use crate::exec::chunk::ArrayNormalizationStep;
        use novarocks_functions::selected_copy::CopyConstructionError;
        for cancelled in [false, true] {
            let (factory, input, _) = frozen_hour_project_fixture();
            let tracker = crate::runtime::mem_tracker::MemTracker::new_root("Project HOUR refusal");
            let errors = Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default());
            let creation = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                tracker.clone(), errors.clone(),
            ).unwrap();
            let mut operator = factory.create_operator();
            operator.set_construction_factory(creation.clone());
            operator.prepare().unwrap();
            operator
                .bind_runtime_state(&RuntimeState::default())
                .unwrap();
            let owner = operator
                .control
                .as_ref()
                .unwrap()
                .construction_owner(operator.prepared_output().unwrap())
                .unwrap();
            let source = Arc::new(arrow::array::Int64Array::from(vec![
                Some(0),
                None,
                Some(23),
            ])) as ArrayRef;
            let stop = crate::runtime::fragment::ExecutionFailure::from(
                novarocks_functions::KernelFailure::Cancelled,
            );
            if cancelled {
                errors.set_error(stop.clone());
            } else {
                tracker.install_limit_once(tracker.current()).unwrap();
            }
            let allocated = tracker.allocated();
            let retained = tracker.current();
            let mut request = |layout| owner.request(layout);
            let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
            let mut scope = novarocks_type_contract::ValueSchemaCreation::new(
                owner.storage_lifetime(),
                &mut request,
                &mut opaque,
            );
            let error =
                novarocks_functions::builtin::calendar_unixtime::realize_legacy_hour_public_result(
                    &source,
                    &FunctionValueType::new(DataType::Int32, true),
                    &mut scope,
                    &mut |_| {
                        owner.observe(
                            0,
                            &[],
                            ArrayNormalizationStep::InspectConstructionWork { units: 1 },
                        )
                    },
                )
                .unwrap_err();
            match error {
                CopyConstructionError::Creator(cause) if cancelled => {
                    assert_eq!(
                        crate::runtime::fragment::ExecutionFailure::from(cause),
                        stop
                    )
                }
                CopyConstructionError::Creator(ConstructionFailure::Memory(_)) if !cancelled => {}
                other => panic!("unexpected original HOUR creation failure: {other:?}"),
            }
            if cancelled {
                assert_eq!(
                    tracker.allocated(),
                    allocated,
                    "control stops before a request"
                );
            }
            // The tracker counts attempted requests too. A rejected request
            // must refund current responsibility, not erase its attempt.
            assert_eq!(
                tracker.current(),
                retained,
                "refusal retains no new storage"
            );
            drop(scope);
            drop(owner);
            drop(operator);
            drop(creation);
            drop(factory);
            drop(input);
            drop(errors);
            assert_eq!(tracker.current(), 0);
        }
    }

    fn real_project_creator(
        tracker: Arc<crate::runtime::mem_tracker::MemTracker>,
    ) -> Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory> {
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        )
        .unwrap()
    }

    #[test]
    fn project_prepare_requires_actual_factory_before_input() {
        let (factory, input) = frozen_project_fixture();
        let mut operator = factory.create_operator();
        assert!(operator.output_chunk_schema.is_none());
        assert!(operator.expr_slot_schemas.is_empty());
        let failure = operator.prepare().unwrap_err();
        assert_eq!(
            failure.cause(),
            &crate::runtime::fragment::ExecutionFailureCause::Construction(
                ConstructionFailure::MissingOwner
            )
        );
        let original = input.columns()[0].clone();
        let failure_after_input = operator
            .push_chunk(&RuntimeState::default(), input)
            .unwrap_err();
        assert_eq!(failure_after_input, failure);
        assert_eq!(
            original
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "{}"
        );
        assert!(operator.pending_output.is_none());
        assert!(operator.expr_slot_schemas.is_empty());
    }

    #[test]
    fn project_prepare_first_real_header_refusal_rolls_back_and_stops_reentry() {
        let (factory, input) = frozen_project_fixture();
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("Project prepare refusal");
        let creation = real_project_creator(tracker.clone());
        let baseline = tracker.current();
        tracker.install_limit_once(baseline + 1).unwrap();
        let mut operator = factory.create_operator();
        operator.set_construction_factory(creation.clone());
        let failure = operator.prepare().unwrap_err();
        assert!(matches!(
            failure.cause(),
            crate::runtime::fragment::ExecutionFailureCause::Construction(
                ConstructionFailure::Memory(_)
            )
        ));
        assert_eq!(tracker.current(), baseline);
        let allocated = tracker.allocated();
        assert_eq!(operator.prepare().unwrap_err(), failure);
        assert_eq!(
            operator
                .push_chunk(&RuntimeState::default(), input)
                .unwrap_err(),
            failure
        );
        assert_eq!(tracker.allocated(), allocated);
        assert!(operator.output_chunk_schema.is_none());
        assert!(operator.expr_slot_schemas.is_empty());
        assert!(operator.pending_output.is_none());
        drop(operator);
        drop(creation);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn project_prepared_independent_slot_keeps_only_finite_creator_until_last_reference() {
        let (factory, input) = frozen_project_fixture();
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("Project independent purpose");
        let creation = real_project_creator(tracker.clone());
        let mut operator = factory.create_operator();
        operator.set_construction_factory(creation.clone());
        operator.prepare().unwrap();
        assert!(operator.declared_output.is_none());
        assert!(operator.purpose_sources.is_none());
        assert_eq!(operator.expr_slot_schemas.len(), 1);
        let slot = operator.prepared_output().unwrap().slots()[0].clone();
        let purpose = operator.expr_slot_schemas[&SlotId::new(9)].clone();
        let source_array = Arc::downgrade(&input.columns()[0]);
        assert_eq!(
            slot.typed_binding().value_type().kind(),
            &ValueKind::Nominal(novarocks_type_contract::ValueLogicalType::Json)
        );
        assert_eq!(slot.data_type(), &DataType::Utf8);
        assert!(slot.nullable());
        drop(operator);
        drop(factory);
        drop(input);
        drop(creation);
        assert!(source_array.upgrade().is_none());
        assert!(tracker.current() > 0);
        drop(slot);
        assert!(tracker.current() > 0);
        assert_eq!(
            purpose.typed_binding().value_type().kind(),
            &ValueKind::Nominal(novarocks_type_contract::ValueLogicalType::Json)
        );
        drop(purpose);
        assert_eq!(tracker.current(), 0);
    }

    struct Release(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Release {}
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Owner {
        storage: ValueTypeStorageLifetime,
        requests: AtomicUsize,
        work: AtomicUsize,
        opaque: AtomicUsize,
        work_at_refusal: AtomicUsize,
        refuse: bool,
    }
    impl InputConstructionOwner for Owner {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            self.storage.clone()
        }
        fn request(&self, _layout: std::alloc::Layout) -> Result<(), ConstructionFailure> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            if self.refuse {
                self.work_at_refusal
                    .store(self.work.load(Ordering::SeqCst), Ordering::SeqCst);
                return Err(ConstructionFailure::from(
                    crate::runtime::fragment::ExecutionFailure::from(
                        novarocks_functions::KernelFailure::DeadlineExceeded,
                    ),
                ));
            }
            Ok(())
        }
        fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
            self.opaque.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }
        fn observe(
            &self,
            _ordinal: usize,
            _path: &[crate::exec::chunk::ArrayPathStep],
            step: crate::exec::chunk::ArrayNormalizationStep<'_>,
        ) -> Result<(), ConstructionFailure> {
            novarocks_functions::array_projection::observe_array_normalization_construction(
                step,
                &mut |layout| self.request(layout),
                &mut |bound| self.observe_opaque_upper_bound(bound.bytes_upper),
                &mut |units| {
                    self.work.fetch_add(units, Ordering::SeqCst);
                    Ok(())
                },
            )
        }
    }
    fn owner(storage: ValueTypeStorageLifetime, refuse: bool) -> Owner {
        Owner {
            storage,
            requests: AtomicUsize::new(0),
            work: AtomicUsize::new(0),
            opaque: AtomicUsize::new(0),
            work_at_refusal: AtomicUsize::new(0),
            refuse,
        }
    }
    fn string_binding(nullable: bool, storage: ValueTypeStorageLifetime) -> TypedValueBinding {
        TypedValueBinding::try_new(
            ValueTypeRef::try_new(ValueKind::String, nullable, storage.clone()).unwrap(),
            ValueLayout::try_new(&DataType::Utf8, storage).unwrap(),
        )
        .unwrap()
    }
    fn declared(binding: &TypedValueBinding) -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new_typed(
                vec![
                    ChunkSlotSchema::try_new_typed(
                        SlotId::new(9),
                        Arc::new(Field::new(
                            "alias",
                            binding.data_type().clone(),
                            binding.nullable(),
                        )),
                        binding.clone(),
                        None,
                        ValueTypeStorageLifetime::pure(),
                    )
                    .unwrap(),
                ],
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        )
    }
    #[allow(deprecated)]
    fn dictionary_source(keys: Int32Array, values: ArrayRef, binding: &TypedValueBinding) -> Chunk {
        let array: ArrayRef =
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(7),
            Arc::new(Field::new_dict(
                "source",
                array.data_type().clone(),
                binding.nullable(),
                17,
                true,
            )),
            binding.clone(),
            None,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            Arc::new(
                ChunkSchema::try_new_typed(vec![slot], ValueTypeStorageLifetime::pure()).unwrap(),
            ),
            vec![array],
            3,
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
        )
        .unwrap()
    }
    struct Control;
    impl crate::exec::expr::ExprEvaluationControl for Control {
        fn checkpoint(&self) -> ExecutionResult<()> {
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> ExecutionResult<()> {
            Ok(())
        }
    }
    impl novarocks_functions::KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
            Ok(())
        }
    }

    #[test]
    fn project_owned_first_creator_refusal_preserves_nominal_cause() {
        let binding = string_binding(true, ValueTypeStorageLifetime::pure());
        let source = dictionary_source(
            Int32Array::from(vec![Some(0), None, Some(1)]),
            Arc::new(StringArray::from(vec!["A", "B"])),
            &binding,
        );
        let owner = owner(ValueTypeStorageLifetime::pure(), true);
        let failure = materialize_project_output_owned(
            vec![source.columns()[0].clone()],
            3,
            declared(&binding),
            |_| Ok(&binding),
            |_| {
                Ok(ProjectEncodingSource::OriginalField(
                    source.chunk_schema().slots()[0].field(),
                ))
            },
            owner.storage_lifetime(),
            &owner,
        )
        .unwrap_err();
        assert_eq!(
            failure.cause(),
            &crate::runtime::fragment::ExecutionFailureCause::Kernel(
                novarocks_functions::KernelFailure::DeadlineExceeded
            )
        );
        assert_eq!(owner.requests.load(Ordering::SeqCst), 1);
        assert_eq!(
            owner.work.load(Ordering::SeqCst),
            owner.work_at_refusal.load(Ordering::SeqCst)
        );
    }

    #[test]
    #[allow(deprecated)]
    fn project_owned_independent_slot_keeps_original_and_new_creator_until_last_reference() {
        let old_released = Arc::new(AtomicUsize::new(0));
        let new_released = Arc::new(AtomicUsize::new(0));
        let binding = string_binding(
            true,
            ValueTypeStorageLifetime::held(Arc::new(Release(old_released.clone()))),
        );
        let source = dictionary_source(
            Int32Array::from(vec![Some(0), None, Some(1)]),
            Arc::new(StringArray::from(vec!["A", "B"])),
            &binding,
        );
        let owner = owner(
            ValueTypeStorageLifetime::held(Arc::new(Release(new_released.clone()))),
            false,
        );
        let storage = source
            .source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                owner.request(layout)
            })
            .unwrap();
        let output = materialize_project_output_owned(
            vec![source.columns()[0].clone()],
            3,
            declared(&binding),
            |_| Ok(&binding),
            |_| {
                Ok(ProjectEncodingSource::OriginalField(
                    source.chunk_schema().slots()[0].field(),
                ))
            },
            storage,
            &owner,
        )
        .unwrap();
        let independent_slot = output.chunk_schema().slots()[0].clone();
        assert_eq!(independent_slot.name(), "alias");
        assert_eq!(independent_slot.field().dict_id(), Some(17));
        assert_eq!(independent_slot.field().dict_is_ordered(), Some(true));
        assert!(Arc::ptr_eq(&output.columns()[0], &source.columns()[0]));
        drop(output);
        drop(source);
        drop(binding);
        drop(owner);
        assert_eq!(old_released.load(Ordering::SeqCst), 0);
        assert_eq!(new_released.load(Ordering::SeqCst), 0);
        drop(independent_slot);
        assert_eq!(old_released.load(Ordering::SeqCst), 1);
        assert_eq!(new_released.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn project_owned_rejects_nested_nominal_erasure_before_reconstruction() {
        let storage = ValueTypeStorageLifetime::pure();
        let json = ValueTypeRef::try_new(
            ValueKind::Nominal(novarocks_type_contract::ValueLogicalType::Json),
            true,
            storage.clone(),
        )
        .unwrap();
        let text = ValueTypeRef::try_new(ValueKind::String, true, storage.clone()).unwrap();
        let layout =
            DataType::Struct(vec![Arc::new(Field::new("member", DataType::Utf8, true))].into());
        let author = |member| {
            FunctionValueType::try_from_value(
                ValueTypeRef::try_new(
                    ValueKind::Struct(
                        vec![ValueField::new("member", member, storage.clone())].into(),
                    ),
                    true,
                    storage.clone(),
                )
                .unwrap(),
                ValueLayout::try_new(&layout, storage.clone()).unwrap(),
            )
            .unwrap()
        };
        let source = author(json);
        let target = author(text);
        let column: ArrayRef = Arc::new(arrow::array::StructArray::new(
            match &layout {
                DataType::Struct(fields) => fields.clone(),
                _ => unreachable!(),
            },
            vec![Arc::new(StringArray::from(vec!["[]"]))],
            None,
        ));
        let owner = owner(storage.clone(), false);
        assert!(
            materialize_project_output_owned(
                vec![column],
                1,
                declared(target.typed_binding()),
                |_| Ok(source.typed_binding()),
                |_| Ok(ProjectEncodingSource::Plain),
                storage,
                &owner
            )
            .is_err()
        );
        assert_eq!(owner.requests.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn project_owned_required_values_never_late_widen_on_actual_null() {
        let binding = string_binding(false, ValueTypeStorageLifetime::pure());
        let column: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        let owner = owner(ValueTypeStorageLifetime::pure(), false);
        assert!(
            materialize_project_output_owned(
                vec![column],
                1,
                declared(&binding),
                |_| Ok(&binding),
                |_| Ok(ProjectEncodingSource::Plain),
                owner.storage_lifetime(),
                &owner
            )
            .is_err()
        );
    }

    #[test]
    fn project_owned_peel_preserves_unused_null_fallback_and_signed_take_selection() {
        let required = string_binding(false, ValueTypeStorageLifetime::pure());
        let nullable = string_binding(true, ValueTypeStorageLifetime::pure());
        let owner = owner(ValueTypeStorageLifetime::pure(), false);
        let mut arena = ExprArena::default();
        let source = arena.push_value_typed(
            ExprNode::SlotId(SlotId::new(7)),
            FunctionValueType::try_from_typed_binding(required.clone()).unwrap(),
        );
        let lower = arena.push_value_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::String("lower"),
                args: vec![source],
            },
            FunctionValueType::try_from_typed_binding(required.clone()).unwrap(),
        );
        let unused_null = dictionary_source(
            Int32Array::from(vec![0, 2, 0]),
            Arc::new(StringArray::from(vec![Some("PAID"), None, Some("New")])),
            &required,
        );
        assert!(
            try_peel_dict_expr_owned(&arena, lower, &unused_null, "lower", &owner, &Control)
                .unwrap()
                .is_none()
        );
        let hydrated = crate::exec::chunk::hydrate_dictionary_columns_owned_observed::<
            ConstructionFailure,
            _,
            _,
            _,
        >(
            &unused_null,
            owner.storage_lifetime(),
            |_, _, _, _| false,
            |ordinal, path, step| owner.observe(ordinal, path, step),
            |layout| owner.request(layout),
        )
        .unwrap();
        let result = arena.eval(lower, &hydrated).unwrap();
        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![Some("paid"), Some("new"), Some("paid")]
        );
        let mut arena = ExprArena::default();
        let source = arena.push_value_typed(
            ExprNode::SlotId(SlotId::new(7)),
            FunctionValueType::try_from_typed_binding(nullable.clone()).unwrap(),
        );
        let length = arena.push_value_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::String("length"),
                args: vec![source],
            },
            FunctionValueType::new(DataType::Int64, true),
        );
        let keys = Int32Array::new(
            arrow::buffer::ScalarBuffer::from(vec![1, 2, -1, 0]),
            Some(arrow::buffer::NullBuffer::from(vec![
                true, true, false, true,
            ])),
        )
        .slice(1, 3);
        let signed = dictionary_source(
            keys,
            Arc::new(StringArray::from(vec!["PAID", "Other", "New"])),
            &nullable,
        );
        let peeled = try_peel_dict_expr_owned(&arena, length, &signed, "length", &owner, &Control)
            .unwrap()
            .unwrap();
        assert!(peeled.fresh_field.is_none());
        let lengths = peeled
            .array
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(
            lengths.iter().collect::<Vec<_>>(),
            vec![Some(3), None, Some(4)]
        );
        assert!(owner.opaque.load(Ordering::SeqCst) > 0);
        // A contradictory full source declaration cannot be upgraded while
        // signing either an identity CSE or the PEEL values working port.
        let json = FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            true,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap();
        let mut wrong_arena = ExprArena::default();
        let wrong_source =
            wrong_arena.push_value_typed(ExprNode::SlotId(SlotId::new(7)), json.clone());
        let wrong_lower = wrong_arena.push_value_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::String("lower"),
                args: vec![wrong_source],
            },
            FunctionValueType::try_from_typed_binding(nullable.clone()).unwrap(),
        );
        let before_requests = owner.requests.load(Ordering::SeqCst);
        assert!(
            try_peel_dict_expr_owned(
                &wrong_arena,
                wrong_lower,
                &signed,
                "lower",
                &owner,
                &Control
            )
            .is_err()
        );
        let op = ProjectProcessorOperator {
            name: "TEST_PROJECT".into(),
            arena: Arc::new(wrong_arena),
            exprs: vec![wrong_source],
            expr_slot_ids: vec![SlotId::new(9)],
            expr_slot_schemas: HashMap::new(),
            output_indices: None,
            output_chunk_schema: Some(declared(json.typed_binding())),
            validate_final_result_input: false,
            declared_output: None,
            purpose_sources: None,
            purpose_storage: None,
            prepare_failure: None,
            construction: None,
            control: Some(
                super::super::compiled_expression::RuntimeKernelControl::new(Arc::new(
                    crate::runtime::runtime_state::RuntimeErrorState::default(),
                )),
            ),
            tracker: None,
            pending_output: None,
            finishing: false,
            finished: false,
        };
        assert!(
            op.eval_project_expr(wrong_source, &signed, "alias", &owner)
                .is_err()
        );
        assert_eq!(owner.requests.load(Ordering::SeqCst), before_requests);

        assert!(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![-1]),
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef
            )
            .is_err()
        );
    }
}
