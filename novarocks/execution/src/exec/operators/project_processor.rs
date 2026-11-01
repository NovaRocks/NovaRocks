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
//! Declared projection materialization shared by the compiled expression owner.
//! Complete bindings, encoding provenance and construction storage stay explicit.

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::runtime::fragment::ExecutionResult;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use novarocks_types::SlotId;
use std::sync::Arc;

#[cfg(test)]
fn projected_slot_schema(
    existing: &ChunkSlotSchema,
    slot_id: SlotId,
    data_type: &DataType,
    actual_nullable: bool,
) -> ExecutionResult<ChunkSlotSchema> {
    let derived = if existing.metadata_origins().is_some()
        && crate::exec::chunk::type_compatibility::check_exact(existing.data_type(), data_type)
            .is_ok()
    {
        existing.reconcile_to_carrier(data_type, actual_nullable)?
    } else {
        existing
            .with_type_and_nullable(data_type.clone(), existing.nullable() || actual_nullable)?
    };
    Ok(derived.with_slot_id(slot_id)?)
}

#[cfg(test)]
fn projected_chunk_schema(
    slots: Vec<ChunkSlotSchema>,
    declared: &ChunkSchema,
) -> ExecutionResult<ChunkSchemaRef> {
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
fn is_final_result_string_dictionary(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Dictionary(key, value)
        if key.as_ref() == &DataType::Int32
            && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8))
}

/// Typed failure retained by the actual compiled final-result validator.
#[derive(Debug)]
struct IdentityInputFailure(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for IdentityInputFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for IdentityInputFailure {}
impl novarocks_failure_contract::FailureProjection for IdentityInputFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::FinalIdentityInput.reason(),
        )
    }
}
fn identity_input_failure(
    detail: impl std::fmt::Display,
) -> crate::runtime::fragment::ExecutionFailure {
    crate::runtime::fragment::ExecutionFailure::from_domain(IdentityInputFailure(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}

pub(crate) fn validate_final_result_identity_input_source(
    chunk: &Chunk,
    output: &ChunkSchema,
    identity_layout: bool,
    pair_count: usize,
    pairs: impl Iterator<Item = ExecutionResult<(SlotId, SlotId)>>,
) -> ExecutionResult<()> {
    use novarocks_type_contract::ValueLogicalType;
    use novarocks_types::logical::NR_LOGICAL_TYPE_KEY;

    fn marker(field: &arrow::datatypes::Field) -> ExecutionResult<Option<ValueLogicalType>> {
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
        Err(identity_input_failure(
            "final result input contains an unknown logical domain marker",
        ))
    }

    fn check_field(
        source: &arrow::datatypes::Field,
        target: &arrow::datatypes::Field,
    ) -> ExecutionResult<()> {
        let actual = marker(source)?;
        let expected = marker(target)?;
        if actual.is_some() && actual != expected {
            return Err(identity_input_failure(
                "final result input logical domain differs from output domain",
            ));
        }
        check_type(source.data_type(), target.data_type())
    }

    fn check_type(source: &DataType, target: &DataType) -> ExecutionResult<()> {
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
            ) => Err(identity_input_failure(
                "final result input carrier differs from output carrier",
            )),
            // Native result descriptors do not admit these opaque recursive carriers.
            (D::Union(_, _) | D::RunEndEncoded(_, _), _)
            | (_, D::Union(_, _) | D::RunEndEncoded(_, _)) => Err(identity_input_failure(
                "final result input carrier differs from output carrier",
            )),
            _ if source == target => Ok(()),
            _ => Err(identity_input_failure(
                "final result input carrier differs from output carrier",
            )),
        }
    }

    fn check_cached(
        source: &novarocks_type_contract::ValueTypeRef,
        target: &arrow::datatypes::Field,
    ) -> ExecutionResult<()> {
        let actual = source.domain();
        if actual.is_some() && actual != marker(target)? {
            return Err(identity_input_failure(
                "final result input logical domain differs from output domain",
            ));
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
                    return Err(identity_input_failure(
                        "final result input semantic shape differs from output shape",
                    ));
                };
                if children.len() != 2 || source.child_count() != 2 {
                    return Err(identity_input_failure(
                        "final result input semantic shape differs from output shape",
                    ));
                }
                children
            }
            _ => {
                return Err(identity_input_failure(
                    "final result input semantic shape differs from output shape",
                ));
            }
        };
        for (index, target) in children.iter().enumerate() {
            check_cached(source.child_type_at(index).expect("checked child"), target)?;
        }
        Ok(())
    }

    if !identity_layout || pair_count != output.slots().len() {
        return Err(identity_input_failure(
            "final result input validation requires an identity output layout",
        ));
    }
    let batch_schema = chunk.record_batch().schema();
    for (pair, target) in pairs.zip(output.slots()) {
        let (source_slot, output_slot) = pair?;
        if output_slot != target.slot_id() || source_slot != output_slot {
            return Err(identity_input_failure(
                "final result input validation requires exact identity slots",
            ));
        }
        let expected = if target.is_typed() {
            target.field_schema().logical_type()
        } else {
            marker(target.field())?
        };
        if target.field_schema().logical_type() != expected {
            return Err(identity_input_failure(
                "final result output contains conflicting logical domain facts",
            ));
        }
        let index = chunk
            .slot_id_to_index()
            .get(&source_slot)
            .copied()
            .ok_or_else(|| identity_input_failure("final result input source slot is missing"))?;
        let actual_field = batch_schema
            .fields()
            .get(index)
            .ok_or_else(|| identity_input_failure("final result input batch field is missing"))?;
        let actual_slot = chunk
            .chunk_schema()
            .slot(source_slot)
            .ok_or_else(|| identity_input_failure("final result input slot field is missing"))?;
        if target.is_typed() {
            if !actual_slot.is_typed() {
                return Err(identity_input_failure(
                    "typed final result input requires its explicit typed source binding",
                ));
            }
            let actual = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                actual_slot.typed_binding().clone(),
            )
            .map_err(crate::runtime::fragment::ExecutionFailure::from_domain)?;
            let expected = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                target.typed_binding().clone(),
            )
            .map_err(crate::runtime::fragment::ExecutionFailure::from_domain)?;
            if !actual.fits_value_type(&expected) {
                return Err(identity_input_failure(
                    "typed final result input semantics differ from output semantics",
                ));
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
            .ok_or_else(|| identity_input_failure("final result input array is missing"))?;
        check_type(array.data_type(), target.data_type())?;
    }
    Ok(())
}

// Test-only metadata materialization has exact, already produced carriers.
// It does not author a conversion or claim typed compiled input semantics.
#[cfg(test)]
pub(crate) fn materialize_project_output(
    final_columns: Vec<ArrayRef>,
    output_chunk_schema: &ChunkSchema,
    _validate_final_result_input: bool,
) -> ExecutionResult<Chunk> {
    if output_chunk_schema.slot_ids().len() != final_columns.len() {
        return Err(identity_input_failure(format_args!(
            "project output slots mismatch: slots={} columns={}",
            output_chunk_schema.slot_ids().len(),
            final_columns.len()
        )));
    }
    let mut output_columns = Vec::with_capacity(final_columns.len());
    let mut slots = Vec::with_capacity(final_columns.len());
    for (array, slot_id) in final_columns.iter().zip(output_chunk_schema.slot_ids()) {
        let declared = output_chunk_schema.slot(*slot_id).ok_or_else(|| {
            identity_input_failure(format_args!("project output slot {slot_id} is missing"))
        })?;
        if array.data_type() != declared.data_type() {
            return Err(identity_input_failure(
                "metadata project requires its exact declared carrier",
            ));
        }
        slots.push(projected_slot_schema(
            declared,
            *slot_id,
            array.data_type(),
            array.null_count() > 0,
        )?);
        output_columns.push(array.clone());
    }
    let output_chunk_schema = projected_chunk_schema(slots, output_chunk_schema)?;
    Ok(Chunk::try_new_with_columns(
        output_chunk_schema,
        output_columns,
    )?)
}

/// Encoding provenance is signed by the actual producing operation. An array
/// alone cannot establish an original dictionary's occurrence facts.
pub(crate) enum ProjectEncodingSource<'a> {
    Plain,
    OriginalField(&'a arrow::datatypes::Field),
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
    .map_err(|source| {
        ConstructionFailure::Source(crate::runtime::fragment::SharedDomainFailure::new(source))
    })?;
    let target = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
        target.clone_observed(work)?,
    )
    .map_err(|source| {
        ConstructionFailure::Source(crate::runtime::fragment::SharedDomainFailure::new(source))
    })?;
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
            *column = novarocks_functions::array_projection::rebuild_array_observed::<
                ConstructionFailure,
                _,
            >(column, declared.slots()[ordinal].data_type(), &mut |step| {
                owner.observe(ordinal, &[], step)
            })?;
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
                ProjectEncodingSource::OriginalField(field) => field,
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray, DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
    };
    use novarocks_types::logical::{LogicalType, field_with_logical_type};
    use std::collections::HashMap;

    fn final_output(data_type: DataType, domain: LogicalType) -> ChunkSchemaRef {
        final_output_with_field(field_with_logical_type(
            Field::new("result", data_type, true),
            domain,
        ))
    }
    fn final_output_with_field(field: Field) -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                field,
                None,
                None,
            )])
            .unwrap(),
        )
    }
    fn typed_output(declaration: &novarocks_type_contract::FunctionValueType) -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new_typed(
                vec![
                    ChunkSlotSchema::try_new_typed(
                        SlotId::new(1),
                        Arc::new(Field::new(
                            "result",
                            declaration.data_type().clone(),
                            declaration.nullable(),
                        )),
                        declaration.typed_binding().clone(),
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
    // The same two real owners used by CompiledProjectProcessor::apply.
    // This fixture supplies only data/ports and the actual construction factory.
    fn validate_and_materialize(output: &ChunkSchemaRef, input: Chunk) -> ExecutionResult<Chunk> {
        validate_final_result_identity_input_source(
            &input,
            output,
            true,
            1,
            std::iter::once(Ok((SlotId::new(1), SlotId::new(1)))),
        )?;
        materialize_declared(output, input)
    }
    fn materialize_declared(output: &ChunkSchemaRef, input: Chunk) -> ExecutionResult<Chunk> {
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("project materialization"),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()))?;
        let owner = factory.begin_typed_construction(output)?;
        let storage = input
            .source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                owner.request(layout)
            })?;
        let mut columns = project_reserve(input.columns().len(), owner.as_ref())?;
        columns.extend(input.columns().iter().cloned());
        materialize_project_output_owned(
            columns,
            input.len(),
            output.clone(),
            |ordinal| Ok(input.chunk_schema().slots()[ordinal].typed_binding()),
            |ordinal| {
                Ok(ProjectEncodingSource::OriginalField(
                    input.chunk_schema().slots()[ordinal].field(),
                ))
            },
            storage,
            owner.as_ref(),
        )
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
        Chunk::try_new_typed_with_columns_owned_observed::<
            crate::runtime::fragment::ExecutionFailure,
            _,
        >(
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
                Some(marker) => {
                    field.with_metadata(std::collections::HashMap::<String, String>::from([(
                        novarocks_types::logical::NR_LOGICAL_TYPE_KEY.to_owned(),
                        marker.to_owned(),
                    )]))
                }
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

    fn final_struct_field(marker: Option<&str>) -> Field {
        let child = Field::new("state", DataType::Binary, true);
        let child = match marker {
            Some(value) => {
                child.with_metadata(std::collections::HashMap::<String, String>::from([(
                    novarocks_types::logical::NR_LOGICAL_TYPE_KEY.to_owned(),
                    value.to_owned(),
                )]))
            }
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
    fn synthetic_nested_project_source_stays_unknown_without_changing_values() {
        // A fresh root map does not prove a foreign nested child map.
        let mut map = HashMap::with_capacity(8192);
        map.insert("source".into(), "unknown".into());
        let child = Arc::new(Field::new("child", DataType::Int32, false).with_metadata(map));
        let data_type = DataType::Struct(vec![Arc::clone(&child)].into());
        let slot = ChunkSlotSchema::try_new_with_field_ref(
            SlotId::new(1),
            Arc::new(Field::new("computed", data_type, true)),
            None,
            None,
        )
        .unwrap();
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
    fn final_identity_rejects_actual_batch_slot_and_cache_conflicts_independently() {
        for (batch, slot, cached) in [
            (Some("hll"), None, None),
            (None, Some("hll"), None),
            (None, None, Some(ValueLogicalType::Hll)),
            (Some("bitmap"), Some("bitmap"), Some(ValueLogicalType::Hll)),
        ] {
            let op = final_output(DataType::Binary, LogicalType::Bitmap);
            let error =
                validate_and_materialize(&op, final_binary_input(batch, slot, cached, false))
                    .unwrap_err();
            assert!(error.detail().contains("logical domain differs"), "{error}");
        }
    }

    #[test]
    fn final_identity_rejects_unknown_batch_and_slot_markers_before_empty_output() {
        for empty in [false, true] {
            for marker in ["unknown", "", "   "] {
                for (batch, slot) in [(Some(marker), None), (None, Some(marker))] {
                    let op = final_output(DataType::Binary, LogicalType::Bitmap);
                    let error =
                        validate_and_materialize(&op, final_binary_input(batch, slot, None, empty))
                            .unwrap_err();
                    assert!(error.detail().contains("unknown logical domain"), "{error}");
                }
            }
            let op = final_output(DataType::Binary, LogicalType::Bitmap);
            assert!(
                validate_and_materialize(&op, final_binary_input(Some("hll"), None, None, empty))
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
                    source_field = source_field.with_metadata(std::collections::HashMap::<
                        String,
                        String,
                    >::from([(
                        "nr_logical_type".into(),
                        attribute.into(),
                    )]));
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
                let op = typed_output(&declaration);
                let output = validate_and_materialize(&op, input).unwrap();
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
        let op = typed_output(&declaration);
        let output = validate_and_materialize(&op, input).unwrap();
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
            let op = final_output(DataType::Binary, LogicalType::Bitmap);
            let error = validate_and_materialize(&op, input).unwrap_err();
            assert!(error.detail().contains("carrier differs"), "{error}");
        }
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
                    let op = final_output_with_field(target.clone());
                    let error = validate_and_materialize(&op, input).unwrap_err();
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
            let op = final_output_with_field(target.clone());
            assert!(
                validate_and_materialize(&op, input)
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
                let op = typed_output(&declaration);
                let output = validate_and_materialize(&op, input).unwrap();
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
            let op = final_output_with_field(Field::new("target", target, true));
            assert!(
                validate_and_materialize(&op, input)
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
        let op = typed_output(&declaration);
        let output = validate_and_materialize(&op, input).unwrap();
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
        use novarocks_type_contract::FunctionValueType;
        let output = final_output(DataType::Binary, LogicalType::Bitmap);
        let input = final_binary_input(None, None, None, false);
        for (identity, pair_count, pair) in [
            (false, 1, (SlotId::new(1), SlotId::new(1))),
            (true, 0, (SlotId::new(1), SlotId::new(1))),
            (true, 1, (SlotId::new(2), SlotId::new(1))),
            (true, 1, (SlotId::new(1), SlotId::new(2))),
        ] {
            let error = validate_final_result_identity_input_source(
                &input,
                &output,
                identity,
                pair_count,
                std::iter::once(Ok(pair)),
            )
            .unwrap_err();
            assert_eq!(
                error.failure_fact().class(),
                novarocks_failure_contract::FailureClass::Invariant
            );
            assert!(error.detail().contains("identity"), "{error}");
        }
        let declaration = FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            ValueLogicalType::Bitmap,
        )
        .unwrap();
        let source_declaration =
            FunctionValueType::try_with_logical_type(DataType::Binary, true, ValueLogicalType::Hll)
                .unwrap();
        let output = typed_output(&declaration);
        let source = || {
            typed_fixture_input_direct(
                Field::new("source", DataType::Binary, true),
                &source_declaration,
                Arc::new(BinaryArray::from(vec![&b"state"[..]])),
                SlotId::new(1),
            )
        };
        assert!(materialize_declared(&output, source()).is_err());
        let error = validate_and_materialize(&output, source()).unwrap_err();
        assert!(
            error
                .detail()
                .contains("typed final result input semantics differ from output semantics"),
            "{error}"
        );
    }
    #[test]
    fn project_runtime_keeps_full_json_declaration_and_separate_source_attributes() {
        use novarocks_type_contract::FunctionValueType;
        let declaration =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let source_format_field =
            Field::new("payload", DataType::Utf8, true).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                "nr_logical_type".into(),
                "unknown format attribute".into(),
            )]));
        let source_slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(source_format_field.clone()),
            declaration.typed_binding().clone(),
            Some(17),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let compute = source_slot
            .with_slot_id_typed_observed::<crate::runtime::fragment::ExecutionFailure>(
                SlotId::new(3),
                || Ok(()),
            )
            .unwrap();
        let duplicate = source_slot
            .with_slot_id_typed_observed::<crate::runtime::fragment::ExecutionFailure>(
                SlotId::new(4),
                || Ok(()),
            )
            .unwrap();
        // Declare pure fields; provider attributes remain attached only to source.
        let target = |slot: ChunkSlotSchema| {
            ChunkSlotSchema::try_new_typed(
                slot.slot_id(),
                Arc::new(Field::new(slot.name(), DataType::Utf8, true)),
                slot.typed_binding().clone(),
                slot.unique_id(),
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap()
        };
        let output_schema = Arc::new(
            ChunkSchema::try_new_typed(
                vec![target(compute), target(duplicate)],
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        );
        for values in [vec![Some("{}")], Vec::new()] {
            let array: ArrayRef = Arc::new(StringArray::from(values));
            let rows = array.len();
            let input = Chunk::try_new_typed_with_columns_owned_observed::<
                crate::runtime::fragment::ExecutionFailure,
                _,
            >(
                Arc::new(
                    ChunkSchema::try_new_typed(
                        vec![source_slot.clone()],
                        ValueTypeStorageLifetime::pure(),
                    )
                    .unwrap(),
                ),
                vec![array.clone()],
                rows,
                ValueTypeStorageLifetime::pure(),
                |_| Ok(()),
            )
            .unwrap();
            let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                crate::runtime::mem_tracker::MemTracker::new_root("project JSON port"),
                Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default())).unwrap();
            let owner = factory.begin_typed_construction(&output_schema).unwrap();
            let storage = input
                .source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                    owner.request(layout)
                })
                .unwrap();
            let mut columns = project_reserve(2, owner.as_ref()).unwrap();
            columns.extend([array.clone(), array]);
            let output = materialize_project_output_owned(
                columns,
                rows,
                output_schema.clone(),
                |_| Ok(source_slot.typed_binding()),
                |_| Ok(ProjectEncodingSource::Plain),
                storage,
                owner.as_ref(),
            )
            .unwrap();
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
    fn project_preserves_explicit_output_chunk_schema() {
        use novarocks_type_contract::FunctionValueType;
        let declaration = FunctionValueType::try_with_logical_type(
            DataType::Binary,
            false,
            ValueLogicalType::Hll,
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(2),
            Arc::new(Field::new("out", DataType::Binary, false)),
            declaration.typed_binding().clone(),
            None,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let declared = Arc::new(
            ChunkSchema::try_new_typed(vec![slot], ValueTypeStorageLifetime::pure()).unwrap(),
        );
        let input = typed_fixture_input_direct(
            Field::new("in", DataType::Binary, false),
            &declaration,
            Arc::new(BinaryArray::from(vec![&b"x"[..]])),
            SlotId::new(1),
        );
        let original = input.columns()[0].clone();
        let output = materialize_declared(&declared, input).unwrap();
        let slot = &output.chunk_schema().slots()[0];
        assert_eq!(slot.slot_id(), SlotId::new(2));
        assert_eq!(slot.name(), "out");
        assert!(!slot.nullable());
        assert_eq!(
            slot.typed_binding().value_type().domain(),
            Some(ValueLogicalType::Hll)
        );
        assert!(slot.field().metadata().is_empty());
        assert!(Arc::ptr_eq(&declared, &output.chunk_schema_ref()));
        assert!(Arc::ptr_eq(&original, &output.columns()[0]));
    }
}

#[cfg(test)]
mod owned_project_tests {
    use super::*;
    use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
    use arrow::array::{DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_type_contract::{
        FunctionValueType, TypedValueBinding, ValueField, ValueKind, ValueLayout, ValueTypeRef,
        ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
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
        Chunk::try_new_typed_with_columns_owned_observed::<
            crate::runtime::fragment::ExecutionFailure,
            _,
        >(
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
    fn project_encoded_input_unused_null_hydration_and_signed_keys() {
        let required = string_binding(false, ValueTypeStorageLifetime::pure());
        let nullable = string_binding(true, ValueTypeStorageLifetime::pure());
        let owner = owner(ValueTypeStorageLifetime::pure(), false);
        let unused_null = dictionary_source(
            Int32Array::from(vec![0, 2, 0]),
            Arc::new(StringArray::from(vec![Some("PAID"), None, Some("New")])),
            &required,
        );
        let hydrate = |source: &Chunk| {
            crate::exec::chunk::hydrate_dictionary_columns_owned_observed::<
                ConstructionFailure,
                _,
                _,
                _,
                _,
            >(
                source,
                owner.storage_lifetime(),
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
                |_, _, _, _| false,
                |ordinal, path, step| owner.observe(ordinal, path, step),
                |layout| owner.request(layout),
                |_| Ok(()),
            )
            .unwrap()
        };
        let hydrated = hydrate(&unused_null);
        assert_eq!(
            hydrated.columns()[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec![Some("PAID"), Some("New"), Some("PAID")])
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
        let hydrated = hydrate(&signed);
        assert_eq!(
            hydrated.columns()[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec![Some("New"), None, Some("PAID")])
        );
        assert!(owner.opaque.load(Ordering::SeqCst) > 0);
        assert!(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![-1]),
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef
            )
            .is_err()
        );
    }
}
