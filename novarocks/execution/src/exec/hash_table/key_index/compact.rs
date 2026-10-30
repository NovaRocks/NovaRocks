// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Gather immutable representative loans into new owned pages. Ordinals are
//! caller coordinates, never hash identities. Old pages and their escaping
//! aliases remain alive while the original owner funds the new construction.

use super::{
    KeySpec,
    canonical_store::OwnedCanonicalColumn,
    checkpoint_at,
    fixed_store::OwnedFixedColumn,
    invariant, original,
    owned_batch::{Column, OwnedKeyBatch, PageRow, validate_compacted_binding},
    prepared::is_fixed,
    read::{KeyReadColumn, KeyReadValues, OwnedKeyReadBatch},
    reserve_push,
    store::OwnedByteColumn,
    vector,
};
use crate::{
    exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::array::{Array, BinaryViewArray, StringViewArray, new_empty_array};
use novarocks_functions::{
    AggregateStateAllocator, KernelEvaluationControl,
    selected_copy::{
        CopyConstructionError, copy_nested_values_construction, preflight_empty_construction,
    },
};
use novarocks_type_contract::{FunctionValueType, ValueLayout, ValueSchemaCreation};
use novarocks_value_arrays::{
    ByteLayout, ValueViolation,
    build::{BuiltArray, ValueBuilder, WriteFailure},
};
use std::sync::Arc;

// An aggregation target only. A larger value is copied into its own page.
const PAGE_TARGET: usize = 64 * 1024;

/// Borrow only the caller's existing construction facts and resource ports.
/// This bundle grants no capacity and never retains a previous page creator.
pub(super) struct Construction<'a> {
    pub(super) spec: &'a KeySpec,
    pub(super) column: usize,
    pub(super) owner: &'a dyn InputConstructionOwner,
    pub(super) host: Arc<dyn AggregateStateAllocator>,
    pub(super) control: &'a dyn KernelEvaluationControl,
}

/// The returned batch row equals the corresponding requested read ordinal.
/// The table owner separately commits stable class locations after success.
pub(super) fn gather(
    read: &OwnedKeyReadBatch<'_>,
    spec: &KeySpec,
    owner: &dyn InputConstructionOwner,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<OwnedKeyBatch> {
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
    checkpoint()?;
    if read.columns().len() != spec.types().len() {
        return Err(invariant(ValueViolation::Layout));
    }
    let mut columns = vector(owner, read.columns().len())?;
    for (column, source) in read.columns().iter().enumerate() {
        checkpoint()?;
        let binding = merge_binding(source, spec, column, owner, control)?;
        let construction = Construction {
            spec,
            column,
            owner,
            host: Arc::clone(&host),
            control,
        };
        let output = if is_fixed(spec.types()[column].data_type()) {
            Column::Fixed(gather_fixed(source, read.len(), binding, &construction)?)
        } else if let Some(layout) = ByteLayout::from_data_type(spec.types()[column].data_type()) {
            let layout = if layout.is_utf8() {
                ByteLayout::Utf8View
            } else {
                ByteLayout::BinaryView
            };
            let built = if layout.is_utf8() {
                let mut values = slots::<Option<&str>>(read.len(), owner, &checkpoint)?;
                for page in source.pages() {
                    let (array, admitted) = match page.values() {
                        KeyReadValues::Bytes { array, page, .. }
                        | KeyReadValues::NominalBytes {
                            raw: array, page, ..
                        } => (*array, *page),
                        _ => return Err(invariant(ValueViolation::Carrier)),
                    };
                    let strings = array
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .ok_or_else(|| invariant(ValueViolation::Carrier))?;
                    for (index, row) in page.rows().iter().enumerate() {
                        checkpoint_at(index, &checkpoint)?;
                        if !admitted.defined_rows().contains(row.row) || row.row >= strings.len() {
                            return Err(invariant(ValueViolation::UndefinedRow));
                        }
                        put(
                            &mut values,
                            row.ordinal,
                            (!strings.is_null(row.row)).then(|| strings.value(row.row)),
                        )?;
                    }
                }
                emit_pages(
                    &values,
                    layout,
                    owner,
                    control,
                    |v| v.len(),
                    |b, v, growth, work| b.append_str(v, growth, work),
                )?
            } else {
                let mut values = slots::<Option<&[u8]>>(read.len(), owner, &checkpoint)?;
                for page in source.pages() {
                    let (array, admitted) = match page.values() {
                        KeyReadValues::Bytes { array, page, .. }
                        | KeyReadValues::NominalBytes {
                            raw: array, page, ..
                        } => (*array, *page),
                        _ => return Err(invariant(ValueViolation::Carrier)),
                    };
                    let bytes = array
                        .as_any()
                        .downcast_ref::<BinaryViewArray>()
                        .ok_or_else(|| invariant(ValueViolation::Carrier))?;
                    for (index, row) in page.rows().iter().enumerate() {
                        checkpoint_at(index, &checkpoint)?;
                        if !admitted.defined_rows().contains(row.row) || row.row >= bytes.len() {
                            return Err(invariant(ValueViolation::UndefinedRow));
                        }
                        put(
                            &mut values,
                            row.ordinal,
                            (!bytes.is_null(row.row)).then(|| bytes.value(row.row)),
                        )?;
                    }
                }
                emit_pages(
                    &values,
                    layout,
                    owner,
                    control,
                    |v| v.len(),
                    |b, v, growth, work| b.append_bytes(v, growth, work),
                )?
            };
            let binding = view_binding(binding, layout, owner, &checkpoint)?;
            let mut pages = vector(owner, built.len())?;
            let mut locations = vector(owner, read.len())?;
            for (page, built) in built.into_iter().enumerate() {
                checkpoint()?;
                let data = OwnedByteColumn::from_compacted(
                    built,
                    binding.clone_observed(&checkpoint)?,
                    &construction,
                )?;
                for row in 0..data.len() {
                    checkpoint_at(locations.len(), &checkpoint)?;
                    locations.push(PageRow { page, row });
                }
                pages.push(data);
            }
            if locations.len() != read.len() {
                return Err(invariant(ValueViolation::Bounds));
            }
            Column::Bytes { pages, locations }
        } else {
            let mut frames = slots::<Option<&[u8]>>(read.len(), owner, &checkpoint)?;
            for page in source.pages() {
                let KeyReadValues::Canonical { encoded, .. } = page.values() else {
                    return Err(invariant(ValueViolation::Carrier));
                };
                for (index, row) in page.rows().iter().enumerate() {
                    checkpoint_at(index, &checkpoint)?;
                    if row.row >= encoded.len() || encoded.is_null(row.row) {
                        return Err(invariant(ValueViolation::Carrier));
                    }
                    // The complete original nullable frame is copied verbatim,
                    // including its explicit SQL NULL marker. No re-encoding.
                    put(&mut frames, row.ordinal, Some(encoded.value(row.row)))?;
                }
            }
            let pages = emit_pages(
                &frames,
                ByteLayout::BinaryView,
                owner,
                control,
                |v| v.len(),
                |b, v, growth, work| b.append_bytes(v, growth, work),
            )?;
            Column::Canonical(OwnedCanonicalColumn::from_compacted(
                pages,
                binding,
                &construction,
            )?)
        };
        columns.push(output);
    }
    checkpoint()?;
    OwnedKeyBatch::from_compacted(spec, columns, read.len(), owner, control)
}

fn slots<T>(
    count: usize,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Vec<Option<T>>> {
    let mut values = vector(owner, count)?;
    for row in 0..count {
        checkpoint_at(row, checkpoint)?;
        values.push(None);
    }
    Ok(values)
}
fn put<T>(slots: &mut [Option<T>], ordinal: usize, value: T) -> ExecutionResult<()> {
    let slot = slots
        .get_mut(ordinal)
        .ok_or_else(|| invariant(ValueViolation::Bounds))?;
    if slot.is_some() {
        return Err(invariant(ValueViolation::RowTransaction));
    }
    *slot = Some(value);
    Ok(())
}

fn merge_binding(
    source: &KeyReadColumn<'_>,
    spec: &KeySpec,
    column: usize,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<FunctionValueType> {
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
    let mut nullable = false;
    for page in source.pages() {
        checkpoint()?;
        let binding = match page.values() {
            KeyReadValues::Fixed { binding, .. } | KeyReadValues::Bytes { binding, .. } => *binding,
            KeyReadValues::NominalBytes {
                logical_binding, ..
            }
            | KeyReadValues::Canonical {
                logical_binding, ..
            } => *logical_binding,
        };
        validate_compacted_binding(spec, column, binding, &checkpoint)?;
        nullable |= binding.nullable();
    }
    // The index already holds this declaration. Do not clone the previous
    // page's layout owner: it can carry the previous insertion creator.
    let base = &spec.types()[column];
    let nullable = if source.pages().is_empty() {
        base.nullable()
    } else {
        nullable
    };
    if base.nullable() == nullable {
        return base.clone_observed(checkpoint);
    }
    base.with_nullable_observed::<ConstructionFailure>(
        nullable,
        owner.storage_lifetime(),
        |layout| owner.request(layout),
        |_| checkpoint().map_err(ConstructionFailure::from),
    )
    .map_err(ExecutionFailure::from)
}
fn view_binding(
    binding: FunctionValueType,
    layout: ByteLayout,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<FunctionValueType> {
    if super::store::is_nominal_byte(&binding) {
        // Raw private view storage does not change a nominal logical carrier.
        return Ok(binding);
    }
    owner.request(ValueLayout::storage_allocation_layout().map_err(ConstructionFailure::from)?)?;
    let physical = ValueLayout::try_from_normalized_arrow_observed::<super::store::BindingFailure>(
        layout.data_type(),
        owner.storage_lifetime(),
        |_| checkpoint().map_err(super::store::BindingFailure::Caller),
    )
    .map_err(ExecutionFailure::from)?;
    FunctionValueType::try_from_value(binding.value_type().clone(), physical)
        .map_err(|e| ExecutionFailure::from(novarocks_functions::KernelFailure::from(e)))
}

fn gather_fixed(
    source: &KeyReadColumn<'_>,
    len: usize,
    binding: FunctionValueType,
    construction: &Construction<'_>,
) -> ExecutionResult<OwnedFixedColumn> {
    let owner = construction.owner;
    let control = construction.control;
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
    let mut sources = vector(owner, source.pages().len())?;
    let mut positions = slots(len, owner, &checkpoint)?;
    for page in source.pages() {
        checkpoint()?;
        let KeyReadValues::Fixed { array, .. } = page.values() else {
            return Err(invariant(ValueViolation::Carrier));
        };
        let index = sources.len();
        sources.push(Arc::clone(array));
        for (i, row) in page.rows().iter().enumerate() {
            checkpoint_at(i, &checkpoint)?;
            if row.row >= array.len() {
                return Err(invariant(ValueViolation::Bounds));
            }
            put(&mut positions, row.ordinal, (index, row.row))?;
        }
    }
    let mut choices = vector(owner, len)?;
    for (ordinal, value) in positions.into_iter().enumerate() {
        checkpoint_at(ordinal, &checkpoint)?;
        choices.push(value.ok_or_else(|| invariant(ValueViolation::UndefinedRow))?);
    }
    let mut request = |layout| owner.request(layout).map_err(ExecutionFailure::from);
    let mut opaque = |bytes| {
        owner
            .observe_opaque_upper_bound(bytes)
            .map_err(ExecutionFailure::from)
    };
    let mut creation =
        ValueSchemaCreation::new(owner.storage_lifetime(), &mut request, &mut opaque);
    let array = if len == 0 {
        preflight_empty_construction(binding.typed_binding().layout(), &mut creation, control)
            .map_err(copy_error)?;
        checkpoint()?;
        let array = new_empty_array(binding.data_type());
        checkpoint()?;
        array
    } else {
        copy_nested_values_construction(&sources, &choices, &mut creation, control)
            .map_err(copy_error)?
    };
    OwnedFixedColumn::from_compacted(array, binding, construction)
}
fn copy_error(error: CopyConstructionError<ExecutionFailure>) -> ExecutionFailure {
    match error {
        CopyConstructionError::Creator(cause) => cause,
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
    }
}

fn emit_pages<T: Copy>(
    values: &[Option<Option<T>>],
    layout: ByteLayout,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
    length: impl Fn(T) -> usize,
    mut append: impl FnMut(
        &mut ValueBuilder,
        T,
        &mut novarocks_value_arrays::control::Growth<'_, ExecutionFailure>,
        &mut novarocks_value_arrays::control::Work<'_, ExecutionFailure>,
    ) -> Result<(), WriteFailure<ExecutionFailure>>,
) -> ExecutionResult<Vec<BuiltArray>> {
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
    let mut pages = Vec::new();
    let mut position = 0;
    loop {
        checkpoint()?;
        let mut builder = ValueBuilder::new(layout);
        let mut bytes = 0usize;
        while let Some(value) = values.get(position) {
            checkpoint_at(position, &checkpoint)?;
            if !builder.is_empty() && bytes >= PAGE_TARGET {
                break;
            }
            let value = (*value).ok_or_else(|| invariant(ValueViolation::UndefinedRow))?;
            let mut growth = |layout| owner.request(layout).map_err(ExecutionFailure::from);
            let mut work = |_| checkpoint();
            let result = if let Some(value) = value {
                append(&mut builder, value, &mut growth, &mut work)
            } else {
                builder.append_null(&mut growth, &mut work)
            };
            match result {
                Ok(()) => {
                    position += 1;
                    bytes = bytes
                        .saturating_add(16)
                        .saturating_add(value.map_or(0, &length));
                }
                Err(WriteFailure::Full) if !builder.is_empty() => break,
                Err(WriteFailure::Full) => return Err(invariant(ValueViolation::RowTransaction)),
                Err(WriteFailure::Failure(cause)) => return Err(original(cause)),
            }
        }
        let built = builder
            .finish_structured_observed(
                &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                &mut |_| checkpoint(),
                &mut |event| {
                    owner
                        .observe(
                            0,
                            &[],
                            crate::exec::chunk::ArrayNormalizationStep::ValueStructure {
                                path: &[],
                                event,
                            },
                        )
                        .map_err(ExecutionFailure::from)
                },
            )
            .map_err(original)?;
        reserve_push(&mut pages, owner, &checkpoint)?;
        pages.push(built);
        if position == values.len() {
            break;
        }
    }
    checkpoint()?;
    Ok(pages)
}
