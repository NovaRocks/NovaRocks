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

//! Immutable key backing. Byte representatives are copied through the public
//! value builder. New payload and its actual creator survive every Arrow alias.

use super::{KeySpec, NullPolicy, PreparedKeyBatch, checkpoint_at, invariant, original, vector};
use crate::{
    exec::{
        chunk::ChunkConstructionFailure,
        pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::array::{Array, ArrayRef};
use novarocks_functions::{
    AggregateStateAllocator, EvaluationCheckpoints, KernelEvaluationControl, SourceBackingOwner,
    opaque_memory::OpaqueRetainedCharge, retain_source_backing, source_custody_prefix_facts,
};
use novarocks_type_contract::{FunctionValueType, ValueLayout, ValueTypeStorageLifetime};
use novarocks_value_arrays::{
    ByteLayout, ValueViolation,
    batch::{OwnedBytesPage, compare::compare_owned_pairs_into, selected::visit_selected},
    build::{BuiltArray, ValueBuilder, WriteFailure},
    hash_eq::NullEquality,
    reachability::{ReachableRows, SelectionSet},
};
use std::sync::Arc;

// A materialization target, not an input limit. A larger individual value is
// accepted in its own page; further values continue in subsequent pages.
const BYTE_PAGE_TARGET: usize = 64 * 1024;

pub(super) struct OwnedByteColumn {
    array: ArrayRef,
    binding: FunctionValueType,
    spec: KeySpec,
    column: usize,
    page: OwnedBytesPage,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the real byte-column creator and raw nominal storage holder; preserve drop order."
        )
    )]
    storage: ValueTypeStorageLifetime,
}
/// Nominal byte values may use raw private view storage, but that storage is
/// never a legal replacement for their separately retained logical carrier.
pub(super) fn is_nominal_byte(binding: &FunctionValueType) -> bool {
    binding.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
        && ByteLayout::from_data_type(binding.data_type()).is_some()
}
struct KeyColumnCreator {
    _binding: FunctionValueType,
    _storage: ValueTypeStorageLifetime,
}
impl OwnedByteColumn {
    pub(super) fn from_compacted(
        mut built: BuiltArray,
        binding: FunctionValueType,
        construction: &super::compact::Construction<'_>,
    ) -> ExecutionResult<Self> {
        let owner = construction.owner;
        let checkpoint = || {
            construction
                .control
                .checkpoint(0)
                .map_err(ExecutionFailure::from)
        };
        checkpoint()?;
        super::owned_batch::validate_compacted_binding(
            construction.spec,
            construction.column,
            &binding,
            &checkpoint,
        )?;
        let logical = ByteLayout::from_data_type(binding.data_type())
            .ok_or_else(|| invariant(ValueViolation::Carrier))?;
        let raw = ByteLayout::from_data_type(built.array.data_type())
            .filter(|layout| layout.is_view())
            .ok_or_else(|| invariant(ValueViolation::Carrier))?;
        if logical.is_utf8() != raw.is_utf8()
            || (!is_nominal_byte(&binding) && binding.data_type() != built.array.data_type())
        {
            return Err(invariant(ValueViolation::Carrier));
        }
        if !binding.nullable() && built.array.null_count() != 0 {
            return Err(invariant(ValueViolation::RequiredNull));
        }
        let storage = owner.storage_lifetime();
        built.array = retain_created_array(
            built.array,
            &binding,
            &storage,
            Arc::clone(&construction.host),
            construction.control,
        )?;
        let array = Arc::clone(&built.array);
        let defined = ReachableRows::all(array.len())
            .materialize_observed(
                &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                &mut |_| checkpoint(),
            )
            .map_err(original)?;
        let page = OwnedBytesPage::new(
            built,
            defined,
            &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| checkpoint(),
        )
        .map_err(original)?;
        checkpoint()?;
        Ok(Self {
            array,
            binding,
            spec: construction.spec.clone(),
            column: construction.column,
            page,
            storage,
        })
    }
    #[cfg(test)]
    pub(super) fn page_target_for_test() -> usize {
        BYTE_PAGE_TARGET
    }
    /// Prepare the selected source once, then publish as many owned pages as
    /// needed in occurrence order. Pagination never rescans a remaining suffix.
    pub(super) fn copy_pages(
        input: &PreparedKeyBatch<'_>,
        column: usize,
        rows: &[usize],
        owner: &dyn InputConstructionOwner,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Vec<Self>> {
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        let source = input
            .bytes(column)?
            .ok_or_else(|| invariant(ValueViolation::Carrier))?;
        let mut sorted = vector(owner, rows.len())?;
        for (i, &row) in rows.iter().enumerate() {
            checkpoint_at(i, &checkpoint)?;
            if !input.input.selected().contains(row) {
                return Err(invariant(ValueViolation::UndefinedRow));
            }
            sorted.push(row);
        }
        super::prepared::sort_rows(&mut sorted, owner, &checkpoint)?;
        let mut distinct = 0;
        for i in 0..sorted.len() {
            checkpoint_at(i, &checkpoint)?;
            if distinct == 0 || sorted[i] != sorted[distinct - 1] {
                sorted[distinct] = sorted[i];
                distinct += 1;
            }
        }
        sorted.truncate(distinct);
        let mut selected = SelectionSet::try_new_with_control_observed(
            input.input.selected().len(),
            &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| checkpoint(),
        )
        .map_err(original)?;
        for (i, &row) in sorted.iter().enumerate() {
            checkpoint_at(i, &checkpoint)?;
            selected
                .insert(row)
                .map_err(ExecutionFailure::from_domain)?;
        }
        let subset = input.byte_subset(column, &selected, &checkpoint)?;
        let mut positions = vector(owner, rows.len())?;
        for (i, row) in rows.iter().enumerate() {
            checkpoint_at(i, &checkpoint)?;
            positions.push(
                sorted
                    .binary_search(row)
                    .map_err(|_| invariant(ValueViolation::UndefinedRow))?,
            );
        }
        if source.is_utf8() {
            // The original decoded STRING author supplies valid str loans.
            // No ad-hoc UTF8 validation, unchecked cast or new codec lives here.
            let mut strings = vector(owner, distinct)?;
            visit_selected(
                &subset,
                &mut |_, row, _| {
                    strings.push(
                        input
                            .tuples()
                            .string_component(column, row)?
                            .ok_or_else(|| invariant(ValueViolation::Carrier))?,
                    );
                    Ok(())
                },
                &mut |_| checkpoint(),
            )
            .map_err(original)?;
            emit_pages(
                input,
                column,
                &positions,
                &strings,
                ByteLayout::Utf8View,
                owner,
                host,
                control,
                |builder, value, growth, work| builder.append_str(value, growth, work),
            )
        } else {
            let mut values = vector(owner, distinct)?;
            visit_selected(
                &subset,
                &mut |_, _, value| {
                    values.push(value);
                    Ok(())
                },
                &mut |_| checkpoint(),
            )
            .map_err(original)?;
            emit_pages(
                input,
                column,
                &positions,
                &values,
                ByteLayout::BinaryView,
                owner,
                host,
                control,
                |builder, value, growth, work| builder.append_bytes(value, growth, work),
            )
        }
    }
    pub(super) fn len(&self) -> usize {
        self.page.len()
    }
    pub(super) fn array_ref(&self) -> &ArrayRef {
        &self.array
    }
    pub(super) fn page(&self) -> &OwnedBytesPage {
        &self.page
    }
    #[cfg(test)]
    pub(super) fn array(&self) -> &dyn Array {
        self.array.as_ref()
    }
    pub(super) fn binding(&self) -> &FunctionValueType {
        &self.binding
    }
    #[cfg(test)]
    pub(super) fn storage(&self) -> &ValueTypeStorageLifetime {
        &self.storage
    }
    pub(super) fn compare_pairs(
        &self,
        input: &PreparedKeyBatch<'_>,
        column: usize,
        input_rows: &[usize],
        page_rows: &[usize],
        output: &mut [Option<bool>],
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        checkpoint()?;
        if column != self.column || !self.spec.same_instance(&input.spec) {
            return Err(invariant(ValueViolation::Layout));
        }
        let nulls = match self.spec.nulls()[column] {
            NullPolicy::Equal => NullEquality::Equal,
            NullPolicy::Unmatchable => NullEquality::Unequal,
        };
        let input = input
            .bytes(column)?
            .ok_or_else(|| invariant(ValueViolation::Carrier))?;
        compare_owned_pairs_into(
            &input,
            input_rows,
            &self.page,
            page_rows,
            nulls,
            output,
            &mut |_| checkpoint(),
        )
        .map_err(original)
    }
}

trait ByteLoan: Copy {
    fn len(self) -> usize;
}
impl ByteLoan for &str {
    fn len(self) -> usize {
        str::len(self)
    }
}
impl ByteLoan for &[u8] {
    fn len(self) -> usize {
        <[u8]>::len(self)
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_pages<T: ByteLoan>(
    input: &PreparedKeyBatch<'_>,
    column: usize,
    positions: &[usize],
    values: &[Option<T>],
    layout: ByteLayout,
    owner: &dyn InputConstructionOwner,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
    mut append: impl FnMut(
        &mut ValueBuilder,
        T,
        &mut novarocks_value_arrays::control::Growth<'_, ExecutionFailure>,
        &mut novarocks_value_arrays::control::Work<'_, ExecutionFailure>,
    ) -> Result<(), WriteFailure<ExecutionFailure>>,
) -> ExecutionResult<Vec<OwnedByteColumn>> {
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
    let mut pages = Vec::new();
    let mut position = 0;
    loop {
        checkpoint()?;
        // The original construction owner funds actual global builder buffers.
        // The separate host funds only its added custody/envelope metadata.
        let mut builder = ValueBuilder::new(layout);
        let mut copied = 0usize;
        while let Some(&ordinal) = positions.get(position) {
            checkpoint_at(position, &checkpoint)?;
            if !builder.is_empty() && copied >= BYTE_PAGE_TARGET {
                break;
            }
            let value = values[ordinal];
            let mut growth = |layout| owner.request(layout).map_err(ExecutionFailure::from);
            let mut work = |_| checkpoint();
            let written = match value {
                Some(value) => append(&mut builder, value, &mut growth, &mut work),
                None => builder.append_null(&mut growth, &mut work),
            };
            match written {
                Ok(()) => {
                    position += 1;
                    copied = copied
                        .saturating_add(16)
                        .saturating_add(value.map_or(0, ByteLoan::len));
                }
                Err(WriteFailure::Full) if !builder.is_empty() => break,
                Err(WriteFailure::Full) => return Err(invariant(ValueViolation::RowTransaction)),
                Err(WriteFailure::Failure(cause)) => return Err(original(cause)),
            }
        }
        let mut built = builder
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
        let storage = owner.storage_lifetime();
        let declared = &input.input.bindings()[column];
        let binding = if is_nominal_byte(declared) {
            // The view is private raw byte storage. Keep the original legal
            // nominal carrier and actual root NULL declaration separately;
            // never create a fictitious Json+Utf8View or Hll+BinaryView type.
            declared.clone_observed(&checkpoint)?
        } else {
            owner.request(
                ValueLayout::storage_allocation_layout().map_err(ConstructionFailure::from)?,
            )?;
            let physical = ValueLayout::try_from_normalized_arrow_observed::<BindingFailure>(
                layout.data_type(),
                storage.clone(),
                |_| checkpoint().map_err(BindingFailure::Caller),
            )
            .map_err(ExecutionFailure::from)?;
            // Root NULL declarations describe the actual admitted input; the
            // index equality domain intentionally ignores root nullability.
            FunctionValueType::try_from_value(declared.value_type().clone(), physical).map_err(
                |cause| ExecutionFailure::from(novarocks_functions::KernelFailure::from(cause)),
            )?
        };
        // Attach this real finite creator to every exported Buffer alias, not
        // just to the page struct. The envelope never holds an input array.
        built.array =
            retain_created_array(built.array, &binding, &storage, Arc::clone(&host), control)?;
        let array = Arc::clone(&built.array);
        let defined = ReachableRows::all(array.len())
            .materialize_observed(
                &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                &mut |_| checkpoint(),
            )
            .map_err(original)?;
        let page = OwnedBytesPage::new(
            built,
            defined,
            &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| checkpoint(),
        )
        .map_err(original)?;
        super::reserve_push(&mut pages, owner, &checkpoint)?;
        pages.push(OwnedByteColumn {
            array,
            binding,
            spec: input.spec.clone(),
            column,
            page,
            storage,
        });
        if position == positions.len() {
            break;
        }
    }
    checkpoint()?;
    Ok(pages)
}

/// Attach only this newly constructed column and its real finite creator.
/// The caller already funded this same array's payload; the supplied original
/// host pays the added custody metadata, never another payload invoice.
pub(super) fn retain_created_array(
    array: ArrayRef,
    binding: &FunctionValueType,
    storage: &ValueTypeStorageLifetime,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    let authority = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
    let _prefix = authority.reserve_operation(source_custody_prefix_facts().requests().bytes())?;
    let mut work = EvaluationCheckpoints::new(control);
    let creator = SourceBackingOwner::try_from_owned_direct(
        KeyColumnCreator {
            _binding: binding.clone(),
            _storage: storage.clone(),
        },
        host,
        &mut work,
    )?;
    let result = retain_source_backing(array, &creator, control)?;
    work.finish()?;
    Ok(result)
}

pub(super) enum BindingFailure {
    Type(novarocks_type_contract::ExecutionValueTypeError),
    Caller(ExecutionFailure),
}
impl From<novarocks_type_contract::ExecutionValueTypeError> for BindingFailure {
    fn from(cause: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Type(cause)
    }
}
impl From<BindingFailure> for ExecutionFailure {
    fn from(cause: BindingFailure) -> Self {
        match cause {
            BindingFailure::Type(cause) => ChunkConstructionFailure::from(cause).into(),
            BindingFailure::Caller(cause) => cause,
        }
    }
}
