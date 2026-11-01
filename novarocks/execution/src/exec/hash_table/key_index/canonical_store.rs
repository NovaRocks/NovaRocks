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

//! Truly complex components keep the existing canonical representation in
//! compact owned byte pages. Encoded bytes never impersonate an Arrow carrier
//! for the separately retained complete List/Struct/Map declaration.

use super::{
    KeySpec, NullPolicy, PreparedKeyBatch, checkpoint_at, invariant, original, reserve_push, vector,
};
use crate::{
    exec::{
        hash_table::key_builder::{
            canonical_component_payload, canonical_nullable_equals_observed,
        },
        pipeline::operator::InputConstructionOwner,
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::array::{Array, ArrayRef, BinaryViewArray};
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl};
use novarocks_type_contract::FunctionValueType;
use novarocks_value_arrays::{
    ByteLayout, ValueViolation,
    batch::OwnedBytesPage,
    build::{BuiltArray, ValueBuilder, WriteFailure},
    reachability::ReachableRows,
};
use std::sync::Arc;

// A soft aggregation target. Large individual encoded values get their own
// page; no key length, number of rows or collision quota follows from it.
const PAGE_TARGET: usize = 64 * 1024;
struct CanonicalPage {
    array: ArrayRef,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the checked OwnedBytesPage custody and defined-row holder; preserve field order."
        )
    )]
    page: OwnedBytesPage,
}
#[derive(Clone, Copy)]
struct Location {
    page: usize,
    row: usize,
    is_null: bool,
}
pub(super) struct OwnedCanonicalColumn {
    pages: Vec<CanonicalPage>,
    locations: Vec<Location>,
    binding: FunctionValueType,
    spec: KeySpec,
    column: usize,
}
/// An explicitly encoded read. Array materialization belongs to the original
/// canonical constructor under the caller's signed output and real creator.
#[cfg(test)]
pub(super) struct CanonicalKeyRead<'a> {
    pub(super) binding: &'a FunctionValueType,
    pub(super) payload: Option<&'a [u8]>,
}
impl OwnedCanonicalColumn {
    pub(super) fn from_compacted(
        built: Vec<BuiltArray>,
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
        let expected = construction
            .spec
            .types()
            .get(construction.column)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        if !expected
            .typed_binding()
            .layout()
            .equals_observed(binding.typed_binding().layout(), &checkpoint)?
        {
            return Err(invariant(ValueViolation::Layout));
        }
        let mut pages = vector(owner, built.len())?;
        let mut locations = Vec::new();
        for (page, mut built) in built.into_iter().enumerate() {
            checkpoint()?;
            let values = built
                .array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?;
            for row in 0..values.len() {
                checkpoint_at(row, &checkpoint)?;
                if values.is_null(row) {
                    return Err(invariant(ValueViolation::Carrier));
                }
                let is_null = canonical_component_payload(values.value(row))?.is_none();
                if is_null && !binding.nullable() {
                    return Err(invariant(ValueViolation::RequiredNull));
                }
                reserve_push(&mut locations, owner, &checkpoint)?;
                locations.push(Location { page, row, is_null });
            }
            built.array = super::store::retain_created_array(
                built.array,
                &binding,
                &owner.storage_lifetime(),
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
            pages.push(CanonicalPage { array, page });
        }
        checkpoint()?;
        Ok(Self {
            pages,
            locations,
            binding,
            spec: construction.spec.clone(),
            column: construction.column,
        })
    }
    pub(super) fn copy_selected(
        input: &PreparedKeyBatch<'_>,
        column: usize,
        rows: &[usize],
        owner: &dyn InputConstructionOwner,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Self> {
        Self::copy_with_target(input, column, rows, owner, host, control, PAGE_TARGET)
    }
    #[cfg(test)]
    pub(super) fn copy_selected_for_target(
        input: &PreparedKeyBatch<'_>,
        column: usize,
        rows: &[usize],
        owner: &dyn InputConstructionOwner,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
        target: usize,
    ) -> ExecutionResult<Self> {
        Self::copy_with_target(input, column, rows, owner, host, control, target)
    }
    #[cfg(test)]
    pub(super) fn page_count(&self) -> usize {
        self.pages.len()
    }
    fn copy_with_target(
        input: &PreparedKeyBatch<'_>,
        column: usize,
        rows: &[usize],
        owner: &dyn InputConstructionOwner,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
        target: usize,
    ) -> ExecutionResult<Self> {
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        let signed = input
            .spec
            .types()
            .get(column)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        if super::prepared::is_fixed(signed.data_type())
            || ByteLayout::from_data_type(signed.data_type()).is_some()
        {
            return Err(invariant(ValueViolation::Carrier));
        }
        let (source, mapping) = input.tuples().physical_component(column)?;
        let binding = input
            .component_binding(column)?
            .clone_observed(checkpoint)?;
        let mut locations = vector(owner, rows.len())?;
        let mut pages = Vec::new();
        let mut builder = ValueBuilder::new(ByteLayout::BinaryView);
        let mut bytes_in_page = 0usize;
        for (i, &row) in rows.iter().enumerate() {
            checkpoint_at(i, &checkpoint)?;
            if !input.input.selected().contains(row) {
                return Err(invariant(ValueViolation::UndefinedRow));
            }
            let physical = super::pairs::physical_row(mapping, row)?;
            // This is the original cooperative serializer, not a new key codec.
            // Its temporary is released after copying this representative.
            let bytes = input.tuples().encode_component_row(
                column,
                row,
                input.allocator.clone(),
                &checkpoint,
            )?;
            if !builder.is_empty() && bytes_in_page >= target {
                publish(
                    builder,
                    &mut pages,
                    &binding,
                    owner,
                    Arc::clone(&host),
                    control,
                )?;
                builder = ValueBuilder::new(ByteLayout::BinaryView);
                bytes_in_page = 0;
            }
            loop {
                let result = builder.append_bytes(
                    &bytes,
                    &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                    &mut |_| checkpoint(),
                );
                match result {
                    Ok(()) => break,
                    Err(WriteFailure::Full) if !builder.is_empty() => {
                        publish(
                            builder,
                            &mut pages,
                            &binding,
                            owner,
                            Arc::clone(&host),
                            control,
                        )?;
                        builder = ValueBuilder::new(ByteLayout::BinaryView);
                        bytes_in_page = 0;
                    }
                    Err(WriteFailure::Full) => {
                        return Err(invariant(ValueViolation::RowTransaction));
                    }
                    Err(WriteFailure::Failure(cause)) => return Err(original(cause)),
                }
            }
            locations.push(Location {
                page: pages.len(),
                row: builder.len() - 1,
                is_null: source.is_null(physical),
            });
            bytes_in_page = bytes_in_page.saturating_add(16).saturating_add(bytes.len());
        }
        if !builder.is_empty() || pages.is_empty() {
            publish(builder, &mut pages, &binding, owner, host, control)?;
        }
        checkpoint()?;
        Ok(Self {
            pages,
            locations,
            binding,
            spec: input.spec.clone(),
            column,
        })
    }
    pub(super) fn len(&self) -> usize {
        self.locations.len()
    }
    pub(super) fn append_reads<'a>(
        &'a self,
        requests: &[super::read::ReadRequest],
        output: &mut Vec<super::read::KeyReadPage<'a>>,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        super::read::append_pages(
            requests,
            |row| {
                let pos = self
                    .locations
                    .get(row)
                    .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                Ok((pos.page, pos.row))
            },
            |page| {
                let data = self
                    .pages
                    .get(page)
                    .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                Ok(super::read::KeyReadValues::Canonical {
                    encoded: data.values()?,
                    logical_binding: &self.binding,
                })
            },
            output,
            owner,
            checkpoint,
        )
    }
    #[cfg(test)]
    pub(super) fn read(&self, row: usize) -> ExecutionResult<CanonicalKeyRead<'_>> {
        let location = self
            .locations
            .get(row)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        let page = self
            .pages
            .get(location.page)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        let values = page.values()?;
        if location.row >= page.page.len() {
            return Err(invariant(ValueViolation::Bounds));
        }
        Ok(CanonicalKeyRead {
            binding: &self.binding,
            payload: canonical_component_payload(values.value(location.row))?,
        })
    }
    /// Ordered input logical rows and owned row coordinates. On failure the
    /// caller discards the whole output, including any completed prefix.
    pub(super) fn compare_pairs(
        &self,
        input: &PreparedKeyBatch<'_>,
        column: usize,
        input_rows: &[usize],
        owned_rows: &[usize],
        output: &mut [u8],
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        checkpoint()?;
        if column != self.column
            || !self.spec.same_instance(&input.spec)
            || input_rows.len() != owned_rows.len()
            || input_rows.len() != output.len()
        {
            return Err(invariant(ValueViolation::Layout));
        }
        let (source, mapping) = input.tuples().physical_component(column)?;
        let nulls_equal = self.spec.nulls()[column] == NullPolicy::Equal;
        let mut cached: Option<(usize, &BinaryViewArray)> = None;
        for (i, ((&row, &owned_row), out)) in input_rows
            .iter()
            .zip(owned_rows)
            .zip(output.iter_mut())
            .enumerate()
        {
            checkpoint_at(i, checkpoint)?;
            if !input.input.selected().contains(row) {
                return Err(invariant(ValueViolation::UndefinedRow));
            }
            let physical = super::pairs::physical_row(mapping, row)?;
            let stored = self
                .locations
                .get(owned_row)
                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
            let input_null = source.is_null(physical);
            *out = if input_null || stored.is_null {
                u8::from(input_null && stored.is_null && nulls_equal)
            } else {
                // Candidate collection groups by owned page. Bind the concrete
                // encoded carrier at page boundaries, not inside a bool table
                // callback or by re-decoding the entire store.
                if cached.is_none_or(|(page, _)| page != stored.page) {
                    let page = self
                        .pages
                        .get(stored.page)
                        .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                    cached = Some((stored.page, page.values()?));
                }
                let encoded = cached.expect("bound owned page").1;
                if stored.row >= encoded.len() {
                    return Err(invariant(ValueViolation::Bounds));
                }
                u8::from(canonical_nullable_equals_observed(
                    source,
                    physical,
                    encoded.value(stored.row),
                    checkpoint,
                )?)
            };
        }
        checkpoint()
    }
}
impl CanonicalPage {
    fn values(&self) -> ExecutionResult<&BinaryViewArray> {
        self.array
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .ok_or_else(|| invariant(ValueViolation::Carrier))
    }
}
fn publish(
    builder: ValueBuilder,
    pages: &mut Vec<CanonicalPage>,
    declaration: &FunctionValueType,
    owner: &dyn InputConstructionOwner,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<()> {
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
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
    // This is private encoded Binary storage. The cold declaration retains its
    // semantic metadata only; it does not bind List/Struct values to BinaryView.
    built.array = super::store::retain_created_array(
        built.array,
        declaration,
        &owner.storage_lifetime(),
        host,
        control,
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
    reserve_push(pages, owner, &checkpoint)?;
    pages.push(CanonicalPage { array, page });
    checkpoint()
}
