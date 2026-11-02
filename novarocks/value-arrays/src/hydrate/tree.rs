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

use super::{
    HydrateEvent, HydrateObserver, HydrateOptions, HydratePathStep, HydrateProjection,
    HydrateRequirements, child, invalid, reserve,
};
use crate::{
    ByteLayout, ValueError, ValueFailure, ValueViolation,
    access::ByteSource,
    arrow_raw::{self, StructuralRef},
    control::{Growth, Work},
    decode::{DecodePlan, ValueConversionContext},
    reachability::SelectionSet,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use novarocks_failure_contract::{RepresentationBoundary, RepresentationFacts};
use novarocks_type_contract::BorrowedTypedValueBinding;
use std::{alloc::Layout, sync::Arc};

pub(super) struct Node {
    pub source: ArrayRef,
    pub children: Vec<Node>,
    pub layout: Option<ByteLayout>,
    // Reading and output layout are separate only for an explicit plain-byte
    // projection. Dictionary parse policy continues to use frozen output L.
    pub source_layout: Option<ByteLayout>,
    pub convert: bool,
    pub clear_nulls: bool,
    pub rebuild: bool,
    pub keep: bool,
    pub dictionary_layout: Option<ByteLayout>,
    pub decode_plan: Option<DecodePlan>,
    pub planned_payload: usize,
    pub planned_regions: usize,
    pub active: SelectionSet,
    extent: u128,
    row_extent: u128,
    row_largest: u64,
}
impl Node {
    pub(super) fn plan<E>(
        source: &ArrayRef,
        binding: BorrowedTypedValueBinding<'_>,
        options: HydrateOptions,
        path: &mut [HydratePathStep],
        depth: usize,
        keep_encoded: &mut impl FnMut(&[HydratePathStep], &DataType) -> bool,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut HydrateObserver<'_, E>,
    ) -> Result<Self, ValueFailure<E>> {
        if depth >= path.len() {
            return Err(ValueError::Invariant(ValueViolation::Depth).into());
        }
        work(1).map_err(ValueFailure::Caller)?;
        observe(HydrateEvent::InspectType(source.data_type())).map_err(ValueFailure::Caller)?;
        let layout = ByteLayout::from_data_type(binding.data_type());
        let dictionary = matches!(source.data_type(), DataType::Dictionary(..));
        let keep = dictionary && keep_encoded(&path[..depth], source.data_type());
        let source_layout =
            if options.projection == HydrateProjection::ExactByteLayouts && !dictionary {
                ByteLayout::from_data_type(source.data_type())
            } else {
                layout
            };
        let byte_projection = options.projection == HydrateProjection::ExactByteLayouts
            && !dictionary
            && layout.is_some()
            && layout != source_layout;
        let required_inactive_nulls = !binding.value_type().nullable() && source.null_count() != 0;
        let convert = dictionary && !keep
            || !dictionary && layout.is_some() && required_inactive_nulls
            || byte_projection;
        let clear_nulls = !dictionary && layout.is_none() && required_inactive_nulls;
        if convert && layout.is_none() {
            return Err(invalid().into());
        }
        let active =
            SelectionSet::try_new_observed(if convert { source.len() } else { 0 }, growth)?;
        let mut result = Self {
            source: Arc::clone(source),
            children: Vec::new(),
            layout,
            source_layout,
            convert,
            clear_nulls,
            rebuild: convert || clear_nulls,
            keep,
            dictionary_layout: options.dictionary_value_layout,
            decode_plan: None,
            planned_payload: 0,
            planned_regions: 0,
            active,
            extent: 0,
            row_extent: 0,
            row_largest: 0,
        };
        let mut push = |input: &ArrayRef,
                        index: usize,
                        step: HydratePathStep|
         -> Result<(), ValueFailure<E>> {
            path[depth] = step;
            let node = Self::plan(
                input,
                child(binding, index)?,
                options,
                path,
                depth + 1,
                keep_encoded,
                growth,
                work,
                observe,
            )?;
            let count = result.children.len() + 1;
            reserve(&mut result.children, count, growth)?;
            result.children.push(node);
            Ok(())
        };
        match arrow_raw::structural_view(source)? {
            StructuralRef::Struct { children, .. } => {
                for (index, input) in children.iter().enumerate() {
                    push(input, index, HydratePathStep::StructField(index))?;
                }
            }
            StructuralRef::Union { fields, array } => {
                for (index, (id, _)) in fields.iter().enumerate() {
                    push(array.child(id), index, HydratePathStep::UnionField(id))?;
                }
            }
            StructuralRef::List32 { values, .. }
            | StructuralRef::List64 { values, .. }
            | StructuralRef::FixedSizeList { values, .. } => {
                push(values, 0, HydratePathStep::ListItem)?
            }
            StructuralRef::Map {
                keys,
                values,
                sorted,
                ..
            } => {
                if !matches!(binding.data_type(), DataType::Map(_, expected) if *expected == sorted)
                {
                    return Err(invalid().into());
                }
                push(keys, 0, HydratePathStep::MapKey)?;
                push(values, 1, HydratePathStep::MapValue)?;
            }
            StructuralRef::Leaf => {}
        }
        if !result.children.is_empty() {
            result.rebuild |= result.children.iter().any(|child| child.rebuild)
                || super::execution::local_metadata_changes(
                    source.data_type(),
                    binding.data_type(),
                )?;
        }
        Ok(result)
    }
    pub(super) fn clear_row(&mut self) {
        self.row_extent = 0;
        self.row_largest = 0;
        for child in &mut self.children {
            child.clear_row();
        }
    }
    pub(super) fn accumulate_row<E>(
        &mut self,
        row: usize,
        work: &mut Work<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        work(1).map_err(ValueFailure::Caller)?;
        if self.convert {
            let source = ByteSource::parse(
                &self.source,
                self.source_layout.ok_or_else(invalid)?,
                self.dictionary_layout,
            )?;
            if let Some(length) = source.value_len(row)? {
                self.row_extent = self
                    .row_extent
                    .checked_add(length as u128)
                    .ok_or_else(invalid)?;
                self.row_largest = self.row_largest.max(length as u64);
            }
            return Ok(());
        }
        if self.source.is_null(row) {
            return Ok(());
        }
        child_rows(&self.source, row, &mut |index, start, end| {
            for child_row in start..end {
                self.children[index].accumulate_row(child_row, work)?;
            }
            Ok(())
        })
    }
    pub(super) fn row_boundary(
        &self,
        prefix: usize,
    ) -> Result<Option<RepresentationFacts>, ValueError> {
        if self.convert {
            let layout = self.layout.ok_or_else(invalid)?;
            if let Some(facts) = check_extent(
                layout,
                self.extent,
                self.row_extent,
                self.row_largest,
                prefix,
            )? {
                return Ok(Some(facts));
            }
        }
        for child in &self.children {
            if let Some(facts) = child.row_boundary(prefix)? {
                return Ok(Some(facts));
            }
        }
        Ok(None)
    }
    pub(super) fn commit_row(&mut self) {
        self.extent += self.row_extent;
        for child in &mut self.children {
            child.commit_row();
        }
    }
    pub(super) fn mark_row<E>(
        &mut self,
        row: usize,
        work: &mut Work<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        work(1).map_err(ValueFailure::Caller)?;
        if self.convert {
            self.active.insert(row)?;
            return Ok(());
        }
        if self.source.is_null(row) {
            return Ok(());
        }
        child_rows(&self.source, row, &mut |index, start, end| {
            for child_row in start..end {
                self.children[index].mark_row(child_row, work)?;
            }
            Ok(())
        })
    }
    pub(super) fn keep_decisions<E>(
        &self,
        out: &mut Vec<bool>,
        growth: &mut Growth<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        if matches!(self.source.data_type(), DataType::Dictionary(..)) {
            let count = out.len() + 1;
            reserve(out, count, growth)?;
            out.push(self.keep);
        }
        for child in &self.children {
            child.keep_decisions(out, growth)?;
        }
        Ok(())
    }
    pub(super) fn prepare_decode<E>(
        &mut self,
        binding: BorrowedTypedValueBinding<'_>,
        context: &mut ValueConversionContext,
        storage: &novarocks_type_contract::ValueTypeStorageLifetime,
        path: &mut [HydratePathStep],
        depth: usize,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut HydrateObserver<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        if self.convert && self.layout.is_some_and(ByteLayout::is_view) {
            let source = ByteSource::parse(
                &self.source,
                self.source_layout.ok_or_else(invalid)?,
                self.dictionary_layout,
            )?
            .with_storage(storage);
            self.decode_plan = Some(context.plan_decode_structured_observed(
                &source,
                &crate::reachability::ReachableRows::Selection(&self.active),
                growth,
                work,
                &mut |event| {
                    observe(HydrateEvent::ValueStructure {
                        path: &path[..depth],
                        event,
                    })
                },
            )?);
            observe(HydrateEvent::OpaqueOperationFinished).map_err(ValueFailure::Caller)?;
        }
        for (index, node) in self.children.iter_mut().enumerate() {
            path[depth] = match arrow_raw::structural_view(&self.source)? {
                StructuralRef::Struct { .. } => HydratePathStep::StructField(index),
                StructuralRef::Union { fields, .. } => {
                    HydratePathStep::UnionField(fields.iter().nth(index).ok_or_else(invalid)?.0)
                }
                StructuralRef::Map { .. } => {
                    if index == 0 {
                        HydratePathStep::MapKey
                    } else {
                        HydratePathStep::MapValue
                    }
                }
                _ => HydratePathStep::ListItem,
            };
            node.prepare_decode(
                child(binding, index)?,
                context,
                storage,
                path,
                depth + 1,
                growth,
                work,
                observe,
            )?;
        }
        Ok(())
    }
    pub(super) fn visit_output_storage<E>(
        &self,
        binding: BorrowedTypedValueBinding<'_>,
        visit: &mut impl FnMut(super::PlannedOutputStorage) -> Result<(), E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        use super::PlannedOutputStorage;
        if self.keep {
            return crate::storage::visit_borrowed(self.source.as_ref(), &mut |node| {
                work(1).map_err(ValueFailure::Caller)?;
                let (rows, source_offset) = match &node {
                    crate::storage::BorrowedNode::Array { array, .. } => {
                        (array.len(), array.offset())
                    }
                    crate::storage::BorrowedNode::RunEnds { .. } => return Err(invalid().into()),
                };
                visit(PlannedOutputStorage::Node {
                    rows,
                    source_offset,
                })
                .map_err(ValueFailure::Caller)?;
                if let Some(nulls) = node.nulls() {
                    visit(PlannedOutputStorage::Buffer {
                        bytes: nulls.inner().inner().len(),
                        newly_owned: false,
                    })
                    .map_err(ValueFailure::Caller)?;
                }
                for index in 0..node.data_buffers() {
                    visit(PlannedOutputStorage::Buffer {
                        bytes: node.buffer(index)?.len(),
                        newly_owned: false,
                    })
                    .map_err(ValueFailure::Caller)?;
                }
                Ok(())
            })
            .map_err(|error| match error {
                ValueFailure::Value(error) => ValueFailure::Value(error),
                ValueFailure::Caller(error) => error,
            });
        }
        work(1).map_err(ValueFailure::Caller)?;
        visit(PlannedOutputStorage::Node {
            rows: self.source.len(),
            source_offset: self.source.offset(),
        })
        .map_err(ValueFailure::Caller)?;
        let mut buffer = |bytes, newly_owned| {
            visit(PlannedOutputStorage::Buffer { bytes, newly_owned }).map_err(ValueFailure::Caller)
        };
        if self.convert {
            let layout = self.layout.ok_or_else(invalid)?;
            // The builder may eliminate an all-valid bitmap; the bound remains
            // valid for nullable keys, NULL values and inactive ancestors.
            if binding.value_type().nullable() {
                buffer(self.source.len().div_ceil(8), true)?;
            }
            if layout.is_view() {
                buffer(self.source.len().checked_mul(16).ok_or_else(invalid)?, true)?;
                buffer(self.planned_payload, true)?;
                if let Some(plan) = &self.decode_plan {
                    let bottom = crate::storage::borrowed_node(plan.decoded().bottom().as_ref())?;
                    for index in 1..bottom.data_buffers() {
                        buffer(bottom.buffer(index)?.len(), false)?;
                    }
                }
            } else {
                let width = if matches!(layout, ByteLayout::Utf8 | ByteLayout::Binary) {
                    4
                } else {
                    8
                };
                buffer(
                    self.source
                        .len()
                        .checked_add(1)
                        .and_then(|n| n.checked_mul(width))
                        .ok_or_else(invalid)?,
                    true,
                )?;
                buffer(self.planned_payload, true)?;
            }
            return Ok(());
        }
        let node = crate::storage::borrowed_node(self.source.as_ref())?;
        if !self.clear_nulls
            && let Some(nulls) = node.nulls()
        {
            buffer(nulls.inner().inner().len(), false)?;
        }
        for index in 0..node.data_buffers() {
            buffer(node.buffer(index)?.len(), false)?;
        }
        for (index, child_node) in self.children.iter().enumerate() {
            child_node.visit_output_storage(child(binding, index)?, visit, work)?;
        }
        Ok(())
    }
    pub(super) fn decode_storage_bytes(&self) -> (usize, usize) {
        let own = self.decode_plan.as_ref().map_or((0, 0), |p| {
            (p.new_plan_storage_bytes(), p.shared_cached_storage_bytes())
        });
        self.children.iter().fold(own, |(new, shared), child| {
            let (cn, cs) = child.decode_storage_bytes();
            (new + cn, shared + cs)
        })
    }
    pub(super) fn requirements<E>(
        &mut self,
        out: &mut HydrateRequirements,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        if self.convert {
            let layout = self.layout.ok_or_else(invalid)?;
            let source = ByteSource::parse(
                &self.source,
                self.source_layout.ok_or_else(invalid)?,
                self.dictionary_layout,
            )?;
            let mut logical = 0u128;
            let mut copied = 0u128;
            for row in self.active.iter() {
                work(1).map_err(ValueFailure::Caller)?;
                if let Some(length) = source.value_len(row)? {
                    logical += length as u128;
                    if !layout.is_view() || length <= 12 {
                        copied += length as u128;
                    }
                }
            }
            out.logical_bytes = out.logical_bytes.checked_add(logical).ok_or_else(invalid)?;
            self.planned_payload = usize::try_from(copied).map_err(|_| invalid())?;
            self.planned_regions = if layout.is_view() {
                self.active.count()
            } else {
                0
            };
            let layouts =
                crate::build::ValueBuilder::<allocator_api2::alloc::Global>::planned_layouts(
                    layout,
                    self.source.len(),
                    self.planned_payload,
                    self.planned_regions,
                )?;
            let count = out.output.len() + layouts.len();
            reserve(&mut out.output, count, growth)?;
            out.output.extend(layouts);
        }
        if !self.children.is_empty() && self.rebuild {
            let count = out.execution_scratch.len() + 2;
            reserve(&mut out.execution_scratch, count, growth)?;
            out.execution_scratch
                .push(Layout::array::<ArrayRef>(self.children.len()).map_err(|_| invalid())?);
            out.execution_scratch
                .push(Layout::array::<DataType>(self.children.len()).map_err(|_| invalid())?);
        }
        for child in &mut self.children {
            child.requirements(out, growth, work)?;
        }
        Ok(())
    }
    pub(super) fn held_scratch_bytes(&self) -> usize {
        self.children.capacity() * std::mem::size_of::<Node>()
            + self.active.descriptor_bytes()
            + self
                .decode_plan
                .as_ref()
                .map_or(0, DecodePlan::held_plan_storage_bytes)
            + self
                .children
                .iter()
                .map(Self::held_scratch_bytes)
                .sum::<usize>()
    }
}
/// Pure real-field arithmetic, also used by the no-multi-GiB boundary oracle.
pub(super) fn check_extent(
    layout: ByteLayout,
    committed: u128,
    row: u128,
    largest: u64,
    prefix: usize,
) -> Result<Option<RepresentationFacts>, ValueError> {
    let limit = layout.extent_limit();
    let required = if layout.is_view() {
        u128::from(largest)
    } else {
        committed.checked_add(row).ok_or_else(invalid)?
    };
    if required <= u128::from(limit) {
        return Ok(None);
    }
    let minimum = if layout.is_view() {
        u128::from(largest)
    } else {
        row
    };
    Ok(Some(layout.representation(
        if prefix == 0 && minimum > u128::from(limit) {
            RepresentationBoundary::MinimumUnit
        } else {
            RepresentationBoundary::SingleOutput
        },
        u64::try_from(required).ok(),
        Some(limit),
        Some(prefix as u64),
    )))
}

/// Actual source geometry; no metadata or projected child length authors rows.
pub(super) fn child_rows<E>(
    source: &ArrayRef,
    row: usize,
    visit: &mut impl FnMut(usize, usize, usize) -> Result<(), ValueFailure<E>>,
) -> Result<(), ValueFailure<E>> {
    if row >= source.len() {
        return Err(ValueError::Invariant(ValueViolation::Bounds).into());
    }
    let range = |start: usize, end: usize, len: usize| {
        if start > end || end > len {
            Err(invalid())
        } else {
            Ok((start, end))
        }
    };
    match arrow_raw::structural_view(source)? {
        StructuralRef::Union { fields, array } => {
            let id = array.type_id(row);
            let index = fields
                .iter()
                .position(|(candidate, _)| candidate == id)
                .ok_or_else(invalid)?;
            let offset = array.value_offset(row);
            let (start, end) = range(
                offset,
                offset.checked_add(1).ok_or_else(invalid)?,
                array.child(id).len(),
            )?;
            visit(index, start, end)?;
        }
        StructuralRef::Leaf => {}
        StructuralRef::Struct { children, .. } => {
            for (index, child) in children.iter().enumerate() {
                if row >= child.len() {
                    return Err(invalid().into());
                }
                visit(index, row, row + 1)?;
            }
        }
        StructuralRef::List32 {
            offsets, values, ..
        } => {
            let (start, end) = range(
                usize::try_from(offsets[row]).map_err(|_| invalid())?,
                usize::try_from(offsets[row + 1]).map_err(|_| invalid())?,
                values.len(),
            )?;
            visit(0, start, end)?;
        }
        StructuralRef::List64 {
            offsets, values, ..
        } => {
            let (start, end) = range(
                usize::try_from(offsets[row]).map_err(|_| invalid())?,
                usize::try_from(offsets[row + 1]).map_err(|_| invalid())?,
                values.len(),
            )?;
            visit(0, start, end)?;
        }
        StructuralRef::FixedSizeList {
            width,
            values,
            child_start,
            ..
        } => {
            let start = row
                .checked_mul(width)
                .and_then(|v| v.checked_add(child_start))
                .ok_or_else(invalid)?;
            let end = start.checked_add(width).ok_or_else(invalid)?;
            let (start, end) = range(start, end, values.len())?;
            visit(0, start, end)?;
        }
        StructuralRef::Map {
            offsets,
            keys,
            values,
            ..
        } => {
            let (start, end) = range(
                usize::try_from(offsets[row]).map_err(|_| invalid())?,
                usize::try_from(offsets[row + 1]).map_err(|_| invalid())?,
                keys.len().min(values.len()),
            )?;
            visit(0, start, end)?;
            visit(1, start, end)?;
        }
    }
    Ok(())
}
