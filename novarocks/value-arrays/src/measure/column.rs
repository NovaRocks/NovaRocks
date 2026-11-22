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

use super::{ColumnTotals, allocate, arithmetic};
use crate::{
    ByteLayout, ValueError, ValueFailure, ValueViolation,
    access::ByteSource,
    arrow_raw::{self, ByteSpan, StructuralRef},
    build::ValueBuilder,
    control::{Growth, Work},
    error::field_representation,
    hydrate::{check_extent, child_rows},
};
use arrow_array::{Array, ArrayRef};
use arrow_buffer::alloc::ALIGNMENT;
use arrow_schema::DataType;
use novarocks_failure_contract::RepresentationBoundary;
use std::{ops::Range, sync::Arc};

#[derive(Clone, Copy)]
pub(super) enum Shape {
    Null,
    Boolean,
    Fixed(usize),
    Bytes {
        source: ByteInput,
        target: ByteLayout,
    },
    Dictionary(ByteLayout),
    Struct,
    List(usize),
    FixedList(usize),
    Map,
}

#[derive(Clone, Copy)]
pub(super) enum ByteInput {
    Variable(ByteLayout),
    Fixed,
}

#[derive(Clone, Copy)]
pub(super) struct SourcePolicy {
    pub(super) constant: bool,
    pub(super) borrow: bool,
    pub(super) project_fixed_binary: bool,
}

#[derive(Clone, Copy, Default)]
pub(super) struct Facts {
    pub rows: usize,
    pub nulls: usize,
    pub logical_bytes: u128,
    pub adopted: usize,
    pub inline_bytes: usize,
    largest_value: u64,
}

impl Facts {
    fn add(self, other: Self, repeats: usize) -> Result<Self, ValueError> {
        fn add_count(committed: usize, row: usize, repeats: usize) -> Result<usize, ValueError> {
            row.checked_mul(repeats)
                .and_then(|n| committed.checked_add(n))
                .ok_or_else(arithmetic)
        }

        Ok(Self {
            rows: add_count(self.rows, other.rows, repeats)?,
            nulls: add_count(self.nulls, other.nulls, repeats)?,
            logical_bytes: other
                .logical_bytes
                .checked_mul(repeats as u128)
                .and_then(|n| self.logical_bytes.checked_add(n))
                .ok_or_else(arithmetic)?,
            adopted: add_count(self.adopted, other.adopted, repeats)?,
            inline_bytes: add_count(self.inline_bytes, other.inline_bytes, repeats)?,
            largest_value: self.largest_value.max(other.largest_value),
        })
    }
}

pub(super) struct Column {
    pub source: ArrayRef,
    pub target: DataType,
    pub constant: bool,
    pub borrow: bool,
    pub shape: Shape,
    pub children: Vec<Column>,
    pub committed: Facts,
    candidate: Facts,
    constant_candidate_ready: bool,
}

impl Column {
    pub(super) fn new<E>(
        source: &ArrayRef,
        target: &DataType,
        policy: SourcePolicy,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        depth: usize,
    ) -> Result<Self, ValueFailure<E>> {
        if depth >= novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
            return Err(ValueError::Invariant(ValueViolation::Depth).into());
        }
        work(1).map_err(ValueFailure::Caller)?;
        let structural = arrow_raw::structural_view(source)?;
        let child_count = match &structural {
            StructuralRef::Struct { children, .. } => children.len(),
            StructuralRef::List32 { .. }
            | StructuralRef::List64 { .. }
            | StructuralRef::FixedSizeList { .. } => 1,
            StructuralRef::Map { .. } => 2,
            StructuralRef::Leaf | StructuralRef::Union { .. } => 0,
        };
        let mut children = allocate(child_count, growth)?;
        let child_policy = SourcePolicy {
            constant: false,
            ..policy
        };
        let mut child = |source: &ArrayRef, target: &DataType| {
            Self::new(source, target, child_policy, growth, work, depth + 1)
        };
        let shape = match (structural, target) {
            (
                StructuralRef::Struct {
                    children: sources, ..
                },
                DataType::Struct(fields),
            ) if sources.len() == fields.len() => {
                // Struct children use the same row coordinates as their
                // parent. Field metadata belongs to the target declaration.
                for (source, field) in sources.iter().zip(fields) {
                    let node = child(source, field.data_type())?;
                    children.push(node);
                }
                Shape::Struct
            }
            (StructuralRef::List32 { values, .. }, DataType::List(field)) => {
                let node = child(values, field.data_type())?;
                children.push(node);
                Shape::List(4)
            }
            (StructuralRef::List64 { values, .. }, DataType::LargeList(field)) => {
                let node = child(values, field.data_type())?;
                children.push(node);
                Shape::List(8)
            }
            (
                StructuralRef::FixedSizeList { values, width, .. },
                DataType::FixedSizeList(field, target_width),
            ) if usize::try_from(*target_width).ok() == Some(width) => {
                let node = child(values, field.data_type())?;
                children.push(node);
                Shape::FixedList(width)
            }
            (
                StructuralRef::Map {
                    keys,
                    values,
                    sorted,
                    ..
                },
                DataType::Map(entries, target_sorted),
            ) if sorted == *target_sorted => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(layout().into());
                };
                let [key, value] = fields.as_ref() else {
                    return Err(layout().into());
                };
                let key = child(keys, key.data_type())?;
                let value = child(values, value.data_type())?;
                children.push(key);
                children.push(value);
                Shape::Map
            }
            (StructuralRef::Leaf, target) => leaf(source, target, policy.project_fixed_binary)?,
            _ => return Err(layout().into()),
        };
        Ok(Self {
            source: Arc::clone(source),
            target: target.clone(),
            constant: policy.constant,
            borrow: policy.borrow,
            shape,
            children,
            committed: Facts::default(),
            candidate: Facts::default(),
            constant_candidate_ready: false,
        })
    }

    pub(super) fn uniform(&self) -> bool {
        self.constant || self.fixed_slice_width().is_some()
    }

    fn fixed_slice_width(&self) -> Option<usize> {
        if !self.borrow {
            return None;
        }
        match self.shape {
            Shape::Fixed(width) => Some(width),
            Shape::Boolean => Some(1),
            Shape::Null => Some(0),
            _ => None,
        }
    }

    pub(super) fn count_fixed_slice<E>(
        &self,
        range: Range<usize>,
        work: &mut Work<'_, E>,
    ) -> Result<u128, ValueFailure<E>> {
        let Some(width) = self.fixed_slice_width() else {
            return Ok(0);
        };
        let mut nulls = 0;
        if let Some(validity) = self.source.nulls() {
            // Count the existing bitmap in word groups. A fixed-width
            // run need not visit each value, including nullable columns.
            const COUNT_TURN: usize = 4096;
            for start in range.clone().step_by(COUNT_TURN) {
                let take = (range.end - start).min(COUNT_TURN);
                work(take).map_err(ValueFailure::Caller)?;
                nulls += validity.slice(start, take).null_count();
            }
        }
        Ok((range.len() - nulls) as u128 * width as u128)
    }

    pub(super) fn prepare_candidate<E>(
        &mut self,
        row: usize,
        work: &mut Work<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        if self.constant_candidate_ready {
            return Ok(());
        }
        self.clear_candidate();
        if self.fixed_slice_width().is_some() {
            // The original slice owns its geometry. Its logical width comes
            // from the accepted range once all columns agree on the prefix.
            self.candidate.rows = 1;
        } else {
            self.accumulate_candidate(Some(if self.constant { 0 } else { row }), work)?;
        }
        self.constant_candidate_ready = self.constant;
        Ok(())
    }

    pub(super) fn clear_prefix(&mut self) {
        self.committed = Facts::default();
        for child in &mut self.children {
            child.clear_prefix();
        }
    }

    fn clear_candidate(&mut self) {
        self.candidate = Facts::default();
        for child in &mut self.children {
            child.clear_candidate();
        }
    }

    fn accumulate_candidate<E>(
        &mut self,
        row: Option<usize>,
        work: &mut Work<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        work(1).map_err(ValueFailure::Caller)?;
        self.candidate.rows = self.candidate.rows.checked_add(1).ok_or_else(arithmetic)?;
        if let Some(row) = row
            && row >= self.source.len()
        {
            return Err(arithmetic().into());
        }
        let row = row.filter(|&row| !self.source.is_null(row));
        if let Some(row) = row {
            match self.shape {
                Shape::Bytes { source, target } => {
                    let span = match source {
                        ByteInput::Variable(source) => self.byte_source(source)?.span(row)?,
                        ByteInput::Fixed => {
                            arrow_raw::fixed_binary_span(self.source.as_ref(), row)?
                        }
                    };
                    if let Some(span) = span {
                        let (bytes, region) = match span {
                            ByteSpan::Inline(bytes) => (bytes.len(), false),
                            ByteSpan::Region { len, .. } => (len, true),
                        };
                        self.add_bytes(bytes)?;
                        if target.is_view() {
                            if region && bytes > 12 {
                                self.candidate.adopted = self
                                    .candidate
                                    .adopted
                                    .checked_add(1)
                                    .ok_or_else(arithmetic)?;
                            } else {
                                self.candidate.inline_bytes = self
                                    .candidate
                                    .inline_bytes
                                    .checked_add(bytes)
                                    .ok_or_else(arithmetic)?;
                            }
                        }
                    } else {
                        // Plain output normalizes a valid dictionary key
                        // pointing to a NULL value into a physical NULL.
                        self.add_null()?;
                    }
                }
                Shape::Dictionary(values) => {
                    if let Some(bytes) = self.byte_source(values)?.value_len(row)? {
                        self.add_bytes(bytes)?;
                    }
                    // A valid key pointing to NULL stays a valid key here.
                    self.candidate.adopted = 1;
                }
                Shape::Fixed(bytes) => self.add_bytes(bytes)?,
                Shape::Boolean => self.add_bytes(1)?,
                _ => (),
            }
            let source = &self.source;
            let children = &mut self.children;
            child_rows(source, row, &mut |index, start, end| {
                for child_row in start..end {
                    children[index].accumulate_candidate(Some(child_row), work)?;
                }
                Ok(())
            })?;
        } else {
            self.add_null()?;
            if !self.borrow {
                match self.shape {
                    Shape::Struct => {
                        for child in &mut self.children {
                            child.accumulate_candidate(None, work)?;
                        }
                    }
                    Shape::FixedList(width) => {
                        for _ in 0..width {
                            self.children[0].accumulate_candidate(None, work)?;
                        }
                    }
                    _ => (),
                }
            }
        }
        Ok(())
    }

    fn add_null(&mut self) -> Result<(), ValueError> {
        self.candidate.nulls = self.candidate.nulls.checked_add(1).ok_or_else(arithmetic)?;
        Ok(())
    }

    fn add_bytes(&mut self, bytes: usize) -> Result<(), ValueError> {
        self.candidate.logical_bytes = self
            .candidate
            .logical_bytes
            .checked_add(bytes as u128)
            .ok_or_else(arithmetic)?;
        self.candidate.largest_value = self.candidate.largest_value.max(bytes as u64);
        Ok(())
    }

    pub(super) fn byte_source(&self, source: ByteLayout) -> Result<ByteSource<'_>, ValueError> {
        ByteSource::parse(&self.source, source, None)
    }

    pub(super) fn candidate_totals(
        &self,
        prefix: usize,
        run: usize,
        out: &mut ColumnTotals,
    ) -> Result<(), ValueError> {
        let facts = self.committed.add(self.candidate, run)?;
        if !self.borrow {
            self.validate_representation(prefix, run, facts)?;
            out.new_bytes = self
                .column_buffers(facts)
                .into_iter()
                .try_fold(out.new_bytes, |total, bytes| {
                    total.checked_add(bytes).ok_or_else(arithmetic)
                })?;
        }
        out.logical_bytes = out
            .logical_bytes
            .checked_add(facts.logical_bytes)
            .ok_or_else(arithmetic)?;
        out.shares_source |= self.shares_source(facts);
        for child in &self.children {
            child.candidate_totals(prefix, run, out)?;
        }
        Ok(())
    }

    fn shares_source(&self, facts: Facts) -> bool {
        facts.rows != 0
            && (self.borrow && !matches!(self.shape, Shape::Null)
                || matches!(self.shape, Shape::Dictionary(_))
                || facts.adopted != 0)
    }

    fn validate_representation(
        &self,
        prefix: usize,
        run: usize,
        facts: Facts,
    ) -> Result<(), ValueError> {
        let boundary_prefix = prefix.max(usize::from(run > 1));
        if let Shape::Bytes { target, .. } = self.shape
            && let Some(failure) = check_extent(
                target,
                self.committed.logical_bytes,
                facts.logical_bytes - self.committed.logical_bytes,
                facts.largest_value,
                boundary_prefix,
            )?
        {
            return Err(ValueError::Representation(failure));
        }
        let offset_limit = match self.shape {
            Shape::List(4) | Shape::Map => Some(i32::MAX as u64),
            Shape::List(8) => Some(i64::MAX as u64),
            _ => None,
        };
        if let Some(limit) = offset_limit {
            let child = &self.children[0];
            let count = child.committed.add(child.candidate, run)?.rows as u128;
            if count > u128::from(limit) {
                return Err(extent(9, count, limit, boundary_prefix));
            }
        }
        if let Shape::Bytes { target, .. } = self.shape {
            let payload = if target.is_view() {
                facts.inline_bytes
            } else {
                usize::try_from(facts.logical_bytes).map_err(|_| arithmetic())?
            };
            if let Err(error) = ValueBuilder::<allocator_api2::alloc::Global>::planned_layouts(
                target,
                facts.rows,
                payload,
                facts.adopted,
            ) {
                return Err(match error {
                    ValueError::Representation(mut facts) => {
                        if boundary_prefix == 0 {
                            facts.boundary = RepresentationBoundary::MinimumUnit;
                        }
                        facts.representable_prefix = Some(prefix as u64);
                        ValueError::Representation(facts)
                    }
                    other => other,
                });
            }
        } else {
            let [geometry, payload, validity] = self.column_buffers(facts);
            let geometry_alignment = match self.shape {
                Shape::List(width) => width,
                Shape::Map => 4,
                _ => ALIGNMENT,
            };
            for (bytes, alignment) in [
                (geometry, geometry_alignment),
                (payload, ALIGNMENT),
                (validity, ALIGNMENT),
            ] {
                let limit = isize::MAX as u64 & !(alignment as u64 - 1);
                if bytes > u128::from(limit) {
                    return Err(extent(11, bytes, limit, boundary_prefix));
                }
            }
        }
        Ok(())
    }

    fn column_buffers(&self, facts: Facts) -> [u128; 3] {
        if matches!(self.shape, Shape::Null) {
            return [0; 3];
        }
        let rows = facts.rows as u128;
        let validity = if facts.nulls == 0 {
            0
        } else {
            rows.div_ceil(8)
        };
        let (geometry, payload) = match self.shape {
            Shape::Null | Shape::Struct | Shape::FixedList(_) => (0, 0),
            Shape::Boolean => (rows.div_ceil(8), 0),
            Shape::Dictionary(_) => (4 * rows, 0),
            Shape::Fixed(width) => (0, width as u128 * rows),
            // ValueBuilder mounts its owned inline staging region along
            // with the view descriptors and the adopted source regions.
            Shape::Bytes { target, .. } if target.is_view() => {
                (16 * rows, facts.inline_bytes as u128)
            }
            Shape::Bytes { target, .. } => {
                let width = if matches!(target, ByteLayout::Utf8 | ByteLayout::Binary) {
                    4
                } else {
                    8
                };
                (width * (rows + 1), facts.logical_bytes)
            }
            Shape::List(width) => (width as u128 * (rows + 1), 0),
            Shape::Map => (4 * (rows + 1), 0),
        };
        [geometry, payload, validity]
    }

    pub(super) fn commit_candidate(&mut self, repeats: usize) -> Result<(), ValueError> {
        self.committed = self.committed.add(self.candidate, repeats)?;
        for child in &mut self.children {
            child.commit_candidate(repeats)?;
        }
        Ok(())
    }
}

fn leaf(
    source: &ArrayRef,
    target: &DataType,
    project_fixed_binary: bool,
) -> Result<Shape, ValueError> {
    let source_type = source.data_type();
    if project_fixed_binary
        && matches!(
            (source_type, target),
            (DataType::FixedSizeBinary(_), DataType::Binary)
        )
    {
        return Ok(Shape::Bytes {
            source: ByteInput::Fixed,
            target: ByteLayout::Binary,
        });
    }
    let source_layout = match source_type {
        DataType::Dictionary(keys, values) => {
            let value_layout = ByteLayout::from_data_type(values).ok_or_else(layout)?;
            if keys.as_ref() != &DataType::Int32 || !value_layout.is_utf8() {
                return Err(ValueError::Invariant(ValueViolation::Dictionary));
            }
            ByteSource::parse(source, value_layout, None)?;
            if target == source_type {
                return Ok(Shape::Dictionary(value_layout));
            }
            Some(value_layout)
        }
        _ => ByteLayout::from_data_type(source_type),
    };
    if let (Some(source), Some(target)) = (source_layout, ByteLayout::from_data_type(target))
        && source.is_utf8() == target.is_utf8()
    {
        return Ok(Shape::Bytes {
            source: ByteInput::Variable(source),
            target,
        });
    }
    if source_type != target {
        return Err(layout());
    }
    match target {
        DataType::Null => Ok(Shape::Null),
        DataType::Boolean => Ok(Shape::Boolean),
        DataType::FixedSizeBinary(bytes) => {
            Ok(Shape::Fixed(usize::try_from(*bytes).map_err(|_| layout())?))
        }
        target => target
            .primitive_width()
            .map(Shape::Fixed)
            .ok_or_else(layout),
    }
}

fn layout() -> ValueError {
    ValueError::Invariant(ValueViolation::Layout)
}

fn extent(code: u16, required: u128, limit: u64, prefix: usize) -> ValueError {
    ValueError::Representation(field_representation(
        code,
        if prefix == 0 {
            RepresentationBoundary::MinimumUnit
        } else {
            RepresentationBoundary::SingleOutput
        },
        u64::try_from(required).ok(),
        Some(limit),
        Some(prefix as u64),
    ))
}
