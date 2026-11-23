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
//! One original GROUP_CONCAT state, ordering, DISTINCT and Arrow state codec.
use crate::aggregate_format::scalar_to_string;
use crate::aggregate_scalar::{
    AggScalarValue, ScalarStateAllocator, ScalarWork, TrackedAggScalarValue,
    aggregate_vec_with_capacity, build_scalar_array, compare_tracked_scalar_values,
    tracked_scalar_from_array, tracked_scalar_heap_capacity, tracked_scalar_to_output,
};
use allocator_api2::vec::Vec as ScalarVec;
use arrow_array::{Array, ArrayRef, ListArray, StructArray, builder::StringBuilder};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields};
use std::{cmp::Ordering, sync::Arc};
#[path = "listagg_emission.rs"]
mod listagg_emission;
#[path = "aggregate_concat_source_failure.rs"]
mod source_failure;
const DEFAULT_SEPARATOR: &str = ",";
#[derive(Debug)]
pub struct GroupConcatState<A: ScalarStateAllocator> {
    pub allocator: A,
    pub rows: ScalarVec<ScalarVec<Option<TrackedAggScalarValue<A>>, A>, A>,
    pub failed: bool,
    payload_bytes: usize,
}
impl<A: ScalarStateAllocator> GroupConcatState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            rows: ScalarVec::new_in(allocator.clone()),
            allocator,
            failed: false,
            payload_bytes: 0,
        }
    }
    pub fn retained_bytes(&self) -> usize {
        self.rows.capacity() * std::mem::size_of::<ScalarVec<Option<TrackedAggScalarValue<A>>, A>>()
            + self.payload_bytes
    }
    pub fn latch_failure(&mut self) {
        self.failed = true;
        self.payload_bytes = 0;
        self.rows = ScalarVec::new_in(self.allocator.clone());
    }
    pub fn push_row(
        &mut self,
        row: ScalarVec<Option<TrackedAggScalarValue<A>>, A>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        work.flush()?;
        self.rows.try_reserve(1).map_err(|source| {
            self.allocator
                .scalar_allocation_error("reserve group_concat row", source)
        })?;
        work.flush()?;
        let mut retained = row.capacity() * std::mem::size_of::<Option<TrackedAggScalarValue<A>>>();
        for value in row.iter().flatten() {
            work.step()?;
            retained = retained
                .checked_add(tracked_scalar_heap_capacity(value, work)?)
                .ok_or(crate::KernelFailure::ResourceExhausted)?;
        }
        self.payload_bytes = self
            .payload_bytes
            .checked_add(retained)
            .ok_or(crate::KernelFailure::ResourceExhausted)?;
        self.rows.push(row);
        Ok(())
    }
    pub fn update_row(
        &mut self,
        columns: &[(&ArrayRef, usize)],
        layout: GroupConcatLayout,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        let mut values = aggregate_vec_with_capacity(
            &self.allocator,
            columns.len(),
            "reserve group_concat row values",
            work,
        )?;
        for (idx, (column, row)) in columns.iter().enumerate() {
            work.step()?;
            let value = tracked_scalar_from_array(column, *row, &self.allocator, work)?;
            if idx < layout.output_col_num && value.is_none() {
                return Ok(());
            }
            values.push(value);
        }
        self.push_row(values, work)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct GroupConcatLayout {
    pub output_col_num: usize,
    pub separator_idx: Option<usize>,
    pub order_by_start: usize,
    pub order_by_num: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupConcatShapeError {
    NoArguments,
    Carrier(novarocks_failure_contract::BoundedDetail),
    OrderColumns { arguments: usize, ordering: usize },
    NoOutputColumns,
    ArgumentLayout,
    IntermediateType(novarocks_failure_contract::BoundedDetail),
    NoIntermediateFields,
    IntermediateField { index: usize },
}
impl std::fmt::Display for GroupConcatShapeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Carrier(detail) => detail.fmt(f),
            Self::NoArguments => f.write_str("group_concat argument count must be positive"),
            Self::OrderColumns {
                arguments,
                ordering,
            } => write!(
                f,
                "group_concat argument count {arguments} is less than order-by columns {ordering}"
            ),
            Self::NoOutputColumns => f.write_str("group_concat output column should not be empty"),
            Self::ArgumentLayout => f.write_str("group_concat argument layout is invalid"),
            Self::IntermediateType(actual) => write!(
                f,
                "group_concat intermediate type must be STRUCT, got {actual}"
            ),
            Self::NoIntermediateFields => {
                f.write_str("group_concat intermediate type must contain at least one field")
            }
            Self::IntermediateField { index } => write!(
                f,
                "group_concat intermediate field {index} must be ARRAY type"
            ),
        }
    }
}
impl std::error::Error for GroupConcatShapeError {}
impl novarocks_failure_contract::FailureProjection for GroupConcatShapeError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(155).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for GroupConcatShapeError {}
impl From<GroupConcatShapeError> for crate::KernelFailure {
    fn from(source: GroupConcatShapeError) -> Self {
        Self::from_domain(source)
    }
}
impl GroupConcatShapeError {
    fn carrier(detail: impl std::fmt::Display) -> Self {
        Self::Carrier(novarocks_failure_contract::BoundedDetail::truncating_display(detail))
    }
}
impl GroupConcatLayout {
    pub fn infer(arg_count: usize, order_by_num: usize) -> Result<Self, GroupConcatShapeError> {
        if arg_count == 0 {
            return Err(GroupConcatShapeError::NoArguments);
        }
        // Subtraction follows the shape check, so even a maximum ordering
        // count is rejected without overflowing before validation.
        if arg_count <= order_by_num {
            return Err(GroupConcatShapeError::OrderColumns {
                arguments: arg_count,
                ordering: order_by_num,
            });
        }
        let value_and_separator = arg_count - order_by_num;
        let (output_col_num, separator_idx, order_by_start) = if value_and_separator == 1 {
            (value_and_separator, None, value_and_separator)
        } else {
            let output_col_num = value_and_separator - 1;
            (output_col_num, Some(output_col_num), output_col_num + 1)
        };
        if output_col_num == 0 {
            return Err(GroupConcatShapeError::NoOutputColumns);
        }
        if order_by_start.checked_add(order_by_num) != Some(arg_count) {
            return Err(GroupConcatShapeError::ArgumentLayout);
        }
        Ok(Self {
            output_col_num,
            separator_idx,
            order_by_start,
            order_by_num,
        })
    }
}

fn truncate_utf8_by_bytes(s: &mut String, max_bytes: usize) {
    if s.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
}

fn append_with_limit(out: &mut String, piece: &str, max_len: usize) -> bool {
    if max_len == 0 || piece.is_empty() {
        return max_len == 0;
    }
    let before = out.len();
    out.push_str(piece);
    if out.len() > max_len {
        truncate_utf8_by_bytes(out, max_len);
        return true;
    }
    before + piece.len() >= max_len
}

pub fn extract_arg_types(input_type: &DataType) -> Vec<DataType> {
    match input_type {
        DataType::Struct(fields) => fields.iter().map(|f| f.data_type().clone()).collect(),
        other => vec![other.clone()],
    }
}

pub fn build_default_intermediate_type(arg_types: &[DataType]) -> DataType {
    let fields = arg_types
        .iter()
        .enumerate()
        .map(|(idx, dt)| {
            Arc::new(Field::new(
                format!("c{idx}"),
                DataType::List(Arc::new(Field::new("item", dt.clone(), true))),
                true,
            ))
        })
        .collect::<Vec<_>>();
    DataType::Struct(Fields::from(fields))
}

pub fn validate_intermediate_type(ty: &DataType) -> Result<&Fields, GroupConcatShapeError> {
    let DataType::Struct(fields) = ty else {
        return Err(GroupConcatShapeError::IntermediateType(
            novarocks_failure_contract::BoundedDetail::truncating_display(format_args!("{ty:?}")),
        ));
    };
    if fields.is_empty() {
        return Err(GroupConcatShapeError::NoIntermediateFields);
    }
    for (index, field) in fields.iter().enumerate() {
        if !matches!(field.data_type(), DataType::List(_)) {
            return Err(GroupConcatShapeError::IntermediateField { index });
        }
    }
    Ok(fields)
}

/// Borrow the same original validated intermediate channels. Their exact
/// order and child NULL/layout facts come from the existing Struct/List owner.
#[derive(Clone, Copy)]
struct IntermediateArgTypes<'a>(&'a Fields);
impl<'a> IntermediateArgTypes<'a> {
    fn try_new(ty: &'a DataType) -> Result<Self, GroupConcatShapeError> {
        validate_intermediate_type(ty).map(Self)
    }
    fn len(self) -> usize {
        self.0.len()
    }
    fn get(self, index: usize) -> &'a DataType {
        let DataType::List(item) = self.0[index].data_type() else {
            unreachable!("validated group_concat ARRAY channel")
        };
        item.data_type()
    }
    fn iter(self) -> impl ExactSizeIterator<Item = &'a DataType> {
        self.0.iter().map(|field| {
            let DataType::List(item) = field.data_type() else {
                unreachable!("validated group_concat ARRAY channel")
            };
            item.data_type()
        })
    }
}
impl std::fmt::Debug for IntermediateArgTypes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}
struct MergeArgTypes<'a, 'b>(&'a [&'b ListArray]);
impl MergeArgTypes<'_, '_> {
    fn iter(&self) -> impl ExactSizeIterator<Item = &DataType> {
        self.0.iter().map(|array| {
            let DataType::List(item) = array.data_type() else {
                unreachable!("validated actual group_concat ListArray")
            };
            item.data_type()
        })
    }
}
impl std::fmt::Debug for MergeArgTypes<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}
pub fn intermediate_arg_types_borrowed(
    intermediate_type: &DataType,
) -> Result<impl ExactSizeIterator<Item = &DataType>, GroupConcatShapeError> {
    Ok(IntermediateArgTypes::try_new(intermediate_type)?.iter())
}

pub fn extract_input_columns(array: &ArrayRef) -> Result<Vec<ArrayRef>, GroupConcatShapeError> {
    match array.data_type() {
        DataType::Struct(_) => {
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    GroupConcatShapeError::carrier("group_concat input must be StructArray")
                })?;
            Ok(struct_arr.columns().to_vec())
        }
        _ => Ok(vec![array.clone()]),
    }
}

fn compare_optional_scalar<A: ScalarStateAllocator>(
    left: &Option<TrackedAggScalarValue<A>>,
    right: &Option<TrackedAggScalarValue<A>>,
    asc: bool,
    nulls_first: bool,
    listagg_order: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Ordering, crate::KernelFailure> {
    let ord = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(left), Some(right)) => {
            let ord = if listagg_order {
                crate::aggregate_scalar::ExtremaDirection::Minimum
                    .compare_tracked(left, right, work)?
            } else {
                compare_tracked_scalar_values(left, right, work)?
            };
            if asc { ord } else { ord.reverse() }
        }
    };
    Ok(ord)
}

fn sort_indices<A: ScalarStateAllocator>(
    state: &GroupConcatState<A>,
    layout: GroupConcatLayout,
    is_asc_order: &[bool],
    nulls_first: &[bool],
    listagg_order: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<usize, A>, crate::KernelFailure> {
    let rows = &state.rows;
    let mut indices = aggregate_vec_with_capacity(
        &state.allocator,
        rows.len(),
        "reserve aggregate sort indices",
        work,
    )?;
    for index in 0..rows.len() {
        work.step()?;
        indices.push(index);
    }
    work.flush()?;
    if layout.order_by_num == 0 {
        return Ok(indices);
    }

    work.flush()?;
    let mut error: Option<crate::KernelFailure> = None;
    indices.sort_unstable_by(|l, r| {
        if error.is_some() {
            return Ordering::Equal;
        }
        for key in 0..layout.order_by_num {
            let col = layout.order_by_start + key;
            match compare_optional_scalar(
                &rows[*l][col],
                &rows[*r][col],
                is_asc_order[key],
                nulls_first[key],
                listagg_order,
                work,
            ) {
                Ok(Ordering::Equal) => continue,
                Ok(ord) => return ord,
                Err(e) => {
                    error = Some(e);
                    return Ordering::Equal;
                }
            }
        }
        l.cmp(r)
    });
    if let Some(err) = error {
        return Err(err);
    }
    work.flush()?;
    Ok(indices)
}

fn rows_equal_on_output<A: ScalarStateAllocator>(
    left: &[Option<TrackedAggScalarValue<A>>],
    right: &[Option<TrackedAggScalarValue<A>>],
    layout: GroupConcatLayout,
    work: &mut ScalarWork<'_, '_>,
) -> Result<bool, crate::KernelFailure> {
    for col in 0..layout.output_col_num {
        work.step()?;
        match (&left[col], &right[col]) {
            (None, None) => {}
            (Some(left), Some(right)) => {
                if compare_tracked_scalar_values(left, right, work)? != Ordering::Equal {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn mark_duplicated_rows<A: ScalarStateAllocator>(
    rows: &[ScalarVec<Option<TrackedAggScalarValue<A>>, A>],
    sorted_indices: &[usize],
    layout: GroupConcatLayout,
    is_distinct: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<bool>, crate::KernelFailure> {
    work.flush()?;
    let mut duplicated = vec![false; rows.len()];
    work.flush()?;
    if !is_distinct {
        return Ok(duplicated);
    }
    for (pos, &idx) in sorted_indices.iter().enumerate() {
        work.step()?;
        for &next_idx in sorted_indices.iter().skip(pos + 1) {
            work.step()?;
            if rows_equal_on_output(&rows[idx], &rows[next_idx], layout, work)? {
                duplicated[idx] = true;
                break;
            }
        }
    }
    Ok(duplicated)
}

fn separator_for_row<A: ScalarStateAllocator>(
    row: &[Option<TrackedAggScalarValue<A>>],
    arg_types: IntermediateArgTypes<'_>,
    layout: GroupConcatLayout,
    work: &mut ScalarWork<'_, '_>,
) -> Result<String, crate::KernelFailure> {
    if let Some(sep_idx) = layout.separator_idx {
        if let Some(separator) = row
            .get(sep_idx)
            .ok_or_else(|| {
                crate::KernelFailure::from(GroupConcatShapeError::carrier(
                    "group_concat separator index out of bounds",
                ))
            })?
            .as_ref()
        {
            scalar_to_string(
                &tracked_scalar_to_output(separator, work)?,
                arg_types.get(sep_idx),
                work,
            )
        } else {
            Ok(String::new())
        }
    } else {
        Ok(DEFAULT_SEPARATOR.to_string())
    }
}

pub struct GroupConcatMerge<'a> {
    array: &'a StructArray,
    layout: GroupConcatLayout,
    list_columns: Vec<&'a ListArray>,
    value_columns: Vec<ArrayRef>,
    offsets: Vec<Vec<i32>>,
}
impl<'a> GroupConcatMerge<'a> {
    pub fn new(
        array: &'a ArrayRef,
        order_by_num: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self, crate::KernelFailure> {
        work.flush()?;
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                GroupConcatShapeError::carrier("group_concat merge input must be StructArray")
            })?;
        let field_num = struct_arr.num_columns();
        let layout = GroupConcatLayout::infer(field_num, order_by_num)?;
        let mut list_columns = Vec::with_capacity(field_num);
        let mut value_columns = Vec::with_capacity(field_num);
        let mut offsets = Vec::with_capacity(field_num);
        for (idx, col) in struct_arr.columns().iter().enumerate() {
            work.step()?;
            let list_col = col.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                GroupConcatShapeError::carrier(format_args!(
                    "group_concat merge field {} must be ListArray",
                    idx
                ))
            })?;
            let DataType::List(_) = list_col.data_type() else {
                return Err(GroupConcatShapeError::carrier(format_args!(
                    "group_concat merge field {} must be ARRAY type",
                    idx
                ))
                .into());
            };
            value_columns.push(list_col.values().clone());
            offsets.push(list_col.value_offsets().to_vec());
            list_columns.push(list_col);
        }

        work.flush()?;
        Ok(Self {
            array: struct_arr,
            layout,
            list_columns,
            value_columns,
            offsets,
        })
    }
    pub fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }
    pub fn merge_row<A: ScalarStateAllocator>(
        &self,
        state: &mut GroupConcatState<A>,
        row: usize,
        intermediate_type: &DataType,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        let field_num = self.list_columns.len();
        work.step()?;
        if self.array.is_null(row) {
            return Ok(());
        }
        let expected_types = IntermediateArgTypes::try_new(intermediate_type)?;
        let actual_types = MergeArgTypes(&self.list_columns);
        let mut matches = actual_types.0.len() == expected_types.len();
        if matches {
            for (actual, expected) in actual_types.iter().zip(expected_types.iter()) {
                work.step()?;
                if !novarocks_type_contract::arrow_data_types_equal_borrowed_observed::<
                    crate::KernelFailure,
                >(actual, expected, || work.step())?
                {
                    matches = false;
                    break;
                }
            }
        }
        if !matches {
            return Err(GroupConcatShapeError::carrier(format_args!(
                "group_concat merge argument types mismatch: expected {expected_types:?}, got {:?}",
                actual_types
            ))
            .into());
        }

        let mut output_list_is_null = false;
        for list_col in self.list_columns.iter().take(self.layout.output_col_num) {
            work.step()?;
            if list_col.is_null(row) {
                output_list_is_null = true;
                break;
            }
        }
        if output_list_is_null {
            return Ok(());
        }

        if self.list_columns[0].is_null(row) {
            return Ok(());
        }
        let base_start = self.offsets[0][row] as usize;
        let base_end = self.offsets[0][row + 1] as usize;
        let row_count = base_end.saturating_sub(base_start);
        for col_idx in 0..field_num {
            work.step()?;
            if self.list_columns[col_idx].is_null(row) {
                return Err(GroupConcatShapeError::carrier(format_args!(
                    "group_concat merge field {} is null while struct row is non-null",
                    col_idx
                ))
                .into());
            }
            let start = self.offsets[col_idx][row] as usize;
            let end = self.offsets[col_idx][row + 1] as usize;
            if end.saturating_sub(start) != row_count {
                return Err(GroupConcatShapeError::carrier(format_args!(
                    "group_concat merge field {} length mismatch: expected {}, got {}",
                    col_idx,
                    row_count,
                    end.saturating_sub(start)
                ))
                .into());
            }
        }

        for idx in 0..row_count {
            work.step()?;
            let mut row_values = aggregate_vec_with_capacity(
                &state.allocator,
                field_num,
                "reserve group_concat merge row",
                work,
            )?;
            let mut has_null_output = false;
            for col_idx in 0..field_num {
                work.step()?;
                let start = self.offsets[col_idx][row] as usize;
                let value = tracked_scalar_from_array(
                    &self.value_columns[col_idx],
                    start + idx,
                    &state.allocator,
                    work,
                )?;
                if col_idx < self.layout.output_col_num && value.is_none() {
                    has_null_output = true;
                    break;
                }
                row_values.push(value);
            }
            if !has_null_output {
                state.push_row(row_values, work)?;
            }
        }

        Ok(())
    }
}
pub fn build_intermediate_array<
    's,
    A: ScalarStateAllocator,
    I: ExactSizeIterator<Item = &'s GroupConcatState<A>>,
>(
    intermediate_type: &DataType,
    group_states: I,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, crate::KernelFailure> {
    let fields = validate_intermediate_type(intermediate_type)?.clone();
    let field_num = fields.len();

    let mut offsets = Vec::with_capacity(group_states.len() + 1);
    offsets.push(0_i32);
    let mut current_len: i32 = 0;
    let mut struct_null_builder = NullBufferBuilder::new(group_states.len());
    let mut list_null_builder = NullBufferBuilder::new(group_states.len());
    let mut flat_values: Vec<Vec<Option<AggScalarValue>>> =
        (0..field_num).map(|_| Vec::new()).collect();

    for state in group_states {
        work.step()?;
        if state.rows.is_empty() {
            offsets.push(current_len);
            struct_null_builder.append_null();
            list_null_builder.append_null();
            continue;
        }

        let next_offset =
            crate::aggregate_scalar::checked_list_output_offset(current_len, state.rows.len())?;
        for row in &state.rows {
            work.step()?;
            if row.len() != field_num {
                return Err(GroupConcatShapeError::carrier(
                    "group_concat state row width mismatch",
                )
                .into());
            }
            for idx in 0..field_num {
                work.step()?;
                flat_values[idx].push(
                    row[idx]
                        .as_ref()
                        .map(|value| tracked_scalar_to_output(value, work))
                        .transpose()?,
                );
            }
        }
        current_len = next_offset;
        offsets.push(current_len);
        struct_null_builder.append_non_null();
        list_null_builder.append_non_null();
    }

    let list_nulls = list_null_builder.finish();
    let mut columns = Vec::with_capacity(field_num);
    for (idx, field) in fields.iter().enumerate() {
        work.step()?;
        let DataType::List(item_field) = field.data_type() else {
            return Err(GroupConcatShapeError::IntermediateField { index: idx }.into());
        };
        let values = build_scalar_array(item_field.data_type(), flat_values[idx].clone(), work)?;
        if values.data_type() != item_field.data_type() {
            return Err(GroupConcatShapeError::carrier(
                "group_concat scalar builder changed the frozen intermediate value type",
            )
            .into());
        }
        let list = ListArray::try_new(
            item_field.clone(),
            OffsetBuffer::new(offsets.clone().into()),
            values,
            list_nulls.clone(),
        )
        .map_err(source_failure::constructor)?;
        columns.push(Arc::new(list) as ArrayRef);
    }

    let out = StructArray::try_new(fields, columns, struct_null_builder.finish())
        .map_err(source_failure::constructor)?;
    work.flush()?;
    Ok(Arc::new(out))
}

pub fn build_final_array<
    's,
    A: ScalarStateAllocator,
    I: ExactSizeIterator<Item = &'s GroupConcatState<A>>,
>(
    intermediate_type: &DataType,
    group_states: I,
    is_distinct: bool,
    is_asc_order: &[bool],
    nulls_first: &[bool],
    max_len: i64,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, crate::KernelFailure> {
    let mut builder = StringBuilder::new();
    for state in group_states {
        work.step()?;
        if state.rows.is_empty() {
            builder.append_null();
            continue;
        }

        let arg_types = IntermediateArgTypes::try_new(intermediate_type)?;
        let layout = GroupConcatLayout::infer(arg_types.len(), is_asc_order.len())?;

        let sorted_indices = sort_indices(state, layout, is_asc_order, nulls_first, false, work)?;
        let duplicated =
            mark_duplicated_rows(&state.rows, &sorted_indices, layout, is_distinct, work)?;
        let mut last_unique_pos = None;
        for (pos, idx) in sorted_indices.iter().enumerate().rev() {
            work.step()?;
            if !duplicated[*idx] {
                last_unique_pos = Some(pos);
                break;
            }
        }
        let Some(last_unique_pos) = last_unique_pos else {
            builder.append_null();
            continue;
        };

        let mut out = String::new();
        let max_len = usize::try_from(max_len).unwrap_or(usize::MAX);
        let mut reached_limit = false;
        for (pos, idx) in sorted_indices.iter().enumerate().take(last_unique_pos + 1) {
            work.step()?;
            if duplicated[*idx] {
                continue;
            }
            let row = &state.rows[*idx];
            for col in 0..layout.output_col_num {
                work.step()?;
                let value = row[col].as_ref().ok_or_else(|| {
                    crate::KernelFailure::from(GroupConcatShapeError::carrier(
                        "group_concat output column should not contain null",
                    ))
                })?;
                if append_with_limit(
                    &mut out,
                    &scalar_to_string(
                        &tracked_scalar_to_output(value, work)?,
                        arg_types.get(col),
                        work,
                    )?,
                    max_len,
                ) {
                    reached_limit = true;
                    break;
                }
            }
            if reached_limit {
                break;
            }
            if pos != last_unique_pos
                && append_with_limit(
                    &mut out,
                    &separator_for_row(row, arg_types, layout, work)?,
                    max_len,
                )
            {
                break;
            }
        }
        builder.append_value(out);
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod shape_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn maximum_ordering_count_is_a_shape_refusal_without_arithmetic_panic() {
        let source = GroupConcatLayout::infer(1, usize::MAX).unwrap_err();
        assert_eq!(
            source,
            GroupConcatShapeError::OrderColumns {
                arguments: 1,
                ordering: usize::MAX
            }
        );
        assert_eq!(source.failure_fact().class(), FailureClass::Invariant);
        let fact = source.failure_fact();
        assert_eq!(crate::KernelFailure::from(source).failure_fact(), fact);
        let legal = GroupConcatLayout::infer(usize::MAX, usize::MAX - 1).unwrap();
        assert_eq!(legal.output_col_num, 1);
        assert_eq!(legal.separator_idx, None);
    }
    #[test]
    fn invalid_nested_schema_is_not_retained_by_the_shape_cause() {
        let field = Arc::new(Field::new(
            "numeric overflow ResourceExhausted",
            DataType::Int32,
            false,
        ));
        let weak = Arc::downgrade(&field);
        let ty = DataType::List(field.clone());
        let source = validate_intermediate_type(&ty).unwrap_err();
        drop(ty);
        drop(field);
        assert!(weak.upgrade().is_none());
        assert_eq!(source.failure_fact().class(), FailureClass::Invariant);
        assert!(source.to_string().contains("numeric overflow"));
    }
    #[test]
    fn aggregate_concat_merge_contract_keeps_its_source_and_releases_actual_input() {
        let field = Arc::new(Field::new("field".repeat(512), DataType::Int64, true));
        let array: ArrayRef = Arc::new(StructArray::new(
            vec![field.clone()].into(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![Some(7)]))],
            None,
        ));
        let weak_array = Arc::downgrade(&array);
        let weak_field = Arc::downgrade(&field);
        let error = match GroupConcatMerge::new(&array, 0, &mut ScalarWork::new(None)) {
            Err(error) => error,
            Ok(_) => panic!("scalar column cannot decode as an intermediate List state"),
        };
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Invariant);
        assert_eq!(fact.reason().code(), 155);
        let mut source = std::error::Error::source(&error);
        let mut nominal = false;
        while let Some(cause) = source {
            nominal |= cause.is::<GroupConcatShapeError>();
            source = cause.source();
        }
        assert!(nominal);
        assert!(error.to_string().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
        drop(array);
        drop(field);
        assert!(weak_array.upgrade().is_none());
        assert!(weak_field.upgrade().is_none());
        assert_eq!(error.failure_fact(), fact);
    }
}

pub(super) fn build_listagg_array<
    's,
    A: ScalarStateAllocator,
    I: ExactSizeIterator<Item = &'s GroupConcatState<A>>,
>(
    states: I,
    distinct: bool,
    ascending: &[bool],
    nulls_first: &[bool],
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, crate::KernelFailure> {
    let mut builder = StringBuilder::new();
    let layout = GroupConcatLayout {
        output_col_num: 5,
        separator_idx: None,
        order_by_start: 5,
        order_by_num: ascending.len(),
    };
    for state in states {
        work.step()?;
        let indices = sort_indices(state, layout, ascending, nulls_first, true, work)?;
        let ordered = indices.iter().map(|index| state.rows[*index].as_slice());
        if let Some(output) = listagg_emission::emit(ordered, distinct, &state.allocator, work)? {
            let value = std::str::from_utf8(&output).map_err(|_| {
                crate::kernel_control::internal("LISTAGG emission did not preserve UTF-8")
            })?;
            work.flush()?;
            builder.append_value(value);
            work.flush()?;
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}
