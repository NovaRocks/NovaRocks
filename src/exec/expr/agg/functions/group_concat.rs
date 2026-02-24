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
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, StringBuilder, StructArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common::{
    AggScalarValue, build_scalar_array, compare_scalar_values, scalar_from_array, scalar_to_string,
};

pub(super) struct GroupConcatAgg;

const DEFAULT_SEPARATOR: &str = ",";

#[derive(Clone, Debug, Default)]
struct GroupConcatState {
    arg_types: Option<Vec<DataType>>,
    rows: Vec<Vec<Option<AggScalarValue>>>,
}

impl GroupConcatState {
    fn ensure_arg_types(&mut self, arg_types: &[DataType]) -> Result<(), String> {
        if let Some(existing) = &self.arg_types {
            if existing.len() != arg_types.len() {
                return Err(format!(
                    "group_concat argument count mismatch: expected {}, got {}",
                    existing.len(),
                    arg_types.len()
                ));
            }
            for (idx, (left, right)) in existing.iter().zip(arg_types.iter()).enumerate() {
                if left != right {
                    return Err(format!(
                        "group_concat argument type mismatch at {}: expected {:?}, got {:?}",
                        idx, left, right
                    ));
                }
            }
        } else {
            self.arg_types = Some(arg_types.to_vec());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct GroupConcatLayout {
    output_col_num: usize,
    separator_idx: Option<usize>,
    order_by_start: usize,
    order_by_num: usize,
}

impl GroupConcatLayout {
    fn infer(arg_count: usize, order_by_num: usize) -> Result<Self, String> {
        if arg_count == 0 {
            return Err("group_concat argument count must be positive".to_string());
        }
        if arg_count < order_by_num + 1 {
            return Err(format!(
                "group_concat argument count {} is less than order-by columns {}",
                arg_count, order_by_num
            ));
        }

        // FE usually rewrites group_concat to include separator as one argument.
        // Keep one-arg mode for compatibility with tests and fallback plans.
        let (output_col_num, separator_idx, order_by_start) = if arg_count == order_by_num + 1 {
            (arg_count - order_by_num, None, arg_count - order_by_num)
        } else {
            let output_col_num = arg_count - order_by_num - 1;
            (output_col_num, Some(output_col_num), output_col_num + 1)
        };
        if output_col_num == 0 {
            return Err("group_concat output column should not be empty".to_string());
        }
        if order_by_start + order_by_num != arg_count {
            return Err("group_concat argument layout is invalid".to_string());
        }
        Ok(Self {
            output_col_num,
            separator_idx,
            order_by_start,
            order_by_num,
        })
    }
}

fn parse_group_concat_metadata(name: &str) -> Result<(bool, Vec<bool>, Vec<bool>, i64), String> {
    let base = name.split('|').next().unwrap_or(name);
    if base != "group_concat" {
        return Err(format!("unsupported group_concat function: {}", name));
    }
    if !name.contains('|') {
        return Ok((false, Vec::new(), Vec::new(), 1024));
    }

    let mut is_distinct = false;
    let mut is_asc_order = Vec::new();
    let mut nulls_first = Vec::new();
    let mut max_len = 1024i64;
    for token in name.split('|').skip(1) {
        let (k, v) = token
            .split_once('=')
            .ok_or_else(|| format!("group_concat metadata token missing '=': {}", token))?;
        match k {
            "d" => is_distinct = parse_bool_flag(v)?,
            "a" => is_asc_order = parse_bool_list(v)?,
            "n" => nulls_first = parse_bool_list(v)?,
            "m" => {
                max_len = v
                    .parse::<i64>()
                    .map_err(|_| format!("group_concat metadata max_len is invalid: {}", v))?;
            }
            other => {
                return Err(format!(
                    "group_concat metadata key '{}' is unsupported",
                    other
                ));
            }
        }
    }

    if is_asc_order.len() != nulls_first.len() {
        return Err(format!(
            "group_concat metadata length mismatch: is_asc_order={} nulls_first={}",
            is_asc_order.len(),
            nulls_first.len()
        ));
    }
    Ok((is_distinct, is_asc_order, nulls_first, max_len.max(4)))
}

fn parse_bool_flag(v: &str) -> Result<bool, String> {
    match v {
        "1" | "true" | "TRUE" => Ok(true),
        "0" | "false" | "FALSE" => Ok(false),
        _ => Err(format!("group_concat metadata bool is invalid: {}", v)),
    }
}

fn parse_bool_list(v: &str) -> Result<Vec<bool>, String> {
    if v.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for token in v.split(',') {
        out.push(parse_bool_flag(token)?);
    }
    Ok(out)
}

fn group_concat_kind(spec: &AggSpec) -> Result<(bool, &[bool], &[bool], i64), String> {
    match &spec.kind {
        AggKind::GroupConcat {
            is_distinct,
            is_asc_order,
            nulls_first,
            max_len,
        } => Ok((
            *is_distinct,
            is_asc_order.as_slice(),
            nulls_first.as_slice(),
            *max_len,
        )),
        _ => Err("group_concat spec kind mismatch".to_string()),
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

fn extract_arg_types(input_type: &DataType) -> Vec<DataType> {
    match input_type {
        DataType::Struct(fields) => fields.iter().map(|f| f.data_type().clone()).collect(),
        other => vec![other.clone()],
    }
}

fn build_default_intermediate_type(arg_types: &[DataType]) -> DataType {
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

fn validate_intermediate_type(ty: &DataType) -> Result<&Fields, String> {
    let DataType::Struct(fields) = ty else {
        return Err(format!(
            "group_concat intermediate type must be STRUCT, got {:?}",
            ty
        ));
    };
    if fields.is_empty() {
        return Err("group_concat intermediate type must contain at least one field".to_string());
    }
    for (idx, field) in fields.iter().enumerate() {
        if !matches!(field.data_type(), DataType::List(_)) {
            return Err(format!(
                "group_concat intermediate field {} must be ARRAY type",
                idx
            ));
        }
    }
    Ok(fields)
}

fn extract_input_columns(array: &ArrayRef) -> Result<Vec<ArrayRef>, String> {
    match array.data_type() {
        DataType::Struct(_) => {
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "group_concat input must be StructArray".to_string())?;
            Ok(struct_arr.columns().to_vec())
        }
        _ => Ok(vec![array.clone()]),
    }
}

fn read_row_values(
    columns: &[ArrayRef],
    row: usize,
    output_col_num: usize,
) -> Result<Option<Vec<Option<AggScalarValue>>>, String> {
    let mut values = Vec::with_capacity(columns.len());
    for (idx, col) in columns.iter().enumerate() {
        let value = scalar_from_array(col, row)?;
        if idx < output_col_num && value.is_none() {
            return Ok(None);
        }
        values.push(value);
    }
    Ok(Some(values))
}

fn compare_optional_scalar(
    left: &Option<AggScalarValue>,
    right: &Option<AggScalarValue>,
    asc: bool,
    nulls_first: bool,
) -> Result<Ordering, String> {
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
            let ord = compare_scalar_values(left, right)?;
            if asc { ord } else { ord.reverse() }
        }
    };
    Ok(ord)
}

fn sort_indices(
    rows: &[Vec<Option<AggScalarValue>>],
    layout: GroupConcatLayout,
    is_asc_order: &[bool],
    nulls_first: &[bool],
) -> Result<Vec<usize>, String> {
    let mut indices: Vec<usize> = (0..rows.len()).collect();
    if layout.order_by_num == 0 {
        return Ok(indices);
    }

    let mut error: Option<String> = None;
    indices.sort_by(|l, r| {
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
            ) {
                Ok(Ordering::Equal) => continue,
                Ok(ord) => return ord,
                Err(e) => {
                    error = Some(e);
                    return Ordering::Equal;
                }
            }
        }
        Ordering::Equal
    });
    if let Some(err) = error {
        return Err(err);
    }
    Ok(indices)
}

fn rows_equal_on_output(
    left: &[Option<AggScalarValue>],
    right: &[Option<AggScalarValue>],
    layout: GroupConcatLayout,
) -> Result<bool, String> {
    for col in 0..layout.output_col_num {
        match (&left[col], &right[col]) {
            (None, None) => {}
            (Some(left), Some(right)) => {
                if compare_scalar_values(left, right)? != Ordering::Equal {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn mark_duplicated_rows(
    rows: &[Vec<Option<AggScalarValue>>],
    sorted_indices: &[usize],
    layout: GroupConcatLayout,
    is_distinct: bool,
) -> Result<Vec<bool>, String> {
    let mut duplicated = vec![false; rows.len()];
    if !is_distinct {
        return Ok(duplicated);
    }
    for (pos, &idx) in sorted_indices.iter().enumerate() {
        for &next_idx in sorted_indices.iter().skip(pos + 1) {
            if rows_equal_on_output(&rows[idx], &rows[next_idx], layout)? {
                duplicated[idx] = true;
                break;
            }
        }
    }
    Ok(duplicated)
}

fn separator_for_row(
    row: &[Option<AggScalarValue>],
    arg_types: &[DataType],
    layout: GroupConcatLayout,
) -> Result<String, String> {
    if let Some(sep_idx) = layout.separator_idx {
        if let Some(separator) = row
            .get(sep_idx)
            .ok_or_else(|| "group_concat separator index out of bounds".to_string())?
            .as_ref()
        {
            scalar_to_string(separator, &arg_types[sep_idx])
        } else {
            Ok(String::new())
        }
    } else {
        Ok(DEFAULT_SEPARATOR.to_string())
    }
}

fn build_intermediate_array(
    spec: &AggSpec,
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let fields = validate_intermediate_type(&spec.intermediate_type)?.clone();
    let field_num = fields.len();

    let mut offsets = Vec::with_capacity(group_states.len() + 1);
    offsets.push(0_i32);
    let mut current_len: i64 = 0;
    let mut struct_null_builder = NullBufferBuilder::new(group_states.len());
    let mut list_null_builder = NullBufferBuilder::new(group_states.len());
    let mut flat_values: Vec<Vec<Option<AggScalarValue>>> =
        (0..field_num).map(|_| Vec::new()).collect();

    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const GroupConcatState) };
        if state.rows.is_empty() {
            offsets.push(current_len as i32);
            struct_null_builder.append_null();
            list_null_builder.append_null();
            continue;
        }

        let state_types = state
            .arg_types
            .as_ref()
            .ok_or_else(|| "group_concat state missing argument types".to_string())?;
        if state_types.len() != field_num {
            return Err(format!(
                "group_concat intermediate field count mismatch: expected {}, got {}",
                field_num,
                state_types.len()
            ));
        }
        for row in &state.rows {
            if row.len() != field_num {
                return Err("group_concat state row width mismatch".to_string());
            }
            for idx in 0..field_num {
                flat_values[idx].push(row[idx].clone());
            }
        }
        current_len += i64::try_from(state.rows.len())
            .map_err(|_| "group_concat intermediate row count overflow".to_string())?;
        if current_len > i32::MAX as i64 {
            return Err("group_concat intermediate offset overflow".to_string());
        }
        offsets.push(current_len as i32);
        struct_null_builder.append_non_null();
        list_null_builder.append_non_null();
    }

    let list_nulls = list_null_builder.finish();
    let mut columns = Vec::with_capacity(field_num);
    for (idx, field) in fields.iter().enumerate() {
        let DataType::List(item_field) = field.data_type() else {
            return Err(format!(
                "group_concat intermediate field {} must be ARRAY type",
                idx
            ));
        };
        let mut values = build_scalar_array(item_field.data_type(), flat_values[idx].clone())?;
        if values.data_type() != item_field.data_type() {
            values = cast(&values, item_field.data_type()).map_err(|e| {
                format!(
                    "group_concat failed to cast intermediate field {} values: {}",
                    idx, e
                )
            })?;
        }
        let list = ListArray::new(
            item_field.clone(),
            OffsetBuffer::new(offsets.clone().into()),
            values,
            list_nulls.clone(),
        );
        columns.push(Arc::new(list) as ArrayRef);
    }

    let out = StructArray::new(fields, columns, struct_null_builder.finish());
    Ok(Arc::new(out))
}

impl AggregateFunction for GroupConcatAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| "group_concat input type missing".to_string())?;
        let (is_distinct, is_asc_order, nulls_first, max_len) =
            parse_group_concat_metadata(&func.name)?;
        let order_by_num = is_asc_order.len();
        let kind = AggKind::GroupConcat {
            is_distinct,
            is_asc_order,
            nulls_first,
            max_len,
        };

        if input_is_intermediate {
            let fields = validate_intermediate_type(input_type)?;
            let _ = GroupConcatLayout::infer(fields.len(), order_by_num)?;
            return Ok(AggSpec {
                kind,
                output_type: DataType::Utf8,
                intermediate_type: input_type.clone(),
                input_arg_type: func.types.as_ref().and_then(|t| t.input_arg_type.clone()),
                count_all: false,
            });
        }

        let arg_types = extract_arg_types(input_type);
        let _ = GroupConcatLayout::infer(arg_types.len(), order_by_num)?;

        let intermediate_type = func
            .types
            .as_ref()
            .and_then(|t| t.intermediate_type.clone())
            .unwrap_or_else(|| build_default_intermediate_type(&arg_types));
        let fields = validate_intermediate_type(&intermediate_type)?;
        if fields.len() != arg_types.len() {
            return Err(format!(
                "group_concat intermediate field count mismatch: expected {}, got {}",
                arg_types.len(),
                fields.len()
            ));
        }

        Ok(AggSpec {
            kind,
            output_type: DataType::Utf8,
            intermediate_type,
            input_arg_type: arg_types.first().cloned(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::GroupConcat { .. } => (
                std::mem::size_of::<GroupConcatState>(),
                std::mem::align_of::<GroupConcatState>(),
            ),
            other => unreachable!("unexpected kind for group_concat: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "group_concat input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "group_concat merge input missing".to_string())?;
        let _ = arr
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "group_concat merge input must be StructArray".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut GroupConcatState, GroupConcatState::default());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut GroupConcatState);
        }
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("group_concat batch input type mismatch".to_string());
        };
        let (_, is_asc_order, _, _) = group_concat_kind(spec)?;
        let columns = extract_input_columns(array)?;
        let layout = GroupConcatLayout::infer(columns.len(), is_asc_order.len())?;
        let arg_types = columns
            .iter()
            .map(|c| c.data_type().clone())
            .collect::<Vec<_>>();

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut GroupConcatState) };
            state.ensure_arg_types(&arg_types)?;
            let Some(row_values) = read_row_values(&columns, row, layout.output_col_num)? else {
                continue;
            };
            state.rows.push(row_values);
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("group_concat merge input type mismatch".to_string());
        };
        let (_, is_asc_order, _, _) = group_concat_kind(spec)?;
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "group_concat merge input must be StructArray".to_string())?;
        let field_num = struct_arr.num_columns();
        let layout = GroupConcatLayout::infer(field_num, is_asc_order.len())?;
        let mut list_columns = Vec::with_capacity(field_num);
        let mut value_columns = Vec::with_capacity(field_num);
        let mut offsets = Vec::with_capacity(field_num);
        let mut arg_types = Vec::with_capacity(field_num);
        for (idx, col) in struct_arr.columns().iter().enumerate() {
            let list_col = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| format!("group_concat merge field {} must be ListArray", idx))?;
            let DataType::List(item_field) = list_col.data_type() else {
                return Err(format!(
                    "group_concat merge field {} must be ARRAY type",
                    idx
                ));
            };
            arg_types.push(item_field.data_type().clone());
            value_columns.push(list_col.values().clone());
            offsets.push(list_col.value_offsets().to_vec());
            list_columns.push(list_col);
        }

        for (row, &base) in state_ptrs.iter().enumerate() {
            if struct_arr.is_null(row) {
                continue;
            }
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut GroupConcatState) };
            state.ensure_arg_types(&arg_types)?;

            let mut output_list_is_null = false;
            for list_col in list_columns.iter().take(layout.output_col_num) {
                if list_col.is_null(row) {
                    output_list_is_null = true;
                    break;
                }
            }
            if output_list_is_null {
                continue;
            }

            if list_columns[0].is_null(row) {
                continue;
            }
            let base_start = offsets[0][row] as usize;
            let base_end = offsets[0][row + 1] as usize;
            let row_count = base_end.saturating_sub(base_start);
            for col_idx in 0..field_num {
                if list_columns[col_idx].is_null(row) {
                    return Err(format!(
                        "group_concat merge field {} is null while struct row is non-null",
                        col_idx
                    ));
                }
                let start = offsets[col_idx][row] as usize;
                let end = offsets[col_idx][row + 1] as usize;
                if end.saturating_sub(start) != row_count {
                    return Err(format!(
                        "group_concat merge field {} length mismatch: expected {}, got {}",
                        col_idx,
                        row_count,
                        end.saturating_sub(start)
                    ));
                }
            }

            for idx in 0..row_count {
                let mut row_values = Vec::with_capacity(field_num);
                let mut has_null_output = false;
                for col_idx in 0..field_num {
                    let start = offsets[col_idx][row] as usize;
                    let value = scalar_from_array(&value_columns[col_idx], start + idx)?;
                    if col_idx < layout.output_col_num && value.is_none() {
                        has_null_output = true;
                        break;
                    }
                    row_values.push(value);
                }
                if !has_null_output {
                    state.rows.push(row_values);
                }
            }
        }
        Ok(())
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            return build_intermediate_array(spec, offset, group_states);
        }

        let (is_distinct, is_asc_order, nulls_first, max_len) = group_concat_kind(spec)?;
        let mut builder = StringBuilder::new();
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const GroupConcatState) };
            if state.rows.is_empty() {
                builder.append_null();
                continue;
            }

            let arg_types = state
                .arg_types
                .as_ref()
                .ok_or_else(|| "group_concat state missing argument types".to_string())?;
            let layout = GroupConcatLayout::infer(arg_types.len(), is_asc_order.len())?;

            let sorted_indices = sort_indices(&state.rows, layout, is_asc_order, nulls_first)?;
            let duplicated =
                mark_duplicated_rows(&state.rows, &sorted_indices, layout, is_distinct)?;
            let last_unique_pos = sorted_indices
                .iter()
                .enumerate()
                .rfind(|(_, idx)| !duplicated[**idx])
                .map(|(pos, _)| pos);
            let Some(last_unique_pos) = last_unique_pos else {
                builder.append_null();
                continue;
            };

            let mut out = String::new();
            let max_len = usize::try_from(max_len).unwrap_or(usize::MAX);
            let mut reached_limit = false;
            for (pos, idx) in sorted_indices.iter().enumerate().take(last_unique_pos + 1) {
                if duplicated[*idx] {
                    continue;
                }
                let row = &state.rows[*idx];
                for col in 0..layout.output_col_num {
                    let value = row[col].as_ref().ok_or_else(|| {
                        "group_concat output column should not contain null".to_string()
                    })?;
                    if append_with_limit(
                        &mut out,
                        &scalar_to_string(value, &arg_types[col])?,
                        max_len,
                    ) {
                        reached_limit = true;
                        break;
                    }
                }
                if reached_limit {
                    break;
                }
                if pos != last_unique_pos {
                    if append_with_limit(
                        &mut out,
                        &separator_for_row(row, arg_types, layout)?,
                        max_len,
                    ) {
                        break;
                    }
                }
            }
            builder.append_value(out);
        }
        Ok(Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use std::mem::MaybeUninit;

    fn intermediate_type(arg_types: &[DataType]) -> DataType {
        build_default_intermediate_type(arg_types)
    }

    fn make_func(
        name: &str,
        input_is_intermediate: bool,
        intermediate_type: DataType,
        input_arg_type: DataType,
    ) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(intermediate_type),
                output_type: Some(DataType::Utf8),
                input_arg_type: Some(input_arg_type),
            }),
        }
    }

    fn run_update_then_finalize(spec: &AggSpec, input: ArrayRef) -> String {
        let view = AggInputView::Any(&input);
        let mut state = MaybeUninit::<GroupConcatState>::uninit();
        GroupConcatAgg.init_state(spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let ptrs = vec![state_ptr; input.len()];
        GroupConcatAgg
            .update_batch(spec, 0, &ptrs, &view)
            .expect("update should succeed");
        let out = GroupConcatAgg
            .build_array(spec, 0, &[state_ptr], false)
            .expect("finalize should succeed");
        GroupConcatAgg.drop_state(spec, state.as_mut_ptr() as *mut u8);
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        out.value(0).to_string()
    }

    #[test]
    fn test_group_concat_spec_with_metadata() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("v", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
            Arc::new(Field::new("k", DataType::Int64, true)),
        ]));
        let arg_types = vec![DataType::Utf8, DataType::Utf8, DataType::Int64];
        let func = make_func(
            "group_concat|d=1|a=1|n=0",
            false,
            intermediate_type(&arg_types),
            input_type.clone(),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        match &spec.kind {
            AggKind::GroupConcat {
                is_distinct,
                is_asc_order,
                nulls_first,
                ..
            } => {
                assert!(*is_distinct);
                assert_eq!(is_asc_order, &vec![true]);
                assert_eq!(nulls_first, &vec![false]);
            }
            other => panic!("unexpected kind: {:?}", other),
        }
    }

    #[test]
    fn test_group_concat_default_separator_single_column() {
        let input_type = DataType::Int64;
        let func = make_func(
            "group_concat",
            false,
            intermediate_type(&[DataType::Int64]),
            input_type.clone(),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        let input = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let out = run_update_then_finalize(&spec, input);
        assert_eq!(out, "1,3");
    }

    #[test]
    fn test_group_concat_multi_output_columns() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
        ]));
        let func = make_func(
            "group_concat|d=0|a=|n=",
            false,
            intermediate_type(&[DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            input_type.clone(),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();

        let input = Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("a", DataType::Utf8, true)),
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(Field::new("sep", DataType::Utf8, true)),
            ]),
            vec![
                Arc::new(StringArray::from(vec![Some("x"), Some("y")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("1"), Some("2")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some(","), Some(",")])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let out = run_update_then_finalize(&spec, input);
        assert_eq!(out, "x1,y2");
    }

    #[test]
    fn test_group_concat_order_by() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("v", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
            Arc::new(Field::new("k", DataType::Int64, true)),
        ]));
        let func = make_func(
            "group_concat|d=0|a=1|n=0",
            false,
            intermediate_type(&[DataType::Utf8, DataType::Utf8, DataType::Int64]),
            input_type.clone(),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        let input = Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(Field::new("sep", DataType::Utf8, true)),
                Arc::new(Field::new("k", DataType::Int64, true)),
            ]),
            vec![
                Arc::new(StringArray::from(vec![Some("b"), Some("a"), Some("c")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("|"), Some("|"), Some("|")])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(3)])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let out = run_update_then_finalize(&spec, input);
        assert_eq!(out, "a|b|c");
    }

    #[test]
    fn test_group_concat_distinct_keep_last_after_sort() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("v", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
            Arc::new(Field::new("k", DataType::Int64, true)),
        ]));
        let func = make_func(
            "group_concat|d=1|a=1|n=0",
            false,
            intermediate_type(&[DataType::Utf8, DataType::Utf8, DataType::Int64]),
            input_type.clone(),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        let input = Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(Field::new("sep", DataType::Utf8, true)),
                Arc::new(Field::new("k", DataType::Int64, true)),
            ]),
            vec![
                Arc::new(StringArray::from(vec![Some("x"), Some("x"), Some("y")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("|"), Some("/"), Some("-")])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let out = run_update_then_finalize(&spec, input);
        assert_eq!(out, "x/y");
    }

    #[test]
    fn test_group_concat_merge_roundtrip() {
        let input_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("v", DataType::Utf8, true)),
            Arc::new(Field::new("sep", DataType::Utf8, true)),
            Arc::new(Field::new("k", DataType::Int64, true)),
        ]));
        let intermediate_ty = intermediate_type(&[DataType::Utf8, DataType::Utf8, DataType::Int64]);
        let func = make_func(
            "group_concat|d=1|a=1|n=0",
            false,
            intermediate_ty.clone(),
            input_type.clone(),
        );
        let spec = GroupConcatAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        let input = Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(Field::new("sep", DataType::Utf8, true)),
                Arc::new(Field::new("k", DataType::Int64, true)),
            ]),
            vec![
                Arc::new(StringArray::from(vec![Some("b"), Some("a"), Some("a")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some(","), Some(","), Some("|")])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(3)])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let view = AggInputView::Any(&input);

        let mut left_state = MaybeUninit::<GroupConcatState>::uninit();
        GroupConcatAgg.init_state(&spec, left_state.as_mut_ptr() as *mut u8);
        let left_ptr = left_state.as_mut_ptr() as AggStatePtr;
        GroupConcatAgg
            .update_batch(&spec, 0, &vec![left_ptr; input.len()], &view)
            .unwrap();
        let intermediate = GroupConcatAgg
            .build_array(&spec, 0, &[left_ptr], true)
            .unwrap();

        let merge_func = make_func(
            "group_concat|d=1|a=1|n=0",
            true,
            intermediate_ty.clone(),
            input_type,
        );
        let merge_spec = GroupConcatAgg
            .build_spec_from_type(&merge_func, Some(&intermediate_ty), true)
            .unwrap();
        let merge_view = AggInputView::Any(&intermediate);
        let mut right_state = MaybeUninit::<GroupConcatState>::uninit();
        GroupConcatAgg.init_state(&merge_spec, right_state.as_mut_ptr() as *mut u8);
        let right_ptr = right_state.as_mut_ptr() as AggStatePtr;
        GroupConcatAgg
            .merge_batch(&merge_spec, 0, &[right_ptr], &merge_view)
            .unwrap();

        let out = GroupConcatAgg
            .build_array(&merge_spec, 0, &[right_ptr], false)
            .unwrap();
        GroupConcatAgg.drop_state(&spec, left_state.as_mut_ptr() as *mut u8);
        GroupConcatAgg.drop_state(&merge_spec, right_state.as_mut_ptr() as *mut u8);
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "b,a");
    }
}
