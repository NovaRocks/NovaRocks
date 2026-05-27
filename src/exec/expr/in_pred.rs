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
use crate::exec::chunk::Chunk;
use crate::exec::expr::function::compare_values_with_null;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::exec::variant::VariantValue;
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, ListArray, MapArray, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::{DataType, TimeUnit};
use serde_json::Value as JsonValue;
use std::sync::Arc;

// IN predicate for Arrow arrays
pub fn eval_in(
    arena: &ExprArena,
    child: ExprId,
    values: &[ExprId],
    is_not_in: bool,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let array = arena.eval(child, chunk)?;
    let len = array.len();
    let lhs_is_literal_like = !expr_contains_slot(arena, child);
    let single_candidate = values.len() == 1;

    if len == 0 {
        return Ok(Arc::new(BooleanArray::from(Vec::<bool>::new())));
    }

    let mut has_null = vec![false; len];
    let mut matched = vec![false; len];

    for value_id in values {
        let candidate = arena.eval(*value_id, chunk)?;
        if candidate.len() != 1 && candidate.len() != len {
            return Err(format!(
                "IN predicate value length mismatch: input has {}, value has {}",
                len,
                candidate.len()
            ));
        }
        for (row, has_null_row) in has_null.iter_mut().enumerate() {
            if candidate.is_null(row_index(row, candidate.len())) {
                *has_null_row = true;
            }
        }
        let eq_array =
            eq_with_candidate(&array, &candidate, lhs_is_literal_like, single_candidate)?;
        for (row, matched_row) in matched.iter_mut().enumerate() {
            if eq_array.is_null(row) {
                has_null[row] = true;
            } else if eq_array.value(row) {
                *matched_row = true;
            }
        }
    }

    // SQL three-valued logic for IN/NOT IN:
    // 1) lhs NULL => NULL
    // 2) any match => TRUE for IN / FALSE for NOT IN
    // 3) no match and list contains NULL => NULL
    // 4) otherwise => FALSE for IN / TRUE for NOT IN
    let mut builder = BooleanBuilder::with_capacity(len);
    for (row, matched_row) in matched.iter().enumerate() {
        if array.is_null(row) || matches!(array.data_type(), DataType::Null) {
            builder.append_null();
            continue;
        }
        if *matched_row {
            builder.append_value(!is_not_in);
            continue;
        }
        if has_null[row] {
            builder.append_null();
            continue;
        }
        builder.append_value(is_not_in);
    }
    Ok(Arc::new(builder.finish()))
}

fn row_index(row: usize, len: usize) -> usize {
    if len == 1 { 0 } else { row }
}

fn eq_with_candidate(
    array: &ArrayRef,
    candidate: &ArrayRef,
    lhs_is_literal_like: bool,
    single_candidate: bool,
) -> Result<BooleanArray, String> {
    if matches!(
        candidate.data_type(),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    ) {
        if array.data_type() != candidate.data_type() {
            return Err(format!(
                "IN nested type mismatch: {:?} vs {:?}",
                array.data_type(),
                candidate.data_type()
            ));
        }
        let mut builder = BooleanBuilder::with_capacity(array.len());
        for i in 0..array.len() {
            if array.is_null(i) {
                builder.append_null();
            } else if candidate.is_null(row_index(i, candidate.len())) {
                builder.append_null();
            } else {
                match compare_values_for_in(
                    array,
                    i,
                    candidate,
                    row_index(i, candidate.len()),
                    lhs_is_literal_like,
                    single_candidate,
                )? {
                    Some(equal) => builder.append_value(equal),
                    None => builder.append_null(),
                }
            }
        }
        return Ok(builder.finish());
    }
    if candidate.len() == array.len()
        && array.data_type() == candidate.data_type()
        && !matches!(
            candidate.data_type(),
            DataType::Utf8
                | DataType::LargeBinary
                | DataType::Timestamp(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::FixedSizeBinary(_)
        )
    {
        return eq(
            &array.as_ref() as &dyn arrow::array::Datum,
            &candidate.as_ref() as &dyn arrow::array::Datum,
        )
        .map_err(|e| e.to_string());
    }
    match candidate.data_type() {
        DataType::Int8 => {
            let arr = candidate.as_any().downcast_ref::<Int8Array>().unwrap();
            let scalar = Int8Array::new_scalar(arr.value(row_index(0, arr.len())));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Int16 => {
            let arr = candidate.as_any().downcast_ref::<Int16Array>().unwrap();
            let scalar = Int16Array::new_scalar(arr.value(row_index(0, arr.len())));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Int32 => {
            let arr = candidate.as_any().downcast_ref::<Int32Array>().unwrap();
            let scalar = Int32Array::new_scalar(arr.value(row_index(0, arr.len())));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Int64 => {
            let arr = candidate.as_any().downcast_ref::<Int64Array>().unwrap();
            let scalar = Int64Array::new_scalar(arr.value(row_index(0, arr.len())));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Float32 => {
            let arr = candidate.as_any().downcast_ref::<Float32Array>().unwrap();
            let scalar = Float32Array::new_scalar(arr.value(row_index(0, arr.len())));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Float64 => {
            let arr = candidate.as_any().downcast_ref::<Float64Array>().unwrap();
            let scalar = Float64Array::new_scalar(arr.value(row_index(0, arr.len())));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Boolean => {
            let arr = candidate.as_any().downcast_ref::<BooleanArray>().unwrap();
            let scalar = BooleanArray::new_scalar(arr.value(row_index(0, arr.len())));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Utf8 => {
            let values = candidate.as_any().downcast_ref::<StringArray>().unwrap();
            let input = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast IN input to StringArray".to_string())?;
            let mut builder = BooleanBuilder::with_capacity(input.len());
            for i in 0..input.len() {
                let value_idx = row_index(i, values.len());
                if input.is_null(i) || values.is_null(value_idx) {
                    builder.append_null();
                    continue;
                }
                let lhs = input.value(i);
                let rhs = values.value(value_idx);
                if let (Some(lhs_json), Some(rhs_json)) = (
                    json_value_from_text_or_variant(lhs),
                    json_value_from_text_or_variant(rhs),
                ) {
                    builder.append_value(lhs_json == rhs_json);
                } else {
                    builder.append_value(lhs == rhs);
                }
            }
            Ok(builder.finish())
        }
        DataType::LargeBinary => {
            let values = candidate
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast IN scalar to LargeBinaryArray".to_string())?;
            let input = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast IN input to LargeBinaryArray".to_string())?;
            let mut builder = BooleanBuilder::with_capacity(input.len());
            for i in 0..input.len() {
                let value_idx = row_index(i, values.len());
                if input.is_null(i) || values.is_null(value_idx) {
                    builder.append_null();
                } else if let (Some(input_json), Some(scalar_json)) = (
                    variant_json_value(input.value(i)),
                    variant_json_value(values.value(value_idx)),
                ) {
                    builder.append_value(input_json == scalar_json);
                } else {
                    builder.append_value(input.value(i) == values.value(value_idx));
                }
            }
            Ok(builder.finish())
        }
        DataType::Date32 => {
            let arr = candidate.as_any().downcast_ref::<Date32Array>().unwrap();
            let scalar = Date32Array::new_scalar(arr.value(row_index(0, arr.len())));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let arr = candidate
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        "failed to downcast IN scalar to TimestampSecondArray".to_string()
                    })?;
                let input = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        "failed to downcast IN input to TimestampSecondArray".to_string()
                    })?;
                let mut builder = BooleanBuilder::with_capacity(input.len());
                for i in 0..input.len() {
                    let value_idx = row_index(i, arr.len());
                    if input.is_null(i) || arr.is_null(value_idx) {
                        builder.append_null();
                    } else {
                        builder.append_value(input.value(i) == arr.value(value_idx));
                    }
                }
                Ok(builder.finish())
            }
            TimeUnit::Millisecond => {
                let arr = candidate
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        "failed to downcast IN scalar to TimestampMillisecondArray".to_string()
                    })?;
                let input = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        "failed to downcast IN input to TimestampMillisecondArray".to_string()
                    })?;
                let mut builder = BooleanBuilder::with_capacity(input.len());
                for i in 0..input.len() {
                    let value_idx = row_index(i, arr.len());
                    if input.is_null(i) || arr.is_null(value_idx) {
                        builder.append_null();
                    } else {
                        builder.append_value(input.value(i) == arr.value(value_idx));
                    }
                }
                Ok(builder.finish())
            }
            TimeUnit::Microsecond => {
                let arr = candidate
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        "failed to downcast IN scalar to TimestampMicrosecondArray".to_string()
                    })?;
                let input = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        "failed to downcast IN input to TimestampMicrosecondArray".to_string()
                    })?;
                let mut builder = BooleanBuilder::with_capacity(input.len());
                for i in 0..input.len() {
                    let value_idx = row_index(i, arr.len());
                    if input.is_null(i) || arr.is_null(value_idx) {
                        builder.append_null();
                    } else {
                        builder.append_value(input.value(i) == arr.value(value_idx));
                    }
                }
                Ok(builder.finish())
            }
            TimeUnit::Nanosecond => {
                let arr = candidate
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        "failed to downcast IN scalar to TimestampNanosecondArray".to_string()
                    })?;
                let input = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        "failed to downcast IN input to TimestampNanosecondArray".to_string()
                    })?;
                let mut builder = BooleanBuilder::with_capacity(input.len());
                for i in 0..input.len() {
                    let value_idx = row_index(i, arr.len());
                    if input.is_null(i) || arr.is_null(value_idx) {
                        builder.append_null();
                    } else {
                        builder.append_value(input.value(i) == arr.value(value_idx));
                    }
                }
                Ok(builder.finish())
            }
        },
        DataType::Decimal128(_, _) => {
            let arr = candidate
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast IN scalar to Decimal128Array".to_string())?;
            let input = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast IN input to Decimal128Array".to_string())?;
            let mut builder = BooleanBuilder::with_capacity(input.len());
            for i in 0..input.len() {
                let value_idx = row_index(i, arr.len());
                if input.is_null(i) || arr.is_null(value_idx) {
                    builder.append_null();
                } else {
                    builder.append_value(input.value(i) == arr.value(value_idx));
                }
            }
            Ok(builder.finish())
        }
        DataType::Decimal256(_, _) => {
            let arr = candidate
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast IN scalar to Decimal256Array".to_string())?;
            let input = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast IN input to Decimal256Array".to_string())?;
            let mut builder = BooleanBuilder::with_capacity(input.len());
            for i in 0..input.len() {
                let value_idx = row_index(i, arr.len());
                if input.is_null(i) || arr.is_null(value_idx) {
                    builder.append_null();
                } else {
                    builder.append_value(input.value(i) == arr.value(value_idx));
                }
            }
            Ok(builder.finish())
        }
        DataType::FixedSizeBinary(width) if *width == 16 => {
            let arr = candidate
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    "failed to downcast IN scalar to FixedSizeBinaryArray".to_string()
                })?;
            let input = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast IN input to FixedSizeBinaryArray".to_string())?;
            let mut builder = BooleanBuilder::with_capacity(input.len());
            for i in 0..input.len() {
                let value_idx = row_index(i, arr.len());
                if input.is_null(i) || arr.is_null(value_idx) {
                    builder.append_null();
                } else {
                    builder.append_value(input.value(i) == arr.value(value_idx));
                }
            }
            Ok(builder.finish())
        }
        other => Err(format!("unsupported IN predicate type: {:?}", other)),
    }
}

fn compare_values_for_in(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
    lhs_is_literal_like: bool,
    single_candidate: bool,
) -> Result<Option<bool>, String> {
    if left.data_type() != right.data_type() {
        return Err(format!(
            "IN nested type mismatch: {:?} vs {:?}",
            left.data_type(),
            right.data_type()
        ));
    }
    if left.is_null(left_idx) || right.is_null(right_idx) {
        return Ok(match left.data_type() {
            DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => {
                if lhs_is_literal_like {
                    None
                } else if left.is_null(left_idx)
                    && right.is_null(right_idx)
                    && matches!(left.data_type(), DataType::Struct(_))
                    && single_candidate
                {
                    None
                } else if left.is_null(left_idx) && right.is_null(right_idx) {
                    Some(true)
                } else if left.is_null(left_idx) {
                    Some(false)
                } else if matches!(left.data_type(), DataType::Struct(_)) && single_candidate {
                    None
                } else {
                    Some(false)
                }
            }
            _ => Some(left.is_null(left_idx) && right.is_null(right_idx)),
        });
    }
    match left.data_type() {
        DataType::List(_) => compare_list_values_for_in(
            left,
            left_idx,
            right,
            right_idx,
            lhs_is_literal_like,
            single_candidate,
        ),
        DataType::Struct(_) => compare_struct_values_for_in(
            left,
            left_idx,
            right,
            right_idx,
            lhs_is_literal_like,
            single_candidate,
        ),
        DataType::Map(_, _) => compare_map_values_for_in(
            left,
            left_idx,
            right,
            right_idx,
            lhs_is_literal_like,
            single_candidate,
        ),
        _ => compare_values_with_null(left, left_idx, right, right_idx, true).map(Some),
    }
}

fn merge_nested_compare(acc: &mut Option<bool>, next: Option<bool>) {
    match next {
        Some(false) => *acc = Some(false),
        None if !matches!(acc, Some(false)) => *acc = None,
        _ => {}
    }
}

fn compare_list_values_for_in(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
    lhs_is_literal_like: bool,
    single_candidate: bool,
) -> Result<Option<bool>, String> {
    let l = left
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| "failed to downcast left to ListArray".to_string())?;
    let r = right
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| "failed to downcast right to ListArray".to_string())?;

    let l_offsets = l.value_offsets();
    let r_offsets = r.value_offsets();
    let l_start = l_offsets[left_idx] as usize;
    let l_end = l_offsets[left_idx + 1] as usize;
    let r_start = r_offsets[right_idx] as usize;
    let r_end = r_offsets[right_idx + 1] as usize;
    let l_len = l_end.saturating_sub(l_start);
    let r_len = r_end.saturating_sub(r_start);
    if l_len != r_len {
        return Ok(Some(false));
    }

    let l_values = l.values();
    let r_values = r.values();
    let mut result = Some(true);
    for offset in 0..l_len {
        let item = compare_values_for_in(
            &l_values,
            l_start + offset,
            &r_values,
            r_start + offset,
            lhs_is_literal_like,
            single_candidate,
        )?;
        merge_nested_compare(&mut result, item);
        if matches!(result, Some(false)) {
            break;
        }
    }
    Ok(result)
}

fn compare_struct_values_for_in(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
    lhs_is_literal_like: bool,
    single_candidate: bool,
) -> Result<Option<bool>, String> {
    let l = left
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| "failed to downcast left to StructArray".to_string())?;
    let r = right
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| "failed to downcast right to StructArray".to_string())?;
    if l.num_columns() != r.num_columns() {
        return Ok(Some(false));
    }
    let mut result = Some(true);
    for col_idx in 0..l.num_columns() {
        let item = compare_values_for_in(
            l.column(col_idx),
            left_idx,
            r.column(col_idx),
            right_idx,
            lhs_is_literal_like,
            single_candidate,
        )?;
        merge_nested_compare(&mut result, item);
        if matches!(result, Some(false)) {
            break;
        }
    }
    Ok(result)
}

fn compare_map_values_for_in(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
    lhs_is_literal_like: bool,
    single_candidate: bool,
) -> Result<Option<bool>, String> {
    let l = left
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| "failed to downcast left to MapArray".to_string())?;
    let r = right
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| "failed to downcast right to MapArray".to_string())?;

    let l_offsets = l.value_offsets();
    let r_offsets = r.value_offsets();
    let l_start = l_offsets[left_idx] as usize;
    let l_end = l_offsets[left_idx + 1] as usize;
    let r_start = r_offsets[right_idx] as usize;
    let r_end = r_offsets[right_idx + 1] as usize;
    let l_len = l_end.saturating_sub(l_start);
    let r_len = r_end.saturating_sub(r_start);
    if l_len != r_len {
        return Ok(Some(false));
    }

    let l_keys = l.keys();
    let r_keys = r.keys();
    let l_values = l.values();
    let r_values = r.values();
    let mut result = Some(true);
    for offset in 0..l_len {
        let li = l_start + offset;
        let ri = r_start + offset;
        merge_nested_compare(
            &mut result,
            compare_values_for_in(
                l_keys,
                li,
                r_keys,
                ri,
                lhs_is_literal_like,
                single_candidate,
            )?,
        );
        merge_nested_compare(
            &mut result,
            compare_values_for_in(
                l_values,
                li,
                r_values,
                ri,
                lhs_is_literal_like,
                single_candidate,
            )?,
        );
        if matches!(result, Some(false)) {
            break;
        }
    }
    Ok(result)
}

fn expr_contains_slot(arena: &ExprArena, id: ExprId) -> bool {
    match arena.node(id) {
        Some(ExprNode::SlotId(_)) => true,
        Some(ExprNode::ArrayExpr { elements }) => elements
            .iter()
            .any(|child| expr_contains_slot(arena, *child)),
        Some(ExprNode::StructExpr { fields }) => {
            fields.iter().any(|child| expr_contains_slot(arena, *child))
        }
        Some(ExprNode::Cast(child))
        | Some(ExprNode::CastTime(child))
        | Some(ExprNode::CastTimeFromDatetime(child))
        | Some(ExprNode::DictDecode { child, .. })
        | Some(ExprNode::Clone(child))
        | Some(ExprNode::Not(child))
        | Some(ExprNode::IsNull(child))
        | Some(ExprNode::IsNotNull(child)) => expr_contains_slot(arena, *child),
        Some(ExprNode::Add(left, right))
        | Some(ExprNode::Sub(left, right))
        | Some(ExprNode::Mul(left, right))
        | Some(ExprNode::Div(left, right))
        | Some(ExprNode::Mod(left, right))
        | Some(ExprNode::Eq(left, right))
        | Some(ExprNode::EqForNull(left, right))
        | Some(ExprNode::Ne(left, right))
        | Some(ExprNode::Lt(left, right))
        | Some(ExprNode::Le(left, right))
        | Some(ExprNode::Gt(left, right))
        | Some(ExprNode::Ge(left, right))
        | Some(ExprNode::And(left, right))
        | Some(ExprNode::Or(left, right)) => {
            expr_contains_slot(arena, *left) || expr_contains_slot(arena, *right)
        }
        Some(ExprNode::In { child, values, .. }) => {
            expr_contains_slot(arena, *child)
                || values.iter().any(|value| expr_contains_slot(arena, *value))
        }
        Some(ExprNode::Case { children, .. }) => children
            .iter()
            .any(|child| expr_contains_slot(arena, *child)),
        Some(ExprNode::FunctionCall { args, .. }) => {
            args.iter().any(|arg| expr_contains_slot(arena, *arg))
        }
        Some(ExprNode::LambdaFunction {
            body,
            common_sub_exprs,
            ..
        }) => {
            expr_contains_slot(arena, *body)
                || common_sub_exprs
                    .iter()
                    .any(|(_, expr)| expr_contains_slot(arena, *expr))
        }
        Some(ExprNode::Literal(_)) | None => false,
    }
}

fn variant_json_value(bytes: &[u8]) -> Option<JsonValue> {
    let text = VariantValue::from_serialized(bytes)
        .ok()?
        .to_json_local()
        .ok()?;
    serde_json::from_str(&text).ok()
}

fn json_value_from_text_or_variant(text: &str) -> Option<JsonValue> {
    serde_json::from_str(text)
        .ok()
        .or_else(|| variant_json_value(text.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{
        Int32Builder, Int64Builder, MapArray, MapBuilder, MapFieldNames, RecordBatch,
    };
    use arrow::datatypes::{Field, Schema};

    fn create_test_map_array(rows: &[Option<&[(i32, i64)]>]) -> MapArray {
        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            Int32Builder::new(),
            Int64Builder::new(),
        );
        for row in rows {
            match row {
                Some(entries) => {
                    for (key, value) in *entries {
                        builder.keys().append_value(*key);
                        builder.values().append_value(*value);
                    }
                    builder.append(true).unwrap();
                }
                None => builder.append(false).unwrap(),
            }
        }
        builder.finish()
    }

    #[test]
    fn eq_with_scalar_supports_map_values() {
        let array = Arc::new(create_test_map_array(&[
            Some(&[(0, 10), (1, 11)]),
            Some(&[(0, 10), (1, 12)]),
            None,
        ])) as ArrayRef;
        let scalar = Arc::new(create_test_map_array(&[Some(&[(0, 10), (1, 11)])])) as ArrayRef;

        let result = eq_with_candidate(&array, &scalar, false, true).expect("compare map scalar");
        assert!(result.value(0));
        assert!(!result.value(1));
        assert!(result.is_null(2));
    }

    #[test]
    fn eval_in_keeps_temporal_matches_when_list_contains_null_literal() {
        let ts_type = DataType::Timestamp(TimeUnit::Microsecond, None);
        let values = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1_672_531_190_000_000_i64),
            Some(1_672_531_193_000_000_i64),
            None,
        ])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new("dt", ts_type.clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };

        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), ts_type.clone());
        let match_lit = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("2022-12-31 23:59:50".to_string())),
            DataType::Utf8,
        );
        let match_cast = arena.push_typed(ExprNode::Cast(match_lit), ts_type.clone());
        let rogue_lit =
            arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let rogue_cast = arena.push_typed(ExprNode::Cast(rogue_lit), ts_type.clone());
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![match_cast, rogue_cast],
                is_not_in: false,
            },
            DataType::Boolean,
        );

        let result = arena.eval(expr, &chunk).expect("eval IN");
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.value(0));
        assert!(result.is_null(1));
        assert!(result.is_null(2));
    }
}
