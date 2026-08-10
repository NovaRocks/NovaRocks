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
use crate::exec::chunk::type_compatibility::{check_exact, retag_column};
use crate::exec::expr::function::compare_values_with_null;
use crate::exec::expr::{ExprArena, ExprId, ExprNode, cast_with_special_rules};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, ListArray, MapArray, Scalar, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::compute::cast;
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_types::value::variant::VariantValue;
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

fn is_string_or_null_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Null
    )
}

fn c1_dictionary_string_value_type(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::Dictionary(key_type, value_type)
            if matches!(key_type.as_ref(), DataType::Int32)
                && matches!(value_type.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            Some(value_type.as_ref())
        }
        _ => None,
    }
}

fn eq_dictionary_input_with_candidate(
    array: &ArrayRef,
    candidate: &ArrayRef,
) -> Result<Option<BooleanArray>, String> {
    let Some(value_type) = c1_dictionary_string_value_type(array.data_type()) else {
        return Ok(None);
    };
    if !is_string_or_null_type(candidate.data_type()) {
        return Ok(None);
    };
    let candidate = if candidate.data_type() == value_type {
        candidate.clone()
    } else {
        cast(candidate, value_type).map_err(|e| e.to_string())?
    };

    if candidate.len() == 1 {
        let scalar = Scalar::new(candidate);
        return eq(
            &array.as_ref() as &dyn arrow::array::Datum,
            &scalar as &dyn arrow::array::Datum,
        )
        .map(Some)
        .map_err(|e| e.to_string());
    }
    eq(
        &array.as_ref() as &dyn arrow::array::Datum,
        &candidate.as_ref() as &dyn arrow::array::Datum,
    )
    .map(Some)
    .map_err(|e| e.to_string())
}

fn is_signed_integer_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn signed_integer_value(array: &ArrayRef, row: usize) -> Result<i64, String> {
    match array.data_type() {
        DataType::Int8 => Ok(array
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| "failed to downcast signed IN value to Int8Array".to_string())?
            .value(row) as i64),
        DataType::Int16 => Ok(array
            .as_any()
            .downcast_ref::<Int16Array>()
            .ok_or_else(|| "failed to downcast signed IN value to Int16Array".to_string())?
            .value(row) as i64),
        DataType::Int32 => Ok(array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| "failed to downcast signed IN value to Int32Array".to_string())?
            .value(row) as i64),
        DataType::Int64 => Ok(array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "failed to downcast signed IN value to Int64Array".to_string())?
            .value(row)),
        other => Err(format!("unsupported signed IN value type: {other:?}")),
    }
}

fn eq_signed_integer_input_with_candidate(
    array: &ArrayRef,
    candidate: &ArrayRef,
) -> Result<Option<BooleanArray>, String> {
    if !is_signed_integer_type(array.data_type())
        || !is_signed_integer_type(candidate.data_type())
        || (candidate.len() != 1 && candidate.len() != array.len())
    {
        return Ok(None);
    }

    let mut builder = BooleanBuilder::with_capacity(array.len());
    for row in 0..array.len() {
        let candidate_row = row_index(row, candidate.len());
        if array.is_null(row) || candidate.is_null(candidate_row) {
            builder.append_null();
            continue;
        }
        builder.append_value(
            signed_integer_value(array, row)? == signed_integer_value(candidate, candidate_row)?,
        );
    }
    Ok(Some(builder.finish()))
}

fn eq_with_candidate(
    array: &ArrayRef,
    candidate: &ArrayRef,
    lhs_is_literal_like: bool,
    single_candidate: bool,
) -> Result<BooleanArray, String> {
    if let Some(result) = eq_dictionary_input_with_candidate(array, candidate)? {
        return Ok(result);
    }
    if matches!(
        candidate.data_type(),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    ) {
        let (array, candidate) =
            normalize_nested_candidate_types(array.clone(), candidate.clone())?;
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
                    &array,
                    i,
                    &candidate,
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
    if let Some(result) = eq_signed_integer_input_with_candidate(array, candidate)? {
        return Ok(result);
    }
    if matches!(array.data_type(), DataType::Utf8)
        && is_numeric_json_candidate(candidate.data_type())
    {
        return eq_utf8_json_with_numeric_candidate(array, candidate);
    }
    let (array, candidate) = normalize_scalar_candidate_types(array.clone(), candidate.clone())?;
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

fn is_numeric_json_candidate(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
    )
}

fn candidate_numeric_json_value(candidate: &ArrayRef, idx: usize) -> Option<JsonValue> {
    match candidate.data_type() {
        DataType::Int8 => candidate
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|arr| JsonValue::Number(serde_json::Number::from(arr.value(idx) as i64))),
        DataType::Int16 => candidate
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|arr| JsonValue::Number(serde_json::Number::from(arr.value(idx) as i64))),
        DataType::Int32 => candidate
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|arr| JsonValue::Number(serde_json::Number::from(arr.value(idx) as i64))),
        DataType::Int64 => candidate
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| JsonValue::Number(serde_json::Number::from(arr.value(idx)))),
        DataType::Float32 => candidate
            .as_any()
            .downcast_ref::<Float32Array>()
            .and_then(|arr| serde_json::Number::from_f64(arr.value(idx) as f64))
            .map(JsonValue::Number),
        DataType::Float64 => candidate
            .as_any()
            .downcast_ref::<Float64Array>()
            .and_then(|arr| serde_json::Number::from_f64(arr.value(idx)))
            .map(JsonValue::Number),
        _ => None,
    }
}

fn eq_utf8_json_with_numeric_candidate(
    array: &ArrayRef,
    candidate: &ArrayRef,
) -> Result<BooleanArray, String> {
    let input = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "failed to downcast IN input to StringArray".to_string())?;
    let mut builder = BooleanBuilder::with_capacity(input.len());
    for row in 0..input.len() {
        let candidate_idx = row_index(row, candidate.len());
        if input.is_null(row) || candidate.is_null(candidate_idx) {
            builder.append_null();
            continue;
        }
        let Some(candidate_json) = candidate_numeric_json_value(candidate, candidate_idx) else {
            builder.append_value(false);
            continue;
        };
        let matched = json_value_from_text_or_variant(input.value(row))
            .map(|lhs_json| lhs_json == candidate_json)
            .unwrap_or_else(|| input.value(row) == candidate_json.to_string());
        builder.append_value(matched);
    }
    Ok(builder.finish())
}

fn is_nested_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    )
}

fn same_nested_kind(left: &DataType, right: &DataType) -> bool {
    matches!(
        (left, right),
        (DataType::List(_), DataType::List(_))
            | (DataType::Struct(_), DataType::Struct(_))
            | (DataType::Map(_, _), DataType::Map(_, _))
    )
}

fn normalize_nested_candidate_types(
    left: ArrayRef,
    right: ArrayRef,
) -> Result<(ArrayRef, ArrayRef), String> {
    if !is_nested_type(left.data_type()) || !is_nested_type(right.data_type()) {
        return Ok((left, right));
    }
    if check_exact(left.data_type(), right.data_type()).is_ok() {
        let right = retag_column(&right, left.data_type()).map_err(|m| format!("{m:?}"))?;
        return Ok((left, right));
    }
    if check_exact(right.data_type(), left.data_type()).is_ok() {
        let left = retag_column(&left, right.data_type()).map_err(|m| format!("{m:?}"))?;
        return Ok((left, right));
    }
    if same_nested_kind(left.data_type(), right.data_type())
        && let Ok(casted) = cast_with_special_rules(&right, left.data_type())
    {
        return Ok((left, casted));
    }
    Ok((left, right))
}

fn normalize_scalar_candidate_types(
    left: ArrayRef,
    right: ArrayRef,
) -> Result<(ArrayRef, ArrayRef), String> {
    if left.data_type() == right.data_type()
        || is_nested_type(left.data_type())
        || is_nested_type(right.data_type())
    {
        return Ok((left, right));
    }

    let Some(target) =
        novarocks_types::comparison_common_type(left.data_type(), right.data_type())?
    else {
        return Ok((left, right));
    };
    let left = if left.data_type() == &target {
        left
    } else {
        cast(&left, &target).map_err(|e| e.to_string())?
    };
    let right = if right.data_type() == &target {
        right
    } else {
        cast(&right, &target).map_err(|e| e.to_string())?
    };
    Ok((left, right))
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
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{
        DictionaryArray, Int32Builder, Int64Array, Int64Builder, LargeStringDictionaryBuilder,
        ListArray, MapArray, MapBuilder, MapFieldNames, PrimitiveDictionaryBuilder, RecordBatch,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Int8Type, Int32Type, Schema};
    use novarocks_types::SlotId;
    use std::collections::HashMap;

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

    fn create_test_chunk_dict_status(values: Vec<Option<&str>>) -> Chunk {
        let array =
            Arc::new(values.into_iter().collect::<DictionaryArray<Int32Type>>()) as ArrayRef;
        create_test_chunk_status_array(array)
    }

    fn create_test_chunk_large_dict_status(values: Vec<Option<&str>>) -> Chunk {
        let mut builder = LargeStringDictionaryBuilder::<Int32Type>::new();
        for value in values {
            match value {
                Some(value) => {
                    builder.append(value).unwrap();
                }
                None => builder.append_null(),
            }
        }
        create_test_chunk_status_array(Arc::new(builder.finish()) as ArrayRef)
    }

    fn create_test_chunk_i8_dict_status(values: Vec<Option<&str>>) -> Chunk {
        let array = Arc::new(values.into_iter().collect::<DictionaryArray<Int8Type>>()) as ArrayRef;
        create_test_chunk_status_array(array)
    }

    fn create_test_chunk_i32_dict_i32_status(values: Vec<Option<i32>>) -> Chunk {
        let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int32Type>::new();
        for value in values {
            match value {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
        create_test_chunk_status_array(Arc::new(builder.finish()) as ArrayRef)
    }

    fn create_test_chunk_status_array(array: ArrayRef) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn bool_values(array: &ArrayRef) -> Vec<Option<bool>> {
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        (0..array.len())
            .map(|idx| (!array.is_null(idx)).then(|| array.value(idx)))
            .collect()
    }

    fn chunk_from_arrays(columns: Vec<(SlotId, &'static str, ArrayRef)>) -> Chunk {
        let fields = columns
            .iter()
            .map(|(_, name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let arrays = columns
            .iter()
            .map(|(_, _, array)| array.clone())
            .collect::<Vec<_>>();
        let slot_ids = columns.iter().map(|(slot, _, _)| *slot).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &slot_ids,
        )
        .unwrap();
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn list_i64_with_field_id(field_id: &str) -> ArrayRef {
        let field = Arc::new(Field::new("item", DataType::Int64, true).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), field_id.to_string())]),
        ));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 2, 4].into()),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            None::<NullBuffer>,
        ))
    }

    fn list_i32_with_field_id(field_id: &str) -> ArrayRef {
        let field = Arc::new(Field::new("item", DataType::Int32, true).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), field_id.to_string())]),
        ));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 2, 4].into()),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            None::<NullBuffer>,
        ))
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
    fn nested_in_ignores_arrow_field_metadata() {
        let child_array = list_i64_with_field_id("6");
        let candidate_array = list_i64_with_field_id("7");
        let chunk = chunk_from_arrays(vec![
            (SlotId::new(1), "child", child_array.clone()),
            (SlotId::new(2), "candidate", candidate_array.clone()),
        ]);
        let mut arena = ExprArena::default();
        let child = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            child_array.data_type().clone(),
        );
        let candidate = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            candidate_array.data_type().clone(),
        );
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![candidate],
                is_not_in: false,
            },
            DataType::Boolean,
        );

        let result = arena
            .eval(expr, &chunk)
            .expect("metadata-only nested IN type difference");

        assert_eq!(bool_values(&result), vec![Some(true), Some(true)]);
    }

    #[test]
    fn nested_in_casts_candidate_list_to_lhs_item_type() {
        let child_array = list_i32_with_field_id("10");
        let candidate_array = list_i64_with_field_id("11");
        let chunk = chunk_from_arrays(vec![
            (SlotId::new(1), "child", child_array.clone()),
            (SlotId::new(2), "candidate", candidate_array.clone()),
        ]);
        let mut arena = ExprArena::default();
        let child = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            child_array.data_type().clone(),
        );
        let candidate = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            candidate_array.data_type().clone(),
        );
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![candidate],
                is_not_in: false,
            },
            DataType::Boolean,
        );

        let result = arena
            .eval(expr, &chunk)
            .expect("nested IN candidate should cast to lhs item type");

        assert_eq!(bool_values(&result), vec![Some(true), Some(true)]);
    }

    #[test]
    fn scalar_in_casts_int_candidate_to_bigint_input() {
        let values = Arc::new(Int64Array::from(vec![Some(10), Some(11), None])) as ArrayRef;
        let chunk = chunk_from_arrays(vec![(SlotId::new(1), "c_int", values)]);
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let ten = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(10)), DataType::Int32);
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![ten],
                is_not_in: false,
            },
            DataType::Boolean,
        );

        let result = arena
            .eval(expr, &chunk)
            .expect("IN should cast scalar candidate to comparison common type");

        assert_eq!(bool_values(&result), vec![Some(true), Some(false), None]);
    }

    #[test]
    fn utf8_json_in_numeric_candidate_compares_json_scalar_values() {
        let json_values = Arc::new(StringArray::from(vec![
            None,
            Some("{\"a\":1}"),
            Some("1"),
            Some("3"),
            Some("{}"),
        ])) as ArrayRef;
        let chunk = chunk_from_arrays(vec![(SlotId::new(1), "json_col", json_values.clone())]);
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let one = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![one],
                is_not_in: false,
            },
            DataType::Boolean,
        );

        let result = arena
            .eval(expr, &chunk)
            .expect("JSON text IN numeric candidate");

        assert_eq!(
            bool_values(&result),
            vec![None, Some(false), Some(true), Some(false), Some(false)]
        );
    }

    #[test]
    fn dictionary_utf8_in_literal_list_uses_logical_values() {
        let chunk = create_test_chunk_dict_status(vec![
            Some("PAID"),
            Some("PENDING"),
            None,
            Some("CLOSED"),
        ]);
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let paid = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("PAID".to_string())),
            DataType::Utf8,
        );
        let closed = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("CLOSED".to_string())),
            DataType::Utf8,
        );
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![paid, closed],
                is_not_in: false,
            },
            DataType::Boolean,
        );

        let result = arena.eval(expr, &chunk).expect("dictionary IN");

        assert_eq!(
            bool_values(&result),
            vec![Some(true), Some(false), None, Some(true)]
        );
    }

    #[test]
    fn dictionary_utf8_not_in_literal_list_preserves_null_semantics() {
        let chunk = create_test_chunk_dict_status(vec![Some("PAID"), Some("PENDING"), None]);
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let paid = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("PAID".to_string())),
            DataType::Utf8,
        );
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![paid],
                is_not_in: true,
            },
            DataType::Boolean,
        );

        let result = arena.eval(expr, &chunk).expect("dictionary NOT IN");

        assert_eq!(bool_values(&result), vec![Some(false), Some(true), None]);
    }

    #[test]
    fn dictionary_utf8_largeutf8_in_literal_list_uses_logical_values() {
        let chunk = create_test_chunk_large_dict_status(vec![Some("PAID"), Some("PENDING"), None]);
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::LargeUtf8);
        let paid = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("PAID".to_string())),
            DataType::LargeUtf8,
        );
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![paid],
                is_not_in: false,
            },
            DataType::Boolean,
        );

        let result = arena.eval(expr, &chunk).expect("dictionary LargeUtf8 IN");

        assert_eq!(bool_values(&result), vec![Some(true), Some(false), None]);
    }

    #[test]
    fn dictionary_utf8_numeric_candidate_does_not_cast_to_string() {
        let chunk = create_test_chunk_dict_status(vec![Some("1")]);
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let one = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(1)), DataType::Int32);
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![one],
                is_not_in: false,
            },
            DataType::Boolean,
        );

        let err = arena
            .eval(expr, &chunk)
            .expect_err("numeric candidate should not match C1 path");

        assert!(
            err.contains("Invalid comparison operation")
                || err.contains("Dictionary")
                || err.contains("not support")
                || err.contains("unsupported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn int32_in_int64_literal_list_compares_by_signed_value() {
        let input = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(i32::MAX),
        ])) as ArrayRef;
        let chunk = create_test_chunk_status_array(input);
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let one = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let max = arena.push_typed(
            ExprNode::Literal(LiteralValue::Int64(i32::MAX as i64)),
            DataType::Int64,
        );
        let too_large = arena.push_typed(
            ExprNode::Literal(LiteralValue::Int64(i32::MAX as i64 + 1)),
            DataType::Int64,
        );
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![one, max, too_large],
                is_not_in: false,
            },
            DataType::Boolean,
        );

        let result = arena.eval(expr, &chunk).expect("integer IN coercion");

        assert_eq!(
            bool_values(&result),
            vec![Some(true), Some(false), None, Some(true)]
        );
    }

    #[test]
    fn dictionary_utf8_non_int32_key_does_not_match_c1_path() {
        let chunk = create_test_chunk_i8_dict_status(vec![Some("PAID")]);
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let paid = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("PAID".to_string())),
            DataType::Utf8,
        );
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![paid],
                is_not_in: false,
            },
            DataType::Boolean,
        );

        let err = arena
            .eval(expr, &chunk)
            .expect_err("non-Int32 dictionary key should fall back");

        assert!(
            err.contains("failed to downcast IN input to StringArray"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn dictionary_utf8_non_string_dictionary_does_not_match_c1_path() {
        let chunk = create_test_chunk_i32_dict_i32_status(vec![Some(1)]);
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let one = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("1".to_string())),
            DataType::Utf8,
        );
        let expr = arena.push_typed(
            ExprNode::In {
                child,
                values: vec![one],
                is_not_in: false,
            },
            DataType::Boolean,
        );

        let err = arena
            .eval(expr, &chunk)
            .expect_err("non-string dictionary should fall back");

        assert!(
            err.contains("failed to downcast IN input to StringArray"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn dictionary_utf8_in_and_not_in_null_candidate_preserves_no_match_null() {
        for (is_not_in, expected) in [
            (false, vec![None, Some(true), None]),
            (true, vec![None, Some(false), None]),
        ] {
            let chunk = create_test_chunk_dict_status(vec![Some("PENDING"), Some("PAID"), None]);
            let mut arena = ExprArena::default();
            let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
            let paid = arena.push_typed(
                ExprNode::Literal(LiteralValue::Utf8("PAID".to_string())),
                DataType::Utf8,
            );
            let null = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Utf8);
            let expr = arena.push_typed(
                ExprNode::In {
                    child,
                    values: vec![paid, null],
                    is_not_in,
                },
                DataType::Boolean,
            );

            let result = arena.eval(expr, &chunk).expect("dictionary IN NULL");

            assert_eq!(bool_values(&result), expected, "is_not_in={is_not_in}");
        }
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
