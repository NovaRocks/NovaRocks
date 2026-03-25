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
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Decimal128Array, PrimitiveArray, StringArray,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use std::sync::Arc;

pub fn eval_nullif(
    arena: &ExprArena,
    _expr: ExprId,
    left: ExprId,
    right: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let left_arr = arena.eval(left, chunk)?;
    let right_arr = arena.eval(right, chunk)?;
    if left_arr.data_type() != right_arr.data_type() {
        return Err("nullif requires arguments with same type".to_string());
    }
    match left_arr.data_type() {
        DataType::Int64 => eval_nullif_primitive::<Int64Type>(&left_arr, &right_arr),
        DataType::Int32 => eval_nullif_primitive::<Int32Type>(&left_arr, &right_arr),
        DataType::Int16 => eval_nullif_primitive::<Int16Type>(&left_arr, &right_arr),
        DataType::Int8 => eval_nullif_primitive::<Int8Type>(&left_arr, &right_arr),
        DataType::Float64 => eval_nullif_primitive::<Float64Type>(&left_arr, &right_arr),
        DataType::Float32 => eval_nullif_primitive::<Float32Type>(&left_arr, &right_arr),
        DataType::Boolean => eval_nullif_bool(&left_arr, &right_arr),
        DataType::Utf8 => eval_nullif_string(&left_arr, &right_arr),
        DataType::Date32 => eval_nullif_primitive::<Date32Type>(&left_arr, &right_arr),
        DataType::Decimal128(p, s) => eval_nullif_decimal(&left_arr, &right_arr, *p, *s),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => eval_nullif_primitive::<TimestampSecondType>(&left_arr, &right_arr),
            TimeUnit::Millisecond => {
                eval_nullif_primitive::<TimestampMillisecondType>(&left_arr, &right_arr)
            }
            TimeUnit::Microsecond => {
                eval_nullif_primitive::<TimestampMicrosecondType>(&left_arr, &right_arr)
            }
            TimeUnit::Nanosecond => {
                eval_nullif_primitive::<TimestampNanosecondType>(&left_arr, &right_arr)
            }
        },
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            eval_nullif_generic(&left_arr, &right_arr)
        }
        other => Err(format!("nullif unsupported type: {:?}", other)),
    }
}

fn eval_nullif_bool(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
    let l = left
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "nullif downcast failed".to_string())?;
    let r = right
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "nullif downcast failed".to_string())?;
    if l.len() != r.len() {
        return Err("nullif boolean length mismatch".to_string());
    }
    let mut builder = BooleanBuilder::with_capacity(l.len());
    for i in 0..l.len() {
        if l.is_null(i) {
            builder.append_null();
            continue;
        }
        if r.is_null(i) {
            builder.append_value(l.value(i));
            continue;
        }
        if l.value(i) == r.value(i) {
            builder.append_null();
        } else {
            builder.append_value(l.value(i));
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn eval_nullif_primitive<T>(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String>
where
    T: ArrowPrimitiveType,
{
    let l = left
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| "nullif downcast failed".to_string())?;
    let r = right
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| "nullif downcast failed".to_string())?;
    let mut values: Vec<Option<T::Native>> = Vec::with_capacity(l.len());
    for i in 0..l.len() {
        if l.is_null(i) {
            values.push(None);
            continue;
        }
        if r.is_null(i) {
            values.push(Some(l.value(i)));
            continue;
        }
        if l.value(i) == r.value(i) {
            values.push(None);
        } else {
            values.push(Some(l.value(i)));
        }
    }
    let arr = PrimitiveArray::<T>::from_iter(values);
    Ok(Arc::new(arr) as ArrayRef)
}

fn eval_nullif_string(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
    let l = left
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "nullif downcast failed".to_string())?;
    let r = right
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "nullif downcast failed".to_string())?;
    let mut out = Vec::with_capacity(l.len());
    for i in 0..l.len() {
        if l.is_null(i) {
            out.push(None);
            continue;
        }
        if r.is_null(i) {
            out.push(Some(l.value(i).to_string()));
            continue;
        }
        if l.value(i) == r.value(i) {
            out.push(None);
        } else {
            out.push(Some(l.value(i).to_string()));
        }
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

/// Generic nullif for complex types (List, Map, Struct) using display-based comparison.
fn eval_nullif_generic(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let len = left.len();
    // Use a distinctive null marker so null elements are not confused with empty strings.
    let fmt_opts = FormatOptions::default().with_null("\\N");
    let left_fmt =
        ArrayFormatter::try_new(left.as_ref(), &fmt_opts).map_err(|e| format!("nullif: {e}"))?;
    let right_fmt =
        ArrayFormatter::try_new(right.as_ref(), &fmt_opts).map_err(|e| format!("nullif: {e}"))?;

    let mut valid = vec![true; len];
    for i in 0..len {
        if left.is_null(i) {
            valid[i] = false;
        } else if !right.is_null(i) {
            let l_str = left_fmt.value(i).to_string();
            let r_str = right_fmt.value(i).to_string();
            if l_str == r_str {
                valid[i] = false;
            }
        }
    }

    let null_buffer = arrow::buffer::NullBuffer::from(valid);
    let data = left.to_data();
    let new_data = data
        .into_builder()
        .null_bit_buffer(Some(null_buffer.inner().inner().clone()))
        .build()
        .map_err(|e| format!("nullif: {e}"))?;
    Ok(arrow::array::make_array(new_data))
}

fn eval_nullif_decimal(
    left: &ArrayRef,
    right: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, String> {
    let l = left
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| "nullif downcast failed".to_string())?;
    let r = right
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| "nullif downcast failed".to_string())?;
    let mut values = Vec::with_capacity(l.len());
    for i in 0..l.len() {
        if l.is_null(i) {
            values.push(None);
            continue;
        }
        if r.is_null(i) {
            values.push(Some(l.value(i)));
            continue;
        }
        if l.value(i) == r.value(i) {
            values.push(None);
        } else {
            values.push(Some(l.value(i)));
        }
    }
    let arr = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(arr) as ArrayRef)
}
