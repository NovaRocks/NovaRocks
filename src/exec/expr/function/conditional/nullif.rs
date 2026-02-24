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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::{ExprId, ExprNode, LiteralValue};
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn chunk_len_1() -> Chunk {
        let array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("dummy", DataType::Int64, false),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Chunk::new(batch)
    }

    fn literal_i64(arena: &mut ExprArena, v: i64) -> ExprId {
        arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
    }

    fn typed_null(arena: &mut ExprArena, data_type: DataType) -> ExprId {
        arena.push_typed(ExprNode::Literal(LiteralValue::Null), data_type)
    }

    #[test]
    fn test_nullif_int_values() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Int64);
        let chunk = chunk_len_1();

        let a = literal_i64(&mut arena, 1);
        let b = literal_i64(&mut arena, 1);
        let arr = eval_nullif(&arena, expr, a, b, &chunk).unwrap();
        let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));

        let c = literal_i64(&mut arena, 1);
        let d = literal_i64(&mut arena, 2);
        let arr2 = eval_nullif(&arena, expr, c, d, &chunk).unwrap();
        let arr2 = arr2.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr2.value(0), 1);
    }
}
