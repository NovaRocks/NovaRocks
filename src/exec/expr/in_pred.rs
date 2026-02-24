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
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::{DataType, TimeUnit};
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

    if len == 0 {
        return Ok(Arc::new(BooleanArray::from(Vec::<bool>::new())));
    }

    let mut has_null_literal = false;
    let mut matched = vec![false; len];
    let dummy_chunk = empty_chunk_with_rows(1)?;

    for value_id in values {
        let scalar_array = arena.eval(*value_id, &dummy_chunk)?;
        if scalar_array.is_null(0) {
            has_null_literal = true;
            continue;
        }
        let eq_array = eq_with_scalar(&array, &scalar_array)?;
        for (row, matched_row) in matched.iter_mut().enumerate() {
            if !eq_array.is_null(row) && eq_array.value(row) {
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
        if array.is_null(row) {
            builder.append_null();
            continue;
        }
        if *matched_row {
            builder.append_value(!is_not_in);
            continue;
        }
        if has_null_literal {
            builder.append_null();
            continue;
        }
        builder.append_value(is_not_in);
    }
    Ok(Arc::new(builder.finish()))
}

fn eq_with_scalar(array: &ArrayRef, scalar: &ArrayRef) -> Result<BooleanArray, String> {
    match scalar.data_type() {
        DataType::Int8 => {
            let arr = scalar.as_any().downcast_ref::<Int8Array>().unwrap();
            let scalar = Int8Array::new_scalar(arr.value(0));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Int16 => {
            let arr = scalar.as_any().downcast_ref::<Int16Array>().unwrap();
            let scalar = Int16Array::new_scalar(arr.value(0));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Int32 => {
            let arr = scalar.as_any().downcast_ref::<Int32Array>().unwrap();
            let scalar = Int32Array::new_scalar(arr.value(0));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Int64 => {
            let arr = scalar.as_any().downcast_ref::<Int64Array>().unwrap();
            let scalar = Int64Array::new_scalar(arr.value(0));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Float32 => {
            let arr = scalar.as_any().downcast_ref::<Float32Array>().unwrap();
            let scalar = Float32Array::new_scalar(arr.value(0));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Float64 => {
            let arr = scalar.as_any().downcast_ref::<Float64Array>().unwrap();
            let scalar = Float64Array::new_scalar(arr.value(0));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Boolean => {
            let arr = scalar.as_any().downcast_ref::<BooleanArray>().unwrap();
            let scalar = BooleanArray::new_scalar(arr.value(0));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Utf8 => {
            let arr = scalar.as_any().downcast_ref::<StringArray>().unwrap();
            let scalar = StringArray::new_scalar(arr.value(0));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Date32 => {
            let arr = scalar.as_any().downcast_ref::<Date32Array>().unwrap();
            let scalar = Date32Array::new_scalar(arr.value(0));
            eq(&array.as_ref() as &dyn arrow::array::Datum, &scalar).map_err(|e| e.to_string())
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let arr = scalar
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
                let scalar_value = arr.value(0);
                let mut builder = BooleanBuilder::with_capacity(input.len());
                for i in 0..input.len() {
                    if input.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(input.value(i) == scalar_value);
                    }
                }
                Ok(builder.finish())
            }
            TimeUnit::Millisecond => {
                let arr = scalar
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
                let scalar_value = arr.value(0);
                let mut builder = BooleanBuilder::with_capacity(input.len());
                for i in 0..input.len() {
                    if input.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(input.value(i) == scalar_value);
                    }
                }
                Ok(builder.finish())
            }
            TimeUnit::Microsecond => {
                let arr = scalar
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
                let scalar_value = arr.value(0);
                let mut builder = BooleanBuilder::with_capacity(input.len());
                for i in 0..input.len() {
                    if input.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(input.value(i) == scalar_value);
                    }
                }
                Ok(builder.finish())
            }
            TimeUnit::Nanosecond => {
                let arr = scalar
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
                let scalar_value = arr.value(0);
                let mut builder = BooleanBuilder::with_capacity(input.len());
                for i in 0..input.len() {
                    if input.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(input.value(i) == scalar_value);
                    }
                }
                Ok(builder.finish())
            }
        },
        DataType::Decimal128(_, _) => {
            let arr = scalar
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast IN scalar to Decimal128Array".to_string())?;
            let input = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast IN input to Decimal128Array".to_string())?;
            let scalar_value = arr.value(0);
            let mut builder = BooleanBuilder::with_capacity(input.len());
            for i in 0..input.len() {
                if input.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(input.value(i) == scalar_value);
                }
            }
            Ok(builder.finish())
        }
        DataType::Decimal256(_, _) => {
            let arr = scalar
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast IN scalar to Decimal256Array".to_string())?;
            let input = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast IN input to Decimal256Array".to_string())?;
            let scalar_value = arr.value(0);
            let mut builder = BooleanBuilder::with_capacity(input.len());
            for i in 0..input.len() {
                if input.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(input.value(i) == scalar_value);
                }
            }
            Ok(builder.finish())
        }
        DataType::FixedSizeBinary(width) if *width == 16 => {
            let arr = scalar
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    "failed to downcast IN scalar to FixedSizeBinaryArray".to_string()
                })?;
            let input = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast IN input to FixedSizeBinaryArray".to_string())?;
            let scalar_value = arr.value(0);
            let mut builder = BooleanBuilder::with_capacity(input.len());
            for i in 0..input.len() {
                if input.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(input.value(i) == scalar_value);
                }
            }
            Ok(builder.finish())
        }
        other => Err(format!("unsupported IN predicate type: {:?}", other)),
    }
}

fn empty_chunk_with_rows(row_count: usize) -> Result<Chunk, String> {
    use arrow::array::RecordBatchOptions;
    use arrow::datatypes::Schema;
    use std::sync::Arc;

    let schema = Arc::new(Schema::empty());
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    let batch = arrow::array::RecordBatch::try_new_with_options(schema, vec![], &options)
        .map_err(|e| e.to_string())?;
    Ok(Chunk::new(batch))
}
