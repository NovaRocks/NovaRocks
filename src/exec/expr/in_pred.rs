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
    if matches!(
        scalar.data_type(),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    ) {
        if array.data_type() != scalar.data_type() {
            return Err(format!(
                "IN nested type mismatch: {:?} vs {:?}",
                array.data_type(),
                scalar.data_type()
            ));
        }
        let mut builder = BooleanBuilder::with_capacity(array.len());
        for i in 0..array.len() {
            if array.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(compare_values_with_null(array, i, scalar, 0, true)?);
            }
        }
        return Ok(builder.finish());
    }
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
    Ok(Chunk::new_with_chunk_schema(
        batch,
        Arc::new(crate::exec::chunk::ChunkSchema::empty()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;
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

        let result = eq_with_scalar(&array, &scalar).expect("compare map scalar");
        assert_eq!(result.value(0), true);
        assert_eq!(result.value(1), false);
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
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("dt", ts_type.clone(), true),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        let chunk = Chunk::new_with_slot_ids(batch, &[SlotId::new(1)]);

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
        assert_eq!(result.value(0), true);
        assert!(result.is_null(1));
        assert!(result.is_null(2));
    }
}
