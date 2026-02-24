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
use arrow::array::{Array, ArrayRef};
use arrow::compute::kernels::boolean::is_not_null;
use arrow::compute::kernels::cast;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;
use std::sync::Arc;

/// Evaluate coalesce function.
/// Returns the first non-NULL value from the arguments, or NULL if all arguments are NULL.
///
/// Supports:
/// - coalesce(expr1, expr2, ...): returns the first non-NULL value
/// - Requires at least 2 arguments
///
/// Implementation aligns with StarRocks BE:
/// - For each row, checks arguments from left to right
/// - Returns first non-NULL value
/// - Returns NULL if all arguments are NULL
/// - Optimizations: skips all-NULL columns, early returns if first column is all non-NULL
pub fn eval_coalesce(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.is_empty() {
        return Err("coalesce: requires at least one argument".to_string());
    }

    // Evaluate all arguments
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(args.len());
    for &arg_id in args {
        let arr = arena.eval(arg_id, chunk)?;
        arrays.push(arr);
    }

    // Determine target type from output type or first non-null array
    let output_type = arena
        .data_type(expr)
        .ok_or_else(|| "coalesce: missing output type".to_string())?;

    let target_type = if matches!(output_type, DataType::Null) {
        // If output type is Null, try to infer from arguments
        arrays
            .iter()
            .find(|arr| !matches!(arr.data_type(), DataType::Null))
            .map(|arr| arr.data_type().clone())
            .unwrap_or(DataType::Null)
    } else {
        output_type.clone()
    };

    // If all arrays are Null type, return a null array
    if matches!(target_type, DataType::Null) {
        let len = arrays[0].len();
        // Create a null array of appropriate type
        // For now, return Int64 null array as a fallback
        let null_values: Vec<Option<i64>> = (0..len).map(|_| None).collect();
        return Ok(Arc::new(arrow::array::Int64Array::from(null_values)) as ArrayRef);
    }

    // Cast all arrays to target type
    let mut typed_arrays: Vec<ArrayRef> = Vec::with_capacity(arrays.len());
    for arr in arrays {
        let typed_arr = if arr.data_type() != &target_type {
            cast(arr.as_ref(), &target_type).map_err(|e| {
                format!(
                    "coalesce: failed to cast array from {:?} to {:?}: {}",
                    arr.data_type(),
                    target_type,
                    e
                )
            })?
        } else {
            arr
        };
        typed_arrays.push(typed_arr);
    }

    let len = typed_arrays[0].len();

    // Optimization: check if first array is all non-null
    let first_mask = is_not_null(typed_arrays[0].as_ref()).map_err(|e| e.to_string())?;
    let first_mask_arr = first_mask
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .ok_or_else(|| "coalesce: failed to downcast mask to BooleanArray".to_string())?;

    // Count non-null values in first array
    let first_non_null_count: usize = (0..len)
        .map(|i| {
            if !first_mask_arr.is_null(i) && first_mask_arr.value(i) {
                1
            } else {
                0
            }
        })
        .sum();

    // If first array is all non-null, return it directly
    if first_non_null_count == len {
        return Ok(typed_arrays[0].clone());
    }

    // If first array is all null, we can skip it
    if first_non_null_count == 0 && typed_arrays.len() > 1 {
        // Skip first array and process remaining
        return eval_coalesce(arena, expr, &args[1..], chunk);
    }

    // General case: build result by iteratively filling nulls
    // Start with the first array
    let mut result = typed_arrays[0].clone();

    // For each subsequent array, use zip to fill nulls in result with values from that array
    // zip(mask, result, arr) returns result where mask is true, arr where mask is false
    // We want: result where result is not null, arr where result is null
    for arr in typed_arrays.iter().skip(1) {
        let result_mask = is_not_null(result.as_ref()).map_err(|e| e.to_string())?;
        // zip with result_mask: if true (result is not null), use result; if false (result is null), use arr
        result = zip(
            &result_mask,
            &result.as_ref() as &dyn arrow::array::Datum,
            &arr.as_ref() as &dyn arrow::array::Datum,
        )
        .map_err(|e: arrow::error::ArrowError| e.to_string())?;
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn create_test_chunk_int(values: Vec<Option<i64>>) -> Chunk {
        let array = Arc::new(Int64Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("col0", DataType::Int64, true),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Chunk::new(batch)
    }

    fn create_test_chunk_string(values: Vec<Option<String>>) -> Chunk {
        let array = Arc::new(StringArray::from_iter(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("col0", DataType::Utf8, true),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Chunk::new(batch)
    }

    #[test]
    fn test_coalesce_two_args_first_not_null() {
        let mut arena = ExprArena::default();
        let lit1 = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));
        let lit2 = arena.push(ExprNode::Literal(LiteralValue::Int64(20)));
        let coalesce = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Coalesce,
                args: vec![lit1, lit2],
            },
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![Some(1)]);

        let result = arena.eval(coalesce, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 10);
    }

    #[test]
    fn test_coalesce_two_args_first_null() {
        let mut arena = ExprArena::default();
        let lit1 = arena.push(ExprNode::Literal(LiteralValue::Null));
        let lit2 = arena.push(ExprNode::Literal(LiteralValue::Int64(20)));
        let coalesce = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Coalesce,
                args: vec![lit1, lit2],
            },
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![Some(1)]);

        let result = arena.eval(coalesce, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 20);
    }

    #[test]
    fn test_coalesce_three_args() {
        let mut arena = ExprArena::default();
        let lit1 = arena.push(ExprNode::Literal(LiteralValue::Null));
        let lit2 = arena.push(ExprNode::Literal(LiteralValue::Null));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(30)));
        let coalesce = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Coalesce,
                args: vec![lit1, lit2, lit3],
            },
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![Some(1)]);

        let result = arena.eval(coalesce, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 30);
    }

    #[test]
    fn test_coalesce_all_null() {
        let mut arena = ExprArena::default();
        let lit1 = arena.push(ExprNode::Literal(LiteralValue::Null));
        let lit2 = arena.push(ExprNode::Literal(LiteralValue::Null));
        let coalesce = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Coalesce,
                args: vec![lit1, lit2],
            },
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![Some(1)]);

        let result = arena.eval(coalesce, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert!(result_arr.is_null(0));
    }

    #[test]
    fn test_coalesce_string() {
        let mut arena = ExprArena::default();
        let lit1 = arena.push(ExprNode::Literal(LiteralValue::Null));
        let lit2 = arena.push(ExprNode::Literal(LiteralValue::Utf8("hello".to_string())));
        let coalesce = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Coalesce,
                args: vec![lit1, lit2],
            },
            DataType::Utf8,
        );

        let chunk = create_test_chunk_string(vec![Some("test".to_string())]);

        let result = arena.eval(coalesce, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_arr.value(0), "hello");
    }
}
