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
use arrow::array::{Array, ArrayRef, BooleanArray, new_null_array};
use arrow::compute::cast;
use arrow::compute::kernels::zip::zip;
use arrow::compute::take;
use arrow::datatypes::DataType;

// IF function for Arrow arrays
pub fn eval_if(
    arena: &ExprArena,
    if_expr: ExprId,
    condition_expr: ExprId,
    then_expr: ExprId,
    else_expr: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let condition_array = arena.eval(condition_expr, chunk)?;
    let len = condition_array.len();

    // StarRocks-compatible semantics:
    // - condition NULL is treated as FALSE (choose else branch)
    // - then/else are cast to IF's output type (or inferred if output type is NULL)
    let condition_mask = condition_array_to_bool_mask(&condition_array)?;

    let output_type = arena
        .data_type(if_expr)
        .ok_or_else(|| "if: missing output type".to_string())?
        .clone();

    // Avoid eagerly evaluating both branches:
    // StarRocks may rewrite `CASE WHEN cond THEN expr ELSE NULL END` into `if(cond, expr, NULL)`.
    // Evaluating both branches unconditionally can raise errors even when the branch is not taken
    // (e.g. `if(x > 0, a / x, NULL)` would divide by zero for rows where x == 0).
    let mut any_true = false;
    let mut any_false = false;
    for i in 0..len {
        if condition_mask.value(i) {
            any_true = true;
        } else {
            any_false = true;
        }
    }

    // Resolve output type without evaluating branches.
    let then_type = arena
        .data_type(then_expr)
        .cloned()
        .unwrap_or(DataType::Null);
    let else_type = arena
        .data_type(else_expr)
        .cloned()
        .unwrap_or(DataType::Null);
    let target_type = if !matches!(output_type, DataType::Null) {
        output_type
    } else if !matches!(then_type, DataType::Null) {
        then_type
    } else if !matches!(else_type, DataType::Null) {
        else_type
    } else {
        return Ok(new_null_array(&DataType::Null, len));
    };

    if !any_true {
        let else_array = arena.eval(else_expr, chunk)?;
        return cast_branch_to(&else_array, &target_type, len).map_err(|e| {
            format!(
                "if: failed to cast else expression to {:?}: {}",
                target_type, e
            )
        });
    }
    if !any_false {
        let then_array = arena.eval(then_expr, chunk)?;
        return cast_branch_to(&then_array, &target_type, len).map_err(|e| {
            format!(
                "if: failed to cast then expression to {:?}: {}",
                target_type, e
            )
        });
    }

    // Mixed mask: evaluate each branch only on the rows where it is taken, then "scatter" the
    // results back into full-length arrays via `take` with nullable indices.
    let mut true_indices = Vec::with_capacity(len);
    let mut false_indices = Vec::with_capacity(len);
    let mut t: u32 = 0;
    let mut f: u32 = 0;
    for i in 0..len {
        if condition_mask.value(i) {
            true_indices.push(Some(t));
            false_indices.push(None);
            t += 1;
        } else {
            true_indices.push(None);
            false_indices.push(Some(f));
            f += 1;
        }
    }

    let then_mask = BooleanArray::from(
        (0..len)
            .map(|i| condition_mask.value(i))
            .collect::<Vec<_>>(),
    );
    let else_mask = BooleanArray::from(
        (0..len)
            .map(|i| !condition_mask.value(i))
            .collect::<Vec<_>>(),
    );

    let then_batch = arrow::compute::filter_record_batch(&chunk.batch, &then_mask)
        .map_err(|e| format!("if: failed to filter then-rows: {e}"))?;
    let else_batch = arrow::compute::filter_record_batch(&chunk.batch, &else_mask)
        .map_err(|e| format!("if: failed to filter else-rows: {e}"))?;

    let then_chunk = Chunk::new(then_batch);
    let else_chunk = Chunk::new(else_batch);

    let then_small = arena.eval(then_expr, &then_chunk)?;
    let else_small = arena.eval(else_expr, &else_chunk)?;

    let then_small = cast_branch_to(&then_small, &target_type, then_small.len()).map_err(|e| {
        format!(
            "if: failed to cast then expression to {:?}: {}",
            target_type, e
        )
    })?;
    let else_small = cast_branch_to(&else_small, &target_type, else_small.len()).map_err(|e| {
        format!(
            "if: failed to cast else expression to {:?}: {}",
            target_type, e
        )
    })?;

    let true_idx_arr = arrow::array::UInt32Array::from(true_indices);
    let false_idx_arr = arrow::array::UInt32Array::from(false_indices);
    let then_full = take(&then_small, &true_idx_arr, None).map_err(|e| e.to_string())?;
    let else_full = take(&else_small, &false_idx_arr, None).map_err(|e| e.to_string())?;

    zip(
        &condition_mask,
        &then_full.as_ref() as &dyn arrow::array::Datum,
        &else_full.as_ref() as &dyn arrow::array::Datum,
    )
    .map_err(|e| e.to_string())
}

fn cast_branch_to(
    array: &ArrayRef,
    target_type: &DataType,
    len: usize,
) -> Result<ArrayRef, String> {
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    if matches!(array.data_type(), DataType::Null) {
        return Ok(new_null_array(target_type, len));
    }
    cast(array.as_ref(), target_type).map_err(|e| e.to_string())
}

fn condition_array_to_bool_mask(array: &ArrayRef) -> Result<BooleanArray, String> {
    let len = array.len();
    let values: Vec<bool> = match array.data_type() {
        DataType::Null => (0..len).map(|_| false).collect(),
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "if: failed to downcast condition to BooleanArray".to_string())?;
            (0..len).map(|i| !arr.is_null(i) && arr.value(i)).collect()
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "if: failed to downcast condition to Int8Array".to_string())?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0)
                .collect()
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "if: failed to downcast condition to Int16Array".to_string())?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0)
                .collect()
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| "if: failed to downcast condition to Int32Array".to_string())?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0)
                .collect()
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| "if: failed to downcast condition to Int64Array".to_string())?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0)
                .collect()
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| "if: failed to downcast condition to Float32Array".to_string())?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0.0)
                .collect()
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "if: failed to downcast condition to Float64Array".to_string())?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0.0)
                .collect()
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| "if: failed to downcast condition to Decimal128Array".to_string())?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0)
                .collect()
        }
        other => {
            return Err(format!(
                "if: condition must be boolean or numeric, got {:?}",
                other
            ));
        }
    };

    Ok(BooleanArray::from(values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{Int8Array, Int64Array, RecordBatchOptions};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn chunk_int64_nullable(values: Vec<Option<i64>>) -> Chunk {
        let array = Arc::new(Int64Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("c0", DataType::Int64, true),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Chunk::new(batch)
    }

    #[test]
    fn if_then_int_else_null_ok() {
        let chunk = chunk_int64_nullable(vec![Some(1), Some(0), None]);
        let mut arena = ExprArena::default();

        let cond = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let then_v = arena.push_typed(ExprNode::Literal(LiteralValue::Int8(1)), DataType::Int8);
        let else_v = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Null);

        let if_expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::If,
                args: vec![cond, then_v, else_v],
            },
            DataType::Int8,
        );

        let result = arena.eval(if_expr, &chunk).unwrap();
        let arr = result.as_any().downcast_ref::<Int8Array>().unwrap();

        assert_eq!(arr.value(0), 1);
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }

    #[test]
    fn if_null_condition_treated_as_false() {
        let chunk = chunk_int64_nullable(vec![Some(1)]);
        let mut arena = ExprArena::default();

        let cond = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Null);
        let then_v = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
        let else_v = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);

        let if_expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::If,
                args: vec![cond, then_v, else_v],
            },
            DataType::Int64,
        );

        let result = arena.eval(if_expr, &chunk).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 20);
    }

    #[test]
    fn if_does_not_eagerly_eval_then_branch() {
        // Guard against errors like division-by-zero in the untaken branch:
        // if(false, 1/0, 1) => 1 (no error).
        let schema = Arc::new(Schema::empty());
        let options = RecordBatchOptions::new().with_row_count(Some(1));
        let batch = RecordBatch::try_new_with_options(schema, vec![], &options).unwrap();
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let cond = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(false)),
            DataType::Boolean,
        );
        let one = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 100,
                precision: 7,
                scale: 2,
            }),
            DataType::Decimal128(7, 2),
        );
        let zero = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 0,
                precision: 7,
                scale: 2,
            }),
            DataType::Decimal128(7, 2),
        );
        let then_div = arena.push_typed(ExprNode::Div(one, zero), DataType::Decimal128(7, 2));
        let else_v = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 100,
                precision: 7,
                scale: 2,
            }),
            DataType::Decimal128(7, 2),
        );

        let if_expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::If,
                args: vec![cond, then_div, else_v],
            },
            DataType::Decimal128(7, 2),
        );

        let result = arena.eval(if_expr, &chunk).unwrap();
        let arr = result
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 100);
    }
}
