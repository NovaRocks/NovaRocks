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

fn inferred_if_output_type(then_type: &DataType, else_type: &DataType) -> Option<DataType> {
    match (then_type, else_type) {
        (left, right) if left == right && !matches!(left, DataType::Boolean | DataType::Null) => {
            Some(left.clone())
        }
        (left, DataType::Null) if !matches!(left, DataType::Boolean | DataType::Null) => {
            Some(left.clone())
        }
        (DataType::Null, right) if !matches!(right, DataType::Boolean | DataType::Null) => {
            Some(right.clone())
        }
        _ => None,
    }
}

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
    let target_type = if matches!(output_type, DataType::Boolean) {
        inferred_if_output_type(&then_type, &else_type).unwrap_or(output_type)
    } else if !matches!(output_type, DataType::Null) {
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

    let then_chunk = Chunk::new_like(then_batch, chunk);
    let else_chunk = Chunk::new_like(else_batch, chunk);

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
