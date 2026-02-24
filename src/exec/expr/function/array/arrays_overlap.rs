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
use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

fn is_numeric_like_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
    ) || crate::common::largeint::is_largeint_data_type(data_type)
}

pub fn eval_arrays_overlap(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr1 = arena.eval(args[0], chunk)?;
    let arr2 = arena.eval(args[1], chunk)?;
    let list1 = arr1
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .ok_or_else(|| {
            format!(
                "arrays_overlap expects ListArray, got {:?}",
                arr1.data_type()
            )
        })?;
    let list2 = arr2
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .ok_or_else(|| {
            format!(
                "arrays_overlap expects ListArray, got {:?}",
                arr2.data_type()
            )
        })?;

    let mut values1 = list1.values().clone();
    let mut values2 = list2.values().clone();
    if values1.data_type() != values2.data_type() {
        let left_type = values1.data_type().clone();
        let right_type = values2.data_type().clone();
        if is_numeric_like_type(&left_type) && is_numeric_like_type(&right_type) {
            values1 = cast(&values1, &DataType::Float64).map_err(|e| {
                format!(
                    "arrays_overlap failed to cast left numeric type {:?} -> DOUBLE: {}",
                    left_type, e
                )
            })?;
            values2 = cast(&values2, &DataType::Float64).map_err(|e| {
                format!(
                    "arrays_overlap failed to cast right numeric type {:?} -> DOUBLE: {}",
                    right_type, e
                )
            })?;
        } else if let Ok(casted) = cast(&values2, &left_type) {
            values2 = casted;
        } else if let Ok(casted) = cast(&values1, &right_type) {
            values1 = casted;
        } else {
            return Err(format!(
                "arrays_overlap type mismatch after coercion attempts: {:?} vs {:?}",
                left_type, right_type
            ));
        }
    }
    let offsets1 = list1.value_offsets();
    let offsets2 = list2.value_offsets();

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let row1 = super::common::row_index(row, list1.len());
        let row2 = super::common::row_index(row, list2.len());
        if list1.is_null(row1) || list2.is_null(row2) {
            out.push(None);
            continue;
        }

        let s1 = offsets1[row1] as usize;
        let e1 = offsets1[row1 + 1] as usize;
        let s2 = offsets2[row2] as usize;
        let e2 = offsets2[row2 + 1] as usize;

        let mut right_has_null = false;
        for j in s2..e2 {
            if values2.is_null(j) {
                right_has_null = true;
                break;
            }
        }

        let mut found = false;
        for i in s1..e1 {
            if values1.is_null(i) {
                if right_has_null {
                    found = true;
                    break;
                }
                continue;
            }
            for j in s2..e2 {
                if values2.is_null(j) {
                    continue;
                }
                if super::common::compare_values_at(&values1, i, &values2, j)? {
                    found = true;
                    break;
                }
            }
            if found {
                break;
            }
        }
        out.push(Some(found));
    }
    Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_arrays_overlap() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, DataType::Boolean);

        let a1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let a2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let b1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let b2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let arr1 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![a1, a2],
            },
            list_type.clone(),
        );
        let arr2 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![b1, b2],
            },
            list_type,
        );

        let out =
            eval_array_function("arrays_overlap", &arena, expr, &[arr1, arr2], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
    }
}
