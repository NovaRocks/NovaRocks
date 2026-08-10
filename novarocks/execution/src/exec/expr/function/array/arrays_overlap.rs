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
use arrow::datatypes::{DataType, Field};
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
    ) || novarocks_types::largeint::is_largeint_data_type(data_type)
}

fn is_varchar_castable_scalar(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::Binary
    ) || novarocks_types::largeint::is_largeint_data_type(data_type)
}

fn common_overlap_value_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    if matches!(left, DataType::Null) {
        return Some(right.clone());
    }
    if matches!(right, DataType::Null) {
        return Some(left.clone());
    }
    match (left, right) {
        (DataType::List(left_item), DataType::List(right_item)) => {
            let item_type =
                common_overlap_value_type(left_item.data_type(), right_item.data_type())?;
            Some(DataType::List(Arc::new(Field::new(
                left_item.name(),
                item_type,
                left_item.is_nullable() || right_item.is_nullable(),
            ))))
        }
        (DataType::List(_), _) | (_, DataType::List(_)) => None,
        _ if is_numeric_like_type(left) && is_numeric_like_type(right) => Some(DataType::Float64),
        _ if matches!(left, DataType::Utf8) && is_varchar_castable_scalar(right) => {
            Some(DataType::Utf8)
        }
        _ if matches!(right, DataType::Utf8) && is_varchar_castable_scalar(left) => {
            Some(DataType::Utf8)
        }
        _ => None,
    }
}

pub fn eval_arrays_overlap(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr1 = arena.eval(args[0], chunk)?;
    let arr2 = arena.eval(args[1], chunk)?;
    if matches!(arr1.data_type(), DataType::Null) || matches!(arr2.data_type(), DataType::Null) {
        return Ok(Arc::new(BooleanArray::from(vec![None; chunk.len()])) as ArrayRef);
    }
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
        if let Some(target_type) = common_overlap_value_type(&left_type, &right_type) {
            values1 =
                super::common::cast_with_special_rules(&values1, &target_type, "arrays_overlap")
                    .map_err(|e| {
                        format!(
                            "arrays_overlap failed to cast left type {:?} -> {:?}: {}",
                            left_type, target_type, e
                        )
                    })?;
            values2 =
                super::common::cast_with_special_rules(&values2, &target_type, "arrays_overlap")
                    .map_err(|e| {
                        format!(
                            "arrays_overlap failed to cast right type {:?} -> {:?}: {}",
                            right_type, target_type, e
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
