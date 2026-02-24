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
use arrow::array::{ArrayRef, ListArray, make_array, new_empty_array, new_null_array};
use arrow::datatypes::{DataType, Field, Fields};
use arrow_buffer::OffsetBuffer;
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

fn coerce_array_expr_element(
    array: ArrayRef,
    target_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef, String> {
    if array.data_type() == target_type {
        return Ok(array);
    }
    if array.data_type() == &DataType::Null && *target_type != DataType::Null {
        return Ok(new_null_array(target_type, num_rows));
    }
    crate::exec::expr::cast::cast_with_special_rules(&array, target_type).map_err(|e| {
        format!(
            "array_expr failed to cast element type {:?} -> {:?}: {}",
            array.data_type(),
            target_type,
            e
        )
    })
}

fn relax_map_entry_nullability(target_type: &DataType, source_type: &DataType) -> DataType {
    let (DataType::Map(target_map_field, ordered), DataType::Map(source_map_field, _)) =
        (target_type, source_type)
    else {
        return target_type.clone();
    };
    let DataType::Struct(target_entries) = target_map_field.data_type() else {
        return target_type.clone();
    };
    let DataType::Struct(source_entries) = source_map_field.data_type() else {
        return target_type.clone();
    };
    if target_entries.len() != source_entries.len() {
        return target_type.clone();
    }

    let mut adjusted_entries = target_entries.iter().cloned().collect::<Vec<_>>();
    let mut changed = false;
    for idx in 0..adjusted_entries.len() {
        if !target_entries[idx].is_nullable() && source_entries[idx].is_nullable() {
            adjusted_entries[idx] = Arc::new(Field::new(
                target_entries[idx].name(),
                target_entries[idx].data_type().clone(),
                true,
            ));
            changed = true;
        }
    }
    if !changed {
        return target_type.clone();
    }

    DataType::Map(
        Arc::new(Field::new(
            target_map_field.name(),
            DataType::Struct(Fields::from(adjusted_entries)),
            target_map_field.is_nullable(),
        )),
        *ordered,
    )
}

pub fn eval_array_expr(
    arena: &ExprArena,
    id: ExprId,
    elements: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let output_type = arena
        .data_type(id)
        .cloned()
        .ok_or_else(|| "array_expr missing output type".to_string())?;
    let (mut field, mut element_type) = match output_type {
        DataType::List(field) => {
            let element_type = field.data_type().clone();
            (field, element_type)
        }
        other => {
            return Err(format!(
                "array_expr output type must be List, got {:?}",
                other
            ));
        }
    };

    let num_rows = chunk.len();
    let num_elements = elements.len();

    if num_elements == 0 {
        let mut offsets = Vec::with_capacity(num_rows + 1);
        offsets.push(0_i32);
        offsets.extend(std::iter::repeat(0_i32).take(num_rows));
        let values = new_empty_array(&element_type);
        let list = ListArray::new(field, OffsetBuffer::new(offsets.into()), values, None);
        return Ok(Arc::new(list));
    }

    let mut raw_arrays = Vec::with_capacity(num_elements);
    for expr_id in elements {
        let array = arena.eval(*expr_id, chunk)?;
        if array.len() != num_rows {
            return Err(format!(
                "array_expr element length mismatch: expected {}, got {}",
                num_rows,
                array.len()
            ));
        }
        element_type = relax_map_entry_nullability(&element_type, array.data_type());
        raw_arrays.push(array);
    }
    if field.data_type() != &element_type {
        field = Arc::new(Field::new(
            field.name(),
            element_type.clone(),
            field.is_nullable(),
        ));
    }

    let mut element_arrays = Vec::with_capacity(num_elements);
    for array in raw_arrays {
        let array = coerce_array_expr_element(array, &element_type, num_rows)?;
        element_arrays.push(array);
    }

    let mut offsets = Vec::with_capacity(num_rows + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    for _ in 0..num_rows {
        current += num_elements as i64;
        if current > i32::MAX as i64 {
            return Err("array_expr offset overflow".to_string());
        }
        offsets.push(current as i32);
    }

    let data_storage: Vec<arrow_data::ArrayData> =
        element_arrays.iter().map(|arr| arr.to_data()).collect();
    let data_refs: Vec<&arrow_data::ArrayData> = data_storage.iter().collect();
    let mut mutable =
        MutableArrayData::new(data_refs, false, num_rows.saturating_mul(num_elements));
    for row in 0..num_rows {
        for idx in 0..element_arrays.len() {
            mutable.extend(idx, row, row + 1);
        }
    }
    let values = make_array(mutable.freeze());

    let list = ListArray::new(field, OffsetBuffer::new(offsets.into()), values, None);
    Ok(Arc::new(list))
}
