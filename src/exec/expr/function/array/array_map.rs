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
use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{Array, ArrayRef, ListArray, UInt32Array, new_empty_array};
use arrow::compute::{concat, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use std::collections::HashSet;
use std::sync::Arc;

pub fn eval_array_map(
    arena: &ExprArena,
    id: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() < 2 {
        return Err(format!(
            "array_map expects at least 2 arguments, got {}",
            args.len()
        ));
    }

    let (lambda_id, array_args) = match arena.node(args[0]) {
        Some(ExprNode::LambdaFunction { .. }) => (args[0], &args[1..]),
        _ => match args.last().and_then(|id| arena.node(*id)) {
            Some(ExprNode::LambdaFunction { .. }) => {
                (args[args.len() - 1], &args[..args.len() - 1])
            }
            _ => {
                return Err(
                    "array_map expects a lambda function as the first or last argument".to_string(),
                );
            }
        },
    };

    let output_type = arena
        .data_type(id)
        .cloned()
        .ok_or_else(|| "array_map missing output type".to_string())?;
    let (output_field, item_type) = match output_type {
        DataType::List(field) => {
            let item_type = field.data_type().clone();
            (field, item_type)
        }
        other => {
            return Err(format!(
                "array_map output type must be List, got {:?}",
                other
            ));
        }
    };

    let (lambda_body, lambda_args, common_sub_exprs) = match arena.node(lambda_id) {
        Some(ExprNode::LambdaFunction {
            body,
            arg_slots,
            common_sub_exprs,
            ..
        }) => (*body, arg_slots.clone(), common_sub_exprs.clone()),
        _ => return Err("array_map expects lambda function expression".to_string()),
    };

    if lambda_args.len() != array_args.len() {
        return Err("Lambda arguments should equal to lambda input arrays.".to_string());
    }

    let num_rows = chunk.len();
    let mut input_arrays = Vec::with_capacity(array_args.len());
    for expr_id in array_args {
        let array = arena.eval(*expr_id, chunk)?;
        if array.len() != num_rows {
            return Err(format!(
                "array_map input length mismatch: expected {}, got {}",
                num_rows,
                array.len()
            ));
        }
        input_arrays.push(array);
    }

    let mut list_refs = Vec::with_capacity(input_arrays.len());
    for array in &input_arrays {
        let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            format!(
                "array_map input must be ListArray, got {:?}",
                array.data_type()
            )
        })?;
        list_refs.push(list);
    }

    let mut row_valid = vec![true; num_rows];
    let mut row_lengths = vec![0usize; num_rows];
    let mut total_elements: usize = 0;

    for row in 0..num_rows {
        let mut len_opt: Option<usize> = None;
        let mut row_null = false;
        for list in &list_refs {
            if list.is_null(row) {
                row_null = true;
                break;
            }
            let offsets = list.value_offsets();
            let start = offsets[row] as i64;
            let end = offsets[row + 1] as i64;
            if end < start {
                return Err("array_map list offsets are invalid".to_string());
            }
            let len = (end - start) as usize;
            if let Some(prev) = len_opt {
                if prev != len {
                    return Err(
                        "Input array element's size is not equal in array_map().".to_string()
                    );
                }
            } else {
                len_opt = Some(len);
            }
        }

        if row_null {
            row_valid[row] = false;
            row_lengths[row] = 0;
            continue;
        }

        let len = len_opt.unwrap_or(0);
        row_lengths[row] = len;
        total_elements = total_elements
            .checked_add(len)
            .ok_or_else(|| "array_map total elements overflow".to_string())?;
    }

    let mut offsets = Vec::with_capacity(num_rows + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    for row in 0..num_rows {
        if row_valid[row] {
            current += row_lengths[row] as i64;
            if current > i32::MAX as i64 {
                return Err("array_map offset overflow".to_string());
            }
        }
        offsets.push(current as i32);
    }

    let mut null_builder = NullBufferBuilder::new(num_rows);
    for valid in &row_valid {
        if *valid {
            null_builder.append_non_null();
        } else {
            null_builder.append_null();
        }
    }
    let nulls = null_builder.finish();

    if total_elements == 0 {
        let values = new_empty_array(&item_type);
        let list = ListArray::new(
            output_field,
            OffsetBuffer::new(offsets.into()),
            values,
            nulls,
        );
        return Ok(Arc::new(list));
    }

    let mut flat_args = Vec::with_capacity(list_refs.len());
    for list in &list_refs {
        let flat = flatten_list_values(list, &row_valid)?;
        if flat.len() != total_elements {
            return Err("array_map flattened argument length mismatch".to_string());
        }
        flat_args.push(flat);
    }

    let captured_slots =
        collect_captured_slots(arena, lambda_body, &common_sub_exprs, &lambda_args);
    let captured_columns =
        replicate_captured_columns(chunk, &captured_slots, &row_lengths, total_elements)?;

    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut seen: HashSet<SlotId> = HashSet::new();

    for (idx, slot_id) in lambda_args.iter().enumerate() {
        if !seen.insert(*slot_id) {
            return Err(format!("duplicate lambda argument slot id: {}", slot_id));
        }
        let col = flat_args[idx].clone();
        fields.push(field_with_slot_id(
            Field::new(format!("lambda_arg_{idx}"), col.data_type().clone(), true),
            *slot_id,
        ));
        columns.push(col);
    }

    for (slot_id, col) in captured_columns {
        if !seen.insert(slot_id) {
            return Err(format!("lambda captured slot id conflicts: {}", slot_id));
        }
        fields.push(field_with_slot_id(
            Field::new(
                format!("lambda_capture_{slot_id}"),
                col.data_type().clone(),
                true,
            ),
            slot_id,
        ));
        columns.push(col);
    }

    let mut lambda_chunk = build_chunk_from_columns(&fields, &columns)?;

    for (idx, (slot_id, expr_id)) in common_sub_exprs.iter().enumerate() {
        if !seen.insert(*slot_id) {
            return Err(format!(
                "lambda common sub expr slot id conflicts: {}",
                slot_id
            ));
        }
        let col = arena.eval(*expr_id, &lambda_chunk)?;
        if col.len() != total_elements {
            return Err(format!(
                "lambda common sub expr length mismatch: expected {}, got {}",
                total_elements,
                col.len()
            ));
        }
        fields.push(field_with_slot_id(
            Field::new(
                format!("lambda_common_{idx}"),
                col.data_type().clone(),
                true,
            ),
            *slot_id,
        ));
        columns.push(col);
        lambda_chunk = build_chunk_from_columns(&fields, &columns)?;
    }

    let result = arena.eval(lambda_body, &lambda_chunk)?;
    if result.len() != total_elements {
        return Err(format!(
            "lambda body length mismatch: expected {}, got {}",
            total_elements,
            result.len()
        ));
    }
    if result.data_type() != &item_type {
        return Err(format!(
            "array_map result type mismatch: expected {:?}, got {:?}",
            item_type,
            result.data_type()
        ));
    }

    let list = ListArray::new(
        output_field,
        OffsetBuffer::new(offsets.into()),
        result,
        nulls,
    );
    Ok(Arc::new(list))
}

fn flatten_list_values(list: &ListArray, row_valid: &[bool]) -> Result<ArrayRef, String> {
    let values = list.values();
    let offsets = list.value_offsets();
    let mut slices: Vec<ArrayRef> = Vec::new();
    for (row, valid) in row_valid.iter().enumerate() {
        if !*valid {
            continue;
        }
        let start = offsets[row] as i64;
        let end = offsets[row + 1] as i64;
        if end < start {
            return Err("array_map list offsets are invalid".to_string());
        }
        let len = (end - start) as usize;
        if len == 0 {
            continue;
        }
        slices.push(values.slice(start as usize, len));
    }

    if slices.is_empty() {
        return Ok(new_empty_array(values.data_type()));
    }

    let refs: Vec<&dyn Array> = slices.iter().map(|arr| arr.as_ref()).collect();
    concat(&refs).map_err(|e| e.to_string())
}

fn collect_captured_slots(
    arena: &ExprArena,
    lambda_body: ExprId,
    common_sub_exprs: &[(SlotId, ExprId)],
    lambda_args: &[SlotId],
) -> Vec<SlotId> {
    let mut referenced = HashSet::new();
    collect_slot_ids(arena, lambda_body, &mut referenced);
    for (_, expr_id) in common_sub_exprs {
        collect_slot_ids(arena, *expr_id, &mut referenced);
    }

    let mut excluded: HashSet<SlotId> = lambda_args.iter().copied().collect();
    for (slot_id, _) in common_sub_exprs {
        excluded.insert(*slot_id);
    }

    let mut captured: Vec<SlotId> = referenced
        .into_iter()
        .filter(|slot_id| !excluded.contains(slot_id))
        .collect();
    captured.sort_by_key(|slot| slot.0);
    captured
}

fn collect_slot_ids(arena: &ExprArena, expr_id: ExprId, out: &mut HashSet<SlotId>) {
    let mut stack = vec![expr_id];
    while let Some(id) = stack.pop() {
        let Some(node) = arena.node(id) else { continue };
        match node {
            ExprNode::Literal(_) => {}
            ExprNode::SlotId(slot_id) => {
                out.insert(*slot_id);
            }
            ExprNode::ArrayExpr { elements } => {
                for child in elements {
                    stack.push(*child);
                }
            }
            ExprNode::StructExpr { fields } => {
                for child in fields {
                    stack.push(*child);
                }
            }
            ExprNode::LambdaFunction { .. } => {
                // Do not descend into nested lambdas when collecting captures.
            }
            ExprNode::DictDecode { child, .. } => {
                stack.push(*child);
            }
            ExprNode::Cast(child)
            | ExprNode::CastTime(child)
            | ExprNode::CastTimeFromDatetime(child)
            | ExprNode::Not(child)
            | ExprNode::IsNull(child)
            | ExprNode::IsNotNull(child)
            | ExprNode::Clone(child) => {
                stack.push(*child);
            }
            ExprNode::Add(a, b)
            | ExprNode::Sub(a, b)
            | ExprNode::Mul(a, b)
            | ExprNode::Div(a, b)
            | ExprNode::Mod(a, b)
            | ExprNode::Eq(a, b)
            | ExprNode::EqForNull(a, b)
            | ExprNode::Ne(a, b)
            | ExprNode::Lt(a, b)
            | ExprNode::Le(a, b)
            | ExprNode::Gt(a, b)
            | ExprNode::Ge(a, b)
            | ExprNode::And(a, b)
            | ExprNode::Or(a, b) => {
                stack.push(*a);
                stack.push(*b);
            }
            ExprNode::In { child, values, .. } => {
                stack.push(*child);
                for value in values {
                    stack.push(*value);
                }
            }
            ExprNode::Case { children, .. } => {
                for child in children {
                    stack.push(*child);
                }
            }
            ExprNode::FunctionCall { args, .. } => {
                for arg in args {
                    stack.push(*arg);
                }
            }
        }
    }
}

fn replicate_captured_columns(
    chunk: &Chunk,
    captured_slots: &[SlotId],
    row_lengths: &[usize],
    total_elements: usize,
) -> Result<Vec<(SlotId, ArrayRef)>, String> {
    if captured_slots.is_empty() {
        return Ok(Vec::new());
    }

    let mut indices: Vec<u32> = Vec::with_capacity(total_elements);
    for (row, len) in row_lengths.iter().enumerate() {
        for _ in 0..*len {
            indices.push(u32::try_from(row).map_err(|_| "array_map index overflow".to_string())?);
        }
    }

    let idx_array = UInt32Array::from(indices);

    let mut out = Vec::with_capacity(captured_slots.len());
    for slot_id in captured_slots {
        let col = chunk.column_by_slot_id(*slot_id)?;
        let flat = if total_elements == 0 {
            new_empty_array(col.data_type())
        } else {
            take(col.as_ref(), &idx_array, None).map_err(|e| e.to_string())?
        };
        if flat.len() != total_elements {
            return Err(format!(
                "captured column length mismatch: expected {}, got {}",
                total_elements,
                flat.len()
            ));
        }
        out.push((*slot_id, flat));
    }
    Ok(out)
}

fn build_chunk_from_columns(fields: &[Field], columns: &[ArrayRef]) -> Result<Chunk, String> {
    let schema = Arc::new(Schema::new(fields.to_vec()));
    let batch = RecordBatch::try_new(schema, columns.to_vec()).map_err(|e| e.to_string())?;
    Chunk::try_new(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::{ExprArena, ExprNode};
    use arrow::array::{ArrayRef, Int64Array, Int64Builder, ListBuilder};
    use arrow::datatypes::DataType;

    fn list_array_from_values(values: &[Option<i64>]) -> ArrayRef {
        let mut builder = ListBuilder::new(Int64Builder::new());
        for v in values {
            if let Some(v) = v {
                builder.values().append_value(*v);
            } else {
                builder.values().append_null();
            }
        }
        builder.append(true);
        Arc::new(builder.finish()) as ArrayRef
    }

    #[test]
    fn eval_array_map_with_lambda() {
        let mut arena = ExprArena::default();
        let item_field = Arc::new(Field::new("item", DataType::Int64, true));
        let list_type = DataType::List(item_field.clone());

        // Lambda args slot refs
        let slot_x = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let slot_y = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);

        // Lambda body: x + y
        let add_expr = arena.push_typed(ExprNode::Add(slot_x, slot_y), DataType::Int64);

        let lambda = arena.push_typed(
            ExprNode::LambdaFunction {
                body: add_expr,
                arg_slots: vec![SlotId::new(1), SlotId::new(2), SlotId::new(3)],
                common_sub_exprs: Vec::new(),
                is_nondeterministic: false,
            },
            DataType::Utf8,
        );

        // Array arguments as slot refs
        let arr1 = arena.push_typed(ExprNode::SlotId(SlotId::new(10)), list_type.clone());
        let arr2 = arena.push_typed(ExprNode::SlotId(SlotId::new(11)), list_type.clone());
        let arr3 = arena.push_typed(ExprNode::SlotId(SlotId::new(12)), list_type.clone());

        let func = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::ArrayMap,
                args: vec![lambda, arr1, arr2, arr3],
            },
            list_type.clone(),
        );

        let col1 = list_array_from_values(&[Some(1)]);
        let col2 = list_array_from_values(&[Some(2)]);
        let col3 = list_array_from_values(&[Some(4)]);

        let fields = vec![
            field_with_slot_id(Field::new("a", list_type.clone(), true), SlotId::new(10)),
            field_with_slot_id(Field::new("b", list_type.clone(), true), SlotId::new(11)),
            field_with_slot_id(Field::new("c", list_type.clone(), true), SlotId::new(12)),
        ];
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![col1, col2, col3]).unwrap();
        let chunk = Chunk::new(batch);

        let result = arena.eval(func, &chunk).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values();
        let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(values.len(), 1);
        assert_eq!(values.value(0), 3);
    }
}
