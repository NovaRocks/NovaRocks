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
use arrow::array::{Array, ArrayRef, MapArray, UInt32Array, make_array, new_empty_array};
use arrow::compute::{cast, concat, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::collections::HashSet;
use std::sync::Arc;

pub fn eval_map_apply(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() < 2 {
        return Err(format!(
            "map_apply expects at least 2 arguments, got {}",
            args.len()
        ));
    }

    let (lambda_id, map_args) = match arena.node(args[0]) {
        Some(ExprNode::LambdaFunction { .. }) => (args[0], &args[1..]),
        _ => match args.last().and_then(|id| arena.node(*id)) {
            Some(ExprNode::LambdaFunction { .. }) => {
                (args[args.len() - 1], &args[..args.len() - 1])
            }
            _ => {
                return Err(
                    "map_apply expects a lambda function as the first or last argument".to_string(),
                );
            }
        },
    };

    if map_args.is_empty() {
        return Err("map_apply expects at least one map argument".to_string());
    }

    let output_type = arena
        .data_type(expr)
        .cloned()
        .ok_or_else(|| "map_apply missing output type".to_string())?;
    let (map_field, output_fields, ordered) = match output_type {
        DataType::Map(field, ordered) => {
            let DataType::Struct(fields) = field.data_type().clone() else {
                return Err("map_apply output map entries type must be Struct".to_string());
            };
            if fields.len() != 2 {
                return Err("map_apply output map entries type must have 2 fields".to_string());
            }
            (field, fields, ordered)
        }
        other => {
            return Err(format!(
                "map_apply output type must be Map, got {:?}",
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
        _ => return Err("map_apply expects lambda function expression".to_string()),
    };

    let expected_lambda_args = map_args
        .len()
        .checked_mul(2)
        .ok_or_else(|| "map_apply lambda argument count overflow".to_string())?;
    if lambda_args.len() != expected_lambda_args {
        return Err(format!(
            "map_apply expects {} lambda arguments, got {}",
            expected_lambda_args,
            lambda_args.len()
        ));
    }

    let num_rows = chunk.len();
    let mut input_maps = Vec::with_capacity(map_args.len());
    for arg in map_args {
        let array = arena.eval(*arg, chunk)?;
        let map = array
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| format!("map_apply expects MapArray, got {:?}", array.data_type()))?;
        if map.len() != 1 && map.len() != num_rows {
            return Err(format!(
                "map_apply input length mismatch: expected 1 or {}, got {}",
                num_rows,
                map.len()
            ));
        }
        input_maps.push(array);
    }

    let mut row_valid = vec![true; num_rows];
    let mut row_lengths = vec![0usize; num_rows];
    let mut total_elements = 0usize;
    for row in 0..num_rows {
        let mut len_opt: Option<usize> = None;
        let mut row_is_null = false;
        for map_arr in &input_maps {
            let map = map_arr
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| "map_apply internal map downcast failed".to_string())?;
            let map_row = super::common::row_index(row, map.len());
            if map.is_null(map_row) {
                row_is_null = true;
                break;
            }
            let offsets = map.value_offsets();
            let len = (offsets[map_row + 1] - offsets[map_row]) as usize;
            if let Some(prev) = len_opt {
                if prev != len {
                    return Err(
                        "Input map element's size are not equal in map_apply().".to_string()
                    );
                }
            } else {
                len_opt = Some(len);
            }
        }

        if row_is_null {
            row_valid[row] = false;
            row_lengths[row] = 0;
            continue;
        }

        let len = len_opt.unwrap_or(0);
        row_lengths[row] = len;
        total_elements = total_elements
            .checked_add(len)
            .ok_or_else(|| "map_apply total element count overflow".to_string())?;
    }

    if total_elements == 0 {
        return build_empty_output_map(output_fields, map_field, ordered, &row_valid, num_rows);
    }

    let mut flat_columns = Vec::<ArrayRef>::with_capacity(input_maps.len() * 2);
    for map_arr in &input_maps {
        let map = map_arr
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "map_apply internal map downcast failed".to_string())?;
        let flat_keys = flatten_map_component(map, &row_valid, true)?;
        let flat_values = flatten_map_component(map, &row_valid, false)?;
        if flat_keys.len() != total_elements || flat_values.len() != total_elements {
            return Err("map_apply flattened input length mismatch".to_string());
        }
        flat_columns.push(flat_keys);
        flat_columns.push(flat_values);
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
        let col = flat_columns[idx].clone();
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

    let lambda_result = arena.eval(lambda_body, &lambda_chunk)?;
    if lambda_result.len() != total_elements {
        return Err(format!(
            "lambda body length mismatch: expected {}, got {}",
            total_elements,
            lambda_result.len()
        ));
    }
    let lambda_map = lambda_result
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| "map_apply lambda must return MAP type".to_string())?;

    let mut lambda_keys = lambda_map.keys().clone();
    let mut lambda_values = lambda_map.values().clone();
    if lambda_keys.data_type() != output_fields[0].data_type() {
        lambda_keys = cast(&lambda_keys, output_fields[0].data_type()).map_err(|e| {
            format!(
                "map_apply failed to cast lambda key to {:?}: {}",
                output_fields[0].data_type(),
                e
            )
        })?;
    }
    if lambda_values.data_type() != output_fields[1].data_type() {
        lambda_values = cast(&lambda_values, output_fields[1].data_type()).map_err(|e| {
            format!(
                "map_apply failed to cast lambda value to {:?}: {}",
                output_fields[1].data_type(),
                e
            )
        })?;
    }

    let keys_data = lambda_keys.to_data();
    let values_data = lambda_values.to_data();
    let mut out_keys = MutableArrayData::new(vec![&keys_data], false, 0);
    let mut out_values = MutableArrayData::new(vec![&values_data], false, 0);

    let lambda_offsets = lambda_map.value_offsets();
    let mut out_offsets = Vec::with_capacity(num_rows + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(num_rows);
    let mut lambda_row_cursor = 0usize;

    for row in 0..num_rows {
        if !row_valid[row] {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        }

        let len = row_lengths[row];
        let mut picked_key_rows = Vec::<usize>::new();

        for offset in 0..len {
            let lambda_row = lambda_row_cursor + offset;
            if lambda_map.is_null(lambda_row) {
                continue;
            }
            let start = lambda_offsets[lambda_row] as usize;
            let end = lambda_offsets[lambda_row + 1] as usize;
            if end <= start {
                continue;
            }
            if end - start != 1 {
                return Err(
                    "map_apply lambda result must contain exactly one key-value pair".to_string(),
                );
            }
            let key_row = start;
            let mut duplicated = false;
            for prev_key_row in &picked_key_rows {
                if super::common::compare_keys_at(
                    &lambda_keys,
                    key_row,
                    &lambda_keys,
                    *prev_key_row,
                )? {
                    duplicated = true;
                    break;
                }
            }
            if duplicated {
                continue;
            }
            out_keys.extend(0, key_row, key_row + 1);
            out_values.extend(0, key_row, key_row + 1);
            picked_key_rows.push(key_row);
            current += 1;
            if current > i32::MAX as i64 {
                return Err("map_apply offset overflow".to_string());
            }
        }

        lambda_row_cursor += len;
        out_offsets.push(current as i32);
        null_builder.append_non_null();
    }

    if lambda_row_cursor != total_elements {
        return Err("map_apply internal lambda cursor mismatch".to_string());
    }

    let out_keys = make_array(out_keys.freeze());
    let out_values = make_array(out_values.freeze());
    let entries = arrow::array::StructArray::new(output_fields, vec![out_keys, out_values], None);
    let out = MapArray::try_new(
        map_field,
        OffsetBuffer::new(out_offsets.into()),
        entries,
        null_builder.finish(),
        ordered,
    )
    .map_err(|e| format!("map_apply: {}", e))?;
    Ok(Arc::new(out))
}

fn build_empty_output_map(
    output_fields: arrow::datatypes::Fields,
    map_field: Arc<Field>,
    ordered: bool,
    row_valid: &[bool],
    num_rows: usize,
) -> Result<ArrayRef, String> {
    let out_keys = new_empty_array(output_fields[0].data_type());
    let out_values = new_empty_array(output_fields[1].data_type());
    let entries = arrow::array::StructArray::new(output_fields, vec![out_keys, out_values], None);
    let mut offsets = vec![0_i32; num_rows + 1];
    let mut null_builder = NullBufferBuilder::new(num_rows);
    for row in 0..num_rows {
        offsets[row + 1] = offsets[row];
        if row_valid[row] {
            null_builder.append_non_null();
        } else {
            null_builder.append_null();
        }
    }
    let out = MapArray::try_new(
        map_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        null_builder.finish(),
        ordered,
    )
    .map_err(|e| format!("map_apply: {}", e))?;
    Ok(Arc::new(out))
}

fn flatten_map_component(
    map: &MapArray,
    row_valid: &[bool],
    keys: bool,
) -> Result<ArrayRef, String> {
    let values = if keys { map.keys() } else { map.values() };
    let offsets = map.value_offsets();
    let mut slices = Vec::<ArrayRef>::new();
    for (row, valid) in row_valid.iter().enumerate() {
        if !*valid {
            continue;
        }
        let map_row = super::common::row_index(row, map.len());
        let start = offsets[map_row] as i64;
        let end = offsets[map_row + 1] as i64;
        if end < start {
            return Err("map_apply map offsets are invalid".to_string());
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
            indices.push(u32::try_from(row).map_err(|_| "map_apply index overflow".to_string())?);
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
    use crate::exec::expr::function::map::eval_map_function;
    use crate::exec::expr::function::map::test_utils::{slot_id_expr, typed_null};
    use arrow::array::{ArrayRef, Int64Array, Int64Builder, MapBuilder};
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use arrow::record_batch::RecordBatch;

    fn map_type_i64_i64() -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Int64, false)),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ])),
                false,
            )),
            false,
        )
    }

    fn single_map_chunk() -> (Chunk, DataType) {
        let mut b = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        b.keys().append_value(1);
        b.values().append_value(10);
        b.keys().append_value(2);
        b.values().append_value(20);
        b.append(true).unwrap();
        b.keys().append_value(3);
        b.values().append_value(30);
        b.append(true).unwrap();
        let map = Arc::new(b.finish()) as ArrayRef;
        let map_type = map.data_type().clone();
        let fields = vec![field_with_slot_id(
            Field::new("m", map_type.clone(), true),
            SlotId::new(1),
        )];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![map]).unwrap();
        (Chunk::new(batch), map_type)
    }

    fn build_lambda_identity_map(arena: &mut ExprArena) -> ExprId {
        let list_i64 = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let map_i64_i64 = map_type_i64_i64();

        let key_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(101)), DataType::Int64);
        let value_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(102)), DataType::Int64);
        let key_arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![key_slot],
            },
            list_i64.clone(),
        );
        let value_arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![value_slot],
            },
            list_i64,
        );
        let body = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Map("map_from_arrays"),
                args: vec![key_arr, value_arr],
            },
            map_i64_i64,
        );
        arena.push_typed(
            ExprNode::LambdaFunction {
                body,
                arg_slots: vec![SlotId::new(101), SlotId::new(102)],
                common_sub_exprs: Vec::new(),
                is_nondeterministic: false,
            },
            DataType::Null,
        )
    }

    #[test]
    fn test_map_apply_identity() {
        let (chunk, map_type) = single_map_chunk();
        let mut arena = ExprArena::default();
        let lambda = build_lambda_identity_map(&mut arena);
        let map_arg = slot_id_expr(&mut arena, 1, map_type.clone());
        let expr = typed_null(&mut arena, map_type.clone());

        let out = eval_map_function("map_apply", &arena, expr, &[lambda, map_arg], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(out.value_length(0), 2);
        assert_eq!(out.value_length(1), 1);
        let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(keys.value(0), 1);
        assert_eq!(values.value(0), 10);
        assert_eq!(keys.value(1), 2);
        assert_eq!(values.value(1), 20);
    }

    #[test]
    fn test_map_apply_lambda_output_must_be_map() {
        let (chunk, map_type) = single_map_chunk();
        let mut arena = ExprArena::default();
        let key_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(101)), DataType::Int64);
        let lambda = arena.push_typed(
            ExprNode::LambdaFunction {
                body: key_slot,
                arg_slots: vec![SlotId::new(101), SlotId::new(102)],
                common_sub_exprs: Vec::new(),
                is_nondeterministic: false,
            },
            DataType::Null,
        );
        let map_arg = slot_id_expr(&mut arena, 1, map_type.clone());
        let expr = typed_null(&mut arena, map_type);

        let err = eval_map_function("map_apply", &arena, expr, &[lambda, map_arg], &chunk)
            .expect_err("must fail");
        assert!(err.contains("must return MAP"));
    }

    #[test]
    fn test_map_apply_transform_values_alias() {
        let (chunk, map_type) = single_map_chunk();
        let mut arena = ExprArena::default();
        let list_i64 = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));

        let key_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(101)), DataType::Int64);
        let value_slot = arena.push_typed(ExprNode::SlotId(SlotId::new(102)), DataType::Int64);
        let one = arena.push_typed(
            ExprNode::Literal(crate::exec::expr::LiteralValue::Int64(1)),
            DataType::Int64,
        );
        let value_plus_one = arena.push_typed(ExprNode::Add(value_slot, one), DataType::Int64);
        let key_arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![key_slot],
            },
            list_i64.clone(),
        );
        let value_arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![value_plus_one],
            },
            list_i64,
        );
        let body = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Map("map_from_arrays"),
                args: vec![key_arr, value_arr],
            },
            map_type.clone(),
        );
        let lambda = arena.push_typed(
            ExprNode::LambdaFunction {
                body,
                arg_slots: vec![SlotId::new(101), SlotId::new(102)],
                common_sub_exprs: Vec::new(),
                is_nondeterministic: false,
            },
            DataType::Null,
        );
        let map_arg = slot_id_expr(&mut arena, 1, map_type.clone());
        let expr = typed_null(&mut arena, map_type);

        let out = eval_map_function("transform_values", &arena, expr, &[lambda, map_arg], &chunk)
            .unwrap();
        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.value(0), 11);
        assert_eq!(values.value(1), 21);
        assert_eq!(values.value(2), 31);
    }
}
