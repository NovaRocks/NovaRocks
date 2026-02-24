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
use arrow::array::{Array, ArrayRef, Int32Array, ListArray, MapArray};
use std::sync::Arc;

pub fn eval_cardinality(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;

    let mut out = Vec::with_capacity(arr.len());
    if let Some(map) = arr.as_any().downcast_ref::<MapArray>() {
        let offsets = map.value_offsets();
        for row in 0..map.len() {
            if map.is_null(row) {
                out.push(None);
            } else {
                out.push(Some(offsets[row + 1] - offsets[row]));
            }
        }
    } else if let Some(list) = arr.as_any().downcast_ref::<ListArray>() {
        let offsets = list.value_offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                out.push(None);
            } else {
                out.push(Some(offsets[row + 1] - offsets[row]));
            }
        }
    } else {
        return Err(format!(
            "cardinality expects ARRAY or MAP, got {:?}",
            arr.data_type()
        ));
    }

    let out = Arc::new(Int32Array::from(out)) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr), "cardinality")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::function::map::eval_map_function;
    use crate::exec::expr::function::map::test_utils::{chunk_len_1, slot_id_expr, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::{ArrayRef, Int32Array, Int64Builder, MapBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_cardinality_array() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, DataType::Int32);
        let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![e1, e2],
            },
            list_type,
        );

        let out = eval_map_function("cardinality", &arena, expr, &[arr], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(out.value(0), 2);
    }

    #[test]
    fn test_cardinality_map() {
        let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        builder.keys().append_value(1);
        builder.values().append_value(10);
        builder.keys().append_value(2);
        builder.values().append_value(20);
        builder.append(true).unwrap();
        let map = Arc::new(builder.finish()) as ArrayRef;
        let map_type = map.data_type().clone();
        let field = field_with_slot_id(Field::new("m", map_type.clone(), true), SlotId::new(1));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![map]).unwrap();
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let arg = slot_id_expr(&mut arena, 1, map_type);
        let expr = typed_null(&mut arena, DataType::Int32);
        let out = eval_map_function("cardinality", &arena, expr, &[arg], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(out.value(0), 2);
    }
}
