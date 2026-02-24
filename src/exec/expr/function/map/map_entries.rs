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
use arrow::array::{Array, ArrayRef, ListArray};
use arrow::datatypes::Field;
use std::sync::Arc;

pub fn eval_map_entries(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let map_arr = arena.eval(args[0], chunk)?;
    let map = map_arr
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .ok_or_else(|| {
            format!(
                "map_entries expects MapArray, got {:?}",
                map_arr.data_type()
            )
        })?;
    let entries = Arc::new(map.entries().clone()) as ArrayRef;
    let list = ListArray::new(
        Arc::new(Field::new("item", map.entries().data_type().clone(), true)),
        map.offsets().clone(),
        entries,
        map.nulls().cloned(),
    );
    let out = Arc::new(list) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr), "map_entries")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::function::map::eval_map_function;
    use crate::exec::expr::function::map::test_utils::{slot_id_expr, typed_null};
    use arrow::array::{
        ArrayRef, Int64Array, Int64Builder, MapBuilder, StringArray, StringBuilder, StructArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use arrow::record_batch::RecordBatch;

    fn single_row_map() -> (Chunk, DataType) {
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
        (Chunk::new(batch), map_type)
    }

    #[test]
    fn test_map_entries() {
        let (chunk, map_type) = single_row_map();
        let mut arena = ExprArena::default();
        let arg = slot_id_expr(&mut arena, 1, map_type);
        let entry_type = {
            let DataType::Map(entry_field, _) = arena.data_type(arg).unwrap() else {
                panic!("map_entries input must be Map");
            };
            entry_field.data_type().clone()
        };
        let expr = typed_null(
            &mut arena,
            DataType::List(Arc::new(Field::new("item", entry_type, true))),
        );
        let out = eval_map_function("map_entries", &arena, expr, &[arg], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let entries = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.value(0), 1);
        assert_eq!(values.value(0), 10);
    }

    #[test]
    fn test_map_entries_cast_nullable_struct_key() {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        builder.keys().append_value("a");
        builder.values().append_value(1);
        builder.keys().append_value("b");
        builder.values().append_value(2);
        builder.append(true).unwrap();

        let map = Arc::new(builder.finish()) as ArrayRef;
        let map_type = map.data_type().clone();
        let field = field_with_slot_id(Field::new("m", map_type.clone(), true), SlotId::new(1));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![map]).unwrap();
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let arg = slot_id_expr(&mut arena, 1, map_type);
        let expr = typed_null(
            &mut arena,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Utf8, true)),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ])),
                true,
            ))),
        );

        let out = eval_map_function("map_entries", &arena, expr, &[arg], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let entries = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(keys.value(0), "a");
        assert_eq!(keys.value(1), "b");
    }
}
