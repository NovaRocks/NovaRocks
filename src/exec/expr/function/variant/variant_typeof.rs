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
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::variant::{VariantValue, variant_typeof};

pub fn eval_variant_typeof(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let variant_array = super::common::eval_variant_arg(arena, args, chunk, "variant_typeof")?;
    let variant_arr = super::common::downcast_variant_arg(&variant_array, "variant_typeof")?;

    let mut builder = StringBuilder::new();
    for row in 0..chunk.len() {
        if variant_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let value = match VariantValue::from_serialized(variant_arr.value(row)) {
            Ok(v) => v,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        match variant_typeof(&value) {
            Ok(v) => builder.append_value(v),
            Err(_) => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::function::variant::test_utils::{slot_id_expr, typed_null};
    use arrow::array::{ArrayRef, LargeBinaryArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_variant_typeof_int64() {
        let variant = super::super::common::variant_primitive_serialized(6, &123_i64.to_le_bytes());
        let variant_arr =
            Arc::new(LargeBinaryArray::from(vec![Some(variant.as_slice())])) as ArrayRef;
        let variant_type = DataType::LargeBinary;
        let field = field_with_slot_id(Field::new("v", variant_type.clone(), true), SlotId::new(1));
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![variant_arr]).unwrap();
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let arg0 = slot_id_expr(&mut arena, 1, variant_type);
        let expr = typed_null(&mut arena, DataType::Utf8);
        let out = eval_variant_typeof(&arena, expr, &[arg0], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "Int64");
    }
}
