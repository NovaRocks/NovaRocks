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
use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

pub fn eval_struct_expr(
    arena: &ExprArena,
    id: ExprId,
    fields: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let output_type = arena
        .data_type(id)
        .cloned()
        .ok_or_else(|| "struct_expr missing output type".to_string())?;
    let struct_fields = match output_type {
        DataType::Struct(fields) => fields,
        other => {
            return Err(format!(
                "struct_expr output type must be Struct, got {:?}",
                other
            ));
        }
    };
    if struct_fields.len() != fields.len() {
        return Err(format!(
            "struct_expr field count mismatch: expected {}, got {}",
            struct_fields.len(),
            fields.len()
        ));
    }

    let num_rows = chunk.len();
    let mut arrays = Vec::with_capacity(fields.len());
    for (idx, expr_id) in fields.iter().enumerate() {
        let array = arena.eval(*expr_id, chunk)?;
        if array.len() != num_rows {
            return Err(format!(
                "struct_expr field length mismatch at {}: expected {}, got {}",
                idx,
                num_rows,
                array.len()
            ));
        }
        let expected_type = struct_fields
            .get(idx)
            .map(|f| f.data_type())
            .ok_or_else(|| "struct_expr field missing".to_string())?;
        if array.data_type() != expected_type {
            return Err(format!(
                "struct_expr field type mismatch at {}: expected {:?}, got {:?}",
                idx,
                expected_type,
                array.data_type()
            ));
        }
        arrays.push(array);
    }

    let array = StructArray::new(struct_fields, arrays, None);
    Ok(Arc::new(array))
}
