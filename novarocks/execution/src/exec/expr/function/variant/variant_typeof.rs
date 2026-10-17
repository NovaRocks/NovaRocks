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
use novarocks_type_contract::ValueLogicalType;
use parquet::variant::json_to_variant;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use novarocks_types::value::variant::{VariantValue, variant_typeof};

pub fn eval_variant_typeof<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let variant_array = super::common::eval_variant_arg(arena, args, chunk, "variant_typeof")?;
    if let Some(declared) = arena.value_type(args[0]) {
        match declared.logical_type() {
            ValueLogicalType::Json => {
                if variant_array.data_type() != &arrow::datatypes::DataType::Utf8 {
                    return Err("variant_typeof Json input requires Utf8".into());
                }
                let mut builder = StringBuilder::new();
                for row in 0..chunk.len() {
                    match json_typeof(&variant_array, row)? {
                        Some(kind) => builder.append_value(kind),
                        None => builder.append_null(),
                    }
                }
                return Ok(Arc::new(builder.finish()) as ArrayRef);
            }
            ValueLogicalType::Variant => {}
            _ => return Err("variant_typeof requires a declared Json or Variant input".into()),
        }
    }
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

/// Parse each JSON row with the same converter used by variant_get. A damaged
/// row retains TYPEOF's NULL policy without suppressing an argument's typed
/// evaluation failure or changing the labels of the surrounding valid rows.
fn json_typeof(input: &ArrayRef, row: usize) -> Result<Option<String>, String> {
    if input.is_null(row) {
        return Ok(None);
    }
    let parsed = match json_to_variant(&input.slice(row, 1)) {
        Ok(parsed) => parsed,
        Err(arrow::error::ArrowError::InvalidArgumentError(_)) => return Ok(None),
        Err(error) => return Err(format!("variant_typeof JSON conversion failed: {error}")),
    };
    let payload = parsed
        .value_field()
        .ok_or("variant_typeof JSON converter omitted its value field")?;
    let metadata =
        super::common::binary_value_at_any(parsed.metadata_field(), 0, "variant_typeof")?
            .ok_or("variant_typeof JSON converter returned NULL metadata for a present row")?;
    let payload = super::common::binary_value_at_any(payload, 0, "variant_typeof")?
        .ok_or("variant_typeof JSON converter returned NULL value for a present row")?;
    Ok(VariantValue::create(metadata, payload)
        .ok()
        .and_then(|value| variant_typeof(&value).ok()))
}

#[cfg(test)]
#[path = "variant_typeof_tests.rs"]
mod tests;
