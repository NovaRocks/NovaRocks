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
