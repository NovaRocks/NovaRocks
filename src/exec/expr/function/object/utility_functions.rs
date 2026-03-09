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
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Int8Array, Int16Array, Int32Array, Int64Array,
};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

pub fn eval_sleep(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let mut builder = BooleanBuilder::with_capacity(input.len());

    macro_rules! eval_sleep_array {
        ($arr:expr, $context:expr) => {{
            for row in 0..$arr.len() {
                if $arr.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let seconds = $arr.value(row).max(0) as u64;
                std::thread::sleep(Duration::from_secs(seconds));
                builder.append_value(true);
            }
            return Ok(Arc::new(builder.finish()) as ArrayRef);
        }};
    }

    if let Some(arr) = input.as_any().downcast_ref::<Int8Array>() {
        eval_sleep_array!(arr, "Int8");
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int16Array>() {
        eval_sleep_array!(arr, "Int16");
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int32Array>() {
        eval_sleep_array!(arr, "Int32");
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int64Array>() {
        eval_sleep_array!(arr, "Int64");
    }

    Err(format!(
        "sleep expects integer input, got {:?}",
        input.data_type()
    ))
}
