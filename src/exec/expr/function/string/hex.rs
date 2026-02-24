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
use arrow::array::{Array, ArrayRef, BinaryArray, LargeBinaryArray, StringArray};
use std::sync::Arc;

pub fn eval_hex(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let input = arena.eval(args[0], chunk)?;
    let len = input.len();
    let mut out = Vec::with_capacity(len);
    if let Some(arr) = input.as_any().downcast_ref::<StringArray>() {
        for i in 0..len {
            if arr.is_null(i) {
                out.push(None);
            } else {
                let s = arr.value(i);
                out.push(Some(hex::encode(s.as_bytes()).to_uppercase()));
            }
        }
    } else if let Some(arr) = input.as_any().downcast_ref::<BinaryArray>() {
        for i in 0..len {
            if arr.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(hex::encode(arr.value(i)).to_uppercase()));
            }
        }
    } else if let Some(arr) = input.as_any().downcast_ref::<LargeBinaryArray>() {
        for i in 0..len {
            if arr.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(hex::encode(arr.value(i)).to_uppercase()));
            }
        }
    } else if let Some(arr) = input.as_any().downcast_ref::<arrow::array::Int64Array>() {
        for i in 0..len {
            if arr.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(format!("{:X}", arr.value(i))));
            }
        }
    } else {
        return Err("hex expects string/binary or integer".to_string());
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_hex_logic() {
        assert_string_function_logic("hex");
    }
}
