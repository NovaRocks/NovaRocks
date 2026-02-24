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
use arrow::array::{Array, ArrayRef, StringArray};

pub fn eval_to_binary(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 && args.len() != 2 {
        return Err("to_binary expects 1 or 2 arguments".to_string());
    }

    let input = arena.eval(args[0], chunk)?;
    let input = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "to_binary expects VARCHAR as first argument".to_string())?;

    let format = if args.len() == 2 {
        Some(
            arena
                .eval(args[1], chunk)?
                .as_any()
                .downcast_ref::<StringArray>()
                .cloned()
                .ok_or_else(|| "to_binary expects VARCHAR format argument".to_string())?,
        )
    } else {
        None
    };

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if input.is_null(row) {
            out.push(None);
            continue;
        }

        let fmt = format
            .as_ref()
            .and_then(|arr| (!arr.is_null(row)).then_some(arr.value(row)));
        let format = super::common::parse_binary_format(fmt);
        let value = input.value(row);

        let bytes = match format {
            super::common::BinaryFormatType::Hex => hex::decode(value).ok(),
            super::common::BinaryFormatType::Encode64 => {
                if value.is_empty() {
                    None
                } else {
                    super::common::decode_base64(value.as_bytes())
                }
            }
            super::common::BinaryFormatType::Utf8 => Some(value.as_bytes().to_vec()),
        };

        out.push(bytes);
    }

    super::common::build_bytes_output_latin1(out, arena.data_type(expr))
}

#[cfg(test)]
mod tests {
    use super::eval_to_binary;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::encryption::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use arrow::array::BinaryArray;
    use arrow::datatypes::DataType;

    #[test]
    fn test_to_binary_hex_default() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Binary);
        let input = literal_string(&mut arena, "4142");

        let out = eval_to_binary(&arena, expr, &[input], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(out.value(0), b"AB");
    }

    #[test]
    fn test_to_binary_utf8_format() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Binary);
        let input = literal_string(&mut arena, "AB");
        let format = literal_string(&mut arena, "utf8");

        let out = eval_to_binary(&arena, expr, &[input, format], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(out.value(0), b"AB");
    }
}
