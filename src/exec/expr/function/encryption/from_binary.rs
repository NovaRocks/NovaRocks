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
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

pub fn eval_from_binary(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 && args.len() != 2 {
        return Err("from_binary expects 1 or 2 arguments".to_string());
    }

    let input = super::common::to_owned_bytes_array(arena.eval(args[0], chunk)?, "from_binary", 0)?;

    let format = if args.len() == 2 {
        Some(super::common::to_owned_bytes_array(
            arena.eval(args[1], chunk)?,
            "from_binary",
            1,
        )?)
    } else {
        None
    };

    let mut formats = Vec::with_capacity(chunk.len());
    let mut utf8_only = true;
    for row in 0..chunk.len() {
        if input.is_null(row) {
            formats.push(None);
            continue;
        }

        let fmt = format.as_ref().and_then(|arr| {
            if arr.is_null(row) {
                None
            } else {
                Some(String::from_utf8_lossy(arr.bytes(row)).to_string())
            }
        });
        let parsed = super::common::parse_binary_format(fmt.as_deref());
        utf8_only &= matches!(parsed, super::common::BinaryFormatType::Utf8);
        formats.push(Some(parsed));
    }

    if utf8_only {
        let mut bytes_out = Vec::with_capacity(chunk.len());
        let mut all_utf8_valid = true;
        for row in 0..chunk.len() {
            if input.is_null(row) {
                bytes_out.push(None);
                continue;
            }
            let value = input.bytes(row).to_vec();
            if std::str::from_utf8(&value).is_err() {
                all_utf8_valid = false;
            }
            bytes_out.push(Some(value));
        }

        if all_utf8_valid {
            let out: Vec<Option<String>> = bytes_out
                .into_iter()
                .map(|v| {
                    v.map(|bytes| {
                        std::str::from_utf8(&bytes)
                            .expect("validated utf8 bytes")
                            .to_string()
                    })
                })
                .collect();
            return Ok(Arc::new(StringArray::from(out)) as ArrayRef);
        }

        return super::common::build_bytes_output_latin1(bytes_out, Some(&DataType::Binary));
    }

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if input.is_null(row) {
            out.push(None);
            continue;
        }

        let bytes = input.bytes(row);
        let value = match formats[row].expect("non-null input row must have a parsed format") {
            super::common::BinaryFormatType::Hex => Some(hex::encode_upper(bytes)),
            super::common::BinaryFormatType::Encode64 => {
                if bytes.is_empty() {
                    None
                } else {
                    Some(super::common::encode_base64(bytes))
                }
            }
            super::common::BinaryFormatType::Utf8 => {
                Some(String::from_utf8_lossy(bytes).to_string())
            }
        };

        out.push(value);
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::eval_from_binary;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use crate::{
        common::ids::SlotId,
        exec::chunk::{Chunk, field_with_slot_id},
    };
    use arrow::{
        array::{ArrayRef, BinaryArray, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    fn binary_chunk(values: Vec<Option<&[u8]>>) -> Chunk {
        let array = Arc::new(BinaryArray::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("b", DataType::Binary, true),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Chunk::new(batch)
    }

    #[test]
    fn test_from_binary_hex_default() {
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(
            ExprNode::Literal(crate::exec::expr::LiteralValue::Null),
            DataType::Utf8,
        );
        let arg = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Binary);

        let out = eval_from_binary(&arena, expr, &[arg], &binary_chunk(vec![Some(b"AB")])).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "4142");
    }

    #[test]
    fn test_from_binary_utf8_valid_bytes_returns_string() {
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Utf8);
        let arg = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Binary);
        let fmt = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("utf8".to_string())),
            DataType::Utf8,
        );

        let out =
            eval_from_binary(&arena, expr, &[arg, fmt], &binary_chunk(vec![Some(b"AB")])).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "AB");
    }

    #[test]
    fn test_from_binary_utf8_invalid_bytes_returns_binary() {
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Utf8);
        let arg = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Binary);
        let fmt = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("utf8".to_string())),
            DataType::Utf8,
        );

        let out = eval_from_binary(
            &arena,
            expr,
            &[arg, fmt],
            &binary_chunk(vec![Some(&[0xAB, 0x01])]),
        )
        .unwrap();
        let out = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(out.value(0), &[0xAB, 0x01]);
    }
}
