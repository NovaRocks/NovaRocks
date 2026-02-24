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
use std::sync::Arc;

/// Evaluate upper function.
/// Converts a string to uppercase.
///
/// Supports:
/// - upper(string): returns string (uppercase version)
///
/// Implementation aligns with StarRocks BE:
/// - For ASCII strings: fast path using bit manipulation
/// - For UTF-8 strings: uses ICU library for proper case conversion
/// - In Rust: uses String::to_uppercase() which handles UTF-8 correctly
pub fn eval_upper(
    arena: &ExprArena,
    value_expr: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let value_arr = arena.eval(value_expr, chunk)?;

    // Downcast to StringArray
    let str_arr = value_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "upper: argument must be a string array".to_string())?;

    let len = str_arr.len();
    let result_values: Vec<Option<String>> = (0..len)
        .map(|i| {
            if str_arr.is_null(i) {
                None
            } else {
                let s = str_arr.value(i);
                // Use Rust's to_uppercase() which properly handles UTF-8
                // This aligns with StarRocks BE's UTF8StringCaseToggleFunction
                Some(s.to_uppercase())
            }
        })
        .collect();

    let result_array = StringArray::from_iter(result_values);
    Ok(Arc::new(result_array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn create_test_chunk_string(values: Vec<String>) -> Chunk {
        let array = Arc::new(StringArray::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("col0", DataType::Utf8, false),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Chunk::new(batch)
    }

    #[test]
    fn test_upper_lowercase() {
        let mut arena = ExprArena::default();
        let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8("hello".to_string())));
        let upper = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Upper,
                args: vec![lit],
            },
            DataType::Utf8,
        );

        let chunk = create_test_chunk_string(vec!["test".to_string()]);

        let result = arena.eval(upper, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_arr.value(0), "HELLO");
    }

    #[test]
    fn test_upper_mixed_case() {
        let mut arena = ExprArena::default();
        let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8(
            "HeLLo WoRLd".to_string(),
        )));
        let upper = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Upper,
                args: vec![lit],
            },
            DataType::Utf8,
        );

        let chunk = create_test_chunk_string(vec!["test".to_string()]);

        let result = arena.eval(upper, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_arr.value(0), "HELLO WORLD");
    }

    #[test]
    fn test_upper_already_uppercase() {
        let mut arena = ExprArena::default();
        let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8("HELLO".to_string())));
        let upper = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Upper,
                args: vec![lit],
            },
            DataType::Utf8,
        );

        let chunk = create_test_chunk_string(vec!["test".to_string()]);

        let result = arena.eval(upper, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_arr.value(0), "HELLO");
    }

    #[test]
    fn test_upper_empty_string() {
        let mut arena = ExprArena::default();
        let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8("".to_string())));
        let upper = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Upper,
                args: vec![lit],
            },
            DataType::Utf8,
        );

        let chunk = create_test_chunk_string(vec!["test".to_string()]);

        let result = arena.eval(upper, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_arr.value(0), "");
    }

    #[test]
    fn test_upper_utf8() {
        // Test UTF-8 characters (e.g., German umlauts, Turkish i)
        let mut arena = ExprArena::default();
        let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8("café".to_string())));
        let upper = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Upper,
                args: vec![lit],
            },
            DataType::Utf8,
        );

        let chunk = create_test_chunk_string(vec!["test".to_string()]);

        let result = arena.eval(upper, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_arr.value(0), "CAFÉ");
    }

    #[test]
    fn test_upper_with_numbers() {
        let mut arena = ExprArena::default();
        let lit = arena.push(ExprNode::Literal(LiteralValue::Utf8(
            "hello123".to_string(),
        )));
        let upper = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Upper,
                args: vec![lit],
            },
            DataType::Utf8,
        );

        let chunk = create_test_chunk_string(vec!["test".to_string()]);

        let result = arena.eval(upper, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_arr.value(0), "HELLO123");
    }
}
