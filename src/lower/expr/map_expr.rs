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
use crate::exec::expr::{ExprArena, ExprId, ExprNode, function::FunctionKind};
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;

/// Lower MAP_EXPR to internal map function call.
///
/// StarRocks MAP_EXPR children are interleaved as: key1, value1, key2, value2, ...
/// novarocks map function expects exactly two ARRAY arguments: keys and values.
pub(crate) fn lower_map_expr(
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    if children.len() % 2 != 0 {
        return Err(format!(
            "MAP_EXPR expects an even number of children, got {}",
            children.len()
        ));
    }

    let DataType::Map(entry_field, _) = &data_type else {
        return Err(format!(
            "MAP_EXPR expects MAP output type, got {:?}",
            data_type
        ));
    };
    let DataType::Struct(entry_fields) = entry_field.data_type() else {
        return Err("MAP_EXPR map entries type must be Struct".to_string());
    };
    if entry_fields.len() != 2 {
        return Err("MAP_EXPR map entries type must have 2 fields".to_string());
    }

    let expected_key_type = entry_fields[0].data_type().clone();
    let expected_value_type = entry_fields[1].data_type().clone();

    let mut key_elements = Vec::with_capacity(children.len() / 2);
    let mut value_elements = Vec::with_capacity(children.len() / 2);

    for (idx, child) in children.iter().enumerate() {
        let child_type = arena
            .data_type(*child)
            .ok_or_else(|| format!("MAP_EXPR missing child type at index {}", idx))?;
        if idx % 2 == 0 {
            if child_type != &expected_key_type {
                return Err(format!(
                    "MAP_EXPR key type mismatch at pair {}: expected {:?}, got {:?}",
                    idx / 2,
                    expected_key_type,
                    child_type
                ));
            }
            key_elements.push(*child);
        } else {
            if child_type != &expected_value_type {
                return Err(format!(
                    "MAP_EXPR value type mismatch at pair {}: expected {:?}, got {:?}",
                    idx / 2,
                    expected_value_type,
                    child_type
                ));
            }
            value_elements.push(*child);
        }
    }

    let keys_array = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: key_elements,
        },
        DataType::List(Arc::new(Field::new("item", expected_key_type, true))),
    );
    let values_array = arena.push_typed(
        ExprNode::ArrayExpr {
            elements: value_elements,
        },
        DataType::List(Arc::new(Field::new("item", expected_value_type, true))),
    );

    Ok(arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Map("map"),
            args: vec![keys_array, values_array],
        },
        data_type,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::datatypes::Fields;

    fn map_i32_i64_type() -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ])),
                false,
            )),
            false,
        )
    }

    #[test]
    fn lower_map_expr_builds_map_function_call() {
        let mut arena = ExprArena::default();
        let k1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(1)), DataType::Int32);
        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
        let k2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(2)), DataType::Int32);
        let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);

        let map_expr =
            lower_map_expr(&[k1, v1, k2, v2], &mut arena, map_i32_i64_type()).expect("lower");

        let ExprNode::FunctionCall { kind, args } = arena.node(map_expr).expect("node") else {
            panic!("MAP_EXPR should lower to FunctionCall");
        };
        assert_eq!(*kind, FunctionKind::Map("map"));
        assert_eq!(args.len(), 2);

        let ExprNode::ArrayExpr {
            elements: key_elements,
        } = arena.node(args[0]).expect("keys")
        else {
            panic!("MAP_EXPR first arg should be ArrayExpr");
        };
        assert_eq!(key_elements, &vec![k1, k2]);

        let ExprNode::ArrayExpr {
            elements: value_elements,
        } = arena.node(args[1]).expect("values")
        else {
            panic!("MAP_EXPR second arg should be ArrayExpr");
        };
        assert_eq!(value_elements, &vec![v1, v2]);
    }

    #[test]
    fn lower_map_expr_allows_empty_map() {
        let mut arena = ExprArena::default();
        let map_expr = lower_map_expr(&[], &mut arena, map_i32_i64_type()).expect("lower");

        let ExprNode::FunctionCall { kind, args } = arena.node(map_expr).expect("node") else {
            panic!("MAP_EXPR should lower to FunctionCall");
        };
        assert_eq!(*kind, FunctionKind::Map("map"));
        assert_eq!(args.len(), 2);

        let ExprNode::ArrayExpr {
            elements: key_elements,
        } = arena.node(args[0]).expect("keys")
        else {
            panic!("MAP_EXPR first arg should be ArrayExpr");
        };
        assert!(key_elements.is_empty());

        let ExprNode::ArrayExpr {
            elements: value_elements,
        } = arena.node(args[1]).expect("values")
        else {
            panic!("MAP_EXPR second arg should be ArrayExpr");
        };
        assert!(value_elements.is_empty());
    }

    #[test]
    fn lower_map_expr_rejects_odd_children() {
        let mut arena = ExprArena::default();
        let k1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(1)), DataType::Int32);
        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
        let k2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(2)), DataType::Int32);

        let err =
            lower_map_expr(&[k1, v1, k2], &mut arena, map_i32_i64_type()).expect_err("must fail");
        assert!(err.contains("even number of children"));
    }

    #[test]
    fn lower_map_expr_rejects_type_mismatch() {
        let mut arena = ExprArena::default();
        let k1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(1)), DataType::Int32);
        let bad_v = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(10)), DataType::Int32);

        let err =
            lower_map_expr(&[k1, bad_v], &mut arena, map_i32_i64_type()).expect_err("must fail");
        assert!(err.contains("value type mismatch"));
    }
}
