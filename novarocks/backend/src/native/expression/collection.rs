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

//! Collection literal expression lowering.

use arrow::datatypes::DataType;

use super::{decode_expr_at, lower_expr_list};
use novarocks::exec::expr::function::FunctionKind;
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks::protocol::FieldPath;
use novarocks_protocol::expr;

use super::NativeExpressionInputLayout;

pub(crate) fn lower_array_literal(
    call: &expr::FunctionCall,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    if !matches!(data_type, DataType::List(_)) {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone(),
            format!("ARRAY literal expects List type, got {data_type:?}"),
        ));
    }
    let elements = lower_expr_list(&call.args, path.field("args"), arena, input_layout)?;
    Ok(arena.push_typed(ExprNode::ArrayExpr { elements }, data_type))
}

pub(crate) fn lower_map_constructor(
    call: &expr::FunctionCall,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    if !call.args.len().is_multiple_of(2) {
        return Err(super::NativeExpressionDecodeError::invalid_value(
            path.clone().field("args"),
            format!(
                "MAP constructor expects an even number of arguments, got {}",
                call.args.len()
            ),
        ));
    }

    let DataType::Map(entry_field, _) = &data_type else {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone(),
            format!("MAP constructor expects MAP output type, got {data_type:?}"),
        ));
    };
    let DataType::Struct(entry_fields) = entry_field.data_type() else {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone(),
            "MAP constructor entries type must be Struct",
        ));
    };
    if entry_fields.len() != 2 {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone(),
            format!(
                "MAP constructor entries type must have 2 fields, got {}",
                entry_fields.len()
            ),
        ));
    }

    let expected_key_type = entry_fields[0].data_type().clone();
    let expected_value_type = entry_fields[1].data_type().clone();
    let mut key_elements = Vec::with_capacity(call.args.len() / 2);
    let mut value_elements = Vec::with_capacity(call.args.len() / 2);

    for (idx, arg) in call.args.iter().enumerate() {
        let child = decode_expr_at(
            arg,
            path.clone().field("args").index(idx),
            arena,
            input_layout,
        )?;
        if idx % 2 == 0 {
            key_elements.push(
                coerce_map_constructor_child(arena, child, &expected_key_type, idx, "key")
                    .map_err(|error| {
                        super::NativeExpressionDecodeError::invalid_value(
                            path.clone().field("args").index(idx),
                            error,
                        )
                    })?,
            );
        } else {
            value_elements.push(
                coerce_map_constructor_child(arena, child, &expected_value_type, idx, "value")
                    .map_err(|error| {
                        super::NativeExpressionDecodeError::invalid_value(
                            path.clone().field("args").index(idx),
                            error,
                        )
                    })?,
            );
        }
    }

    let mut args = Vec::with_capacity(call.args.len());
    for (key, value) in key_elements.into_iter().zip(value_elements) {
        args.push(key);
        args.push(value);
    }

    Ok(arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Map("map"),
            args,
        },
        data_type,
    ))
}

fn coerce_map_constructor_child(
    arena: &mut ExprArena,
    child: ExprId,
    expected_type: &DataType,
    arg_idx: usize,
    role: &str,
) -> Result<ExprId, String> {
    let child_type = arena.data_type(child).cloned().ok_or_else(|| {
        format!(
            "MAP constructor missing {role} child type at pair {}",
            arg_idx / 2
        )
    })?;
    if &child_type == expected_type || matches!(expected_type, DataType::Null) {
        return Ok(child);
    }
    Ok(arena.push_typed(ExprNode::Cast(child), expected_type.clone()))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        int_lit, lower, lower_err_with_slots, null_lit, scalar_expr, string_lit,
    };
    use arrow::datatypes::{DataType, Field, Fields};
    use novarocks::exec::expr::{ExprNode, LiteralValue, function::FunctionKind};
    use novarocks_protocol::{common, expr};
    use std::sync::Arc;

    #[test]
    fn lowers_array_literal_internal_function_to_array_expr() {
        let array_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let array = scalar_expr(
            array_type.clone(),
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                function_name: "__array_literal".to_string(),
                args: vec![
                    int_lit(1),
                    int_lit(2),
                    scalar_expr(
                        DataType::Int64,
                        expr::expr::Kind::Literal(expr::LiteralExpr {
                            value: Some(common::LiteralValue {
                                value: Some(common::literal_value::Value::NullValue(true)),
                            }),
                        }),
                    ),
                ],
                distinct: false,
            }),
        );

        let (arena, id) = lower(&array);
        let Some(ExprNode::ArrayExpr { elements }) = arena.node(id) else {
            panic!("expected array literal to lower as ArrayExpr");
        };
        assert_eq!(elements.len(), 3);
        assert_eq!(arena.data_type(id), Some(&array_type));
        assert!(matches!(
            arena.node(elements[0]),
            Some(ExprNode::Literal(LiteralValue::Int64(1)))
        ));
        assert!(matches!(
            arena.node(elements[2]),
            Some(ExprNode::Literal(LiteralValue::Null))
        ));
    }

    #[test]
    fn lowers_variadic_map_constructor_to_literal_call() {
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Int64, true)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ])),
                false,
            )),
            false,
        );
        let map = scalar_expr(
            map_type.clone(),
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                function_name: "map".to_string(),
                args: vec![int_lit(1), string_lit("a"), int_lit(2), string_lit("b")],
                distinct: false,
            }),
        );

        let (arena, id) = lower(&map);
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(id) else {
            panic!("expected map constructor to lower as function call");
        };
        assert_eq!(*kind, FunctionKind::Map("map"));
        assert_eq!(args.len(), 4);
        assert_eq!(arena.data_type(args[0]), Some(&DataType::Int64));
        assert_eq!(arena.data_type(args[1]), Some(&DataType::Utf8));
        assert_eq!(arena.data_type(args[2]), Some(&DataType::Int64));
        assert_eq!(arena.data_type(args[3]), Some(&DataType::Utf8));
        assert_eq!(arena.data_type(id), Some(&map_type));
    }

    #[test]
    fn map_constructor_casts_null_children_to_entry_types() {
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Int64, true)),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ])),
                false,
            )),
            false,
        );
        let map = scalar_expr(
            map_type,
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                function_name: "map".to_string(),
                args: vec![
                    null_lit(DataType::Null),
                    int_lit(10),
                    int_lit(2),
                    null_lit(DataType::Null),
                ],
                distinct: false,
            }),
        );

        let (arena, id) = lower(&map);
        let Some(ExprNode::FunctionCall { args, .. }) = arena.node(id) else {
            panic!("expected map constructor to lower as function call");
        };

        assert_eq!(args.len(), 4);
        assert_eq!(arena.data_type(args[0]), Some(&DataType::Int64));
        assert!(matches!(arena.node(args[0]), Some(ExprNode::Cast(_))));
        assert_eq!(arena.data_type(args[3]), Some(&DataType::Int64));
        assert!(matches!(arena.node(args[3]), Some(ExprNode::Cast(_))));
    }

    #[test]
    fn array_literal_requires_list_result_type() {
        let array = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                function_name: "__array_literal".to_string(),
                args: vec![int_lit(1)],
                distinct: false,
            }),
        );

        let err = lower_err_with_slots(&array, &[]);
        assert!(err.contains("ARRAY literal expects List type"), "{err}");
    }
}
