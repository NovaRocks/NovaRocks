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
use crate::connector::MinMaxPredicate;
use crate::lower::compat::layout::Layout;
use crate::thrift::exprs;

/// Parse a min/max conjunct TExpr into MinMaxPredicates used for pruning.
pub(crate) fn parse_min_max_conjuncts(
    expr: &exprs::TExpr,
    layout: &Layout,
) -> Result<Vec<MinMaxPredicate>, String> {
    parse_min_max_conjuncts_with_column_resolver(expr, |slot_ref| {
        get_column_name_from_slot(slot_ref, layout)
    })
}

pub(crate) use super::min_max_parser::parse_min_max_conjuncts_with_column_resolver;

fn get_column_name_from_slot(
    slot_ref: &exprs::TSlotRef,
    layout: &Layout,
) -> Result<String, String> {
    let key = (slot_ref.tuple_id, slot_ref.slot_id);
    let idx = layout
        .index
        .get(&key)
        .ok_or_else(|| format!("slot not found in layout: {:?}", key))?;

    Ok(idx.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{MinMaxPredicate, MinMaxPredicateValue};
    use crate::lower::compat::type_lowering::THRIFT_TIME_UNIT_NANOS;
    use crate::sql::codegen::type_infer::arrow_type_to_type_desc;
    use crate::thrift::types;
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    fn datetime_literal_node(time_unit: Option<i32>, value: &str) -> exprs::TExprNode {
        exprs::TExprNode {
            node_type: exprs::TExprNodeType::STRING_LITERAL,
            type_: types::TTypeDesc {
                types: Some(vec![types::TTypeNode {
                    type_: types::TTypeNodeType::SCALAR,
                    scalar_type: Some(types::TScalarType {
                        type_: types::TPrimitiveType::DATETIME,
                        len: None,
                        precision: None,
                        scale: None,
                        time_unit,
                    }),
                    struct_fields: None,
                    is_named: None,
                }]),
            },
            string_literal: Some(exprs::TStringLiteral {
                value: value.to_string(),
            }),
            ..default_t_expr_node()
        }
    }

    fn datetime_literal_expr(time_unit: Option<i32>, value: &str) -> exprs::TExpr {
        exprs::TExpr {
            nodes: vec![
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    opcode: Some(crate::thrift::opcodes::TExprOpcode::EQ),
                    num_children: 2,
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::SLOT_REF,
                    slot_ref: Some(exprs::TSlotRef {
                        slot_id: 1,
                        tuple_id: 1,
                    }),
                    ..default_t_expr_node()
                },
                datetime_literal_node(time_unit, value),
            ],
        }
    }

    #[test]
    fn string_literal_on_nanosecond_column_produces_datetime_nanos() {
        let expr = datetime_literal_expr(
            Some(THRIFT_TIME_UNIT_NANOS),
            "2024-01-02 03:04:05.123456789",
        );
        let expected_nanos = chrono::NaiveDateTime::parse_from_str(
            "2024-01-02 03:04:05.123456789",
            "%Y-%m-%d %H:%M:%S%.f",
        )
        .unwrap()
        .and_utc()
        .timestamp_nanos_opt()
        .unwrap();
        let parsed = parse_min_max_conjuncts(&expr, &single_slot_layout()).expect("parse");
        assert_eq!(
            parsed,
            vec![MinMaxPredicate::Eq {
                column: "0".to_string(),
                value: MinMaxPredicateValue::DateTimeNanos(expected_nanos),
            }]
        );
    }

    #[test]
    fn string_literal_on_microsecond_column_still_micros() {
        let expr = datetime_literal_expr(None, "2024-01-02 03:04:05.123456");
        let parsed = parse_min_max_conjuncts(&expr, &single_slot_layout()).expect("parse");
        assert!(matches!(
            parsed.as_slice(),
            [MinMaxPredicate::Eq {
                value: MinMaxPredicateValue::DateTimeMicros(_),
                ..
            }]
        ));
    }

    fn create_dummy_type() -> types::TTypeDesc {
        types::TTypeDesc {
            types: Some(vec![types::TTypeNode {
                type_: types::TTypeNodeType::SCALAR,
                scalar_type: None,
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn default_t_expr_node() -> exprs::TExprNode {
        exprs::TExprNode {
            node_type: exprs::TExprNodeType::INT_LITERAL,
            type_: create_dummy_type(),
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: None,
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: 0,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }
    }

    fn single_slot_layout() -> Layout {
        Layout {
            order: vec![(1, 1)],
            index: HashMap::from([((1, 1), 0usize)]),
        }
    }

    fn type_desc(data_type: &DataType) -> types::TTypeDesc {
        arrow_type_to_type_desc(data_type).expect("type desc")
    }

    #[test]
    fn parse_min_max_conjunct_skips_non_literal_rhs() {
        let expr = exprs::TExpr {
            nodes: vec![
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    opcode: Some(crate::thrift::opcodes::TExprOpcode::EQ),
                    num_children: 2,
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::SLOT_REF,
                    slot_ref: Some(exprs::TSlotRef {
                        slot_id: 1,
                        tuple_id: 1,
                    }),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::CAST_EXPR,
                    num_children: 1,
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::INT_LITERAL,
                    int_literal: Some(exprs::TIntLiteral { value: 7 }),
                    ..default_t_expr_node()
                },
            ],
        };

        let parsed = parse_min_max_conjuncts(&expr, &single_slot_layout()).expect("parse");
        assert!(
            parsed.is_empty(),
            "non-literal rhs should not produce min/max pruning"
        );
    }

    #[test]
    fn parse_min_max_conjunct_keeps_scalar_literal_rhs() {
        let expr = exprs::TExpr {
            nodes: vec![
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    opcode: Some(crate::thrift::opcodes::TExprOpcode::EQ),
                    num_children: 2,
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::SLOT_REF,
                    slot_ref: Some(exprs::TSlotRef {
                        slot_id: 1,
                        tuple_id: 1,
                    }),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::INT_LITERAL,
                    int_literal: Some(exprs::TIntLiteral { value: 7 }),
                    ..default_t_expr_node()
                },
            ],
        };

        let parsed = parse_min_max_conjuncts(&expr, &single_slot_layout()).expect("parse");
        assert_eq!(
            parsed,
            vec![MinMaxPredicate::Eq {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int64(7),
            }]
        );
    }

    #[test]
    fn parse_min_max_conjunct_casts_numeric_literal_for_varchar_slot() {
        let expr = exprs::TExpr {
            nodes: vec![
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    opcode: Some(crate::thrift::opcodes::TExprOpcode::EQ),
                    num_children: 2,
                    child_type_desc: Some(type_desc(&DataType::Utf8)),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::SLOT_REF,
                    type_: type_desc(&DataType::Utf8),
                    slot_ref: Some(exprs::TSlotRef {
                        slot_id: 1,
                        tuple_id: 1,
                    }),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::INT_LITERAL,
                    type_: type_desc(&DataType::Int64),
                    int_literal: Some(exprs::TIntLiteral { value: 1 }),
                    ..default_t_expr_node()
                },
            ],
        };

        let parsed = parse_min_max_conjuncts(&expr, &single_slot_layout()).expect("parse");
        assert_eq!(
            parsed,
            vec![MinMaxPredicate::Eq {
                column: "0".to_string(),
                value: MinMaxPredicateValue::ByteArray(b"1".to_vec()),
            }]
        );
    }

    #[test]
    fn parse_min_max_conjunct_skips_when_compare_type_differs_from_slot_type() {
        let expr = exprs::TExpr {
            nodes: vec![
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    opcode: Some(crate::thrift::opcodes::TExprOpcode::EQ),
                    num_children: 2,
                    child_type_desc: Some(type_desc(&DataType::Utf8)),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::SLOT_REF,
                    type_: type_desc(&DataType::Int64),
                    slot_ref: Some(exprs::TSlotRef {
                        slot_id: 1,
                        tuple_id: 1,
                    }),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::STRING_LITERAL,
                    type_: type_desc(&DataType::Utf8),
                    string_literal: Some(exprs::TStringLiteral {
                        value: "1".to_string(),
                    }),
                    ..default_t_expr_node()
                },
            ],
        };

        let parsed = parse_min_max_conjuncts(&expr, &single_slot_layout()).expect("parse");
        assert!(
            parsed.is_empty(),
            "string comparison semantics are unsafe for numeric min/max pruning"
        );
    }

    #[test]
    fn parse_min_max_conjuncts_expands_compound_and() {
        let expr = exprs::TExpr {
            nodes: vec![
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::COMPOUND_PRED,
                    opcode: Some(crate::thrift::opcodes::TExprOpcode::COMPOUND_AND),
                    num_children: 2,
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    opcode: Some(crate::thrift::opcodes::TExprOpcode::GE),
                    num_children: 2,
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::SLOT_REF,
                    slot_ref: Some(exprs::TSlotRef {
                        slot_id: 1,
                        tuple_id: 1,
                    }),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::INT_LITERAL,
                    int_literal: Some(exprs::TIntLiteral { value: 1 }),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    opcode: Some(crate::thrift::opcodes::TExprOpcode::LE),
                    num_children: 2,
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::SLOT_REF,
                    slot_ref: Some(exprs::TSlotRef {
                        slot_id: 1,
                        tuple_id: 1,
                    }),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::INT_LITERAL,
                    int_literal: Some(exprs::TIntLiteral { value: 3 }),
                    ..default_t_expr_node()
                },
            ],
        };

        let parsed = parse_min_max_conjuncts(&expr, &single_slot_layout()).expect("parse");
        assert_eq!(
            parsed,
            vec![
                MinMaxPredicate::Ge {
                    column: "0".to_string(),
                    value: MinMaxPredicateValue::Int64(1),
                },
                MinMaxPredicate::Le {
                    column: "0".to_string(),
                    value: MinMaxPredicateValue::Int64(3),
                },
            ]
        );
    }
}
