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
use arrow::datatypes::DataType;

use crate::exprs;
use crate::opcodes;

/// Lower ARITHMETIC_EXPR expression to arithmetic ExprNode.
pub(crate) fn lower_arithmetic(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let opcode = node
        .opcode
        .ok_or_else(|| "ARITHMETIC_EXPR missing opcode".to_string())?;

    let mut lower_bit_function = |name: &'static str, arity: usize| -> Result<ExprId, String> {
        if children.len() != arity {
            return Err(format!(
                "ARITHMETIC_EXPR {:?} expected {} children, got {}",
                opcode,
                arity,
                children.len()
            ));
        }
        Ok(arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Bit(name),
                args: children.to_vec(),
            },
            data_type.clone(),
        ))
    };

    match opcode {
        o if o == opcodes::TExprOpcode::BITNOT => return lower_bit_function("bitnot", 1),
        o if o == opcodes::TExprOpcode::BITAND => return lower_bit_function("bitand", 2),
        o if o == opcodes::TExprOpcode::BITOR => return lower_bit_function("bitor", 2),
        o if o == opcodes::TExprOpcode::BITXOR => return lower_bit_function("bitxor", 2),
        o if o == opcodes::TExprOpcode::BIT_SHIFT_LEFT => {
            return lower_bit_function("bit_shift_left", 2);
        }
        o if o == opcodes::TExprOpcode::BIT_SHIFT_RIGHT => {
            return lower_bit_function("bit_shift_right", 2);
        }
        o if o == opcodes::TExprOpcode::BIT_SHIFT_RIGHT_LOGICAL => {
            return lower_bit_function("bit_shift_right_logical", 2);
        }
        _ => {}
    }

    if children.len() != 2 {
        return Err(format!(
            "ARITHMETIC_EXPR expected 2 children, got {}",
            children.len()
        ));
    }
    let left = children[0];
    let right = children[1];
    let id = match opcode {
        o if o == opcodes::TExprOpcode::ADD => {
            arena.push_typed(ExprNode::Add(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::SUBTRACT => {
            arena.push_typed(ExprNode::Sub(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::MULTIPLY => {
            arena.push_typed(ExprNode::Mul(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::DIVIDE => {
            arena.push_typed(ExprNode::Div(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::INT_DIVIDE => {
            arena.push_typed(ExprNode::Div(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::MOD => {
            arena.push_typed(ExprNode::Mod(left, right), data_type.clone())
        }
        _ => return Err(format!("unsupported ARITHMETIC_EXPR opcode: {:?}", opcode)),
    };
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, LiteralValue};
    use crate::exprs::{TExpr, TExprNode, TExprNodeType, TIntLiteral};
    use crate::lower::expr::lower_t_expr;
    use crate::lower::layout::Layout;
    use crate::opcodes::TExprOpcode;
    use crate::types::{TTypeDesc, TTypeNode, TTypeNodeType};
    use arrow::array::{
        Array, ArrayRef, Float64Array, Int64Array, RecordBatch, RecordBatchOptions,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_dummy_type() -> TTypeDesc {
        TTypeDesc {
            types: Some(vec![TTypeNode {
                type_: TTypeNodeType::SCALAR,
                scalar_type: None,
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn default_t_expr_node() -> TExprNode {
        TExprNode {
            node_type: TExprNodeType::INT_LITERAL, // Default dummy
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

    fn empty_chunk_with_rows(row_count: usize) -> Chunk {
        let schema = Arc::new(Schema::empty());
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let batch = RecordBatch::try_new_with_options(schema, vec![], &options).expect("batch");
        Chunk::new(batch)
    }

    fn eval_scalar_array(arena: &ExprArena, id: ExprId, chunk: &Chunk) -> ArrayRef {
        let array = arena.eval(id, chunk).expect("eval");
        assert_eq!(array.len(), 1, "expected scalar array");
        array
    }

    #[test]
    fn test_arithmetic_ops() {
        let cases = vec![
            (TExprOpcode::ADD, 3),
            (TExprOpcode::SUBTRACT, -1),
            (TExprOpcode::MULTIPLY, 2),
            (TExprOpcode::DIVIDE, 0), // 1/2 in int64 is 0
            (TExprOpcode::MOD, 1),
        ];

        for (opcode, expected) in cases {
            let node = TExprNode {
                node_type: TExprNodeType::ARITHMETIC_EXPR,
                type_: create_dummy_type(),
                num_children: 2,
                opcode: Some(opcode),
                output_scale: 0,
                ..default_t_expr_node()
            };

            let left = TExprNode {
                node_type: TExprNodeType::INT_LITERAL,
                type_: create_dummy_type(),
                num_children: 0,
                int_literal: Some(TIntLiteral { value: 1 }),
                output_scale: 0,
                ..default_t_expr_node()
            };

            let right = TExprNode {
                node_type: TExprNodeType::INT_LITERAL,
                type_: create_dummy_type(),
                num_children: 0,
                int_literal: Some(TIntLiteral { value: 2 }),
                output_scale: 0,
                ..default_t_expr_node()
            };

            let expr = TExpr {
                nodes: vec![node, left, right],
            };

            let mut arena = ExprArena::default();
            let layout = Layout {
                index: HashMap::new(),
                order: vec![],
            };

            let result = lower_t_expr(&expr, &mut arena, &layout, None, None);
            assert!(result.is_ok(), "failed to lower op {:?}", opcode);
            let id = result.unwrap();
            let chunk = empty_chunk_with_rows(1);
            let array = eval_scalar_array(&arena, id, &chunk);
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            assert!(!arr.is_null(0));
            assert_eq!(arr.value(0), expected, "op {:?} failed", opcode);
        }
    }

    #[test]
    fn test_arithmetic_ops_slots() {
        // Test c0 + c1
        let node = TExprNode {
            node_type: TExprNodeType::ARITHMETIC_EXPR,
            type_: create_dummy_type(),
            num_children: 2,
            opcode: Some(TExprOpcode::ADD),
            output_scale: 0,
            ..default_t_expr_node()
        };

        let left = TExprNode {
            node_type: TExprNodeType::SLOT_REF,
            type_: create_dummy_type(),
            num_children: 0,
            slot_ref: Some(crate::exprs::TSlotRef {
                slot_id: 0,
                tuple_id: 0,
            }),
            output_scale: 0,
            ..default_t_expr_node()
        };

        let right = TExprNode {
            node_type: TExprNodeType::SLOT_REF,
            type_: create_dummy_type(),
            num_children: 0,
            slot_ref: Some(crate::exprs::TSlotRef {
                slot_id: 1,
                tuple_id: 0,
            }),
            output_scale: 0,
            ..default_t_expr_node()
        };

        let expr = TExpr {
            nodes: vec![node, left, right],
        };

        let mut arena = ExprArena::default();
        let mut index = HashMap::new();
        index.insert((0, 0), 0);
        index.insert((0, 1), 1);
        let layout = Layout {
            index,
            order: vec![],
        };

        let result = lower_t_expr(&expr, &mut arena, &layout, None, None);
        assert!(result.is_ok(), "failed to lower add expr");
        let id = result.unwrap();

        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("c0", DataType::Int64, true), SlotId::new(0)),
            field_with_slot_id(Field::new("c1", DataType::Int64, true), SlotId::new(1)),
        ]));
        let col0 = Arc::new(Int64Array::from(vec![Some(10)]));
        let col1 = Arc::new(Int64Array::from(vec![Some(20)]));
        let batch = RecordBatch::try_new(schema, vec![col0, col1]).expect("batch");
        let chunk = Chunk::new(batch);
        let array = eval_scalar_array(&arena, id, &chunk);
        let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), 30);
    }

    #[test]
    fn test_arithmetic_ops_float() {
        use crate::exprs::TFloatLiteral;
        use thrift::OrderedFloat;

        let cases = vec![
            (TExprOpcode::ADD, 3.5),
            (TExprOpcode::SUBTRACT, -0.5),
            (TExprOpcode::MULTIPLY, 3.0),
            (TExprOpcode::DIVIDE, 0.75),
        ];

        for (opcode, expected) in cases {
            let node = TExprNode {
                node_type: TExprNodeType::ARITHMETIC_EXPR,
                type_: create_dummy_type(),
                num_children: 2,
                opcode: Some(opcode),
                output_scale: 0,
                ..default_t_expr_node()
            };

            let left = TExprNode {
                node_type: TExprNodeType::FLOAT_LITERAL,
                type_: create_dummy_type(),
                num_children: 0,
                float_literal: Some(TFloatLiteral {
                    value: OrderedFloat(1.5),
                }),
                output_scale: 0,
                ..default_t_expr_node()
            };

            let right = TExprNode {
                node_type: TExprNodeType::FLOAT_LITERAL,
                type_: create_dummy_type(),
                num_children: 0,
                float_literal: Some(TFloatLiteral {
                    value: OrderedFloat(2.0),
                }),
                output_scale: 0,
                ..default_t_expr_node()
            };

            let expr = TExpr {
                nodes: vec![node, left, right],
            };

            let mut arena = ExprArena::default();
            let layout = Layout {
                index: HashMap::new(),
                order: vec![],
            };

            let result = lower_t_expr(&expr, &mut arena, &layout, None, None);
            assert!(result.is_ok(), "failed to lower float op {:?}", opcode);
            let id = result.unwrap();
            let chunk = empty_chunk_with_rows(1);
            let array = eval_scalar_array(&arena, id, &chunk);
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!(!arr.is_null(0));
            let v = arr.value(0);
            assert!(
                (v - expected).abs() < 1e-10,
                "op {:?} failed: got {}, expected {}",
                opcode,
                v,
                expected
            );
        }
    }

    #[test]
    fn test_arithmetic_mixed_int_float() {
        use crate::exprs::TFloatLiteral;
        use thrift::OrderedFloat;

        // Test: 10 (int) + 2.5 (float) = 12.5 (float)
        let node = TExprNode {
            node_type: TExprNodeType::ARITHMETIC_EXPR,
            type_: create_dummy_type(),
            num_children: 2,
            opcode: Some(TExprOpcode::ADD),
            output_scale: 0,
            ..default_t_expr_node()
        };

        let left = TExprNode {
            node_type: TExprNodeType::INT_LITERAL,
            type_: create_dummy_type(),
            num_children: 0,
            int_literal: Some(TIntLiteral { value: 10 }),
            output_scale: 0,
            ..default_t_expr_node()
        };

        let right = TExprNode {
            node_type: TExprNodeType::FLOAT_LITERAL,
            type_: create_dummy_type(),
            num_children: 0,
            float_literal: Some(TFloatLiteral {
                value: OrderedFloat(2.5),
            }),
            output_scale: 0,
            ..default_t_expr_node()
        };

        let expr = TExpr {
            nodes: vec![node, left, right],
        };

        let mut arena = ExprArena::default();
        let layout = Layout {
            index: HashMap::new(),
            order: vec![],
        };

        let result = lower_t_expr(&expr, &mut arena, &layout, None, None);
        assert!(result.is_ok(), "failed to lower mixed int+float expr");
        let id = result.unwrap();
        let chunk = empty_chunk_with_rows(1);
        let array = eval_scalar_array(&arena, id, &chunk);
        let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(!arr.is_null(0));
        let v = arr.value(0);
        assert!((v - 12.5).abs() < 1e-10, "expected 12.5, got {}", v);
    }

    #[test]
    fn test_lower_arithmetic_bitnot_unary_opcode() {
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let node = TExprNode {
            node_type: TExprNodeType::ARITHMETIC_EXPR,
            type_: create_dummy_type(),
            num_children: 1,
            opcode: Some(TExprOpcode::BITNOT),
            output_scale: 0,
            ..default_t_expr_node()
        };

        let id = lower_arithmetic(&node, &[child], &mut arena, DataType::Int64).unwrap();
        let chunk = empty_chunk_with_rows(1);
        let array = eval_scalar_array(&arena, id, &chunk);
        let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), !1_i64);
    }
}
