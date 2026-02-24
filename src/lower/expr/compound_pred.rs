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
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::datatypes::DataType;

use crate::exprs;
use crate::opcodes;

/// Lower COMPOUND_PRED expression to logical ExprNode.
pub(crate) fn lower_compound_pred(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let opcode = node
        .opcode
        .ok_or_else(|| "COMPOUND_PRED missing opcode".to_string())?;
    let id = match opcode {
        o if o == opcodes::TExprOpcode::COMPOUND_NOT => {
            if children.len() != 1 {
                return Err(format!(
                    "COMPOUND_NOT expected 1 child, got {}",
                    children.len()
                ));
            }
            arena.push_typed(ExprNode::Not(children[0]), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::COMPOUND_AND => {
            if children.len() != 2 {
                return Err(format!(
                    "COMPOUND_AND expected 2 children, got {}",
                    children.len()
                ));
            }
            arena.push_typed(ExprNode::And(children[0], children[1]), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::COMPOUND_OR => {
            if children.len() != 2 {
                return Err(format!(
                    "COMPOUND_OR expected 2 children, got {}",
                    children.len()
                ));
            }
            arena.push_typed(ExprNode::Or(children[0], children[1]), data_type.clone())
        }
        _ => return Err(format!("unsupported COMPOUND_PRED opcode: {:?}", opcode)),
    };
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::ExprArena;
    use crate::exprs::{TExpr, TExprNode, TExprNodeType};
    use crate::lower::expr::lower_t_expr;
    use crate::lower::layout::Layout;
    use crate::opcodes::TExprOpcode;
    use crate::types::{TTypeDesc, TTypeNode, TTypeNodeType};
    use arrow::array::{
        Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchOptions,
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

    #[allow(dead_code)]
    fn empty_chunk_with_rows(row_count: usize) -> Chunk {
        let schema = Arc::new(Schema::empty());
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let batch = RecordBatch::try_new_with_options(schema, vec![], &options).expect("batch");
        Chunk::new(batch)
    }

    #[allow(dead_code)]
    fn eval_scalar_array(arena: &ExprArena, id: ExprId, chunk: &Chunk) -> ArrayRef {
        let array = arena.eval(id, chunk).expect("eval");
        assert_eq!(array.len(), 1, "expected scalar array");
        array
    }

    #[test]
    fn test_compound_not_slot() {
        let node = TExprNode {
            node_type: TExprNodeType::COMPOUND_PRED,
            type_: create_dummy_type(),
            num_children: 1,
            opcode: Some(TExprOpcode::COMPOUND_NOT),
            output_scale: 0,
            ..default_t_expr_node()
        };

        let child = TExprNode {
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

        let expr = TExpr {
            nodes: vec![node, child],
        };

        let mut arena = ExprArena::default();
        let mut index = HashMap::new();
        index.insert((0, 0), 0);
        let layout = Layout {
            index,
            order: vec![],
        };

        let id = lower_t_expr(&expr, &mut arena, &layout, None, None).expect("lower");

        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("c0", DataType::Int64, true),
            SlotId::new(0),
        )]));
        let col0 = Arc::new(Int64Array::from(vec![Some(0), Some(7), None, Some(-2)]));
        let batch = RecordBatch::try_new(schema, vec![col0]).expect("batch");
        let chunk = Chunk::new(batch);

        let array = arena.eval(id, &chunk).expect("eval");
        let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(arr.len(), 4);
        assert_eq!(arr.value(0), true);
        assert_eq!(arr.value(1), false);
        assert!(arr.is_null(2));
        assert_eq!(arr.value(3), false);
    }
}
