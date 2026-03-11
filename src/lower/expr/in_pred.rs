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
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::datatypes::DataType;

use crate::exprs;
use crate::lower::type_lowering::{arrow_type_from_desc, arrow_type_from_primitive};
use crate::types;

fn is_temporal_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Timestamp(_, _))
}

fn is_numeric_like_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::FixedSizeBinary(16)
    )
}

/// Lower IN_PRED expression to ExprNode::In.
pub(crate) fn lower_in_pred(
    node: &exprs::TExprNode,
    children: Vec<ExprId>,
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let in_pred = node
        .in_predicate
        .as_ref()
        .ok_or_else(|| "IN_PRED missing in_predicate payload".to_string())?;
    if children.is_empty() {
        return Err("IN_PRED must have at least one child".to_string());
    }
    let mut child = children[0];
    if let Some(dt) = arena.data_type(child) {
        if matches!(dt, DataType::LargeBinary) {
            return Err("VARIANT is not supported in IN predicates".to_string());
        }
    }

    // Align with FE comparison semantics: IN_PRED may carry a compare child type.
    // Cast both lhs and rhs values to that common type before execution.
    let mut compare_type = if let Some(desc) = node.child_type_desc.as_ref() {
        arrow_type_from_desc(desc).ok_or_else(|| {
            format!(
                "IN_PRED unsupported child_type_desc from FE plan: {:?}",
                desc
            )
        })?
    } else if let Some(primitive) = node.child_type {
        if let Some(t) = arrow_type_from_primitive(primitive) {
            t
        } else if matches!(
            primitive,
            types::TPrimitiveType::DECIMAL32
                | types::TPrimitiveType::DECIMAL64
                | types::TPrimitiveType::DECIMAL128
                | types::TPrimitiveType::DECIMAL256
                | types::TPrimitiveType::DECIMAL
                | types::TPrimitiveType::DECIMALV2
        ) {
            arena
                .data_type(child)
                .cloned()
                .ok_or_else(|| "IN_PRED child type missing".to_string())?
        } else {
            return Err(format!(
                "IN_PRED unsupported child_type from FE plan: {:?}",
                primitive
            ));
        }
    } else {
        arena
            .data_type(child)
            .cloned()
            .ok_or_else(|| "IN_PRED child type missing".to_string())?
    };

    if let Some(child_type) = arena.data_type(child) {
        if is_temporal_type(child_type) && is_numeric_like_type(&compare_type) {
            compare_type = child_type.clone();
        }
    }

    if matches!(compare_type, DataType::LargeBinary) {
        return Err("VARIANT is not supported in IN predicates".to_string());
    }

    if arena
        .data_type(child)
        .is_some_and(|child_type| child_type != &compare_type)
    {
        child = arena.push_typed(ExprNode::Cast(child), compare_type.clone());
    }

    let mut values = children[1..].to_vec();
    for value in &mut values {
        let value_expr = *value;
        let value_type_mismatch = arena
            .data_type(value_expr)
            .is_some_and(|value_type| value_type != &compare_type);
        // FE may tag string literals with target compare type in metadata while payload still
        // arrives as UTF-8 text. Keep an explicit cast so eval converts literal payload.
        let utf8_literal_requires_cast = matches!(
            arena.node(value_expr),
            Some(ExprNode::Literal(LiteralValue::Utf8(_)))
        ) && !matches!(compare_type, DataType::Utf8);
        if value_type_mismatch || utf8_literal_requires_cast {
            *value = arena.push_typed(ExprNode::Cast(*value), compare_type.clone());
        }
    }
    Ok(arena.push_typed(
        ExprNode::In {
            child,
            values,
            is_not_in: in_pred.is_not_in,
        },
        data_type,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;

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

    #[test]
    fn lower_in_pred_prefers_temporal_lhs_over_numeric_compare_type() {
        let mut arena = ExprArena::default();
        let ts_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), ts_type.clone());
        let value = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let node = exprs::TExprNode {
            node_type: exprs::TExprNodeType::IN_PRED,
            num_children: 2,
            in_predicate: Some(exprs::TInPredicate { is_not_in: false }),
            child_type: Some(types::TPrimitiveType::BIGINT),
            ..default_t_expr_node()
        };

        let lowered =
            lower_in_pred(&node, vec![child, value], &mut arena, DataType::Boolean).unwrap();

        let ExprNode::In { child, values, .. } = arena.node(lowered).unwrap() else {
            panic!("expected ExprNode::In");
        };
        assert_eq!(arena.data_type(*child), Some(&ts_type));
        assert_eq!(values.len(), 1);
        assert_eq!(arena.data_type(values[0]), Some(&ts_type));
    }
}
