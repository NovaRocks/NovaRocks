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
mod literals;
mod min_max;

// Node type handlers
mod arithmetic;
mod array_expr;
mod binary_pred;
mod case;
mod cast;
mod clone;
mod collection_element;
mod compound_pred;
mod function_call;
mod in_pred;
mod info_func;
mod is_null_pred;
mod lambda;
mod like_pred;
mod literal;
mod map_expr;
mod slot;
mod subfield;

use crate::common::ids::SlotId;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::datatypes::DataType;
use std::collections::{BTreeMap, HashMap};

use crate::exprs;
use crate::lower::layout::Layout;
use crate::lower::type_lowering::{
    arrow_type_from_desc, arrow_type_from_primitive, primitive_type_from_desc,
};
use crate::types;

// Import node handlers
use arithmetic::lower_arithmetic;
use array_expr::lower_array_expr;
use binary_pred::lower_binary_pred;
use case::lower_case;
use cast::lower_cast;
use clone::lower_clone;
use collection_element::{lower_array_element_expr, lower_map_element_expr};
use compound_pred::lower_compound_pred;
use function_call::lower_function_call;
use in_pred::lower_in_pred;
use info_func::lower_info_func;
use is_null_pred::lower_is_null_pred;
use lambda::lower_lambda_function;
use like_pred::lower_like_pred;
use literal::lower_literal;
use map_expr::lower_map_expr;
use slot::lower_slot_ref;
use subfield::lower_subfield_expr;

pub(crate) use min_max::parse_min_max_conjunct;

struct CommonSlotLoweringCtx<'a> {
    /// StarRocks FE may factor out repeated sub-expressions into a `common_slot_map`, and then
    /// reference them from predicates / join conjuncts via SlotRef(<slot_id>).
    ///
    /// Those "slots" are not materialized columns coming from upstream chunks, so we must inline
    /// (lower) the mapped expression instead of producing ExprNode::SlotId(slot_id).
    map: Option<&'a BTreeMap<types::TSlotId, exprs::TExpr>>,
    cache: HashMap<types::TSlotId, ExprId>,
    stack: Vec<types::TSlotId>,
    // PLACEHOLDER_EXPR replacement used by DICT_EXPR lowering.
    placeholder_overrides: HashMap<types::TSlotId, ExprId>,
}

fn is_integer_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn is_string_like_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

fn resolve_node_data_type(node: &exprs::TExprNode) -> Option<DataType> {
    if let Some(data_type) = arrow_type_from_desc(&node.type_) {
        return Some(data_type);
    }
    if matches!(
        node.node_type,
        exprs::TExprNodeType::FUNCTION_CALL
            | exprs::TExprNodeType::COMPUTE_FUNCTION_CALL
            | exprs::TExprNodeType::AGG_EXPR
    ) {
        return node
            .fn_
            .as_ref()
            .and_then(|f| arrow_type_from_desc(&f.ret_type));
    }
    if matches!(
        node.node_type,
        exprs::TExprNodeType::FUNCTION_CALL | exprs::TExprNodeType::COMPUTE_FUNCTION_CALL
    ) {
        if let Some(output_type) = node.output_type.as_ref() {
            if let Some(data_type) = arrow_type_from_primitive(output_type.type_) {
                return Some(data_type);
            }
        }
    }
    None
}

fn missing_type_descriptor_err(node: &exprs::TExprNode) -> String {
    format!(
        "unsupported or missing expr type descriptor for node_type={:?}",
        node.node_type
    )
}

fn merge_prefer_non_null_type(
    left: Option<&DataType>,
    right: Option<&DataType>,
) -> Option<DataType> {
    match (left, right) {
        (Some(l), Some(r)) if l == r => Some(l.clone()),
        (Some(l), Some(r)) if matches!(l, DataType::Null) => Some(r.clone()),
        (Some(l), Some(r)) if matches!(r, DataType::Null) => Some(l.clone()),
        (Some(l), Some(_)) => Some(l.clone()),
        (Some(l), None) => Some(l.clone()),
        (None, Some(r)) => Some(r.clone()),
        (None, None) => None,
    }
}

fn infer_function_return_type_from_children(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &ExprArena,
) -> Option<DataType> {
    if !matches!(
        node.node_type,
        exprs::TExprNodeType::FUNCTION_CALL | exprs::TExprNodeType::COMPUTE_FUNCTION_CALL
    ) {
        return None;
    }
    let fn_name = node.fn_.as_ref()?.name.function_name.to_ascii_lowercase();
    match fn_name.as_str() {
        // FE sometimes omits decimal precision/scale in IF return type descriptor.
        "if" if children.len() >= 3 => {
            merge_prefer_non_null_type(arena.data_type(children[1]), arena.data_type(children[2]))
        }
        "ifnull" | "nvl" if children.len() >= 2 => {
            merge_prefer_non_null_type(arena.data_type(children[0]), arena.data_type(children[1]))
        }
        "coalesce" if !children.is_empty() => {
            for child in children {
                let data_type = arena.data_type(*child)?;
                if !matches!(data_type, DataType::Null) {
                    return Some(data_type.clone());
                }
            }
            Some(DataType::Null)
        }
        _ => {
            let mut candidate: Option<DataType> = None;
            for child in children {
                let dt = arena.data_type(*child)?;
                if matches!(dt, DataType::Null) {
                    continue;
                }
                match &candidate {
                    None => candidate = Some(dt.clone()),
                    Some(existing) if existing == dt => {}
                    Some(_) => return None,
                }
            }
            candidate
        }
    }
}

fn lower_dict_expr_node(
    node: &exprs::TExprNode,
    nodes: &[exprs::TExprNode],
    idx: &mut usize,
    arena: &mut ExprArena,
    input_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
    ctx: &mut CommonSlotLoweringCtx<'_>,
) -> Result<ExprId, String> {
    if node.num_children <= 0 {
        return Err("DICT_EXPR expects at least one child".to_string());
    }
    let dict_child =
        lower_expr_node_impl(nodes, idx, arena, input_layout, last_query_id, fe_addr, ctx)?;

    let mut inserted_override = None;
    let dict_slot_id = match arena.node(dict_child) {
        Some(ExprNode::SlotId(slot_id)) => Some(*slot_id),
        _ => None,
    };
    if let Some(slot_id) = dict_slot_id {
        if let Some(dict) = arena.query_global_dict(slot_id).cloned() {
            let decoded_expr = arena.push_typed(
                ExprNode::DictDecode {
                    child: dict_child,
                    dict,
                },
                DataType::Utf8,
            );
            let raw_slot_id = i32::try_from(slot_id.as_u32())
                .map_err(|_| format!("slot id {} overflows i32", slot_id))?;
            ctx.placeholder_overrides.insert(raw_slot_id, decoded_expr);
            inserted_override = Some(raw_slot_id);
        }
    }
    let mut mapped_children = Vec::with_capacity((node.num_children - 1) as usize);
    for _ in 1..node.num_children {
        mapped_children.push(lower_expr_node_impl(
            nodes,
            idx,
            arena,
            input_layout,
            last_query_id,
            fe_addr,
            ctx,
        )?);
    }
    if let Some(raw_slot_id) = inserted_override {
        ctx.placeholder_overrides.remove(&raw_slot_id);
    }

    let Some(&mapped_expr) = mapped_children.first() else {
        return Ok(dict_child);
    };
    let output_type = match resolve_node_data_type(node) {
        Some(data_type) => data_type,
        None if cfg!(test) => DataType::Null,
        None => return Err(missing_type_descriptor_err(node)),
    };
    let mapped_type = arena
        .data_type(mapped_expr)
        .cloned()
        .unwrap_or(DataType::Null);

    // DictDefine path: keep encoded dictionary ids and let decode node materialize final strings.
    if is_integer_type(&output_type) && is_string_like_type(&mapped_type) {
        return Ok(dict_child);
    }
    Ok(mapped_expr)
}

/// Lower a Thrift TExpr to ExprId in the arena.
pub(crate) fn lower_t_expr(
    expr: &exprs::TExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<ExprId, String> {
    lower_t_expr_with_common_slot_map(expr, arena, input_layout, last_query_id, fe_addr, None)
}

pub(crate) fn lower_t_expr_with_common_slot_map(
    expr: &exprs::TExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
    common_slot_map: Option<&BTreeMap<types::TSlotId, exprs::TExpr>>,
) -> Result<ExprId, String> {
    let mut idx = 0usize;
    let mut ctx = CommonSlotLoweringCtx {
        map: common_slot_map,
        cache: HashMap::new(),
        stack: Vec::new(),
        placeholder_overrides: HashMap::new(),
    };
    lower_expr_node_impl(
        &expr.nodes,
        &mut idx,
        arena,
        input_layout,
        last_query_id,
        fe_addr,
        &mut ctx,
    )
}

/// Lower a single TExprNode to ExprId in the arena.
/// This function dispatches to appropriate handlers based on node type.
pub(crate) fn lower_expr_node(
    nodes: &[exprs::TExprNode],
    idx: &mut usize,
    arena: &mut ExprArena,
    input_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<ExprId, String> {
    let mut ctx = CommonSlotLoweringCtx {
        map: None,
        cache: HashMap::new(),
        stack: Vec::new(),
        placeholder_overrides: HashMap::new(),
    };
    lower_expr_node_impl(
        nodes,
        idx,
        arena,
        input_layout,
        last_query_id,
        fe_addr,
        &mut ctx,
    )
}

fn lower_expr_node_impl(
    nodes: &[exprs::TExprNode],
    idx: &mut usize,
    arena: &mut ExprArena,
    input_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
    ctx: &mut CommonSlotLoweringCtx<'_>,
) -> Result<ExprId, String> {
    let node = nodes
        .get(*idx)
        .ok_or_else(|| "invalid expr node index".to_string())?;
    *idx += 1;
    if node.node_type == exprs::TExprNodeType::DICT_EXPR {
        return lower_dict_expr_node(
            node,
            nodes,
            idx,
            arena,
            input_layout,
            last_query_id,
            fe_addr,
            ctx,
        );
    }
    let mut children = Vec::with_capacity(node.num_children as usize);
    for _ in 0..node.num_children {
        children.push(lower_expr_node_impl(
            nodes,
            idx,
            arena,
            input_layout,
            last_query_id,
            fe_addr,
            ctx,
        )?);
    }

    let data_type = match resolve_node_data_type(node)
        .or_else(|| infer_function_return_type_from_children(node, &children, arena))
    {
        Some(data_type) => data_type,
        None if cfg!(test) => {
            // Unit tests in this module intentionally use minimal dummy thrift type descriptors.
            // Production lowering must not guess a fallback type.
            DataType::Null
        }
        None => return Err(missing_type_descriptor_err(node)),
    };
    let id = match node.node_type {
        // Literal expressions
        t if t == exprs::TExprNodeType::INT_LITERAL
            || t == exprs::TExprNodeType::DECIMAL_LITERAL
            || t == exprs::TExprNodeType::STRING_LITERAL
            || t == exprs::TExprNodeType::BINARY_LITERAL
            || t == exprs::TExprNodeType::DATE_LITERAL
            || t == exprs::TExprNodeType::BOOL_LITERAL
            || t == exprs::TExprNodeType::NULL_LITERAL
            || t == exprs::TExprNodeType::FLOAT_LITERAL
            || t == exprs::TExprNodeType::LARGE_INT_LITERAL =>
        {
            lower_literal(node, arena, data_type)?
        }
        // Slot reference
        t if t == exprs::TExprNodeType::SLOT_REF => {
            if let Some(slot) = node.slot_ref.as_ref() {
                if let Some(map) = ctx.map {
                    if let Some(common_expr) = map.get(&slot.slot_id) {
                        if let Some(cached) = ctx.cache.get(&slot.slot_id) {
                            return Ok(*cached);
                        }
                        if ctx.stack.contains(&slot.slot_id) {
                            return Err(format!(
                                "common_slot_map contains a cycle at slot_id={}",
                                slot.slot_id
                            ));
                        }
                        ctx.stack.push(slot.slot_id);
                        let mut sub_idx = 0usize;
                        let lowered = lower_expr_node_impl(
                            &common_expr.nodes,
                            &mut sub_idx,
                            arena,
                            input_layout,
                            last_query_id,
                            fe_addr,
                            ctx,
                        )?;
                        ctx.stack.pop();
                        ctx.cache.insert(slot.slot_id, lowered);
                        return Ok(lowered);
                    }
                }
            }
            lower_slot_ref(node, arena, input_layout, data_type)?
        }
        // Placeholder reference used by FE global-dict rewritten expressions.
        // It behaves like a slot reference with slot id carried in `vslot_ref`.
        t if t == exprs::TExprNodeType::PLACEHOLDER_EXPR => {
            let slot_id = node
                .vslot_ref
                .as_ref()
                .and_then(|v| v.slot_id)
                .ok_or_else(|| "PLACEHOLDER_EXPR missing vslot_ref.slot_id".to_string())?;
            if let Some(rewritten_expr) = ctx.placeholder_overrides.get(&slot_id) {
                return Ok(*rewritten_expr);
            }
            let slot_id = SlotId::try_from(slot_id)?;
            arena.push_typed(crate::exec::expr::ExprNode::SlotId(slot_id), data_type)
        }
        // Cast expression
        t if t == exprs::TExprNodeType::CAST_EXPR => {
            let target_primitive = primitive_type_from_desc(&node.type_);
            lower_cast(
                &children,
                arena,
                data_type,
                target_primitive,
                node.child_type,
            )?
        }
        // Arithmetic expression
        t if t == exprs::TExprNodeType::ARITHMETIC_EXPR => {
            lower_arithmetic(node, &children, arena, data_type)?
        }
        // Binary predicate
        t if t == exprs::TExprNodeType::BINARY_PRED => {
            lower_binary_pred(node, &children, arena, data_type)?
        }
        // Compound predicate
        t if t == exprs::TExprNodeType::COMPOUND_PRED => {
            lower_compound_pred(node, &children, arena, data_type)?
        }
        // Is null predicate
        t if t == exprs::TExprNodeType::IS_NULL_PRED => {
            lower_is_null_pred(node, &children, arena, data_type)?
        }
        // Like predicate
        t if t == exprs::TExprNodeType::LIKE_PRED => {
            lower_like_pred(node, &children, arena, data_type)?
        }
        // Info function
        t if t == exprs::TExprNodeType::INFO_FUNC => lower_info_func(node, arena, data_type)?,
        // Function call / legacy compute function call
        t if t == exprs::TExprNodeType::FUNCTION_CALL
            || t == exprs::TExprNodeType::COMPUTE_FUNCTION_CALL =>
        {
            lower_function_call(node, &children, arena, data_type, last_query_id, fe_addr)?
        }
        // Array expression
        t if t == exprs::TExprNodeType::ARRAY_EXPR => lower_array_expr(children, arena, data_type)?,
        // Map expression
        t if t == exprs::TExprNodeType::MAP_EXPR => lower_map_expr(&children, arena, data_type)?,
        // Lambda function expression
        t if t == exprs::TExprNodeType::LAMBDA_FUNCTION_EXPR => {
            lower_lambda_function(node, &children, arena, data_type)?
        }
        // Case expression
        t if t == exprs::TExprNodeType::CASE_EXPR => lower_case(node, children, arena, data_type)?,
        // In predicate
        t if t == exprs::TExprNodeType::IN_PRED => lower_in_pred(node, children, arena, data_type)?,
        // Array element expression
        t if t == exprs::TExprNodeType::ARRAY_ELEMENT_EXPR => {
            lower_array_element_expr(node, &children, arena, data_type)?
        }
        // Map element expression
        t if t == exprs::TExprNodeType::MAP_ELEMENT_EXPR => {
            lower_map_element_expr(node, &children, arena, data_type)?
        }
        // Struct/JSON subfield expression
        t if t == exprs::TExprNodeType::SUBFIELD_EXPR => {
            lower_subfield_expr(node, &children, arena, data_type)?
        }
        // Clone expression
        t if t == exprs::TExprNodeType::CLONE_EXPR => lower_clone(&children, arena, data_type)?,
        t => {
            return Err(format!("unsupported expr node type: {:?}", t));
        }
    };
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exprs::{TExpr, TExprNode, TExprNodeType, TSlotRef};
    use crate::opcodes::TExprOpcode;
    use crate::types::{TTypeDesc, TTypeNode, TTypeNodeType};
    use arrow::array::BooleanArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
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

    #[test]
    fn common_slot_map_inlines_slot_refs() {
        // SlotRef(100) is a "common sub expr" (100 := SlotRef(1) AND SlotRef(2)).
        // Without inlining, evaluating SlotRef(100) would fail because the upstream chunk doesn't
        // contain column slot_id=100.
        let expr = TExpr {
            nodes: vec![TExprNode {
                node_type: TExprNodeType::SLOT_REF,
                type_: create_dummy_type(),
                num_children: 0,
                slot_ref: Some(TSlotRef {
                    slot_id: 100,
                    tuple_id: 0,
                }),
                ..default_t_expr_node()
            }],
        };

        let common_expr = TExpr {
            nodes: vec![
                TExprNode {
                    node_type: TExprNodeType::COMPOUND_PRED,
                    type_: create_dummy_type(),
                    num_children: 2,
                    opcode: Some(TExprOpcode::COMPOUND_AND),
                    ..default_t_expr_node()
                },
                TExprNode {
                    node_type: TExprNodeType::SLOT_REF,
                    type_: create_dummy_type(),
                    num_children: 0,
                    slot_ref: Some(TSlotRef {
                        slot_id: 1,
                        tuple_id: 0,
                    }),
                    ..default_t_expr_node()
                },
                TExprNode {
                    node_type: TExprNodeType::SLOT_REF,
                    type_: create_dummy_type(),
                    num_children: 0,
                    slot_ref: Some(TSlotRef {
                        slot_id: 2,
                        tuple_id: 0,
                    }),
                    ..default_t_expr_node()
                },
            ],
        };

        let mut common_slot_map = BTreeMap::new();
        common_slot_map.insert(100, common_expr);

        let mut arena = ExprArena::default();
        let layout = Layout {
            order: Vec::new(),
            index: HashMap::new(),
        };
        let lowered = lower_t_expr_with_common_slot_map(
            &expr,
            &mut arena,
            &layout,
            None,
            None,
            Some(&common_slot_map),
        )
        .expect("lower");
        assert!(
            !matches!(arena.node(lowered), Some(ExprNode::SlotId(_))),
            "expected SlotRef(100) to inline to its common expr"
        );

        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("c1", DataType::Boolean, true), SlotId::new(1)),
            field_with_slot_id(Field::new("c2", DataType::Boolean, true), SlotId::new(2)),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(BooleanArray::from(vec![Some(false)])),
            ],
        )
        .unwrap();
        let chunk = Chunk::new(batch);

        let out = arena.eval(lowered, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(out.value(0), false);
    }
}
