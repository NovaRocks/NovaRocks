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
use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::join::{JoinDistributionMode, JoinNode, JoinRuntimeFilterSpec, JoinType};
use crate::exec::node::{ExecNode, ExecNodeKind};

use crate::lower::expr::lower_t_expr;
use crate::lower::layout::{Layout, schema_for_layout};
use crate::lower::node::Lowered;
use crate::novarocks_logging::warn;

use crate::{descriptors, plan_nodes, runtime_filter, types};

fn common_decimal_compare_type(left: &DataType, right: &DataType) -> Result<DataType, String> {
    let (lp, ls, left_is_256) = match left {
        DataType::Decimal128(p, s) => (*p, *s, false),
        DataType::Decimal256(p, s) => (*p, *s, true),
        _ => {
            return Err(format!(
                "HASH_JOIN decimal key type requires decimal children (left={:?}, right={:?})",
                left, right
            ));
        }
    };
    let (rp, rs, right_is_256) = match right {
        DataType::Decimal128(p, s) => (*p, *s, false),
        DataType::Decimal256(p, s) => (*p, *s, true),
        _ => {
            return Err(format!(
                "HASH_JOIN decimal key type requires decimal children (left={:?}, right={:?})",
                left, right
            ));
        }
    };

    let target_scale: i8 = ls.max(rs);
    let lhs_int_digits: i16 = (lp as i16) - (ls as i16);
    let rhs_int_digits: i16 = (rp as i16) - (rs as i16);
    let int_digits: i16 = lhs_int_digits.max(rhs_int_digits).max(0);
    let target_precision: i16 = int_digits + (target_scale as i16);
    if target_precision <= 0 {
        return Err(format!(
            "HASH_JOIN invalid decimal key precision (left={:?}, right={:?})",
            left, right
        ));
    }
    let target_precision_u8 = target_precision as u8;
    let need_decimal256 = left_is_256 || right_is_256 || target_precision > 38;
    if need_decimal256 {
        if target_precision > 76 {
            return Err(format!(
                "HASH_JOIN decimal key precision overflow (left={:?}, right={:?}, target=Decimal256({}, {}))",
                left, right, target_precision, target_scale
            ));
        }
        return Ok(DataType::Decimal256(target_precision_u8, target_scale));
    }
    Ok(DataType::Decimal128(target_precision_u8, target_scale))
}

fn common_join_key_type(left: &DataType, right: &DataType) -> Result<Option<DataType>, String> {
    if left == right {
        return Ok(Some(left.clone()));
    }
    match (left, right) {
        (
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
        ) => Ok(Some(common_decimal_compare_type(left, right)?)),
        (DataType::List(left_field), DataType::List(right_field)) => {
            let Some(elem_type) =
                common_join_key_type(left_field.data_type(), right_field.data_type())?
            else {
                return Ok(None);
            };
            Ok(Some(DataType::List(Arc::new(Field::new(
                left_field.name(),
                elem_type,
                left_field.is_nullable() || right_field.is_nullable(),
            )))))
        }
        _ => Ok(None),
    }
}

/// Lower a HASH_JOIN_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_hash_join_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    arena: &mut ExprArena,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    if children.len() != 2 {
        return Err(format!(
            "HASH_JOIN_NODE expected 2 children, got {}",
            children.len()
        ));
    }

    let mut it = children.into_iter();
    let left = it.next().expect("left");
    let right = it.next().expect("right");
    let Some(join) = node.hash_join_node.as_ref() else {
        return Err("HASH_JOIN_NODE missing hash_join_node payload".to_string());
    };

    let join_type = match join.join_op {
        plan_nodes::TJoinOp::INNER_JOIN => JoinType::Inner,
        plan_nodes::TJoinOp::LEFT_OUTER_JOIN => JoinType::LeftOuter,
        plan_nodes::TJoinOp::RIGHT_OUTER_JOIN => JoinType::RightOuter,
        plan_nodes::TJoinOp::FULL_OUTER_JOIN => JoinType::FullOuter,
        plan_nodes::TJoinOp::LEFT_SEMI_JOIN => JoinType::LeftSemi,
        plan_nodes::TJoinOp::RIGHT_SEMI_JOIN => JoinType::RightSemi,
        plan_nodes::TJoinOp::LEFT_ANTI_JOIN => JoinType::LeftAnti,
        plan_nodes::TJoinOp::RIGHT_ANTI_JOIN => JoinType::RightAnti,
        plan_nodes::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN => JoinType::NullAwareLeftAnti,
        other => {
            return Err(format!(
                "unsupported HASH_JOIN_NODE join_op={other:?} (supported: INNER/LEFT_OUTER/RIGHT_OUTER/FULL_OUTER/LEFT_SEMI/RIGHT_SEMI/LEFT_ANTI/RIGHT_ANTI/NULL_AWARE_LEFT_ANTI)"
            ));
        }
    };
    let is_skew_join = join.is_skew_join.unwrap_or(false);

    if join.eq_join_conjuncts.is_empty() {
        return Err("HASH_JOIN_NODE requires non-empty eq_join_conjuncts".to_string());
    }

    // Lower residual join predicates (FE: other_join_conjuncts) on the joined output layout
    // (left columns then right columns).
    let mut residual_predicate: Option<crate::exec::expr::ExprId> = None;
    let distribution_mode = match join.distribution_mode {
        Some(plan_nodes::TJoinDistributionMode::BROADCAST)
        | Some(plan_nodes::TJoinDistributionMode::REPLICATED) => JoinDistributionMode::Broadcast,
        _ => JoinDistributionMode::Partitioned,
    };

    let mut probe_keys = Vec::with_capacity(join.eq_join_conjuncts.len());
    let mut build_keys = Vec::with_capacity(join.eq_join_conjuncts.len());
    let mut eq_null_safe = Vec::with_capacity(join.eq_join_conjuncts.len());
    for cond in &join.eq_join_conjuncts {
        let null_safe = match cond.opcode {
            Some(op) if op == crate::opcodes::TExprOpcode::EQ_FOR_NULL => true,
            Some(op) if op == crate::opcodes::TExprOpcode::EQ => false,
            None => false,
            Some(other) => {
                return Err(format!(
                    "unsupported HASH_JOIN_NODE eq_join_conjunct opcode={other:?} (expected EQ or EQ_FOR_NULL)"
                ));
            }
        };
        eq_null_safe.push(null_safe);
        probe_keys.push(lower_t_expr(
            &cond.left,
            arena,
            &left.layout,
            last_query_id,
            fe_addr,
        )?);
        build_keys.push(lower_t_expr(
            &cond.right,
            arena,
            &right.layout,
            last_query_id,
            fe_addr,
        )?);
    }
    for idx in 0..probe_keys.len() {
        let probe_expr = *probe_keys
            .get(idx)
            .ok_or_else(|| "HASH_JOIN probe key missing".to_string())?;
        let build_expr = *build_keys
            .get(idx)
            .ok_or_else(|| "HASH_JOIN build key missing".to_string())?;
        let probe_type = arena
            .data_type(probe_expr)
            .ok_or_else(|| "HASH_JOIN probe key type missing".to_string())?
            .clone();
        let build_type = arena
            .data_type(build_expr)
            .ok_or_else(|| "HASH_JOIN build key type missing".to_string())?
            .clone();
        if probe_type == build_type {
            continue;
        }

        let common_type = common_join_key_type(&probe_type, &build_type)?;
        match common_type {
            Some(target_type) => {
                if probe_type != target_type {
                    let casted = arena.push_typed(ExprNode::Cast(probe_expr), target_type.clone());
                    probe_keys[idx] = casted;
                }
                if build_type != target_type {
                    let casted = arena.push_typed(ExprNode::Cast(build_expr), target_type);
                    build_keys[idx] = casted;
                }
            }
            None => {
                let casted = arena.push_typed(ExprNode::Cast(build_expr), probe_type);
                build_keys[idx] = casted;
            }
        }
    }
    for key in probe_keys.iter().chain(build_keys.iter()) {
        if let Some(dt) = arena.data_type(*key) {
            if matches!(dt, DataType::LargeBinary) {
                return Err("VARIANT is not supported in HASH_JOIN keys".to_string());
            }
        }
    }

    // Join outputs concatenated rows (left then right) in child output order.
    // Use a layout that matches that physical row layout so SLOT_REF resolution stays correct.
    let mut order = Vec::with_capacity(left.layout.order.len() + right.layout.order.len());
    order.extend_from_slice(&left.layout.order);
    order.extend_from_slice(&right.layout.order);
    let index = order.iter().enumerate().map(|(i, key)| (*key, i)).collect();
    let layout = Layout { order, index };

    if let Some(other) = join.other_join_conjuncts.as_ref().filter(|v| !v.is_empty()) {
        let mut lowered = Vec::with_capacity(other.len());
        for e in other {
            lowered.push(lower_t_expr(e, arena, &layout, last_query_id, fe_addr)?);
        }
        let mut it = lowered.into_iter();
        let Some(first) = it.next() else {
            return Err("HASH_JOIN_NODE other_join_conjuncts is empty".to_string());
        };
        let mut acc = first;
        for next in it {
            acc = arena.push_typed(ExprNode::And(acc, next), DataType::Boolean);
        }
        residual_predicate = Some(acc);
    }

    let mut runtime_filters = Vec::new();
    if is_skew_join {
        if join
            .build_runtime_filters
            .as_ref()
            .is_some_and(|filters| !filters.is_empty())
        {
            warn!(
                "skip runtime filters for skew hash join: node_id={}",
                node.node_id
            );
        }
    } else if let Some(filters) = join
        .build_runtime_filters
        .as_ref()
        .filter(|v| !v.is_empty())
    {
        if matches!(
            join_type,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        ) {
            if join.eq_join_conjuncts.is_empty() {
                return Err("HASH_JOIN_NODE runtime filters require eq_join_conjuncts".to_string());
            }
            for desc in filters {
                let filter_id = desc
                    .filter_id
                    .ok_or_else(|| "runtime filter missing filter_id".to_string())?;
                let expr_order = desc
                    .expr_order
                    .ok_or_else(|| format!("runtime filter {} missing expr_order", filter_id))?
                    as usize;
                if expr_order >= join.eq_join_conjuncts.len() {
                    return Err(format!(
                        "runtime filter {} expr_order {} out of range (eq_join_conjuncts={})",
                        filter_id,
                        expr_order,
                        join.eq_join_conjuncts.len()
                    ));
                }
                if eq_null_safe.get(expr_order).copied().unwrap_or(false) {
                    // Null-safe equality (`<=>`) must preserve NULL-key matches.
                    // Runtime filters currently prune NULL probe rows, so skip building
                    // runtime filters on null-safe join keys.
                    continue;
                }
                if desc.filter_type != Some(runtime_filter::TRuntimeFilterBuildType::JOIN_FILTER) {
                    return Err(format!(
                        "runtime filter {} has unsupported filter_type {:?}",
                        filter_id, desc.filter_type
                    ));
                }
                let build_key = build_keys
                    .get(expr_order)
                    .ok_or_else(|| "runtime filter build key missing".to_string())?;
                let probe_key = probe_keys
                    .get(expr_order)
                    .ok_or_else(|| "runtime filter probe key missing".to_string())?;
                let build_type = arena
                    .data_type(*build_key)
                    .ok_or_else(|| "runtime filter build key type missing".to_string())?;
                let probe_type = arena
                    .data_type(*probe_key)
                    .ok_or_else(|| "runtime filter probe key type missing".to_string())?;
                let supported = |t: &DataType| {
                    matches!(
                        t,
                        DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::Float32
                            | DataType::Float64
                            | DataType::Boolean
                            | DataType::Utf8
                            | DataType::Date32
                            | DataType::Timestamp(_, _)
                            | DataType::Decimal128(_, _)
                    )
                };
                if !supported(build_type) || !supported(probe_type) {
                    warn!(
                        "skip runtime filter {} due to unsupported key types build={:?} probe={:?}",
                        filter_id, build_type, probe_type
                    );
                    continue;
                }
                let Some(ExprNode::SlotId(probe_slot_id)) = arena.node(*probe_key) else {
                    continue;
                };
                let merge_nodes = desc.runtime_filter_merge_nodes.clone().unwrap_or_default();
                let has_remote_targets = desc.has_remote_targets.unwrap_or(false);
                runtime_filters.push(JoinRuntimeFilterSpec {
                    filter_id,
                    expr_order,
                    probe_slot_id: *probe_slot_id,
                    merge_nodes,
                    has_remote_targets,
                });
            }
        }
    }

    let output_layout = match join_type {
        JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
            layout.clone()
        }
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::NullAwareLeftAnti => {
            left.layout.clone()
        }
        JoinType::RightSemi | JoinType::RightAnti => right.layout.clone(),
    };

    // For SEMI/ANTI joins, FE may still attach both tuples in row_tuples (join-scope),
    // while the logical output is output-side only. Accept that, but require the output-side
    // tuple(s) to be present when row_tuples is not empty.
    if matches!(
        join_type,
        JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftAnti
            | JoinType::RightAnti
            | JoinType::NullAwareLeftAnti
    ) {
        let out_tuples: HashSet<_> = node.row_tuples.iter().copied().collect();
        if !out_tuples.is_empty() {
            let left_tuples: HashSet<_> = left.layout.order.iter().map(|(t, _)| *t).collect();
            let right_tuples: HashSet<_> = right.layout.order.iter().map(|(t, _)| *t).collect();
            let expected = match join_type {
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::NullAwareLeftAnti => {
                    left_tuples
                }
                JoinType::RightSemi | JoinType::RightAnti => right_tuples,
                JoinType::Inner
                | JoinType::LeftOuter
                | JoinType::RightOuter
                | JoinType::FullOuter => HashSet::new(),
            };
            if !expected.is_empty() && !expected.is_subset(&out_tuples) {
                return Err(format!(
                    "HASH_JOIN_NODE row_tuples {:?} must include output side tuples {:?} for join_type={:?}",
                    node.row_tuples, expected, join_type
                ));
            }
        }
    }

    let Some(desc_tbl) = desc_tbl else {
        return Err("HASH_JOIN_NODE requires desc_tbl for schema".to_string());
    };
    let left_schema = schema_for_layout(desc_tbl, &left.layout)?;
    let right_schema = schema_for_layout(desc_tbl, &right.layout)?;
    let join_scope_schema = schema_for_layout(desc_tbl, &layout)?;

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Join(JoinNode {
                left: Box::new(left.node),
                right: Box::new(right.node),
                node_id: node.node_id,
                join_type,
                distribution_mode,
                left_schema,
                right_schema,
                join_scope_schema,
                probe_keys,
                build_keys,
                eq_null_safe,
                residual_predicate,
                runtime_filters,
            }),
        },
        layout: output_layout,
    })
}
