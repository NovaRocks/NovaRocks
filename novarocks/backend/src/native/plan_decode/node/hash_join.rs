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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use super::common::{concat_layouts, proto_join_type};
use super::{DecodedNode, NativePlanDecodeContext};
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks::exec::node::join::{
    JoinDistributionMode, JoinNode, JoinRuntimeFilterExecution, JoinType,
};
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::plan;
use novarocks::protocol::common::error::FieldPath;
use novarocks_types::wider_type;

pub(super) fn lower_hash_join_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    join: &plan::HashJoinNode,
    path: FieldPath,
    node_path: FieldPath,
    physical_output_path: FieldPath,
    children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let mut it = children.into_iter();
    let left = it.next().expect("left");
    let right = it.next().expect("right");
    if join.eq_conditions.is_empty() {
        return Err(NativeFragmentDecodeError::missing(
            path.clone().field("eq_conditions"),
            "HashJoinNode requires non-empty eq_conditions",
        ));
    }
    let join_type = NativeFragmentDecodeError::map_invalid(
        path.clone().field("join_type"),
        proto_join_type(join.join_type, "HashJoinNode"),
    )?;
    let distribution_mode = hash_join_distribution_mode(join, path.clone())?;
    let join_layout = NativeFragmentDecodeError::map_invalid(
        node_path.clone().field("children"),
        concat_layouts(&left.layout, &right.layout),
    )?;
    let join_scope_chunk_schema = Arc::new(NativeFragmentDecodeError::map_invalid(
        node_path.field("children"),
        ChunkSchema::concat(&[left.output_schema.clone(), right.output_schema.clone()]),
    )?);
    let output_schema = join_output_chunk_schema(
        physical,
        join_scope_chunk_schema.clone(),
        "HashJoinNode",
        physical_output_path,
        ctx,
    )?;

    let mut probe_keys = Vec::with_capacity(join.eq_conditions.len());
    let mut build_keys = Vec::with_capacity(join.eq_conditions.len());
    let mut eq_null_safe = Vec::with_capacity(join.eq_conditions.len());
    let right_semi_physical_right_probe = join_type == JoinType::RightSemi;
    for (idx, cond) in join.eq_conditions.iter().enumerate() {
        let cond_path = path.clone().field("eq_conditions").index(idx);
        let left_expr = cond.left.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                cond_path.clone().field("left"),
                format!("HashJoinNode eq_conditions[{idx}] left missing"),
            )
        })?;
        let right_expr = cond.right.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                cond_path.clone().field("right"),
                format!("HashJoinNode eq_conditions[{idx}] right missing"),
            )
        })?;
        let probe_key = ctx.decode_expression(
            left_expr,
            cond_path.clone().field("left"),
            arena,
            &left.layout,
        )?;
        let build_key =
            ctx.decode_expression(right_expr, cond_path.field("right"), arena, &right.layout)?;
        if right_semi_physical_right_probe {
            probe_keys.push(build_key);
            build_keys.push(probe_key);
        } else {
            probe_keys.push(probe_key);
            build_keys.push(build_key);
        }
        eq_null_safe.push(cond.null_safe);
    }
    NativeFragmentDecodeError::map_invalid(
        path.clone().field("eq_conditions"),
        coerce_join_key_types(&mut probe_keys, &mut build_keys, arena),
    )?;
    for key in probe_keys.iter().chain(build_keys.iter()) {
        if let Some(dt) = arena.data_type(*key)
            && matches!(dt, DataType::LargeBinary)
        {
            return Err(NativeFragmentDecodeError::unsupported(
                path.clone().field("eq_conditions"),
                "VARIANT is not supported in HASH_JOIN keys",
            ));
        }
    }

    let residual_predicate = join
        .other_condition
        .as_ref()
        .map(|expr| {
            ctx.decode_expression(
                expr,
                path.clone().field("other_condition"),
                arena,
                &join_layout,
            )
        })
        .transpose()?;
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Join(JoinNode {
                left: Box::new(left.node),
                right: Box::new(right.node),
                node_id: node.node_id,
                join_type,
                distribution_mode,
                left_chunk_schema: left.output_schema,
                right_chunk_schema: right.output_schema,
                join_scope_chunk_schema: output_schema.clone(),
                probe_keys,
                build_keys,
                eq_null_safe,
                residual_predicate,
                runtime_filter_execution: JoinRuntimeFilterExecution::empty(),
            }),
        },
        layout: join_layout,
        output_schema,
    })
}

pub(super) fn join_output_chunk_schema(
    physical: &plan::PlanNode,
    fallback: ChunkSchemaRef,
    _node_kind: &str,
    path: FieldPath,
    ctx: &NativePlanDecodeContext,
) -> Result<ChunkSchemaRef, NativeFragmentDecodeError> {
    if physical.output_columns.is_empty() {
        return Ok(fallback);
    }
    let output_schema = ctx
        .decode_output_layout(&physical.output_columns, path)?
        .chunk_schema();
    if output_schema.slot_ids() == fallback.slot_ids() {
        return Ok(output_schema);
    }
    Ok(fallback)
}

fn hash_join_distribution_mode(
    join: &plan::HashJoinNode,
    path: FieldPath,
) -> Result<JoinDistributionMode, NativeFragmentDecodeError> {
    if let Some(mode) = join.execution_mode {
        return match plan::JoinExecutionMode::try_from(mode).map_err(|_| {
            NativeFragmentDecodeError::invalid_enum(
                path.clone().field("execution_mode"),
                format!("HashJoinNode unknown execution_mode {mode}"),
            )
        })? {
            plan::JoinExecutionMode::Broadcast => Ok(JoinDistributionMode::Broadcast),
            plan::JoinExecutionMode::Partitioned | plan::JoinExecutionMode::Colocate => {
                Ok(JoinDistributionMode::Partitioned)
            }
            plan::JoinExecutionMode::Unspecified => Err(NativeFragmentDecodeError::invalid_enum(
                path.field("execution_mode"),
                "HashJoinNode execution_mode is unspecified",
            )),
        };
    }

    match plan::JoinDistribution::try_from(join.distribution).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone().field("distribution"),
            format!("HashJoinNode unknown distribution {}", join.distribution),
        )
    })? {
        plan::JoinDistribution::Broadcast | plan::JoinDistribution::Unknown => {
            Ok(JoinDistributionMode::Broadcast)
        }
        plan::JoinDistribution::Shuffle | plan::JoinDistribution::Colocate => {
            Ok(JoinDistributionMode::Partitioned)
        }
        plan::JoinDistribution::Unspecified => Err(NativeFragmentDecodeError::invalid_enum(
            path.field("distribution"),
            "HashJoinNode distribution is unspecified",
        )),
    }
}

pub(super) fn exprs_equivalent(arena: &ExprArena, left: ExprId, right: ExprId) -> bool {
    if arena.data_type(left) != arena.data_type(right) {
        return false;
    }
    let Some(left_node) = arena.node(left) else {
        return false;
    };
    let Some(right_node) = arena.node(right) else {
        return false;
    };
    match (left_node, right_node) {
        (ExprNode::Literal(left), ExprNode::Literal(right)) => {
            format!("{left:?}") == format!("{right:?}")
        }
        (ExprNode::SlotId(left), ExprNode::SlotId(right)) => left == right,
        (ExprNode::ArrayExpr { elements: left }, ExprNode::ArrayExpr { elements: right })
        | (ExprNode::StructExpr { fields: left }, ExprNode::StructExpr { fields: right }) => {
            expr_id_slices_equivalent(arena, left, right)
        }
        (
            ExprNode::LambdaFunction {
                body: left_body,
                arg_slots: left_args,
                common_sub_exprs: left_common,
                is_nondeterministic: left_nondeterministic,
            },
            ExprNode::LambdaFunction {
                body: right_body,
                arg_slots: right_args,
                common_sub_exprs: right_common,
                is_nondeterministic: right_nondeterministic,
            },
        ) => {
            left_args == right_args
                && left_nondeterministic == right_nondeterministic
                && exprs_equivalent(arena, *left_body, *right_body)
                && common_sub_exprs_equivalent(arena, left_common, right_common)
        }
        (
            ExprNode::DictDecode {
                child: left,
                dict: left_dict,
            },
            ExprNode::DictDecode {
                child: right,
                dict: right_dict,
            },
        ) => Arc::ptr_eq(left_dict, right_dict) && exprs_equivalent(arena, *left, *right),
        (ExprNode::Cast(left), ExprNode::Cast(right))
        | (ExprNode::CastTime(left), ExprNode::CastTime(right))
        | (ExprNode::CastTimeFromDatetime(left), ExprNode::CastTimeFromDatetime(right))
        | (ExprNode::Not(left), ExprNode::Not(right))
        | (ExprNode::IsNull(left), ExprNode::IsNull(right))
        | (ExprNode::IsNotNull(left), ExprNode::IsNotNull(right))
        | (ExprNode::Clone(left), ExprNode::Clone(right)) => exprs_equivalent(arena, *left, *right),
        (ExprNode::Add(ll, lr), ExprNode::Add(rl, rr))
        | (ExprNode::Sub(ll, lr), ExprNode::Sub(rl, rr))
        | (ExprNode::Mul(ll, lr), ExprNode::Mul(rl, rr))
        | (ExprNode::Div(ll, lr), ExprNode::Div(rl, rr))
        | (ExprNode::Mod(ll, lr), ExprNode::Mod(rl, rr))
        | (ExprNode::Eq(ll, lr), ExprNode::Eq(rl, rr))
        | (ExprNode::EqForNull(ll, lr), ExprNode::EqForNull(rl, rr))
        | (ExprNode::Ne(ll, lr), ExprNode::Ne(rl, rr))
        | (ExprNode::Lt(ll, lr), ExprNode::Lt(rl, rr))
        | (ExprNode::Le(ll, lr), ExprNode::Le(rl, rr))
        | (ExprNode::Gt(ll, lr), ExprNode::Gt(rl, rr))
        | (ExprNode::Ge(ll, lr), ExprNode::Ge(rl, rr))
        | (ExprNode::And(ll, lr), ExprNode::And(rl, rr))
        | (ExprNode::Or(ll, lr), ExprNode::Or(rl, rr)) => {
            exprs_equivalent(arena, *ll, *rl) && exprs_equivalent(arena, *lr, *rr)
        }
        (
            ExprNode::In {
                child: left_child,
                values: left_values,
                is_not_in: left_not,
            },
            ExprNode::In {
                child: right_child,
                values: right_values,
                is_not_in: right_not,
            },
        ) => {
            left_not == right_not
                && exprs_equivalent(arena, *left_child, *right_child)
                && expr_id_slices_equivalent(arena, left_values, right_values)
        }
        (
            ExprNode::Case {
                has_case_expr: left_has_case,
                has_else_expr: left_has_else,
                children: left_children,
            },
            ExprNode::Case {
                has_case_expr: right_has_case,
                has_else_expr: right_has_else,
                children: right_children,
            },
        ) => {
            left_has_case == right_has_case
                && left_has_else == right_has_else
                && expr_id_slices_equivalent(arena, left_children, right_children)
        }
        (
            ExprNode::FunctionCall {
                kind: left_kind,
                args: left_args,
            },
            ExprNode::FunctionCall {
                kind: right_kind,
                args: right_args,
            },
        ) => left_kind == right_kind && expr_id_slices_equivalent(arena, left_args, right_args),
        _ => false,
    }
}

fn expr_id_slices_equivalent(arena: &ExprArena, left: &[ExprId], right: &[ExprId]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| exprs_equivalent(arena, *left, *right))
}

fn common_sub_exprs_equivalent(
    arena: &ExprArena,
    left: &[(SlotId, ExprId)],
    right: &[(SlotId, ExprId)],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|((left_slot, left_expr), (right_slot, right_expr))| {
                left_slot == right_slot && exprs_equivalent(arena, *left_expr, *right_expr)
            })
}

fn coerce_join_key_types(
    probe_keys: &mut [ExprId],
    build_keys: &mut [ExprId],
    arena: &mut ExprArena,
) -> Result<(), String> {
    for idx in 0..probe_keys.len() {
        let probe_expr = probe_keys[idx];
        let build_expr = build_keys[idx];
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
                    probe_keys[idx] =
                        arena.push_typed(ExprNode::Cast(probe_expr), target_type.clone());
                }
                if build_type != target_type {
                    build_keys[idx] = arena.push_typed(ExprNode::Cast(build_expr), target_type);
                }
            }
            None => {
                build_keys[idx] = arena.push_typed(ExprNode::Cast(build_expr), probe_type);
            }
        }
    }
    Ok(())
}

fn common_join_key_type(left: &DataType, right: &DataType) -> Result<Option<DataType>, String> {
    if left == right {
        return Ok(Some(left.clone()));
    }
    match (left, right) {
        (
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
        ) => Ok(Some(novarocks_types::coercion::decimal_compare_type(
            left, right,
        )?)),
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
        _ => Ok(Some(wider_type(left, right))),
    }
}
