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

//! Fragment hash-join decoding.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use crate::fragment_error::NativeFragmentDecodeError;
use crate::fragment_expression::decode_expr_for_slot_layout;
use crate::fragment_layout::decode_output_layout;
use crate::fragment_plan_node::NativeLoweredPlanNode as DecodedNode;
use crate::fragment_plan_node::{concat_slot_layouts, proto_join_type};
use novarocks_execution::exec::chunk::{ChunkSchema, ChunkSchemaRef, SlotLayout};
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks_execution::exec::node::join::{
    JoinDistributionMode, JoinNode, JoinRuntimeFilterExecution, JoinType,
};
use novarocks_execution::exec::node::project::ProjectNode;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::plan;
use novarocks_types::SlotId;
use novarocks_types::wider_type;

#[expect(
    clippy::too_many_arguments,
    reason = "The frozen native boundary keeps independently validated inputs explicit."
)]
pub fn lower_hash_join_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    join: &plan::HashJoinNode,
    path: FieldPath,
    node_path: FieldPath,
    physical_output_path: FieldPath,
    children: Vec<DecodedNode>,
    arena: &mut ExprArena,
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
        concat_slot_layouts(&left.layout, &right.layout),
    )?;
    let (nullable_left, nullable_right) = hash_join_nullable_sides(join_type);
    let join_scope_chunk_schema = join_scope_chunk_schema(
        &left.output_schema,
        &right.output_schema,
        nullable_left,
        nullable_right,
        node_path.field("children"),
    )?;

    let mut probe_keys = Vec::with_capacity(join.eq_conditions.len());
    let mut build_keys = Vec::with_capacity(join.eq_conditions.len());
    let mut eq_null_safe = Vec::with_capacity(join.eq_conditions.len());
    let build_is_left = hash_join_build_is_left(join_type);
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
        let probe_key = decode_expr_for_slot_layout(
            left_expr,
            cond_path.clone().field("left"),
            arena,
            &left.layout,
        )?;
        let build_key = decode_expr_for_slot_layout(
            right_expr,
            cond_path.field("right"),
            arena,
            &right.layout,
        )?;
        if build_is_left {
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
            decode_expr_for_slot_layout(
                expr,
                path.clone().field("other_condition"),
                arena,
                &join_layout,
            )
        })
        .transpose()?;
    let preserved_layout = match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::NullAwareLeftAnti => {
            Some(left.layout.clone())
        }
        JoinType::RightSemi | JoinType::RightAnti => Some(right.layout.clone()),
        _ => None,
    };
    let join_node = DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Join(JoinNode {
                left: Box::new(left.node),
                right: Box::new(right.node),
                node_id: node.node_id,
                join_type,
                distribution_mode,
                left_chunk_schema: left.output_schema,
                right_chunk_schema: right.output_schema,
                join_scope_chunk_schema: join_scope_chunk_schema.clone(),
                probe_keys,
                build_keys,
                eq_null_safe,
                residual_predicate,
                runtime_filter_execution: JoinRuntimeFilterExecution::empty(),
            }),
        },
        layout: join_layout,
        output_schema: join_scope_chunk_schema,
    };
    build_join_output_projection(
        ("HashJoinNode", node.node_id),
        join_node,
        &physical.output_columns,
        physical_output_path,
        preserved_layout.as_ref(),
        arena,
    )
}

pub(crate) const fn hash_join_build_is_left(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::RightSemi | JoinType::RightAnti)
}

fn build_join_output_projection(
    identity: (&str, i32),
    join_node: DecodedNode,
    output_columns: &[novarocks_proto_models::common::OutputColumn],
    path: FieldPath,
    preserved_layout: Option<&SlotLayout>,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let (node_kind, node_id) = identity;
    if output_columns.is_empty() {
        if preserved_layout.is_some() {
            return Err(NativeFragmentDecodeError::missing(
                path,
                format!("{node_kind} semi/anti join requires an explicit preserved-side output"),
            ));
        }
        return Ok(join_node);
    }
    let output_layout = decode_output_layout(output_columns, path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    let output_schema = output_layout.chunk_schema();
    if let Some(preserved_layout) = preserved_layout
        && output_schema.slot_ids() != preserved_layout.order()
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            format!(
                "{node_kind} semi/anti output must equal the preserved-side slots: expected {:?}, got {:?}",
                preserved_layout.order(),
                output_schema.slot_ids()
            ),
        ));
    }
    let layout = SlotLayout::for_slots(output_layout.slot_ids().iter().copied());
    let expr_slot_schemas = output_layout.slot_schemas().to_vec();
    let mut exprs = Vec::with_capacity(layout.order().len());
    for output in output_schema.slots() {
        let Some(source) = join_node.output_schema.slot(output.slot_id()) else {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone(),
                format!(
                    "{node_kind} output slot {} is not present in the complete join scope",
                    output.slot_id()
                ),
            ));
        };
        if output.data_type() != source.data_type() || output.nullable() != source.nullable() {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone(),
                format!(
                    "{node_kind} output slot {} type/nullability differs from the complete join scope",
                    output.slot_id()
                ),
            ));
        }
        let expr = arena.push_typed(
            ExprNode::SlotId(output.slot_id()),
            source.data_type().clone(),
        );
        arena.set_field_schema(expr, source.field_schema().clone());
        exprs.push(expr);
    }
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(join_node.node),
                node_id,
                is_subordinate: true,
                exprs,
                expr_slot_ids: layout.order().to_vec(),
                expr_slot_schemas: Some(expr_slot_schemas),
                output_indices: None,
                output_chunk_schema: output_schema.clone(),
            }),
        },
        layout,
        output_schema,
    })
}

fn join_scope_chunk_schema(
    left: &ChunkSchemaRef,
    right: &ChunkSchemaRef,
    nullable_left: bool,
    nullable_right: bool,
    path: FieldPath,
) -> Result<ChunkSchemaRef, NativeFragmentDecodeError> {
    let slots = left
        .slots()
        .iter()
        .map(|slot| {
            if nullable_left {
                slot.with_nullable(true)
            } else {
                slot.clone()
            }
        })
        .chain(right.slots().iter().map(|slot| {
            if nullable_right {
                slot.with_nullable(true)
            } else {
                slot.clone()
            }
        }))
        .collect();
    ChunkSchema::try_new(slots)
        .map(Arc::new)
        .map_err(|error| NativeFragmentDecodeError::inconsistent(path, error))
}

const fn hash_join_nullable_sides(join_type: JoinType) -> (bool, bool) {
    match join_type {
        JoinType::LeftOuter
        | JoinType::LeftSemi
        | JoinType::LeftAnti
        | JoinType::NullAwareLeftAnti => (false, true),
        JoinType::RightOuter | JoinType::RightSemi | JoinType::RightAnti => (true, false),
        JoinType::FullOuter => (true, true),
        JoinType::Inner => (false, false),
    }
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

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
fn exprs_equivalent(arena: &ExprArena, left: ExprId, right: ExprId) -> bool {
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
        (ExprNode::Cast(left, lp), ExprNode::Cast(right, rp))
        | (ExprNode::CastTime(left, lp), ExprNode::CastTime(right, rp))
        | (ExprNode::CastTimeFromDatetime(left, lp), ExprNode::CastTimeFromDatetime(right, rp)) => {
            lp == rp && exprs_equivalent(arena, *left, *right)
        }
        (ExprNode::Not(left), ExprNode::Not(right))
        | (ExprNode::IsNull(left), ExprNode::IsNull(right))
        | (ExprNode::IsNotNull(left), ExprNode::IsNotNull(right))
        | (ExprNode::Clone(left), ExprNode::Clone(right)) => exprs_equivalent(arena, *left, *right),
        (ExprNode::Add(ll, lr, lp), ExprNode::Add(rl, rr, rp))
        | (ExprNode::Sub(ll, lr, lp), ExprNode::Sub(rl, rr, rp))
        | (ExprNode::Mul(ll, lr, lp), ExprNode::Mul(rl, rr, rp))
        | (ExprNode::Div(ll, lr, lp), ExprNode::Div(rl, rr, rp))
        | (ExprNode::Mod(ll, lr, lp), ExprNode::Mod(rl, rr, rp)) => {
            lp == rp && exprs_equivalent(arena, *ll, *rl) && exprs_equivalent(arena, *lr, *rr)
        }
        (ExprNode::Eq(ll, lr), ExprNode::Eq(rl, rr))
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

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
fn expr_id_slices_equivalent(arena: &ExprArena, left: &[ExprId], right: &[ExprId]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| exprs_equivalent(arena, *left, *right))
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
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
                    probe_keys[idx] = arena.push_typed(
                        ExprNode::Cast(
                            probe_expr,
                            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                        ),
                        target_type.clone(),
                    );
                }
                if build_type != target_type {
                    build_keys[idx] = arena.push_typed(
                        ExprNode::Cast(
                            build_expr,
                            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                        ),
                        target_type,
                    );
                }
            }
            None => {
                build_keys[idx] = arena.push_typed(
                    ExprNode::Cast(
                        build_expr,
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    ),
                    probe_type,
                );
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

#[cfg(test)]
mod decimal_policy_tests {
    use super::*;
    use novarocks_execution::exec::expr::LiteralValue;
    use novarocks_type_contract::DecimalOverflowPolicy as Policy;

    #[test]
    fn join_expression_equivalence_preserves_decimal_policy_identity() {
        let mut arena = ExprArena::default();
        let input = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let a = arena.push_typed(ExprNode::Cast(input, Policy::OutputNull), DataType::Int64);
        let b = arena.push_typed(ExprNode::Cast(input, Policy::ReportError), DataType::Int64);
        let c = arena.push_typed(ExprNode::Cast(input, Policy::OutputNull), DataType::Int64);
        assert!(!exprs_equivalent(&arena, a, b));
        assert!(exprs_equivalent(&arena, a, c));
        let a = arena.push_typed(
            ExprNode::Add(input, input, Policy::OutputNull),
            DataType::Int64,
        );
        let b = arena.push_typed(
            ExprNode::Add(input, input, Policy::ReportError),
            DataType::Int64,
        );
        assert!(!exprs_equivalent(&arena, a, b));
    }
}
