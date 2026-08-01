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

use arrow::datatypes::{DataType, Field, Fields};

use super::common::build_slot_projection;
use super::{DecodedNode, NativePlanDecodeContext};
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::ChunkSchema;
use novarocks::exec::expr::{ExprArena, ExprNode};
use novarocks::exec::node::aggregate::{
    AggFunction, AggOrderSpec, AggTypeSignature, AggregateNode, AggregateRuntimeFilterSpec,
};
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::plan;
use novarocks::protocol::common::error::FieldPath;
use novarocks_types::aggregate::{infer_agg_function_types, mangle_distinct_aggregate_name};

pub(super) fn lower_hash_aggregate_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    aggregate: &plan::HashAggregateNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    if aggregate.is_merge.len() != aggregate.aggregates.len() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("is_merge"),
            format!(
                "HashAggregateNode is_merge length mismatch: is_merge={} aggregates={}",
                aggregate.is_merge.len(),
                aggregate.aggregates.len()
            ),
        ));
    }
    let mode = plan::AggMode::try_from(aggregate.mode).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone().field("mode"),
            format!("HashAggregateNode unknown mode {}", aggregate.mode),
        )
    })?;
    if mode == plan::AggMode::Unspecified {
        return Err(NativeFragmentDecodeError::invalid_enum(
            path.clone().field("mode"),
            "HashAggregateNode mode is unspecified",
        ));
    }
    let output_layout = aggregate.output_layout.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("output_layout"),
            "HashAggregateNode output_layout missing",
        )
    })?;
    let group_key_path = path
        .clone()
        .field("output_layout")
        .field("group_key_columns");
    let aggregate_columns_path = path
        .clone()
        .field("output_layout")
        .field("aggregate_columns");
    let mut aggregate_slot_schemas = ctx
        .decode_output_layout(&output_layout.group_key_columns, group_key_path)?
        .slot_schemas()
        .to_vec();
    aggregate_slot_schemas.extend(
        ctx.decode_output_layout(&output_layout.aggregate_columns, aggregate_columns_path)?
            .slot_schemas()
            .iter()
            .cloned(),
    );
    let aggregate_layout =
        Layout::for_slots(aggregate_slot_schemas.iter().map(|slot| slot.slot_id()));
    let aggregate_output_schema = Arc::new(ChunkSchema::try_new(aggregate_slot_schemas).map_err(
        |error| NativeFragmentDecodeError::inconsistent(path.clone().field("output_layout"), error),
    )?);
    if output_layout.aggregate_columns.len() != aggregate.aggregates.len() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone()
                .field("output_layout")
                .field("aggregate_columns"),
            format!(
                "HashAggregateNode output_layout aggregate column mismatch: columns={} aggregates={}",
                output_layout.aggregate_columns.len(),
                aggregate.aggregates.len()
            ),
        ));
    }
    if output_layout.group_key_columns.len() != aggregate.group_by.len() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone()
                .field("output_layout")
                .field("group_key_columns"),
            format!(
                "HashAggregateNode output_layout group key mismatch: columns={} group_by={}",
                output_layout.group_key_columns.len(),
                aggregate.group_by.len()
            ),
        ));
    }

    let group_by = aggregate
        .group_by
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            ctx.decode_expression(
                expr,
                path.clone().field("group_by").index(idx),
                arena,
                &child.layout,
            )
        })
        .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
    for expr_id in &group_by {
        if let Some(dt) = arena.data_type(*expr_id)
            && matches!(dt, DataType::LargeBinary)
        {
            return Err(NativeFragmentDecodeError::unsupported(
                path.clone().field("group_by"),
                "VARIANT is not supported in GROUP BY",
            ));
        }
    }

    let need_finalize = matches!(mode, plan::AggMode::Single | plan::AggMode::Global);
    let mut functions = Vec::with_capacity(aggregate.aggregates.len());
    for (idx, call) in aggregate.aggregates.iter().enumerate() {
        let is_merge = aggregate.is_merge[idx];
        let output_col = output_layout.aggregate_columns.get(idx).ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone()
                    .field("output_layout")
                    .field("aggregate_columns")
                    .index(idx),
                format!("HashAggregateNode aggregate column {idx} missing"),
            )
        })?;
        let result_type = call.result_type.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone()
                    .field("aggregates")
                    .index(idx)
                    .field("result_type"),
                format!("HashAggregateNode aggregate {idx} result_type missing"),
            )
        })?;
        let result_type = NativeFragmentDecodeError::map_invalid(
            path.clone()
                .field("aggregates")
                .index(idx)
                .field("result_type"),
            crate::native::type_decode::decode_type(result_type),
        )?;
        let function_name = aggregate_function_name(call);
        let signature_arg_types =
            aggregate_signature_arg_types(call, path.clone().field("aggregates").index(idx))?;
        let (semantic_output_type, intermediate_type) =
            infer_agg_function_types(&function_name, &signature_arg_types, call.distinct).map_err(
                |err| {
                    NativeFragmentDecodeError::invalid_value(
                        path.clone().field("aggregates").index(idx),
                        format!("HashAggregateNode aggregate {idx} type inference: {err}"),
                    )
                },
            )?;
        let signature_input_arg_type = signature_arg_types.first().cloned();
        let signature_output_type = if need_finalize {
            result_type
        } else {
            semantic_output_type
        };

        let raw_args = if is_merge {
            let slot = SlotId::new(output_col.column_id);
            let data_type = intermediate_type.clone().ok_or_else(|| NativeFragmentDecodeError::invalid_value(path.clone().field("aggregates").index(idx), format!(
                    "HashAggregateNode merge aggregate {idx} requires a known intermediate type for {}",
                    function_name
                )))?;
            vec![arena.push_typed(ExprNode::SlotId(slot), data_type)]
        } else {
            lower_aggregate_update_inputs(
                call,
                idx,
                path.clone().field("aggregates").index(idx),
                &child,
                arena,
                ctx,
            )?
        };
        let inputs = NativeFragmentDecodeError::map_invalid(
            path.clone().field("aggregates").index(idx),
            select_aggregate_inputs(&call.name.to_ascii_lowercase(), is_merge, raw_args, arena),
        )?;
        functions.push(AggFunction {
            name: function_name,
            inputs,
            input_is_intermediate: is_merge,
            types: Some(AggTypeSignature {
                intermediate_type,
                output_type: Some(signature_output_type),
                input_arg_type: signature_input_arg_type,
            }),
            order: aggregate_order_spec(call),
        });
    }

    let input_is_intermediate = functions.iter().all(|f| f.input_is_intermediate);
    let aggregate_node = DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                group_by,
                functions,
                need_finalize,
                input_is_intermediate,
                output_chunk_schema: aggregate_output_schema.clone(),
                runtime_filter_spec: AggregateRuntimeFilterSpec::empty(),
                streaming_preaggregation_mode: None,
            }),
        },
        layout: aggregate_layout,
        output_schema: aggregate_output_schema,
    };
    let Some((visible_output_columns, visible_path)) = (if !aggregate.output_columns.is_empty() {
        Some((
            aggregate.output_columns.as_slice(),
            path.clone().field("output_columns"),
        ))
    } else if !physical.output_columns.is_empty() {
        Some((physical.output_columns.as_slice(), physical_output_path))
    } else {
        None
    }) else {
        return Ok(aggregate_node);
    };
    let visible_layout = Layout::for_slots(
        ctx.decode_output_layout(visible_output_columns, visible_path.clone())?
            .slot_ids()
            .iter()
            .copied(),
    );
    if visible_layout.order() == aggregate_node.layout.order() {
        return Ok(aggregate_node);
    }
    build_slot_projection(
        "HashAggregateNode",
        aggregate_node,
        visible_output_columns,
        visible_path,
        node.node_id,
        arena,
        ctx,
    )
}

fn aggregate_function_name(call: &plan::PlanAggregateCall) -> String {
    // Delegate the DISTINCT-mangling table to the single source of truth; this
    // proto-typed wrapper differs from the planner-typed one only in its input.
    mangle_distinct_aggregate_name(&call.name, call.distinct)
}

fn aggregate_signature_arg_types(
    call: &plan::PlanAggregateCall,
    path: FieldPath,
) -> Result<Vec<DataType>, NativeFragmentDecodeError> {
    let mut types = call
        .args
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            let ty = expr.r#type.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone().field("args").index(idx).field("type"),
                    format!("aggregate {} argument {idx} type missing", call.name),
                )
            })?;
            NativeFragmentDecodeError::map_invalid(
                path.clone().field("args").index(idx).field("type"),
                crate::native::type_decode::decode_type(ty),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (idx, item) in call.order_by.iter().enumerate() {
        let expr = item.expr.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone().field("order_by").index(idx).field("expr"),
                format!("aggregate {} order_by[{idx}] expr missing", call.name),
            )
        })?;
        let data_type = expr.r#type.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone()
                    .field("order_by")
                    .index(idx)
                    .field("expr")
                    .field("type"),
                format!("aggregate {} order_by[{idx}] type missing", call.name),
            )
        })?;
        let data_type = NativeFragmentDecodeError::map_invalid(
            path.clone()
                .field("order_by")
                .index(idx)
                .field("expr")
                .field("type"),
            crate::native::type_decode::decode_type(data_type),
        )?;
        types.push(data_type);
    }
    Ok(types)
}

fn lower_aggregate_update_inputs(
    call: &plan::PlanAggregateCall,
    aggregate_idx: usize,
    path: FieldPath,
    child: &DecodedNode,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<Vec<novarocks::exec::expr::ExprId>, NativeFragmentDecodeError> {
    if call.name.eq_ignore_ascii_case("count_if") && !call.order_by.is_empty() {
        return Err(NativeFragmentDecodeError::unsupported(
            path.clone().field("order_by"),
            format!(
                "HashAggregateNode aggregate {aggregate_idx} count_if does not support ORDER BY"
            ),
        ));
    }
    let mut inputs = Vec::with_capacity(call.args.len() + call.order_by.len());
    for (arg_idx, expr) in call.args.iter().enumerate() {
        inputs.push(ctx.decode_expression(
            expr,
            path.clone().field("args").index(arg_idx),
            arena,
            &child.layout,
        )?);
    }
    for (order_idx, item) in call.order_by.iter().enumerate() {
        let item_path = path.clone().field("order_by").index(order_idx);
        let expr = item.expr.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                item_path.clone().field("expr"),
                format!(
                    "HashAggregateNode aggregate {aggregate_idx} order_by[{order_idx}] expr missing"
                ),
            )
        })?;
        inputs.push(ctx.decode_expression(expr, item_path.field("expr"), arena, &child.layout)?);
    }
    Ok(inputs)
}

fn aggregate_order_spec(call: &plan::PlanAggregateCall) -> AggOrderSpec {
    AggOrderSpec {
        is_asc_order: call.order_by.iter().map(|item| item.asc).collect(),
        nulls_first: call.order_by.iter().map(|item| item.nulls_first).collect(),
        is_distinct: call.distinct,
        group_concat_max_len: if call.name.eq_ignore_ascii_case("group_concat")
            || call.name.eq_ignore_ascii_case("string_agg")
        {
            Some(1024)
        } else {
            None
        },
    }
}

fn select_aggregate_inputs(
    fn_name: &str,
    is_merge: bool,
    args: Vec<novarocks::exec::expr::ExprId>,
    arena: &mut ExprArena,
) -> Result<Vec<novarocks::exec::expr::ExprId>, String> {
    if is_merge {
        return args
            .into_iter()
            .next()
            .map(|expr| vec![expr])
            .ok_or_else(|| format!("{fn_name} merge input missing"));
    }
    if fn_name == "count_if" {
        return match args.len() {
            1 => Ok(args),
            2 => Ok(vec![args[1]]),
            other => Err(format!("count_if expects 1 or 2 arguments, got {other}")),
        };
    }
    pack_struct_inputs(args, arena)
}

fn pack_struct_inputs(
    args: Vec<novarocks::exec::expr::ExprId>,
    arena: &mut ExprArena,
) -> Result<Vec<novarocks::exec::expr::ExprId>, String> {
    if args.len() <= 1 {
        return Ok(args);
    }
    let mut fields = Vec::with_capacity(args.len());
    for (idx, expr_id) in args.iter().enumerate() {
        let data_type = arena
            .data_type(*expr_id)
            .ok_or_else(|| "aggregate input type missing".to_string())?;
        fields.push(Field::new(format!("f{idx}"), data_type.clone(), true));
    }
    let struct_type = DataType::Struct(Fields::from(fields));
    let struct_expr = arena.push_typed(ExprNode::StructExpr { fields: args }, struct_type);
    Ok(vec![struct_expr])
}
