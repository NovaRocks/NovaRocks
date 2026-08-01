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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};

use super::{DecodedNode, NativePlanDecodeContext, sort};
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks::exec::expr::{ExprArena, ExprNode};
use novarocks::exec::node::analytic::{
    AnalyticNode, AnalyticOutputColumn, WindowBoundary, WindowFrame, WindowFunctionKind,
    WindowFunctionSpec, WindowType,
};
use novarocks::exec::node::sort::{SortExpression, SortNode, SortTopNType};
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::{expr, plan};
use novarocks::protocol::common::error::FieldPath;

pub(super) fn lower_window_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    window: &plan::WindowNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    if window.window_exprs.is_empty() {
        return Err(NativeFragmentDecodeError::missing(
            path.clone().field("window_exprs"),
            "WindowNode has no window expressions",
        ));
    }
    let (output_columns, output_columns_path) = if !window.output_columns.is_empty() {
        (
            window.output_columns.as_slice(),
            path.clone().field("output_columns"),
        )
    } else {
        (physical.output_columns.as_slice(), physical_output_path)
    };
    if output_columns.is_empty() {
        return Err(NativeFragmentDecodeError::missing(
            output_columns_path.clone(),
            "WindowNode output_columns missing",
        ));
    }
    let output_layout = ctx.decode_output_layout(output_columns, output_columns_path)?;
    let final_layout = Layout::for_slots(output_layout.slot_ids().iter().copied());
    let final_output_schema = output_layout.chunk_schema();

    let groups = group_window_exprs_by_spec(&window.window_exprs);
    if groups.is_empty() {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("window_exprs"),
            "WindowNode produced no window expression groups",
        ));
    }

    let mut current = child;
    let mut next_node_id = node.node_id;
    for (group_idx, group_indices) in groups.iter().enumerate() {
        let first_idx = group_indices.first().copied().ok_or_else(|| {
            NativeFragmentDecodeError::invalid_value(
                path.clone().field("window_exprs"),
                format!("WindowNode group {group_idx} is empty"),
            )
        })?;
        let first = &window.window_exprs[first_idx];
        let is_last = group_idx + 1 == groups.len();

        if group_idx > 0 && window_expr_has_sort_keys(first) {
            current = sort_window_group_input(
                next_node_id,
                group_idx,
                first,
                path.clone().field("window_exprs").index(first_idx),
                current,
                arena,
                ctx,
            )?;
            next_node_id = next_node_id.checked_add(1).ok_or_else(|| {
                NativeFragmentDecodeError::out_of_range(
                    path.clone(),
                    format!(
                        "WindowNode node_id {next_node_id} overflows after sort group {group_idx}"
                    ),
                )
            })?;
        }

        let (layout, output_schema) = if is_last {
            (final_layout.clone(), final_output_schema.clone())
        } else {
            intermediate_window_output(
                &current,
                group_indices,
                window,
                &final_output_schema,
                path.clone(),
            )?
        };

        let partition_exprs = first
            .partition_by
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                ctx.decode_expression(
                    expr,
                    path.clone()
                        .field("window_exprs")
                        .index(first_idx)
                        .field("partition_by")
                        .index(idx),
                    arena,
                    &current.layout,
                )
            })
            .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
        let order_by_exprs = first
            .order_by
            .iter()
            .enumerate()
            .map(|(idx, item)| {
                let item_path = path
                    .clone()
                    .field("window_exprs")
                    .index(first_idx)
                    .field("order_by")
                    .index(idx);
                let expr = item.expr.as_ref().ok_or_else(|| {
                    NativeFragmentDecodeError::missing(
                        item_path.clone().field("expr"),
                        format!("WindowNode group {group_idx} order_by[{idx}] expr missing"),
                    )
                })?;
                ctx.decode_expression(expr, item_path.field("expr"), arena, &current.layout)
            })
            .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
        let frame = first
            .window_frame
            .as_ref()
            .map(|frame| {
                lower_window_frame(
                    frame,
                    path.clone()
                        .field("window_exprs")
                        .index(first_idx)
                        .field("window_frame"),
                )
            })
            .transpose()?;
        NativeFragmentDecodeError::map_invalid(
            path.clone()
                .field("window_exprs")
                .index(first_idx)
                .field("window_frame"),
            validate_window_frame(&frame, order_by_exprs.is_empty()),
        )?;

        let mut functions = Vec::with_capacity(group_indices.len());
        for (_local_idx, expr_idx) in group_indices.iter().copied().enumerate() {
            let expr = &window.window_exprs[expr_idx];
            functions.push(lower_window_function(
                expr,
                path.clone().field("window_exprs").index(expr_idx),
                arena,
                &current.layout,
                ctx,
            )?);
        }

        let mut function_by_slot = HashMap::with_capacity(group_indices.len());
        for (local_idx, expr_idx) in group_indices.iter().copied().enumerate() {
            let expr = &window.window_exprs[expr_idx];
            let slot = SlotId::new(expr.output_column_id);
            if function_by_slot.insert(slot, local_idx).is_some() {
                return Err(NativeFragmentDecodeError::inconsistent(
                    path.clone()
                        .field("window_exprs")
                        .index(expr_idx)
                        .field("output_column_id"),
                    format!(
                        "WindowNode duplicate output_column_id {}",
                        expr.output_column_id
                    ),
                ));
            }
        }
        let analytic_output_columns = NativeFragmentDecodeError::map_invalid(
            path.clone().field("window_exprs"),
            window_analytic_output_columns(&layout, &current.layout, &function_by_slot, group_idx),
        )?;
        let group_node_id = next_node_id;
        next_node_id = next_node_id.checked_add(1).ok_or_else(|| {
            NativeFragmentDecodeError::out_of_range(
                path.clone(),
                format!(
                    "WindowNode node_id {next_node_id} overflows after analytic group {group_idx}"
                ),
            )
        })?;

        current = DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::Analytic(AnalyticNode {
                    input: Box::new(current.node),
                    node_id: group_node_id,
                    partition_exprs,
                    order_by_exprs,
                    functions,
                    window: frame,
                    output_columns: analytic_output_columns,
                    output_chunk_schema: output_schema.clone(),
                }),
            },
            layout,
            output_schema,
        };
    }

    Ok(current)
}

fn window_expr_has_sort_keys(expr: &plan::WindowExpr) -> bool {
    !expr.partition_by.is_empty() || !expr.order_by.is_empty()
}

fn sort_window_group_input(
    node_id: i32,
    group_idx: usize,
    first: &plan::WindowExpr,
    path: FieldPath,
    input: DecodedNode,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let mut order_by = Vec::with_capacity(first.partition_by.len() + first.order_by.len());
    for (idx, expr) in first.partition_by.iter().enumerate() {
        let expr = ctx.decode_expression(
            expr,
            path.clone().field("partition_by").index(idx),
            arena,
            &input.layout,
        )?;
        order_by.push(SortExpression {
            expr,
            asc: true,
            nulls_first: true,
        });
    }
    order_by.extend(sort::lower_sort_items_with_context(
        &format!("WindowNode group {group_idx} sort"),
        &first.order_by,
        path.field("order_by"),
        arena,
        &input.layout,
        ctx,
    )?);

    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Sort(SortNode {
                input: Box::new(input.node),
                node_id,
                use_top_n: false,
                order_by,
                limit: None,
                offset: 0,
                topn_type: SortTopNType::RowNumber,
                max_buffered_rows: None,
                max_buffered_bytes: None,
                partition_exprs: Vec::new(),
                partition_limit: None,
            }),
        },
        layout: input.layout,
        output_schema: input.output_schema,
    })
}

fn group_window_exprs_by_spec(exprs: &[plan::WindowExpr]) -> Vec<Vec<usize>> {
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (idx, expr) in exprs.iter().enumerate() {
        if let Some(group) = groups
            .iter_mut()
            .find(|group| same_window_spec(&exprs[group[0]], expr))
        {
            group.push(idx);
        } else {
            groups.push(vec![idx]);
        }
    }
    groups
}

fn intermediate_window_output(
    current: &DecodedNode,
    group_indices: &[usize],
    window: &plan::WindowNode,
    final_output_schema: &ChunkSchema,
    path: FieldPath,
) -> Result<(Layout, ChunkSchemaRef), NativeFragmentDecodeError> {
    let mut slot_ids = current.layout.order().to_vec();
    let mut slots = Vec::with_capacity(slot_ids.len() + group_indices.len());
    for slot_id in current.layout.order() {
        let slot = current.output_schema.slot(*slot_id).cloned().ok_or_else(|| NativeFragmentDecodeError::inconsistent(path.clone().field("output_columns"), format!("current output schema missing input slot {} for intermediate WindowNode output", slot_id)))?;
        slots.push(slot);
    }

    for expr_idx in group_indices {
        let expr = window.window_exprs.get(*expr_idx).ok_or_else(|| {
            NativeFragmentDecodeError::out_of_range(
                path.clone().field("window_exprs").index(*expr_idx),
                format!("window expression index {expr_idx} is out of bounds"),
            )
        })?;
        let slot_id = SlotId::new(expr.output_column_id);
        if slot_ids.contains(&slot_id) {
            continue;
        }
        slot_ids.push(slot_id);
        slots.push(window_expr_slot_schema(
            expr,
            final_output_schema,
            path.clone().field("window_exprs").index(*expr_idx),
        )?);
    }

    Ok((
        Layout::for_slots(slot_ids),
        Arc::new(NativeFragmentDecodeError::map_invalid(
            path.field("output_columns"),
            ChunkSchema::try_new(slots),
        )?),
    ))
}

fn window_expr_slot_schema(
    expr: &plan::WindowExpr,
    final_output_schema: &ChunkSchema,
    path: FieldPath,
) -> Result<ChunkSlotSchema, NativeFragmentDecodeError> {
    let slot_id = SlotId::new(expr.output_column_id);
    if let Some(slot) = final_output_schema.slot(slot_id) {
        return Ok(slot.clone());
    }

    let type_desc = expr.result_type.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("result_type"),
            format!("window function {} result_type missing", expr.name),
        )
    })?;
    let data_type = NativeFragmentDecodeError::map_invalid(
        path.clone().field("result_type"),
        crate::native::type_decode::decode_type(type_desc),
    )?;
    let field = Field::new(&expr.output_name, data_type, true);
    NativeFragmentDecodeError::map_invalid(
        path.field("output_column_id"),
        ChunkSchema::slot_schema_from_arrow_field(slot_id, &field),
    )
}

fn window_analytic_output_columns(
    layout: &Layout,
    input_layout: &Layout,
    function_by_slot: &HashMap<SlotId, usize>,
    group_idx: usize,
) -> Result<Vec<AnalyticOutputColumn>, String> {
    layout
        .order()
        .iter()
        .map(|slot| {
            if let Some(idx) = function_by_slot.get(slot) {
                Ok(AnalyticOutputColumn::Window(*idx))
            } else if input_layout.contains_slot(*slot) {
                Ok(AnalyticOutputColumn::InputSlotId(*slot))
            } else {
                Err(format!(
                    "WindowNode group {group_idx} output slot {} has no input slot or window result",
                    slot
                ))
            }
        })
        .collect()
}

fn same_window_spec(left: &plan::WindowExpr, right: &plan::WindowExpr) -> bool {
    left.partition_by == right.partition_by
        && left.order_by == right.order_by
        && left.window_frame == right.window_frame
}

fn lower_window_function(
    expr: &plan::WindowExpr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &Layout,
    ctx: &NativePlanDecodeContext,
) -> Result<WindowFunctionSpec, NativeFragmentDecodeError> {
    let name = expr.name.to_ascii_lowercase();
    let kind = NativeFragmentDecodeError::map_invalid(
        path.clone().field("name"),
        window_function_kind(&name, expr.distinct, expr.ignore_nulls),
    )?;
    let return_type = expr.result_type.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("result_type"),
            format!("window function {} result_type missing", expr.name),
        )
    })?;
    let return_type = NativeFragmentDecodeError::map_invalid(
        path.clone().field("result_type"),
        crate::native::type_decode::decode_type(return_type),
    )?;
    let mut args = expr
        .args
        .iter()
        .enumerate()
        .map(|(idx, arg)| {
            ctx.decode_expression(
                arg,
                path.clone().field("args").index(idx),
                arena,
                input_layout,
            )
        })
        .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
    if matches!(
        kind,
        WindowFunctionKind::ArrayAgg { .. }
            | WindowFunctionKind::MaxBy
            | WindowFunctionKind::MaxByV2
            | WindowFunctionKind::MinBy
            | WindowFunctionKind::MinByV2
    ) {
        args = NativeFragmentDecodeError::map_invalid(
            path.clone().field("args"),
            pack_window_function_inputs(args, arena),
        )?;
    }
    NativeFragmentDecodeError::map_invalid(
        path.clone(),
        validate_window_function_signature(&kind, &args, &return_type, arena),
    )?;
    Ok(WindowFunctionSpec {
        kind,
        args,
        return_type,
    })
}

fn window_function_kind(
    name: &str,
    distinct: bool,
    ignore_nulls: bool,
) -> Result<WindowFunctionKind, String> {
    let base = name.split('|').next().unwrap_or(name);
    match base {
        "row_number" => Ok(WindowFunctionKind::RowNumber),
        "rank" => Ok(WindowFunctionKind::Rank),
        "dense_rank" => Ok(WindowFunctionKind::DenseRank),
        "cume_dist" => Ok(WindowFunctionKind::CumeDist),
        "percent_rank" => Ok(WindowFunctionKind::PercentRank),
        "ntile" => Ok(WindowFunctionKind::Ntile),
        "first_value" => Ok(WindowFunctionKind::FirstValue { ignore_nulls }),
        "first_value_rewrite" => Ok(WindowFunctionKind::FirstValueRewrite { ignore_nulls }),
        "last_value" => Ok(WindowFunctionKind::LastValue { ignore_nulls }),
        "lead" => Ok(WindowFunctionKind::Lead { ignore_nulls }),
        "lag" => Ok(WindowFunctionKind::Lag { ignore_nulls }),
        "session_number" => Ok(WindowFunctionKind::SessionNumber),
        "count" => Ok(WindowFunctionKind::Count),
        "sum" => Ok(WindowFunctionKind::Sum),
        "avg" => Ok(WindowFunctionKind::Avg),
        "min" => Ok(WindowFunctionKind::Min),
        "max" => Ok(WindowFunctionKind::Max),
        "bitmap_union" => Ok(WindowFunctionKind::BitmapUnion),
        "bitmap_union_count" => Ok(WindowFunctionKind::BitmapUnionCount),
        "max_by" => Ok(WindowFunctionKind::MaxBy),
        "max_by_v2" => Ok(WindowFunctionKind::MaxByV2),
        "min_by" => Ok(WindowFunctionKind::MinBy),
        "min_by_v2" => Ok(WindowFunctionKind::MinByV2),
        "var_samp" | "variance_samp" => Ok(WindowFunctionKind::VarianceSamp),
        "stddev_samp" => Ok(WindowFunctionKind::StddevSamp),
        "bool_or" | "boolor_agg" => Ok(WindowFunctionKind::BoolOr),
        "covar_pop" => Ok(WindowFunctionKind::CovarPop),
        "covar_samp" => Ok(WindowFunctionKind::CovarSamp),
        "corr" => Ok(WindowFunctionKind::Corr),
        "array_agg" | "array_agg_distinct" | "array_unique_agg" => {
            Ok(WindowFunctionKind::ArrayAgg {
                is_distinct: distinct || matches!(base, "array_agg_distinct" | "array_unique_agg"),
                is_asc_order: Vec::new(),
                nulls_first: Vec::new(),
            })
        }
        "approx_top_k" => Ok(WindowFunctionKind::ApproxTopK),
        other => Err(format!("unsupported window function: {other}")),
    }
}

fn lower_window_frame(
    frame: &expr::WindowFrame,
    path: FieldPath,
) -> Result<WindowFrame, NativeFragmentDecodeError> {
    let window_type = match expr::WindowFrameType::try_from(frame.frame_type).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone().field("frame_type"),
            format!("WindowNode unknown frame type {}", frame.frame_type),
        )
    })? {
        expr::WindowFrameType::Rows => WindowType::Rows,
        expr::WindowFrameType::Range => WindowType::Range,
        expr::WindowFrameType::Unspecified => {
            return Err(NativeFragmentDecodeError::invalid_enum(
                path.clone().field("frame_type"),
                "WindowNode frame type is unspecified",
            ));
        }
    };
    let start = match frame.start.as_ref() {
        Some(bound) => lower_window_bound(bound, true, &window_type, path.clone().field("start"))?,
        None => None,
    };
    let end = match frame.end.as_ref() {
        Some(bound) => lower_window_bound(bound, false, &window_type, path.clone().field("end"))?,
        None => None,
    };
    Ok(WindowFrame {
        start,
        end,
        window_type,
    })
}

fn lower_window_bound(
    bound: &expr::WindowBound,
    is_start: bool,
    window_type: &WindowType,
    path: FieldPath,
) -> Result<Option<WindowBoundary>, NativeFragmentDecodeError> {
    use expr::window_bound::Bound;

    let label = if is_start { "start" } else { "end" };
    let bound = bound.bound.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("bound"),
            format!("WindowNode {label} bound missing"),
        )
    })?;
    match bound {
        Bound::UnboundedPreceding(true) if is_start => Ok(None),
        Bound::UnboundedFollowing(true) if !is_start => Ok(None),
        Bound::CurrentRow(true) => Ok(Some(WindowBoundary::CurrentRow)),
        Bound::Preceding(value) => {
            if !matches!(window_type, WindowType::Rows) {
                return Err(NativeFragmentDecodeError::unsupported(
                    path.clone().field("preceding"),
                    "RANGE window boundary PRECEDING not supported",
                ));
            }
            Ok(Some(WindowBoundary::Preceding(*value)))
        }
        Bound::Following(value) => {
            if !matches!(window_type, WindowType::Rows) {
                return Err(NativeFragmentDecodeError::unsupported(
                    path.clone().field("following"),
                    "RANGE window boundary FOLLOWING not supported",
                ));
            }
            Ok(Some(WindowBoundary::Following(*value)))
        }
        Bound::UnboundedPreceding(false)
        | Bound::UnboundedFollowing(false)
        | Bound::CurrentRow(false) => Err(NativeFragmentDecodeError::invalid_value(
            path.clone(),
            format!("WindowNode {label} boolean bound marker must be true"),
        )),
        Bound::UnboundedPreceding(true) => Err(NativeFragmentDecodeError::invalid_value(
            path.clone(),
            format!("WindowNode {label} cannot be UNBOUNDED PRECEDING"),
        )),
        Bound::UnboundedFollowing(true) => Err(NativeFragmentDecodeError::invalid_value(
            path,
            format!("WindowNode {label} cannot be UNBOUNDED FOLLOWING"),
        )),
    }
}

fn validate_window_frame(
    frame: &Option<WindowFrame>,
    order_by_is_empty: bool,
) -> Result<(), String> {
    let Some(frame) = frame.as_ref() else {
        return Ok(());
    };
    if matches!(frame.window_type, WindowType::Range) {
        if frame.start.is_some() {
            return Err("RANGE window must have UNBOUNDED PRECEDING start".to_string());
        }
        if let Some(end) = frame.end.as_ref()
            && !matches!(end, WindowBoundary::CurrentRow)
        {
            return Err("RANGE window end must be CURRENT ROW or UNBOUNDED FOLLOWING".to_string());
        }
        if order_by_is_empty {
            return Err("RANGE window requires non-empty order_by_exprs".to_string());
        }
    }
    Ok(())
}

fn pack_window_function_inputs(
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
            .ok_or_else(|| "window function input type missing".to_string())?;
        if matches!(data_type, DataType::Null) {
            return Err("window function input type is null".to_string());
        }
        fields.push(Field::new(format!("f{idx}"), data_type.clone(), true));
    }
    let struct_type = DataType::Struct(Fields::from(fields));
    let struct_expr = arena.push_typed(ExprNode::StructExpr { fields: args }, struct_type);
    Ok(vec![struct_expr])
}

fn validate_window_function_signature(
    kind: &WindowFunctionKind,
    args: &[novarocks::exec::expr::ExprId],
    return_type: &DataType,
    arena: &ExprArena,
) -> Result<(), String> {
    match kind {
        WindowFunctionKind::RowNumber
        | WindowFunctionKind::Rank
        | WindowFunctionKind::DenseRank
        | WindowFunctionKind::Ntile
        | WindowFunctionKind::SessionNumber
        | WindowFunctionKind::Count => {
            if !matches!(return_type, DataType::Int64) {
                return Err(format!(
                    "window function expects Int64 return type, got {:?}",
                    return_type
                ));
            }
        }
        WindowFunctionKind::CumeDist
        | WindowFunctionKind::PercentRank
        | WindowFunctionKind::VarianceSamp
        | WindowFunctionKind::StddevSamp
        | WindowFunctionKind::CovarPop
        | WindowFunctionKind::CovarSamp
        | WindowFunctionKind::Corr => {
            if !matches!(return_type, DataType::Float64) {
                return Err(format!(
                    "window function expects Float64 return type, got {:?}",
                    return_type
                ));
            }
        }
        WindowFunctionKind::BoolOr => {
            if !matches!(return_type, DataType::Boolean) {
                return Err(format!(
                    "window function expects Boolean return type, got {:?}",
                    return_type
                ));
            }
        }
        _ => {}
    }

    match kind {
        WindowFunctionKind::RowNumber
        | WindowFunctionKind::Rank
        | WindowFunctionKind::DenseRank
        | WindowFunctionKind::CumeDist
        | WindowFunctionKind::PercentRank => {
            if !args.is_empty() {
                return Err("window function expects 0 arguments".to_string());
            }
        }
        WindowFunctionKind::Ntile => {
            if args.len() != 1 {
                return Err("ntile expects 1 argument".to_string());
            }
        }
        WindowFunctionKind::FirstValue { .. } | WindowFunctionKind::LastValue { .. } => {
            if args.len() != 1 {
                return Err("first_value/last_value expects 1 argument".to_string());
            }
            validate_window_arg_matches_return(args[0], return_type, arena)?;
        }
        WindowFunctionKind::FirstValueRewrite { .. } => {
            if !(1..=2).contains(&args.len()) {
                return Err("first_value_rewrite expects 1 or 2 arguments".to_string());
            }
            validate_window_arg_matches_return(args[0], return_type, arena)?;
        }
        WindowFunctionKind::Lead { .. } | WindowFunctionKind::Lag { .. } => {
            if !(1..=3).contains(&args.len()) {
                return Err("lead/lag expects 1 to 3 arguments".to_string());
            }
            validate_window_arg_matches_return(args[0], return_type, arena)?;
        }
        WindowFunctionKind::SessionNumber => {
            if args.len() != 2 {
                return Err("session_number expects 2 arguments".to_string());
            }
        }
        WindowFunctionKind::Count => {
            if args.len() > 1 {
                return Err("count expects 0 or 1 arguments".to_string());
            }
        }
        WindowFunctionKind::BitmapUnion | WindowFunctionKind::BitmapUnionCount => {
            if args.len() != 1 {
                return Err("bitmap_union/bitmap_union_count expects 1 argument".to_string());
            }
        }
        WindowFunctionKind::MaxBy
        | WindowFunctionKind::MaxByV2
        | WindowFunctionKind::MinBy
        | WindowFunctionKind::MinByV2 => {
            if args.len() != 1 {
                return Err(
                    "max_by/max_by_v2/min_by/min_by_v2 expects 1 packed struct argument"
                        .to_string(),
                );
            }
        }
        WindowFunctionKind::Sum
        | WindowFunctionKind::Avg
        | WindowFunctionKind::Min
        | WindowFunctionKind::Max
        | WindowFunctionKind::VarianceSamp
        | WindowFunctionKind::StddevSamp
        | WindowFunctionKind::BoolOr => {
            if args.len() != 1 {
                return Err("aggregate window function expects 1 argument".to_string());
            }
            if matches!(kind, WindowFunctionKind::Min | WindowFunctionKind::Max) {
                validate_window_arg_matches_return(args[0], return_type, arena)?;
            }
        }
        WindowFunctionKind::CovarPop | WindowFunctionKind::CovarSamp | WindowFunctionKind::Corr => {
            if args.len() != 2 {
                return Err("covar/corr window function expects 2 arguments".to_string());
            }
        }
        WindowFunctionKind::ApproxTopK => {
            if !(1..=3).contains(&args.len()) {
                return Err("approx_top_k window function expects 1 to 3 arguments".to_string());
            }
        }
        WindowFunctionKind::ArrayAgg { .. } => {
            if args.len() != 1 {
                return Err("array_agg window function expects 1 argument".to_string());
            }
            if !matches!(return_type, DataType::List(_)) {
                return Err(format!(
                    "array_agg window function expects LIST return type, got {:?}",
                    return_type
                ));
            }
        }
    }

    Ok(())
}

fn validate_window_arg_matches_return(
    arg: novarocks::exec::expr::ExprId,
    return_type: &DataType,
    arena: &ExprArena,
) -> Result<(), String> {
    let arg_type = arena
        .data_type(arg)
        .ok_or_else(|| "missing arg type in arena".to_string())?;
    if arg_type != return_type {
        return Err(format!(
            "window function return type mismatch: arg={:?} ret={:?}",
            arg_type, return_type
        ));
    }
    Ok(())
}
