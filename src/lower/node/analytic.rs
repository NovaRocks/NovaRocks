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

use arrow::datatypes::{DataType, Field, Fields};

use crate::common::ids::SlotId;
use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
use crate::exec::node::analytic::{
    AnalyticNode, AnalyticOutputColumn, WindowBoundary, WindowFrame, WindowFunctionKind,
    WindowFunctionSpec, WindowType,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::expr::{lower_expr_node, lower_t_expr};
use crate::lower::layout::Layout;
use crate::lower::node::Lowered;
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::{exprs, plan_nodes, types};

pub(crate) fn lower_analytic_node(
    child: Lowered,
    node: &plan_nodes::TPlanNode,
    arena: &mut ExprArena,
    out_layout: &Layout,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    let Some(analytic) = node.analytic_node.as_ref() else {
        return Err("ANALYTIC_EVAL_NODE missing analytic_node payload".to_string());
    };

    // Lower partition/order expressions against child layout.
    let mut partition_exprs = Vec::with_capacity(analytic.partition_exprs.len());
    for e in &analytic.partition_exprs {
        partition_exprs.push(lower_t_expr(
            e,
            arena,
            &child.layout,
            last_query_id,
            fe_addr,
        )?);
    }
    let mut order_by_exprs = Vec::with_capacity(analytic.order_by_exprs.len());
    for e in &analytic.order_by_exprs {
        order_by_exprs.push(lower_t_expr(
            e,
            arena,
            &child.layout,
            last_query_id,
            fe_addr,
        )?);
    }

    let mut functions = Vec::with_capacity(analytic.analytic_functions.len());
    for e in &analytic.analytic_functions {
        functions.push(lower_window_function(
            e,
            arena,
            &child.layout,
            last_query_id,
            fe_addr,
        )?);
    }

    let window = analytic
        .window
        .as_ref()
        .map(lower_window_frame)
        .transpose()?;

    // Validate RANGE window constraints aligned with StarRocks BE.
    if let Some(w) = window.as_ref() {
        if matches!(w.window_type, WindowType::Range) {
            if w.start.is_some() {
                return Err("RANGE window must have UNBOUNDED PRECEDING start".to_string());
            }
            if let Some(end) = w.end.as_ref() {
                if !matches!(end, WindowBoundary::CurrentRow) {
                    return Err(
                        "RANGE window end must be CURRENT ROW or UNBOUNDED FOLLOWING".to_string(),
                    );
                }
            }
            if order_by_exprs.is_empty() {
                return Err("RANGE window requires non-empty order_by_exprs".to_string());
            }
        }
    }

    let output_tuple_id = analytic.output_tuple_id;
    let output_slot_ids = tuple_slots
        .get(&output_tuple_id)
        .ok_or_else(|| format!("missing tuple_slots for output_tuple_id={output_tuple_id}"))?;
    if output_slot_ids.len() != functions.len() {
        return Err(format!(
            "analytic output tuple slots mismatch: output_tuple_id={} slots={} functions={}",
            output_tuple_id,
            output_slot_ids.len(),
            functions.len()
        ));
    }

    let mut slot_to_func: HashMap<types::TSlotId, usize> = HashMap::new();
    for (i, slot_id) in output_slot_ids.iter().enumerate() {
        slot_to_func.insert(*slot_id, i);
    }

    let child_slot_ids: std::collections::HashSet<types::TSlotId> = child
        .layout
        .order
        .iter()
        .map(|(_, slot_id)| *slot_id)
        .collect();

    // Build output column plan so physical output order matches `out_layout`.
    let mut output_columns = Vec::with_capacity(out_layout.order.len());
    for (tuple_id, slot_id) in &out_layout.order {
        if *tuple_id == output_tuple_id {
            let func_idx = slot_to_func.get(slot_id).copied().ok_or_else(|| {
                format!(
                    "analytic output layout refers to unknown output slot: tuple_id={} slot_id={}",
                    tuple_id, slot_id
                )
            })?;
            output_columns.push(AnalyticOutputColumn::Window(func_idx));
            continue;
        }
        if child_slot_ids.contains(slot_id) {
            output_columns.push(AnalyticOutputColumn::InputSlotId(SlotId::try_from(
                *slot_id,
            )?));
            continue;
        }
        return Err(format!(
            "analytic output layout refers to unknown column: tuple_id={} slot_id={}",
            tuple_id, slot_id
        ));
    }

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Analytic(AnalyticNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                partition_exprs,
                order_by_exprs,
                functions,
                window,
                output_columns,
                output_slots: out_layout
                    .order
                    .iter()
                    .map(|(_, slot_id)| SlotId::try_from(*slot_id))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
        },
        layout: out_layout.clone(),
    })
}

fn lower_window_frame(w: &plan_nodes::TAnalyticWindow) -> Result<WindowFrame, String> {
    let window_type = match w.type_ {
        plan_nodes::TAnalyticWindowType::RANGE => WindowType::Range,
        plan_nodes::TAnalyticWindowType::ROWS => WindowType::Rows,
        other => return Err(format!("unknown analytic window type: {:?}", other)),
    };
    let start = w
        .window_start
        .as_ref()
        .map(|b| lower_window_boundary(b, window_type.clone()))
        .transpose()?;
    let end = w
        .window_end
        .as_ref()
        .map(|b| lower_window_boundary(b, window_type.clone()))
        .transpose()?;
    Ok(WindowFrame {
        start,
        end,
        window_type,
    })
}

fn lower_window_boundary(
    b: &plan_nodes::TAnalyticWindowBoundary,
    window_type: WindowType,
) -> Result<WindowBoundary, String> {
    match b.type_ {
        plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW => Ok(WindowBoundary::CurrentRow),
        plan_nodes::TAnalyticWindowBoundaryType::PRECEDING => {
            let Some(v) = b.rows_offset_value else {
                return Err("window boundary PRECEDING missing rows_offset_value".to_string());
            };
            if !matches!(window_type, WindowType::Rows) {
                return Err("RANGE window boundary PRECEDING not supported".to_string());
            }
            Ok(WindowBoundary::Preceding(v))
        }
        plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING => {
            let Some(v) = b.rows_offset_value else {
                return Err("window boundary FOLLOWING missing rows_offset_value".to_string());
            };
            if !matches!(window_type, WindowType::Rows) {
                return Err("RANGE window boundary FOLLOWING not supported".to_string());
            }
            Ok(WindowBoundary::Following(v))
        }
        other => Err(format!(
            "unknown analytic window boundary type: {:?}",
            other
        )),
    }
}

fn lower_window_function(
    e: &exprs::TExpr,
    arena: &mut ExprArena,
    layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<WindowFunctionSpec, String> {
    let root = e
        .nodes
        .get(0)
        .ok_or_else(|| "empty analytic function expr".to_string())?;
    let fn_ = root
        .fn_
        .as_ref()
        .ok_or_else(|| "analytic function missing fn".to_string())?;
    let fn_name = fn_.name.function_name.to_lowercase();
    let fn_base = fn_name.split('|').next().unwrap_or(fn_name.as_str());
    let ignore_nulls = fn_.ignore_nulls.unwrap_or(false);

    let kind = match fn_base {
        "row_number" => WindowFunctionKind::RowNumber,
        "rank" => WindowFunctionKind::Rank,
        "dense_rank" => WindowFunctionKind::DenseRank,
        "cume_dist" => WindowFunctionKind::CumeDist,
        "percent_rank" => WindowFunctionKind::PercentRank,
        "ntile" => WindowFunctionKind::Ntile,
        "first_value" => WindowFunctionKind::FirstValue { ignore_nulls },
        "first_value_rewrite" => WindowFunctionKind::FirstValueRewrite { ignore_nulls },
        "last_value" => WindowFunctionKind::LastValue { ignore_nulls },
        "lead" => WindowFunctionKind::Lead { ignore_nulls },
        "lag" => WindowFunctionKind::Lag { ignore_nulls },
        "session_number" => WindowFunctionKind::SessionNumber,
        "count" => WindowFunctionKind::Count,
        "sum" => WindowFunctionKind::Sum,
        "avg" => WindowFunctionKind::Avg,
        "min" => WindowFunctionKind::Min,
        "max" => WindowFunctionKind::Max,
        "bitmap_union" => WindowFunctionKind::BitmapUnion,
        "min_by" => WindowFunctionKind::MinBy,
        "min_by_v2" => WindowFunctionKind::MinByV2,
        "var_samp" | "variance_samp" => WindowFunctionKind::VarianceSamp,
        "stddev_samp" => WindowFunctionKind::StddevSamp,
        "bool_or" | "boolor_agg" => WindowFunctionKind::BoolOr,
        "covar_pop" => WindowFunctionKind::CovarPop,
        "covar_samp" => WindowFunctionKind::CovarSamp,
        "corr" => WindowFunctionKind::Corr,
        "array_agg" | "array_agg_distinct" | "array_unique_agg" => {
            let aggregate_fn = fn_.aggregate_fn.as_ref();
            let (name_is_asc_order, name_nulls_first) = parse_array_agg_name_metadata(&fn_name)?;
            let is_distinct = match fn_base {
                "array_agg_distinct" | "array_unique_agg" => true,
                _ => aggregate_fn
                    .and_then(|agg| agg.is_distinct)
                    .unwrap_or(false),
            };
            let (is_asc_order, nulls_first) =
                if !name_is_asc_order.is_empty() || !name_nulls_first.is_empty() {
                    (name_is_asc_order, name_nulls_first)
                } else {
                    (
                        aggregate_fn
                            .and_then(|agg| agg.is_asc_order.clone())
                            .unwrap_or_default(),
                        aggregate_fn
                            .and_then(|agg| agg.nulls_first.clone())
                            .unwrap_or_default(),
                    )
                };
            if is_asc_order.len() != nulls_first.len() {
                return Err(format!(
                    "array_agg order metadata length mismatch: is_asc_order={} nulls_first={}",
                    is_asc_order.len(),
                    nulls_first.len()
                ));
            }
            WindowFunctionKind::ArrayAgg {
                is_distinct,
                is_asc_order,
                nulls_first,
            }
        }
        "approx_top_k" => WindowFunctionKind::ApproxTopK,
        other => {
            return Err(format!("unsupported window function: {}", other));
        }
    };

    let return_type = arrow_type_from_desc(&root.type_)
        .or_else(|| arrow_type_from_desc(&fn_.ret_type))
        .ok_or_else(|| {
            format!(
                "unsupported window function return type: node={:?} fn={:?}",
                root.type_, fn_.ret_type
            )
        })?;

    let mut args = Vec::new();
    let mut idx = 1usize;
    for _ in 0..root.num_children {
        args.push(lower_expr_node(
            &e.nodes,
            &mut idx,
            arena,
            layout,
            last_query_id,
            fe_addr,
        )?);
    }
    if matches!(
        kind,
        WindowFunctionKind::ArrayAgg { .. }
            | WindowFunctionKind::MinBy
            | WindowFunctionKind::MinByV2
    ) {
        args = pack_window_function_inputs(args, arena)?;
    }

    validate_window_function_signature(&kind, &args, &return_type, arena)?;

    Ok(WindowFunctionSpec {
        kind,
        args,
        return_type,
    })
}

fn parse_array_agg_name_metadata(name: &str) -> Result<(Vec<bool>, Vec<bool>), String> {
    if !name.contains('|') {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut is_asc_order = Vec::new();
    let mut nulls_first = Vec::new();
    for token in name.split('|').skip(1) {
        let (k, v) = token
            .split_once('=')
            .ok_or_else(|| format!("array_agg metadata token missing '=': {}", token))?;
        match k {
            "a" => is_asc_order = parse_bool_list(v)?,
            "n" => nulls_first = parse_bool_list(v)?,
            other => {
                return Err(format!("array_agg metadata key '{}' is unsupported", other));
            }
        }
    }
    if is_asc_order.len() != nulls_first.len() {
        return Err(format!(
            "array_agg metadata length mismatch: is_asc_order={} nulls_first={}",
            is_asc_order.len(),
            nulls_first.len()
        ));
    }
    Ok((is_asc_order, nulls_first))
}

fn parse_bool_list(v: &str) -> Result<Vec<bool>, String> {
    if v.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for token in v.split(',') {
        out.push(parse_bool_flag(token)?);
    }
    Ok(out)
}

fn parse_bool_flag(v: &str) -> Result<bool, String> {
    match v {
        "1" | "true" | "TRUE" => Ok(true),
        "0" | "false" | "FALSE" => Ok(false),
        _ => Err(format!("array_agg metadata bool is invalid: {}", v)),
    }
}

fn pack_window_function_inputs(
    args: Vec<crate::exec::expr::ExprId>,
    arena: &mut ExprArena,
) -> Result<Vec<crate::exec::expr::ExprId>, String> {
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
    let struct_expr = arena.push_typed(
        crate::exec::expr::ExprNode::StructExpr { fields: args },
        struct_type,
    );
    Ok(vec![struct_expr])
}

fn validate_window_function_signature(
    kind: &WindowFunctionKind,
    args: &[crate::exec::expr::ExprId],
    return_type: &DataType,
    arena: &ExprArena,
) -> Result<(), String> {
    let ret_is_i64 = matches!(return_type, DataType::Int64);
    let ret_is_f64 = matches!(return_type, DataType::Float64);

    match kind {
        WindowFunctionKind::RowNumber
        | WindowFunctionKind::Rank
        | WindowFunctionKind::DenseRank
        | WindowFunctionKind::Ntile
        | WindowFunctionKind::SessionNumber
        | WindowFunctionKind::Count => {
            if !ret_is_i64 {
                return Err(format!(
                    "window function expects Int64 return type, got {:?}",
                    return_type
                ));
            }
        }
        WindowFunctionKind::CumeDist | WindowFunctionKind::PercentRank => {
            if !ret_is_f64 {
                return Err(format!(
                    "window function expects Float64 return type, got {:?}",
                    return_type
                ));
            }
        }
        WindowFunctionKind::VarianceSamp
        | WindowFunctionKind::StddevSamp
        | WindowFunctionKind::CovarPop
        | WindowFunctionKind::CovarSamp
        | WindowFunctionKind::Corr => {
            if !ret_is_f64 {
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
            let arg0_type = arena
                .data_type(args[0])
                .ok_or_else(|| "missing arg type in arena".to_string())?;
            if arg0_type != return_type {
                return Err(format!(
                    "window function return type mismatch: arg={:?} ret={:?}",
                    arg0_type, return_type
                ));
            }
        }
        WindowFunctionKind::FirstValueRewrite { .. } => {
            if !(1..=2).contains(&args.len()) {
                return Err("first_value_rewrite expects 1 or 2 arguments".to_string());
            }
            let arg0_type = arena
                .data_type(args[0])
                .ok_or_else(|| "missing arg type in arena".to_string())?;
            if arg0_type != return_type {
                return Err(format!(
                    "window function return type mismatch: arg={:?} ret={:?}",
                    arg0_type, return_type
                ));
            }
            if args.len() == 2 {
                // second arg must be integer literal
                let node = arena
                    .node(args[1])
                    .ok_or_else(|| "invalid ExprId".to_string())?;
                match node {
                    ExprNode::Literal(LiteralValue::Int64(_))
                    | ExprNode::Literal(LiteralValue::Int32(_)) => {}
                    _ => {
                        return Err(
                            "first_value_rewrite pad value must be integer literal".to_string()
                        );
                    }
                }
            }
        }
        WindowFunctionKind::Lead { .. } | WindowFunctionKind::Lag { .. } => {
            if !(1..=3).contains(&args.len()) {
                return Err("lead/lag expects 1 to 3 arguments".to_string());
            }
            let arg0_type = arena
                .data_type(args[0])
                .ok_or_else(|| "missing arg type in arena".to_string())?;
            if arg0_type != return_type {
                return Err(format!(
                    "window function return type mismatch: arg={:?} ret={:?}",
                    arg0_type, return_type
                ));
            }
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
        WindowFunctionKind::BitmapUnion => {
            if args.len() != 1 {
                return Err("bitmap_union expects 1 argument".to_string());
            }
        }
        WindowFunctionKind::MinBy | WindowFunctionKind::MinByV2 => {
            if args.len() != 1 {
                return Err("min_by/min_by_v2 expects 1 packed struct argument".to_string());
            }
            let arg0_type = arena
                .data_type(args[0])
                .ok_or_else(|| "missing arg type in arena".to_string())?;
            let DataType::Struct(fields) = arg0_type else {
                return Err(format!(
                    "min_by/min_by_v2 expects packed struct input, got {:?}",
                    arg0_type
                ));
            };
            if fields.len() != 2 {
                return Err(format!(
                    "min_by/min_by_v2 expects 2 struct fields, got {}",
                    fields.len()
                ));
            }
            if fields[0].data_type() != return_type {
                return Err(format!(
                    "min_by/min_by_v2 return type mismatch: arg={:?} ret={:?}",
                    fields[0].data_type(),
                    return_type
                ));
            }
        }
        WindowFunctionKind::Sum
        | WindowFunctionKind::Avg
        | WindowFunctionKind::Min
        | WindowFunctionKind::Max => {
            if args.len() != 1 {
                return Err("aggregate window function expects 1 argument".to_string());
            }
            if matches!(kind, WindowFunctionKind::Min | WindowFunctionKind::Max) {
                let arg0_type = arena
                    .data_type(args[0])
                    .ok_or_else(|| "missing arg type in arena".to_string())?;
                if arg0_type != return_type {
                    return Err(format!(
                        "min/max return type mismatch: arg={:?} ret={:?}",
                        arg0_type, return_type
                    ));
                }
            }
        }
        WindowFunctionKind::VarianceSamp
        | WindowFunctionKind::StddevSamp
        | WindowFunctionKind::BoolOr => {
            if args.len() != 1 {
                return Err("aggregate window function expects 1 argument".to_string());
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
        WindowFunctionKind::ArrayAgg {
            is_asc_order,
            nulls_first,
            ..
        } => {
            if args.len() != 1 {
                return Err("array_agg window function expects 1 packed argument".to_string());
            }
            if is_asc_order.len() != nulls_first.len() {
                return Err(format!(
                    "array_agg order metadata length mismatch: is_asc_order={} nulls_first={}",
                    is_asc_order.len(),
                    nulls_first.len()
                ));
            }
            let DataType::List(list_item) = return_type else {
                return Err(format!(
                    "array_agg window function expects LIST return type, got {:?}",
                    return_type
                ));
            };
            let arg0_type = arena
                .data_type(args[0])
                .ok_or_else(|| "missing arg type in arena".to_string())?;
            match arg0_type {
                DataType::Struct(fields) => {
                    let value_field = fields.first().ok_or_else(|| {
                        "array_agg packed input struct must contain at least one field".to_string()
                    })?;
                    if value_field.data_type() != list_item.data_type() {
                        return Err(format!(
                            "array_agg return type mismatch: arg={:?} ret_item={:?}",
                            value_field.data_type(),
                            list_item.data_type()
                        ));
                    }
                    if fields.len() < (1 + is_asc_order.len()) {
                        return Err(format!(
                            "array_agg packed input field count mismatch: fields={} order_keys={}",
                            fields.len(),
                            is_asc_order.len()
                        ));
                    }
                }
                other => {
                    if !is_asc_order.is_empty() {
                        return Err(
                            "array_agg ORDER BY requires packed struct input with order keys"
                                .to_string(),
                        );
                    }
                    if other != list_item.data_type() {
                        return Err(format!(
                            "array_agg return type mismatch: arg={:?} ret_item={:?}",
                            other,
                            list_item.data_type()
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}
