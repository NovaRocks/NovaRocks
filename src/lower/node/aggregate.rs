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
use crate::exec::expr::ExprArena;
use crate::exec::node::aggregate::{AggFunction, AggTypeSignature, AggregateNode};
use crate::exec::node::{ExecNode, ExecNodeKind};

use crate::common::ids::SlotId;
use crate::lower::expr::{lower_expr_node, lower_t_expr};
use crate::lower::layout::Layout;
use crate::lower::node::Lowered;
use crate::lower::type_lowering::arrow_type_from_desc;

use crate::{exprs, plan_nodes, types};
use arrow::datatypes::{DataType, Field, Fields};

/// Lower an AGGREGATION_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_aggregate_node(
    child: Lowered,
    node: &plan_nodes::TPlanNode,
    arena: &mut ExprArena,
    query_opts: Option<&crate::internal_service::TQueryOptions>,
    out_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    let Some(agg) = node.agg_node.as_ref() else {
        return Err("AGGREGATION_NODE missing agg_node payload".to_string());
    };

    // Grouping keys
    let mut group_by =
        Vec::with_capacity(agg.grouping_exprs.as_ref().map(|v| v.len()).unwrap_or(0));
    if let Some(exprs) = &agg.grouping_exprs {
        for e in exprs {
            group_by.push(lower_t_expr(
                e,
                arena,
                &child.layout,
                last_query_id,
                fe_addr,
            )?);
        }
    }
    for expr_id in &group_by {
        if let Some(dt) = arena.data_type(*expr_id) {
            if matches!(dt, arrow::datatypes::DataType::LargeBinary) {
                return Err("VARIANT is not supported in GROUP BY".to_string());
            }
        }
    }

    // Agg functions
    let mut functions = Vec::new();
    for e in &agg.aggregate_functions {
        let root = e.nodes.get(0).ok_or_else(|| "empty agg expr".to_string())?;
        let is_merge = root
            .agg_expr
            .as_ref()
            .map(|agg_expr| agg_expr.is_merge_agg)
            .unwrap_or(false);
        let fn_name_raw = root
            .fn_
            .as_ref()
            .map(|f| f.name.function_name.to_lowercase())
            .ok_or_else(|| "agg expr missing function name".to_string())?;
        let fn_name = encode_aggregate_name(root, &fn_name_raw, query_opts)?;
        let type_sig = agg_type_signature_from_node(root)?;

        // Lower arguments
        let mut args = Vec::new();
        let mut idx = 1; // Skip root
        for _ in 0..root.num_children {
            args.push(lower_expr_node(
                &e.nodes,
                &mut idx,
                arena,
                &child.layout,
                last_query_id,
                fe_addr,
            )?);
        }

        let inputs = select_aggregate_inputs(&fn_name_raw, is_merge, args, arena)?;
        let func = AggFunction {
            name: fn_name.clone(),
            inputs,
            input_is_intermediate: is_merge,
            types: Some(type_sig),
        };
        functions.push(func);
    }
    let input_is_intermediate = functions.iter().all(|f| f.input_is_intermediate);

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                group_by,
                functions,
                need_finalize: agg.need_finalize,
                input_is_intermediate,
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

fn agg_type_signature_from_node(node: &exprs::TExprNode) -> Result<AggTypeSignature, String> {
    let fn_ = node
        .fn_
        .as_ref()
        .ok_or_else(|| "agg expr missing function".to_string())?;
    let intermediate_type = fn_
        .aggregate_fn
        .as_ref()
        .and_then(|agg_fn| arrow_type_from_desc(&agg_fn.intermediate_type));
    let output_type = arrow_type_from_desc(&fn_.ret_type)
        .ok_or_else(|| "agg ret_type missing/unsupported".to_string())?;
    let input_arg_type = fn_.arg_types.first().and_then(arrow_type_from_desc);
    Ok(AggTypeSignature {
        intermediate_type,
        output_type: Some(output_type),
        input_arg_type,
    })
}

fn encode_aggregate_name(
    node: &exprs::TExprNode,
    fn_name: &str,
    query_opts: Option<&crate::internal_service::TQueryOptions>,
) -> Result<String, String> {
    if fn_name == "array_agg" {
        let aggregate_fn = node.fn_.as_ref().and_then(|f| f.aggregate_fn.as_ref());
        let is_distinct = aggregate_fn
            .and_then(|agg| agg.is_distinct)
            .unwrap_or(false);
        let base = if is_distinct {
            "array_agg_distinct"
        } else {
            "array_agg"
        };
        let is_asc_order = aggregate_fn
            .and_then(|agg| agg.is_asc_order.clone())
            .unwrap_or_default();
        let nulls_first = aggregate_fn
            .and_then(|agg| agg.nulls_first.clone())
            .unwrap_or_default();
        if is_asc_order.len() != nulls_first.len() {
            return Err(format!(
                "array_agg order metadata length mismatch: is_asc_order={} nulls_first={}",
                is_asc_order.len(),
                nulls_first.len()
            ));
        }
        if is_asc_order.is_empty() {
            return Ok(base.to_string());
        }
        let asc = encode_bool_list(&is_asc_order);
        let nulls = encode_bool_list(&nulls_first);
        return Ok(format!("{base}|a={asc}|n={nulls}"));
    }

    if fn_name != "group_concat" {
        return Ok(fn_name.to_string());
    }

    let aggregate_fn = node.fn_.as_ref().and_then(|f| f.aggregate_fn.as_ref());
    let is_distinct = aggregate_fn
        .and_then(|agg| agg.is_distinct)
        .unwrap_or(false);
    let is_asc_order = aggregate_fn
        .and_then(|agg| agg.is_asc_order.clone())
        .unwrap_or_default();
    let nulls_first = aggregate_fn
        .and_then(|agg| agg.nulls_first.clone())
        .unwrap_or_default();
    if is_asc_order.len() != nulls_first.len() {
        return Err(format!(
            "group_concat order metadata length mismatch: is_asc_order={} nulls_first={}",
            is_asc_order.len(),
            nulls_first.len()
        ));
    }

    let asc = encode_bool_list(&is_asc_order);
    let nulls = encode_bool_list(&nulls_first);
    let group_concat_max_len = query_opts
        .and_then(|opts| opts.group_concat_max_len)
        .unwrap_or(1024)
        .max(4);
    Ok(format!(
        "group_concat|d={}|a={}|n={}|m={}",
        if is_distinct { 1 } else { 0 },
        asc,
        nulls,
        group_concat_max_len
    ))
}

fn encode_bool_list(v: &[bool]) -> String {
    v.iter()
        .map(|b| if *b { "1" } else { "0" })
        .collect::<Vec<_>>()
        .join(",")
}

fn select_aggregate_inputs(
    fn_name: &str,
    is_merge: bool,
    args: Vec<crate::exec::expr::ExprId>,
    arena: &mut ExprArena,
) -> Result<Vec<crate::exec::expr::ExprId>, String> {
    let select_first_for_merge = |args: Vec<crate::exec::expr::ExprId>,
                                  name: &str|
     -> Result<Vec<crate::exec::expr::ExprId>, String> {
        let first = args
            .into_iter()
            .next()
            .ok_or_else(|| format!("{name} merge input missing"))?;
        Ok(vec![first])
    };

    match fn_name {
        // FE rewrites count_if(expr) to count_if(1, expr). Keep only the effective input:
        // predicate for update, intermediate count for merge.
        "count_if" => {
            if is_merge {
                let first = args
                    .into_iter()
                    .next()
                    .ok_or_else(|| "count_if merge input missing".to_string())?;
                return Ok(vec![first]);
            }
            if args.len() == 1 {
                return Ok(args);
            }
            if args.len() == 2 {
                let mut it = args.into_iter();
                let _ = it.next();
                let predicate = it
                    .next()
                    .ok_or_else(|| "count_if predicate input missing".to_string())?;
                return Ok(vec![predicate]);
            }
            return Err(format!(
                "count_if expects 1 or 2 arguments, got {}",
                args.len()
            ));
        }
        // FE may still keep constant arguments when building merge-stage aggregate calls.
        // These aggregates only consume the first intermediate state argument during merge.
        "count_distinct"
        | "multi_distinct_count"
        | "ds_theta_count_distinct"
        | "approx_count_distinct_hll_sketch"
            if is_merge =>
        {
            return select_first_for_merge(args, "count_distinct");
        }
        // Merge group_concat consumes intermediate state; FE may still carry separator in args.
        "group_concat" if is_merge => {
            return select_first_for_merge(args, "group_concat");
        }
        // Merge array_agg consumes intermediate state only.
        "array_agg" | "array_unique_agg" if is_merge => {
            return select_first_for_merge(args, fn_name);
        }
        // Merge approx_top_k consumes intermediate binary state only.
        "approx_top_k" if is_merge => {
            return select_first_for_merge(args, "approx_top_k");
        }
        // Merge dict_merge consumes intermediate state; FE may still carry threshold in args.
        "dict_merge" if is_merge => {
            return select_first_for_merge(args, "dict_merge");
        }
        "mann_whitney_u_test" | "percentile_cont" | "percentile_disc" | "percentile_disc_lc"
            if is_merge =>
        {
            return select_first_for_merge(args, fn_name);
        }
        _ => {}
    }

    pack_struct_inputs(args, arena)
}

fn pack_struct_inputs(
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
            .ok_or_else(|| "aggregate input type missing".to_string())?;
        if matches!(data_type, DataType::Null) {
            return Err("aggregate input type is null".to_string());
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
