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
use arrow::datatypes::DataType;

use crate::exec::expr::ExprArena;
use crate::exec::expr::ExprNode;
use crate::exec::node::project::ProjectNode;
use crate::exec::node::sort::{SortExpression, SortNode, SortTopNType};
use crate::exec::node::{ExecNode, ExecNodeKind};

use crate::common::ids::SlotId;
use crate::lower::expr::lower_t_expr;
use crate::lower::layout::{Layout, chunk_schema_for_layout};
use crate::lower::node::Lowered;
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::thrift::descriptors;

use crate::thrift::{exprs, plan_nodes, types};

/// Lower a SORT_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_sort_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    arena: &mut ExprArena,
    out_layout: Layout,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    if children.len() != 1 {
        return Err(format!(
            "SORT_NODE expected 1 child, got {}",
            children.len()
        ));
    }
    let child = children.into_iter().next().expect("child");
    let Some(sort) = node.sort_node.as_ref() else {
        return Err("SORT_NODE missing sort_node payload".to_string());
    };
    let info = &sort.sort_info;

    // StarRocks' `sort_tuple_slot_exprs` is used to materialize an internal tuple for sorting.
    // It should not change the Sort node's visible output columns.
    //
    // FE may assign a new output tuple_id and/or reorder the output slots for a Sort node.
    // novarocks's Sort operator does not reorder columns by itself, so we insert a Project to
    // permute the child columns to match `out_layout` when needed.
    let (child_for_sort, sort_input_layout, sort_output_layout) = normalize_sort_input(
        child,
        arena,
        &out_layout,
        node.node_id,
        desc_tbl,
        sort,
        last_query_id,
        fe_addr,
    )?;

    let order_by = build_sort_order_by(
        info,
        arena,
        &sort_input_layout,
        &format!("SORT_NODE node_id={}", node.node_id),
        last_query_id,
        fe_addr,
    )?;

    let limit = if node.limit >= 0 {
        Some(node.limit as usize)
    } else {
        None
    };

    let offset = match sort.offset.unwrap_or(0) {
        v if v < 0 => {
            return Err(format!("SORT_NODE offset must be >= 0, got {v}"));
        }
        v => v as usize,
    };

    let use_top_n = sort.use_top_n;
    let topn_type = parse_sort_topn_type(sort, node.node_id)?;
    // StarRocks enforces `offset == 0` for rank-based topn semantics.
    // Keep the same invariant so execution does not need fallback behavior.
    if use_top_n && topn_type != SortTopNType::RowNumber && offset != 0 {
        return Err(format!(
            "SORT_NODE node_id={} topn_type {:?} requires offset=0, got {}",
            node.node_id, topn_type, offset
        ));
    }

    let max_buffered_rows =
        parse_optional_positive_i64(sort.max_buffered_rows, node.node_id, "max_buffered_rows")?;
    let max_buffered_bytes =
        parse_optional_positive_i64(sort.max_buffered_bytes, node.node_id, "max_buffered_bytes")?;

    // Per-partition TopN (StarRocks PartitionSort). partition_exprs are grouping
    // keys; partition_limit caps rows per group. Compiled like ordering exprs but
    // grouping-only — we mark them asc/nulls_first so exec makes groups adjacent.
    let partition_exprs = match sort.partition_exprs.as_ref() {
        None => Vec::new(),
        Some(exprs) => {
            let mut out = Vec::with_capacity(exprs.len());
            for e in exprs {
                let expr_id = lower_t_expr(e, arena, &sort_input_layout, last_query_id, fe_addr)?;
                out.push(SortExpression {
                    expr: expr_id,
                    asc: true,
                    nulls_first: true,
                });
            }
            out
        }
    };
    let partition_limit = match sort.partition_limit {
        None => None,
        // StarRocks uses partition_limit = -1 as the "no per-partition cap"
        // sentinel. It emits the field whenever partition_exprs is present
        // (SortNode.java setPartition_limit is guarded by getPartitionExprs() !=
        // null), so an ordinary global TopN still arrives with partition_limit =
        // -1 and an empty partition_exprs list. Treat any negative value as
        // absent rather than rejecting it — StarRocks itself gates on
        // `partitionLimit >= 0`. Rejecting it broke every FE-issued
        // `ORDER BY ... LIMIT n`.
        Some(v) if v < 0 => None,
        Some(v) => Some(v as usize),
    };
    if partition_limit.is_some() && !use_top_n {
        return Err(format!(
            "SORT_NODE node_id={} partition_limit requires use_top_n=true",
            node.node_id
        ));
    }
    // Partition-TopN is intentionally decoupled from the global limit: the per-partition
    // cap (partition_limit) replaces the global row cap, so use_top_n=true is valid even
    // when there is no global limit. Only reject the combination when BOTH partition_limit
    // and global limit are absent — that would be an unconstrained TopN with no limit at all.
    if use_top_n && limit.is_none() && partition_limit.is_none() {
        return Err(format!(
            "SORT_NODE node_id={} use_top_n=true requires node.limit >= 0",
            node.node_id
        ));
    }

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Sort(SortNode {
                input: Box::new(child_for_sort),
                node_id: node.node_id,
                use_top_n,
                order_by,
                limit,
                offset,
                topn_type,
                max_buffered_rows,
                max_buffered_bytes,
                partition_exprs,
                partition_limit,
            }),
        },
        layout: sort_output_layout,
    })
}

fn parse_sort_topn_type(
    sort: &plan_nodes::TSortNode,
    node_id: i32,
) -> Result<SortTopNType, String> {
    let Some(topn_type) = sort.topn_type else {
        return Ok(SortTopNType::RowNumber);
    };
    // Keep explicit mapping for `DENSE_RANK` even though current StarRocks FE
    // ranking-window pushdown normally emits ROW_NUMBER/RANK only.
    match topn_type {
        plan_nodes::TTopNType::ROW_NUMBER => Ok(SortTopNType::RowNumber),
        plan_nodes::TTopNType::RANK => Ok(SortTopNType::Rank),
        plan_nodes::TTopNType::DENSE_RANK => Ok(SortTopNType::DenseRank),
        other => Err(format!(
            "SORT_NODE node_id={} has unknown topn_type value {}",
            node_id, other.0
        )),
    }
}

fn parse_optional_positive_i64(
    value: Option<i64>,
    node_id: i32,
    field_name: &str,
) -> Result<Option<usize>, String> {
    let Some(v) = value else {
        return Ok(None);
    };
    if v <= 0 {
        return Err(format!(
            "SORT_NODE node_id={} {} must be > 0 when set, got {}",
            node_id, field_name, v
        ));
    }
    Ok(Some(v as usize))
}

fn build_sort_order_by(
    info: &plan_nodes::TSortInfo,
    arena: &mut ExprArena,
    input_layout: &Layout,
    node_label: &str,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SortExpression>, String> {
    let key_count = info.ordering_exprs.len();
    if info.is_asc_order.len() != key_count {
        return Err(format!(
            "{node_label} sort_info.is_asc_order length mismatch: ordering_exprs={} is_asc_order={}",
            key_count,
            info.is_asc_order.len()
        ));
    }
    if info.nulls_first.len() != key_count {
        return Err(format!(
            "{node_label} sort_info.nulls_first length mismatch: ordering_exprs={} nulls_first={}",
            key_count,
            info.nulls_first.len()
        ));
    }

    let mut order_by = Vec::with_capacity(key_count);
    for (i, expr) in info.ordering_exprs.iter().enumerate() {
        let expr_id = lower_t_expr(expr, arena, input_layout, last_query_id, fe_addr)?;
        order_by.push(SortExpression {
            expr: expr_id,
            asc: info.is_asc_order[i],
            nulls_first: info.nulls_first[i],
        });
    }
    Ok(order_by)
}

fn normalize_sort_input(
    child: Lowered,
    arena: &mut ExprArena,
    out_layout: &Layout,
    node_id: i32,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    sort: &plan_nodes::TSortNode,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<(ExecNode, Layout, Layout), String> {
    let effective_out_layout = normalize_sort_output_layout(&child.layout, out_layout)?;
    let original_child_layout = child.layout.clone();
    let mut child = child;

    if child.layout.order.len() != effective_out_layout.order.len() {
        child = build_sort_tuple_projection(
            child,
            arena,
            sort,
            &effective_out_layout,
            node_id,
            desc_tbl,
            last_query_id,
            fe_addr,
        )?;
    }

    if child.layout.order == effective_out_layout.order {
        let sort_input_layout = add_slot_aliases(child.layout.clone(), &original_child_layout);
        return Ok((child.node, sort_input_layout, effective_out_layout));
    }

    // Map output slot_id -> child physical index. We require a 1:1 mapping to avoid guessing.
    let mut child_slot_set = std::collections::HashSet::<types::TSlotId>::new();
    for (_t, slot_id) in &child.layout.order {
        if !child_slot_set.insert(*slot_id) {
            return Err(format!(
                "SORT_NODE child layout has duplicate slot_id={}, cannot build a stable mapping",
                slot_id
            ));
        }
    }

    // Build a project that reorders columns into `out_layout` order using slot ids.
    let mut exprs = Vec::with_capacity(effective_out_layout.order.len());
    for (tuple_id, slot_id) in &effective_out_layout.order {
        if !child_slot_set.contains(slot_id) {
            return Err(format!(
                "SORT_NODE output layout refers to missing child slot: tuple_id={} slot_id={}",
                tuple_id, slot_id
            ));
        }
        let slot_id = SlotId::try_from(*slot_id)?;
        exprs.push(arena.push(ExprNode::SlotId(slot_id)));
    }

    let projected = ExecNode {
        kind: ExecNodeKind::Project(ProjectNode {
            input: Box::new(child.node),
            node_id,
            is_subordinate: true,
            exprs,
            expr_slot_ids: effective_out_layout
                .order
                .iter()
                .map(|(_, slot_id)| SlotId::try_from(*slot_id))
                .collect::<Result<Vec<_>, _>>()?,
            expr_slot_schemas: None,
            output_indices: None,
            output_chunk_schema: chunk_schema_for_layout(
                desc_tbl.ok_or_else(|| {
                    "SORT_NODE requires desc_tbl for projection chunk schema".to_string()
                })?,
                &effective_out_layout,
            )?,
        }),
    };

    // Expressions (e.g. ordering_exprs) may still reference the original child tuple_id.
    // Add alias entries so slot refs can be resolved deterministically without falling back.
    let mut sort_input_layout = effective_out_layout.clone();
    sort_input_layout = add_slot_aliases(sort_input_layout, &child.layout);
    sort_input_layout = add_slot_aliases(sort_input_layout, &original_child_layout);

    Ok((projected, sort_input_layout, effective_out_layout))
}

fn build_sort_tuple_projection(
    child: Lowered,
    arena: &mut ExprArena,
    sort: &plan_nodes::TSortNode,
    out_layout: &Layout,
    node_id: i32,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    let sort_tuple_exprs = sort.sort_info.sort_tuple_slot_exprs.as_ref().ok_or_else(|| {
        format!(
            "SORT_NODE node_id={} output column count mismatch: child={} sort={} and sort_tuple_slot_exprs is missing",
            node_id,
            child.layout.order.len(),
            out_layout.order.len()
        )
    })?;

    if sort_tuple_exprs.len() > out_layout.order.len() {
        return Err(format!(
            "SORT_NODE node_id={} sort_tuple_slot_exprs longer than output layout: exprs={} out_layout={}",
            node_id,
            sort_tuple_exprs.len(),
            out_layout.order.len()
        ));
    }

    let mut exprs = Vec::with_capacity(out_layout.order.len());
    for expr in sort_tuple_exprs {
        exprs.push(lower_t_expr(
            expr,
            arena,
            &child.layout,
            last_query_id,
            fe_addr,
        )?);
    }

    if exprs.len() < out_layout.order.len() {
        let pre_agg_exprs = sort.pre_agg_exprs.as_ref().ok_or_else(|| {
            format!(
                "SORT_NODE node_id={} has {} output slots but only {} sort_tuple_slot_exprs and missing pre_agg_exprs",
                node_id,
                out_layout.order.len(),
                exprs.len()
            )
        })?;
        let pre_agg_slots = sort.pre_agg_output_slot_id.as_ref().ok_or_else(|| {
            format!(
                "SORT_NODE node_id={} has {} output slots but only {} sort_tuple_slot_exprs and missing pre_agg_output_slot_id",
                node_id,
                out_layout.order.len(),
                exprs.len()
            )
        })?;
        if pre_agg_exprs.len() != pre_agg_slots.len() {
            return Err(format!(
                "SORT_NODE node_id={} pre_agg length mismatch: pre_agg_exprs={} pre_agg_output_slot_id={}",
                node_id,
                pre_agg_exprs.len(),
                pre_agg_slots.len()
            ));
        }

        let mut passthrough_by_slot = std::collections::HashMap::<types::TSlotId, _>::new();
        for (slot_id, agg_expr) in pre_agg_slots.iter().zip(pre_agg_exprs.iter()) {
            let passthrough = lower_pre_agg_fallback_expr(
                agg_expr,
                arena,
                &child.layout,
                last_query_id,
                fe_addr,
                node_id,
            )?;
            if passthrough_by_slot.insert(*slot_id, passthrough).is_some() {
                return Err(format!(
                    "SORT_NODE node_id={} duplicate pre_agg_output_slot_id={}",
                    node_id, slot_id
                ));
            }
        }

        for (_tuple_id, slot_id) in out_layout.order.iter().skip(exprs.len()) {
            let passthrough = passthrough_by_slot.remove(slot_id).ok_or_else(|| {
                format!(
                    "SORT_NODE node_id={} cannot materialize output slot_id={} from pre_agg metadata",
                    node_id, slot_id
                )
            })?;
            exprs.push(passthrough);
        }

        if !passthrough_by_slot.is_empty() {
            return Err(format!(
                "SORT_NODE node_id={} has unused pre_agg_output_slot_id values: {:?}",
                node_id,
                passthrough_by_slot.keys().collect::<Vec<_>>()
            ));
        }
    }

    let output_slots = out_layout
        .order
        .iter()
        .map(|(_, slot_id)| SlotId::try_from(*slot_id))
        .collect::<Result<Vec<_>, _>>()?;

    let projected = ExecNode {
        kind: ExecNodeKind::Project(ProjectNode {
            input: Box::new(child.node),
            node_id,
            is_subordinate: true,
            exprs,
            expr_slot_ids: output_slots.clone(),
            expr_slot_schemas: None,
            output_indices: None,
            output_chunk_schema: chunk_schema_for_layout(
                desc_tbl.ok_or_else(|| {
                    "SORT_NODE requires desc_tbl for tuple projection chunk schema".to_string()
                })?,
                out_layout,
            )?,
        }),
    };

    Ok(Lowered {
        node: projected,
        layout: out_layout.clone(),
    })
}

fn lower_pre_agg_fallback_expr(
    agg_expr: &exprs::TExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
    node_id: i32,
) -> Result<crate::exec::expr::ExprId, String> {
    let Some(root) = agg_expr.nodes.first() else {
        return Err(format!(
            "SORT_NODE node_id={} pre_agg_expr has empty nodes",
            node_id
        ));
    };
    if root.num_children <= 0 {
        let fn_name = root
            .fn_
            .as_ref()
            .map(|f| f.name.function_name.to_ascii_lowercase())
            .unwrap_or_default();
        if fn_name == "count" {
            return Ok(arena.push(ExprNode::Literal(crate::exec::expr::LiteralValue::Int8(1))));
        }
        return Err(format!(
            "SORT_NODE node_id={} pre_agg_expr root has no children",
            node_id
        ));
    }

    let first_child_start = 1usize;
    let first_child_end = subtree_end(&agg_expr.nodes, first_child_start)?;
    let child_expr = exprs::TExpr {
        nodes: agg_expr.nodes[first_child_start..first_child_end].to_vec(),
    };
    let child_id = lower_t_expr(&child_expr, arena, input_layout, last_query_id, fe_addr)?;

    // Cast the passthrough to the aggregate's declared output type if they differ.
    // For example, sum(INT) has BIGINT output type but the raw passthrough is INT.
    // The exchange receiver uses the slot descriptor type (BIGINT), so we must upcast here.
    //
    if let Some(agg_output_type) = arrow_type_from_desc(&root.type_) {
        let child_type = arena.data_type(child_id).cloned().unwrap_or(DataType::Null);
        if child_type != agg_output_type {
            if matches!(
                agg_output_type,
                DataType::Binary | DataType::LargeBinary | DataType::Utf8 | DataType::LargeUtf8
            ) {
                return Err(format!(
                    "SORT_NODE node_id={node_id} pre-agg passthrough declares opaque aggregate state {:?} but child expression is {:?}; source descriptor must be raw/return-typed or runtime must serialize",
                    agg_output_type, child_type
                ));
            }
            if !matches!(agg_output_type, DataType::Null) {
                return Ok(arena.push_typed(ExprNode::Cast(child_id), agg_output_type));
            }
        }
    }

    Ok(child_id)
}

fn walk_subtree_end(nodes: &[exprs::TExprNode], idx: &mut usize) -> Result<(), String> {
    let node = nodes
        .get(*idx)
        .ok_or_else(|| format!("invalid expr node index {}", *idx))?;
    *idx += 1;
    for _ in 0..node.num_children {
        walk_subtree_end(nodes, idx)?;
    }
    Ok(())
}

fn subtree_end(nodes: &[exprs::TExprNode], start: usize) -> Result<usize, String> {
    let mut idx = start;
    walk_subtree_end(nodes, &mut idx)?;
    Ok(idx)
}

fn add_slot_aliases(mut layout: Layout, source_layout: &Layout) -> Layout {
    let mut out_slot_to_out_idx = std::collections::HashMap::<types::TSlotId, usize>::new();
    for (idx, (_t, slot_id)) in layout.order.iter().enumerate() {
        out_slot_to_out_idx.entry(*slot_id).or_insert(idx);
    }
    for (tuple_id, slot_id) in &source_layout.order {
        if let Some(out_idx) = out_slot_to_out_idx.get(slot_id).copied() {
            layout.index.insert((*tuple_id, *slot_id), out_idx);
        }
    }
    layout
}

fn normalize_sort_output_layout(
    _child_layout: &Layout,
    out_layout: &Layout,
) -> Result<Layout, String> {
    Ok(out_layout.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::Chunk;
    use crate::exec::node::values::ValuesNode;
    use crate::lower::type_lowering::scalar_type_desc;
    use crate::thrift::exprs::{TExpr, TExprNode, TExprNodeType, TSlotRef};
    use crate::thrift::types::{TTypeDesc, TTypeNode, TTypeNodeType};
    use std::collections::HashMap;

    fn dummy_type_desc() -> TTypeDesc {
        TTypeDesc {
            types: Some(vec![TTypeNode {
                type_: TTypeNodeType::SCALAR,
                scalar_type: None,
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn default_expr_node() -> TExprNode {
        TExprNode {
            node_type: TExprNodeType::INT_LITERAL,
            type_: dummy_type_desc(),
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

    fn slot_ref_expr(tuple_id: i32, slot_id: i32) -> TExpr {
        TExpr {
            nodes: vec![TExprNode {
                node_type: TExprNodeType::SLOT_REF,
                type_: dummy_type_desc(),
                num_children: 0,
                slot_ref: Some(TSlotRef { slot_id, tuple_id }),
                ..default_expr_node()
            }],
        }
    }

    fn avg_pre_agg_expr_with_opaque_varchar_state(tuple_id: i32, slot_id: i32) -> TExpr {
        let varchar_type = scalar_type_desc(types::TPrimitiveType::VARCHAR);
        let bigint_type = scalar_type_desc(types::TPrimitiveType::BIGINT);

        TExpr {
            nodes: vec![
                TExprNode {
                    node_type: TExprNodeType::FUNCTION_CALL,
                    type_: varchar_type.clone(),
                    num_children: 1,
                    agg_expr: Some(exprs::TAggregateExpr {
                        is_merge_agg: false,
                    }),
                    fn_: Some(types::TFunction {
                        name: types::TFunctionName {
                            db_name: None,
                            function_name: "avg".to_string(),
                        },
                        binary_type: types::TFunctionBinaryType::BUILTIN,
                        arg_types: vec![bigint_type.clone()],
                        ret_type: varchar_type.clone(),
                        has_var_args: false,
                        comment: None,
                        signature: None,
                        hdfs_location: None,
                        scalar_fn: None,
                        aggregate_fn: Some(types::TAggregateFunction {
                            intermediate_type: varchar_type.clone(),
                            update_fn_symbol: None,
                            init_fn_symbol: None,
                            serialize_fn_symbol: None,
                            merge_fn_symbol: None,
                            finalize_fn_symbol: None,
                            get_value_fn_symbol: None,
                            remove_fn_symbol: None,
                            is_analytic_only_fn: None,
                            symbol: None,
                            is_asc_order: None,
                            nulls_first: None,
                            is_distinct: None,
                        }),
                        id: None,
                        checksum: None,
                        agg_state_desc: None,
                        fid: None,
                        table_fn: None,
                        could_apply_dict_optimize: None,
                        ignore_nulls: None,
                        isolated: None,
                        input_type: None,
                        content: None,
                    }),
                    ..default_expr_node()
                },
                TExprNode {
                    node_type: TExprNodeType::SLOT_REF,
                    type_: bigint_type,
                    num_children: 0,
                    slot_ref: Some(TSlotRef { slot_id, tuple_id }),
                    ..default_expr_node()
                },
            ],
        }
    }

    fn single_slot_layout(tuple_id: i32, slot_id: i32) -> Layout {
        let mut index = HashMap::new();
        index.insert((tuple_id, slot_id), 0);
        Layout {
            order: vec![(tuple_id, slot_id)],
            index,
        }
    }

    fn dummy_child(layout: Layout) -> Lowered {
        Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::default(),
                    node_id: 0,
                }),
            },
            layout,
        }
    }

    fn sort_plan_node(sort_info: plan_nodes::TSortInfo) -> plan_nodes::TPlanNode {
        let mut node =
            crate::lower::node::test_plan_node(7, plan_nodes::TPlanNodeType::SORT_NODE, 1);
        node.sort_node = Some(plan_nodes::TSortNode {
            sort_info,
            use_top_n: false,
            offset: Some(0),
            ordering_exprs: None,
            is_asc_order: None,
            is_default_limit: None,
            nulls_first: None,
            sort_tuple_slot_exprs: None,
            has_outer_join_child: None,
            sql_sort_keys: None,
            analytic_partition_exprs: None,
            partition_exprs: None,
            partition_limit: None,
            topn_type: None,
            build_runtime_filters: None,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            late_materialization: None,
            enable_parallel_merge: None,
            analytic_partition_skewed: None,
            pre_agg_exprs: None,
            pre_agg_output_slot_id: None,
            pre_agg_insert_local_shuffle: None,
            parallel_merge_late_materialize_mode: None,
            per_pipeline: None,
        });
        node
    }

    fn lower_sort_from_node(
        node: &plan_nodes::TPlanNode,
        layout: Layout,
    ) -> Result<Lowered, String> {
        let mut arena = ExprArena::default();
        lower_sort_node(
            vec![dummy_child(layout.clone())],
            node,
            &mut arena,
            layout,
            None,
            None,
            None,
        )
    }

    #[test]
    fn lower_pre_agg_fallback_rejects_opaque_passthrough_type_drift() {
        let input_layout = single_slot_layout(0, 1);
        let agg_expr = avg_pre_agg_expr_with_opaque_varchar_state(0, 1);
        let mut arena = ExprArena::default();

        let err =
            lower_pre_agg_fallback_expr(&agg_expr, &mut arena, &input_layout, None, None, 7001)
                .expect_err("opaque pre-agg passthrough must fail until descriptor is raw");
        assert!(
            err.contains("pre-agg passthrough declares opaque aggregate state"),
            "err={err}"
        );
    }

    #[test]
    fn lower_sort_node_rejects_is_asc_length_mismatch() {
        let layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![],
            nulls_first: vec![true],
            sort_tuple_slot_exprs: None,
        };
        let node = sort_plan_node(sort_info);

        let mut arena = ExprArena::default();
        let err = lower_sort_node(
            vec![dummy_child(layout.clone())],
            &node,
            &mut arena,
            layout,
            None,
            None,
            None,
        )
        .unwrap_err();

        assert!(
            err.contains("sort_info.is_asc_order length mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn lower_sort_node_rejects_nulls_first_length_mismatch() {
        let layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![true],
            nulls_first: vec![],
            sort_tuple_slot_exprs: None,
        };
        let node = sort_plan_node(sort_info);

        let mut arena = ExprArena::default();
        let err = lower_sort_node(
            vec![dummy_child(layout.clone())],
            &node,
            &mut arena,
            layout,
            None,
            None,
            None,
        )
        .unwrap_err();

        assert!(
            err.contains("sort_info.nulls_first length mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn lower_sort_node_accepts_rank_topn_type() {
        let layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![true],
            nulls_first: vec![true],
            sort_tuple_slot_exprs: None,
        };
        let mut node = sort_plan_node(sort_info);
        {
            let sort_node = node.sort_node.as_mut().expect("sort node");
            sort_node.use_top_n = true;
            sort_node.topn_type = Some(plan_nodes::TTopNType::RANK);
        }
        node.limit = 10;
        node.sort_node.as_mut().expect("sort node").offset = Some(0);

        let lowered = lower_sort_from_node(&node, layout).expect("lower sort");
        let ExecNodeKind::Sort(sort) = lowered.node.kind else {
            panic!("expected sort node");
        };
        assert!(sort.use_top_n);
        assert_eq!(sort.topn_type, SortTopNType::Rank);
    }

    #[test]
    fn lower_sort_node_rejects_rank_topn_with_offset() {
        let layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![true],
            nulls_first: vec![true],
            sort_tuple_slot_exprs: None,
        };
        let mut node = sort_plan_node(sort_info);
        {
            let sort_node = node.sort_node.as_mut().expect("sort node");
            sort_node.use_top_n = true;
            sort_node.topn_type = Some(plan_nodes::TTopNType::DENSE_RANK);
            sort_node.offset = Some(1);
        }
        node.limit = 10;

        let err = lower_sort_from_node(&node, layout).unwrap_err();
        assert!(err.contains("requires offset=0"), "unexpected error: {err}");
    }

    #[test]
    fn lower_sort_node_rejects_topn_without_limit() {
        let layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![true],
            nulls_first: vec![true],
            sort_tuple_slot_exprs: None,
        };
        let mut node = sort_plan_node(sort_info);
        node.sort_node.as_mut().expect("sort node").use_top_n = true;

        let err = lower_sort_from_node(&node, layout).unwrap_err();
        assert!(
            err.contains("use_top_n=true requires node.limit >= 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn lower_sort_node_rejects_non_positive_buffer_hints() {
        let layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![true],
            nulls_first: vec![true],
            sort_tuple_slot_exprs: None,
        };
        let mut node = sort_plan_node(sort_info);
        node.sort_node
            .as_mut()
            .expect("sort node")
            .max_buffered_rows = Some(0);

        let err = lower_sort_from_node(&node, layout).unwrap_err();
        assert!(
            err.contains("max_buffered_rows must be > 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn lower_sort_node_accepts_row_number_and_buffer_hints() {
        let layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![true],
            nulls_first: vec![true],
            sort_tuple_slot_exprs: None,
        };
        let mut node = sort_plan_node(sort_info);
        {
            let sort_node = node.sort_node.as_mut().expect("sort node");
            sort_node.use_top_n = true;
            sort_node.topn_type = Some(plan_nodes::TTopNType::ROW_NUMBER);
            sort_node.max_buffered_rows = Some(1024);
            sort_node.max_buffered_bytes = Some(2048);
        }
        node.limit = 10;

        let lowered = lower_sort_from_node(&node, layout).expect("lower sort");
        let ExecNodeKind::Sort(sort) = lowered.node.kind else {
            panic!("expected sort node");
        };
        assert!(sort.use_top_n);
        assert_eq!(sort.topn_type, SortTopNType::RowNumber);
        assert_eq!(sort.max_buffered_rows, Some(1024));
        assert_eq!(sort.max_buffered_bytes, Some(2048));
    }

    #[test]
    fn lower_sort_node_accepts_dense_rank_topn_type() {
        let layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![true],
            nulls_first: vec![true],
            sort_tuple_slot_exprs: None,
        };
        let mut node = sort_plan_node(sort_info);
        {
            let sort_node = node.sort_node.as_mut().expect("sort node");
            sort_node.use_top_n = true;
            sort_node.topn_type = Some(plan_nodes::TTopNType::DENSE_RANK);
            sort_node.offset = Some(0);
        }
        node.limit = 10;

        let lowered = lower_sort_from_node(&node, layout).expect("lower sort");
        let ExecNodeKind::Sort(sort) = lowered.node.kind else {
            panic!("expected sort node");
        };
        assert!(sort.use_top_n);
        assert_eq!(sort.topn_type, SortTopNType::DenseRank);
    }

    #[test]
    fn lower_sort_node_reads_partition_limit_and_exprs() {
        let layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![true],
            nulls_first: vec![true],
            sort_tuple_slot_exprs: None,
        };
        let mut node = sort_plan_node(sort_info);
        {
            let sort_node = node.sort_node.as_mut().expect("sort node");
            sort_node.use_top_n = true;
            sort_node.topn_type = Some(plan_nodes::TTopNType::RANK);
            sort_node.partition_limit = Some(3);
            sort_node.partition_exprs = Some(vec![slot_ref_expr(0, 1)]);
        }
        node.limit = 10;

        let lowered = lower_sort_from_node(&node, layout).expect("lower sort");
        let ExecNodeKind::Sort(sort) = lowered.node.kind else {
            panic!("expected sort node");
        };
        assert_eq!(sort.partition_limit, Some(3));
        assert_eq!(sort.partition_exprs.len(), 1);
    }

    /// Partition-TopN with use_top_n=true and NO global limit must be accepted.
    /// The per-partition cap (partition_limit) replaces the global limit, so
    /// node.limit=-1 is valid when partition_limit is set.
    #[test]
    fn lower_sort_node_accepts_partition_topn_without_global_limit() {
        let layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![true],
            nulls_first: vec![true],
            sort_tuple_slot_exprs: None,
        };
        let mut node = sort_plan_node(sort_info);
        {
            let sort_node = node.sort_node.as_mut().expect("sort node");
            sort_node.use_top_n = true;
            sort_node.topn_type = Some(plan_nodes::TTopNType::RANK);
            sort_node.partition_limit = Some(2);
            sort_node.partition_exprs = Some(vec![slot_ref_expr(0, 1)]);
        }
        // Intentionally leave node.limit = -1 (no global limit) — this is the
        // partition-topn decoupled-from-global-limit contract.
        assert_eq!(node.limit, -1, "test precondition: no global limit");

        let lowered = lower_sort_from_node(&node, layout)
            .expect("partition-topn without global limit must be accepted");
        let ExecNodeKind::Sort(sort) = lowered.node.kind else {
            panic!("expected sort node");
        };
        assert!(sort.use_top_n);
        assert_eq!(sort.limit, None, "global limit must remain None");
        assert_eq!(sort.partition_limit, Some(2));
        assert_eq!(sort.partition_exprs.len(), 1);
    }

    /// StarRocks sends partition_limit = -1 (the "no per-partition cap"
    /// sentinel) for an ordinary global TopN: it sets the field whenever
    /// partition_exprs is present, defaulting the cap to -1. Lowering must
    /// treat any negative partition_limit as None rather than rejecting it,
    /// otherwise every FE-issued `ORDER BY ... LIMIT n` fails to lower.
    #[test]
    fn lower_sort_node_treats_negative_partition_limit_as_none() {
        let layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![true],
            nulls_first: vec![true],
            sort_tuple_slot_exprs: None,
        };
        let mut node = sort_plan_node(sort_info);
        {
            let sort_node = node.sort_node.as_mut().expect("sort node");
            sort_node.use_top_n = true;
            sort_node.topn_type = Some(plan_nodes::TTopNType::ROW_NUMBER);
            // FE emits an (empty) partition_exprs list plus the -1 sentinel for
            // a plain global TopN.
            sort_node.partition_exprs = Some(vec![]);
            sort_node.partition_limit = Some(-1);
            sort_node.offset = Some(0);
        }
        node.limit = 5;

        let lowered = lower_sort_from_node(&node, layout)
            .expect("negative partition_limit must lower as no per-partition cap");
        let ExecNodeKind::Sort(sort) = lowered.node.kind else {
            panic!("expected sort node");
        };
        assert_eq!(
            sort.partition_limit, None,
            "negative partition_limit sentinel must map to None"
        );
        assert!(sort.use_top_n);
        assert_eq!(sort.limit, Some(5), "global limit must be preserved");
        assert!(sort.partition_exprs.is_empty());
    }
}
