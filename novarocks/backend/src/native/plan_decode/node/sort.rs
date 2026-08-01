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

use super::common::{
    build_slot_projection, parse_distributed_limit, parse_optional_nonnegative_i64,
};
use super::{DecodedNode, NativePlanDecodeContext};
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::exec::expr::ExprArena;
use novarocks::exec::node::sort::{SortExpression, SortNode, SortTopNType};
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::{expr, plan};
use novarocks::protocol::common::error::{FieldPath, ProtocolErrorKind};

pub(super) fn lower_sort_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    sort: &plan::SortNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    let (output_columns, output_columns_path) = if sort.output_columns.is_empty() {
        (&physical.output_columns, physical_output_path)
    } else {
        (&sort.output_columns, path.clone().field("output_columns"))
    };
    let order_by = lower_sort_items_with_context(
        "SortNode",
        &sort.items,
        path.clone().field("items"),
        arena,
        &child.layout,
        ctx,
    )?;
    let limit = NativeFragmentDecodeError::map_invalid(
        path.clone().field("limit"),
        parse_distributed_limit(node.limit, "SortNode DistributedNode.limit"),
    )?;
    let offset = NativeFragmentDecodeError::map_invalid(
        path.clone().field("offset"),
        parse_optional_nonnegative_i64(sort.offset, "SortNode.offset"),
    )?
    .unwrap_or(0);
    let topn_type = NativeFragmentDecodeError::map_invalid(
        path.clone().field("topn_type"),
        parse_sort_topn_type(sort.topn_type),
    )?;
    let partition_exprs = sort
        .analytic_partition_by
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            let expr = ctx.decode_expression(
                expr,
                path.clone().field("analytic_partition_by").index(idx),
                arena,
                &child.layout,
            )?;
            Ok(SortExpression {
                expr,
                asc: true,
                nulls_first: true,
            })
        })
        .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
    let partition_limit = sort.partition_limit.map(|value| value as usize);
    let use_top_n = partition_limit.is_some();
    if use_top_n && topn_type != SortTopNType::RowNumber && offset != 0 {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("offset"),
            format!(
                "SortNode node_id={} topn_type {:?} requires offset=0, got {}",
                node.node_id, topn_type, offset
            ),
        ));
    }
    let sort_node = ExecNode {
        kind: ExecNodeKind::Sort(SortNode {
            input: Box::new(child.node),
            node_id: node.node_id,
            use_top_n,
            order_by,
            limit,
            offset,
            topn_type,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            partition_exprs,
            partition_limit,
        }),
    };
    let sorted = DecodedNode {
        node: sort_node,
        layout: child.layout.clone(),
        output_schema: child.output_schema.clone(),
    };
    if output_columns.is_empty() {
        return Ok(sorted);
    }

    let output_layout = ctx.decode_output_layout(output_columns, output_columns_path.clone())?;
    let layout = Layout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    if layout.order() == child.layout.order() {
        return Ok(DecodedNode {
            node: sorted.node,
            layout,
            output_schema,
        });
    }

    build_slot_projection(
        "SortNode",
        sorted,
        output_columns,
        output_columns_path,
        node.node_id,
        arena,
        ctx,
    )
}

#[cfg(test)]
pub(super) fn lower_sort_items(
    node_kind: &str,
    items: &[expr::SortItem],
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<Vec<SortExpression>, NativeFragmentDecodeError> {
    let context = NativePlanDecodeContext::default();
    lower_sort_items_with_decoder(node_kind, items, path, arena, input_layout, Some(&context))
}

pub(super) fn lower_sort_items_with_context(
    node_kind: &str,
    items: &[expr::SortItem],
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &Layout,
    ctx: &NativePlanDecodeContext,
) -> Result<Vec<SortExpression>, NativeFragmentDecodeError> {
    lower_sort_items_with_decoder(node_kind, items, path, arena, input_layout, Some(ctx))
}

fn lower_sort_items_with_decoder(
    node_kind: &str,
    items: &[expr::SortItem],
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &Layout,
    ctx: Option<&NativePlanDecodeContext>,
) -> Result<Vec<SortExpression>, NativeFragmentDecodeError> {
    items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let item_path = path.clone().index(idx);
            let expr = item.expr.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    item_path.clone().field("expr"),
                    format!("{node_kind} sort item {idx} expr missing"),
                )
            })?;
            let expr = match ctx {
                Some(ctx) => {
                    ctx.decode_expression(expr, item_path.field("expr"), arena, input_layout)
                }
                None => Err(NativeFragmentDecodeError::unsupported(
                    item_path.field("expr"),
                    "native expression decoder must be supplied by the backend runtime",
                )),
            }?;
            Ok(SortExpression {
                expr,
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

pub(super) fn parse_sort_topn_type(
    value: Option<i32>,
) -> Result<SortTopNType, crate::native::plan_decode::error::NativeFragmentLeafDecodeError> {
    let Some(value) = value else {
        return Ok(SortTopNType::RowNumber);
    };
    match plan::SortTopNType::try_from(value).map_err(|_| {
        crate::native::plan_decode::error::NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "topn_type",
            format!("SortNode unknown topn_type {value}"),
        )
    })? {
        plan::SortTopNType::SortTopnTypeUnspecified | plan::SortTopNType::SortTopnTypeRowNumber => {
            Ok(SortTopNType::RowNumber)
        }
        plan::SortTopNType::SortTopnTypeRank => Ok(SortTopNType::Rank),
        plan::SortTopNType::SortTopnTypeDenseRank => Ok(SortTopNType::DenseRank),
    }
}
