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

//! Fragment top-N decoding.

use super::common::{merge_limits, parse_distributed_limit, parse_optional_nonnegative_i64};
use super::sort::{lower_sort_items_with_context, parse_sort_topn_type};
use super::{DecodedNode, NativePlanDecodeContext};
use crate::fragment::decode::plan::error::NativeFragmentDecodeError;
use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::node::sort::SortNode;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto::FieldPath;
use novarocks_proto::plan;

pub(super) fn lower_topn_node(
    node: &plan::DistributedNode,
    topn: &plan::TopNNode,
    path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    let payload_limit = NativeFragmentDecodeError::map_invalid(
        path.clone().field("limit"),
        parse_optional_nonnegative_i64(topn.limit, "TopNNode.limit"),
    )?;
    let outer_limit = NativeFragmentDecodeError::map_invalid(
        path.clone().field("limit"),
        parse_distributed_limit(node.limit, "TopNNode DistributedNode.limit"),
    )?;
    let limit = NativeFragmentDecodeError::map_invalid(
        path.clone().field("limit"),
        merge_limits("TopNNode", payload_limit, outer_limit),
    )?;
    if limit.is_none() {
        return Err(NativeFragmentDecodeError::missing(
            path.clone().field("limit"),
            "TopNNode requires a non-negative limit",
        ));
    }
    let offset = NativeFragmentDecodeError::map_invalid(
        path.clone().field("offset"),
        parse_optional_nonnegative_i64(topn.offset, "TopNNode.offset"),
    )?
    .unwrap_or(0);
    let phase = plan::TopNPhase::try_from(topn.phase).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone().field("phase"),
            format!("TopNNode unknown phase {}", topn.phase),
        )
    })?;
    if phase == plan::TopNPhase::TopnPhaseUnspecified {
        return Err(NativeFragmentDecodeError::invalid_enum(
            path.clone().field("phase"),
            "TopNNode phase is unspecified",
        ));
    }
    if topn.is_split && phase == plan::TopNPhase::TopnPhaseFinal {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("is_split"),
            "TopNNode final split must be represented as ExchangeReceiver TopNSplit",
        ));
    }
    let order_by = lower_sort_items_with_context(
        "TopNNode",
        &topn.items,
        path.clone().field("items"),
        arena,
        &child.layout,
        ctx,
    )?;
    let topn_type = NativeFragmentDecodeError::map_invalid(
        path.clone().field("topn_type"),
        parse_sort_topn_type(None),
    )?;
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Sort(SortNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                use_top_n: true,
                order_by,
                limit,
                offset,
                topn_type,
                max_buffered_rows: None,
                max_buffered_bytes: None,
                partition_exprs: Vec::new(),
                partition_limit: None,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::{NativePlanDecodeContext, decode_node};
    use crate::fragment::decode::type_decode::encode_type;
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_proto::{common, expr, plan};

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            is_internal: false,
        }
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    fn column_ref(column_id: u32, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            })),
        }
    }

    fn sort_item(column_id: u32) -> expr::SortItem {
        expr::SortItem {
            expr: Some(column_ref(column_id, DataType::Int64)),
            asc: true,
            nulls_first: false,
        }
    }

    fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(kind),
            })),
        }
    }

    fn one_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn lower(node: &plan::DistributedNode) -> super::super::DecodedNode {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &NativePlanDecodeContext::default()).expect("lower node")
    }

    #[test]
    fn lowers_partial_split_topn() {
        let topn = physical_node(
            30,
            plan::plan_node::Kind::Topn(plan::TopNNode {
                items: vec![sort_item(1)],
                limit: Some(3),
                offset: Some(0),
                phase: plan::TopNPhase::TopnPhasePartial as i32,
                is_split: true,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&topn);
        let ExecNodeKind::Sort(topn) = lowered.node.kind else {
            panic!("expected split TopN as Sort");
        };
        assert!(topn.use_top_n);
        assert_eq!(topn.limit, Some(3));
        assert_eq!(topn.offset, 0);
    }

    #[test]
    fn rejects_final_split_topn_physical_node() {
        let topn = physical_node(
            30,
            plan::plan_node::Kind::Topn(plan::TopNNode {
                items: vec![sort_item(1)],
                limit: Some(3),
                offset: Some(0),
                phase: plan::TopNPhase::TopnPhaseFinal as i32,
                is_split: true,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let mut arena = ExprArena::default();
        let err = decode_node(&topn, &mut arena, &NativePlanDecodeContext::default()).unwrap_err();
        assert!(err.contains("TopNNode final split"));
        assert!(err.contains("ExchangeReceiver TopNSplit"));
    }
}
