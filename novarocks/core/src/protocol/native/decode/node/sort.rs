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

use super::super::NativeFragmentDecodeError;
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
use super::super::expr::decode_expr_at;
use super::super::layout::Layout;
use super::common::{
    build_slot_projection, parse_distributed_limit, parse_optional_nonnegative_i64,
};
use super::{DecodedNode, NativePlanDecodeContext};
use crate::exec::expr::ExprArena;
use crate::exec::node::sort::{SortExpression, SortNode, SortTopNType};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::protocol::common::error::{FieldPath, ProtocolErrorKind};
use novarocks_protocol::{expr, plan};

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

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub(super) fn lower_sort_items(
    node_kind: &str,
    items: &[expr::SortItem],
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<Vec<SortExpression>, NativeFragmentDecodeError> {
    lower_sort_items_with_decoder(node_kind, items, path, arena, input_layout, None)
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
                None => {
                    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
                    {
                        decode_expr_at(expr, item_path.field("expr"), arena, input_layout)
                    }
                    #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
                    {
                        Err(NativeFragmentDecodeError::unsupported(
                            item_path.field("expr"),
                            "native expression decoder must be supplied by the backend runtime",
                        ))
                    }
                }
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
) -> Result<SortTopNType, super::super::error::NativeFragmentLeafDecodeError> {
    let Some(value) = value else {
        return Ok(SortTopNType::RowNumber);
    };
    match plan::SortTopNType::try_from(value).map_err(|_| {
        super::super::error::NativeFragmentLeafDecodeError::at_field(
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

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::{NativePlanDecodeContext, decode_node};
    use crate::exec::expr::ExprArena;
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::sort::SortTopNType;
    use crate::protocol::native::type_mapping::encode_type;
    use novarocks_protocol::{common, expr, plan};
    use novarocks_types::SlotId;

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

    fn two_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20)],
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
    fn lowers_sort_and_topn_shapes() {
        let mut sort = physical_node(
            20,
            plan::plan_node::Kind::Sort(plan::SortNode {
                items: vec![sort_item(1)],
                analytic_partition_by: Vec::new(),
                output_columns: vec![output_column(1, "id", DataType::Int64)],
                offset: Some(2),
                partition_limit: None,
                topn_type: None,
            }),
            vec![output_column(1, "id", DataType::Int64)],
            vec![one_col_values_node(10)],
        );
        sort.limit = 9;
        let lowered_sort = lower(&sort);
        let ExecNodeKind::Sort(sort) = lowered_sort.node.kind else {
            panic!("expected Sort");
        };
        assert!(!sort.use_top_n);
        assert_eq!(sort.limit, Some(9));
        assert_eq!(sort.offset, 2);
        assert_eq!(sort.order_by.len(), 1);

        let topn = physical_node(
            30,
            plan::plan_node::Kind::Topn(plan::TopNNode {
                items: vec![sort_item(1)],
                limit: Some(3),
                offset: Some(0),
                phase: plan::TopNPhase::TopnPhaseFinal as i32,
                is_split: false,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered_topn = lower(&topn);
        let ExecNodeKind::Sort(topn) = lowered_topn.node.kind else {
            panic!("expected TopN as Sort");
        };
        assert!(topn.use_top_n);
        assert_eq!(topn.limit, Some(3));
        assert_eq!(topn.offset, 0);
        assert_eq!(topn.topn_type, SortTopNType::RowNumber);
    }

    #[test]
    fn lowers_sort_output_reorder_as_subordinate_project() {
        let sort = physical_node(
            20,
            plan::plan_node::Kind::Sort(plan::SortNode {
                items: vec![sort_item(1)],
                analytic_partition_by: Vec::new(),
                output_columns: vec![
                    output_column(2, "b", DataType::Int64),
                    output_column(1, "a", DataType::Int64),
                ],
                offset: None,
                partition_limit: None,
                topn_type: None,
            }),
            vec![
                output_column(2, "b", DataType::Int64),
                output_column(1, "a", DataType::Int64),
            ],
            vec![two_col_values_node(10)],
        );

        let lowered = lower(&sort);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected reorder project");
        };
        assert!(project.is_subordinate);
        assert_eq!(project.node_id, 20);
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(2), SlotId::new(1)]);
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[SlotId::new(2), SlotId::new(1)]
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(2), SlotId::new(1)]);
        let ExecNodeKind::Sort(sort) = project.input.kind else {
            panic!("expected Sort below reorder project");
        };
        assert_eq!(sort.order_by.len(), 1);
        assert!(matches!(sort.input.kind, ExecNodeKind::Values(_)));
    }
}
