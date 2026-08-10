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

use std::time::Duration;

use super::super::NativeFragmentDecodeError;
use super::common::parse_optional_nonnegative_i64;
use super::{DecodedNode, NativePlanDecodeContext, sort};
use crate::common::config::exchange_wait_ms;
use crate::protocol::common::error::FieldPath;
use crate::protocol::native::decode::layout::Layout;
use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::node::exchange_source::ExchangeSourceNode;
use novarocks_execution::exec::node::limit::LimitNode;
use novarocks_execution::exec::node::sort::{SortNode, SortTopNType};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_protocol::plan;

pub(super) fn lower_exchange_receiver(
    node: &plan::DistributedNode,
    exchange: &plan::ExchangeReceiver,
    path: FieldPath,
    _children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let flavor = exchange
        .flavor
        .as_ref()
        .and_then(|flavor| flavor.kind.as_ref())
        .ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone().field("flavor").field("kind"),
                "ExchangeReceiver flavor missing",
            )
        })?;
    match flavor {
        plan::exchange_flavor::Kind::Distribution(true) => {}
        plan::exchange_flavor::Kind::Distribution(false) => {
            return Err(NativeFragmentDecodeError::invalid_value(
                path.clone().field("flavor").field("distribution"),
                "ExchangeReceiver distribution flavor must be true",
            ));
        }
        plan::exchange_flavor::Kind::LimitOffset(_) => {}
        plan::exchange_flavor::Kind::TopnSplit(_) => {}
        plan::exchange_flavor::Kind::CteMulticast(_) => {}
    }

    // Validate that the instance provides a sender count for this exchange node.
    // The instance-scoped ExchangeKey and sender count are materialized into the
    // per-node ExchangeBinding at execution time, not baked into the static node.
    NativeFragmentDecodeError::map_invalid(path.clone(), ctx.exchange_input(node.node_id))?;
    let output_layout = ctx.decode_output_layout(
        &exchange.output_columns,
        path.clone().field("output_columns"),
    )?;
    let layout = Layout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    let mut lowered = DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::ExchangeSource(ExchangeSourceNode::new(
                node.node_id,
                Duration::from_millis(exchange_wait_ms()),
                output_schema.clone(),
            )),
        },
        layout,
        output_schema,
    };

    match flavor {
        plan::exchange_flavor::Kind::LimitOffset(limit_offset) => {
            let limit = parse_optional_nonnegative_i64(
                limit_offset.limit,
                "ExchangeReceiver LimitOffset.limit",
            )
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    path.clone()
                        .field("flavor")
                        .field("limit_offset")
                        .field("limit"),
                    error,
                )
            })?;
            let offset = parse_optional_nonnegative_i64(
                limit_offset.offset,
                "ExchangeReceiver LimitOffset.offset",
            )
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    path.clone()
                        .field("flavor")
                        .field("limit_offset")
                        .field("offset"),
                    error,
                )
            })?
            .unwrap_or(0);
            if limit.is_some() || offset > 0 {
                lowered.node = ExecNode {
                    kind: ExecNodeKind::Limit(LimitNode {
                        input: Box::new(lowered.node),
                        node_id: node.node_id,
                        limit,
                        offset,
                    }),
                };
            }
        }
        plan::exchange_flavor::Kind::TopnSplit(topn) => {
            let order_by = sort::lower_sort_items_with_context(
                "ExchangeReceiver TopNSplit",
                &topn.items,
                path.clone()
                    .field("flavor")
                    .field("topn_split")
                    .field("items"),
                arena,
                &lowered.layout,
                ctx,
            )?;
            let limit = NativeFragmentDecodeError::map_invalid(
                path.clone()
                    .field("flavor")
                    .field("topn_split")
                    .field("limit"),
                parse_optional_nonnegative_i64(topn.limit, "ExchangeReceiver TopNSplit.limit"),
            )?;
            let offset = NativeFragmentDecodeError::map_invalid(
                path.clone()
                    .field("flavor")
                    .field("topn_split")
                    .field("offset"),
                parse_optional_nonnegative_i64(topn.offset, "ExchangeReceiver TopNSplit.offset"),
            )?
            .unwrap_or(0);
            lowered.node = ExecNode {
                kind: ExecNodeKind::Sort(SortNode {
                    input: Box::new(lowered.node),
                    node_id: node.node_id,
                    use_top_n: false,
                    order_by,
                    limit,
                    offset,
                    topn_type: SortTopNType::RowNumber,
                    max_buffered_rows: None,
                    max_buffered_bytes: None,
                    partition_exprs: Vec::new(),
                    partition_limit: None,
                }),
            };
        }
        _ => {}
    }

    Ok(lowered)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::{NativePlanDecodeContext, decode_node};
    use crate::protocol::native::type_mapping::encode_type;
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_execution::runtime::exchange::ExchangeKey;
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

    fn topn_exchange_node(node_id: i32) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Hash as i32,
                    partition_exprs: Vec::new(),
                    source_fragment_id: 7,
                    output_columns: vec![output_column(1, "id", DataType::Int64)],
                    output_qualifier: None,
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::TopnSplit(
                            plan::TopNSplitFlavor {
                                items: vec![sort_item(1)],
                                limit: Some(3),
                                offset: Some(1),
                            },
                        )),
                    }),
                },
            )),
        }
    }

    fn limit_offset_exchange_node(
        node_id: i32,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: limit.unwrap_or(-1),
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Unpartitioned as i32,
                    partition_exprs: Vec::new(),
                    source_fragment_id: 7,
                    output_columns: vec![output_column(1, "id", DataType::Int64)],
                    output_qualifier: None,
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::LimitOffset(
                            plan::LimitOffsetFlavor { limit, offset },
                        )),
                    }),
                },
            )),
        }
    }

    fn cte_multicast_exchange_node(node_id: i32) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Unpartitioned as i32,
                    partition_exprs: Vec::new(),
                    source_fragment_id: 7,
                    output_columns: vec![output_column(1, "id", DataType::Int64)],
                    output_qualifier: None,
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::CteMulticast(
                            plan::CteMulticastFlavor {
                                cte_id: 3,
                                receive_producer_column_ids: vec![1],
                            },
                        )),
                    }),
                },
            )),
        }
    }

    #[test]
    fn exchange_receiver_requires_sender_count() {
        let exchange = plan::DistributedNode {
            node_id: 40,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Hash as i32,
                    partition_exprs: Vec::new(),
                    source_fragment_id: 7,
                    output_columns: vec![output_column(1, "id", DataType::Int64)],
                    output_qualifier: None,
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                    }),
                },
            )),
        };

        let mut arena = ExprArena::default();
        let err =
            decode_node(&exchange, &mut arena, &NativePlanDecodeContext::default()).unwrap_err();
        assert!(err.contains("ExchangeReceiver"));
        assert!(err.contains("sender count"));

        let lowered = decode_node(
            &exchange,
            &mut arena,
            &NativePlanDecodeContext::default().with_exchange_sender_count(
                ExchangeKey {
                    finst_id_hi: 0,
                    finst_id_lo: 0,
                    node_id: 40,
                },
                2,
            ),
        )
        .expect("plain exchange");
        let ExecNodeKind::ExchangeSource(exchange) = lowered.node.kind else {
            panic!("expected ExchangeSource");
        };
        assert_eq!(exchange.node_id, 40);
        assert_eq!(exchange.expected_chunk_schema.slot_ids(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_topn_split_exchange_receiver_as_merging_sort() {
        let mut arena = ExprArena::default();
        let lowered = decode_node(
            &topn_exchange_node(41),
            &mut arena,
            &NativePlanDecodeContext::default().with_exchange_sender_count(
                ExchangeKey {
                    finst_id_hi: 0,
                    finst_id_lo: 0,
                    node_id: 41,
                },
                2,
            ),
        )
        .expect("TopNSplit exchange receiver");

        let ExecNodeKind::Sort(sort) = lowered.node.kind else {
            panic!("expected Sort");
        };
        assert_eq!(sort.node_id, 41);
        assert_eq!(sort.limit, Some(3));
        assert_eq!(sort.offset, 1);
        assert_eq!(sort.order_by.len(), 1);
        let ExecNodeKind::ExchangeSource(exchange) = sort.input.kind else {
            panic!("expected ExchangeSource under Sort");
        };
        assert_eq!(exchange.node_id, 41);
        assert_eq!(exchange.expected_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_limit_offset_exchange_receiver_as_limit_node() {
        let mut arena = ExprArena::default();
        let lowered = decode_node(
            &limit_offset_exchange_node(42, Some(3), Some(1)),
            &mut arena,
            &NativePlanDecodeContext::default().with_exchange_sender_count(
                ExchangeKey {
                    finst_id_hi: 0,
                    finst_id_lo: 0,
                    node_id: 42,
                },
                2,
            ),
        )
        .expect("LimitOffset exchange receiver");

        let ExecNodeKind::Limit(limit) = lowered.node.kind else {
            panic!("expected Limit");
        };
        assert_eq!(limit.node_id, 42);
        assert_eq!(limit.limit, Some(3));
        assert_eq!(limit.offset, 1);
        let ExecNodeKind::ExchangeSource(exchange) = limit.input.kind else {
            panic!("expected ExchangeSource under Limit");
        };
        assert_eq!(exchange.node_id, 42);
        assert_eq!(exchange.expected_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_cte_multicast_exchange_receiver_as_exchange_source() {
        let mut arena = ExprArena::default();
        let lowered = decode_node(
            &cte_multicast_exchange_node(43),
            &mut arena,
            &NativePlanDecodeContext::default().with_exchange_sender_count(
                ExchangeKey {
                    finst_id_hi: 0,
                    finst_id_lo: 0,
                    node_id: 43,
                },
                2,
            ),
        )
        .expect("CTE multicast exchange receiver");

        let ExecNodeKind::ExchangeSource(exchange) = lowered.node.kind else {
            panic!("expected ExchangeSource");
        };
        assert_eq!(exchange.node_id, 43);
        assert_eq!(exchange.expected_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }
}
