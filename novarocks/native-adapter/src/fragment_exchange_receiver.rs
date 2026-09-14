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

//! Fragment exchange-node decoding.

use crate::fragment_decode_context::NativePlanDecodeContext;
use crate::fragment_error::NativeFragmentDecodeError;
use crate::fragment_expression::decode_expr_for_slot_layout;
use crate::fragment_layout::decode_fragment_output_layout;
use crate::fragment_plan_node::NativeLoweredPlanNode as DecodedNode;
use crate::fragment_plan_node::{lower_sort_items_for_layout, parse_optional_nonnegative_i64};
use novarocks_execution::exec::chunk::SlotLayout as Layout;
use novarocks_execution::exec::expr::{ExprArena, ExprId};
use novarocks_execution::exec::node::exchange_source::ExchangeSourceNode;
use novarocks_execution::exec::node::limit::LimitNode;
use novarocks_execution::exec::node::sort::{SortNode, SortTopNType};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::plan;

pub fn lower_exchange_receiver(
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
    let output_layout = decode_fragment_output_layout(
        &exchange.output_columns,
        path.clone().field("output_columns"),
    )?;
    let layout = Layout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    // The sender's hash key, decoded against this receiver's own tuple. The
    // planner keeps a stream edge's receiver columns equal to what the sender
    // sends, so the sender-side partition expressions resolve here unchanged.
    // A non-hash edge, or a hash edge that carries no expressions, leaves the
    // key empty rather than inventing one.
    let hash_partition_exprs = decode_hash_partition_exprs(
        exchange,
        path.clone().field("partition_exprs"),
        arena,
        &layout,
    )?;
    let mut lowered = DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::ExchangeSource(
                ExchangeSourceNode::new(node.node_id, ctx.exchange_wait(), output_schema.clone())
                    .with_hash_partition_exprs(hash_partition_exprs),
            ),
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
            let order_by = lower_sort_items_for_layout(
                "ExchangeReceiver TopNSplit",
                &topn.items,
                path.clone()
                    .field("flavor")
                    .field("topn_split")
                    .field("items"),
                arena,
                &lowered.layout,
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

/// Decode the hash key of a hash-partitioned exchange edge.
///
/// Only `PARTITION_TYPE_HASH` carries a key. Any other partition type returns an
/// empty key, and so does a hash edge whose expression list is empty: an empty
/// key means "this edge gives a consumer nothing to re-partition by", which is
/// exactly how a non-hash edge behaves. It is deliberately not an error here —
/// the sending side is where a hash sink without expressions fails.
fn decode_hash_partition_exprs(
    exchange: &plan::ExchangeReceiver,
    path: FieldPath,
    arena: &mut ExprArena,
    layout: &Layout,
) -> Result<Vec<ExprId>, NativeFragmentDecodeError> {
    if !matches!(
        plan::PartitionType::try_from(exchange.partition_type),
        Ok(plan::PartitionType::Hash)
    ) {
        return Ok(Vec::new());
    }
    exchange
        .partition_exprs
        .iter()
        .enumerate()
        .map(|(index, expr)| {
            decode_expr_for_slot_layout(expr, path.clone().index(index), arena, layout)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{DecodedNode, NativePlanDecodeContext};
    use novarocks_proto_codec::FieldPath;

    fn decode_node(
        node: &plan::DistributedNode,
        arena: &mut ExprArena,
        ctx: &NativePlanDecodeContext,
    ) -> Result<DecodedNode, crate::fragment_error::NativeFragmentDecodeError> {
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = node.payload.as_ref()
        else {
            panic!("fixture node carries an exchange payload");
        };
        super::lower_exchange_receiver(
            node,
            exchange,
            FieldPath::root("plan_fragment")
                .field("root")
                .field("payload")
                .field("exchange"),
            Vec::new(),
            arena,
            ctx,
        )
    }

    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_execution::runtime::exchange::ExchangeKey;
    use novarocks_plan_codec::encode_native_type as encode_type;
    use novarocks_proto_models::{common, expr, plan};
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

    fn distribution_exchange_node(
        node_id: i32,
        partition_type: plan::PartitionType,
        partition_exprs: Vec<expr::Expr>,
    ) -> plan::DistributedNode {
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
                    partition_type: partition_type as i32,
                    partition_exprs,
                    source_fragment_id: 7,
                    output_columns: vec![
                        output_column(1, "id", DataType::Int64),
                        output_column(2, "label", DataType::Int64),
                    ],
                    output_qualifier: None,
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                    }),
                },
            )),
        }
    }

    fn decode_exchange_source(
        node: &plan::DistributedNode,
        arena: &mut ExprArena,
    ) -> novarocks_execution::exec::node::exchange_source::ExchangeSourceNode {
        let lowered = decode_node(
            node,
            arena,
            &NativePlanDecodeContext::default().with_exchange_sender_count(
                ExchangeKey {
                    finst_id_hi: 0,
                    finst_id_lo: 0,
                    node_id: node.node_id,
                },
                2,
            ),
        )
        .expect("exchange receiver");
        let ExecNodeKind::ExchangeSource(exchange) = lowered.node.kind else {
            panic!("expected ExchangeSource");
        };
        exchange
    }

    /// A hash edge's key is what a consumer needing per-driver exclusivity
    /// re-partitions by, so it has to survive decode resolved against this
    /// receiver's own tuple rather than be rediscovered from column names.
    #[test]
    fn carries_the_hash_key_of_a_hash_partitioned_exchange() {
        let mut arena = ExprArena::default();
        let exchange = decode_exchange_source(
            &distribution_exchange_node(
                50,
                plan::PartitionType::Hash,
                vec![column_ref(2, DataType::Int64)],
            ),
            &mut arena,
        );

        let [key] = exchange.hash_partition_exprs() else {
            panic!("expected exactly one hash key expression");
        };
        assert!(
            matches!(
                arena.node(*key),
                Some(novarocks_execution::exec::expr::ExprNode::SlotId(slot)) if *slot == SlotId::new(2)
            ),
            "the key must resolve to the receiver's own slot for that column"
        );
    }

    /// An unpartitioned edge promises nothing about which rows travel together,
    /// so it must not hand a consumer a key to re-partition by.
    #[test]
    fn carries_no_hash_key_for_an_unpartitioned_exchange() {
        let mut arena = ExprArena::default();
        let exchange = decode_exchange_source(
            &distribution_exchange_node(
                51,
                plan::PartitionType::Unpartitioned,
                vec![column_ref(2, DataType::Int64)],
            ),
            &mut arena,
        );
        assert!(exchange.hash_partition_exprs().is_empty());
    }

    /// A hash edge that names no expression gives a consumer nothing to
    /// re-partition by. That is an empty key, not a decode failure: the sending
    /// side is where a hash sink without expressions is refused.
    #[test]
    fn carries_no_hash_key_when_a_hash_exchange_names_no_expression() {
        let mut arena = ExprArena::default();
        let exchange = decode_exchange_source(
            &distribution_exchange_node(52, plan::PartitionType::Hash, Vec::new()),
            &mut arena,
        );
        assert!(exchange.hash_partition_exprs().is_empty());
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
