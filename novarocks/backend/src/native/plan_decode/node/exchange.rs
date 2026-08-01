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

use super::common::parse_optional_nonnegative_i64;
use super::{DecodedNode, NativePlanDecodeContext, sort};
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::common::config::exchange_wait_ms;
use novarocks::exec::expr::ExprArena;
use novarocks::exec::node::exchange_source::ExchangeSourceNode;
use novarocks::exec::node::limit::LimitNode;
use novarocks::exec::node::sort::{SortNode, SortTopNType};
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::plan;
use novarocks::protocol::common::error::FieldPath;

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
