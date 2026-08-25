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

pub(crate) use self::type_mapping::encode_type;
use super::expr::encode_expr;
use crate::query_execution::preparation::NativeScanFactsView;
use novarocks_proto::{common, plan};
use novarocks_sql::plan_read::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan, FragmentEdge,
    FragmentEdgeKind, FragmentEdgeOutputCatalog, FragmentStreamKind, NodeExecutionColumn,
    NodeOutputCatalog, OutputColumn as AnalysisOutputColumn, PlanFragment, TypedExpr,
    WriteContractCatalog, distributed_kind_to_physical,
};

use output::apply_sealed_node_output_columns;
use relational::encode_physical_node;

mod output;
mod relational;
mod scan;
mod topology;
mod type_mapping;
mod write;

type ContextRef<'a, T> = Option<&'a T>;

pub(super) struct NativePlanEncodeContext<'a> {
    pub(super) scan_facts: Option<NativeScanFactsView<'a>>,
    /// The sealed node-output contract. The encoder reads each covered node's
    /// (join / scan / set-op / sort) execution output from here rather than
    /// re-deriving or repairing it. `None` only in bare-node encoder unit tests
    /// that have no sealed plan; those rely on the payload columns encoded by
    /// `encode_physical_node`, which is the same data the catalog is built from.
    pub(super) node_outputs: ContextRef<'a, NodeOutputCatalog>,
    /// The sealed fragment-output / stream-edge-projection contract (CGO-9C
    /// Task 2). The encoder maps each fragment's finalized output columns and each
    /// stream edge's finalized sender/receiver projection from here instead of
    /// re-deriving a stream schema or patching the exchange receiver. `None` only
    /// in bare-node/bare-fragment encoder unit tests that have no sealed plan.
    pub(super) fragment_edge_outputs: ContextRef<'a, FragmentEdgeOutputCatalog>,
    /// The sealed Iceberg write output / change-stream router partition contract
    /// (CGO-9C Task 3). The encoder maps each Iceberg write fragment's finalized
    /// output expressions and target output schema, and each change-stream router
    /// branch's finalized partition, from here instead of synthesizing the write
    /// output or reconstructing a partition from `output_partition_ordinals`.
    /// `None` only in bare-node/bare-fragment encoder unit tests, which never
    /// encode a write or router fragment.
    pub(super) write_contracts: ContextRef<'a, WriteContractCatalog>,
}

impl<'a> NativePlanEncodeContext<'a> {
    fn complete(src: &'a DistributedPlan, scan_facts: NativeScanFactsView<'a>) -> Self {
        Self {
            scan_facts: Some(scan_facts),
            node_outputs: Some(src.node_outputs()),
            fragment_edge_outputs: Some(src.fragment_edge_outputs()),
            write_contracts: Some(src.write_contracts()),
        }
    }
}

fn required_context_ref<'a, T>(
    value: ContextRef<'a, T>,
    missing: impl FnOnce() -> String,
) -> Result<&'a T, String> {
    value.ok_or_else(missing)
}

fn optional_context_ref<T>(value: Option<&T>) -> Option<&T> {
    value
}

pub(super) fn encode_distributed_plan(
    src: &DistributedPlan,
    scan_facts: NativeScanFactsView<'_>,
) -> Result<plan::DistributedPlan, String> {
    encode_distributed_plan_with_context_inner(
        src,
        NativePlanEncodeContext::complete(src, scan_facts),
    )
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
pub(super) fn encode_distributed_plan_with_context(
    src: &DistributedPlan,
    ctx: NativePlanEncodeContext<'_>,
) -> Result<plan::DistributedPlan, String> {
    encode_distributed_plan_with_context_inner(
        src,
        NativePlanEncodeContext {
            scan_facts: ctx.scan_facts,
            node_outputs: Some(src.node_outputs()),
            fragment_edge_outputs: Some(src.fragment_edge_outputs()),
            write_contracts: Some(src.write_contracts()),
        },
    )
}

fn encode_distributed_plan_with_context_inner(
    src: &DistributedPlan,
    ctx: NativePlanEncodeContext<'_>,
) -> Result<plan::DistributedPlan, String> {
    // The sealed node-output contract of `src` is authoritative for every covered
    // node, so bind it here: all fragment/node encoding then reads each covered
    // node's execution output from it instead of re-deriving or repairing it,
    // regardless of how the incoming context was constructed.
    let mut fragments = src
        .fragments()
        .iter()
        .map(|fragment| topology::encode_plan_fragment_with_context(fragment, &ctx))
        .collect::<Result<Vec<_>, _>>()?;
    topology::attach_stream_sinks(src, &mut fragments, &ctx)?;
    Ok(plan::DistributedPlan {
        fragments,
        root_fragment_id: src.root_fragment_id(),
        edges: src
            .edges()
            .iter()
            .map(topology::encode_fragment_edge)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub(crate) fn encode_data_partition(src: &DataPartition) -> Result<plan::DataPartition, String> {
    type_mapping::encode_data_partition(src)
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
pub(crate) fn encode_node(src: &DistributedNode) -> Result<plan::DistributedNode, String> {
    encode_node_with_context(
        src,
        &NativePlanEncodeContext {
            scan_facts: None,
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
        },
    )
}

pub(super) fn encode_node_with_context(
    src: &DistributedNode,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::DistributedNode, String> {
    let children = src
        .children
        .iter()
        .map(|child| encode_node_with_context(child, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let payload = match &src.payload {
        DistributedNodeKind::Exchange(exchange) => {
            // A stream-edge receiver carries exactly the edge's finalized
            // projection (the planner's authoritative reconciliation of this
            // receiver against what its source fragment sends); the encoder maps
            // it 1:1 here instead of a later exchange-receiver patch. A receiver
            // that is not a finalized stream-edge target (a CTE-multicast or
            // change-stream-router receiver) keeps its declared columns.
            let output_columns = optional_context_ref(ctx.fragment_edge_outputs)
                .and_then(|catalog| catalog.stream_edge_projection(src.fragment_id, src.node_id))
                .unwrap_or(&exchange.output_columns);
            plan::distributed_node::Payload::Exchange(scan::encode_exchange_receiver(
                exchange,
                output_columns,
            )?)
        }
        other => {
            let physical = distributed_kind_to_physical(other);
            plan::distributed_node::Payload::Physical(encode_physical_node(
                &physical,
                src.node_id,
                ctx,
            )?)
        }
    };
    let mut node = plan::DistributedNode {
        node_id: src.node_id,
        fragment_id: src.fragment_id,
        tuple_ids: src.tuple_ids.clone(),
        nullable_tuple_ids: src.nullable_tuple_ids.clone(),
        limit: src.limit,
        runtime_filter_binding_ids: src.runtime_filter_binding_ids().collect(),
        children,
        payload: Some(payload),
    };
    apply_sealed_node_output_columns(&mut node, src, ctx)?;
    Ok(node)
}

fn encode_exprs(src: &[TypedExpr]) -> Result<Vec<novarocks_proto::expr::Expr>, String> {
    src.iter().map(encode_expr).collect()
}
