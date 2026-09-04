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

use crate::analysis::cte::CteId;
use crate::analysis::{OutputColumn, TypedExpr};
use crate::column_id::ColumnId;
use crate::planner::runtime_filter::wait_graph::RefinedFragmentEdge;

use super::node::DistributedNode;
use crate::planner::runtime_filter::activation::ActivationConstraint;
use crate::planner::runtime_filter::graph::RuntimeFilterGraphData;

pub type FragmentId = u32;

#[derive(Clone, Copy, Debug)]
pub enum PartitionKind {
    Unpartitioned,
    Random,
    Hash,
}

#[derive(Clone, Debug)]
pub struct DataPartition {
    pub kind: PartitionKind,
    pub exprs: Vec<TypedExpr>,
}

impl DataPartition {
    pub fn unpartitioned() -> Self {
        Self {
            kind: PartitionKind::Unpartitioned,
            exprs: Vec::new(),
        }
    }

    #[allow(
        dead_code,
        reason = "The random partition constructor is retained for distributed-plan fixture construction."
    )]
    pub(crate) fn random() -> Self {
        Self {
            kind: PartitionKind::Random,
            exprs: Vec::new(),
        }
    }

    pub(crate) fn hash(exprs: Vec<TypedExpr>) -> Self {
        Self {
            kind: PartitionKind::Hash,
            exprs,
        }
    }

    pub(crate) fn explain_label(&self) -> String {
        match self.kind {
            PartitionKind::Unpartitioned => "UNPARTITIONED".to_string(),
            PartitionKind::Random => "RANDOM".to_string(),
            PartitionKind::Hash => {
                if self.exprs.is_empty() {
                    "HASH_PARTITIONED".to_string()
                } else {
                    let exprs = self
                        .exprs
                        .iter()
                        .map(crate::explain::format_expr)
                        .collect::<Vec<_>>();
                    format!("HASH_PARTITIONED ({})", exprs.join(", "))
                }
            }
        }
    }
}

/// Planner-owned fragment sink intent lowered by codegen.
#[derive(Clone, Debug)]
pub enum DataSink {
    Result,
    Noop,
    ChangeStreamRouter(super::write::change_stream::ChangeStreamRouterSink),
}

#[derive(Clone, Debug)]
pub struct PlanFragment {
    pub fragment_id: FragmentId,
    pub root: DistributedNode,
    pub data_partition: DataPartition,
    pub output_partition: DataPartition,
    pub sink: DataSink,
    pub output_exprs: Option<Vec<TypedExpr>>,
    pub output_columns: Vec<OutputColumn>,
    pub cte_id: Option<CteId>,
    pub cte_exchange_nodes: Vec<(CteId, i32, Vec<ColumnId>)>,
}

/// Result of emitting a multi-fragment plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FragmentEdgeKind {
    Stream,
    CteMulticast {
        cte_id: CteId,
        receive_producer_column_ids: Vec<ColumnId>,
    },
    ChangeStreamRouter {
        router_group_id: i32,
        route_id: novarocks_spi::connector::ConnectorWriteRouteId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FragmentStreamKind {
    Gather,
    Broadcast,
    Partitioned,
    Other,
}

#[derive(Clone, Debug)]
pub struct FragmentEdge {
    pub source_fragment_id: FragmentId,
    pub target_fragment_id: FragmentId,
    pub target_exchange_node_id: i32,
    pub output_partition: DataPartition,
    pub stream_kind: FragmentStreamKind,
    pub edge_kind: FragmentEdgeKind,
    pub output_slot_ids: Vec<i32>,
}

impl FragmentEdge {
    pub(crate) fn as_refined_runtime_filter_edge(&self) -> RefinedFragmentEdge {
        RefinedFragmentEdge {
            source_fragment: self.source_fragment_id,
            target_fragment: self.target_fragment_id,
            target_exchange_node: self.target_exchange_node_id,
        }
    }
}

#[derive(Debug)]
pub(in crate::planner::distributed) struct DistributedPlanDraft {
    pub(in crate::planner::distributed) fragments: Vec<PlanFragment>,
    pub(in crate::planner::distributed) root_fragment_id: Option<FragmentId>,
    pub(in crate::planner::distributed) edges: Vec<FragmentEdge>,
    // Planner-private graph whose consumer activations remain constraints until
    // the seal-time activation decision pass consumes and materializes them.
    pub(in crate::planner::distributed) runtime_filter_graph:
        RuntimeFilterGraphData<ActivationConstraint>,
}
