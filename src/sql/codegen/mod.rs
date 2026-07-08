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

//! Physical plan layer — converts [`LogicalPlanNode`] into Thrift execution plans.
//!
//! This layer allocates physical resources (tuple_id, slot_id, node_id),
//! compiles `TypedExpr` into Thrift `TExpr`, and assembles the Thrift
//! plan structures expected by the pipeline executor.

pub(crate) mod boundary_schema;
pub(crate) mod connector_scan_wire;
pub(crate) mod descriptors;
pub(crate) mod expr_compiler;
pub(crate) mod fallback_audit;
pub(crate) mod fragment_builder;
pub(crate) mod fragment_request;
pub(crate) mod helpers;
pub(crate) mod iceberg_change_stream_router_wire;
pub(crate) mod iceberg_delta_scan_wire;
pub(crate) mod iceberg_write_sink_wire;
pub(crate) mod ir;
pub(crate) mod nodes;
pub(crate) mod proto_encode;
pub(crate) mod resolve;
pub(crate) mod runtime_filter_lowering;
pub(crate) mod scalar_materialize;
pub(crate) mod type_infer;

use arrow::datatypes::DataType;

use crate::thrift::data_sinks;
use crate::thrift::descriptors as thrift_descriptors;
use crate::thrift::internal_service;
use crate::thrift::partitions;
use crate::thrift::plan_nodes;

use super::analysis::cte::CteId;
use super::column_id::ColumnId;

pub(crate) type FragmentId = u32;

pub(crate) use fragment_request::FragmentBuildRequest;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct OutputColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Result of emitting a multi-fragment plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FragmentEdgeKind {
    Stream,
    CteMulticast {
        cte_id: CteId,
        receive_producer_column_ids: Vec<ColumnId>,
    },
    IcebergChangeStreamRouter {
        router_group_id: i32,
        branch_id: i32,
        branch_kind: crate::sql::common::ChangeStreamBranchKind,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FragmentOutputKind {
    Result,
    TerminalWrite,
    NonTerminal,
}

impl FragmentOutputKind {
    pub(crate) fn is_terminal_write(self) -> bool {
        matches!(self, FragmentOutputKind::TerminalWrite)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FragmentStreamKind {
    Gather,
    Broadcast,
    Partitioned,
    Other,
}

#[derive(Clone, Debug)]
pub(crate) struct FragmentEdge {
    pub source_fragment_id: FragmentId,
    pub target_fragment_id: FragmentId,
    pub target_exchange_node_id: i32,
    pub output_partition: crate::sql::planner::DataPartition,
    pub stream_kind: FragmentStreamKind,
    pub edge_kind: FragmentEdgeKind,
    pub output_slot_ids: Vec<i32>,
}

#[derive(Clone, Debug)]
pub(crate) struct LoweredFragmentEdge {
    pub edge: FragmentEdge,
    pub compat_partition: partitions::TDataPartition,
}

/// Native per-fragment metadata used by scheduling and coordinator routing.
/// This intentionally contains no thrift plan, descriptor, sink, or exec-param
/// payload. Compat payloads stay in `FragmentBuildResult` until E9 gates the
/// compact lowering path.
#[derive(Clone, Debug)]
pub(crate) struct FragmentSchedulingMetadata {
    pub fragment_id: FragmentId,
    pub has_scan_nodes: bool,
    pub output_kind: FragmentOutputKind,
    pub native_scan_ranges:
        std::collections::BTreeMap<i32, Vec<crate::runtime::scan_range::ScanRangeParams>>,
    pub output_columns: Vec<OutputColumn>,
    pub boundary_schemas: Vec<boundary_schema::BoundarySchemaReport>,
    pub cte_id: Option<CteId>,
    pub cte_exchange_nodes: Vec<(CteId, i32, Vec<ColumnId>)>,
}

pub(crate) struct MultiFragmentBuildResult {
    /// Per-fragment build results.
    pub fragment_results: Vec<FragmentBuildResult>,
    pub fragment_schedules: Vec<FragmentSchedulingMetadata>,
    /// Which fragment is the root (result sink).
    pub root_fragment_id: FragmentId,
    /// Fragment-to-fragment data edges.
    pub edges: Vec<FragmentEdge>,
    /// Codegen-side lowering products for edges that still feed compat thrift runtime sinks.
    pub lowered_edges: Vec<LoweredFragmentEdge>,
    pub boundary_schemas: Vec<boundary_schema::BoundarySchemaReport>,
    /// Runtime filter planning result (populated for standalone mode).
    pub rf_plan: Option<RuntimeFilterPlanResult>,
}

impl MultiFragmentBuildResult {
    pub(crate) fn refresh_fragment_schedules(&mut self) {
        self.fragment_schedules = self
            .fragment_results
            .iter()
            .map(FragmentBuildResult::scheduling_metadata)
            .collect();
    }
}

/// Result of lowering runtime-filter annotations to execution wiring.
///
/// Assembled by [`fragment_builder::PlanFragmentBuilder`] directly from the
/// planner-side runtime-filter annotations on the distributed plan. Consumed by
/// the execution coordinator (`setup_runtime_filter_params`).
pub(crate) struct RuntimeFilterPlanResult {
    /// filter_id -> native RF descriptor for coordinator-side wiring.
    pub all_filters: std::collections::HashMap<i32, crate::sql::planner::PlannedRuntimeFilter>,
    /// fragment_id -> build-side filter IDs in that fragment.
    pub build_side_filters: std::collections::HashMap<FragmentId, Vec<i32>>,
    /// fragment_id -> (filter_id, probe_target_node_id) for probe-side targets.
    pub probe_side_filters: std::collections::HashMap<FragmentId, Vec<(i32, i32)>>,
}

/// Physical emission result for a single fragment.
pub(crate) struct FragmentBuildResult {
    pub fragment_id: FragmentId,
    pub has_scan_nodes: bool,
    pub output_kind: FragmentOutputKind,
    pub plan: plan_nodes::TPlan,
    pub desc_tbl: thrift_descriptors::TDescriptorTable,
    pub exec_params: internal_service::TPlanFragmentExecParams,
    pub native_scan_ranges:
        std::collections::BTreeMap<i32, Vec<crate::runtime::scan_range::ScanRangeParams>>,
    #[allow(dead_code)]
    // populated by fragment builder, will be read when standalone multi-fragment execution is wired
    pub output_sink: data_sinks::TDataSink,
    pub output_exprs: Option<Vec<crate::thrift::exprs::TExpr>>,
    pub output_columns: Vec<OutputColumn>,
    pub boundary_schemas: Vec<boundary_schema::BoundarySchemaReport>,
    /// CTE ID if this is a multicast fragment.
    pub cte_id: Option<CteId>,
    /// Exchange node IDs in this fragment that consume from CTE fragments:
    /// `(cte_id, exchange_node_id, receive_producer_column_ids)`.
    pub cte_exchange_nodes: Vec<(CteId, i32, Vec<ColumnId>)>,
    /// Per-fragment global dictionaries emitted to `TPlanFragment.query_global_dicts`.
    /// Standalone SQL lowering no longer populates this after the native Decode
    /// path was retired; external fragment producers may still carry it.
    pub query_global_dicts: Option<Vec<crate::thrift::data::TGlobalDict>>,
    /// Per-fragment dictionary expressions emitted to
    /// `TPlanFragment.query_global_dict_exprs`. Wired through for Task 7+;
    /// today this stays `None` because no codegen path populates it.
    pub query_global_dict_exprs:
        Option<std::collections::BTreeMap<i32, crate::thrift::exprs::TExpr>>,
}

impl FragmentBuildResult {
    pub(crate) fn scheduling_metadata(&self) -> FragmentSchedulingMetadata {
        FragmentSchedulingMetadata {
            fragment_id: self.fragment_id,
            has_scan_nodes: self.has_scan_nodes,
            output_kind: self.output_kind,
            native_scan_ranges: self.native_scan_ranges.clone(),
            output_columns: self.output_columns.clone(),
            boundary_schemas: self.boundary_schemas.clone(),
            cte_id: self.cte_id,
            cte_exchange_nodes: self.cte_exchange_nodes.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::proto_encode::types::decode_type;
    use crate::proto::common;

    #[test]
    fn proto_type_decode_is_available_to_sibling_lowering_modules() {
        let desc = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                r#type: common::PrimitiveType::Int as i32,
                len: None,
                precision: None,
                scale: None,
                time_unit: None,
            })),
        };

        assert_eq!(
            decode_type(&desc).expect("decode int TypeDesc"),
            DataType::Int32
        );
    }
}
