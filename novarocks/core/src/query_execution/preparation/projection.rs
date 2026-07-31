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

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::DataType;

use super::runtime_filter_binding::RuntimeFilterBindingTable;
use super::scan::ScanExecutionBindings;
use crate::runtime::scan_range::ScanRangeParams;
use crate::runtime_filter::model::graph::RuntimeFilterGraph;
use crate::sql::analysis::cte::CteId;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{BoundaryContract, FragmentEdge, FragmentId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparedOutputColumn {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) nullable: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreparedFragmentRole {
    Result,
    Statistics,
    TerminalWrite,
    NonTerminal,
}

impl PreparedFragmentRole {
    pub(crate) fn uses_result_buffer(self) -> bool {
        matches!(self, Self::Result)
    }

    pub(crate) fn is_statistics(self) -> bool {
        matches!(self, Self::Statistics)
    }

    pub(crate) fn is_terminal_write(self) -> bool {
        matches!(self, Self::TerminalWrite)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedBoundaryProjection {
    output_columns: Vec<PreparedOutputColumn>,
    cte_id: Option<CteId>,
    cte_exchange_nodes: Vec<(CteId, i32, Vec<ColumnId>)>,
    contracts: Vec<BoundaryContract>,
}

impl PreparedBoundaryProjection {
    pub(crate) fn output_columns(&self) -> &[PreparedOutputColumn] {
        &self.output_columns
    }

    pub(crate) fn cte_id(&self) -> Option<CteId> {
        self.cte_id
    }

    pub(crate) fn cte_exchange_nodes(&self) -> &[(CteId, i32, Vec<ColumnId>)] {
        &self.cte_exchange_nodes
    }

    pub(crate) fn contracts(&self) -> &[BoundaryContract] {
        &self.contracts
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedFragment {
    fragment_id: FragmentId,
    runtime_filter_bindings: RuntimeFilterBindingTable,
    scan_node_ids: Vec<i32>,
    execution_role: PreparedFragmentRole,
    boundary_projection: PreparedBoundaryProjection,
}

impl PreparedFragment {
    pub(crate) fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub(crate) fn runtime_filter_bindings(&self) -> &RuntimeFilterBindingTable {
        &self.runtime_filter_bindings
    }

    pub(crate) fn scan_node_ids(&self) -> &[i32] {
        &self.scan_node_ids
    }

    pub(crate) fn has_scan_nodes(&self) -> bool {
        !self.scan_node_ids.is_empty()
    }

    pub(crate) fn execution_role(&self) -> PreparedFragmentRole {
        self.execution_role
    }

    pub(crate) fn boundary_projection(&self) -> &PreparedBoundaryProjection {
        &self.boundary_projection
    }
}

#[derive(Clone, Debug)]
struct PreparedPlanProjection {
    topological_fragment_order: Vec<FragmentId>,
    execution_anchor_fragment_id: FragmentId,
    edges: Vec<FragmentEdge>,
    runtime_filter_join_progress: crate::sql::planner::distributed::JoinBuildProgressCatalog,
}

pub(crate) struct PreparedFragmentSet {
    by_fragment: BTreeMap<FragmentId, PreparedFragment>,
    scan_bindings: ScanExecutionBindings,
    projection: PreparedPlanProjection,
    // Task 4 consumes the sealed graph at the pre-submit compiler boundary.
    #[allow(dead_code)]
    runtime_filter_graph: RuntimeFilterGraph,
}

impl PreparedFragmentSet {
    pub(super) fn new(
        by_fragment: BTreeMap<FragmentId, PreparedFragment>,
        scan_bindings: ScanExecutionBindings,
        topological_fragment_order: Vec<FragmentId>,
        execution_anchor_fragment_id: FragmentId,
        edges: Vec<FragmentEdge>,
        runtime_filter_graph: RuntimeFilterGraph,
        runtime_filter_join_progress: crate::sql::planner::distributed::JoinBuildProgressCatalog,
    ) -> Self {
        Self {
            by_fragment,
            scan_bindings,
            projection: PreparedPlanProjection {
                topological_fragment_order,
                execution_anchor_fragment_id,
                edges,
                runtime_filter_join_progress,
            },
            runtime_filter_graph,
        }
    }

    pub(crate) fn scheduling_view(&self) -> PreparedFragmentSchedulingView<'_> {
        PreparedFragmentSchedulingView {
            by_fragment: &self.by_fragment,
            projection: &self.projection,
            scan_bindings: &self.scan_bindings,
        }
    }

    pub(crate) fn scan_bindings(&self) -> &ScanExecutionBindings {
        &self.scan_bindings
    }

    pub(crate) fn fragment_ids(&self) -> BTreeSet<FragmentId> {
        self.by_fragment.keys().copied().collect()
    }

    pub(crate) fn fragment(&self, fragment_id: FragmentId) -> Option<&PreparedFragment> {
        self.by_fragment.get(&fragment_id)
    }

    pub(crate) fn runtime_filter_graph(&self) -> &RuntimeFilterGraph {
        &self.runtime_filter_graph
    }

    pub(crate) fn runtime_filter_join_progress(
        &self,
    ) -> &crate::sql::planner::distributed::JoinBuildProgressCatalog {
        &self.projection.runtime_filter_join_progress
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PreparedFragmentSchedulingView<'a> {
    by_fragment: &'a BTreeMap<FragmentId, PreparedFragment>,
    projection: &'a PreparedPlanProjection,
    scan_bindings: &'a ScanExecutionBindings,
}

impl<'a> PreparedFragmentSchedulingView<'a> {
    pub(crate) fn fragment_ids(self) -> impl ExactSizeIterator<Item = FragmentId> + 'a {
        self.by_fragment.keys().copied()
    }

    pub(crate) fn fragments(self) -> impl ExactSizeIterator<Item = &'a PreparedFragment> + 'a {
        self.by_fragment.values()
    }

    pub(crate) fn fragment(self, fragment_id: FragmentId) -> Option<&'a PreparedFragment> {
        self.by_fragment.get(&fragment_id)
    }

    pub(crate) fn topological_order(self) -> &'a [FragmentId] {
        &self.projection.topological_fragment_order
    }

    pub(crate) fn execution_anchor(self) -> FragmentId {
        self.projection.execution_anchor_fragment_id
    }

    pub(crate) fn edges(self) -> &'a [FragmentEdge] {
        &self.projection.edges
    }

    pub(crate) fn scan_ranges(
        self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&'a [ScanRangeParams]> {
        self.scan_bindings.scan_ranges(fragment_id, node_id)
    }

    pub(crate) fn connector_read(
        self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&'a super::scan::PlannedConnectorRead> {
        self.scan_bindings.connector_read(fragment_id, node_id)
    }

    pub(crate) fn boundary_projection(
        self,
        fragment_id: FragmentId,
    ) -> Option<&'a PreparedBoundaryProjection> {
        self.fragment(fragment_id)
            .map(PreparedFragment::boundary_projection)
    }
}

pub(super) fn prepared_fragment(
    fragment_id: FragmentId,
    runtime_filter_bindings: RuntimeFilterBindingTable,
    scan_node_ids: Vec<i32>,
    execution_role: PreparedFragmentRole,
    output_columns: Vec<PreparedOutputColumn>,
    cte_id: Option<CteId>,
    cte_exchange_nodes: Vec<(CteId, i32, Vec<ColumnId>)>,
    contracts: Vec<BoundaryContract>,
) -> PreparedFragment {
    PreparedFragment {
        fragment_id,
        runtime_filter_bindings,
        scan_node_ids,
        execution_role,
        boundary_projection: PreparedBoundaryProjection {
            output_columns,
            cte_id,
            cte_exchange_nodes,
            contracts,
        },
    }
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub(crate) fn prepared_fragment_set_for_test(
    fragments: Vec<(
        FragmentId,
        PreparedFragmentRole,
        Vec<(i32, Vec<ScanRangeParams>)>,
    )>,
    topological_fragment_order: Vec<FragmentId>,
    execution_anchor_fragment_id: FragmentId,
    edges: Vec<FragmentEdge>,
) -> PreparedFragmentSet {
    let mut by_fragment = BTreeMap::new();
    let mut scan_bindings = ScanExecutionBindings::default();
    for (fragment_id, role, scan_nodes) in fragments {
        let mut scan_node_ids = Vec::new();
        for (node_id, ranges) in scan_nodes {
            scan_node_ids.push(node_id);
            scan_bindings
                .insert_scan_ranges(fragment_id, node_id, ranges)
                .expect("unique test scan range key");
        }
        scan_node_ids.sort_unstable();
        by_fragment.insert(
            fragment_id,
            prepared_fragment(
                fragment_id,
                RuntimeFilterBindingTable::empty(fragment_id),
                scan_node_ids,
                role,
                Vec::new(),
                None,
                Vec::new(),
                Vec::new(),
            ),
        );
    }
    PreparedFragmentSet::new(
        by_fragment,
        scan_bindings,
        topological_fragment_order,
        execution_anchor_fragment_id,
        edges,
        RuntimeFilterGraph::default(),
        Default::default(),
    )
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub(crate) fn prepared_fragment_set_with_runtime_filter_for_test(
    fragments: Vec<(
        FragmentId,
        PreparedFragmentRole,
        Vec<(i32, Vec<ScanRangeParams>)>,
    )>,
    topological_fragment_order: Vec<FragmentId>,
    execution_anchor_fragment_id: FragmentId,
    edges: Vec<FragmentEdge>,
    runtime_filter_graph: RuntimeFilterGraph,
    runtime_filter_join_progress: crate::sql::planner::distributed::JoinBuildProgressCatalog,
) -> PreparedFragmentSet {
    let mut prepared = prepared_fragment_set_for_test(
        fragments,
        topological_fragment_order,
        execution_anchor_fragment_id,
        edges,
    );
    prepared.runtime_filter_graph = runtime_filter_graph;
    prepared.projection.runtime_filter_join_progress = runtime_filter_join_progress;
    prepared
}
