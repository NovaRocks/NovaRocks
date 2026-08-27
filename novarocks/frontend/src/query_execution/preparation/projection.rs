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

use super::scan::ScanExecutionBindings;
use novarocks_proto_codec::lifecycle::ScanRangeParams;
use novarocks_sql::plan_read::{BoundaryContract, ColumnId, CteId, FragmentEdge, FragmentId};
use novarocks_sql::planning::query_execution::{
    SqlPreparedRuntimeFilterFacts, SqlRuntimeFilterBindingFacts,
};

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
    runtime_filter_bindings: Vec<SqlRuntimeFilterBindingFacts>,
    scan_node_ids: Vec<i32>,
    execution_role: PreparedFragmentRole,
    boundary_projection: PreparedBoundaryProjection,
}

impl PreparedFragment {
    pub(crate) fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub(crate) fn runtime_filter_bindings(&self) -> &[SqlRuntimeFilterBindingFacts] {
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
}

/// Exact scan/binding preparation for one sealed distributed plan. Its fields
/// remain private; Frontend can only pass it to the native encoder paired with
/// the matching Core-produced plan carrier.
pub struct PreparedFragmentSet {
    by_fragment: BTreeMap<FragmentId, PreparedFragment>,
    scan_bindings: ScanExecutionBindings,
    projection: PreparedPlanProjection,
    // The SQL-owned sealed facts are shared with DistributedPlan. This carrier
    // cannot mutate or reconstruct the query-global graph.
    runtime_filter_facts: SqlPreparedRuntimeFilterFacts,
}

impl PreparedFragmentSet {
    pub(super) fn new(
        by_fragment: BTreeMap<FragmentId, PreparedFragment>,
        scan_bindings: ScanExecutionBindings,
        topological_fragment_order: Vec<FragmentId>,
        execution_anchor_fragment_id: FragmentId,
        edges: Vec<FragmentEdge>,
        runtime_filter_facts: SqlPreparedRuntimeFilterFacts,
    ) -> Self {
        Self {
            by_fragment,
            scan_bindings,
            projection: PreparedPlanProjection {
                topological_fragment_order,
                execution_anchor_fragment_id,
                edges,
            },
            runtime_filter_facts,
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

    pub(crate) fn runtime_filter_facts(&self) -> &SqlPreparedRuntimeFilterFacts {
        &self.runtime_filter_facts
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

    pub(crate) fn typed_scan(
        self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&'a super::scan::PreparedTypedConnectorScan> {
        self.scan_bindings.typed_scan(fragment_id, node_id)
    }

    #[allow(
        dead_code,
        reason = "Boundary projection remains available to target-gated native encoding paths."
    )]
    pub(crate) fn boundary_projection(
        self,
        fragment_id: FragmentId,
    ) -> Option<&'a PreparedBoundaryProjection> {
        self.fragment(fragment_id)
            .map(PreparedFragment::boundary_projection)
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "The fragment preparation boundary keeps each frozen planning fact explicit."
)]
pub(super) fn prepared_fragment(
    fragment_id: FragmentId,
    runtime_filter_bindings: Vec<SqlRuntimeFilterBindingFacts>,
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
