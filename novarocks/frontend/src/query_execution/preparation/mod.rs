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

mod boundary;
mod cte;
mod native_encoding_view;
mod projection;
pub(crate) mod runtime_filter_binding;
pub(crate) mod runtime_filter_view;
pub(crate) mod scan;
pub(crate) mod scan_preparation;
mod topology;

use std::collections::{BTreeMap, BTreeSet};

use crate::catalog_application::query_bindings::QueryTableBindingStore;

use boundary::validate_and_group_boundary_contracts;
use cte::sealed_cte_projection;

pub use native_encoding_view::{
    NativeConnectorReadView, NativeRequiredReadReason, NativeRequiredReadView,
    NativeScanBindingView, NativeScanColumnKind, NativeScanColumnView, NativeScanExecutionKind,
    NativeScanFactsView,
};
pub use projection::PreparedFragmentSet;
pub(crate) use projection::{
    PreparedFragment, PreparedFragmentRole, PreparedFragmentSchedulingView, PreparedOutputColumn,
};
pub(crate) use scan_preparation::ScanPreparationOptions;
use scan_preparation::prepare_scan_bindings;
use topology::{collect_scan_nodes, validate_binding_keys, validate_topology_roles};

pub(crate) fn prepare_fragments(
    plan: &novarocks_sql::plan_read::DistributedPlan,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    query_table_bindings: Option<&QueryTableBindingStore>,
    resolver: Option<&dyn scan::ScanBindingResolver>,
    scan_options: ScanPreparationOptions,
) -> Result<PreparedFragmentSet, String> {
    let preparation_facts =
        novarocks_sql::planning::query_execution::project_execution_preparation_facts(plan);
    let runtime_filter_facts =
        novarocks_sql::planning::query_execution::project_runtime_filter_facts(plan)?;
    let sealed_ids = plan
        .fragments()
        .iter()
        .map(|fragment| fragment.fragment_id)
        .collect::<BTreeSet<_>>();
    let topological_fragment_order = preparation_facts.topological_fragment_order().to_vec();
    let ordered_ids = topological_fragment_order
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if ordered_ids.len() != topological_fragment_order.len() || ordered_ids != sealed_ids {
        return Err(format!(
            "prepared fragment topology order {topological_fragment_order:?} does not match sealed fragment ids {sealed_ids:?}"
        ));
    }
    let execution_anchor_fragment_id = preparation_facts.execution_anchor_fragment_id();
    if !sealed_ids.contains(&execution_anchor_fragment_id) {
        return Err(format!(
            "prepared execution anchor fragment {execution_anchor_fragment_id} is not among sealed fragment ids {sealed_ids:?}"
        ));
    }
    let result_fragment_id = preparation_facts.result_fragment_id();
    let terminal_write_fragment_ids = preparation_facts
        .terminal_write_fragment_ids()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let producer_fragment_ids = preparation_facts
        .producer_fragment_ids()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    validate_topology_roles(
        &sealed_ids,
        result_fragment_id,
        &terminal_write_fragment_ids,
        &producer_fragment_ids,
        execution_anchor_fragment_id,
    )?;
    // A terminal writer has an Iceberg-write boundary even when it has no
    // query output contract. The latter is an application result-shape fact,
    // while the former is part of the sealed execution topology.
    let write_contract_fragment_ids = terminal_write_fragment_ids.clone();
    let boundary_contracts = validate_and_group_boundary_contracts(
        result_fragment_id,
        &write_contract_fragment_ids,
        plan.edges(),
        preparation_facts.boundary_contracts(),
        &sealed_ids,
    )?;
    let scan_bindings = prepare_scan_bindings(
        plan,
        controls,
        context,
        query_table_bindings,
        resolver,
        scan_options,
    )?;
    // Scan-domain target ordinals are derived only after the exact provider read
    // is pinned.  Never materialize RF bindings against a later catalog view.
    let source_resolutions = runtime_filter_binding::resolve_runtime_filter_source_targets(
        runtime_filter_facts.source_scan_requests().cloned(),
        &scan_bindings,
    )?;
    let runtime_filter_facts =
        novarocks_sql::planning::query_execution::finalize_runtime_filter_facts(
            runtime_filter_facts,
            source_resolutions,
        )?;

    let mut by_fragment = BTreeMap::new();
    let mut expected_range_keys = BTreeSet::new();
    let mut expected_binding_node_ids = BTreeSet::new();
    let mut expected_connector_read_keys = BTreeSet::new();
    for fragment in plan.fragments() {
        let mut scan_nodes = Vec::new();
        collect_scan_nodes(fragment.fragment_id, &fragment.root, &mut scan_nodes);
        scan_nodes.sort_by_key(|(node_id, _)| *node_id);
        for (node_id, _source) in &scan_nodes {
            expected_range_keys.insert((fragment.fragment_id, *node_id));
            if scan_bindings
                .scan_ranges(fragment.fragment_id, *node_id)
                .is_none()
            {
                return Err(format!(
                    "prepared fragment missing scan ranges fragment_id={} node_id={node_id}",
                    fragment.fragment_id
                ));
            }
            expected_binding_node_ids.insert(*node_id);
            if scan_bindings.binding(*node_id).is_none() {
                return Err(format!(
                    "prepared fragment missing scan binding fragment_id={} node_id={node_id}",
                    fragment.fragment_id
                ));
            }
            if scan_bindings
                .connector_read(fragment.fragment_id, *node_id)
                .is_some()
            {
                expected_connector_read_keys.insert((fragment.fragment_id, *node_id));
            }
        }
        let scan_node_ids = scan_nodes.into_iter().map(|(node_id, _)| node_id).collect();
        let execution_role = if matches!(
            &fragment.sink,
            novarocks_sql::plan_read::DataSink::Statistics(_)
        ) {
            PreparedFragmentRole::Statistics
        } else if result_fragment_id == Some(fragment.fragment_id) {
            PreparedFragmentRole::Result
        } else if terminal_write_fragment_ids.contains(&fragment.fragment_id) {
            PreparedFragmentRole::TerminalWrite
        } else {
            PreparedFragmentRole::NonTerminal
        };
        // Query-path output is finalized by FragmentEdgeOutputCatalog. Iceberg
        // target-schema output belongs to WriteContractCatalog and is not a
        // fetch/result projection. The two sealed catalogs are complementary:
        // exactly write fragments must be absent from fragment-edge outputs.
        let sealed_output_columns = plan
            .fragment_edge_outputs()
            .fragment_output_columns(fragment.fragment_id);
        let output_columns = match (
            write_contract_fragment_ids.contains(&fragment.fragment_id),
            sealed_output_columns,
        ) {
            // A connector writer's carrier output is not a query result. The
            // write contract owns its target schema; preparation therefore
            // deliberately projects no query-output columns whether or not
            // sealing retained the writer's carrier columns.
            (true, _) => Vec::new(),
            (false, Some(columns)) => columns
                .iter()
                .map(|column| PreparedOutputColumn {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                })
                .collect(),
            (false, None) => {
                return Err(format!(
                    "prepared sealed output mismatch fragment_id={}: non-write fragment is missing FragmentEdgeOutputCatalog output",
                    fragment.fragment_id
                ));
            }
        };
        let (cte_id, cte_exchange_nodes) = sealed_cte_projection(plan.edges(), fragment)?;
        let contracts = boundary_contracts
            .get(&fragment.fragment_id)
            .cloned()
            .unwrap_or_default();
        let prepared = projection::prepared_fragment(
            fragment.fragment_id,
            runtime_filter_facts
                .bindings_for_fragment(fragment.fragment_id)
                .to_vec(),
            scan_node_ids,
            execution_role,
            output_columns,
            cte_id,
            cte_exchange_nodes,
            contracts,
        );
        if by_fragment.insert(fragment.fragment_id, prepared).is_some() {
            return Err(format!(
                "duplicate prepared fragment id={}",
                fragment.fragment_id
            ));
        }
    }

    validate_binding_keys(
        "scan ranges",
        &expected_range_keys,
        &scan_bindings.scan_range_keys().collect(),
    )?;
    validate_binding_keys(
        "scan bindings",
        &expected_binding_node_ids,
        &scan_bindings.binding_node_ids().collect(),
    )?;
    validate_binding_keys(
        "connector reads",
        &expected_connector_read_keys,
        &scan_bindings.connector_read_keys().collect(),
    )?;
    Ok(PreparedFragmentSet::new(
        by_fragment,
        scan_bindings,
        topological_fragment_order,
        execution_anchor_fragment_id,
        plan.edges().to_vec(),
        runtime_filter_facts,
    ))
}

pub(crate) fn prepared_fragment_set_for_native_encode_test(
    plan: &novarocks_sql::plan_read::DistributedPlan,
) -> Result<PreparedFragmentSet, String> {
    let preparation_facts =
        novarocks_sql::planning::query_execution::project_execution_preparation_facts(plan);
    let runtime_filter_facts =
        novarocks_sql::planning::query_execution::project_runtime_filter_facts(plan)?;
    let runtime_filter_facts =
        novarocks_sql::planning::query_execution::finalize_runtime_filter_facts(
            runtime_filter_facts,
            Vec::new(),
        )?;
    let result_fragment_id = preparation_facts.result_fragment_id();
    let terminal_write_fragment_ids = preparation_facts
        .terminal_write_fragment_ids()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut by_fragment = BTreeMap::new();
    for fragment in plan.fragments() {
        let role = if matches!(
            &fragment.sink,
            novarocks_sql::plan_read::DataSink::Statistics(_)
        ) {
            PreparedFragmentRole::Statistics
        } else if result_fragment_id == Some(fragment.fragment_id) {
            PreparedFragmentRole::Result
        } else if terminal_write_fragment_ids.contains(&fragment.fragment_id) {
            PreparedFragmentRole::TerminalWrite
        } else {
            PreparedFragmentRole::NonTerminal
        };
        by_fragment.insert(
            fragment.fragment_id,
            projection::prepared_fragment(
                fragment.fragment_id,
                runtime_filter_facts
                    .bindings_for_fragment(fragment.fragment_id)
                    .to_vec(),
                Vec::new(),
                role,
                Vec::new(),
                None,
                Vec::new(),
                Vec::new(),
            ),
        );
    }
    Ok(PreparedFragmentSet::new(
        by_fragment,
        scan::ScanExecutionBindings::default(),
        preparation_facts.topological_fragment_order().to_vec(),
        preparation_facts.execution_anchor_fragment_id(),
        plan.edges().to_vec(),
        runtime_filter_facts,
    ))
}

#[cfg(test)]
mod test_support {
    pub(super) fn result_plan() -> novarocks_sql::plan_read::DistributedPlan {
        novarocks_sql::test_support::native_preparation_plan(
            novarocks_sql::test_support::NativePreparationFixture::ResultOutput,
        )
        .expect("sealed result preparation fixture")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn write_plan() -> novarocks_sql::plan_read::DistributedPlan {
        novarocks_sql::test_support::native_preparation_plan(
            novarocks_sql::test_support::NativePreparationFixture::TerminalWrite,
        )
        .expect("sealed terminal-write preparation fixture")
    }

    #[test]
    fn production_preparation_accepts_write_without_query_output_contract() {
        let plan = write_plan();
        assert!(
            plan.fragment_edge_outputs()
                .fragment_output_columns(9)
                .is_some_and(|columns| !columns.is_empty())
        );
        let registry = novarocks::connector::ConnectorRegistry::new();
        let controls = novarocks::connector::FixtureControlResolver::new(registry.clone());
        let prepared = prepare_fragments(
            &plan,
            &controls,
            &novarocks::connector::test_request_context(),
            None,
            None,
            ScanPreparationOptions::single_backend_fixture(),
        )
        .expect("sealed write output absence is legal");
        assert!(
            prepared
                .fragment(9)
                .expect("prepared writer")
                .boundary_projection()
                .output_columns()
                .is_empty()
        );
    }

    #[test]
    fn production_preparation_rejects_missing_non_write_output_contract() {
        let plan = novarocks_sql::test_support::native_preparation_plan(
            novarocks_sql::test_support::NativePreparationFixture::MissingResultOutput,
        )
        .expect("closed missing-output preparation fixture");
        let registry = novarocks::connector::ConnectorRegistry::new();
        let controls = novarocks::connector::FixtureControlResolver::new(registry.clone());
        let error = match prepare_fragments(
            &plan,
            &controls,
            &novarocks::connector::test_request_context(),
            None,
            None,
            ScanPreparationOptions::single_backend_fixture(),
        ) {
            Ok(_) => {
                panic!("non-write output absence must fail through production preparation")
            }
            Err(error) => error,
        };
        assert_eq!(
            error,
            "prepared sealed output mismatch fragment_id=7: non-write fragment is missing FragmentEdgeOutputCatalog output"
        );
    }
}
