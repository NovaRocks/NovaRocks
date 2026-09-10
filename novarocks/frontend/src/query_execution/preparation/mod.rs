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

mod attempt_access;
mod boundary;
mod cte;
mod native_encoding_view;
mod projection;
pub(crate) mod runtime_filter_binding;
pub(crate) mod runtime_filter_view;
pub(crate) mod scan;
pub(crate) mod scan_preparation;
mod topology;
#[allow(
    dead_code,
    reason = "Predicate lowering exposes the full typed vocabulary; scan preparation uses the entry points, the round driver uses the rest."
)]
pub(crate) mod typed_predicate;
pub(crate) mod typed_scan;

use std::collections::{BTreeMap, BTreeSet};

use crate::catalog_application::query_bindings::QueryTableBindingStore;

use boundary::validate_and_group_boundary_contracts;
use cte::sealed_cte_projection;

pub(crate) use attempt_access::{
    ConnectorAttemptAccessEntry, ConnectorAttemptAccessPlan, FrozenDescriptionInputs,
};
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
use topology::{collect_scan_nodes, validate_binding_keys, validate_topology_roles};

/// Move-only handoff from preparation into logical-query assembly. The pure
/// fragment set is kept separate from the Catalog-tracked process-local access
/// scope and the receipts that P4.2 will consume while sealing the description.
pub(crate) struct PreparedFragmentHandoff {
    sealed_plan: novarocks_sql::planning::query_execution::SealedPreparationPlan,
    prepared: PreparedFragmentSet,
    attempt_access: attempt_access::ConnectorAttemptAccessPlan,
    description_inputs: attempt_access::FrozenDescriptionInputs,
    selected_mv_query_inputs:
        Option<novarocks_query_application::preparation::SelectedMvQueryInputs>,
}

impl PreparedFragmentHandoff {
    pub(crate) const fn sealed_plan(
        &self,
    ) -> &novarocks_sql::planning::query_execution::SealedPreparationPlan {
        &self.sealed_plan
    }

    pub(crate) const fn prepared(&self) -> &PreparedFragmentSet {
        &self.prepared
    }

    pub(crate) fn for_test(
        sealed_plan: novarocks_sql::planning::query_execution::SealedPreparationPlan,
        prepared: PreparedFragmentSet,
    ) -> Self {
        Self {
            sealed_plan,
            prepared,
            attempt_access: attempt_access::ConnectorAttemptAccessPlanBuilder::new().finish(),
            description_inputs: attempt_access::FrozenDescriptionInputsBuilder::new().finish(),
            selected_mv_query_inputs: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn negotiation_receipt_count(&self) -> usize {
        self.description_inputs.len()
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        novarocks_sql::planning::query_execution::SealedPreparationPlan,
        PreparedFragmentSet,
        attempt_access::ConnectorAttemptAccessPlan,
        attempt_access::FrozenDescriptionInputs,
        Option<novarocks_query_application::preparation::SelectedMvQueryInputs>,
    ) {
        (
            self.sealed_plan,
            self.prepared,
            self.attempt_access,
            self.description_inputs,
            self.selected_mv_query_inputs,
        )
    }
}

impl std::ops::Deref for PreparedFragmentHandoff {
    type Target = PreparedFragmentSet;

    fn deref(&self) -> &Self::Target {
        self.prepared()
    }
}

pub(crate) fn prepare_fragments(
    plan: &novarocks_sql::plan_read::DistributedPlan,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    query_table_bindings: Option<&QueryTableBindingStore>,
    resolver: Option<&dyn scan::ScanBindingResolver>,
    scan_options: ScanPreparationOptions,
) -> Result<PreparedFragmentHandoff, String> {
    let sealed_plan =
        novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(plan.clone());
    prepare_fragments_for_sealed_plan(
        &sealed_plan,
        controls,
        context,
        query_table_bindings,
        resolver,
        scan_options,
    )
}

pub(crate) fn prepare_fragments_for_sealed_plan(
    sealed_plan: &novarocks_sql::planning::query_execution::SealedPreparationPlan,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    query_table_bindings: Option<&QueryTableBindingStore>,
    resolver: Option<&dyn scan::ScanBindingResolver>,
    scan_options: ScanPreparationOptions,
) -> Result<PreparedFragmentHandoff, String> {
    let plan = sealed_plan.plan();
    let mut mv_rewrite_actions = sealed_plan
        .scan_contracts()?
        .into_iter()
        .filter_map(|scan| scan.mv_rewrite_action());
    let mv_rewrite_action = mv_rewrite_actions.next();
    if mv_rewrite_actions.next().is_some() {
        return Err("sealed distributed plan selects more than one MV rewrite action".to_string());
    }
    let scan_options = &scan_options;
    let preparation_facts =
        novarocks_sql::planning::query_execution::project_execution_preparation_facts(plan);
    let runtime_filter_facts =
        novarocks_sql::planning::query_execution::project_runtime_filter_facts(plan)?;
    let write_root_targets = project_write_root_targets(plan)?;
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
    let producer_fragment_ids = preparation_facts
        .producer_fragment_ids()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    validate_topology_roles(
        &sealed_ids,
        result_fragment_id,
        &producer_fragment_ids,
        execution_anchor_fragment_id,
    )?;
    let boundary_contracts = validate_and_group_boundary_contracts(
        result_fragment_id,
        plan.edges(),
        preparation_facts.boundary_contracts(),
        &sealed_ids,
    )?;
    // Every scan-domain request is handed to scan preparation, so the scan that
    // owns it declares the filter on its own typed carrier. A filter added
    // after the relation is frozen would never reach the reader.
    let source_scan_requests = runtime_filter_facts
        .source_scan_requests()
        .cloned()
        .collect::<Vec<_>>();
    let prepared_scans = scan_preparation::prepare_scan_bindings_for_sealed_plan(
        sealed_plan,
        controls,
        context,
        query_table_bindings,
        resolver,
        scan_options,
        &source_scan_requests,
    )?;
    let scan_preparation::PreparedScanSet {
        bindings: scan_bindings,
        native_connector_scans,
        attempt_access,
        description_inputs,
    } = prepared_scans;
    let selected_mv_query_inputs = match mv_rewrite_action {
        Some(action) => Some(
            query_table_bindings
                .ok_or_else(|| {
                    "selected MV rewrite requires the query's exact pre-rewrite binding store"
                        .to_string()
                })?
                .prove_selected_mv_query_inputs(action)?,
        ),
        None => None,
    };
    // Resolution runs against the typed scans preparation just froze, never
    // against a later catalog view.
    let source_resolutions = runtime_filter_binding::resolve_runtime_filter_source_targets(
        source_scan_requests,
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
    let mut expected_typed_scan_keys = BTreeSet::new();
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
                .typed_scan(fragment.fragment_id, *node_id)
                .is_some()
            {
                expected_typed_scan_keys.insert((fragment.fragment_id, *node_id));
            }
        }
        let scan_node_ids = scan_nodes.into_iter().map(|(node_id, _)| node_id).collect();
        let execution_role = if result_fragment_id == Some(fragment.fragment_id) {
            PreparedFragmentRole::Result
        } else {
            PreparedFragmentRole::NonTerminal
        };
        // Query-path output is finalized by FragmentEdgeOutputCatalog.
        let output_columns = match plan
            .fragment_edge_outputs()
            .fragment_output_columns(fragment.fragment_id)
        {
            Some(columns) => columns
                .iter()
                .map(|column| PreparedOutputColumn {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                })
                .collect(),
            None => {
                return Err(format!(
                    "prepared sealed output mismatch fragment_id={}: fragment is missing FragmentEdgeOutputCatalog output",
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
        "typed connector scans",
        &expected_typed_scan_keys,
        &scan_bindings.typed_scan_keys().collect(),
    )?;
    let prepared = PreparedFragmentSet::new(
        by_fragment,
        scan_bindings,
        topological_fragment_order,
        execution_anchor_fragment_id,
        plan.edges().to_vec(),
        runtime_filter_facts,
        write_root_targets,
        native_connector_scans,
    );
    Ok(PreparedFragmentHandoff {
        sealed_plan: sealed_plan.clone(),
        prepared,
        attempt_access,
        description_inputs,
        selected_mv_query_inputs,
    })
}

fn project_write_root_targets(
    plan: &novarocks_sql::plan_read::DistributedPlan,
) -> Result<Option<Vec<novarocks_spi::connector::write_stack::WriteTargetOrdinal>>, String> {
    let mut projected = None;
    for fragment in plan.fragments() {
        let novarocks_sql::plan_read::DistributedNodeKind::TableFinish(finish) =
            &fragment.root.payload
        else {
            continue;
        };
        if projected.is_some() {
            return Err("sealed distributed plan contains more than one TableFinish root".into());
        }
        projected = Some(finish.expected_target_ordinals().to_vec());
    }
    Ok(projected)
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
    let write_root_targets = project_write_root_targets(plan)?;
    let result_fragment_id = preparation_facts.result_fragment_id();
    let mut by_fragment = BTreeMap::new();
    for fragment in plan.fragments() {
        let role = if result_fragment_id == Some(fragment.fragment_id) {
            PreparedFragmentRole::Result
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
        write_root_targets,
        native_encoding_view::FrozenNativeConnectorScansBuilder::new().finish(),
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

    #[test]
    fn production_preparation_rejects_a_missing_output_contract() {
        let plan = novarocks_sql::test_support::native_preparation_plan(
            novarocks_sql::test_support::NativePreparationFixture::MissingResultOutput,
        )
        .expect("closed missing-output preparation fixture");
        let registry = crate::connector::FixtureConnectorRegistry::new();
        let controls = crate::connector::FixtureControlResolver::new(registry.clone());
        let error = match prepare_fragments(
            &plan,
            &controls,
            &crate::connector::test_request_context(),
            None,
            None,
            ScanPreparationOptions::single_backend_fixture(),
        ) {
            Ok(_) => {
                panic!("output absence must fail through production preparation")
            }
            Err(error) => error,
        };
        assert_eq!(
            error,
            "prepared sealed output mismatch fragment_id=7: fragment is missing FragmentEdgeOutputCatalog output"
        );
    }
}
