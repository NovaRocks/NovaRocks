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
mod iceberg_delta;
mod projection;
pub(crate) mod runtime_filter_binding;
pub(crate) mod scan;
mod scan_preparation;
mod topology;

use std::collections::{BTreeMap, BTreeSet};

use crate::connector::ConnectorRegistry;
use crate::sql::planner::table::ScanSource;

use boundary::validate_and_group_boundary_contracts;
use cte::sealed_cte_projection;

pub(crate) use projection::{
    PreparedFragment, PreparedFragmentRole, PreparedFragmentSchedulingView, PreparedFragmentSet,
    PreparedOutputColumn,
};
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub(crate) use projection::{
    prepared_fragment_set_for_test, prepared_fragment_set_with_runtime_filter_for_test,
};
pub(crate) use scan_preparation::ScanPreparationOptions;
use scan_preparation::prepare_scan_bindings;
use topology::{collect_scan_nodes, validate_binding_keys, validate_topology_roles};

pub(crate) fn prepare_fragments(
    plan: &crate::sql::planner::distributed::DistributedPlan,
    connectors: &ConnectorRegistry,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    resolver: Option<&dyn scan::ScanBindingResolver>,
    scan_options: ScanPreparationOptions,
) -> Result<PreparedFragmentSet, String> {
    let sealed_ids = plan
        .fragments()
        .iter()
        .map(|fragment| fragment.fragment_id)
        .collect::<BTreeSet<_>>();
    let topological_fragment_order = plan.topology().topological_fragment_order().to_vec();
    let ordered_ids = topological_fragment_order
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if ordered_ids.len() != topological_fragment_order.len() || ordered_ids != sealed_ids {
        return Err(format!(
            "prepared fragment topology order {topological_fragment_order:?} does not match sealed fragment ids {sealed_ids:?}"
        ));
    }
    let execution_anchor_fragment_id = plan.topology().execution_anchor_fragment_id();
    if !sealed_ids.contains(&execution_anchor_fragment_id) {
        return Err(format!(
            "prepared execution anchor fragment {execution_anchor_fragment_id} is not among sealed fragment ids {sealed_ids:?}"
        ));
    }
    let result_fragment_id = plan.topology().result_fragment_id();
    let terminal_write_fragment_ids = plan
        .topology()
        .terminal_write_fragment_ids()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let producer_fragment_ids = plan
        .topology()
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
    let write_contract_fragment_ids = sealed_ids
        .iter()
        .copied()
        .filter(|&fragment_id| {
            plan.write_contracts()
                .connector_write_output(fragment_id)
                .is_some()
        })
        .collect::<BTreeSet<_>>();
    let boundary_contracts = validate_and_group_boundary_contracts(
        result_fragment_id,
        &write_contract_fragment_ids,
        plan.edges(),
        plan.boundaries().contracts(),
        &sealed_ids,
    )?;
    let mut runtime_filter_binding_tables =
        runtime_filter_binding::materialize_runtime_filter_binding_tables(
            plan.runtime_filter_graph(),
            plan.fragments(),
        )?;
    let scan_bindings =
        prepare_scan_bindings(plan, connectors, controls, context, resolver, scan_options)?;

    let mut by_fragment = BTreeMap::new();
    let mut expected_range_keys = BTreeSet::new();
    let mut expected_binding_node_ids = BTreeSet::new();
    let mut expected_connector_read_keys = BTreeSet::new();
    let mut expected_starrocks_node_ids = BTreeSet::new();
    for fragment in plan.fragments() {
        let mut scan_nodes = Vec::new();
        collect_scan_nodes(fragment.fragment_id, &fragment.root, &mut scan_nodes);
        scan_nodes.sort_by_key(|(node_id, _)| *node_id);
        for (node_id, source) in &scan_nodes {
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
            match source {
                ScanSource::IcebergMetadataTable { .. } => {}
                ScanSource::StarRocks { .. } => {
                    expected_starrocks_node_ids.insert(*node_id);
                    if scan_bindings.starrocks_source(*node_id).is_none() {
                        return Err(format!(
                            "prepared fragment missing StarRocks source fragment_id={} node_id={node_id}",
                            fragment.fragment_id
                        ));
                    }
                }
                _ => {
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
            }
        }
        let scan_node_ids = scan_nodes.into_iter().map(|(node_id, _)| node_id).collect();
        let execution_role = if matches!(
            &fragment.sink,
            crate::sql::planner::distributed::DataSink::Statistics(_)
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
            (true, None) => Vec::new(),
            (true, Some(_)) => {
                return Err(format!(
                    "prepared sealed output mismatch fragment_id={}: Iceberg write fragment unexpectedly has FragmentEdgeOutputCatalog output",
                    fragment.fragment_id
                ));
            }
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
            runtime_filter_binding_tables
                .remove(&fragment.fragment_id)
                .expect("binding materialization creates one table per fragment"),
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
    debug_assert!(runtime_filter_binding_tables.is_empty());

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
    validate_binding_keys(
        "StarRocks descriptors",
        &expected_starrocks_node_ids,
        &scan_bindings.starrocks_source_node_ids().collect(),
    )?;

    Ok(PreparedFragmentSet::new(
        by_fragment,
        scan_bindings,
        topological_fragment_order,
        execution_anchor_fragment_id,
        plan.edges().to_vec(),
        plan.runtime_filter_graph().clone(),
        plan.runtime_filter_join_progress().clone(),
    ))
}

#[cfg(test)]
pub(crate) fn prepared_fragment_set_for_native_encode_test(
    plan: &crate::sql::planner::distributed::DistributedPlan,
) -> Result<PreparedFragmentSet, String> {
    let mut binding_tables = runtime_filter_binding::materialize_runtime_filter_binding_tables(
        plan.runtime_filter_graph(),
        plan.fragments(),
    )?;
    let result_fragment_id = plan.topology().result_fragment_id();
    let terminal_write_fragment_ids = plan
        .topology()
        .terminal_write_fragment_ids()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut by_fragment = BTreeMap::new();
    for fragment in plan.fragments() {
        let role = if matches!(
            &fragment.sink,
            crate::sql::planner::distributed::DataSink::Statistics(_)
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
                binding_tables
                    .remove(&fragment.fragment_id)
                    .expect("binding materialization creates one table per fragment"),
                Vec::new(),
                role,
                Vec::new(),
                None,
                Vec::new(),
                Vec::new(),
            ),
        );
    }
    debug_assert!(binding_tables.is_empty());
    Ok(PreparedFragmentSet::new(
        by_fragment,
        scan::ScanExecutionBindings::default(),
        plan.topology().topological_fragment_order().to_vec(),
        plan.topology().execution_anchor_fragment_id(),
        plan.edges().to_vec(),
        plan.runtime_filter_graph().clone(),
        plan.runtime_filter_join_progress().clone(),
    ))
}

#[cfg(test)]
mod test_support {
    use arrow::datatypes::DataType;

    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
    };
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    pub(super) fn result_plan() -> crate::sql::planner::distributed::DistributedPlan {
        let columns = vec![
            OutputColumn {
                column_id: ColumnId::new_for_test(1),
                name: "a".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId::new_for_test(2),
                name: "b".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                is_internal: false,
            },
        ];
        let fragment = PlanFragment {
            fragment_id: 7,
            root: DistributedNode {
                node_id: 70,
                fragment_id: 7,
                tuple_ids: vec![70],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: PhysicalPlanStats {
                    output_row_count: 0.0,
                    row_count_confidence: PlannerConfidence::Fallback,
                    column_statistics: Default::default(),
                    cost_estimate: None,
                    broadcast_decision: None,
                },
                payload: DistributedNodeKind::Values(PlanValuesNode {
                    rows: Vec::new(),
                    columns: columns.clone(),
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![fragment],
            root_fragment_id: 7,
            edges: Vec::new(),
            runtime_filter_graph: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::runtime_filter::model::contract::{
        BindingId, ChannelId, ConsumerActivation, LateApplyGranularity,
    };
    use crate::runtime_filter::model::graph::RuntimeFilterBindingRoleData;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
    };
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    fn draft_runtime_filter_graph() -> crate::sql::planner::distributed::DraftRuntimeFilterGraph {
        use crate::runtime_filter::model::contract::{
            ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
            ContributionKind, CoverageWitnessId, LateApplyGranularity, NullSemantics,
            PlanFragmentId, PlanNodeId, ReductionRequirement, RuntimeFilterLifecycle,
            RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
        };
        use crate::runtime_filter::model::coverage::Coverage;
        use crate::runtime_filter::model::graph::{
            ApplyPoint, ConsumerBindingTarget, ConsumerRequirementData, PlanLocation,
            ProducerRequirement, RuntimeFilterBindingRoleData, RuntimeFilterBindingSpecData,
            RuntimeFilterChannelSpec,
        };
        use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
        use crate::sql::planner::distributed::{
            ActivationConstraint, DraftRuntimeFilterGraph, RequiredLiveReason,
        };

        let channel_id = ChannelId::new(1);
        let witness_id = CoverageWitnessId::new(1);
        let location = PlanLocation {
            fragment_id: PlanFragmentId::new(7),
            node_id: PlanNodeId::new(70),
        };
        let expression = || TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        };
        let mut graph = DraftRuntimeFilterGraph::default();
        graph
            .insert_channel(RuntimeFilterChannelSpec {
                channel_id,
                logical_domain: RuntimeFilterLogicalDomain::Membership {
                    value_type: DataType::Int64,
                    null_semantics: NullSemantics::NullSafeEqual,
                },
                lifecycle: RuntimeFilterLifecycle::CompleteOnce,
                availability_coverage: Coverage::AllOf(vec![Coverage::Leaf(witness_id)]),
                terminal_coverage: Coverage::AllOf(vec![Coverage::Leaf(witness_id)]),
                reduction_requirement: ReductionRequirement::SetUnion,
                allowed_contribution_kinds: BTreeSet::from([
                    ContributionKind::FinalDomainShard,
                    ContributionKind::ProducerClosed,
                ]),
                required_consumer_capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                policy: RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: 1024,
                    max_artifact_bytes: 4096,
                    deadline_ms: 30_000,
                    max_retries: 3,
                },
            })
            .expect("unique channel");
        graph
            .insert_binding(RuntimeFilterBindingSpecData {
                binding_id: BindingId::new(1),
                channel_id,
                coverage_witness_id: Some(witness_id),
                location,
                expression: expression(),
                apply_point: ApplyPoint::NodeOutput,
                role: RuntimeFilterBindingRoleData::Producer(ProducerRequirement {
                    contribution_kinds: BTreeSet::from([
                        ContributionKind::FinalDomainShard,
                        ContributionKind::ProducerClosed,
                    ]),
                    completion_requirement: CompletionRequirement::FencedFinalDomain(
                        CompletionFenceKind::CommittedDomainFrozen,
                    ),
                    target:
                        crate::runtime_filter::model::graph::ProducerBindingTarget::JoinBuildKey {
                            ordinal: 0,
                        },
                }),
            })
            .expect("unique producer binding");
        graph
            .insert_binding(RuntimeFilterBindingSpecData {
                binding_id: BindingId::new(2),
                channel_id,
                coverage_witness_id: None,
                location,
                expression: expression(),
                apply_point: ApplyPoint::NodeInput,
                role: RuntimeFilterBindingRoleData::Consumer(ConsumerRequirementData {
                    capabilities: BTreeSet::from([
                        ArtifactCapability::Membership,
                        ArtifactCapability::EmptyDomain,
                    ]),
                    activation: ActivationConstraint::LiveOnly {
                        late_apply: LateApplyGranularity::Batch,
                        reason: RequiredLiveReason::FencedFinalDomainContract,
                    },
                    target: ConsumerBindingTarget::DirectInput { input_ordinal: 0 },
                }),
            })
            .expect("unique consumer binding");
        graph
    }

    fn write_plan() -> crate::sql::planner::distributed::DistributedPlan {
        let columns = vec![OutputColumn {
            column_id: ColumnId::new_for_test(1),
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }];
        let fragment = PlanFragment {
            fragment_id: 9,
            root: DistributedNode {
                node_id: 90,
                fragment_id: 9,
                tuple_ids: vec![90],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: PhysicalPlanStats {
                    output_row_count: 0.0,
                    row_count_confidence: PlannerConfidence::Fallback,
                    column_statistics: Default::default(),
                    cost_estimate: None,
                    broadcast_decision: None,
                },
                payload: DistributedNodeKind::Values(PlanValuesNode {
                    rows: Vec::new(),
                    columns: columns.clone(),
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::ConnectorWrite(
                crate::sql::planner::distributed::write::sink::ConnectorWriteFragmentSink {
                    handle: None,
                    input: crate::sql::planner::distributed::write::sink::ConnectorWriteInputBinding::RootOutputByOrdinal,
                    output_contract: None,
                },
            ),
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![fragment],
            root_fragment_id: 9,
            edges: Vec::new(),
            runtime_filter_graph: Default::default(),
        }
    }

    #[test]
    fn production_preparation_accepts_write_without_query_output_contract() {
        let plan = write_plan();
        assert!(
            plan.fragment_edge_outputs()
                .fragment_output_columns(9)
                .is_none()
        );
        let registry = crate::connector::ConnectorRegistry::new();
        let controls = crate::connector::FixtureControlResolver::new(registry.clone());
        let prepared = prepare_fragments(
            &plan,
            &registry,
            &controls,
            &crate::connector::test_request_context(),
            None,
            ScanPreparationOptions::default(),
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
        let mut plan = test_support::result_plan();
        crate::sql::planner::distributed::test_support::remove_fragment_output_for_test(
            &mut plan, 7,
        );
        let registry = crate::connector::ConnectorRegistry::new();
        let controls = crate::connector::FixtureControlResolver::new(registry.clone());
        let error = match prepare_fragments(
            &plan,
            &registry,
            &controls,
            &crate::connector::test_request_context(),
            None,
            ScanPreparationOptions::default(),
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

    #[test]
    fn prepared_fragment_set_retains_sealed_runtime_filter_graph() {
        let plan = crate::sql::planner::distributed::test_support::rebuild_test_plan(
            test_support::result_plan(),
            draft_runtime_filter_graph(),
            |builder| {
                builder.fragments_mut()[0].root.runtime_filter_binding_ids =
                    vec![BindingId::new(1), BindingId::new(2)];
            },
        );
        let registry = crate::connector::ConnectorRegistry::new();
        let controls = crate::connector::FixtureControlResolver::new(registry.clone());
        let prepared = prepare_fragments(
            &plan,
            &registry,
            &controls,
            &crate::connector::test_request_context(),
            None,
            ScanPreparationOptions::default(),
        )
        .expect("prepare sealed graph");

        assert_eq!(prepared.runtime_filter_graph().channel_count(), 1);
        assert_eq!(prepared.runtime_filter_graph().binding_count(), 2);
        let RuntimeFilterBindingRoleData::Consumer(consumer) = &prepared
            .runtime_filter_graph()
            .binding(BindingId::new(2))
            .expect("sealed consumer binding")
            .role
        else {
            panic!("binding 2 must remain a consumer");
        };
        assert_eq!(
            consumer.activation,
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            }
        );
        assert_eq!(
            prepared
                .runtime_filter_graph()
                .channel(ChannelId::new(1))
                .unwrap()
                .policy,
            plan.runtime_filter_graph()
                .channel(ChannelId::new(1))
                .unwrap()
                .policy
        );
    }
}
