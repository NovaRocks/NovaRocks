// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Explicit empty component program; no SQL/BE execution capability is claimed.
//! Its original frozen Spread leaf is scheduled by the production Native owner.

pub(super) fn description(
    carrier: &crate::query_execution::package_freeze::CompiledPackageCarrier,
) -> (
    novarocks_query_application::preparation::FrozenExecutionDescription,
    novarocks_query_application::api::PlanSeal,
    crate::query_execution::physical_encoding::EncodedCompletedPlan,
) {
    use novarocks_physical_plan as p;
    use novarocks_query_application::preparation::{
        CompletedPhysicalPlanCandidate, CompletedPlanWithAccess, ExecutionResourceRequirements,
        FrozenCostEstimate, FrozenEstimateUnknownReason, FrozenExecutionDescription,
        OutputContract, ReadAccessSink,
    };
    let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
    let version = p::PlanVersionId::try_new([0xa4; 16]).unwrap();
    let mut plan = p::PlanBuilder::new(version, crate::application::test_constant_policy());
    let edge = plan.reserve_edge_id().unwrap();
    let leaf_id = p::FragmentId::new(1);
    let root_id = p::FragmentId::new(2);
    let dop = p::PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    };
    let mut leaf = p::FragmentBuilder::new(leaf_id);
    let values = leaf.reserve_node_id().unwrap();
    // An empty Read still declares its actual output value. No expression
    // or row is invented to supply that declaration.
    let value_type = p::ValueType::new(arrow::datatypes::DataType::Int64, false);
    let sent_value = leaf
        .add_value(
            value_type.clone(),
            p::ValueOrigin::NodeOutput {
                node: values,
                output_ordinal: 0,
            },
        )
        .unwrap();
    leaf.add_values(values, Box::default(), Box::from([sent_value]))
        .unwrap();
    let leaf = leaf
        .finish_definition(
            values,
            p::FragmentSink::Stream { edge },
            dop,
            p::StagePlacementPolicy::Spread {
                max_instances: None,
            },
        )
        .unwrap();
    let mut root = p::FragmentBuilder::new(root_id);
    let receiving = root.reserve_node_id().unwrap();
    let received_value = root
        .add_value(
            value_type.clone(),
            p::ValueOrigin::ExchangeImport {
                edge,
                source_value: sent_value,
            },
        )
        .unwrap();
    root.add_exchange_source(
        receiving,
        edge,
        Box::from([(sent_value, received_value)]),
        Box::from([received_value]),
        p::Distribution::Singleton,
        p::RowMultiplicity::SingleCopy,
    )
    .unwrap();
    let root = root
        .finish_definition(
            receiving,
            p::FragmentSink::RootResult(Box::new(
                novarocks_result_contract::RootOutputContract::new(
                    novarocks_result_contract::RootProfileId::V1,
                    novarocks_result_contract::FrozenRootOutput::CountOnly,
                ),
            )),
            dop,
            p::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    plan.add_fragment(leaf).unwrap();
    plan.add_fragment(root).unwrap();
    plan.add_edge(p::Edge {
        id: edge,
        role: p::OutputRole::Relational,
        delivery: p::DeliveryRequirement::DemandDriven,
        kind: p::EdgeKind::Stream,
        source: p::EdgeSource {
            fragment: leaf_id,
            port: p::FragmentOutputPort::Root,
            projection: Box::from([sent_value]),
        },
        destination: p::EdgeDestination {
            fragment: root_id,
            node: receiving,
            receive_mapping: Box::from([(sent_value, received_value)]),
        },
        partitioning: p::EdgePartitioning {
            source: p::Distribution::Singleton,
            source_multiplicity: p::RowMultiplicity::SingleCopy,
            destination: p::Distribution::Singleton,
            destination_multiplicity: p::RowMultiplicity::SingleCopy,
            routing_only: Box::new([]),
        },
    })
    .unwrap();
    plan.set_result_port(p::ResultPort {
        scalar_schema: None,
        fragment: root_id,
        output: p::OutputPort {
            node: receiving,
            columns: Box::from([received_value]),
        },
        fields: Box::from([p::ResultField {
            domain: p::ResultValueDomain::Plain,
            name: "empty_value".into(),
            alias: None,
            value: received_value,
            ty: value_type,
        }]),
    })
    .unwrap();
    let candidate =
        CompletedPhysicalPlanCandidate::for_program(plan.finish().unwrap(), &control).unwrap();
    let kind = novarocks_query_application::api::QueryExecutionKind::Read;
    let output = OutputContract::from_completed_candidate(kind, &candidate).unwrap();
    let description = FrozenExecutionDescription::for_completed_plan(
        kind,
        candidate.clone(),
        Vec::new(),
        output,
        novarocks_coordination_contract::ExecutionEffect::None,
        novarocks_coordination_contract::RecoveryMode::RestartAttemptBeforeVisibility,
        Vec::new(),
        FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
        ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
    )
    .unwrap()
    .into_internal_computation()
    .unwrap();
    // An empty original sidecar is exact: this program declares no scan.
    let access =
        ReadAccessSink::<crate::query_execution::provider_read_facts::FrozenProviderRead>::new()
            .try_into_access()
            .unwrap_or_else(|(error, _)| panic!("empty component access: {error}"));
    let paired = CompletedPlanWithAccess::try_pair(candidate, access)
        .unwrap_or_else(|(error, _)| panic!("pair the original empty component: {error}"));
    let encoded =
        crate::query_execution::physical_encoding::encode_empty_values_exchange_program_for_test(
            paired, carrier, &control,
        )
        .unwrap();
    let seal = novarocks_query_application::api::PlanSeal::Version(version);
    assert!(description.matches_plan_seal(seal));
    (description, seal, encoded)
}

#[tokio::test]
async fn actual_successor_participations_grow_while_the_original_predecessor_disposal_is_held() {
    super::run_with_successor_participations(2).await;
}
