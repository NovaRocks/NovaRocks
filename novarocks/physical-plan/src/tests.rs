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

#[cfg(test)]
use crate::tests::unit_fixture::DefinitionFixture;
use arrow_schema::DataType;
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding, ConnectorReadRelationKind,
    ConnectorReadRelationPayload,
};

use super::*;

mod aggregate_sequence_contract;
pub(crate) mod constant_fixture;
mod contract_regressions;
mod exchange_occurrence_contract;
mod guarantee_admission;
mod ordering_window_assertion_contract;
pub(crate) mod original_call_requests;
mod package_contract;
mod partition_scan_contract;
mod runtime_filter_binding_contract;
mod runtime_filter_wait_contract;
mod set_operation_contract;
mod sink_contract;
pub(crate) mod unit_fixture;
mod unpivot_contract;

fn version() -> PlanVersionId {
    PlanVersionId::try_new([7; 16]).unwrap()
}

fn write_route_id(byte: u8) -> ConnectorWriteRouteId {
    ConnectorWriteRouteId::from_bytes([byte; 32])
}

fn write_target_ordinal(value: u32) -> WriteTargetOrdinal {
    WriteTargetOrdinal::try_new(value).expect("test write target ordinal is bounded")
}

fn ty(data_type: DataType, nullable: bool) -> ValueType {
    ValueType::new(data_type, nullable)
}

fn unconstrained() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn singleton() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn hash_properties(value: ValueId) -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Hash {
            keys: Box::from([value]),
            scheme: hash_scheme(41),
        },
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn hash_scheme(seed: u8) -> HashPartitionScheme {
    HashPartitionScheme {
        space: PartitionSpaceId::try_new([seed; 32]).unwrap(),
        count: PartitionCountParameter {
            id: PartitionCountParameterId::try_new([seed.wrapping_add(1); 32]).unwrap(),
            admissible: PartitionCountDomain {
                min: 1,
                max: 64,
                requires_power_of_two: true,
            },
        },
        definition: HashDefinition::native_exchange(),
    }
}

fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 8,
        requires_power_of_two: true,
    }
}

/// The binding slice a fragment carries when it is its plan's only fragment:
/// its local endpoints numbered from 1 in attachment order, producers before
/// consumers.
fn single_fragment_runtime_filter_bindings(
    fragment: FragmentId,
    filters: &[RuntimeFilter],
) -> Box<[RuntimeFilterBindingCut]> {
    let mut bindings = Vec::new();
    for filter in filters {
        let producers = filter
            .producers
            .iter()
            .enumerate()
            .filter(|(_, producer)| producer.endpoint.fragment == fragment)
            .map(|(index, _)| RuntimeFilterBindingRole::Producer(index));
        let consumers = filter
            .consumers
            .iter()
            .enumerate()
            .filter(|(_, consumer)| consumer.endpoint.fragment == fragment)
            .map(|(index, _)| RuntimeFilterBindingRole::Consumer(index));
        for role in producers.chain(consumers) {
            bindings.push(RuntimeFilterBindingCut {
                binding_id: u32::try_from(bindings.len() + 1).unwrap(),
                filter: filter.id,
                role,
            });
        }
    }
    bindings.into_boxed_slice()
}

fn literal_fragment(
    fragment_id: FragmentId,
    sink: FragmentSink,
    nullable: bool,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> (Fragment, ValueId) {
    let mut builder = FragmentBuilder::new(fragment_id);
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, nullable);
    let expression = builder
        .define(
            node,
            (value_type.clone()).clone(),
            fixture_constants.i64(11, &(value_type.clone())),
        )
        .unwrap();
    let expression = builder.root_definition(
        node,
        expression,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    let value = builder
        .add_value(
            value_type,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([expression])]),
            },
        })
        .unwrap();
    crate::tests::unit_fixture::seal_pure_unit(&mut builder, node);
    (
        builder
            .finish_fixture_definition(node, sink, dop(), crate::StagePlacementPolicy::Singleton)
            .unwrap(),
        value,
    )
}

fn connector_binding() -> ConnectorReadBinding {
    connector_binding_for("lakehouse")
}

fn connector_binding_for(instance: &str) -> ConnectorReadBinding {
    let provider_id = ConnectorProviderId::parse("iceberg").unwrap();
    let instance_id = ConnectorInstanceId::parse(instance).unwrap();
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id,
            instance_id: instance_id.clone(),
        },
        CatalogHandle::new(instance_id, CatalogVersion::from_bytes([3; 32])),
    )
}

fn encoded(
    binding: &ConnectorReadBinding,
    category: ConnectorCodecCategory,
    byte: u8,
) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            category,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![byte].into(),
    )
}

fn metadata_relation(binding: &ConnectorReadBinding, column: ProviderColumnReference) -> Relation {
    Relation::Metadata(MetadataRelation {
        kind: MetadataRelationKind::try_new("iceberg.manifest.entries").unwrap(),
        read: ProviderReadReference {
            binding: binding.clone(),
            input_version: ExactInputVersion::try_new(vec![9]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::SystemTable,
                encoded(binding, ConnectorCodecCategory::ReadTable, 1),
                encoded(binding, ConnectorCodecCategory::ReadView, 2),
            ),
        },

        selection_digest: [8; 32],
        schema: Box::from([RelationField {
            column,
            ty: ty(DataType::Int64, false),
        }]),
        predicate_guarantees: Box::default(),
        provided_properties: unconstrained(),
        coverage_evidence: Box::from([4]),
    })
}

fn finish_scan_relation(relation: Relation) -> Result<Fragment, ValidationErrors> {
    finish_scan_relation_at(
        relation,
        FragmentId::new(91),
        ProviderReadOccurrenceId::new(0),
    )
}

fn finish_scan_relation_at(
    relation: Relation,
    fragment_id: FragmentId,
    occurrence: ProviderReadOccurrenceId,
) -> Result<Fragment, ValidationErrors> {
    let column = relation.schema()[0].column.clone();
    let output_properties = relation.provided_properties().clone();
    let mut builder = FragmentBuilder::new(fragment_id);
    let scan = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            relation.schema()[0].ty.clone(),
            ValueOrigin::ProviderField {
                scan_node: scan,
                field: column.clone(),
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: scan,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties,
            output: OutputPort {
                node: scan,
                columns: Box::from([value]),
            },
            kind: NodeKind::Scan {
                occurrence,
                relation: Box::new(relation),
                provider_outputs: Box::from([(column, value)]),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    builder.finish_fixture_definition(
        scan,
        FragmentSink::Noop,
        dop(),
        crate::StagePlacementPolicy::Singleton,
    )
}

#[test]
fn scan_occurrence_is_unique_without_splitting_provider_relation_identity() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 33),
    };
    let relation = metadata_relation(&binding, column);
    let first = finish_scan_relation_at(
        relation.clone(),
        FragmentId::new(201),
        ProviderReadOccurrenceId::new(11),
    )
    .unwrap();
    let second = finish_scan_relation_at(
        relation.clone(),
        FragmentId::new(202),
        ProviderReadOccurrenceId::new(12),
    )
    .unwrap();
    assert_eq!(
        match &first.nodes()[&first.root()].kind {
            NodeKind::Scan { relation, .. } => relation.read(),
            _ => unreachable!(),
        },
        match &second.nodes()[&second.root()].kind {
            NodeKind::Scan { relation, .. } => relation.read(),
            _ => unreachable!(),
        }
    );
    let mut valid = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    valid.add_fragment(first).unwrap();
    valid.add_fragment(second).unwrap();
    valid.finish().expect("distinct scan occurrences are valid");

    let duplicate_first = finish_scan_relation_at(
        relation.clone(),
        FragmentId::new(203),
        ProviderReadOccurrenceId::new(13),
    )
    .unwrap();
    let duplicate_second = finish_scan_relation_at(
        relation,
        FragmentId::new(204),
        ProviderReadOccurrenceId::new(13),
    )
    .unwrap();
    let mut duplicate = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    duplicate.add_fragment(duplicate_first).unwrap();
    duplicate.add_fragment(duplicate_second).unwrap();
    let error = duplicate.finish().unwrap_err().to_string();
    assert!(error.contains("provider read occurrence 13 is already owned"));
}

fn finish_scan_predicate_contract(
    guarantees: &[PredicateGuaranteeKind],
    residual_count: usize,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Result<Fragment, ValidationErrors> {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 33),
    };
    let mut relation = metadata_relation(&binding, column.clone());
    let mut builder = FragmentBuilder::new(FragmentId::new(92));
    let scan = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ProviderField {
                scan_node: scan,
                field: column.clone(),
            },
        )
        .unwrap();
    let predicate = builder
        .define(
            scan,
            (ty(DataType::Boolean, false)).clone(),
            fixture_constants.boolean(true, &(ty(DataType::Boolean, false))),
        )
        .unwrap();
    let predicate = builder.root_definition(
        scan,
        predicate,
        novarocks_type_contract::EvaluationDemand::TruthOnly,
        crate::RootScope::Port(0),
    );
    let predicate_guarantees = guarantees
        .iter()
        .map(|kind| PredicateGuarantee {
            predicate,
            kind: *kind,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    match &mut relation {
        Relation::Data(relation) => relation.predicate_guarantees = predicate_guarantees,
        Relation::Metadata(relation) => relation.predicate_guarantees = predicate_guarantees,
    }
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: scan,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node: scan,
                columns: Box::from([value]),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                provider_outputs: Box::from([(column, value)]),
                residuals: std::iter::repeat_n(predicate, residual_count)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    crate::tests::unit_fixture::seal_pure_unit(&mut builder, scan);
    builder.finish_fixture_definition(
        scan,
        FragmentSink::Noop,
        dop(),
        crate::StagePlacementPolicy::Singleton,
    )
}

#[test]
fn pruning_only_relation_predicate_requires_one_scan_residual() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let missing = finish_scan_predicate_contract(
        &[PredicateGuaranteeKind::PruningOnly],
        0,
        &mut fixture_constants,
    )
    .unwrap_err()
    .to_string();
    assert!(missing.contains("pruning-only relation predicate must be evaluated"));

    finish_scan_predicate_contract(
        &[PredicateGuaranteeKind::PruningOnly],
        1,
        &mut fixture_constants,
    )
    .unwrap();
    finish_scan_predicate_contract(&[PredicateGuaranteeKind::Exact], 0, &mut fixture_constants)
        .unwrap();
    finish_scan_predicate_contract(&[PredicateGuaranteeKind::Exact], 1, &mut fixture_constants)
        .unwrap();
}

#[test]
fn scan_predicate_contract_rejects_duplicate_guarantees_and_residuals() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let duplicate_guarantee = finish_scan_predicate_contract(
        &[
            PredicateGuaranteeKind::Exact,
            PredicateGuaranteeKind::PruningOnly,
        ],
        1,
        &mut fixture_constants,
    )
    .unwrap_err()
    .to_string();
    assert!(duplicate_guarantee.contains("conflicting guarantees"));

    let duplicate_residual = finish_scan_predicate_contract(
        &[PredicateGuaranteeKind::PruningOnly],
        2,
        &mut fixture_constants,
    )
    .unwrap_err()
    .to_string();
    assert!(duplicate_residual.contains("duplicate residual predicate"));
}

#[test]
fn provider_relation_requires_distinct_table_and_view_payloads_from_the_exact_binding() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 3),
    };

    let mut swapped = metadata_relation(&binding, column.clone());
    let Relation::Metadata(metadata) = &mut swapped else {
        unreachable!();
    };
    metadata.read.relation = ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::SystemTable,
        encoded(&binding, ConnectorCodecCategory::ReadView, 1),
        encoded(&binding, ConnectorCodecCategory::ReadTable, 2),
    );
    assert!(
        finish_scan_relation(swapped)
            .unwrap_err()
            .to_string()
            .contains("provider-private payload has the wrong category")
    );

    let mut mismatched = metadata_relation(&binding, column);
    let Relation::Metadata(metadata) = &mut mismatched else {
        unreachable!();
    };
    let other_binding = connector_binding_for("other-lakehouse");
    metadata.read.relation = ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::SystemTable,
        encoded(&binding, ConnectorCodecCategory::ReadTable, 1),
        encoded(&other_binding, ConnectorCodecCategory::ReadView, 2),
    );
    assert!(
        finish_scan_relation(mismatched)
            .unwrap_err()
            .to_string()
            .contains("provider-private payload header differs from the exact relation binding")
    );
}

#[test]
fn complete_plan_preserves_repeated_result_occurrences_and_exact_cuts() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let edge = EdgeId::new(5);
    let (source, source_value) = literal_fragment(
        FragmentId::new(1),
        FragmentSink::Stream { edge },
        false,
        &mut fixture_constants,
    );

    let mut destination_builder = FragmentBuilder::new(FragmentId::new(2));
    let destination_node = destination_builder.reserve_node_id().unwrap();
    let destination_value = destination_builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    destination_builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: destination_node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node: destination_node,
                columns: Box::from([destination_value]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, destination_value)]),
            },
        })
        .unwrap();
    let result_node = destination_builder.reserve_node_id().unwrap();
    let result_expression = destination_builder
        .define(
            result_node,
            ty(DataType::Int64, false),
            ExprKind::Value(destination_value),
        )
        .unwrap();
    let result_root_0 = destination_builder.root_definition(
        result_node,
        result_expression,
        novarocks_type_contract::EvaluationDemand::Value,
        RootScope::Port(0),
    );
    let result_root_1 = destination_builder.root_definition(
        result_node,
        result_expression,
        novarocks_type_contract::EvaluationDemand::Value,
        RootScope::Port(0),
    );
    destination_builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: result_node,
            inputs: Box::from([destination_node]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: unconstrained(),
            output: OutputPort {
                node: result_node,
                columns: Box::from([destination_value, destination_value]),
            },
            kind: NodeKind::Project {
                expressions: Box::from([
                    (result_root_0, destination_value),
                    (result_root_1, destination_value),
                ]),
            },
        })
        .unwrap();
    let destination = destination_builder
        .finish_fixture_definition(
            result_node,
            FragmentSink::Result,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    plan.add_edge(Edge {
        role: crate::OutputRole::Relational,
        delivery: crate::DeliveryRequirement::DemandDriven,
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: crate::FragmentOutputPort::Root,
            fragment: FragmentId::new(1),
            projection: Box::from([source_value]),
        },
        destination: EdgeDestination {
            fragment: FragmentId::new(2),
            node: destination_node,
            receive_mapping: Box::from([(source_value, destination_value)]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Unconstrained,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Unconstrained,
            destination_multiplicity: RowMultiplicity::SingleCopy,
            routing_only: Box::default(),
        },
    })
    .unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: FragmentId::new(2),
        output: OutputPort {
            node: result_node,
            columns: Box::from([destination_value, destination_value]),
        },
        fields: Box::from([
            ResultField {
                domain: ResultValueDomain::Plain,
                name: "x".into(),
                alias: None,
                value: destination_value,
                ty: ty(DataType::Int64, false),
            },
            ResultField {
                domain: ResultValueDomain::Plain,
                name: "y".into(),
                alias: None,
                value: destination_value,
                ty: ty(DataType::Int64, false),
            },
        ]),
    })
    .unwrap();

    let plan = crate::tests::constant_fixture::publish_plan(plan, &fixture_constants).unwrap();
    assert_eq!(
        plan.result_port().unwrap().output.columns.as_ref(),
        &[destination_value, destination_value]
    );

    // The same plan is accepted or refused purely by the bounds it is given,
    // and a refusal is reported as admission information rather than as a
    // malformed plan. That difference is the whole reason the bounds are a
    // value: an operator can act on the first and cannot act on the second.
    validate_plan_with_limits(&plan, PlanLimits::FROZEN).expect("valid under the frozen bounds");
    let errors = validate_plan_with_limits(
        &plan,
        PlanLimits {
            plan_fragments: 1,
            ..PlanLimits::FROZEN
        },
    )
    .expect_err("a two-fragment plan cannot fit a one-fragment bound");
    assert!(errors.has(ValidationErrorCategory::ResourceLimit));
    assert!(!errors.is_producer_defect());
    let cuts = fragment_cuts(&plan, FragmentId::new(2)).unwrap();
    assert_eq!(cuts.inbound.len(), 1);
    let all_cuts = derive_fragment_cuts(&plan).unwrap();
    assert_eq!(all_cuts.get(&FragmentId::new(2)), Some(&cuts));
    validate_fragment(plan.fragments().get(&FragmentId::new(2)).unwrap(), &cuts).unwrap();
}

#[test]
fn edge_rejects_a_receiver_that_narrows_nullability_before_plan_publication() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let edge = EdgeId::new(5);
    let (source, source_value) = literal_fragment(
        FragmentId::new(1),
        FragmentSink::Stream { edge },
        true,
        &mut fixture_constants,
    );
    let mut destination_builder = FragmentBuilder::new(FragmentId::new(2));
    let destination_node = destination_builder.reserve_node_id().unwrap();
    let destination_value = destination_builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    destination_builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: destination_node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node: destination_node,
                columns: Box::from([destination_value]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, destination_value)]),
            },
        })
        .unwrap();
    let destination = destination_builder
        .finish_fixture_definition(
            destination_node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    plan.add_edge(Edge {
        role: crate::OutputRole::Relational,
        delivery: crate::DeliveryRequirement::DemandDriven,
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: crate::FragmentOutputPort::Root,
            fragment: FragmentId::new(1),
            projection: Box::from([source_value]),
        },
        destination: EdgeDestination {
            fragment: FragmentId::new(2),
            node: destination_node,
            receive_mapping: Box::from([(source_value, destination_value)]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Unconstrained,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Unconstrained,
            destination_multiplicity: RowMultiplicity::SingleCopy,
            routing_only: Box::default(),
        },
    })
    .unwrap();
    let error = crate::tests::constant_fixture::publish_plan(plan, &fixture_constants)
        .unwrap_err()
        .to_string();
    assert!(error.contains("source and destination types differ"));
}

#[test]
fn edge_accepts_a_receiver_that_admits_null_the_sender_never_writes() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let edge = EdgeId::new(5);
    let (source, source_value) = literal_fragment(
        FragmentId::new(1),
        FragmentSink::Stream { edge },
        false,
        &mut fixture_constants,
    );
    let mut destination_builder = FragmentBuilder::new(FragmentId::new(2));
    let destination_node = destination_builder.reserve_node_id().unwrap();
    let destination_value = destination_builder
        .add_value(
            ty(DataType::Int64, true),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    destination_builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: destination_node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node: destination_node,
                columns: Box::from([destination_value]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, destination_value)]),
            },
        })
        .unwrap();
    let destination = destination_builder
        .finish_fixture_definition(
            destination_node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    plan.add_edge(Edge {
        role: crate::OutputRole::Relational,
        delivery: crate::DeliveryRequirement::DemandDriven,
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: crate::FragmentOutputPort::Root,
            fragment: FragmentId::new(1),
            projection: Box::from([source_value]),
        },
        destination: EdgeDestination {
            fragment: FragmentId::new(2),
            node: destination_node,
            receive_mapping: Box::from([(source_value, destination_value)]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Unconstrained,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Unconstrained,
            destination_multiplicity: RowMultiplicity::SingleCopy,
            routing_only: Box::default(),
        },
    })
    .unwrap();
    crate::tests::constant_fixture::publish_plan(plan, &fixture_constants).unwrap();
}

fn broadcast_edge_plan(
    destination_multiplicity: RowMultiplicity,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Result<PhysicalPlan, ValidationErrors> {
    let edge = EdgeId::new(6);
    let (source, source_value) = literal_fragment(
        FragmentId::new(3),
        FragmentSink::Stream { edge },
        false,
        fixture_constants,
    );
    let mut destination_builder = FragmentBuilder::new(FragmentId::new(4));
    let receiver = destination_builder.reserve_node_id().unwrap();
    let imported = destination_builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    destination_builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: receiver,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Broadcast,
                row_multiplicity: RowMultiplicity::Replicated,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: receiver,
                columns: Box::from([imported]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, imported)]),
            },
        })
        .unwrap();
    let destination = destination_builder
        .finish_fixture_definition(
            receiver,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    plan.add_edge(Edge {
        role: crate::OutputRole::Relational,
        delivery: crate::DeliveryRequirement::DemandDriven,
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: crate::FragmentOutputPort::Root,
            fragment: FragmentId::new(3),
            projection: Box::from([source_value]),
        },
        destination: EdgeDestination {
            fragment: FragmentId::new(4),
            node: receiver,
            receive_mapping: Box::from([(source_value, imported)]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Broadcast,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Broadcast,
            destination_multiplicity,
            routing_only: Box::default(),
        },
    })
    .unwrap();
    crate::tests::constant_fixture::publish_plan(plan, fixture_constants)
}

#[test]
fn broadcast_edge_is_the_explicit_single_copy_to_replicated_transition() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    broadcast_edge_plan(RowMultiplicity::Replicated, &mut fixture_constants).unwrap();

    let error = broadcast_edge_plan(RowMultiplicity::SingleCopy, &mut fixture_constants)
        .unwrap_err()
        .to_string();
    assert!(error.contains("edge source and destination partitioning or row multiplicity"));
}

#[test]
fn broadcast_properties_cannot_claim_single_copy_rows() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut builder = FragmentBuilder::new(FragmentId::new(5));
    let node = builder.reserve_node_id().unwrap();
    let expression = builder
        .define(
            node,
            (ty(DataType::Int64, false)).clone(),
            fixture_constants.i64(1, &(ty(DataType::Int64, false))),
        )
        .unwrap();
    let value = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let value_field_root_0 = builder.root_definition(
        node,
        expression,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Broadcast,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([value_field_root_0])]),
            },
        })
        .unwrap();

    let error = builder
        .finish_fixture_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("broadcast physical properties require replicated row multiplicity"));
}

#[test]
fn non_empty_values_require_an_exact_singleton_or_replicated_broadcast_placement() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut finish = |fragment_id: u32, output_properties: PhysicalProperties| {
        let mut builder = FragmentBuilder::new(FragmentId::new(fragment_id));
        let node = builder.reserve_node_id().unwrap();
        let value_type = ty(DataType::Int64, false);
        let expression = builder
            .define(
                node,
                (value_type.clone()).clone(),
                fixture_constants.i64(1, &(value_type.clone())),
            )
            .unwrap();
        let value = builder
            .add_value(
                value_type,
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        let value_field_root_0 = builder.root_definition(
            node,
            expression,
            novarocks_type_contract::EvaluationDemand::Value,
            crate::RootScope::AllPorts,
        );
        builder
            .insert_node_unchecked(PhysicalNode {
                expressions: None,
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties,
                output: OutputPort {
                    node,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([value_field_root_0])]),
                },
            })
            .unwrap();
        builder.finish_fixture_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
    };

    let error = finish(65, unconstrained()).unwrap_err().to_string();
    assert!(
        error.contains("VALUES distribution and row multiplicity lack an exact placement proof")
    );

    finish(66, singleton()).unwrap();
    finish(
        67,
        PhysicalProperties {
            distribution: Distribution::Broadcast,
            row_multiplicity: RowMultiplicity::Replicated,
            ordering: Box::default(),
        },
    )
    .unwrap();
}

#[test]
fn metadata_relation_retains_its_closed_source_contract() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 2),
    };
    let mut relation = metadata_relation(&binding, column.clone());
    let Relation::Metadata(metadata) = &mut relation else {
        unreachable!();
    };
    metadata.provided_properties = PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let build = |relation: Relation| {
        let mut builder = FragmentBuilder::new(FragmentId::new(9));
        let node = builder.reserve_node_id().unwrap();
        let value = builder
            .add_value(
                ty(DataType::Int64, false),
                ValueOrigin::ProviderField {
                    scan_node: node,
                    field: column.clone(),
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                expressions: None,
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
                output: OutputPort {
                    node,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Scan {
                    occurrence: ProviderReadOccurrenceId::new(0),
                    relation: Box::new(relation),
                    provider_outputs: Box::from([(column.clone(), value)]),
                    residuals: Box::default(),
                    derived_values: Box::default(),
                },
            })
            .unwrap();
        builder.finish_fixture_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
    };
    let fragment = build(relation.clone()).unwrap();
    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(fragment).unwrap();
    let plan = plan.finish().unwrap();
    assert!(matches!(
        plan.fragments().get(&FragmentId::new(9)).unwrap().sink(),
        FragmentSink::Noop
    ));
    let Relation::Metadata(metadata) = &relation else {
        unreachable!()
    };
    assert_eq!(metadata.selection_digest, [8; 32]);
    assert_eq!(metadata.coverage_evidence.as_ref(), &[4]);
    assert_eq!(
        metadata.read.input_version,
        ExactInputVersion::try_new(vec![9]).unwrap()
    );
    let mut zero_selection = relation.clone();
    let Relation::Metadata(metadata) = &mut zero_selection else {
        unreachable!()
    };
    metadata.selection_digest = [0; 32];
    assert!(
        build(zero_selection)
            .unwrap_err()
            .errors()
            .iter()
            .any(|error| {
                error
                    .message()
                    .contains("relation selection digest is zero")
            })
    );
    for evidence in [
        Box::<[u8]>::default(),
        vec![4; MAX_METADATA_COVERAGE_EVIDENCE_BYTES + 1].into_boxed_slice(),
    ] {
        let mut invalid_coverage = relation.clone();
        let Relation::Metadata(metadata) = &mut invalid_coverage else {
            unreachable!()
        };
        metadata.coverage_evidence = evidence;
        assert!(
            build(invalid_coverage)
                .unwrap_err()
                .errors()
                .iter()
                .any(|error| {
                    error.message().contains(
                        "metadata relation coverage evidence must be bounded and non-empty",
                    )
                })
        );
    }
}

#[test]
fn duplicate_builder_insert_does_not_replace_the_existing_definition() {
    let mut builder = FragmentBuilder::new(FragmentId::new(1));
    let node = builder.reserve_node_id().unwrap();
    let id = ValueId::new(3);
    builder
        .insert_value(ValueDef {
            id,
            ty: ty(DataType::Int64, false),
            origin: ValueOrigin::WriterDerived {
                writer_node: node,
                kind: WriterDerivedKind::AffectedRows,
            },
        })
        .unwrap();
    assert_eq!(
        builder
            .insert_value(ValueDef {
                id,
                ty: ty(DataType::Utf8, true),
                origin: ValueOrigin::WriterDerived {
                    writer_node: node,
                    kind: WriterDerivedKind::CommitFragment,
                },
            })
            .unwrap_err(),
        BuildError::DuplicateValue(id)
    );
}

#[test]
fn cte_import_is_proven_by_the_exact_multicast_cut() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let edge = EdgeId::new(17);
    let producer_id = FragmentId::new(3);
    let consumer_id = FragmentId::new(4);
    let (producer, producer_value) = literal_fragment(
        producer_id,
        FragmentSink::Multicast {
            edges: Box::from([edge]),
        },
        false,
        &mut fixture_constants,
    );
    let mut consumer_builder = FragmentBuilder::new(consumer_id);
    let receiver = consumer_builder.reserve_node_id().unwrap();
    let imported = consumer_builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::CteImport {
                edge,
                producer_fragment: producer_id,
                producer_value,
            },
        )
        .unwrap();
    consumer_builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: receiver,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node: receiver,
                columns: Box::from([imported]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(producer_value, imported)]),
            },
        })
        .unwrap();
    let consumer = consumer_builder
        .finish_fixture_definition(
            receiver,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut builder = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    builder.add_fragment(producer).unwrap();
    builder.add_fragment(consumer).unwrap();
    builder
        .add_edge(Edge {
            role: crate::OutputRole::Relational,
            delivery: crate::DeliveryRequirement::DemandDriven,
            id: edge,
            kind: EdgeKind::CteMulticast,
            source: EdgeSource {
                port: crate::FragmentOutputPort::Root,
                fragment: producer_id,
                projection: Box::from([producer_value]),
            },
            destination: EdgeDestination {
                fragment: consumer_id,
                node: receiver,
                receive_mapping: Box::from([(producer_value, imported)]),
            },
            partitioning: EdgePartitioning {
                source: Distribution::Unconstrained,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Unconstrained,
                destination_multiplicity: RowMultiplicity::SingleCopy,
                routing_only: Box::default(),
            },
        })
        .unwrap();
    let plan = crate::tests::constant_fixture::publish_plan(builder, &fixture_constants).unwrap();
    let cuts = fragment_cuts(&plan, consumer_id).unwrap();
    validate_fragment(plan.fragments().get(&consumer_id).unwrap(), &cuts).unwrap();

    let wrong = FragmentCuts {
        inbound: Box::from([InboundFragmentCut {
            edge,
            kind: EdgeKind::Stream,
            source_fragment: producer_id,
            destination_node: receiver,
            imports: cuts.inbound[0].imports.clone(),
            partitioning: EdgePartitioning {
                source: Distribution::Unconstrained,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Unconstrained,
                destination_multiplicity: RowMultiplicity::SingleCopy,
                routing_only: Box::default(),
            },
            source_port: crate::FragmentOutputPort::Root,
            payload: crate::ExchangePayloadContract::Relational,
            delivery: crate::DeliveryRequirement::DemandDriven,
        }]),
        outbound: Box::default(),
        ..FragmentCuts::default()
    };
    assert!(
        validate_fragment(plan.fragments().get(&consumer_id).unwrap(), &wrong)
            .unwrap_err()
            .to_string()
            .contains("inbound cut type or destination origin is inconsistent")
    );
}

/// Builds a single-copy singleton `Values` node returning one boolean column,
/// and returns the builder plus that node.
fn boolean_source(
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> (FragmentBuilder, NodeId, RootOrdinal) {
    let mut builder = FragmentBuilder::new(FragmentId::new(12));
    let values = builder.reserve_node_id().unwrap();
    let boolean = ty(DataType::Boolean, false);
    let literal = builder
        .define(
            values,
            (boolean.clone()).clone(),
            fixture_constants.boolean(true, &(boolean.clone())),
        )
        .unwrap();
    let literal = builder.root_definition(
        values,
        literal,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    let value = builder
        .add_value(
            boolean,
            ValueOrigin::NodeOutput {
                node: values,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: values,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node: values,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([literal])]),
            },
        })
        .unwrap();
    crate::tests::unit_fixture::seal_pure_unit(&mut builder, values);
    (builder, values, literal)
}

#[test]
fn a_typed_constructor_refuses_before_a_node_exists() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    // These are the states the contract used to accept and then reject in a
    // whole-fragment pass. Refusing them at the construction point means the
    // caller finds out where the mistake was made, and the fragment never
    // holds an illegal node at all.
    let (mut builder, values, literal) = boolean_source(&mut fixture_constants);
    let filter = builder.reserve_node_id().unwrap();

    assert!(matches!(
        builder.add_filter(filter, values, Box::default()),
        Err(BuildError::FilterWithoutPredicate(_))
    ));
    assert!(matches!(
        builder.add_filter(filter, NodeId::new(404), Box::from([literal])),
        Err(BuildError::UndefinedInput { .. })
    ));
    // The ordinal is local to Values; this filter has no such unit root.
    assert!(matches!(
        builder.add_filter(filter, values, Box::from([literal])),
        Err(BuildError::UndefinedRoot { .. })
    ));

    // A fresh builder, because a rejected predicate would otherwise leave an
    // expression no operator reaches - itself a contract violation.
    {
        let (mut other, other_values, _) = boolean_source(&mut fixture_constants);
        let other_filter = other.reserve_node_id().unwrap();
        let non_boolean = other
            .define(
                other_filter,
                (ty(DataType::Int64, false)).clone(),
                fixture_constants.i64(1, &(ty(DataType::Int64, false))),
            )
            .unwrap();
        let non_boolean = other.root_definition(
            other_filter,
            non_boolean,
            novarocks_type_contract::EvaluationDemand::Value,
            RootScope::Port(0),
        );
        assert!(matches!(
            other.add_filter(other_filter, other_values, Box::from([non_boolean])),
            Err(BuildError::PredicateIsNotBoolean(_))
        ));
    }

    // Nothing above left a node behind.
    assert!(builder.node_output_properties(filter).is_none());

    let predicate = builder
        .define(
            filter,
            (ty(DataType::Boolean, false)).clone(),
            fixture_constants.boolean(true, &(ty(DataType::Boolean, false))),
        )
        .unwrap();
    let predicate = builder.root_definition(
        filter,
        predicate,
        novarocks_type_contract::EvaluationDemand::TruthOnly,
        RootScope::Port(0),
    );
    builder
        .add_filter(filter, values, Box::from([predicate]))
        .expect("a boolean predicate over an existing input is a filter");
    // The caller never stated the output port or the properties, so it cannot
    // have stated them wrongly.
    let properties = builder.node_output_properties(filter).unwrap().clone();
    assert_eq!(
        properties,
        derive_filter_output_properties(&singleton(), true)
    );
    builder
        .finish_fixture_definition(
            filter,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .expect("a derived filter validates");
}

#[test]
fn a_violated_contract_invariant_is_reported_as_a_producer_defect() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut builder = FragmentBuilder::new(FragmentId::new(8));
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let expression = builder
        .define(
            node,
            (value_type.clone()).clone(),
            fixture_constants.i64(1, &(value_type.clone())),
        )
        .unwrap();
    let value = builder
        .add_value(
            value_type,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let value_field_root_0 = builder.root_definition(
        node,
        expression,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([value_field_root_0])]),
            },
        })
        .unwrap();

    // A root that names an undefined node cannot be produced by a correct
    // planner, cannot succeed on another backend, and cannot be cleared by
    // raising a limit.
    let errors = builder
        .finish_fixture_definition(
            NodeId::new(999),
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap_err();
    assert!(errors.is_producer_defect());
    assert!(!errors.has(ValidationErrorCategory::ResourceLimit));
    assert!(!errors.has(ValidationErrorCategory::UnsupportedCapability));
}

#[test]
fn output_properties_cannot_name_a_value_absent_from_the_output_port() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut builder = FragmentBuilder::new(FragmentId::new(10));
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let expression = builder
        .define(
            node,
            (value_type.clone()).clone(),
            fixture_constants.i64(1, &(value_type.clone())),
        )
        .unwrap();
    let visible = builder
        .add_value(
            value_type.clone(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let hidden = builder
        .add_value(
            value_type,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let value_field_root_0 = builder.root_definition(
        node,
        expression,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Hash {
                    keys: Box::from([hidden]),
                    scheme: hash_scheme(42),
                },
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: Box::from([visible]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([value_field_root_0])]),
            },
        })
        .unwrap();

    let error = builder
        .finish_fixture_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("physical property key 1 is absent from the port"));
}

#[test]
fn project_cannot_publish_an_uncomputed_output() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut builder = FragmentBuilder::new(FragmentId::new(11));
    let source = builder.reserve_node_id().unwrap();
    let source_expression = builder
        .define(
            source,
            (ty(DataType::Int64, false)).clone(),
            fixture_constants.i64(1, &(ty(DataType::Int64, false))),
        )
        .unwrap();
    let source_value = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let value_field_root_0 = builder.root_definition(
        source,
        source_expression,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node: source,
                columns: Box::from([source_value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([value_field_root_0])]),
            },
        })
        .unwrap();
    let project = builder.reserve_node_id().unwrap();
    let fabricated = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::Expr {
                node: project,
                root: RootOrdinal::new(0),
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: project,
            inputs: Box::from([source]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: unconstrained(),
            output: OutputPort {
                node: project,
                columns: Box::from([fabricated]),
            },
            kind: NodeKind::Project {
                expressions: Box::default(),
            },
        })
        .unwrap();

    let error = builder
        .finish_fixture_definition(
            project,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("exact produced/pass-through sequence"));
}

#[test]
fn exchange_partitioning_maps_source_and_destination_value_domains() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let edge = EdgeId::new(21);
    let (source, source_value) = literal_fragment(
        FragmentId::new(21),
        FragmentSink::Stream { edge },
        false,
        &mut fixture_constants,
    );
    let mut receiver = FragmentBuilder::new(FragmentId::new(22));
    let receiver_node = receiver.reserve_node_id().unwrap();
    let imported = receiver
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    receiver
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: receiver_node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: hash_properties(imported),
            output: OutputPort {
                node: receiver_node,
                columns: Box::from([imported]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, imported)]),
            },
        })
        .unwrap();
    let receiver = receiver
        .finish_fixture_definition(
            receiver_node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(receiver).unwrap();
    plan.add_edge(Edge {
        role: crate::OutputRole::Relational,
        delivery: crate::DeliveryRequirement::DemandDriven,
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: crate::FragmentOutputPort::Root,
            fragment: FragmentId::new(21),
            projection: Box::from([source_value]),
        },
        destination: EdgeDestination {
            fragment: FragmentId::new(22),
            node: receiver_node,
            receive_mapping: Box::from([(source_value, imported)]),
        },
        partitioning: EdgePartitioning {
            source: hash_properties(source_value).distribution,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: hash_properties(imported).distribution,
            destination_multiplicity: RowMultiplicity::SingleCopy,
            routing_only: Box::default(),
        },
    })
    .unwrap();
    let plan = crate::tests::constant_fixture::publish_plan(plan, &fixture_constants).unwrap();
    let mut cuts = fragment_cuts(&plan, FragmentId::new(22)).unwrap();
    cuts.inbound[0].partitioning.source = Distribution::RoundRobin;
    let error = validate_fragment(plan.fragments().get(&FragmentId::new(22)).unwrap(), &cuts)
        .unwrap_err()
        .to_string();
    assert!(error.contains("source and destination partitioning"));
}

#[test]
fn integer_division_keeps_its_resolved_signed_result() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut builder = FragmentBuilder::new(FragmentId::new(23));
    let node = builder.reserve_node_id().unwrap();
    let left = builder
        .define(
            node,
            (ty(DataType::Int64, false)).clone(),
            fixture_constants.i64(6, &(ty(DataType::Int64, false))),
        )
        .unwrap();
    let right = builder
        .define(
            node,
            (ty(DataType::Int64, false)).clone(),
            fixture_constants.i64(2, &(ty(DataType::Int64, false))),
        )
        .unwrap();
    let divide = builder
        .define(
            node,
            ty(DataType::Int64, false),
            ExprKind::Binary {
                allow_throw_exception: Some(novarocks_type_contract::SemanticParameterRef {
                    id: novarocks_type_contract::SemanticParameterId::new(0),
                    expected_key:
                        novarocks_type_contract::SemanticParameterKey::AllowThrowException,
                }),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                left,
                op: BinaryOperator::Divide,
                right,
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let value_field_root_0 = builder.root_definition(
        node,
        divide,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([value_field_root_0])]),
            },
        })
        .unwrap();
    builder
        .finish_fixture_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
}

#[test]
fn left_outer_join_cannot_publish_the_original_build_value() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut builder = FragmentBuilder::new(FragmentId::new(24));
    let left_node = builder.reserve_node_id().unwrap();
    let left_expr = builder
        .define(
            left_node,
            (ty(DataType::Int64, false)).clone(),
            fixture_constants.i64(1, &(ty(DataType::Int64, false))),
        )
        .unwrap();
    let left = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node: left_node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let value_field_root_0 = builder.root_definition(
        left_node,
        left_expr,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: left_node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node: left_node,
                columns: Box::from([left]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([value_field_root_0])]),
            },
        })
        .unwrap();
    let right_node = builder.reserve_node_id().unwrap();
    let right = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node: right_node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: right_node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node: right_node,
                columns: Box::from([right]),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        })
        .unwrap();
    let join = builder.reserve_node_id().unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: join,
            inputs: Box::from([left_node, right_node]),
            required_inputs: Box::from([unconstrained(), unconstrained()]),
            output_properties: unconstrained(),
            output: OutputPort {
                node: join,
                columns: Box::from([left, right]),
            },
            kind: NodeKind::NestLoopJoin {
                kind: JoinKind::LeftOuter,
                distribution: NestLoopJoinDistribution::BroadcastRight,
                predicate: None,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    let error = builder
        .finish_fixture_definition(
            join,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("join output value"));
}

#[test]
fn project_drops_ordering_when_its_key_is_not_projected() {
    let mut builder = FragmentBuilder::new(FragmentId::new(25));
    let source = builder.reserve_node_id().unwrap();
    let edge = EdgeId::new(250);
    let source_a = ValueId::new(900);
    let source_b = ValueId::new(901);
    let a = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport {
                edge,
                source_value: source_a,
            },
        )
        .unwrap();
    let b = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport {
                edge,
                source_value: source_b,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: source,
                columns: Box::from([a, b]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_a, a), (source_b, b)]),
            },
        })
        .unwrap();
    let sort = builder.reserve_node_id().unwrap();
    let sort_key = builder
        .define(sort, ty(DataType::Int64, false), ExprKind::Value(a))
        .unwrap();
    let sort_key = builder.root_definition(
        sort,
        sort_key,
        novarocks_type_contract::EvaluationDemand::Value,
        RootScope::Port(0),
    );
    let sorted = PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::from([OrderingKey {
            value: a,
            direction: SortDirection::Ascending,
            null_ordering: NullOrdering::Last,
        }]),
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: sort,
            inputs: Box::from([source]),
            required_inputs: Box::from([PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            }]),
            output_properties: sorted,
            output: OutputPort {
                node: sort,
                columns: Box::from([a, b]),
            },
            kind: NodeKind::Sort {
                order_by: Box::from([SortExpr {
                    expr: sort_key,
                    direction: SortDirection::Ascending,
                    null_ordering: NullOrdering::Last,
                }]),
                mode: SortMode::Global,
            },
        })
        .unwrap();
    let project = builder.reserve_node_id().unwrap();
    let project_b = builder
        .define(project, ty(DataType::Int64, false), ExprKind::Value(b))
        .unwrap();
    let project_b = builder.root_definition(
        project,
        project_b,
        novarocks_type_contract::EvaluationDemand::Value,
        RootScope::Port(0),
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: project,
            inputs: Box::from([sort]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: project,
                columns: Box::from([b]),
            },
            kind: NodeKind::Project {
                expressions: Box::from([(project_b, b)]),
            },
        })
        .unwrap();
    builder
        .finish_fixture_definition(
            project,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
}

#[test]
fn repeat_publishes_a_distinct_nullable_grouping_value() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut builder = FragmentBuilder::new(FragmentId::new(28));
    let source = builder.reserve_node_id().unwrap();
    let expression = builder
        .define(
            source,
            (ty(DataType::Int64, false)).clone(),
            fixture_constants.i64(1, &(ty(DataType::Int64, false))),
        )
        .unwrap();
    let input = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let value_field_root_0 = builder.root_definition(
        source,
        expression,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node: source,
                columns: Box::from([input]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([value_field_root_0])]),
            },
        })
        .unwrap();
    let repeat = builder.reserve_node_id().unwrap();
    let nullable = builder
        .add_value(
            ty(DataType::Int64, true),
            ValueOrigin::NullExtended {
                node: repeat,
                of: input,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: repeat,
            inputs: Box::from([source]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: singleton(),
            output: OutputPort {
                node: repeat,
                columns: Box::from([nullable]),
            },
            kind: NodeKind::Repeat {
                rollup_keys: Box::from([input]),
                grouping_sets: Box::from([Box::from([input]), Box::default()]),
                grouping_values: Box::from([(input, nullable)]),
                grouping_outputs: Box::default(),
            },
        })
        .unwrap();
    builder
        .finish_fixture_definition(
            repeat,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
}

#[test]
fn arrow_data_type_depth_is_bounded_iteratively() {
    use std::sync::Arc;

    use arrow_schema::Field;

    let mut data_type = DataType::Int64;
    for _ in 0..MAX_DATA_TYPE_DEPTH {
        data_type = DataType::List(Arc::new(Field::new_list_field(data_type, true)));
    }
    assert_eq!(
        ValueType::try_with_logical_type(
            data_type,
            true,
            novarocks_type_contract::ValueLogicalType::Physical
        )
        .unwrap_err(),
        novarocks_type_contract::ValueTypeError::TooDeep
    );
}

#[test]
fn arrow_data_type_node_count_is_bounded_before_stack_growth() {
    use std::sync::Arc;

    use arrow_schema::{Field, Fields};

    let fields = (0..MAX_DATA_TYPE_NODES)
        .map(|index| Arc::new(Field::new(index.to_string(), DataType::Int64, true)))
        .collect::<Vec<_>>();
    assert_eq!(
        ValueType::try_with_logical_type(
            DataType::Struct(Fields::from(fields)),
            true,
            novarocks_type_contract::ValueLogicalType::Physical
        )
        .unwrap_err(),
        novarocks_type_contract::ValueTypeError::TooManyNodes
    );
}

#[test]
fn declared_value_type_author_rejects_invalid_carrier_parameters() {
    for data_type in [
        DataType::Decimal128(0, 0),
        DataType::FixedSizeBinary(-1),
        DataType::Time32(arrow_schema::TimeUnit::Nanosecond),
        DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Int64)),
    ] {
        assert!(
            ValueType::try_with_logical_type(
                data_type,
                true,
                novarocks_type_contract::ValueLogicalType::Physical
            )
            .is_err()
        );
    }
}

#[test]
fn declared_value_names_are_bounded_and_provider_metadata_is_not_execution_semantics() {
    use arrow_schema::Field;
    use std::{collections::HashMap, sync::Arc};
    let mut metadata = HashMap::new();
    metadata.insert(
        "key".to_owned(),
        "v".repeat(MAX_DATA_TYPE_FIELD_METADATA_VALUE_BYTES + 1),
    );
    let field = Field::new(
        "n".repeat(MAX_DATA_TYPE_FIELD_NAME_BYTES + 1),
        DataType::Int64,
        true,
    )
    .with_metadata(metadata.clone());
    assert!(
        ValueType::try_with_logical_type(
            DataType::Struct(vec![Arc::new(field)].into()),
            true,
            novarocks_type_contract::ValueLogicalType::Physical
        )
        .is_err()
    );
    let list = ValueType::new(
        DataType::List(Arc::new(
            Field::new("role", DataType::Int64, true).with_metadata(metadata),
        )),
        true,
    );
    let DataType::List(item) = list.data_type() else {
        unreachable!()
    };
    assert_eq!(item.name(), "item");
    assert!(item.metadata().is_empty());
}

#[test]
fn annotation_count_and_value_bytes_are_bounded() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (fragment, _) = literal_fragment(
        FragmentId::new(31),
        FragmentSink::Noop,
        false,
        &mut fixture_constants,
    );
    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(fragment).unwrap();
    for _ in 0..=MAX_ANNOTATIONS {
        plan.add_annotation(PlanAnnotation {
            subject: AnnotationSubject::Plan,
            key: "diagnostic".into(),
            value: "ok".into(),
        });
    }
    let count_error = crate::tests::constant_fixture::publish_plan(plan, &fixture_constants)
        .unwrap_err()
        .to_string();
    assert!(count_error.contains("annotations: contains 4097 items, exceeding 4096"));

    let (fragment, _) = literal_fragment(
        FragmentId::new(32),
        FragmentSink::Noop,
        false,
        &mut fixture_constants,
    );
    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(fragment).unwrap();
    plan.add_annotation(PlanAnnotation {
        subject: AnnotationSubject::Plan,
        key: "diagnostic".into(),
        value: "v".repeat(MAX_ANNOTATION_VALUE_BYTES + 1).into(),
    });
    let value_error = crate::tests::constant_fixture::publish_plan(plan, &fixture_constants)
        .unwrap_err()
        .to_string();
    assert!(value_error.contains("annotation `diagnostic` value is 16385 bytes, exceeding 16384"));
}

#[test]
fn independent_fragment_cut_checks_complete_valid_value_binding() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (fragment, value) = literal_fragment(
        FragmentId::new(33),
        FragmentSink::Noop,
        false,
        &mut fixture_constants,
    );
    let cuts = FragmentCuts {
        inbound: Box::default(),
        outbound: Box::from([OutboundFragmentCut {
            edge: EdgeId::new(44),
            kind: EdgeKind::Stream,
            destination_fragment: FragmentId::new(34),
            destination_node: NodeId::new(34),
            projection: Box::from([CutValue {
                value,
                ty: ty(DataType::Boolean, false),
            }]),
            destination_imports: Box::default(),
            partitioning: EdgePartitioning {
                source: Distribution::Unconstrained,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Unconstrained,
                destination_multiplicity: RowMultiplicity::SingleCopy,
                routing_only: Box::default(),
            },
            source_port: crate::FragmentOutputPort::Root,
            payload: crate::ExchangePayloadContract::Relational,
            delivery: crate::DeliveryRequirement::DemandDriven,
        }]),
        runtime_filters: Box::default(),
        runtime_filter_bindings: Box::default(),
    };
    let error = validate_fragment(&fragment, &cuts).unwrap_err().to_string();
    assert!(error.contains("outbound cut type differs from its source value"));
}
