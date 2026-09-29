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

use arrow_schema::DataType;
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding, ConnectorReadRelationKind,
    ConnectorReadRelationPayload, ConnectorReadWorkSource,
};

use super::*;

mod aggregate_sequence_contract;
mod artifact_provenance_contract;
mod contract_regressions;
mod exchange_occurrence_contract;
mod ordering_window_assertion_contract;
mod partition_scan_contract;
mod runtime_filter_wait_contract;
mod set_operation_contract;
mod sink_contract;
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

fn scan_budget() -> ScanReadBudget {
    ScanReadBudget {
        max_batch_rows: 4096,
        max_batch_bytes: 8 * 1024 * 1024,
    }
}

fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 8,
        requires_power_of_two: true,
    }
}

fn literal_fragment(
    fragment_id: FragmentId,
    sink: FragmentSink,
    nullable: bool,
) -> (Fragment, ValueId) {
    let mut builder = FragmentBuilder::new(fragment_id);
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, nullable);
    let expression = builder
        .add_expression(
            node,
            value_type.clone(),
            ExprKind::Literal(LiteralValue::Int64(11)),
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
    builder
        .insert_node_unchecked(PhysicalNode {
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
    (builder.finish_definition(node, sink, dop()).unwrap(), value)
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
        work_source: ConnectorReadWorkSource::RuntimeSplits,
        selection_digest: [8; 32],
        schema: Box::from([RelationField {
            column,
            ty: ty(DataType::Int64, false),
        }]),
        predicate_guarantees: Box::default(),
        provided_properties: unconstrained(),
        artifact_inputs: Box::default(),
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
                read_budget: scan_budget(),
                provider_outputs: Box::from([(column, value)]),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    builder.finish_definition(scan, FragmentSink::Noop, dop())
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
    let mut valid = PlanBuilder::new(version());
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
    let mut duplicate = PlanBuilder::new(version());
    duplicate.add_fragment(duplicate_first).unwrap();
    duplicate.add_fragment(duplicate_second).unwrap();
    let error = duplicate.finish().unwrap_err().to_string();
    assert!(error.contains("provider read occurrence 13 is already owned"));
}

fn finish_scan_predicate_contract(
    guarantees: &[PredicateGuaranteeKind],
    residual_count: usize,
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
        .add_expression(
            scan,
            ty(DataType::Boolean, false),
            ExprKind::Literal(LiteralValue::Boolean(true)),
        )
        .unwrap();
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
                read_budget: scan_budget(),
                provider_outputs: Box::from([(column, value)]),
                residuals: std::iter::repeat_n(predicate, residual_count)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    builder.finish_definition(scan, FragmentSink::Noop, dop())
}

#[test]
fn pruning_only_relation_predicate_requires_one_scan_residual() {
    let missing = finish_scan_predicate_contract(&[PredicateGuaranteeKind::PruningOnly], 0)
        .unwrap_err()
        .to_string();
    assert!(missing.contains("pruning-only relation predicate must be evaluated"));

    finish_scan_predicate_contract(&[PredicateGuaranteeKind::PruningOnly], 1).unwrap();
    finish_scan_predicate_contract(&[PredicateGuaranteeKind::Exact], 0).unwrap();
    finish_scan_predicate_contract(&[PredicateGuaranteeKind::Exact], 1).unwrap();
}

#[test]
fn scan_predicate_contract_rejects_duplicate_guarantees_and_residuals() {
    let duplicate_guarantee = finish_scan_predicate_contract(
        &[
            PredicateGuaranteeKind::Exact,
            PredicateGuaranteeKind::PruningOnly,
        ],
        1,
    )
    .unwrap_err()
    .to_string();
    assert!(duplicate_guarantee.contains("conflicting guarantees"));

    let duplicate_residual =
        finish_scan_predicate_contract(&[PredicateGuaranteeKind::PruningOnly], 2)
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
    let edge = EdgeId::new(5);
    let (source, source_value) =
        literal_fragment(FragmentId::new(1), FragmentSink::Stream { edge }, false);

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
        .add_expression(
            result_node,
            ty(DataType::Int64, false),
            ExprKind::Value(destination_value),
        )
        .unwrap();
    destination_builder
        .insert_node_unchecked(PhysicalNode {
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
                    (result_expression, destination_value),
                    (result_expression, destination_value),
                ]),
            },
        })
        .unwrap();
    let destination = destination_builder
        .finish_definition(result_node, FragmentSink::Result, dop())
        .unwrap();

    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    plan.add_edge(Edge {
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
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
        },
    })
    .unwrap();
    plan.set_result_port(ResultPort {
        fragment: FragmentId::new(2),
        output: OutputPort {
            node: result_node,
            columns: Box::from([destination_value, destination_value]),
        },
        fields: Box::from([
            ResultField {
                name: "x".into(),
                alias: None,
                value: destination_value,
                ty: ty(DataType::Int64, false),
            },
            ResultField {
                name: "y".into(),
                alias: None,
                value: destination_value,
                ty: ty(DataType::Int64, false),
            },
        ]),
    })
    .unwrap();

    let plan = plan.finish().unwrap();
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
    let edge = EdgeId::new(5);
    let (source, source_value) =
        literal_fragment(FragmentId::new(1), FragmentSink::Stream { edge }, true);
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
        .finish_definition(destination_node, FragmentSink::Noop, dop())
        .unwrap();
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    plan.add_edge(Edge {
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
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
        },
    })
    .unwrap();
    let error = plan.finish().unwrap_err().to_string();
    assert!(error.contains("source and destination types differ"));
}

#[test]
fn edge_accepts_a_receiver_that_admits_null_the_sender_never_writes() {
    let edge = EdgeId::new(5);
    let (source, source_value) =
        literal_fragment(FragmentId::new(1), FragmentSink::Stream { edge }, false);
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
        .finish_definition(destination_node, FragmentSink::Noop, dop())
        .unwrap();
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    plan.add_edge(Edge {
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
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
        },
    })
    .unwrap();
    plan.finish().unwrap();
}

fn broadcast_edge_plan(
    destination_multiplicity: RowMultiplicity,
) -> Result<PhysicalPlan, ValidationErrors> {
    let edge = EdgeId::new(6);
    let (source, source_value) =
        literal_fragment(FragmentId::new(3), FragmentSink::Stream { edge }, false);
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
        .finish_definition(receiver, FragmentSink::Noop, dop())
        .unwrap();
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    plan.add_edge(Edge {
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
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
        },
    })
    .unwrap();
    plan.finish()
}

#[test]
fn broadcast_edge_is_the_explicit_single_copy_to_replicated_transition() {
    broadcast_edge_plan(RowMultiplicity::Replicated).unwrap();

    let error = broadcast_edge_plan(RowMultiplicity::SingleCopy)
        .unwrap_err()
        .to_string();
    assert!(error.contains("edge source and destination partitioning or row multiplicity"));
}

#[test]
fn broadcast_properties_cannot_claim_single_copy_rows() {
    let mut builder = FragmentBuilder::new(FragmentId::new(5));
    let node = builder.reserve_node_id().unwrap();
    let expression = builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(1)),
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
    builder
        .insert_node_unchecked(PhysicalNode {
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
                rows: Box::from([Box::from([expression])]),
            },
        })
        .unwrap();

    let error = builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .unwrap_err()
        .to_string();
    assert!(error.contains("broadcast physical properties require replicated row multiplicity"));
}

#[test]
fn non_empty_values_require_an_exact_singleton_or_replicated_broadcast_placement() {
    let finish = |fragment_id: u32, output_properties: PhysicalProperties| {
        let mut builder = FragmentBuilder::new(FragmentId::new(fragment_id));
        let node = builder.reserve_node_id().unwrap();
        let value_type = ty(DataType::Int64, false);
        let expression = builder
            .add_expression(
                node,
                value_type.clone(),
                ExprKind::Literal(LiteralValue::Int64(1)),
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
        builder
            .insert_node_unchecked(PhysicalNode {
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties,
                output: OutputPort {
                    node,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([expression])]),
                },
            })
            .unwrap();
        builder.finish_definition(node, FragmentSink::Noop, dop())
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
fn metadata_relation_and_progressive_artifact_sink_are_closed_contracts() {
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
                read_budget: scan_budget(),
                provider_outputs: Box::from([(column, value)]),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    let source = ArtifactSourceBinding {
        source: ProviderReadReference {
            binding: binding.clone(),
            input_version: ExactInputVersion::try_new(vec![9]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::SystemTable,
                encoded(&binding, ConnectorCodecCategory::ReadTable, 1),
                encoded(&binding, ConnectorCodecCategory::ReadView, 2),
            ),
        },
        selection_digest: [8; 32],
    };
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::SealedArtifact(Box::new(SealedArtifactSinkSpec {
                kind: ArtifactKind::try_new("split-directory").unwrap(),
                format: ArtifactFormat {
                    id: ArtifactFormatId::try_new("uea4.discovery").unwrap(),
                    revision: 1,
                },
                input: Box::from([ArtifactInputField {
                    value,
                    ty: ty(DataType::Int64, false),
                }]),
                partition_by: Box::default(),
                order_by: Box::default(),
                group_boundaries: Box::from([value]),
                source,
                required_coverage: CoverageSet {
                    domain: "manifest-entry".into(),
                    selection_digest: [8; 32],
                    ranges: Box::from([
                        CoverageRange {
                            start: None,
                            end: Some(Box::from([10])),
                        },
                        CoverageRange {
                            start: Some(Box::from([10])),
                            end: None,
                        },
                    ]),
                    complete_input: true,
                },
                max_reference_bytes: 4096,
            })),
            dop(),
        )
        .unwrap();
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(fragment).unwrap();
    let plan = plan.finish().unwrap();
    assert!(matches!(
        plan.fragments().get(&FragmentId::new(9)).unwrap().sink(),
        FragmentSink::SealedArtifact(_)
    ));
}

#[test]
fn artifact_coverage_rejects_overlapping_ranges() {
    let coverage = CoverageSet {
        domain: "groups".into(),
        selection_digest: [1; 32],
        ranges: Box::from([
            CoverageRange {
                start: Some(Box::from([1])),
                end: Some(Box::from([5])),
            },
            CoverageRange {
                start: Some(Box::from([4])),
                end: Some(Box::from([7])),
            },
        ]),
        complete_input: false,
    };
    let mut errors = super::validation::ValidationContext::new();
    super::validation::validate_coverage(&coverage, "coverage", &mut errors);
    assert_eq!(errors.len(), 1);
    assert!(errors[0].message().contains("overlap"));
}

#[test]
fn complete_artifact_coverage_rejects_a_gap() {
    let coverage = CoverageSet {
        domain: "groups".into(),
        selection_digest: [1; 32],
        ranges: Box::from([
            CoverageRange {
                start: None,
                end: Some(Box::from([10])),
            },
            CoverageRange {
                start: Some(Box::from([20])),
                end: None,
            },
        ]),
        complete_input: true,
    };
    let mut errors = super::validation::ValidationContext::new();
    super::validation::validate_coverage(&coverage, "coverage", &mut errors);
    assert!(errors.iter().any(|error| {
        error
            .message()
            .contains("complete coverage must form one gap-free unbounded domain")
    }));
}

#[test]
fn complete_artifact_coverage_rejects_bounded_ends() {
    let coverage = CoverageSet {
        domain: "groups".into(),
        selection_digest: [1; 32],
        ranges: Box::from([CoverageRange {
            start: Some(Box::from([10])),
            end: Some(Box::from([20])),
        }]),
        complete_input: true,
    };
    let mut errors = super::validation::ValidationContext::new();
    super::validation::validate_coverage(&coverage, "coverage", &mut errors);
    assert!(errors.iter().any(|error| {
        error
            .message()
            .contains("complete coverage must form one gap-free unbounded domain")
    }));
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
    let edge = EdgeId::new(17);
    let producer_id = FragmentId::new(3);
    let consumer_id = FragmentId::new(4);
    let (producer, producer_value) = literal_fragment(
        producer_id,
        FragmentSink::Multicast {
            edges: Box::from([edge]),
        },
        false,
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
        .finish_definition(receiver, FragmentSink::Noop, dop())
        .unwrap();

    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(producer).unwrap();
    builder.add_fragment(consumer).unwrap();
    builder
        .add_edge(Edge {
            id: edge,
            kind: EdgeKind::CteMulticast,
            source: EdgeSource {
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
            },
        })
        .unwrap();
    let plan = builder.finish().unwrap();
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
            },
            source_bindings: Box::default(),
            has_source_free_rows: false,
            change_stream_writer: None,
            writer_result: None,
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

#[test]
fn expression_semantic_depth_is_bounded_without_recursive_validation() {
    let mut builder = FragmentBuilder::new(FragmentId::new(8));
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let mut expression = builder
        .add_expression(
            node,
            value_type.clone(),
            ExprKind::Literal(LiteralValue::Int64(1)),
        )
        .unwrap();
    for _ in 1..=PlanLimits::FROZEN.expression_semantic_depth {
        expression = builder
            .add_expression(
                node,
                value_type.clone(),
                ExprKind::Cast {
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    expr: expression,
                    target: DataType::Int64,
                },
            )
            .unwrap();
    }
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

    let errors = builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .unwrap_err();
    assert!(
        errors
            .to_string()
            .contains("expression semantic depth exceeds 256")
    );
    // Exceeding a declared bound is admission information, not proof that the
    // producer emitted an illegal plan. A consumer must be able to tell those
    // apart without reading the prose.
    assert!(errors.has(ValidationErrorCategory::ResourceLimit));
    assert!(!errors.is_producer_defect());
}

/// Builds a fragment whose only expression is one n-ary connective over
/// `count` boolean literals, and returns the validation outcome.
fn wide_connective_fragment(
    count: usize,
    connective: fn(Box<[ExprId]>) -> ExprKind,
) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(9));
    let node = builder.reserve_node_id().unwrap();
    let boolean = ty(DataType::Boolean, false);
    let args = (0..count)
        .map(|_| {
            builder
                .add_expression(
                    node,
                    boolean.clone(),
                    ExprKind::Literal(LiteralValue::Boolean(true)),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let root = builder
        .add_expression(node, boolean.clone(), connective(args.into_boxed_slice()))
        .unwrap();
    let value = builder
        .add_value(
            boolean,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([root])]),
            },
        })
        .unwrap();
    builder.finish_definition(node, FragmentSink::Noop, dop())
}

/// Builds a single-copy singleton `Values` node returning one boolean column,
/// and returns the builder plus that node.
fn boolean_source() -> (FragmentBuilder, NodeId, ExprId) {
    let mut builder = FragmentBuilder::new(FragmentId::new(12));
    let values = builder.reserve_node_id().unwrap();
    let boolean = ty(DataType::Boolean, false);
    let literal = builder
        .add_expression(
            values,
            boolean.clone(),
            ExprKind::Literal(LiteralValue::Boolean(true)),
        )
        .unwrap();
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
    (builder, values, literal)
}

#[test]
fn a_typed_constructor_refuses_before_a_node_exists() {
    // These are the states the contract used to accept and then reject in a
    // whole-fragment pass. Refusing them at the construction point means the
    // caller finds out where the mistake was made, and the fragment never
    // holds an illegal node at all.
    let (mut builder, values, literal) = boolean_source();
    let filter = builder.reserve_node_id().unwrap();

    assert!(matches!(
        builder.add_filter(filter, values, Box::default()),
        Err(BuildError::FilterWithoutPredicate(_))
    ));
    assert!(matches!(
        builder.add_filter(filter, NodeId::new(404), Box::from([literal])),
        Err(BuildError::UndefinedInput { .. })
    ));
    // The literal belongs to the Values node, not to the filter.
    assert!(matches!(
        builder.add_filter(filter, values, Box::from([literal])),
        Err(BuildError::ExpressionOutsideOwner { .. })
    ));

    // A fresh builder, because a rejected predicate would otherwise leave an
    // expression no operator reaches - itself a contract violation.
    {
        let (mut other, other_values, _) = boolean_source();
        let other_filter = other.reserve_node_id().unwrap();
        let non_boolean = other
            .add_expression(
                other_filter,
                ty(DataType::Int64, false),
                ExprKind::Literal(LiteralValue::Int64(1)),
            )
            .unwrap();
        assert!(matches!(
            other.add_filter(other_filter, other_values, Box::from([non_boolean])),
            Err(BuildError::PredicateIsNotBoolean(_))
        ));
    }

    // Nothing above left a node behind.
    assert!(builder.node_output_properties(filter).is_none());

    let predicate = builder
        .add_expression(
            filter,
            ty(DataType::Boolean, false),
            ExprKind::Literal(LiteralValue::Boolean(true)),
        )
        .unwrap();
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
        .finish_definition(filter, FragmentSink::Noop, dop())
        .expect("a derived filter validates");
}

#[test]
fn a_wide_predicate_is_width_and_not_depth() {
    // Comfortably past PlanLimits::FROZEN.expression_semantic_depth. A dashboard filter panel
    // or a generated `(a=1 AND b=2) OR ...` reaches this size routinely, so it
    // must validate: the depth bound exists to stop pathological nesting, not
    // to cap how many conditions a query may state.
    const CONJUNCTS: usize = PlanLimits::FROZEN.expression_semantic_depth * 2;
    const _: () = assert!(
        CONJUNCTS > PlanLimits::FROZEN.expression_semantic_depth,
        "the fixture must exceed the depth bound or it proves nothing"
    );
    wide_connective_fragment(CONJUNCTS, |args| ExprKind::Conjunction { args })
        .expect("a wide conjunction is an ordinary query");
    wide_connective_fragment(CONJUNCTS, |args| ExprKind::Disjunction { args })
        .expect("a wide disjunction is an ordinary query");
}

#[test]
fn a_boolean_connective_needs_at_least_two_arguments() {
    // One spelling per predicate: a single-argument connective would be a
    // second way to write the argument itself.
    for count in [0, 1] {
        let errors = wide_connective_fragment(count, |args| ExprKind::Conjunction { args })
            .expect_err("a connective below arity two has no meaning");
        assert!(errors.is_producer_defect());
        assert!(
            errors
                .to_string()
                .contains("boolean connective requires at least two arguments")
        );
    }
}

#[test]
fn a_violated_contract_invariant_is_reported_as_a_producer_defect() {
    let mut builder = FragmentBuilder::new(FragmentId::new(8));
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let expression = builder
        .add_expression(
            node,
            value_type.clone(),
            ExprKind::Literal(LiteralValue::Int64(1)),
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
    builder
        .insert_node_unchecked(PhysicalNode {
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

    // A root that names an undefined node cannot be produced by a correct
    // planner, cannot succeed on another backend, and cannot be cleared by
    // raising a limit.
    let errors = builder
        .finish_definition(NodeId::new(999), FragmentSink::Noop, dop())
        .unwrap_err();
    assert!(errors.is_producer_defect());
    assert!(!errors.has(ValidationErrorCategory::ResourceLimit));
    assert!(!errors.has(ValidationErrorCategory::UnsupportedCapability));
}

#[test]
fn output_properties_cannot_name_a_value_absent_from_the_output_port() {
    let mut builder = FragmentBuilder::new(FragmentId::new(10));
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let expression = builder
        .add_expression(
            node,
            value_type.clone(),
            ExprKind::Literal(LiteralValue::Int64(1)),
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
    builder
        .insert_node_unchecked(PhysicalNode {
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
                rows: Box::from([Box::from([expression])]),
            },
        })
        .unwrap();

    let error = builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .unwrap_err()
        .to_string();
    assert!(error.contains("physical property key 1 is absent from the port"));
}

#[test]
fn project_cannot_publish_an_uncomputed_output() {
    let mut builder = FragmentBuilder::new(FragmentId::new(11));
    let source = builder.reserve_node_id().unwrap();
    let source_expression = builder
        .add_expression(
            source,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(1)),
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
    builder
        .insert_node_unchecked(PhysicalNode {
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node: source,
                columns: Box::from([source_value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([source_expression])]),
            },
        })
        .unwrap();
    let project = builder.reserve_node_id().unwrap();
    let fabricated_expression = builder
        .add_expression(
            project,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(2)),
        )
        .unwrap();
    let fabricated = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::Expr {
                node: project,
                expr: fabricated_expression,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
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
        .finish_definition(project, FragmentSink::Noop, dop())
        .unwrap_err()
        .to_string();
    assert!(error.contains("exact produced/pass-through sequence"));
}

#[test]
fn literal_representation_must_match_its_declared_type() {
    let mut builder = FragmentBuilder::new(FragmentId::new(12));
    let node = builder.reserve_node_id().unwrap();
    let expression = builder
        .add_expression(
            node,
            ty(DataType::Utf8, false),
            ExprKind::Literal(LiteralValue::Int64(1)),
        )
        .unwrap();
    let value = builder
        .add_value(
            ty(DataType::Utf8, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
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

    let error = builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .unwrap_err()
        .to_string();
    assert!(error.contains("differs from its declared type"));
}

#[test]
fn fragment_validation_requires_its_exact_artifact_cut() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 4),
    };
    let mut relation = metadata_relation(&binding, column.clone());
    if let Relation::Metadata(metadata) = &mut relation {
        metadata.selection_digest = [6; 32];
    }
    let source = ArtifactSourceBinding {
        source: relation.read().clone(),
        selection_digest: [6; 32],
    };
    let requirement = ArtifactInputRequirement {
        artifact: ArtifactRefId::new(1),
        kind: ArtifactKind::try_new("split-directory").unwrap(),
        format: ArtifactFormat {
            id: ArtifactFormatId::try_new("uea4.discovery").unwrap(),
            revision: 1,
        },
        schema: Box::from([ty(DataType::Int64, false)]),
        source,
        required_coverage: CoverageSet {
            domain: "manifest-entry".into(),
            selection_digest: [6; 32],
            ranges: Box::from([CoverageRange {
                start: None,
                end: None,
            }]),
            complete_input: true,
        },
    };
    match &mut relation {
        Relation::Metadata(metadata) => {
            metadata.artifact_inputs = Box::from([requirement]);
        }
        Relation::Data(_) => unreachable!(),
    }
    let mut builder = FragmentBuilder::new(FragmentId::new(13));
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
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                read_budget: scan_budget(),
                provider_outputs: Box::from([(column, value)]),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .unwrap();

    let error = validate_fragment(&fragment, &FragmentCuts::default())
        .unwrap_err()
        .to_string();
    assert!(error.contains("artifact references in fragment cuts differ"));
}

#[test]
fn exchange_partitioning_maps_source_and_destination_value_domains() {
    let edge = EdgeId::new(21);
    let (source, source_value) =
        literal_fragment(FragmentId::new(21), FragmentSink::Stream { edge }, false);
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
        .finish_definition(receiver_node, FragmentSink::Noop, dop())
        .unwrap();
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(receiver).unwrap();
    plan.add_edge(Edge {
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
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
        },
    })
    .unwrap();
    let plan = plan.finish().unwrap();
    let mut cuts = fragment_cuts(&plan, FragmentId::new(22)).unwrap();
    cuts.inbound[0].partitioning.source = Distribution::RoundRobin;
    let error = validate_fragment(plan.fragments().get(&FragmentId::new(22)).unwrap(), &cuts)
        .unwrap_err()
        .to_string();
    assert!(error.contains("source and destination partitioning"));
}

#[test]
fn integer_division_keeps_its_resolved_float_result() {
    let mut builder = FragmentBuilder::new(FragmentId::new(23));
    let node = builder.reserve_node_id().unwrap();
    let left = builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(6)),
        )
        .unwrap();
    let right = builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(2)),
        )
        .unwrap();
    let divide = builder
        .add_expression(
            node,
            ty(DataType::Float64, false),
            ExprKind::Binary {
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                left,
                op: BinaryOperator::Divide,
                right,
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            ty(DataType::Float64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([divide])]),
            },
        })
        .unwrap();
    builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .unwrap();
}

#[test]
fn left_outer_join_cannot_publish_the_original_build_value() {
    let mut builder = FragmentBuilder::new(FragmentId::new(24));
    let left_node = builder.reserve_node_id().unwrap();
    let left_expr = builder
        .add_expression(
            left_node,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(1)),
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
    builder
        .insert_node_unchecked(PhysicalNode {
            id: left_node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node: left_node,
                columns: Box::from([left]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([left_expr])]),
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
        .finish_definition(join, FragmentSink::Noop, dop())
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
        .add_expression(sort, ty(DataType::Int64, false), ExprKind::Value(a))
        .unwrap();
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
        .add_expression(project, ty(DataType::Int64, false), ExprKind::Value(b))
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
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
        .finish_definition(project, FragmentSink::Noop, dop())
        .unwrap();
}

#[test]
fn sealed_artifact_accepts_exact_source_provenance_across_an_exchange() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 7),
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
    let source_binding = relation.source_binding();
    let edge = EdgeId::new(26);
    let source_id = FragmentId::new(26);
    let mut source_builder = FragmentBuilder::new(source_id);
    let scan = source_builder.reserve_node_id().unwrap();
    let source_value = source_builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ProviderField {
                scan_node: scan,
                field: column.clone(),
            },
        )
        .unwrap();
    source_builder
        .insert_node_unchecked(PhysicalNode {
            id: scan,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: scan,
                columns: Box::from([source_value]),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                read_budget: scan_budget(),
                provider_outputs: Box::from([(column, source_value)]),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    let source = source_builder
        .finish_definition(scan, FragmentSink::Stream { edge }, dop())
        .unwrap();

    let destination_id = FragmentId::new(27);
    let mut destination_builder = FragmentBuilder::new(destination_id);
    let receiver = destination_builder.reserve_node_id().unwrap();
    let imported = destination_builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    destination_builder
        .insert_node_unchecked(PhysicalNode {
            id: receiver,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
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
        .finish_definition(
            receiver,
            FragmentSink::SealedArtifact(Box::new(SealedArtifactSinkSpec {
                kind: ArtifactKind::try_new("split-directory").unwrap(),
                format: ArtifactFormat {
                    id: ArtifactFormatId::try_new("uea4.discovery").unwrap(),
                    revision: 1,
                },
                input: Box::from([ArtifactInputField {
                    value: imported,
                    ty: ty(DataType::Int64, false),
                }]),
                partition_by: Box::default(),
                order_by: Box::default(),
                group_boundaries: Box::from([imported]),
                source: source_binding,
                required_coverage: CoverageSet {
                    domain: "manifest-entry".into(),
                    selection_digest: [8; 32],
                    ranges: Box::from([CoverageRange {
                        start: None,
                        end: None,
                    }]),
                    complete_input: true,
                },
                max_reference_bytes: 4096,
            })),
            dop(),
        )
        .unwrap();

    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    plan.add_edge(Edge {
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            fragment: source_id,
            projection: Box::from([source_value]),
        },
        destination: EdgeDestination {
            fragment: destination_id,
            node: receiver,
            receive_mapping: Box::from([(source_value, imported)]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Singleton,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Singleton,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    let plan = plan.finish().unwrap();
    let cuts = fragment_cuts(&plan, destination_id).unwrap();
    assert_eq!(cuts.inbound[0].source_bindings.len(), 1);

    let source_fragment = plan.fragments().get(&source_id).unwrap();
    let mut source_cuts = fragment_cuts(&plan, source_id).unwrap();
    source_cuts.outbound[0].source_bindings[0].selection_digest = [99; 32];
    let error = validate_fragment(source_fragment, &source_cuts)
        .expect_err("the source fragment must recheck its outbound provenance")
        .to_string();
    assert!(error.contains("outbound source provenance differs"));

    let mut source_cuts = fragment_cuts(&plan, source_id).unwrap();
    source_cuts.outbound[0].has_source_free_rows = true;
    let error = validate_fragment(source_fragment, &source_cuts)
        .expect_err("the source fragment must recheck source-free row provenance")
        .to_string();
    assert!(error.contains("outbound source provenance differs"));
}

#[test]
fn repeat_publishes_a_distinct_nullable_grouping_value() {
    let mut builder = FragmentBuilder::new(FragmentId::new(28));
    let source = builder.reserve_node_id().unwrap();
    let expression = builder
        .add_expression(
            source,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(1)),
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
    builder
        .insert_node_unchecked(PhysicalNode {
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node: source,
                columns: Box::from([input]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([expression])]),
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
        .finish_definition(repeat, FragmentSink::Noop, dop())
        .unwrap();
}

#[test]
fn lambda_parameter_type_is_part_of_its_declaration() {
    let mut builder = FragmentBuilder::new(FragmentId::new(29));
    let node = builder.reserve_node_id().unwrap();
    let lambda = builder.reserve_expression_id().unwrap();
    let parameter = builder
        .add_expression_in_scope(
            node,
            Some(lambda),
            ty(DataType::Utf8, false),
            ExprKind::LambdaParameter { lambda, ordinal: 0 },
        )
        .unwrap();
    builder
        .insert_expression(ExprNode {
            id: lambda,
            owner: node,
            lambda_scope: None,
            ty: ty(DataType::Utf8, false),
            kind: ExprKind::Lambda {
                parameter_types: Box::from([ty(DataType::Int64, false)]),
                body: parameter,
            },
        })
        .unwrap();
    let output = builder
        .add_value(
            ty(DataType::Utf8, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([lambda])]),
            },
        })
        .unwrap();
    let error = builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .unwrap_err()
        .to_string();
    assert!(error.contains("lambda parameter does not reference a matching lambda owner"));
}

fn finish_fragment_with_declared_type(data_type: DataType) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(30));
    let node = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ty(data_type, true),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        })
        .unwrap();
    builder.finish_definition(node, FragmentSink::Noop, dop())
}

#[test]
fn arrow_data_type_depth_is_bounded_iteratively() {
    use std::sync::Arc;

    use arrow_schema::Field;

    let mut data_type = DataType::Int64;
    for _ in 0..MAX_DATA_TYPE_DEPTH {
        data_type = DataType::List(Arc::new(Field::new_list_field(data_type, true)));
    }
    let error = finish_fragment_with_declared_type(data_type)
        .unwrap_err()
        .to_string();
    assert!(error.contains("Arrow data type depth exceeds"));
}

#[test]
fn arrow_data_type_node_count_is_bounded_before_stack_growth() {
    use std::sync::Arc;

    use arrow_schema::{Field, Fields};

    let fields = (0..MAX_DATA_TYPE_NODES)
        .map(|index| Arc::new(Field::new(index.to_string(), DataType::Int64, true)))
        .collect::<Vec<_>>();
    let error = finish_fragment_with_declared_type(DataType::Struct(Fields::from(fields)))
        .unwrap_err()
        .to_string();
    assert!(error.contains("Arrow data type contains more than"));
}

#[test]
fn arrow_data_type_rejects_invalid_decimal_and_fixed_size_parameters() {
    let decimal_error = finish_fragment_with_declared_type(DataType::Decimal128(0, 0))
        .unwrap_err()
        .to_string();
    assert!(decimal_error.contains("Arrow decimal precision/scale"));

    let fixed_error = finish_fragment_with_declared_type(DataType::FixedSizeBinary(-1))
        .unwrap_err()
        .to_string();
    assert!(fixed_error.contains("Arrow fixed-size length -1"));

    let time_error =
        finish_fragment_with_declared_type(DataType::Time32(arrow_schema::TimeUnit::Nanosecond))
            .unwrap_err()
            .to_string();
    assert!(time_error.contains("Arrow Time32 must use second or millisecond units"));

    let dictionary_error = finish_fragment_with_declared_type(DataType::Dictionary(
        Box::new(DataType::Utf8),
        Box::new(DataType::Int64),
    ))
    .unwrap_err()
    .to_string();
    assert!(dictionary_error.contains("Arrow dictionary key must be an integer type"));
}

#[test]
fn arrow_field_name_and_metadata_are_bounded() {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::Field;

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
    .with_metadata(metadata);
    let error = finish_fragment_with_declared_type(DataType::List(Arc::new(field)))
        .unwrap_err()
        .to_string();
    assert!(error.contains("Arrow field name exceeds"));
    assert!(error.contains("Arrow field metadata key or value exceeds"));
}

#[test]
fn annotation_count_and_value_bytes_are_bounded() {
    let (fragment, _) = literal_fragment(FragmentId::new(31), FragmentSink::Noop, false);
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(fragment).unwrap();
    for _ in 0..=MAX_ANNOTATIONS {
        plan.add_annotation(PlanAnnotation {
            subject: AnnotationSubject::Plan,
            key: "diagnostic".into(),
            value: "ok".into(),
        });
    }
    let count_error = plan.finish().unwrap_err().to_string();
    assert!(count_error.contains("annotations: contains 4097 items, exceeding 4096"));

    let (fragment, _) = literal_fragment(FragmentId::new(32), FragmentSink::Noop, false);
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(fragment).unwrap();
    plan.add_annotation(PlanAnnotation {
        subject: AnnotationSubject::Plan,
        key: "diagnostic".into(),
        value: "v".repeat(MAX_ANNOTATION_VALUE_BYTES + 1).into(),
    });
    let value_error = plan.finish().unwrap_err().to_string();
    assert!(value_error.contains("annotation `diagnostic` value is 16385 bytes, exceeding 16384"));
}

#[test]
fn independent_fragment_cut_types_share_the_same_resource_validation() {
    let (fragment, value) = literal_fragment(FragmentId::new(33), FragmentSink::Noop, false);
    let cuts = FragmentCuts {
        inbound: Box::default(),
        outbound: Box::from([OutboundFragmentCut {
            edge: EdgeId::new(44),
            kind: EdgeKind::Stream,
            destination_fragment: FragmentId::new(34),
            projection: Box::from([CutValue {
                value,
                ty: ty(DataType::Decimal128(0, 0), false),
            }]),
            destination_imports: Box::default(),
            partitioning: EdgePartitioning {
                source: Distribution::Unconstrained,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Unconstrained,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
            change_stream_writer: None,
            writer_result: None,
            source_bindings: Box::default(),
            has_source_free_rows: false,
        }]),
        artifact_refs: Box::default(),
        runtime_filters: Box::default(),
        runtime_filter_proof: RuntimeFilterProofGraph::default(),
    };
    let error = validate_fragment(&fragment, &cuts).unwrap_err().to_string();
    assert!(error.contains("Arrow decimal precision/scale"));
}

#[test]
fn physical_binary_policy_rejects_reporting_comparisons_before_publication() {
    use novarocks_type_contract::DecimalOverflowPolicy as Policy;
    for (op, policy, accepted) in [
        (BinaryOperator::Eq, Policy::OutputNull, true),
        (BinaryOperator::Eq, Policy::ReportError, false),
        (BinaryOperator::Add, Policy::OutputNull, true),
        (BinaryOperator::Add, Policy::ReportError, true),
    ] {
        let mut builder = FragmentBuilder::new(FragmentId::new(31));
        let node = builder.reserve_node_id().unwrap();
        let operand = builder
            .add_expression(
                node,
                ty(DataType::Int64, false),
                ExprKind::Literal(LiteralValue::Int64(1)),
            )
            .unwrap();
        let result_type = if matches!(op, BinaryOperator::Eq) {
            DataType::Boolean
        } else {
            DataType::Int64
        };
        let expr = builder
            .add_expression(
                node,
                ty(result_type.clone(), false),
                ExprKind::Binary {
                    left: operand,
                    op,
                    right: operand,
                    decimal_overflow_policy: policy,
                },
            )
            .unwrap();
        let value = builder
            .add_value(
                ty(result_type, false),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: singleton(),
                output: OutputPort {
                    node,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([expr])]),
                },
            })
            .unwrap();
        let result = builder.finish_definition(node, FragmentSink::Noop, dop());
        if accepted {
            result.unwrap();
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("requires OutputNull decimal policy")
            );
        }
    }
}

#[test]
fn physical_nested_decimal_cast_requires_a_supported_frozen_policy() {
    use arrow_schema::Field;
    use novarocks_type_contract::DecimalOverflowPolicy as Policy;
    use std::sync::Arc;
    let source = DataType::List(Arc::new(Field::new(
        "element",
        DataType::Decimal128(10, 2),
        true,
    )));
    let target = DataType::List(Arc::new(Field::new(
        "element",
        DataType::Decimal128(12, 3),
        true,
    )));
    for (result_type, policy, accepted) in [
        (target.clone(), Policy::ReportError, false),
        (target, Policy::OutputNull, true),
        (source.clone(), Policy::ReportError, true),
    ] {
        let mut builder = FragmentBuilder::new(FragmentId::new(32));
        let node = builder.reserve_node_id().unwrap();
        let operand = builder
            .add_expression(
                node,
                ty(source.clone(), true),
                ExprKind::Literal(LiteralValue::Null),
            )
            .unwrap();
        let expression = builder
            .add_expression(
                node,
                ty(result_type.clone(), true),
                ExprKind::Cast {
                    expr: operand,
                    target: result_type.clone(),
                    decimal_overflow_policy: policy,
                },
            )
            .unwrap();
        let value = builder
            .add_value(
                ty(result_type, true),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
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
        let result = builder.finish_definition(node, FragmentSink::Noop, dop());
        if accepted {
            result.unwrap();
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("does not support nested type conversion")
            );
        }
    }
}
