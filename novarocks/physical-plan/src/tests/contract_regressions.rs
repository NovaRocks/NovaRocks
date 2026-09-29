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

use super::*;

fn coverage(
    witnesses: impl IntoIterator<Item = RuntimeFilterWitnessId>,
    all_of: bool,
) -> RuntimeFilterCoverage {
    let witnesses = witnesses.into_iter().collect::<Vec<_>>();
    let mut nodes = witnesses
        .iter()
        .copied()
        .map(RuntimeFilterCoverageNode::Witness)
        .collect::<Vec<_>>();
    let children = (0..u32::try_from(witnesses.len()).unwrap()).collect::<Vec<_>>();
    nodes.push(if all_of {
        RuntimeFilterCoverageNode::AllOf {
            children: children.into_boxed_slice(),
        }
    } else {
        RuntimeFilterCoverageNode::AnyOf {
            children: children.into_boxed_slice(),
        }
    });
    RuntimeFilterCoverage {
        root: u32::try_from(nodes.len() - 1).unwrap(),
        nodes: nodes.into_boxed_slice(),
    }
}

fn append_literal(builder: &mut FragmentBuilder, nullable: bool) -> (NodeId, ValueId) {
    append_literal_with_distribution(builder, nullable, Distribution::Singleton)
}

fn append_literal_with_distribution(
    builder: &mut FragmentBuilder,
    nullable: bool,
    distribution: Distribution,
) -> (NodeId, ValueId) {
    let row_multiplicity = if distribution == Distribution::Broadcast {
        RowMultiplicity::Replicated
    } else {
        RowMultiplicity::SingleCopy
    };
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, nullable);
    let expression = builder
        .add_expression(
            node,
            value_type.clone(),
            ExprKind::Literal(if nullable {
                LiteralValue::Null
            } else {
                LiteralValue::Int64(11)
            }),
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
            output_properties: PhysicalProperties {
                distribution,
                row_multiplicity,
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
    (node, value)
}

fn finish_largeint_literal(value_type: ValueType) -> Result<Fragment, String> {
    let mut builder = FragmentBuilder::new(FragmentId::new(99));
    let node = builder
        .reserve_node_id()
        .map_err(|error| error.to_string())?;
    let expression = builder
        .add_expression(
            node,
            value_type.clone(),
            ExprKind::Literal(LiteralValue::LargeInt(i128::MIN)),
        )
        .map_err(|error| error.to_string())?;
    let value = builder
        .add_value(
            value_type,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .map_err(|error| error.to_string())?;
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
        .map_err(|error| error.to_string())?;
    builder
        .finish_definition(node, FragmentSink::Noop, dop())
        .map_err(|error| error.to_string())
}

/// A large-int literal must be carried as a large integer, and the carrier
/// may admit null even though the value never is: an exact value standing
/// where the statement admits null is sound, and is how a literal reaches a
/// position typed conservatively.
#[test]
fn largeint_literal_requires_a_largeint_carrier_that_may_admit_null() {
    let largeint = DataType::FixedSizeBinary(novarocks_type_contract::LARGEINT_BYTE_WIDTH);
    finish_largeint_literal(ty(largeint.clone(), false)).unwrap();
    finish_largeint_literal(ty(largeint, true)).unwrap();

    let wrong_type = finish_largeint_literal(ty(DataType::Int64, false)).unwrap_err();
    assert!(wrong_type.contains("literal largeint differs from its declared type Int64"));
}

fn null_safe_join_filter(
    null_semantics: RuntimeFilterNullSemantics,
) -> (Fragment, RuntimeFilter, ValueId) {
    let fragment_id = FragmentId::new(100);
    let mut builder = FragmentBuilder::new(fragment_id);
    let (left_source, left_value) = append_literal(&mut builder, true);
    let left = builder.reserve_node_id().unwrap();
    let left_identity = builder
        .add_expression(left, ty(DataType::Int64, true), ExprKind::Value(left_value))
        .unwrap();
    let other_expression = builder
        .add_expression(
            left,
            ty(DataType::Int64, true),
            ExprKind::Literal(LiteralValue::Null),
        )
        .unwrap();
    let other_left_value = builder
        .add_value(
            ty(DataType::Int64, true),
            ValueOrigin::Expr {
                node: left,
                expr: other_expression,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: left,
            inputs: Box::from([left_source]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: left,
                columns: Box::from([left_value, other_left_value]),
            },
            kind: NodeKind::Project {
                expressions: Box::from([
                    (left_identity, left_value),
                    (other_expression, other_left_value),
                ]),
            },
        })
        .unwrap();
    let (right, right_value) =
        append_literal_with_distribution(&mut builder, true, Distribution::Broadcast);
    let join = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, true);
    let left_key = builder
        .add_expression(join, value_type.clone(), ExprKind::Value(left_value))
        .unwrap();
    let right_key = builder
        .add_expression(join, value_type.clone(), ExprKind::Value(right_value))
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: join,
            inputs: Box::from([left, right]),
            required_inputs: Box::from([
                unconstrained(),
                PhysicalProperties {
                    distribution: Distribution::Broadcast,
                    row_multiplicity: RowMultiplicity::Replicated,
                    ordering: Box::default(),
                },
            ]),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: join,
                columns: Box::from([left_value, right_value]),
            },
            kind: NodeKind::HashJoin {
                kind: JoinKind::Inner,
                build_side: JoinSide::Right,
                keys: Box::from([JoinKey {
                    left: left_key,
                    right: right_key,
                    null_safe: true,
                }]),
                distribution: JoinDistribution::BroadcastBuild,
                residual: None,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    let filter_id = RuntimeFilterId::new(100);
    builder.attach_runtime_filter(filter_id).unwrap();
    let fragment = builder
        .finish_definition(join, FragmentSink::Noop, dop())
        .unwrap();
    let witness = RuntimeFilterWitnessId::new(1);
    let equality = RuntimeFilterEqualityWitnessId::new(1);
    let filter = RuntimeFilter {
        id: filter_id,
        kind: RuntimeFilterKind::InList,
        domain: RuntimeFilterDomain::Membership {
            ty: value_type,
            null_semantics,
        },
        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
        availability_coverage: coverage([witness], true),
        terminal_coverage: coverage([witness], true),
        equality_witnesses: Box::from([RuntimeFilterEqualityWitness {
            id: equality,
            fragment: fragment_id,
            join,
            key_ordinal: 0,
            domain_side: JoinSide::Right,
        }]),
        reduction: RuntimeFilterReduction::SetUnion,
        producers: Box::from([RuntimeFilterProducer {
            witness,
            endpoint: RuntimeFilterEndpoint {
                fragment: fragment_id,
                node: join,
                values: Box::from([right_value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 },
            contribution_kinds: Box::from([
                RuntimeFilterContributionKind::FinalDomainShard,
                RuntimeFilterContributionKind::ProducerClosed,
            ]),
            completion: RuntimeFilterCompletion::FencedCommittedDomain,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::default(),
                non_build_edges: Box::default(),
            },
            target: RuntimeFilterProducerTarget::JoinBuildKey { equality },
        }]),
        consumers: Box::from([RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: fragment_id,
                node: join,
                values: Box::from([left_value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
            capabilities: Box::from([
                RuntimeFilterArtifactCapability::Membership,
                RuntimeFilterArtifactCapability::EmptyDomain,
            ]),
            activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: RuntimeFilterConsumerTarget::JoinProbeKey { equality },
        }]),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 100,
            max_retries: 1,
        },
    };
    (fragment, filter, other_left_value)
}

fn scan_lineage_filter(
    through_filter: bool,
    through_project: bool,
    through_inner_join: bool,
    second_provider_field: bool,
) -> (Fragment, RuntimeFilter, Option<ValueId>) {
    let fragment_id = FragmentId::new(101);
    let binding = connector_binding();
    let first_column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 41),
    };
    let second_column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 42),
    };
    let mut relation = metadata_relation(&binding, first_column.clone());
    if second_provider_field {
        let schema = Box::from([
            RelationField {
                column: first_column.clone(),
                ty: ty(DataType::Int64, false),
            },
            RelationField {
                column: second_column.clone(),
                ty: ty(DataType::Int64, false),
            },
        ]);
        match &mut relation {
            Relation::Data(relation) => relation.schema = schema,
            Relation::Metadata(relation) => relation.schema = schema,
        }
    }

    let mut builder = FragmentBuilder::new(fragment_id);
    let scan = builder.reserve_node_id().unwrap();
    let provider_outputs = relation
        .schema()
        .iter()
        .map(|field| {
            let value = builder
                .add_value(
                    field.ty.clone(),
                    ValueOrigin::ProviderField {
                        scan_node: scan,
                        field: field.column.clone(),
                    },
                )
                .unwrap();
            (field.column.clone(), value)
        })
        .collect::<Vec<_>>();
    let scan_values = provider_outputs
        .iter()
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: scan,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node: scan,
                columns: scan_values.clone().into_boxed_slice(),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                read_budget: scan_budget(),
                provider_outputs: provider_outputs.into_boxed_slice(),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
        })
        .unwrap();

    let (lineage_input, lineage_value, mut lineage) = if through_filter {
        let filter = builder.reserve_node_id().unwrap();
        let predicate = builder
            .add_expression(
                filter,
                ty(DataType::Boolean, false),
                ExprKind::Literal(LiteralValue::Boolean(true)),
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: filter,
                inputs: Box::from([scan]),
                required_inputs: Box::from([unconstrained()]),
                output_properties: unconstrained(),
                output: OutputPort {
                    node: filter,
                    columns: scan_values.clone().into_boxed_slice(),
                },
                kind: NodeKind::Filter {
                    predicates: Box::from([predicate]),
                },
            })
            .unwrap();
        (
            filter,
            scan_values[0],
            vec![RuntimeFilterLineageStep::FilterPassThrough {
                fragment: fragment_id,
                node: filter,
                input_ordinal: 0,
            }],
        )
    } else {
        (scan, scan_values[0], Vec::new())
    };

    let (mut probe_node, probe_value, mut lineage) = if through_project {
        let project = builder.reserve_node_id().unwrap();
        let identity = builder
            .add_expression(
                project,
                ty(DataType::Int64, false),
                ExprKind::Value(lineage_value),
            )
            .unwrap();
        let projected = builder
            .add_value(
                ty(DataType::Int64, false),
                ValueOrigin::Expr {
                    node: project,
                    expr: identity,
                },
            )
            .unwrap();
        let other_expression = builder
            .add_expression(
                project,
                ty(DataType::Int64, false),
                ExprKind::Literal(LiteralValue::Int64(7)),
            )
            .unwrap();
        let other = builder
            .add_value(
                ty(DataType::Int64, false),
                ValueOrigin::Expr {
                    node: project,
                    expr: other_expression,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: project,
                inputs: Box::from([lineage_input]),
                required_inputs: Box::from([unconstrained()]),
                output_properties: unconstrained(),
                output: OutputPort {
                    node: project,
                    columns: Box::from([projected, projected, other]),
                },
                kind: NodeKind::Project {
                    expressions: Box::from([
                        (identity, projected),
                        (identity, projected),
                        (other_expression, other),
                    ]),
                },
            })
            .unwrap();
        lineage.insert(
            0,
            RuntimeFilterLineageStep::ProjectIdentity {
                fragment: fragment_id,
                node: project,
                output_ordinal: 0,
            },
        );
        (project, projected, lineage)
    } else {
        (lineage_input, lineage_value, lineage)
    };

    if through_inner_join {
        let (equivalent, equivalent_value) =
            append_literal_with_distribution(&mut builder, false, Distribution::Broadcast);
        let inner_join = builder.reserve_node_id().unwrap();
        let left_key = builder
            .add_expression(
                inner_join,
                ty(DataType::Int64, false),
                ExprKind::Value(probe_value),
            )
            .unwrap();
        let right_key = builder
            .add_expression(
                inner_join,
                ty(DataType::Int64, false),
                ExprKind::Value(equivalent_value),
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: inner_join,
                inputs: Box::from([probe_node, equivalent]),
                required_inputs: Box::from([
                    unconstrained(),
                    PhysicalProperties {
                        distribution: Distribution::Broadcast,
                        row_multiplicity: RowMultiplicity::Replicated,
                        ordering: Box::default(),
                    },
                ]),
                output_properties: unconstrained(),
                output: OutputPort {
                    node: inner_join,
                    columns: Box::from([probe_value, equivalent_value]),
                },
                kind: NodeKind::HashJoin {
                    kind: JoinKind::Inner,
                    keys: Box::from([JoinKey {
                        left: left_key,
                        right: right_key,
                        null_safe: false,
                    }]),
                    build_side: JoinSide::Right,
                    distribution: JoinDistribution::BroadcastBuild,
                    residual: None,
                    null_extended: Box::default(),
                },
            })
            .unwrap();
        lineage.insert(
            0,
            RuntimeFilterLineageStep::JoinEquality {
                fragment: fragment_id,
                node: inner_join,
                key_ordinal: 0,
                source_side: JoinSide::Left,
                target_side: JoinSide::Left,
            },
        );
        probe_node = inner_join;
    }

    let (build, build_value) =
        append_literal_with_distribution(&mut builder, false, Distribution::Broadcast);
    let join = builder.reserve_node_id().unwrap();
    let probe_key = builder
        .add_expression(
            join,
            ty(DataType::Int64, false),
            ExprKind::Value(probe_value),
        )
        .unwrap();
    let build_key = builder
        .add_expression(
            join,
            ty(DataType::Int64, false),
            ExprKind::Value(build_value),
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: join,
            inputs: Box::from([probe_node, build]),
            required_inputs: Box::from([
                unconstrained(),
                PhysicalProperties {
                    distribution: Distribution::Broadcast,
                    row_multiplicity: RowMultiplicity::Replicated,
                    ordering: Box::default(),
                },
            ]),
            output_properties: unconstrained(),
            output: OutputPort {
                node: join,
                columns: Box::from([probe_value, build_value]),
            },
            kind: NodeKind::HashJoin {
                kind: JoinKind::Inner,
                build_side: JoinSide::Right,
                keys: Box::from([JoinKey {
                    left: probe_key,
                    right: build_key,
                    null_safe: false,
                }]),
                distribution: JoinDistribution::BroadcastBuild,
                residual: None,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    let filter_id = RuntimeFilterId::new(101);
    builder.attach_runtime_filter(filter_id).unwrap();
    let fragment = builder
        .finish_definition(join, FragmentSink::Noop, dop())
        .unwrap();
    let witness = RuntimeFilterWitnessId::new(101);
    let equality = RuntimeFilterEqualityWitnessId::new(101);
    let filter = RuntimeFilter {
        id: filter_id,
        kind: RuntimeFilterKind::InList,
        domain: RuntimeFilterDomain::Membership {
            ty: ty(DataType::Int64, false),
            null_semantics: RuntimeFilterNullSemantics::NeverMatches,
        },
        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
        reduction: RuntimeFilterReduction::SetUnion,
        availability_coverage: coverage([witness], false),
        terminal_coverage: coverage([witness], false),
        equality_witnesses: Box::from([RuntimeFilterEqualityWitness {
            id: equality,
            fragment: fragment_id,
            join,
            key_ordinal: 0,
            domain_side: JoinSide::Right,
        }]),
        producers: Box::from([RuntimeFilterProducer {
            witness,
            endpoint: RuntimeFilterEndpoint {
                fragment: fragment_id,
                node: join,
                values: Box::from([build_value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 },
            contribution_kinds: Box::from([
                RuntimeFilterContributionKind::ValueDomainDelta,
                RuntimeFilterContributionKind::ProducerClosed,
            ]),
            completion: RuntimeFilterCompletion::ProducerClosed,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::default(),
                non_build_edges: Box::default(),
            },
            target: RuntimeFilterProducerTarget::JoinBuildKey { equality },
        }]),
        consumers: Box::from([RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: fragment_id,
                node: scan,
                values: Box::from([scan_values[0]]),
            },
            apply_point: RuntimeFilterApplyPoint::ScanSource,
            capabilities: Box::from([
                RuntimeFilterArtifactCapability::Membership,
                RuntimeFilterArtifactCapability::EmptyDomain,
            ]),
            activation: RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
                late_apply: LateApplyGranularity::Batch,
            },
            target: RuntimeFilterConsumerTarget::ScanField {
                equality,
                lineage: lineage.into_boxed_slice(),
            },
        }]),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 100,
            max_retries: 1,
        },
    };
    (fragment, filter, scan_values.get(1).copied())
}

fn assert_runtime_filter_plan_accepted(fragment: Fragment, filter: RuntimeFilter) {
    let cuts = local_runtime_filter_cuts(&fragment, &filter);
    validate_fragment(&fragment, &cuts).unwrap();
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment).unwrap();
    builder.add_runtime_filter(filter).unwrap();
    builder.finish().unwrap();
}

fn local_runtime_filter_cuts(fragment: &Fragment, filter: &RuntimeFilter) -> FragmentCuts {
    FragmentCuts {
        inbound: Box::default(),
        outbound: Box::default(),
        artifact_refs: Box::default(),
        runtime_filters: Box::from([filter.clone()]),
        runtime_filter_proof: RuntimeFilterProofGraph {
            fragments: Box::from([fragment.clone()]),
            edges: Box::default(),
            filters: Box::from([filter.clone()]),
        },
    }
}

fn aggregate_topn_filter() -> (Fragment, RuntimeFilter) {
    let fragment_id = FragmentId::new(105);
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 51),
    };
    let mut relation = metadata_relation(&binding, column.clone());
    match &mut relation {
        Relation::Data(relation) => relation.provided_properties = singleton(),
        Relation::Metadata(relation) => relation.provided_properties = singleton(),
    }
    let mut builder = FragmentBuilder::new(fragment_id);
    let scan = builder.reserve_node_id().unwrap();
    let scan_value = builder
        .add_value(
            ty(DataType::Int64, false),
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
            output_properties: singleton(),
            output: OutputPort {
                node: scan,
                columns: Box::from([scan_value]),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                read_budget: scan_budget(),
                provider_outputs: Box::from([(column, scan_value)]),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    let aggregate = builder.reserve_node_id().unwrap();
    let group_key = builder
        .add_expression(
            aggregate,
            ty(DataType::Int64, false),
            ExprKind::Value(scan_value),
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: aggregate,
            inputs: Box::from([scan]),
            required_inputs: Box::from([singleton()]),
            output_properties: singleton(),
            output: OutputPort {
                node: aggregate,
                columns: Box::from([scan_value]),
            },
            kind: NodeKind::Aggregate {
                group_by: Box::from([(group_key, scan_value)]),
                calls: Box::default(),
                grouping: crate::AggregateGrouping::Complete,
            },
        })
        .unwrap();
    let topn = builder.reserve_node_id().unwrap();
    let order_key = builder
        .add_expression(
            topn,
            ty(DataType::Int64, false),
            ExprKind::Value(scan_value),
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: topn,
            inputs: Box::from([aggregate]),
            required_inputs: Box::from([singleton()]),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::from([OrderingKey {
                    value: scan_value,
                    direction: SortDirection::Ascending,
                    null_ordering: NullOrdering::Last,
                }]),
            },
            output: OutputPort {
                node: topn,
                columns: Box::from([scan_value]),
            },
            kind: NodeKind::TopN {
                order_by: Box::from([SortExpr {
                    expr: order_key,
                    direction: SortDirection::Ascending,
                    null_ordering: NullOrdering::Last,
                }]),
                limit: 5,
                offset: 0,
                phase: TopNPhase::Single,
            },
        })
        .unwrap();
    let filter_id = RuntimeFilterId::new(105);
    builder.attach_runtime_filter(filter_id).unwrap();
    let fragment = builder
        .finish_definition(topn, FragmentSink::Noop, dop())
        .unwrap();
    let witness = RuntimeFilterWitnessId::new(105);
    let filter = RuntimeFilter {
        id: filter_id,
        kind: RuntimeFilterKind::MinMax,
        domain: RuntimeFilterDomain::Ordered {
            key: RuntimeFilterOrderKey {
                ty: ty(DataType::Int64, false),
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            },
            inclusive: true,
            comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
        },
        lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
        reduction: RuntimeFilterReduction::TightenOrderedBound,
        availability_coverage: coverage([witness], true),
        terminal_coverage: coverage([witness], true),
        equality_witnesses: Box::default(),
        producers: Box::from([RuntimeFilterProducer {
            witness,
            endpoint: RuntimeFilterEndpoint {
                fragment: fragment_id,
                node: aggregate,
                values: Box::from([scan_value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
            contribution_kinds: Box::from([
                RuntimeFilterContributionKind::OrderedBoundUpdate,
                RuntimeFilterContributionKind::ProducerClosed,
            ]),
            completion: RuntimeFilterCompletion::ProducerClosed,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::default(),
                non_build_edges: Box::default(),
            },
            target: RuntimeFilterProducerTarget::AggregateTopNKey {
                group_key_ordinal: 0,
                topn,
                phase: TopNPhase::Single,
                order_key_ordinal: 0,
                limit: 5,
                offset: 0,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            },
        }]),
        consumers: Box::from([RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: fragment_id,
                node: scan,
                values: Box::from([scan_value]),
            },
            apply_point: RuntimeFilterApplyPoint::ScanSource,
            capabilities: Box::from([RuntimeFilterArtifactCapability::OrderedRange]),
            activation: RuntimeFilterConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
            target: RuntimeFilterConsumerTarget::AggregateTopNScanField {
                producer: witness,
                lineage: Box::default(),
            },
        }]),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 100,
            max_retries: 1,
        },
    };
    (fragment, filter)
}

#[test]
fn aggregate_topn_runtime_filter_requires_a_positive_frozen_limit() {
    let (fragment, filter) = aggregate_topn_filter();
    assert_runtime_filter_plan_accepted(fragment.clone(), filter.clone());

    let mut invalid = filter;
    let RuntimeFilterProducerTarget::AggregateTopNKey { limit, .. } =
        &mut invalid.producers[0].target
    else {
        unreachable!();
    };
    *limit = 0;
    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        invalid,
        "runtime filter Aggregate TopN producer has a zero limit",
    );
}

#[test]
fn aggregate_topn_runtime_filter_recomputes_the_exact_topn_witness() {
    let (fragment, mut filter) = aggregate_topn_filter();
    let RuntimeFilterProducerTarget::AggregateTopNKey { direction, .. } =
        &mut filter.producers[0].target
    else {
        unreachable!();
    };
    *direction = SortDirection::Descending;
    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "runtime filter Aggregate TopN producer target does not match its exact group key",
    );
}

#[test]
fn aggregate_topn_runtime_filter_consumer_requires_its_exact_producer_witness() {
    let (fragment, mut filter) = aggregate_topn_filter();
    let RuntimeFilterConsumerTarget::AggregateTopNScanField { producer, .. } =
        &mut filter.consumers[0].target
    else {
        unreachable!();
    };
    *producer = RuntimeFilterWitnessId::new(10_105);
    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "runtime filter Aggregate TopN consumer has no exact producer witness",
    );
}

#[test]
fn aggregate_topn_monotonic_updates_require_live_activation() {
    let (fragment, mut filter) = aggregate_topn_filter();
    filter.consumers[0].activation =
        RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
            late_apply: LateApplyGranularity::Batch,
        };

    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "monotonic runtime-filter consumer requires non-blocking live-update activation",
    );
}

#[test]
fn runtime_filter_scan_field_accepts_direct_and_identity_project_lineage() {
    for (through_filter, through_project) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let (fragment, filter, _) =
            scan_lineage_filter(through_filter, through_project, false, false);
        assert_runtime_filter_plan_accepted(fragment, filter);
    }
}

#[test]
fn runtime_filter_scan_field_rejects_filter_lineage_drift() {
    let (fragment, mut filter, _) = scan_lineage_filter(true, false, false, false);
    let RuntimeFilterConsumerTarget::ScanField { lineage, .. } = &mut filter.consumers[0].target
    else {
        unreachable!();
    };
    let RuntimeFilterLineageStep::FilterPassThrough { input_ordinal, .. } = &mut lineage[0] else {
        unreachable!();
    };
    *input_ordinal = 1;

    let cuts = local_runtime_filter_cuts(&fragment, &filter);
    let independent = validate_fragment(&fragment, &cuts).unwrap_err();
    assert!(independent.to_string().contains(
        "runtime filter scan consumer is not connected to its exact probe key by a safe lineage"
    ));
}

#[test]
fn runtime_filter_project_identity_uses_the_exact_output_occurrence() {
    let (fragment, mut filter, _) = scan_lineage_filter(false, true, false, false);
    assert_runtime_filter_plan_accepted(fragment.clone(), filter.clone());

    let RuntimeFilterConsumerTarget::ScanField { lineage, .. } = &mut filter.consumers[0].target
    else {
        unreachable!();
    };
    let RuntimeFilterLineageStep::ProjectIdentity { output_ordinal, .. } = &mut lineage[0] else {
        unreachable!();
    };
    *output_ordinal = 2;
    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "runtime filter scan consumer is not connected to its exact probe key by a safe lineage",
    );
}

#[test]
fn runtime_filter_join_equality_lineage_recomputes_inner_key_direction() {
    let (fragment, mut filter, _) = scan_lineage_filter(false, false, true, false);
    assert_runtime_filter_plan_accepted(fragment.clone(), filter.clone());

    let RuntimeFilterConsumerTarget::ScanField { lineage, .. } = &mut filter.consumers[0].target
    else {
        unreachable!();
    };
    let RuntimeFilterLineageStep::JoinEquality { target_side, .. } = &mut lineage[0] else {
        unreachable!();
    };
    *target_side = JoinSide::Right;
    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "runtime filter scan consumer is not connected to its exact probe key by a safe lineage",
    );
}

#[test]
fn runtime_filter_scan_field_rejects_a_same_typed_non_key_provider_field() {
    let (fragment, mut filter, second) = scan_lineage_filter(false, false, false, true);
    filter.consumers[0].endpoint.values = Box::from([second.unwrap()]);

    let cuts = local_runtime_filter_cuts(&fragment, &filter);
    let independent = validate_fragment(&fragment, &cuts).unwrap_err();
    assert!(independent.to_string().contains(
        "runtime filter scan consumer is not connected to its exact probe key by a safe lineage"
    ));
    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment).unwrap();
    builder.add_runtime_filter(filter).unwrap();
    let error = builder.finish().unwrap_err();
    assert!(error.to_string().contains(
        "runtime filter scan consumer is not connected to its exact probe key by a safe lineage"
    ));
}

#[test]
fn null_safe_join_membership_filter_preserves_null_matches_at_both_boundaries() {
    for null_semantics in [
        RuntimeFilterNullSemantics::NeverMatches,
        RuntimeFilterNullSemantics::NullSafeEqual,
    ] {
        let (fragment, filter, _) = null_safe_join_filter(null_semantics);
        let cuts = local_runtime_filter_cuts(&fragment, &filter);
        let standalone_result = validate_fragment(&fragment, &cuts);
        let mut builder = PlanBuilder::new(version());
        builder.add_fragment(fragment).unwrap();
        builder.add_runtime_filter(filter).unwrap();
        let plan_result = builder.finish();
        if null_semantics == RuntimeFilterNullSemantics::NeverMatches {
            for error in [standalone_result.unwrap_err(), plan_result.unwrap_err()] {
                assert!(error.to_string().contains(
                    "runtime filter equality witness does not prove a safe hash-join key direction"
                ));
            }
        } else {
            standalone_result.unwrap();
            let plan = plan_result.unwrap();
            let fragment_id = FragmentId::new(100);
            validate_fragment(
                &plan.fragments()[&fragment_id],
                &fragment_cuts(&plan, fragment_id).unwrap(),
            )
            .unwrap();
        }
    }
}

#[test]
fn runtime_filter_consumer_cannot_substitute_a_same_typed_non_key_value() {
    let (fragment, mut filter, non_key) =
        null_safe_join_filter(RuntimeFilterNullSemantics::NullSafeEqual);
    filter.consumers[0].endpoint.values = Box::from([non_key]);

    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "runtime filter consumer target is not locally valid for its equality witness",
    );
}

#[test]
fn runtime_filter_consumer_requires_a_producer_for_its_exact_equality_witness() {
    let (fragment, mut filter, _) =
        null_safe_join_filter(RuntimeFilterNullSemantics::NullSafeEqual);
    let mut second = filter.equality_witnesses[0];
    second.id = RuntimeFilterEqualityWitnessId::new(10_001);
    filter.equality_witnesses = Box::from([filter.equality_witnesses[0], second]);
    match &mut filter.consumers[0].target {
        RuntimeFilterConsumerTarget::JoinProbeKey { equality }
        | RuntimeFilterConsumerTarget::ScanField { equality, .. } => *equality = second.id,
        RuntimeFilterConsumerTarget::AggregateTopNScanField { .. } => {
            panic!("null-safe join fixture must use a join equality target")
        }
    }

    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "runtime filter consumer equality witness has no producer for the same join key",
    );
}

fn assert_local_runtime_filter_rejected_at_both_boundaries(
    fragment: Fragment,
    filter: RuntimeFilter,
    expected: &str,
) {
    let cuts = local_runtime_filter_cuts(&fragment, &filter);
    let independent = validate_fragment(&fragment, &cuts).unwrap_err();

    let mut builder = PlanBuilder::new(version());
    builder.add_fragment(fragment).unwrap();
    builder.add_runtime_filter(filter).unwrap();
    let whole_plan = builder.finish().unwrap_err();

    for error in [independent, whole_plan] {
        assert!(
            error.to_string().contains(expected),
            "expected '{expected}' in {error}"
        );
    }
}

#[test]
fn complete_once_coverage_requires_the_same_boolean_shape_not_only_the_same_leaves() {
    let (fragment, mut filter, _) =
        null_safe_join_filter(RuntimeFilterNullSemantics::NullSafeEqual);
    let witness = filter.producers[0].witness;
    filter.terminal_coverage = coverage([witness], false);

    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "complete-once runtime filter has different availability and terminal coverage",
    );
}

#[test]
fn runtime_filter_coverage_depth_is_bounded_without_recursive_values() {
    let (fragment, mut filter, _) =
        null_safe_join_filter(RuntimeFilterNullSemantics::NullSafeEqual);
    let witness = filter.producers[0].witness;
    let mut nodes = vec![RuntimeFilterCoverageNode::Witness(witness)];
    for child in 0..PlanLimits::FROZEN.runtime_filter_coverage_depth {
        nodes.push(RuntimeFilterCoverageNode::AllOf {
            children: Box::from([u32::try_from(child).unwrap()]),
        });
    }
    let coverage = RuntimeFilterCoverage {
        root: u32::try_from(nodes.len() - 1).unwrap(),
        nodes: nodes.into_boxed_slice(),
    };
    filter.availability_coverage = coverage.clone();
    filter.terminal_coverage = coverage;

    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "runtime filter availability coverage exceeds semantic depth",
    );
}

#[test]
fn membership_filter_rejects_each_contribution_and_completion_matrix_drift() {
    let (fragment, filter, _) = null_safe_join_filter(RuntimeFilterNullSemantics::NullSafeEqual);

    let mut wrong_coverage = filter.clone();
    let witness = wrong_coverage.producers[0].witness;
    wrong_coverage.availability_coverage = coverage([witness], false);
    wrong_coverage.terminal_coverage = coverage([witness], false);
    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment.clone(),
        wrong_coverage,
        "fenced final-domain runtime filter requires null-safe AllOf coverage",
    );

    let mut wrong_contributions = filter.clone();
    wrong_contributions.producers[0].contribution_kinds = Box::from([
        RuntimeFilterContributionKind::ValueDomainDelta,
        RuntimeFilterContributionKind::FinalDomainShard,
        RuntimeFilterContributionKind::ProducerClosed,
    ]);
    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment.clone(),
        wrong_contributions,
        "runtime filter producer contributions differ from the channel matrix",
    );

    let mut wrong_completion = filter;
    wrong_completion.producers[0].completion = RuntimeFilterCompletion::ProducerClosed;
    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        wrong_completion,
        "runtime filter producer completion differs from the channel matrix",
    );
}

fn ordered_join_filter() -> (Fragment, RuntimeFilter) {
    let fragment_id = FragmentId::new(104);
    let mut builder = FragmentBuilder::new(fragment_id);
    let scheme = hash_scheme(104);
    let (probe, probe_value) = append_hash_exchange_source(
        &mut builder,
        EdgeId::new(1040),
        ValueId::new(1040),
        scheme.clone(),
    );
    let (aggregate_input, aggregate_value) = append_hash_exchange_source(
        &mut builder,
        EdgeId::new(1041),
        ValueId::new(1041),
        scheme.clone(),
    );
    let aggregate = builder.reserve_node_id().unwrap();
    let group_key = builder
        .add_expression(
            aggregate,
            ty(DataType::Int64, false),
            ExprKind::Value(aggregate_value),
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: aggregate,
            inputs: Box::from([aggregate_input]),
            required_inputs: Box::from([PhysicalProperties {
                distribution: Distribution::Hash {
                    keys: Box::from([aggregate_value]),
                    scheme: scheme.clone(),
                },
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            }]),
            output_properties: PhysicalProperties {
                distribution: Distribution::Hash {
                    keys: Box::from([aggregate_value]),
                    scheme: scheme.clone(),
                },
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: aggregate,
                columns: Box::from([aggregate_value]),
            },
            kind: NodeKind::Aggregate {
                group_by: Box::from([(group_key, aggregate_value)]),
                calls: Box::default(),
                grouping: crate::AggregateGrouping::Complete,
            },
        })
        .unwrap();
    let hash = |value| PhysicalProperties {
        distribution: Distribution::Hash {
            keys: Box::from([value]),
            scheme: scheme.clone(),
        },
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let join = append_partitioned_inner_join(
        &mut builder,
        (probe, probe_value),
        (aggregate, aggregate_value),
        hash(probe_value),
        hash(aggregate_value),
    );
    let filter_id = RuntimeFilterId::new(104);
    builder.attach_runtime_filter(filter_id).unwrap();
    let fragment = builder
        .finish_definition(join, FragmentSink::Noop, dop())
        .unwrap();
    let witness = RuntimeFilterWitnessId::new(104);
    let equality = RuntimeFilterEqualityWitnessId::new(104);
    let filter = RuntimeFilter {
        id: filter_id,
        kind: RuntimeFilterKind::MinMax,
        domain: RuntimeFilterDomain::Ordered {
            key: RuntimeFilterOrderKey {
                ty: ty(DataType::Int64, false),
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            },
            inclusive: true,
            comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
        },
        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
        reduction: RuntimeFilterReduction::UnionOrderedHull,
        availability_coverage: coverage([witness], true),
        terminal_coverage: coverage([witness], true),
        equality_witnesses: Box::from([RuntimeFilterEqualityWitness {
            id: equality,
            fragment: fragment_id,
            join,
            key_ordinal: 0,
            domain_side: JoinSide::Right,
        }]),
        producers: Box::from([RuntimeFilterProducer {
            witness,
            endpoint: RuntimeFilterEndpoint {
                fragment: fragment_id,
                node: join,
                values: Box::from([aggregate_value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 },
            contribution_kinds: Box::from([
                RuntimeFilterContributionKind::FinalOrderedHullShard,
                RuntimeFilterContributionKind::ProducerClosed,
            ]),
            completion: RuntimeFilterCompletion::FencedCommittedDomain,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::default(),
                non_build_edges: Box::default(),
            },
            target: RuntimeFilterProducerTarget::JoinBuildKey { equality },
        }]),
        consumers: Box::from([RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: fragment_id,
                node: join,
                values: Box::from([probe_value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
            capabilities: Box::from([
                RuntimeFilterArtifactCapability::OrderedRange,
                RuntimeFilterArtifactCapability::EmptyDomain,
            ]),
            activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: RuntimeFilterConsumerTarget::JoinProbeKey { equality },
        }]),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 100,
            max_retries: 1,
        },
    };
    (fragment, filter)
}

fn append_hash_exchange_source(
    builder: &mut FragmentBuilder,
    edge: EdgeId,
    source_value: ValueId,
    scheme: HashPartitionScheme,
) -> (NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Hash {
                    keys: Box::from([value]),
                    scheme,
                },
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, value)]),
            },
        })
        .unwrap();
    (node, value)
}

fn append_partitioned_inner_join(
    builder: &mut FragmentBuilder,
    left: (NodeId, ValueId),
    right: (NodeId, ValueId),
    left_properties: PhysicalProperties,
    right_properties: PhysicalProperties,
) -> NodeId {
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let left_key = builder
        .add_expression(node, value_type.clone(), ExprKind::Value(left.1))
        .unwrap();
    let right_key = builder
        .add_expression(node, value_type, ExprKind::Value(right.1))
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([left.0, right.0]),
            required_inputs: Box::from([left_properties.clone(), right_properties]),
            output_properties: left_properties,
            output: OutputPort {
                node,
                columns: Box::from([left.1, right.1]),
            },
            kind: NodeKind::HashJoin {
                kind: JoinKind::Inner,
                build_side: JoinSide::Right,
                keys: Box::from([JoinKey {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }]),
                distribution: JoinDistribution::Partitioned,
                residual: None,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    node
}

#[test]
fn runtime_filter_producer_target_must_match_its_reduction() {
    let (fragment, mut filter) = ordered_join_filter();
    filter.producers[0].apply_point = RuntimeFilterApplyPoint::NodeOutput;

    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "runtime filter producer target does not match its equality witness and exact build key",
    );
}

#[test]
fn ordered_runtime_filter_rejects_an_unfrozen_comparison_domain() {
    let (fragment, mut filter) = ordered_join_filter();
    let RuntimeFilterDomain::Ordered { key, .. } = &mut filter.domain else {
        panic!("ordered fixture must have an ordered domain");
    };
    key.ty = ty(DataType::Float64, false);

    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "ordered runtime-filter key type is unsupported by its comparison algorithm",
    );
}

#[test]
fn runtime_filter_safety_must_match_the_exact_consumer_join_semantics() {
    let (fragment, mut filter) = ordered_join_filter();
    filter.consumers[0].target = RuntimeFilterConsumerTarget::JoinProbeKey {
        equality: RuntimeFilterEqualityWitnessId::new(999),
    };

    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "runtime filter equality witness references differ from their definitions",
    );
}

#[test]
fn runtime_filter_late_apply_granularity_must_match_the_consumer_target() {
    let (fragment, mut filter, _) =
        null_safe_join_filter(RuntimeFilterNullSemantics::NullSafeEqual);
    filter.consumers[0].activation =
        RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
            late_apply: LateApplyGranularity::File,
        };

    assert_local_runtime_filter_rejected_at_both_boundaries(
        fragment,
        filter,
        "runtime filter late-apply granularity is unsupported at its consumer target",
    );
}

#[derive(Clone)]
struct JoinBuildFrontierFixture {
    fragments: Vec<Fragment>,
    edges: Vec<Edge>,
    filter: RuntimeFilter,
    target_fragment: FragmentId,
    nested_build_edges: [EdgeId; 1],
    probe_edge: EdgeId,
    outer_sibling_edge: EdgeId,
}

fn append_exchange_source(
    builder: &mut FragmentBuilder,
    edge: EdgeId,
    source_value: ValueId,
    properties: PhysicalProperties,
) -> (NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let value = builder
        .add_value(
            value_type,
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, value)]),
            },
        })
        .unwrap();
    (node, value)
}

fn append_inner_join_with_properties(
    builder: &mut FragmentBuilder,
    left: (NodeId, ValueId),
    right: (NodeId, ValueId),
    left_properties: PhysicalProperties,
    right_properties: PhysicalProperties,
    output_properties: PhysicalProperties,
) -> NodeId {
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let left_key = builder
        .add_expression(node, value_type.clone(), ExprKind::Value(left.1))
        .unwrap();
    let right_key = builder
        .add_expression(node, value_type, ExprKind::Value(right.1))
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([left.0, right.0]),
            required_inputs: Box::from([left_properties, right_properties]),
            output_properties,
            output: OutputPort {
                node,
                columns: Box::from([left.1, right.1]),
            },
            kind: NodeKind::HashJoin {
                kind: JoinKind::Inner,
                build_side: JoinSide::Right,
                keys: Box::from([JoinKey {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }]),
                distribution: JoinDistribution::BroadcastBuild,
                residual: None,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    node
}

fn append_broadcast_inner_join(
    builder: &mut FragmentBuilder,
    left: (NodeId, ValueId),
    right: (NodeId, ValueId),
) -> NodeId {
    let single_copy = PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let broadcast = PhysicalProperties {
        distribution: Distribution::Broadcast,
        row_multiplicity: RowMultiplicity::Replicated,
        ordering: Box::default(),
    };
    append_inner_join_with_properties(
        builder,
        left,
        right,
        single_copy.clone(),
        broadcast,
        single_copy,
    )
}

fn join_build_frontier_fixture() -> JoinBuildFrontierFixture {
    let target_fragment = FragmentId::new(210);
    let probe_edge = EdgeId::new(210);
    let nested_probe_edge = EdgeId::new(211);
    let nested_leaf_build_edge = EdgeId::new(212);
    let outer_sibling_edge = EdgeId::new(213);
    let nested_build_edges = [EdgeId::new(214)];
    let edge_ids = [
        probe_edge,
        nested_probe_edge,
        nested_leaf_build_edge,
        outer_sibling_edge,
    ];

    let mut fragments = Vec::new();
    let mut source_values = Vec::new();
    for (ordinal, edge) in edge_ids.into_iter().enumerate() {
        let (fragment, value) = literal_fragment(
            FragmentId::new(220 + u32::try_from(ordinal).unwrap()),
            FragmentSink::Stream { edge },
            false,
        );
        fragments.push(fragment);
        source_values.push(value);
    }

    let single_copy = PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let broadcast = PhysicalProperties {
        distribution: Distribution::Broadcast,
        row_multiplicity: RowMultiplicity::Replicated,
        ordering: Box::default(),
    };

    let nested_fragment_id = FragmentId::new(224);
    let mut nested = FragmentBuilder::new(nested_fragment_id);
    let nested_probe = append_exchange_source(
        &mut nested,
        nested_probe_edge,
        source_values[1],
        single_copy.clone(),
    );
    let nested_leaf_build = append_exchange_source(
        &mut nested,
        nested_leaf_build_edge,
        source_values[2],
        broadcast.clone(),
    );
    let nested_root = append_broadcast_inner_join(&mut nested, nested_probe, nested_leaf_build);
    let nested = nested
        .finish_definition(
            nested_root,
            FragmentSink::Stream {
                edge: nested_build_edges[0],
            },
            dop(),
        )
        .unwrap();
    let nested_output = nested.nodes()[&nested_root].output.columns[1];

    let mut target = FragmentBuilder::new(target_fragment);
    let probe = append_exchange_source(
        &mut target,
        probe_edge,
        source_values[0],
        single_copy.clone(),
    );
    let nested_build = append_exchange_source(
        &mut target,
        nested_build_edges[0],
        nested_output,
        broadcast.clone(),
    );
    let outer_sibling =
        append_exchange_source(&mut target, outer_sibling_edge, source_values[3], broadcast);
    let producer_join = append_broadcast_inner_join(&mut target, probe, nested_build);
    let root = append_broadcast_inner_join(&mut target, (producer_join, probe.1), outer_sibling);

    let filter_id = RuntimeFilterId::new(210);
    target.attach_runtime_filter(filter_id).unwrap();
    let target = target
        .finish_definition(root, FragmentSink::Noop, dop())
        .unwrap();

    let witness = RuntimeFilterWitnessId::new(210);
    let equality = RuntimeFilterEqualityWitnessId::new(210);
    let filter = RuntimeFilter {
        id: filter_id,
        kind: RuntimeFilterKind::InList,
        domain: RuntimeFilterDomain::Membership {
            ty: ty(DataType::Int64, false),
            null_semantics: RuntimeFilterNullSemantics::NeverMatches,
        },
        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
        reduction: RuntimeFilterReduction::SetUnion,
        availability_coverage: coverage([witness], false),
        terminal_coverage: coverage([witness], false),
        equality_witnesses: Box::from([RuntimeFilterEqualityWitness {
            id: equality,
            fragment: target_fragment,
            join: producer_join,
            key_ordinal: 0,
            domain_side: JoinSide::Right,
        }]),
        producers: Box::from([RuntimeFilterProducer {
            witness,
            endpoint: RuntimeFilterEndpoint {
                fragment: target_fragment,
                node: producer_join,
                values: Box::from([nested_build.1]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 },
            contribution_kinds: Box::from([
                RuntimeFilterContributionKind::ValueDomainDelta,
                RuntimeFilterContributionKind::ProducerClosed,
            ]),
            completion: RuntimeFilterCompletion::ProducerClosed,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::from(nested_build_edges),
                non_build_edges: Box::from([probe_edge, outer_sibling_edge]),
            },
            target: RuntimeFilterProducerTarget::JoinBuildKey { equality },
        }]),
        consumers: Box::from([RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: target_fragment,
                node: producer_join,
                values: Box::from([probe.1]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
            capabilities: Box::from([
                RuntimeFilterArtifactCapability::Membership,
                RuntimeFilterArtifactCapability::EmptyDomain,
            ]),
            activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: RuntimeFilterConsumerTarget::JoinProbeKey { equality },
        }]),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 100,
            max_retries: 1,
        },
    };

    let target_nodes = target.nodes();
    let nested_nodes = nested.nodes();
    let mut edges = Vec::new();
    for (ordinal, edge) in edge_ids.into_iter().enumerate() {
        let source_fragment = fragments[ordinal].id();
        let destination_fragment = if edge == nested_probe_edge || edge == nested_leaf_build_edge {
            nested_fragment_id
        } else {
            target_fragment
        };
        let destination_node = if destination_fragment == nested_fragment_id {
            nested_nodes
        } else {
            target_nodes
        }
            .values()
            .find(|node| matches!(node.kind, NodeKind::ExchangeSource { edge: found, .. } if found == edge))
            .unwrap();
        let destination_value = destination_node.output.columns[0];
        let is_broadcast = edge == nested_leaf_build_edge || edge == outer_sibling_edge;
        edges.push(Edge {
            id: edge,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                fragment: source_fragment,
                projection: Box::from([source_values[ordinal]]),
            },
            destination: EdgeDestination {
                fragment: destination_fragment,
                node: destination_node.id,
                receive_mapping: Box::from([(source_values[ordinal], destination_value)]),
            },
            partitioning: EdgePartitioning {
                source: if is_broadcast {
                    Distribution::Broadcast
                } else {
                    Distribution::Singleton
                },
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: if is_broadcast {
                    Distribution::Broadcast
                } else {
                    Distribution::Singleton
                },
                destination_multiplicity: if is_broadcast {
                    RowMultiplicity::Replicated
                } else {
                    RowMultiplicity::SingleCopy
                },
            },
        });
    }
    let nested_destination = target_nodes
        .values()
        .find(|node| matches!(node.kind, NodeKind::ExchangeSource { edge, .. } if edge == nested_build_edges[0]))
        .unwrap();
    edges.push(Edge {
        id: nested_build_edges[0],
        kind: EdgeKind::Stream,
        source: EdgeSource {
            fragment: nested_fragment_id,
            projection: Box::from([nested_output]),
        },
        destination: EdgeDestination {
            fragment: target_fragment,
            node: nested_destination.id,
            receive_mapping: Box::from([(nested_output, nested_destination.output.columns[0])]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Broadcast,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Broadcast,
            destination_multiplicity: RowMultiplicity::Replicated,
        },
    });
    fragments.push(nested);
    fragments.push(target);

    JoinBuildFrontierFixture {
        fragments,
        edges,
        filter,
        target_fragment,
        nested_build_edges,
        probe_edge,
        outer_sibling_edge,
    }
}

fn build_join_build_frontier_plan(
    fixture: &JoinBuildFrontierFixture,
    filter: RuntimeFilter,
) -> Result<PhysicalPlan, ValidationErrors> {
    let mut builder = PlanBuilder::new(version());
    for fragment in &fixture.fragments {
        builder.add_fragment(fragment.clone()).unwrap();
    }
    for edge in &fixture.edges {
        builder.add_edge(edge.clone()).unwrap();
    }
    builder.add_runtime_filter(filter).unwrap();
    builder.finish()
}

fn assert_frontier_rejected_at_both_boundaries(
    fixture: &JoinBuildFrontierFixture,
    filter: RuntimeFilter,
) {
    let valid_plan = build_join_build_frontier_plan(fixture, fixture.filter.clone()).unwrap();
    let fragment = &valid_plan.fragments()[&fixture.target_fragment];
    let mut cuts = fragment_cuts(&valid_plan, fixture.target_fragment).unwrap();
    cuts.runtime_filters = Box::from([filter.clone()]);
    let independent = validate_fragment(fragment, &cuts).unwrap_err();
    let whole_plan = build_join_build_frontier_plan(fixture, filter).unwrap_err();

    for error in [independent, whole_plan] {
        assert!(error.to_string().contains(
            "runtime filter join-build frontier differs from the exact join input exchange cuts"
        ));
    }
}

#[test]
fn join_build_frontier_cannot_omit_a_nested_build_subtree_edge() {
    let fixture = join_build_frontier_fixture();
    let mut filter = fixture.filter.clone();
    filter.producers[0].progress = RuntimeFilterProducerProgress {
        build_edges: Box::default(),
        non_build_edges: Box::from([fixture.probe_edge, fixture.outer_sibling_edge]),
    };

    assert_frontier_rejected_at_both_boundaries(&fixture, filter);
}

#[test]
fn join_build_frontier_non_build_partition_includes_outer_sibling_inbound_edges() {
    let fixture = join_build_frontier_fixture();
    let mut filter = fixture.filter.clone();
    filter.producers[0].progress = RuntimeFilterProducerProgress {
        build_edges: Box::from(fixture.nested_build_edges),
        non_build_edges: Box::from([fixture.probe_edge]),
    };

    assert_frontier_rejected_at_both_boundaries(&fixture, filter);
}

#[derive(Clone, Copy)]
enum TableFunctionFixture {
    Valid,
    PassThroughAsResult,
    NonNullableOuterResult,
}

fn table_function_fragment(
    left_outer: bool,
    fixture: TableFunctionFixture,
) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(101));
    let (input, outer_value) = append_literal(&mut builder, false);
    let node = builder.reserve_node_id().unwrap();
    let argument = builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::Value(outer_value),
        )
        .unwrap();
    let nullable = left_outer && !matches!(fixture, TableFunctionFixture::NonNullableOuterResult);
    let text_result = builder
        .add_value(
            ty(DataType::Utf8, nullable),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let number_result = if matches!(fixture, TableFunctionFixture::PassThroughAsResult) {
        outer_value
    } else {
        builder
            .add_value(
                ty(DataType::Int64, nullable),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 2,
                },
            )
            .unwrap()
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: Box::from([text_result, outer_value, number_result]),
            },
            kind: NodeKind::TableFunction {
                function: BoundTableFunction {
                    function_id: FunctionId::try_new("builtin/test_relation/v1").unwrap(),
                    overload: FunctionOverloadId::try_new("i64-to-i64-utf8").unwrap(),
                    argument_types: Box::from([FunctionArgumentType::Value(ty(
                        DataType::Int64,
                        false,
                    ))]),
                    result_types: Box::from([
                        ty(DataType::Int64, false),
                        ty(DataType::Utf8, false),
                    ]),
                    volatility: FunctionVolatility::Immutable,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error:
                        novarocks_type_contract::FunctionIntrinsicRowError::NoRowError,
                },
                arguments: Box::from([argument]),
                outputs: Box::from([
                    TableFunctionOutput::FunctionResult {
                        result_ordinal: 1,
                        value: text_result,
                    },
                    TableFunctionOutput::PassThrough(outer_value),
                    TableFunctionOutput::FunctionResult {
                        result_ordinal: 0,
                        value: number_result,
                    },
                ]),
                left_outer,
            },
        })
        .unwrap();
    builder.finish_definition(node, FragmentSink::Noop, dop())
}

#[test]
fn table_function_relation_results_and_outer_occurrences_have_separate_mappings() {
    for left_outer in [false, true] {
        let fragment = table_function_fragment(left_outer, TableFunctionFixture::Valid).unwrap();
        let root = &fragment.nodes()[&fragment.root()];
        let columns = &root.output.columns;
        assert_eq!(
            fragment.values()[&columns[0]].ty,
            ty(DataType::Utf8, left_outer)
        );
        assert_eq!(
            fragment.values()[&columns[1]].ty,
            ty(DataType::Int64, false)
        );
        assert_eq!(
            fragment.values()[&columns[2]].ty,
            ty(DataType::Int64, left_outer)
        );
        let mut builder = PlanBuilder::new(version());
        builder.add_fragment(fragment).unwrap();
        let plan = builder.finish().unwrap();
        let fragment_id = FragmentId::new(101);
        validate_fragment(
            &plan.fragments()[&fragment_id],
            &fragment_cuts(&plan, fragment_id).unwrap(),
        )
        .unwrap();
    }
}

#[test]
fn table_function_cannot_claim_outer_input_as_a_function_result() {
    let error = table_function_fragment(false, TableFunctionFixture::PassThroughAsResult)
        .unwrap_err()
        .to_string();
    assert!(error.contains("table function result value is not owned by its output occurrence"));
}

#[test]
fn left_outer_table_function_requires_nullable_function_results() {
    let error = table_function_fragment(true, TableFunctionFixture::NonNullableOuterResult)
        .unwrap_err()
        .to_string();
    assert!(error.contains("table function result type differs from its bound schema"));
}

#[test]
fn table_function_binding_cannot_be_published_as_a_scalar_call() {
    let mut builder = FragmentBuilder::new(FragmentId::new(102));
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let expression = builder
        .add_expression(
            node,
            value_type.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction {
                    function_id: FunctionId::try_new("builtin/test_relation/v1").unwrap(),
                    overload: FunctionOverloadId::try_new("disguised-scalar").unwrap(),
                    kind: FunctionKind::Table,
                    argument_types: Box::default(),
                    result_type: value_type.clone(),
                    volatility: FunctionVolatility::Immutable,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error:
                        novarocks_type_contract::FunctionIntrinsicRowError::NoRowError,
                },
                args: Box::default(),
            },
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
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
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
    assert!(error.contains("scalar call has non-scalar binding"));
}

#[derive(Clone, Copy)]
enum HigherOrderFixture {
    Valid,
    ScalarBinding,
    ParameterTypeDrift,
    ReusedAsExpressionChild,
    ReusedAsValueArgument,
    LambdaOperatorRoot,
}

fn higher_order_function_fragment(
    fixture: HigherOrderFixture,
) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(104));
    let (input, _) = append_literal(&mut builder, false);
    let node = builder.reserve_node_id().unwrap();
    let lambda = builder.reserve_expression_id().unwrap();
    let parameter_type = ty(DataType::Int64, false);
    let parameter = builder
        .add_expression_in_scope(
            node,
            Some(lambda),
            parameter_type.clone(),
            ExprKind::LambdaParameter { lambda, ordinal: 0 },
        )
        .unwrap();
    builder
        .insert_expression(ExprNode {
            id: lambda,
            owner: node,
            lambda_scope: None,
            ty: parameter_type.clone(),
            kind: ExprKind::Lambda {
                parameter_types: Box::from([parameter_type.clone()]),
                body: parameter,
            },
        })
        .unwrap();
    let expected_argument = match fixture {
        HigherOrderFixture::Valid
        | HigherOrderFixture::ReusedAsExpressionChild
        | HigherOrderFixture::ReusedAsValueArgument
        | HigherOrderFixture::LambdaOperatorRoot => FunctionArgumentType::Lambda {
            parameter_types: Box::from([parameter_type.clone()]),
            result_type: parameter_type.clone(),
        },
        HigherOrderFixture::ScalarBinding => FunctionArgumentType::Value(parameter_type.clone()),
        HigherOrderFixture::ParameterTypeDrift => FunctionArgumentType::Lambda {
            parameter_types: Box::from([ty(DataType::Utf8, false)]),
            result_type: parameter_type.clone(),
        },
    };
    let (argument_types, arguments): (Box<[FunctionArgumentType]>, Box<[ExprId]>) =
        if matches!(fixture, HigherOrderFixture::ReusedAsValueArgument) {
            (
                Box::from([
                    expected_argument,
                    FunctionArgumentType::Value(parameter_type.clone()),
                ]),
                Box::from([lambda, lambda]),
            )
        } else {
            (Box::from([expected_argument]), Box::from([lambda]))
        };
    let call = builder
        .add_expression(
            node,
            parameter_type.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction {
                    function_id: FunctionId::try_new("builtin/test_higher_order/v1").unwrap(),
                    overload: FunctionOverloadId::try_new("lambda-i64-to-i64").unwrap(),
                    kind: FunctionKind::Scalar,
                    argument_types,
                    result_type: parameter_type.clone(),
                    volatility: FunctionVolatility::Immutable,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error:
                        novarocks_type_contract::FunctionIntrinsicRowError::NoRowError,
                },
                args: arguments,
            },
        )
        .unwrap();
    let result = if matches!(fixture, HigherOrderFixture::ReusedAsExpressionChild) {
        builder
            .add_expression(
                node,
                parameter_type.clone(),
                ExprKind::Binary {
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    left: call,
                    op: BinaryOperator::Add,
                    right: lambda,
                },
            )
            .unwrap()
    } else {
        call
    };
    let output = builder
        .add_value(
            parameter_type.clone(),
            ValueOrigin::Expr { node, expr: result },
        )
        .unwrap();
    let mut outputs = vec![output];
    let mut projections = vec![(result, output)];
    if matches!(fixture, HigherOrderFixture::LambdaOperatorRoot) {
        let lambda_output = builder
            .add_value(parameter_type, ValueOrigin::Expr { node, expr: lambda })
            .unwrap();
        outputs.push(lambda_output);
        projections.push((lambda, lambda_output));
    }
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: outputs.into_boxed_slice(),
            },
            kind: NodeKind::Project {
                expressions: projections.into_boxed_slice(),
            },
        })
        .unwrap();
    builder.finish_definition(node, FragmentSink::Noop, dop())
}

#[test]
fn higher_order_function_binding_matches_the_exact_lambda_shape() {
    higher_order_function_fragment(HigherOrderFixture::Valid).unwrap();
    for fixture in [
        HigherOrderFixture::ScalarBinding,
        HigherOrderFixture::ParameterTypeDrift,
    ] {
        let error = higher_order_function_fragment(fixture)
            .unwrap_err()
            .to_string();
        assert!(error.contains("argument 0 shape differs from its bound signature"));
    }
}

#[test]
fn lambda_cannot_be_reused_outside_its_exact_bound_argument_positions() {
    for fixture in [
        HigherOrderFixture::ReusedAsExpressionChild,
        HigherOrderFixture::ReusedAsValueArgument,
        HigherOrderFixture::LambdaOperatorRoot,
    ] {
        let error = higher_order_function_fragment(fixture)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("lambda must appear only as an exact bound-function lambda argument")
        );
    }
}

#[derive(Clone, Copy)]
enum TableWriterFixture {
    InputPort,
    TargetField,
    Distribution,
    SchemaRevision,
    SchemaRole,
}

fn invalid_table_writer_fragment(
    fixture: TableWriterFixture,
) -> Result<Fragment, ValidationErrors> {
    use novarocks_connector_contract::{ConnectorCodecCategory, ConnectorWriteFieldToken};

    let mut builder = FragmentBuilder::new(FragmentId::new(105));
    let (input, input_value) = append_literal(&mut builder, false);
    let writer = builder.reserve_node_id().unwrap();
    let output_specs = [
        (
            "kind",
            DataType::Int8,
            false,
            WriterRelationFieldRole::Kind,
            WriterDerivedKind::RelationKind,
        ),
        (
            "write_target_ordinal",
            DataType::Int32,
            false,
            WriterRelationFieldRole::TargetOrdinal,
            WriterDerivedKind::WriteTargetOrdinal,
        ),
        (
            "row_count",
            DataType::Int64,
            true,
            WriterRelationFieldRole::RowCount,
            WriterDerivedKind::AffectedRows,
        ),
        (
            "commit_fragment",
            DataType::Binary,
            true,
            WriterRelationFieldRole::CommitFragment,
            WriterDerivedKind::CommitFragment,
        ),
    ];
    let mut output_fields = output_specs
        .into_iter()
        .map(|(name, data_type, nullable, role, kind)| {
            let value_type = ty(data_type, nullable);
            let value = builder
                .add_value(
                    value_type.clone(),
                    ValueOrigin::WriterDerived {
                        writer_node: writer,
                        kind,
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: name.into(),
                ty: value_type,
                role,
            }
        })
        .collect::<Vec<_>>();
    if matches!(fixture, TableWriterFixture::SchemaRole) {
        output_fields[1].role = WriterRelationFieldRole::Auxiliary;
    }
    let output_value = output_fields[2].value;
    let binding = connector_binding();
    let writer_distribution = if matches!(fixture, TableWriterFixture::Distribution) {
        Distribution::Singleton
    } else {
        Distribution::Unconstrained
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            id: writer,
            inputs: Box::from([input]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: unconstrained(),
            output: OutputPort {
                node: writer,
                columns: output_fields.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableWriter {
                target: WriterTarget {
                    handle: encoded(&binding, ConnectorCodecCategory::WriteHandle, 7),
                    write_target_ordinal: write_target_ordinal(0),
                    input: Box::from([if matches!(fixture, TableWriterFixture::InputPort) {
                        output_value
                    } else {
                        input_value
                    }]),
                    required_distribution: writer_distribution,
                    target_fields: Box::from([WriterTargetField {
                        token: ConnectorWriteFieldToken::from_bytes([7; 32]),
                        input: if matches!(fixture, TableWriterFixture::TargetField) {
                            output_value
                        } else {
                            input_value
                        },
                        ty: ty(DataType::Int64, false),
                        hidden: false,
                    }]),
                    output_schema: WriterRelationSchema {
                        revision: if matches!(fixture, TableWriterFixture::SchemaRevision) {
                            WRITER_MULTIPLEX_SCHEMA_REVISION + 1
                        } else {
                            WRITER_MULTIPLEX_SCHEMA_REVISION
                        },
                        fields: output_fields.into_boxed_slice(),
                    },
                    partial_aggregates: Box::default(),
                },
            },
        })
        .unwrap();
    builder.finish_definition(writer, FragmentSink::Noop, dop())
}

#[test]
fn table_writer_input_must_equal_its_exact_child_port() {
    let error = invalid_table_writer_fragment(TableWriterFixture::InputPort)
        .unwrap_err()
        .to_string();
    assert!(error.contains("table writer input differs from its exact child output"));
}

#[test]
fn table_writer_target_fields_must_come_from_its_exact_child_port() {
    let error = invalid_table_writer_fragment(TableWriterFixture::TargetField)
        .unwrap_err()
        .to_string();
    assert!(error.contains("table writer target field is absent from its exact child output"));
}

#[test]
fn table_writer_has_one_input_distribution_authority() {
    let error = invalid_table_writer_fragment(TableWriterFixture::Distribution)
        .unwrap_err()
        .to_string();
    assert!(error.contains("table writer has two different input distribution contracts"));
}

#[test]
fn table_writer_rejects_an_unknown_relation_schema_revision() {
    let error = invalid_table_writer_fragment(TableWriterFixture::SchemaRevision)
        .unwrap_err()
        .to_string();
    assert!(error.contains("writer relation schema has an unsupported revision or width"));
}

#[test]
fn table_writer_rejects_a_relabelled_fixed_relation_field() {
    let error = invalid_table_writer_fragment(TableWriterFixture::SchemaRole)
        .unwrap_err()
        .to_string();
    assert!(error.contains("writer relation field differs from its closed role contract"));
}

#[derive(Clone, Copy)]
enum GroupedWriterFixture {
    SharedChannel,
    SingleStatisticsTarget,
    MissingTarget,
    NonFinalInput,
    UnmappedFinalOutput,
    WrongConstantType,
    MissingGroupedUnpivot,
    DuplicateMapping,
    NonLiteralScalar,
    MismatchedValueOutputType,
    OverlappingOutputRoles,
    UncoveredAuxiliaryOutput,
    EmptyMapKey,
    UnsortedMapKeys,
    DuplicateMapKey,
    InputSchemaDrift,
    AggregateInputDrift,
    MultiArgumentBinding,
    GroupingRoleDrift,
    PassthroughOriginDrift,
    OutputLimitExceeded,
    NestedLiteralBudgetExceeded,
}

fn writer_schema_role(ordinal: usize) -> WriterRelationFieldRole {
    match ordinal {
        0 => WriterRelationFieldRole::Kind,
        1 => WriterRelationFieldRole::TargetOrdinal,
        2 => WriterRelationFieldRole::RowCount,
        3 => WriterRelationFieldRole::CommitFragment,
        _ => WriterRelationFieldRole::Auxiliary,
    }
}

fn grouped_writer_fragment(fixture: GroupedWriterFixture) -> Result<Fragment, ValidationErrors> {
    use std::sync::Arc;

    use arrow_schema::{Field, Fields};

    const WRITE_RELATION_COLUMN_COUNT: usize = 4;
    const WRITE_RELATION_TARGET_INDEX: usize = 1;
    const ROOT_WRITE_RESULT_TARGET_INDEX: usize = 1;
    const ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX: usize = 4;
    const ROOT_WRITE_RESULT_BLOB_TYPE_INDEX: usize = 5;
    const ROOT_WRITE_RESULT_BODY_INDEX: usize = 6;
    const ROOT_WRITE_RESULT_PROPERTIES_INDEX: usize = 7;
    const WRITER_MULTIPLEX_SCHEMA_REVISION: u32 = 1;
    const ROOT_WRITE_RESULT_SCHEMA_REVISION: u32 = 1;

    let mut builder = FragmentBuilder::new(FragmentId::new(103));
    let input = builder.reserve_node_id().unwrap();
    let finish = builder.reserve_node_id().unwrap();
    let writer_fields = [
        ("kind", DataType::Int8, false),
        ("write_target_ordinal", DataType::Int32, false),
        ("row_count", DataType::Int64, true),
        ("commit_fragment", DataType::Binary, true),
        ("shared_statistics", DataType::Binary, true),
    ];
    let input_fields = writer_fields
        .into_iter()
        .enumerate()
        .map(|(ordinal, (name, data_type, nullable))| {
            let value_type = ty(data_type, nullable);
            let value = builder
                .add_value(
                    value_type.clone(),
                    ValueOrigin::NodeOutput {
                        node: input,
                        output_ordinal: u32::try_from(ordinal).unwrap(),
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: name.into(),
                ty: value_type,
                role: writer_schema_role(ordinal),
            }
        })
        .collect::<Vec<_>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: input,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: input,
                columns: input_fields.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        })
        .unwrap();

    let input_item = Arc::new(Field::new("item", DataType::Int32, false));
    let property_entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        false,
    ));
    let root_fields = [
        ("kind", DataType::Int8, false),
        ("write_target_ordinal", DataType::Int32, true),
        ("row_count", DataType::Int64, true),
        ("commit_fragment", DataType::Binary, true),
        ("input_fields", DataType::List(input_item), true),
        ("blob_type", DataType::Utf8, true),
        ("body", DataType::Binary, true),
        ("properties", DataType::Map(property_entries, false), true),
    ];
    let output_fields = root_fields
        .into_iter()
        .enumerate()
        .map(|(ordinal, (name, data_type, nullable))| {
            let value_type = ty(data_type, nullable);
            let value = builder
                .add_value(
                    value_type.clone(),
                    ValueOrigin::WriterDerived {
                        writer_node: finish,
                        kind: match writer_schema_role(ordinal) {
                            WriterRelationFieldRole::TargetOrdinal => {
                                if matches!(fixture, GroupedWriterFixture::PassthroughOriginDrift) {
                                    WriterDerivedKind::AffectedRows
                                } else {
                                    WriterDerivedKind::WriteTargetOrdinal
                                }
                            }
                            WriterRelationFieldRole::RowCount => WriterDerivedKind::AffectedRows,
                            WriterRelationFieldRole::CommitFragment => {
                                WriterDerivedKind::CommitFragment
                            }
                            WriterRelationFieldRole::Kind => WriterDerivedKind::RelationKind,
                            WriterRelationFieldRole::Auxiliary => {
                                WriterDerivedKind::RelationAuxiliary
                            }
                        },
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: name.into(),
                ty: value_type,
                role: writer_schema_role(ordinal),
            }
        })
        .collect::<Vec<_>>();
    let grouping_output = builder
        .add_value(
            ty(DataType::Int32, false),
            ValueOrigin::WriterDerived {
                writer_node: finish,
                kind: WriterDerivedKind::GroupingKey,
            },
        )
        .unwrap();
    let shared_final_output = builder
        .add_value(
            ty(DataType::Binary, true),
            ValueOrigin::WriterDerived {
                writer_node: finish,
                kind: WriterDerivedKind::ArtifactReference,
            },
        )
        .unwrap();
    let shared_state_input = input_fields[WRITE_RELATION_COLUMN_COUNT].value;
    let final_aggregate = WriterAggregateCall {
        input: if matches!(fixture, GroupedWriterFixture::AggregateInputDrift) {
            shared_final_output
        } else {
            shared_state_input
        },
        binding: AggregateBinding {
            function: BoundFunction {
                function_id: FunctionId::try_new("builtin/test_statistics/v1").unwrap(),
                overload: FunctionOverloadId::try_new("i64-to-binary").unwrap(),
                kind: FunctionKind::Aggregate,
                argument_types: if matches!(fixture, GroupedWriterFixture::MultiArgumentBinding) {
                    Box::from([
                        FunctionArgumentType::Value(ty(DataType::Int64, true)),
                        FunctionArgumentType::Value(ty(DataType::Int64, true)),
                    ])
                } else {
                    Box::from([FunctionArgumentType::Value(ty(DataType::Int64, true))])
                },
                result_type: ty(DataType::Binary, true),
                volatility: FunctionVolatility::Immutable,
                argument_evaluation: FunctionArgumentEvaluation::Eager,
                failure_behavior: FunctionFailureBehavior::Propagate,
                intrinsic_row_error:
                    novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
            },
            phase: AggregatePhase::Final {
                sequence: AggregateSequenceId::new(1),
            },
            logical_argument_count: if matches!(fixture, GroupedWriterFixture::MultiArgumentBinding)
            {
                2
            } else {
                1
            },
            intermediate_type: ty(DataType::Binary, true),
            state_format: AggregateStateFormatId::try_new("test_statistics/state-v1").unwrap(),
        },
        output: shared_final_output,
    };
    let mut final_aggregates = vec![final_aggregate];
    if matches!(fixture, GroupedWriterFixture::UnmappedFinalOutput) {
        let mut extra = final_aggregates[0].clone();
        extra.output = builder
            .add_value(
                ty(DataType::Binary, true),
                ValueOrigin::WriterDerived {
                    writer_node: finish,
                    kind: WriterDerivedKind::ArtifactReference,
                },
            )
            .unwrap();
        final_aggregates.push(extra);
    }
    let mut mappings = Vec::new();
    for target in 0..if matches!(
        fixture,
        GroupedWriterFixture::MissingTarget | GroupedWriterFixture::SingleStatisticsTarget
    ) {
        1
    } else {
        2
    } {
        let scalar_constant = if matches!(fixture, GroupedWriterFixture::NonLiteralScalar) {
            builder
                .add_expression(
                    finish,
                    output_fields[ROOT_WRITE_RESULT_BLOB_TYPE_INDEX].ty.clone(),
                    ExprKind::Value(output_fields[ROOT_WRITE_RESULT_BLOB_TYPE_INDEX].value),
                )
                .unwrap()
        } else if matches!(fixture, GroupedWriterFixture::MismatchedValueOutputType) {
            builder
                .add_expression(
                    finish,
                    ty(DataType::Binary, false),
                    ExprKind::Literal(LiteralValue::Binary(
                        format!("target-{target}-statistics").into_bytes().into(),
                    )),
                )
                .unwrap()
        } else {
            builder
                .add_expression(
                    finish,
                    ty(DataType::Utf8, false),
                    ExprKind::Literal(LiteralValue::Utf8(
                        format!("target-{target}-statistics").into(),
                    )),
                )
                .unwrap()
        };
        let properties = match fixture {
            GroupedWriterFixture::EmptyMapKey if target == 0 => {
                Box::from([("".into(), "value".into())])
            }
            GroupedWriterFixture::UnsortedMapKeys if target == 0 => Box::from([
                ("zeta".into(), "first".into()),
                ("alpha".into(), "second".into()),
            ]),
            GroupedWriterFixture::DuplicateMapKey if target == 0 => Box::from([
                ("same".into(), "first".into()),
                ("same".into(), "second".into()),
            ]),
            _ => Box::default(),
        };
        let mut constants = vec![
            if matches!(fixture, GroupedWriterFixture::NestedLiteralBudgetExceeded) && target == 0 {
                UnpivotConstant::Int32List(
                    vec![0; PlanLimits::FROZEN.unpivot_collection_items + 1].into_boxed_slice(),
                )
            } else if matches!(fixture, GroupedWriterFixture::WrongConstantType) {
                UnpivotConstant::Utf8Map(Box::default())
            } else {
                UnpivotConstant::Int32List(Box::from([i32::try_from(target + 11).unwrap()]))
            },
            UnpivotConstant::Scalar(scalar_constant),
            UnpivotConstant::Utf8Map(properties),
        ];
        if matches!(fixture, GroupedWriterFixture::UncoveredAuxiliaryOutput) {
            constants.pop();
        }
        mappings.push(WriterGroupedUnpivotMapping {
            write_target_ordinal: write_target_ordinal(target),
            input: if matches!(fixture, GroupedWriterFixture::NonFinalInput) {
                shared_state_input
            } else {
                shared_final_output
            },
            constants: constants.into_boxed_slice(),
        });
    }
    if matches!(fixture, GroupedWriterFixture::DuplicateMapping) {
        mappings.insert(1, mappings[0].clone());
    }
    let value_output = if matches!(fixture, GroupedWriterFixture::MismatchedValueOutputType) {
        output_fields[ROOT_WRITE_RESULT_BLOB_TYPE_INDEX].value
    } else {
        output_fields[ROOT_WRITE_RESULT_BODY_INDEX].value
    };
    let mut literal_outputs = if matches!(fixture, GroupedWriterFixture::MismatchedValueOutputType)
    {
        vec![
            output_fields[ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX].value,
            output_fields[ROOT_WRITE_RESULT_BODY_INDEX].value,
            output_fields[ROOT_WRITE_RESULT_PROPERTIES_INDEX].value,
        ]
    } else {
        vec![
            output_fields[ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX].value,
            output_fields[ROOT_WRITE_RESULT_BLOB_TYPE_INDEX].value,
            output_fields[ROOT_WRITE_RESULT_PROPERTIES_INDEX].value,
        ]
    };
    if matches!(fixture, GroupedWriterFixture::OverlappingOutputRoles) {
        literal_outputs[0] = value_output;
    } else if matches!(fixture, GroupedWriterFixture::UncoveredAuxiliaryOutput) {
        literal_outputs.pop();
    }
    let grouped_unpivot = WriterGroupedUnpivotSpec {
        statistics_target_ordinals: if matches!(
            fixture,
            GroupedWriterFixture::SingleStatisticsTarget
        ) {
            Box::from([write_target_ordinal(0)])
        } else {
            Box::from([write_target_ordinal(0), write_target_ordinal(1)])
        },
        grouping_input: input_fields[WRITE_RELATION_TARGET_INDEX].value,
        grouping_output,
        passthrough_output: output_fields[ROOT_WRITE_RESULT_TARGET_INDEX].value,
        value_output,
        literal_outputs: literal_outputs.into_boxed_slice(),
        mappings: mappings.into_boxed_slice(),
        max_output_rows: if matches!(fixture, GroupedWriterFixture::OutputLimitExceeded) {
            MAX_UNPIVOT_OUTPUT_ROWS + 1
        } else {
            4096
        },
        max_output_bytes: 64 * 1024,
    };
    let mut finish_input_fields = input_fields.clone();
    if matches!(fixture, GroupedWriterFixture::InputSchemaDrift) {
        finish_input_fields.swap(0, 1);
    }
    if matches!(fixture, GroupedWriterFixture::GroupingRoleDrift) {
        finish_input_fields[WRITE_RELATION_TARGET_INDEX].role = WriterRelationFieldRole::Auxiliary;
    }
    let output = OutputPort {
        node: finish,
        columns: output_fields.iter().map(|field| field.value).collect(),
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            id: finish,
            inputs: Box::from([input]),
            required_inputs: Box::from([PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            }]),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output,
            kind: NodeKind::TableFinish(WriterFinishSpec {
                expected_target_ordinals: Box::from([
                    write_target_ordinal(0),
                    write_target_ordinal(1),
                ]),
                input_schema: WriterRelationSchema {
                    revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: finish_input_fields.into_boxed_slice(),
                },
                output_schema: WriterRelationSchema {
                    revision: ROOT_WRITE_RESULT_SCHEMA_REVISION,
                    fields: output_fields.into_boxed_slice(),
                },
                final_aggregates: final_aggregates.into_boxed_slice(),
                grouped_unpivot: if matches!(fixture, GroupedWriterFixture::MissingGroupedUnpivot) {
                    None
                } else {
                    Some(grouped_unpivot)
                },
            }),
        })
        .unwrap();
    builder.finish_definition(finish, FragmentSink::Noop, dop())
}

#[test]
fn writer_grouped_unpivot_allows_a_write_target_without_statistics() {
    let fragment = grouped_writer_fragment(GroupedWriterFixture::SingleStatisticsTarget).unwrap();
    let NodeKind::TableFinish(finish) = &fragment.nodes()[&fragment.root()].kind else {
        panic!("expected a table finish node");
    };
    let unpivot = finish.grouped_unpivot.as_ref().unwrap();
    assert_eq!(
        finish.expected_target_ordinals.as_ref(),
        [write_target_ordinal(0), write_target_ordinal(1)]
    );
    assert_eq!(
        unpivot.statistics_target_ordinals.as_ref(),
        [write_target_ordinal(0)]
    );
    assert_eq!(unpivot.mappings.len(), 1);
}

#[test]
fn writer_grouped_unpivot_keeps_target_local_mappings_for_a_shared_aggregate_channel() {
    let fragment = grouped_writer_fragment(GroupedWriterFixture::SharedChannel).unwrap();
    let NodeKind::TableFinish(finish) = &fragment.nodes()[&fragment.root()].kind else {
        panic!("expected a table finish node");
    };
    let unpivot = finish.grouped_unpivot.as_ref().unwrap();
    assert_eq!(finish.final_aggregates.len(), 1);
    assert_eq!(unpivot.mappings.len(), 2);
    assert_eq!(
        unpivot.mappings[0].write_target_ordinal,
        write_target_ordinal(0)
    );
    assert_eq!(
        unpivot.mappings[1].write_target_ordinal,
        write_target_ordinal(1)
    );
    assert_eq!(unpivot.mappings[0].input, unpivot.mappings[1].input);
    assert_ne!(unpivot.mappings[0].constants, unpivot.mappings[1].constants);
    assert!(fragment.values()[&unpivot.passthrough_output].ty.nullable);
    assert!(!fragment.values()[&unpivot.grouping_output].ty.nullable);
}

#[test]
fn writer_grouped_unpivot_requires_every_expected_target() {
    let error = grouped_writer_fragment(GroupedWriterFixture::MissingTarget)
        .unwrap_err()
        .to_string();
    assert!(error.contains("writer grouped Unpivot mappings do not cover every statistics target"));
}

#[test]
fn writer_grouped_unpivot_cannot_expand_an_intermediate_input_as_a_final_result() {
    let error = grouped_writer_fragment(GroupedWriterFixture::NonFinalInput)
        .unwrap_err()
        .to_string();
    assert!(error.contains("writer grouped Unpivot mapping input is not a final aggregate output"));
}

#[test]
fn writer_grouped_unpivot_cannot_drop_a_declared_final_aggregate_output() {
    let error = grouped_writer_fragment(GroupedWriterFixture::UnmappedFinalOutput)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("writer grouped Unpivot mappings do not cover every final aggregate output")
    );
}

#[test]
fn writer_grouped_unpivot_constants_must_match_the_root_relation_field_types() {
    let error = grouped_writer_fragment(GroupedWriterFixture::WrongConstantType)
        .unwrap_err()
        .to_string();
    assert!(error.contains("writer grouped Unpivot constant type differs from its literal output"));
}

#[test]
fn writer_final_aggregates_require_a_grouped_unpivot_contract() {
    let error = grouped_writer_fragment(GroupedWriterFixture::MissingGroupedUnpivot)
        .unwrap_err()
        .to_string();
    assert!(error.contains(
        "table finish final aggregates and grouped Unpivot must be both absent or both present"
    ));
}

#[test]
fn writer_grouped_unpivot_rejects_a_duplicate_target_input_pair() {
    let error = grouped_writer_fragment(GroupedWriterFixture::DuplicateMapping)
        .unwrap_err()
        .to_string();
    assert!(error.contains("writer grouped Unpivot repeats a target and aggregate output mapping"));
}

#[test]
fn writer_grouped_unpivot_scalar_constants_must_be_literals() {
    let error = grouped_writer_fragment(GroupedWriterFixture::NonLiteralScalar)
        .unwrap_err()
        .to_string();
    assert!(error.contains("writer grouped Unpivot constant type differs from its literal output"));
}

#[test]
fn writer_grouped_unpivot_value_output_type_must_match_the_final_aggregate() {
    let error = grouped_writer_fragment(GroupedWriterFixture::MismatchedValueOutputType)
        .unwrap_err()
        .to_string();
    assert!(error.contains("writer grouped Unpivot aggregate type differs from its value output"));
}

#[test]
fn writer_grouped_unpivot_value_and_literal_roles_cannot_overlap() {
    let error = grouped_writer_fragment(GroupedWriterFixture::OverlappingOutputRoles)
        .unwrap_err()
        .to_string();
    assert!(error.contains(
        "writer grouped Unpivot roles do not exactly cover distinct auxiliary finish fields"
    ));
}

#[test]
fn writer_grouped_unpivot_roles_must_cover_every_auxiliary_output() {
    let error = grouped_writer_fragment(GroupedWriterFixture::UncoveredAuxiliaryOutput)
        .unwrap_err()
        .to_string();
    assert!(error.contains(
        "writer grouped Unpivot roles do not exactly cover distinct auxiliary finish fields"
    ));
}

#[test]
fn writer_grouped_unpivot_map_keys_must_be_non_empty() {
    let error = grouped_writer_fragment(GroupedWriterFixture::EmptyMapKey)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("writer grouped Unpivot map keys must be non-empty and strictly increasing")
    );
}

#[test]
fn writer_grouped_unpivot_map_keys_must_be_strictly_ordered() {
    let error = grouped_writer_fragment(GroupedWriterFixture::UnsortedMapKeys)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("writer grouped Unpivot map keys must be non-empty and strictly increasing")
    );
}

#[test]
fn writer_grouped_unpivot_map_keys_must_be_unique() {
    let error = grouped_writer_fragment(GroupedWriterFixture::DuplicateMapKey)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("writer grouped Unpivot map keys must be non-empty and strictly increasing")
    );
}

#[test]
fn table_finish_input_schema_must_equal_its_exact_child_port() {
    let error = grouped_writer_fragment(GroupedWriterFixture::InputSchemaDrift)
        .unwrap_err()
        .to_string();
    assert!(error.contains("table finish input schema differs from its exact child output"));
}

#[test]
fn table_finish_aggregate_input_must_come_from_its_exact_child_port() {
    let error = grouped_writer_fragment(GroupedWriterFixture::AggregateInputDrift)
        .unwrap_err()
        .to_string();
    assert!(error.contains("table finish aggregate input is absent from its exact child output"));
}

#[test]
fn writer_aggregate_single_input_carrier_rejects_multi_argument_bindings() {
    let error = grouped_writer_fragment(GroupedWriterFixture::MultiArgumentBinding)
        .unwrap_err()
        .to_string();
    assert!(error.contains("writer aggregate carrier supports exactly one logical argument"));
}

#[test]
fn writer_grouped_unpivot_grouping_input_must_have_the_target_ordinal_role() {
    let error = grouped_writer_fragment(GroupedWriterFixture::GroupingRoleDrift)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(
            "writer grouped Unpivot grouping input is not the finish target ordinal field"
        )
    );
}

#[test]
fn writer_grouped_unpivot_passthrough_must_be_owned_as_the_target_ordinal() {
    let error = grouped_writer_fragment(GroupedWriterFixture::PassthroughOriginDrift)
        .unwrap_err()
        .to_string();
    assert!(error.contains(
        "writer grouped Unpivot passthrough output is not this finish node's target ordinal"
    ));
}

#[test]
fn writer_grouped_unpivot_rejects_output_bounds_above_the_contract_maximum() {
    let error = grouped_writer_fragment(GroupedWriterFixture::OutputLimitExceeded)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unpivot row/byte bounds are zero or exceed the contract maximum"));
}

#[test]
fn writer_grouped_unpivot_rejects_nested_literal_collections_above_the_budget() {
    let error = grouped_writer_fragment(GroupedWriterFixture::NestedLiteralBudgetExceeded)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unpivot literal collections exceed the contract budget"));
}
