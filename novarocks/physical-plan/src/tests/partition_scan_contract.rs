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

fn bucket_scheme(seed: u8) -> BucketPartitionScheme {
    BucketPartitionScheme {
        space: PartitionSpaceId::try_new([seed; 32]).unwrap(),
        bucket_count: 16,
        hash: PartitionHashAlgorithm::NativeBucketCrc32V1,
        layout: BucketLayoutAlgorithm::DenseZeroBasedV1,
        ordinal_domain: BucketOrdinalDomainProof {
            first_ordinal: 0,
            ordinal_count: 16,
            evidence_digest: [seed.wrapping_add(1); 32],
        },
    }
}

fn append_exchange_input(
    builder: &mut FragmentBuilder,
    edge: EdgeId,
    distribution: impl FnOnce(ValueId) -> Distribution,
) -> (NodeId, ValueId, PhysicalProperties) {
    let node = builder.reserve_node_id().unwrap();
    let source_value = ValueId::new(10_000 + edge.get());
    let value = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    let distribution = distribution(value);
    let properties = PhysicalProperties {
        row_multiplicity: if distribution == Distribution::Broadcast {
            RowMultiplicity::Replicated
        } else {
            RowMultiplicity::SingleCopy
        },
        distribution,
        ordering: Box::default(),
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties.clone(),
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
    (node, value, properties)
}

fn finish_join(
    left_distribution: impl FnOnce(ValueId) -> Distribution,
    right_distribution: impl FnOnce(ValueId) -> Distribution,
    distribution: JoinDistribution,
) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(301));
    let (left, left_value, left_properties) =
        append_exchange_input(&mut builder, EdgeId::new(301), left_distribution);
    let (right, right_value, right_properties) =
        append_exchange_input(&mut builder, EdgeId::new(302), right_distribution);
    let join = builder.reserve_node_id().unwrap();
    let left_key = builder
        .add_expression(
            join,
            ty(DataType::Int64, false),
            ExprKind::Value(left_value),
        )
        .unwrap();
    let right_key = builder
        .add_expression(
            join,
            ty(DataType::Int64, false),
            ExprKind::Value(right_value),
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: join,
            inputs: Box::from([left, right]),
            required_inputs: Box::from([left_properties.clone(), right_properties]),
            output_properties: left_properties,
            output: OutputPort {
                node: join,
                columns: Box::from([left_value, right_value]),
            },
            kind: NodeKind::HashJoin {
                kind: JoinKind::Inner,
                build_side: JoinSide::Right,
                condition: crate::HashJoinCondition::Ordinary {
                    keys: Box::from([JoinKey {
                        left: left_key,
                        right: right_key,
                        null_safe: false,
                    }]),
                    residual: None,
                },
                distribution,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    builder.finish_definition(
        join,
        FragmentSink::Noop,
        dop(),
        crate::StagePlacementPolicy::Singleton,
    )
}

fn finish_scan_with_budget(
    relation: Relation,
    read_budget: ScanReadBudget,
) -> Result<Fragment, ValidationErrors> {
    let column = relation.schema()[0].column.clone();
    let mut builder = FragmentBuilder::new(FragmentId::new(302));
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
            output_properties: relation.provided_properties().clone(),
            output: OutputPort {
                node: scan,
                columns: Box::from([value]),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                read_budget,
                provider_outputs: Box::from([(column, value)]),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    builder.finish_definition(
        scan,
        FragmentSink::Noop,
        dop(),
        crate::StagePlacementPolicy::Singleton,
    )
}

fn contains_error(result: Result<Fragment, ValidationErrors>, expected: &str) -> bool {
    result
        .expect_err("fixture must be rejected")
        .errors()
        .iter()
        .any(|error| error.message().contains(expected))
}

fn finish_plan_with_partition_distributions(
    distributions: impl IntoIterator<Item = Distribution>,
) -> Result<PhysicalPlan, ValidationErrors> {
    let binding = connector_binding();
    let mut plan = PlanBuilder::new(version());
    for (ordinal, distribution) in distributions.into_iter().enumerate() {
        let fragment_id = FragmentId::new(500 + u32::try_from(ordinal).unwrap());
        let mut relation = metadata_relation(
            &binding,
            ProviderColumnReference {
                column_payload: encoded(
                    &binding,
                    ConnectorCodecCategory::ReadColumn,
                    u8::try_from(ordinal + 80).unwrap(),
                ),
            },
        );
        let Relation::Metadata(metadata) = &mut relation else {
            unreachable!();
        };
        metadata.provided_properties = PhysicalProperties {
            distribution,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };

        let column = relation.schema()[0].column.clone();
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
        assert_eq!(value, ValueId::new(0));
        builder
            .insert_node_unchecked(PhysicalNode {
                id: scan,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: relation.provided_properties().clone(),
                output: OutputPort {
                    node: scan,
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
        plan.add_fragment(
            builder
                .finish_definition(
                    scan,
                    FragmentSink::Noop,
                    dop(),
                    crate::StagePlacementPolicy::Singleton,
                )
                .unwrap(),
        )
        .unwrap();
    }
    plan.finish()
}

fn contains_plan_error(result: Result<PhysicalPlan, ValidationErrors>, expected: &str) -> bool {
    result
        .expect_err("plan fixture must be rejected")
        .errors()
        .iter()
        .any(|error| error.message().contains(expected))
}

#[test]
fn partition_count_domain_defines_assignment_time_membership() {
    let domain = PartitionCountDomain {
        min: 3,
        max: 16,
        requires_power_of_two: true,
    };
    assert!(!domain.admits(2));
    assert!(domain.admits(4));
    assert!(domain.admits(8));
    assert!(domain.admits(16));
    assert!(!domain.admits(0));
    assert!(!domain.admits(3));
    assert!(!domain.admits(32));
}

#[test]
fn plan_global_partition_count_parameter_has_one_domain() {
    let left = hash_scheme(55);
    let mut right = hash_scheme(56);
    right.count.id = left.count.id;
    right.count.admissible = PartitionCountDomain {
        min: 2,
        max: 32,
        requires_power_of_two: true,
    };
    assert_ne!(left.count.admissible, right.count.admissible);

    assert!(contains_plan_error(
        finish_plan_with_partition_distributions([
            Distribution::Hash {
                keys: Box::from([ValueId::new(0)]),
                scheme: left,
            },
            Distribution::Hash {
                keys: Box::from([ValueId::new(0)]),
                scheme: right,
            },
        ]),
        "partition-count parameter identity has conflicting admissible domains"
    ));
}

#[test]
fn plan_global_partition_space_has_one_definition() {
    let left = hash_scheme(57);
    let mut changed_count = hash_scheme(58);
    changed_count.space = left.space;
    assert!(contains_plan_error(
        finish_plan_with_partition_distributions([
            Distribution::Hash {
                keys: Box::from([ValueId::new(0)]),
                scheme: left.clone(),
            },
            Distribution::Hash {
                keys: Box::from([ValueId::new(0)]),
                scheme: changed_count,
            },
        ]),
        "partition-space identity has conflicting definitions"
    ));

    let mut bucket = bucket_scheme(59);
    bucket.space = left.space;
    assert!(contains_plan_error(
        finish_plan_with_partition_distributions([
            Distribution::Hash {
                keys: Box::from([ValueId::new(0)]),
                scheme: left,
            },
            Distribution::BucketShuffle {
                keys: Box::from([ValueId::new(0)]),
                scheme: bucket,
            },
        ]),
        "partition-space identity has conflicting definitions"
    ));
}

#[test]
fn partitioned_join_requires_one_shared_symbolic_partition_space() {
    let shared = hash_scheme(51);
    finish_join(
        |key| Distribution::Hash {
            keys: Box::from([key]),
            scheme: shared.clone(),
        },
        |key| Distribution::Hash {
            keys: Box::from([key]),
            scheme: shared.clone(),
        },
        JoinDistribution::Partitioned,
    )
    .unwrap();

    let left = hash_scheme(52);
    let right = hash_scheme(53);
    assert!(contains_error(
        finish_join(
            |key| Distribution::Hash {
                keys: Box::from([key]),
                scheme: left,
            },
            |key| Distribution::Hash {
                keys: Box::from([key]),
                scheme: right,
            },
            JoinDistribution::Partitioned,
        ),
        "exact partition-space proof"
    ));
}

#[test]
fn broadcast_build_requires_an_exact_broadcast_build_input() {
    assert!(contains_error(
        finish_join(
            |_| Distribution::Unconstrained,
            |_| Distribution::Unconstrained,
            JoinDistribution::BroadcastBuild,
        ),
        "exact partition-space proof"
    ));
    finish_join(
        |_| Distribution::Unconstrained,
        |_| Distribution::Broadcast,
        JoinDistribution::BroadcastBuild,
    )
    .unwrap();
}

#[test]
fn singleton_join_requires_two_single_copy_singleton_inputs() {
    finish_join(
        |_| Distribution::Singleton,
        |_| Distribution::Singleton,
        JoinDistribution::Singleton,
    )
    .unwrap();

    assert!(contains_error(
        finish_join(
            |_| Distribution::Unconstrained,
            |_| Distribution::Singleton,
            JoinDistribution::Singleton,
        ),
        "exact partition-space proof"
    ));
}

#[test]
fn invalid_symbolic_partition_count_domain_is_rejected() {
    let mut invalid = hash_scheme(54);
    invalid.count.admissible = PartitionCountDomain {
        min: 3,
        max: 3,
        requires_power_of_two: true,
    };
    let invalid_right = invalid.clone();
    assert!(contains_error(
        finish_join(
            |key| Distribution::Hash {
                keys: Box::from([key]),
                scheme: invalid,
            },
            |key| Distribution::Hash {
                keys: Box::from([key]),
                scheme: invalid_right,
            },
            JoinDistribution::Partitioned,
        ),
        "power-of-two partition-count domain has no admissible member"
    ));
}

#[test]
fn colocated_join_requires_exact_bucket_shuffle_compatibility() {
    let shared = bucket_scheme(61);
    finish_join(
        |key| Distribution::BucketShuffle {
            keys: Box::from([key]),
            scheme: shared.clone(),
        },
        |key| Distribution::BucketShuffle {
            keys: Box::from([key]),
            scheme: shared.clone(),
        },
        JoinDistribution::Colocated,
    )
    .unwrap();

    assert!(contains_error(
        finish_join(
            |_| Distribution::Unconstrained,
            |_| Distribution::Unconstrained,
            JoinDistribution::Colocated,
        ),
        "exact partition-space proof"
    ));

    let left = bucket_scheme(62);
    let mut right = left.clone();
    right.ordinal_domain.evidence_digest = [99; 32];
    assert!(contains_error(
        finish_join(
            |key| Distribution::BucketShuffle {
                keys: Box::from([key]),
                scheme: left,
            },
            |key| Distribution::BucketShuffle {
                keys: Box::from([key]),
                scheme: right,
            },
            JoinDistribution::Colocated,
        ),
        "exact partition-space proof"
    ));
}

#[test]
fn bucket_hash_and_dense_ordinal_domain_are_validated() {
    let mut wrong_hash = bucket_scheme(63);
    wrong_hash.hash = PartitionHashAlgorithm::NativeExchangeV1;
    let wrong_hash_right = wrong_hash.clone();
    assert!(contains_error(
        finish_join(
            |key| Distribution::BucketShuffle {
                keys: Box::from([key]),
                scheme: wrong_hash,
            },
            |key| Distribution::BucketShuffle {
                keys: Box::from([key]),
                scheme: wrong_hash_right,
            },
            JoinDistribution::Colocated,
        ),
        "wrong hash algorithm"
    ));

    let mut invalid_layout = bucket_scheme(64);
    invalid_layout.ordinal_domain.first_ordinal = 1;
    invalid_layout.ordinal_domain.ordinal_count = 15;
    let invalid_layout_right = invalid_layout.clone();
    assert!(contains_error(
        finish_join(
            |key| Distribution::BucketShuffle {
                keys: Box::from([key]),
                scheme: invalid_layout,
            },
            |key| Distribution::BucketShuffle {
                keys: Box::from([key]),
                scheme: invalid_layout_right,
            },
            JoinDistribution::Colocated,
        ),
        "complete dense ordinal-domain proof"
    ));
}

#[test]
fn system_metadata_accepts_unconstrained_distribution_and_plan_dop() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 31),
    };
    let mut system = metadata_relation(&binding, column);
    if let Relation::Metadata(metadata) = &mut system {
        metadata.provided_properties.distribution = Distribution::Unconstrained;
    }
    let fragment = finish_scan_with_budget(system, scan_budget()).unwrap();
    assert_eq!(fragment.dop_domain(), dop());
}

#[test]
fn scan_read_budget_rejects_zero_and_over_limit_values() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 34),
    };
    let relation = metadata_relation(&binding, column);
    assert!(contains_error(
        finish_scan_with_budget(
            relation.clone(),
            ScanReadBudget {
                max_batch_rows: 0,
                max_batch_bytes: 1,
            },
        ),
        "scan read budget"
    ));
    assert!(contains_error(
        finish_scan_with_budget(
            relation,
            ScanReadBudget {
                max_batch_rows: 1,
                max_batch_bytes: MAX_SCAN_BATCH_BYTES + 1,
            },
        ),
        "scan read budget"
    ));
}
