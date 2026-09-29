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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_schema::DataType;

use crate::resource::MAX_PLAN_DERIVED_CUT_ITEMS;
use crate::{
    AggregatePhase, Distribution, EdgeId, ExprId, ExprKind, FragmentCuts, FragmentId, FragmentSink,
    NodeId, NodeKind, PhysicalNode, PhysicalPlan, RequiredContracts, RowMultiplicity, ValueId,
    ValueOrigin, ValueType,
};

#[cfg(test)]
mod validation_error_tests {
    use super::*;

    #[test]
    fn mixed_decimal_largeint_validates_only_exact_new_add_sub_output() {
        let expression = |id, data_type| crate::ExprNode {
            id: ExprId::new(id),
            owner: NodeId::new(1),
            lambda_scope: None,
            ty: ValueType::new(data_type, true),
            kind: ExprKind::Literal(crate::LiteralValue::Null),
        };
        let decimal = expression(1, DataType::Decimal128(38, 15));
        let integer = expression(2, DataType::FixedSizeBinary(16));
        for op in [crate::BinaryOperator::Add, crate::BinaryOperator::Subtract] {
            let exact = expression(3, DataType::Decimal256(55, 15));
            let mut errors = ValidationContext::new();
            validate_binary_types(&decimal, op, &integer, &exact, "binary", &mut errors);
            assert!(errors.is_empty());
            let wrong = expression(3, DataType::Decimal128(38, 15));
            let mut errors = ValidationContext::new();
            validate_binary_types(&decimal, op, &integer, &wrong, "binary", &mut errors);
            assert!(!errors.is_empty());
        }
        for op in [
            crate::BinaryOperator::Multiply,
            crate::BinaryOperator::Divide,
            crate::BinaryOperator::Modulo,
        ] {
            let exact = expression(3, DataType::Decimal256(55, 15));
            let mut errors = ValidationContext::new();
            validate_binary_types(&decimal, op, &integer, &exact, "binary", &mut errors);
            assert!(!errors.is_empty());
        }
    }

    #[test]
    fn validation_diagnostics_have_a_fixed_cardinality_and_display_bound() {
        let mut collector = ValidationContext::new();
        for ordinal in 0..(MAX_VALIDATION_ERRORS * 4) {
            collector.push(ValidationError::new(
                format!("expressions[{ordinal}]"),
                "invalid expression",
            ));
        }
        let errors = ValidationErrors::from_collector(collector);
        assert_eq!(errors.errors().len(), MAX_VALIDATION_ERRORS + 1);
        assert_eq!(
            errors
                .errors()
                .iter()
                .filter(|error| error.message().contains("truncated"))
                .count(),
            1
        );
        assert!(errors.to_string().len() < 16 * 1024);
    }

    #[test]
    fn binary_arithmetic_uses_the_type_contract_largeint_domain() {
        let expression = |id, data_type| crate::ExprNode {
            id: ExprId::new(id),
            owner: NodeId::new(1),
            lambda_scope: None,
            ty: ValueType::new(data_type, false),
            kind: ExprKind::Literal(crate::LiteralValue::Null),
        };
        let largeint = DataType::FixedSizeBinary(novarocks_type_contract::LARGEINT_BYTE_WIDTH);
        let left = expression(1, largeint.clone());
        let right = expression(2, DataType::Int64);
        let output = expression(3, largeint);
        let mut errors = ValidationContext::new();
        validate_binary_types(
            &left,
            crate::BinaryOperator::Add,
            &right,
            &output,
            "binary",
            &mut errors,
        );
        assert!(errors.is_empty());
    }

    #[test]
    fn occurrence_indexes_preserve_duplicate_mapping_semantics() {
        let source = ValueId::new(1);
        let other_source = ValueId::new(2);
        let destination = ValueId::new(3);
        let repeated =
            ValueMappingIndex::from_pairs(&[(source, destination), (source, destination)]);
        assert_eq!(repeated.resolve(destination, false), None);
        assert_eq!(repeated.resolve(destination, true), Some(source));
        assert!(repeated.contains(source, destination));

        let conflicting =
            ValueMappingIndex::from_pairs(&[(source, destination), (other_source, destination)]);
        assert_eq!(conflicting.resolve(destination, false), None);
        assert_eq!(conflicting.resolve(destination, true), None);

        let port = ValuePortIndex::new(&[source, source, other_source]);
        assert_eq!(port.occurrences.get(&source), Some(&2));
        assert!(port.contains(&other_source));
    }

    #[test]
    fn semantic_trace_mapping_index_is_charged_once_and_each_lookup_is_bounded() {
        let mapping = (0_u32..1024)
            .map(|ordinal| (ValueId::new(ordinal), ValueId::new(ordinal + 2048)))
            .collect::<Vec<_>>();
        let expected = [mapping[0].1, mapping[512].1, mapping[1023].1];
        let mut indexes = SemanticTraceIndexes::default();
        let mut budget = SemanticTraceWorkBudget::new(&PlanLimits::FROZEN);
        let initial = budget.remaining;

        assert!(
            indexes
                .map_edge_values(EdgeId::new(1), &mapping, &expected, false, &mut budget)
                .is_some()
        );
        assert_eq!(initial - budget.remaining, mapping.len() + expected.len());

        let after_first = budget.remaining;
        assert!(
            indexes
                .map_edge_values(EdgeId::new(1), &mapping, &expected, false, &mut budget)
                .is_some()
        );
        assert_eq!(after_first - budget.remaining, expected.len());
    }

    #[test]
    fn source_provenance_fanout_shares_the_complete_binding_set() {
        let source = CompactSourceBindingSet::from_ids(&(0..4096).collect::<Vec<_>>()).unwrap();
        let empty = CompactSourceBindingSet::from_ids(&[]).unwrap();

        let fanout = (0..1024)
            .map(|_| CompactSourceBindingSet::union(&empty, [source.clone()]))
            .collect::<Vec<_>>();

        assert!(fanout.iter().all(|set| Arc::ptr_eq(&set.ids, &source.ids)));
    }

    #[test]
    fn source_provenance_rejects_high_fanout_before_destination_materialization() {
        let mut items = 0;
        assert!(charge_provenance_cut_items(&mut items, 4096, 8193).is_none());
        assert!(items > MAX_PLAN_DERIVED_CUT_ITEMS);
    }

    #[test]
    fn runtime_filter_witness_index_fails_closed_on_duplicate_or_missing_identity() {
        let witness = crate::RuntimeFilterEqualityWitness {
            id: crate::RuntimeFilterEqualityWitnessId::new(1),
            fragment: FragmentId::new(1),
            join: NodeId::new(1),
            key_ordinal: 0,
            domain_side: crate::JoinSide::Right,
        };
        let witnesses = [witness, witness];
        let duplicated = runtime_filter_witness_index(&witnesses);

        assert!(
            runtime_filter_equality_witness(
                &duplicated,
                crate::RuntimeFilterEqualityWitnessId::new(1)
            )
            .is_none()
        );
        assert!(
            runtime_filter_equality_witness(
                &duplicated,
                crate::RuntimeFilterEqualityWitnessId::new(2)
            )
            .is_none()
        );
    }

    #[test]
    fn runtime_filter_indexes_build_one_large_port_and_frontier_per_shared_site() {
        let mut builder = crate::FragmentBuilder::new(FragmentId::new(901));
        let node = builder.reserve_node_id().unwrap();
        let values = (0..4096)
            .map(|ordinal| {
                builder
                    .add_value(
                        ValueType::new(DataType::Int64, false),
                        ValueOrigin::NodeOutput {
                            node,
                            output_ordinal: ordinal,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: crate::PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
                output: crate::OutputPort {
                    node,
                    columns: values.clone().into_boxed_slice(),
                },
                kind: NodeKind::Values {
                    rows: Box::default(),
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                node,
                FragmentSink::Noop,
                crate::PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let node = fragment.nodes().get(&node).unwrap();
        let mut indexes = RuntimeFilterLineageIndexes::default();

        assert_eq!(
            indexes.apply_port_contains_all(
                &fragment,
                node,
                crate::RuntimeFilterApplyPoint::NodeOutput,
                &[values[0], values[4095]],
            ),
            Some(true)
        );
        assert_eq!(
            indexes.apply_port_contains_all(
                &fragment,
                node,
                crate::RuntimeFilterApplyPoint::NodeOutput,
                &[values[2048]],
            ),
            Some(true)
        );
        assert_eq!(indexes.apply_ports.len(), 1);

        let inbound = BTreeSet::new();
        assert!(
            indexes
                .build_frontier(&fragment, node.id, &inbound)
                .build
                .is_empty()
        );
        assert!(
            indexes
                .build_frontier(&fragment, node.id, &inbound)
                .build
                .is_empty()
        );
        assert_eq!(indexes.build_frontiers.len(), 1);
    }

    #[test]
    fn wide_identity_project_semantics_remain_valid() {
        let mut builder = crate::FragmentBuilder::new(FragmentId::new(902));
        let input = builder.reserve_node_id().unwrap();
        let value_type = ValueType::new(DataType::Int64, false);
        let input_values = (0..4096)
            .map(|ordinal| {
                builder
                    .add_value(
                        value_type.clone(),
                        ValueOrigin::NodeOutput {
                            node: input,
                            output_ordinal: ordinal,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let singleton = crate::PhysicalProperties {
            distribution: Distribution::Singleton,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        builder
            .insert_node_unchecked(PhysicalNode {
                id: input,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: singleton.clone(),
                output: crate::OutputPort {
                    node: input,
                    columns: input_values.clone().into_boxed_slice(),
                },
                kind: NodeKind::Values {
                    rows: Box::default(),
                },
            })
            .unwrap();
        let project = builder.reserve_node_id().unwrap();
        let expressions = input_values
            .iter()
            .map(|input_value| {
                let expression = builder
                    .add_expression(project, value_type.clone(), ExprKind::Value(*input_value))
                    .unwrap();
                (expression, *input_value)
            })
            .collect::<Vec<_>>();
        let output_values = input_values.clone();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: project,
                inputs: Box::from([input]),
                required_inputs: Box::from([singleton.clone()]),
                output_properties: singleton,
                output: crate::OutputPort {
                    node: project,
                    columns: output_values.clone().into_boxed_slice(),
                },
                kind: NodeKind::Project {
                    expressions: expressions.clone().into_boxed_slice(),
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                project,
                FragmentSink::Noop,
                crate::PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .expect("a wide identity project is semantically valid");
        validate_fragment(&fragment, &FragmentCuts::default())
            .expect("independent validation accepts the wide identity project");

        let project_node = fragment.nodes().get(&project).unwrap();
        let input_node = fragment.nodes().get(&input).unwrap();
        let mut indexes = SemanticTraceIndexes::default();
        let mut budget = SemanticTraceWorkBudget::new(&PlanLimits::FROZEN);
        assert_eq!(
            indexes
                .map_project_values(
                    &fragment,
                    project_node,
                    input_node,
                    &expressions,
                    &output_values,
                    &mut budget,
                )
                .as_deref(),
            Some(input_values.as_slice())
        );
    }

    #[test]
    fn wide_hash_distribution_keys_validate_one_exact_colocation_mapping() {
        let source_keys = (1..=4096).map(ValueId::new).collect::<Vec<_>>();
        let destination_keys = (5001..=9096).map(ValueId::new).collect::<Vec<_>>();
        let mapping = source_keys
            .iter()
            .copied()
            .zip(destination_keys.iter().copied())
            .collect::<Vec<_>>();
        let scheme = crate::HashPartitionScheme {
            space: novarocks_type_contract::PartitionSpaceId::try_new([17; 32]).unwrap(),
            count: crate::PartitionCountParameter {
                id: novarocks_type_contract::PartitionCountParameterId::try_new([18; 32]).unwrap(),
                admissible: crate::PartitionCountDomain {
                    min: 1,
                    max: 4096,
                    requires_power_of_two: true,
                },
            },
            definition: crate::HashDefinition::native_exchange(),
        };
        let partitioning = crate::EdgePartitioning {
            source: Distribution::Hash {
                keys: source_keys.clone().into_boxed_slice(),
                scheme: scheme.clone(),
            },
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Hash {
                keys: destination_keys.clone().into_boxed_slice(),
                scheme,
            },
            destination_multiplicity: RowMultiplicity::SingleCopy,
        };
        let mut errors = ValidationContext::new();

        validate_mapped_partitioning(&partitioning, &mapping, "wide_hash", &mut errors);

        assert!(errors.is_empty());
        assert!(mapped_partition_keys_match(
            &source_keys,
            &destination_keys,
            &mapping
        ));
        assert!(distribution_colocates_by(
            &partitioning.source,
            &source_keys
        ));
    }

    #[test]
    fn aggregate_sequence_index_builds_once_for_many_distinct_lookups_and_ambiguity() {
        let binding = |sequence| crate::AggregateBinding {
            function: crate::BoundFunction {
                function_id: crate::FunctionId::try_new("builtin/test_sum/v1").unwrap(),
                overload: crate::FunctionOverloadId::try_new("i64").unwrap(),
                kind: crate::FunctionKind::Aggregate,
                argument_types: Box::from([crate::FunctionArgumentType::Value(ValueType::new(
                    DataType::Int64,
                    false,
                ))]),
                result_type: ValueType::new(DataType::Int64, false),
                volatility: crate::FunctionVolatility::Immutable,
                argument_evaluation: crate::FunctionArgumentEvaluation::Eager,
                failure_behavior: crate::FunctionFailureBehavior::Propagate,
            },
            phase: AggregatePhase::Partial { sequence },
            logical_argument_count: 1,
            intermediate_type: ValueType::new(DataType::Binary, false),
            state_format: crate::AggregateStateFormatId::try_new("test_sum/state-v1").unwrap(),
        };
        let mut calls = (1..=4096)
            .map(|id| crate::AggregateCall {
                id: crate::AggregateCallId::new(id),
                binding: binding(crate::AggregateSequenceId::new(id)),
                arguments: Box::default(),
                distinct: false,
                order_by: Box::default(),
                output: ValueId::new(id),
            })
            .collect::<Vec<_>>();
        calls.push(crate::AggregateCall {
            id: crate::AggregateCallId::new(4097),
            binding: binding(crate::AggregateSequenceId::new(4096)),
            arguments: Box::default(),
            distinct: false,
            order_by: Box::default(),
            output: ValueId::new(4097),
        });
        let mut indexes = SemanticTraceIndexes::default();
        let mut budget = SemanticTraceWorkBudget::new(&PlanLimits::FROZEN);
        let initial_work = budget.remaining;

        for id in 1..4096 {
            let call = indexes
                .aggregate_sequence_call(
                    FragmentId::new(903),
                    NodeId::new(1),
                    &calls,
                    crate::AggregateSequenceId::new(id),
                    &mut budget,
                )
                .expect("each unique non-final sequence resolves to its exact call");
            assert_eq!(call.id, crate::AggregateCallId::new(id));
        }
        assert!(
            indexes
                .aggregate_sequence_call(
                    FragmentId::new(903),
                    NodeId::new(1),
                    &calls,
                    crate::AggregateSequenceId::new(4096),
                    &mut budget,
                )
                .is_none(),
            "a repeated sequence must remain ambiguous"
        );
        assert_eq!(indexes.aggregate_sequences.len(), 1);
        assert_eq!(initial_work - budget.remaining, calls.len());
    }

    #[test]
    fn overlapping_runtime_filter_hulls_charge_each_revisited_static_contribution() {
        let runtime_filter = |id, witness_count| crate::RuntimeFilter {
            id: crate::RuntimeFilterId::new(id),
            kind: crate::RuntimeFilterKind::InList,
            domain: crate::RuntimeFilterDomain::Membership {
                ty: ValueType::new(DataType::Int64, false),
                null_semantics: crate::RuntimeFilterNullSemantics::NeverMatches,
            },
            lifecycle: crate::RuntimeFilterLifecycle::CompleteOnce,
            reduction: crate::RuntimeFilterReduction::SetUnion,
            availability_coverage: crate::RuntimeFilterCoverage {
                nodes: Box::default(),
                root: 0,
            },
            terminal_coverage: crate::RuntimeFilterCoverage {
                nodes: Box::default(),
                root: 0,
            },
            equality_witnesses: (0..witness_count)
                .map(|ordinal| crate::RuntimeFilterEqualityWitness {
                    id: crate::RuntimeFilterEqualityWitnessId::new(ordinal + 1),
                    fragment: FragmentId::new(1),
                    join: NodeId::new(1),
                    key_ordinal: ordinal,
                    domain_side: crate::JoinSide::Right,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            producers: Box::default(),
            consumers: Box::default(),
            policy: crate::RuntimeFilterPolicy {
                max_contribution_bytes: 1,
                max_artifact_bytes: 1,
                deadline_ms: 1,
                max_retries: 1,
            },
        };
        let common = crate::RuntimeFilterId::new(1);
        let mut filters = BTreeMap::from([(common, runtime_filter(1, 4096))]);
        for id in 2..=257 {
            filters.insert(crate::RuntimeFilterId::new(id), runtime_filter(id, 0));
        }
        let plan = PhysicalPlan::from(crate::PhysicalPlanParts {
            version: crate::PlanVersionId::try_new([1; 16]).unwrap(),
            fragments: BTreeMap::new(),
            edges: BTreeMap::new(),
            runtime_filters: filters,
            result_port: None,
            artifact_refs: BTreeMap::new(),
            required: RequiredContracts::default(),
            annotations: Box::default(),
        });
        let mut cache = RuntimeFilterBuildDependencyCache::default();
        let mut budget = SemanticTraceWorkBudget::new(&PlanLimits::FROZEN);
        let mut rejected = false;

        for id in 2..=257 {
            let mut fragments = BTreeSet::new();
            let mut edges = BTreeSet::new();
            if extend_runtime_filter_proof_hull(
                &plan,
                [common, crate::RuntimeFilterId::new(id)],
                &mut fragments,
                &mut edges,
                &mut cache,
                &mut budget,
            )
            .is_none()
            {
                rejected = true;
                break;
            }
        }

        assert!(
            rejected,
            "overlapping but distinct proof hulls must not rescan a wide shared filter beyond the work budget"
        );
    }

    #[test]
    fn fragment_validation_indexes_share_one_wide_child_port_across_project_fanout() {
        let mut builder = crate::FragmentBuilder::new(FragmentId::new(904));
        let source = builder.reserve_node_id().unwrap();
        let value_type = ValueType::new(DataType::Int64, false);
        let source_values = (0..4096)
            .map(|ordinal| {
                builder
                    .add_value(
                        value_type.clone(),
                        ValueOrigin::NodeOutput {
                            node: source,
                            output_ordinal: ordinal,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let singleton = crate::PhysicalProperties {
            distribution: Distribution::Singleton,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        builder
            .insert_node_unchecked(PhysicalNode {
                id: source,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: singleton.clone(),
                output: crate::OutputPort {
                    node: source,
                    columns: source_values.clone().into_boxed_slice(),
                },
                kind: NodeKind::Values {
                    rows: Box::default(),
                },
            })
            .unwrap();

        let mut projects = Vec::new();
        let mut mappings = Vec::new();
        for value in source_values.iter().take(256) {
            let project = builder.reserve_node_id().unwrap();
            let expression = builder
                .add_expression(project, value_type.clone(), ExprKind::Value(*value))
                .unwrap();
            builder
                .insert_node_unchecked(PhysicalNode {
                    id: project,
                    inputs: Box::from([source]),
                    required_inputs: Box::from([singleton.clone()]),
                    output_properties: singleton.clone(),
                    output: crate::OutputPort {
                        node: project,
                        columns: Box::from([*value]),
                    },
                    kind: NodeKind::Project {
                        expressions: Box::from([(expression, *value)]),
                    },
                })
                .unwrap();
            projects.push(project);
            mappings.push(Box::from([*value]));
        }

        let union = builder.reserve_node_id().unwrap();
        let union_output = builder
            .add_value(
                value_type,
                ValueOrigin::NodeOutput {
                    node: union,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: union,
                inputs: projects.clone().into_boxed_slice(),
                required_inputs: vec![singleton.clone(); projects.len()].into_boxed_slice(),
                output_properties: singleton,
                output: crate::OutputPort {
                    node: union,
                    columns: Box::from([union_output]),
                },
                kind: NodeKind::SetOp {
                    kind: crate::SetOperationKind::UnionAll,
                    input_mappings: mappings.into_boxed_slice(),
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                union,
                FragmentSink::Noop,
                crate::PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .expect("the wide shared-child project fanout is semantically valid");

        let indexes = FragmentValidationIndexes::new(&fragment);
        let source_output = indexes.output_ports.get(&source).unwrap();
        assert!(projects.iter().all(|project| {
            matches!(
                indexes.visible_inputs.get(project),
                Some(VisibleInputIndex::One(port)) if Arc::ptr_eq(port, source_output)
            )
        }));
    }
}
