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

//! Small physical definitions isolate input-order invariants from SQL costs,
//! placement and runtime scheduling. Each expected cycle is derived from the
//! stated producer/input order, not from the graph implementation's output.

use super::*;
use crate::{
    AggregateGrouping, DeliveryRequirement, Distribution, EdgeDestination, EdgeKind,
    EdgePartitioning, EdgeSource, ExprKind, FragmentBuilder, FragmentOutputPort, FragmentSink,
    JoinDistribution, JoinKey, JoinKind, JoinSide, LiteralValue, NestLoopJoinDistribution,
    OutputRole, PhysicalProperties, PipelineDopDomain, RequiredInputs, RowMultiplicity,
    RuntimeFilterArtifactCapability, RuntimeFilterCompletion, RuntimeFilterConsumer,
    RuntimeFilterContributionKind, RuntimeFilterCoverage, RuntimeFilterCoverageNode,
    RuntimeFilterDomain, RuntimeFilterEndpoint, RuntimeFilterEqualityWitnessId, RuntimeFilterKind,
    RuntimeFilterLifecycle, RuntimeFilterNullSemantics, RuntimeFilterPolicy, RuntimeFilterProducer,
    RuntimeFilterProducerProgress, RuntimeFilterReduction, RuntimeFilterWitnessId,
    StagePlacementPolicy, ValueType,
};
use arrow_schema::DataType;

fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

#[derive(Default)]
struct Fixture {
    builders: BTreeMap<FragmentId, FragmentBuilder>,
    roots: BTreeMap<FragmentId, NodeId>,
    edges: BTreeMap<EdgeId, Edge>,
}

impl Fixture {
    fn builder(&mut self, fragment: u32, root: u32) -> &mut FragmentBuilder {
        let fragment = FragmentId::new(fragment);
        self.roots.insert(fragment, NodeId::new(root));
        self.builders
            .entry(fragment)
            .or_insert_with(|| FragmentBuilder::new(fragment))
    }

    fn values(&mut self, fragment: u32, node: u32) {
        self.builder(fragment, node)
            .add_values(
                NodeId::new(node),
                Box::from([Box::default()]),
                Box::default(),
            )
            .unwrap();
    }

    fn exchange(&mut self, fragment: u32, node: u32, source: u32) {
        let edge = EdgeId::new(u32::try_from(self.edges.len()).unwrap());
        self.builder(fragment, node)
            .add_exchange_source(
                NodeId::new(node),
                edge,
                Box::default(),
                Box::default(),
                Distribution::Singleton,
                RowMultiplicity::SingleCopy,
            )
            .unwrap();
        self.edges.insert(
            edge,
            Edge {
                id: edge,
                role: OutputRole::Relational,
                delivery: DeliveryRequirement::DemandDriven,
                kind: EdgeKind::CteMulticast,
                source: EdgeSource {
                    fragment: FragmentId::new(source),
                    port: FragmentOutputPort::Root,
                    projection: Box::default(),
                },
                destination: EdgeDestination {
                    fragment: FragmentId::new(fragment),
                    node: NodeId::new(node),
                    receive_mapping: Box::default(),
                },
                partitioning: EdgePartitioning {
                    source: Distribution::Singleton,
                    source_multiplicity: RowMultiplicity::SingleCopy,
                    destination: Distribution::Singleton,
                    destination_multiplicity: RowMultiplicity::SingleCopy,
                },
            },
        );
    }

    fn hash_join(&mut self, fragment: u32, node: u32, left: u32, right: u32, build_side: JoinSide) {
        let builder = self.builder(fragment, node);
        let key = builder
            .add_expression(
                NodeId::new(node),
                ValueType::new(DataType::Int64, false),
                ExprKind::Literal(LiteralValue::Int64(1)),
            )
            .unwrap();
        builder
            .add_join(
                NodeId::new(node),
                [NodeId::new(left), NodeId::new(right)],
                Box::from([properties(), properties()]),
                Box::default(),
                Distribution::Singleton,
                NodeKind::HashJoin {
                    kind: JoinKind::Inner,
                    keys: Box::from([JoinKey {
                        left: key,
                        right: key,
                        null_safe: false,
                    }]),
                    build_side,
                    distribution: JoinDistribution::Singleton,
                    residual: None,
                    null_extended: Box::default(),
                },
            )
            .unwrap();
    }

    fn nested_loop_join(&mut self, fragment: u32, node: u32, left: u32, right: u32) {
        self.builder(fragment, node)
            .add_join(
                NodeId::new(node),
                [NodeId::new(left), NodeId::new(right)],
                Box::from([properties(), properties()]),
                Box::default(),
                Distribution::Singleton,
                NodeKind::NestLoopJoin {
                    kind: JoinKind::Cross,
                    distribution: NestLoopJoinDistribution::Singleton,
                    predicate: None,
                    null_extended: Box::default(),
                },
            )
            .unwrap();
    }

    fn aggregate(&mut self, fragment: u32, node: u32, input: u32) {
        self.builder(fragment, node)
            .add_row_consuming(
                NodeId::new(node),
                Box::from([NodeId::new(input)]),
                RequiredInputs::Singleton,
                Distribution::Singleton,
                Box::default(),
                NodeKind::Aggregate {
                    group_by: Box::default(),
                    calls: Box::default(),
                    grouping: AggregateGrouping::Complete,
                },
            )
            .unwrap();
    }

    fn project(&mut self, fragment: u32, node: u32, input: u32) {
        self.builder(fragment, node)
            .add_project(
                NodeId::new(node),
                NodeId::new(input),
                Box::default(),
                Box::default(),
            )
            .unwrap();
    }

    fn set_op(&mut self, fragment: u32, node: u32, inputs: &[u32], kind: SetOperationKind) {
        self.builder(fragment, node)
            .add_row_consuming(
                NodeId::new(node),
                inputs
                    .iter()
                    .copied()
                    .map(NodeId::new)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                RequiredInputs::Singleton,
                Distribution::Singleton,
                Box::default(),
                NodeKind::SetOp {
                    kind,
                    input_mappings: inputs
                        .iter()
                        .map(|_| Box::default())
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                },
            )
            .unwrap();
    }

    fn finish(self, filters: &[RuntimeFilter]) -> WaitGraph {
        let mut fragments = BTreeMap::new();
        for (id, builder) in self.builders {
            let outgoing = self
                .edges
                .values()
                .filter(|edge| edge.source.fragment == id)
                .map(|edge| edge.id)
                .collect::<Vec<_>>();
            let sink = match outgoing.as_slice() {
                [] => FragmentSink::Noop,
                [edge] => FragmentSink::Stream { edge: *edge },
                _ => FragmentSink::Multicast {
                    edges: outgoing.into_boxed_slice(),
                },
            };
            let fragment = builder
                .finish_definition(
                    self.roots[&id],
                    sink,
                    PipelineDopDomain {
                        min: 1,
                        max: 1,
                        requires_power_of_two: false,
                    },
                    StagePlacementPolicy::Singleton,
                )
                .unwrap();
            fragments.insert(id, fragment);
        }
        WaitGraph::build(&fragments, &self.edges, filters).unwrap()
    }
}

fn operator(fragment: u32, node: u32) -> OperatorRef {
    OperatorRef {
        fragment: FragmentId::new(fragment),
        node: NodeId::new(node),
    }
}

fn membership_filter(id: u32, producer: OperatorRef, consumer: OperatorRef) -> RuntimeFilter {
    let equality = RuntimeFilterEqualityWitnessId::new(id);
    let witness = RuntimeFilterWitnessId::new(id);
    let coverage = RuntimeFilterCoverage {
        nodes: Box::from([RuntimeFilterCoverageNode::Witness(witness)]),
        root: 0,
    };
    RuntimeFilter {
        id: RuntimeFilterId::new(id),
        kind: RuntimeFilterKind::Bloom,
        domain: RuntimeFilterDomain::Membership {
            ty: ValueType::new(DataType::Int64, false),
            null_semantics: RuntimeFilterNullSemantics::NeverMatches,
        },
        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
        reduction: RuntimeFilterReduction::SetUnion,
        availability_coverage: coverage.clone(),
        terminal_coverage: coverage,
        equality_witnesses: Box::default(),
        producers: Box::from([RuntimeFilterProducer {
            witness,
            endpoint: RuntimeFilterEndpoint {
                fragment: producer.fragment,
                node: producer.node,
                values: Box::default(),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 },
            contribution_kinds: Box::from([RuntimeFilterContributionKind::ProducerClosed]),
            completion: RuntimeFilterCompletion::ProducerClosed,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::default(),
                non_build_edges: Box::default(),
            },
            target: RuntimeFilterProducerTarget::JoinBuildKey { equality },
        }]),
        consumers: Box::from([RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: consumer.fragment,
                node: consumer.node,
                values: Box::default(),
            },
            apply_point: RuntimeFilterApplyPoint::NodeOutput,
            capabilities: Box::from([RuntimeFilterArtifactCapability::Membership]),
            activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: RuntimeFilterConsumerTarget::ScanField {
                equality,
                lineage: Box::default(),
            },
        }]),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 4096,
            max_artifact_bytes: 4096,
            deadline_ms: 1000,
            max_retries: 0,
        },
    }
}

fn assert_real_cycles(graph: &WaitGraph, resolution: &WaitResolution) {
    assert!(!resolution.data_cycles.is_empty());
    for witness in &resolution.data_cycles {
        assert_eq!(witness.nodes.first(), witness.nodes.last());
        assert_eq!(witness.nodes.len(), witness.edges.len() + 1);
        for (pair, kinds) in witness.nodes.windows(2).zip(witness.edges.iter()) {
            assert_eq!(&graph.dependencies[&pair[0]][&pair[1]], kinds);
            assert!(!kinds.is_empty());
        }
    }
}

#[test]
fn shared_producer_cannot_feed_both_ordered_hash_join_inputs() {
    for build_side in [JoinSide::Left, JoinSide::Right] {
        let mut fixture = Fixture::default();
        fixture.values(0, 0);
        fixture.exchange(1, 0, 0);
        fixture.exchange(1, 1, 0);
        fixture.hash_join(1, 2, 0, 1, build_side);
        let mut graph = fixture.finish(&[]);
        let result = graph.resolve().unwrap();
        assert!(result.filter_downgrades.is_empty());
        assert_real_cycles(&graph, &result);
    }
}

#[test]
fn opposite_priorities_across_two_shared_producers_form_a_global_cycle() {
    let mut fixture = Fixture::default();
    fixture.values(0, 0); // A
    fixture.values(1, 0); // B
    fixture.exchange(2, 0, 1); // probe B
    fixture.exchange(2, 1, 0); // build A
    fixture.hash_join(2, 2, 0, 1, JoinSide::Right);
    fixture.exchange(3, 0, 0); // probe A
    fixture.exchange(3, 1, 1); // build B
    fixture.hash_join(3, 2, 0, 1, JoinSide::Right);
    let mut graph = fixture.finish(&[]);
    let result = graph.resolve().unwrap();
    assert_real_cycles(&graph, &result);
    let nodes = &result.data_cycles[0].nodes;
    assert!(nodes.contains(&WaitNode::Completion(operator(0, 0))));
    assert!(nodes.contains(&WaitNode::Completion(operator(1, 0))));
}

#[test]
fn complete_aggregate_on_low_priority_branch_absorbs_the_shared_output() {
    let mut fixture = Fixture::default();
    fixture.values(0, 0);
    fixture.exchange(1, 0, 0);
    fixture.aggregate(1, 1, 0);
    fixture.exchange(2, 0, 1); // probe receives complete aggregate output
    fixture.exchange(2, 1, 0); // build receives shared output directly
    fixture.hash_join(2, 2, 0, 1, JoinSide::Right);
    let mut graph = fixture.finish(&[]);
    assert!(graph.resolve().unwrap().is_acyclic());
}

#[test]
fn complete_aggregate_on_high_priority_branch_does_not_break_the_cycle() {
    let mut fixture = Fixture::default();
    fixture.values(0, 0);
    fixture.exchange(1, 0, 0);
    fixture.aggregate(1, 1, 0);
    fixture.exchange(2, 0, 0); // probe blocks shared output
    fixture.exchange(2, 1, 1); // aggregate build still needs all shared rows
    fixture.hash_join(2, 2, 0, 1, JoinSide::Right);
    let mut graph = fixture.finish(&[]);
    let result = graph.resolve().unwrap();
    assert_real_cycles(&graph, &result);
}

#[test]
fn every_later_set_op_stage_keeps_its_own_admission_gate() {
    for kind in [SetOperationKind::Intersect, SetOperationKind::Except] {
        let mut fixture = Fixture::default();
        fixture.values(0, 0);
        for input in 0..3 {
            fixture.exchange(1, input, 0);
        }
        fixture.set_op(1, 3, &[0, 1, 2], kind);
        let mut graph = fixture.finish(&[]);
        let result = graph.resolve().unwrap();
        assert_real_cycles(&graph, &result);
        assert!(graph.dependencies.contains_key(&WaitNode::Input {
            operator: operator(1, 3),
            ordinal: 2
        }));
    }
}

#[test]
fn pipeline_backpressure_crosses_fragment_boundaries() {
    let mut fixture = Fixture::default();
    fixture.values(0, 0);
    fixture.exchange(1, 0, 0);
    fixture.project(1, 1, 0);
    fixture.exchange(2, 0, 1);
    fixture.exchange(2, 1, 0);
    fixture.hash_join(2, 2, 0, 1, JoinSide::Right);
    let mut graph = fixture.finish(&[]);
    let result = graph.resolve().unwrap();
    assert_real_cycles(&graph, &result);
}

#[test]
fn rf_downgrade_removes_an_optional_edge_from_a_mixed_wait_cycle() {
    let mut fixture = Fixture::default();
    fixture.values(0, 0); // shared A
    fixture.values(1, 0);
    fixture.exchange(2, 0, 1); // B is blocked by its RF gate
    fixture.exchange(3, 0, 0); // join probe A
    fixture.exchange(3, 1, 2); // join build B
    fixture.hash_join(3, 2, 0, 1, JoinSide::Right);
    fixture.values(4, 0);
    fixture.exchange(4, 1, 0); // independent producer builds filter from A
    fixture.hash_join(4, 2, 0, 1, JoinSide::Right);
    let filters = [membership_filter(1, operator(4, 2), operator(2, 0))];
    let mut graph = fixture.finish(&filters);
    let result = graph.resolve().unwrap();
    assert_eq!(
        result.filter_downgrades.as_ref(),
        &[FilterConsumerRef {
            filter: RuntimeFilterId::new(1),
            ordinal: 0
        }]
    );
    assert!(result.is_acyclic());
    assert!(graph.resolve().unwrap().filter_downgrades.is_empty());
}

#[test]
fn independent_filter_builds_do_not_require_the_old_multicast_over_downgrade() {
    let mut fixture = Fixture::default();
    fixture.values(0, 0);
    for fragment in [1, 2] {
        fixture.exchange(fragment, 0, 0);
        fixture.values(fragment, 1); // build can finish without the shared output
        fixture.hash_join(fragment, 2, 0, 1, JoinSide::Right);
    }
    let filters = [
        membership_filter(1, operator(1, 2), operator(2, 0)),
        membership_filter(2, operator(2, 2), operator(1, 0)),
    ];
    let mut graph = fixture.finish(&filters);
    let result = graph.resolve().unwrap();
    assert!(result.filter_downgrades.is_empty());
    assert!(result.is_acyclic());
}

#[test]
fn rf_can_be_downgraded_without_erasing_a_required_data_cycle() {
    let mut fixture = Fixture::default();
    fixture.values(0, 0);
    fixture.exchange(1, 0, 0);
    fixture.exchange(1, 1, 0);
    fixture.hash_join(1, 2, 0, 1, JoinSide::Right);
    let filters = [membership_filter(1, operator(1, 2), operator(1, 0))];
    let mut graph = fixture.finish(&filters);
    let result = graph.resolve().unwrap();
    assert_eq!(result.filter_downgrades.len(), 1);
    assert_real_cycles(&graph, &result);
}

#[test]
fn union_all_reads_shared_branches_without_an_input_order_dependency() {
    let mut fixture = Fixture::default();
    fixture.values(0, 0);
    fixture.exchange(1, 0, 0);
    fixture.exchange(1, 1, 0);
    fixture.set_op(1, 2, &[0, 1], SetOperationKind::UnionAll);
    let mut graph = fixture.finish(&[]);
    assert!(graph.resolve().unwrap().is_acyclic());
}

#[test]
fn shared_local_input_and_nested_loop_build_keep_the_same_wait_identity() {
    let mut fixture = Fixture::default();
    fixture.values(0, 0);
    fixture.nested_loop_join(0, 1, 0, 0);
    let mut graph = fixture.finish(&[]);
    let result = graph.resolve().unwrap();
    assert_real_cycles(&graph, &result);
}

#[test]
fn nested_nonshared_rf_wait_downgrades_only_its_cyclic_consumer() {
    // Same dependency shape as the pre-existing RF-cycle test: an outer
    // build is an inner join whose probe waits for that outer filter.
    let mut fixture = Fixture::default();
    fixture.values(0, 0);
    fixture.values(0, 1);
    fixture.hash_join(0, 2, 0, 1, JoinSide::Right);
    fixture.values(0, 3);
    fixture.hash_join(0, 4, 2, 3, JoinSide::Left);
    let mut filter = membership_filter(44, operator(0, 4), operator(0, 2));
    filter.producers[0].apply_point = RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 };
    filter.consumers[0].apply_point = RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 };
    let mut independent_consumer = filter.consumers[0].clone();
    independent_consumer.endpoint.node = NodeId::new(4);
    independent_consumer.apply_point = RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 };
    filter.consumers = Box::from([filter.consumers[0].clone(), independent_consumer]);
    let mut graph = fixture.finish(&[filter]);
    let result = graph.resolve().unwrap();
    assert_eq!(
        result.filter_downgrades.as_ref(),
        &[FilterConsumerRef {
            filter: RuntimeFilterId::new(44),
            ordinal: 0
        }]
    );
    assert!(result.is_acyclic());
}

#[test]
fn ordered_join_chain_graph_size_follows_input_edges_not_ancestor_pairs() {
    for joins in [1_u32, 8, 64] {
        let mut fixture = Fixture::default();
        fixture.values(0, 0);
        for join in 1..=joins {
            fixture.values(0, 2 * join - 1);
            fixture.hash_join(0, 2 * join, 2 * join - 2, 2 * join - 1, JoinSide::Left);
        }
        let mut graph = fixture.finish(&[]);
        // The physical fixture has 2j+1 operators, 2j input references,
        // j pipelined probe inputs and j build-before-probe gates.
        let operators = (2 * joins + 1) as usize;
        let inputs = (2 * joins) as usize;
        let gates = joins as usize;
        let expected_vertices = 2 * operators + gates;
        let expected_edges = operators + inputs + gates + 2 * gates;
        assert_eq!(graph.dependencies.len(), expected_vertices);
        assert_eq!(
            graph
                .dependencies
                .values()
                .map(BTreeMap::len)
                .sum::<usize>(),
            expected_edges
        );
        assert!(graph.resolve().unwrap().is_acyclic());
    }
}

#[test]
fn wide_set_op_graph_uses_a_linear_completion_prefix() {
    for inputs in [2_u32, 8, 64] {
        let mut fixture = Fixture::default();
        for input in 0..inputs {
            fixture.values(0, input);
        }
        fixture.set_op(
            0,
            inputs,
            &(0..inputs).collect::<Vec<_>>(),
            SetOperationKind::Intersect,
        );
        let mut graph = fixture.finish(&[]);
        // m independent sources plus one set-op have m real input edges.
        // Prefixes add m roots, m-1 prefix links and two edges per gate.
        let input_count = inputs as usize;
        let operators = input_count + 1;
        let gates = input_count - 1;
        let expected_vertices = 2 * operators + input_count + gates;
        let expected_edges = operators + input_count + input_count + gates + 2 * gates;
        assert_eq!(graph.dependencies.len(), expected_vertices);
        assert_eq!(
            graph
                .dependencies
                .values()
                .map(BTreeMap::len)
                .sum::<usize>(),
            expected_edges
        );
        assert!(graph.resolve().unwrap().is_acyclic());
    }
}

#[test]
fn optional_rf_wait_is_removed_before_judging_required_rf_cycles() {
    let required = FilterConsumerRef {
        filter: RuntimeFilterId::new(1),
        ordinal: 0,
    };
    let optional = FilterConsumerRef {
        filter: RuntimeFilterId::new(2),
        ordinal: 0,
    };
    let required_gate = WaitNode::FilterConsumer(required);
    let optional_gate = WaitNode::FilterConsumer(optional);
    let required_build = WaitNode::Filter(required.filter);
    let optional_build = WaitNode::Filter(optional.filter);
    // Required TopN build -> ordinary blocking consumer -> ordinary build ->
    // TopN consumer -> TopN build. Only the ordinary wait may be removed.
    let mut graph = WaitGraph::default();
    graph.non_downgradable.insert(required);
    graph.add_edge(
        required_gate,
        required_build,
        WaitEdgeKind::FilterWait(required),
    );
    graph.add_edge(required_build, optional_gate, WaitEdgeKind::DataWait);
    graph.add_edge(
        optional_gate,
        optional_build,
        WaitEdgeKind::FilterWait(optional),
    );
    graph.add_edge(optional_build, required_gate, WaitEdgeKind::DataWait);
    let resolved = graph.resolve().unwrap();
    assert_eq!(resolved.filter_downgrades.as_ref(), &[optional]);
    assert!(resolved.is_acyclic());
    assert!(
        graph.dependencies[&required_gate][&required_build]
            .contains(&WaitEdgeKind::FilterWait(required))
    );
}

#[test]
fn required_rf_wait_is_rejected_when_its_own_cycle_remains() {
    let required = FilterConsumerRef {
        filter: RuntimeFilterId::new(1),
        ordinal: 0,
    };
    let gate = WaitNode::FilterConsumer(required);
    let build = WaitNode::Filter(required.filter);
    let mut graph = WaitGraph::default();
    graph.non_downgradable.insert(required);
    graph.add_edge(gate, build, WaitEdgeKind::FilterWait(required));
    graph.add_edge(build, gate, WaitEdgeKind::DataWait);
    assert_eq!(
        graph.resolve(),
        Err(WaitGraphError::UnresolvableFilterConsumer(required))
    );
}

#[test]
fn summary_backpressure_does_not_stop_the_computing_root() {
    // The provider route drains the computing root. The independent summary
    // is read only afterwards. Blocking its delivery must not block that drain.
    let root = operator(0, 0);
    let writer = operator(1, 0);
    let summary_receiver = operator(2, 0);
    let consumer = operator(2, 1);
    let mut graph = WaitGraph::default();
    for operator in [root, writer, summary_receiver, consumer] {
        graph.add_edge(
            WaitNode::Completion(operator),
            WaitNode::Backpressure(operator),
            WaitEdgeKind::DataWait,
        );
    }
    graph.add_operator_input(writer, root, true);
    graph.add_operator_input(consumer, writer, false);
    graph.add_operator_input(consumer, summary_receiver, true);
    graph.add_summary_output(root);
    graph.add_summary_input(summary_receiver, root.fragment);
    graph.add_edge(
        WaitNode::Backpressure(summary_receiver),
        WaitNode::Completion(writer),
        WaitEdgeKind::DataWait,
    );
    assert!(graph.resolve().unwrap().is_acyclic());
    // If summary were aliased to the relational root, as it was before the
    // separate B1b output port existed, this same gate would falsely cycle.
    graph.add_edge(
        WaitNode::Backpressure(root),
        WaitNode::SummaryBackpressure(root.fragment),
        WaitEdgeKind::Backpressure,
    );
    let resolution = graph.resolve().unwrap();
    assert_real_cycles(&graph, &resolution);
}

#[test]
fn graph_resolution_observes_the_original_control_and_latches_its_first_refusal() {
    struct StopOnWork {
        cause: CompileControlError,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl novarocks_type_contract::PureCompileControl for StopOnWork {
        fn checkpoint(
            &self,
            _phase: novarocks_type_contract::CompilePhase,
            units: u32,
        ) -> Result<(), CompileControlError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if units == 0 {
                Ok(())
            } else {
                Err(self.cause.clone())
            }
        }
    }
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let mut fixture = Fixture::default();
        fixture.values(0, 0);
        for node in 1..1024 {
            fixture.project(0, node, node - 1);
        }
        let mut graph = fixture.finish(&[]);
        let control = StopOnWork {
            cause: cause.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let mut work =
            CompileCheckpoints::try_new(&control, novarocks_type_contract::CompilePhase::Validate)
                .unwrap();
        let result = graph.resolve_observed(&mut work);
        assert_eq!(result, Err(WaitGraphError::Control(cause.clone())));
        let calls = control.calls.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            work.finish_result(result, WaitGraphError::Control),
            Err(WaitGraphError::Control(cause))
        );
        assert_eq!(
            control.calls.load(std::sync::atomic::Ordering::Relaxed),
            calls,
            "an originating refusal must not call the caller control again"
        );
    }
}

#[test]
fn frozen_wait_inspection_keeps_every_optional_rf_edge_until_planning_resolves_it() {
    let mut fixture = Fixture::default();
    fixture.values(0, 0);
    fixture.values(0, 1);
    fixture.hash_join(0, 2, 0, 1, JoinSide::Right);
    fixture.values(0, 3);
    fixture.hash_join(0, 4, 2, 3, JoinSide::Left);
    let mut filter = membership_filter(44, operator(0, 4), operator(0, 2));
    filter.producers[0].apply_point = RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 };
    filter.consumers[0].apply_point = RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 };
    let mut graph = fixture.finish(&[filter]);
    let before = graph.dependencies.clone();
    let mut work = CompileCheckpoints::try_new(
        &TestCompileControl,
        novarocks_type_contract::CompilePhase::Validate,
    )
    .unwrap();
    let cycles = graph.active_cycles_observed(&mut work).unwrap();
    work.finish().unwrap();
    assert!(cycles.filter);
    assert_eq!(graph.dependencies, before);
    let resolution = graph.resolve().unwrap();
    assert_eq!(
        resolution.filter_downgrades.as_ref(),
        &[FilterConsumerRef {
            filter: RuntimeFilterId::new(44),
            ordinal: 0,
        }]
    );
    assert!(resolution.is_acyclic());
}
