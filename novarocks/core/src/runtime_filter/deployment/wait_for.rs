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

use crate::sql::planner::runtime_filter::progress::JoinBuildProgressCatalog;
use crate::sql::planner::runtime_filter::wait_graph::{
    ConsumerWaitInput, RefinedFragmentEdge, build_refined_wait_graph,
};

use super::{DeploymentError, planning_adapter};

// Design: ADR-0001 (docs/adr/ADR-0001-runtime-filter-strict-fail-liveness-philosophy.md)
pub fn validate_wait_for(
    edges: &[RefinedFragmentEdge],
    consumers: &[ConsumerWaitInput],
    join_progress: &JoinBuildProgressCatalog,
) -> Result<(), DeploymentError> {
    let refined = build_refined_wait_graph(edges, consumers, join_progress)
        .map_err(|_| DeploymentError::FragmentCycle)?;
    match refined.find_cycle() {
        Some(cycle) => {
            let wait = cycle.primary_wait();
            Err(DeploymentError::BlockingFeedbackCycle {
                channel: planning_adapter::channel_id(wait.channel),
                binding: planning_adapter::binding_id(wait.consumer_binding),
                cycle: cycle.render(),
            })
        }
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::DeploymentError;
    use super::*;
    use crate::sql::planner::runtime_filter::contract::{
        BindingId, ChannelId, CompletionFenceKind, CompletionRequirement, ConsumerActivation,
        LateApplyGranularity,
    };
    use crate::sql::planner::runtime_filter::progress::{FrontierEdge, JoinBuildProgressProof};
    use crate::sql::planner::runtime_filter::wait_graph::{
        ConsumerWaitBehavior, ExecutionDependencyGraph, ProducerWaitInput, ProofRejection,
        revalidate_proof,
    };

    pub(super) fn edge(source: u32, target: u32) -> RefinedFragmentEdge {
        RefinedFragmentEdge {
            source_fragment: source,
            target_fragment: target,
            target_exchange_node: 0,
        }
    }

    fn partitioned_edge(source: u32, target: u32, exchange_node: i32) -> RefinedFragmentEdge {
        RefinedFragmentEdge {
            source_fragment: source,
            target_fragment: target,
            target_exchange_node: exchange_node,
        }
    }

    fn proof(
        producer_fragment: u32,
        join_node: i32,
        frontier: Vec<(u32, i32)>,
        non_build: Vec<(u32, i32)>,
    ) -> JoinBuildProgressProof {
        let fe = |(s, x): &(u32, i32)| FrontierEdge {
            source_fragment: *s,
            target_exchange_node: *x,
        };
        JoinBuildProgressProof {
            channel: ChannelId::new(7),
            producer_binding: BindingId::new(100),
            producer_fragment,
            join_node_id: join_node,
            build_frontier: frontier.iter().map(fe).collect(),
            non_build_inputs: non_build.iter().map(fe).collect(),
        }
    }

    fn wait(
        binding: u32,
        consumer_frag: u32,
        activation: ConsumerActivation,
        producers: Vec<u32>,
    ) -> ConsumerWaitInput {
        ConsumerWaitInput {
            channel: ChannelId::new(7),
            binding: BindingId::new(binding),
            consumer_fragment: consumer_frag,
            behavior: match activation {
                ConsumerActivation::BlockingSnapshot => ConsumerWaitBehavior::BlocksUntilComplete,
                ConsumerActivation::NonBlockingLive { .. } => ConsumerWaitBehavior::NeverBlocks,
            },
            producers: producers
                .into_iter()
                .enumerate()
                .map(|(index, fragment)| ProducerWaitInput {
                    channel: ChannelId::new(7),
                    binding: BindingId::new(100 + u32::try_from(index).unwrap()),
                    fragment,
                    node_id: 10,
                    completion_requirement: CompletionRequirement::ProducerClosed,
                })
                .collect(),
        }
    }

    fn validate(
        _deps: &ExecutionDependencyGraph,
        edges: &[RefinedFragmentEdge],
        consumers: &[ConsumerWaitInput],
    ) -> Result<(), DeploymentError> {
        validate_wait_for(edges, consumers, &Default::default())
    }

    fn catalog(proofs: Vec<JoinBuildProgressProof>) -> JoinBuildProgressCatalog {
        proofs
            .into_iter()
            .map(|p| ((p.channel, p.producer_binding, p.producer_fragment), p))
            .collect()
    }

    fn forged_catalog(proof: JoinBuildProgressProof) -> JoinBuildProgressCatalog {
        [((ChannelId::new(7), BindingId::new(100), 1), proof)]
            .into_iter()
            .collect()
    }

    fn validate_with_progress(
        _deps: &ExecutionDependencyGraph,
        edges: &[RefinedFragmentEdge],
        consumers: &[ConsumerWaitInput],
        catalog: &JoinBuildProgressCatalog,
    ) -> Result<(), DeploymentError> {
        validate_wait_for(edges, consumers, catalog)
    }

    fn revalidate_exact(
        proof: &JoinBuildProgressProof,
        edges: &[RefinedFragmentEdge],
        deps: &ExecutionDependencyGraph,
        consumer_fragment: u32,
    ) -> Result<(), ProofRejection> {
        let producer = ProducerWaitInput {
            channel: proof.channel,
            binding: proof.producer_binding,
            fragment: proof.producer_fragment,
            node_id: proof.join_node_id,
            completion_requirement: CompletionRequirement::ProducerClosed,
        };
        revalidate_proof(
            proof,
            edges,
            deps,
            consumer_fragment,
            proof.channel,
            &producer,
        )
    }

    #[test]
    fn reachability_is_transitive_and_acyclic_chain_ok() {
        // data flow 3 -> 2 -> 1 (leaf 3 feeds 2 feeds root 1); target depends on source.
        let g = ExecutionDependencyGraph::from_fragment_edges(&[edge(3, 2), edge(2, 1)]).unwrap();
        assert!(g.reaches(1, 2));
        assert!(g.reaches(1, 3)); // transitive
        assert!(g.reaches(2, 3));
        assert!(!g.reaches(3, 1));
        assert!(!g.reaches(2, 1));
    }

    #[test]
    fn cycle_is_rejected() {
        let err = ExecutionDependencyGraph::from_fragment_edges(&[edge(1, 2), edge(2, 1)]);
        assert!(err.is_err());
    }

    #[test]
    fn reachability_dedups_diamond_paths() {
        // D(4) feeds both B(2) and C(3); both feed A(1). A reaches D via two paths.
        let g = ExecutionDependencyGraph::from_fragment_edges(&[
            edge(4, 2),
            edge(4, 3),
            edge(2, 1),
            edge(3, 1),
        ])
        .unwrap();
        assert!(g.reaches(1, 2));
        assert!(g.reaches(1, 3));
        assert!(g.reaches(1, 4)); // transitive via both paths, deduped
        assert!(!g.reaches(4, 1));
    }

    #[test]
    fn empty_edges_yields_empty_graph() {
        let g = ExecutionDependencyGraph::from_fragment_edges(&[]).unwrap();
        assert!(!g.reaches(1, 1));
    }

    #[test]
    fn revalidation_accepts_exact_partition() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![(3, 30)], vec![(2, 20)]);
        assert_eq!(revalidate_exact(&p, &edges, &deps, 2), Ok(()));
    }

    #[test]
    fn revalidation_rejects_forged_exchange_node() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![(3, 31)], vec![(2, 20)]); // 31 not sealed
        assert_eq!(
            revalidate_exact(&p, &edges, &deps, 2),
            Err(ProofRejection::PartitionMismatch)
        );
    }

    #[test]
    fn revalidation_rejects_incomplete_partition() {
        // Sealed in-edges of frag 1 are {2->1@20, 3->1@30, 4->1@40}; the proof
        // omits 4->1@40 entirely -> planner missed an input, reject.
        let edges = vec![
            partitioned_edge(2, 1, 20),
            partitioned_edge(3, 1, 30),
            partitioned_edge(4, 1, 40),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![(3, 30)], vec![(2, 20)]);
        assert_eq!(
            revalidate_exact(&p, &edges, &deps, 2),
            Err(ProofRejection::PartitionMismatch)
        );
    }

    #[test]
    fn revalidation_rejects_overlapping_partition() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![(3, 30), (2, 20)], vec![(2, 20)]);
        assert_eq!(
            revalidate_exact(&p, &edges, &deps, 2),
            Err(ProofRejection::OverlappingPartition)
        );
    }

    #[test]
    fn revalidation_sanity_requires_consumer_under_non_build_input() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![(3, 30)], vec![(2, 20)]);
        // consumer on frag 3 (the build input) violates placement sanity.
        assert_eq!(
            revalidate_exact(&p, &edges, &deps, 3),
            Err(ProofRejection::ConsumerOutsideProbeRegion)
        );
    }

    #[test]
    fn revalidation_allows_same_fragment_local_consumer() {
        // Colocate-style: no in-edges at all, consumer co-located with producer.
        let edges: Vec<RefinedFragmentEdge> = vec![];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p = proof(1, 10, vec![], vec![]);
        assert_eq!(revalidate_exact(&p, &edges, &deps, 1), Ok(()));
    }

    #[test]
    fn blocking_consumer_upstream_of_producer_is_a_cycle() {
        // scan(2) -> topn(1): producer topn(1) depends on consumer scan(2).
        let deps = ExecutionDependencyGraph::from_fragment_edges(&[edge(2, 1)]).unwrap();
        let c = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let err = validate(&deps, &[edge(2, 1)], &[c]).unwrap_err();
        assert!(matches!(err, DeploymentError::BlockingFeedbackCycle { .. }));
    }

    #[test]
    fn non_blocking_same_shape_is_allowed() {
        let deps = ExecutionDependencyGraph::from_fragment_edges(&[edge(2, 1)]).unwrap();
        let c = wait(
            10,
            2,
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Split,
            },
            vec![1],
        );
        assert!(validate(&deps, &[edge(2, 1)], &[c]).is_ok());
    }

    #[test]
    fn blocking_consumer_downstream_of_producer_is_fine() {
        // build(2) -> probe(1): consumer probe(1) depends on producer build(2). No cycle.
        let deps = ExecutionDependencyGraph::from_fragment_edges(&[edge(2, 1)]).unwrap();
        let c = wait(10, 1, ConsumerActivation::BlockingSnapshot, vec![2]);
        assert!(validate(&deps, &[edge(2, 1)], &[c]).is_ok());
    }

    #[test]
    fn blocking_consumer_with_one_cyclic_producer_among_many_is_rejected() {
        // consumer on fragment 2; producers on fragment 9 (unrelated, not in the
        // graph) and fragment 1 (depends on 2 via edge(2,1) → closes a cycle).
        let deps = ExecutionDependencyGraph::from_fragment_edges(&[edge(2, 1)]).unwrap();
        let c = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![9, 1]);
        let err = validate(&deps, &[edge(2, 1)], &[c]).unwrap_err();
        assert!(matches!(err, DeploymentError::BlockingFeedbackCycle { .. }));
    }

    #[test]
    fn accepted_proof_refines_wait_edge_and_passes() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 30)], vec![(2, 20)])]);
        assert!(validate_with_progress(&deps, &edges, &[consumer], &c).is_ok());
    }

    #[test]
    fn forged_proof_channel_cannot_redirect_wait_to_build_ready() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let mut forged = proof(1, 10, vec![(3, 30)], vec![(2, 20)]);
        forged.channel = ChannelId::new(8);

        assert!(matches!(
            validate_with_progress(&deps, &edges, &[consumer], &forged_catalog(forged)),
            Err(DeploymentError::BlockingFeedbackCycle { .. })
        ));
    }

    #[test]
    fn forged_proof_binding_cannot_redirect_wait_to_build_ready() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let mut forged = proof(1, 10, vec![(3, 30)], vec![(2, 20)]);
        forged.producer_binding = BindingId::new(101);

        assert!(matches!(
            validate_with_progress(&deps, &edges, &[consumer], &forged_catalog(forged)),
            Err(DeploymentError::BlockingFeedbackCycle { .. })
        ));
    }

    #[test]
    fn forged_proof_fragment_cannot_redirect_wait_to_build_ready() {
        let edges = vec![
            partitioned_edge(2, 1, 20),
            partitioned_edge(3, 1, 30),
            partitioned_edge(2, 9, 20),
            partitioned_edge(3, 9, 30),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let forged = proof(9, 10, vec![(3, 30)], vec![(2, 20)]);

        assert!(matches!(
            validate_with_progress(&deps, &edges, &[consumer], &forged_catalog(forged)),
            Err(DeploymentError::BlockingFeedbackCycle { .. })
        ));
    }

    #[test]
    fn forged_proof_join_owner_cannot_redirect_wait_to_build_ready() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let canonical = proof(1, 10, vec![(3, 30)], vec![(2, 20)]);
        let mut forged = canonical;
        forged.join_node_id = 11;
        let forged = forged_catalog(forged);

        assert!(matches!(
            validate_wait_for(&edges, &[consumer], &forged),
            Err(DeploymentError::BlockingFeedbackCycle { .. })
        ));
    }

    #[test]
    fn mismatched_producer_facts_cannot_authorize_build_ready() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 30)], vec![(2, 20)])]);
        let mut consumer = consumer;
        consumer.producers[0].channel = ChannelId::new(8);

        assert!(matches!(
            validate_wait_for(&edges, &[consumer], &c),
            Err(DeploymentError::BlockingFeedbackCycle { .. })
        ));
    }

    #[test]
    fn incompatible_completion_cannot_authorize_build_ready() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 30)], vec![(2, 20)])]);
        let mut consumer = consumer;
        consumer.producers[0].completion_requirement =
            CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen);

        assert!(matches!(
            validate_wait_for(&edges, &[consumer], &c),
            Err(DeploymentError::BlockingFeedbackCycle { .. })
        ));
    }

    #[test]
    fn frontier_depending_on_consumer_is_rejected() {
        let edges = vec![
            partitioned_edge(2, 1, 20),
            partitioned_edge(3, 1, 30),
            edge(2, 3),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 30)], vec![(2, 20)])]);
        let err = validate_with_progress(&deps, &edges, &[consumer], &c).unwrap_err();
        assert!(matches!(err, DeploymentError::BlockingFeedbackCycle { .. }));
    }

    #[test]
    fn forged_proof_falls_back_to_coarse_edge_and_rejects() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 31)], vec![(2, 20)])]);
        let err = validate_with_progress(&deps, &edges, &[consumer], &c).unwrap_err();
        let DeploymentError::BlockingFeedbackCycle { cycle, .. } = err else {
            panic!("expected cycle");
        };
        assert!(
            cycle
                .iter()
                .any(|step| step.contains("proof rejected: PartitionMismatch"))
        );
    }

    #[test]
    fn cycle_provenance_attributes_consumer_and_producer_bindings() {
        let edges = vec![partitioned_edge(2, 1, 20), partitioned_edge(3, 1, 30)];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 31)], vec![(2, 20)])]);
        let err = validate_with_progress(&deps, &edges, &[consumer], &c).unwrap_err();
        let DeploymentError::BlockingFeedbackCycle {
            channel,
            binding,
            cycle,
        } = err
        else {
            panic!("expected cycle");
        };
        assert_eq!(
            channel,
            crate::runtime_filter::model::contract::ChannelId::new(7)
        );
        assert_eq!(
            binding,
            crate::runtime_filter::model::contract::BindingId::new(10)
        );
        let rendered = cycle.join(", ");
        assert!(rendered.contains("channel=7"));
        assert!(rendered.contains("consumer-binding=10"));
        assert!(rendered.contains("producer-binding=100"));
        assert!(rendered.contains("proof rejected: PartitionMismatch"));
    }

    #[test]
    fn two_individually_valid_proofs_composing_a_cycle_are_rejected() {
        let edges = vec![
            partitioned_edge(1, 2, 20),
            partitioned_edge(3, 2, 30),
            edge(4, 3),
            partitioned_edge(4, 5, 50),
            partitioned_edge(6, 5, 60),
            edge(1, 6),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let p1 = proof(2, 10, vec![(3, 30)], vec![(1, 20)]);
        let mut p2 = proof(5, 11, vec![(6, 60)], vec![(4, 50)]);
        p2.channel = ChannelId::new(8);
        p2.producer_binding = BindingId::new(200);
        let c1 = ConsumerWaitInput {
            channel: ChannelId::new(7),
            binding: BindingId::new(10),
            consumer_fragment: 1,
            behavior: ConsumerWaitBehavior::BlocksUntilComplete,
            producers: vec![ProducerWaitInput {
                channel: ChannelId::new(7),
                binding: BindingId::new(100),
                fragment: 2,
                node_id: 10,
                completion_requirement: CompletionRequirement::ProducerClosed,
            }],
        };
        let c2 = ConsumerWaitInput {
            channel: ChannelId::new(8),
            binding: BindingId::new(11),
            consumer_fragment: 4,
            behavior: ConsumerWaitBehavior::BlocksUntilComplete,
            producers: vec![ProducerWaitInput {
                channel: ChannelId::new(8),
                binding: BindingId::new(200),
                fragment: 5,
                node_id: 11,
                completion_requirement: CompletionRequirement::ProducerClosed,
            }],
        };
        let c = catalog(vec![p1, p2]);
        let err = validate_with_progress(&deps, &edges, &[c1, c2], &c).unwrap_err();
        let DeploymentError::BlockingFeedbackCycle { cycle, .. } = err else {
            panic!("expected cycle");
        };
        assert!(!cycle.is_empty());
    }

    #[test]
    fn multicast_backpressure_edge_closes_cycle() {
        let edges = vec![
            partitioned_edge(1, 4, 21),
            partitioned_edge(1, 3, 22),
            partitioned_edge(4, 2, 20),
            partitioned_edge(3, 5, 24),
            partitioned_edge(5, 2, 23),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 5, ConsumerActivation::BlockingSnapshot, vec![2]);
        let c = catalog(vec![proof(2, 10, vec![(4, 20)], vec![(5, 23)])]);
        let err = validate_with_progress(&deps, &edges, &[consumer], &c).unwrap_err();
        let DeploymentError::BlockingFeedbackCycle { binding, cycle, .. } = err else {
            panic!("expected cycle");
        };
        assert_eq!(
            binding,
            crate::runtime_filter::model::contract::BindingId::new(10)
        );
        assert!(cycle.iter().any(|step| step.contains(
            "--backpressure(frag 1) channel=7 consumer-binding=10 producer-binding=100-->"
        )));
    }

    #[test]
    fn multicast_branch_without_consumer_adds_no_backpressure_edge() {
        let edges = vec![
            partitioned_edge(1, 4, 21),
            partitioned_edge(1, 3, 22),
            partitioned_edge(4, 2, 20),
            partitioned_edge(5, 2, 23),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 5, ConsumerActivation::BlockingSnapshot, vec![2]);
        let c = catalog(vec![proof(2, 10, vec![(4, 20)], vec![(5, 23)])]);
        assert!(validate_with_progress(&deps, &edges, &[consumer], &c).is_ok());
    }

    #[test]
    fn fragment_local_consumer_passes_with_empty_frontier_proof() {
        let edges: Vec<RefinedFragmentEdge> = vec![];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 1, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![], vec![])]);
        assert!(validate_with_progress(&deps, &edges, &[consumer], &c).is_ok());
    }

    #[test]
    fn cycle_detection_is_deterministic_under_input_permutation() {
        let edges_a = vec![
            partitioned_edge(2, 1, 20),
            partitioned_edge(3, 1, 30),
            edge(2, 3),
        ];
        let mut edges_b = edges_a.clone();
        edges_b.reverse();
        let deps_a = ExecutionDependencyGraph::from_fragment_edges(&edges_a).unwrap();
        let deps_b = ExecutionDependencyGraph::from_fragment_edges(&edges_b).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let c = catalog(vec![proof(1, 10, vec![(3, 30)], vec![(2, 20)])]);
        let e_a = validate_with_progress(&deps_a, &edges_a, &[consumer.clone()], &c).unwrap_err();
        let e_b = validate_with_progress(&deps_b, &edges_b, &[consumer], &c).unwrap_err();
        let DeploymentError::BlockingFeedbackCycle { cycle, .. } = &e_a else {
            panic!("expected cycle");
        };
        assert!(!cycle.is_empty());
        assert_eq!(e_a, e_b);
    }

    #[test]
    fn multi_build_frontier_proof_cannot_bypass_blocking_feedback_cycle() {
        let edges = vec![
            partitioned_edge(2, 1, 20),
            partitioned_edge(3, 1, 30),
            partitioned_edge(4, 1, 40),
            edge(2, 4),
        ];
        let deps = ExecutionDependencyGraph::from_fragment_edges(&edges).unwrap();
        let consumer = wait(10, 2, ConsumerActivation::BlockingSnapshot, vec![1]);
        let mut proof = proof(1, 10, vec![(3, 30)], vec![(2, 20)]);
        proof.build_frontier.push(FrontierEdge {
            source_fragment: 4,
            target_exchange_node: 40,
        });
        let catalog = [(
            (
                proof.channel,
                proof.producer_binding,
                proof.producer_fragment,
            ),
            proof,
        )]
        .into_iter()
        .collect();

        assert!(matches!(
            validate_with_progress(&deps, &edges, &[consumer], &catalog),
            Err(DeploymentError::BlockingFeedbackCycle { .. })
        ));
    }
}
