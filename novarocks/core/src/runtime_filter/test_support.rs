//! Test-only immutable deployment fixtures shared by Backend runtime-filter
//! conformance tests. This module has no Service, router, registry, or global
//! lifecycle state; it constructs a compiler-produced install contract only.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::DataType;

use crate::query_execution::backend::LiveBackendSnapshot;
use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime_filter::deployment::compiler::compile;
use crate::runtime_filter::deployment::{
    RuntimeFilterDeploymentPlan, RuntimeFilterDeploymentPolicy,
};
use crate::runtime_filter::model::contract::*;
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::model::graph::{
    ApplyPoint, ConsumerRequirement, PlanLocation, ProducerRequirement, RuntimeFilterBindingRole,
    RuntimeFilterBindingSpec, RuntimeFilterChannelSpec, RuntimeFilterGraph,
};
use crate::runtime_filter::port::identity::*;
use crate::runtime_filter::port::install::*;
use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
use crate::sql::planner::distributed::{
    DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
};
use novarocks_types::UniqueId;

/// RFO-1 test-only session. It proves that Core operator tests depend only on
/// the execution capability contract, not on a Core-owned installed Service.
/// Consumer bindings fail open explicitly; ownership and delivery behavior is
/// covered by Backend participant tests.
pub struct FailOpenRuntimeFilterSession;

struct FailOpenProducer;

impl novarocks_execution::runtime_filter::RuntimeFilterProducer for FailOpenProducer {
    fn max_contribution_bytes(&self) -> usize {
        1024 * 1024
    }

    fn submit(
        &self,
        _partition: novarocks_execution::runtime_filter::PartitionId,
        _sequence: novarocks_execution::runtime_filter::ProducerSequence,
        _contribution: novarocks_execution::runtime_filter::RuntimeFilterContribution,
    ) -> Result<
        novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome,
        novarocks_execution::runtime_filter::RuntimeFilterContractViolation,
    > {
        Ok(novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome::Applied)
    }

    fn close_partition(
        &self,
        _partition: novarocks_execution::runtime_filter::PartitionId,
        _terminal: novarocks_execution::runtime_filter::ProducerSequence,
    ) -> Result<
        novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome,
        novarocks_execution::runtime_filter::RuntimeFilterContractViolation,
    > {
        Ok(novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome::Completed)
    }

    fn fail(
        &self,
        _reason: novarocks_execution::runtime_filter::RuntimeFilterProducerFailure,
    ) -> Result<
        novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome,
        novarocks_execution::runtime_filter::RuntimeFilterContractViolation,
    > {
        Ok(novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome::TerminalNoop)
    }
}

impl novarocks_execution::runtime_filter::RuntimeFilterSession for FailOpenRuntimeFilterSession {
    fn open_producer(
        &self,
        request: novarocks_execution::runtime_filter::RuntimeFilterProducerOpenRequest,
    ) -> Result<
        novarocks_execution::runtime_filter::RuntimeFilterBindOutcome<
            novarocks_execution::runtime_filter::RuntimeFilterProducerHandle,
        >,
        novarocks_execution::runtime_filter::RuntimeFilterContractViolation,
    > {
        if request.local_partition_count() == 0 {
            return Err(novarocks_execution::runtime_filter::RuntimeFilterContractViolation::new(
                novarocks_execution::runtime_filter::RuntimeFilterContractViolationKind::InvalidPartitionCount,
                "test runtime-filter producer requires a positive partition count",
            ));
        }
        Ok(
            novarocks_execution::runtime_filter::RuntimeFilterBindOutcome::Bound(
                std::sync::Arc::new(FailOpenProducer),
            ),
        )
    }

    fn subscribe(
        &self,
        _request: novarocks_execution::runtime_filter::RuntimeFilterSubscriptionRequest,
    ) -> Result<
        novarocks_execution::runtime_filter::RuntimeFilterBindOutcome<
            novarocks_execution::runtime_filter::RuntimeFilterSubscriptionHandle,
        >,
        novarocks_execution::runtime_filter::RuntimeFilterContractViolation,
    > {
        Ok(
            novarocks_execution::runtime_filter::RuntimeFilterBindOutcome::Unavailable(
                novarocks_execution::runtime_filter::UnavailableReason::IncompleteCoverage,
            ),
        )
    }

    fn open_final_domain_completion(
        &self,
        _request: novarocks_execution::runtime_filter::RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        novarocks_execution::runtime_filter::RuntimeFilterBindOutcome<
            novarocks_execution::runtime_filter::RuntimeFilterFinalDomainCompletionHandle,
        >,
        novarocks_execution::runtime_filter::RuntimeFilterContractViolation,
    > {
        Ok(
            novarocks_execution::runtime_filter::RuntimeFilterBindOutcome::Unavailable(
                novarocks_execution::runtime_filter::UnavailableReason::IncompleteCoverage,
            ),
        )
    }
}

pub fn fail_open_session() -> novarocks_execution::runtime_filter::RuntimeFilterSessionRef {
    std::sync::Arc::new(FailOpenRuntimeFilterSession)
}

pub fn compiled_three_backend_all_of_plan() -> RuntimeFilterDeploymentPlan {
    let channel_id = ChannelId::new(5);
    let producer_binding = BindingId::new(10);
    let consumer_binding = BindingId::new(11);
    let witness = CoverageWitnessId::new(1);
    let coverage = Coverage::AllOf(vec![Coverage::Leaf(witness)]);
    let contributions = BTreeSet::from([
        ContributionKind::ValueDomainDelta,
        ContributionKind::ProducerClosed,
    ]);
    let capabilities = BTreeSet::from([
        ArtifactCapability::Membership,
        ArtifactCapability::EmptyDomain,
    ]);
    let expression = TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(1)),
        data_type: DataType::Int64,
        nullable: false,
    };
    let mut graph = RuntimeFilterGraph::default();
    graph
        .insert_channel(RuntimeFilterChannelSpec {
            channel_id,
            logical_domain: RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: coverage.clone(),
            terminal_coverage: coverage,
            reduction_requirement: ReductionRequirement::SetUnion,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: capabilities.clone(),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 1,
            },
        })
        .unwrap();
    graph
        .insert_binding(RuntimeFilterBindingSpec {
            binding_id: producer_binding,
            channel_id,
            coverage_witness_id: Some(witness),
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(2),
                node_id: PlanNodeId::new(1),
            },
            expression: expression.clone(),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                contribution_kinds: contributions,
                completion_requirement: CompletionRequirement::ProducerClosed,
                target: crate::runtime_filter::model::graph::ProducerBindingTarget::JoinBuildKey {
                    ordinal: 0,
                },
            }),
        })
        .unwrap();
    graph
        .insert_binding(RuntimeFilterBindingSpec {
            binding_id: consumer_binding,
            channel_id,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(1),
                node_id: PlanNodeId::new(2),
            },
            expression,
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                capabilities,
                activation: ConsumerActivation::BlockingSnapshot,
                target: crate::runtime_filter::model::graph::ConsumerBindingTarget::SourceBoundary,
            }),
        })
        .unwrap();
    let placement = |fragment_id, instance_index, backend_idx, finst_id, endpoint: &str| {
        FragmentInstancePlacement {
            fragment_id,
            instance_index,
            finst_id,
            backend_idx,
            endpoint: RuntimeEndpoint::from_socket_addr(endpoint.parse().unwrap()),
            scan_ranges: BTreeMap::new(),
            connector_splits: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        }
    };
    let local_producer = UniqueId::new(1, 3);
    let remote_producer = UniqueId::new(1, 4);
    let scheduling = SchedulingPlan {
        root_fragment_id: 1,
        by_fragment: BTreeMap::from([
            (
                1,
                vec![
                    placement(1, 0, 2, UniqueId::new(1, 1), "10.0.0.2:9060"),
                    placement(1, 1, 11, UniqueId::new(1, 2), "10.0.0.11:9060"),
                ],
            ),
            (
                2,
                vec![
                    placement(2, 0, 2, local_producer, "10.0.0.2:9060"),
                    placement(2, 1, 7, remote_producer, "10.0.0.7:9060"),
                ],
            ),
        ]),
        root_finst_id: UniqueId::new(1, 1),
        root_backend_idx: 2,
    };
    let edges = vec![FragmentEdge {
        source_fragment_id: 2,
        target_fragment_id: 1,
        target_exchange_node_id: 1,
        output_partition: DataPartition::unpartitioned(),
        stream_kind: FragmentStreamKind::Gather,
        edge_kind: FragmentEdgeKind::Stream,
        output_slot_ids: Vec::new(),
    }];
    let backends = LiveBackendSnapshot::new(vec![
        (2, "10.0.0.2:9060".parse().unwrap()),
        (7, "10.0.0.7:9060".parse().unwrap()),
        (11, "10.0.0.11:9060".parse().unwrap()),
    ]);
    compile(
        &graph,
        &scheduling,
        &edges,
        &backends,
        &RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(8192),
            replica_redundancy: 2,
            materialization: MaterializationPolicy::for_test(),
        },
        DeploymentEpoch::new(9),
    )
    .unwrap()
}
