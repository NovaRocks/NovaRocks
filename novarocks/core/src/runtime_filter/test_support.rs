//! Test-only immutable deployment fixtures shared by Backend runtime-filter
//! conformance tests. This module has no Service, router, registry, or global
//! lifecycle state; it constructs a compiler-produced install contract only.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::num::NonZeroU32;

use arrow::datatypes::DataType;

use crate::query_execution::backend::LiveBackendSnapshot;
use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime_filter::deployment::compiler::compile;
use crate::runtime_filter::deployment::{
    RuntimeFilterDeploymentPlan, RuntimeFilterDeploymentPolicy,
};
use crate::runtime_filter::model::contract::{
    BindingId, ChannelId, CoverageWitnessId, NullOrder, PlanFragmentId, SortDirection,
};
use crate::runtime_filter::port::identity::*;
use crate::runtime_filter::port::install::*;
use crate::runtime_filter::port::ordered_bound::RuntimeOrderContract;
use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
use crate::sql::planner::distributed::{
    DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
};
use crate::sql::planner::runtime_filter::{
    contract as sql_contract, coverage::Coverage as SqlCoverage, graph as sql_graph,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterFixtureCoverage {
    AllOf,
    AnyOf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterFixtureProducer {
    binding_id: BindingId,
    witness_id: CoverageWitnessId,
    fragment_id: PlanFragmentId,
    instance_id: UniqueId,
}

impl RuntimeFilterFixtureProducer {
    pub const fn binding_id(&self) -> BindingId {
        self.binding_id
    }

    pub const fn witness_id(&self) -> CoverageWitnessId {
        self.witness_id
    }

    pub const fn fragment_id(&self) -> PlanFragmentId {
        self.fragment_id
    }

    pub const fn instance_id(&self) -> UniqueId {
        self.instance_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterFixtureConsumer {
    binding_id: BindingId,
    fragment_id: PlanFragmentId,
    instance_id: UniqueId,
}

impl RuntimeFilterFixtureConsumer {
    pub const fn binding_id(&self) -> BindingId {
        self.binding_id
    }

    pub const fn fragment_id(&self) -> PlanFragmentId {
        self.fragment_id
    }

    pub const fn instance_id(&self) -> UniqueId {
        self.instance_id
    }
}

#[derive(Clone, Debug)]
pub struct CompiledRuntimeFilterServiceFixture {
    install: RuntimeFilterParticipantInstall,
    channel_id: ChannelId,
    producers: Vec<RuntimeFilterFixtureProducer>,
    consumer: RuntimeFilterFixtureConsumer,
}

impl CompiledRuntimeFilterServiceFixture {
    pub const fn install(&self) -> &RuntimeFilterParticipantInstall {
        &self.install
    }

    pub fn into_install(self) -> RuntimeFilterParticipantInstall {
        self.install
    }

    pub const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    pub fn producers(&self) -> &[RuntimeFilterFixtureProducer] {
        &self.producers
    }

    pub const fn consumer(&self) -> RuntimeFilterFixtureConsumer {
        self.consumer
    }
}

fn producer(
    binding_id: u32,
    witness_id: u32,
    fragment_id: u32,
    instance_id: UniqueId,
) -> RuntimeFilterFixtureProducer {
    RuntimeFilterFixtureProducer {
        binding_id: BindingId::new(binding_id),
        witness_id: CoverageWitnessId::new(witness_id),
        fragment_id: PlanFragmentId::new(fragment_id),
        instance_id,
    }
}

fn consumer(
    binding_id: u32,
    fragment_id: u32,
    instance_id: UniqueId,
) -> RuntimeFilterFixtureConsumer {
    RuntimeFilterFixtureConsumer {
        binding_id: BindingId::new(binding_id),
        fragment_id: PlanFragmentId::new(fragment_id),
        instance_id,
    }
}

fn expression() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(1)),
        data_type: DataType::Int64,
        nullable: false,
    }
}

fn policy(max_bytes: u64, deadline_ms: u64) -> sql_contract::RuntimeFilterPolicyRequirement {
    sql_contract::RuntimeFilterPolicyRequirement {
        max_contribution_bytes: max_bytes,
        max_artifact_bytes: max_bytes,
        deadline_ms,
        max_retries: 1,
    }
}

fn membership_capabilities() -> BTreeSet<sql_contract::ArtifactCapability> {
    BTreeSet::from([
        sql_contract::ArtifactCapability::Membership,
        sql_contract::ArtifactCapability::EmptyDomain,
    ])
}

fn coverage(
    kind: RuntimeFilterFixtureCoverage,
    producers: &[RuntimeFilterFixtureProducer],
) -> SqlCoverage {
    let leaves = producers
        .iter()
        .map(|producer| {
            SqlCoverage::Leaf(sql_contract::CoverageWitnessId::new(
                producer.witness_id.get(),
            ))
        })
        .collect();
    match kind {
        RuntimeFilterFixtureCoverage::AllOf => SqlCoverage::AllOf(leaves),
        RuntimeFilterFixtureCoverage::AnyOf => SqlCoverage::AnyOf(leaves),
    }
}

fn producer_binding(
    channel_id: ChannelId,
    producer: RuntimeFilterFixtureProducer,
    node_id: i32,
    contributions: BTreeSet<sql_contract::ContributionKind>,
    completion: sql_contract::CompletionRequirement,
) -> sql_graph::RuntimeFilterBindingSpec {
    sql_graph::RuntimeFilterBindingSpec {
        binding_id: sql_contract::BindingId::new(producer.binding_id.get()),
        channel_id: sql_contract::ChannelId::new(channel_id.get()),
        coverage_witness_id: Some(sql_contract::CoverageWitnessId::new(
            producer.witness_id.get(),
        )),
        location: sql_graph::PlanLocation {
            fragment_id: sql_contract::PlanFragmentId::new(producer.fragment_id.get()),
            node_id: sql_contract::PlanNodeId::new(node_id),
        },
        expression: expression(),
        apply_point: sql_graph::ApplyPoint::NodeOutput,
        role: sql_graph::RuntimeFilterBindingRole::Producer(sql_graph::ProducerRequirement {
            contribution_kinds: contributions,
            completion_requirement: completion,
            target: sql_graph::ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
        }),
    }
}

fn consumer_binding(
    channel_id: ChannelId,
    consumer: RuntimeFilterFixtureConsumer,
    capabilities: BTreeSet<sql_contract::ArtifactCapability>,
    activation: sql_contract::ConsumerActivation,
) -> sql_graph::RuntimeFilterBindingSpec {
    sql_graph::RuntimeFilterBindingSpec {
        binding_id: sql_contract::BindingId::new(consumer.binding_id.get()),
        channel_id: sql_contract::ChannelId::new(channel_id.get()),
        coverage_witness_id: None,
        location: sql_graph::PlanLocation {
            fragment_id: sql_contract::PlanFragmentId::new(consumer.fragment_id.get()),
            node_id: sql_contract::PlanNodeId::new(30),
        },
        expression: expression(),
        apply_point: sql_graph::ApplyPoint::NodeInput,
        role: sql_graph::RuntimeFilterBindingRole::Consumer(sql_graph::ConsumerRequirement {
            capabilities,
            activation,
            target: sql_graph::ConsumerBindingTarget::SourceBoundary,
        }),
    }
}

fn membership_graph(
    channel_id: ChannelId,
    producers: &[RuntimeFilterFixtureProducer],
    consumer: RuntimeFilterFixtureConsumer,
    coverage_kind: RuntimeFilterFixtureCoverage,
    activation: sql_contract::ConsumerActivation,
) -> sql_graph::RuntimeFilterGraph {
    let contributions = BTreeSet::from([
        sql_contract::ContributionKind::ValueDomainDelta,
        sql_contract::ContributionKind::ProducerClosed,
    ]);
    let capabilities = membership_capabilities();
    let channel_coverage = coverage(coverage_kind, producers);
    let mut graph = sql_graph::RuntimeFilterGraph::default();
    graph
        .insert_channel(sql_graph::RuntimeFilterChannelSpec {
            channel_id: sql_contract::ChannelId::new(channel_id.get()),
            logical_domain: sql_contract::RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: sql_contract::NullSemantics::NeverMatches,
            },
            lifecycle: sql_contract::RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: channel_coverage.clone(),
            terminal_coverage: channel_coverage,
            reduction_requirement: sql_contract::ReductionRequirement::SetUnion,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: capabilities.clone(),
            policy: policy(4096, 1000),
        })
        .expect("insert membership channel");
    for (index, producer) in producers.iter().copied().enumerate() {
        graph
            .insert_binding(producer_binding(
                channel_id,
                producer,
                index as i32 + 1,
                contributions.clone(),
                sql_contract::CompletionRequirement::ProducerClosed,
            ))
            .expect("insert membership producer");
    }
    graph
        .insert_binding(consumer_binding(
            channel_id,
            consumer,
            capabilities,
            activation,
        ))
        .expect("insert membership consumer");
    graph
}

fn final_domain_graph(
    channel_id: ChannelId,
    producers: &[RuntimeFilterFixtureProducer],
    consumer: RuntimeFilterFixtureConsumer,
) -> sql_graph::RuntimeFilterGraph {
    let contributions = BTreeSet::from([
        sql_contract::ContributionKind::FinalDomainShard,
        sql_contract::ContributionKind::ProducerClosed,
    ]);
    let capabilities = membership_capabilities();
    let channel_coverage = coverage(RuntimeFilterFixtureCoverage::AllOf, producers);
    let mut graph = sql_graph::RuntimeFilterGraph::default();
    graph
        .insert_channel(sql_graph::RuntimeFilterChannelSpec {
            channel_id: sql_contract::ChannelId::new(channel_id.get()),
            logical_domain: sql_contract::RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: sql_contract::NullSemantics::NullSafeEqual,
            },
            lifecycle: sql_contract::RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: channel_coverage.clone(),
            terminal_coverage: channel_coverage,
            reduction_requirement: sql_contract::ReductionRequirement::SetUnion,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: capabilities.clone(),
            policy: policy(4096, 1000),
        })
        .expect("insert final-domain channel");
    for (index, producer) in producers.iter().copied().enumerate() {
        graph
            .insert_binding(producer_binding(
                channel_id,
                producer,
                index as i32 + 1,
                contributions.clone(),
                sql_contract::CompletionRequirement::FencedFinalDomain(
                    sql_contract::CompletionFenceKind::CommittedDomainFrozen,
                ),
            ))
            .expect("insert final-domain producer");
    }
    graph
        .insert_binding(consumer_binding(
            channel_id,
            consumer,
            capabilities,
            sql_contract::ConsumerActivation::NonBlockingLive {
                late_apply: sql_contract::LateApplyGranularity::Batch,
            },
        ))
        .expect("insert final-domain consumer");
    graph
}

fn sql_order_contract(contract: &RuntimeOrderContract) -> sql_contract::OrderContract {
    sql_contract::OrderContract {
        keys: contract
            .keys()
            .iter()
            .map(|key| sql_contract::OrderKeyContract {
                data_type: key.data_type().clone(),
                direction: match key.direction() {
                    SortDirection::Ascending => sql_contract::SortDirection::Ascending,
                    SortDirection::Descending => sql_contract::SortDirection::Descending,
                },
                null_order: match key.null_order() {
                    NullOrder::First => sql_contract::NullOrder::First,
                    NullOrder::Last => sql_contract::NullOrder::Last,
                },
            })
            .collect(),
        inclusive: true,
        comparator_digest: sql_contract::ComparatorDigest::new(
            contract.plan_comparator_digest().get(),
        ),
    }
}

fn ordered_graph(
    channel_id: ChannelId,
    producers: &[RuntimeFilterFixtureProducer],
    consumer: RuntimeFilterFixtureConsumer,
    contract: &RuntimeOrderContract,
    topk: Option<NonZeroU32>,
) -> sql_graph::RuntimeFilterGraph {
    let contribution = if topk.is_some() {
        sql_contract::ContributionKind::TopKSummary
    } else {
        sql_contract::ContributionKind::OrderedBoundUpdate
    };
    let contributions =
        BTreeSet::from([contribution, sql_contract::ContributionKind::ProducerClosed]);
    let capabilities = BTreeSet::from([sql_contract::ArtifactCapability::OrderedRange]);
    let channel_coverage = coverage(RuntimeFilterFixtureCoverage::AllOf, producers);
    let reduction_requirement = match topk {
        Some(k) => sql_contract::ReductionRequirement::MergeTopKSummary(
            sql_contract::TopKSummaryRequirement::try_new(k.get()).expect("nonzero TopK fixture"),
        ),
        None => sql_contract::ReductionRequirement::TightenOrderedBound,
    };
    let mut graph = sql_graph::RuntimeFilterGraph::default();
    graph
        .insert_channel(sql_graph::RuntimeFilterChannelSpec {
            channel_id: sql_contract::ChannelId::new(channel_id.get()),
            logical_domain: sql_contract::RuntimeFilterLogicalDomain::OrderedBound(
                sql_order_contract(contract),
            ),
            lifecycle: sql_contract::RuntimeFilterLifecycle::MonotonicUpdates,
            availability_coverage: channel_coverage.clone(),
            terminal_coverage: channel_coverage,
            reduction_requirement,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: capabilities.clone(),
            policy: policy(4096, 1000),
        })
        .expect("insert ordered channel");
    for (index, producer) in producers.iter().copied().enumerate() {
        graph
            .insert_binding(producer_binding(
                channel_id,
                producer,
                index as i32 + 1,
                contributions.clone(),
                sql_contract::CompletionRequirement::ProducerClosed,
            ))
            .expect("insert ordered producer");
    }
    graph
        .insert_binding(consumer_binding(
            channel_id,
            consumer,
            capabilities,
            sql_contract::ConsumerActivation::NonBlockingLive {
                late_apply: sql_contract::LateApplyGranularity::Batch,
            },
        ))
        .expect("insert ordered consumer");
    graph
}

fn placement(
    fragment_id: PlanFragmentId,
    instance_index: usize,
    instance_id: UniqueId,
    backend_idx: usize,
    endpoint: SocketAddr,
) -> FragmentInstancePlacement {
    FragmentInstancePlacement {
        fragment_id: fragment_id.get(),
        instance_index,
        finst_id: instance_id,
        backend_idx,
        endpoint: RuntimeEndpoint::from_socket_addr(endpoint),
        scan_ranges: BTreeMap::new(),
        connector_splits: BTreeMap::new(),
        destinations: Vec::new(),
        per_exch_num_senders: BTreeMap::new(),
    }
}

fn compile_service_fixture(
    graph: sql_graph::RuntimeFilterGraph,
    channel_id: ChannelId,
    producers: Vec<RuntimeFilterFixtureProducer>,
    consumer: RuntimeFilterFixtureConsumer,
    endpoint: SocketAddr,
    backend_idx: usize,
    epoch: DeploymentEpoch,
) -> CompiledRuntimeFilterServiceFixture {
    let mut by_fragment = BTreeMap::<u32, Vec<FragmentInstancePlacement>>::new();
    for (fragment_id, instance_id) in producers
        .iter()
        .map(|producer| (producer.fragment_id, producer.instance_id))
        .chain(std::iter::once((
            consumer.fragment_id,
            consumer.instance_id,
        )))
    {
        let placements = by_fragment.entry(fragment_id.get()).or_default();
        if placements
            .iter()
            .all(|placement| placement.finst_id != instance_id)
        {
            placements.push(placement(
                fragment_id,
                placements.len(),
                instance_id,
                backend_idx,
                endpoint,
            ));
        }
    }
    let scheduling = SchedulingPlan {
        root_fragment_id: consumer.fragment_id.get(),
        by_fragment,
        root_finst_id: consumer.instance_id,
        root_backend_idx: backend_idx,
    };
    let edges = producers
        .iter()
        .filter(|producer| producer.fragment_id != consumer.fragment_id)
        .enumerate()
        .map(|(index, producer)| FragmentEdge {
            source_fragment_id: producer.fragment_id.get(),
            target_fragment_id: consumer.fragment_id.get(),
            target_exchange_node_id: index as i32 + 1,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        })
        .collect::<Vec<_>>();
    let backends = LiveBackendSnapshot::new(vec![(backend_idx, endpoint)]);
    let mut plan = compile(
        &graph,
        &scheduling,
        &edges,
        &backends,
        &RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(16 * 1024),
            replica_redundancy: 1,
            materialization: MaterializationPolicy::for_test(),
        },
        epoch,
    )
    .expect("runtime-filter fixture graph compiles against its sealed placement");
    let participant = crate::runtime_filter::deployment::participant_id_for_backend(backend_idx)
        .expect("fixture backend has a representable participant identity");
    let install = RuntimeFilterParticipantInstall::new(
        plan.install_views
            .remove(&participant)
            .expect("compiler projects fixture Core install"),
        plan.routing_shards
            .remove(&participant)
            .expect("compiler projects fixture routing shard"),
    );
    CompiledRuntimeFilterServiceFixture {
        install,
        channel_id,
        producers,
        consumer,
    }
}

fn m4_producers() -> Vec<RuntimeFilterFixtureProducer> {
    vec![
        producer(10, 101, 1, UniqueId::new(94, 10)),
        producer(20, 102, 2, UniqueId::new(94, 20)),
    ]
}

fn m4_consumer() -> RuntimeFilterFixtureConsumer {
    consumer(30, 3, UniqueId::new(94, 30))
}

fn m4_endpoint() -> SocketAddr {
    "127.0.0.1:9060".parse().expect("valid M4 fixture endpoint")
}

pub fn compiled_membership_service_fixture(
    coverage_kind: RuntimeFilterFixtureCoverage,
) -> CompiledRuntimeFilterServiceFixture {
    let channel_id = ChannelId::new(1);
    let producers = m4_producers();
    let consumer = m4_consumer();
    let graph = membership_graph(
        channel_id,
        &producers,
        consumer,
        coverage_kind,
        sql_contract::ConsumerActivation::BlockingSnapshot,
    );
    compile_service_fixture(
        graph,
        channel_id,
        producers,
        consumer,
        m4_endpoint(),
        0,
        DeploymentEpoch::new(1),
    )
}

pub fn compiled_live_final_domain_fixture() -> CompiledRuntimeFilterServiceFixture {
    let channel_id = ChannelId::new(1);
    let producers = m4_producers();
    let consumer = m4_consumer();
    let graph = final_domain_graph(channel_id, &producers, consumer);
    compile_service_fixture(
        graph,
        channel_id,
        producers,
        consumer,
        m4_endpoint(),
        0,
        DeploymentEpoch::new(1),
    )
}

pub fn compiled_fenced_final_fixture() -> CompiledRuntimeFilterServiceFixture {
    let channel_id = ChannelId::new(1);
    // Preserve the legacy colocated aggregate fixture shape: the producer and
    // consumer are two bindings on the same fragment instance. Backend tests
    // consume these runtime facts instead of reconstructing planner identity.
    let instance_id = UniqueId::new(70, 10);
    let producers = vec![producer(10, 101, 0, instance_id)];
    let consumer = consumer(30, 0, instance_id);
    let graph = final_domain_graph(channel_id, &producers, consumer);
    compile_service_fixture(
        graph,
        channel_id,
        producers,
        consumer,
        m4_endpoint(),
        0,
        DeploymentEpoch::new(1),
    )
}

pub fn compiled_ordered_bound_fixture(
    contract: &RuntimeOrderContract,
) -> CompiledRuntimeFilterServiceFixture {
    let channel_id = ChannelId::new(1);
    let producers = vec![m4_producers()[0]];
    let consumer = m4_consumer();
    let graph = ordered_graph(channel_id, &producers, consumer, contract, None);
    compile_service_fixture(
        graph,
        channel_id,
        producers,
        consumer,
        m4_endpoint(),
        0,
        DeploymentEpoch::new(1),
    )
}

pub fn compiled_topk_fixture(
    contract: &RuntimeOrderContract,
    k: NonZeroU32,
) -> CompiledRuntimeFilterServiceFixture {
    let channel_id = ChannelId::new(1);
    let producers = m4_producers();
    let consumer = m4_consumer();
    let graph = ordered_graph(channel_id, &producers, consumer, contract, Some(k));
    compile_service_fixture(
        graph,
        channel_id,
        producers,
        consumer,
        m4_endpoint(),
        0,
        DeploymentEpoch::new(1),
    )
}

pub fn compiled_three_backend_all_of_plan() -> RuntimeFilterDeploymentPlan {
    let channel_id = sql_contract::ChannelId::new(5);
    let producer_binding = sql_contract::BindingId::new(10);
    let consumer_binding = sql_contract::BindingId::new(11);
    let witness = sql_contract::CoverageWitnessId::new(1);
    let channel_coverage = SqlCoverage::AllOf(vec![SqlCoverage::Leaf(witness)]);
    let contributions = BTreeSet::from([
        sql_contract::ContributionKind::ValueDomainDelta,
        sql_contract::ContributionKind::ProducerClosed,
    ]);
    let capabilities = membership_capabilities();
    let mut graph = sql_graph::RuntimeFilterGraph::default();
    graph
        .insert_channel(sql_graph::RuntimeFilterChannelSpec {
            channel_id,
            logical_domain: sql_contract::RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: sql_contract::NullSemantics::NeverMatches,
            },
            lifecycle: sql_contract::RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: channel_coverage.clone(),
            terminal_coverage: channel_coverage,
            reduction_requirement: sql_contract::ReductionRequirement::SetUnion,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: capabilities.clone(),
            policy: policy(1024, 100),
        })
        .expect("insert three-backend channel");
    graph
        .insert_binding(sql_graph::RuntimeFilterBindingSpec {
            binding_id: producer_binding,
            channel_id,
            coverage_witness_id: Some(witness),
            location: sql_graph::PlanLocation {
                fragment_id: sql_contract::PlanFragmentId::new(2),
                node_id: sql_contract::PlanNodeId::new(1),
            },
            expression: expression(),
            apply_point: sql_graph::ApplyPoint::NodeOutput,
            role: sql_graph::RuntimeFilterBindingRole::Producer(sql_graph::ProducerRequirement {
                contribution_kinds: contributions,
                completion_requirement: sql_contract::CompletionRequirement::ProducerClosed,
                target: sql_graph::ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
            }),
        })
        .expect("insert three-backend producer");
    graph
        .insert_binding(sql_graph::RuntimeFilterBindingSpec {
            binding_id: consumer_binding,
            channel_id,
            coverage_witness_id: None,
            location: sql_graph::PlanLocation {
                fragment_id: sql_contract::PlanFragmentId::new(1),
                node_id: sql_contract::PlanNodeId::new(2),
            },
            expression: expression(),
            apply_point: sql_graph::ApplyPoint::NodeInput,
            role: sql_graph::RuntimeFilterBindingRole::Consumer(sql_graph::ConsumerRequirement {
                capabilities,
                activation: sql_contract::ConsumerActivation::BlockingSnapshot,
                target: sql_graph::ConsumerBindingTarget::SourceBoundary,
            }),
        })
        .expect("insert three-backend consumer");

    let placement = |fragment_id: u32,
                     instance_index: usize,
                     backend_idx: usize,
                     finst_id: UniqueId,
                     endpoint: &str| FragmentInstancePlacement {
        fragment_id,
        instance_index,
        finst_id,
        backend_idx,
        endpoint: RuntimeEndpoint::from_socket_addr(endpoint.parse().unwrap()),
        scan_ranges: BTreeMap::new(),
        connector_splits: BTreeMap::new(),
        destinations: Vec::new(),
        per_exch_num_senders: BTreeMap::new(),
    };
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
                    placement(2, 0, 2, UniqueId::new(1, 3), "10.0.0.2:9060"),
                    placement(2, 1, 7, UniqueId::new(1, 4), "10.0.0.7:9060"),
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
    .expect("three-backend fixture compiles")
}
