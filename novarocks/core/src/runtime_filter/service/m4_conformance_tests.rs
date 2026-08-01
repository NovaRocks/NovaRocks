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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::datatypes::DataType;

use crate::common::types::UniqueId;
use crate::query_execution::backend::LiveBackendSnapshot;
use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime_filter::deployment::RuntimeFilterDeploymentPolicy;
use crate::runtime_filter::deployment::compiler;
use crate::runtime_filter::materializer::codec::{
    ArtifactDecodeExpectations, decode_leaf, encode_physical_leaf,
};
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
    ConsumerActivation, ContributionKind, CoverageWitnessId, LateApplyGranularity, NullOrder,
    NullSemantics, OrderContract, OrderKeyContract, PlanFragmentId, PlanNodeId,
    ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
    RuntimeFilterPolicyRequirement, SortDirection, TopKSummaryRequirement,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::model::graph::{
    ApplyPoint, ConsumerRequirement, PlanLocation, ProducerRequirement, RuntimeFilterBindingRole,
    RuntimeFilterBindingSpec, RuntimeFilterChannelSpec, RuntimeFilterGraph,
};
use crate::runtime_filter::port::artifact::{
    ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
    PhysicalArtifact,
};
use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
use crate::runtime_filter::port::final_domain::{
    CollectingFinalDomainTestIssuer, FinalDomainTestIssuerTransition, FrozenFinalDomainTestIssuer,
};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence, ProducerStreamId,
    RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::install::{
    MaterializationPolicy, RuntimeFilterCoreBudget, RuntimeFilterParticipantInstall,
};
use crate::runtime_filter::port::ordered_bound::{
    COMPARATOR_ALGORITHM_VERSION, OrderedBoundUpdate, OrderedScalar, OrderedTuple,
    RuntimeOrderContract, comparator_digest_for_test,
};
use crate::runtime_filter::port::producer::{
    FinalDomainProducerAdapter, InstallOutcome, OrderedBoundProducerAdapter, ProducerAdapter,
    ProducerHandle, ProducerPortKind, RuntimeContractViolation, RuntimeContractViolationKind,
    SubmitOutcome, TopKSummaryProducerAdapter,
};
use crate::runtime_filter::port::subscription::{
    BlockingSnapshotSubscription, LivePollOutcome, LiveTerminal, NonBlockingLiveSubscription,
    SubscriptionHandle, SubscriptionKind, UnavailableReason,
};
use crate::runtime_filter::port::support::{
    ArtifactRetainedBudget, MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
};
use crate::runtime_filter::port::topk_summary::{RuntimeTopKSummaryContract, TopKSummary};
use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};
use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
use crate::sql::planner::distributed::{
    DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
};

use super::RuntimeFilterService;
use super::memory::MemTrackerMemoryAccount;

const CHANNEL: ChannelId = ChannelId::new(1);
const PRODUCER_A: BindingId = BindingId::new(10);
const PRODUCER_B: BindingId = BindingId::new(20);
const CONSUMER: BindingId = BindingId::new(30);
const WITNESS_A: CoverageWitnessId = CoverageWitnessId::new(101);
const WITNESS_B: CoverageWitnessId = CoverageWitnessId::new(102);
const PRODUCER_FRAGMENT_A: PlanFragmentId = PlanFragmentId::new(1);
const PRODUCER_FRAGMENT_B: PlanFragmentId = PlanFragmentId::new(2);
const CONSUMER_FRAGMENT: PlanFragmentId = PlanFragmentId::new(3);
const INSTANCE_A: UniqueId = UniqueId::new(94, 10);
const INSTANCE_B: UniqueId = UniqueId::new(94, 20);
const CONSUMER_INSTANCE: UniqueId = UniqueId::new(94, 30);
const PARTICIPANT: RuntimeFilterParticipantId = RuntimeFilterParticipantId::new(1);

fn fixture_backend_idx() -> usize {
    usize::try_from(PARTICIPANT.get() - 1).expect("fixture participant fits backend identity")
}

struct ProducerFixture {
    binding: BindingId,
    witness: CoverageWitnessId,
    fragment: PlanFragmentId,
    instance: UniqueId,
}

struct MembershipHarness {
    service: Arc<RuntimeFilterService>,
    blocking: Arc<dyn BlockingSnapshotSubscription>,
}

struct MembershipProducer {
    port: Arc<dyn ProducerAdapter>,
}

impl MembershipProducer {
    fn submit_values(
        &self,
        partition: u32,
        sequence: u64,
        values: impl IntoIterator<Item = i64>,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.port.submit(
            PartitionId::new(partition),
            ProducerSequence::new(sequence),
            ValueDomainDelta::new(MembershipValues::int64(values), false),
        )
    }

    fn close(
        &self,
        partition: u32,
        terminal_sequence: u64,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.port.close_partition(
            PartitionId::new(partition),
            ProducerSequence::new(terminal_sequence),
        )
    }
}

impl MembershipHarness {
    fn producer(&self, binding: BindingId, instance: UniqueId) -> MembershipProducer {
        let ProducerHandle::Membership(port) = self
            .service
            .open_producer(binding, instance, 1, ProducerPortKind::Membership)
            .expect("compiler-installed producer is authorized")
        else {
            panic!("membership graph must install only the Membership producer port")
        };
        MembershipProducer { port }
    }
}

struct DeterministicClock(Instant);

impl RuntimeFilterClock for DeterministicClock {
    fn now(&self) -> Instant {
        self.0
    }
}

#[derive(Default)]
struct RecordingEvents(Mutex<Vec<RuntimeFilterEvent>>);

impl RuntimeFilterEventSink for RecordingEvents {
    fn record(&self, event: RuntimeFilterEvent) {
        self.0.lock().unwrap().push(event);
    }
}

fn producer_fixtures() -> [ProducerFixture; 2] {
    [
        ProducerFixture {
            binding: PRODUCER_A,
            witness: WITNESS_A,
            fragment: PRODUCER_FRAGMENT_A,
            instance: INSTANCE_A,
        },
        ProducerFixture {
            binding: PRODUCER_B,
            witness: WITNESS_B,
            fragment: PRODUCER_FRAGMENT_B,
            instance: INSTANCE_B,
        },
    ]
}

fn expression() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(1)),
        data_type: DataType::Int64,
        nullable: false,
    }
}

fn membership_graph(
    coverage: Coverage,
    producers: &[ProducerFixture],
    activation: ConsumerActivation,
) -> RuntimeFilterGraph {
    let capabilities = BTreeSet::from([
        ArtifactCapability::Membership,
        ArtifactCapability::EmptyDomain,
    ]);
    let contributions = BTreeSet::from([
        ContributionKind::ValueDomainDelta,
        ContributionKind::ProducerClosed,
    ]);
    let mut graph = RuntimeFilterGraph::default();
    graph
        .insert_channel(RuntimeFilterChannelSpec {
            channel_id: CHANNEL,
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
                max_contribution_bytes: 4096,
                max_artifact_bytes: 4096,
                deadline_ms: 1000,
                max_retries: 1,
            },
        })
        .unwrap();
    for (index, producer) in producers.iter().enumerate() {
        graph
            .insert_binding(RuntimeFilterBindingSpec {
                binding_id: producer.binding,
                channel_id: CHANNEL,
                coverage_witness_id: Some(producer.witness),
                location: PlanLocation {
                    fragment_id: producer.fragment,
                    node_id: PlanNodeId::new(index as i32 + 1),
                },
                expression: expression(),
                apply_point: ApplyPoint::NodeOutput,
                role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                    contribution_kinds: contributions.clone(),
                    completion_requirement: CompletionRequirement::ProducerClosed,
                    target:
                        crate::runtime_filter::model::graph::ProducerBindingTarget::JoinBuildKey {
                            ordinal: 0,
                        },
                }),
            })
            .unwrap();
    }
    graph
        .insert_binding(RuntimeFilterBindingSpec {
            binding_id: CONSUMER,
            channel_id: CHANNEL,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: CONSUMER_FRAGMENT,
                node_id: PlanNodeId::new(30),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                capabilities,
                activation,
                target: crate::runtime_filter::model::graph::ConsumerBindingTarget::SourceBoundary,
            }),
        })
        .unwrap();
    graph
        .validate()
        .expect("M4 fixture graph must pass RFD-1 validation before compilation");
    graph
}

fn aggregate_graph(producers: &[ProducerFixture]) -> RuntimeFilterGraph {
    let capabilities = BTreeSet::from([
        ArtifactCapability::Membership,
        ArtifactCapability::EmptyDomain,
    ]);
    let contributions = BTreeSet::from([
        ContributionKind::FinalDomainShard,
        ContributionKind::ProducerClosed,
    ]);
    let coverage = Coverage::AllOf(
        producers
            .iter()
            .map(|producer| Coverage::Leaf(producer.witness))
            .collect(),
    );
    let mut graph = RuntimeFilterGraph::default();
    graph
        .insert_channel(RuntimeFilterChannelSpec {
            channel_id: CHANNEL,
            logical_domain: RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NullSafeEqual,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: coverage.clone(),
            terminal_coverage: coverage,
            reduction_requirement: ReductionRequirement::SetUnion,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: capabilities.clone(),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 4096,
                max_artifact_bytes: 4096,
                deadline_ms: 1000,
                max_retries: 1,
            },
        })
        .unwrap();
    for (index, producer) in producers.iter().enumerate() {
        graph
            .insert_binding(RuntimeFilterBindingSpec {
                binding_id: producer.binding,
                channel_id: CHANNEL,
                coverage_witness_id: Some(producer.witness),
                location: PlanLocation {
                    fragment_id: producer.fragment,
                    node_id: PlanNodeId::new(index as i32 + 1),
                },
                expression: expression(),
                apply_point: ApplyPoint::NodeOutput,
                role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                    contribution_kinds: contributions.clone(),
                    completion_requirement: CompletionRequirement::FencedFinalDomain(
                        CompletionFenceKind::CommittedDomainFrozen,
                    ),
                    target:
                        crate::runtime_filter::model::graph::ProducerBindingTarget::JoinBuildKey {
                            ordinal: 0,
                        },
                }),
            })
            .unwrap();
    }
    graph
        .insert_binding(RuntimeFilterBindingSpec {
            binding_id: CONSUMER,
            channel_id: CHANNEL,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: CONSUMER_FRAGMENT,
                node_id: PlanNodeId::new(30),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                capabilities,
                activation: ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
                target: crate::runtime_filter::model::graph::ConsumerBindingTarget::SourceBoundary,
            }),
        })
        .unwrap();
    graph
        .validate()
        .expect("Aggregate fixture graph must pass RFD-1 validation before compilation");
    graph
}

fn placement(
    fragment: PlanFragmentId,
    instance_index: usize,
    instance: UniqueId,
    endpoint: SocketAddr,
) -> FragmentInstancePlacement {
    FragmentInstancePlacement {
        fragment_id: fragment.get(),
        instance_index,
        finst_id: instance,
        backend_idx: fixture_backend_idx(),
        endpoint: RuntimeEndpoint::from_socket_addr(endpoint),
        scan_ranges: BTreeMap::new(),
        connector_splits: BTreeMap::new(),
        destinations: Vec::new(),
        per_exch_num_senders: BTreeMap::new(),
    }
}

fn scheduling_plan(producers: &[ProducerFixture]) -> SchedulingPlan {
    let endpoint = fixture_endpoint();
    let mut by_fragment = producers
        .iter()
        .map(|producer| {
            (
                producer.fragment.get(),
                vec![placement(producer.fragment, 0, producer.instance, endpoint)],
            )
        })
        .collect::<BTreeMap<_, _>>();
    by_fragment.insert(
        CONSUMER_FRAGMENT.get(),
        vec![placement(CONSUMER_FRAGMENT, 0, CONSUMER_INSTANCE, endpoint)],
    );
    SchedulingPlan {
        root_fragment_id: CONSUMER_FRAGMENT.get(),
        by_fragment,
        root_finst_id: CONSUMER_INSTANCE,
        root_backend_idx: fixture_backend_idx(),
    }
}

fn fixture_endpoint() -> SocketAddr {
    "127.0.0.1:9060".parse().unwrap()
}

fn fragment_edges(producers: &[ProducerFixture]) -> Vec<FragmentEdge> {
    producers
        .iter()
        .enumerate()
        .map(|(index, producer)| FragmentEdge {
            source_fragment_id: producer.fragment.get(),
            target_fragment_id: CONSUMER_FRAGMENT.get(),
            target_exchange_node_id: index as i32 + 1,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        })
        .collect()
}

fn compile_participant_install(
    graph: &RuntimeFilterGraph,
    scheduling: &SchedulingPlan,
    edges: &[FragmentEdge],
    participant: RuntimeFilterParticipantId,
) -> RuntimeFilterParticipantInstall {
    let backends = LiveBackendSnapshot::from_endpoints(vec![fixture_endpoint()]);
    let policy = RuntimeFilterDeploymentPolicy {
        core_budget: RuntimeFilterCoreBudget::new(16 * 1024),
        replica_redundancy: backends.entries().len() as u32,
        materialization: MaterializationPolicy::for_test(),
    };
    let mut plan = compiler::compile(
        graph,
        scheduling,
        edges,
        &backends,
        &policy,
        DeploymentEpoch::new(1),
    )
    .expect("valid graph and live placement must compile");
    let core_view = plan
        .install_views
        .remove(&participant)
        .expect("compiler must project the colocated participant install view");
    let routing_shard = plan
        .routing_shards
        .remove(&participant)
        .expect("compiler must project the matching participant routing shard");
    RuntimeFilterParticipantInstall::new(core_view, routing_shard)
}

fn install_service(install: RuntimeFilterParticipantInstall) -> Arc<RuntimeFilterService> {
    install_service_with_memory(
        install,
        MemTrackerMemoryAccount::new_root_for_test("m4-conformance"),
    )
}

fn install_service_with_memory(
    install: RuntimeFilterParticipantInstall,
    memory: Arc<dyn RuntimeFilterMemoryAccount>,
) -> Arc<RuntimeFilterService> {
    let service = Arc::new(RuntimeFilterService::new_with_dependencies(
        UniqueId::new(94, 0),
        Arc::new(DeterministicClock(Instant::now())),
        Arc::new(RecordingEvents::default()),
        memory,
    ));
    assert_eq!(service.install(install).unwrap(), InstallOutcome::Installed);
    service
}

fn join_harness(coverage: Coverage) -> MembershipHarness {
    let producers = producer_fixtures();
    let graph = membership_graph(coverage, &producers, ConsumerActivation::BlockingSnapshot);
    let scheduling = scheduling_plan(&producers);
    let edges = fragment_edges(&producers);
    let service = install_service(compile_participant_install(
        &graph,
        &scheduling,
        &edges,
        PARTICIPANT,
    ));
    for producer in &producers {
        let ProducerHandle::Membership(_) = service
            .open_producer(
                producer.binding,
                producer.instance,
                1,
                ProducerPortKind::Membership,
            )
            .expect("all scheduled producer instances open before execution")
        else {
            panic!("membership graph must install only Membership producer ports")
        };
    }
    let SubscriptionHandle::Blocking(blocking) = service
        .subscribe(
            CONSUMER,
            CONSUMER_INSTANCE,
            SubscriptionKind::BlockingSnapshot,
        )
        .expect("compiler-installed blocking consumer is authorized")
    else {
        panic!("blocking graph consumer must install only BlockingSnapshot")
    };
    MembershipHarness { service, blocking }
}

fn join_allof_harness() -> MembershipHarness {
    join_harness(Coverage::AllOf(vec![
        Coverage::Leaf(WITNESS_A),
        Coverage::Leaf(WITNESS_B),
    ]))
}

fn join_anyof_harness() -> MembershipHarness {
    join_harness(Coverage::AnyOf(vec![
        Coverage::Leaf(WITNESS_A),
        Coverage::Leaf(WITNESS_B),
    ]))
}

fn publish_membership(
    harness: &MembershipHarness,
    binding: BindingId,
    instance: UniqueId,
    values: &[i64],
) {
    let producer = harness.producer(binding, instance);
    producer
        .submit_values(0, 0, values.iter().copied())
        .unwrap();
    producer.close(0, 1).unwrap();
}

fn membership_payload(artifact: &PhysicalArtifact) -> &[u8] {
    let bytes = artifact.canonical_bytes();
    assert_eq!(&bytes[..4], b"NRFL");
    let schema_len = u16::from_be_bytes(bytes[39..41].try_into().unwrap()) as usize;
    let mut cursor = 41 + schema_len;
    assert_eq!(
        LogicalVersion::new(u64::from_be_bytes(
            bytes[cursor..cursor + 8].try_into().unwrap()
        )),
        artifact.version()
    );
    cursor += 8;
    let flags = bytes[cursor];
    assert_eq!(flags & 1 != 0, artifact.contains_null());
    cursor += 1;
    assert_eq!(bytes[cursor], 0, "membership ValueSet has no hash contract");
    cursor += 1;
    let payload_len = u64::from_be_bytes(bytes[cursor..cursor + 8].try_into().unwrap()) as usize;
    cursor += 8;
    assert_eq!(cursor + payload_len, bytes.len());
    &bytes[cursor..]
}

fn assert_membership_values(bundle: &ArtifactBundle, expected: &[i64]) {
    let [(ArtifactKind::ValueSet, artifact)] = bundle.artifacts() else {
        panic!("non-empty Int64 membership must publish one ValueSet leaf")
    };
    let payload = membership_payload(artifact);
    assert_eq!(payload[0], 5, "canonical membership payload must be Int64");
    let count = u64::from_be_bytes(payload[1..9].try_into().unwrap()) as usize;
    assert_eq!(payload.len(), 9 + count * 8);
    let values = payload[9..]
        .chunks_exact(8)
        .map(|bytes| i64::from_be_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(values, expected);
}

fn membership_profile() -> ConsumerArtifactProfile {
    ConsumerArtifactProfile::new(
        BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
        None,
    )
    .unwrap()
}

fn assert_fixture_remote_equivalent(local: &ArtifactBundle) {
    let [(kind, local_leaf)] = local.artifacts() else {
        panic!("Join fixture publishes one physical membership leaf")
    };
    let profile = membership_profile();
    assert_eq!(local.profile_id(), profile.id());

    let schema =
        ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
    let encoded = encode_physical_leaf(
        *kind,
        &schema,
        local_leaf.version(),
        local_leaf.contains_null(),
        None,
        membership_payload(local_leaf),
    )
    .unwrap();
    assert_eq!(encoded, local_leaf.canonical_bytes());

    let retained_bytes = PhysicalArtifact::accounted_resident_bytes(encoded.len()).unwrap();
    let decoded_memory: Arc<dyn RuntimeFilterMemoryAccount> =
        MemTrackerMemoryAccount::new_root_for_test("m4-fixture-remote-decode");
    let remote_leaf = decode_leaf(
        &encoded,
        ArtifactDecodeExpectations {
            expected_kind: *kind,
            expected_schema_digest: local_leaf.schema_digest(),
            expected_logical_version: local.version(),
            expected_hash_contract: None,
        },
        encoded.len(),
        Arc::new(ArtifactRetainedBudget::new(retained_bytes)),
        decoded_memory,
    )
    .unwrap();
    let remote = ArtifactBundle::new(
        local.channel_id(),
        local.version(),
        &profile,
        vec![(*kind, remote_leaf)],
        local.encoded_bytes(),
    )
    .unwrap();

    assert_eq!(local.artifacts()[0].0, remote.artifacts()[0].0);
    assert_eq!(local.profile_id(), remote.profile_id());
    assert_eq!(local.version(), remote.version());
    assert_eq!(local.canonical_digest(), remote.canonical_digest());
}

fn order_plan(
    direction: SortDirection,
    null_order: NullOrder,
) -> (OrderContract, Arc<RuntimeOrderContract>) {
    let keys = vec![OrderKeyContract {
        data_type: DataType::Int64,
        direction,
        null_order,
    }];
    let plan = OrderContract {
        comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
        keys,
        inclusive: true,
    };
    let contract = Arc::new(RuntimeOrderContract::try_from_plan(&plan).unwrap());
    (plan, contract)
}

fn plan_from_runtime_contract(contract: &RuntimeOrderContract) -> OrderContract {
    OrderContract {
        keys: contract
            .keys()
            .iter()
            .map(|key| OrderKeyContract {
                data_type: key.data_type().clone(),
                direction: key.direction(),
                null_order: key.null_order(),
            })
            .collect(),
        inclusive: true,
        comparator_digest: contract.plan_comparator_digest(),
    }
}

fn ordered_binding(
    producer: &ProducerFixture,
    contributions: &BTreeSet<ContributionKind>,
    node_id: i32,
) -> RuntimeFilterBindingSpec {
    RuntimeFilterBindingSpec {
        binding_id: producer.binding,
        channel_id: CHANNEL,
        coverage_witness_id: Some(producer.witness),
        location: PlanLocation {
            fragment_id: producer.fragment,
            node_id: PlanNodeId::new(node_id),
        },
        expression: expression(),
        apply_point: ApplyPoint::NodeOutput,
        role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
            contribution_kinds: contributions.clone(),
            completion_requirement: CompletionRequirement::ProducerClosed,
            target: crate::runtime_filter::model::graph::ProducerBindingTarget::JoinBuildKey {
                ordinal: 0,
            },
        }),
    }
}

fn ordered_consumer_binding() -> RuntimeFilterBindingSpec {
    RuntimeFilterBindingSpec {
        binding_id: CONSUMER,
        channel_id: CHANNEL,
        coverage_witness_id: None,
        location: PlanLocation {
            fragment_id: CONSUMER_FRAGMENT,
            node_id: PlanNodeId::new(30),
        },
        expression: expression(),
        apply_point: ApplyPoint::NodeInput,
        role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
            capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
            activation: ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
            target: crate::runtime_filter::model::graph::ConsumerBindingTarget::SourceBoundary,
        }),
    }
}

fn ordered_graph(
    contract: &RuntimeOrderContract,
    producer: &ProducerFixture,
) -> RuntimeFilterGraph {
    let contributions = BTreeSet::from([
        ContributionKind::OrderedBoundUpdate,
        ContributionKind::ProducerClosed,
    ]);
    let coverage = Coverage::Leaf(producer.witness);
    let mut graph = RuntimeFilterGraph::default();
    graph
        .insert_channel(RuntimeFilterChannelSpec {
            channel_id: CHANNEL,
            logical_domain: RuntimeFilterLogicalDomain::OrderedBound(plan_from_runtime_contract(
                contract,
            )),
            lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
            availability_coverage: coverage.clone(),
            terminal_coverage: coverage,
            reduction_requirement: ReductionRequirement::TightenOrderedBound,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 4096,
                max_artifact_bytes: 4096,
                deadline_ms: 1000,
                max_retries: 1,
            },
        })
        .unwrap();
    graph
        .insert_binding(ordered_binding(producer, &contributions, 1))
        .unwrap();
    graph.insert_binding(ordered_consumer_binding()).unwrap();
    graph
        .validate()
        .expect("direct TopN graph must pass RFD-1 validation before compilation");
    graph
}

fn topk_graph(
    plan: OrderContract,
    requirement: TopKSummaryRequirement,
    producers: &[ProducerFixture],
) -> RuntimeFilterGraph {
    let contributions = BTreeSet::from([
        ContributionKind::TopKSummary,
        ContributionKind::ProducerClosed,
    ]);
    let coverage = Coverage::AllOf(
        producers
            .iter()
            .map(|producer| Coverage::Leaf(producer.witness))
            .collect(),
    );
    let mut graph = RuntimeFilterGraph::default();
    graph
        .insert_channel(RuntimeFilterChannelSpec {
            channel_id: CHANNEL,
            logical_domain: RuntimeFilterLogicalDomain::OrderedBound(plan),
            lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
            availability_coverage: coverage.clone(),
            terminal_coverage: coverage,
            reduction_requirement: ReductionRequirement::MergeTopKSummary(requirement),
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 4096,
                max_artifact_bytes: 4096,
                deadline_ms: 1000,
                max_retries: 1,
            },
        })
        .unwrap();
    for (index, producer) in producers.iter().enumerate() {
        graph
            .insert_binding(ordered_binding(producer, &contributions, index as i32 + 1))
            .unwrap();
    }
    graph.insert_binding(ordered_consumer_binding()).unwrap();
    graph
        .validate()
        .expect("TopKSummary graph must pass RFD-1 validation before compilation");
    graph
}

struct DirectTopNHarness {
    _service: Arc<RuntimeFilterService>,
    producer: Arc<dyn OrderedBoundProducerAdapter>,
    live: Arc<dyn NonBlockingLiveSubscription>,
}

fn direct_topn_harness(contract: Arc<RuntimeOrderContract>) -> DirectTopNHarness {
    let producers = producer_fixtures();
    let direct = &producers[0];
    let graph = ordered_graph(&contract, direct);
    let scheduling = scheduling_plan(std::slice::from_ref(direct));
    let edges = fragment_edges(std::slice::from_ref(direct));
    let service = install_service(compile_participant_install(
        &graph,
        &scheduling,
        &edges,
        PARTICIPANT,
    ));
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(
            direct.binding,
            direct.instance,
            1,
            ProducerPortKind::OrderedBound,
        )
        .expect("compiler-installed direct TopN producer is authorized")
    else {
        panic!("direct TopN graph must install only the OrderedBound producer port")
    };
    let SubscriptionHandle::Live(live) = service
        .subscribe(
            CONSUMER,
            CONSUMER_INSTANCE,
            SubscriptionKind::NonBlockingLive,
        )
        .expect("compiler-installed ordered consumer is authorized")
    else {
        panic!("ordered graph consumer must install only NonBlockingLive")
    };
    DirectTopNHarness {
        _service: service,
        producer,
        live,
    }
}

struct TopNHeapAdapter {
    k: usize,
    contract: Arc<RuntimeOrderContract>,
    producer: Arc<dyn OrderedBoundProducerAdapter>,
    candidates: Vec<OrderedTuple>,
    next_sequence: u64,
    published: Vec<LogicalVersion>,
}

impl TopNHeapAdapter {
    fn new(
        k: usize,
        contract: Arc<RuntimeOrderContract>,
        producer: Arc<dyn OrderedBoundProducerAdapter>,
    ) -> Self {
        assert!(k > 0, "TopN adapter requires a positive limit");
        Self {
            k,
            contract,
            producer,
            candidates: Vec::new(),
            next_sequence: 0,
            published: Vec::new(),
        }
    }

    fn push(
        &mut self,
        row: OrderedTuple,
    ) -> Result<Option<LogicalVersion>, RuntimeContractViolation> {
        let previous_kth = self.candidates.get(self.k - 1).cloned();
        self.candidates.push(row);
        self.candidates.sort_by(|left, right| {
            self.contract
                .compare(left, right)
                .expect("TopN candidates match their runtime order contract")
        });
        self.candidates.truncate(self.k);
        let Some(current_kth) = self.candidates.get(self.k - 1).cloned() else {
            return Ok(None);
        };
        if previous_kth.as_ref().is_some_and(|previous| {
            self.contract
                .compare(&current_kth, previous)
                .expect("TopN candidates match their runtime order contract")
                != Ordering::Less
        }) {
            return Ok(None);
        }

        let outcome = self.producer.submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(self.next_sequence),
            OrderedBoundUpdate::new(&self.contract, current_kth)
                .expect("TopN kth tuple matches its runtime order contract"),
        )?;
        assert_eq!(
            outcome,
            SubmitOutcome::Published,
            "a genuinely tighter direct TopN bound must publish"
        );
        self.next_sequence += 1;
        let version = self
            .published
            .last()
            .copied()
            .and_then(LogicalVersion::checked_next)
            .unwrap_or(LogicalVersion::FIRST);
        self.published.push(version);
        Ok(Some(version))
    }

    fn published_versions(&self) -> &[LogicalVersion] {
        &self.published
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IndependentOrder {
    AscNullsLast,
    DescNullsFirst,
}

impl IndependentOrder {
    const fn direction(self) -> SortDirection {
        match self {
            Self::AscNullsLast => SortDirection::Ascending,
            Self::DescNullsFirst => SortDirection::Descending,
        }
    }

    const fn null_order(self) -> NullOrder {
        match self {
            Self::AscNullsLast => NullOrder::Last,
            Self::DescNullsFirst => NullOrder::First,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExpectedBound(Option<i64>);

struct TopNCase {
    order: IndependentOrder,
    contract: Arc<RuntimeOrderContract>,
    raw_values: Vec<Option<i64>>,
    rows: Vec<OrderedTuple>,
    final_topn_values: Vec<Option<i64>>,
    expected_bounds: Vec<Option<ExpectedBound>>,
}

fn tuple(contract: &RuntimeOrderContract, value: Option<i64>) -> OrderedTuple {
    OrderedTuple::try_new(contract, [value.map(OrderedScalar::Int64)])
        .expect("finite TopN sample matches the Int64 order contract")
}

fn independent_ordering(
    order: IndependentOrder,
    left: &Option<i64>,
    right: &Option<i64>,
) -> Ordering {
    match order {
        IndependentOrder::AscNullsLast => match (left, right) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(left), Some(right)) => left.cmp(right),
        },
        IndependentOrder::DescNullsFirst => match (left, right) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(left), Some(right)) => right.cmp(left),
        },
    }
}

fn independent_topn(order: IndependentOrder, rows: &[Option<i64>], k: usize) -> Vec<Option<i64>> {
    let mut ranked = rows.to_vec();
    ranked.sort_by(|left, right| independent_ordering(order, left, right));
    ranked.truncate(k);
    ranked
}

fn independent_publication_bounds(
    order: IndependentOrder,
    rows: &[Option<i64>],
    k: usize,
) -> Vec<Option<ExpectedBound>> {
    let mut prefix = Vec::with_capacity(rows.len());
    let mut previous_bound: Option<Option<i64>> = None;
    rows.iter()
        .map(|row| {
            prefix.push(*row);
            let ranked = independent_topn(order, &prefix, k);
            let Some(bound) = ranked.get(k - 1).copied() else {
                return None;
            };
            let publish = previous_bound.as_ref().is_none_or(|previous| {
                independent_ordering(order, &bound, previous) == Ordering::Less
            });
            previous_bound = Some(bound);
            publish.then_some(ExpectedBound(bound))
        })
        .collect()
}

fn topn_case(order: IndependentOrder, values: impl IntoIterator<Item = Option<i64>>) -> TopNCase {
    let (_, contract) = order_plan(order.direction(), order.null_order());
    let raw_values = values.into_iter().collect::<Vec<_>>();
    let rows = raw_values
        .iter()
        .copied()
        .map(|value| tuple(&contract, value))
        .collect::<Vec<_>>();
    let final_topn_values = independent_topn(order, &raw_values, 3);
    let expected_bounds = independent_publication_bounds(order, &raw_values, 3);
    TopNCase {
        order,
        contract,
        raw_values,
        rows,
        final_topn_values,
        expected_bounds,
    }
}

fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

fn lcg_shuffle<T>(state: &mut u64, values: &mut [T]) {
    for upper in (1..values.len()).rev() {
        let index = (lcg_next(state) % (upper as u64 + 1)) as usize;
        values.swap(upper, index);
    }
}

fn topn_cases_with_fixed_seed() -> Vec<TopNCase> {
    let mut cases = vec![
        topn_case(
            IndependentOrder::AscNullsLast,
            [
                Some(30),
                Some(20),
                Some(10),
                Some(100),
                Some(30),
                Some(5),
                Some(20),
                Some(1),
                None,
            ],
        ),
        topn_case(
            IndependentOrder::DescNullsFirst,
            [
                Some(10),
                Some(20),
                Some(30),
                Some(-100),
                Some(10),
                Some(40),
                Some(20),
                Some(50),
                None,
            ],
        ),
    ];
    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    for index in 0..64 {
        let base = ((lcg_next(&mut state) >> 32) % 20_000) as i64 - 10_000;
        let mode = index % 4;
        if index % 2 == 0 {
            let mut initial = vec![Some(base + 30), Some(base + 20), Some(base + 10)];
            lcg_shuffle(&mut state, &mut initial);
            let mut tail = vec![Some(base + 5), Some(base + 1)];
            match mode {
                0 => tail.extend([Some(base + 100), Some(base + 30)]),
                1 => tail.extend([None, Some(base + 100), Some(base + 20)]),
                2 => tail.extend([Some(base + 80), Some(base + 70)]),
                3 => tail.extend([None, None, Some(base + 10), Some(base + 90)]),
                _ => unreachable!(),
            }
            lcg_shuffle(&mut state, &mut tail);
            initial.extend(tail);
            cases.push(topn_case(IndependentOrder::AscNullsLast, initial));
        } else {
            let mut initial = vec![Some(base + 10), Some(base + 20), Some(base + 30)];
            lcg_shuffle(&mut state, &mut initial);
            let mut tail = vec![Some(base + 40), Some(base + 50)];
            match mode {
                0 => tail.extend([Some(base - 100), Some(base + 10)]),
                1 => tail.extend([None, Some(base - 100), Some(base + 20)]),
                2 => tail.extend([Some(base - 80), Some(base - 70)]),
                3 => tail.extend([None, None, Some(base + 30), Some(base - 90)]),
                _ => unreachable!(),
            }
            lcg_shuffle(&mut state, &mut tail);
            initial.extend(tail);
            cases.push(topn_case(IndependentOrder::DescNullsFirst, initial));
        }
    }
    assert_eq!(cases.len(), 66);
    cases
}

fn relative_value_pattern(values: &[Option<i64>]) -> Vec<Option<usize>> {
    let unique = values
        .iter()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    values
        .iter()
        .map(|value| value.map(|value| unique.binary_search(&value).unwrap()))
        .collect()
}

fn published_expected_bounds(case: &TopNCase) -> Vec<Option<i64>> {
    case.expected_bounds
        .iter()
        .flatten()
        .map(|bound| bound.0)
        .collect()
}

fn assert_fixed_seed_case_diversity(cases: &[TopNCase]) {
    assert_eq!(cases.len(), 66, "two fixed plus 64 LCG-generated cases");
    assert_eq!(cases[0].final_topn_values, vec![Some(1), Some(5), Some(10)]);
    assert_eq!(
        published_expected_bounds(&cases[0]),
        vec![Some(30), Some(20), Some(10)]
    );
    assert_eq!(cases[1].final_topn_values, vec![None, Some(50), Some(40)]);
    assert_eq!(
        published_expected_bounds(&cases[1]),
        vec![Some(10), Some(20), Some(30), Some(40)]
    );

    let generated = &cases[2..];
    assert_eq!(generated.len(), 64);
    assert!(generated.iter().any(|case| case.raw_values.contains(&None)));
    assert!(
        generated
            .iter()
            .any(|case| !case.raw_values.contains(&None))
    );
    assert!(generated.iter().any(|case| {
        let mut seen = BTreeSet::new();
        case.raw_values.iter().any(|value| !seen.insert(*value))
    }));
    assert!(generated.iter().any(|case| {
        let mut seen = BTreeSet::new();
        case.raw_values.iter().all(|value| seen.insert(*value))
    }));
    assert!(generated.iter().all(|case| {
        case.expected_bounds
            .iter()
            .filter(|bound| bound.is_some())
            .count()
            >= 3
    }));

    let relative_patterns = generated
        .iter()
        .map(|case| relative_value_pattern(&case.raw_values))
        .collect::<BTreeSet<_>>();
    let tightening_cadences = generated
        .iter()
        .map(|case| {
            case.expected_bounds
                .iter()
                .enumerate()
                .filter_map(|(index, bound)| bound.map(|_| index))
                .collect::<Vec<_>>()
        })
        .collect::<BTreeSet<_>>();
    assert!(relative_patterns.len() >= 16);
    assert!(tightening_cadences.len() >= 8);
}

fn assert_bound_is_sound_for_final_topn(
    live: &Arc<dyn NonBlockingLiveSubscription>,
    version: LogicalVersion,
    order: IndependentOrder,
    expected_bound: ExpectedBound,
    final_topn: &[Option<i64>],
) -> Arc<ArtifactBundle> {
    let LivePollOutcome::Updated {
        bundle,
        terminal: None,
    } = live.poll_after(None)
    else {
        panic!("direct TopN tightening must be visible as a non-terminal live update")
    };
    assert_eq!(bundle.version(), version);
    let [(ArtifactKind::Range, artifact)] = bundle.artifacts() else {
        panic!("ordered TopN must materialize exactly one Range artifact")
    };
    let range = artifact.range().expect("Range leaf owns ordered data");
    let [actual_bound] = range.bound().values() else {
        panic!("TopN fixture expects a single-key Range bound")
    };
    let actual_bound = match actual_bound {
        None => None,
        Some(OrderedScalar::Int64(value)) => Some(*value),
        Some(_) => panic!("TopN fixture expects an Int64 Range bound"),
    };
    assert_eq!(actual_bound, expected_bound.0);
    let [key] = range.contract().keys() else {
        panic!("TopN fixture expects a single-key order contract")
    };
    assert_eq!(key.direction(), order.direction());
    assert_eq!(key.null_order(), order.null_order());
    for row in final_topn {
        assert!(
            independent_bound_survives(order, *row, actual_bound),
            "visible TopN bound must not eliminate a row in the final TopN"
        );
    }
    bundle
}

fn independent_bound_survives(
    order: IndependentOrder,
    row: Option<i64>,
    bound: Option<i64>,
) -> bool {
    match order {
        IndependentOrder::AscNullsLast => match (row, bound) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(row), Some(bound)) => row <= bound,
        },
        IndependentOrder::DescNullsFirst => match (row, bound) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(row), Some(bound)) => row >= bound,
        },
    }
}

fn assert_immutable_version_history(
    observed: &[(Arc<ArtifactBundle>, LogicalVersion, [u8; 32])],
    expected_versions: &[LogicalVersion],
) {
    assert_eq!(observed.len(), expected_versions.len());
    assert!(observed.len() >= 2);
    for (record, expected) in observed.iter().zip(expected_versions) {
        assert_eq!(record.0.version(), record.1);
        assert_eq!(record.0.version(), *expected);
        assert_eq!(record.0.canonical_digest(), record.2);
    }
    for pair in observed.windows(2) {
        assert!(pair[0].1 < pair[1].1);
        assert!(!Arc::ptr_eq(&pair[0].0, &pair[1].0));
    }
}

struct TopKSummaryHarness {
    service: Arc<RuntimeFilterService>,
    contract: Arc<RuntimeTopKSummaryContract>,
    live: Arc<dyn NonBlockingLiveSubscription>,
}

impl TopKSummaryHarness {
    fn producer(
        &self,
        binding: BindingId,
        instance: UniqueId,
    ) -> Arc<dyn TopKSummaryProducerAdapter> {
        let ProducerHandle::TopKSummary(producer) = self
            .service
            .open_producer(binding, instance, 1, ProducerPortKind::TopKSummary)
            .expect("compiler-installed TopKSummary producer is authorized")
        else {
            panic!("TopKSummary graph must install only the TopKSummary producer port")
        };
        producer
    }

    fn submit_summary(
        &self,
        binding: BindingId,
        instance: UniqueId,
        values: &[i64],
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let mut values = values.to_vec();
        values.sort_unstable();
        let candidates = values
            .iter()
            .map(|value| tuple(self.contract.order(), Some(*value)))
            .collect::<Vec<_>>();
        self.producer(binding, instance).submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            TopKSummary::try_new(&self.contract, candidates)
                .expect("TopKSummary fixture constructs canonical candidates"),
        )
    }

    fn close_all(&self) -> Result<(), RuntimeContractViolation> {
        let visible_version = self
            .live
            .snapshot()
            .expect("TopKSummary bound is visible before close")
            .version();
        let first = self
            .producer(PRODUCER_A, INSTANCE_A)
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))?;
        assert_ne!(first, SubmitOutcome::Completed);
        assert!(matches!(
            self.live.poll_after(Some(visible_version)),
            LivePollOutcome::Idle {
                latest_version: Some(latest),
                terminal: None,
            } if latest == visible_version
        ));
        let second = self
            .producer(PRODUCER_B, INSTANCE_B)
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))?;
        assert_eq!(second, SubmitOutcome::Completed);
        Ok(())
    }
}

fn topk_allof_harness(k: u32) -> TopKSummaryHarness {
    let producers = producer_fixtures();
    let (plan, _) = order_plan(SortDirection::Ascending, NullOrder::Last);
    let requirement = TopKSummaryRequirement::try_new(k).expect("TopK requires positive K");
    let contract = Arc::new(
        RuntimeTopKSummaryContract::try_from_plan(&plan, requirement)
            .expect("TopKSummary plan contract is valid"),
    );
    let graph = topk_graph(plan, requirement, &producers);
    let scheduling = scheduling_plan(&producers);
    let edges = fragment_edges(&producers);
    let service = install_service(compile_participant_install(
        &graph,
        &scheduling,
        &edges,
        PARTICIPANT,
    ));
    let SubscriptionHandle::Live(live) = service
        .subscribe(
            CONSUMER,
            CONSUMER_INSTANCE,
            SubscriptionKind::NonBlockingLive,
        )
        .expect("compiler-installed TopKSummary consumer is authorized")
    else {
        panic!("TopKSummary consumer must install only NonBlockingLive")
    };
    TopKSummaryHarness {
        service,
        contract,
        live,
    }
}

fn expect_live_update(outcome: LivePollOutcome) -> Arc<ArtifactBundle> {
    let LivePollOutcome::Updated {
        bundle,
        terminal: None,
    } = outcome
    else {
        panic!("expected a non-terminal live range update")
    };
    bundle
}

fn assert_sound_topk_bound(bundle: &ArtifactBundle, final_topk: &[i64]) {
    let [(ArtifactKind::Range, artifact)] = bundle.artifacts() else {
        panic!("TopKSummary must materialize exactly one Range artifact")
    };
    let range = artifact.range().expect("Range leaf owns ordered data");
    let [Some(OrderedScalar::Int64(actual_bound))] = range.bound().values() else {
        panic!("TopKSummary fixture expects one non-null Int64 bound")
    };
    let [key] = range.contract().keys() else {
        panic!("TopKSummary fixture expects a single-key order contract")
    };
    assert_eq!(key.direction(), SortDirection::Ascending);
    assert_eq!(key.null_order(), NullOrder::Last);
    assert_eq!(Some(actual_bound), final_topk.last());
    for value in final_topk {
        assert!(
            independent_bound_survives(
                IndependentOrder::AscNullsLast,
                Some(*value),
                Some(*actual_bound),
            ),
            "merged TopK bound must not eliminate a final TopK row"
        );
    }
}

fn assert_completed_without_new_unsound_version(
    live: &Arc<dyn NonBlockingLiveSubscription>,
    version: LogicalVersion,
) {
    assert!(matches!(
        live.poll_after(Some(version)),
        LivePollOutcome::Idle {
            latest_version: Some(latest),
            terminal: Some(LiveTerminal::Completed),
        } if latest == version
    ));
    assert_eq!(live.snapshot().unwrap().version(), version);
}

struct AggregateHarness {
    service: Arc<RuntimeFilterService>,
    producers: Vec<(BindingId, UniqueId, Arc<dyn FinalDomainProducerAdapter>)>,
    live: Arc<dyn NonBlockingLiveSubscription>,
}

impl AggregateHarness {
    fn producer(
        &self,
        binding: BindingId,
        instance: UniqueId,
    ) -> &Arc<dyn FinalDomainProducerAdapter> {
        &self
            .producers
            .iter()
            .find(|(installed_binding, installed_instance, _)| {
                *installed_binding == binding && *installed_instance == instance
            })
            .expect("Aggregate fixture keeps every compiler-installed producer open")
            .2
    }

    fn collecting_issuer(
        &self,
        binding: BindingId,
        instance: UniqueId,
        open_drivers: u32,
    ) -> CollectingFinalDomainTestIssuer {
        self.service
            .final_domain_test_issuer(binding, instance, open_drivers)
            .expect("opened final-domain producer owns the test-only fence authority")
    }

    fn freeze(
        &self,
        binding: BindingId,
        instance: UniqueId,
        open_drivers: u32,
    ) -> FrozenFinalDomainTestIssuer {
        let mut transition = FinalDomainTestIssuerTransition::Collecting(self.collecting_issuer(
            binding,
            instance,
            open_drivers,
        ));
        loop {
            transition = match transition {
                FinalDomainTestIssuerTransition::Collecting(collecting) => {
                    collecting.close_driver()
                }
                FinalDomainTestIssuerTransition::Frozen(frozen) => return frozen,
            };
        }
    }

    fn complete(
        &self,
        binding: BindingId,
        instance: UniqueId,
        sequence: u64,
        issuer: &FrozenFinalDomainTestIssuer,
        values: &[i64],
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let partition = PartitionId::new(0);
        let sequence = ProducerSequence::new(sequence);
        let shard = issuer
            .issue_shard(
                ProducerStreamId::new(binding, instance, partition),
                sequence,
                ValueDomainDelta::new(MembershipValues::int64(values.iter().copied()), false),
            )
            .expect("frozen Aggregate issuer signs only its installed producer stream");
        self.producer(binding, instance)
            .complete(partition, sequence, shard)
    }

    fn close(
        &self,
        binding: BindingId,
        instance: UniqueId,
        partition: u32,
        terminal_sequence: u64,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.producer(binding, instance).close_partition(
            PartitionId::new(partition),
            ProducerSequence::new(terminal_sequence),
        )
    }
}

fn aggregate_harness_with_memory(
    producers: &[ProducerFixture],
    memory: Arc<dyn RuntimeFilterMemoryAccount>,
) -> AggregateHarness {
    let graph = aggregate_graph(producers);
    let scheduling = scheduling_plan(producers);
    let edges = fragment_edges(producers);
    let service = install_service_with_memory(
        compile_participant_install(&graph, &scheduling, &edges, PARTICIPANT),
        memory,
    );
    let mut opened = Vec::with_capacity(producers.len());
    for producer in producers {
        let error = service
            .open_producer(
                producer.binding,
                producer.instance,
                1,
                ProducerPortKind::Membership,
            )
            .expect_err("fenced-final Aggregate graph must reject the Membership producer port");
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ProducerPortMismatch
        );
        let ProducerHandle::FinalDomain(handle) = service
            .open_producer(
                producer.binding,
                producer.instance,
                1,
                ProducerPortKind::FinalDomain,
            )
            .expect("compiler-installed Aggregate producer exposes FinalDomain")
        else {
            panic!("Aggregate graph must install only the FinalDomain producer port")
        };
        opened.push((producer.binding, producer.instance, handle));
    }
    let SubscriptionHandle::Live(live) = service
        .subscribe(
            CONSUMER,
            CONSUMER_INSTANCE,
            SubscriptionKind::NonBlockingLive,
        )
        .expect("compiler-installed Aggregate consumer is authorized")
    else {
        panic!("Aggregate graph consumer must install only NonBlockingLive")
    };
    AggregateHarness {
        service,
        producers: opened,
        live,
    }
}

fn aggregate_allof_harness() -> AggregateHarness {
    let producers = producer_fixtures();
    aggregate_harness_with_memory(
        &producers,
        MemTrackerMemoryAccount::new_root_for_test("m4-aggregate-allof"),
    )
}

fn expect_collecting(
    transition: FinalDomainTestIssuerTransition,
) -> CollectingFinalDomainTestIssuer {
    let FinalDomainTestIssuerTransition::Collecting(collecting) = transition else {
        panic!("one local Aggregate driver must remain open")
    };
    collecting
}

fn expect_frozen(transition: FinalDomainTestIssuerTransition) -> FrozenFinalDomainTestIssuer {
    let FinalDomainTestIssuerTransition::Frozen(frozen) = transition else {
        panic!("Aggregate issuer freezes only after the last local driver closes")
    };
    frozen
}

fn expect_live_completed(outcome: LivePollOutcome) -> Arc<ArtifactBundle> {
    let LivePollOutcome::Updated {
        bundle,
        terminal: Some(LiveTerminal::Completed),
    } = outcome
    else {
        panic!("fenced-final AllOf must publish one terminal live artifact")
    };
    bundle
}

fn assert_explicit_empty_is_empty_domain() {
    let producers = producer_fixtures();
    let empty = aggregate_harness_with_memory(
        &producers[..1],
        MemTrackerMemoryAccount::new_root_for_test("m4-aggregate-explicit-empty"),
    );
    let frozen = empty.freeze(PRODUCER_A, INSTANCE_A, 1);
    empty
        .complete(PRODUCER_A, INSTANCE_A, 0, &frozen, &[])
        .unwrap();
    assert_eq!(
        empty.close(PRODUCER_A, INSTANCE_A, 0, 1).unwrap(),
        SubmitOutcome::Completed
    );
    let bundle = expect_live_completed(empty.live.poll_after(None));
    assert_eq!(bundle.version(), LogicalVersion::FIRST);
    let [(ArtifactKind::EmptyDomain, _)] = bundle.artifacts() else {
        panic!("explicit empty Aggregate shard must publish exactly one EmptyDomain artifact")
    };
}

struct RejectingMemoryAccount;

impl RuntimeFilterMemoryAccount for RejectingMemoryAccount {
    fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
        Err(MemoryAccountError::CapacityExceeded)
    }

    fn release(&self, _bytes: usize) {}
}

fn assert_resource_failure_is_unavailable() {
    let producers = producer_fixtures();
    let unavailable =
        aggregate_harness_with_memory(&producers[..1], Arc::new(RejectingMemoryAccount));
    let frozen = unavailable.freeze(PRODUCER_A, INSTANCE_A, 1);
    assert_eq!(
        unavailable
            .complete(PRODUCER_A, INSTANCE_A, 0, &frozen, &[])
            .unwrap(),
        SubmitOutcome::TerminalNoop
    );
    assert!(unavailable.live.snapshot().is_none());
    assert!(matches!(
        unavailable.live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(UnavailableReason::ResourceLimit)),
        }
    ));
}

#[test]
fn m4_join_conformance_uses_graph_compiler_public_ports_and_route_equivalent_artifacts() {
    let all_of = join_allof_harness();
    let first = all_of.producer(PRODUCER_A, INSTANCE_A);
    first.submit_values(0, 0, [1]).unwrap();
    first.close(0, 1).unwrap();
    assert!(all_of.blocking.snapshot().is_none());
    let second = all_of.producer(PRODUCER_B, INSTANCE_B);
    second.submit_values(0, 0, [2]).unwrap();
    second.close(0, 1).unwrap();
    let local = all_of
        .blocking
        .snapshot()
        .expect("AllOf publishes after both witnesses");
    assert_eq!(local.version(), LogicalVersion::FIRST);
    assert_membership_values(&local, &[1, 2]);
    assert_fixture_remote_equivalent(&local);

    let any_of = join_anyof_harness();
    publish_membership(&any_of, PRODUCER_A, INSTANCE_A, &[7]);
    let first = any_of
        .blocking
        .snapshot()
        .expect("first valid replica publishes");
    publish_membership(&any_of, PRODUCER_B, INSTANCE_B, &[9]);
    let after_late = any_of.blocking.snapshot().expect("winner remains visible");
    assert_eq!(first.version(), after_late.version());
    assert_eq!(first.canonical_digest(), after_late.canonical_digest());
}

#[test]
fn m4_direct_topn_conformance_delays_until_n_and_preserves_sound_monotonic_bounds() {
    let cases = topn_cases_with_fixed_seed();
    assert_fixed_seed_case_diversity(&cases);
    for case in cases {
        let harness = direct_topn_harness(case.contract.clone());
        let mut adapter = TopNHeapAdapter::new(3, case.contract.clone(), harness.producer.clone());
        assert!(case.expected_bounds[0].is_none());
        assert!(case.expected_bounds[1].is_none());
        assert!(adapter.push(case.rows[0].clone()).unwrap().is_none());
        assert!(adapter.push(case.rows[1].clone()).unwrap().is_none());
        assert!(matches!(
            harness.live.poll_after(None),
            LivePollOutcome::Idle { .. }
        ));
        let mut observed = Vec::new();
        for (index, row) in case.rows.into_iter().enumerate().skip(2) {
            let published = adapter.push(row).unwrap();
            assert_eq!(published.is_some(), case.expected_bounds[index].is_some());
            if let Some(version) = published {
                let bundle = assert_bound_is_sound_for_final_topn(
                    &harness.live,
                    version,
                    case.order,
                    case.expected_bounds[index]
                        .expect("an expected publication carries its independent oracle bound"),
                    &case.final_topn_values,
                );
                observed.push((bundle.clone(), bundle.version(), bundle.canonical_digest()));
            }
        }
        assert!(adapter.published_versions().len() >= 2);
        assert_immutable_version_history(&observed, adapter.published_versions());
    }
}

#[test]
fn m4_topk_summary_conformance_merges_incomplete_shards_only_after_allof() {
    let harness = topk_allof_harness(3);
    harness
        .submit_summary(PRODUCER_A, INSTANCE_A, &[1, 4])
        .unwrap();
    assert!(matches!(
        harness.live.poll_after(None),
        LivePollOutcome::Idle { .. }
    ));
    harness
        .submit_summary(PRODUCER_B, INSTANCE_B, &[2, 3])
        .unwrap();
    let first = expect_live_update(harness.live.poll_after(None));
    assert_sound_topk_bound(&first, &[1, 2, 3]);
    harness.close_all().unwrap();
    assert_completed_without_new_unsound_version(&harness.live, first.version());
}

#[test]
fn m4_aggregate_conformance_requires_frozen_allof_and_separates_empty_unavailable() {
    let aggregate = aggregate_allof_harness();
    let collecting = aggregate.collecting_issuer(PRODUCER_A, INSTANCE_A, 2);
    let collecting = expect_collecting(collecting.close_driver());
    assert!(aggregate.live.snapshot().is_none());
    let frozen_a = expect_frozen(collecting.close_driver());
    let frozen_b = aggregate.freeze(PRODUCER_B, INSTANCE_B, 1);
    aggregate
        .complete(PRODUCER_A, INSTANCE_A, 0, &frozen_a, &[1])
        .unwrap();
    aggregate.close(PRODUCER_A, INSTANCE_A, 0, 1).unwrap();
    assert!(aggregate.live.snapshot().is_none());
    aggregate
        .complete(PRODUCER_B, INSTANCE_B, 0, &frozen_b, &[2])
        .unwrap();
    aggregate.close(PRODUCER_B, INSTANCE_B, 0, 1).unwrap();
    let bundle = expect_live_completed(aggregate.live.poll_after(None));
    assert_eq!(bundle.version(), LogicalVersion::FIRST);
    assert_membership_values(&bundle, &[1, 2]);
    assert!(matches!(
        aggregate.live.poll_after(Some(bundle.version())),
        LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: Some(LiveTerminal::Completed),
        }
    ));

    assert_explicit_empty_is_empty_domain();
    assert_resource_failure_is_unavailable();
}
