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

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_execution::runtime_filter as execution;
use novarocks_execution::runtime_filter::RuntimeFilterSession;

use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::operators::{AggregateFinalDomainSessionBuilder, AggregateProcessorFactory};
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::query_execution::backend::LiveBackendSnapshot;
use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter::deployment::{RuntimeFilterDeploymentPolicy, compiler};
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
    ConsumerActivation, ContributionKind, CoverageWitnessId, LateApplyGranularity, NullSemantics,
    PlanFragmentId, PlanNodeId, ReductionRequirement, RuntimeFilterLifecycle,
    RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::artifact::{
    ArtifactBundle, ArtifactKind, ResidentMembershipIndexView,
};
use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, LogicalVersion, RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::install::{
    MaterializationPolicy, RuntimeFilterCoreBudget, RuntimeFilterParticipantInstall,
};
use crate::runtime_filter::port::producer::InstallOutcome;
use crate::runtime_filter::port::subscription::{
    LivePollOutcome, LiveTerminal, NonBlockingLiveSubscription, UnavailableReason,
};
use crate::runtime_filter::port::support::{RuntimeFilterClock, RuntimeFilterMemoryAccount};
use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
use crate::sql::planner::distributed::{
    DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
};
use crate::sql::planner::runtime_filter::{
    contract as sql_contract, coverage::Coverage as SqlCoverage, graph as sql_graph,
};

use super::RuntimeFilterService;
use super::memory::MemTrackerMemoryAccount;
use super::native_execution::NativeRuntimeFilterExecutionContext;

const CHANNEL: ChannelId = ChannelId::new(401);
const PRODUCER_A: BindingId = BindingId::new(410);
const PRODUCER_B: BindingId = BindingId::new(420);
const CONSUMER: BindingId = BindingId::new(430);
const WITNESS_A: CoverageWitnessId = CoverageWitnessId::new(411);
const WITNESS_B: CoverageWitnessId = CoverageWitnessId::new(421);
const PRODUCER_FRAGMENT_A: PlanFragmentId = PlanFragmentId::new(41);
const PRODUCER_FRAGMENT_B: PlanFragmentId = PlanFragmentId::new(42);
const CONSUMER_FRAGMENT: PlanFragmentId = PlanFragmentId::new(43);
const INSTANCE_A: UniqueId = UniqueId::new(406, 10);
const INSTANCE_B: UniqueId = UniqueId::new(406, 20);
const CONSUMER_INSTANCE: UniqueId = UniqueId::new(406, 30);
const PARTICIPANT: RuntimeFilterParticipantId = RuntimeFilterParticipantId::new(1);
const AGGREGATE_DOP: i32 = 2;
const GROUP_SLOT: SlotId = SlotId::new(401);

#[derive(Clone, Copy, Debug)]
enum Witness {
    A,
    B,
}

struct ProducerFixture {
    binding: BindingId,
    witness: CoverageWitnessId,
    fragment: PlanFragmentId,
    instance: UniqueId,
}

struct DeterministicClock(Instant);

impl RuntimeFilterClock for DeterministicClock {
    fn now(&self) -> Instant {
        self.0
    }
}

struct DiscardEvents;

impl RuntimeFilterEventSink for DiscardEvents {
    fn record(&self, _event: RuntimeFilterEvent) {}
}

struct WitnessProcessors {
    _factory: AggregateProcessorFactory,
    drivers: Vec<Option<Box<dyn Operator>>>,
}

impl WitnessProcessors {
    fn open(service: &Arc<RuntimeFilterService>, binding: BindingId, instance: UniqueId) -> Self {
        let context = NativeRuntimeFilterExecutionContext::new(
            Arc::clone(service),
            UniqueId::new(406, 0),
            DeploymentEpoch::new(1),
            instance,
        );
        let resolved = context
            .resolve_producer(
                binding,
                CHANNEL,
                crate::runtime_filter::port::producer::ProducerPortKind::FinalDomain,
            )
            .expect("compiler-installed final-domain producer resolves");
        let request = execution::RuntimeFilterFinalDomainOpenRequest::new(
            execution::RuntimeFilterProducerContract::new(
                execution::RuntimeFilterBindingId::new(binding.get()),
                execution::RuntimeFilterChannelId::new(CHANNEL.get()),
                execution::RuntimeFilterProducerKind::FinalDomain,
                resolved.execution_contract(),
            ),
            AGGREGATE_DOP as u32,
        );
        let execution::RuntimeFilterBindOutcome::Bound(completion) = context
            .open_final_domain_completion(request)
            .expect("compiler-installed aggregate completion capability opens")
        else {
            panic!("compiler-installed aggregate completion capability is available")
        };
        let session = AggregateFinalDomainSessionBuilder::new(completion, AGGREGATE_DOP, 4096)
            .expect("aggregate processor accepts the installed completion session");
        let factory = aggregate_factory(session);
        let drivers = (0..AGGREGATE_DOP)
            .map(|driver_id| {
                let mut operator = factory.create(AGGREGATE_DOP, driver_id);
                operator.prepare().expect("prepare aggregate processor");
                Some(operator)
            })
            .collect();
        Self {
            _factory: factory,
            drivers,
        }
    }

    fn finish_driver(&mut self, driver: usize, values: &[i64]) {
        let operator = self
            .drivers
            .get_mut(driver)
            .and_then(Option::as_mut)
            .expect("aggregate driver remains live until its requested terminal action");
        let state = RuntimeState::default();
        let processor = operator
            .as_processor_mut()
            .expect("aggregate factory creates processor operators");
        processor
            .push_chunk(&state, group_chunk(values))
            .expect("aggregate processor accepts its local rows");
        processor
            .set_finishing(&state)
            .expect("aggregate processor finalizes its local rows");
        processor
            .pull_chunk(&state)
            .expect("aggregate output pull succeeds")
            .expect("grouped aggregate emits its final output chunk");
    }

    fn drop_driver(&mut self, driver: usize) {
        drop(
            self.drivers
                .get_mut(driver)
                .and_then(Option::take)
                .expect("aggregate driver is dropped exactly once"),
        );
    }
}

struct LiveAggregateHarness {
    service: Arc<RuntimeFilterService>,
    producer_a: WitnessProcessors,
    producer_b: WitnessProcessors,
    live: Arc<dyn NonBlockingLiveSubscription>,
}

impl LiveAggregateHarness {
    fn new() -> Self {
        let producers = producer_fixtures();
        let graph = aggregate_graph(&producers);
        let scheduling = scheduling_plan(&producers);
        let edges = fragment_edges(&producers);
        let service = install_service(compile_participant_install(&graph, &scheduling, &edges));
        let producer_a = WitnessProcessors::open(&service, PRODUCER_A, INSTANCE_A);
        let producer_b = WitnessProcessors::open(&service, PRODUCER_B, INSTANCE_B);
        let live = service
            .subscribe(
                CONSUMER,
                CONSUMER_INSTANCE,
                crate::runtime_filter::port::subscription::SubscriptionKind::NonBlockingLive,
            )
            .expect("compiler-installed aggregate consumer subscribes")
            .into_live()
            .expect("aggregate graph installs a live consumer");
        Self {
            service,
            producer_a,
            producer_b,
            live,
        }
    }

    fn finish_driver(&mut self, witness: Witness, driver: usize, values: &[i64]) {
        self.producer(witness).finish_driver(driver, values);
    }

    fn drop_driver(&mut self, witness: Witness, driver: usize) {
        self.producer(witness).drop_driver(driver);
    }

    fn producer(&mut self, witness: Witness) -> &mut WitnessProcessors {
        match witness {
            Witness::A => &mut self.producer_a,
            Witness::B => &mut self.producer_b,
        }
    }
}

impl Drop for LiveAggregateHarness {
    fn drop(&mut self) {
        self.service.cancel();
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

fn aggregate_graph(producers: &[ProducerFixture]) -> sql_graph::RuntimeFilterGraph {
    let capabilities = BTreeSet::from([
        sql_contract::ArtifactCapability::Membership,
        sql_contract::ArtifactCapability::EmptyDomain,
    ]);
    let contributions = BTreeSet::from([
        sql_contract::ContributionKind::FinalDomainShard,
        sql_contract::ContributionKind::ProducerClosed,
    ]);
    let coverage = SqlCoverage::AllOf(
        producers
            .iter()
            .map(|producer| {
                SqlCoverage::Leaf(sql_contract::CoverageWitnessId::new(producer.witness.get()))
            })
            .collect(),
    );
    let mut graph = sql_graph::RuntimeFilterGraph::default();
    graph
        .insert_channel(sql_graph::RuntimeFilterChannelSpec {
            channel_id: sql_contract::ChannelId::new(CHANNEL.get()),
            logical_domain: sql_contract::RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: sql_contract::NullSemantics::NullSafeEqual,
            },
            lifecycle: sql_contract::RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: coverage.clone(),
            terminal_coverage: coverage,
            reduction_requirement: sql_contract::ReductionRequirement::SetUnion,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: capabilities.clone(),
            policy: sql_contract::RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 4096,
                max_artifact_bytes: 4096,
                deadline_ms: 1000,
                max_retries: 1,
            },
        })
        .expect("insert aggregate channel");
    for (index, producer) in producers.iter().enumerate() {
        graph
            .insert_binding(sql_graph::RuntimeFilterBindingSpec {
                binding_id: sql_contract::BindingId::new(producer.binding.get()),
                channel_id: sql_contract::ChannelId::new(CHANNEL.get()),
                coverage_witness_id: Some(sql_contract::CoverageWitnessId::new(
                    producer.witness.get(),
                )),
                location: sql_graph::PlanLocation {
                    fragment_id: sql_contract::PlanFragmentId::new(producer.fragment.get()),
                    node_id: sql_contract::PlanNodeId::new(index as i32 + 1),
                },
                expression: expression(),
                apply_point: sql_graph::ApplyPoint::NodeOutput,
                role: sql_graph::RuntimeFilterBindingRole::Producer(
                    sql_graph::ProducerRequirement {
                        contribution_kinds: contributions.clone(),
                        completion_requirement:
                            sql_contract::CompletionRequirement::FencedFinalDomain(
                                sql_contract::CompletionFenceKind::CommittedDomainFrozen,
                            ),
                        target: sql_graph::ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
                    },
                ),
            })
            .expect("insert aggregate producer binding");
    }
    graph
        .insert_binding(sql_graph::RuntimeFilterBindingSpec {
            binding_id: sql_contract::BindingId::new(CONSUMER.get()),
            channel_id: sql_contract::ChannelId::new(CHANNEL.get()),
            coverage_witness_id: None,
            location: sql_graph::PlanLocation {
                fragment_id: sql_contract::PlanFragmentId::new(CONSUMER_FRAGMENT.get()),
                node_id: sql_contract::PlanNodeId::new(30),
            },
            expression: expression(),
            apply_point: sql_graph::ApplyPoint::NodeInput,
            role: sql_graph::RuntimeFilterBindingRole::Consumer(sql_graph::ConsumerRequirement {
                capabilities,
                activation: sql_contract::ConsumerActivation::NonBlockingLive {
                    late_apply: sql_contract::LateApplyGranularity::Batch,
                },
                target: sql_graph::ConsumerBindingTarget::SourceBoundary,
            }),
        })
        .expect("insert aggregate consumer binding");
    graph
        .validate()
        .expect("live aggregate graph validates before compilation");
    graph
}

fn fixture_endpoint() -> SocketAddr {
    "127.0.0.1:9460".parse().expect("fixture endpoint is valid")
}

fn placement(fragment: PlanFragmentId, instance: UniqueId) -> FragmentInstancePlacement {
    FragmentInstancePlacement {
        fragment_id: fragment.get(),
        instance_index: 0,
        finst_id: instance,
        backend_idx: 0,
        endpoint: RuntimeEndpoint::from_socket_addr(fixture_endpoint()),
        scan_ranges: BTreeMap::new(),
        connector_splits: BTreeMap::new(),
        destinations: Vec::new(),
        per_exch_num_senders: BTreeMap::new(),
    }
}

fn scheduling_plan(producers: &[ProducerFixture]) -> SchedulingPlan {
    let mut by_fragment = producers
        .iter()
        .map(|producer| {
            (
                producer.fragment.get(),
                vec![placement(producer.fragment, producer.instance)],
            )
        })
        .collect::<BTreeMap<_, _>>();
    by_fragment.insert(
        CONSUMER_FRAGMENT.get(),
        vec![placement(CONSUMER_FRAGMENT, CONSUMER_INSTANCE)],
    );
    SchedulingPlan {
        root_fragment_id: CONSUMER_FRAGMENT.get(),
        by_fragment,
        root_finst_id: CONSUMER_INSTANCE,
        root_backend_idx: 0,
    }
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
    graph: &sql_graph::RuntimeFilterGraph,
    scheduling: &SchedulingPlan,
    edges: &[FragmentEdge],
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
    .expect("aggregate graph compiles against the live placement");
    RuntimeFilterParticipantInstall::new(
        plan.install_views
            .remove(&PARTICIPANT)
            .expect("compiler projects the aggregate service install"),
        plan.routing_shards
            .remove(&PARTICIPANT)
            .expect("compiler projects the aggregate service routes"),
    )
}

fn install_service(install: RuntimeFilterParticipantInstall) -> Arc<RuntimeFilterService> {
    let memory: Arc<dyn RuntimeFilterMemoryAccount> =
        MemTrackerMemoryAccount::new_root_for_test("live-aggregate-conformance");
    let service = Arc::new(RuntimeFilterService::new_with_dependencies(
        UniqueId::new(406, 0),
        Arc::new(DeterministicClock(Instant::now())),
        Arc::new(DiscardEvents),
        memory,
    ));
    assert_eq!(
        service.install(install).expect("install aggregate service"),
        InstallOutcome::Installed
    );
    service
}

fn aggregate_factory(session: AggregateFinalDomainSessionBuilder) -> AggregateProcessorFactory {
    let mut arena = ExprArena::default();
    let group_expr = arena.push_typed(ExprNode::SlotId(GROUP_SLOT), DataType::Int64);
    let output_field = Field::new("group_key", DataType::Int64, true);
    let output_schema = Arc::new(
        ChunkSchema::try_new(vec![
            ChunkSlotSchema::from_field(GROUP_SLOT, &output_field, None)
                .expect("aggregate output slot"),
        ])
        .expect("aggregate output schema"),
    );
    AggregateProcessorFactory::new_native(
        401,
        Arc::new(arena),
        vec![group_expr],
        Vec::new(),
        false,
        true,
        output_schema,
        Vec::new(),
        None,
        AGGREGATE_DOP,
        Some(session),
    )
    .expect("build aggregate factory")
}

fn group_chunk(values: &[i64]) -> Chunk {
    let field = Field::new("group_key", DataType::Int64, false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field.clone()])),
        vec![Arc::new(Int64Array::from(values.to_vec())) as ArrayRef],
    )
    .expect("aggregate input batch");
    let chunk_schema = Arc::new(
        ChunkSchema::try_new(vec![
            ChunkSlotSchema::from_field(GROUP_SLOT, &field, None).expect("aggregate input slot"),
        ])
        .expect("aggregate input schema"),
    );
    Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("aggregate input chunk")
}

fn membership_i64_values(bundle: &ArtifactBundle) -> BTreeSet<i64> {
    let (artifact, index) = bundle
        .artifacts()
        .iter()
        .find_map(|(_, artifact)| artifact.membership_index().map(|index| (artifact, index)))
        .expect("published membership artifact carries a resident index");
    match index.view() {
        ResidentMembershipIndexView::Fixed {
            values,
            count,
            width,
            ..
        } => {
            assert_eq!(width, std::mem::size_of::<i64>());
            let decoded = artifact.canonical_bytes()[values.clone()]
                .chunks_exact(width)
                .map(|bytes| i64::from_be_bytes(bytes.try_into().expect("i64 bytes")))
                .collect::<BTreeSet<_>>();
            assert_eq!(decoded.len(), count);
            decoded
        }
        other => panic!("expected fixed-width membership index, got {other:?}"),
    }
}

fn expect_completed_union(
    live: &Arc<dyn NonBlockingLiveSubscription>,
    expected: impl IntoIterator<Item = i64>,
) -> Arc<ArtifactBundle> {
    let LivePollOutcome::Updated {
        bundle,
        terminal: Some(LiveTerminal::Completed),
    } = live.poll_after(None)
    else {
        panic!("all frozen aggregate witnesses must publish one completed artifact")
    };
    assert_eq!(bundle.version(), LogicalVersion::FIRST);
    assert_eq!(
        membership_i64_values(&bundle),
        expected.into_iter().collect()
    );
    bundle
}

#[test]
fn live_aggregate_final_domain_requires_all_frozen_witnesses() {
    let mut harness = LiveAggregateHarness::new();
    harness.finish_driver(Witness::A, 0, &[1, 2]);
    harness.finish_driver(Witness::A, 1, &[3]);

    assert!(harness.live.snapshot().is_none());
    assert!(matches!(
        harness.live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: None,
        }
    ));

    harness.finish_driver(Witness::B, 0, &[3, 4]);
    harness.finish_driver(Witness::B, 1, &[5]);
    expect_completed_union(&harness.live, [1, 2, 3, 4, 5]);
}

#[test]
fn live_aggregate_dop_two_waits_for_last_driver() {
    let mut harness = LiveAggregateHarness::new();
    harness.finish_driver(Witness::B, 0, &[20]);
    harness.finish_driver(Witness::B, 1, &[21]);
    harness.finish_driver(Witness::A, 0, &[10]);

    assert!(harness.live.snapshot().is_none());
    assert!(matches!(
        harness.live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: None,
        }
    ));

    harness.finish_driver(Witness::A, 1, &[11]);
    expect_completed_union(&harness.live, [10, 11, 20, 21]);
}

#[test]
fn live_aggregate_out_of_order_finish_materializes_once() {
    let mut harness = LiveAggregateHarness::new();
    harness.finish_driver(Witness::B, 1, &[22]);
    harness.finish_driver(Witness::A, 0, &[10]);
    harness.finish_driver(Witness::B, 0, &[20, 21]);
    assert!(harness.live.snapshot().is_none());

    harness.finish_driver(Witness::A, 1, &[11, 12]);
    let first = expect_completed_union(&harness.live, [10, 11, 12, 20, 21, 22]);
    assert!(matches!(
        harness.live.poll_after(Some(first.version())),
        LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: Some(LiveTerminal::Completed),
        }
    ));
    let snapshot = harness
        .live
        .snapshot()
        .expect("completed aggregate retains version one");
    assert_eq!(snapshot.version(), LogicalVersion::FIRST);
    assert_eq!(
        membership_i64_values(&snapshot),
        BTreeSet::from([10, 11, 12, 20, 21, 22])
    );
}

#[test]
fn live_aggregate_empty_is_completed_not_unavailable() {
    let mut harness = LiveAggregateHarness::new();
    harness.finish_driver(Witness::A, 0, &[]);
    harness.finish_driver(Witness::B, 1, &[]);
    harness.finish_driver(Witness::A, 1, &[]);
    harness.finish_driver(Witness::B, 0, &[]);

    let LivePollOutcome::Updated {
        bundle,
        terminal: Some(LiveTerminal::Completed),
    } = harness.live.poll_after(None)
    else {
        panic!("an exact empty aggregate domain must complete with an artifact")
    };
    assert_eq!(bundle.version(), LogicalVersion::FIRST);
    let [(ArtifactKind::EmptyDomain, artifact)] = bundle.artifacts() else {
        panic!("an exact empty aggregate domain must publish EmptyDomain")
    };
    assert!(matches!(
        artifact
            .membership_index()
            .expect("EmptyDomain carries a resident index")
            .view(),
        ResidentMembershipIndexView::EmptyDomain
    ));
}

#[test]
fn live_aggregate_failed_witness_never_publishes_partial_union() {
    let mut harness = LiveAggregateHarness::new();
    harness.finish_driver(Witness::A, 0, &[1]);
    harness.finish_driver(Witness::A, 1, &[2]);
    assert!(harness.live.snapshot().is_none());

    harness.drop_driver(Witness::B, 1);
    assert!(harness.live.snapshot().is_none());
    assert!(matches!(
        harness.live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(UnavailableReason::ProducerFailed)),
        }
    ));

    harness.finish_driver(Witness::B, 0, &[3]);
    assert!(harness.live.snapshot().is_none());
    assert!(matches!(
        harness.live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(UnavailableReason::ProducerFailed)),
        }
    ));
}
