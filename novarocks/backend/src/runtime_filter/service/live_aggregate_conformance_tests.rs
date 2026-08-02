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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_execution::runtime_filter as execution;
use novarocks_execution::runtime_filter::RuntimeFilterSession;

use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use novarocks::exec::expr::{ExprArena, ExprNode};
use novarocks::exec::operators::{AggregateFinalDomainSessionBuilder, AggregateProcessorFactory};
use novarocks::exec::pipeline::operator::Operator;
use novarocks::exec::pipeline::operator_factory::OperatorFactory;
use novarocks::runtime::runtime_state::RuntimeState;
use novarocks::runtime_filter_transition::model::contract::{BindingId, ChannelId};
use novarocks::runtime_filter_transition::port::artifact::{
    ArtifactBundle, ArtifactKind, ResidentMembershipIndexView,
};
use novarocks::runtime_filter_transition::port::events::{
    RuntimeFilterEvent, RuntimeFilterEventSink,
};
use novarocks::runtime_filter_transition::port::identity::{DeploymentEpoch, LogicalVersion};
use novarocks::runtime_filter_transition::port::install::RuntimeFilterParticipantInstall;
use novarocks::runtime_filter_transition::port::producer::{InstallOutcome, ProducerPortKind};
use novarocks::runtime_filter_transition::port::subscription::{
    LivePollOutcome, LiveTerminal, NonBlockingLiveSubscription, UnavailableReason,
};
use novarocks::runtime_filter_transition::port::support::{
    RuntimeFilterClock, RuntimeFilterMemoryAccount,
};
use novarocks::runtime_filter_transition::test_support::compiled_live_final_domain_fixture;
use novarocks_types::UniqueId;

use super::RuntimeFilterService;
use super::memory::MemTrackerMemoryAccount;
use super::native_execution::NativeRuntimeFilterExecutionContext;

const CHANNEL: ChannelId = ChannelId::new(1);
const PRODUCER_A: BindingId = BindingId::new(10);
const PRODUCER_B: BindingId = BindingId::new(20);
const CONSUMER: BindingId = BindingId::new(30);
const INSTANCE_A: UniqueId = UniqueId::new(94, 10);
const INSTANCE_B: UniqueId = UniqueId::new(94, 20);
const CONSUMER_INSTANCE: UniqueId = UniqueId::new(94, 30);
const AGGREGATE_DOP: i32 = 2;
const GROUP_SLOT: SlotId = SlotId::new(401);

#[derive(Clone, Copy, Debug)]
enum Witness {
    A,
    B,
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
            .resolve_producer(binding, CHANNEL, ProducerPortKind::FinalDomain)
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
        let fixture = compiled_live_final_domain_fixture();
        assert_eq!(fixture.channel_id(), CHANNEL);
        assert_eq!(fixture.producers().len(), 2);
        assert_eq!(fixture.producers()[0].binding_id(), PRODUCER_A);
        assert_eq!(fixture.producers()[0].instance_id(), INSTANCE_A);
        assert_eq!(fixture.producers()[1].binding_id(), PRODUCER_B);
        assert_eq!(fixture.producers()[1].instance_id(), INSTANCE_B);
        assert_eq!(fixture.consumer().binding_id(), CONSUMER);
        assert_eq!(fixture.consumer().instance_id(), CONSUMER_INSTANCE);
        let service = install_service(fixture.into_install());
        let producer_a = WitnessProcessors::open(&service, PRODUCER_A, INSTANCE_A);
        let producer_b = WitnessProcessors::open(&service, PRODUCER_B, INSTANCE_B);
        let live = service
            .subscribe(
                CONSUMER,
                CONSUMER_INSTANCE,
                novarocks::runtime_filter_transition::port::subscription::SubscriptionKind::NonBlockingLive,
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
