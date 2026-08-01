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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use arrow::datatypes::DataType;

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::*;
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::artifact::ConsumerArtifactProfile;
use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
use crate::runtime_filter::port::identity::*;
use crate::runtime_filter::port::install::*;
use crate::runtime_filter::port::ordered_bound::{
    COMPARATOR_ALGORITHM_VERSION, OrderedBoundUpdate, OrderedScalar, OrderedTuple,
    RuntimeOrderContract, comparator_digest_for_test,
};
use crate::runtime_filter::port::producer::{
    ProducerHandle, ProducerPortKind, RuntimeContractViolationKind, SubmitOutcome,
};
use crate::runtime_filter::port::subscription::{
    LivePollOutcome, LiveTerminal, SubscriptionHandle, SubscriptionKind,
};
use crate::runtime_filter::port::support::{
    MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount, TemporaryContributionLease,
};
use crate::runtime_filter::port::topk_summary::{RuntimeTopKSummaryContract, TopKSummary};

use super::RuntimeFilterService;
use super::memory::MemTrackerMemoryAccount;

const PRODUCER_A: BindingId = BindingId::new(1);
const CONSUMER: BindingId = BindingId::new(2);
const PRODUCER_B: BindingId = BindingId::new(3);

fn uid(lo: i64) -> UniqueId {
    UniqueId::new(80, lo)
}

#[derive(Default)]
struct Events(Mutex<Vec<RuntimeFilterEvent>>);

impl RuntimeFilterEventSink for Events {
    fn record(&self, event: RuntimeFilterEvent) {
        self.0.lock().unwrap().push(event);
    }
}

struct Clock(Instant);

impl RuntimeFilterClock for Clock {
    fn now(&self) -> Instant {
        self.0
    }
}

#[derive(Default)]
struct ArmableMemoryAccount {
    rejecting: AtomicBool,
    current: AtomicUsize,
}

#[derive(Default)]
struct TopKReentrantEvents {
    recorded: Mutex<Vec<RuntimeFilterEvent>>,
    producer: Mutex<
        Option<
            std::sync::Weak<dyn crate::runtime_filter::port::producer::TopKSummaryProducerAdapter>,
        >,
    >,
    replay: Mutex<Option<TopKSummary>>,
    panicked: AtomicBool,
    reentered: AtomicBool,
}

struct CrossChannelTopKEvents {
    recorded: Mutex<Vec<RuntimeFilterEvent>>,
    trigger_channel: ChannelId,
    trigger_binding: BindingId,
    nested_producer: Mutex<
        Option<
            std::sync::Weak<dyn crate::runtime_filter::port::producer::TopKSummaryProducerAdapter>,
        >,
    >,
    nested_summary: Mutex<Option<TopKSummary>>,
    fired: AtomicBool,
}

impl RuntimeFilterEventSink for CrossChannelTopKEvents {
    fn record(&self, event: RuntimeFilterEvent) {
        self.recorded.lock().unwrap().push(event.clone());
        let RuntimeFilterEvent::TopKSummaryApplied { identity } = event else {
            return;
        };
        if identity.channel_id() != self.trigger_channel
            || identity.stream().binding_id() != self.trigger_binding
            || self.fired.swap(true, Ordering::SeqCst)
        {
            return;
        }
        let producer = self
            .nested_producer
            .lock()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .expect("cross-channel top-k producer remains live");
        let summary = self
            .nested_summary
            .lock()
            .unwrap()
            .clone()
            .expect("cross-channel reentry owns a nested summary");
        assert_eq!(
            producer
                .submit_summary(PartitionId::new(0), ProducerSequence::new(0), summary,)
                .unwrap(),
            SubmitOutcome::Published
        );
    }
}

impl RuntimeFilterEventSink for TopKReentrantEvents {
    fn record(&self, event: RuntimeFilterEvent) {
        self.recorded.lock().unwrap().push(event.clone());
        if !matches!(event, RuntimeFilterEvent::TopKSummaryApplied { .. }) {
            return;
        }
        if !self.panicked.swap(true, Ordering::SeqCst) {
            panic!("intentional top-k event sink panic");
        }
        if self.reentered.swap(true, Ordering::SeqCst) {
            return;
        }
        let producer = self
            .producer
            .lock()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .expect("top-k reentry producer remains live");
        let replay = self
            .replay
            .lock()
            .unwrap()
            .clone()
            .expect("top-k reentry owns a replay summary");
        assert_eq!(
            producer
                .submit_summary(PartitionId::new(0), ProducerSequence::new(0), replay,)
                .unwrap(),
            SubmitOutcome::Duplicate
        );
    }
}

impl RuntimeFilterMemoryAccount for ArmableMemoryAccount {
    fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
        if self.rejecting.load(Ordering::SeqCst) {
            return Err(MemoryAccountError::CapacityExceeded);
        }
        self.current.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        let previous = self.current.fetch_sub(bytes, Ordering::SeqCst);
        assert!(previous >= bytes);
    }
}

struct Fixture {
    service: Arc<RuntimeFilterService>,
    contract: Arc<RuntimeTopKSummaryContract>,
    events: Arc<Events>,
}

fn topk_deployment(
    plan: OrderContract,
    requirement: TopKSummaryRequirement,
) -> RuntimeFilterChannelDeployment {
    topk_deployment_with_ids(
        plan,
        requirement,
        ChannelId::new(1),
        (PRODUCER_A, CoverageWitnessId::new(1), uid(1)),
        (PRODUCER_B, CoverageWitnessId::new(2), uid(3)),
        (CONSUMER, RouteEdgeId::new(1), uid(2)),
    )
}

fn topk_deployment_with_ids(
    plan: OrderContract,
    requirement: TopKSummaryRequirement,
    channel_id: ChannelId,
    producer_a: (BindingId, CoverageWitnessId, UniqueId),
    producer_b: (BindingId, CoverageWitnessId, UniqueId),
    consumer: (BindingId, RouteEdgeId, UniqueId),
) -> RuntimeFilterChannelDeployment {
    let range_contract = RuntimeOrderContract::try_from_plan(&plan).unwrap();
    let witnesses = [producer_a.1, producer_b.1];
    let coverage = Coverage::AllOf(witnesses.into_iter().map(Coverage::Leaf).collect());
    RuntimeFilterChannelDeployment::new(
        channel_id,
        RuntimeFilterLogicalDomain::OrderedBound(plan),
        RuntimeFilterLifecycle::MonotonicUpdates,
        coverage.clone(),
        coverage,
        ReductionRequirement::MergeTopKSummary(requirement),
        BTreeSet::from([
            ContributionKind::TopKSummary,
            ContributionKind::ProducerClosed,
        ]),
        CompletionRequirement::ProducerClosed,
        RuntimeFilterPolicyRequirement {
            max_contribution_bytes: 4096,
            max_artifact_bytes: 4096,
            deadline_ms: 100,
            max_retries: 1,
        },
        RuntimeFilterCoreBudget::new(16 * 1024),
        MaterializationPolicy::for_test(),
        BTreeMap::from([
            (
                producer_a.0,
                ProducerDeployment::new(witnesses[0], BTreeSet::from([producer_a.2])),
            ),
            (
                producer_b.0,
                ProducerDeployment::new(witnesses[1], BTreeSet::from([producer_b.2])),
            ),
        ]),
        BTreeMap::from([(
            consumer.0,
            ConsumerDeployment::with_profile(
                ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
                BTreeSet::from([ArtifactCapability::OrderedRange]),
                ConsumerArtifactProfile::new_ordered_range(range_contract.digest()).unwrap(),
                BTreeSet::from([consumer.1]),
                BTreeSet::from([consumer.2]),
            ),
        )]),
    )
}

fn fixture_with_account(memory: Arc<dyn RuntimeFilterMemoryAccount>) -> Fixture {
    let events = Arc::new(Events::default());
    let (service, contract) = installed_service_with_sink(memory, events.clone());
    Fixture {
        service,
        contract,
        events,
    }
}

fn installed_service_with_sink(
    memory: Arc<dyn RuntimeFilterMemoryAccount>,
    events: Arc<dyn RuntimeFilterEventSink>,
) -> (Arc<RuntimeFilterService>, Arc<RuntimeTopKSummaryContract>) {
    let service = Arc::new(RuntimeFilterService::new_with_dependencies(
        uid(0),
        Arc::new(Clock(Instant::now())),
        events,
        memory,
    ));
    let keys = vec![OrderKeyContract {
        data_type: DataType::Int64,
        direction: SortDirection::Ascending,
        null_order: NullOrder::Last,
    }];
    let plan = OrderContract {
        comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
        keys,
        inclusive: true,
    };
    let requirement = TopKSummaryRequirement::try_new(4).unwrap();
    let contract = Arc::new(RuntimeTopKSummaryContract::try_from_plan(&plan, requirement).unwrap());
    let deployment = topk_deployment(plan, requirement);
    service
        .install(local_participant_install_for_test(
            RuntimeFilterInstallView::new(
                DeploymentEpoch::new(9),
                RuntimeFilterParticipantId::new(3),
                BTreeMap::from([(ChannelId::new(1), deployment)]),
            ),
        ))
        .unwrap();
    (service, contract)
}

fn fixture() -> Fixture {
    fixture_with_account(MemTrackerMemoryAccount::new_root_for_test(
        "topk-summary-service-test",
    ))
}

fn summary(contract: &RuntimeTopKSummaryContract, values: &[i64]) -> TopKSummary {
    TopKSummary::try_new(
        contract,
        values
            .iter()
            .map(|value| {
                OrderedTuple::try_new(contract.order(), [Some(OrderedScalar::Int64(*value))])
                    .unwrap()
            })
            .collect(),
    )
    .unwrap()
}

fn summary_producer(
    service: &RuntimeFilterService,
    binding: BindingId,
    instance: UniqueId,
) -> Arc<dyn crate::runtime_filter::port::producer::TopKSummaryProducerAdapter> {
    let ProducerHandle::TopKSummary(producer) = service
        .open_producer(binding, instance, 1, ProducerPortKind::TopKSummary)
        .unwrap()
    else {
        panic!("top-k route must return a summary producer")
    };
    producer
}

fn live(
    service: &RuntimeFilterService,
) -> Arc<dyn crate::runtime_filter::port::subscription::NonBlockingLiveSubscription> {
    let SubscriptionHandle::Live(live) = service
        .subscribe(CONSUMER, uid(2), SubscriptionKind::NonBlockingLive)
        .unwrap()
    else {
        panic!("top-k range consumer must be live")
    };
    live
}

fn range_value(outcome: LivePollOutcome) -> (LogicalVersion, i64, Option<LiveTerminal>) {
    let LivePollOutcome::Updated { bundle, terminal } = outcome else {
        panic!("expected a live range update")
    };
    let [(crate::runtime_filter::port::artifact::ArtifactKind::Range, artifact)] =
        bundle.artifacts()
    else {
        panic!("expected exactly one range artifact")
    };
    let [Some(OrderedScalar::Int64(value))] = artifact.range().unwrap().bound().values() else {
        panic!("expected an int64 range bound")
    };
    (bundle.version(), *value, terminal)
}

#[test]
fn topk_open_returns_summary_handle_and_rejects_logical_domain_ports() {
    let fixture = fixture();
    for wrong in [ProducerPortKind::Membership, ProducerPortKind::OrderedBound] {
        assert_eq!(
            fixture
                .service
                .open_producer(PRODUCER_A, uid(1), 1, wrong)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::ProducerPortMismatch
        );
    }
    assert!(matches!(
        fixture
            .service
            .open_producer(PRODUCER_A, uid(1), 1, ProducerPortKind::TopKSummary)
            .unwrap(),
        ProducerHandle::TopKSummary(_)
    ));
}

#[test]
fn direct_and_topk_ports_never_alias_cached_handles() {
    let events = Arc::new(Events::default());
    let service = Arc::new(RuntimeFilterService::new_with_dependencies(
        uid(0),
        Arc::new(Clock(Instant::now())),
        events,
        MemTrackerMemoryAccount::new_root_for_test("direct-topk-cache-identity"),
    ));
    let keys = vec![OrderKeyContract {
        data_type: DataType::Int64,
        direction: SortDirection::Ascending,
        null_order: NullOrder::Last,
    }];
    let plan = OrderContract {
        comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
        keys,
        inclusive: true,
    };
    let runtime_order = Arc::new(RuntimeOrderContract::try_from_plan(&plan).unwrap());
    let requirement = TopKSummaryRequirement::try_new(4).unwrap();
    let topk_contract =
        Arc::new(RuntimeTopKSummaryContract::try_from_plan(&plan, requirement).unwrap());
    let direct_witness = CoverageWitnessId::new(10);
    let direct_deployment = RuntimeFilterChannelDeployment::new(
        ChannelId::new(2),
        RuntimeFilterLogicalDomain::OrderedBound(plan.clone()),
        RuntimeFilterLifecycle::MonotonicUpdates,
        Coverage::Leaf(direct_witness),
        Coverage::Leaf(direct_witness),
        ReductionRequirement::TightenOrderedBound,
        BTreeSet::from([
            ContributionKind::OrderedBoundUpdate,
            ContributionKind::ProducerClosed,
        ]),
        CompletionRequirement::ProducerClosed,
        RuntimeFilterPolicyRequirement {
            max_contribution_bytes: 4096,
            max_artifact_bytes: 4096,
            deadline_ms: 100,
            max_retries: 1,
        },
        RuntimeFilterCoreBudget::new(16 * 1024),
        MaterializationPolicy::for_test(),
        BTreeMap::from([(
            BindingId::new(10),
            ProducerDeployment::new(direct_witness, BTreeSet::from([uid(10)])),
        )]),
        BTreeMap::from([(
            BindingId::new(11),
            ConsumerDeployment::with_profile(
                ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
                BTreeSet::from([ArtifactCapability::OrderedRange]),
                ConsumerArtifactProfile::new_ordered_range(runtime_order.digest()).unwrap(),
                BTreeSet::from([RouteEdgeId::new(2)]),
                BTreeSet::from([uid(11)]),
            ),
        )]),
    );
    service
        .install(local_participant_install_for_test(
            RuntimeFilterInstallView::new(
                DeploymentEpoch::new(9),
                RuntimeFilterParticipantId::new(3),
                BTreeMap::from([
                    (ChannelId::new(1), topk_deployment(plan, requirement)),
                    (ChannelId::new(2), direct_deployment),
                ]),
            ),
        ))
        .unwrap();

    let topk = summary_producer(&service, PRODUCER_A, uid(1));
    let topk_again = summary_producer(&service, PRODUCER_A, uid(1));
    assert!(Arc::ptr_eq(&topk, &topk_again));
    let ProducerHandle::OrderedBound(direct) = service
        .open_producer(
            BindingId::new(10),
            uid(10),
            1,
            ProducerPortKind::OrderedBound,
        )
        .unwrap()
    else {
        panic!("direct route must return an ordered-bound producer")
    };
    let ProducerHandle::OrderedBound(direct_again) = service
        .open_producer(
            BindingId::new(10),
            uid(10),
            1,
            ProducerPortKind::OrderedBound,
        )
        .unwrap()
    else {
        panic!("cached direct route must remain ordered-bound")
    };
    assert!(Arc::ptr_eq(&direct, &direct_again));
    assert_eq!(
        service
            .open_producer(
                BindingId::new(10),
                uid(10),
                1,
                ProducerPortKind::TopKSummary,
            )
            .unwrap_err()
            .kind(),
        RuntimeContractViolationKind::ProducerPortMismatch
    );
    assert_eq!(
        service
            .open_producer(PRODUCER_A, uid(1), 1, ProducerPortKind::OrderedBound)
            .unwrap_err()
            .kind(),
        RuntimeContractViolationKind::ProducerPortMismatch
    );

    let direct_live = service
        .subscribe(
            BindingId::new(11),
            uid(11),
            SubscriptionKind::NonBlockingLive,
        )
        .unwrap()
        .into_live()
        .unwrap();
    let topk_live = service
        .subscribe(CONSUMER, uid(2), SubscriptionKind::NonBlockingLive)
        .unwrap()
        .into_live()
        .unwrap();
    direct
        .submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(0),
            OrderedBoundUpdate::new(
                &runtime_order,
                OrderedTuple::try_new(&runtime_order, [Some(OrderedScalar::Int64(99))]).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
    topk.submit_summary(
        PartitionId::new(0),
        ProducerSequence::new(0),
        summary(&topk_contract, &[1, 4]),
    )
    .unwrap();
    summary_producer(&service, PRODUCER_B, uid(3))
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&topk_contract, &[2, 2]),
        )
        .unwrap();
    assert_eq!(range_value(direct_live.poll_after(None)).1, 99);
    assert_eq!(range_value(topk_live.poll_after(None)).1, 4);
}

#[test]
fn topk_service_reuses_live_range_versions_and_exposes_terminal() {
    let fixture = fixture();
    let live = live(&fixture.service);
    let first = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    let second = summary_producer(&fixture.service, PRODUCER_B, uid(3));

    assert_eq!(
        first
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(0),
                summary(&fixture.contract, &[1, 4]),
            )
            .unwrap(),
        SubmitOutcome::StreamAcceptedNoGlobalChange
    );
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: None
        }
    ));
    assert_eq!(
        second
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(0),
                summary(&fixture.contract, &[2, 2]),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert_eq!(
        range_value(live.poll_after(None)),
        (LogicalVersion::FIRST, 4, None)
    );

    assert_eq!(
        first
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(1),
                summary(&fixture.contract, &[0, 1, 3, 4]),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert_eq!(
        range_value(live.poll_after(Some(LogicalVersion::FIRST))),
        (LogicalVersion::new(2), 2, None)
    );
    assert_eq!(
        first
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(2),
                summary(&fixture.contract, &[0, 1, 2, 4]),
            )
            .unwrap(),
        SubmitOutcome::StreamAcceptedNoGlobalChange
    );
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::new(2))),
        LivePollOutcome::Idle {
            latest_version: Some(version),
            terminal: None
        } if version == LogicalVersion::new(2)
    ));

    assert_ne!(
        first
            .close_partition(PartitionId::new(0), ProducerSequence::new(3))
            .unwrap(),
        SubmitOutcome::Completed
    );
    assert_eq!(
        second
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
            .unwrap(),
        SubmitOutcome::Completed
    );
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::new(2))),
        LivePollOutcome::Idle {
            latest_version: Some(version),
            terminal: Some(LiveTerminal::Completed)
        } if version == LogicalVersion::new(2)
    ));
}

#[test]
fn topk_resource_failure_retains_latest_as_degraded() {
    let before_account = Arc::new(ArmableMemoryAccount::default());
    let before = fixture_with_account(before_account.clone());
    let before_live = live(&before.service);
    let before_producer = summary_producer(&before.service, PRODUCER_A, uid(1));
    before_account.rejecting.store(true, Ordering::SeqCst);
    assert_eq!(
        before_producer
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(0),
                summary(&before.contract, &[1, 4]),
            )
            .unwrap(),
        SubmitOutcome::TerminalNoop
    );
    assert!(matches!(
        before_live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(
                crate::runtime_filter::port::subscription::UnavailableReason::ResourceLimit
            ))
        }
    ));
    before_account.rejecting.store(false, Ordering::SeqCst);
    drop(before);
    assert_eq!(before_account.current.load(Ordering::SeqCst), 0);

    let after_account = Arc::new(ArmableMemoryAccount::default());
    let after = fixture_with_account(after_account.clone());
    let after_live = live(&after.service);
    let first = summary_producer(&after.service, PRODUCER_A, uid(1));
    let second = summary_producer(&after.service, PRODUCER_B, uid(3));
    first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&after.contract, &[1, 4]),
        )
        .unwrap();
    assert_eq!(
        second
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(0),
                summary(&after.contract, &[2, 2]),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert_eq!(
        range_value(after_live.poll_after(None)),
        (LogicalVersion::FIRST, 4, None)
    );
    let retained_before_failure = after_account.current.load(Ordering::SeqCst);
    after_account.rejecting.store(true, Ordering::SeqCst);
    assert_eq!(
        first
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(1),
                summary(&after.contract, &[0, 1, 3, 4]),
            )
            .unwrap(),
        SubmitOutcome::TerminalNoop
    );
    assert_eq!(
        range_value(after_live.poll_after(None)),
        (
            LogicalVersion::FIRST,
            4,
            Some(LiveTerminal::DegradedLogical(
                crate::runtime_filter::port::subscription::UnavailableReason::ResourceLimit
            ))
        )
    );
    assert!(after_account.current.load(Ordering::SeqCst) <= retained_before_failure);
    after_account.rejecting.store(false, Ordering::SeqCst);
    drop(first);
    drop(second);
    drop(after_live);
    drop(after);
    assert_eq!(after_account.current.load(Ordering::SeqCst), 0);
}

#[test]
fn topk_shutdown_joins_caller_owned_materialization() {
    let account = Arc::new(ArmableMemoryAccount::default());
    let Fixture {
        service,
        contract,
        events,
    } = fixture_with_account(account.clone());
    let live = live(&service);
    let first = summary_producer(&service, PRODUCER_A, uid(1));
    let second = summary_producer(&service, PRODUCER_B, uid(3));
    first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&contract, &[1, 4]),
        )
        .unwrap();

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let active = Arc::new(AtomicUsize::new(0));
    service.set_before_encode_hook(Arc::new({
        let active = active.clone();
        let release_rx = release_rx.clone();
        move |_| {
            active.fetch_add(1, Ordering::SeqCst);
            entered_tx.send(()).unwrap();
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
        }
    }));
    service.set_after_encode_hook(Arc::new({
        let active = active.clone();
        move |_| {
            active.fetch_sub(1, Ordering::SeqCst);
        }
    }));
    let contract_for_submit = contract.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let submit = std::thread::spawn(move || {
        done_tx
            .send(second.submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(0),
                summary(&contract_for_submit, &[2, 2]),
            ))
            .unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 1);

    service.shutdown();
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            terminal: Some(LiveTerminal::Cancelled),
            ..
        }
    ));
    let weak_service = Arc::downgrade(&service);
    let weak_dispatcher = Arc::downgrade(&service.dispatcher);
    let weak_registry = Arc::downgrade(&service.registry);
    drop(service);
    assert!(weak_service.upgrade().is_none());
    assert_eq!(active.load(Ordering::SeqCst), 1);

    release_tx.send(()).unwrap();
    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap(),
        SubmitOutcome::Published
    );
    submit.join().unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(!events.0.lock().unwrap().iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::ArtifactPublished { .. } | RuntimeFilterEvent::LoopbackDelivered { .. }
    )));
    drop(first);
    drop(live);
    drop(contract);
    assert!(weak_dispatcher.upgrade().is_none());
    assert!(weak_registry.upgrade().is_none());
    assert_eq!(account.current.load(Ordering::SeqCst), 0);
}

#[test]
fn topk_event_sink_reentry_preserves_causal_order() {
    let sink = Arc::new(TopKReentrantEvents::default());
    let (service, contract) = installed_service_with_sink(
        MemTrackerMemoryAccount::new_root_for_test("topk-event-reentry"),
        sink.clone(),
    );
    let first = summary_producer(&service, PRODUCER_A, uid(1));
    let second = summary_producer(&service, PRODUCER_B, uid(3));
    first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&contract, &[1, 4]),
        )
        .unwrap();
    let second_summary = summary(&contract, &[2, 2]);
    *sink.producer.lock().unwrap() = Some(Arc::downgrade(&second));
    *sink.replay.lock().unwrap() = Some(second_summary.clone());
    assert_eq!(
        second
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(0),
                second_summary,
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert!(sink.panicked.load(Ordering::SeqCst));
    assert!(sink.reentered.load(Ordering::SeqCst));

    let events = sink.recorded.lock().unwrap().clone();
    let position = |predicate: fn(&RuntimeFilterEvent) -> bool| {
        events
            .iter()
            .position(predicate)
            .expect("expected top-k causal event")
    };
    let applied = position(|event| {
        matches!(
            event,
            RuntimeFilterEvent::TopKSummaryApplied { identity }
                if identity.stream().binding_id() == PRODUCER_B
        )
    });
    let tightened =
        position(|event| matches!(event, RuntimeFilterEvent::OrderedGlobalTightened { .. }));
    let published =
        position(|event| matches!(event, RuntimeFilterEvent::LogicalVersionPublished { .. }));
    let replayed = position(|event| {
        matches!(
            event,
            RuntimeFilterEvent::TopKSummaryEqual { identity }
                if identity.stream().binding_id() == PRODUCER_B
        )
    });
    assert!(applied < tightened);
    assert!(tightened < published);
    assert!(published < replayed);
}

#[test]
fn topk_public_port_cross_channel_reentry_preserves_causal_order_and_cache_identity() {
    let keys = vec![OrderKeyContract {
        data_type: DataType::Int64,
        direction: SortDirection::Ascending,
        null_order: NullOrder::Last,
    }];
    let plan = OrderContract {
        comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
        keys,
        inclusive: true,
    };
    let requirement = TopKSummaryRequirement::try_new(4).unwrap();
    let contract = Arc::new(RuntimeTopKSummaryContract::try_from_plan(&plan, requirement).unwrap());
    let sink = Arc::new(CrossChannelTopKEvents {
        recorded: Mutex::new(Vec::new()),
        trigger_channel: ChannelId::new(1),
        trigger_binding: PRODUCER_B,
        nested_producer: Mutex::new(None),
        nested_summary: Mutex::new(None),
        fired: AtomicBool::new(false),
    });
    let service = Arc::new(RuntimeFilterService::new_with_dependencies(
        uid(0),
        Arc::new(Clock(Instant::now())),
        sink.clone(),
        MemTrackerMemoryAccount::new_root_for_test("topk-cross-channel-reentry"),
    ));
    service
        .install(local_participant_install_for_test(
            RuntimeFilterInstallView::new(
                DeploymentEpoch::new(9),
                RuntimeFilterParticipantId::new(3),
                BTreeMap::from([
                    (
                        ChannelId::new(1),
                        topk_deployment(plan.clone(), requirement),
                    ),
                    (
                        ChannelId::new(2),
                        topk_deployment_with_ids(
                            plan,
                            requirement,
                            ChannelId::new(2),
                            (BindingId::new(11), CoverageWitnessId::new(11), uid(11)),
                            (BindingId::new(13), CoverageWitnessId::new(12), uid(13)),
                            (BindingId::new(12), RouteEdgeId::new(2), uid(12)),
                        ),
                    ),
                ]),
            ),
        ))
        .unwrap();

    let outer_first = summary_producer(&service, PRODUCER_A, uid(1));
    let outer_second = summary_producer(&service, PRODUCER_B, uid(3));
    let nested_first = summary_producer(&service, BindingId::new(11), uid(11));
    let nested_second = summary_producer(&service, BindingId::new(13), uid(13));
    assert!(!Arc::ptr_eq(&outer_second, &nested_second));
    assert!(Arc::ptr_eq(
        &nested_second,
        &summary_producer(&service, BindingId::new(13), uid(13))
    ));
    outer_first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&contract, &[1, 4]),
        )
        .unwrap();
    nested_first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&contract, &[10, 40]),
        )
        .unwrap();
    *sink.nested_producer.lock().unwrap() = Some(Arc::downgrade(&nested_second));
    *sink.nested_summary.lock().unwrap() = Some(summary(&contract, &[20, 20]));
    sink.recorded.lock().unwrap().clear();

    let (done_tx, done_rx) = mpsc::channel();
    let outer_summary = summary(&contract, &[2, 2]);
    let submit = std::thread::spawn(move || {
        done_tx
            .send(outer_second.submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(0),
                outer_summary,
            ))
            .unwrap();
    });
    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap(),
        SubmitOutcome::Published
    );
    submit.join().unwrap();
    assert!(sink.fired.load(Ordering::SeqCst));

    let events = sink.recorded.lock().unwrap().clone();
    let position = |predicate: &dyn Fn(&RuntimeFilterEvent) -> bool| {
        events
            .iter()
            .position(predicate)
            .expect("expected cross-channel top-k event")
    };
    let outer_applied = position(&|event| {
        matches!(
            event,
            RuntimeFilterEvent::TopKSummaryApplied { identity }
                if identity.channel_id() == ChannelId::new(1)
                    && identity.stream().binding_id() == PRODUCER_B
        )
    });
    let nested_applied = position(&|event| {
        matches!(
            event,
            RuntimeFilterEvent::TopKSummaryApplied { identity }
                if identity.channel_id() == ChannelId::new(2)
        )
    });
    let nested_published = position(&|event| {
        matches!(
            event,
            RuntimeFilterEvent::LogicalVersionPublished { identity, .. }
                if identity.channel_id() == ChannelId::new(2)
        )
    });
    let outer_tightened = position(&|event| {
        matches!(
            event,
            RuntimeFilterEvent::OrderedGlobalTightened { identity, .. }
                if identity.channel_id() == ChannelId::new(1)
        )
    });
    let outer_published = position(&|event| {
        matches!(
            event,
            RuntimeFilterEvent::LogicalVersionPublished { identity, .. }
                if identity.channel_id() == ChannelId::new(1)
        )
    });
    assert!(outer_applied < nested_applied);
    assert!(outer_applied < outer_tightened);
    assert!(outer_tightened < outer_published);
    assert!(outer_published < nested_applied);
    assert!(nested_applied < nested_published);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::LogicalVersionPublished { .. }))
            .count(),
        2
    );
}

#[test]
fn topk_service_emits_typed_input_events_and_keeps_global_event_names() {
    let fixture = fixture();
    let first = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    let second = summary_producer(&fixture.service, PRODUCER_B, uid(3));
    fixture.events.0.lock().unwrap().clear();

    first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(3),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap();
    first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(2),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap();
    first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(4),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap();
    second
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&fixture.contract, &[2, 2]),
        )
        .unwrap();

    let events = fixture.events.0.lock().unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::TopKStreamUpdated { identity }
            if identity.sequence() == ProducerSequence::new(3)
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::TopKSummaryStale { identity }
            if identity.sequence() == ProducerSequence::new(2)
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::TopKSummaryEqual { identity }
            if identity.sequence() == ProducerSequence::new(4)
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::TopKSummaryApplied { identity }
            if identity.sequence() == ProducerSequence::new(0)
                && identity.stream().binding_id() == PRODUCER_B
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::OrderedGlobalTightened { version, .. }
            if *version == LogicalVersion::FIRST
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::LogicalVersionPublished { version, .. }
            if *version == LogicalVersion::FIRST
    )));
}

#[test]
fn topk_exact_replay_emits_equal_without_publishing_another_version() {
    let fixture = fixture();
    let live = live(&fixture.service);
    let first = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    let second = summary_producer(&fixture.service, PRODUCER_B, uid(3));

    first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap();
    second
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&fixture.contract, &[2, 2]),
        )
        .unwrap();
    assert_eq!(
        range_value(live.poll_after(None)),
        (LogicalVersion::FIRST, 4, None)
    );
    fixture.events.0.lock().unwrap().clear();

    assert_eq!(
        second
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(0),
                summary(&fixture.contract, &[2, 2]),
            )
            .unwrap(),
        SubmitOutcome::Duplicate
    );
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            latest_version: Some(version),
            terminal: None
        } if version == LogicalVersion::FIRST
    ));
    let events = fixture.events.0.lock().unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::TopKSummaryEqual { identity }
            if identity.stream().binding_id() == PRODUCER_B
                && identity.stream().fragment_instance_id() == uid(3)
                && identity.stream().partition_id() == PartitionId::new(0)
                && identity.sequence() == ProducerSequence::new(0)
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::OrderedGlobalTightened { .. }
            | RuntimeFilterEvent::LogicalVersionPublished { .. }
    )));
}

#[test]
fn topk_public_port_concurrent_same_version_replays_do_not_duplicate_route_events() {
    let fixture = fixture();
    let live = live(&fixture.service);
    let first = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    let second = summary_producer(&fixture.service, PRODUCER_B, uid(3));
    first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap();
    let replay = summary(&fixture.contract, &[2, 2]);
    second
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            replay.clone(),
        )
        .unwrap();
    fixture.events.0.lock().unwrap().clear();

    let start = Arc::new(std::sync::Barrier::new(3));
    let (done_tx, done_rx) = mpsc::channel();
    let mut submits = Vec::new();
    for _ in 0..2 {
        let start = start.clone();
        let done_tx = done_tx.clone();
        let second = second.clone();
        let replay = replay.clone();
        submits.push(std::thread::spawn(move || {
            start.wait();
            done_tx
                .send(second.submit_summary(PartitionId::new(0), ProducerSequence::new(0), replay))
                .unwrap();
        }));
    }
    start.wait();
    for _ in 0..2 {
        assert_eq!(
            done_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Duplicate
        );
    }
    for submit in submits {
        submit.join().unwrap();
    }
    assert!(Arc::ptr_eq(
        &second,
        &summary_producer(&fixture.service, PRODUCER_B, uid(3))
    ));
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            latest_version: Some(version),
            terminal: None
        } if version == LogicalVersion::FIRST
    ));
    let events = fixture.events.0.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::TopKSummaryEqual { .. }))
            .count(),
        2
    );
    assert!(!events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::OrderedGlobalTightened { .. }
            | RuntimeFilterEvent::LogicalVersionPublished { .. }
            | RuntimeFilterEvent::ArtifactPublished { .. }
            | RuntimeFilterEvent::LoopbackDelivered { .. }
    )));
}

#[test]
fn topk_close_states_emit_typed_input_events_before_return() {
    let satisfied = fixture();
    let satisfied_producer = summary_producer(&satisfied.service, PRODUCER_A, uid(1));
    satisfied_producer
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&satisfied.contract, &[1, 4]),
        )
        .unwrap();
    satisfied.events.0.lock().unwrap().clear();

    assert_eq!(
        satisfied_producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
            .unwrap(),
        SubmitOutcome::Applied
    );
    assert!(
        satisfied
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(
                event,
                RuntimeFilterEvent::TopKSummaryApplied { identity }
                    if identity.stream().binding_id() == PRODUCER_A
                        && identity.sequence() == ProducerSequence::new(1)
            ))
    );
    satisfied.events.0.lock().unwrap().clear();

    assert_eq!(
        satisfied_producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
            .unwrap(),
        SubmitOutcome::Duplicate
    );
    assert!(
        satisfied
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(
                event,
                RuntimeFilterEvent::TopKSummaryEqual { identity }
                    if identity.stream().binding_id() == PRODUCER_A
                        && identity.sequence() == ProducerSequence::new(1)
            ))
    );

    let pending = fixture();
    let pending_producer = summary_producer(&pending.service, PRODUCER_A, uid(1));
    pending_producer
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&pending.contract, &[1, 4]),
        )
        .unwrap();
    pending.events.0.lock().unwrap().clear();

    assert_eq!(
        pending_producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(2))
            .unwrap(),
        SubmitOutcome::PendingFinalSnapshot
    );
    assert!(
        pending
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(
                event,
                RuntimeFilterEvent::TopKStreamUpdated { identity }
                    if identity.stream().binding_id() == PRODUCER_A
                        && identity.sequence() == ProducerSequence::new(2)
            ))
    );
}

#[test]
fn topk_close_preflight_rejections_are_typed_before_return() {
    let fixture = fixture();
    let producer = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    producer
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(1),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap();
    fixture.events.0.lock().unwrap().clear();

    let outside_terminal = producer
        .close_partition(PartitionId::new(0), ProducerSequence::new(1))
        .unwrap_err();
    assert_eq!(
        outside_terminal.kind(),
        RuntimeContractViolationKind::SequenceOutsideTerminalRange
    );
    assert!(
        fixture
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(
                event,
                RuntimeFilterEvent::TopKSummaryRejected { identity, violation }
                    if identity.sequence() == ProducerSequence::new(1)
                        && *violation
                            == RuntimeContractViolationKind::SequenceOutsideTerminalRange
            ))
    );

    producer
        .close_partition(PartitionId::new(0), ProducerSequence::new(2))
        .unwrap();
    fixture.events.0.lock().unwrap().clear();
    let conflicting = producer
        .close_partition(PartitionId::new(0), ProducerSequence::new(3))
        .unwrap_err();
    assert_eq!(
        conflicting.kind(),
        RuntimeContractViolationKind::ConflictingTerminalSequence
    );
    assert!(
        fixture
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(
                event,
                RuntimeFilterEvent::TopKSummaryRejected { identity, violation }
                    if identity.sequence() == ProducerSequence::new(3)
                        && *violation
                            == RuntimeContractViolationKind::ConflictingTerminalSequence
            ))
    );
}

#[test]
fn topk_close_rejection_cannot_overtake_earlier_accepted_close() {
    let fixture = fixture();
    let producer = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    let channel = fixture
        .service
        .registry
        .active_installation()
        .unwrap()
        .channels()
        .next()
        .unwrap()
        .1;
    fixture.events.0.lock().unwrap().clear();

    let accepted = channel
        .close_topk_partition(
            PRODUCER_A,
            uid(1),
            PartitionId::new(0),
            ProducerSequence::new(0),
        )
        .unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let rejected = std::thread::spawn(move || {
        let error = producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
            .unwrap_err();
        done_tx.send(error.kind()).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    while fixture
        .service
        .dispatcher
        .pending_action_count(ChannelId::new(1))
        == 0
    {
        assert!(
            Instant::now() < deadline,
            "close rejection never reached dispatcher"
        );
        std::thread::yield_now();
    }
    assert!(fixture.events.0.lock().unwrap().is_empty());

    fixture
        .service
        .dispatcher
        .dispatch(ChannelId::new(1), accepted)
        .unwrap();
    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RuntimeContractViolationKind::ConflictingTerminalSequence
    );
    rejected.join().unwrap();
    let typed = fixture
        .events
        .0
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| match event {
            RuntimeFilterEvent::TopKSummaryApplied { identity } => {
                Some((identity.sequence(), None))
            }
            RuntimeFilterEvent::TopKSummaryRejected {
                identity,
                violation,
            } => Some((identity.sequence(), Some(*violation))),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        typed,
        vec![
            (ProducerSequence::new(0), None),
            (
                ProducerSequence::new(1),
                Some(RuntimeContractViolationKind::ConflictingTerminalSequence),
            ),
        ]
    );
}

#[test]
fn topk_authorize_preflight_and_core_rejections_are_typed_before_return() {
    let account = Arc::new(ArmableMemoryAccount::default());
    let fixture = fixture_with_account(account.clone());
    let producer = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    fixture.events.0.lock().unwrap().clear();

    let unauthorized = producer
        .submit_summary(
            PartitionId::new(1),
            ProducerSequence::new(0),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap_err();
    assert_eq!(
        unauthorized.kind(),
        RuntimeContractViolationKind::InvalidPartition
    );
    assert!(
        fixture
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(
                event,
                RuntimeFilterEvent::TopKSummaryRejected { identity, violation }
                    if identity.stream().partition_id() == PartitionId::new(1)
                        && *violation == RuntimeContractViolationKind::InvalidPartition
            ))
    );

    fixture.events.0.lock().unwrap().clear();
    let keys = vec![OrderKeyContract {
        data_type: DataType::Int64,
        direction: SortDirection::Descending,
        null_order: NullOrder::Last,
    }];
    let wrong_plan = OrderContract {
        comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
        keys,
        inclusive: true,
    };
    let wrong_contract = RuntimeTopKSummaryContract::try_from_plan(
        &wrong_plan,
        TopKSummaryRequirement::try_new(4).unwrap(),
    )
    .unwrap();
    account.rejecting.store(true, Ordering::SeqCst);
    let preflight = producer
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&wrong_contract, &[4, 1]),
        )
        .unwrap_err();
    account.rejecting.store(false, Ordering::SeqCst);
    assert_eq!(
        preflight.kind(),
        RuntimeContractViolationKind::OrderedContractMismatch
    );
    assert!(
        fixture
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(
                event,
                RuntimeFilterEvent::TopKSummaryRejected { identity, violation }
                    if identity.sequence() == ProducerSequence::new(0)
                        && *violation == RuntimeContractViolationKind::OrderedContractMismatch
            ))
    );

    fixture.events.0.lock().unwrap().clear();
    producer
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap();
    fixture.events.0.lock().unwrap().clear();
    let conflict = producer
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&fixture.contract, &[1, 3]),
        )
        .unwrap_err();
    assert_eq!(
        conflict.kind(),
        RuntimeContractViolationKind::ConflictingReplay
    );
    assert!(
        fixture
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(
                event,
                RuntimeFilterEvent::TopKSummaryRejected { identity, violation }
                    if identity.sequence() == ProducerSequence::new(0)
                        && *violation == RuntimeContractViolationKind::ConflictingReplay
            ))
    );
}

#[test]
fn topk_rejection_cannot_overtake_an_earlier_accepted_dispatch() {
    let fixture = fixture();
    let producer = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    let channel = fixture
        .service
        .registry
        .active_installation()
        .unwrap()
        .channels()
        .next()
        .unwrap()
        .1;
    fixture.events.0.lock().unwrap().clear();

    let accepted_summary = summary(&fixture.contract, &[1, 4]);
    let accepted_bytes = accepted_summary.canonical_contribution_bytes().unwrap();
    let accepted = channel
        .submit_topk_summary(
            PRODUCER_A,
            uid(1),
            PartitionId::new(0),
            ProducerSequence::new(0),
            accepted_summary,
            TemporaryContributionLease::new(
                MemTrackerMemoryAccount::new_root_for_test("topk-event-order-test"),
                accepted_bytes,
            ),
        )
        .unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    let contract = fixture.contract.clone();
    let rejected = std::thread::spawn(move || {
        let error = producer
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(1),
                summary(&contract, &[1, 5]),
            )
            .unwrap_err();
        done_tx.send(error.kind()).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    while fixture
        .service
        .dispatcher
        .pending_action_count(ChannelId::new(1))
        == 0
    {
        assert!(
            Instant::now() < deadline,
            "rejection never reached dispatcher"
        );
        std::thread::yield_now();
    }
    assert!(fixture.events.0.lock().unwrap().is_empty());

    fixture
        .service
        .dispatcher
        .dispatch(ChannelId::new(1), accepted)
        .unwrap();
    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RuntimeContractViolationKind::OrderedBoundLoosened
    );
    rejected.join().unwrap();
    let typed = fixture
        .events
        .0
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| match event {
            RuntimeFilterEvent::TopKSummaryApplied { identity } => {
                Some((identity.sequence(), None))
            }
            RuntimeFilterEvent::TopKSummaryRejected {
                identity,
                violation,
            } => Some((identity.sequence(), Some(*violation))),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        typed,
        vec![
            (ProducerSequence::new(0), None),
            (
                ProducerSequence::new(1),
                Some(RuntimeContractViolationKind::OrderedBoundLoosened),
            ),
        ]
    );
}
