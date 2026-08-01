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
use std::sync::{Arc, Barrier, Mutex, Weak, mpsc};
use std::time::{Duration, Instant};

use arrow::datatypes::DataType;

use crate::common::types::UniqueId;
use crate::runtime_filter::core::channel::ChannelAction;
use crate::runtime_filter::model::contract::*;
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::artifact::{ArtifactKind, ConsumerArtifactProfile};
use crate::runtime_filter::port::events::{
    FinalDomainRejectionKind, RuntimeFilterEvent, RuntimeFilterEventSink,
};
use crate::runtime_filter::port::final_domain::{
    FinalDomainTestIssuerTransition, FrozenFinalDomainTestIssuer,
};
use crate::runtime_filter::port::identity::*;
use crate::runtime_filter::port::install::*;
use crate::runtime_filter::port::producer::{
    FinalDomainProducerAdapter, ProducerHandle, ProducerPortKind, RuntimeContractViolationKind,
    SubmitOutcome,
};
use crate::runtime_filter::port::subscription::{
    LivePollOutcome, LiveTerminal, SubscriptionKind, UnavailableReason,
};
use crate::runtime_filter::port::support::{
    MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
};
use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};

use super::RuntimeFilterService;
use super::memory::MemTrackerMemoryAccount;

const CHANNEL: ChannelId = ChannelId::new(1);
const PRODUCER_A: BindingId = BindingId::new(10);
const PRODUCER_B: BindingId = BindingId::new(20);
const CONSUMER: BindingId = BindingId::new(30);

fn uid(lo: i64) -> UniqueId {
    UniqueId::new(91, lo)
}

struct Clock(Instant);

impl RuntimeFilterClock for Clock {
    fn now(&self) -> Instant {
        self.0
    }
}

#[derive(Default)]
struct Events(Mutex<Vec<RuntimeFilterEvent>>);

impl Events {
    fn clear(&self) {
        self.0.lock().unwrap().clear();
    }

    fn snapshot(&self) -> Vec<RuntimeFilterEvent> {
        self.0.lock().unwrap().clone()
    }
}

impl RuntimeFilterEventSink for Events {
    fn record(&self, event: RuntimeFilterEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[derive(Default)]
struct ArmableMemoryAccount {
    rejecting: AtomicBool,
    calls: AtomicUsize,
    current: AtomicUsize,
}

impl RuntimeFilterMemoryAccount for ArmableMemoryAccount {
    fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
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

struct BlockingCallMemoryAccount {
    block_call: usize,
    reject_blocked: bool,
    calls: AtomicUsize,
    current: AtomicUsize,
    entered: mpsc::Sender<()>,
    release_blocked: Mutex<mpsc::Receiver<()>>,
}

impl RuntimeFilterMemoryAccount for BlockingCallMemoryAccount {
    fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == self.block_call {
            self.entered.send(()).unwrap();
            self.release_blocked.lock().unwrap().recv().unwrap();
            if self.reject_blocked {
                return Err(MemoryAccountError::CapacityExceeded);
            }
        }
        self.current.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        let previous = self.current.fetch_sub(bytes, Ordering::SeqCst);
        assert!(previous >= bytes);
    }
}

fn blocking_memory_account(
    block_call: usize,
    reject_blocked: bool,
) -> (
    Arc<BlockingCallMemoryAccount>,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
) {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    (
        Arc::new(BlockingCallMemoryAccount {
            block_call,
            reject_blocked,
            calls: AtomicUsize::new(0),
            current: AtomicUsize::new(0),
            entered: entered_tx,
            release_blocked: Mutex::new(release_rx),
        }),
        entered_rx,
        release_tx,
    )
}

fn deployment_for(
    channel_id: ChannelId,
    producers: &[(BindingId, CoverageWitnessId, UniqueId)],
) -> RuntimeFilterChannelDeployment {
    let mut producer_instances = BTreeMap::new();
    for (binding, witness, instance) in producers {
        producer_instances
            .entry((*binding, *witness))
            .or_insert_with(BTreeSet::new)
            .insert(*instance);
    }
    let coverage = Coverage::AllOf(
        producer_instances
            .iter()
            .map(|((_, witness), _)| Coverage::Leaf(*witness))
            .collect(),
    );
    RuntimeFilterChannelDeployment::new(
        channel_id,
        RuntimeFilterLogicalDomain::Membership {
            value_type: DataType::Int64,
            null_semantics: NullSemantics::NullSafeEqual,
        },
        RuntimeFilterLifecycle::CompleteOnce,
        coverage.clone(),
        coverage,
        ReductionRequirement::SetUnion,
        BTreeSet::from([
            ContributionKind::FinalDomainShard,
            ContributionKind::ProducerClosed,
        ]),
        CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen),
        RuntimeFilterPolicyRequirement {
            max_contribution_bytes: 4096,
            max_artifact_bytes: 4096,
            deadline_ms: 100,
            max_retries: 1,
        },
        RuntimeFilterCoreBudget::new(16 * 1024),
        MaterializationPolicy::for_test(),
        producer_instances
            .into_iter()
            .map(|((binding, witness), instances)| {
                (binding, ProducerDeployment::new(witness, instances))
            })
            .collect(),
        BTreeMap::from([(
            CONSUMER,
            ConsumerDeployment::with_profile(
                ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
                BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                ConsumerArtifactProfile::new(
                    BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
                    None,
                )
                .unwrap(),
                BTreeSet::from([RouteEdgeId::new(40)]),
                BTreeSet::from([uid(30)]),
            ),
        )]),
    )
}

fn installed_service(
    producers: &[(BindingId, CoverageWitnessId, UniqueId)],
    events: Arc<dyn RuntimeFilterEventSink>,
    memory: Arc<dyn RuntimeFilterMemoryAccount>,
) -> Arc<RuntimeFilterService> {
    installed_service_for(
        uid(0),
        DeploymentEpoch::new(1),
        CHANNEL,
        producers,
        events,
        memory,
    )
}

fn installed_service_for(
    query_id: UniqueId,
    epoch: DeploymentEpoch,
    channel_id: ChannelId,
    producers: &[(BindingId, CoverageWitnessId, UniqueId)],
    events: Arc<dyn RuntimeFilterEventSink>,
    memory: Arc<dyn RuntimeFilterMemoryAccount>,
) -> Arc<RuntimeFilterService> {
    let service = Arc::new(RuntimeFilterService::new_with_dependencies(
        query_id,
        Arc::new(Clock(Instant::now())),
        events,
        memory,
    ));
    service
        .install(local_participant_install_for_test(
            RuntimeFilterInstallView::new(
                epoch,
                RuntimeFilterParticipantId::new(1),
                BTreeMap::from([(channel_id, deployment_for(channel_id, producers))]),
            ),
        ))
        .unwrap();
    service
}

fn open_final(
    service: &RuntimeFilterService,
    binding: BindingId,
    instance: UniqueId,
) -> Arc<dyn FinalDomainProducerAdapter> {
    open_final_with_partitions(service, binding, instance, 1)
}

fn open_final_with_partitions(
    service: &RuntimeFilterService,
    binding: BindingId,
    instance: UniqueId,
    local_partition_count: u32,
) -> Arc<dyn FinalDomainProducerAdapter> {
    let ProducerHandle::FinalDomain(producer) = service
        .open_producer(
            binding,
            instance,
            local_partition_count,
            ProducerPortKind::FinalDomain,
        )
        .unwrap()
    else {
        panic!("fenced-final install must expose only the typed final-domain port")
    };
    producer
}

fn frozen_issuer(
    service: &RuntimeFilterService,
    binding: BindingId,
    instance: UniqueId,
    open_drivers: u32,
) -> FrozenFinalDomainTestIssuer {
    let collecting = service
        .final_domain_test_issuer(binding, instance, open_drivers)
        .expect("private service adapter owns the test-only authority");
    let mut transition = FinalDomainTestIssuerTransition::Collecting(collecting);
    loop {
        transition = match transition {
            FinalDomainTestIssuerTransition::Collecting(collecting) => collecting.close_driver(),
            FinalDomainTestIssuerTransition::Frozen(frozen) => return frozen,
        };
    }
}

fn shard(
    issuer: &FrozenFinalDomainTestIssuer,
    binding: BindingId,
    instance: UniqueId,
    sequence: u64,
    values: &[i64],
) -> crate::runtime_filter::port::final_domain::FinalDomainShard {
    shard_at(
        issuer,
        binding,
        instance,
        PartitionId::new(0),
        sequence,
        values,
    )
}

fn shard_at(
    issuer: &FrozenFinalDomainTestIssuer,
    binding: BindingId,
    instance: UniqueId,
    partition: PartitionId,
    sequence: u64,
    values: &[i64],
) -> crate::runtime_filter::port::final_domain::FinalDomainShard {
    issuer
        .issue_shard(
            ProducerStreamId::new(binding, instance, partition),
            ProducerSequence::new(sequence),
            ValueDomainDelta::new(MembershipValues::int64(values.iter().copied()), false),
        )
        .unwrap()
}

fn assert_coordinate(
    identity: ContributionIdentity,
    binding: BindingId,
    instance: UniqueId,
    sequence: u64,
) {
    assert_eq!(identity.query_id(), uid(0));
    assert_eq!(
        identity.participant_id(),
        RuntimeFilterParticipantId::new(1)
    );
    assert_eq!(identity.channel_id(), CHANNEL);
    assert_eq!(identity.epoch(), DeploymentEpoch::new(1));
    assert_eq!(identity.stream().binding_id(), binding);
    assert_eq!(identity.stream().fragment_instance_id(), instance);
    assert_eq!(identity.stream().partition_id(), PartitionId::new(0));
    assert_eq!(identity.sequence(), ProducerSequence::new(sequence));
}

fn event_count(
    events: &[RuntimeFilterEvent],
    predicate: impl Fn(&RuntimeFilterEvent) -> bool,
) -> usize {
    events.iter().filter(|event| predicate(event)).count()
}

#[test]
fn final_complete_close_race_has_exactly_one_version_terminal_and_delivery_in_128_runs() {
    for iteration in 0..128 {
        let events = Arc::new(Events::default());
        let service = installed_service(
            &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
            events.clone(),
            MemTrackerMemoryAccount::new_root_for_test("m3c-complete-close-race"),
        );
        let producer = open_final(&service, PRODUCER_A, uid(10));
        let issuer = frozen_issuer(&service, PRODUCER_A, uid(10), 1);
        let live = service
            .subscribe(CONSUMER, uid(30), SubscriptionKind::NonBlockingLive)
            .unwrap()
            .into_live()
            .unwrap();
        let input = shard(&issuer, PRODUCER_A, uid(10), 0, &[iteration]);
        events.clear();

        let barrier = Arc::new(Barrier::new(3));
        let (result_tx, result_rx) = mpsc::channel();
        let complete_worker = {
            let producer = producer.clone();
            let barrier = barrier.clone();
            let result_tx = result_tx.clone();
            std::thread::spawn(move || {
                barrier.wait();
                result_tx
                    .send(
                        producer
                            .complete(PartitionId::new(0), ProducerSequence::new(0), input)
                            .unwrap(),
                    )
                    .unwrap();
            })
        };
        let close_worker = {
            let producer = producer.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                result_tx
                    .send(
                        producer
                            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                            .unwrap(),
                    )
                    .unwrap();
            })
        };
        barrier.wait();
        let outcomes = [
            result_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("complete/close race did not produce its first result"),
            result_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("complete/close race did not produce its second result"),
        ];
        complete_worker.join().unwrap();
        close_worker.join().unwrap();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == SubmitOutcome::Completed)
                .count(),
            1,
            "iteration {iteration} must have one completion linearization point"
        );

        let LivePollOutcome::Updated { bundle, terminal } = live.poll_after(None) else {
            panic!("iteration {iteration} must publish exactly one public bundle")
        };
        assert_eq!(bundle.version(), LogicalVersion::FIRST);
        assert_eq!(terminal, Some(LiveTerminal::Completed));
        assert!(matches!(
            live.poll_after(Some(LogicalVersion::FIRST)),
            LivePollOutcome::Idle {
                latest_version: Some(LogicalVersion::FIRST),
                terminal: Some(LiveTerminal::Completed)
            }
        ));
        let recorded = events.snapshot();
        assert_eq!(
            event_count(&recorded, |event| matches!(
                event,
                RuntimeFilterEvent::ChannelCompleted { .. }
            )),
            1
        );
        assert_eq!(
            event_count(&recorded, |event| matches!(
                event,
                RuntimeFilterEvent::ArtifactPublished { .. }
            )),
            1
        );
        assert_eq!(
            event_count(&recorded, |event| matches!(
                event,
                RuntimeFilterEvent::LoopbackDelivered { .. }
            )),
            1
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum CompetingTerminal {
    Cancel,
    Deadline,
    Shutdown,
}

#[test]
fn final_complete_races_cancel_deadline_and_shutdown_without_late_publish() {
    for terminal in [
        CompetingTerminal::Cancel,
        CompetingTerminal::Deadline,
        CompetingTerminal::Shutdown,
    ] {
        for iteration in 0..16 {
            let events = Arc::new(Events::default());
            let service = installed_service(
                &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
                events.clone(),
                MemTrackerMemoryAccount::new_root_for_test("m3c-complete-terminal-race"),
            );
            let producer = open_final(&service, PRODUCER_A, uid(10));
            let issuer = frozen_issuer(&service, PRODUCER_A, uid(10), 1);
            let live = service
                .subscribe(CONSUMER, uid(30), SubscriptionKind::NonBlockingLive)
                .unwrap()
                .into_live()
                .unwrap();
            assert_eq!(
                producer
                    .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                    .unwrap(),
                SubmitOutcome::PendingGap
            );
            let input = shard(&issuer, PRODUCER_A, uid(10), 0, &[iteration]);
            events.clear();

            let barrier = Arc::new(Barrier::new(3));
            let (complete_tx, complete_rx) = mpsc::channel();
            let complete_worker = {
                let producer = producer.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    complete_tx
                        .send(
                            producer
                                .complete(PartitionId::new(0), ProducerSequence::new(0), input)
                                .unwrap(),
                        )
                        .unwrap();
                })
            };
            let (terminal_tx, terminal_rx) = mpsc::channel();
            let terminal_worker = {
                let service = service.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    match terminal {
                        CompetingTerminal::Cancel => service.cancel(),
                        CompetingTerminal::Deadline => {
                            service.expire_deadlines(Instant::now() + Duration::from_secs(1));
                        }
                        CompetingTerminal::Shutdown => service.shutdown(),
                    }
                    terminal_tx.send(()).unwrap();
                })
            };
            barrier.wait();
            let complete_outcome = complete_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("complete side of terminal race did not return");
            terminal_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("terminal side of race did not return");
            complete_worker.join().unwrap();
            terminal_worker.join().unwrap();

            let expected_losing_terminal = match terminal {
                CompetingTerminal::Deadline => {
                    LiveTerminal::Unavailable(UnavailableReason::IncompleteCoverage)
                }
                CompetingTerminal::Cancel | CompetingTerminal::Shutdown => LiveTerminal::Cancelled,
            };
            let recorded = events.snapshot();
            let observed_public = live.poll_after(None);
            assert!(matches!(
                complete_outcome,
                SubmitOutcome::Completed | SubmitOutcome::TerminalNoop
            ));
            match observed_public {
                LivePollOutcome::Updated {
                    bundle,
                    terminal: Some(LiveTerminal::Completed),
                } => {
                    assert_eq!(complete_outcome, SubmitOutcome::Completed);
                    assert_eq!(bundle.version(), LogicalVersion::FIRST);
                    assert_eq!(
                        event_count(&recorded, |event| matches!(
                            event,
                            RuntimeFilterEvent::ArtifactPublished { .. }
                        )),
                        1
                    );
                }
                LivePollOutcome::Updated {
                    bundle,
                    terminal: Some(observed),
                } if observed == expected_losing_terminal => {
                    // Completion was already admitted and published before
                    // close reached quiescence; cancellation/deadline then
                    // terminalized the subscription while retaining its
                    // latest bundle. This is the same linearization guaranteed
                    // by the live-subscription cancellation contract.
                    assert_eq!(complete_outcome, SubmitOutcome::Completed);
                    assert_eq!(bundle.version(), LogicalVersion::FIRST);
                    assert_eq!(
                        event_count(&recorded, |event| matches!(
                            event,
                            RuntimeFilterEvent::ArtifactPublished { .. }
                        )),
                        1
                    );
                }
                LivePollOutcome::Idle {
                    latest_version: None,
                    terminal: Some(observed),
                } if observed == expected_losing_terminal => {
                    assert_eq!(
                        event_count(&recorded, |event| matches!(
                            event,
                            RuntimeFilterEvent::ArtifactPublished { .. }
                                | RuntimeFilterEvent::LoopbackDelivered { .. }
                        )),
                        0,
                        "terminal winner must prevent late publication"
                    );
                }
                other => panic!(
                    "unexpected complete-vs-terminal public outcome: complete={complete_outcome:?}, public={other:?}"
                ),
            }
            assert_eq!(
                event_count(&recorded, |event| matches!(
                    event,
                    RuntimeFilterEvent::ChannelCompleted { .. }
                        | RuntimeFilterEvent::ChannelUnavailable { .. }
                        | RuntimeFilterEvent::ChannelCancelled { .. }
                )),
                1,
                "race must expose exactly one logical terminal"
            );
        }
    }
}

#[test]
fn final_temporary_resource_race_repreflights_concurrent_duplicate() {
    let events = Arc::new(Events::default());
    let (memory, entered, release) = blocking_memory_account(0, true);
    let service = installed_service(
        &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
        events.clone(),
        memory.clone(),
    );
    let producer = open_final(&service, PRODUCER_A, uid(10));
    let issuer = frozen_issuer(&service, PRODUCER_A, uid(10), 1);
    let input = shard(&issuer, PRODUCER_A, uid(10), 0, &[1]);
    events.clear();

    let (outer_tx, outer_rx) = mpsc::channel();
    let outer_producer = producer.clone();
    let outer_input = input.clone();
    let outer_worker = std::thread::spawn(move || {
        outer_tx
            .send(outer_producer.complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                outer_input,
            ))
            .unwrap();
    });
    entered
        .recv_timeout(Duration::from_secs(5))
        .expect("outer final complete did not reach its temporary reservation");
    assert_eq!(
        producer
            .complete(PartitionId::new(0), ProducerSequence::new(0), input)
            .unwrap(),
        SubmitOutcome::Applied
    );
    let retained_after_inner = memory.current.load(Ordering::SeqCst);
    assert!(retained_after_inner > 0);
    release.send(()).unwrap();
    assert_eq!(
        outer_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("resource-race complete did not return")
            .unwrap(),
        SubmitOutcome::Duplicate
    );
    outer_worker.join().unwrap();
    assert_eq!(memory.calls.load(Ordering::SeqCst), 3);
    assert_eq!(memory.current.load(Ordering::SeqCst), retained_after_inner);
    let recorded = events.snapshot();
    assert_eq!(
        event_count(&recorded, |event| matches!(
            event,
            RuntimeFilterEvent::FinalDomainShardAccepted { .. }
        )),
        1
    );
    assert_eq!(
        event_count(&recorded, |event| matches!(
            event,
            RuntimeFilterEvent::FinalDomainShardDuplicate { .. }
        )),
        1
    );
    assert_eq!(
        event_count(&recorded, |event| matches!(
            event,
            RuntimeFilterEvent::FinalDomainShardRejected {
                rejection: FinalDomainRejectionKind::ResourceLimit,
                ..
            } | RuntimeFilterEvent::ChannelUnavailable { .. }
        )),
        0,
        "a concurrent exact replay must win re-preflight over stale resource failure"
    );
    service.cancel();
    assert_eq!(memory.current.load(Ordering::SeqCst), 0);
}

#[test]
fn final_retained_reservation_repreflights_duplicate_and_cancel_without_leak() {
    let (duplicate_memory, duplicate_entered, duplicate_release) =
        blocking_memory_account(1, false);
    let duplicate_service = installed_service(
        &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
        Arc::new(Events::default()),
        duplicate_memory.clone(),
    );
    let duplicate_producer = open_final(&duplicate_service, PRODUCER_A, uid(10));
    let duplicate_issuer = frozen_issuer(&duplicate_service, PRODUCER_A, uid(10), 1);
    let duplicate_input = shard(&duplicate_issuer, PRODUCER_A, uid(10), 0, &[1]);
    let (outer_tx, outer_rx) = mpsc::channel();
    let outer_producer = duplicate_producer.clone();
    let outer_input = duplicate_input.clone();
    let duplicate_worker = std::thread::spawn(move || {
        outer_tx
            .send(outer_producer.complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                outer_input,
            ))
            .unwrap();
    });
    duplicate_entered
        .recv_timeout(Duration::from_secs(5))
        .expect("outer complete did not reach retained reservation");
    let outer_temporary_bytes = duplicate_memory.current.load(Ordering::SeqCst);
    assert!(outer_temporary_bytes > 0);
    assert_eq!(
        duplicate_producer
            .complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                duplicate_input,
            )
            .unwrap(),
        SubmitOutcome::Applied
    );
    let retained_after_inner = duplicate_memory.current.load(Ordering::SeqCst);
    assert!(retained_after_inner > 0);
    duplicate_release.send(()).unwrap();
    assert_eq!(
        outer_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("duplicate re-preflight did not return")
            .unwrap(),
        SubmitOutcome::Duplicate
    );
    duplicate_worker.join().unwrap();
    assert_eq!(duplicate_memory.calls.load(Ordering::SeqCst), 4);
    assert_eq!(
        duplicate_memory.current.load(Ordering::SeqCst),
        retained_after_inner - outer_temporary_bytes,
        "duplicate re-preflight must release the outer temporary and retained reservations"
    );
    duplicate_service.cancel();
    assert_eq!(duplicate_memory.current.load(Ordering::SeqCst), 0);

    let (cancel_memory, cancel_entered, cancel_release) = blocking_memory_account(1, false);
    let cancel_service = installed_service(
        &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
        Arc::new(Events::default()),
        cancel_memory.clone(),
    );
    let cancel_producer = open_final(&cancel_service, PRODUCER_A, uid(10));
    let cancel_issuer = frozen_issuer(&cancel_service, PRODUCER_A, uid(10), 1);
    let cancel_input = shard(&cancel_issuer, PRODUCER_A, uid(10), 0, &[1]);
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let outer_cancel_producer = cancel_producer.clone();
    let cancel_worker = std::thread::spawn(move || {
        cancel_tx
            .send(outer_cancel_producer.complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                cancel_input,
            ))
            .unwrap();
    });
    cancel_entered
        .recv_timeout(Duration::from_secs(5))
        .expect("outer complete did not reach retained reservation before cancel");
    cancel_service.cancel();
    cancel_release.send(()).unwrap();
    assert_eq!(
        cancel_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("cancel re-preflight did not return")
            .unwrap(),
        SubmitOutcome::TerminalNoop
    );
    cancel_worker.join().unwrap();
    assert_eq!(cancel_memory.calls.load(Ordering::SeqCst), 2);
    assert_eq!(cancel_memory.current.load(Ordering::SeqCst), 0);
    assert!(matches!(
        cancel_service
            .registry
            .installation_for_dispatch()
            .unwrap()
            .channels()
            .next()
            .unwrap()
            .1
            .terminal_action(),
        ChannelAction::Cancelled { .. }
    ));
}

#[test]
fn final_allof_multi_instance_partition_witness_permutations_publish_once() {
    let permutations = [
        [0_usize, 1, 2, 3, 4, 5],
        [5, 4, 3, 2, 1, 0],
        [2, 4, 0, 5, 1, 3],
    ];
    for order in permutations {
        let events = Arc::new(Events::default());
        let service = installed_service(
            &[
                (PRODUCER_A, CoverageWitnessId::new(10), uid(10)),
                (PRODUCER_A, CoverageWitnessId::new(10), uid(11)),
                (PRODUCER_B, CoverageWitnessId::new(20), uid(20)),
            ],
            events.clone(),
            MemTrackerMemoryAccount::new_root_for_test("m3c-witness-permutation"),
        );
        let producer_a0 = open_final_with_partitions(&service, PRODUCER_A, uid(10), 2);
        let producer_a1 = open_final_with_partitions(&service, PRODUCER_A, uid(11), 2);
        let producer_b = open_final_with_partitions(&service, PRODUCER_B, uid(20), 2);
        let issuer_a0 = frozen_issuer(&service, PRODUCER_A, uid(10), 1);
        let issuer_a1 = frozen_issuer(&service, PRODUCER_A, uid(11), 1);
        let issuer_b = frozen_issuer(&service, PRODUCER_B, uid(20), 1);
        let submissions = [
            (
                producer_a0.clone(),
                PartitionId::new(0),
                shard_at(
                    &issuer_a0,
                    PRODUCER_A,
                    uid(10),
                    PartitionId::new(0),
                    0,
                    &[1],
                ),
            ),
            (
                producer_a0,
                PartitionId::new(1),
                shard_at(&issuer_a0, PRODUCER_A, uid(10), PartitionId::new(1), 0, &[]),
            ),
            (
                producer_a1.clone(),
                PartitionId::new(0),
                shard_at(
                    &issuer_a1,
                    PRODUCER_A,
                    uid(11),
                    PartitionId::new(0),
                    0,
                    &[2],
                ),
            ),
            (
                producer_a1,
                PartitionId::new(1),
                shard_at(
                    &issuer_a1,
                    PRODUCER_A,
                    uid(11),
                    PartitionId::new(1),
                    0,
                    &[3],
                ),
            ),
            (
                producer_b.clone(),
                PartitionId::new(0),
                shard_at(&issuer_b, PRODUCER_B, uid(20), PartitionId::new(0), 0, &[4]),
            ),
            (
                producer_b,
                PartitionId::new(1),
                shard_at(&issuer_b, PRODUCER_B, uid(20), PartitionId::new(1), 0, &[5]),
            ),
        ];
        let live = service
            .subscribe(CONSUMER, uid(30), SubscriptionKind::NonBlockingLive)
            .unwrap()
            .into_live()
            .unwrap();
        events.clear();

        for (position, submission_index) in order.into_iter().enumerate() {
            let (producer, partition, shard) = &submissions[submission_index];
            assert_eq!(
                producer
                    .complete(*partition, ProducerSequence::new(0), shard.clone())
                    .unwrap(),
                SubmitOutcome::Applied
            );
            let close_outcome = producer
                .close_partition(*partition, ProducerSequence::new(1))
                .unwrap();
            if position + 1 == submissions.len() {
                assert_eq!(close_outcome, SubmitOutcome::Completed);
            } else {
                assert_ne!(close_outcome, SubmitOutcome::Completed);
                assert!(matches!(
                    live.poll_after(None),
                    LivePollOutcome::Idle {
                        latest_version: None,
                        terminal: None
                    }
                ));
            }
        }

        let LivePollOutcome::Updated { bundle, terminal } = live.poll_after(None) else {
            panic!("the last AllOf witness must publish one public bundle")
        };
        assert_eq!(bundle.version(), LogicalVersion::FIRST);
        assert_eq!(terminal, Some(LiveTerminal::Completed));
        let recorded = events.snapshot();
        assert_eq!(
            event_count(&recorded, |event| matches!(
                event,
                RuntimeFilterEvent::ChannelCompleted { .. }
            )),
            1
        );
        assert_eq!(
            event_count(&recorded, |event| matches!(
                event,
                RuntimeFilterEvent::ArtifactPublished { .. }
            )),
            1
        );
        assert_eq!(
            event_count(&recorded, |event| matches!(
                event,
                RuntimeFilterEvent::LoopbackDelivered { .. }
            )),
            1
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum FinalTerminalCase {
    Completed,
    Unavailable,
    Cancelled,
}

#[test]
fn wrong_final_scope_is_rejected_before_each_terminal_without_state_or_memory_mutation() {
    let producers = [
        (PRODUCER_A, CoverageWitnessId::new(10), uid(10)),
        (PRODUCER_B, CoverageWitnessId::new(20), uid(20)),
    ];
    let wrong_channel_service = installed_service_for(
        uid(0),
        DeploymentEpoch::new(1),
        ChannelId::new(2),
        &producers,
        Arc::new(Events::default()),
        MemTrackerMemoryAccount::new_root_for_test("m3c-wrong-channel-source"),
    );
    let wrong_channel_issuer = {
        let _producer = open_final(&wrong_channel_service, PRODUCER_A, uid(10));
        frozen_issuer(&wrong_channel_service, PRODUCER_A, uid(10), 1)
    };
    let wrong_epoch_service = installed_service_for(
        uid(0),
        DeploymentEpoch::new(2),
        CHANNEL,
        &producers,
        Arc::new(Events::default()),
        MemTrackerMemoryAccount::new_root_for_test("m3c-wrong-epoch-source"),
    );
    let wrong_epoch_issuer = {
        let _producer = open_final(&wrong_epoch_service, PRODUCER_A, uid(10));
        frozen_issuer(&wrong_epoch_service, PRODUCER_A, uid(10), 1)
    };

    for terminal_case in [
        FinalTerminalCase::Completed,
        FinalTerminalCase::Unavailable,
        FinalTerminalCase::Cancelled,
    ] {
        let events = Arc::new(Events::default());
        let memory = Arc::new(ArmableMemoryAccount::default());
        let service = installed_service(&producers, events.clone(), memory.clone());
        let producer_a = open_final(&service, PRODUCER_A, uid(10));
        let producer_b = open_final(&service, PRODUCER_B, uid(20));
        let issuer_a = frozen_issuer(&service, PRODUCER_A, uid(10), 1);
        let issuer_b = frozen_issuer(&service, PRODUCER_B, uid(20), 1);
        let valid_a = shard(&issuer_a, PRODUCER_A, uid(10), 0, &[1]);
        let channel = service
            .registry
            .active_installation()
            .unwrap()
            .channels()
            .next()
            .unwrap()
            .1
            .clone();

        match terminal_case {
            FinalTerminalCase::Completed => {
                producer_a
                    .complete(
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                        valid_a.clone(),
                    )
                    .unwrap();
                producer_a
                    .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                    .unwrap();
                producer_b
                    .complete(
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                        shard(&issuer_b, PRODUCER_B, uid(20), 0, &[2]),
                    )
                    .unwrap();
                assert_eq!(
                    producer_b
                        .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                        .unwrap(),
                    SubmitOutcome::Completed
                );
            }
            FinalTerminalCase::Unavailable => {
                memory.rejecting.store(true, Ordering::SeqCst);
                assert_eq!(
                    producer_a
                        .complete(
                            PartitionId::new(0),
                            ProducerSequence::new(0),
                            valid_a.clone(),
                        )
                        .unwrap(),
                    SubmitOutcome::TerminalNoop
                );
                memory.rejecting.store(false, Ordering::SeqCst);
            }
            FinalTerminalCase::Cancelled => service.cancel(),
        }
        let retained_before = memory.current.load(Ordering::SeqCst);
        let memory_calls_before = memory.calls.load(Ordering::SeqCst);
        let snapshot_before = channel.snapshot().map(|snapshot| snapshot.version());
        events.clear();

        let wrong_inputs = [
            (
                shard(&wrong_channel_issuer, PRODUCER_A, uid(10), 0, &[3]),
                RuntimeContractViolationKind::TypeMismatch,
            ),
            (
                shard(&wrong_epoch_issuer, PRODUCER_A, uid(10), 0, &[4]),
                RuntimeContractViolationKind::TypeMismatch,
            ),
            (
                shard(&issuer_b, PRODUCER_B, uid(20), 0, &[5]),
                RuntimeContractViolationKind::UnauthorizedBinding,
            ),
            (
                shard(&issuer_a, PRODUCER_A, uid(10), 1, &[6]),
                RuntimeContractViolationKind::ConflictingReplay,
            ),
        ];
        for (wrong, expected) in wrong_inputs {
            assert_eq!(
                producer_a
                    .complete(PartitionId::new(0), ProducerSequence::new(0), wrong)
                    .unwrap_err()
                    .kind(),
                expected,
                "{terminal_case:?} must validate scope before terminal precedence"
            );
        }
        assert_eq!(
            memory.current.load(Ordering::SeqCst),
            retained_before,
            "scope rejection must not change retained accounting"
        );
        assert_eq!(
            memory.calls.load(Ordering::SeqCst),
            memory_calls_before,
            "scope rejection must not attempt temporary or retained reservation"
        );
        assert_eq!(
            channel.snapshot().map(|snapshot| snapshot.version()),
            snapshot_before,
            "scope rejection must not change the logical version"
        );
        let recorded = events.snapshot();
        assert_eq!(
            event_count(&recorded, |event| matches!(
                event,
                RuntimeFilterEvent::FinalDomainShardRejected { .. }
            )),
            4
        );
        assert_eq!(
            event_count(&recorded, |event| matches!(
                event,
                RuntimeFilterEvent::ChannelCompleted { .. }
                    | RuntimeFilterEvent::ChannelUnavailable { .. }
                    | RuntimeFilterEvent::ChannelCancelled { .. }
                    | RuntimeFilterEvent::ArtifactPublished { .. }
            )),
            0,
            "scope rejection must not create another terminal or publication"
        );

        match terminal_case {
            FinalTerminalCase::Completed => {
                assert!(retained_before > 0);
                assert_eq!(
                    producer_a
                        .complete(PartitionId::new(0), ProducerSequence::new(0), valid_a,)
                        .unwrap(),
                    SubmitOutcome::Duplicate
                );
                assert_eq!(
                    producer_a
                        .complete(
                            PartitionId::new(0),
                            ProducerSequence::new(0),
                            shard(&issuer_a, PRODUCER_A, uid(10), 0, &[99]),
                        )
                        .unwrap_err()
                        .kind(),
                    RuntimeContractViolationKind::ConflictingReplay
                );
                assert!(matches!(
                    channel.terminal_action(),
                    ChannelAction::Completed { .. }
                ));
                assert_eq!(memory.current.load(Ordering::SeqCst), retained_before);
            }
            FinalTerminalCase::Unavailable | FinalTerminalCase::Cancelled => {
                for late_valid in [valid_a, shard(&issuer_a, PRODUCER_A, uid(10), 0, &[99])] {
                    assert_eq!(
                        producer_a
                            .complete(PartitionId::new(0), ProducerSequence::new(0), late_valid,)
                            .unwrap(),
                        SubmitOutcome::TerminalNoop
                    );
                }
                assert_eq!(memory.current.load(Ordering::SeqCst), 0);
                match terminal_case {
                    FinalTerminalCase::Unavailable => assert!(matches!(
                        channel.terminal_action(),
                        ChannelAction::Unavailable {
                            reason: UnavailableReason::ResourceLimit,
                            ..
                        }
                    )),
                    FinalTerminalCase::Cancelled => {
                        assert!(matches!(
                            channel.terminal_action(),
                            ChannelAction::Cancelled { .. }
                        ))
                    }
                    FinalTerminalCase::Completed => unreachable!(),
                }
            }
        }
    }
}

#[test]
fn public_subscription_distinguishes_explicit_empty_from_unavailable() {
    let empty_events = Arc::new(Events::default());
    let empty_service = installed_service(
        &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
        empty_events,
        MemTrackerMemoryAccount::new_root_for_test("m3c-public-explicit-empty"),
    );
    let empty_producer = open_final(&empty_service, PRODUCER_A, uid(10));
    let empty_issuer = frozen_issuer(&empty_service, PRODUCER_A, uid(10), 1);
    let empty_live = empty_service
        .subscribe(CONSUMER, uid(30), SubscriptionKind::NonBlockingLive)
        .unwrap()
        .into_live()
        .unwrap();
    empty_producer
        .complete(
            PartitionId::new(0),
            ProducerSequence::new(0),
            shard(&empty_issuer, PRODUCER_A, uid(10), 0, &[]),
        )
        .unwrap();
    assert_eq!(
        empty_producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
            .unwrap(),
        SubmitOutcome::Completed
    );
    let LivePollOutcome::Updated {
        bundle: empty_bundle,
        terminal: empty_terminal,
    } = empty_live.poll_after(None)
    else {
        panic!("explicit empty must be a published public artifact")
    };
    assert_eq!(empty_terminal, Some(LiveTerminal::Completed));
    assert_eq!(empty_bundle.version(), LogicalVersion::FIRST);
    assert_eq!(empty_bundle.artifacts().len(), 1);
    assert_eq!(empty_bundle.artifacts()[0].0, ArtifactKind::EmptyDomain);

    let unavailable_events = Arc::new(Events::default());
    let unavailable_memory = Arc::new(ArmableMemoryAccount::default());
    let unavailable_service = installed_service(
        &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
        unavailable_events,
        unavailable_memory.clone(),
    );
    let unavailable_producer = open_final(&unavailable_service, PRODUCER_A, uid(10));
    let unavailable_issuer = frozen_issuer(&unavailable_service, PRODUCER_A, uid(10), 1);
    let unavailable_live = unavailable_service
        .subscribe(CONSUMER, uid(30), SubscriptionKind::NonBlockingLive)
        .unwrap()
        .into_live()
        .unwrap();
    unavailable_memory.rejecting.store(true, Ordering::SeqCst);
    assert_eq!(
        unavailable_producer
            .complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                shard(&unavailable_issuer, PRODUCER_A, uid(10), 0, &[]),
            )
            .unwrap(),
        SubmitOutcome::TerminalNoop
    );
    assert!(unavailable_live.snapshot().is_none());
    assert!(matches!(
        unavailable_live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(UnavailableReason::ResourceLimit))
        }
    ));
    assert_eq!(unavailable_memory.current.load(Ordering::SeqCst), 0);
}

#[test]
fn public_final_port_freezes_after_all_local_drivers_and_publishes_once_after_allof() {
    let events = Arc::new(Events::default());
    let service = installed_service(
        &[
            (PRODUCER_A, CoverageWitnessId::new(10), uid(10)),
            (PRODUCER_B, CoverageWitnessId::new(20), uid(20)),
        ],
        events.clone(),
        MemTrackerMemoryAccount::new_root_for_test("m3c-public-final"),
    );
    let producer_a = open_final(&service, PRODUCER_A, uid(10));
    let producer_b = open_final(&service, PRODUCER_B, uid(20));
    let live = service
        .subscribe(CONSUMER, uid(30), SubscriptionKind::NonBlockingLive)
        .unwrap()
        .into_live()
        .unwrap();

    let issuer_a = frozen_issuer(&service, PRODUCER_A, uid(10), 2);
    let issuer_b = frozen_issuer(&service, PRODUCER_B, uid(20), 2);
    assert_eq!(
        producer_a
            .complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                shard(&issuer_a, PRODUCER_A, uid(10), 0, &[7]),
            )
            .unwrap(),
        SubmitOutcome::Applied
    );
    producer_a
        .close_partition(PartitionId::new(0), ProducerSequence::new(1))
        .unwrap();
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: None
        }
    ));

    producer_b
        .complete(
            PartitionId::new(0),
            ProducerSequence::new(0),
            shard(&issuer_b, PRODUCER_B, uid(20), 0, &[9]),
        )
        .unwrap();
    assert_eq!(
        producer_b
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
            .unwrap(),
        SubmitOutcome::Completed
    );
    let LivePollOutcome::Updated { bundle, terminal } = live.poll_after(None) else {
        panic!("AllOf completion must publish one terminal bundle")
    };
    assert_eq!(terminal, Some(LiveTerminal::Completed));
    assert_eq!(bundle.version(), LogicalVersion::FIRST);
    assert_eq!(bundle.artifacts().len(), 1);
    assert_eq!(bundle.artifacts()[0].0, ArtifactKind::ValueSet);
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: Some(LiveTerminal::Completed)
        }
    ));
    let recorded = events.snapshot();
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }))
            .count(),
        1
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ChannelCompleted { .. }))
            .count(),
        1
    );
}

#[test]
fn final_input_events_keep_coordinates_order_and_resource_rejection_precedes_unavailable() {
    let events = Arc::new(Events::default());
    let service = installed_service(
        &[
            (PRODUCER_A, CoverageWitnessId::new(10), uid(10)),
            (PRODUCER_B, CoverageWitnessId::new(20), uid(20)),
        ],
        events.clone(),
        MemTrackerMemoryAccount::new_root_for_test("m3c-causal-events"),
    );
    let producer_a = open_final(&service, PRODUCER_A, uid(10));
    let _producer_b = open_final(&service, PRODUCER_B, uid(20));
    let issuer_a = frozen_issuer(&service, PRODUCER_A, uid(10), 1);
    let issuer_b = frozen_issuer(&service, PRODUCER_B, uid(20), 1);
    events.clear();

    let accepted = shard(&issuer_a, PRODUCER_A, uid(10), 0, &[1]);
    assert_eq!(
        producer_a
            .complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                accepted.clone(),
            )
            .unwrap(),
        SubmitOutcome::Applied
    );
    assert_eq!(
        producer_a
            .complete(PartitionId::new(0), ProducerSequence::new(0), accepted,)
            .unwrap(),
        SubmitOutcome::Duplicate
    );
    assert_eq!(
        producer_a
            .complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                shard(&issuer_a, PRODUCER_A, uid(10), 0, &[2]),
            )
            .unwrap_err()
            .kind(),
        RuntimeContractViolationKind::ConflictingReplay
    );
    assert_eq!(
        producer_a
            .complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                shard(&issuer_b, PRODUCER_B, uid(20), 0, &[3]),
            )
            .unwrap_err()
            .kind(),
        RuntimeContractViolationKind::UnauthorizedBinding
    );

    let typed = events
        .snapshot()
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                RuntimeFilterEvent::FinalDomainShardAccepted { .. }
                    | RuntimeFilterEvent::FinalDomainShardDuplicate { .. }
                    | RuntimeFilterEvent::FinalDomainShardRejected { .. }
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(typed.len(), 4);
    for event in &typed {
        let identity = match event {
            RuntimeFilterEvent::FinalDomainShardAccepted { identity }
            | RuntimeFilterEvent::FinalDomainShardDuplicate { identity }
            | RuntimeFilterEvent::FinalDomainShardRejected { identity, .. } => *identity,
            _ => unreachable!(),
        };
        assert_coordinate(identity, PRODUCER_A, uid(10), 0);
    }
    assert!(matches!(
        typed[0],
        RuntimeFilterEvent::FinalDomainShardAccepted { .. }
    ));
    assert!(matches!(
        typed[1],
        RuntimeFilterEvent::FinalDomainShardDuplicate { .. }
    ));
    assert!(matches!(
        typed[2],
        RuntimeFilterEvent::FinalDomainShardRejected {
            rejection: FinalDomainRejectionKind::Contract(
                RuntimeContractViolationKind::ConflictingReplay
            ),
            ..
        }
    ));
    assert!(matches!(
        typed[3],
        RuntimeFilterEvent::FinalDomainShardRejected {
            rejection: FinalDomainRejectionKind::Contract(
                RuntimeContractViolationKind::UnauthorizedBinding
            ),
            ..
        }
    ));

    let resource_events = Arc::new(Events::default());
    let memory = Arc::new(ArmableMemoryAccount::default());
    let resource_service = installed_service(
        &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
        resource_events.clone(),
        memory.clone(),
    );
    let resource_producer = open_final(&resource_service, PRODUCER_A, uid(10));
    let issuer = frozen_issuer(&resource_service, PRODUCER_A, uid(10), 1);
    let input = shard(&issuer, PRODUCER_A, uid(10), 0, &[1]);
    resource_events.clear();
    memory.rejecting.store(true, Ordering::SeqCst);
    assert_eq!(
        resource_producer
            .complete(PartitionId::new(0), ProducerSequence::new(0), input,)
            .unwrap(),
        SubmitOutcome::TerminalNoop
    );
    let recorded = resource_events.snapshot();
    let rejected = recorded
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeFilterEvent::FinalDomainShardRejected {
                    rejection: FinalDomainRejectionKind::ResourceLimit,
                    ..
                }
            )
        })
        .expect("resource failure emits typed rejection");
    let unavailable = recorded
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::ChannelUnavailable { .. }))
        .expect("resource failure emits unavailable terminal");
    assert!(rejected < unavailable);
    assert_eq!(memory.current.load(Ordering::SeqCst), 0);
}

#[test]
fn final_semantic_rejection_linearizes_before_a_competing_terminal() {
    let events = Arc::new(Events::default());
    let service = installed_service(
        &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
        events.clone(),
        MemTrackerMemoryAccount::new_root_for_test("m3c-semantic-rejection-order"),
    );
    let producer = open_final(&service, PRODUCER_A, uid(10));
    let issuer = frozen_issuer(&service, PRODUCER_A, uid(10), 1);
    producer
        .complete(
            PartitionId::new(0),
            ProducerSequence::new(0),
            shard(&issuer, PRODUCER_A, uid(10), 0, &[1]),
        )
        .unwrap();
    events.clear();

    let channel = service
        .registry
        .active_installation()
        .unwrap()
        .channels()
        .next()
        .unwrap()
        .1
        .clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    channel.set_before_final_semantic_rejection_hook(Arc::new(move |next_dispatch_order| {
        entered_tx.send(next_dispatch_order).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
    }));

    let rejected_producer = producer.clone();
    let rejected = shard(&issuer, PRODUCER_A, uid(10), 0, &[2]);
    let (result_tx, result_rx) = mpsc::channel();
    let rejection_thread = std::thread::spawn(move || {
        result_tx
            .send(rejected_producer.complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                rejected,
            ))
            .unwrap();
    });
    let observed_next_order = entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("semantic rejection did not reach the in-lock linearization hook");

    let cancelling_service = service.clone();
    let (cancel_started_tx, cancel_started_rx) = mpsc::channel();
    let cancel_thread = std::thread::spawn(move || {
        cancel_started_tx.send(()).unwrap();
        cancelling_service.cancel();
    });
    cancel_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("competing terminal thread did not start");
    std::thread::sleep(Duration::from_millis(50));
    release_tx.send(()).unwrap();
    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err()
            .kind(),
        RuntimeContractViolationKind::ConflictingReplay
    );
    rejection_thread.join().unwrap();
    cancel_thread.join().unwrap();
    assert_eq!(
        observed_next_order, 2,
        "Core must reserve rejection order while the semantic-decision lock is still held"
    );

    let recorded = events.snapshot();
    let rejected = recorded
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::FinalDomainShardRejected { .. }))
        .expect("semantic rejection event must be delivered");
    let cancelled = recorded
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
        .expect("competing terminal event must be delivered");
    assert!(
        rejected < cancelled,
        "semantic rejection must linearize before the terminal that waited on its Core lock"
    );
    assert_eq!(service.dispatcher.pending_action_count(CHANNEL), 0);
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::FinalDomainShardRejected { .. }))
            .count(),
        1
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
            .count(),
        1
    );
    assert!(matches!(
        channel.terminal_action(),
        ChannelAction::Cancelled { .. }
    ));
}

#[test]
fn final_semantic_rejection_follows_a_terminal_that_linearized_first() {
    let events = Arc::new(Events::default());
    let service = installed_service(
        &[
            (PRODUCER_A, CoverageWitnessId::new(10), uid(10)),
            (PRODUCER_B, CoverageWitnessId::new(20), uid(20)),
        ],
        events.clone(),
        MemTrackerMemoryAccount::new_root_for_test("m3c-terminal-first-order"),
    );
    let producer_a = open_final(&service, PRODUCER_A, uid(10));
    let _producer_b = open_final(&service, PRODUCER_B, uid(20));
    let issuer_b = frozen_issuer(&service, PRODUCER_B, uid(20), 1);
    events.clear();

    let channel = service
        .registry
        .active_installation()
        .unwrap()
        .channels()
        .next()
        .unwrap()
        .1
        .clone();
    let terminal = channel.cancel();
    assert!(matches!(terminal, ChannelAction::Cancelled { .. }));

    let invalid = shard(&issuer_b, PRODUCER_B, uid(20), 0, &[7]);
    let (result_tx, result_rx) = mpsc::channel();
    let rejection_thread = std::thread::spawn(move || {
        result_tx
            .send(producer_a.complete(PartitionId::new(0), ProducerSequence::new(0), invalid))
            .unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    while service.dispatcher.pending_action_count(CHANNEL) == 0 {
        assert!(
            Instant::now() < deadline,
            "later semantic rejection never queued behind the terminal action"
        );
        std::thread::yield_now();
    }
    service.dispatcher.dispatch(CHANNEL, terminal).unwrap();
    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err()
            .kind(),
        RuntimeContractViolationKind::UnauthorizedBinding
    );
    rejection_thread.join().unwrap();
    assert_eq!(service.dispatcher.pending_action_count(CHANNEL), 0);

    let recorded = events.snapshot();
    let cancelled = recorded
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
        .expect("terminal event must be delivered");
    let rejected = recorded
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::FinalDomainShardRejected { .. }))
        .expect("later semantic rejection event must be delivered");
    assert!(cancelled < rejected);
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
            .count(),
        1
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::FinalDomainShardRejected { .. }))
            .count(),
        1
    );
    assert!(matches!(
        channel.terminal_action(),
        ChannelAction::Cancelled { .. }
    ));
}

#[derive(Default)]
struct AdversarialEvents {
    recorded: Mutex<Vec<RuntimeFilterEvent>>,
    service: Mutex<Option<Weak<RuntimeFilterService>>>,
    panicked: AtomicBool,
    cancelled: AtomicBool,
}

impl RuntimeFilterEventSink for AdversarialEvents {
    fn record(&self, event: RuntimeFilterEvent) {
        self.recorded.lock().unwrap().push(event.clone());
        if matches!(event, RuntimeFilterEvent::FinalDomainShardAccepted { .. })
            && !self.panicked.swap(true, Ordering::SeqCst)
        {
            panic!("intentional final-domain event sink panic");
        }
        if matches!(event, RuntimeFilterEvent::FinalDomainShardDuplicate { .. })
            && !self.cancelled.swap(true, Ordering::SeqCst)
            && let Some(service) = self
                .service
                .lock()
                .unwrap()
                .as_ref()
                .and_then(Weak::upgrade)
        {
            service.cancel();
        }
    }
}

#[test]
fn sink_panic_reentry_cancel_and_weak_handle_drop_are_safe_and_single_publish() {
    let events = Arc::new(AdversarialEvents::default());
    let service = installed_service(
        &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
        events.clone(),
        MemTrackerMemoryAccount::new_root_for_test("m3c-adversarial-events"),
    );
    *events.service.lock().unwrap() = Some(Arc::downgrade(&service));
    let producer = open_final(&service, PRODUCER_A, uid(10));
    let issuer = frozen_issuer(&service, PRODUCER_A, uid(10), 1);
    let input = shard(&issuer, PRODUCER_A, uid(10), 0, &[1]);
    assert_eq!(
        producer
            .complete(PartitionId::new(0), ProducerSequence::new(0), input.clone(),)
            .unwrap(),
        SubmitOutcome::Applied
    );
    assert!(events.panicked.load(Ordering::SeqCst));
    let duplicate_producer = producer.clone();
    let duplicate_input = input.clone();
    let (duplicate_tx, duplicate_rx) = mpsc::channel();
    let duplicate_worker = std::thread::spawn(move || {
        duplicate_tx
            .send(duplicate_producer.complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                duplicate_input,
            ))
            .unwrap();
    });
    assert_eq!(
        duplicate_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("sink-to-cancel reentry deadlocked")
            .unwrap(),
        SubmitOutcome::Duplicate
    );
    duplicate_worker.join().unwrap();
    assert!(events.cancelled.load(Ordering::SeqCst));
    assert_eq!(
        producer
            .complete(PartitionId::new(0), ProducerSequence::new(0), input,)
            .unwrap(),
        SubmitOutcome::TerminalNoop
    );
    let recorded = events.recorded.lock().unwrap().clone();
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
            .count(),
        1
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }))
            .count(),
        0
    );

    let weak = Arc::downgrade(&producer);
    drop(producer);
    assert!(weak.upgrade().is_none());
    let service_weak = Arc::downgrade(&service);
    drop(service);
    assert!(service_weak.upgrade().is_none());
}
