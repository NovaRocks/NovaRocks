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

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use novarocks::runtime_filter_transition::model::contract::{BindingId, ChannelId};
use novarocks::runtime_filter_transition::port::identity::{
    LogicalVersion, PartitionId, ProducerSequence,
};
use novarocks::runtime_filter_transition::port::producer::{
    ProducerFailureReason, ProducerHandle, ProducerPortKind, RuntimeContractViolationKind,
    SubmitOutcome,
};
use novarocks::runtime_filter_transition::port::subscription::{
    ArtifactDeliveryOutcome, LivePollOutcome, LiveTerminal, SubscriptionHandle, SubscriptionKind,
    UnavailableReason,
};
use novarocks::runtime_filter_transition::port::support::{
    MemoryAccountError, RuntimeFilterMemoryAccount,
};

use super::tests::{
    installed_ordered_service_fixture, installed_ordered_service_with_account, ordered_update,
};

fn uid(lo: i64) -> novarocks_types::UniqueId {
    novarocks_types::UniqueId::new(70, lo)
}

fn live_handle(
    service: &super::RuntimeFilterService,
) -> Arc<dyn novarocks::runtime_filter_transition::port::subscription::NonBlockingLiveSubscription>
{
    match service
        .subscribe(BindingId::new(2), uid(2), SubscriptionKind::NonBlockingLive)
        .unwrap()
    {
        SubscriptionHandle::Live(live) => live,
        SubscriptionHandle::Blocking(_) => panic!("ordered consumer returned blocking handle"),
    }
}

fn updated_version(outcome: LivePollOutcome) -> LogicalVersion {
    match outcome {
        LivePollOutcome::Updated { bundle, .. } => bundle.version(),
        other => panic!("expected live update, got {other:?}"),
    }
}

#[test]
fn ordered_service_live_poll_has_no_shared_cursor_and_skips_to_latest() {
    let service = installed_ordered_service_fixture();
    let first = live_handle(&service);
    let second = live_handle(&service);
    assert!(matches!(
        first.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: None
        }
    ));
    let (_, contract) = installed_ordered_service_with_account(
        super::memory::MemTrackerMemoryAccount::new_root_for_test("unused-ordered-contract"),
    );
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };

    assert_eq!(
        producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ordered_update(&contract, 100),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert_eq!(
        updated_version(first.poll_after(None)),
        LogicalVersion::FIRST
    );
    assert_eq!(
        updated_version(second.poll_after(None)),
        LogicalVersion::FIRST
    );

    assert_eq!(
        producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(1),
                ordered_update(&contract, 70),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert_eq!(
        updated_version(first.poll_after(Some(LogicalVersion::FIRST))),
        LogicalVersion::new(2)
    );
    assert_eq!(
        updated_version(second.poll_after(None)),
        LogicalVersion::new(2)
    );
}

#[test]
fn ordered_service_update_and_completed_terminal_are_one_live_snapshot() {
    let (service, contract) = installed_ordered_service_with_account(
        super::memory::MemTrackerMemoryAccount::new_root_for_test("ordered-live-completed"),
    );
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    producer
        .submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(0),
            ordered_update(&contract, 100),
        )
        .unwrap();
    assert_eq!(
        producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
            .unwrap(),
        SubmitOutcome::Completed
    );

    let outcome = live.poll_after(None);
    assert!(
        matches!(
            outcome,
            LivePollOutcome::Updated {
                terminal: Some(LiveTerminal::Completed),
                ..
            }
        ),
        "unexpected live completion snapshot: {outcome:?}"
    );
}

#[test]
fn ordered_completion_cannot_overtake_first_bundle_delivery() {
    let (service, contract) = installed_ordered_service_with_account(
        super::memory::MemTrackerMemoryAccount::new_root_for_test(
            "ordered-live-first-bundle-completion-race",
        ),
    );
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    let (owner_finished_tx, owner_finished_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    service.set_after_owner_finish_hook(Arc::new(move || {
        owner_finished_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
    }));

    let submitter = {
        let producer = producer.clone();
        let contract = contract.clone();
        std::thread::spawn(move || {
            producer.submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ordered_update(&contract, 100),
            )
        })
    };
    owner_finished_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first version must pause after the publish gate");
    let (close_tx, close_rx) = mpsc::channel();
    let closer = {
        let producer = producer.clone();
        std::thread::spawn(move || {
            close_tx
                .send(producer.close_partition(PartitionId::new(0), ProducerSequence::new(1)))
                .unwrap();
        })
    };

    if close_rx.recv_timeout(Duration::from_millis(100)).is_ok() {
        let visible = live.poll_after(None);
        assert!(
            matches!(
                visible,
                LivePollOutcome::Updated {
                    terminal: Some(LiveTerminal::Completed),
                    ..
                }
            ),
            "completion became visible before its first bundle: {visible:?}"
        );
    }

    release_tx.send(()).unwrap();
    submitter.join().unwrap().unwrap();
    closer.join().unwrap();
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Updated {
            terminal: Some(LiveTerminal::Completed),
            ..
        }
    ));
}

#[test]
fn ordered_service_completed_without_artifact_is_exact_live_terminal() {
    let service = installed_ordered_service_fixture();
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    assert_eq!(
        producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(0))
            .unwrap(),
        SubmitOutcome::CompletedWithoutArtifact
    );
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::CompletedWithoutArtifact)
        }
    ));
}

#[test]
fn ordered_live_activation_mismatch_is_typed_and_does_not_poison_live_handle() {
    let service = installed_ordered_service_fixture();
    let error = service
        .subscribe(
            BindingId::new(2),
            uid(2),
            SubscriptionKind::BlockingSnapshot,
        )
        .unwrap_err();
    assert_eq!(
        error.kind(),
        RuntimeContractViolationKind::SubscriptionActivationMismatch
    );
    assert!(matches!(
        service
            .subscribe(BindingId::new(2), uid(2), SubscriptionKind::NonBlockingLive,)
            .unwrap(),
        SubscriptionHandle::Live(_)
    ));
}

#[derive(Default)]
struct ArmableLargeAllocationRejector {
    armed: AtomicBool,
    current: AtomicUsize,
}

impl RuntimeFilterMemoryAccount for ArmableLargeAllocationRejector {
    fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
        if self.armed.load(Ordering::SeqCst) && bytes > 256 {
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

#[test]
fn ordered_final_artifact_failure_retains_latest_and_degraded_terminal() {
    let account = Arc::new(ArmableLargeAllocationRejector::default());
    let (service, contract) = installed_ordered_service_with_account(account.clone());
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    producer
        .submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(0),
            ordered_update(&contract, 100),
        )
        .unwrap();
    assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);

    account.armed.store(true, Ordering::SeqCst);
    assert_eq!(
        producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(1),
                ordered_update(&contract, 70),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: Some(LiveTerminal::DegradedArtifact(
                UnavailableReason::ResourceLimit
            ))
        }
    ));

    account.armed.store(false, Ordering::SeqCst);
    assert_eq!(
        producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(2))
            .unwrap(),
        SubmitOutcome::Completed
    );
    let outcome = live.poll_after(Some(LogicalVersion::FIRST));
    assert!(
        matches!(
            outcome,
            LivePollOutcome::Idle {
                latest_version: Some(LogicalVersion::FIRST),
                terminal: Some(LiveTerminal::DegradedArtifact(
                    UnavailableReason::ResourceLimit
                ))
            }
        ),
        "unexpected live completion after artifact degradation: {outcome:?}"
    );
}

#[test]
fn ordered_service_first_materialization_failure_is_unavailable() {
    let account = Arc::new(ArmableLargeAllocationRejector::default());
    let (service, contract) = installed_ordered_service_with_account(account.clone());
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    account.armed.store(true, Ordering::SeqCst);

    assert_eq!(
        producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ordered_update(&contract, 100),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert!(live.snapshot().is_none());
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(UnavailableReason::ResourceLimit))
        }
    ));
}

#[test]
fn ordered_service_first_route_failure_is_unavailable() {
    let service = installed_ordered_service_fixture();
    let live = live_handle(&service);
    let installed = service.registry.active_installation().unwrap();
    let routes = installed.artifact_plan(ChannelId::new(1)).unwrap().groups()[0]
        .route_edges()
        .to_vec();

    assert_eq!(
        installed.router().route_live(
            &routes,
            Some(&ArtifactDeliveryOutcome::Unavailable(
                UnavailableReason::RouteUnavailable,
            )),
            None,
        ),
        routes
    );
    assert!(live.snapshot().is_none());
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(
                UnavailableReason::RouteUnavailable
            ))
        }
    ));
}

#[test]
fn ordered_service_cancellation_retains_latest_live_snapshot() {
    let (service, contract) = installed_ordered_service_with_account(
        super::memory::MemTrackerMemoryAccount::new_root_for_test("ordered-live-cancelled"),
    );
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    producer
        .submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(0),
            ordered_update(&contract, 100),
        )
        .unwrap();
    assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);

    service.cancel();

    assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: Some(LiveTerminal::Cancelled)
        }
    ));
}

#[test]
fn ordered_service_cancel_overrides_completed_and_retains_latest() {
    let (service, contract) = installed_ordered_service_with_account(
        super::memory::MemTrackerMemoryAccount::new_root_for_test(
            "ordered-live-completed-then-cancelled",
        ),
    );
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    producer
        .submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(0),
            ordered_update(&contract, 100),
        )
        .unwrap();
    producer
        .close_partition(PartitionId::new(0), ProducerSequence::new(1))
        .unwrap();
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            terminal: Some(LiveTerminal::Completed),
            ..
        }
    ));

    service.cancel();

    assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            terminal: Some(LiveTerminal::Cancelled),
            ..
        }
    ));
}

#[test]
fn ordered_service_cancel_overrides_unavailable_without_artifact() {
    let service = installed_ordered_service_fixture();
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    assert_eq!(
        producer
            .fail(ProducerFailureReason::ExecutionFailed)
            .unwrap(),
        SubmitOutcome::CoverageStillPossible
    );
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(UnavailableReason::ProducerFailed))
        }
    ));

    service.cancel();

    assert!(live.snapshot().is_none());
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Cancelled)
        }
    ));
}

#[test]
fn ordered_cancel_between_reducer_commit_and_dispatch_releases_every_owner() {
    let account = Arc::new(ArmableLargeAllocationRejector::default());
    let (service, contract) = installed_ordered_service_with_account(account.clone());
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    service.set_producer_before_dispatch_hook(
        BindingId::new(1),
        uid(1),
        Arc::new(move || {
            ready_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }),
    );

    let (result_tx, result_rx) = mpsc::channel();
    let submit = std::thread::spawn(move || {
        result_tx
            .send(producer.submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ordered_update(&contract, 100),
            ))
            .unwrap();
    });
    ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("ordered action must pause after reducer commit");
    assert!(account.current.load(Ordering::SeqCst) > 0);

    service.cancel();
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Cancelled)
        }
    ));
    release_tx.send(()).unwrap();
    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        SubmitOutcome::Published
    );
    submit.join().unwrap();
    assert!(live.snapshot().is_none());

    drop(live);
    drop(service);
    assert_eq!(account.current.load(Ordering::SeqCst), 0);
}

#[test]
fn service_drop_cancels_caller_owned_encode_without_retaining_service() {
    let account = Arc::new(ArmableLargeAllocationRejector::default());
    let (service, contract) = installed_ordered_service_with_account(account.clone());
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    let (encode_tx, encode_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    service.set_before_encode_hook(Arc::new(move |_| {
        encode_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
    }));

    let (result_tx, result_rx) = mpsc::channel();
    let submit = std::thread::spawn(move || {
        result_tx
            .send(producer.submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ordered_update(&contract, 100),
            ))
            .unwrap();
    });
    encode_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("ordered v1 encode must enter its caller-owned scoped work");

    let weak_service = Arc::downgrade(&service);
    drop(service);
    assert!(weak_service.upgrade().is_none());
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Cancelled)
        }
    ));

    release_tx.send(()).unwrap();
    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        SubmitOutcome::Published
    );
    submit.join().unwrap();
    assert!(live.snapshot().is_none());
    drop(live);
    assert_eq!(account.current.load(Ordering::SeqCst), 0);
}
