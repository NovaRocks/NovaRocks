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

use super::*;
use crate::runtime::fragment::io::exchange_unit::{
    ExchangeAdmissionVerdict, ExchangeDeliveryCallback, ExchangeDemandCallback, ExchangeGrantWake,
    ExchangeUnitReceipt,
};
use novarocks_execution_contract::{ExchangeSource, FragmentNodeId, TaskIdentity};
use novarocks_types::UniqueId;
use novarocks_types::identity::{AttemptId, QueryExecutionId, QueryId, StageId, TaskId};
use std::sync::atomic::{AtomicBool, Ordering};

type Retry = Box<dyn FnOnce() + Send + 'static>;
struct Call {
    frame: ExchangeUnitFrame,
    activity: ExchangeCallActivity,
    done: ExchangeDeliveryCallback,
}
impl Call {
    fn complete(self, custody: u64, granted: u64) {
        (self.done)(observed(custody, granted));
        drop(self.activity);
    }
}
#[derive(Default)]
struct Scripted {
    grant: AtomicBool,
    calls: Mutex<VecDeque<Call>>,
    demands: Mutex<Vec<ExchangeDemandEntry>>,
    watches: Mutex<HashMap<ExchangeStreamIdentity, ExchangeGrantWake>>,
    retries: Mutex<VecDeque<Retry>>,
    stop_replies: Mutex<VecDeque<ExchangeDemandCallOutcome>>,
    demand_replies: Mutex<VecDeque<ExchangeDemandCallOutcome>>,
    normal_closes: Mutex<Vec<ExchangeNormalCloseAccepted>>,
    normal_close_reentry: Mutex<Option<Weak<ExchangeStreamSender>>>,
}
impl Scripted {
    fn granting() -> Arc<Self> {
        Arc::new(Self {
            grant: AtomicBool::new(true),
            ..Default::default()
        })
    }
    fn take(&self) -> Call {
        self.calls
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted active call")
    }
    fn queued(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
    fn notice(&self, stream: ExchangeStreamIdentity, notice: ExchangeGrantNotice) {
        let wake = self
            .watches
            .lock()
            .unwrap()
            .get(&stream)
            .cloned()
            .expect("watched stream");
        wake(notice);
    }
    fn retry(&self) {
        let retry = self
            .retries
            .lock()
            .unwrap()
            .pop_front()
            .expect("scheduled retry");
        retry();
    }
}
impl ExchangeFrameTransmitter for Scripted {
    fn normal_close_accepted(&self, notice: ExchangeNormalCloseAccepted) {
        if let Some(sender) = self
            .normal_close_reentry
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
        {
            assert!(
                sender.state.try_lock().is_ok(),
                "normal close observation runs outside the sender lock"
            );
            let snapshot = sender.snapshot();
            assert_eq!(snapshot.stop_obligation, ExchangeSenderStopState::Confirmed);
            assert_eq!(notice.stream, sender.config.stream);
            assert_eq!(notice.sender_count, sender.config.sender_count);
        }
        self.normal_closes.lock().unwrap().push(notice);
    }
    fn deliver_unit(
        &self,
        frame: ExchangeUnitFrame,
        _stop: CancellationToken,
        activity: ExchangeCallActivity,
        done: ExchangeDeliveryCallback,
    ) {
        self.calls.lock().unwrap().push_back(Call {
            frame,
            activity,
            done,
        });
    }
    fn register_unit_demands(
        &self,
        _destination: RuntimeEndpoint,
        entries: Vec<ExchangeDemandEntry>,
        _stop: CancellationToken,
        done: ExchangeDemandCallback,
    ) {
        self.demands.lock().unwrap().extend(entries.iter().copied());
        if entries
            .iter()
            .all(|entry| entry.operation == ExchangeDemandOperation::StopSender)
        {
            let reply = self.stop_replies.lock().unwrap().pop_front();
            if let Some(reply) = reply {
                done(reply);
                return;
            }
        }
        if entries
            .iter()
            .all(|entry| matches!(entry.operation, ExchangeDemandOperation::Register(_)))
        {
            let reply = self.demand_replies.lock().unwrap().pop_front();
            if let Some(reply) = reply {
                done(reply);
                return;
            }
        }
        let facts = entries
            .into_iter()
            .map(|entry| {
                let facts = match entry.operation {
                    ExchangeDemandOperation::Register(demand) => ExchangeStreamFacts {
                        granted_through: if self.grant.load(Ordering::Relaxed) {
                            demand.sequence.get()
                        } else {
                            0
                        },
                        ..Default::default()
                    },
                    ExchangeDemandOperation::StopSender => ExchangeStreamFacts {
                        sender_stopped: true,
                        ..Default::default()
                    },
                };
                (entry.stream, Ok(facts))
            })
            .collect();
        done(ExchangeDemandCallOutcome::Observed(facts));
    }
    fn watch_unit_facts(
        &self,
        _destination: RuntimeEndpoint,
        stream: ExchangeStreamIdentity,
        wake: ExchangeGrantWake,
    ) -> Result<(), FailureFact> {
        self.watches.lock().unwrap().insert(stream, wake);
        Ok(())
    }
    fn forget_unit_facts(&self, stream: ExchangeStreamIdentity) {
        self.watches.lock().unwrap().remove(&stream);
    }
    fn schedule_unit_retry(&self, _delay: Duration, retry: Retry) -> Result<(), FailureFact> {
        self.retries.lock().unwrap().push_back(retry);
        Ok(())
    }
}
fn observed(custody: u64, granted: u64) -> ExchangeCallOutcome {
    ExchangeCallOutcome::Observed(ExchangeUnitReceipt {
        verdict: ExchangeAdmissionVerdict::Applied,
        facts: ExchangeStreamFacts {
            custody_through: custody,
            granted_through: granted,
            ..Default::default()
        },
    })
}
fn stream_in_query(ordinal: u32, query: i64) -> ExchangeStreamIdentity {
    let execution =
        QueryExecutionId::new(QueryId::new(query, 2), AttemptId::new(1).unwrap()).unwrap();
    let task = |n, process| {
        TaskIdentity::new(
            execution,
            StageId::new(n).unwrap(),
            TaskId::new(n).unwrap(),
            process,
        )
    };
    ExchangeStreamIdentity::try_new(
        ExchangeSource::new(
            task(1, BackendProcessId::new_v7()),
            UniqueId::new(1, i64::from(ordinal) + 1),
            ordinal,
        ),
        task(2, BackendProcessId::new_v7()),
        UniqueId::new(2, 2),
        FragmentNodeId::new(7),
    )
    .unwrap()
}
fn sender(
    transport: Arc<Scripted>,
    scheduler: Arc<ExchangeUnitSendScheduler>,
    ordinal: u32,
    delivery: ExchangeDeliveryRequirement,
) -> Arc<ExchangeStreamSender> {
    sender_in_query(transport, scheduler, ordinal, delivery, 1)
}
fn sender_in_query(
    transport: Arc<Scripted>,
    scheduler: Arc<ExchangeUnitSendScheduler>,
    ordinal: u32,
    delivery: ExchangeDeliveryRequirement,
    query: i64,
) -> Arc<ExchangeStreamSender> {
    ExchangeStreamSender::try_new(
        ExchangeStreamSenderConfig {
            stream: stream_in_query(ordinal, query),
            destination: RuntimeEndpoint::new("127.0.0.1", 9000).unwrap(),
            sender_count: 16,
            sender_id: 4,
            backend_number: 5,
            delivery,
            retained_window: 64,
            max_unit_bytes: 1024,
            retry_delay: Duration::from_millis(1),
        },
        transport,
        scheduler,
        Arc::new(Observable::new()),
    )
    .unwrap()
}
fn scheduler(bytes: usize) -> Arc<ExchangeUnitSendScheduler> {
    ExchangeUnitSendScheduler::try_new(bytes, 64, 1024).unwrap()
}
fn publish(
    sender: &Arc<ExchangeStreamSender>,
    bytes: &'static [u8],
    end: bool,
) -> PublishedStreamRange {
    match sender.publish(&Bytes::from_static(bytes), end).unwrap() {
        ExchangeUnitPublishOutcome::Published(range) => range,
        ExchangeUnitPublishOutcome::Backpressured => panic!("expected publication capacity"),
    }
}

#[test]
fn grant_precedes_offer_and_shared_activity_and_capacity_wait_spends_no_sequence() {
    let transport = Arc::new(Scripted::default());
    let scheduler = scheduler(16);
    let sender = sender(
        transport.clone(),
        scheduler.clone(),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    let mut invalid = sender.config.clone();
    invalid.retry_delay = Duration::ZERO;
    assert!(matches!(
        ExchangeStreamSender::try_new(
            invalid,
            transport.clone(),
            scheduler.clone(),
            Arc::new(Observable::new())
        ),
        Err(ExchangeUnitSenderError::InvalidLimits)
    ));
    assert_eq!(publish(&sender, b"one", false).data_sequence, Some(1));
    let before = sender.snapshot();
    let generation = sender.observable().generation();
    let payload = Bytes::from_static(b"two");
    assert_eq!(
        sender.publish(&payload, false),
        Ok(ExchangeUnitPublishOutcome::Backpressured)
    );
    assert_eq!(sender.snapshot(), before);
    assert_eq!(sender.failure(), None);
    assert_eq!(sender.observable().generation(), generation);
    assert_eq!(payload.as_ref(), b"two");
    assert_eq!(before.retention.produced_through, 1);
    assert_eq!(before.retention.offered_through, 0);
    assert_eq!(scheduler.snapshot().bytes, 0);
    transport.notice(
        sender.config.stream,
        ExchangeGrantNotice::Facts(ExchangeStreamFacts {
            granted_through: 1,
            ..Default::default()
        }),
    );
    assert_eq!(transport.take().frame.receiver.item.sequence().get(), 1);
    assert!(sender.observable().generation() > generation);
    assert_eq!(
        sender.publish(&payload, false),
        Ok(ExchangeUnitPublishOutcome::Published(
            PublishedStreamRange {
                data_sequence: Some(2),
                end_sequence: None,
            }
        ))
    );
    sender.stop();
}

#[test]
fn unknown_retains_bytes_but_actual_exit_unblocks_a_healthy_peer() {
    let transport = Scripted::granting();
    let scheduler = scheduler(3);
    let dead = sender(
        transport.clone(),
        scheduler.clone(),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    let healthy = sender(
        transport.clone(),
        scheduler.clone(),
        2,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    publish(&dead, b"one", false);
    publish(&healthy, b"two", false);
    assert_eq!(transport.queued(), 1);
    let first = transport.take();
    let pointer = first.frame.receiver.payload.as_ptr();
    (first.done)(ExchangeCallOutcome::Unknown("lost reply".into()));
    assert_eq!(scheduler.snapshot().bytes, 3);
    assert_eq!(dead.snapshot().retention.data_positions, 1);
    drop(first.activity);
    let healthy_call = transport.take();
    assert_eq!(healthy_call.frame.receiver.stream, healthy.config.stream);
    transport.retry();
    assert_eq!(transport.queued(), 0);
    healthy_call.complete(1, 1);
    let replay = transport.take();
    assert_eq!(replay.frame.receiver.stream, dead.config.stream);
    assert_eq!(replay.frame.receiver.item.sequence().get(), 1);
    assert_eq!(replay.frame.receiver.payload.as_ptr(), pointer);
    replay.complete(1, 1);
    assert_eq!(scheduler.snapshot().bytes, 0);
    dead.stop();
    healthy.stop();
}

#[test]
fn admitted_but_not_custody_covered_waits_for_subscription_facts() {
    let transport = Scripted::granting();
    let scheduler = scheduler(16);
    let sender = sender(
        transport.clone(),
        scheduler.clone(),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    publish(&sender, b"one", false);
    transport.take().complete(0, 1);
    assert_eq!(scheduler.snapshot().bytes, 0);
    assert_eq!(sender.snapshot().retention.data_positions, 1);
    assert!(sender.snapshot().watching);
    transport.notice(
        sender.config.stream,
        ExchangeGrantNotice::Facts(ExchangeStreamFacts {
            custody_through: 1,
            granted_through: 1,
            ..Default::default()
        }),
    );
    assert_eq!(sender.snapshot().retention.data_positions, 0);
    sender.stop();
}

#[test]
fn stop_preserves_actual_call_activity_until_its_real_exit() {
    let transport = Scripted::granting();
    let scheduler = scheduler(16);
    let sender = sender(
        transport.clone(),
        scheduler.clone(),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    publish(&sender, b"one", false);
    let call = transport.take();
    sender.stop();
    assert_eq!(
        sender.snapshot().status,
        ExchangeStreamSenderStatus::Stopped
    );
    assert_eq!(sender.snapshot().active_calls, 1);
    assert_eq!(scheduler.snapshot().bytes, 3);
    assert_eq!(call.frame.receiver.payload.as_ref(), b"one");
    (call.done)(ExchangeCallOutcome::NotProcessed("cancelled".into()));
    assert_eq!(scheduler.snapshot().bytes, 3);
    drop(call.activity);
    assert_eq!(scheduler.snapshot().calls, 0);
    assert_eq!(sender.snapshot().active_calls, 0);
    assert!(
        transport
            .demands
            .lock()
            .unwrap()
            .iter()
            .any(|entry| entry.operation == ExchangeDemandOperation::StopSender)
    );
}

#[test]
fn concurrent_publishers_share_one_contiguous_sequence_domain() {
    let transport = Scripted::granting();
    let scheduler = scheduler(1024);
    let sender = sender(
        transport.clone(),
        scheduler,
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let handles = (0..8)
        .map(|_| {
            let sender = sender.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let payload = Bytes::from_static(b"x");
                barrier.wait();
                loop {
                    match sender.publish(&payload, false) {
                        Ok(ExchangeUnitPublishOutcome::Published(range)) => {
                            break range.data_sequence.unwrap();
                        }
                        Ok(ExchangeUnitPublishOutcome::Backpressured) => std::thread::yield_now(),
                        Err(error) => panic!("unexpected publication failure: {error}"),
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    let mut sequences = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    sequences.sort_unstable();
    assert_eq!(sequences, (1..=8).collect::<Vec<_>>());
    assert_eq!(sender.snapshot().retention.produced_through, 8);
    sender.stop();
    while transport.queued() > 0 {
        let call = transport.take();
        drop(call);
    }
}

#[test]
fn empty_stream_end_needs_no_grant_and_custody_is_not_consumption() {
    let transport = Arc::new(Scripted::default());
    let scheduler = scheduler(16);
    let sender = sender(
        transport.clone(),
        scheduler.clone(),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    assert_eq!(sender.finish().unwrap().end_sequence, Some(1));
    let call = transport.take();
    assert!(matches!(call.frame.receiver.item, ExchangeUnitItem::End(_)));
    assert!(transport.demands.lock().unwrap().is_empty());
    call.complete(1, 0);
    assert_eq!(
        sender.snapshot().status,
        ExchangeStreamSenderStatus::CustodyComplete
    );
    assert_eq!(scheduler.snapshot().calls, 0);
}

#[test]
fn unit_and_end_can_be_offered_before_earlier_custody() {
    let transport = Scripted::granting();
    let scheduler = scheduler(16);
    let sender = sender(
        transport.clone(),
        scheduler,
        1,
        ExchangeDeliveryRequirement::MustDrain,
    );
    publish(&sender, b"one", false);
    assert_eq!(publish(&sender, b"two", true).end_sequence, Some(3));
    let first = transport.take();
    let last = transport.take();
    assert!(matches!(
        last.frame.receiver.item,
        ExchangeUnitItem::Unit {
            end_after_unit: true,
            ..
        }
    ));
    last.complete(0, 2);
    assert_eq!(sender.snapshot().retention.consumed_through, 0);
    first.complete(3, 2);
    assert_eq!(
        sender.snapshot().status,
        ExchangeStreamSenderStatus::CustodyComplete
    );
}

#[test]
fn receiver_withdrawal_obeys_delivery_requirement_and_consumed_requires_exact_end() {
    // A receiver may withdraw before it grants any Unit. Demand replies and
    // grant subscriptions share the same committed fact author as delivery.
    for through_demand in [true, false] {
        let transport = Arc::new(Scripted::default());
        let sender = sender(
            transport.clone(),
            scheduler(16),
            1,
            ExchangeDeliveryRequirement::DemandDriven,
        );
        *transport.normal_close_reentry.lock().unwrap() = Some(Arc::downgrade(&sender));
        let facts = ExchangeStreamFacts {
            receiver_closed: Some(ExchangeUnitCloseDisposition::Withdrawn),
            ..Default::default()
        };
        if through_demand {
            transport.demand_replies.lock().unwrap().push_back(
                ExchangeDemandCallOutcome::Observed(vec![(sender.config.stream, Ok(facts))]),
            );
        }
        publish(&sender, b"one", false);
        if !through_demand {
            transport.notice(sender.config.stream, ExchangeGrantNotice::Facts(facts));
        }
        assert_eq!(
            transport.queued(),
            0,
            "close before grant never delivers a Unit"
        );
        assert_eq!(
            sender.snapshot().status,
            ExchangeStreamSenderStatus::Withdrawn
        );
        assert_eq!(
            *transport.normal_closes.lock().unwrap(),
            vec![ExchangeNormalCloseAccepted {
                stream: sender.config.stream,
                sender_count: sender.config.sender_count,
                facts,
            }]
        );
    }
    for delivery in [
        ExchangeDeliveryRequirement::DemandDriven,
        ExchangeDeliveryRequirement::MustDrain,
    ] {
        let transport = Scripted::granting();
        let scheduler = scheduler(16);
        let sender = sender(transport.clone(), scheduler, 1, delivery);
        publish(&sender, b"one", false);
        let call = transport.take();
        (call.done)(ExchangeCallOutcome::Observed(ExchangeUnitReceipt {
            verdict: ExchangeAdmissionVerdict::Idempotent,
            facts: ExchangeStreamFacts {
                receiver_closed: Some(ExchangeUnitCloseDisposition::Withdrawn),
                ..Default::default()
            },
        }));
        assert_eq!(
            sender.failure().is_some(),
            delivery == ExchangeDeliveryRequirement::MustDrain
        );
        assert_eq!(
            transport.normal_closes.lock().unwrap().len(),
            usize::from(delivery == ExchangeDeliveryRequirement::DemandDriven)
        );
        drop(call.activity);
        assert_eq!(sender.snapshot().retention.data_positions, 0);
    }
    let transport = Scripted::granting();
    let sender = sender(
        transport.clone(),
        scheduler(16),
        1,
        ExchangeDeliveryRequirement::MustDrain,
    );
    publish(&sender, b"one", true);
    let call = transport.take();
    (call.done)(ExchangeCallOutcome::Observed(ExchangeUnitReceipt {
        verdict: ExchangeAdmissionVerdict::Applied,
        facts: ExchangeStreamFacts {
            custody_through: 2,
            granted_through: 1,
            receiver_closed: Some(ExchangeUnitCloseDisposition::Consumed {
                final_sequence: ExchangeUnitSequence::try_new(1).unwrap(),
            }),
            ..Default::default()
        },
    }));
    assert!(sender.failure().is_some());
    assert!(
        transport.normal_closes.lock().unwrap().is_empty(),
        "an invalid End cannot publish a normal-close observation"
    );
    drop(call.activity);
}

#[test]
fn omitted_snapshot_stream_replays_original_demand_and_retained_item() {
    let transport = Arc::new(Scripted::default());
    let scheduler = scheduler(16);
    let sender = sender(
        transport.clone(),
        scheduler,
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    publish(&sender, b"one", false);
    transport.notice(sender.config.stream, ExchangeGrantNotice::Resynchronize);
    assert_eq!(transport.demands.lock().unwrap().len(), 2);
    transport.notice(
        sender.config.stream,
        ExchangeGrantNotice::Facts(ExchangeStreamFacts {
            granted_through: 1,
            ..Default::default()
        }),
    );
    let first = transport.take();
    let bytes = first.frame.receiver.payload.clone();
    first.complete(0, 1);
    transport.notice(sender.config.stream, ExchangeGrantNotice::Resynchronize);
    let replay = transport.take();
    assert_eq!(replay.frame.receiver.payload, bytes);
    assert_eq!(replay.frame.receiver.item.sequence().get(), 1);
    assert_eq!(sender.snapshot().retention.consumed_through, 0);
    replay.complete(1, 1);
    sender.stop();
}

#[test]
fn oversized_legal_activity_runs_exclusively_and_is_not_starved_by_small_calls() {
    let transport = Scripted::granting();
    let scheduler = scheduler(4);
    let first = sender(
        transport.clone(),
        scheduler.clone(),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    let oversized = sender(
        transport.clone(),
        scheduler.clone(),
        2,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    let small = sender(
        transport.clone(),
        scheduler.clone(),
        3,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    publish(&first, b"one", false);
    publish(&oversized, b"123456789", false);
    publish(&small, b"x", false);
    assert_eq!(transport.queued(), 1);
    transport.take().complete(1, 1);
    assert_eq!(scheduler.snapshot().bytes, 9);
    assert_eq!(scheduler.snapshot().calls, 1);
    let large = transport.take();
    assert_eq!(large.frame.receiver.stream, oversized.config.stream);
    large.complete(1, 1);
    let last = transport.take();
    assert_eq!(last.frame.receiver.stream, small.config.stream);
    last.complete(1, 1);
    assert_eq!(scheduler.snapshot().bytes, 0);
    first.stop();
    oversized.stop();
    small.stop();
}

#[test]
fn subscription_refusal_is_originating_and_never_retried() {
    let transport = Scripted::granting();
    let scheduler = scheduler(16);
    let sender = sender(
        transport.clone(),
        scheduler.clone(),
        1,
        ExchangeDeliveryRequirement::MustDrain,
    );
    publish(&sender, b"one", false);
    let call = transport.take();
    let failure = contract_failure();
    transport.notice(sender.config.stream, ExchangeGrantNotice::Refused(failure));
    assert_eq!(sender.failure(), Some(failure));
    assert_eq!(sender.snapshot().retention.data_positions, 0);
    assert_eq!(scheduler.snapshot().bytes, 3);
    assert!(transport.retries.lock().unwrap().is_empty());
    (call.done)(ExchangeCallOutcome::Unknown(
        "late transport timeout".into(),
    ));
    assert!(transport.retries.lock().unwrap().is_empty());
    drop(call.activity);
    assert_eq!(scheduler.snapshot().calls, 0);
    assert_eq!(sender.snapshot().active_calls, 0);
}

#[test]
fn unknown_stop_retries_one_bounded_obligation_without_holding_data_activity() {
    let transport = Scripted::granting();
    let scheduler = scheduler(16);
    let sender = sender(
        transport.clone(),
        scheduler.clone(),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    publish(&sender, b"one", false);
    let call = transport.take();
    transport
        .stop_replies
        .lock()
        .unwrap()
        .push_back(ExchangeDemandCallOutcome::Unknown("stop reply lost".into()));
    sender.stop();
    assert_eq!(
        sender.snapshot().stop_obligation,
        ExchangeSenderStopState::Pending
    );
    assert_eq!(sender.snapshot().retention.data_positions, 0);
    assert_eq!(scheduler.snapshot().bytes, 3);
    (call.done)(ExchangeCallOutcome::NotProcessed("cancelled".into()));
    drop(call.activity);
    assert_eq!(scheduler.snapshot().bytes, 0);
    assert_eq!(
        sender.snapshot().stop_obligation,
        ExchangeSenderStopState::Pending
    );
    transport.retry();
    assert_eq!(
        sender.snapshot().stop_obligation,
        ExchangeSenderStopState::Confirmed
    );
    assert_eq!(
        transport
            .demands
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.operation == ExchangeDemandOperation::StopSender)
            .count(),
        2
    );
    assert!(transport.retries.lock().unwrap().is_empty());
}

#[test]
fn authoritative_route_retirement_ends_stop_retries_without_forging_confirmation() {
    let transport = Scripted::granting();
    let sender = sender(
        transport.clone(),
        scheduler(16),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    publish(&sender, b"one", false);
    transport.take().complete(0, 1);
    transport
        .stop_replies
        .lock()
        .unwrap()
        .push_back(ExchangeDemandCallOutcome::NotProcessed("offline".into()));
    sender.stop();
    assert_eq!(
        sender.snapshot().stop_obligation,
        ExchangeSenderStopState::Pending
    );
    sender.retire_after_route_retirement();
    transport.retry();
    assert_eq!(
        sender.snapshot().stop_obligation,
        ExchangeSenderStopState::RouteRetired
    );
    assert_eq!(
        transport
            .demands
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.operation == ExchangeDemandOperation::StopSender)
            .count(),
        1,
    );
}

#[test]
fn exact_consumed_proof_covers_its_end_even_with_an_older_custody_snapshot() {
    let transport = Scripted::granting();
    let sender = sender(
        transport.clone(),
        scheduler(16),
        1,
        ExchangeDeliveryRequirement::MustDrain,
    );
    publish(&sender, b"one", true);
    let call = transport.take();
    (call.done)(ExchangeCallOutcome::Observed(ExchangeUnitReceipt {
        verdict: ExchangeAdmissionVerdict::Idempotent,
        facts: ExchangeStreamFacts {
            custody_through: 1,
            granted_through: 1,
            receiver_closed: Some(ExchangeUnitCloseDisposition::Consumed {
                final_sequence: ExchangeUnitSequence::try_new(2).unwrap(),
            }),
            ..Default::default()
        },
    }));
    drop(call.activity);
    assert_eq!(
        sender.snapshot().status,
        ExchangeStreamSenderStatus::CustodyComplete
    );
    assert_eq!(sender.snapshot().retention.consumed_through, 2);
}

#[test]
fn a_many_stream_query_does_not_take_the_single_stream_querys_turn() {
    let transport = Scripted::granting();
    let scheduler = scheduler(3);
    let active = sender_in_query(
        transport.clone(),
        scheduler.clone(),
        0,
        ExchangeDeliveryRequirement::DemandDriven,
        1,
    );
    publish(&active, b"one", false);
    let many = (1..=6)
        .map(|ordinal| {
            sender_in_query(
                transport.clone(),
                scheduler.clone(),
                ordinal,
                ExchangeDeliveryRequirement::DemandDriven,
                1,
            )
        })
        .collect::<Vec<_>>();
    for stream in &many {
        publish(stream, b"one", false);
    }
    let single = sender_in_query(
        transport.clone(),
        scheduler.clone(),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
        2,
    );
    publish(&single, b"two", false);
    assert_eq!(transport.queued(), 1);
    transport.take().complete(1, 1);
    let first_a = transport.take();
    assert_eq!(
        first_a
            .frame
            .receiver
            .stream
            .source()
            .task()
            .query_execution_id(),
        active.config.stream.source().task().query_execution_id()
    );
    first_a.complete(1, 1);
    let first_b = transport.take();
    assert_eq!(first_b.frame.receiver.stream, single.config.stream);
    assert!(
        many.iter()
            .filter(|sender| sender.snapshot().retention.consumed_through == 0)
            .count()
            >= 5
    );
    first_b.complete(1, 1);
    active.stop();
    single.stop();
    for stream in &many {
        stream.stop();
    }
    while transport.queued() > 0 {
        drop(transport.take());
    }
    assert_eq!(scheduler.snapshot().calls, 0);
}

#[test]
fn final_owner_exit_removes_watch_and_does_not_restart_queued_retries() {
    let transport = Arc::new(Scripted::default());
    let sender = sender(
        transport.clone(),
        scheduler(16),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    let stream = sender.config.stream;
    let data_stop = sender.cancellation.clone();
    let control_stop = sender.stop_cancellation.clone();
    publish(&sender, b"one", false);
    assert!(transport.watches.lock().unwrap().contains_key(&stream));
    transport
        .demand_replies
        .lock()
        .unwrap()
        .push_back(ExchangeDemandCallOutcome::Unknown(
            "lost demand reply".into(),
        ));
    transport.notice(stream, ExchangeGrantNotice::Resynchronize);
    let sent = transport.demands.lock().unwrap().len();
    let weak = Arc::downgrade(&sender);
    drop(sender);
    assert!(weak.upgrade().is_none());
    assert!(transport.watches.lock().unwrap().is_empty());
    assert!(data_stop.is_cancelled() && control_stop.is_cancelled());
    transport.retry();
    assert_eq!(transport.demands.lock().unwrap().len(), sent);

    let stopped = sender_in_query(
        transport.clone(),
        scheduler(16),
        2,
        ExchangeDeliveryRequirement::DemandDriven,
        1,
    );
    publish(&stopped, b"one", false);
    transport
        .stop_replies
        .lock()
        .unwrap()
        .push_back(ExchangeDemandCallOutcome::Unknown("lost stop reply".into()));
    stopped.stop();
    assert_eq!(
        stopped.snapshot().stop_obligation,
        ExchangeSenderStopState::Pending
    );
    let sent = transport.demands.lock().unwrap().len();
    let weak = Arc::downgrade(&stopped);
    drop(stopped);
    assert!(weak.upgrade().is_none());
    transport.retry();
    assert_eq!(transport.demands.lock().unwrap().len(), sent);
}

#[test]
fn peer_blocked_large_unit_does_not_block_healthy_peer_with_available_bytes() {
    let transport = Scripted::granting();
    let scheduler = ExchangeUnitSendScheduler::try_new(10, 1, 1024).unwrap();
    let first = sender_in_query(
        transport.clone(),
        scheduler.clone(),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
        1,
    );
    let stream = first.config.stream;
    let mut config = first.config.clone();
    config.stream = ExchangeStreamIdentity::try_new(
        ExchangeSource::new(stream.source().task(), UniqueId::new(1, 99), 2),
        stream.destination(),
        stream.destination_fragment_instance_id(),
        stream.destination_node(),
    )
    .unwrap();
    let blocked = ExchangeStreamSender::try_new(
        config,
        transport.clone(),
        scheduler.clone(),
        Arc::new(Observable::new()),
    )
    .unwrap();
    let healthy = sender_in_query(
        transport.clone(),
        scheduler.clone(),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
        2,
    );
    publish(&first, b"one", false);
    publish(&blocked, b"12345678", false);
    publish(&healthy, b"x", false);
    assert_eq!(scheduler.snapshot().bytes, 4);
    assert_eq!(transport.queued(), 2);
    let first_call = transport.take();
    let healthy_call = transport.take();
    assert_eq!(healthy_call.frame.receiver.stream, healthy.config.stream);
    healthy_call.complete(1, 1);
    assert_eq!(transport.queued(), 0);
    first_call.complete(1, 1);
    let blocked_call = transport.take();
    assert_eq!(blocked_call.frame.receiver.stream, blocked.config.stream);
    blocked_call.complete(1, 1);
    first.stop();
    blocked.stop();
    healthy.stop();
}

#[test]
fn validated_remote_stop_proofs_avoid_redundant_stop_without_forging_activity_exit() {
    let proofs = [
        ExchangeStreamFacts {
            receiver_closed: Some(ExchangeUnitCloseDisposition::Withdrawn),
            ..Default::default()
        },
        ExchangeStreamFacts {
            sender_stopped: true,
            ..Default::default()
        },
        ExchangeStreamFacts {
            custody_through: 1,
            granted_through: 1,
            receiver_closed: Some(ExchangeUnitCloseDisposition::Consumed {
                final_sequence: ExchangeUnitSequence::try_new(2).unwrap(),
            }),
            ..Default::default()
        },
    ];
    for facts in proofs {
        let transport = Scripted::granting();
        let scheduler = scheduler(16);
        let sender = sender(
            transport.clone(),
            scheduler.clone(),
            1,
            if matches!(
                facts.receiver_closed,
                Some(ExchangeUnitCloseDisposition::Consumed { .. })
            ) {
                ExchangeDeliveryRequirement::MustDrain
            } else {
                ExchangeDeliveryRequirement::DemandDriven
            },
        );
        publish(&sender, b"one", true);
        let call = transport.take();
        transport
            .stop_replies
            .lock()
            .unwrap()
            .push_back(ExchangeDemandCallOutcome::Unknown(
                "redundant stop must not run".into(),
            ));
        (call.done)(ExchangeCallOutcome::Observed(ExchangeUnitReceipt {
            verdict: ExchangeAdmissionVerdict::Idempotent,
            facts,
        }));
        assert_eq!(
            transport.normal_closes.lock().unwrap().len(),
            usize::from(facts.receiver_closed.is_some()),
            "a stop acknowledgement alone is not a receiver close"
        );
        assert_eq!(
            sender.snapshot().stop_obligation,
            ExchangeSenderStopState::Confirmed
        );
        assert_eq!(sender.snapshot().active_calls, 1);
        assert_eq!(scheduler.snapshot().bytes, 3);
        assert_eq!(
            sender.snapshot().retention.consumed_through,
            if matches!(
                facts.receiver_closed,
                Some(ExchangeUnitCloseDisposition::Consumed { .. })
            ) {
                2
            } else {
                0
            },
        );
        // Later local cleanup must preserve the already validated proof too.
        sender.stop();
        assert_eq!(
            sender.snapshot().stop_obligation,
            ExchangeSenderStopState::Confirmed
        );
        assert!(
            transport
                .demands
                .lock()
                .unwrap()
                .iter()
                .all(|entry| entry.operation != ExchangeDemandOperation::StopSender)
        );
        assert!(transport.retries.lock().unwrap().is_empty());
        assert_eq!(sender.snapshot().active_calls, 1);
        drop(call.activity);
        assert_eq!(sender.snapshot().active_calls, 0);
        assert_eq!(scheduler.snapshot().calls, 0);
    }
}

#[test]
fn late_remote_stop_proof_confirms_unknown_stop_without_replaying_custody_or_retry() {
    for closed in [
        None,
        Some(ExchangeUnitCloseDisposition::Withdrawn),
        Some(ExchangeUnitCloseDisposition::Consumed {
            final_sequence: ExchangeUnitSequence::try_new(2).unwrap(),
        }),
    ] {
        let transport = Scripted::granting();
        let scheduler = scheduler(16);
        let sender = sender(
            transport.clone(),
            scheduler.clone(),
            1,
            if matches!(closed, Some(ExchangeUnitCloseDisposition::Consumed { .. })) {
                ExchangeDeliveryRequirement::MustDrain
            } else {
                ExchangeDeliveryRequirement::DemandDriven
            },
        );
        publish(&sender, b"one", true);
        let call = transport.take();
        transport
            .stop_replies
            .lock()
            .unwrap()
            .push_back(ExchangeDemandCallOutcome::Unknown("stop reply lost".into()));
        sender.stop();
        let stopped = sender.snapshot();
        assert_eq!(stopped.stop_obligation, ExchangeSenderStopState::Pending);
        assert_eq!(stopped.retention.consumed_through, 0);
        (call.done)(ExchangeCallOutcome::Observed(ExchangeUnitReceipt {
            verdict: ExchangeAdmissionVerdict::Idempotent,
            facts: ExchangeStreamFacts {
                custody_through: if matches!(
                    closed,
                    Some(ExchangeUnitCloseDisposition::Consumed { .. })
                ) {
                    2
                } else {
                    0
                },
                granted_through: 1,
                sender_stopped: closed.is_none(),
                receiver_closed: closed,
            },
        }));
        let confirmed = sender.snapshot();
        assert_eq!(
            confirmed.stop_obligation,
            ExchangeSenderStopState::Confirmed
        );
        assert_eq!(confirmed.status, ExchangeStreamSenderStatus::Stopped);
        assert_eq!(confirmed.retention, stopped.retention);
        assert_eq!(confirmed.active_calls, 1);
        assert_eq!(scheduler.snapshot().bytes, 3);
        assert_eq!(
            sender.publish(&Bytes::from_static(b"late"), false),
            Err(ExchangeUnitSenderError::Stopped)
        );
        transport.retry();
        assert_eq!(
            transport
                .demands
                .lock()
                .unwrap()
                .iter()
                .filter(|entry| entry.operation == ExchangeDemandOperation::StopSender)
                .count(),
            1
        );
        assert!(transport.retries.lock().unwrap().is_empty());
        drop(call.activity);
        assert_eq!(sender.snapshot().active_calls, 0);
        assert_eq!(scheduler.snapshot().calls, 0);
    }
}

fn invalid_stop_proofs() -> [ExchangeStreamFacts; 3] {
    [
        ExchangeStreamFacts {
            receiver_closed: Some(ExchangeUnitCloseDisposition::Withdrawn),
            ..Default::default()
        },
        ExchangeStreamFacts {
            custody_through: 1,
            granted_through: 1,
            receiver_closed: Some(ExchangeUnitCloseDisposition::Consumed {
                final_sequence: ExchangeUnitSequence::try_new(1).unwrap(),
            }),
            ..Default::default()
        },
        ExchangeStreamFacts {
            custody_through: 3,
            granted_through: 3,
            sender_stopped: true,
            ..Default::default()
        },
    ]
}

#[test]
fn invalid_late_stop_proofs_fail_without_confirming_or_reviving_the_sealed_stream() {
    for facts in invalid_stop_proofs() {
        let transport = Scripted::granting();
        let scheduler = scheduler(16);
        let sender = sender(
            transport.clone(),
            scheduler.clone(),
            1,
            ExchangeDeliveryRequirement::MustDrain,
        );
        publish(&sender, b"one", true);
        let call = transport.take();
        transport
            .stop_replies
            .lock()
            .unwrap()
            .push_back(ExchangeDemandCallOutcome::Unknown("stop reply lost".into()));
        sender.stop();
        let stopped = sender.snapshot();
        (call.done)(ExchangeCallOutcome::Observed(ExchangeUnitReceipt {
            verdict: ExchangeAdmissionVerdict::Idempotent,
            facts,
        }));
        assert_eq!(sender.failure(), Some(contract_failure()));
        assert_eq!(
            sender.snapshot().stop_obligation,
            ExchangeSenderStopState::Pending
        );
        assert_eq!(sender.snapshot().retention, stopped.retention);
        assert_eq!(sender.snapshot().active_calls, 1);
        assert_eq!(scheduler.snapshot().bytes, 3);
        drop(call.activity);
        assert_eq!(sender.snapshot().active_calls, 0);
    }
}

#[test]
fn stop_rpc_proofs_require_the_same_delivery_end_and_custody_validation() {
    for facts in invalid_stop_proofs() {
        let transport = Scripted::granting();
        let sender = sender(
            transport.clone(),
            scheduler(16),
            1,
            ExchangeDeliveryRequirement::MustDrain,
        );
        publish(&sender, b"one", true);
        let call = transport.take();
        transport
            .stop_replies
            .lock()
            .unwrap()
            .push_back(ExchangeDemandCallOutcome::Observed(vec![(
                sender.config.stream,
                Ok(facts),
            )]));
        sender.stop();
        assert_eq!(sender.failure(), Some(contract_failure()));
        assert!(transport.normal_closes.lock().unwrap().is_empty());
        assert_eq!(
            sender.snapshot().stop_obligation,
            ExchangeSenderStopState::Refused(contract_failure())
        );
        assert_eq!(sender.snapshot().retention.consumed_through, 0);
        assert_eq!(sender.snapshot().active_calls, 1);
        drop(call);
        assert_eq!(sender.snapshot().active_calls, 0);
    }
}

#[test]
fn invalid_active_stop_proofs_do_not_confirm_before_the_full_merge_commits() {
    for facts in invalid_stop_proofs() {
        let transport = Scripted::granting();
        let scheduler = scheduler(16);
        let sender = sender(
            transport.clone(),
            scheduler.clone(),
            1,
            ExchangeDeliveryRequirement::MustDrain,
        );
        publish(&sender, b"one", true);
        let call = transport.take();
        transport
            .stop_replies
            .lock()
            .unwrap()
            .push_back(ExchangeDemandCallOutcome::Unknown(
                "cleanup stop reply lost".into(),
            ));
        (call.done)(ExchangeCallOutcome::Observed(ExchangeUnitReceipt {
            verdict: ExchangeAdmissionVerdict::Idempotent,
            facts,
        }));
        assert_eq!(sender.failure(), Some(contract_failure()));
        assert_eq!(
            sender.snapshot().stop_obligation,
            ExchangeSenderStopState::Pending
        );
        assert_eq!(sender.snapshot().retention.consumed_through, 0);
        assert_eq!(sender.snapshot().retention.data_positions, 0);
        assert_eq!(sender.snapshot().active_calls, 1);
        assert_eq!(scheduler.snapshot().bytes, 3);
        drop(call.activity);
        assert_eq!(sender.snapshot().active_calls, 0);
    }
}

#[test]
fn stopping_an_uncontacted_stream_creates_no_remote_stop_obligation() {
    let transport = Scripted::granting();
    let sender = sender(
        transport.clone(),
        scheduler(16),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    sender.stop();
    sender.stop();
    assert_eq!(
        sender.snapshot().status,
        ExchangeStreamSenderStatus::Stopped
    );
    assert_eq!(
        sender.snapshot().stop_obligation,
        ExchangeSenderStopState::NotRequired
    );
    assert_eq!(sender.failure(), None);
    assert_eq!(
        sender.publish(&Bytes::from_static(b"late"), false),
        Err(ExchangeUnitSenderError::Stopped)
    );
    assert_eq!(sender.finish(), Err(ExchangeUnitSenderError::Stopped));
    assert!(transport.demands.lock().unwrap().is_empty());
    assert!(transport.watches.lock().unwrap().is_empty());
    assert!(transport.retries.lock().unwrap().is_empty());
    assert_eq!(transport.queued(), 0);
}

#[test]
fn reentrant_stop_after_local_publication_but_before_contact_creates_no_remote_debt() {
    for end_only in [false, true] {
        let transport = Scripted::granting();
        let scheduler = scheduler(16);
        let sender = sender(
            transport.clone(),
            scheduler.clone(),
            1,
            ExchangeDeliveryRequirement::DemandDriven,
        );
        let weak = Arc::downgrade(&sender);
        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_in_callback = stopped.clone();
        sender.observable().add_observer(Arc::new(move || {
            if !stopped_in_callback.swap(true, Ordering::AcqRel) {
                weak.upgrade().expect("publication owner is live").stop();
            }
        }));
        if end_only {
            assert_eq!(sender.finish().unwrap().end_sequence, Some(1));
        } else {
            assert_eq!(publish(&sender, b"one", true).end_sequence, Some(2));
        }
        assert!(stopped.load(Ordering::Acquire));
        let snapshot = sender.snapshot();
        assert_eq!(snapshot.status, ExchangeStreamSenderStatus::Stopped);
        assert_eq!(
            snapshot.stop_obligation,
            ExchangeSenderStopState::NotRequired
        );
        assert_eq!(snapshot.retention.offered_through, 0);
        assert_eq!(snapshot.retention.consumed_through, 0);
        assert!(snapshot.retention.sealed);
        assert_eq!(snapshot.retention.payload_bytes, 0);
        assert_eq!(snapshot.active_calls, 0);
        assert!(transport.demands.lock().unwrap().is_empty());
        assert!(transport.watches.lock().unwrap().is_empty());
        assert!(transport.retries.lock().unwrap().is_empty());
        assert_eq!(transport.queued(), 0);
        assert_eq!(scheduler.snapshot().calls, 0);
    }
}

#[test]
fn unknown_demand_contact_requires_stop_even_after_all_local_activity_is_idle() {
    let transport = Scripted::granting();
    let sender = sender(
        transport.clone(),
        scheduler(16),
        1,
        ExchangeDeliveryRequirement::DemandDriven,
    );
    *transport.normal_close_reentry.lock().unwrap() = Some(Arc::downgrade(&sender));
    transport
        .demand_replies
        .lock()
        .unwrap()
        .push_back(ExchangeDemandCallOutcome::Unknown(
            "first demand reply lost".into(),
        ));
    publish(&sender, b"one", false);
    assert_eq!(sender.snapshot().active_calls, 0);
    assert_eq!(sender.snapshot().retention.offered_through, 0);
    assert_eq!(transport.queued(), 0);
    transport
        .stop_replies
        .lock()
        .unwrap()
        .push_back(ExchangeDemandCallOutcome::Unknown("stop reply lost".into()));
    sender.stop();
    assert_eq!(
        sender.snapshot().stop_obligation,
        ExchangeSenderStopState::Pending
    );
    // The old demand retry cannot re-open a locally stopped stream. The stop
    // retry remains required because the first demand may have been accepted.
    transport.retry();
    assert_eq!(
        sender.snapshot().stop_obligation,
        ExchangeSenderStopState::Pending
    );
    let facts = ExchangeStreamFacts {
        receiver_closed: Some(ExchangeUnitCloseDisposition::Withdrawn),
        ..Default::default()
    };
    transport
        .stop_replies
        .lock()
        .unwrap()
        .push_back(ExchangeDemandCallOutcome::Observed(vec![(
            sender.config.stream,
            Ok(facts),
        )]));
    transport.retry();
    assert_eq!(
        sender.snapshot().stop_obligation,
        ExchangeSenderStopState::Confirmed
    );
    assert_eq!(
        *transport.normal_closes.lock().unwrap(),
        vec![ExchangeNormalCloseAccepted {
            stream: sender.config.stream,
            sender_count: sender.config.sender_count,
            facts,
        }]
    );
    let demands = transport.demands.lock().unwrap();
    assert_eq!(
        demands
            .iter()
            .filter(|entry| matches!(entry.operation, ExchangeDemandOperation::Register(_)))
            .count(),
        1
    );
    assert_eq!(
        demands
            .iter()
            .filter(|entry| entry.operation == ExchangeDemandOperation::StopSender)
            .count(),
        2
    );
    assert!(transport.retries.lock().unwrap().is_empty());
    assert_eq!(transport.queued(), 0);
}
