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
use novarocks_execution::runtime::exchange::{
    ExchangeFailure, ExchangeReceiverPin, ExchangeReceiverSnapshot,
};
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::runtime::task::io::{
    ExchangeReceiverKey, ExchangeReceiverRegistration, UnavailableExchangeReceiverPort,
};
use novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitSequence;
use novarocks_execution_contract::{ExchangeSource, FragmentNodeId};
use novarocks_types::UniqueId;
use tokio_stream::StreamExt;

#[derive(Default)]
struct TestPort {
    states: Mutex<BTreeMap<ExchangeStreamIdentity, ExchangeStreamFacts>>,
    listener: Mutex<Option<Arc<dyn ExchangeUnitStateListener>>>,
    deliveries: AtomicU64,
    delivery_hold: Mutex<Option<Arc<DeliveryHold>>>,
}

#[derive(Default)]
struct DeliveryHold {
    entered: Notify,
    released: Mutex<bool>,
    changed: std::sync::Condvar,
}
impl DeliveryHold {
    fn block(&self) {
        self.entered.notify_one();
        let (released, _) = self
            .changed
            .wait_timeout_while(
                self.released.lock().unwrap(),
                Duration::from_secs(8),
                |released| !*released,
            )
            .unwrap();
        assert!(*released, "test decoder hold exceeded its failure watchdog");
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

impl TestPort {
    fn publish(&self, stream: ExchangeStreamIdentity, facts: ExchangeStreamFacts) {
        self.states.lock().unwrap().insert(stream, facts);
        self.observe(stream, facts);
    }
    fn observe(&self, stream: ExchangeStreamIdentity, facts: ExchangeStreamFacts) {
        let listener = self.listener.lock().unwrap().clone();
        if let Some(listener) = listener {
            listener.changed(ExchangeStreamObservation { stream, facts });
        }
    }
    fn retire(&self, stream: ExchangeStreamIdentity, facts: ExchangeStreamFacts) {
        self.states.lock().unwrap().remove(&stream);
        self.observe(stream, facts);
    }
}

impl ExchangeReceiverPort for TestPort {
    fn bind_unit_receiver(
        &self,
        key: ExchangeReceiverKey,
        streams: Vec<ExchangeStreamIdentity>,
        control: Arc<novarocks_execution::runtime::runtime_state::RuntimeErrorState>,
        grantable: u64,
    ) -> Result<(), ExchangeFailure> {
        UnavailableExchangeReceiverPort.bind_unit_receiver(key, streams, control, grantable)
    }

    fn register_unit_demand(
        &self,
        _: ExchangeReceiverKey,
        entry: novarocks_execution::runtime::task::io::exchange_unit::ExchangeDemandEntry,
    ) -> Result<ExchangeStreamObservation, FailureFact> {
        use novarocks_execution::runtime::task::io::exchange_unit::ExchangeDemandOperation;
        let facts = {
            let mut states = self.states.lock().unwrap();
            let facts = states.entry(entry.stream).or_default();
            match entry.operation {
                ExchangeDemandOperation::Register(demand) if !facts.sender_stopped => {
                    facts.granted_through = facts.granted_through.max(demand.sequence.get())
                }
                ExchangeDemandOperation::Register(_) => {}
                ExchangeDemandOperation::StopSender => facts.sender_stopped = true,
            }
            *facts
        };
        self.observe(entry.stream, facts);
        Ok(ExchangeStreamObservation {
            stream: entry.stream,
            facts,
        })
    }
    fn receive_unit(
        &self,
        _: ExchangeReceiverKey,
        frame: novarocks_execution::runtime::task::io::exchange_unit::ExchangeUnitReceiverFrame,
    ) -> Result<
        novarocks_execution::runtime::task::io::exchange_unit::ExchangeUnitReceipt,
        FailureFact,
    > {
        assert!(
            frame.next_demand.is_none(),
            "piggyback is registered once by the adapter"
        );
        self.deliveries.fetch_add(1, Ordering::Relaxed);
        let hold = self.delivery_hold.lock().unwrap().clone();
        if let Some(hold) = hold {
            hold.block();
        }
        let facts = {
            let mut states = self.states.lock().unwrap();
            let facts = states.entry(frame.stream).or_default();
            facts.custody_through = frame.item.sequence().get();
            *facts
        };
        Ok(novarocks_execution::runtime::task::io::exchange_unit::ExchangeUnitReceipt {
            verdict: novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeAdmissionVerdict::Applied, facts,
        })
    }
    fn unit_sender_states(
        &self,
        sender: BackendProcessId,
        after: Option<ExchangeStreamIdentity>,
        limit: usize,
    ) -> Result<Vec<ExchangeStreamObservation>, FailureFact> {
        Ok(self
            .states
            .lock()
            .unwrap()
            .iter()
            .filter(|(stream, _)| {
                stream.source().task().backend_process_id() == sender
                    && after.is_none_or(|after| **stream > after)
            })
            .take(limit)
            .map(|(stream, facts)| ExchangeStreamObservation {
                stream: *stream,
                facts: *facts,
            })
            .collect())
    }
    fn set_unit_state_listener(
        &self,
        listener: Arc<dyn ExchangeUnitStateListener>,
    ) -> Result<(), FailureFact> {
        *self.listener.lock().unwrap() = Some(listener);
        Ok(())
    }
    fn register(&self, registration: ExchangeReceiverRegistration) -> Result<(), ExchangeFailure> {
        UnavailableExchangeReceiverPort.register(registration)
    }

    fn cancel(&self, _: ExchangeReceiverKey) {}
    fn remove(&self, _: ExchangeReceiverKey) {}
    fn cancel_task(&self, _: UniqueId) {}
    fn receiver_pin(
        &self,
        key: ExchangeReceiverKey,
        count: usize,
    ) -> Result<ExchangeReceiverPin, ExchangeFailure> {
        UnavailableExchangeReceiverPort.receiver_pin(key, count)
    }
    fn stream_close_facts(
        &self,
        key: ExchangeReceiverKey,
        streams: &[ExchangeStreamIdentity],
    ) -> Result<Vec<novarocks_execution_contract::ExchangeStreamClose>, ExchangeFailure> {
        // This subscription script has no actual consuming input and therefore
        // cannot author a close disposition from its observation-only states.
        UnavailableExchangeReceiverPort.stream_close_facts(key, streams)
    }
    fn ensure_mem_tracker(
        &self,
        key: ExchangeReceiverKey,
        root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, ExchangeFailure> {
        UnavailableExchangeReceiverPort.ensure_mem_tracker(key, root)
    }

    fn snapshot(&self, _: ExchangeReceiverKey) -> Option<ExchangeReceiverSnapshot> {
        None
    }
}

fn base_stream() -> ExchangeStreamIdentity {
    crate::exchange_transmitter::tests::unit_frame()
        .receiver
        .stream
}
fn sibling(base: ExchangeStreamIdentity, node: usize) -> ExchangeStreamIdentity {
    ExchangeStreamIdentity::try_new(
        ExchangeSource::new(
            base.source().task(),
            base.source().task_key(),
            base.source().sender_ordinal(),
        ),
        base.destination(),
        base.destination_task_key(),
        FragmentNodeId::new(node as i32),
    )
    .unwrap()
}
fn facts(c: u64, g: u64) -> ExchangeStreamFacts {
    ExchangeStreamFacts {
        custody_through: c,
        granted_through: g,
        ..Default::default()
    }
}
fn subscription(
    stream: ExchangeStreamIdentity,
    generation: u64,
) -> codec::DecodedExchangeGrantSubscription {
    codec::DecodedExchangeGrantSubscription {
        pair: ExchangeProcessPair::for_stream(stream),
        generation: NonZeroU64::new(generation).unwrap(),
    }
}
fn peer(stream: ExchangeStreamIdentity) -> Arc<ClientPeer> {
    Arc::new(ClientPeer {
        destination: RuntimeEndpoint::new("127.0.0.1", 9090).unwrap(),
        pair: ExchangeProcessPair::for_stream(stream),
        watches: Mutex::default(),
        revision: AtomicU64::new(0),
        generation: AtomicU64::new(2),
        changed: Notify::new(),
        stop: CancellationToken::new(),
    })
}

#[tokio::test]
async fn snapshot_pages_complete_before_retirement_and_coalesced_live_deltas() {
    let base = base_stream();
    let port = Arc::new(TestPort::default());
    for node in 0..PAGE + 3 {
        port.states
            .lock()
            .unwrap()
            .insert(sibling(base, node), facts(0, 1));
    }
    let publisher = ExchangeGrantPublisher::install(port.clone()).unwrap();
    let mut stream = publisher.subscribe(subscription(base, 1), None).unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert!(first.snapshot && !first.snapshot_complete);
    assert_eq!(first.states.len(), PAGE);
    let retired = sibling(base, 0);
    let terminal = ExchangeStreamFacts {
        sender_stopped: true,
        receiver_closed: Some(ExchangeUnitCloseDisposition::Withdrawn),
        ..facts(1, 1)
    };
    port.retire(retired, terminal);
    port.observe(retired, facts(0, 1));
    let second = stream.next().await.unwrap().unwrap();
    assert!(second.snapshot && !second.snapshot_complete);
    assert_eq!(second.states.len(), 3);
    let complete = stream.next().await.unwrap().unwrap();
    assert!(complete.snapshot && complete.snapshot_complete && complete.states.is_empty());
    let delta = stream.next().await.unwrap().unwrap();
    assert!(!delta.snapshot && !delta.snapshot_complete);
    let decoded =
        codec::decode_grant_batch(&delta, subscription(base, 1), FieldPath::root("test")).unwrap();
    assert_eq!(decoded.states.len(), 1);
    assert_eq!(decoded.states[0].stream, retired);
    assert_eq!(decoded.states[0].facts, terminal);
}

#[tokio::test]
async fn higher_generation_stops_old_stream_and_delayed_lower_cannot_replace_it() {
    let base = base_stream();
    let port = Arc::new(TestPort::default());
    port.states.lock().unwrap().insert(base, facts(0, 1));
    let publisher = ExchangeGrantPublisher::install(port.clone()).unwrap();
    let mut old = publisher.subscribe(subscription(base, 4), None).unwrap();
    let mut current = publisher.subscribe(subscription(base, 6), None).unwrap();
    for generation in [4, 5, 6] {
        assert!(
            publisher
                .subscribe(subscription(base, generation), None)
                .is_err()
        );
    }
    assert_eq!(
        old.next().await.unwrap().unwrap_err().code(),
        Code::Cancelled
    );
    assert!(old.next().await.is_none());
    port.publish(base, facts(1, 2));
    assert_eq!(current.next().await.unwrap().unwrap().generation, 6);
    drop(current);
    assert!(publisher.subscribe(subscription(base, 5), None).is_err());
    assert!(publisher.subscribe(subscription(base, 7), None).is_ok());
}

#[tokio::test]
async fn pending_facts_survive_a_slow_subscriber_and_retirement_without_rebuild() {
    let base = base_stream();
    let port = Arc::new(TestPort::default());
    let publisher = ExchangeGrantPublisher::install(port.clone()).unwrap();
    let mut stream = publisher.subscribe(subscription(base, 1), None).unwrap();
    // Cross the former 8192-entry quota with one coalesced fact per stream.
    let stream_count = 8193;
    for node in 0..stream_count {
        port.observe(sibling(base, node), facts(0, 1));
    }
    let retired = sibling(base, stream_count - 1);
    let terminal = ExchangeStreamFacts {
        sender_stopped: true,
        receiver_closed: Some(ExchangeUnitCloseDisposition::Consumed {
            final_sequence: ExchangeUnitSequence::try_new(2).unwrap(),
        }),
        ..facts(2, 1)
    };
    port.retire(retired, terminal);
    port.observe(retired, facts(0, 1));
    {
        let pending = stream.feed.pending.lock().unwrap();
        assert_eq!(pending.facts.len(), stream_count);
        assert!(pending.closed.is_none());
    }
    let snapshot = stream.next().await.unwrap().unwrap();
    assert!(snapshot.snapshot && snapshot.snapshot_complete && snapshot.states.is_empty());
    let mut delivered = BTreeMap::new();
    while delivered.len() < stream_count {
        let delta = stream.next().await.unwrap().unwrap();
        assert!(!delta.snapshot && !delta.snapshot_complete);
        assert!(delta.states.len() <= PAGE);
        let decoded =
            codec::decode_grant_batch(&delta, subscription(base, 1), FieldPath::root("test"))
                .unwrap();
        for state in decoded.states {
            assert!(delivered.insert(state.stream, state.facts).is_none());
        }
    }
    assert_eq!(delivered[&retired], terminal);
    assert!(stream.feed.pending.lock().unwrap().facts.is_empty());
}

#[tokio::test]
async fn delta_pages_rotate_past_continuously_refreshed_lower_streams() {
    let base = base_stream();
    let port = Arc::new(TestPort::default());
    let publisher = ExchangeGrantPublisher::install(port.clone()).unwrap();
    let mut stream = publisher.subscribe(subscription(base, 1), None).unwrap();
    let retired = sibling(base, PAGE);
    let terminal = ExchangeStreamFacts {
        sender_stopped: true,
        receiver_closed: Some(ExchangeUnitCloseDisposition::Consumed {
            final_sequence: ExchangeUnitSequence::try_new(2).unwrap(),
        }),
        ..facts(2, 1)
    };
    port.retire(retired, terminal);
    let snapshot = stream.next().await.unwrap().unwrap();
    assert!(snapshot.snapshot && snapshot.snapshot_complete);
    let mut delivered_terminal = false;
    for round in 0..4 {
        // Refill a whole page before every poll. Starting from the smallest
        // key each time would leave the retired higher-key stream forever.
        for node in 0..PAGE {
            port.observe(sibling(base, node), facts(round, round + 1));
        }
        let delta = stream.next().await.unwrap().unwrap();
        assert!(!delta.snapshot);
        let decoded =
            codec::decode_grant_batch(&delta, subscription(base, 1), FieldPath::root("test"))
                .unwrap();
        if let Some(state) = decoded.states.iter().find(|state| state.stream == retired) {
            assert_eq!(state.facts, terminal);
            delivered_terminal = true;
        }
        if round == 1 {
            assert!(delivered_terminal);
        }
    }
    assert!(delivered_terminal);
    assert!(stream.feed.pending.lock().unwrap().closed.is_none());
}

#[test]
fn observation_peer_metadata_uses_lifecycles_without_a_second_peer_quota() {
    let base = base_stream();
    let port = Arc::new(TestPort::default());
    let publisher = ExchangeGrantPublisher::install(port).unwrap();
    // More than the former observation-layer limit; physical connection
    // admission belongs to the existing Native transport owner.
    let feeds = (0..33)
        .map(|_| {
            let mut request = subscription(base, 1);
            request.pair.sender = BackendProcessId::new_v7();
            publisher.subscribe(request, None).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(publisher.peers.lock().unwrap().len(), feeds.len());
    drop(feeds);
    publisher.reclaim_inactive_peers();
    assert!(publisher.peers.lock().unwrap().is_empty());
}

#[test]
fn out_of_order_full_observations_merge_watermarks_and_sticky_termination() {
    let terminal = ExchangeStreamFacts {
        sender_stopped: true,
        receiver_closed: Some(ExchangeUnitCloseDisposition::Consumed {
            final_sequence: ExchangeUnitSequence::try_new(4).unwrap(),
        }),
        ..facts(4, 3)
    };
    assert_eq!(merge_facts(terminal, facts(2, 3)).unwrap(), terminal);
    assert_eq!(merge_facts(facts(2, 2), facts(1, 3)).unwrap(), facts(2, 3));
    assert!(merge_facts(terminal, facts(3, 4)).is_err());
    assert!(
        merge_facts(
            terminal,
            ExchangeStreamFacts {
                receiver_closed: Some(ExchangeUnitCloseDisposition::Withdrawn),
                ..facts(4, 3)
            }
        )
        .is_err()
    );
    assert!(
        merge_facts(
            facts(0, 0),
            ExchangeStreamFacts {
                receiver_closed: Some(ExchangeUnitCloseDisposition::Consumed {
                    final_sequence: ExchangeUnitSequence::try_new(u64::MAX).unwrap()
                }),
                ..facts(u64::MAX, u64::MAX)
            }
        )
        .is_err()
    );
}

#[tokio::test]
async fn contradictory_final_change_is_typed_refusal_not_reconnect() {
    let base = base_stream();
    let port = Arc::new(TestPort::default());
    let publisher = ExchangeGrantPublisher::install(port.clone()).unwrap();
    let mut stream = publisher.subscribe(subscription(base, 1), None).unwrap();
    port.observe(
        base,
        ExchangeStreamFacts {
            receiver_closed: Some(ExchangeUnitCloseDisposition::Consumed {
                final_sequence: ExchangeUnitSequence::try_new(2).unwrap(),
            }),
            ..facts(2, 1)
        },
    );
    port.observe(base, facts(2, 2));
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(
        status_failure(&error),
        Some(subscription_contract_failure())
    );
}

#[test]
fn snapshot_absence_resynchronizes_without_synthesizing_facts_and_callbacks_are_unlocked() {
    let base = base_stream();
    let peer = peer(base);
    let notices = Arc::new(Mutex::new(Vec::new()));
    let weak = Arc::downgrade(&peer);
    let captured = notices.clone();
    let wake: ExchangeGrantWake = Arc::new(move |notice| {
        assert!(weak.upgrade().unwrap().watches.try_lock().is_ok());
        captured.lock().unwrap().push(notice);
    });
    peer.watches.lock().unwrap().entries.insert(
        base,
        Watcher {
            wake,
            last: Some(facts(1, 2)),
            refused: None,
        },
    );
    peer.resynchronize_missing(NonZeroU64::new(2).unwrap(), &BTreeSet::new());
    assert!(matches!(
        notices.lock().unwrap().as_slice(),
        [ExchangeGrantNotice::Resynchronize]
    ));
    assert_eq!(
        peer.watches.lock().unwrap().entries[&base].last,
        Some(facts(1, 2))
    );
    peer.apply(
        NonZeroU64::new(1).unwrap(),
        vec![codec::DecodedExchangeStreamState {
            stream: base,
            facts: facts(2, 3),
        }],
    );
    assert_eq!(notices.lock().unwrap().len(), 1);
    peer.apply(
        NonZeroU64::new(2).unwrap(),
        vec![codec::DecodedExchangeStreamState {
            stream: base,
            facts: facts(2, 3),
        }],
    );
    assert_eq!(notices.lock().unwrap().len(), 2);
    peer.stop.cancel();
    peer.apply(
        NonZeroU64::new(2).unwrap(),
        vec![codec::DecodedExchangeStreamState {
            stream: base,
            facts: facts(3, 4),
        }],
    );
    peer.resynchronize_missing(NonZeroU64::new(2).unwrap(), &BTreeSet::new());
    assert_eq!(notices.lock().unwrap().len(), 2);
}

#[test]
fn well_formed_dataloss_details_preserve_remote_failure_without_a_second_byte_limit() {
    let fact = exchange_failure(FailureClass::Unsupported, 461);
    assert_eq!(status_failure(&failure_status(fact)), Some(fact));
    assert!(
        status_failure(&Status::with_details(
            Code::Unavailable,
            "network",
            Bytes::from_static(b"invalid")
        ))
        .is_none()
    );
    assert!(
        status_failure(&Status::with_details(
            Code::DataLoss,
            "truncated",
            Bytes::from_static(b"invalid")
        ))
        .is_none()
    );
    let mut details = novarocks_task_codec::status::encode_failure_fact(fact).encode_to_vec();
    // Unknown protobuf fields remain governed by the existing Native metadata
    // limit, rather than an additional observation-layer 4096-byte quota.
    prost::encoding::encode_key(
        127,
        prost::encoding::WireType::LengthDelimited,
        &mut details,
    );
    prost::encoding::encode_varint(4097, &mut details);
    details.resize(details.len() + 4097, 0);
    assert_eq!(
        status_failure(&Status::with_details(
            Code::DataLoss,
            "remote failure",
            Bytes::from(details)
        )),
        Some(fact)
    );
}

fn installed_plane(
    stream: ExchangeStreamIdentity,
    port: Arc<TestPort>,
) -> (
    crate::exchange_data_plane::NativeExchangeDataPlane,
    Arc<novarocks_worker::TaskInboundCapabilities>,
) {
    use novarocks_execution_contract::{
        ExchangeDeliveryRequirement, ExchangeInbound, ExchangeTopology, TaskDescriptor,
    };
    let topology = ExchangeTopology::try_new(
        Vec::new(),
        vec![
            ExchangeInbound::try_new(
                stream.destination_node(),
                vec![stream.source()],
                ExchangeDeliveryRequirement::DemandDriven,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let descriptor = TaskDescriptor::try_new(
        stream.destination(),
        stream.destination_task_key(),
        std::num::NonZeroUsize::new(1).unwrap(),
        Vec::new(),
        topology,
    )
    .unwrap();
    let capabilities = novarocks_worker::TaskInboundCapabilities::new();
    capabilities.install(Arc::new(descriptor)).unwrap();
    (
        crate::exchange_data_plane::NativeExchangeDataPlane::new(
            port,
            capabilities.clone(),
            stream.destination().backend_process_id(),
        ),
        capabilities,
    )
}

#[test]
fn worker_exact_route_authorizes_unit_piggyback_demand_and_stop_roundtrips() {
    use novarocks_execution_contract::ordered_retained_stream::exchange::{
        ExchangeUnitDemand, ExchangeUnitItem,
    };
    let base = base_stream();
    let stream = ExchangeStreamIdentity::try_new(
        ExchangeSource::new(base.source().task(), base.source().task_key(), 0),
        base.destination(),
        base.destination_task_key(),
        base.destination_node(),
    )
    .unwrap();
    let port = Arc::new(TestPort::default());
    let (plane, _capabilities) = installed_plane(stream, port.clone());
    let sender = stream.source().task().backend_process_id();
    let first = ExchangeUnitDemand {
        sequence: ExchangeUnitSequence::try_new(1).unwrap(),
        wire_bytes: 3,
    };
    let next = ExchangeUnitDemand {
        sequence: ExchangeUnitSequence::try_new(2).unwrap(),
        wire_bytes: 5,
    };
    let registration = codec::DecodedExchangeDemandEntry {
        stream,
        operation: codec::ExchangeDemandOperation::Demand(first),
    };
    let response = plane
        .register_unit_demands(
            codec::encode_demand_request(&[registration], FieldPath::root("test")).unwrap(),
            sender,
        )
        .unwrap();
    assert_eq!(
        codec::decode_demand_response(&response, &[stream], FieldPath::root("test")).unwrap()[0]
            .outcome,
        Ok(facts(0, 1))
    );
    let response = plane
        .deliver_unit(
            codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {
                stream,
                sender_count: 1,
                sender_id: 0,
                backend_number: 0,
                item: ExchangeUnitItem::Unit {
                    demand: first,
                    end_after_unit: false,
                },
                payload: Bytes::from_static(b"abc"),
                next_demand: Some(next),
            }),
            sender,
        )
        .unwrap();
    assert!(
        matches!(codec::decode_delivery_response(&response, stream, first.sequence, FieldPath::root("test")).unwrap(), codec::DecodedExchangeDeliveryOutcome::Observed { facts: observed, .. } if observed == facts(1, 2))
    );
    assert_eq!(port.deliveries.load(Ordering::Relaxed), 1);
    let stop = codec::DecodedExchangeDemandEntry {
        stream,
        operation: codec::ExchangeDemandOperation::StopSender,
    };
    plane
        .register_unit_demands(
            codec::encode_demand_request(&[stop], FieldPath::root("test")).unwrap(),
            sender,
        )
        .unwrap();
    plane
        .register_unit_demands(
            codec::encode_demand_request(&[registration], FieldPath::root("test")).unwrap(),
            sender,
        )
        .unwrap();
    assert!(port.states.lock().unwrap()[&stream].sender_stopped);
    // Another destination node is not in the Worker's frozen inbound roster.
    let absent = sibling(stream, 99);
    let entry = codec::DecodedExchangeDemandEntry {
        stream: absent,
        operation: codec::ExchangeDemandOperation::StopSender,
    };
    let response = plane
        .register_unit_demands(
            codec::encode_demand_request(&[entry], FieldPath::root("test")).unwrap(),
            sender,
        )
        .unwrap();
    assert!(
        codec::decode_demand_response(&response, &[absent], FieldPath::root("test")).unwrap()[0]
            .outcome
            .is_err()
    );
    assert!(!port.states.lock().unwrap().contains_key(&absent));
}

#[test]
fn subscription_validates_authenticated_pair_zero_generation_and_missing_receiver_owner() {
    let base = base_stream();
    let sender = base.source().task().backend_process_id();
    let port = Arc::new(TestPort::default());
    let plane = crate::exchange_data_plane::NativeExchangeDataPlane::new(
        port,
        novarocks_worker::TaskInboundCapabilities::new(),
        base.destination().backend_process_id(),
    );
    let request = codec::encode_grant_subscription(subscription(base, 1));
    assert_eq!(
        plane
            .subscribe_unit_grants(request.clone(), BackendProcessId::new_v7(), None)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    let mut zero = request.clone();
    zero.generation = 0;
    assert_eq!(
        plane
            .subscribe_unit_grants(zero, sender, None)
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
    let unavailable = crate::exchange_data_plane::NativeExchangeDataPlane::new(
        Arc::new(UnavailableExchangeReceiverPort),
        novarocks_worker::TaskInboundCapabilities::new(),
        base.destination().backend_process_id(),
    );
    let error = unavailable
        .subscribe_unit_grants(request, sender, None)
        .err()
        .unwrap();
    let expected = UnavailableExchangeReceiverPort
        .unit_sender_states(sender, None, 1)
        .unwrap_err();
    assert_eq!(status_failure(&error), Some(expected));
}

#[test]
fn caller_projection_requires_verified_backend_extension() {
    use novarocks_native_trust::{
        DeploymentId, NativeCallerSubject, NativeProcessIdentity, NativeServerAdmission,
        NativeTransportMode, NativeTrust, ValidatedSharedSecret,
    };
    let process = BackendProcessId::new_v7();
    let trust = NativeTrust::new(
        DeploymentId::parse("unit-test").unwrap(),
        ValidatedSharedSecret::new(novarocks_secret::SecretValue::new(
            "0123456789abcdef0123456789abcdef",
        ))
        .unwrap(),
        NativeCallerSubject::parse("be@127.0.0.1:9090").unwrap(),
        NativeTransportMode::Disabled,
    );
    trust
        .bind_process_identity(NativeProcessIdentity::Backend(process))
        .unwrap();
    let mut request = tonic::Request::new(());
    assert_eq!(
        crate::exchange_data_plane::authenticated_unit_sender(&request)
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    trust
        .apply_client_authorization(request.metadata_mut())
        .unwrap();
    let caller = NativeServerAdmission::new(trust)
        .admit_metadata(request.metadata())
        .unwrap();
    request.extensions_mut().insert(caller);
    assert_eq!(
        crate::exchange_data_plane::authenticated_unit_sender(&request).unwrap(),
        process
    );
}

struct UnusedIngress;
#[tonic::async_trait]
impl crate::task_protocol::TaskExecutionIngress for UnusedIngress {
    #[cfg(novarocks_dev_controls)]
    fn with_registry_lock_for_test(
        &self,
        _: &mut dyn FnMut() -> Result<(), Status>,
    ) -> Result<(), Status> {
        unreachable!()
    }
    async fn apply_task_operations(
        &self,
        _: wire::ApplyTaskOperationsRequest,
    ) -> Result<wire::ApplyTaskOperationsResponse, Status> {
        unreachable!()
    }
    async fn apply_task_operations_at(
        &self,
        _: wire::ApplyTaskOperationsRequest,
        _: crate::task_protocol::TaskIngressTiming,
        _: Arc<NativeIngressOwnership>,
    ) -> Result<wire::ApplyTaskOperationsResponse, Status> {
        unreachable!()
    }
    async fn apply_task_control_operations(
        &self,
        _: wire::ApplyTaskControlOperationsRequest,
    ) -> Result<wire::ApplyTaskOperationsResponse, Status> {
        unreachable!()
    }
    async fn apply_task_control_operations_at(
        &self,
        _: wire::ApplyTaskControlOperationsRequest,
        _: crate::task_protocol::TaskIngressTiming,
        _: Arc<NativeIngressOwnership>,
    ) -> Result<wire::ApplyTaskOperationsResponse, Status> {
        unreachable!()
    }
    fn subscribe_task_status(
        &self,
        _: wire::SubscribeTaskStatusRequest,
        _: std::time::Instant,
        _: Arc<NativeIngressOwnership>,
    ) -> Result<crate::task_protocol::TaskStatusEventStream, Status> {
        unreachable!()
    }
    async fn fetch_task_dynamic_filters(
        &self,
        _: wire::FetchTaskDynamicFiltersRequest,
        _: std::time::Instant,
    ) -> Result<wire::FetchTaskDynamicFiltersResponse, Status> {
        unreachable!()
    }
    async fn get_final_task_info(
        &self,
        _: wire::GetFinalTaskInfoRequest,
        _: std::time::Instant,
    ) -> Result<wire::GetFinalTaskInfoResponse, Status> {
        unreachable!()
    }
}
impl crate::catalog_prune_rpc::CatalogReachabilityAuthority for UnusedIngress {
    fn prune_unreachable_catalogs(
        &self,
        _: BTreeSet<novarocks_spi::connector::CatalogHandle>,
    ) -> novarocks_worker::CatalogPruneResult {
        unreachable!()
    }
}
impl crate::runtime_filter_rpc::BackendRuntimeFilterEnvelopeIngress for UnusedIngress {
    fn accept(
        &self,
        _: crate::runtime_filter_rpc::BackendNativeRuntimeFilterEnvelope,
    ) -> novarocks_worker::runtime_filter::domain::BackendIngressResult {
        unreachable!()
    }
}
impl novarocks_worker::WorkerAdmissionEpochAuthority for UnusedIngress {
    fn admission_epoch_capability(&self) -> novarocks_execution_contract::AdmissionEpochCapability {
        novarocks_execution_contract::AdmissionEpochCapability::try_from_bytes([1; 16]).unwrap()
    }
}

#[tokio::test]
async fn authenticated_tonic_ingress_backend_service_roundtrips_all_three_unit_methods() {
    use crate::native_transport_admission::{NativeTransportAdmission, TransportClass};
    use novarocks_execution_contract::ordered_retained_stream::exchange::{
        ExchangeUnitDemand, ExchangeUnitItem,
    };
    use novarocks_native_trust::{NativeIncomingAdapter, NativeProcessIdentity};
    use novarocks_proto_codec::native_rpc::NativeEndpointDomain;
    let base = base_stream();
    let stream = ExchangeStreamIdentity::try_new(
        ExchangeSource::new(base.source().task(), base.source().task_key(), 0),
        base.destination(),
        base.destination_task_key(),
        base.destination_node(),
    )
    .unwrap();
    let port = Arc::new(TestPort::default());
    let (_, capabilities) = installed_plane(stream, port.clone());
    let trust = crate::backend_test_support::test_backend_native_trust();
    trust
        .bind_process_identity(NativeProcessIdentity::Backend(
            stream.source().task().backend_process_id(),
        ))
        .unwrap();
    let receiver_trust = crate::backend_test_support::test_backend_native_trust();
    receiver_trust
        .bind_process_identity(NativeProcessIdentity::Backend(
            stream.destination().backend_process_id(),
        ))
        .unwrap();
    let descriptor = novarocks_execution_contract::BackendProcessDescriptor::try_new(
        stream.destination().backend_process_id(),
        RuntimeEndpoint::new("127.0.0.1", 9090).unwrap(),
        RuntimeEndpoint::new("127.0.0.1", 9091).unwrap(),
        "test",
        "test",
        novarocks_types::NativeCompatibilityId::new([1; 32]),
        1,
    )
    .unwrap();
    let service = crate::backend_rpc_service::BackendRpcService::new(
        Arc::new(UnusedIngress),
        Arc::new(UnusedIngress),
        Arc::new(UnusedIngress),
        port.clone(),
        capabilities.clone(),
        crate::backend_heartbeat::BackendHeartbeatResponder::new(
            descriptor,
            Arc::new(novarocks_worker::WorkerDrainState::new()),
            Arc::new(UnusedIngress),
        ),
    );
    let mut server = crate::NativeRpcListener::start_with_admission(
        "127.0.0.1",
        0,
        service,
        receiver_trust,
        NativeIncomingAdapter::plaintext(),
        "exchange-unit-test",
        NativeEndpointDomain::BackendData,
        "exchange-unit-test",
        || {},
        || {},
        crate::native_server::NativeIngressConfig {
            worker_threads: 2,
            ..Default::default()
        },
        NativeTransportAdmission::new().unwrap(),
        TransportClass::Data,
        None,
    )
    .unwrap();
    let runtime = BackendDataRuntime::new(
        tokio::runtime::Handle::current(),
        trust,
        crate::BackendNativeTransport::Plaintext,
    );
    let client = NativeRpcClient::new_backend_endpoint(
        runtime.clone(),
        RuntimeEndpoint::from_socket_addr(server.bound_addr())
            .native_endpoint()
            .clone(),
        stream.destination().backend_process_id(),
    );
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        let first = ExchangeUnitDemand { sequence: ExchangeUnitSequence::try_new(1).unwrap(), wire_bytes: 3 };
        let entry = codec::DecodedExchangeDemandEntry { stream, operation: codec::ExchangeDemandOperation::Demand(first) };
        let reply = client.register_exchange_demand_async(codec::encode_demand_request(&[entry], FieldPath::root("test")).unwrap(), Duration::from_secs(2), CancellationToken::new()).await.unwrap();
        assert_eq!(codec::decode_demand_response(&reply, &[stream], FieldPath::root("test")).unwrap()[0].outcome, Ok(facts(0, 1)));
        let reply = client.deliver_exchange_unit_async(codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {
            stream, sender_count: 1, sender_id: 0, backend_number: 0, item: ExchangeUnitItem::Unit { demand: first, end_after_unit: false }, payload: Bytes::from_static(b"abc"), next_demand: None,
        }), Duration::from_secs(2), CancellationToken::new()).await.unwrap();
        assert!(matches!(codec::decode_delivery_response(&reply, stream, first.sequence, FieldPath::root("test")).unwrap(), codec::DecodedExchangeDeliveryOutcome::Observed { facts: state, .. } if state == facts(1, 1)));
        let subscription = subscription(stream, 1);
        let mut live = client.subscribe_exchange_grants_async(codec::encode_grant_subscription(subscription), Duration::from_secs(2), CancellationToken::new()).await.unwrap();
        let page = live.message().await.unwrap().unwrap();
        assert_eq!(codec::decode_grant_batch(&page, subscription, FieldPath::root("test")).unwrap().states[0].facts, facts(1, 1));
        assert!(live.message().await.unwrap().unwrap().snapshot_complete);
        port.publish(stream, ExchangeStreamFacts { sender_stopped: true, ..facts(1, 1) });
        let delta = live.message().await.unwrap().unwrap();
        assert!(codec::decode_grant_batch(&delta, subscription, FieldPath::root("test")).unwrap().states[0].facts.sender_stopped);
        drop(live);
        // The owner counter survives removing the last watch. Each reopened
        // observation begins with an authoritative snapshot on the same peer.
        let clients = ExchangeGrantClients::new(runtime.clone(), Duration::from_secs(2));
        let mut prior = 0;
        for _ in 0..2 {
            let (notice_tx, notice_rx) = tokio::sync::oneshot::channel();
            let notice_tx = Mutex::new(Some(notice_tx));
            clients.watch(RuntimeEndpoint::from_socket_addr(server.bound_addr()), stream, Arc::new(move |notice| {
                if let ExchangeGrantNotice::Facts(facts) = notice
                    && let Some(sender) = notice_tx.lock().unwrap().take()
                {
                    let _ = sender.send(facts);
                }
            })).unwrap();
            assert!(notice_rx.await.unwrap().sender_stopped);
            let generation = clients.generations.load(Ordering::Acquire);
            assert!(generation > prior); prior = generation;
            clients.forget(stream);
            assert!(clients.peers.lock().unwrap().is_empty());
        }
        drop(clients);
        // A pre-dispatch cancellation proves no request was processed.
        let cancelled = CancellationToken::new(); cancelled.cancel();
        assert!(matches!(client.deliver_exchange_unit_async(codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {
            stream, sender_count: 1, sender_id: 0, backend_number: 0, item: ExchangeUnitItem::End(ExchangeUnitSequence::try_new(2).unwrap()), payload: Bytes::new(), next_demand: None,
        }), Duration::from_secs(2), cancelled).await, Err(crate::native_client::ExchangeRpcFailure::NotProcessed(_))));
        // The remote handler is definitely executing before the local deadline.
        // Its blocking owner survives the caller's Unknown notification.
        let hold = Arc::new(DeliveryHold::default());
        *port.delivery_hold.lock().unwrap() = Some(hold.clone());
        let transmitter = crate::exchange_transmitter::grpc_exchange_transmitter(runtime.clone(), Duration::from_millis(150), #[cfg(novarocks_dev_controls)] None);
        let exited = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let exit_flag = exited.clone();
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        let activity = novarocks_execution::runtime::task::io::exchange_unit::ExchangeCallActivity::new(move || {
            exit_flag.store(true, Ordering::Release); let _ = exit_tx.send(());
        });
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        transmitter.deliver_unit(novarocks_execution::runtime::task::io::exchange_unit::ExchangeUnitFrame {
            destination: RuntimeEndpoint::from_socket_addr(server.bound_addr()),
            receiver: novarocks_execution::runtime::task::io::exchange_unit::ExchangeUnitReceiverFrame {
                stream, sender_count: 1, sender_id: 0, backend_number: 0, item: ExchangeUnitItem::End(ExchangeUnitSequence::try_new(2).unwrap()), payload: Bytes::new(), next_demand: None,
            },
        }, CancellationToken::new(), activity, Box::new(move |outcome| { let _ = done_tx.send((outcome, exited.load(Ordering::Acquire))); }));
        hold.entered.notified().await;
        let (outcome, released_at_notification) = done_rx.await.unwrap();
        hold.release();
        assert!(matches!(outcome, novarocks_execution::runtime::task::io::exchange_unit::ExchangeCallOutcome::Unknown(_)));
        assert!(!released_at_notification);
        exit_rx.await.unwrap();

        // Retained Worker close facts answer late data, End and demand on the
        // real authenticated Native service without reentering the live port.
        use novarocks_execution_contract::{
            ExchangeCloseDisposition, ExchangeDeliveryRequirement, ExchangeInbound,
            ExchangeStreamClose, ExchangeTopology, TaskDescriptor, TaskIdentity,
        };
        let deliveries = port.deliveries.load(Ordering::Relaxed);
        for (ordinal, delivery, disposition) in [
            (41, ExchangeDeliveryRequirement::MustDrain,
                ExchangeCloseDisposition::Consumed { final_sequence: ExchangeUnitSequence::try_new(2).unwrap() }),
            (42, ExchangeDeliveryRequirement::DemandDriven, ExchangeCloseDisposition::Withdrawn),
        ] {
            let destination = TaskIdentity::new(
                stream.destination().query_execution_id(), stream.destination().stage_id(),
                novarocks_types::TaskId::new(ordinal).unwrap(), stream.destination().backend_process_id(),
            );
            let closed = ExchangeStreamIdentity::try_new(
                stream.source(), destination, UniqueId::new(91844, i64::from(ordinal)), stream.destination_node(),
            ).unwrap();
            let descriptor = TaskDescriptor::try_new(
                destination, closed.destination_task_key(), std::num::NonZeroUsize::new(1).unwrap(),
                Vec::new(), ExchangeTopology::try_new(Vec::new(), vec![ExchangeInbound::try_new(
                    closed.destination_node(), vec![closed.source()], delivery,
                ).unwrap()]).unwrap(),
            ).unwrap();
            capabilities.install(Arc::new(descriptor.clone())).unwrap();
            if delivery == ExchangeDeliveryRequirement::MustDrain {
                assert!(capabilities.close_normally_with_facts(&descriptor, vec![ExchangeStreamClose {
                    stream: closed, disposition: ExchangeCloseDisposition::Withdrawn,
                }], || panic!("strict withdrawal must not retire the receiver")).is_err());
            }
            capabilities.close_normally_with_facts(&descriptor, vec![ExchangeStreamClose {
                stream: closed, disposition,
            }], || {}).unwrap();
            let expected = ExchangeStreamFacts {
                receiver_closed: Some(disposition),
                ..if matches!(disposition, ExchangeCloseDisposition::Consumed { .. }) { facts(2, 1) } else { facts(0, 0) }
            };
            for item in [ExchangeUnitItem::Unit { demand: first, end_after_unit: false },
                ExchangeUnitItem::End(ExchangeUnitSequence::try_new(2).unwrap())] {
                let payload = if matches!(item, ExchangeUnitItem::Unit { .. }) { Bytes::from_static(b"abc") } else { Bytes::new() };
                let reply = client.deliver_exchange_unit_async(codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {
                    stream: closed, sender_count: 1, sender_id: 0, backend_number: 0,
                    item, payload, next_demand: None,
                }), Duration::from_secs(2), CancellationToken::new()).await.unwrap();
                assert!(matches!(codec::decode_delivery_response(&reply, closed, item.sequence(), FieldPath::root("test")).unwrap(),
                    codec::DecodedExchangeDeliveryOutcome::Observed { verdict: novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeAdmissionVerdict::Idempotent, facts } if facts == expected));
            }
            for operation in [codec::ExchangeDemandOperation::Demand(first), codec::ExchangeDemandOperation::StopSender] {
                let entry = codec::DecodedExchangeDemandEntry { stream: closed, operation };
                let reply = client.register_exchange_demand_async(codec::encode_demand_request(&[entry], FieldPath::root("test")).unwrap(),
                    Duration::from_secs(2), CancellationToken::new()).await.unwrap();
                assert_eq!(codec::decode_demand_response(&reply, &[closed], FieldPath::root("test")).unwrap()[0].outcome, Ok(expected));
            }
            let reply = client.deliver_exchange_unit_async(codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {
                stream: closed, sender_count: 2, sender_id: 0, backend_number: 0,
                item: ExchangeUnitItem::End(ExchangeUnitSequence::try_new(2).unwrap()), payload: Bytes::new(), next_demand: None,
            }), Duration::from_secs(2), CancellationToken::new()).await.unwrap();
            assert!(matches!(codec::decode_delivery_response(&reply, closed, ExchangeUnitSequence::try_new(2).unwrap(), FieldPath::root("test")).unwrap(),
                codec::DecodedExchangeDeliveryOutcome::Refused(_)));
            let absent = sibling(closed, closed.destination_node().get() as usize + 1);
            let reply = client.register_exchange_demand_async(codec::encode_demand_request(&[
                codec::DecodedExchangeDemandEntry { stream: absent, operation: codec::ExchangeDemandOperation::Demand(first) },
            ], FieldPath::root("test")).unwrap(), Duration::from_secs(2), CancellationToken::new()).await.unwrap();
            assert!(codec::decode_demand_response(&reply, &[absent], FieldPath::root("test")).unwrap()[0].outcome.is_err());
            assert!(!port.states.lock().unwrap().contains_key(&closed));
        }
        assert_eq!(port.deliveries.load(Ordering::Relaxed), deliveries);

    }).await;
    server.stop().unwrap();
    result.expect("native unit fixture exceeded its failure watchdog");
}

#[test]
fn all_three_unit_methods_share_one_frozen_exchange_connection_quota() {
    use crate::native_channel_identity::InlineNativeChannelIdentity;
    use crate::native_connection_key_capacity::NativeConnectionKeyCapacity;
    use novarocks_proto_codec::native_rpc::NativeRpcMethod;
    let peer = BackendProcessId::new_v7();
    let endpoint = RuntimeEndpoint::new("127.0.0.1", 9090).unwrap();
    let identity = |method| {
        InlineNativeChannelIdentity::from_parts(Some(peer), endpoint.native_endpoint(), method)
            .unwrap()
    };
    let deliver = identity(NativeRpcMethod::DeliverExchangeUnit);
    let register = identity(NativeRpcMethod::RegisterExchangeDemand);
    let subscribe = identity(NativeRpcMethod::SubscribeExchangeGrants);
    assert_eq!(deliver, register);
    assert_eq!(register, subscribe);
    let capacity = NativeConnectionKeyCapacity::new().unwrap();
    let first = capacity.claim(deliver).unwrap();
    assert_eq!(
        capacity.claim(register).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    capacity.install(first).unwrap();
    let second = capacity.claim(subscribe).unwrap();
    capacity.install(second).unwrap();
    let third = capacity.claim(register).unwrap();
    assert_eq!(
        capacity.install(third).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    capacity.exit(third).unwrap();
    capacity.exit(second).unwrap();
    capacity.exit(first).unwrap();
}

/// This observer holds the actual receiver only after its unique decode lease
/// exists. It never supplies a decoder result or an exchange acknowledgement.
#[cfg(novarocks_dev_controls)]
#[derive(Default)]
struct RealUnitDecodeObservation {
    hold: Arc<DeliveryHold>,
    starts: AtomicU64,
    exits: AtomicU64,
    exited: Notify,
}
#[cfg(novarocks_dev_controls)]
impl std::fmt::Debug for RealUnitDecodeObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealUnitDecodeObservation")
            .finish_non_exhaustive()
    }
}
#[cfg(novarocks_dev_controls)]
impl novarocks_execution::runtime::exchange::TestEventObserver for RealUnitDecodeObservation {
    fn is_active(&self) -> bool {
        true
    }
    fn observe(&self, event: &str) {
        if event.contains("event=unit_decode_begin ") {
            let previous = self.starts.fetch_add(1, Ordering::AcqRel);
            assert_eq!(previous, 0, "replay must not enter a second decoder");
            self.hold.block();
        } else if event.contains("event=unit_decode_exited ") {
            self.exits.fetch_add(1, Ordering::AcqRel);
            self.exited.notify_one();
        }
    }
}

#[cfg(novarocks_dev_controls)]
struct RealUnitCompileControl;
#[cfg(novarocks_dev_controls)]
impl novarocks_type_contract::PureCompileControl for RealUnitCompileControl {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}
#[cfg(novarocks_dev_controls)]
fn real_unit_schema(
    factory: &dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory,
    ty: arrow::datatypes::DataType,
) -> novarocks_execution::exec::chunk::ChunkSchemaRef {
    let binding = novarocks_type_contract::FunctionValueType::new(ty.clone(), false);
    let declaration = novarocks_local_program::StaticLayout::try_new_typed_for_compile(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("v", ty, false),
        ])),
        Arc::from([novarocks_types::SlotId::new(1)]),
        vec![(binding.typed_binding().clone(), None)],
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &RealUnitCompileControl,
    )
    .unwrap();
    factory.create_compiled_chunk_schema(&declaration).unwrap()
}

#[cfg(novarocks_dev_controls)]
struct RealUnitReceiverFixture {
    stream: ExchangeStreamIdentity,
    execution: Arc<novarocks_execution::runtime::execution_runtime::ExecutionRuntime>,
    owner: Arc<novarocks_execution::runtime::task_control::TaskExecutionControl>,
    failure: Arc<novarocks_execution::runtime::runtime_state::RuntimeErrorState>,
    factory: Arc<dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory>,
    port: Arc<novarocks_execution::runtime::task::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort>,
    key: ExchangeReceiverKey,
    registry_key: novarocks_execution::runtime::exchange::ExchangeKey,
    descriptor: Arc<novarocks_execution_contract::TaskDescriptor>,
    _preparation: crate::backend_task_execution::node_test_support::NativeNodePreparation,
}

#[cfg(novarocks_dev_controls)]
fn real_unit_receiver_fixture(
    delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
) -> RealUnitReceiverFixture {
    use novarocks_execution::runtime::exchange::{ExchangeColumnBinding, ExchangeKey};
    use novarocks_execution::runtime::execution_runtime::{
        ExecutionRuntime, ExecutionRuntimeConfig,
    };
    use novarocks_execution::runtime::runtime_state::RuntimeErrorState;
    use novarocks_execution::runtime::task::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort;
    use novarocks_execution_contract::{
        ExchangeInbound, ExchangeTopology, TaskDescriptor, TaskFailurePhase,
    };
    let base = base_stream();
    let stream = ExchangeStreamIdentity::try_new(
        ExchangeSource::new(base.source().task(), base.source().task_key(), 0),
        base.destination(),
        base.destination_task_key(),
        base.destination_node(),
    )
    .unwrap();
    let mut functions =
        novarocks_execution::runtime::function_set::ExecutionFunctionSetBuilder::new();
    novarocks_sql::compiler::contribute_builtin_functions(functions.catalog_builder_mut()).unwrap();
    let execution = Arc::new(
        ExecutionRuntime::new(
            ExecutionRuntimeConfig {
                driver_threads: 1,
                exchange_io_max_inflight_bytes: 1,
                exchange_max_transmit_batched_bytes: 1,
                operator_buffer_chunks: 1,
                local_exchange_buffer_mem_limit_per_driver: 1,
                local_exchange_max_buffered_rows: 1,
                runtime_filter_scan_wait_time_ms_override: None,
                runtime_filter_wait_timeout_ms_override: None,
                sink_io_worker_threads: 1,
                sink_io_max_blocking_threads: 1,
            },
            Arc::new(functions.seal().unwrap()),
            crate::backend_test_support::test_memory_authority(),
        )
        .unwrap(),
    );
    let preparation = crate::backend_task_execution::node_test_support::NativeNodePreparation::new(
        stream.destination(),
    );
    let owner = Arc::new(
        preparation
            .execution_control()
            .in_phase(TaskFailurePhase::Execution),
    );
    let failure = Arc::new(RuntimeErrorState::for_task(owner.clone()));
    let factory = novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        novarocks_execution::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        MemTracker::new_root("real native receiver decoder"), failure.clone(),
    ).unwrap();
    let port = Arc::new(ExecutionRuntimeExchangeReceiverPort::new(execution.clone()));
    let key = ExchangeReceiverKey {
        task_key: stream.destination_task_key(),
        node_id: stream.destination_node().get(),
    };
    port.register({
        let expected_chunk_schema =
            real_unit_schema(factory.as_ref(), arrow::datatypes::DataType::Int64);
        ExchangeReceiverRegistration {
            key,
            expected_senders: 1,
            wire_chunk_schema: expected_chunk_schema.clone(),
            expected_chunk_schema,
            column_binding: ExchangeColumnBinding::Positional,
            creation_factory: factory.clone(),
        }
    })
    .unwrap();
    port.bind_unit_receiver(key, vec![stream], failure.clone(), 64 * 1024 * 1024)
        .unwrap();
    let registry_key = ExchangeKey {
        task_key_hi: key.task_key.high(),
        task_key_lo: key.task_key.low(),
        node_id: key.node_id,
    };
    let descriptor = TaskDescriptor::try_new(
        stream.destination(),
        key.task_key,
        std::num::NonZeroUsize::new(1).unwrap(),
        Vec::new(),
        ExchangeTopology::try_new(
            Vec::new(),
            vec![
                ExchangeInbound::try_new(
                    stream.destination_node(),
                    vec![stream.source()],
                    delivery,
                )
                .unwrap(),
            ],
        )
        .unwrap(),
    )
    .unwrap();
    RealUnitReceiverFixture {
        stream,
        execution,
        owner,
        failure,
        factory,
        port,
        key,
        registry_key,
        descriptor: Arc::new(descriptor),
        _preparation: preparation,
    }
}

#[cfg(novarocks_dev_controls)]
fn start_real_unit_receiver_service(
    stream: ExchangeStreamIdentity,
    port: Arc<novarocks_execution::runtime::task::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort>,
    descriptor: Arc<novarocks_execution_contract::TaskDescriptor>,
) -> (
    crate::NativeRpcListener,
    Arc<NativeRpcClient>,
    Arc<novarocks_worker::TaskInboundCapabilities>,
) {
    use crate::native_transport_admission::{NativeTransportAdmission, TransportClass};
    use novarocks_native_trust::{NativeIncomingAdapter, NativeProcessIdentity};
    use novarocks_proto_codec::native_rpc::NativeEndpointDomain;
    let capabilities = novarocks_worker::TaskInboundCapabilities::new();
    capabilities.install(descriptor).unwrap();
    let trust = crate::backend_test_support::test_backend_native_trust();
    trust
        .bind_process_identity(NativeProcessIdentity::Backend(
            stream.source().task().backend_process_id(),
        ))
        .unwrap();
    let receiver_trust = crate::backend_test_support::test_backend_native_trust();
    receiver_trust
        .bind_process_identity(NativeProcessIdentity::Backend(
            stream.destination().backend_process_id(),
        ))
        .unwrap();
    let backend = novarocks_execution_contract::BackendProcessDescriptor::try_new(
        stream.destination().backend_process_id(),
        RuntimeEndpoint::new("127.0.0.1", 9090).unwrap(),
        RuntimeEndpoint::new("127.0.0.1", 9091).unwrap(),
        "test",
        "test",
        novarocks_types::NativeCompatibilityId::new([1; 32]),
        1,
    )
    .unwrap();
    let service = crate::backend_rpc_service::BackendRpcService::new(
        Arc::new(UnusedIngress),
        Arc::new(UnusedIngress),
        Arc::new(UnusedIngress),
        port.clone(),
        capabilities.clone(),
        crate::backend_heartbeat::BackendHeartbeatResponder::new(
            backend,
            Arc::new(novarocks_worker::WorkerDrainState::new()),
            Arc::new(UnusedIngress),
        ),
    );
    let server = crate::NativeRpcListener::start_with_admission(
        "127.0.0.1",
        0,
        service,
        receiver_trust,
        NativeIncomingAdapter::plaintext(),
        "real-exchange-unit-test",
        NativeEndpointDomain::BackendData,
        "real-exchange-unit-test",
        || {},
        || {},
        crate::native_server::NativeIngressConfig {
            worker_threads: 2,
            ..Default::default()
        },
        NativeTransportAdmission::new().unwrap(),
        TransportClass::Data,
        None,
    )
    .unwrap();
    let runtime = BackendDataRuntime::new(
        tokio::runtime::Handle::current(),
        trust,
        crate::BackendNativeTransport::Plaintext,
    );
    let client = Arc::new(NativeRpcClient::new_backend_endpoint(
        runtime,
        RuntimeEndpoint::from_socket_addr(server.bound_addr())
            .native_endpoint()
            .clone(),
        stream.destination().backend_process_id(),
    ));
    (server, client, capabilities)
}

#[cfg(novarocks_dev_controls)]
#[tokio::test]
async fn authenticated_tonic_unknown_reply_and_decoding_replay_preserve_real_decoder_failure() {
    use crate::backend_task_execution::execution_host::tests::{
        declared_exchange_recipe, encode_declared_exchange_unit,
    };
    use novarocks_execution::exec::chunk::Chunk;
    use novarocks_execution_contract::ordered_retained_stream::exchange::{
        ExchangeAdmissionVerdict, ExchangeUnitDemand, ExchangeUnitItem,
    };
    use novarocks_execution_contract::{ExchangeCloseDisposition, ExchangeDeliveryRequirement};

    // If an assertion unwinds, release the actual blocking handler before the
    // Native server's own Drop joins it. This does not author receiver closure.
    struct ReleaseDecode(Arc<DeliveryHold>);
    impl Drop for ReleaseDecode {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    let RealUnitReceiverFixture {
        stream,
        execution,
        owner,
        failure,
        factory,
        port,
        key,
        registry_key,
        descriptor,
        _preparation,
    } = real_unit_receiver_fixture(ExchangeDeliveryRequirement::MustDrain);
    let registry = execution.exchange_registry();
    let observation = Arc::new(RealUnitDecodeObservation::default());
    registry
        .install_test_observer(registry_key, observation.clone())
        .unwrap();
    let receiver = port.receiver_pin(key, 1).unwrap();
    // Keep the receiver's valid NRU1 header and recipe digest, but give its
    // Int64 decoder an actual Int32 batch body. With no Schema message, the
    // four-byte values buffer must fail the receiver's eight-byte geometry
    // check after the first RPC's outcome has become unknown.
    let chunk = Chunk::try_new_with_columns(
        real_unit_schema(factory.as_ref(), arrow::datatypes::DataType::Int32),
        vec![Arc::new(arrow::array::Int32Array::from(vec![7]))],
    )
    .unwrap();
    let mut payload = encode_declared_exchange_unit(&chunk);
    let receiver_recipe = declared_exchange_recipe(&real_unit_schema(
        factory.as_ref(),
        arrow::datatypes::DataType::Int64,
    ));
    assert_eq!(&payload[..4], b"NRU1");
    payload[4..36].copy_from_slice(receiver_recipe.digest());
    assert_eq!(&payload[4..36], receiver_recipe.digest());
    let payload = Bytes::from(payload);
    let demand = ExchangeUnitDemand {
        sequence: ExchangeUnitSequence::FIRST,
        wire_bytes: payload.len() as u64,
    };
    let frame = codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {
        stream,
        sender_count: 1,
        sender_id: 0,
        backend_number: 0,
        item: ExchangeUnitItem::Unit {
            demand,
            end_after_unit: false,
        },
        payload,
        next_demand: None,
    });
    let (mut server, client, _capabilities) =
        start_real_unit_receiver_service(stream, port.clone(), descriptor);
    let release_on_exit = ReleaseDecode(observation.hold.clone());
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        let reply = client.register_exchange_demand_async(codec::encode_demand_request(&[
            codec::DecodedExchangeDemandEntry { stream, operation: codec::ExchangeDemandOperation::Demand(demand) },
        ], FieldPath::root("real_test")).unwrap(), Duration::from_secs(2), CancellationToken::new()).await.unwrap();
        assert_eq!(codec::decode_demand_response(&reply, &[stream], FieldPath::root("real_test")).unwrap()[0].outcome, Ok(facts(0, 1)));
        let first_client = client.clone();
        let first_frame = frame.clone();
        let first = tokio::spawn(async move {
            first_client.deliver_exchange_unit_async(first_frame, Duration::from_millis(150), CancellationToken::new()).await
        });
        observation.hold.entered.notified().await;
        assert!(matches!(first.await.unwrap(), Err(crate::native_client::ExchangeRpcFailure::Unknown(_))));
        assert!(owner.accepted().is_none());
        // This request must return while the original still owns Decoding.
        // Its response is custody0, rather than a false admission/consumption.
        let replay = client.deliver_exchange_unit_async(frame, Duration::from_secs(2), CancellationToken::new()).await.unwrap();
        assert!(matches!(codec::decode_delivery_response(&replay, stream, demand.sequence, FieldPath::root("real_test")).unwrap(),
            codec::DecodedExchangeDeliveryOutcome::Observed { verdict: ExchangeAdmissionVerdict::Idempotent, facts: state } if state == facts(0, 1)));
        assert_eq!(observation.starts.load(Ordering::Acquire), 1);
        assert_eq!(observation.exits.load(Ordering::Acquire), 0);
        assert!(owner.accepted().is_none());
        observation.hold.release();
        observation.exited.notified().await;
        // The decoder submits to the real Context core. Its physical exit and
        // that core's acceptance are separate facts, so observe the original
        // stop publication before borrowing the accepted originating source.
        assert!(owner.wait_until_stopped(Duration::from_secs(2)));
        let accepted = owner.accepted().expect("real decoder failure must reach the original Task owner despite Unknown");
        let original = accepted.originating_failure().expect("an originating decoder failure");
        let projected = failure.error().unwrap();
        assert!(std::ptr::eq(projected.cause(), original.cause()));
        assert!(original.detail().contains("IPC fixed-width values are too short"), "the original unit body geometry guard must author the failure: {original}");
        assert!(!original.detail().contains("recipe digest or version differs"), "the valid receiver header must reach body validation: {original}");
        assert_eq!(observation.exits.load(Ordering::Acquire), 1);
        assert!(receiver.try_pop_next_with_stats(1).unwrap().is_none());
        assert_eq!(registry.unit_sender_states(stream.source().task().backend_process_id(), None, 1)[0].facts.custody_through, 0);
        assert_eq!(port.stream_close_facts(key, &[stream]).unwrap()[0].disposition, ExchangeCloseDisposition::Withdrawn);
    }).await;
    drop(release_on_exit);
    port.cancel(key);
    server.stop().unwrap();
    result.expect("real native decoder regression exceeded its failure watchdog");
}

#[cfg(novarocks_dev_controls)]
#[tokio::test]
async fn authenticated_tonic_stop_before_first_demand_fences_the_real_frozen_receiver() {
    use novarocks_execution::exec::chunk::Chunk;
    use novarocks_execution::runtime::exchange::encode_chunks;
    use novarocks_execution_contract::ordered_retained_stream::exchange::{
        ExchangeAdmissionVerdict, ExchangeUnitDemand, ExchangeUnitItem,
    };
    use novarocks_execution_contract::{
        ExchangeCloseDisposition, ExchangeDeliveryRequirement, TaskIdentity,
    };

    async fn demand(
        client: &NativeRpcClient,
        stream: ExchangeStreamIdentity,
        operation: codec::ExchangeDemandOperation,
    ) -> Result<ExchangeStreamFacts, FailureFact> {
        let entry = codec::DecodedExchangeDemandEntry { stream, operation };
        let response = client
            .register_exchange_demand_async(
                codec::encode_demand_request(&[entry], FieldPath::root("real_stop_test")).unwrap(),
                Duration::from_secs(2),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        codec::decode_demand_response(&response, &[stream], FieldPath::root("real_stop_test"))
            .unwrap()[0]
            .outcome
    }

    let fixture = real_unit_receiver_fixture(ExchangeDeliveryRequirement::DemandDriven);
    let stream = fixture.stream;
    let registry = fixture.execution.exchange_registry();
    let receiver = fixture.port.receiver_pin(fixture.key, 1).unwrap();
    let (mut server, client, capabilities) =
        start_real_unit_receiver_service(stream, fixture.port.clone(), fixture.descriptor.clone());
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        let initial = registry.unit_sender_states(stream.source().task().backend_process_id(), None, 2);
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].stream, stream);
        assert_eq!(initial[0].facts, facts(0, 0));

        // This authenticated call is the first operation on the exact stream:
        // registration froze its receipt, but no demand or unit has existed.
        let stopped = demand(&client, stream, codec::ExchangeDemandOperation::StopSender).await.unwrap();
        assert_eq!(stopped, ExchangeStreamFacts { sender_stopped: true, ..facts(0, 0) });
        let chunk = Chunk::try_new_with_columns(
            real_unit_schema(fixture.factory.as_ref(), arrow::datatypes::DataType::Int64),
            vec![Arc::new(arrow::array::Int64Array::from(vec![7]))],
        ).unwrap();
        let payload = Bytes::from(encode_chunks(&[chunk], true).unwrap());
        let first = ExchangeUnitDemand { sequence: ExchangeUnitSequence::FIRST, wire_bytes: payload.len() as u64 };
        assert_eq!(demand(&client, stream, codec::ExchangeDemandOperation::Demand(first)).await.unwrap(), stopped);
        // A real valid IPC payload, and then an End, must not decode, publish
        // rows, mint custody or pretend that this receiver consumed input.
        for (item, payload) in [
            (ExchangeUnitItem::Unit { demand: first, end_after_unit: false }, payload),
            (ExchangeUnitItem::End(first.sequence.checked_next().unwrap()), Bytes::new()),
        ] {
            let response = client.deliver_exchange_unit_async(codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {
                stream, sender_count: 1, sender_id: 0, backend_number: 0, item, payload, next_demand: None,
            }), Duration::from_secs(2), CancellationToken::new()).await.unwrap();
            assert!(matches!(codec::decode_delivery_response(&response, stream, item.sequence(), FieldPath::root("real_stop_test")).unwrap(),
                codec::DecodedExchangeDeliveryOutcome::Observed { verdict: ExchangeAdmissionVerdict::Idempotent, facts } if facts == stopped));
        }
        assert_eq!(demand(&client, stream, codec::ExchangeDemandOperation::StopSender).await.unwrap(), stopped);

        // Authentication of a BE process does not grant another Task or node
        // access to this receipt, even after its sender has stopped.
        let original = stream.source().task();
        let foreign_task = TaskIdentity::new(
            original.query_execution_id(), original.stage_id(),
            novarocks_types::TaskId::new(original.task_id().get().checked_add(1).unwrap()).unwrap(),
            original.backend_process_id(),
        );
        let foreign = ExchangeStreamIdentity::try_new(
            ExchangeSource::new(foreign_task, stream.source().task_key(), stream.source().sender_ordinal()),
            stream.destination(), stream.destination_task_key(), stream.destination_node(),
        ).unwrap();
        for absent in [foreign, sibling(stream, stream.destination_node().get() as usize + 1)] {
            assert!(demand(&client, absent, codec::ExchangeDemandOperation::StopSender).await.is_err());
        }
        assert_eq!(registry.unit_sender_states(original.backend_process_id(), None, 2), vec![ExchangeStreamObservation { stream, facts: stopped }]);
        let flow = registry.try_flow_snapshot().expect("original real slot ledger is observable");
        assert_eq!(flow.live_instances, 1);
        assert_eq!((flow.in_use, flow.decoding, flow.pending_demands, flow.floating_in_use, flow.max_instance_in_use), (0, 0, 0, 0, 0));
        let queue = fixture.port.snapshot(fixture.key).unwrap();
        assert_eq!((queue.queued_chunks, queue.queued_rows, queue.finished_senders), (0, 0, 0));
        assert!(!queue.canceled);
        assert!(receiver.try_pop_next_with_stats(1).unwrap().is_none());
        assert!(receiver.complete_input_consumption(1).is_err());
        assert_eq!(fixture.port.stream_close_facts(fixture.key, &[stream]).unwrap()[0].disposition, ExchangeCloseDisposition::Withdrawn);
        assert!(fixture.owner.accepted().is_none());
        assert!(fixture.failure.error().is_none());

        // The same real Worker retirement now wins before late requests.
        // Its retained Withdrawn proof, rather than an absent queue, answers
        // both Stop and Demand without inventing a Consumed prefix.
        capabilities.close_normally_with_facts(
            &fixture.descriptor,
            fixture.port.stream_close_facts(fixture.key, &[stream]).unwrap(),
            || fixture.port.remove(fixture.key),
        ).unwrap();
        for operation in [codec::ExchangeDemandOperation::StopSender, codec::ExchangeDemandOperation::Demand(first)] {
            let closed = demand(&client, stream, operation).await.unwrap();
            assert_eq!(closed.receiver_closed, Some(ExchangeCloseDisposition::Withdrawn));
            assert_eq!((closed.custody_through, closed.granted_through), (0, 0));
        }
        let retired = registry.try_flow_snapshot().unwrap();
        // The original receiver handle still pins the retired ledger. An
        // empty slot census does not mean that this actual holder exited.
        assert_eq!((retired.live_instances, retired.closed_instances), (1, 1));
        assert_eq!((retired.in_use, retired.decoding, retired.pending_demands, retired.floating_in_use), (0, 0, 0, 0));
        drop(receiver);
        assert_eq!(registry.try_flow_snapshot().unwrap().live_instances, 0);
    }).await;
    fixture.port.cancel(fixture.key);
    server.stop().unwrap();
    result.expect("real native stop-before-demand regression exceeded its failure watchdog");
}
