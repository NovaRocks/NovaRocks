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

//! Process-pair observation of receiver-authored cumulative Exchange facts.
//!
//! Changes carry complete facts so retiring a live receiver cannot lose its
//! last update between publication and lookup. The pending map coalesces facts
//! by exact stream, never becoming a second receiver ledger or event history.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::{NonZeroU16, NonZeroU64};
use std::ops::Bound::{Excluded, Unbounded};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::runtime::fragment::io::ExchangeReceiverPort;
use novarocks_execution::runtime::fragment::io::exchange_unit::{
    ExchangeGrantNotice, ExchangeGrantWake, ExchangeStreamObservation, ExchangeUnitStateListener,
};
use novarocks_execution_contract::ExchangeStreamIdentity;
use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason, domain};
use novarocks_execution_contract::ordered_retained_stream::exchange::{
    ExchangeStreamFacts, ExchangeUnitCloseDisposition,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as wire;
use novarocks_task_codec::exchange_unit_codec::{self as codec, ExchangeProcessPair};
use novarocks_types::BackendProcessId;
use prost::Message;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};

use crate::BackendDataRuntime;
use crate::native_client::NativeRpcClient;
use crate::native_ingress::NativeIngressOwnership;

// A publication work quantum, not a protocol limit on legal batch length.
const PAGE: usize = 256;
const RECONNECT_BACKOFF: Duration = Duration::from_millis(25);

pub(crate) fn exchange_failure(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::EXCHANGE, NonZeroU16::new(code).unwrap()),
    )
}

pub(crate) fn subscription_contract_failure() -> FailureFact {
    exchange_failure(FailureClass::Invariant, 450)
}

/// Out-of-order publication must not replace a newer cumulative observation.
/// End can put custody exactly one past the granted data prefix.
fn merge_facts(
    old: ExchangeStreamFacts,
    new: ExchangeStreamFacts,
) -> Result<ExchangeStreamFacts, FailureFact> {
    if let (Some(old), Some(new)) = (old.receiver_closed, new.receiver_closed)
        && old != new
    {
        return Err(subscription_contract_failure());
    }
    let merged = ExchangeStreamFacts {
        custody_through: old.custody_through.max(new.custody_through),
        granted_through: old.granted_through.max(new.granted_through),
        sender_stopped: old.sender_stopped || new.sender_stopped,
        receiver_closed: old.receiver_closed.or(new.receiver_closed),
    };
    if merged.custody_through > merged.granted_through.saturating_add(1)
        || matches!(merged.receiver_closed, Some(ExchangeUnitCloseDisposition::Consumed { final_sequence })
            if final_sequence.get() != merged.custody_through
                || merged.granted_through.checked_add(1) != Some(final_sequence.get()))
    {
        return Err(subscription_contract_failure());
    }
    Ok(merged)
}

pub(crate) fn failure_status(fact: FailureFact) -> Status {
    let bytes = novarocks_task_codec::status::encode_failure_fact(fact).encode_to_vec();
    Status::with_details(
        Code::DataLoss,
        "exchange subscription contract failure",
        Bytes::from(bytes),
    )
}

fn status_failure(status: &Status) -> Option<FailureFact> {
    if status.code() != Code::DataLoss {
        return None;
    }
    let value = wire::FailureFact::decode(status.details()).ok()?;
    novarocks_task_codec::status::decode_failure_fact(
        &value,
        FieldPath::root("exchange_subscription_failure"),
    )
    .ok()
}

#[derive(Default)]
struct Pending {
    facts: BTreeMap<ExchangeStreamIdentity, ExchangeStreamFacts>,
    delta_after: Option<ExchangeStreamIdentity>,
    closed: Option<Status>,
}

struct Feed {
    generation: NonZeroU64,
    pending: Mutex<Pending>,
    changed: Arc<Notify>,
}

impl Feed {
    fn close(&self, status: Status) {
        let mut pending = self.pending.lock().expect("exchange grant feed");
        if pending.closed.is_none() {
            pending.closed = Some(status);
        }
        drop(pending);
        self.changed.notify_one();
    }

    fn push(&self, observation: ExchangeStreamObservation) {
        let mut pending = self.pending.lock().expect("exchange grant feed");
        if pending.closed.is_some() {
            return;
        }
        let merged = match pending.facts.get(&observation.stream) {
            Some(old) => merge_facts(*old, observation.facts),
            None => merge_facts(ExchangeStreamFacts::default(), observation.facts),
        };
        match merged {
            Ok(facts) => {
                pending.facts.insert(observation.stream, facts);
            }
            Err(fact) => pending.closed = Some(failure_status(fact)),
        }
        drop(pending);
        self.changed.notify_one();
    }
}

struct PeerFeed {
    high_generation: NonZeroU64,
    active: Weak<Feed>,
}

pub(crate) struct ExchangeGrantPublisher {
    port: Arc<dyn ExchangeReceiverPort>,
    peers: Mutex<BTreeMap<BackendProcessId, PeerFeed>>,
}

struct PublisherListener(Weak<ExchangeGrantPublisher>);

impl ExchangeUnitStateListener for PublisherListener {
    fn changed(&self, observation: ExchangeStreamObservation) {
        let Some(publisher) = self.0.upgrade() else {
            return;
        };
        let feed = publisher
            .peers
            .lock()
            .expect("exchange grant publishers")
            .get(&observation.stream.source().task().backend_process_id())
            .and_then(|peer| peer.active.upgrade());
        if let Some(feed) = feed {
            feed.push(observation);
        }
    }
}

impl ExchangeGrantPublisher {
    /// Called only when unit observation is installed, never on the legacy
    /// startup path. A missing receiver capability remains an exact refusal.
    pub(crate) fn install(port: Arc<dyn ExchangeReceiverPort>) -> Result<Arc<Self>, FailureFact> {
        let publisher = Arc::new(Self {
            port: Arc::clone(&port),
            peers: Mutex::default(),
        });
        port.set_unit_state_listener(Arc::new(PublisherListener(Arc::downgrade(&publisher))))?;
        Ok(publisher)
    }

    fn reclaim_inactive_peers(&self) {
        let candidates = self
            .peers
            .lock()
            .expect("exchange grant publishers")
            .iter()
            .filter(|(_, peer)| peer.active.upgrade().is_none())
            .map(|(sender, peer)| (*sender, peer.high_generation))
            .collect::<Vec<_>>();
        for (sender, generation) in candidates {
            // Empty observation only retires unused publisher metadata. It
            // never concludes that a Task failed or that any flow was closed.
            if !self
                .port
                .unit_sender_states(sender, None, 1)
                .is_ok_and(|states| states.is_empty())
            {
                continue;
            }
            let mut peers = self.peers.lock().expect("exchange grant publishers");
            if peers.get(&sender).is_some_and(|peer| {
                peer.high_generation == generation && peer.active.upgrade().is_none()
            }) {
                peers.remove(&sender);
            }
        }
    }

    pub(crate) fn subscribe(
        self: &Arc<Self>,
        subscription: codec::DecodedExchangeGrantSubscription,
        ownership: Option<Arc<NativeIngressOwnership>>,
    ) -> Result<ExchangeGrantStream, Status> {
        self.reclaim_inactive_peers();
        let mut peers = self.peers.lock().expect("exchange grant publishers");
        if let Some(peer) = peers.get(&subscription.pair.sender)
            && subscription.generation <= peer.high_generation
        {
            return Err(Status::already_exists(
                "exchange subscription generation is not newer",
            ));
        }
        let feed = Arc::new(Feed {
            generation: subscription.generation,
            pending: Mutex::default(),
            changed: Arc::new(Notify::new()),
        });
        if let Some(old) = peers
            .insert(
                subscription.pair.sender,
                PeerFeed {
                    high_generation: subscription.generation,
                    active: Arc::downgrade(&feed),
                },
            )
            .and_then(|old| old.active.upgrade())
        {
            old.close(Status::cancelled(
                "exchange subscription superseded by a newer generation",
            ));
        }
        drop(peers);
        crate::backend_metrics::exchange_grant_event("opened");
        Ok(ExchangeGrantStream {
            publisher: Arc::clone(self),
            subscription,
            feed,
            after: None,
            snapshot_done: false,
            finished: false,
            notified: None,
            _ownership: ownership,
        })
    }
}

pub(crate) struct ExchangeGrantStream {
    publisher: Arc<ExchangeGrantPublisher>,
    subscription: codec::DecodedExchangeGrantSubscription,
    feed: Arc<Feed>,
    after: Option<ExchangeStreamIdentity>,
    snapshot_done: bool,
    finished: bool,
    notified: Option<Pin<Box<tokio::sync::futures::OwnedNotified>>>,
    _ownership: Option<Arc<NativeIngressOwnership>>,
}

impl ExchangeGrantStream {
    fn encode(
        &mut self,
        snapshot: bool,
        complete: bool,
        states: Vec<codec::DecodedExchangeStreamState>,
    ) -> Result<wire::ExchangeGrantBatch, Status> {
        // Linearize publication with replacement. No port calls or callbacks
        // run while this short feed lock is held.
        let pending = self.feed.pending.lock().expect("exchange grant feed");
        if let Some(status) = pending.closed.clone() {
            self.finished = true;
            return Err(status);
        }
        crate::backend_metrics::exchange_grant_event(if snapshot { "snapshot" } else { "delta" });
        let result = codec::encode_grant_batch(
            &codec::DecodedExchangeGrantBatch {
                generation: self.feed.generation,
                snapshot,
                snapshot_complete: complete,
                states,
            },
            self.subscription.pair,
            FieldPath::root("exchange_grant_publication"),
        )
        .map_err(|_| failure_status(subscription_contract_failure()));
        if result.is_err() {
            self.finished = true;
        }
        result
    }
}

impl tokio_stream::Stream for ExchangeGrantStream {
    type Item = Result<wire::ExchangeGrantBatch, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        loop {
            let closed = this
                .feed
                .pending
                .lock()
                .expect("exchange grant feed")
                .closed
                .clone();
            if let Some(status) = closed {
                this.finished = true;
                return Poll::Ready(Some(Err(status)));
            }
            if !this.snapshot_done {
                let page = match this.publisher.port.unit_sender_states(
                    this.subscription.pair.sender,
                    this.after,
                    PAGE,
                ) {
                    Ok(states) => states,
                    Err(fact) => {
                        this.finished = true;
                        return Poll::Ready(Some(Err(failure_status(fact))));
                    }
                };
                if page.len() > PAGE
                    || page.windows(2).any(|pair| pair[0].stream >= pair[1].stream)
                    || page
                        .first()
                        .is_some_and(|state| this.after.is_some_and(|after| state.stream <= after))
                {
                    this.finished = true;
                    return Poll::Ready(Some(Err(failure_status(subscription_contract_failure()))));
                }
                let complete = page.is_empty();
                if let Some(last) = page.last() {
                    this.after = Some(last.stream);
                }
                this.snapshot_done = complete;
                return Poll::Ready(Some(
                    this.encode(
                        true,
                        complete,
                        page.into_iter()
                            .map(|state| codec::DecodedExchangeStreamState {
                                stream: state.stream,
                                facts: state.facts,
                            })
                            .collect(),
                    ),
                ));
            }
            let states = {
                let mut pending = this.feed.pending.lock().expect("exchange grant feed");
                // Resume after the last delivered stream, wrapping once. A
                // continuously changing low key must not starve higher keys.
                let keys = match pending.delta_after {
                    Some(after) => pending
                        .facts
                        .range((Excluded(after), Unbounded))
                        .chain(pending.facts.range(..=after))
                        .take(PAGE)
                        .map(|(stream, _)| *stream)
                        .collect::<Vec<_>>(),
                    None => pending.facts.keys().take(PAGE).copied().collect(),
                };
                if let Some(last) = keys.last() {
                    pending.delta_after = Some(*last);
                }
                keys.into_iter()
                    .map(|stream| codec::DecodedExchangeStreamState {
                        stream,
                        facts: pending
                            .facts
                            .remove(&stream)
                            .expect("selected pending stream"),
                    })
                    .collect::<Vec<_>>()
            };
            if !states.is_empty() {
                return Poll::Ready(Some(this.encode(false, false, states)));
            }
            if this.notified.is_none() {
                this.notified = Some(Box::pin(Arc::clone(&this.feed.changed).notified_owned()));
            }
            match this
                .notified
                .as_mut()
                .expect("installed feed notification")
                .as_mut()
                .poll(cx)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => this.notified = None,
            }
        }
    }
}

struct Watcher {
    wake: ExchangeGrantWake,
    last: Option<ExchangeStreamFacts>,
    refused: Option<FailureFact>,
}

#[derive(Default)]
struct ClientWatches {
    entries: BTreeMap<ExchangeStreamIdentity, Watcher>,
    refused: Option<FailureFact>,
}

struct ClientPeer {
    destination: RuntimeEndpoint,
    pair: ExchangeProcessPair,
    watches: Mutex<ClientWatches>,
    revision: AtomicU64,
    generation: AtomicU64,
    changed: Notify,
    stop: CancellationToken,
}

impl ClientPeer {
    fn apply(&self, generation: NonZeroU64, states: Vec<codec::DecodedExchangeStreamState>) {
        if self.stop.is_cancelled() || self.generation.load(Ordering::Acquire) != generation.get() {
            return;
        }
        let mut wakes = Vec::new();
        {
            let mut watches = self.watches.lock().expect("exchange grant client watches");
            for state in states {
                let Some(watcher) = watches.entries.get_mut(&state.stream) else {
                    continue;
                };
                if watcher.refused.is_some() {
                    continue;
                }
                let merged = merge_facts(watcher.last.unwrap_or_default(), state.facts);
                let notice = match merged {
                    Ok(facts) => {
                        watcher.last = Some(facts);
                        ExchangeGrantNotice::Facts(facts)
                    }
                    Err(fact) => {
                        watcher.refused = Some(fact);
                        ExchangeGrantNotice::Refused(fact)
                    }
                };
                wakes.push((Arc::clone(&watcher.wake), notice));
            }
        }
        for (wake, notice) in wakes {
            if self.stop.is_cancelled()
                || self.generation.load(Ordering::Acquire) != generation.get()
            {
                break;
            }
            wake(notice);
        }
    }

    fn resynchronize_missing(
        &self,
        generation: NonZeroU64,
        seen: &BTreeSet<ExchangeStreamIdentity>,
    ) {
        let wakes = self
            .watches
            .lock()
            .expect("exchange grant client watches")
            .entries
            .iter()
            .filter(|(stream, watcher)| !seen.contains(stream) && watcher.refused.is_none())
            .map(|(_, watcher)| Arc::clone(&watcher.wake))
            .collect::<Vec<_>>();
        for wake in wakes {
            if self.stop.is_cancelled()
                || self.generation.load(Ordering::Acquire) != generation.get()
            {
                break;
            }
            wake(ExchangeGrantNotice::Resynchronize);
        }
    }

    fn refuse_all(&self, fact: FailureFact) {
        let wakes = {
            let mut watches = self.watches.lock().expect("exchange grant client watches");
            watches.refused = Some(fact);
            watches
                .entries
                .values_mut()
                .filter_map(|watcher| {
                    if watcher.refused.replace(fact).is_some() {
                        None
                    } else {
                        Some(Arc::clone(&watcher.wake))
                    }
                })
                .collect::<Vec<_>>()
        };
        for wake in wakes {
            if self.stop.is_cancelled() {
                break;
            }
            wake(ExchangeGrantNotice::Refused(fact));
        }
    }
}

/// The generation counter belongs to this process-level client owner, rather
/// than a peer entry: removing the last watch never resets a reconnect to 1.
pub(crate) struct ExchangeGrantClients {
    runtime: BackendDataRuntime,
    timeout: Duration,
    generations: Arc<AtomicU64>,
    peers: Mutex<BTreeMap<BackendProcessId, Arc<ClientPeer>>>,
}

impl ExchangeGrantClients {
    pub(crate) fn new(runtime: BackendDataRuntime, timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            timeout,
            generations: Arc::new(AtomicU64::new(0)),
            peers: Mutex::default(),
        })
    }

    pub(crate) fn watch(
        &self,
        destination: RuntimeEndpoint,
        stream: ExchangeStreamIdentity,
        wake: ExchangeGrantWake,
    ) -> Result<(), FailureFact> {
        let sender = match self.runtime.native_trust().local_process_identity() {
            Some(novarocks_native_trust::NativeProcessIdentity::Backend(sender)) => sender,
            _ => return Err(exchange_failure(FailureClass::Data, 451)),
        };
        let pair = ExchangeProcessPair::for_stream(stream);
        if pair.sender != sender {
            return Err(exchange_failure(FailureClass::Data, 451));
        }
        let mut peers = self.peers.lock().expect("exchange grant client peers");
        let mut start = false;
        let peer = match peers.get(&pair.receiver) {
            Some(peer) if peer.destination != destination => {
                return Err(exchange_failure(FailureClass::Invariant, 452));
            }
            Some(peer) => Arc::clone(peer),
            None => {
                start = true;
                let peer = Arc::new(ClientPeer {
                    destination,
                    pair,
                    watches: Mutex::default(),
                    revision: AtomicU64::new(0),
                    generation: AtomicU64::new(0),
                    changed: Notify::new(),
                    stop: CancellationToken::new(),
                });
                peers.insert(pair.receiver, Arc::clone(&peer));
                peer
            }
        };
        {
            let mut watches = peer.watches.lock().expect("exchange grant client watches");
            if let Some(fact) = watches.refused {
                return Err(fact);
            }
            if let Some(existing) = watches.entries.get_mut(&stream) {
                if let Some(fact) = existing.refused {
                    return Err(fact);
                }
                existing.wake = wake;
            } else {
                watches.entries.insert(
                    stream,
                    Watcher {
                        wake,
                        last: None,
                        refused: None,
                    },
                );
                peer.revision.fetch_add(1, Ordering::Release);
            }
        }
        drop(peers);
        peer.changed.notify_one();
        if start {
            let runtime = self.runtime.clone();
            let timeout = self.timeout;
            let generations = Arc::clone(&self.generations);
            self.runtime.handle().spawn(async move {
                run_client(runtime, timeout, generations, peer).await;
            });
        }
        Ok(())
    }

    pub(crate) fn forget(&self, stream: ExchangeStreamIdentity) {
        let receiver = stream.destination().backend_process_id();
        let mut peers = self.peers.lock().expect("exchange grant client peers");
        let Some(peer) = peers.get(&receiver).cloned() else {
            return;
        };
        let empty = {
            let mut watches = peer.watches.lock().expect("exchange grant client watches");
            if watches.entries.remove(&stream).is_some() {
                peer.revision.fetch_add(1, Ordering::Release);
            }
            watches.entries.is_empty()
        };
        if empty {
            peer.stop.cancel();
            peers.remove(&receiver);
        }
        drop(peers);
        peer.changed.notify_one();
    }
}

impl Drop for ExchangeGrantClients {
    fn drop(&mut self) {
        for peer in self
            .peers
            .lock()
            .expect("exchange grant client peers")
            .values()
        {
            peer.stop.cancel();
        }
    }
}

async fn reconnect_delay(peer: &ClientPeer) -> bool {
    tokio::select! {
        biased;
        _ = peer.stop.cancelled() => false,
        _ = tokio::time::sleep(RECONNECT_BACKOFF) => true,
    }
}

async fn run_client(
    runtime: BackendDataRuntime,
    timeout: Duration,
    generations: Arc<AtomicU64>,
    peer: Arc<ClientPeer>,
) {
    while !peer.stop.is_cancelled() {
        crate::backend_metrics::exchange_grant_event("rebuild");
        let generation =
            match generations.fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                old.checked_add(1)
            }) {
                Ok(old) => NonZeroU64::new(old + 1).expect("checked nonzero generation"),
                Err(_) => {
                    peer.refuse_all(exchange_failure(FailureClass::Resource, 454));
                    return;
                }
            };
        peer.generation.store(generation.get(), Ordering::Release);
        let revision = peer.revision.load(Ordering::Acquire);
        let subscription = codec::DecodedExchangeGrantSubscription {
            pair: peer.pair,
            generation,
        };
        let client = NativeRpcClient::new_backend_endpoint(
            runtime.clone(),
            peer.destination.native_endpoint().clone(),
            peer.pair.receiver,
        );
        let request = codec::encode_grant_subscription(subscription);
        let mut stream = match client
            .subscribe_exchange_grants_async(request, timeout, peer.stop.clone())
            .await
        {
            Ok(stream) => stream,
            Err(status) => {
                if let Some(fact) = status_failure(&status) {
                    peer.refuse_all(fact);
                    return;
                }
                if !reconnect_delay(&peer).await {
                    return;
                }
                continue;
            }
        };
        let mut snapshot = true;
        let mut seen = BTreeSet::new();
        loop {
            if revision != peer.revision.load(Ordering::Acquire) {
                break;
            }
            let message = tokio::select! {
                biased;
                _ = peer.stop.cancelled() => return,
                _ = peer.changed.notified() => { continue; }
                message = stream.message() => message,
            };
            let message = match message {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(status) => {
                    if let Some(fact) = status_failure(&status) {
                        peer.refuse_all(fact);
                        return;
                    }
                    break;
                }
            };
            if message.generation < generation.get() {
                continue;
            }
            let batch = match codec::decode_grant_batch(
                &message,
                subscription,
                FieldPath::root("exchange_grant_batch"),
            ) {
                Ok(batch) => batch,
                Err(_) => {
                    peer.refuse_all(subscription_contract_failure());
                    return;
                }
            };
            if batch.snapshot != snapshot {
                peer.refuse_all(subscription_contract_failure());
                return;
            }
            if snapshot {
                let watches = peer.watches.lock().expect("exchange grant client watches");
                seen.extend(
                    batch
                        .states
                        .iter()
                        .filter(|state| watches.entries.contains_key(&state.stream))
                        .map(|state| state.stream),
                );
            }
            peer.apply(generation, batch.states);
            if batch.snapshot_complete {
                snapshot = false;
                peer.resynchronize_missing(generation, &seen);
                seen.clear();
            }
        }
        if !reconnect_delay(&peer).await {
            return;
        }
    }
}

#[cfg(test)]
#[path = "exchange_grant_subscription_tests.rs"]
mod tests;
