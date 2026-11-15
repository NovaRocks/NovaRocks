// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original result queues, decoded receipts and domain source capabilities.
//! Logical decisions arrive as effects; no domain completeness rule lives here.

use crate::api::*;
use crate::coordination::LogicalExecutionActorError;
use futures::{FutureExt, future::Shared};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptComputationCut, MonotonicInstant, QueryFailureReason,
};
use novarocks_execution_contract::{
    ResultPacketSequence, SafeDetail, TaskIdentity, failure::FailureProjection,
    root_result::RootResultEnd,
};
use novarocks_logical_execution_protocol::{
    LogicalRequestToken, LogicalSourceId,
    effect::{ConsumerEffect, InternalResponse},
    input::{ConsumerFact, FailureReport, InternalEndReceiptId, InternalRequest},
    start::{LogicalPlanIdentity, RootCarrier},
};
use novarocks_types::{QueryExecutionId, QueryId};
use novarocks_workload_control::WorkCancellationRequester;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex, MutexGuard, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, oneshot, watch};

type Key = (u64, QueryExecutionId, u64);
fn key(a: AttemptActivationIdentity) -> Key {
    (a.actor().get(), a.execution(), a.generation())
}
type Exit = Shared<LogicalFuture<'static, Option<Result<(), QueryExecutionError>>>>;
type Liveness = Shared<LogicalFuture<'static, ()>>;
type SegmentReceipt = Shared<LogicalFuture<'static, Option<ResultDeliveryDisposition>>>;
#[derive(Default)]
struct SegmentState {
    accepted: u64,
    delivered_through: Option<u64>,
    flight: Option<(u64, SegmentReceipt)>,
}

/// A projection implemented by the original observation owner at composition.
/// It may report only observations actually published by that owner.
pub trait ConsumerObservationPort: Send + Sync {
    fn diagnostics(
        &self,
        activation: AttemptActivationIdentity,
        deadline: MonotonicInstant,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>>;
    fn convergence(
        &self,
        activation: AttemptActivationIdentity,
        all_participations_closed: bool,
        remote_tracking_ended: bool,
        local_resources_exited: bool,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>>;
}

#[derive(Clone)]
enum Transport {
    Client(QueryResultTransport),
    Internal(QueryResultTransport<InternalResultDelivery>),
}
pub(crate) enum ConsumerSlot {
    Client(ResultQueuePermit),
    Internal(ResultQueuePermit<InternalResultDelivery>),
}
impl Transport {
    async fn reserve(&self) -> Result<ConsumerSlot, QueryExecutionError> {
        match self {
            Self::Client(t) => t.reserve_owned().await.map(ConsumerSlot::Client),
            Self::Internal(t) => t.reserve_owned().await.map(ConsumerSlot::Internal),
        }
    }
    fn segment(
        &self,
        slot: ConsumerSlot,
        delivery: RootSegmentDelivery,
    ) -> Result<(), QueryExecutionError> {
        match (self, slot) {
            (Self::Client(t), ConsumerSlot::Client(s)) => {
                t.enqueue(s, ResultDelivery::Segment(delivery));
            }
            (Self::Internal(t), ConsumerSlot::Internal(s)) => {
                t.enqueue(
                    s,
                    InternalResultDelivery::Segment(InternalSegmentDelivery::from_root(delivery)),
                );
            }
            _ => {
                return Err(protocol(
                    "result position belongs to another original consumer",
                ));
            }
        }
        Ok(())
    }
}
struct InternalEndState {
    end: RootResultEnd,
    receipt: SegmentReceipt,
    checked: Option<InternalEndAcknowledgement>,
    outcome: Option<Result<(), QueryExecutionError>>,
    acknowledgement_lost: bool,
    settling: Arc<AsyncMutex<()>>,
}
struct Internal {
    seal: Arc<InternalConsumerSeal>,
    transport: Transport,
    failure: watch::Sender<Option<QueryExecutionError>>,
    ready: watch::Sender<Option<Result<ComputationReadyToken, QueryExecutionError>>>,
    // The exact accepted token in the original no-await publication tail.
    // This is not a second payload, authorization or actual-exit fact.
    ready_issued: Option<ComputationReadyToken>,
    failure_issued: Option<QueryExecutionError>,
    liveness: Liveness,
    exit: Option<Exit>,
    decoded: Option<(InternalEndReceiptId, RootResultEnd)>,
    end_delivery: Option<InternalEndState>,
}
/// The original channel pair and the failure already promised under State.
/// This tail carries no new result decision and performs no async wait.
struct InternalFailureNotice {
    failure: watch::Sender<Option<QueryExecutionError>>,
    ready: watch::Sender<Option<Result<ComputationReadyToken, QueryExecutionError>>>,
    error: QueryExecutionError,
}
impl InternalFailureNotice {
    fn reserve(original: &mut Internal, error: QueryExecutionError) -> Self {
        Self {
            failure: original.failure.clone(),
            ready: original.ready.clone(),
            error: original.failure_issued.get_or_insert(error).clone(),
        }
    }
    fn notify(self) {
        notify_original_failure(&self.failure, self.error.clone());
        self.ready.send_if_modified(|value| {
            if value.is_none() {
                *value = Some(Err(self.error));
                true
            } else {
                false
            }
        });
    }
}
fn notify_original_failure(
    sender: &watch::Sender<Option<QueryExecutionError>>,
    error: QueryExecutionError,
) {
    sender.send_if_modified(|value| {
        if value.is_none() {
            *value = Some(error);
            true
        } else {
            false
        }
    });
}
enum Reply {
    Internal(oneshot::Sender<Result<InternalResponse, QueryExecutionError>>),
    Ready(oneshot::Sender<Result<ComputationReadyToken, QueryExecutionError>>),
}
struct Pending {
    source: LogicalSourceId,
    activation: AttemptActivationIdentity,
    reply: Reply,
}
struct Completion {
    requester: WorkCancellationRequester,
    reply: Option<oneshot::Sender<Result<ExecutionOutputObligation, QueryExecutionError>>>,
    published: Option<Result<(), QueryExecutionError>>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ClientEndPublication {
    #[default]
    Owed,
    // The original no-await handoff tail owns the position and End. Failure
    // may not guess its outcome from a temporarily absent queue permit.
    Publishing,
    HandedOff,
}
struct State {
    next: u64,
    source_id: Option<LogicalSourceId>,
    pending: BTreeMap<LogicalRequestToken, Pending>,
    internal: BTreeMap<Key, Internal>,
    client: Option<Transport>,
    schema: Option<ResultDeliveryReceipt>,
    failure: Option<watch::Sender<Option<QueryExecutionError>>>,
    client_failure_issued: Option<QueryExecutionError>,
    output: Option<QueryResultStream>,
    exposed: bool,
    end: BTreeMap<Key, RootResultEnd>,
    end_slots: BTreeMap<Key, ConsumerSlot>,
    retired_end: BTreeSet<Key>,
    segments: BTreeMap<Key, SegmentState>,
    completion: Option<Completion>,
    supervision: Option<QueryExecutionError>,
    client_end_publication: ClientEndPublication,
}
fn reserve_client_failure(
    state: &mut State,
    error: QueryExecutionError,
) -> Option<(
    watch::Sender<Option<QueryExecutionError>>,
    QueryExecutionError,
)> {
    if state.client_end_publication != ClientEndPublication::Owed {
        return None;
    }
    state.failure.clone().map(|sender| {
        let original = state.client_failure_issued.get_or_insert(error).clone();
        (sender, original)
    })
}

pub struct ConsumerExecutor {
    carrier: Option<RootCarrier>,
    plan: Option<PlanSeal>,
    capacity: Option<NonZeroUsize>,
    internal_mode: bool,
    state: Mutex<State>,
    source: AsyncMutex<Option<Box<dyn LogicalFactSource>>>,
    observations: Option<Arc<dyn ConsumerObservationPort>>,
}
impl ConsumerExecutor {
    pub fn new_client(
        query: QueryId,
        schema: ResultSchema,
        carrier: RootCarrier,
        plan: PlanSeal,
        capacity: NonZeroUsize,
        observations: Option<Arc<dyn ConsumerObservationPort>>,
    ) -> Result<Self, QueryExecutionError> {
        let row = ResultRowCarrier::relayed(carrier.kind, carrier.client_rows)?;
        let (transport, receipt, failure, output) =
            QueryResultStream::try_channel(query, schema, row, capacity.get())?;
        Ok(Self::new(
            Some(carrier),
            Some(plan),
            Some(capacity),
            false,
            Some(transport),
            Some(receipt),
            Some(failure),
            Some(output),
            observations,
        ))
    }
    pub fn new_internal(
        carrier: RootCarrier,
        plan: PlanSeal,
        capacity: NonZeroUsize,
        observations: Option<Arc<dyn ConsumerObservationPort>>,
    ) -> Result<Self, QueryExecutionError> {
        ResultRowCarrier::relayed(carrier.kind, carrier.client_rows)?;
        Ok(Self::new(
            Some(carrier),
            Some(plan),
            Some(capacity),
            true,
            None,
            None,
            None,
            None,
            observations,
        ))
    }
    /// Consumes the original Query Application reply, whose receiver already
    /// belongs to the statement start. Completion owns no row/schema channel.
    pub fn new_completion(
        requester: WorkCancellationRequester,
        original_reply: oneshot::Sender<Result<ExecutionOutputObligation, QueryExecutionError>>,
    ) -> Self {
        let owner = Self::new(None, None, None, false, None, None, None, None, None);
        owner.lock().completion = Some(Completion {
            requester,
            reply: Some(original_reply),
            published: None,
        });
        owner
    }
    #[allow(clippy::too_many_arguments)]
    fn new(
        carrier: Option<RootCarrier>,
        plan: Option<PlanSeal>,
        capacity: Option<NonZeroUsize>,
        internal_mode: bool,
        client: Option<QueryResultTransport>,
        schema: Option<ResultDeliveryReceipt>,
        failure: Option<watch::Sender<Option<QueryExecutionError>>>,
        output: Option<QueryResultStream>,
        observations: Option<Arc<dyn ConsumerObservationPort>>,
    ) -> Self {
        Self {
            carrier,
            plan,
            capacity,
            internal_mode,
            state: Mutex::new(State {
                next: 1,
                source_id: None,
                pending: BTreeMap::new(),
                internal: BTreeMap::new(),
                client: client.map(Transport::Client),
                schema,
                failure,
                client_failure_issued: None,
                output,
                exposed: false,
                end: BTreeMap::new(),
                end_slots: BTreeMap::new(),
                retired_end: BTreeSet::new(),
                segments: BTreeMap::new(),
                completion: None,
                supervision: None,
                client_end_publication: ClientEndPublication::Owed,
            }),
            source: AsyncMutex::new(None),
            observations,
        }
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Latch the exact original owner failure separately from normal business
    /// failure. Actual Ready/completion/End handoffs remain immutable. This
    /// does not settle a request, delivery, future, capture or original Join.
    pub fn begin_supervised_failure(&self, error: QueryExecutionError) {
        let (error, failure, internal, completion) = {
            let mut state = self.lock();
            let error = state.supervision.get_or_insert(error).clone();
            let failure = reserve_client_failure(&mut state, error.clone());
            let internal = state
                .internal
                .values_mut()
                .filter(|original| original.ready_issued.is_none())
                .map(|original| InternalFailureNotice::reserve(original, error.clone()))
                .collect::<Vec<_>>();
            let completion = state.completion.as_mut().and_then(|original| {
                if original.published.is_none() {
                    original.published = Some(Err(error.clone()));
                    original.reply.take()
                } else {
                    None
                }
            });
            (error, failure, internal, completion)
        };
        // All notifications and original sender destruction happen outside
        // the custody lock. New publication checks the same latched level.
        if let Some((failure, original)) = failure {
            notify_original_failure(&failure, original);
        }
        for original in internal {
            original.notify();
        }
        if let Some(reply) = completion {
            let _ = reply.send(Err(error));
        }
    }

    /// Drain at most one exact original request. A lost caller does not create
    /// a replacement reply or prove that its producer has actually exited.
    pub fn fail_one_supervised_request(&self) -> bool {
        let row = {
            let mut state = self.lock();
            let Some(error) = state.supervision.clone() else {
                return false;
            };
            state
                .pending
                .pop_first()
                .map(|(_, original)| (original.reply, error))
        };
        let Some((reply, error)) = row else {
            return false;
        };
        match reply {
            Reply::Internal(reply) => {
                let _ = reply.send(Err(error));
            }
            Reply::Ready(reply) => {
                let _ = reply.send(Err(error));
            }
        }
        true
    }
    /// This controlled observer waits on the actual channel. Scope cancellation
    /// is requested by the logical owner after the passive close observation.
    pub fn observe_client_closed(&self) -> LogicalFuture<'_, Result<(), LogicalSourceClosed>> {
        Box::pin(async move {
            let transport = self.lock().client.clone();
            let Some(Transport::Client(transport)) = transport else {
                return Err(LogicalSourceClosed);
            };
            transport.closed().await;
            let mut source = self.source.lock().await;
            let source = source.as_mut().ok_or(LogicalSourceClosed)?;
            source.send(LogicalHostFact::ConsumerClosed).await
        })
    }
    pub(crate) fn carrier(&self) -> Result<RootCarrier, QueryExecutionError> {
        self.carrier
            .ok_or_else(|| protocol("completion-only execution has no row carrier"))
    }
    pub(crate) fn is_internal(&self) -> bool {
        self.internal_mode
    }
    pub async fn attach_source(
        &self,
        source: Box<dyn LogicalFactSource>,
    ) -> Result<(), QueryExecutionError> {
        let mut original = self.source.lock().await;
        if original.is_some() {
            return Err(protocol("consumer source was already attached"));
        }
        {
            let mut state = self.lock();
            if let Some(error) = &state.supervision {
                return Err(error.clone());
            }
            state.source_id = Some(source.id());
        }
        *original = Some(source);
        Ok(())
    }
    /// The caller retains this exact stream through statement orchestration.
    pub fn take_exposed_output(&self) -> Option<QueryResultStream> {
        let mut s = self.lock();
        if s.exposed { s.output.take() } else { None }
    }
    pub(crate) fn bind_internal(
        self: &Arc<Self>,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
    ) -> Result<InternalConsumerActivation, QueryExecutionError> {
        if !self.internal_mode || root.query_execution_id() != activation.execution() {
            return Err(protocol("internal consumer activation has another root"));
        }
        let mut state = self.lock();
        if let Some(error) = &state.supervision {
            return Err(error.clone());
        }
        if state.internal.contains_key(&key(activation)) {
            return Err(protocol("internal consumer was already bound"));
        }
        let (Some(carrier), Some(plan), Some(capacity)) = (self.carrier, self.plan, self.capacity)
        else {
            return Err(protocol(
                "internal consumer has no original row configuration",
            ));
        };
        let seal = Arc::new(InternalConsumerSeal {
            activation,
            root,
            plan,
        });
        let (transport, failure, stream) = InternalResultStream::channel(
            Arc::clone(&seal),
            ResultRowCarrier::relayed(carrier.kind, carrier.client_rows)?,
            capacity,
        );
        let port = RuntimeReadyPort {
            owner: Arc::downgrade(self),
            seal: Arc::clone(&seal),
        };
        let (source, liveness) = ComputationReadySource::issued_runtime(port);
        let liveness = (Box::pin(async move {
            let _ = liveness.await;
        }) as LogicalFuture<'static, ()>)
            .shared();
        let (ready, receiver) = watch::channel(None);
        state.internal.insert(
            key(activation),
            Internal {
                seal,
                transport: Transport::Internal(transport),
                failure,
                ready,
                ready_issued: None,
                failure_issued: None,
                liveness,
                exit: None,
                decoded: None,
                end_delivery: None,
            },
        );
        Ok(InternalConsumerActivation {
            stream,
            source,
            ready: receiver,
        })
    }
    pub(crate) async fn register_internal(
        &self,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
    ) -> Result<(), QueryExecutionError> {
        match self
            .request(
                activation,
                InternalRequest::RegisterSource {
                    root,
                    plan: plan_identity(
                        self.plan
                            .ok_or_else(|| protocol("internal source has no original plan"))?,
                    ),
                    supervised_exit: true,
                },
            )
            .await?
        {
            InternalResponse::SourceRegistered => Ok(()),
            InternalResponse::Rejected(e) => Err(e),
            _ => Err(protocol("internal registration received another reply")),
        }
    }
    fn transport(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<Transport, QueryExecutionError> {
        let state = self.lock();
        if self.internal_mode {
            state
                .internal
                .get(&key(activation))
                .map(|i| i.transport.clone())
        } else {
            state.client.clone()
        }
        .ok_or_else(|| protocol("original consumer transport is absent"))
    }
    pub(crate) async fn reserve_segment(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<ConsumerSlot, QueryExecutionError> {
        match self.transport(activation)?.reserve().await {
            Ok(slot) => Ok(slot),
            Err(_) if self.internal_mode => {
                self.consumer_dropped(activation).await?;
                Err(protocol("original internal channel closed"))
            }
            Err(error) => Err(error),
        }
    }
    /// Actual segment receipt, never an executor acknowledgement, advances ACK.
    pub(crate) fn deliver_segment(
        &self,
        activation: AttemptActivationIdentity,
        slot: ConsumerSlot,
        delivery: RootSegmentDelivery,
        receipt: ResultDeliveryReceipt,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        // The original receipt receiver stays in the stable single-flight
        // position even if the caller stops waiting. There is no new receiver.
        let staged = (|| {
            let transport = self.transport(activation)?;
            let mut state = self.lock();
            let original = state.segments.entry(key(activation)).or_default();
            if delivery.execution_id() != activation.execution()
                || delivery.sequence().get() != original.accepted
                || original.flight.is_some()
            {
                return Err(protocol(
                    "segment is foreign, gapped or another original delivery is in flight",
                ));
            }
            let following = original
                .accepted
                .checked_add(1)
                .ok_or_else(|| protocol("original data sequence exhausted"))?;
            let sequence = delivery.sequence().get();
            let receipt = (Box::pin(async move { receipt.await.ok() })
                as LogicalFuture<'static, Option<ResultDeliveryDisposition>>)
                .shared();
            original.flight = Some((sequence, receipt));
            transport.segment(slot, delivery)?;
            original.accepted = following;
            Ok(())
        })();
        Box::pin(async move {
            staged?;
            self.settle_segment(activation).await
        })
    }
    /// Reentry consumes the same receipt after a cancelled waiter. Actual
    /// failure/drop never advances the durable delivered watermark.
    pub(crate) async fn settle_segment(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), QueryExecutionError> {
        let (sequence, receipt) = {
            let state = self.lock();
            let original = state
                .segments
                .get(&key(activation))
                .ok_or_else(|| protocol("segment settlement has no original position"))?;
            match &original.flight {
                Some((sequence, receipt)) => (*sequence, receipt.clone()),
                None => return Ok(()),
            }
        };
        let result = match receipt.await {
            Some(ResultDeliveryDisposition::Completed) => Ok(()),
            Some(ResultDeliveryDisposition::Failed(error)) => Err(*error),
            _ if self.internal_mode => self.consumer_dropped(activation).await,
            _ => Err(novarocks_logical_execution_protocol::failure::result_delivery_failed()),
        };
        let mut state = self.lock();
        let original = state
            .segments
            .get_mut(&key(activation))
            .expect("original segment position retained");
        if original
            .flight
            .as_ref()
            .is_some_and(|(pending, _)| *pending == sequence)
        {
            if result.is_ok() {
                original.delivered_through = Some(sequence);
            }
            original.flight.take();
        }
        result
    }
    #[cfg(test)]
    pub(crate) fn delivered_through(&self, activation: AttemptActivationIdentity) -> Option<u64> {
        self.lock()
            .segments
            .get(&key(activation))
            .and_then(|s| s.delivered_through)
    }
    /// Revokes only this attempt's unused EOF position. A reserve that was
    /// already waiting rechecks the same tombstone before publishing its slot.
    pub(crate) fn retire_pending_end(&self, activation: AttemptActivationIdentity) {
        let mut state = self.lock();
        state.retired_end.insert(key(activation));
        state.end_slots.remove(&key(activation));
    }
    pub(crate) fn record_end(
        &self,
        activation: AttemptActivationIdentity,
        end: RootResultEnd,
    ) -> Result<(), QueryExecutionError> {
        let mut state = self.lock();
        if state
            .end
            .get(&key(activation))
            .is_some_and(|previous| *previous != end)
        {
            return Err(protocol("original End changed within one activation"));
        }
        state.end.insert(key(activation), end);
        Ok(())
    }
    /// Provisional Internal End crosses the real domain channel before Ready.
    #[cfg(test)]
    pub(crate) async fn deliver_internal_end(
        &self,
        activation: AttemptActivationIdentity,
        end: RootResultEnd,
    ) -> Result<(), QueryExecutionError> {
        let slot = self.reserve_segment(activation).await?;
        self.deliver_internal_end_in(activation, end, slot).await
    }
    pub(crate) fn deliver_internal_end_in(
        &self,
        activation: AttemptActivationIdentity,
        end: RootResultEnd,
        slot: ConsumerSlot,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        let staged = (|| {
            let transport = self.transport(activation)?;
            let mut state = self.lock();
            let original = state
                .internal
                .get_mut(&key(activation))
                .ok_or_else(|| protocol("internal End has no original source"))?;
            if original.end_delivery.is_some() {
                return Err(protocol("internal End was handed off twice"));
            }
            let (delivery, receipt, checked) =
                InternalEndDelivery::new(Arc::clone(&original.seal), end);
            let receipt = (Box::pin(async move { receipt.await.ok() })
                as LogicalFuture<'static, Option<ResultDeliveryDisposition>>)
                .shared();
            original.end_delivery = Some(InternalEndState {
                end,
                receipt,
                checked: Some(checked),
                outcome: None,
                acknowledgement_lost: false,
                settling: Arc::new(AsyncMutex::new(())),
            });
            match (transport, slot) {
                (Transport::Internal(t), ConsumerSlot::Internal(s)) => {
                    t.enqueue(s, InternalResultDelivery::End(delivery));
                }
                _ => return Err(protocol("internal End has another consumer position")),
            }
            Ok(())
        })();
        Box::pin(async move {
            staged?;
            self.settle_internal_end(activation).await
        })
    }
    /// The receipt and checked acknowledgement stay in the original activation
    /// record. Stop can resume this settlement after the Start waiter vanished.
    pub(crate) async fn settle_internal_end(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), QueryExecutionError> {
        let gate = {
            let state = self.lock();
            let Some(end) = state
                .internal
                .get(&key(activation))
                .and_then(|i| i.end_delivery.as_ref())
            else {
                return Ok(());
            };
            Arc::clone(&end.settling)
        };
        let _settling = gate.lock().await;
        let (end, seal, receipt, acknowledgement_lost) = {
            let state = self.lock();
            let original = state
                .internal
                .get(&key(activation))
                .ok_or_else(|| protocol("original internal source disappeared"))?;
            let delivery = original
                .end_delivery
                .as_ref()
                .ok_or_else(|| protocol("original internal End disappeared"))?;
            if let Some(outcome) = &delivery.outcome {
                return outcome.clone();
            }
            (
                delivery.end,
                Arc::clone(&original.seal),
                delivery.receipt.clone(),
                delivery.acknowledgement_lost,
            )
        };
        if acknowledgement_lost {
            let answer = self.consumer_dropped(activation).await;
            self.lock()
                .internal
                .get_mut(&key(activation))
                .and_then(|i| i.end_delivery.as_mut())
                .expect("original End retained")
                .outcome = Some(answer.clone());
            return answer;
        }
        let result = match receipt.await {
            Some(ResultDeliveryDisposition::Completed) => {
                let id = {
                    let mut state = self.lock();
                    let existing = state.internal.get(&key(activation)).and_then(|i| i.decoded);
                    if let Some((id, held)) = existing {
                        if held != end {
                            return Err(protocol(
                                "internal End changed during original settlement",
                            ));
                        }
                        id
                    } else {
                        let id = InternalEndReceiptId(next(&mut state)?);
                        state
                            .internal
                            .get_mut(&key(activation))
                            .expect("original source retained")
                            .decoded = Some((id, end));
                        id
                    }
                };
                match self
                    .request(activation, InternalRequest::EndDecoded { receipt: id, end })
                    .await
                {
                    Ok(InternalResponse::Applied) => {
                        Ok(DecodedInternalEndReceipt::issued(seal, end))
                    }
                    Ok(InternalResponse::Rejected(e)) | Err(e) => Err(e),
                    _ => Err(protocol("decoded End received another reply")),
                }
            }
            Some(ResultDeliveryDisposition::Failed(e)) => Err(*e),
            _ => match self.consumer_dropped(activation).await {
                Err(error) => Err(error),
                Ok(()) => Err(protocol("internal End was dropped")),
            },
        };
        let mut answer = result.as_ref().map(|_| ()).map_err(Clone::clone);
        let (checked, success) = {
            let mut state = self.lock();
            let delivery = state
                .internal
                .get_mut(&key(activation))
                .and_then(|i| i.end_delivery.as_mut())
                .expect("original End retained");
            (delivery.checked.take(), result.is_ok())
        };
        if let Some(checked) = checked
            && checked.send(result).is_err()
            && success
        {
            self.lock()
                .internal
                .get_mut(&key(activation))
                .and_then(|i| i.end_delivery.as_mut())
                .expect("original End retained")
                .acknowledgement_lost = true;
            answer = self.consumer_dropped(activation).await;
        }
        self.lock()
            .internal
            .get_mut(&key(activation))
            .and_then(|i| i.end_delivery.as_mut())
            .expect("original End retained")
            .outcome = Some(answer.clone());
        answer
    }
    /// Independently driven after Stop's synchronous admission cut. These are
    /// the original physical receipts, not the canceled Start's stack futures.
    pub(crate) async fn settle_deliveries(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), QueryExecutionError> {
        let segment = self
            .lock()
            .segments
            .get(&key(activation))
            .is_some_and(|s| s.flight.is_some());
        let first = if segment {
            self.settle_segment(activation).await.err()
        } else {
            None
        };
        let end = self.settle_internal_end(activation).await.err();
        first.or(end).map_or(Ok(()), Err)
    }
    pub(crate) fn deliveries_settled(&self, activation: AttemptActivationIdentity) -> bool {
        let state = self.lock();
        state
            .segments
            .get(&key(activation))
            .is_none_or(|s| s.flight.is_none())
            && state
                .internal
                .get(&key(activation))
                .and_then(|i| i.end_delivery.as_ref())
                .is_none_or(|e| e.outcome.is_some())
    }
    async fn consumer_dropped(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), QueryExecutionError> {
        let exit = self
            .lock()
            .internal
            .get(&key(activation))
            .and_then(|i| i.exit.clone())
            .ok_or_else(|| {
                protocol("internal delivery dropped without its reserved exit verdict")
            })?;
        let outcome = exit.await;
        let error = match &outcome {
            Some(Err(e)) => e.clone(),
            _ => protocol("internal consumer exited before consuming its original delivery"),
        };
        let _ = self
            .request(activation, InternalRequest::ExitObserved { outcome })
            .await?;
        Err(error)
    }
    pub(crate) async fn observe_internal_source_exit(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), QueryExecutionError> {
        let liveness = self
            .lock()
            .internal
            .get(&key(activation))
            .map(|i| i.liveness.clone())
            .ok_or_else(|| protocol("source observation has no original consumer"))?;
        liveness.await;
        self.request(activation, InternalRequest::SourceClosed)
            .await?;
        self.consumer_dropped(activation).await
    }
    pub(crate) async fn wait_internal_ready(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<ComputationReadyToken, QueryExecutionError> {
        let (mut ready, liveness) = {
            let state = self.lock();
            let i = state
                .internal
                .get(&key(activation))
                .ok_or_else(|| protocol("ready wait has no original source"))?;
            (i.ready.subscribe(), i.liveness.clone())
        };
        tokio::pin!(liveness);
        loop {
            if let Some(result) = ready.borrow().clone() {
                return result;
            }
            tokio::select! {
                biased;
                changed = ready.changed() => { if changed.is_err() { return Err(protocol("original Ready observation closed")); } }
                _ = &mut liveness => {
                    if let Some(result) = ready.borrow().clone() { return result; }
                    self.request(activation, InternalRequest::SourceClosed).await?;
                    return self.consumer_dropped(activation).await.and_then(|()| Err(protocol("internal source closed before Ready")));
                }
            }
        }
    }
    async fn send(
        &self,
        activation: AttemptActivationIdentity,
        request: InternalRequest,
        reply: Reply,
    ) -> Result<LogicalRequestToken, QueryExecutionError> {
        let token = {
            let mut state = self.lock();
            if let Some(error) = &state.supervision {
                return Err(error.clone());
            }
            let source = state
                .source_id
                .ok_or_else(|| protocol("original consumer source is not attached"))?;
            let token = LogicalRequestToken::new(next(&mut state)?);
            state.pending.insert(
                token,
                Pending {
                    source,
                    activation,
                    reply,
                },
            );
            token
        };
        let result = {
            let mut source = self.source.lock().await;
            let source = source
                .as_mut()
                .ok_or_else(|| protocol("original consumer source disappeared"))?;
            source
                .send(LogicalHostFact::Internal {
                    token,
                    activation,
                    request,
                })
                .await
        };
        if result.is_err() {
            let (original, error) = {
                let mut state = self.lock();
                (state.pending.remove(&token), state.supervision.clone())
            };
            drop(original);
            return Err(
                error.unwrap_or_else(|| protocol("original consumer standing position closed"))
            );
        }
        Ok(token)
    }
    async fn request(
        &self,
        activation: AttemptActivationIdentity,
        request: InternalRequest,
    ) -> Result<InternalResponse, QueryExecutionError> {
        let (reply, receiver) = oneshot::channel();
        self.send(activation, request, Reply::Internal(reply))
            .await?;
        receiver.await.map_err(|_| {
            self.lock()
                .supervision
                .clone()
                .unwrap_or_else(|| protocol("original internal request reply closed"))
        })?
    }
    fn reply(
        &self,
        source: LogicalSourceId,
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        response: InternalResponse,
    ) -> Result<(), QueryExecutionError> {
        let mut state = self.lock();
        if let Some(error) = &state.supervision {
            return Err(error.clone());
        }
        let p = state
            .pending
            .get(&token)
            .ok_or_else(|| protocol("internal reply has no original request"))?;
        if p.source != source || p.activation != activation {
            return Err(protocol(
                "internal reply belongs to another source or activation",
            ));
        }
        let p = state
            .pending
            .remove(&token)
            .expect("validated original request");
        drop(state);
        match p.reply {
            Reply::Internal(reply) => {
                let _ = reply.send(Ok(response));
            }
            Reply::Ready(reply) => {
                let error = match response {
                    InternalResponse::Rejected(e) => e,
                    _ => protocol("Ready request did not receive its accepted cut"),
                };
                let _ = reply.send(Err(error));
            }
        }
        Ok(())
    }
    fn publish_ready(
        &self,
        source: LogicalSourceId,
        token: LogicalRequestToken,
        cut: AttemptComputationCut,
    ) -> Result<(), QueryExecutionError> {
        let mut state = self.lock();
        if let Some(error) = &state.supervision {
            return Err(error.clone());
        }
        let p = state
            .pending
            .get(&token)
            .ok_or_else(|| protocol("Ready cut has no original request"))?;
        if p.source != source
            || p.activation != cut.activation
            || !matches!(p.reply, Reply::Ready(_))
        {
            return Err(protocol("Ready cut belongs to another request"));
        }
        let i = state
            .internal
            .get_mut(&key(cut.activation))
            .ok_or_else(|| protocol("Ready cut has no original source"))?;
        if i.seal.root != cut.root || i.decoded.map(|(_, e)| e) != Some(cut.end) {
            return Err(protocol("Ready cut differs from the original decoded End"));
        }
        if let Some(error) = &i.failure_issued {
            return Err(error.clone());
        }
        let accepted = ComputationReadyToken::from_runtime_cut(cut);
        i.ready_issued = Some(accepted);
        let ready = i.ready.clone();
        let p = state
            .pending
            .remove(&token)
            .expect("validated original Ready request");
        drop(state);
        // The same accepted token and original reply own this no-await tail.
        // A reentrant failure notification observes that retained tail, while
        // no callback can run under the consumer custody lock.
        ready.send_replace(Some(Ok(accepted)));
        if let Reply::Ready(reply) = p.reply {
            let _ = reply.send(Ok(accepted));
        }
        Ok(())
    }
    pub fn execute(&self, effect: ConsumerEffect) -> LogicalFuture<'_, ConsumerFact> {
        Box::pin(async move {
            let result: Result<ConsumerFact, QueryExecutionError> = async {
                if !matches!(
                    effect,
                    ConsumerEffect::PublishDiagnostics { .. }
                        | ConsumerEffect::PublishConvergence { .. }
                ) && let Some(error) = self.lock().supervision.clone()
                {
                    return Err(error);
                }
                match effect {
                    ConsumerEffect::WriteSchema { .. } => {
                        let receipt =
                            self.lock().schema.take().ok_or_else(|| {
                                protocol("original schema receipt was already taken")
                            })?;
                        consumed(receipt).await?;
                        Ok(ConsumerFact::SchemaWritten)
                    }
                    ConsumerEffect::ReserveEndSlot { activation } => {
                        let slot = self.transport(activation)?.reserve().await?;
                        let mut state = self.lock();
                        if let Some(error) = &state.supervision {
                            return Err(error.clone());
                        }
                        if state.retired_end.contains(&key(activation)) {
                            return Err(protocol(
                                "original End position was revoked before reservation completed",
                            ));
                        }
                        if state.end_slots.contains_key(&key(activation)) {
                            return Err(protocol("original End position was reserved twice"));
                        }
                        state.end_slots.insert(key(activation), slot);
                        Ok(ConsumerFact::EndSlotReady)
                    }
                    ConsumerEffect::EnqueueEnd { activation } => {
                        let (transport, slot, end) = {
                            let mut state = self.lock();
                            if let Some(error) = &state.supervision {
                                return Err(error.clone());
                            }
                            if let Some(error) = &state.client_failure_issued {
                                return Err(error.clone());
                            }
                            if state.client_end_publication != ClientEndPublication::Owed {
                                return Err(protocol(
                                    "original successful End handoff already started",
                                ));
                            }
                            if !matches!(
                                state.end_slots.get(&key(activation)),
                                Some(ConsumerSlot::Client(_))
                            ) {
                                return Err(protocol(
                                    "successful client EOF has no original client position",
                                ));
                            }
                            let end = *state.end.get(&key(activation)).ok_or_else(|| {
                                protocol("successful client EOF has no original End")
                            })?;
                            let transport = state.client.clone().ok_or_else(|| {
                                protocol("successful client EOF has no original channel")
                            })?;
                            state.client_end_publication = ClientEndPublication::Publishing;
                            (
                                transport,
                                state.end_slots.remove(&key(activation)).ok_or_else(|| {
                                    protocol("successful client EOF has no reserved position")
                                })?,
                                end,
                            )
                        };
                        let (delivery, receipt) = EndDelivery::success_eof_with_root_rows(
                            activation.execution(),
                            ResultPacketSequence::new(end.sequence.get() - 1),
                            Some(end.output_rows),
                        );
                        match (transport, slot) {
                            (Transport::Client(t), ConsumerSlot::Client(s)) => {
                                t.enqueue(s, ResultDelivery::End(delivery));
                                self.lock().client_end_publication =
                                    ClientEndPublication::HandedOff;
                            }
                            _ => {
                                return Err(protocol(
                                    "successful client EOF has another consumer position",
                                ));
                            }
                        }
                        consumed(receipt).await?;
                        Ok(ConsumerFact::EndSettled)
                    }
                    ConsumerEffect::ExposeOutput { .. } => {
                        let mut state = self.lock();
                        if let Some(error) = &state.supervision {
                            return Err(error.clone());
                        }
                        if state.exposed || state.output.is_none() {
                            return Err(protocol(
                                "original output was already exposed or is absent",
                            ));
                        }
                        state.exposed = true;
                        Ok(ConsumerFact::OutputExposed)
                    }
                    ConsumerEffect::PublishCompletion => {
                        let mut state = self.lock();
                        if let Some(error) = &state.supervision {
                            return Err(error.clone());
                        }
                        let original = state.completion.as_mut().ok_or_else(|| {
                            protocol("completion publication has no original completion reply")
                        })?;
                        if let Some(Err(error)) = &original.published {
                            return Err(error.clone());
                        }
                        let publication = if original.published.is_none() {
                            let reply = original
                                .reply
                                .take()
                                .ok_or_else(|| protocol("original completion reply is absent"))?;
                            original.published = Some(Ok(()));
                            let output = ExecutionOutputObligation::new(
                                original.requester.clone(),
                                ExecutionOutput::Completion,
                            );
                            Some((reply, output))
                        } else {
                            None
                        };
                        drop(state);
                        if let Some((reply, output)) = publication {
                            let _ = reply.send(Ok(output));
                        }
                        Ok(ConsumerFact::Applied)
                    }
                    ConsumerEffect::PublishAttemptFailure { activation, error } => {
                        let original = {
                            let mut state = self.lock();
                            if let Some(error) = &state.supervision {
                                return Err(error.clone());
                            }
                            let original =
                                state.internal.get_mut(&key(activation)).ok_or_else(|| {
                                    protocol("attempt failure has no original internal source")
                                })?;
                            original
                                .ready_issued
                                .is_none()
                                .then(|| InternalFailureNotice::reserve(original, error))
                        };
                        if let Some(original) = original {
                            original.notify();
                        }
                        Ok(ConsumerFact::Applied)
                    }
                    ConsumerEffect::PublishFailure(error) => {
                        let (completion, failure, internal) = {
                            let mut state = self.lock();
                            if let Some(error) = &state.supervision {
                                return Err(error.clone());
                            }
                            let completion = state.completion.as_mut().and_then(|original| {
                                if original.published.is_none() {
                                    original.published = Some(Err(error.clone()));
                                    original.reply.take()
                                } else {
                                    None
                                }
                            });
                            let failure = reserve_client_failure(&mut state, error.clone());
                            let internal = state
                                .internal
                                .values_mut()
                                .filter(|original| original.ready_issued.is_none())
                                .map(|original| {
                                    InternalFailureNotice::reserve(original, error.clone())
                                })
                                .collect::<Vec<_>>();
                            (completion, failure, internal)
                        };
                        if let Some(reply) = completion {
                            let _ = reply.send(Err(error));
                        }
                        if let Some((failure, original)) = failure {
                            notify_original_failure(&failure, original);
                        }
                        for original in internal {
                            original.notify();
                        }
                        Ok(ConsumerFact::Applied)
                    }
                    ConsumerEffect::InternalReply {
                        source,
                        token,
                        activation,
                        response,
                    } => {
                        self.reply(source, token, activation, response)?;
                        Ok(ConsumerFact::Applied)
                    }
                    ConsumerEffect::PublishReady { source, token, cut } => {
                        self.publish_ready(source, token, cut)?;
                        Ok(ConsumerFact::ReadyHandedOff)
                    }
                    ConsumerEffect::PublishDiagnostics {
                        activation,
                        deadline,
                    } => {
                        self.observations
                            .as_ref()
                            .ok_or_else(|| {
                                protocol("original diagnostic publication owner is not configured")
                            })?
                            .diagnostics(activation, deadline)
                            .await?;
                        Ok(ConsumerFact::DiagnosticsPublished)
                    }
                    ConsumerEffect::PublishConvergence {
                        activation,
                        all_participations_closed,
                        remote_tracking_ended,
                        local_resources_exited,
                    } => {
                        self.observations
                            .as_ref()
                            .ok_or_else(|| {
                                protocol("original convergence publication owner is not configured")
                            })?
                            .convergence(
                                activation,
                                all_participations_closed,
                                remote_tracking_ended,
                                local_resources_exited,
                            )
                            .await?;
                        Ok(ConsumerFact::ConvergencePublished)
                    }
                }
            }
            .await;
            result.unwrap_or_else(ConsumerFact::Failed)
        })
    }
}

/// The original seal remains strong; the executor reference is weak so an
/// active domain source cannot keep the installed execution alive in a cycle.
pub(crate) struct RuntimeReadyPort {
    owner: Weak<ConsumerExecutor>,
    seal: Arc<InternalConsumerSeal>,
}
impl RuntimeReadyPort {
    pub(crate) fn origin(&self) -> &InternalConsumerSeal {
        &self.seal
    }
    pub(crate) async fn accept(
        &self,
        decoded: DecodedInternalEndReceipt,
    ) -> Result<ComputationReadyToken, QueryExecutionError> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| protocol("original consumer owner closed"))?;
        let (id, end) = owner
            .lock()
            .internal
            .get(&key(self.seal.activation))
            .and_then(|i| i.decoded)
            .ok_or_else(|| protocol("Ready proposal has no original decoded receipt"))?;
        if !decoded.matches(&self.seal, end) {
            return Err(protocol(
                "Ready proposal belongs to another original consumer",
            ));
        }
        let (reply, receiver) = oneshot::channel();
        owner
            .send(
                self.seal.activation,
                InternalRequest::AcceptReady {
                    root: self.seal.root,
                    plan: plan_identity(self.seal.plan),
                    receipt: id,
                    end,
                },
                Reply::Ready(reply),
            )
            .await?;
        receiver
            .await
            .map_err(|_| protocol("original Ready reply closed"))?
    }
    pub(crate) async fn fail(&self, error: QueryExecutionError) -> Result<(), QueryExecutionError> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| protocol("original consumer owner closed"))?;
        match owner
            .request(self.seal.activation, InternalRequest::Failed(report(error)))
            .await?
        {
            InternalResponse::Rejected(e) => Err(e),
            _ => Ok(()),
        }
    }
    pub(crate) async fn supervise_exit(
        &self,
        receiver: oneshot::Receiver<Result<(), QueryExecutionError>>,
    ) -> Result<(), LogicalExecutionActorError> {
        let owner = self
            .owner
            .upgrade()
            .ok_or(LogicalExecutionActorError::MailboxClosed)?;
        let mut state = owner.lock();
        let i = state
            .internal
            .get_mut(&key(self.seal.activation))
            .ok_or(LogicalExecutionActorError::WrongExecution)?;
        if !Arc::ptr_eq(&i.seal, &self.seal) || i.exit.is_some() {
            return Err(LogicalExecutionActorError::InvariantViolation);
        }
        i.exit = Some(
            (Box::pin(async move { receiver.await.ok() })
                as LogicalFuture<'static, Option<Result<(), QueryExecutionError>>>)
                .shared(),
        );
        Ok(())
    }
}
fn next(state: &mut State) -> Result<NonZeroU64, QueryExecutionError> {
    let next = NonZeroU64::new(state.next)
        .ok_or_else(|| protocol("consumer request identity exhausted"))?;
    state.next = state
        .next
        .checked_add(1)
        .ok_or_else(|| protocol("consumer request identity exhausted"))?;
    Ok(next)
}
fn plan_identity(plan: PlanSeal) -> LogicalPlanIdentity {
    match plan {
        PlanSeal::Version(version) => LogicalPlanIdentity(*version.as_bytes()),
    }
}
pub(crate) fn report(error: QueryExecutionError) -> FailureReport {
    FailureReport {
        original_attempt_failure: error.execution_failure().cloned().map(Box::new),
        error,
        pre_ready_replacement: None,
    }
}
pub(crate) fn protocol(detail: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(
        novarocks_coordination_contract::ExecutionFailureInput::new(
            QueryFailureReason::ResultProtocol.failure_fact(),
            SafeDetail::truncating(detail),
            novarocks_coordination_contract::ExecutionFailureSite::Coordinator,
        ),
    )
}
async fn consumed(receipt: ResultDeliveryReceipt) -> Result<(), QueryExecutionError> {
    match receipt.await {
        Ok(ResultDeliveryDisposition::Completed) => Ok(()),
        Ok(ResultDeliveryDisposition::Failed(e)) => Err(*e),
        _ => Err(novarocks_logical_execution_protocol::failure::result_delivery_failed()),
    }
}

#[cfg(test)]
#[path = "consumer/tests.rs"]
mod tests;
