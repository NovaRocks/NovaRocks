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

//! Ordered Root reads and original consumer receipts. Authorization comes
//! exclusively from the logical owner; transport outcomes do not choose retry.

use super::{
    consumer::{ConsumerExecutor, protocol, report},
    native::NativeExecutor,
};
use crate::{
    api::*,
    coordination::{
        RootRelayBinding, RootRelayStep, RootResultFetchFailure, StreamedRowsComputedCut,
    },
};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_execution_contract::{
    ResultPacketSequence,
    failure::FailureProjection,
    root_result::{RootResultEnd, RootResultRead},
};
use novarocks_logical_execution_protocol::{
    LogicalRequestToken, LogicalSourceId,
    effect::{RelayEffect, RelayResponse},
    input::{RelayFact, RelayRequest},
};
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::{ResultWindowClass, WorkScope};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    sync::{Arc, Mutex, MutexGuard},
    task::Poll,
};
use tokio::sync::{oneshot, watch};

type Key = (u64, QueryExecutionId, u64);
fn key(a: AttemptActivationIdentity) -> Key {
    (a.actor().get(), a.execution(), a.generation())
}
struct Attempt {
    source: Option<Box<dyn LogicalFactSource>>,
    stop: watch::Sender<bool>,
    started: bool,
    cut: Option<StreamedRowsComputedCut>,
    rejected_ack: Option<super::native::RootAckFuture>,
    rejected_consumer: Option<(InternalConsumerActivation, InternalConsumerExitReporter)>,
}
struct Pending {
    source: LogicalSourceId,
    activation: AttemptActivationIdentity,
    reply: oneshot::Sender<RelayResponse>,
}
struct State {
    next: u64,
    attempts: BTreeMap<Key, Attempt>,
    pending: BTreeMap<LogicalRequestToken, Pending>,
    stopped: BTreeSet<Key>,
}
pub struct RelayExecutor {
    scope: WorkScope,
    native: Arc<NativeExecutor>,
    consumer: Arc<ConsumerExecutor>,
    state: Mutex<State>,
}
impl RelayExecutor {
    pub fn new(
        scope: WorkScope,
        native: Arc<NativeExecutor>,
        consumer: Arc<ConsumerExecutor>,
    ) -> Self {
        Self {
            scope,
            native,
            consumer,
            state: Mutex::new(State {
                next: 1,
                attempts: BTreeMap::new(),
                pending: BTreeMap::new(),
                stopped: BTreeSet::new(),
            }),
        }
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Attaches the one original attempt data position, never an Installed Arc.
    pub fn attach_source(
        &self,
        activation: AttemptActivationIdentity,
        source: Box<dyn LogicalFactSource>,
    ) -> Result<(), QueryExecutionError> {
        let mut state = self.lock();
        if state.attempts.contains_key(&key(activation)) {
            return Err(protocol("original relay source was already attached"));
        }
        let (stop, _) = watch::channel(state.stopped.contains(&key(activation)));
        state.attempts.insert(
            key(activation),
            Attempt {
                source: Some(source),
                stop,
                started: false,
                cut: None,
                rejected_ack: None,
                rejected_consumer: None,
            },
        );
        Ok(())
    }
    /// Only the carrier that observed this original Start's actual join may
    /// take and retire the producer cut after its pump frame has exited.
    pub fn take_joined_cut(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Option<StreamedRowsComputedCut> {
        self.lock()
            .attempts
            .get_mut(&key(activation))
            .and_then(|a| a.cut.take())
    }
    pub fn execute(&self, effect: RelayEffect) -> LogicalFuture<'_, RelayFact> {
        Box::pin(async move {
            match effect {
                RelayEffect::Reply {
                    source,
                    token,
                    activation,
                    response,
                } => match self.reply(source, token, activation, response) {
                    Ok(()) => RelayFact::Applied,
                    Err(error) => RelayFact::Failed {
                        activation,
                        all_deliveries_settled: self.consumer.deliveries_settled(activation),
                        failure: report(error),
                    },
                },
                RelayEffect::Stop { activation } => {
                    self.consumer.retire_pending_end(activation);
                    {
                        let mut state = self.lock();
                        state.stopped.insert(key(activation));
                        if let Some(a) = state.attempts.get(&key(activation)) {
                            a.stop.send_replace(true);
                        }
                    }
                    match self.consumer.settle_deliveries(activation).await {
                        Ok(()) => RelayFact::Stopped {
                            activation,
                            all_deliveries_settled: self.consumer.deliveries_settled(activation),
                        },
                        Err(error) => RelayFact::Failed {
                            activation,
                            all_deliveries_settled: self.consumer.deliveries_settled(activation),
                            failure: report(error),
                        },
                    }
                }
                RelayEffect::Start { activation } => match self.start(activation).await {
                    Ok(()) => RelayFact::Stopped {
                        activation,
                        all_deliveries_settled: true,
                    },
                    Err(error) => RelayFact::Failed {
                        activation,
                        all_deliveries_settled: self.consumer.deliveries_settled(activation),
                        failure: error,
                    },
                },
            }
        })
    }
    fn reply(
        &self,
        source: LogicalSourceId,
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        response: RelayResponse,
    ) -> Result<(), QueryExecutionError> {
        consume_reply(&mut self.lock(), source, token, activation, response)
    }
    async fn request(
        &self,
        source: &mut dyn LogicalFactSource,
        activation: AttemptActivationIdentity,
        request: RelayRequest,
    ) -> Result<RelayResponse, QueryExecutionError> {
        relay_request(&self.state, source, activation, request).await
    }
    async fn start(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), novarocks_logical_execution_protocol::input::FailureReport> {
        let (mut source, mut stop) = {
            let mut state = self.lock();
            let a = state
                .attempts
                .get_mut(&key(activation))
                .ok_or_else(|| report(protocol("relay Start has no original source")))?;
            if a.started {
                return Err(report(protocol("original relay was started twice")));
            }
            a.started = true;
            (
                a.source
                    .take()
                    .ok_or_else(|| report(protocol("original relay source was already taken")))?,
                a.stop.subscribe(),
            )
        };
        if *stop.borrow() {
            return Ok(());
        }
        let rows = self
            .native
            .take_rows(activation)
            .map_err(report)?
            .ok_or_else(|| report(protocol("relay Start has no original Native rows")))?;
        let binding = rows.binding;
        let root = binding.frontier.root();
        let carrier = self.consumer.carrier().map_err(report)?;
        validate_binding(&self.scope, &self.consumer, activation, &binding).map_err(report)?;
        if self.consumer.is_internal() {
            let original = self
                .consumer
                .bind_internal(activation, root)
                .map_err(report)?;
            let exit = original.supervise_exit().await.map_err(|error| {
                report(QueryExecutionError::from_execution(
                    novarocks_coordination_contract::ExecutionFailureInput::new(
                        error.failure_fact(),
                        error.safe_detail(),
                        novarocks_coordination_contract::ExecutionFailureSite::Coordinator,
                    ),
                ))
            })?;
            if let Err(error) = self.consumer.register_internal(activation, root).await {
                exit.complete(Err(error.clone()));
                return Err(report(error));
            }
            let mut incoming = Some((original, exit));
            if let Err(error) = self
                .native
                .stage_supervised_consumer_in(activation, &mut incoming)
                .await
            {
                self.lock()
                    .attempts
                    .get_mut(&key(activation))
                    .expect("original relay retained")
                    .rejected_consumer = incoming;
                return Err(report(error));
            }
        }
        let opened = tokio::select! {
            biased;
            _ = stopped(&mut stop) => return Ok(()),
            result = self.request(source.as_mut(), activation, RelayRequest::OpenDelivery { root, carrier }) => result.map_err(report)?,
        };
        if !matches!(
            opened,
            RelayResponse::Opened | RelayResponse::DeliveryGranted
        ) {
            return Err(report(protocol("relay Open received another response")));
        }
        let (end, mut final_ack, summary) = match Self::pump_data(
            &self.state,
            &self.consumer,
            activation,
            source.as_mut(),
            &mut stop,
            binding,
        )
        .await?
        {
            Some(value) => value,
            None => return Ok(()),
        };
        self.consumer.record_end(activation, end).map_err(report)?;
        let consumed = RelayRequest::EndConsumed {
            root,
            end,
            accepted_packets: summary.accepted_packets,
            delivered_through: summary.delivered_through,
        };
        let result = tokio::select! {
            biased;
            _ = stopped(&mut stop) => return Ok(()),
            result = self.request(source.as_mut(), activation, consumed) => result.map_err(report)?,
        };
        if !matches!(result, RelayResponse::Applied) {
            return Err(report(protocol("consumed End received another response")));
        }
        if self.consumer.is_internal() {
            let slot = tokio::select! {
                biased;
                _ = stopped(&mut stop) => return Ok(()),
                result = self.consumer.reserve_segment(activation) => result.map_err(report)?,
            };
            let transfer = {
                let state = self.lock();
                if state.stopped.contains(&key(activation)) {
                    return Ok(());
                }
                self.consumer.deliver_internal_end_in(activation, end, slot)
            };
            tokio::pin!(transfer);
            tokio::select! {
                biased;
                result = &mut transfer => result.map_err(report)?,
                _ = stopped(&mut stop) => { let _ = transfer.await; return Ok(()); },
            }
            tokio::select! {
                biased;
                result = self.consumer.wait_internal_ready(activation) => { let ready = result.map_err(report)?; if ready.activation() != activation || ready.root() != root || ready.end() != end { return Err(report(protocol("accepted Ready differs from original relay End"))); } }
                _ = stopped(&mut stop) => return Ok(()),
            }
        }
        if let Some(ack) = final_ack.take() {
            let mut original = Some(ack.port.read(ack.request, ack.window));
            if let Err(error) = self
                .native
                .stage_root_ack_in(activation, root, &mut original)
            {
                self.lock()
                    .attempts
                    .get_mut(&key(activation))
                    .expect("original relay retained")
                    .rejected_ack = original;
                return Err(report(error));
            }
        }
        if let Some(original) = self.native.streamed_computation_cut_handle().await {
            let cut = StreamedRowsComputedCut::new(original, activation, root);
            self.lock()
                .attempts
                .get_mut(&key(activation))
                .expect("original relay record retained")
                .cut = Some(cut);
        }
        Ok(())
    }
    async fn pump_data(
        state: &Mutex<State>,
        consumer: &ConsumerExecutor,
        activation: AttemptActivationIdentity,
        source: &mut dyn LogicalFactSource,
        stop: &mut watch::Receiver<bool>,
        binding: RootRelayBinding,
    ) -> Result<
        Option<(RootResultEnd, Option<FinalAck>, DeliverySummary)>,
        novarocks_logical_execution_protocol::input::FailureReport,
    > {
        let RootRelayBinding {
            port,
            mut frontier,
            window,
            max_wait,
        } = binding;
        let root = frontier.root();
        let resident = RootRelayResidentWindow::default();
        let mut delivering: Option<(
            NonZeroU64,
            LogicalFuture<'_, Result<(), QueryExecutionError>>,
        )> = None;
        let mut reading: Option<
            LogicalFuture<'static, Result<RetainedRootReply, RootResultFetchFailure>>,
        > = None;
        let mut visible = consumer.is_internal();
        let mut summary = DeliverySummary::default();
        let lifecycle = async {
            if consumer.is_internal() {
                consumer.observe_internal_source_exit(activation).await
            } else {
                std::future::pending().await
            }
        };
        tokio::pin!(lifecycle);
        loop {
            if delivering.is_none()
                && let Some(segment) = resident.take()
            {
                let sequence = segment.sequence;
                let resident_for_delivery = resident.clone();
                // The real queue position is obtained before asking for the
                // first visibility grant. The original bytes remain retained.
                let slot = tokio::select! {
                    biased;
                    _ = stopped(stop) => { resident.freeze(); return Ok(None); }
                    result = consumer.reserve_segment(activation) => result.map_err(report)?,
                };
                if !visible {
                    let granted = tokio::select! {
                        biased;
                        _ = stopped(stop) => { resident.freeze(); return Ok(None); }
                        result = relay_request(state, source, activation, RelayRequest::RequestVisibility) => result.map_err(report)?,
                    };
                    if !matches!(granted, RelayResponse::VisibilityGranted) {
                        return Err(report(protocol(
                            "first visibility received another response",
                        )));
                    }
                    visible = true;
                }
                let (delivery, receipt) = RootSegmentDelivery::try_new(
                    activation.execution(),
                    ResultPacketSequence::new(sequence.get() - 1),
                    segment.reply,
                    segment.client_rows,
                    segment.rows,
                )
                .map_err(report)?;
                let delivery = delivery.with_resident_window(resident_for_delivery);
                let receipt_wait = {
                    let state = lock_state(state);
                    if state.stopped.contains(&key(activation)) {
                        resident.freeze();
                        return Ok(None);
                    }
                    consumer.deliver_segment(activation, slot, delivery, receipt)
                };
                delivering = Some((sequence, receipt_wait));
            }
            if let Some(end) = frontier.end_consumed()
                && delivering.is_none()
                && resident.is_empty()
                && reading.is_none()
            {
                // Keep the optional final ACK unpolled until the logical
                // owner has accepted success or the exact Internal Ready.
                let read = frontier
                    .next_read()
                    .unwrap_or(crate::coordination::RootRelayRead {
                        wanted: None,
                        consumed: end.sequence.get(),
                    });
                let final_ack = if read.wanted.is_none() {
                    RootResultRead::try_new(
                        root,
                        frontier.profile(),
                        frontier.kind(),
                        None,
                        read.consumed,
                        max_wait,
                    )
                    .ok()
                    .map(|request| FinalAck {
                        port: Arc::clone(&port),
                        request,
                        window: window.clone(),
                    })
                } else {
                    None
                };
                return Ok(Some((end, final_ack, summary)));
            }
            if reading.is_none()
                && let Some(read) = frontier.next_read()
                && read.wanted.is_some()
            {
                let request = RootResultRead::try_new(
                    root,
                    frontier.profile(),
                    frontier.kind(),
                    read.wanted,
                    read.consumed,
                    max_wait,
                )
                .map_err(|_| {
                    report(protocol(
                        "root read differs from its frozen request contract",
                    ))
                })?;
                frontier.sent(read);
                reading = Some(paced_read(port.as_ref(), request, window.clone()));
            }
            if delivering.is_none() && reading.is_none() {
                return Err(report(protocol(
                    "original relay has neither read nor delivery",
                )));
            }
            let event = tokio::select! {
                biased;
                _ = stopped(stop) => {
                    resident.freeze();
                    reading.take();
                    // Stop cannot certify an outstanding delivered item. Keep
                    // the original receipt wait until the consumer settles it.
                    if let Some((_, delivery)) = delivering.take() { let _ = delivery.await; }
                    return Ok(None);
                }
                result = &mut lifecycle => {
                    resident.freeze();
                    reading.take();
                    if let Some((_, delivery)) = delivering.take() { let _ = delivery.await; }
                    result.map_err(report)?;
                    return Ok(None);
                }
                event = std::future::poll_fn(|cx| {
                    if let Some((_, f)) = delivering.as_mut() && let Poll::Ready(result) = f.as_mut().poll(cx) { return Poll::Ready(Event::Delivered(result)); }
                    if let Some(f) = reading.as_mut() && let Poll::Ready(result) = f.as_mut().poll(cx) { return Poll::Ready(Event::Read(Box::new(result))); }
                    Poll::Pending
                }) => event,
            };
            match event {
                Event::Delivered(result) => {
                    let (sequence, _) = delivering.take().expect("original delivery completed");
                    resident.retire(sequence);
                    result.map_err(report)?;
                    frontier.receipt(sequence).map_err(|_| {
                        report(protocol("original delivery receipt is out of order"))
                    })?;
                    summary.delivered_through = Some(sequence.get() - 1);
                }
                Event::Read(result) => {
                    reading = None;
                    let reply = (*result).map_err(fetch_report)?;
                    let client_rows = frontier.client_rows();
                    let body = match reply.outcome() {
                        RootReplyView::Data { body, .. } => Some(body),
                        _ => None,
                    };
                    match frontier.accept(reply.response(), body).map_err(|error| report(QueryExecutionError::from_execution(novarocks_coordination_contract::ExecutionFailureInput::new(error.failure_fact(), error.safe_detail(), novarocks_coordination_contract::ExecutionFailureSite::RootRead { root }))))? {
                        RootRelayStep::Deliver { sequence, rows, .. } => {
                            summary.accepted_packets = summary.accepted_packets.checked_add(1).ok_or_else(|| report(protocol("actual accepted packet count overflowed")))?;
                            if !resident.publish(ResidentRootSegment { sequence, reply: Arc::new(reply), client_rows, rows }) { return Err(report(protocol("original resident window rejected its next segment"))); }
                        }
                        RootRelayStep::AwaitTerminalControl => {
                            // A withdrawal is not a terminal verdict. The core
                            // retains exact control authority; no new read/ACK.
                            resident.freeze();
                            reading.take();
                            let wait = relay_request(state, source, activation, RelayRequest::RootWithdrawn { root });
                            tokio::select! { biased; _ = stopped(stop) => {}, result = wait => { result.map_err(report)?; stopped(stop).await; } }
                            if let Some((_, delivery)) = delivering.take() { let _ = delivery.await; }
                            return Ok(None);
                        }
                        RootRelayStep::Refused(refusal) => return Err(report(QueryExecutionError::from_execution(novarocks_coordination_contract::ExecutionFailureInput::new(refusal.fact(), refusal.detail().clone(), novarocks_coordination_contract::ExecutionFailureSite::RootRead { root })))),
                        RootRelayStep::Deferred(_) | RootRelayStep::NotReady | RootRelayStep::EndKnown(_) | RootRelayStep::Acknowledged => {}
                    }
                }
            }
        }
    }
}
fn validate_binding(
    scope: &WorkScope,
    consumer: &ConsumerExecutor,
    activation: AttemptActivationIdentity,
    binding: &RootRelayBinding,
) -> Result<(), QueryExecutionError> {
    let root = binding.frontier.root();
    let carrier = consumer.carrier()?;
    if root.query_execution_id() != activation.execution()
        || !binding.window.is_for_scope(scope)
        || carrier.kind != binding.frontier.kind()
        || carrier.client_rows != binding.frontier.client_rows().map(|(p, _)| p)
        || binding.window.class()
            != if consumer.is_internal() {
                ResultWindowClass::Internal
            } else if carrier.kind == novarocks_result_contract::RootOutputKind::ClientRows {
                ResultWindowClass::Client
            } else {
                ResultWindowClass::Internal
            }
    {
        return Err(protocol(
            "relay identity differs from its original root, carrier or window",
        ));
    }
    Ok(())
}

fn lock_state(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(|e| e.into_inner())
}
async fn relay_request(
    state: &Mutex<State>,
    source: &mut dyn LogicalFactSource,
    activation: AttemptActivationIdentity,
    request: RelayRequest,
) -> Result<RelayResponse, QueryExecutionError> {
    let (reply, receiver) = oneshot::channel();
    let token = {
        let mut state = lock_state(state);
        let value = NonZeroU64::new(state.next)
            .ok_or_else(|| protocol("relay request identity exhausted"))?;
        state.next = state
            .next
            .checked_add(1)
            .ok_or_else(|| protocol("relay request identity exhausted"))?;
        let token = LogicalRequestToken::new(value);
        state.pending.insert(
            token,
            Pending {
                source: source.id(),
                activation,
                reply,
            },
        );
        token
    };
    if source
        .send(LogicalHostFact::Relay {
            token,
            activation,
            request,
        })
        .await
        .is_err()
    {
        lock_state(state).pending.remove(&token);
        return Err(protocol("original relay standing position closed"));
    }
    match receiver
        .await
        .map_err(|_| protocol("original relay reply closed"))?
    {
        RelayResponse::Rejected(e) => Err(e),
        other => Ok(other),
    }
}

#[derive(Default)]
struct DeliverySummary {
    accepted_packets: u64,
    delivered_through: Option<u64>,
}

struct FinalAck {
    port: Arc<dyn BoundedRootReadPort>,
    request: RootResultRead,
    window: novarocks_workload_control::ResultWindowAlias,
}

fn consume_reply(
    state: &mut State,
    source: LogicalSourceId,
    token: LogicalRequestToken,
    activation: AttemptActivationIdentity,
    response: RelayResponse,
) -> Result<(), QueryExecutionError> {
    let p = state
        .pending
        .get(&token)
        .ok_or_else(|| protocol("relay reply has no original request"))?;
    if p.source != source || p.activation != activation {
        return Err(protocol(
            "relay reply belongs to another source or activation",
        ));
    }
    let p = state
        .pending
        .remove(&token)
        .expect("validated original relay request");
    let _ = p.reply.send(response);
    Ok(())
}

enum Event {
    Delivered(Result<(), QueryExecutionError>),
    Read(Box<Result<RetainedRootReply, RootResultFetchFailure>>),
}
async fn stopped(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow_and_update() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}
fn fetch_report(
    failure: RootResultFetchFailure,
) -> novarocks_logical_execution_protocol::input::FailureReport {
    novarocks_logical_execution_protocol::input::FailureReport {
        error: failure.error().clone(),
        original_attempt_failure: Some(Box::new(failure.input().clone())),
        pre_ready_replacement: failure.pre_ready_replacement().cloned(),
    }
}
fn paced_read(
    port: &dyn BoundedRootReadPort,
    request: RootResultRead,
    window: novarocks_workload_control::ResultWindowAlias,
) -> LogicalFuture<'static, Result<RetainedRootReply, RootResultFetchFailure>> {
    let not_before = tokio::time::Instant::now() + request.max_wait();
    let read = port.read(request, window);
    Box::pin(async move {
        let reply = read.await?;
        if matches!(
            reply.outcome(),
            RootReplyView::Deferred(_) | RootReplyView::NotReady
        ) {
            tokio::time::sleep_until(not_before).await;
        }
        Ok(reply)
    })
}

#[cfg(test)]
#[path = "relay/tests.rs"]
mod tests;
