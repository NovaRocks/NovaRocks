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

//! Composition of the original admission and SendBatch Sources.

use super::{
    ActorSendReports, ActorTransportExecutor, AdmissionSource, AttemptCallSource,
    AttemptSubscriptions, BodyOutcome, FrozenSendRequests, NativeProgressInput, NativeReceiptInput,
    PreparedActorSend, SendContext, SendKey, SendMemberReport, SendReportRows,
    call_source::{AttemptCallReply, TransportRelease},
    releases::{ReleaseCapture, TransportReleases},
    send_reports::close_unlaunched,
};
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect},
        host::{
            EffectBindRefusal, EffectExecutor, EffectSource, PendingHandoff, PreparedEffectJob,
            SourceProgress,
        },
        transport_withdrawal::AttemptEffectEnvelope,
    },
    task_execution::bridge::abort::{
        AbortInputRetirement, AbortReceiptSlot, DrainedAbortSubmissions,
    },
    task_execution::host::custody::{ReturnedActorTransportResult, TransportSourceEvent},
    task_execution::host::failure_publication::FailureContext,
    task_execution::host::failure_supervisor::{
        FailureCleanupPort, FailureSupervisor, RunningFailureSupervisor,
    },
    task_execution::host::publication::OriginalHostFailure,
};
use novarocks_attempt_protocol::{
    effect::{EffectBody, OperationIntent},
    failure::HostFailureRef,
    input::SendGeneration,
    start::PayloadRef,
};
use novarocks_control_runtime::{
    actor_turn::{
        ActorHostTurn, ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket,
        TimerSourceState,
    },
    causal_input::CausalInputObligation,
    exit_custody::ExitCustody,
    obligation::WorkRetirementObserver,
    reported_work::ReportedWork,
    transaction::ScopeHandle,
};
use novarocks_control_runtime_core::{
    identity::{ActorId, InputId, Tick},
    mailbox::InputClass,
    scheduler::SchedulerError,
};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::TaskOperationId;
use novarocks_types::BackendProcessId;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Weak},
    task::{Poll, Waker},
};

type Position = CausalInputObligation<FrontendActor>;

#[derive(Clone, Copy, Default)]
pub(crate) struct NativeReportReferences {
    pub(crate) filter: Option<PayloadRef>,
    pub(crate) workload: Option<HostFailureRef>,
}

pub(crate) struct AttemptSendSource {
    pub(crate) requests: FrozenSendRequests,
    pub(crate) contexts: Arc<BTreeMap<BackendProcessId, SendContext>>,
    pub(crate) report_references:
        BTreeMap<(TaskOperationId, SendGeneration), NativeReportReferences>,
    pub(crate) report_author: ReportedWork,
    pub(crate) idle: [VecDeque<Position>; 2],
}

enum Prepaid {
    Admission,
    Call(usize),
    OpenSubscription,
    Send(usize, Box<PreparedActorSend>),
    Other,
}

struct SendInFlight {
    lane: usize,
    header: WorkRetirementObserver,
    result: Option<ReturnedActorTransportResult>,
    retired: bool,
    abort_keys: Vec<(TaskOperationId, SendGeneration)>,
}

pub(crate) struct AttemptFailureBinding {
    pub(crate) supervisor: FailureSupervisor,
    pub(crate) port: Box<dyn FailureCleanupPort>,
    pub(crate) abort_drain: Option<tokio::sync::oneshot::Receiver<DrainedAbortSubmissions>>,
}

enum Supervisor {
    Armed(AttemptFailureBinding),
    Running(RunningFailureSupervisor),
}

#[must_use = "the failure custodian owns the original send result and header"]
pub(crate) struct FailedSendResult {
    pub(crate) effect: AttemptActionId,
    pub(crate) original: ReturnedActorTransportResult,
    pub(crate) header: WorkRetirementObserver,
}

pub(crate) struct AttemptTransportSource {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    native_source: ActorId,
    scope: ScopeHandle,
    admission: AdmissionSource,
    calls: [Option<AttemptCallSource>; 6],
    subscriptions: Option<AttemptSubscriptions>,
    requests: FrozenSendRequests,
    contexts: Arc<BTreeMap<BackendProcessId, SendContext>>,
    report_references: BTreeMap<(TaskOperationId, SendGeneration), NativeReportReferences>,
    report_author: Option<ReportedWork>,
    send_idle: [VecDeque<Position>; 2],
    send_live: BTreeMap<AttemptActionId, SendInFlight>,
    abort_receipts: BTreeMap<(TaskOperationId, SendGeneration), Weak<AbortReceiptSlot>>,
    failed_sends: BTreeMap<AttemptActionId, Vec<(TaskOperationId, SendGeneration)>>,
    send_ready: VecDeque<AttemptActionId>,
    send_scan: usize,
    admission_scan: usize,
    call_scan: usize,
    pending: Option<Prepaid>,
    failure: Option<FailureContext>,
    supervisor: Option<Supervisor>,
    abort_drain: Option<tokio::sync::oneshot::Receiver<DrainedAbortSubmissions>>,
    abort_inputs: VecDeque<AbortInputRetirement>,
    abort_scan: usize,
    in_flight: BTreeSet<AttemptActionId>,
    external_closed: bool,
    releases: Option<TransportReleases>,
    other: Box<dyn EffectSource>,
}

impl AttemptTransportSource {
    pub(crate) fn new(
        runtime: ActorRuntime<FrontendActor>,
        registration: ActorRegistration,
        native_source: ActorId,
        scope: ScopeHandle,
        admission: AdmissionSource,
        calls: [Option<AttemptCallSource>; 6],
        subscriptions: Option<AttemptSubscriptions>,
        send: AttemptSendSource,
        mut failure: (FailureContext, AttemptFailureBinding),
        other: Box<dyn EffectSource>,
    ) -> Self {
        assert!(registration.matches_scope(&scope) && admission.matches_scope(&scope));
        assert!(
            calls
                .iter()
                .flatten()
                .all(|source| source.matches_scope(&scope))
        );
        assert!(scope.same_scope(send.report_author.work().scope()));
        let capacity = send.idle.iter().map(VecDeque::len).sum();
        Self {
            runtime,
            registration,
            native_source,
            scope,
            admission,
            calls,
            subscriptions,
            requests: send.requests,
            contexts: send.contexts,
            report_references: send.report_references,
            report_author: Some(send.report_author),
            send_idle: send.idle,
            send_live: BTreeMap::new(),
            abort_receipts: BTreeMap::new(),
            failed_sends: BTreeMap::new(),
            send_ready: VecDeque::with_capacity(capacity),
            send_scan: 0,
            admission_scan: 0,
            call_scan: 0,
            pending: None,
            failure: Some(failure.0),
            abort_drain: failure.1.abort_drain.take(),
            abort_inputs: VecDeque::new(),
            abort_scan: 0,
            supervisor: Some(Supervisor::Armed(failure.1)),
            in_flight: BTreeSet::new(),
            external_closed: false,
            releases: None,
            other,
        }
    }

    pub(crate) fn with_releases(mut self, executor: tokio::runtime::Handle, wake: Waker) -> Self {
        assert!(self.calls[3].is_some());
        self.releases = Some(TransportReleases::new(executor, wake));
        self
    }

    fn advance_supervisor(&mut self, turn: &ActorHostTurn<FrontendActor>) -> bool {
        let closing = self.result_admission_closed()
            && self
                .calls
                .iter()
                .flatten()
                .all(AttemptCallSource::is_drained);
        match self.supervisor.as_mut() {
            Some(Supervisor::Armed(_)) if closing => {
                let actor = self.registration.actor();
                if self
                    .runtime
                    .snapshot(actor)
                    .expect("original attempt")
                    .failed
                    || !matches!(
                        self.runtime.timer_source_state(actor),
                        Ok(TimerSourceState::ClosedDrained)
                    )
                {
                    return false;
                }
                match turn.seal_drained() {
                    Ok(()) => {}
                    Err(
                        ActorTurnError::ActorNotRetired
                        | ActorTurnError::Scheduler(SchedulerError::Busy),
                    ) => return false,
                    Err(error) => std::panic::resume_unwind(Box::new(error)),
                }
                let Some(Supervisor::Armed(original)) = self.supervisor.take() else {
                    unreachable!()
                };
                self.supervisor = Some(Supervisor::Running(
                    original.supervisor.close_unused(original.port),
                ));
                // Poll the new Join on the next turn to register this host's wake.
                true
            }
            Some(Supervisor::Running(running)) => match running.progress() {
                Poll::Pending => false,
                Poll::Ready(Ok(())) => {
                    self.supervisor.take();
                    if let Some(mut handoff) = self.abort_drain.take() {
                        let failed = self
                            .runtime
                            .snapshot(self.registration.actor())
                            .expect("original attempt registration")
                            .failed;
                        match handoff.try_recv() {
                            Ok(drained) => {
                                assert!(failed && self.abort_inputs.is_empty());
                                self.abort_inputs = drained.retirements;
                                if !drained.errors.is_empty() {
                                    self.other
                                        .retain_transport_failure(Box::new(drained.errors));
                                }
                            }
                            Err(tokio::sync::oneshot::error::TryRecvError::Closed) if !failed => {}
                            Err(error) => std::panic::resume_unwind(Box::new(error)),
                        }
                    }
                    true
                }
                Poll::Ready(Err(error)) => {
                    self.other
                        .retain_transport_failure(Box::new((error, running.panic().clone())));
                    false
                }
            },
            _ => false,
        }
    }

    fn reserve_reports(
        &self,
        effect: &AttemptEffectEnvelope,
    ) -> Result<Arc<ActorSendReports>, ActorTurnError> {
        let EffectBody::SendBatch { batch, .. } = &effect.protocol.body else {
            unreachable!()
        };
        let work = self
            .report_author
            .as_ref()
            .expect("original live report author")
            .work();
        let reserve = || {
            let position = self.runtime.reserve_admitted_input_for(
                &self.registration,
                self.native_source,
                InputClass::Lifecycle,
                work,
            )?;
            position.into_standing_input().map_err(|(error, original)| {
                original
                    .revoke_unused()
                    .expect("unused original report position");
                error
            })
        };
        let started = NativeProgressInput::new(reserve()?);
        let mut members = Vec::with_capacity(batch.operations.len());
        for (operation, grant) in batch.operations.iter().zip(&batch.queued_inputs) {
            let key = SendKey {
                operation: operation.operation_id(),
                generation: grant.generation(),
            };
            let member = (|| {
                let mut receipt = NativeReceiptInput::new(key, operation.kind(), reserve()?);
                let first_poll = match operation {
                    OperationIntent::EstablishParticipation { participation, .. } => {
                        match reserve() {
                            Ok(position) => {
                                Some((*participation, NativeProgressInput::new(position)))
                            }
                            Err(error) => {
                                assert!(matches!(
                                    receipt.retire_ready_after_exit(),
                                    Ok(std::task::Poll::Ready(None))
                                ));
                                return Err(error);
                            }
                        }
                    }
                    _ => None,
                };
                let references = self
                    .report_references
                    .get(&(key.operation, key.generation))
                    .copied()
                    .unwrap_or_default();
                let mut member = SendMemberReport::new(
                    receipt,
                    references.filter,
                    references.workload,
                    first_poll,
                );
                if let OperationIntent::AbortParticipation(request) = operation {
                    member.retain_abort_receipt(request.participation());
                }
                Ok(member)
            })();
            match member {
                Ok(member) => members.push(member),
                Err(error) => {
                    // These positions were never handed to a producer.
                    close_unlaunched(Arc::new(ActorSendReports::new(
                        effect.protocol.id,
                        SendReportRows {
                            started,
                            members: members.into_boxed_slice(),
                        },
                    )));
                    return Err(error);
                }
            }
        }
        Ok(Arc::new(ActorSendReports::new(
            effect.protocol.id,
            SendReportRows {
                started,
                members: members.into_boxed_slice(),
            },
        )))
    }

    fn prepare_send(&mut self, effect: &Arc<AttemptEffectEnvelope>) -> Option<Position> {
        let EffectBody::SendBatch { batch, .. } = &effect.protocol.body else {
            unreachable!()
        };
        let lane = usize::from(batch.control_progress);
        if self.send_idle[lane].is_empty() {
            return None;
        }
        let context = self
            .contexts
            .get(&batch.backend)
            .expect("original frozen send target")
            .clone();
        let reports = match self.reserve_reports(effect) {
            Ok(reports) => reports,
            Err(ActorTurnError::InputCapacity) => return None,
            Err(_) if self.scope.gates().is_ok_and(|gates| gates.failed) => return None,
            Err(error) => std::panic::resume_unwind(Box::new(error)),
        };
        let prepared = PreparedActorSend::prepare(
            effect.clone(),
            &self.requests,
            &self.admission,
            context,
            reports,
        )
        .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)))
        .with_lifecycle(
            self.failure
                .as_ref()
                .expect("live attempt lifecycle")
                .lifecycle
                .starts(),
        );
        self.pending = Some(Prepaid::Send(lane, Box::new(prepared)));
        self.send_idle[lane].pop_front()
    }

    fn advance_send(&mut self) -> bool {
        let effect = self
            .send_ready
            .pop_front()
            .expect("original send result scan");
        let row = self.send_live.get_mut(&effect).expect("same original send");
        let ready = row.retired
            && row.result.as_ref().is_some_and(|result| {
                result
                    .original
                    .retire_ready_after_exit()
                    .expect("original send result")
            })
            && self
                .scope
                .reap_observed_header(&row.header)
                .expect("original send header");
        if !ready {
            self.send_ready.push_back(effect);
            return false;
        }
        let row = self.send_live.remove(&effect).expect("same retired send");
        for key in row.abort_keys {
            self.abort_receipts.remove(&key);
        }
        let result = row.result.unwrap();
        self.send_idle[row.lane].push_back(result.original);
        true
    }
}

impl EffectSource for AttemptTransportSource {
    fn prepay_result(&mut self, effect: &FrontendEffect) -> Option<Position> {
        assert!(self.pending.is_none());
        if let FrontendEffect::Attempt(original) = effect {
            if let EffectBody::ReleaseCreationPayload { payload, .. } = original.protocol.body
                && !self.admission.can_release_creation(payload)
            {
                return None;
            }
            let call_lane = match original {
                original if original.is_qa_call() => Some(0),
                original if original.is_process_call() => Some(1),
                original if original.is_publication_call() => Some(2),
                original if original.is_transport_release() => Some(3),
                original if original.is_read_confirmation() => Some(4),
                original if original.is_subscription_control() => Some(5),
                _ => None,
            };
            if let Some(lane) = call_lane {
                let result = self.calls[lane]
                    .as_mut()
                    .expect("original attempt call Source")
                    .prepay(original)
                    .expect("original call prepayment")?;
                self.pending = Some(Prepaid::Call(lane));
                return Some(result);
            }
            match original.protocol.body {
                EffectBody::AdmitQueuedInput { .. } => {
                    let result = self
                        .admission
                        .prepay(original)
                        .expect("original admission prepayment")?;
                    self.pending = Some(Prepaid::Admission);
                    return Some(result);
                }
                EffectBody::SendBatch { .. } => return self.prepare_send(original),
                EffectBody::OpenSubscription(_) => {
                    let result = self
                        .subscriptions
                        .as_mut()
                        .expect("original subscription Source")
                        .prepay(original)?;
                    self.pending = Some(Prepaid::OpenSubscription);
                    return Some(result);
                }
                _ => {}
            }
        }
        let result = self.other.prepay_result(effect)?;
        self.pending = Some(Prepaid::Other);
        Some(result)
    }

    fn bind_job(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
    ) -> PreparedEffectJob {
        self.try_bind_job(runtime, turn, ticket, result)
            .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)))
    }

    fn try_bind_job(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
    ) -> Result<PreparedEffectJob, EffectBindRefusal> {
        match self.pending.take().expect("same original prepayment") {
            Prepaid::Call(lane) => {
                let FrontendEffect::Attempt(effect) = &ticket.effect else {
                    unreachable!()
                };
                if let EffectBody::Port(port) = &effect.protocol.body
                    && crate::task_execution::bridge::root_control::reply_kind(&port.body).is_some()
                {
                    use novarocks_coordination_contract::AttemptToLogicalBody;
                    let failure = match &port.body {
                        AttemptToLogicalBody::SealResult {
                            result: Err(failure),
                            ..
                        }
                        | AttemptToLogicalBody::TerminalControlResult {
                            result: Err(failure),
                            ..
                        } => {
                            crate::task_execution::bridge::terminal::host_failure_reference(failure)
                        }
                        _ => None,
                    };
                    let projection = failure.and_then(|reference| {
                        let input = self
                            .admission
                            .project_failure(reference)
                            .or_else(|| self.subscriptions.as_ref()?.project_failure(reference))
                            .or_else(|| {
                                self.calls
                                    .iter()
                                    .flatten()
                                    .find_map(|source| source.project_failure(reference))
                            })?;
                        Some((reference, input))
                    });
                    let source = self.calls[lane].as_mut().unwrap();
                    if let Err((error, original)) =
                        source.commit_root_reply(runtime, ticket, port, projection)
                    {
                        source.return_unclaimed(result);
                        return Err(EffectBindRefusal {
                            error,
                            original: Box::new(original),
                        });
                    }
                }
                assert!(self.in_flight.insert(effect.protocol.id));
                if effect.is_transport_release() {
                    let release = || match effect.protocol.body {
                        EffectBody::ReleaseQueuedInput { grant } => self
                            .admission
                            .release_grant(grant)
                            .map(TransportRelease::QueuedInput),
                        EffectBody::ReleaseCreationPayload { task, payload } => self
                            .admission
                            .take_creation(task, payload)
                            .map(|seed| TransportRelease::Creation {
                                request: self.requests.retire_creation(task, payload),
                                seed,
                            }),
                        _ => unreachable!("original transport release"),
                    };
                    return Ok(self.calls[lane]
                        .as_mut()
                        .unwrap()
                        .bind_with_release(ticket, result, release));
                }
                let receipt = match &effect.protocol.body {
                    EffectBody::Port(port) => match port.body {
                        novarocks_coordination_contract::AttemptToLogicalBody::AbortSettlement {
                            operation_id, receipt_generation: Some(generation), ..
                        } => self.abort_receipts.get(&(operation_id, SendGeneration::new(generation))).and_then(Weak::upgrade),
                        _ => None,
                    },
                    _ => None,
                };
                Ok(self.calls[lane]
                    .as_mut()
                    .unwrap()
                    .bind_with_abort_receipt(ticket, result, receipt))
            }
            Prepaid::OpenSubscription => {
                let FrontendEffect::Attempt(effect) = &ticket.effect else {
                    unreachable!()
                };
                assert!(self.in_flight.insert(effect.protocol.id));
                Ok(self
                    .subscriptions
                    .as_mut()
                    .unwrap()
                    .bind_job(ticket, result))
            }
            Prepaid::Admission => {
                let FrontendEffect::Attempt(effect) = &ticket.effect else {
                    unreachable!()
                };
                assert!(self.in_flight.insert(effect.protocol.id));
                Ok(self.admission.bind(ticket, result))
            }
            Prepaid::Other => self.other.try_bind_job(runtime, turn, ticket, result),
            Prepaid::Send(lane, prepared) => {
                let FrontendEffect::Attempt(effect) = &ticket.effect else {
                    unreachable!()
                };
                assert!(self.in_flight.insert(effect.protocol.id));
                let abort_receipts = prepared.abort_receipts();
                let (job, header) = prepared
                    .bind_tracked(runtime, ticket, result, &mut self.admission)
                    .map_err(|original| EffectBindRefusal {
                        error: original.error,
                        original: Box::new(original),
                    })?;
                let abort_keys = abort_receipts
                    .into_iter()
                    .map(|(key, receipt)| {
                        let address = (key.operation, key.generation);
                        assert!(self.abort_receipts.insert(address, receipt).is_none());
                        address
                    })
                    .collect();
                assert!(
                    self.send_live
                        .insert(
                            effect.protocol.id,
                            SendInFlight {
                                lane,
                                header,
                                result: None,
                                retired: false,
                                abort_keys,
                            }
                        )
                        .is_none()
                );
                Ok(job)
            }
        }
    }

    fn return_unclaimed_result(&mut self, result: Position) {
        match self.pending.take().expect("same original prepayment") {
            Prepaid::Call(lane) => self.calls[lane].as_mut().unwrap().return_unclaimed(result),
            Prepaid::Admission => self.admission.return_unclaimed(result),
            Prepaid::OpenSubscription => self
                .subscriptions
                .as_mut()
                .unwrap()
                .return_unclaimed(result),
            Prepaid::Other => self.other.return_unclaimed_result(result),
            Prepaid::Send(lane, prepared) => {
                prepared.revoke_unused_reports();
                self.send_idle[lane].push_back(result);
            }
        }
    }

    fn core_consumed(&mut self, inputs: &[InputId]) {
        if let Some(subscriptions) = self.subscriptions.as_mut() {
            subscriptions.core_consumed(inputs);
        }
        self.other.core_consumed(inputs);
    }

    fn transport_event(&mut self, effect: AttemptActionId, event: TransportSourceEvent) {
        if let Some(subscriptions) = self.subscriptions.as_mut()
            && subscriptions.owns(effect)
        {
            match event {
                TransportSourceEvent::Result(original) => {
                    if let Err(original) = subscriptions.accept(effect, original) {
                        self.other.retain_transport_failure(original);
                    }
                }
                TransportSourceEvent::Retired => {
                    self.in_flight.remove(&effect);
                    subscriptions.transport_retired(effect);
                }
                original => self.other.transport_event(effect, original),
            }
            return;
        }
        if let Some(lane) = self
            .calls
            .iter()
            .position(|source| source.as_ref().is_some_and(|source| source.owns(effect)))
        {
            match event {
                TransportSourceEvent::Result(mut original) => {
                    if let Some(BodyOutcome::Call(AttemptCallReply::Terminal(publication))) =
                        &mut original.outcome
                        && let Some(reference) = publication.host_reference()
                        && let Some(failure) = self
                            .admission
                            .take_failure(reference)
                            .map(OriginalHostFailure::Task)
                            .or_else(|| self.subscriptions.as_ref()?.take_failure(reference))
                            .or_else(|| {
                                self.calls
                                    .iter_mut()
                                    .flatten()
                                    .find_map(|source| source.take_failure(reference))
                            })
                    {
                        publication.attach_host_failure(reference, failure);
                    }
                    let source = self.calls[lane].as_mut().unwrap();
                    if let Err(refused) = source.accept(original) {
                        self.other.retain_transport_failure(Box::new(refused));
                    }
                    if let Some(original) = source.take_refused_root_input() {
                        if let Some(releases) = &mut self.releases {
                            releases.retain_refused_root(Box::new(original));
                        } else {
                            self.other.retain_transport_release(Box::new(original));
                        }
                    }
                }
                TransportSourceEvent::Retired => {
                    assert!(self.in_flight.remove(&effect));
                    self.calls[lane].as_mut().unwrap().transport_retired(effect);
                }
                original => self.other.transport_event(effect, original),
            }
            return;
        }
        match event {
            TransportSourceEvent::Result(result) => {
                if self.send_live.contains_key(&effect) {
                    let resolved = matches!(&result.outcome, Some(BodyOutcome::ReceiptsRecorded))
                        || (matches!(&result.outcome, Some(BodyOutcome::Unused(_)))
                            && matches!(&result.withdrawal, Some(Ok(_))));
                    if resolved {
                        let row = self.send_live.get_mut(&effect).unwrap();
                        assert!(row.result.replace(result).is_none());
                        self.send_ready.push_back(effect);
                    } else {
                        let row = self.send_live.remove(&effect).unwrap();
                        assert!(row.result.is_none());
                        assert!(self.failed_sends.insert(effect, row.abort_keys).is_none());
                        self.other
                            .retain_transport_failure(Box::new(FailedSendResult {
                                effect,
                                original: result,
                                header: row.header,
                            }));
                    }
                } else {
                    if let Err(refused) = self.admission.accept(result, &mut self.requests) {
                        self.other.retain_transport_failure(Box::new(refused));
                    }
                }
            }
            TransportSourceEvent::Retired => {
                self.in_flight.remove(&effect);
                if let Some(row) = self.send_live.get_mut(&effect) {
                    row.retired = true;
                } else if let Some(abort_keys) = self.failed_sends.remove(&effect) {
                    for key in abort_keys {
                        self.abort_receipts.remove(&key);
                    }
                    self.other
                        .transport_event(effect, TransportSourceEvent::Retired);
                } else {
                    assert!(self.admission.transport_retired(effect));
                }
            }
            original => self.other.transport_event(effect, original),
        }
    }

    fn owner_failed_binding(
        &mut self,
        ticket: EffectTicket<FrontendEffect>,
        refused: EffectBindRefusal,
    ) {
        self.other.owner_failed_binding(ticket, refused);
    }

    fn owner_failed_pending(&mut self, pending: PendingHandoff) {
        self.other.owner_failed_pending(pending);
    }

    fn owner_failed_cleanup(
        &mut self,
        ticket: EffectTicket<FrontendEffect>,
        context: Option<FailureContext>,
        observations: Option<
            crate::task_execution::host::observations::host_turn::OrderedObservationFailure,
        >,
    ) {
        assert!(context.is_none());
        let original = self.failure.take().expect("one original failure lifecycle");
        match self.supervisor.take() {
            Some(Supervisor::Armed(binding)) => {
                self.close_external();
                self.supervisor = Some(Supervisor::Running(binding.supervisor.start(
                    ticket,
                    original,
                    std::mem::take(&mut self.in_flight),
                    binding.port,
                    observations,
                )));
            }
            None => std::panic::resume_unwind(Box::new((ticket, original, observations))),
            Some(Supervisor::Running(running)) => {
                std::panic::resume_unwind(Box::new((ticket, original, running, observations)))
            }
        }
    }

    fn close_external(&mut self) {
        if !self.external_closed {
            if let Some(qa) = &self.calls[0] {
                qa.close_abort_ingress();
            }
            self.other.close_external();
            self.external_closed = true;
        }
    }

    fn result_admission_closed(&self) -> bool {
        self.other.result_admission_closed()
    }

    fn progress_retirement(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        now: Tick,
        budget: usize,
    ) -> SourceProgress {
        let mut units = 0;
        let mut progressed = false;
        let mut release_advanced = false;
        if budget != 0
            && let Some(releases) = self.releases.as_mut().filter(|source| source.has_work())
        {
            units += 1;
            match releases.progress(self.calls[3].as_ref().expect("original release author")) {
                Ok(changed) => release_advanced = changed,
                Err(error) => self.other.retain_transport_failure(Box::new(error)),
            }
        }
        let closing = self.result_admission_closed();
        // A terminal already in flight can still claim an exact raw error
        // from any call Source. Dispose unselected rows only after all paid
        // call results retire and no further result loan can be issued.
        let retire_failures = closing
            && self
                .calls
                .iter()
                .flatten()
                .all(AttemptCallSource::is_quiescent);
        for _ in 0..self.calls.len() {
            if units == budget {
                break;
            }
            let lane = self.call_scan;
            self.call_scan = (lane + 1) % self.calls.len();
            let Some(source) = self.calls[lane]
                .as_mut()
                .filter(|source| source.needs_progress(closing, retire_failures))
            else {
                continue;
            };
            units += 1;
            progressed |= source.retire_result().expect("original call result");
            if closing
                && self
                    .releases
                    .as_ref()
                    .is_none_or(TransportReleases::accepts_batch)
            {
                progressed |= source.close_idle().expect("original idle call role");
                if retire_failures && let Some(original) = source.take_unselected_failure() {
                    retain_release(
                        &mut self.releases,
                        &mut self.other,
                        ReleaseCapture::Original(Box::new(original)),
                    );
                    progressed = true;
                }
                if let Some(original) = source.take_drained_owner() {
                    retain_release(
                        &mut self.releases,
                        &mut self.other,
                        ReleaseCapture::Original(Box::new(original)),
                    );
                    progressed = true;
                }
            }
        }
        let release_accepting = self
            .releases
            .as_ref()
            .is_none_or(TransportReleases::accepts_batch);
        let subscription_progress = self.subscriptions.as_mut().map(|subscriptions| {
            let failed = self
                .runtime
                .snapshot(self.registration.actor())
                .expect("original subscription host")
                .failed;
            let allowance = if release_accepting {
                (budget - units).min(1)
            } else {
                0
            };
            subscriptions.progress(
                failed,
                closing || self.external_closed,
                retire_failures,
                allowance,
                |original| {
                    retain_release(&mut self.releases, &mut self.other, original);
                },
            )
        });
        if let Some(progress) = &subscription_progress {
            units += progress.units;
            progressed |= progress.progressed;
        }
        let supervisor_advanced = if units < budget && self.supervisor.is_some() {
            units += 1;
            self.advance_supervisor(turn)
        } else {
            false
        };
        if self.abort_scan == 0 {
            self.abort_scan = self.abort_inputs.len();
        }
        if units < budget && self.abort_scan != 0 {
            self.abort_scan -= 1;
            units += 1;
            let mut original = self
                .abort_inputs
                .pop_front()
                .expect("original drained Abort input");
            if original
                .retire_ready()
                .expect("original Abort input retirement")
            {
                progressed = true;
            } else {
                self.abort_inputs.push_back(original);
            }
        }
        if self.admission_scan == 0 {
            self.admission_scan = self.admission.awaiting_result_retirement();
        }
        if units < budget && self.admission_scan != 0 {
            self.admission_scan -= 1;
            units += 1;
            progressed |= self
                .admission
                .retire_result()
                .expect("original admission result");
        }
        if self.send_scan == 0 {
            self.send_scan = self.send_ready.len();
        }
        if units < budget && self.send_scan != 0 {
            self.send_scan -= 1;
            units += 1;
            progressed |= self.advance_send();
        }
        if self.result_admission_closed() && units < budget {
            if self
                .admission
                .close_one_idle()
                .expect("original idle admission position")
            {
                units += 1;
                progressed = true;
            } else if let Some(positions) = self
                .send_idle
                .iter_mut()
                .find(|positions| !positions.is_empty())
            {
                positions
                    .front_mut()
                    .unwrap()
                    .close_after_exit()
                    .expect("original idle send position");
                positions.pop_front();
                units += 1;
                progressed = true;
            } else if self.send_live.is_empty()
                && self.failed_sends.is_empty()
                && let Some(original) = self.report_author.take()
            {
                let header = original.work().retirement_observer();
                let (custody, exit) = ExitCustody::from_admitted(original);
                exit.revoke_unused_and_exit()
                    .expect("original report author exit");
                assert!(
                    custody
                        .try_retire()
                        .expect("original report author retirement")
                );
                assert!(
                    self.scope
                        .reap_observed_header(&header)
                        .expect("original report author header")
                );
                units += 1;
                progressed = true;
            } else if self.send_live.is_empty()
                && self.failed_sends.is_empty()
                && self.admission.is_drained()
                && !self.requests.is_empty()
                && self
                    .releases
                    .as_ref()
                    .is_none_or(TransportReleases::accepts_batch)
            {
                retain_release(
                    &mut self.releases,
                    &mut self.other,
                    ReleaseCapture::Original(Box::new(std::mem::take(&mut self.requests))),
                );
                units += 1;
                progressed = true;
            }
        }
        let mut progress = self
            .other
            .progress_retirement(runtime, turn, now, budget - units);
        progress.units += units;
        progress.progressed |= progressed || supervisor_advanced || release_advanced;
        progress.more |= subscription_progress
            .as_ref()
            .is_some_and(|progress| release_accepting && progress.more)
            || supervisor_advanced
            || self.abort_scan != 0
            || self.admission_scan != 0
            || self.send_scan != 0
            || release_advanced
            || (budget == 0 || progressed)
                && self
                    .releases
                    .as_ref()
                    .is_some_and(TransportReleases::has_work)
            || progressed && self.result_admission_closed();
        progress.retired &= subscription_progress.is_none_or(|progress| progress.retired)
            && self.supervisor.is_none()
            && self.abort_inputs.is_empty()
            && self
                .calls
                .iter()
                .flatten()
                .all(AttemptCallSource::is_drained)
            && self.report_author.is_none()
            && self.send_live.is_empty()
            && self.failed_sends.is_empty()
            && self.requests.is_empty()
            && self
                .releases
                .as_ref()
                .is_none_or(TransportReleases::is_empty)
            && self.admission.is_drained();
        progress
    }
}

fn retain_release(
    releases: &mut Option<TransportReleases>,
    other: &mut Box<dyn EffectSource>,
    original: ReleaseCapture,
) {
    if let Some(releases) = releases {
        releases.retain(original);
    } else if let ReleaseCapture::Original(original) = original {
        other.retain_transport_release(original);
    } else {
        unreachable!("subscription Source requires its original blocking release owner");
    }
}

pub(crate) struct AttemptTransportExecutor {
    pub(crate) native: ActorTransportExecutor,
    pub(crate) other: Arc<dyn EffectExecutor>,
}
impl EffectExecutor for AttemptTransportExecutor {
    fn handoff(&self, job: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
        if matches!(&job.effect, FrontendEffect::Attempt(effect) if effect.uses_join_custody()) {
            self.native.handoff(job)
        } else {
            self.other.handoff(job)
        }
    }
}
