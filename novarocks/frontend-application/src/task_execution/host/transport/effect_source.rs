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
    send_binding::NativeSplitPayloadStore,
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
        AbortIngressRetirement, AbortIngressRetirementProgress, AbortInputRetirement,
        AbortReceiptSlot, AbortSettlementOwner, DrainedAbortSubmissions,
    },
    task_execution::host::RetirementScan,
    task_execution::host::custody::{ReturnedActorTransportResult, TransportSourceEvent},
    task_execution::host::failure_publication::FailureContext,
    task_execution::host::failure_supervisor::{
        FailureCleanupPort, FailureSupervisor, RunningFailureSupervisor,
    },
    task_execution::host::observations::host_turn::OrderedObservationHost,
    task_execution::host::publication::{AttemptDiagnosticSources, OriginalHostFailure},
    task_execution::host::pumps::AttemptPumpSource,
};
use novarocks_attempt_protocol::{
    effect::{EffectBody, OperationIntent},
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
    num::NonZeroU64,
    sync::{Arc, Weak},
    task::{Poll, Waker},
};

type Position = CausalInputObligation<FrontendActor>;

pub(crate) struct AttemptSendSource {
    pub(crate) requests: FrozenSendRequests,
    pub(crate) contexts: Arc<BTreeMap<BackendProcessId, SendContext>>,
    pub(crate) report_author: ReportedWork,
    pub(crate) idle: [VecDeque<Position>; 2],
}

enum Prepaid {
    Admission,
    Call(usize),
    OpenSubscription,
    Send(usize, Box<PreparedActorSend>),
    Pump,
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
#[expect(
    dead_code,
    reason = "An unresolved result and its actual header stay in original failure custody."
)]
pub(crate) struct FailedSendResult {
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
    pumps: Option<AttemptPumpSource>,
    diagnostics: Option<Arc<AttemptDiagnosticSources>>,
    next_payload: Option<NonZeroU64>,
    split_next: bool,
    local_pumps_closing: bool,
    requests: FrozenSendRequests,
    contexts: Arc<BTreeMap<BackendProcessId, SendContext>>,
    report_author: Option<ReportedWork>,
    send_idle: [VecDeque<Position>; 2],
    send_live: BTreeMap<AttemptActionId, SendInFlight>,
    abort_receipts: BTreeMap<(TaskOperationId, SendGeneration), Weak<AbortReceiptSlot>>,
    failed_sends: BTreeMap<AttemptActionId, Vec<(TaskOperationId, SendGeneration)>>,
    send_ready: VecDeque<AttemptActionId>,
    send_scan: RetirementScan,
    admission_scan: RetirementScan,
    call_scan: usize,
    pending: Option<Prepaid>,
    failure: Option<FailureContext>,
    supervisor: Option<Supervisor>,
    abort_drain: Option<tokio::sync::oneshot::Receiver<DrainedAbortSubmissions>>,
    abort_inputs: VecDeque<AbortInputRetirement>,
    abort_retirement: Option<AbortIngressRetirement>,
    abort_scan: RetirementScan,
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
            pumps: None,
            diagnostics: None,
            next_payload: None,
            split_next: false,
            local_pumps_closing: false,
            requests: send.requests,
            contexts: send.contexts,
            report_author: Some(send.report_author),
            send_idle: send.idle,
            send_live: BTreeMap::new(),
            abort_receipts: BTreeMap::new(),
            failed_sends: BTreeMap::new(),
            send_ready: VecDeque::with_capacity(capacity),
            send_scan: RetirementScan::default(),
            admission_scan: RetirementScan::default(),
            call_scan: 0,
            pending: None,
            failure: Some(failure.0),
            abort_drain: failure.1.abort_drain.take(),
            abort_inputs: VecDeque::new(),
            abort_retirement: None,
            abort_scan: RetirementScan::default(),
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

    pub(crate) fn with_abort_retirement(
        mut self,
        owner: &AbortSettlementOwner,
        wake: Waker,
    ) -> Self {
        assert!(
            self.abort_retirement
                .replace(owner.ingress_retirement(wake))
                .is_none()
        );
        self
    }

    pub(crate) fn with_pumps(
        mut self,
        pumps: AttemptPumpSource,
        diagnostics: Arc<AttemptDiagnosticSources>,
        next_payload: NonZeroU64,
    ) -> Self {
        assert!(self.releases.is_some());
        assert!(pumps.matches(&self.registration));
        assert!(self.pumps.replace(pumps).is_none());
        self.diagnostics = Some(diagnostics);
        self.next_payload = Some(next_payload);
        self
    }

    fn take_unclaimed_release(&mut self) -> Option<TransportRelease> {
        if !self.send_live.is_empty()
            || !self.failed_sends.is_empty()
            || !self.calls[3]
                .as_ref()
                .is_none_or(AttemptCallSource::is_quiescent)
        {
            return None;
        }
        if let Some(original) = self.admission.take_unclaimed_grant() {
            return Some(TransportRelease::QueuedInput(original));
        }
        if self
            .pumps
            .as_ref()
            .is_some_and(AttemptPumpSource::has_split_work)
        {
            return None;
        }
        let (task, payload, seed) = self.admission.take_unclaimed_creation()?;
        Some(TransportRelease::Creation {
            request: self.requests.retire_creation(task, payload),
            seed,
        })
    }

    fn transport_drained(&self) -> bool {
        self.abort_retirement.is_none()
            && self
                .subscriptions
                .as_ref()
                .is_none_or(AttemptSubscriptions::is_drained)
            && self
                .pumps
                .as_ref()
                .is_none_or(AttemptPumpSource::is_drained)
            && self
                .calls
                .iter()
                .flatten()
                .all(AttemptCallSource::is_drained)
            && self.report_author.is_none()
            && self.send_live.is_empty()
            && self.failed_sends.is_empty()
            && self.requests.is_empty()
            && self.admission.is_drained()
            && self
                .releases
                .as_ref()
                .is_none_or(TransportReleases::is_empty)
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
                                self.abort_scan.request();
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
        &mut self,
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
        let next_payload = &mut self.next_payload;
        let diagnostics = self.diagnostics.as_ref();
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
                let mut filter = None;
                let diagnostic = if let Some(diagnostics) = diagnostics
                    && let OperationIntent::ReleaseParticipation(request) = operation
                {
                    let next = next_payload
                        .as_mut()
                        .expect("original Native payload namespace");
                    let reference = PayloadRef(*next);
                    *next = next
                        .checked_add(1)
                        .expect("Native payload identities exhausted");
                    filter = Some(reference);
                    Some(
                        diagnostics
                            .release_report(request.participation(), reference)
                            .expect("original release participation"),
                    )
                } else {
                    None
                };
                let mut member = SendMemberReport::new(
                    receipt,
                    filter,
                    self.admission.failure_records(),
                    first_poll,
                );
                if let OperationIntent::AbortParticipation(request) = operation {
                    member.retain_abort_receipt(request.participation());
                }
                if let Some(report) = diagnostic {
                    member.bind_release_diagnostic(report);
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
        let result = row.result.as_ref().expect("original send result");
        if !row.retired {
            self.send_ready.push_back(effect);
            return false;
        }
        if !result
            .original
            .retire_ready_after_exit()
            .expect("original send result")
        {
            self.send_ready.push_back(effect);
            return match result.original.take_failed_payload() {
                Ok(Some(original)) => {
                    original.dispose().expect("original failed send input");
                    true
                }
                Ok(None)
                | Err(ActorTurnError::ActorNotRetired | ActorTurnError::InputAuthorMismatch) => {
                    false
                }
                Err(error) => std::panic::resume_unwind(Box::new(error)),
            };
        }
        if !self
            .scope
            .reap_observed_header(&row.header)
            .expect("original send header")
        {
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
            if let Some(pumps) = self
                .pumps
                .as_mut()
                .filter(|source| source.handles(original))
            {
                let result = pumps.prepay(original).expect("original pump prepayment")?;
                self.pending = Some(Prepaid::Pump);
                return Some(result);
            }
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
                EffectBody::AdmitQueuedInput { ref operation, .. } => {
                    let transport = self
                        .contexts
                        .get(&operation.backend_process_id())
                        .expect("original Native backend transport context")
                        .transport;
                    let result = self
                        .admission
                        .prepay(original, &self.requests, transport)
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
                            .or_else(|| self.pumps.as_ref()?.project_failure(reference))
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
            Prepaid::Pump => Ok(self.pumps.as_mut().unwrap().bind_job(ticket, result)),
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
            Prepaid::Pump => self.pumps.as_mut().unwrap().return_unclaimed(result),
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
        if let Some(pumps) = &mut self.pumps {
            pumps.core_consumed(&self.runtime, inputs);
        }
        self.other.core_consumed(inputs);
    }

    fn transport_event(&mut self, effect: AttemptActionId, event: TransportSourceEvent) {
        if let TransportSourceEvent::Report(report) = event {
            if report.is_disposable() {
                retain_release(
                    &mut self.releases,
                    &mut self.other,
                    ReleaseCapture::Original(Box::new(report)),
                );
            } else {
                self.other.retain_transport_failure(Box::new(report));
            }
            return;
        }
        if let Some(pumps) = self.pumps.as_mut().filter(|source| source.owns(effect)) {
            if let Err(original) = pumps.transport_event(effect, event) {
                self.other.retain_transport_failure(original);
            }
            return;
        }
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
                            .or_else(|| self.pumps.as_mut()?.take_failure(reference))
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
            TransportSourceEvent::Result(mut result) => {
                if self.send_live.contains_key(&effect) {
                    let fenced = matches!(&result.outcome, Some(BodyOutcome::Unused(_)))
                        && matches!(&result.withdrawal, Some(Err(refused)) if matches!(
                            refused.error, ActorTurnError::ActorFailed | ActorTurnError::Closed
                        ));
                    if fenced {
                        let Some(Err(original)) = result.withdrawal.take() else {
                            unreachable!("original fenced withdrawal");
                        };
                        retain_release(
                            &mut self.releases,
                            &mut self.other,
                            ReleaseCapture::Original(Box::new(original)),
                        );
                    }
                    let resolved = fenced
                        || matches!(&result.outcome, Some(BodyOutcome::ReceiptsRecorded))
                        || (matches!(&result.outcome, Some(BodyOutcome::Unused(_)))
                            && matches!(&result.withdrawal, Some(Ok(_))));
                    if resolved {
                        let row = self.send_live.get_mut(&effect).unwrap();
                        assert!(row.result.replace(result).is_none());
                        self.send_ready.push_back(effect);
                        self.send_scan.request();
                    } else {
                        let row = self.send_live.remove(&effect).unwrap();
                        assert!(row.result.is_none());
                        assert!(self.failed_sends.insert(effect, row.abort_keys).is_none());
                        self.other
                            .retain_transport_failure(Box::new(FailedSendResult {
                                original: result,
                                header: row.header,
                            }));
                    }
                } else {
                    self.admission_scan.request();
                    if let Err(refused) = self.admission.accept(result, &mut self.requests) {
                        self.other.retain_transport_failure(Box::new(refused));
                    }
                }
            }
            TransportSourceEvent::Retired => {
                self.in_flight.remove(&effect);
                if let Some(row) = self.send_live.get_mut(&effect) {
                    row.retired = true;
                    self.send_scan.request();
                } else if let Some(abort_keys) = self.failed_sends.remove(&effect) {
                    for key in abort_keys {
                        self.abort_receipts.remove(&key);
                    }
                    self.other
                        .transport_event(effect, TransportSourceEvent::Retired);
                } else {
                    assert!(self.admission.transport_retired(effect));
                    self.admission_scan.request();
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
        if matches!(&pending.ticket.effect, FrontendEffect::Attempt(effect) if effect.uses_join_custody())
            && pending.job.captures.is::<super::BoundActorTransport>()
        {
            let releases = self
                .releases
                .as_ref()
                .expect("original Native release executor");
            let PendingHandoff { ticket, job } = pending;
            match super::handoff_actor_transport(releases.executor(), job) {
                Ok(_) => {
                    if let Err(original) = self.runtime.effect_completed(ticket) {
                        self.other.retain_transport_failure(Box::new(original));
                    }
                }
                Err(job) => self
                    .other
                    .owner_failed_pending(PendingHandoff { ticket, job }),
            }
            return;
        }
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

    fn transport_retirement_ready(&self) -> bool {
        self.releases
            .as_ref()
            .is_none_or(TransportReleases::accepts_batch)
    }

    fn attempt_notice_ready(
        &self,
        notice: &crate::task_execution::host::adapter::AttemptNotice,
    ) -> bool {
        !notice.requires_reporting()
            || self
                .releases
                .as_ref()
                .is_none_or(TransportReleases::accepts_batch)
    }

    fn consume_attempt_notice(
        &mut self,
        notice: Arc<crate::task_execution::host::adapter::AttemptNotice>,
    ) {
        if notice.requires_reporting() {
            self.releases
                .as_mut()
                .expect("original attempt notice release")
                .retain(ReleaseCapture::Notice(notice));
        }
    }

    fn begin_local_convergence(&mut self) {
        if let Some(qa) = &self.calls[0] {
            qa.close_abort_ingress();
        }
        self.local_pumps_closing = true;
    }

    fn local_convergence_exited(&self) -> bool {
        self.local_pumps_closing
            && self.in_flight.is_empty()
            && self.send_live.is_empty()
            && self.failed_sends.is_empty()
            && self.admission.awaiting_result_retirement() == 0
            && self
                .calls
                .iter()
                .flatten()
                .all(AttemptCallSource::is_quiescent)
            && self
                .subscriptions
                .as_ref()
                .is_none_or(AttemptSubscriptions::local_exited)
            && self
                .pumps
                .as_ref()
                .is_none_or(AttemptPumpSource::local_exited)
            && self
                .releases
                .as_ref()
                .is_none_or(TransportReleases::is_empty)
    }

    fn can_close_ordered_observations(&self, failed: bool) -> bool {
        self.result_admission_closed()
            && self.releases.is_some()
            && self.transport_drained()
            // The failed supervisor consumes the original failed tail. In
            // the normal path seal_drained first needs these roles returned.
            && (!failed || self.supervisor.is_none())
    }

    fn close_ordered_observations(&mut self, original: OrderedObservationHost) {
        self.releases
            .as_mut()
            .expect("original observation close executor")
            .retain(ReleaseCapture::Observations(original));
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
        let failed = self
            .runtime
            .snapshot(self.registration.actor())
            .expect("original attempt transport host")
            .failed;
        if (failed || closing)
            && units < budget
            && self.calls[2]
                .as_ref()
                .is_none_or(AttemptCallSource::is_quiescent)
            && let Some(releases) = self
                .releases
                .as_mut()
                .filter(|source| source.accepts_batch())
            && let Some(original) = self
                .diagnostics
                .as_ref()
                .and_then(|records| records.take_release_records())
        {
            releases.retain(ReleaseCapture::Original(Box::new(original)));
            units += 1;
            progressed = true;
        }
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
        if turn.was_notified() || release_advanced {
            self.abort_scan.request();
            self.admission_scan.request();
            self.send_scan.request();
            if let Some(subscriptions) = &mut self.subscriptions {
                subscriptions.request_retirement_scan();
            }
            if let Some(pumps) = &mut self.pumps {
                pumps.request_retirement_scan();
            }
        }
        let subscription_progress = self.subscriptions.as_mut().map(|subscriptions| {
            let allowance = if release_accepting {
                (budget - units).min(1)
            } else {
                0
            };
            subscriptions.progress(
                failed,
                closing || self.external_closed || self.local_pumps_closing,
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
        let mut pump_more = false;
        let mut split_changed = false;
        let pumps_closing = closing || self.local_pumps_closing;
        if let Some(pumps) = &mut self.pumps {
            let releases = self
                .releases
                .as_mut()
                .expect("original pump release executor");
            let split_first = self.split_next;
            for (index, is_split) in [split_first, !split_first].into_iter().enumerate() {
                if units == budget {
                    self.split_next = is_split;
                    pump_more |= (index == 0 || is_split) && pumps.has_split_work();
                    pump_more |= (index == 0 || !is_split)
                        && pumps.needs_retirement_scan(failed || pumps_closing, retire_failures);
                    break;
                }
                if is_split {
                    if !pumps.has_split_work() {
                        continue;
                    }
                    let mut payloads = NativeSplitPayloadStore {
                        requests: &mut self.requests,
                        admission: &mut self.admission,
                        next: self
                            .next_payload
                            .as_mut()
                            .expect("original Native payload namespace"),
                    };
                    let executor = releases.executor().clone();
                    let wake = releases.wake().clone();
                    units += 1;
                    match pumps.progress_split(
                        failed,
                        pumps_closing,
                        pumps_closing || failed && self.supervisor.is_none(),
                        releases.accepts_batch(),
                        &mut payloads,
                        &executor,
                        &wake,
                        |original| releases.retain(ReleaseCapture::Original(original)),
                    ) {
                        Ok(changed) => split_changed = changed,
                        Err(error) => self.other.retain_transport_failure(error),
                    }
                } else {
                    let progress = pumps.progress(
                        failed,
                        pumps_closing,
                        retire_failures,
                        1,
                        releases.accepts_batch(),
                        |original| releases.retain(ReleaseCapture::Original(original)),
                    );
                    units += progress.units;
                    progressed |= progress.progressed;
                    pump_more |= progress.more;
                }
            }
            progressed |= split_changed;
        }
        let supervisor_advanced = if units < budget && self.supervisor.is_some() {
            units += 1;
            self.advance_supervisor(turn)
        } else {
            false
        };
        if self
            .abort_scan
            .take_visits(self.abort_inputs.len(), (budget - units).min(1))
            != 0
        {
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
        let mut abort_retirement_more = false;
        if closing && let Some(retirement) = &mut self.abort_retirement {
            if units == budget {
                abort_retirement_more = true;
            } else {
                units += 1;
                match retirement
                    .progress()
                    .expect("original Abort role retirement")
                {
                    AbortIngressRetirementProgress::Blocked => {}
                    AbortIngressRetirementProgress::Advanced => abort_retirement_more = true,
                    AbortIngressRetirementProgress::Complete => {
                        self.abort_retirement = None;
                        progressed = true;
                    }
                }
            }
        }
        if self.admission_scan.take_visits(
            self.admission.awaiting_result_retirement(),
            (budget - units).min(1),
        ) != 0
        {
            units += 1;
            let retired = self
                .admission
                .retire_result()
                .expect("original admission result");
            if retired {
                self.admission_scan.request();
                progressed = true;
            }
        }
        if self
            .send_scan
            .take_visits(self.send_ready.len(), (budget - units).min(1))
            != 0
        {
            units += 1;
            if self.advance_send() {
                self.send_scan.request();
                progressed = true;
            }
        }
        if retire_failures
            && units < budget
            && self.send_live.is_empty()
            && self.failed_sends.is_empty()
            && self.admission.is_quiescent()
            && self
                .releases
                .as_ref()
                .is_none_or(TransportReleases::accepts_batch)
            && let Some(original) = self.admission.take_unselected_failure()
        {
            retain_release(
                &mut self.releases,
                &mut self.other,
                ReleaseCapture::Original(Box::new(original)),
            );
            units += 1;
            progressed = true;
        }
        if closing
            && units < budget
            && self
                .releases
                .as_ref()
                .is_some_and(TransportReleases::accepts_batch)
            && let Some(original) = self.take_unclaimed_release()
        {
            self.releases
                .as_mut()
                .expect("original Native release executor")
                .retain(ReleaseCapture::Transport(original));
            units += 1;
            progressed = true;
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
        if closing && self.transport_drained() {
            self.other.transport_roles_drained();
        }
        let mut progress = self
            .other
            .progress_retirement(runtime, turn, now, budget - units);
        progress.units += units;
        progress.progressed |= progressed || supervisor_advanced || release_advanced;
        progress.more |= subscription_progress
            .as_ref()
            .is_some_and(|progress| release_accepting && progress.more)
            || pump_more
            || split_changed
            || supervisor_advanced
            || self.abort_scan.has_work(self.abort_inputs.len())
            || abort_retirement_more
            || self
                .admission_scan
                .has_work(self.admission.awaiting_result_retirement())
            || self.send_scan.has_work(self.send_ready.len())
            || release_advanced
            || (budget == 0 || progressed)
                && self
                    .releases
                    .as_ref()
                    .is_some_and(TransportReleases::has_work)
            || progressed && self.result_admission_closed();
        progress.retired &=
            self.supervisor.is_none() && self.abort_inputs.is_empty() && self.transport_drained();
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
        if matches!(&job.effect, FrontendEffect::Attempt(effect)
            if effect.uses_join_custody()
                && (!effect.is_pump_call() || job.captures.is::<super::BoundActorTransport>()))
        {
            self.native.handoff(job)
        } else {
            self.other.handoff(job)
        }
    }
}
