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
    ActorSendReports, ActorTransportExecutor, AdmissionSource, BodyOutcome, FrozenSendRequests,
    NativeProgressInput, NativeReceiptInput, PreparedActorSend, SendContext, SendKey,
    SendMemberReport, SendReportRows, send_reports::close_unlaunched,
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
    task_execution::host::custody::{ReturnedActorTransportResult, TransportSourceEvent},
    task_execution::host::failure_publication::FailureContext,
    task_execution::host::failure_supervisor::{
        FailureCleanupPort, FailureSupervisor, RunningFailureSupervisor,
    },
};
use novarocks_attempt_protocol::{
    effect::{EffectBody, OperationIntent, QueuedInputPayload},
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
    sync::Arc,
    task::Poll,
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
    Send(usize, Box<PreparedActorSend>),
    Other,
}

struct SendInFlight {
    lane: usize,
    header: WorkRetirementObserver,
    result: Option<ReturnedActorTransportResult>,
    retired: bool,
}

pub(crate) struct AttemptFailureBinding {
    pub(crate) supervisor: FailureSupervisor,
    pub(crate) port: Box<dyn FailureCleanupPort>,
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
    requests: FrozenSendRequests,
    contexts: Arc<BTreeMap<BackendProcessId, SendContext>>,
    report_references: BTreeMap<(TaskOperationId, SendGeneration), NativeReportReferences>,
    report_author: Option<ReportedWork>,
    send_idle: [VecDeque<Position>; 2],
    send_live: BTreeMap<AttemptActionId, SendInFlight>,
    failed_sends: BTreeSet<AttemptActionId>,
    send_ready: VecDeque<AttemptActionId>,
    send_scan: usize,
    admission_scan: usize,
    pending: Option<Prepaid>,
    failure: Option<FailureContext>,
    supervisor: Option<Supervisor>,
    in_flight: BTreeSet<AttemptActionId>,
    external_closed: bool,
    other: Box<dyn EffectSource>,
}

impl AttemptTransportSource {
    pub(crate) fn new(
        runtime: ActorRuntime<FrontendActor>,
        registration: ActorRegistration,
        native_source: ActorId,
        scope: ScopeHandle,
        admission: AdmissionSource,
        send: AttemptSendSource,
        failure: (FailureContext, AttemptFailureBinding),
        other: Box<dyn EffectSource>,
    ) -> Self {
        assert!(registration.matches_scope(&scope) && admission.matches_scope(&scope));
        assert!(scope.same_scope(send.report_author.work().scope()));
        let capacity = send.idle.iter().map(VecDeque::len).sum();
        Self {
            runtime,
            registration,
            native_source,
            scope,
            admission,
            requests: send.requests,
            contexts: send.contexts,
            report_references: send.report_references,
            report_author: Some(send.report_author),
            send_idle: send.idle,
            send_live: BTreeMap::new(),
            failed_sends: BTreeSet::new(),
            send_ready: VecDeque::with_capacity(capacity),
            send_scan: 0,
            admission_scan: 0,
            pending: None,
            failure: Some(failure.0),
            supervisor: Some(Supervisor::Armed(failure.1)),
            in_flight: BTreeSet::new(),
            external_closed: false,
            other,
        }
    }

    fn advance_supervisor(&mut self, turn: &ActorHostTurn<FrontendActor>) -> bool {
        let closing = self.result_admission_closed();
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
                Ok(SendMemberReport::new(
                    receipt,
                    references.filter,
                    references.workload,
                    first_poll,
                ))
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
        let result = row.result.unwrap();
        self.send_idle[row.lane].push_back(result.original);
        true
    }
}

impl EffectSource for AttemptTransportSource {
    fn prepay_result(&mut self, effect: &FrontendEffect) -> Option<Position> {
        assert!(self.pending.is_none());
        if let FrontendEffect::Attempt(original) = effect {
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
            Prepaid::Admission => {
                let FrontendEffect::Attempt(effect) = &ticket.effect else {
                    unreachable!()
                };
                assert!(self.in_flight.insert(effect.protocol.id));
                Ok(self.admission.bind(ticket, result))
            }
            Prepaid::Other => {
                if let FrontendEffect::Attempt(effect) = &ticket.effect {
                    match effect.protocol.body {
                        EffectBody::ReleaseQueuedInput { grant } => {
                            if let Some(original) = self.admission.release_grant(grant) {
                                self.other.retain_transport_release(Box::new((
                                    effect.protocol.id,
                                    original,
                                )));
                            }
                        }
                        EffectBody::ReleaseCreationPayload { task, payload } => {
                            if let Some(original) = self.requests.retire_creation(task, payload) {
                                self.other.retain_transport_release(Box::new((
                                    effect.protocol.id,
                                    original,
                                    self.admission.take_payload(payload),
                                )));
                            }
                        }
                        _ => {}
                    }
                }
                self.other.try_bind_job(runtime, turn, ticket, result)
            }
            Prepaid::Send(lane, prepared) => {
                let FrontendEffect::Attempt(effect) = &ticket.effect else {
                    unreachable!()
                };
                assert!(self.in_flight.insert(effect.protocol.id));
                let (job, header) = prepared
                    .bind_tracked(runtime, ticket, result, &mut self.admission)
                    .map_err(|original| EffectBindRefusal {
                        error: original.error,
                        original: Box::new(original),
                    })?;
                assert!(
                    self.send_live
                        .insert(
                            effect.protocol.id,
                            SendInFlight {
                                lane,
                                header,
                                result: None,
                                retired: false,
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
            Prepaid::Admission => self.admission.return_unclaimed(result),
            Prepaid::Other => self.other.return_unclaimed_result(result),
            Prepaid::Send(lane, prepared) => {
                prepared.revoke_unused_reports();
                self.send_idle[lane].push_back(result);
            }
        }
    }

    fn core_consumed(&mut self, inputs: &[InputId]) {
        self.other.core_consumed(inputs);
    }

    fn transport_event(&mut self, effect: AttemptActionId, event: TransportSourceEvent) {
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
                        assert!(self.failed_sends.insert(effect));
                        self.other
                            .retain_transport_failure(Box::new(FailedSendResult {
                                effect,
                                original: result,
                                header: row.header,
                            }));
                    }
                } else {
                    let creation = match (&result.outcome, &result.effect.protocol.body) {
                        (
                            Some(BodyOutcome::InputAdmitted(_)),
                            EffectBody::AdmitQueuedInput {
                                operation:
                                    OperationIntent::CreateTask {
                                        envelope,
                                        identity,
                                        creation,
                                        ..
                                    },
                                payload: QueuedInputPayload::NewCreation(payload),
                                ..
                            },
                        ) if creation == payload => {
                            Some((envelope.operation_id(), *identity, *payload))
                        }
                        _ => None,
                    };
                    match self.admission.accept(result) {
                        Err(refused) => self.other.retain_transport_failure(Box::new(refused)),
                        Ok(_) => {
                            if let Some((operation, task, payload)) = creation {
                                let pin = self
                                    .admission
                                    .clone_payload_pin(payload)
                                    .expect("published creation keeps its original payload");
                                self.requests
                                    .bind_creation(operation, task, payload, pin)
                                    .unwrap_or_else(|original| {
                                        std::panic::resume_unwind(Box::new(original))
                                    });
                            }
                        }
                    }
                }
            }
            TransportSourceEvent::Retired => {
                self.in_flight.remove(&effect);
                if let Some(row) = self.send_live.get_mut(&effect) {
                    row.retired = true;
                } else if self.failed_sends.remove(&effect) {
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
                )));
            }
            None => std::panic::resume_unwind(Box::new((ticket, original))),
            Some(Supervisor::Running(running)) => {
                std::panic::resume_unwind(Box::new((ticket, original, running)))
            }
        }
    }

    fn close_external(&mut self) {
        if !self.external_closed {
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
        let supervisor_advanced = if budget != 0 && self.supervisor.is_some() {
            units += 1;
            self.advance_supervisor(turn)
        } else {
            false
        };
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
            {
                self.other
                    .retain_transport_release(Box::new(std::mem::take(&mut self.requests)));
                units += 1;
                progressed = true;
            }
        }
        let mut progress = self
            .other
            .progress_retirement(runtime, turn, now, budget - units);
        progress.units += units;
        progress.progressed |= progressed || supervisor_advanced;
        progress.more |= supervisor_advanced
            || self.admission_scan != 0
            || self.send_scan != 0
            || progressed && self.result_admission_closed();
        progress.retired &= self.supervisor.is_none()
            && self.report_author.is_none()
            && self.send_live.is_empty()
            && self.failed_sends.is_empty()
            && self.requests.is_empty()
            && self.admission.is_drained();
        progress
    }
}

pub(crate) struct AttemptTransportExecutor {
    pub(crate) native: ActorTransportExecutor,
    pub(crate) other: Arc<dyn EffectExecutor>,
}
impl EffectExecutor for AttemptTransportExecutor {
    fn handoff(&self, job: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
        if matches!(&job.effect, FrontendEffect::Attempt(effect) if effect.handoff().is_some()) {
            self.native.handoff(job)
        } else {
            self.other.handoff(job)
        }
    }
}
