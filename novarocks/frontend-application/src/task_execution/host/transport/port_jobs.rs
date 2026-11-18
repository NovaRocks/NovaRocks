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

//! Original call custody for the three passive AP-to-LP fact families.

use super::{ActorTransportExecutor, AttemptCallSource};
use crate::task_execution::host::RetirementScan;
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect},
        host::{
            EffectBindRefusal, EffectExecutor, OriginalUntrackedTransport, PreparedEffectBinding,
            PreparedEffectJob, SourceProgress,
        },
        transport_withdrawal::AttemptEffectEnvelope,
    },
    task_execution::host::port::{
        AttemptPortEndpoints,
        facts::{
            FactError, FactPositionLoanObligation, FactPublishRefusal, FactRole,
            FailedReceiverLoanObligation, RootReplyFactHandoff, RootStatusFactReceipt,
        },
    },
};
use novarocks_control_runtime::{
    actor_turn::{ActorFailureProof, ActorRuntime, ActorTurnError, EffectTicket},
    causal_input::CausalInputObligation,
};
use novarocks_coordination_contract::{
    AttemptRequestToken,
    logical_port::{AttemptToLogical, AttemptToLogicalBody},
};
use novarocks_execution_contract::ParticipationRef;
use std::sync::{Arc, Weak};

type Position = CausalInputObligation<FrontendActor>;

/// Checked by the enclosed original role loan's Drop; neither return grants
/// input consumption, capture disposal or physical producer exit.
#[must_use = "Return the same original paid fact-position loan"]
pub(super) enum PortFactLoanObligation {
    Live(FactPositionLoanObligation),
    Failed {
        proof: ActorFailureProof,
        original: FailedReceiverLoanObligation,
    },
}
enum FactReceipt {
    Plain,
    Status(RootStatusFactReceipt),
    Reply(RootReplyFactHandoff),
}
impl PortFactLoanObligation {
    fn publish(
        mut self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        message: AttemptToLogical,
        endpoints: &AttemptPortEndpoints,
    ) -> Result<FactReceipt, FactPublishRefusal> {
        // A typed refusal returned the exact unsubmitted payload. Only this
        // branch may convert the same paid role to its failed-receiver cell.
        let message = match &mut self {
            Self::Live(loan) => match publish_live(loan, runtime, ticket, message) {
                Ok(receipt) => return Ok(receipt),
                Err(original)
                    if matches!(
                        original.error,
                        FactError::Runtime(ActorTurnError::ActorFailed) | FactError::Closed
                    ) =>
                {
                    original.message
                }
                Err(original) => return Err(original),
            },
            Self::Failed { proof, original } => {
                return publish_failed(original, runtime, ticket, proof, message);
            }
        };
        let proof = match runtime.failure_proof_for(endpoints.logical()) {
            Ok(proof) => proof,
            Err(error) => {
                return Err(FactPublishRefusal {
                    error: FactError::Runtime(error),
                    message,
                });
            }
        };
        let Self::Live(loan) = self else {
            unreachable!("same unsubmitted live loan")
        };
        let mut loan = match loan.into_failed_receiver(runtime, &proof) {
            Ok(loan) => loan,
            Err((error, original)) => {
                drop(original);
                return Err(FactPublishRefusal {
                    error: FactError::Runtime(error),
                    message,
                });
            }
        };
        publish_failed(&mut loan, runtime, ticket, &proof, message)
    }
}
fn publish_live(
    loan: &mut FactPositionLoanObligation,
    runtime: &ActorRuntime<FrontendActor>,
    ticket: &EffectTicket<FrontendEffect>,
    message: AttemptToLogical,
) -> Result<FactReceipt, FactPublishRefusal> {
    match message.body {
        AttemptToLogicalBody::RootStatus(_) => loan
            .publish_root_status_now(runtime, ticket, message)
            .map(FactReceipt::Status),
        AttemptToLogicalBody::SealResult { .. }
        | AttemptToLogicalBody::TerminalControlResult { .. } => loan
            .publish_root_reply_now(runtime, ticket, message)
            .map(FactReceipt::Reply),
        _ => loan
            .publish_now(runtime, ticket, message)
            .map(|_| FactReceipt::Plain),
    }
}
fn publish_failed(
    loan: &mut FailedReceiverLoanObligation,
    runtime: &ActorRuntime<FrontendActor>,
    ticket: &EffectTicket<FrontendEffect>,
    proof: &ActorFailureProof,
    message: AttemptToLogical,
) -> Result<FactReceipt, FactPublishRefusal> {
    match message.body {
        AttemptToLogicalBody::RootStatus(_) => loan
            .publish_root_status_now(runtime, ticket, proof, message)
            .map(FactReceipt::Status),
        AttemptToLogicalBody::SealResult { .. }
        | AttemptToLogicalBody::TerminalControlResult { .. } => loan
            .publish_root_reply_now(runtime, ticket, proof, message)
            .map(FactReceipt::Reply),
        _ => loan
            .publish_now(runtime, ticket, proof, message)
            .map(|_| FactReceipt::Plain),
    }
}

/// Consumed by the same request table/call carrier and original Host tracker.
/// This row has no Drop completion shortcut; actual report/Join/header retire.
#[must_use = "Retain the original job and unlaunched retirement until handoff"]
struct OriginalPortJobObligation {
    effect: Arc<AttemptEffectEnvelope>,
    request: Option<AttemptRequestToken>,
    job: Option<PreparedEffectJob>,
    transport: Option<Box<crate::task_execution::host::custody::TransportRetirement>>,
}
enum AbortInputRetirement {
    Open,
    Retiring(super::super::port::abort::AbortIngressRetirement),
    Closed,
}
/// A fixed row per original call role. A request row holds only its token;
/// the complete original job lives solely in the same endpoint request table.
pub(super) struct OriginalPortJobs {
    pub(super) endpoints: Arc<AttemptPortEndpoints>,
    establish: Option<Arc<super::AttemptEstablishMaterial>>,
    compatibility: novarocks_types::NativeCompatibilityId,
    receipts: Arc<super::super::port::abort::AbortReceiptTable>,
    reader: super::super::port::control::ControlReaderObligation,
    reader_closed: bool,
    abort: AbortInputRetirement,
    abort_input_scan: RetirementScan,
    abort_input_after: Option<ParticipationRef>,
    abort_input_next: bool,
    reader_more: bool,
    inputs: Weak<crate::control_runtime::logical::LogicalInputBank>,
    index: usize,
    author_return_issued: bool,
    rows: [Option<OriginalPortJobObligation>; 3],
    next: usize,
    remaining: usize,
}
impl OriginalPortJobs {
    pub(super) fn new(
        endpoints: Arc<AttemptPortEndpoints>,
        establish: Arc<super::AttemptEstablishMaterial>,
        compatibility: novarocks_types::NativeCompatibilityId,
        receipts: Arc<super::super::port::abort::AbortReceiptTable>,
        reader: super::super::port::control::ControlReaderObligation,
        logical: &crate::query_execution::logical_binding::BoundLogicalPorts,
        link: &crate::control_runtime::logical::links::AttemptActivationLinkLoan,
    ) -> Self {
        assert!(logical.registration.same_registration(endpoints.logical()));
        assert!(logical.links.matches_loan(link, endpoints.activation()));
        assert!(link.matches_child_registration(endpoints.activation(), endpoints.attempt()));
        assert!(reader.matches_pair(
            endpoints.logical(),
            endpoints.attempt(),
            endpoints.activation()
        ));
        assert!(receipts.matches_activation(endpoints.activation()));
        let inputs = Arc::downgrade(&logical.inputs);
        let index = link.index();
        Self {
            endpoints,
            establish: Some(establish),
            compatibility,
            receipts,
            reader,
            reader_closed: false,
            abort: AbortInputRetirement::Open,
            abort_input_scan: RetirementScan::default(),
            abort_input_after: None,
            abort_input_next: false,
            reader_more: true,
            inputs,
            index,
            author_return_issued: false,
            rows: std::array::from_fn(|_| None),
            next: 0,
            remaining: 0,
        }
    }
    pub(super) fn project(
        &self,
        effect: &AttemptEffectEnvelope,
        mut resolve: impl FnMut(
            novarocks_attempt_protocol::failure::HostFailureRef,
        ) -> Option<
            novarocks_query_application::execution_error::ExecutionFailureInput,
        >,
    ) -> Result<AttemptToLogical, Box<dyn std::any::Any + Send>> {
        use novarocks_attempt_protocol::port_output::AttemptToLogicalBody as Body;
        use novarocks_attempt_protocol::{
            effect::EffectBody, port_projection::project_attempt_message,
        };
        let original = match &effect.protocol.body {
            EffectBody::EstablishAuthorization { operation, request } => {
                let Body::AuthorizationRequest {
                    request: authorization,
                    ..
                } = request.body
                else {
                    unreachable!("original Establish authorization")
                };
                self.establish
                    .as_ref()
                    .expect("original live Establish material")
                    .for_authorization(operation, authorization)
                    .map_err(|original| Box::new(original) as Box<dyn std::any::Any + Send>)?;
                request
            }
            EffectBody::Port(original) => original,
            _ => unreachable!("same passive AP effect"),
        };
        // The pure AP error and a retained host error can have the same safe
        // projection. Read this SAME passive message before that projection;
        // never take its underlying original failure from Source custody.
        #[cfg(test)]
        if matches!(
            &original.body,
            Body::PreReadyReplacementQuery { .. }
                | Body::AttemptTerminal(_)
                | Body::AttemptLost { .. }
                | Body::SealResult { result: Err(_), .. }
                | Body::TerminalControlResult { result: Err(_), .. }
        ) {
            eprintln!("original AP failure before port projection: {original:?}");
        }
        project_attempt_message(
            original,
            &mut resolve,
            &mut |participation, operation_id| {
                self.establish
                    .as_ref()
                    .expect("original live Establish material")
                    .authorization_facts(participation, operation_id, self.compatibility)
            },
            &mut |participation, operation, generation| {
                self.receipts.resolve(participation, operation, generation)
            },
        )
        .map_err(|original| Box::new(original) as Box<dyn std::any::Any + Send>)
    }
    pub(super) fn prepay(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        role: FactRole,
        request: bool,
    ) -> Option<PortFactLoanObligation> {
        if let Some(row) = &self.rows[role.index()] {
            tracing::debug!(
                target: "novarocks_original_wait",
                actor = ?self.endpoints.attempt().actor(),
                activation = ?self.endpoints.activation(),
                role = ?role,
                held_action = ?row.effect.protocol.id,
                request = ?row.request,
                job = row.job.is_some(),
                transport = row.transport.is_some(),
                remaining = self.remaining,
                "attempt original port prepay refused at retained row"
            );
            return None;
        }
        if request && !self.endpoints.requests.has_capacity() {
            tracing::debug!(
                target: "novarocks_original_wait",
                actor = ?self.endpoints.attempt().actor(),
                activation = ?self.endpoints.activation(),
                role = ?role,
                "attempt original port prepay refused at request capacity"
            );
            return None;
        }
        match runtime
            .snapshot(self.endpoints.logical().actor())
            .expect("original logical registration")
            .failed
        {
            false => self
                .endpoints
                .facts
                .try_loan(role)
                .map(PortFactLoanObligation::Live),
            true => {
                let proof = runtime
                    .failure_proof_for(self.endpoints.logical())
                    .expect("same actual failed logical actor");
                self.endpoints
                    .facts
                    .try_failed_receiver_loan(runtime, &proof, role)
                    .expect("same original failed receiver role")
                    .map(|original| PortFactLoanObligation::Failed { proof, original })
            }
        }
    }
    pub(super) fn bind(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
        call: &mut AttemptCallSource,
        loan: PortFactLoanObligation,
        message: AttemptToLogical,
    ) -> Result<PreparedEffectBinding, EffectBindRefusal> {
        let role = super::super::port::facts::role_for(&message.body);
        let request = request_token(&message);
        let root = matches!(
            message.body,
            AttemptToLogicalBody::RootStatus(_)
                | AttemptToLogicalBody::SealResult { .. }
                | AttemptToLogicalBody::TerminalControlResult { .. }
        );
        if root {
            let receipt = match loan.publish(runtime, ticket, message, &self.endpoints) {
                Ok(receipt) => receipt,
                Err(original) => {
                    call.return_unclaimed(result);
                    return Err(refused(original));
                }
            };
            tracing::debug!(
                actor = ?self.endpoints.attempt().actor(),
                action = ?match &ticket.effect {
                    FrontendEffect::Attempt(effect) => Some(effect.protocol.id),
                    _ => None,
                },
                receipt_kind = ?std::mem::discriminant(&receipt),
                "attempt original root fact handed off before publication job"
            );
            let job = match receipt {
                FactReceipt::Status(receipt) => call.bind_with_root_fact(ticket, result, receipt),
                FactReceipt::Reply(handoff) => {
                    let receipt = self
                        .endpoints
                        .roots
                        .acknowledge_fact_handoff(handoff)
                        .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)));
                    call.bind_with_root_reply(ticket, result, receipt)
                }
                FactReceipt::Plain => unreachable!("original root fact receipt"),
            };
            return Ok(PreparedEffectBinding::Job(job));
        }
        let FrontendEffect::Attempt(effect) = &ticket.effect else {
            unreachable!("same attempt ticket")
        };
        let mut job = call.bind(ticket, result);
        let transport = job.transport.take();
        let slot = &mut self.rows[role.index()];
        assert!(slot.is_none(), "one original job per paid call role");
        *slot = Some(OriginalPortJobObligation {
            effect: effect.clone(),
            request,
            job: None,
            transport,
        });
        if request.is_some() {
            // Original reply can reenter during the following fact commit.
            // Its complete paid job must already be in the exact request row.
            self.endpoints
                .requests
                .insert(&message, job)
                .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)));
        } else {
            slot.as_mut().unwrap().job = Some(job);
        }
        if let Err(original) = loan.publish(runtime, ticket, message, &self.endpoints) {
            return Err(refused(original));
        }
        self.remaining = 3;
        Ok(PreparedEffectBinding::Retained {
            transport: slot.as_mut().unwrap().transport.take(),
        })
    }
    pub(super) fn register_abort_receipt(
        &self,
        original: &Arc<super::super::port::abort::AbortReceiptSlot>,
    ) {
        self.receipts
            .register(original)
            .expect("same original physical Abort receipt registration");
    }
    pub(super) fn retire_abort_receipt(&self, key: super::SendKey) {
        assert!(
            self.receipts.retire(key),
            "original canonical receipt aliases have actually exited"
        );
    }
    pub(super) fn failed_binding(
        &mut self,
        effect: &Arc<AttemptEffectEnvelope>,
    ) -> Option<OriginalUntrackedTransport> {
        let row = self
            .rows
            .iter_mut()
            .flatten()
            .find(|row| Arc::ptr_eq(&row.effect, effect))?;
        self.remaining = 3;
        row.transport
            .take()
            .map(|original| (row.effect.clone(), original))
    }
    pub(super) fn notify(&mut self) {
        self.remaining = 3;
        self.reader.begin_pass();
        self.reader_more = !self.reader_closed;
        self.abort_input_scan.request();
    }
    pub(super) fn core_consumed(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        inputs: &[novarocks_control_runtime_core::identity::InputId],
    ) {
        self.reader
            .index_committed(runtime, inputs)
            .expect("same original consumed control positions");
        if !inputs.is_empty() {
            self.reader_more = !self.reader_closed;
            self.abort_input_scan.request();
        }
    }
    pub(super) fn progress_control(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        failed: bool,
        budget: usize,
    ) -> SourceProgress {
        if self.reader_closed || budget == 0 {
            return SourceProgress {
                units: 0,
                progressed: false,
                more: !self.reader_closed
                    && (self.reader_more
                        || self
                            .abort_input_scan
                            .has_work(self.endpoints.abort_participation_count())),
                retired: self.reader_closed,
            };
        }
        if failed {
            let proof = runtime
                .failure_proof_for(self.endpoints.attempt())
                .expect("same failed AP reader");
            self.reader_closed = self
                .reader
                .close_after_failure(runtime, &proof)
                .expect("same failed reader retirement");
            return SourceProgress {
                units: 1,
                progressed: self.reader_closed,
                more: false,
                retired: self.reader_closed,
            };
        }
        let inventory = self.endpoints.abort_participation_count();
        let mut units = 0;
        let mut progressed = false;
        while units < budget {
            let abort_due = matches!(self.abort, AbortInputRetirement::Open)
                && self.abort_input_scan.has_work(inventory);
            if abort_due && (self.abort_input_next || !self.reader_more) {
                let visits = self.abort_input_scan.take_visits(inventory, 1);
                assert_eq!(visits, 1, "same dirty original Abort roster visit");
                if let Some((participation, retired)) = self
                    .endpoints
                    .retire_ready_abort_occurrence_after(self.abort_input_after)
                    .expect("same original Abort occurrence retirement")
                {
                    self.abort_input_after = Some(participation);
                    progressed |= retired;
                }
                units += 1;
                self.abort_input_next = false;
            } else {
                let p = self
                    .reader
                    .progress_ordered(1)
                    .expect("same original ordered control reader");
                units += p.units;
                progressed |= p.progressed;
                self.reader_more = p.more;
                self.abort_input_next = true;
                if p.units == 0 && !abort_due {
                    break;
                }
            }
        }
        SourceProgress {
            units,
            progressed,
            more: self.reader_more
                || matches!(self.abort, AbortInputRetirement::Open)
                    && self.abort_input_scan.has_work(inventory),
            retired: false,
        }
    }
    /// Called after real protocol calls/pumps/sends drain. These original
    /// recipient roles must close BEFORE normal actor sealing. The failure
    /// supervisor's actual exit remains a separate producer-exit prerequisite.
    pub(super) fn close_recipient_inputs(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
    ) -> (bool, bool) {
        use super::super::port::abort::AbortIngressRetirementProgress;
        let mut progressed = false;
        let mut more = false;
        if !self.reader_closed {
            self.reader_closed = self
                .reader
                .close_after_exit(runtime)
                .expect("same original control reader exit");
            progressed |= self.reader_closed;
        }
        // Constructing the cursor closes the same original intake. Keep that
        // transition at real Source retirement, never at installation.
        if matches!(self.abort, AbortInputRetirement::Open) {
            self.abort = AbortInputRetirement::Retiring(self.endpoints.abort_retirement());
            progressed = true;
        }
        if let AbortInputRetirement::Retiring(abort) = &mut self.abort {
            match abort
                .progress()
                .expect("same original Abort control positions")
            {
                AbortIngressRetirementProgress::Complete => {
                    self.abort = AbortInputRetirement::Closed;
                    progressed = true;
                }
                AbortIngressRetirementProgress::Advanced => {
                    progressed = true;
                    more = true;
                }
                AbortIngressRetirementProgress::Blocked => {}
            }
        }
        (progressed, more)
    }
    pub(super) fn producer_work_exited(&self) -> bool {
        if self.reader_closed
            && matches!(self.abort, AbortInputRetirement::Closed)
            && !self.endpoints.producer_exited()
        {
            self.endpoints.original_producer_exited();
            self.endpoints.requests.close_admission();
            return true;
        }
        false
    }
    pub(super) fn reader_closed(&self) -> bool {
        self.reader_closed
    }
    pub(super) fn take_input_author_return(
        &mut self,
    ) -> Option<super::releases::OriginalInputAuthorReturnObligation> {
        if self.author_return_issued {
            return None;
        }
        let original = super::releases::OriginalInputAuthorReturnObligation::after_close(
            self.inputs.clone(),
            self.index,
            self.endpoints.clone(),
        )?;
        self.author_return_issued = true;
        Some(original)
    }
    pub(super) fn take_establish_after_producer_exit(
        &mut self,
    ) -> Option<Arc<super::AttemptEstablishMaterial>> {
        if !self.endpoints.producer_exited() {
            return None;
        }
        self.establish.take()
    }
    pub(super) fn inputs_return_issued(&self) -> bool {
        self.author_return_issued && self.establish.is_none()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.rows.iter().all(Option::is_none)
    }
    pub(super) fn progress(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        executor: &ActorTransportExecutor,
        budget: usize,
    ) -> SourceProgress {
        let mut units = 0;
        let mut progressed = false;
        while units < budget && self.remaining != 0 {
            let index = self.next;
            self.next = (index + 1) % 3;
            self.remaining -= 1;
            units += 1;
            let Some(row) = &mut self.rows[index] else {
                continue;
            };
            assert!(
                row.transport.is_none(),
                "original Join tracked before retained job progress"
            );
            if let Some(token) = row.request {
                if runtime
                    .snapshot(self.endpoints.attempt().actor())
                    .expect("original attempt")
                    .failed
                {
                    let proof = runtime
                        .failure_proof_for(self.endpoints.attempt())
                        .expect("same failed attempt proof");
                    if let Some(original) = self
                        .endpoints
                        .requests
                        .take_one_after_attempt_failure(&proof)
                        .expect("same failed request owner")
                    {
                        // The table may return any original row, including a
                        // reply already committed before the AP failure fence.
                        let destination = self
                            .rows
                            .iter_mut()
                            .flatten()
                            .find(|row| row.request == Some(original.token))
                            .expect("same original failed request index");
                        assert!(destination.job.is_none());
                        destination.request = None;
                        destination.job = Some(original.job);
                        self.remaining = 3;
                        progressed = true;
                    }
                } else if let Some(original) = self.endpoints.requests.take_replied(token) {
                    row.request = None;
                    row.job = Some(original.job);
                    progressed = true;
                }
            }
            let Some(row) = &mut self.rows[index] else {
                continue;
            };
            if let Some(job) = row.job.take() {
                match executor.handoff(job) {
                    Ok(()) => {
                        let action = row.effect.protocol.id;
                        self.rows[index] = None;
                        progressed = true;
                        tracing::debug!(
                            target: "novarocks_original_credit",
                            actor = ?self.endpoints.attempt().actor(),
                            activation = ?self.endpoints.activation(),
                            action = ?action,
                            role_index = index,
                            "attempt original retained port job handed off and row released"
                        );
                    }
                    Err(job) => {
                        row.job = Some(job);
                    }
                }
            }
        }
        SourceProgress {
            units,
            progressed,
            more: self.remaining != 0,
            retired: self.is_empty(),
        }
    }
}
pub(super) fn request_token(message: &AttemptToLogical) -> Option<AttemptRequestToken> {
    match message.body {
        AttemptToLogicalBody::AuthorizationRequest { token, .. }
        | AttemptToLogicalBody::PreReadyReplacementQuery { token, .. }
        | AttemptToLogicalBody::EstablishSettlement { token, .. } => Some(token),
        _ => None,
    }
}
fn refused(original: FactPublishRefusal) -> EffectBindRefusal {
    EffectBindRefusal {
        error: match original.error {
            FactError::Runtime(error) => error,
            _ => ActorTurnError::PublicationTargetMismatch,
        },
        original: Box::new(original),
    }
}
