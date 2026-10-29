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

//! A bounded turn over one original R's source jobs and observed return.
//! The slots own the admitted J itself, including its authority and captures.
//! Protocol decisions remain inputs to the same Root actor.

use super::*;
use crate::control_runtime::establish_executors::{EstablishExecutorHandles, EstablishSubmitError};
use novarocks_control_runtime_core::identity::Generation;

#[derive(Debug)]
pub(crate) enum RequestIoError {
    Control(ControlRequestError),
    Submit(EstablishSubmitError),
    Observed(EstablishIngressOutcome),
}
impl From<ControlRequestError> for RequestIoError {
    fn from(error: ControlRequestError) -> Self {
        Self::Control(error)
    }
}
/// These counts describe real handoffs, not admission, reply publication or
/// whole Source exit. A retired R still requires its original retirement gate.
#[derive(Debug, Default)]
pub(crate) struct RequestIoProgress {
    pub visited: usize,
    pub dispatched: usize,
    pub submitted: usize,
    pub retired: usize,
}

impl ControlRequestCustody {
    pub(in crate::control_runtime) fn stage_reply_disposal(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ControlRequestError> {
        let WorkerAction::ControlReply(reply) = &ticket.effect else {
            return Err(ControlRequestError::WrongEffect);
        };
        if !self.ingress_issued
            || self.pending_disposal.is_some()
            || matches!(
                reply.result,
                RequestResult::TimeoutWhileAccepted
                    | RequestResult::Outcome(
                        OperationOutcome::Accepted | OperationOutcome::Idempotent
                    )
            )
        {
            return Ok(());
        }
        self.pending_disposal = self.begin_rejected_ingress_disposal(ticket)?;
        if self.pending_disposal.is_some() {
            self.note_progress();
        }
        Ok(())
    }
    pub(in crate::control_runtime) fn stage_getter(
        &mut self,
        original: EstablishIngressJob,
    ) -> Result<(), (ControlRequestError, EstablishIngressJob)> {
        if self
            .ingress_owner
            .as_ref()
            .is_none_or(|owner| !original.belongs_to_owner(owner))
        {
            return Err((ControlRequestError::WrongRequest, original));
        }
        if self.pending_getter.is_some()
            || self.ingress_outcome.is_some()
            || self.submitted
            || self.withdrawal
        {
            return Err((ControlRequestError::WrongPhase, original));
        }
        self.pending_getter = Some(original);
        self.note_progress();
        Ok(())
    }
    pub(in crate::control_runtime) fn stage_disposal(
        &mut self,
        original: EstablishDisposalJob,
    ) -> Result<(), (ControlRequestError, EstablishDisposalJob)> {
        if self
            .ingress_owner
            .as_ref()
            .is_none_or(|owner| !original.belongs_to_owner(owner))
        {
            return Err((ControlRequestError::WrongRequest, original));
        }
        if self.pending_disposal.is_some() {
            return Err((ControlRequestError::WrongPhase, original));
        }
        self.pending_disposal = Some(original);
        self.note_progress();
        Ok(())
    }
    fn dispatch_sources(
        &mut self,
        handles: &EstablishExecutorHandles,
    ) -> Result<usize, RequestIoError> {
        let mut dispatched = 0;
        if let Some(original) = self.pending_getter.take() {
            if let Err((error, original)) = handles.try_submit_getter(original) {
                self.pending_getter = Some(original);
                return Err(RequestIoError::Submit(error));
            }
            dispatched += 1;
        }
        if let Some(original) = self.pending_disposal.take() {
            if let Err((error, original)) = handles.try_submit_disposal(original) {
                self.pending_disposal = Some(original);
                return Err(RequestIoError::Submit(error));
            }
            dispatched += 1;
        }
        Ok(dispatched)
    }
    fn dispatch_notification(
        &mut self,
        handles: &EstablishExecutorHandles,
    ) -> Result<usize, RequestIoError> {
        if self.pending_notification.is_none() {
            self.pending_notification = self.notification.take_job().map(|job| {
                super::super::request_endpoint::RequestNotificationJob {
                    original: job,
                    notice: super::super::request_endpoint::RequestNotice(
                        self.endpoint.external.slot().clone(),
                    ),
                }
            });
        }
        let Some(job) = self.pending_notification.take() else {
            return Ok(0);
        };
        let before = self.endpoint.ready.source_capacity_revision();
        match handles.try_submit_notification(job) {
            Ok(()) => {
                self.endpoint
                    .ready
                    .clear_capacity_wait(self.endpoint.external.slot())?;
                Ok(1)
            }
            Err((error, job)) => {
                self.pending_notification = Some(job);
                match error {
                    EstablishSubmitError::Host(error)
                        if crate::control_runtime::request_endpoint::notification_submit_busy(
                            &error,
                        ) =>
                    {
                        self.endpoint
                            .ready
                            .park_capacity(self.endpoint.external.slot(), before)?;
                        Ok(0)
                    }
                    error => Err(RequestIoError::Submit(error)),
                }
            }
        }
    }
    pub(in crate::control_runtime) fn advance_protocol_observation(
        &mut self,
        handles: &EstablishExecutorHandles,
        ports: Option<&crate::TaskExecutionPorts>,
    ) -> Result<usize, RequestIoError> {
        if self.protocol_observation.failed() {
            return Err(ControlRequestError::SubmissionUnresolved.into());
        }
        if self.pending_protocol_observation.is_none() && self.protocol_observation.has_pending() {
            let ports = ports.ok_or(ControlRequestError::MissingObservationPort)?;
            self.pending_protocol_observation = self.protocol_observation.take_job(
                ports,
                self.scope.id(),
                self.work,
                super::super::request_endpoint::RequestNotice(
                    self.endpoint.external.slot().clone(),
                ),
            );
        }
        let Some(job) = self.pending_protocol_observation.take() else {
            return Ok(0);
        };
        let before = self.endpoint.ready.source_capacity_revision();
        match handles.try_submit_observation(job) {
            Ok(()) => {
                self.endpoint
                    .ready
                    .clear_capacity_wait(self.endpoint.external.slot())?;
                Ok(1)
            }
            Err((error, original)) => {
                self.pending_protocol_observation = Some(original);
                match error {
                    EstablishSubmitError::Host(error)
                        if crate::control_runtime::request_endpoint::notification_submit_busy(
                            &error,
                        ) =>
                    {
                        self.endpoint
                            .ready
                            .park_capacity(self.endpoint.external.slot(), before)?;
                        Ok(0)
                    }
                    error => Err(RequestIoError::Submit(error)),
                }
            }
        }
    }
    pub(in crate::control_runtime) fn advance_source_io(
        &mut self,
        handles: &EstablishExecutorHandles,
        generation: Generation,
        failure_work: WorkId,
    ) -> Result<RequestIoProgress, RequestIoError> {
        if self.ingress_source_phase() == Some(ExitRolePhase::Unresolved) {
            return Err(ControlRequestError::SourceUnresolved {
                input: self.input_id(),
            }
            .into());
        }
        if self.failed_submission.is_some() {
            return Err(ControlRequestError::SubmissionUnresolved.into());
        }
        let mut progress = RequestIoProgress {
            visited: 1,
            dispatched: self.dispatch_sources(handles)? + self.dispatch_notification(handles)?,
            ..Default::default()
        };
        if self.ingress_issued && !self.submitted && !self.withdrawal {
            // take_establish_ingress restores the original authority and caches
            // the complete neutral outcome before any branch can return.
            if self.ingress_outcome.is_none()
                && let Err(error) = self.take_establish_ingress()
            {
                if self.authority.is_none() && self.role.phase() == ExitRolePhase::Unresolved {
                    // This existing gate verifies Taken/Unknown and the actual
                    // lost original, without inventing an observed call result.
                    self.submit_ingress_owner_failure(failure_work)
                        .map_err(|refusal| RequestIoError::Control(refusal.error))?;
                    progress.submitted += 1;
                } else {
                    return Err(error.into());
                }
            }
            if !self.submitted {
                match self.ingress_outcome {
                    Some(EstablishIngressOutcome::Projected(projection)) => {
                        self.submit(ControlInput::Establish {
                            request: projection.operation,
                            context: projection.context,
                            ticket: projection.ticket,
                            payload: projection.payload,
                            semantics: projection.semantics,
                            generation,
                        })
                        .map_err(|refusal| RequestIoError::Control(refusal.error))?;
                        progress.submitted += 1;
                    }
                    Some(EstablishIngressOutcome::GetterPanicked) => {
                        self.submit_ingress_owner_failure(failure_work)
                            .map_err(|refusal| RequestIoError::Control(refusal.error))?;
                        progress.submitted += 1;
                    }
                    Some(observed) => return Err(RequestIoError::Observed(observed)),
                    None => {}
                }
            }
        }
        if self.pending_disposal.is_none() {
            // These original gates require M's actual loan/call retirement or
            // a cancelled replay's real Context leg. A reply alone is no proof.
            self.pending_disposal = self
                .take_consumed_creator_disposal()?
                .or_else(|| self.take_retired_route_disposal());
        }
        progress.dispatched += self.dispatch_sources(handles)?;
        Ok(progress)
    }
}
