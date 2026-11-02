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

//! Physical destruction of a never-loaded capture in its original paid R.

//! Uncaptured domain raw returns to its already-admitted R. The physical Job
//! borrows that R's unique authority and returns it through its original report.

use super::rejected_capture::RejectedCaptureFailure;
use super::*;
use crate::control_runtime::{
    node_api::WorkerIngressResponsibility,
    process_control::rejected_ingress::{PanicPayload, RejectedIngressPayload},
    request_endpoint::RequestNotice,
};
use novarocks_control_runtime::owned_return::{OwnedReturnReader, OwnedReturnSender};
use novarocks_execution_contract::{AdvanceQueryContextDomain, UpdateTask};
use std::{mem::ManuallyDrop, sync::Mutex};

struct State {
    authority: Option<ExitAuthority>,
    already_started: bool,
    payload: Option<ManuallyDrop<RejectedIngressPayload>>,
    sender: Option<OwnedReturnSender<ExitAuthority>>,
    panics: Vec<PanicPayload>,
    failure: Option<RejectedCaptureFailure>,
}
struct Custody {
    work: ScopedWorkRef,
    notice: RequestNotice,
    state: Mutex<State>,
    _ingress: WorkerIngressResponsibility,
    retained: Mutex<Option<Arc<Custody>>>,
}
pub(crate) struct RejectedDomainCapture {
    owner: Arc<Custody>,
    reader: OwnedReturnReader<ExitAuthority>,
    returned: bool,
    job: Option<RejectedDomainJob>,
    waiting_revision: Option<u64>,
}
impl RejectedDomainCapture {
    fn new(
        original: &mut ContextRequestCustody,
        payload: RejectedIngressPayload,
        ingress: WorkerIngressResponsibility,
        start_original: bool,
    ) -> Result<Self, SameRRawRefusal> {
        let (sender, reader) = match original.report.claim_owned_return::<ExitAuthority>() {
            Ok(original) => original,
            Err(error) => {
                return Err(SameRRawRefusal {
                    error: ContextRequestError::Runtime(error),
                    payload,
                    ingress,
                });
            }
        };
        if start_original {
            if let Err(error) = original
                .authority
                .as_mut()
                .expect("original inspection R authority")
                .start()
            {
                // This unique return cell is still unpublished. Positively
                // revoke it before returning the original raw; an impossible
                // revocation failure is a runtime ledger invariant failure.
                sender
                    .revoke_unused()
                    .unwrap_or_else(|_| std::process::abort());
                return Err(SameRRawRefusal {
                    error: ContextRequestError::Exit(error),
                    payload,
                    ingress,
                });
            }
            original.started = true;
        }
        // The same serial R owns the checked authority. No Work, report,
        // Source, input or financial position is admitted by this move.
        let authority = original
            .authority
            .take()
            .expect("original domain R authority");
        let owner = Arc::new(Custody {
            work: original.work,
            notice: RequestNotice(original.endpoint.external().slot().clone()),
            state: Mutex::new(State {
                authority: Some(authority),
                already_started: original.started,
                payload: Some(ManuallyDrop::new(payload)),
                sender: Some(sender),
                panics: Vec::new(),
                failure: None,
            }),
            _ingress: ingress,
            retained: Mutex::new(None),
        });
        *owner
            .retained
            .lock()
            .expect("same original domain R retention") = Some(owner.clone());
        Ok(Self {
            job: Some(RejectedDomainJob {
                owner: owner.clone(),
                active: true,
            }),
            owner,
            reader,
            returned: false,
            waiting_revision: None,
        })
    }
    pub(super) fn returned(&self) -> bool {
        self.returned
    }
    pub(super) fn take_authority(&mut self) -> Result<Option<ExitAuthority>, ContextRequestError> {
        if self.returned {
            return Ok(None);
        }
        if let Some(failure) = self
            .owner
            .state
            .lock()
            .expect("same original domain R result")
            .failure
        {
            return Err(ContextRequestError::RejectedCapture(failure));
        }
        let authority = self
            .reader
            .try_take()
            .map_err(ContextRequestError::Runtime)?;
        if authority.is_some() {
            self.returned = true;
        }
        Ok(authority)
    }
    pub(super) fn release_retention(&mut self) {
        assert!(
            self.returned,
            "raw Drop and same original authority must return before R retirement"
        );
        self.owner
            .retained
            .lock()
            .expect("retired original domain R retention")
            .take();
    }
    pub(super) fn advance(
        &mut self,
        ports: &super::super::task_driver::TaskDriverPorts,
        registration: &novarocks_control_runtime::driver::ActorRegistration,
        context: novarocks_execution_contract::QueryContextRef,
        endpoint: &ContextRequestEndpoint,
    ) -> Result<(), ContextRequestError> {
        let Some(job) = self.job.take() else {
            return Ok(());
        };
        let (Some(executor), Some(capacity)) = (&ports.lifecycle, &ports.capacity) else {
            self.job = Some(job);
            return Err(ContextRequestError::MissingNotificationPort);
        };
        let before = capacity.revision();
        if self.waiting_revision == Some(before) {
            self.job = Some(job);
            return Ok(());
        }
        match executor.try_submit_for_group(
            super::super::task_driver::TaskDriverHostJob::DisposeRejectedDomain(job),
            crate::control_runtime::preparation::HostFairnessGroup::Context(context),
        ) {
            Ok(()) => {
                self.waiting_revision = None;
                endpoint
                    .ready()
                    .clear_capacity_wait(endpoint.external().slot())
                    .map_err(ContextRequestError::Ready)?;
            }
            Err((error, original)) => {
                let super::super::task_driver::TaskDriverHostJob::DisposeRejectedDomain(job) =
                    original
                else {
                    unreachable!("same original domain R raw Drop Job");
                };
                self.job = Some(job);
                if !crate::control_runtime::request_endpoint::notification_submit_busy(&error) {
                    return Err(ContextRequestError::NotificationSubmit(error));
                }
                self.waiting_revision = Some(before);
                endpoint
                    .ready()
                    .park_capacity(endpoint.external().slot(), before)
                    .map_err(ContextRequestError::Ready)?;
                capacity
                    .register_waiter(registration, before)
                    .map_err(|_| ContextRequestError::MissingNotificationPort)?;
                if capacity.revision() != before {
                    self.owner.notice.notify();
                }
            }
        }
        Ok(())
    }
}

#[must_use = "Return the same R authority only after its actual raw Drop"]
pub(crate) struct RejectedDomainJob {
    owner: Arc<Custody>,
    active: bool,
}
impl RejectedDomainJob {
    pub(crate) fn queue_key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        (self.owner.work.scope, self.owner.work.work)
    }
    pub(crate) fn run(mut self) {
        let (mut authority, already_started, payload, sender) = {
            let mut state = self
                .owner
                .state
                .lock()
                .expect("original domain R raw custody");
            (
                state.authority.take().expect("same original R authority"),
                state.already_started,
                state.payload.take().expect("same original raw domain"),
                state.sender.take().expect("same original authority return"),
            )
        };
        // A healthy metadata R starts before its neutral Core input is filled.
        // Its same authority still owns this later raw Drop; the failed-only
        // path instead lends an admitted, never-started original authority.
        let started = if already_started {
            Ok(())
        } else {
            authority.start()
        };
        let failure = match started {
            Err(error) => {
                self.owner
                    .state
                    .lock()
                    .expect("original domain R start refusal")
                    .payload = Some(payload);
                drop(authority);
                drop(sender);
                Some(RejectedCaptureFailure::Start(error))
            }
            Ok(()) => {
                let panics = ManuallyDrop::into_inner(payload).dispose();
                if !panics.is_empty() {
                    self.owner
                        .state
                        .lock()
                        .expect("original domain R raw panic")
                        .panics = panics;
                    drop(authority);
                    drop(sender);
                    Some(RejectedCaptureFailure::DropPanicked)
                } else if let Err(original) = sender.publish(authority) {
                    let error = original.error;
                    std::mem::forget(original);
                    Some(RejectedCaptureFailure::Return(error))
                } else {
                    None
                }
            }
        };
        self.owner
            .state
            .lock()
            .expect("same original domain R outcome")
            .failure = failure;
        self.active = false;
        self.owner.notice.notify();
    }
}
impl Drop for RejectedDomainJob {
    fn drop(&mut self) {
        if self.active {
            let (authority, sender) = {
                let mut state = self.owner.state.lock().expect("original domain R lost Job");
                state.failure = Some(RejectedCaptureFailure::JobLost);
                (state.authority.take(), state.sender.take())
            };
            drop(authority);
            drop(sender);
            self.owner.notice.notify();
        }
    }
}

impl ContextHostRecord {
    /// Retain a healthy metadata inspection's original rich input in the same
    /// already-paid R. The subsequent neutral Core input can answer before
    /// this physical Drop returns; only actual return permits R retirement.
    pub(crate) fn capture_task_update_inspection(
        &mut self,
        work: ScopedWorkRef,
        raw: UpdateTask,
        ingress: WorkerIngressResponsibility,
    ) -> Result<(), (ContextRequestError, UpdateTask, WorkerIngressResponsibility)> {
        let registration = self.registration().clone();
        let request_scope = self.request_scope().clone();
        let runtime = self.control_runtime.clone();
        let context = self.identity;
        let actor = self.actor;
        let Some(original) = self.resources.requests.get_mut(&work) else {
            return Err((ContextRequestError::UnknownRequest, raw, ingress));
        };
        let task = raw.identity();
        let metadata = original.endpoint.metadata();
        let same_request = original.request.is_some_and(|request| {
            request.author == work
                && request.operation == metadata.operation
                && request.deadline == metadata.deadline
                && original
                    .permit
                    .as_ref()
                    .is_some_and(|permit| permit.id() == request.token)
        });
        if original.task_update_inspection != Some(task)
            || original.expected_task != Some(task)
            || task.verify_query_context(context).is_err()
            || raw.envelope().operation_id() != metadata.operation
            || original.work != work
            || work.scope != request_scope.id()
            || !original.scope.same_scope(&request_scope)
            || !original.runtime.same_runtime(&runtime)
            || original.actor != actor
            || !original
                .core_consumer
                .registration()
                .same_registration(&registration)
            || !same_request
            || original.submitted
            || original.started
            || original.authority.is_none()
            || original.task_route.is_some()
            || original.domain.is_some()
            || original.create.is_some()
            || original.rejected_domain.is_some()
            || original.failed_message.is_some()
            || original.deferred_root.is_some()
            || original.deferred_task_read.is_some()
            || original.observation.is_some()
            || original.read_payload.is_some()
            || original.retired_task_read.is_some()
            || original.unresolved_revocation
        {
            return Err((ContextRequestError::WrongRequest, raw, ingress));
        }
        // The original report's return must be claimed while this R is still
        // queued, before starting and lending its same authority to disposal.
        let capture = RejectedDomainCapture::new(
            original,
            RejectedIngressPayload::TaskDomain(raw),
            ingress,
            true,
        )
        .map_err(|original| {
            let RejectedIngressPayload::TaskDomain(raw) = original.payload else {
                unreachable!("same original inspection Task input");
            };
            (original.error, raw, original.ingress)
        })?;
        original.rejected_domain = Some(capture);
        original.endpoint.external().notify();
        Ok(())
    }

    fn capture_failed_uncaptured_domain(
        &mut self,
        work: ScopedWorkRef,
        payload: RejectedIngressPayload,
        ingress: WorkerIngressResponsibility,
    ) -> Result<(), SameRRawRefusal> {
        let refusal = |error, payload, ingress| SameRRawRefusal {
            error,
            payload,
            ingress,
        };
        match self.control_runtime.snapshot(self.actor) {
            Err(error) => {
                return Err(refusal(
                    ContextRequestError::Driver(error),
                    payload,
                    ingress,
                ));
            }
            Ok(actual) if !actual.failed => {
                return Err(refusal(ContextRequestError::WrongPhase, payload, ingress));
            }
            Ok(_) => {}
        }
        let Some(original) = self.resources.requests.get_mut(&work) else {
            return Err(refusal(
                ContextRequestError::UnknownRequest,
                payload,
                ingress,
            ));
        };
        let same_request = match &payload {
            RejectedIngressPayload::TaskDomain(raw) => {
                original
                    .task_route
                    .as_ref()
                    .is_some_and(|route| route.task() == raw.identity())
                    && original.expected_task == Some(raw.identity())
                    && raw.identity().verify_query_context(self.identity).is_ok()
                    && raw.envelope().operation_id() == original.endpoint.metadata().operation
            }
            RejectedIngressPayload::ContextDomain(raw) => {
                original.task_route.is_none()
                    && original.expected_task.is_none()
                    && raw.context() == self.identity
                    && raw.envelope().operation_id() == original.endpoint.metadata().operation
            }
            RejectedIngressPayload::Create { .. } => false,
        };
        if !same_request
            || original.work != work
            || original.submitted
            || original.started
            || original.authority.is_none()
            || original.create.is_some()
            || original.rejected_domain.is_some()
            || original.failed_message.is_some()
            || original.deferred_root.is_some()
            || original.observation.is_some()
            || original.read_payload.is_some()
            || original.retired_task_read.is_some()
            || original.unresolved_revocation
            || original
                .domain
                .as_ref()
                .is_some_and(|domain| !domain.is_uncaptured_unused())
        {
            return Err(refusal(ContextRequestError::WrongPhase, payload, ingress));
        }
        let capture = RejectedDomainCapture::new(original, payload, ingress, false)?;
        original.rejected_domain = Some(capture);
        original.endpoint.external().notify();
        Ok(())
    }
    pub(crate) fn capture_refused_task_domain(
        &mut self,
        work: ScopedWorkRef,
        raw: UpdateTask,
        ingress: WorkerIngressResponsibility,
    ) -> Result<(), (ContextRequestError, UpdateTask, WorkerIngressResponsibility)> {
        self.capture_failed_uncaptured_domain(
            work,
            RejectedIngressPayload::TaskDomain(raw),
            ingress,
        )
        .map_err(|original| {
            let RejectedIngressPayload::TaskDomain(raw) = original.payload else {
                unreachable!("same original Task domain refusal");
            };
            (original.error, raw, original.ingress)
        })
    }
    pub(crate) fn capture_refused_context_domain(
        &mut self,
        work: ScopedWorkRef,
        raw: AdvanceQueryContextDomain,
        ingress: WorkerIngressResponsibility,
    ) -> Result<
        (),
        (
            ContextRequestError,
            AdvanceQueryContextDomain,
            WorkerIngressResponsibility,
        ),
    > {
        self.capture_failed_uncaptured_domain(
            work,
            RejectedIngressPayload::ContextDomain(raw),
            ingress,
        )
        .map_err(|original| {
            let RejectedIngressPayload::ContextDomain(raw) = original.payload else {
                unreachable!("same original Context domain refusal");
            };
            (original.error, raw, original.ingress)
        })
    }
}
