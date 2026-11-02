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

//! A failed admission's rich payload stays with its one original R. Only the
//! actual lifecycle worker destroys it; no replacement Work or input is issued.

use super::*;
use crate::control_runtime::{
    context_ready::ContextReadyNotice,
    node_api::WorkerIngressResponsibility,
    process_control::rejected_ingress::{PanicPayload, RejectedIngressPayload},
};
use novarocks_control_runtime::owned_return::{OwnedReturnReader, OwnedReturnSender};
use novarocks_control_runtime_core::completion::ReturnState;
use std::{mem::ManuallyDrop, sync::Mutex};

pub(crate) struct SameRRawRefusal {
    pub error: ContextRequestError,
    pub payload: RejectedIngressPayload,
    pub ingress: WorkerIngressResponsibility,
}
impl std::fmt::Debug for SameRRawRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SameRRawRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum RejectedCaptureFailure {
    Start(ExitRoleError),
    Exit(ExitRoleError),
    Return(RuntimeError),
    DropPanicked,
    JobLost,
}
impl std::fmt::Display for RejectedCaptureFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Start(error) => write!(f, "Rejected capture could not start: {error}"),
            Self::Exit(error) => write!(f, "Rejected capture exit observation failed: {error}"),
            Self::Return(error) => write!(f, "Rejected capture original return failed: {error}"),
            Self::DropPanicked => f.write_str(
                "Rejected capture destructor panicked; original custody remains unresolved",
            ),
            Self::JobLost => {
                f.write_str("Rejected capture original job was lost; custody remains unresolved")
            }
        }
    }
}
impl std::error::Error for RejectedCaptureFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Start(error) | Self::Exit(error) => Some(error),
            Self::Return(error) => Some(error),
            Self::DropPanicked | Self::JobLost => None,
        }
    }
}
struct State {
    authority: Option<ExitAuthority>,
    payload: Option<ManuallyDrop<RejectedIngressPayload>>,
    sender: Option<OwnedReturnSender<()>>,
    panics: Vec<PanicPayload>,
    failure: Option<RejectedCaptureFailure>,
}
struct Custody {
    work: ScopedWorkRef,
    role: ExitRole,
    notice: ContextReadyNotice,
    state: Mutex<State>,
    // Pure original Native admission bookkeeping. Unknown cleanup retains it;
    // successful cleanup releases it only after the last actual holder exits.
    _ingress: WorkerIngressResponsibility,
    retained: Mutex<Option<Arc<Custody>>>,
}
pub(crate) struct RejectedAdmissionPartial {
    owner: Arc<Custody>,
    ledger: RuntimeLedger,
    header: WorkRetirementObserver,
    returned: OwnedReturnReader<()>,
    return_taken: bool,
    sources: Option<SourceSlotIssuePartial>,
    outer: Option<ReservationBundle>,
    job: Option<RejectedCaptureJob>,
    waiting_revision: Option<u64>,
    reap_pending: bool,
}
impl RejectedAdmissionPartial {
    fn capture(
        mut partial: Box<ContextRequestPartial>,
        payload: RejectedIngressPayload,
        ingress: WorkerIngressResponsibility,
        notice: ContextReadyNotice,
    ) -> Result<Self, (Box<ContextRequestPartial>, SameRRawRefusal)> {
        let report = partial.report.take().expect("validated original R report");
        let reported = match ReportedWork::try_new(partial.work, partial.capacity, report) {
            Ok(reported) => reported,
            Err(original) => {
                partial.work = original.work;
                partial.capacity = original.capacity;
                partial.report = Some(original.report);
                return Err((
                    partial,
                    SameRRawRefusal {
                        error: ContextRequestError::Binding(original.error),
                        payload,
                        ingress,
                    },
                ));
            }
        };
        // This actual report validates Queued/unclaimed/no exit before raw is
        // attached. Capture binding itself never starts the original Work.
        let (sender, returned) = match reported.report().claim_owned_return::<()>() {
            Ok(original) => original,
            Err(error) => {
                let (work, capacity, report) = reported.into_parts();
                partial.work = work;
                partial.capacity = capacity;
                partial.report = Some(report);
                partial.unresolved = true;
                return Err((
                    partial,
                    SameRRawRefusal {
                        error: ContextRequestError::Runtime(error),
                        payload,
                        ingress,
                    },
                ));
            }
        };
        let work = reported.work().reference();
        let (role, authority) = ExitRole::from_admitted(reported);
        let owner = Arc::new(Custody {
            work,
            role,
            notice,
            state: Mutex::new(State {
                authority: Some(authority),
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
            .expect("original rejected R retention") = Some(owner.clone());
        Ok(Self {
            ledger: partial.ledger,
            header: partial.work_retirement,
            returned,
            return_taken: false,
            sources: partial.sources,
            outer: partial.outer,
            job: Some(RejectedCaptureJob {
                owner: owner.clone(),
                active: true,
            }),
            owner,
            waiting_revision: None,
            reap_pending: false,
        })
    }
    pub(super) fn try_retire(&mut self) -> Result<bool, ContextRequestError> {
        self.reap_pending = false;
        if let Some(failure) = self
            .owner
            .state
            .lock()
            .expect("original rejected R state")
            .failure
        {
            return Err(ContextRequestError::RejectedCapture(failure));
        }
        if !self.return_taken {
            if self
                .returned
                .try_take()
                .map_err(ContextRequestError::Runtime)?
                .is_none()
            {
                return Ok(false);
            }
            self.return_taken = true;
        }
        if let Some(sources) = &mut self.sources {
            if !sources
                .try_retire(&self.ledger)
                .map_err(ContextRequestError::SourceSlots)?
            {
                self.reap_pending = true;
                return Ok(false);
            }
            self.sources = None;
        }
        if !self
            .owner
            .role
            .actual_exit_ready()
            .map_err(ContextRequestError::Exit)?
        {
            return Ok(false);
        }
        // The actual input was never published and its unused permit was
        // revoked before capture. No input pins this header. Keep Requests1
        // in its original bundle (or Task outer) until this exact retirement.
        if !self.header.is_retired()
            && !self
                .ledger
                .reap_observed_header(&self.header)
                .map_err(ContextRequestError::Runtime)?
        {
            self.reap_pending = true;
            return Ok(false);
        }
        if self.owner.role.phase() != ExitRolePhase::Retired
            && !self
                .owner
                .role
                .try_retire()
                .map_err(ContextRequestError::Exit)?
        {
            return Ok(false);
        }
        if let Some(outer) = &mut self.outer {
            let remaining = outer.resources().map_err(ContextRequestError::Runtime)?;
            outer
                .release_quota(&remaining.quota)
                .and_then(|()| outer.finish())
                .map_err(ContextRequestError::Runtime)?;
            self.outer = None;
        }
        self.owner
            .retained
            .lock()
            .expect("same rejected R retention")
            .take();
        Ok(true)
    }
    fn advance(
        &mut self,
        ports: &super::super::task_driver::TaskDriverPorts,
        registration: &novarocks_control_runtime::driver::ActorRegistration,
        context: novarocks_execution_contract::QueryContextRef,
    ) -> Result<bool, ContextRequestError> {
        if self.job.is_some() {
            let capacity = ports
                .capacity
                .as_ref()
                .ok_or(ContextRequestError::MissingNotificationPort)?;
            if self.waiting_revision == Some(capacity.revision()) {
                // Another Context event can visit this same original row,
                // but only a physical capacity change retries its Full Job.
                return Ok(false);
            }
        }
        if let Some(job) = self.job.take() {
            let Some(executor) = &ports.lifecycle else {
                self.job = Some(job);
                return Err(ContextRequestError::MissingNotificationPort);
            };
            let Some(capacity) = &ports.capacity else {
                self.job = Some(job);
                return Err(ContextRequestError::MissingNotificationPort);
            };
            let before = capacity.revision();
            match executor.try_submit_for_group(
                super::super::task_driver::TaskDriverHostJob::DisposeRejectedPartial(job),
                crate::control_runtime::preparation::HostFairnessGroup::Context(context),
            ) {
                Ok(()) => self.waiting_revision = None,
                Err((error, original)) => {
                    let super::super::task_driver::TaskDriverHostJob::DisposeRejectedPartial(job) =
                        original
                    else {
                        unreachable!("same original rejected R Job");
                    };
                    self.job = Some(job);
                    if !crate::control_runtime::request_endpoint::notification_submit_busy(&error) {
                        return Err(ContextRequestError::NotificationSubmit(error));
                    }
                    self.waiting_revision = Some(before);
                    capacity
                        .register_waiter(registration, before)
                        .map_err(|_| ContextRequestError::MissingNotificationPort)?;
                    // Store -> arm -> probe: a tail between the actual Full
                    // result and waiter insertion cannot lose this retry.
                    if capacity.revision() != before {
                        self.owner.notice.notify();
                    }
                    return Ok(false);
                }
            }
        }
        self.try_retire()
    }
}

#[must_use = "Run this original R's rich capture on its actual lifecycle host"]
pub(crate) struct RejectedCaptureJob {
    owner: Arc<Custody>,
    active: bool,
}
impl RejectedCaptureJob {
    pub(crate) fn queue_key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        (self.owner.work.scope, self.owner.work.work)
    }
    pub(crate) fn run(mut self) {
        let (mut authority, payload, sender) = {
            let mut state = self
                .owner
                .state
                .lock()
                .expect("original rejected R capture");
            (
                state.authority.take().expect("original R authority"),
                state.payload.take().expect("original rejected R payload"),
                state.sender.take().expect("original R return"),
            )
        };
        let failure = match authority.start() {
            Err(error) => {
                self.owner
                    .state
                    .lock()
                    .expect("original rejected R start")
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
                        .expect("original rejected R panics")
                        .panics = panics;
                    drop(authority);
                    drop(sender);
                    Some(RejectedCaptureFailure::DropPanicked)
                } else if let Err(error) = authority.withdraw_unpublished_and_exit() {
                    drop(sender);
                    Some(RejectedCaptureFailure::Exit(error))
                } else if let Err(original) = sender.publish(()) {
                    let error = original.error;
                    // Preserve the actual refused sender/value; never replace
                    // it with a fabricated return or a second report fill.
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
            .expect("original rejected R outcome")
            .failure = failure;
        self.active = false;
        self.owner.notice.notify();
    }
}
impl Drop for RejectedCaptureJob {
    fn drop(&mut self) {
        if self.active {
            let (authority, sender) = {
                let mut state = self
                    .owner
                    .state
                    .lock()
                    .expect("original rejected R lost Job");
                state.failure = Some(RejectedCaptureFailure::JobLost);
                (state.authority.take(), state.sender.take())
            };
            // Stable raw and pure Native backing remain in the original
            // retained custody. Missing Job is Unknown, never unused exit.
            drop(authority);
            drop(sender);
            self.owner.notice.notify();
        }
    }
}

/// Cursor metadata only; the original partial map remains the sole owner.
#[derive(Default)]
pub(in crate::control_runtime::context_host) struct AdmissionPartialSweep {
    version: Option<(u64, Option<u64>)>,
    after: Option<ScopedWorkRef>,
    until: Option<ScopedWorkRef>,
    complete: bool,
}
impl ContextHostRecord {
    pub(crate) fn capture_failed_admission_partial(
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
        let same_context = match &payload {
            RejectedIngressPayload::Create { payload, binding } => {
                payload.request.context() == self.identity
                    && binding.as_ref().is_none_or(|binding| {
                        binding.context == self.identity
                            && binding.registration.same_registration(self.registration())
                    })
            }
            RejectedIngressPayload::TaskDomain(request) => request
                .identity()
                .verify_query_context(self.identity)
                .is_ok(),
            RejectedIngressPayload::ContextDomain(request) => request.context() == self.identity,
        };
        if !same_context {
            return Err(refusal(ContextRequestError::WrongRequest, payload, ingress));
        }
        let Some(ContextRequestAdmissionPartial::Unused(partial)) =
            self.resources.request_partials.get_mut(&work)
        else {
            return Err(refusal(ContextRequestError::WrongPhase, payload, ingress));
        };
        if partial.work.reference() != work
            || !partial.work.scope().same_scope(&self.request_scope)
            || partial.unresolved
            || partial.exited
            || partial.report_revoked
            || partial.report.is_none()
        {
            return Err(refusal(ContextRequestError::WrongPhase, payload, ingress));
        }
        match partial
            .report
            .as_ref()
            .expect("validated report")
            .owned_return_state()
        {
            Ok(ReturnState::Unclaimed) => {}
            Ok(_) => return Err(refusal(ContextRequestError::WrongPhase, payload, ingress)),
            Err(error) => {
                return Err(refusal(
                    ContextRequestError::Runtime(error),
                    payload,
                    ingress,
                ));
            }
        }
        if let Some(permit) = partial.permit.take()
            && let Err(error) = permit.revoke_unused()
        {
            partial.unresolved = true;
            return Err(refusal(
                ContextRequestError::Driver(error),
                payload,
                ingress,
            ));
        }
        let notice = self.ready_notice().clone();
        let Some(ContextRequestAdmissionPartial::Unused(partial)) =
            self.resources.request_partials.remove(&work)
        else {
            unreachable!("same serial Context owns its unused R partial");
        };
        match RejectedAdmissionPartial::capture(partial, payload, ingress, notice.clone()) {
            Ok(original) => {
                self.resources.request_partials.insert(
                    work,
                    ContextRequestAdmissionPartial::RejectedCapture(Box::new(original)),
                );
                notice.notify();
                Ok(())
            }
            Err((partial, original)) => {
                self.resources
                    .request_partials
                    .insert(work, ContextRequestAdmissionPartial::Unused(partial));
                Err(original)
            }
        }
    }

    pub(in crate::control_runtime::context_host) fn advance_admission_partials(
        &mut self,
        ports: &super::super::task_driver::TaskDriverPorts,
        budget: usize,
    ) -> Result<(), ContextRequestError> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        if budget == 0 || self.resources.request_partials.is_empty() {
            return Ok(());
        }
        let version = (
            self.ready_notice().version(),
            ports.capacity.as_ref().map(|signal| signal.revision()),
        );
        let sweep = &mut self.task_driver.admission_partial_sweep;
        if sweep.version.is_none() || (sweep.complete && sweep.version != Some(version)) {
            sweep.version = Some(version);
            sweep.after = None;
            sweep.until = self
                .resources
                .request_partials
                .last_key_value()
                .map(|(&key, _)| key);
            sweep.complete = sweep.until.is_none();
        }
        if sweep.complete {
            return Ok(());
        }
        let registration = self.registration().clone();
        let context = self.identity;
        for _ in 0..budget {
            let sweep = &self.task_driver.admission_partial_sweep;
            let Some(until) = sweep.until else {
                break;
            };
            let Some((&work, _)) = self
                .resources
                .request_partials
                .range((sweep.after.map_or(Unbounded, Excluded), Included(until)))
                .next()
            else {
                break;
            };
            self.task_driver.admission_partial_sweep.after = Some(work);
            let (retired, unused, progressed) = match self.resources.request_partials.get_mut(&work)
            {
                Some(ContextRequestAdmissionPartial::Unused(original)) => {
                    let progress = original.try_retire_progress()?;
                    (progress.retired, true, progress.progressed)
                }
                Some(ContextRequestAdmissionPartial::RejectedCapture(original)) => {
                    let retired = original.advance(ports, &registration, context)?;
                    if original.reap_pending {
                        self.ready_notice().notify();
                    }
                    (retired, false, false)
                }
                // A bound cut is Unknown custody, never an unused admission.
                Some(ContextRequestAdmissionPartial::BoundTask(_)) | None => continue,
            };
            if retired {
                self.resources.request_partials.remove(&work);
                if unused {
                    self.notify_host_progress();
                }
            } else if unused && progressed {
                // Only actual work in this bounded attempt justifies another
                // sweep. A blocked header/report waits for its real source.
                self.ready_notice().notify();
            }
        }
        let sweep = &mut self.task_driver.admission_partial_sweep;
        sweep.complete = self
            .resources
            .request_partials
            .range((
                sweep.after.map_or(Unbounded, Excluded),
                Included(sweep.until.expect("original sweep upper key")),
            ))
            .next()
            .is_none();
        if !sweep.complete {
            // Continue only this finite metadata sweep, without incrementing
            // the external notice revision and scheduling another live-I/O sweep.
            self.control_runtime
                .notify_host_actor(&registration)
                .map_err(ContextRequestError::Driver)?;
        }
        Ok(())
    }
}
