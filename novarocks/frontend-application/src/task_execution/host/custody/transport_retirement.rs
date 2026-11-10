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

use super::{
    CustodyError, CustodyRefusal, NativeDisposalCompletion, OriginalDisposalObligation,
    OriginalTransportJoinObligation, PhysicalInputRetirementObligation, PhysicalReturnError,
    SharedApplyJoinCompletion, actor_result::ReturnedActorTransportResult,
};
use crate::{
    native::apply_send_owner::{
        NativeDisposalRequest, RegistrationFailure, SharedApplyFunding, SharedApplyRetirement,
        complete_shared_join,
    },
    task_execution::host::transport::{
        SendReportRetirement, SendReportRetirementStep, SendReportReturn,
    },
};
use novarocks_control_runtime::{
    actor_turn::ActorTurnError, exit_custody::ExitCustody, reported_work::ReportedWork,
};
use novarocks_control_runtime_core::identity::ConsumerId;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{runtime::Handle, sync::futures::OwnedNotified};

#[must_use = "retain each original refused proof and its actual failure"]
pub(crate) enum TransportRetirementError {
    Custody(CustodyError),
    JoinFunding(CustodyError, SharedApplyJoinCompletion),
    NativeJoin(SharedApplyJoinCompletion, SharedApplyFunding),
    MissingCleanup(NativeDisposalRequest),
    StartCleanup(CustodyRefusal, NativeDisposalRequest),
    NativeCleanup(NativeDisposalCompletion),
    Physical(PhysicalReturnError),
    ReportInput(ActorTurnError),
}

#[must_use]
pub(crate) enum TransportRetirementStep {
    Advanced,
    Scanned,
    Report(SendReportReturn),
    Retired,
}

/// One original transport record driven by bounded Source turns. The caller
/// provides its real host waker and already admitted disposal roles. A
/// returned actor result can leave before Native reply aliases drain.
#[must_use = "drive or retain the original transport and cleanup custody"]
pub(crate) struct TransportRetirement {
    original: OriginalTransportJoinObligation,
    joining: bool,
    native: Option<SharedApplyRetirement>,
    native_wake: Option<Pin<Box<OwnedNotified>>>,
    cleanup: VecDeque<ReportedWork>,
    observer: ConsumerId,
    disposal: Option<OriginalDisposalObligation>,
    unused: Option<ExitCustody>,
    physical: Option<PhysicalInputRetirementObligation>,
    reports: Option<SendReportRetirement>,
    report_scan_remaining: usize,
    rejected_attachment: Option<RegistrationFailure>,
    failed: bool,
    retired: bool,
}

impl TransportRetirement {
    pub(crate) fn wake_layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<OwnedNotified>()
    }
    pub(crate) fn new(
        original: OriginalTransportJoinObligation,
        cleanup: VecDeque<ReportedWork>,
        observer: ConsumerId,
    ) -> Self {
        Self {
            original,
            joining: true,
            native: None,
            native_wake: None,
            cleanup,
            observer,
            disposal: None,
            unused: None,
            physical: None,
            reports: None,
            report_scan_remaining: 0,
            rejected_attachment: None,
            failed: false,
            retired: false,
        }
    }

    pub(crate) fn original(&self) -> &OriginalTransportJoinObligation {
        &self.original
    }

    pub(crate) fn active_disposal(&self) -> Option<&OriginalDisposalObligation> {
        self.disposal.as_ref()
    }

    pub(crate) fn take_rejected_attachment(&mut self) -> Option<RegistrationFailure> {
        self.rejected_attachment.take()
    }

    pub(crate) fn take_actor_result(
        &mut self,
    ) -> Result<Option<ReturnedActorTransportResult>, ReturnedActorTransportResult> {
        let Some(mut result) = self.original.take_actor_result() else {
            return Ok(None);
        };
        match result.take_send_report_retirement() {
            Ok(reports) => {
                self.report_scan_remaining = reports.as_ref().map_or(0, |r| r.positions());
                self.reports = reports;
                Ok(Some(result))
            }
            Err(_) => {
                self.failed = true;
                Err(result)
            }
        }
    }

    /// One charged continuation. Pending depends on original Join/Native
    /// wakes or on the caller's original input/Work retirement progress.
    pub(crate) fn poll_step(
        &mut self,
        runtime: &Handle,
        cx: &mut Context<'_>,
    ) -> Poll<Result<TransportRetirementStep, TransportRetirementError>> {
        if self.retired || self.failed {
            return Poll::Pending;
        }
        let result = self.advance(runtime, cx);
        match &result {
            Poll::Ready(Err(_)) => self.failed = true,
            Poll::Ready(Ok(TransportRetirementStep::Retired)) => self.retired = true,
            _ => (),
        }
        result
    }

    fn advance(
        &mut self,
        runtime: &Handle,
        cx: &mut Context<'_>,
    ) -> Poll<Result<TransportRetirementStep, TransportRetirementError>> {
        use TransportRetirementError as Error;
        use TransportRetirementStep::{Advanced, Retired};
        if self.joining {
            let completion = std::task::ready!(self.original.poll_shared_completion(cx))
                .map_err(Error::Custody)?;
            self.joining = false;
            if let Some(completion) = completion {
                let funding = match self.original.take_apply_funding(&completion) {
                    Ok(funding) => funding,
                    Err(error) => return Poll::Ready(Err(Error::JoinFunding(error, completion))),
                };
                self.native = Some(
                    complete_shared_join(completion, funding)
                        .map_err(|(completion, funding)| Error::NativeJoin(completion, funding))?,
                );
            } else if let Some((rejected, physical)) = self
                .original
                .take_unattached_retirement()
                .map_err(Error::Custody)?
            {
                self.rejected_attachment = rejected;
                self.physical = Some(physical);
            }
            return Poll::Ready(Ok(Advanced));
        }
        if let Some(physical) = self.physical.as_mut() {
            if physical.finish_one().map_err(Error::Physical)? {
                self.physical = None;
            }
            return Poll::Ready(Ok(Advanced));
        }
        if let Some(disposal) = self.disposal.as_mut() {
            let completion =
                std::task::ready!(disposal.poll_complete(cx)).map_err(Error::Custody)?;
            self.native
                .as_mut()
                .expect("active original Native row")
                .complete_disposal(completion)
                .map_err(Error::NativeCleanup)?;
            self.disposal = None;
            return Poll::Ready(Ok(Advanced));
        }
        if let Some(reports) = &mut self.reports {
            self.report_scan_remaining -= 1;
            match reports.advance().map_err(Error::ReportInput)? {
                SendReportRetirementStep::Advanced(returned) => {
                    self.report_scan_remaining = reports.positions();
                    return Poll::Ready(Ok(
                        returned.map_or(Advanced, TransportRetirementStep::Report)
                    ));
                }
                SendReportRetirementStep::Pending if self.report_scan_remaining != 0 => {
                    return Poll::Ready(Ok(TransportRetirementStep::Scanned));
                }
                SendReportRetirementStep::Pending => {
                    // This pass only inspected busy or closed positions.
                    // Check Native readiness, then wait for an actual wake.
                    self.report_scan_remaining = reports.positions();
                }
                SendReportRetirementStep::Retired => {
                    self.reports = None;
                    return Poll::Ready(Ok(Advanced));
                }
            }
        }
        if let Some(native) = self.native.as_mut() {
            let wait = self
                .native_wake
                .get_or_insert_with(|| Box::pin(native.notified()));
            if wait.as_mut().poll(cx).is_ready() {
                wait.as_mut().set(native.notified());
                if wait.as_mut().poll(cx).is_ready() {
                    cx.waker().wake_by_ref();
                }
            }
            if let Some(returned) = native.take_physical_return() {
                self.physical = Some(PhysicalInputRetirementObligation::new(returned));
                return Poll::Ready(Ok(Advanced));
            }
            if let Some(request) = native.take_disposal_request() {
                let Some(work) = self.cleanup.pop_front() else {
                    return Poll::Ready(Err(Error::MissingCleanup(request)));
                };
                self.disposal = Some(
                    OriginalDisposalObligation::start(runtime, work, self.observer, request)
                        .map_err(|(refused, request)| Error::StartCleanup(refused, request))?,
                );
                return Poll::Ready(Ok(Advanced));
            }
            if let Err(original) = self.native.take().unwrap().try_retire() {
                self.native = Some(original);
                return Poll::Pending;
            }
            self.native_wake = None;
            return Poll::Ready(Ok(Advanced));
        }
        if let Some(unused) = &self.unused {
            if unused
                .try_retire()
                .map_err(CustodyError::from)
                .map_err(Error::Custody)?
            {
                self.unused = None;
                return Poll::Ready(Ok(Advanced));
            }
        } else if let Some(work) = self.cleanup.pop_front() {
            let (custody, exit) = ExitCustody::from_admitted(work);
            self.unused = Some(custody);
            exit.revoke_unused_and_exit()
                .map_err(CustodyError::from)
                .map_err(Error::Custody)?;
            return Poll::Ready(Ok(Advanced));
        } else if self.reports.is_none() && self.original.try_retire().map_err(Error::Custody)? {
            return Poll::Ready(Ok(Retired));
        }
        let reaped = self
            .original
            .native
            .scope
            .reap_ready(1)
            .map_err(CustodyError::from)
            .map_err(Error::Custody)?;
        if reaped != 0 {
            Poll::Ready(Ok(Advanced))
        } else {
            Poll::Pending
        }
    }
}
