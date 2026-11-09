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

//! Source-owned actual transport and disposal Joins. Only these spawn paths
//! install a Join. A Native completion consumes its original row obligation
//! after the same Join and original role observations, never a numeric hint.

use crate::native::apply_send_owner::{
    JoinFacts, NativeDisposalBody, NativeDisposalRequest, NativeDisposalReturn,
    SharedApplyJoinObligation,
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyError, ExitCustodyPhase, ExitObligation},
    exit_work::{ExitWorkError, PanicSink, run_blocking},
    reported_work::ReportedWork,
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{completion::ConsumerKind, identity::ConsumerId};
use std::{
    future::Future,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
};
use tokio::{
    runtime::Handle,
    task::{JoinError, JoinHandle},
};

#[derive(Debug)]
pub(crate) enum CustodyError {
    Report(RuntimeError),
    Exit(ExitCustodyError),
    WrongObserver,
    MissingOriginalJoin,
    OriginalHandoffFailed,
    OriginalJoinFailed,
    OriginalWorkFailed,
    OriginalCapturesUnresolved,
    CompletionAlreadyIssued,
}
impl From<RuntimeError> for CustodyError {
    fn from(e: RuntimeError) -> Self {
        Self::Report(e)
    }
}
impl From<ExitCustodyError> for CustodyError {
    fn from(e: ExitCustodyError) -> Self {
        Self::Exit(e)
    }
}
impl std::fmt::Display for CustodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "original Source custody failure: {self:?}")
    }
}
impl std::error::Error for CustodyError {}
pub(crate) struct CustodyRefusal {
    pub(crate) error: CustodyError,
    pub(crate) original: ReportedWork,
}
struct Joined<T> {
    join: Option<JoinHandle<T>>,
    outcome: Option<T>,
    error: Option<ManuallyDrop<JoinError>>,
    facts: Option<JoinFacts>,
    observer_resolved: bool,
    handoff_failed: bool,
    join_failed: bool,
    handoff_failure: Option<ManuallyDrop<Box<dyn std::any::Any + Send>>>,
}
struct OriginalRole<T> {
    custody: ExitCustody,
    report: ReportSlot,
    observer: ConsumerId,
    panic: PanicSink,
    joined: Mutex<Joined<T>>,
    // Losing an owner retains the original Join and raw failures as UNKNOWN.
    retained: Mutex<Option<Arc<Self>>>,
}
impl<T> OriginalRole<T> {
    fn new(
        original: ReportedWork,
        observer: ConsumerId,
        panic: PanicSink,
    ) -> Result<(Arc<Self>, ExitObligation), CustodyRefusal> {
        let kind = original.report().consumer_kind(observer);
        if !matches!(kind, Ok(ConsumerKind::Observer)) {
            return Err(CustodyRefusal {
                error: kind
                    .err()
                    .map_or(CustodyError::WrongObserver, CustodyError::Report),
                original,
            });
        }
        let report = original.report().clone();
        let (custody, exit) = ExitCustody::from_admitted(original);
        let role = Arc::new(Self {
            custody,
            report,
            observer,
            panic,
            joined: Mutex::new(Joined {
                join: None,
                outcome: None,
                error: None,
                facts: None,
                observer_resolved: false,
                handoff_failed: false,
                join_failed: false,
                handoff_failure: None,
            }),
            retained: Mutex::new(None),
        });
        *role.retained.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&role));
        Ok((role, exit))
    }
    fn record_handoff_failure(&self, cause: Box<dyn std::any::Any + Send>) {
        let mut state = self.joined.lock().unwrap_or_else(|e| e.into_inner());
        assert!(!state.handoff_failed, "one original handoff failure");
        state.handoff_failed = true;
        state.handoff_failure = Some(ManuallyDrop::new(cause));
    }
    fn take_handoff_failure(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.joined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .handoff_failure
            .take()
            .map(ManuallyDrop::into_inner)
    }
    fn take_join_failure(&self) -> Option<JoinError> {
        self.joined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .error
            .take()
            .map(ManuallyDrop::into_inner)
    }
    fn take_work_panic(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.panic.take()
    }
    fn install_join(&self, join: JoinHandle<T>) {
        let mut state = self.joined.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            state.join.is_none() && state.facts.is_none(),
            "one original Source Join"
        );
        state.join = Some(join);
    }
    fn poll_join(self: &Arc<Self>, cx: &mut Context<'_>) -> Poll<Result<(), CustodyError>> {
        let join = {
            let mut state = self.joined.lock().unwrap_or_else(|e| e.into_inner());
            if state.handoff_failed {
                return Poll::Ready(Err(CustodyError::OriginalHandoffFailed));
            }
            if state.facts.is_some() {
                return Poll::Ready(Ok(()));
            }
            state.join.take()
        };
        let Some(join) = join else {
            return Poll::Ready(Err(CustodyError::MissingOriginalJoin));
        };
        // Poll/Waker calls run outside Source locks. The loan returns this same
        // Join on Pending or unwind, never replacing it with another task.
        let mut loan = JoinPollLoan {
            role: Arc::clone(self),
            join: Some(join),
        };
        let id = loan.join.as_ref().unwrap().id();
        let result = match Pin::new(loan.join.as_mut().unwrap()).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        drop(loan.join.take());
        let facts = JoinFacts {
            id,
            panic: result.as_ref().is_err_and(|e| e.is_panic()),
            cancelled: result.as_ref().is_err_and(|e| e.is_cancelled()),
        };
        let mut state = self.joined.lock().unwrap_or_else(|e| e.into_inner());
        match result {
            Ok(outcome) => state.outcome = Some(outcome),
            Err(error) => {
                state.join_failed = true;
                state.error = Some(ManuallyDrop::new(error));
            }
        }
        state.facts = Some(facts);
        Poll::Ready(Ok(()))
    }
    fn resolve_local_report(&self) -> Result<(), CustodyError> {
        // These local flags are authored only after original body/captures
        // teardown. actual_exit_ready waits for children and would deadlock
        // the Native physical cleanup that must release those same children.
        if self.custody.phase() != ExitCustodyPhase::Exited
            || !self.custody.body_exited()
            || !self.custody.captures_exited()
        {
            return Err(CustodyError::OriginalCapturesUnresolved);
        }
        if self
            .joined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .observer_resolved
        {
            return Ok(());
        }
        match self.report.publication_state()? {
            novarocks_control_runtime_core::completion::SlotState::Filled => {
                self.report.consume(self.observer)?
            }
            novarocks_control_runtime_core::completion::SlotState::RevokedUnused => {}
            novarocks_control_runtime_core::completion::SlotState::Reserved => {
                return Err(CustodyError::OriginalCapturesUnresolved);
            }
        }
        self.joined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .observer_resolved = true;
        Ok(())
    }
    fn retire(&self) -> Result<bool, CustodyError> {
        if self.custody.phase() == ExitCustodyPhase::Retired || self.custody.try_retire()? {
            // A raw failed Join stays with this original Source until its
            // failure consumer takes it; retirement never destroys it here.
            let failures_transferred = {
                let state = self.joined.lock().unwrap_or_else(|e| e.into_inner());
                state.error.is_none() && state.handoff_failure.is_none()
            };
            if failures_transferred && !self.panic.has_panic() {
                let retained = self
                    .retained
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                drop(retained);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
struct JoinPollLoan<T> {
    role: Arc<OriginalRole<T>>,
    join: Option<JoinHandle<T>>,
}
impl<T> Drop for JoinPollLoan<T> {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            let mut state = self.role.joined.lock().unwrap_or_else(|e| e.into_inner());
            assert!(state.join.is_none(), "original Join loan returns once");
            state.join = Some(join);
        }
    }
}

#[must_use = "consume only through its matching original Native row"]
pub(crate) struct NativeDisposalCompletion {
    original: NativeDisposalReturn,
}
impl NativeDisposalCompletion {
    pub(crate) fn original(&self) -> &NativeDisposalReturn {
        &self.original
    }
    pub(crate) fn into_original(self) -> NativeDisposalReturn {
        self.original
    }
}
/// Checked by the original Work/report/Join path; dropping this obligation
/// retains unresolved originals and cannot advance the Native row.
#[must_use = "retain the actual cleanup Join and unresolved original value"]
pub(crate) struct OriginalDisposalObligation {
    role: Arc<OriginalRole<Result<(), ExitWorkError>>>,
    original: Option<NativeDisposalReturn>,
}
fn disposal_work(
    exit: ExitObligation,
    body: NativeDisposalBody,
    panic: PanicSink,
) -> impl FnOnce() -> Result<(), ExitWorkError> {
    let work = run_blocking(exit, move || body.dispose(), panic);
    move || work.run()
}
impl OriginalDisposalObligation {
    pub(crate) fn start(
        runtime: &Handle,
        original: ReportedWork,
        observer: ConsumerId,
        request: NativeDisposalRequest,
    ) -> Result<Self, (CustodyRefusal, NativeDisposalRequest)> {
        let panic = PanicSink::new();
        let (role, exit) = match OriginalRole::new(original, observer, panic.clone()) {
            Ok(value) => value,
            Err(refused) => return Err((refused, request)),
        };
        let (body, original) = request.into_parts();
        let work = disposal_work(exit, body, panic);
        // The same obligated wrapper already exists before carrier handoff.
        // A never-started callback cannot claim its Native value was disposed.
        // Establish the owner before crossing the carrier boundary. Runtime
        // spawn panic retains this same Return and raw cause in a reachable
        // original Source object, with no fabricated Join or completion.
        let owner = Self {
            role,
            original: Some(original),
        };
        match catch_unwind(AssertUnwindSafe(|| runtime.spawn_blocking(work))) {
            Ok(join) => owner.role.install_join(join),
            Err(cause) => owner.role.record_handoff_failure(cause),
        }
        Ok(owner)
    }
    pub(crate) fn take_handoff_failure(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.role.take_handoff_failure()
    }
    /// Move originals only to the Source's credentialed failure consumer.
    /// Taking a cause never clears its recorded failure or signs completion.
    pub(crate) fn take_join_failure(&self) -> Option<JoinError> {
        self.role.take_join_failure()
    }
    pub(crate) fn take_work_panic(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.role.take_work_panic()
    }

    /// Pending relies on the original Join and ledger progress wakes. The
    /// Source must register those real wakes, not schedule a polling timer.
    pub(crate) fn poll_complete(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<NativeDisposalCompletion, CustodyError>> {
        if self.original.is_none() {
            return Poll::Ready(Err(CustodyError::CompletionAlreadyIssued));
        }
        match self.role.poll_join(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        {
            let state = self.role.joined.lock().unwrap_or_else(|e| e.into_inner());
            if state.join_failed {
                return Poll::Ready(Err(CustodyError::OriginalJoinFailed));
            }
            if !matches!(state.outcome, Some(Ok(()))) || self.role.panic.has_panic() {
                return Poll::Ready(Err(CustodyError::OriginalWorkFailed));
            }
        }
        if let Err(e) = self.role.resolve_local_report() {
            return Poll::Ready(Err(e));
        }
        match self.role.retire() {
            Ok(true) => Poll::Ready(Ok(NativeDisposalCompletion {
                original: self.original.take().unwrap(),
            })),
            Ok(false) => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
impl Drop for OriginalDisposalObligation {
    fn drop(&mut self) {
        if let Some(original) = self.original.take() {
            // Owner loss is UNKNOWN. Its original Return stays retained, never
            // final-dropped on an actor poller or treated as successful disposal.
            std::mem::forget(original);
        }
    }
}

struct TransportRegistrationState {
    original: Option<SharedApplyJoinObligation>,
    issued: bool,
}
struct TransportRegistration {
    state: Mutex<TransportRegistrationState>,
    retained: Mutex<Option<Arc<Self>>>,
}
#[derive(Clone)]
pub(crate) struct TransportJoinRegistration {
    original: Weak<TransportRegistration>,
}
impl TransportJoinRegistration {
    /// Actual Source row registration only. It neither installs a Join nor
    /// signs exit; the same spawn path below owns that one actual Join.
    pub(crate) fn install_shared(
        &self,
        original: SharedApplyJoinObligation,
    ) -> Result<(), SharedApplyJoinObligation> {
        let Some(record) = self.original.upgrade() else {
            return Err(original);
        };
        let mut state = record.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.original.is_some() || state.issued {
            return Err(original);
        }
        state.original = Some(original);
        Ok(())
    }
}
#[must_use = "consume only at the same original Shared Apply row"]
pub(crate) struct SharedApplyJoinCompletion {
    original: SharedApplyJoinObligation,
    facts: JoinFacts,
}
impl SharedApplyJoinCompletion {
    pub(crate) fn original(&self) -> &SharedApplyJoinObligation {
        &self.original
    }
    pub(crate) fn into_original(self) -> SharedApplyJoinObligation {
        self.original
    }
    pub(crate) fn join_facts(&self) -> JoinFacts {
        self.facts
    }
}
#[must_use = "retain parent custody until physical returns and all children exit"]
pub(crate) struct OriginalTransportJoinObligation {
    role: Arc<OriginalRole<()>>,
    native: Arc<TransportRegistration>,
}
impl OriginalTransportJoinObligation {
    /// The factory receives the original role's unique exit and weak row port.
    /// It constructs its credentialed future before Tokio handoff. No method
    /// accepts a caller-supplied JoinHandle or a numeric completion claim.
    pub(crate) fn spawn<F, Make>(
        runtime: &Handle,
        original: ReportedWork,
        observer: ConsumerId,
        panic: PanicSink,
        make: Make,
    ) -> Result<Self, CustodyRefusal>
    where
        F: Future<Output = ()> + Send + 'static,
        Make: FnOnce(ExitObligation, TransportJoinRegistration) -> F,
    {
        let (role, exit) = OriginalRole::new(original, observer, panic)?;
        let native = Arc::new(TransportRegistration {
            state: Mutex::new(TransportRegistrationState {
                original: None,
                issued: false,
            }),
            retained: Mutex::new(None),
        });
        *native.retained.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&native));
        let registration = TransportJoinRegistration {
            original: Arc::downgrade(&native),
        };
        let owner = Self { role, native };
        match catch_unwind(AssertUnwindSafe(|| {
            let future = make(exit, registration);
            runtime.spawn(future)
        })) {
            Ok(join) => owner.role.install_join(join),
            Err(cause) => owner.role.record_handoff_failure(cause),
        }
        Ok(owner)
    }
    pub(crate) fn take_handoff_failure(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.role.take_handoff_failure()
    }
    /// Move originals only to the Source's credentialed failure consumer.
    /// Taking a cause never clears its recorded failure or signs completion.
    pub(crate) fn take_join_failure(&self) -> Option<JoinError> {
        self.role.take_join_failure()
    }
    pub(crate) fn take_work_panic(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.role.take_work_panic()
    }

    pub(crate) fn poll_shared_completion(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SharedApplyJoinCompletion>, CustodyError>> {
        match self.role.poll_join(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        if let Err(e) = self.role.resolve_local_report() {
            return Poll::Ready(Err(e));
        }
        let mut state = self.native.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.issued {
            return Poll::Ready(Err(CustodyError::CompletionAlreadyIssued));
        }
        let original = state.original.take();
        state.issued = true;
        let facts = self
            .role
            .joined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .facts
            .unwrap();
        Poll::Ready(Ok(
            original.map(|original| SharedApplyJoinCompletion { original, facts })
        ))
    }
    /// Retire only the original unused local role after factory/spawn failed
    /// without returning a Join. This emits no Native completion and releases
    /// no staged financial/input packet. Native rows, if any, remain UNKNOWN.
    pub(crate) fn retire_unused_without_join(&self) -> Result<bool, CustodyError> {
        {
            let state = self.role.joined.lock().unwrap_or_else(|e| e.into_inner());
            if !state.handoff_failed || state.join.is_some() || state.facts.is_some() {
                return Ok(false);
            }
        }
        if self
            .native
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .original
            .is_some()
            || self.role.report.publication_state()?
                != novarocks_control_runtime_core::completion::SlotState::RevokedUnused
            || !self.role.custody.actual_exit_ready()?
        {
            return Ok(false);
        }
        self.role.resolve_local_report()?;
        if self.role.retire()? {
            let retained = self
                .native
                .retained
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            drop(retained);
            Ok(true)
        } else {
            Ok(false)
        }
    }
    /// This later boundary checks the full admitted child tree and every
    /// report consumer. Local Join completion alone cannot retire the parent.
    pub(crate) fn try_retire(&self) -> Result<bool, CustodyError> {
        let issued = self
            .native
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .issued;
        if !issued {
            return Ok(false);
        }
        if self.role.retire()? {
            let retained = self
                .native
                .retained
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            drop(retained);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Original allocations only. Inline handles belong to their canonical Source
/// record; a shared PanicSink backing is charged once, never once per alias.
pub(crate) struct CustodyLayouts {
    pub(crate) transport_record: std::alloc::Layout,
    pub(crate) transport_registration: std::alloc::Layout,
    pub(crate) disposal_record: std::alloc::Layout,
    pub(crate) exit_custody: std::alloc::Layout,
    pub(crate) panic_sink: std::alloc::Layout,
    pub(crate) disposal_carrier: std::alloc::Layout,
}
pub(crate) fn storage_layouts()
-> Result<CustodyLayouts, novarocks_type_contract::owned_resources::layout::LayoutResourceError> {
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    use std::alloc::Layout;
    fn carrier_layout<F>(_: fn(ExitObligation, NativeDisposalBody, PanicSink) -> F) -> Layout {
        Layout::new::<F>()
    }
    Ok(CustodyLayouts {
        transport_record: arc_layout(Layout::new::<OriginalRole<()>>())?,
        transport_registration: arc_layout(Layout::new::<TransportRegistration>())?,
        disposal_record: arc_layout(Layout::new::<OriginalRole<Result<(), ExitWorkError>>>())?,
        exit_custody: arc_layout(ExitCustody::backing_payload_layout())?,
        panic_sink: arc_layout(PanicSink::backing_payload_layout())?,
        disposal_carrier: carrier_layout(disposal_work),
    })
}

mod physical_return;
pub(crate) use physical_return::{PhysicalInputRetirementObligation, PhysicalReturnError};

#[cfg(test)]
mod tests;
