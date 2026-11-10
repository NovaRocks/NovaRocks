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

use super::transport::{ReturnedQueuedInput, ReturnedQuota, SendKey};
use crate::native::apply_send_owner::{
    ApplySlot, JoinFacts, NativeDisposalBody, NativeDisposalRequest, NativeDisposalReturn,
    RegistrationFailure, SharedApplyFunding, SharedApplyJoinObligation, SharedApplyPreparation,
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyError, ExitCustodyPhase, ExitObligation},
    exit_work::{ExitWorkError, PanicSink, run_blocking},
    obligation::{ReservationObligation, StepRegistration},
    reported_work::ReportedWork,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    completion::ConsumerKind,
    identity::{ConsumerId, ReservationId, WorkId},
};
use novarocks_workload_control::joint_admission::NativeQueuedInputGrant;
use std::{
    future::Future,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
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
    OriginalAttachmentUnresolved,
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
    handoff_pending: bool,
    handoff_waiter: Option<Waker>,
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
                handoff_pending: true,
                handoff_waiter: None,
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
        let wake = {
            let mut state = self.joined.lock().unwrap_or_else(|e| e.into_inner());
            assert!(state.handoff_pending, "one original handoff failure");
            state.handoff_pending = false;
            state.handoff_failed = true;
            state.handoff_failure = Some(ManuallyDrop::new(cause));
            state.handoff_waiter.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
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
        let wake = {
            let mut state = self.joined.lock().unwrap_or_else(|e| e.into_inner());
            assert!(state.handoff_pending, "one original Source Join");
            state.handoff_pending = false;
            state.join = Some(join);
            state.handoff_waiter.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
    fn poll_join(self: &Arc<Self>, cx: &mut Context<'_>) -> Poll<Result<(), CustodyError>> {
        // Cloning and dropping an executor Waker may call its implementation.
        let mut waiter = Some(cx.waker().clone());
        let join = {
            let mut state = self.joined.lock().unwrap_or_else(|e| e.into_inner());
            if state.handoff_pending {
                if state
                    .handoff_waiter
                    .as_ref()
                    .is_none_or(|w| !w.will_wake(cx.waker()))
                {
                    std::mem::swap(&mut state.handoff_waiter, &mut waiter);
                }
                return Poll::Pending;
            }
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

enum TransportRegistrationState {
    Unattached,
    UnattachedJoined,
    Attaching,
    Attached(SharedApplyJoinObligation, ReservationId),
    Refused(RefusedAttachment),
    PhysicalRetiring,
    Issued,
}
struct RefusedAttachment {
    original: ManuallyDrop<SharedApplyAttachRefusal>,
    join_observed: bool,
}
struct OriginalSendQueue {
    partition: ReservationId,
    quota: QueueReturn,
    keys: Arc<[SendKey]>,
    returned: Box<[SendInputPosition]>,
}
enum QueueReturn {
    Lent,
    Staged(ReservationObligation),
    Taken,
}
impl OriginalSendQueue {
    fn take_quota(&mut self) -> Option<ReservationObligation> {
        if !matches!(self.quota, QueueReturn::Staged(_)) {
            return None;
        }
        let QueueReturn::Staged(queue) = std::mem::replace(&mut self.quota, QueueReturn::Taken)
        else {
            unreachable!("qualified original queue")
        };
        Some(queue)
    }
}
enum SendInputPosition {
    Unissued,
    Lent,
    Returned(ReturnedQueuedInput),
}

pub(crate) fn send_input_positions_layout(
    items: usize,
) -> Result<std::alloc::Layout, std::alloc::LayoutError> {
    std::alloc::Layout::array::<SendInputPosition>(items)
}

struct TransportRegistration {
    scope: ScopeHandle,
    parent: WorkId,
    state: Mutex<TransportRegistrationState>,
    send: Mutex<Option<OriginalSendQueue>>,
    encoding: Mutex<Option<encoding::EncodingChild>>,
    actor_result: Mutex<actor_result::ActorResultState>,
    retained: Mutex<Option<Arc<Self>>>,
}
#[derive(Clone)]
pub(crate) struct TransportJoinRegistration {
    original: Weak<TransportRegistration>,
}
pub(crate) type SharedApplyAttachRefusal = (
    RegistrationFailure,
    ApplySlot,
    Vec<NativeQueuedInputGrant>,
    StepRegistration,
);
impl TransportJoinRegistration {
    /// Install the original Source's fixed, already-priced return positions
    /// before lending its inputs. The partition is later matched to the
    /// actual returned reservation in this same ledger and Scope.
    pub(crate) fn register_send(
        &self,
        partition: &ReservationObligation,
        keys: Arc<[SendKey]>,
    ) -> bool {
        assert!(!keys.is_empty(), "one original input for every send member");
        let Some(record) = self.original.upgrade() else {
            return false;
        };
        if !partition.belongs_to_bound_work(&record.scope, record.parent) {
            return false;
        }
        let returned = std::iter::repeat_with(|| SendInputPosition::Unissued)
            .take(keys.len())
            .collect();
        let state = record
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !matches!(*state, TransportRegistrationState::Unattached) {
            return false;
        }
        let mut send = record
            .send
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if send.is_some() {
            return false;
        }
        *send = Some(OriginalSendQueue {
            partition: partition.id(),
            quota: QueueReturn::Lent,
            keys,
            returned,
        });
        true
    }

    pub(crate) fn matches_send(&self, partition: ReservationId, keys: &Arc<[SendKey]>) -> bool {
        self.original.upgrade().is_some_and(|record| {
            record
                .send
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
                .is_some_and(|send| send.partition == partition && Arc::ptr_eq(&send.keys, keys))
        })
    }

    pub(crate) fn same_send_owner(&self, other: &Self) -> bool {
        self.original.ptr_eq(&other.original)
    }

    /// The capture returns the actual partition without refunding anything.
    pub(crate) fn stage_queue(
        &self,
        queue: ReservationObligation,
    ) -> Result<(), ReservationObligation> {
        let Some(record) = self.original.upgrade() else {
            return Err(queue);
        };
        if !queue.belongs_to(&record.scope) {
            return Err(queue);
        }
        let mut send = record
            .send
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(send) = send.as_mut() else {
            return Err(queue);
        };
        if send.partition != queue.id() || !matches!(send.quota, QueueReturn::Lent) {
            return Err(queue);
        }
        send.quota = QueueReturn::Staged(queue);
        Ok(())
    }

    pub(crate) fn claim_input(&self, index: usize, key: SendKey) -> bool {
        let Some(record) = self.original.upgrade() else {
            return false;
        };
        let mut send = record
            .send
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(send) = send.as_mut() else {
            return false;
        };
        if send.keys.get(index) != Some(&key)
            || !matches!(send.returned[index], SendInputPosition::Unissued)
        {
            return false;
        }
        send.returned[index] = SendInputPosition::Lent;
        true
    }

    pub(crate) fn stage_input(
        &self,
        index: usize,
        input: ReturnedQueuedInput,
    ) -> Result<(), ReturnedQueuedInput> {
        let Some(record) = self.original.upgrade() else {
            return Err(input);
        };
        let mut send = record
            .send
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(send) = send.as_mut() else {
            return Err(input);
        };
        if send.keys.get(index) != Some(&input.key())
            || !matches!(send.returned[index], SendInputPosition::Lent)
        {
            return Err(input);
        }
        send.returned[index] = SendInputPosition::Returned(input);
        Ok(())
    }

    /// The actual send attempts attachment once. A refusal stays with the
    /// original Join owner through the pre-handoff withdrawal boundary.
    pub(crate) fn attach_send(
        &self,
        slot: ApplySlot,
        inputs: Vec<NativeQueuedInputGrant>,
        encoding: StepRegistration,
    ) -> Result<SharedApplyPreparation, RegistrationFailure> {
        let refused = match self.attach_shared(slot, inputs, encoding) {
            Ok(preparation) => return Ok(preparation),
            Err(original) => RefusedAttachment {
                original: ManuallyDrop::new(original),
                join_observed: false,
            },
        };
        let error = refused.original.0;
        if error != RegistrationFailure::BrokenOriginalBound
            && let Some(record) = self.original.upgrade()
        {
            let mut state = record.state.lock().unwrap_or_else(|e| e.into_inner());
            if matches!(*state, TransportRegistrationState::Unattached) {
                *state = TransportRegistrationState::Refused(refused);
                return Err(error);
            }
        }
        // An invalid second attachment must not drop an unretired grant,
        // guard, or slot. The executor retains this exact panic payload.
        std::panic::panic_any(refused)
    }

    /// Move the same reserved Apply row and its original inputs and guard
    /// under this job's Join owner before exposing any preparation authority.
    /// An ordinary refusal gives every original object back to the Source.
    pub(crate) fn attach_shared(
        &self,
        slot: ApplySlot,
        inputs: Vec<NativeQueuedInputGrant>,
        encoding: StepRegistration,
    ) -> Result<SharedApplyPreparation, SharedApplyAttachRefusal> {
        let Some(record) = self.original.upgrade() else {
            return Err((RegistrationFailure::Closed, slot, inputs, encoding));
        };
        if !encoding.is_direct_child_of(&record.scope, record.parent) {
            return Err((
                RegistrationFailure::BrokenOriginalBound,
                slot,
                inputs,
                encoding,
            ));
        }
        {
            let mut state = record.state.lock().unwrap_or_else(|e| e.into_inner());
            if !matches!(*state, TransportRegistrationState::Unattached) {
                return Err((
                    RegistrationFailure::BrokenOriginalBound,
                    slot,
                    inputs,
                    encoding,
                ));
            }
            *state = TransportRegistrationState::Attaching;
        }
        // Native may wake its own owner here. An unwind leaves the original
        // attachment unresolved, so an observed Join cannot clear the row.
        let encoding_id = encoding.reservation_id();
        let attached = slot.prepare_shared(inputs, encoding);
        let mut state = record.state.lock().unwrap_or_else(|e| e.into_inner());
        match attached {
            Ok((preparation, join)) => {
                *state = TransportRegistrationState::Attached(join, encoding_id);
                Ok(preparation)
            }
            Err(refusal) => {
                *state = TransportRegistrationState::Unattached;
                Err(refusal)
            }
        }
    }
}
/// Follows the exact financial partitions and queued grants to their
/// actual final return. Dropping it leaves the parent retirement closed.
#[must_use]
pub(crate) struct TransportPhysicalRetirement(Weak<TransportRegistration>);
impl TransportPhysicalRetirement {
    fn finish(self) {
        let original = self
            .0
            .upgrade()
            .expect("Source retains its original Join record");
        let mut state = original
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert!(matches!(
            *state,
            TransportRegistrationState::PhysicalRetiring
        ));
        *state = TransportRegistrationState::Issued;
    }
}
#[must_use = "consume only at the same original Shared Apply row"]
pub(crate) struct SharedApplyJoinCompletion {
    original: SharedApplyJoinObligation,
    physical_retirement: TransportPhysicalRetirement,
    facts: JoinFacts,
}
impl SharedApplyJoinCompletion {
    pub(crate) fn original(&self) -> &SharedApplyJoinObligation {
        &self.original
    }
    pub(crate) fn into_original(self) -> (SharedApplyJoinObligation, TransportPhysicalRetirement) {
        (self.original, self.physical_retirement)
    }
    pub(crate) fn join_facts(&self) -> JoinFacts {
        self.facts
    }
    pub(crate) fn matches_funding(&self, funding: &SharedApplyFunding) -> bool {
        let Some(record) = self.physical_retirement.0.upgrade() else {
            return false;
        };
        let (queue, encoding) = match funding {
            SharedApplyFunding::Encoded(original) => (&original.queue, Some(&original.encoding)),
            SharedApplyFunding::UnusedQueue(queue) => (queue, None),
        };
        if !queue.belongs_to(&record.scope)
            || encoding.is_some_and(|encoding| !encoding.belongs_to(&record.scope))
        {
            return false;
        }
        {
            let send = record.send.lock().unwrap_or_else(|e| e.into_inner());
            let Some(send) = send.as_ref() else {
                return false;
            };
            if send.partition != queue.id() || !matches!(send.quota, QueueReturn::Taken) {
                return false;
            }
        }
        let original = record.encoding.lock().unwrap_or_else(|e| e.into_inner());
        match (&*original, encoding) {
            (Some(child), Some(partition)) => child.taken_partition == Some(partition.id()),
            (None, None) => true,
            _ => false,
        }
    }
}
#[must_use = "retain parent custody until physical returns and all children exit"]
pub(crate) struct OriginalTransportJoinObligation {
    role: Arc<OriginalRole<()>>,
    native: Arc<TransportRegistration>,
}
/// The only authority to install the original transport Join. It stays with
/// the prepared job until the executor accepts that job for real.
#[must_use]
pub(crate) struct TransportJoinHandoff {
    role: Arc<OriginalRole<()>>,
    registration: TransportJoinRegistration,
}
impl TransportJoinHandoff {
    pub(crate) fn owns_exit(&self, exit: &ExitObligation) -> bool {
        self.role.custody.owns_exit(exit)
    }

    pub(crate) fn spawn<F>(
        self,
        runtime: &Handle,
        exit: ExitObligation,
        make: impl FnOnce(ExitObligation, TransportJoinRegistration) -> F,
    ) -> Result<(), CustodyError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        assert!(self.owns_exit(&exit), "same prepared transport exit");
        let Self { role, registration } = self;
        match catch_unwind(AssertUnwindSafe(|| runtime.spawn(make(exit, registration)))) {
            Ok(join) => {
                role.install_join(join);
                Ok(())
            }
            Err(cause) => {
                role.record_handoff_failure(cause);
                Err(CustodyError::OriginalHandoffFailed)
            }
        }
    }
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
        let (owner, exit, handoff) = Self::prepare(original, observer, panic)?;
        let _ = handoff.spawn(runtime, exit, make);
        Ok(owner)
    }

    pub(crate) fn prepare(
        original: ReportedWork,
        observer: ConsumerId,
        panic: PanicSink,
    ) -> Result<(Self, ExitObligation, TransportJoinHandoff), CustodyRefusal> {
        let scope = original.work().scope().clone();
        let parent = original.work().id();
        let (role, exit) = OriginalRole::new(original, observer, panic)?;
        let native = Arc::new(TransportRegistration {
            scope,
            parent,
            state: Mutex::new(TransportRegistrationState::Unattached),
            send: Mutex::new(None),
            encoding: Mutex::new(None),
            actor_result: Mutex::new(actor_result::ActorResultState::Unbound),
            retained: Mutex::new(None),
        });
        *native.retained.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&native));
        let handoff = TransportJoinHandoff {
            role: role.clone(),
            registration: TransportJoinRegistration {
                original: Arc::downgrade(&native),
            },
        };
        Ok((Self { role, native }, exit, handoff))
    }

    pub(crate) fn registration(&self) -> TransportJoinRegistration {
        TransportJoinRegistration {
            original: Arc::downgrade(&self.native),
        }
    }

    /// The encoding body is inline in the original transport Join. Move its
    /// one typed return only after that Join and the parent report were read.
    pub(crate) fn take_encoding_return(
        &mut self,
    ) -> Result<Option<ReservationObligation>, CustodyError> {
        if !matches!(
            *self.native.state.lock().unwrap_or_else(|e| e.into_inner()),
            TransportRegistrationState::PhysicalRetiring
        ) {
            return Err(CustodyError::OriginalAttachmentUnresolved);
        }
        let Some(mut child) = self
            .native
            .encoding
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        else {
            return Ok(None);
        };
        let result = child.take_partition().map(Some);
        *self
            .native
            .encoding
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(child);
        result
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
        match &mut *state {
            TransportRegistrationState::Refused(refused) if !refused.join_observed => {
                refused.join_observed = true;
                return Poll::Ready(Ok(None));
            }
            TransportRegistrationState::Refused(_)
            | TransportRegistrationState::UnattachedJoined
            | TransportRegistrationState::PhysicalRetiring
            | TransportRegistrationState::Issued => {
                return Poll::Ready(Err(CustodyError::CompletionAlreadyIssued));
            }
            TransportRegistrationState::Attaching => {
                return Poll::Ready(Err(CustodyError::OriginalAttachmentUnresolved));
            }
            _ => {}
        }
        let next = if matches!(*state, TransportRegistrationState::Attached(..)) {
            TransportRegistrationState::PhysicalRetiring
        } else if self
            .native
            .send
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_some()
        {
            TransportRegistrationState::UnattachedJoined
        } else {
            TransportRegistrationState::Issued
        };
        let original = match std::mem::replace(&mut *state, next) {
            TransportRegistrationState::Unattached => None,
            TransportRegistrationState::Attached(original, _) => Some(original),
            _ => unreachable!("qualified original attachment"),
        };
        let facts = self
            .role
            .joined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .facts
            .unwrap();
        Poll::Ready(Ok(original.map(|original| SharedApplyJoinCompletion {
            original,
            physical_retirement: TransportPhysicalRetirement(Arc::downgrade(&self.native)),
            facts,
        })))
    }
    /// Transfer the original queue and any encoding partition to the same
    /// attached Native row, after its actual Join and capture exit.
    pub(crate) fn take_apply_funding(
        &mut self,
        completion: &SharedApplyJoinCompletion,
    ) -> Result<SharedApplyFunding, CustodyError> {
        if !completion
            .physical_retirement
            .0
            .ptr_eq(&Arc::downgrade(&self.native))
        {
            return Err(CustodyError::WrongObserver);
        }
        if !self
            .native
            .send
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|send| matches!(send.quota, QueueReturn::Staged(_)))
        {
            return Err(CustodyError::OriginalCapturesUnresolved);
        }
        let encoding = self.take_encoding_return()?;
        let mut send = self.native.send.lock().unwrap_or_else(|e| e.into_inner());
        let queue = send
            .as_mut()
            .and_then(OriginalSendQueue::take_quota)
            .expect("qualified original queue partition");
        Ok(match encoding {
            Some(encoding) => SharedApplyFunding::Encoded(ReturnedQuota { queue, encoding }),
            None => SharedApplyFunding::UnusedQueue(queue),
        })
    }

    /// The original send joined without an Apply row. Move its actual queue
    /// funding and grants to the same financial-first retirement path used
    /// by attached sends.
    pub(crate) fn take_unattached_retirement(
        &self,
    ) -> Result<
        Option<(
            Option<RegistrationFailure>,
            PhysicalInputRetirementObligation,
        )>,
        CustodyError,
    > {
        let mut state = self.native.state.lock().unwrap_or_else(|e| e.into_inner());
        let refused = match &*state {
            TransportRegistrationState::UnattachedJoined => false,
            TransportRegistrationState::Refused(refusal) if refusal.join_observed => true,
            _ => return Ok(None),
        };
        let mut queue_owner = self.native.send.lock().unwrap_or_else(|e| e.into_inner());
        let send = queue_owner
            .as_mut()
            .expect("joined send owns its original queue");
        if !refused
            && send
                .returned
                .iter()
                .any(|slot| !matches!(slot, SendInputPosition::Returned(_)))
        {
            return Err(CustodyError::OriginalCapturesUnresolved);
        }
        let queue = send
            .take_quota()
            .ok_or(CustodyError::OriginalCapturesUnresolved)?;
        let previous = std::mem::replace(&mut *state, TransportRegistrationState::PhysicalRetiring);
        let returned = std::mem::take(&mut send.returned);
        drop(queue_owner);
        drop(state);
        let retirement = TransportPhysicalRetirement(Arc::downgrade(&self.native));
        let (error, obligation) = match previous {
            TransportRegistrationState::Refused(refusal) => {
                let (error, slot, inputs, encoding) = ManuallyDrop::into_inner(refusal.original);
                let obligation =
                    PhysicalInputRetirementObligation::unused_queue(queue, inputs, retirement);
                drop(slot);
                drop(encoding);
                (Some(error), obligation)
            }
            TransportRegistrationState::UnattachedJoined => {
                let inputs = returned
                    .into_vec()
                    .into_iter()
                    .map(|slot| match slot {
                        SendInputPosition::Returned(input) => input.into_original(),
                        _ => unreachable!("every original loan returned"),
                    })
                    .collect();
                (
                    None,
                    PhysicalInputRetirementObligation::unused_queue(queue, inputs, retirement),
                )
            }
            _ => unreachable!("qualified original Join"),
        };
        Ok(Some((error, obligation)))
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
        if !matches!(
            *self.native.state.lock().unwrap_or_else(|e| e.into_inner()),
            TransportRegistrationState::Unattached
        ) || self
            .native
            .send
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_some()
            || self.role.report.publication_state()?
                != novarocks_control_runtime_core::completion::SlotState::RevokedUnused
            || !self.role.custody.actual_exit_ready()?
        {
            return Ok(false);
        }
        self.role.resolve_local_report()?;
        if self.actor_result_held() {
            let mut state = self.native.state.lock().unwrap_or_else(|e| e.into_inner());
            if matches!(*state, TransportRegistrationState::Unattached) {
                *state = TransportRegistrationState::Issued;
            }
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
    /// This later boundary checks the full admitted child tree and every
    /// report consumer. Local Join completion alone cannot retire the parent.
    pub(crate) fn try_retire(&self) -> Result<bool, CustodyError> {
        if !matches!(
            *self.native.state.lock().unwrap_or_else(|e| e.into_inner()),
            TransportRegistrationState::Issued
        ) {
            return Ok(false);
        }
        if self.actor_result_held() {
            return Ok(false);
        }
        let encoding = self
            .native
            .encoding
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|child| child.custody.clone());
        if let Some(encoding) = encoding
            && !encoding::EncodingChild::retired(&encoding)?
        {
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
mod encoding;
pub(crate) use encoding::{EncodingBinding, EncodingJob, owned_layouts as encoding_owned_layouts};
mod actor_result;

#[cfg(test)]
pub(crate) mod tests;
