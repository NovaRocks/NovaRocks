// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original source operation custody, early authored failures and pure exit proof.
use crate::connector::{ConnectorError, ConnectorErrorKind};
use std::{
    any::Any,
    collections::BTreeMap,
    fmt,
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
pub(super) type StopRequest = Arc<dyn Fn() + Send + Sync>;
type PanicPayload = Box<dyn Any + Send>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorSourceExitProof {
    Exited,
    Unresolved,
}
#[derive(Debug)]
pub enum ConnectorOperationExit {
    Exited,
    WorkFailed(ConnectorError),
    /// The original owner cannot prove this operation's resource exit.
    /// This is cleanup/health, never an originating work error.
    CleanupUnresolved(ConnectorError),
}
#[derive(Debug)]
pub enum ConnectorSourceFailureEvent {
    Required(ConnectorError),
    CleanupUnresolved(ConnectorError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorSourceFailure {
    ReceiverAlreadyClaimed,
    StopCallbackPanicked,
    StopCallbackDropPanicked,
    OperationIdentityExtent,
}
impl fmt::Display for ConnectorSourceFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ReceiverAlreadyClaimed => "connector source failure receiver was already claimed",
            Self::StopCallbackPanicked => "connector source stop callback did not prove exit",
            Self::StopCallbackDropPanicked => {
                "connector source stop callback destruction did not prove exit"
            }
            Self::OperationIdentityExtent => {
                "connector source operation identity space was exhausted"
            }
        })
    }
}
impl std::error::Error for ConnectorSourceFailure {}
impl novarocks_failure_contract::FailureProjection for ConnectorSourceFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match self {
            Self::ReceiverAlreadyClaimed => (FailureClass::Invariant, 180),
            Self::StopCallbackPanicked => (FailureClass::Unexpected, 181),
            Self::StopCallbackDropPanicked => (FailureClass::Unexpected, 182),
            Self::OperationIdentityExtent => (FailureClass::Resource, 183),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::CONNECTOR, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
fn error(cause: ConnectorSourceFailure) -> ConnectorError {
    ConnectorError::from_domain(
        match cause {
            ConnectorSourceFailure::ReceiverAlreadyClaimed => ConnectorErrorKind::InvalidRequest,
            ConnectorSourceFailure::OperationIdentityExtent => {
                ConnectorErrorKind::ResourceExhausted
            }
            _ => ConnectorErrorKind::Internal,
        },
        cause,
    )
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    NotStarted,
    Started,
    Exited,
    Unresolved,
}
struct StopState {
    action: Option<ManuallyDrop<StopRequest>>,
    work_exited: bool,
    invocation: Phase,
    destruction: Phase,
    payloads: [Option<ManuallyDrop<PanicPayload>>; 2],
}
struct StopSlot {
    state: Mutex<StopState>,
}
impl StopSlot {
    fn new(action: StopRequest) -> Self {
        Self {
            state: Mutex::new(StopState {
                action: Some(ManuallyDrop::new(action)),
                work_exited: false,
                invocation: Phase::NotStarted,
                destruction: Phase::NotStarted,
                payloads: [None, None],
            }),
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, StopState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn invoke(&self) -> Option<ConnectorSourceFailure> {
        let action = {
            let mut s = self.lock();
            if s.work_exited || s.invocation != Phase::NotStarted {
                return None;
            }
            s.invocation = Phase::Started;
            Arc::clone(s.action.as_ref().expect("live original stop callback"))
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| action()));
        // The original slot retains another Arc; this clone cannot run its body Drop.
        drop(action);
        let mut s = self.lock();
        match result {
            Ok(()) => {
                s.invocation = Phase::Exited;
                None
            }
            Err(payload) => {
                s.payloads[0] = Some(ManuallyDrop::new(payload));
                s.invocation = Phase::Unresolved;
                Some(ConnectorSourceFailure::StopCallbackPanicked)
            }
        }
    }
    fn finish_work(&self) {
        self.lock().work_exited = true;
    }
    fn retire(&self) -> Option<ConnectorSourceFailure> {
        let action = {
            let mut s = self.lock();
            if !s.work_exited
                || matches!(s.invocation, Phase::Started | Phase::Unresolved)
                || s.destruction != Phase::NotStarted
            {
                return None;
            }
            s.destruction = Phase::Started;
            s.action.take()
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(action.map(ManuallyDrop::into_inner))
        }));
        let mut s = self.lock();
        match result {
            Ok(()) => {
                s.destruction = Phase::Exited;
                None
            }
            Err(payload) => {
                s.payloads[1] = Some(ManuallyDrop::new(payload));
                s.destruction = Phase::Unresolved;
                Some(ConnectorSourceFailure::StopCallbackDropPanicked)
            }
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceFaultCause {
    Stop(ConnectorSourceFailure),
    Authored(ConnectorError),
}
impl SourceFaultCause {
    fn error(&self) -> ConnectorError {
        match self {
            Self::Stop(cause) => error(*cause),
            Self::Authored(cause) => cause.clone(),
        }
    }
}
struct SourceFault {
    slot: Arc<StopSlot>,
    cause: SourceFaultCause,
    consumed: bool,
}
#[derive(Clone)]
pub struct ConnectorSourceOperations {
    inner: Arc<Mutex<SourceState>>,
}
#[derive(Default)]
struct SourceState {
    sealed: bool,
    sealing: bool,
    retiring: usize,
    next_id: u64,
    live: BTreeMap<u64, Arc<StopSlot>>,
    work_recorded: bool,
    first_work: Option<Arc<ConnectorError>>,
    receiver_claimed: bool,
    faults: Vec<SourceFault>,
    waiters: Vec<Waker>,
    event_waiters: Vec<Waker>,
    parent: Option<ConnectorOperationTicket>,
}
struct Notice {
    waiters: Vec<Waker>,
    parent: Option<ConnectorOperationTicket>,
}
impl Notice {
    fn deliver(self) {
        for w in self.waiters {
            w.wake();
        }
        if let Some(parent) = self.parent {
            parent.end(ConnectorOperationExit::Exited);
        }
    }
}
impl SourceState {
    fn terminal(&self) -> bool {
        self.sealed && !self.sealing && self.live.is_empty() && self.retiring == 0
    }
    fn notice(&mut self) -> Notice {
        if !self.terminal() {
            return Notice {
                waiters: vec![],
                parent: None,
            };
        }
        let mut waiters = std::mem::take(&mut self.waiters);
        waiters.append(&mut self.event_waiters);
        Notice {
            waiters,
            parent: self.parent.take(),
        }
    }
    fn parent_source(&self) -> Option<ConnectorSourceOperations> {
        self.parent.as_ref().map(|p| p.operations.clone())
    }
}
impl ConnectorSourceOperations {
    /// Issued by an application owner while its admitted work is live.
    pub fn root(_admission: &novarocks_control_runtime::obligation::AdmissionProof) -> Self {
        Self::empty()
    }
    fn empty() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SourceState::default())),
        }
    }
    #[cfg(any(test, novarocks_dev_controls))]
    pub fn for_test() -> Self {
        Self::empty()
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, SourceState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn admit(&self, stop: StopRequest) -> Result<ConnectorOperationTicket, ConnectorError> {
        let mut s = self.lock();
        if s.sealed || !s.faults.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector source is closed and admits no new operation",
            ));
        }
        let id = s.next_id;
        s.next_id = s
            .next_id
            .checked_add(1)
            .ok_or_else(|| error(ConnectorSourceFailure::OperationIdentityExtent))?;
        s.live.insert(id, Arc::new(StopSlot::new(stop)));
        Ok(ConnectorOperationTicket {
            operations: self.clone(),
            id,
            ended: false,
        })
    }
    pub fn child(&self) -> Result<Self, ConnectorError> {
        let child = Self::empty();
        let weak = Arc::downgrade(&child.inner);
        let ticket = self.admit(Arc::new(move || {
            if let Some(inner) = weak.upgrade() {
                Self { inner }.seal();
            }
        }))?;
        let notice = {
            let mut s = child.lock();
            s.parent = Some(ticket);
            s.notice()
        };
        notice.deliver();
        Ok(child)
    }
    fn record_work(&self, cause: Arc<ConnectorError>) {
        let (parent, wakes) = {
            let mut s = self.lock();
            if s.work_recorded {
                return;
            }
            s.work_recorded = true;
            s.first_work = Some(cause.clone());
            (s.parent_source(), std::mem::take(&mut s.event_waiters))
        };
        for w in wakes {
            w.wake();
        }
        if let Some(parent) = parent {
            parent.record_work(cause);
        }
    }
    fn record_fault(&self, slot: Arc<StopSlot>, cause: SourceFaultCause) {
        let (parent, wakes) = {
            let mut s = self.lock();
            if s.faults
                .iter()
                .any(|f| Arc::ptr_eq(&f.slot, &slot) && f.cause == cause)
            {
                return;
            }
            s.faults.push(SourceFault {
                slot: slot.clone(),
                cause: cause.clone(),
                consumed: false,
            });
            let mut wakes = std::mem::take(&mut s.waiters);
            wakes.append(&mut s.event_waiters);
            (s.parent_source(), wakes)
        };
        // Retain the original slot all the way to the root, including if a child
        // ledger later drops. Rich payloads never enter Task diagnostics.
        for w in wakes {
            w.wake();
        }
        if let Some(parent) = parent {
            parent.record_fault(slot, cause);
        }
    }
    pub fn claim_failure_receiver(&self) -> Result<ConnectorSourceFailureReceiver, ConnectorError> {
        let mut s = self.lock();
        if s.receiver_claimed {
            return Err(error(ConnectorSourceFailure::ReceiverAlreadyClaimed));
        }
        s.receiver_claimed = true;
        Ok(ConnectorSourceFailureReceiver {
            operations: self.clone(),
        })
    }
    pub fn seal(&self) {
        let stops = {
            let mut s = self.lock();
            if s.sealed {
                return;
            }
            s.sealed = true;
            s.sealing = true;
            s.live.values().cloned().collect::<Vec<_>>()
        };
        for slot in stops {
            if let Some(cause) = slot.invoke() {
                self.record_fault(slot.clone(), SourceFaultCause::Stop(cause));
            }
            if let Some(cause) = slot.retire() {
                self.record_fault(slot, SourceFaultCause::Stop(cause));
            }
        }
        let notice = {
            let mut s = self.lock();
            s.sealing = false;
            s.notice()
        };
        notice.deliver();
    }
    pub fn is_sealed(&self) -> bool {
        self.lock().sealed
    }
    pub fn live_operations(&self) -> usize {
        self.lock().live.len()
    }
    pub fn is_exited(&self) -> bool {
        let s = self.lock();
        s.terminal() && s.faults.is_empty()
    }
    pub fn exited(&self) -> ConnectorSourceExit {
        ConnectorSourceExit {
            operations: self.clone(),
        }
    }
    fn end(&self, id: u64, result: ConnectorOperationExit) {
        // Preserve authored roles before touching a foreign closure destructor.
        let unresolved = match result {
            ConnectorOperationExit::Exited => None,
            ConnectorOperationExit::WorkFailed(cause) => {
                self.record_work(Arc::new(cause));
                None
            }
            ConnectorOperationExit::CleanupUnresolved(cause) => Some(cause),
        };
        let slot = {
            let mut s = self.lock();
            let Some(slot) = s.live.remove(&id) else {
                return;
            };
            s.retiring += 1;
            slot
        };
        if let Some(cause) = unresolved {
            // Keep the original stop slot in fault custody. Its unknown work
            // has not earned finish_work or foreign capture destruction.
            self.record_fault(slot, SourceFaultCause::Authored(cause));
        } else {
            slot.finish_work();
            if let Some(cause) = slot.retire() {
                self.record_fault(slot, SourceFaultCause::Stop(cause));
            }
        }
        let notice = {
            let mut s = self.lock();
            s.retiring -= 1;
            s.notice()
        };
        notice.deliver();
    }
}
impl fmt::Debug for ConnectorSourceOperations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.lock();
        f.debug_struct("ConnectorSourceOperations")
            .field("sealed", &s.sealed)
            .field("sealing", &s.sealing)
            .field("live", &s.live.len())
            .field("work_recorded", &s.work_recorded)
            .field("unresolved_slots", &s.faults.len())
            .finish()
    }
}
#[must_use = "the source ticket ends only after its original operation exits"]
pub struct ConnectorOperationTicket {
    operations: ConnectorSourceOperations,
    id: u64,
    ended: bool,
}
impl ConnectorOperationTicket {
    /// Publish this admitted operation's authored failure without claiming its
    /// physical exit. Later cleanup or an unknown destructor keeps the same slot.
    pub fn report_work_failure(&self, cause: ConnectorError) {
        self.operations.record_work(Arc::new(cause));
    }

    pub fn end(mut self, result: ConnectorOperationExit) {
        self.ended = true;
        self.operations.end(self.id, result);
    }
}
impl Drop for ConnectorOperationTicket {
    fn drop(&mut self) {
        if !self.ended {
            self.operations.end(self.id, ConnectorOperationExit::Exited);
        }
    }
}
pub struct ConnectorSourceExit {
    operations: ConnectorSourceOperations,
}
impl Future for ConnectorSourceExit {
    type Output = ConnectorSourceExitProof;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut s = self.operations.lock();
        if s.terminal() {
            return Poll::Ready(if s.faults.is_empty() {
                ConnectorSourceExitProof::Exited
            } else {
                ConnectorSourceExitProof::Unresolved
            });
        }
        if !s.waiters.iter().any(|w| w.will_wake(cx.waker())) {
            s.waiters.push(cx.waker().clone());
        }
        Poll::Pending
    }
}
pub struct ConnectorSourceFailureReceiver {
    operations: ConnectorSourceOperations,
}
impl ConnectorSourceFailureReceiver {
    pub fn poll_event(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ConnectorSourceFailureEvent>> {
        let mut s = self.operations.lock();
        if let Some(cause) = s.first_work.take() {
            return Poll::Ready(Some(ConnectorSourceFailureEvent::Required(
                (*cause).clone(),
            )));
        }
        if let Some(fault) = s.faults.iter_mut().find(|f| !f.consumed) {
            fault.consumed = true;
            return Poll::Ready(Some(ConnectorSourceFailureEvent::CleanupUnresolved(
                fault.cause.error(),
            )));
        }
        if s.terminal() {
            return Poll::Ready(None);
        }
        if !s.event_waiters.iter().any(|w| w.will_wake(cx.waker())) {
            s.event_waiters.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod author_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct WakeCount(AtomicUsize);
    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn poll_event(
        events: &mut ConnectorSourceFailureReceiver,
    ) -> Poll<Option<ConnectorSourceFailureEvent>> {
        events.poll_event(&mut Context::from_waker(futures::task::noop_waker_ref()))
    }
    #[test]
    fn child_work_failure_reaches_original_root_before_sibling_exit_and_is_consumed_once() {
        let root = ConnectorSourceOperations::for_test();
        let child = root.child().unwrap();
        let mut events = root.claim_failure_receiver().unwrap();
        let duplicate = root.claim_failure_receiver().err().unwrap();
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(
            duplicate.failure_fact(),
            ConnectorSourceFailure::ReceiverAlreadyClaimed.failure_fact()
        );
        let wake = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(wake.clone());
        assert!(
            events
                .poll_event(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let failed = child.admit(Arc::new(|| {})).unwrap();
        let live = child.admit(Arc::new(|| {})).unwrap();
        let original = ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "original leaf operation failed",
        );
        failed.end(ConnectorOperationExit::WorkFailed(original.clone()));
        assert!(wake.0.load(Ordering::SeqCst) > 0);
        let Poll::Ready(Some(ConnectorSourceFailureEvent::Required(observed))) =
            poll_event(&mut events)
        else {
            panic!("the original author reports before its sibling exit");
        };
        assert_eq!(observed, original);
        assert_eq!(child.live_operations(), 1);
        assert_eq!(root.live_operations(), 1);
        assert!(!root.is_sealed());
        assert!(poll_event(&mut events).is_pending());
        root.seal();
        assert!(!root.is_exited());
        drop(live);
        assert!(root.is_exited());
        assert_eq!(
            futures::executor::block_on(root.exited()),
            ConnectorSourceExitProof::Exited
        );
        assert!(matches!(poll_event(&mut events), Poll::Ready(None)));
    }
    struct OpaquePayload(Arc<AtomicUsize>);
    impl Drop for OpaquePayload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn stop_ending_its_ticket_then_panicking_keeps_original_slot_and_later_work_failure_visible() {
        let root = ConnectorSourceOperations::for_test();
        let mut events = root.claim_failure_receiver().unwrap();
        let ticket = Arc::new(Mutex::new(None::<ConnectorOperationTicket>));
        let calls = Arc::new(AtomicUsize::new(0));
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let callback_ticket = ticket.clone();
        let callback_calls = calls.clone();
        let callback_payload = payload_drops.clone();
        *ticket.lock().unwrap() = Some(
            root.admit(Arc::new(move || {
                callback_calls.fetch_add(1, Ordering::SeqCst);
                callback_ticket
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .end(ConnectorOperationExit::Exited);
                std::panic::panic_any(OpaquePayload(callback_payload.clone()));
            }))
            .unwrap(),
        );
        let later = root.admit(Arc::new(|| {})).unwrap();
        root.seal();
        assert_eq!(root.live_operations(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
        let Poll::Ready(Some(ConnectorSourceFailureEvent::CleanupUnresolved(cleanup))) =
            poll_event(&mut events)
        else {
            panic!("stop panic is never an originating work failure");
        };
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(
            cleanup.failure_fact(),
            ConnectorSourceFailure::StopCallbackPanicked.failure_fact()
        );
        assert!(
            poll_event(&mut events).is_pending(),
            "an unresolved cleanup does not drop the remaining work receiver"
        );
        assert!(!root.is_exited());
        let original =
            ConnectorError::new(ConnectorErrorKind::CorruptData, "later actual work failure");
        later.end(ConnectorOperationExit::WorkFailed(original.clone()));
        let Poll::Ready(Some(ConnectorSourceFailureEvent::Required(observed))) =
            poll_event(&mut events)
        else {
            panic!("actual accepted work can still fail after unknown cleanup");
        };
        assert_eq!(observed, original);
        assert_eq!(root.live_operations(), 0);
        assert_eq!(
            futures::executor::block_on(root.exited()),
            ConnectorSourceExitProof::Unresolved
        );
        assert!(!root.is_exited());
        assert!(matches!(poll_event(&mut events), Poll::Ready(None)));
        root.seal();
        root.seal();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            root.lock().faults.len(),
            1,
            "the original admission slot is retained once"
        );
    }
    struct CallbackDrop {
        calls: Arc<AtomicUsize>,
        payload: Arc<AtomicUsize>,
    }
    impl Drop for CallbackDrop {
        fn drop(&mut self) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(OpaquePayload(self.payload.clone()));
        }
    }
    #[test]
    fn callback_owner_destruction_is_a_separate_real_exit_phase() {
        let root = ConnectorSourceOperations::for_test();
        let mut events = root.claim_failure_receiver().unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let backing = CallbackDrop {
            calls: drops.clone(),
            payload: payload_drops.clone(),
        };
        let ticket = root
            .admit(Arc::new(move || {
                let _borrow = &backing;
            }))
            .unwrap();
        ticket.end(ConnectorOperationExit::Exited);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
        assert!(
            root.admit(Arc::new(|| {})).is_err(),
            "unknown source custody refuses later admission"
        );
        let Poll::Ready(Some(ConnectorSourceFailureEvent::CleanupUnresolved(cause))) =
            poll_event(&mut events)
        else {
            panic!("closure owner Drop is cleanup, not work");
        };
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(
            cause.failure_fact(),
            ConnectorSourceFailure::StopCallbackDropPanicked.failure_fact()
        );
        root.seal();
        assert_eq!(
            futures::executor::block_on(root.exited()),
            ConnectorSourceExitProof::Unresolved
        );
        assert!(matches!(poll_event(&mut events), Poll::Ready(None)));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn child_unknown_callback_custody_is_retained_by_the_original_root_after_child_drop() {
        let root = ConnectorSourceOperations::for_test();
        let child = root.child().unwrap();
        let mut events = root.claim_failure_receiver().unwrap();
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let callback_payload = payload_drops.clone();
        let work = child
            .admit(Arc::new(move || {
                std::panic::panic_any(OpaquePayload(callback_payload.clone()));
            }))
            .unwrap();
        root.seal();
        let Poll::Ready(Some(ConnectorSourceFailureEvent::CleanupUnresolved(cleanup))) =
            poll_event(&mut events)
        else {
            panic!("a child cleanup fault reaches the original root immediately");
        };
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(
            cleanup.failure_fact(),
            ConnectorSourceFailure::StopCallbackPanicked.failure_fact()
        );
        assert_eq!(
            root.live_operations(),
            1,
            "the original child scope remains live until its real work exits"
        );
        assert!(poll_event(&mut events).is_pending());
        work.end(ConnectorOperationExit::Exited);
        assert_eq!(root.live_operations(), 0);
        let original_slot = root.lock().faults[0].slot.clone();
        assert!(
            Arc::ptr_eq(&original_slot, &child.lock().faults[0].slot),
            "propagation retains the original admission slot, not copied opaque payloads"
        );
        drop(child);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            futures::executor::block_on(root.exited()),
            ConnectorSourceExitProof::Unresolved
        );
        assert!(!root.is_exited());
        assert!(matches!(poll_event(&mut events), Poll::Ready(None)));
        assert_eq!(root.lock().faults.len(), 1);
        assert!(
            root.lock().faults[0].slot.lock().action.is_some(),
            "the unknown original callback remains protected after its child ledger drops"
        );
    }
    #[test]
    fn borrowed_work_report_preserves_its_slot_before_authored_cleanup_and_later_exit() {
        let root = ConnectorSourceOperations::for_test();
        let child = root.child().unwrap();
        let mut events = root.claim_failure_receiver().unwrap();
        let failed = child.admit(Arc::new(|| {})).unwrap();
        let healthy = child.admit(Arc::new(|| {})).unwrap();
        let original = ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "known work error before its holder retirement",
        );
        failed.report_work_failure(original.clone());
        failed.report_work_failure(original.clone());
        let Poll::Ready(Some(ConnectorSourceFailureEvent::Required(observed))) =
            poll_event(&mut events)
        else {
            panic!("original work report");
        };
        assert_eq!(observed, original);
        assert_eq!(
            child.live_operations(),
            2,
            "report does not finish work or end its parent ticket"
        );
        assert_eq!(root.live_operations(), 1);
        assert!(poll_event(&mut events).is_pending());
        let cleanup = error(ConnectorSourceFailure::StopCallbackDropPanicked);
        failed.end(ConnectorOperationExit::CleanupUnresolved(cleanup.clone()));
        let Poll::Ready(Some(ConnectorSourceFailureEvent::CleanupUnresolved(observed))) =
            poll_event(&mut events)
        else {
            panic!("independent cleanup role");
        };
        assert_eq!(observed, cleanup);
        root.seal();
        assert!(
            poll_event(&mut events).is_pending(),
            "receiver remains for its real sibling"
        );
        assert_eq!(child.live_operations(), 1);
        healthy.end(ConnectorOperationExit::Exited);
        assert_eq!(
            futures::executor::block_on(root.exited()),
            ConnectorSourceExitProof::Unresolved
        );
        assert!(matches!(poll_event(&mut events), Poll::Ready(None)));
        let slot = child.lock().faults[0].slot.clone();
        assert!(Arc::ptr_eq(&slot, &root.lock().faults[0].slot));
        assert!(!slot.lock().work_exited);
        assert!(
            slot.lock().action.is_some(),
            "authored unknown retains the original closure holder"
        );
    }
}
