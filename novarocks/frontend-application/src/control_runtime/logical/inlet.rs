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

//! One original, prepaid standing position. The outer logical Source retains
//! its same ReportedWork; this inlet neither admits Work nor certifies its exit.

use crate::control_runtime::actor::{FrontendActor, FrontendInput};
use futures::task::AtomicWaker;
use novarocks_control_runtime::actor_turn::{ActorTurnError, StandingInputObligation};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_logical_execution_protocol::LogicalSourceId;
use novarocks_query_application::api::{
    LogicalFactSource, LogicalFuture, LogicalHostFact, LogicalSourceClosed, LogicalSourceKind,
    LogicalSourceRefusal,
};
use std::{
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InletFailureCause {
    WrongKind,
    Kernel(ActorTurnError),
}
/// The original rejected passive input goes to the Source's already-paid
/// failure custodian. It is never queued or replayed by this inlet.
pub(super) struct InletFailure {
    pub(super) source: LogicalSourceId,
    pub(super) expected: LogicalSourceKind,
    pub(super) cause: InletFailureCause,
    pub(super) original: FrontendInput,
}
pub(super) type InletFailureCallback = Arc<dyn Fn(InletFailure) + Send + Sync>;

#[derive(Default)]
struct IngressCloseState {
    closed: AtomicBool,
    waiter: AtomicWaker,
}
/// Passive observation of the original ingress fence and pending send
/// settlement. This retains no Shared, standing position, Work or owner, and
/// proves neither producer exit nor physical role/resource exit.
/// Clones share one original observer registration for the Registry waiter.
#[derive(Clone)]
pub(crate) struct LogicalIngressCloseObserver {
    state: Arc<IngressCloseState>,
}
impl LogicalIngressCloseObserver {
    pub(crate) fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.state.closed.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        self.state.waiter.register(cx.waker());
        if self.state.closed.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

struct State {
    position: Option<StandingInputObligation<FrontendActor>>,
    submitted: Option<InputId>,
    submission_unsettled: bool,
    closing: bool,
    closed: bool,
    failure_reported: bool,
    frames: usize,
    host_waiting_loan: bool,
    host_waiting_frames: bool,
    sender_waiting_loan: Option<Waker>,
}
struct Shared {
    source: LogicalSourceId,
    kind: LogicalSourceKind,
    state: Mutex<State>,
    retired_or_closed: Notify,
    host: Waker,
    fail_owner: InletFailureCallback,
    admission_closed: Option<Arc<AtomicBool>>,
    ingress_close: Arc<IngressCloseState>,
}
fn wake_after_return(wake: impl FnOnce()) {
    if std::thread::panicking() {
        // The original loan/frame is already stable. A secondary foreign
        // waker panic must not replace the work's primary unwind.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(wake));
    } else {
        wake();
    }
}
impl Shared {
    fn publish_ingress_closed(&self) {
        if self.ingress_close.closed.load(Ordering::Acquire) {
            return;
        }
        let ready = {
            let state = self.lock();
            state.closed
                || (state.closing || self.admission_closed())
                    && state.submitted.is_none()
                    && !state.submission_unsettled
                    && state.frames == 0
                    && state.position.is_some()
        };
        if ready && !self.ingress_close.closed.swap(true, Ordering::AcqRel) {
            // The original custody metadata is stable before any foreign wake.
            // Later rejected frames and read-only host loans cannot reopen it.
            self.ingress_close.waiter.wake();
        }
    }
    fn admission_closed(&self) -> bool {
        self.admission_closed
            .as_ref()
            .is_some_and(|gate| gate.load(Ordering::Acquire))
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
    fn close(&self) {
        let wake_sender = {
            let mut state = self.lock();
            if state.closing {
                return;
            }
            state.closing = true;
            state.sender_waiting_loan.take()
        };
        self.publish_ingress_closed();
        // A close is a real state change. Neither a Busy check nor a loan
        // return sends this notification.
        self.retired_or_closed.notify_waiters();
        if let Some(waker) = wake_sender {
            waker.wake();
        }
        self.host.wake_by_ref();
    }
    fn fail(&self, cause: InletFailureCause, original: FrontendInput) {
        let report = {
            let mut state = self.lock();
            if state.failure_reported {
                false
            } else {
                state.failure_reported = true;
                true
            }
        };
        self.close();
        if report {
            (self.fail_owner)(InletFailure {
                source: self.source,
                expected: self.kind,
                cause,
                original,
            });
        }
    }
    fn host_loan(self: &Arc<Self>) -> Option<PositionLoanObligation> {
        let mut state = self.lock();
        if state.closed {
            return None;
        }
        match state.position.take() {
            Some(position) => Some(PositionLoanObligation {
                shared: Arc::clone(self),
                position: Some(position),
            }),
            None => {
                state.host_waiting_loan = true;
                None
            }
        }
    }
    fn sender_loan(self: &Arc<Self>) -> SendPosition {
        let mut state = self.lock();
        if state.closing || self.admission_closed() {
            SendPosition::Closed
        } else if state.submitted.is_some() || state.submission_unsettled {
            SendPosition::Retirement
        } else if let Some(position) = state.position.take() {
            // Until the SAME position returns, a caller unwind may leave an
            // actual kernel fill without its local submitted-id assignment.
            state.submission_unsettled = true;
            SendPosition::Loan(PositionLoanObligation {
                shared: Arc::clone(self),
                position: Some(position),
            })
        } else {
            SendPosition::Availability
        }
    }
    fn poll_sender_availability(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.lock();
        if state.closing || self.admission_closed() || state.position.is_some() {
            Poll::Ready(())
        } else {
            // &mut LogicalInletSender admits at most one live send waiter.
            // This is one requested availability wake, not another input slot.
            if state
                .sender_waiting_loan
                .as_ref()
                .is_none_or(|old| !old.will_wake(cx.waker()))
            {
                state.sender_waiting_loan = Some(cx.waker().clone());
            }
            Poll::Pending
        }
    }
}

enum SendPosition {
    Loan(PositionLoanObligation),
    Retirement,
    Availability,
    Closed,
}

/// Checked on normal return and unwinding. Drop returns the SAME position
/// before invoking a foreign waker and never reports producer/capture exit.
#[must_use = "return the original standing-position loan"]
struct PositionLoanObligation {
    shared: Arc<Shared>,
    position: Option<StandingInputObligation<FrontendActor>>,
}
impl PositionLoanObligation {
    fn position(&self) -> &StandingInputObligation<FrontendActor> {
        self.position.as_ref().expect("one original position loan")
    }
    fn position_mut(&mut self) -> &mut StandingInputObligation<FrontendActor> {
        self.position.as_mut().expect("one original position loan")
    }
}
impl Drop for PositionLoanObligation {
    fn drop(&mut self) {
        let (wake_host, wake_sender) = {
            let mut state = self.shared.lock();
            assert!(
                state.position.is_none(),
                "one standing-position loan exists"
            );
            state.position = self.position.take();
            assert!(state.position.is_some(), "the original loan returns intact");
            (
                std::mem::take(&mut state.host_waiting_loan),
                state.sender_waiting_loan.take(),
            )
        };
        // No producer Notify here: a Busy retirement loan has changed no
        // input capacity and must not spin the sender or installed host.
        wake_after_return(|| {
            self.shared.publish_ingress_closed();
            if wake_host {
                self.shared.host.wake_by_ref();
            }
            if let Some(waker) = wake_sender {
                waker.wake();
            }
        });
    }
}

#[must_use = "the active sender frame remains distinct from original producer Work exit"]
struct SenderFrameObligation {
    shared: Arc<Shared>,
}
impl SenderFrameObligation {
    fn enter(shared: Arc<Shared>) -> Self {
        {
            let mut state = shared.lock();
            state.frames = state.frames.checked_add(1).expect("bounded sender frames");
        }
        Self { shared }
    }
}
impl Drop for SenderFrameObligation {
    fn drop(&mut self) {
        let wake = {
            let mut state = self.shared.lock();
            state.frames = state
                .frames
                .checked_sub(1)
                .expect("one actual sender frame");
            state.sender_waiting_loan = None;
            state.frames == 0 && std::mem::take(&mut state.host_waiting_frames)
        };
        wake_after_return(|| {
            self.shared.publish_ingress_closed();
            if wake {
                self.shared.host.wake_by_ref();
            }
        });
    }
}

/// Host-owned, non-cloneable original role. It has no destructor that invents
/// exit. The kernel retains role credit until progress closes this position.
/// The outer Source must retain the original bound ReportedWork throughout.
#[must_use = "close the original inlet only after actual producer and input retirement"]
pub(super) struct LogicalInletObligation {
    shared: Arc<Shared>,
    sender_taken: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InletProgress {
    Idle,
    Changed,
    Closed,
}
impl LogicalInletObligation {
    pub(super) fn new(
        position: StandingInputObligation<FrontendActor>,
        source: LogicalSourceId,
        kind: LogicalSourceKind,
        host: Waker,
        fail_owner: InletFailureCallback,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                source,
                kind,
                state: Mutex::new(State {
                    position: Some(position),
                    submitted: None,
                    submission_unsettled: false,
                    closing: false,
                    closed: false,
                    failure_reported: false,
                    frames: 0,
                    host_waiting_loan: false,
                    host_waiting_frames: false,
                    sender_waiting_loan: None,
                }),
                retired_or_closed: Notify::new(),
                host,
                fail_owner,
                admission_closed: None,
                ingress_close: Arc::new(IngressCloseState::default()),
            }),
            sender_taken: false,
        }
    }
    /// Installation chooses the SAME execution-wide admission gate before
    /// publishing a weak sender. This flag never certifies producer exit.
    pub(super) fn with_admission_gate(mut self, gate: Arc<AtomicBool>) -> Self {
        Arc::get_mut(&mut self.shared)
            .expect("unpublished original inlet")
            .admission_closed = Some(gate);
        self
    }
    pub(super) fn take_sender(&mut self) -> Result<LogicalInletSender, LogicalSourceRefusal> {
        if self.shared.lock().closing || self.shared.admission_closed() {
            return Err(LogicalSourceRefusal::Closed);
        }
        if self.sender_taken {
            return Err(LogicalSourceRefusal::AlreadyTaken);
        }
        self.sender_taken = true;
        Ok(LogicalInletSender {
            shared: Arc::downgrade(&self.shared),
            source: self.shared.source,
        })
    }
    /// Close admission, not Work/capture custody. Weak handles need not drop.
    pub(super) fn close(&self) {
        self.shared.close();
    }
    /// The execution-wide gate already rejects new senders. A normal Source
    /// drains each admitted sender frame before closing its original inlet,
    /// so gate closure cannot overturn that frame's actual retirement reply.
    /// Failure fencing instead uses close() immediately and preserves Closed.
    pub(super) fn close_after_sender_frames(&self) {
        assert!(
            self.shared.admission_closed(),
            "normal inlet close requires its original admission fence"
        );
        {
            let mut state = self.shared.lock();
            if state.frames != 0 {
                state.host_waiting_frames = true;
                return;
            }
        }
        self.shared.close();
    }
    pub(super) fn ingress_close_observer(&self) -> LogicalIngressCloseObserver {
        self.shared.publish_ingress_closed();
        LogicalIngressCloseObserver {
            state: Arc::clone(&self.shared.ingress_close),
        }
    }
    pub(super) fn is_closed(&self) -> bool {
        self.shared.lock().closed
    }
    /// One paid host visit. producer_exited is the outer Source's actual
    /// original body/capture exit observation, not a gate/terminal/empty flag.
    /// failed means the original actor is kernel-fenced, not logical Failed.
    /// All kernel operations and callbacks occur outside the inlet mutex.
    pub(super) fn progress(
        &mut self,
        producer_exited: bool,
        failed: bool,
    ) -> Result<InletProgress, ActorTurnError> {
        if self.is_closed() {
            return Ok(InletProgress::Closed);
        }
        if failed {
            self.close();
        }
        let Some(mut loan) = self.shared.host_loan() else {
            return Ok(InletProgress::Idle);
        };
        let actual = loan.position().in_flight()?;
        let retired = {
            let mut state = self.shared.lock();
            let unsettled = state.submission_unsettled;
            let retired = match (state.submitted, actual) {
                (Some(held), Some(real)) if held != real => {
                    return Err(ActorTurnError::InputAuthorMismatch);
                }
                // Recover the original kernel fact even if a fill/retirement
                // waker unwound before the local metadata update. No identity
                // or exit is reconstructed from absence of a caller future.
                (None, Some(real)) => {
                    state.submitted = Some(real);
                    false
                }
                (Some(_), None) => {
                    state.submitted = None;
                    true
                }
                _ => false,
            };
            // This visit holds the returned SAME position and has read its
            // actual kernel occurrence. Host scans cannot clear a live loan.
            state.submission_unsettled = false;
            retired || unsettled && actual.is_none()
        };
        let mut changed = retired;
        let mut notify_retired = retired;
        if actual.is_some() {
            if loan.position().retire_ready_after_exit()? {
                self.shared.lock().submitted = None;
                changed = true;
                notify_retired = true;
            } else {
                if failed {
                    match loan.position().take_failed_role_payload() {
                        Ok(Some(original)) => {
                            // FrontendInput::Logical contains passive facts only.
                            // Disposal remains outside the inlet mutex.
                            original.dispose()?;
                            changed = true;
                        }
                        // A prior role at the same original Source FIFO must
                        // retire first; this role cannot jump over it.
                        Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                        Err(error) => return Err(error),
                    }
                }
                return Ok(if changed {
                    InletProgress::Changed
                } else {
                    InletProgress::Idle
                });
            }
        }
        let close_now = {
            let mut state = self.shared.lock();
            if state.closing && state.frames != 0 {
                state.host_waiting_frames = true;
            }
            state.closing && producer_exited && state.frames == 0 && state.submitted.is_none()
        };
        if close_now {
            loan.position_mut().close_after_exit()?;
            self.shared.lock().closed = true;
            changed = true;
        }
        drop(loan);
        if notify_retired {
            self.shared.retired_or_closed.notify_waiters();
        }
        Ok(if self.is_closed() {
            InletProgress::Closed
        } else if changed {
            InletProgress::Changed
        } else {
            InletProgress::Idle
        })
    }
}

/// One weak, non-cloneable endpoint. Dropping it is never producer exit proof.
pub(crate) struct LogicalInletSender {
    shared: Weak<Shared>,
    source: LogicalSourceId,
}
impl LogicalFactSource for LogicalInletSender {
    fn id(&self) -> LogicalSourceId {
        self.source
    }
    fn send(
        &mut self,
        fact: LogicalHostFact,
    ) -> LogicalFuture<'_, Result<(), LogicalSourceClosed>> {
        Box::pin(async move {
            let shared = self.shared.upgrade().ok_or(LogicalSourceClosed)?;
            let _frame = SenderFrameObligation::enter(Arc::clone(&shared));
            if shared.lock().closing || shared.admission_closed() {
                return Err(LogicalSourceClosed);
            }
            let allowed = fact.belongs_to(shared.kind);
            let input = FrontendInput::Logical(Box::new(fact.into_input(shared.source)));
            if !allowed {
                shared.fail(InletFailureCause::WrongKind, input);
                return Err(LogicalSourceClosed);
            }
            // This future owns exactly this one passive input. It never
            // buffers another fact or retries a Busy kernel fill.
            let loan = loop {
                let retired = shared.retired_or_closed.notified();
                tokio::pin!(retired);
                retired.as_mut().enable(); // register before checking the original position
                match shared.sender_loan() {
                    SendPosition::Loan(loan) => break loan,
                    SendPosition::Closed => return Err(LogicalSourceClosed),
                    SendPosition::Retirement => retired.await,
                    SendPosition::Availability => {
                        std::future::poll_fn(|cx| shared.poll_sender_availability(cx)).await;
                    }
                }
            };
            let submitted = match loan.position().fill(input) {
                Ok(input) => input,
                Err((error, original)) => {
                    shared.lock().submission_unsettled = false;
                    drop(loan);
                    if matches!(error, ActorTurnError::Closed | ActorTurnError::ActorFailed) {
                        shared.close();
                    } else {
                        shared.fail(InletFailureCause::Kernel(error), original);
                    }
                    return Err(LogicalSourceClosed);
                }
            };
            {
                let mut state = shared.lock();
                assert!(
                    state.submitted.replace(submitted).is_none(),
                    "the sole original position accepts one occurrence"
                );
                state.submission_unsettled = false;
            }
            drop(loan);
            shared.host.wake_by_ref();
            loop {
                let retired = shared.retired_or_closed.notified();
                tokio::pin!(retired);
                retired.as_mut().enable();
                let (same_occurrence, closing) = {
                    let state = shared.lock();
                    (state.submitted == Some(submitted), state.closing)
                };
                if !same_occurrence {
                    return if closing {
                        Err(LogicalSourceClosed)
                    } else {
                        Ok(())
                    };
                }
                // Closing alone cannot acknowledge a submitted occurrence.
                // Even its cancelled waiter leaves this identity in the role.
                retired.await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_runtime::actor::{FrontendEffect, TestActor};
    use novarocks_control_runtime::{
        actor::{ActorConfig, ActorStep},
        actor_turn::{ActorRegistration, ActorRuntime},
        obligation::{ReservationObligation, WorkObligation},
        transaction::{RuntimeLedger, ScopeHandle},
    };
    use novarocks_control_runtime_core::{
        capacity::{ReservationResources, ResourceVector},
        effect::{EffectDescriptor, EffectKind},
        identity::{ActionId, ActorId, Generation, Tick},
        mailbox::{InputClass, InputEnvelope},
        owned_storage::OwnedSlotNeed,
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_logical_execution_protocol::{LogicalRequestToken, input::LogicalInput};
    use std::{
        future::Future,
        num::NonZeroU64,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::Wake,
    };

    #[derive(Default)]
    struct Probe {
        calls: AtomicUsize,
        shared: Mutex<Weak<Shared>>,
    }
    impl Wake for Probe {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(shared) = self.shared.lock().unwrap().upgrade() {
                assert!(
                    shared.state.try_lock().is_ok(),
                    "foreign wake runs outside inlet mutex"
                );
            }
        }
    }
    struct Recorder {
        seen: Arc<Mutex<Vec<(LogicalSourceId, LogicalRequestToken)>>>,
        hold_effect: bool,
    }
    impl TestActor for Recorder {
        fn handle(
            &mut self,
            _: Tick,
            _: InputEnvelope,
            input: FrontendInput,
            step: &mut ActorStep<'_, FrontendEffect>,
        ) {
            let FrontendInput::Logical(input) = input else {
                panic!("the original inlet only submits passive logical facts");
            };
            let LogicalInput::Snapshot { source, token } = *input else {
                panic!("the original snapshot source cannot substitute another fact");
            };
            assert!(step.charge(1));
            self.seen.lock().unwrap().push((source, token));
            if self.hold_effect {
                step.effect(
                    EffectDescriptor {
                        id: ActionId::new(1).unwrap(),
                        work: step.input_work().expect("same admitted source Work"),
                        kind: EffectKind::Cleanup,
                        dependencies: Vec::new(),
                    },
                    FrontendEffect::Test { value: token.get() },
                )
                .unwrap();
            }
        }
    }
    struct Fixture {
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        work: WorkObligation,
        payer: ReservationObligation,
        runtime: ActorRuntime<FrontendActor>,
        registration: ActorRegistration,
        inlet: LogicalInletObligation,
        seen: Arc<Mutex<Vec<(LogicalSourceId, LogicalRequestToken)>>>,
        failures: Arc<Mutex<Vec<InletFailure>>>,
        host: Arc<Probe>,
        producer_exited: bool,
    }
    fn fixture(hold_effect: bool) -> Fixture {
        fixture_with_gate(hold_effect, None)
    }
    fn fixture_with_gate(hold_effect: bool, gate: Option<Arc<AtomicBool>>) -> Fixture {
        fixture_with_positions(hold_effect, gate, 1)
    }
    fn fixture_with_positions(
        hold_effect: bool,
        gate: Option<Arc<AtomicBool>>,
        positions: usize,
    ) -> Fixture {
        let epoch = Generation::new(1).unwrap();
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger.open_scope_next(None, epoch).unwrap();
        let (mut work, payer) = scope
            .reserve_next(
                epoch,
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        inputs: positions,
                        batches: 2,
                        effects: 1,
                        ..OwnedSlotNeed::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let actor = ActorId::new(1).unwrap();
        let registration = runtime
            .register_supervised_bound(
                actor,
                scope.clone(),
                FrontendActor::Test(Box::new(Recorder {
                    seen: Arc::clone(&seen),
                    hold_effect,
                })),
                ActorConfig {
                    mailbox: [2; 4],
                    max_batches: 8,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 16,
                    max_step_work: 2,
                },
                &work,
            )
            .unwrap_or_else(|_| panic!("original paid actor registration"));
        let position = runtime
            .reserve_admitted_input_for(
                &registration,
                ActorId::new(2).unwrap(),
                InputClass::Ordinary,
                &work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap_or_else(|_| panic!("same original paid standing"));
        let host = Arc::new(Probe::default());
        let failures = Arc::new(Mutex::new(Vec::new()));
        let rejected = Arc::clone(&failures);
        let callback_probe = Arc::clone(&host);
        let callback: InletFailureCallback = Arc::new(move |failure| {
            if let Some(shared) = callback_probe.shared.lock().unwrap().upgrade() {
                assert!(
                    shared.state.try_lock().is_ok(),
                    "original owner failure callback runs outside inlet mutex"
                );
            }
            rejected.lock().unwrap().push(failure);
        });
        let mut inlet = LogicalInletObligation::new(
            position,
            source(),
            LogicalSourceKind::Snapshot,
            Waker::from(Arc::clone(&host)),
            callback,
        );
        if let Some(gate) = gate {
            inlet = inlet.with_admission_gate(gate);
        }
        *host.shared.lock().unwrap() = Arc::downgrade(&inlet.shared);
        Fixture {
            ledger,
            scope,
            work,
            payer,
            runtime,
            registration,
            inlet,
            seen,
            failures,
            host,
            producer_exited: false,
        }
    }
    fn source() -> LogicalSourceId {
        LogicalSourceId::new(NonZeroU64::new(17).unwrap())
    }
    fn fact(token: u64) -> LogicalHostFact {
        LogicalHostFact::Snapshot {
            token: LogicalRequestToken::new(NonZeroU64::new(token).unwrap()),
        }
    }
    fn once<F: Future + ?Sized>(mut future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.as_mut().poll(&mut Context::from_waker(waker))
    }
    impl Fixture {
        fn consume(&self) {
            assert!(self.runtime.poll_one(Tick(1)).unwrap());
        }
        fn producer_exit(&mut self) {
            if !self.producer_exited {
                self.work.body_exited().unwrap();
                self.work.captures_exited().unwrap();
                self.producer_exited = true;
            }
        }
        fn finish(mut self) {
            self.inlet.close();
            self.producer_exit();
            assert_eq!(
                self.inlet.progress(true, false).unwrap(),
                InletProgress::Closed
            );
            self.ledger.reap_scope_ready(self.scope.id(), 10).unwrap();
            self.payer
                .release_quota(&self.payer.resources().unwrap().quota)
                .unwrap();
            self.payer.finish().unwrap();
            self.scope.close_external().unwrap();
            while let Some(host) = self.runtime.claim_host_actor() {
                host.release(false).unwrap();
            }
            assert!(self.runtime.remove_actor(self.registration.actor()).is_ok());
            self.ledger.remove_scope(self.scope.id()).unwrap();
            assert!(self.ledger.is_empty());
        }
    }

    #[tokio::test]
    async fn committed_fact_waits_original_effect_retirement_and_busy_checks_do_not_wake_producer()
    {
        let mut f = fixture(true);
        let mut sender = f.inlet.take_sender().unwrap();
        assert_eq!(sender.id(), source());
        assert!(matches!(
            f.inlet.take_sender(),
            Err(LogicalSourceRefusal::AlreadyTaken)
        ));
        let producer = Arc::new(Probe::default());
        let waker = Waker::from(Arc::clone(&producer));
        let mut send = sender.send(fact(1));
        assert!(once(send.as_mut(), &waker).is_pending());
        let original = f.inlet.shared.lock().submitted.unwrap();
        f.consume();
        assert_eq!(
            f.seen.lock().unwrap().as_slice(),
            &[(source(), LogicalRequestToken::new(NonZeroU64::MIN))]
        );
        let host_before = f.host.calls.load(Ordering::SeqCst);
        let producer_before = producer.calls.load(Ordering::SeqCst);
        for _ in 0..64 {
            assert_eq!(f.inlet.progress(false, false).unwrap(), InletProgress::Idle);
        }
        assert_eq!(f.host.calls.load(Ordering::SeqCst), host_before);
        assert_eq!(producer.calls.load(Ordering::SeqCst), producer_before);
        assert!(
            once(send.as_mut(), &waker).is_pending(),
            "commit is not effect/capture retirement"
        );
        let effect = f
            .runtime
            .take_effect(f.registration.actor(), original, ActionId::new(1).unwrap())
            .unwrap();
        f.runtime.effect_completed(effect).unwrap();
        assert_eq!(
            f.inlet.progress(false, false).unwrap(),
            InletProgress::Changed
        );
        assert!(producer.calls.load(Ordering::SeqCst) > producer_before);
        assert_eq!(send.await, Ok(()));
        assert!(f.inlet.shared.lock().submitted.is_none());
        f.finish();
    }

    #[tokio::test]
    async fn cancelled_sender_waiter_keeps_same_occurrence_and_next_fact_waits_without_a_queue() {
        let mut f = fixture(false);
        let mut sender = f.inlet.take_sender().unwrap();
        let waker = Waker::noop();
        let mut first = sender.send(fact(1));
        assert!(once(first.as_mut(), waker).is_pending());
        let first_id = f.inlet.shared.lock().submitted.unwrap();
        drop(first);
        assert_eq!(f.inlet.shared.lock().submitted, Some(first_id));
        let mut next = sender.send(fact(2));
        assert!(once(next.as_mut(), waker).is_pending());
        assert_eq!(f.inlet.shared.lock().submitted, Some(first_id));
        assert!(f.seen.lock().unwrap().is_empty());
        f.consume();
        assert_eq!(
            f.inlet.progress(false, false).unwrap(),
            InletProgress::Changed
        );
        assert!(once(next.as_mut(), waker).is_pending());
        let next_id = f.inlet.shared.lock().submitted.unwrap();
        assert_ne!(first_id, next_id);
        assert_eq!(f.seen.lock().unwrap().len(), 1);
        f.consume();
        assert_eq!(
            f.inlet.progress(false, false).unwrap(),
            InletProgress::Changed
        );
        assert_eq!(next.await, Ok(()));
        assert_eq!(
            f.seen
                .lock()
                .unwrap()
                .iter()
                .map(|(_, t)| t.get())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        f.finish();
    }

    #[tokio::test]
    async fn close_waits_submitted_retirement_and_active_sender_frame_but_not_weak_handle_drop() {
        let mut f = fixture(false);
        let mut sender = f.inlet.take_sender().unwrap();
        let mut send = sender.send(fact(1));
        assert!(once(send.as_mut(), Waker::noop()).is_pending());
        f.inlet.close();
        f.producer_exit();
        assert_eq!(f.inlet.progress(true, false).unwrap(), InletProgress::Idle);
        assert!(!f.inlet.is_closed());
        assert!(
            once(send.as_mut(), Waker::noop()).is_pending(),
            "close cannot acknowledge queued input"
        );
        f.consume();
        assert_eq!(
            f.inlet.progress(true, false).unwrap(),
            InletProgress::Changed
        );
        assert!(
            !f.inlet.is_closed(),
            "the original submitted sender frame is still alive"
        );
        let before = f.host.calls.load(Ordering::SeqCst);
        assert_eq!(send.await, Err(LogicalSourceClosed));
        assert_eq!(f.host.calls.load(Ordering::SeqCst), before + 1);
        assert_eq!(
            f.inlet.progress(true, false).unwrap(),
            InletProgress::Closed
        );
        assert_eq!(sender.send(fact(2)).await, Err(LogicalSourceClosed));
        assert!(f.failures.lock().unwrap().is_empty());
        f.finish();
    }

    #[tokio::test]
    async fn normal_source_fence_drains_the_same_submitted_frame_before_closing_its_inlet() {
        let gate = Arc::new(AtomicBool::new(false));
        let mut f = fixture_with_gate(false, Some(Arc::clone(&gate)));
        let mut sender = f.inlet.take_sender().unwrap();
        let mut send = sender.send(fact(1));
        assert!(once(send.as_mut(), Waker::noop()).is_pending());
        let submitted = f.inlet.shared.lock().submitted.unwrap();
        gate.store(true, Ordering::Release);
        f.inlet.close_after_sender_frames();
        f.producer_exit();
        assert!(!f.inlet.shared.lock().closing);
        assert_eq!(f.inlet.shared.lock().submitted, Some(submitted));
        assert_eq!(f.inlet.progress(true, false).unwrap(), InletProgress::Idle);
        assert!(once(send.as_mut(), Waker::noop()).is_pending());
        f.consume();
        assert_eq!(
            f.inlet.progress(true, false).unwrap(),
            InletProgress::Changed
        );
        let before = f.host.calls.load(Ordering::SeqCst);
        assert_eq!(
            send.await,
            Ok(()),
            "the original healthy occurrence really retired"
        );
        assert_eq!(f.host.calls.load(Ordering::SeqCst), before + 1);
        assert_eq!(sender.send(fact(2)).await, Err(LogicalSourceClosed));
        f.inlet.close_after_sender_frames();
        assert_eq!(
            f.inlet.progress(true, false).unwrap(),
            InletProgress::Closed
        );
        assert!(f.failures.lock().unwrap().is_empty());
        f.finish();
    }

    #[test]
    fn original_position_loan_returns_on_unwind_with_only_requested_availability_wake() {
        let mut f = fixture(false);
        let original = f.inlet.shared.lock().position.as_ref().unwrap().id();
        let loan = f.inlet.shared.host_loan().unwrap();
        let before = f.host.calls.load(Ordering::SeqCst);
        assert_eq!(f.inlet.progress(false, false).unwrap(), InletProgress::Idle);
        assert_eq!(f.inlet.progress(false, false).unwrap(), InletProgress::Idle);
        assert_eq!(f.host.calls.load(Ordering::SeqCst), before);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _same_original_loan = loan;
            panic!("original producer unwind while holding its position loan");
        }));
        assert!(panic.is_err());
        assert_eq!(f.host.calls.load(Ordering::SeqCst), before + 1);
        assert_eq!(
            f.inlet.shared.lock().position.as_ref().unwrap().id(),
            original
        );
        for _ in 0..16 {
            assert_eq!(f.inlet.progress(false, false).unwrap(), InletProgress::Idle);
        }
        assert_eq!(f.host.calls.load(Ordering::SeqCst), before + 1);
        f.inlet.close();
        assert_eq!(f.inlet.progress(false, false).unwrap(), InletProgress::Idle);
        assert!(
            !f.inlet.is_closed(),
            "empty/returned position does not certify producer exit"
        );
        assert!(!f.work.actual_exit_ready().unwrap());
        f.finish();
    }

    #[tokio::test]
    async fn sender_waiting_for_short_host_loan_gets_one_requested_wake_without_retirement_notify()
    {
        let mut f = fixture(false);
        let mut sender = f.inlet.take_sender().unwrap();
        let loan = f.inlet.shared.host_loan().unwrap();
        let producer = Arc::new(Probe::default());
        let waker = Waker::from(Arc::clone(&producer));
        let mut send = sender.send(fact(1));
        assert!(once(send.as_mut(), &waker).is_pending());
        let before = producer.calls.load(Ordering::SeqCst);
        drop(loan);
        assert_eq!(producer.calls.load(Ordering::SeqCst), before + 1);
        assert!(once(send.as_mut(), &waker).is_pending());
        assert!(f.inlet.shared.lock().submitted.is_some());
        f.consume();
        assert_eq!(
            f.inlet.progress(false, false).unwrap(),
            InletProgress::Changed
        );
        assert_eq!(send.await, Ok(()));
        f.finish();
    }

    #[tokio::test]
    async fn wrong_kind_retains_original_input_in_owner_failure_callback_and_returns_closed() {
        let mut f = fixture(false);
        let mut sender = f.inlet.take_sender().unwrap();
        let wrong = LogicalHostFact::Cancelled(
            novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
        );
        assert_eq!(sender.send(wrong).await, Err(LogicalSourceClosed));
        {
            let failures = f.failures.lock().unwrap();
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].source, source());
            assert_eq!(failures[0].expected, LogicalSourceKind::Snapshot);
            assert_eq!(failures[0].cause, InletFailureCause::WrongKind);
            assert!(matches!(&failures[0].original,
                FrontendInput::Logical(input) if matches!(input.as_ref(),
                    LogicalInput::Cancelled(
                        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested
                    )
                )
            ));
        }
        assert!(f.inlet.shared.lock().submitted.is_none());
        assert!(f.seen.lock().unwrap().is_empty());
        assert_eq!(sender.send(fact(2)).await, Err(LogicalSourceClosed));
        assert_eq!(f.failures.lock().unwrap().len(), 1);
        assert_eq!(f.inlet.progress(false, false).unwrap(), InletProgress::Idle);
        f.finish();
    }

    #[tokio::test]
    async fn unexpected_busy_fill_is_owner_failure_without_retry_or_lost_original_occurrence() {
        let mut f = fixture(false);
        let mut sender = f.inlet.take_sender().unwrap();
        // Model a kernel fill whose foreign wake unwound before updating the
        // local metadata. Its actual original InputId remains in the kernel.
        let loan = f.inlet.shared.host_loan().unwrap();
        let original = loan
            .position()
            .fill(FrontendInput::Logical(Box::new(
                fact(1).into_input(source()),
            )))
            .unwrap();
        drop(loan);
        assert!(f.inlet.shared.lock().submitted.is_none());
        assert_eq!(sender.send(fact(2)).await, Err(LogicalSourceClosed));
        {
            let failures = f.failures.lock().unwrap();
            assert_eq!(failures.len(), 1);
            assert_eq!(
                failures[0].cause,
                InletFailureCause::Kernel(ActorTurnError::StandingInputBusy)
            );
            assert!(matches!(&failures[0].original,
                FrontendInput::Logical(input) if matches!(input.as_ref(),
                    LogicalInput::Snapshot { token, .. } if token.get() == 2)
            ));
        }
        assert_eq!(f.inlet.progress(false, false).unwrap(), InletProgress::Idle);
        assert_eq!(f.inlet.shared.lock().submitted, Some(original));
        f.consume();
        assert_eq!(
            f.inlet.progress(false, false).unwrap(),
            InletProgress::Changed
        );
        assert_eq!(
            f.seen
                .lock()
                .unwrap()
                .iter()
                .map(|(_, t)| t.get())
                .collect::<Vec<_>>(),
            vec![1]
        );
        f.finish();
    }

    #[test]
    fn kernel_progress_error_returns_original_loan_without_claiming_retirement_or_actual_exit() {
        let mut f = fixture(false);
        let loan = f.inlet.shared.host_loan().unwrap();
        let original = loan
            .position()
            .fill(FrontendInput::Logical(Box::new(
                fact(1).into_input(source()),
            )))
            .unwrap();
        drop(loan);
        // A host may not declare a healthy kernel actor failed. Preserve its
        // actual error, position and queued original instead of returning Closed.
        assert_eq!(
            f.inlet.progress(false, true),
            Err(ActorTurnError::ActorNotRetired)
        );
        assert_eq!(f.inlet.shared.lock().submitted, Some(original));
        assert!(!f.inlet.is_closed());
        assert!(!f.work.actual_exit_ready().unwrap());
        f.consume();
        assert_eq!(
            f.inlet.progress(false, false).unwrap(),
            InletProgress::Changed
        );
        f.finish();
    }
    #[tokio::test]
    async fn shared_admission_close_rejects_wrong_kind_before_budgeted_role_close_without_late_callback()
     {
        let gate = Arc::new(AtomicBool::new(false));
        let mut f = fixture_with_gate(false, Some(Arc::clone(&gate)));
        let mut sender = f.inlet.take_sender().unwrap();
        gate.store(true, Ordering::Release);
        assert!(
            !f.inlet.shared.lock().closing,
            "the budgeted row has not run yet"
        );
        assert_eq!(
            sender.send(LogicalHostFact::ConsumerClosed).await,
            Err(LogicalSourceClosed)
        );
        assert!(f.failures.lock().unwrap().is_empty());
        assert_eq!(f.inlet.progress(false, false).unwrap(), InletProgress::Idle);
        assert!(
            !f.inlet.is_closed(),
            "the admission flag is not original producer exit"
        );
        f.inlet.close();
        f.producer_exit();
        assert_eq!(
            f.inlet.progress(true, false).unwrap(),
            InletProgress::Closed
        );
        assert_eq!(
            sender.send(LogicalHostFact::ConsumerClosed).await,
            Err(LogicalSourceClosed)
        );
        assert!(
            f.failures.lock().unwrap().is_empty(),
            "closed weak callers cannot invoke a retired failure owner"
        );
        f.finish();
    }

    fn poll_ingress(observer: &LogicalIngressCloseObserver, probe: &Arc<Probe>) -> Poll<()> {
        observer.poll_closed(&mut Context::from_waker(&Waker::from(Arc::clone(probe))))
    }
    fn ingress_probe(f: &Fixture) -> Arc<Probe> {
        Arc::new(Probe {
            calls: AtomicUsize::new(0),
            shared: Mutex::new(Arc::downgrade(&f.inlet.shared)),
        })
    }
    fn fail_original_actor(f: &Fixture) -> StandingInputObligation<FrontendActor> {
        let position = f
            .runtime
            .reserve_admitted_input_for(
                &f.registration,
                f.registration.actor(),
                InputClass::Lifecycle,
                &f.work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap();
        position
            .fill(FrontendInput::LogicalInfrastructureFailed)
            .unwrap();
        f.consume();
        f.runtime.failure_proof_for(&f.registration).unwrap();
        position
    }
    fn settle_original_actor_failure(
        f: &Fixture,
        mut position: StandingInputObligation<FrontendActor>,
    ) {
        use novarocks_control_runtime::actor_turn::FailureSourceState;
        let actor = f.registration.actor();
        let FailureSourceState::ClosedPending(supervisor) =
            f.runtime.failure_source_state(actor).unwrap()
        else {
            panic!("the same original failure supervisor input");
        };
        let effect = f
            .runtime
            .take_effect(actor, supervisor, ActionId::new(1).unwrap())
            .unwrap();
        f.runtime.effect_completed(effect).unwrap();
        f.runtime.retire_input(actor, supervisor).unwrap();
        assert!(position.retire_ready_after_exit().unwrap());
        position.close_after_exit().unwrap();
        assert!(f.runtime.take_failure_payload(actor).unwrap().is_some());
    }

    #[test]
    fn ingress_close_waits_actual_input_effect_and_sender_frame_without_claiming_producer_exit() {
        let mut f = fixture(true);
        let observer = f.inlet.ingress_close_observer();
        let clone = observer.clone();
        let probe = ingress_probe(&f);
        assert!(poll_ingress(&observer, &probe).is_pending());
        let mut sender = f.inlet.take_sender().unwrap();
        let mut send = sender.send(fact(10));
        assert!(once(send.as_mut(), Waker::noop()).is_pending());
        let original = f.inlet.shared.lock().submitted.unwrap();
        f.inlet.close();
        assert!(poll_ingress(&clone, &probe).is_pending());
        f.consume();
        assert_eq!(f.inlet.progress(false, false).unwrap(), InletProgress::Idle);
        assert!(poll_ingress(&observer, &probe).is_pending());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
        let effect = f
            .runtime
            .take_effect(f.registration.actor(), original, ActionId::new(1).unwrap())
            .unwrap();
        f.runtime.effect_completed(effect).unwrap();
        assert_eq!(
            f.inlet.progress(false, false).unwrap(),
            InletProgress::Changed
        );
        assert!(f.inlet.shared.lock().submitted.is_none());
        assert!(
            poll_ingress(&observer, &probe).is_pending(),
            "original sender frame is live"
        );
        assert_eq!(
            once(send.as_mut(), Waker::noop()),
            Poll::Ready(Err(LogicalSourceClosed))
        );
        assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
        assert!(poll_ingress(&observer, &probe).is_ready());
        assert!(poll_ingress(&clone, &probe).is_ready());
        assert!(!f.producer_exited && !f.inlet.is_closed());
        assert!(!f.work.actual_exit_ready().unwrap());
        let before = probe.calls.load(Ordering::SeqCst);
        for _ in 0..8 {
            assert_eq!(f.inlet.progress(false, false).unwrap(), InletProgress::Idle);
            f.inlet.close();
        }
        assert_eq!(probe.calls.load(Ordering::SeqCst), before);
        drop(send);
        let mut rejected = sender.send(fact(11));
        assert_eq!(
            once(rejected.as_mut(), Waker::noop()),
            Poll::Ready(Err(LogicalSourceClosed))
        );
        assert!(poll_ingress(&observer, &probe).is_ready());
        assert_eq!(probe.calls.load(Ordering::SeqCst), before);
        drop(rejected);
        assert!(f.failures.lock().unwrap().is_empty());
        f.finish();
    }

    #[test]
    fn ingress_close_requires_same_position_return_and_last_frame_after_shared_gate() {
        let gate = Arc::new(AtomicBool::new(false));
        let mut f = fixture_with_gate(false, Some(Arc::clone(&gate)));
        let observer = f.inlet.ingress_close_observer();
        let probe = ingress_probe(&f);
        let original = f.inlet.shared.lock().position.as_ref().unwrap().id();
        let loan = f.inlet.shared.host_loan().unwrap();
        let frame = SenderFrameObligation::enter(Arc::clone(&f.inlet.shared));
        assert!(poll_ingress(&observer, &probe).is_pending());
        gate.store(true, Ordering::Release);
        for _ in 0..8 {
            assert_eq!(f.inlet.progress(false, false).unwrap(), InletProgress::Idle);
        }
        assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
        // A real state return observes the same permanent gate. Neither the
        // flag alone nor one remaining holder may publish settlement.
        drop(frame);
        assert!(poll_ingress(&observer, &probe).is_pending());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
        drop(loan);
        assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            f.inlet.shared.lock().position.as_ref().unwrap().id(),
            original
        );
        assert!(poll_ingress(&observer, &probe).is_ready());
        assert!(!f.inlet.shared.lock().closing);
        assert!(!f.producer_exited && !f.inlet.is_closed());
        assert!(!f.work.actual_exit_ready().unwrap());
        f.finish();
    }

    #[test]
    fn ingress_close_unwinding_original_loan_and_frame_publish_once_outside_custody_lock() {
        let f = fixture(false);
        let observer = f.inlet.ingress_close_observer();
        let probe = ingress_probe(&f);
        let loan = f.inlet.shared.host_loan().unwrap();
        let frame = SenderFrameObligation::enter(Arc::clone(&f.inlet.shared));
        f.inlet.close();
        assert!(poll_ingress(&observer, &probe).is_pending());
        let original = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _same_loan = loan;
            let _same_frame = frame;
            panic!("original send frame and position unwind");
        }));
        assert!(original.is_err());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
        assert!(poll_ingress(&observer, &probe).is_ready());
        assert_eq!(f.inlet.shared.lock().frames, 0);
        assert!(!f.producer_exited && !f.inlet.is_closed());
        f.finish();
    }

    #[test]
    fn failed_ingress_waits_original_source_payload_disposal_retirement_and_frame_return() {
        let mut f = fixture_with_positions(false, None, 2);
        let observer = f.inlet.ingress_close_observer();
        let probe = ingress_probe(&f);
        let mut sender = f.inlet.take_sender().unwrap();
        let mut send = sender.send(fact(12));
        assert!(once(send.as_mut(), Waker::noop()).is_pending());
        let original = f.inlet.shared.lock().submitted.unwrap();
        let failure_position = fail_original_actor(&f);
        assert!(
            f.seen.lock().unwrap().is_empty(),
            "the queued original never reached failed Core"
        );
        assert!(poll_ingress(&observer, &probe).is_pending());
        assert_eq!(
            f.inlet.progress(false, true).unwrap(),
            InletProgress::Changed
        );
        assert_eq!(f.inlet.shared.lock().submitted, Some(original));
        assert!(
            poll_ingress(&observer, &probe).is_pending(),
            "disposal is not retirement"
        );
        assert_eq!(
            f.inlet.progress(false, true).unwrap(),
            InletProgress::Changed
        );
        assert!(f.inlet.shared.lock().submitted.is_none());
        assert!(
            poll_ingress(&observer, &probe).is_pending(),
            "the actual sender frame remains"
        );
        assert_eq!(
            once(send.as_mut(), Waker::noop()),
            Poll::Ready(Err(LogicalSourceClosed))
        );
        assert!(poll_ingress(&observer, &probe).is_ready());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
        assert!(!f.producer_exited && !f.inlet.is_closed());
        assert!(!f.work.actual_exit_ready().unwrap());
        drop(send);
        settle_original_actor_failure(&f, failure_position);
        f.finish();
    }

    #[test]
    fn committed_fill_unwind_stays_unknown_until_same_position_failed_payload_retires() {
        let mut f = fixture_with_positions(false, None, 2);
        let observer = f.inlet.ingress_close_observer();
        let probe = ingress_probe(&f);
        assert!(poll_ingress(&observer, &probe).is_pending());
        let frame = SenderFrameObligation::enter(Arc::clone(&f.inlet.shared));
        let SendPosition::Loan(loan) = f.inlet.shared.sender_loan() else {
            panic!("the actual original sender position");
        };
        let original = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _same_frame = frame;
            let same_loan = loan;
            let input = same_loan
                .position()
                .fill(FrontendInput::Logical(Box::new(
                    fact(13).into_input(source()),
                )))
                .unwrap();
            assert_eq!(same_loan.position().in_flight().unwrap(), Some(input));
            // This is an actual kernel commit followed by the precise caller
            // cut before writing Shared.submitted, not a fabricated receipt.
            panic!("original caller unwind before local submitted id");
        }));
        assert!(original.is_err());
        {
            let state = f.inlet.shared.lock();
            assert!(state.submitted.is_none() && state.submission_unsettled);
            assert!(state.position.is_some() && state.frames == 0);
        }
        f.inlet.close();
        assert!(poll_ingress(&observer, &probe).is_pending());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
        let failure_position = fail_original_actor(&f);
        assert_eq!(
            f.inlet.progress(false, true).unwrap(),
            InletProgress::Changed
        );
        {
            let state = f.inlet.shared.lock();
            assert!(state.submitted.is_some() && !state.submission_unsettled);
        }
        assert!(poll_ingress(&observer, &probe).is_pending());
        assert_eq!(
            f.inlet.progress(false, true).unwrap(),
            InletProgress::Changed
        );
        assert!(poll_ingress(&observer, &probe).is_ready());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
        assert!(f.seen.lock().unwrap().is_empty());
        assert!(!f.producer_exited && !f.inlet.is_closed());
        assert!(!f.work.actual_exit_ready().unwrap());
        settle_original_actor_failure(&f, failure_position);
        f.finish();
    }

    #[test]
    fn detached_original_shared_without_close_latch_stays_unknown_and_observer_retains_no_owner() {
        let f = fixture(false);
        let before = Arc::strong_count(&f.inlet.shared);
        let observer = f.inlet.ingress_close_observer();
        let clone = observer.clone();
        assert_eq!(Arc::strong_count(&f.inlet.shared), before);
        let weak = Arc::downgrade(&f.inlet.shared);
        let mut position = f.inlet.shared.lock().position.take().unwrap();
        let Fixture {
            ledger,
            scope,
            mut work,
            mut payer,
            runtime,
            registration,
            inlet,
            seen: _,
            failures: _,
            host: _,
            producer_exited: _,
        } = f;
        drop(inlet);
        assert!(
            weak.upgrade().is_none(),
            "observation cannot keep the original owner alive"
        );
        let probe = Arc::new(Probe::default());
        assert!(poll_ingress(&observer, &probe).is_pending());
        assert!(poll_ingress(&clone, &probe).is_pending());
        // Finish through the retained SAME original role and Work, without
        // asserting that the detached passive observer saw any close event.
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        position.close_after_exit().unwrap();
        ledger.reap_scope_ready(scope.id(), 10).unwrap();
        payer
            .release_quota(&payer.resources().unwrap().quota)
            .unwrap();
        payer.finish().unwrap();
        scope.close_external().unwrap();
        while let Some(host) = runtime.claim_host_actor() {
            host.release(false).unwrap();
        }
        assert!(runtime.remove_actor(registration.actor()).is_ok());
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
        assert!(poll_ingress(&observer, &probe).is_pending());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    }
}
