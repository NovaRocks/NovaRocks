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

// Design: ADR-0218 (docs/adr/ADR-0218-work-credentials-and-credentialed-entries.md)
//! Exit wrappers keep one original obligation with the actual future or job.
//! The wrapper destroys the work and its captures before publishing exit. This
//! covers this work item, not detached work started internally by dependencies.

use crate::{
    exit_custody::{ExitCustodyError, ExitObligation},
    sync::{Arc, Mutex},
};
use std::{
    any::Any,
    future::Future,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use novarocks_control_runtime_core::identity::Generation;

pub type PanicPayload = Box<dyn Any + Send + 'static>;

#[derive(Default)]
struct PanicSlots {
    primary: Option<ManuallyDrop<PanicPayload>>,
    cleanup: Option<ManuallyDrop<PanicPayload>>,
}

/// One work item's original panic and, if explicit teardown also panics, its
/// cleanup panic. Clones share custody. Payload destructors run only after an
/// owner takes them; abandoning a sink deliberately retains foreign payloads.
/// Taking a payload never manufactures successful exit.
#[derive(Clone, Default)]
pub struct PanicSink(Arc<Mutex<PanicSlots>>);
impl PanicSink {
    pub fn new() -> Self {
        Self::default()
    }
    /// Takes the original panic first, then any still-retained cleanup panic.
    pub fn take(&self) -> Option<PanicPayload> {
        let mut slots = self.0.lock().expect("work panic custody lock");
        slots
            .primary
            .take()
            .or_else(|| slots.cleanup.take())
            .map(ManuallyDrop::into_inner)
    }
    /// Takes only the second panic from explicit capture destruction.
    pub fn take_cleanup(&self) -> Option<PanicPayload> {
        self.0
            .lock()
            .expect("work panic custody lock")
            .cleanup
            .take()
            .map(ManuallyDrop::into_inner)
    }
    pub fn has_panic(&self) -> bool {
        let slots = self.0.lock().expect("work panic custody lock");
        slots.primary.is_some() || slots.cleanup.is_some()
    }
    fn store(&self, payload: PanicPayload) {
        self.store_panics(payload, None);
    }
    fn store_panics(&self, payload: PanicPayload, cleanup: Option<PanicPayload>) {
        let payload = ManuallyDrop::new(payload);
        let cleanup = cleanup.map(ManuallyDrop::new);
        let mut slots = self.0.lock().expect("work panic custody lock");
        // Each sink belongs to one invocation. Reusing an occupied sink would
        // discard an original panic, so this is an invariant failure.
        assert!(
            slots.primary.is_none() && slots.cleanup.is_none(),
            "work panic sink already holds a payload"
        );
        slots.primary = Some(payload);
        // Publish both stages under one lock: an owner may take the primary
        // as soon as it observes a panic, even before this work reports exit.
        slots.cleanup = cleanup;
    }
}
/// The actual panic remains in the work's PanicSink. This marker continues
/// unwinding so the carrier and its supervisor still observe a failed task.
#[derive(Debug)]
pub struct ForwardedWorkPanic;
fn resume_forwarded_panic() -> ! {
    // A Box cannot be owned by both the custodian and the unwinding carrier.
    // The original remains in the sink; the carrier propagates this marker.
    resume_unwind(Box::new(ForwardedWorkPanic))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExitWorkError {
    NotStarted(ExitCustodyError),
    Exit(ExitCustodyError),
}
impl std::fmt::Display for ExitWorkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "obligated work failed: {self:?}")
    }
}
impl std::error::Error for ExitWorkError {}

struct ExitState {
    obligation: Option<ExitObligation>,
    started: bool,
    destroying: bool,
}
impl ExitState {
    fn new(obligation: ExitObligation) -> Self {
        Self {
            obligation: Some(obligation),
            started: false,
            destroying: false,
        }
    }
    fn start(&mut self) -> Result<(), ExitCustodyError> {
        self.obligation
            .as_mut()
            .expect("work has not completed")
            .start()?;
        self.started = true;
        Ok(())
    }
    fn exit(&mut self) -> Result<(), ExitCustodyError> {
        let obligation = self.obligation.take().expect("work exits once");
        if self.started {
            obligation.exited()
        } else {
            obligation.revoke_unused_and_exit()
        }
    }
}

fn destroy_inner<T>(
    inner: &mut Option<T>,
    state: &mut ExitState,
    panic: &PanicSink,
    body_panic: Option<PanicPayload>,
) {
    state.destroying = true;
    // Catch explicit teardown separately from the body. In particular, keep a
    // body panic in custody while teardown runs, rather than dropping its Box
    // during a second unwind. Neither panic proves that captures exited.
    let destroyed = catch_unwind(AssertUnwindSafe(|| drop(inner.take())));
    match destroyed {
        Ok(()) => {
            state.destroying = false;
            if let Some(payload) = body_panic {
                panic.store(payload);
            }
        }
        Err(cleanup) => {
            if let Some(payload) = body_panic {
                panic.store_panics(payload, Some(cleanup));
            } else {
                panic.store(cleanup);
            }
            // Publish Unknown through the original obligation before the
            // carrier can observe the forwarded failure. Never publish exit.
            drop(state.obligation.take());
            resume_forwarded_panic()
        }
    }
}

#[must_use = "The future owns an admitted work obligation until actual destruction"]
pub struct ObligatedFuture<F> {
    inner: Option<Pin<Box<F>>>,
    state: ExitState,
    panic: PanicSink,
}
pub fn run_future<F: Future>(
    obligation: ExitObligation,
    future: F,
    panic: PanicSink,
) -> ObligatedFuture<F> {
    ObligatedFuture {
        inner: Some(Box::pin(future)),
        state: ExitState::new(obligation),
        panic,
    }
}
impl<F> ObligatedFuture<F> {
    fn destroy_inner(&mut self, body_panic: Option<PanicPayload>) {
        destroy_inner(&mut self.inner, &mut self.state, &self.panic, body_panic);
    }
}
impl<F: Future> Future for ObligatedFuture<F> {
    type Output = Result<F::Output, ExitWorkError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(
            !this.state.destroying,
            "work capture destruction previously panicked"
        );
        assert!(
            this.inner.is_some(),
            "obligated future polled after completion"
        );
        if !this.state.started
            && let Err(error) = this.state.start()
        {
            this.destroy_inner(None);
            return Poll::Ready(match this.state.exit() {
                Ok(()) => Err(ExitWorkError::NotStarted(error)),
                Err(error) => Err(ExitWorkError::Exit(error)),
            });
        }
        match catch_unwind(AssertUnwindSafe(|| {
            this.inner.as_mut().expect("live future").as_mut().poll(cx)
        })) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => {
                this.destroy_inner(None);
                Poll::Ready(
                    this.state
                        .exit()
                        .map(|()| value)
                        .map_err(ExitWorkError::Exit),
                )
            }
            Err(payload) => {
                this.destroy_inner(Some(payload));
                // An exit error remains unresolved in the original custody.
                // Preserve and propagate the original business panic regardless.
                let _ = this.state.exit();
                resume_forwarded_panic()
            }
        }
    }
}
impl<F> Drop for ObligatedFuture<F> {
    fn drop(&mut self) {
        if self.state.obligation.is_some() && !self.state.destroying {
            self.destroy_inner(None);
            let _ = self.state.exit();
        }
        // Failed explicit destruction already dropped the original obligation
        // to record unresolved exit, never a positive exit.
    }
}

#[must_use = "The blocking job owns its admitted work until invocation or destruction"]
pub struct ObligatedBlocking<F> {
    inner: Option<F>,
    state: ExitState,
    panic: PanicSink,
}
pub fn run_blocking<F>(
    obligation: ExitObligation,
    job: F,
    panic: PanicSink,
) -> ObligatedBlocking<F> {
    ObligatedBlocking {
        inner: Some(job),
        state: ExitState::new(obligation),
        panic,
    }
}
impl<F> ObligatedBlocking<F> {
    fn destroy_inner(&mut self) {
        destroy_inner(&mut self.inner, &mut self.state, &self.panic, None);
    }
    /// Once invoked, a blocking closure runs until it returns or unwinds.
    /// Panic inside FnOnce::call_once (including its internal RAII teardown)
    /// follows the work panic path, like panic inside Future::poll. Only the
    /// wrapper's explicit destruction can identify a capture destructor panic.
    /// Its return value is transferred to the caller before this work retires.
    pub fn run<R>(mut self) -> Result<R, ExitWorkError>
    where
        F: FnOnce() -> R,
    {
        if let Err(error) = self.state.start() {
            self.destroy_inner();
            return match self.state.exit() {
                Ok(()) => Err(ExitWorkError::NotStarted(error)),
                Err(error) => Err(ExitWorkError::Exit(error)),
            };
        }
        let job = self.inner.take().expect("blocking job runs once");
        match catch_unwind(AssertUnwindSafe(job)) {
            Ok(value) => self
                .state
                .exit()
                .map(|()| value)
                .map_err(ExitWorkError::Exit),
            Err(payload) => {
                self.panic.store(payload);
                let _ = self.state.exit();
                resume_forwarded_panic()
            }
        }
    }
}
impl<F> Drop for ObligatedBlocking<F> {
    fn drop(&mut self) {
        if self.state.obligation.is_some() && !self.state.destroying {
            self.destroy_inner();
            let _ = self.state.exit();
        }
    }
}

/// The only competition between withdrawal and the actual handoff. Starting
/// wins before the original generation gate; a refused gate still exits unused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandoffState {
    Pending,
    Starting,
    Withdrawn,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Withdrawal {
    Withdrawn,
    TooLate,
}
struct HandoffRecord {
    state: HandoffState,
    waker: Option<Waker>,
}
pub struct HandoffCell(Mutex<HandoffRecord>);
impl Default for HandoffCell {
    fn default() -> Self {
        Self::new()
    }
}
impl HandoffCell {
    pub fn new() -> Self {
        Self(Mutex::new(HandoffRecord {
            state: HandoffState::Pending,
            waker: None,
        }))
    }
    pub fn state(&self) -> HandoffState {
        self.0.lock().expect("work handoff lock").state
    }
    /// Exactly one Pending -> Withdrawn or Pending -> Starting transition wins.
    pub fn withdraw(&self) -> Withdrawal {
        let wake = {
            let mut record = self.0.lock().expect("work handoff lock");
            if record.state != HandoffState::Pending {
                return Withdrawal::TooLate;
            }
            record.state = HandoffState::Withdrawn;
            record.waker.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        Withdrawal::Withdrawn
    }
    fn poll_withdrawn(&self, cx: &Context<'_>) -> Poll<()> {
        let mut record = self.0.lock().expect("work handoff lock");
        if record.state == HandoffState::Withdrawn {
            return Poll::Ready(());
        }
        if record
            .waker
            .as_ref()
            .is_none_or(|w| !w.will_wake(cx.waker()))
        {
            record.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
    fn try_start(&self) -> bool {
        let mut record = self.0.lock().expect("work handoff lock");
        if record.state != HandoffState::Pending {
            return false;
        }
        record.state = HandoffState::Starting;
        record.waker.take();
        true
    }
    fn clear_waiter(&self) {
        self.0.lock().expect("work handoff lock").waker.take();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatedNotStarted {
    Withdrawn,
    GenerationRejected(ExitCustodyError),
}
#[derive(Debug, Eq, PartialEq)]
pub enum GatedOutcome<T> {
    Completed(T),
    NotStarted(GatedNotStarted),
}

/// Holds the original, unstarted exit obligation and its captures. This owner
/// checks exit through ExitCustody even when it is dropped before `run`.
#[must_use = "Gated work retains its original exit obligation until actual destruction"]
pub struct GatedWork<C> {
    captures: Option<C>,
    state: ExitState,
    handoff: Arc<HandoffCell>,
    panic: PanicSink,
}
impl<C> GatedWork<C> {
    pub fn new(
        obligation: ExitObligation,
        captures: C,
        handoff: Arc<HandoffCell>,
        panic: PanicSink,
    ) -> Self {
        Self {
            captures: Some(captures),
            state: ExitState::new(obligation),
            handoff,
            panic,
        }
    }
    /// Waiting does not start the admitted work. Its future owns all pre-handoff
    /// admission, quote and capacity registrations. On refusal, destruction is
    /// ordered: waiting future/output, body closure, captures, then unused exit.
    /// The body must keep any resources needed after handoff in its future.
    pub fn run<P, B, F>(
        mut self,
        generation: Generation,
        pre_handoff: P,
        body: B,
    ) -> impl Future<Output = Result<GatedOutcome<F::Output>, ExitWorkError>>
    where
        P: Future,
        B: FnOnce(C, P::Output) -> F,
        F: Future,
    {
        // Construct this guard before constructing the async future so that
        // even a future that is never polled has the same teardown ordering.
        let mut pending = PendingGatedWork {
            values: Some(PendingGatedValues {
                pre_handoff: Some(Box::pin(pre_handoff)),
                prepared: None,
                body: Some(body),
                captures: self.captures.take(),
            }),
            state: ExitState {
                obligation: self.state.obligation.take(),
                started: false,
                destroying: false,
            },
            handoff: self.handoff.clone(),
            panic: self.panic.clone(),
        };
        async move {
            let ready = std::future::poll_fn(|cx| {
                // Check/register and withdrawal share the same lock. A wake
                // between this check and the pending poll cannot be lost.
                if pending.handoff.poll_withdrawn(cx).is_ready() {
                    return Poll::Ready(false);
                }
                let polled = catch_unwind(AssertUnwindSafe(|| {
                    pending
                        .values
                        .as_mut()
                        .expect("pending gated values")
                        .pre_handoff
                        .as_mut()
                        .expect("live pre-handoff future")
                        .as_mut()
                        .poll(cx)
                }));
                match polled {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(value)) => {
                        pending
                            .values
                            .as_mut()
                            .expect("pending gated values")
                            .prepared = Some(value);
                        Poll::Ready(true)
                    }
                    Err(payload) => {
                        pending.destroy(Some(payload));
                        let _ = pending.state.exit();
                        resume_forwarded_panic()
                    }
                }
            })
            .await;
            if !ready || !pending.handoff.try_start() {
                return pending.not_started(GatedNotStarted::Withdrawn);
            }
            let started = pending
                .state
                .obligation
                .as_mut()
                .expect("original gated obligation")
                .start_in_generation(generation);
            if let Err(error) = started {
                return pending.not_started(GatedNotStarted::GenerationRejected(error));
            }
            pending.state.started = true;
            pending.handoff.clear_waiter();
            // Destroy the completed waiting object before running the body.
            // A destructor panic retains Unknown through the same custody as
            // every other explicit work destruction.
            pending.destroy_pre();
            let values = pending.values.as_mut().expect("pending gated values");
            let captures = values.captures.take().expect("gated captures");
            let prepared = values.prepared.take().expect("completed pre-handoff");
            let body = values.body.take().expect("gated body");
            let future = GatedBody {
                future: None,
                initial: Some(Box::new((body, captures, prepared))),
            };
            // Reuse the accepted-work wrapper without starting a second time.
            let running = ObligatedFuture {
                inner: Some(Box::pin(future)),
                state: ExitState {
                    obligation: pending.state.obligation.take(),
                    started: true,
                    destroying: false,
                },
                panic: pending.panic.clone(),
            };
            running.await.map(GatedOutcome::Completed)
        }
    }
}
impl<C> Drop for GatedWork<C> {
    fn drop(&mut self) {
        if self.state.obligation.is_some() && !self.state.destroying {
            self.handoff.clear_waiter();
            destroy_inner(&mut self.captures, &mut self.state, &self.panic, None);
            let _ = self.state.exit();
        }
    }
}
// Keep the produced future alive after Ready. An async `.await` wrapper
// would destroy it inside poll, misclassifying a destructor panic as body panic.
struct GatedBody<C, P, B, F> {
    future: Option<Pin<Box<F>>>,
    initial: Option<Box<(B, C, P)>>,
}
impl<C, P, B, F> Future for GatedBody<C, P, B, F>
where
    B: FnOnce(C, P) -> F,
    F: Future,
{
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(initial) = this.initial.take() {
            let (body, captures, prepared) = *initial;
            this.future = Some(Box::pin(body(captures, prepared)));
        }
        this.future
            .as_mut()
            .expect("started gated body")
            .as_mut()
            .poll(cx)
    }
}
// Field order is part of unused-work cleanup: release the waiting owner and
// any prepared quote first, then the never-invoked body closure, then captures.
struct PendingGatedValues<C, P: Future, B> {
    pre_handoff: Option<Pin<Box<P>>>,
    prepared: Option<P::Output>,
    body: Option<B>,
    captures: Option<C>,
}
struct PendingGatedWork<C, P: Future, B> {
    values: Option<PendingGatedValues<C, P, B>>,
    state: ExitState,
    handoff: Arc<HandoffCell>,
    panic: PanicSink,
}
impl<C, P: Future, B> PendingGatedWork<C, P, B> {
    fn destroy(&mut self, payload: Option<PanicPayload>) {
        self.handoff.clear_waiter();
        destroy_inner(&mut self.values, &mut self.state, &self.panic, payload);
    }
    fn destroy_pre(&mut self) {
        self.state.destroying = true;
        let pre = self
            .values
            .as_mut()
            .expect("pending gated values")
            .pre_handoff
            .take();
        match catch_unwind(AssertUnwindSafe(|| drop(pre))) {
            Ok(()) => self.state.destroying = false,
            Err(payload) => {
                // Finish explicit capture disposal before forwarding a waiting
                // destructor panic; retain both stages if cleanup also panics.
                let cleanup = catch_unwind(AssertUnwindSafe(|| drop(self.values.take()))).err();
                self.panic.store_panics(payload, cleanup);
                drop(self.state.obligation.take());
                resume_forwarded_panic()
            }
        }
    }
    fn not_started<T>(
        &mut self,
        reason: GatedNotStarted,
    ) -> Result<GatedOutcome<T>, ExitWorkError> {
        self.destroy(None);
        self.state
            .exit()
            .map(|()| GatedOutcome::NotStarted(reason))
            .map_err(ExitWorkError::Exit)
    }
}
impl<C, P: Future, B> Drop for PendingGatedWork<C, P, B> {
    fn drop(&mut self) {
        if self.state.obligation.is_some() && !self.state.destroying {
            self.destroy(None);
            let _ = self.state.exit();
        }
    }
}

#[cfg(all(test, not(loom)))]
#[path = "exit_work_tests.rs"]
mod tests;
