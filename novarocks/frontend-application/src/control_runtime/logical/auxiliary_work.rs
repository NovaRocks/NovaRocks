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

//! Credentialed original Source cleanup with no logical Core input or action.
//! This obligation supervises only its own job, not the Native tail obligation.
use super::work::{FailureReporter, OBSERVER};
use crate::control_runtime::actor::FrontendActor;
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime},
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyError, ExitCustodyPhase, ExitObligation},
    exit_work::{ExitWorkError, PanicSink, run_future},
    header_readiness::{HeaderReadyError, HeaderReadyRegistration},
    obligation::WorkRetirementObserver,
    reported_work::ReportedWork,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::completion::SlotState;
use novarocks_query_application::api::logical_runtime::LogicalFuture;
use std::{
    error::Error,
    fmt,
    future::Future,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use tokio::{runtime::Handle, task::JoinHandle};

/// Called once by borrowing the retained factory. FnMut prevents consuming its
/// rich captures on the factory's panic stack; move-only inputs use Option.take.
pub(super) type AuxiliaryFactory = Box<dyn FnMut() -> LogicalFuture<'static, ()> + Send>;
pub(super) type AuxiliaryJoinOutcome = Result<(), Arc<dyn Error + Send + Sync>>;

pub(super) struct AuxiliaryHeaderOwner {
    pub(super) runtime: ActorRuntime<FrontendActor>,
    pub(super) actor: ActorRegistration,
}
pub(super) struct AuxiliaryWorkStart {
    pub(super) handle: Handle,
    pub(super) original: ReportedWork,
    pub(super) make: AuxiliaryFactory,
    pub(super) wake: Waker,
    pub(super) failure: FailureReporter,
    pub(super) retirement: AuxiliaryHeaderOwner,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AuxiliaryWorkError {
    LaunchFailed,
    JoinFailed,
    Work(ExitWorkError),
    OriginalCapturesUnresolved,
    Exit(ExitCustodyError),
    Runtime(RuntimeError),
    Header(HeaderReadyError),
}
impl fmt::Display for AuxiliaryWorkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "original auxiliary work failed: {self:?}")
    }
}
impl Error for AuxiliaryWorkError {}

struct OriginalBody {
    factory: Option<ManuallyDrop<AuxiliaryFactory>>,
    future: Option<ManuallyDrop<LogicalFuture<'static, ()>>>,
    called: bool,
}
type BodySlot = Arc<Mutex<OriginalBody>>;
struct State {
    launch: Option<ExitObligation>,
    launch_finished: bool,
    join: Option<JoinHandle<Result<(), ExitWorkError>>>,
    joined: bool,
    join_outcome: Option<AuxiliaryJoinOutcome>,
    report_consumed: bool,
    error: Option<AuxiliaryWorkError>,
    retirement_error: Option<AuxiliaryWorkError>,
    complete: bool,
}
#[must_use = "Retain the original auxiliary Join and unresolved work until actual retirement"]
pub(super) struct AuxiliaryWorkObligation {
    custody: ExitCustody,
    scope: ScopeHandle,
    report: ReportSlot,
    observer: WorkRetirementObserver,
    readiness: Mutex<Option<HeaderReadyRegistration>>,
    retirement: AuxiliaryHeaderOwner,
    body: BodySlot,
    state: Mutex<State>,
    panic: PanicSink,
    wake: Waker,
    failure: FailureReporter,
    retained: Mutex<Option<Arc<Self>>>,
}
impl AuxiliaryWorkObligation {
    pub(super) fn start(start: AuxiliaryWorkStart) -> Arc<Self> {
        let AuxiliaryWorkStart {
            handle,
            mut original,
            make,
            wake,
            failure,
            retirement,
        } = start;
        assert!(
            matches!(
                original.work().kind(),
                Ok(novarocks_control_runtime_core::scope::WorkKind::Cleanup
                    | novarocks_control_runtime_core::scope::WorkKind::Settlement)
            ),
            "auxiliary work consumes only original cleanup or settlement authority"
        );
        let readiness = original
            .header_readiness_registration()
            .expect("original auxiliary header readiness");
        let scope = original.work().scope().clone();
        let observer = original.work().retirement_observer();
        let report = original.report().clone();
        let (custody, exit) = ExitCustody::from_admitted(original);
        let owner = Arc::new(Self {
            custody,
            scope,
            report,
            observer,
            readiness: Mutex::new(Some(readiness)),
            retirement,
            body: Arc::new(Mutex::new(OriginalBody {
                factory: Some(ManuallyDrop::new(make)),
                future: None,
                called: false,
            })),
            state: Mutex::new(State {
                launch: Some(exit),
                launch_finished: false,
                join: None,
                joined: false,
                join_outcome: None,
                report_consumed: false,
                error: None,
                retirement_error: None,
                complete: false,
            }),
            panic: PanicSink::new(),
            wake,
            failure,
            retained: Mutex::new(None),
        });
        *owner.retained.lock().unwrap() = Some(owner.clone());
        // The spawned future owns only a slot alias until its first poll.
        // Cancellation before that poll leaves the unique Exit and factory here.
        let slot = owner.clone();
        let launched = catch_unwind(AssertUnwindSafe(|| {
            handle.spawn(async move {
                let exit = slot
                    .state
                    .lock()
                    .unwrap()
                    .launch
                    .take()
                    .expect("same original auxiliary launch");
                run_future(
                    exit,
                    AuxiliaryBodyFuture {
                        body: slot.body.clone(),
                    },
                    slot.panic.clone(),
                )
                .await
            })
        }));
        match launched {
            Ok(join) => {
                let mut state = owner.state.lock().unwrap();
                state.join = Some(join);
                state.launch_finished = true;
            }
            Err(original) => {
                owner.state.lock().unwrap().error = Some(AuxiliaryWorkError::LaunchFailed);
                (owner.failure)(original);
            }
        }
        owner.wake.wake_by_ref();
        owner
    }
    fn poll_join(self: &Arc<Self>, cx: &mut Context<'_>) -> Poll<Result<(), AuxiliaryWorkError>> {
        let join = {
            let mut state = self.state.lock().unwrap();
            if state.joined {
                return Poll::Ready(Ok(()));
            }
            if let Some(AuxiliaryWorkError::LaunchFailed) = state.error {
                return Poll::Ready(Err(AuxiliaryWorkError::LaunchFailed));
            }
            if !state.launch_finished {
                return Poll::Pending;
            }
            state.join.take()
        };
        let Some(join) = join else {
            return Poll::Pending;
        };
        let mut loan = JoinLoan {
            owner: self.clone(),
            join: Some(join),
        };
        let result = match Pin::new(loan.join.as_mut().unwrap()).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        drop(loan.join.take());
        let (outcome, error) = match result {
            Ok(Ok(())) => (Ok(()), None),
            Ok(Err(error)) => {
                let original = Arc::new(error);
                let shared: Arc<dyn Error + Send + Sync> = original.clone();
                (self.failure)(Box::new(original));
                (Err(shared), Some(AuxiliaryWorkError::Work(error)))
            }
            Err(error) => {
                let original = Arc::new(error);
                let shared: Arc<dyn Error + Send + Sync> = original.clone();
                (self.failure)(Box::new(original));
                (Err(shared), Some(AuxiliaryWorkError::JoinFailed))
            }
        };
        {
            let mut state = self.state.lock().unwrap();
            state.joined = true;
            state.join_outcome = Some(outcome);
            state.error = error;
        }
        while let Some(original) = self.panic.take() {
            (self.failure)(original);
        }
        Poll::Ready(Ok(()))
    }
    /// Returns the real Tokio Join (or its real wrapper error) once. An error
    /// Arc aliases the SAME original object already retained by the reporter.
    pub(super) fn take_join_outcome(&self) -> Option<AuxiliaryJoinOutcome> {
        self.state.lock().unwrap().join_outcome.take()
    }
    pub(super) fn retired(&self) -> bool {
        self.state.lock().unwrap().complete
    }
    pub(super) fn actual_exit(&self) -> bool {
        matches!(
            self.custody.phase(),
            ExitCustodyPhase::Exited | ExitCustodyPhase::Retired
        ) && self.custody.body_exited()
            && self.custody.captures_exited()
    }
    /// Observe the original Join and retire its actual work/report, without
    /// waiting for header pins held by the supervisor's own input/ticket.
    /// The caller must then complete those originals before poll_complete can
    /// reap the same header. `false` retains real outstanding work custody.
    pub(super) fn poll_work_retirement(
        self: &Arc<Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<bool, AuxiliaryWorkError>> {
        std::task::ready!(self.poll_join(cx))?;
        if !self.actual_exit() {
            return Poll::Ready(Err(AuxiliaryWorkError::OriginalCapturesUnresolved));
        }
        Poll::Ready(self.try_retire_work())
    }
    pub(super) fn poll_complete(
        self: &Arc<Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), AuxiliaryWorkError>> {
        let completed = {
            let state = self.state.lock().unwrap();
            state.complete.then_some(state.error.map_or(Ok(()), Err))
        };
        if let Some(outcome) = completed {
            return Poll::Ready(outcome);
        }
        match self.poll_join(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        if !self.actual_exit() {
            return Poll::Ready(Err(AuxiliaryWorkError::OriginalCapturesUnresolved));
        }
        let result = self.try_retire();
        match result {
            Ok(true) => {
                let outcome = {
                    let mut state = self.state.lock().unwrap();
                    state.complete = true;
                    state.error.map_or(Ok(()), Err)
                };
                let registration = self.readiness.lock().unwrap().take();
                if let Some(mut registration) = registration {
                    registration.cancel();
                }
                drop(self.retained.lock().unwrap().take());
                Poll::Ready(outcome)
            }
            Ok(false) => {
                let mut registration = self
                    .readiness
                    .lock()
                    .unwrap()
                    .take()
                    .expect("same original auxiliary header registration");
                let armed = registration.arm(
                    &self.retirement.runtime,
                    &self.retirement.actor,
                    &self.observer,
                );
                *self.readiness.lock().unwrap() = Some(registration);
                match armed {
                    Ok(_) => Poll::Pending,
                    Err(error) => self.retirement_failure(AuxiliaryWorkError::Header(error)),
                }
            }
            Err(error) => self.retirement_failure(error),
        }
    }
    fn retirement_failure(
        &self,
        error: AuxiliaryWorkError,
    ) -> Poll<Result<(), AuxiliaryWorkError>> {
        let first = {
            let mut state = self.state.lock().unwrap();
            let first = state.retirement_error.is_none();
            state.retirement_error.get_or_insert(error);
            state.error.get_or_insert(error);
            first
        };
        if first {
            (self.failure)(Box::new(error));
        }
        Poll::Ready(Err(error))
    }
    fn try_retire(&self) -> Result<bool, AuxiliaryWorkError> {
        Ok(self.try_retire_work()?
            && self
                .scope
                .reap_observed_header(&self.observer)
                .map_err(AuxiliaryWorkError::Runtime)?)
    }
    fn try_retire_work(&self) -> Result<bool, AuxiliaryWorkError> {
        if !self.state.lock().unwrap().report_consumed {
            match self
                .report
                .publication_state()
                .map_err(AuxiliaryWorkError::Runtime)?
            {
                SlotState::Filled => self
                    .report
                    .consume(OBSERVER)
                    .map_err(AuxiliaryWorkError::Runtime)?,
                SlotState::RevokedUnused => {}
                SlotState::Reserved => return Err(AuxiliaryWorkError::OriginalCapturesUnresolved),
            }
            self.state.lock().unwrap().report_consumed = true;
        }
        Ok(self.custody.phase() == ExitCustodyPhase::Retired
            || self
                .custody
                .try_retire()
                .map_err(AuxiliaryWorkError::Exit)?)
    }
}
struct JoinLoan {
    owner: Arc<AuxiliaryWorkObligation>,
    join: Option<JoinHandle<Result<(), ExitWorkError>>>,
}
impl Drop for JoinLoan {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            assert!(
                self.owner
                    .state
                    .lock()
                    .unwrap()
                    .join
                    .replace(join)
                    .is_none(),
                "same auxiliary Join loan returns once"
            );
        }
    }
}
struct FactoryLoan {
    body: BodySlot,
    factory: Option<ManuallyDrop<AuxiliaryFactory>>,
}
impl Drop for FactoryLoan {
    fn drop(&mut self) {
        if let Some(factory) = self.factory.take() {
            assert!(
                self.body.lock().unwrap().factory.replace(factory).is_none(),
                "same auxiliary factory loan returns once"
            );
        }
    }
}
struct FutureLoan {
    body: BodySlot,
    future: Option<ManuallyDrop<LogicalFuture<'static, ()>>>,
}
impl Drop for FutureLoan {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            assert!(
                self.body.lock().unwrap().future.replace(future).is_none(),
                "same auxiliary future loan returns once"
            );
        }
    }
}
struct AuxiliaryBodyFuture {
    body: BodySlot,
}
impl Future for AuxiliaryBodyFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let factory = {
            let mut body = self.body.lock().unwrap();
            if !body.called {
                body.called = true;
                body.factory.take()
            } else {
                None
            }
        };
        if let Some(factory) = factory {
            let mut loan = FactoryLoan {
                body: self.body.clone(),
                factory: Some(factory),
            };
            let make: &mut AuxiliaryFactory = loan.factory.as_mut().unwrap();
            let future = make();
            assert!(
                self.body
                    .lock()
                    .unwrap()
                    .future
                    .replace(ManuallyDrop::new(future))
                    .is_none(),
                "one original auxiliary future"
            );
        }
        let future = self
            .body
            .lock()
            .unwrap()
            .future
            .take()
            .expect("same original auxiliary future");
        let mut loan = FutureLoan {
            body: self.body.clone(),
            future: Some(future),
        };
        let future: &mut LogicalFuture<'static, ()> = loan.future.as_mut().unwrap();
        future.as_mut().poll(cx)
    }
}
impl Drop for AuxiliaryBodyFuture {
    fn drop(&mut self) {
        let future = self.body.lock().unwrap().future.take();
        if let Some(future) = future {
            drop(ManuallyDrop::into_inner(future));
        }
        // If future F-Drop panics, the stable factory remains untouched. Its
        // rich destructor must never run during the first destructor unwind.
        let factory = self.body.lock().unwrap().factory.take();
        if let Some(factory) = factory {
            drop(ManuallyDrop::into_inner(factory));
        }
    }
}

#[cfg(test)]
mod tests;
