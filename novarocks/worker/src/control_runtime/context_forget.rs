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

//! Once-only disposal of the original execution admission fence, admitted only
//! when its retained horizon expires. The existing Root Cleanup role pays the
//! callback, capture and return; a cold Context owns no reserved forget Work.

use super::context_ready::ContextReadyNotice;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::WorkRetirementObserver,
    owned_return::{OwnedReturnReader, OwnedReturnSender},
    reported_work::{ReportedWork, ReportedWorkBindingError},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    completion::{ConsumerKind, Consumption, ReturnState},
    identity::{ConsumerId, ScopeId, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::WorkKind,
};
use novarocks_execution_contract::QueryContextRef;
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContextForgetError {
    WrongOwner,
    WrongStorage,
    WrongObserver,
    StorageBinding(ReportedWorkBindingError),
    Runtime(RuntimeError),
    Exit(ExitRoleError),
}
impl std::fmt::Display for ContextForgetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner => {
                f.write_str("Context forget role does not belong to the original owner")
            }
            Self::WrongStorage => f.write_str("Context forget role has the wrong admitted storage"),
            Self::WrongObserver => {
                f.write_str("Context forget role has the wrong original observer")
            }
            Self::StorageBinding(error) => {
                write!(f, "Context forget storage binding failed: {error}")
            }
            Self::Runtime(error) => write!(f, "Context forget runtime operation failed: {error}"),
            Self::Exit(error) => write!(f, "Context forget physical exit failed: {error}"),
        }
    }
}
impl std::error::Error for ContextForgetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::StorageBinding(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Exit(error) => Some(error),
            Self::WrongOwner | Self::WrongStorage | Self::WrongObserver => None,
        }
    }
}

impl From<RuntimeError> for ContextForgetError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl From<ExitRoleError> for ContextForgetError {
    fn from(error: ExitRoleError) -> Self {
        Self::Exit(error)
    }
}
#[must_use = "A refused forget retains every original capture and reported role"]
pub(crate) struct ContextForgetRefusal {
    pub error: ContextForgetError,
    pub context: QueryContextRef,
    pub notice: ContextReadyNotice,
    pub host: Arc<dyn crate::TaskExecutionHost>,
    pub original: ReportedWork,
}
impl std::fmt::Debug for ContextForgetRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextForgetRefusal")
            .field("error", &self.error)
            .field("context", &self.context)
            .field("original", &self.original)
            .finish_non_exhaustive()
    }
}

type HostCapture = Box<ManuallyDrop<Arc<dyn crate::TaskExecutionHost>>>;
struct State {
    // A stable backing permits one actual destructor attempt. A partial Drop
    // retains this backing and its panic without touching it on the poller.
    host: Option<HostCapture>,
    authority: Option<ExitAuthority>,
    sender: Option<OwnedReturnSender<()>>,
    reader: OwnedReturnReader<()>,
    started: bool,
    panic: Option<ManuallyDrop<Box<dyn Any + Send>>>,
    error: Option<ContextForgetError>,
}
struct Custody {
    context: QueryContextRef,
    key: (ScopeId, WorkId),
    notice: ContextReadyNotice,
    observer: ConsumerId,
    role: ExitRole,
    report: ReportSlot,
    retirement: WorkRetirementObserver,
    state: Mutex<State>,
    // Retains only this original admitted role's finite capture on Unknown.
    // It neither creates a replacement Work nor permits another invocation.
    retained: Mutex<Option<Arc<Self>>>,
}
#[derive(Clone)]
#[must_use = "Keep the original forget owner through report consumption and header reaping"]
pub(crate) struct ContextForgetOwner(Arc<Custody>);
#[must_use = "A rejected physical queue returns this same original forget job"]
pub(crate) struct ContextForgetJob(Arc<Custody>);

impl ContextForgetOwner {
    pub(crate) fn from_admitted(
        root: &ScopeHandle,
        context: QueryContextRef,
        notice: ContextReadyNotice,
        host: Arc<dyn crate::TaskExecutionHost>,
        observer: ConsumerId,
        original: ReportedWork,
    ) -> Result<(Self, ContextForgetJob), ContextForgetRefusal> {
        let checked = (|| -> Result<(), ContextForgetError> {
            if !original.work().scope().same_scope(root) || notice.context() != context {
                return Err(ContextForgetError::WrongOwner);
            }
            if original.work().kind()? != WorkKind::Cleanup {
                return Err(ContextForgetError::WrongOwner);
            }
            let expected = OwnedSlotNeed {
                work_headers: 1,
                completions: 1,
                captures: 1,
                ..OwnedSlotNeed::default()
            };
            if original
                .bound_storage_profile()
                .map_err(ContextForgetError::StorageBinding)?
                != expected
            {
                return Err(ContextForgetError::WrongStorage);
            }
            if original.report().consumer_kind(observer)? != ConsumerKind::Observer
                || original.report().consumption(observer)? != Consumption::Pending
            {
                return Err(ContextForgetError::WrongObserver);
            }
            Ok(())
        })();
        if let Err(error) = checked {
            return Err(ContextForgetRefusal {
                error,
                context,
                notice,
                host,
                original,
            });
        }
        let (sender, reader) = match original.report().claim_owned_return() {
            Ok(pair) => pair,
            Err(error) => {
                return Err(ContextForgetRefusal {
                    error: error.into(),
                    context,
                    notice,
                    host,
                    original,
                });
            }
        };
        let key = (original.work().scope().id(), original.work().id());
        let retirement = original.work().retirement_observer();
        let report = original.report().clone();
        let (role, authority) = ExitRole::from_admitted(original);
        let custody = Arc::new(Custody {
            context,
            key,
            notice,
            observer,
            role,
            report,
            retirement,
            state: Mutex::new(State {
                host: Some(Box::new(ManuallyDrop::new(host))),
                authority: Some(authority),
                sender: Some(sender),
                reader,
                started: false,
                panic: None,
                error: None,
            }),
            retained: Mutex::new(None),
        });
        *custody
            .retained
            .lock()
            .expect("original Context forget retention") = Some(custody.clone());
        Ok((Self(custody.clone()), ContextForgetJob(custody)))
    }
    fn poll_locked(state: &mut State) -> Result<bool, ContextForgetError> {
        if let Some(error) = state.error {
            return Err(error);
        }
        state.reader.try_take()?;
        Ok(state.reader.state()? == ReturnState::Taken)
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.0.role.phase() == ExitRolePhase::Retired && self.0.retirement.is_retired()
    }
    /// Only this original return's observer is consumed. The actual Root
    /// ledger separately reaps ready headers; this owner never guesses its
    /// ledger from a numeric Scope or the removed execution Scope's notice.
    pub(crate) fn try_retire(&self) -> Result<bool, ContextForgetError> {
        // Serialize observers of this same owner so two visits cannot race
        // the original consumer acknowledgement or report retirement.
        let mut state = self
            .0
            .state
            .lock()
            .expect("original Context forget retirement");
        if !Self::poll_locked(&mut state)? {
            return Ok(false);
        }
        if self.0.role.phase() != ExitRolePhase::Retired {
            if !self.0.role.actual_exit_ready()? {
                return Ok(false);
            }
            match self.0.report.consumption(self.0.observer)? {
                Consumption::Pending => self.0.report.consume(self.0.observer)?,
                Consumption::Consumed => (),
                Consumption::AbandonedByOwnerFailure => {
                    return Err(ContextForgetError::WrongObserver);
                }
            }
            if !self.0.role.try_retire()? {
                return Ok(false);
            }
        }
        if !self.is_retired() {
            return Ok(false);
        }
        drop(state);
        let retained = self
            .0
            .retained
            .lock()
            .expect("original Context forget retired")
            .take();
        drop(retained);
        Ok(true)
    }
}
impl ContextForgetJob {
    pub(crate) fn context(&self) -> QueryContextRef {
        self.0.context
    }
    pub(crate) fn queue_key(&self) -> (ScopeId, WorkId) {
        self.0.key
    }
    fn retain_unknown(
        &self,
        capture: HostCapture,
        authority: ExitAuthority,
        panic: Box<dyn Any + Send>,
    ) {
        let mut state = self.0.state.lock().expect("original Context forget panic");
        state.host = Some(capture);
        state.panic = Some(ManuallyDrop::new(panic));
        drop(state);
        // Loss of this actual holder records Unresolved. It never observes a
        // successful body/capture exit or fabricates a completion payload.
        drop(authority);
        self.0.notice.notify();
    }
    pub(crate) fn run(self) {
        let mut authority = {
            let mut state = self.0.state.lock().expect("original Context forget start");
            assert!(!state.started, "the original Context forget runs once");
            state.started = true;
            state
                .authority
                .take()
                .expect("same original forget authority")
        };
        if let Err(error) = authority.start() {
            self.0
                .state
                .lock()
                .expect("original forget start refusal")
                .error = Some(error.into());
            drop(authority);
            self.0.notice.notify();
            return;
        }
        let mut capture = self
            .0
            .state
            .lock()
            .expect("original Context forget capture")
            .host
            .take()
            .expect("same original execution host");
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| {
            capture.as_ref().forget_context_admission(self.0.context);
        })) {
            self.retain_unknown(capture, authority, panic);
            return;
        }
        let dropped = catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: this unique job took the stable original capture. It
            // attempts destruction once and retains the backing on unwind.
            unsafe { ManuallyDrop::drop(capture.as_mut()) };
        }));
        if let Err(panic) = dropped {
            self.retain_unknown(capture, authority, panic);
            return;
        }
        drop(capture);
        if let Err(error) = authority.exited() {
            self.0
                .state
                .lock()
                .expect("original Context forget exit refusal")
                .error = Some(error.into());
            self.0.notice.notify();
            return;
        }
        let sender = self
            .0
            .state
            .lock()
            .expect("original Context forget publication")
            .sender
            .take()
            .expect("same original forget sender");
        if let Err(refusal) = sender.publish(()) {
            let mut state = self
                .0
                .state
                .lock()
                .expect("original Context forget refused return");
            state.error = Some(refusal.error.into());
            state.sender = Some(refusal.sender);
        }
        self.0.notice.notify();
    }
}
impl Drop for ContextForgetJob {
    fn drop(&mut self) {
        let authority = {
            let mut state = self
                .0
                .state
                .lock()
                .expect("original Context forget job drop");
            if state.started {
                return;
            }
            // Losing a queued job is not cancellation proof. Retain the host
            // and return sender, and make this original loss non-retryable.
            state.started = true;
            state.authority.take()
        };
        drop(authority);
        self.0.notice.notify();
    }
}
