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

//! Stable action custody in the original registered statement, never a Work role.
//!
//! Design: ADR-0198 (docs/adr/ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md).

use super::*;

/// Immutable business completion facts selected by the original statement action.
/// These values grant no statement visibility, publication or protocol authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatementCommandCompletion {
    affected_rows: u64,
}
impl StatementCommandCompletion {
    pub const fn new(affected_rows: u64) -> Self {
        Self { affected_rows }
    }
    pub const fn affected_rows(&self) -> u64 {
        self.affected_rows
    }
}

/// An action cannot return a second governed owner through its value slot.
pub enum StatementActionValue {
    Rows(crate::api::ExecutionHandle),
    Local(crate::api::OwnedLocalResult),
    Ok(StatementCommandCompletion),
    SessionChanges(crate::session_effects::SessionEffects),
}
use crate::api::StatementExternalEffectDisposition;
use crate::session_error::QueryServiceError;
use futures::task::AtomicWaker;
use std::any::Any;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::{Context, Poll, Wake};

/// An original accepted responsibility after its business result is delivered.
/// Pending retains that exact responsibility; only actual retirement is Ready.
/// This source has no default timeout disposition and grants no new capacity.
pub trait GovernedStatementResidual: Send + 'static {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), crate::api::StatementNativeHealthFailure>>;
}

/// FA implements the business state machine. Polling may only borrow its same
/// stable inputs. Ready means all local action futures/jobs were actually
/// observed and their remaining custody was installed in an original receiver.
/// This interface grants neither execution nor a second root owner.
pub trait GovernedStatementAction: Send + 'static {
    /// Read only facts already retained by the original action. This must not
    /// poll a future, perform I/O, dispatch an operation, or grant local exit.
    fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        None
    }

    /// A stable local source could not prove actual exit. This is independent
    /// of a query-domain error and retains this same action without repolling.
    fn local_health_failure(&self) -> Option<crate::api::StatementNativeHealthFailure> {
        None
    }

    /// Silently install the actual residual before any business/action exit.
    /// The original synchronous loan restores this slot even if handoff panics.
    /// Implementations must leave an occupied target and their input unchanged.
    fn handoff_residual_in(
        &mut self,
        _target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), crate::api::StatementNativeHealthFailure> {
        Ok(())
    }

    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>>;
}

enum StatementActionFailureCause {
    Panic(Mutex<Box<dyn Any + Send>>),
    LocalHealth(crate::api::StatementNativeHealthFailure),
}

pub struct StatementActionFailure {
    primary: StatementActionFailureCause,
    external_effect: AtomicU8,
}
impl StatementActionFailure {
    pub fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        match self.external_effect.load(Ordering::Acquire) {
            0 => None,
            1 => Some(StatementExternalEffectDisposition::KnownUncommitted),
            2 => Some(StatementExternalEffectDisposition::CommitUnknown),
            3 => Some(StatementExternalEffectDisposition::KnownCommitted),
            _ => unreachable!("closed external effect disposition"),
        }
    }

    pub fn panic_payload(&self) -> Option<std::sync::MutexGuard<'_, Box<dyn Any + Send>>> {
        match &self.primary {
            StatementActionFailureCause::Panic(payload) => {
                Some(payload.lock().unwrap_or_else(|p| p.into_inner()))
            }
            StatementActionFailureCause::LocalHealth(_) => None,
        }
    }
    pub fn health(&self) -> Option<&crate::api::StatementNativeHealthFailure> {
        match &self.primary {
            StatementActionFailureCause::Panic(_) => None,
            StatementActionFailureCause::LocalHealth(health) => Some(health),
        }
    }
}
impl std::fmt::Debug for StatementActionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StatementActionFailure(..)")
    }
}

/// An observer never owns the only statement/action. Its loss requests cancel;
/// the original supervisor continues polling the same source.
pub struct StatementActionWaiter {
    source: Arc<StatementActionSource>,
    service: QueryControlService,
    completed: bool,
}
#[derive(Debug)]
pub enum StatementActionWaitError {
    Failed(Arc<StatementActionFailure>),
    /// The original stopped source actually disposed its local action/result
    /// and settled its original protocol owner. This is not an absence test.
    Stopped {
        external_effect: Option<StatementExternalEffectDisposition>,
    },
}
pub struct StatementActionReady {
    pub result: Result<StatementActionValue, QueryServiceError>,
    pub owner: GovernedQueryStatementOwner,
}
impl StatementActionReady {
    /// Validate before either move. Refusal leaves both stable slots unchanged.
    /// Success performs only moves and closed constructors, with no callback.
    pub fn transfer_protocol_in(
        source: &mut Option<Self>,
        target: &mut Option<crate::protocol_delivery::QuerySessionOutput>,
    ) -> Result<(), &'static str> {
        use crate::protocol_delivery::*;
        if target.is_some() {
            return Err("protocol destination is occupied");
        }
        let ready = source.as_ref().ok_or("statement receiver is empty")?;
        if let Ok(StatementActionValue::Local(result)) = &ready.result
            && !result.is_for_scope(ready.owner.scope())
        {
            return Err("Local result belongs to another statement");
        }
        if let Ok(StatementActionValue::Rows(execution)) = &ready.result
            && !execution.has_rows_output()
        {
            return Err("ordinary Read has no original row output");
        }
        let ready = source.take().expect("checked original receiver");
        *target = Some(match ready.result {
            Ok(StatementActionValue::Rows(execution)) => {
                QuerySessionOutput::StreamingQuery(Box::new(
                    StreamingStatementResult::from_verified_execution(execution, ready.owner),
                ))
            }
            Ok(StatementActionValue::Local(result)) => QuerySessionOutput::GovernedQuery(
                GovernedImmediateStatementResult::from_verified_owned_local(result, ready.owner),
            ),
            Ok(StatementActionValue::SessionChanges(effects)) => {
                QuerySessionOutput::GovernedCompletion(
                    GovernedCompletionStatementResult::with_session_effects(ready.owner, effects),
                )
            }
            Ok(StatementActionValue::Ok(completion)) => QuerySessionOutput::GovernedCompletion(
                GovernedCompletionStatementResult::new(completion, ready.owner),
            ),
            Err(error) => QuerySessionOutput::GovernedError(GovernedErrorStatementResult::new(
                error,
                ready.owner,
            )),
        });
        Ok(())
    }
}
/// Move-only observation of Ready. The original source STILL owns both the
/// result and owner until silent installation in a real stable destination.
pub struct StatementActionReceipt {
    source: Arc<StatementActionSource>,
    service: QueryControlService,
    transferred: bool,
}
impl StatementActionReceipt {
    pub fn take_ready_in(
        mut self,
        destination: &mut Option<StatementActionReady>,
    ) -> Result<StatementActionAttempt, Self> {
        if destination.is_some() {
            return Err(self);
        }
        let root = self
            .source
            .wake
            .root
            .upgrade()
            .expect("original supervisor retains every source");
        {
            let mut state = self.source.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.closed
                || state.business_failed
                || state.settled
                || state.owner.is_none()
                || state.result.is_none()
            {
                drop(state);
                return Err(self);
            }
            let mut owner = state
                .owner
                .take()
                .expect("checked original owner")
                .into_sql();
            owner.restore_control_link(&self.service);
            let result = state.result.take().expect("checked original result");
            state.delivered = true;
            *destination = Some(StatementActionReady { result, owner });
        }
        assert!(
            self.service.port.detach_statement_action(
                destination.as_ref().expect("stable receiver").owner.token(),
                &self.source
            ),
            "same original statement source"
        );
        self.transferred = true;
        self.source.retire_if_possible();
        Ok(StatementActionAttempt {
            result: Ok(()),
            root,
            pending: true,
        })
    }
}
impl Drop for StatementActionReceipt {
    fn drop(&mut self) {
        if !self.transferred {
            self.source.abandon();
        }
    }
}
impl std::future::Future for StatementActionWaiter {
    type Output = Result<StatementActionReceipt, StatementActionWaitError>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.completed, "a statement observer is consumed once");
        let source = Arc::clone(&self.source);
        source.waiter.register(cx.waker());
        {
            let state = source.state.lock().unwrap_or_else(|p| p.into_inner());
            // The original poll loan must restore its retained facts before
            // an observer can expose a failure caused by that same poll.
            if state.polling {
                return Poll::Pending;
            }
            if state.business_failed {
                return Poll::Ready(Err(StatementActionWaitError::Failed(Arc::clone(
                    state
                        .failure
                        .as_ref()
                        .expect("business failure retains its cause"),
                ))));
            }
            if state.settled {
                return Poll::Ready(Err(StatementActionWaitError::Stopped {
                    external_effect: state.external_effect,
                }));
            }
            if !state.closed {
                return Poll::Pending;
            }
        }
        self.completed = true;
        Poll::Ready(Ok(StatementActionReceipt {
            source,
            service: self.service.clone(),
            transferred: false,
        }))
    }
}

impl Drop for StatementActionWaiter {
    fn drop(&mut self) {
        if !self.completed {
            self.source.abandon();
        }
    }
}

enum ActionOwner {
    Sql(GovernedQueryStatementOwner),
    Background(super::background_source::BackgroundActionCustody),
}
impl ActionOwner {
    fn sql_mut(&mut self) -> &mut GovernedQueryStatementOwner {
        match self {
            Self::Sql(owner) => owner,
            Self::Background(_) => panic!("SQL facade requires its original SQL owner"),
        }
    }
    fn into_sql(self) -> GovernedQueryStatementOwner {
        match self {
            Self::Sql(owner) => owner,
            Self::Background(_) => panic!("SQL receipt requires its original SQL owner"),
        }
    }
    fn is_background(&self) -> bool {
        matches!(self, Self::Background(_))
    }
    fn execution_owner_slot(&mut self) -> &mut Option<WorkOwner> {
        match self {
            Self::Sql(owner) => owner.execution_owner_slot(),
            Self::Background(owner) => &mut owner.owner,
        }
    }
}
enum ActionBody {
    Sql(Box<dyn GovernedStatementAction>),
    Background(Box<dyn super::background_source::GovernedBackgroundAction>),
}
impl ActionBody {
    fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        match self {
            Self::Sql(a) => a.external_effect_disposition(),
            Self::Background(a) => a.external_effect_disposition(),
        }
    }
    fn local_health_failure(&self) -> Option<crate::api::StatementNativeHealthFailure> {
        match self {
            Self::Sql(a) => a.local_health_failure(),
            Self::Background(a) => a.local_health_failure(),
        }
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), crate::api::StatementNativeHealthFailure> {
        match self {
            Self::Sql(a) => a.handoff_residual_in(target),
            Self::Background(a) => a.handoff_residual_in(target),
        }
    }
    fn poll_action(
        &mut self,
        owner: &mut ActionOwner,
        source: &Arc<StatementActionSource>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        match (self, owner) {
            (Self::Sql(action), ActionOwner::Sql(owner)) => {
                let window = owner.result_window_alias();
                let mut context = owner.registered_action_context(window.as_ref(), source);
                action.poll_action(&mut context, cx)
            }
            (Self::Background(action), ActionOwner::Background(owner)) => {
                owner.poll(action.as_mut(), cx).map(|result| {
                    // Background actions return no SQL mutation count.
                    result.map(|()| StatementActionValue::Ok(StatementCommandCompletion::new(0)))
                })
            }
            _ => panic!("same original action and owner kind"),
        }
    }
}
#[derive(Clone)]
enum ActionCancellation {
    Sql(GovernedStatementCancellation),
    Background(novarocks_workload_control::WorkCancellationRequester),
}
impl ActionCancellation {
    fn requester(&self) -> &novarocks_workload_control::WorkCancellationRequester {
        match self {
            Self::Sql(c) => c.requester(),
            Self::Background(c) => c,
        }
    }
}

struct StatementActionState {
    // Passive result backing drops before its original owner/window/payload.
    result: Option<Result<StatementActionValue, QueryServiceError>>,
    owner: Option<ActionOwner>,
    action: Option<ActionBody>,
    residual: Option<Box<dyn GovernedStatementResidual>>,
    residual_work: Option<WorkOwner>,
    retirement_complete: bool,
    delivered: bool,
    cancellation: Option<ActionCancellation>,
    failure: Option<Arc<StatementActionFailure>>,
    // Retirement health retains the source without vetoing completed business.
    business_failed: bool,
    secondary: Option<Arc<StatementActionFailure>>,
    external_effect: Option<StatementExternalEffectDisposition>,
    settled: bool,
    polling: bool,
    closed: bool,
}
#[doc(hidden)]
pub struct StatementActionSource {
    state: Mutex<StatementActionState>,
    waiter: AtomicWaker,
    abandoned: AtomicBool,
    cancel_requested: AtomicBool,
    wake: Arc<StatementActionWake>,
}
struct StatementActionWake {
    root: Weak<StatementProgressSlot>,
    background: AtomicWaker,
}
impl Wake for StatementActionWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(root) = self.root.upgrade() {
            root.notify();
        }
        self.background.wake();
    }
}
impl StatementActionSource {
    fn empty(root: &Arc<StatementProgressSlot>) -> Arc<Self> {
        Self::empty_with_root(Arc::downgrade(root))
    }
    fn empty_with_root(root: Weak<StatementProgressSlot>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(StatementActionState {
                owner: None,
                action: None,
                residual: None,
                residual_work: None,
                retirement_complete: true,
                delivered: false,
                cancellation: None,
                result: None,
                failure: None,
                business_failed: false,
                secondary: None,
                external_effect: None,
                settled: false,
                polling: false,
                closed: false,
            }),
            waiter: AtomicWaker::new(),
            abandoned: AtomicBool::new(false),
            cancel_requested: AtomicBool::new(false),
            wake: Arc::new(StatementActionWake {
                root,
                background: AtomicWaker::new(),
            }),
        })
    }
    pub(crate) fn install(
        &self,
        owner: &mut Option<GovernedQueryStatementOwner>,
        action: &mut Option<Box<dyn GovernedStatementAction>>,
        cancellation: &GovernedStatementCancellation,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(state.owner.is_none() && state.action.is_none());
        // The caller still borrows the strong service. Demotion is a pure
        // authority-preserving link change, never an owner reconstruction.
        owner
            .as_mut()
            .expect("checked original owner")
            .park_control_link();
        state.owner = owner.take().map(ActionOwner::Sql);
        state.action = action.take().map(ActionBody::Sql);
        state.cancellation = Some(ActionCancellation::Sql(cancellation.clone()));
    }
    fn abandon(&self) {
        self.abandoned.store(true, Ordering::Release);
        if let Err(primary) = catch_unwind(AssertUnwindSafe(|| {
            self.request_cancel(CancellationReason::OwnerDropped);
            self.notify_root();
        })) {
            self.record_failure(primary);
        }
    }
    pub(crate) fn failure(&self) -> Option<Arc<StatementActionFailure>> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .failure
            .clone()
    }
    #[cfg(test)]
    pub(crate) fn dispose_poisoned_for_test(self: &Arc<Self>, service: &QueryControlService) {
        let (action, owner, primary, secondary, residual, residual_work, settled) = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            (
                state.action.take(),
                state.owner.take(),
                state.failure.take(),
                state.secondary.take(),
                state.residual.take(),
                state.residual_work.take(),
                state.settled,
            )
        };
        drop(action);
        drop(residual);
        if let Some(work) = residual_work {
            work.complete_after_terminal_cancel_settled();
        }
        if let Some(owner) = owner {
            let mut owner = owner.into_sql();
            owner.restore_control_link(service);
            if !settled {
                assert!(service.port.detach_statement_action(owner.token(), self));
                owner.protocol_fail();
            } else {
                drop(owner);
            }
        }
        drop((primary, secondary));
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.closed = true;
            state.settled = true;
            state.retirement_complete = true;
        }
        self.retire_if_possible();
    }
    fn record_failure(&self, primary: Box<dyn Any + Send>) {
        self.record_cause(StatementActionFailureCause::Panic(Mutex::new(primary)));
    }
    fn record_cause(&self, primary: StatementActionFailureCause) {
        self.record_cause_in(primary, true);
    }
    fn record_retirement_cause(&self, primary: StatementActionFailureCause) {
        self.record_cause_in(primary, false);
    }
    fn record_cause_in(&self, primary: StatementActionFailureCause, blocks_business: bool) {
        let packet = Arc::new(StatementActionFailure {
            primary,
            external_effect: AtomicU8::new(0),
        });
        // The actual packet's raw cause is already retained. Initialize its
        // one quoted platform backing before sharing the packet with observers.
        drop(packet.panic_payload());
        let overflow = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            packet.external_effect.store(
                external_effect_code(state.external_effect),
                Ordering::Release,
            );
            state.business_failed |= blocks_business;
            if state.failure.is_none() {
                state.failure = Some(packet);
                None
            } else if state.secondary.is_none() {
                state.secondary = Some(packet);
                None
            } else {
                Some(packet)
            }
        };
        // A failure beyond the fixed source-local slots goes to the SAME
        // original root panic packet,
        // never ordinary opaque-cause destruction under this metadata lock.
        if let Some(packet) = overflow {
            std::panic::panic_any(packet);
        }
    }
    fn retain_local_health(&self, action: &ActionBody) -> bool {
        // Borrow foreign metadata outside State. The original action remains
        // in its restoration loan on panic and on an actual-exit refusal.
        match catch_unwind(AssertUnwindSafe(|| action.local_health_failure())) {
            Ok(None) => true,
            Ok(Some(failure)) => {
                self.record_cause(StatementActionFailureCause::LocalHealth(failure));
                false
            }
            Err(primary) => {
                self.record_failure(primary);
                false
            }
        }
    }
    fn retain_external_effect(&self, action: &ActionBody) -> bool {
        // Foreign code runs outside the metadata lock, while the original
        // action/owner loan and its first raw failure remain retained.
        let fact = match catch_unwind(AssertUnwindSafe(|| action.external_effect_disposition())) {
            Ok(fact) => fact,
            Err(primary) => {
                self.record_failure(primary);
                return false;
            }
        };
        if let Some(fact) = fact {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.external_effect = Some(fact);
            for packet in [&state.failure, &state.secondary].into_iter().flatten() {
                packet
                    .external_effect
                    .store(external_effect_code(Some(fact)), Ordering::Release);
            }
        }
        true
    }
    fn notify_root(&self) {
        self.wake.wake_by_ref();
    }
    fn request_cancel(&self, reason: CancellationReason) {
        // A source exit cannot repeatedly manufacture cancellation progress.
        // Publish this one-shot cut before any real WC callback can unwind.
        if self.cancel_requested.swap(true, Ordering::AcqRel) {
            return;
        }

        let cancellation = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancellation
            .clone();
        if let Some(cancellation) = cancellation {
            let _ = cancellation.requester().request(reason);
        }
    }
    pub(crate) fn stop(&self) {
        self.abandoned.store(true, Ordering::Release);
    }
    fn poll(self: &Arc<Self>) {
        if self.failure().is_some() {
            // Completed business may still settle its protocol after an
            // independent retirement failure. Neither action nor residual is repolled.
            self.finish_abandoned();
            return;
        }
        if self.abandoned.load(Ordering::Acquire)
            && !self
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .delivered
        {
            self.request_cancel(CancellationReason::OwnerDropped);
        }
        let Some(mut loan) = StatementPollLoan::take(Arc::clone(self)) else {
            self.poll_retirement();
            self.finish_abandoned();
            self.retire_if_possible();
            return;
        };
        let waker = std::task::Waker::from(Arc::clone(&self.wake));
        let mut cx = Context::from_waker(&waker);
        let result = catch_unwind(AssertUnwindSafe(|| {
            loan.action
                .as_mut()
                .unwrap()
                .poll_action(loan.owner.as_mut().unwrap(), self, &mut cx)
        }));
        match result {
            Ok(Poll::Pending) => {
                let healthy =
                    self.retain_local_health(loan.action.as_ref().expect("same original action"));
                if !healthy {
                    self.retain_external_effect(
                        loan.action.as_ref().expect("same original action"),
                    );
                }
                drop(loan);
                if !healthy {
                    self.waiter.wake();
                }
            }
            Err(primary) => {
                // The same original cause becomes stable BEFORE any action/F
                // cleanup. The poisoned action is retained and never repolled.
                self.record_failure(primary);
                self.retain_external_effect(loan.action.as_ref().expect("same original action"));
                drop(loan);
                self.waiter.wake();
            }
            Ok(Poll::Ready(result)) => {
                {
                    let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    state.result = Some(result);
                }
                // A close failure preserves the whole action/owner even when
                // its safe business result is Ready. It is not a fake panic.
                if !self.retain_local_health(loan.action.as_ref().expect("same original action")) {
                    self.retain_external_effect(
                        loan.action.as_ref().expect("same original action"),
                    );
                    drop(loan);
                    self.waiter.wake();
                    return;
                }
                // The value is stable before reading the original effect fact,
                // which must survive a subsequent action destructor panic.
                if !self.retain_external_effect(loan.action.as_ref().expect("same original action"))
                {
                    drop(loan);
                    self.waiter.wake();
                    return;
                }
                let handoff = catch_unwind(AssertUnwindSafe(|| {
                    loan.action
                        .as_mut()
                        .expect("same original action")
                        .handoff_residual_in(&mut loan.residual)
                }));
                match handoff {
                    Ok(Ok(())) => {}
                    Ok(Err(failure)) => {
                        self.record_cause(StatementActionFailureCause::LocalHealth(failure));
                        drop(loan);
                        self.waiter.wake();
                        return;
                    }
                    Err(primary) => {
                        self.record_failure(primary);
                        drop(loan);
                        self.waiter.wake();
                        return;
                    }
                }
                if loan.residual.is_some() {
                    let work = loan
                        .owner
                        .as_mut()
                        .expect("same protocol owner")
                        .execution_owner_slot();
                    let Some(original) = work.take() else {
                        self.record_cause(StatementActionFailureCause::LocalHealth(
                            crate::api::StatementNativeHealthFailure::actual_exit_unproven(
                                "statement residual has no original execution owner",
                                Arc::new(WorkError::Conflict),
                            ),
                        ));
                        drop(loan);
                        self.waiter.wake();
                        return;
                    };
                    loan.residual_work = Some(original);
                }
                let action = loan.action.take();
                // Restore owner to its stable source before arbitrary Drop.
                drop(loan);
                let cleanup = catch_unwind(AssertUnwindSafe(|| drop(action)));
                match cleanup {
                    Ok(()) => {
                        // The residual is stable and the action actually exited.
                        // Release query concurrency at business terminal, without
                        // dropping the original Root or waiting for unrelated GC.
                        let query_permit = {
                            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                            match state.owner.as_mut() {
                                Some(ActionOwner::Background(owner)) => {
                                    owner.take_business_query_permit()
                                }
                                _ => None,
                            }
                        };
                        match catch_unwind(AssertUnwindSafe(|| drop(query_permit))) {
                            Ok(()) => {
                                self.state.lock().unwrap_or_else(|p| p.into_inner()).closed = true
                            }
                            Err(primary) => self.record_failure(primary),
                        }
                    }
                    Err(primary) => self.record_failure(primary),
                }
                let has_residual = {
                    let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    state.residual.is_some()
                };
                if has_residual {
                    // Accepted custody is one real progress event. Register
                    // retirement even when no UI observer takes the result.
                    self.notify_root();
                }
                self.waiter.wake();
                self.finish_abandoned();
                self.retire_if_possible();
            }
        }
    }
    fn poll_retirement(self: &Arc<Self>) {
        let Some(mut loan) = StatementRetirementLoan::take(Arc::clone(self)) else {
            return;
        };
        let waker = std::task::Waker::from(Arc::clone(&self.wake));
        let mut cx = Context::from_waker(&waker);
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            loan.residual
                .as_mut()
                .expect("same original residual")
                .poll_retirement(&mut cx)
        }));
        match outcome {
            Ok(Poll::Pending) => {
                // The original source alone supplies the next real progress event.
                drop(loan);
            }
            Ok(Poll::Ready(Err(failure))) => {
                self.record_retirement_cause(StatementActionFailureCause::LocalHealth(failure));
                drop(loan);
                self.waiter.wake();
            }
            Err(primary) => {
                self.record_retirement_cause(StatementActionFailureCause::Panic(Mutex::new(
                    primary,
                )));
                drop(loan);
                self.waiter.wake();
            }
            Ok(Poll::Ready(Ok(()))) => {
                // Restore original Work before foreign Drop. Failed destruction
                // retains its actual root, and the poisoned residual is not repolled.
                let residual = loan.residual.take();
                drop(loan);
                if let Err(primary) = catch_unwind(AssertUnwindSafe(|| drop(residual))) {
                    self.record_retirement_cause(StatementActionFailureCause::Panic(Mutex::new(
                        primary,
                    )));
                    self.waiter.wake();
                    return;
                }
                let background_restored = {
                    let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    if matches!(state.owner, Some(ActionOwner::Background(_))) {
                        let work = state.residual_work.take();
                        if let Some(ActionOwner::Background(owner)) = state.owner.as_mut() {
                            assert!(owner.owner.is_none());
                            owner.owner = work;
                            if owner.owner.is_some() {
                                state.retirement_complete = true;
                                true
                            } else {
                                false
                            }
                        } else {
                            unreachable!()
                        }
                    } else {
                        false
                    }
                };
                if background_restored {
                    self.waiter.wake();
                    return;
                }
                let work = self
                    .state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .residual_work
                    .take();
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    work.expect("accepted residual retains one original root")
                        .complete_after_terminal_cancel_settled();
                }));
                match outcome {
                    Ok(()) => {
                        self.state
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .retirement_complete = true
                    }
                    Err(primary) => self.record_retirement_cause(
                        StatementActionFailureCause::Panic(Mutex::new(primary)),
                    ),
                }
                self.waiter.wake();
            }
        }
    }
    pub(crate) fn can_retire(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.closed
            && !state.polling
            && state.failure.is_none()
            && state.retirement_complete
            && state.residual.is_none()
            && state.residual_work.is_none()
            && (state.settled || (state.owner.is_none() && state.result.is_none()))
    }
    fn retire_if_possible(self: &Arc<Self>) {
        if !self.can_retire() {
            return;
        }
        if let Some(root) = self.wake.root.upgrade()
            && let Some(control) = root.control()
        {
            let _ = control.port.retire_statement_action_source(self);
        }
    }
    fn finish_abandoned(self: &Arc<Self>) {
        if !self.abandoned.load(Ordering::Acquire) {
            return;
        }
        let result = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.closed
                || state.settled
                || state.business_failed
                || state.polling
                || state.owner.is_none()
                || state.owner.as_ref().is_some_and(ActionOwner::is_background)
            {
                return;
            }
            state.result.take()
        };
        // The original owner remains in Source while result Drop runs. A
        // partial/destructive result exit does not become a successful close.
        if let Err(primary) = catch_unwind(AssertUnwindSafe(|| drop(result))) {
            self.record_failure(primary);
            return;
        }
        let Some(mut loan) = StatementCleanupLoan::take(Arc::clone(self)) else {
            return;
        };
        let token = loan
            .owner
            .as_mut()
            .expect("same original owner")
            .sql_mut()
            .token();
        let port = loan
            .owner
            .as_mut()
            .expect("same original owner")
            .sql_mut()
            .service
            .port();
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            loan.owner
                .as_mut()
                .expect("same original owner")
                .sql_mut()
                .cleanup_abandoned_action_in_place()
        }));
        if let Err(primary) = outcome {
            // Stable first cause BEFORE returning the same in-place owner.
            self.record_failure(primary);
            drop(loan);
            self.waiter.wake();
            return;
        }
        drop(loan);
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.business_failed {
                return;
            }
            state.settled = true;
        }
        // No callback/foreign Drop remains in this exact-source transaction.
        // A Source Arc is still held here while the original entry is removed.
        assert!(
            port.settle_statement_action_silent(token, self),
            "same actually settled original source"
        );
        self.waiter.wake();
    }
    pub(crate) fn empty_background() -> Arc<Self> {
        Self::empty_with_root(Weak::new())
    }
    pub(crate) fn install_background(
        &self,
        custody: super::background_source::BackgroundActionCustody,
        requester: novarocks_workload_control::WorkCancellationRequester,
        action: &mut Option<Box<dyn super::background_source::GovernedBackgroundAction>>,
    ) {
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            assert!(state.owner.is_none() && state.action.is_none());
            state.owner = Some(ActionOwner::Background(custody));
            state.action = action.take().map(ActionBody::Background);
            state.cancellation = Some(ActionCancellation::Background(requester));
        }
    }
    pub(crate) fn install_background_owner(&self, owner: &mut Option<WorkOwner>) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let Some(ActionOwner::Background(custody)) = state.owner.as_mut() else {
            panic!("original background custody is installed before its bank mount");
        };
        custody.install_owner(owner);
    }
    pub(crate) fn validate_background_owner(&self, owner: &WorkOwner) -> bool {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        matches!(&state.owner, Some(ActionOwner::Background(c))
            if c.owner.is_none() && c.cancellation().is_for_scope(&owner.scope()))
    }
    pub(crate) fn poll_background(
        self: &Arc<Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), crate::api::QueryExecutionError>> {
        self.wake.background.register(cx.waker());
        self.poll();
        if self.failure().is_some() {
            return Poll::Ready(Err(super::background_source::background_failed()));
        }
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed
            && !state.polling
            && state.retirement_complete
            && state.residual.is_none()
            && state.residual_work.is_none()
            && matches!(&state.owner, Some(ActionOwner::Background(_)))
        {
            // The original business receipt already retains an ordinary Err.
            // This role reports supervision health: clean action/residual exit
            // cannot poison the process supervisor for a domain failure.
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    pub(crate) fn finish_background_after_bank_exit(self: &Arc<Self>) {
        {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.closed
                || state.polling
                || state.failure.is_some()
                || !state.retirement_complete
                || state.residual.is_some()
                || state.residual_work.is_some()
                || !matches!(&state.owner, Some(ActionOwner::Background(c)) if !c.finished())
            {
                return;
            }
        }
        // Keep the canonical business result in this SAME Source through the
        // original bank footer; acknowledgement never destroys its raw error.
        let Some(mut loan) = StatementCleanupLoan::take(Arc::clone(self)) else {
            return;
        };
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let Some(ActionOwner::Background(custody)) = loan.owner.as_mut() else {
                panic!("same background owner cleanup loan");
            };
            custody.close();
        }));
        if let Err(primary) = outcome {
            // Same common first-cause slots precede restoration/foreign Drop.
            self.record_failure(primary);
        }
        drop(loan);
        // The original bank receiver publishes its footer before notifying.
        // No arbitrary receiver wake is permitted in this silent close cut.
    }
    pub(crate) fn background_actual_exit(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.failure.is_none()
            && !state.polling
            && state.closed
            && state.retirement_complete
            && state.residual.is_none()
            && state.residual_work.is_none()
            && matches!(&state.owner, Some(ActionOwner::Background(c)) if c.finished())
    }
    pub(crate) fn poll_background_ready(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Arc<StatementActionFailure>>> {
        self.waiter.register(cx.waker());
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.polling {
            return Poll::Pending;
        }
        if state.business_failed {
            return Poll::Ready(Err(Arc::clone(state.failure.as_ref().unwrap())));
        }
        if state.closed && state.result.is_some() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    pub(crate) fn try_background_result(&self) -> Option<BackgroundActionResultGuard<'_>> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        (state.closed && state.result.is_some()).then_some(BackgroundActionResultGuard { state })
    }
    pub(crate) fn background_result(&self) -> BackgroundActionResultGuard<'_> {
        BackgroundActionResultGuard {
            state: self.state.lock().unwrap_or_else(|p| p.into_inner()),
        }
    }
    pub(crate) fn acknowledge_background(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(state.closed && state.result.is_some() && !state.delivered);
        // Delivery is a pure acknowledgement. It does not wake or gate the
        // independent original execution/retirement path.
        state.delivered = true;
    }
    pub(crate) fn abandon_background(&self) {
        if !self.background_actual_exit() {
            self.abandon();
        }
    }
    pub(crate) fn stop_background(&self) {
        self.stop();
        self.notify_root();
    }
    pub(crate) fn background_geometry() -> super::background_source::BackgroundActionSourceGeometry
    {
        use novarocks_type_contract::owned_resources::layout::arc_layout;
        use std::alloc::Layout;
        super::background_source::BackgroundActionSourceGeometry {
            source: arc_layout(Layout::new::<Self>()).expect("actual common action Source"),
            wake: arc_layout(Layout::new::<StatementActionWake>())
                .expect("actual common action wake"),
            failure: arc_layout(Layout::new::<StatementActionFailure>())
                .expect("actual original failure"),
            // Exact Arc<str> allocation made by the same background_failed author.
            summary: arc_layout(
                Layout::array::<u8>(super::background_source::BACKGROUND_FAILURE_DETAIL.len())
                    .expect("actual background failure detail"),
            )
            .expect("actual background failure Arc"),
            ready: Layout::new::<super::background_source::BackgroundActionReady>(),
            receipt: Layout::new::<super::background_source::BackgroundActionReceipt>(),
            custody: Layout::new::<super::background_source::BackgroundActionCustody>(),
            mutex_backing: if cfg!(all(target_os = "macos", target_pointer_width = "64")) {
                Layout::new::<(isize, [u8; 56])>()
            } else if cfg!(target_os = "linux") {
                Layout::new::<()>()
            } else {
                panic!("unsupported common action Source Mutex backing")
            },
        }
    }
    pub(crate) fn is_settled(&self) -> bool {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).settled
    }
}
fn external_effect_code(fact: Option<StatementExternalEffectDisposition>) -> u8 {
    match fact {
        None => 0,
        Some(StatementExternalEffectDisposition::KnownUncommitted) => 1,
        Some(StatementExternalEffectDisposition::CommitUnknown) => 2,
        Some(StatementExternalEffectDisposition::KnownCommitted) => 3,
    }
}

// Same-source synchronous loan, not a new owner or an async guard. It restores
// the original owner on every unwind BEFORE the root can retain its packet.
struct StatementCleanupLoan {
    source: Arc<StatementActionSource>,
    owner: Option<ActionOwner>,
}
impl StatementCleanupLoan {
    fn take(source: Arc<StatementActionSource>) -> Option<Self> {
        let owner = {
            let mut state = source.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.polling || state.business_failed || state.owner.is_none() {
                return None;
            }
            state.polling = true;
            state.owner.take()
        };
        Some(Self { source, owner })
    }
}
impl Drop for StatementCleanupLoan {
    fn drop(&mut self) {
        let mut state = self.source.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(state.owner.is_none(), "one original cleanup loan");
        state.owner = self.owner.take();
        state.polling = false;
    }
}
struct StatementPollLoan {
    source: Arc<StatementActionSource>,
    action: Option<ActionBody>,
    owner: Option<ActionOwner>,
    residual: Option<Box<dyn GovernedStatementResidual>>,
    residual_work: Option<WorkOwner>,
}
impl StatementPollLoan {
    fn take(source: Arc<StatementActionSource>) -> Option<Self> {
        let (action, owner, residual, residual_work) = {
            let mut state = source.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.polling || state.closed || state.failure.is_some() || state.action.is_none() {
                return None;
            }
            state.polling = true;
            (
                state.action.take(),
                state.owner.take(),
                state.residual.take(),
                state.residual_work.take(),
            )
        };
        Some(Self {
            source,
            action,
            owner,
            residual,
            residual_work,
        })
    }
}
impl Drop for StatementPollLoan {
    fn drop(&mut self) {
        let mut state = self.source.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            state.owner.is_none() && state.action.is_none(),
            "same action loan returned once"
        );
        state.owner = self.owner.take();
        state.action = self.action.take();
        state.residual = self.residual.take();
        state.residual_work = self.residual_work.take();
        if state.residual.is_some() || state.residual_work.is_some() {
            state.retirement_complete = false;
        }
        state.polling = false;
    }
}

struct StatementRetirementLoan {
    source: Arc<StatementActionSource>,
    residual: Option<Box<dyn GovernedStatementResidual>>,
}
impl StatementRetirementLoan {
    fn take(source: Arc<StatementActionSource>) -> Option<Self> {
        let residual = {
            let mut state = source.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.closed || state.polling || state.failure.is_some() || state.residual.is_none()
            {
                return None;
            }
            state.polling = true;
            state.residual.take()
        };
        Some(Self { source, residual })
    }
}
impl Drop for StatementRetirementLoan {
    fn drop(&mut self) {
        let mut state = self.source.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(state.residual.is_none(), "one original retirement loan");
        state.residual = self.residual.take();
        state.polling = false;
    }
}

/// One process composition field, not a work registry or an execution role.
/// Its strong control alias is OUTSIDE Control/Registry. Control only stores a
/// Weak back-link; action futures may therefore use the original Registry.
#[doc(hidden)]
pub struct StatementProgressSlot {
    control: Mutex<Option<QueryControlService>>,
    accepting: AtomicBool,
    ready: AtomicBool,
    wake: AtomicWaker,
}
#[derive(Default)]
pub(crate) struct StatementProgressCursor {
    after: Option<usize>,
    active: bool,
}
impl StatementProgressSlot {
    pub(crate) fn empty() -> Arc<Self> {
        Arc::new(Self {
            control: Mutex::new(None),
            accepting: AtomicBool::new(true),
            ready: AtomicBool::new(false),
            wake: AtomicWaker::new(),
        })
    }
    pub(crate) fn install(
        self: &Arc<Self>,
        control: QueryControlService,
        source_slots: NonZeroUsize,
    ) -> Result<(), QueryControlError> {
        // Install is a serial pre-Ready composition step, not a hot replacement.
        if !self.accepting.load(Ordering::Acquire) {
            return Err(QueryControlError::StatementBusy);
        }
        let mut slot = self.control.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Err(QueryControlError::StatementBusy);
        }
        control.port.attach_statement_progress(self, source_slots)?;
        *slot = Some(control);
        drop(slot);
        self.notify();
        Ok(())
    }
    fn control(&self) -> Option<QueryControlService> {
        self.control
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    fn notify(&self) {
        self.ready.store(true, Ordering::Release);
        self.wake.wake();
    }
    pub(crate) fn close(&self) {
        self.accepting.store(false, Ordering::Release);
        if let Some(control) = self.control() {
            control.port.close_statement_actions();
        }
        self.notify();
    }
    pub(crate) fn failure(&self) -> Option<Arc<StatementActionFailure>> {
        self.control()
            .and_then(|control| control.port.statement_action_failure())
    }
    pub(crate) fn has_sources(&self) -> bool {
        self.control()
            .is_some_and(|control| control.port.has_statement_actions())
    }
    pub(crate) fn poll_turn(
        &self,
        cursor: &mut StatementProgressCursor,
        budget: NonZeroUsize,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        self.wake.register(cx.waker());
        if !cursor.active {
            if !self.ready.swap(false, Ordering::AcqRel) {
                return Poll::Pending;
            }
            cursor.active = true;
            cursor.after = None;
        }
        let Some(control) = self.control() else {
            cursor.active = false;
            return Poll::Pending;
        };
        for _ in 0..budget.get() {
            let Some((key, source)) = control.port.next_statement_action(cursor.after) else {
                cursor.active = false;
                cursor.after = None;
                return Poll::Ready(());
            };
            cursor.after = Some(key);
            if let Some(source) = source {
                source.poll();
            }
        }
        // Preserve the finite scan frontier. No event/empty prefix is treated
        // as evidence that later original sessions have no runnable action.
        cx.waker().wake_by_ref();
        Poll::Ready(())
    }
}

#[must_use = "finish or drop only outside the publisher's locks"]
pub struct StatementActionAttempt {
    result: Result<(), QueryControlError>,
    root: Arc<StatementProgressSlot>,
    pending: bool,
}
impl StatementActionAttempt {
    pub fn result(&self) -> &Result<(), QueryControlError> {
        &self.result
    }
    pub fn finish(mut self) -> Result<(), QueryControlError> {
        self.notify();
        self.result.clone()
    }
    fn notify(&mut self) {
        if self.pending {
            self.pending = false;
            self.root.notify();
        }
    }
}
impl Drop for StatementActionAttempt {
    fn drop(&mut self) {
        self.notify();
    }
}
impl QueryControlService {
    pub fn publish_statement_action_in(
        &self,
        owner: &mut Option<GovernedQueryStatementOwner>,
        action: &mut Option<Box<dyn GovernedStatementAction>>,
        output: &mut Option<StatementActionWaiter>,
    ) -> Result<StatementActionAttempt, QueryControlError> {
        if owner.is_none() || action.is_none() || output.is_some() {
            return Err(QueryControlError::StatementBusy);
        }
        if !Arc::ptr_eq(
            &owner.as_ref().expect("checked owner").service.port(),
            &self.port,
        ) {
            return Err(QueryControlError::StaleSession);
        }
        let root = self
            .port
            .statement_progress_slot()
            .ok_or(QueryControlError::UnknownSession)?;
        let source = StatementActionSource::empty(&root);
        let result = self
            .port
            .publish_statement_action_in(owner, action, &source);
        if result.is_ok() {
            *output = Some(StatementActionWaiter {
                source: Arc::clone(&source),
                service: self.clone(),
                completed: false,
            });
        }
        let pending = result.is_ok();
        Ok(StatementActionAttempt {
            result,
            root,
            pending,
        })
    }
    pub fn statement_action_geometry() -> StatementProgressGeometry {
        StatementProgressGeometry {
            registry_record: std::alloc::Layout::new::<Option<Arc<StatementActionSource>>>(),
            source: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<StatementActionSource>(),
            )
            .expect("fixed source layout"),
            wake: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<StatementActionWake>(),
            )
            .expect("fixed wake layout"),
            driver: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<StatementProgressSlot>(),
            )
            .expect("fixed statement driver layout"),
            observer: std::alloc::Layout::new::<StatementActionWaiter>(),
            receipt: std::alloc::Layout::new::<StatementActionReceipt>(),
            failure: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<StatementActionFailure>(),
            )
            .expect("fixed source failure layout"),
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct StatementProgressGeometry {
    pub registry_record: std::alloc::Layout,
    pub source: std::alloc::Layout,
    pub wake: std::alloc::Layout,
    pub driver: std::alloc::Layout,
    pub observer: std::alloc::Layout,
    pub receipt: std::alloc::Layout,
    pub failure: std::alloc::Layout,
}

pub struct BackgroundActionResultGuard<'a> {
    state: std::sync::MutexGuard<'a, StatementActionState>,
}
impl BackgroundActionResultGuard<'_> {
    pub fn result(&self) -> super::background_source::BackgroundActionResultView<'_> {
        match self
            .state
            .result
            .as_ref()
            .expect("live original background business receipt")
        {
            Ok(StatementActionValue::Ok(_)) => {
                super::background_source::BackgroundActionResultView::Succeeded
            }
            Err(error) => super::background_source::BackgroundActionResultView::Failed(error),
            _ => unreachable!("closed Background action returns no SQL output"),
        }
    }
}
