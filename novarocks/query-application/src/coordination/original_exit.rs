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

//! The original role's fixed receipt, not an execution or settlement authority.
use crate::api::logical_runtime::LogicalExit;
use crate::api::{QueryExecutionError, QueryExecutionErrorKind};
use novarocks_types::identity::QueryExecutionId;
use novarocks_workload_control::PollCompletion;
use std::{
    alloc::Layout,
    fmt,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task::{Context, Poll},
};

type RoleResult = PollCompletion<Result<(), QueryExecutionError>>;

// Original runtime exit is retained directly. There is no logical actor task
// JoinHandle and no synthesized Tokio JoinError for an isolated runtime scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalActorExitPhase {
    Unreserved,
    Reserved,
    Installed,
    Joined,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalExecutionDisposition {
    ActualSourcesExited,
    OriginalPaidOwnerReturn,
}
/// Attempt closures are not a whole-statement remote receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteExecutionDisposition {
    RemoteNotReported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct OriginalAssociation {
    pub registry: u64,
    pub initial: QueryExecutionId,
    pub generation: u64,
}
struct State {
    phase: LogicalActorExitPhase,
    association: Option<OriginalAssociation>,
    runtime: Option<LogicalExit>,
    pending_native_returns: u64,
    native_pre_actor_started: bool,
    native_body_exited: bool,
    runtime_installed: bool,
    native_failure: Option<Box<dyn std::any::Any + Send>>,
    native_body_cleanup_panic: Option<Box<dyn std::any::Any + Send>>,
    run_panic: Option<Box<dyn std::any::Any + Send>>,
    run_cleanup_panic: Option<Box<dyn std::any::Any + Send>>,
    owner_failure: Option<QueryExecutionError>,
    consumer_failed: bool,
    native_dormant_failed: bool,
    native_closure_seen: bool,
    native_return_disposition: Option<LocalExecutionDisposition>,
    role: Option<RoleResult>,
    role_ended: bool,
    role_failed: bool,
    health_published: bool,
    health_disposed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OriginalNativeBodyCloseRefusal {
    ForeignSource,
    NotRuntimeFailure,
    LocalWorkPending,
    BodyUnproven,
    BodyDropPanicked,
}
impl OriginalNativeBodyCloseRefusal {
    pub(super) const fn detail(self) -> &'static str {
        match self {
            Self::ForeignSource => "Native body cleanup belongs to another original source",
            Self::NotRuntimeFailure => {
                "Native body cleanup requires the original runtime owner failure"
            }
            Self::LocalWorkPending => "Native body cleanup still owns original local work",
            Self::BodyUnproven => "original Native body destruction remains unproven",
            Self::BodyDropPanicked => "original Native body destruction panicked",
        }
    }
}
pub(super) struct OriginalLogicalExit {
    state: Mutex<State>,
    pub(super) native: Arc<tokio::sync::Mutex<OriginalNativePreparation>>,
    pub(super) background: Option<Arc<crate::session_control::StatementActionSource>>,
    changed: futures::task::AtomicWaker,
    owner_failure_changed: futures::task::AtomicWaker,
}
impl fmt::Debug for OriginalLogicalExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OriginalLogicalExit")
    }
}
impl fmt::Display for OriginalLogicalExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original logical execution failed; original cause retained")
    }
}
impl std::error::Error for OriginalLogicalExit {}

impl OriginalLogicalExit {
    pub(super) fn new() -> Arc<Self> {
        let source = Arc::new(Self {
            state: Mutex::new(State {
                phase: LogicalActorExitPhase::Unreserved,
                association: None,
                runtime: None,
                pending_native_returns: 0,
                native_pre_actor_started: false,
                native_body_exited: true,
                runtime_installed: false,
                native_failure: None,
                native_body_cleanup_panic: None,
                run_panic: None,
                run_cleanup_panic: None,
                owner_failure: None,
                consumer_failed: false,
                native_dormant_failed: false,
                native_closure_seen: false,
                native_return_disposition: None,
                role: None,
                role_ended: false,
                role_failed: false,
                health_published: false,
                health_disposed: false,
            }),
            native: Arc::new(tokio::sync::Mutex::new(OriginalNativePreparation::default())),
            background: None,
            changed: futures::task::AtomicWaker::new(),
            owner_failure_changed: futures::task::AtomicWaker::new(),
        });
        // Initialize the one quoted platform backing before any alias can
        // race its lazy initialization through a newly published Source.
        drop(source.state.lock().unwrap_or_else(|p| p.into_inner()));
        source
    }
    pub(super) fn with_background(
        source: Arc<crate::session_control::StatementActionSource>,
    ) -> Arc<Self> {
        let mut original = Self::new();
        Arc::get_mut(&mut original).unwrap().background = Some(source);
        original
    }
    pub(super) fn with_native(
        request: crate::api::QueryExecutionRequest,
        port: Arc<dyn crate::api::LogicalExecutionNativePort>,
    ) -> Arc<Self> {
        let mut source = Self::new();
        let original = Arc::get_mut(&mut source).unwrap();
        original.state.get_mut().unwrap().native_body_exited = false;
        // Only this source owns the new Native body before publication.
        let body = Arc::get_mut(&mut original.native).unwrap().get_mut();
        body.start = Some(request);
        body.port = Some(port);
        source
    }
    /// Called only after the real role frame cleared. The private Native-body
    /// mutex serializes its own borrow; no Source/Registry/WC/health lock is
    /// held across a first-party destructor or callback.
    pub(super) fn retire_native_body_silent(&self) {
        {
            let state = self.lock();
            if !state.role_ended
                || state.native_body_exited
                || state.runtime_installed && state.phase != LogicalActorExitPhase::Joined
            {
                return;
            }
        }
        let role_failed = self.lock().role_failed;
        let Ok(mut body) = self.native.try_lock() else {
            return;
        };
        if body.dormant.has_failure() && !body.dormant.owner_retired_for_cleanup() {
            // The original two causes remain beside their original owner.
            // Publish only this failure fact, never copy into native_failure.
            drop(body);
            let mut state = self.lock();
            state.native_dormant_failed = true;
            state.role_failed = true;
            return;
        }
        if role_failed
            || body.exiting
            || body.pending_activated.is_some()
            || body
                .session
                .as_ref()
                .is_some_and(crate::api::LogicalNativeSession::preparation_drop_unproven)
            || body.dormant.drop_unproven()
        {
            return;
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body.close()));
        drop(body);
        let mut state = self.lock();
        match result {
            Ok(Ok(())) => {
                state.native_body_exited = true;
                state.native_pre_actor_started = false;
            }
            Ok(Err(())) => {
                state.native_dormant_failed = true;
                state.role_failed = true;
            }
            Err(original) => {
                // A failed real Drop is not a successful local return. This
                // fixed source keeps the original opaque cause for health.
                assert!(state.native_failure.is_none());
                state.native_failure = Some(original);
                state.role_failed = true;
            }
        }
    }

    /// The original failure job has already closed its SAME Registry ingress.
    /// Its local-exit qualifier runs under this original body loan, never a
    /// Registry/state lock. Only a real successful Drop records body exit;
    /// role/actor joins and retained health remain independent.
    pub(super) async fn close_runtime_native_body_for_cleanup<F>(
        &self,
        original_body: &Arc<tokio::sync::Mutex<OriginalNativePreparation>>,
        initial: QueryExecutionId,
        qualify_local_exit: F,
    ) -> Result<(), OriginalNativeBodyCloseRefusal>
    where
        F: FnOnce(&OriginalNativePreparation) -> Result<(), OriginalNativeBodyCloseRefusal> + Send,
    {
        use OriginalNativeBodyCloseRefusal as Refusal;
        if !Arc::ptr_eq(&self.native, original_body) {
            return Err(Refusal::ForeignSource);
        }
        let mut body = self.native.lock().await;
        {
            let state = self.lock();
            if !state
                .association
                .is_some_and(|original| original.initial == initial)
            {
                return Err(Refusal::ForeignSource);
            }
            if !state.runtime_installed
                || state.owner_failure.is_none()
                || state.phase != LogicalActorExitPhase::Installed
            {
                return Err(Refusal::NotRuntimeFailure);
            }
            if !state.role_ended || state.pending_native_returns != 0 {
                return Err(Refusal::LocalWorkPending);
            }
            if state.native_body_exited {
                return Ok(());
            }
            if state.native_body_cleanup_panic.is_some() {
                return Err(Refusal::BodyUnproven);
            }
        }
        if body.exiting
            || body.pending_activated.is_some()
            || body.dormant.has_owner_for_cleanup()
            || body.dormant.drop_unproven()
            || body
                .session
                .as_ref()
                .is_some_and(crate::api::LogicalNativeSession::preparation_drop_unproven)
        {
            return Err(Refusal::BodyUnproven);
        }
        qualify_local_exit(&body)?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body.close()));
        // The same body mutex remains borrowed until its close receipt is
        // stable, so another caller cannot turn an empty body into success.
        let mut state = self.lock();
        match result {
            Ok(Ok(())) => {
                state.native_body_exited = true;
                state.native_pre_actor_started = false;
            }
            Ok(Err(())) => return Err(Refusal::BodyUnproven),
            Err(original) => {
                // A separate fixed slot preserves an earlier native_failure.
                // body.exiting stays set and no later call retries its Drop.
                state.native_body_cleanup_panic = Some(original);
                return Err(Refusal::BodyDropPanicked);
            }
        }
        drop(state);
        drop(body);
        self.notify();
        Ok(())
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub(super) fn reserve_silent(&self, association: OriginalAssociation) {
        let mut s = self.lock();
        assert!(s.phase == LogicalActorExitPhase::Unreserved && s.association.is_none());
        s.association = Some(association);
        s.phase = LogicalActorExitPhase::Reserved;
    }
    pub(super) fn unreserve_silent(&self, association: OriginalAssociation) {
        let mut s = self.lock();
        assert_eq!(s.association, Some(association));
        assert_eq!(s.phase, LogicalActorExitPhase::Reserved);
        s.phase = LogicalActorExitPhase::Unreserved;
        s.association = None;
    }

    /// Commit the SAME source's install-only runtime affiliation. Ending
    /// its role frame cannot close the Native body still used by that actor.
    pub(super) fn runtime_installed_silent(&self, association: OriginalAssociation) {
        let mut s = self.lock();
        assert_eq!(s.association, Some(association));
        assert_eq!(s.phase, LogicalActorExitPhase::Reserved);
        s.runtime_installed = true;
        s.phase = LogicalActorExitPhase::Installed;
    }

    /// Passive original Registry fact. The install-only role ending does not
    /// close the consumer retained by this installed runtime.
    pub(super) fn logical_runtime_installed(&self) -> bool {
        self.lock().runtime_installed
    }
    pub(super) fn native_pre_actor_started_silent(&self) {
        self.lock().native_pre_actor_started = true;
    }
    pub(super) fn native_reserved_silent(&self) {
        let mut s = self.lock();
        s.pending_native_returns = s
            .pending_native_returns
            .checked_add(1)
            .expect("finite original Native reservations cannot overflow");
    }

    /// Called solely by the exact original ActiveSlot's successful remove.
    /// A replayed remote closure cannot invoke this once-return author.
    pub(super) fn native_return_silent(
        &self,
        key: super::NativeActiveCustodyKey,
        local: LocalExecutionDisposition,
    ) {
        let mut s = self.lock();
        let association = s
            .association
            .expect("original Native slot retains its source association");
        assert_eq!(association.initial, key.initial);
        assert_eq!(association.generation, key.generation);
        s.pending_native_returns = s
            .pending_native_returns
            .checked_sub(1)
            .expect("same original Native slot returns once");
        s.native_return_disposition = Some(match (s.native_return_disposition, local) {
            (Some(LocalExecutionDisposition::OriginalPaidOwnerReturn), _)
            | (_, LocalExecutionDisposition::OriginalPaidOwnerReturn) => {
                LocalExecutionDisposition::OriginalPaidOwnerReturn
            }
            _ => LocalExecutionDisposition::ActualSourcesExited,
        });
    }
    /// Only the original join author calls this after its exact native barrier.
    pub(super) fn runtime_joined_silent(
        &self,
        association: OriginalAssociation,
        result: crate::api::logical_runtime::LogicalExit,
    ) {
        let mut s = self.lock();
        assert_eq!(s.association, Some(association));
        assert_eq!(s.phase, LogicalActorExitPhase::Installed);
        assert!(s.runtime.is_none());
        s.runtime = Some(result);
        s.phase = LogicalActorExitPhase::Joined;
    }

    pub(super) fn notify(&self) {
        self.changed.wake();
    }
    pub(super) fn role_ended_silent(&self, failed: bool) {
        let mut s = self.lock();
        s.role_ended = true;
        s.role_failed |= failed;
    }
    pub(super) fn retain_local_consumer_failure_silent(
        &self,
        failure: crate::api::StatementNativeHealthFailure,
    ) {
        let original = Box::new(failure);
        let mut state = self.lock();
        assert!(
            state.native_failure.is_none(),
            "one original local close failure"
        );
        state.native_failure = Some(original);
        state.role_failed = true;
    }
    pub(super) fn health_published(&self) -> bool {
        self.lock().health_published
    }
    pub(super) fn mark_health_published_silent(&self) {
        self.lock().health_published = true;
    }
    /// Intake only: no foreign Drop, formatting, notification or authority.
    /// A run failure does not deny a later positive close of the same Active.
    pub(super) fn retain_run_panic_silent(
        &self,
        incoming: &mut Option<Box<dyn std::any::Any + Send>>,
        cleanup: bool,
    ) {
        let mut state = self.lock();
        let destination = if cleanup {
            &mut state.run_cleanup_panic
        } else {
            &mut state.run_panic
        };
        if destination.is_none() {
            *destination = incoming.take();
        }
    }
    pub(super) fn consumer_failed_silent(&self) {
        self.lock().consumer_failed = true;
    }
    /// Keep the original typed owner failure on the same outer statement/job
    /// source. This is an isolated owner verdict; role, Native and actor exits remain
    /// independently required by disposition().
    pub(super) fn retain_owner_failure_silent(&self, error: QueryExecutionError) -> bool {
        let mut state = self.lock();
        if state.owner_failure.is_some() {
            return false;
        }
        state.owner_failure = Some(error);
        true
    }
    pub(super) fn notify_owner_failure(&self) {
        self.owner_failure_changed.wake();
    }
    pub(super) fn owner_failure(&self) -> Option<QueryExecutionError> {
        self.lock().owner_failure.clone()
    }
    #[cfg(test)]
    pub(super) fn run_failed(&self) -> bool {
        let state = self.lock();
        state.run_panic.is_some() || state.run_cleanup_panic.is_some()
    }
    pub(super) fn has_unhandled_failure(&self) -> bool {
        if self
            .background
            .as_ref()
            .is_some_and(|source| source.failure().is_some())
        {
            return true;
        }
        let s = self.lock();
        !s.health_disposed
            && (s.role_failed
                || s.native_failure.is_some()
                || s.native_body_cleanup_panic.is_some()
                || s.run_panic.is_some()
                || s.run_cleanup_panic.is_some()
                || s.consumer_failed)
    }
    pub(super) fn retire_background_after_bank_exit_silent(&self, packet: &RoleResult) {
        let actual_frame_exit = match packet {
            PollCompletion::Ready(_) => true,
            PollCompletion::Panicked(original) => original.value().cleanup().is_none(),
        };
        if actual_frame_exit && let Some(source) = &self.background {
            source.finish_background_after_bank_exit();
        }
    }
    pub(super) fn receive_role_silent(&self, packet: &mut Option<RoleResult>) {
        let mut s = self.lock();
        assert!(
            s.role.is_none() && packet.is_some(),
            "one original role completion"
        );
        s.role_ended = true;
        s.role_failed |= matches!(packet, Some(PollCompletion::Panicked(_)));
        s.role = packet.take();
    }
    pub(super) fn has_failure(&self) -> bool {
        if self
            .background
            .as_ref()
            .is_some_and(|source| source.failure().is_some())
        {
            return true;
        }
        let s = self.lock();
        s.role_failed
            || s.owner_failure.is_some()
            || s.native_failure.is_some()
            || s.native_body_cleanup_panic.is_some()
            || s.run_panic.is_some()
            || s.run_cleanup_panic.is_some()
            || s.consumer_failed
            || s.runtime
                .as_ref()
                .is_some_and(|result| matches!(result, LogicalExit::OwnerFailed(_)))
    }
    /// Legacy role-owned actor cleanup only. A runtime install-only role may
    /// end while its actor is Running; RuntimeInstalled must use core readiness.
    pub(super) fn pending_actor(&self) -> bool {
        let s = self.lock();
        s.role_ended
            && !s.runtime_installed
            && matches!(
                s.phase,
                LogicalActorExitPhase::Reserved | LogicalActorExitPhase::Installed
            )
    }
    fn disposition(&self) -> Option<LocalExecutionDisposition> {
        if self
            .background
            .as_ref()
            .is_some_and(|source| !source.background_actual_exit())
        {
            return None;
        }
        let s = self.lock();
        let cleared = match &s.role {
            Some(PollCompletion::Ready(_)) => true,
            Some(PollCompletion::Panicked(p)) => p.value().cleanup().is_none(),
            None => false,
        };
        if !cleared || !s.native_body_exited {
            return None;
        }
        match s.phase {
            LogicalActorExitPhase::Unreserved
                if !s.native_pre_actor_started && s.pending_native_returns == 0 =>
            {
                Some(LocalExecutionDisposition::ActualSourcesExited)
            }
            LogicalActorExitPhase::Unreserved => None,
            LogicalActorExitPhase::Joined
                if !s.native_pre_actor_started && s.pending_native_returns == 0 =>
            {
                Some(
                    s.native_return_disposition
                        .unwrap_or(LocalExecutionDisposition::ActualSourcesExited),
                )
            }
            LogicalActorExitPhase::Joined => None,
            _ => None,
        }
    }
}
/// The original source's Native body, never owned by a caught/cancelled role
/// Future. Its private mutex protects only this original local body.
#[derive(Default)]
pub(super) struct OriginalNativePreparation {
    pub(super) start: Option<crate::api::QueryExecutionRequest>,
    pub(super) port: Option<Arc<dyn crate::api::LogicalExecutionNativePort>>,
    pub(super) seed: Option<crate::api::NativeLogicalExecutionSeed>,
    pub(super) open: Option<crate::api::LogicalNativeOpenRequest>,
    pub(super) acceptance: Option<crate::api::LogicalNativeOpenAcceptance>,
    pub(super) session: Option<crate::api::LogicalNativeSession>,
    pub(super) dormant: crate::api::NativeDormantCustody,
    // A successful activation becomes stable before dormant Drop. The Box
    // stays in its ORIGINAL slot; Source holds rows and only a Weak receipt.
    pub(super) pending_activated:
        Option<super::native_active_custody::PendingNativeActivationObligation>,
    pub(super) description: Option<Arc<crate::preparation::FrozenExecutionDescription>>,
    pub(super) consumer: Option<Arc<dyn crate::api::InternalComputationConsumer>>,
    pub(super) capacity: Option<crate::admitted_query_context::QueryResultCapacityBinding>,
    pub(super) streamed_computation:
        Option<crate::admitted_query_context::StreamedComputationProducer>,
    pub(super) native_actions:
        Option<novarocks_workload_control::joint_admission::NativeAsyncActionAuthority>,
    pub(super) exiting: bool,
}
impl OriginalNativePreparation {
    fn close(&mut self) -> Result<(), ()> {
        self.exiting = true;
        // This actual Drop, not None or an empty converge future, is the local
        // author. Any unwind leaves exiting=true and Source unproven.
        self.dormant.retire()?;
        drop(self.start.take());
        drop(self.open.take());
        drop(self.seed.take());
        drop(self.acceptance.take());
        drop(self.session.take());
        drop(self.native_actions.take());
        drop(self.capacity.take());
        drop(self.consumer.take());
        drop(self.description.take());
        drop(self.port.take());
        if let Some(streamed) = self.streamed_computation.as_ref() {
            streamed.native_body_exited().map_err(|_| ())?;
        }
        drop(self.streamed_computation.take());
        self.exiting = false;
        Ok(())
    }
}

/// Installed only in the original health slot. Dropping a rejected wrapper
/// does not dispose its source; the caller disarms it before returning custody
/// to the same finite bank Position.
pub(super) struct LogicalExitHealthCustody {
    source: Arc<OriginalLogicalExit>,
    armed: bool,
}
impl LogicalExitHealthCustody {
    pub(super) fn new(source: Arc<OriginalLogicalExit>) -> Self {
        Self {
            source,
            armed: true,
        }
    }
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for LogicalExitHealthCustody {
    fn drop(&mut self) {
        if self.armed {
            self.source.lock().health_disposed = true;
            // OriginalHealth is taken out of its lock before actual Drop.
            self.source.notify();
        }
    }
}

/// Unique observation edge. Cancelling/dropping a wait never cancels its source.
pub struct LogicalExecutionExitObserver {
    source: Option<Arc<OriginalLogicalExit>>,
}
/// Read-only view of the same original supervision verdict. The original
/// statement retains its unique exit observer or receipt while this Weak is
/// borrowed across a domain await. It grants no Work, report or exit right.
#[derive(Clone)]
pub struct LogicalOwnerFailureObservation {
    original: Weak<OriginalLogicalExit>,
}
impl LogicalOwnerFailureObservation {
    pub fn failure(&self) -> Result<Option<QueryExecutionError>, QueryExecutionError> {
        self.original
            .upgrade()
            .map(|original| original.owner_failure())
            .ok_or_else(|| {
                use novarocks_execution_contract::failure::FailureProjection;
                QueryExecutionError::from_execution(
                    crate::execution_error::ExecutionFailureInput::new(
                        crate::execution_error::QueryFailureReason::ResultProtocol.failure_fact(),
                        novarocks_execution_contract::SafeDetail::truncating(
                            "original logical supervision source is unavailable",
                        ),
                        crate::execution_error::ExecutionFailureSite::Coordinator,
                    ),
                )
            })
    }
}
/// Retains the original completion/footer until this exact receipt exits.
pub struct LogicalExecutionExitReceipt {
    source: Arc<OriginalLogicalExit>,
    local: LocalExecutionDisposition,
}
/// Borrow-only access; no raw packet, runtime failure or footer can be taken.
pub struct LogicalExecutionExitView<'a> {
    state: MutexGuard<'a, State>,
}
impl LogicalExecutionExitView<'_> {
    pub fn actor_phase(&self) -> LogicalActorExitPhase {
        self.state.phase
    }

    /// A runtime exit is not a synthesized Tokio cancellation or panic result.
    pub fn runtime_actor_result(&self) -> Option<&crate::api::logical_runtime::LogicalExit> {
        self.state.runtime.as_ref()
    }
    pub fn role_ended(&self) -> bool {
        self.state.role_ended
    }
    pub fn role_failed(&self) -> bool {
        self.state.role_failed
    }
    pub fn role_result(&self) -> Option<&RoleResult> {
        self.state.role.as_ref()
    }
    /// Diagnostic facts only; none substitutes for an actual local return.
    pub fn native_preparation_failure(&self) -> Option<&(dyn std::any::Any + Send)> {
        self.state.native_failure.as_deref()
    }
    /// The SAME body's failed final destructor, separate from an earlier
    /// preparation cause. This observation never grants a local exit.
    pub fn native_body_cleanup_panic(&self) -> Option<&(dyn std::any::Any + Send)> {
        self.state.native_body_cleanup_panic.as_deref()
    }
    /// Health fact only; each raw consumer cause stays in its original attempt slot.
    pub fn internal_consumer_failed(&self) -> bool {
        self.state.consumer_failed
    }
    /// Raw run causes stay in this Source independently of a positive Active
    /// close. These borrows never transfer the original payload or its charge.
    pub fn native_run_panic(&self) -> Option<&(dyn std::any::Any + Send)> {
        self.state.run_panic.as_deref()
    }
    pub fn native_run_cleanup_panic(&self) -> Option<&(dyn std::any::Any + Send)> {
        self.state.run_cleanup_panic.as_deref()
    }
    /// A diagnostic failure fact, never an actual-exit receipt. Raw causes
    /// remain in the original Native body beside the dormant owner.
    pub fn native_dormant_failed(&self) -> bool {
        self.state.native_dormant_failed
    }
    pub fn native_preparation_exited(&self) -> bool {
        self.state.native_body_exited
    }
    pub fn native_action_started(&self) -> bool {
        self.state.native_pre_actor_started
    }
    pub fn pending_native_returns(&self) -> u64 {
        self.state.pending_native_returns
    }
    pub fn native_closure_observed(&self) -> bool {
        self.state.native_closure_seen
    }
    pub fn native_return_disposition(&self) -> Option<LocalExecutionDisposition> {
        self.state.native_return_disposition
    }
}
impl LogicalExecutionExitObserver {
    /// The same unique business observer has one independent failure waiter.
    /// Physical exit polling does not consume this independent wake.
    pub fn poll_owner_failure(&mut self, cx: &mut Context<'_>) -> Option<QueryExecutionError> {
        let original = self.source.as_ref()?;
        original.owner_failure_changed.register(cx.waker());
        original.owner_failure()
    }
    pub fn owner_failure_observation(&self) -> Option<LogicalOwnerFailureObservation> {
        self.source
            .as_ref()
            .map(|original| LogicalOwnerFailureObservation {
                original: Arc::downgrade(original),
            })
    }
    /// Borrow the canonical background business result without cloning its cause.
    pub fn background_result(
        &self,
    ) -> Option<crate::session_control::BackgroundActionResultGuard<'_>> {
        self.source
            .as_ref()?
            .background
            .as_ref()?
            .try_background_result()
    }
    pub fn background_failure(
        &self,
    ) -> Option<Arc<crate::session_control::StatementActionFailure>> {
        self.source.as_ref()?.background.as_ref()?.failure()
    }
    pub(super) fn new(source: Arc<OriginalLogicalExit>) -> Self {
        Self {
            source: Some(source),
        }
    }
    pub fn view(&self) -> Option<LogicalExecutionExitView<'_>> {
        self.source.as_ref().map(|source| LogicalExecutionExitView {
            state: source.lock(),
        })
    }
    pub fn poll_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<LogicalExecutionExitReceipt, QueryExecutionError>> {
        let Some(source) = self.source.as_ref() else {
            return Poll::Ready(Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original exit observation was already received",
            )));
        };
        source.changed.register(cx.waker());
        if let Some(local) = source.disposition() {
            let source = self.source.take().unwrap();
            drop(source.changed.take());
            return Poll::Ready(Ok(LogicalExecutionExitReceipt { source, local }));
        }
        Poll::Pending
    }
}
impl Drop for LogicalExecutionExitObserver {
    fn drop(&mut self) {
        if let Some(source) = &self.source {
            drop(source.changed.take());
            drop(source.owner_failure_changed.take());
        }
    }
}
impl Drop for LogicalExecutionExitReceipt {
    fn drop(&mut self) {
        drop(self.source.owner_failure_changed.take());
    }
}
impl LogicalExecutionExitReceipt {
    pub fn poll_owner_failure(&mut self, cx: &mut Context<'_>) -> Option<QueryExecutionError> {
        self.source.owner_failure_changed.register(cx.waker());
        self.source.owner_failure()
    }
    pub fn owner_failure_observation(&self) -> LogicalOwnerFailureObservation {
        LogicalOwnerFailureObservation {
            original: Arc::downgrade(&self.source),
        }
    }
    /// The same original Source survives the bank footer and retains raw facts.
    pub fn background_result(
        &self,
    ) -> Option<crate::session_control::BackgroundActionResultGuard<'_>> {
        self.source.background.as_ref()?.try_background_result()
    }
    pub fn background_failure(
        &self,
    ) -> Option<Arc<crate::session_control::StatementActionFailure>> {
        self.source.background.as_ref()?.failure()
    }
    pub const fn local_disposition(&self) -> LocalExecutionDisposition {
        self.local
    }
    pub const fn remote_disposition(&self) -> RemoteExecutionDisposition {
        RemoteExecutionDisposition::RemoteNotReported
    }
    pub fn view(&self) -> LogicalExecutionExitView<'_> {
        LogicalExecutionExitView {
            state: self.source.lock(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionExitGeometry {
    pub source: Layout,
    pub native_body: Layout,
    pub observer: Layout,
    pub receipt: Layout,
    pub input_header: Layout,
    pub publication_tail: Layout,
    pub health_wrapper: Layout,
}
impl LogicalExecutionExitGeometry {
    /// These independent original allocations coexist for one mounted role.
    /// The whole Ready/Panicked packet is inline in Source.State.role; its
    /// footer is already inside self.source. The raw geometry separately
    /// covers Position, the constructed F frame and retained outer completion.
    /// Do not charge another fictional Box for either inline packet.
    pub(super) fn role_source_backing(
        self,
    ) -> Result<
        novarocks_workload_control::PollRoleSourceBacking,
        novarocks_workload_control::WorkError,
    > {
        // Same pinned std 1.98.1 model as the existing original platform
        // authors: Darwin pthread_mutex_t is (c_long, [u8; 56]); Linux's
        // futex storage is inline. Unknown targets cannot silently quote zero.
        let mutex_backing = if cfg!(all(target_os = "macos", target_pointer_width = "64")) {
            Layout::new::<(isize, [u8; 56])>()
        } else if cfg!(target_os = "linux") {
            Layout::new::<()>()
        } else {
            return Err(novarocks_workload_control::WorkError::InvalidConfig(
                "unsupported logical Source Mutex backing",
            ));
        };
        let background = crate::session_control::StatementActionSource::background_geometry();
        novarocks_workload_control::PollRoleSourceBacking::from_layouts(
            &[
                self.source,
                self.native_body,
                self.health_wrapper,
                mutex_backing,
                background.source,
                background.wake,
                background.failure,
                background.failure,
                background.summary,
                // Common source State plus its two possible raw-cause mutexes.
                mutex_backing,
                mutex_backing,
                mutex_backing,
            ],
            &[],
        )
    }
}
impl LogicalExecutionExitObserver {
    /// Source header includes the whole original packet inline. Task/opaque
    /// pointees, platform Mutex and waker internals remain original-owner facts.
    pub fn metadata_geometry() -> Result<
        LogicalExecutionExitGeometry,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        Ok(LogicalExecutionExitGeometry {
            source: novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
                OriginalLogicalExit,
            >())?,
            native_body: novarocks_type_contract::owned_resources::layout::arc_layout(
                Layout::new::<tokio::sync::Mutex<OriginalNativePreparation>>(),
            )?,
            observer: Layout::new::<Self>(),
            receipt: Layout::new::<LogicalExecutionExitReceipt>(),
            input_header: Layout::new::<super::supervisor::OriginalStartHeader>(),
            publication_tail: Layout::new::<super::supervisor::LogicalExecutionStartAttempt>(),
            health_wrapper: Layout::new::<LogicalExitHealthCustody>(),
        })
    }
}

#[cfg(test)]
#[path = "original_exit/tests.rs"]
mod tests;
