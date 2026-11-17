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

//! Per-request composition of the same installed logical and attempt sources.

use crate::control_runtime::actor::{FrontendActor, FrontendEffect};
use crate::control_runtime::host::HostExit;
use crate::control_runtime::logical::links::{AttemptActivationLinkLoan, LogicalAttemptLinks};
use crate::control_runtime::logical::{LogicalHostPorts, LogicalInputBank};
use crate::control_runtime::logical::{LogicalPortInstallContext, LogicalPortInstaller};
use crate::query_execution::lifecycle_diagnostics::{
    FrontendLifecycleDiagnostics, QueryLifecycleConvergenceSnapshot,
    RuntimeFilterTerminalRollupSnapshot,
};
use crate::task_execution::host::{publication::AttemptPublicationOwner, startup::AttemptStartup};
use novarocks_control_runtime::actor_turn::{ActorRegistration, ActorRuntime, EffectTicket};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, ExecutionFailureInput, ExecutionFailureSite, MonotonicInstant,
    QueryExecutionError, QueryFailureReason, RuntimeFilterDiagnostic,
    logical_port::LogicalToAttempt,
};
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_logical_execution_protocol::{
    effect::AttemptLifecycleDiagnostic, start::LogicalStart,
};
use novarocks_query_application::{
    api::{
        ComputationReadyToken, LogicalFuture, NativeAttemptStartupOutcome,
        NativeInstalledAttemptPort, StatementNativeHealthFailure,
    },
    coordination::executor::ConsumerObservationPort,
};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tokio::sync::Notify;

/// One cell is created with the original request/factory. It never owns the FE
/// process, HostRegistry, Source, Session, installer or observation facade.
pub(crate) struct LogicalAttemptBindingCell {
    #[cfg(test)]
    original_failure_probe: OnceLock<Arc<dyn Fn() -> String + Send + Sync>>,
    #[cfg(test)]
    original_failure_trigger: OnceLock<(
        Arc<dyn Fn() + Send + Sync>,
        novarocks_control_runtime_core::identity::ScopedWorkRef,
    )>,
    bound: OnceLock<Arc<BoundLogicalPorts>>,
    closed: crate::task_execution::host::port::abort::ClosedParticipations,
    retain_profile: bool,
    lifecycle: Arc<FrontendLifecycleDiagnostics>,
}

pub(crate) struct BoundLogicalPorts {
    #[cfg(test)]
    original_consumer: Weak<novarocks_query_application::coordination::executor::ConsumerExecutor>,
    pub(crate) start: LogicalStart,
    pub(crate) runtime: ActorRuntime<FrontendActor>,
    pub(crate) registration: ActorRegistration,
    pub(crate) inputs: Arc<LogicalInputBank>,
    pub(crate) links: Arc<LogicalAttemptLinks>,
    pub(crate) closed: crate::task_execution::host::port::abort::ClosedParticipations,
    rows: Vec<Mutex<Option<Arc<OriginalAttemptObservation>>>>,
    retain_profile: bool,
    lifecycle: Arc<FrontendLifecycleDiagnostics>,
}

/// A small receipt of the original publisher, never a copy of its rich profile
/// snapshot or an additional reader of canonical Native replies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OriginalProfilePublication {
    NoProfile {
        cut: ComputationReadyToken,
    },
    Published {
        cut: ComputationReadyToken,
        selected_at: MonotonicInstant,
        closure: crate::task_execution::profile_diagnostics::ProfileDiagnosticClosure,
    },
}

struct DetachedLifecycle {
    selection: Option<Arc<RuntimeFilterDiagnostic>>,
    snapshot: RuntimeFilterTerminalRollupSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadyConvergencePublished {
    cut: ComputationReadyToken,
    all_participations_closed: bool,
    remote_tracking_ended: bool,
    local_resources_exited: bool,
}

struct ObservationState {
    cut: Option<ComputationReadyToken>,
    registered_cut: Option<ComputationReadyToken>,
    profile: Option<Result<OriginalProfilePublication, QueryExecutionError>>,
    convergence: Option<ReadyConvergencePublished>,
    lifecycle: Option<DetachedLifecycle>,
    exit: Option<Arc<dyn OriginalInstalledHostExit>>,
    startup: Option<AttemptStartup>,
    infrastructure: Option<crate::task_execution::host::auxiliary::AttemptInfrastructureHealth>,
    publisher: Option<Weak<dyn OriginalComputationCutPublisher>>,
    committed_local_exit: bool,
    committed_closure: bool,
    lifecycle_selection: Option<Arc<RuntimeFilterDiagnostic>>,
}

/// A weak view of the one original publisher. The facade never retains its
/// rich PublicationRecord or obtains a replacement publisher from an identity.
pub(crate) trait OriginalComputationCutPublisher: Send + Sync {
    fn register_ready_with(
        &self,
        original: ComputationReadyToken,
        commit: &mut dyn FnMut() -> Result<
            (),
            crate::task_execution::host::port::control::PortControlError,
        >,
    ) -> Result<(), crate::task_execution::host::port::control::PortControlError>;
}

/// Narrow original weak host capability. This hides the host's internal
/// registry graph without extending any Source/Actor/Scope lifetime.
trait OriginalInstalledHostExit: Send + Sync {
    fn observe_actual_exit(&self) -> tokio::sync::watch::Receiver<bool>;
    fn request_close(&self) -> Result<(), novarocks_control_runtime::actor_turn::ActorTurnError>;
}
impl OriginalInstalledHostExit for HostExit {
    fn observe_actual_exit(&self) -> tokio::sync::watch::Receiver<bool> {
        HostExit::observe_actual_exit(self)
    }
    fn request_close(&self) -> Result<(), novarocks_control_runtime::actor_turn::ActorTurnError> {
        HostExit::request_close(self)
    }
}

/// Exact fixed-A row bound with an already-issued original link. It contains
/// passive receipts and the original weak HostExit capability, no rich owner.
pub(crate) struct OriginalAttemptObservation {
    activation: AttemptActivationIdentity,
    retain_profile: bool,
    observe_lifecycle: bool,
    state: Mutex<ObservationState>,
    changed: Notify,
}

impl LogicalAttemptBindingCell {
    pub(crate) fn new(
        retain_profile: bool,
        lifecycle: Arc<FrontendLifecycleDiagnostics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            bound: OnceLock::new(),
            #[cfg(test)]
            original_failure_probe: OnceLock::new(),
            #[cfg(test)]
            original_failure_trigger: OnceLock::new(),
            closed: crate::task_execution::host::port::abort::ClosedParticipations::new(),
            retain_profile,
            lifecycle,
        })
    }
    #[cfg(test)]
    pub(crate) fn inject_original_core_fault_for_fixture(
        &self,
    ) -> novarocks_control_runtime_core::identity::ScopedWorkRef {
        let (trigger, work) = self
            .original_failure_trigger
            .get()
            .expect("same bound failure source");
        trigger();
        *work
    }
    #[cfg(test)]
    pub(crate) fn original_failure_for_fixture(&self) -> String {
        self.original_failure_probe.get().map_or_else(
            || "original logical ports not bound".to_owned(),
            |probe| probe(),
        )
    }
    /// Inspect the original immutable publication; this neither polls Core nor
    /// constructs an acceptance token or a replacement convergence snapshot.
    #[cfg(test)]
    pub(crate) fn original_convergence_for_fixture(
        &self,
        expected: ComputationReadyToken,
    ) -> Result<Option<(bool, bool, bool)>, QueryExecutionError> {
        let row = self.bound()?.observation(expected.activation())?;
        let state = row.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.registered_cut.is_some_and(|cut| cut != expected) {
            return Err(binding_error(
                "fixture supplied a foreign original Ready token",
            ));
        }
        let Some(original) = state.convergence else {
            return Ok(None);
        };
        if state.registered_cut != Some(expected) || original.cut != expected {
            return Err(binding_error(
                "original convergence belongs to a different Ready token",
            ));
        }
        Ok(Some((
            original.all_participations_closed,
            original.remote_tracking_ended,
            original.local_resources_exited,
        )))
    }
    pub(crate) fn closed_participations(
        &self,
    ) -> crate::task_execution::host::port::abort::ClosedParticipations {
        self.closed.clone()
    }
    pub(crate) fn bound(&self) -> Result<Arc<BoundLogicalPorts>, QueryExecutionError> {
        self.bound
            .get()
            .cloned()
            .ok_or_else(|| binding_error("original logical ports are not installed"))
    }
}

impl LogicalPortInstaller for LogicalAttemptBindingCell {
    fn bind(&self, original: LogicalPortInstallContext<'_>) -> Arc<dyn LogicalHostPorts> {
        #[cfg(test)]
        assert!(
            self.original_failure_trigger
                .set(original.original_failure_trigger_for_fixture())
                .is_ok()
        );
        #[cfg(test)]
        assert!(
            self.original_failure_probe
                .set(original.original_failure_probe_for_fixture())
                .is_ok()
        );
        let ports = Arc::new(BoundLogicalPorts {
            #[cfg(test)]
            original_consumer: original.original_consumer_for_fixture(),
            start: original.start().clone(),
            runtime: original.runtime().clone(),
            registration: original.registration().clone(),
            inputs: original.inputs(),
            links: original.links(),
            closed: self.closed.clone(),
            rows: (0..original.start().budgets.max_attempts.get())
                .map(|_| Mutex::new(None))
                .collect(),
            retain_profile: self.retain_profile,
            lifecycle: self.lifecycle.clone(),
        });
        assert!(
            self.bound.set(ports.clone()).is_ok(),
            "one original per-request logical binding"
        );
        ports
    }
}

impl BoundLogicalPorts {
    /// The fixed row must actually have been bound by this original install.
    /// This Weak grants no activation, source, resource or exit authority.
    #[cfg(test)]
    pub(crate) fn original_consumer_for_fixture(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<
        Weak<novarocks_query_application::coordination::executor::ConsumerExecutor>,
        QueryExecutionError,
    > {
        self.observation(activation)?;
        Ok(self.original_consumer.clone())
    }
    pub(crate) fn bind_observation(
        &self,
        original: &AttemptActivationLinkLoan,
        activation: AttemptActivationIdentity,
        observe_lifecycle: bool,
    ) -> Arc<OriginalAttemptObservation> {
        assert_eq!(activation.actor(), self.start.actor_instance);
        assert!(
            self.links.matches_loan(original, activation),
            "same original activation link"
        );
        // The original loan supplies the fixed position; a numeric activation
        // never creates a row or acquires another Work/Scope.
        let slot = &self.rows[original.index()];
        let mut target = slot.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            target.is_none(),
            "one original observation row per activation link"
        );
        let row =
            OriginalAttemptObservation::new(activation, self.retain_profile, observe_lifecycle);
        *target = Some(row.clone());
        row
    }
    fn observation(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<Arc<OriginalAttemptObservation>, QueryExecutionError> {
        self.rows
            .iter()
            .find_map(|slot| {
                slot.lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .filter(|row| row.activation == activation)
                    .cloned()
            })
            .ok_or_else(|| binding_error("original activation has no installed observation row"))
    }
}

impl OriginalAttemptObservation {
    pub(crate) fn new(
        activation: AttemptActivationIdentity,
        retain_profile: bool,
        observe_lifecycle: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            activation,
            retain_profile,
            observe_lifecycle,
            state: Mutex::new(ObservationState {
                cut: None,
                registered_cut: None,
                profile: None,
                convergence: None,
                lifecycle: None,
                exit: None,
                startup: None,
                infrastructure: None,
                publisher: None,
                committed_local_exit: false,
                committed_closure: false,
                lifecycle_selection: None,
            }),
            changed: Notify::new(),
        })
    }
    pub(crate) fn bind_publisher(&self, original: &Arc<AttemptPublicationOwner>) {
        assert_eq!(original.activation(), self.activation);
        let original: Arc<dyn OriginalComputationCutPublisher> = original.clone();
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            state.publisher.is_none(),
            "one original publication capability"
        );
        state.publisher = Some(Arc::downgrade(&original));
    }
    pub(crate) fn activation(&self) -> AttemptActivationIdentity {
        self.activation
    }
    pub(crate) fn observes_lifecycle(&self) -> bool {
        self.observe_lifecycle
    }
    pub(crate) fn bind_source(
        &self,
        link: &AttemptActivationLinkLoan,
        registration: &ActorRegistration,
        original: Arc<HostExit>,
        startup: AttemptStartup,
        publisher: &Arc<AttemptPublicationOwner>,
        infrastructure: crate::task_execution::host::auxiliary::AttemptInfrastructureHealth,
    ) {
        assert!(
            link.matches_registered(self.activation, registration, &original),
            "same original registered child exit"
        );
        assert!(startup.matches_registration(registration));
        assert!(infrastructure.matches(registration));
        assert_eq!(publisher.activation(), self.activation);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(state.exit.is_none(), "one original HostExit observer");
        state.exit = Some(original);
        state.startup = Some(startup);
        state.infrastructure = Some(infrastructure);
        let publisher: Arc<dyn OriginalComputationCutPublisher> = publisher.clone();
        assert!(
            state
                .publisher
                .as_ref()
                .is_some_and(|original| Weak::ptr_eq(original, &Arc::downgrade(&publisher))),
            "same already-bound original publisher"
        );
        drop(state);
        self.changed.notify_waiters();
    }

    /// Called only after this original AP fact was actually committed under
    /// its current effect ticket. Remote/local reports may be separate inputs;
    /// neither alone authorizes final Source closure or certifies HostExit.
    pub(crate) fn convergence_committed(
        &self,
        activation: AttemptActivationIdentity,
        local_exited: bool,
        closure: bool,
        lifecycle_runtime_filters: Option<Arc<RuntimeFilterDiagnostic>>,
    ) {
        assert_eq!(activation, self.activation);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.committed_local_exit |= local_exited;
        state.committed_closure |= closure;
        if let Some(selection) = lifecycle_runtime_filters {
            assert!(
                state
                    .lifecycle_selection
                    .as_ref()
                    .is_none_or(|previous| Arc::ptr_eq(previous, &selection)),
                "one original lifecycle selection"
            );
            state.lifecycle_selection = Some(selection);
        }
        drop(state);
        self.changed.notify_waiters();
    }

    pub(crate) fn lifecycle_selection(&self) -> Option<Arc<RuntimeFilterDiagnostic>> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .lifecycle_selection
            .clone()
    }
    /// Serialize small metadata with the SAME publisher across the actual
    /// current-ticket/session handoff. No rich publication lock is acquired.
    pub(crate) fn register_cut_with(
        &self,
        original: ComputationReadyToken,
        commit: &mut dyn FnMut() -> Result<
            (),
            crate::task_execution::host::port::control::PortControlError,
        >,
    ) -> Result<(), crate::task_execution::host::port::control::PortControlError> {
        use crate::task_execution::host::port::control::PortControlError;
        let publisher = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .publisher
            .clone()
            .and_then(|publisher| publisher.upgrade());
        if let Some(publisher) = publisher {
            return publisher.register_ready_with(original, commit);
        }
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.registered_cut != Some(original) {
            return Err(PortControlError::ComputationCut(binding_error(
                "original computation publisher has no exact registration receipt",
            )));
        }
        commit()
    }
    pub(crate) fn commit_registered_cut(
        &self,
        original: ComputationReadyToken,
        commit: &mut dyn FnMut() -> Result<
            (),
            crate::task_execution::host::port::control::PortControlError,
        >,
    ) -> Result<(), crate::task_execution::host::port::control::PortControlError> {
        use crate::task_execution::host::port::control::PortControlError;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if original.activation() != self.activation
            || state.cut.is_some_and(|cut| cut != original)
            || state.registered_cut.is_some_and(|cut| cut != original)
        {
            return Err(PortControlError::ComputationCut(binding_error(
                "original computation cut cannot be replaced",
            )));
        }
        commit()?;
        state.cut = Some(original);
        state.registered_cut = Some(original);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn original_registered_cut(&self) -> Option<ComputationReadyToken> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .registered_cut
    }
    pub(crate) fn notify_cut_registered(&self) {
        self.changed.notify_waiters();
    }
    pub(crate) fn register_cut(
        &self,
        original: ComputationReadyToken,
    ) -> Result<(), QueryExecutionError> {
        self.register_cut_with(original, &mut || Ok(()))
            .map_err(|error| binding_error(format!("{error:?}")))?;
        self.notify_cut_registered();
        Ok(())
    }
    async fn wait_registered_cut(&self) -> Result<ComputationReadyToken, QueryExecutionError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let exit = {
                let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(cut) = state.registered_cut {
                    return Ok(cut);
                }
                state.exit.clone()
            }
            .ok_or_else(|| binding_error("original computation publisher has no bound HostExit"))?;
            let mut actual = exit.observe_actual_exit();
            if *actual.borrow() {
                return self
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .registered_cut
                    .ok_or_else(|| {
                        binding_error(
                            "original computation publisher exited without registering its cut",
                        )
                    });
            }
            tokio::select! {
                _ = changed => {},
                result = actual.wait_for(|exited| *exited) => {
                    if result.is_err() {
                        return Err(binding_error("original computation publisher exit is unavailable"));
                    }
                }
            }
        }
    }
    pub(crate) fn profile_published(
        &self,
        original: OriginalProfilePublication,
    ) -> Result<(), QueryExecutionError> {
        let cut = match original {
            OriginalProfilePublication::NoProfile { cut }
            | OriginalProfilePublication::Published { cut, .. } => cut,
        };
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.cut != Some(cut)
            || matches!(original, OriginalProfilePublication::NoProfile { .. })
                == self.retain_profile
        {
            return Err(binding_error(
                "profile publication differs from its original cut or policy",
            ));
        }
        if let Some(previous) = &state.profile {
            return if *previous == Ok(original) {
                Ok(())
            } else {
                Err(binding_error(
                    "original profile publication cannot be replaced",
                ))
            };
        }
        state.profile = Some(Ok(original));
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    pub(crate) fn profile_failed(&self, original: QueryExecutionError) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.profile.is_none() {
            state.profile = Some(Err(original));
        }
        drop(state);
        self.changed.notify_waiters();
    }
    pub(crate) fn detach_lifecycle(
        &self,
        selection: Option<Arc<RuntimeFilterDiagnostic>>,
        snapshot: RuntimeFilterTerminalRollupSnapshot,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            state.lifecycle.is_none(),
            "one original detached lifecycle snapshot"
        );
        state.lifecycle = Some(DetachedLifecycle {
            selection,
            snapshot,
        });
        drop(state);
        self.changed.notify_waiters();
    }
}

impl NativeInstalledAttemptPort for LogicalAttemptBindingCell {
    fn start(
        &self,
        activation: AttemptActivationIdentity,
    ) -> LogicalFuture<'_, Result<NativeAttemptStartupOutcome, QueryExecutionError>> {
        Box::pin(async move {
            let bound = self.bound()?;
            let row = bound.observation(activation)?;
            let mut startup = row
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .startup
                .clone()
                .ok_or_else(|| binding_error("original installed startup is unavailable"))?;
            startup
                .start_protocol(&bound.runtime)
                .await
                .map_err(|_| binding_error("original startup did not complete"))
        })
    }

    fn record_ready(
        &self,
        original: ComputationReadyToken,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async move {
            let row = self.bound()?.observation(original.activation())?;
            row.register_cut(original)
        })
    }

    fn resources_exited(
        &self,
        activation: AttemptActivationIdentity,
    ) -> LogicalFuture<'_, Result<(), StatementNativeHealthFailure>> {
        Box::pin(async move {
            let row = self
                .bound()
                .and_then(|bound| bound.observation(activation))
                .map_err(|error| {
                    unproven_exit(
                        "original installed attempt observation is unavailable",
                        error,
                    )
                })?;
            let mut close_requested = false;
            loop {
                let changed = row.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let (exit, retirement_ready, infrastructure) = {
                    let state = row.state.lock().unwrap_or_else(|e| e.into_inner());
                    (
                        state.exit.clone(),
                        state.committed_local_exit
                            && state.committed_closure
                            && (!row.observe_lifecycle || state.lifecycle_selection.is_some()),
                        state.infrastructure.clone(),
                    )
                };
                let exit = exit.ok_or_else(|| {
                    unproven_exit(
                        "original installed HostExit is unavailable",
                        binding_error("original HostExit was not bound"),
                    )
                })?;
                let mut infrastructure = infrastructure.ok_or_else(|| {
                    unproven_exit(
                        "original installed infrastructure observer is unavailable",
                        binding_error("original infrastructure observer was not bound"),
                    )
                })?;
                if let Some(failure) = infrastructure.exit_failure() {
                    return Err(failure);
                }
                let mut actual = exit.observe_actual_exit();
                // An AP failure can actually exit before its normal final
                // report. Only this original true witness bypasses that fence.
                if *actual.borrow() {
                    return Ok(());
                }
                if retirement_ready && !close_requested {
                    exit.request_close().map_err(|error| {
                        unproven_exit("original attempt host close was refused", error)
                    })?;
                    close_requested = true;
                }
                tokio::select! {
                    biased;
                    cause = infrastructure.exit_failed() => return Err(cause),
                    _ = changed => {},
                    result = actual.wait_for(|exited| *exited) => {
                        result.map_err(|error| unproven_exit("original attempt actual exit is unavailable", error))?;
                        // Recheck the same original health record published
                        // before Source retirement. HostExit remains a separate fact.
                    }
                }
            }
        })
    }
}

fn unproven_exit(
    detail: &'static str,
    cause: impl std::error::Error + Send + Sync + 'static,
) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(detail, Arc::new(cause))
}

impl ConsumerObservationPort for LogicalAttemptBindingCell {
    fn diagnostics(
        &self,
        activation: AttemptActivationIdentity,
        deadline: MonotonicInstant,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async move {
            let row = self.bound()?.observation(activation)?;
            let registered = row.wait_registered_cut().await?;
            loop {
                let changed = row.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let (cut, publication, exit) = {
                    let state = row.state.lock().unwrap_or_else(|e| e.into_inner());
                    (state.cut, state.profile.clone(), state.exit.clone())
                };
                let cut = cut.filter(|cut| *cut == registered).ok_or_else(|| {
                    binding_error("diagnostic publication has no same registered computation cut")
                })?;
                if cut.diagnostic_deadline() != deadline {
                    return Err(binding_error(
                        "diagnostic deadline differs from the original cut",
                    ));
                }
                if let Some(original) = publication {
                    return original.map(|_| ());
                }
                if !row.retain_profile {
                    return row.profile_published(OriginalProfilePublication::NoProfile { cut });
                }
                let mut exit = exit
                    .ok_or_else(|| binding_error("diagnostic publisher has no original HostExit"))?
                    .observe_actual_exit();
                if *exit.borrow() {
                    // Publication precedes the actual exit. Re-read its SAME
                    // receipt after observing that exit, so a concurrent
                    // publication cannot be mistaken for missing output.
                    return row.state.lock().unwrap_or_else(|e| e.into_inner())
                        .profile.clone().ok_or_else(|| binding_error("original diagnostic publisher exited without a publication receipt"))?
                        .map(|_| ());
                }
                tokio::select! {
                    _ = changed => {},
                    result = exit.wait_for(|exited| *exited) => {
                        if result.is_err() { return Err(binding_error("original diagnostic publisher exit is unavailable")); }
                    }
                }
            }
        })
    }
    fn convergence(
        &self,
        activation: AttemptActivationIdentity,
        all_participations_closed: bool,
        remote_tracking_ended: bool,
        local_resources_exited: bool,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async move {
            let row = self.bound()?.observation(activation)?;
            let cut = row.wait_registered_cut().await?;
            let mut state = row.state.lock().unwrap_or_else(|e| e.into_inner());
            let original = ReadyConvergencePublished {
                cut,
                all_participations_closed,
                remote_tracking_ended,
                local_resources_exited,
            };
            if state
                .convergence
                .is_some_and(|previous| previous != original)
            {
                return Err(binding_error(
                    "original convergence publication cannot be replaced",
                ));
            }
            state.convergence = Some(original);
            drop(state);
            row.changed.notify_waiters();
            Ok(())
        })
    }
}

impl LogicalHostPorts for BoundLogicalPorts {
    fn deliver(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        original: LogicalToAttempt,
    ) -> Result<(), QueryExecutionError> {
        self.links
            .endpoint(original.activation)
            .ok_or_else(|| binding_error("original attempt endpoints are not installed"))?
            .deliver(runtime, ticket, original)
            .map_err(|error| binding_error(error.to_string()))
    }
    fn publish_lifecycle(
        &self,
        original: AttemptLifecycleDiagnostic,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async move {
            if !self.lifecycle.is_enabled() {
                return Ok(());
            }
            let row = self.observation(original.activation)?;
            let detached = {
                let mut state = row.state.lock().unwrap_or_else(|e| e.into_inner());
                let detached = state.lifecycle.as_ref().ok_or_else(|| {
                    binding_error(
                        "original lifecycle snapshot was not transferred before attempt exit",
                    )
                })?;
                let same_selection = match (&detached.selection, &original.runtime_filters) {
                    (None, None) => true,
                    (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                    _ => false,
                };
                if !same_selection {
                    return Err(binding_error(
                        "lifecycle publication has a foreign original selection",
                    ));
                }
                state.lifecycle.take().unwrap()
            };
            self.lifecycle.publish(QueryLifecycleConvergenceSnapshot {
                execution_id: original.activation.execution(),
                primary_error: None,
                failure: original.accepted_failure,
                worker_participations_converged: original.all_participations_closed,
                runtime_filter: detached.snapshot,
            });
            Ok(())
        })
    }
}

fn binding_error(detail: impl Into<String>) -> QueryExecutionError {
    let detail = detail.into();
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::PreparationProtocol.failure_fact(),
        SafeDetail::truncating(&detail),
        ExecutionFailureSite::Coordinator,
    ))
}

#[cfg(test)]
mod startup_tests {
    use super::*;
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId};

    #[tokio::test]
    async fn missing_original_binding_cannot_report_protocol_closed_or_ready() {
        let cell = LogicalAttemptBindingCell::new(
            false,
            Arc::new(FrontendLifecycleDiagnostics::default()),
        );
        let execution =
            QueryExecutionId::new(QueryId::new(1, 1), AttemptId::new(1).unwrap()).unwrap();
        let activation = AttemptActivationIdentity::from_parts(1, execution, 1);
        let error = cell.start(activation).await.unwrap_err();
        let failure = error.execution_failure().unwrap();
        assert_eq!(
            failure.fact(),
            QueryFailureReason::PreparationProtocol.failure_fact()
        );
        assert_eq!(failure.site(), ExecutionFailureSite::Coordinator);
        assert!(cell.bound.get().is_none());
    }
}
