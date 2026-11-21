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

//! Native actions retain the original tickets, owners and borrowed-future causes.
//! The FE carrier supplies work credentials and observes actual work joins.

use crate::api::{
    BoundedRootReadPort, LogicalFuture, NativeAttemptPreparationError, NativeAttemptStartupOutcome,
    NativeAttemptTopologyRequirement, NativeRowsAttemptRuntime, QueryExecutionError,
    QueryExecutionErrorKind,
};
use crate::coordination::{
    AttemptSchedule, LogicalExecutionRegistration, LogicalExecutionRuntimeRegistryHandle,
    native_active_custody::NativeActivationReturnObligation,
    original_exit::{OriginalLogicalExit, OriginalNativePreparation},
    replacement::{
        ActiveReplacementResources, QualifiedReplacementReservation,
        ReplacementQualificationEffectAdmission, ReplacementQualificationEffectPort,
        ReplacementQualificationEffectSubmission, ReplacementQualificationFailure,
        ReplacementQualificationIdentity, ReplacementQualificationRequest,
        ReplacementQualificationSettlement, receipt_channel,
    },
    schedule::build_attempt_schedule,
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, ExecutionFailureInput, ExecutionFailureSite, MonotonicClock,
    MonotonicInstant, QueryFailureReason,
};
use novarocks_execution_contract::{
    SafeDetail, TaskIdentity, TaskOperationId, failure::FailureProjection,
};
use novarocks_logical_execution_protocol::{
    effect::NativeEffect,
    input::{
        FailureReport, NativeFact, QualificationIdentity, QualificationSettlement,
        QualifiedAdmission, QualifiedReplacementFacts, SuccessorTopology,
    },
};
use novarocks_types::{FrontendProcessId, QueryExecutionId};
use novarocks_workload_control::{CancellationReason, CancellationView};
use std::{
    any::Any,
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
    time::Instant,
};
use tokio::sync::{Mutex as AsyncMutex, watch};

mod cleanup;
pub use cleanup::{
    LogicalOwnerFailureObserver, NativeCleanupActive, NativeCleanupInventory,
    NativeCleanupPreparation, NativeCleanupPreparationPhase, NativeCleanupQualification,
};

type ActivationKey = (u64, QueryExecutionId, u64);
fn activation_key(identity: AttemptActivationIdentity) -> ActivationKey {
    (
        identity.actor().get(),
        identity.execution(),
        identity.generation(),
    )
}

type PanicPayload = Box<dyn Any + Send>;
type OriginalJoinFailure = Arc<dyn Error + Send + Sync>;
/// Original per-attempt consumer input, not an execution grant or identity.
struct OriginalConsumerStart {
    consumer: Arc<dyn crate::api::InternalComputationConsumer>,
    activation: crate::api::InternalConsumerActivation,
    exit: crate::api::InternalConsumerExitReporter,
    activity: Option<crate::admitted_query_context::ResultCapacityActivityLease>,
}
pub(crate) type RootAckFuture = LogicalFuture<
    'static,
    Result<crate::api::RetainedRootReply, crate::coordination::RootResultFetchFailure>,
>;

/// Exactly two causes per original future: factory/poll and subsequent destruction.
#[derive(Default)]
struct FutureCauses {
    primary: Mutex<Option<PanicPayload>>,
    cleanup: Mutex<Option<PanicPayload>>,
}
impl FutureCauses {
    fn retain(&self, payload: PanicPayload, cleanup: bool) {
        let mut slot = if cleanup {
            &self.cleanup
        } else {
            &self.primary
        }
        .lock()
        .unwrap_or_else(|e| e.into_inner());
        assert!(slot.is_none(), "one cause for each original future phase");
        *slot = Some(payload);
    }
    fn failed(&self) -> bool {
        self.primary
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
            || self
                .cleanup
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some()
    }
}

impl fmt::Debug for FutureCauses {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original Native future causes")
    }
}
impl fmt::Display for FutureCauses {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original Native future failed; raw causes retained")
    }
}
impl Error for FutureCauses {}

#[derive(Default)]
struct CleanupFailure {
    reported: Mutex<Option<crate::api::StatementNativeHealthFailure>>,
}
impl fmt::Debug for CleanupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original installed Native cleanup failure")
    }
}
impl fmt::Display for CleanupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original installed Native cleanup failed; typed cause retained")
    }
}
impl Error for CleanupFailure {}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WaitFailure {
    Panicked,
    Cancelled(CancellationReason),
    Deadline,
}

/// Ready values reach their original destination before the borrowed F is dropped.
/// The carrier never aborts this wrapper: its actual Join is a separate receipt.
async fn poll_original<T>(
    mut future: LogicalFuture<'_, T>,
    causes: &FutureCauses,
    cancellation: Option<&CancellationView>,
    deadline: Option<tokio::time::Instant>,
    mut receive: impl FnMut(T),
) -> Result<(), WaitFailure> {
    let result = if let Some(reason) = cancellation.and_then(CancellationView::reason) {
        Err(WaitFailure::Cancelled(reason))
    } else if deadline.is_some_and(|at| tokio::time::Instant::now() >= at) {
        // Tokio's timer wheel may round an already-expired instant to its next
        // tick. Do not grant one more original Native poll during that gap.
        Err(WaitFailure::Deadline)
    } else {
        let output = std::future::poll_fn(|cx| {
            match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(Poll::Ready(value)) => {
                    receive(value);
                    Poll::Ready(Ok(()))
                }
                Ok(Poll::Pending) => Poll::Pending,
                Err(payload) => {
                    causes.retain(payload, false);
                    Poll::Ready(Err(WaitFailure::Panicked))
                }
            }
        });
        tokio::pin!(output);
        let cancelled = async {
            match cancellation {
                Some(view) => {
                    let reason = view.cancelled().await;
                    view.reason().unwrap_or(reason)
                }
                None => std::future::pending().await,
            }
        };
        let expired = async {
            match deadline {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            reason = cancelled => Err(WaitFailure::Cancelled(reason)),
            () = expired => Err(WaitFailure::Deadline),
            result = &mut output => result,
        }
    };
    if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(future))) {
        causes.retain(payload, true);
    }
    result
}

struct PreparedAttempt {
    schedule: Arc<AttemptSchedule>,
    preparation_deadline: Option<MonotonicInstant>,
    binding_complete: bool,
    activation_started: Option<AttemptActivationIdentity>,
    terminal_cut: Option<(
        AttemptActivationIdentity,
        MonotonicInstant,
        MonotonicInstant,
    )>,
}
struct OriginalRootRead {
    port: Arc<dyn BoundedRootReadPort>,
    root: TaskIdentity,
    attempted: bool,
    closing: bool,
    sealed: bool,
    failure: Option<QueryExecutionError>,
    // The sole failed-owner continuation may await the original action.
    // This inline observer grants no second callback or execution capacity.
    close_waiter: Option<Waker>,
}
impl OriginalRootRead {
    fn from_rows(rows: &NativeRowsAttemptRuntime) -> Self {
        Self {
            port: rows.binding.port.clone(),
            root: rows.binding.frontier.root(),
            attempted: false,
            closing: false,
            sealed: false,
            failure: None,
            close_waiter: None,
        }
    }
}
struct OriginalReadCloseProgress<'a> {
    native: &'a NativeExecutor,
    activation: AttemptActivationIdentity,
}
impl Drop for OriginalReadCloseProgress<'_> {
    fn drop(&mut self) {
        let waiter = {
            let mut storage = self.native.lock();
            let original = storage
                .active
                .get_mut(&activation_key(self.activation))
                .and_then(|active| active.read.as_mut())
                .expect("same original in-flight read close");
            original.closing = false;
            original.close_waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}
struct ActiveOriginal {
    owner: Arc<AsyncMutex<NativeActivationReturnObligation>>,
    rows: Option<NativeRowsAttemptRuntime>,
    read: Option<OriginalRootRead>,
    terminal_cut: Option<(MonotonicInstant, MonotonicInstant)>,
    streamed_success: bool,
    computation_ready: Option<crate::api::ComputationReadyToken>,
    run_started: bool,
    resources_exited: bool,
    cleanup_adopted: bool,
    dormant_retired_for_cleanup: bool,
    cleanup_failure: Arc<CleanupFailure>,
}
type QualificationClose = (
    Option<QualifiedReplacementReservation>,
    Option<ActiveReplacementResources>,
    watch::Sender<bool>,
);
struct QualificationOriginal {
    identity: QualificationIdentity,
    operation: Option<TaskOperationId>,
    // Claims the original local action once; not evidence of transport handoff.
    dispatch_started: bool,
    // The custody decision precedes the original watch's lock-free wake tail.
    cancellation_issued: bool,
    // Empty slots do not prove exit after an opaque operation panics.
    activation_in_flight: bool,
    resources_retiring: bool,
    dispatch_settled: bool,
    cancellation: watch::Sender<bool>,
    qualified: Option<QualifiedReplacementReservation>,
    active: Option<ActiveReplacementResources>,
    activation: Option<AttemptActivationIdentity>,
    abandoned: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparationPhase {
    Pending,
    Failed,
    Prepared,
}

#[derive(Default)]
struct NativeStorage {
    prepared: BTreeMap<QueryExecutionId, PreparedAttempt>,
    preparations: BTreeMap<QueryExecutionId, PreparationPhase>,
    dormant_execution: Option<QueryExecutionId>,
    cleaned_dormant_execution: Option<QueryExecutionId>,
    active: BTreeMap<ActivationKey, ActiveOriginal>,
    qualifications: BTreeMap<QueryExecutionId, QualificationOriginal>,
    causes: BTreeMap<(QueryExecutionId, u8), Arc<FutureCauses>>,
    tails: BTreeMap<(ActivationKey, OriginalTailKind), Arc<OriginalTailSlot>>,
    ready_tails: VecDeque<(ActivationKey, OriginalTailKind)>,
}

impl NativeStorage {
    fn record_terminal_cut(
        &mut self,
        initial: AttemptActivationIdentity,
        activation: AttemptActivationIdentity,
        cut: MonotonicInstant,
        deadline: MonotonicInstant,
    ) -> Result<(), QueryExecutionError> {
        let exact = activation == initial
            || self
                .qualifications
                .get(&activation.execution())
                .is_some_and(|original| {
                    original.identity.actor == activation.actor()
                        && original.identity.replacement == activation.execution()
                        && original.identity.generation == activation.generation()
                });
        if !exact {
            return Err(protocol(
                "terminal cut has another original activation identity",
            ));
        }
        let prepared = self
            .prepared
            .get_mut(&activation.execution())
            .ok_or_else(|| protocol("terminal cut has no original prepared owner"))?;
        let original = prepared
            .terminal_cut
            .get_or_insert((activation, cut, deadline));
        if original.0 != activation {
            return Err(protocol(
                "terminal cut belongs to another prepared generation",
            ));
        }
        original.2 = original.2.min(deadline);
        if let Some(active) = self.active.get_mut(&activation_key(activation)) {
            let current = active.terminal_cut.get_or_insert((original.1, original.2));
            current.1 = current.1.min(original.2);
        }
        Ok(())
    }
    fn ensure_qualification(
        &mut self,
        initial: AttemptActivationIdentity,
        identity: QualificationIdentity,
        operation: Option<TaskOperationId>,
    ) -> Result<&mut QualificationOriginal, QueryExecutionError> {
        if !self.qualifications.contains_key(&identity.replacement) {
            let known = identity.actor == initial.actor()
                && identity.failed.query_id() == initial.execution().query_id()
                && identity.replacement.query_id() == initial.execution().query_id()
                && identity.failed.attempt_id() < identity.replacement.attempt_id()
                && self
                    .prepared
                    .get(&identity.failed)
                    .is_some_and(|p| p.schedule.execution() == identity.failed)
                && self.prepared.get(&identity.replacement).is_some_and(|p| {
                    p.schedule.execution() == identity.replacement && p.binding_complete
                });
            if !known {
                return Err(protocol(
                    "qualification cleanup has no exact original prepared identity",
                ));
            }
            let (cancellation, _) = watch::channel(false);
            self.qualifications.insert(
                identity.replacement,
                QualificationOriginal {
                    identity,
                    operation,
                    dispatch_started: false,
                    cancellation_issued: false,
                    activation_in_flight: false,
                    resources_retiring: false,
                    dispatch_settled: false,
                    cancellation,
                    qualified: None,
                    active: None,
                    activation: None,
                    abandoned: false,
                },
            );
        }
        let original = self
            .qualifications
            .get_mut(&identity.replacement)
            .expect("checked original qualification slot");
        if original.identity != identity
            || operation.is_some_and(|op| original.operation.is_some_and(|old| old != op))
        {
            return Err(qualification_error(
                ReplacementQualificationFailure::MismatchedReceipt,
            ));
        }
        if original.operation.is_none() {
            original.operation = operation;
        }
        Ok(original)
    }
    fn close_qualification(
        &mut self,
        initial: AttemptActivationIdentity,
        identity: QualificationIdentity,
        operation: Option<TaskOperationId>,
        abandon: bool,
    ) -> Result<QualificationClose, QueryExecutionError> {
        let original = self.ensure_qualification(initial, identity, operation)?;
        original.cancellation_issued = true;
        let cancellation = original.cancellation.clone();
        if abandon {
            if original.activation_in_flight || original.resources_retiring {
                return Err(panic_error(
                    "original qualification resource operation remains unproven",
                ));
            }
            original.abandoned = true;
            original.resources_retiring = original.qualified.is_some() || original.active.is_some();
            Ok((
                original.qualified.take(),
                original.active.take(),
                cancellation,
            ))
        } else {
            Ok((None, None, cancellation))
        }
    }
    fn begin_qualification_dispatch(
        &mut self,
        initial: AttemptActivationIdentity,
        identity: QualificationIdentity,
        operation: TaskOperationId,
    ) -> Result<watch::Receiver<bool>, QueryExecutionError> {
        let original = self.ensure_qualification(initial, identity, Some(operation))?;
        if original.abandoned || original.cancellation_issued || *original.cancellation.borrow() {
            return Err(qualification_error(
                ReplacementQualificationFailure::Rejected,
            ));
        }
        if original.dispatch_started {
            return Err(qualification_error(
                ReplacementQualificationFailure::InvalidReservation,
            ));
        }
        original.dispatch_started = true;
        // No submitted receipt exists yet. This remains an exact unissued
        // disposition until the original submit boundary below.
        original.dispatch_settled = true;
        Ok(original.cancellation.subscribe())
    }
    fn inherited_terminal_cut(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Option<(MonotonicInstant, MonotonicInstant)> {
        self.prepared
            .get(&activation.execution())
            .and_then(|prepared| prepared.terminal_cut)
            .filter(|(identity, _, _)| *identity == activation)
            .map(|(_, cut, deadline)| (cut, deadline))
    }
}

/// Owns application originals only. It never creates an identity or recovery decision.
/// Construction consumes references to the originals already established before install.
pub struct NativeExecutor {
    replacements: Option<Arc<dyn ReplacementQualificationEffectPort>>,
    installed: Arc<dyn crate::api::NativeInstalledAttemptPort>,
    body: Arc<AsyncMutex<OriginalNativePreparation>>,
    original_exit: Weak<OriginalLogicalExit>,
    registry: LogicalExecutionRuntimeRegistryHandle,
    registration: Arc<LogicalExecutionRegistration>,
    initial_activation: AttemptActivationIdentity,
    frontend_process: FrontendProcessId,
    clock: Arc<dyn MonotonicClock>,
    cancellation: CancellationView,
    storage: Mutex<NativeStorage>,
    tail_ready: futures::task::AtomicWaker,
}
impl NativeExecutor {
    /// Adopt only an already successful original output for physical cleanup.
    /// None means no pending output was present, never that resources exited.
    /// The passive identity comes from the original Activate action, while the
    /// actual owner/rows come from its same Pending and original custody slot.
    pub fn adopt_pending_activation_for_cleanup(
        &self,
    ) -> LogicalFuture<'_, Result<Option<AttemptActivationIdentity>, QueryExecutionError>> {
        Box::pin(async move {
            let mut body = self.body.lock().await;
            let Some(pending) = body.pending_activated.as_ref() else {
                return Ok(None);
            };
            let execution = pending.staged_execution().ok_or_else(|| {
                super::super::PendingNativeCleanupRefusal::NotStaged.query_error()
            })?;
            let activation = {
                let storage = self.lock();
                let activation = storage
                    .prepared
                    .get(&execution)
                    .and_then(|original| original.activation_started)
                    .ok_or_else(|| {
                        protocol("staged cleanup has no original Activate action identity")
                    })?;
                if storage.active.contains_key(&activation_key(activation)) {
                    return Err(protocol(
                        "staged cleanup already has an active original owner",
                    ));
                }
                activation
            };
            let original_exit = self.original_exit.upgrade().ok_or_else(|| {
                super::super::PendingNativeCleanupRefusal::ForeignSource.query_error()
            })?;
            let (owner, rows) = self
                .registry
                .take_pending_native_for_cleanup(
                    &self.registration,
                    &original_exit,
                    &self.body,
                    &mut body.pending_activated,
                )
                .map_err(super::super::PendingNativeCleanupRefusal::query_error)?;
            let mut storage = self.lock();
            let terminal_cut = storage.inherited_terminal_cut(activation);
            if storage.active.contains_key(&activation_key(activation)) {
                // The unique original body loan excludes a second transfer.
                // A carrier invariant must not replace/drop an existing owner.
                std::mem::forget((owner, rows));
                std::process::abort();
            }
            storage.active.insert(
                activation_key(activation),
                ActiveOriginal {
                    owner: Arc::new(AsyncMutex::new(owner)),
                    read: rows.as_ref().map(OriginalRootRead::from_rows),
                    rows,
                    terminal_cut,
                    streamed_success: false,
                    computation_ready: None,
                    run_started: false,
                    resources_exited: false,
                    cleanup_adopted: true,
                    dormant_retired_for_cleanup: false,
                    cleanup_failure: Arc::new(CleanupFailure::default()),
                },
            );
            // No AttemptActivated, registry activation, dormant-converged or
            // local/remote exit fact is emitted by this cleanup-only transfer.
            Ok(Some(activation))
        })
    }
    /// Physically retire only the original dormant wrapper left beside an
    /// adopted output. No converge/RPC is rerun and no remote fact is emitted.
    /// An earlier unproven Drop stays unproven; owner absence is not success.
    pub fn retire_adopted_dormant_for_cleanup(
        &self,
        activation: AttemptActivationIdentity,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async move {
            let mut body = self.body.lock().await;
            {
                let storage = self.lock();
                let original = storage
                    .active
                    .get(&activation_key(activation))
                    .filter(|original| original.cleanup_adopted)
                    .ok_or_else(|| {
                        protocol("dormant cleanup has no exact adopted original owner")
                    })?;
                if original.dormant_retired_for_cleanup {
                    return Ok(());
                }
                if storage.dormant_execution != Some(activation.execution()) {
                    return Err(protocol(
                        "adopted dormant cleanup has another original execution",
                    ));
                }
            }
            let causes = self.causes(activation.execution(), 24);
            match catch_unwind(AssertUnwindSafe(|| body.dormant.retire())) {
                Ok(Ok(())) => {
                    let mut storage = self.lock();
                    storage
                        .active
                        .get_mut(&activation_key(activation))
                        .expect("same original adopted owner")
                        .dormant_retired_for_cleanup = true;
                    storage.dormant_execution = None;
                    Ok(())
                }
                Ok(Err(())) => Err(panic_error(
                    "original adopted dormant Drop remains unproven",
                )),
                Err(original) => {
                    causes.retain(original, true);
                    Err(panic_error("original adopted dormant Drop panicked"))
                }
            }
        })
    }
    /// Notify the same original outer owner without replacing Ready, result
    /// delivery, a physical Join, or encoding this typed cause as a panic.
    pub fn record_owner_failure(&self, error: QueryExecutionError) -> bool {
        self.owner_failure_observer().report(error)
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "the constructor receives distinct original owners"
    )]
    pub(in crate::coordination) fn new(
        replacements: Option<Arc<dyn ReplacementQualificationEffectPort>>,
        installed: Arc<dyn crate::api::NativeInstalledAttemptPort>,
        body: Arc<AsyncMutex<OriginalNativePreparation>>,
        original_exit: Weak<OriginalLogicalExit>,
        registry: LogicalExecutionRuntimeRegistryHandle,
        registration: Arc<LogicalExecutionRegistration>,
        initial_activation: AttemptActivationIdentity,
        initial_schedule: Arc<AttemptSchedule>,
        frontend_process: FrontendProcessId,
        clock: Arc<dyn MonotonicClock>,
        cancellation: CancellationView,
    ) -> Self {
        let mut storage = NativeStorage {
            dormant_execution: Some(initial_schedule.execution()),
            ..NativeStorage::default()
        };
        storage
            .preparations
            .insert(initial_schedule.execution(), PreparationPhase::Prepared);
        storage.prepared.insert(
            initial_schedule.execution(),
            PreparedAttempt {
                schedule: initial_schedule,
                preparation_deadline: None,
                terminal_cut: None,
                binding_complete: true,
                activation_started: None,
            },
        );
        Self {
            replacements,
            installed,
            body,
            original_exit,
            registry,
            registration,
            initial_activation,
            frontend_process,
            clock,
            cancellation,
            storage: Mutex::new(storage),
            tail_ready: futures::task::AtomicWaker::new(),
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, NativeStorage> {
        self.storage.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Original recorded metadata only; absence grants no exit or promise.
    pub fn original_computation_ready(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<Option<crate::api::ComputationReadyToken>, QueryExecutionError> {
        let original = self
            .lock()
            .active
            .get(&activation_key(activation))
            .and_then(|row| row.computation_ready);
        if original.is_some_and(|cut| cut.activation() != activation) {
            return Err(protocol(
                "original recorded computation cut has a foreign activation",
            ));
        }
        Ok(original)
    }
    fn causes(&self, execution: QueryExecutionId, kind: u8) -> Arc<FutureCauses> {
        Arc::clone(self.lock().causes.entry((execution, kind)).or_default())
    }
    fn absolute(&self, deadline: MonotonicInstant) -> Instant {
        absolute_deadline(self.clock.as_ref(), deadline)
    }
    fn schedule(
        &self,
        execution: QueryExecutionId,
    ) -> Result<Arc<AttemptSchedule>, QueryExecutionError> {
        self.lock()
            .prepared
            .get(&execution)
            .map(|p| Arc::clone(&p.schedule))
            .ok_or_else(|| protocol("Native action has no original prepared schedule"))
    }
    pub(crate) fn take_rows(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Result<Option<NativeRowsAttemptRuntime>, QueryExecutionError> {
        self.lock()
            .active
            .get_mut(&activation_key(activation))
            .map(|active| active.rows.take())
            .ok_or_else(|| protocol("Native rows belong to another activation"))
    }
    fn original_native_failed(&self, execution: QueryExecutionId) -> bool {
        self.lock()
            .causes
            .range((execution, 0)..=(execution, u8::MAX))
            .any(|(_, causes)| causes.failed())
    }
    pub fn has_unhandled_failure(&self) -> bool {
        let storage = self.lock();
        storage.causes.values().any(|cause| cause.failed())
            || storage.active.values().any(|active| {
                active
                    .cleanup_failure
                    .reported
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_some()
            })
            || storage.tails.values().any(|tail| tail.failed())
    }
    /// Shares only the already-allocated original cause slot with health custody.
    /// The packet has no strong edge back to this executor or the registry.
    pub fn failure_source(&self) -> Option<Arc<dyn Error + Send + Sync>> {
        let storage = self.lock();
        for causes in storage.causes.values() {
            if causes.failed() {
                return Some(Arc::clone(causes) as Arc<dyn Error + Send + Sync>);
            }
        }
        for active in storage.active.values() {
            if active
                .cleanup_failure
                .reported
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some()
            {
                return Some(Arc::clone(&active.cleanup_failure) as Arc<dyn Error + Send + Sync>);
            }
        }
        for tail in storage.tails.values() {
            if tail.failed() {
                return Some(Arc::clone(tail) as Arc<dyn Error + Send + Sync>);
            }
        }
        None
    }
    pub fn execute(&self, effect: NativeEffect) -> LogicalFuture<'_, NativeFact> {
        let (execution, kind, run) = match &effect {
            NativeEffect::ActivateAttempt { activation, .. } => (activation.execution(), 10, false),
            NativeEffect::RunAttempt { activation } => (activation.execution(), 11, true),
            NativeEffect::PrepareSuccessor { replacement, .. } => (*replacement, 12, false),
            NativeEffect::DispatchQualification { identity, .. } => {
                (identity.replacement, 13, false)
            }
            NativeEffect::ActivateReservation { identity, .. } => (identity.replacement, 14, false),
            NativeEffect::CancelQualification { identity, .. } => (identity.replacement, 15, false),
            NativeEffect::AbandonReservation { identity } => (identity.replacement, 16, false),
            NativeEffect::SealRootRead { activation, .. } => (activation.execution(), 24, false),
            NativeEffect::FinishResources { activation } => (activation.execution(), 17, false),
            NativeEffect::AbandonResources { activation } => (activation.execution(), 18, false),
            NativeEffect::ConvergeDormant { execution, .. } => (*execution, 19, false),
            NativeEffect::RecordTerminalCut { activation, .. } => {
                (activation.execution(), 20, false)
            }
            NativeEffect::RecordStreamingSuccess { activation } => {
                (activation.execution(), 21, false)
            }
            NativeEffect::RetirePreparation => (self.registration.initial_execution(), 22, false),
            NativeEffect::RecordComputationReady { cut } => (cut.activation.execution(), 23, false),
        };
        let run_activation = match &effect {
            NativeEffect::RunAttempt { activation } => Some(*activation),
            _ => None,
        };
        Box::pin(async move {
            let causes = self.causes(execution, kind);
            let mut result = None;
            let outcome = poll_original(self.execute_inner(effect), &causes, None, None, |fact| {
                result = Some(fact)
            })
            .await;
            if run && causes.failed() {
                let activation =
                    run_activation.expect("RunAttempt carries its original activation");
                return match result {
                    Some(NativeFact::Failed { failure, .. })
                    | Some(NativeFact::NativeRunFailed { failure, .. }) => {
                        NativeFact::NativeRunFailed {
                            activation,
                            failure,
                        }
                    }
                    _ => NativeFact::NativeRunFailed {
                        activation,
                        failure: report(panic_error("Native attempt run panicked")),
                    },
                };
            }
            match (outcome, result) {
                (Ok(()), Some(fact)) => fact,
                _ if run => NativeFact::NativeRunFailed {
                    activation: run_activation.expect("RunAttempt carries its original activation"),
                    failure: report(panic_error("Native attempt run panicked")),
                },
                _ => failed(execution, panic_error("Native action panicked")),
            }
        })
    }
    fn execute_inner(&self, effect: NativeEffect) -> LogicalFuture<'_, NativeFact> {
        Box::pin(async move {
            match effect {
                NativeEffect::ActivateAttempt {
                    activation,
                    inherited_admissions,
                } => self.activate(activation, inherited_admissions).await,
                NativeEffect::RunAttempt { activation } => self.run(activation).await,
                NativeEffect::SealRootRead { activation, sealed } => {
                    self.seal_root_read(activation, sealed).await
                }
                NativeEffect::PrepareSuccessor {
                    failed: _,
                    replacement,
                    topology,
                    deadline,
                } => self.prepare(replacement, topology, deadline).await,
                NativeEffect::DispatchQualification {
                    identity,
                    operation_id,
                    failed_participations,
                    replacement_participations,
                    issued_at,
                    expires_at,
                } => {
                    self.qualify(
                        identity,
                        operation_id,
                        failed_participations,
                        replacement_participations,
                        issued_at,
                        expires_at,
                    )
                    .await
                }
                NativeEffect::ActivateReservation {
                    identity,
                    operation_id,
                } => self.activate_reservation(identity, operation_id),
                NativeEffect::CancelQualification {
                    identity,
                    operation_id,
                } => {
                    let closed = {
                        let mut storage = self.lock();
                        storage.close_qualification(
                            self.initial_activation,
                            identity,
                            Some(operation_id),
                            false,
                        )
                    };
                    match closed {
                        Ok((_, _, cancellation)) => {
                            cancellation.send_replace(true);
                            NativeFact::Applied
                        }
                        Err(error) => failed(identity.replacement, error),
                    }
                }
                NativeEffect::AbandonReservation { identity } => {
                    self.abandon_qualification(identity)
                }
                NativeEffect::FinishResources { activation } => {
                    self.finish_resources(activation, true).await
                }
                NativeEffect::AbandonResources { activation } => {
                    self.finish_resources(activation, false).await
                }
                NativeEffect::ConvergeDormant {
                    execution,
                    remote_deadline: _,
                } => self.cleanup_dormant(execution).await,
                NativeEffect::RecordTerminalCut {
                    activation,
                    cut,
                    remote_deadline,
                } => {
                    match self.lock().record_terminal_cut(
                        self.initial_activation,
                        activation,
                        cut,
                        remote_deadline,
                    ) {
                        Ok(()) => NativeFact::Applied,
                        Err(error) => failed(activation.execution(), error),
                    }
                }
                NativeEffect::RecordStreamingSuccess { activation } => {
                    let mut storage = self.lock();
                    match storage.active.get_mut(&activation_key(activation)) {
                        Some(active) => {
                            active.streamed_success = true;
                            NativeFact::Applied
                        }
                        None => failed(
                            activation.execution(),
                            protocol("streaming success has no original active owner"),
                        ),
                    }
                }
                NativeEffect::RecordComputationReady { cut } => {
                    let token = crate::api::ComputationReadyToken::from_runtime_cut(cut);
                    {
                        let mut storage = self.lock();
                        if !storage
                            .prepared
                            .get(&cut.activation.execution())
                            .is_some_and(|p| p.schedule.root() == cut.root)
                        {
                            return failed(
                                cut.activation.execution(),
                                protocol("computation cut has another original root"),
                            );
                        }
                        let Some(active) = storage.active.get_mut(&activation_key(cut.activation))
                        else {
                            return failed(
                                cut.activation.execution(),
                                protocol("computation cut has no exact original activation"),
                            );
                        };
                        if active
                            .computation_ready
                            .is_some_and(|original| original != token)
                        {
                            return failed(
                                cut.activation.execution(),
                                protocol(
                                    "computation readiness differs from its original frozen cut",
                                ),
                            );
                        }
                        active.computation_ready = Some(token);
                        let terminal = active
                            .terminal_cut
                            .get_or_insert((cut.cut, cut.convergence_deadline));
                        terminal.1 = terminal.1.min(cut.convergence_deadline);
                    }
                    let causes = self.causes(cut.activation.execution(), 25);
                    let installed = &self.installed;
                    let future =
                        match catch_unwind(AssertUnwindSafe(move || installed.record_ready(token)))
                        {
                            Ok(future) => future,
                            Err(payload) => {
                                causes.retain(payload, false);
                                return failed(
                                    cut.activation.execution(),
                                    panic_error("Native computation-ready registration panicked"),
                                );
                            }
                        };
                    let mut returned = None;
                    let outcome = poll_original(future, &causes, None, None, |result| {
                        returned = Some(result)
                    })
                    .await;
                    match returned {
                        Some(Err(error)) => failed(cut.activation.execution(), error),
                        Some(Ok(())) if outcome.is_ok() && !causes.failed() => NativeFact::Applied,
                        _ => failed(
                            cut.activation.execution(),
                            panic_error("Native computation-ready registration remains unproven"),
                        ),
                    }
                }
                NativeEffect::RetirePreparation => {
                    let mut body = self.body.lock().await;
                    let Some(session) = body.session.as_mut() else {
                        return failed(
                            self.registration.initial_execution(),
                            protocol("original Native session is absent"),
                        );
                    };
                    let causes = self.causes(self.registration.initial_execution(), 5);
                    match catch_unwind(AssertUnwindSafe(|| session.retire_preparation())) {
                        Ok(()) => NativeFact::Applied,
                        Err(payload) => {
                            causes.retain(payload, false);
                            failed(
                                self.registration.initial_execution(),
                                panic_error("Native preparation destruction panicked"),
                            )
                        }
                    }
                }
            }
        })
    }

    async fn activate(
        &self,
        activation: AttemptActivationIdentity,
        inherited: Vec<novarocks_coordination_contract::AttemptInheritedAdmission>,
    ) -> NativeFact {
        let execution = activation.execution();
        let schedule = match self.schedule(execution) {
            Ok(schedule) => schedule,
            Err(error) => return failed(execution, error),
        };
        if self.lock().active.contains_key(&activation_key(activation)) {
            return failed(
                execution,
                protocol("Native activation already owns its original active slot"),
            );
        }
        {
            let storage = self.lock();
            let original = storage
                .prepared
                .get(&execution)
                .expect("the original schedule came from this prepared record");
            if original.activation_started.is_some() {
                return failed(
                    execution,
                    protocol("the original Activate action was already issued"),
                );
            }
        }
        let mut reservation = match self.registry.reserve_active_native_owner(
            &self.registration,
            execution,
            schedule.participations(),
        ) {
            Ok(reservation) => reservation,
            Err(error) => {
                return failed(
                    execution,
                    projected(error.failure_fact(), error.safe_detail()),
                );
            }
        };
        let admissions = {
            let mut storage = self.lock();
            match storage.qualifications.get_mut(&execution) {
                Some(original)
                    if original.identity.replacement == execution
                        && original.identity.actor == activation.actor()
                        && original.identity.generation == activation.generation() =>
                {
                    if original.abandoned
                        || original.cancellation_issued
                        || *original.cancellation.borrow()
                    {
                        return failed(
                            execution,
                            qualification_error(ReplacementQualificationFailure::Rejected),
                        );
                    }
                    let Some(resources) = original.active.as_mut() else {
                        return failed(
                            execution,
                            protocol(
                                "replacement activation has no original active isolation owner",
                            ),
                        );
                    };
                    let Some(admissions) = resources.take_admissions() else {
                        return failed(
                            execution,
                            protocol(
                                "original replacement admission evidence was already consumed",
                            ),
                        );
                    };
                    let matches = admissions.len() == inherited.len()
                        && admissions
                            .iter()
                            .zip(inherited.iter())
                            .all(|(original, passive)| {
                                original.participation() == passive.participation
                                    && original.receipt() == passive.receipt
                            });
                    if !matches {
                        let _ = resources.restore_admissions(admissions);
                        return failed(
                            execution,
                            protocol(
                                "replacement initialization differs from original admission evidence",
                            ),
                        );
                    }
                    original.activation = Some(activation);
                    Some(admissions)
                }
                _ if inherited.is_empty() && activation == self.initial_activation => None,
                _ => {
                    return failed(
                        execution,
                        protocol("inherited admissions have no original isolation owner"),
                    );
                }
            }
        };
        self.lock()
            .prepared
            .get_mut(&execution)
            .expect("validated original Activate schedule")
            .activation_started = Some(activation);
        let causes = self.causes(execution, 0);
        let mut body = self.body.lock().await;
        let OriginalNativePreparation {
            dormant,
            pending_activated,
            ..
        } = &mut *body;
        if pending_activated.is_some() {
            return failed(
                execution,
                protocol("an original activation output is already staged"),
            );
        }
        let Some((owner, original_panic, original_health)) = dormant.activation_parts() else {
            return failed(
                execution,
                protocol("original dormant activation owner is unavailable"),
            );
        };
        let future = match start_activation(
            owner,
            activation,
            &schedule,
            admissions,
            self.cancellation.clone(),
            original_panic,
        ) {
            Ok(future) => future,
            Err(error) => return failed(execution, error),
        };
        let mut refusal = None;
        let outcome = poll_original(future, &causes, None, None, |result| match result {
            Ok(activated) => reservation.receive_activated_in(pending_activated, activated),
            Err(error) => match error.into_result() {
                Ok(original) => refusal = Some(original),
                Err(original) => {
                    assert!(
                        original_health.is_none(),
                        "one original activation health result"
                    );
                    *original_health =
                        Some(crate::api::DormantConvergenceFailure::Reported(original));
                }
            },
        })
        .await;
        if let Some(original) = dormant.convergence_failure().cloned() {
            let detail = original.detail();
            drop(body);
            self.registry.record_process_failure(original);
            return failed(
                execution,
                QueryExecutionError::new(QueryExecutionErrorKind::Failed, detail),
            );
        }
        if let Some(failure) = refusal {
            return NativeFact::Failed {
                execution,
                failure: FailureReport::from_attempt(&failure),
            };
        }
        if outcome.is_err() || causes.failed() {
            // If Ready succeeded, the same output is still in its original stable slot.
            if pending_activated.is_some() {
                let _ =
                    reservation.stage_activated_in(activation, schedule.root(), pending_activated);
            }
            return failed(execution, panic_error("Native attempt activation panicked"));
        }
        if let Err(error) =
            reservation.stage_activated_in(activation, schedule.root(), pending_activated)
        {
            return failed(execution, error);
        }
        match catch_unwind(AssertUnwindSafe(|| dormant.retire())) {
            Ok(Ok(())) => (),
            Ok(Err(())) => {
                return failed(
                    execution,
                    panic_error("Native dormant owner destruction is unproven"),
                );
            }
            Err(payload) => {
                causes.retain(payload, false);
                return failed(
                    execution,
                    panic_error("Native dormant owner destruction panicked"),
                );
            }
        }
        let (owner, rows) = match reservation.finish_activated_in(pending_activated) {
            Ok(parts) => parts,
            Err(error) => return failed(execution, error),
        };
        let root = rows.as_ref().map(|_| schedule.root());
        {
            let mut storage = self.lock();
            let terminal_cut = storage.inherited_terminal_cut(activation);
            storage.dormant_execution = None;
            storage.active.insert(
                activation_key(activation),
                ActiveOriginal {
                    owner: Arc::new(AsyncMutex::new(owner)),
                    read: rows.as_ref().map(OriginalRootRead::from_rows),
                    rows,
                    terminal_cut,
                    streamed_success: false,
                    computation_ready: None,
                    run_started: false,
                    resources_exited: false,
                    cleanup_adopted: false,
                    dormant_retired_for_cleanup: false,
                    cleanup_failure: Arc::new(CleanupFailure::default()),
                },
            );
        }
        if let Err(error) = self
            .registry
            .register_runtime_activation(&self.registration, activation)
        {
            return failed(
                execution,
                projected(error.failure_fact(), error.safe_detail()),
            );
        }
        NativeFact::AttemptActivated { activation, root }
    }

    async fn run(&self, activation: AttemptActivationIdentity) -> NativeFact {
        let exited = {
            let mut storage = self.lock();
            let Some(active) = storage.active.get_mut(&activation_key(activation)) else {
                return failed(
                    activation.execution(),
                    protocol("Native run has no exact original activation"),
                );
            };
            if active.run_started {
                return failed(
                    activation.execution(),
                    protocol("Native run was already issued"),
                );
            }
            active.run_started = true;
            active.resources_exited
        };
        if exited {
            return NativeFact::RunCompleted { activation };
        }
        let causes = self.causes(activation.execution(), 1);
        let installed = &self.installed;
        let future = match catch_unwind(AssertUnwindSafe(move || installed.start(activation))) {
            Ok(future) => future,
            Err(payload) => {
                causes.retain(payload, false);
                return NativeFact::NativeRunFailed {
                    activation,
                    failure: report(panic_error("Native attempt run panicked")),
                };
            }
        };
        let mut result = None;
        let outcome =
            poll_original(future, &causes, None, None, |value| result = Some(value)).await;
        let supervision_failed = outcome.is_err() || causes.failed();
        if supervision_failed && let Some(source) = self.original_exit.upgrade() {
            for cleanup in [false, true] {
                let mut payload = if cleanup {
                    &causes.cleanup
                } else {
                    &causes.primary
                }
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
                source.retain_run_panic_silent(&mut payload, cleanup);
                if let Some(payload) = payload {
                    causes.retain(payload, cleanup);
                }
            }
        }
        project_start_result(activation, result, supervision_failed)
    }

    async fn prepare(
        &self,
        execution: QueryExecutionId,
        topology: SuccessorTopology,
        deadline: MonotonicInstant,
    ) -> NativeFact {
        {
            let mut storage = self.lock();
            if storage.preparations.contains_key(&execution) || storage.dormant_execution.is_some()
            {
                return failed(
                    execution,
                    protocol("Native successor is already prepared or preparing"),
                );
            }
            storage
                .preparations
                .insert(execution, PreparationPhase::Pending);
        }
        let result = self.prepare_original(execution, topology, deadline).await;
        let phase = if matches!(&result, NativeFact::SuccessorPrepared { .. }) {
            PreparationPhase::Prepared
        } else {
            PreparationPhase::Failed
        };
        self.lock().preparations.insert(execution, phase);
        result
    }
    async fn prepare_original(
        &self,
        execution: QueryExecutionId,
        topology: SuccessorTopology,
        deadline: MonotonicInstant,
    ) -> NativeFact {
        let causes = self.causes(execution, 2);
        let mut body = self.body.lock().await;
        let OriginalNativePreparation {
            session, dormant, ..
        } = &mut *body;
        let Some(session) = session.as_mut() else {
            return failed(execution, protocol("original Native session is absent"));
        };
        let requirement = match topology {
            SuccessorTopology::LiveSnapshot => NativeAttemptTopologyRequirement::LiveSnapshot,
            SuccessorTopology::ExcludeProcess(process) => {
                NativeAttemptTopologyRequirement::ExcludeProcess(process)
            }
        };
        if let Err(error) = session.begin_preparation(execution, requirement) {
            return failed(
                execution,
                projected(error.failure_fact(), error.safe_detail()),
            );
        }
        let session_owner = &mut *session;
        let future = match catch_unwind(AssertUnwindSafe(move || {
            let owner = session_owner;
            owner.prepare_pending()
        })) {
            Ok(future) => future,
            Err(payload) => {
                causes.retain(payload, false);
                return failed(
                    execution,
                    panic_error("Native replacement preparation panicked"),
                );
            }
        };
        let mut prepared = None;
        let outcome = poll_original(
            future,
            &causes,
            Some(&self.cancellation),
            Some(self.absolute(deadline).into()),
            |result| prepared = Some(result),
        )
        .await;
        if let Err(error) = outcome {
            return failed(
                execution,
                wait_error(
                    error,
                    "Native replacement preparation was cancelled",
                    "Native replacement preparation exceeded its recovery budget",
                    "Native replacement preparation panicked",
                ),
            );
        }
        match prepared {
            Some(Ok(())) => (),
            Some(Err(NativeAttemptPreparationError::Contract(error))) => {
                return failed(
                    execution,
                    projected(error.failure_fact(), error.safe_detail()),
                );
            }
            Some(Err(NativeAttemptPreparationError::Runtime(failure))) => {
                return NativeFact::Failed {
                    execution,
                    failure: FailureReport::from_attempt(&failure),
                };
            }
            None => {
                return failed(
                    execution,
                    protocol("Native preparation returned no original result"),
                );
            }
        }
        if causes.failed() {
            return failed(
                execution,
                panic_error("Native replacement preparation destruction panicked"),
            );
        }
        let facts = match session.accept_pending_in(dormant) {
            Ok(facts) => facts,
            Err(error) => {
                return failed(
                    execution,
                    projected(error.failure_fact(), error.safe_detail()),
                );
            }
        };
        self.lock().dormant_execution = Some(execution);
        let schedule = match build_attempt_schedule(
            execution,
            self.frontend_process,
            &facts.eligible_backends,
            &facts.scheduling,
            #[cfg(novarocks_dev_controls)]
            dormant.as_mut().test_preferred_backend_process(execution),
        ) {
            Ok(schedule) => Arc::new(schedule),
            Err(error) => {
                return failed(
                    execution,
                    QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string()),
                );
            }
        };
        {
            let mut storage = self.lock();
            storage.prepared.insert(
                execution,
                PreparedAttempt {
                    schedule: Arc::clone(&schedule),
                    preparation_deadline: Some(deadline),
                    terminal_cut: None,
                    binding_complete: false,
                    activation_started: None,
                },
            );
        }
        let binding_causes = self.causes(execution, 3);
        if let Err(error) = bind_original_schedule(
            dormant,
            &schedule,
            &self.cancellation,
            self.absolute(deadline).into(),
            &binding_causes,
        )
        .await
        {
            let original_health = dormant.convergence_failure().cloned();
            drop(body);
            if let Some(original) = original_health {
                self.registry.record_process_failure(original);
            }
            return failed(execution, error);
        }
        let participations = Arc::from(schedule.participations());
        self.lock()
            .prepared
            .get_mut(&execution)
            .expect("same original bound successor slot")
            .binding_complete = true;
        NativeFact::SuccessorPrepared {
            execution,
            participations,
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "all fields belong to one immutable qualification request"
    )]
    async fn qualify(
        &self,
        identity: QualificationIdentity,
        operation: TaskOperationId,
        failed_participations: Arc<[novarocks_execution_contract::ParticipationRef]>,
        replacement_participations: Arc<[novarocks_execution_contract::ParticipationRef]>,
        issued_at: MonotonicInstant,
        expires_at: MonotonicInstant,
    ) -> NativeFact {
        let rejected = |error| NativeFact::QualificationSettled {
            identity,
            operation_id: operation,
            dispatched_at: None,
            settlement: QualificationSettlement::Rejected(error),
        };
        if !self
            .lock()
            .prepared
            .get(&identity.replacement)
            .is_some_and(|original| {
                original.binding_complete
                    && original
                        .preparation_deadline
                        .is_some_and(|deadline| expires_at <= deadline)
            })
        {
            return rejected(protocol(
                "qualification has no completed original preparation and dependency binding",
            ));
        }
        let mut cancel_rx = match self.lock().begin_qualification_dispatch(
            self.initial_activation,
            identity,
            operation,
        ) {
            Ok(receiver) => receiver,
            Err(error) => return rejected(error),
        };
        let native_actions = {
            let body = self.body.lock().await;
            if body.session.is_none() {
                return rejected(protocol("original Native session is absent"));
            }
            body.native_actions.clone()
        };
        let port = self.replacements.clone();
        let Some(port) = port else {
            return rejected(qualification_error(
                ReplacementQualificationFailure::EffectOwnerClosed,
            ));
        };
        let original_identity = ReplacementQualificationIdentity::new(
            identity.actor,
            identity.failed,
            identity.replacement,
            identity.generation,
        );
        let request = Arc::new(ReplacementQualificationRequest::from_logical(
            operation,
            original_identity,
            failed_participations,
            replacement_participations,
            issued_at,
            expires_at,
            self.absolute(expires_at),
            native_actions,
        ));
        let causes = self.causes(identity.replacement, 4);
        let mut capacity = port.subscribe_capacity();
        let absolute: tokio::time::Instant = request.absolute_expiry().into();
        let reservation = loop {
            if *cancel_rx.borrow() {
                return rejected(qualification_error(
                    ReplacementQualificationFailure::Rejected,
                ));
            }
            if self.clock.now().has_reached(expires_at) {
                return rejected(qualification_error(
                    ReplacementQualificationFailure::Expired,
                ));
            }
            match catch_unwind(AssertUnwindSafe(|| port.try_reserve(&request))) {
                Ok(ReplacementQualificationEffectAdmission::Admitted(reservation)) => {
                    break reservation;
                }
                Ok(ReplacementQualificationEffectAdmission::Backpressured) => (),
                Err(payload) => {
                    causes.retain(payload, false);
                    return rejected(panic_error("Native qualification admission panicked"));
                }
            }
            tokio::select! { biased;
                reason=self.cancellation.cancelled()=>return rejected(cancellation_error(reason,"Native replacement qualification was cancelled")),
                ()=tokio::time::sleep_until(absolute)=>return rejected(qualification_error(ReplacementQualificationFailure::Expired)),
                changed=capacity.changed()=>if changed.is_err(){return rejected(qualification_error(ReplacementQualificationFailure::EffectOwnerClosed));},
                _=cancel_rx.changed()=>return rejected(qualification_error(ReplacementQualificationFailure::Rejected)),
            }
        };
        let (send, mut receive) = receipt_channel();
        let dispatched_at = self.clock.now();
        let submission = ReplacementQualificationEffectSubmission::new(request, send, cancel_rx);
        self.lock()
            .qualifications
            .get_mut(&identity.replacement)
            .expect("same original qualification dispatch")
            .dispatch_settled = false;
        let submit = catch_unwind(AssertUnwindSafe(|| reservation.submit(submission)));
        if let Err(payload) = submit {
            causes.retain(payload, false);
        }
        // A submitted effect always settles its original position; cancellation is
        // requested through its original cell and never drops the receipt receiver.
        let mut cancellation_error = None;
        let receipt = {
            tokio::select! { biased;
                receipt=receive.recv()=>receipt,
                reason=self.cancellation.cancelled()=>{
                    let _=reason;
                    let closed = {
                        let mut storage = self.lock();
                        storage.close_qualification(self.initial_activation, identity, Some(operation), false)
                    };
                    match closed {
                        Ok((_, _, cancellation)) => { cancellation.send_replace(true); }
                        Err(error) => cancellation_error = Some(error),
                    }
                    receive.recv().await
                },
                ()=tokio::time::sleep_until(absolute)=>{
                    let closed = {
                        let mut storage = self.lock();
                        storage.close_qualification(self.initial_activation, identity, Some(operation), false)
                    };
                    match closed {
                        Ok((_, _, cancellation)) => { cancellation.send_replace(true); }
                        Err(error) => cancellation_error = Some(error),
                    }
                    receive.recv().await
                },
            }
        };
        let settlement = match receipt {
            Some(receipt) => {
                let (actual_operation, actual_identity, settlement) = receipt.into_parts();
                if actual_operation != operation || actual_identity != original_identity {
                    QualificationSettlement::Rejected(qualification_error(
                        ReplacementQualificationFailure::MismatchedReceipt,
                    ))
                } else {
                    match settlement {
                        ReplacementQualificationSettlement::Failed(
                            ReplacementQualificationFailure::OutcomeUnknown,
                        ) => QualificationSettlement::OutcomeUnknown,
                        ReplacementQualificationSettlement::Failed(reason) => {
                            QualificationSettlement::Rejected(qualification_error(reason))
                        }
                        ReplacementQualificationSettlement::Qualified(qualified) => {
                            let facts = QualifiedReplacementFacts {
                                identity,
                                operation_id: operation,
                                topology_revision: qualified.topology_revision(),
                                admissions: qualified
                                    .admissions()
                                    .iter()
                                    .map(|admission| QualifiedAdmission {
                                        request: admission.request(),
                                        receipt: admission.receipt(),
                                    })
                                    .collect::<Vec<_>>()
                                    .into_boxed_slice(),
                            };
                            let mut storage = self.lock();
                            let original = storage
                                .qualifications
                                .get_mut(&identity.replacement)
                                .expect("the exact original qualification remains retained");
                            original.qualified = Some(qualified);
                            let abandoned = if original.abandoned {
                                original.resources_retiring = true;
                                original.qualified.take()
                            } else {
                                None
                            };
                            drop(storage);
                            drop(abandoned);
                            self.lock()
                                .qualifications
                                .get_mut(&identity.replacement)
                                .expect("same original qualification")
                                .resources_retiring = false;
                            QualificationSettlement::Qualified(facts)
                        }
                    }
                }
            }
            None => QualificationSettlement::OutcomeUnknown,
        };
        self.lock()
            .qualifications
            .get_mut(&identity.replacement)
            .expect("same settled original qualification")
            .dispatch_settled = true;
        // Even a cancellation-contract error cannot discard an already
        // submitted receipt or the original resources it eventually returns.
        let settlement = cancellation_error.map_or(settlement, QualificationSettlement::Rejected);
        NativeFact::QualificationSettled {
            identity,
            operation_id: operation,
            dispatched_at: Some(dispatched_at),
            settlement,
        }
    }

    fn activate_reservation(
        &self,
        identity: QualificationIdentity,
        operation: TaskOperationId,
    ) -> NativeFact {
        let mut storage = self.lock();
        let Some(original) = storage
            .qualifications
            .get_mut(&identity.replacement)
            .filter(|original| {
                original.identity == identity && original.operation == Some(operation)
            })
        else {
            return failed(
                identity.replacement,
                qualification_error(ReplacementQualificationFailure::MismatchedReceipt),
            );
        };
        if original.abandoned
            || original.cancellation_issued
            || *original.cancellation.borrow()
            || original.active.is_some()
        {
            return failed(
                identity.replacement,
                qualification_error(ReplacementQualificationFailure::InvalidReservation),
            );
        }
        if original.activation_in_flight || original.resources_retiring {
            return failed(
                identity.replacement,
                panic_error("original qualification operation remains unproven"),
            );
        }
        let Some(qualified) = original.qualified.take() else {
            return failed(
                identity.replacement,
                qualification_error(ReplacementQualificationFailure::InvalidReservation),
            );
        };
        original.activation_in_flight = true;
        drop(storage);
        // Retain the original wrapper through a foreign panic. Rich Drop and
        // callbacks run outside the original storage lock.
        let mut qualified = std::mem::ManuallyDrop::new(qualified);
        let activated = catch_unwind(AssertUnwindSafe(|| qualified.activate_retained()));
        let qualified = std::mem::ManuallyDrop::into_inner(qualified);
        let mut storage = self.lock();
        let original = storage
            .qualifications
            .get_mut(&identity.replacement)
            .expect("same original reservation activation");
        original.qualified = Some(qualified);
        match activated {
            Ok(result) => {
                original.activation_in_flight = false;
                match result {
                    Ok(activated) => {
                        original.active = Some(ActiveReplacementResources::replacement(activated));
                        NativeFact::ReservationActivated {
                            identity,
                            operation_id: operation,
                        }
                    }
                    Err(reason) => failed(identity.replacement, qualification_error(reason)),
                }
            }
            Err(payload) => {
                drop(storage);
                self.causes(identity.replacement, 14).retain(payload, false);
                failed(
                    identity.replacement,
                    panic_error("original reservation activation panicked"),
                )
            }
        }
    }

    fn abandon_qualification(&self, identity: QualificationIdentity) -> NativeFact {
        let (qualified, active, cancellation) = {
            let mut storage = self.lock();
            match storage.close_qualification(self.initial_activation, identity, None, true) {
                Ok(originals) => originals,
                Err(error) => return failed(identity.replacement, error),
            }
        };
        // The exact cancellation and resource transfer were fixed under
        // storage custody. A reentrant observer can now inspect that decision.
        cancellation.send_replace(true);
        // Keep the same fixed round slot for an already-submitted action's late
        // original result. Only its original opaque resources retire here.
        drop((qualified, active));
        self.lock()
            .qualifications
            .get_mut(&identity.replacement)
            .expect("same retired original qualification")
            .resources_retiring = false;
        NativeFact::Applied
    }
    pub(crate) fn retained_read_root(
        &self,
        activation: AttemptActivationIdentity,
    ) -> Option<TaskIdentity> {
        self.lock()
            .active
            .get(&activation_key(activation))
            .and_then(|active| active.read.as_ref())
            .map(|read| read.root)
    }
    async fn seal_root_read(
        &self,
        activation: AttemptActivationIdentity,
        sealed: novarocks_execution_contract::root_lifetime::RootReadSealed,
    ) -> NativeFact {
        let port = std::future::poll_fn(|cx| {
            let waiter = cx.waker().clone();
            let mut previous = None;
            let result = {
                let mut storage = self.lock();
                let Some(original) = storage
                    .active
                    .get_mut(&activation_key(activation))
                    .and_then(|active| active.read.as_mut())
                else {
                    return Poll::Ready(Err(protocol(
                        "original read close has no exact retained port",
                    )));
                };
                if original.root != sealed.task()
                    || sealed.task().query_execution_id() != activation.execution()
                {
                    return Poll::Ready(Err(protocol("original read close has a foreign root")));
                }
                if original.closing {
                    previous = original.close_waiter.replace(waiter);
                    Poll::Pending
                } else if original.sealed {
                    Poll::Ready(Ok(None))
                } else if original.attempted {
                    Poll::Ready(Err(original.failure.clone().unwrap_or_else(|| {
                        protocol("original read close remains unproven")
                    })))
                } else {
                    // Never repeat a possibly partial opaque callback after refusal or
                    // panic. The original action's custody retains its raw outcome.
                    original.attempted = true;
                    original.closing = true;
                    Poll::Ready(Ok(Some(original.port.clone())))
                }
            };
            drop(previous);
            result
        })
        .await;
        let port = match port {
            Ok(Some(port)) => port,
            Ok(None) => return NativeFact::Applied,
            Err(error) => return failed(activation.execution(), error),
        };
        let _original_progress = OriginalReadCloseProgress {
            native: self,
            activation,
        };
        if let Err(error) = port.seal(sealed) {
            self.lock()
                .active
                .get_mut(&activation_key(activation))
                .and_then(|active| active.read.as_mut())
                .expect("same original refused read close")
                .failure = Some(error.clone());
            return failed(activation.execution(), error);
        }
        self.lock()
            .active
            .get_mut(&activation_key(activation))
            .expect("same original read-close record")
            .read
            .as_mut()
            .expect("same original read port")
            .sealed = true;
        NativeFact::Applied
    }
    async fn finish_resources(
        &self,
        activation: AttemptActivationIdentity,
        finish: bool,
    ) -> NativeFact {
        if self
            .lock()
            .active
            .get(&activation_key(activation))
            .is_some_and(|active| active.read.as_ref().is_some_and(|read| !read.sealed))
        {
            return failed(
                activation.execution(),
                protocol("original root read admission has not closed"),
            );
        }
        let active = {
            let mut storage = self.lock();
            storage
                .active
                .get_mut(&activation_key(activation))
                .map(|active| {
                    (
                        Arc::clone(&active.owner),
                        active.rows.take(),
                        active.terminal_cut,
                        Arc::clone(&active.cleanup_failure),
                        active.resources_exited,
                    )
                })
        };
        let cleanup = if let Some((owner, rows, terminal, reported, exited)) = active {
            if exited {
                return self.retire_isolation(activation, finish, NativeFact::Applied);
            }
            if let Some(failure) = reported
                .reported
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
            {
                return failed(
                    activation.execution(),
                    QueryExecutionError::new(QueryExecutionErrorKind::Failed, failure.detail()),
                );
            }
            let Some(_) = terminal else {
                return failed(
                    activation.execution(),
                    protocol("active cleanup has no original absolute terminal cut"),
                );
            };
            drop(rows);
            let mut original = owner.lock().await;
            let causes = self.causes(activation.execution(), 7);
            if causes.failed() {
                return failed(
                    activation.execution(),
                    panic_error("Native installed resource cleanup remains unproven"),
                );
            }
            let future = match catch_unwind(AssertUnwindSafe(|| {
                self.installed.resources_exited(activation)
            })) {
                Ok(future) => future,
                Err(payload) => {
                    causes.retain(payload, false);
                    return failed(
                        activation.execution(),
                        panic_error("Native installed resource cleanup panicked"),
                    );
                }
            };
            let mut completed = false;
            let outcome = poll_original(future, &causes, None, None, |value| match value {
                Ok(()) => completed = true,
                Err(failure) => {
                    *reported.reported.lock().unwrap_or_else(|e| e.into_inner()) = Some(failure)
                }
            })
            .await;
            let received_failure = reported
                .reported
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(failure) = received_failure {
                let detail = failure.detail();
                drop(original);
                self.registry.record_process_failure(failure);
                return failed(
                    activation.execution(),
                    QueryExecutionError::new(QueryExecutionErrorKind::Failed, detail),
                );
            }
            if outcome.is_err() || causes.failed() || !completed {
                return failed(
                    activation.execution(),
                    panic_error("Native installed resource cleanup is unproven"),
                );
            }
            let progress = original.retire_local();
            if let Some(active) = self.lock().active.get_mut(&activation_key(activation)) {
                active.resources_exited = true;
            }
            drop(original);
            progress.notify_waiters();
            NativeFact::Applied
        } else {
            let dormant = self.lock().dormant_execution;
            if dormant != Some(activation.execution()) {
                return failed(
                    activation.execution(),
                    protocol("resource cleanup has no exact original Native owner"),
                );
            }
            // The deadline controls remote tracking only. Never time out a local
            // original owner and call its resources exited.
            self.cleanup_dormant(activation.execution()).await
        };
        if matches!(cleanup, NativeFact::Failed { .. }) {
            return cleanup;
        }
        self.retire_isolation(activation, finish, cleanup)
    }
    fn retire_isolation(
        &self,
        activation: AttemptActivationIdentity,
        finish: bool,
        cleanup: NativeFact,
    ) -> NativeFact {
        let resources = {
            let mut storage = self.lock();
            match storage.qualifications.get_mut(&activation.execution()) {
                Some(original)
                    if original.identity.actor == activation.actor()
                        && original.identity.replacement == activation.execution()
                        && original.identity.generation == activation.generation() =>
                {
                    if original.activation_in_flight || original.resources_retiring {
                        return failed(
                            activation.execution(),
                            panic_error("original isolation resource operation remains unproven"),
                        );
                    }
                    original.resources_retiring = original.active.is_some();
                    original.active.take()
                }
                None if activation == self.initial_activation => None,
                _ => {
                    return failed(
                        activation.execution(),
                        protocol("resource retirement has another original activation"),
                    );
                }
            }
        };
        if let Some(resources) = resources {
            if finish {
                resources.finish();
            } else {
                drop(resources);
            }
        }
        if let Some(original) = self.lock().qualifications.get_mut(&activation.execution()) {
            original.resources_retiring = false;
        }
        cleanup
    }
    async fn cleanup_dormant(&self, execution: QueryExecutionId) -> NativeFact {
        let (phase, dormant_execution, active) = {
            let storage = self.lock();
            (
                storage.preparations.get(&execution).copied(),
                storage.dormant_execution,
                storage
                    .active
                    .keys()
                    .any(|(_, original, _)| *original == execution),
            )
        };
        if dormant_execution.is_some_and(|original| original != execution) {
            return failed(
                execution,
                protocol("dormant cleanup has another original execution identity"),
            );
        }
        if active || (dormant_execution.is_none() && phase != Some(PreparationPhase::Failed)) {
            return failed(
                execution,
                protocol("dormant cleanup has no explicit original failed preparation"),
            );
        }
        let causes = self.causes(execution, 6);
        let mut body = self.body.lock().await;
        if body.pending_activated.is_some() {
            return failed(
                execution,
                protocol("dormant cleanup still retains a successful original active output"),
            );
        }
        if dormant_execution.is_none() {
            if body.dormant.drop_unproven() || self.original_native_failed(execution) {
                return failed(
                    execution,
                    panic_error("original failed preparation remains unproven"),
                );
            }
            let has_pending = body
                .session
                .as_ref()
                .is_some_and(|session| session.pending_prepared_present());
            if has_pending {
                let OriginalNativePreparation {
                    session, dormant, ..
                } = &mut *body;
                let session = session
                    .as_mut()
                    .expect("pending original output has its session");
                match session.accept_pending_in(dormant) {
                    Ok(facts) if facts.execution == execution => {
                        self.lock().dormant_execution = Some(execution)
                    }
                    Ok(_) => {
                        return failed(
                            execution,
                            protocol("pending cleanup owner has another original execution"),
                        );
                    }
                    Err(error) => {
                        return failed(
                            execution,
                            projected(error.failure_fact(), error.safe_detail()),
                        );
                    }
                }
            } else {
                let retire_causes = self.causes(execution, 8);
                if let Some(session) = body.session.as_mut() {
                    if let Err(payload) =
                        catch_unwind(AssertUnwindSafe(|| session.retire_preparation()))
                    {
                        retire_causes.retain(payload, false);
                        return failed(
                            execution,
                            panic_error("Native failed preparation destruction panicked"),
                        );
                    }
                    if session.preparation_drop_unproven() {
                        return failed(
                            execution,
                            protocol("original failed preparation Drop is unproven"),
                        );
                    }
                }
                // This exact failed action never produced an owner, and its
                // original pending preparation was actually destroyed normally.
                return NativeFact::DormantConverged { execution };
            }
        }
        let custody = &mut body.dormant;
        if custody.convergence_completed() {
            return if custody.has_failure() || self.original_native_failed(execution) {
                failed(
                    execution,
                    panic_error("Native dormant cleanup retains an original failure"),
                )
            } else {
                NativeFact::DormantConverged { execution }
            };
        }
        if let Some(original) = custody.convergence_failure() {
            return failed(
                execution,
                QueryExecutionError::new(QueryExecutionErrorKind::Failed, original.detail()),
            );
        }
        if causes.failed() {
            custody.record_convergence_unproven();
            return failed(
                execution,
                panic_error("original dormant cleanup remains unproven"),
            );
        }
        if custody.convergence_panic().is_some() {
            return failed(
                execution,
                protocol("original dormant cleanup remains unproven"),
            );
        }
        let Some((owner, original)) = custody.convergence_parts() else {
            return failed(
                execution,
                protocol("original dormant cleanup source is unavailable"),
            );
        };
        let cancellation = self.cancellation.clone();
        let future = match catch_unwind(AssertUnwindSafe(move || owner.converge(cancellation))) {
            Ok(future) => future,
            Err(payload) => {
                *original = Some(crate::api::DormantConvergenceFailure::PollPanic(payload));
                return failed(
                    execution,
                    panic_error("Native dormant attempt convergence panicked"),
                );
            }
        };
        let mut succeeded = false;
        let outcome = poll_original(future, &causes, None, None, |result| match result {
            Ok(()) => succeeded = true,
            Err(failure) => {
                *original = Some(crate::api::DormantConvergenceFailure::Reported(failure))
            }
        })
        .await;
        if succeeded && outcome.is_ok() && !causes.failed() {
            custody.record_convergence_completed();
        } else {
            custody.record_convergence_unproven();
        }
        if outcome.is_err()
            || causes.failed()
            || custody.has_failure()
            || self.original_native_failed(execution)
            || !succeeded
        {
            if let Some(original) = custody.convergence_failure() {
                return failed(
                    execution,
                    QueryExecutionError::new(QueryExecutionErrorKind::Failed, original.detail()),
                );
            }
            return failed(
                execution,
                panic_error("Native dormant attempt convergence is unproven"),
            );
        }
        // This is local cleanup only. The logical ledger alone classifies an
        // unissued participation as NeverEstablished; partial activation is possible.
        NativeFact::DormantConverged { execution }
    }
}

async fn bind_original_schedule(
    custody: &mut crate::api::NativeDormantCustody,
    schedule: &AttemptSchedule,
    cancellation: &CancellationView,
    deadline: tokio::time::Instant,
    causes: &FutureCauses,
) -> Result<(), QueryExecutionError> {
    let Some((owner, original, original_health)) = custody.activation_parts() else {
        return Err(protocol(
            "original qualification binding owner is unavailable",
        ));
    };
    let binding = std::future::poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| {
            owner.poll_bind_qualification(schedule, cx)
        })) {
            Ok(Poll::Ready(result)) => {
                Poll::Ready(result.map_err(|failure| failure.into_query_error(original_health)))
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                *original = Some(payload);
                Poll::Ready(Err(panic_error("Native qualification binding panicked")))
            }
        }
    });
    let mut result = None;
    let outcome = poll_original(
        Box::pin(binding),
        causes,
        Some(cancellation),
        Some(deadline),
        |value| result = Some(value),
    )
    .await;
    // The original reported close failure precedes any secondary F Drop
    // failure; both remain retained in their independent original slots.
    if let Some(original) = custody.convergence_failure() {
        return Err(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            original.detail(),
        ));
    }
    outcome.map_err(|error| {
        wait_error(
            error,
            "Native replacement dependency binding was cancelled",
            "Native replacement dependency binding exceeded its recovery budget",
            "Native qualification binding panicked",
        )
    })?;
    result.ok_or_else(|| protocol("Native binding returned no original result"))??;
    if causes.failed() {
        return Err(panic_error(
            "Native qualification binding destruction panicked",
        ));
    }
    Ok(())
}

/// Startup is mechanical work; AP terminal facts arrive through its original port.
/// Protocol closure settles this callback without authoring Ready, successful
/// execution, resource exit or an independent Native failure.
fn project_start_result(
    activation: AttemptActivationIdentity,
    result: Option<Result<NativeAttemptStartupOutcome, QueryExecutionError>>,
    supervision_failed: bool,
) -> NativeFact {
    match result {
        Some(Err(error)) => NativeFact::NativeRunFailed {
            activation,
            failure: report(error),
        },
        Some(Ok(
            NativeAttemptStartupOutcome::Ready | NativeAttemptStartupOutcome::ClosedByProtocol,
        )) if !supervision_failed => NativeFact::RunCompleted { activation },
        _ => NativeFact::NativeRunFailed {
            activation,
            failure: report(if supervision_failed {
                panic_error("Native attempt startup panicked")
            } else {
                protocol("Native original startup returned without its result")
            }),
        },
    }
}
fn start_activation<'a>(
    owner: &'a mut dyn crate::api::DormantNativeAttemptOwner,
    activation_identity: AttemptActivationIdentity,
    schedule: &'a AttemptSchedule,
    admissions: Option<Box<[crate::coordination::ReplacementWorkerAdmissionEvidence]>>,
    cancellation: CancellationView,
    original_panic: &mut Option<PanicPayload>,
) -> Result<crate::api::NativeAttemptActivationFuture<'a>, QueryExecutionError> {
    match catch_unwind(AssertUnwindSafe(move || {
        owner.activate(activation_identity, schedule, admissions, cancellation)
    })) {
        Ok(future) => Ok(future),
        Err(payload) => {
            *original_panic = Some(payload);
            Err(panic_error("Native attempt activation panicked"))
        }
    }
}
fn absolute_deadline(clock: &dyn MonotonicClock, deadline: MonotonicInstant) -> Instant {
    let observed = clock.now();
    let now = Instant::now();
    if deadline >= observed {
        now.checked_add(deadline.saturating_duration_since(observed))
            .unwrap_or(now)
    } else {
        now.checked_sub(observed.saturating_duration_since(deadline))
            .unwrap_or(now)
    }
}
fn projected(
    fact: novarocks_execution_contract::failure::FailureFact,
    detail: SafeDetail,
) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        fact,
        detail,
        ExecutionFailureSite::Coordinator,
    ))
}
fn protocol(detail: &'static str) -> QueryExecutionError {
    projected(
        QueryFailureReason::ResultProtocol.failure_fact(),
        SafeDetail::truncating(detail),
    )
}
fn panic_error(detail: &'static str) -> QueryExecutionError {
    projected(
        QueryFailureReason::NativeFuturePanicked.failure_fact(),
        SafeDetail::truncating(detail),
    )
}
fn qualification_error(reason: ReplacementQualificationFailure) -> QueryExecutionError {
    projected(reason.failure_fact(), reason.safe_detail())
}
fn cancellation_error(reason: CancellationReason, detail: &'static str) -> QueryExecutionError {
    if reason == CancellationReason::FrontendDrainDeadlineExceeded {
        return QueryExecutionError::from_execution(
            crate::session_outcome::governed_cancellation_failure_input(reason),
        );
    }
    projected(
        if reason == CancellationReason::DeadlineExceeded {
            QueryFailureReason::DeadlineExceeded
        } else {
            QueryFailureReason::Cancelled
        }
        .failure_fact(),
        SafeDetail::truncating(detail),
    )
}
fn wait_error(
    error: WaitFailure,
    cancelled: &'static str,
    deadline: &'static str,
    panicked: &'static str,
) -> QueryExecutionError {
    match error {
        WaitFailure::Panicked => panic_error(panicked),
        WaitFailure::Cancelled(reason) => cancellation_error(reason, cancelled),
        WaitFailure::Deadline => projected(
            QueryFailureReason::RecoveryPreparationBudgetExceeded.failure_fact(),
            SafeDetail::truncating(deadline),
        ),
    }
}
fn report(error: QueryExecutionError) -> FailureReport {
    let original_attempt_failure = error.execution_failure().cloned().map(Box::new);
    FailureReport {
        error,
        original_attempt_failure,
        pre_ready_replacement: None,
    }
}
fn failed(execution: QueryExecutionId, error: QueryExecutionError) -> NativeFact {
    NativeFact::Failed {
        execution,
        failure: report(error),
    }
}

// Original tail work is defined below; its fixed slots retain results through actual Join.

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum OriginalTailKind {
    RootAck,
    Consumer,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TailPhase {
    Queued,
    Claimed,
    Running,
    Returned,
    Joined,
}
enum OriginalTailInput {
    RootAck(RootAckFuture),
    Consumer(Box<OriginalConsumerStart>),
}
struct OriginalTailState {
    phase: TailPhase,
    input: Option<OriginalTailInput>,
    ack_result:
        Option<Result<crate::api::RetainedRootReply, crate::coordination::RootResultFetchFailure>>,
    ack_failure: Option<crate::coordination::RootResultFetchFailure>,
    consumer_result: Option<Result<(), QueryExecutionError>>,
    activity: Option<crate::admitted_query_context::ResultCapacityActivityLease>,
    cleanup_returned: bool,
    join_failure: Option<OriginalJoinFailure>,
}
struct OriginalTailSlot {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    kind: OriginalTailKind,
    state: Mutex<OriginalTailState>,
    causes: FutureCauses,
    original_exit: Weak<OriginalLogicalExit>,
}
impl OriginalTailSlot {
    fn lock(&self) -> std::sync::MutexGuard<'_, OriginalTailState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn failed(&self) -> bool {
        let state = self.lock();
        self.causes.failed() || state.join_failure.is_some()
    }
}

impl fmt::Debug for OriginalTailSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("original Native tail")
            .field("activation", &self.activation)
            .field("root", &self.root)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for OriginalTailSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original Native tail failed; original input and causes retained")
    }
}
impl Error for OriginalTailSlot {}

/// A checked original slot must be consumed by FA's credentialed run_future.
/// Its future may return before actual Join; only tail_joined closes that slot.
/// Dropping an unconsumed obligation leaves the original queued input unproven.
#[must_use = "original tail work must be run and its actual Join reported"]
pub struct OriginalTailWorkObligation {
    slot: Arc<OriginalTailSlot>,
}
/// Retained by FA until the original credentialed work's actual Join. This
/// obligation has no public constructor or Clone; leaving it unconsumed keeps
/// the same tail unjoined, so tails_exited remains false (runtime checked).
#[must_use = "the original tail Join must return to its same Native slot"]
pub struct OriginalTailJoinObligation {
    slot: Arc<OriginalTailSlot>,
}
impl OriginalTailWorkObligation {
    pub fn activation(&self) -> AttemptActivationIdentity {
        self.slot.activation
    }
    pub fn kind(&self) -> OriginalTailKind {
        self.slot.kind
    }
    pub fn into_future(self) -> (LogicalFuture<'static, ()>, OriginalTailJoinObligation) {
        let joined = OriginalTailJoinObligation {
            slot: Arc::clone(&self.slot),
        };
        let future: LogicalFuture<'static, ()> = Box::pin(async move {
            let slot = self.slot;
            let input = {
                let mut state = slot.lock();
                assert_eq!(
                    state.phase,
                    TailPhase::Claimed,
                    "the exact tail obligation is consumed once"
                );
                state.phase = TailPhase::Running;
                state
                    .input
                    .take()
                    .expect("original never-polled input remains in its slot")
            };
            match input {
                OriginalTailInput::RootAck(future) => {
                    let outcome = poll_original(future, &slot.causes, None, None, |result| {
                        let mut state = slot.lock();
                        if let Err(failure) = &result {
                            state.ack_failure = Some(failure.clone());
                        }
                        state.ack_result = Some(result);
                    })
                    .await;
                    let mut state = slot.lock();
                    let _ = outcome;
                    state.cleanup_returned = slot
                        .causes
                        .cleanup
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .is_none();
                    state.phase = TailPhase::Returned;
                }
                OriginalTailInput::Consumer(input) => {
                    let OriginalConsumerStart {
                        consumer,
                        activation,
                        exit,
                        activity,
                    } = *input;
                    assert!(
                        activity.is_none(),
                        "activity stays in the original slot through actual Join"
                    );
                    let mut outcome = None;
                    match catch_unwind(AssertUnwindSafe(|| consumer.consume(activation))) {
                        Err(payload) => {
                            slot.causes.retain(payload, false);
                            outcome = Some(Err(consumer_panic()));
                            slot.lock().cleanup_returned = true;
                        }
                        Ok(future) => {
                            let result =
                                poll_original(future, &slot.causes, None, None, |result| {
                                    // Original domain outcome is stable before original future Drop.
                                    slot.lock().consumer_result = Some(result.clone());
                                    outcome = Some(result);
                                })
                                .await;
                            if result.is_err() {
                                outcome = Some(Err(consumer_panic()));
                            }
                            slot.lock().cleanup_returned = slot
                                .causes
                                .cleanup
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .is_none();
                        }
                    }
                    let outcome = outcome
                        .expect("the original consumer factory/poll returns or reports panic");
                    {
                        let mut state = slot.lock();
                        state.consumer_result = Some(outcome.clone());
                        state.phase = TailPhase::Returned;
                    }
                    if slot.causes.failed()
                        && let Some(original) = slot.original_exit.upgrade()
                    {
                        original.consumer_failed_silent();
                    }
                    // A reporter-notification panic is caught by FA's actual Join and
                    // reported to this original tail's separate join-failure position.
                    exit.complete(outcome);
                }
            }
        });
        (future, joined)
    }
}
impl NativeExecutor {
    /// Rejection leaves the original ACK future in the caller's fixed position.
    pub(crate) fn stage_root_ack_in(
        &self,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        incoming: &mut Option<RootAckFuture>,
    ) -> Result<(), QueryExecutionError> {
        let mut storage = self.lock();
        if incoming.is_none()
            || !storage.active.contains_key(&activation_key(activation))
            || !storage
                .prepared
                .get(&activation.execution())
                .is_some_and(|prepared| prepared.schedule.root() == root)
            || storage
                .tails
                .contains_key(&(activation_key(activation), OriginalTailKind::RootAck))
        {
            return Err(protocol(
                "ACK tail does not belong to an unused original attempt slot",
            ));
        }
        let original = incoming.take().expect("validated original ACK future");
        storage.tails.insert(
            (activation_key(activation), OriginalTailKind::RootAck),
            Arc::new(OriginalTailSlot {
                activation,
                root,
                kind: OriginalTailKind::RootAck,
                state: Mutex::new(OriginalTailState {
                    phase: TailPhase::Queued,
                    input: Some(OriginalTailInput::RootAck(original)),
                    ack_result: None,
                    ack_failure: None,
                    consumer_result: None,
                    activity: None,
                    cleanup_returned: false,
                    join_failure: None,
                }),
                causes: FutureCauses::default(),
                original_exit: self.original_exit.clone(),
            }),
        );
        storage
            .ready_tails
            .push_back((activation_key(activation), OriginalTailKind::RootAck));
        drop(storage);
        self.tail_ready.wake();
        Ok(())
    }
    pub(crate) async fn streamed_computation_cut_handle(
        &self,
    ) -> Option<crate::admitted_query_context::StreamedComputationCutHandle> {
        // Business Ready can retire preparation before Relay completes. The
        // original Native producer still owns the same activity source.
        self.body
            .lock()
            .await
            .streamed_computation
            .as_ref()
            .map(|producer| producer.cut_handle())
    }
    /// The caller installs the original exit receiver before acknowledging source
    /// registration. Refusal preserves the activation and reporter in `incoming`.
    pub(crate) async fn stage_supervised_consumer_in(
        &self,
        identity: AttemptActivationIdentity,
        incoming: &mut Option<(
            crate::api::InternalConsumerActivation,
            crate::api::InternalConsumerExitReporter,
        )>,
    ) -> Result<(), QueryExecutionError> {
        let matching = incoming.as_ref().is_some_and(|(activation, _)| {
            activation.source.activation() == identity && activation.stream.activation() == identity
        });
        if !matching {
            return Err(protocol("bound consumer has another activation identity"));
        }
        let (consumer, activity) = {
            let body = self.body.lock().await;
            let consumer = body
                .consumer
                .as_ref()
                .cloned()
                .ok_or_else(|| protocol("original internal computation consumer is absent"))?;
            let activity = body
                .session
                .as_ref()
                .and_then(|session| session.streamed_computation_cut_handle())
                .map(|source| source.begin_consumer_activity())
                .transpose()
                .map_err(|error| {
                    projected(
                        error.failure_fact(),
                        SafeDetail::truncating("query workload control refused the operation"),
                    )
                })?;
            (consumer, activity)
        };
        let (activation, exit) = incoming
            .take()
            .expect("validated original supervised consumer input");
        let mut staged = Some(OriginalConsumerStart {
            consumer,
            activation,
            exit,
            activity,
        });
        match self.stage_consumer_in(identity, &mut staged) {
            Ok(()) => Ok(()),
            Err(error) => {
                let OriginalConsumerStart {
                    consumer,
                    activation,
                    exit,
                    activity,
                } = staged
                    .take()
                    .expect("refusal preserves the entire original consumer input");
                *incoming = Some((activation, exit));
                // The original input is stable before a capacity-release callback.
                drop((consumer, activity));
                Err(error)
            }
        }
    }
    /// Refusals preserve the exact consumer/stream/reporter/activity in `incoming`.
    fn stage_consumer_in(
        &self,
        activation: AttemptActivationIdentity,
        incoming: &mut Option<OriginalConsumerStart>,
    ) -> Result<(), QueryExecutionError> {
        let input = incoming
            .as_ref()
            .ok_or_else(|| protocol("original consumer input is absent"))?;
        let root = input.activation.source.root();
        if input.activation.source.activation() != activation
            || input.activation.stream.activation() != activation
        {
            return Err(protocol(
                "original consumer stream and source have different activation identities",
            ));
        }
        // Reserve while the original incoming position still owns every field.
        let mut storage = self.lock();
        if !storage.active.contains_key(&activation_key(activation))
            || !storage
                .prepared
                .get(&activation.execution())
                .is_some_and(|prepared| prepared.schedule.root() == root)
            || storage
                .tails
                .contains_key(&(activation_key(activation), OriginalTailKind::Consumer))
        {
            return Err(protocol(
                "consumer does not belong to an unused original attempt tail",
            ));
        }
        let mut input = incoming.take().expect("validated original consumer input");
        let activity = input.activity.take();
        storage.tails.insert(
            (activation_key(activation), OriginalTailKind::Consumer),
            Arc::new(OriginalTailSlot {
                activation,
                root,
                kind: OriginalTailKind::Consumer,
                state: Mutex::new(OriginalTailState {
                    phase: TailPhase::Queued,
                    input: Some(OriginalTailInput::Consumer(Box::new(input))),
                    ack_result: None,
                    ack_failure: None,
                    consumer_result: None,
                    activity,
                    cleanup_returned: false,
                    join_failure: None,
                }),
                causes: FutureCauses::default(),
                original_exit: self.original_exit.clone(),
            }),
        );
        storage
            .ready_tails
            .push_back((activation_key(activation), OriginalTailKind::Consumer));
        drop(storage);
        self.tail_ready.wake();
        Ok(())
    }
    pub fn poll_tail_work(&self, cx: &mut Context<'_>) -> Poll<OriginalTailWorkObligation> {
        self.tail_ready.register(cx.waker());
        let mut storage = self.lock();
        if let Some(key) = storage.ready_tails.pop_front() {
            let slot = storage
                .tails
                .get(&key)
                .expect("ready index names an original fixed slot");
            let mut state = slot.lock();
            assert_eq!(
                state.phase,
                TailPhase::Queued,
                "one ready entry per original tail"
            );
            state.phase = TailPhase::Claimed;
            return Poll::Ready(OriginalTailWorkObligation {
                slot: Arc::clone(slot),
            });
        }
        Poll::Pending
    }
    /// Called by FA only after consuming the original credentialed task Join.
    /// Returned is not Join, and a failed Join does not fabricate original F Drop.
    pub fn tail_joined(
        &self,
        original: OriginalTailJoinObligation,
        outcome: Result<(), Arc<dyn Error + Send + Sync>>,
    ) -> Result<(), QueryExecutionError> {
        let slot = original.slot;
        let registered = self
            .lock()
            .tails
            .get(&(activation_key(slot.activation), slot.kind))
            .is_some_and(|expected| Arc::ptr_eq(expected, &slot));
        if !registered {
            return Err(protocol(
                "tail Join does not belong to this original Native executor",
            ));
        }
        receive_tail_join(&slot, outcome)
    }
    pub fn tails_exited(&self, activation: AttemptActivationIdentity) -> bool {
        self.lock()
            .tails
            .iter()
            .filter(|((identity, _), _)| *identity == activation_key(activation))
            .all(|(_, slot)| {
                let state = slot.lock();
                state.phase == TailPhase::Joined && state.cleanup_returned
            })
    }
}
fn receive_tail_join(
    slot: &OriginalTailSlot,
    outcome: Result<(), OriginalJoinFailure>,
) -> Result<(), QueryExecutionError> {
    let (activity, ack) = {
        let mut state = slot.lock();
        if state.phase == TailPhase::Joined {
            return Err(protocol("the original tail Join was already consumed"));
        }
        if let Err(failure) = outcome {
            state.join_failure = Some(failure);
            if slot.kind == OriginalTailKind::Consumer
                && let Some(source) = slot.original_exit.upgrade()
            {
                source.consumer_failed_silent();
            }
        }
        if state.join_failure.is_none() && state.phase != TailPhase::Returned {
            return Err(protocol("tail Join does not prove original future cleanup"));
        }
        state.phase = TailPhase::Joined;
        // Failed/unproven cleanup retains the original activity and inputs.
        let released = state.cleanup_returned;
        (
            if released {
                state.activity.take()
            } else {
                None
            },
            if released {
                state.ack_result.take()
            } else {
                None
            },
        )
    };
    if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop((activity, ack)))) {
        slot.causes.retain(payload, true);
    }
    Ok(())
}
fn consumer_panic() -> QueryExecutionError {
    projected(
        QueryFailureReason::InternalConsumerPanicked.failure_fact(),
        SafeDetail::truncating("internal computation consumer panicked"),
    )
}

#[cfg(test)]
#[path = "native/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "native/qualification_cleanup_tests.rs"]
mod qualification_cleanup_tests;

#[cfg(test)]
mod cleanup_tests;

/// Fixtures which do not install an AP must refuse every installed operation.
/// They cannot produce a startup, Ready registration, or actual-exit receipt.
#[cfg(test)]
struct UnboundInstalledAttemptPort;
#[cfg(test)]
impl crate::api::NativeInstalledAttemptPort for UnboundInstalledAttemptPort {
    fn start(
        &self,
        _: AttemptActivationIdentity,
    ) -> LogicalFuture<'_, Result<NativeAttemptStartupOutcome, QueryExecutionError>> {
        Box::pin(async { Err(protocol("original test attempt sources are not installed")) })
    }
    fn record_ready(
        &self,
        _: crate::api::ComputationReadyToken,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async { Err(protocol("original test attempt publisher is not installed")) })
    }
    fn resources_exited(
        &self,
        _: AttemptActivationIdentity,
    ) -> LogicalFuture<'_, Result<(), crate::api::StatementNativeHealthFailure>> {
        Box::pin(async {
            Err(
                crate::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "original test attempt sources are not installed",
                    Arc::new(protocol("original test attempt sources are not installed")),
                ),
            )
        })
    }
}
