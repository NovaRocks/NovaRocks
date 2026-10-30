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

//! Process-owned logical execution supervision.
//!
//! The supervisor is the sole constructor of [`QueryExecutionClient`]. A
//! original fixed role cell receives one governed work owner before returning
//! a future to the caller. The same supervisor task polls every logical role;
//! dropping a caller reply cannot detach its original frame or input. Actor
//! and Native active custody retain their existing exit contracts.
//!
//! Design: ADR-0198 (docs/adr/ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md).

use super::LogicalExecutionExitObserver;
use super::original_exit::OriginalLogicalExit;
use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::{Duration, Instant};

use novarocks_types::identity::{
    AttemptId, FrontendProcessId, LocalQuerySequence, QueryExecutionId, QueryIdAttribution,
    QueryProcessNamespace,
};
use novarocks_workload_control::{
    CancellationReason, PollCompletion, PollTurnCompletion, RoleBackingRetirementObserver,
    RoleIngress, RolePollBank, Stage, WorkError, WorkOwner, WorkloadControl,
};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::api::{
    ActiveNativeAttemptOwner, ExecutionHandle, LogicalExecutionNativePort, LogicalNativeOpenError,
    LogicalNativeOpenRequest, LogicalNativeSession, NativeActiveConvergenceClosure,
    NativeActiveConvergenceRequest, NativeAttemptConvergence, NativeAttemptPreparationError,
    NativeAttemptPreparationFailure, NativeAttemptTerminal, NativeContextConvergenceKind,
    NativeRowsAttemptRuntime, QueryExecutionClient, QueryExecutionDriver, QueryExecutionError,
    QueryExecutionErrorKind, QueryExecutionFuture, QueryExecutionRequest,
    ReservedExecutionIdentity, ResultSchema, StatementNativeHealthFailure,
};
use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason};
use crate::preparation::OutputContract;
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};

use super::{
    LogicalExecutionActorConfig, LogicalExecutionRuntimeRegistry,
    LogicalExecutionRuntimeRegistryError, LogicalExecutionRuntimeRegistryHandle,
    LogicalExecutionRuntimeShutdownError, NativeAttemptDrive, RecoveryDecision, RecoveryInput,
    ResultPumpFailure, SupervisedActiveNativeOwner, assert_shutdown_complete,
    build_attempt_schedule, evaluate_recovery,
};
use novarocks_coordination_contract::{MonotonicClock, RecoveryMode};

static NEXT_PROCESS_QUERY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const DEFAULT_REMOTE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Explicit process bounds used by the logical execution supervisor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionSupervisorConfig {
    actor_mailbox_capacity: NonZeroUsize,
    max_admission_issues_per_context: NonZeroUsize,
    max_establish_authorizations_per_context: NonZeroUsize,
    rows: LogicalExecutionRowsConfig,
    remote_cleanup_timeout: Duration,
    diagnostic_timeout: Duration,
}

/// Process-fixed bounds for every row-producing logical execution.
///
/// These limits are role composition facts. A frozen query description cannot
/// raise them, and the supervisor supplies the same bounds to each replacement
/// attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionRowsConfig {
    delivery_capacity: NonZeroUsize,
    max_attempts: NonZeroU32,
    replacement_reservation_valid_for: Duration,
}

impl LogicalExecutionRowsConfig {
    pub const fn new(
        delivery_capacity: NonZeroUsize,
        max_attempts: NonZeroU32,
        replacement_reservation_valid_for: Duration,
    ) -> Self {
        Self {
            delivery_capacity,
            max_attempts,
            replacement_reservation_valid_for,
        }
    }
}

impl LogicalExecutionSupervisorConfig {
    pub const fn new(
        actor_mailbox_capacity: NonZeroUsize,
        max_admission_issues_per_context: NonZeroUsize,
        max_establish_authorizations_per_context: NonZeroUsize,
        rows: LogicalExecutionRowsConfig,
    ) -> Self {
        Self {
            actor_mailbox_capacity,
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
            rows,
            remote_cleanup_timeout: DEFAULT_REMOTE_CLEANUP_TIMEOUT,
            diagnostic_timeout: DEFAULT_REMOTE_CLEANUP_TIMEOUT,
        }
    }

    pub const fn replacement_reservation_valid_for(self) -> Duration {
        self.rows.replacement_reservation_valid_for
    }

    /// Sets the one absolute observation budget that starts after the FE has
    /// reached a logical terminal outcome. It governs FE tracking only; it
    /// does not assert a Worker stop deadline.
    pub const fn with_remote_cleanup_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "remote cleanup timeout must be nonzero");
        self.remote_cleanup_timeout = timeout;
        self
    }

    pub const fn with_diagnostic_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "diagnostic timeout must be nonzero");
        self.diagnostic_timeout = timeout;
        self
    }

    pub const fn remote_cleanup_timeout(self) -> Duration {
        self.remote_cleanup_timeout
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalExecutionSupervisorShutdownError {
    DeadlineExceeded,
    Registry(LogicalExecutionRuntimeRegistryError),
    SupervisorFailed(QueryExecutionError),
    SupervisorPanicked,
    StatementNativeUnresolved(QueryExecutionError),
}

impl fmt::Display for LogicalExecutionSupervisorShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExceeded => {
                formatter.write_str("logical execution supervisor shutdown deadline exceeded")
            }
            Self::Registry(error) => write!(
                formatter,
                "logical execution supervisor Registry shutdown failed: {error}"
            ),
            Self::SupervisorFailed(error) => {
                write!(formatter, "logical execution supervisor failed: {error}")
            }
            Self::StatementNativeUnresolved(error) => {
                write!(
                    formatter,
                    "statement Native owner has unproven actual exit: {error}"
                )
            }
            Self::SupervisorPanicked => {
                formatter.write_str("logical execution supervisor task panicked")
            }
        }
    }
}

impl std::error::Error for LogicalExecutionSupervisorShutdownError {}

/// Unique process owner for logical execution admission, tasks, and Registry.
///
/// Shutdown borrows this owner. A timed-out or cancelled wait leaves the exact
/// join handle and Registry owner here so a later call resumes the same
/// convergence.
// Design: ADR-0198 (docs/adr/ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md)
pub struct LogicalExecutionSupervisor {
    shutdown: watch::Sender<bool>,
    join: Option<JoinHandle<OriginalRootOutcome>>,
    run_outcome: Option<SupervisorRunOutcome>,
    original_root_failure: std::mem::ManuallyDrop<Option<Arc<tokio::task::JoinError>>>,
    original_root_panic: std::mem::ManuallyDrop<Option<OriginalRootPanic>>,
    registry: LogicalExecutionRuntimeRegistry,
    statements: Arc<crate::session_control::StatementProgressSlot>,
    role_retirement: RoleBackingRetirementObserver,
    shutdown_complete: bool,
}

enum OriginalRootOutcome {
    Completed(Result<(), QueryExecutionError>),
    Panicked(OriginalRootPanic),
}
#[derive(Clone)]
struct OriginalRootPanic {
    // One original root packet shared only by its Supervisor and task guard.
    // It never points to Registry/health. An unresolved packet's implicit
    // destruction must not destroy its bank, opaque panic or raw join cause.
    shared: Arc<OriginalRootRetirement>,
}
struct OriginalRootRetirement {
    custody: std::sync::Mutex<std::mem::ManuallyDrop<Option<OriginalRootPanicCustody>>>,
    join_failure: std::sync::OnceLock<std::mem::ManuallyDrop<Arc<tokio::task::JoinError>>>,
}
struct OriginalRootPanicCustody {
    _bank: Box<dyn std::any::Any + Send>,
    bank_layout: std::alloc::Layout,
    // Cancellation is reported by the genuine original JoinError, not a
    // synthetic panic payload. A caught service panic remains unchanged here.
    primary: Option<Box<dyn std::any::Any + Send>>,
    cleanup: [Option<Box<dyn std::any::Any + Send>>; 3],
    cancelled: bool,
}
impl OriginalRootPanic {
    fn empty() -> Self {
        Self {
            shared: Arc::new(OriginalRootRetirement {
                custody: std::sync::Mutex::new(std::mem::ManuallyDrop::new(None)),
                join_failure: std::sync::OnceLock::new(),
            }),
        }
    }
    fn custody(&self) -> OriginalRootBankView<'_> {
        OriginalRootBankView {
            guard: self
                .shared
                .custody
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
        }
    }
    fn has_failure(&self) -> bool {
        self.shared
            .custody
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_some()
            || self.shared.join_failure.get().is_some()
    }
    fn retain(&self, original: OriginalRootPanicCustody) {
        let mut original = Some(original);
        {
            let mut target = self
                .shared
                .custody
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if target.is_none() {
                **target = original.take();
            }
        }
        // A second bank cannot be normally destroyed on an invariant failure.
        if let Some(original) = original {
            std::panic::panic_any(std::mem::ManuallyDrop::new(original));
        }
    }
    fn retain_join_failure(&self, original: Arc<tokio::task::JoinError>) {
        if let Err(original) = self
            .shared
            .join_failure
            .set(std::mem::ManuallyDrop::new(original))
        {
            std::panic::panic_any(original);
        }
    }
    fn dispose_for_process_exit(self) {
        // The normal joined root leaves only this original Supervisor alias.
        // If an illegal live alias remains, preserve it to process exit rather
        // than pretending that the task's actual sources have exited.
        if let Ok(mut shared) = Arc::try_unwrap(self.shared) {
            let original = shared
                .custody
                .into_inner()
                .unwrap_or_else(|error| error.into_inner());
            drop(std::mem::ManuallyDrop::into_inner(original));
            if let Some(original) = shared.join_failure.take() {
                drop(std::mem::ManuallyDrop::into_inner(original));
            }
        }
    }
}
struct OriginalRootBankView<'a> {
    guard: std::sync::MutexGuard<'a, std::mem::ManuallyDrop<Option<OriginalRootPanicCustody>>>,
}
impl std::ops::Deref for OriginalRootBankView<'_> {
    type Target = OriginalRootPanicCustody;
    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().expect("retained original root")
    }
}
/// A borrow from the original root owner, never a take/projected panic string.
/// Do not retain this view while requesting the same owner's shutdown.
pub struct LogicalExecutionRootFailureView<'a> {
    original: OriginalRootBankView<'a>,
}
impl LogicalExecutionRootFailureView<'_> {
    pub fn primary(&self) -> Option<&(dyn std::any::Any + Send)> {
        self.original.primary.as_deref()
    }
    pub fn cleanup(&self, index: usize) -> Option<&(dyn std::any::Any + Send)> {
        self.original.cleanup.get(index).and_then(Option::as_deref)
    }
    pub fn retained_bank_layout(&self) -> std::alloc::Layout {
        self.original.bank_layout
    }
    pub fn was_cancelled(&self) -> bool {
        self.original.cancelled
    }
}

type LogicalRootRoleBank<F, M> = RolePollBank<LogicalRoleInput, F, M, Arc<OriginalLogicalExit>>;

/// The unique original bank, outside the caught borrowed service. Drop closes
/// the same ingress even before the root's first poll, then retains the whole
/// bank in the original Supervisor packet. It never ordinarily drops sources.
struct OriginalRootBank<F, M>
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
    M: Fn(WorkOwner, LogicalRoleInput) -> F + Send + 'static,
{
    bank: std::mem::ManuallyDrop<Option<LogicalRootRoleBank<F, M>>>,
    retirement: OriginalRootPanic,
    statements: Arc<crate::session_control::StatementProgressSlot>,
}
impl<F, M> OriginalRootBank<F, M>
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
    M: Fn(WorkOwner, LogicalRoleInput) -> F + Send + 'static,
{
    fn bank_mut(&mut self) -> &mut LogicalRootRoleBank<F, M> {
        self.bank.as_mut().expect("original root bank")
    }
    fn take(&mut self) -> LogicalRootRoleBank<F, M> {
        self.bank
            .take()
            .expect("original root bank transferred once")
    }
}
impl<F, M> Drop for OriginalRootBank<F, M>
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
    M: Fn(WorkOwner, LogicalRoleInput) -> F + Send + 'static,
{
    fn drop(&mut self) {
        if let Some(bank) = self.bank.take() {
            // stop_ingress publishes the original State cut; the catch retains
            // any real exceptional tail without replacing the task JoinError.
            let statement_closing =
                catch_unwind(AssertUnwindSafe(|| self.statements.close())).err();
            let bank_closing = catch_unwind(AssertUnwindSafe(|| bank.stop_ingress())).err();
            self.retirement.retain(OriginalRootPanicCustody {
                _bank: Box::new((bank, Arc::clone(&self.statements))),
                bank_layout: std::alloc::Layout::new::<(
                    RolePollBank<LogicalRoleInput, F, M, Arc<OriginalLogicalExit>>,
                    Arc<crate::session_control::StatementProgressSlot>,
                )>(),
                primary: None,
                cleanup: [statement_closing, bank_closing, None],
                cancelled: true,
            });
        }
    }
}

/// Independent facts; the bank value is moved to the failure Box, not copied.
/// Task storage, borrowed service frame and opaque pointees are separate owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionRootCustodyGeometry {
    pub supervisor: std::alloc::Layout,
    pub failure: std::alloc::Layout,
    pub join_output: std::alloc::Layout,
    pub retained_bank: std::alloc::Layout,
    pub cancellation_guard: std::alloc::Layout,
    pub shared_retirement_packet: std::alloc::Layout,
}
fn root_custody_geometry<F, M>(_: &M) -> LogicalExecutionRootCustodyGeometry
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
    M: Fn(WorkOwner, LogicalRoleInput) -> F + Send + 'static,
{
    LogicalExecutionRootCustodyGeometry {
        supervisor: std::alloc::Layout::new::<LogicalExecutionSupervisor>(),
        failure: std::alloc::Layout::new::<OriginalRootPanic>(),
        join_output: std::alloc::Layout::new::<OriginalRootOutcome>(),
        cancellation_guard: std::alloc::Layout::new::<OriginalRootBank<F, M>>(),
        shared_retirement_packet: std::alloc::Layout::new::<OriginalRootRetirement>(),
        retained_bank: std::alloc::Layout::new::<(
            RolePollBank<LogicalRoleInput, F, M, Arc<OriginalLogicalExit>>,
            Arc<crate::session_control::StatementProgressSlot>,
        )>(),
    }
}

enum SupervisorRunOutcome {
    Completed,
    Failed(Box<QueryExecutionError>),
    Panicked,
}

impl fmt::Debug for LogicalExecutionSupervisor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalExecutionSupervisor")
            .field("shutdown_requested", &*self.shutdown.borrow())
            .field("shutdown_complete", &self.shutdown_complete)
            .finish_non_exhaustive()
    }
}

impl Drop for LogicalExecutionSupervisor {
    fn drop(&mut self) {
        assert_shutdown_complete(self.shutdown_complete, "logical execution supervisor");
        if self.shutdown_complete {
            drop(self.original_root_failure.take());
            drop(self.original_root_panic.take());
        }
        // Illegal owner loss leaves the SAME original finite failure custody
        // intact. This is not successful or normal product shutdown.
    }
}

impl LogicalExecutionSupervisor {
    /// Observe the original service task without joining or consuming its
    /// retained outcome. Role readiness must reject an already exited root.
    pub fn service_has_exited(&self) -> bool {
        self.join.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Process-health observation is independent of the service task's exit.
    /// The existing registry retains its original cause and source owner.
    pub fn process_failure(&self) -> Option<QueryExecutionError> {
        self.registry.process_health()
    }

    /// Borrows the original source's same cause; no absence or root task
    /// completion is projected as action exit.
    pub fn statement_action_failure(
        &self,
    ) -> Option<Arc<crate::session_control::StatementActionFailure>> {
        self.statements.failure()
    }

    /// Serial composition before statement publication/Ready. The same root
    /// polls original statement actions without mounting their root twice.
    pub fn install_statement_control(
        &mut self,
        service: crate::session_control::QueryControlService,
        source_slots: std::num::NonZeroUsize,
    ) -> Result<(), crate::session_control::QueryControlError> {
        self.statements.install(service, source_slots)
    }
    /// Pure layout facts from this owner's actual logical-role factory.
    /// No Work, input, task or bank is created. Composition must separately
    /// fund independent pointees and the real health wrapper overlap before
    /// installing a complete Native role profile.
    pub fn logical_role_poll_geometry()
    -> Result<novarocks_workload_control::PollRoleGeometry, novarocks_workload_control::WorkError>
    {
        let geometry = novarocks_workload_control::PollRoleGeometry::for_factory_with_context::<
            _,
            _,
            _,
            Arc<OriginalLogicalExit>,
        >(&make_logical_role)?;
        geometry.with_source_backing(Self::logical_role_source_backing()?)
    }

    pub fn background_action_source_geometry()
    -> crate::session_control::BackgroundActionSourceGeometry {
        crate::session_control::StatementActionSource::background_geometry()
    }

    fn logical_role_source_backing()
    -> Result<novarocks_workload_control::PollRoleSourceBacking, WorkError> {
        LogicalExecutionExitObserver::metadata_geometry()
            .map_err(|_| WorkError::ArithmeticOverflow)?
            .role_source_backing()
    }

    /// Actual factory types only; this does not construct an input or bank.
    pub fn logical_root_custody_geometry() -> LogicalExecutionRootCustodyGeometry {
        root_custody_geometry(&make_logical_role)
    }

    /// Installs the same factory in the original fixed inventory before Ready.
    /// The composing owner supplies the complete profile and role index. There
    /// is no counted or per-task fallback when installation is refused.
    pub fn try_new(
        control: &mut WorkloadControl,
        role: usize,
        turn_budget: NonZeroUsize,
        runtime: Handle,
        native: Arc<dyn LogicalExecutionNativePort>,
        namespace: QueryProcessNamespace,
        frontend_process_id: FrontendProcessId,
        config: LogicalExecutionSupervisorConfig,
    ) -> Result<(Self, QueryExecutionClient), WorkError> {
        Self::try_with_clock(
            control,
            role,
            turn_budget,
            runtime,
            native,
            namespace,
            frontend_process_id,
            config,
            Arc::new(super::ProcessMonotonicClock::new()),
        )
    }

    /// Uses the original role-local timeline and one bank across all classes.
    /// turn_budget bounds service work between yields; it grants no capacity.
    pub fn try_with_clock(
        control: &mut WorkloadControl,
        role: usize,
        turn_budget: NonZeroUsize,
        runtime: Handle,
        native: Arc<dyn LogicalExecutionNativePort>,
        namespace: QueryProcessNamespace,
        frontend_process_id: FrontendProcessId,
        config: LogicalExecutionSupervisorConfig,
        clock: Arc<dyn MonotonicClock>,
    ) -> Result<(Self, QueryExecutionClient), WorkError> {
        // Finish fallible inventory registration before constructing any task
        // or Registry owner requiring asynchronous shutdown.
        let bank = RolePollBank::<_, _, _, Arc<OriginalLogicalExit>>::install_with_context_and_source_backing(
            control,
            role,
            make_logical_role,
            Self::logical_role_source_backing()?,
        )?;
        Self::start_installed_bank(
            bank,
            turn_budget,
            runtime,
            native,
            namespace,
            frontend_process_id,
            config,
            clock,
        )
    }

    // Only the original installer or its private unit fixture can provide a
    // bank. This is not an ownerless registration or public admission API.
    fn start_installed_bank<F, M>(
        bank: RolePollBank<LogicalRoleInput, F, M, Arc<OriginalLogicalExit>>,
        turn_budget: NonZeroUsize,
        runtime: Handle,
        native: Arc<dyn LogicalExecutionNativePort>,
        namespace: QueryProcessNamespace,
        frontend_process_id: FrontendProcessId,
        config: LogicalExecutionSupervisorConfig,
        clock: Arc<dyn MonotonicClock>,
    ) -> Result<(Self, QueryExecutionClient), WorkError>
    where
        F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
        M: Fn(WorkOwner, LogicalRoleInput) -> F + Send + 'static,
    {
        let ingress = bank.ingress()?;
        let role_retirement = bank.retirement_observer();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let registry = LogicalExecutionRuntimeRegistry::new(runtime.clone());
        let registry_handle = registry.handle();
        let query_ids = Arc::new(ProcessQueryIdAllocator::new(namespace));
        let driver = QueryExecutionDriverHandle {
            ingress,
            shutdown: shutdown_rx.clone(),
            query_ids,
            native,
            frontend_process_id,
            config,
            clock,
            registry: registry_handle.clone(),
        };
        let original_root = OriginalRootPanic::empty();
        let statements = crate::session_control::StatementProgressSlot::empty();
        let join = runtime.spawn(run_supervisor_owned(
            registry_handle,
            bank,
            turn_budget,
            shutdown_rx,
            original_root.clone(),
            Arc::clone(&statements),
        ));
        Ok((
            Self {
                shutdown,
                join: Some(join),
                run_outcome: None,
                original_root_failure: std::mem::ManuallyDrop::new(None),
                original_root_panic: std::mem::ManuallyDrop::new(Some(original_root)),
                registry,
                statements,
                role_retirement,
                shutdown_complete: false,
            },
            QueryExecutionClient::new(driver),
        ))
    }

    /// Observe only original bank retirement after separately joining this root.
    /// The composing owner keeps the original progress service alive. A packet
    /// retained by health may keep its footer charged beyond the root's exit.
    pub fn role_retirement(&self) -> &RoleBackingRetirementObserver {
        &self.role_retirement
    }

    /// Safe borrow; the original failure remains with the actual root owner.
    pub fn original_root_join_failure(&self) -> Option<&tokio::task::JoinError> {
        self.original_root_failure.as_deref()
    }

    pub fn original_root_panic(&self) -> Option<LogicalExecutionRootFailureView<'_>> {
        let original = self.original_root_panic.as_ref()?;
        let view = original.custody();
        if view.guard.is_none() {
            return None;
        }
        Some(LogicalExecutionRootFailureView { original: view })
    }

    pub async fn shutdown_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), LogicalExecutionSupervisorShutdownError> {
        self.shutdown.send_replace(true);
        if self.shutdown_complete {
            return Ok(());
        }
        if self.run_outcome.is_none() {
            let join = self
                .join
                .as_mut()
                .expect("unfinished supervisor retains its join handle");
            self.run_outcome = match tokio::time::timeout_at(deadline.into(), join).await {
                Err(_) => {
                    // Preserve the original healthy/error boundary while the
                    // same JoinHandle/Registry still own unresolved local work.
                    return Err(match self.registry.process_health() {
                        Some(error) => {
                            LogicalExecutionSupervisorShutdownError::StatementNativeUnresolved(
                                error,
                            )
                        }
                        None => LogicalExecutionSupervisorShutdownError::DeadlineExceeded,
                    });
                }
                Ok(Err(error)) => Some(self.receive_original_root_join_failure(error)),
                Ok(Ok(OriginalRootOutcome::Completed(Err(error)))) => {
                    Some(SupervisorRunOutcome::Failed(Box::new(error)))
                }
                Ok(Ok(OriginalRootOutcome::Completed(Ok(())))) => {
                    Some(SupervisorRunOutcome::Completed)
                }
                Ok(Ok(OriginalRootOutcome::Panicked(original))) => {
                    *self.original_root_panic = Some(original);
                    Some(SupervisorRunOutcome::Panicked)
                }
            };
            self.join.take();
        }

        match self.registry.shutdown_until(deadline).await {
            Ok(()) => {}
            Err(LogicalExecutionRuntimeShutdownError::DeadlineExceeded { .. }) => {
                return Err(match self.registry.process_health() {
                    Some(error) => {
                        LogicalExecutionSupervisorShutdownError::StatementNativeUnresolved(error)
                    }
                    None => LogicalExecutionSupervisorShutdownError::DeadlineExceeded,
                });
            }
            Err(LogicalExecutionRuntimeShutdownError::Registry(error)) => {
                return Err(LogicalExecutionSupervisorShutdownError::Registry(error));
            }
        }

        if let Some(error) = self.registry.process_health()
            && matches!(self.run_outcome, Some(SupervisorRunOutcome::Completed))
        {
            self.run_outcome = Some(SupervisorRunOutcome::Failed(Box::new(error)));
        }
        if self.original_root_failure.is_some()
            || self
                .original_root_panic
                .as_ref()
                .is_some_and(OriginalRootPanic::has_failure)
        {
            return Err(LogicalExecutionSupervisorShutdownError::SupervisorPanicked);
        }
        self.shutdown_complete = true;
        match self
            .run_outcome
            .take()
            .expect("registry shutdown follows a collected supervisor outcome")
        {
            SupervisorRunOutcome::Completed => Ok(()),
            SupervisorRunOutcome::Failed(error) => Err(
                LogicalExecutionSupervisorShutdownError::SupervisorFailed(*error),
            ),
            SupervisorRunOutcome::Panicked => {
                Err(LogicalExecutionSupervisorShutdownError::SupervisorPanicked)
            }
        }
    }

    fn receive_original_root_join_failure(
        &mut self,
        error: tokio::task::JoinError,
    ) -> SupervisorRunOutcome {
        let original = Arc::new(error);
        *self.original_root_failure = Some(Arc::clone(&original));
        self.original_root_panic
            .as_ref()
            .unwrap()
            .retain_join_failure(Arc::clone(&original));
        // The original bank and actual raw cause are stable before health can
        // execute any callback. The health wrapper holds only the same cause,
        // never this packet or bank (which could form an ownership cycle).
        self.registry
            .record_native_supervision_panic(Box::new(original));
        SupervisorRunOutcome::Panicked
    }

    /// Ends process-local ownership after bounded graceful convergence failed
    /// and the FE runner has irrevocably committed to process exit.
    ///
    /// This does not abort Tokio tasks. Their join handles are released only at
    /// the process boundary, where the runtime and every process-local task are
    /// about to cease together. Ordinary callers must keep using
    /// [`Self::shutdown_until`] and may not use this as a detach path.
    pub fn abandon_for_process_exit(&mut self) {
        self.shutdown.send_replace(true);
        self.join.take();
        self.run_outcome.take();
        self.registry.abandon_for_process_exit();
        // The process owner has irrevocably selected actual process exit.
        // Dispose whole original packets here, never on a normal health reject.
        drop(self.original_root_failure.take());
        if let Some(original) = self.original_root_panic.take() {
            original.dispose_for_process_exit();
        }
        self.shutdown_complete = true;
    }

    #[cfg(test)]
    fn inject_registry_shutdown_error_once(&mut self, error: LogicalExecutionRuntimeRegistryError) {
        self.registry.inject_shutdown_error_once(error);
    }
}

struct QueryExecutionDriverHandle {
    ingress: RoleIngress<LogicalRoleInput, Arc<OriginalLogicalExit>>,
    shutdown: watch::Receiver<bool>,
    query_ids: Arc<ProcessQueryIdAllocator>,
    native: Arc<dyn LogicalExecutionNativePort>,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    clock: Arc<dyn MonotonicClock>,
    registry: LogicalExecutionRuntimeRegistryHandle,
}

impl fmt::Debug for QueryExecutionDriverHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryExecutionDriverHandle")
            .field("shutdown_requested", &*self.shutdown.borrow())
            .finish_non_exhaustive()
    }
}

impl QueryExecutionDriver for QueryExecutionDriverHandle {
    fn report_process_failure(&self, failure: crate::api::StatementNativeHealthFailure) {
        self.registry.record_process_failure(failure);
    }

    fn reserve_identity(&self) -> Result<ReservedExecutionIdentity, QueryExecutionError> {
        if *self.shutdown.borrow() || self.shutdown.has_changed().is_err() {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Rejected,
                "logical execution supervisor is shutting down",
            ));
        }
        self.query_ids.reserve()
    }

    fn prepare_observed_start(
        &self,
        request: QueryExecutionRequest,
        reserved: Option<ReservedExecutionIdentity>,
    ) -> Result<
        OriginalLogicalStartInput,
        (
            QueryExecutionError,
            QueryExecutionRequest,
            Option<ReservedExecutionIdentity>,
        ),
    > {
        let (reply, response) = oneshot::channel();
        let internal_cut = request.description().finish_boundary()
            == crate::preparation::ExecutionFinishBoundary::ComputationReady;
        let source = OriginalLogicalExit::with_native(request, Arc::clone(&self.native));
        let input = LogicalRoleInput::Native(NativeLogicalRoleInput {
            reply,
            reserved,
            query_ids: Arc::clone(&self.query_ids),
            frontend_process_id: self.frontend_process_id,
            config: self.config,
            clock: Arc::clone(&self.clock),
            registry: self.registry.clone(),
            shutdown: self.shutdown.clone(),
            original_exit: Arc::downgrade(&source),
        });
        Ok(OriginalLogicalStartInput {
            input: Some(input),
            response: Some(response),
            context: Some(source),
            internal_cut,
            issuer: Arc::clone(&self.query_ids.issuer),
        })
    }

    fn start_observed_in(
        &self,
        input: &mut Option<OriginalLogicalStartInput>,
        owner: &mut Option<WorkOwner>,
        ready: &mut Option<QueryExecutionFuture>,
        exit: &mut Option<LogicalExecutionExitObserver>,
    ) -> Result<LogicalExecutionStartAttempt, QueryExecutionError> {
        if ready.is_some() || exit.is_some() {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original observed-start receiver is occupied",
            ));
        }
        if *self.shutdown.borrow() || self.shutdown.has_changed().is_err() {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Rejected,
                "logical execution supervisor is shutting down",
            ));
        }
        let original = input.as_mut().ok_or_else(|| {
            QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original start source is absent",
            )
        })?;
        if !Arc::ptr_eq(&original.issuer, &self.query_ids.issuer) {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original start source belongs to another supervisor",
            ));
        }
        if original.input.is_none() || original.response.is_none() || original.context.is_none() {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original start source was already mounted",
            ));
        }
        let work = owner.as_ref().ok_or_else(|| {
            QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original work owner is absent",
            )
        })?;
        let cancellation = work.scope().cancellation().map_err(work_error)?;
        let source = Arc::clone(original.context.as_ref().unwrap());
        let attempt = self
            .ingress
            .try_mount_in(owner, &mut original.input, &mut original.context);
        if attempt.result().is_err() {
            // All original sources stay in the caller's stable fields. Carry
            // the actual sender and cancellation tail out of caller locks.
            return Ok(LogicalExecutionStartAttempt {
                attempt: Some(attempt),
                cancellation: Some(cancellation),
                header: None,
            });
        }
        let response = original.response.take().unwrap();
        let internal_cut = original.internal_cut;
        let future: QueryExecutionFuture = Box::pin(async move {
            if internal_cut {
                return response.await.unwrap_or_else(|_| {
                    Err(QueryExecutionError::new(
                        QueryExecutionErrorKind::Failed,
                        "logical execution supervisor closed without an internal verdict",
                    ))
                });
            }
            tokio::select! {
                biased;
                result = response => result.unwrap_or_else(|_| Err(QueryExecutionError::new(
                    QueryExecutionErrorKind::Failed, "logical execution supervisor closed without a start verdict"))),
                reason = cancellation.cancelled() => Err(start_cancellation_error(cancellation.reason().unwrap_or(reason))),
            }
        });
        // No foreign callback between actual mount and both stable receivers.
        *ready = Some(future);
        *exit = Some(LogicalExecutionExitObserver::new(source));
        let header = input.take().map(OriginalStartHeader::Native);
        Ok(LogicalExecutionStartAttempt {
            attempt: Some(attempt),
            cancellation: None,
            header,
        })
    }

    fn prepare_background_action_in(
        &self,
        owner: &Option<WorkOwner>,
        context: &mut Option<crate::admitted_query_context::QueryExecutionContext>,
        permit: &mut Option<novarocks_workload_control::QueryConcurrencyPermit>,
        payload: &mut Option<novarocks_workload_control::joint_admission::RetainedPayloadGrant>,
        action: &mut Option<Box<dyn crate::session_control::GovernedBackgroundAction>>,
        destination: &mut Option<OriginalBackgroundActionInput>,
    ) -> Result<(), QueryExecutionError> {
        use crate::session_control::{BackgroundActionCustody, StatementActionSource};
        if destination.is_some() || action.is_none() || context.is_none() || permit.is_none() {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "background original source or destination is unavailable",
            ));
        }
        if *self.shutdown.borrow() || self.shutdown.has_changed().is_err() {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Rejected,
                "logical execution supervisor is shutting down",
            ));
        }
        let owner = owner.as_ref().ok_or_else(|| {
            QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original background Root is absent",
            )
        })?;
        let timeout = BackgroundActionCustody::validate(
            owner,
            context.as_ref().unwrap(),
            permit.as_ref().unwrap(),
            payload.as_ref(),
        )
        .map_err(work_error)?;
        let requester = owner.cancellation_requester();
        // Allocate all fixed shared headers while the caller's original slots
        // remain intact. There is no callback or asynchronous work here.
        let source = StatementActionSource::empty_background();
        let original = OriginalLogicalExit::with_background(Arc::clone(&source));
        let custody = BackgroundActionCustody::take_in(owner, context, permit, payload, timeout)
            .map_err(work_error)?;
        source.install_background(custody, requester, action);
        *destination = Some(OriginalBackgroundActionInput {
            input: Some(LogicalRoleInput::Background {
                original_exit: Arc::downgrade(&original),
                shutdown: self.shutdown.clone(),
            }),
            context: Some(original),
            issuer: Arc::clone(&self.query_ids.issuer),
        });
        Ok(())
    }
    fn start_background_action_in(
        &self,
        input: &mut Option<OriginalBackgroundActionInput>,
        owner: &mut Option<WorkOwner>,
        ready: &mut Option<crate::session_control::BackgroundActionReady>,
        exit: &mut Option<LogicalExecutionExitObserver>,
    ) -> Result<LogicalExecutionStartAttempt, QueryExecutionError> {
        if ready.is_some() || exit.is_some() {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original background receiver is occupied",
            ));
        }
        if *self.shutdown.borrow() || self.shutdown.has_changed().is_err() {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Rejected,
                "logical execution supervisor is shutting down",
            ));
        }
        let original = input.as_mut().ok_or_else(|| {
            QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original background source is absent",
            )
        })?;
        if !Arc::ptr_eq(&original.issuer, &self.query_ids.issuer)
            || original.input.is_none()
            || original.context.is_none()
        {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original background source belongs to another supervisor or is mounted",
            ));
        }
        let context = Arc::clone(original.context.as_ref().unwrap());
        let source = Arc::clone(
            context
                .background
                .as_ref()
                .expect("typed background context"),
        );
        if !owner
            .as_ref()
            .is_some_and(|owner| source.validate_background_owner(owner))
        {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "background source requires its exact original Root",
            ));
        }
        let cancellation = owner
            .as_ref()
            .unwrap()
            .scope()
            .cancellation()
            .map_err(work_error)?;
        let attempt = self
            .ingress
            .try_mount_in(owner, &mut original.input, &mut original.context);
        if attempt.result().is_err() {
            return Ok(LogicalExecutionStartAttempt {
                attempt: Some(attempt),
                cancellation: Some(cancellation),
                header: None,
            });
        }
        *ready = Some(crate::session_control::BackgroundActionReady {
            source,
            completed: false,
        });
        *exit = Some(LogicalExecutionExitObserver::new(context));
        Ok(LogicalExecutionStartAttempt {
            attempt: Some(attempt),
            cancellation: None,
            header: input.take().map(OriginalStartHeader::Background),
        })
    }

    fn start(
        &self,
        request: QueryExecutionRequest,
        owner: WorkOwner,
        reserved: Option<ReservedExecutionIdentity>,
    ) -> QueryExecutionFuture {
        let mut source = match self.prepare_observed_start(request, reserved) {
            Ok(source) => Some(source),
            Err((error, request, _reserved)) => {
                drop(request);
                owner.complete_after_terminal_cancel_settled();
                return Box::pin(async move { Err(error) });
            }
        };
        let mut owner = Some(owner);
        let mut ready = None;
        let mut exit = None;
        let attempted = self.start_observed_in(&mut source, &mut owner, &mut ready, &mut exit);
        let result = match attempted {
            Ok(attempt) => attempt.finish(),
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            drop(source);
            if let Some(owner) = owner {
                owner.complete_after_terminal_cancel_settled();
            }
            return Box::pin(async move { Err(error) });
        }
        let ready = ready.unwrap();
        let observer = exit.unwrap();
        Box::pin(async move {
            let result = ready.await;
            match result {
                Ok(mut handle) => {
                    handle.retain_original_exit(observer);
                    Ok(handle)
                }
                Err(error) => {
                    drop(observer);
                    Err(error)
                }
            }
        })
    }
}

/// Original prepared input; not an identity, quota or execution grant.
/// Keep this move-only value in the caller's stable original source field.
pub struct OriginalLogicalStartInput {
    input: Option<LogicalRoleInput>,
    response: Option<oneshot::Receiver<Result<ExecutionHandle, QueryExecutionError>>>,
    context: Option<Arc<OriginalLogicalExit>>,
    internal_cut: bool,
    issuer: Arc<()>,
}
pub(super) enum OriginalStartHeader {
    Native(OriginalLogicalStartInput),
    Background(OriginalBackgroundActionInput),
}
impl OriginalStartHeader {
    fn exit(self) {
        match self {
            Self::Native(input) => drop(input),
            Self::Background(input) => drop(input),
        }
    }
}

/// Move-only original background preparation. Keep this in the job's stable
/// input field through acceptance; it owns no second Root or runtime.
pub struct OriginalBackgroundActionInput {
    input: Option<LogicalRoleInput>,
    context: Option<Arc<OriginalLogicalExit>>,
    issuer: Arc<()>,
}

#[must_use = "Finish the original publication tail after caller locks exit"]
pub struct LogicalExecutionStartAttempt {
    attempt: Option<
        novarocks_workload_control::MountAttempt<LogicalRoleInput, Arc<OriginalLogicalExit>>,
    >,
    cancellation: Option<novarocks_workload_control::CancellationView>,
    header: Option<OriginalStartHeader>,
}
impl LogicalExecutionStartAttempt {
    pub fn result(&self) -> Result<(), QueryExecutionError> {
        self.attempt
            .as_ref()
            .unwrap()
            .result()
            .as_ref()
            .map(|_| ())
            .map_err(|e| work_error(e.clone()))
    }
    pub fn finish(mut self) -> Result<(), QueryExecutionError> {
        let result = self.attempt.take().unwrap().finish().map_err(work_error);
        drop(self.cancellation.take());
        if let Some(header) = self.header.take() {
            header.exit();
        }
        result
    }
}

/// Why a start never reached its verdict.
///
/// A statement whose deadline ran out and a statement someone killed reach
/// this point the same way, and the caller acts on the difference: one is the
/// query's own budget, the other is a decision about it. Naming both
/// `Cancelled` here made the answer depend on which of two futures the
/// runtime happened to poll first, since the same expiry also reaches the
/// pre-install wait, which has always classified it.
fn start_cancellation_error(reason: CancellationReason) -> QueryExecutionError {
    cancellation_error(
        reason,
        "logical execution start was cancelled before its start verdict",
    )
}

/// Project only the actual workload cancellation verdict, without inventing a Task phase.
fn cancellation_error(reason: CancellationReason, detail: &'static str) -> QueryExecutionError {
    let reason = match reason {
        CancellationReason::DeadlineExceeded
        | CancellationReason::FrontendDrainDeadlineExceeded => QueryFailureReason::DeadlineExceeded,
        _ => QueryFailureReason::Cancelled,
    };
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    ))
}

#[cfg(test)]
fn rejected_start(owner: WorkOwner, message: &'static str) -> QueryExecutionFuture {
    owner.complete_after_terminal_cancel_settled();
    Box::pin(async move {
        Err(QueryExecutionError::new(
            QueryExecutionErrorKind::Rejected,
            message,
        ))
    })
}

/// Input to the one concrete logical role factory. The original WorkOwner is
/// supplied separately; these aliases retain their existing runtime owners.
/// This type alone is not a complete byte quote or a startup admission grant.
enum LogicalRoleInput {
    Native(NativeLogicalRoleInput),
    Background {
        original_exit: Weak<OriginalLogicalExit>,
        shutdown: watch::Receiver<bool>,
    },
}
struct NativeLogicalRoleInput {
    original_exit: Weak<OriginalLogicalExit>,
    reply: oneshot::Sender<Result<ExecutionHandle, QueryExecutionError>>,
    reserved: Option<ReservedExecutionIdentity>,
    query_ids: Arc<ProcessQueryIdAllocator>,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    clock: Arc<dyn MonotonicClock>,
    registry: LogicalExecutionRuntimeRegistryHandle,
    shutdown: watch::Receiver<bool>,
}

// Design: ADR-0092 (docs/adr/ADR-0092-process-scoped-query-execution-attribution.md)
#[derive(Debug)]
struct ProcessQueryIdAllocator {
    namespace: QueryProcessNamespace,
    issuer: Arc<()>,
}

impl ProcessQueryIdAllocator {
    fn new(namespace: QueryProcessNamespace) -> Self {
        Self {
            namespace,
            issuer: Arc::new(()),
        }
    }

    fn reserve(&self) -> Result<ReservedExecutionIdentity, QueryExecutionError> {
        Ok(ReservedExecutionIdentity::issued(
            self.first_execution()?,
            Arc::clone(&self.issuer),
        ))
    }

    fn consume(
        &self,
        reserved: Option<ReservedExecutionIdentity>,
    ) -> Result<QueryExecutionId, QueryExecutionError> {
        match reserved {
            Some(reserved) => reserved.consume(&self.issuer),
            None => self.first_execution(),
        }
    }

    fn first_execution(&self) -> Result<QueryExecutionId, QueryExecutionError> {
        self.first_execution_from(&NEXT_PROCESS_QUERY_SEQUENCE)
    }

    fn first_execution_from(
        &self,
        sequence: &AtomicU64,
    ) -> Result<QueryExecutionId, QueryExecutionError> {
        let previous = sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current
                    .checked_add(1)
                    .filter(|next| *next <= i64::MAX as u64)
            })
            .map_err(|_| {
                QueryExecutionError::new(
                    QueryExecutionErrorKind::Failed,
                    "logical execution query id sequence is exhausted",
                )
            })?;
        let sequence = LocalQuerySequence::new(previous + 1).expect("validated nonzero sequence");
        QueryExecutionId::new(
            QueryIdAttribution::new(self.namespace, sequence).into_query_id(),
            AttemptId::new(1).expect("initial attempt id is nonzero"),
        )
        .map_err(|error| {
            QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string())
        })
    }
}

fn observe_logical_completion<F, M>(
    registry: &LogicalExecutionRuntimeRegistryHandle,
    bank: &mut RolePollBank<LogicalRoleInput, F, M, Arc<OriginalLogicalExit>>,
    completion: PollTurnCompletion<Arc<OriginalLogicalExit>, Result<(), QueryExecutionError>>,
    first: &mut Option<QueryExecutionError>,
) where
    F: Future<Output = Result<(), QueryExecutionError>>,
    M: Fn(WorkOwner, LogicalRoleInput) -> F,
{
    let failed = matches!(completion.completion(), PollCompletion::Panicked(_));
    if first.is_none() {
        match completion.completion() {
            PollCompletion::Ready(output) => {
                if let Err(error) = output.value() {
                    *first = Some(error.clone());
                }
            }
            PollCompletion::Panicked(_) => {
                *first = Some(QueryExecutionError::new(
                    QueryExecutionErrorKind::Failed,
                    "logical execution supervision task panicked",
                ))
            }
        }
    }
    let source = Arc::clone(completion.context());
    source.role_ended_silent(failed);
    source.retire_background_after_bank_exit_silent(completion.completion());
    source.retire_native_body_silent();
    // An accepted wrapper is installed BEFORE stripping C. Rejection keeps
    // the unchanged whole packet in its SAME original finite bank position.
    let notice = if source.has_failure() {
        registry.publish_original_exit_health_silent(&source)
    } else {
        None
    };
    if source.has_failure() && !source.health_published() {
        let mut original = Some(completion);
        bank.retain_completion_in(&mut original)
            .expect("same completed original footer must return to its own position");
        debug_assert!(original.is_none());
    } else {
        let (_, completion) = completion.into_parts();
        let mut packet = Some(completion);
        source.receive_role_silent(&mut packet);
    }
    registry.mark_original_exit_ready();
    if let Some(notice) = notice {
        notice.notify();
    }
    source.notify();
}

/// The bank is an original wrapper local OUTSIDE the caught borrowed service.
/// Generator cleanup inside run_supervisor can never own/drop this bank.
fn run_supervisor_owned<F, M>(
    registry: LogicalExecutionRuntimeRegistryHandle,
    bank: RolePollBank<LogicalRoleInput, F, M, Arc<OriginalLogicalExit>>,
    turn_budget: NonZeroUsize,
    shutdown: watch::Receiver<bool>,
    retirement: OriginalRootPanic,
    statements: Arc<crate::session_control::StatementProgressSlot>,
) -> impl Future<Output = OriginalRootOutcome> + Send + 'static
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
    M: Fn(WorkOwner, LogicalRoleInput) -> F + Send + 'static,
{
    // This guard is constructed before returning the root Future. Ordinary
    // cancellation/first-poll loss therefore closes ingress too.
    let mut original_bank = OriginalRootBank {
        bank: std::mem::ManuallyDrop::new(Some(bank)),
        retirement,
        statements,
    };
    async move {
        let (outcome, cleanup) = {
            let statements = Arc::clone(&original_bank.statements);
            let mut service = Box::pin(run_supervisor(
                registry,
                original_bank.bank_mut(),
                turn_budget,
                shutdown,
                statements,
            ));
            let outcome = std::future::poll_fn(|cx| {
                match catch_unwind(AssertUnwindSafe(|| service.as_mut().poll(cx))) {
                    Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
                    Ok(Poll::Pending) => Poll::Pending,
                    Err(primary) => Poll::Ready(Err(primary)),
                }
            })
            .await;
            let cleanup = catch_unwind(AssertUnwindSafe(|| drop(service))).err();
            (outcome, cleanup)
        };
        if outcome.is_ok() && cleanup.is_none() {
            // The original service proved its sources exited. Only this path
            // ordinarily destroys the same bank and clears the guard.
            drop(original_bank.take());
            return OriginalRootOutcome::Completed(outcome.unwrap_or_else(|_| unreachable!()));
        }
        let statement_closing =
            catch_unwind(AssertUnwindSafe(|| original_bank.statements.close())).err();
        let bank_closing =
            catch_unwind(AssertUnwindSafe(|| original_bank.bank_mut().stop_ingress())).err();
        let (primary, cleanup) = match outcome {
            Err(primary) => (primary, [cleanup, statement_closing, bank_closing]),
            Ok(_) => (
                cleanup.expect("cleanup is the first failure"),
                [statement_closing, bank_closing, None],
            ),
        };
        let original = original_bank.retirement.clone();
        let bank = original_bank.take();
        original.retain(OriginalRootPanicCustody {
            _bank: Box::new((bank, Arc::clone(&original_bank.statements))),
            bank_layout: std::alloc::Layout::new::<(
                RolePollBank<LogicalRoleInput, F, M, Arc<OriginalLogicalExit>>,
                Arc<crate::session_control::StatementProgressSlot>,
            )>(),
            primary: Some(primary),
            cleanup,
            cancelled: false,
        });
        OriginalRootOutcome::Panicked(original)
    }
}

async fn run_supervisor<F, M>(
    registry: LogicalExecutionRuntimeRegistryHandle,
    bank: &mut RolePollBank<LogicalRoleInput, F, M, Arc<OriginalLogicalExit>>,
    turn_budget: NonZeroUsize,
    mut shutdown: watch::Receiver<bool>,
    statements: Arc<crate::session_control::StatementProgressSlot>,
) -> Result<(), QueryExecutionError>
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
    M: Fn(WorkOwner, LogicalRoleInput) -> F + Send + 'static,
{
    let mut task_failure = None;
    let mut stopping = false;
    let mut original_exit_cursor = super::runtime_registry::OriginalExitCursor::default();
    let mut statement_cursor = crate::session_control::StatementProgressCursor::default();
    loop {
        if !stopping && (*shutdown.borrow() || shutdown.has_changed().is_err()) {
            // The original State serializes this cut with publication. Every
            // already accepted input is visible; later attempts must refuse.
            bank.stop_ingress();
            statements.close();
            stopping = true;
        }

        std::future::poll_fn(|cx| {
            let _ = statements.poll_turn(&mut statement_cursor, turn_budget, cx);
            Poll::Ready(())
        })
        .await;
        registry.progress_root_ack_tails();
        if let Some(completion) = bank.progress_turn(turn_budget) {
            observe_logical_completion(&registry, bank, completion, &mut task_failure);
        }
        std::future::poll_fn(|cx| {
            let _ = registry.poll_original_exit_turn(&mut original_exit_cursor, turn_budget, cx);
            Poll::Ready(())
        })
        .await;
        if let Some(error) = registry.process_health() {
            task_failure.get_or_insert(error);
        }
        if stopping
            && !bank.has_owned_sources_after_stop().map_err(work_error)?
            && registry.root_ack_tail_count() == 0
            && !registry.original_exit_pending()
            && !statements.has_sources()
        {
            break;
        }

        // Every finite service turn yields, including a continuously ready
        // FIFO. None from progress_turn is not an idle certificate: poll_ready
        // registers its original waiter then checks the actual FIFO level.
        tokio::task::yield_now().await;
        tokio::select! {
            biased;
            _ = shutdown.changed(), if !stopping => {}
            _ = registry.process_health_changed() => {}
            _ = registry.root_ack_tail_changed() => {}
            _ = registry.original_exit_changed() => {}
            _ = std::future::poll_fn(|cx| statements.poll_turn(&mut statement_cursor, turn_budget, cx)) => {}
            _ = std::future::poll_fn(|cx| registry.poll_original_exit_turn(
                &mut original_exit_cursor, turn_budget, cx)) => {}
            _ = std::future::poll_fn(|cx| bank.poll_ready(cx)) => {}
        }
    }
    // Only bank-owned sources were awaited. Health may still hold an original
    // PollOutput footer; FA observes actual retirement after the real root join
    // while its original progress service remains alive.
    task_failure.map_or(Ok(()), Err)
}

// This noncapturing function item is the actual production future factory.
// Its concrete input, future and output can be quoted and installed together;
// no dummy future type or caller-provided async closure describes this role.
fn make_logical_role(
    owner: WorkOwner,
    input: LogicalRoleInput,
) -> impl Future<Output = Result<(), QueryExecutionError>> + Send {
    match input {
        LogicalRoleInput::Native(input) => {
            futures::future::Either::Left(make_native_logical_role(owner, input))
        }
        LogicalRoleInput::Background {
            original_exit,
            shutdown,
        } => {
            let source = original_exit
                .upgrade()
                .expect("original background bank context");
            let action = Arc::clone(
                source
                    .background
                    .as_ref()
                    .expect("background source installed"),
            );
            // Synchronous pure move from MountedInput to the stable context,
            // before the borrowed role F exists or any background action poll.
            action.install_background_owner(&mut Some(owner));
            futures::future::Either::Right(run_background_action_role(action, shutdown))
        }
    }
}
async fn run_background_action_role(
    source: Arc<crate::session_control::StatementActionSource>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), QueryExecutionError> {
    let mut stopping = false;
    loop {
        if !stopping && (*shutdown.borrow() || shutdown.has_changed().is_err()) {
            stopping = true;
            source.stop_background();
        }
        tokio::select! {
            biased;
            result = std::future::poll_fn(|cx| source.poll_background(cx)) => return result,
            _ = shutdown.changed(), if !stopping => {},
        }
    }
}
async fn make_native_logical_role(
    owner: WorkOwner,
    input: NativeLogicalRoleInput,
) -> Result<(), QueryExecutionError> {
    let original_exit = input.original_exit.clone();
    let consumer = {
        let source = original_exit
            .upgrade()
            .expect("original role source exists");
        let body = source
            .native
            .try_lock()
            .expect("unpolled original Native body");
        body.start
            .as_ref()
            .and_then(crate::api::QueryExecutionRequest::original_internal_consumer)
    };
    let result = run_logical_role(owner, input).await;
    if let Some(consumer) = consumer {
        // The unique role has stopped producing attempts. This also covers
        // every pre-actor refusal: no actor absence is used as an exit receipt.
        // Installed executions already start this close at the actor cut below.
        consumer.begin_local_close();
        if let Err(original) = std::future::poll_fn(|cx| consumer.poll_local_closed(cx)).await {
            original_exit
                .upgrade()
                .expect("original source retains its role")
                .retain_local_consumer_failure_silent(original);
            return first_supervision_error(
                result,
                Err(internal_consumer_panicked(
                    "Original Internal local close remains unresolved",
                )),
            );
        }
    }
    result
}

async fn run_logical_role(
    owner: WorkOwner,
    input: NativeLogicalRoleInput,
) -> Result<(), QueryExecutionError> {
    let NativeLogicalRoleInput {
        original_exit,
        reply,
        reserved,
        query_ids,
        frontend_process_id,
        config,
        clock,
        registry,
        mut shutdown,
    } = input;
    let mut pending_owner = PendingWorkOwner(Some(owner));
    // A queued, never-polled input must not call Native open during shutdown.
    // This preserves the old accepted-mailbox drain verdict before any create.
    if *shutdown.borrow() || shutdown.has_changed().is_err() {
        return fail_uninstalled_start(
            pending_owner,
            reply,
            QueryExecutionError::new(
                QueryExecutionErrorKind::Rejected,
                "logical execution supervisor shut down before starting the accepted request",
            ),
        );
    }
    let requester = pending_owner.owner().cancellation_requester();
    let scope = pending_owner.owner().scope();
    let initial_execution = match query_ids.consume(reserved) {
        Ok(execution) => execution,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, error),
    };
    // Only this original body is strongly borrowed by the role. It has no
    // backedge to Source; C keeps its strong anchor outside this Future.
    let native_body = match original_exit.upgrade() {
        Some(source) => Arc::clone(&source.native),
        None => {
            return fail_uninstalled_start(
                pending_owner,
                reply,
                QueryExecutionError::new(
                    QueryExecutionErrorKind::Failed,
                    "original Native source is absent",
                ),
            );
        }
    };
    let mut native_guard = native_body.lock_owned().await;
    let body = &mut *native_guard;
    let request = body.start.as_ref().expect("original stable start input");
    if let Err(error) = request.validate_internal_consumer() {
        return fail_uninstalled_start(pending_owner, reply, error);
    }
    if let Err(error) = request.validate_result_capacity(&scope) {
        return fail_uninstalled_start(pending_owner, reply, error);
    }
    if !request.native_parts_match() {
        return fail_uninstalled_start(
            pending_owner,
            reply,
            QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "original Native seed differs from its frozen description",
            ),
        );
    }
    // into_parts only checks the atomically frozen seed/plan identity. Its
    // private constructor already validated that relation; there is no foreign
    // callback between this take and all original stable destination fields.
    let (description, seed, consumer, capacity) = body.start.take().unwrap().into_parts();
    body.description = Some(Arc::new(description));
    body.seed = Some(seed);
    body.consumer = consumer;
    body.capacity = capacity;
    body.streamed_computation = body
        .capacity
        .as_ref()
        .and_then(|capacity| capacity.streamed_computation_producer());
    let description = Arc::clone(body.description.as_ref().unwrap());
    let internal_consumer = body.consumer.clone();
    let internal = internal_consumer.is_some();
    let active_plan = super::ActiveLogicalPlan::activate(description.completed_candidate().clone());
    let result_schema = match description.output() {
        OutputContract::Rows(fields) => {
            let mut schema = ResultSchema::new(Arc::clone(fields));
            let plan = description.completed_candidate().plan();
            if let Some(novarocks_physical_plan::FragmentSink::RootResult(root)) = plan
                .result_port()
                .and_then(|port| plan.fragments().get(&port.fragment))
                .map(|fragment| fragment.sink())
                && let novarocks_result_contract::FrozenRootOutput::ClientRows(render) =
                    root.output()
            {
                schema = schema.with_client_render(render.clone());
            }
            Some(schema)
        }
        OutputContract::CompletionOnly => None,
    };
    if !internal && result_schema.is_none() && description.recovery() != RecoveryMode::NoRecovery {
        return fail_uninstalled_start(
            pending_owner,
            reply,
            QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "completion-only logical execution cannot restart as a row attempt",
            ),
        );
    }
    let cancellation = match scope.cancellation() {
        Ok(cancellation) => cancellation,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, work_error(error)),
    };
    let (request, acceptance) = LogicalNativeOpenRequest::issue(
        initial_execution,
        Arc::clone(&description),
        scope.id(),
        cancellation.clone(),
        body.seed.take().unwrap(),
    );
    body.open = Some(request);
    body.acceptance = Some(acceptance);
    if let Some(source) = original_exit.upgrade() {
        source.native_pre_actor_started_silent();
    }
    let opened = body
        .port
        .as_ref()
        .unwrap()
        .bind_in(&mut body.open, &mut body.session);
    if let Err(error) = opened {
        return fail_uninstalled_start(pending_owner, reply, native_open_error(error));
    }
    if let Err(error) = body
        .acceptance
        .as_ref()
        .unwrap()
        .validate(body.session.as_ref().unwrap())
    {
        return fail_uninstalled_start(pending_owner, reply, contract_error(error));
    }
    drop(body.acceptance.take());
    body.native_actions = match body
        .capacity
        .as_ref()
        .map(|capacity| capacity.native_action_authority())
        .transpose()
    {
        Ok(source) => source,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, work_error(error)),
    };
    body.session
        .as_mut()
        .unwrap()
        .set_result_capacity_in(&mut body.capacity);
    let super::original_exit::OriginalNativePreparation {
        session,
        dormant,
        pending_activated,
        ..
    } = body;
    let session = session.as_mut().unwrap();
    if let Err(error) = session.begin_preparation(
        initial_execution,
        crate::api::NativeAttemptTopologyRequirement::LiveSnapshot,
    ) {
        return fail_uninstalled_start(pending_owner, reply, contract_error(error));
    }
    match await_before_actor_install(
        session.prepare_pending(),
        cancellation.clone(),
        &mut shutdown,
        &requester,
    )
    .await
    {
        PreInstallWait::Completed(Ok(())) => {}
        PreInstallWait::Completed(Err(error)) => {
            return fail_uninstalled_start(pending_owner, reply, native_prepare_error(error));
        }
        PreInstallWait::Cancelled(reason) => {
            return fail_uninstalled_start(
                pending_owner,
                reply,
                pre_install_cancellation_error(reason),
            );
        }
    }
    let prepared = match session.accept_pending_in(dormant) {
        Ok(prepared) => prepared,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, contract_error(error)),
    };
    debug_assert_eq!(prepared.execution, initial_execution);
    let schedule = match build_attempt_schedule(
        initial_execution,
        frontend_process_id,
        &prepared.eligible_backends,
        &prepared.scheduling,
        #[cfg(novarocks_dev_controls)]
        dormant
            .as_mut()
            .test_preferred_backend_process(initial_execution),
    ) {
        Ok(schedule) => schedule,
        Err(error) => {
            return fail_uninstalled_start(
                pending_owner,
                reply,
                QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string()),
            );
        }
    };
    let stage = match scope.track_stage(Stage::Execution) {
        Ok(stage) => stage,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, work_error(error)),
    };
    let contexts = schedule.contexts().to_vec();
    let original_exit = match original_exit.upgrade() {
        Some(source) => source,
        None => {
            return fail_uninstalled_start(
                pending_owner,
                reply,
                QueryExecutionError::new(
                    QueryExecutionErrorKind::Failed,
                    "original fixed role lost its exit source",
                ),
            );
        }
    };
    let reservation = match registry.reserve_with_original_exit(
        initial_execution,
        contexts.clone(),
        Some(original_exit),
    ) {
        Ok(reservation) => reservation,
        Err(error) => {
            drop(stage);
            return fail_uninstalled_start(pending_owner, reply, registry_error(error));
        }
    };
    let row_carrier = if internal || result_schema.is_some() {
        match description.row_carrier() {
            Ok(carrier) => Some(carrier),
            Err(error) => {
                drop(stage);
                return fail_uninstalled_start(
                    pending_owner,
                    reply,
                    QueryExecutionError::new(QueryExecutionErrorKind::InvalidRequest, error),
                );
            }
        }
    } else {
        None
    };
    let max_establish_authorizations = config.max_establish_authorizations_per_context;
    let actor_config = if internal {
        let config_result = match description.recovery() {
            RecoveryMode::NoRecovery => LogicalExecutionActorConfig::no_recovery_completion(
                initial_execution,
                description.effect(),
                config.actor_mailbox_capacity,
                contexts,
                config.max_admission_issues_per_context,
                max_establish_authorizations,
                pending_owner.take(),
                stage,
            ),
            RecoveryMode::RestartAttemptBeforeVisibility => match session.replacement_effect_port()
            {
                Some(replacement) => {
                    LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
                        initial_execution,
                        config.actor_mailbox_capacity,
                        contexts,
                        config.max_admission_issues_per_context,
                        max_establish_authorizations,
                        config.rows.max_attempts,
                        replacement,
                        pending_owner.take(),
                        stage,
                        config.rows.replacement_reservation_valid_for,
                    )
                }
                None => Err(super::LogicalExecutionActorError::InvariantViolation),
            },
        };
        config_result
            .and_then(|actor| {
                actor.with_internal_computation(&description, config.rows.delivery_capacity)
            })
            .map(|actor| {
                actor.with_internal_observation_budgets(
                    crate::api::ComputationObservationBudgets::new(
                        config.diagnostic_timeout,
                        config.remote_cleanup_timeout,
                    ),
                )
            })
    } else {
        match (&result_schema, description.recovery()) {
            (None, RecoveryMode::NoRecovery) => {
                LogicalExecutionActorConfig::no_recovery_completion(
                    initial_execution,
                    description.effect(),
                    config.actor_mailbox_capacity,
                    contexts,
                    config.max_admission_issues_per_context,
                    max_establish_authorizations,
                    pending_owner.take(),
                    stage,
                )
            }
            (Some(schema), RecoveryMode::NoRecovery) => {
                LogicalExecutionActorConfig::single_attempt_read_rows(
                    initial_execution,
                    config.actor_mailbox_capacity,
                    contexts,
                    config.max_admission_issues_per_context,
                    max_establish_authorizations,
                    pending_owner.take(),
                    stage,
                    schema.clone(),
                    config.rows.delivery_capacity,
                    row_carrier.expect("row output carrier was validated"),
                )
            }
            (Some(schema), RecoveryMode::RestartAttemptBeforeVisibility) => {
                let replacement = session
                    .replacement_effect_port()
                    .ok_or(super::LogicalExecutionActorError::InvariantViolation);
                match replacement {
                    Ok(replacement) => {
                        LogicalExecutionActorConfig::read_only_pre_visibility_recovery_rows(
                            initial_execution,
                            config.actor_mailbox_capacity,
                            contexts,
                            config.max_admission_issues_per_context,
                            max_establish_authorizations,
                            config.rows.max_attempts,
                            replacement,
                            pending_owner.take(),
                            stage,
                            config.rows.replacement_reservation_valid_for,
                            schema.clone(),
                            config.rows.delivery_capacity,
                            row_carrier.expect("row output carrier was validated"),
                        )
                    }
                    Err(error) => Err(error),
                }
            }
            (None, RecoveryMode::RestartAttemptBeforeVisibility) => {
                unreachable!("completion-only recovery was rejected before actor construction")
            }
        }
    };
    let actor_config = match actor_config {
        Ok(actor_config) => actor_config
            .with_native_action_authority(body.native_actions.take())
            .with_clock(Arc::clone(&clock))
            .with_abort_query_context_effect_port(
                session.abort_effect_port(),
                max_establish_authorizations,
            ),
        Err(error) => {
            let _ = reply.send(Err(actor_error(error)));
            return Ok(());
        }
    };
    let installed = match reservation.spawn_and_install(actor_config) {
        Ok(installed) => installed,
        Err(error) => {
            let error = registry_error(error);
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }
    };
    let (registration, initial, output) = installed.into_parts();
    let actor = match registry.actor(&registration) {
        Ok(actor) => actor,
        Err(error) => {
            let error = registry_error(error);
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }
    };
    let outcome = actor.internal_outcome();
    let (reply, internal_reply) = if internal {
        let (local_reply, local_result) = oneshot::channel();
        (local_reply, Some((reply, local_result)))
    } else {
        (reply, None)
    };
    let outcome_requester = requester.clone();
    let local_consumer = internal_consumer.clone();
    let supervision = async move {
        let dormant = dormant;
        let mut active_custody = match registry.reserve_active_native_owner(
            &registration,
            schedule.execution(),
            schedule.contexts(),
        ) {
            Ok(reservation) => Some(reservation),
            Err(error) => {
                let error = registry_error(error);
                let _ = reply.send(Err(error.clone()));
                let actor_result = actor
                    .initialization_failed_with_error(initial, error.clone())
                    .await
                    .map(|_| ())
                    .or_else(actor_terminal_refusal);
                let convergence =
                    converge_dormant(dormant, cancellation, &mut shutdown, &requester).await;
                if convergence.is_err() {
                    return first_supervision_error(
                        Err(error),
                        first_supervision_error(actor_result, convergence),
                    );
                }
                dormant
                    .retire()
                    .expect("original dormant cleanup has proven its own exit");
                let retire_result = retire_logical(&registry, registration).await;
                return first_supervision_error(
                    Err(error),
                    first_supervision_error(actor_result, retire_result),
                );
            }
        };
        assert!(
            pending_activated.is_none(),
            "one original activation output"
        );
        let activation = await_with_shutdown(
            activate_dormant(
                dormant,
                pending_activated,
                &schedule,
                None,
                cancellation.clone(),
            ),
            &mut shutdown,
            &requester,
        )
        .await;
        let (mut active, rows_runtime) = match activation {
            Ok(Ok(())) => {
                // The original poll author installed this whole return before
                // dropping its Future. Stage that SAME Source field now.
                active_custody
                    .as_ref()
                    .expect("reserved before activation")
                    .stage_activated_in(pending_activated)
                    .expect("same original activation slot");
                dormant
                    .retire()
                    .expect("successful original activation has no dormant failure");
                active_custody
                    .as_mut()
                    .expect("reserved before activation")
                    .finish_activated_in(pending_activated)
                    .expect("same original activation output")
            }
            Ok(Err(failure)) => {
                drop(active_custody.take());
                session.retire_preparation();
                let error = failure.into_failure().error().clone();
                let _ = reply.send(Err(error.clone()));
                let actor_result = actor
                    .initialization_failed_with_error(initial, error.clone())
                    .await
                    .map(|_| ())
                    .or_else(actor_terminal_refusal);
                let convergence =
                    converge_dormant(dormant, cancellation, &mut shutdown, &requester).await;
                if convergence.is_err() {
                    return first_supervision_error(actor_result, convergence);
                }
                dormant
                    .retire()
                    .expect("original dormant cleanup has proven its own exit");
                let retire_result = retire_logical(&registry, registration).await;
                return first_supervision_error(actor_result, retire_result);
            }
            Err(()) => {
                drop(active_custody.take());
                session.retire_preparation();
                let error = native_future_panicked("Native attempt activation panicked");
                let _ = reply.send(Err(error.clone()));
                let actor_result = actor
                    .initialization_failed_with_error(initial, error.clone())
                    .await
                    .map(|_| ())
                    .or_else(actor_terminal_refusal);
                let convergence =
                    converge_dormant(dormant, cancellation, &mut shutdown, &requester).await;
                if convergence.is_err() {
                    return first_supervision_error(
                        Err(error),
                        first_supervision_error(actor_result, convergence),
                    );
                }
                dormant
                    .retire()
                    .expect("original dormant cleanup has proven its own exit");
                let retire_result = retire_logical(&registry, registration).await;
                return first_supervision_error(
                    Err(error),
                    first_supervision_error(actor_result, retire_result),
                );
            }
        };
        match (internal || result_schema.is_some(), rows_runtime) {
            (true, Some(rows_runtime)) => {
                let running = match actor.activate(initial.ready()).await {
                    Ok(running) => running,
                    Err(error) => {
                        session.retire_preparation();
                        let supervision = actor_terminal_refusal(error);
                        if supervision.is_err() {
                            let _ = requester.request(CancellationReason::Requested);
                        }
                        let error = actor_error(error);
                        let convergence = converge_supervised_active(
                            &mut active,
                            cancellation,
                            &mut shutdown,
                            &requester,
                            config.remote_cleanup_timeout(),
                        )
                        .await;
                        drop(active);
                        let convergence_result = record_reported_active_convergence(
                            &registry,
                            &registration,
                            schedule.contexts(),
                            convergence,
                        )
                        .await;
                        let _ = reply.send(Err(error.clone()));
                        let retire_result = retire_logical(&registry, registration).await;
                        return first_supervision_error(
                            supervision,
                            first_supervision_error(convergence_result, retire_result),
                        );
                    }
                };
                let internal_output = match internal_consumer {
                    Some(consumer) => {
                        drop(reply);
                        Some(InternalExecutionConsumer {
                            consumer,
                            accepted: None,
                        })
                    }
                    None => {
                        let handle = ExecutionHandle::new(requester.clone(), output.into_output());
                        let _ = reply.send(Ok(handle));
                        None
                    }
                };
                let actor_result = supervise_rows(
                    &actor,
                    session,
                    dormant,
                    pending_activated,
                    Arc::clone(&description),
                    scope,
                    frontend_process_id,
                    config,
                    &registry,
                    &registration,
                    &mut shutdown,
                    &requester,
                    active_plan,
                    internal_output,
                    Arc::clone(&clock),
                    RowsAttempt {
                        schedule,
                        active,
                        running,
                        runtime: rows_runtime,
                    },
                )
                .await;
                if dormant.drop_unproven() {
                    // A failed replacement's original body remains in Source.
                    // No later actor retirement may manufacture its closure or
                    // prevent that same Source from publishing its health fact.
                    return first_supervision_error(
                        actor_result,
                        Err(dormant_unproven_error(dormant)),
                    );
                }
                let retire_result = retire_logical(&registry, registration).await;
                return first_supervision_error(actor_result, retire_result);
            }
            (true, None) => {
                session.retire_preparation();
                let error = result_protocol_error(
                    "Native activated a row execution without its root result runtime",
                );
                let actor_result = actor
                    .initialization_failed_with_error(initial, error.clone())
                    .await
                    .map(|_| ())
                    .or_else(actor_terminal_refusal);
                // Publish this already authored startup failure before its
                // internal stop signal can race the caller's start waiter.
                let _ = reply.send(Err(error.clone()));
                let _ = requester.request(CancellationReason::Requested);
                let convergence = converge_supervised_active(
                    &mut active,
                    cancellation,
                    &mut shutdown,
                    &requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                drop(active);
                let convergence_result = record_reported_active_convergence(
                    &registry,
                    &registration,
                    schedule.contexts(),
                    convergence,
                )
                .await;
                let retire_result = retire_logical(&registry, registration).await;
                return first_supervision_error(
                    Err(error),
                    first_supervision_error(
                        actor_result,
                        first_supervision_error(convergence_result, retire_result),
                    ),
                );
            }
            (false, Some(_)) => {
                session.retire_preparation();
                let error = result_protocol_error(
                    "Native attached a root result runtime to a completion-only execution",
                );
                let actor_result = actor
                    .initialization_failed_with_error(initial, error.clone())
                    .await
                    .map(|_| ())
                    .or_else(actor_terminal_refusal);
                let _ = requester.request(CancellationReason::Requested);
                let convergence = converge_supervised_active(
                    &mut active,
                    cancellation,
                    &mut shutdown,
                    &requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                drop(active);
                let convergence_result = record_reported_active_convergence(
                    &registry,
                    &registration,
                    schedule.contexts(),
                    convergence,
                )
                .await;
                let _ = reply.send(Err(error.clone()));
                let retire_result = retire_logical(&registry, registration).await;
                return first_supervision_error(
                    Err(error),
                    first_supervision_error(
                        actor_result,
                        first_supervision_error(convergence_result, retire_result),
                    ),
                );
            }
            (false, None) => {}
        }
        let running = match actor.activate(initial.ready()).await {
            Ok(running) => running,
            Err(error) => {
                session.retire_preparation();
                let supervision = actor_terminal_refusal(error);
                if supervision.is_err() {
                    let _ = requester.request(CancellationReason::Requested);
                }
                let error = actor_error(error);
                let convergence = converge_supervised_active(
                    &mut active,
                    cancellation,
                    &mut shutdown,
                    &requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                drop(active);
                let convergence_result = record_reported_active_convergence(
                    &registry,
                    &registration,
                    schedule.contexts(),
                    convergence,
                )
                .await;
                let _ = reply.send(Err(error.clone()));
                let retire_result = retire_logical(&registry, registration).await;
                return first_supervision_error(
                    supervision,
                    first_supervision_error(convergence_result, retire_result),
                );
            }
        };
        let drive = NativeAttemptDrive::new(&running);
        let original_run = active.original_exit();
        let terminal = await_with_shutdown(
            catch_native_run_panic(active.as_mut(), &drive, cancellation.clone(), &original_run),
            &mut shutdown,
            &requester,
        )
        .await;
        active.begin_terminal_cut(Instant::now());
        session.retire_preparation();
        let (actor_result, convergence) = match terminal {
            Ok(NativeAttemptTerminal::Completed) => {
                // The logical conclusion is the client-visible terminal boundary.
                // It must precede bounded remote observation so the statement
                // owner can release its warehouse query permit while FE cleanup
                // continues independently.
                let actor_result = match actor.complete_attempt(running).await {
                    Ok(super::LogicalConclusion::Succeeded) => {
                        let handle = ExecutionHandle::new(requester.clone(), output.into_output());
                        let _ = reply.send(Ok(handle));
                        Ok(())
                    }
                    Ok(conclusion) => {
                        let error = QueryExecutionError::new(
                            QueryExecutionErrorKind::Failed,
                            format!("completion execution concluded as {conclusion:?}"),
                        );
                        let _ = reply.send(Err(error.clone()));
                        Err(error)
                    }
                    Err(error) => {
                        let supervision = actor_terminal_refusal(error);
                        let _ = reply.send(Err(actor_error(error)));
                        supervision
                    }
                };
                let convergence = converge_supervised_active(
                    &mut active,
                    cancellation,
                    &mut shutdown,
                    &requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                drop(active);
                (actor_result, convergence)
            }
            Ok(NativeAttemptTerminal::Failed(failure)) => {
                let client_error = failure.error().clone();
                let actor_result = actor
                    .fail_attempt_with_error(running, client_error.clone())
                    .await
                    .map(|_| ())
                    .or_else(actor_terminal_refusal);
                let _ = reply.send(Err(client_error));
                let convergence = converge_supervised_active(
                    &mut active,
                    cancellation,
                    &mut shutdown,
                    &requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                drop(active);
                (actor_result, convergence)
            }
            Err(()) => {
                let error = native_future_panicked("Native attempt run panicked");
                let actor_result = actor
                    .fail_attempt_with_error(running, error.clone())
                    .await
                    .map(|_| ())
                    .or_else(actor_terminal_refusal);
                let _ = reply.send(Err(error.clone()));
                let convergence = converge_supervised_active(
                    &mut active,
                    cancellation,
                    &mut shutdown,
                    &requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                drop(active);
                (
                    first_supervision_error(Err(error), actor_result),
                    convergence,
                )
            }
        };
        let convergence_result = record_reported_active_convergence(
            &registry,
            &registration,
            schedule.contexts(),
            convergence,
        )
        .await;
        let retire_result = retire_logical(&registry, registration).await;
        first_supervision_error(
            actor_result,
            first_supervision_error(convergence_result, retire_result),
        )
    };
    match internal_reply {
        Some((reply, local_result)) => {
            supervise_internal_outcome(
                supervision,
                outcome,
                outcome_requester,
                reply,
                local_result,
                local_consumer.expect("validated internal execution has its original consumer"),
            )
            .await
        }
        None => supervision.await,
    }
}

/// Relay the actor's logical verdict while retaining the original supervisor
/// future through its actual exit. A local branch reply is not a competing
/// terminal authority while the actor's outcome owner is still alive.
async fn supervise_internal_outcome(
    supervision: impl Future<Output = Result<(), QueryExecutionError>>,
    mut outcome: Option<
        watch::Receiver<Option<Result<crate::api::ComputationReadyToken, QueryExecutionError>>>,
    >,
    requester: novarocks_workload_control::WorkCancellationRequester,
    reply: oneshot::Sender<Result<ExecutionHandle, QueryExecutionError>>,
    mut local_result: oneshot::Receiver<Result<ExecutionHandle, QueryExecutionError>>,
    consumer: Arc<dyn crate::api::InternalComputationConsumer>,
) -> Result<(), QueryExecutionError> {
    tokio::pin!(supervision);
    let mut reply = Some(reply);
    let mut supervision_result: Option<Result<(), QueryExecutionError>> = None;
    let mut local_started = false;
    let mut local_finished = false;
    let mut local_failure = None;
    loop {
        if let Some(verdict) = outcome
            .as_ref()
            .and_then(|outcome| outcome.borrow().clone())
        {
            if !local_started {
                // This is the statement outcome, published only after the
                // original actor revoked production/transport issue rights.
                // Attempt failure or a vanished watch is not this cut.
                consumer.begin_local_close();
                local_started = true;
            }
            if let Some(reply) = reply.take() {
                let _ = reply.send(
                    verdict
                        .map(|token| ExecutionHandle::computation_ready(requester.clone(), token)),
                );
            }
        }
        if supervision_result.is_some() && (!local_started || local_finished) {
            let result = supervision_result.take().unwrap();
            if let Some(reply) = reply.take() {
                let verdict = outcome
                    .as_ref()
                    .and_then(|outcome| outcome.borrow().clone());
                let candidate = match verdict {
                    Some(verdict) => verdict
                        .map(|token| ExecutionHandle::computation_ready(requester.clone(), token)),
                    None => local_result.try_recv().unwrap_or_else(|_| {
                        Err(result.as_ref().err().cloned().unwrap_or_else(|| result_protocol_error(
                            "internal computation lost its logical outcome owner before readiness",
                        )))
                    }),
                };
                let _ = reply.send(candidate);
            }
            // The original Source still owns the consumer and its raw local
            // cause. The role's final local-close author retains that failure
            // before its original completion can be published.
            return if local_failure.is_some() {
                first_supervision_error(
                    result,
                    Err(internal_consumer_panicked(
                        "Original Internal local close remains unresolved",
                    )),
                )
            } else {
                result
            };
        }
        tokio::select! {
            biased;
            changed = wait_for_internal_ready(outcome.as_mut()),
                if outcome.is_some() && (!local_started || reply.is_some()) => {
                if changed.is_err() {
                    // Preserve a final retained verdict. Losing an undecided
                    // sender never authorizes a local close.
                    let final_verdict = outcome.as_ref().and_then(|outcome| outcome.borrow().clone());
                    if final_verdict.is_none() {
                        outcome = None;
                    }
                }
            }
            closed = std::future::poll_fn(|cx| consumer.poll_local_closed(cx)),
                if local_started && !local_finished => {
                local_finished = true;
                local_failure = closed.err();
            }
            candidate = &mut local_result, if outcome.is_none() && reply.is_some() => {
                if let Some(reply) = reply.take() {
                    let _ = reply.send(candidate.unwrap_or_else(|_| Err(result_protocol_error(
                        "internal computation lost its logical outcome owner before readiness",
                    ))));
                }
            }
            result = &mut supervision, if supervision_result.is_none() => {
                supervision_result = Some(result);
            }
        }
    }
}

struct RowsAttempt {
    schedule: super::AttemptSchedule,
    active: SupervisedActiveNativeOwner,
    running: super::RunningAttemptPermit,
    runtime: NativeRowsAttemptRuntime,
}

type ResidualRowsConvergence = JoinSet<(
    Box<[novarocks_execution_contract::QueryContextRef]>,
    ActiveAttemptConvergence,
)>;

/// The supervisor's local outcome for an active Native attempt. Native owns
/// only positive Worker/process closure evidence; bounded FE observation is a
/// Query Application fact and therefore has a separate variant.
#[derive(Debug)]
enum ActiveAttemptConvergence {
    Evidence(NativeAttemptConvergence),
    RemoteTrackingEnded {
        exact_contexts: Box<[novarocks_execution_contract::QueryContextRef]>,
    },
    Unresolved(StatementNativeHealthFailure),
    Recorded(Box<Result<(), QueryExecutionError>>),
}

fn supervise_residual_rows_attempt(
    residuals: &mut ResidualRowsConvergence,
    mut active: SupervisedActiveNativeOwner,
    contexts: Box<[novarocks_execution_contract::QueryContextRef]>,
    cancellation: novarocks_workload_control::CancellationView,
    mut shutdown: watch::Receiver<bool>,
    requester: novarocks_workload_control::WorkCancellationRequester,
    cleanup_timeout: Duration,
) {
    residuals.spawn(async move {
        let convergence = converge_supervised_active(
            &mut active,
            cancellation,
            &mut shutdown,
            &requester,
            cleanup_timeout,
        )
        .await;
        (contexts, convergence)
    });
}

async fn converge_rows_attempts(
    active: &mut SupervisedActiveNativeOwner,
    active_contexts: &[novarocks_execution_contract::QueryContextRef],
    residuals: &mut ResidualRowsConvergence,
    cancellation: novarocks_workload_control::CancellationView,
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    cleanup_timeout: Duration,
) -> Result<(), QueryExecutionError> {
    let active_convergence =
        converge_supervised_active(active, cancellation, shutdown, requester, cleanup_timeout)
            .await;
    let result = record_reported_active_convergence(
        registry,
        registration,
        active_contexts,
        active_convergence,
    )
    .await;
    first_supervision_error(
        result,
        converge_residual_rows_attempts(residuals, registry, registration).await,
    )
}

async fn converge_residual_rows_attempts(
    residuals: &mut ResidualRowsConvergence,
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
) -> Result<(), QueryExecutionError> {
    let mut result = Ok(());
    while let Some(joined) = residuals.join_next().await {
        let convergence = match joined {
            Ok((contexts, convergence)) => {
                record_reported_active_convergence(registry, registration, &contexts, convergence)
                    .await
            }
            Err(error) => {
                let diagnostic = QueryExecutionError::new(
                    QueryExecutionErrorKind::Failed,
                    format!("residual Native attempt convergence task failed: {error}"),
                );
                registry.record_native_supervision_join_failure(error);
                Err(diagnostic)
            }
        };
        result = first_supervision_error(result, convergence);
    }
    result
}

struct RootRelayWork {
    scope: novarocks_workload_control::WorkScope,
    cancellation: novarocks_workload_control::CancellationView,
}

async fn drive_rows_attempt_until_pump_decision(
    active: &mut SupervisedActiveNativeOwner,
    drive: &NativeAttemptDrive,
    work: RootRelayWork,
    running: super::RunningAttemptPermit,
    root: novarocks_execution_contract::TaskIdentity,
    runtime: NativeRowsAttemptRuntime,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    internal: &mut Option<InternalExecutionConsumer>,
    streamed: Option<crate::admitted_query_context::StreamedComputationCutHandle>,
) -> (
    Result<super::LogicalConclusion, ResultPumpFailure>,
    Option<super::result_pump::StreamedRowsComputedCut>,
) {
    let RootRelayWork {
        scope,
        cancellation,
    } = work;
    let root_ack = match active.reserve_root_ack_tail(running.identity(), root) {
        Ok(reservation) => reservation,
        Err(error) => {
            return (
                Err(super::result_pump::pump_failure(
                    running,
                    super::result_pump::contract_failure(error),
                )),
                None,
            );
        }
    };
    let mut ready = if let Some(internal) = internal.as_ref() {
        let activation = match running.bind_internal_consumer(root).await {
            Ok(activation) => activation,
            Err(error) => {
                return (
                    Err(super::result_pump::pump_actor_failure(
                        running,
                        "bind internal consumer",
                        error,
                    )),
                    None,
                );
            }
        };
        let ready = activation.ready.clone();
        let exit = match activation.supervise_exit().await {
            Ok(exit) => exit,
            Err(error) => {
                drop(activation);
                return (
                    Err(super::result_pump::pump_actor_failure(
                        running,
                        "register internal consumer supervision",
                        error,
                    )),
                    None,
                );
            }
        };
        let activity = match streamed
            .as_ref()
            .map(|source| source.begin_consumer_activity())
            .transpose()
        {
            Ok(activity) => activity,
            Err(error) => {
                drop(activation);
                return (
                    Err(super::result_pump::pump_failure(
                        running,
                        super::result_pump::contract_failure(work_error(error)),
                    )),
                    None,
                );
            }
        };
        let mut input = Some(super::native_active_custody::OriginalConsumerStart {
            consumer: Arc::clone(&internal.consumer),
            activation,
            exit,
            activity,
        });
        if let Err(error) = active.start_consumer_in(&mut input) {
            // Refusal has not moved any original input, and no factory was called.
            drop(input);
            return (
                Err(super::result_pump::pump_failure(
                    running,
                    super::result_pump::contract_failure(error),
                )),
                None,
            );
        }
        Some(ready)
    } else {
        None
    };
    let (terminal_sender, terminal_source) = super::native_attempt_terminal_channel();
    let mut terminal_sender = Some(terminal_sender);
    let original_run = active.original_exit();
    let native_run = catch_native_run_panic(active.as_mut(), drive, cancellation, &original_run);
    let pump_ready = ready.clone();
    let pump = async move {
        if let Some(ready) = pump_ready {
            super::result_pump::run_internal_root_relay(
                running,
                root,
                scope,
                runtime.binding,
                runtime.statuses,
                terminal_source,
                root_ack,
                ready,
                streamed,
            )
            .await
        } else {
            super::result_pump::run_streamed_root_relay(
                running,
                root,
                scope,
                runtime.binding,
                runtime.statuses,
                terminal_source,
                root_ack,
                streamed,
            )
            .await
        }
    };
    tokio::pin!(native_run);
    tokio::pin!(pump);
    let pump_result = loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = requester.request(CancellationReason::ServerShutdown);
                }
            }
            outcome = &mut native_run, if terminal_sender.is_some() => {
                let terminal = outcome.unwrap_or_else(|()| {
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                        native_future_panic_input("Native row attempt run panicked"),
                    ))
                });
                terminal_sender
                    .take()
                    .expect("Native terminal sender is present while run is polled")
                    .publish(terminal);
            }
            changed = wait_for_internal_ready(ready.as_mut()), if ready.is_some() => {
                if changed.is_ok() {
                    observe_internal_ready(internal, ready.as_ref());
                }
                ready = None;
            }
            outcome = &mut pump => break outcome,
        }
    };
    observe_internal_ready(internal, ready.as_ref());
    match pump_result {
        Ok(decision) => (Ok(decision.conclusion), decision.streamed_cut),
        Err(error) => (Err(error), None),
    }
}

struct InternalExecutionConsumer {
    consumer: Arc<dyn crate::api::InternalComputationConsumer>,
    accepted: Option<crate::api::ComputationReadyToken>,
}

async fn wait_for_internal_ready(
    ready: Option<
        &mut watch::Receiver<
            Option<Result<crate::api::ComputationReadyToken, QueryExecutionError>>,
        >,
    >,
) -> Result<(), watch::error::RecvError> {
    let ready = ready.expect("ready wait is gated by its observer");
    if ready.borrow().is_some() {
        return Ok(());
    }
    ready.changed().await
}

fn observe_internal_ready(
    internal: &mut Option<InternalExecutionConsumer>,
    ready: Option<
        &watch::Receiver<Option<Result<crate::api::ComputationReadyToken, QueryExecutionError>>>,
    >,
) {
    if let Some(Ok(token)) = ready.and_then(|ready| ready.borrow().clone())
        && let Some(internal) = internal.as_mut()
    {
        internal.accepted = Some(token);
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the attempt supervisor takes exact per-query capabilities and active-attempt facts from the single coordinator assembly boundary"
)]
async fn supervise_rows(
    actor: &super::LogicalExecutionActor,
    session: &mut LogicalNativeSession,
    dormant: &mut crate::api::NativeDormantCustody,
    pending_activated: &mut Option<super::native_active_custody::PendingNativeActivation>,
    description: Arc<crate::preparation::FrozenExecutionDescription>,
    scope: novarocks_workload_control::WorkScope,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    active_plan: super::ActiveLogicalPlan,
    mut internal: Option<InternalExecutionConsumer>,
    clock: Arc<dyn MonotonicClock>,
    attempt: RowsAttempt,
) -> Result<(), QueryExecutionError> {
    let result = supervise_rows_inner(
        actor,
        session,
        dormant,
        pending_activated,
        description,
        scope,
        frontend_process_id,
        config,
        registry,
        registration,
        shutdown,
        requester,
        active_plan,
        &mut internal,
        clock.as_ref(),
        attempt,
    )
    .await;
    // The original process service joins each consumer in its SAME attempt slot.
    // This borrowed wait owns no join and survives cancellation without detaching it.
    registry.wait_registration_consumers(registration).await;
    if let Some(original) = internal.as_ref()
        && let Some(failure) = original.consumer.local_health_failure()
    {
        if registry
            .retain_internal_consumer_health(registration, &original.consumer, &failure)
            .is_err()
        {
            // An impossible stale/occupied receiver cannot discard either
            // original input. Keep this same finite supervisor/consumer,
            // rather than claiming a successful retirement or reminting.
            let _original_custody = (Arc::clone(&original.consumer), failure);
            std::future::pending::<()>().await;
            unreachable!("original Internal health receiver remains unresolved");
        }
        return first_supervision_error(
            result,
            Err(internal_consumer_panicked(
                "Original Internal CPU failure remains in custody",
            )),
        );
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn supervise_rows_inner(
    actor: &super::LogicalExecutionActor,
    session: &mut LogicalNativeSession,
    dormant: &mut crate::api::NativeDormantCustody,
    pending_activated: &mut Option<super::native_active_custody::PendingNativeActivation>,
    description: Arc<crate::preparation::FrozenExecutionDescription>,
    scope: novarocks_workload_control::WorkScope,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    active_plan: super::ActiveLogicalPlan,
    internal: &mut Option<InternalExecutionConsumer>,
    clock: &dyn MonotonicClock,
    mut attempt: RowsAttempt,
) -> Result<(), QueryExecutionError> {
    let streamed_computation = session.streamed_computation_cut_handle();
    let mut residuals = JoinSet::new();
    // One recovery window covers every replacement preparation. A failed
    // successor cannot renew the topology wait by starting another attempt.
    let mut replacement_preparation_deadline = None;
    loop {
        // Every attempt, the first and each replacement, runs the activated
        // plan itself: the one the description froze, by identity.
        if !Arc::ptr_eq(active_plan.plan(), description.completed_candidate().plan()) {
            return Err(QueryExecutionError::from_execution(
                ExecutionFailureInput::new(
                    QueryFailureReason::PreparationProtocol.failure_fact(),
                    SafeDetail::truncating("replacement attempt lost the activated physical plan"),
                    ExecutionFailureSite::Coordinator,
                ),
            ));
        }
        let drive = NativeAttemptDrive::new(&attempt.running);
        let (pump_result, streamed_cut) = drive_rows_attempt_until_pump_decision(
            &mut attempt.active,
            &drive,
            RootRelayWork {
                cancellation: scope.cancellation().map_err(work_error)?,
                scope: scope.clone(),
            },
            attempt.running,
            attempt.schedule.root(),
            attempt.runtime,
            shutdown,
            requester,
            internal,
            streamed_computation.clone(),
        )
        .await;
        attempt.active.begin_terminal_cut(Instant::now());
        if let Some(token) = internal.as_ref().and_then(|internal| internal.accepted) {
            attempt.active.retain_computation_ready(token)?;
            // Consume the budget established at the actor cut. Time spent
            // handing off the ready result cannot renew remote observation.
            let remaining = token
                .convergence_deadline()
                .saturating_duration_since(clock.now());
            attempt
                .active
                .shorten_remote_deadline(Instant::now() + remaining);
        }
        drop(drive);

        match pump_result {
            Ok(
                super::LogicalConclusion::Succeeded | super::LogicalConclusion::ComputationReady,
            ) => {
                // The original pump/driver future has returned and dropped its
                // local producer slots. Only now can its private cut retire P.
                if let Some(cut) = streamed_cut {
                    cut.retire_after_pump_exit().await.map_err(work_error)?;
                }
                session.retire_preparation();
                return converge_rows_attempts(
                    &mut attempt.active,
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
            }
            Ok(conclusion) => {
                session.retire_preparation();
                let convergence = converge_rows_attempts(
                    &mut attempt.active,
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                return first_supervision_error(
                    Err(QueryExecutionError::new(
                        QueryExecutionErrorKind::Failed,
                        format!("row execution concluded as {conclusion:?}"),
                    )),
                    convergence,
                );
            }
            Err(ResultPumpFailure::Concluded(failure)) => {
                session.retire_preparation();

                let convergence = converge_rows_attempts(
                    &mut attempt.active,
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                return if matches!(
                    failure.conclusion(),
                    super::LogicalConclusion::Cancelled | super::LogicalConclusion::Failed
                ) {
                    convergence
                } else {
                    first_supervision_error(Err(failure.error().clone()), convergence)
                };
            }
            Err(ResultPumpFailure::ActorOutcomeUnknown(failure)) => {
                session.retire_preparation();

                let convergence = converge_rows_attempts(
                    &mut attempt.active,
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                return first_supervision_error(Err(failure.error().clone()), convergence);
            }
            Err(ResultPumpFailure::DecisionPending(decision)) => {
                let cancellation = scope.cancellation().map_err(work_error)?;
                let attempts_started =
                    u32::try_from(attempt.schedule.execution().attempt_id().get())
                        .unwrap_or(u32::MAX);
                let recovery = evaluate_recovery(RecoveryInput {
                    mode: description.recovery(),
                    effect: description.effect(),
                    failure: decision.class(),
                    pre_ready_replacement: decision.pre_ready_replacement().filter(|witness| {
                        witness.context().query_execution_id() == attempt.schedule.execution()
                            && decision
                                .error()
                                .execution_failure()
                                .is_some_and(|input| witness.matches_input(input))
                            && cancellation.reason().is_none()
                    }),
                    output_visible: false,
                    attempts_started,
                    max_attempts: config.rows.max_attempts.get(),
                    deadline_reached: matches!(
                        cancellation.reason(),
                        Some(CancellationReason::DeadlineExceeded)
                    ),
                });
                let run_source_failed = attempt
                    .active
                    .original_exit()
                    .upgrade()
                    .is_some_and(|source| source.run_failed());
                if recovery != RecoveryDecision::BeginReplacement || run_source_failed {
                    session.retire_preparation();
                    let actor_result = decision
                        .fail_logical(actor)
                        .await
                        .map(|_| ())
                        .or_else(actor_terminal_refusal);

                    let convergence = converge_rows_attempts(
                        &mut attempt.active,
                        attempt.schedule.contexts(),
                        &mut residuals,
                        cancellation,
                        registry,
                        registration,
                        shutdown,
                        requester,
                        config.remote_cleanup_timeout(),
                    )
                    .await;
                    return first_supervision_error(actor_result, convergence);
                }

                let replacement = match next_attempt(attempt.schedule.execution()) {
                    Ok(replacement) => replacement,
                    Err(error) => {
                        session.retire_preparation();
                        let actor_result = decision
                            .fail_logical(actor)
                            .await
                            .map(|_| ())
                            .or_else(actor_terminal_refusal);
                        let convergence = converge_rows_attempts(
                            &mut attempt.active,
                            attempt.schedule.contexts(),
                            &mut residuals,
                            cancellation,
                            registry,
                            registration,
                            shutdown,
                            requester,
                            config.remote_cleanup_timeout(),
                        )
                        .await;
                        return first_supervision_error(
                            Err(error),
                            first_supervision_error(actor_result, convergence),
                        );
                    }
                };
                let preparation_deadline =
                    *replacement_preparation_deadline.get_or_insert_with(|| {
                        tokio::time::Instant::now() + config.rows.replacement_reservation_valid_for
                    });
                let prepared = prepare_native_attempt(
                    session,
                    dormant,
                    replacement,
                    decision.topology_requirement(),
                    frontend_process_id,
                    shutdown,
                    requester,
                    cancellation.clone(),
                    preparation_deadline,
                )
                .await;
                let replacement_schedule = match prepared {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        session.retire_preparation();
                        let actor_outcome = if internal.is_some() {
                            decision.fail_logical_with_error(actor, error.clone()).await
                        } else {
                            // An already handed-off client stream retains its
                            // originating attempt failure. The separate
                            // preparation verdict remains supervisor health.
                            decision.fail_logical(actor).await
                        };

                        let supervision = preparation_supervision_result(error, actor_outcome);
                        let convergence = converge_rows_attempts(
                            &mut attempt.active,
                            attempt.schedule.contexts(),
                            &mut residuals,
                            cancellation,
                            registry,
                            registration,
                            shutdown,
                            requester,
                            config.remote_cleanup_timeout(),
                        )
                        .await;
                        return first_supervision_error(supervision, convergence);
                    }
                };
                // The exact successor dormant owner binds its complete original
                // dependency union BEFORE the actor dispatches qualification.
                // Every pending constructor/reply stays in that owner; this
                // borrowed wait owns neither the old grant nor a fresh source.
                if let Err(error) = await_with_shutdown(
                    bind_dormant_qualification(
                        dormant,
                        &replacement_schedule,
                        &cancellation,
                        preparation_deadline,
                    ),
                    shutdown,
                    requester,
                )
                .await
                {
                    session.retire_preparation();
                    let actor_outcome = if internal.is_some() {
                        decision.fail_logical_with_error(actor, error.clone()).await
                    } else {
                        decision.fail_logical(actor).await
                    };
                    let supervision = preparation_supervision_result(error, actor_outcome);
                    // No successor qualification was dispatched and no successor
                    // context was registered. Close only its actual local owner.
                    let replacement_convergence =
                        converge_dormant(dormant, cancellation.clone(), shutdown, requester).await;
                    let old_convergence = converge_rows_attempts(
                        &mut attempt.active,
                        attempt.schedule.contexts(),
                        &mut residuals,
                        cancellation,
                        registry,
                        registration,
                        shutdown,
                        requester,
                        config.remote_cleanup_timeout(),
                    )
                    .await;
                    return first_supervision_error(
                        supervision,
                        first_supervision_error(replacement_convergence, old_convergence),
                    );
                }
                let qualification = match decision
                    .begin_replacement(actor, replacement, replacement_schedule.contexts().to_vec())
                    .await
                {
                    Ok((qualification, _)) => qualification,
                    Err(error) => {
                        session.retire_preparation();
                        let replacement_convergence = match converge_dormant(
                            dormant,
                            cancellation,
                            shutdown,
                            requester,
                        )
                        .await
                        {
                            Ok(()) => {
                                record_active_convergence(
                                    registry,
                                    registration,
                                    replacement_schedule.contexts(),
                                )
                                .await
                            }
                            Err(original) => Err(original),
                        };
                        let old_convergence = converge_rows_attempts(
                            &mut attempt.active,
                            attempt.schedule.contexts(),
                            &mut residuals,
                            scope.cancellation().map_err(work_error)?,
                            registry,
                            registration,
                            shutdown,
                            requester,
                            config.remote_cleanup_timeout(),
                        )
                        .await;
                        return first_supervision_error(
                            actor_terminal_refusal(error),
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                };

                supervise_residual_rows_attempt(
                    &mut residuals,
                    attempt.active,
                    attempt.schedule.contexts().to_vec().into_boxed_slice(),
                    cancellation.clone(),
                    shutdown.clone(),
                    requester.clone(),
                    config.remote_cleanup_timeout(),
                );
                let instantiation = match await_with_shutdown(
                    actor.activate_replacement(qualification),
                    shutdown,
                    requester,
                )
                .await
                {
                    Ok(instantiation) => instantiation,
                    Err(error) => {
                        session.retire_preparation();

                        let replacement_convergence = match converge_dormant(
                            dormant,
                            cancellation,
                            shutdown,
                            requester,
                        )
                        .await
                        {
                            Ok(()) => {
                                record_active_convergence(
                                    registry,
                                    registration,
                                    replacement_schedule.contexts(),
                                )
                                .await
                            }
                            Err(original) => Err(original),
                        };
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            actor_terminal_refusal(error),
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                };
                let replacement_admissions = match await_with_shutdown(
                    instantiation.take_replacement_admission_evidence(),
                    shutdown,
                    requester,
                )
                .await
                {
                    Ok(admissions) => admissions,
                    Err(error) => {
                        session.retire_preparation();
                        let replacement_convergence = match converge_dormant(
                            dormant,
                            cancellation,
                            shutdown,
                            requester,
                        )
                        .await
                        {
                            Ok(()) => {
                                record_active_convergence(
                                    registry,
                                    registration,
                                    replacement_schedule.contexts(),
                                )
                                .await
                            }
                            Err(original) => Err(original),
                        };
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            actor_terminal_refusal(error),
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                };
                let mut replacement_custody = match registry.reserve_active_native_owner(
                    registration,
                    replacement_schedule.execution(),
                    replacement_schedule.contexts(),
                ) {
                    Ok(reservation) => Some(reservation),
                    Err(error) => {
                        session.retire_preparation();
                        let error = registry_error(error);
                        let actor_result = actor
                            .initialization_failed_with_error(instantiation, error.clone())
                            .await
                            .map(|_| ())
                            .or_else(actor_terminal_refusal);
                        let replacement_convergence = match converge_dormant(
                            dormant,
                            cancellation,
                            shutdown,
                            requester,
                        )
                        .await
                        {
                            Ok(()) => {
                                record_active_convergence(
                                    registry,
                                    registration,
                                    replacement_schedule.contexts(),
                                )
                                .await
                            }
                            Err(original) => Err(original),
                        };
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            Err(error),
                            first_supervision_error(
                                actor_result,
                                first_supervision_error(replacement_convergence, old_convergence),
                            ),
                        );
                    }
                };
                assert!(
                    pending_activated.is_none(),
                    "one original activation output"
                );
                let activated = await_with_shutdown(
                    activate_dormant(
                        dormant,
                        pending_activated,
                        &replacement_schedule,
                        replacement_admissions,
                        cancellation.clone(),
                    ),
                    shutdown,
                    requester,
                )
                .await;
                match activated {
                    Ok(Ok(())) => {
                        replacement_custody
                            .as_ref()
                            .expect("reserved before replacement activation")
                            .stage_activated_in(pending_activated)
                            .expect("same original replacement slot");
                    }
                    Ok(Err(failure)) => {
                        drop(replacement_custody.take());
                        session.retire_preparation();
                        let error = failure.into_failure().error().clone();
                        let actor_outcome = actor
                            .initialization_failed_with_error(instantiation, error.clone())
                            .await;

                        let supervision = preparation_supervision_result(error, actor_outcome);
                        let replacement_convergence = match converge_dormant(
                            dormant,
                            cancellation,
                            shutdown,
                            requester,
                        )
                        .await
                        {
                            Ok(()) => {
                                record_active_convergence(
                                    registry,
                                    registration,
                                    replacement_schedule.contexts(),
                                )
                                .await
                            }
                            Err(original) => Err(original),
                        };
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            supervision,
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                    Err(()) => {
                        drop(replacement_custody.take());
                        session.retire_preparation();
                        let error = native_future_panicked(
                            "Native replacement attempt activation panicked",
                        );
                        let actor_result = actor
                            .initialization_failed_with_error(instantiation, error.clone())
                            .await
                            .map(|_| ())
                            .or_else(actor_terminal_refusal);
                        let replacement_convergence = match converge_dormant(
                            dormant,
                            cancellation,
                            shutdown,
                            requester,
                        )
                        .await
                        {
                            Ok(()) => {
                                record_active_convergence(
                                    registry,
                                    registration,
                                    replacement_schedule.contexts(),
                                )
                                .await
                            }
                            Err(original) => Err(original),
                        };
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            Err(error),
                            first_supervision_error(
                                actor_result,
                                first_supervision_error(replacement_convergence, old_convergence),
                            ),
                        );
                    }
                };
                dormant
                    .retire()
                    .expect("successful original activation has no dormant failure");
                let (active, rows_runtime) = replacement_custody
                    .as_mut()
                    .expect("reserved before replacement activation")
                    .finish_activated_in(pending_activated)
                    .expect("same original replacement output");
                let Some(rows_runtime) = rows_runtime else {
                    session.retire_preparation();
                    let error =
                        result_protocol_error("Native replacement omitted its root result runtime");
                    let actor_result = actor
                        .initialization_failed_with_error(instantiation, error.clone())
                        .await
                        .map(|_| ())
                        .or_else(actor_terminal_refusal);
                    let mut active = active;
                    let convergence = converge_supervised_active(
                        &mut active,
                        cancellation,
                        shutdown,
                        requester,
                        config.remote_cleanup_timeout(),
                    )
                    .await;
                    drop(active);
                    let replacement_convergence = record_reported_active_convergence(
                        registry,
                        registration,
                        replacement_schedule.contexts(),
                        convergence,
                    )
                    .await;
                    let old_convergence =
                        converge_residual_rows_attempts(&mut residuals, registry, registration)
                            .await;
                    return first_supervision_error(
                        Err(error),
                        first_supervision_error(
                            actor_result,
                            first_supervision_error(replacement_convergence, old_convergence),
                        ),
                    );
                };
                let running = match actor.activate(instantiation.ready()).await {
                    Ok(running) => running,
                    Err(error) => {
                        session.retire_preparation();
                        let mut active = active;
                        let convergence = converge_supervised_active(
                            &mut active,
                            cancellation,
                            shutdown,
                            requester,
                            config.remote_cleanup_timeout(),
                        )
                        .await;
                        let replacement_convergence = record_reported_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                            convergence,
                        )
                        .await;
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            actor_terminal_refusal(error),
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                };
                attempt = RowsAttempt {
                    schedule: replacement_schedule,
                    active,
                    running,
                    runtime: rows_runtime,
                };
            }
        }
    }
}

async fn prepare_native_attempt(
    session: &mut LogicalNativeSession,
    dormant: &mut crate::api::NativeDormantCustody,
    execution: QueryExecutionId,
    topology_requirement: crate::api::NativeAttemptTopologyRequirement,
    frontend_process_id: FrontendProcessId,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    cancellation: novarocks_workload_control::CancellationView,
    deadline: tokio::time::Instant,
) -> Result<super::AttemptSchedule, QueryExecutionError> {
    session
        .begin_preparation(execution, topology_requirement)
        .map_err(contract_error)?;
    {
        let preparation = await_with_shutdown(session.prepare_pending(), shutdown, requester);
        tokio::pin!(preparation);
        tokio::select! {
        biased;
        reason = cancellation.cancelled() => {
            return Err(cancellation_error(
                cancellation.reason().unwrap_or(reason),
                "Native replacement preparation was cancelled",
            ));
        }
        _ = tokio::time::sleep_until(deadline) => {
            return Err(QueryExecutionError::from_execution(ExecutionFailureInput::new(
                QueryFailureReason::RecoveryPreparationBudgetExceeded.failure_fact(),
                SafeDetail::truncating("Native replacement preparation exceeded its recovery budget"),
                ExecutionFailureSite::Coordinator,
            )));
        }
        prepared = &mut preparation => prepared,
    }
    .map_err(native_prepare_error)?;
    }
    let prepared = session.accept_pending_in(dormant).map_err(contract_error)?;
    let schedule = build_attempt_schedule(
        execution,
        frontend_process_id,
        &prepared.eligible_backends,
        &prepared.scheduling,
        #[cfg(novarocks_dev_controls)]
        dormant.as_mut().test_preferred_backend_process(execution),
    )
    .map_err(|error| {
        QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string())
    })?;
    Ok(schedule)
}

fn next_attempt(execution: QueryExecutionId) -> Result<QueryExecutionId, QueryExecutionError> {
    let next = execution.attempt_id().get().checked_add(1).ok_or_else(|| {
        QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "logical execution attempt id space is exhausted",
        )
    })?;
    QueryExecutionId::new(
        execution.query_id(),
        AttemptId::new(next).expect("checked successor attempt id is nonzero"),
    )
    .map_err(|error| QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string()))
}

async fn record_active_convergence(
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    contexts: &[novarocks_execution_contract::QueryContextRef],
) -> Result<(), QueryExecutionError> {
    let mut first_error = None;
    for &context in contexts {
        if let Err(error) = registry
            .observe_worker_stopped_and_context_fenced(registration, context)
            .await
        {
            first_error.get_or_insert_with(|| registry_error(error));
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn record_reported_active_convergence(
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    expected_contexts: &[novarocks_execution_contract::QueryContextRef],
    convergence: ActiveAttemptConvergence,
) -> Result<(), QueryExecutionError> {
    let convergence = match convergence {
        ActiveAttemptConvergence::Recorded(result) => return *result,
        ActiveAttemptConvergence::Unresolved(failure) => {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                failure.detail(),
            ));
        }
        ActiveAttemptConvergence::RemoteTrackingEnded { exact_contexts } => {
            let expected = expected_contexts.iter().copied().collect::<BTreeSet<_>>();
            let reported = exact_contexts.iter().copied().collect::<BTreeSet<_>>();
            if expected != reported || reported.len() != exact_contexts.len() {
                return Err(QueryExecutionError::new(
                    QueryExecutionErrorKind::InvalidRequest,
                    "Native remote tracking omitted or duplicated original contexts",
                ));
            }
            return record_remote_tracking_ended(registry, registration, expected_contexts).await;
        }
        ActiveAttemptConvergence::Evidence(convergence) => convergence,
    };
    let NativeAttemptConvergence::Contexts(facts) = convergence else {
        return record_active_convergence(registry, registration, expected_contexts).await;
    };
    let expected = expected_contexts.iter().copied().collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    for fact in &facts {
        if !expected.contains(&fact.context()) || !observed.insert(fact.context()) {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "Native active convergence returned duplicate or foreign context evidence",
            ));
        }
    }
    if observed != expected {
        return Err(QueryExecutionError::new(
            QueryExecutionErrorKind::InvalidRequest,
            "Native active convergence omitted scheduled context evidence",
        ));
    }
    let mut first_error = None;
    for fact in facts {
        let result = match fact.kind() {
            NativeContextConvergenceKind::NeverEstablished => {
                registry
                    .observe_context_never_established(registration, fact.context())
                    .await
            }
            NativeContextConvergenceKind::WorkerStoppedAndContextFenced => {
                registry
                    .observe_worker_stopped_and_context_fenced(registration, fact.context())
                    .await
            }
            NativeContextConvergenceKind::WorkerProcessReplaced => {
                registry
                    .observe_worker_process_replaced(registration, fact.context())
                    .await
            }
        };
        if let Err(error) = result {
            first_error.get_or_insert_with(|| registry_error(error));
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn record_remote_tracking_ended(
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    contexts: &[novarocks_execution_contract::QueryContextRef],
) -> Result<(), QueryExecutionError> {
    let mut first_error = None;
    for &context in contexts {
        if let Err(error) = registry
            .end_remote_context_tracking(registration, context)
            .await
        {
            first_error.get_or_insert_with(|| registry_error(error));
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// The borrowed polling frame retains only references. The original dormant
/// projection owns the paid binding state and the original custody owns panic.
async fn bind_dormant_qualification(
    custody: &mut crate::api::NativeDormantCustody,
    schedule: &super::AttemptSchedule,
    cancellation: &novarocks_workload_control::CancellationView,
    deadline: tokio::time::Instant,
) -> Result<(), QueryExecutionError> {
    let Some((owner, original)) = custody.activation_parts() else {
        return Err(result_protocol_error(
            "original qualification binding owner is unavailable",
        ));
    };
    let binding = std::future::poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| {
            owner.poll_bind_qualification(schedule, cx)
        })) {
            Ok(Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
            Ok(Poll::Ready(Err(failure))) => {
                Poll::Ready(Err(failure.into_failure().error().clone()))
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                *original = Some(payload);
                Poll::Ready(Err(native_future_panicked(
                    "Native qualification binding panicked",
                )))
            }
        }
    });
    tokio::pin!(binding);
    tokio::select! {
        biased;
        reason = cancellation.cancelled() => Err(cancellation_error(
            cancellation.reason().unwrap_or(reason),
            "Native replacement dependency binding was cancelled",
        )),
        _ = tokio::time::sleep_until(deadline) => {
            Err(QueryExecutionError::from_execution(ExecutionFailureInput::new(
                QueryFailureReason::RecoveryPreparationBudgetExceeded.failure_fact(),
                SafeDetail::truncating("Native replacement dependency binding exceeded its recovery budget"),
                ExecutionFailureSite::Coordinator,
            )))
        }
        outcome = &mut binding => outcome,
    }
}

/// Only this original dormant owner can receive these two causes. Capture
/// into its fixed field before the borrowed Future's actual Drop can unwind.
async fn activate_dormant(
    custody: &mut crate::api::NativeDormantCustody,
    pending_activated: &mut Option<super::native_active_custody::PendingNativeActivation>,
    schedule: &super::AttemptSchedule,
    admissions: Option<Box<[super::ReplacementWorkerAdmissionEvidence]>>,
    cancellation: novarocks_workload_control::CancellationView,
) -> Result<Result<(), crate::api::NativeAttemptActivationFailure>, ()> {
    assert!(
        pending_activated.is_none(),
        "one original activation output"
    );
    let Some((owner, original)) = custody.activation_parts() else {
        // These are real owned arguments. Their Drop may notify WC progress;
        // the original failed owner/cause stays stable before that callback.
        drop(admissions);
        drop(cancellation);
        return Err(());
    };
    let mut future = match catch_unwind(AssertUnwindSafe(move || {
        owner.activate(schedule, admissions, cancellation)
    })) {
        Ok(original_future) => original_future,
        Err(payload) => {
            *original = Some(payload);
            return Err(());
        }
    };
    std::future::poll_fn(move |cx| {
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Ready(Ok(activated))) => {
                // Complete the same original stable handoff BEFORE original F
                // Drop. A secondary Drop panic cannot destroy this new owner.
                *pending_activated = Some(
                    super::native_active_custody::PendingNativeActivation::new(activated),
                );
                Poll::Ready(Ok(Ok(())))
            }
            Ok(Poll::Ready(Err(failure))) => Poll::Ready(Ok(Err(failure))),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                *original = Some(payload);
                Poll::Ready(Err(()))
            }
        }
    })
    .await
}

fn dormant_unproven_error(custody: &crate::api::NativeDormantCustody) -> QueryExecutionError {
    if custody.activation_panic().is_some() {
        native_future_panicked("Native attempt activation panicked")
    } else if custody.convergence_panic().is_some() {
        native_future_panicked("Native dormant attempt convergence panicked")
    } else if let Some(original) = custody.convergence_failure() {
        // This is the process-health projection, not a new Task/SQL verdict.
        // The original typed error/cause remains in the same fixed source slot.
        QueryExecutionError::new(QueryExecutionErrorKind::Failed, original.detail())
    } else {
        result_protocol_error("original dormant cleanup source is unavailable")
    }
}

async fn converge_dormant(
    custody: &mut crate::api::NativeDormantCustody,
    cancellation: novarocks_workload_control::CancellationView,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) -> Result<(), QueryExecutionError> {
    if custody.convergence_completed() {
        // CancellationView Drop can wake the original progress observer. Do
        // that while any original failure is still in its stable fixed slot,
        // before constructing even the safe projection returned below.
        drop(cancellation);
        return if custody.has_failure() {
            Err(dormant_unproven_error(custody))
        } else {
            Ok(())
        };
    }
    if custody.convergence_panic().is_some() || custody.convergence_failure().is_some() {
        drop(cancellation);
        return Err(dormant_unproven_error(custody));
    }
    let outcome = {
        let Some((owner, original)) = custody.convergence_parts() else {
            drop(cancellation);
            return Err(result_protocol_error(
                "original dormant cleanup source is unavailable",
            ));
        };
        match catch_unwind(AssertUnwindSafe(move || owner.converge(cancellation))) {
            Ok(mut future) => {
                // Poll the original F directly. An async-await wrapper would
                // Drop a Ready(Err) F before this original error can be stored.
                let capture = std::future::poll_fn(move |cx| {
                    match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                        Ok(Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
                        Ok(Poll::Ready(Err(failure))) => {
                            *original =
                                Some(crate::api::DormantConvergenceFailure::Reported(failure));
                            Poll::Ready(Err(()))
                        }
                        Ok(Poll::Pending) => Poll::Pending,
                        Err(payload) => {
                            *original =
                                Some(crate::api::DormantConvergenceFailure::PollPanic(payload));
                            Poll::Ready(Err(()))
                        }
                    }
                });
                await_with_shutdown(capture, shutdown, requester).await
            }
            Err(payload) => {
                *original = Some(crate::api::DormantConvergenceFailure::PollPanic(payload));
                Err(())
            }
        }
    };
    if outcome.is_ok() {
        custody.record_convergence_completed();
    }
    if outcome.is_err() || custody.has_failure() {
        // Do not recreate a panicked cleanup Future or wash an activation
        // panic into an exit just because the borrowed cleanup returned Ok.
        return Err(dormant_unproven_error(custody));
    }
    Ok(())
}

async fn converge_supervised_active(
    owner: &mut SupervisedActiveNativeOwner,
    cancellation: novarocks_workload_control::CancellationView,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    cleanup_timeout: Duration,
) -> ActiveAttemptConvergence {
    let request = owner.request(cancellation, cleanup_timeout);
    let failure_custody = owner.convergence_failure_custody();
    let (outcome, deadline) = converge_active_request(
        owner.as_mut(),
        request,
        shutdown,
        requester,
        failure_custody.original(),
    )
    .await;
    owner.shorten_remote_deadline(deadline);
    let result = match outcome {
        ActiveAttemptConvergence::Evidence(evidence) => {
            owner
                .record_and_close(NativeActiveConvergenceClosure::Evidence(evidence))
                .await
        }
        ActiveAttemptConvergence::RemoteTrackingEnded { exact_contexts } => {
            owner
                .record_and_close(NativeActiveConvergenceClosure::RemoteTrackingEnded {
                    exact_contexts,
                })
                .await
        }
        ActiveAttemptConvergence::Unresolved(failure) => Err(owner.retain_unresolved(failure)),
        ActiveAttemptConvergence::Recorded(result) => *result,
    };
    ActiveAttemptConvergence::Recorded(Box::new(result))
}

#[cfg(test)]
async fn converge_active(
    owner: &mut dyn ActiveNativeAttemptOwner,
    cancellation: novarocks_workload_control::CancellationView,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    cleanup_timeout: Duration,
) -> ActiveAttemptConvergence {
    let original = super::NativeConvergenceFailureSlot::default();
    converge_active_request(
        owner,
        NativeActiveConvergenceRequest::new(cancellation, Instant::now() + cleanup_timeout),
        shutdown,
        requester,
        &original,
    )
    .await
    .0
}

async fn converge_active_request(
    owner: &mut dyn ActiveNativeAttemptOwner,
    request: NativeActiveConvergenceRequest,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    original: &super::NativeConvergenceFailureSlot,
) -> (ActiveAttemptConvergence, Instant) {
    let mut deadline = request.remote_observation_deadline();
    let mut shutdown_cut = false;
    loop {
        if *shutdown.borrow() && !shutdown_cut {
            let _ = requester.request(CancellationReason::ServerShutdown);
            deadline = deadline.min(Instant::now());
            shutdown_cut = true;
        }
        enum Turn {
            Closed(
                Result<
                    Result<NativeActiveConvergenceClosure, StatementNativeHealthFailure>,
                    StatementNativeHealthFailure,
                >,
            ),
            Shutdown,
        }
        let turn_deadline = deadline;
        let turn = {
            let convergence = super::catch_native_convergence_panic(
                owner,
                request
                    .clone()
                    .with_remote_observation_deadline(turn_deadline),
                original,
            );
            tokio::pin!(convergence);
            tokio::select! {
                result = &mut convergence => Turn::Closed(result),
                changed = shutdown.changed(), if !shutdown_cut => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = requester.request(CancellationReason::ServerShutdown);
                        deadline = deadline.min(Instant::now());
                        shutdown_cut = true;
                    }
                    Turn::Shutdown
                }
            }
        };
        let result = match turn {
            Turn::Shutdown => continue,
            Turn::Closed(result) => result,
        };
        let outcome = match result {
            Ok(Ok(NativeActiveConvergenceClosure::Evidence(evidence))) => {
                ActiveAttemptConvergence::Evidence(evidence)
            }
            Ok(Ok(NativeActiveConvergenceClosure::RemoteTrackingEnded { exact_contexts })) => {
                ActiveAttemptConvergence::RemoteTrackingEnded { exact_contexts }
            }
            Ok(Err(health)) | Err(health) => ActiveAttemptConvergence::Unresolved(health),
        };
        return (outcome, deadline);
    }
}

/// Poll the actual trait future so its raw panic reaches the existing Source
/// before the actual future is destroyed or the safe business verdict is sent.
async fn catch_native_run_panic(
    owner: &mut dyn ActiveNativeAttemptOwner,
    drive: &NativeAttemptDrive,
    cancellation: novarocks_workload_control::CancellationView,
    original: &Weak<OriginalLogicalExit>,
) -> Result<NativeAttemptTerminal, ()> {
    if original.upgrade().is_some_and(|source| source.run_failed()) {
        drop(cancellation);
        return Err(());
    }
    let mut future = match catch_unwind(AssertUnwindSafe(move || owner.run(drive, cancellation))) {
        Ok(future) => future,
        Err(payload) => {
            retain_native_run_panic(original, payload, false);
            return Err(());
        }
    };
    let result = std::future::poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Ready(terminal)) => Poll::Ready(Ok(terminal)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                retain_native_run_panic(original, payload, false);
                Poll::Ready(Err(()))
            }
        }
    })
    .await;
    if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(future))) {
        retain_native_run_panic(original, payload, true);
        // The real run already authored its terminal. A secondary destructor
        // failure is Source health, not a replacement business verdict.
    }
    result
}

fn retain_native_run_panic(
    original: &Weak<OriginalLogicalExit>,
    payload: Box<dyn std::any::Any + Send>,
    cleanup: bool,
) {
    let mut incoming = Some(payload);
    if let Some(source) = original.upgrade() {
        source.retain_run_panic_silent(&mut incoming, cleanup);
    }
    // The original loop never restarts after a run panic. An invariant breach
    // must preserve a rejected raw packet outside the Source lock as well.
    if let Some(rejected) = incoming {
        std::panic::panic_any(std::mem::ManuallyDrop::new(rejected));
    }
}

async fn await_with_shutdown<F>(
    future: F,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) -> F::Output
where
    F: Future,
{
    tokio::pin!(future);
    let mut shutdown_open = true;
    loop {
        if *shutdown.borrow() {
            let _ = requester.request(CancellationReason::ServerShutdown);
            return future.await;
        }
        tokio::select! {
            output = &mut future => return output,
            changed = shutdown.changed(), if shutdown_open => {
                shutdown_open = changed.is_ok();
                if !shutdown_open || *shutdown.borrow() {
                    let _ = requester.request(CancellationReason::ServerShutdown);
                }
            }
        }
    }
}

/// Outcome of waiting for an operation before its work owner transfers to a
/// logical-execution actor.
enum PreInstallWait<T> {
    Completed(T),
    Cancelled(CancellationReason),
}

/// Waits for a pre-install operation without letting a deadline strand the
/// only work owner behind an unbounded Native or admission wait.
///
/// This is deliberately limited to the interval before actor installation:
/// the same Native preparation body remains in original stable custody. Its
/// explicit original close/paid return is observed after the role frame clears. Once installed, cancellation remains actor-owned and ordinary
/// control delivery cannot be acknowledged as a substitute for terminal
/// facts. Shutdown retains its existing behavior and lets the in-flight
/// operation observe its shutdown cancellation before returning.
async fn await_before_actor_install<F>(
    future: F,
    cancellation: novarocks_workload_control::CancellationView,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) -> PreInstallWait<F::Output>
where
    F: Future,
{
    if let Some(reason) = cancellation.reason() {
        return PreInstallWait::Cancelled(reason);
    }
    tokio::pin!(future);
    let mut shutdown_open = true;
    loop {
        if *shutdown.borrow() {
            let _ = requester.request(CancellationReason::ServerShutdown);
            return PreInstallWait::Completed(future.await);
        }
        tokio::select! {
            biased;
            reason = cancellation.cancelled() => {
                let _ = requester.request(reason.clone());
                return PreInstallWait::Cancelled(
                    cancellation.reason().unwrap_or(reason),
                );
            }
            output = &mut future => return PreInstallWait::Completed(output),
            changed = shutdown.changed(), if shutdown_open => {
                shutdown_open = changed.is_ok();
                if !shutdown_open || *shutdown.borrow() {
                    let _ = requester.request(CancellationReason::ServerShutdown);
                }
            }
        }
    }
}

struct PendingWorkOwner(Option<WorkOwner>);

impl PendingWorkOwner {
    fn owner(&self) -> &WorkOwner {
        self.0
            .as_ref()
            .expect("pending start retains its work owner")
    }

    fn take(&mut self) -> WorkOwner {
        self.0
            .take()
            .expect("pending start transfers its work owner once")
    }

    /// Actor control can be settled before installation; the fixed original
    /// Native body/footer separately retains every preparation source until its
    /// actual close. This does not assert that all Native custody has exited.
    fn complete_uninstalled_failure(mut self) {
        self.take().complete_after_terminal_cancel_settled();
    }
}

impl Drop for PendingWorkOwner {
    fn drop(&mut self) {
        if let Some(owner) = self.0.take() {
            owner.complete_after_terminal_cancel_settled();
        }
    }
}

fn fail_uninstalled_start(
    owner: PendingWorkOwner,
    reply: oneshot::Sender<Result<ExecutionHandle, QueryExecutionError>>,
    error: QueryExecutionError,
) -> Result<(), QueryExecutionError> {
    owner.complete_uninstalled_failure();
    let _ = reply.send(Err(error));
    Ok(())
}

async fn retire_logical(
    registry: &super::LogicalExecutionRuntimeRegistryHandle,
    registration: super::LogicalExecutionRegistration,
) -> Result<(), QueryExecutionError> {
    if let Some(error) = registry.active_native_registration_health(&registration) {
        return Err(error);
    }
    registry
        .join_and_retire(registration)
        .await
        .map_err(registry_error)
}

fn first_supervision_error(
    first: Result<(), QueryExecutionError>,
    convergence: Result<(), QueryExecutionError>,
) -> Result<(), QueryExecutionError> {
    first.and(convergence)
}

fn contract_error(error: crate::api::NativeExecutionContractError) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        error.failure_fact(),
        error.safe_detail(),
        ExecutionFailureSite::Coordinator,
    ))
}

fn result_protocol_error(detail: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::ResultProtocol.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    ))
}

fn native_open_error(error: LogicalNativeOpenError) -> QueryExecutionError {
    match error {
        LogicalNativeOpenError::Contract(error) => contract_error(error),
        LogicalNativeOpenError::Runtime(failure) => failure.error().clone(),
    }
}

fn native_prepare_error(error: NativeAttemptPreparationError) -> QueryExecutionError {
    match error {
        NativeAttemptPreparationError::Contract(error) => contract_error(error),
        NativeAttemptPreparationError::Runtime(failure) => failure.error().clone(),
    }
}

/// Only a terminal disposition authored by the actor proves this command
/// lost to an already accepted conclusion. Other refusals remain failures.
fn actor_terminal_refusal(
    error: super::LogicalExecutionActorError,
) -> Result<(), QueryExecutionError> {
    match error {
        super::LogicalExecutionActorError::ExecutionConcluded(_) => Ok(()),
        error => Err(actor_error(error)),
    }
}

fn preparation_supervision_result(
    error: QueryExecutionError,
    actor_outcome: Result<super::LogicalConclusion, super::LogicalExecutionActorError>,
) -> Result<(), QueryExecutionError> {
    let accepted_control_conclusion = matches!(
        &actor_outcome,
        Err(super::LogicalExecutionActorError::ExecutionConcluded(_))
    ) && matches!(
        error.kind(),
        QueryExecutionErrorKind::Cancelled | QueryExecutionErrorKind::DeadlineExceeded
    );
    let actor_result = actor_outcome.map(|_| ()).or_else(actor_terminal_refusal);
    if accepted_control_conclusion {
        actor_result
    } else {
        first_supervision_error(Err(error), actor_result)
    }
}

fn actor_error(error: super::LogicalExecutionActorError) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        error.failure_fact(),
        error.safe_detail(),
        ExecutionFailureSite::Coordinator,
    ))
}

fn registry_error(error: LogicalExecutionRuntimeRegistryError) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        error.failure_fact(),
        error.safe_detail(),
        ExecutionFailureSite::Coordinator,
    ))
}

fn work_error(error: WorkError) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        error.failure_fact(),
        SafeDetail::truncating("query workload control refused the operation"),
        ExecutionFailureSite::Coordinator,
    ))
}

fn pre_install_cancellation_error(reason: CancellationReason) -> QueryExecutionError {
    cancellation_error(
        reason,
        "logical execution cancelled before actor installation",
    )
}

fn native_future_panic_input(message: &'static str) -> ExecutionFailureInput {
    ExecutionFailureInput::new(
        QueryFailureReason::NativeFuturePanicked.failure_fact(),
        SafeDetail::truncating(message),
        ExecutionFailureSite::Coordinator,
    )
}

pub(super) fn internal_consumer_panicked(message: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::InternalConsumerPanicked.failure_fact(),
        SafeDetail::truncating(message),
        ExecutionFailureSite::Coordinator,
    ))
}

fn native_future_panicked(message: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(native_future_panic_input(message))
}

#[cfg(test)]
mod tests {
    include!("supervisor_internal_tests.rs");
    include!("supervisor_background_tests.rs");
    include!("supervisor_role_bank_fixture.rs");
    include!("supervisor_original_exit_tests.rs");
    include!("supervisor_qualification_binding_tests.rs");
    include!("supervisor_client_computation_tests.rs");
    #[test]
    fn terminal_actor_disposition_does_not_hide_invalid_phase_or_service_invariants() {
        use super::super::{LogicalConclusion, LogicalExecutionActorError};
        assert!(
            actor_terminal_refusal(LogicalExecutionActorError::ExecutionConcluded(
                LogicalConclusion::Cancelled
            ))
            .is_ok()
        );
        for error in [
            LogicalExecutionActorError::WrongPhase,
            LogicalExecutionActorError::StaleAuthority,
            LogicalExecutionActorError::WrongExecution,
            LogicalExecutionActorError::InvariantViolation,
            LogicalExecutionActorError::MailboxClosed,
        ] {
            let failure = actor_terminal_refusal(error).unwrap_err();
            assert_eq!(
                failure.execution_failure().unwrap().fact(),
                error.failure_fact()
            );
            let cancellation = cancellation_error(
                CancellationReason::Requested,
                "the preparation observed workload cancellation",
            );
            assert!(preparation_supervision_result(cancellation, Err(error)).is_err());
        }
    }

    #[test]
    fn producer_failures_keep_their_facts_instead_of_claiming_query_cancellation_or_deadline() {
        use novarocks_execution_contract::failure::{FailureClass, FailureEvidence};
        let source = super::super::LogicalExecutionActorError::WrongPhase;
        let error = super::actor_error(source);
        let input = error.execution_failure().unwrap();
        assert_eq!(input.fact(), source.failure_fact());
        assert_eq!(input.detail(), &source.safe_detail());
        assert_eq!(input.fact().evidence(), FailureEvidence::StopObservation);
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            input.attempt_class(),
            novarocks_coordination_contract::AttemptFailureClass::ExecutionFailure
        );

        let source = WorkError::CapacityWaitTimeout;
        let error = super::work_error(source.clone());
        let input = error.execution_failure().unwrap();
        assert_eq!(input.fact(), source.failure_fact());
        assert_eq!(input.fact().class(), FailureClass::Resource);
        assert_eq!(input.fact().resource(), None);
        assert_eq!(error.kind(), QueryExecutionErrorKind::Rejected);

        let source = WorkError::Cancelled(CancellationReason::Requested);
        let error = super::work_error(source.clone());
        let input = error.execution_failure().unwrap();
        assert_eq!(input.fact(), source.failure_fact());
        assert_eq!(input.fact().evidence(), FailureEvidence::StopObservation);
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
    }

    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    use novarocks_execution_contract::{
        AdmissionEpochCapability, AdmissionTicketId, CodecOwnedContent, ConfidentialContent,
        ContentFingerprint, CredentialEpoch, CredentialLeaseId, CredentialUpdate,
        EstablishQueryContext, LeaseValidFor, OperationOutcome, QueryContextAdmissionTicketReceipt,
        QueryContextRef, TaskOperationId, TaskOutputFacts, TaskState, TaskStatus,
        TaskStatusVersion,
    };
    use novarocks_types::NativeCompatibilityId;
    use novarocks_types::identity::{BackendProcessId, FrontendProcessId};
    use novarocks_workload_control::{
        CancellationReason, ResourceConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

    use crate::api::{
        ActivatedNativeAttempt, ActiveNativeAttemptOwner, DormantNativeAttemptOwner,
        NativeActiveAttemptConvergenceFuture, NativeAttemptActivationFailure,
        NativeAttemptActivationFuture, NativeAttemptConvergence, NativeAttemptConvergenceFuture,
        NativeAttemptPreparationFailure, NativeAttemptPreparationFuture,
        NativeAttemptPreparationPort, NativeAttemptRunFuture,
    };
    use crate::coordination::{
        AdmissionIssueSettlement, EstablishIssueIdentity, EstablishIssueSubmit,
        EstablishTransportAdmission, EstablishTransportReservation, EstablishTransportSink,
        EstablishTransportSubmission,
    };
    use crate::coordination::{
        AttemptSchedule, PermanentlyBackpressuredAbortEffectPort,
        ReplacementWorkerAdmissionEvidence,
    };
    use crate::preparation::{
        ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
        FrozenExecutionDescription, OutputContract,
    };
    use novarocks_coordination_contract::AttemptFailureClass;

    use super::*;

    /// A test-owned injected failure is a legal execution failure, not a
    /// production protocol violation or a fictitious Task failure.
    #[derive(Clone, Copy, Debug)]
    enum FixtureFailureReason {
        InjectedExecution,
    }
    impl FailureProjection for FixtureFailureReason {
        fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
            use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason};
            match self {
                Self::InjectedExecution => FailureFact::new(
                    FailureClass::Unexpected,
                    FailureReason::new(
                        std::num::NonZeroU16::MAX,
                        std::num::NonZeroU16::new(1).unwrap(),
                    ),
                ),
            }
        }
    }
    fn fixture_execution_input(message: &'static str) -> ExecutionFailureInput {
        ExecutionFailureInput::new(
            FixtureFailureReason::InjectedExecution.failure_fact(),
            SafeDetail::truncating(message),
            ExecutionFailureSite::Coordinator,
        )
    }
    fn fixture_cancellation_input(
        reason: CancellationReason,
        message: &'static str,
    ) -> ExecutionFailureInput {
        let reason = match reason {
            CancellationReason::DeadlineExceeded
            | CancellationReason::FrontendDrainDeadlineExceeded => {
                QueryFailureReason::DeadlineExceeded
            }
            _ => QueryFailureReason::Cancelled,
        };
        ExecutionFailureInput::new(
            reason.failure_fact(),
            SafeDetail::truncating(message),
            ExecutionFailureSite::Coordinator,
        )
    }
    fn fixture_root_reply_failure(
        root: novarocks_execution_contract::TaskIdentity,
        error: WorkError,
    ) -> super::super::RootResultFetchFailure {
        tracing::debug!(error = %error, "fixture root retained reply violated its contract");
        super::super::RootResultFetchFailure::from_input(ExecutionFailureInput::new(
            QueryFailureReason::ResultProtocol.failure_fact(),
            SafeDetail::truncating("fixture root reply violated its retained window contract"),
            ExecutionFailureSite::RootRead { root },
        ))
    }

    /// Scheduling facts for a request whose plan reads no provider.
    ///
    /// The shape comes from the description's own plan, so a test schedules
    /// the fragments its statement actually has.
    fn no_scan_scheduling(
        request: &crate::api::NativeAttemptPreparationRequest,
    ) -> crate::api::ExecutionSchedulingFacts {
        crate::api::ExecutionSchedulingFacts::from_frozen_description(
            request.description(),
            &std::collections::BTreeMap::new(),
        )
        .expect("a plan with no provider read needs no enumerated work")
    }

    fn supervisor_config() -> LogicalExecutionSupervisorConfig {
        LogicalExecutionSupervisorConfig::new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            LogicalExecutionRowsConfig::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroU32::new(2).unwrap(),
                Duration::from_secs(30),
            ),
        )
    }

    #[derive(Debug)]
    struct NeverConverges;

    impl ActiveNativeAttemptOwner for NeverConverges {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { NativeAttemptTerminal::Completed })
        }

        fn converge<'a>(
            &'a mut self,
            request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            // No local work exists in this fixture; only remote observation is
            // stuck. The true owner, rather than the supervisor, ends it.
            Box::pin(async move {
                tokio::time::sleep_until(request.remote_observation_deadline().into()).await;
                Ok(NativeActiveConvergenceClosure::RemoteTrackingEnded {
                    exact_contexts: Box::new([]),
                })
            })
        }
    }

    #[tokio::test]
    async fn active_cleanup_deadline_ends_fe_tracking_without_fabricating_stop_evidence() {
        let mut _control = {
            let (_control, root) = governance().await;
            let cancellation = root.owner.scope().cancellation().unwrap();
            let requester = root.owner.cancellation_requester();
            let (_shutdown_sender, mut shutdown) = watch::channel(false);
            let mut owner = NeverConverges;

            let outcome = converge_active(
                &mut owner,
                cancellation,
                &mut shutdown,
                &requester,
                Duration::from_millis(1),
            )
            .await;

            assert!(matches!(
                outcome,
                ActiveAttemptConvergence::RemoteTrackingEnded { .. }
            ));
            root.owner.complete();
            root.business.release();

            _control
        };
        acknowledge_all_control(&_control);
        _control.finish().await;
    }

    #[tokio::test]
    async fn supervisor_shutdown_ends_active_remote_tracking_without_waiting_for_cleanup_window() {
        let mut _control = {
            let (_control, root) = governance().await;
            let cancellation = root.owner.scope().cancellation().unwrap();
            let requester = root.owner.cancellation_requester();
            let (shutdown_sender, mut shutdown) = watch::channel(false);
            let mut owner = NeverConverges;
            shutdown_sender.send_replace(true);

            let outcome = tokio::time::timeout(
                Duration::from_millis(10),
                converge_active(
                    &mut owner,
                    cancellation,
                    &mut shutdown,
                    &requester,
                    Duration::from_secs(30),
                ),
            )
            .await
            .expect("shutdown must not wait for the normal cleanup window");

            assert!(matches!(
                outcome,
                ActiveAttemptConvergence::RemoteTrackingEnded { .. }
            ));
            root.owner.complete();
            root.business.release();

            _control
        };
        acknowledge_all_control(&_control);
        _control.finish().await;
    }

    #[derive(Debug)]
    struct HeldLocalConvergence(Arc<tokio::sync::Notify>);
    impl ActiveNativeAttemptOwner for HeldLocalConvergence {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancel: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { NativeAttemptTerminal::Completed })
        }
        fn converge<'a>(
            &'a mut self,
            _request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.0.notified().await;
                Ok(NativeActiveConvergenceClosure::RemoteTrackingEnded {
                    exact_contexts: Box::new([]),
                })
            })
        }
    }
    #[tokio::test]
    async fn shutdown_remote_budget_never_closes_a_held_original_local_owner() {
        let mut _control = {
            let (_control, root) = governance().await;
            let cancellation = root.owner.scope().cancellation().unwrap();
            let requester = root.owner.cancellation_requester();
            let (shutdown_sender, mut shutdown) = watch::channel(false);
            shutdown_sender.send_replace(true);
            let release = Arc::new(tokio::sync::Notify::new());
            let mut owner = HeldLocalConvergence(Arc::clone(&release));
            let convergence = converge_active(
                &mut owner,
                cancellation,
                &mut shutdown,
                &requester,
                Duration::from_secs(30),
            );
            tokio::pin!(convergence);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut convergence)
                    .await
                    .is_err()
            );
            release.notify_one();
            assert!(matches!(
                convergence.await,
                ActiveAttemptConvergence::RemoteTrackingEnded { .. }
            ));
            root.owner.complete_after_terminal_cancel_settled();
            root.business.release();

            _control
        };
        acknowledge_all_control(&_control);
        _control.finish().await;
    }

    #[tokio::test]
    async fn accepted_start_returns_on_cancellation_before_supervisor_verdict() {
        let (control, root) = governance().await;
        let mut control = {
            let (mut bank, shutdown, mut registry, client) =
                unpolled_test_driver(&control, Arc::new(PanickingOpenNativePort));
            let requester = root.owner.cancellation_requester();
            let start = client.start(completion_request(UnreachablePreparationPort), root.owner);
            requester
                .request(CancellationReason::DeadlineExceeded)
                .unwrap();
            let error = match tokio::time::timeout(Duration::from_secs(1), start)
                .await
                .expect("accepted start returns after cancellation")
            {
                Ok(_) => panic!("cancellation wins before a start verdict"),
                Err(error) => error,
            };
            assert_eq!(
                error.kind(),
                QueryExecutionErrorKind::DeadlineExceeded,
                "{}",
                error.message()
            );
            // The response consumer exited, but the SAME accepted input still
            // owns its Work until the real factory is polled and dropped.
            assert_ne!(control.snapshot().root_responsibilities, 0);
            shutdown.send_replace(true);
            bank.stop_ingress();
            let completion = bank.progress_turn(NonZeroUsize::new(7).unwrap()).unwrap();
            let (_source, completion) = completion.into_parts();
            assert!(matches!(&completion, PollCompletion::Ready(output) if output.value().is_ok()));
            drop(completion);
            drop(bank);
            registry
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            root.business.release();
            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn uninstalled_failure_settles_its_known_cancel_control() {
        let mut control = {
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            root.owner.cancel(CancellationReason::Requested);

            PendingWorkOwner(Some(root.owner)).complete_uninstalled_failure();
            root.business.release();

            scope.wait_released().await;
            assert_eq!(control.snapshot().root_responsibilities, 0);
            assert!(
                control.next_control().is_none(),
                "a failure before actor installation has no effect owner left to acknowledge"
            );

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    fn fixture_version() -> [u8; 16] {
        static NEXT_VERSION: AtomicU64 = AtomicU64::new(1);
        let sequence = NEXT_VERSION.fetch_add(1, Ordering::Relaxed);
        let mut version = [72; 16];
        version[8..].copy_from_slice(&sequence.to_be_bytes());
        version
    }

    fn completion_request(attempts: impl NativeAttemptPreparationPort) -> QueryExecutionRequest {
        let plan = crate::completed_plan_fixture::completed_noop_plan(fixture_version());
        let description = FrozenExecutionDescription::for_completed_plan(
            crate::api::QueryExecutionKind::Maintenance,
            plan.candidate().clone(),
            Vec::new(),
            OutputContract::CompletionOnly,
            novarocks_coordination_contract::ExecutionEffect::None,
            RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap();
        QueryExecutionRequest::bind_native(
            description,
            PermanentlyBackpressuredAbortEffectPort::shared(),
            None,
            attempts,
        )
    }

    fn rows_request(
        _control: &WorkloadControl,
        scope: &novarocks_workload_control::WorkScope,
        recovery: RecoveryMode,
        replacements: Option<Arc<dyn super::super::ReplacementQualificationEffectPort>>,
        mut attempts: impl TestRowsPreparationPort,
    ) -> QueryExecutionRequest {
        let plan = crate::completed_plan_fixture::completed_values_plan_blocking(fixture_version());
        use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
        let render = novarocks_sql::compiler::client_render_schema(
            plan.candidate().plan(),
            novarocks_sql::compiler::RenderFamilyRequest::MysqlText {
                timezone_offset_seconds: 0,
            },
        )
        .unwrap();
        let candidate = crate::preparation::CompletedPhysicalPlanCandidate::for_program(
            plan.candidate()
                .plan()
                .as_ref()
                .clone()
                .with_root_output(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::ClientRows(render),
                ))
                .unwrap(),
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let window = scope
            .result_capacity()
            .unwrap()
            .try_acquire(scope, novarocks_workload_control::ResultWindowClass::Client)
            .unwrap();
        attempts.bind_window(window.retain_alias());
        let result_capacity = crate::admitted_query_context::QueryResultCapacityBinding::try_new(
            scope,
            window.retain_alias(),
        )
        .unwrap();
        let output = OutputContract::from_completed_plan(
            crate::api::QueryExecutionKind::Read,
            candidate.plan(),
        )
        .unwrap();
        let description = FrozenExecutionDescription::for_completed_plan(
            crate::api::QueryExecutionKind::Read,
            candidate,
            Vec::new(),
            output,
            novarocks_coordination_contract::ExecutionEffect::None,
            recovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap();
        QueryExecutionRequest::bind_native(
            description,
            PermanentlyBackpressuredAbortEffectPort::shared(),
            replacements,
            attempts,
        )
        .with_result_capacity(result_capacity)
        .unwrap()
    }

    #[derive(Clone)]
    struct TestWindow(novarocks_workload_control::ResultWindowAlias);
    impl std::fmt::Debug for TestWindow {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestWindow")
                .field("scope", &self.0.scope_id())
                .finish()
        }
    }

    trait TestRowsPreparationPort: NativeAttemptPreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias);
    }

    struct TestRootReadPort {
        sent: AtomicBool,
        payload: Option<&'static [u8]>,
    }
    impl crate::api::BoundedRootReadPort for TestRootReadPort {
        fn read(
            &self,
            request: novarocks_execution_contract::root_result::RootResultRead,
            guard: novarocks_workload_control::ResultWindowAlias,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            crate::api::RetainedRootReply,
                            super::super::RootResultFetchFailure,
                        >,
                    > + Send,
            >,
        > {
            use novarocks_execution_contract::root_result::{
                RootReadOutcome, RootResultData, RootResultReply,
            };
            let payload = if !self.sent.swap(true, Ordering::SeqCst) {
                self.payload
            } else {
                None
            };
            Box::pin(async move {
                let Some(payload) = payload else {
                    return std::future::pending().await;
                };
                let reply = RootResultReply {
                    root_task: request.root_task(),
                    profile: request.profile(),
                    kind: request.kind(),
                    accepted_consumed: request.consumed(),
                    outcome: RootReadOutcome::Data(
                        RootResultData::try_new(
                            request.kind(),
                            std::num::NonZeroU64::MIN,
                            bytes::Bytes::from_static(payload),
                            None,
                        )
                        .unwrap(),
                    ),
                };
                crate::api::RetainedRootReply::try_new(
                    novarocks_execution_contract::root_result::RootReadResponse::Reply(reply),
                    guard,
                    64 * 1024,
                )
                .map_err(|error| fixture_root_reply_failure(request.root_task(), error))
            })
        }
        fn seal(
            &self,
            _: novarocks_execution_contract::root_lifetime::RootReadSealed,
        ) -> Result<(), QueryExecutionError> {
            Ok(())
        }
    }

    fn test_root_binding(
        root: novarocks_execution_contract::TaskIdentity,
        window: novarocks_workload_control::ResultWindowAlias,
        payload: Option<&'static [u8]>,
    ) -> super::super::RootRelayBinding {
        use novarocks_result_contract::{
            ClientRowProfile, RootOutputKind, RootProfileId, RootProfileV1,
        };
        super::super::RootRelayBinding {
            port: Arc::new(TestRootReadPort {
                sent: AtomicBool::new(false),
                payload,
            }),
            frontier: super::super::RootRelayFrontier::new(
                root,
                RootProfileId::V1,
                RootOutputKind::ClientRows,
                Some(
                    ClientRowProfile::try_new(
                        RootProfileV1::SEGMENT_BYTES,
                        RootProfileV1::ROW_PAYLOAD_BYTES,
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
            window,
            max_wait: Duration::from_millis(10),
        }
    }

    fn running_status(root: novarocks_execution_contract::TaskIdentity) -> TaskStatus {
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(1).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::new(false),
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct UnreachablePreparationPort;

    impl NativeAttemptPreparationPort for UnreachablePreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            _request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            Box::pin(async { panic!("unreachable Native attempt preparation") })
        }
    }

    #[derive(Debug)]
    struct DropTrackedPreparationPort {
        drops: Arc<AtomicU64>,
    }

    impl Drop for DropTrackedPreparationPort {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl NativeAttemptPreparationPort for DropTrackedPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            _request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            Box::pin(async { panic!("rejected Native seed must never prepare an attempt") })
        }
    }

    fn drop_tracked_request(drops: &Arc<AtomicU64>) -> QueryExecutionRequest {
        completion_request(DropTrackedPreparationPort {
            drops: Arc::clone(drops),
        })
    }

    struct CaptureAdmittedCapacityPort {
        captured:
            Option<oneshot::Sender<crate::admitted_query_context::QueryResultCapacityBinding>>,
    }
    impl fmt::Debug for CaptureAdmittedCapacityPort {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("CaptureAdmittedCapacityPort")
        }
    }
    impl NativeAttemptPreparationPort for CaptureAdmittedCapacityPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            self.captured
                .take()
                .unwrap()
                .send(request.result_capacity().unwrap().clone())
                .unwrap_or_else(|_| panic!("original admitted binding receiver exited"));
            Box::pin(async {
                Err(
                    NativeAttemptPreparationFailure::from_input(fixture_execution_input(
                        "observed original preparation request",
                    ))
                    .into(),
                )
            })
        }
    }

    #[tokio::test]
    async fn admitted_capacity_reaches_original_request_and_survives_failed_start() {
        let mut control = {
            use crate::admitted_query_context::QueryResultCapacityBinding;
            use std::future::Future;
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let capacity = scope.result_capacity().unwrap();
            let window = capacity
                .try_acquire(
                    &scope,
                    novarocks_workload_control::ResultWindowClass::Internal,
                )
                .unwrap();
            let original =
                QueryResultCapacityBinding::try_new(&scope, window.retain_alias()).unwrap();
            let activity = original.begin_result_activity().unwrap();
            let (captured, received) = oneshot::channel();
            let request = completion_request(CaptureAdmittedCapacityPort {
                captured: Some(captured),
            })
            .with_result_capacity(original.clone())
            .unwrap();
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x75),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );
            let result = client.start(request, root.owner).await;
            assert!(result.is_err());
            let issued = received.await.unwrap();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            let observed = &issued;
            assert!(
                observed
                    .window_alias()
                    .shares_capacity_with(&original.window_alias())
            );
            assert!(observed.window_alias().is_for_scope(&scope));
            // An activity started through the original binding remains visible
            // through the actual request. Reconstructing from an alias would fail.
            let mut exited = Box::pin(observed.result_activities_exited());
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(exited.as_mut().poll(&mut context).is_pending());
            drop(activity);
            exited.await;
            drop((original, window));
            assert_eq!(capacity.snapshot().held_positions[2], 1);
            drop(issued);
            assert_eq!(capacity.snapshot().held_positions[2], 0);
            root.business.release();
            scope.wait_released().await;
            assert_eq!(control.snapshot().root_responsibilities, 0);

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn foreign_runtime_capacity_is_rejected_before_native_open() {
        let (mut control, mut foreign_control) = {
            use crate::admitted_query_context::QueryResultCapacityBinding;
            let (control, root) = governance().await;
            let (foreign_control, foreign_root) = governance().await;
            let scope = root.owner.scope();
            let foreign_scope = foreign_root.owner.scope();
            assert_eq!(
                scope.id(),
                foreign_scope.id(),
                "runtime identity must also be checked"
            );
            let capacity = foreign_scope.result_capacity().unwrap();
            let window = capacity
                .try_acquire(
                    &foreign_scope,
                    novarocks_workload_control::ResultWindowClass::Internal,
                )
                .unwrap();
            let original =
                QueryResultCapacityBinding::try_new(&foreign_scope, window.retain_alias()).unwrap();
            let request = completion_request(UnreachablePreparationPort)
                .with_result_capacity(original.clone())
                .unwrap();
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(PanickingOpenNativePort),
                QueryProcessNamespace::new(0x76),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );
            let Err(error) = client.start(request, root.owner).await else {
                panic!("a foreign runtime binding cannot open Native work");
            };
            assert_eq!(error.kind(), QueryExecutionErrorKind::InvalidRequest);
            assert_eq!(capacity.snapshot().held_positions[2], 1);
            drop((original, window));
            assert_eq!(capacity.snapshot().held_positions[2], 0);
            root.business.release();
            foreign_root.owner.complete();
            foreign_root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            scope.wait_released().await;
            foreign_scope.wait_released().await;
            assert_eq!(control.snapshot().root_responsibilities, 0);
            assert_eq!(foreign_control.snapshot().root_responsibilities, 0);

            (control, foreign_control)
        };
        acknowledge_all_control(&control);
        control.finish().await;
        acknowledge_all_control(&foreign_control);
        foreign_control.finish().await;
    }

    #[tokio::test]
    async fn cancelled_rejected_start_settles_uninstalled_control() {
        let mut control = {
            let (control, root) = governance().await;
            let cancellation = root.owner.scope().cancellation().unwrap();
            root.owner.cancel(CancellationReason::DeadlineExceeded);
            let error = match rejected_start(root.owner, "supervisor closed").await {
                Err(error) => error,
                Ok(_) => panic!("closed supervisor cannot accept the start"),
            };
            assert_eq!(error.kind(), QueryExecutionErrorKind::Rejected);
            assert!(
                error.execution_failure().is_none(),
                "local admission is not a Task failure"
            );
            assert_eq!(
                cancellation.reason(),
                Some(CancellationReason::DeadlineExceeded)
            );
            root.business.release();
            assert_eq!(control.snapshot().root_responsibilities, 0);
            assert!(control.next_control().is_none());

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    fn acknowledge_all_control(control: &WorkloadControl) {
        while let Some(permit) = control.next_control() {
            permit.acknowledge();
        }
    }

    #[derive(Debug)]
    struct FailingActivationNativePort {
        opened_execution_low: Arc<AtomicU64>,
    }

    impl LogicalExecutionNativePort for FailingActivationNativePort {
        fn bind_in(
            &self,
            source: &mut Option<LogicalNativeOpenRequest>,
            target: &mut Option<LogicalNativeSession>,
        ) -> Result<(), LogicalNativeOpenError> {
            let execution = source.as_ref().unwrap().initial_execution();
            self.opened_execution_low
                .store(execution.query_id().low() as u64, Ordering::SeqCst);
            LogicalNativeOpenRequest::bind_in(source, target).map_err(Into::into)
        }
    }

    #[derive(Debug)]
    struct FailingActivationPreparationPort {
        backend: BackendProcessId,
        activated: Arc<AtomicBool>,
        residual_converged: Arc<AtomicBool>,
    }

    impl NativeAttemptPreparationPort for FailingActivationPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let backend = self.backend;
            let dormant = FailingDormantOwner {
                backend,
                activated: Arc::clone(&self.activated),
                residual_converged: Arc::clone(&self.residual_converged),
            };
            request.stage_fixture_owner(dormant);
            Box::pin(async move {
                let scheduling = no_scan_scheduling(request);
                request.bind_staged(scheduling).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct FailingDormantOwner {
        backend: BackendProcessId,
        activated: Arc<AtomicBool>,
        residual_converged: Arc<AtomicBool>,
    }

    impl DormantNativeAttemptOwner for FailingDormantOwner {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            self.activated.store(true, Ordering::SeqCst);
            Box::pin(async move {
                Err(NativeAttemptActivationFailure::new(
                    NativeAttemptPreparationFailure::from_input(fixture_execution_input(
                        "injected Native activation failure",
                    )),
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.residual_converged.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    type FailingNativeFixture = (
        Arc<dyn LogicalExecutionNativePort>,
        FailingActivationPreparationPort,
        Arc<AtomicU64>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
    );

    fn failing_native() -> FailingNativeFixture {
        let opened = Arc::new(AtomicU64::new(0));
        let activated = Arc::new(AtomicBool::new(false));
        let residual = Arc::new(AtomicBool::new(false));
        (
            Arc::new(FailingActivationNativePort {
                opened_execution_low: Arc::clone(&opened),
            }),
            FailingActivationPreparationPort {
                backend: BackendProcessId::new_v7(),
                activated: Arc::clone(&activated),
                residual_converged: Arc::clone(&residual),
            },
            opened,
            activated,
            residual,
        )
    }

    /// An owner abandoned by an unwind must not replace the panic that
    /// abandoned it. Without the guard in its destructor this test does not
    /// fail: it aborts the whole binary and takes every other test's result
    /// with it.
    #[tokio::test]
    async fn an_owner_abandoned_by_an_unwind_reports_the_original_panic() {
        let mut control = {
            let control = governance_empty();
            let (mut supervisor, _client) = test_supervisor(
                &control,
                Handle::current(),
                failing_native().0,
                QueryProcessNamespace::new(0x4f),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let mut original_join = supervisor.join.take().unwrap();
            let payload = catch_unwind(AssertUnwindSafe(move || {
                let _abandoned = supervisor;
                panic!("the assertion that actually failed");
            }))
            .expect_err("the closure panics");

            assert_eq!(
                *payload
                    .downcast_ref::<&str>()
                    .expect("the original panic payload survives the unwind"),
                "the assertion that actually failed"
            );

            let joined = tokio::time::timeout(Duration::from_secs(1), &mut original_join)
                .await
                .expect("the same abandoned root must actually join")
                .unwrap();
            assert!(matches!(joined, OriginalRootOutcome::Completed(Ok(()))));

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[test]
    fn native_contract_wrappers_keep_original_fact_and_safe_detail() {
        use crate::api::NativeExecutionContractError;
        for source in [
            NativeExecutionContractError::ForeignAttemptTicket,
            NativeExecutionContractError::AttemptPreparationRetired,
        ] {
            for error in [
                native_open_error(source.into()),
                native_prepare_error(source.into()),
            ] {
                let input = error
                    .execution_failure()
                    .expect("native contract author must retain its fact");
                assert_eq!(input.fact(), source.failure_fact());
                assert_eq!(input.detail(), &source.safe_detail());
                assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
                assert_eq!(
                    input.topology_requirement(),
                    crate::api::NativeAttemptTopologyRequirement::LiveSnapshot
                );
                assert_eq!(
                    input.user_error().phase(),
                    novarocks_user_error::ErrorPhase::Execution
                );
                assert_eq!(input.user_error().location(), None);
            }
        }
    }

    #[tokio::test]
    async fn activation_failure_retains_residual_and_retires_exact_registry_entry() {
        let mut control = {
            let (native, attempts, opened, activated, residual) = failing_native();
            let (control, root) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                native,
                QueryProcessNamespace::new(0x45),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let scope = root.owner.scope();

            let error = match client.start(completion_request(attempts), root.owner).await {
                Ok(_) => panic!("activation failure must not return an execution handle"),
                Err(error) => error,
            };
            assert_eq!(error.message(), "injected Native activation failure");
            assert_eq!(
                error.execution_failure(),
                Some(&fixture_execution_input(
                    "injected Native activation failure"
                ))
            );
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            scope.wait_released().await;
            assert_ne!(opened.load(Ordering::SeqCst), 0);
            assert!(activated.load(Ordering::SeqCst));
            assert!(residual.load(Ordering::SeqCst));
            assert_eq!(control.snapshot().root_responsibilities, 0);

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn dropping_an_accepted_reply_does_not_drop_its_work_owner() {
        let mut control = {
            let (native, attempts, _opened, activated, residual) = failing_native();
            let (control, root) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                native,
                QueryProcessNamespace::new(0x46),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let scope = root.owner.scope();
            let future = client.start(completion_request(attempts), root.owner);
            drop(future);
            root.business.release();

            tokio::time::timeout(Duration::from_secs(1), async {
                while !activated.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("accepted request continues after its reply receiver is dropped");

            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            scope.wait_released().await;
            assert!(activated.load(Ordering::SeqCst));
            assert!(residual.load(Ordering::SeqCst));

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum AttemptPanicPhase {
        Activate,
        Run,
        Converge,
    }

    #[derive(Debug)]
    struct BindingNativePort;

    impl LogicalExecutionNativePort for BindingNativePort {
        fn bind_in(
            &self,
            source: &mut Option<LogicalNativeOpenRequest>,
            target: &mut Option<LogicalNativeSession>,
        ) -> Result<(), LogicalNativeOpenError> {
            LogicalNativeOpenRequest::bind_in(source, target).map_err(Into::into)
        }
    }

    #[derive(Debug, Default)]
    struct ClosedSuccessSealPort;

    impl super::super::AcceptedRootControlPort for ClosedSuccessSealPort {
        fn enqueue_root_control(
            &self,
            request: super::super::AcceptedRootControlRequest,
        ) -> Result<(), super::super::AcceptedRootControlRequest> {
            Err(request)
        }
    }

    #[derive(Debug)]
    struct RowsAttemptPreparationPort {
        relay_window: Option<TestWindow>,
        backend: BackendProcessId,
        converged: Arc<AtomicBool>,
    }

    impl TestRowsPreparationPort for RowsAttemptPreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            self.relay_window = Some(TestWindow(window));
        }
    }

    impl NativeAttemptPreparationPort for RowsAttemptPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let owner = RowsDormantOwner {
                relay_window: self
                    .relay_window
                    .as_ref()
                    .expect("fixture window was admitted")
                    .clone(),
                backend: self.backend,
                converged: Arc::clone(&self.converged),
            };
            request.stage_fixture_owner(owner);
            Box::pin(async move {
                let scheduling = no_scan_scheduling(request);
                request.bind_staged(scheduling).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct RowsDormantOwner {
        relay_window: TestWindow,
        backend: BackendProcessId,
        converged: Arc<AtomicBool>,
    }

    impl DormantNativeAttemptOwner for RowsDormantOwner {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let root = schedule.root();
            let converged = Arc::clone(&self.converged);
            let window = self.relay_window.clone();
            Box::pin(async move {
                let (status_sender, statuses) =
                    super::super::accepted_root_status_projection_with_control_port(
                        root,
                        Arc::new(ClosedSuccessSealPort),
                    );
                status_sender
                    .publish_attempt_observation(
                        running_status(root),
                        super::super::AcceptedAttemptFailure::None,
                    )
                    .unwrap();
                let binding = test_root_binding(root, window.0, None);
                Ok(ActivatedNativeAttempt::relayed_rows(
                    RowsActiveOwner {
                        converged,
                        _status_sender: status_sender,
                    },
                    binding,
                    statuses,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.converged.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    struct RowsActiveOwner {
        converged: Arc<AtomicBool>,
        _status_sender: super::super::AcceptedRootStatusSender,
    }

    impl fmt::Debug for RowsActiveOwner {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("RowsActiveOwner")
        }
    }

    impl ActiveNativeAttemptOwner for RowsActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                let reason = cancellation.cancelled().await;
                NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                    fixture_cancellation_input(reason, "Rows test attempt was cancelled"),
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.converged.store(true, Ordering::SeqCst);
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    #[derive(Default)]
    struct PreparationExitSignals {
        factory_exited: AtomicBool,
        convergence_entered: AtomicBool,
        convergence_completed: AtomicBool,
        reply_captured: AtomicBool,
        release_convergence: tokio::sync::Notify,
        trigger_failure: tokio::sync::Notify,
        reply: std::sync::Mutex<Option<crate::api::RetainedRootReply>>,
        transport: std::sync::Mutex<Option<novarocks_workload_control::ResultWindowAlias>>,
    }
    impl fmt::Debug for PreparationExitSignals {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("PreparationExitSignals")
        }
    }

    #[derive(Debug)]
    struct PreparationExitState {
        window: Option<TestWindow>,
        signals: Arc<PreparationExitSignals>,
    }
    impl Drop for PreparationExitState {
        fn drop(&mut self) {
            // Witness actual state/window destruction, not just wrapper destruction.
            drop(self.window.take());
            self.signals.factory_exited.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct PreparationExitPort {
        state: Arc<PreparationExitState>,
        backend: BackendProcessId,
    }
    impl TestRowsPreparationPort for PreparationExitPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            Arc::get_mut(&mut self.state)
                .expect("window admission precedes the first preparation future")
                .window = Some(TestWindow(window));
        }
    }
    impl NativeAttemptPreparationPort for PreparationExitPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            // The original Session retains the factory; this fixture stages
            // its actual dormant owner before returning the borrowed future.
            let state = Arc::clone(&self.state);
            let backend = self.backend;
            let dormant = PreparationExitDormant {
                inner: RowsDormantOwner {
                    relay_window: state.window.as_ref().expect("admitted window").clone(),
                    backend,
                    converged: Arc::new(AtomicBool::new(false)),
                },
                signals: Arc::clone(&state.signals),
            };
            request.stage_fixture_owner(dormant);
            drop(state);
            Box::pin(async move {
                let scheduling = no_scan_scheduling(request);

                request.bind_staged(scheduling).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct PreparationExitDormant {
        inner: RowsDormantOwner,
        signals: Arc<PreparationExitSignals>,
    }
    impl DormantNativeAttemptOwner for PreparationExitDormant {
        fn poll_bind_qualification(
            &mut self,
            schedule: &crate::coordination::AttemptSchedule,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            self.inner.poll_bind_qualification(schedule, cx)
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            self.inner.eligible_backends()
        }
        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let signals = Arc::clone(&self.signals);
            let activation = self.inner.activate(schedule, admissions, cancellation);
            Box::pin(async move {
                let activated = activation.await?;
                let (inner, runtime) = activated.into_parts();
                let mut runtime = runtime.expect("original Rows fixture carries its runtime");
                runtime.binding.port = Arc::new(PreparationExitReadPort {
                    signals: Arc::clone(&signals),
                });
                Ok(ActivatedNativeAttempt::relayed_rows(
                    PreparationExitActive { inner, signals },
                    runtime.binding,
                    runtime.statuses,
                ))
            })
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            self.inner.converge(cancellation)
        }
    }

    #[derive(Debug)]
    struct PreparationExitActive {
        inner: Box<dyn ActiveNativeAttemptOwner>,
        signals: Arc<PreparationExitSignals>,
    }
    impl ActiveNativeAttemptOwner for PreparationExitActive {
        fn run<'a>(
            &'a mut self,
            drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            let signals = Arc::clone(&self.signals);
            let run = self.inner.run(drive, cancellation);
            Box::pin(async move {
                tokio::select! {
                    terminal = run => terminal,
                    _ = signals.trigger_failure.notified() => {
                        NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(fixture_execution_input("held-convergence fixture execution failure")))
                    }
                }
            })
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            let signals = Arc::clone(&self.signals);
            Box::pin(async move {
                signals.convergence_entered.store(true, Ordering::SeqCst);
                signals.release_convergence.notified().await;
                let result = self.inner.converge(cancellation).await;
                signals.convergence_completed.store(true, Ordering::SeqCst);
                result
            })
        }
    }

    #[derive(Debug)]
    struct PreparationExitReadPort {
        signals: Arc<PreparationExitSignals>,
    }
    impl crate::api::BoundedRootReadPort for PreparationExitReadPort {
        fn read(
            &self,
            request: novarocks_execution_contract::root_result::RootResultRead,
            guard: novarocks_workload_control::ResultWindowAlias,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            crate::api::RetainedRootReply,
                            super::super::RootResultFetchFailure,
                        >,
                    > + Send,
            >,
        > {
            let signals = Arc::clone(&self.signals);
            Box::pin(async move {
                use novarocks_execution_contract::root_result::{
                    RootReadOutcome, RootResultData, RootResultReply,
                };
                // This reply is deliberately held by an independent decode owner.
                // It is never delivered as a synthetic valid MySQL row or ACKed.
                let reply = RootResultReply {
                    root_task: request.root_task(),
                    profile: request.profile(),
                    kind: request.kind(),
                    accepted_consumed: request.consumed(),
                    outcome: RootReadOutcome::Data(
                        RootResultData::try_new(
                            request.kind(),
                            std::num::NonZeroU64::MIN,
                            bytes::Bytes::from_static(b"\x00"),
                            None,
                        )
                        .expect("one nonempty byte is a bounded root data body"),
                    ),
                };
                let retained = crate::api::RetainedRootReply::try_new(
                    novarocks_execution_contract::root_result::RootReadResponse::Reply(reply),
                    guard,
                    64 * 1024,
                )
                .map_err(|error| fixture_root_reply_failure(request.root_task(), error))?;
                let transport = retained.retain_physical_guard();
                *signals.transport.lock().expect("fixture transport lock") = Some(transport);
                *signals.reply.lock().expect("fixture reply lock") = Some(retained);
                signals.reply_captured.store(true, Ordering::SeqCst);
                std::future::pending().await
            })
        }
        fn seal(
            &self,
            _: novarocks_execution_contract::root_lifetime::RootReadSealed,
        ) -> Result<(), QueryExecutionError> {
            Ok(())
        }
    }

    async fn wait_preparation_exit_fact(flag: &AtomicBool) -> Result<(), &'static str> {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !flag.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "fixture fact was not observed within its bound")
    }

    #[derive(Debug)]
    struct PreparationExitFacts {
        factory_exited: bool,
        convergence_still_held: bool,
        held_with_reply_and_transport: usize,
        held_after_reply_exit: usize,
        held_after_last_transport_exit: usize,
    }

    async fn terminal_preparation_exit_fixture(fail: bool) {
        let mut control = {
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let signals = Arc::new(PreparationExitSignals::default());
            let attempts = PreparationExitPort {
                state: Arc::new(PreparationExitState {
                    window: None,
                    signals: Arc::clone(&signals),
                }),
                backend: BackendProcessId::new_v7(),
            };
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x74),
                FrontendProcessId::new_v7(),
                supervisor_config().with_remote_cleanup_timeout(Duration::from_secs(10)),
            );
            let request = rows_request(&control, &scope, RecoveryMode::NoRecovery, None, attempts);
            let capacity = scope
                .result_capacity()
                .expect("original scope result capacity");
            let original_client_positions = capacity.snapshot().held_positions[0];
            // Capture every negative observation without asserting while the actual
            // convergence owner is blocked. The outer cleanup always releases it.
            let observations: Result<PreparationExitFacts, &'static str> = async {
                let mut handle =
                    tokio::time::timeout(Duration::from_secs(1), client.start(request, root.owner))
                        .await
                        .map_err(|_| "Rows handle handoff timed out")?
                        .map_err(|_| "Rows handle handoff failed")?;
                let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output()
                else {
                    return Err("original request did not hand off Rows");
                };
                wait_preparation_exit_fact(&signals.reply_captured).await?;
                if fail {
                    signals.trigger_failure.notify_one();
                } else {
                    handle
                        .request_cancel()
                        .map_err(|_| "original cancel request failed")?;
                }
                let terminal = tokio::time::timeout(Duration::from_secs(1), stream.next())
                    .await
                    .map_err(|_| "Rows terminal result timed out")?;
                if terminal.is_ok() {
                    return Err("terminal Rows result did not preserve a typed error");
                }
                drop(stream);
                drop(handle);
                wait_preparation_exit_fact(&signals.convergence_entered).await?;
                let factory_exited = signals.factory_exited.load(Ordering::SeqCst);
                let held_with_reply_and_transport = capacity.snapshot().held_positions[0];
                let reply = signals
                    .reply
                    .lock()
                    .map_err(|_| "fixture reply lock poisoned")?
                    .take();
                drop(reply);
                let held_after_reply_exit = capacity.snapshot().held_positions[0];
                let transport = signals
                    .transport
                    .lock()
                    .map_err(|_| "fixture transport lock poisoned")?
                    .take();
                drop(transport);
                let held_after_last_transport_exit = capacity.snapshot().held_positions[0];
                let convergence_still_held = !signals.convergence_completed.load(Ordering::SeqCst);
                Ok(PreparationExitFacts {
                    factory_exited,
                    convergence_still_held,
                    held_with_reply_and_transport,
                    held_after_reply_exit,
                    held_after_last_transport_exit,
                })
            }
            .await;

            // Preserve cleanup even when an observation failed. Poisoned fixture locks
            // retain their actual slots until explicitly taken; do not abandon owners.
            drop(
                signals
                    .reply
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take(),
            );
            drop(
                signals
                    .transport
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take(),
            );
            signals.release_convergence.notify_one();
            // Process shutdown may legitimately end optional remote tracking.
            // Observe this original fixture's convergence before requesting it.
            let convergence_exit = wait_preparation_exit_fact(&signals.convergence_completed).await;
            root.business.release();
            let shutdown = supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(2))
                .await;
            // A timed-out handoff may have continued in the original supervisor.
            // Once shutdown joins it, drain slots again before checking scope release.
            drop(
                signals
                    .reply
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take(),
            );
            drop(
                signals
                    .transport
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take(),
            );
            acknowledge_all_control(&control);
            let settled = tokio::time::timeout(Duration::from_secs(2), scope.wait_released()).await;

            // All assertions happen after the original owner has been released/joined.
            assert!(
                shutdown.is_ok(),
                "original supervisor did not shut down: {shutdown:?}"
            );
            assert!(settled.is_ok(), "original scope did not settle");
            assert!(
                convergence_exit.is_ok(),
                "original convergence did not exit"
            );
            let facts = observations.expect("all bounded lifetime observations must complete");
            assert_eq!(original_client_positions, 1);
            assert!(
                facts.factory_exited,
                "terminal supervisor retained its preparation factory: {facts:?}"
            );
            assert!(
                facts.convergence_still_held,
                "fixture released remote convergence too early"
            );
            assert_eq!(facts.held_with_reply_and_transport, 1);
            assert_eq!(
                facts.held_after_reply_exit, 1,
                "actual transport alias must keep the window"
            );
            assert_eq!(
                facts.held_after_last_transport_exit, 0,
                "last actual alias must return capacity before remote convergence"
            );
            assert!(signals.convergence_completed.load(Ordering::SeqCst));

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_cancel_retires_preparation_before_held_convergence_but_preserves_reply_aliases()
     {
        terminal_preparation_exit_fixture(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_failure_retires_preparation_before_held_convergence_but_preserves_reply_aliases()
     {
        terminal_preparation_exit_fixture(true).await;
    }

    #[derive(Clone, Copy, Debug)]
    enum InitialTerminalPhase {
        ActivateFailure,
        ActivatePanic,
        CompletionSuccess,
        CompletionFailure,
        CompletionFailureDropPanic,
        CompletionPanic,
    }

    #[derive(Debug)]
    struct InitialTerminalPreparationPort {
        state: Arc<PreparationExitState>,
        backend: BackendProcessId,
        frontend: FrontendProcessId,
        phase: InitialTerminalPhase,
    }
    impl NativeAttemptPreparationPort for InitialTerminalPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let state = Arc::clone(&self.state);
            let backend = self.backend;
            let frontend = self.frontend;
            let phase = self.phase;
            let dormant = InitialTerminalDormant {
                inner: SuccessfulCompletionDormantOwner {
                    backend,
                    expected_frontend: frontend,
                },
                // The actual attempt guard is independent of the factory alias.
                guard: state
                    .window
                    .as_ref()
                    .expect("original admitted window")
                    .clone(),
                signals: Arc::clone(&state.signals),
                phase,
            };
            request.stage_fixture_owner(dormant);
            drop(state);
            Box::pin(async move {
                let scheduling = no_scan_scheduling(request);

                request.bind_staged(scheduling).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct InitialTerminalDormant {
        inner: SuccessfulCompletionDormantOwner,
        guard: TestWindow,
        signals: Arc<PreparationExitSignals>,
        phase: InitialTerminalPhase,
    }
    impl DormantNativeAttemptOwner for InitialTerminalDormant {
        fn poll_bind_qualification(
            &mut self,
            schedule: &crate::coordination::AttemptSchedule,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            self.inner.poll_bind_qualification(schedule, cx)
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            self.inner.eligible_backends()
        }
        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            match self.phase {
                InitialTerminalPhase::ActivateFailure => Box::pin(async {
                    Err(NativeAttemptActivationFailure::new(
                        NativeAttemptPreparationFailure::from_input(fixture_execution_input(
                            "initial terminal fixture activation failure",
                        )),
                    ))
                }),
                InitialTerminalPhase::ActivatePanic => {
                    Box::pin(async { panic!("initial terminal fixture activation panic") })
                }
                phase => {
                    let activation = self.inner.activate(schedule, admissions, cancellation);
                    let guard = self.guard.clone();
                    let signals = Arc::clone(&self.signals);
                    Box::pin(async move {
                        let activated = activation.await?;
                        let (inner, runtime) = activated.into_parts();
                        debug_assert!(runtime.is_none());
                        Ok(ActivatedNativeAttempt::completion(InitialTerminalActive {
                            inner,
                            _guard: guard,
                            signals,
                            phase,
                        }))
                    })
                }
            }
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            let signals = Arc::clone(&self.signals);
            Box::pin(async move {
                signals.convergence_entered.store(true, Ordering::SeqCst);
                signals.release_convergence.notified().await;
                self.inner.converge(cancellation).await?;
                signals.convergence_completed.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    #[derive(Debug)]
    struct InitialTerminalActive {
        inner: Box<dyn ActiveNativeAttemptOwner>,
        _guard: TestWindow,
        signals: Arc<PreparationExitSignals>,
        phase: InitialTerminalPhase,
    }
    impl ActiveNativeAttemptOwner for InitialTerminalActive {
        fn run<'a>(
            &'a mut self,
            drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            match self.phase {
                // Delegate the existing exact Establish/admission fixture rather
                // than returning an unqualified synthetic Completed verdict.
                InitialTerminalPhase::CompletionSuccess => self.inner.run(drive, cancellation),
                InitialTerminalPhase::CompletionFailure => Box::pin(async {
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                        fixture_execution_input("initial terminal fixture run failure"),
                    ))
                }),
                InitialTerminalPhase::CompletionFailureDropPanic => {
                    Box::pin(TerminalDropPanicFuture(Some(
                        NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                            fixture_execution_input("initial terminal fixture run failure"),
                        )),
                    )))
                }
                InitialTerminalPhase::CompletionPanic => {
                    Box::pin(async { panic!("initial terminal fixture run panic") })
                }
                _ => unreachable!("activation failure has no active owner"),
            }
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            let signals = Arc::clone(&self.signals);
            Box::pin(async move {
                signals.convergence_entered.store(true, Ordering::SeqCst);
                signals.release_convergence.notified().await;
                let result = self.inner.converge(cancellation).await;
                signals.convergence_completed.store(true, Ordering::SeqCst);
                result
            })
        }
    }

    struct TerminalDropPanicFuture(Option<NativeAttemptTerminal>);
    impl Future for TerminalDropPanicFuture {
        type Output = NativeAttemptTerminal;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> Poll<Self::Output> {
            Poll::Ready(
                self.0
                    .take()
                    .expect("the actual run Future returns only once"),
            )
        }
    }
    impl Drop for TerminalDropPanicFuture {
        fn drop(&mut self) {
            panic!("initial terminal fixture returned Future Drop panic");
        }
    }

    #[derive(Debug)]
    struct DormantOriginalPanic(Arc<AtomicU64>);
    impl Drop for DormantOriginalPanic {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[derive(Debug)]
    struct DormantSecondaryDropPanic;
    #[derive(Debug)]
    struct DormantReportedCause(Arc<AtomicU64>);
    impl fmt::Display for DormantReportedCause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("injected dormant source-close failure")
        }
    }
    impl std::error::Error for DormantReportedCause {}
    impl Drop for DormantReportedCause {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[derive(Debug)]
    struct DormantCaptureOwner {
        polls: Arc<AtomicU64>,
        calls: Arc<AtomicU64>,
        owner_drops: Arc<AtomicU64>,
        primary_drops: Arc<AtomicU64>,
        reported: Option<Arc<DormantReportedCause>>,
        panic_on_future_drop: bool,
    }
    impl Drop for DormantCaptureOwner {
        fn drop(&mut self) {
            self.owner_drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct DormantCaptureFuture<'a> {
        owner: &'a mut DormantCaptureOwner,
        polled: bool,
    }
    impl Future for DormantCaptureFuture<'_> {
        type Output = Result<(), StatementNativeHealthFailure>;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> Poll<Self::Output> {
            assert!(!self.polled, "the same failed cleanup cannot be re-polled");
            self.polled = true;
            self.owner.polls.fetch_add(1, Ordering::SeqCst);
            if let Some(original) = &self.owner.reported {
                Poll::Ready(Err(StatementNativeHealthFailure::actual_exit_unproven(
                    "injected dormant source-close failure",
                    original.clone(),
                )))
            } else {
                std::panic::panic_any(DormantOriginalPanic(Arc::clone(&self.owner.primary_drops)))
            }
        }
    }
    impl Drop for DormantCaptureFuture<'_> {
        fn drop(&mut self) {
            if self.polled && self.owner.panic_on_future_drop {
                std::panic::panic_any(DormantSecondaryDropPanic);
            }
        }
    }
    impl DormantNativeAttemptOwner for DormantCaptureOwner {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            &[]
        }
        fn activate<'a>(
            &'a mut self,
            _: &'a AttemptSchedule,
            _: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            Box::pin(async { panic!("cleanup-only fixture cannot activate") })
        }
        fn converge<'a>(
            &'a mut self,
            _: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(DormantCaptureFuture {
                owner: self,
                polled: false,
            })
        }
    }

    #[tokio::test]
    async fn dormant_poll_panic_is_stable_before_original_future_drop_panics() {
        let mut control = {
            let (control, root) = governance().await;
            let cancellation = root.owner.scope().cancellation().unwrap();
            let requester = root.owner.cancellation_requester();
            let (_sender, mut shutdown) = watch::channel(false);
            let polls = Arc::new(AtomicU64::new(0));
            let calls = Arc::new(AtomicU64::new(0));
            let primary_drops = Arc::new(AtomicU64::new(0));
            let owner_drops = Arc::new(AtomicU64::new(0));
            let mut custody = crate::api::NativeDormantCustody::test_owner(DormantCaptureOwner {
                polls: Arc::clone(&polls),
                calls: Arc::clone(&calls),
                primary_drops: Arc::clone(&primary_drops),
                owner_drops: Arc::clone(&owner_drops),
                reported: None,
                panic_on_future_drop: true,
            });
            let original_owner = custody.as_mut() as *mut dyn DormantNativeAttemptOwner as *mut ();
            let secondary = futures::FutureExt::catch_unwind(AssertUnwindSafe(converge_dormant(
                &mut custody,
                cancellation.clone(),
                &mut shutdown,
                &requester,
            )))
            .await
            .expect_err("the real borrowed Future Drop must panic");
            assert!(secondary.is::<DormantSecondaryDropPanic>());
            assert!(
                custody
                    .convergence_panic()
                    .unwrap()
                    .is::<DormantOriginalPanic>()
            );
            assert_eq!(primary_drops.load(Ordering::SeqCst), 0);
            assert_eq!(owner_drops.load(Ordering::SeqCst), 0);
            assert_eq!(
                custody.as_mut() as *mut dyn DormantNativeAttemptOwner as *mut (),
                original_owner
            );
            assert!(
                converge_dormant(&mut custody, cancellation, &mut shutdown, &requester)
                    .await
                    .is_err()
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert!(custody.retire().is_err());
            assert_eq!(primary_drops.load(Ordering::SeqCst), 0);
            // Explicit fixture disposition, never a claimed Native exit.
            drop(secondary);
            drop(custody);
            assert_eq!(primary_drops.load(Ordering::SeqCst), 1);
            assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
            root.owner.complete();
            root.business.release();
            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn dormant_typed_failure_is_stable_before_original_future_drop_panics() {
        let mut control = {
            let (control, root) = governance().await;
            let cancellation = root.owner.scope().cancellation().unwrap();
            let requester = root.owner.cancellation_requester();
            let (_sender, mut shutdown) = watch::channel(false);
            let calls = Arc::new(AtomicU64::new(0));
            let cause_drops = Arc::new(AtomicU64::new(0));
            let cause = Arc::new(DormantReportedCause(Arc::clone(&cause_drops)));
            let mut custody = crate::api::NativeDormantCustody::test_owner(DormantCaptureOwner {
                polls: Arc::new(AtomicU64::new(0)),
                calls: Arc::clone(&calls),
                primary_drops: Arc::new(AtomicU64::new(0)),
                owner_drops: Arc::new(AtomicU64::new(0)),
                reported: Some(Arc::clone(&cause)),
                panic_on_future_drop: true,
            });
            let secondary = futures::FutureExt::catch_unwind(AssertUnwindSafe(converge_dormant(
                &mut custody,
                cancellation.clone(),
                &mut shutdown,
                &requester,
            )))
            .await
            .expect_err("the real borrowed Future Drop must panic");
            assert!(secondary.is::<DormantSecondaryDropPanic>());
            let original = custody.convergence_failure().unwrap();
            assert!(std::ptr::eq(
                original
                    .cause()
                    .downcast_ref::<DormantReportedCause>()
                    .unwrap(),
                cause.as_ref(),
            ));
            assert!(custody.convergence_panic().is_none());
            assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
            let error = converge_dormant(&mut custody, cancellation, &mut shutdown, &requester)
                .await
                .unwrap_err();
            assert_eq!(error.message(), "injected dormant source-close failure");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(custody.retire().is_err());
            drop(secondary);
            drop(custody);
            assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
            drop(cause);
            assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
            root.owner.complete();
            root.business.release();
            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[derive(Debug)]
    struct ReadyActivationDropOwner(Arc<AtomicU64>);
    impl DormantNativeAttemptOwner for ReadyActivationDropOwner {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            &[]
        }
        fn activate<'a>(
            &'a mut self,
            _: &'a AttemptSchedule,
            _: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            Box::pin(ReadyActivationDropFuture {
                original: Some(ActivatedNativeAttempt::completion(
                    ReadyActivationActiveOwner(Arc::clone(&self.0)),
                )),
                returned: false,
            })
        }
        fn converge<'a>(
            &'a mut self,
            _: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }
    struct ReadyActivationDropFuture {
        original: Option<ActivatedNativeAttempt>,
        returned: bool,
    }
    impl Future for ReadyActivationDropFuture {
        type Output = Result<ActivatedNativeAttempt, NativeAttemptActivationFailure>;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> Poll<Self::Output> {
            self.returned = true;
            Poll::Ready(Ok(self
                .original
                .take()
                .expect("one original activated output")))
        }
    }
    impl Drop for ReadyActivationDropFuture {
        fn drop(&mut self) {
            if self.returned {
                std::panic::panic_any(DormantSecondaryDropPanic);
            }
        }
    }
    #[derive(Debug)]
    struct ReadyActivationActiveOwner(Arc<AtomicU64>);
    impl Drop for ReadyActivationActiveOwner {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl ActiveNativeAttemptOwner for ReadyActivationActiveOwner {
        fn run<'a>(
            &'a mut self,
            _: &'a NativeAttemptDrive,
            _: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { NativeAttemptTerminal::Completed })
        }
        fn converge<'a>(
            &'a mut self,
            _: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async {
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }
    #[tokio::test]
    async fn activation_ready_output_survives_original_future_drop_panic() {
        let mut control = {
            let (control, root) = governance().await;
            let (_, attempts, _, _, _) = failing_native();
            let request = completion_request(attempts);
            let scheduling = crate::api::ExecutionSchedulingFacts::from_frozen_description(
                request.description(),
                &std::collections::BTreeMap::new(),
            )
            .unwrap();
            let execution = QueryExecutionId::new(
                novarocks_types::QueryId::new(0x72, 0x19),
                AttemptId::new(1).unwrap(),
            )
            .unwrap();
            let schedule = build_attempt_schedule(
                execution,
                FrontendProcessId::new_v7(),
                &[BackendProcessId::new_v7()],
                &scheduling,
                #[cfg(novarocks_dev_controls)]
                Ok(None),
            )
            .unwrap();
            let drops = Arc::new(AtomicU64::new(0));
            let mut custody = crate::api::NativeDormantCustody::test_owner(
                ReadyActivationDropOwner(Arc::clone(&drops)),
            );
            // This is the SAME receiving field used by OriginalNativePreparation.
            let mut pending_activated = None;
            let failure = futures::FutureExt::catch_unwind(AssertUnwindSafe(activate_dormant(
                &mut custody,
                &mut pending_activated,
                &schedule,
                None,
                root.owner.scope().cancellation().unwrap(),
            )))
            .await
            .expect_err("the actual returned Future Drop must panic");
            assert!(failure.is::<DormantSecondaryDropPanic>());
            assert!(pending_activated.is_some());
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert!(
                custody.activation_panic().is_none(),
                "the failure was F Drop, not activation poll"
            );
            // Explicit test disposition only, never a fabricated Native receipt.
            drop(failure);
            drop(pending_activated.take());
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            drop(custody);
            drop(request);
            root.owner.complete();
            root.business.release();
            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    async fn wait_for_dormant_failure(exit: &mut crate::api::LogicalExecutionExitObserver) {
        tokio::time::timeout(
            Duration::from_secs(1),
            std::future::poll_fn(|cx| {
                match exit.poll_exit(cx) {
                    Poll::Pending => {}
                    Poll::Ready(original) => std::panic::panic_any(original),
                }
                // Register the original observer before rechecking its level fact.
                if exit.view().is_some_and(|view| view.native_dormant_failed()) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("the original Source must publish its dormant failure fact");
    }

    async fn initial_terminal_preparation_fixture(phase: InitialTerminalPhase) {
        let mut control = {
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let capacity = scope
                .result_capacity()
                .expect("original scope result capacity");
            let window = capacity
                .try_acquire(
                    &scope,
                    novarocks_workload_control::ResultWindowClass::Internal,
                )
                .expect("original Internal window admission");
            let signals = Arc::new(PreparationExitSignals::default());
            let frontend = FrontendProcessId::new_v7();
            let attempts = InitialTerminalPreparationPort {
                state: Arc::new(PreparationExitState {
                    window: Some(TestWindow(window.retain_alias())),
                    signals: Arc::clone(&signals),
                }),
                backend: BackendProcessId::new_v7(),
                frontend,
                phase,
            };
            drop(window);
            let original_internal_positions = capacity.snapshot().held_positions[2];
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x78),
                frontend,
                supervisor_config().with_remote_cleanup_timeout(Duration::from_secs(10)),
            );
            let mut original_exit = None;
            let future = if matches!(
                phase,
                InitialTerminalPhase::ActivatePanic
                    | InitialTerminalPhase::CompletionPanic
                    | InitialTerminalPhase::CompletionFailureDropPanic
            ) {
                let mut input =
                    match client.prepare_observed_start(completion_request(attempts), None) {
                        Ok(input) => Some(input),
                        Err(original) => std::panic::panic_any(original),
                    };
                let mut owner = Some(root.owner);
                let mut ready = None;
                client
                    .start_observed_in(&mut input, &mut owner, &mut ready, &mut original_exit)
                    .unwrap()
                    .finish()
                    .unwrap();
                ready.take().unwrap()
            } else {
                client.start(completion_request(attempts), root.owner)
            };
            // Capture failure observations without asserting while a real owner waits.
            let observations: Result<(bool, bool, usize), &'static str> = async {
                let reply = tokio::time::timeout(Duration::from_secs(1), future)
                    .await
                    .map_err(|_| "initial terminal reply timed out")?;
                match phase {
                    InitialTerminalPhase::CompletionSuccess => {
                        let mut handle = reply.map_err(|_| "exact Establish completion failed")?;
                        if !matches!(
                            handle.take_output(),
                            Some(crate::api::ExecutionOutput::Completion)
                        ) {
                            return Err("success did not preserve original Completion output");
                        }
                        drop(handle);
                    }
                    phase => {
                        let error = match reply {
                            Err(error) => error,
                            Ok(handle) => {
                                drop(handle);
                                return Err("terminal failure returned success");
                            }
                        };
                        let expected = match phase {
                            InitialTerminalPhase::ActivateFailure => {
                                "initial terminal fixture activation failure"
                            }
                            InitialTerminalPhase::ActivatePanic => {
                                "Native attempt activation panicked"
                            }
                            InitialTerminalPhase::CompletionFailure
                            | InitialTerminalPhase::CompletionFailureDropPanic => {
                                "initial terminal fixture run failure"
                            }
                            InitialTerminalPhase::CompletionPanic => "Native attempt run panicked",
                            _ => unreachable!(),
                        };
                        if error.message() != expected {
                            return Err("original terminal failure source/order changed");
                        }
                    }
                }
                wait_preparation_exit_fact(&signals.convergence_entered).await?;
                Ok((
                    signals.factory_exited.load(Ordering::SeqCst),
                    !signals.convergence_completed.load(Ordering::SeqCst),
                    capacity.snapshot().held_positions[2],
                ))
            }
            .await;
            // Release and observe the same original convergence before shutdown can
            // legitimately end its optional remote tracking. Then settle actual scope.
            signals.release_convergence.notify_one();
            let convergence_exit = wait_preparation_exit_fact(&signals.convergence_completed).await;
            root.business.release();
            let shutdown = supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(2))
                .await;
            acknowledge_all_control(&control);
            let settled = if matches!(
                phase,
                InitialTerminalPhase::ActivatePanic
                    | InitialTerminalPhase::CompletionPanic
                    | InitialTerminalPhase::CompletionFailureDropPanic
            ) {
                None
            } else {
                Some(tokio::time::timeout(Duration::from_secs(2), scope.wait_released()).await)
            };
            assert!(
                convergence_exit.is_ok(),
                "original convergence did not exit: {phase:?}"
            );
            let joined_with_expected_outcome = matches!(
                (&shutdown, phase),
                (
                    Ok(()),
                    InitialTerminalPhase::ActivateFailure
                        | InitialTerminalPhase::CompletionSuccess
                        | InitialTerminalPhase::CompletionFailure,
                ) | (
                    Err(LogicalExecutionSupervisorShutdownError::Registry(
                        LogicalExecutionRuntimeRegistryError::OriginalExitFailureUnresolved,
                    )),
                    InitialTerminalPhase::ActivatePanic
                        | InitialTerminalPhase::CompletionPanic
                        | InitialTerminalPhase::CompletionFailureDropPanic,
                )
            );
            assert!(
                joined_with_expected_outcome,
                "original supervisor owner did not join with its expected outcome: {phase:?} {shutdown:?}"
            );
            if matches!(phase, InitialTerminalPhase::ActivatePanic) {
                let mut exit = original_exit.take().expect("same original observed start");
                wait_for_dormant_failure(&mut exit).await;
                let view = exit.view().unwrap();
                assert!(view.native_dormant_failed());
                assert!(!view.native_preparation_exited());
                drop(view);
                let original = exit.dormant_failure().unwrap();
                assert!(original.activation_panic().is_some());
                assert!(original.convergence_panic().is_none());
                assert!(original.reported_failure().is_none());
                drop(original);
                assert!(futures::poll!(std::future::poll_fn(|cx| exit.poll_exit(cx))).is_pending());
                assert!(futures::poll!(Box::pin(scope.wait_released())).is_pending());
                assert_eq!(capacity.snapshot().held_positions[2], 1);
                assert!(
                    supervisor.join.is_none(),
                    "the original root actually joined"
                );
                assert!(supervisor.run_outcome.is_some());
                assert!(!supervisor.shutdown_complete);
                drop(exit);
            } else if matches!(
                phase,
                InitialTerminalPhase::CompletionPanic
                    | InitialTerminalPhase::CompletionFailureDropPanic
            ) {
                let exit = original_exit.take().expect("same original run Source");
                let view = exit.view().unwrap();
                if matches!(phase, InitialTerminalPhase::CompletionPanic) {
                    assert_eq!(
                        view.native_run_panic().unwrap().downcast_ref::<&str>(),
                        Some(&"initial terminal fixture run panic")
                    );
                    assert!(view.native_run_cleanup_panic().is_none());
                } else {
                    assert!(view.native_run_panic().is_none());
                    assert_eq!(
                        view.native_run_cleanup_panic()
                            .unwrap()
                            .downcast_ref::<&str>(),
                        Some(&"initial terminal fixture returned Future Drop panic")
                    );
                }
                assert!(view.native_preparation_exited());
                assert!(!view.role_failed());
                assert_eq!(view.pending_native_returns(), 0);
                drop(view);
                assert_eq!(supervisor.registry.active_native_custody_count(), 0);
                assert!(futures::poll!(Box::pin(scope.wait_released())).is_pending());
                drop(exit);
            } else {
                assert!(
                    settled.unwrap().is_ok(),
                    "original scope did not settle: {phase:?}"
                );
            }
            let (factory_exited, convergence_held, held_positions) =
                observations.expect("bounded terminal facts");
            assert_eq!(original_internal_positions, 1);
            assert!(
                factory_exited,
                "terminal caller retained preparation state: {phase:?}"
            );
            assert!(
                convergence_held,
                "original convergence released before observation: {phase:?}"
            );
            // The real Dormant/Active guard must remain live even after the unused
            // factory drops. Zero here would be a false release, not a desired result.
            assert_eq!(
                held_positions, 1,
                "original attempt guard lost its window: {phase:?}"
            );
            if matches!(
                phase,
                InitialTerminalPhase::ActivatePanic
                    | InitialTerminalPhase::CompletionPanic
                    | InitialTerminalPhase::CompletionFailureDropPanic
            ) {
                // This is the existing explicit process disposition, not a
                // successful cleanup or an invented source-exit receipt.
                supervisor.abandon_for_process_exit();
            } else {
                assert_eq!(capacity.snapshot().held_positions[2], 0);
            }

            control
        };
        if matches!(
            phase,
            InitialTerminalPhase::ActivatePanic
                | InitialTerminalPhase::CompletionPanic
                | InitialTerminalPhase::CompletionFailureDropPanic
        ) {
            control.finish_for_process_exit().await;
        } else {
            acknowledge_all_control(&control);
            control.finish().await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn initial_activation_failure_retires_only_unused_preparation_before_convergence() {
        initial_terminal_preparation_fixture(InitialTerminalPhase::ActivateFailure).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn initial_activation_panic_retires_only_unused_preparation_before_convergence() {
        initial_terminal_preparation_fixture(InitialTerminalPhase::ActivatePanic).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn completion_success_retires_only_unused_preparation_before_convergence() {
        initial_terminal_preparation_fixture(InitialTerminalPhase::CompletionSuccess).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn completion_failure_retires_only_unused_preparation_before_convergence() {
        initial_terminal_preparation_fixture(InitialTerminalPhase::CompletionFailure).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn returned_business_failure_survives_actual_run_future_drop_panic() {
        initial_terminal_preparation_fixture(InitialTerminalPhase::CompletionFailureDropPanic)
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completion_panic_retires_only_unused_preparation_before_convergence() {
        initial_terminal_preparation_fixture(InitialTerminalPhase::CompletionPanic).await;
    }

    #[tokio::test]
    async fn rows_handle_is_handed_off_before_the_attempt_converges() {
        let mut control = {
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let converged = Arc::new(AtomicBool::new(false));
            let attempts = RowsAttemptPreparationPort {
                relay_window: None,
                backend: BackendProcessId::new_v7(),
                converged: Arc::clone(&converged),
            };
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x52),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let mut handle = tokio::time::timeout(
                Duration::from_secs(1),
                client.start(
                    rows_request(&control, &scope, RecoveryMode::NoRecovery, None, attempts),
                    root.owner,
                ),
            )
            .await
            .expect("Rows execution must hand off its bounded result consumer")
            .unwrap();
            let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
                panic!("Rows execution must return its result stream");
            };
            assert!(!converged.load(Ordering::SeqCst));
            handle.request_cancel().unwrap();
            let Err(error) = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("scope cancellation must terminate the result stream")
            else {
                panic!("cancelled Rows stream must end with a typed error");
            };
            assert!(!error.message().is_empty());
            drop(stream);
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            drop(handle);
            scope.wait_released().await;
            assert!(converged.load(Ordering::SeqCst));

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[derive(Debug)]
    struct ImmediateReplacementPort {
        capacity: watch::Sender<u64>,
    }

    impl Default for ImmediateReplacementPort {
        fn default() -> Self {
            let (capacity, _) = watch::channel(1);
            Self { capacity }
        }
    }

    #[derive(Debug)]
    struct ImmediateReplacementEffectReservation;

    impl super::super::ReplacementQualificationEffectReservation
        for ImmediateReplacementEffectReservation
    {
        fn submit(
            self: Box<Self>,
            submission: super::super::ReplacementQualificationEffectSubmission,
        ) {
            let request = submission.request();
            let admissions: Box<[_]> = request
                .replacement_contexts()
                .iter()
                .enumerate()
                .map(|(index, context)| {
                    let tag = u8::try_from(index + 1).unwrap();
                    let valid_for = LeaseValidFor::new(Duration::from_secs(30)).unwrap();
                    let admission_request =
                        novarocks_execution_contract::AcquireQueryContextAdmissionTicket::new(
                            TaskOperationId::new_v7(),
                            *context,
                            valid_for,
                            NativeCompatibilityId::new([tag; 32]),
                            AdmissionEpochCapability::try_from_bytes([tag; 16]).unwrap(),
                        );
                    let ticket = QueryContextAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([tag; 16]).unwrap(),
                        *context,
                        valid_for,
                    );
                    super::super::QualifiedWorkerAdmission::try_new(
                        request.identity().replacement(),
                        admission_request,
                        ticket,
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            let reservation = ImmediateIsolationReservation {
                identity: request.identity(),
                failed_contexts: request.failed_contexts().iter().copied().collect(),
                replacement_contexts: request.replacement_contexts().iter().copied().collect(),
                admissions,
            };
            let qualified = super::super::QualifiedReplacementReservation::try_new(
                request,
                Box::new(reservation),
            )
            .unwrap();
            submission.qualified(qualified).unwrap();
        }
    }

    impl super::super::ReplacementQualificationEffectPort for ImmediateReplacementPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _request: &super::super::ReplacementQualificationRequest,
        ) -> super::super::ReplacementQualificationEffectAdmission {
            super::super::ReplacementQualificationEffectAdmission::Admitted(Box::new(
                ImmediateReplacementEffectReservation,
            ))
        }
    }

    #[derive(Debug)]
    struct ImmediateIsolationReservation {
        identity: super::super::ReplacementQualificationIdentity,
        failed_contexts: BTreeSet<QueryContextRef>,
        replacement_contexts: BTreeSet<QueryContextRef>,
        admissions: Box<[super::super::QualifiedWorkerAdmission]>,
    }

    impl super::super::AttemptIsolationReservation for ImmediateIsolationReservation {
        fn identity(&self) -> super::super::ReplacementQualificationIdentity {
            self.identity
        }

        fn topology_revision(&self) -> u64 {
            1
        }

        fn admissions(&self) -> &[super::super::QualifiedWorkerAdmission] {
            &self.admissions
        }

        fn validate_binding(
            &self,
            failed_contexts: &[QueryContextRef],
        ) -> Result<(), super::super::ReplacementQualificationFailure> {
            let failed: BTreeSet<_> = failed_contexts.iter().copied().collect();
            let replacements: BTreeSet<_> = self
                .admissions
                .iter()
                .map(super::super::QualifiedWorkerAdmission::context)
                .collect();
            (failed == self.failed_contexts && replacements == self.replacement_contexts)
                .then_some(())
                .ok_or(super::super::ReplacementQualificationFailure::InvalidReservation)
        }

        fn activate(
            self: Box<Self>,
        ) -> Result<
            Box<dyn super::super::ActiveAttemptIsolationOwner>,
            super::super::AttemptIsolationActivationFailure,
        > {
            Ok(Box::new(ImmediateActiveIsolation {
                execution: self.identity.replacement(),
            }))
        }

        fn abandon(self: Box<Self>) {}
    }

    #[derive(Debug)]
    struct ImmediateActiveIsolation {
        execution: QueryExecutionId,
    }

    impl super::super::ActiveAttemptIsolationOwner for ImmediateActiveIsolation {
        fn execution(&self) -> QueryExecutionId {
            self.execution
        }

        fn finish(self: Box<Self>) {}

        fn abandon(self: Box<Self>) {}
    }

    fn fixture_recovery_backends(processes: usize) -> Box<[BackendProcessId]> {
        (0..processes).map(|_| BackendProcessId::new_v7()).collect()
    }

    #[derive(Debug)]
    struct RecoveringRowsPreparationPort {
        relay_window: Option<TestWindow>,
        backends: Box<[BackendProcessId]>,
        prepares: Arc<AtomicU64>,
        runs: Arc<AtomicU64>,
        convergences: Arc<AtomicU64>,
        allow_initial_convergence: Arc<tokio::sync::Notify>,
        block_initial_convergence: bool,
        replacement_missing_rows: bool,
        replacement_activation_panic: bool,
        failures_before_success: u64,
    }

    impl TestRowsPreparationPort for RecoveringRowsPreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            self.relay_window = Some(TestWindow(window));
        }
    }

    impl NativeAttemptPreparationPort for RecoveringRowsPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let ordinal = self.prepares.fetch_add(1, Ordering::SeqCst) + 1;
            // Every process identity was created with this fixture, before
            // any BackendLost input. Previously failed processes are not reused.
            let requirement = request.topology_requirement();
            let backend = self
                .backends
                .iter()
                .copied()
                .skip(usize::try_from(ordinal - 1).unwrap())
                .find(|process| match requirement {
                    crate::api::NativeAttemptTopologyRequirement::LiveSnapshot => true,
                    crate::api::NativeAttemptTopologyRequirement::ExcludeProcess(excluded) => {
                        *process != excluded
                    }
                })
                .expect("fixture has enough original nonexcluded process identities");
            if let crate::api::NativeAttemptTopologyRequirement::ExcludeProcess(excluded) =
                requirement
            {
                assert_ne!(backend, excluded);
            }
            let owner = RecoveringRowsDormantOwner {
                relay_window: self
                    .relay_window
                    .as_ref()
                    .expect("fixture window was admitted")
                    .clone(),
                backend,
                ordinal,
                runs: Arc::clone(&self.runs),
                convergences: Arc::clone(&self.convergences),
                allow_initial_convergence: Arc::clone(&self.allow_initial_convergence),
                block_initial_convergence: self.block_initial_convergence,
                replacement_missing_rows: self.replacement_missing_rows,
                replacement_activation_panic: self.replacement_activation_panic,
                failures_before_success: self.failures_before_success,
            };
            request.stage_fixture_owner(owner);
            Box::pin(async move {
                let scheduling = no_scan_scheduling(request);
                request.bind_staged(scheduling).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct RecoveringRowsDormantOwner {
        relay_window: TestWindow,
        backend: BackendProcessId,
        ordinal: u64,
        runs: Arc<AtomicU64>,
        convergences: Arc<AtomicU64>,
        allow_initial_convergence: Arc<tokio::sync::Notify>,
        block_initial_convergence: bool,
        replacement_missing_rows: bool,
        replacement_activation_panic: bool,
        failures_before_success: u64,
    }

    impl DormantNativeAttemptOwner for RecoveringRowsDormantOwner {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let root = schedule.root();
            let context = schedule.contexts()[0];
            let ordinal = self.ordinal;
            let runs = Arc::clone(&self.runs);
            let convergences = Arc::clone(&self.convergences);
            let allow_initial_convergence = Arc::clone(&self.allow_initial_convergence);
            let block_initial_convergence = self.block_initial_convergence;
            let replacement_missing_rows = self.replacement_missing_rows;
            let replacement_activation_panic = self.replacement_activation_panic;
            let failures_before_success = self.failures_before_success;
            let window = self.relay_window.clone();
            Box::pin(async move {
                if replacement_activation_panic && ordinal > 1 {
                    panic!("injected Native replacement activation panic");
                }
                if replacement_missing_rows && ordinal > 1 {
                    return Ok(ActivatedNativeAttempt::completion(PanickingActiveOwner {
                        phase: AttemptPanicPhase::Run,
                        convergence_calls: convergences,
                    }));
                }
                let (status_sender, statuses) =
                    super::super::accepted_root_status_projection_with_control_port(
                        root,
                        Arc::new(ClosedSuccessSealPort),
                    );
                status_sender
                    .publish_attempt_observation(
                        running_status(root),
                        super::super::AcceptedAttemptFailure::None,
                    )
                    .unwrap();
                let binding = test_root_binding(
                    root,
                    window.0,
                    Some(&[
                        14, 0, 0, 0, 1, b'2', 11, b'r', b'e', b'p', b'l', b'a', b'c', b'e', b'm',
                        b'e', b'n', b't',
                    ]),
                );
                Ok(ActivatedNativeAttempt::relayed_rows(
                    RecoveringRowsActiveOwner {
                        ordinal,
                        failures_before_success,
                        context,
                        runs,
                        convergences,
                        allow_initial_convergence,
                        block_initial_convergence,
                        _status_sender: status_sender,
                    },
                    binding,
                    statuses,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.convergences.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    struct RecoveringRowsActiveOwner {
        ordinal: u64,
        failures_before_success: u64,
        context: QueryContextRef,
        runs: Arc<AtomicU64>,
        convergences: Arc<AtomicU64>,
        allow_initial_convergence: Arc<tokio::sync::Notify>,
        block_initial_convergence: bool,
        _status_sender: super::super::AcceptedRootStatusSender,
    }

    impl fmt::Debug for RecoveringRowsActiveOwner {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("RecoveringRowsActiveOwner")
                .field("ordinal", &self.ordinal)
                .finish_non_exhaustive()
        }
    }

    impl ActiveNativeAttemptOwner for RecoveringRowsActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                self.runs.fetch_add(1, Ordering::SeqCst);
                if self.ordinal <= self.failures_before_success {
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                        ExecutionFailureInput::new(
                            QueryFailureReason::BackendLost.failure_fact(),
                            SafeDetail::truncating(
                                "injected pre-visibility infrastructure failure",
                            ),
                            ExecutionFailureSite::Backend {
                                process: self.context.backend_process_id(),
                            },
                        ),
                    ))
                } else {
                    let reason = cancellation.cancelled().await;
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                        fixture_cancellation_input(reason, "replacement attempt was cancelled"),
                    ))
                }
            })
        }

        fn converge<'a>(
            &'a mut self,
            request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                if self.ordinal == 1 && self.block_initial_convergence {
                    // This fixture has no local close work: the held fact is
                    // only its original remote observation. Shutdown can end
                    // that wait without fabricating WorkerStopped evidence.
                    tokio::select! {
                        biased;
                        _ = self.allow_initial_convergence.notified() => {}
                        _ = tokio::time::sleep_until(request.remote_observation_deadline().into()) => {
                            return Ok(NativeActiveConvergenceClosure::RemoteTrackingEnded {
                                exact_contexts: vec![self.context].into_boxed_slice(),
                            });
                        }
                    }
                }
                self.convergences.fetch_add(1, Ordering::SeqCst);
                Ok(NativeActiveConvergenceClosure::Evidence(
                    if self.ordinal == 1 {
                        NativeAttemptConvergence::contexts(vec![
                            crate::api::NativeContextConvergence::worker_process_replaced(
                                self.context,
                            ),
                        ])
                    } else {
                        NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced()
                    },
                ))
            })
        }
    }

    #[derive(Debug)]
    struct HeldReplacementPreparationPort {
        inner: RecoveringRowsPreparationPort,
        release: Arc<tokio::sync::Notify>,
        exits: Arc<AtomicU64>,
        entered: Arc<AtomicU64>,
    }

    impl TestRowsPreparationPort for HeldReplacementPreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            self.inner.bind_window(window);
        }
    }

    impl NativeAttemptPreparationPort for HeldReplacementPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let replacement = request.execution().attempt_id().get() > 1;
            let prepared = self.inner.prepare_in(request);
            let release = Arc::clone(&self.release);
            let exits = Arc::clone(&self.exits);
            let entered = Arc::clone(&self.entered);
            Box::pin(async move {
                struct PreparationExit(Arc<AtomicU64>);
                impl Drop for PreparationExit {
                    fn drop(&mut self) {
                        self.0.fetch_add(1, Ordering::SeqCst);
                    }
                }
                if replacement {
                    let _exit = PreparationExit(exits);
                    entered.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    prepared.await
                } else {
                    prepared.await
                }
            })
        }
    }

    async fn exercise_held_replacement_preparation(
        cancel_reason: Option<CancellationReason>,
        release: bool,
    ) {
        let mut control = {
            let cancel = cancel_reason.is_some();
            let prepares = Arc::new(AtomicU64::new(0));
            let runs = Arc::new(AtomicU64::new(0));
            let convergences = Arc::new(AtomicU64::new(0));
            let exits = Arc::new(AtomicU64::new(0));
            let entered = Arc::new(AtomicU64::new(0));
            let gate = Arc::new(tokio::sync::Notify::new());
            let attempts = HeldReplacementPreparationPort {
                inner: RecoveringRowsPreparationPort {
                    relay_window: None,
                    backends: fixture_recovery_backends(2),
                    prepares: Arc::clone(&prepares),
                    runs: Arc::clone(&runs),
                    convergences: Arc::clone(&convergences),
                    allow_initial_convergence: Arc::new(tokio::sync::Notify::new()),
                    block_initial_convergence: false,
                    replacement_missing_rows: false,
                    replacement_activation_panic: false,
                    failures_before_success: 1,
                },
                release: Arc::clone(&gate),
                exits: Arc::clone(&exits),
                entered: Arc::clone(&entered),
            };
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let requester = root.owner.cancellation_requester();
            let mut config = supervisor_config();
            config.rows.replacement_reservation_valid_for = if cancel || release {
                Duration::from_secs(30)
            } else {
                Duration::from_millis(50)
            };
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x59),
                FrontendProcessId::new_v7(),
                config,
            );
            let mut handle = client
                .start(
                    rows_request(
                        &control,
                        &scope,
                        RecoveryMode::RestartAttemptBeforeVisibility,
                        Some(Arc::new(ImmediateReplacementPort::default())),
                        attempts,
                    ),
                    root.owner,
                )
                .await
                .unwrap();
            let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
                panic!("recoverable Rows execution must retain its result consumer");
            };
            tokio::time::timeout(Duration::from_secs(1), async {
                while entered.load(Ordering::SeqCst) != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the failed attempt must enter replacement preparation");
            assert_eq!(runs.load(Ordering::SeqCst), 1);
            stream.begin_schema().unwrap().complete();
            if release {
                gate.notify_one();
                let crate::api::ResultDelivery::Segment(delivery) =
                    tokio::time::timeout(Duration::from_secs(1), stream.next())
                        .await
                        .expect("replacement released within the budget must deliver")
                        .unwrap()
                        .unwrap()
                else {
                    panic!("replacement must deliver its actual result batch");
                };
                delivery.complete();
                assert_eq!(runs.load(Ordering::SeqCst), 2);
            }
            if let Some(reason) = cancel_reason.clone() {
                requester.request(reason.clone()).unwrap();
                requester.request(CancellationReason::Requested).unwrap();
                assert_eq!(scope.cancellation().unwrap().reason(), Some(reason));
            } else if release {
                handle.request_cancel().unwrap();
            }
            let terminal = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("bounded preparation or cancellation must terminate the public stream");
            let Err(error) = terminal else {
                panic!("the stream must expose the failed logical execution");
            };
            if cancel || release {
                let expected = match cancel_reason.as_ref() {
                    Some(
                        CancellationReason::DeadlineExceeded
                        | CancellationReason::FrontendDrainDeadlineExceeded,
                    ) => QueryFailureReason::DeadlineExceeded,
                    _ => QueryFailureReason::Cancelled,
                };
                let input = error
                    .execution_failure()
                    .expect("public cancellation retains canonical input");
                assert_eq!(input.fact(), expected.failure_fact());
                assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
                assert_eq!(
                    error.kind(),
                    if matches!(expected, QueryFailureReason::DeadlineExceeded) {
                        QueryExecutionErrorKind::DeadlineExceeded
                    } else {
                        QueryExecutionErrorKind::Cancelled
                    },
                );
                if matches!(expected, QueryFailureReason::Cancelled) {
                    assert_eq!(
                        error.to_string(),
                        "logical execution was cancelled before success EOF"
                    );
                }
            } else {
                assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
                assert_eq!(
                    error.to_string(),
                    "injected pre-visibility infrastructure failure"
                );
                let original = error
                    .execution_failure()
                    .expect("the originating attempt failure remains canonical");
                assert_eq!(
                    original.fact(),
                    QueryFailureReason::BackendLost.failure_fact()
                );
                assert!(matches!(
                    original.site(),
                    ExecutionFailureSite::Backend { .. }
                ));
            }
            if !release {
                assert_eq!(
                    runs.load(Ordering::SeqCst),
                    1,
                    "a held prepare cannot activate"
                );
            }
            tokio::time::timeout(Duration::from_secs(1), async {
                while exits.load(Ordering::SeqCst) != 1 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("the pending preparation must really exit before shutdown is requested");
            drop(stream);
            root.business.release();
            let shutdown = supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await;
            if release {
                shutdown.unwrap();
            } else if cancel {
                shutdown.expect("accepted query cancellation is not a supervisor health failure");
            } else {
                let Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(error)) =
                    shutdown
                else {
                    panic!("the supervisor must retain the precise preparation failure");
                };
                assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
                assert_eq!(
                    error.to_string(),
                    "Native replacement preparation exceeded its recovery budget"
                );
                assert_recovery_preparation_budget_input(&error);
            }
            acknowledge_all_control(&control);
            drop(handle);
            scope.wait_released().await;
            assert_eq!(exits.load(Ordering::SeqCst), 1);
            assert_eq!(
                convergences.load(Ordering::SeqCst),
                if release { 2 } else { 1 }
            );

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_recovery_does_not_refresh_the_preparation_deadline() {
        let mut control = {
            let prepares = Arc::new(AtomicU64::new(0));
            let runs = Arc::new(AtomicU64::new(0));
            let convergences = Arc::new(AtomicU64::new(0));
            let exits = Arc::new(AtomicU64::new(0));
            let entered = Arc::new(AtomicU64::new(0));
            let gate = Arc::new(tokio::sync::Notify::new());
            let attempts = HeldReplacementPreparationPort {
                inner: RecoveringRowsPreparationPort {
                    relay_window: None,
                    backends: fixture_recovery_backends(3),
                    prepares: Arc::clone(&prepares),
                    runs: Arc::clone(&runs),
                    convergences: Arc::clone(&convergences),
                    allow_initial_convergence: Arc::new(tokio::sync::Notify::new()),
                    block_initial_convergence: false,
                    replacement_missing_rows: false,
                    replacement_activation_panic: false,
                    failures_before_success: 2,
                },
                release: Arc::clone(&gate),
                exits: Arc::clone(&exits),
                entered: Arc::clone(&entered),
            };
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let mut config = supervisor_config();
            config.rows.max_attempts = NonZeroU32::new(3).unwrap();
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x5a),
                FrontendProcessId::new_v7(),
                config,
            );
            let mut handle = client
                .start(
                    rows_request(
                        &control,
                        &scope,
                        RecoveryMode::RestartAttemptBeforeVisibility,
                        Some(Arc::new(ImmediateReplacementPort::default())),
                        attempts,
                    ),
                    root.owner,
                )
                .await
                .unwrap();
            let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
                panic!("recovery must retain the public stream");
            };
            stream.begin_schema().unwrap().complete();
            while entered.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_secs(20)).await;
            gate.notify_one();
            while entered.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
            assert_eq!(runs.load(Ordering::SeqCst), 2);
            tokio::time::advance(Duration::from_secs(10)).await;
            tokio::time::timeout(Duration::from_secs(1), async {
                while exits.load(Ordering::SeqCst) != 2 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("the held third prepare must exit at the original deadline");
            let terminal = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("the third prepare must expire at the first recovery deadline");
            let Err(error) = terminal else {
                panic!("the expired replacement must retain the originating failure");
            };
            assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
            assert_eq!(
                error.to_string(),
                "injected pre-visibility infrastructure failure"
            );
            assert_eq!(exits.load(Ordering::SeqCst), 2);
            assert_eq!(
                runs.load(Ordering::SeqCst),
                2,
                "expired third prepare cannot activate"
            );
            drop(stream);
            root.business.release();
            // shutdown_until converts the supplied std Instant back into Tokio
            // time. Use the advanced clock so this is a future deadline, rather
            // than the wall clock's already-past instant in this paused test.
            let shutdown_deadline = tokio::time::Instant::now().into_std() + Duration::from_secs(1);
            let shutdown = supervisor.shutdown_until(shutdown_deadline).await;
            let Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(error)) = shutdown
            else {
                panic!("the preparation budget failure must survive owned shutdown: {shutdown:?}");
            };
            assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
            assert_eq!(
                error.message(),
                "Native replacement preparation exceeded its recovery budget"
            );
            assert_recovery_preparation_budget_input(&error);
            acknowledge_all_control(&control);
            drop(handle);
            scope.wait_released().await;
            assert_eq!(convergences.load(Ordering::SeqCst), 2);

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn replacement_preparation_budget_expires_and_converges_the_original_owner() {
        exercise_held_replacement_preparation(None, false).await;
    }

    #[tokio::test]
    async fn cancellation_interrupts_pending_replacement_preparation() {
        exercise_held_replacement_preparation(Some(CancellationReason::Requested), false).await;
    }

    #[tokio::test]
    async fn accepted_deadline_interrupts_pending_replacement_preparation() {
        exercise_held_replacement_preparation(Some(CancellationReason::DeadlineExceeded), false)
            .await;
    }

    fn assert_recovery_preparation_budget_input(error: &QueryExecutionError) {
        let input = error
            .execution_failure()
            .expect("recovery budget retains its own canonical input");
        assert_eq!(
            input.fact(),
            QueryFailureReason::RecoveryPreparationBudgetExceeded.failure_fact()
        );
        assert_eq!(
            input.fact().class(),
            novarocks_execution_contract::failure::FailureClass::Dependency
        );
        assert_eq!(input.fact().reason().code(), 151);
        assert_eq!(input.fact().resource(), None);
        assert_eq!(input.fact().representation(), None);
        assert_eq!(input.fact().row_position(), None);
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(
            input.topology_requirement(),
            crate::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
    }

    #[tokio::test]
    async fn replacement_preparation_released_within_budget_can_activate() {
        exercise_held_replacement_preparation(None, true).await;
    }

    #[tokio::test]
    async fn recoverable_pre_visibility_failure_activates_a_rows_replacement() {
        let mut control = {
            let prepares = Arc::new(AtomicU64::new(0));
            let runs = Arc::new(AtomicU64::new(0));
            let convergences = Arc::new(AtomicU64::new(0));
            let allow_initial_convergence = Arc::new(tokio::sync::Notify::new());
            let attempts = RecoveringRowsPreparationPort {
                relay_window: None,
                backends: fixture_recovery_backends(2),
                prepares: Arc::clone(&prepares),
                runs: Arc::clone(&runs),
                convergences: Arc::clone(&convergences),
                allow_initial_convergence: Arc::clone(&allow_initial_convergence),
                block_initial_convergence: true,
                replacement_missing_rows: false,
                replacement_activation_panic: false,
                failures_before_success: 1,
            };
            let replacements = Arc::new(ImmediateReplacementPort::default());
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x54),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );
            let mut handle = client
                .start(
                    rows_request(
                        &control,
                        &scope,
                        RecoveryMode::RestartAttemptBeforeVisibility,
                        Some(replacements),
                        attempts,
                    ),
                    root.owner,
                )
                .await
                .unwrap();
            let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
                panic!("recoverable Rows execution must keep one result consumer");
            };
            tokio::time::timeout(Duration::from_secs(1), async {
                while runs.load(Ordering::SeqCst) < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("successor activation must not wait for old attempt convergence");
            assert_eq!(prepares.load(Ordering::SeqCst), 2);
            assert_eq!(runs.load(Ordering::SeqCst), 2);
            assert_eq!(convergences.load(Ordering::SeqCst), 0);
            stream
                .begin_schema()
                .expect("replacement Rows execution must retain the frozen schema")
                .complete();
            let crate::api::ResultDelivery::Segment(delivery) =
                tokio::time::timeout(Duration::from_secs(1), stream.next())
                    .await
                    .expect("replacement must deliver while old convergence is blocked")
                    .unwrap()
                    .unwrap()
            else {
                panic!("replacement must deliver a result batch");
            };
            delivery.complete();
            assert_eq!(convergences.load(Ordering::SeqCst), 0);
            allow_initial_convergence.notify_one();
            handle.request_cancel().unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("replacement cancellation must terminate the result stream");
            drop(stream);
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            drop(handle);
            scope.wait_released().await;
            assert_eq!(convergences.load(Ordering::SeqCst), 2);

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn shutdown_finishes_remote_tracking_for_a_lost_residual_attempt() {
        let mut control = {
            let prepares = Arc::new(AtomicU64::new(0));
            let runs = Arc::new(AtomicU64::new(0));
            let convergences = Arc::new(AtomicU64::new(0));
            let allow_initial_convergence = Arc::new(tokio::sync::Notify::new());
            let attempts = RecoveringRowsPreparationPort {
                relay_window: None,
                backends: fixture_recovery_backends(2),
                prepares,
                runs: Arc::clone(&runs),
                convergences: Arc::clone(&convergences),
                allow_initial_convergence: Arc::clone(&allow_initial_convergence),
                block_initial_convergence: true,
                replacement_missing_rows: false,
                replacement_activation_panic: false,
                failures_before_success: 1,
            };
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x55),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );
            let mut handle = client
                .start(
                    rows_request(
                        &control,
                        &scope,
                        RecoveryMode::RestartAttemptBeforeVisibility,
                        Some(Arc::new(ImmediateReplacementPort::default())),
                        attempts,
                    ),
                    root.owner,
                )
                .await
                .unwrap();
            let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
                panic!("recoverable Rows execution must keep one result consumer");
            };
            tokio::time::timeout(Duration::from_secs(1), async {
                while runs.load(Ordering::SeqCst) < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("successor activation must not wait for the lost residual attempt");
            stream.begin_schema().unwrap().complete();
            let crate::api::ResultDelivery::Segment(delivery) =
                tokio::time::timeout(Duration::from_secs(1), stream.next())
                    .await
                    .expect("successor result must remain available")
                    .unwrap()
                    .unwrap()
            else {
                panic!("successor must deliver a result batch");
            };
            delivery.complete();

            handle.request_cancel().unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("successor cancellation must terminate the result stream");
            drop(stream);
            root.business.release();

            supervisor
                .shutdown_until(Instant::now() + Duration::from_millis(10))
                .await
                .expect(
                    "shutdown must end FE tracking without waiting for a lost residual attempt",
                );
            assert_eq!(convergences.load(Ordering::SeqCst), 1);
            drop(supervisor);

            // A later worker fact cannot revive the retired FE observation.
            allow_initial_convergence.notify_one();
            // The convenience handle still owns the original exit observer
            // and its completion footer until this actual last use finishes.
            drop(handle);
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    acknowledge_all_control(&control);
                    tokio::select! {
                        () = scope.wait_released() => break,
                        () = tokio::time::sleep(Duration::from_millis(1)) => {}
                    }
                }
            })
            .await
            .expect("detached logical work must release after the late residual fact");

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn replacement_activation_rejects_a_missing_rows_runtime() {
        let mut control = {
            let prepares = Arc::new(AtomicU64::new(0));
            let runs = Arc::new(AtomicU64::new(0));
            let convergences = Arc::new(AtomicU64::new(0));
            let attempts = RecoveringRowsPreparationPort {
                relay_window: None,
                backends: fixture_recovery_backends(2),
                prepares: Arc::clone(&prepares),
                runs,
                convergences: Arc::clone(&convergences),
                allow_initial_convergence: Arc::new(tokio::sync::Notify::new()),
                block_initial_convergence: false,
                replacement_missing_rows: true,
                replacement_activation_panic: false,
                failures_before_success: 1,
            };
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x55),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );
            let mut handle = client
                .start(
                    rows_request(
                        &control,
                        &scope,
                        RecoveryMode::RestartAttemptBeforeVisibility,
                        Some(Arc::new(ImmediateReplacementPort::default())),
                        attempts,
                    ),
                    root.owner,
                )
                .await
                .unwrap();
            let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
                panic!("recoverable Rows execution must keep one result consumer");
            };
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            // Reading before schema delivery is a separate local protocol
            // refusal. Keep the first attempt invisible until this SAME
            // replacement actually reaches preparation, then observe its
            // real missing-runtime failure through the valid reader API.
            tokio::time::timeout_at(deadline, async {
                while prepares.load(Ordering::SeqCst) < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the original failed attempt must prepare its replacement");
            stream.begin_schema().unwrap().complete();
            let Err(error) = tokio::time::timeout_at(deadline, stream.next())
                .await
                .expect("invalid replacement runtime must terminate the stream")
            else {
                panic!("invalid replacement runtime must fail the stream");
            };
            assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
            let input = error
                .execution_failure()
                .expect("the output guard authors an execution contract fact");
            assert_eq!(
                input.fact(),
                QueryFailureReason::ResultProtocol.failure_fact()
            );
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                input.user_error().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
            assert_eq!(input.user_error().location(), None);
            assert_eq!(
                prepares.load(Ordering::SeqCst),
                2,
                "actual output failure: {error:?}"
            );
            drop(stream);
            root.business.release();
            assert!(matches!(
                supervisor
                    .shutdown_until(Instant::now() + Duration::from_secs(1))
                    .await,
                Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
            ));
            acknowledge_all_control(&control);
            drop(handle);
            scope.wait_released().await;
            assert_eq!(convergences.load(Ordering::SeqCst), 2);

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn replacement_activation_panic_keeps_the_original_dormant_source_unproven() {
        let mut control = {
            let prepares = Arc::new(AtomicU64::new(0));
            let runs = Arc::new(AtomicU64::new(0));
            let convergences = Arc::new(AtomicU64::new(0));
            let attempts = RecoveringRowsPreparationPort {
                relay_window: None,
                backends: fixture_recovery_backends(2),
                prepares: Arc::clone(&prepares),
                runs: Arc::clone(&runs),
                convergences: Arc::clone(&convergences),
                allow_initial_convergence: Arc::new(tokio::sync::Notify::new()),
                block_initial_convergence: false,
                replacement_missing_rows: false,
                replacement_activation_panic: true,
                failures_before_success: 1,
            };
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x58),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );
            let request = rows_request(
                &control,
                &scope,
                RecoveryMode::RestartAttemptBeforeVisibility,
                Some(Arc::new(ImmediateReplacementPort::default())),
                attempts,
            );
            let mut input = match client.prepare_observed_start(request, None) {
                Ok(original) => Some(original),
                Err(original) => std::panic::panic_any(original),
            };
            let mut owner = Some(root.owner);
            let mut ready = None;
            let mut exit = None;
            client
                .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
                .unwrap()
                .finish()
                .unwrap();
            let mut handle = ready.take().unwrap().await.unwrap();
            let mut exit = exit.take().unwrap();
            let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
                panic!("the same recoverable Rows output must remain owned");
            };
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            tokio::time::timeout_at(deadline, async {
                while prepares.load(Ordering::SeqCst) < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the original failed attempt must prepare its replacement");
            stream.begin_schema().unwrap().complete();
            let Err(error) = tokio::time::timeout_at(deadline, stream.next())
                .await
                .expect("the real replacement panic must terminate its output")
            else {
                panic!("the replacement activation cannot return a successful Rows result");
            };
            assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
            assert_eq!(
                error.message(),
                "Native replacement attempt activation panicked"
            );
            assert_eq!(
                error.execution_failure().unwrap().fact(),
                QueryFailureReason::NativeFuturePanicked.failure_fact()
            );
            wait_for_dormant_failure(&mut exit).await;
            assert_eq!(prepares.load(Ordering::SeqCst), 2);
            assert_eq!(runs.load(Ordering::SeqCst), 1);
            // One original active cleanup and one original replacement cleanup.
            assert_eq!(convergences.load(Ordering::SeqCst), 2);
            assert!(exit.view().unwrap().native_dormant_failed());
            assert!(!exit.view().unwrap().native_preparation_exited());
            let original = exit.dormant_failure().unwrap();
            assert!(original.activation_panic().is_some());
            assert!(original.convergence_panic().is_none());
            drop(original);
            assert!(futures::poll!(std::future::poll_fn(|cx| exit.poll_exit(cx))).is_pending());
            drop(stream);
            drop(handle);
            root.business.release();
            assert!(matches!(
                supervisor
                    .shutdown_until(Instant::now() + Duration::from_secs(1))
                    .await,
                Err(LogicalExecutionSupervisorShutdownError::StatementNativeUnresolved(_))
            ));
            assert!(futures::poll!(Box::pin(scope.wait_released())).is_pending());
            assert!(
                supervisor.join.is_some(),
                "the original root join remains owned"
            );
            drop(exit);
            supervisor.abandon_for_process_exit();
            control
        };
        control.finish_for_process_exit().await;
    }

    #[derive(Debug)]
    struct VisibleFailurePreparationPort {
        relay_window: Option<TestWindow>,
        backend: BackendProcessId,
        prepares: Arc<AtomicU64>,
        fail: Arc<tokio::sync::Notify>,
    }

    impl TestRowsPreparationPort for VisibleFailurePreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            self.relay_window = Some(TestWindow(window));
        }
    }

    impl NativeAttemptPreparationPort for VisibleFailurePreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            self.prepares.fetch_add(1, Ordering::SeqCst);
            let owner = VisibleFailureDormantOwner {
                relay_window: self
                    .relay_window
                    .as_ref()
                    .expect("fixture window was admitted")
                    .clone(),
                backend: self.backend,
                fail: Arc::clone(&self.fail),
            };
            request.stage_fixture_owner(owner);
            Box::pin(async move {
                let scheduling = no_scan_scheduling(request);
                request.bind_staged(scheduling).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct VisibleFailureDormantOwner {
        relay_window: TestWindow,
        backend: BackendProcessId,
        fail: Arc<tokio::sync::Notify>,
    }

    impl DormantNativeAttemptOwner for VisibleFailureDormantOwner {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let root = schedule.root();
            let fail = Arc::clone(&self.fail);
            let window = self.relay_window.clone();
            Box::pin(async move {
                let (status_sender, statuses) =
                    super::super::accepted_root_status_projection_with_control_port(
                        root,
                        Arc::new(ClosedSuccessSealPort),
                    );
                status_sender
                    .publish_attempt_observation(
                        running_status(root),
                        super::super::AcceptedAttemptFailure::None,
                    )
                    .unwrap();
                let binding = test_root_binding(
                    root,
                    window.0,
                    Some(&[
                        10, 0, 0, 0, 1, b'1', 7, b'v', b'i', b's', b'i', b'b', b'l', b'e',
                    ]),
                );
                Ok(ActivatedNativeAttempt::relayed_rows(
                    VisibleFailureActiveOwner {
                        backend: root.backend_process_id(),
                        fail,
                        _status_sender: status_sender,
                    },
                    binding,
                    statuses,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    struct VisibleFailureActiveOwner {
        backend: BackendProcessId,
        fail: Arc<tokio::sync::Notify>,
        _status_sender: super::super::AcceptedRootStatusSender,
    }

    impl fmt::Debug for VisibleFailureActiveOwner {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("VisibleFailureActiveOwner")
        }
    }

    impl ActiveNativeAttemptOwner for VisibleFailureActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                self.fail.notified().await;
                NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                    ExecutionFailureInput::new(
                        QueryFailureReason::BackendLost.failure_fact(),
                        SafeDetail::truncating("injected failure after visible output"),
                        ExecutionFailureSite::Backend {
                            process: self.backend,
                        },
                    ),
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async {
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    #[tokio::test]
    async fn visible_rows_failure_does_not_prepare_a_replacement() {
        let mut control = {
            let prepares = Arc::new(AtomicU64::new(0));
            let fail = Arc::new(tokio::sync::Notify::new());
            let attempts = VisibleFailurePreparationPort {
                relay_window: None,
                backend: BackendProcessId::new_v7(),
                prepares: Arc::clone(&prepares),
                fail: Arc::clone(&fail),
            };
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x56),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );
            let mut handle = client
                .start(
                    rows_request(
                        &control,
                        &scope,
                        RecoveryMode::RestartAttemptBeforeVisibility,
                        Some(Arc::new(ImmediateReplacementPort::default())),
                        attempts,
                    ),
                    root.owner,
                )
                .await
                .unwrap();
            let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
                panic!("Rows execution must return one result stream");
            };
            stream
                .begin_schema()
                .expect("Rows execution must begin with its frozen result schema")
                .complete();
            let crate::api::ResultDelivery::Segment(delivery) =
                stream.next().await.unwrap().unwrap()
            else {
                panic!("the first delivery must be a result batch");
            };
            delivery.complete();
            fail.notify_one();
            let Err(error) = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("visible failure must terminate the stream")
            else {
                panic!("visible failure must be caller-visible");
            };
            assert_eq!(error.message(), "injected failure after visible output");
            assert_eq!(prepares.load(Ordering::SeqCst), 1);
            drop(stream);
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            drop(handle);
            scope.wait_released().await;

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[derive(Debug)]
    struct PanickingAttemptPreparationPort {
        backend: BackendProcessId,
        phase: AttemptPanicPhase,
        convergence_calls: Arc<AtomicU64>,
    }

    impl TestRowsPreparationPort for PanickingAttemptPreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            drop(window);
        }
    }

    impl NativeAttemptPreparationPort for PanickingAttemptPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let backend = self.backend;
            let owner = PanickingDormantOwner {
                backend,
                phase: self.phase,
                convergence_calls: Arc::clone(&self.convergence_calls),
            };
            request.stage_fixture_owner(owner);
            Box::pin(async move {
                let scheduling = no_scan_scheduling(request);
                request.bind_staged(scheduling).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct PanickingDormantOwner {
        backend: BackendProcessId,
        phase: AttemptPanicPhase,
        convergence_calls: Arc<AtomicU64>,
    }

    impl DormantNativeAttemptOwner for PanickingDormantOwner {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            match self.phase {
                AttemptPanicPhase::Activate => {
                    Box::pin(async { panic!("injected Native activation panic") })
                }
                AttemptPanicPhase::Run | AttemptPanicPhase::Converge => {
                    let convergence_calls = Arc::clone(&self.convergence_calls);
                    let phase = self.phase;
                    Box::pin(async move {
                        Ok(ActivatedNativeAttempt::completion(PanickingActiveOwner {
                            phase,
                            convergence_calls,
                        }))
                    })
                }
            }
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.convergence_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    #[derive(Debug)]
    struct PanickingActiveOwner {
        phase: AttemptPanicPhase,
        convergence_calls: Arc<AtomicU64>,
    }

    impl ActiveNativeAttemptOwner for PanickingActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            match self.phase {
                AttemptPanicPhase::Run => Box::pin(OriginalRunPanicFuture {
                    primary_drops: Arc::new(AtomicU64::new(0)),
                    cleanup_drops: Arc::new(AtomicU64::new(0)),
                }),
                AttemptPanicPhase::Converge => Box::pin(async {
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                        fixture_execution_input("injected Native attempt failure"),
                    ))
                }),
                AttemptPanicPhase::Activate => unreachable!("activation panic has no active owner"),
            }
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                let previous = self.convergence_calls.fetch_add(1, Ordering::SeqCst);
                if self.phase == AttemptPanicPhase::Converge && previous == 0 {
                    panic!("injected Native convergence panic");
                }
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    #[derive(Debug)]
    struct OriginalRunPanicPayload(Arc<AtomicU64>);
    impl Drop for OriginalRunPanicPayload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct OriginalRunPanicFuture {
        primary_drops: Arc<AtomicU64>,
        cleanup_drops: Arc<AtomicU64>,
    }
    impl Future for OriginalRunPanicFuture {
        type Output = NativeAttemptTerminal;
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::panic::panic_any(OriginalRunPanicPayload(Arc::clone(&self.primary_drops)));
        }
    }
    impl Drop for OriginalRunPanicFuture {
        fn drop(&mut self) {
            std::panic::panic_any(OriginalRunPanicPayload(Arc::clone(&self.cleanup_drops)));
        }
    }

    async fn assert_attempt_panic_converges(
        phase: AttemptPanicPhase,
        namespace: u64,
        expected_message: &'static str,
    ) {
        let mut control = {
            let convergence_calls = Arc::new(AtomicU64::new(0));
            let attempts = PanickingAttemptPreparationPort {
                backend: BackendProcessId::new_v7(),
                phase,
                convergence_calls: Arc::clone(&convergence_calls),
            };
            let (control, root) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(namespace),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let scope = root.owner.scope();

            let mut original_exit = None;
            let future = if phase != AttemptPanicPhase::Converge {
                let mut input =
                    match client.prepare_observed_start(completion_request(attempts), None) {
                        Ok(input) => Some(input),
                        Err(original) => std::panic::panic_any(original),
                    };
                let mut owner = Some(root.owner);
                let mut ready = None;
                client
                    .start_observed_in(&mut input, &mut owner, &mut ready, &mut original_exit)
                    .unwrap()
                    .finish()
                    .unwrap();
                ready.take().unwrap()
            } else {
                client.start(completion_request(attempts), root.owner)
            };
            let error = match future.await {
                Ok(_) => panic!("a panicking Native attempt must not return an execution handle"),
                Err(error) => error,
            };
            assert_eq!(error.message(), expected_message);
            let input = error
                .execution_failure()
                .expect("real Native future panic retains its input");
            assert_eq!(
                input.fact(),
                QueryFailureReason::NativeFuturePanicked.failure_fact()
            );
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(input.attempt_class(), AttemptFailureClass::ExecutionFailure);
            root.business.release();
            let shutdown = supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await;
            if phase == AttemptPanicPhase::Activate {
                assert!(matches!(
                    shutdown,
                    Err(LogicalExecutionSupervisorShutdownError::Registry(
                        LogicalExecutionRuntimeRegistryError::OriginalExitFailureUnresolved,
                    ))
                ));
                let mut exit = original_exit.take().expect("same original observed start");
                wait_for_dormant_failure(&mut exit).await;
                assert!(exit.view().unwrap().native_dormant_failed());
                assert!(!exit.view().unwrap().native_preparation_exited());
                assert!(exit.dormant_failure().unwrap().activation_panic().is_some());
                assert!(futures::poll!(std::future::poll_fn(|cx| exit.poll_exit(cx))).is_pending());
                assert!(futures::poll!(Box::pin(scope.wait_released())).is_pending());
                assert_eq!(control.snapshot().root_responsibilities, 1);
                assert!(
                    supervisor.join.is_none(),
                    "the original root actually joined"
                );
                assert!(supervisor.run_outcome.is_some());
                assert!(!supervisor.shutdown_complete);
                drop(exit);
                supervisor.abandon_for_process_exit();
            } else if phase == AttemptPanicPhase::Run {
                assert!(
                    matches!(
                        shutdown,
                        Err(LogicalExecutionSupervisorShutdownError::Registry(
                            LogicalExecutionRuntimeRegistryError::OriginalExitFailureUnresolved,
                        ))
                    ),
                    "original run shutdown: {shutdown:?}"
                );
                let exit = original_exit.take().expect("same original run Source");
                let view = exit.view().unwrap();
                let primary = Arc::clone(
                    &view
                        .native_run_panic()
                        .unwrap()
                        .downcast_ref::<OriginalRunPanicPayload>()
                        .unwrap()
                        .0,
                );
                let cleanup = Arc::clone(
                    &view
                        .native_run_cleanup_panic()
                        .unwrap()
                        .downcast_ref::<OriginalRunPanicPayload>()
                        .unwrap()
                        .0,
                );
                assert_eq!(primary.load(Ordering::SeqCst), 0);
                assert_eq!(cleanup.load(Ordering::SeqCst), 0);
                assert!(view.native_preparation_exited());
                assert_eq!(view.pending_native_returns(), 0);
                assert!(!view.native_dormant_failed());
                assert!(!view.role_failed());
                drop(view);
                assert_eq!(supervisor.registry.active_native_custody_count(), 0);
                assert!(futures::poll!(Box::pin(scope.wait_released())).is_pending());
                drop(exit);
                supervisor.abandon_for_process_exit();
                assert_eq!(primary.load(Ordering::SeqCst), 1);
                assert_eq!(cleanup.load(Ordering::SeqCst), 1);
            } else {
                assert!(matches!(
                    shutdown,
                    Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
                ));
                acknowledge_all_control(&control);
                scope.wait_released().await;
                assert_eq!(control.snapshot().root_responsibilities, 0);
            }
            assert_eq!(convergence_calls.load(Ordering::SeqCst), 1);

            control
        };
        if phase != AttemptPanicPhase::Converge {
            control.finish_for_process_exit().await;
        } else {
            acknowledge_all_control(&control);
            control.finish().await;
        }
    }

    #[tokio::test]
    async fn activation_future_panic_retains_owner_for_convergence() {
        assert_attempt_panic_converges(
            AttemptPanicPhase::Activate,
            0x4b,
            "Native attempt activation panicked",
        )
        .await;
    }

    #[tokio::test]
    async fn run_future_panic_retains_owner_for_convergence() {
        assert_attempt_panic_converges(AttemptPanicPhase::Run, 0x4c, "Native attempt run panicked")
            .await;
    }

    #[tokio::test]
    async fn rows_activation_rejects_a_missing_root_result_runtime() {
        let mut control = {
            let convergence_calls = Arc::new(AtomicU64::new(0));
            let attempts = PanickingAttemptPreparationPort {
                backend: BackendProcessId::new_v7(),
                phase: AttemptPanicPhase::Run,
                convergence_calls: Arc::clone(&convergence_calls),
            };
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x53),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let error = match client
                .start(
                    rows_request(&control, &scope, RecoveryMode::NoRecovery, None, attempts),
                    root.owner,
                )
                .await
            {
                Ok(_) => panic!("Rows activation must transfer its exact pump runtime"),
                Err(error) => error,
            };
            assert_eq!(
                error.kind(),
                QueryExecutionErrorKind::Failed,
                "actual output failure: {error:?}"
            );
            let input = error
                .execution_failure()
                .expect("the output guard authors an execution contract fact");
            assert_eq!(
                input.fact(),
                QueryFailureReason::ResultProtocol.failure_fact()
            );
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                input.user_error().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
            assert_eq!(input.user_error().location(), None);
            assert_eq!(
                error.message(),
                "Native activated a row execution without its root result runtime"
            );
            root.business.release();
            assert!(matches!(
                supervisor
                    .shutdown_until(Instant::now() + Duration::from_secs(1))
                    .await,
                Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
            ));
            acknowledge_all_control(&control);
            scope.wait_released().await;
            assert_eq!(convergence_calls.load(Ordering::SeqCst), 1);

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn convergence_poll_panic_retains_same_owner_and_health_until_process_exit() {
        let mut control = {
            let convergence_calls = Arc::new(AtomicU64::new(0));
            let attempts = PanickingAttemptPreparationPort {
                backend: BackendProcessId::new_v7(),
                phase: AttemptPanicPhase::Converge,
                convergence_calls: Arc::clone(&convergence_calls),
            };
            let (control, root) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x50),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let scope = root.owner.scope();

            let error = match client.start(completion_request(attempts), root.owner).await {
                Ok(_) => panic!("a failed Native attempt must not return an execution handle"),
                Err(error) => error,
            };
            assert_eq!(error.message(), "injected Native attempt failure");
            let input = error
                .execution_failure()
                .expect("original injected failure is preserved");
            assert_eq!(
                input.fact(),
                FixtureFailureReason::InjectedExecution.failure_fact()
            );
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(input.attempt_class(), AttemptFailureClass::ExecutionFailure);
            root.business.release();
            let shutdown = supervisor
                .shutdown_until(Instant::now() + Duration::from_millis(10))
                .await;
            assert!(matches!(
                shutdown,
                Err(LogicalExecutionSupervisorShutdownError::StatementNativeUnresolved(_))
            ));
            assert_eq!(
                convergence_calls.load(Ordering::SeqCst),
                1,
                "unproven convergence is never re-polled into fabricated closure"
            );
            assert_eq!(supervisor.registry.active_native_custody_count(), 1);
            assert_eq!(control.snapshot().root_responsibilities, 1);
            assert!(
                scope.cancellation().is_ok(),
                "original actor scope remains present before actual exit"
            );
            supervisor.abandon_for_process_exit();

            control
        };
        control.finish_for_process_exit().await;
    }

    #[derive(Debug)]
    struct TestContent(u8);

    impl CodecOwnedContent for TestContent {
        fn fingerprint(&self) -> ContentFingerprint {
            ContentFingerprint::from_bytes([self.0; 16])
        }

        fn encoded_len(&self) -> usize {
            1
        }
    }

    #[derive(Debug)]
    struct TestSecret;

    impl ConfidentialContent for TestSecret {
        fn encoded_len(&self) -> usize {
            1
        }

        fn matches(&self, other: &dyn ConfidentialContent) -> bool {
            other.encoded_len() == 1
        }
    }

    #[derive(Debug)]
    struct ImmediateEstablishTransport;

    impl EstablishTransportSink for ImmediateEstablishTransport {
        fn try_reserve(&self, _identity: EstablishIssueIdentity) -> EstablishTransportAdmission {
            EstablishTransportAdmission::Admitted(Box::new(ImmediateEstablishReservation))
        }
    }

    #[derive(Debug)]
    struct ImmediateEstablishReservation;

    impl EstablishTransportReservation for ImmediateEstablishReservation {
        fn submit(self: Box<Self>, submission: EstablishTransportSubmission) {
            submission
                .worker_settled(OperationOutcome::Accepted)
                .expect("test transport publishes exact Establish success");
        }
    }

    #[derive(Debug, Default)]
    struct ActivationCancellationSignals {
        entered: AtomicBool,
        release_activation: tokio::sync::Notify,
        convergence_entered: AtomicBool,
        release_convergence: tokio::sync::Notify,
        converged: AtomicBool,
    }

    #[derive(Debug)]
    struct ActivationCancellationPreparationPort {
        backend: BackendProcessId,
        frontend: FrontendProcessId,
        signals: Arc<ActivationCancellationSignals>,
    }

    impl NativeAttemptPreparationPort for ActivationCancellationPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let dormant = ActivationCancellationDormant {
                inner: SuccessfulCompletionDormantOwner {
                    backend: self.backend,
                    expected_frontend: self.frontend,
                },
                signals: Arc::clone(&self.signals),
            };
            request.stage_fixture_owner(dormant);
            Box::pin(async move {
                let scheduling = no_scan_scheduling(request);
                request.bind_staged(scheduling).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct ActivationCancellationDormant {
        inner: SuccessfulCompletionDormantOwner,
        signals: Arc<ActivationCancellationSignals>,
    }

    impl DormantNativeAttemptOwner for ActivationCancellationDormant {
        fn poll_bind_qualification(
            &mut self,
            schedule: &crate::coordination::AttemptSchedule,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            self.inner.poll_bind_qualification(schedule, cx)
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            self.inner.eligible_backends()
        }
        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let signals = Arc::clone(&self.signals);
            let activation = self.inner.activate(schedule, admissions, cancellation);
            Box::pin(async move {
                signals.entered.store(true, Ordering::SeqCst);
                signals.release_activation.notified().await;
                let activated = activation.await?;
                let (inner, rows) = activated.into_parts();
                assert!(rows.is_none());
                Ok(ActivatedNativeAttempt::completion(
                    ActivationCancellationActive { inner, signals },
                ))
            })
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            self.inner.converge(cancellation)
        }
    }

    #[derive(Debug)]
    struct ActivationCancellationActive {
        inner: Box<dyn ActiveNativeAttemptOwner>,
        signals: Arc<ActivationCancellationSignals>,
    }

    impl ActiveNativeAttemptOwner for ActivationCancellationActive {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { panic!("an already concluded activation must never run") })
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            let signals = Arc::clone(&self.signals);
            Box::pin(async move {
                signals.convergence_entered.store(true, Ordering::SeqCst);
                signals.release_convergence.notified().await;
                let convergence = self.inner.converge(cancellation).await;
                signals.converged.store(true, Ordering::SeqCst);
                convergence
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn accepted_cancel_during_native_activation_retires_the_original_owner_without_poisoning_supervisor()
     {
        for reason in [
            CancellationReason::Requested,
            CancellationReason::DeadlineExceeded,
        ] {
            let mut control = {
                let frontend = FrontendProcessId::new_v7();
                let signals = Arc::new(ActivationCancellationSignals::default());
                let (control, root) = governance().await;
                let scope = root.owner.scope();
                let requester = root.owner.cancellation_requester();
                let (mut supervisor, client) = test_supervisor(
                    &control,
                    Handle::current(),
                    Arc::new(BindingNativePort),
                    QueryProcessNamespace::new(0x771),
                    frontend,
                    supervisor_config(),
                );
                let start = client.start(
                    completion_request(ActivationCancellationPreparationPort {
                        backend: BackendProcessId::new_v7(),
                        frontend,
                        signals: Arc::clone(&signals),
                    }),
                    root.owner,
                );
                wait_preparation_exit_fact(&signals.entered).await.unwrap();
                requester.request(reason.clone()).unwrap();
                let error = match start.await {
                    Ok(_) => panic!("cancelled start cannot become success"),
                    Err(error) => error,
                };
                assert_eq!(
                    error.kind(),
                    if matches!(reason, CancellationReason::DeadlineExceeded) {
                        QueryExecutionErrorKind::DeadlineExceeded
                    } else {
                        QueryExecutionErrorKind::Cancelled
                    }
                );
                signals.release_activation.notify_one();
                wait_preparation_exit_fact(&signals.convergence_entered)
                    .await
                    .unwrap();
                assert!(!signals.converged.load(Ordering::SeqCst));
                assert_ne!(control.snapshot().root_responsibilities, 0);
                // A terminal query does not poison the process or block a sibling start.
                let sibling_control = &control;
                let sibling = control.begin_root().await;
                let sibling_scope = sibling.owner.scope();
                let mut sibling_handle = client
                    .start(
                        completion_request(SuccessfulCompletionPreparationPort {
                            backend: BackendProcessId::new_v7(),
                            expected_frontend: frontend,
                        }),
                        sibling.owner,
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    sibling_handle.take_output(),
                    Some(crate::api::ExecutionOutput::Completion)
                ));
                sibling.business.release();
                signals.release_convergence.notify_one();
                wait_preparation_exit_fact(&signals.converged)
                    .await
                    .unwrap();
                root.business.release();
                supervisor
                    .shutdown_until(Instant::now() + Duration::from_secs(2))
                    .await
                    .unwrap();
                acknowledge_all_control(&control);
                acknowledge_all_control(sibling_control);
                drop(sibling_handle);
                scope.wait_released().await;
                sibling_scope.wait_released().await;

                control
            };
            acknowledge_all_control(&control);
            control.finish().await;
        }
    }

    #[derive(Debug)]
    struct SuccessfulCompletionPreparationPort {
        backend: BackendProcessId,
        expected_frontend: FrontendProcessId,
    }

    impl NativeAttemptPreparationPort for SuccessfulCompletionPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let backend = self.backend;
            let expected_frontend = self.expected_frontend;
            request.stage_fixture_owner(SuccessfulCompletionDormantOwner {
                backend,
                expected_frontend,
            });
            Box::pin(async move {
                let scheduling = no_scan_scheduling(request);
                request.bind_staged(scheduling).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct SuccessfulCompletionDormantOwner {
        backend: BackendProcessId,
        expected_frontend: FrontendProcessId,
    }

    impl DormantNativeAttemptOwner for SuccessfulCompletionDormantOwner {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            assert!(
                schedule
                    .contexts()
                    .iter()
                    .all(|context| context.frontend_process_id() == self.expected_frontend)
            );
            let contexts = schedule.contexts().to_vec();
            Box::pin(async move {
                Ok(ActivatedNativeAttempt::completion(
                    SuccessfulCompletionActiveOwner { contexts },
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Debug)]
    struct SuccessfulCompletionActiveOwner {
        contexts: Vec<QueryContextRef>,
    }

    impl ActiveNativeAttemptOwner for SuccessfulCompletionActiveOwner {
        fn run<'a>(
            &'a mut self,
            drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                let valid_for = LeaseValidFor::new(Duration::from_secs(30)).unwrap();
                for (index, &context) in self.contexts.iter().enumerate() {
                    let tag = u8::try_from(index + 1).unwrap();
                    let compatibility = NativeCompatibilityId::new([tag; 32]);
                    let issue = drive
                        .begin_admission_issue(
                            novarocks_execution_contract::AcquireQueryContextAdmissionTicket::new(
                                TaskOperationId::new_v7(),
                                context,
                                valid_for,
                                compatibility,
                                AdmissionEpochCapability::try_from_bytes([tag; 16]).unwrap(),
                            ),
                        )
                        .await
                        .unwrap();
                    let ticket = QueryContextAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([tag; 16]).unwrap(),
                        context,
                        valid_for,
                    );
                    drive
                        .settle_admission_issue(
                            issue,
                            AdmissionIssueSettlement::applied(
                                issue.operation_id(),
                                OperationOutcome::Accepted,
                                ticket,
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    let content = |offset: u8| {
                        Arc::new(TestContent(tag + offset)) as Arc<dyn CodecOwnedContent>
                    };
                    let establish = Arc::new(EstablishQueryContext::new(
                        TaskOperationId::new_v7(),
                        context,
                        ticket.ticket_id(),
                        content(0),
                        content(1),
                        content(2),
                        CredentialUpdate::new(
                            CredentialLeaseId::new(u64::from(tag)),
                            CredentialEpoch::new(1).unwrap(),
                            Arc::new(TestSecret),
                        ),
                        valid_for,
                    ));
                    let permit = drive
                        .authorize_establish(establish, compatibility)
                        .await
                        .unwrap();
                    assert_eq!(
                        permit.try_submit(&ImmediateEstablishTransport).unwrap(),
                        EstablishIssueSubmit::Accepted
                    );
                }
                tokio::task::yield_now().await;
                NativeAttemptTerminal::Completed
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async {
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    #[tokio::test]
    async fn completion_becomes_visible_only_after_exact_establish_success() {
        let mut control = {
            let frontend = FrontendProcessId::new_v7();
            let attempts = SuccessfulCompletionPreparationPort {
                backend: BackendProcessId::new_v7(),
                expected_frontend: frontend,
            };
            let (control, root) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x50),
                frontend,
                supervisor_config(),
            );

            let scope = root.owner.scope();
            let mut handle = client
                .start(completion_request(attempts), root.owner)
                .await
                .expect("exact Establish success permits completion visibility");
            assert!(matches!(
                handle.take_output(),
                Some(crate::api::ExecutionOutput::Completion)
            ));
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            drop(handle);
            scope.wait_released().await;

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[derive(Debug)]
    struct PrematureCompletionPreparationPort {
        backend: BackendProcessId,
    }

    impl NativeAttemptPreparationPort for PrematureCompletionPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let backend = self.backend;
            request.stage_fixture_owner(PrematureCompletionDormant { backend });
            Box::pin(async move {
                let scheduling = no_scan_scheduling(request);
                request.bind_staged(scheduling).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct PrematureCompletionDormant {
        backend: BackendProcessId,
    }

    impl DormantNativeAttemptOwner for PrematureCompletionDormant {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            Box::pin(async {
                Ok(ActivatedNativeAttempt::completion(
                    PrematureCompletionActive,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Debug)]
    struct PrematureCompletionActive;

    impl ActiveNativeAttemptOwner for PrematureCompletionActive {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { NativeAttemptTerminal::Completed })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async {
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    #[tokio::test]
    async fn completion_is_not_visible_without_exact_establish_success() {
        let mut control = {
            let attempts = PrematureCompletionPreparationPort {
                backend: BackendProcessId::new_v7(),
            };
            let (control, root) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x4a),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let scope = root.owner.scope();
            let error = match client.start(completion_request(attempts), root.owner).await {
                Ok(_) => panic!("Native completion without Establish proof must fail closed"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
            root.business.release();
            let shutdown = supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await;
            assert!(matches!(
                shutdown,
                Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
            ));
            acknowledge_all_control(&control);
            scope.wait_released().await;

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn start_handoff_has_no_second_capacity_and_closed_supervisor_rejects_work() {
        let control = governance_empty();
        let mut control = {
            let seed_drops = Arc::new(AtomicU64::new(0));
            let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
            let (mut bank, shutdown, mut registry, client) = unpolled_test_driver(
                &control,
                Arc::new(RecordingRejectedOpen {
                    opened: Arc::clone(&opened),
                }),
            );
            let first = control.begin_root().await;
            let second = control.begin_root().await;
            // Reserve this real owner before the bank's global closing cut.
            let closed = control.begin_root().await;
            let first_scope = first.owner.scope();
            let second_scope = second.owner.scope();
            let closed_scope = closed.owner.scope();
            let second_cancellation = second_scope.cancellation().unwrap();
            let closed_cancellation = closed_scope.cancellation().unwrap();
            let first_start = client.start(drop_tracked_request(&seed_drops), first.owner);
            let second_start = client.start(drop_tracked_request(&seed_drops), second.owner);
            assert!(
                control.next_control().is_none(),
                "handoff must not manufacture cancellation work"
            );
            assert_eq!(second_cancellation.reason(), None);
            assert!(
                opened.lock().unwrap().is_empty(),
                "queued ordinary starts do not consume identity or open Native before their factory"
            );
            assert_eq!(
                seed_drops.load(Ordering::SeqCst),
                0,
                "original accepted requests remain in the actual fixed inputs"
            );
            shutdown.send_replace(true);
            bank.stop_ingress();
            // The original factory supplies the old accepted-but-not-started
            // rejection; no manual extraction/completion of an owned Work.
            for _ in 0..2 {
                let completion = bank.progress_turn(NonZeroUsize::new(7).unwrap()).unwrap();
                let (_source, completion) = completion.into_parts();
                assert!(
                    matches!(&completion, PollCompletion::Ready(output) if output.value().is_ok())
                );
                drop(completion);
            }
            let first_error = first_start
                .await
                .err()
                .expect("injected handoff reply must fail");
            let second_error = second_start
                .await
                .err()
                .expect("injected handoff reply must fail");
            assert_eq!(first_error.kind(), QueryExecutionErrorKind::Rejected);
            assert_eq!(second_error.kind(), QueryExecutionErrorKind::Rejected);
            assert_eq!(seed_drops.load(Ordering::SeqCst), 2);
            assert!(
                opened.lock().unwrap().is_empty(),
                "shutdown drain has no Native effect"
            );
            first.business.release();
            second.business.release();
            first_scope.wait_released().await;
            second_scope.wait_released().await;
            let closed_error = client
                .start(drop_tracked_request(&seed_drops), closed.owner)
                .await
                .err()
                .expect("a closed supervisor must reject new work");
            assert_eq!(closed_error.kind(), QueryExecutionErrorKind::Rejected);
            assert!(
                control.next_control().is_none(),
                "closed admission rejection must not manufacture cancellation work"
            );
            assert_eq!(closed_cancellation.reason(), None);
            assert_eq!(seed_drops.load(Ordering::SeqCst), 3);
            closed.business.release();
            closed_scope.wait_released().await;
            drop(bank);
            registry
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[derive(Debug)]
    struct PanickingOpenNativePort;

    impl LogicalExecutionNativePort for PanickingOpenNativePort {
        fn bind_in(
            &self,
            _source: &mut Option<LogicalNativeOpenRequest>,
            _target: &mut Option<LogicalNativeSession>,
        ) -> Result<(), LogicalNativeOpenError> {
            panic!("injected Native bind panic")
        }
    }

    #[derive(Debug)]
    struct IsolatedTaskPanicNativePort {
        opens: AtomicU64,
        first_opened: Arc<AtomicBool>,
        second_opened: Arc<AtomicBool>,
    }

    impl LogicalExecutionNativePort for IsolatedTaskPanicNativePort {
        fn bind_in(
            &self,
            source: &mut Option<LogicalNativeOpenRequest>,
            target: &mut Option<LogicalNativeSession>,
        ) -> Result<(), LogicalNativeOpenError> {
            if self.opens.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first_opened.store(true, Ordering::SeqCst);
                panic!("injected isolated logical bind panic");
            }
            self.second_opened.store(true, Ordering::SeqCst);
            LogicalNativeOpenRequest::bind_in(source, target).map_err(Into::into)
        }
    }

    #[tokio::test]
    async fn one_logical_task_panic_does_not_cancel_or_stop_another_execution() {
        let mut first_control = {
            let frontend = FrontendProcessId::new_v7();
            let first_opened = Arc::new(AtomicBool::new(false));
            let second_opened = Arc::new(AtomicBool::new(false));
            let second_gate = Arc::new(tokio::sync::Notify::new());
            let native = Arc::new(IsolatedTaskPanicNativePort {
                opens: AtomicU64::new(0),
                first_opened: Arc::clone(&first_opened),
                second_opened: Arc::clone(&second_opened),
            });
            let (first_control, first) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &first_control,
                Handle::current(),
                native,
                QueryProcessNamespace::new(0x51),
                frontend,
                supervisor_config(),
            );

            let first_scope = first.owner.scope();
            let first_cancellation = first_scope.cancellation().unwrap();
            let first_result =
                client.start(completion_request(UnreachablePreparationPort), first.owner);
            tokio::time::timeout(Duration::from_secs(1), async {
                while !first_opened.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the first logical task reached its isolated panic");

            let second_control = &first_control;
            let second = first_control.begin_root().await;
            let second_scope = second.owner.scope();
            let second_cancellation = second_scope.cancellation().unwrap();
            let second_result = client.start(
                completion_request(OriginalPreparationGate::release(
                    SuccessfulCompletionPreparationPort {
                        backend: BackendProcessId::new_v7(),
                        expected_frontend: frontend,
                    },
                    Arc::clone(&second_gate),
                )),
                second.owner,
            );

            let first_error = match first_result.await {
                Ok(_) => panic!("the panicking logical task must fail its own request"),
                Err(error) => error,
            };
            assert_eq!(first_error.kind(), QueryExecutionErrorKind::Failed);
            tokio::time::timeout(Duration::from_secs(1), async {
                while !second_opened.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the sibling logical task remains admitted after the panic");
            assert_eq!(first_cancellation.reason(), None);
            assert_eq!(second_cancellation.reason(), None);
            assert!(first_control.next_control().is_none());
            assert!(second_control.next_control().is_none());

            second_gate.notify_one();
            let mut second_handle = second_result
                .await
                .expect("a sibling execution remains supervised after the panic");
            assert!(matches!(
                second_handle.take_output(),
                Some(crate::api::ExecutionOutput::Completion)
            ));
            assert_eq!(first_cancellation.reason(), None);
            assert!(first_control.next_control().is_none());

            first.business.release();
            second.business.release();
            assert!(matches!(
                supervisor
                    .shutdown_until(Instant::now() + Duration::from_secs(1))
                    .await,
                Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
            ));
            assert_eq!(first_cancellation.reason(), None);
            assert_ne!(
                first_control.snapshot().root_responsibilities,
                0,
                "the real whole health packet still retains its original footer"
            );
            supervisor.abandon_for_process_exit();
            acknowledge_all_control(&first_control);
            acknowledge_all_control(second_control);
            drop(second_handle);
            first_scope.wait_released().await;
            second_scope.wait_released().await;

            first_control
        };
        acknowledge_all_control(&first_control);
        first_control.finish().await;
    }

    #[tokio::test]
    async fn process_source_health_is_observed_before_the_original_service_exits() {
        #[derive(Debug)]
        struct OriginalCause(Arc<AtomicU64>);
        impl std::fmt::Display for OriginalCause {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("original process source failure")
            }
        }
        impl std::error::Error for OriginalCause {}
        impl Drop for OriginalCause {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let mut control = governance_empty();
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x61),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        assert!(supervisor.process_failure().is_none());
        let drops = Arc::new(AtomicU64::new(0));
        let cause = Arc::new(OriginalCause(drops.clone()));
        let original = Arc::downgrade(&cause);
        client.report_process_failure(
            crate::api::StatementNativeHealthFailure::actual_exit_unproven(
                "original background source failed",
                cause,
            ),
        );
        // The production driver has already accepted the same cause into the
        // original health cell. No logical Work or Native identity is minted.
        let observed = supervisor.process_failure().is_some();
        tokio::task::yield_now().await;
        let still_live = !supervisor.service_has_exited();
        let retained = original.upgrade().is_some() && drops.load(Ordering::SeqCst) == 0;
        let roots = control.snapshot().root_responsibilities;
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        supervisor.abandon_for_process_exit();
        drop(client);
        drop(supervisor);
        acknowledge_all_control(&control);
        control.finish().await;
        assert!(observed && still_live && retained);
        assert_eq!(roots, 0);
        assert!(matches!(
            shutdown,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn logical_task_panic_completes_uninstalled_work_and_drains_supervisor() {
        let mut control = {
            let (control, root) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(PanickingOpenNativePort),
                QueryProcessNamespace::new(0x49),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let scope = root.owner.scope();
            let start = client.start(completion_request(UnreachablePreparationPort), root.owner);
            let error = match start.await {
                Ok(_) => panic!("panicking Native open must not return an execution handle"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
            root.business.release();
            let shutdown = supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await;
            assert!(matches!(
                shutdown,
                Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
            ));
            assert_ne!(
                control.snapshot().root_responsibilities,
                0,
                "the real whole health packet still retains its original footer"
            );
            supervisor.abandon_for_process_exit();
            acknowledge_all_control(&control);
            scope.wait_released().await;
            assert_eq!(control.snapshot().root_responsibilities, 0);

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn deadline_before_actor_install_settles_its_cancel_control() {
        let mut control = {
            let control = governance_empty();
            let root = control
                .begin_root_request(WorkRequest {
                    class: WorkClass::Query,
                    deadline: Some((Instant::now() + Duration::from_millis(25)).into()),
                })
                .await;
            let scope = root.owner.scope();
            let entered = Arc::new(tokio::sync::Notify::new());
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x4a),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let start = client.start(
                completion_request(OriginalPreparationGate::pending(
                    UnreachablePreparationPort,
                    Arc::clone(&entered),
                )),
                root.owner,
            );
            tokio::time::timeout(Duration::from_secs(1), entered.notified())
                .await
                .expect("Native open did not begin");
            tokio::time::sleep(Duration::from_millis(30)).await;
            control.expire_deadlines();

            let start = tokio::time::timeout(Duration::from_secs(1), start)
                .await
                .expect("deadline must stop a pre-install Native open");
            let Err(error) = start else {
                panic!("deadline must not return an execution handle");
            };
            assert_eq!(
                error.kind(),
                QueryExecutionErrorKind::DeadlineExceeded,
                "{}",
                error.message()
            );
            let input = error
                .execution_failure()
                .expect("pre-install deadline retains canonical input");
            assert_eq!(
                input.fact(),
                QueryFailureReason::DeadlineExceeded.failure_fact()
            );
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                scope.cancellation().unwrap().reason(),
                Some(CancellationReason::DeadlineExceeded)
            );
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            scope.wait_released().await;
            assert_eq!(control.snapshot().root_responsibilities, 0);
            assert!(
                control.next_control().is_none(),
                "the terminal pre-install failure must settle its cancellation control"
            );

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn shutdown_deadline_keeps_the_same_supervisor_join_for_retry() {
        let mut control = {
            let (control, root) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x47),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let scope = root.owner.scope();
            let start = client.start(
                completion_request(OriginalPreparationGate::delayed_cancel(
                    UnreachablePreparationPort,
                )),
                root.owner,
            );
            tokio::task::yield_now().await;

            assert_eq!(
                supervisor.shutdown_until(Instant::now()).await,
                Err(LogicalExecutionSupervisorShutdownError::DeadlineExceeded)
            );
            let error = match start.await {
                Ok(_) => panic!("shutdown during Native open must not return an execution handle"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            scope.wait_released().await;

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[test]
    fn query_sequence_is_not_reused_by_a_rebuilt_supervisor_allocator() {
        let namespace = QueryProcessNamespace::new(0x4e);
        let first = ProcessQueryIdAllocator::new(namespace)
            .first_execution()
            .unwrap();
        let rebuilt = ProcessQueryIdAllocator::new(namespace)
            .first_execution()
            .unwrap();

        assert_ne!(first, rebuilt);
    }

    #[test]
    fn query_id_issuance_keeps_one_namespace_and_positive_continuous_sequences() {
        let sequence = AtomicU64::new(0);
        let namespace = QueryProcessNamespace::new(0xfedc_ba98_7654_3210);
        let allocator = ProcessQueryIdAllocator::new(namespace);
        let first = allocator.first_execution_from(&sequence).unwrap();
        let second = allocator.first_execution_from(&sequence).unwrap();

        for (execution, expected) in [(first, 1), (second, 2)] {
            let attribution = execution.query_id().process_attribution().unwrap();
            assert_eq!(attribution.namespace(), namespace);
            assert_eq!(attribution.sequence().get(), expected);
            assert_eq!(execution.attempt_id(), AttemptId::new(1).unwrap());
        }
        assert_ne!(first, second);
        assert_eq!(sequence.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn query_id_issuance_preserves_distinct_process_namespaces() {
        let first = ProcessQueryIdAllocator::new(QueryProcessNamespace::new(11))
            .first_execution_from(&AtomicU64::new(0))
            .unwrap();
        let second = ProcessQueryIdAllocator::new(QueryProcessNamespace::new(12))
            .first_execution_from(&AtomicU64::new(0))
            .unwrap();

        assert_eq!(first.query_id().low(), second.query_id().low());
        assert_ne!(first.query_id().high(), second.query_id().high());
        assert_ne!(first, second);
    }

    #[test]
    fn query_id_issuance_exhaustion_never_wraps_or_reuses_the_final_sequence() {
        let final_sequence = i64::MAX as u64;
        let sequence = AtomicU64::new(final_sequence - 1);
        let namespace = QueryProcessNamespace::new(13);
        let allocator = ProcessQueryIdAllocator::new(namespace);
        let final_execution = allocator.first_execution_from(&sequence).unwrap();
        let attribution = final_execution.query_id().process_attribution().unwrap();
        assert_eq!(attribution.namespace(), namespace);
        assert_eq!(attribution.sequence().get(), final_sequence);

        // Both the original and rebuilt allocator use the same checked author.
        // No test changes the process-global issuing counter.
        for issuer in [allocator, ProcessQueryIdAllocator::new(namespace)] {
            let error = issuer.first_execution_from(&sequence).unwrap_err();
            assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
            assert_eq!(
                error.message(),
                "logical execution query id sequence is exhausted"
            );
            assert_eq!(sequence.load(Ordering::Relaxed), final_sequence);
        }
    }

    #[derive(Debug)]
    struct RecordingRejectedOpen {
        opened: Arc<std::sync::Mutex<Vec<QueryExecutionId>>>,
    }

    impl LogicalExecutionNativePort for RecordingRejectedOpen {
        fn bind_in(
            &self,
            source: &mut Option<LogicalNativeOpenRequest>,
            _target: &mut Option<LogicalNativeSession>,
        ) -> Result<(), LogicalNativeOpenError> {
            self.opened
                .lock()
                .unwrap()
                .push(source.as_ref().unwrap().initial_execution());
            Err(crate::api::LogicalNativeOpenFailure::rejected(
                "injected Native bind rejection after identity observation",
            )
            .into())
        }
    }

    #[tokio::test]
    async fn reserved_identity_reaches_native_open_without_reallocation() {
        let mut control = {
            let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
            let (control, root) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(RecordingRejectedOpen {
                    opened: Arc::clone(&opened),
                }),
                QueryProcessNamespace::new(0x51),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );
            let reserved = client.reserve_identity().unwrap();
            let expected = reserved.execution();

            let scope = root.owner.scope();
            let result = client
                .start_reserved(
                    completion_request(UnreachablePreparationPort),
                    root.owner,
                    reserved,
                )
                .await;
            assert!(
                matches!(result, Err(ref error) if error.kind() == QueryExecutionErrorKind::Rejected)
            );
            assert_eq!(*opened.lock().unwrap(), vec![expected]);
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            scope.wait_released().await;

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn foreign_reservation_is_refused_before_native_side_effects() {
        let mut control = {
            let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
            let namespace = QueryProcessNamespace::new(0x52);
            let donor = ProcessQueryIdAllocator::new(namespace);
            let reserved = donor.reserve().unwrap();
            let (control, root) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(RecordingRejectedOpen {
                    opened: Arc::clone(&opened),
                }),
                namespace,
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let scope = root.owner.scope();
            let result = client
                .start_reserved(
                    completion_request(UnreachablePreparationPort),
                    root.owner,
                    reserved,
                )
                .await;
            assert!(
                matches!(result, Err(ref error) if error.kind() == QueryExecutionErrorKind::InvalidRequest)
            );
            assert!(opened.lock().unwrap().is_empty());
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            scope.wait_released().await;

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn registry_shutdown_error_retains_owner_and_run_failure_for_retry() {
        let mut _control = {
            let (_control, root) = governance().await;
            let (mut supervisor, client) = test_supervisor(
                &_control,
                Handle::current(),
                Arc::new(PanickingOpenNativePort),
                QueryProcessNamespace::new(0x4f),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );

            let scope = root.owner.scope();
            let error = match client
                .start(completion_request(UnreachablePreparationPort), root.owner)
                .await
            {
                Ok(_) => panic!("panicking Native open must fail the logical task"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
            root.business.release();
            supervisor.inject_registry_shutdown_error_once(
                LogicalExecutionRuntimeRegistryError::JoinNotReady,
            );

            assert_eq!(
                supervisor
                    .shutdown_until(Instant::now() + Duration::from_secs(1))
                    .await,
                Err(LogicalExecutionSupervisorShutdownError::Registry(
                    LogicalExecutionRuntimeRegistryError::JoinNotReady,
                ))
            );
            assert!(!supervisor.shutdown_complete);

            assert!(matches!(
                supervisor
                    .shutdown_until(Instant::now() + Duration::from_secs(1))
                    .await,
                Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
            ));
            assert!(supervisor.shutdown_complete);
            assert_ne!(
                _control.snapshot().root_responsibilities,
                0,
                "the real whole health packet still retains its original footer"
            );
            supervisor.abandon_for_process_exit();
            scope.wait_released().await;

            _control
        };
        acknowledge_all_control(&_control);
        _control.finish().await;
    }

    #[tokio::test]
    async fn one_actual_logical_bank_consumes_original_owners_from_all_five_classes() {
        let mut control = {
            let control = governance_empty();
            let frontend = FrontendProcessId::new_v7();
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x9b51),
                frontend,
                supervisor_config(),
            );
            for class in [
                WorkClass::Query,
                WorkClass::MaterializedView,
                WorkClass::Statistics,
                WorkClass::TableMaintenance,
                WorkClass::Management,
            ] {
                let root = control.begin_root_request(WorkRequest::new(class)).await;
                let scope = root.owner.scope();
                assert_eq!(
                    control
                        .snapshot()
                        .scopes
                        .iter()
                        .find(|row| row.id == scope.id())
                        .unwrap()
                        .class,
                    class
                );
                let mut handle = client
                    .start(
                        completion_request(SuccessfulCompletionPreparationPort {
                            backend: BackendProcessId::new_v7(),
                            expected_frontend: frontend,
                        }),
                        root.owner,
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    handle.take_output(),
                    Some(crate::api::ExecutionOutput::Completion)
                ));
                assert_eq!(
                    control
                        .snapshot()
                        .scopes
                        .iter()
                        .find(|row| row.id == scope.id())
                        .unwrap()
                        .class,
                    class,
                    "same-bank handoff cannot substitute a Query owner for another class"
                );
                root.business.release();
                drop(handle);
                scope.wait_released().await;
            }
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }
}
