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
//! The supervisor is the sole constructor of [`QueryExecutionClient`]. An
//! original fixed role cell receives one governed work owner before returning
//! a future to the caller. The role prepares and installs logical execution;
//! the FE runtime then owns its progress. Dropping a caller reply cannot detach
//! its original frame or input. Original exit and Native custody stay separate.
//!
//! Design: ADR-0198 (docs/adr/ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md).

use super::LogicalExecutionExitObserver;
use super::original_exit::OriginalLogicalExit;
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
use tokio::task::JoinHandle;

use crate::api::{
    ExecutionOutputObligation, LogicalExecutionNativePort, LogicalNativeOpenError,
    LogicalNativeOpenRequest, NativeAttemptPreparationError, QueryExecutionClient,
    QueryExecutionDriver, QueryExecutionError, QueryExecutionErrorKind, QueryExecutionFuture,
    QueryExecutionRequest, ReservedExecutionIdentity, ResultSchema,
};
use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason};
use crate::preparation::OutputContract;
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};

use super::{
    LogicalExecutionRuntimeRegistry, LogicalExecutionRuntimeRegistryError,
    LogicalExecutionRuntimeRegistryHandle, LogicalExecutionRuntimeShutdownError,
    assert_shutdown_complete, build_attempt_schedule,
};
use novarocks_coordination_contract::{MonotonicClock, RecoveryMode};

static NEXT_PROCESS_QUERY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const DEFAULT_REMOTE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Explicit process bounds used by the logical execution supervisor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionSupervisorConfig {
    max_admission_issues_per_participation: NonZeroUsize,
    max_establish_authorizations_per_participation: NonZeroUsize,
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
        max_admission_issues_per_participation: NonZeroUsize,
        max_establish_authorizations_per_participation: NonZeroUsize,
        rows: LogicalExecutionRowsConfig,
    ) -> Self {
        Self {
            max_admission_issues_per_participation,
            max_establish_authorizations_per_participation,
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
        let registry = LogicalExecutionRuntimeRegistry::new();
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
        #[expect(
            clippy::disallowed_methods,
            reason = "The process supervisor service retains its original shutdown channel and JoinHandle; execution effects use the credentialed host."
        )]
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
    response: Option<oneshot::Receiver<Result<ExecutionOutputObligation, QueryExecutionError>>>,
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
        CancellationReason::FrontendDrainDeadlineExceeded => {
            return QueryExecutionError::from_execution(
                crate::session_outcome::governed_cancellation_failure_input(reason),
            );
        }
        CancellationReason::DeadlineExceeded => QueryFailureReason::DeadlineExceeded,
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
    reply: oneshot::Sender<Result<ExecutionOutputObligation, QueryExecutionError>>,
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
    if let Some(consumer) = consumer.filter(|_| {
        !original_exit
            .upgrade()
            .expect("original role source remains retained")
            .logical_runtime_installed()
    }) {
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
    let mut native_guard = native_body.clone().lock_owned().await;
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
        session, dormant, ..
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
    let participations = schedule.participations().to_vec();
    let logical_exit = match original_exit.upgrade() {
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
    let mut reservation = match registry.reserve_with_original_exit(
        initial_execution,
        participations.clone(),
        Some(logical_exit),
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
    use super::executor::{
        CancelObserver, ConsumerExecutor, GovernanceExecutor, LogicalExecutors, NativeExecutor,
        RelayExecutor,
    };
    use novarocks_coordination_contract::AttemptActivationIdentity;
    use novarocks_logical_execution_protocol::start::{
        InitialAttempt, LogicalBudgets, LogicalFinishBoundary, LogicalOutputMode,
        LogicalPlanIdentity, LogicalPolicy, LogicalStart, ObservationBudgets, RootCarrier,
    };
    let (composition, replacements) = match session.take_runtime_composition() {
        Ok(original) => original,
        Err(error) => {
            drop(stage);
            return fail_uninstalled_start(pending_owner, reply, error);
        }
    };
    let (installer, attempts, observations, retain_diagnostics) = composition.into_parts();
    let actor = reservation.actor_instance();
    let activation = AttemptActivationIdentity::from_parts(actor.get(), initial_execution, 1);
    let carrier = row_carrier.map(
        |crate::api::ResultRowCarrier::Relayed { kind, client_rows }| RootCarrier {
            kind,
            client_rows,
        },
    );
    let crate::api::PlanSeal::Version(version) = description.plan_identity();
    let started_at = clock.now();
    let host_now = tokio::time::Instant::now();
    let uuid = uuid::Uuid::now_v7().as_u128();
    let id_counter = (((uuid >> 64) & 0x07ff) << 62) | (uuid & ((1u128 << 62) - 1));
    let start = LogicalStart {
        identity: initial_execution.query_id(),
        actor_instance: actor,
        policy: LogicalPolicy {
            recovery: description.recovery(),
            effect: description.effect(),
            output: if internal {
                LogicalOutputMode::InternalResultStream
            } else if result_schema.is_some() {
                LogicalOutputMode::ResultStream
            } else {
                LogicalOutputMode::CompletionOnly
            },
            finish: match description.finish_boundary() {
                crate::preparation::ExecutionFinishBoundary::ClientEof => {
                    LogicalFinishBoundary::ClientEof
                }
                crate::preparation::ExecutionFinishBoundary::TaskCompletion => {
                    LogicalFinishBoundary::TaskCompletion
                }
                crate::preparation::ExecutionFinishBoundary::ComputationReady => {
                    LogicalFinishBoundary::ComputationReady
                }
            },
            retain_diagnostics,
            root_carrier: carrier,
            internal_plan: internal.then_some(LogicalPlanIdentity(*version.as_bytes())),
        },
        budgets: LogicalBudgets {
            max_attempts: match description.recovery() {
                RecoveryMode::NoRecovery => NonZeroU32::MIN,
                RecoveryMode::RestartAttemptBeforeVisibility => config.rows.max_attempts,
            },
            replacement_reservation_valid_for: (description.recovery()
                == RecoveryMode::RestartAttemptBeforeVisibility)
                .then_some(config.rows.replacement_reservation_valid_for),
            remote_cleanup_timeout: config.remote_cleanup_timeout,
            admission_issues_per_participation: config.max_admission_issues_per_participation,
            establish_per_participation: config.max_establish_authorizations_per_participation,
            abort_per_participation: config.max_establish_authorizations_per_participation,
            observation: ObservationBudgets {
                diagnostics: config.diagnostic_timeout,
                convergence: config.remote_cleanup_timeout,
            },
        },
        first_attempt: InitialAttempt {
            activation,
            participations: participations.into_boxed_slice(),
        },
        statement_deadline: cancellation
            .deadline()
            .map(|at| started_at.saturating_add(at.saturating_duration_since(host_now))),
        id_seed: novarocks_coordination_contract::IdSeed::new(id_counter, (uuid >> 80) as u64),
        started_at,
    };
    if let Err(error) = start.validate() {
        drop(stage);
        return fail_uninstalled_start(
            pending_owner,
            reply,
            QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                format!("logical runtime start refused: {error:?}"),
            ),
        );
    }
    let consumer = if internal {
        ConsumerExecutor::new_internal(
            carrier.expect("validated internal carrier"),
            description.plan_identity(),
            config.rows.delivery_capacity,
            observations,
        )
        .map(Some)
    } else if let Some(schema) = result_schema {
        ConsumerExecutor::new_client(
            initial_execution.query_id(),
            schema,
            carrier.expect("validated client carrier"),
            description.plan_identity(),
            config.rows.delivery_capacity,
            observations,
        )
        .map(Some)
    } else {
        Ok(None)
    };
    let consumer = match consumer {
        Ok(original) => original,
        Err(error) => {
            drop(stage);
            return fail_uninstalled_start(pending_owner, reply, error);
        }
    };
    let governance =
        match GovernanceExecutor::new(start.identity, actor, pending_owner.take(), stage) {
            Ok(original) => Arc::new(original),
            Err(refusal) => {
                drop(refusal.stage);
                return fail_uninstalled_start(
                    PendingWorkOwner(Some(refusal.owner)),
                    reply,
                    refusal.error,
                );
            }
        };
    let consumer = Arc::new(match consumer {
        Some(original) => original.with_start_reply(requester, reply),
        None => ConsumerExecutor::new_completion(requester, reply),
    });
    let native = Arc::new(NativeExecutor::new(
        replacements,
        attempts,
        native_body,
        original_exit,
        registry.clone(),
        reservation.runtime_registration(),
        activation,
        Arc::new(schedule),
        frontend_process_id,
        clock,
        cancellation.clone(),
    ));
    let executors = LogicalExecutors {
        relay: Arc::new(RelayExecutor::new(scope, native.clone(), consumer.clone())),
        native,
        consumer: consumer.clone(),
        cancel: Arc::new(CancelObserver::new(cancellation)),
        governance: governance.clone(),
    };
    // No original body mutex is held when FA binds ports or makes a Source
    // visible. Every earlier header wake remains behind the Registry gate.
    drop(native_guard);
    let installed = match installer.install(start.clone(), executors) {
        Ok(original) => original,
        Err(refusal) => {
            let error = QueryExecutionError::new(
                QueryExecutionErrorKind::Rejected,
                format!("logical runtime installation refused: {refusal:?}"),
            );
            consumer.begin_supervised_failure(error);
            governance.close_uninstalled()?;
            return Ok(());
        }
    };
    match reservation.install_runtime(&start, installed) {
        Ok(_) => Ok(()),
        Err(refusal) => {
            let error = registry_error(refusal.error);
            consumer.begin_supervised_failure(error.clone());
            // Retain the installed Source and its exact original cleanup. A
            // contract refusal never fabricates startup, Scope exit or join.
            refusal.installed.request_close(
                novarocks_logical_execution_protocol::input::RegistryClose::Shutdown,
            );
            Err(error)
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
    reply: oneshot::Sender<Result<ExecutionOutputObligation, QueryExecutionError>>,
    error: QueryExecutionError,
) -> Result<(), QueryExecutionError> {
    owner.complete_uninstalled_failure();
    let _ = reply.send(Err(error));
    Ok(())
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

pub(super) fn internal_consumer_panicked(message: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::InternalConsumerPanicked.failure_fact(),
        SafeDetail::truncating(message),
        ExecutionFailureSite::Coordinator,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        LogicalExecutionRowsConfig, LogicalExecutionSupervisor, LogicalExecutionSupervisorConfig,
        LogicalExecutionSupervisorShutdownError, LogicalRoleInput, OriginalRootOutcome,
        OriginalRootPanic, PendingWorkOwner, ProcessQueryIdAllocator, QueryExecutionDriverHandle,
        make_logical_role, native_open_error, native_prepare_error, observe_logical_completion,
        pre_install_cancellation_error, rejected_start, run_supervisor_owned,
        start_cancellation_error,
    };
    use crate::api::{
        ComputationReadyToken, InternalComputationConsumer, InternalConsumerActivation,
        InternalConsumerFuture, LogicalExecutionNativePort, LogicalNativeOpenError,
        LogicalNativeOpenRequest, LogicalNativeSession, NativeAttemptPreparationFailure,
        NativeAttemptPreparationFuture, NativeAttemptPreparationPort, QueryExecutionClient,
        QueryExecutionError, QueryExecutionErrorKind, QueryExecutionRequest,
        StatementNativeHealthFailure,
    };
    use crate::coordination::original_exit::OriginalLogicalExit;
    use crate::coordination::{
        LogicalExecutionRuntimeRegistry, LogicalExecutionRuntimeRegistryError,
    };
    use crate::execution_error::{
        ExecutionFailureInput, ExecutionFailurePresentation, ExecutionFailureSite,
        QueryFailureReason,
    };
    use crate::preparation::{
        ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
        FrozenExecutionDescription, OutputContract,
    };
    use novarocks_coordination_contract::{MonotonicClock, RecoveryMode};
    use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
    use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
    use novarocks_types::identity::{
        AttemptId, FrontendProcessId, QueryExecutionId, QueryProcessNamespace,
    };
    use novarocks_workload_control::{
        CancellationReason, PollCompletion, ResourceConfig, RolePollBank, RootWork, WorkClass,
        WorkError, WorkOwner, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use std::{
        fmt,
        future::Future,
        num::{NonZeroU32, NonZeroUsize},
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        task::Poll,
        time::{Duration, Instant},
    };
    use tokio::{
        runtime::Handle,
        sync::{oneshot, watch},
        task::JoinHandle,
    };

    include!("supervisor_neutral_support.rs");
    include!("supervisor_role_bank_fixture.rs");
    include!("supervisor_preinstall_tests.rs");
    include!("supervisor_background_tests.rs");
    include!("supervisor_original_exit_tests.rs");
    include!("supervisor_migration_minimum_tests.rs");
}
