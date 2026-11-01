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

//! The FE's finite execution lane for internal result codecs and collectors.
//! This is a separate process worker set from command execution: a command
//! may wait for domain progress while holding its own worker.

#[cfg(test)]
use std::num::NonZeroUsize;
use std::time::Instant;

use crate::query_execution::internal_consumer::domain_state::{
    InternalDomainFailure, InternalDomainSlot,
};
use novarocks_execution_contract::{
    SafeDetail, failure::FailureProjection, root_result::RootResultEnd,
};
use novarocks_query_application::api::RetainedRootReply;
use novarocks_query_application::cpu::{
    QueryBlockingExecutor, QueryBlockingExecutorConfig, QueryBlockingExecutorOwner,
    QueryBlockingFailure, QueryBlockingPublishRefusal, QueryBlockingReceipt,
    QueryBlockingReservation, QueryBlockingReserveRefusal,
};
use novarocks_query_application::{
    cancellation::{QueryCancellationReason, QueryCancellationView},
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
};
use novarocks_workload_control::{
    ResultCapacityConfig, ResultWindowAlias, ResultWindowClass, WorkError, WorkScope,
};
use tokio::sync::oneshot;

use crate::native::data_runtime::FrontendDataRuntime;
use crate::task_execution::status_intake::StatusIntakeWake;
use novarocks_workload_control::joint_admission::{
    InternalComputationGrant, InternalCpuActionAdmission, InternalCpuActionSource,
    InternalCpuFactoryBinding, JointLocalGrant,
};
use std::sync::{Arc, Weak};

mod execution_control;
use execution_control::CpuWorkControl;
mod creator_job;
mod metadata_geometry;
mod scoped_statistics;
mod scoped_view_show;
pub(crate) use creator_job::CreatorCpuJob;
pub use metadata_geometry::InternalCpuActionQuote;
pub use metadata_geometry::InternalCpuMetadataGeometry;
use scoped_statistics::ClosedCreatorInput;
pub(crate) use scoped_statistics::StatisticsPlanCpuJob;
pub(crate) use scoped_statistics::scoped_mv_refresh::{MvCpuJob, MvRefreshCpuTurn};

// Historical COW coexistence envelope. This is not a generic Root, Statistics
// or CPU factory minimum; actual domain byte receipts are separate.
pub(crate) const INTERNAL_PEAK_BYTES: u64 = (2 * 336 + 256 + 32 + 32 + 8 + 8) * 1024 * 1024;

/// Coverage check for closed first-party factories which embed the supplied
/// alias into their actual payload backing before handing it off.
pub(crate) fn require_internal_result_capacity(
    scope: &WorkScope,
    window: &ResultWindowAlias,
) -> Result<(), String> {
    if !window.is_for_scope(scope) {
        return Err("internal result CPU window belongs to a foreign scope".into());
    }
    scope.check().map_err(|error| error.to_string())?;
    if window.class() != ResultWindowClass::Internal {
        return Err("internal result CPU requires an admitted Internal window".into());
    }
    window
        .check_backing_total(0)
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Private payload retention for a closed Internal handoff. A write union
/// may reuse only this exact allowance and attribution, keeping one guard.
#[derive(Clone)]
pub(crate) struct InternalResultRetention {
    binding: novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    bytes: Arc<std::sync::Mutex<novarocks_workload_control::ResultPayloadBytes>>,
    merged: Vec<Arc<std::sync::Mutex<novarocks_workload_control::ResultPayloadBytes>>>,
}
impl InternalResultRetention {
    pub(crate) fn try_new(
        binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<Self, String> {
        let window = binding
            .try_window_alias()
            .map_err(|error| error.to_string())?;
        require_internal_result_capacity(binding.scope(), &window)?;
        let bytes = window
            .reserve_payload_bytes(0)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            binding: binding.try_clone().map_err(|error| error.to_string())?,
            bytes: Arc::new(std::sync::Mutex::new(bytes)),
            merged: Vec::new(),
        })
    }
    pub(crate) fn try_spi_guard(
        &self,
    ) -> Result<
        novarocks_spi::connector::ConnectorPayloadRetentionGuard,
        novarocks_workload_control::WorkError,
    > {
        // The guard must retain actual data-byte receipts as well as the
        // original responsibility pin. A pin alone is not byte authority.
        Ok(
            novarocks_spi::connector::ConnectorPayloadRetentionGuard::from_byte_budget(Arc::new(
                self.clone(),
            )),
        )
    }
    pub(crate) fn reserve_bytes(&self, additional: usize) -> Result<(), WorkError> {
        let additional = u64::try_from(additional).map_err(|_| WorkError::ArithmeticOverflow)?;
        self.bytes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .try_grow(additional)
    }
    /// Return only this decoder's aggregate bytes after their actual backing
    /// has been destroyed. Other merged segment receipts remain unchanged.
    pub(crate) fn release_bytes_destroyed(&self, bytes: usize) -> Result<(), WorkError> {
        let bytes = u64::try_from(bytes).map_err(|_| WorkError::ArithmeticOverflow)?;
        self.bytes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .release_destroyed(bytes)
    }
    /// Keep the same receipts when moving another completed segment into the
    /// statement union. No allocation is charged again by this handoff.
    pub(crate) fn merge_retention(&mut self, incoming: &Self) {
        for charge in std::iter::once(&incoming.bytes).chain(incoming.merged.iter()) {
            if !Arc::ptr_eq(&self.bytes, charge)
                && !self.merged.iter().any(|old| Arc::ptr_eq(old, charge))
            {
                self.merged.push(Arc::clone(charge));
            }
        }
    }
    // Explicit old infallible bridge, as with SPI's existing Clone callers.
    pub(crate) fn spi_guard(&self) -> novarocks_spi::connector::ConnectorPayloadRetentionGuard {
        self.try_spi_guard()
            .expect("original Internal payload retention pin clone failed")
    }
    pub(crate) fn is_same_admission(&self, other: &Self) -> bool {
        let window = self.binding.window_alias();
        window.shares_capacity_with(&other.binding.window_alias())
            && window.root_scope().id() == other.binding.window_alias().root_scope().id()
    }
}
impl novarocks_spi::connector::ConnectorPayloadByteBudget for InternalResultRetention {
    fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> Result<
        novarocks_spi::connector::ConnectorPayloadRetentionGuard,
        novarocks_spi::connector::ConnectorError,
    > {
        use novarocks_spi::connector::{
            ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
        };
        let bytes = u64::try_from(bytes).map_err(|_| {
            ConnectorError::from_domain(
                ConnectorErrorKind::ResourceExhausted,
                WorkError::ArithmeticOverflow,
            )
        })?;
        let window = self.binding.try_window_alias().map_err(|error| {
            ConnectorError::from_domain(ConnectorErrorKind::InvalidRequest, error)
        })?;
        let receipt = window.reserve_payload_bytes(bytes).map_err(|error| {
            ConnectorError::from_domain(ConnectorErrorKind::ResourceExhausted, error)
        })?;
        // This receipt, rather than the whole collector's other buffers,
        // follows this one actual allocation through its final Arrow alias.
        Ok(ConnectorPayloadRetentionGuard::new(receipt))
    }
}
impl std::fmt::Debug for InternalResultRetention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InternalResultRetention")
    }
}

/// Frozen FE result-window configuration. CPU workers and their queue use
/// the original executor configuration independently of retained windows.
#[derive(Clone, Copy, Debug)]
pub struct FrontendResultCapacityProfile {
    capacity: ResultCapacityConfig,
}
impl FrontendResultCapacityProfile {
    pub fn try_new(
        capacity: ResultCapacityConfig,
    ) -> Result<Self, novarocks_workload_control::WorkError> {
        let capacity = capacity.validate()?;
        Ok(Self { capacity })
    }

    /// Known metadata from the actual closed production job factory.
    /// The result is not payment or a complete task/payload quote. In
    /// particular, Internal window positions do not bound all forwarders
    /// waiting for queue admission; composition must supply its real job cap.
    pub fn internal_cpu_metadata_geometry(
        self,
    ) -> Result<
        InternalCpuMetadataGeometry,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        metadata_geometry::geometry()
    }

    /// Same actual closed call factory and original queue record as dispatch.
    /// This quote alone does not authorize a CPU action or fund its lifetime.
    pub fn internal_cpu_action_quote(self) -> Result<InternalCpuActionQuote, WorkError> {
        metadata_geometry::operation_action_quote_for_composition()
    }

    pub const fn declared(self) -> ResultCapacityConfig {
        self.capacity
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::try_new(ResultCapacityConfig::V1).expect("test result capacity is supported")
    }
}

pub(crate) struct InternalResultCpuOwner {
    workers: QueryBlockingExecutorOwner,
    runtime: InternalResultCpu,
}

#[derive(Clone)]
pub(crate) struct InternalResultCpu {
    executor: QueryBlockingExecutor,
    factory: Option<InternalCpuFactoryBinding>,
}

impl InternalResultCpuOwner {
    pub(crate) fn try_new(
        config: QueryBlockingExecutorConfig,
        workload: &mut novarocks_workload_control::WorkloadControl,
    ) -> Result<Self, String> {
        let mut owner = Self::new_workers(config)?;
        owner.bind_for_composition(workload)?;
        Ok(owner)
    }
    fn new_workers(config: QueryBlockingExecutorConfig) -> Result<Self, String> {
        let workers = QueryBlockingExecutorOwner::try_new(config)?;
        let runtime = InternalResultCpu {
            executor: workers.executor(),
            factory: None,
        };
        Ok(Self { workers, runtime })
    }
    fn bind_for_composition(
        &mut self,
        workload: &mut novarocks_workload_control::WorkloadControl,
    ) -> Result<(), String> {
        let descriptor = self
            .workers
            .bind_internal_result_closed_factory_for_composition(make_cpu_work)
            .map_err(|error| format!("bind original internal CPU factory: {error}"))?;
        let binding = workload
            .bind_internal_result_closed_factory_for_composition(descriptor)
            .map_err(|error| format!("bind original internal CPU workload owner: {error}"))?;
        self.runtime.factory = Some(binding);
        Ok(())
    }
    // Generic fault injection stays on the original unbound physical pool.
    // Root fixtures must explicitly bind this owner to their real before-Ready
    // WC owner; this constructor never grants a finite source qualification.
    #[cfg(test)]
    pub(crate) fn try_new_for_test() -> Result<Self, String> {
        Self::new_workers(QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(32).unwrap(),
        ))
    }
    #[cfg(test)]
    pub(crate) fn bind_for_test_before_ready(
        &mut self,
        workload: &mut novarocks_workload_control::WorkloadControl,
    ) -> Result<(), String> {
        self.bind_for_composition(workload)
    }

    pub(crate) fn runtime(&self) -> InternalResultCpu {
        self.runtime.clone()
    }

    pub(crate) async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.workers.shutdown_until(deadline).await
    }

    pub(crate) fn request_shutdown_for_process_exit(&self) {
        self.workers.request_shutdown_for_process_exit();
    }
}

/// Private first-party domain state, followed by its complete physical guard.
/// No bare graph extraction is provided. Actual domain output transfer must
/// explicitly carry the guard into its final payload owners.
pub(crate) struct InternalResultValue<T> {
    value: T,
    window: ResultWindowAlias,
    activity:
        Option<novarocks_query_application::admitted_query_context::ResultCapacityActivityLease>,
}
impl<T> InternalResultValue<T> {
    /// Check coverage before invoking a constructor that may grow the graph.
    pub(crate) fn try_produce(
        scope: &WorkScope,
        window: ResultWindowAlias,
        produce: impl FnOnce(&ResultWindowAlias) -> Result<T, String>,
    ) -> Result<Self, String> {
        require_internal_result_capacity(scope, &window)?;
        let value = produce(&window)?;
        Ok(Self {
            value,
            window,
            activity: None,
        })
    }

    pub(crate) fn track_actual_exit(
        mut self,
        binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<Self, String> {
        if self.activity.is_some()
            || !self.window.is_for_scope(binding.scope())
            || !self.window.shares_capacity_with(&binding.window_alias())
        {
            return Err("internal CPU activity differs from its input admission".into());
        }
        self.activity = Some(
            binding
                .begin_result_activity()
                .map_err(|error| error.to_string())?,
        );
        Ok(self)
    }

    pub(crate) fn track_computation_exit(
        mut self,
        binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        computation: novarocks_workload_control::joint_admission::InternalComputationAlias,
    ) -> Result<Self, String> {
        let original = binding
            .try_window_alias()
            .map_err(|error| error.to_string())?;
        if self.activity.is_some()
            || !self.window.is_for_scope(binding.scope())
            || !self.window.shares_capacity_with(&original)
        {
            return Err("internal CPU activity differs from its original Root payload".into());
        }
        self.activity = Some(
            binding
                .begin_internal_result_activity(computation)
                .map_err(|error| error.to_string())?,
        );
        Ok(self)
    }

    pub(crate) fn value(&self) -> &T {
        &self.value
    }
    pub(crate) fn try_transform<R, E>(
        self,
        transform: impl FnOnce(T, &ResultWindowAlias) -> Result<R, E>,
    ) -> Result<InternalResultValue<R>, E> {
        let Self {
            value,
            window,
            activity,
        } = self;
        let value = transform(value, &window)?;
        Ok(InternalResultValue {
            value,
            window,
            activity,
        })
    }

    /// Final closed handoff only: the callback must return payloads whose
    /// actual backing was already guarded by the first-party domain factory.
    /// The full allowance remains held throughout the callback itself.
    pub(crate) fn hand_off<R>(self, handoff: impl FnOnce(T, &ResultWindowAlias) -> R) -> R {
        let Self {
            value,
            window,
            activity,
        } = self;
        let output = handoff(value, &window);
        drop(window);
        drop(activity);
        output
    }

    /// A closed application transformation keeps the same whole allowance.
    /// The closure must retain the supplied alias in any payload it hands to
    /// a provider or separately clonable backing before returning that graph.
    pub(crate) fn transform<R>(
        self,
        transform: impl FnOnce(T, &ResultWindowAlias) -> R,
    ) -> InternalResultValue<R> {
        let Self {
            value,
            window,
            activity,
        } = self;
        let value = transform(value, &window);
        InternalResultValue {
            value,
            window,
            activity,
        }
    }
}

fn cpu_execution_failure(detail: &'static str) -> ExecutionFailureInput {
    ExecutionFailureInput::new(
        QueryFailureReason::LogicalFailureUnspecified.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    )
}

fn cpu_cancellation_failure(reason: QueryCancellationReason) -> ExecutionFailureInput {
    let reason = match reason {
        QueryCancellationReason::DeadlineExceeded { .. }
        | QueryCancellationReason::FrontendDrainDeadlineExceeded { .. } => {
            QueryFailureReason::DeadlineExceeded
        }
        _ => QueryFailureReason::Cancelled,
    };
    ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating("internal result CPU was stopped by its statement control owner"),
        ExecutionFailureSite::Coordinator,
    )
}

/// At most one result-codec job per serial root consumer. Its forwarder owns
/// the bounded receipt. Closing its waiter never cancels a running CPU closure:
/// the same forwarder still awaits its receipt and keeps unclaimed output.
/// An unclaimed output contains the same window until its actual destruction.
/// One fixed retirement packet belongs to this original job and forwarder.
/// At most one executor result, one early undelivered projection, one worker
/// failure, one distinct physical failure, two existing notification failures
/// and one true JoinError exist.
struct InternalCpuRetirement<T> {
    state: std::sync::Mutex<InternalCpuRetirementState<T>>,
}
type OperationOutput = Result<InternalDomainSlot, InternalDomainFailure>;
type OperationResult = Result<InternalResultValue<OperationOutput>, ExecutionFailureInput>;

// This is a field of the same original retirement packet, not another queue.
#[derive(Default)]
struct CpuActionState {
    admission: Option<InternalCpuActionAdmission>,
    physical: Option<JointLocalGrant<QueryBlockingReservation>>,
    receipt: Option<QueryBlockingReceipt<OperationResult>>,
    published: bool,
}
struct InternalCpuRetirementState<T> {
    closed_input: Option<InternalResultValue<InternalDomainSlot>>,
    closed_operation: Option<InternalResultOperation>,
    provider_input: Option<Box<ClosedCreatorInput>>,
    provider_backing: Option<crate::task_execution::blocking_io::ConnectorBlockingIoBacking>,
    provider_marker_failure: Option<QueryBlockingFailure>,
    provider_retirement_failure: Option<QueryBlockingFailure>,
    provider_close_failure: Option<QueryBlockingFailure>,
    action: Option<CpuActionState>,
    admission_failure: Option<WorkError>,
    reserve_refusal: Option<QueryBlockingReserveRefusal>,
    publication_refusal: Option<QueryBlockingPublishRefusal>,
    physical_failure: Option<QueryBlockingFailure>,
    physical_unproven: bool,
    outcome: Option<Result<InternalResultValue<T>, ExecutionFailureInput>>,
    early: Option<ExecutionFailureInput>,
    worker: Option<QueryBlockingFailure>,
    notifications: [Option<QueryBlockingFailure>; 2],
    forwarder: Option<Arc<tokio::task::JoinError>>,
    // These original sources never become a worker-unwind local. In the
    // healthy case they retire after the same actual receipt/forwarder join.
    window: Option<ResultWindowAlias>,
    activity:
        Option<novarocks_query_application::admitted_query_context::ResultCapacityActivityLease>,
}
pub(crate) struct InternalCpuRetired<T> {
    _closed_input: Option<InternalResultValue<InternalDomainSlot>>,
    _closed_operation: Option<InternalResultOperation>,
    _action: Option<CpuActionState>,
    _admission_failure: Option<WorkError>,
    _outcome: Option<Result<InternalResultValue<T>, ExecutionFailureInput>>,
    _window: Option<ResultWindowAlias>,
    _activity:
        Option<novarocks_query_application::admitted_query_context::ResultCapacityActivityLease>,
}
impl<T> InternalCpuRetirement<T> {
    fn new(
        window: ResultWindowAlias,
        activity: Option<
            novarocks_query_application::admitted_query_context::ResultCapacityActivityLease,
        >,
    ) -> Self {
        Self {
            state: std::sync::Mutex::new(InternalCpuRetirementState {
                closed_input: None,
                closed_operation: None,
                provider_input: None,
                provider_backing: None,
                provider_marker_failure: None,
                provider_retirement_failure: None,
                provider_close_failure: None,
                action: None,
                admission_failure: None,
                reserve_refusal: None,
                publication_refusal: None,
                physical_failure: None,
                physical_unproven: false,
                outcome: None,
                early: None,
                worker: None,
                notifications: [None, None],
                forwarder: None,
                window: Some(window),
                activity,
            }),
        }
    }
    fn has_failure(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .worker
            .as_ref()
            .is_some_and(QueryBlockingFailure::is_unexpected)
            || state.forwarder.is_some()
            || state.provider_marker_failure.is_some()
            || state.provider_retirement_failure.is_some()
            || state.provider_close_failure.is_some()
            || state.physical_unproven
            || state.physical_failure.is_some()
            || state.admission_failure.as_ref().is_some_and(|error| {
                error.failure_fact().class() == novarocks_failure_contract::FailureClass::Invariant
            })
            || state.notifications.iter().any(Option::is_some)
    }
    /// The original work has actually returned. Restore its same activity to
    /// a successful output before publication: an unclaimed output must keep
    /// the existing actual-exit fence until it is claimed or really destroyed.
    fn receive_work(
        &self,
        result: Result<Result<InternalResultValue<T>, ExecutionFailureInput>, QueryBlockingFailure>,
    ) -> Result<InternalResultValue<T>, ExecutionFailureInput> {
        match result {
            Ok(Ok(mut output)) => {
                output.activity = self
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .activity
                    .take();
                Ok(output)
            }
            Ok(Err(error)) => Err(error),
            Err(original) => {
                self.state.lock().unwrap_or_else(|e| e.into_inner()).worker = Some(original);
                Err(cpu_execution_failure("internal result CPU executor failed"))
            }
        }
    }
    fn notify(&self, index: usize, wake: &dyn StatusIntakeWake) {
        if let Err(original) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| wake.wake()))
        {
            // Each branch calls each fixed index once. Stable before any later
            // callback, and keep awaiting the same work after an early panic.
            self.state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .notifications[index] = Some(QueryBlockingFailure::panicked(original));
        }
    }
}
impl<T> std::fmt::Debug for InternalCpuRetirement<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InternalCpuRetirement")
    }
}
impl<T> std::fmt::Display for InternalCpuRetirement<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original Internal CPU failure is retained")
    }
}
impl<T: Send + 'static> std::error::Error for InternalCpuRetirement<T> {}

pub(crate) struct InternalResultCpuJob<T: Send + 'static> {
    completion: oneshot::Receiver<Result<InternalResultValue<T>, ExecutionFailureInput>>,
    retirement: Arc<InternalCpuRetirement<T>>,
    forwarder: Option<tokio::task::JoinHandle<()>>,
    closing: bool,
    claimed: bool,
    closed: Option<Result<(), Arc<tokio::task::JoinError>>>,
}
impl<T: Send + 'static> InternalResultCpuJob<T> {
    /// Refuse further result delivery. Sender::closed wakes the same original
    /// forwarder, which continues awaiting the already-owned CPU receipt.
    pub(crate) fn begin_close(&mut self) {
        drop(self.close_delivery());
    }

    /// Move unclaimed output to the stable caller so its potentially large
    /// graph is destroyed outside the caller's custody lock. Returning this
    /// value is not actual exit; the original forwarder must still be joined.
    pub(crate) fn close_delivery(
        &mut self,
    ) -> Option<Result<InternalResultValue<T>, ExecutionFailureInput>> {
        self.closing = true;
        self.completion.close();
        self.completion.try_recv().ok()
    }

    /// Observe the original join without destroying delivery output. A stable
    /// caller closes delivery separately and retires its result outside locks.
    pub(crate) fn poll_forwarder_closed(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Arc<dyn std::error::Error + Send + Sync>>> {
        use std::{future::Future, pin::Pin, task::Poll};
        if self.closed.is_none() {
            let result = std::task::ready!(
                Pin::new(
                    self.forwarder
                        .as_mut()
                        .expect("original CPU forwarder join")
                )
                .poll(cx)
            );
            self.closed = Some(result.map_err(Arc::new));
            self.forwarder.take();
        }
        if let Some(Err(original)) = &self.closed {
            self.retirement
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .forwarder = Some(Arc::clone(original));
        }
        match self.health_failure() {
            Some(original) => Poll::Ready(Err(original)),
            None => Poll::Ready(Ok(())),
        }
    }

    /// A borrowed projection shares the same complete packet. It does not
    /// release this job, consume an outcome or acknowledge health disposition.
    pub(crate) fn health_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        self.retirement
            .has_failure()
            .then(|| Arc::clone(&self.retirement) as Arc<dyn std::error::Error + Send + Sync>)
    }

    /// Only after a successful original join may the caller retire a late
    /// normal result. Return ownership so destruction occurs outside locks.
    pub(crate) fn take_retired_outcome(&mut self) -> Option<InternalCpuRetired<T>> {
        if !matches!(&self.closed, Some(Ok(()))) || self.retirement.has_failure() {
            return None;
        }
        let mut state = self
            .retirement
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Provider input/opaque source must be explicitly retired outside the
        // finite worker and custody lock before ordinary packet retirement.
        if state.provider_input.is_some() {
            return None;
        }
        Some(InternalCpuRetired {
            _closed_input: state.closed_input.take(),
            _closed_operation: state.closed_operation.take(),
            _action: state.action.take(),
            _admission_failure: state.admission_failure.take(),
            _outcome: state.outcome.take(),
            _window: state.window.take(),
            _activity: state.activity.take(),
        })
    }

    /// Shares the same original join error with health custody. The task's
    /// result was moved into this Arc once; no failure is reconstructed.
    #[cfg(test)]
    pub(crate) fn join_failure(&self) -> Option<Arc<tokio::task::JoinError>> {
        self.closed
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(Arc::clone)
    }

    #[cfg(test)]
    pub(crate) async fn join_closed(
        &mut self,
    ) -> Result<(), Arc<dyn std::error::Error + Send + Sync>> {
        self.begin_close();
        match std::future::poll_fn(|cx| self.poll_forwarder_closed(cx)).await {
            Ok(()) => drop(self.take_retired_outcome()),
            Err(original) => return Err(original),
        }
        match self.health_failure() {
            Some(original) => Err(original),
            None => Ok(()),
        }
    }

    /// Poll the original bounded receipt directly. A cancelled waiter leaves
    /// both the receipt and the original join in this job's stable owner.
    pub(crate) fn poll_take(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<InternalResultValue<T>, ExecutionFailureInput>> {
        use std::{future::Future, pin::Pin, task::Poll};
        if self.closing || self.claimed {
            return Poll::Ready(Err(cpu_execution_failure(
                "internal result CPU receipt is no longer available",
            )));
        }
        let result = std::task::ready!(Pin::new(&mut self.completion).poll(cx));
        self.claimed = true;
        Poll::Ready(match result {
            Ok(mut result) => {
                if let Ok(value) = &mut result {
                    value.activity.take();
                }
                result
            }
            Err(_) => Err(cpu_execution_failure(
                "internal result CPU forwarder exited without its outcome",
            )),
        })
    }
}
impl<T: Send + 'static> Drop for InternalResultCpuJob<T> {
    fn drop(&mut self) {
        // Detached fallback is not exit proof. A Native aggregate retaining
        // this job uses the original join and result activity independently.
        self.begin_close();
    }
}

pub(crate) enum ProviderCreatorCpuSource {
    Bound(InternalCpuActionSource),
    #[cfg(test)]
    UnqualifiedFixture,
}

/// Original Provider serial owner keeps this concrete same job and join.
/// Cancellation of finish/close only drops a borrow, never the receipt/input.
pub(crate) struct ProviderReadCpuJob {
    job: Option<InternalResultCpuJob<OperationOutput>>,
    marker: Option<OperationResult>,
    finished: bool,
}
#[cfg(test)]
pub(crate) type ProviderCreatorFixtureDisposition = InternalResultCpuJob<OperationOutput>;

impl ProviderReadCpuJob {
    #[cfg(test)]
    pub(crate) fn fixture_worker_failure_is<T: std::any::Any>(&self) -> bool {
        self.job.as_ref().is_some_and(|job| {
            job.retirement
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .worker
                .as_ref()
                .is_some_and(QueryBlockingFailure::original_payload_is::<T>)
        })
    }
    /// Explicit fixture-only disposition, not a normal exit receipt. A real
    /// close must already have joined the original forwarder and delivered
    /// this same health packet to its stable receiving source.
    #[cfg(test)]
    pub(crate) fn take_joined_for_fixture_disposition(
        &mut self,
        observed: &novarocks_query_application::api::StatementNativeHealthFailure,
    ) -> Result<ProviderCreatorFixtureDisposition, &'static str> {
        let job = self.job.as_ref().ok_or("fixture job already moved")?;
        if job.forwarder.is_some() || !matches!(&job.closed, Some(Ok(()))) {
            return Err("fixture requires the actual successful forwarder join");
        }
        let same: &(dyn std::error::Error + Send + Sync) = job.retirement.as_ref();
        if !job.retirement.has_failure() || !std::ptr::addr_eq(observed.cause(), same) {
            return Err("fixture requires the same observed health source");
        }
        {
            let packet = job
                .retirement
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if packet.physical_unproven || packet.physical_failure.is_some() {
                return Err("fixture cannot dispose an unproven physical exit");
            }
        }
        // No finished flag, normal retirement, new receipt or worker. The same
        // opaque packet remains owned by observed health after this move.
        Ok(self.job.take().expect("same validated fixture job"))
    }
    pub(crate) fn health_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        self.job
            .as_ref()
            .and_then(InternalResultCpuJob::health_failure)
    }
    pub(crate) fn poll_finish_in(
        &mut self,
        cx: &mut std::task::Context<'_>,
        destination: &mut Option<
            Result<
                novarocks_sql::compiler::ProviderReadFact,
                super::provider_read_facts::ProviderReadError,
            >,
        >,
    ) -> std::task::Poll<Result<(), Arc<dyn std::error::Error + Send + Sync>>> {
        use std::task::Poll;
        let job = self.job.as_mut().expect("same original Provider CPU job");
        if self.marker.is_none() {
            self.marker = Some(std::task::ready!(job.poll_take(cx)));
        }
        std::task::ready!(job.poll_forwarder_closed(cx))?;
        if destination.is_none() {
            if matches!(self.marker.as_ref(),Some(Ok(value)) if matches!(value.value(),Ok(InternalDomainSlot::ProviderCreatorFinished)))
            {
                job.retirement
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .provider_input
                    .as_mut()
                    .expect("same retained Provider input")
                    .take_read_result_in(destination)
                    .expect("one stable Provider result receiver");
            } else {
                use super::provider_read_facts::{ProviderReadCpuFailure, ProviderReadError};
                let error = match self.marker.as_ref() {
                    Some(Err(execution)) => {
                        let admission = job
                            .retirement
                            .state
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .admission_failure
                            .clone();
                        ProviderReadError::Cpu(ProviderReadCpuFailure {
                            execution: execution.clone(),
                            admission,
                        })
                    }
                    Some(Ok(value)) => match value.value() {
                        Err(InternalDomainFailure::Source(source)) => {
                            ProviderReadError::Original(source.clone())
                        }
                        Err(InternalDomainFailure::Diagnostic(message)) => {
                            ProviderReadError::Contract(message.clone())
                        }
                        _ => ProviderReadError::Contract(
                            "Provider creator stopped before result delivery".into(),
                        ),
                    },
                    None => unreachable!("same original marker was received above"),
                };
                *destination = Some(Err(error));
            }
        }
        self.retire_sources()?;
        Poll::Ready(Ok(()))
    }
    pub(crate) async fn close(&mut self) -> Result<(), Arc<dyn std::error::Error + Send + Sync>> {
        let job = self.job.as_mut().expect("same original Provider CPU job");
        // A closed receiver never cancels an already running CPU operation.
        // It keeps the actual unreceived value/input in the same packet.
        if !job.closing {
            job.closing = true;
            if let Err(cause) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.completion.close()))
            {
                job.retirement
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .provider_close_failure = Some(QueryBlockingFailure::panicked(cause));
            }
        }
        std::future::poll_fn(|cx| job.poll_forwarder_closed(cx)).await?;
        self.retire_sources()
    }
    fn retire_sources(&mut self) -> Result<(), Arc<dyn std::error::Error + Send + Sync>> {
        let job = self.job.as_mut().expect("same original Provider CPU job");
        // Both original output and input are stable before any actual Drop.
        // Each named destruction cut executes once. Opaque payloads remain in
        // the original packet; no formatting, retry, extra worker or reaper.
        if let Some(marker) = self.marker.take()
            && let Err(cause) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(marker)))
        {
            job.retirement
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .provider_marker_failure = Some(QueryBlockingFailure::panicked(cause));
        }
        if let Some(cause) = job.health_failure() {
            return Err(cause);
        }
        let input = job
            .retirement
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .provider_input
            .take();
        if let Err(cause) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(input))) {
            job.retirement
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .provider_retirement_failure = Some(QueryBlockingFailure::panicked(cause));
        }
        if let Some(cause) = job.health_failure() {
            return Err(cause);
        }
        let retired = job.take_retired_outcome();
        if let Err(cause) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(retired)))
        {
            // Input retirement was healthy above; this same named terminal
            // retirement slot is still empty. Never repeat either Drop cut.
            job.retirement
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .provider_retirement_failure = Some(QueryBlockingFailure::panicked(cause));
        }
        if let Some(cause) = job.health_failure() {
            return Err(cause);
        }
        self.finished = true;
        Ok(())
    }
}
impl Drop for ProviderReadCpuJob {
    fn drop(&mut self) {
        if !self.finished
            && let Some(job) = self.job.take()
        {
            // Invalid whole-source loss is fail-closed, not normal close.
            // Keep the finite original packet/join/backing; do not spawn a
            // replacement or pretend the actual source has exited.
            std::mem::forget(job);
        }
    }
}

/// Closed production operations for the original internal result worker.
/// This limits its call surface; it does not certify Provider codecs, locks,
/// callbacks or destruction as finite, or grant a host-only protection policy.
pub(crate) enum InternalResultOperation {
    ProviderCreator,
    Body(Arc<RetainedRootReply>),
    End(RootResultEnd),
    FinishStatistics,
    AcceptWrite(crate::query_execution::write_session::WriteReceivingPort),
}
impl InternalResultOperation {
    fn apply(self, slot: InternalDomainSlot) -> Result<InternalDomainSlot, InternalDomainFailure> {
        match self {
            Self::ProviderCreator => Err("Provider creator used a domain payload path".into()),
            Self::Body(reply) => slot.apply_body(reply),
            Self::End(end) => slot.finish_eof(end),
            Self::FinishStatistics => slot.finish_statistics(),
            Self::AcceptWrite(receiving) => slot.accept_write(receiving),
        }
    }
}

impl InternalResultCpu {
    /// Verify the original live WC/factory binding without a capacity quote.
    pub(crate) fn validate_workload_source(
        &self,
        workload: &novarocks_workload_control::WorkloadControl,
    ) -> Result<(), WorkError> {
        self.factory
            .as_ref()
            .ok_or(WorkError::NotReady)?
            .validate_in(workload)
    }

    pub(crate) fn capture_action_source(
        &self,
        grant: &InternalComputationGrant,
        scope: &WorkScope,
    ) -> Result<InternalCpuActionSource, WorkError> {
        self.factory
            .as_ref()
            .ok_or(WorkError::NotReady)?
            .bind_source(grant.cpu_action_source(scope)?)
    }
    pub(crate) fn provider_creator_source(
        &self,
        scope: &WorkScope,
    ) -> Result<InternalCpuActionSource, WorkError> {
        self.factory
            .as_ref()
            .ok_or(WorkError::NotReady)?
            .provider_creator_source(scope)
    }

    pub(crate) fn metadata_cpu_continuation_source(
        &self,
        io: &crate::query_execution::maintenance::io_admission::MetadataIoAdmission,
        root: &WorkScope,
    ) -> Result<InternalCpuActionSource, WorkError> {
        io.metadata_cpu_continuation_source(self.factory.as_ref().ok_or(WorkError::NotReady)?, root)
    }

    /// Same factory, bounded pool, original receipt/forwarder/cause algorithm.
    /// Input metadata is separate from the cumulative Internal result window.
    pub(crate) fn submit_provider_creator(
        &self,
        input: super::provider_read_facts::ProviderReadCreatorInput,
        window: ResultWindowAlias,
        backing: crate::task_execution::blocking_io::ConnectorBlockingIoBacking,
        source: ProviderCreatorCpuSource,
        cancellation: QueryCancellationView,
        runtime: &tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> ProviderReadCpuJob {
        let source = match source {
            ProviderCreatorCpuSource::Bound(source) => Some(source),
            #[cfg(test)]
            ProviderCreatorCpuSource::UnqualifiedFixture => None,
        };
        self.submit_creator_input(
            ClosedCreatorInput::Read(input),
            window,
            Some(backing),
            source,
            cancellation,
            runtime,
            wake,
        )
    }

    fn submit_creator_input(
        &self,
        input: ClosedCreatorInput,
        window: ResultWindowAlias,
        backing: Option<crate::task_execution::blocking_io::ConnectorBlockingIoBacking>,
        source: Option<InternalCpuActionSource>,
        cancellation: QueryCancellationView,
        runtime: &tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> ProviderReadCpuJob {
        self.submit_creator_input_with_control(
            input,
            window,
            backing,
            source,
            cancellation.into(),
            runtime,
            wake,
        )
    }

    fn submit_creator_input_with_control(
        &self,
        input: ClosedCreatorInput,
        window: ResultWindowAlias,
        backing: Option<crate::task_execution::blocking_io::ConnectorBlockingIoBacking>,
        source: Option<InternalCpuActionSource>,
        control: CpuWorkControl,
        runtime: &tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> ProviderReadCpuJob {
        let (completed, completion) = oneshot::channel();
        let retirement = Arc::new(InternalCpuRetirement::new(window, None));
        {
            let mut state = retirement.state.lock().unwrap_or_else(|e| e.into_inner());
            state.closed_operation = Some(InternalResultOperation::ProviderCreator);
            state.provider_input = Some(Box::new(input));
            state.provider_backing = backing;
            state.action = Some(CpuActionState::default());
        }
        let forwarder = runtime.spawn(make_operation_forwarder(
            self.executor.clone(),
            self.factory.clone(),
            source,
            control,
            wake,
            Arc::clone(&retirement),
            completed,
        ));
        ProviderReadCpuJob {
            job: Some(InternalResultCpuJob {
                completion,
                retirement,
                forwarder: Some(forwarder),
                closing: false,
                claimed: false,
                closed: None,
            }),
            marker: None,
            finished: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn configured_capacity(&self) -> QueryBlockingExecutorConfig {
        self.executor.configured_capacity()
    }
    #[cfg(test)]
    pub(crate) fn operation_action_quote(&self) -> Result<InternalCpuActionQuote, WorkError> {
        metadata_geometry::operation_action_quote(&self.executor)
    }

    pub(crate) fn submit_operation(
        &self,
        input: InternalResultValue<InternalDomainSlot>,
        cancellation: QueryCancellationView,
        data_runtime: &FrontendDataRuntime,
        wake: Arc<dyn StatusIntakeWake>,
        operation: InternalResultOperation,
    ) -> InternalResultCpuJob<Result<InternalDomainSlot, InternalDomainFailure>> {
        self.submit_closed_operation(input, None, cancellation, data_runtime, wake, operation)
    }

    /// Root-backed production only. The original positive computation source
    /// is captured by Domain under its lock; all admission and job publication
    /// happens in this same original forwarder, outside that lock.
    pub(crate) fn submit_root_operation(
        &self,
        input: InternalResultValue<InternalDomainSlot>,
        source: InternalCpuActionSource,
        cancellation: QueryCancellationView,
        data_runtime: &FrontendDataRuntime,
        wake: Arc<dyn StatusIntakeWake>,
        operation: InternalResultOperation,
    ) -> InternalResultCpuJob<OperationOutput> {
        self.submit_closed_operation(
            input,
            Some(source),
            cancellation,
            data_runtime,
            wake,
            operation,
        )
    }
    fn submit_closed_operation(
        &self,
        mut input: InternalResultValue<InternalDomainSlot>,
        source: Option<InternalCpuActionSource>,
        cancellation: QueryCancellationView,
        data_runtime: &FrontendDataRuntime,
        wake: Arc<dyn StatusIntakeWake>,
        operation: InternalResultOperation,
    ) -> InternalResultCpuJob<OperationOutput> {
        let (completed, completion) = oneshot::channel();
        let retirement = Arc::new(InternalCpuRetirement::new(
            input.window.clone(),
            input.activity.take(),
        ));
        {
            let mut state = retirement.state.lock().unwrap_or_else(|e| e.into_inner());
            state.closed_input = Some(input);
            state.closed_operation = Some(operation);
            state.action = Some(CpuActionState::default());
        }
        let forwarder = data_runtime.spawn(make_operation_forwarder(
            self.executor.clone(),
            self.factory.clone(),
            source,
            cancellation.into(),
            wake,
            Arc::clone(&retirement),
            completed,
        ));
        InternalResultCpuJob {
            completion,
            retirement,
            forwarder: Some(forwarder),
            closing: false,
            claimed: false,
            closed: None,
        }
    }

    // The original fault-injection tests require arbitrary jobs, including real
    // blocked and panicking destructors. This is not a production entry point.
    #[cfg(test)]
    pub(crate) fn submit<I, O, F>(
        &self,
        input: InternalResultValue<I>,
        cancellation: QueryCancellationView,
        data_runtime: &FrontendDataRuntime,
        wake: Arc<dyn StatusIntakeWake>,
        apply: F,
    ) -> InternalResultCpuJob<O>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: FnOnce(I, &ResultWindowAlias) -> O + Send + 'static,
    {
        self.submit_with(
            input,
            cancellation,
            data_runtime,
            wake,
            apply,
            make_forwarder,
        )
    }

    #[cfg(test)]
    fn submit_with<I, O, A, M, F>(
        &self,
        input: InternalResultValue<I>,
        cancellation: QueryCancellationView,
        data_runtime: &FrontendDataRuntime,
        wake: Arc<dyn StatusIntakeWake>,
        apply: A,
        make: M,
    ) -> InternalResultCpuJob<O>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: std::future::Future<Output = ()> + Send + 'static,
        M: FnOnce(
            QueryBlockingExecutor,
            InternalResultValue<I>,
            QueryCancellationView,
            Arc<dyn StatusIntakeWake>,
            Arc<InternalCpuRetirement<O>>,
            oneshot::Sender<Result<InternalResultValue<O>, ExecutionFailureInput>>,
            A,
        ) -> F,
    {
        let executor = self.executor.clone();
        let (completed, completion) = oneshot::channel();
        let mut input = input;
        let retirement = Arc::new(InternalCpuRetirement::new(
            input.window.clone(),
            input.activity.take(),
        ));
        let original_retirement = Arc::clone(&retirement);
        let forwarder = data_runtime.spawn(make(
            executor,
            input,
            cancellation,
            wake,
            original_retirement,
            completed,
            apply,
        ));
        InternalResultCpuJob {
            completion,
            retirement,
            forwarder: Some(forwarder),
            closing: false,
            claimed: false,
            closed: None,
        }
    }
}

// Only this original forwarder borrows its action fields. A short poll lease
// restores the same fields before any await or unwind. No foreign callback or
// destructor runs under retirement.state, and the input remains in the packet.
struct CpuActionLease<'a> {
    packet: &'a InternalCpuRetirement<OperationOutput>,
    value: Option<CpuActionState>,
}
impl<'a> CpuActionLease<'a> {
    fn take(packet: &'a InternalCpuRetirement<OperationOutput>) -> Self {
        let value = packet
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .action
            .take();
        assert!(value.is_some(), "one original CPU action poller");
        Self { packet, value }
    }
    fn state(&mut self) -> &mut CpuActionState {
        self.value.as_mut().expect("same original CPU action")
    }
}
impl Drop for CpuActionLease<'_> {
    fn drop(&mut self) {
        let mut state = self.packet.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(state.action.is_none(), "same original CPU action return");
        state.action = self.value.take();
    }
}

#[derive(Clone, Copy, Debug)]
enum CpuActionFailureSite {
    Begin,
    LocalOffer,
    PreparePublication,
    Publish,
}

const CPU_ACTION_DETAIL_BYTES: usize = novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES;

fn cpu_action_failure_detail(site: CpuActionFailureSite, error: &WorkError) -> SafeDetail {
    struct Bounded {
        bytes: [u8; CPU_ACTION_DETAIL_BYTES],
        len: usize,
    }
    impl std::fmt::Write for Bounded {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            let mut end = value.len().min(self.bytes.len() - self.len);
            while !value.is_char_boundary(end) {
                end -= 1;
            }
            self.bytes[self.len..self.len + end].copy_from_slice(&value.as_bytes()[..end]);
            self.len += end;
            if end == value.len() {
                Ok(())
            } else {
                Err(std::fmt::Error)
            }
        }
    }
    let mut output = Bounded {
        bytes: [0; CPU_ACTION_DETAIL_BYTES],
        len: 0,
    };
    // Only the closed local governance vocabulary is rendered. Stack formatting
    // has no geometric heap growth; str::to_owned below requests one <=512B copy.
    let _ = std::fmt::write(
        &mut output,
        format_args!("internal CPU action {site:?} refused: {error}"),
    );
    SafeDetail::truncating(std::str::from_utf8(&output.bytes[..output.len]).expect("UTF-8 prefix"))
}

fn action_failure(
    packet: &InternalCpuRetirement<OperationOutput>,
    site: CpuActionFailureSite,
    error: WorkError,
) -> ExecutionFailureInput {
    // Only this paid serial Statistics port extends the original diagnostic.
    // Other closed CPU families keep their pre-existing fixed text budget.
    let statistics_provider = {
        let state = packet.state.lock().unwrap_or_else(|e| e.into_inner());
        matches!(
            state.provider_input.as_deref(),
            Some(ClosedCreatorInput::StatisticsProvider(_))
        )
    };
    let detail = if statistics_provider {
        cpu_action_failure_detail(site, &error)
    } else {
        SafeDetail::truncating("internal CPU action admission failed")
    };
    let projected = ExecutionFailureInput::new(
        error.failure_fact(),
        detail,
        ExecutionFailureSite::Coordinator,
    );
    let mut state = packet.state.lock().unwrap_or_else(|e| e.into_inner());
    if state.admission_failure.is_none() {
        state.admission_failure = Some(error);
    }
    projected
}

struct ProviderCreatorInputLoan<'a> {
    packet: &'a InternalCpuRetirement<OperationOutput>,
    input: Option<Box<ClosedCreatorInput>>,
}
impl<'a> ProviderCreatorInputLoan<'a> {
    fn take(packet: &'a InternalCpuRetirement<OperationOutput>) -> Self {
        let input = packet
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .provider_input
            .take();
        Self { packet, input }
    }
}
impl Drop for ProviderCreatorInputLoan<'_> {
    fn drop(&mut self) {
        let mut state = self.packet.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            state.provider_input.is_none(),
            "one original Provider input receiver"
        );
        state.provider_input = self.input.take();
    }
}

// Only the real worker calls this factory output. Preparation captures no
// unique input/Operation and cannot destroy them on forwarder cancellation.
fn make_cpu_work(
    packet: Weak<InternalCpuRetirement<OperationOutput>>,
    control: CpuWorkControl,
) -> impl FnOnce() -> OperationResult + Send + 'static {
    move || {
        let packet = packet
            .upgrade()
            .ok_or_else(|| cpu_execution_failure("internal CPU job lost its original packet"))?;
        if let Some(failure) = control.failure() {
            return Err(failure);
        }
        let operation = packet
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed_operation
            .take()
            .expect("one real worker operation");
        if matches!(operation, InternalResultOperation::ProviderCreator) {
            let mut input = ProviderCreatorInputLoan::take(&packet);
            input
                .input
                .as_mut()
                .expect("same Provider creator input")
                .run_once();
            drop(input); // restore to the original packet, never Drop on CPU
            let window = packet
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .window
                .as_ref()
                .expect("same Provider source window")
                .clone();
            return Ok(InternalResultValue {
                value: Ok(InternalDomainSlot::ProviderCreatorFinished),
                window,
                activity: None,
            });
        }
        let input = packet
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed_input
            .take()
            .expect("one real domain worker input");
        // The original queued/running physical guard now covers this explicit
        // execution handoff. First-party apply and every Drop remain lock-free;
        // packet window/activity and opaque failure custody are independent.
        Ok(input.transform(move |slot, _| operation.apply(slot)))
    }
}

fn poll_cpu_offer(
    packet: &InternalCpuRetirement<OperationOutput>,
    executor: &QueryBlockingExecutor,
    factory: &InternalCpuFactoryBinding,
    original: &mut Option<QueryBlockingReservation>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<Result<bool, ExecutionFailureInput>> {
    use std::task::Poll;
    let mut lease = CpuActionLease::take(packet);
    let mut notification = None;
    let mut reserve_refusal = None;
    let result = (|| {
        let CpuActionState {
            admission,
            physical,
            ..
        } = lease.state();
        let mut offer = match admission
            .as_mut()
            .expect("same finite CPU request")
            .poll_local_offer(cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(failure)) => return Poll::Ready(Err(failure.error)),
            Poll::Ready(Ok(offer)) => offer,
        };
        if original.is_none() {
            match executor.try_reserve_internal_result_closed_factory(factory) {
                Ok(reservation) => *original = Some(reservation),
                Err(reason) => {
                    notification = Some(offer.reobserve_after_drop(original)?);
                    if reason == QueryBlockingReserveRefusal::Full {
                        return Poll::Ready(Ok(false));
                    }
                    reserve_refusal = Some(reason);
                    return Poll::Ready(Err(match reason {
                        QueryBlockingReserveRefusal::Closed => WorkError::Closed,
                        QueryBlockingReserveRefusal::FactoryMismatch => WorkError::Conflict,
                        QueryBlockingReserveRefusal::Full => unreachable!("handled backpressure"),
                    }));
                }
            }
        }
        match offer.accept_in(original, physical) {
            Ok(notify) => notification = Some(notify),
            Err(error) => {
                notification = Some(offer.reobserve_after_drop(original)?);
                return Poll::Ready(Err(error));
            }
        }
        Poll::Ready(Ok(true))
    })();
    drop(lease);
    if let Some(reason) = reserve_refusal {
        packet
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reserve_refusal = Some(reason);
    }
    // Stable original admission/physical phase precedes either notification.
    if let Some(notification) = notification {
        notification.notify();
    }
    result.map(|result| {
        result.map_err(|error| action_failure(packet, CpuActionFailureSite::LocalOffer, error))
    })
}

async fn acquire_cpu_offer(
    packet: &InternalCpuRetirement<OperationOutput>,
    executor: &QueryBlockingExecutor,
    factory: &InternalCpuFactoryBinding,
) -> Result<(), ExecutionFailureInput> {
    // This is an unstarted queue reservation, not an executing job. Dropping
    // the outer cancellation select withdraws the same semaphore wait/permit.
    let mut original = None;
    loop {
        if std::future::poll_fn(|cx| poll_cpu_offer(packet, executor, factory, &mut original, cx))
            .await?
        {
            return Ok(());
        }
        original = Some(
            executor
                .reserve_internal_result_closed_factory(factory)
                .await
                .map_err(|reason| {
                    let error = match reason {
                        QueryBlockingReserveRefusal::Closed => WorkError::Closed,
                        QueryBlockingReserveRefusal::FactoryMismatch => WorkError::Conflict,
                        QueryBlockingReserveRefusal::Full => {
                            unreachable!("waiting reservation cannot refuse temporary fullness")
                        }
                    };
                    action_failure(packet, CpuActionFailureSite::LocalOffer, error)
                })?,
        );
    }
}

// Publication borrows only the physical field: the original receipt must
// already remain in the packet while the row becomes visible to a worker.
struct CpuPublicationLease<'a> {
    packet: &'a InternalCpuRetirement<OperationOutput>,
    physical: Option<JointLocalGrant<QueryBlockingReservation>>,
}
impl<'a> CpuPublicationLease<'a> {
    fn take(packet: &'a InternalCpuRetirement<OperationOutput>) -> Self {
        let physical = packet
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .action
            .as_mut()
            .expect("same original CPU publication")
            .physical
            .take();
        Self { packet, physical }
    }
}
impl Drop for CpuPublicationLease<'_> {
    fn drop(&mut self) {
        let mut state = self.packet.state.lock().unwrap_or_else(|e| e.into_inner());
        let action = state
            .action
            .as_mut()
            .expect("same original CPU publication return");
        assert!(action.physical.is_none(), "one original physical publisher");
        action.physical = self.physical.take();
    }
}

fn publish_cpu_work(
    packet: &Arc<InternalCpuRetirement<OperationOutput>>,
    executor: &QueryBlockingExecutor,
    control: &CpuWorkControl,
) -> Result<(), ExecutionFailureInput> {
    let mut lease = CpuActionLease::take(packet);
    let state = lease.state();
    let work = make_cpu_work(Arc::downgrade(packet), control.clone());
    let prepared = match executor.prepare_preserving_in(
        state
            .physical
            .as_mut()
            .expect("accepted original CPU reservation"),
        work,
        &mut state.receipt,
    ) {
        Ok(prepared) => prepared,
        Err(refused) => {
            let reason = refused.reason();
            drop(refused.into_work());
            drop(lease);
            packet
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .publication_refusal = Some(reason);
            return Err(action_failure(
                packet,
                CpuActionFailureSite::PreparePublication,
                WorkError::Conflict,
            ));
        }
    };
    // Restore the receipt to the original packet before making its row
    // visible. The publication lease takes only the original physical field.
    drop(lease);
    let mut publication = CpuPublicationLease::take(packet);
    // This local frame owns only a Weak packet call, never its original input.
    let mut prepared = Some(prepared);
    let result = executor.publish_joint_in(&mut publication.physical, &mut prepared);
    if result.is_ok() {
        packet
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .action
            .as_mut()
            .expect("same published original CPU action")
            .published = true;
    }
    drop(prepared);
    drop(publication);
    match result {
        Ok(notification) => {
            notification.notify();
            Ok(())
        }
        Err(reason) => {
            packet
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .publication_refusal = Some(reason);
            Err(action_failure(
                packet,
                CpuActionFailureSite::Publish,
                match reason {
                    QueryBlockingPublishRefusal::Closed
                    | QueryBlockingPublishRefusal::ReceiptClosed => WorkError::Closed,
                    _ => WorkError::Conflict,
                },
            ))
        }
    }
}

// No queued F exists on this path. Discard its Weak call first (in the caller),
// then drop the accepted reservation outside locks; original business input
// stays in its packet through the same forwarder join and explicit retirement.
fn finish_unpublished(packet: &InternalCpuRetirement<OperationOutput>) {
    let mut lease = CpuActionLease::take(packet);
    assert!(!lease.state().published, "unpublished original CPU action");
    let admission = lease.state().admission.take();
    let physical = lease.state().physical.take();
    drop(lease);
    drop(admission);
    drop(physical);
}

fn poll_cpu_result(
    packet: &InternalCpuRetirement<OperationOutput>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<Result<OperationResult, QueryBlockingFailure>> {
    let mut lease = CpuActionLease::take(packet);
    lease
        .state()
        .receipt
        .as_mut()
        .expect("stable original worker receipt")
        .poll_result(cx)
}
fn poll_cpu_actual_exit(
    packet: &InternalCpuRetirement<OperationOutput>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<()> {
    use std::task::Poll;
    let mut lease = CpuActionLease::take(packet);
    let receipt = lease
        .state()
        .receipt
        .as_ref()
        .expect("same actual-exit observer");
    let result = receipt.poll_actual_exit(cx);
    let failure = if result.is_ready() {
        receipt.actual_exit_failure()
    } else {
        None
    };
    drop(lease);
    match result {
        Poll::Pending => Poll::Pending,
        Poll::Ready(result) => {
            let mut state = packet.state.lock().unwrap_or_else(|e| e.into_inner());
            state.physical_unproven = result.is_err();
            state.physical_failure = failure;
            Poll::Ready(())
        }
    }
}

// Execution and geometry use this exact production factory. The original
// packet already owns input/Operation before the forwarder can first poll.
async fn make_operation_forwarder(
    executor: QueryBlockingExecutor,
    factory: Option<InternalCpuFactoryBinding>,
    source: Option<InternalCpuActionSource>,
    control: CpuWorkControl,
    wake: Arc<dyn StatusIntakeWake>,
    packet: Arc<InternalCpuRetirement<OperationOutput>>,
    mut completed: oneshot::Sender<OperationResult>,
) {
    let Some(source) = source else {
        if control.requires_original_source() {
            finish_unpublished(&packet);
            let error = cpu_execution_failure("metadata obligation lost its original CPU source");
            if let Err(original) = completed.send(Err(error)) {
                packet
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .outcome = Some(original);
            }
            packet.notify(0, wake.as_ref());
            return;
        }
        // Ordinary non-Root work consumes the exact same registered closed
        // factory but receives no joint claim or finite proof exemption.
        make_closed_legacy_forwarder(executor, control, wake, packet, completed).await;
        return;
    };
    let setup = (|| {
        let quote = metadata_geometry::operation_action_quote(&executor)?;
        let admission = source
            .begin(quote.metadata_bytes, quote.physical_queue_bytes)
            .map_err(|failure| failure.error)?;
        packet
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .action
            .as_mut()
            .expect("same original CPU action")
            .admission = Some(admission);
        Ok::<(), WorkError>(())
    })();
    if let Err(error) = setup {
        let error = action_failure(&packet, CpuActionFailureSite::Begin, error);
        finish_unpublished(&packet);
        if let Err(original) = completed.send(Err(error)) {
            packet
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .outcome = Some(original);
        }
        packet.notify(0, wake.as_ref());
        return;
    }
    let factory = factory
        .as_ref()
        .expect("Root source captured from the same bound CPU owner");
    let admission = tokio::select! {
        biased;
        failure = control.stopped() => Err(failure),
        _ = completed.closed() => Err(cpu_execution_failure("internal CPU consumer closed before publication")),
        result = acquire_cpu_offer(&packet, &executor, factory) => result,
    };
    let admission = admission.and_then(|()| publish_cpu_work(&packet, &executor, &control));
    if let Err(error) = admission {
        finish_unpublished(&packet);
        if let Err(original) = completed.send(Err(error)) {
            packet
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .outcome = Some(original);
        }
        packet.notify(0, wake.as_ref());
        return;
    }
    let early = tokio::select! {
        biased;
        failure = control.stopped() => Err(failure),
        _ = completed.closed() => Err(cpu_execution_failure("internal result CPU consumer closed")),
        result = std::future::poll_fn(|cx| poll_cpu_result(&packet, cx)) => Ok(result),
    };
    match early {
        Ok(result) => {
            let result = packet.receive_work(result);
            if let Err(original) = completed.send(result) {
                packet
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .outcome = Some(original);
            }
            packet.notify(0, wake.as_ref());
        }
        Err(error) => {
            if let Err(Err(original)) = completed.send(Err(error)) {
                packet.state.lock().unwrap_or_else(|e| e.into_inner()).early = Some(original);
            }
            packet.notify(0, wake.as_ref());
            let result = std::future::poll_fn(|cx| poll_cpu_result(&packet, cx)).await;
            let result = packet.receive_work(result);
            packet
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .outcome = Some(result);
            packet.notify(1, wake.as_ref());
        }
    }
    // A result/forwarder receipt alone is not the original worker exit.
    // FailedUnproven retains the same original carrier in this same packet.
    std::future::poll_fn(|cx| poll_cpu_actual_exit(&packet, cx)).await;
}

async fn make_closed_legacy_forwarder(
    executor: QueryBlockingExecutor,
    control: CpuWorkControl,
    wake: Arc<dyn StatusIntakeWake>,
    packet: Arc<InternalCpuRetirement<OperationOutput>>,
    mut completed: oneshot::Sender<OperationResult>,
) {
    let work = executor.execute_preserving(make_cpu_work(Arc::downgrade(&packet), control.clone()));
    tokio::pin!(work);
    let early = tokio::select! {
        biased;
        failure = control.stopped() => Err(failure),
        _ = completed.closed() => Err(cpu_execution_failure("internal result CPU consumer closed")),
        result = &mut work => Ok(result),
    };
    match early {
        Ok(result) => {
            let result = packet.receive_work(result);
            if let Err(original) = completed.send(result) {
                packet
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .outcome = Some(original);
            }
            packet.notify(0, wake.as_ref());
        }
        Err(error) => {
            if let Err(Err(original)) = completed.send(Err(error)) {
                packet.state.lock().unwrap_or_else(|e| e.into_inner()).early = Some(original);
            }
            packet.notify(0, wake.as_ref());
            let result = packet.receive_work(work.await);
            packet
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .outcome = Some(result);
            packet.notify(1, wake.as_ref());
        }
    }
}

#[cfg(test)]
async fn make_forwarder<I, O, F>(
    executor: QueryBlockingExecutor,
    input: InternalResultValue<I>,
    cancellation: QueryCancellationView,
    wake: Arc<dyn StatusIntakeWake>,
    original_retirement: Arc<InternalCpuRetirement<O>>,
    mut completed: oneshot::Sender<Result<InternalResultValue<O>, ExecutionFailureInput>>,
    apply: F,
) where
    I: Send + 'static,
    O: Send + 'static,
    F: FnOnce(I, &ResultWindowAlias) -> O + Send + 'static,
{
    let worker_cancellation = cancellation.clone();
    let work = executor.execute_preserving(move || {
        if let Some(reason) = worker_cancellation.reason() {
            return Err(cpu_cancellation_failure(reason));
        }
        Ok(input.transform(apply))
    });
    tokio::pin!(work);
    let early_error = tokio::select! {
        biased;
        reason = cancellation.cancelled() => cpu_cancellation_failure(reason),
        _ = completed.closed() => cpu_execution_failure("internal result CPU consumer closed"),
        result = &mut work => {
            let result = original_retirement.receive_work(result);
            if let Err(original) = completed.send(result) {
                original_retirement.state.lock().unwrap_or_else(|e| e.into_inner()).outcome = Some(original);
            }
            original_retirement.notify(0, wake.as_ref());
            return;
        }
    };
    if let Err(Err(original)) = completed.send(Err(early_error)) {
        original_retirement
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .early = Some(original);
    }
    original_retirement.notify(0, wake.as_ref());
    // An early cancellation/closed receiver or notification panic is
    // not exit. Keep this exact preserving receipt until actual return.
    let late = original_retirement.receive_work(work.await);
    original_retirement
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .outcome = Some(late);
    original_retirement.notify(1, wake.as_ref());
}

#[cfg(test)]
pub(crate) fn admitted_internal_fixture() -> (
    novarocks_workload_control::WorkloadControl,
    novarocks_workload_control::RootWork,
    novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    novarocks_workload_control::ResultCapacityHandle,
) {
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1024 * 1024,
            control_bytes: 1024,
            per_scope_bytes: 1024 * 1024 - 1024,
        },
    )
    .unwrap();
    let capacity = control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    let (root, window) = control
        .root_admission()
        .try_begin_root_with_result(
            WorkRequest::new(WorkClass::Management),
            ResultWindowClass::Internal,
        )
        .unwrap();
    let binding =
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(
            &root.owner.scope(),
            window.retain_alias(),
        )
        .unwrap();
    drop(window);
    (control, root, binding, capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_execution::status_intake::TestIntakeWake;
    use novarocks_query_application::cancellation::{
        QueryCancellationReason, QueryCancellationSource,
    };
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use std::sync::mpsc;
    use std::time::Duration;

    fn control() -> (
        WorkloadControl,
        novarocks_workload_control::ResultCapacityHandle,
    ) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        (control, capacity)
    }

    async fn take<T: Send + 'static>(
        job: &mut InternalResultCpuJob<T>,
    ) -> Result<InternalResultValue<T>, ExecutionFailureInput> {
        tokio::time::timeout(
            Duration::from_secs(5),
            std::future::poll_fn(|cx| job.poll_take(cx)),
        )
        .await
        .expect("CPU outcome watchdog")
    }

    // These component tests use the original Root/factory fixture and the
    // production forwarder, not the unqualified generic closure bridge.
    struct RootCpuBackpressure {
        owner: InternalResultCpuOwner,
        control: WorkloadControl,
        root: novarocks_workload_control::RootWork,
        driver: novarocks_workload_control::joint_admission::JointProgressDriver,
        window: ResultWindowAlias,
    }
    impl RootCpuBackpressure {
        fn new() -> Self {
            let (fixture, payload) = Self::new_with_payload();
            drop(payload);
            fixture
        }
        fn new_with_payload() -> (
            Self,
            novarocks_workload_control::joint_admission::RetainedPayloadGrant,
        ) {
            use crate::query_execution::internal_consumer::domain_cpu::root_action_tests::{
                bind, paid_claim, root_control,
            };
            use novarocks_workload_control::joint_admission::{
                JointLane, JointResource, RetainedPayloadGrant,
            };
            let mut owner = InternalResultCpuOwner::new_workers(QueryBlockingExecutorConfig::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(2).unwrap(),
            ))
            .unwrap();
            let (control, handle, mut driver) = root_control(&mut owner);
            let root = control
                .try_begin_root(WorkRequest::new(WorkClass::Query))
                .unwrap();
            let scope = root.owner.scope();
            bind(&scope, &mut driver);
            let mut raw = Some(paid_claim(
                &handle,
                &root.owner,
                JointLane::Custody,
                JointResource::PayloadBytes,
                INTERNAL_PEAK_BYTES,
                &mut driver,
            ));
            let mut payload = None;
            RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
                .unwrap()
                .notify();
            let window =
                ResultWindowAlias::from_internal_payload(&scope, &scope, payload.as_ref().unwrap())
                    .unwrap();
            // The alias keeps this same positive payload, without a new window.
            let payload = payload.unwrap();
            (
                Self {
                    owner,
                    control,
                    root,
                    driver,
                    window,
                },
                payload,
            )
        }
        fn drive(&mut self) {
            use crate::query_execution::internal_consumer::domain_cpu::root_action_tests::drive;
            self.driver.recheck_retired_backing();
            drive(&mut self.driver);
        }
        async fn close(self) {
            let Self {
                mut owner,
                control,
                root,
                driver,
                window,
            } = self;
            drop(window);
            root.owner.complete();
            root.business.release();
            drop(driver);
            drop(control);
            owner
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
        }
        async fn close_after_terminal_cancel_settled(self) {
            let Self {
                mut owner,
                control,
                root,
                driver,
                window,
            } = self;
            drop(window);
            root.owner.complete_after_terminal_cancel_settled();
            root.business.release();
            drop(driver);
            drop(control);
            owner
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
        }
    }

    struct MetadataCanary {
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
        barrier: std::sync::Mutex<Option<(oneshot::Sender<()>, mpsc::Receiver<()>)>>,
    }
    impl novarocks_spi::connector::ProviderMetadataCpuSource for MetadataCanary {
        fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
            novarocks_spi::connector::ProviderBindingEpoch::from_bytes([19; 16])
        }
        fn run_metadata(&self) {
            use std::sync::atomic::Ordering;
            self.calls.fetch_add(1, Ordering::AcqRel);
            let barrier = self.barrier.lock().unwrap().take();
            if let Some((entered, release)) = barrier {
                let _ = entered.send(());
                // A finite test-only stop inside this concrete input phase,
                // never a production Source or arbitrary closure qualification.
                release.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        }
    }
    impl Drop for MetadataCanary {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
    }
    struct MetadataCanaryObservation {
        original: Weak<dyn novarocks_spi::connector::ProviderMetadataCpuSource>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    fn metadata_canary(
        barrier: Option<(oneshot::Sender<()>, mpsc::Receiver<()>)>,
    ) -> (
        novarocks_spi::connector::ProviderMetadataCpuInput,
        MetadataCanaryObservation,
    ) {
        use novarocks_spi::connector::{
            ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
            ProviderMetadataCpuInput, ProviderMetadataCpuSource,
        };
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let original: Arc<dyn ProviderMetadataCpuSource> = Arc::new(MetadataCanary {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("cpu-backpressure-component").unwrap(),
            },
            calls: Arc::clone(&calls),
            drops: Arc::clone(&drops),
            barrier: std::sync::Mutex::new(barrier),
        });
        let observation = MetadataCanaryObservation {
            original: Arc::downgrade(&original),
            calls,
            drops,
        };
        (ProviderMetadataCpuInput(original), observation)
    }
    type OriginalCpuForwarder = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
    fn original_metadata_forwarder(
        fixture: &RootCpuBackpressure,
        input: novarocks_spi::connector::ProviderMetadataCpuInput,
        cancellation: &QueryCancellationSource,
    ) -> (
        Arc<InternalCpuRetirement<OperationOutput>>,
        OriginalCpuForwarder,
        oneshot::Receiver<OperationResult>,
    ) {
        let source = fixture
            .owner
            .runtime()
            .provider_creator_source(&fixture.root.owner.scope())
            .unwrap();
        original_metadata_forwarder_controlled(fixture, input, source, cancellation.view().into())
    }
    fn original_metadata_forwarder_controlled(
        fixture: &RootCpuBackpressure,
        input: novarocks_spi::connector::ProviderMetadataCpuInput,
        source: InternalCpuActionSource,
        control: CpuWorkControl,
    ) -> (
        Arc<InternalCpuRetirement<OperationOutput>>,
        OriginalCpuForwarder,
        oneshot::Receiver<OperationResult>,
    ) {
        let cpu = fixture.owner.runtime();
        let packet = Arc::new(InternalCpuRetirement::new(fixture.window.clone(), None));
        {
            let mut state = packet.state.lock().unwrap();
            state.closed_operation = Some(InternalResultOperation::ProviderCreator);
            state.provider_input = Some(Box::new(ClosedCreatorInput::ProviderMetadata(input)));
            state.action = Some(CpuActionState::default());
        }
        let (completed, completion) = oneshot::channel();
        let forwarder: OriginalCpuForwarder = Box::pin(make_operation_forwarder(
            cpu.executor.clone(),
            cpu.factory.clone(),
            Some(source),
            control,
            Arc::new(TestIntakeWake::default()),
            Arc::clone(&packet),
            completed,
        ));
        (packet, forwarder, completion)
    }
    fn poll_original_forwarder(forwarder: &mut OriginalCpuForwarder) -> std::task::Poll<()> {
        std::future::Future::poll(
            forwarder.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop()),
        )
    }
    fn assert_original_metadata_input(
        packet: &InternalCpuRetirement<OperationOutput>,
        observation: &MetadataCanaryObservation,
    ) {
        let state = packet.state.lock().unwrap();
        let ClosedCreatorInput::ProviderMetadata(input) = state.provider_input.as_deref().unwrap()
        else {
            panic!("the original closed metadata input was replaced")
        };
        assert!(Weak::ptr_eq(
            &Arc::downgrade(&input.0),
            &observation.original
        ));
        assert!(state.admission_failure.is_none());
        assert!(
            state.reserve_refusal.is_none(),
            "Full is backpressure, not Resource"
        );
        assert!(state.publication_refusal.is_none());
    }
    async fn finish_original_forwarder(
        fixture: &mut RootCpuBackpressure,
        forwarder: &mut OriginalCpuForwarder,
    ) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                fixture.drive();
                if poll_original_forwarder(forwarder).is_ready() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("original forwarder/worker actual-exit watchdog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn positive_metadata_obligation_uses_same_queue_after_root_cancel_and_keeps_finite_control()
     {
        use novarocks_spi::connector::{
            ConnectorOperationControl, ConnectorRequestContext, ConnectorStopOwner,
        };
        use std::sync::atomic::Ordering;
        for stop_obligation in [false, true] {
            let (mut fixture, payload) = RootCpuBackpressure::new_with_payload();
            let cpu = fixture.owner.runtime();
            let factory = cpu.factory.as_ref().unwrap();
            let mut held = Vec::new();
            for _ in 0..cpu.configured_capacity().queue_capacity().get() {
                held.push(
                    cpu.executor
                        .try_reserve_internal_result_closed_factory(factory)
                        .unwrap(),
                );
            }
            let business = QueryCancellationSource::new();
            business.request(QueryCancellationReason::ExecutionCancellationRequested);
            assert!(business.view().is_cancelled());
            fixture
                .root
                .owner
                .cancel(novarocks_workload_control::CancellationReason::ServerShutdown);
            fixture.control.close_admission();
            assert!(
                cpu.provider_creator_source(&fixture.root.owner.scope())
                    .is_err()
            );
            let source = factory
                .provider_metadata_continuation_source(&payload, &fixture.root.owner.scope())
                .unwrap();
            let initial_stop = ConnectorStopOwner::new();
            let obligation_stop = ConnectorStopOwner::new();
            let initial = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(10),
                initial_stop.view(),
                1024,
                4096,
            )
            .unwrap();
            initial_stop.request_stop();
            assert!(initial.check_active().is_err());
            let obligation = initial.for_existing_obligation(
                Instant::now() + Duration::from_secs(10),
                obligation_stop.view(),
            );
            let (input, observation) = metadata_canary(None);
            let (packet, mut forwarder, mut completion) = original_metadata_forwarder_controlled(
                &fixture,
                input,
                source,
                CpuWorkControl::existing_metadata_obligation(&obligation),
            );
            assert!(poll_original_forwarder(&mut forwarder).is_pending());
            fixture.drive();
            assert!(poll_original_forwarder(&mut forwarder).is_pending());
            assert_original_metadata_input(&packet, &observation);
            assert_eq!(observation.calls.load(Ordering::Acquire), 0);
            assert!(matches!(
                completion.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            if stop_obligation {
                obligation_stop.request_stop();
                assert!(poll_original_forwarder(&mut forwarder).is_ready());
                let failure = completion
                    .try_recv()
                    .unwrap()
                    .err()
                    .expect("the obligation's own finite stop still refuses unstarted CPU");
                assert_eq!(
                    failure.fact(),
                    obligation.check_active().unwrap_err().failure_fact()
                );
                assert_eq!(observation.calls.load(Ordering::Acquire), 0);
                assert!(
                    packet
                        .state
                        .lock()
                        .unwrap()
                        .action
                        .as_ref()
                        .unwrap()
                        .receipt
                        .is_none()
                );
            } else {
                drop(held.pop());
                finish_original_forwarder(&mut fixture, &mut forwarder).await;
                let output = completion.try_recv().unwrap().unwrap();
                assert!(matches!(
                    output.value(),
                    Ok(InternalDomainSlot::ProviderCreatorFinished)
                ));
                assert_eq!(observation.calls.load(Ordering::Acquire), 1);
                assert!(
                    poll_cpu_actual_exit(
                        &packet,
                        &mut std::task::Context::from_waker(std::task::Waker::noop())
                    )
                    .is_ready()
                );
                assert!(!packet.has_failure());
                drop(output);
            }
            assert_original_metadata_input(&packet, &observation);
            drop(forwarder);
            drop(packet);
            assert!(observation.original.upgrade().is_none());
            assert_eq!(observation.drops.load(Ordering::Acquire), 1);
            drop(held);
            drop(payload);
            fixture.close_after_terminal_cancel_settled().await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bound_root_forwarder_keeps_original_input_across_full_queue_and_actual_exit() {
        use std::sync::atomic::Ordering;
        let mut fixture = RootCpuBackpressure::new();
        let cpu = fixture.owner.runtime();
        let factory = cpu.factory.as_ref().unwrap();
        let mut held = Vec::new();
        for _ in 0..cpu.configured_capacity().queue_capacity().get() {
            held.push(
                cpu.executor
                    .try_reserve_internal_result_closed_factory(factory)
                    .unwrap(),
            );
        }
        assert!(matches!(
            cpu.executor
                .try_reserve_internal_result_closed_factory(factory),
            Err(QueryBlockingReserveRefusal::Full)
        ));
        let cancellation = QueryCancellationSource::new();
        let (input, observation) = metadata_canary(None);
        let (packet, mut forwarder, mut completion) =
            original_metadata_forwarder(&fixture, input, &cancellation);
        assert!(poll_original_forwarder(&mut forwarder).is_pending());
        fixture.drive();
        assert!(poll_original_forwarder(&mut forwarder).is_pending());
        assert_original_metadata_input(&packet, &observation);
        {
            let state = packet.state.lock().unwrap();
            let action = state.action.as_ref().unwrap();
            // reobserve_after_drop keeps the original admission; no new Source
            // or request is constructed by the semaphore wait/retry loop.
            assert!(action.admission.is_some());
            assert!(!action.published && action.receipt.is_none() && action.physical.is_none());
        }
        assert!(matches!(
            completion.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(observation.calls.load(Ordering::Acquire), 0);
        assert_eq!(observation.drops.load(Ordering::Acquire), 0);
        drop(held.pop());
        // Do not run the WC driver: the exact original reservation is now
        // acquired, but the original rolled-back request has not regranted.
        assert!(poll_original_forwarder(&mut forwarder).is_pending());
        assert!(matches!(
            cpu.executor
                .try_reserve_internal_result_closed_factory(factory),
            Err(QueryBlockingReserveRefusal::Full)
        ));
        assert_original_metadata_input(&packet, &observation);
        finish_original_forwarder(&mut fixture, &mut forwarder).await;
        let output = completion.try_recv().unwrap().unwrap();
        assert!(matches!(
            output.value(),
            Ok(InternalDomainSlot::ProviderCreatorFinished)
        ));
        assert!(
            poll_cpu_actual_exit(
                &packet,
                &mut std::task::Context::from_waker(std::task::Waker::noop())
            )
            .is_ready()
        );
        assert_original_metadata_input(&packet, &observation);
        assert_eq!(observation.calls.load(Ordering::Acquire), 1);
        assert!(!packet.has_failure());
        drop(output);
        drop(forwarder);
        drop(packet);
        assert!(observation.original.upgrade().is_none());
        assert_eq!(observation.drops.load(Ordering::Acquire), 1);
        drop(held);
        fixture.close().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bound_root_forwarder_cancel_after_reserve_does_not_publish_or_retire_other_work() {
        use std::sync::atomic::Ordering;
        let mut fixture = RootCpuBackpressure::new();
        let cpu = fixture.owner.runtime();
        let factory = cpu.factory.as_ref().unwrap();
        let (entered, started) = oneshot::channel();
        let (release, blocked) = mpsc::channel();
        let other_cancellation = QueryCancellationSource::new();
        let (other_input, other) = metadata_canary(Some((entered, blocked)));
        let (other_packet, mut other_forwarder, mut other_completion) =
            original_metadata_forwarder(&fixture, other_input, &other_cancellation);
        assert!(poll_original_forwarder(&mut other_forwarder).is_pending());
        fixture.drive();
        assert!(poll_original_forwarder(&mut other_forwarder).is_pending());
        tokio::time::timeout(Duration::from_secs(5), started)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(other.calls.load(Ordering::Acquire), 1);
        // Dequeue returns the original queue permit even while the same
        // published call and its physical/actual-exit responsibility remain.
        let mut held = vec![
            cpu.executor
                .try_reserve_internal_result_closed_factory(factory)
                .expect("running call must no longer consume a queue reservation"),
        ];
        assert!(
            poll_cpu_actual_exit(
                &other_packet,
                &mut std::task::Context::from_waker(std::task::Waker::noop()),
            )
            .is_pending()
        );
        for _ in 1..cpu.configured_capacity().queue_capacity().get() {
            held.push(
                cpu.executor
                    .try_reserve_internal_result_closed_factory(factory)
                    .unwrap(),
            );
        }
        assert!(matches!(
            cpu.executor
                .try_reserve_internal_result_closed_factory(factory),
            Err(QueryBlockingReserveRefusal::Full)
        ));
        let cancellation = QueryCancellationSource::new();
        let (input, cancelled) = metadata_canary(None);
        let (packet, mut forwarder, mut completion) =
            original_metadata_forwarder(&fixture, input, &cancellation);
        assert!(poll_original_forwarder(&mut forwarder).is_pending());
        fixture.drive();
        assert!(poll_original_forwarder(&mut forwarder).is_pending());
        assert_original_metadata_input(&packet, &cancelled);
        drop(held.pop());
        // Acquire the permit, then pause proof progress before any publication.
        assert!(poll_original_forwarder(&mut forwarder).is_pending());
        assert!(matches!(
            cpu.executor
                .try_reserve_internal_result_closed_factory(factory),
            Err(QueryBlockingReserveRefusal::Full)
        ));
        cancellation.request(QueryCancellationReason::ExecutionCancellationRequested);
        assert!(poll_original_forwarder(&mut forwarder).is_ready());
        let result = completion.try_recv().unwrap();
        assert!(matches!(result, Err(ref failure)
            if failure.fact() == QueryFailureReason::Cancelled.failure_fact()));
        assert_original_metadata_input(&packet, &cancelled);
        {
            let state = packet.state.lock().unwrap();
            let action = state.action.as_ref().unwrap();
            assert!(!action.published && action.receipt.is_none());
            assert!(action.admission.is_none() && action.physical.is_none());
        }
        assert_eq!(cancelled.calls.load(Ordering::Acquire), 0);
        assert_eq!(cancelled.drops.load(Ordering::Acquire), 0);
        // Exactly the cancelled unstarted reservation returned; the remaining
        // independently held reservations still fill the rest of the queue.
        // The other published job keeps its distinct actual-exit responsibility.
        let returned = cpu
            .executor
            .try_reserve_internal_result_closed_factory(factory)
            .unwrap();
        assert!(matches!(
            cpu.executor
                .try_reserve_internal_result_closed_factory(factory),
            Err(QueryBlockingReserveRefusal::Full)
        ));
        assert!(
            poll_cpu_actual_exit(
                &other_packet,
                &mut std::task::Context::from_waker(std::task::Waker::noop())
            )
            .is_pending()
        );
        assert!(matches!(
            other_completion.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(other.drops.load(Ordering::Acquire), 0);
        drop(returned);
        drop(held);
        let _ = release.send(());
        finish_original_forwarder(&mut fixture, &mut other_forwarder).await;
        let other_output = other_completion.try_recv().unwrap().unwrap();
        assert!(matches!(
            other_output.value(),
            Ok(InternalDomainSlot::ProviderCreatorFinished)
        ));
        assert_eq!(other.calls.load(Ordering::Acquire), 1);
        assert_original_metadata_input(&other_packet, &other);
        assert!(!packet.has_failure() && !other_packet.has_failure());
        drop(result);
        drop(other_output);
        drop(forwarder);
        drop(other_forwarder);
        drop(packet);
        drop(other_packet);
        assert_eq!(cancelled.drops.load(Ordering::Acquire), 1);
        assert_eq!(other.drops.load(Ordering::Acquire), 1);
        fixture.close().await;
    }

    #[test]
    fn statistics_action_detail_requests_one_bounded_utf8_backing() {
        const TEXT: &str = concat!(
            "界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界",
            "界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界",
            "界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界",
        );
        let detail =
            cpu_action_failure_detail(CpuActionFailureSite::Begin, &WorkError::Capacity(TEXT));
        assert!(detail.as_str().contains("Begin"));
        assert!(detail.as_str().len() <= CPU_ACTION_DETAIL_BYTES);
        assert!(detail.as_str().len() > CPU_ACTION_DETAIL_BYTES - 4);
        assert!(std::str::from_utf8(detail.as_str().as_bytes()).is_ok());
    }

    #[test]
    fn original_action_refusal_keeps_typed_axis_detail_and_first_packet_error() {
        let (control, capacity) = control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let packet = InternalCpuRetirement::<OperationOutput>::new(window.retain_alias(), None);
        let refusal = WorkError::Capacity("original result payload bytes");
        let marker = action_failure(&packet, CpuActionFailureSite::LocalOffer, refusal.clone());
        assert_eq!(marker.fact(), refusal.failure_fact());
        assert_eq!(
            marker.detail().as_str(),
            "internal CPU action admission failed"
        );
        let detail = cpu_action_failure_detail(CpuActionFailureSite::LocalOffer, &refusal);
        assert!(detail.as_str().contains("LocalOffer"));
        assert!(detail.as_str().contains("original result payload bytes"));
        assert!(
            !packet.has_failure(),
            "ordinary capacity is not process health"
        );
        let later = action_failure(
            &packet,
            CpuActionFailureSite::Begin,
            WorkError::Capacity("original executor queue"),
        );
        assert_eq!(
            later.detail().as_str(),
            "internal CPU action admission failed"
        );
        assert_eq!(
            packet.state.lock().unwrap().admission_failure,
            Some(refusal)
        );
        drop(window);
        assert_eq!(capacity.snapshot().held_positions[2], 1);
        drop(packet);
        assert_eq!(capacity.snapshot().held_positions[2], 0);
        root.owner.complete();
        root.business.release();
    }

    #[tokio::test]
    async fn result_windows_and_actual_cpu_geometry_are_independent() {
        let profile = FrontendResultCapacityProfile::try_new(ResultCapacityConfig {
            positions: [320, 16, 1, 64],
            all_objects_bytes: [
                8 * 1024 * 1024,
                96 * 1024 * 1024,
                INTERNAL_PEAK_BYTES,
                8 * 1024 * 1024,
            ],
            ..ResultCapacityConfig::V1
        })
        .unwrap();
        let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        let capacity = control
            .configure_result_capacity(profile.declared())
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let window = capacity
            .try_acquire(&root.owner.scope(), ResultWindowClass::Internal)
            .unwrap();
        assert!(matches!(
            capacity.try_acquire(&root.owner.scope(), ResultWindowClass::Internal),
            Err(novarocks_workload_control::WorkError::Capacity(_))
        ));
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        assert!(
            window
                .retain_alias()
                .check_backing_total(INTERNAL_PEAK_BYTES)
                .is_ok()
        );
        assert!(
            window
                .retain_alias()
                .check_backing_total(INTERNAL_PEAK_BYTES + 1)
                .is_err()
        );
        let mut owner = InternalResultCpuOwner::new_workers(QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(3).unwrap(),
        ))
        .unwrap();
        let installed = owner.runtime().executor.configured_capacity();
        assert_eq!(installed.worker_threads().get(), 2);
        assert_eq!(installed.queue_capacity().get(), 3);
        drop(window);
        root.owner.complete();
        root.business.release();
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[test]
    fn invalid_result_declarations_fail_but_small_actual_budgets_are_supported() {
        for (positions, bytes) in [(0, 64), (1, 0)] {
            let mut capacity = ResultCapacityConfig::V1;
            capacity.positions[2] = positions;
            capacity.all_objects_bytes[2] = bytes;
            assert!(FrontendResultCapacityProfile::try_new(capacity).is_err());
        }
        let mut capacity = ResultCapacityConfig::V1;
        capacity.positions[2] = 1;
        capacity.all_objects_bytes[2] = 64;
        assert!(FrontendResultCapacityProfile::try_new(capacity).is_ok());
    }

    #[test]
    fn foreign_class_refuses_before_constructor_and_small_original_budget_is_supported() {
        let (control, _) = control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let result = InternalResultValue::<()>::try_produce(
            &root.owner.scope(),
            window.retain_alias(),
            |_| {
                panic!("a refused class must not construct a graph");
            },
        );
        assert!(result.is_err());
        let (foreign_control, _) = self::control();
        let (foreign_root, foreign_window) = foreign_control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        assert!(
            InternalResultValue::<()>::try_produce(
                &root.owner.scope(),
                foreign_window.retain_alias(),
                |_| {
                    panic!("foreign host capacity must refuse before growth");
                }
            )
            .is_err()
        );
        drop(foreign_window);
        foreign_root.owner.complete();
        foreign_root.business.release();
        drop(window);
        root.owner.complete();
        root.business.release();

        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 64,
                per_scope_bytes: 960,
            },
        )
        .unwrap();
        control
            .configure_result_capacity(ResultCapacityConfig {
                all_objects_bytes: [64; 4],
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        control.mark_ready().unwrap();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let value = InternalResultValue::<()>::try_produce(
            &root.owner.scope(),
            window.retain_alias(),
            |_| Ok(()),
        )
        .expect("small actual results have no universal construction floor");
        drop(value);
        drop(window);
        root.owner.complete();
        root.business.release();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn domain_cpu_does_not_block_control_and_result_retains_the_original_window() {
        let (control, capacity) = control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let (entered, started) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let input =
            InternalResultValue::try_produce(&root.owner.scope(), window.retain_alias(), |_| {
                Ok(vec![7])
            })
            .unwrap();
        let cancellation = QueryCancellationSource::new();
        let mut job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(TestIntakeWake::default()),
            move |input, _| {
                entered.send(()).unwrap();
                blocked.recv().unwrap();
                input
            },
        );
        drop(window);
        root.owner.complete();
        root.business.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while started.try_recv().is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(tokio::spawn(async { 11 }).await.unwrap(), 11);
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        release.send(()).unwrap();
        let result = take(&mut job).await.unwrap();
        assert_eq!(result.value(), &vec![7]);
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(result);
        drop(job);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unclaimed_success_keeps_capacity_and_drops_payload_before_its_window() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Notice {
            capacity: novarocks_workload_control::ResultCapacityHandle,
            dropped: Arc<AtomicBool>,
        }
        impl Drop for Notice {
            fn drop(&mut self) {
                assert_eq!(self.capacity.snapshot().held_positions, [0, 0, 1, 0]);
                self.dropped.store(true, Ordering::Release);
            }
        }
        struct Wake(Arc<AtomicBool>);
        impl std::fmt::Debug for Wake {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("Wake")
            }
        }
        impl StatusIntakeWake for Wake {
            fn wake(&self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let (control, capacity) = control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let dropped = Arc::new(AtomicBool::new(false));
        let input =
            InternalResultValue::try_produce(&root.owner.scope(), window.retain_alias(), |_| {
                Ok(Notice {
                    capacity: capacity.clone(),
                    dropped: Arc::clone(&dropped),
                })
            })
            .unwrap();
        let published = Arc::new(AtomicBool::new(false));
        let cancellation = QueryCancellationSource::new();
        let job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(Wake(Arc::clone(&published))),
            |input, _| input,
        );
        drop(window);
        root.owner.complete();
        root.business.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !published.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!dropped.load(Ordering::Acquire));
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(job);
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_waiter_keeps_running_input_until_actual_worker_exit() {
        let (control, capacity) = control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let binding = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias()).unwrap();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let (entered, started) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let input =
            InternalResultValue::try_produce(&root.owner.scope(), window.retain_alias(), |_| {
                Ok(vec![1])
            })
            .unwrap()
            .track_actual_exit(&binding)
            .unwrap();
        let cancellation = QueryCancellationSource::new();
        let mut job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(TestIntakeWake::default()),
            move |input, _| {
                entered.send(()).unwrap();
                blocked.recv().unwrap();
                input
            },
        );
        drop(window);
        root.owner.complete();
        root.business.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while started.try_recv().is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancellation.request(QueryCancellationReason::ExplicitKill {
            requester_session: novarocks_query_application::session_control::ClientSessionKey::new(
                1,
            )
            .expect("nonzero fixture requester"),
        });
        assert!(take(&mut job).await.is_err());
        drop(job);
        let (fence_done, exited) = mpsc::channel();
        let fence = std::thread::spawn(move || {
            binding.wait_result_activities_exited();
            drop(binding);
            fence_done.send(()).unwrap();
        });
        assert!(matches!(exited.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        release.send(()).unwrap();
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        // The original forwarder retires the worker receipt on this runtime.
        // Waiting for its activity fence must leave that runtime able to poll it.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match exited.try_recv() {
                    Ok(()) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("result activity fence exited without its observation")
                    }
                }
            }
        })
        .await
        .unwrap();
        fence.join().unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}

#[cfg(test)]
mod actual_close_tests {
    use super::*;
    use novarocks_query_application::cancellation::{
        QueryCancellationReason, QueryCancellationSource,
    };
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::Duration,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn early_cancel_and_explicit_close_keep_original_cpu_join_until_payload_exit() {
        close_case(None).await;
        for reason in [
            QueryCancellationReason::ExplicitKill {
                requester_session:
                    novarocks_query_application::session_control::ClientSessionKey::new(1)
                        .expect("nonzero fixture requester"),
            },
            QueryCancellationReason::DeadlineExceeded { timeout_ms: 91 },
            QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms: 92 },
        ] {
            close_case(Some(reason)).await;
        }
    }

    async fn close_case(cancel: Option<QueryCancellationReason>) {
        let explicit_close = cancel.is_none();
        let expected_reason = match &cancel {
            Some(
                QueryCancellationReason::DeadlineExceeded { .. }
                | QueryCancellationReason::FrontendDrainDeadlineExceeded { .. },
            ) => QueryFailureReason::DeadlineExceeded,
            _ => QueryFailureReason::Cancelled,
        };
        struct Payload(Arc<AtomicBool>);
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let dropped = Arc::new(AtomicBool::new(false));
        let input =
            InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| {
                Ok(Payload(Arc::clone(&dropped)))
            })
            .unwrap()
            .track_actual_exit(&binding)
            .unwrap();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let (entered, started) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let cancellation = QueryCancellationSource::new();
        let mut job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(crate::task_execution::status_intake::TestIntakeWake::default()),
            move |input, _| {
                entered.send(()).unwrap();
                let _ = blocked.recv();
                input
            },
        );
        root.owner.complete();
        root.business.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while started.try_recv().is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // This is the real loop race: its control observation was still clear,
        // then cancellation arrives before the owner claims the CPU outcome.
        let control_was_clear = !cancellation.view().is_cancelled();
        let early = if explicit_close {
            job.begin_close();
            Ok(None)
        } else {
            cancellation.request(cancel.expect("explicit control reason"));
            tokio::time::timeout(Duration::from_secs(5), async {
                std::future::poll_fn(|cx| job.poll_take(cx)).await.err()
            })
            .await
        };
        job.begin_close();
        let still_joining = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(job.poll_forwarder_closed(cx).is_pending())
        })
        .await;
        let mut first_waiter = Box::pin(binding.result_activities_exited());
        let mut surviving_waiter = Box::pin(binding.result_activities_exited());
        let waiting = std::future::poll_fn(|cx| {
            use std::future::Future;
            std::task::Poll::Ready(
                first_waiter.as_mut().poll(cx).is_pending()
                    && surviving_waiter.as_mut().poll(cx).is_pending(),
            )
        })
        .await;
        drop(first_waiter);
        let held =
            capacity.snapshot().held_positions == [0, 0, 1, 0] && !dropped.load(Ordering::Acquire);
        // Release and join all real workers before asserting the observations.
        release.send(()).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(5), job.join_closed())
            .await
            .unwrap()
            .is_ok();
        tokio::time::timeout(Duration::from_secs(5), surviving_waiter)
            .await
            .unwrap();
        binding.result_activities_exited().await;
        let dropped_at_join = dropped.load(Ordering::Acquire);
        let no_join_failure = job.join_failure().is_none();
        drop(job);
        drop(binding);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let early = early.unwrap();
        if explicit_close {
            assert!(early.is_none());
        } else {
            let input = early.expect("the original statement control authors the CPU result");
            assert_eq!(input.fact(), expected_reason.failure_fact());
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                input.detail().as_str(),
                "internal result CPU was stopped by its statement control owner"
            );
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (1317, "70100")
            );
            assert_eq!(
                input.user_error().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
        }
        assert!(control_was_clear);
        assert!(still_joining && waiting && held);
        assert!(joined && dropped_at_join && no_join_failure);
        // Successful joins have no failure to hand to process health.
        // `job` was already disposed after the actual close above.
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}

#[cfg(test)]
mod original_failure_tests {
    use super::*;
    use novarocks_query_application::cancellation::{
        QueryCancellationReason, QueryCancellationSource,
    };
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        time::Duration,
    };

    struct OriginalWorkerPanic {
        drops: Arc<AtomicUsize>,
        _original_window: ResultWindowAlias,
    }
    impl Drop for OriginalWorkerPanic {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }
    struct OriginalNotificationPanic(Arc<AtomicUsize>);
    impl Drop for OriginalNotificationPanic {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[derive(Debug)]
    struct Wake {
        panic: bool,
        drops: Arc<AtomicUsize>,
    }
    impl StatusIntakeWake for Wake {
        fn wake(&self) {
            if self.panic {
                std::panic::panic_any(OriginalNotificationPanic(Arc::clone(&self.drops)));
            }
        }
    }
    fn original_runtime() -> FrontendDataRuntime {
        use novarocks_native_trust::{
            DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust,
            ValidatedSharedSecret,
        };
        let trust = NativeTrust::new(
            DeploymentId::parse("internal-cpu-original-failure").unwrap(),
            ValidatedSharedSecret::new(novarocks_secret::SecretValue::new(
                "0123456789abcdef0123456789abcdef",
            ))
            .unwrap(),
            NativeCallerSubject::parse("fe@127.0.0.1:19040").unwrap(),
            NativeTransportMode::Disabled,
        );
        // No subscription/reaper service and no socket is started by this owner.
        FrontendDataRuntime::new_with_native_trust(
            tokio::runtime::Handle::current(),
            Arc::new(trust),
            novarocks_native_adapter::FrontendNativeTransport::plaintext(),
            novarocks_task_codec::TransportBudget::DEFAULT,
            8,
        )
        .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn original_worker_and_both_actual_notification_causes_reach_health_custody() {
        for early_cancel in [false, true] {
            let (_control, root, binding, capacity) = admitted_internal_fixture();
            let worker_drops = Arc::new(AtomicUsize::new(0));
            let wake_drops = Arc::new(AtomicUsize::new(0));
            let input =
                InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| {
                    Ok(())
                })
                .unwrap()
                .track_actual_exit(&binding)
                .unwrap();
            let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
            let runtime = original_runtime();
            let cancellation = QueryCancellationSource::new();
            let (entered, started) = mpsc::channel();
            let (release, blocked) = mpsc::channel();
            let original_drops = Arc::clone(&worker_drops);
            let mut job: InternalResultCpuJob<()> = owner.runtime().submit(
                input,
                cancellation.view(),
                &runtime,
                Arc::new(Wake {
                    panic: early_cancel,
                    drops: Arc::clone(&wake_drops),
                }),
                move |(), window| {
                    let _ = entered.send(());
                    let _ = blocked.recv();
                    std::panic::panic_any(OriginalWorkerPanic {
                        drops: original_drops,
                        _original_window: window.clone(),
                    });
                },
            );
            let entered = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match started.try_recv() {
                        Ok(()) => break true,
                        Err(mpsc::TryRecvError::Disconnected) => break false,
                        Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    }
                }
            })
            .await;
            if early_cancel {
                cancellation.request(QueryCancellationReason::ExplicitKill {
                    requester_session:
                        novarocks_query_application::session_control::ClientSessionKey::new(1)
                            .expect("nonzero fixture requester"),
                });
            } else {
                let _ = release.send(());
            }
            let outcome = tokio::time::timeout(
                Duration::from_secs(5),
                std::future::poll_fn(|cx| job.poll_take(cx)),
            )
            .await;
            // Release even if an observation above failed. No assertion can
            // strand the real worker before its actual receipt/forwarder join.
            if early_cancel {
                let _ = release.send(());
            }
            drop(release);
            let joined = tokio::time::timeout(Duration::from_secs(5), job.join_closed()).await;
            let (worker_is_original, both_wakes, late_retained) = {
                let packet = job
                    .retirement
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                (
                    packet
                        .worker
                        .as_ref()
                        .is_some_and(|cause| cause.original_payload_is::<OriginalWorkerPanic>()),
                    packet
                        .notifications
                        .iter()
                        .filter(|cause| {
                            cause.as_ref().is_some_and(|cause| {
                                cause.original_payload_is::<OriginalNotificationPanic>()
                            })
                        })
                        .count(),
                    packet.outcome.is_some(),
                )
            };
            let no_forwarder_panic = job.join_failure().is_none();
            let health = job.health_failure().map(|original|
                novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "Original Internal CPU failure remains in custody", original));
            let abnormal_join = matches!(&joined, Ok(Err(_)));
            drop(joined);
            drop(outcome);
            drop(job);
            root.owner.complete();
            root.business.release();
            drop(binding);
            let shutdown = owner
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await;
            let held_by_health = capacity.snapshot().held_positions == [0, 0, 1, 0]
                && worker_drops.load(Ordering::Acquire) == 0
                && wake_drops.load(Ordering::Acquire) == 0;
            let has_health = health.is_some();
            // This explicit receiver disposition is the last original cause
            // alias. Safe error presentation never consumes the packet.
            drop(health);
            let retired = capacity.snapshot().held_positions == [0; 4];
            assert!(matches!(entered, Ok(true)));
            assert!(shutdown.is_ok());
            assert!(abnormal_join && no_forwarder_panic && has_health && held_by_health);
            assert!(worker_is_original && retired);
            assert_eq!(worker_drops.load(Ordering::Acquire), 1);
            assert_eq!(both_wakes, if early_cancel { 2 } else { 0 });
            assert_eq!(
                wake_drops.load(Ordering::Acquire),
                if early_cancel { 2 } else { 0 }
            );
            assert_eq!(late_retained, early_cancel);
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn unclaimed_success_keeps_original_activity_after_forwarder_join() {
        use futures::FutureExt;
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let input =
            InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| Ok(()))
                .unwrap()
                .track_actual_exit(&binding)
                .unwrap();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = original_runtime();
        let wake_drops = Arc::new(AtomicUsize::new(0));
        let mut job = owner.runtime().submit(
            input,
            QueryCancellationSource::new().view(),
            &runtime,
            Arc::new(Wake {
                panic: false,
                drops: wake_drops,
            }),
            |(), _| (),
        );
        // Join without claiming/draining the original oneshot. This is the
        // explicit poll_forwarder_closed contract, not normal delivery close.
        let joined = tokio::time::timeout(
            Duration::from_secs(5),
            std::future::poll_fn(|cx| job.poll_forwarder_closed(cx)),
        )
        .await;
        let joined_normally = matches!(&joined, Ok(Ok(())));
        drop(joined);
        drop(job.take_retired_outcome());
        let queued_still_active = binding.result_activities_exited().now_or_never().is_none();
        let output = job.close_delivery();
        let original_success = matches!(&output, Some(Ok(_)));
        drop(job);
        let drained_still_active = binding.result_activities_exited().now_or_never().is_none();
        drop(output);
        let output_exited =
            tokio::time::timeout(Duration::from_secs(5), binding.result_activities_exited())
                .await
                .is_ok();
        root.owner.complete();
        root.business.release();
        drop(binding);
        let worker_exit = owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await;
        assert!(joined_normally && original_success && worker_exit.is_ok());
        assert!(queued_still_active && drained_still_active && output_exited);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}
