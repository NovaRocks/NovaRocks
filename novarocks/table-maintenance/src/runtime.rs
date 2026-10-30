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

//! Bounded current-process maintenance job ownership.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::Hash;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use crate::MaintenanceEffectId;

// Cooperative observation work, not a job admission or custody limit. A
// current Product future is polled after at most this many original headers.
const EXIT_OBSERVATIONS_PER_TURN: usize = 32;

pub const MAX_ACTIVE_OR_QUEUED_JOBS: usize = 1024;
pub const MAX_RECENT_TERMINAL_JOBS: usize = 4096;
pub const RECENT_TERMINAL_JOB_RETENTION_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MaintenanceJobState {
    Pending,
    Running,
    Finished,
    KnownUncommitted,
    CommitUnknown,
    KnownCommittedFinalizationFailed,
    PreDispatchFailed,
    Failed,
    TargetReplaced,
    CancelledBeforeDispatch,
}

impl MaintenanceJobState {
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending | Self::Running)
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Finished => "FINISHED",
            Self::KnownUncommitted => "KNOWN_UNCOMMITTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
            Self::KnownCommittedFinalizationFailed => "KNOWN_COMMITTED_FINALIZATION_FAILED",
            Self::PreDispatchFailed => "PRE_DISPATCH_FAILED",
            Self::Failed => "FAILED",
            Self::TargetReplaced => "TARGET_REPLACED",
            Self::CancelledBeforeDispatch => "CANCELLED_BEFORE_DISPATCH",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobCreate<T> {
    pub target: T,
    pub effect_id: Option<MaintenanceEffectId>,
    pub object_id: Vec<u8>,
    pub base_snapshot_id: i64,
    pub created_at_ms: i64,
}

/// One negative observation edge in the existing job record. Clones borrow
/// this same slot; none manufactures an execution, capacity or teardown right.
#[derive(Clone, Default)]
pub struct OriginalJobExit {
    original: Arc<Mutex<OriginalJobExitBody>>,
    #[cfg(test)]
    polls: Arc<std::sync::atomic::AtomicUsize>,
}
#[derive(Default)]
struct OriginalJobExitBody {
    actual_exited: bool,
    observer: Option<novarocks_query_application::api::LogicalExecutionExitObserver>,
    receipt: Option<novarocks_query_application::api::LogicalExecutionExitReceipt>,
    failure: Option<novarocks_query_application::api::QueryExecutionError>,
    busy: bool,
}
impl std::fmt::Debug for OriginalJobExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OriginalJobExit")
    }
}
impl PartialEq for OriginalJobExit {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.original, &other.original)
    }
}
impl Eq for OriginalJobExit {}
impl OriginalJobExit {
    /// Silent receiving before start-tail notification; rejects leave the
    /// original observer and this destination intact.
    pub fn install_in(
        &self,
        observer: &mut Option<novarocks_query_application::api::LogicalExecutionExitObserver>,
    ) -> Result<(), String> {
        let mut slot = self
            .original
            .lock()
            .map_err(|_| "optimize exit slot is poisoned")?;
        if slot.busy
            || slot.observer.is_some()
            || slot.receipt.is_some()
            || slot.failure.is_some()
            || observer.is_none()
        {
            return Err("optimize original exit is not transferable".into());
        }
        slot.observer = observer.take();
        Ok(())
    }
    pub fn actual_exit_observed(&self) -> bool {
        self.original
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .actual_exited
    }
    fn should_poll(&self) -> bool {
        let slot = self.original.lock().unwrap_or_else(|p| p.into_inner());
        !slot.busy && !slot.actual_exited && slot.observer.is_some()
    }
    pub fn known_layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<Mutex<OriginalJobExitBody>>()
    }
    pub fn poll_original(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), String>> {
        #[cfg(test)]
        self.polls.fetch_add(1, Ordering::Relaxed);
        let mut loan = OriginalJobExitLoan::take(self)?;
        if loan.body.actual_exited {
            return std::task::Poll::Ready(Ok(()));
        }
        if loan.body.receipt.is_some() {
            // Retained original ownership is a paid return, never actual exit.
            return std::task::Poll::Pending;
        }
        if let Some(error) = &loan.body.failure {
            return std::task::Poll::Ready(Err(error.to_string()));
        }
        let Some(observer) = loan.body.observer.as_mut() else {
            return std::task::Poll::Pending;
        };
        match observer.poll_exit(cx) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(Ok(receipt)) => {
                drop(loan.body.observer.take());
                if receipt.local_disposition() == novarocks_query_application::api::LocalExecutionDisposition::ActualSourcesExited {
                    // Actual receipt Drop also runs outside both original
                    // mutexes. Publish observed only after normal return.
                    drop(receipt);
                    loan.body.actual_exited = true;
                    std::task::Poll::Ready(Ok(()))
                } else {
                    loan.body.receipt = Some(receipt);
                    std::task::Poll::Pending
                }
            }
            std::task::Poll::Ready(Err(error)) => {
                loan.body.failure = Some(error);
                std::task::Poll::Ready(Err(
                    "original optimize exit failed; exact cause retained".into()
                ))
            }
        }
    }
}
struct OriginalJobExitLoan<'a> {
    source: &'a OriginalJobExit,
    body: OriginalJobExitBody,
}
impl<'a> OriginalJobExitLoan<'a> {
    fn take(source: &'a OriginalJobExit) -> Result<Self, String> {
        let mut slot = source
            .original
            .lock()
            .map_err(|_| "optimize exit slot is poisoned")?;
        if slot.busy {
            return Err("optimize exit slot already has its original observer".into());
        }
        let mut body = std::mem::take(&mut *slot);
        body.busy = false;
        slot.busy = true;
        Ok(Self { source, body })
    }
}
impl Drop for OriginalJobExitLoan<'_> {
    fn drop(&mut self) {
        let mut slot = self
            .source
            .original
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        assert!(
            slot.busy && slot.observer.is_none() && slot.receipt.is_none(),
            "same optimize exit slot"
        );
        *slot = std::mem::take(&mut self.body);
    }
}

/// Private custody in the original repository, never in a public snapshot.
enum JobAdmissionRoute {
    WorkerRoot,
    Prepared(Arc<PreparedJobOriginals>),
    Transferred,
}
pub struct PreparedJobOriginals {
    original: Mutex<Option<crate::worker::OptimizeJobOriginals>>,
}
impl PreparedJobOriginals {
    pub fn known_layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<Self>()
    }
}
impl std::fmt::Debug for JobAdmissionRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::WorkerRoot => "WorkerRoot",
            Self::Prepared(_) => "PreparedOriginal",
            Self::Transferred => "TransferredOriginal",
        })
    }
}
impl PartialEq for JobAdmissionRoute {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::WorkerRoot, Self::WorkerRoot) | (Self::Transferred, Self::Transferred) => true,
            (Self::Prepared(a), Self::Prepared(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}
impl Eq for JobAdmissionRoute {}
impl JobAdmissionRoute {
    fn take_original(&self) -> Option<crate::worker::OptimizeJobOriginals> {
        match self {
            Self::WorkerRoot | Self::Transferred => None,
            Self::Prepared(source) => source
                .original
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take(),
        }
    }
}
pub(crate) struct ClaimedOriginalJob<T, O> {
    pub(crate) job: JobRecord<T, O>,
    pub(crate) originals: Option<crate::worker::OptimizeJobOriginals>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobRecord<T, O> {
    pub failure_cause: Option<novarocks_failure_contract::SharedFailureCause>,
    /// Actual cumulative effect, independent of the original terminal cause.
    pub effect: Option<novarocks_coordination_contract::ExecutionEffect>,
    pub job_id: i64,
    pub target: T,
    pub effect_id: Option<MaintenanceEffectId>,
    pub object_id: Vec<u8>,
    pub base_snapshot_id: i64,
    pub state: MaintenanceJobState,
    pub outcome: Option<O>,
    pub error_message: Option<String>,
    pub compile_control: Option<novarocks_type_contract::CompileControlError>,
    pub execution_failure:
        Option<novarocks_query_application::execution_error::ExecutionFailureInput>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

/// Names one exact current-process maintenance job.
///
/// A handle never means "the job currently active for this target": callers
/// may only wait on the immutable identifier returned at submission time.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JobHandle {
    job_id: i64,
}

impl JobHandle {
    pub const fn new(job_id: i64) -> Self {
        Self { job_id }
    }

    pub const fn job_id(self) -> i64 {
        self.job_id
    }
}

impl<T, O> JobRecord<T, O> {
    pub const fn handle(&self) -> JobHandle {
        JobHandle::new(self.job_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeErrorKind {
    AlreadyActive,
    Capacity,
    IdExhausted,
    InvalidTransition,
    NotFound,
    Poisoned,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeError {
    kind: RuntimeErrorKind,
    message: String,
}

impl RuntimeError {
    pub const fn kind(&self) -> RuntimeErrorKind {
        self.kind
    }
    pub fn new(kind: RuntimeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for RuntimeError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalError {
    pub failure_cause: Option<novarocks_failure_contract::SharedFailureCause>,
    pub effect: Option<novarocks_coordination_contract::ExecutionEffect>,
    pub state: MaintenanceJobState,
    pub message: String,
    pub compile_control: Option<novarocks_type_contract::CompileControlError>,
    pub execution_failure:
        Option<novarocks_query_application::execution_error::ExecutionFailureInput>,
}

impl TerminalError {
    pub fn with_failure_cause(
        mut self,
        cause: Option<novarocks_failure_contract::SharedFailureCause>,
    ) -> Self {
        self.failure_cause = cause;
        self
    }
    /// Selection only delegates to an actual carrier; no reason is inferred.
    pub fn selected_failure_fact(&self) -> Option<novarocks_failure_contract::FailureFact> {
        use novarocks_failure_contract::FailureProjection;
        self.failure_cause
            .as_ref()
            .map(FailureProjection::failure_fact)
            .or_else(|| self.execution_failure.as_ref().map(|input| input.fact()))
            .or_else(|| {
                self.compile_control
                    .as_ref()
                    .map(FailureProjection::failure_fact)
            })
    }

    pub fn with_effect(mut self, effect: novarocks_coordination_contract::ExecutionEffect) -> Self {
        self.effect = Some(effect);
        self
    }

    pub fn with_execution_failure(
        mut self,
        input: Option<novarocks_query_application::execution_error::ExecutionFailureInput>,
    ) -> Self {
        self.execution_failure = input;
        self
    }

    /// Preserve an ordinary producer failure without turning it into a compiler interruption.
    pub fn from_original_cause(
        error: impl novarocks_failure_contract::FailureProjection
        + std::error::Error
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self::pre_dispatch_failed("table-maintenance original operation failed").with_failure_cause(
            Some(
                novarocks_failure_contract::SharedFailureCause::from_static_detail(
                    error,
                    "table-maintenance original operation failed",
                ),
            ),
        )
    }
    pub fn from_compile_control(error: novarocks_type_contract::CompileControlError) -> Self {
        let mut failure = Self::pre_dispatch_failed(error.to_string());
        failure.compile_control = Some(error);
        failure
    }
    pub fn with_compile_control(
        mut self,
        error: Option<novarocks_type_contract::CompileControlError>,
    ) -> Self {
        self.compile_control = error;
        self
    }
    pub fn pre_dispatch_failed(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::PreDispatchFailed,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: Some(novarocks_coordination_contract::ExecutionEffect::None),
        }
    }
    pub fn failed(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::Failed,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: None,
        }
    }
    pub fn target_replaced(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::TargetReplaced,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: Some(novarocks_coordination_contract::ExecutionEffect::None),
        }
    }
    pub fn cancelled_before_dispatch(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::CancelledBeforeDispatch,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: Some(novarocks_coordination_contract::ExecutionEffect::None),
        }
    }
    pub fn known_uncommitted(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::KnownUncommitted,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: Some(novarocks_coordination_contract::ExecutionEffect::None),
        }
    }
    pub fn commit_unknown(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::CommitUnknown,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: None,
        }
    }
    pub fn known_committed_finalization_failed(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::KnownCommittedFinalizationFailed,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: Some(novarocks_coordination_contract::ExecutionEffect::External),
        }
    }
}

impl From<String> for TerminalError {
    fn from(error: String) -> Self {
        Self::pre_dispatch_failed(error)
    }
}
impl fmt::Display for TerminalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for TerminalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.failure_cause
            .as_ref()
            .map(|cause| cause as &dyn std::error::Error)
            .or_else(|| {
                self.compile_control
                    .as_ref()
                    .map(|cause| cause as &dyn std::error::Error)
            })
    }
}

// This extension has exactly one repository owner. Business snapshots only
// contain the Copy effect fact; no clone can prolong a receiving Source.
struct RuntimeCustody {
    admission: JobAdmissionRoute,
    original_exit: Option<OriginalJobExit>,
    exit_installed: bool,
}
struct RuntimeEntry<T, O> {
    job: JobRecord<T, O>,
    custody: RuntimeCustody,
}
impl<T, O> std::ops::Deref for RuntimeEntry<T, O> {
    type Target = JobRecord<T, O>;
    fn deref(&self) -> &Self::Target {
        &self.job
    }
}
impl<T, O> std::ops::DerefMut for RuntimeEntry<T, O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.job
    }
}

// One coalesced notification edge for the original exit observers. It owns
// neither RuntimeState nor any business Source, permit or completion fact.
#[derive(Default)]
struct OriginalExitWake {
    dirty: AtomicBool,
    worker: Mutex<Option<std::task::Waker>>,
}
impl OriginalExitWake {
    fn register(&self, waker: &std::task::Waker) {
        let next = waker.clone();
        let old = self
            .worker
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .replace(next);
        drop(old);
    }
    fn notify(&self) {
        self.dirty.store(true, Ordering::Release);
        let worker = self.worker.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(worker) = worker {
            worker.wake();
        }
    }
}
impl std::task::Wake for OriginalExitWake {
    fn wake(self: Arc<Self>) {
        self.notify();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}

struct RuntimeState<T, O, P> {
    active: HashMap<i64, RuntimeEntry<T, O>>,
    active_targets: HashMap<T, i64>,
    permits: HashMap<i64, P>,
    cancellation_requested: HashSet<i64>,
    terminal: VecDeque<RuntimeEntry<T, O>>,
    // This ordered header indexes the SAME original negative observation cell.
    // It contains no business Source, permit, queue or capacity authority.
    exit_headers: BTreeMap<i64, OriginalJobExit>,
    exit_cursor: Option<i64>,
    exit_round_end: Option<i64>,
}

impl<T, O, P> Default for RuntimeState<T, O, P> {
    fn default() -> Self {
        Self {
            active: HashMap::new(),
            active_targets: HashMap::new(),
            permits: HashMap::new(),
            cancellation_requested: HashSet::new(),
            terminal: VecDeque::new(),
            exit_headers: BTreeMap::new(),
            exit_cursor: None,
            exit_round_end: None,
        }
    }
}

#[derive(Clone)]
pub struct ProcessRuntime<T, O, P> {
    state: Arc<Mutex<RuntimeState<T, O, P>>>,
    next_id: Arc<AtomicI64>,
    id_base: i64,
    accepting: Arc<AtomicBool>,
    changed: Arc<tokio::sync::Notify>,
    exit_wake: Arc<OriginalExitWake>,
}

impl<T, O, P> Default for ProcessRuntime<T, O, P> {
    fn default() -> Self {
        let entropy = Uuid::now_v7().as_u128() as u64;
        Self {
            state: Arc::new(Mutex::new(RuntimeState::default())),
            next_id: Arc::new(AtomicI64::new(1)),
            id_base: ((entropy & 0x3fff_ffff_ffff_0000) as i64).max(1),
            accepting: Arc::new(AtomicBool::new(true)),
            changed: Arc::new(tokio::sync::Notify::new()),
            exit_wake: Arc::new(OriginalExitWake::default()),
        }
    }
}

impl<T, O, P> ProcessRuntime<T, O, P>
where
    T: Clone + Eq + Hash,
    O: Clone,
{
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn submit(
        &self,
        request: JobCreate<T>,
        permit: P,
    ) -> Result<JobRecord<T, O>, RuntimeError> {
        self.install_record(
            request,
            &mut Some(permit),
            JobAdmissionRoute::WorkerRoot,
            None,
        )
    }
    fn install_record(
        &self,
        request: JobCreate<T>,
        permit: &mut Option<P>,
        admission: JobAdmissionRoute,
        originals: Option<&mut Option<crate::worker::OptimizeJobOriginals>>,
    ) -> Result<JobRecord<T, O>, RuntimeError> {
        if permit.is_none() {
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidTransition,
                "target permit is absent",
            ));
        }
        if !self.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidTransition,
                "the table-maintenance runtime is shutting down",
            ));
        }
        let mut state = self.lock()?;
        Self::prune_locked(&mut state, now_ms());
        if state.active_targets.contains_key(&request.target) {
            return Err(RuntimeError::new(
                RuntimeErrorKind::AlreadyActive,
                "a maintenance job is already active for this target",
            ));
        }
        if state.active.len() >= MAX_ACTIVE_OR_QUEUED_JOBS {
            return Err(RuntimeError::new(
                RuntimeErrorKind::Capacity,
                "table-maintenance runtime capacity is exhausted",
            ));
        }
        let job_id = self.allocate_id()?;
        if let JobAdmissionRoute::Prepared(receiving) = &admission {
            let mut slot = receiving.original.lock().unwrap_or_else(|p| p.into_inner());
            assert!(slot.is_none(), "same queued Optimize receiving slot");
            *slot = originals
                .expect("prepared insertion carries original slots")
                .take();
            assert!(slot.is_some(), "validated queued Optimize originals");
        }

        let job = JobRecord {
            job_id,
            target: request.target,
            effect_id: request.effect_id,
            object_id: request.object_id,
            base_snapshot_id: request.base_snapshot_id,
            state: MaintenanceJobState::Pending,
            outcome: None,
            error_message: None,
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: Some(novarocks_coordination_contract::ExecutionEffect::None),
            created_at_ms: request.created_at_ms,
            started_at_ms: None,
            finished_at_ms: None,
        };
        state.active_targets.insert(job.target.clone(), job_id);
        state.permits.insert(
            job_id,
            permit.take().expect("validated original target permit"),
        );
        let notify = matches!(admission, JobAdmissionRoute::WorkerRoot);
        let view = job.clone();
        state.active.insert(
            job_id,
            RuntimeEntry {
                job,
                custody: RuntimeCustody {
                    admission,
                    original_exit: None,
                    exit_installed: false,
                },
            },
        );
        drop(state);
        if notify {
            self.changed.notify_waiters();
        }
        Ok(view)
    }
    pub fn install_original_record_in(
        &self,
        request: JobCreate<T>,
        permit: &mut Option<P>,
        originals: &mut Option<crate::worker::OptimizeJobOriginals>,
    ) -> Result<JobRecord<T, O>, RuntimeError> {
        let source = originals.as_ref().ok_or_else(|| {
            RuntimeError::new(
                RuntimeErrorKind::InvalidTransition,
                "Optimize original receiving is empty",
            )
        })?;
        let owner = source.owner.as_ref().ok_or_else(|| {
            RuntimeError::new(
                RuntimeErrorKind::InvalidTransition,
                "Optimize original Root is absent",
            )
        })?;
        let scope = owner.scope();
        if !source
            .query_concurrency
            .as_ref()
            .is_some_and(|p| p.is_for_scope(&scope))
        {
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidTransition,
                "Optimize originals do not share the same Root",
            ));
        }
        scope
            .check()
            .map_err(|e| RuntimeError::new(RuntimeErrorKind::InvalidTransition, e.to_string()))?;
        let receiving = Arc::new(PreparedJobOriginals {
            original: Mutex::new(None),
        });
        // The caller's handoff receiving cell remains affirmative until all
        // record insertion checks succeed. No WC call occurs under this lock.
        let job = self.install_record(
            request,
            permit,
            JobAdmissionRoute::Prepared(receiving),
            Some(originals),
        )?;
        // Notification is separate. The caller first wakes the original
        // serial worker, then publishes public repository/Root notices.
        Ok(job)
    }
    pub(crate) fn notify_original_change(&self) {
        self.changed.notify_waiters();
    }
    pub(crate) async fn claim_prepared(
        &self,
        at_ms: i64,
    ) -> Result<Option<ClaimedOriginalJob<T, O>>, RuntimeError> {
        let mut state = self.lock()?;
        let Some(id) = state
            .active
            .values()
            .filter(|job| {
                job.state == MaintenanceJobState::Pending
                    && matches!(job.custody.admission, JobAdmissionRoute::Prepared(_))
            })
            .min_by_key(|job| job.job_id)
            .map(|job| job.job_id)
        else {
            return Ok(None);
        };
        let job = state.active.get_mut(&id).unwrap();
        let originals = job.custody.admission.take_original().ok_or_else(|| {
            RuntimeError::new(
                RuntimeErrorKind::InvalidTransition,
                "queued Optimize original Source is empty",
            )
        })?;
        let spent = std::mem::replace(&mut job.custody.admission, JobAdmissionRoute::Transferred);
        job.state = MaintenanceJobState::Running;
        job.started_at_ms = Some(at_ms);
        let job = job.job.clone();
        drop(state);
        drop(spent);
        self.changed.notify_waiters();
        Ok(Some(ClaimedOriginalJob {
            job,
            originals: Some(originals),
        }))
    }
    pub async fn get(&self, job_id: i64) -> Result<Option<JobRecord<T, O>>, RuntimeError> {
        let state = self.lock()?;
        Ok(state
            .active
            .get(&job_id)
            .map(|entry| entry.job.clone())
            .or_else(|| {
                state
                    .terminal
                    .iter()
                    .find(|job| job.job_id == job_id)
                    .map(|entry| entry.job.clone())
            }))
    }
    pub async fn list(&self) -> Result<Vec<JobRecord<T, O>>, RuntimeError> {
        let mut state = self.lock()?;
        Self::prune_locked(&mut state, now_ms());
        let mut jobs: Vec<_> = state
            .active
            .values()
            .map(|entry| entry.job.clone())
            .collect();
        jobs.extend(state.terminal.iter().map(|entry| entry.job.clone()));
        jobs.sort_by_key(|job| job.job_id);
        Ok(jobs)
    }
    pub async fn claim_next(&self, at_ms: i64) -> Result<Option<JobRecord<T, O>>, RuntimeError> {
        let mut state = self.lock()?;
        let Some(id) = state
            .active
            .values()
            .filter(|job| {
                job.state == MaintenanceJobState::Pending
                    && matches!(job.custody.admission, JobAdmissionRoute::WorkerRoot)
            })
            .min_by_key(|job| job.job_id)
            .map(|job| job.job_id)
        else {
            return Ok(None);
        };
        let job = state.active.get_mut(&id).expect("selected job exists");
        job.state = MaintenanceJobState::Running;
        job.started_at_ms = Some(at_ms);
        let result = job.job.clone();
        drop(state);
        self.changed.notify_waiters();
        Ok(Some(result))
    }
    /// Install the original observer only after its actual Root Source paid
    /// this receiving header. Pending job submission allocates no exit slot.
    pub fn install_original_exit_in(
        &self,
        job_id: i64,
        observer: &mut Option<novarocks_query_application::api::LogicalExecutionExitObserver>,
    ) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let job = state
            .active
            .get_mut(&job_id)
            .ok_or_else(|| Self::not_found(job_id))?;
        if job.state != MaintenanceJobState::Running
            || job.custody.exit_installed
            || observer.is_none()
        {
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidTransition,
                "original optimize exit is not transferable",
            ));
        }
        let destination = OriginalJobExit::default();
        destination
            .install_in(observer)
            .map_err(|e| RuntimeError::new(RuntimeErrorKind::InvalidTransition, e))?;
        job.custody.original_exit = Some(destination.clone());
        job.custody.exit_installed = true;
        // The same unique job ID has never installed an exit header. There is
        // no replaced original to drop under the repository mutex.
        assert!(!state.exit_headers.contains_key(&job_id));
        state.exit_headers.insert(job_id, destination);
        drop(state);
        // An observer installed beyond the current round's high watermark
        // has not registered its own wake yet. Preserve this insertion across
        // the remaining batches before forwarding outside the repository lock.
        self.exit_wake.notify();
        Ok(())
    }

    pub async fn finish(
        &self,
        job_id: i64,
        outcome: Result<O, TerminalError>,
        at_ms: i64,
    ) -> Result<JobRecord<T, O>, RuntimeError> {
        self.finish_with_effect(job_id, outcome, None, at_ms).await
    }
    pub async fn finish_with_effect(
        &self,
        job_id: i64,
        outcome: Result<O, TerminalError>,
        effect: Option<novarocks_coordination_contract::ExecutionEffect>,
        at_ms: i64,
    ) -> Result<JobRecord<T, O>, RuntimeError> {
        let mut state = self.lock()?;
        let job = state
            .active
            .remove(&job_id)
            .ok_or_else(|| Self::not_found(job_id))?;
        if job.state != MaintenanceJobState::Running {
            state.active.insert(job_id, job);
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidTransition,
                "maintenance job is not running",
            ));
        }
        let mut terminal = job;
        match outcome {
            Ok(outcome) => {
                terminal.state = MaintenanceJobState::Finished;
                terminal.outcome = Some(outcome);
                terminal.effect = effect;
            }
            Err(error) => {
                terminal.state = error.state;
                terminal.error_message = Some(error.message);
                terminal.compile_control = error.compile_control;
                terminal.execution_failure = error.execution_failure;
                terminal.failure_cause = error.failure_cause;
                terminal.effect = error.effect;
            }
        }
        terminal.finished_at_ms = Some(at_ms);
        state.active_targets.remove(&terminal.target);
        state.permits.remove(&job_id);
        state.cancellation_requested.remove(&job_id);
        let view = terminal.job.clone();
        state.terminal.push_back(terminal);
        Self::prune_locked(&mut state, at_ms);
        drop(state);
        self.changed.notify_waiters();
        Ok(view)
    }
    pub async fn wait_for_completion(&self, job_id: i64) -> Result<JobRecord<T, O>, RuntimeError> {
        loop {
            let notified = self.changed.notified();
            let job = self
                .get(job_id)
                .await?
                .ok_or_else(|| Self::not_found(job_id))?;
            if job.state.is_terminal() {
                return Ok(job);
            }
            notified.await;
        }
    }
    pub async fn request_cancel(&self, job_id: i64) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        if !state.active.contains_key(&job_id) {
            if state.terminal.iter().any(|job| job.job_id == job_id) {
                return Ok(());
            }
            return Err(Self::not_found(job_id));
        }
        state.cancellation_requested.insert(job_id);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    pub async fn cancellation_requested(&self, job_id: i64) -> Result<bool, RuntimeError> {
        Ok(self.lock()?.cancellation_requested.contains(&job_id))
    }
    pub async fn request_shutdown_cancellation(&self) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let at_ms = now_ms();
        let mut retired_originals = Vec::new();
        let mut retired_receivers = Vec::new();
        let pending: Vec<_> = state
            .active
            .iter()
            .filter_map(|(&job_id, job)| {
                (job.state == MaintenanceJobState::Pending).then_some(job_id)
            })
            .collect();
        for job_id in pending {
            let mut terminal = state
                .active
                .remove(&job_id)
                .expect("pending maintenance job exists");
            if let Some(original) = terminal.custody.admission.take_original() {
                retired_originals.push(original);
            }
            retired_receivers.push(std::mem::replace(
                &mut terminal.custody.admission,
                JobAdmissionRoute::Transferred,
            ));
            terminal.state = MaintenanceJobState::CancelledBeforeDispatch;
            terminal.error_message =
                Some("maintenance job cancelled before provider dispatch".into());
            terminal.finished_at_ms = Some(at_ms);
            state.active_targets.remove(&terminal.target);
            state.permits.remove(&job_id);
            state.cancellation_requested.remove(&job_id);
            state.terminal.push_back(terminal);
        }
        let running: Vec<_> = state.active.keys().copied().collect();
        state.cancellation_requested.extend(running);
        Self::prune_locked(&mut state, at_ms);
        drop(state);
        self.changed.notify_waiters();
        // Pure unlaunched Sources retire only after releasing the repository
        // lock; WorkOwner/permit/grant Drop never reenters WC under this lock.
        drop(retired_originals);
        drop(retired_receivers);
        Ok(())
    }
    pub(crate) fn has_worker_pending(&self) -> Result<bool, RuntimeError> {
        Ok(self.lock()?.active.values().any(|job| {
            job.state == MaintenanceJobState::Pending
                && matches!(job.custody.admission, JobAdmissionRoute::WorkerRoot)
        }))
    }
    /// Register before inspecting the original queue. Callers enable this
    /// receiver before the check so a silent-to-installed handoff cannot race
    /// the transition into waiting.
    pub(crate) fn change_notification(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }
    pub async fn wait_for_change(&self) {
        self.changed.notified().await;
    }
    pub fn stop_admission(&self) {
        self.accepting.store(false, Ordering::Release);
        self.changed.notify_waiters();
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, RuntimeState<T, O, P>>, RuntimeError> {
        self.state.lock().map_err(|_| {
            RuntimeError::new(
                RuntimeErrorKind::Poisoned,
                "table-maintenance runtime lock is poisoned",
            )
        })
    }
    fn not_found(job_id: i64) -> RuntimeError {
        RuntimeError::new(
            RuntimeErrorKind::NotFound,
            format!("maintenance job {job_id} is not in this process"),
        )
    }
    fn allocate_id(&self) -> Result<i64, RuntimeError> {
        let offset = self
            .next_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                if next <= 0 {
                    return None;
                }
                let id = self.id_base.checked_add(next)?;
                if id <= 0 {
                    return None;
                }
                // The base is positive, so the final valid ID leaves room
                // for one exhausted offset. Every later call refuses without
                // changing that offset or wrapping the shared counter.
                next.checked_add(1)
            })
            .map_err(|_| {
                RuntimeError::new(
                    RuntimeErrorKind::IdExhausted,
                    "table-maintenance job-id range exhausted",
                )
            })?;
        self.id_base.checked_add(offset).ok_or_else(|| {
            RuntimeError::new(
                RuntimeErrorKind::IdExhausted,
                "table-maintenance job-id overflow",
            )
        })
    }
    /// Poll a bounded slice of the original observation headers. A round has
    /// an immutable high watermark, so concurrent inserts cannot starve its
    /// tail. Every header is visited at most once before the cursor wraps.
    pub(crate) fn poll_exits(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), RuntimeError>> {
        // Register outside State. Actual observer wakes remain dirty for the
        // entire round even if a later cooperative batch consumes their wake.
        self.exit_wake.register(cx.waker());
        let observer_waker = std::task::Waker::from(self.exit_wake.clone());
        let mut observer_cx = std::task::Context::from_waker(&observer_waker);
        // Fixed stack transfer, not a copied inventory or an event queue.
        // Declare it before acquiring State so unwind also drops clones only
        // after the repository guard has left scope.
        let mut batch: [Option<(i64, OriginalJobExit)>; EXIT_OBSERVATIONS_PER_TURN] =
            std::array::from_fn(|_| None);
        let more = {
            let mut state = self.lock()?;
            if state.exit_round_end.is_none() {
                // A fresh scan acknowledges preceding notifications. Any
                // notification during this scan must survive until its end.
                self.exit_wake.dirty.swap(false, Ordering::AcqRel);
                state.exit_round_end = state.exit_headers.last_key_value().map(|(&id, _)| id);
            }
            let mut last = None;
            let more = if let Some(end) = state.exit_round_end {
                let lower = state.exit_cursor.map_or(Unbounded, Excluded);
                let mut range = state.exit_headers.range((lower, Included(end)));
                for slot in &mut batch {
                    let Some((&id, exit)) = range.next() else {
                        break;
                    };
                    *slot = Some((id, exit.clone()));
                    last = Some(id);
                }
                range.next().is_some()
            } else {
                false
            };
            if more {
                state.exit_cursor = last;
            } else {
                state.exit_cursor = None;
                state.exit_round_end = None;
            }
            more
        };
        let mut observed = false;
        for (id, exit) in batch.into_iter().flatten() {
            // Neither the original slot lock nor foreign poll/Drop runs
            // under RuntimeState. Retained returns and raw failures stay put.
            if exit.should_poll()
                && matches!(
                    exit.poll_original(&mut observer_cx),
                    std::task::Poll::Ready(Ok(()))
                )
            {
                let retired = {
                    let mut state = self.lock()?;
                    let same = state
                        .exit_headers
                        .get(&id)
                        .is_some_and(|stored| stored == &exit);
                    same.then(|| state.exit_headers.remove(&id)).flatten()
                };
                drop(retired);
                // The private job record still names the same now-empty cell.
                // Its actual-exited bit remains the sole prune permission.
                observed = true;
            }
        }
        let notified_during_round = if more {
            false
        } else {
            // Re-arm before acknowledging dirty. notify() takes the worker
            // waker, so a notification after the final check still needs a
            // live forwarding edge; an earlier notification is covered by the
            // explicit next-round wake below. No foreign operation holds State.
            self.exit_wake.register(cx.waker());
            self.exit_wake.dirty.swap(false, Ordering::AcqRel)
        };
        // A completed pending round only repeats for an actual observer wake.
        // Cooperative continuation alone never leaves dirty set or causes spin.
        if more || notified_during_round {
            cx.waker().wake_by_ref();
        }
        if observed {
            self.changed.notify_waiters();
            std::task::Poll::Ready(Ok(()))
        } else {
            std::task::Poll::Pending
        }
    }

    fn prune_locked(state: &mut RuntimeState<T, O, P>, at_ms: i64) {
        loop {
            let over_count = state.terminal.len() > MAX_RECENT_TERMINAL_JOBS;
            let removable = state.terminal.iter().position(|job| {
                job.custody
                    .original_exit
                    .as_ref()
                    .is_none_or(OriginalJobExit::actual_exit_observed)
                    && (over_count
                        || job.finished_at_ms.is_some_and(|finished| {
                            at_ms.saturating_sub(finished) > RECENT_TERMINAL_JOB_RETENTION_MS
                        }))
            });
            let Some(index) = removable else {
                break;
            };
            // A governed exit cell is empty of foreign observer/receipt/cause
            // here; pending/retained originals can never be pruned as history.
            state.terminal.remove(index);
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{JobCreate, MaintenanceJobState, ProcessRuntime, RuntimeErrorKind, TerminalError};
    use crate::MaintenanceEffectId;

    fn create(target: &str, at_ms: i64) -> JobCreate<String> {
        JobCreate {
            target: target.to_owned(),
            effect_id: None,
            object_id: b"object".to_vec(),
            base_snapshot_id: 7,
            created_at_ms: at_ms,
        }
    }

    #[tokio::test]
    async fn job_preserves_caller_frozen_automatic_effect_identity() {
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let effect_id = MaintenanceEffectId::from_bytes([5; 16]);
        let mut request = create("table", 1);
        request.effect_id = Some(effect_id);
        let submitted = runtime.submit(request, ()).await.expect("submit");
        assert_eq!(submitted.effect_id, Some(effect_id));
        let claimed = runtime.claim_next(2).await.expect("claim").expect("job");
        assert_eq!(claimed.effect_id, Some(effect_id));
        let terminal = runtime
            .finish(claimed.job_id, Ok(()), 3)
            .await
            .expect("finish");
        assert_eq!(terminal.effect_id, Some(effect_id));
    }

    #[tokio::test]
    async fn target_is_exclusive_and_completed_before_wait_is_observable() {
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let job = runtime
            .submit(create("orders", 1), ())
            .await
            .expect("submit");
        assert_eq!(
            runtime
                .submit(create("orders", 2), ())
                .await
                .unwrap_err()
                .kind(),
            RuntimeErrorKind::AlreadyActive
        );
        runtime.claim_next(2).await.expect("claim").expect("job");
        runtime
            .finish(job.job_id, Err(TerminalError::failed("failed")), 3)
            .await
            .expect("finish");
        let completed = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            runtime.wait_for_completion(job.job_id),
        )
        .await
        .expect("wait must not miss completed-before-subscribe")
        .expect("job");
        assert_eq!(completed.state, MaintenanceJobState::Failed);
        runtime
            .submit(create("orders", 4), ())
            .await
            .expect("released target");
    }

    #[tokio::test]
    async fn terminal_classification_is_retained() {
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let job = runtime
            .submit(create("orders", 1), ())
            .await
            .expect("submit");
        runtime.claim_next(2).await.expect("claim");
        runtime
            .finish(
                job.job_id,
                Err(TerminalError::cancelled_before_dispatch("cancelled")),
                3,
            )
            .await
            .expect("finish");
        assert_eq!(
            runtime.get(job.job_id).await.expect("get").unwrap().state,
            MaintenanceJobState::CancelledBeforeDispatch
        );
    }

    #[tokio::test]
    async fn shutdown_cancellation_completes_a_pending_job_before_dispatch() {
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let job = runtime
            .submit(create("orders", 1), ())
            .await
            .expect("submit");
        runtime
            .request_shutdown_cancellation()
            .await
            .expect("shutdown cancellation");
        let terminal = runtime
            .wait_for_completion(job.job_id)
            .await
            .expect("terminal job");
        assert_eq!(terminal.state, MaintenanceJobState::CancelledBeforeDispatch);
        assert!(
            !runtime
                .cancellation_requested(job.job_id)
                .await
                .expect("cancellation state")
        );
    }
    #[tokio::test]
    async fn completed_job_retains_control_separately_from_publication_state() {
        use novarocks_type_contract::CompileControlError;
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
            let job = runtime.submit(create("orders", 1), ()).await.unwrap();
            runtime.claim_next(2).await.unwrap().unwrap();
            let failure = TerminalError::known_uncommitted("cohort compile failed")
                .with_compile_control(Some(control.clone()));
            runtime.finish(job.job_id, Err(failure), 3).await.unwrap();
            let terminal = runtime.wait_for_completion(job.job_id).await.unwrap();
            assert_eq!(terminal.compile_control, Some(control));
            assert_eq!(terminal.state, MaintenanceJobState::KnownUncommitted);
            assert_eq!(
                terminal.error_message.as_deref(),
                Some("cohort compile failed")
            );
            runtime
                .submit(create("orders", 4), ())
                .await
                .expect("exact terminal released target");
        }
    }

    #[tokio::test]
    async fn per_job_cancel_retains_pending_and_running_work_until_actual_finish() {
        for dispatch in [false, true] {
            let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
            let job = runtime.submit(create("orders", 1), ()).await.unwrap();
            if dispatch {
                runtime.claim_next(2).await.unwrap().unwrap();
            }
            runtime.request_cancel(job.job_id).await.unwrap();
            assert!(runtime.cancellation_requested(job.job_id).await.unwrap());
            let wait = runtime.wait_for_completion(job.job_id);
            tokio::pin!(wait);
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    wait.as_mut().poll(cx).is_pending()
                ))
                .await
            );
            let terminal = if dispatch {
                Ok(())
            } else {
                runtime.claim_next(2).await.unwrap().unwrap();
                Err(TerminalError::cancelled_before_dispatch("cancelled"))
            };
            runtime.finish(job.job_id, terminal, 3).await.unwrap();
            let actual = wait.await.unwrap();
            assert_eq!(
                actual.state,
                if dispatch {
                    MaintenanceJobState::Finished
                } else {
                    MaintenanceJobState::CancelledBeforeDispatch
                }
            );
            runtime.request_cancel(job.job_id).await.unwrap();
            assert!(!runtime.cancellation_requested(job.job_id).await.unwrap());
        }
    }
}

#[cfg(test)]
mod selected_execution_retention_tests {
    use super::*;
    #[tokio::test]
    async fn completed_job_keeps_selected_execution_input_in_its_original_terminal_state() {
        let input =
            novarocks_query_application::session_error::QueryServiceError::from_compile_control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
            .execution_failure()
            .unwrap()
            .clone();
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let request = JobCreate {
            target: "orders".into(),
            effect_id: None,
            object_id: vec![1],
            base_snapshot_id: 1,
            created_at_ms: 1,
        };
        let job = runtime.submit(request, ()).await.unwrap();
        runtime.claim_next(2).await.unwrap().unwrap();
        runtime
            .finish(
                job.job_id,
                Err(TerminalError::known_uncommitted("selected failure")
                    .with_execution_failure(Some(input.clone()))),
                3,
            )
            .await
            .unwrap();
        let terminal = runtime.wait_for_completion(job.job_id).await.unwrap();
        assert_eq!(terminal.execution_failure, Some(input));
        assert_eq!(terminal.state, MaintenanceJobState::KnownUncommitted);
        assert_eq!(terminal.error_message.as_deref(), Some("selected failure"));
    }
}

#[cfg(test)]
mod original_custody_tests {
    use super::*;
    fn create() -> JobCreate<String> {
        JobCreate {
            target: "orders".into(),
            effect_id: None,
            object_id: vec![1],
            base_snapshot_id: 1,
            created_at_ms: 1,
        }
    }
    #[tokio::test]
    async fn original_private_exit_is_not_cloned_by_public_snapshots() {
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let job = runtime.submit(create(), ()).await.unwrap();
        runtime.claim_next(2).await.unwrap().unwrap();
        // Only the private negative receiving header is needed to check
        // cloning. No observer, actual-exit receipt or grant is fabricated.
        let exit = OriginalJobExit::default();
        runtime
            .lock()
            .unwrap()
            .active
            .get_mut(&job.job_id)
            .unwrap()
            .custody
            .original_exit = Some(exit.clone());
        let before = Arc::strong_count(&exit.original);
        let get = runtime.get(job.job_id).await.unwrap().unwrap();
        let list = runtime.list().await.unwrap();
        let finished = runtime.finish(job.job_id, Ok(()), 3).await.unwrap();
        assert_eq!(get.job_id, finished.job_id);
        assert_eq!(list.len(), 1);
        assert_eq!(Arc::strong_count(&exit.original), before);
        assert!(
            runtime
                .lock()
                .unwrap()
                .terminal
                .front()
                .unwrap()
                .custody
                .original_exit
                .is_some()
        );
    }
    #[test]
    fn original_job_id_author_allocates_across_the_old_lifetime_cut() {
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let peer = runtime.clone();
        runtime.next_id.store(65_535, Ordering::Relaxed);
        let ids = [
            runtime.allocate_id().unwrap(),
            peer.allocate_id().unwrap(),
            runtime.allocate_id().unwrap(),
        ];
        assert_eq!(
            ids,
            [
                runtime.id_base + 65_535,
                runtime.id_base + 65_536,
                runtime.id_base + 65_537,
            ]
        );
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(peer.next_id.load(Ordering::Relaxed), 65_538);
    }

    #[test]
    fn original_job_id_author_exhausts_only_at_positive_i64_overflow() {
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let final_offset = i64::MAX.checked_sub(runtime.id_base).unwrap();
        runtime.next_id.store(final_offset - 1, Ordering::Relaxed);
        assert_eq!(runtime.allocate_id().unwrap(), i64::MAX - 1);
        assert_eq!(runtime.allocate_id().unwrap(), i64::MAX);
        let exhausted_offset = final_offset.checked_add(1).unwrap();
        assert_eq!(runtime.next_id.load(Ordering::Relaxed), exhausted_offset);
        for _ in 0..4 {
            assert_eq!(
                runtime.allocate_id().unwrap_err().kind(),
                RuntimeErrorKind::IdExhausted
            );
            assert_eq!(runtime.next_id.load(Ordering::Relaxed), exhausted_offset);
        }
    }
}

#[cfg(test)]
#[path = "runtime/exit_fairness_tests.rs"]
mod exit_fairness_tests;
