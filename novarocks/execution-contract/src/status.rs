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

//! Task state, immutable versioned status snapshots, and final task info.
//!
//! `TaskStatus` is the single authority for a task's lifecycle and terminal
//! outcome. Every published version binds one complete immutable snapshot: a
//! backend never re-renders an already published version from mutable
//! metrics, and a terminal snapshot can never be overwritten.
//!
//! `FinalTaskInfo` is observation only. Losing it costs diagnostics, never
//! correctness: success and failure are decided by `TaskStatus` alone.

use std::fmt;
use std::time::Duration;

use crate::task_execution::domain::DomainVersion;
use crate::task_execution::identity::{IdentityMismatch, TaskIdentity};

/// Largest human-readable detail carried by a failure or rejection.
pub const SAFE_DETAIL_MAX_BYTES: usize = 512;

/// Largest structured field path carried by a validation failure.
pub const SAFE_FIELD_PATH_MAX_BYTES: usize = 256;

/// Lifecycle state of one task.
///
/// The state family matches what a pipelined engine actually needs to
/// distinguish, rather than collapsing to a success flag: a task that has
/// finished executing but still owes output is `FLUSHING`, and normal
/// cancellation is a different terminal state from failure and from abort.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TaskState {
    /// Identity and descriptor were accepted; execution has not started. This
    /// is only ever a very short internally observable window.
    Planned,
    /// The pipeline is consuming input or running operators.
    Running,
    /// Execution input is complete, but output is not yet fully produced,
    /// consumed, or acknowledged.
    Flushing,
    /// Execution and this task's fixed output responsibility both completed.
    Finished,
    /// This task is no longer needed and is stopping normally.
    Canceling,
    /// Normal early termination completed.
    Canceled,
    /// A query or stage failure is forcing this task down.
    Aborting,
    /// Forced termination completed.
    Aborted,
    /// This task's own error is converging and cleaning up.
    Failing,
    /// This task's own error terminated it.
    Failed,
}

impl TaskState {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Finished | Self::Canceled | Self::Aborted | Self::Failed
        )
    }

    /// Whether this state means the task's own error terminated or is
    /// terminating it.
    pub const fn is_failure(self) -> bool {
        matches!(self, Self::Failing | Self::Failed)
    }

    /// Whether the task is on a normal early-termination path.
    pub const fn is_cancellation(self) -> bool {
        matches!(self, Self::Canceling | Self::Canceled)
    }

    /// Whether the task is on a forced-termination path.
    pub const fn is_abort(self) -> bool {
        matches!(self, Self::Aborting | Self::Aborted)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "PLANNED",
            Self::Running => "RUNNING",
            Self::Flushing => "FLUSHING",
            Self::Finished => "FINISHED",
            Self::Canceling => "CANCELING",
            Self::Canceled => "CANCELED",
            Self::Aborting => "ABORTING",
            Self::Aborted => "ABORTED",
            Self::Failing => "FAILING",
            Self::Failed => "FAILED",
        }
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why a bounded string is not representable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SafeTextTooLong {
    limit: usize,
    actual: usize,
}

impl SafeTextTooLong {
    pub const fn limit(self) -> usize {
        self.limit
    }

    pub const fn actual(self) -> usize {
        self.actual
    }
}

impl fmt::Display for SafeTextTooLong {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "text is {} bytes, limit is {}",
            self.actual, self.limit
        )
    }
}

impl std::error::Error for SafeTextTooLong {}

/// Bounded, already-redacted diagnostic text.
///
/// Constructing this type is the point where a producer asserts the text
/// carries no credential material and no reversible derivative of one. The
/// bound is enforced here so an anomalous task cannot inflate a status
/// snapshot, a retained terminal record, or a profile.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SafeDetail(String);

impl SafeDetail {
    pub fn new(value: impl Into<String>) -> Result<Self, SafeTextTooLong> {
        let value = value.into();
        if value.len() > SAFE_DETAIL_MAX_BYTES {
            return Err(SafeTextTooLong {
                limit: SAFE_DETAIL_MAX_BYTES,
                actual: value.len(),
            });
        }
        Ok(Self(value))
    }

    /// Truncates on a UTF-8 boundary instead of rejecting.
    ///
    /// Use this only where losing the tail of a diagnostic is better than
    /// losing the whole failure, never for protocol content.
    pub fn truncating(value: &str) -> Self {
        if value.len() <= SAFE_DETAIL_MAX_BYTES {
            return Self(value.to_owned());
        }
        let mut end = SAFE_DETAIL_MAX_BYTES;
        while end > 0 && !value.is_char_boundary(end) {
            end -= 1;
        }
        Self(value[..end].to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SafeDetail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Bounded structured field path of a validation failure.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SafeFieldPath(String);

impl SafeFieldPath {
    pub fn new(value: impl Into<String>) -> Result<Self, SafeTextTooLong> {
        let value = value.into();
        if value.len() > SAFE_FIELD_PATH_MAX_BYTES {
            return Err(SafeTextTooLong {
                limit: SAFE_FIELD_PATH_MAX_BYTES,
                actual: value.len(),
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SafeFieldPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Closed category of a task's own failure.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TaskFailureCategory {
    /// An operator, connector, or runtime error inside this task.
    Execution,
    /// This task could not obtain memory, threads, or another local resource.
    ResourceExhausted,
    /// An inbound exchange frame was rejected, or a destination failed.
    Exchange,
    /// A malformed or illegal protocol request reached this task.
    Protocol,
    /// An engine invariant was violated.
    Internal,
}

impl TaskFailureCategory {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Execution => "EXECUTION",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::Exchange => "EXCHANGE",
            Self::Protocol => "PROTOCOL",
            Self::Internal => "INTERNAL",
        }
    }
}

impl fmt::Display for TaskFailureCategory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A task's own bounded, redacted failure cause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskFailure {
    category: TaskFailureCategory,
    detail: SafeDetail,
}

impl TaskFailure {
    pub const fn new(category: TaskFailureCategory, detail: SafeDetail) -> Self {
        Self { category, detail }
    }

    pub const fn category(&self) -> TaskFailureCategory {
        self.category
    }

    pub const fn detail(&self) -> &SafeDetail {
        &self.detail
    }
}

impl fmt::Display for TaskFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.category, self.detail)
    }
}

/// The only normal cancellation reason this release recognises.
///
/// A closed single-reason enum is deliberate: it is what keeps a real failure
/// from being dressed up as a success-compatible cancellation. Client
/// cancellation, query failure, and lease expiry all take the abort path
/// instead and produce `ABORTED`.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CancelReason {
    /// The consumer of this task's output no longer needs it.
    UpstreamNoLongerNeeded,
}

impl CancelReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UpstreamNoLongerNeeded => "UPSTREAM_NO_LONGER_NEEDED",
        }
    }
}

impl fmt::Display for CancelReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why a task was forcibly terminated.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum AbortCause {
    /// The client or the query itself failed, including an explicit kill.
    QueryFailed,
    /// The query execution lease expired on this backend.
    LeaseExpired,
    /// Another task of the same query failed.
    PeerTaskFailed,
}

impl AbortCause {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueryFailed => "QUERY_FAILED",
            Self::LeaseExpired => "LEASE_EXPIRED",
            Self::PeerTaskFailed => "PEER_TASK_FAILED",
        }
    }
}

impl fmt::Display for AbortCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The cause a non-finished terminal or terminating state must carry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TerminationDetail {
    Canceled(CancelReason),
    Aborted(AbortCause),
    Failed(TaskFailure),
}

impl TerminationDetail {
    /// Whether this cause is compatible with the query still succeeding.
    ///
    /// Only a normal cancellation is. It is what lets a `LIMIT` query finish
    /// while its upstream tasks are still being stood down.
    pub const fn is_success_compatible(&self) -> bool {
        matches!(self, Self::Canceled(_))
    }

    /// Whether this cause only reports that something else ended the attempt.
    ///
    /// `PeerTaskFailed` is the one such cause: a query context carries it to
    /// say a task of the same query failed, and the backend that sends it
    /// deliberately does not claim to know which or why -- the failing task's
    /// own terminal is the authority on that. It is therefore a placeholder,
    /// not an explanation, and an attempt that reports it to a client has told
    /// them nothing they can act on.
    pub const fn is_derived(&self) -> bool {
        matches!(self, Self::Aborted(AbortCause::PeerTaskFailed))
    }
}

/// Monotonic status version.
///
/// The first snapshot a task ever publishes is version one, and it is the one
/// carried back by `CreateTaskAck`, which closes the window between creating a
/// task and observing it.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TaskStatusVersion(std::num::NonZeroU64);

/// Why a status version is not representable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ZeroTaskStatusVersion;

impl fmt::Display for ZeroTaskStatusVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("task status version must be nonzero")
    }
}

impl std::error::Error for ZeroTaskStatusVersion {}

impl TaskStatusVersion {
    /// The version of the snapshot returned by `CreateTaskAck`.
    pub const FIRST: Self = Self(std::num::NonZeroU64::new(1).expect("one is nonzero"));

    pub fn new(value: u64) -> Result<Self, ZeroTaskStatusVersion> {
        std::num::NonZeroU64::new(value)
            .map(Self)
            .ok_or(ZeroTaskStatusVersion)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// The next version this task will publish.
    pub const fn next(self) -> Option<Self> {
        match std::num::NonZeroU64::new(self.0.get() + 1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for TaskStatusVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(formatter)
    }
}

/// What a task advertises about its dynamic filter domain.
///
/// Only the version and bounded counts travel in a status snapshot. The
/// payload is read separately, so a large filter can never inflate the
/// lifecycle channel.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct DynamicFilterAdvertisement {
    version: DomainVersion,
    domain_count: u32,
}

impl DynamicFilterAdvertisement {
    pub const fn new(version: DomainVersion, domain_count: u32) -> Self {
        Self {
            version,
            domain_count,
        }
    }

    pub const fn version(self) -> DomainVersion {
        self.version
    }

    pub const fn domain_count(self) -> u32 {
        self.domain_count
    }
}

/// Output facts of one task.
///
/// `responsibility_complete` is the fact that separates `FLUSHING` from
/// `FINISHED`: an operator returning end-of-stream is not enough, this task's
/// output must also have been produced, consumed, or acknowledged.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskOutputFacts {
    responsibility_complete: bool,
    buffered_rows: Option<u64>,
    buffered_bytes: Option<u64>,
}

impl TaskOutputFacts {
    pub const fn new(responsibility_complete: bool) -> Self {
        Self {
            responsibility_complete,
            buffered_rows: None,
            buffered_bytes: None,
        }
    }

    pub const fn with_buffered(mut self, rows: u64, bytes: u64) -> Self {
        self.buffered_rows = Some(rows);
        self.buffered_bytes = Some(bytes);
        self
    }

    pub const fn responsibility_complete(self) -> bool {
        self.responsibility_complete
    }

    /// Buffered rows, if this task reports them.
    ///
    /// A consumer must treat `None` as "not reported" and must never
    /// substitute a default: an absent metric is not zero.
    pub const fn buffered_rows(self) -> Option<u64> {
        self.buffered_rows
    }

    pub const fn buffered_bytes(self) -> Option<u64> {
        self.buffered_bytes
    }
}

/// Resource facts of one task. Every metric is optional and absence never
/// means zero.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskResourceFacts {
    queued_splits: Option<u64>,
    running_drivers: Option<u32>,
    memory_reservation_bytes: Option<u64>,
    cpu_time: Option<Duration>,
}

impl TaskResourceFacts {
    pub const fn empty() -> Self {
        Self {
            queued_splits: None,
            running_drivers: None,
            memory_reservation_bytes: None,
            cpu_time: None,
        }
    }

    pub const fn with_queued_splits(mut self, value: u64) -> Self {
        self.queued_splits = Some(value);
        self
    }

    pub const fn with_running_drivers(mut self, value: u32) -> Self {
        self.running_drivers = Some(value);
        self
    }

    pub const fn with_memory_reservation_bytes(mut self, value: u64) -> Self {
        self.memory_reservation_bytes = Some(value);
        self
    }

    pub const fn with_cpu_time(mut self, value: Duration) -> Self {
        self.cpu_time = Some(value);
        self
    }

    pub const fn queued_splits(self) -> Option<u64> {
        self.queued_splits
    }

    pub const fn running_drivers(self) -> Option<u32> {
        self.running_drivers
    }

    pub const fn memory_reservation_bytes(self) -> Option<u64> {
        self.memory_reservation_bytes
    }

    pub const fn cpu_time(self) -> Option<Duration> {
        self.cpu_time
    }
}

/// Writer facts of a distributed write task. Absent for a read task.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskWriterFacts {
    written_rows: Option<u64>,
    written_bytes: Option<u64>,
    prepared_write_entries: Option<u32>,
}

impl TaskWriterFacts {
    pub const fn empty() -> Self {
        Self {
            written_rows: None,
            written_bytes: None,
            prepared_write_entries: None,
        }
    }

    pub const fn with_written(mut self, rows: u64, bytes: u64) -> Self {
        self.written_rows = Some(rows);
        self.written_bytes = Some(bytes);
        self
    }

    pub const fn with_prepared_write_entries(mut self, value: u32) -> Self {
        self.prepared_write_entries = Some(value);
        self
    }

    pub const fn written_rows(self) -> Option<u64> {
        self.written_rows
    }

    pub const fn written_bytes(self) -> Option<u64> {
        self.written_bytes
    }

    pub const fn prepared_write_entries(self) -> Option<u32> {
        self.prepared_write_entries
    }
}

/// Why a status snapshot is not a legal value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskStatusError {
    /// `FAILED` or `FAILING` without a failure cause, and the mirror cases.
    MissingTerminationDetail(TaskState),
    /// A cause that does not match the state it is attached to.
    TerminationDetailMismatch {
        state: TaskState,
        detail: TerminationDetail,
    },
    /// A non-terminating state carrying a termination cause.
    UnexpectedTerminationDetail(TaskState),
    /// `FINISHED` without a complete output responsibility.
    FinishedWithoutOutputCompletion,
}

impl fmt::Display for TaskStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingTerminationDetail(state) => {
                write!(formatter, "{state} requires a termination cause")
            }
            Self::TerminationDetailMismatch { state, .. } => {
                write!(formatter, "termination cause does not match {state}")
            }
            Self::UnexpectedTerminationDetail(state) => {
                write!(formatter, "{state} must not carry a termination cause")
            }
            Self::FinishedWithoutOutputCompletion => formatter
                .write_str("FINISHED requires this task's output responsibility to be complete"),
        }
    }
}

impl std::error::Error for TaskStatusError {}

/// One immutable, complete status snapshot.
///
/// A published version is bound to exactly this value. A backend may drop a
/// coalesced non-current snapshot, but it may never publish a different value
/// under a version it already used.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskStatus {
    identity: TaskIdentity,
    version: TaskStatusVersion,
    state: TaskState,
    termination: Option<TerminationDetail>,
    dynamic_filters: Option<DynamicFilterAdvertisement>,
    output: TaskOutputFacts,
    resources: TaskResourceFacts,
    writer: Option<TaskWriterFacts>,
}

impl TaskStatus {
    /// The first snapshot of a freshly created task.
    pub fn created(identity: TaskIdentity) -> Self {
        Self {
            identity,
            version: TaskStatusVersion::FIRST,
            state: TaskState::Planned,
            termination: None,
            dynamic_filters: None,
            output: TaskOutputFacts::default(),
            resources: TaskResourceFacts::empty(),
            writer: None,
        }
    }

    /// Builds a snapshot, rejecting every state and cause combination the
    /// protocol does not allow.
    pub fn try_new(
        identity: TaskIdentity,
        version: TaskStatusVersion,
        state: TaskState,
        termination: Option<TerminationDetail>,
        output: TaskOutputFacts,
    ) -> Result<Self, TaskStatusError> {
        match (state, &termination) {
            (TaskState::Finished, None) => {
                if !output.responsibility_complete() {
                    return Err(TaskStatusError::FinishedWithoutOutputCompletion);
                }
            }
            (TaskState::Canceling | TaskState::Canceled, Some(TerminationDetail::Canceled(_)))
            | (TaskState::Aborting | TaskState::Aborted, Some(TerminationDetail::Aborted(_)))
            | (TaskState::Failing | TaskState::Failed, Some(TerminationDetail::Failed(_))) => {}
            (
                TaskState::Canceling
                | TaskState::Canceled
                | TaskState::Aborting
                | TaskState::Aborted
                | TaskState::Failing
                | TaskState::Failed,
                None,
            ) => return Err(TaskStatusError::MissingTerminationDetail(state)),
            (TaskState::Planned | TaskState::Running | TaskState::Flushing, Some(_))
            | (TaskState::Finished, Some(_)) => {
                return Err(TaskStatusError::UnexpectedTerminationDetail(state));
            }
            (_, Some(detail)) => {
                return Err(TaskStatusError::TerminationDetailMismatch {
                    state,
                    detail: detail.clone(),
                });
            }
            (TaskState::Planned | TaskState::Running | TaskState::Flushing, None) => {}
        }
        Ok(Self {
            identity,
            version,
            state,
            termination,
            dynamic_filters: None,
            output,
            resources: TaskResourceFacts::empty(),
            writer: None,
        })
    }

    pub fn with_dynamic_filters(mut self, value: DynamicFilterAdvertisement) -> Self {
        self.dynamic_filters = Some(value);
        self
    }

    pub const fn with_resources(mut self, value: TaskResourceFacts) -> Self {
        self.resources = value;
        self
    }

    pub const fn with_writer(mut self, value: TaskWriterFacts) -> Self {
        self.writer = Some(value);
        self
    }

    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    pub const fn version(&self) -> TaskStatusVersion {
        self.version
    }

    pub const fn state(&self) -> TaskState {
        self.state
    }

    pub const fn termination(&self) -> Option<&TerminationDetail> {
        self.termination.as_ref()
    }

    pub const fn dynamic_filters(&self) -> Option<DynamicFilterAdvertisement> {
        self.dynamic_filters
    }

    pub const fn output(&self) -> TaskOutputFacts {
        self.output
    }

    pub const fn resources(&self) -> TaskResourceFacts {
        self.resources
    }

    pub const fn writer(&self) -> Option<TaskWriterFacts> {
        self.writer
    }

    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Whether this terminal snapshot is compatible with query success.
    pub fn is_success_compatible_terminal(&self) -> bool {
        match self.state {
            TaskState::Finished => true,
            TaskState::Canceled => self
                .termination
                .as_ref()
                .is_some_and(TerminationDetail::is_success_compatible),
            _ => false,
        }
    }
}

/// A frontend's observation position for one task.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TaskStatusCursor {
    identity: TaskIdentity,
    current_version: Option<TaskStatusVersion>,
}

impl TaskStatusCursor {
    /// A cursor for a task that has not been observed yet.
    pub const fn unobserved(identity: TaskIdentity) -> Self {
        Self {
            identity,
            current_version: None,
        }
    }

    pub const fn at(identity: TaskIdentity, version: TaskStatusVersion) -> Self {
        Self {
            identity,
            current_version: Some(version),
        }
    }

    pub const fn identity(self) -> TaskIdentity {
        self.identity
    }

    pub const fn current_version(self) -> Option<TaskStatusVersion> {
        self.current_version
    }

    pub const fn advanced_to(self, version: TaskStatusVersion) -> Self {
        Self {
            identity: self.identity,
            current_version: Some(version),
        }
    }
}

/// One named counter an operator's own profile subtree carried.
///
/// The name is an engine constant, never anything a statement supplied, so
/// this is a bounded diagnostic value rather than redacted content. The value
/// is signed because the profile model's counters are.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorCounter {
    name: SafeDetail,
    value: i64,
}

impl OperatorCounter {
    pub const fn new(name: SafeDetail, value: i64) -> Self {
        Self { name, value }
    }

    pub const fn name(&self) -> &SafeDetail {
        &self.name
    }

    pub const fn value(&self) -> i64 {
        self.value
    }
}

/// Largest number of named counters one operator entry may carry.
///
/// The three projected row and time counters are separate fields and do not
/// count against this. The bound exists because a profile subtree's counter
/// set is open: an operator that grows a new counter must not be able to grow
/// this channel without a decision.
pub const OPERATOR_COUNTER_BUDGET: usize = 64;

/// Bounded operator statistics of one final task info entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorStatistics {
    plan_node_id: i32,
    operator: SafeDetail,
    input_rows: Option<u64>,
    output_rows: Option<u64>,
    wall_time: Option<Duration>,
    /// Every other counter this operator's own profile subtree carried, up to
    /// `OPERATOR_COUNTER_BUDGET`.
    ///
    /// Carried rather than dropped because an operator's diagnostic counters
    /// are the operator's only statement about what it did beyond rows and
    /// time: a connector page source opened, a conjunct applied, a row group
    /// pruned. Dropping them made every reader of those names find nothing and
    /// report zero, which reads as "it did not happen".
    counters: Vec<OperatorCounter>,
    /// Set when the subtree carried more than the budget allows.
    ///
    /// Explicit for the same reason the operator-entry truncation is: a
    /// counter that did not fit and a counter that stayed at zero must not read
    /// the same way.
    counters_truncated: bool,
}

impl OperatorStatistics {
    pub const fn new(plan_node_id: i32, operator: SafeDetail) -> Self {
        Self {
            plan_node_id,
            operator,
            input_rows: None,
            output_rows: None,
            wall_time: None,
            counters: Vec::new(),
            counters_truncated: false,
        }
    }

    /// Records the operator's remaining named counters, truncating at the
    /// budget and saying so.
    pub fn with_counters(mut self, counters: Vec<OperatorCounter>) -> Self {
        self.counters_truncated = counters.len() > OPERATOR_COUNTER_BUDGET;
        self.counters = counters;
        self.counters.truncate(OPERATOR_COUNTER_BUDGET);
        self
    }

    pub const fn with_rows(mut self, input: u64, output: u64) -> Self {
        self.input_rows = Some(input);
        self.output_rows = Some(output);
        self
    }

    /// Records only the rows that entered the operator.
    ///
    /// The two row counts are separate options because a profile can carry one
    /// counter and not the other. Forcing a producer through `with_rows` would
    /// make it invent the missing half as zero, which claims the operator
    /// observed no rows rather than that it reported none.
    pub const fn with_input_rows(mut self, input: u64) -> Self {
        self.input_rows = Some(input);
        self
    }

    /// Records only the rows that left the operator.
    pub const fn with_output_rows(mut self, output: u64) -> Self {
        self.output_rows = Some(output);
        self
    }

    pub const fn with_wall_time(mut self, value: Duration) -> Self {
        self.wall_time = Some(value);
        self
    }

    pub const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub const fn operator(&self) -> &SafeDetail {
        &self.operator
    }

    pub const fn input_rows(&self) -> Option<u64> {
        self.input_rows
    }

    pub const fn output_rows(&self) -> Option<u64> {
        self.output_rows
    }

    pub const fn wall_time(&self) -> Option<Duration> {
        self.wall_time
    }

    /// Marks the counter set as cut by the producer's own budget.
    ///
    /// Separate from `with_counters` because the decoder carries the
    /// producer's statement across the wire rather than re-deriving it: a set
    /// that arrives exactly at the budget cannot be told apart from one that
    /// was cut to it.
    pub const fn with_counters_truncated(mut self) -> Self {
        self.counters_truncated = true;
        self
    }

    pub fn counters(&self) -> &[OperatorCounter] {
        &self.counters
    }

    pub const fn counters_truncated(&self) -> bool {
        self.counters_truncated
    }
}

/// Largest number of operator statistics entries one final info may carry.
pub const FINAL_TASK_INFO_MAX_OPERATORS: usize = 4096;

/// Why a final task info is not a legal value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalTaskInfoError {
    /// The carried status is not terminal.
    StatusNotTerminal(TaskState),
    /// The carried status does not address the same task.
    IdentityMismatch(IdentityMismatch),
    /// More operator entries than the bound allows and no truncation marker.
    OperatorBudgetExceeded { limit: usize, actual: usize },
}

impl fmt::Display for FinalTaskInfoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StatusNotTerminal(state) => {
                write!(
                    formatter,
                    "final task info requires a terminal status, got {state}"
                )
            }
            Self::IdentityMismatch(mismatch) => write!(formatter, "final task info {mismatch}"),
            Self::OperatorBudgetExceeded { limit, actual } => write!(
                formatter,
                "final task info carries {actual} operator entries, limit is {limit}"
            ),
        }
    }
}

impl std::error::Error for FinalTaskInfoError {}

/// The bounded, redacted final observation of one terminal task.
///
/// It is frozen once and never changes. It carries no result payload, no raw
/// split, no credential, and no commit fragment: those have their own owners
/// and data planes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalTaskInfo {
    final_status: TaskStatus,
    operator_statistics: Vec<OperatorStatistics>,
    operator_statistics_truncated: bool,
}

impl FinalTaskInfo {
    pub fn try_new(
        identity: TaskIdentity,
        final_status: TaskStatus,
        operator_statistics: Vec<OperatorStatistics>,
        operator_statistics_truncated: bool,
    ) -> Result<Self, FinalTaskInfoError> {
        if let Err(mismatch) = identity.verify_matches(final_status.identity()) {
            return Err(FinalTaskInfoError::IdentityMismatch(mismatch));
        }
        if !final_status.is_terminal() {
            return Err(FinalTaskInfoError::StatusNotTerminal(final_status.state()));
        }
        if operator_statistics.len() > FINAL_TASK_INFO_MAX_OPERATORS {
            return Err(FinalTaskInfoError::OperatorBudgetExceeded {
                limit: FINAL_TASK_INFO_MAX_OPERATORS,
                actual: operator_statistics.len(),
            });
        }
        Ok(Self {
            final_status,
            operator_statistics,
            operator_statistics_truncated,
        })
    }

    pub const fn final_status(&self) -> &TaskStatus {
        &self.final_status
    }

    pub fn operator_statistics(&self) -> &[OperatorStatistics] {
        &self.operator_statistics
    }

    /// Whether operator statistics were dropped to stay inside the bound.
    ///
    /// Truncation is reported explicitly rather than silently, so a missing
    /// operator is never mistaken for an operator that did no work.
    pub const fn operator_statistics_truncated(&self) -> bool {
        self.operator_statistics_truncated
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SAFE_DETAIL_MAX_BYTES, SafeDetail, SafeFieldPath, TaskOutputFacts, TaskState, TaskStatus,
        TaskStatusError, TaskStatusVersion,
    };
    use crate::TaskIdentity;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn identity() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).expect("attempt"))
                .expect("query execution"),
            StageId::new(2).expect("stage"),
            TaskId::new(3).expect("task"),
            BackendProcessId::new_v7(),
        )
    }

    #[test]
    fn task_state_families_remain_distinct() {
        assert!(TaskState::Finished.is_terminal());
        assert!(TaskState::Failed.is_failure());
        assert!(TaskState::Canceled.is_cancellation());
        assert!(TaskState::Aborted.is_abort());
        assert!(!TaskState::Flushing.is_terminal());
    }

    #[test]
    fn safe_text_is_bounded() {
        assert!(SafeDetail::new("x".repeat(SAFE_DETAIL_MAX_BYTES)).is_ok());
        assert!(SafeDetail::new("x".repeat(SAFE_DETAIL_MAX_BYTES + 1)).is_err());
        assert!(SafeFieldPath::new("field").is_ok());
    }

    #[test]
    fn finished_requires_completed_output_responsibility() {
        assert_eq!(
            TaskStatus::try_new(
                identity(),
                TaskStatusVersion::new(2).expect("version"),
                TaskState::Finished,
                None,
                TaskOutputFacts::new(false),
            ),
            Err(TaskStatusError::FinishedWithoutOutputCompletion)
        );
        assert!(
            TaskStatus::try_new(
                identity(),
                TaskStatusVersion::new(2).expect("version"),
                TaskState::Finished,
                None,
                TaskOutputFacts::new(true),
            )
            .is_ok()
        );
    }
}
