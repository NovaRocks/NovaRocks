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

use std::fmt;

use uuid::Uuid;

/// Process-lifetime identity for one backend process.
///
/// A backend generates this identifier once during process startup. It is not
/// a durable membership id and is never reused after a restart.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BackendProcessId(Uuid);

/// Transport-neutral validation failure for a backend process identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendProcessIdentityError {
    Nil,
    NotUuidV7,
}

impl fmt::Display for BackendProcessIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Nil => "backend process id must not be nil",
            Self::NotUuidV7 => "backend process id must be UUIDv7",
        })
    }
}

impl std::error::Error for BackendProcessIdentityError {}

impl BackendProcessId {
    /// Allocates a fresh process-lifetime UUIDv7 identity.
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn try_from_uuid(value: Uuid) -> Result<Self, BackendProcessIdentityError> {
        if value.is_nil() {
            return Err(BackendProcessIdentityError::Nil);
        }
        if value.get_version_num() != 7 {
            return Err(BackendProcessIdentityError::NotUuidV7);
        }
        Ok(Self(value))
    }

    pub fn try_from_bytes(value: [u8; 16]) -> Result<Self, BackendProcessIdentityError> {
        Self::try_from_uuid(Uuid::from_bytes(value))
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0.into_bytes()
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for BackendProcessId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for BackendProcessId {
    type Err = BackendProcessIdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = Uuid::parse_str(value).map_err(|_| BackendProcessIdentityError::NotUuidV7)?;
        Self::try_from_uuid(value)
    }
}

/// Process-lifetime identity for one frontend process.
///
/// A frontend mints this identifier once per process start. It fences query
/// context ownership across frontend restarts so that a request from a dead
/// frontend incarnation can never be applied under a live owner. It is not a
/// health proof: application liveness is carried by the query execution lease.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct FrontendProcessId(Uuid);

/// Transport-neutral validation failure for a frontend process identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendProcessIdentityError {
    Nil,
    NotUuidV7,
}

impl fmt::Display for FrontendProcessIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Nil => "frontend process id must not be nil",
            Self::NotUuidV7 => "frontend process id must be UUIDv7",
        })
    }
}

impl std::error::Error for FrontendProcessIdentityError {}

impl FrontendProcessId {
    /// Allocates a fresh process-lifetime UUIDv7 identity.
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn try_from_uuid(value: Uuid) -> Result<Self, FrontendProcessIdentityError> {
        if value.is_nil() {
            return Err(FrontendProcessIdentityError::Nil);
        }
        if value.get_version_num() != 7 {
            return Err(FrontendProcessIdentityError::NotUuidV7);
        }
        Ok(Self(value))
    }

    pub fn try_from_bytes(value: [u8; 16]) -> Result<Self, FrontendProcessIdentityError> {
        Self::try_from_uuid(Uuid::from_bytes(value))
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0.into_bytes()
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for FrontendProcessId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for FrontendProcessId {
    type Err = FrontendProcessIdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = Uuid::parse_str(value).map_err(|_| FrontendProcessIdentityError::NotUuidV7)?;
        Self::try_from_uuid(value)
    }
}

/// Bit-exact identifier used for protocol, fragment, and execution identities.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UniqueId {
    high: i64,
    low: i64,
}

impl UniqueId {
    pub const fn new(high: i64, low: i64) -> Self {
        Self { high, low }
    }

    pub const fn high(self) -> i64 {
        self.high
    }

    pub const fn low(self) -> i64 {
        self.low
    }

    pub fn to_uuid_string(self) -> String {
        format_uuid(self.high, self.low)
    }
}

impl fmt::Display for UniqueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_uuid(f, self.high, self.low)
    }
}

/// Query identity shared by coordinator and runtime ownership domains.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct QueryId {
    high: i64,
    low: i64,
}

impl QueryId {
    pub const fn new(high: i64, low: i64) -> Self {
        Self { high, low }
    }

    pub const fn high(self) -> i64 {
        self.high
    }

    pub const fn low(self) -> i64 {
        self.low
    }

    /// Returns the process attribution carried by a query id allocated by the
    /// native frontend allocator.  A non-positive low half is not an
    /// attributable local sequence and is therefore rejected rather than
    /// guessed.
    pub const fn process_attribution(self) -> Option<QueryIdAttribution> {
        QueryIdAttribution::from_query_id(self)
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_uuid(f, self.high, self.low)
    }
}

/// Transport-neutral validation failure for a native execution identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionIdentityError {
    ZeroAttemptId,
    ZeroQueryId,
    ZeroStageId,
    ZeroTaskId,
}

impl fmt::Display for ExecutionIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroAttemptId => "attempt id must be nonzero",
            Self::ZeroQueryId => "query id must be nonzero",
            Self::ZeroStageId => "stage id must be nonzero",
            Self::ZeroTaskId => "task id must be nonzero",
        })
    }
}

impl std::error::Error for ExecutionIdentityError {}

/// Nonzero ordinal for one physical attempt of a logical query.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AttemptId(u64);

impl AttemptId {
    pub fn new(value: u64) -> Result<Self, ExecutionIdentityError> {
        if value == 0 {
            return Err(ExecutionIdentityError::ZeroAttemptId);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Immutable identity for one physical execution attempt of a logical query.
///
/// This deliberately remains a small `Copy` value rather than a transport
/// representation: FE and BE registries use it as a map key.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryExecutionId {
    query_id: QueryId,
    attempt_id: AttemptId,
}

impl QueryExecutionId {
    pub fn new(query_id: QueryId, attempt_id: AttemptId) -> Result<Self, ExecutionIdentityError> {
        if query_id.high() == 0 && query_id.low() == 0 {
            return Err(ExecutionIdentityError::ZeroQueryId);
        }
        Ok(Self {
            query_id,
            attempt_id,
        })
    }

    pub const fn query_id(self) -> QueryId {
        self.query_id
    }

    pub const fn attempt_id(self) -> AttemptId {
        self.attempt_id
    }
}

impl Ord for QueryExecutionId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.query_id.high(), self.query_id.low(), self.attempt_id).cmp(&(
            other.query_id.high(),
            other.query_id.low(),
            other.attempt_id,
        ))
    }
}

impl PartialOrd for QueryExecutionId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Nonzero, never-reused logical stage identity inside one query execution.
///
/// The frontend planner and coordinator freeze stage ids; a transport retry
/// never re-mints one, and an id is never reused inside the same
/// [`QueryExecutionId`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct StageId(std::num::NonZeroU32);

impl StageId {
    pub fn new(value: u32) -> Result<Self, ExecutionIdentityError> {
        std::num::NonZeroU32::new(value)
            .map(Self)
            .ok_or(ExecutionIdentityError::ZeroStageId)
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for StageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(formatter)
    }
}

/// Nonzero, never-reused task identity inside one stage.
///
/// The owning `StageExecution` allocates task ids. They are stable for the
/// lifetime of the query execution and are never reused, so a late request can
/// always be matched against exactly one task.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TaskId(std::num::NonZeroU32);

impl TaskId {
    pub fn new(value: u32) -> Result<Self, ExecutionIdentityError> {
        std::num::NonZeroU32::new(value)
            .map(Self)
            .ok_or(ExecutionIdentityError::ZeroTaskId)
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.get().fmt(formatter)
    }
}

/// Process-local, high-entropy namespace carried in the high half of a native
/// query identity.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct QueryProcessNamespace(u64);

impl QueryProcessNamespace {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn into_query_id_high(self) -> i64 {
        self.0 as i64
    }
}

impl fmt::Display for QueryProcessNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:016x}", self.0)
    }
}

/// Strictly positive, never-reused sequence within one query process
/// namespace.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LocalQuerySequence(std::num::NonZeroU64);

impl LocalQuerySequence {
    pub const fn new(value: u64) -> Option<Self> {
        if value > i64::MAX as u64 {
            return None;
        }
        match std::num::NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub const fn into_query_id_low(self) -> i64 {
        self.get() as i64
    }
}

impl fmt::Display for LocalQuerySequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Typed diagnostic view of the process namespace and local sequence embedded
/// in a native query id.
// Design: ADR-0092 (docs/adr/ADR-0092-process-scoped-query-execution-attribution.md)
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct QueryIdAttribution {
    namespace: QueryProcessNamespace,
    sequence: LocalQuerySequence,
}

impl QueryIdAttribution {
    pub const fn new(namespace: QueryProcessNamespace, sequence: LocalQuerySequence) -> Self {
        Self {
            namespace,
            sequence,
        }
    }

    pub const fn namespace(self) -> QueryProcessNamespace {
        self.namespace
    }

    pub const fn sequence(self) -> LocalQuerySequence {
        self.sequence
    }

    pub const fn into_query_id(self) -> QueryId {
        QueryId::new(
            self.namespace.into_query_id_high(),
            self.sequence.into_query_id_low(),
        )
    }

    pub const fn from_query_id(query_id: QueryId) -> Option<Self> {
        if query_id.low <= 0 {
            return None;
        }
        match LocalQuerySequence::new(query_id.low as u64) {
            Some(sequence) => Some(Self::new(
                QueryProcessNamespace::new(query_id.high as u64),
                sequence,
            )),
            None => None,
        }
    }
}

impl fmt::Display for QueryIdAttribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "namespace={} sequence={}", self.namespace, self.sequence)
    }
}

pub fn format_uuid(high: i64, low: i64) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        ((high as u64) >> 32) as u32,
        ((high as u64) >> 16) as u16,
        (high as u64) as u16,
        ((low as u64) >> 48) as u16,
        (low as u64) & 0x0000_FFFF_FFFF_FFFF
    )
}

fn write_uuid(f: &mut fmt::Formatter<'_>, high: i64, low: i64) -> fmt::Result {
    f.write_str(&format_uuid(high, low))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use super::{
        AttemptId, BackendProcessId, BackendProcessIdentityError, ExecutionIdentityError,
        FrontendProcessId, FrontendProcessIdentityError, LocalQuerySequence, QueryExecutionId,
        QueryId, QueryIdAttribution, QueryProcessNamespace, StageId, TaskId, UniqueId, format_uuid,
    };
    use uuid::Uuid;

    #[test]
    fn backend_process_id_is_exact_uuid_v7_and_non_nil() {
        let id = BackendProcessId::new_v7();
        assert_eq!(BackendProcessId::try_from_bytes(id.to_bytes()), Ok(id));
        assert_eq!(id.as_uuid().get_version_num(), 7);
        assert_eq!(
            BackendProcessId::try_from_uuid(Uuid::nil()),
            Err(BackendProcessIdentityError::Nil)
        );
        assert_eq!(
            BackendProcessId::try_from_uuid(Uuid::new_v4()),
            Err(BackendProcessIdentityError::NotUuidV7)
        );
    }

    #[test]
    fn identities_preserve_the_java_uuid_bit_layout() {
        let high = 116135542886790518;
        let low = -7531368976812794106;
        let expected = "019c98a9-3390-7576-977b-33d188ad1f06";

        assert_eq!(format_uuid(high, low), expected);
        assert_eq!(UniqueId::new(high, low).to_string(), expected);
        assert_eq!(QueryId::new(high, low).to_string(), expected);
    }

    #[test]
    fn identities_are_value_ordered_and_hashable() {
        let lower = UniqueId::new(1, -1);
        let higher = UniqueId::new(2, -1);
        assert!(lower < higher);

        let mut ordered = BTreeSet::new();
        ordered.insert(higher);
        ordered.insert(lower);
        assert_eq!(ordered.into_iter().collect::<Vec<_>>(), vec![lower, higher]);

        let mut hashed = HashSet::new();
        hashed.insert(QueryId::new(7, 9));
        assert!(hashed.contains(&QueryId::new(7, 9)));
    }

    #[test]
    fn query_process_attribution_round_trips_without_changing_query_id_display() {
        let namespace = QueryProcessNamespace::new(0xfedc_ba98_7654_3210);
        let sequence = LocalQuerySequence::new(7).expect("nonzero sequence");
        let attribution = QueryIdAttribution::new(namespace, sequence);
        let query_id = attribution.into_query_id();

        assert_eq!(query_id.high(), namespace.into_query_id_high());
        assert_eq!(query_id.low(), 7);
        assert_eq!(query_id.process_attribution(), Some(attribution));
        assert_eq!(
            attribution.to_string(),
            "namespace=0xfedcba9876543210 sequence=7"
        );
        assert_eq!(
            query_id.to_string(),
            format_uuid(namespace.into_query_id_high(), sequence.into_query_id_low())
        );
    }

    #[test]
    fn query_process_attribution_rejects_missing_or_non_local_sequence() {
        assert!(LocalQuerySequence::new(0).is_none());
        assert!(LocalQuerySequence::new((i64::MAX as u64) + 1).is_none());
        assert!(QueryId::new(1, 0).process_attribution().is_none());
        assert!(QueryId::new(1, -1).process_attribution().is_none());
    }

    #[test]
    fn execution_identity_is_nonzero_ordered_and_hashable() {
        assert_eq!(
            AttemptId::new(0),
            Err(ExecutionIdentityError::ZeroAttemptId)
        );
        let first = QueryExecutionId::new(
            QueryId::new(7, 9),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id");
        let second = QueryExecutionId::new(
            QueryId::new(7, 9),
            AttemptId::new(2).expect("nonzero attempt"),
        )
        .expect("nonzero query id");
        assert!(first < second);
        assert_eq!(
            QueryExecutionId::new(QueryId::new(0, 0), first.attempt_id()),
            Err(ExecutionIdentityError::ZeroQueryId)
        );

        let mut values = HashSet::new();
        values.insert(first);
        assert!(values.contains(&first));
    }

    #[test]
    fn frontend_process_id_is_exact_uuid_v7_and_non_nil() {
        let id = FrontendProcessId::new_v7();
        assert_eq!(FrontendProcessId::try_from_bytes(id.to_bytes()), Ok(id));
        assert_eq!(id.as_uuid().get_version_num(), 7);
        assert_eq!(
            FrontendProcessId::try_from_uuid(Uuid::nil()),
            Err(FrontendProcessIdentityError::Nil)
        );
        assert_eq!(
            FrontendProcessId::try_from_uuid(Uuid::new_v4()),
            Err(FrontendProcessIdentityError::NotUuidV7)
        );
        assert_ne!(FrontendProcessId::new_v7(), id);
    }

    #[test]
    fn stage_and_task_ids_reject_zero_and_stay_ordered() {
        assert_eq!(StageId::new(0), Err(ExecutionIdentityError::ZeroStageId));
        assert_eq!(TaskId::new(0), Err(ExecutionIdentityError::ZeroTaskId));

        let first = StageId::new(1).expect("nonzero stage");
        let second = StageId::new(2).expect("nonzero stage");
        assert!(first < second);
        assert_eq!(first.get(), 1);
        assert_eq!(first.to_string(), "1");

        let task = TaskId::new(7).expect("nonzero task");
        assert_eq!(task.get(), 7);
        assert_eq!(task.to_string(), "7");

        let mut values = HashSet::new();
        values.insert(task);
        assert!(values.contains(&TaskId::new(7).expect("nonzero task")));
    }
}
