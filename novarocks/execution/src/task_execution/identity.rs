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

//! Composite task protocol identities and their exact-match classification.

use std::fmt;

use novarocks_types::identity::{
    BackendProcessId, FrontendProcessId, QueryExecutionId, StageId, TaskId,
};
use uuid::Uuid;

/// Identity of one protocol operation.
///
/// Every mutation and observation read carries an operation id so that an
/// unknown transport outcome can be resolved by replaying the exact same
/// immutable request. Replaying a different content under the same id is a
/// conflict, never an update.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TaskOperationId(Uuid);

/// Transport-neutral validation failure for an operation identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskOperationIdError {
    Nil,
    NotUuidV7,
}

impl fmt::Display for TaskOperationIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Nil => "task operation id must not be nil",
            Self::NotUuidV7 => "task operation id must be UUIDv7",
        })
    }
}

impl std::error::Error for TaskOperationIdError {}

impl TaskOperationId {
    /// Allocates a fresh operation identity.
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn try_from_uuid(value: Uuid) -> Result<Self, TaskOperationIdError> {
        if value.is_nil() {
            return Err(TaskOperationIdError::Nil);
        }
        if value.get_version_num() != 7 {
            return Err(TaskOperationIdError::NotUuidV7);
        }
        Ok(Self(value))
    }

    pub fn try_from_bytes(value: [u8; 16]) -> Result<Self, TaskOperationIdError> {
        Self::try_from_uuid(Uuid::from_bytes(value))
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0.into_bytes()
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for TaskOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Which identity component failed an exact match.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum IdentityField {
    QueryExecution,
    Stage,
    Task,
    BackendProcess,
    FrontendProcess,
}

impl IdentityField {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueryExecution => "query execution id",
            Self::Stage => "stage id",
            Self::Task => "task id",
            Self::BackendProcess => "backend process id",
            Self::FrontendProcess => "frontend process id",
        }
    }
}

impl fmt::Display for IdentityField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A fatal identity mismatch.
///
/// Every component of a task identity or query context reference is compared
/// exactly. A mismatch is always a fatal protocol error: an operation is never
/// retargeted at a different task, stage, or process.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct IdentityMismatch {
    field: IdentityField,
}

impl IdentityMismatch {
    pub const fn new(field: IdentityField) -> Self {
        Self { field }
    }

    pub const fn field(self) -> IdentityField {
        self.field
    }
}

impl fmt::Display for IdentityMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} mismatch", self.field)
    }
}

impl std::error::Error for IdentityMismatch {}

/// The logical stage a task belongs to.
///
/// Stage state is a frontend-derived aggregate observation over a stage's
/// complete, frozen task set. It is never a wire authority, so this reference
/// exists only to group tasks inside one query execution.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct StageRef {
    query_execution_id: QueryExecutionId,
    stage_id: StageId,
}

impl StageRef {
    pub const fn new(query_execution_id: QueryExecutionId, stage_id: StageId) -> Self {
        Self {
            query_execution_id,
            stage_id,
        }
    }

    pub const fn query_execution_id(self) -> QueryExecutionId {
        self.query_execution_id
    }

    pub const fn stage_id(self) -> StageId {
        self.stage_id
    }
}

impl fmt::Display for StageRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}/stage-{}",
            self.query_execution_id.query_id(),
            self.stage_id
        )
    }
}

/// The indivisible wire identity of one task.
///
/// Exactly this tuple addresses every task update, status, final info, and
/// termination request. `BackendProcessId` is part of the identity, so a
/// restarted backend that reuses an endpoint can never accept or impersonate a
/// request minted for its predecessor.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TaskIdentity {
    query_execution_id: QueryExecutionId,
    stage_id: StageId,
    task_id: TaskId,
    backend_process_id: BackendProcessId,
}

impl TaskIdentity {
    pub const fn new(
        query_execution_id: QueryExecutionId,
        stage_id: StageId,
        task_id: TaskId,
        backend_process_id: BackendProcessId,
    ) -> Self {
        Self {
            query_execution_id,
            stage_id,
            task_id,
            backend_process_id,
        }
    }

    pub const fn query_execution_id(self) -> QueryExecutionId {
        self.query_execution_id
    }

    pub const fn stage_id(self) -> StageId {
        self.stage_id
    }

    pub const fn task_id(self) -> TaskId {
        self.task_id
    }

    pub const fn backend_process_id(self) -> BackendProcessId {
        self.backend_process_id
    }

    pub const fn stage_ref(self) -> StageRef {
        StageRef::new(self.query_execution_id, self.stage_id)
    }

    /// Compares two task identities component by component.
    ///
    /// The first differing component is reported so that callers can classify
    /// a process replacement separately from a wrong task inside the same
    /// process.
    pub fn verify_matches(self, other: Self) -> Result<(), IdentityMismatch> {
        if self.query_execution_id != other.query_execution_id {
            return Err(IdentityMismatch::new(IdentityField::QueryExecution));
        }
        if self.stage_id != other.stage_id {
            return Err(IdentityMismatch::new(IdentityField::Stage));
        }
        if self.task_id != other.task_id {
            return Err(IdentityMismatch::new(IdentityField::Task));
        }
        if self.backend_process_id != other.backend_process_id {
            return Err(IdentityMismatch::new(IdentityField::BackendProcess));
        }
        Ok(())
    }

    /// Verifies that a create request's query context reference addresses the
    /// same query execution and the same backend process as this task.
    ///
    /// The frontend process id is deliberately not part of a task identity, so
    /// it is not compared here: a query context reference carries it, and the
    /// backend fences it against the context it owns.
    pub fn verify_query_context(self, context: QueryContextRef) -> Result<(), IdentityMismatch> {
        if self.query_execution_id != context.query_execution_id() {
            return Err(IdentityMismatch::new(IdentityField::QueryExecution));
        }
        if self.backend_process_id != context.backend_process_id() {
            return Err(IdentityMismatch::new(IdentityField::BackendProcess));
        }
        Ok(())
    }
}

impl fmt::Display for TaskIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}/stage-{}/task-{}@{}",
            self.query_execution_id.query_id(),
            self.stage_id,
            self.task_id,
            self.backend_process_id
        )
    }
}

/// Exact reference to the shared execution context of one query on one
/// backend process.
///
/// A backend holds at most one query context per reference. Zero-task backends
/// never establish one, so the participant set of a query is exactly the set
/// of backends carrying at least one scheduled task.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct QueryContextRef {
    query_execution_id: QueryExecutionId,
    frontend_process_id: FrontendProcessId,
    backend_process_id: BackendProcessId,
}

impl QueryContextRef {
    pub const fn new(
        query_execution_id: QueryExecutionId,
        frontend_process_id: FrontendProcessId,
        backend_process_id: BackendProcessId,
    ) -> Self {
        Self {
            query_execution_id,
            frontend_process_id,
            backend_process_id,
        }
    }

    pub const fn query_execution_id(self) -> QueryExecutionId {
        self.query_execution_id
    }

    pub const fn frontend_process_id(self) -> FrontendProcessId {
        self.frontend_process_id
    }

    pub const fn backend_process_id(self) -> BackendProcessId {
        self.backend_process_id
    }

    /// Compares two context references component by component.
    pub fn verify_matches(self, other: Self) -> Result<(), IdentityMismatch> {
        if self.query_execution_id != other.query_execution_id {
            return Err(IdentityMismatch::new(IdentityField::QueryExecution));
        }
        if self.frontend_process_id != other.frontend_process_id {
            return Err(IdentityMismatch::new(IdentityField::FrontendProcess));
        }
        if self.backend_process_id != other.backend_process_id {
            return Err(IdentityMismatch::new(IdentityField::BackendProcess));
        }
        Ok(())
    }
}

impl fmt::Display for QueryContextRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}@fe-{}/be-{}",
            self.query_execution_id.query_id(),
            self.frontend_process_id,
            self.backend_process_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IdentityField, IdentityMismatch, QueryContextRef, TaskIdentity, TaskOperationId,
        TaskOperationIdError,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use uuid::Uuid;

    fn execution(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(7, 9),
            AttemptId::new(attempt).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn identity(
        execution_id: QueryExecutionId,
        stage: u32,
        task: u32,
        backend: BackendProcessId,
    ) -> TaskIdentity {
        TaskIdentity::new(
            execution_id,
            StageId::new(stage).expect("nonzero stage"),
            TaskId::new(task).expect("nonzero task"),
            backend,
        )
    }

    #[test]
    fn operation_id_is_exact_uuid_v7_and_non_nil() {
        let id = TaskOperationId::new_v7();
        assert_eq!(TaskOperationId::try_from_bytes(id.to_bytes()), Ok(id));
        assert_eq!(
            TaskOperationId::try_from_uuid(Uuid::nil()),
            Err(TaskOperationIdError::Nil)
        );
        assert_eq!(
            TaskOperationId::try_from_uuid(Uuid::new_v4()),
            Err(TaskOperationIdError::NotUuidV7)
        );
    }

    #[test]
    fn task_identity_reports_the_first_mismatching_component() {
        let backend = BackendProcessId::new_v7();
        let base = identity(execution(1), 2, 3, backend);

        assert_eq!(base.verify_matches(base), Ok(()));
        assert_eq!(
            base.verify_matches(identity(execution(2), 2, 3, backend)),
            Err(IdentityMismatch::new(IdentityField::QueryExecution))
        );
        assert_eq!(
            base.verify_matches(identity(execution(1), 4, 3, backend)),
            Err(IdentityMismatch::new(IdentityField::Stage))
        );
        assert_eq!(
            base.verify_matches(identity(execution(1), 2, 5, backend)),
            Err(IdentityMismatch::new(IdentityField::Task))
        );
        assert_eq!(
            base.verify_matches(identity(execution(1), 2, 3, BackendProcessId::new_v7())),
            Err(IdentityMismatch::new(IdentityField::BackendProcess))
        );
    }

    #[test]
    fn create_requires_a_context_ref_matching_query_and_backend_exactly() {
        let backend = BackendProcessId::new_v7();
        let frontend = FrontendProcessId::new_v7();
        let task = identity(execution(1), 2, 3, backend);
        let context = QueryContextRef::new(execution(1), frontend, backend);

        assert_eq!(task.verify_query_context(context), Ok(()));
        assert_eq!(
            task.verify_query_context(QueryContextRef::new(execution(2), frontend, backend)),
            Err(IdentityMismatch::new(IdentityField::QueryExecution))
        );
        assert_eq!(
            task.verify_query_context(QueryContextRef::new(
                execution(1),
                frontend,
                BackendProcessId::new_v7()
            )),
            Err(IdentityMismatch::new(IdentityField::BackendProcess))
        );
        // A different frontend incarnation is fenced by the context owner, not
        // by the task identity: the task tuple never carries it.
        assert_eq!(
            task.verify_query_context(QueryContextRef::new(
                execution(1),
                FrontendProcessId::new_v7(),
                backend
            )),
            Ok(())
        );
    }

    #[test]
    fn context_ref_fences_the_frontend_incarnation() {
        let backend = BackendProcessId::new_v7();
        let frontend = FrontendProcessId::new_v7();
        let base = QueryContextRef::new(execution(1), frontend, backend);

        assert_eq!(base.verify_matches(base), Ok(()));
        assert_eq!(
            base.verify_matches(QueryContextRef::new(
                execution(1),
                FrontendProcessId::new_v7(),
                backend
            )),
            Err(IdentityMismatch::new(IdentityField::FrontendProcess))
        );
    }

    #[test]
    fn identities_render_every_component() {
        let backend = BackendProcessId::new_v7();
        let task = identity(execution(1), 2, 3, backend);
        let rendered = task.to_string();
        assert!(rendered.contains("stage-2"), "{rendered}");
        assert!(rendered.contains("task-3"), "{rendered}");
        assert!(rendered.contains(&backend.to_string()), "{rendered}");
        assert_eq!(task.stage_ref().stage_id().get(), 2);
    }
}
