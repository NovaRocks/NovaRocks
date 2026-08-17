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

//! Materialized-view application and engine ports.

mod refresh_artifact;

use std::fmt;

use novarocks_spi::connector::{ConnectorExecutionBindingKey, ConnectorWriteOperationId};
use uuid::Uuid;

use crate::mv::repository::{
    CreateMvRepositoryRequest, MV_REPOSITORY_UNAVAILABLE_MESSAGE, MvRepository, MvTarget,
};
use crate::runtime::query_result::QueryResult;
use novarocks_sql::planning::mv::{MvRefreshFinalizeFacts, MvRefreshStatement, SqlMvTarget};
use novarocks_sql::syntax::{
    CreateMaterializedViewStmt, IcebergPartitionFieldExpr, MaterializedViewRefreshPolicy,
    MvAdmittedStatement,
};

pub(crate) use refresh_artifact::{
    MvFirstRefreshExecutionArtifact, MvFirstRefreshLogicalContext, MvFirstRefreshWritePreparer,
    MvFirstRefreshWriteRequest, MvIncrementalExecutionArtifact, MvIncrementalJoinMode,
    MvIncrementalRewriteEvidence, MvIncrementalWriteMode, MvIncrementalWritePreparer,
    MvIncrementalWriteRequest, MvStagedRefreshWriteMode,
};

/// Frontend-preallocated identities for a single MV refresh lifecycle. These
/// are application lifecycle values: SQL may validate them but cannot create
/// a connector operation or persist their durable intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshAttemptIdentity {
    pub refresh_id: i64,
    pub request_id: [u8; 16],
    pub staging_branch: String,
    pub marker_token: String,
    pub staging_create_operation_id: [u8; 16],
    pub write_operation_id: ConnectorWriteOperationId,
    pub publication_operation_id: [u8; 16],
    pub staging_drop_operation_id: [u8; 16],
}

impl MvRefreshAttemptIdentity {
    pub fn validate(&self) -> Result<(), String> {
        if self.refresh_id <= 0 || self.staging_branch.is_empty() || self.marker_token.is_empty() {
            return Err(
                "MV refresh preparation requires a positive identity and non-empty staging marker"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// Application request supplied to the side-effect-free SQL preparation port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshPreparationRequest {
    pub statement: MvRefreshStatement,
    pub target: SqlMvTarget,
    pub attempt: MvRefreshAttemptIdentity,
}

impl MvRefreshPreparationRequest {
    pub fn validate(&self) -> Result<(), String> {
        self.statement.validate_supported()?;
        self.attempt.validate()
    }
}

/// Application-owned work handoff after SQL planning. SQL plans determine the
/// semantic shape; this lifecycle envelope owns operation/cohort-bearing
/// artifacts and is the only value admitted by frontend staging.
pub enum PreparedMvRefreshWork {
    NoOp,
    MetadataOnly,
    DataProducing { write: PreparedMvRefreshWrite },
}

/// Exactly one SQL-prepared staged write for a data-producing refresh.
pub enum PreparedMvRefreshWrite {
    FirstRefresh(PreparedMvFirstRefreshWrite),
    Incremental(PreparedMvIncrementalWrite),
}

impl PreparedMvRefreshWrite {
    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        match self {
            Self::FirstRefresh(write) => write.operation_id(),
            Self::Incremental(write) => write.operation_id(),
        }
    }

    pub fn primary_cohort(&self) -> novarocks_spi::connector::ConnectorWriteCohortId {
        match self {
            Self::FirstRefresh(write) => write.primary_cohort(),
            Self::Incremental(write) => write.primary_cohort(),
        }
    }

    pub fn publication_intent(&self) -> &MvRefreshPublicationIntent {
        match self {
            Self::FirstRefresh(write) => write.publication_intent(),
            Self::Incremental(write) => write.publication_intent(),
        }
    }
}

/// Frontend lifecycle artifact assembled from SQL facts and a reserved attempt.
pub struct PreparedMvRefresh {
    pub statement: MvRefreshStatement,
    pub attempt: MvRefreshAttemptIdentity,
    pub observed_binding: ConnectorExecutionBindingKey,
    pub finalize: MvRefreshFinalizeFacts,
    pub work: PreparedMvRefreshWork,
}

/// SQL preparation port consumed by the frontend lifecycle owner. Its request
/// and output are application envelopes around immutable SQL values, never a
/// way for SQL to acquire lifecycle or connector authority itself.
pub trait MvRefreshPreparationService: Send + Sync {
    fn prepare_step(
        &self,
        request: MvRefreshPreparationRequest,
    ) -> Result<PreparedMvRefresh, String>;
}

#[cfg(test)]
mod refresh_preparation_tests {
    use super::*;

    fn attempt() -> MvRefreshAttemptIdentity {
        MvRefreshAttemptIdentity {
            refresh_id: 7,
            request_id: [1; 16],
            staging_branch: "__nova_mv_7".to_string(),
            marker_token: "marker".to_string(),
            staging_create_operation_id: [2; 16],
            write_operation_id: ConnectorWriteOperationId::from_bytes([3; 16]),
            publication_operation_id: [4; 16],
            staging_drop_operation_id: [5; 16],
        }
    }

    #[test]
    fn sqlx2_application_refresh_attempt_is_lifecycle_owned() {
        attempt().validate().expect("complete attempt identity");
        assert!(
            MvRefreshAttemptIdentity {
                marker_token: String::new(),
                ..attempt()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn sqlx2_application_refresh_request_keeps_sql_rejection_and_attempt_together() {
        let request = MvRefreshPreparationRequest {
            statement: MvRefreshStatement {
                name_parts: vec!["mv".to_string()],
                full: false,
            },
            target: SqlMvTarget {
                catalog: Some("iceberg".to_string()),
                database: "db".to_string(),
                name: "mv".to_string(),
            },
            attempt: attempt(),
        };
        request.validate().expect("complete request");
        assert!(
            MvRefreshPreparationRequest {
                statement: MvRefreshStatement {
                    full: true,
                    ..request.statement
                },
                ..request
            }
            .validate()
            .is_err()
        );
    }
}
pub use refresh_artifact::{
    MvRefreshCommittedFacts, MvRefreshPublicationBase, MvRefreshPublicationIntent,
    MvRefreshPublicationTechnique, MvRefreshPublishedFacts, PreparedMvFirstRefreshWrite,
    PreparedMvIncrementalWrite,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvCreatePartitionField {
    Identity { column: String },
    Year { column: String },
    Month { column: String },
    Day { column: String },
    Hour { column: String },
    Bucket { column: String, num_buckets: u32 },
    Truncate { column: String, width: u32 },
    Void { column: String },
}

impl From<&IcebergPartitionFieldExpr> for MvCreatePartitionField {
    fn from(value: &IcebergPartitionFieldExpr) -> Self {
        match value {
            IcebergPartitionFieldExpr::Identity { column } => Self::Identity {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Year { column } => Self::Year {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Month { column } => Self::Month {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Day { column } => Self::Day {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Hour { column } => Self::Hour {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Bucket {
                column,
                num_buckets,
            } => Self::Bucket {
                column: column.clone(),
                num_buckets: *num_buckets,
            },
            IcebergPartitionFieldExpr::Truncate { column, width } => Self::Truncate {
                column: column.clone(),
                width: *width,
            },
            IcebergPartitionFieldExpr::Void { column } => Self::Void {
                column: column.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvCreateDistribution {
    pub hash_columns: Vec<String>,
    pub bucket_count: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum MvCreateRefreshPolicy {
    #[default]
    Manual,
    AsyncOnChange,
    AsyncInterval {
        interval_ms: i64,
    },
}

impl From<&MaterializedViewRefreshPolicy> for MvCreateRefreshPolicy {
    fn from(value: &MaterializedViewRefreshPolicy) -> Self {
        match value {
            MaterializedViewRefreshPolicy::Manual => Self::Manual,
            MaterializedViewRefreshPolicy::AsyncOnChange => Self::AsyncOnChange,
            MaterializedViewRefreshPolicy::AsyncInterval { interval_ms } => Self::AsyncInterval {
                interval_ms: *interval_ms,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MvCreateStatement {
    pub name_parts: Vec<String>,
    pub if_not_exists: bool,
    pub partition_by: Option<Vec<MvCreatePartitionField>>,
    pub distribution: Option<MvCreateDistribution>,
    pub refresh_policy: MvCreateRefreshPolicy,
    pub select_sql: String,
    pub select_query: sqlparser::ast::Query,
    pub properties: Vec<(String, String)>,
    pub primary_key: Option<Vec<String>>,
}

impl From<&CreateMaterializedViewStmt> for MvCreateStatement {
    fn from(value: &CreateMaterializedViewStmt) -> Self {
        Self {
            name_parts: value.name.parts.clone(),
            if_not_exists: value.if_not_exists,
            partition_by: value
                .partition_by
                .as_ref()
                .map(|fields| fields.iter().map(MvCreatePartitionField::from).collect()),
            distribution: value
                .distribution
                .as_ref()
                .map(|distribution| MvCreateDistribution {
                    hash_columns: distribution.hash_columns.clone(),
                    bucket_count: distribution.bucket_count,
                }),
            refresh_policy: MvCreateRefreshPolicy::from(&value.refresh_policy),
            select_sql: value.select_sql.clone(),
            select_query: value.select_query.clone(),
            properties: value.properties.clone(),
            primary_key: value.primary_key.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum MvApplicationStatement {
    Create(MvCreateStatement),
    Refresh(MvRefreshStatement),
    Unhandled,
}

pub(crate) fn project_statement(statement: &MvAdmittedStatement) -> MvApplicationStatement {
    match statement {
        MvAdmittedStatement::Create(statement) => {
            MvApplicationStatement::Create(MvCreateStatement::from(statement))
        }
        MvAdmittedStatement::Refresh(statement) => {
            MvApplicationStatement::Refresh(MvRefreshStatement::from(statement))
        }
        _ => MvApplicationStatement::Unhandled,
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MvRequestContext<'a> {
    pub current_catalog: Option<&'a str>,
    pub current_database: &'a str,
}

#[derive(Clone, Debug)]
pub enum MvStatementResult {
    Ok,
    Query(QueryResult),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvApplicationErrorKind {
    InvalidRequest,
    Engine,
    Repository,
    Unavailable,
    AlreadyActive,
    TargetGone,
    Corruption,
    RecoveryRequired,
    ShutdownCancelled,
    CommitUnknown,
    KnownCommittedFinalizeFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvApplicationError {
    kind: MvApplicationErrorKind,
    message: String,
}

impl MvApplicationError {
    pub fn new(kind: MvApplicationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> MvApplicationErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvApplicationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvEngineErrorKind {
    InvalidRequest,
    Analysis,
    TargetOperation,
    DescriptorSync,
    CatalogRegistration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvEngineError {
    kind: MvEngineErrorKind,
    message: String,
}

impl MvEngineError {
    pub fn new(kind: MvEngineErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> MvEngineErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvEngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvEngineError {}

#[derive(Clone, Copy, Debug)]
pub struct PrepareMvCreateRequest<'a> {
    pub statement: &'a MvCreateStatement,
    pub context: MvRequestContext<'a>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedMvCreate {
    pub target: MvTarget,
    pub repository_request: CreateMvRepositoryRequest,
}

impl PreparedMvCreate {
    pub fn new(target: MvTarget, repository_request: CreateMvRepositoryRequest) -> Self {
        Self {
            target,
            repository_request,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedMvTarget {
    pub target: MvTarget,
    pub table_uuid: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedMvDefinition {
    pub repository_request: CreateMvRepositoryRequest,
}

pub trait MvApplicationService: Send + Sync {
    fn try_handle_statement(
        &self,
        engine: &dyn MvEngine,
        statement: &MvApplicationStatement,
        context: MvRequestContext<'_>,
    ) -> Result<Option<MvStatementResult>, MvApplicationError>;

    /// Execute a fully SQL-prepared refresh attempt.  This deliberately sits
    /// beside the CREATE-only `MvEngine` port: refresh owns distributed
    /// execution, external publication, and durable intent in the frontend,
    /// not in a widened engine backend.
    fn execute_prepared_refresh(
        &self,
        _refresh: PreparedMvRefresh,
        _connector_context: novarocks_spi::connector::ConnectorRequestContext,
        _execution: &crate::query_execution::request_context::QueryExecutionContext,
    ) -> Result<MvStatementResult, MvApplicationError> {
        Err(MvApplicationError::new(
            MvApplicationErrorKind::Unavailable,
            "frontend MV refresh lifecycle is unavailable",
        ))
    }

    /// Frontend-owned admission of a SQL-prepared refresh. The caller supplies
    /// only the side-effect-free SQL preparation port; the frontend reserves
    /// the attempt identity, persists durable intent, and owns every external
    /// lifecycle phase.
    fn prepare_and_execute_refresh(
        &self,
        _preparation: &dyn MvRefreshPreparationService,
        _statement: MvApplicationStatement,
        _target: MvTarget,
        _connector_context: novarocks_spi::connector::ConnectorRequestContext,
        _execution: &crate::query_execution::request_context::QueryExecutionContext,
    ) -> Result<MvStatementResult, MvApplicationError> {
        Err(MvApplicationError::new(
            MvApplicationErrorKind::Unavailable,
            "frontend MV refresh lifecycle is unavailable",
        ))
    }

    /// Run the bounded frontend-owned startup recovery pass after catalog
    /// attachment and MV target restore.  The default is deliberately a
    /// no-op for unavailable/test application services; production frontend
    /// composition overrides it and retains unresolved attempts as fences.
    fn recover_startup_mv_refreshes(&self) -> Result<(), MvApplicationError> {
        Ok(())
    }
}

pub trait MvEngine: Send + Sync {
    fn prepare_create(
        &self,
        request: PrepareMvCreateRequest<'_>,
        repository: &dyn MvRepository,
    ) -> Result<PreparedMvCreate, MvEngineError>;

    fn create_target(
        &self,
        plan: &PreparedMvCreate,
        operation_id: Uuid,
    ) -> Result<CreatedMvTarget, MvEngineError>;

    fn inspect_created_target(
        &self,
        plan: &PreparedMvCreate,
        target: &CreatedMvTarget,
    ) -> Result<PreparedMvDefinition, MvEngineError>;

    fn sync_target_descriptor(
        &self,
        target: &CreatedMvTarget,
        definition: &crate::mv::persistence::definition::StoredMvDefinition,
    ) -> Result<(), MvEngineError>;

    fn register_target(&self, target: &CreatedMvTarget) -> Result<(), MvEngineError>;

    fn drop_created_target(&self, target: &CreatedMvTarget) -> Result<(), MvEngineError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UnavailableMvApplicationService;

impl MvApplicationService for UnavailableMvApplicationService {
    fn try_handle_statement(
        &self,
        _engine: &dyn MvEngine,
        statement: &MvApplicationStatement,
        _context: MvRequestContext<'_>,
    ) -> Result<Option<MvStatementResult>, MvApplicationError> {
        if matches!(
            statement,
            MvApplicationStatement::Create(_) | MvApplicationStatement::Refresh(_)
        ) {
            return Err(MvApplicationError::new(
                MvApplicationErrorKind::Unavailable,
                MV_REPOSITORY_UNAVAILABLE_MESSAGE,
            ));
        }
        Ok(None)
    }
}
