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

use std::fmt;

use uuid::Uuid;

use crate::mv::repository::{
    CreateMvRepositoryRequest, MV_REPOSITORY_UNAVAILABLE_MESSAGE, MvRepository, MvTarget,
};
use crate::runtime::query_result::QueryResult;
use crate::sql::mv_refresh::first_refresh::PreparedMvFirstRefreshWrite;
use crate::sql::mv_refresh::incremental::PreparedMvIncrementalWrite;
use crate::sql::mv_refresh::{
    MvRefreshPreparationService, MvRefreshStatement, PreparedDistributedWriteRequest,
    PreparedMvRefresh,
};
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, IcebergPartitionFieldExpr, MaterializedViewRefreshPolicy, Statement,
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

pub(crate) fn project_statement(statement: &Statement) -> MvApplicationStatement {
    match statement {
        Statement::CreateMaterializedView(statement) => {
            MvApplicationStatement::Create(MvCreateStatement::from(statement))
        }
        Statement::RefreshMaterializedView(statement) => {
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

/// Core-owned provider activation and native fragment preparation for a
/// SQL-shaped first-refresh artifact. The frontend owns intent persistence,
/// write-session admission, execution, commit, publication, and cleanup; the
/// port only turns an already frozen artifact into the generic result-free
/// distributed write request after the exact lease is retained.
pub trait MvFirstRefreshWriteActivator: Send + Sync {
    fn bind_first_refresh_write(
        &self,
        prepared: PreparedMvFirstRefreshWrite,
        exact_lease: &novarocks_spi::connector::ConnectorWriteLease,
        execution: &crate::query_execution::request_context::QueryExecutionContext,
    ) -> Result<PreparedDistributedWriteRequest, String>;

    /// Bind one value-only incremental change-stream artifact after the same
    /// retained exact lease has admitted its staging attempt.
    fn bind_incremental_refresh_write(
        &self,
        prepared: PreparedMvIncrementalWrite,
        exact_lease: &novarocks_spi::connector::ConnectorWriteLease,
        execution: &crate::query_execution::request_context::QueryExecutionContext,
    ) -> Result<PreparedDistributedWriteRequest, String>;
}

/// Frontend composition sink installed before Core opens. Core binds its
/// provider activation adapter only after connector control and the engine
/// state are available, avoiding a direct all-in-one call path.
pub trait MvFirstRefreshWriteActivatorSink: Send + Sync {
    fn bind_mv_first_refresh_write_activator(
        &self,
        activator: std::sync::Arc<dyn MvFirstRefreshWriteActivator>,
    ) -> Result<(), String>;
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
