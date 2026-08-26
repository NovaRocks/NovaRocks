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

use crate::mv::domain::persistence::descriptor::MvDescriptorV3;
use crate::mv::domain::repository::{
    CreateMvRepositoryRequest, MV_REPOSITORY_UNAVAILABLE_MESSAGE, MvRepository, MvTarget,
};
use crate::runtime::query_result::QueryResult;
use novarocks_parser::ast::{
    LiteralKind, MaterializedViewPartitionArgument, MaterializedViewPartitionField, Query,
};
use novarocks_sql::semantic::IcebergPartitionFieldExpr;

/// Join refresh shape retained until query assembly admits exact connector
/// bindings for the corresponding write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvIncrementalJoinMode {
    AppendOnly,
    Coalesce,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvIncrementalWriteMode {
    FastAppend,
    RowDelta,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvIncrementalRewriteEvidence {
    None,
    Aggregate,
    JoinAggregate,
    BranchUnionAggregate,
}

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

impl From<&MaterializedViewPartitionField> for MvCreatePartitionField {
    fn from(value: &MaterializedViewPartitionField) -> Self {
        let (transform, arguments) = match value {
            MaterializedViewPartitionField::Identity(column) => {
                return Self::Identity {
                    column: normalized_partition_identifier(&column.value),
                };
            }
            MaterializedViewPartitionField::Transform {
                name, arguments, ..
            } => (name.value.to_ascii_lowercase(), arguments.as_slice()),
        };
        let column = partition_column_argument(arguments, &transform);
        match transform.as_str() {
            "identity" => Self::Identity { column },
            "year" => Self::Year { column },
            "month" => Self::Month { column },
            "day" => Self::Day { column },
            "hour" => Self::Hour { column },
            "void" => Self::Void { column },
            "bucket" => Self::Bucket {
                column,
                num_buckets: partition_u32_argument(arguments, "bucket"),
            },
            "truncate" => Self::Truncate {
                column,
                width: partition_u32_argument(arguments, "truncate"),
            },
            _ => unreachable!("MV partition transform was validated during SQL admission"),
        }
    }
}

impl From<&MvCreatePartitionField> for IcebergPartitionFieldExpr {
    fn from(value: &MvCreatePartitionField) -> Self {
        match value {
            MvCreatePartitionField::Identity { column } => Self::Identity {
                column: column.clone(),
            },
            MvCreatePartitionField::Year { column } => Self::Year {
                column: column.clone(),
            },
            MvCreatePartitionField::Month { column } => Self::Month {
                column: column.clone(),
            },
            MvCreatePartitionField::Day { column } => Self::Day {
                column: column.clone(),
            },
            MvCreatePartitionField::Hour { column } => Self::Hour {
                column: column.clone(),
            },
            MvCreatePartitionField::Bucket {
                column,
                num_buckets,
            } => Self::Bucket {
                column: column.clone(),
                num_buckets: *num_buckets,
            },
            MvCreatePartitionField::Truncate { column, width } => Self::Truncate {
                column: column.clone(),
                width: *width,
            },
            MvCreatePartitionField::Void { column } => Self::Void {
                column: column.clone(),
            },
        }
    }
}

fn normalized_partition_identifier(value: &str) -> String {
    novarocks_types::naming::normalize_identifier(value)
        .expect("MV partition identifier was validated during SQL admission")
}

fn partition_column_argument(
    arguments: &[MaterializedViewPartitionArgument],
    transform: &str,
) -> String {
    let Some(MaterializedViewPartitionArgument::Ident(column)) = arguments.first() else {
        unreachable!("MV {transform} partition transform was validated during SQL admission");
    };
    normalized_partition_identifier(&column.value)
}

fn partition_u32_argument(arguments: &[MaterializedViewPartitionArgument], transform: &str) -> u32 {
    let Some(MaterializedViewPartitionArgument::Literal(value)) = arguments.get(1) else {
        unreachable!("MV {transform} partition transform was validated during SQL admission");
    };
    let LiteralKind::Number(value) = &value.kind else {
        unreachable!("MV {transform} partition transform was validated during SQL admission");
    };
    let value = value
        .parse::<u32>()
        .expect("MV partition numeric argument was validated during SQL admission");
    assert!(
        value > 0,
        "MV partition numeric argument was validated during SQL admission"
    );
    value
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

/// Frontend-owned request for dropping one admitted materialized view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvDropStatement {
    pub name_parts: Vec<String>,
    pub if_exists: bool,
}

/// Frontend-owned admitted materialized-view alteration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvAlterAction {
    SetRefresh(MvCreateRefreshPolicy),
    SetProperties(Vec<(String, String)>),
    PauseRefresh,
    ResumeRefresh,
    Repartition(Vec<MvCreatePartitionField>),
}

/// Frontend-owned request for altering one admitted materialized view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvAlterStatement {
    pub name_parts: Vec<String>,
    pub action: MvAlterAction,
}

// Design: ADR-0101 (docs/adr/ADR-0101-native-sql-language-authority-and-owner-boundaries.md)
/// Frontend-owned refresh request. The request keeps the admitted target name
/// distinct from SQL planning's immutable refresh semantic value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshRequest {
    pub name_parts: Vec<String>,
    pub full: bool,
}

impl MvRefreshRequest {
    pub fn sql_refresh_statement(&self) -> novarocks_sql::planning::mv::MvRefreshStatement {
        novarocks_sql::planning::mv::MvRefreshStatement {
            name_parts: self.name_parts.clone(),
            full: self.full,
        }
    }
}

/// Frontend-owned request for listing materialized views.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvShowStatement {
    pub database: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MvCreateStatement {
    pub name_parts: Vec<String>,
    pub if_not_exists: bool,
    pub partition_by: Option<Vec<MvCreatePartitionField>>,
    pub distribution: Option<MvCreateDistribution>,
    pub refresh_policy: MvCreateRefreshPolicy,
    pub select_sql: String,
    pub select_query: Query,
    pub properties: Vec<(String, String)>,
    pub primary_key: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "The SQL-facing statement carrier retains its direct create payload to preserve the existing application boundary."
)]
pub enum MvApplicationStatement {
    Create(MvCreateStatement),
    Unhandled,
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
    /// The complete lake authority assembled only after exact target
    /// observation. It must commit before the StateStore projection exists.
    pub descriptor: MvDescriptorV3,
}

pub trait MvApplicationService: Send + Sync {
    fn try_handle_statement(
        &self,
        engine: &dyn MvEngine,
        statement: &MvApplicationStatement,
        context: MvRequestContext<'_>,
    ) -> Result<Option<MvStatementResult>, MvApplicationError>;
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
        descriptor: &MvDescriptorV3,
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
        if matches!(statement, MvApplicationStatement::Create(_)) {
            return Err(MvApplicationError::new(
                MvApplicationErrorKind::Unavailable,
                MV_REPOSITORY_UNAVAILABLE_MESSAGE,
            ));
        }
        Ok(None)
    }
}
