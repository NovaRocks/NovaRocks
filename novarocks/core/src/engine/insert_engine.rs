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

//! Transitional reverse port for the frontend-owned INSERT application flow.
//!
//! The types in this module deliberately expose neither [`super::StandaloneState`]
//! nor connector implementations. The frontend owns INSERT conversion,
//! dispatch, shaping, and transaction orchestration; core retains execution,
//! connector, and external commit truth behind this object-safe boundary.

use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_catalog::schema::ColumnDef;

use crate::connector::iceberg::commit::{CommitOpKind, CommitOutcome, CommitServiceError};
use crate::engine::statistics::StatisticsEngine;
use crate::query_execution::request_context::{QueryExecutionContext, RequestContext};
use crate::runtime::query_options::QueryOptions;

/// Parse one statement through NovaRocks' StarRocks normalizer and return its
/// raw INSERT AST.
///
/// This is intentionally only a recognition primitive. Target resolution,
/// custom command conversion, dispatch, and execution belong to the frontend
/// DML application service.
pub fn parse_insert_statement(sql: &str) -> Result<Option<sqlparser::ast::Insert>, String> {
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Insert(insert) => Ok(Some(insert)),
        _ => Ok(None),
    }
}

/// One admitted INSERT statement at the frontend route boundary.
pub struct InsertRequest<'a> {
    pub statement: &'a sqlparser::ast::Insert,
    pub context: &'a RequestContext,
    pub query_options: Option<&'a QueryOptions>,
}

/// A target name before catalog/backend resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsertTargetName {
    pub parts: Vec<String>,
}

/// Backend capability selected for an INSERT target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertTargetBackend {
    Local,
    StarRocks,
    Iceberg,
}

/// INSERT literal independent of core's legacy custom statement AST.
#[derive(Clone, Debug, PartialEq)]
pub enum InsertValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Date(String),
    Array(Vec<InsertValue>),
    Map(Vec<(InsertValue, InsertValue)>),
    Struct(Vec<InsertValue>),
}

/// Overwrite semantics owned by the frontend INSERT command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertOverwriteMode {
    Append,
    FullTable,
    DynamicPartitions,
}

/// Resolve and load a target using the immutable execution admitted for this
/// statement.
pub struct ResolveInsertTarget {
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub target: InsertTargetName,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

/// Connector-neutral target metadata used by frontend dispatch and shaping.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedInsertTarget {
    pub backend: InsertTargetBackend,
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
    pub supports_pipeline_insert: bool,
}

/// Append already target-ordered literal rows.
pub struct AppendRowsRequest {
    pub target: ResolvedInsertTarget,
    pub rows: Vec<Vec<InsertValue>>,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

/// Execute an INSERT SELECT query with the exact admitted execution identity.
pub struct InsertQueryRequest {
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub query: Box<sqlparser::ast::Query>,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

/// One query-result column exposed for frontend batch shaping.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryInsertColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Query output without core runtime `QueryResult` or `Chunk` internals.
#[derive(Clone, Debug)]
pub struct QueryInsertBatch {
    pub columns: Vec<QueryInsertColumn>,
    pub batches: Vec<RecordBatch>,
}

/// Append a batch already aligned to the target schema.
pub struct AppendBatchRequest {
    pub target: ResolvedInsertTarget,
    pub batch: RecordBatch,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

/// One non-UNION source for an Iceberg INSERT transaction.
#[derive(Clone, Debug, PartialEq)]
pub enum IcebergInsertSource {
    Rows(Vec<Vec<InsertValue>>),
    Query(Box<sqlparser::ast::Query>),
}

/// Prepare an Iceberg INSERT without starting writers or external commit.
pub struct PrepareIcebergInsert {
    pub target: ResolvedInsertTarget,
    pub insert_columns: Vec<String>,
    pub source: IcebergInsertSource,
    pub overwrite_mode: InsertOverwriteMode,
    pub target_ref: String,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

/// Opaque core-owned prepared write payload.
pub trait IcebergPreparedInsert: Send + Sync {}

/// Opaque core-owned commit payload produced by a coordinated write.
pub trait IcebergInsertCommit: Send + Sync {}

/// Stable operation facts required by the frontend transaction runner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergInsertOperation {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub target_ref: String,
    pub attempt_id: String,
    pub commit_op_kind: CommitOpKind,
    pub base_snapshot_id: Option<i64>,
}

/// Prepared operation facts plus a payload that frontend can only return.
pub struct PreparedIcebergInsert {
    pub operation: IcebergInsertOperation,
    pub handle: Arc<dyn IcebergPreparedInsert>,
}

/// Connector-neutral result of the coordinated writer phase.
pub enum IcebergWriteReport {
    Aborted {
        reason: String,
        has_staged_files: bool,
    },
    NoOp(Arc<dyn IcebergInsertCommit>),
    Committable(Arc<dyn IcebergInsertCommit>),
}

/// Transitional one-to-one core domain port used by frontend DML.
// Design: ADR-0017 (docs/adr/ADR-0017-frontend-insert-application-owner.md)
pub trait InsertEngine: StatisticsEngine + Send + Sync {
    fn resolve_target(&self, request: ResolveInsertTarget) -> Result<ResolvedInsertTarget, String>;

    fn append_rows(&self, request: AppendRowsRequest) -> Result<(), String>;

    fn execute_insert_query(&self, request: InsertQueryRequest)
    -> Result<QueryInsertBatch, String>;

    fn append_batch(&self, request: AppendBatchRequest) -> Result<(), String>;

    fn prepare_iceberg_write(
        &self,
        request: PrepareIcebergInsert,
    ) -> Result<PreparedIcebergInsert, String>;

    fn run_iceberg_write(
        &self,
        prepared: &dyn IcebergPreparedInsert,
    ) -> Result<IcebergWriteReport, String>;

    fn commit_iceberg_write(
        &self,
        prepared: &dyn IcebergPreparedInsert,
        commit: &dyn IcebergInsertCommit,
    ) -> Result<CommitOutcome, CommitServiceError>;

    fn finalize_iceberg_write(&self, prepared: &dyn IcebergPreparedInsert) -> Result<(), String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_engine_is_object_safe() {
        fn accepts_object_safe_engine(_engine: Option<&dyn InsertEngine>) {}
        accepts_object_safe_engine(None);
    }

    #[test]
    fn parse_insert_statement_returns_insert_ast() {
        let statement = parse_insert_statement("INSERT INTO db.t VALUES (1)")
            .expect("INSERT should parse")
            .expect("INSERT should be recognized");
        assert!(!statement.overwrite);
    }

    #[test]
    fn parse_insert_statement_returns_none_for_non_insert() {
        assert!(
            parse_insert_statement("SELECT 1")
                .expect("SELECT should parse")
                .is_none()
        );
    }

    #[test]
    fn parse_insert_statement_preserves_dynamic_overwrite_marker_semantics() {
        let statement = parse_insert_statement("INSERT OVERWRITE PARTITIONS TABLE db.t VALUES (1)")
            .expect("dynamic overwrite should parse")
            .expect("dynamic overwrite should be recognized");
        assert!(statement.overwrite);
        let sqlparser::ast::TableObject::TableName(name) = statement.table else {
            panic!("expected table-name INSERT target");
        };
        assert_eq!(
            name.0[0]
                .as_ident()
                .expect("dynamic overwrite marker should be an identifier")
                .value,
            "__nr_op_dyn"
        );
    }

    #[test]
    fn query_request_clones_one_execution_context() {
        let cancellation = crate::query_execution::cancellation::QueryCancellationSource::new();
        let context = crate::query_execution::request_context::RequestContext::admit(
            crate::query_execution::request_context::RequestAdmission::new(
                None,
                "db".to_string(),
                crate::common::app_config::ClusterRole::Fe,
                crate::query_execution::backend::BackendTopologySnapshot::empty(17),
                None,
                cancellation.view(),
                crate::query_execution::request_context::SessionOptimizerSettings::default(),
            ),
        );
        let query = match crate::sql::parser::parse_normalized_sql_raw("SELECT 1")
            .expect("query should parse")
        {
            sqlparser::ast::Statement::Query(query) => query,
            other => panic!("expected query, got {other:?}"),
        };
        let request = InsertQueryRequest {
            current_catalog: None,
            current_database: "db".to_string(),
            query,
            query_options: None,
            execution: context.execution().clone(),
        };

        assert_eq!(request.execution.topology().revision(), 17);
        cancellation.request(
            crate::query_execution::cancellation::QueryCancellationReason::ClientDisconnected,
        );
        assert!(request.execution.cancellation().is_cancelled());
        assert!(context.execution().cancellation().is_cancelled());
    }
}
