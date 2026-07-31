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

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_catalog::schema::ColumnDef;

use crate::connector::backend::ResolvedTable;
use crate::connector::iceberg::commit::{CommitOpKind, CommitOutcome, CommitServiceError};
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::statistics::StatisticsEngine;
use crate::engine::{StandaloneState, iceberg_writer};
use crate::query_execution::request_context::{QueryExecutionContext, RequestContext};
use crate::query_execution::write::WriteCommitInput;
use crate::runtime::query_options::QueryOptions;
use crate::sql::parser::ast::{InsertSource, Literal, ObjectName, OverwriteMode};

const DEFAULT_CATALOG_NAME: &str = "default_catalog";

/// Parse one statement through NovaRocks' StarRocks normalizer and return its
/// raw INSERT AST.
///
/// This is intentionally only a recognition primitive. Target resolution,
/// custom command conversion, dispatch, and execution belong to the frontend
/// DML application service.
pub fn parse_insert_statement(sql: &str) -> Result<Option<sqlparser::ast::Insert>, String> {
    let sql = sql.trim_start();
    let keyword_end = sql
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_alphabetic()).then_some(index))
        .unwrap_or(sql.len());
    if !sql[..keyword_end].eq_ignore_ascii_case("insert") {
        return Ok(None);
    }
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Insert(insert) => Ok(Some(insert)),
        _ => Ok(None),
    }
}

/// Encode one constant JSON literal for frontend-owned INSERT conversion.
///
/// The binary format remains an execution-layer concern; frontend receives
/// only opaque bytes and owns the decision to fold `parse_json(...)`.
pub fn encode_insert_variant_json(json_text: &str) -> Result<Vec<u8>, String> {
    crate::exec::variant_encode::encode_json_text_to_variant_bytes(json_text)
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
pub trait IcebergPreparedInsert: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

/// Opaque core-owned commit payload produced by a coordinated write.
pub trait IcebergInsertCommit: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

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

struct CorePreparedIcebergInsert {
    prepared: iceberg_writer::PreparedIcebergInsertWrite,
}

impl IcebergPreparedInsert for CorePreparedIcebergInsert {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct CoreIcebergInsertCommit {
    input: Option<WriteCommitInput>,
}

impl IcebergInsertCommit for CoreIcebergInsertCommit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl InsertEngine for Arc<StandaloneState> {
    fn resolve_target(&self, request: ResolveInsertTarget) -> Result<ResolvedInsertTarget, String> {
        let name = ObjectName {
            parts: request.target.parts,
        };
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;

        if let Some(local) = resolve_local_insert_target(
            &name,
            request.current_catalog.as_deref(),
            &request.current_database,
        )? {
            let table = self
                .catalog_service
                .local()
                .read()
                .expect("standalone catalog read lock")
                .get(&local.database, &local.table)?;
            let backend = match table.source {
                crate::sql::planner::table::ScanSource::StarRocks { .. } => {
                    InsertTargetBackend::StarRocks
                }
                _ => InsertTargetBackend::Local,
            };
            return Ok(ResolvedInsertTarget {
                backend,
                catalog: "default_catalog".to_string(),
                namespace: local.database,
                table: local.table,
                columns: table.columns,
                supports_pipeline_insert: matches!(backend, InsertTargetBackend::StarRocks),
            });
        }

        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            self,
            &name,
            request.current_catalog.as_deref(),
            &request.current_database,
        )?;
        crate::connector::metadata_load_table(
            self.connector_control.as_ref(),
            connector_context,
            &target.catalog,
            &target.namespace,
            &target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?;
        crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table(
            self,
            &target,
            crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Insert,
        )?;
        // Connector metadata intentionally carries only Arrow schema fields.
        // INSERT shaping additionally needs Iceberg write-defaults and logical
        // types, so expose the write-side catalog view through this narrow
        // adapter instead of making frontend depend on connector internals.
        let columns = {
            let entry = self
                .iceberg_catalogs
                .read()
                .map_err(|error| format!("iceberg catalog registry read lock: {error}"))?
                .get(&target.catalog)?;
            crate::connector::iceberg::catalog::registry::load_table(
                &entry,
                &target.namespace,
                &target.table,
            )?
            .columns
        };
        Ok(ResolvedInsertTarget {
            backend: InsertTargetBackend::Iceberg,
            catalog: target.catalog,
            namespace: target.namespace,
            table: target.table,
            columns,
            supports_pipeline_insert: true,
        })
    }

    fn append_rows(&self, request: AppendRowsRequest) -> Result<(), String> {
        let resolved = resolved_table(&request.target);
        let rows = request
            .rows
            .iter()
            .map(|row| row.iter().map(insert_value_to_literal).collect())
            .collect::<Vec<Vec<_>>>();
        match request.target.backend {
            InsertTargetBackend::StarRocks => append_starrocks_rows(
                self,
                &request.target.namespace,
                &request.target.table,
                &rows,
            ),
            InsertTargetBackend::Local => {
                let batch = crate::engine::insert::build_local_insert_batch(
                    &request.target.columns,
                    &rows,
                )?;
                self.connectors
                    .read()
                    .expect("connector registry read")
                    .table_sink("local")?
                    .append_batch(&resolved, batch)
            }
            InsertTargetBackend::Iceberg => {
                Err("Iceberg rows must use prepare_iceberg_write".to_string())
            }
        }
    }

    fn execute_insert_query(
        &self,
        request: InsertQueryRequest,
    ) -> Result<QueryInsertBatch, String> {
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        let result = crate::engine::execute_query_with_catalog_service_with_execution(
            self,
            request.current_catalog.as_deref(),
            &request.current_database,
            &request.query,
            request.query_options,
            &request.execution,
            &connector_context,
        )?;
        Ok(QueryInsertBatch {
            columns: result
                .columns
                .into_iter()
                .map(|column| QueryInsertColumn {
                    name: column.name,
                    data_type: column.data_type,
                    nullable: column.nullable,
                })
                .collect(),
            batches: result.chunks.into_iter().map(|chunk| chunk.batch).collect(),
        })
    }

    fn append_batch(&self, request: AppendBatchRequest) -> Result<(), String> {
        if request.batch.num_rows() == 0 {
            return Ok(());
        }
        let resolved = resolved_table(&request.target);
        match request.target.backend {
            InsertTargetBackend::StarRocks => append_starrocks_batch(
                self,
                &request.target.namespace,
                &request.target.table,
                request.batch,
            ),
            InsertTargetBackend::Local => self
                .connectors
                .read()
                .expect("connector registry read")
                .table_sink("local")?
                .append_batch(&resolved, request.batch),
            InsertTargetBackend::Iceberg => {
                Err("Iceberg batches must use prepare_iceberg_write".to_string())
            }
        }
    }

    fn prepare_iceberg_write(
        &self,
        request: PrepareIcebergInsert,
    ) -> Result<PreparedIcebergInsert, String> {
        if request.target.backend != InsertTargetBackend::Iceberg {
            return Err("prepare_iceberg_write requires an Iceberg target".to_string());
        }
        let target = target_backend(&request.target, "iceberg");
        let resolved = resolved_table(&request.target);
        let source = match request.source {
            IcebergInsertSource::Rows(rows) => InsertSource::Values(
                rows.iter()
                    .map(|row| row.iter().map(insert_value_to_literal).collect())
                    .collect(),
            ),
            IcebergInsertSource::Query(query) => InsertSource::FromQuery(query),
        };
        let overwrite_mode = match request.overwrite_mode {
            InsertOverwriteMode::Append => OverwriteMode::None,
            InsertOverwriteMode::FullTable => OverwriteMode::FullTable,
            InsertOverwriteMode::DynamicPartitions => OverwriteMode::DynamicPartitions,
        };
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        let prepared = iceberg_writer::prepare_iceberg_insert_or_overwrite(
            self,
            &target,
            &resolved,
            &request.insert_columns,
            &source,
            overwrite_mode,
            &request.target_ref,
            Some(request.execution),
            &connector_context,
        )?;
        let operation = IcebergInsertOperation {
            catalog: prepared.target().catalog.clone(),
            namespace: prepared.target().namespace.clone(),
            table: prepared.target().table.clone(),
            target_ref: request.target_ref,
            attempt_id: prepared.attempt_id().to_string(),
            commit_op_kind: prepared.commit_op_kind(),
            base_snapshot_id: prepared.base_snapshot_id(),
        };
        Ok(PreparedIcebergInsert {
            operation,
            handle: Arc::new(CorePreparedIcebergInsert { prepared }),
        })
    }

    fn run_iceberg_write(
        &self,
        prepared: &dyn IcebergPreparedInsert,
    ) -> Result<IcebergWriteReport, String> {
        let prepared = downcast_prepared(prepared)?;
        Ok(iceberg_write_report_from_result(
            prepared.prepared.run_coordinated_write()?,
        ))
    }

    fn commit_iceberg_write(
        &self,
        prepared: &dyn IcebergPreparedInsert,
        commit: &dyn IcebergInsertCommit,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let prepared = downcast_prepared(prepared).map_err(|message| {
            CommitServiceError::known_uncommitted(
                message,
                crate::connector::iceberg::commit::CleanupAttempt::not_attempted(),
            )
        })?;
        let commit = commit
            .as_any()
            .downcast_ref::<CoreIcebergInsertCommit>()
            .ok_or_else(|| {
                CommitServiceError::known_uncommitted(
                    "foreign Iceberg INSERT commit handle".to_string(),
                    crate::connector::iceberg::commit::CleanupAttempt::not_attempted(),
                )
            })?;
        let input = commit.input.as_ref().ok_or_else(|| {
            CommitServiceError::known_uncommitted(
                "Iceberg write produced no commit input".to_string(),
                crate::connector::iceberg::commit::CleanupAttempt::not_attempted(),
            )
        })?;
        prepared.prepared.commit(input)
    }

    fn finalize_iceberg_write(&self, prepared: &dyn IcebergPreparedInsert) -> Result<(), String> {
        downcast_prepared(prepared)?.prepared.finalize()
    }
}

fn resolve_local_insert_target(
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Option<novarocks_catalog::identifier::LocalTableIdentity>, String> {
    let explicitly_local = name.parts.len() == 3
        && novarocks_catalog::identifier::normalize_identifier(&name.parts[0])?
            == DEFAULT_CATALOG_NAME;
    let session_is_local = current_catalog
        .map(novarocks_catalog::identifier::normalize_identifier)
        .transpose()?
        .is_none_or(|catalog| catalog == DEFAULT_CATALOG_NAME);
    let local_parts = if explicitly_local {
        &name.parts[1..]
    } else if session_is_local && name.parts.len() <= 2 {
        name.parts.as_slice()
    } else {
        return Ok(None);
    };
    novarocks_catalog::identifier::resolve_local_table_name(local_parts, current_database).map(Some)
}

fn downcast_prepared(
    prepared: &dyn IcebergPreparedInsert,
) -> Result<&CorePreparedIcebergInsert, String> {
    prepared
        .as_any()
        .downcast_ref::<CorePreparedIcebergInsert>()
        .ok_or_else(|| "foreign Iceberg INSERT prepared handle".to_string())
}

fn iceberg_write_report_from_result(
    result: crate::query_execution::outcome::QueryExecutionResult,
) -> IcebergWriteReport {
    if let Some(abort) = result.write_abort {
        let has_staged_files = abort
            .completed_writer_outputs
            .iter()
            .flat_map(|writer| &writer.iceberg_commits)
            .any(|commit| commit.iceberg_data_file.is_some());
        return IcebergWriteReport::Aborted {
            reason: abort.reason,
            has_staged_files,
        };
    }
    let has_files = result
        .write_commit
        .as_ref()
        .is_some_and(crate::engine::write_transaction::write_commit_has_files);
    let commit: Arc<dyn IcebergInsertCommit> = Arc::new(CoreIcebergInsertCommit {
        input: result.write_commit,
    });
    if has_files {
        IcebergWriteReport::Committable(commit)
    } else {
        IcebergWriteReport::NoOp(commit)
    }
}

fn target_backend(target: &ResolvedInsertTarget, backend_name: &'static str) -> TargetBackend {
    TargetBackend {
        backend_name,
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
    }
}

fn resolved_table(target: &ResolvedInsertTarget) -> ResolvedTable {
    ResolvedTable {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
        columns: target.columns.clone(),
    }
}

fn insert_value_to_literal(value: &InsertValue) -> Literal {
    match value {
        InsertValue::Null => Literal::Null,
        InsertValue::Bool(value) => Literal::Bool(*value),
        InsertValue::Int(value) => Literal::Int(*value),
        InsertValue::Float(value) => Literal::Float(*value),
        InsertValue::String(value) => Literal::String(value.clone()),
        InsertValue::Date(value) => Literal::Date(value.clone()),
        InsertValue::Array(values) => {
            Literal::Array(values.iter().map(insert_value_to_literal).collect())
        }
        InsertValue::Map(values) => Literal::Map(
            values
                .iter()
                .map(|(key, value)| (insert_value_to_literal(key), insert_value_to_literal(value)))
                .collect(),
        ),
        InsertValue::Struct(values) => {
            Literal::Struct(values.iter().map(insert_value_to_literal).collect())
        }
    }
}

#[cfg(feature = "compat")]
fn append_starrocks_rows(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    rows: &[Vec<Literal>],
) -> Result<(), String> {
    crate::connector::starrocks::table::txn::insert_rows_into_starrocks_table(
        state, database, table, rows,
    )
}

#[cfg(not(feature = "compat"))]
fn append_starrocks_rows(
    _state: &Arc<StandaloneState>,
    _database: &str,
    _table: &str,
    _rows: &[Vec<Literal>],
) -> Result<(), String> {
    Err("StarRocks table INSERT requires the compat feature".to_string())
}

#[cfg(feature = "compat")]
fn append_starrocks_batch(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    batch: RecordBatch,
) -> Result<(), String> {
    crate::connector::starrocks::table::txn::insert_batch_into_starrocks_table(
        state, database, table, batch,
    )
}

#[cfg(not(feature = "compat"))]
fn append_starrocks_batch(
    _state: &Arc<StandaloneState>,
    _database: &str,
    _table: &str,
    _batch: RecordBatch,
) -> Result<(), String> {
    Err("StarRocks table INSERT requires the compat feature".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::UniqueId;
    use crate::query_execution::outcome::QueryExecutionResult;
    use crate::query_execution::write::WriteCommitInput;
    use crate::runtime::query_result::QueryResult;

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
    fn parse_insert_statement_does_not_parse_core_only_commands() {
        for sql in [
            "CREATE EXTERNAL CATALOG ice PROPERTIES (\"type\"=\"iceberg\")",
            "ADD BACKEND '127.0.0.1:19170'",
        ] {
            assert!(
                parse_insert_statement(sql)
                    .unwrap_or_else(|error| panic!("`{sql}` must bypass INSERT parsing: {error}"))
                    .is_none(),
                "`{sql}` must remain owned by the core command route"
            );
        }
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

    #[test]
    fn local_target_resolution_uses_default_catalog_session() {
        let resolved = resolve_local_insert_target(
            &ObjectName {
                parts: vec!["orders".to_string()],
            },
            None,
            "sales",
        )
        .unwrap()
        .expect("local target");
        assert_eq!(resolved.database, "sales");
        assert_eq!(resolved.table, "orders");
    }

    #[test]
    fn local_target_resolution_honors_explicit_default_catalog() {
        let resolved = resolve_local_insert_target(
            &ObjectName {
                parts: vec![
                    "default_catalog".to_string(),
                    "sales".to_string(),
                    "orders".to_string(),
                ],
            },
            Some("ice"),
            "ignored",
        )
        .unwrap()
        .expect("explicit local target");
        assert_eq!(resolved.database, "sales");
        assert_eq!(resolved.table, "orders");
    }

    #[test]
    fn local_target_resolution_defers_to_external_catalog() {
        let resolved = resolve_local_insert_target(
            &ObjectName {
                parts: vec!["orders".to_string()],
            },
            Some("ice"),
            "sales",
        )
        .unwrap();
        assert!(resolved.is_none());
    }

    #[test]
    fn query_execution_result_maps_empty_commit_to_opaque_noop_handle() {
        let report = iceberg_write_report_from_result(QueryExecutionResult {
            query_result: QueryResult::empty(),
            write_commit: Some(WriteCommitInput {
                write_id: UniqueId { hi: 1, lo: 2 },
                writers: Vec::new(),
            }),
            write_abort: None,
            fragment_profiles: Vec::new(),
        });

        let IcebergWriteReport::NoOp(handle) = report else {
            panic!("fileless writer output must be a no-op report");
        };
        let handle = handle
            .as_any()
            .downcast_ref::<CoreIcebergInsertCommit>()
            .expect("core commit handle");
        assert!(handle.input.is_some());
    }

    #[test]
    fn query_execution_result_maps_writer_abort_with_staged_files() {
        let report = iceberg_write_report_from_result(QueryExecutionResult {
            query_result: QueryResult::empty(),
            write_commit: None,
            write_abort: Some(
                crate::engine::write_operation_lifecycle::test_support::write_abort_with_data_file(
                ),
            ),
            fragment_profiles: Vec::new(),
        });

        let IcebergWriteReport::Aborted {
            reason,
            has_staged_files,
        } = report
        else {
            panic!("writer abort must stay an aborted report");
        };
        assert!(reason.contains("timed out"));
        assert!(has_staged_files);
    }

    #[test]
    fn query_execution_result_does_not_treat_empty_commit_info_as_staged_file() {
        let mut abort =
            crate::engine::write_operation_lifecycle::test_support::write_abort_with_data_file();
        abort.completed_writer_outputs[0].iceberg_commits =
            vec![crate::proto::novarocks::IcebergCommitInfo::default()];
        let report = iceberg_write_report_from_result(QueryExecutionResult {
            query_result: QueryResult::empty(),
            write_commit: None,
            write_abort: Some(abort),
            fragment_profiles: Vec::new(),
        });

        let IcebergWriteReport::Aborted {
            has_staged_files, ..
        } = report
        else {
            panic!("writer abort must stay an aborted report");
        };
        assert!(!has_staged_files);
    }
}
