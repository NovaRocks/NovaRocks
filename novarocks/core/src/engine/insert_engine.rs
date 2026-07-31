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

use novarocks_catalog::schema::ColumnDef;

use crate::connector::backend::ResolvedTable;
use crate::connector::iceberg::commit::{CommitOpKind, CommitOutcome, CommitServiceError};
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::statistics::StatisticsEngine;
use crate::engine::{StandaloneState, iceberg_writer};
use crate::query_execution::request_context::{QueryExecutionContext, RequestContext};
use crate::query_execution::write::WriteCommitInput;
use crate::runtime::query_options::QueryOptions;
use crate::sql::parser::ast::{Literal, ObjectName};

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

/// Iceberg target metadata used by frontend dispatch and shaping.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedInsertTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
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
    NoOp,
    CommitRequired(Arc<dyn IcebergInsertCommit>),
}

/// Iceberg write port used by frontend-owned native INSERT orchestration.
// Design: ADR-0021 (docs/adr/ADR-0021-native-frontend-insert-is-iceberg-only.md)
pub trait InsertEngine: StatisticsEngine + Send + Sync {
    fn resolve_target(&self, request: ResolveInsertTarget) -> Result<ResolvedInsertTarget, String>;

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
    prepared: iceberg_writer::PreparedIcebergWrite,
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
        crate::connector::validate_request_context(&connector_context)?;

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
            catalog: target.catalog,
            namespace: target.namespace,
            table: target.table,
            columns,
        })
    }

    fn prepare_iceberg_write(
        &self,
        request: PrepareIcebergInsert,
    ) -> Result<PreparedIcebergInsert, String> {
        let target = target_backend(&request.target, "iceberg");
        let resolved = resolved_table(&request.target);
        let source = match request.source {
            IcebergInsertSource::Rows(rows) => iceberg_writer::IcebergWriteInput::Rows(
                rows.iter()
                    .map(|row| row.iter().map(insert_value_to_literal).collect())
                    .collect(),
            ),
            IcebergInsertSource::Query(query) => iceberg_writer::IcebergWriteInput::Query(query),
        };
        let overwrite_mode = match request.overwrite_mode {
            InsertOverwriteMode::Append => iceberg_writer::IcebergWriteMode::Append,
            InsertOverwriteMode::FullTable => iceberg_writer::IcebergWriteMode::FullTableOverwrite,
            InsertOverwriteMode::DynamicPartitions => {
                iceberg_writer::IcebergWriteMode::DynamicPartitionOverwrite
            }
        };
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        crate::connector::validate_request_context(&connector_context)?;
        let prepared = iceberg_writer::prepare_iceberg_write(
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
            prepared.prepared.commit_op_kind(),
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
    commit_op_kind: CommitOpKind,
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
    let Some(input) = result.write_commit else {
        return IcebergWriteReport::NoOp;
    };
    let has_files = crate::engine::write_transaction::write_commit_has_files(&input);
    if !has_files && matches!(commit_op_kind, CommitOpKind::FastAppend) {
        return IcebergWriteReport::NoOp;
    }
    let commit: Arc<dyn IcebergInsertCommit> =
        Arc::new(CoreIcebergInsertCommit { input: Some(input) });
    IcebergWriteReport::CommitRequired(commit)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::app_config::ClusterRole;
    use crate::common::types::UniqueId;
    use crate::query_execution::backend::BackendTopologySnapshot;
    use crate::query_execution::cancellation::{QueryCancellationReason, QueryCancellationSource};
    use crate::query_execution::outcome::QueryExecutionResult;
    use crate::query_execution::request_context::{RequestAdmission, RequestContext};
    use crate::query_execution::write::WriteCommitInput;
    use crate::runtime::query_result::QueryResult;
    use crate::sql::optimizer::options::SessionOptimizerSettings;

    fn cancelled_execution() -> QueryExecutionContext {
        let cancellation = QueryCancellationSource::new();
        let request = RequestContext::admit(RequestAdmission::new(
            None,
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(19),
            None,
            cancellation.view(),
            SessionOptimizerSettings::default(),
        ));
        cancellation.request(QueryCancellationReason::ClientDisconnected);
        request.execution().clone()
    }

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
    fn target_resolution_rechecks_cancellation_before_metadata_lookup() {
        let state = Arc::new(StandaloneState::default());
        let error = state
            .resolve_target(ResolveInsertTarget {
                current_catalog: None,
                current_database: "db".to_string(),
                target: InsertTargetName {
                    parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()],
                },
                query_options: None,
                execution: cancelled_execution(),
            })
            .expect_err("cancelled INSERT must fail before connector metadata lookup");

        assert_eq!(error, "connector request was cancelled");
    }

    #[test]
    fn query_execution_result_maps_fileless_overwrite_to_commit_required_handle() {
        let report = iceberg_write_report_from_result(
            QueryExecutionResult {
                query_result: QueryResult::empty(),
                write_commit: Some(WriteCommitInput {
                    write_id: UniqueId { hi: 1, lo: 2 },
                    writers: Vec::new(),
                }),
                write_abort: None,
                fragment_profiles: Vec::new(),
            },
            CommitOpKind::Overwrite,
        );

        let IcebergWriteReport::CommitRequired(handle) = report else {
            panic!("fileless overwrite output must require a commit");
        };
        let handle = handle
            .as_any()
            .downcast_ref::<CoreIcebergInsertCommit>()
            .expect("core commit handle");
        assert!(handle.input.is_some());
    }

    #[test]
    fn query_execution_result_maps_absent_commit_to_noop() {
        let report = iceberg_write_report_from_result(
            QueryExecutionResult {
                query_result: QueryResult::empty(),
                write_commit: None,
                write_abort: None,
                fragment_profiles: Vec::new(),
            },
            CommitOpKind::Overwrite,
        );

        assert!(matches!(report, IcebergWriteReport::NoOp));
    }

    #[test]
    fn query_execution_result_maps_fileless_fast_append_to_noop() {
        let report = iceberg_write_report_from_result(
            QueryExecutionResult {
                query_result: QueryResult::empty(),
                write_commit: Some(WriteCommitInput {
                    write_id: UniqueId { hi: 1, lo: 2 },
                    writers: Vec::new(),
                }),
                write_abort: None,
                fragment_profiles: Vec::new(),
            },
            CommitOpKind::FastAppend,
        );

        assert!(matches!(report, IcebergWriteReport::NoOp));
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
        }, CommitOpKind::FastAppend);

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
        let report = iceberg_write_report_from_result(
            QueryExecutionResult {
                query_result: QueryResult::empty(),
                write_commit: None,
                write_abort: Some(abort),
                fragment_profiles: Vec::new(),
            },
            CommitOpKind::FastAppend,
        );

        let IcebergWriteReport::Aborted {
            has_staged_files, ..
        } = report
        else {
            panic!("writer abort must stay an aborted report");
        };
        assert!(!has_staged_files);
    }
}
