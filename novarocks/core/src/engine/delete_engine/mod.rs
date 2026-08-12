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

//! Transitional reverse port for frontend-owned DELETE application routing.

pub(crate) mod equality;
pub(crate) mod standard;

use std::any::Any;
use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::query_execution::request_context::QueryExecutionContext;
use novarocks_execution::runtime::query_options::QueryOptions;

/// DELETE statements recognized by the frontend command router.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteStatementKind {
    Predicate,
    Equality,
}

/// Recognize a standard SQL DELETE without executing it.
pub fn parse_delete_statement(sql: &str) -> Result<Option<sqlparser::ast::Delete>, String> {
    let sql = sql.trim_start();
    let keyword_end = sql
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_alphabetic()).then_some(index))
        .unwrap_or(sql.len());
    if !sql[..keyword_end].eq_ignore_ascii_case("delete") {
        return Ok(None);
    }
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Delete(delete) => Ok(Some(delete)),
        _ => Ok(None),
    }
}

/// Recognize the NovaRocks equality-delete ALTER TABLE extension.
pub fn parse_equality_delete_statement(sql: &str) -> Result<Option<()>, String> {
    if !crate::engine::statement::looks_like_add_equality_delete(sql) {
        return Ok(None);
    }
    crate::engine::statement::parse_add_equality_delete_sql(sql)?;
    Ok(Some(()))
}

/// One admitted frontend DELETE request. The raw SQL stays inside the narrow
/// reverse port so the frontend never handles core-private DELETE AST payloads.
pub struct PrepareDeleteRequest<'a> {
    pub sql: &'a str,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
    pub kind: DeleteStatementKind,
}

pub trait DeletePrepared: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub trait DeleteCommit: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteOperation {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub target_ref: String,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
}

pub struct PreparedDelete {
    pub operation: DeleteOperation,
    pub handle: Arc<dyn DeletePrepared>,
}

pub enum DeleteWriteReport {
    Aborted {
        reason: String,
        has_staged_files: bool,
    },
    NoOp,
    CommitRequired(Arc<dyn DeleteCommit>),
}

pub(crate) trait PreparedDeleteExecution: Send + Sync {
    fn run(&self) -> Result<crate::query_execution::outcome::QueryExecutionResult, String>;
    fn commit_terminal(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    >;
    fn finalize(&self) -> Result<(), String>;
}

/// One-to-one core capability used only by the frontend DML application owner.
// Design: ADR-0020 (docs/adr/ADR-0020-frontend-delete-application-owner.md)
pub trait DeleteEngine: Send + Sync {
    fn prepare_delete(&self, request: PrepareDeleteRequest<'_>) -> Result<PreparedDelete, String>;
    fn run_delete(&self, prepared: &dyn DeletePrepared) -> Result<DeleteWriteReport, String>;
    fn commit_delete_terminal(
        &self,
        _prepared: &dyn DeletePrepared,
        _commit: &dyn DeleteCommit,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        Err("DELETE engine does not expose a connector terminal outcome".to_string())
    }
    fn finalize_delete(&self, prepared: &dyn DeletePrepared) -> Result<(), String>;
}

impl DeleteEngine for Arc<StandaloneState> {
    fn prepare_delete(&self, request: PrepareDeleteRequest<'_>) -> Result<PreparedDelete, String> {
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        match request.kind {
            DeleteStatementKind::Predicate => {
                let delete = parse_delete_statement(request.sql)?.ok_or_else(|| {
                    "DELETE request did not contain a DELETE statement".to_string()
                })?;
                let statement =
                    crate::engine::statement::convert_sqlparser_delete_to_custom(&delete)?;
                standard::prepare_delete_statement(
                    self,
                    &statement,
                    request.current_catalog.as_deref(),
                    &request.current_database,
                    &request.execution,
                    &connector_context,
                )
            }
            DeleteStatementKind::Equality => {
                let statement =
                    crate::engine::statement::parse_add_equality_delete_sql(request.sql)?;
                equality::prepare_equality_delete_statement(
                    self,
                    &statement,
                    request.current_catalog.as_deref(),
                    &request.current_database,
                    &request.execution,
                    &connector_context,
                )
            }
        }
    }

    fn run_delete(&self, prepared: &dyn DeletePrepared) -> Result<DeleteWriteReport, String> {
        let prepared = downcast_prepared(prepared)?;
        let result = prepared.execution.run()?;
        if let Some(abort) = result.write_abort {
            let has_staged_files = abort
                .completed_writer_outputs
                .iter()
                .any(|writer| !writer.connector_staged_report_frames.is_empty());
            return Ok(DeleteWriteReport::Aborted {
                reason: abort.reason,
                has_staged_files,
            });
        }
        let Some(completion) = result.connector_completion else {
            return Ok(DeleteWriteReport::NoOp);
        };
        let has_staged_output = completion
            .input()
            .map_err(|error| error.to_string())?
            .reports()
            .iter()
            .any(|report| report.summary().artifact_count > 0);
        if !has_staged_output {
            completion
                .finish_known_empty_noop()
                .map_err(|error| error.to_string())?;
            return Ok(DeleteWriteReport::NoOp);
        }
        Ok(DeleteWriteReport::CommitRequired(Arc::new(
            CoreDeleteCommit { completion },
        )))
    }

    fn commit_delete_terminal(
        &self,
        prepared: &dyn DeletePrepared,
        commit: &dyn DeleteCommit,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        let prepared = downcast_prepared(prepared)?;
        let commit = commit
            .as_any()
            .downcast_ref::<CoreDeleteCommit>()
            .ok_or_else(|| "foreign DELETE commit handle".to_string())?;
        prepared.execution.commit_terminal(&commit.completion)
    }

    fn finalize_delete(&self, prepared: &dyn DeletePrepared) -> Result<(), String> {
        downcast_prepared(prepared)?.execution.finalize()
    }
}

struct CorePreparedDelete {
    operation: DeleteOperation,
    execution: Arc<dyn PreparedDeleteExecution>,
}

impl DeletePrepared for CorePreparedDelete {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct CoreDeleteCommit {
    completion: crate::query_execution::ConnectorWriteCompletion,
}

impl DeleteCommit for CoreDeleteCommit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) fn prepared_delete(
    operation: DeleteOperation,
    execution: Arc<dyn PreparedDeleteExecution>,
) -> PreparedDelete {
    PreparedDelete {
        operation: operation.clone(),
        handle: Arc::new(CorePreparedDelete {
            operation,
            execution,
        }),
    }
}

fn downcast_prepared(prepared: &dyn DeletePrepared) -> Result<&CorePreparedDelete, String> {
    prepared
        .as_any()
        .downcast_ref::<CorePreparedDelete>()
        .ok_or_else(|| "foreign DELETE prepared handle".to_string())
}
