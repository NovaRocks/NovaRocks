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

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::kernels::DmlExecutionKernel;
use novarocks_proto::lifecycle::QueryOptions;

/// One parser-owned DELETE variant admitted by the typed statement router.
///
/// The source is retained separately in [`PrepareDeleteRequest`] and may only
/// be sliced through the selected AST node's [`novarocks_parser::Span`].
#[derive(Clone, Copy, Debug)]
pub enum DeleteStatement<'a> {
    Predicate(&'a novarocks_parser::ast::Delete),
    Equality(&'a novarocks_parser::ast::AddEqualityDelete),
}

/// DELETE statements recognized by the frontend command router.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteStatementKind {
    Predicate,
    Equality,
}

impl DeleteStatement<'_> {
    pub const fn kind(self) -> DeleteStatementKind {
        match self {
            Self::Predicate(_) => DeleteStatementKind::Predicate,
            Self::Equality(_) => DeleteStatementKind::Equality,
        }
    }
}

/// One admitted frontend DELETE request. `source` is retained only for exact
/// slices selected by a parser-owned AST span; it must not be reparsed or used
/// to rediscover the statement family.
pub struct PrepareDeleteRequest<'a> {
    pub statement: DeleteStatement<'a>,
    pub source: &'a str,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

impl PrepareDeleteRequest<'_> {
    pub const fn kind(&self) -> DeleteStatementKind {
        self.statement.kind()
    }
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
    pub sql_source: String,
}

pub enum DeleteWriteReport {
    Aborted {
        reason: String,
        has_staged_files: bool,
    },
    NoOp,
    CommitRequired(Arc<dyn DeleteCommit>),
}

/// Borrowed native encoder input for a Core-sealed DELETE request. Holding the
/// guard keeps the exact request carrier unavailable for replacement or
/// execution until Frontend has produced the corresponding bundle.
pub struct DeleteNativeEncoding<'a> {
    inner: DeleteNativeEncodingInner<'a>,
}

enum DeleteNativeEncodingInner<'a> {
    Assembly(
        std::sync::MutexGuard<
            'a,
            Option<crate::query_execution::compiler::PreparedDmlWriteAssembly>,
        >,
    ),
    TestFixture(&'static crate::query_execution::compiler::NativeFragmentEncodingInput),
}

impl DeleteNativeEncoding<'_> {
    pub fn input(
        &self,
    ) -> Result<&crate::query_execution::compiler::NativeFragmentEncodingInput, String> {
        match &self.inner {
            DeleteNativeEncodingInner::Assembly(assembly) => assembly
                .as_ref()
                .map(crate::query_execution::compiler::PreparedDmlWriteAssembly::encoding)
                .ok_or_else(|| "prepared DELETE native assembly was already consumed".to_string()),
            DeleteNativeEncodingInner::TestFixture(input) => Ok(input),
        }
    }

    /// Feature-gated sealed fixture for Frontend DELETE application doubles.
    /// It exposes only immutable encoder input, never a raw plan or mutable
    /// preparation handle.
    #[doc(hidden)]
    pub fn test_fixture() -> Result<DeleteNativeEncoding<'static>, String> {
        use std::sync::OnceLock;

        static INPUT: OnceLock<crate::query_execution::compiler::NativeFragmentEncodingInput> =
            OnceLock::new();
        let input = INPUT.get_or_init(|| {
            let plan = novarocks_sql::planning::dml::native_encoder_test_fixture_plan()
                .expect("test native DELETE fixture plan must seal");
            let prepared =
                crate::query_execution::preparation::prepared_fragment_set_for_native_encode_test(
                    &plan,
                )
                .expect("test native DELETE fixture must prepare");
            crate::query_execution::compiler::NativeFragmentEncodingInput::new(plan, prepared)
        });
        Ok(DeleteNativeEncoding {
            inner: DeleteNativeEncodingInner::TestFixture(input),
        })
    }
}

pub(crate) trait PreparedDeleteExecution: Send + Sync {
    /// Expose the exact write authority this preparation activated, so the
    /// coordinator can fence it before anything is dispatched.
    ///
    /// The default refuses. There is deliberately no unfenced dispatch: an
    /// execution that cannot expose its write authority must not run a writer.
    fn external_fence_authority(
        &self,
    ) -> Result<
        crate::query_execution::dml::external_write_fence::ExternalWriteFenceAuthority,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(
            crate::query_execution::dml::external_write_fence::external_fence_authority_unavailable(
                "DELETE execution does not expose an external operation fence authority",
            ),
        )
    }

    fn native_encoding(
        &self,
    ) -> Result<DeleteNativeEncoding<'_>, crate::dml::error::DmlExecutionError>;
    fn run_with_native_bundle(
        &self,
        native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<crate::query_execution::outcome::QueryExecutionResult, String>;
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

    /// Establish this attempt's external write fence before anything is
    /// dispatched.
    ///
    /// The default fails closed. There is deliberately no unfenced dispatch: an
    /// engine that cannot expose its write authority must not run a writer.
    fn establish_delete_external_fence(
        &self,
        _prepared: &dyn DeletePrepared,
        _proposal: &dyn crate::query_execution::dml::external_write_fence::ExternalWriteFenceProposal,
    ) -> Result<
        novarocks_spi::connector::ConnectorEstablishedWriteFence,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(
            crate::query_execution::dml::external_write_fence::external_fence_authority_unavailable(
                "DELETE engine does not expose an external operation fence authority",
            ),
        )
    }

    fn run_delete(&self, prepared: &dyn DeletePrepared) -> Result<DeleteWriteReport, String>;
    fn delete_native_encoding<'a>(
        &self,
        _prepared: &'a dyn DeletePrepared,
    ) -> Result<DeleteNativeEncoding<'a>, crate::dml::error::DmlExecutionError> {
        Err(crate::dml::error::DmlExecutionError::from(
            "DELETE engine does not expose native encoding input".to_string(),
        ))
    }
    fn run_delete_with_native_bundle(
        &self,
        _prepared: &dyn DeletePrepared,
        _native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<DeleteWriteReport, String> {
        Err("DELETE engine requires Frontend native fragment assembly".to_string())
    }
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

impl DeleteEngine for DmlExecutionKernel {
    fn establish_delete_external_fence(
        &self,
        prepared: &dyn DeletePrepared,
        proposal: &dyn crate::query_execution::dml::external_write_fence::ExternalWriteFenceProposal,
    ) -> Result<
        novarocks_spi::connector::ConnectorEstablishedWriteFence,
        novarocks_spi::connector::ConnectorError,
    > {
        downcast_prepared(prepared)
            .map_err(crate::query_execution::dml::external_write_fence::invalid_fence_request)?
            .execution
            .external_fence_authority()?
            .establish(proposal)
    }

    fn prepare_delete(&self, request: PrepareDeleteRequest<'_>) -> Result<PreparedDelete, String> {
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        let mut prepared = match request.statement {
            DeleteStatement::Predicate(statement) => standard::prepare_delete_statement(
                self,
                statement,
                request.source,
                request.current_catalog.as_deref(),
                &request.current_database,
                &request.execution,
                &connector_context,
            ),
            DeleteStatement::Equality(statement) => equality::prepare_equality_delete_statement(
                self,
                statement,
                request.current_catalog.as_deref(),
                &request.current_database,
                &request.execution,
                &connector_context,
            ),
        }?;
        prepared.sql_source = request.source.to_string();
        Ok(prepared)
    }

    fn run_delete(&self, prepared: &dyn DeletePrepared) -> Result<DeleteWriteReport, String> {
        let _ = prepared;
        Err("DELETE requires Frontend native fragment assembly".to_string())
    }

    fn delete_native_encoding<'a>(
        &self,
        prepared: &'a dyn DeletePrepared,
    ) -> Result<DeleteNativeEncoding<'a>, crate::dml::error::DmlExecutionError> {
        downcast_prepared(prepared)?.execution.native_encoding()
    }

    fn run_delete_with_native_bundle(
        &self,
        prepared: &dyn DeletePrepared,
        native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<DeleteWriteReport, String> {
        let prepared = downcast_prepared(prepared)?;
        let result = prepared.execution.run_with_native_bundle(native_bundle)?;
        delete_write_report_from_result(result)
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

fn delete_write_report_from_result(
    result: crate::query_execution::outcome::QueryExecutionResult,
) -> Result<DeleteWriteReport, String> {
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

struct CorePreparedDelete {
    #[allow(
        dead_code,
        reason = "Retained for staged query-execution DML recovery and connector wiring."
    )]
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
        sql_source: String::new(),
    }
}

fn downcast_prepared(prepared: &dyn DeletePrepared) -> Result<&CorePreparedDelete, String> {
    prepared
        .as_any()
        .downcast_ref::<CorePreparedDelete>()
        .ok_or_else(|| "foreign DELETE prepared handle".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::ast::{DmlStatement, Statement};

    #[test]
    fn typed_variant_determines_delete_kind() {
        let statements =
            novarocks_parser::parse("DELETE FROM t WHERE id = 1").expect("parse DELETE");
        let Statement::Dml(DmlStatement::Delete(delete)) = &statements[0] else {
            panic!("expected DELETE");
        };

        assert_eq!(
            DeleteStatement::Predicate(delete).kind(),
            DeleteStatementKind::Predicate
        );
    }
}
