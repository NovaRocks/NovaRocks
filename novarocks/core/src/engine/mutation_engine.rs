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

//! Transitional reverse port for frontend-owned UPDATE and MERGE routing.
//!
//! The frontend owns admission and durable operation lifecycle.  Core retains
//! parser-private ASTs, Iceberg metadata, rewrite planning, and the exact
//! connector write session behind opaque handles.

use std::any::Any;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use crate::connector::iceberg::commit::CommitServiceError;
use crate::connector::iceberg::commit::{CommitOpKind, CommitOutcome};
use crate::query_execution::request_context::QueryExecutionContext;
use novarocks_execution::runtime::query_options::QueryOptions;

const PREPARED: u8 = 0;
const STAGED: u8 = 1;
const TERMINAL: u8 = 2;

/// UPDATE and MERGE remain distinct frontend application commands while using
/// the same narrow core reverse port.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationStatementKind {
    Update,
    Merge,
}

/// Recognize a standard SQL UPDATE without exposing the core-private AST.
pub fn parse_update_statement(sql: &str) -> Result<Option<()>, String> {
    let sql = sql.trim_start();
    let keyword_end = sql
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_alphabetic()).then_some(index))
        .unwrap_or(sql.len());
    if !sql[..keyword_end].eq_ignore_ascii_case("update") {
        return Ok(None);
    }
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Update { .. } => Ok(Some(())),
        _ => Ok(None),
    }
}

/// Recognize a NovaRocks MERGE statement without exposing its custom AST.
pub fn parse_merge_statement(sql: &str) -> Result<Option<()>, String> {
    let sql = sql.trim_start();
    let keyword_end = sql
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_alphabetic()).then_some(index))
        .unwrap_or(sql.len());
    if !sql[..keyword_end].eq_ignore_ascii_case("merge") {
        return Ok(None);
    }
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Merge(_) => Ok(Some(())),
        _ => Ok(None),
    }
}

/// One admitted frontend mutation request. Raw SQL remains inside the reverse
/// port so frontend never needs custom ASTs or catalog objects.
pub struct PrepareMutationRequest<'a> {
    pub sql: &'a str,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
    pub kind: MutationStatementKind,
}

pub trait MutationPrepared: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub trait MutationCommit: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub trait MutationAbort: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

/// Stable facts available to the frontend before durable intent is created.
/// The concrete COW/MOR provider action remains inside opaque handles/evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationOperation {
    pub kind: MutationStatementKind,
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub target_ref: String,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
}

pub struct PreparedMutation {
    pub operation: MutationOperation,
    pub handle: Arc<dyn MutationPrepared>,
}

pub enum MutationStageOutcome {
    NoOp,
    AbortRequired {
        reason: String,
        handle: Arc<dyn MutationAbort>,
    },
    CommitRequired(Arc<dyn MutationCommit>),
}

/// One-to-one capability consumed only by the frontend DML application owner.
// Design: ADR-0033 (docs/adr/ADR-0033-frontend-update-merge-application-owner.md)
pub trait MutationEngine: Send + Sync {
    fn prepare_mutation(
        &self,
        request: PrepareMutationRequest<'_>,
    ) -> Result<PreparedMutation, String>;

    fn stage_mutation(
        &self,
        prepared: &dyn MutationPrepared,
    ) -> Result<MutationStageOutcome, String>;

    fn abort_mutation(
        &self,
        _prepared: &dyn MutationPrepared,
        _abort: &dyn MutationAbort,
    ) -> Result<CommitOutcome, CommitServiceError> {
        Err(CommitServiceError::invalid_input(
            "mutation engine does not expose a legacy Iceberg abort result".to_string(),
        ))
    }

    fn abort_mutation_terminal(
        &self,
        _prepared: &dyn MutationPrepared,
        _abort: &dyn MutationAbort,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        Err("mutation engine does not expose a connector terminal abort outcome".to_string())
    }

    fn commit_mutation(
        &self,
        _prepared: &dyn MutationPrepared,
        _commit: &dyn MutationCommit,
    ) -> Result<CommitOutcome, CommitServiceError> {
        Err(CommitServiceError::invalid_input(
            "mutation engine does not expose a legacy Iceberg commit result".to_string(),
        ))
    }

    fn commit_mutation_terminal(
        &self,
        _prepared: &dyn MutationPrepared,
        _commit: &dyn MutationCommit,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        Err("mutation engine does not expose a connector terminal commit outcome".to_string())
    }

    fn finalize_mutation(&self, prepared: &dyn MutationPrepared) -> Result<(), String>;
}

enum PreparedKernel {
    Update(crate::engine::mutation_flow::PreparedUpdateMutation),
    Merge(crate::engine::mutation_flow::PreparedMergeMutation),
}

struct CoreMutationPrepared {
    operation: MutationOperation,
    kernel: Mutex<Option<PreparedKernel>>,
    finalizer: Mutex<Option<Arc<dyn crate::engine::mutation_flow::MutationExecution>>>,
    state: AtomicU8,
}

struct CoreMutationCommit {
    kind: MutationStatementKind,
    attempt_id: String,
    execution: Arc<dyn crate::engine::mutation_flow::MutationExecution>,
    completion: Mutex<Option<crate::query_execution::ConnectorWriteCompletion>>,
}

struct CoreMutationAbort {
    kind: MutationStatementKind,
    attempt_id: String,
    execution: Arc<dyn crate::engine::mutation_flow::MutationExecution>,
    reason: String,
}

impl MutationPrepared for CoreMutationPrepared {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl MutationCommit for CoreMutationCommit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl MutationAbort for CoreMutationAbort {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn prepared_handle(prepared: &dyn MutationPrepared) -> Result<&CoreMutationPrepared, String> {
    prepared
        .as_any()
        .downcast_ref::<CoreMutationPrepared>()
        .ok_or_else(|| "mutation engine received a foreign prepared handle".to_string())
}

fn commit_handle(commit: &dyn MutationCommit) -> Result<&CoreMutationCommit, CommitServiceError> {
    commit
        .as_any()
        .downcast_ref::<CoreMutationCommit>()
        .ok_or_else(|| {
            CommitServiceError::invalid_input(
                "mutation engine received a foreign commit handle".to_string(),
            )
        })
}

fn abort_handle(abort: &dyn MutationAbort) -> Result<&CoreMutationAbort, CommitServiceError> {
    abort
        .as_any()
        .downcast_ref::<CoreMutationAbort>()
        .ok_or_else(|| {
            CommitServiceError::invalid_input(
                "mutation engine received a foreign abort handle".to_string(),
            )
        })
}

impl MutationEngine for Arc<crate::engine::StandaloneState> {
    fn prepare_mutation(
        &self,
        request: PrepareMutationRequest<'_>,
    ) -> Result<PreparedMutation, String> {
        let raw = crate::sql::parser::parse_sql_raw(request.sql)?;
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        let (kernel, target, base_snapshot_id) = match request.kind {
            MutationStatementKind::Update => {
                let stmt = crate::engine::statement::convert_sqlparser_update_to_custom(&raw)?;
                let prepared = crate::engine::mutation_flow::prepare_update_mutation(
                    self,
                    &stmt,
                    request.current_catalog.as_deref(),
                    &request.current_database,
                    &request.execution,
                    &connector_context,
                )?;
                let base_snapshot_id = if prepared.target_ref == "main" {
                    prepared
                        .table
                        .metadata()
                        .current_snapshot()
                        .map(|snapshot| snapshot.snapshot_id())
                } else {
                    novarocks_connector_iceberg::ref_snapshot::resolve_branch_head_snapshot_id(
                        prepared.table.metadata(),
                        &prepared.target_ref,
                    )?
                };
                (
                    PreparedKernel::Update(prepared),
                    stmt.table,
                    base_snapshot_id,
                )
            }
            MutationStatementKind::Merge => {
                let stmt = crate::engine::statement::convert_sqlparser_merge_to_custom(&raw)?;
                let prepared = crate::engine::mutation_flow::prepare_merge_mutation(
                    self,
                    &stmt,
                    request.current_catalog.as_deref(),
                    &request.current_database,
                    &request.execution,
                    &connector_context,
                )?;
                let base_snapshot_id = prepared
                    .table
                    .metadata()
                    .current_snapshot()
                    .map(|snapshot| snapshot.snapshot_id());
                (
                    PreparedKernel::Merge(prepared),
                    stmt.table,
                    base_snapshot_id,
                )
            }
        };
        let (catalog, namespace, table, target_ref) = match &kernel {
            PreparedKernel::Update(prepared) => (
                prepared.target.catalog.clone(),
                prepared.target.namespace.clone(),
                prepared.target.table.clone(),
                prepared.target_ref.clone(),
            ),
            PreparedKernel::Merge(prepared) => (
                prepared.target.catalog.clone(),
                prepared.target.namespace.clone(),
                prepared.target.table.clone(),
                "main".to_string(),
            ),
        };
        let operation = MutationOperation {
            kind: request.kind,
            catalog,
            namespace,
            table,
            target_ref,
            attempt_id: uuid::Uuid::new_v4().to_string(),
            base_snapshot_id,
        };
        let handle: Arc<dyn MutationPrepared> = Arc::new(CoreMutationPrepared {
            operation: operation.clone(),
            kernel: Mutex::new(Some(kernel)),
            finalizer: Mutex::new(None),
            state: AtomicU8::new(PREPARED),
        });
        Ok(PreparedMutation { operation, handle })
    }

    fn stage_mutation(
        &self,
        prepared: &dyn MutationPrepared,
    ) -> Result<MutationStageOutcome, String> {
        let prepared = prepared_handle(prepared)?;
        if prepared
            .state
            .compare_exchange(PREPARED, STAGED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(
                "mutation prepared handle has already reached a terminal stage".to_string(),
            );
        }
        let kernel = prepared
            .kernel
            .lock()
            .expect("mutation prepared kernel lock poisoned")
            .take()
            .ok_or_else(|| "mutation prepared handle has no pending kernel".to_string())?;
        match kernel {
            PreparedKernel::Update(kernel) => {
                match crate::engine::mutation_flow::stage_prepared_update_mutation(self, kernel)? {
                    crate::engine::mutation_flow::MutationStagedWrite::NoOp => {
                        prepared.state.store(TERMINAL, Ordering::Release);
                        Ok(MutationStageOutcome::NoOp)
                    }
                    crate::engine::mutation_flow::MutationStagedWrite::AbortRequired {
                        reason,
                        execution,
                    } => Ok(MutationStageOutcome::AbortRequired {
                        handle: Arc::new(CoreMutationAbort {
                            kind: prepared.operation.kind,
                            attempt_id: prepared.operation.attempt_id.clone(),
                            execution,
                            reason: reason.clone(),
                        }),
                        reason,
                    }),
                    crate::engine::mutation_flow::MutationStagedWrite::CommitRequired {
                        execution,
                        completion,
                    } => Ok(MutationStageOutcome::CommitRequired(Arc::new(
                        CoreMutationCommit {
                            kind: prepared.operation.kind,
                            attempt_id: prepared.operation.attempt_id.clone(),
                            execution,
                            completion: Mutex::new(Some(completion)),
                        },
                    ))),
                }
            }
            PreparedKernel::Merge(kernel) => {
                match crate::engine::mutation_flow::stage_prepared_merge_mutation(self, kernel)? {
                    crate::engine::mutation_flow::MutationStagedWrite::NoOp => {
                        prepared.state.store(TERMINAL, Ordering::Release);
                        Ok(MutationStageOutcome::NoOp)
                    }
                    crate::engine::mutation_flow::MutationStagedWrite::AbortRequired {
                        reason,
                        execution,
                    } => Ok(MutationStageOutcome::AbortRequired {
                        handle: Arc::new(CoreMutationAbort {
                            kind: prepared.operation.kind,
                            attempt_id: prepared.operation.attempt_id.clone(),
                            execution,
                            reason: reason.clone(),
                        }),
                        reason,
                    }),
                    crate::engine::mutation_flow::MutationStagedWrite::CommitRequired {
                        execution,
                        completion,
                    } => Ok(MutationStageOutcome::CommitRequired(Arc::new(
                        CoreMutationCommit {
                            kind: prepared.operation.kind,
                            attempt_id: prepared.operation.attempt_id.clone(),
                            execution,
                            completion: Mutex::new(Some(completion)),
                        },
                    ))),
                }
            }
        }
    }

    fn abort_mutation(
        &self,
        prepared: &dyn MutationPrepared,
        abort: &dyn MutationAbort,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let prepared = prepared_handle(prepared).map_err(CommitServiceError::invalid_input)?;
        let abort = abort_handle(abort)?;
        if prepared.operation.kind != abort.kind
            || prepared.operation.attempt_id != abort.attempt_id
        {
            return Err(CommitServiceError::invalid_input(
                "mutation abort handle does not belong to this prepared mutation".to_string(),
            ));
        }
        if prepared.state.swap(TERMINAL, Ordering::AcqRel) == TERMINAL {
            return Err(CommitServiceError::invalid_input(
                "mutation abort was decided more than once".to_string(),
            ));
        }
        *prepared
            .finalizer
            .lock()
            .expect("mutation finalizer lock poisoned") = Some(Arc::clone(&abort.execution));
        abort.execution.abort(abort.reason.clone())
    }

    fn abort_mutation_terminal(
        &self,
        prepared: &dyn MutationPrepared,
        abort: &dyn MutationAbort,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        let prepared = prepared_handle(prepared)?;
        let abort = abort_handle(abort)
            .map_err(|error| format!("invalid mutation abort handle: {error:?}"))?;
        if prepared.operation.kind != abort.kind
            || prepared.operation.attempt_id != abort.attempt_id
        {
            return Err(
                "mutation abort handle does not belong to this prepared mutation".to_string(),
            );
        }
        if prepared.state.swap(TERMINAL, Ordering::AcqRel) == TERMINAL {
            return Err("mutation abort was decided more than once".to_string());
        }
        *prepared
            .finalizer
            .lock()
            .expect("mutation finalizer lock poisoned") = Some(Arc::clone(&abort.execution));
        abort.execution.abort_terminal()
    }

    fn commit_mutation(
        &self,
        prepared: &dyn MutationPrepared,
        commit: &dyn MutationCommit,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let prepared = prepared_handle(prepared).map_err(CommitServiceError::invalid_input)?;
        let commit = commit_handle(commit).map_err(|error| {
            CommitServiceError::invalid_input(format!("invalid mutation commit handle: {error:?}"))
        })?;
        if prepared.operation.kind != commit.kind
            || prepared.operation.attempt_id != commit.attempt_id
        {
            return Err(CommitServiceError::invalid_input(
                "mutation commit handle does not belong to this prepared mutation".to_string(),
            ));
        }
        if prepared.state.swap(TERMINAL, Ordering::AcqRel) == TERMINAL {
            return Err(CommitServiceError::invalid_input(
                "mutation commit was decided more than once".to_string(),
            ));
        }
        *prepared
            .finalizer
            .lock()
            .expect("mutation finalizer lock poisoned") = Some(Arc::clone(&commit.execution));
        let completion = commit
            .completion
            .lock()
            .expect("mutation commit completion lock poisoned")
            .take()
            .ok_or_else(|| {
                CommitServiceError::invalid_input(
                    "mutation commit completion was already consumed".to_string(),
                )
            })?;
        commit.execution.commit(&completion)
    }

    fn commit_mutation_terminal(
        &self,
        prepared: &dyn MutationPrepared,
        commit: &dyn MutationCommit,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        let prepared = prepared_handle(prepared)?;
        let commit = commit_handle(commit)
            .map_err(|error| format!("invalid mutation commit handle: {error:?}"))?;
        if prepared.operation.kind != commit.kind
            || prepared.operation.attempt_id != commit.attempt_id
        {
            return Err(
                "mutation commit handle does not belong to this prepared mutation".to_string(),
            );
        }
        if prepared.state.swap(TERMINAL, Ordering::AcqRel) == TERMINAL {
            return Err("mutation commit was decided more than once".to_string());
        }
        *prepared
            .finalizer
            .lock()
            .expect("mutation finalizer lock poisoned") = Some(Arc::clone(&commit.execution));
        let completion = commit
            .completion
            .lock()
            .expect("mutation commit completion lock poisoned")
            .take()
            .ok_or_else(|| "mutation commit completion was already consumed".to_string())?;
        commit.execution.commit_terminal(&completion)
    }

    fn finalize_mutation(&self, prepared: &dyn MutationPrepared) -> Result<(), String> {
        let prepared = prepared_handle(prepared)?;
        if prepared.state.load(Ordering::Acquire) != TERMINAL {
            return Err("mutation finalize requires a terminal commit decision".to_string());
        }
        let execution = prepared
            .finalizer
            .lock()
            .expect("mutation finalizer lock poisoned")
            .take()
            .ok_or_else(|| "mutation finalizer has no committed execution".to_string())?;
        execution.finalize()
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::{
        CoreMutationAbort, CoreMutationPrepared, MutationAbort, MutationEngine, MutationOperation,
        MutationStatementKind, STAGED, parse_merge_statement, parse_update_statement,
    };
    use crate::connector::iceberg::commit::CommitServiceError;
    use crate::connector::iceberg::commit::{CommitOpKind, CommitOutcome};
    use crate::engine::mutation_flow::MutationExecution;
    use crate::query_execution::ConnectorWriteCompletion;
    use crate::query_execution::outcome::QueryExecutionResult;
    use novarocks_spi::connector::{ConnectorWriteAbortOutcome, ExternalMutationFinalization};

    struct TestExecution {
        abort_outcome: Mutex<Option<Result<CommitOutcome, CommitServiceError>>>,
        abort_calls: AtomicUsize,
    }

    impl TestExecution {
        fn known_uncommitted() -> Self {
            Self {
                abort_outcome: Mutex::new(Some(Ok(CommitOutcome {
                    new_snapshot_id: 0,
                    written_manifest_paths: Vec::new(),
                }))),
                abort_calls: AtomicUsize::new(0),
            }
        }
    }

    impl MutationExecution for TestExecution {
        fn stage(&self) -> Result<QueryExecutionResult, String> {
            Err("test execution must not stage".to_string())
        }

        fn abort(&self, _reason: String) -> Result<CommitOutcome, CommitServiceError> {
            self.abort_calls.fetch_add(1, Ordering::AcqRel);
            self.abort_outcome
                .lock()
                .expect("test abort outcome lock")
                .take()
                .expect("abort called only once")
        }

        fn abort_terminal(&self) -> Result<ConnectorWriteAbortOutcome, String> {
            self.abort_calls.fetch_add(1, Ordering::AcqRel);
            Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
            })
        }

        fn commit(
            &self,
            _completion: &ConnectorWriteCompletion,
        ) -> Result<CommitOutcome, CommitServiceError> {
            panic!("test execution must not commit")
        }

        fn finalize(&self) -> Result<(), String> {
            Ok(())
        }
    }

    struct ForeignAbort;

    impl MutationAbort for ForeignAbort {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn prepared(kind: MutationStatementKind, attempt_id: &str) -> CoreMutationPrepared {
        CoreMutationPrepared {
            operation: MutationOperation {
                kind,
                catalog: "iceberg".to_string(),
                namespace: "db".to_string(),
                table: "t".to_string(),
                target_ref: "main".to_string(),
                attempt_id: attempt_id.to_string(),
                base_snapshot_id: Some(7),
            },
            kernel: Mutex::new(None),
            finalizer: Mutex::new(None),
            state: super::AtomicU8::new(STAGED),
        }
    }

    fn abort_handle(
        kind: MutationStatementKind,
        attempt_id: &str,
        execution: Arc<TestExecution>,
    ) -> CoreMutationAbort {
        CoreMutationAbort {
            kind,
            attempt_id: attempt_id.to_string(),
            execution,
            reason: "test staging failure".to_string(),
        }
    }

    #[test]
    fn recognition_is_statement_specific() {
        assert!(
            parse_update_statement("UPDATE t SET k = 1")
                .unwrap()
                .is_some()
        );
        assert!(
            parse_update_statement("MERGE INTO t USING s ON t.k = s.k")
                .unwrap()
                .is_none()
        );
        assert!(
            parse_merge_statement("UPDATE t SET k = 1")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn abort_rejects_foreign_and_cross_statement_handles_without_consuming_prepared_state() {
        let engine = Arc::new(crate::engine::StandaloneState::default());
        let prepared = prepared(MutationStatementKind::Update, "update-attempt");
        let execution = Arc::new(TestExecution::known_uncommitted());

        let foreign = ForeignAbort;
        let error = engine.abort_mutation(&prepared, &foreign).unwrap_err();
        assert!(matches!(error, CommitServiceError::InvalidInput { .. }));
        assert_eq!(prepared.state.load(Ordering::Acquire), STAGED);

        let merge_abort = abort_handle(MutationStatementKind::Merge, "update-attempt", execution);
        let error = engine.abort_mutation(&prepared, &merge_abort).unwrap_err();
        assert!(matches!(error, CommitServiceError::InvalidInput { .. }));
        assert_eq!(prepared.state.load(Ordering::Acquire), STAGED);
    }

    #[test]
    fn abort_consumes_the_exact_handle_once_and_keeps_its_execution_for_finalization() {
        let engine = Arc::new(crate::engine::StandaloneState::default());
        let prepared = prepared(MutationStatementKind::Update, "update-attempt");
        let execution = Arc::new(TestExecution::known_uncommitted());
        let abort = abort_handle(
            MutationStatementKind::Update,
            "update-attempt",
            Arc::clone(&execution),
        );

        let outcome = engine.abort_mutation(&prepared, &abort).unwrap();
        assert_eq!(outcome.new_snapshot_id, 0);
        assert_eq!(execution.abort_calls.load(Ordering::Acquire), 1);
        assert_eq!(prepared.state.load(Ordering::Acquire), super::TERMINAL);
        engine.finalize_mutation(&prepared).unwrap();

        let error = engine.abort_mutation(&prepared, &abort).unwrap_err();
        assert!(matches!(error, CommitServiceError::InvalidInput { .. }));
        assert_eq!(execution.abort_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn terminal_abort_consumes_the_exact_handle_once_and_returns_connector_truth() {
        let engine = Arc::new(crate::engine::StandaloneState::default());
        let prepared = prepared(MutationStatementKind::Update, "update-attempt");
        let execution = Arc::new(TestExecution::known_uncommitted());
        let abort = abort_handle(
            MutationStatementKind::Update,
            "update-attempt",
            Arc::clone(&execution),
        );

        let outcome = engine
            .abort_mutation_terminal(&prepared, &abort)
            .expect("terminal abort outcome");
        assert_eq!(
            outcome,
            ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
            }
        );
        assert_eq!(execution.abort_calls.load(Ordering::Acquire), 1);
        assert_eq!(prepared.state.load(Ordering::Acquire), super::TERMINAL);

        let error = engine
            .abort_mutation_terminal(&prepared, &abort)
            .expect_err("terminal abort must be one-shot");
        assert!(error.contains("decided more than once"));
        assert_eq!(execution.abort_calls.load(Ordering::Acquire), 1);
    }
}
