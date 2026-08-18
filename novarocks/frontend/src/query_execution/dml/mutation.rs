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

use crate::common::admitted_query_context::QueryExecutionContext;
use novarocks_protocol::lifecycle::QueryOptions;

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
    match novarocks_sql::planning::dml::parse_raw_statement(sql)? {
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
    match novarocks_sql::planning::dml::parse_raw_statement(sql)? {
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

/// Frontend-owned native encoder for one Core-sealed mutation plan. Core never
/// acquires a second binding or fabricates native bytes; it accepts only the
/// bundle produced from this immutable input.
pub trait MutationNativeFragmentEncoder: Send + Sync {
    fn encode(
        &self,
        input: &crate::query_execution::compiler::NativeFragmentEncodingInput,
    ) -> Result<crate::query_execution::native_fragment::NativeFragmentAttachment, String>;
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

    /// Stage through the Frontend-owned native encoding boundary. The default
    /// preserves narrow test doubles that never materialize a Core plan.
    fn stage_mutation_with_native_encoder(
        &self,
        prepared: &dyn MutationPrepared,
        _encoder: &dyn MutationNativeFragmentEncoder,
    ) -> Result<MutationStageOutcome, String> {
        self.stage_mutation(prepared)
    }

    fn abort_mutation_terminal(
        &self,
        _prepared: &dyn MutationPrepared,
        _abort: &dyn MutationAbort,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        Err("mutation engine does not expose a connector terminal abort outcome".to_string())
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

    /// Establish this attempt's external write fence before anything is
    /// dispatched.
    ///
    /// UPDATE and MERGE derive their write lease at preparation precisely so
    /// this can happen before staging: `derive_write_lease` mints a fresh fence
    /// cell on every call, so a fence established on a lease derived later would
    /// not be the one the commit travels through.
    ///
    /// The default fails closed. There is deliberately no unfenced dispatch.
    fn establish_mutation_external_fence(
        &self,
        _prepared: &dyn MutationPrepared,
        _proposal: &dyn crate::query_execution::dml::external_write_fence::ExternalWriteFenceProposal,
    ) -> Result<
        novarocks_spi::connector::ConnectorEstablishedWriteFence,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(
            crate::query_execution::dml::external_write_fence::external_fence_authority_unavailable(
                "mutation engine does not expose an external operation fence authority",
            ),
        )
    }

    fn finalize_mutation(&self, prepared: &dyn MutationPrepared) -> Result<(), String>;
}

enum PreparedKernel {
    Update(crate::query_execution::dml::mutation_flow::PreparedUpdateMutation),
    Merge(crate::query_execution::dml::mutation_flow::PreparedMergeMutation),
}

struct CoreMutationPrepared {
    operation: MutationOperation,
    kernel: Mutex<Option<PreparedKernel>>,
    finalizer:
        Mutex<Option<Arc<dyn crate::query_execution::dml::mutation_flow::MutationExecution>>>,
    state: AtomicU8,
}

struct CoreMutationCommit {
    kind: MutationStatementKind,
    attempt_id: String,
    execution: Arc<dyn crate::query_execution::dml::mutation_flow::MutationExecution>,
    completion: Mutex<Option<crate::query_execution::ConnectorWriteCompletion>>,
}

struct CoreMutationAbort {
    kind: MutationStatementKind,
    attempt_id: String,
    execution: Arc<dyn crate::query_execution::dml::mutation_flow::MutationExecution>,
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

fn commit_handle(commit: &dyn MutationCommit) -> Result<&CoreMutationCommit, String> {
    commit
        .as_any()
        .downcast_ref::<CoreMutationCommit>()
        .ok_or_else(|| "mutation engine received a foreign commit handle".to_string())
}

fn abort_handle(abort: &dyn MutationAbort) -> Result<&CoreMutationAbort, String> {
    abort
        .as_any()
        .downcast_ref::<CoreMutationAbort>()
        .ok_or_else(|| "mutation engine received a foreign abort handle".to_string())
}

impl MutationEngine for crate::query_execution::kernels::DmlExecutionKernel {
    fn establish_mutation_external_fence(
        &self,
        prepared: &dyn MutationPrepared,
        proposal: &dyn crate::query_execution::dml::external_write_fence::ExternalWriteFenceProposal,
    ) -> Result<
        novarocks_spi::connector::ConnectorEstablishedWriteFence,
        novarocks_spi::connector::ConnectorError,
    > {
        let handle = prepared_handle(prepared)
            .map_err(crate::query_execution::dml::external_write_fence::invalid_fence_request)?;
        let kernel = handle.kernel.lock().map_err(|error| {
            crate::query_execution::dml::external_write_fence::invalid_fence_request(format!(
                "mutation prepared kernel lock: {error}"
            ))
        })?;
        let kernel = kernel.as_ref().ok_or_else(|| {
            crate::query_execution::dml::external_write_fence::invalid_fence_request(
                "mutation prepared kernel was already consumed".to_string(),
            )
        })?;
        match kernel {
            PreparedKernel::Update(update) => update.external_fence_authority()?,
            PreparedKernel::Merge(merge) => merge.external_fence_authority()?,
        }
        .establish(proposal)
    }

    fn prepare_mutation(
        &self,
        request: PrepareMutationRequest<'_>,
    ) -> Result<PreparedMutation, String> {
        let raw = novarocks_sql::planning::dml::parse_raw_statement(request.sql)?;
        let connector_context = novarocks::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        let (kernel, target, base_snapshot_id) = match request.kind {
            MutationStatementKind::Update => {
                let stmt =
                    novarocks::catalog_application::statement::convert_sqlparser_update_to_custom(
                        &raw,
                    )?;
                let prepared = crate::query_execution::dml::mutation_flow::prepare_update_mutation(
                    self,
                    &stmt,
                    request.current_catalog.as_deref(),
                    &request.current_database,
                    &request.execution,
                    &connector_context,
                )?;
                // The provider signed this during admission for the exact
                // target ref; the journal records that value rather than
                // re-reading a table handle here.
                let base_snapshot_id = prepared.admitted_base_snapshot_id;
                (
                    PreparedKernel::Update(prepared),
                    stmt.table,
                    base_snapshot_id,
                )
            }
            MutationStatementKind::Merge => {
                let stmt =
                    novarocks::catalog_application::statement::convert_sqlparser_merge_to_custom(
                        &raw,
                    )?;
                let prepared = crate::query_execution::dml::mutation_flow::prepare_merge_mutation(
                    self,
                    &stmt,
                    request.current_catalog.as_deref(),
                    &request.current_database,
                    &request.execution,
                    &connector_context,
                )?;
                let base_snapshot_id = prepared.admitted_base_snapshot_id;
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
        _prepared: &dyn MutationPrepared,
    ) -> Result<MutationStageOutcome, String> {
        Err("mutation staging requires the Frontend native fragment encoder".to_string())
    }

    fn stage_mutation_with_native_encoder(
        &self,
        prepared: &dyn MutationPrepared,
        encoder: &dyn MutationNativeFragmentEncoder,
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
                match crate::query_execution::dml::mutation_flow::stage_prepared_update_mutation(self, kernel, encoder)? {
                    crate::query_execution::dml::mutation_flow::MutationStagedWrite::NoOp => {
                        prepared.state.store(TERMINAL, Ordering::Release);
                        Ok(MutationStageOutcome::NoOp)
                    }
                    crate::query_execution::dml::mutation_flow::MutationStagedWrite::AbortRequired {
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
                    crate::query_execution::dml::mutation_flow::MutationStagedWrite::CommitRequired {
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
                match crate::query_execution::dml::mutation_flow::stage_prepared_merge_mutation(self, kernel, encoder)? {
                    crate::query_execution::dml::mutation_flow::MutationStagedWrite::NoOp => {
                        prepared.state.store(TERMINAL, Ordering::Release);
                        Ok(MutationStageOutcome::NoOp)
                    }
                    crate::query_execution::dml::mutation_flow::MutationStagedWrite::AbortRequired {
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
                    crate::query_execution::dml::mutation_flow::MutationStagedWrite::CommitRequired {
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

    fn abort_mutation_terminal(
        &self,
        prepared: &dyn MutationPrepared,
        abort: &dyn MutationAbort,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        let prepared = prepared_handle(prepared)?;
        let abort = abort_handle(abort)?;
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
        let commit = commit_handle(commit)?;
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
    use crate::query_execution::dml::mutation_flow::MutationExecution;
    use crate::query_execution::outcome::QueryExecutionResult;
    use novarocks_spi::connector::{ConnectorWriteAbortOutcome, ExternalMutationFinalization};

    fn test_dml_kernel() -> crate::query_execution::kernels::DmlExecutionKernel {
        let connector_control: Arc<dyn novarocks_spi::connector::ConnectorControlRegistry> =
            Arc::new(crate::query_execution::compiler::TestConnectorControlRegistry::default());
        crate::query_execution::kernels::DmlExecutionKernel::new(
            Arc::new(novarocks::catalog_application::query_catalog::new_query_catalog_service()),
            None,
            Arc::clone(&connector_control),
            Arc::new(
                novarocks::connector::unified_statistics::UnifiedStatisticsResolver::default(),
            ),
            Arc::new(novarocks::mv::storage_observation::UnavailableMvStorageObservationPort),
            crate::query_execution::compiler::test_query_execution_service(),
        )
    }

    struct TestExecution {
        abort_calls: AtomicUsize,
    }

    impl TestExecution {
        fn known_uncommitted() -> Self {
            Self {
                abort_calls: AtomicUsize::new(0),
            }
        }
    }

    impl MutationExecution for TestExecution {
        fn stage(&self) -> Result<QueryExecutionResult, String> {
            Err("test execution must not stage".to_string())
        }

        fn abort_terminal(&self) -> Result<ConnectorWriteAbortOutcome, String> {
            self.abort_calls.fetch_add(1, Ordering::AcqRel);
            Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
            })
        }

        fn terminal_context(&self) -> novarocks_spi::connector::ConnectorRequestContext {
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
    fn terminal_abort_rejects_foreign_and_cross_statement_handles_without_consuming_prepared_state()
    {
        let engine = test_dml_kernel();
        let prepared = prepared(MutationStatementKind::Update, "update-attempt");
        let execution = Arc::new(TestExecution::known_uncommitted());

        let foreign = ForeignAbort;
        let error = engine
            .abort_mutation_terminal(&prepared, &foreign)
            .unwrap_err();
        assert!(error.contains("foreign abort handle"));
        assert_eq!(prepared.state.load(Ordering::Acquire), STAGED);

        let merge_abort = abort_handle(MutationStatementKind::Merge, "update-attempt", execution);
        let error = engine
            .abort_mutation_terminal(&prepared, &merge_abort)
            .unwrap_err();
        assert!(error.contains("does not belong"));
        assert_eq!(prepared.state.load(Ordering::Acquire), STAGED);
    }

    #[test]
    fn terminal_abort_keeps_its_execution_for_finalization() {
        let engine = test_dml_kernel();
        let prepared = prepared(MutationStatementKind::Update, "update-attempt");
        let execution = Arc::new(TestExecution::known_uncommitted());
        let abort = abort_handle(
            MutationStatementKind::Update,
            "update-attempt",
            Arc::clone(&execution),
        );

        let outcome = engine.abort_mutation_terminal(&prepared, &abort).unwrap();
        assert_eq!(
            outcome,
            ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
            }
        );
        assert_eq!(execution.abort_calls.load(Ordering::Acquire), 1);
        assert_eq!(prepared.state.load(Ordering::Acquire), super::TERMINAL);
        engine.finalize_mutation(&prepared).unwrap();

        let error = engine
            .abort_mutation_terminal(&prepared, &abort)
            .unwrap_err();
        assert!(error.contains("decided more than once"));
        assert_eq!(execution.abort_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn terminal_abort_consumes_the_exact_handle_once_and_returns_connector_truth() {
        let engine = test_dml_kernel();
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
