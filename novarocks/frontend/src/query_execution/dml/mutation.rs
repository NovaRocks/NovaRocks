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
use novarocks_parser::Span;
use novarocks_parser::ast::{
    DmlStatement, MergeClause, MergeMatchedAction, MutationSource, ObjectName as ParsedObjectName,
};
use novarocks_proto::lifecycle::QueryOptions;
use novarocks_sql::semantic::ObjectName;

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

/// One admitted frontend mutation request. The typed statement establishes the
/// command family; `source` may only be read through spans carried by it.
pub struct PrepareMutationRequest<'a> {
    pub statement: &'a DmlStatement,
    pub source: &'a str,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
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
    pub sql_source: String,
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
    ) -> Result<MutationStageOutcome, crate::dml::error::DmlExecutionError>;

    /// Stage through the Frontend-owned native encoding boundary. The default
    /// preserves narrow test doubles that never materialize a Core plan.
    fn stage_mutation_with_native_encoder(
        &self,
        prepared: &dyn MutationPrepared,
        _encoder: &dyn MutationNativeFragmentEncoder,
    ) -> Result<MutationStageOutcome, crate::dml::error::DmlExecutionError> {
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
    #[allow(
        dead_code,
        reason = "Preserves the typed abort payload for the staged core mutation adapter."
    )]
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

fn source_slice<'a>(source: &'a str, span: Span, context: &str) -> Result<&'a str, String> {
    source
        .get(span.start()..span.end())
        .ok_or_else(|| format!("{context} span is outside the admitted SQL source"))
}

fn lower_object_name(name: &ParsedObjectName) -> ObjectName {
    ObjectName {
        parts: name.parts.iter().map(|part| part.value.clone()).collect(),
    }
}

fn lower_alias(
    alias: &Option<novarocks_parser::ast::TableAlias>,
) -> Result<Option<String>, String> {
    alias
        .as_ref()
        .map(|alias| {
            if alias.columns.is_empty() {
                Ok(alias.name.value.clone())
            } else {
                Err("mutation aliases with column lists are not supported".to_string())
            }
        })
        .transpose()
}

fn lower_mutation_source(
    source: &MutationSource,
    sql: &str,
) -> Result<crate::query_execution::dml::mutation_flow::PreparedMutationSource, String> {
    use crate::query_execution::dml::mutation_flow::PreparedMutationSource;

    match source {
        MutationSource::Table { name, alias, .. } => Ok(PreparedMutationSource::Table {
            name: lower_object_name(name),
            alias: lower_alias(alias)?,
        }),
        MutationSource::Query {
            lateral,
            query,
            alias,
            ..
        } => {
            if *lateral {
                return Err("lateral mutation sources are not supported".to_string());
            }
            Ok(PreparedMutationSource::Query {
                query_text: source_slice(sql, query.span, "derived mutation query")?.to_string(),
                alias: lower_alias(alias)?,
            })
        }
    }
}

fn lower_update_statement(
    statement: &novarocks_parser::ast::Update,
    source: &str,
) -> Result<crate::query_execution::dml::mutation_flow::PreparedUpdateStatement, String> {
    use crate::query_execution::dml::mutation_flow::{
        PreparedMutationAssignment, PreparedUpdateStatement,
    };

    let assignments = statement
        .assignments
        .iter()
        .map(|assignment| {
            if assignment.target.parts.len() != 1 {
                return Err("only single-column UPDATE assignments are supported".to_string());
            }
            Ok(PreparedMutationAssignment {
                column: assignment.target.parts[0].value.clone(),
                value_sql: source_slice(source, assignment.value.span(), "UPDATE assignment")?
                    .to_string(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if assignments.is_empty() {
        return Err("UPDATE requires at least one assignment".to_string());
    }
    Ok(PreparedUpdateStatement {
        table: lower_object_name(&statement.target),
        alias: lower_alias(&statement.alias)?,
        assignments,
        source: statement
            .source
            .as_ref()
            .map(|mutation_source| lower_mutation_source(mutation_source, source))
            .transpose()?,
        where_sql: statement
            .selection
            .as_ref()
            .map(|predicate| source_slice(source, predicate.span(), "UPDATE WHERE"))
            .transpose()?
            .map(str::to_string),
    })
}

fn lower_merge_statement(
    statement: &novarocks_parser::ast::Merge,
    source: &str,
) -> Result<crate::query_execution::dml::mutation_flow::PreparedMergeStatement, String> {
    use crate::query_execution::dml::mutation_flow::{
        PreparedMergeClause, PreparedMergeMatchedAction, PreparedMergeNotMatchedAction,
        PreparedMergeStatement, PreparedMutationAssignment,
    };

    let mut matched = None;
    let mut not_matched = None;
    for clause in &statement.clauses {
        match clause {
            MergeClause::Matched {
                predicate, action, ..
            } => {
                if matched.is_some() {
                    return Err("MERGE supports at most one WHEN MATCHED clause".to_string());
                }
                let action = match action {
                    MergeMatchedAction::Update { assignments, .. } => {
                        if assignments.is_empty() {
                            return Err(
                                "MERGE WHEN MATCHED UPDATE requires at least one assignment"
                                    .to_string(),
                            );
                        }
                        PreparedMergeMatchedAction::Update {
                            assignments: assignments
                                .iter()
                                .map(|assignment| {
                                    if assignment.target.parts.len() != 1 {
                                        return Err(
                                            "only single-column MERGE UPDATE assignments are supported"
                                                .to_string(),
                                        );
                                    }
                                    Ok(PreparedMutationAssignment {
                                        column: assignment.target.parts[0].value.clone(),
                                        value_sql: source_slice(
                                            source,
                                            assignment.value.span(),
                                            "MERGE assignment",
                                        )?
                                        .to_string(),
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                        }
                    }
                    MergeMatchedAction::Delete { .. } => PreparedMergeMatchedAction::Delete,
                };
                matched = Some(PreparedMergeClause {
                    predicate_sql: predicate
                        .as_ref()
                        .map(|predicate| source_slice(source, predicate.span(), "MERGE predicate"))
                        .transpose()?
                        .map(str::to_string),
                    action,
                });
            }
            MergeClause::NotMatched {
                predicate, action, ..
            } => {
                if not_matched.is_some() {
                    return Err("MERGE supports at most one WHEN NOT MATCHED clause".to_string());
                }
                if !action.columns.is_empty() && action.columns.len() != action.values.len() {
                    return Err(format!(
                        "MERGE INSERT column count {} does not match VALUES count {}",
                        action.columns.len(),
                        action.values.len()
                    ));
                }
                not_matched = Some(PreparedMergeClause {
                    predicate_sql: predicate
                        .as_ref()
                        .map(|predicate| source_slice(source, predicate.span(), "MERGE predicate"))
                        .transpose()?
                        .map(str::to_string),
                    action: PreparedMergeNotMatchedAction {
                        columns: action
                            .columns
                            .iter()
                            .map(|column| column.value.clone())
                            .collect(),
                        values_sql: action
                            .values
                            .iter()
                            .map(|value| source_slice(source, value.span(), "MERGE INSERT value"))
                            .collect::<Result<Vec<_>, _>>()?
                            .into_iter()
                            .map(str::to_string)
                            .collect(),
                    },
                });
            }
            MergeClause::NotMatchedBySource { .. } => {
                return Err("MERGE WHEN NOT MATCHED BY SOURCE is not supported".to_string());
            }
        }
    }
    if matched.is_none() && not_matched.is_none() {
        return Err("MERGE requires at least one WHEN clause".to_string());
    }
    Ok(PreparedMergeStatement {
        table: lower_object_name(&statement.target),
        target_alias: lower_alias(&statement.target_alias)?,
        source: lower_mutation_source(&statement.source, source)?,
        on_sql: source_slice(source, statement.on.span(), "MERGE ON")?.to_string(),
        matched,
        not_matched,
    })
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
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )?;
        let (kind, kernel, _target, base_snapshot_id) = match request.statement {
            DmlStatement::Update(statement) => {
                let stmt = lower_update_statement(statement, request.source)?;
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
                    MutationStatementKind::Update,
                    PreparedKernel::Update(prepared),
                    stmt.table,
                    base_snapshot_id,
                )
            }
            DmlStatement::Merge(statement) => {
                let stmt = lower_merge_statement(statement, request.source)?;
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
                    MutationStatementKind::Merge,
                    PreparedKernel::Merge(prepared),
                    stmt.table,
                    base_snapshot_id,
                )
            }
            _ => {
                return Err(
                    "mutation engine received a non-UPDATE/MERGE typed statement".to_string(),
                );
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
            kind,
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
        Ok(PreparedMutation {
            operation,
            handle,
            sql_source: request.source.to_string(),
        })
    }

    fn stage_mutation(
        &self,
        _prepared: &dyn MutationPrepared,
    ) -> Result<MutationStageOutcome, crate::dml::error::DmlExecutionError> {
        Err(crate::dml::error::DmlExecutionError::from(
            "mutation staging requires the Frontend native fragment encoder".to_string(),
        ))
    }

    fn stage_mutation_with_native_encoder(
        &self,
        prepared: &dyn MutationPrepared,
        encoder: &dyn MutationNativeFragmentEncoder,
    ) -> Result<MutationStageOutcome, crate::dml::error::DmlExecutionError> {
        let prepared = prepared_handle(prepared)?;
        if prepared
            .state
            .compare_exchange(PREPARED, STAGED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(
                "mutation prepared handle has already reached a terminal stage"
                    .to_string()
                    .into(),
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
        MutationStatementKind, STAGED,
    };
    use crate::query_execution::dml::mutation_flow::MutationExecution;
    use crate::query_execution::outcome::QueryExecutionResult;
    use novarocks_parser::ast::DmlStatement;
    use novarocks_spi::connector::{ConnectorWriteAbortOutcome, ExternalMutationFinalization};

    fn test_dml_kernel() -> crate::query_execution::kernels::DmlExecutionKernel {
        let connector_control: Arc<dyn novarocks_spi::connector::ConnectorControlRegistry> =
            Arc::new(crate::query_execution::compiler::TestConnectorControlRegistry::default());
        crate::query_execution::kernels::DmlExecutionKernel::new(
            Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
            None,
            Arc::clone(&connector_control),
            Arc::new(crate::connector::unified_statistics::UnifiedStatisticsResolver::default()),
            Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
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
    fn typed_mutation_family_is_statement_specific() {
        let update = novarocks_parser::parse("UPDATE t SET k = 1").expect("parse UPDATE");
        let merge =
            novarocks_parser::parse("MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN DELETE")
                .expect("parse MERGE");
        assert!(matches!(
            update.as_slice(),
            [novarocks_parser::ast::Statement::Dml(DmlStatement::Update(
                _
            ))]
        ));
        assert!(matches!(
            merge.as_slice(),
            [novarocks_parser::ast::Statement::Dml(DmlStatement::Merge(
                _
            ))]
        ));
    }

    #[test]
    fn typed_update_keeps_predicates_and_assignments_as_original_source_slices() {
        let source = "UPDATE t SET v = (a + b) * c WHERE (id = 1 OR id = 2)";
        let statements = novarocks_parser::parse(source).expect("parse UPDATE");
        let [novarocks_parser::ast::Statement::Dml(DmlStatement::Update(statement))] =
            statements.as_slice()
        else {
            panic!("expected typed UPDATE");
        };
        let lowered = super::lower_update_statement(statement, source).expect("lower UPDATE");
        assert_eq!(lowered.assignments[0].value_sql, "(a + b) * c");
        assert_eq!(lowered.where_sql.as_deref(), Some("(id = 1 OR id = 2)"));
    }

    #[test]
    fn typed_merge_keeps_derived_query_as_original_query_span() {
        let source = "MERGE INTO t USING (SELECT a + 1 AS id FROM s) src ON t.id = src.id WHEN MATCHED THEN DELETE";
        let statements = novarocks_parser::parse(source).expect("parse MERGE");
        let [novarocks_parser::ast::Statement::Dml(DmlStatement::Merge(statement))] =
            statements.as_slice()
        else {
            panic!("expected typed MERGE");
        };
        let lowered = super::lower_merge_statement(statement, source).expect("lower MERGE");
        match lowered.source {
            crate::query_execution::dml::mutation_flow::PreparedMutationSource::Query {
                query_text,
                alias,
            } => {
                assert_eq!(query_text, "SELECT a + 1 AS id FROM s");
                assert_eq!(alias.as_deref(), Some("src"));
            }
            other => panic!("expected derived source, got {other:?}"),
        }
        assert_eq!(lowered.on_sql, "t.id = src.id");
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
