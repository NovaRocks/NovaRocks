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

//! Frontend-owned UPDATE and MERGE application use cases.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::common::admitted_query_context::RequestContext;
use crate::query_execution::dml::mutation::{
    MutationAbort, MutationCommit, MutationEngine, MutationNativeFragmentEncoder,
    MutationStageOutcome, MutationStatementKind, PrepareMutationRequest, PreparedMutation,
};
use novarocks_parser::ast::{DmlStatement, MergeClause, MutationSource};
use novarocks_proto::lifecycle::QueryOptions;

use crate::dml::coordination::DmlExternalFenceProposal;
use crate::dml::error::{AdmitError, DmlError};
use crate::dml::model::{OperationKind, OperationTarget, WriteTransactionSpec};
use crate::dml::runner::{
    ActiveWriteTransactionRunner, CoordinatedWriteReport, WriteExecutor, preparing_request,
};
use crate::dml::service::DmlService;

struct MutationWriteExecutor<'a> {
    engine: &'a dyn MutationEngine,
    prepared: &'a PreparedMutation,
}

/// The Frontend application is the native FE-to-BE encoder caller for durable
/// row-mutation staging. Core supplies only the exact sealed plan/preparation
/// input and receives the resulting bundle for neutral request construction.
struct FrontendMutationNativeFragmentEncoder;

impl MutationNativeFragmentEncoder for FrontendMutationNativeFragmentEncoder {
    fn encode(
        &self,
        input: &crate::query_execution::compiler::NativeFragmentEncodingInput,
    ) -> Result<crate::query_execution::native_fragment::NativeFragmentAttachment, String> {
        crate::native::fragment_encoder::encode_native_fragment_bundle(input.encoding_view())
    }
}

impl WriteExecutor for MutationWriteExecutor<'_> {
    type CommitHandle = Arc<dyn MutationCommit>;
    type AbortHandle = Arc<dyn MutationAbort>;

    /// UPDATE and MERGE both fence through the exact write authority the
    /// mutation preparation retained, and the same fence must cover the
    /// terminal abort of an already activated authority.
    ///
    /// The reverse port does not expose that authority yet, so this route fails
    /// closed: no writer and no commit may run without a fence the provider can
    /// compare at its external linearization point.
    /// UPDATE and MERGE derive their write lease at preparation precisely so the
    /// fence can be established here, before staging dispatches anything.
    ///
    /// `derive_write_lease` mints a fresh fence cell on every call, so a lease
    /// derived later inside staging would carry no fence at all — which is why
    /// the derivation was hoisted rather than the fence pushed later.
    fn establish_external_fence(
        &self,
        _spec: &WriteTransactionSpec,
        proposal: &DmlExternalFenceProposal,
    ) -> Result<
        novarocks_spi::connector::ConnectorEstablishedWriteFence,
        novarocks_spi::connector::ConnectorError,
    > {
        self.engine.establish_mutation_external_fence(
            self.prepared.handle.as_ref(),
            &|operation_id, table, target_ref| proposal.seal(operation_id, table, target_ref),
        )
    }

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<Self::CommitHandle, Self::AbortHandle>, DmlError> {
        let native_encoder = FrontendMutationNativeFragmentEncoder;
        match self
            .engine
            .stage_mutation_with_native_encoder(self.prepared.handle.as_ref(), &native_encoder)
            .map_err(|error| error.into_dml_error(Some(&self.prepared.sql_source)))?
        {
            MutationStageOutcome::NoOp => Ok(CoordinatedWriteReport::NoOp),
            MutationStageOutcome::AbortRequired { reason, handle } => {
                Ok(CoordinatedWriteReport::AbortRequired { reason, handle })
            }
            MutationStageOutcome::CommitRequired(handle) => {
                Ok(CoordinatedWriteReport::CommitRequired(handle))
            }
        }
    }

    fn abort(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::AbortHandle,
    ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
        self.engine
            .abort_mutation_terminal(self.prepared.handle.as_ref(), handle.as_ref())
    }

    fn commit(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::CommitHandle,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        self.engine
            .commit_mutation_terminal(self.prepared.handle.as_ref(), handle.as_ref())
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        self.engine.finalize_mutation(self.prepared.handle.as_ref())
    }
}

fn write_transaction_spec(prepared: &PreparedMutation, subkind: &str) -> WriteTransactionSpec {
    let operation = &prepared.operation;
    WriteTransactionSpec {
        target: OperationTarget {
            catalog: operation.catalog.clone(),
            namespace: operation.namespace.clone(),
            table: operation.table.clone(),
            ref_name: (operation.target_ref != "main").then(|| operation.target_ref.clone()),
        },
        operation_kind: OperationKind::RowDelta,
        operation_subkind: Some(subkind.to_string()),
        attempt_id: operation.attempt_id.clone(),
        base_snapshot_id: operation.base_snapshot_id,
        base_snapshot_map: BTreeMap::new(),
    }
}

impl DmlService {
    /// Executes an UPDATE or MERGE already classified by SQLP-5's typed AST.
    /// The statement family comes from the variant, never from `source` text.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub fn try_execute_typed_mutation(
        &self,
        engine: &dyn MutationEngine,
        statement: &DmlStatement,
        source: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<(), DmlError> {
        let (_kind, subkind) = admit_mutation(statement, source)?;

        let session = context.session();
        let prepared = engine
            .prepare_mutation(PrepareMutationRequest {
                statement,
                source,
                current_catalog: session.current_catalog().map(ToOwned::to_owned),
                current_database: session.current_database().to_string(),
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
            })
            .map_err(DmlError::executor)?;
        // Preparation is deliberately inert. The admitted and claimed intent
        // below precedes matching, cohort registration and all staging side
        // effects.
        self.require_journal()?;
        let executor = MutationWriteExecutor {
            engine,
            prepared: &prepared,
        };
        let spec = write_transaction_spec(&prepared, subkind);
        let operation = self.begin_write_operation(preparing_request(&spec))?;
        ActiveWriteTransactionRunner::new(operation, &executor).run(spec)?;
        Ok(())
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn admit_mutation(
    statement: &DmlStatement,
    source: &str,
) -> Result<(MutationStatementKind, &'static str), DmlError> {
    match statement {
        DmlStatement::Update(statement) => {
            if statement
                .alias
                .as_ref()
                .is_some_and(|alias| !alias.columns.is_empty())
                || statement
                    .assignments
                    .iter()
                    .any(|assignment| assignment.target.parts.len() != 1)
                || matches!(
                    &statement.source,
                    Some(MutationSource::Query { lateral: true, .. })
                )
                || matches!(
                    &statement.source,
                    Some(MutationSource::Query { alias: None, .. })
                )
            {
                return Err(DmlError::admit(
                    AdmitError::UpdateUnsupportedForm.to_user_error(
                        source,
                        statement.span,
                        "UPDATE form is not supported by the current frontend capability",
                    ),
                ));
            }
            Ok((MutationStatementKind::Update, "UPDATE"))
        }
        DmlStatement::Merge(statement) => {
            if statement
                .target_alias
                .as_ref()
                .is_some_and(|alias| !alias.columns.is_empty())
                || matches!(
                    &statement.source,
                    MutationSource::Query { lateral: true, .. }
                        | MutationSource::Query { alias: None, .. }
                )
            {
                return Err(DmlError::admit(
                    AdmitError::MergeUnsupportedForm.to_user_error(
                        source,
                        statement.span,
                        "MERGE form is not supported by the current frontend capability",
                    ),
                ));
            }
            let mut matched = false;
            let mut not_matched = false;
            for clause in &statement.clauses {
                match clause {
                    MergeClause::Matched { action, span, .. } => {
                        let qualified_assignment = matches!(
                            action,
                            novarocks_parser::ast::MergeMatchedAction::Update {
                                assignments,
                                ..
                            } if assignments.iter().any(|assignment| assignment.target.parts.len() != 1)
                        );
                        if matched || qualified_assignment {
                            return Err(DmlError::admit(
                                AdmitError::MergeUnsupportedForm.to_user_error(
                                    source,
                                    *span,
                                    "MERGE WHEN MATCHED form is not supported",
                                ),
                            ));
                        }
                        matched = true;
                    }
                    MergeClause::NotMatched { action, span, .. } => {
                        if not_matched
                            || (!action.columns.is_empty()
                                && action.columns.len() != action.values.len())
                        {
                            return Err(DmlError::admit(
                                AdmitError::MergeUnsupportedForm.to_user_error(
                                    source,
                                    *span,
                                    "MERGE WHEN NOT MATCHED form is not supported",
                                ),
                            ));
                        }
                        not_matched = true;
                    }
                    MergeClause::NotMatchedBySource { span, .. } => {
                        return Err(DmlError::admit(
                            AdmitError::MergeUnsupportedForm.to_user_error(
                                source,
                                *span,
                                "MERGE WHEN NOT MATCHED BY SOURCE is not supported",
                            ),
                        ));
                    }
                }
            }
            if !matched && !not_matched {
                return Err(DmlError::admit(
                    AdmitError::MergeUnsupportedForm.to_user_error(
                        source,
                        statement.span,
                        "MERGE requires at least one WHEN clause",
                    ),
                ));
            }
            Ok((MutationStatementKind::Merge, "MERGE"))
        }
        other => Err(DmlError::admit(
            AdmitError::UpdateUnsupportedForm.to_user_error(
                source,
                other.span(),
                "typed mutation entry requires UPDATE or MERGE",
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use crate::common::admitted_query_context::{
        RequestAdmission, RequestContext, SessionOptimizerSettings,
    };
    use crate::common::backend_topology::BackendTopologySnapshot;
    use crate::common::query_cancellation::QueryCancellationSource;
    use crate::query_execution::dml::mutation::{
        MutationEngine, MutationPrepared, MutationStageOutcome, PrepareMutationRequest,
        PreparedMutation,
    };
    use novarocks_types::ClusterRole;

    use super::*;
    use crate::dml::OperationJournal;
    use crate::dml::journal::testing::InMemoryOperationJournal;
    use crate::dml::model::OperationState;

    struct TestPrepared;

    impl MutationPrepared for TestPrepared {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct RecordingMutationEngine {
        journal: Arc<InMemoryOperationJournal>,
        events: Mutex<Vec<&'static str>>,
    }

    impl MutationEngine for RecordingMutationEngine {
        fn prepare_mutation(
            &self,
            request: PrepareMutationRequest<'_>,
        ) -> Result<PreparedMutation, String> {
            self.events.lock().expect("events").push("prepare");
            Ok(PreparedMutation {
                operation: crate::query_execution::dml::mutation::MutationOperation {
                    kind: match request.statement {
                        DmlStatement::Update(_) => MutationStatementKind::Update,
                        DmlStatement::Merge(_) => MutationStatementKind::Merge,
                        _ => panic!("test engine received a non-mutation statement"),
                    },
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: "t".to_string(),
                    target_ref: "main".to_string(),
                    attempt_id: "mutation-test".to_string(),
                    base_snapshot_id: Some(7),
                },
                handle: Arc::new(TestPrepared),
                sql_source: request.source.to_string(),
            })
        }

        fn stage_mutation(
            &self,
            _prepared: &dyn MutationPrepared,
        ) -> Result<MutationStageOutcome, crate::dml::error::DmlExecutionError> {
            let operations = self.journal.list_operations().unwrap();
            assert_eq!(operations.len(), 1);
            assert_eq!(operations[0].state, OperationState::Writing);
            self.events.lock().expect("events").push("stage");
            Ok(MutationStageOutcome::NoOp)
        }

        fn finalize_mutation(&self, _prepared: &dyn MutationPrepared) -> Result<(), String> {
            unreachable!("no-op mutation must not finalize")
        }
    }

    fn context() -> RequestContext {
        let cancellation = QueryCancellationSource::new();
        RequestContext::admit(RequestAdmission::new(
            Some("ice".to_string()),
            "db".to_string(),
            ClusterRole::AllInOne,
            BackendTopologySnapshot::empty(11),
            Some(Instant::now() + Duration::from_secs(30)),
            cancellation.view(),
            SessionOptimizerSettings::default(),
        ))
    }

    fn typed_mutation(source: &str) -> DmlStatement {
        let parsed = novarocks_parser::parse(source).expect("parse mutation test input");
        let [novarocks_parser::ast::Statement::Dml(statement)] = parsed.as_slice() else {
            panic!("expected mutation statement: {source}");
        };
        statement.clone()
    }

    /// The durable intent is still published before anything reaches the
    /// mutation engine, and the statement subkind is still recorded. What
    /// changed with CP-3B is that staging no longer follows: a route whose
    /// write authority cannot establish an external operation fence must not
    /// dispatch a writer at all.
    #[test]
    fn update_intent_is_durable_and_no_stage_runs_without_an_external_fence() {
        let journal = Arc::new(InMemoryOperationJournal::default());
        let service = DmlService::new(journal.clone());
        let engine = RecordingMutationEngine {
            journal: journal.clone(),
            events: Mutex::new(Vec::new()),
        };

        let error = service
            .try_execute_typed_mutation(
                &engine,
                &typed_mutation("UPDATE t SET k = 1"),
                "UPDATE t SET k = 1",
                &context(),
                None,
            )
            .expect_err("an unfenced UPDATE must not stage");

        assert_eq!(*engine.events.lock().unwrap(), ["prepare"]);
        let record = journal.list_operations().unwrap().pop().unwrap();
        assert_eq!(record.operation_subkind.as_deref(), Some("UPDATE"));
        assert_eq!(record.state, OperationState::Writing);
        assert_eq!(error.operation_id(), Some(record.operation_id));
    }

    #[test]
    fn merge_intent_is_durable_and_no_stage_runs_without_an_external_fence() {
        let journal = Arc::new(InMemoryOperationJournal::default());
        let service = DmlService::new(journal.clone());
        let engine = RecordingMutationEngine {
            journal: journal.clone(),
            events: Mutex::new(Vec::new()),
        };

        let error = service
            .try_execute_typed_mutation(
                &engine,
                &typed_mutation(
                    "MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN UPDATE SET k = s.k",
                ),
                "MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN UPDATE SET k = s.k",
                &context(),
                None,
            )
            .expect_err("an unfenced MERGE must not stage");

        assert_eq!(*engine.events.lock().unwrap(), ["prepare"]);
        let record = journal.list_operations().unwrap().pop().unwrap();
        assert_eq!(record.operation_subkind.as_deref(), Some("MERGE"));
        assert_eq!(record.state, OperationState::Writing);
        assert_eq!(error.operation_id(), Some(record.operation_id));
    }
}
