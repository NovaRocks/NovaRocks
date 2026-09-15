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

use std::sync::Arc;

use crate::query_execution::dml::mutation::{
    MutationAbort, MutationCommit, MutationEngine, MutationNativeFragmentEncoder,
    MutationStageOutcome, MutationStatementKind, PrepareMutationRequest, PreparedMutation,
};
use novarocks_parser::ast::{DmlStatement, MergeClause, MutationSource};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::RequestContext;

use crate::dml::error::DmlError;
use crate::dml::runner::{
    CoordinatedWriteReport, StatementWriteTransactionRunner, WriteExecutor, WriteTarget,
    WriteTransactionSpec,
};
use crate::dml::service::DmlService;
use novarocks_query_application::sql::dml_admission::DmlAdmissionError;
use novarocks_spi::connector::{LakePublicationFamily, LakePublicationId};

/// A prepared UPDATE or MERGE whose native staging has not begun.
///
/// The opaque handle remains exact to the admitted statement. Dropping it
/// before the dispatch edge cannot create a write or publication attempt.
pub(crate) struct PreparedMutationAttempt {
    prepared: PreparedMutation,
    subkind: &'static str,
}

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
        // A merge-on-read change stream is a write-dataflow plan, so its
        // writer nodes need the recipes the sealed input carries. The
        // entrypoint reads that off the input rather than being chosen here.
        crate::native::fragment_encoder::encode_native_fragment_bundle_for_input(input)
    }
}

impl WriteExecutor for MutationWriteExecutor<'_> {
    type CommitHandle = Arc<dyn MutationCommit>;
    type AbortHandle = Arc<dyn MutationAbort>;

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

    fn adjudicate_publication(
        &self,
        _spec: &WriteTransactionSpec,
        handle: &Self::CommitHandle,
        evidence: novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        self.engine.adjudicate_mutation_publication(
            self.prepared.handle.as_ref(),
            handle.as_ref(),
            evidence,
        )
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        self.engine.finalize_mutation(self.prepared.handle.as_ref())
    }
}

fn write_transaction_spec(prepared: &PreparedMutation, _subkind: &str) -> WriteTransactionSpec {
    let operation = &prepared.operation;
    WriteTransactionSpec {
        publication_id: operation.publication_id,
        target: WriteTarget {
            catalog: operation.catalog.clone(),
            namespace: operation.namespace.clone(),
            table: operation.table.clone(),
            reference: (operation.target_ref != "main").then(|| operation.target_ref.clone()),
        },
    }
}

impl DmlService {
    /// Prepares an UPDATE or MERGE already classified by SQLP-5's typed AST.
    /// The statement family comes from the variant, never from `source` text.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub(crate) fn prepare_typed_mutation(
        &self,
        engine: &dyn MutationEngine,
        statement: &DmlStatement,
        source: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<PreparedMutationAttempt, DmlError> {
        let (_kind, subkind) = admit_mutation(statement, source)?;
        let publication_id = LakePublicationId::new_v7();

        let session = context.session();
        let prepared = engine
            .prepare_mutation(PrepareMutationRequest {
                publication_id,
                statement,
                source,
                current_catalog: session.current_catalog().map(ToOwned::to_owned),
                current_database: session.current_database().to_string(),
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
            })
            .map_err(DmlError::executor)?;
        Ok(PreparedMutationAttempt { prepared, subkind })
    }

    /// Crosses the statement's one-shot native staging and publication edge.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub(crate) fn execute_prepared_mutation(
        &self,
        engine: &dyn MutationEngine,
        prepared: PreparedMutationAttempt,
    ) -> Result<(), DmlError> {
        let _ = self;
        // Preparation is inert; the statement-local attempt owns the later
        // staging and publication boundary without durable DML admission.
        let executor = MutationWriteExecutor {
            engine,
            prepared: &prepared.prepared,
        };
        let spec = write_transaction_spec(&prepared.prepared, prepared.subkind);
        StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::DataMutation)
            .run(spec)?;
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
                    DmlAdmissionError::UpdateUnsupportedForm.to_user_error(
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
                    DmlAdmissionError::MergeUnsupportedForm.to_user_error(
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
                                DmlAdmissionError::MergeUnsupportedForm.to_user_error(
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
                                DmlAdmissionError::MergeUnsupportedForm.to_user_error(
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
                            DmlAdmissionError::MergeUnsupportedForm.to_user_error(
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
                    DmlAdmissionError::MergeUnsupportedForm.to_user_error(
                        source,
                        statement.span,
                        "MERGE requires at least one WHEN clause",
                    ),
                ));
            }
            Ok((MutationStatementKind::Merge, "MERGE"))
        }
        other => Err(DmlError::admit(
            DmlAdmissionError::UpdateUnsupportedForm.to_user_error(
                source,
                other.span(),
                "typed mutation entry requires UPDATE or MERGE",
            ),
        )),
    }
}
