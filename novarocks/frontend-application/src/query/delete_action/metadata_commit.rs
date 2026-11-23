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

//! Execute the accepted metadata deletion in a governed Connector job.

use std::sync::{Arc, Mutex};

use novarocks_failure_contract::SharedFailureCause;
use novarocks_query_application::session_control::StatementCommandCompletion;
use novarocks_query_application::session_error::QueryServiceError;
use novarocks_spi::connector::{
    ConnectorDeleteCondition, ConnectorError, ConnectorMutationOperationId,
    ConnectorOperationControl, ConnectorProcessLocalPayload, ConnectorRequestContext,
    ExternalMutationFinalization, LakePublicationDisposition,
};

use crate::connector::data_mutation::{
    DataMutationDispatchState, DataMutationIntent, DataMutationSession,
    KnownUncommittedDataMutation, ResolvedDataMutation,
};
use crate::connector::write_target::ConnectorWriteTargetBinding;
use crate::dml::attempt::{
    DmlPublicationAdjudication, DmlPublicationAdjudicationOutcome, DmlPublicationAttempt,
    DmlPublicationFinalization,
};
use crate::query_execution::kernels::DmlExecutionKernel;
use crate::task_execution::blocking_io::ConnectorSourceCloseResult;

pub(crate) struct DeleteCommitInput {
    pub(crate) kernel: DmlExecutionKernel,
    pub(crate) target: ConnectorWriteTargetBinding,
    pub(crate) context: ConnectorRequestContext,
    pub(crate) condition: ConnectorDeleteCondition,
    pub(crate) payload: Option<ConnectorProcessLocalPayload>,
    pub(crate) operation: ConnectorMutationOperationId,
    pub(crate) attempt: Arc<Mutex<DmlPublicationAttempt>>,
}

impl DeleteCommitInput {
    pub(crate) fn run(
        original: &mut Self,
        _: &ConnectorSourceCloseResult<'_>,
    ) -> Option<Result<StatementCommandCompletion, QueryServiceError>> {
        Some(original.execute())
    }

    fn execute(&mut self) -> Result<StatementCommandCompletion, QueryServiceError> {
        let resolved = self
            .context
            .check_active()
            .and_then(|()| {
                self.kernel
                    .connector_control()
                    .acquire_exact_data_mutation(self.target.planning_pin().control_runtime_id())
            })
            .map_err(|error| ResolvedDataMutation::KnownUncommitted {
                failure: KnownUncommittedDataMutation::Planning(error),
                cleanup: ExternalMutationFinalization::Complete,
            })
            .and_then(|lease| {
                DataMutationSession::plan_on_session(
                    lease,
                    self.operation,
                    self.target.identity().clone(),
                    DataMutationIntent::DeleteByCondition {
                        original_table: self.target.handle().clone(),
                        condition: self.condition,
                        process_local: self.payload.take(),
                    },
                    self.context.clone(),
                )
            });
        let mut session = match resolved {
            Ok(session) => session,
            Err(outcome) => return self.finish(outcome, None),
        };
        if let Err(error) = self.context.check_active() {
            return self.finish(
                ResolvedDataMutation::KnownUncommitted {
                    failure: KnownUncommittedDataMutation::Planning(error),
                    cleanup: ExternalMutationFinalization::Complete,
                },
                None,
            );
        }
        self.attempt
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .mark_dispatch_possible()
            .map_err(|error| crate::query::internal_error(error.to_string()))?;
        let result = session.execute_once(&self.kernel);
        match result {
            ResolvedDataMutation::CommitUnknown { evidence, .. } => {
                let adjudication = self
                    .attempt
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .begin_adjudication()
                    .map_err(|error| crate::query::internal_error(error.to_string()))?;
                self.finish(
                    session.adjudicate_once(evidence, &self.kernel),
                    Some(adjudication),
                )
            }
            other => self.finish(other, None),
        }
    }

    fn finish(
        &self,
        outcome: ResolvedDataMutation,
        adjudication: Option<DmlPublicationAdjudication>,
    ) -> Result<StatementCommandCompletion, QueryServiceError> {
        let (disposition, finalization, result) = match outcome {
            ResolvedDataMutation::KnownCommitted(completed) => {
                let (finalization, result) = match completed.finalization {
                    ExternalMutationFinalization::Complete => (
                        DmlPublicationFinalization::Succeeded,
                        Ok(completed.receipt.deleted_rows().map_or_else(
                            StatementCommandCompletion::no_count,
                            StatementCommandCompletion::new,
                        )),
                    ),
                    ExternalMutationFinalization::Failed(error) => (
                        DmlPublicationFinalization::Failed,
                        Err(QueryServiceError::from_failure_cause(
                            SharedFailureCause::new(error),
                        )),
                    ),
                };
                (
                    LakePublicationDisposition::KnownCommitted,
                    finalization,
                    result,
                )
            }
            ResolvedDataMutation::KnownUncommitted { failure, .. } => (
                LakePublicationDisposition::KnownUncommitted,
                DmlPublicationFinalization::NotApplicable,
                Err(match failure {
                    KnownUncommittedDataMutation::Provider(error) => {
                        QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
                    }
                    KnownUncommittedDataMutation::Planning(error) => connector_error(error),
                }),
            ),
            ResolvedDataMutation::CommitUnknown { failure, .. } => (
                LakePublicationDisposition::CommitUnknown,
                DmlPublicationFinalization::NotApplicable,
                Err(QueryServiceError::from_failure_cause(
                    SharedFailureCause::new(failure),
                )),
            ),
            ResolvedDataMutation::ContractFailure { error, dispatch } => (
                match dispatch {
                    DataMutationDispatchState::ConfirmedNotDispatched => {
                        LakePublicationDisposition::KnownUncommitted
                    }
                    DataMutationDispatchState::PossiblyDispatched => {
                        LakePublicationDisposition::CommitUnknown
                    }
                },
                DmlPublicationFinalization::NotApplicable,
                Err(connector_error(error)),
            ),
        };
        let mut attempt = self
            .attempt
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let terminal = match adjudication {
            Some(original) => attempt.finish_adjudication(
                original,
                match disposition {
                    LakePublicationDisposition::KnownCommitted => {
                        DmlPublicationAdjudicationOutcome::KnownCommitted
                    }
                    LakePublicationDisposition::KnownUncommitted => {
                        DmlPublicationAdjudicationOutcome::KnownUncommitted
                    }
                    LakePublicationDisposition::CommitUnknown => {
                        DmlPublicationAdjudicationOutcome::CommitUnknown
                    }
                },
                finalization,
            ),
            None => match disposition {
                LakePublicationDisposition::KnownCommitted => {
                    attempt.terminal_known_committed(finalization)
                }
                LakePublicationDisposition::KnownUncommitted => {
                    attempt.terminal_known_uncommitted()
                }
                LakePublicationDisposition::CommitUnknown => attempt.terminal_commit_unknown(),
            },
        }
        .map_err(|error| crate::query::internal_error(error.to_string()))?;
        result.map_err(|error| {
            error.attach_external_effect_disposition(crate::query::external_effect_from_lake(
                terminal.disposition(),
            ))
        })
    }
}

fn connector_error(error: ConnectorError) -> QueryServiceError {
    QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
}
