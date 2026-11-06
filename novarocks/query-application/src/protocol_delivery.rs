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

//! Move-only protocol settlement for governed query application output.

use crate::api::{
    ExecutionOutput, ExecutionOutputObligation, LocalResultProducer, OwnedLocalResult,
    QueryExecutionError, QueryResult, QueryResultStream, ResultDelivery, ResultFailureView,
    SchemaDelivery,
};
use crate::cancellation::QueryCancellationView;
use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason};
use crate::session_control::{
    GovernedQueryStatementOwner, GovernedStatementFinishOutcome,
    GovernedStatementVisibilitySealOutcome, StatementCommandCompletion, StatementToken,
};
use crate::session_error::QueryServiceError;
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_workload_control::WorkError;

/// Shared move-only owner for any governed query result presented to a client.
#[must_use = "the governed protocol owner must be settled by its protocol adapter"]
pub struct GovernedProtocolOwner {
    statement: Option<GovernedQueryStatementOwner>,
    settled: bool,
    external_effect: Option<crate::api::StatementExternalEffectDisposition>,
}

impl GovernedProtocolOwner {
    pub fn new(statement: GovernedQueryStatementOwner) -> Self {
        Self {
            statement: Some(statement),
            settled: false,
            external_effect: None,
        }
    }

    /// The original Action's cumulative effect fact grants no publication,
    /// visibility, control or resource authority.
    pub fn external_effect_disposition(
        &self,
    ) -> Option<crate::api::StatementExternalEffectDisposition> {
        self.external_effect
    }

    /// Project the original live statement identity without retaining its owner.
    pub fn statement_token(&self) -> Option<StatementToken> {
        self.statement
            .as_ref()
            .map(GovernedQueryStatementOwner::token)
    }

    pub fn result_window_alias(
        &self,
    ) -> Result<novarocks_workload_control::ResultWindowAlias, WorkError> {
        self.statement
            .as_ref()
            .expect("original protocol owner")
            .result_window_alias()
            .ok_or(WorkError::Conflict)
    }

    /// Match a read-only observer to this original statement's workload owner.
    /// No scope, grant, or new observer escapes the protocol owner.
    pub fn is_observed_by(
        &self,
        observer: &novarocks_workload_control::WorkloadObservationHandle,
    ) -> bool {
        self.statement
            .as_ref()
            .is_some_and(|statement| observer.observes_scope(statement.scope()))
    }

    pub fn cancellation(&self) -> QueryCancellationView {
        let statement = self
            .statement
            .as_ref()
            .expect("protocol result retains its governed owner");
        QueryCancellationView::governed(statement.cancellation().clone(), statement.timeout_ms())
    }

    pub fn seal_success_visibility(&mut self) -> GovernedStatementVisibilitySealOutcome {
        self.statement
            .as_mut()
            .expect("protocol result retains its governed owner")
            .seal_success_visibility()
    }

    pub fn accept_cancel_delivery_cut(&mut self) -> Result<bool, WorkError> {
        self.statement
            .as_mut()
            .ok_or(WorkError::Released)?
            .accept_cancel_delivery_cut()
    }
    pub fn accept_failed_delivery_cut(&mut self) -> Result<bool, WorkError> {
        self.statement
            .as_mut()
            .ok_or(WorkError::Released)?
            .accept_failed_delivery_cut()
    }

    pub fn complete(&mut self) -> GovernedStatementFinishOutcome {
        self.settled = true;
        self.statement
            .take()
            .expect("protocol result retains its governed owner")
            .finish()
    }

    pub fn settle_cancellation(&mut self) -> GovernedStatementFinishOutcome {
        self.complete()
    }

    pub fn fail(&mut self) -> GovernedStatementFinishOutcome {
        self.settled = true;
        self.statement
            .take()
            .expect("protocol result retains its governed owner")
            .protocol_fail()
    }

    pub fn client_cancel_requested(&mut self) -> GovernedStatementFinishOutcome {
        self.fail_with_reason(novarocks_workload_control::CancellationReason::ClientCancelRequested)
    }

    pub fn client_disconnected(&mut self) -> GovernedStatementFinishOutcome {
        self.fail_with_reason(novarocks_workload_control::CancellationReason::ClientDisconnected)
    }

    fn fail_with_reason(
        &mut self,
        reason: novarocks_workload_control::CancellationReason,
    ) -> GovernedStatementFinishOutcome {
        self.settled = true;
        self.statement
            .take()
            .expect("protocol result retains its governed owner")
            .fail(reason)
    }
}

impl Drop for GovernedProtocolOwner {
    fn drop(&mut self) {
        if !self.settled {
            drop(self.statement.take());
        }
    }
}

/// Query-application output delivered to a client-session protocol adapter.
///
/// The adapter owns wire framing, while these values retain every application
/// lifetime that must remain live until the terminal protocol outcome.
pub enum QuerySessionOutput {
    /// Application-local command output. Protocol adapters must refuse this
    /// until the application wraps it with the original governed owner.
    Query(QueryResult),
    GovernedQuery(GovernedImmediateStatementResult),
    StreamingQuery(Box<StreamingStatementResult>),
    GovernedCompletion(GovernedCompletionStatementResult),
    GovernedError(GovernedErrorStatementResult),
    Ok,
}

impl QuerySessionOutput {
    pub fn external_effect_disposition(
        &self,
    ) -> Option<crate::api::StatementExternalEffectDisposition> {
        match self {
            Self::GovernedQuery(result) => result.protocol.external_effect_disposition(),
            Self::StreamingQuery(result) => result.protocol.external_effect_disposition(),
            Self::GovernedCompletion(result) => result.protocol.external_effect_disposition(),
            Self::GovernedError(result) => crate::api::StatementExternalEffectDisposition::combine(
                result.protocol.external_effect_disposition(),
                result.error.external_effect_disposition(),
            ),
            Self::Query(_) | Self::Ok => None,
        }
    }

    pub(crate) fn retain_original_external_effect(
        &mut self,
        effect: Option<crate::api::StatementExternalEffectDisposition>,
    ) {
        let protocol = match self {
            Self::GovernedQuery(result) => &mut result.protocol,
            Self::StreamingQuery(result) => &mut result.protocol,
            Self::GovernedCompletion(result) => &mut result.protocol,
            Self::GovernedError(result) => &mut result.protocol,
            Self::Query(_) | Self::Ok => return,
        };
        protocol.external_effect = effect;
    }
}

impl std::fmt::Debug for QuerySessionOutput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(result) => formatter.debug_tuple("Query").field(result).finish(),
            Self::GovernedQuery(_) => formatter.write_str("GovernedQuery(..)"),
            Self::StreamingQuery(_) => formatter.write_str("StreamingQuery(..)"),
            Self::GovernedCompletion(_) => formatter.write_str("GovernedCompletion(..)"),
            Self::GovernedError(_) => formatter.write_str("GovernedError(..)"),
            Self::Ok => formatter.write_str("Ok"),
        }
    }
}

/// Fully materialized result whose statement permit remains live through the
/// final protocol outcome.
#[must_use = "the governed query result must be settled by its protocol owner"]
pub struct GovernedImmediateStatementResult {
    result: OwnedLocalResult,
    protocol: GovernedProtocolOwner,
}

impl GovernedImmediateStatementResult {
    pub(crate) fn from_verified_owned_local(
        result: OwnedLocalResult,
        statement: GovernedQueryStatementOwner,
    ) -> Self {
        Self {
            result,
            protocol: GovernedProtocolOwner::new(statement),
        }
    }

    /// Transfer a freshly produced, closed Local graph while the admitted
    /// statement still retains the exact producer window. The application
    /// source audit, not the graph's runtime shape, establishes exclusivity.
    pub fn try_new(
        result: QueryResult,
        statement: GovernedQueryStatementOwner,
        family: novarocks_result_contract::ClientPresentationFamily,
    ) -> Result<Self, (String, GovernedQueryStatementOwner)> {
        let sealed = (|| {
            let window = statement
                .result_window_alias()
                .ok_or("Local result has no admitted producer window")?;
            let producer = LocalResultProducer::try_new(statement.scope(), window, family)?;
            producer.produce(|| Ok(result))
        })();
        match sealed {
            Ok(result) => Ok(Self {
                result,
                protocol: GovernedProtocolOwner::new(statement),
            }),
            Err(error) => Err((error, statement)),
        }
    }

    pub fn into_parts(self) -> (OwnedLocalResult, GovernedProtocolOwner) {
        (self.result, self.protocol)
    }
}

/// Completion-only output that retains its statement permit through the
/// terminal protocol OK packet.
#[must_use = "the governed completion must be settled by its protocol owner"]
pub struct GovernedCompletionStatementResult {
    completion: StatementCommandCompletion,
    completion_rows: Option<OwnedLocalResult>,
    protocol: GovernedProtocolOwner,
    effects: crate::session_effects::SessionEffects,
}

impl GovernedCompletionStatementResult {
    pub fn new(
        completion: StatementCommandCompletion,
        statement: GovernedQueryStatementOwner,
    ) -> Self {
        Self {
            completion,
            completion_rows: None,
            protocol: GovernedProtocolOwner::new(statement),
            effects: crate::session_effects::SessionEffects::default(),
        }
    }
    pub(crate) fn with_prepared_rows(
        completion: StatementCommandCompletion,
        completion_rows: Option<OwnedLocalResult>,
        statement: GovernedQueryStatementOwner,
    ) -> Self {
        Self {
            completion,
            completion_rows,
            protocol: GovernedProtocolOwner::new(statement),
            effects: crate::session_effects::SessionEffects::default(),
        }
    }
    pub const fn completion(&self) -> StatementCommandCompletion {
        self.completion
    }
    pub fn into_parts_with_rows(
        self,
    ) -> (
        StatementCommandCompletion,
        Option<OwnedLocalResult>,
        GovernedProtocolOwner,
    ) {
        (self.completion, self.completion_rows, self.protocol)
    }
    pub fn with_session_effects(
        statement: GovernedQueryStatementOwner,
        effects: crate::session_effects::SessionEffects,
    ) -> Self {
        Self {
            // Session mutations are control statements with no affected rows.
            // DML completion carries its own explicit logical row count.
            completion: StatementCommandCompletion::empty(),
            completion_rows: None,
            protocol: GovernedProtocolOwner::new(statement),
            effects,
        }
    }
    pub(crate) fn take_session_effects(&mut self) -> crate::session_effects::SessionEffects {
        std::mem::take(&mut self.effects)
    }

    pub fn into_parts(self) -> (StatementCommandCompletion, GovernedProtocolOwner) {
        (self.completion, self.protocol)
    }
}

/// Error output that retains its statement permit through the terminal
/// protocol error packet.
#[must_use = "the governed error must be settled by its protocol owner"]
pub struct GovernedErrorStatementResult {
    error: QueryServiceError,
    protocol: GovernedProtocolOwner,
}

impl GovernedErrorStatementResult {
    pub fn new(error: QueryServiceError, statement: GovernedQueryStatementOwner) -> Self {
        Self {
            error,
            protocol: GovernedProtocolOwner::new(statement),
        }
    }

    pub fn into_parts(self) -> (QueryServiceError, GovernedProtocolOwner) {
        (self.error, self.protocol)
    }
}

/// Query Application row stream retained by a protocol adapter.
///
/// The owner holds the logical execution control handle and statement permit
/// through the final protocol outcome. Dropping it cancels the execution and
/// lets the logical actor settle undelivered stream items.
#[must_use = "the streaming statement must be completed or explicitly failed by its protocol owner"]
pub struct StreamingStatementResult {
    execution: ExecutionOutputObligation,
    stream: QueryResultStream,
    protocol: GovernedProtocolOwner,
    settled: bool,
}

impl StreamingStatementResult {
    pub(crate) fn from_verified_execution(
        mut execution: ExecutionOutputObligation,
        statement: GovernedQueryStatementOwner,
    ) -> Self {
        let Some(ExecutionOutput::Rows(stream)) = execution.take_output() else {
            unreachable!("same verified original output");
        };
        Self {
            execution,
            stream,
            protocol: GovernedProtocolOwner::new(statement),
            settled: false,
        }
    }

    pub fn try_from_execution(
        mut execution: ExecutionOutputObligation,
        statement: GovernedQueryStatementOwner,
    ) -> Result<Self, QueryExecutionError> {
        let stream = match execution.take_output() {
            Some(ExecutionOutput::Rows(stream)) => stream,
            Some(ExecutionOutput::Completion) => {
                let _ = execution.request_cancel();
                return Err(QueryExecutionError::from_execution(
                    ExecutionFailureInput::new(
                        QueryFailureReason::ResultProtocol.failure_fact(),
                        SafeDetail::truncating("read execution returned completion-only output"),
                        ExecutionFailureSite::Coordinator,
                    ),
                ));
            }
            None => {
                let _ = execution.request_cancel();
                return Err(QueryExecutionError::from_execution(
                    ExecutionFailureInput::new(
                        QueryFailureReason::ResultProtocol.failure_fact(),
                        SafeDetail::truncating("read execution output was already transferred"),
                        ExecutionFailureSite::Coordinator,
                    ),
                ));
            }
        };
        Ok(Self {
            execution,
            stream,
            protocol: GovernedProtocolOwner::new(statement),
            settled: false,
        })
    }

    pub fn external_effect_disposition(
        &self,
    ) -> Option<crate::api::StatementExternalEffectDisposition> {
        self.protocol.external_effect_disposition()
    }

    /// Project the protocol owner's exact identity; observation grants no control.
    pub fn statement_token(&self) -> Option<StatementToken> {
        self.protocol.statement_token()
    }

    pub fn is_observed_by(
        &self,
        observer: &novarocks_workload_control::WorkloadObservationHandle,
    ) -> bool {
        self.protocol.is_observed_by(observer)
    }

    pub fn begin_schema(&mut self) -> Option<SchemaDelivery> {
        self.stream.begin_schema()
    }

    pub async fn next_delivery(&mut self) -> Result<Option<ResultDelivery>, QueryExecutionError> {
        self.stream.next().await
    }

    pub fn failure_view(&self) -> Option<ResultFailureView> {
        self.stream.failure_view()
    }

    pub fn request_cancel(&self) -> Result<(), QueryExecutionError> {
        self.execution.request_cancel()
    }

    pub fn cancellation(&self) -> QueryCancellationView {
        self.protocol.cancellation()
    }

    pub fn seal_success_visibility(&mut self) -> GovernedStatementVisibilitySealOutcome {
        self.protocol.seal_success_visibility()
    }

    pub fn result_window_alias(
        &self,
    ) -> Result<novarocks_workload_control::ResultWindowAlias, WorkError> {
        self.protocol.result_window_alias()
    }

    pub fn complete(mut self) -> GovernedStatementFinishOutcome {
        self.settled = true;
        self.protocol.complete()
    }

    pub fn fail(mut self) -> GovernedStatementFinishOutcome {
        let _ = self.execution.request_cancel();
        self.settled = true;
        self.protocol.fail()
    }

    pub fn settle_cancellation(mut self) -> GovernedStatementFinishOutcome {
        let _ = self.execution.request_cancel();
        self.settled = true;
        self.protocol.settle_cancellation()
    }

    pub fn client_cancel_requested(mut self) -> GovernedStatementFinishOutcome {
        // Publish the specific client cause before the generic execution interrupt.
        let outcome = self.protocol.client_cancel_requested();
        let _ = self.execution.request_cancel();
        self.settled = true;
        outcome
    }

    pub fn client_disconnected(mut self) -> GovernedStatementFinishOutcome {
        let outcome = self.protocol.client_disconnected();
        let _ = self.execution.request_cancel();
        self.settled = true;
        outcome
    }
}

impl Drop for StreamingStatementResult {
    fn drop(&mut self) {
        if !self.settled {
            let _ = self.execution.request_cancel();
        }
    }
}

#[cfg(test)]
mod execution_handoff_tests {
    use super::*;
    use crate::query_control::QueryApplicationControl;
    use crate::session_control::{QueryControlService, SessionIdentity};
    use novarocks_workload_control::{
        CancellationReason, ResourceConfig, WorkloadConfig, WorkloadControl,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn explicit_client_cancel_preserves_sealed_success_and_specific_unsealed_cause() {
        for sealed in [false, true] {
            let application = Arc::new(QueryApplicationControl::default());
            let service = QueryControlService::new(application);
            let session = service
                .register_session(SessionIdentity::client_statement(
                    service.mint_client_query_id(),
                    "root",
                ))
                .unwrap();
            let workload = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 1024,
                    control_bytes: 128,
                    per_scope_bytes: 896,
                },
            )
            .unwrap();
            workload.mark_ready().unwrap();
            let mut statement = service
                .begin_queued_governed_query_statement(
                    session.token(),
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            let owner = statement.take_execution_owner().unwrap();
            let scope = statement.scope().clone();
            // Keep the original read-only facts before settlement releases Work.
            let cancellation = scope.cancellation().unwrap();
            owner.complete();
            let mut protocol = GovernedProtocolOwner::new(statement);
            if sealed {
                assert_eq!(
                    protocol.seal_success_visibility(),
                    GovernedStatementVisibilitySealOutcome::Sealed
                );
            }
            let outcome = protocol.client_cancel_requested();
            assert_eq!(
                outcome,
                if sealed {
                    GovernedStatementFinishOutcome::ProtocolFailed
                } else {
                    GovernedStatementFinishOutcome::Cancelled(
                        CancellationReason::ClientCancelRequested,
                    )
                }
            );
            assert_eq!(
                cancellation.reason(),
                if sealed {
                    None
                } else {
                    Some(CancellationReason::ClientCancelRequested)
                }
            );
            drop(cancellation);
            while let Some(permit) = workload.next_control() {
                permit.acknowledge();
            }
            scope.wait_released().await;
            assert_eq!(workload.snapshot().admitted_queries, 0);
            assert_eq!(workload.snapshot().root_responsibilities, 0);
        }
    }

    #[tokio::test]
    async fn real_output_handoff_guards_preserve_cancel_and_original_work_exit() {
        for already_transferred in [false, true] {
            let application = Arc::new(QueryApplicationControl::default());
            let service = QueryControlService::new(application);
            let session = service
                .register_session(SessionIdentity::client_statement(
                    service.mint_client_query_id(),
                    "root",
                ))
                .unwrap();
            let workload = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 1024,
                    control_bytes: 128,
                    per_scope_bytes: 896,
                },
            )
            .unwrap();
            workload.mark_ready().unwrap();
            let mut statement = service
                .begin_queued_governed_query_statement(
                    session.token(),
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            let scope = statement.scope().clone();
            let owner = statement.take_execution_owner().unwrap();
            let mut execution = ExecutionOutputObligation::new(
                owner.cancellation_requester(),
                ExecutionOutput::Completion,
            );
            if already_transferred {
                assert!(matches!(
                    execution.take_output(),
                    Some(ExecutionOutput::Completion)
                ));
            }
            let error = match StreamingStatementResult::try_from_execution(execution, statement) {
                Err(error) => error,
                Ok(_) => panic!("Rows handoff cannot accept this output shape"),
            };
            let input = error
                .execution_failure()
                .expect("the guard supplies its own result contract fact");
            assert_eq!(
                input.fact(),
                QueryFailureReason::ResultProtocol.failure_fact()
            );
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                input.user_error().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
            assert_eq!(input.user_error().location(), None);
            assert_eq!(
                error.message(),
                if already_transferred {
                    "read execution output was already transferred"
                } else {
                    "read execution returned completion-only output"
                }
            );
            assert_eq!(
                scope.cancellation().unwrap().reason(),
                Some(CancellationReason::Requested)
            );
            assert_eq!(workload.snapshot().root_responsibilities, 1);
            owner.complete();
            while let Some(permit) = workload.next_control() {
                permit.acknowledge();
            }
            scope.wait_released().await;
            assert_eq!(workload.snapshot().root_responsibilities, 0);
        }
    }
}
