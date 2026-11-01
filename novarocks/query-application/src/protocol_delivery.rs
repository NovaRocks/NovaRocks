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
    ExecutionHandle, ExecutionOutput, LocalResultProducer, OwnedLocalResult, QueryExecutionError,
    QueryResult, QueryResultStream, ResultDelivery, ResultFailureView, SchemaDelivery,
};
use crate::cancellation::QueryCancellationView;
use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason};
use crate::session_control::{
    GovernedQueryStatementOwner, GovernedStatementFinishOutcome,
    GovernedStatementVisibilitySealOutcome, StatementCommandCompletion, StatementToken,
};
use crate::session_error::QueryServiceError;
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_workload_control::{ResultWindowClass, ResultWindowGrant, WorkError};

/// Shared move-only owner for any governed query result presented to a client.
#[must_use = "the governed protocol owner must be settled by its protocol adapter"]
pub struct GovernedProtocolOwner {
    statement: Option<GovernedQueryStatementOwner>,
    settled: bool,
}

impl GovernedProtocolOwner {
    pub fn new(statement: GovernedQueryStatementOwner) -> Self {
        Self {
            statement: Some(statement),
            settled: false,
        }
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

    fn closing_capacity(
        &self,
        cut: novarocks_workload_control::ResultClosingCut,
    ) -> Result<ResultWindowGrant, WorkError> {
        let statement = self.statement.as_ref().ok_or(WorkError::Released)?;
        statement
            .scope()
            .result_capacity()?
            .try_acquire_closing(statement.scope(), cut)
    }

    pub fn try_closing_capacity(
        &mut self,
        cancelled: bool,
    ) -> Result<ResultWindowGrant, WorkError> {
        let cut = if cancelled {
            self.accept_cancel_delivery_cut()?;
            novarocks_workload_control::ResultClosingCut::AcceptedCancellation
        } else {
            self.accept_failed_delivery_cut()?;
            novarocks_workload_control::ResultClosingCut::OriginatingFailure
        };
        self.closing_capacity(cut)
    }

    /// A Local caller destroys its renderer/source and transfers only the
    /// bounded writer tail. Any still-live ordinary backing keeps its alias.
    pub fn into_closing_delivery<W>(
        self,
        writer: W,
        capacity: ResultWindowGrant,
        simultaneously_live_backing_bytes: u64,
    ) -> Result<ClosingDelivery<W>, (Self, W, ResultWindowGrant)> {
        match ClosingDelivery::try_new(writer, self, capacity, simultaneously_live_backing_bytes) {
            Ok(mut closing) => {
                std::sync::Arc::get_mut(closing.tail.as_mut().expect("new closing tail"))
                    .expect("new closing has no aliases")
                    .protocol
                    .as_mut()
                    .expect("closing protocol")
                    .statement
                    .as_mut()
                    .expect("closing statement")
                    .release_transferred_result_window();
                Ok(closing)
            }
            Err((writer, protocol, capacity)) => Err((protocol, writer, capacity)),
        }
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

/// Move-only protocol tail with its own complete capacity position. `W` owns
/// the detached socket writer, frozen metadata, packet cursor and validated
/// resident current-row tail; it has no root fetch/render capability. Any
/// physical writer alias must retain `retained_guard()` through actual exit.
#[must_use = "a closing delivery retains its statement generation and writer until actual exit"]
pub struct ClosingDelivery<W> {
    writer: Option<W>,
    tail: Option<std::sync::Arc<ClosingTailOwner>>,
    completion: Option<tokio::sync::oneshot::Receiver<GovernedStatementFinishOutcome>>,
}
struct ClosingTailOwner {
    protocol: Option<GovernedProtocolOwner>,
    capacity: ResultWindowGrant,
    writer_exited: std::sync::atomic::AtomicBool,
    completion: Option<tokio::sync::oneshot::Sender<GovernedStatementFinishOutcome>>,
}
impl Drop for ClosingTailOwner {
    fn drop(&mut self) {
        let mut protocol = self
            .protocol
            .take()
            .expect("closing retains its protocol owner");
        let outcome = if !self
            .writer_exited
            .load(std::sync::atomic::Ordering::Acquire)
        {
            protocol.client_disconnected()
        } else {
            match self
                .capacity
                .closing_cut()
                .expect("closing requires an accepted cut")
            {
                novarocks_workload_control::ResultClosingCut::AcceptedCancellation => {
                    protocol.settle_cancellation()
                }
                novarocks_workload_control::ResultClosingCut::OriginatingFailure => protocol.fail(),
            }
        };
        if let Some(completion) = self.completion.take() {
            let _ = completion.send(outcome);
        }
    }
}
/// Every physical writer/backing alias retains BOTH the statement generation
/// and the independent closing position. Its last drop is the actual exit cut.
#[derive(Clone)]
pub struct ClosingDeliveryAlias {
    tail: std::sync::Arc<ClosingTailOwner>,
}
impl ClosingDeliveryAlias {
    pub fn scope_id(&self) -> novarocks_workload_control::WorkId {
        self.tail.capacity.scope_id()
    }
}
impl<W> ClosingDelivery<W> {
    /// The caller checks full simultaneous backing capacity before moving the
    /// writer. A closing grant with preexisting raw aliases cannot transfer:
    /// all subsequent physical aliases must retain the protocol owner as well.
    /// Rejection returns every owner intact for immediate disconnect.
    pub fn try_new(
        writer: W,
        protocol: GovernedProtocolOwner,
        capacity: ResultWindowGrant,
        simultaneously_live_backing_bytes: u64,
    ) -> Result<Self, (W, GovernedProtocolOwner, ResultWindowGrant)> {
        let valid_scope = protocol.statement.as_ref().is_some_and(|owner| {
            capacity.is_for_scope(owner.scope())
                && owner.accepted_delivery_cut().is_some()
                && owner.accepted_delivery_cut() == capacity.closing_cut()
        });
        if capacity.class() != ResultWindowClass::Closing
            || !valid_scope
            || capacity.has_retained_aliases()
            || capacity
                .check_backing_total(simultaneously_live_backing_bytes)
                .is_err()
        {
            return Err((writer, protocol, capacity));
        }
        let (completion, receiver) = tokio::sync::oneshot::channel();
        Ok(Self {
            writer: Some(writer),
            tail: Some(std::sync::Arc::new(ClosingTailOwner {
                protocol: Some(protocol),
                capacity,
                writer_exited: std::sync::atomic::AtomicBool::new(false),
                completion: Some(completion),
            })),
            completion: Some(receiver),
        })
    }
    pub fn writer_mut(&mut self) -> &mut W {
        self.writer
            .as_mut()
            .expect("closing owns its detached writer")
    }
    pub fn retained_guard(&self) -> ClosingDeliveryAlias {
        ClosingDeliveryAlias {
            tail: std::sync::Arc::clone(
                self.tail.as_ref().expect("closing retains its tail owner"),
            ),
        }
    }
    /// Invoke after ERR/flush completes or disconnect. Generation settlement
    /// still waits for every physical alias. Cancelling this wait cannot settle
    /// or release a still-live alias; its owner retains the protocol authority.
    pub async fn settle_after_writer_exit(mut self) -> GovernedStatementFinishOutcome {
        drop(self.writer.take());
        self.tail
            .as_ref()
            .expect("closing owns tail")
            .writer_exited
            .store(true, std::sync::atomic::Ordering::Release);
        let completion = self.completion.take().expect("closing owns completion");
        drop(self.tail.take());
        completion
            .await
            .expect("last closing holder publishes settlement")
    }
}
impl<W> Drop for ClosingDelivery<W> {
    fn drop(&mut self) {
        // Writer destruction precedes its tail owner. Last-alias destruction
        // settles cancellation/disconnect; dropping a waiter never proves exit.
        drop(self.writer.take());
        drop(self.tail.take());
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
            protocol: GovernedProtocolOwner::new(statement),
            effects: crate::session_effects::SessionEffects::default(),
        }
    }
    pub fn with_session_effects(
        statement: GovernedQueryStatementOwner,
        effects: crate::session_effects::SessionEffects,
    ) -> Self {
        Self {
            // Session mutations are control statements with no affected rows.
            // DML completion carries its own explicit logical row count.
            completion: StatementCommandCompletion::new(0),
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
    execution: ExecutionHandle,
    stream: QueryResultStream,
    protocol: GovernedProtocolOwner,
    settled: bool,
}

impl StreamingStatementResult {
    pub(crate) fn from_verified_execution(
        mut execution: ExecutionHandle,
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
        mut execution: ExecutionHandle,
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

    /// Fix the delivery verdict and return computation capacity before a
    /// possibly slow protocol tail. Closing admission is a single try.
    pub fn try_closing_capacity(
        &mut self,
        cancelled: bool,
    ) -> Result<ResultWindowGrant, WorkError> {
        let _ = self.execution.request_cancel();
        let cut = if cancelled {
            self.protocol.accept_cancel_delivery_cut()?;
            novarocks_workload_control::ResultClosingCut::AcceptedCancellation
        } else {
            self.protocol.accept_failed_delivery_cut()?;
            novarocks_workload_control::ResultClosingCut::OriginatingFailure
        };
        self.protocol.closing_capacity(cut)
    }

    /// Transfer only after the independent capacity proves the complete tail.
    /// A rejected handoff returns every original owner intact.
    pub fn into_closing_delivery<W>(
        mut self,
        writer: W,
        capacity: ResultWindowGrant,
        simultaneously_live_backing_bytes: u64,
    ) -> Result<ClosingDelivery<W>, (Self, W, ResultWindowGrant)> {
        let _ = self.execution.request_cancel();
        self.settled = true;
        let protocol = std::mem::replace(
            &mut self.protocol,
            GovernedProtocolOwner {
                statement: None,
                settled: true,
            },
        );
        match ClosingDelivery::try_new(
            writer,
            protocol,
            capacity,
            simultaneously_live_backing_bytes,
        ) {
            Ok(mut closing) => {
                let tail = std::sync::Arc::get_mut(
                    closing.tail.as_mut().expect("new closing retains tail"),
                )
                .expect("new closing has no aliases");
                tail.protocol
                    .as_mut()
                    .expect("new closing retains protocol")
                    .statement
                    .as_mut()
                    .expect("closing protocol retains statement")
                    .release_transferred_result_window();
                Ok(closing)
            }
            Err((writer, protocol, capacity)) => {
                self.protocol = protocol;
                self.settled = false;
                Err((self, writer, capacity))
            }
        }
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
    use crate::client_connection::ClientConnectionToken;
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
                .register_session(SessionIdentity::new(
                    ClientConnectionToken::new(7, 1).unwrap(),
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
            let mut execution =
                ExecutionHandle::new(owner.cancellation_requester(), ExecutionOutput::Completion);
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
