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

use super::query_result::QueryResult;
use crate::common::query_cancellation::QueryCancellationView;
use crate::query_execution::control::{
    GovernedQueryStatementOwner, GovernedStatementFinishOutcome,
    GovernedStatementVisibilitySealOutcome,
};
use novarocks_query_application::api::{
    ExecutionHandle, ExecutionOutput, QueryExecutionError, QueryExecutionErrorKind,
    QueryResultStream, ResultDelivery, ResultFailureView, SchemaDelivery,
};
use novarocks_workload_control::{LocalResourceAuthority, WorkScope};

/// Neutral statement result carrier shared by Core domain handlers and the
/// Frontend query-assembly owner.
pub enum StatementResult {
    Query(QueryResult),
    /// Immediate query output whose business and cancellation owner remains
    /// live through the final MySQL protocol outcome.
    GovernedQuery(GovernedImmediateStatementResult),
    /// Move-only Query Application output. The protocol adapter owns this
    /// value until schema, every batch, and success EOF have reached the
    /// client or the connection has failed.
    StreamingQuery(StreamingStatementResult),
    Ok,
}

impl std::fmt::Debug for StatementResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(result) => formatter.debug_tuple("Query").field(result).finish(),
            Self::GovernedQuery(_) => formatter.write_str("GovernedQuery(..)"),
            Self::StreamingQuery(_) => formatter.write_str("StreamingQuery(..)"),
            Self::Ok => formatter.write_str("Ok"),
        }
    }
}

/// Shared move-only owner for any governed query result presented to a client.
pub(crate) struct GovernedProtocolOwner {
    statement: Option<GovernedQueryStatementOwner>,
    resources: LocalResourceAuthority,
    settled: bool,
}

impl GovernedProtocolOwner {
    fn new(statement: GovernedQueryStatementOwner, resources: LocalResourceAuthority) -> Self {
        Self {
            statement: Some(statement),
            resources,
            settled: false,
        }
    }

    pub(crate) fn cancellation(&self) -> QueryCancellationView {
        let statement = self
            .statement
            .as_ref()
            .expect("protocol result retains its governed owner");
        QueryCancellationView::governed(statement.cancellation().clone(), statement.timeout_ms())
    }

    pub(crate) fn reservation_inputs(&self) -> (LocalResourceAuthority, WorkScope) {
        let statement = self
            .statement
            .as_ref()
            .expect("protocol result retains its governed owner");
        (self.resources.clone(), statement.scope().clone())
    }

    pub(crate) fn seal_success_visibility(&mut self) -> GovernedStatementVisibilitySealOutcome {
        self.statement
            .as_mut()
            .expect("protocol result retains its governed owner")
            .seal_success_visibility()
    }

    pub(crate) fn complete(&mut self) -> GovernedStatementFinishOutcome {
        self.settled = true;
        self.statement
            .take()
            .expect("protocol result retains its governed owner")
            .finish()
    }

    pub(crate) fn settle_cancellation(&mut self) -> GovernedStatementFinishOutcome {
        self.complete()
    }

    pub(crate) fn fail(&mut self) -> GovernedStatementFinishOutcome {
        self.settled = true;
        self.statement
            .take()
            .expect("protocol result retains its governed owner")
            .protocol_fail()
    }

    pub(crate) fn client_disconnected(&mut self) -> GovernedStatementFinishOutcome {
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

#[must_use = "the governed query result must be settled by its protocol owner"]
pub struct GovernedImmediateStatementResult {
    result: QueryResult,
    protocol: GovernedProtocolOwner,
}

impl GovernedImmediateStatementResult {
    pub(crate) fn new(
        result: QueryResult,
        resources: LocalResourceAuthority,
        statement: GovernedQueryStatementOwner,
    ) -> Self {
        Self {
            result,
            protocol: GovernedProtocolOwner::new(statement, resources),
        }
    }

    pub(crate) fn into_parts(self) -> (QueryResult, GovernedProtocolOwner) {
        (self.result, self.protocol)
    }
}

/// The unique Frontend owner of a Query Application row stream while MySQL
/// consumes it.
///
/// This owner deliberately retains the execution control handle and the one
/// governed statement owner which holds business admission plus the active
/// statement generation. Dropping it means the protocol consumer disappeared,
/// so the logical execution is cancelled and remaining stream deliveries are
/// settled by their actor.
#[must_use = "the streaming statement must be completed or explicitly failed by its protocol owner"]
pub struct StreamingStatementResult {
    execution: ExecutionHandle,
    stream: QueryResultStream,
    resources: LocalResourceAuthority,
    protocol: GovernedProtocolOwner,
    settled: bool,
}

impl StreamingStatementResult {
    pub(crate) fn try_from_execution(
        mut execution: ExecutionHandle,
        resources: LocalResourceAuthority,
        statement: GovernedQueryStatementOwner,
    ) -> Result<Self, QueryExecutionError> {
        let stream = match execution.take_output() {
            Some(ExecutionOutput::Rows(stream)) => stream,
            Some(ExecutionOutput::Completion) => {
                let _ = execution.request_cancel();
                return Err(QueryExecutionError::new(
                    QueryExecutionErrorKind::InvalidRequest,
                    "read execution returned completion-only output",
                ));
            }
            None => {
                let _ = execution.request_cancel();
                return Err(QueryExecutionError::new(
                    QueryExecutionErrorKind::InvalidRequest,
                    "read execution output was already transferred",
                ));
            }
        };
        Ok(Self {
            execution,
            stream,
            resources: resources.clone(),
            protocol: GovernedProtocolOwner::new(statement, resources),
            settled: false,
        })
    }

    pub(crate) fn begin_schema(&mut self) -> Option<SchemaDelivery> {
        self.stream.begin_schema()
    }

    pub(crate) async fn next_delivery(
        &mut self,
    ) -> Result<Option<ResultDelivery>, QueryExecutionError> {
        self.stream.next().await
    }

    pub(crate) fn failure_view(&self) -> Option<ResultFailureView> {
        self.stream.failure_view()
    }

    pub(crate) const fn resources(&self) -> &LocalResourceAuthority {
        &self.resources
    }

    pub(crate) fn reservation_inputs(&self) -> (LocalResourceAuthority, WorkScope) {
        self.protocol.reservation_inputs()
    }

    pub(crate) fn request_cancel(&self) -> Result<(), QueryExecutionError> {
        self.execution.request_cancel()
    }

    pub(crate) fn cancellation(&self) -> QueryCancellationView {
        self.protocol.cancellation()
    }

    pub(crate) fn seal_success_visibility(&mut self) -> GovernedStatementVisibilitySealOutcome {
        self.protocol.seal_success_visibility()
    }

    pub(crate) fn complete(mut self) -> GovernedStatementFinishOutcome {
        self.settled = true;
        self.protocol.complete()
    }

    pub(crate) fn fail(mut self) -> GovernedStatementFinishOutcome {
        let _ = self.execution.request_cancel();
        self.settled = true;
        self.protocol.fail()
    }

    pub(crate) fn settle_cancellation(mut self) -> GovernedStatementFinishOutcome {
        let _ = self.execution.request_cancel();
        self.settled = true;
        self.protocol.settle_cancellation()
    }

    pub(crate) fn client_disconnected(mut self) -> GovernedStatementFinishOutcome {
        let _ = self.execution.request_cancel();
        self.settled = true;
        self.protocol.client_disconnected()
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
mod tests {
    use std::sync::Arc;

    use novarocks_workload_control::{ResourceConfig, WorkloadConfig, WorkloadControl};

    use super::*;
    use crate::client_connection::ClientConnectionToken;
    use crate::query_control::FrontendQueryControl;
    use crate::query_execution::control::{
        QueryControlPort, QueryControlService, QuerySessionLease, SessionIdentity,
    };

    fn immediate_fixture() -> (
        GovernedImmediateStatementResult,
        WorkloadControl,
        QuerySessionLease,
    ) {
        let control = QueryControlService::new(
            Arc::new(FrontendQueryControl::default()) as Arc<dyn QueryControlPort>
        );
        let session = control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(301, 1).expect("connection token"),
                "root",
            ))
            .expect("register session");
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload control");
        workload.mark_ready().expect("workload ready");
        let mut statement = control
            .begin_governed_query_statement(session.token(), &workload.root_admission(), None, None)
            .expect("governed statement");
        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();
        let result = GovernedImmediateStatementResult::new(
            QueryResult::empty(),
            workload.resources(),
            statement,
        );
        (result, workload, session)
    }

    #[test]
    fn governed_immediate_retains_business_through_success_visibility_seal() {
        let (result, workload, _session) = immediate_fixture();
        assert_eq!(workload.snapshot().businesses, 1);

        let (_, mut protocol) = result.into_parts();
        assert_eq!(
            protocol.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        assert_eq!(workload.snapshot().businesses, 1);
        assert_eq!(
            protocol.complete(),
            GovernedStatementFinishOutcome::Completed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_immediate_eof_failure_after_seal_is_protocol_failed() {
        let (result, workload, _session) = immediate_fixture();
        let (_, mut protocol) = result.into_parts();
        assert_eq!(
            protocol.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        assert_eq!(
            protocol.fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[tokio::test]
    async fn governed_protocol_cancellation_preserves_configured_timeout() {
        let control = QueryControlService::new(
            Arc::new(FrontendQueryControl::default()) as Arc<dyn QueryControlPort>
        );
        let session = control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(302, 1).expect("connection token"),
                "root",
            ))
            .expect("register session");
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload control");
        workload.mark_ready().expect("workload ready");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(1);
        let statement = control
            .begin_governed_query_statement(
                session.token(),
                &workload.root_admission(),
                Some(deadline),
                Some(73),
            )
            .expect("governed statement");
        let protocol = GovernedProtocolOwner::new(statement, workload.resources());

        assert_eq!(
            protocol.cancellation().cancelled().await,
            crate::common::query_cancellation::QueryCancellationReason::DeadlineExceeded {
                timeout_ms: 73,
            }
        );
    }
}
