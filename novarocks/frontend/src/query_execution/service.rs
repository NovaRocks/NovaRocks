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

//! Explicit value injection for distributed query execution.

use std::sync::Arc;

use crate::query_execution::completion::{
    LogicalQueryReservation, PreparedDistributedQuery, PreparedRetriableDistributedRequest,
    QueryAttemptReservation,
};
use crate::query_execution::contract::{
    DistributedQueryCoordinator, DistributedQueryError, DistributedQueryOutcome,
    DistributedQueryRequest,
};
use crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession;
use crate::runtime::statement_result::StatementResult;
use novarocks_spi::connector::{
    ConnectorDistributedRewriteLease, ConnectorDistributedRewritePlan, ConnectorRequestContext,
};

#[derive(Clone)]
pub struct QueryExecutionService {
    coordinator: Arc<dyn DistributedQueryCoordinator>,
}

impl QueryExecutionService {
    pub fn new(coordinator: Arc<dyn DistributedQueryCoordinator>) -> Self {
        Self { coordinator }
    }

    /// Submit a fully prepared request to the frontend-owned coordinator.
    pub fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.coordinator.execute(request)
    }

    /// Execute one statement-owned reserved attempt without allowing the
    /// coordinator to construct an automatic replacement round.
    pub(crate) fn execute_reserved(
        &self,
        request: DistributedQueryRequest,
        reservation: QueryAttemptReservation,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.coordinator.execute_reserved(request, reservation)
    }

    /// Submit one statement-owned distributed operation. Production may
    /// replace a pre-ready round, but only through a factory that returns a
    /// complete new request and matching completion formatter.
    pub(crate) fn execute_prepared(
        &self,
        operation: PreparedDistributedQuery,
    ) -> Result<StatementResult, DistributedQueryError> {
        self.coordinator.execute_prepared(operation)
    }

    /// Reserve one logical query identity for preparation diagnostics. This
    /// does not create an execution attempt or any attempt-scoped capability.
    pub(crate) fn reserve_logical_query(
        &self,
    ) -> Result<LogicalQueryReservation, DistributedQueryError> {
        self.coordinator.reserve_logical_query()
    }

    /// Reserve a candidate first-round identity for an effectful statement
    /// whose staged protocol must own attempt-scoped capabilities during
    /// request construction. Read-only query preparation never calls this.
    pub(crate) fn reserve_initial_attempt(
        &self,
    ) -> Result<QueryAttemptReservation, DistributedQueryError> {
        self.coordinator.reserve_initial_attempt()
    }

    /// Submit a statement-owned operation whose caller must retain the raw
    /// distributed outcome, including write terminal handles.
    pub(crate) fn execute_prepared_raw(
        &self,
        operation: PreparedRetriableDistributedRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.coordinator.execute_prepared_raw(operation)
    }

    /// Seal a provider-frozen distributed rewrite against the composite lease
    /// that selected its metadata, rewrite and C1 write capabilities together.
    pub fn begin_distributed_rewrite_operation_with_lease(
        &self,
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
        write_stack: crate::connector::control_host::ConnectorWriteStackLease,
        table: &novarocks_spi::connector::ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorDistributedRewriteSession, DistributedQueryError> {
        ConnectorDistributedRewriteSession::try_begin(plan, lease, write_stack, table, context)
            .map_err(|error| {
                DistributedQueryError::new(
                    crate::query_execution::contract::DistributedQueryErrorKind::Failed,
                    format!("seal distributed rewrite operation cohorts: {error}"),
                )
            })
    }
}
