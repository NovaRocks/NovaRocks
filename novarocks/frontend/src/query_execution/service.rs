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

use crate::query_execution::contract::{
    ConnectorWriteOperationRegistration, DistributedQueryCoordinator, DistributedQueryError,
    DistributedQueryOutcome, DistributedQueryRequest,
};
use crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession;
use crate::query_execution::write_operation::ConnectorWriteOperationSession;
use novarocks_spi::connector::{
    ConnectorDistributedRewriteLease, ConnectorDistributedRewritePlan, ConnectorRequestContext,
    ConnectorWriteLease,
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

    /// Seal every cohort against the application-retained exact control lease
    /// before any distributed writer attempt is staged.
    pub fn begin_write_operation(
        &self,
        registration: ConnectorWriteOperationRegistration,
        lease: ConnectorWriteLease,
    ) -> Result<ConnectorWriteOperationSession, DistributedQueryError> {
        self.coordinator.begin_write_operation(registration, lease)
    }

    /// Seal a provider-frozen distributed rewrite against the composite lease
    /// that selected its metadata, rewrite and C1 write capabilities together.
    pub fn begin_distributed_rewrite_operation_with_lease(
        &self,
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorDistributedRewriteSession, DistributedQueryError> {
        ConnectorDistributedRewriteSession::try_begin(plan, lease, context).map_err(|error| {
            DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::Failed,
                format!("seal distributed rewrite operation cohorts: {error}"),
            )
        })
    }
}
