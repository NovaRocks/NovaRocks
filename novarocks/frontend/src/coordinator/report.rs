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

use std::sync::Arc;

use crate::query_execution::contract::DistributedQueryErrorKind;
use crate::query_lifecycle::{QueryLifecycleError, QueryLifecycleErrorCode, QueryTerminalIngress};
use novarocks_protocol::lifecycle::{
    ParticipantTerminalOutcome, QueryTerminalReportAck, QueryTerminalReportOutcome,
};

use super::query_registry::FrontendQueryRegistry;

/// Typed unary fallback owner for QLC-4 terminal delivery.  It shares the
/// query registry with stream delivery but is intentionally not a fragment
/// execution-status handler.
#[derive(Clone)]
pub struct FrontendCoordinatorTerminalIngress {
    registry: Arc<FrontendQueryRegistry>,
}

impl FrontendCoordinatorTerminalIngress {
    pub(crate) fn new(registry: Arc<FrontendQueryRegistry>) -> Self {
        Self { registry }
    }
}

impl QueryTerminalIngress for FrontendCoordinatorTerminalIngress {
    fn report_query_terminal(
        &self,
        outcome: ParticipantTerminalOutcome,
    ) -> Result<QueryTerminalReportAck, QueryLifecycleError> {
        match self.registry.report_query_terminal(outcome) {
            Ok(true) => terminal_report_ack(
                QueryTerminalReportOutcome::Accepted,
                "terminal outcome stored",
            ),
            Ok(false) => terminal_report_ack(
                QueryTerminalReportOutcome::AlreadyAccepted,
                "terminal outcome already stored",
            ),
            Err(error) if matches!(error.kind(), DistributedQueryErrorKind::Rejected) => {
                terminal_report_ack(QueryTerminalReportOutcome::RejectedGone, error.message())
            }
            Err(error) if matches!(error.kind(), DistributedQueryErrorKind::ContractViolation) => {
                terminal_report_ack(
                    QueryTerminalReportOutcome::RejectedConflict,
                    error.message(),
                )
            }
            Err(error) => Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Internal,
                error.message(),
            )),
        }
    }
}

fn terminal_report_ack(
    outcome: QueryTerminalReportOutcome,
    detail: impl Into<String>,
) -> Result<QueryTerminalReportAck, QueryLifecycleError> {
    QueryTerminalReportAck::new(outcome, detail).map_err(|error| {
        QueryLifecycleError::new(
            QueryLifecycleErrorCode::Internal,
            format!("failed to construct query terminal report acknowledgement: {error}"),
        )
    })
}
