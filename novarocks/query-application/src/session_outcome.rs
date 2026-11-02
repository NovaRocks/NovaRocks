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

//! Session-local deadline and terminal-error interpretation.

use std::time::{Duration, Instant};

use crate::api::{QueryExecutionError, QueryExecutionErrorKind};
use crate::cancellation::QueryCancellationReason;
use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason};
use crate::session_control::GovernedStatementFinishOutcome;
use crate::session_error::{QueryServiceError, QueryServiceErrorKind};
use crate::sql::session::SessionSqlState;
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_workload_control::CancellationReason as WorkCancellationReason;

pub fn governed_query_deadline(
    state: &SessionSqlState,
) -> Result<(Option<Instant>, Option<u64>), QueryServiceError> {
    let timeout_secs = state.execution_settings().query_timeout_secs();
    let deadline = match timeout_secs {
        Some(seconds) => Some(
            Instant::now()
                .checked_add(Duration::from_secs(seconds))
                .ok_or_else(|| internal_error("query deadline exceeds monotonic clock range"))?,
        ),
        None => None,
    };
    Ok((
        deadline,
        timeout_secs.map(|seconds| seconds.saturating_mul(1_000)),
    ))
}

pub fn cancellation_error(reason: QueryCancellationReason) -> QueryServiceError {
    QueryServiceError::from_execution_failure(cancellation_failure_input(reason))
}

/// Project an actually observed statement control reason once for every
/// service and execution-result exit. This claims no Task acceptance or phase.
pub fn cancellation_failure_input(reason: QueryCancellationReason) -> ExecutionFailureInput {
    let (reason, message) = match reason {
        QueryCancellationReason::ExecutionCancellationRequested => (
            QueryFailureReason::Cancelled,
            "Query execution cancellation was requested".to_string(),
        ),
        QueryCancellationReason::ExecutionOwnerDropped => (
            QueryFailureReason::Cancelled,
            "Query execution protocol owner was dropped".to_string(),
        ),
        QueryCancellationReason::DeadlineExceeded { timeout_ms } => (
            QueryFailureReason::DeadlineExceeded,
            format!("query timed out after {timeout_ms} ms"),
        ),
        QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms } => (
            QueryFailureReason::DeadlineExceeded,
            format!(
                "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded after {timeout_ms} ms"
            ),
        ),
        QueryCancellationReason::ExplicitKill { .. } => (
            QueryFailureReason::Cancelled,
            "Query execution was interrupted".to_string(),
        ),
        QueryCancellationReason::ExplicitKillConnection { .. } => (
            QueryFailureReason::Cancelled,
            "Query execution was interrupted because the connection was killed".to_string(),
        ),
        QueryCancellationReason::ClientCancelRequested
        | QueryCancellationReason::ClientDisconnected => (
            QueryFailureReason::Cancelled,
            "Query execution was interrupted because the client disconnected".to_string(),
        ),
        QueryCancellationReason::ServerShutdown => (
            QueryFailureReason::Cancelled,
            "Query execution was interrupted because the server is shutting down".to_string(),
        ),
    };
    ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating(&message),
        ExecutionFailureSite::Coordinator,
    )
}

pub fn governed_execution_error(
    error: QueryExecutionError,
    completion: GovernedStatementFinishOutcome,
) -> QueryServiceError {
    // Statement completion has already settled its own registration. Its
    // cancellation or protocol outcome cannot reclassify a selected cause.
    if error.execution_failure().is_some() {
        return governed_query_execution_error(error);
    }
    match completion {
        GovernedStatementFinishOutcome::Cancelled(reason) => governed_cancellation_error(reason),
        GovernedStatementFinishOutcome::Completed | GovernedStatementFinishOutcome::Stale => {
            governed_query_execution_error(error)
        }
        GovernedStatementFinishOutcome::ProtocolFailed => internal_error(format!(
            "query protocol ownership failed while handling execution error: {error}"
        )),
    }
}

pub fn governed_query_execution_error(error: QueryExecutionError) -> QueryServiceError {
    if let Some(input) = error.execution_failure() {
        return QueryServiceError::from_execution_failure(input.clone());
    }
    let kind = match error.kind() {
        QueryExecutionErrorKind::Cancelled => QueryServiceErrorKind::Interrupted,
        QueryExecutionErrorKind::DeadlineExceeded => QueryServiceErrorKind::Timeout,
        QueryExecutionErrorKind::Rejected => QueryServiceErrorKind::Unavailable,
        QueryExecutionErrorKind::InvalidRequest | QueryExecutionErrorKind::Failed => {
            QueryServiceErrorKind::Internal
        }
    };
    QueryServiceError::new(kind, error.to_string())
}

pub fn governed_statement_begin_error(
    error: crate::session_control::GovernedQueryStatementBeginError,
) -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::Unavailable,
        format!("begin governed query statement failed: {error}"),
    )
}

pub fn scalar_query_error(message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::InvalidValue, message.into())
}

pub fn governed_cancellation_error(reason: WorkCancellationReason) -> QueryServiceError {
    QueryServiceError::from_execution_failure(governed_cancellation_failure_input(reason))
}

/// Keep workload-owned cancellation facts on the same neutral result boundary
/// as statement cancellation, including its original safe diagnostic.
pub fn governed_cancellation_failure_input(
    reason: WorkCancellationReason,
) -> ExecutionFailureInput {
    let (reason, message) = match reason {
        WorkCancellationReason::DeadlineExceeded => (
            QueryFailureReason::DeadlineExceeded,
            "query deadline exceeded".to_string(),
        ),
        WorkCancellationReason::FrontendDrainDeadlineExceeded => (
            QueryFailureReason::DeadlineExceeded,
            "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded".to_string(),
        ),
        WorkCancellationReason::ExplicitKill { .. } => (
            QueryFailureReason::Cancelled,
            "Query execution was interrupted".to_string(),
        ),
        WorkCancellationReason::ExplicitKillConnection { .. } => (
            QueryFailureReason::Cancelled,
            "Query execution was interrupted because the connection was killed".to_string(),
        ),
        WorkCancellationReason::ClientCancelRequested
        | WorkCancellationReason::ClientDisconnected => (
            QueryFailureReason::Cancelled,
            "Query execution was interrupted because the client disconnected".to_string(),
        ),
        WorkCancellationReason::ServerShutdown => (
            QueryFailureReason::Cancelled,
            "Query execution was interrupted because the server is shutting down".to_string(),
        ),
        WorkCancellationReason::Requested => (
            QueryFailureReason::Cancelled,
            "Query execution cancellation was requested".to_string(),
        ),
        WorkCancellationReason::OwnerDropped => (
            QueryFailureReason::Cancelled,
            "Query execution protocol owner was dropped".to_string(),
        ),
    };
    ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating(&message),
        ExecutionFailureSite::Coordinator,
    )
}

pub fn cancellation_requires_statement_fence(reason: &QueryCancellationReason) -> bool {
    matches!(reason, QueryCancellationReason::ExplicitKill { .. })
}

fn internal_error(message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_execution_error_keeps_resource_facts_site_and_descriptor() {
        use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite};
        use novarocks_execution_contract::{
            SafeDetail,
            failure::{
                FailureClass, FailureFact, FailureReason, ResourceDimension, ResourceFacts,
                ResourceScope,
            },
        };
        use std::num::NonZeroU16;
        let fact = FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(
                NonZeroU16::new(u16::MAX).unwrap(),
                NonZeroU16::new(7).unwrap(),
            ),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Driver),
            constraint_scope: Some(ResourceScope::Query),
            dimension: ResourceDimension::Bytes,
            requested: Some(19),
            required_growth: None,
            limit: Some(13),
            live: Some(11),
            committed: Some(12),
        });
        let input = ExecutionFailureInput::new(
            fact,
            SafeDetail::new("bounded allocation refusal").unwrap(),
            ExecutionFailureSite::Coordinator,
        );
        let service =
            governed_query_execution_error(QueryExecutionError::from_execution(input.clone()));
        assert_eq!(service.execution_failure(), Some(&input));
        assert_ne!(service.kind(), QueryServiceErrorKind::Parse);
        let user = service.user_error().unwrap();
        assert_eq!(user.phase(), novarocks_user_error::ErrorPhase::Execution);
        assert_eq!(user.code().as_str(), "query.execution.resource_exhausted");
        assert_eq!(user.location(), None);
        assert_eq!(user.message(), "bounded allocation refusal");
        assert_eq!(
            service.message(),
            "[query.execution.resource_exhausted] bounded allocation refusal"
        );
        let descriptor_only = QueryServiceError::from_user_error(user.clone());
        assert_eq!(descriptor_only.kind(), QueryServiceErrorKind::Internal);
        assert_eq!(descriptor_only.user_error(), Some(user));
        assert_eq!(
            descriptor_only.execution_failure(),
            None,
            "a public descriptor alone never manufactures a fact"
        );
    }

    fn assert_canonical_cancellation(
        error: QueryServiceError,
        reason: QueryFailureReason,
        kind: QueryServiceErrorKind,
        safe_message: &str,
    ) {
        let input = error
            .execution_failure()
            .expect("the Query owner supplies a canonical fact");
        assert_eq!(input.fact(), reason.failure_fact());
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(
            (input.descriptor().errno, input.descriptor().sqlstate),
            (1317, "70100")
        );
        assert_eq!(error.kind(), kind);
        let user = error.user_error().unwrap();
        assert_eq!(user.phase(), novarocks_user_error::ErrorPhase::Execution);
        assert_eq!(user.location(), None);
        assert_eq!(user.message(), safe_message);
        let code = match reason {
            QueryFailureReason::Cancelled => "query.execution.cancelled",
            QueryFailureReason::DeadlineExceeded => "query.execution.deadline_exceeded",
            _ => panic!("unexpected control projection reason: {reason:?}"),
        };
        assert_eq!(user.code().as_str(), code);
        assert_eq!(error.message(), format!("[{code}] {safe_message}"));
    }

    #[test]
    fn cancellation_errors_keep_timeout_distinct_from_interrupts() {
        assert_canonical_cancellation(
            cancellation_error(QueryCancellationReason::DeadlineExceeded { timeout_ms: 7 }),
            QueryFailureReason::DeadlineExceeded,
            QueryServiceErrorKind::Timeout,
            "query timed out after 7 ms",
        );
        assert_canonical_cancellation(
            governed_cancellation_error(WorkCancellationReason::DeadlineExceeded),
            QueryFailureReason::DeadlineExceeded,
            QueryServiceErrorKind::Timeout,
            "query deadline exceeded",
        );
        for error in [
            cancellation_error(QueryCancellationReason::ClientDisconnected),
            governed_cancellation_error(WorkCancellationReason::ClientDisconnected),
        ] {
            assert_canonical_cancellation(
                error,
                QueryFailureReason::Cancelled,
                QueryServiceErrorKind::Interrupted,
                "Query execution was interrupted because the client disconnected",
            );
        }
    }

    #[test]
    fn frontend_drain_deadlines_keep_their_safe_diagnostic_with_deadline_facts() {
        assert_canonical_cancellation(
            cancellation_error(QueryCancellationReason::FrontendDrainDeadlineExceeded {
                timeout_ms: 17,
            }),
            QueryFailureReason::DeadlineExceeded,
            QueryServiceErrorKind::Timeout,
            "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded after 17 ms",
        );
        assert_canonical_cancellation(
            governed_cancellation_error(WorkCancellationReason::FrontendDrainDeadlineExceeded),
            QueryFailureReason::DeadlineExceeded,
            QueryServiceErrorKind::Timeout,
            "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded",
        );
    }

    #[test]
    fn accepted_cancellation_helpers_keep_known_messages_without_requester_information() {
        let request_id = 1188237;
        for (query, work, message) in [
            (
                QueryCancellationReason::ExecutionCancellationRequested,
                WorkCancellationReason::Requested,
                "Query execution cancellation was requested",
            ),
            (
                QueryCancellationReason::ExecutionOwnerDropped,
                WorkCancellationReason::OwnerDropped,
                "Query execution protocol owner was dropped",
            ),
            (
                QueryCancellationReason::ExplicitKill {
                    requester_session: crate::session_control::ClientSessionKey::new(request_id)
                        .unwrap(),
                },
                WorkCancellationReason::ExplicitKill {
                    requester_session: request_id,
                },
                "Query execution was interrupted",
            ),
            (
                QueryCancellationReason::ExplicitKillConnection {
                    requester_session: crate::session_control::ClientSessionKey::new(request_id)
                        .unwrap(),
                },
                WorkCancellationReason::ExplicitKillConnection {
                    requester_session: request_id,
                },
                "Query execution was interrupted because the connection was killed",
            ),
            (
                QueryCancellationReason::ClientDisconnected,
                WorkCancellationReason::ClientDisconnected,
                "Query execution was interrupted because the client disconnected",
            ),
            (
                QueryCancellationReason::ServerShutdown,
                WorkCancellationReason::ServerShutdown,
                "Query execution was interrupted because the server is shutting down",
            ),
        ] {
            for error in [cancellation_error(query), governed_cancellation_error(work)] {
                assert!(!error.message().contains(&request_id.to_string()));
                assert_canonical_cancellation(
                    error,
                    QueryFailureReason::Cancelled,
                    QueryServiceErrorKind::Interrupted,
                    message,
                );
            }
        }
    }

    #[test]
    fn actual_cancellation_source_keeps_first_deadline_after_late_kill() {
        use crate::cancellation::{QueryCancellationRequestResult, QueryCancellationSource};
        let source = QueryCancellationSource::new();
        let view = source.view();
        let deadline = QueryCancellationReason::DeadlineExceeded { timeout_ms: 23 };
        assert_eq!(
            source.request(deadline.clone()),
            QueryCancellationRequestResult::Requested
        );
        let before = cancellation_error(view.reason().expect("the source accepted the deadline"));
        assert_eq!(
            source.request(QueryCancellationReason::ExplicitKill {
                requester_session: crate::session_control::ClientSessionKey::new(1188237)
                    .expect("nonzero fixture requester")
            }),
            QueryCancellationRequestResult::AlreadyRequested(deadline.clone())
        );
        assert_eq!(view.reason(), Some(deadline));
        let after = cancellation_error(view.reason().unwrap());
        assert_eq!(before.execution_failure(), after.execution_failure());
        for error in [before, after] {
            assert_canonical_cancellation(
                error,
                QueryFailureReason::DeadlineExceeded,
                QueryServiceErrorKind::Timeout,
                "query timed out after 23 ms",
            );
        }
    }

    #[test]
    fn owned_cancellation_inputs_match_service_and_execution_result_exits() {
        for reason in [
            QueryCancellationReason::ExecutionCancellationRequested,
            QueryCancellationReason::ExecutionOwnerDropped,
            QueryCancellationReason::ExplicitKill {
                requester_session: crate::session_control::ClientSessionKey::new(1188237)
                    .expect("nonzero fixture requester"),
            },
            QueryCancellationReason::ExplicitKillConnection {
                requester_session: crate::session_control::ClientSessionKey::new(1188237)
                    .expect("nonzero fixture requester"),
            },
            QueryCancellationReason::ClientDisconnected,
            QueryCancellationReason::ServerShutdown,
            QueryCancellationReason::DeadlineExceeded { timeout_ms: 23 },
            QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms: 17 },
        ] {
            let input = cancellation_failure_input(reason.clone());
            let service = cancellation_error(reason);
            assert_eq!(service.execution_failure(), Some(&input));
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                governed_query_execution_error(QueryExecutionError::from_execution(input)),
                service,
            );
        }
        for reason in [
            WorkCancellationReason::Requested,
            WorkCancellationReason::OwnerDropped,
            WorkCancellationReason::ExplicitKill {
                requester_session: 1188237,
            },
            WorkCancellationReason::ExplicitKillConnection {
                requester_session: 1188237,
            },
            WorkCancellationReason::ClientDisconnected,
            WorkCancellationReason::ServerShutdown,
            WorkCancellationReason::DeadlineExceeded,
            WorkCancellationReason::FrontendDrainDeadlineExceeded,
        ] {
            let input = governed_cancellation_failure_input(reason.clone());
            let service = governed_cancellation_error(reason);
            assert_eq!(service.execution_failure(), Some(&input));
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                governed_query_execution_error(QueryExecutionError::from_execution(input)),
                service,
            );
        }
    }

    #[test]
    fn selected_task_and_root_inputs_survive_statement_completion_outcomes() {
        use novarocks_execution_contract::{TaskFailurePhase, TaskIdentity};
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(91, 9), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(2).unwrap(),
            TaskId::new(3).unwrap(),
            BackendProcessId::new_v7(),
        );
        let inputs = [
            ExecutionFailureInput::new(
                ConnectorError::new(ConnectorErrorKind::Internal, "private Task source")
                    .failure_fact(),
                SafeDetail::new("selected Task failed").unwrap(),
                ExecutionFailureSite::Task {
                    identity,
                    phase: TaskFailurePhase::Execution,
                },
            ),
            ExecutionFailureInput::new(
                QueryFailureReason::ResultProtocol.failure_fact(),
                SafeDetail::new("selected root reply was refused").unwrap(),
                ExecutionFailureSite::RootRead { root: identity },
            ),
        ];
        for input in inputs {
            let expected =
                governed_query_execution_error(QueryExecutionError::from_execution(input.clone()));
            for completion in [
                GovernedStatementFinishOutcome::Completed,
                GovernedStatementFinishOutcome::Stale,
                GovernedStatementFinishOutcome::ProtocolFailed,
                GovernedStatementFinishOutcome::Cancelled(WorkCancellationReason::ExplicitKill {
                    requester_session: 1188237,
                }),
                GovernedStatementFinishOutcome::Cancelled(
                    WorkCancellationReason::ExplicitKillConnection {
                        requester_session: 1188237,
                    },
                ),
                GovernedStatementFinishOutcome::Cancelled(
                    WorkCancellationReason::ClientDisconnected,
                ),
                GovernedStatementFinishOutcome::Cancelled(WorkCancellationReason::ServerShutdown),
                GovernedStatementFinishOutcome::Cancelled(WorkCancellationReason::DeadlineExceeded),
            ] {
                let service = governed_execution_error(
                    QueryExecutionError::from_execution(input.clone()),
                    completion,
                );
                assert_eq!(service, expected);
                assert_eq!(service.execution_failure(), Some(&input));
                assert_eq!(service.user_error(), Some(&input.user_error()));
                assert!(!service.message().contains("private"));
            }
        }
    }

    #[test]
    fn unselected_application_error_keeps_original_completion_refusals() {
        let application =
            QueryExecutionError::new(QueryExecutionErrorKind::Failed, "application start failed");
        for reason in [
            WorkCancellationReason::ExplicitKill {
                requester_session: 7,
            },
            WorkCancellationReason::ExplicitKillConnection {
                requester_session: 7,
            },
            WorkCancellationReason::ClientDisconnected,
            WorkCancellationReason::ServerShutdown,
            WorkCancellationReason::DeadlineExceeded,
        ] {
            assert_eq!(
                governed_execution_error(
                    application.clone(),
                    GovernedStatementFinishOutcome::Cancelled(reason.clone()),
                ),
                governed_cancellation_error(reason),
            );
        }
        let protocol =
            governed_execution_error(application, GovernedStatementFinishOutcome::ProtocolFailed);
        assert_eq!(protocol.kind(), QueryServiceErrorKind::Internal);
        assert!(protocol.execution_failure().is_none());
        assert_eq!(
            protocol.message(),
            "query protocol ownership failed while handling execution error: application start failed",
        );
    }

    #[test]
    fn only_kill_query_fences_the_successor_statement() {
        assert!(cancellation_requires_statement_fence(
            &QueryCancellationReason::ExplicitKill {
                requester_session: crate::session_control::ClientSessionKey::new(8)
                    .expect("nonzero fixture requester")
            }
        ));
        assert!(!cancellation_requires_statement_fence(
            &QueryCancellationReason::ClientDisconnected
        ));
    }
}
