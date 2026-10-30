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

//! Governed Query Application result delivery over the MySQL wire.

use std::io;

use novarocks_query_application::api::{
    QueryExecutionError, QueryExecutionErrorKind, ResultFailureView,
};
use novarocks_query_application::cancellation::{QueryCancellationReason, QueryCancellationView};
use novarocks_query_application::protocol_delivery::{
    GovernedImmediateStatementResult, StreamingStatementResult,
};
use opensrv_mysql::QueryResultWriter;
use tokio::io::AsyncWrite;

pub enum MysqlStatementWriteOutcome<'writer, W: AsyncWrite + Unpin> {
    Continue(QueryResultWriter<'writer, W>),
    Terminated,
}

pub async fn write_governed_query_result<W: AsyncWrite + Unpin>(
    result: GovernedImmediateStatementResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    match write_governed_query_result_one(result, results).await? {
        MysqlStatementWriteOutcome::Continue(results) => results.no_more_results().await,
        MysqlStatementWriteOutcome::Terminated => Ok(()),
    }
}

pub async fn write_governed_query_result_one<'writer, W: AsyncWrite + Unpin>(
    result: GovernedImmediateStatementResult,
    results: QueryResultWriter<'writer, W>,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    crate::local_result_writer::write_local_result_one(result, results, false).await
}

/// Writes one Query Application result without detaching its delivery and
/// resource owners from the protocol operation which consumes them.
pub async fn write_streaming_query_result<W: AsyncWrite + Unpin>(
    result: StreamingStatementResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    match write_streaming_query_result_one(result, results).await? {
        MysqlStatementWriteOutcome::Continue(results) => results.no_more_results().await,
        MysqlStatementWriteOutcome::Terminated => Ok(()),
    }
}

pub async fn write_streaming_query_result_one<'writer, W: AsyncWrite + Unpin>(
    result: StreamingStatementResult,
    results: QueryResultWriter<'writer, W>,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    write_streaming_query_result_with_more(result, results, false).await
}

pub(crate) async fn write_streaming_query_result_with_more<'writer, W: AsyncWrite + Unpin>(
    result: StreamingStatementResult,
    results: QueryResultWriter<'writer, W>,
    more_results: bool,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    write_streaming_query_result_kernel(
        result,
        results,
        more_results,
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        None,
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        None,
    )
    .await
}

#[cfg(feature = "mem-1-m07-exact-mysql-write")]
pub(crate) async fn write_streaming_query_result_with_gate<W: AsyncWrite + Unpin>(
    result: StreamingStatementResult,
    results: QueryResultWriter<'_, W>,
    hook: Option<crate::mysql_write_gate::late_binding::MysqlWriteRelayHook>,
) -> io::Result<()> {
    match write_streaming_query_result_kernel(
        result,
        results,
        false,
        hook,
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        None,
    )
    .await?
    {
        MysqlStatementWriteOutcome::Continue(results) => results.no_more_results().await,
        MysqlStatementWriteOutcome::Terminated => Ok(()),
    }
}

#[cfg(feature = "mem-1-m07-closing-pressure")]
pub(crate) async fn write_streaming_query_result_with_pressure<W: AsyncWrite + Unpin>(
    result: StreamingStatementResult,
    results: QueryResultWriter<'_, W>,
    pressure_hook: Option<crate::closing_pressure_gate::relay::PressureRelayHook>,
) -> io::Result<()> {
    match write_streaming_query_result_kernel(
        result,
        results,
        false,
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        None,
        pressure_hook,
    )
    .await?
    {
        MysqlStatementWriteOutcome::Continue(results) => results.no_more_results().await,
        MysqlStatementWriteOutcome::Terminated => Ok(()),
    }
}

async fn write_streaming_query_result_kernel<'writer, W: AsyncWrite + Unpin>(
    mut result: StreamingStatementResult,
    results: QueryResultWriter<'writer, W>,
    more_results: bool,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")] hook: Option<
        crate::mysql_write_gate::late_binding::MysqlWriteRelayHook,
    >,
    #[cfg(feature = "mem-1-m07-closing-pressure")] pressure_hook: Option<
        crate::closing_pressure_gate::relay::PressureRelayHook,
    >,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    let schema_delivery = match result.begin_schema() {
        Some(delivery) => delivery,
        None => {
            let _ = result.fail();
            return Err(invalid_data_error(
                "streaming query result has no schema delivery".to_string(),
            ));
        }
    };
    crate::relay_result_writer::write_relay_result_one(
        result,
        schema_delivery,
        results,
        more_results,
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        hook,
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        pressure_hook,
    )
    .await
}

pub(crate) fn is_terminal_cancellation(error: &QueryExecutionError) -> bool {
    matches!(
        error.kind(),
        QueryExecutionErrorKind::Cancelled | QueryExecutionErrorKind::DeadlineExceeded
    )
}

pub(crate) async fn wait_terminal_result_failure(
    failure: &mut ResultFailureView,
    cancellation: &QueryCancellationView,
) -> QueryExecutionError {
    tokio::select! {
        biased;
        error = failure.wait() => normalize_terminal_cancellation(error, cancellation.reason()),
        reason = cancellation.cancelled() => cancelled_query_result_delivery(reason),
    }
}

fn normalize_terminal_cancellation(
    error: QueryExecutionError,
    reason: Option<QueryCancellationReason>,
) -> QueryExecutionError {
    // An already selected execution input is authoritative at the renderer.
    // A local cancellation view cannot replace its fact, site or safe detail.
    if error.execution_failure().is_some() {
        return error;
    }
    match error.kind() {
        QueryExecutionErrorKind::DeadlineExceeded => match reason {
            Some(
                reason @ (QueryCancellationReason::DeadlineExceeded { .. }
                | QueryCancellationReason::FrontendDrainDeadlineExceeded { .. }),
            ) => cancelled_query_result_delivery(reason),
            // A legacy actor kind alone is not an accepted Query deadline;
            // preserve it rather than borrowing a different late cancellation.
            _ => QueryExecutionError::new(
                QueryExecutionErrorKind::DeadlineExceeded,
                "query timed out",
            ),
        },
        QueryExecutionErrorKind::Cancelled => match reason {
            Some(reason) => cancelled_query_result_delivery(reason),
            None => error,
        },
        _ => error,
    }
}

pub(crate) fn cancelled_query_result_delivery(
    reason: QueryCancellationReason,
) -> QueryExecutionError {
    QueryExecutionError::from_execution(
        novarocks_query_application::session_outcome::cancellation_failure_input(reason),
    )
}

pub(crate) fn governed_cancelled_query_result_delivery(
    reason: novarocks_workload_control::CancellationReason,
) -> QueryExecutionError {
    QueryExecutionError::from_execution(
        novarocks_query_application::session_outcome::governed_cancellation_failure_input(reason),
    )
}

fn invalid_data_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
pub(crate) mod streaming_result_tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use novarocks_query_application::api::{
        QueryExecutionError, QueryExecutionErrorKind, ResultDelivery, ResultField,
    };
    use novarocks_query_application::test_support::{
        ResultStreamTestProducer, TestResultDeliveryDisposition,
    };
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
    use novarocks_workload_control::{
        CancellationReason, ResourceConfig, WorkloadConfig, WorkloadControl,
    };

    use super::*;
    use novarocks_query_application::client_connection::ClientConnectionToken;
    use novarocks_query_application::protocol_delivery::StreamingStatementResult;
    use novarocks_query_application::query_control::QueryApplicationControl;
    use novarocks_query_application::session_control::{
        QueryCancelOutcome, QueryControlPort, QueryControlService, QuerySessionLease,
        SessionIdentity,
    };

    pub(crate) struct Fixture {
        pub(crate) producer: ResultStreamTestProducer,
        pub(crate) result: Option<StreamingStatementResult>,
        capacity: novarocks_workload_control::ResultCapacityHandle,
        pub(crate) protocol_capacity: novarocks_workload_control::ResultCapacityHandle,
        _governance: WorkloadControl,
        control: QueryControlService,
        session: QuerySessionLease,
        schema_receipt: novarocks_query_application::test_support::TestResultDeliveryReceipt,
    }

    fn fixture(fields: Vec<ResultField>) -> Fixture {
        fixture_with_timeout(fields, None)
    }

    fn fixture_with_timeout(fields: Vec<ResultField>, timeout_ms: Option<u64>) -> Fixture {
        fixture_with_admission_deadline(fields, None, timeout_ms)
    }

    pub(crate) fn fixture_with_admission_deadline(
        fields: Vec<ResultField>,
        deadline: Option<tokio::time::Instant>,
        timeout_ms: Option<u64>,
    ) -> Fixture {
        let execution_id = QueryExecutionId::new(
            QueryId::new(71, 1),
            AttemptId::new(1).expect("test attempt"),
        )
        .expect("test execution");
        let (producer, execution, _resources, schema_receipt) = ResultStreamTestProducer::open(
            execution_id,
            fields,
            1,
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .expect("open result stream");

        let port: Arc<dyn QueryControlPort> = Arc::new(QueryApplicationControl::default());
        let control = QueryControlService::new(port);
        let session = control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(91, 1).expect("connection token"),
                "root",
            ))
            .expect("register session");
        let governance = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .expect("test statement workload");
        // The protocol statement has its own host. The producer host's profile
        // cannot authorize Closing on this statement's exact scope.
        let protocol_capacity = governance
            .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
            .expect("install the protocol host's complete result and Closing profile");
        governance.mark_ready().expect("statement workload ready");
        let mut statement = control
            .begin_governed_query_statement(
                session.token(),
                &governance.root_admission(),
                deadline,
                timeout_ms,
                None,
            )
            .expect("begin governed statement");
        statement
            .take_execution_owner()
            .expect("test transfers execution owner")
            .complete();
        let result = StreamingStatementResult::try_from_execution(execution, statement)
            .expect("bind streaming statement");
        Fixture {
            result: Some(result),
            capacity: producer.result_capacity(),
            protocol_capacity,
            producer,
            _governance: governance,
            control,
            session,
            schema_receipt,
        }
    }

    pub(crate) fn string_fields() -> Vec<ResultField> {
        vec![ResultField::new(
            "value",
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
        )]
    }

    pub(crate) fn text_body(value: &str) -> Vec<u8> {
        assert!(value.len() < 251);
        let mut body = Vec::new();
        body.extend_from_slice(&((value.len() + 1) as u32).to_le_bytes());
        body.push(value.len() as u8);
        body.extend_from_slice(value.as_bytes());
        body
    }

    #[test]
    fn selected_execution_input_survives_late_cancel_and_deadline_normalization() {
        use novarocks_execution_contract::{
            SafeDetail,
            failure::{FailureClass, FailureFact, FailureProjection, FailureReason},
        };
        use novarocks_query_application::{
            api::{ExecutionFailureInput, ExecutionFailureSite},
            execution_error::QueryFailureReason,
        };
        use std::num::NonZeroU16;
        let facts = [
            FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(
                    NonZeroU16::new(u16::MAX).unwrap(),
                    NonZeroU16::new(7).unwrap(),
                ),
            ),
            QueryFailureReason::Cancelled.failure_fact(),
            QueryFailureReason::DeadlineExceeded.failure_fact(),
        ];
        for fact in facts {
            let input = ExecutionFailureInput::new(
                fact,
                SafeDetail::new("original accepted diagnostic").unwrap(),
                ExecutionFailureSite::Coordinator,
            );
            let original = QueryExecutionError::from_execution(input.clone());
            let rendered = normalize_terminal_cancellation(
                original.clone(),
                Some(QueryCancellationReason::ClientDisconnected),
            );
            assert_eq!(rendered, original);
            assert_eq!(rendered.execution_failure(), Some(&input));
            let payload = crate::relay_result_writer::error_payload(&rendered, 4096);
            assert!(
                std::str::from_utf8(&payload[9..])
                    .unwrap()
                    .ends_with("original accepted diagnostic")
            );
            let service =
                novarocks_query_application::session_outcome::governed_query_execution_error(
                    rendered,
                );
            assert_eq!(service.execution_failure(), Some(&input));
        }
    }

    #[test]
    fn deadline_delivery_cancellation_keeps_the_statement_timeout_message() {
        let error = cancelled_query_result_delivery(QueryCancellationReason::DeadlineExceeded {
            timeout_ms: 1_000,
        });
        assert_eq!(error.kind(), QueryExecutionErrorKind::DeadlineExceeded);
        assert_eq!(error.to_string(), "query timed out after 1000 ms");
    }

    #[test]
    fn accepted_query_and_work_cancellation_packets_keep_domain_fact_and_wire_contract() {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_query_application::{
            api::ExecutionFailureSite, execution_error::QueryFailureReason,
        };
        use novarocks_user_error::ErrorPhase;
        let cases = [
            (
                cancelled_query_result_delivery(QueryCancellationReason::ExplicitKill {
                    requester_session:
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            3_090_001,
                        )
                        .expect("nonzero fixture requester"),
                }),
                QueryFailureReason::Cancelled,
                QueryExecutionErrorKind::Cancelled,
                "query.execution.cancelled",
                "Query execution was interrupted",
            ),
            (
                cancelled_query_result_delivery(QueryCancellationReason::DeadlineExceeded {
                    timeout_ms: 1_000,
                }),
                QueryFailureReason::DeadlineExceeded,
                QueryExecutionErrorKind::DeadlineExceeded,
                "query.execution.deadline_exceeded",
                "query timed out after 1000 ms",
            ),
            (
                cancelled_query_result_delivery(
                    QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms: 2_000 },
                ),
                QueryFailureReason::DeadlineExceeded,
                QueryExecutionErrorKind::DeadlineExceeded,
                "query.execution.deadline_exceeded",
                "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded after 2000 ms",
            ),
            (
                governed_cancelled_query_result_delivery(CancellationReason::ExplicitKill {
                    requester_session: 3_090_001,
                }),
                QueryFailureReason::Cancelled,
                QueryExecutionErrorKind::Cancelled,
                "query.execution.cancelled",
                "Query execution was interrupted",
            ),
            (
                governed_cancelled_query_result_delivery(CancellationReason::DeadlineExceeded),
                QueryFailureReason::DeadlineExceeded,
                QueryExecutionErrorKind::DeadlineExceeded,
                "query.execution.deadline_exceeded",
                "query deadline exceeded",
            ),
            (
                governed_cancelled_query_result_delivery(
                    CancellationReason::FrontendDrainDeadlineExceeded,
                ),
                QueryFailureReason::DeadlineExceeded,
                QueryExecutionErrorKind::DeadlineExceeded,
                "query.execution.deadline_exceeded",
                "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded",
            ),
        ];
        for (error, reason, kind, code, detail) in cases {
            let input = error
                .execution_failure()
                .expect("accepted cancellation has its domain input");
            assert_eq!(input.fact(), reason.failure_fact());
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(input.detail().as_str(), detail);
            assert_eq!(input.descriptor().descriptor.phase, ErrorPhase::Execution);
            assert_eq!(error.kind(), kind);
            let payload = crate::relay_result_writer::error_payload(&error, 4096);
            assert_eq!(payload[0], 0xff);
            assert_eq!(u16::from_le_bytes([payload[1], payload[2]]), 1317);
            assert_eq!(payload[3], b'#');
            assert_eq!(&payload[4..9], b"70100");
            assert_eq!(
                std::str::from_utf8(&payload[9..]).unwrap(),
                format!("[{code}] {detail}")
            );
            assert!(
                !std::str::from_utf8(&payload[9..])
                    .unwrap()
                    .contains("3090001")
            );
        }
    }

    #[test]
    fn observed_query_deadline_normalizes_only_its_accepted_deadline_reason() {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_query_application::execution_error::QueryFailureReason;
        let error = normalize_terminal_cancellation(
            QueryExecutionError::new(QueryExecutionErrorKind::DeadlineExceeded, "actor deadline"),
            Some(QueryCancellationReason::DeadlineExceeded { timeout_ms: 1_000 }),
        );
        assert_eq!(
            error.execution_failure().unwrap().fact(),
            QueryFailureReason::DeadlineExceeded.failure_fact()
        );
        let payload = crate::relay_result_writer::error_payload(&error, 4096);
        assert_eq!(u16::from_le_bytes([payload[1], payload[2]]), 1317);
        assert_eq!(&payload[4..9], b"70100");
        assert_eq!(
            &payload[9..],
            b"[query.execution.deadline_exceeded] query timed out after 1000 ms"
        );
    }

    #[test]
    fn actor_deadline_terminal_is_settled_as_a_mysql_timeout() {
        let error = normalize_terminal_cancellation(
            QueryExecutionError::new(
                QueryExecutionErrorKind::DeadlineExceeded,
                "logical execution deadline expired before success EOF",
            ),
            None,
        );

        assert!(is_terminal_cancellation(&error));
        assert_eq!(error.kind(), QueryExecutionErrorKind::DeadlineExceeded);
        assert_eq!(error.to_string(), "query timed out");
    }

    #[test]
    fn explicit_kill_delivery_cancellation_keeps_the_statement_message() {
        let error = cancelled_query_result_delivery(QueryCancellationReason::ExplicitKill {
            requester_session: novarocks_query_application::session_control::ClientSessionKey::new(
                7,
            )
            .expect("nonzero fixture requester"),
        });
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        assert_eq!(error.to_string(), "Query execution was interrupted");
    }

    async fn observe_ready_actor_failure(
        error: QueryExecutionError,
        reason: Option<QueryCancellationReason>,
    ) -> QueryExecutionError {
        let timeout_ms = match reason.as_ref() {
            Some(QueryCancellationReason::DeadlineExceeded { timeout_ms })
            | Some(QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms }) => {
                Some(*timeout_ms)
            }
            _ => None,
        };
        let mut fixture = fixture_with_timeout(string_fields(), timeout_ms);
        let cancellation = fixture.result.as_ref().unwrap().cancellation();
        let mut failure = fixture.result.as_ref().unwrap().failure_view().unwrap();
        if let Some(reason) = reason {
            assert!(matches!(
                fixture
                    .control
                    .cancel_session_statement(fixture.session.token(), reason),
                QueryCancelOutcome::Requested
            ));
        }
        // Both observations are ready before the production selector is polled.
        fixture.producer.fail(error);
        let observed = wait_terminal_result_failure(&mut failure, &cancellation).await;
        if is_terminal_cancellation(&observed) {
            fixture.result.take().unwrap().settle_cancellation();
        } else {
            fixture.result.take().unwrap().fail();
        }
        assert!(matches!(
            fixture.control.cancel_session_statement(
                fixture.session.token(),
                QueryCancellationReason::ClientDisconnected,
            ),
            QueryCancelOutcome::NoActiveStatement
        ));
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        observed
    }

    #[tokio::test]
    async fn ready_actor_cancellation_and_explicit_kill_use_the_statement_message() {
        let error = observe_ready_actor_failure(
            QueryExecutionError::new(
                QueryExecutionErrorKind::Cancelled,
                "logical execution was cancelled before success EOF",
            ),
            Some(QueryCancellationReason::ExplicitKill {
                requester_session:
                    novarocks_query_application::session_control::ClientSessionKey::new(7)
                        .expect("nonzero fixture requester"),
            }),
        )
        .await;
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        assert_eq!(error.to_string(), "Query execution was interrupted");
        // This path used the original QueryControl cancellation and completed
        // its real stream/schema/resource owners before encoding the same ERR.
        let payload = crate::relay_result_writer::error_payload(&error, 4096);
        assert_eq!(u16::from_le_bytes([payload[1], payload[2]]), 1317);
        assert_eq!(&payload[4..9], b"70100");
        assert_eq!(
            &payload[9..],
            b"[query.execution.cancelled] Query execution was interrupted"
        );
    }

    #[tokio::test]
    async fn ready_actor_cancellation_uses_each_exact_statement_reason() {
        for reason in [
            QueryCancellationReason::ExecutionCancellationRequested,
            QueryCancellationReason::ExecutionOwnerDropped,
            QueryCancellationReason::ExplicitKillConnection {
                requester_session:
                    novarocks_query_application::session_control::ClientSessionKey::new(7)
                        .expect("nonzero fixture requester"),
            },
            QueryCancellationReason::ClientDisconnected,
            QueryCancellationReason::DeadlineExceeded { timeout_ms: 1_000 },
            QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms: 2_000 },
            QueryCancellationReason::ServerShutdown,
        ] {
            let expected = cancelled_query_result_delivery(reason.clone());
            let error = observe_ready_actor_failure(
                QueryExecutionError::new(QueryExecutionErrorKind::Cancelled, "actor cancellation"),
                Some(reason),
            )
            .await;
            assert_eq!(error, expected);
        }
    }

    #[tokio::test]
    async fn actor_cancellation_without_a_statement_reason_is_not_invented() {
        let expected =
            QueryExecutionError::new(QueryExecutionErrorKind::Cancelled, "actor cancellation");
        assert_eq!(
            observe_ready_actor_failure(expected.clone(), None).await,
            expected
        );
    }

    #[tokio::test]
    async fn ready_actor_failures_are_not_retagged_by_a_statement_kill() {
        for kind in [
            QueryExecutionErrorKind::Failed,
            QueryExecutionErrorKind::InvalidRequest,
        ] {
            let expected = QueryExecutionError::new(kind, "original actor failure");
            let error = observe_ready_actor_failure(
                expected.clone(),
                Some(QueryCancellationReason::ExplicitKill {
                    requester_session:
                        novarocks_query_application::session_control::ClientSessionKey::new(7)
                            .expect("nonzero fixture requester"),
                }),
            )
            .await;
            assert_eq!(error, expected);
        }
    }

    #[tokio::test]
    async fn ready_actor_deadline_keeps_its_typed_origin_despite_a_statement_kill() {
        let error = observe_ready_actor_failure(
            QueryExecutionError::new(
                QueryExecutionErrorKind::DeadlineExceeded,
                "logical execution deadline expired before success EOF",
            ),
            Some(QueryCancellationReason::ExplicitKill {
                requester_session:
                    novarocks_query_application::session_control::ClientSessionKey::new(7)
                        .expect("nonzero fixture requester"),
            }),
        )
        .await;
        assert_eq!(error.kind(), QueryExecutionErrorKind::DeadlineExceeded);
        assert_eq!(error.to_string(), "query timed out");
    }

    #[tokio::test]
    async fn schema_batch_and_eof_complete_only_in_protocol_order() {
        let mut fixture = fixture(string_fields());
        let result = fixture.result.as_mut().unwrap();
        result.begin_schema().expect("schema").complete();
        assert_eq!(
            fixture.schema_receipt.wait().await,
            TestResultDeliveryDisposition::Completed
        );

        let batch_receipt = fixture
            .producer
            .enqueue_client_body(0, text_body("abc"), 1)
            .await
            .unwrap();
        let ResultDelivery::Segment(delivery) = result.next_delivery().await.unwrap().unwrap()
        else {
            panic!("expected batch")
        };
        assert_eq!(fixture.capacity.snapshot().held_positions, [1, 0, 0, 0]);
        delivery.complete();
        assert_eq!(
            batch_receipt.wait().await,
            TestResultDeliveryDisposition::Completed
        );
        assert_eq!(fixture.capacity.snapshot().held_positions, [1, 0, 0, 0]);

        let end_receipt = fixture.producer.enqueue_end(1).await;
        let ResultDelivery::End(end) = result.next_delivery().await.unwrap().unwrap() else {
            panic!("expected EOF")
        };
        end.complete();
        assert_eq!(
            end_receipt.wait().await,
            TestResultDeliveryDisposition::Completed
        );
        fixture.result.take().unwrap().complete();
        assert!(matches!(
            fixture.control.cancel_session_statement(
                fixture.session.token(),
                QueryCancellationReason::ClientDisconnected,
            ),
            QueryCancelOutcome::NoActiveStatement
        ));
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn slow_protocol_owner_retains_original_window_until_actual_exit() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let receipt = fixture
            .producer
            .enqueue_client_body(0, text_body("slow"), 1)
            .await
            .unwrap();
        let ResultDelivery::Segment(delivery) = fixture
            .result
            .as_mut()
            .unwrap()
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected batch")
        };
        tokio::task::yield_now().await;
        assert_eq!(fixture.capacity.snapshot().held_positions, [1, 0, 0, 0]);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), receipt.wait())
                .await
                .is_err()
        );
        delivery.complete();
        assert_eq!(fixture.capacity.snapshot().held_positions, [1, 0, 0, 0]);
        drop(fixture.result.take());
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn partial_protocol_failure_fails_receipt_under_original_window() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let receipt = fixture
            .producer
            .enqueue_client_body(0, text_body("partial"), 1)
            .await
            .unwrap();
        let ResultDelivery::Segment(delivery) = fixture
            .result
            .as_mut()
            .unwrap()
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected batch")
        };
        delivery.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "client disconnected after a partial row write",
        ));
        assert!(matches!(
            receipt.wait().await,
            TestResultDeliveryDisposition::Failed(_)
        ));
        assert_eq!(fixture.capacity.snapshot().held_positions, [1, 0, 0, 0]);
        fixture.result.take().unwrap().fail();
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn malformed_client_body_is_refused_before_protocol_delivery() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let error = fixture
            .producer
            .enqueue_client_body(0, vec![2, 0, 0, 0, b'a'], 1)
            .await;
        assert!(error.is_err(), "truncated row must fail before enqueue");
        fixture.result.take().unwrap().fail();
        assert_eq!(
            fixture.producer.cancellation_reason(),
            Some(CancellationReason::Requested)
        );
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn native_failure_view_interrupts_an_in_progress_protocol_owner() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let receipt = fixture
            .producer
            .enqueue_client_body(0, text_body("blocked client"), 1)
            .await
            .unwrap();
        let ResultDelivery::Segment(delivery) = fixture
            .result
            .as_mut()
            .unwrap()
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected batch")
        };
        let mut failure = fixture.result.as_ref().unwrap().failure_view().unwrap();
        let expected = QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "native attempt failed during client backpressure",
        );
        fixture.producer.fail(expected.clone());
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_millis(50), failure.wait())
                .await
                .expect("failure observation must not wait for another batch"),
            expected
        );
        delivery.fail(expected);
        fixture.result.take().unwrap().fail();
        assert!(matches!(
            receipt.wait().await,
            TestResultDeliveryDisposition::Failed(_)
        ));
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn owner_drop_cancels_execution_and_drops_undelivered_schema() {
        let mut fixture = fixture(string_fields());
        drop(fixture.result.take());
        assert_eq!(
            fixture.schema_receipt.wait().await,
            TestResultDeliveryDisposition::Dropped
        );
        assert_eq!(
            fixture.producer.cancellation_reason(),
            Some(CancellationReason::Requested)
        );
        assert!(matches!(
            fixture.control.cancel_session_statement(
                fixture.session.token(),
                QueryCancellationReason::ClientDisconnected,
            ),
            QueryCancelOutcome::NoActiveStatement
        ));
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn query_control_cancellation_remains_live_until_stream_owner_finishes() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        assert!(matches!(
            fixture.control.cancel_session_statement(
                fixture.session.token(),
                QueryCancellationReason::ClientDisconnected,
            ),
            QueryCancelOutcome::Requested
        ));
        assert_eq!(
            fixture.result.as_ref().unwrap().cancellation().reason(),
            Some(QueryCancellationReason::ClientDisconnected)
        );
        fixture.result.take().unwrap().fail();
        assert_eq!(
            fixture.producer.cancellation_reason(),
            Some(CancellationReason::Requested)
        );
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}
