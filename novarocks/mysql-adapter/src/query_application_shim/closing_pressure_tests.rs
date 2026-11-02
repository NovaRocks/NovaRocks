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

//! Actual registered TCP intermediary components. Synthetic unused Arm entries and
//! test producers do not prove sixty-four Native queries or native-root coexistence.
use super::*;
use crate::closing_pressure_gate::{ArmInput, ORIGINAL_SQL_SHA256, Phase, PressureOwner, ROW_CUT};
use arrow::datatypes::DataType;
use novarocks_query_application::{
    api::ResultField,
    protocol_delivery::StreamingStatementResult,
    query_control::QueryApplicationControl,
    session_control::{QueryControlService, QuerySessionLease, SessionIdentity},
    test_support::ResultStreamTestProducer,
};
use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
use novarocks_workload_control::{
    ResourceConfig, ResultCapacityConfig, ResultClosingCut, ResultWindowClass, WorkClass,
    WorkRequest, WorkloadConfig, WorkloadControl,
};
use sha2::{Digest, Sha256};
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

struct Session {
    connection: ClientConnectionToken,
    result: Mutex<Option<StreamingStatementResult>>,
    lease: QuerySessionLease,
}
struct Factory(Arc<Session>);
impl QuerySessionFactory for Factory {
    fn open_statement_session(
        &self,
        _: novarocks_query_application::session::StatementSessionRequest,
    ) -> Result<
        Arc<dyn QuerySession>,
        novarocks_query_application::session_value::StatementSessionError,
    > {
        unreachable!("this fixture exercises the connection session entry")
    }

    fn open_session(
        &self,
        request: QuerySessionOpenRequest,
    ) -> Result<Arc<dyn QuerySession>, QueryServiceError> {
        assert_eq!(request.connection_id(), self.0.connection.connection_id());
        Ok(self.0.clone())
    }
    fn cancel_all(&self, _: QueryCancellationReason) {}
}
#[async_trait::async_trait]
impl QuerySession for Session {
    fn connection_session(
        &self,
        connection: ClientConnectionToken,
    ) -> Option<novarocks_query_application::session_control::SessionToken> {
        (connection == self.connection).then_some(self.lease.token())
    }

    async fn init_database(
        &self,
        _: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        unreachable!("no init DB")
    }
    async fn execute_statement(
        &self,
        _: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        Ok(
            novarocks_query_application::session::QuerySessionStatement::output_owned(
                StatementResult::StreamingQuery(Box::new(
                    self.result
                        .lock()
                        .unwrap()
                        .take()
                        .expect("single original query"),
                )),
                novarocks_query_application::statement_kind::StatementKind::Query,
            ),
        )
    }
    async fn execute_batch(
        &self,
        sql: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        self.execute_statement(sql).await
    }
    fn cancel_current(&self, _: QueryCancellationReason) {}
    fn close(&self) {}
}
async fn packet(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0; 4];
    stream.read_exact(&mut header).await.unwrap();
    header[3] = 0;
    let length = u32::from_le_bytes(header) as usize;
    assert!(length < 4096);
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await.unwrap();
    bytes
}
async fn send(stream: &mut TcpStream, seq: u8, bytes: &[u8]) {
    let mut header = (bytes.len() as u32).to_le_bytes();
    header[3] = seq;
    stream.write_all(&header).await.unwrap();
    stream.write_all(bytes).await.unwrap();
}

const QUERY: &str = "SELECT REPEAT('x', 1048576) AS payload FROM generate_series(1, 1)";

async fn original_pressure_intermediary(pool_refusal: bool) {
    use novarocks_execution_contract::{
        TaskIdentity,
        root_result::{RootReadOutcome, RootResultData, RootResultReply},
    };
    use novarocks_result_contract::{RootOutputKind, RootProfileId, RootProfileV1};
    use novarocks_types::{BackendProcessId, StageId, TaskId};

    assert_eq!(
        <[u8; 32]>::from(Sha256::digest(QUERY.as_bytes())),
        ORIGINAL_SQL_SHA256
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let registry = MysqlClientConnectionRegistry::new();
    let registration = registry.register().unwrap();
    let connection = registration.token();
    let control = QueryControlService::new(Arc::new(QueryApplicationControl::default()));
    let lease = control
        .register_session(SessionIdentity::new(connection, "root"))
        .unwrap();
    let workload = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    let capacity = workload
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    workload.mark_ready().unwrap();
    let mut statement = control
        .begin_queued_governed_query_statement_with_result(
            lease.token(),
            &workload.root_admission(),
            None,
            None,
            None,
            ResultWindowClass::Client,
        )
        .await
        .unwrap();
    statement.take_execution_owner().unwrap().complete();
    let execution_id =
        QueryExecutionId::new(QueryId::new(772, 1), AttemptId::new(1).unwrap()).unwrap();
    // This producer owns its own test window. Only the protocol owner's Workload identity
    // and its real Closing grant are verified here; this is not a full Native inventory.
    let (producer, execution, _resources, _schema) = ResultStreamTestProducer::open(
        execution_id,
        vec![ResultField::new(
            "payload",
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
        )],
        2,
        ResourceConfig {
            total_bytes: 1024 * 1024,
            control_bytes: 1024,
            per_scope_bytes: 1024 * 1024 - 1024,
        },
    )
    .unwrap();
    let task = TaskIdentity::new(
        execution_id,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    let mut first = vec![b'x'; RootProfileV1::SEGMENT_BYTES];
    first[..4].copy_from_slice(&1_048_580_u32.to_le_bytes());
    first[4..8].copy_from_slice(&[0xfd, 0, 0, 16]);
    let replies = [first, vec![b'x'; 8]];
    let mut sequence = 0;
    let replies = replies.map(|body| {
        sequence += 1;
        let data = RootResultData::try_new(
            RootOutputKind::ClientRows,
            std::num::NonZeroU64::new(sequence).unwrap(),
            body.into(),
            None,
        )
        .unwrap();
        RootResultReply {
            root_task: task,
            profile: RootProfileId::V1,
            kind: RootOutputKind::ClientRows,
            accepted_consumed: 0, // Prefetch has generated no delivery receipt or ACK.
            outcome: RootReadOutcome::Data(data),
        }
    });
    let _receipt = producer
        .enqueue_resident_client_pair(replies)
        .await
        .unwrap();
    let result = StreamingStatementResult::try_from_execution(execution, statement).unwrap();
    assert!(result.is_observed_by(&workload.observation()));
    let foreign = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    assert!(!result.is_observed_by(&foreign.observation()));
    let (foreign_owner, foreign_controller) = PressureOwner::with_workload(
        novarocks_types::FrontendProcessId::new_v7(),
        std::time::Instant::now() + Duration::from_secs(4),
        Some(foreign.observation()),
    )
    .unwrap();
    assert!(foreign_owner.validate_streaming_owner(&result).is_err());
    assert_eq!(
        foreign_controller.snapshot(0).unwrap().failure,
        Some(crate::closing_pressure_gate::Failure::Identity)
    );
    let session = Arc::new(Session {
        connection,
        result: Mutex::new(Some(result)),
        lease,
    });
    let factory: Arc<dyn QuerySessionFactory> = Arc::new(Factory(session.clone()));
    let holder = workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let mut full = Vec::new();
    if pool_refusal {
        // Real pool exhaustion from component grants; these are not sixty-four queries.
        for _ in 0..64 {
            full.push(
                capacity
                    .try_acquire_closing(
                        &holder.owner.scope(),
                        ResultClosingCut::OriginatingFailure,
                    )
                    .unwrap(),
            );
        }
    }
    let observation = Arc::new(crate::listener::MysqlFixtureSessionJoins::for_closing_pressure());
    let permit = observation
        .reserve_watcher(registration.retain_owner())
        .unwrap();
    let frontend = novarocks_types::FrontendProcessId::new_v7();
    let (owner, mut controller) = PressureOwner::with_workload(
        frontend,
        std::time::Instant::now() + Duration::from_secs(4),
        Some(workload.observation()),
    )
    .unwrap();
    let owner = Arc::new(owner);
    let slot = if pool_refusal { 64 } else { 0 };
    let mut targets = std::array::from_fn(|i| ArmInput {
        handshake_connection_id: 1000 + i as u32,
        original_sql_sha256: ORIGINAL_SQL_SHA256,
    });
    targets[slot].handshake_connection_id = connection.connection_id();
    controller.arm_targets(frontend, targets).unwrap();
    let server = async {
        let (stream, peer) = listener.accept().await.unwrap();
        serve_registered_mysql_connection(
            "root".into(),
            "test".into(),
            factory,
            registration,
            stream,
            peer,
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            None,
            Some(permit),
            Some(observation.clone()),
            Some(owner.clone()),
        )
        .await;
        observation.abort_remaining_watchers();
        while observation.next_watcher().await.is_some() {}
        registry.wait_drained().await;
    };
    let client = async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        authenticate(&mut stream).await;
        send(&mut stream, 0, format!("\x03{QUERY}").as_bytes()).await;
        assert_eq!(packet(&mut stream).await, [1]);
        let _column = packet(&mut stream).await;
        assert_eq!(packet(&mut stream).await[0], 0xfe);
        let mut prefix = vec![0; ROW_CUT as usize];
        stream.read_exact(&mut prefix).await.unwrap();
        assert_eq!(&prefix[..8], &[4, 0, 16, 4, 0xfd, 0, 0, 16]);
        assert!(prefix[8..].iter().all(|byte| *byte == b'x'));
        if pool_refusal {
            let mut suffix = Vec::new();
            stream.read_to_end(&mut suffix).await.unwrap();
            assert!(
                suffix.is_empty(),
                "no ERR or continuation after original capacity refusal"
            );
        } else {
            let mut tail = [0; 9];
            stream.read_exact(&mut tail).await.unwrap();
            assert_eq!(tail, [b'x'; 9]);
            let error = packet(&mut stream).await;
            assert_eq!(error[0], 0xff);
            assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1317);
            send(&mut stream, 0, &[1]).await; // Original COM_QUIT on this same socket.
        }
    };
    let killer = async {
        loop {
            if controller.snapshot(slot).unwrap().rows_blocked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        control.cancel_session_statement(
            session.lease.token(),
            QueryCancellationReason::ExplicitKill {
                requester_session: session.lease.token().key(),
            },
        );
        if !pool_refusal {
            loop {
                let actual = controller.snapshot(slot).unwrap();
                if actual.phase == Phase::ClosingHeld && actual.paired_closing_polls > 0 {
                    assert!(actual.real_closing_observed && actual.writer_attached);
                    assert!(!actual.writer_destructor_returned);
                    assert!(actual.closing_write_blocked || actual.closing_flush_blocked);
                    assert_eq!(capacity.snapshot().held_positions, [0, 0, 0, 1]);
                    assert_eq!(
                        workload.observation().result_capacity_snapshot(),
                        capacity.snapshot()
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            controller.release(slot).unwrap();
        }
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(server, client, killer);
    })
    .await
    .unwrap();
    let facts = controller.snapshot(slot).unwrap();
    assert!(facts.failure.is_none());
    assert!(facts.writer_destructor_returned);
    assert_eq!(facts.accepted_prefix_bytes, ROW_CUT);
    let mut expected_prefix = vec![b'x'; ROW_CUT as usize];
    expected_prefix[..8].copy_from_slice(&[4, 0, 16, 4, 0xfd, 0, 0, 16]);
    assert_eq!(
        facts.accepted_prefix_sha256,
        <[u8; 32]>::from(Sha256::digest(&expected_prefix))
    );
    assert_eq!(observation.snapshot().protocol_io_failures, 0);
    assert_eq!(observation.watcher_snapshot().joined, 1);
    if pool_refusal {
        assert_eq!(facts.phase, Phase::CapacityRefused);
        assert!(facts.original_capacity_source_retained);
        assert_eq!(facts.paired_closing_polls, 0);
        assert_eq!(observation.snapshot().pressure_capacity_eofs, 1);
        let actual = observation.take_pressure_eof_after_join().unwrap();
        let typed =
            crate::closing_pressure_gate::relay::PressureCapacityEof::from_error(&actual.cause)
                .unwrap();
        assert!(typed.matches(connection));
        assert!(typed.matches_controller(&controller));
        assert!(
            !typed.matches(
                ClientConnectionToken::new(connection.connection_id(), connection.generation() + 1)
                    .unwrap()
            )
        );
        assert!(
            std::error::Error::source(typed)
                .unwrap()
                .downcast_ref::<io::Error>()
                .is_some()
        );
        // Neither text copying, wrong mode, Control classification nor replay waives a failure.
        let wrong_mode = crate::listener::MysqlFixtureSessionJoins::default();
        wrong_mode.observe_protocol_failure(
            connection,
            MysqlConnectionClass::Ordinary,
            actual.cause,
        );
        assert_eq!(wrong_mode.snapshot().protocol_io_failures, 1);
        let actual = wrong_mode.take_protocol_failure_after_join().unwrap();
        observation.observe_protocol_failure(
            connection,
            MysqlConnectionClass::Ordinary,
            io::Error::new(actual.cause.kind(), actual.cause.to_string()),
        );
        assert_eq!(observation.snapshot().protocol_io_failures, 1);
        let wrong_class = crate::listener::MysqlFixtureSessionJoins::for_closing_pressure();
        wrong_class.observe_protocol_failure(
            connection,
            MysqlConnectionClass::Control,
            actual.cause,
        );
        assert_eq!(wrong_class.snapshot().protocol_io_failures, 1);
        let actual = wrong_class.take_protocol_failure_after_join().unwrap();
        observation.observe_protocol_failure(
            connection,
            MysqlConnectionClass::Ordinary,
            actual.cause,
        );
        assert_eq!(observation.snapshot().protocol_io_failures, 2);
        assert_eq!(observation.snapshot().pressure_capacity_eofs, 1);
    } else {
        assert_eq!(facts.phase, Phase::Released);
        assert!(facts.paired_closing_polls > 0);
        assert_eq!(observation.snapshot().pressure_capacity_eofs, 0);
    }
    controller.stop();
    drop(full);
    drop(holder);
    drop(session);
    drop(producer);
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
}

#[tokio::test]
async fn original_pressure_tcp_writer_holds_real_closing_then_row_and_err1317() {
    original_pressure_intermediary(false).await;
}
#[tokio::test]
async fn original_pressure_tcp_capacity_refusal_keeps_opaque_source_and_no_extra_bytes() {
    original_pressure_intermediary(true).await;
}

async fn authenticate(stream: &mut TcpStream) {
    assert_eq!(packet(stream).await[0], 10);
    let flags = CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_PLUGIN_AUTH;
    let mut auth = Vec::new();
    auth.extend_from_slice(&flags.bits().to_le_bytes());
    auth.extend_from_slice(&(64_u32 * 1024 * 1024).to_le_bytes());
    auth.push(33);
    auth.extend_from_slice(&[0; 23]);
    auth.extend_from_slice(b"root\0");
    auth.push(0);
    auth.extend_from_slice(b"mysql_native_password\0");
    send(stream, 1, &auth).await;
    assert_eq!(packet(stream).await[0], 0);
}

struct ErrorSession(Mutex<Option<QueryServiceError>>);
struct ErrorFactory(Arc<ErrorSession>);
impl QuerySessionFactory for ErrorFactory {
    fn open_statement_session(
        &self,
        _: novarocks_query_application::session::StatementSessionRequest,
    ) -> Result<
        Arc<dyn QuerySession>,
        novarocks_query_application::session_value::StatementSessionError,
    > {
        unreachable!("this fixture exercises the connection session entry")
    }

    fn open_session(
        &self,
        _: QuerySessionOpenRequest,
    ) -> Result<Arc<dyn QuerySession>, QueryServiceError> {
        Ok(self.0.clone())
    }
    fn cancel_all(&self, _: QueryCancellationReason) {}
}
#[async_trait::async_trait]
impl QuerySession for ErrorSession {
    fn connection_session(
        &self,
        _: ClientConnectionToken,
    ) -> Option<novarocks_query_application::session_control::SessionToken> {
        None
    }

    async fn init_database(
        &self,
        _: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        unreachable!()
    }
    async fn execute_statement(
        &self,
        _: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        Err(self.0.lock().unwrap().take().unwrap())
    }
    async fn execute_batch(
        &self,
        sql: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        self.execute_statement(sql).await
    }
    fn cancel_current(&self, _: QueryCancellationReason) {}
    fn close(&self) {}
}

#[tokio::test]
async fn selected_original_tcp_query_failure_retains_actual_query_error_and_guard() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let registry = MysqlClientConnectionRegistry::new();
    let registration = registry.register().unwrap();
    let connection = registration.token();
    let original = QueryServiceError::from_user_error(
        novarocks_parser::parse("SHOW")
            .unwrap_err()
            .to_user_error("SHOW"),
    );
    let message_pointer = original.message().as_ptr();
    let expected = original.clone();
    let factory: Arc<dyn QuerySessionFactory> = Arc::new(ErrorFactory(Arc::new(ErrorSession(
        Mutex::new(Some(original)),
    ))));
    let frontend = novarocks_types::FrontendProcessId::new_v7();
    let (owner, mut controller) =
        PressureOwner::new(frontend, std::time::Instant::now() + Duration::from_secs(3)).unwrap();
    let owner = Arc::new(owner);
    let mut targets = std::array::from_fn(|i| ArmInput {
        handshake_connection_id: 1000 + i as u32,
        original_sql_sha256: ORIGINAL_SQL_SHA256,
    });
    targets[0].handshake_connection_id = connection.connection_id();
    controller.arm_targets(frontend, targets).unwrap();
    let observation = Arc::new(crate::listener::MysqlFixtureSessionJoins::for_closing_pressure());
    let permit = observation
        .reserve_watcher(registration.retain_owner())
        .unwrap();
    let server = async {
        let (stream, peer) = listener.accept().await.unwrap();
        serve_registered_mysql_connection(
            "root".into(),
            "test".into(),
            factory,
            registration,
            stream,
            peer,
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            None,
            Some(permit),
            Some(observation.clone()),
            Some(owner),
        )
        .await;
        observation.abort_remaining_watchers();
        while observation.next_watcher().await.is_some() {}
        registry.wait_drained().await;
    };
    let client = async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        authenticate(&mut stream).await;
        send(&mut stream, 0, format!("\x03{QUERY}").as_bytes()).await;
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        assert!(
            bytes.is_empty(),
            "failed selected query emits no synthetic result"
        );
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(server, client);
    })
    .await
    .unwrap();
    assert_eq!(observation.snapshot().protocol_io_failures, 1);
    assert_eq!(observation.snapshot().pressure_capacity_eofs, 0);
    assert_eq!(observation.watcher_snapshot().joined, 1);
    let actual = observation.take_protocol_failure_after_join().unwrap();
    let retained = actual
        .cause
        .get_ref()
        .unwrap()
        .downcast_ref::<PressureQueryRejection>()
        .unwrap();
    assert_eq!(retained.original, expected);
    assert_eq!(
        retained.original.message().as_ptr(),
        message_pointer,
        "original String allocation was moved, not reformatted"
    );
    assert!(
        std::error::Error::source(retained)
            .unwrap()
            .downcast_ref::<QueryServiceError>()
            .is_some()
    );
    assert_eq!(retained.guard.kind(), io::ErrorKind::InvalidData);
    assert!(!actual.cause.to_string().contains("SHOW"));
    assert_eq!(
        controller.snapshot(0).unwrap().failure,
        Some(crate::closing_pressure_gate::Failure::Transition)
    );
    controller.stop();
}

#[derive(Debug)]
struct UnformattableIo(Arc<()>);
impl std::fmt::Display for UnformattableIo {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("unknown source formatter must never be executed")
    }
}
impl std::error::Error for UnformattableIo {}

#[test]
fn pressure_protocol_observation_moves_original_io_without_executing_source_formatter() {
    let observation = crate::listener::MysqlFixtureSessionJoins::for_closing_pressure();
    let identity = Arc::new(());
    let original = io::Error::new(io::ErrorKind::Other, UnformattableIo(identity.clone()));
    let connection = ClientConnectionToken::new(1, 71).unwrap();
    tracing::subscriber::with_default(FiniteLogSubscriber, || {
        observe_pressure_protocol_failure(
            &observation,
            connection,
            MysqlConnectionClass::Ordinary,
            "127.0.0.1:1".parse().unwrap(),
            original,
        );
    });
    assert_eq!(observation.snapshot().protocol_io_failures, 1);
    assert_eq!(observation.snapshot().pressure_capacity_eofs, 0);
    let retained = observation.take_protocol_failure_after_join().unwrap();
    assert!(Arc::ptr_eq(
        &retained
            .cause
            .get_ref()
            .unwrap()
            .downcast_ref::<UnformattableIo>()
            .unwrap()
            .0,
        &identity
    ));
    assert_eq!(retained.connection, connection);
}

#[tokio::test]
async fn pressure_original_listener_bind_failure_survives_until_actual_join_and_wakes_observer() {
    use crate::closing_pressure_fixture::ClosingPressureFixture;
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let workload = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    let mut fixture = ClosingPressureFixture::new(
        novarocks_types::FrontendProcessId::new_v7(),
        std::time::Instant::now() + Duration::from_secs(3),
        workload.observation(),
    )
    .unwrap();
    let observed = fixture.failure_observation();
    let binding = fixture.listener_binding().unwrap();
    assert!(fixture.listener_binding().is_err());
    let connections = Arc::new(MysqlClientConnectionRegistry::new());
    let actual = serve_query_application_mysql_until_drain_then_shutdown_pressure(
        crate::ResolvedMysqlListenerSettings::new(address, "root"),
        "test".into(),
        Arc::new(ErrorFactory(Arc::new(ErrorSession(Mutex::new(None))))),
        connections.clone(),
        std::future::pending::<()>(),
        async {},
        Duration::from_secs(1),
        |_| panic!("occupied listener cannot be ready"),
        binding,
    )
    .await;
    assert!(actual.is_err()); // Actual listener future returned; no Drop/join substitution.
    tokio::time::timeout(Duration::from_secs(1), observed.wait_for_failure())
        .await
        .unwrap();
    fixture
        .verify_original_connection_drain(&connections)
        .await
        .unwrap();
    fixture.stop();
    let failure = fixture.finish_after_original_listener_join().unwrap_err();
    let source = std::error::Error::source(&failure)
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert_eq!(source.kind(), io::ErrorKind::AddrInUse);
    assert!(source.raw_os_error().is_some());
}

#[tokio::test]
async fn pressure_single_drain_poll_refuses_live_original_registration_without_second_wait() {
    let connections = MysqlClientConnectionRegistry::new();
    let live = connections.register().unwrap();
    let workload = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    let fixture = crate::closing_pressure_fixture::ClosingPressureFixture::new(
        novarocks_types::FrontendProcessId::new_v7(),
        std::time::Instant::now() + Duration::from_secs(3),
        workload.observation(),
    )
    .unwrap();
    assert!(
        fixture
            .verify_original_connection_drain(&connections)
            .await
            .is_err()
    );
    drop(live);
    fixture
        .verify_original_connection_drain(&connections)
        .await
        .unwrap();
}

#[test]
fn real_capacity_count_without_original_closing_writers_cannot_prove_joint_pressure() {
    let workload = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    let capacity = workload
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    workload.mark_ready().unwrap();
    let root = workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let grants: Vec<_> = (0..64)
        .map(|_| {
            capacity
                .try_acquire_closing(&root.owner.scope(), ResultClosingCut::OriginatingFailure)
                .unwrap()
        })
        .collect();
    let mut fixture = crate::closing_pressure_fixture::ClosingPressureFixture::new(
        novarocks_types::FrontendProcessId::new_v7(),
        std::time::Instant::now() + Duration::from_secs(3),
        workload.observation(),
    )
    .unwrap();
    assert_eq!(capacity.snapshot().held_positions[3], 64);
    assert!(fixture.joint_closing_snapshot(false).is_err());
    assert_eq!(
        fixture.snapshot(0).unwrap().failure,
        Some(crate::closing_pressure_gate::Failure::Transition)
    );
    fixture.stop();
    drop(grants);
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
}

// Enable tracing and force all event fields to render, so an accidental err={} is exercised.
struct FiniteLogSubscriber;
impl tracing::Subscriber for FiniteLogSubscriber {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Render;
        impl tracing::field::Visit for Render {
            fn record_debug(&mut self, _: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let _ = format!("{value:?}");
            }
        }
        event.record(&mut Render);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test]
async fn component_resident_pair_rejects_piggyback_end_and_fabricated_consumption_frontier() {
    use novarocks_execution_contract::{
        TaskIdentity,
        root_result::{RootReadOutcome, RootResultData, RootResultEnd, RootResultReply},
    };
    use novarocks_result_contract::{RootOutputKind, RootProfileId};
    use novarocks_types::{BackendProcessId, StageId, TaskId};
    let execution_id =
        QueryExecutionId::new(QueryId::new(773, 1), AttemptId::new(1).unwrap()).unwrap();
    let (producer, _execution, _resources, _schema) = ResultStreamTestProducer::open(
        execution_id,
        vec![ResultField::new(
            "payload",
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
        )],
        1,
        ResourceConfig {
            total_bytes: 1024 * 1024,
            control_bytes: 1024,
            per_scope_bytes: 1024 * 1024 - 1024,
        },
    )
    .unwrap();
    let task = TaskIdentity::new(
        execution_id,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    for (first_end, next_end, next_frontier) in
        [(true, false, 0), (false, true, 0), (false, false, 1)]
    {
        let replies = std::array::from_fn(|index| {
            let sequence = std::num::NonZeroU64::new(index as u64 + 1).unwrap();
            let end = if [first_end, next_end][index] {
                Some(RootResultEnd {
                    sequence: std::num::NonZeroU64::new(sequence.get() + 1).unwrap(),
                    output_rows: index as u64 + 1,
                })
            } else {
                None
            };
            RootResultReply {
                root_task: task,
                profile: RootProfileId::V1,
                kind: RootOutputKind::ClientRows,
                accepted_consumed: if index == 0 { 0 } else { next_frontier },
                outcome: RootReadOutcome::Data(
                    RootResultData::try_new(
                        RootOutputKind::ClientRows,
                        sequence,
                        vec![4, 0, 0, 0, 3, b'a', b'b', b'c'].into(),
                        end,
                    )
                    .unwrap(),
                ),
            }
        });
        let source = match producer.enqueue_resident_client_pair(replies).await {
            Err(source) => source,
            Ok(_) => panic!("invalid component pair cannot enter original delivery queue"),
        };
        assert_eq!(
            source.kind(),
            novarocks_query_application::api::QueryExecutionErrorKind::InvalidRequest
        );
    }
}
