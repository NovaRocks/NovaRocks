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

//! Component tests through the original registered TCP intermediary, not native evidence.
use super::*;
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

async fn original_intermediary_eof(pool_refusal: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let registry = MysqlClientConnectionRegistry::new();
    let registration = registry.register().unwrap();
    let connection = registration.token();
    let port: Arc<dyn novarocks_query_application::session_control::QueryControlPort> =
        Arc::new(QueryApplicationControl::default());
    let control = QueryControlService::new(port);
    let lease = control
        .register_session(SessionIdentity::new(connection, "root"))
        .unwrap();
    let workload = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1024 * 1024,
            control_bytes: 1024,
            per_scope_bytes: 1024 * 1024 - 1024,
        },
    )
    .unwrap();
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
        QueryExecutionId::new(QueryId::new(771, 1), AttemptId::new(1).unwrap()).unwrap();
    let (producer, execution, _resources, _schema) = ResultStreamTestProducer::open(
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
    let row = if pool_refusal {
        vec![4, 0, 0, 0, 3, b'a', b'b', b'c']
    } else {
        // Real V1 validator: the native u32 row length precedes a partial valid text row.
        let mut first = vec![b'x'; novarocks_result_contract::RootProfileV1::SEGMENT_BYTES];
        first[..4].copy_from_slice(&((3 * 1024 * 1024 + 4) as u32).to_le_bytes());
        first[4..8].copy_from_slice(&[0xfd, 0, 0, 0x30]);
        first
    };
    let _receipt = producer
        .enqueue_client_body(0, row, if pool_refusal { 1 } else { 0 })
        .await
        .unwrap();
    let result = StreamingStatementResult::try_from_execution(execution, statement).unwrap();
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
        for _ in 0..ResultCapacityConfig::V1.positions[3] {
            full.push(
                capacity
                    .try_acquire_closing(
                        &holder.owner.scope(),
                        ResultClosingCut::OriginatingFailure,
                    )
                    .unwrap(),
            );
        }
        assert_eq!(capacity.snapshot().held_positions[3], 64);
    }
    let observation = Arc::new(crate::listener::MysqlFixtureSessionJoins::default());
    let permit = observation
        .reserve_watcher(registration.retain_owner())
        .unwrap();
    let (hub, mut controller) = crate::mysql_write_gate::late_binding::MysqlWriteGateHub::new(
        novarocks_types::FrontendProcessId::new_v7(),
        [3; 16],
        std::time::Instant::now() + Duration::from_secs(3),
    )
    .unwrap();
    let cut = if pool_refusal { 2 } else { 8 };
    let query = "SELECT exact_component";
    controller
        .arm(
            controller.snapshot().frontend,
            [3; 16],
            connection.connection_id(),
            Sha256::digest(query.as_bytes()).into(),
            cut,
        )
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
            Some(hub.clone()),
            Some(permit),
            Some(observation.clone()),
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            None,
        )
        .await;
        observation.abort_remaining_watchers();
        while observation.next_watcher().await.is_some() {}
        registry.wait_drained().await;
    };
    let client = async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        assert_eq!(packet(&mut stream).await[0], 10);
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
        send(&mut stream, 1, &auth).await;
        assert_eq!(packet(&mut stream).await[0], 0);
        let command = format!("\x03{query}");
        send(&mut stream, 0, command.as_bytes()).await;
        assert_eq!(packet(&mut stream).await, [1]);
        let _column = packet(&mut stream).await;
        assert_eq!(packet(&mut stream).await[0], 0xfe);
        let mut partial = Vec::new();
        stream.read_to_end(&mut partial).await.unwrap();
        assert_eq!(
            partial.len(),
            cut as usize,
            "no ERR or next response after prescribed EOF"
        );
        assert_eq!(partial[0], 4);
    };
    let killer = async {
        loop {
            if controller
                .snapshot()
                .gate
                .is_some_and(|gate| gate.blocked_after_acceptance)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let _ = control.kill_query(session.lease.token(), connection.connection_id());
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(server, client, killer);
    })
    .await
    .unwrap();
    let actual = observation
        .take_prescribed_eof_after_join()
        .expect("original intermediary typed EOF");
    let typed =
        crate::mysql_write_gate::late_binding::PrescribedRelayEof::from_error(&actual.cause)
            .unwrap();
    assert!(typed.matches(connection, None));
    assert!(
        !typed.matches(
            ClientConnectionToken::new(connection.connection_id(), connection.generation() + 1)
                .unwrap(),
            None
        )
    );
    assert!(typed.matches_gate(&controller.snapshot().gate.unwrap()));
    assert!(actual.cause.to_string().contains(if pool_refusal {
        "ClosingAdmissionCapacityRefused"
    } else {
        "MissingResidentTail"
    }));
    assert!(
        std::error::Error::source(typed)
            .unwrap()
            .downcast_ref::<io::Error>()
            .is_some()
    );
    assert_eq!(observation.snapshot().protocol_io_failures, 0);
    assert_eq!(observation.snapshot().prescribed_protocol_eofs, 1);
    assert_eq!(observation.watcher_snapshot().joined, 1);
    let freeze = controller.snapshot().original_freeze;
    if pool_refusal {
        assert!(
            freeze.is_none(),
            "capacity refusal occurs before original freeze"
        );
    } else {
        let freeze = freeze.expect("actual original close captured fallback scalars");
        assert!(!freeze.had_resident_window);
        assert!(freeze.slots.iter().all(Option::is_none));
        assert!(matches!(
            freeze.current_source,
            crate::mysql_write_gate::original_freeze::CurrentBodySource::OriginalDeliveryFallback
        ));
        let data = freeze.fallback_delivery.unwrap();
        assert_eq!(data.root_task.query_execution_id(), execution_id);
        assert_eq!(data.native_sequence.get(), 1);
        assert_eq!(data.body_bytes, 1048576);
        assert_eq!(
            freeze.framing,
            controller.snapshot().gate.unwrap().cancel_receipt.unwrap()
        );
        assert!(!freeze.tail_complete);
        assert_eq!(freeze.tail_parts, 0);
        assert_eq!(freeze.tail_selected_bytes, 0);
        assert_eq!(freeze.current.unwrap().before_remaining, 0);
        assert!(freeze.current.unwrap().after_remaining > 0);
        assert!(freeze.next.is_none());
    }
    assert!(controller.snapshot().failure.is_none());
    assert!(controller.snapshot().original_writer_exited);
    // Keep the real error: a matching string/kind on another error never acquires provenance.
    observation.observe_protocol_failure(
        connection,
        MysqlConnectionClass::Ordinary,
        io::Error::new(actual.cause.kind(), actual.cause.to_string()),
    );
    assert_eq!(observation.snapshot().protocol_io_failures, 1);
    assert!(
        crate::mysql_write_gate::late_binding::PrescribedRelayEof::from_error(
            &observation
                .take_protocol_failure_after_join()
                .unwrap()
                .cause
        )
        .is_none()
    );
    // The original opaque error still cannot waive a Control exit or a second target EOF.
    observation.observe_protocol_failure(
        connection,
        if pool_refusal {
            MysqlConnectionClass::Control
        } else {
            MysqlConnectionClass::Ordinary
        },
        actual.cause,
    );
    assert_eq!(observation.snapshot().protocol_io_failures, 2);
    let refused = observation.take_protocol_failure_after_join().unwrap();
    assert!(
        crate::mysql_write_gate::late_binding::PrescribedRelayEof::from_error(&refused.cause)
            .is_some()
    );
    controller.stop();
    controller.finish_after_protocol_join().unwrap();
    drop(full);
    drop(holder);
    drop(session);
    drop(producer);
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
}

#[tokio::test]
async fn original_registered_intermediary_missing_v1_tail_preserves_typed_eof_cause() {
    original_intermediary_eof(false).await;
}
#[tokio::test]
async fn original_registered_intermediary_full_v1_closing_pool_preserves_typed_eof_cause() {
    original_intermediary_eof(true).await;
}
