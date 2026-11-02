// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! No-service components. One real V1 governed Closing grant is not Closing64 Native.
use super::*;
use novarocks_query_application::{
    cancellation::QueryCancellationReason,
    protocol_delivery::GovernedProtocolOwner,
    query_control::QueryApplicationControl,
    session_control::{QueryControlService, QuerySessionLease, SessionIdentity},
};
use novarocks_workload_control::{
    ResultCapacityConfig, ResultCapacityHandle, ResultWindowClass, WorkloadConfig, WorkloadControl,
};
use opensrv_mysql::{
    ClosingMysqlWriter, ErrorKind, OwnedStreamingMysqlWriter, ProtocolLimits, ResidentTailPart,
};
use std::{future::Future, sync::atomic::AtomicUsize, time::Duration};
use tokio::io::{AsyncWriteExt, Sink};

type Original = InitiallyRawPressureWriter<Sink>;
type Framer = OwnedStreamingMysqlWriter<Original>;
struct Fixture {
    _host: WorkloadControl,
    capacity: ResultCapacityHandle,
    control: QueryControlService,
    _session: QuerySessionLease,
    protocol: Option<GovernedProtocolOwner>,
    owner: Arc<PressureOwner>,
    controller: FixtureController,
    hook: PressureRelayHook,
}
// The component keeps either the existing kernel owner or the original authorized fixture.
// This is a test-local composition seam, never a second production controller.
enum FixtureController {
    Kernel(PressureController),
    Authorized(crate::closing_pressure_fixture::ClosingPressureFixture),
}
impl FixtureController {
    fn snapshot(&self, slot: usize) -> io::Result<SlotSnapshot> {
        match self {
            Self::Kernel(owner) => owner.snapshot(slot),
            Self::Authorized(owner) => owner.snapshot(slot),
        }
    }
    fn release(&mut self, slot: usize) -> io::Result<()> {
        match self {
            Self::Kernel(owner) => owner.release(slot),
            Self::Authorized(owner) => owner.release(slot),
        }
    }
    fn stop(&mut self) {
        match self {
            Self::Kernel(owner) => owner.stop(),
            Self::Authorized(owner) => owner.stop(),
        }
    }
    fn inspect_after_original_join(&mut self) -> io::Result<()> {
        match self {
            Self::Kernel(owner) => owner.inspect_after_original_join(),
            Self::Authorized(owner) => owner
                .finish_after_original_listener_join()
                .map_err(io::Error::other),
        }
    }
}
impl Fixture {
    async fn new() -> Self {
        Self::with_authorization(
            None,
            FrontendProcessId::new_v7(),
            Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap()
    }
    async fn with_authorization(
        authorization: Option<crate::test_fixture_authorization::MysqlFixtureAuthorization>,
        fe: FrontendProcessId,
        original_deadline: Instant,
    ) -> io::Result<Self> {
        let host = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        let capacity = host
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        host.mark_ready().unwrap();
        let control = QueryControlService::new(Arc::new(QueryApplicationControl::default()));
        let connection = ClientConnectionToken::new(100, 59).unwrap();
        let session = control
            .register_session(SessionIdentity::new(connection, "root"))
            .unwrap();
        let statement = control
            .begin_queued_governed_query_statement_with_result(
                session.token(),
                &host.root_admission(),
                None,
                None,
                None,
                ResultWindowClass::Client,
            )
            .await
            .unwrap();
        let protocol = GovernedProtocolOwner::new(statement);
        let actual_statement = protocol.statement_token().unwrap();
        // Unused entries exercise finite geometry only: they are NOT sixty-four real statements.
        let targets = std::array::from_fn(|i| ArmInput {
            handshake_connection_id: 100 + i as u32,
            original_sql_sha256: ORIGINAL_SQL_SHA256,
        });
        let (owner, controller) = match authorization {
            None => {
                let (owner, mut controller) = PressureOwner::new(fe, original_deadline)?;
                controller.arm_targets(fe, targets)?;
                (Arc::new(owner), FixtureController::Kernel(controller))
            }
            Some(authorization) => {
                let mut fixture =
                    crate::closing_pressure_fixture::ClosingPressureFixture::bind_authorized(
                        authorization,
                        fe,
                        host.observation(),
                    )
                    .map_err(io::Error::other)?;
                let binding = fixture.listener_binding().map_err(io::Error::other)?;
                fixture.arm_targets(fe, targets)?;
                (
                    Arc::clone(&binding.owner),
                    FixtureController::Authorized(fixture),
                )
            }
        };
        let hook = owner
            .bind_relay(
                connection,
                actual_statement.session(),
                actual_statement,
                ORIGINAL_SQL_SHA256,
            )
            .unwrap()
            .unwrap();
        Ok(Self {
            _host: host,
            capacity,
            control,
            _session: session,
            protocol: Some(protocol),
            owner,
            controller,
            hook,
        })
    }
    fn framer(&self) -> Framer {
        OwnedStreamingMysqlWriter::new(
            InitiallyRawPressureWriter::new(
                tokio::io::sink(),
                ClientConnectionToken::new(100, 59).unwrap(),
                Arc::clone(&self.owner),
            ),
            ProtocolLimits::default(),
            4,
        )
        .unwrap()
    }
    async fn real_rows_cut(&self, writer: &mut Framer) -> io::Result<FramingCursor> {
        writer.flush_socket().await?; // Actual original carrier attaches; no fabricated attached flag.
        self.hook.begin_rows(writer.receipt())?;
        writer.start_row(1_048_580)?;
        {
            let original = async {
                writer.write_slice(&[0xfd, 0, 0, 16]).await?;
                let chunk = [b'x'; 4096];
                for _ in 0..256 {
                    writer.write_slice(&chunk).await?;
                }
                writer.flush_pending().await
            };
            tokio::pin!(original);
            tokio::select! {
                result=&mut original=> {result?;return Err(error(Failure::Transition));},
                result=self.hook.scope.wait_rows_blocked()=>{result?;},
            }
        } // Actual borrowed original Data write dropped before reading its original cursor.
        let actual = writer.receipt();
        self.control.cancel_session_statement(
            self.hook.statement.session(),
            QueryCancellationReason::ExplicitKill {
                requester_session:
                    novarocks_query_application::session_control::ClientSessionKey::new(100)
                        .expect("nonzero fixture requester"),
            },
        );
        if !self
            .protocol
            .as_ref()
            .unwrap()
            .cancellation()
            .is_cancelled()
        {
            return Err(error(Failure::Transition));
        }
        self.hook.record_cancel(actual)?;
        Ok(actual)
    }
    fn closing(&mut self, writer: Framer) -> ClosingDelivery<ComponentClosing<Original>> {
        let receipt = writer.receipt();
        let missing = receipt
            .logical_remaining()
            .checked_sub(writer.buffered_row_bytes())
            .unwrap();
        assert!(missing <= 9, "original x frozen remaining wire geometry");
        let mut tail = Vec::with_capacity(1);
        if missing != 0 {
            tail.push(ResidentTailPart::new(Arc::from(vec![b'x'; missing]), 0..missing).unwrap());
        }
        let writer = writer
            .into_closing(tail, ErrorKind::ER_QUERY_INTERRUPTED, b"interrupted")
            .unwrap_or_else(|(_, source)| {
                panic!(
                    "component original Closing writer refused: kind={:?}",
                    source.kind()
                )
            });
        let mut protocol = self.protocol.take().unwrap();
        let actual_capacity = protocol.try_closing_capacity(true).unwrap();
        protocol
            .into_closing_delivery(
                ComponentClosing {
                    writer: Some(writer),
                },
                actual_capacity,
                crate::relay_result_writer::CLOSING_OBJECT_BYTES,
            )
            .unwrap_or_else(|(_, _, _)| {
                panic!("component original governed Closing handoff refused")
            })
    }
}
/// NO-service adapter for public consuming ClosingMysqlWriter::finish. The production helper
/// uses the original ClosingResponseLease directly; this component does not instantiate that lease.
struct ComponentClosing<W> {
    writer: Option<ClosingMysqlWriter<W>>,
}
impl<W: AsyncWrite + Unpin> ComponentClosing<W> {
    async fn finish(&mut self) -> io::Result<W> {
        self.writer
            .take()
            .ok_or_else(|| error(Failure::Transition))?
            .finish()
            .await
    }
}
async fn first_poll<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

#[tokio::test]
async fn same_original_carrier_holds_real_v1_closing_then_release_and_settlement() {
    let mut fixture = Fixture::new().await;
    let mut writer = fixture.framer();
    let actual = fixture.real_rows_cut(&mut writer).await.unwrap();
    let mut closing = fixture.closing(writer);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
    let original_restored = {
        let work = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
            fixture.hook.scope.observe_installed_closing(
                fixture.hook.statement,
                actual,
                &closing,
            )?;
            let original = closing.writer_mut().finish();
            tokio::pin!(original);
            paired_finish(&fixture.hook.scope, original).await
        });
        tokio::pin!(work);
        assert!(first_poll(work.as_mut()).await.is_pending());
        let facts = fixture.controller.snapshot(0).unwrap();
        assert!(
            facts.real_closing_observed
                && facts.paired_closing_polls > 0
                && facts.closing_write_blocked
        );
        assert!(!facts.writer_destructor_returned);
        fixture.controller.release(0).unwrap();
        work.await.unwrap().unwrap()
    };
    assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
    let _ = closing.settle_after_writer_exit().await;
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
    assert!(
        !fixture
            .hook
            .scope
            .snapshot()
            .unwrap()
            .writer_destructor_returned
    );
    drop(original_restored);
    assert!(
        fixture
            .hook
            .scope
            .snapshot()
            .unwrap()
            .writer_destructor_returned
    );
    assert!(fixture.hook.scope.snapshot().unwrap().failure.is_none());
}

#[tokio::test]
async fn original_five_second_timeout_runs_while_gate_held_no_pre_timeout_wait() {
    let mut fixture = Fixture::new().await;
    let mut writer = fixture.framer();
    let actual = fixture.real_rows_cut(&mut writer).await.unwrap();
    let mut closing = fixture.closing(writer);
    let outcome = {
        let work = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
            fixture.hook.scope.observe_installed_closing(
                fixture.hook.statement,
                actual,
                &closing,
            )?;
            let original = closing.writer_mut().finish();
            tokio::pin!(original);
            paired_finish(&fixture.hook.scope, original).await
        });
        work.await
    }; // Original timeout dropped its actual borrowed finish, never the outer owner.
    assert!(outcome.is_err());
    let facts = fixture.hook.scope.snapshot().unwrap();
    assert!(
        facts.real_closing_observed
            && facts.paired_closing_polls > 0
            && facts.writer_destructor_returned
    );
    assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
    drop(closing);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
}

#[tokio::test]
async fn dropping_borrowed_finish_drops_writer_but_original_protocol_grant_remains_outer() {
    let mut fixture = Fixture::new().await;
    let mut writer = fixture.framer();
    let actual = fixture.real_rows_cut(&mut writer).await.unwrap();
    let mut closing = fixture.closing(writer);
    {
        let work = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
            fixture.hook.scope.observe_installed_closing(
                fixture.hook.statement,
                actual,
                &closing,
            )?;
            let original = closing.writer_mut().finish();
            tokio::pin!(original);
            paired_finish(&fixture.hook.scope, original).await
        });
        tokio::pin!(work);
        assert!(first_poll(work.as_mut()).await.is_pending());
    } // Whole original borrowed future/storage dropped here, not merely its Pin reference.
    assert!(
        !fixture
            .hook
            .scope
            .core
            .state(0)
            .unwrap()
            .closing_poll_active
    );
    assert!(
        fixture
            .hook
            .scope
            .snapshot()
            .unwrap()
            .writer_destructor_returned
    );
    assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
    drop(closing);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
}

#[tokio::test]
async fn wrong_actual_closing_writer_cannot_claim_pair_from_hook_identity_alone() {
    let mut fixture = Fixture::new().await;
    let mut original = fixture.framer();
    let actual = fixture.real_rows_cut(&mut original).await.unwrap();
    // A valid real V1 governed Closing object, deliberately containing a different ungated W.
    let unrelated =
        OwnedStreamingMysqlWriter::new(tokio::io::sink(), ProtocolLimits::default(), 4).unwrap();
    let unrelated = unrelated
        .into_closing(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"interrupted")
        .unwrap_or_else(|(_, _)| panic!("valid unrelated component Closing"));
    let mut protocol = fixture.protocol.take().unwrap();
    let capacity = protocol.try_closing_capacity(true).unwrap();
    let mut closing = protocol
        .into_closing_delivery(
            ComponentClosing {
                writer: Some(unrelated),
            },
            capacity,
            crate::relay_result_writer::CLOSING_OBJECT_BYTES,
        )
        .unwrap_or_else(|(_, _, _)| panic!("actual Closing handoff"));
    let refused = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
        fixture
            .hook
            .scope
            .observe_installed_closing(fixture.hook.statement, actual, &closing)?;
        let wrong = closing.writer_mut().finish();
        tokio::pin!(wrong);
        paired_finish(&fixture.hook.scope, wrong).await
    })
    .await
    .unwrap();
    assert!(refused.is_err());
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().paired_closing_polls,
        0
    );
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().failure,
        Some(Failure::Identity)
    );
    drop(original);
    drop(closing);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
}

#[tokio::test]
async fn unrelated_async_pending_is_not_a_same_writer_witness() {
    let mut fixture = Fixture::new().await;
    let mut original = fixture.framer();
    let actual = fixture.real_rows_cut(&mut original).await.unwrap();
    let mut closing = fixture.closing(original);
    let refusal = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
        fixture
            .hook
            .scope
            .observe_installed_closing(fixture.hook.statement, actual, &closing)?;
        let wrong = std::future::pending::<io::Result<()>>();
        tokio::pin!(wrong);
        paired_finish(&fixture.hook.scope, wrong).await
    })
    .await
    .unwrap();
    assert!(refusal.is_err());
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().paired_closing_polls,
        0
    );
    assert!(
        !fixture
            .hook
            .scope
            .core
            .state(0)
            .unwrap()
            .closing_poll_active
    );
    drop(closing);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
}

#[tokio::test]
async fn original_async_panic_preserves_payload_and_clears_only_poll_guard_not_outer_grant() {
    let mut fixture = Fixture::new().await;
    let mut original = fixture.framer();
    let actual = fixture.real_rows_cut(&mut original).await.unwrap();
    let mut closing = fixture.closing(original);
    let identity = Arc::new(());
    let payload = {
        let original_identity = Arc::clone(&identity);
        let work = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
            fixture.hook.scope.observe_installed_closing(
                fixture.hook.statement,
                actual,
                &closing,
            )?;
            let panic_future = async move {
                std::panic::panic_any(Canary(original_identity));
                #[allow(unreachable_code)]
                Ok::<(), io::Error>(())
            };
            tokio::pin!(panic_future);
            paired_finish(&fixture.hook.scope, panic_future).await
        });
        tokio::pin!(work);
        std::future::poll_fn(|cx| {
            Poll::Ready(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || work.as_mut().poll(cx),
            )))
        })
        .await
        .unwrap_err()
    }; // The original panic is still owned, and the poisoned async future is never repolled.
    assert!(Arc::ptr_eq(
        &payload.downcast_ref::<Canary>().unwrap().0,
        &identity
    ));
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().failure,
        Some(Failure::Panic)
    );
    assert!(fixture.controller.release(0).is_err());
    let _ = fixture.owner.core.fail(Failure::Deadline);
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().failure,
        Some(Failure::Panic)
    );
    assert!(
        !fixture
            .hook
            .scope
            .core
            .state(0)
            .unwrap()
            .closing_poll_active
    );
    assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
    assert!(
        !fixture
            .hook
            .scope
            .snapshot()
            .unwrap()
            .writer_destructor_returned
    );
    drop(closing);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
    assert!(
        fixture
            .hook
            .scope
            .snapshot()
            .unwrap()
            .writer_destructor_returned
    );
    fixture.controller.stop();
    assert!(fixture.controller.inspect_after_original_join().is_err());
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().failure,
        Some(Failure::Panic)
    );
}

struct ShutdownCount(Arc<AtomicUsize>);
impl AsyncWrite for ShutdownCount {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn mismatched_connection_fails_attachment_but_actual_shutdown_still_passes() {
    let fixture = Fixture::new().await;
    let count = Arc::new(AtomicUsize::new(0));
    let mut wrong = InitiallyRawPressureWriter::new(
        ShutdownCount(Arc::clone(&count)),
        ClientConnectionToken::new(100, 60).unwrap(),
        Arc::clone(&fixture.owner),
    );
    assert!(wrong.flush().await.is_err());
    assert!(!fixture.hook.scope.snapshot().unwrap().writer_attached);
    wrong.shutdown().await.unwrap();
    assert_eq!(count.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().failure,
        Some(Failure::Identity)
    );
}

#[derive(Debug)]
struct Canary(Arc<()>);
impl std::fmt::Display for Canary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("secret source canary")
    }
}
impl std::error::Error for Canary {}
#[test]
fn refusal_primary_and_secondary_original_sources_survive_finite_presentation() {
    let a = Arc::new(());
    let b = Arc::new(());
    let combined = preserve_refusal_hook_failure(
        io::Error::new(io::ErrorKind::BrokenPipe, Canary(Arc::clone(&a))),
        io::Error::new(io::ErrorKind::InvalidData, Canary(Arc::clone(&b))),
    );
    let owned = combined
        .get_ref()
        .unwrap()
        .downcast_ref::<RefusalHookFailure>()
        .unwrap();
    assert!(Arc::ptr_eq(
        &owned
            .original
            .get_ref()
            .unwrap()
            .downcast_ref::<Canary>()
            .unwrap()
            .0,
        &a
    ));
    assert!(Arc::ptr_eq(
        &owned
            .hook
            .get_ref()
            .unwrap()
            .downcast_ref::<Canary>()
            .unwrap()
            .0,
        &b
    ));
    assert!(!format!("{combined:?} {combined}").contains("secret source canary"));
}

#[tokio::test]
async fn cancelled_borrowed_same_carrier_invalidates_hold_before_writer_and_grant_exit() {
    let mut fixture = Fixture::new().await;
    let mut writer = fixture.framer();
    let actual = fixture.real_rows_cut(&mut writer).await.unwrap();
    let mut closing = fixture.closing(writer);
    fixture
        .hook
        .scope
        .observe_installed_closing(fixture.hook.statement, actual, &closing)
        .unwrap();
    // A component moves the same original W out of its consuming component writer
    // so a borrowed IO future can be cancelled while W remains physically alive.
    // This does not instantiate the production ClosingResponseLease or a Native query.
    let mut original_w = closing.writer_mut().writer.take().unwrap().into_inner();
    {
        let original = original_w.write_all(b"same original carrier");
        tokio::pin!(original);
        let finish = paired_finish(&fixture.hook.scope, original);
        tokio::pin!(finish);
        assert!(first_poll(finish.as_mut()).await.is_pending());
        let facts = fixture.controller.snapshot(0).unwrap();
        assert_eq!(facts.phase, Phase::ClosingHeld);
        assert!(facts.paired_closing_polls > 0 && !facts.writer_destructor_returned);
        assert!(facts.failure.is_none());
    } // Actual borrowed future storage exits; W and the real grant remain outside it.
    let facts = fixture.controller.snapshot(0).unwrap();
    assert_eq!(facts.failure, Some(Failure::Transition));
    assert!(!facts.writer_destructor_returned);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
    assert!(fixture.controller.release(0).is_err());
    drop(closing);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
    assert!(
        !fixture
            .controller
            .snapshot(0)
            .unwrap()
            .writer_destructor_returned
    );
    drop(original_w);
    assert!(
        fixture
            .controller
            .snapshot(0)
            .unwrap()
            .writer_destructor_returned
    );
    assert_eq!(
        fixture.controller.snapshot(0).unwrap().failure,
        Some(Failure::Transition)
    );
}

#[tokio::test]
async fn scoped_fe_authorization_hits_real_governed_closing_and_end_waits_for_actual_tail_exit()
-> io::Result<()> {
    use crate::test_fixture_authorization::MysqlFixtureAuthorization;
    use novarocks_failpoint::{
        liveness::{PreparedRoleSession, RunnerSession},
        scope::ProcessBinding,
        session::{DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole},
    };
    use std::os::unix::fs::DirBuilderExt;
    #[derive(Debug)]
    struct OwnedErrors(Vec<io::Error>);
    impl std::fmt::Display for OwnedErrors {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "scoped real Closing owner failures: {}", self.0.len())
        }
    }
    impl std::error::Error for OwnedErrors {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.0.first().map(|e| e as _)
        }
    }
    struct OriginalPanic(std::sync::Mutex<Box<dyn std::any::Any + Send>>);
    impl std::fmt::Debug for OriginalPanic {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let _retained = &self.0;
            f.write_str("OriginalScopedClosingPanic { payload_retained: true }")
        }
    }
    impl std::fmt::Display for OriginalPanic {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(self, f)
        }
    }
    impl std::error::Error for OriginalPanic {}
    let original_deadline = Instant::now() + Duration::from_secs(10);
    let frontend = FrontendProcessId::new_v7();
    let root = std::path::PathBuf::from(format!("/tmp/nr-real-closing-{}", frontend));
    std::fs::DirBuilder::new().mode(0o700).create(&root)?;
    let socket = root.join("pressure.sock");
    let path = root.join("declaration.json");
    let declaration = SessionDeclaration {
        schema_version: DECLARATION_VERSION,
        session_id: "real-closing-session".into(),
        launch_id: "real-closing-launch".into(),
        role: SessionRole::Fe,
        lifetime_millis: 10_000,
        liveness_socket: root.join("live.sock"),
        handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
        domain_grants: vec![DomainGrant {
            domain: MysqlFixtureAuthorization::DOMAIN.into(),
            grant_id: "closing-once".into(),
            parameters: serde_json::json!({"action":"closing-pressure", "socket":socket, "nonce_hex":"01010101010101010101010101010101"}),
        }],
    };
    declaration.write_private(&path)?;
    let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)?;
    let mut runner = RunnerSession::start_until(declaration, original_deadline)?;
    let mut role = match prepared.bind(ProcessBinding::Frontend(frontend)) {
        Ok(role) => role,
        Err(primary) => {
            let mut failures = vec![primary];
            if let Err(error) = runner.end_and_join() {
                failures.push(error);
            }
            return Err(io::Error::other(OwnedErrors(failures)));
        }
    };
    let scope = role.scope().clone();
    let mut fixture_owner = None;
    let mut closing_owner = None;
    let mut original_stop_error = None;
    let primary = {
        let body = async {
            assert!(MysqlFixtureAuthorization::from_grants(Vec::new())?.is_none());
            assert!(!socket.exists());
            let authorization = MysqlFixtureAuthorization::from_grants(
                scope.grants_for(MysqlFixtureAuthorization::DOMAIN),
            )?
            .unwrap();
            fixture_owner = Some(
                Fixture::with_authorization(Some(authorization), frontend, original_deadline)
                    .await?,
            );
            let fixture = fixture_owner.as_mut().unwrap();
            let FixtureController::Authorized(original_fixture) = &fixture.controller else {
                unreachable!()
            };
            let cancellation = original_fixture.control_cancellation().unwrap();
            assert!(cancellation.deadline() <= original_deadline);
            assert!(socket.exists());
            let mut writer = fixture.framer();
            let actual = fixture.real_rows_cut(&mut writer).await?;
            assert_eq!(
                scope
                    .observations()
                    .iter()
                    .filter(|o| o.event == "hit")
                    .count(),
                0,
                "Rows do not credit Closing"
            );
            closing_owner = Some(fixture.closing(writer));
            assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
            let original_error = {
                let closing = closing_owner.as_mut().unwrap();
                let work =
                    tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
                        fixture.hook.scope.observe_installed_closing(
                            fixture.hook.statement,
                            actual,
                            closing,
                        )?;
                        let original = closing.writer_mut().finish();
                        tokio::pin!(original);
                        paired_finish(&fixture.hook.scope, original).await
                    });
                tokio::pin!(work);
                assert!(first_poll(work.as_mut()).await.is_pending());
                let facts = fixture.controller.snapshot(0)?;
                assert!(
                    facts.real_closing_observed
                        && facts.paired_closing_polls > 0
                        && facts.closing_write_blocked
                );
                assert!(!facts.writer_destructor_returned);
                assert_eq!(
                    scope
                        .observations()
                        .iter()
                        .filter(|o| o.event == "hit")
                        .count(),
                    1
                );
                assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
                runner.end_and_join()?;
                cancellation.cancelled().await;
                assert!(!cancellation.is_active());
                // The original role owner explicitly stops its fixture on scope closure.
                // Scope closure alone does not settle the still-live protocol/Closing grant.
                fixture.controller.stop();
                assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
                let result = work.await.map_err(|_| error(Failure::Deadline))?;
                match result {
                    Err(original) => original,
                    Ok(original_writer) => {
                        drop(original_writer);
                        return Err(io::Error::other(
                            "stopped original Closing unexpectedly completed",
                        ));
                    }
                }
            };
            assert_eq!(original_error.kind(), io::ErrorKind::Interrupted);
            eprintln!(
                "expected original scoped Closing stop retained: kind={:?}",
                original_error.kind()
            );
            original_stop_error = Some(original_error);
            assert!(fixture.controller.snapshot(0)?.writer_destructor_returned);
            // The physical writer has exited, but the original outer tail still owns capacity and generation.
            assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
            assert!(matches!(
                fixture.control.begin_statement(fixture._session.token()),
                Err(novarocks_query_application::session_control::QueryControlError::StatementBusy)
            ));
            drop(closing_owner.take());
            assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
            assert!(
                fixture
                    .control
                    .begin_statement(fixture._session.token())
                    .is_ok()
            );
            Ok::<_, io::Error>(())
        };
        tokio::pin!(body);
        poll_fn(|cx| {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body.as_mut().poll(cx)))
            {
                Ok(poll) => poll,
                Err(original) => Poll::Ready(Err(io::Error::other(OriginalPanic(
                    std::sync::Mutex::new(original),
                )))),
            }
        })
        .await
    };
    let mut failures = Vec::new();
    if let Err(error) = primary {
        failures.push(error);
    }
    // Every original borrowed work future has left scope before physical tail cleanup.
    drop(closing_owner.take());
    if let Some(fixture) = fixture_owner.as_mut() {
        fixture.controller.stop();
        if fixture.capacity.snapshot().held_positions != [0; 4] {
            failures.push(io::Error::other(
                "Closing capacity survived actual owner cleanup",
            ));
        }
        if let FixtureController::Authorized(owner) = &mut fixture.controller {
            if let Err(error) = owner.close_control() {
                failures.push(io::Error::other(error));
            }
        }
    }
    if let Err(error) = runner.end_and_join() {
        failures.push(error);
    }
    scope.wait_closed();
    if let Err(error) = role.close_and_join() {
        failures.push(error);
    }
    drop(fixture_owner);
    for event in ["claimed", "hit", "claim-released"] {
        if scope
            .observations()
            .iter()
            .filter(|o| o.event == event)
            .count()
            != 1
        {
            failures.push(io::Error::other(format!(
                "real Closing did not record exactly one {event}"
            )));
        }
    }
    if socket.exists() || root.join("live.sock").exists() {
        failures.push(io::Error::other(
            "original real Closing owner socket remained",
        ));
    }
    for artifact in [
        runner.process_receipt_path().to_path_buf(),
        path.with_extension("claimed"),
        path,
    ] {
        if let Err(error) = std::fs::remove_file(artifact) {
            failures.push(error);
        }
    }
    if let Err(error) = std::fs::remove_dir(root) {
        failures.push(error);
    }
    if failures.is_empty() {
        Ok(())
    } else {
        if let Some(original) = original_stop_error {
            failures.push(original);
        }
        Err(io::Error::other(OwnedErrors(failures)))
    }
}
