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

//! State-kernel and original IO components. Synthetic transitions are never Native Closing evidence.
use super::*;
use novarocks_query_application::session_control::SessionToken;
use opensrv_mysql::{OwnedStreamingMysqlWriter, ProtocolLimits};
use std::{
    collections::VecDeque, future::poll_fn, sync::atomic::AtomicUsize, task::Wake, time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};

fn frontend() -> FrontendProcessId {
    "019a0203-0405-7000-8000-000000000001"
        .parse()
        .expect("fixed UUIDv7")
}
fn inputs() -> [ArmInput; TARGETS] {
    std::array::from_fn(|i| ArmInput {
        handshake_connection_id: i as u32 + 100,
        original_sql_sha256: ORIGINAL_SQL_SHA256,
    })
}
fn statement(slot: usize) -> StatementToken {
    StatementToken::new(
        SessionToken::new(
            novarocks_query_application::session_control::ClientSessionKey::new(slot as u64 + 100)
                .unwrap(),
        ),
        83,
    )
}
fn bound(slot: usize) -> io::Result<(PressureOwner, PressureController, PressureScope)> {
    let (owner, mut controller) =
        PressureOwner::new(frontend(), Instant::now() + Duration::from_secs(20))?;
    controller.arm_targets(frontend(), inputs())?;
    let scope = owner
        .bind_statement(
            ClientConnectionToken::new(slot as u32 + 100, 59).expect("fixed token"),
            statement(slot).session(),
            statement(slot),
            ORIGINAL_SQL_SHA256,
        )?
        .expect("selected scope");
    Ok((owner, controller, scope))
}
fn baseline() -> FramingCursor {
    OwnedStreamingMysqlWriter::new(tokio::io::sink(), ProtocolLimits::default(), 4)
        .expect("real framer")
        .receipt()
}
/// Synthetic state-kernel receipt, not physical socket/Closing/Root authority.
fn synthetic_cancel() -> FramingCursor {
    FramingCursor {
        phase: WritePhase::Row,
        sequence: 4,
        logical_total: 1_048_580,
        logical_written: 1_048_571,
        packet_payload_length: 1_048_580,
        packet_payload_written: 1_048_571,
        header: [4, 0, 16, 4],
        header_written: 4,
        zero_terminal_pending: false,
        committed_wire_bytes: ROW_CUT,
        rows_completed: 0,
    }
}
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
fn counted_waker() -> (Arc<WakeCount>, Waker) {
    let count = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    (count, waker)
}
enum Step {
    Pending,
    Zero,
    Take(usize),
    Over,
    Error(io::Error),
}
struct Scripted {
    steps: VecDeque<Step>,
    vectored: bool,
    drops: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
}
impl Scripted {
    fn new(steps: impl IntoIterator<Item = Step>, vectored: bool) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            vectored,
            drops: Arc::new(AtomicUsize::new(0)),
            polls: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn scripted_poll(&mut self, offered: usize) -> Poll<io::Result<usize>> {
        self.polls.fetch_add(1, Ordering::AcqRel);
        match self.steps.pop_front() {
            Some(Step::Pending) => Poll::Pending,
            Some(Step::Zero) => Poll::Ready(Ok(0)),
            Some(Step::Take(n)) => Poll::Ready(Ok(n.min(offered))),
            Some(Step::Over) => Poll::Ready(Ok(offered + 1)),
            Some(Step::Error(error)) => Poll::Ready(Err(error)),
            None => Poll::Ready(Ok(offered)),
        }
    }
}
impl Drop for Scripted {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}
impl AsyncWrite for Scripted {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().scripted_poll(bytes.len())
    }
    fn is_write_vectored(&self) -> bool {
        self.vectored
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        slices: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .scripted_poll(slices.iter().map(|s| s.len()).sum())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
async fn reach_synthetic_cancel(
    scope: &PressureScope,
    gate: &mut ClosingPressureGate<Scripted>,
    slot: usize,
) -> io::Result<()> {
    scope.begin_rows(statement(slot), baseline())?;
    let chunk = [b'a'; 4096];
    let mut left = ROW_CUT as usize;
    while left != 0 {
        let take = left.min(chunk.len());
        gate.write_all(&chunk[..take]).await?;
        left -= take;
    }
    let (_, waker) = counted_waker();
    let mut cx = Context::from_waker(&waker);
    if !Pin::new(gate).poll_write(&mut cx, b"tail").is_pending() {
        return Err(error(Failure::Transition));
    }
    scope.observe_cancel(statement(slot), synthetic_cancel())
}

#[tokio::test]
async fn frozen_65_arm_rejects_duplicate_and_cannot_rearm() {
    let (owner, mut controller) =
        PressureOwner::new(frontend(), Instant::now() + Duration::from_secs(20)).unwrap();
    let mut targets = inputs();
    targets[64].handshake_connection_id = targets[0].handshake_connection_id;
    assert!(controller.arm_targets(frontend(), targets).is_err());
    assert_eq!(
        controller.snapshot(0).unwrap().failure,
        Some(Failure::Identity)
    );
    assert!(controller.arm_targets(frontend(), inputs()).is_err());
    assert!(
        owner
            .bind_statement(
                ClientConnectionToken::new(100, 59).unwrap(),
                statement(0).session(),
                statement(0),
                ORIGINAL_SQL_SHA256
            )
            .unwrap()
            .is_none()
    );
    // Invalid Arm installs no selected target. This connection therefore stays
    // raw; the attempt remains failed and cannot produce a scope or acceptance.
    let rejected = controller.snapshot(0).unwrap();
    assert_eq!(rejected.phase, Phase::Unarmed);
    assert_eq!(rejected.failure, Some(Failure::Identity));
    assert!(!rejected.writer_attached);
    assert!(
        owner
            .bind_statement(
                ClientConnectionToken::new(999, 59).unwrap(),
                StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(999)
                            .expect("nonzero fixture session")
                    ),
                    83
                )
                .session(),
                StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(999)
                            .expect("nonzero fixture session")
                    ),
                    83
                ),
                [0; 32]
            )
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn original_distinct_tokens_and_duplicate_binding_stay_first_cause() {
    let (owner, controller, scope) = bound(0).unwrap();
    let facts = scope.snapshot().unwrap();
    assert_eq!(facts.connection.unwrap().generation(), 59);
    assert_eq!(facts.statement.unwrap().session(), statement(0).session());
    assert_eq!(facts.statement.unwrap().generation(), 83);
    assert!(
        owner
            .bind_statement(
                facts.connection.unwrap(),
                statement(0).session(),
                statement(0),
                ORIGINAL_SQL_SHA256
            )
            .is_err()
    );
    assert!(scope.begin_rows(statement(0), baseline()).is_err());
    assert_eq!(
        controller.snapshot(0).unwrap().failure,
        Some(Failure::Transition)
    );
}
#[tokio::test]
async fn frozen_sql_and_foreign_original_session_are_refused() {
    let (owner, mut controller) =
        PressureOwner::new(frontend(), Instant::now() + Duration::from_secs(20)).unwrap();
    let mut targets = inputs();
    targets[3].original_sql_sha256 = [0; 32];
    assert!(controller.arm_targets(frontend(), targets).is_err());
    drop(owner);
    drop(controller);
    let (owner, mut controller) =
        PressureOwner::new(frontend(), Instant::now() + Duration::from_secs(20)).unwrap();
    controller.arm_targets(frontend(), inputs()).unwrap();
    assert!(
        owner
            .bind_statement(
                ClientConnectionToken::new(100, 59).unwrap(),
                statement(0).session(),
                StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(999)
                            .unwrap()
                    ),
                    83
                ),
                ORIGINAL_SQL_SHA256
            )
            .is_err()
    );
    assert_eq!(
        controller.snapshot(0).unwrap().failure,
        Some(Failure::Identity)
    );
}
#[tokio::test]
async fn pending_zero_actual_multi_ready_and_empty_write_account_only_actual_bytes() {
    let (_owner, controller, scope) = bound(0).unwrap();
    let mut gate = ClosingPressureGate::new(
        Scripted::new(
            [Step::Pending, Step::Zero, Step::Take(2), Step::Take(3)],
            true,
        ),
        &scope,
    )
    .unwrap();
    scope.begin_rows(statement(0), baseline()).unwrap();
    let (_, waker) = counted_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(
        Pin::new(&mut gate)
            .poll_write(&mut cx, b"abcde")
            .is_pending()
    );
    assert!(matches!(
        Pin::new(&mut gate).poll_write(&mut cx, b"abcde"),
        Poll::Ready(Ok(0))
    ));
    assert!(matches!(
        Pin::new(&mut gate).poll_write(&mut cx, b"abcde"),
        Poll::Ready(Ok(2))
    ));
    assert!(matches!(
        Pin::new(&mut gate)
            .poll_write_vectored(&mut cx, &[IoSlice::new(b"c"), IoSlice::new(b"de")]),
        Poll::Ready(Ok(3))
    ));
    assert!(matches!(
        Pin::new(&mut gate).poll_write_vectored(&mut cx, &[]),
        Poll::Ready(Ok(0))
    ));
    let facts = controller.snapshot(0).unwrap();
    assert_eq!(facts.accepted_prefix_bytes, 5);
    assert_eq!(facts.successful_inner_writes, 2);
    assert_eq!(
        facts.accepted_prefix_sha256,
        <[u8; 32]>::from(Sha256::digest(b"abcde"))
    );
    assert_eq!(
        (facts.scalar_inner_polls, facts.vectored_inner_polls),
        (3, 2)
    );
    assert!(gate.is_write_vectored());
    drop(gate);
    assert!(scope.snapshot().unwrap().writer_destructor_returned);
}
#[derive(Clone)]
struct Canary(Arc<()>);
impl std::fmt::Debug for Canary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("private-source-canary")
    }
}
impl std::fmt::Display for Canary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for Canary {}
#[tokio::test]
async fn original_inner_error_identity_survives_gate_failure_and_drop() {
    let (_owner, controller, scope) = bound(0).unwrap();
    let identity = Arc::new(());
    let mut gate = ClosingPressureGate::new(
        Scripted::new(
            [Step::Error(io::Error::new(
                io::ErrorKind::BrokenPipe,
                Canary(Arc::clone(&identity)),
            ))],
            false,
        ),
        &scope,
    )
    .unwrap();
    scope.begin_rows(statement(0), baseline()).unwrap();
    let original = gate.write(b"bytes").await.unwrap_err();
    drop(gate);
    assert_eq!(original.kind(), io::ErrorKind::BrokenPipe);
    assert!(Arc::ptr_eq(
        &original
            .get_ref()
            .unwrap()
            .downcast_ref::<Canary>()
            .unwrap()
            .0,
        &identity
    ));
    assert_eq!(
        controller.snapshot(0).unwrap().failure,
        Some(Failure::InnerIo)
    );
    assert_eq!(controller.snapshot(0).unwrap().accepted_prefix_bytes, 0);
    assert!(!format!("{:?}", controller.snapshot(0).unwrap()).contains("private-source-canary"));
}
#[tokio::test]
async fn invalid_inner_length_is_sticky_and_selected_phase_race_never_polls_inner() {
    let (_owner, controller, scope) = bound(0).unwrap();
    let inner = Scripted::new([Step::Over], false);
    let polls = Arc::clone(&inner.polls);
    let mut gate = ClosingPressureGate::new(inner, &scope).unwrap();
    scope.begin_rows(statement(0), baseline()).unwrap();
    assert!(gate.write(b"x").await.is_err());
    assert_eq!(polls.load(Ordering::Acquire), 1);
    assert_eq!(
        controller.snapshot(0).unwrap().failure,
        Some(Failure::Length)
    );
    drop(gate);
    drop(controller);
    let (_owner, controller, scope) = bound(0).unwrap();
    let gate = ClosingPressureGate::new(Scripted::new([], false), &scope).unwrap();
    let (_, waker) = counted_waker();
    let cx = Context::from_waker(&waker);
    let old_plan = gate.plan(&cx, false, true).unwrap().unwrap();
    scope.begin_rows(statement(0), baseline()).unwrap();
    assert!(gate.start_poll(old_plan, false, false).is_err());
    assert_eq!(controller.snapshot(0).unwrap().scalar_inner_polls, 0);
    assert_eq!(
        controller.snapshot(0).unwrap().failure,
        Some(Failure::Transition)
    );
}
#[tokio::test]
async fn synthetic_closing_hold_blocks_all_writes_and_flush_then_release_recovery_is_raw() {
    let (owner, mut controller, scope) = bound(0).unwrap();
    let inner = Scripted::new([], true);
    let polls = Arc::clone(&inner.polls);
    let mut gate = ClosingPressureGate::new(inner, &scope).unwrap();
    reach_synthetic_cancel(&scope, &mut gate, 0).await.unwrap();
    // Private state-kernel seam only: no ClosingDelivery/grant/body exists in this test.
    scope
        .install_observation(statement(0), synthetic_cancel())
        .unwrap();
    let (count, waker) = counted_waker();
    let mut cx = Context::from_waker(&waker);
    let before = polls.load(Ordering::Acquire);
    assert!(Pin::new(&mut gate).poll_write(&mut cx, b"").is_pending());
    assert!(
        Pin::new(&mut gate)
            .poll_write_vectored(&mut cx, &[])
            .is_pending()
    );
    assert!(Pin::new(&mut gate).poll_flush(&mut cx).is_pending());
    assert_eq!(polls.load(Ordering::Acquire), before);
    controller.release(0).unwrap();
    assert!(count.0.load(Ordering::Acquire) > 0);
    assert_eq!(gate.write(b"tail").await.unwrap(), 4);
    gate.flush().await.unwrap();
    let later = StatementToken::new(statement(0).session(), 84);
    assert!(
        owner
            .bind_statement(
                ClientConnectionToken::new(100, 59).unwrap(),
                later.session(),
                later,
                [1; 32]
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(scope.snapshot().unwrap().accepted_prefix_bytes, ROW_CUT);
    assert!(controller.release(0).is_err());
    assert_eq!(scope.snapshot().unwrap().failure, Some(Failure::Transition));
}
#[tokio::test]
async fn bad_cancel_receipt_wakes_blocked_writer_and_stop_shutdown_drop_are_distinct() {
    let (_owner, mut controller, scope) = bound(0).unwrap();
    let inner = Scripted::new([], false);
    let drops = Arc::clone(&inner.drops);
    let mut gate = ClosingPressureGate::new(inner, &scope).unwrap();
    reach_synthetic_cancel(&scope, &mut gate, 0).await.unwrap();
    let (count, waker) = counted_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(
        Pin::new(&mut gate)
            .poll_write(&mut cx, b"tail")
            .is_pending()
    );
    let wrong = StatementToken::new(statement(0).session(), 84);
    assert!(
        scope
            .install_observation(wrong, synthetic_cancel())
            .is_err()
    );
    assert!(count.0.load(Ordering::Acquire) > 0);
    controller.stop();
    assert!(gate.write(b"tail").await.is_err());
    gate.shutdown().await.unwrap();
    let facts = scope.snapshot().unwrap();
    assert!(facts.physical_shutdown_completed);
    assert!(!facts.writer_destructor_returned);
    drop(gate);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert!(scope.snapshot().unwrap().writer_destructor_returned);
    assert_eq!(scope.snapshot().unwrap().failure, Some(Failure::Identity));
    assert!(controller.inspect_after_original_join().is_err()); // One writer cannot prove sixty-four.
}
#[tokio::test]
async fn sixty_fifth_retains_only_moved_capacity_source_and_rejects_duplicate() {
    let (_owner, controller, scope) = bound(64).unwrap();
    let mut gate = ClosingPressureGate::new(Scripted::new([], false), &scope).unwrap();
    reach_synthetic_cancel(&scope, &mut gate, 64).await.unwrap();
    scope
        .observe_capacity_refused(
            statement(64),
            synthetic_cancel(),
            WorkError::Capacity("original capacity source"),
        )
        .unwrap();
    assert!(scope.snapshot().unwrap().original_capacity_source_retained);
    let rejected = scope
        .observe_capacity_refused(
            statement(64),
            synthetic_cancel(),
            WorkError::Capacity("second source canary"),
        )
        .unwrap_err();
    let aggregate = rejected
        .get_ref()
        .unwrap()
        .downcast_ref::<RejectedAdmission>()
        .unwrap();
    assert!(matches!(
        &aggregate.admission,
        WorkError::Capacity("second source canary")
    ));
    assert!(!format!("{rejected:?}").contains("second source canary"));
    assert!(matches!(
        scope.core.state(64).unwrap().refusal.as_ref(),
        Some(WorkError::Capacity("original capacity source"))
    ));
    assert_eq!(
        controller.snapshot(64).unwrap().failure,
        Some(Failure::Transition)
    );
}
#[tokio::test]
async fn noncapacity_original_source_is_retained_without_claiming_refusal() {
    let (_owner, _controller, scope) = bound(64).unwrap();
    let mut gate = ClosingPressureGate::new(Scripted::new([], false), &scope).unwrap();
    reach_synthetic_cancel(&scope, &mut gate, 64).await.unwrap();
    let rejected = scope
        .observe_capacity_refused(statement(64), synthetic_cancel(), WorkError::Closed)
        .unwrap_err();
    assert!(matches!(
        rejected
            .get_ref()
            .unwrap()
            .downcast_ref::<RejectedAdmission>()
            .unwrap()
            .admission,
        WorkError::Closed
    ));
    assert!(!scope.snapshot().unwrap().original_capacity_source_retained);
    assert_eq!(scope.snapshot().unwrap().phase, Phase::CancelObserved);
}

#[tokio::test]
async fn stale_and_live_poll_plans_cannot_cross_original_cancel_transition() {
    let (_owner, _controller, scope) = bound(0).unwrap();
    let gate = ClosingPressureGate::new(Scripted::new([], false), &scope).unwrap();
    let (_, waker) = counted_waker();
    let cx = Context::from_waker(&waker);
    let plan = gate.plan(&cx, false, true).unwrap().unwrap();
    let guard = gate.start_poll(plan, false, false).unwrap();
    assert!(scope.begin_rows(statement(0), baseline()).is_err());
    assert_eq!(scope.snapshot().unwrap().phase, Phase::Bound);
    assert!(scope.core.slots[0].inner_poll.load(Ordering::Acquire));
    drop(guard);
    assert!(!scope.core.slots[0].inner_poll.load(Ordering::Acquire));
    assert_eq!(scope.snapshot().unwrap().failure, Some(Failure::Transition));
}

#[tokio::test]
async fn rows_writev_refuses_thirty_third_slice_without_inner_poll() {
    let (_owner, controller, scope) = bound(0).unwrap();
    let inner = Scripted::new([], true);
    let polls = Arc::clone(&inner.polls);
    let mut gate = ClosingPressureGate::new(inner, &scope).unwrap();
    scope.begin_rows(statement(0), baseline()).unwrap();
    let (_, waker) = counted_waker();
    let mut cx = Context::from_waker(&waker);
    let slices = [IoSlice::new(b"x"); 33];
    assert!(matches!(
        Pin::new(&mut gate).poll_write_vectored(&mut cx, &slices),
        Poll::Ready(Err(_))
    ));
    assert_eq!(polls.load(Ordering::Acquire), 0);
    assert_eq!(
        controller.snapshot(0).unwrap().failure,
        Some(Failure::Length)
    );
}

#[tokio::test]
async fn expired_original_clock_never_refreshes_and_stop_wakes_without_claiming_exit() {
    assert!(PressureOwner::new(frontend(), Instant::now() - Duration::from_millis(1)).is_err());
    let (_owner, mut controller, scope) = bound(0).unwrap();
    let mut gate = ClosingPressureGate::new(Scripted::new([], false), &scope).unwrap();
    reach_synthetic_cancel(&scope, &mut gate, 0).await.unwrap();
    let (count, waker) = counted_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(
        Pin::new(&mut gate)
            .poll_write(&mut cx, b"tail")
            .is_pending()
    );
    controller.stop();
    assert!(count.0.load(Ordering::Acquire) > 0);
    assert!(matches!(
        Pin::new(&mut gate).poll_write(&mut cx, b"tail"),
        Poll::Ready(Err(_))
    ));
    assert!(!scope.snapshot().unwrap().writer_destructor_returned);
    drop(gate);
    assert!(scope.snapshot().unwrap().writer_destructor_returned);
}

#[tokio::test]
async fn held_release_before_an_actual_closing_poll_is_refused() {
    let (_owner, mut controller, scope) = bound(0).unwrap();
    let mut gate = ClosingPressureGate::new(Scripted::new([], false), &scope).unwrap();
    reach_synthetic_cancel(&scope, &mut gate, 0).await.unwrap();
    scope
        .install_observation(statement(0), synthetic_cancel())
        .unwrap();
    assert!(controller.release(0).is_err());
    assert_eq!(scope.snapshot().unwrap().phase, Phase::ClosingHeld);
    assert_eq!(scope.snapshot().unwrap().failure, Some(Failure::Transition));
}

struct DropProbe {
    core: Arc<Core>,
    saw_unreturned: Arc<AtomicBool>,
    panic: bool,
}
impl AsyncWrite for DropProbe {
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
        Poll::Ready(Ok(()))
    }
}
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.saw_unreturned.store(
            !self.core.snapshot(0).unwrap().writer_destructor_returned,
            Ordering::Release,
        );
        if self.panic {
            std::panic::panic_any("original W destructor canary");
        }
    }
}
#[tokio::test]
async fn concrete_destructor_returns_before_exit_fact_and_panic_never_fabricates_exit() {
    let (_owner, _controller, scope) = bound(0).unwrap();
    let seen = Arc::new(AtomicBool::new(false));
    let gate = ClosingPressureGate::new(
        DropProbe {
            core: Arc::clone(&scope.core),
            saw_unreturned: Arc::clone(&seen),
            panic: false,
        },
        &scope,
    )
    .unwrap();
    drop(gate);
    assert!(seen.load(Ordering::Acquire));
    assert!(scope.snapshot().unwrap().writer_destructor_returned);
    let (_owner, _controller, scope) = bound(0).unwrap();
    let seen = Arc::new(AtomicBool::new(false));
    let gate = ClosingPressureGate::new(
        DropProbe {
            core: Arc::clone(&scope.core),
            saw_unreturned: Arc::clone(&seen),
            panic: true,
        },
        &scope,
    )
    .unwrap();
    let original =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(gate))).unwrap_err();
    assert_eq!(
        original.downcast_ref::<&str>(),
        Some(&"original W destructor canary")
    );
    assert!(seen.load(Ordering::Acquire));
    assert!(!scope.snapshot().unwrap().writer_destructor_returned);
}

struct Scalar<W>(W);
impl<W: AsyncWrite + Unpin> AsyncWrite for Scalar<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}
/// The parent retains original JoinSet handles and TCP until all body outcomes, including panic.
/// This is a real TCP Rows cut test; it never manufactures a real ClosingDelivery.
async fn tcp_cut<W: AsyncWrite + Unpin>(
    make: impl FnOnce(tokio::net::tcp::OwnedWriteHalf) -> W,
) -> io::Result<()> {
    let original_deadline = Instant::now() + Duration::from_secs(20);
    let (owner, controller) = PressureOwner::new(frontend(), original_deadline)?;
    tcp_cut_owned(
        make,
        original_deadline,
        frontend(),
        Arc::new(owner),
        &mut TcpCutController::Kernel(controller),
    )
    .await
}

// Both paths borrow the same original TCP actor and independent wire oracle.
enum TcpCutController {
    Kernel(PressureController),
    #[cfg(novarocks_dev_controls)]
    Authorized(crate::closing_pressure_fixture::ClosingPressureFixture),
}
impl TcpCutController {
    fn arm(&mut self, frontend: FrontendProcessId) -> io::Result<()> {
        match self {
            Self::Kernel(owner) => owner.arm_targets(frontend, inputs()),
            #[cfg(novarocks_dev_controls)]
            Self::Authorized(owner) => owner.arm_targets(frontend, inputs()),
        }
    }
    fn stop(&mut self) {
        match self {
            Self::Kernel(owner) => owner.stop(),
            #[cfg(novarocks_dev_controls)]
            Self::Authorized(owner) => owner.stop(),
        }
    }
}
async fn tcp_cut_owned<W: AsyncWrite + Unpin>(
    make: impl FnOnce(tokio::net::tcp::OwnedWriteHalf) -> W,
    original_deadline: Instant,
    actual_frontend: FrontendProcessId,
    owner: Arc<PressureOwner>,
    controller: &mut TcpCutController,
) -> io::Result<()> {
    let listener =
        tokio::time::timeout_at(original_deadline.into(), TcpListener::bind("127.0.0.1:0"))
            .await
            .map_err(|_| error(Failure::Deadline))??;
    let peer = tokio::time::timeout_at(
        original_deadline.into(),
        TcpStream::connect(listener.local_addr()?),
    )
    .await
    .map_err(|_| error(Failure::Deadline))??;
    let (server, _) = tokio::time::timeout_at(original_deadline.into(), listener.accept())
        .await
        .map_err(|_| error(Failure::Deadline))??;
    drop(listener);
    let mut children = JoinSet::new();
    children.spawn(async move {
        let mut peer = peer;
        let mut scratch = [0u8; 4096];
        let mut count = 0u64;
        let mut digest = Sha256::new();
        loop {
            let n = peer.read(&mut scratch).await?;
            if n == 0 {
                break;
            }
            count = count
                .checked_add(n as u64)
                .ok_or_else(|| error(Failure::Counter))?;
            if count > ROW_CUT {
                return Err(error(Failure::Length));
            }
            digest.update(&scratch[..n]);
        }
        Ok::<_, io::Error>((count, <[u8; 32]>::from(digest.finalize())))
    });
    let (_, write_half) = server.into_split();
    let mut original_writer = None;
    let mut scope_owner = None;
    let primary = {
        let mut body = Box::pin(async {
            controller.arm(actual_frontend)?;
            let scope = owner
                .bind_statement(
                    ClientConnectionToken::new(100, 59).unwrap(),
                    statement(0).session(),
                    statement(0),
                    ORIGINAL_SQL_SHA256,
                )?
                .ok_or_else(|| error(Failure::Identity))?;
            let gate = ClosingPressureGate::new(make(write_half), &scope)?;
            original_writer = Some(OwnedStreamingMysqlWriter::new(
                gate,
                ProtocolLimits::default(),
                4,
            )?);
            scope_owner = Some(scope);
            let scope = scope_owner.as_ref().unwrap();
            let writer = original_writer.as_mut().unwrap();
            scope.begin_rows(statement(0), writer.receipt())?;
            writer.start_row(1_048_580)?;
            {
                let write = async {
                    writer.write_slice(&[0xfd, 0, 0, 16]).await?;
                    let chunk = [b'x'; 4096];
                    for _ in 0..256 {
                        writer.write_slice(&chunk).await?;
                    }
                    writer.flush_pending().await
                };
                tokio::pin!(write);
                tokio::select! {
                    result=&mut write=> { result?; return Err(error(Failure::Transition)); },
                    result=scope.wait_rows_blocked()=> { result?; },
                }
            } // Borrowed original write is dropped; writer/real socket remains in parent.
            let actual = writer.receipt();
            scope.observe_cancel(statement(0), actual)?;
            if actual.committed_wire_bytes != ROW_CUT
                || scope.snapshot()?.accepted_prefix_bytes != ROW_CUT
            {
                return Err(error(Failure::Length));
            }
            Ok(())
        });
        poll_fn(|cx| {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body.as_mut().poll(cx)))
            {
                Ok(poll) => poll,
                Err(original) => {
                    Poll::Ready(Err(io::Error::other(OwnedPanic(Mutex::new(original)))))
                }
            }
        })
        .await
    };
    controller.stop();
    drop(original_writer.take()); // Physical original socket drops before peer join.
    let (delivered, mut cleanup) = settle_original_peer(&mut children, original_deadline).await;
    if let Err(primary) = primary {
        return Err(io::Error::other(OwnedFailure { primary, cleanup }));
    }
    if let Some(primary) = cleanup[0].take().or_else(|| cleanup[1].take()) {
        return Err(io::Error::other(OwnedFailure { primary, cleanup }));
    }
    let (count, digest) = delivered.ok_or_else(|| error(Failure::Transition))?;
    let mut expected = Sha256::new();
    expected.update([4, 0, 16, 4, 0xfd, 0, 0, 16]);
    let chunk = [b'x'; 4096];
    let mut left = ROW_CUT as usize - 8;
    while left != 0 {
        let take = left.min(chunk.len());
        expected.update(&chunk[..take]);
        left -= take;
    }
    if count != ROW_CUT || digest != <[u8; 32]>::from(expected.finalize()) {
        return Err(error(Failure::Length));
    }
    let facts = scope_owner
        .as_ref()
        .ok_or_else(|| error(Failure::Transition))?
        .snapshot()?;
    if !facts.writer_destructor_returned
        || facts.failure.is_some()
        || facts.accepted_prefix_sha256 != digest
    {
        return Err(error(Failure::Transition));
    }
    Ok(())
}
/// Exactly one original peer: at most deadline plus one actual terminal source.
async fn settle_original_peer(
    children: &mut JoinSet<io::Result<(u64, [u8; 32])>>,
    original_deadline: Instant,
) -> (Option<(u64, [u8; 32])>, [Option<io::Error>; 2]) {
    let joined = tokio::time::timeout_at(original_deadline.into(), children.join_next()).await;
    // One original child permits at most a timeout and its actual terminal
    // failure. Keep both owned sources; deadline never substitutes for join.
    let mut cleanup: [Option<io::Error>; 2] = [None, None];
    let mut delivered = None;
    match joined {
        Ok(Some(Ok(Ok(value)))) => delivered = Some(value),
        Ok(Some(Ok(Err(error)))) => cleanup[0] = Some(error),
        Ok(Some(Err(error))) => cleanup[0] = Some(io::Error::other(error)),
        Ok(None) => cleanup[0] = Some(error(Failure::Transition)),
        Err(_) => cleanup[0] = Some(error(Failure::Deadline)),
    }
    children.abort_all();
    while let Some(joined) = children.join_next().await {
        match joined {
            Err(source) if !source.is_cancelled() => cleanup[1] = Some(io::Error::other(source)),
            Ok(Err(source)) => cleanup[1] = Some(source),
            _ => {}
        }
    } // Actual original child handles drained even on failure/expiry; no new cleanup clock.
    (delivered, cleanup)
}
struct OwnedPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Debug for OwnedPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original component panic retained")
    }
}
impl std::fmt::Display for OwnedPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for OwnedPanic {}
struct OwnedFailure {
    primary: io::Error,
    cleanup: [Option<io::Error>; 2],
}
#[tokio::test]
async fn synthetic_complete_inventory_still_refuses_late_original_join_inspection() {
    // State-kernel validation only. No Closing grant or writer is minted here.
    let deadline = Instant::now() + Duration::from_millis(500);
    let (owner, mut controller) = PressureOwner::new(frontend(), deadline).unwrap();
    controller.arm_targets(frontend(), inputs()).unwrap();
    for index in 0..TARGETS {
        let mut state = owner.core.state(index).unwrap();
        state.facts.writer_attached = true;
        state.facts.writer_destructor_returned = true;
        state.facts.cancel_receipt = Some(synthetic_cancel());
        state.facts.phase = if index < CLOSING_TARGETS {
            Phase::Released
        } else {
            Phase::CapacityRefused
        };
        if index == CLOSING_TARGETS {
            state.refusal = Some(WorkError::Capacity("synthetic predicate source"));
        }
    }
    controller.stop();
    tokio::time::sleep_until(deadline.into()).await;
    assert_eq!(
        controller.inspect_after_original_join().unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(
        controller.snapshot(0).unwrap().failure,
        Some(Failure::Deadline)
    );
}

#[derive(Debug)]
struct PollPanic(Arc<()>);
struct PanicWriter(Arc<()>);
impl AsyncWrite for PanicWriter {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        std::panic::panic_any(PollPanic(Arc::clone(&self.0)));
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn original_inner_poll_panic_preserves_payload_and_sticky_failure_until_actual_drop() {
    let (_owner, mut controller, scope) = bound(0).unwrap();
    let identity = Arc::new(());
    let mut gate = ClosingPressureGate::new(PanicWriter(Arc::clone(&identity)), &scope).unwrap();
    let (_, waker) = counted_waker();
    let mut cx = Context::from_waker(&waker);
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Pin::new(&mut gate).poll_write(&mut cx, b"original")
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        &payload.downcast_ref::<PollPanic>().unwrap().0,
        &identity
    ));
    assert_eq!(scope.snapshot().unwrap().failure, Some(Failure::Panic));
    assert!(!scope.core.slots[0].inner_poll.load(Ordering::Acquire));
    assert!(!scope.snapshot().unwrap().writer_destructor_returned);
    controller.stop();
    drop(gate);
    assert!(scope.snapshot().unwrap().writer_destructor_returned);
    assert!(controller.inspect_after_original_join().is_err());
    assert_eq!(scope.snapshot().unwrap().failure, Some(Failure::Panic));
}

#[derive(Debug)]
struct CleanupCanary(Arc<()>);
impl std::fmt::Display for CleanupCanary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cleanup original source canary")
    }
}
impl std::error::Error for CleanupCanary {}
struct CancellationPanic(Arc<()>);
impl Drop for CancellationPanic {
    fn drop(&mut self) {
        std::panic::panic_any(CleanupCanary(Arc::clone(&self.0)));
    }
}
#[tokio::test]
async fn original_cleanup_timeout_and_actual_terminal_panic_remain_distinct_owned_slots() {
    let identity = Arc::new(());
    let (started, ready) = tokio::sync::oneshot::channel();
    let child_identity = Arc::clone(&identity);
    let mut children = JoinSet::new();
    children.spawn(async move {
        let _original = CancellationPanic(child_identity);
        started.send(()).unwrap();
        std::future::pending::<()>().await;
        Ok::<_, io::Error>((0, [0u8; 32]))
    });
    ready.await.unwrap(); // The same original task is live before the clock starts.
    let (delivered, cleanup) =
        settle_original_peer(&mut children, Instant::now() + Duration::from_millis(5)).await;
    assert!(delivered.is_none());
    assert!(children.is_empty()); // Original JoinSet actually drained, not merely aborted.
    let mut failure = OwnedFailure {
        primary: error(Failure::Transition),
        cleanup,
    };
    assert_eq!(
        failure.cleanup[0].as_ref().unwrap().kind(),
        io::ErrorKind::TimedOut
    );
    assert!(!format!("{failure:?} {failure}").contains("cleanup original source canary"));
    let original = failure.cleanup[1]
        .take()
        .unwrap()
        .into_inner()
        .unwrap()
        .downcast::<tokio::task::JoinError>()
        .unwrap();
    assert!(original.is_panic());
    let payload = original.into_panic();
    assert!(Arc::ptr_eq(
        &payload.downcast_ref::<CleanupCanary>().unwrap().0,
        &identity
    ));
}

impl std::fmt::Debug for OwnedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original TCP component failed; cleanup_failure={}",
            self.cleanup.iter().any(Option::is_some)
        )
    }
}
impl std::fmt::Display for OwnedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for OwnedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.primary)
    }
}
#[tokio::test]
async fn actual_tcp_scalar_rows_cut_original_drop_and_peer_join() {
    tcp_cut(Scalar).await.unwrap();
}
#[tokio::test]
async fn actual_tcp_vectored_rows_cut_original_drop_and_peer_join() {
    tcp_cut(|writer| writer).await.unwrap();
}

#[cfg(novarocks_dev_controls)]
mod authorization_tests {
    use super::*;
    use crate::{
        closing_pressure_fixture::ClosingPressureFixture,
        test_fixture_authorization::MysqlFixtureAuthorization,
    };
    use novarocks_failpoint::{
        liveness::{PreparedRoleSession, RunnerSession},
        scope::ProcessBinding,
        session::{DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole},
    };
    use std::{error::Error, os::unix::fs::DirBuilderExt};
    use tokio::net::UnixStream;

    #[derive(Debug)]
    struct OwnedErrors(Vec<io::Error>);
    impl std::fmt::Display for OwnedErrors {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "authorized pressure owner failures: {}", self.0.len())
        }
    }
    impl Error for OwnedErrors {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.0.first().map(|e| e as _)
        }
    }
    #[tokio::test]
    async fn finite_fe_grant_rows_and_control_eof_do_not_credit_closing_and_helpers_join()
    -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(20);
        let frontend = FrontendProcessId::new_v7();
        let root = std::path::PathBuf::from(format!("/tmp/nr-pressure-auth-{}", frontend));
        std::fs::DirBuilder::new().mode(0o700).create(&root)?;
        let socket = root.join("gate.sock");
        let path = root.join("declaration.json");
        let declaration = SessionDeclaration {
            schema_version: DECLARATION_VERSION,
            session_id: "pressure-session".into(),
            launch_id: "pressure-launch".into(),
            role: SessionRole::Fe,
            lifetime_millis: 20_000,
            liveness_socket: root.join("live.sock"),
            handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
            domain_grants: vec![DomainGrant {
                domain: MysqlFixtureAuthorization::DOMAIN.into(),
                grant_id: "pressure-once".into(),
                parameters: serde_json::json!({"action":"closing-pressure", "socket":socket,"nonce_hex":"01010101010101010101010101010101"}),
            }],
        };
        declaration.write_private(&path)?;
        let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)?;
        let mut runner = RunnerSession::start_until(declaration, deadline)?;
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
        let grants = scope.grants_for(MysqlFixtureAuthorization::DOMAIN);
        let mut controller = None;
        let mut expected_disconnect = None;
        let result = {
            let primary = async {
                assert!(MysqlFixtureAuthorization::from_grants(Vec::new())?.is_none());
                assert!(!socket.exists());
                let workload = novarocks_workload_control::WorkloadControl::try_new_counted(
                    novarocks_workload_control::WorkloadConfig::default(),
                )
                .map_err(io::Error::other)?
                .owner;
                let mut fixture = ClosingPressureFixture::bind_authorized(
                    MysqlFixtureAuthorization::from_grants(grants.clone())?.unwrap(),
                    frontend,
                    workload.observation(),
                )
                .map_err(io::Error::other)?;
                assert!(socket.exists());
                assert!(
                    ClosingPressureFixture::bind_authorized(
                        MysqlFixtureAuthorization::from_grants(grants.clone())?.unwrap(),
                        frontend,
                        workload.observation()
                    )
                    .is_err()
                );
                let cancellation = fixture.control_cancellation().unwrap();
                assert!(cancellation.deadline() <= deadline);
                let binding = fixture.listener_binding().map_err(io::Error::other)?;
                controller = Some(TcpCutController::Authorized(fixture));
                // Reuse the original actor, actual peer join, wire digest and physical writer destructor oracle.
                tcp_cut_owned(
                    |writer| writer,
                    deadline,
                    frontend,
                    Arc::clone(&binding.owner),
                    controller.as_mut().unwrap(),
                )
                .await?;
                assert_eq!(
                    scope
                        .observations()
                        .iter()
                        .filter(|o| o.event == "hit")
                        .count(),
                    0,
                    "Rows cutoff alone must never credit the Closing grant"
                );
                let TcpCutController::Authorized(fixture) = controller.as_mut().unwrap() else {
                    unreachable!()
                };
                let peer = async {
                    let mut peer = UnixStream::connect(&socket).await?;
                    let mut body = b"NRCL65\0\x01".to_vec();
                    body.push(2);
                    body.extend_from_slice(&[1; 16]);
                    body.extend_from_slice(&frontend.to_bytes());
                    body.extend_from_slice(&1u16.to_le_bytes());
                    body.push(0);
                    peer.write_all(&(body.len() as u32).to_le_bytes()).await?;
                    peer.write_all(&body).await?;
                    let mut length = [0; 4];
                    peer.read_exact(&mut length).await?;
                    let length = u32::from_le_bytes(length) as usize;
                    if !(10..=4092).contains(&length) {
                        return Err(io::Error::other("pressure reply exceeded original bound"));
                    }
                    let mut reply = vec![0; length];
                    peer.read_exact(&mut reply).await?;
                    if &reply[..8] != b"NRCL65\0\x01" || reply[8..10] != [2, 0] {
                        return Err(io::Error::other("original pressure snapshot refused"));
                    }
                    drop(peer); // Real original control EOF, never a fabricated disconnect DTO.
                    Ok::<_, io::Error>(())
                };
                let peer = async {
                    tokio::time::timeout_at(deadline.into(), peer)
                        .await
                        .map_err(|_| error(Failure::Deadline))?
                };
                let (control, peer) = tokio::join!(fixture.run_control(), peer);
                if let Err(primary) = peer {
                    let mut failures = vec![primary];
                    if let Err(error) = control {
                        failures.push(io::Error::other(error));
                    }
                    return Err(io::Error::other(OwnedErrors(failures)));
                }
                let disconnect =
                    control.expect_err("control EOF must remain an actual owner failure");
                let original = disconnect
                    .source()
                    .ok_or_else(|| io::Error::other("control disconnect source missing"))?;
                if !original.to_string().contains("class=Eof") {
                    return Err(io::Error::other(disconnect));
                }
                eprintln!(
                    "expected original pressure disconnect retained: {disconnect}; original_source={original}"
                );
                runner.end_and_join()?;
                cancellation.cancelled().await;
                assert!(!cancellation.is_active());
                drop(binding);
                expected_disconnect = Some(disconnect);
                Ok::<_, io::Error>(())
            };
            tokio::pin!(primary);
            poll_fn(|cx| {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    primary.as_mut().poll(cx)
                })) {
                    Ok(poll) => poll,
                    Err(original) => {
                        Poll::Ready(Err(io::Error::other(OwnedPanic(Mutex::new(original)))))
                    }
                }
            })
            .await
        };
        let mut failures = Vec::new();
        if let Err(error) = result {
            failures.push(error);
        }
        if let Some(TcpCutController::Authorized(fixture)) = controller.as_mut() {
            if !failures.is_empty() {
                fixture.fail_and_stop();
            }
            if let Err(error) = fixture.close_control() {
                failures.push(io::Error::other(error));
            }
        }
        if let Err(error) = runner.end_and_join() {
            failures.push(error);
        }
        scope.wait_closed();
        if let Err(error) = role.close_and_join() {
            failures.push(error);
        }
        drop(controller);
        for (event, expected) in [("claimed", 1), ("hit", 0), ("claim-released", 1)] {
            if scope
                .observations()
                .iter()
                .filter(|o| o.event == event)
                .count()
                != expected
            {
                failures.push(io::Error::other(format!(
                    "Rows-only pressure action did not record {expected} {event}"
                )));
            }
        }
        if socket.exists() || root.join("live.sock").exists() {
            failures.push(io::Error::other("original pressure owner socket remained"));
        }
        if let Err(error) = std::fs::remove_file(runner.process_receipt_path()) {
            failures.push(error);
        }
        if let Err(error) = std::fs::remove_file(path.with_extension("claimed")) {
            failures.push(error);
        }
        if let Err(error) = std::fs::remove_file(path) {
            failures.push(error);
        }
        if let Err(error) = std::fs::remove_dir(root) {
            failures.push(error);
        }
        if failures.is_empty() {
            Ok(())
        } else {
            if let Some(original) = expected_disconnect {
                failures.push(io::Error::other(original));
            }
            Err(io::Error::other(OwnedErrors(failures)))
        }
    }
}
