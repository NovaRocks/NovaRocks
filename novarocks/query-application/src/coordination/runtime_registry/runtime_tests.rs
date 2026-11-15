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

//! Port-seam tests: the fake reports explicit original Scope exits. It is not
//! evidence that remote closure or a Ready decision implies actual exit.
use super::runtime_installed::RuntimeReplyValue;
use super::*;
use crate::api::logical_runtime::{
    LogicalFactSource, LogicalFuture, LogicalSourceClosed, LogicalSourceRefusal,
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, ExecutionEffect, IdSeed, MonotonicInstant, RecoveryMode,
};
use novarocks_logical_execution_protocol::{
    LogicalSourceId, cancel::LogicalCancellationReason, start::*,
};
use novarocks_types::identity::AttemptId;
use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    task::{Context, Poll},
    time::Duration,
};

#[derive(Default)]
struct PortState {
    sources: usize,
    allow_fact: bool,
    readiness: Option<LogicalJoinReadiness>,
    exit: Option<LogicalExit>,
    facts: Vec<LogicalHostFact>,
    closes: Vec<novarocks_logical_execution_protocol::input::RegistryClose>,
    readiness_requests: Vec<bool>,
    owner_failed: bool,
    failure_ingress_closed: bool,
}
#[derive(Default)]
struct Port {
    state: Mutex<PortState>,
    wake: futures::task::AtomicWaker,
    exit_wake: futures::task::AtomicWaker,
}
impl Port {
    fn progress(&self) {
        self.exit_wake.wake();
        self.wake.wake();
    }
}
struct Source(Arc<Port>);
impl LogicalFactSource for Source {
    fn id(&self) -> LogicalSourceId {
        LogicalSourceId::new(NonZeroU64::new(1).unwrap())
    }
    fn send(
        &mut self,
        fact: LogicalHostFact,
    ) -> LogicalFuture<'_, Result<(), LogicalSourceClosed>> {
        Box::pin(async move {
            std::future::poll_fn(|cx| {
                self.0.wake.register(cx.waker());
                let state = self.0.state.lock().unwrap();
                if state.failure_ingress_closed {
                    Poll::Ready(Err(LogicalSourceClosed))
                } else if state.allow_fact {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            })
            .await?;
            self.0.state.lock().unwrap().facts.push(fact);
            Ok(())
        })
    }
}
struct Installed(Arc<Port>);
impl InstalledRuntime for Installed {
    fn source(
        &self,
        kind: LogicalSourceKind,
    ) -> Result<Box<dyn LogicalFactSource>, LogicalSourceRefusal> {
        assert_eq!(kind, LogicalSourceKind::Registry);
        let mut state = self.0.state.lock().unwrap();
        if state.sources != 0 {
            return Err(LogicalSourceRefusal::AlreadyTaken);
        }
        state.sources += 1;
        Ok(Box::new(Source(self.0.clone())))
    }
    fn request_close(&self, close: novarocks_logical_execution_protocol::input::RegistryClose) {
        self.0.state.lock().unwrap().closes.push(close);
    }
    fn join_readiness(
        &self,
        close: bool,
    ) -> LogicalFuture<'_, Result<LogicalJoinReadiness, LogicalSourceClosed>> {
        Box::pin(async move {
            self.0.state.lock().unwrap().readiness_requests.push(close);
            std::future::poll_fn(|cx| {
                self.0.wake.register(cx.waker());
                self.0
                    .state
                    .lock()
                    .unwrap()
                    .readiness
                    .map_or(Poll::Pending, |v| Poll::Ready(Ok(v)))
            })
            .await
        })
    }
    fn begin_owner_failed_registry_close(&self) -> Result<(), LogicalSourceClosed> {
        {
            let mut state = self.0.state.lock().unwrap();
            if !state.owner_failed {
                return Err(LogicalSourceClosed);
            }
            state.failure_ingress_closed = true;
        }
        self.request_close(novarocks_logical_execution_protocol::input::RegistryClose::Retire);
        self.0.progress();
        Ok(())
    }
    fn poll_owner_failed_registry_closed(
        &self,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), LogicalSourceClosed>> {
        if self.0.state.lock().unwrap().failure_ingress_closed {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn poll_exit(&self, cx: &mut Context<'_>) -> Poll<LogicalExit> {
        self.0.exit_wake.register(cx.waker());
        self.0
            .state
            .lock()
            .unwrap()
            .exit
            .clone()
            .map_or(Poll::Pending, Poll::Ready)
    }
}
fn execution() -> QueryExecutionId {
    QueryExecutionId::new(QueryId::new(81, 7), AttemptId::new(1).unwrap()).unwrap()
}
fn start(actor: LogicalExecutionActorId) -> LogicalStart {
    LogicalStart {
        identity: execution().query_id(),
        actor_instance: actor,
        policy: LogicalPolicy {
            recovery: RecoveryMode::NoRecovery,
            effect: ExecutionEffect::None,
            output: LogicalOutputMode::CompletionOnly,
            finish: LogicalFinishBoundary::TaskCompletion,
            retain_diagnostics: false,
            root_carrier: None,
            internal_plan: None,
        },
        budgets: LogicalBudgets {
            max_attempts: NonZeroU32::new(1).unwrap(),
            replacement_reservation_valid_for: None,
            remote_cleanup_timeout: Duration::from_secs(1),
            admission_issues_per_participation: NonZeroUsize::new(1).unwrap(),
            establish_per_participation: NonZeroUsize::new(1).unwrap(),
            abort_per_participation: NonZeroUsize::new(1).unwrap(),
            observation: ObservationBudgets {
                diagnostics: Duration::from_secs(1),
                convergence: Duration::from_secs(1),
            },
        },
        first_attempt: InitialAttempt {
            activation: AttemptActivationIdentity::from_parts(actor.get(), execution(), 1),
            participations: Box::new([]),
        },
        statement_deadline: None,
        id_seed: IdSeed::new(1, 1),
        started_at: MonotonicInstant::from_origin(Duration::ZERO),
    }
}
fn carrier() -> (Arc<RuntimeCarrier>, Arc<Port>) {
    let port = Arc::new(Port::default());
    let installed = Arc::new(Installed(port.clone()));
    let source = installed.source(LogicalSourceKind::Registry).unwrap();
    (
        RuntimeCarrier::new(installed, source, std::sync::Weak::new()),
        port,
    )
}

#[tokio::test]
async fn cancelled_waiter_keeps_original_pending_send_and_fifo() {
    let (carrier, port) = carrier();
    let first = carrier.enqueue_fact(LogicalHostFact::Cancelled(
        LogicalCancellationReason::Requested,
    ));
    let mut waiter = Box::pin(std::future::poll_fn(|cx| carrier.poll_reply(&first, cx)));
    assert!(futures::poll!(&mut waiter).is_pending());
    drop(waiter);
    assert!(!carrier.source_idle());
    let second = carrier.enqueue_fact(LogicalHostFact::Cancelled(
        LogicalCancellationReason::ServerShutdown,
    ));
    port.state.lock().unwrap().allow_fact = true;
    port.progress();
    assert!(
        std::future::poll_fn(|cx| carrier.poll_reply(&first, cx))
            .await
            .is_ok()
    );
    assert!(
        std::future::poll_fn(|cx| carrier.poll_reply(&second, cx))
            .await
            .is_ok()
    );
    assert!(carrier.source_idle());
    let state = port.state.lock().unwrap();
    assert_eq!(state.sources, 1);
    assert!(matches!(
        state.facts.as_slice(),
        [
            LogicalHostFact::Cancelled(LogicalCancellationReason::Requested),
            LogicalHostFact::Cancelled(LogicalCancellationReason::ServerShutdown)
        ]
    ));
}

#[tokio::test]
async fn readiness_waits_exact_original_reply_and_preserves_borrowed_wait_cancellation() {
    let (carrier, port) = carrier();
    let reply = carrier.enqueue_readiness(false);
    let mut waiter = Box::pin(std::future::poll_fn(|cx| carrier.poll_reply(&reply, cx)));
    assert!(futures::poll!(&mut waiter).is_pending());
    drop(waiter);
    assert_eq!(port.state.lock().unwrap().readiness_requests, [false]);
    port.state.lock().unwrap().readiness = Some(LogicalJoinReadiness::WaitingForConvergence);
    port.progress();
    assert!(matches!(
        std::future::poll_fn(|cx| carrier.poll_reply(&reply, cx)).await,
        Ok(runtime_installed::RuntimeReplyValue::Readiness(
            LogicalJoinReadiness::WaitingForConvergence
        ))
    ));
    assert_eq!(port.state.lock().unwrap().readiness_requests, [false]);
    assert!(carrier.source_idle());
}

#[tokio::test]
async fn waiting_readiness_does_not_finish_ingress_before_actual_exit() {
    let (carrier, port) = carrier();
    port.state.lock().unwrap().readiness = Some(LogicalJoinReadiness::WaitingForConvergence);
    let mut wait = Box::pin(carrier.clone().wait_ingress());
    assert!(futures::poll!(&mut wait).is_pending());
    port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    port.progress();
    wait.await;
    assert_eq!(port.state.lock().unwrap().readiness_requests, [true]);
    assert_eq!(
        std::future::poll_fn(|cx| carrier.poll_exit(cx)).await,
        LogicalExit::Completed
    );
}

#[tokio::test]
async fn runtime_install_retains_same_registration_and_requires_actual_exit_for_join() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let mut reservation = handle.reserve(execution(), vec![]).unwrap();
    let registration = reservation.runtime_registration();
    assert!(Arc::ptr_eq(
        &registration,
        &reservation.runtime_registration()
    ));
    let start = start(reservation.actor_instance());
    let port = Arc::new(Port::default());
    port.state.lock().unwrap().readiness = Some(LogicalJoinReadiness::Ready);
    let runtime: Arc<dyn InstalledRuntime> = Arc::new(Installed(port.clone()));
    let installed = reservation
        .install_runtime(&start, runtime.clone())
        .unwrap();
    assert!(Arc::ptr_eq(&registration, &installed.registration));
    assert!(Arc::ptr_eq(&runtime, &installed.installed));
    assert_eq!(
        handle.join_readiness(&registration).await.unwrap(),
        LogicalExecutionJoinReadiness::Ready
    );
    let mut borrowed = LogicalExecutionRegistration {
        registry_id: registration.registry_id,
        initial_execution: registration.initial_execution,
        generation: registration.generation,
    };
    let mut joining = Box::pin(handle.join(&mut borrowed));
    assert!(futures::poll!(&mut joining).is_pending());
    drop(joining);
    let error = crate::api::QueryExecutionError::new(
        crate::api::QueryExecutionErrorKind::Failed,
        "original runtime owner failed",
    );
    port.state.lock().unwrap().exit = Some(LogicalExit::OwnerFailed(error));
    port.progress();
    let receipt = handle.join(&mut borrowed).await.unwrap();
    assert_eq!(
        receipt.outcome(),
        LogicalExecutionJoinOutcome::RuntimeOwnerFailed
    );
    handle.retire(receipt).unwrap();
    assert_eq!(port.state.lock().unwrap().sources, 1);
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn invalid_install_returns_original_reservation_without_taking_source() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let mut reservation = handle.reserve(execution(), vec![]).unwrap();
    let registration = reservation.runtime_registration();
    let port = Arc::new(Port::default());
    let refusal = reservation
        .install_runtime(
            &start(LogicalExecutionActorId::from_raw(999)),
            Arc::new(Installed(port.clone())),
        )
        .err()
        .unwrap();
    assert_eq!(
        refusal.error,
        LogicalExecutionRuntimeRegistryError::ConfigDoesNotMatchReservation
    );
    let mut reservation = refusal.reservation;
    assert!(Arc::ptr_eq(
        &registration,
        &reservation.runtime_registration()
    ));
    assert_eq!(port.state.lock().unwrap().sources, 0);
    drop(reservation);
    drop(refusal.installed);
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn dropping_latest_waiter_does_not_steal_previous_original_wakeup() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Flag(AtomicBool);
    impl std::task::Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }
    let (carrier, port) = carrier();
    let first = carrier.enqueue_fact(LogicalHostFact::Cancelled(
        LogicalCancellationReason::Requested,
    ));
    let second = carrier.enqueue_fact(LogicalHostFact::Cancelled(
        LogicalCancellationReason::ServerShutdown,
    ));
    let flag = Arc::new(Flag(AtomicBool::new(false)));
    let waker = std::task::Waker::from(flag.clone());
    let mut cx = Context::from_waker(&waker);
    let mut first_wait = Box::pin(carrier.wait_reply(&first));
    assert!(first_wait.as_mut().poll(&mut cx).is_pending());
    let mut second_wait = Box::pin(carrier.wait_reply(&second));
    let noop = futures::task::noop_waker();
    let mut last_cx = Context::from_waker(&noop);
    assert!(second_wait.as_mut().poll(&mut last_cx).is_pending());
    drop(second_wait);
    flag.0.store(false, Ordering::Release);
    port.state.lock().unwrap().allow_fact = true;
    port.progress();
    assert!(
        flag.0.load(Ordering::Acquire),
        "the earlier standing waiter must receive the original source wake"
    );
    assert!(first_wait.await.is_ok());
    assert!(carrier.wait_reply(&second).await.is_ok());
}

#[tokio::test]
async fn successor_convergence_uses_exact_activation_beyond_initial_empty_roster() {
    use novarocks_types::identity::{BackendProcessId, FrontendProcessId};
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let reservation = handle.reserve(execution(), vec![]).unwrap();
    let start = start(reservation.actor_instance());
    let port = Arc::new(Port::default());
    {
        let mut state = port.state.lock().unwrap();
        state.allow_fact = true;
        state.readiness = Some(LogicalJoinReadiness::Ready);
    }
    let installed = reservation
        .install_runtime(&start, Arc::new(Installed(port.clone())))
        .unwrap();
    let successor =
        QueryExecutionId::new(execution().query_id(), AttemptId::new(2).unwrap()).unwrap();
    let activation =
        AttemptActivationIdentity::from_parts(start.actor_instance.get(), successor, 2);
    handle
        .register_runtime_activation(&installed.registration, activation)
        .unwrap();
    let mut front = [1; 16];
    front[6] = 0x71;
    front[8] = 0x81;
    for tag in [2, 3] {
        let mut back = [tag; 16];
        back[6] = 0x71;
        back[8] = 0x81;
        let participation = ParticipationRef::new(
            successor,
            FrontendProcessId::try_from_bytes(front).unwrap(),
            BackendProcessId::try_from_bytes(back).unwrap(),
        );
        handle
            .observe_participation_never_established(&installed.registration, participation)
            .await
            .unwrap();
    }
    {
        let state = port.state.lock().unwrap();
        assert_eq!(state.facts.len(), 2);
        assert!(state.facts.iter().all(|fact| matches!(fact, LogicalHostFact::RegistryParticipation { activation: found, .. } if *found == activation)));
    }
    port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    let mut borrowed = LogicalExecutionRegistration {
        registry_id: installed.registration.registry_id,
        initial_execution: installed.registration.initial_execution,
        generation: installed.registration.generation,
    };
    let receipt = handle.join(&mut borrowed).await.unwrap();
    handle.retire(receipt).unwrap();
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn ordinary_registry_source_wake_never_retires_a_live_role() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let original = OriginalLogicalExit::new();
    let reservation = handle
        .reserve_with_original_exit(execution(), vec![], Some(original))
        .unwrap();
    let start = start(reservation.actor_instance());
    let port = Arc::new(Port::default());
    port.state.lock().unwrap().readiness = Some(LogicalJoinReadiness::WaitingForConvergence);
    let installed = reservation
        .install_runtime(&start, Arc::new(Installed(port.clone())))
        .unwrap();
    let carrier = handle
        .runtime_carrier(&installed.registration)
        .unwrap()
        .unwrap();
    let reply = carrier.enqueue_readiness(false);
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut cursor = OriginalExitCursor::default();
    assert!(
        handle
            .poll_original_exit_turn(&mut cursor, NonZeroUsize::new(4).unwrap(), &mut cx)
            .is_pending()
    );
    assert!(carrier.wait_reply(&reply).await.is_ok());
    assert!(
        port.state.lock().unwrap().closes.is_empty(),
        "normal source progress cannot manufacture cancellation"
    );
    port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    let mut borrowed = LogicalExecutionRegistration {
        registry_id: installed.registration.registry_id,
        initial_execution: installed.registration.initial_execution,
        generation: installed.registration.generation,
    };
    let receipt = handle.join(&mut borrowed).await.unwrap();
    handle.retire(receipt).unwrap();
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn shutdown_during_reservation_reaches_same_installed_original() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let reservation = handle.reserve(execution(), vec![]).unwrap();
    let start = start(reservation.actor_instance());
    handle.begin_shutdown();
    let port = Arc::new(Port::default());
    {
        let mut state = port.state.lock().unwrap();
        state.readiness = Some(LogicalJoinReadiness::Ready);
        state.exit = Some(LogicalExit::Completed);
    }
    let installed = reservation
        .install_runtime(&start, Arc::new(Installed(port.clone())))
        .unwrap();
    assert!(matches!(
        port.state.lock().unwrap().closes.as_slice(),
        [novarocks_logical_execution_protocol::input::RegistryClose::Shutdown]
    ));
    let mut borrowed = LogicalExecutionRegistration {
        registry_id: installed.registration.registry_id,
        initial_execution: installed.registration.initial_execution,
        generation: installed.registration.generation,
    };
    let receipt = handle.join(&mut borrowed).await.unwrap();
    handle.retire(receipt).unwrap();
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

fn install_for(
    handle: &LogicalExecutionRuntimeRegistryHandle,
    execution: QueryExecutionId,
    participations: Vec<ParticipationRef>,
) -> (Arc<LogicalExecutionRegistration>, Arc<Port>) {
    let reservation = handle.reserve(execution, participations.clone()).unwrap();
    let mut start = start(reservation.actor_instance());
    start.identity = execution.query_id();
    start.first_attempt.activation =
        AttemptActivationIdentity::from_parts(start.actor_instance.get(), execution, 1);
    start.first_attempt.participations = participations.into_boxed_slice();
    let port = Arc::new(Port::default());
    port.state.lock().unwrap().readiness = Some(LogicalJoinReadiness::Ready);
    let installed = reservation
        .install_runtime(&start, Arc::new(Installed(port.clone())))
        .unwrap();
    (installed.registration, port)
}
fn borrowed(registration: &LogicalExecutionRegistration) -> LogicalExecutionRegistration {
    LogicalExecutionRegistration {
        registry_id: registration.registry_id,
        initial_execution: registration.initial_execution,
        generation: registration.generation,
    }
}

#[tokio::test]
async fn repeated_ingress_close_reuses_the_original_accepted_ready_reply() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let (registration, port) = install_for(&handle, execution(), vec![]);
    handle.close_ingress_for_join(&registration).await.unwrap();
    // A closed original source cannot be required to mint a second reply.
    port.state.lock().unwrap().readiness = None;
    handle.close_ingress_for_join(&registration).await.unwrap();
    assert_eq!(port.state.lock().unwrap().readiness_requests, [true]);
    port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    let receipt = handle.join(&mut borrowed(&registration)).await.unwrap();
    handle.retire(receipt).unwrap();
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn shutdown_timeout_and_cancelled_wait_keep_same_runtime_registration_until_exit() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let (registration, port) = install_for(&handle, execution(), vec![]);
    assert_eq!(
        registry.shutdown_until(Instant::now()).await,
        Err(LogicalExecutionRuntimeShutdownError::DeadlineExceeded {
            remaining_entries: 1
        })
    );
    assert_eq!(handle.entry_count(), 1);
    assert!(
        handle
            .reserve(
                QueryExecutionId::new(QueryId::new(99, 1), AttemptId::new(1).unwrap()).unwrap(),
                vec![]
            )
            .is_err()
    );
    let mut shutdown = Box::pin(registry.shutdown_until(Instant::now() + Duration::from_secs(60)));
    assert!(futures::poll!(&mut shutdown).is_pending());
    drop(shutdown);
    assert_eq!(handle.entry_count(), 1);
    let same = handle.runtime_carrier(&registration).unwrap().unwrap();
    {
        let state = handle.lock();
        assert!(
            matches!(state.entries.get(&execution().query_id()), Some(RegistryEntry::RuntimeInstalled(entry)) if Arc::ptr_eq(&registration, &entry.registration) && Arc::ptr_eq(&same, &entry.carrier))
        );
    }
    port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    port.progress();
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(handle.entry_count(), 0);
    assert_eq!(port.state.lock().unwrap().sources, 1);
}

#[tokio::test]
async fn expired_shutdown_broadcasts_to_all_originals_despite_blocked_registry_source() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let (first, first_port) = install_for(&handle, execution(), vec![]);
    let other = QueryExecutionId::new(QueryId::new(81, 8), AttemptId::new(1).unwrap()).unwrap();
    let (_second, second_port) = install_for(&handle, other, vec![]);
    let carrier = handle.runtime_carrier(&first).unwrap().unwrap();
    let _pending = carrier.enqueue_readiness(false);
    first_port.state.lock().unwrap().readiness = None;
    let waker = futures::task::noop_waker();
    carrier.poll_source(&mut Context::from_waker(&waker));
    assert_eq!(
        registry.shutdown_until(Instant::now()).await,
        Err(LogicalExecutionRuntimeShutdownError::DeadlineExceeded {
            remaining_entries: 2
        })
    );
    for port in [&first_port, &second_port] {
        assert!(
            port.state
                .lock()
                .unwrap()
                .closes
                .contains(&novarocks_logical_execution_protocol::input::RegistryClose::Shutdown)
        );
    }
    for port in [&first_port, &second_port] {
        let mut state = port.state.lock().unwrap();
        state.readiness = Some(LogicalJoinReadiness::Ready);
        state.exit = Some(LogicalExit::Completed);
        drop(state);
        port.progress();
    }
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(handle.entry_count(), 0);
}

#[tokio::test]
async fn runtime_receipt_refusal_returns_original_proof_and_stale_generation_is_rejected() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let foreign = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let (registration, port) = install_for(&handle, execution(), vec![]);
    port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    let receipt = handle.join(&mut borrowed(&registration)).await.unwrap();
    let (error, receipt) = foreign.handle().retire(receipt).unwrap_err();
    assert_eq!(error, LogicalExecutionRuntimeRegistryError::ForeignRegistry);
    assert_eq!(receipt.generation, registration.generation);
    assert_eq!(handle.entry_count(), 1);
    handle.retire(receipt).unwrap();
    let successor =
        QueryExecutionId::new(execution().query_id(), AttemptId::new(2).unwrap()).unwrap();
    let (new, port) = install_for(&handle, successor, vec![]);
    assert_eq!(
        handle.runtime_carrier(&registration).unwrap_err(),
        LogicalExecutionRuntimeRegistryError::StaleGeneration
    );
    port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    let receipt = handle.join(&mut borrowed(&new)).await.unwrap();
    handle.retire(receipt).unwrap();
    foreign.shutdown().unwrap();
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[derive(Debug)]
struct HeldNative(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for HeldNative {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl crate::api::ActiveNativeAttemptOwner for HeldNative {
    fn run<'a>(
        &'a mut self,
        _drive: &'a crate::coordination::NativeAttemptDrive,
        _cancellation: novarocks_workload_control::CancellationView,
    ) -> crate::api::NativeAttemptRunFuture<'a> {
        Box::pin(async { panic!("fixture returns original closure explicitly") })
    }
    fn converge<'a>(
        &'a mut self,
        _request: crate::api::NativeActiveConvergenceRequest,
    ) -> crate::api::NativeActiveAttemptConvergenceFuture<'a> {
        Box::pin(async { panic!("fixture returns original closure explicitly") })
    }
}
fn participation(execution: QueryExecutionId) -> ParticipationRef {
    ParticipationRef::new(
        execution,
        novarocks_types::identity::FrontendProcessId::new_v7(),
        novarocks_types::identity::BackendProcessId::new_v7(),
    )
}

#[tokio::test]
async fn runtime_exit_and_ready_cannot_retire_held_original_native_custody() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let participation = participation(execution());
    let (registration, port) = install_for(&handle, execution(), vec![participation]);
    port.state.lock().unwrap().allow_fact = true;
    port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut original = handle
        .reserve_active_native_owner(&registration, execution(), &[participation])
        .unwrap()
        .bind(Box::new(HeldNative(drops.clone())));
    assert_eq!(
        handle.join_readiness(&registration).await.unwrap(),
        LogicalExecutionJoinReadiness::WaitingForConvergence
    );
    let mut borrowed = borrowed(&registration);
    let mut joined = Box::pin(handle.join(&mut borrowed));
    assert!(futures::poll!(&mut joined).is_pending());
    // The logical map row and its separate original Native custody slot are
    // two still-owned records. Neither may disappear before its own receipt.
    {
        let state = handle.lock();
        assert_eq!(state.entries.len(), 1);
        assert!(
            matches!(state.entries.get(&execution().query_id()), Some(RegistryEntry::RuntimeInstalled(entry)) if Arc::ptr_eq(&entry.registration, &registration))
        );
    }
    assert_eq!(handle.active_native_custody_count(), 1);
    assert_eq!(handle.entry_count(), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    original
        .record_and_close(crate::api::NativeActiveConvergenceClosure::Evidence(
            crate::api::NativeAttemptConvergence::participations(vec![
                crate::api::NativeParticipationConvergence::never_established(participation),
            ]),
        ))
        .await
        .unwrap();
    let receipt = joined.await.unwrap();
    assert_eq!(handle.active_native_custody_count(), 0);
    assert_eq!(handle.lock().entries.len(), 1);
    assert_eq!(handle.entry_count(), 1);
    handle.retire(receipt).unwrap();
    assert_eq!(handle.entry_count(), 0);
    drop(original);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(handle.active_native_custody_count(), 0);
    assert!(
        matches!(port.state.lock().unwrap().facts.as_slice(), [LogicalHostFact::RegistryParticipation { participation: found, .. }] if *found == participation)
    );
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn unknown_original_native_custody_keeps_box_while_healthy_runtime_really_retires() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let participation = participation(execution());
    let (registration, port) = install_for(&handle, execution(), vec![participation]);
    port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let original = handle
        .reserve_active_native_owner(&registration, execution(), &[participation])
        .unwrap()
        .bind(Box::new(HeldNative(drops.clone())));
    drop(original);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let other = QueryExecutionId::new(QueryId::new(81, 8), AttemptId::new(1).unwrap()).unwrap();
    let (healthy, healthy_port) = install_for(&handle, other, vec![]);
    healthy_port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    let receipt = handle.join(&mut borrowed(&healthy)).await.unwrap();
    handle.retire(receipt).unwrap();
    {
        let state = handle.lock();
        assert_eq!(state.entries.len(), 1);
        assert!(
            matches!(state.entries.get(&execution().query_id()), Some(RegistryEntry::RuntimeInstalled(entry)) if Arc::ptr_eq(&entry.registration, &registration))
        );
        assert!(!state.entries.contains_key(&other.query_id()));
    }
    assert_eq!(handle.active_native_custody_count(), 1);
    assert_eq!(handle.entry_count(), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(
        handle
            .active_native_registration_health(&registration)
            .is_some()
    );
    registry.abandon_for_process_exit();
    assert_eq!(handle.lock().entries.len(), 0);
    assert_eq!(handle.active_native_custody_count(), 0);
    assert_eq!(handle.entry_count(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn completed_install_role_cannot_retire_running_runtime_before_exact_core_readiness() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let original = OriginalLogicalExit::new();
    let reservation = handle
        .reserve_with_original_exit(execution(), vec![], Some(original.clone()))
        .unwrap();
    let start = start(reservation.actor_instance());
    let port = Arc::new(Port::default());
    port.state.lock().unwrap().readiness = Some(LogicalJoinReadiness::WaitingForConvergence);
    let installed = reservation
        .install_runtime(&start, Arc::new(Installed(port.clone())))
        .unwrap();
    original.role_ended_silent(false);
    assert!(
        original.pending_actor(),
        "the old role-owned cleanup predicate is insufficient for an install-only role"
    );
    assert!(
        !handle.original_exit_pending(),
        "the install role cannot block supervisor shutdown on logical business completion"
    );
    handle.mark_original_exit_ready();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut cursor = OriginalExitCursor::default();
    for _ in 0..4 {
        let _ = handle.poll_original_exit_turn(&mut cursor, NonZeroUsize::new(4).unwrap(), &mut cx);
    }
    assert!(port.state.lock().unwrap().closes.is_empty());
    assert_eq!(
        port.state.lock().unwrap().readiness_requests,
        [false],
        "a false core reply must not create a polling loop"
    );
    assert_eq!(handle.entry_count(), 1);
    // This is an explicit protocol-ready transition, not the installation
    // role's earlier exit. The real handle must wake its registered observer.
    port.state.lock().unwrap().readiness = Some(LogicalJoinReadiness::Ready);
    port.progress();
    for _ in 0..4 {
        let _ = handle.poll_original_exit_turn(&mut cursor, NonZeroUsize::new(4).unwrap(), &mut cx);
    }
    assert_eq!(
        port.state.lock().unwrap().readiness_requests,
        [false, false, true]
    );
    assert!(matches!(
        port.state.lock().unwrap().closes.as_slice(),
        [novarocks_logical_execution_protocol::input::RegistryClose::Retire]
    ));
    assert_eq!(
        handle.entry_count(),
        1,
        "core readiness and ingress closure do not prove actual Scope exit"
    );
    assert!(
        matches!(handle.lock().entries.get(&execution().query_id()), Some(RegistryEntry::RuntimeInstalled(entry)) if Arc::ptr_eq(&entry.registration, &installed.registration))
    );
    port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    port.progress();
    for _ in 0..4 {
        let _ = handle.poll_original_exit_turn(&mut cursor, NonZeroUsize::new(4).unwrap(), &mut cx);
    }
    assert_eq!(handle.entry_count(), 0);
    assert!(!original.pending_actor());
    assert!(!original.has_failure());
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn natural_service_cannot_steal_cancelled_explicit_join_receipt() {
    let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let handle = registry.handle();
    let (registration, port) = install_for(&handle, execution(), vec![]);
    let mut borrowed = borrowed(&registration);
    let mut joining = Box::pin(handle.join(&mut borrowed));
    assert!(futures::poll!(&mut joining).is_pending());
    drop(joining);
    port.state.lock().unwrap().exit = Some(LogicalExit::Completed);
    port.progress();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut cursor = OriginalExitCursor::default();
    for _ in 0..4 {
        let _ = handle.poll_original_exit_turn(&mut cursor, NonZeroUsize::new(4).unwrap(), &mut cx);
    }
    assert_eq!(handle.entry_count(), 1);
    assert!(
        matches!(handle.lock().entries.get(&execution().query_id()), Some(RegistryEntry::RuntimeInstalled(entry)) if Arc::ptr_eq(&entry.registration, &registration))
    );
    assert_eq!(
        port.state.lock().unwrap().readiness_requests,
        [true],
        "service must not mint another readiness request for the explicit borrower"
    );
    let receipt = handle.join(&mut borrowed).await.unwrap();
    assert_eq!(receipt.generation, registration.generation);
    handle.retire(receipt).unwrap();
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn failed_owner_ingress_close_returns_original_source_without_waiting_for_actor_exit_or_join()
{
    let (carrier, port) = carrier();
    let first = carrier.enqueue_fact(LogicalHostFact::Cancelled(
        LogicalCancellationReason::ServerShutdown,
    ));
    let mut pending = Box::pin(carrier.wait_reply(&first));
    assert!(matches!(futures::poll!(pending.as_mut()), Poll::Pending));
    drop(pending);
    let second = carrier.enqueue_fact(LogicalHostFact::ConsumerClosed);
    let readiness = carrier.enqueue_readiness(true);
    port.state.lock().unwrap().owner_failed = true;
    carrier.close_ingress_after_owner_failure().await.unwrap();
    assert!(carrier.source_idle());
    assert!(!carrier.explicit_join_claimed());
    for original in [&first, &second, &readiness] {
        assert!(matches!(
            carrier.wait_reply(original).await,
            Err(LogicalSourceClosed)
        ));
    }
    let late = carrier.enqueue_fact(LogicalHostFact::ConsumerClosed);
    assert!(matches!(
        carrier.wait_reply(&late).await,
        Err(LogicalSourceClosed)
    ));
    let state = port.state.lock().unwrap();
    assert_eq!(state.sources, 1);
    assert!(state.exit.is_none());
    assert!(state.facts.is_empty());
    assert!(state.readiness_requests.is_empty());
}

#[tokio::test]
async fn failed_owner_close_refuses_healthy_carrier_without_closing_its_original_queue() {
    let (carrier, port) = carrier();
    assert_eq!(
        carrier.close_ingress_after_owner_failure().await,
        Err(LogicalSourceClosed)
    );
    assert!(port.state.lock().unwrap().closes.is_empty());
    port.state.lock().unwrap().allow_fact = true;
    let original = carrier.enqueue_fact(LogicalHostFact::ConsumerClosed);
    assert!(matches!(
        carrier.wait_reply(&original).await,
        Ok(RuntimeReplyValue::Fact)
    ));
    assert_eq!(port.state.lock().unwrap().facts.len(), 1);
}
