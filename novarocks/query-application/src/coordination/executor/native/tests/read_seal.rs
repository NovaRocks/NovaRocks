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

//! Original-port component tests; scripted runtime exit is not FA Source proof.
use super::*;
use crate::api::{NativeInstalledAttemptPort, RetainedRootReply, StatementNativeHealthFailure};
use crate::coordination::{LogicalExecutionRuntimeRegistry, RootRelayBinding, RootRelayFrontier};
use novarocks_execution_contract::{
    root_lifetime::{RootReadConvergence, RootReadSealed},
    root_result::{RootReadOutcome, RootReadResponse, RootResultRead, RootResultReply},
};
use novarocks_result_contract::{RootOutputKind, RootProfileId};
use novarocks_workload_control::{
    ResourceConfig, ResultCapacityConfig, ResultWindowAlias, ResultWindowClass, RootWork,
    WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::sync::{Barrier, atomic::AtomicU8};

struct Probe {
    root: TaskIdentity,
    native: Mutex<Weak<NativeExecutor>>,
    mode: AtomicU8,
    calls: AtomicUsize,
    closed: AtomicBool,
    ack_polls: Arc<AtomicUsize>,
    ack_failure: AtomicBool,
    entered: Barrier,
    release: Barrier,
}
impl BoundedRootReadPort for Probe {
    fn seal(&self, sealed: RootReadSealed) -> Result<(), QueryExecutionError> {
        assert_eq!(sealed.task(), self.root);
        self.calls.fetch_add(1, Ordering::SeqCst);
        let native = self.native.lock().unwrap().upgrade().unwrap();
        {
            let storage = native
                .storage
                .try_lock()
                .expect("callback runs outside Native lock");
            let read = storage
                .active
                .get(&activation_key(native.initial_activation))
                .unwrap()
                .read
                .as_ref()
                .unwrap();
            assert!(read.attempted && read.closing && !read.sealed);
        }
        match self.mode.load(Ordering::SeqCst) {
            0 => (),
            1 => return Err(protocol("exact original root close refusal")),
            2 => std::panic::panic_any(String::from("exact original root close panic")),
            3 => {
                self.entered.wait();
                self.release.wait();
            }
            _ => panic!("unknown probe mode"),
        }
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn read(&self, request: RootResultRead, window: ResultWindowAlias) -> RootAckFuture {
        let admitted = !self.closed.load(Ordering::SeqCst);
        let polls = self.ack_polls.clone();
        let failure = self.ack_failure.load(Ordering::SeqCst);
        Box::pin(async move {
            polls.fetch_add(1, Ordering::SeqCst);
            if !admitted || failure {
                return Err(ack_failure());
            }
            Ok(RetainedRootReply::try_new(
                RootReadResponse::Reply(RootResultReply {
                    root_task: request.root_task(),
                    profile: request.profile(),
                    kind: request.kind(),
                    accepted_consumed: request.consumed(),
                    outcome: RootReadOutcome::AckOnly,
                }),
                window,
                0,
            )
            .unwrap())
        })
    }
}
struct Installed(AtomicUsize);
impl NativeInstalledAttemptPort for Installed {
    fn start(
        &self,
        _: AttemptActivationIdentity,
    ) -> LogicalFuture<'_, Result<NativeAttemptStartupOutcome, QueryExecutionError>> {
        Box::pin(async { panic!("read-close probe never starts an AP") })
    }
    fn record_ready(
        &self,
        _: crate::api::ComputationReadyToken,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        Box::pin(async { panic!("read-close probe never publishes Ready") })
    }
    fn resources_exited(
        &self,
        _: AttemptActivationIdentity,
    ) -> LogicalFuture<'_, Result<(), StatementNativeHealthFailure>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        // Explicit no-I/O component owner. This is not a real AP exit oracle.
        Box::pin(async { Ok(()) })
    }
}
struct Fixture {
    native: Arc<NativeExecutor>,
    probe: Arc<Probe>,
    installed: Arc<Installed>,
    registry: LogicalExecutionRuntimeRegistry,
    runtime_exit: Box<dyn FnOnce() + Send>,
    work: RootWork,
    control: WorkloadControl,
}
impl Fixture {
    fn new(mode: u8) -> Self {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let schedule = Arc::new(schedule());
        let root = schedule.root();
        let registry = LogicalExecutionRuntimeRegistry::new();
        let handle = registry.handle();
        let (registration, activation, runtime_exit) =
            crate::coordination::runtime_registry::runtime_tests::install_native_component_runtime(
                &handle,
                execution(1),
                schedule.participations().to_vec(),
            );

        let installed = Arc::new(Installed(AtomicUsize::new(0)));
        let native = Arc::new(NativeExecutor::new(
            None,
            installed.clone(),
            Arc::new(AsyncMutex::new(OriginalNativePreparation::default())),
            Weak::new(),
            handle.clone(),
            registration.clone(),
            activation,
            schedule.clone(),
            FrontendProcessId::new_v7(),
            Arc::new(FixedClock(MonotonicInstant::ORIGIN)),
            work.owner.scope().cancellation().unwrap(),
        ));
        let probe = Arc::new(Probe {
            root,
            native: Mutex::new(Arc::downgrade(&native)),
            mode: AtomicU8::new(mode),
            calls: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            ack_polls: Arc::new(AtomicUsize::new(0)),
            ack_failure: AtomicBool::new(false),
            entered: Barrier::new(2),
            release: Barrier::new(2),
        });
        let window = capacity
            .try_acquire(&work.owner.scope(), ResultWindowClass::Client)
            .unwrap();
        let binding = RootRelayBinding {
            port: probe.clone(),
            frontier: RootRelayFrontier::new(
                root,
                RootProfileId::V1,
                RootOutputKind::CountOnly,
                None,
            )
            .unwrap(),
            window: window.retain_alias(),
            max_wait: Duration::from_millis(1),
        };
        let mut reservation = handle
            .reserve_active_native_owner(&registration, execution(1), schedule.participations())
            .unwrap();
        let mut pending = None;
        reservation.receive_activated_in(
            &mut pending,
            crate::api::ActivatedNativeAttempt::relayed_rows(activation, binding),
        );
        reservation
            .stage_activated_in(activation, root, &mut pending)
            .unwrap();
        let (owner, rows) = reservation.finish_activated_in(&mut pending).unwrap();
        let rows = rows.expect("the same original row binding returns from its slot");
        assert!(pending.is_none());
        let read = OriginalRootRead::from_rows(&rows);
        {
            let mut storage = native.lock();
            storage
                .prepared
                .get_mut(&execution(1))
                .unwrap()
                .activation_started = Some(activation);
            storage.active.insert(
                activation_key(activation),
                ActiveOriginal {
                    owner: Arc::new(AsyncMutex::new(owner)),
                    rows: Some(rows),
                    read: Some(read),
                    terminal_cut: Some((
                        MonotonicInstant::ORIGIN,
                        MonotonicInstant::from_origin(Duration::from_secs(1)),
                    )),
                    streamed_success: false,
                    computation_ready: None,
                    run_started: false,
                    resources_exited: false,
                    cleanup_adopted: false,
                    dormant_retired_for_cleanup: false,
                    cleanup_failure: Arc::new(CleanupFailure::default()),
                },
            );
        }
        drop(window);
        Self {
            native,
            probe,
            installed,
            registry,
            runtime_exit: Box::new(runtime_exit),
            work,
            control,
        }
    }
    fn effect(&self) -> NativeEffect {
        NativeEffect::SealRootRead {
            activation: self.native.initial_activation,
            sealed: RootReadConvergence::new(self.probe.root).seal_terminated(),
        }
    }
    async fn close(self, proven: bool) {
        if proven {
            assert!(matches!(
                self.native
                    .execute(NativeEffect::FinishResources {
                        activation: self.native.initial_activation
                    })
                    .await,
                NativeFact::Applied
            ));
            assert_eq!(self.installed.0.load(Ordering::SeqCst), 1);
            assert_eq!(self.registry.active_native_custody_count(), 0);
        } else {
            assert!(matches!(
                self.native
                    .execute(NativeEffect::FinishResources {
                        activation: self.native.initial_activation
                    })
                    .await,
                NativeFact::Failed { .. }
            ));
            assert_eq!(self.installed.0.load(Ordering::SeqCst), 0);
            assert_eq!(self.registry.active_native_custody_count(), 1);
        }
        let Self {
            native,
            probe,
            installed: _,
            mut registry,
            runtime_exit,
            work,
            control,
        } = self;
        drop(native);
        if proven {
            runtime_exit();
            registry
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
        } else {
            // Explicit process-exit teardown after asserting retained UNKNOWN.
            // This does not certify a successful original resource close.
            registry.abandon_for_process_exit();
            drop(runtime_exit);
        }
        assert_eq!(registry.active_native_custody_count(), 0);
        assert!(probe.native.lock().unwrap().upgrade().is_none());
        drop(probe);
        work.business.release();
        work.owner.complete();
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }
}
fn failure(fact: NativeFact) -> QueryExecutionError {
    match fact {
        NativeFact::Failed { failure, .. } => failure.error,
        _ => panic!("original failure required"),
    }
}
#[tokio::test]
async fn foreign_read_close_does_not_consume_original_port_or_poison_its_later_close() {
    let f = Fixture::new(0);
    let a = f.native.initial_activation;
    let r = f.probe.root;
    for foreign in [
        TaskIdentity::new(
            r.query_execution_id(),
            StageId::new(2).unwrap(),
            r.task_id(),
            r.backend_process_id(),
        ),
        TaskIdentity::new(
            execution(2),
            r.stage_id(),
            r.task_id(),
            r.backend_process_id(),
        ),
    ] {
        let _ = failure(
            f.native
                .execute(NativeEffect::SealRootRead {
                    activation: a,
                    sealed: RootReadConvergence::new(foreign).seal_terminated(),
                })
                .await,
        );
    }
    let _ = failure(
        f.native
            .execute(NativeEffect::SealRootRead {
                activation: AttemptActivationIdentity::from_parts(
                    a.actor().get(),
                    a.execution(),
                    a.generation() + 1,
                ),
                sealed: RootReadConvergence::new(r).seal_terminated(),
            })
            .await,
    );
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        f.native.execute(f.effect()).await,
        NativeFact::Applied
    ));
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 1);
    f.close(true).await;
}
#[tokio::test]
async fn successful_read_close_uses_original_port_once_after_rows_move_and_outside_lock() {
    let f = Fixture::new(0);
    let rows = f
        .native
        .take_rows(f.native.initial_activation)
        .unwrap()
        .unwrap();
    assert_eq!(
        f.native.retained_read_root(f.native.initial_activation),
        Some(f.probe.root)
    );
    assert!(matches!(
        f.native.execute(f.effect()).await,
        NativeFact::Applied
    ));
    assert!(matches!(
        f.native.execute(f.effect()).await,
        NativeFact::Applied
    ));
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 1);
    let row = f.native.cleanup_inventory().unwrap().active[0];
    assert!(row.read_sealed && !row.read_closing && !row.resources_exited);
    drop(rows);
    f.close(true).await;
}
#[tokio::test]
async fn original_read_close_refusal_is_retained_and_never_retried_or_upgraded_to_exit() {
    let f = Fixture::new(1);
    let original = failure(f.native.execute(f.effect()).await);
    assert_eq!(original, protocol("exact original root close refusal"));
    f.probe.mode.store(0, Ordering::SeqCst);
    assert_eq!(failure(f.native.execute(f.effect()).await), original);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 1);
    assert!(!f.native.cleanup_inventory().unwrap().active[0].read_sealed);
    f.close(false).await;
}
#[tokio::test]
async fn original_read_close_panic_keeps_raw_action_cause_without_repolling_callback() {
    let f = Fixture::new(2);
    let _ = failure(f.native.execute(f.effect()).await);
    let causes = f.native.causes(execution(1), 24);
    assert_eq!(
        causes
            .primary
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .downcast_ref::<String>()
            .unwrap(),
        "exact original root close panic"
    );
    assert!(f.native.has_unhandled_failure());
    f.probe.mode.store(0, Ordering::SeqCst);
    let _ = failure(f.native.execute(f.effect()).await);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 1);
    assert!(!f.native.cleanup_inventory().unwrap().active[0].read_closing);
    drop(causes);
    f.close(false).await;
}
#[derive(Default)]
struct WakeCount(AtomicUsize);
impl std::task::Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn concurrent_read_close_waits_for_original_callback_and_does_not_turn_inflight_into_failure()
{
    let f = Fixture::new(3);
    let native = f.native.clone();
    let effect = f.effect();
    let worker = std::thread::spawn(move || futures::executor::block_on(native.execute(effect)));
    f.probe.entered.wait();
    let row = f.native.cleanup_inventory().unwrap().active[0];
    assert!(row.read_closing && !row.read_sealed);
    let effect = f.effect();
    let mut observer = f.native.execute(effect);
    let wake = Arc::new(WakeCount::default());
    let waker = Waker::from(wake.clone());
    assert!(
        observer
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.installed.0.load(Ordering::SeqCst), 0);
    f.probe.release.wait();
    assert!(matches!(worker.join().unwrap(), NativeFact::Applied));
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(observer.await, NativeFact::Applied));
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 1);
    f.close(true).await;
}
#[tokio::test]
async fn admitted_ack_survives_close_while_close_never_waits_for_optional_ack_success() {
    for ack_refuses in [false, true] {
        let f = Fixture::new(0);
        f.probe.ack_failure.store(ack_refuses, Ordering::SeqCst);
        let rows = f
            .native
            .take_rows(f.native.initial_activation)
            .unwrap()
            .unwrap();
        let request = RootResultRead::try_new(
            f.probe.root,
            RootProfileId::V1,
            RootOutputKind::CountOnly,
            None,
            1,
            Duration::from_millis(1),
        )
        .unwrap();
        let mut incoming = Some(f.probe.read(request.clone(), rows.binding.window.clone()));
        f.native
            .stage_root_ack_in(f.native.initial_activation, f.probe.root, &mut incoming)
            .unwrap();
        assert!(incoming.is_none());
        assert_eq!(f.probe.ack_polls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            f.native.execute(f.effect()).await,
            NativeFact::Applied
        ));
        assert_eq!(f.probe.ack_polls.load(Ordering::SeqCst), 0);
        assert!(!f.native.tails_exited(f.native.initial_activation));
        assert!(
            f.probe
                .read(request, rows.binding.window.clone())
                .await
                .is_err()
        );
        let obligation = std::future::poll_fn(|cx| f.native.poll_tail_work(cx)).await;
        let (future, joined) = obligation.into_future();
        // This is the actual component Tokio Join, not a credentialed FA Join.
        tokio::spawn(future).await.unwrap();
        assert!(!f.native.tails_exited(f.native.initial_activation));
        f.native.tail_joined(joined, Ok(())).unwrap();
        assert!(f.native.tails_exited(f.native.initial_activation));
        assert!(
            !f.native.has_unhandled_failure(),
            "ordinary optional ACK refusal stays diagnostic"
        );
        drop(rows);
        f.close(true).await;
    }
}

// Physical original-tail tier: the installed port above is explicitly no-I/O.
// No AP exit, Ready, or remote closure is manufactured by this component.
#[tokio::test]
async fn original_ack_join_survives_cancelled_wait_while_original_resource_cleanup_completes() {
    struct Originals {
        fixture: Option<Fixture>,
        incoming: Option<RootAckFuture>,
        rows: Option<crate::api::NativeRowsAttemptRuntime>,
        join: Option<tokio::task::JoinHandle<()>>,
        join_failure: Option<tokio::task::JoinError>,
        receipt: Option<OriginalTailJoinObligation>,
        release: Option<tokio::sync::oneshot::Sender<()>>,
    }
    struct KeepWhole(Option<Box<Originals>>);
    impl Drop for KeepWhole {
        fn drop(&mut self) {
            if std::thread::panicking() {
                static ORIGINALS: std::sync::OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
                    std::sync::OnceLock::new();
                if let Some(original) = self.0.take() {
                    ORIGINALS
                        .get_or_init(Default::default)
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .push(original);
                }
            }
        }
    }
    struct CaptureDrop(Arc<AtomicUsize>);
    impl Drop for CaptureDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let mut whole = KeepWhole(Some(Box::new(Originals {
        fixture: Some(Fixture::new(0)),
        incoming: None,
        rows: None,
        join: None,
        join_failure: None,
        receipt: None,
        release: None,
    })));
    let original = whole.0.as_mut().unwrap();
    let f = original.fixture.as_ref().unwrap();
    let activation = f.native.initial_activation;
    original.rows = f.native.take_rows(activation).unwrap();
    let request = RootResultRead::try_new(
        f.probe.root,
        RootProfileId::V1,
        RootOutputKind::CountOnly,
        None,
        1,
        Duration::from_millis(1),
    )
    .unwrap();
    // Admission is authored by the SAME original port before its seal. The
    // test's physical dependency holds that very future, never a fake result.
    let ack = f.probe.read(
        request,
        original.rows.as_ref().unwrap().binding.window.clone(),
    );
    let (release, held) = tokio::sync::oneshot::channel();
    original.release = Some(release);
    let entered = Arc::new(AtomicBool::new(false));
    let entered_ack = entered.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let capture = CaptureDrop(drops.clone());
    original.incoming = Some(Box::pin(async move {
        let _capture = capture;
        entered_ack.store(true, Ordering::SeqCst);
        held.await
            .expect("the same original ACK dependency must be released");
        ack.await
    }));
    f.native
        .stage_root_ack_in(activation, f.probe.root, &mut original.incoming)
        .unwrap();
    assert!(original.incoming.is_none());
    assert!(matches!(
        f.native.execute(f.effect()).await,
        NativeFact::Applied
    ));
    assert_eq!(f.probe.ack_polls.load(Ordering::SeqCst), 0);
    // The caller releases its row binding. The fixed ACK slot alone now holds
    // the original response window; no caller clone can explain its retention.
    drop(original.rows.take());
    let obligation = std::future::poll_fn(|cx| f.native.poll_tail_work(cx)).await;
    let (future, receipt) = obligation.into_future();
    original.receipt = Some(receipt);
    original.join = Some(tokio::spawn(future));
    for _ in 0..64 {
        if entered.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        entered.load(Ordering::SeqCst),
        "the actual ACK task must reach its dependency"
    );
    assert!(!original.join.as_ref().unwrap().is_finished());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(!f.native.tails_exited(activation));
    {
        let mut wait = Box::pin(async { original.join.as_mut().unwrap().await });
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        // Cancel only this borrow, retaining the SAME Join and unique receipt.
    }
    assert!(!original.join.as_ref().unwrap().is_finished());
    assert!(original.receipt.is_some());
    assert!(matches!(
        f.native
            .execute(NativeEffect::FinishResources { activation })
            .await,
        NativeFact::Applied
    ));
    assert_eq!(f.installed.0.load(Ordering::SeqCst), 1);
    assert_eq!(f.registry.active_native_custody_count(), 0);
    assert!(
        !f.native.tails_exited(activation),
        "resource cleanup is not ACK Join"
    );
    assert_eq!(f.probe.ack_polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    original.release.take().unwrap().send(()).unwrap();
    // Resume the same original Join after cancellation of its earlier await.
    if let Err(failure) = original.join.as_mut().unwrap().await {
        original.join_failure = Some(failure);
        panic!("the same actual ACK Join failed; its original cause remains in custody");
    }
    drop(original.join.take());
    assert_eq!(f.probe.ack_polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(
        !f.native.tails_exited(activation),
        "actual Join still needs its exact receipt"
    );
    f.native
        .tail_joined(original.receipt.take().unwrap(), Ok(()))
        .unwrap();
    assert!(f.native.tails_exited(activation));
    assert!(!f.native.has_unhandled_failure());
    // Reuses the original fixture's truthful component runtime teardown. It
    // does not certify an actual FA/AP Scope exit from the no-I/O port above.
    original.fixture.take().unwrap().close(true).await;
}
