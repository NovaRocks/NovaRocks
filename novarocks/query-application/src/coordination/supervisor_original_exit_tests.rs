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

// Included in the original supervisor tests; no synthetic completion packet.

struct OriginalOpenPanic {
    dropped: Arc<AtomicU64>,
}
impl Drop for OriginalOpenPanic {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct OriginalOpenPanicPort {
    dropped: Arc<AtomicU64>,
}
impl LogicalExecutionNativePort for OriginalOpenPanicPort {
    fn bind_in(
        &self,
        _source: &mut Option<LogicalNativeOpenRequest>,
        _target: &mut Option<LogicalNativeSession>,
    ) -> Result<(), LogicalNativeOpenError> {
        let dropped = Arc::clone(&self.dropped);
        std::panic::panic_any(OriginalOpenPanic { dropped })
    }
}
struct OriginalExitPanicWake;
impl std::task::Wake for OriginalExitPanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(OriginalExitWakePanic);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(OriginalExitWakePanic);
    }
}
struct OriginalExitWakePanic;

#[tokio::test]
async fn observed_unissued_start_keeps_ready_exit_and_original_footer_separate() {
    let (control, root) = governance().await;
    let mut control = {
        let retirement = root.owner.retirement_observer().unwrap();
        let (mut bank, shutdown, mut registry, client) =
            unpolled_test_driver(&control, Arc::new(PanickingOpenNativePort));
        let mut input = match client
            .prepare_observed_start(completion_request(UnreachablePreparationPort), None)
        {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        let mut owner = Some(root.owner);
        let mut ready = None;
        let mut exit = None;
        let tail = client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap();
        assert!(tail.result().is_ok());
        assert!(input.is_none() && owner.is_none() && ready.is_some() && exit.is_some());
        tail.finish().unwrap();
        let mut observer = exit.take().unwrap();
        let mut wait = Box::pin(std::future::poll_fn(|cx| observer.poll_exit(cx)));
        assert!(futures::poll!(&mut wait).is_pending());
        drop(wait);
        shutdown.send_replace(true);
        bank.stop_ingress();
        let completion = bank.progress_turn(NonZeroUsize::new(7).unwrap()).unwrap();
        observe_logical_completion(&registry.handle(), &mut bank, completion, &mut None);
        let error = ready
            .take()
            .unwrap()
            .await
            .err()
            .expect("unissued shutdown rejects start");
        assert_eq!(error.kind(), QueryExecutionErrorKind::Rejected);
        let receipt = std::future::poll_fn(|cx| observer.poll_exit(cx))
            .await
            .unwrap();
        assert_eq!(
            receipt.local_disposition(),
            crate::api::LocalExecutionDisposition::ActualSourcesExited
        );
        assert_eq!(
            receipt.remote_disposition(),
            crate::api::RemoteExecutionDisposition::RemoteNotReported
        );
        assert_eq!(
            receipt.view().actor_phase(),
            crate::api::LogicalActorExitPhase::Unreserved
        );
        root.business.release();
        drop(bank);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        assert!(
            !retirement.is_reclaimed().unwrap(),
            "receipt still owns the exact original footer"
        );
        drop(receipt);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !retirement.is_reclaimed().unwrap() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn observed_real_open_panic_survives_cancelled_wait_until_original_health_disposes() {
    let (control, root) = governance().await;
    let mut control = {
        let dropped = Arc::new(AtomicU64::new(0));
        let (mut bank, _shutdown, mut registry, client) = unpolled_test_driver(
            &control,
            Arc::new(OriginalOpenPanicPort {
                dropped: Arc::clone(&dropped),
            }),
        );
        let mut input = match client
            .prepare_observed_start(completion_request(UnreachablePreparationPort), None)
        {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        let mut owner = Some(root.owner);
        let mut ready = None;
        let mut exit = None;
        client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap()
            .finish()
            .unwrap();
        let mut observer = exit.take().unwrap();
        let mut wait = Box::pin(std::future::poll_fn(|cx| observer.poll_exit(cx)));
        assert!(futures::poll!(&mut wait).is_pending());
        drop(wait);
        let completion = bank.progress_turn(NonZeroUsize::new(7).unwrap()).unwrap();
        observe_logical_completion(&registry.handle(), &mut bank, completion, &mut None);
        assert!(ready.take().unwrap().await.is_err());
        {
            let view = observer.view().unwrap();
            assert_eq!(
                view.actor_phase(),
                crate::api::LogicalActorExitPhase::Unreserved
            );
            assert!(view.native_action_started());
            match view.role_result().unwrap() {
                PollCompletion::Panicked(packet) => {
                    assert!(packet.value().primary().is::<OriginalOpenPanic>())
                }
                _ => panic!("actual Native open panic was lost"),
            }
        }
        assert!(
            futures::poll!(std::future::poll_fn(|cx| observer.poll_exit(cx))).is_pending(),
            "Native session return is not fabricated from no actor"
        );
        drop(observer);
        root.business.release();
        drop(bank);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            0,
            "original health still retains original payload"
        );
        registry.abandon_for_process_exit();
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "explicit original health disposal destroys the actual payload"
        );
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn rejected_late_health_completion_and_callback_panic_return_the_same_bank_on_original_join()
{
    let control = governance_empty();
    let mut control = {
        let first = control.begin_root().await;
        let second = control.begin_root().await;
        let dropped = Arc::new(AtomicU64::new(0));
        let (mut bank, shutdown, mut registry, client) = unpolled_test_driver(
            &control,
            Arc::new(OriginalOpenPanicPort {
                dropped: Arc::clone(&dropped),
            }),
        );
        let first_ready = client.start(completion_request(UnreachablePreparationPort), first.owner);
        let completion = bank.progress_turn(NonZeroUsize::new(7).unwrap()).unwrap();
        observe_logical_completion(&registry.handle(), &mut bank, completion, &mut None);
        assert!(first_ready.await.is_err());
        let mut input = match client
            .prepare_observed_start(completion_request(UnreachablePreparationPort), None)
        {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        let mut owner = Some(second.owner);
        let mut ready = None;
        let mut exit = None;
        client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap()
            .finish()
            .unwrap();
        let mut observer = exit.take().unwrap();
        let wake = std::task::Waker::from(Arc::new(OriginalExitPanicWake));
        let mut cx = std::task::Context::from_waker(&wake);
        assert!(observer.poll_exit(&mut cx).is_pending());
        let root_retirement = OriginalRootPanic::empty();
        let mut original_join = Handle::current().spawn(run_supervisor_owned(
            registry.handle(),
            bank,
            NonZeroUsize::new(7).unwrap(),
            shutdown.subscribe(),
            root_retirement.clone(),
            crate::session_control::StatementProgressSlot::empty(),
        ));
        let outcome = tokio::time::timeout(Duration::from_secs(2), &mut original_join)
            .await
            .expect("the same root actually returns its failure custody")
            .unwrap();
        let original = match outcome {
            OriginalRootOutcome::Panicked(original) => original,
            OriginalRootOutcome::Completed(_) => panic!("actual registered callback must panic"),
        };
        drop(root_retirement);
        assert!(
            original
                .custody()
                .primary
                .as_ref()
                .unwrap()
                .is::<OriginalExitWakePanic>()
        );
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            0,
            "both actual role causes are still retained"
        );
        assert!(ready.take().unwrap().await.is_err());
        assert!(
            observer
                .poll_exit(&mut std::task::Context::from_waker(
                    futures::task::noop_waker_ref()
                ))
                .is_pending()
        );
        assert!(
            observer.view().unwrap().role_result().is_none(),
            "rejected whole packet remains in the original Position"
        );
        drop(observer);
        first.business.release();
        second.business.release();
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        registry.abandon_for_process_exit();
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "first original health explicitly disposed only its own source"
        );
        // Irrevocable process-exit disposition of the SAME actual RootFailure.
        original.dispose_for_process_exit();
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            2,
            "returned bank still held the rejected whole source"
        );
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

struct OriginalReplyPanicOnce(std::sync::atomic::AtomicBool);
impl std::task::Wake for OriginalReplyPanicOnce {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if self.0.swap(false, Ordering::SeqCst) {
            std::panic::panic_any(OriginalExitWakePanic);
        }
    }
}

#[tokio::test]
async fn original_root_joins_installed_actor_after_actual_reply_wake_panics_and_keeps_serving() {
    let (control, first) = governance().await;
    let mut control = {
        let frontend = FrontendProcessId::new_v7();
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x7d),
            frontend,
            supervisor_config(),
        );
        let mut input = match client.prepare_observed_start(
            completion_request(PanickingAttemptPreparationPort {
                backend: BackendProcessId::new_v7(),
                phase: AttemptPanicPhase::Activate,
                convergence_calls: Arc::new(AtomicU64::new(0)),
            }),
            None,
        ) {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        let mut owner = Some(first.owner);
        let mut ready = None;
        let mut exit = None;
        let tail = client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap();
        let wake = std::task::Waker::from(Arc::new(OriginalReplyPanicOnce(
            std::sync::atomic::AtomicBool::new(true),
        )));
        let mut cx = std::task::Context::from_waker(&wake);
        assert!(ready.as_mut().unwrap().as_mut().poll(&mut cx).is_pending());
        tail.finish().unwrap();
        let mut observer = exit.take().unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while observer.view().unwrap().actor_phase()
                != crate::api::LogicalActorExitPhase::Joined
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the same original root continues exact actor close/join without process shutdown");
        assert!(ready.take().unwrap().await.is_err());
        {
            let view = observer.view().unwrap();
            assert_eq!(
                view.pending_native_returns(),
                0,
                "unused original reservation returned exactly once"
            );
            assert!(matches!(view.actor_result(), Some(Ok(()))));
            match view.role_result().unwrap() {
                PollCompletion::Panicked(packet) => {
                    assert!(packet.value().primary().is::<OriginalExitWakePanic>())
                }
                _ => panic!("actual reply wake panic was not retained"),
            }
        }
        assert!(futures::poll!(std::future::poll_fn(|cx| observer.poll_exit(cx))).is_pending());
        let second = control.begin_root().await;
        let second_handle = tokio::time::timeout(
            Duration::from_secs(2),
            client.start(
                completion_request(SuccessfulCompletionPreparationPort {
                    backend: BackendProcessId::new_v7(),
                    expected_frontend: frontend,
                }),
                second.owner,
            ),
        )
        .await
        .expect("healthy peer remains served by the same root")
        .unwrap();
        drop(second_handle);
        first.business.release();
        second.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await;
        let actual_remaining = (
            supervisor.registry.handle().active_native_custody_count(),
            supervisor.registry.handle().root_ack_tail_count(),
            supervisor.registry.handle().original_exit_pending(),
            supervisor
                .join
                .as_ref()
                .map(tokio::task::JoinHandle::is_finished),
            supervisor.original_root_panic().is_some(),
        );
        // Dispose through the real process owner before asserting the health
        // classification. A failed assertion must not strand a live root.
        supervisor.abandon_for_process_exit();
        drop(observer);
        assert!(
            matches!(
                shutdown,
                Err(LogicalExecutionSupervisorShutdownError::Registry(
                    LogicalExecutionRuntimeRegistryError::OriginalExitFailureUnresolved
                ))
            ),
            "actual shutdown must retain the original exit failure: {shutdown:?}; remaining native/ack/pending/join/root-failure: {actual_remaining:?}"
        );
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_root_abort_closes_ingress_before_first_poll_and_after_real_prepare_pending() {
    for after_open in [false, true] {
        let control = governance_empty();
        let first = control.begin_root().await;
        let second = control.begin_root().await;
        let first_retirement = first.owner.retirement_observer().unwrap();
        let dropped = Arc::new(AtomicU64::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x8e),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        // No await between spawning this original root and aborting it in the
        // first branch: current_thread proves its body has never been polled.
        let first_ready = client.start(
            completion_request(OriginalPreparationGate::pending(
                DropTrackedPreparationPort {
                    drops: Arc::clone(&dropped),
                },
                Arc::clone(&entered),
            )),
            first.owner,
        );
        if after_open {
            tokio::time::timeout(Duration::from_secs(2), entered.notified())
                .await
                .expect("actual original open was entered");
        }
        supervisor.join.as_ref().unwrap().abort();
        let joined =
            tokio::time::timeout(Duration::from_secs(2), supervisor.join.as_mut().unwrap())
                .await
                .expect("same original root JoinHandle must actually exit");
        let raw = match joined {
            Err(raw) => raw,
            Ok(_) => panic!("the actual root was aborted before normal completion"),
        };
        let was_cancelled = raw.is_cancelled();
        let outcome = supervisor.receive_original_root_join_failure(raw);
        supervisor.run_outcome = Some(outcome);
        supervisor.join.take();
        let shutdown_still_open = !*supervisor.shutdown.borrow();

        let mut input = match client
            .prepare_observed_start(completion_request(UnreachablePreparationPort), None)
        {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        let mut owner = Some(second.owner);
        let mut ready = None;
        let mut exit = None;
        let attempted = client.start_observed_in(&mut input, &mut owner, &mut ready, &mut exit);
        let refused = match attempted {
            Ok(attempt) => attempt.finish().is_err(),
            Err(_) => true,
        };
        let sources_preserved =
            input.is_some() && owner.is_some() && ready.is_none() && exit.is_none();
        let original_input_retained = dropped.load(Ordering::SeqCst) == 0;
        let not_reclaimed = !first_retirement.is_reclaimed().unwrap();
        let cancelled_bank_retained = supervisor
            .original_root_panic()
            .is_some_and(|view| view.was_cancelled() && view.primary().is_none());
        let exact_join_retained = supervisor
            .original_root_join_failure()
            .is_some_and(tokio::task::JoinError::is_cancelled);
        let same_raw_join = Arc::ptr_eq(
            supervisor.original_root_failure.as_ref().unwrap(),
            supervisor
                .original_root_panic
                .as_ref()
                .unwrap()
                .shared
                .join_failure
                .get()
                .unwrap(),
        );

        // These are abnormal-root facts, not normal shutdown success. Dispose
        // through the original process-exit author before asserting them.
        drop(input.take());
        owner
            .take()
            .unwrap()
            .complete_after_terminal_cancel_settled();
        first.business.release();
        second.business.release();
        drop(first_ready);
        supervisor.abandon_for_process_exit();
        drop(client);
        let after_disposition = dropped.load(Ordering::SeqCst);
        let mut control = control;
        acknowledge_all_control(&control);
        // Explicit process-exit disposal destroys the unpolled original owner
        // as Orphaned; it does not author normal completion or WC Drained.
        control.finish_for_process_exit().await;
        assert!(was_cancelled && exact_join_retained && same_raw_join);
        assert!(
            shutdown_still_open,
            "the same live Supervisor did not initiate shutdown"
        );
        assert!(
            refused && sources_preserved,
            "a retained but service-less original ingress must refuse without consuming sources"
        );
        assert!(original_input_retained && not_reclaimed && cancelled_bank_retained);
        assert_eq!(
            after_disposition, 1,
            "only explicit original process disposition destroyed the accepted source"
        );
    }
}

#[derive(Debug)]
enum OriginalPreparationGateKind {
    Pending(Arc<tokio::sync::Notify>),
    Release(Arc<tokio::sync::Notify>),
    DelayedCancel,
}
#[derive(Debug)]
struct OriginalPreparationGate {
    inner: Box<dyn NativeAttemptPreparationPort>,
    gate: OriginalPreparationGateKind,
}
impl OriginalPreparationGate {
    fn pending(
        inner: impl NativeAttemptPreparationPort,
        entered: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            gate: OriginalPreparationGateKind::Pending(entered),
        }
    }
    fn release(
        inner: impl NativeAttemptPreparationPort,
        release: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            gate: OriginalPreparationGateKind::Release(release),
        }
    }
    fn delayed_cancel(inner: impl NativeAttemptPreparationPort) -> Self {
        Self {
            inner: Box::new(inner),
            gate: OriginalPreparationGateKind::DelayedCancel,
        }
    }
}
impl NativeAttemptPreparationPort for OriginalPreparationGate {
    fn prepare_in<'a>(
        &'a mut self,
        request: &'a mut crate::api::NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a> {
        let inner = &mut self.inner;
        match &self.gate {
            OriginalPreparationGateKind::Pending(entered) => {
                entered.notify_one();
                Box::pin(std::future::pending())
            }
            OriginalPreparationGateKind::Release(release) => Box::pin(async move {
                release.notified().await;
                inner.prepare_in(request).await
            }),
            OriginalPreparationGateKind::DelayedCancel => {
                let cancellation = request.cancellation();
                Box::pin(async move {
                    let reason = cancellation.cancelled().await;
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    Err(
                        NativeAttemptPreparationFailure::from_input(fixture_cancellation_input(
                            reason,
                            "server shutdown interrupted Native preparation",
                        ))
                        .into(),
                    )
                })
            }
        }
    }
}

#[tokio::test]
async fn cancelled_real_preparation_wait_closes_original_session_before_local_exit_receipt() {
    let (control, root) = governance().await;
    let mut control = {
        let scope = root.owner.scope();
        let retirement = root.owner.retirement_observer().unwrap();
        let requester = root.owner.cancellation_requester();
        let entered = Arc::new(tokio::sync::Notify::new());
        let drops = Arc::new(AtomicU64::new(0));
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x8f),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let mut input = match client.prepare_observed_start(
            completion_request(OriginalPreparationGate::pending(
                DropTrackedPreparationPort {
                    drops: Arc::clone(&drops),
                },
                Arc::clone(&entered),
            )),
            None,
        ) {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        let mut owner = Some(root.owner);
        let mut ready = None;
        let mut exit = None;
        client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap()
            .finish()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .expect("actual original borrowed prepare is waiting");
        let mut observer = exit.take().unwrap();
        assert!(futures::poll!(std::future::poll_fn(|cx| observer.poll_exit(cx))).is_pending());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        requester.request(CancellationReason::Requested).unwrap();
        assert!(ready.take().unwrap().await.is_err());
        let receipt = tokio::time::timeout(
            Duration::from_secs(2),
            std::future::poll_fn(|cx| observer.poll_exit(cx)),
        )
        .await
        .expect("original role clear and explicit Native body Drop must complete")
        .unwrap();
        let view = receipt.view();
        assert!(view.native_preparation_exited());
        assert_eq!(
            view.actor_phase(),
            crate::api::LogicalActorExitPhase::Unreserved
        );
        assert!(view.native_preparation_failure().is_none());
        drop(view);
        assert_eq!(
            receipt.local_disposition(),
            crate::api::LocalExecutionDisposition::ActualSourcesExited
        );
        assert_eq!(
            receipt.remote_disposition(),
            crate::api::RemoteExecutionDisposition::RemoteNotReported
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "same original port actually dropped"
        );
        assert!(
            !retirement.is_reclaimed().unwrap(),
            "original receipt footer is still live"
        );
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        drop(receipt);
        drop(observer);
        acknowledge_all_control(&control);
        tokio::time::timeout(Duration::from_secs(2), scope.wait_released())
            .await
            .unwrap();
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[derive(Debug)]
struct OriginalDormantDropPanic {
    ordinal: u64,
}
#[derive(Debug)]
struct CompletedActivationDropPreparation {
    window: Option<TestWindow>,
    backends: Box<[BackendProcessId]>,
    prepares: Arc<AtomicU64>,
    target_drops: Arc<AtomicU64>,
    panic_ordinal: u64,
}
impl TestRowsPreparationPort for CompletedActivationDropPreparation {
    fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
        self.window = Some(TestWindow(window));
    }
}
impl NativeAttemptPreparationPort for CompletedActivationDropPreparation {
    fn prepare_in<'a>(
        &'a mut self,
        request: &'a mut crate::api::NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a> {
        let ordinal = self.prepares.fetch_add(1, Ordering::SeqCst) + 1;
        let backend = self.backends[usize::try_from(ordinal - 1).unwrap()];
        request.stage_fixture_owner(CompletedActivationDropDormant {
            inner: RecoveringRowsDormantOwner {
                relay_window: self.window.as_ref().unwrap().clone(),
                backend,
                ordinal,
                runs: Arc::new(AtomicU64::new(0)),
                convergences: Arc::new(AtomicU64::new(0)),
                allow_initial_convergence: Arc::new(tokio::sync::Notify::new()),
                block_initial_convergence: false,
                replacement_missing_rows: false,
                replacement_activation_panic: false,
                failures_before_success: self.panic_ordinal - 1,
            },
            target_drops: Arc::clone(&self.target_drops),
            panic_ordinal: self.panic_ordinal,
            completed: false,
        });
        Box::pin(async move {
            let scheduling = no_scan_scheduling(request);
            request.bind_staged(scheduling).map_err(Into::into)
        })
    }
}
#[derive(Debug)]
struct CompletedActivationDropDormant {
    inner: RecoveringRowsDormantOwner,
    target_drops: Arc<AtomicU64>,
    panic_ordinal: u64,
    completed: bool,
}
impl Drop for CompletedActivationDropDormant {
    fn drop(&mut self) {
        if self.completed && self.inner.ordinal == self.panic_ordinal {
            std::panic::panic_any(OriginalDormantDropPanic {
                ordinal: self.inner.ordinal,
            });
        }
    }
}
impl DormantNativeAttemptOwner for CompletedActivationDropDormant {
    fn poll_bind_qualification(
        &mut self,
        schedule: &crate::coordination::AttemptSchedule,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
        self.inner.poll_bind_qualification(schedule, cx)
    }

    fn eligible_backends(&self) -> &[BackendProcessId] {
        self.inner.eligible_backends()
    }
    fn activate<'a>(
        &'a mut self,
        schedule: &'a AttemptSchedule,
        admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> NativeAttemptActivationFuture<'a> {
        Box::pin(async move {
            let activated = self
                .inner
                .activate(schedule, admissions, cancellation)
                .await?;
            let (inner, rows) = activated.into_parts();
            let owner = CompletedActivationDropActive {
                inner,
                drops: Arc::clone(&self.target_drops),
                tracked: self.inner.ordinal == self.panic_ordinal,
            };
            self.completed = true;
            Ok(match rows {
                Some(rows) => {
                    ActivatedNativeAttempt::relayed_rows(owner, rows.binding, rows.statuses)
                }
                None => ActivatedNativeAttempt::completion(owner),
            })
        })
    }
    fn converge<'a>(
        &'a mut self,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a> {
        self.inner.converge(cancellation)
    }
}
#[derive(Debug)]
struct CompletedActivationDropActive {
    inner: Box<dyn ActiveNativeAttemptOwner>,
    drops: Arc<AtomicU64>,
    tracked: bool,
}
impl Drop for CompletedActivationDropActive {
    fn drop(&mut self) {
        if self.tracked {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl ActiveNativeAttemptOwner for CompletedActivationDropActive {
    fn run<'a>(
        &'a mut self,
        drive: &'a NativeAttemptDrive,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> NativeAttemptRunFuture<'a> {
        self.inner.run(drive, cancellation)
    }
    fn converge<'a>(
        &'a mut self,
        request: NativeActiveConvergenceRequest,
    ) -> NativeActiveAttemptConvergenceFuture<'a> {
        self.inner.converge(request)
    }
}

#[tokio::test]
async fn initial_and_replacement_completed_outputs_survive_original_dormant_drop_panic() {
    for panic_ordinal in [1, 2] {
        let (control, root) = governance().await;
        let scope = root.owner.scope();
        let prepares = Arc::new(AtomicU64::new(0));
        let target_drops = Arc::new(AtomicU64::new(0));
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x91 + panic_ordinal),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let recovery = if panic_ordinal == 1 {
            RecoveryMode::NoRecovery
        } else {
            RecoveryMode::RestartAttemptBeforeVisibility
        };
        let replacements = (panic_ordinal == 2).then(|| {
            Arc::new(ImmediateReplacementPort::default())
                as Arc<dyn super::super::ReplacementQualificationEffectPort>
        });
        let request = rows_request(
            &control,
            &scope,
            recovery,
            replacements,
            CompletedActivationDropPreparation {
                window: None,
                backends: fixture_recovery_backends(2),
                prepares: Arc::clone(&prepares),
                target_drops: Arc::clone(&target_drops),
                panic_ordinal,
            },
        );
        let mut input = match client.prepare_observed_start(request, None) {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        // This probe aliases only the original Source, never the activated
        // output. Without the production slot, the target owner really drops.
        let source = Arc::clone(input.as_ref().unwrap().context.as_ref().unwrap());
        let mut owner = Some(root.owner);
        let mut ready = None;
        let mut exit = None;
        client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap()
            .finish()
            .unwrap();
        let ready_result = tokio::time::timeout(Duration::from_secs(2), ready.take().unwrap())
            .await
            .expect("the same original ready source must resolve");
        let mut observer = exit.take().unwrap();
        tokio::time::timeout(
            Duration::from_secs(2),
            std::future::poll_fn(|cx| {
                if observer.view().unwrap().role_ended() {
                    return Poll::Ready(());
                }
                let _ = observer.poll_exit(cx);
                if observer.view().unwrap().role_ended() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("actual initial or replacement dormant Drop must finish unwinding");
        let exact_cause = match observer.view().unwrap().role_result() {
            Some(PollCompletion::Panicked(packet)) => packet
                .value()
                .primary()
                .downcast_ref::<OriginalDormantDropPanic>()
                .is_some_and(|cause| cause.ordinal == panic_ordinal),
            _ => false,
        };
        let output_retained = source.native.try_lock().unwrap().pending_activated.as_ref()
            .is_some_and(super::super::native_active_custody::PendingNativeActivation::has_original_rows_and_slot_owner);
        let before_disposition = target_drops.load(Ordering::SeqCst);
        let pending_returns = observer.view().unwrap().pending_native_returns();
        let no_receipt =
            futures::poll!(std::future::poll_fn(|cx| observer.poll_exit(cx))).is_pending();
        root.business.release();
        drop(ready_result);
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await;
        let before_process_exit = target_drops.load(Ordering::SeqCst);
        let still_staged_before_process_exit = source.native.try_lock().unwrap()
            .pending_activated.as_ref().is_some_and(
                super::super::native_active_custody::PendingNativeActivation::has_original_rows_and_slot_owner);
        // This is an abnormal original-source test. Only the original process
        // author may destroy the SAME slot's Box and then the Source rows.
        supervisor.abandon_for_process_exit();
        let retained = source.native.try_lock().unwrap().pending_activated.take();
        drop(retained); // Only the original rows/Weak receipt; Box stayed in its Slot.
        drop(observer);
        drop(source);
        drop(client);
        let after_disposition = target_drops.load(Ordering::SeqCst);
        acknowledge_all_control(&control);
        let mut control = control;
        control.finish_for_process_exit().await;
        assert!(
            shutdown.is_err(),
            "unhandled original panic cannot be normal shutdown"
        );
        assert!(exact_cause && output_retained && no_receipt);
        assert_eq!(prepares.load(Ordering::SeqCst), panic_ordinal);
        assert_eq!(
            before_disposition, 0,
            "original active output must survive dormant Drop unwind"
        );
        assert_eq!(
            before_process_exit, 0,
            "failed shutdown is not original-source disposition"
        );
        assert!(still_staged_before_process_exit);
        assert!(
            pending_returns > 0,
            "the staged original slot has not returned its Native source"
        );
        assert_eq!(
            after_disposition, 1,
            "only explicit original-source disposition drops the active owner"
        );
    }
}

#[tokio::test]
async fn actual_root_cancel_closes_bank_ingress_after_statement_wake_panics() {
    struct StatementStopCause;
    struct StopWake<T: Send + Sync + 'static> {
        payload: std::sync::Mutex<Option<T>>,
    }
    impl<T: Send + Sync + 'static> std::task::Wake for StopWake<T> {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let payload = self.payload.lock().unwrap().take();
            if let Some(payload) = payload {
                std::panic::panic_any(payload);
            }
        }
    }
    let control = governance_empty();
    let mut control = {
        let (bank, shutdown, mut registry, client) =
            unpolled_test_driver(&control, Arc::new(PanickingOpenNativePort));
        let statements = crate::session_control::StatementProgressSlot::empty();
        statements
            .install(
                crate::query_control::QueryApplicationControl::service(),
                control.statement_source_slots,
            )
            .unwrap();
        let statement_waker = std::task::Waker::from(Arc::new(StopWake {
            payload: std::sync::Mutex::new(Some(StatementStopCause)),
        }));
        let mut cursor = crate::session_control::StatementProgressCursor::default();
        let budget = NonZeroUsize::new(7).unwrap();
        // Prime the real finite scanner using an inert waker. Register the
        // throwing waker only once idle, so it still fails at the actual
        // stop notification rather than an ordinary bounded scan yield.
        let idle_waker = futures::task::noop_waker();
        let mut idle_cx = std::task::Context::from_waker(&idle_waker);
        let mut idle = false;
        for _ in 0..=control.statement_source_slots.get() {
            if statements
                .poll_turn(&mut cursor, budget, &mut idle_cx)
                .is_pending()
            {
                idle = true;
                break;
            }
        }
        assert!(idle, "the actual finite scanner must reach idle");
        assert!(
            statements
                .poll_turn(
                    &mut cursor,
                    budget,
                    &mut std::task::Context::from_waker(&statement_waker)
                )
                .is_pending()
        );
        // Admit the real source before the ingress cut. stop_ingress closes
        // the original WC too, so a new root after cancellation must refuse.
        let root = control.begin_root().await;
        let retirement = OriginalRootPanic::empty();
        // Tokio spawn never synchronously polls; current-thread abort occurs
        // before yielding, while the real pre-poll guard already exists.
        let join = Handle::current().spawn(run_supervisor_owned(
            registry.handle(),
            bank,
            budget,
            shutdown.subscribe(),
            retirement.clone(),
            Arc::clone(&statements),
        ));
        join.abort();
        let original = match join.await {
            Err(original) => original,
            Ok(_) => panic!("the real root was cancelled before first poll"),
        };
        assert!(original.is_cancelled());
        retirement.retain_join_failure(Arc::new(original));
        {
            let packet = retirement.custody();
            assert!(packet.cancelled);
            assert!(
                packet.cleanup[0]
                    .as_ref()
                    .unwrap()
                    .is::<StatementStopCause>()
            );
            assert!(
                packet.cleanup[1].is_none(),
                "the actual bank stop is a silent State cut"
            );
        }
        // The bank stop really happened after the statement notify panicked:
        // its original ingress refuses and preserves the actual source tuple.
        let mut input = Some(
            client
                .prepare_observed_start(completion_request(UnreachablePreparationPort), None)
                .unwrap_or_else(|_| panic!("original source")),
        );
        let mut owner = Some(root.owner);
        let mut ready = None;
        let mut exit = None;
        let refusal = client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap_or_else(|_| panic!("the original refusal packet is constructible"));
        assert!(
            refusal.result().is_err(),
            "the actual original ingress is closed"
        );
        assert!(refusal.finish().is_err());
        assert!(input.is_some() && owner.is_some() && ready.is_none() && exit.is_none());
        drop(input);
        owner
            .take()
            .unwrap()
            .complete_after_terminal_cancel_settled();
        root.business.release();
        drop(client);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        drop(statements);
        retirement.dispose_for_process_exit();
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

// The ordinary Root read belongs to the real logical role, not the optional
// final-ACK tail. This fixture retains the SAME Future until its Drop returns.
#[derive(Default)]
struct RootReadDropSignals {
    expected_window: std::sync::Mutex<Option<novarocks_workload_control::ResultWindowAlias>>,
    blocked: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    requests: std::sync::Mutex<Vec<novarocks_execution_contract::root_result::RootResultRead>>,
    polled: AtomicBool,
    same_window: AtomicBool,
    drop_entered: AtomicBool,
    exited: AtomicBool,
    converged: Arc<AtomicBool>,
}
impl fmt::Debug for RootReadDropSignals {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RootReadDropSignals")
    }
}
struct RootReadDropRescue(Option<std::sync::mpsc::Sender<()>>);
impl RootReadDropRescue {
    fn release(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}
impl Drop for RootReadDropRescue {
    fn drop(&mut self) {
        // Unwinding a failed observation also rescues the actual Drop gate.
        self.release();
    }
}
struct RootReadDropBody {
    signals: Arc<RootReadDropSignals>,
    release: std::sync::mpsc::Receiver<()>,
    window: Option<novarocks_workload_control::ResultWindowAlias>,
}
impl std::future::Future for RootReadDropBody {
    type Output = Result<crate::api::RetainedRootReply, super::super::RootResultFetchFailure>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.signals.polled.store(true, Ordering::SeqCst);
        std::task::Poll::Pending
    }
}
impl Drop for RootReadDropBody {
    fn drop(&mut self) {
        self.signals.drop_entered.store(true, Ordering::SeqCst);
        let _ = self.release.recv();
        drop(self.window.take());
        self.signals.exited.store(true, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct RootReadDropPort(Arc<RootReadDropSignals>);
impl crate::api::BoundedRootReadPort for RootReadDropPort {
    fn read(
        &self,
        request: novarocks_execution_contract::root_result::RootResultRead,
        window: novarocks_workload_control::ResultWindowAlias,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        crate::api::RetainedRootReply,
                        super::super::RootResultFetchFailure,
                    >,
                > + Send,
        >,
    > {
        self.0.requests.lock().unwrap().push(request);
        let expected = self.0.expected_window.lock().unwrap().take().unwrap();
        self.0
            .same_window
            .store(window.shares_capacity_with(&expected), Ordering::SeqCst);
        drop(expected);
        Box::pin(RootReadDropBody {
            signals: Arc::clone(&self.0),
            release: self.0.blocked.lock().unwrap().take().unwrap(),
            window: Some(window),
        })
    }
    fn seal(
        &self,
        _: novarocks_execution_contract::root_lifetime::RootReadSealed,
    ) -> Result<(), QueryExecutionError> {
        Ok(())
    }
}
#[derive(Debug)]
struct RootReadDropPreparation {
    signals: Arc<RootReadDropSignals>,
    window: Option<TestWindow>,
    backend: BackendProcessId,
}
impl TestRowsPreparationPort for RootReadDropPreparation {
    fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
        *self.signals.expected_window.lock().unwrap() = Some(window.clone());
        self.window = Some(TestWindow(window));
    }
}
impl NativeAttemptPreparationPort for RootReadDropPreparation {
    fn prepare_in<'a>(
        &'a mut self,
        request: &'a mut crate::api::NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a> {
        request.stage_fixture_owner(RootReadDropDormant {
            inner: RowsDormantOwner {
                relay_window: self.window.as_ref().unwrap().clone(),
                backend: self.backend,
                converged: Arc::clone(&self.signals.converged),
            },
            signals: Arc::clone(&self.signals),
        });
        Box::pin(async move {
            let scheduling = no_scan_scheduling(request);
            request.bind_staged(scheduling).map_err(Into::into)
        })
    }
}
#[derive(Debug)]
struct RootReadDropDormant {
    inner: RowsDormantOwner,
    signals: Arc<RootReadDropSignals>,
}
impl DormantNativeAttemptOwner for RootReadDropDormant {
    fn poll_bind_qualification(
        &mut self,
        schedule: &AttemptSchedule,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), NativeAttemptActivationFailure>> {
        self.inner.poll_bind_qualification(schedule, cx)
    }
    fn eligible_backends(&self) -> &[BackendProcessId] {
        self.inner.eligible_backends()
    }
    fn activate<'a>(
        &'a mut self,
        schedule: &'a AttemptSchedule,
        admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> NativeAttemptActivationFuture<'a> {
        let signals = Arc::clone(&self.signals);
        let activation = self.inner.activate(schedule, admissions, cancellation);
        Box::pin(async move {
            let activated = activation.await?;
            let (inner, runtime) = activated.into_parts();
            let mut runtime = runtime.expect("the original Rows owner carries its runtime");
            runtime.binding.port = Arc::new(RootReadDropPort(signals));
            Ok(ActivatedNativeAttempt::relayed_rows(
                RootReadDropActive(inner),
                runtime.binding,
                runtime.statuses,
            ))
        })
    }
    fn converge<'a>(
        &'a mut self,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a> {
        self.inner.converge(cancellation)
    }
}
#[derive(Debug)]
struct RootReadDropActive(Box<dyn ActiveNativeAttemptOwner>);
impl ActiveNativeAttemptOwner for RootReadDropActive {
    fn run<'a>(
        &'a mut self,
        drive: &'a NativeAttemptDrive,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> NativeAttemptRunFuture<'a> {
        self.0.run(drive, cancellation)
    }
    fn converge<'a>(
        &'a mut self,
        request: NativeActiveConvergenceRequest,
    ) -> NativeActiveAttemptConvergenceFuture<'a> {
        self.0.converge(request)
    }
}
async fn root_read_drop_fact(flag: &AtomicBool) -> Result<(), &'static str> {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !flag.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| "the original Root read did not reach its fixture phase")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_ordinary_root_read_keeps_same_window_until_its_actual_future_drop_exits() {
    let (release, blocked) = std::sync::mpsc::channel();
    // This sender is owned outside every awaited observation. Normal teardown
    // and unwind both release the worker blocked in the original Future Drop.
    let mut rescue = RootReadDropRescue(Some(release));
    let (control, root) = governance().await;
    let mut control = {
        let scope = root.owner.scope();
        let retirement = root.owner.retirement_observer().unwrap();
        let requester = root.owner.cancellation_requester();
        let signals = Arc::new(RootReadDropSignals::default());
        *signals.blocked.lock().unwrap() = Some(blocked);
        let attempts = RootReadDropPreparation {
            signals: Arc::clone(&signals),
            window: None,
            backend: BackendProcessId::new_v7(),
        };
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x97),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let request = rows_request(&control, &scope, RecoveryMode::NoRecovery, None, attempts);
        let capacity = scope.result_capacity().unwrap();
        let mut input = Some(
            client
                .prepare_observed_start(request, None)
                .unwrap_or_else(|original| std::panic::panic_any(original)),
        );
        let mut owner = Some(root.owner);
        let mut ready = None;
        let mut exit = None;
        client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap()
            .finish()
            .unwrap();
        let mut observer = exit.take().unwrap();
        let mut handle = tokio::time::timeout(Duration::from_secs(2), ready.take().unwrap())
            .await
            .expect("the original Rows handle must be handed off")
            .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("the original request must retain Rows output");
        };
        root_read_drop_fact(&signals.polled).await.unwrap();
        requester.request(CancellationReason::Requested).unwrap();
        let entered = root_read_drop_fact(&signals.drop_entered).await;
        let actual_exit_pending =
            std::future::poll_fn(|cx| std::task::Poll::Ready(observer.poll_exit(cx).is_pending()))
                .await;
        let window_held = capacity.snapshot().held_positions[0] == 1;
        let body_still_held = !signals.exited.load(Ordering::SeqCst);
        let same_window = signals.same_window.load(Ordering::SeqCst);
        let requests_before_release = signals.requests.lock().unwrap().clone();
        // Rescue precedes all assertions, so a failed negative oracle cannot
        // strand the real role inside its destructor or prevent runtime join.
        rescue.release();
        let terminal = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
        drop(stream);
        drop(handle);
        let receipt = tokio::time::timeout(
            Duration::from_secs(2),
            std::future::poll_fn(|cx| observer.poll_exit(cx)),
        )
        .await
        .expect("the same original role must finish actual Drop and convergence")
        .unwrap();
        let actual_drop_exited = signals.exited.load(Ordering::SeqCst);
        let native_converged = signals.converged.load(Ordering::SeqCst);
        let local = receipt.local_disposition();
        let remote = receipt.remote_disposition();
        let native_body_exited = receipt.view().native_preparation_exited();
        let same_requests = signals.requests.lock().unwrap().len() == requests_before_release.len();
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await;
        let footer_still_held = !retirement.is_reclaimed().unwrap();
        drop(receipt);
        drop(observer);
        acknowledge_all_control(&control);
        let settled = tokio::time::timeout(Duration::from_secs(2), scope.wait_released()).await;
        assert!(entered.is_ok());
        assert!(actual_exit_pending && body_still_held && window_held && same_window);
        assert_eq!(requests_before_release.len(), 1);
        assert!(requests_before_release[0].wanted().is_some());
        assert_eq!(requests_before_release[0].consumed(), 0);
        assert!(
            same_requests,
            "cancellation must not issue a next read or an ACK-only request"
        );
        assert!(
            matches!(terminal, Ok(Err(_))),
            "the real cancelled stream must retain failure"
        );
        assert!(actual_drop_exited && native_converged && native_body_exited);
        assert_eq!(
            local,
            crate::api::LocalExecutionDisposition::OriginalPaidOwnerReturn
        );
        assert_eq!(
            remote,
            crate::api::RemoteExecutionDisposition::RemoteNotReported
        );
        assert!(shutdown.is_ok() && settled.is_ok());
        assert!(
            footer_still_held,
            "the actual receipt remains the original footer owner"
        );
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}
