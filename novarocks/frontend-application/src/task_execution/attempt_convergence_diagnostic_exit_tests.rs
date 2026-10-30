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

// Included inside attempt_convergence::tests to use its original TaskRound fixtures.
mod diagnostic_exit_tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    use crate::native::fragment_transport::{
        DynamicFilterRead, DynamicFilterReadError, FinalTaskInfoRead, TaskResultTransport,
    };
    use crate::task_execution::profile_diagnostics::TaskFinalInfoCollector;
    use crate::task_execution::status_intake::NotifyWake;
    use novarocks_execution_contract::{DomainVersion, TaskIdentity};

    #[derive(Default)]
    struct FinalReadProbe {
        started: Mutex<Vec<TaskIdentity>>,
        completed: Mutex<Vec<TaskIdentity>>,
        dropped: Mutex<Vec<TaskIdentity>>,
    }

    struct OriginalReadGuard {
        task: TaskIdentity,
        probe: Arc<FinalReadProbe>,
    }

    impl Drop for OriginalReadGuard {
        fn drop(&mut self) {
            self.probe.dropped.lock().unwrap().push(self.task);
        }
    }

    struct GatedOriginalFinalInfo {
        gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        probe: Arc<FinalReadProbe>,
    }

    impl TaskResultTransport for GatedOriginalFinalInfo {
        fn final_task_info(
            &self,
            task: TaskIdentity,
        ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
            let gate = self
                .gate
                .lock()
                .unwrap()
                .take()
                .expect("only the original final-info request may start");
            self.probe.started.lock().unwrap().push(task);
            let guard = OriginalReadGuard {
                task,
                probe: Arc::clone(&self.probe),
            };
            Box::pin(async move {
                let _original_guard = guard;
                gate.await
                    .map_err(|_| "the original final-info fixture gate closed".to_owned())?;
                self.probe.completed.lock().unwrap().push(task);
                Ok(FinalTaskInfoRead::Unavailable(OperationOutcome::Gone))
            })
        }

        fn dynamic_filters(
            &self,
            _: TaskIdentity,
            _: Option<DomainVersion>,
        ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
            panic!("final-info observation must not fetch dynamic filters")
        }
    }

    async fn original_final_info_wait_yields_after_remote_ready(expire_before_release: bool) {
        let (mut round, acks, release) =
            crate::task_execution::tests::manifest_normal_release_ack_regression_fixture();
        let context = *round.execution().graph().contexts().next().unwrap();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let mut owner = original_convergence(&round);
        let notify = Arc::new(tokio::sync::Notify::new());
        let (release_read, read_gate) = tokio::sync::oneshot::channel();
        let probe = Arc::new(FinalReadProbe::default());
        let transport: Arc<dyn TaskResultTransport> = Arc::new(GatedOriginalFinalInfo {
            gate: Mutex::new(Some(read_gate)),
            probe: Arc::clone(&probe),
        });
        let mut collector = TaskFinalInfoCollector::new(
            transport,
            true,
            &round,
            Arc::new(NotifyWake::new(Arc::clone(&notify))),
        );
        std::future::poll_fn(|cx| {
            collector.poll_observe(&round, cx);
            Poll::Ready(())
        })
        .await;
        let original = probe.started.lock().unwrap().clone();
        assert_eq!(original.len(), 1);
        assert!(!collector.is_closed());
        assert!(!collector.unread.is_empty());
        // Exercise the original retained-read state after collection stops.
        // This does not fabricate a Ready token or claim snapshot-cut coverage.
        collector.stop_starting();

        let control = novarocks_workload_control::WorkloadControl::try_new(
            novarocks_workload_control::WorkloadConfig::default(),
            novarocks_workload_control::ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 10,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(novarocks_workload_control::WorkRequest::new(
                novarocks_workload_control::WorkClass::Query,
            ))
            .unwrap();
        let cancellation = root.owner.scope().cancellation().unwrap();
        let first_deadline = std::time::Instant::now() - Duration::from_secs(1);
        if expire_before_release {
            let mut borrowing_wait = Box::pin(owner.drive(
                &mut round,
                &split_delivery,
                NativeAttemptConvergenceMode::Normal,
                NativeActiveConvergenceRequest::new(cancellation.clone(), first_deadline),
                &notify,
                Some(&mut collector),
            ));
            assert!(futures::poll!(borrowing_wait.as_mut()).is_pending());
            drop(borrowing_wait);
            assert_eq!(owner.remote_deadline, Some(first_deadline));
            assert!(matches!(
                owner.remote_closure,
                Some(NativeActiveConvergenceClosure::RemoteTrackingEnded { .. })
            ));
            assert!(!collector.is_closed());
            assert!(probe.completed.lock().unwrap().is_empty());
            assert!(probe.dropped.lock().unwrap().is_empty());
            assert_eq!(*probe.started.lock().unwrap(), original);
        }

        // Settle the exact original Release and covered physical-exit evidence.
        // Neither is substituted by a wall-clock delay or a dropped TaskRound.
        let OperationIntent::ReleaseQueryContext(request) = &release else {
            unreachable!("the fixture selects its exact Release")
        };
        acks.publish(OperationAcknowledgement::new(
            release.operation_id(),
            OperationKind::ReleaseQueryContext,
            OperationOutcome::Accepted,
            AckPayload::Release {
                receipt: QueryContextReceipt::new(
                    request.context(),
                    QueryContextState::TerminalRetained,
                ),
                outcome: ReleaseOutcome::Released,
                runtime_filter: Some(
                    crate::task_execution::tests::fixture_runtime_filter_contribution(),
                ),
            },
        ));
        round.turn().expect("the original Release ACK settles");
        publish_exit(&mut round, context);
        owner.begin(
            &mut round,
            &split_delivery,
            NativeAttemptConvergenceMode::Normal,
        );
        assert!(round.attempt_drained());
        assert!(round.pumps_closed());
        assert!(owner.observed_contexts(&round).is_some());
        assert!(!collector.is_closed());
        assert!(probe.completed.lock().unwrap().is_empty());
        assert!(probe.dropped.lock().unwrap().is_empty());

        let release_read = Arc::new(Mutex::new(Some(release_read)));
        let timer_ran = Arc::new(AtomicBool::new(false));
        let timer_saw_original_held = Arc::new(AtomicBool::new(false));
        let watchdog_fired = Arc::new(AtomicBool::new(false));
        let (watchdog_stop, watchdog_stopped) = std::sync::mpsc::channel();
        // This is only a test-harness escape hatch for the historical busy loop.
        // It is not either product deadline and never proves remote/local exit.
        let watchdog = {
            let gate = Arc::clone(&release_read);
            let fired = Arc::clone(&watchdog_fired);
            std::thread::spawn(move || {
                if matches!(
                    watchdog_stopped.recv_timeout(Duration::from_secs(5)),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                ) {
                    fired.store(true, Ordering::SeqCst);
                    if let Some(gate) = gate.lock().unwrap().take() {
                        let _ = gate.send(());
                    }
                }
            })
        };
        // This task and convergence use the SAME current-thread Tokio worker.
        // A synchronous Ready -> continue loop prevents this timer from firing.
        let sibling = {
            let gate = Arc::clone(&release_read);
            let ran = Arc::clone(&timer_ran);
            let held = Arc::clone(&timer_saw_original_held);
            let probe = Arc::clone(&probe);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                held.store(
                    probe.completed.lock().unwrap().is_empty()
                        && probe.dropped.lock().unwrap().is_empty(),
                    Ordering::SeqCst,
                );
                ran.store(true, Ordering::SeqCst);
                if let Some(gate) = gate.lock().unwrap().take() {
                    let _ = gate.send(());
                }
            })
        };
        let later_deadline = std::time::Instant::now() + Duration::from_secs(60);
        let result = owner
            .drive(
                &mut round,
                &split_delivery,
                NativeAttemptConvergenceMode::Normal,
                NativeActiveConvergenceRequest::new(cancellation, later_deadline),
                &notify,
                Some(&mut collector),
            )
            .await;
        let sibling_ran_before_return = timer_ran.load(Ordering::SeqCst);
        let _ = watchdog_stop.send(());
        watchdog.join().expect("the bounded watchdog exits");
        sibling.await.expect("the sibling task actually exits");
        let closure = result.expect("original local work exits normally");

        assert!(
            !watchdog_fired.load(Ordering::SeqCst),
            "convergence monopolized the worker until the external watchdog released its RPC"
        );
        assert!(sibling_ran_before_return);
        assert!(timer_saw_original_held.load(Ordering::SeqCst));
        assert_eq!(*probe.started.lock().unwrap(), original);
        assert_eq!(*probe.completed.lock().unwrap(), original);
        assert_eq!(*probe.dropped.lock().unwrap(), original);
        assert!(collector.is_closed());
        assert!(!collector.all_terminal_observed());
        assert!(collector.collected.is_empty());
        if expire_before_release {
            assert_eq!(owner.remote_deadline, Some(first_deadline));
            match closure {
                NativeActiveConvergenceClosure::RemoteTrackingEnded { exact_contexts } => {
                    assert_eq!(exact_contexts.as_ref(), &[context]);
                }
                other => panic!("reentry replaced the first remote disposition: {other:?}"),
            }
        } else {
            assert_eq!(owner.remote_deadline, Some(later_deadline));
            assert!(matches!(
                closure,
                NativeActiveConvergenceClosure::Evidence(_)
            ));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remote_ready_with_original_final_info_pending_yields_until_actual_read_exit() {
        original_final_info_wait_yields_after_remote_ready(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remote_deadline_and_reentry_do_not_retire_or_reborrow_original_final_info_read() {
        original_final_info_wait_yields_after_remote_ready(true).await;
    }

    include!("attempt_convergence_profile_snapshot_tests.rs");
}
