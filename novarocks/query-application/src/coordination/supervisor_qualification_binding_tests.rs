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

#[derive(Debug, Default)]
struct QualificationBindingGate {
    open: AtomicBool,
    entered: AtomicU64,
    closed: AtomicU64,
    wake: std::sync::Mutex<Option<std::task::Waker>>,
}
impl QualificationBindingGate {
    fn poll(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), NativeAttemptActivationFailure>> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        *self.wake.lock().unwrap() = Some(cx.waker().clone());
        if self.open.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn release(&self) {
        self.open.store(true, Ordering::SeqCst);
        let wake = self.wake.lock().unwrap().take();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

#[derive(Debug)]
struct BindingGatePreparation {
    window: Option<TestWindow>,
    backends: [BackendProcessId; 2],
    prepares: Arc<AtomicU64>,
    runs: Arc<AtomicU64>,
    closes: Arc<AtomicU64>,
    binding: Arc<QualificationBindingGate>,
}
impl TestRowsPreparationPort for BindingGatePreparation {
    fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
        self.window = Some(TestWindow(window));
    }
}
impl NativeAttemptPreparationPort for BindingGatePreparation {
    fn prepare_in<'a>(
        &'a mut self,
        request: &'a mut crate::api::NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a> {
        let ordinal = self.prepares.fetch_add(1, Ordering::SeqCst) + 1;
        let inner = RecoveringRowsDormantOwner {
            relay_window: self.window.as_ref().unwrap().clone(),
            backend: self.backends[usize::try_from(ordinal - 1).unwrap()],
            ordinal,
            runs: Arc::clone(&self.runs),
            convergences: Arc::clone(&self.closes),
            allow_initial_convergence: Arc::new(tokio::sync::Notify::new()),
            block_initial_convergence: false,
            replacement_missing_rows: false,
            replacement_activation_panic: false,
            failures_before_success: 1,
        };
        request.stage_fixture_owner(BindingGateDormant {
            inner,
            binding: Arc::clone(&self.binding),
        });
        Box::pin(async move {
            let scheduling = no_scan_scheduling(request);
            request.bind_staged(scheduling).map_err(Into::into)
        })
    }
}
#[derive(Debug)]
struct BindingGateDormant {
    inner: RecoveringRowsDormantOwner,
    binding: Arc<QualificationBindingGate>,
}
impl DormantNativeAttemptOwner for BindingGateDormant {
    fn eligible_backends(&self) -> &[BackendProcessId] {
        self.inner.eligible_backends()
    }
    fn poll_bind_qualification(
        &mut self,
        schedule: &AttemptSchedule,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), NativeAttemptActivationFailure>> {
        assert!(
            schedule
                .contexts()
                .iter()
                .all(|c| c.backend_process_id() == self.inner.backend)
        );
        self.binding.poll(cx)
    }
    fn activate<'a>(
        &'a mut self,
        schedule: &'a AttemptSchedule,
        admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> NativeAttemptActivationFuture<'a> {
        if self.inner.ordinal > 1 {
            assert!(self.binding.open.load(Ordering::SeqCst));
        }
        self.inner.activate(schedule, admissions, cancellation)
    }
    fn converge<'a>(
        &'a mut self,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a> {
        self.binding.closed.fetch_add(1, Ordering::SeqCst);
        self.inner.converge(cancellation)
    }
}

#[derive(Debug)]
struct BindingCheckedQualifier {
    inner: ImmediateReplacementPort,
    binding: Arc<QualificationBindingGate>,
    expected: BackendProcessId,
    submissions: Arc<AtomicU64>,
}
#[derive(Debug)]
struct BindingCheckedQualification {
    inner: Box<dyn super::super::ReplacementQualificationEffectReservation>,
    binding: Arc<QualificationBindingGate>,
    expected: BackendProcessId,
    submissions: Arc<AtomicU64>,
}
impl super::super::ReplacementQualificationEffectReservation for BindingCheckedQualification {
    fn submit(self: Box<Self>, submission: super::super::ReplacementQualificationEffectSubmission) {
        assert!(
            self.binding.open.load(Ordering::SeqCst),
            "qualification must never precede successful original binding"
        );
        assert!(
            submission
                .request()
                .replacement_contexts()
                .iter()
                .all(|c| c.backend_process_id() == self.expected)
        );
        self.submissions.fetch_add(1, Ordering::SeqCst);
        self.inner.submit(submission);
    }
}
impl super::super::ReplacementQualificationEffectPort for BindingCheckedQualifier {
    fn subscribe_capacity(&self) -> watch::Receiver<u64> {
        super::super::ReplacementQualificationEffectPort::subscribe_capacity(&self.inner)
    }
    fn try_reserve(
        &self,
        request: &super::super::ReplacementQualificationRequest,
    ) -> super::super::ReplacementQualificationEffectAdmission {
        match super::super::ReplacementQualificationEffectPort::try_reserve(&self.inner, request) {
            super::super::ReplacementQualificationEffectAdmission::Admitted(inner) => {
                super::super::ReplacementQualificationEffectAdmission::Admitted(Box::new(
                    BindingCheckedQualification {
                        inner,
                        binding: Arc::clone(&self.binding),
                        expected: self.expected,
                        submissions: Arc::clone(&self.submissions),
                    },
                ))
            }
            other => other,
        }
    }
}

#[tokio::test]
async fn successor_qualification_is_blocked_by_original_binding_and_pending_cancel_sends_nothing() {
    for release in [false, true] {
        let mut control = {
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let gate = Arc::new(QualificationBindingGate::default());
            let submissions = Arc::new(AtomicU64::new(0));
            let runs = Arc::new(AtomicU64::new(0));
            let backends = [BackendProcessId::new_v7(), BackendProcessId::new_v7()];
            let attempts = BindingGatePreparation {
                window: None,
                backends,
                prepares: Arc::new(AtomicU64::new(0)),
                runs: Arc::clone(&runs),
                closes: Arc::new(AtomicU64::new(0)),
                binding: Arc::clone(&gate),
            };
            let qualifier = Arc::new(BindingCheckedQualifier {
                inner: ImmediateReplacementPort::default(),
                binding: Arc::clone(&gate),
                expected: backends[1],
                submissions: Arc::clone(&submissions),
            });
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0xb1 + u64::from(release)),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );
            let mut handle = client
                .start(
                    rows_request(
                        &control,
                        &scope,
                        RecoveryMode::RestartAttemptBeforeVisibility,
                        Some(qualifier),
                        attempts,
                    ),
                    root.owner,
                )
                .await
                .unwrap();
            let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
                panic!("original Rows stream");
            };
            tokio::time::timeout(Duration::from_secs(1), async {
                while gate.entered.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("replacement must poll its original binding before qualification");
            assert_eq!(submissions.load(Ordering::SeqCst), 0);
            assert_eq!(runs.load(Ordering::SeqCst), 1);
            stream.begin_schema().unwrap().complete();
            if release {
                gate.release();
                let crate::api::ResultDelivery::Segment(segment) =
                    tokio::time::timeout(Duration::from_secs(1), stream.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap()
                else {
                    panic!("qualified successor must deliver");
                };
                segment.complete();
                assert_eq!(submissions.load(Ordering::SeqCst), 1);
                assert_eq!(runs.load(Ordering::SeqCst), 2);
            }
            handle.request_cancel().unwrap();
            let terminal = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap();
            assert!(terminal.is_err());
            drop(stream);
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            drop(handle);
            scope.wait_released().await;
            assert_eq!(submissions.load(Ordering::SeqCst), u64::from(release));
            if !release {
                assert_eq!(
                    gate.closed.load(Ordering::SeqCst),
                    1,
                    "original waiting dormant owner actually converged"
                );
            }
            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }
}

#[derive(Debug)]
struct QualificationPollPanicOwner {
    inner: SuccessfulCompletionDormantOwner,
    drops: Arc<AtomicU64>,
    polls: Arc<AtomicU64>,
    closes: Arc<AtomicU64>,
}
impl DormantNativeAttemptOwner for QualificationPollPanicOwner {
    fn eligible_backends(&self) -> &[BackendProcessId] {
        self.inner.eligible_backends()
    }
    fn poll_bind_qualification(
        &mut self,
        _: &AttemptSchedule,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), NativeAttemptActivationFailure>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(DormantOriginalPanic(Arc::clone(&self.drops)));
    }
    fn activate<'a>(
        &'a mut self,
        _: &'a AttemptSchedule,
        _: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
        _: novarocks_workload_control::CancellationView,
    ) -> NativeAttemptActivationFuture<'a> {
        Box::pin(async {
            panic!("a failed original binding cannot activate");
        })
    }
    fn converge<'a>(
        &'a mut self,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.inner.converge(cancellation)
    }
}
#[tokio::test]
async fn qualification_poll_panic_stays_in_original_custody_through_convergence() {
    let mut control = {
        let (control, root) = governance().await;
        let (_, attempts, _, _, _) = failing_native();
        let request = completion_request(attempts);
        let facts = crate::api::ExecutionSchedulingFacts::from_frozen_description(
            request.description(),
            &std::collections::BTreeMap::new(),
        )
        .unwrap();
        let execution = QueryExecutionId::new(
            novarocks_types::QueryId::new(0xb3, 0x19),
            AttemptId::new(1).unwrap(),
        )
        .unwrap();
        let frontend = FrontendProcessId::new_v7();
        let backend = BackendProcessId::new_v7();
        let schedule = build_attempt_schedule(
            execution,
            frontend,
            &[backend],
            &facts,
            #[cfg(novarocks_dev_controls)]
            Ok(None),
        )
        .unwrap();
        let drops = Arc::new(AtomicU64::new(0));
        let polls = Arc::new(AtomicU64::new(0));
        let closes = Arc::new(AtomicU64::new(0));
        let mut custody =
            crate::api::NativeDormantCustody::test_owner(QualificationPollPanicOwner {
                inner: SuccessfulCompletionDormantOwner {
                    backend,
                    expected_frontend: frontend,
                },
                drops: Arc::clone(&drops),
                polls: Arc::clone(&polls),
                closes: Arc::clone(&closes),
            });
        let cancellation = root.owner.scope().cancellation().unwrap();
        let result = bind_dormant_qualification(
            &mut custody,
            &schedule,
            &cancellation,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert!(result.is_err());
        assert!(
            custody
                .activation_panic()
                .unwrap()
                .is::<DormantOriginalPanic>()
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(
            bind_dormant_qualification(
                &mut custody,
                &schedule,
                &cancellation,
                tokio::time::Instant::now() + Duration::from_secs(1)
            )
            .await
            .is_err()
        );
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let (_sender, mut shutdown) = watch::channel(false);
        assert!(
            converge_dormant(
                &mut custody,
                cancellation,
                &mut shutdown,
                &root.owner.cancellation_requester()
            )
            .await
            .is_err()
        );
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(custody.retire().is_err());
        drop(custody); // Explicit fixture disposition of the original opaque cause.
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        root.owner.complete();
        root.business.release();
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}
