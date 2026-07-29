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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use novarocks::UniqueId;
    use novarocks::query_execution::artifact::{
        RuntimeFilterAbortEnvelope, RuntimeFilterDeploymentDispatcher, RuntimeFilterInstallEnvelope,
    };
    use novarocks::query_execution::contract::QueryId;
    use novarocks::query_execution::contract_test_support::{
        assert_profile_outcome_preserved, assert_result_outcome_preserved,
        assert_write_abort_reason, assert_write_outcome_preserved,
        non_empty_profile_contract_fixture,
        non_empty_profile_contract_fixture_with_query_timeout_seconds,
        non_empty_result_contract_fixture, non_empty_runtime_filter_contract_fixture,
        non_empty_write_contract_fixture,
        non_empty_write_contract_fixture_with_query_timeout_seconds,
    };
    use novarocks::query_execution::fragment_transport::{
        FetchOutcome, FetchedQueryBatch, FragmentDispatcher, NativeFragmentEnvelope,
    };
    use novarocks::query_execution::write::NativeExecutionReport;

    use crate::coordinator::FrontendDistributedQueryCoordinator;
    use crate::coordinator::query_registry::FrontendQueryRegistry;
    use crate::coordinator::scheduler::{FrontendBackendSnapshot, FrontendFragmentScheduler};
    use crate::topology::FrontendTopologyController;

    fn report_endpoint() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19040)
    }

    struct NoopRuntimeFilterDispatcher;

    impl RuntimeFilterDeploymentDispatcher for NoopRuntimeFilterDispatcher {
        fn install(
            &self,
            _backend_idx: usize,
            _endpoint: SocketAddr,
            _participant_id: u32,
            _deadline: Duration,
            _envelope: RuntimeFilterInstallEnvelope,
        ) -> Result<(), String> {
            Ok(())
        }

        fn abort(
            &self,
            _backend_idx: usize,
            _endpoint: SocketAddr,
            _participant_id: u32,
            _deadline: Duration,
            _envelope: RuntimeFilterAbortEnvelope,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    struct RecordingRuntimeFilterDispatcher {
        install_calls: AtomicUsize,
        abort_calls: AtomicUsize,
        fail_install_call: Option<usize>,
        fail_aborts: bool,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    struct ConcurrentInstallGate {
        expected: usize,
        entered: Mutex<usize>,
        wake: Condvar,
    }

    impl ConcurrentInstallGate {
        fn new(expected: usize) -> Self {
            Self {
                expected,
                entered: Mutex::new(0),
                wake: Condvar::new(),
            }
        }
    }

    impl RuntimeFilterDeploymentDispatcher for ConcurrentInstallGate {
        fn install(
            &self,
            _backend_idx: usize,
            _endpoint: SocketAddr,
            _participant_id: u32,
            _deadline: Duration,
            _envelope: RuntimeFilterInstallEnvelope,
        ) -> Result<(), String> {
            let mut entered = self.entered.lock().unwrap();
            *entered += 1;
            self.wake.notify_all();
            let (entered, timeout) = self
                .wake
                .wait_timeout_while(entered, Duration::from_millis(250), |entered| {
                    *entered < self.expected
                })
                .unwrap();
            if timeout.timed_out() && *entered < self.expected {
                Err(format!(
                    "runtime-filter install fanout was serialized: entered {} of {}",
                    *entered, self.expected
                ))
            } else {
                Ok(())
            }
        }

        fn abort(
            &self,
            _backend_idx: usize,
            _endpoint: SocketAddr,
            _participant_id: u32,
            _deadline: Duration,
            _envelope: RuntimeFilterAbortEnvelope,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    struct ConcurrentAbortGate {
        failing_participant: u32,
        expected: usize,
        entered: Mutex<usize>,
        wake: Condvar,
        timed_out: AtomicBool,
    }

    impl ConcurrentAbortGate {
        fn new(failing_participant: u32, expected: usize) -> Self {
            Self {
                failing_participant,
                expected,
                entered: Mutex::new(0),
                wake: Condvar::new(),
                timed_out: AtomicBool::new(false),
            }
        }
    }

    impl RuntimeFilterDeploymentDispatcher for ConcurrentAbortGate {
        fn install(
            &self,
            _backend_idx: usize,
            _endpoint: SocketAddr,
            participant_id: u32,
            _deadline: Duration,
            _envelope: RuntimeFilterInstallEnvelope,
        ) -> Result<(), String> {
            if participant_id == self.failing_participant {
                Err("injected concurrent install failure".to_string())
            } else {
                Ok(())
            }
        }

        fn abort(
            &self,
            _backend_idx: usize,
            _endpoint: SocketAddr,
            _participant_id: u32,
            _deadline: Duration,
            _envelope: RuntimeFilterAbortEnvelope,
        ) -> Result<(), String> {
            let mut entered = self.entered.lock().unwrap();
            *entered += 1;
            self.wake.notify_all();
            let (entered, timeout) = self
                .wake
                .wait_timeout_while(entered, Duration::from_millis(250), |entered| {
                    *entered < self.expected
                })
                .unwrap();
            if timeout.timed_out() && *entered < self.expected {
                self.timed_out.store(true, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    impl RecordingRuntimeFilterDispatcher {
        fn new(fail_install_call: Option<usize>, events: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                install_calls: AtomicUsize::new(0),
                abort_calls: AtomicUsize::new(0),
                fail_install_call,
                fail_aborts: false,
                events,
            }
        }

        fn with_abort_failures(
            fail_install_call: usize,
            events: Arc<Mutex<Vec<&'static str>>>,
        ) -> Self {
            Self {
                install_calls: AtomicUsize::new(0),
                abort_calls: AtomicUsize::new(0),
                fail_install_call: Some(fail_install_call),
                fail_aborts: true,
                events,
            }
        }
    }

    impl RuntimeFilterDeploymentDispatcher for RecordingRuntimeFilterDispatcher {
        fn install(
            &self,
            _backend_idx: usize,
            _endpoint: SocketAddr,
            _participant_id: u32,
            _deadline: Duration,
            _envelope: RuntimeFilterInstallEnvelope,
        ) -> Result<(), String> {
            let call = self.install_calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.events.lock().unwrap().push("install");
            if self.fail_install_call == Some(call) {
                Err(format!("injected install failure {call}"))
            } else {
                Ok(())
            }
        }

        fn abort(
            &self,
            _backend_idx: usize,
            _endpoint: SocketAddr,
            _participant_id: u32,
            _deadline: Duration,
            _envelope: RuntimeFilterAbortEnvelope,
        ) -> Result<(), String> {
            let call = self.abort_calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.events.lock().unwrap().push("abort");
            if self.fail_aborts {
                Err(format!("injected abort failure {call}"))
            } else {
                Ok(())
            }
        }
    }

    #[derive(Default)]
    struct RecordingDispatcher {
        submissions: Mutex<Vec<(usize, UniqueId)>>,
        submission_reporting: Mutex<Vec<(bool, bool)>>,
        fetches: Mutex<Vec<(usize, UniqueId)>>,
        cancellations: Mutex<Vec<(usize, Vec<UniqueId>)>>,
        cancellation_query_ids: Mutex<Vec<QueryId>>,
        outcomes: Mutex<VecDeque<FetchOutcome>>,
        fail_on_submit: Option<usize>,
        cancel_on_submit: Mutex<
            Option<(
                usize,
                novarocks::query_execution::cancellation::QueryCancellationSource,
            )>,
        >,
        report_on_submit: Mutex<Option<(usize, Box<dyn FnOnce() + Send>)>>,
        events: Option<Arc<Mutex<Vec<&'static str>>>>,
    }

    impl RecordingDispatcher {
        fn with_result(batch: FetchedQueryBatch) -> Self {
            Self {
                outcomes: Mutex::new(VecDeque::from([
                    FetchOutcome::Ready(batch),
                    FetchOutcome::Eof,
                ])),
                ..Self::default()
            }
        }

        fn with_results(batches: Vec<FetchedQueryBatch>) -> Self {
            let outcomes = batches
                .into_iter()
                .flat_map(|batch| [FetchOutcome::Ready(batch), FetchOutcome::Eof])
                .collect();
            Self {
                outcomes: Mutex::new(outcomes),
                ..Self::default()
            }
        }

        fn with_result_and_events(
            batch: FetchedQueryBatch,
            events: Arc<Mutex<Vec<&'static str>>>,
        ) -> Self {
            Self {
                outcomes: Mutex::new(VecDeque::from([
                    FetchOutcome::Ready(batch),
                    FetchOutcome::Eof,
                ])),
                events: Some(events),
                ..Self::default()
            }
        }

        fn with_submit_failure(
            fail_on_submit: usize,
            events: Arc<Mutex<Vec<&'static str>>>,
        ) -> Self {
            Self {
                fail_on_submit: Some(fail_on_submit),
                events: Some(events),
                ..Self::default()
            }
        }

        fn with_result_and_cancellation(
            batch: FetchedQueryBatch,
            cancellation: novarocks::query_execution::cancellation::QueryCancellationSource,
            submit_count: usize,
        ) -> Self {
            Self {
                outcomes: Mutex::new(VecDeque::from([
                    FetchOutcome::Ready(batch),
                    FetchOutcome::Eof,
                ])),
                cancel_on_submit: Mutex::new(Some((submit_count, cancellation))),
                ..Self::default()
            }
        }

        fn with_cancellation(
            cancellation: novarocks::query_execution::cancellation::QueryCancellationSource,
            submit_count: usize,
        ) -> Self {
            Self {
                cancel_on_submit: Mutex::new(Some((submit_count, cancellation))),
                ..Self::default()
            }
        }

        fn report_on_submit(
            &self,
            submit_count: usize,
            report: NativeExecutionReport,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            self.reports_on_submit(submit_count, vec![report], coordinator);
        }

        fn native_wire_report_on_submit(
            &self,
            submit_count: usize,
            report: novarocks::proto::novarocks::ExecStatusReport,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            let handler = coordinator.report_handler();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    novarocks::query_execution::report::NativeReportHandler::handle_native_report(
                        &handler, report,
                    )
                    .expect("frontend native report ingress accepts the active query");
                }),
            ));
        }

        fn reports_on_submit(
            &self,
            submit_count: usize,
            reports: Vec<NativeExecutionReport>,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            let handler = coordinator.report_handler();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    for report in reports {
                        handler
                            .handle_native_report(report)
                            .expect("frontend report handler accepts active query report");
                    }
                }),
            ));
        }

        fn delayed_reports_on_submit(
            &self,
            submit_count: usize,
            reports: Vec<NativeExecutionReport>,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            let handler = coordinator.report_handler();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    std::thread::spawn(move || {
                        std::thread::sleep(Duration::from_millis(25));
                        for report in reports {
                            handler.handle_native_report(report).expect(
                                "frontend report handler accepts delayed active query report",
                            );
                        }
                    });
                }),
            ));
        }

        fn rejected_report_on_submit(
            &self,
            submit_count: usize,
            report: NativeExecutionReport,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            let handler = coordinator.report_handler();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    let error = handler
                        .handle_native_report(report)
                        .expect_err("unexpected writer output must be rejected");
                    assert_eq!(
                        error.kind(),
                        novarocks::query_execution::contract::DistributedQueryErrorKind::ContractViolation
                    );
                }),
            ));
        }

        fn accepted_then_rejected_reports_on_submit(
            &self,
            submit_count: usize,
            accepted: NativeExecutionReport,
            rejected: NativeExecutionReport,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            let handler = coordinator.report_handler();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    handler
                        .handle_native_report(accepted)
                        .expect("first writer final is accepted");
                    let error = handler
                        .handle_native_report(rejected)
                        .expect_err("conflicting writer final must be rejected immediately");
                    assert_eq!(
                        error.kind(),
                        novarocks::query_execution::contract::DistributedQueryErrorKind::ContractViolation
                    );
                }),
            ));
        }

        fn backend_loss_on_submit(
            &self,
            submit_count: usize,
            backend_idx: usize,
            coordinator: &FrontendDistributedQueryCoordinator,
        ) {
            let activity = coordinator.backend_query_activity();
            *self.report_on_submit.lock().unwrap() = Some((
                submit_count,
                Box::new(move || {
                    assert_eq!(activity.backend_lost(backend_idx).len(), 1);
                }),
            ));
        }
    }

    impl FragmentDispatcher for RecordingDispatcher {
        fn submit_fragment(
            &self,
            backend_idx: usize,
            submission: NativeFragmentEnvelope,
        ) -> Result<(), String> {
            let mut submissions = self.submissions.lock().unwrap();
            if let Some(events) = &self.events {
                events.lock().unwrap().push("submit");
            }
            submissions.push((backend_idx, submission.fragment_instance_id()?));
            self.submission_reporting.lock().unwrap().push((
                submission.has_report_endpoint(),
                submission.uses_typed_result_sink(),
            ));
            let should_cancel = self
                .cancel_on_submit
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|(submit_count, _)| submissions.len() == *submit_count);
            if should_cancel {
                if let Some((_, cancellation)) = self.cancel_on_submit.lock().unwrap().take() {
                    let _ = cancellation.request(
                        novarocks::query_execution::cancellation::QueryCancellationReason::ClientDisconnected,
                    );
                }
            }
            let report_callback = {
                let mut report_on_submit = self.report_on_submit.lock().unwrap();
                if report_on_submit
                    .as_ref()
                    .is_some_and(|(submit_count, _)| submissions.len() == *submit_count)
                {
                    report_on_submit.take().map(|(_, callback)| callback)
                } else {
                    None
                }
            };
            drop(submissions);
            if let Some(report_callback) = report_callback {
                report_callback();
            }
            if self.fail_on_submit == Some(self.submissions.lock().unwrap().len()) {
                Err("injected submit failure with unknown remote outcome".to_string())
            } else {
                Ok(())
            }
        }

        fn fetch_result(
            &self,
            backend_idx: usize,
            finst_id: UniqueId,
            _max_wait_ms: i64,
            _expected_output_schema: Option<
                novarocks::query_execution::fragment_transport::ExpectedOutputSchemaView<'_>,
            >,
        ) -> Result<FetchOutcome, String> {
            self.fetches.lock().unwrap().push((backend_idx, finst_id));
            Ok(self
                .outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(FetchOutcome::Eof))
        }

        fn cancel_fragments(&self, backend_idx: usize, query_id: QueryId, finst_ids: &[UniqueId]) {
            if let Some(events) = &self.events {
                events.lock().unwrap().push("cancel");
            }
            self.cancellation_query_ids.lock().unwrap().push(query_id);
            self.cancellations
                .lock()
                .unwrap()
                .push((backend_idx, finst_ids.to_vec()));
        }

        fn backend_count(&self) -> usize {
            2
        }

        fn needs_fragment_status_report(&self) -> bool {
            true
        }
    }

    fn test_coordinator(
        query_id: QueryId,
        scheduler: FrontendFragmentScheduler,
        dispatcher: Arc<RecordingDispatcher>,
    ) -> FrontendDistributedQueryCoordinator {
        FrontendDistributedQueryCoordinator::new_for_test(
            query_id,
            report_endpoint(),
            scheduler,
            dispatcher,
            NonZeroUsize::new(1).unwrap(),
            Arc::new(NoopRuntimeFilterDispatcher),
        )
    }

    #[test]
    fn frontend_consumes_non_empty_result_contract() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(41, 73), scheduler, dispatcher.clone());

        let outcome = coordinator
            .execute(request)
            .expect("frontend executes fixture");

        assert_result_outcome_preserved(outcome, 1).expect("engine consumes Result payload");
        assert!(!dispatcher.submissions.lock().unwrap().is_empty());
        let reporting = dispatcher.submission_reporting.lock().unwrap();
        assert!(reporting.iter().all(|(has_endpoint, _)| *has_endpoint));
        assert_eq!(
            reporting
                .iter()
                .filter(|(_, typed_result_sink)| *typed_result_sink)
                .count(),
            1
        );
        assert!(!dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn successful_fragment_submission_updates_frontend_topology_telemetry() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let topology = Arc::new(FrontendTopologyController::new(1));
        for (_, endpoint) in &backends {
            novarocks::query_execution::backend::BackendTopologyPort::add_backend(
                topology.as_ref(),
                *endpoint,
            )
            .unwrap();
        }
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test_with_topology(
            QueryId::new(41, 73),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
            NonZeroUsize::new(1).unwrap(),
            Arc::new(NoopRuntimeFilterDispatcher),
            topology.clone(),
        );

        coordinator
            .execute(request)
            .expect("frontend executes fixture");

        let submissions = dispatcher.submissions.lock().unwrap().clone();
        for backend_idx in 0..topology.backend_count_for_test() {
            let expected = submissions
                .iter()
                .filter(|(submitted_backend_idx, _)| *submitted_backend_idx == backend_idx)
                .count() as u64;
            assert_eq!(
                topology.scheduled_fragment_count_for_test(backend_idx),
                expected,
                "frontend topology telemetry must count successful submissions per backend"
            );
        }
    }

    #[test]
    fn frontend_native_report_ingress_shares_the_execution_registry() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let report = fixture.successful_fragment_report_proto();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(41, 73), scheduler, dispatcher.clone());
        dispatcher.native_wire_report_on_submit(2, report, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("frontend accepts native report through the transport port");

        assert_result_outcome_preserved(outcome, 1).expect("engine consumes Result payload");
        assert_eq!(dispatcher.submissions.lock().unwrap().len(), 2);
    }

    #[test]
    fn frontend_resolves_a_fresh_backend_snapshot_for_each_query() {
        let first = non_empty_result_contract_fixture();
        let backends = first.backends().to_vec();
        let first_batch = first.result_batch();
        let first_request = first.into_request();
        let second = non_empty_result_contract_fixture();
        let second_batch = second.result_batch();
        let second_request = second.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_results(vec![
            first_batch,
            second_batch,
        ]));
        let schedulers = backends
            .iter()
            .copied()
            .map(|backend| {
                FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(vec![backend]).unwrap())
            })
            .collect();
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test_with_backend_sequence(
            QueryId::new(41, 73),
            report_endpoint(),
            schedulers,
            dispatcher.clone(),
            NonZeroUsize::new(1).unwrap(),
            Arc::new(NoopRuntimeFilterDispatcher),
        );

        assert_result_outcome_preserved(coordinator.execute(first_request).unwrap(), 1).unwrap();
        assert_result_outcome_preserved(coordinator.execute(second_request).unwrap(), 1).unwrap();

        let submitted_backends = dispatcher
            .submissions
            .lock()
            .unwrap()
            .iter()
            .map(|(backend_idx, _)| *backend_idx)
            .collect::<Vec<_>>();
        assert_eq!(submitted_backends, vec![3, 3, 8, 8]);
    }

    #[test]
    fn frontend_returns_non_empty_write_contract() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let reports = vec![
            fixture.successful_non_writer_report(),
            fixture.successful_writer_report(),
        ];
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.delayed_reports_on_submit(2, reports, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("frontend executes write fixture");

        assert_write_outcome_preserved(outcome).expect("engine consumes Write payload");
        assert!(!dispatcher.submissions.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn duplicate_identical_writer_report_is_idempotent() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let reports = vec![
            fixture.successful_non_writer_report(),
            fixture.successful_writer_report(),
            fixture.successful_writer_report(),
        ];
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.reports_on_submit(2, reports, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("identical final writer retries must remain idempotent");

        assert_write_outcome_preserved(outcome).expect("duplicate report preserves commit");
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn conflicting_final_writer_retry_returns_structured_abort() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let successful = fixture.successful_writer_report();
        let conflicting = fixture.conflicting_writer_report();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.accepted_then_rejected_reports_on_submit(
            2,
            successful,
            conflicting,
            &coordinator,
        );

        let outcome = coordinator
            .execute(request)
            .expect("conflicting writer retry must retain abort recovery data");
        assert_write_abort_reason(outcome, "conflicting final writer output")
            .expect("conflicting writer output cannot commit");
        assert!(!dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn multi_writer_conflict_latches_without_waiting_for_the_missing_writer() {
        let fixture = non_empty_write_contract_fixture();
        let successful = fixture.successful_writer_report();
        let writer = successful.fragment_instance_id();
        let conflicting = fixture.conflicting_writer_report();
        let missing_writer = UniqueId {
            hi: 51,
            lo: i64::from(24_u32) << 16,
        };
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = QueryId::new(51, 91);
        let _guard = registry
            .register(
                query_id,
                novarocks::query_execution::contract::DistributedQueryIntent::Write,
                dispatcher.clone(),
            )
            .unwrap();
        registry.set_scheduled_backends(query_id, &[3, 8]).unwrap();
        registry
            .set_writer_instances(query_id, &[(writer, 0), (missing_writer, 1)])
            .unwrap();
        registry.record_attempt(query_id, 3, writer).unwrap();
        registry.finish_attempt(query_id).unwrap();
        registry
            .record_attempt(query_id, 8, missing_writer)
            .unwrap();
        registry.finish_attempt(query_id).unwrap();

        registry.record_report(successful).unwrap();
        let error = registry
            .record_report(conflicting)
            .expect_err("conflicting retry is rejected at report ingress");

        assert!(error.message().contains("conflicting final writer output"));
        let progress = registry
            .report_progress(query_id, &[writer, missing_writer])
            .unwrap();
        assert_eq!(progress.0, 1);
        assert!(
            progress
                .1
                .as_deref()
                .is_some_and(|message| message.contains("conflicting final writer output"))
        );
        assert_eq!(
            dispatcher.cancellations.lock().unwrap().as_slice(),
            [(3, vec![writer]), (8, vec![missing_writer])],
            "conflict cancels immediately instead of waiting for the missing writer"
        );
        assert_eq!(
            registry.seal_and_take_completion(query_id).unwrap().1.len(),
            1,
            "only the canonical writer final is retained"
        );
    }

    #[test]
    fn late_writer_retry_after_completion_snapshot_is_rejected_without_mutation() {
        let fixture = non_empty_write_contract_fixture();
        let report = fixture.successful_writer_report();
        let retry = fixture.successful_writer_report();
        let writer = report.fragment_instance_id();
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = QueryId::new(51, 91);
        let _guard = registry
            .register(
                query_id,
                novarocks::query_execution::contract::DistributedQueryIntent::Write,
                dispatcher,
            )
            .unwrap();
        registry.set_scheduled_backends(query_id, &[3]).unwrap();
        registry
            .set_writer_instances(query_id, &[(writer, 0)])
            .unwrap();
        registry.record_attempt(query_id, 3, writer).unwrap();
        registry.finish_attempt(query_id).unwrap();
        registry.record_report(report).unwrap();

        let (failure, reports) = registry.seal_and_take_completion(query_id).unwrap();
        assert_eq!(failure, None);
        assert_eq!(reports.len(), 1);

        let error = registry
            .record_report(retry)
            .expect_err("late retry after the atomic completion snapshot is rejected");
        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::Rejected
        );
        assert_eq!(registry.first_failure(query_id), None);
    }

    #[test]
    fn wrong_backend_writer_identity_returns_structured_abort() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let report = fixture.wrong_backend_writer_report();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.rejected_report_on_submit(2, report, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("writer identity mismatch must retain abort recovery data");
        assert_write_abort_reason(outcome, "unknown writer report with write metadata")
            .expect("wrong writer backend cannot commit");
        assert!(!dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn failed_non_writer_report_forces_write_abort_even_after_writer_success() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let reports = vec![
            fixture.failed_non_writer_report(),
            fixture.successful_writer_report(),
        ];
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.reports_on_submit(2, reports, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("producer failure is represented by write abort");
        assert_write_abort_reason(outcome, "contract producer failure")
            .expect("producer failure must prevent commit");
        assert!(!dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn non_writer_write_metadata_is_rejected_and_forces_abort() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let report = fixture.non_writer_report_with_write_metadata();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.rejected_report_on_submit(2, report, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("unexpected writer output is represented by write abort");
        assert_write_abort_reason(outcome, "unknown writer report with write metadata")
            .expect("unexpected writer metadata must prevent commit");
    }

    #[test]
    fn nonfinal_non_writer_write_metadata_is_rejected_and_forces_abort() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let report = fixture.nonfinal_non_writer_report_with_write_metadata();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.rejected_report_on_submit(2, report, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("periodic unknown writer output is represented by write abort");
        assert_write_abort_reason(outcome, "unknown writer report with write metadata")
            .expect("periodic unexpected writer metadata must prevent commit");
    }

    #[test]
    fn backend_loss_cannot_turn_a_missing_writer_report_into_commit() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.backend_loss_on_submit(1, 3, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("backend loss must produce a recoverable write abort");
        assert_write_abort_reason(outcome, "backend 3 lost")
            .expect("backend loss without writer output cannot commit");
        let attempted = dispatcher.submissions.lock().unwrap()[0];
        assert_eq!(
            dispatcher.cancellations.lock().unwrap().as_slice(),
            [(attempted.0, vec![attempted.1])]
        );
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn missing_writer_report_times_out_to_structured_abort_and_exact_cancellation() {
        let fixture = non_empty_write_contract_fixture_with_query_timeout_seconds(0);
        let backends = fixture.backends().to_vec();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator =
            test_coordinator(QueryId::new(51, 91), scheduler, Arc::clone(&dispatcher));

        let outcome = coordinator
            .execute(request)
            .expect("write timeout must preserve the structured abort recovery payload");

        assert_write_abort_reason(outcome, "waiting for write final reports")
            .expect("missing writer output cannot commit");
        let submitted = dispatcher.submissions.lock().unwrap().clone();
        let cancelled = dispatcher.cancellations.lock().unwrap().clone();
        assert_eq!(
            cancelled.iter().map(|(_, ids)| ids.len()).sum::<usize>(),
            submitted.len(),
            "the timeout cancels every and only attempted fragment instance"
        );
    }

    #[test]
    fn write_cancellation_after_partial_submit_returns_abort_payload() {
        let fixture = non_empty_write_contract_fixture();
        let backends = fixture.backends().to_vec();
        let cancellation = fixture.cancellation_source();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_cancellation(cancellation, 1));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());

        let outcome = coordinator
            .execute(request)
            .expect("write cancellation must retain abort recovery data");
        assert_write_abort_reason(outcome, "query cancelled")
            .expect("cancelled write cannot commit");
        assert_eq!(dispatcher.submissions.lock().unwrap().len(), 1);
        assert!(!dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn frontend_returns_non_empty_profile_contract() {
        let fixture = non_empty_profile_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let reports = fixture.fragment_profile_reports();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(61, 101), scheduler, dispatcher.clone());
        dispatcher.delayed_reports_on_submit(1, reports, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("frontend executes profile fixture");

        assert_profile_outcome_preserved(outcome, 1)
            .expect("engine consumes non-empty Profile payload");
        assert!(!dispatcher.submissions.lock().unwrap().is_empty());
        assert!(!dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn missing_profile_report_times_out_and_cancels_attempted_instances() {
        let fixture = non_empty_profile_contract_fixture_with_query_timeout_seconds(1);
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator =
            test_coordinator(QueryId::new(61, 101), scheduler, Arc::clone(&dispatcher));

        let error = match coordinator.execute(request) {
            Ok(_) => panic!("profile completion must not succeed without profile reports"),
            Err(error) => error,
        };

        assert!(
            error
                .message()
                .contains("waiting for fragment profile reports"),
            "{error}"
        );
        let submitted = dispatcher.submissions.lock().unwrap().clone();
        let cancelled = dispatcher.cancellations.lock().unwrap().clone();
        assert_eq!(
            cancelled.iter().map(|(_, ids)| ids.len()).sum::<usize>(),
            submitted.len(),
            "the profile timeout cancels every and only attempted fragment instance"
        );
    }

    #[test]
    fn profile_progress_requires_profile_payloads_not_only_final_status() {
        let fixture = non_empty_profile_contract_fixture();
        let reports = fixture.fragment_final_reports_without_profiles();
        let expected_instances = reports
            .iter()
            .map(NativeExecutionReport::fragment_instance_id)
            .collect::<Vec<_>>();
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = QueryId::new(61, 101);
        let _guard = registry
            .register(
                query_id,
                novarocks::query_execution::contract::DistributedQueryIntent::Profile,
                dispatcher,
            )
            .unwrap();
        registry.set_scheduled_backends(query_id, &[3]).unwrap();
        for report in reports {
            registry
                .record_attempt(query_id, 3, report.fragment_instance_id())
                .unwrap();
            registry.finish_attempt(query_id).unwrap();
            registry.record_report(report).unwrap();
        }

        assert_eq!(
            registry
                .report_progress(query_id, &expected_instances)
                .unwrap(),
            (0, None, false)
        );
    }

    #[test]
    fn unattempted_profile_report_is_rejected_without_aggregation_or_cancellation() {
        let fixture = non_empty_profile_contract_fixture();
        let mut reports = fixture.fragment_profile_reports().into_iter();
        let attempted_instance = reports.next().unwrap().fragment_instance_id();
        let unattempted_report = reports.next().unwrap();
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = QueryId::new(61, 101);
        let _guard = registry
            .register(
                query_id,
                novarocks::query_execution::contract::DistributedQueryIntent::Profile,
                dispatcher.clone(),
            )
            .unwrap();
        registry.set_scheduled_backends(query_id, &[3]).unwrap();
        registry
            .record_attempt(query_id, 3, attempted_instance)
            .unwrap();
        registry.finish_attempt(query_id).unwrap();

        let error = registry
            .record_report(unattempted_report)
            .expect_err("an unattempted profile instance must be rejected at ingress");

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::ContractViolation
        );
        assert!(error.message().contains("unattempted fragment instance"));
        assert_eq!(registry.first_failure(query_id), None);
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(
            registry
                .seal_and_take_completion(query_id)
                .unwrap()
                .1
                .is_empty()
        );
    }

    #[test]
    fn unattempted_failed_report_is_rejected_without_poisoning_the_real_query() {
        let fixture = non_empty_result_contract_fixture();
        let unattempted_report = fixture.failed_fragment_report();
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = QueryId::new(41, 73);
        let attempted_instance = UniqueId { hi: 999, lo: 1_999 };
        let _guard = registry
            .register(
                query_id,
                novarocks::query_execution::contract::DistributedQueryIntent::Result,
                dispatcher.clone(),
            )
            .unwrap();
        registry.set_scheduled_backends(query_id, &[3]).unwrap();
        registry
            .record_attempt(query_id, 3, attempted_instance)
            .unwrap();
        registry.finish_attempt(query_id).unwrap();

        let error = registry
            .record_report(unattempted_report)
            .expect_err("an unattempted failure must be rejected at ingress");

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::ContractViolation
        );
        assert!(error.message().contains("unattempted fragment instance"));
        assert_eq!(registry.first_failure(query_id), None);
        assert!(dispatcher.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn frontend_contract_cancels_only_through_dispatcher() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let cancellation = fixture.cancellation_source();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result_and_cancellation(
            batch,
            cancellation,
            2,
        ));
        let local_cleanup_calls = AtomicUsize::new(0);
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(41, 73), scheduler, dispatcher.clone());

        let error = match coordinator.execute(request) {
            Ok(_) => panic!("cancellation must stop execution"),
            Err(error) => error,
        };

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::Failed
        );
        assert_eq!(dispatcher.submissions.lock().unwrap().len(), 2);
        assert!(!dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
        assert_eq!(local_cleanup_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn failed_report_cancels_via_dispatcher_without_local_cleanup() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let report = fixture.failed_fragment_report();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let local_cleanup_calls = AtomicUsize::new(0);
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(41, 73), scheduler, dispatcher.clone());
        dispatcher.report_on_submit(2, report, &coordinator);

        let error = match coordinator.execute(request) {
            Ok(_) => panic!("failed native report must fail execution"),
            Err(error) => error,
        };

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::Failed
        );
        assert!(!dispatcher.submissions.lock().unwrap().is_empty());
        assert!(!dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
        assert_eq!(local_cleanup_calls.load(Ordering::SeqCst), 0);

        let write_fixture = non_empty_write_contract_fixture();
        let backends = write_fixture.backends().to_vec();
        let report = write_fixture.failed_writer_report();
        let request = write_fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = test_coordinator(QueryId::new(51, 91), scheduler, dispatcher.clone());
        dispatcher.report_on_submit(2, report, &coordinator);

        let outcome = coordinator
            .execute(request)
            .expect("failed writer report returns abort payload");

        assert_write_outcome_preserved(outcome).expect("engine preserves Write abort payload");
        assert!(!dispatcher.cancellations.lock().unwrap().is_empty());
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
    }

    #[test]
    fn real_frontend_report_handler_cancels_exact_attempted_instances() {
        let fixture = non_empty_result_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let report = fixture.failed_fragment_report();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test(
            QueryId::new(41, 73),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
            NonZeroUsize::new(1).unwrap(),
            Arc::new(NoopRuntimeFilterDispatcher),
        );
        dispatcher.report_on_submit(2, report, &coordinator);

        let error = match coordinator.execute(request) {
            Ok(_) => panic!("failed native report must fail the active query"),
            Err(error) => error,
        };

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::Failed
        );
        let attempted = dispatcher.submissions.lock().unwrap().clone();
        let cancelled = dispatcher
            .cancellations
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(backend_idx, finst_ids)| {
                finst_ids
                    .iter()
                    .copied()
                    .map(|finst_id| (*backend_idx, finst_id))
            })
            .collect::<Vec<_>>();
        assert_eq!(cancelled, attempted);
        assert!(dispatcher.fetches.lock().unwrap().is_empty());
        assert_eq!(coordinator.runtime_filter_barrier_calls(), 1);
    }

    #[test]
    fn runtime_filter_install_barrier_precedes_every_fragment_submission() {
        let fixture = non_empty_runtime_filter_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let events = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = Arc::new(RecordingDispatcher::with_result_and_events(
            batch,
            Arc::clone(&events),
        ));
        let runtime_filters = Arc::new(RecordingRuntimeFilterDispatcher::new(
            None,
            Arc::clone(&events),
        ));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test(
            QueryId::new(71, 111),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
            NonZeroUsize::new(2).unwrap(),
            runtime_filters.clone(),
        );

        let outcome = coordinator
            .execute(request)
            .expect("runtime-filter fixture executes");

        assert_result_outcome_preserved(outcome, 1).expect("result payload");
        assert!(runtime_filters.install_calls.load(Ordering::SeqCst) > 0);
        let events = events.lock().unwrap();
        let first_submit = events
            .iter()
            .position(|event| *event == "submit")
            .expect("fragment submission event");
        assert!(
            events[..first_submit]
                .iter()
                .all(|event| *event == "install"),
            "all events before first submit must be runtime-filter installs: {events:?}"
        );
        assert!(
            events[first_submit..]
                .iter()
                .all(|event| *event == "submit"),
            "runtime-filter installs must finish before submission: {events:?}"
        );
    }

    #[test]
    fn runtime_filter_install_fanout_attempts_all_participants_concurrently() {
        let fixture = non_empty_runtime_filter_contract_fixture();
        let backends = fixture.backends().to_vec();
        let batch = fixture.result_batch();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::with_result(batch));
        let runtime_filters = Arc::new(ConcurrentInstallGate::new(2));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test(
            QueryId::new(71, 115),
            report_endpoint(),
            scheduler,
            dispatcher,
            NonZeroUsize::new(2).unwrap(),
            runtime_filters.clone(),
        );

        let outcome = coordinator
            .execute(request)
            .expect("all RF participants enter the install barrier concurrently");

        assert_result_outcome_preserved(outcome, 1).expect("result payload");
        assert_eq!(*runtime_filters.entered.lock().unwrap(), 2);
    }

    #[test]
    fn runtime_filter_rollback_fanout_aborts_all_participants_concurrently() {
        let fixture = non_empty_runtime_filter_contract_fixture();
        let backends = fixture.backends().to_vec();
        let request = fixture.into_request();
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let runtime_filters = Arc::new(ConcurrentAbortGate::new(4, 2));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test(
            QueryId::new(71, 116),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
            NonZeroUsize::new(2).unwrap(),
            runtime_filters.clone(),
        );

        let error = coordinator
            .execute(request)
            .err()
            .expect("the injected RF install failure must reject execution");

        assert!(
            error
                .message()
                .contains("injected concurrent install failure"),
            "{error}"
        );
        assert_eq!(*runtime_filters.entered.lock().unwrap(), 2);
        assert!(
            !runtime_filters.timed_out.load(Ordering::SeqCst),
            "rollback participants must enter concurrently"
        );
        assert!(dispatcher.submissions.lock().unwrap().is_empty());
    }

    #[test]
    fn runtime_filter_install_failure_rolls_back_before_any_fragment_submission() {
        let fixture = non_empty_runtime_filter_contract_fixture();
        let backends = fixture.backends().to_vec();
        let request = fixture.into_request();
        let events = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let runtime_filters = Arc::new(RecordingRuntimeFilterDispatcher::new(
            Some(1),
            Arc::clone(&events),
        ));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test(
            QueryId::new(71, 112),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
            NonZeroUsize::new(2).unwrap(),
            runtime_filters.clone(),
        );

        let error = match coordinator.execute(request) {
            Ok(_) => panic!("runtime-filter install failure must reject execution"),
            Err(error) => error,
        };

        assert_eq!(
            error.kind(),
            novarocks::query_execution::contract::DistributedQueryErrorKind::Failed
        );
        assert!(error.message().contains("injected install failure 1"));
        assert!(dispatcher.submissions.lock().unwrap().is_empty());
        assert_eq!(
            runtime_filters.install_calls.load(Ordering::SeqCst),
            2,
            "the barrier attempts every participant even when one install fails"
        );
        assert_eq!(
            runtime_filters.abort_calls.load(Ordering::SeqCst),
            2,
            "every attempted participant is rolled back"
        );
        assert_eq!(
            *events.lock().unwrap(),
            vec!["install", "install", "abort", "abort"]
        );
    }

    #[test]
    fn unknown_submit_outcome_is_cancelled_before_runtime_filter_rollback() {
        let fixture = non_empty_runtime_filter_contract_fixture();
        let backends = fixture.backends().to_vec();
        let request = fixture.into_request();
        let events = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = Arc::new(RecordingDispatcher::with_submit_failure(
            1,
            Arc::clone(&events),
        ));
        let runtime_filters = Arc::new(RecordingRuntimeFilterDispatcher::new(
            None,
            Arc::clone(&events),
        ));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test(
            QueryId::new(71, 113),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
            NonZeroUsize::new(2).unwrap(),
            runtime_filters,
        );

        let error = coordinator
            .execute(request)
            .err()
            .expect("an unknown submit outcome must fail the query");

        assert!(
            error
                .message()
                .contains("injected submit failure with unknown remote outcome"),
            "{error}"
        );
        let attempted = dispatcher.submissions.lock().unwrap().clone();
        let cancelled = dispatcher
            .cancellations
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(backend_idx, finst_ids)| {
                finst_ids
                    .iter()
                    .copied()
                    .map(|finst_id| (*backend_idx, finst_id))
            })
            .collect::<Vec<_>>();
        assert_eq!(cancelled, attempted);

        let events = events.lock().unwrap();
        let cancel = events.iter().position(|event| *event == "cancel").unwrap();
        let abort = events.iter().position(|event| *event == "abort").unwrap();
        assert!(
            cancel < abort,
            "unknown-outcome fragment cancellation must precede RF rollback: {events:?}"
        );
    }

    #[test]
    fn partial_runtime_filter_install_aborts_every_attempt_and_preserves_primary_error() {
        let fixture = non_empty_runtime_filter_contract_fixture();
        let backends = fixture.backends().to_vec();
        let request = fixture.into_request();
        let events = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let runtime_filters = Arc::new(RecordingRuntimeFilterDispatcher::with_abort_failures(
            2,
            Arc::clone(&events),
        ));
        let scheduler =
            FrontendFragmentScheduler::new(FrontendBackendSnapshot::new(backends).unwrap());
        let coordinator = FrontendDistributedQueryCoordinator::new_for_test(
            QueryId::new(71, 114),
            report_endpoint(),
            scheduler,
            dispatcher.clone(),
            NonZeroUsize::new(2).unwrap(),
            runtime_filters.clone(),
        );

        let error = coordinator
            .execute(request)
            .err()
            .expect("partial RF install failure must reject execution");

        assert!(
            error
                .message()
                .starts_with("runtime-filter install failed for participant"),
            "the install error must remain primary: {error}"
        );
        assert!(
            error.message().contains("injected install failure 2"),
            "{error}"
        );
        assert!(
            error.message().contains("runtime-filter rollback failed"),
            "{error}"
        );
        assert_eq!(runtime_filters.install_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            runtime_filters.abort_calls.load(Ordering::SeqCst),
            2,
            "every attempted participant must be aborted despite abort failures"
        );
        assert!(dispatcher.submissions.lock().unwrap().is_empty());
    }
}
