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

//! Profile handoff and original diagnostic observation boundaries.

use crate::native::task_result_transport::{FinalTaskInfoRead, TaskResultTransport};
use crate::query_execution::internal_consumer::profile::{
    ReadyProfileComputation, profile_computation_consumer,
};
use crate::task_execution::profile_diagnostics::{
    ProfileDiagnosticClosure, ProfileDiagnosticMissing, ProfileTaskDiagnostic,
};
use novarocks_execution_contract::{OperationOutcome, TaskIdentity};
use novarocks_query_application::api::{InternalComputationConsumer, QueryExecutionError};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

async fn watchdog<T>(stage: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("Profile component watchdog: {stage}"))
}

#[derive(Default)]
struct FinalInfoProbe {
    requested: Mutex<Option<TaskIdentity>>,
    entered: AtomicBool,
    completed: AtomicBool,
    exited: AtomicBool,
}

struct FinalInfoExit(Arc<FinalInfoProbe>);
impl Drop for FinalInfoExit {
    fn drop(&mut self) {
        self.0.exited.store(true, Ordering::SeqCst);
    }
}

struct HeldOriginalFinalInfo {
    gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    probe: Arc<FinalInfoProbe>,
}
impl TaskResultTransport for HeldOriginalFinalInfo {
    fn final_task_info(
        &self,
        requested: TaskIdentity,
    ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
        assert!(
            self.probe
                .requested
                .lock()
                .unwrap()
                .replace(requested)
                .is_none(),
            "the same original diagnostic future is created once"
        );
        let gate = self.gate.lock().unwrap().take().expect("one original read");
        let probe = Arc::clone(&self.probe);
        Box::pin(async move {
            let _exit = FinalInfoExit(Arc::clone(&probe));
            probe.entered.store(true, Ordering::SeqCst);
            gate.await.expect("release the original final-info read");
            probe.completed.store(true, Ordering::SeqCst);
            Ok(FinalTaskInfoRead::Unavailable(OperationOutcome::Gone))
        })
    }
}

struct OriginalProfileProgressTrace<'a> {
    stage: &'static str,
    binding: &'a crate::query_execution::logical_binding::LogicalAttemptBindingCell,
    installed: &'a crate::task_execution::host::install::InstalledAttemptHost,
    peer: &'a crate::native::task_transport::tests::Loopback,
}
impl OriginalProfileProgressTrace<'_> {
    fn report(&self) {
        let bound = self.binding.bound().unwrap();
        eprintln!(
            "original independent Profile wait expired: stage={}, lp={:?}, ap={:?}, subscription={:?}, actual_ap_exit={}, peer_root_progress={:?}, peer_release={}, original_failure={}",
            self.stage,
            bound.runtime.snapshot(bound.registration.actor()),
            bound.runtime.snapshot(self.installed.fixture_sources.0),
            bound.runtime.snapshot(self.installed.fixture_sources.1),
            *self.installed.fixture_exit.observe_actual_exit().borrow(),
            self.peer.single_root_result_progress(),
            self.peer.single_root_release_observed(),
            self.binding.original_failure_for_fixture(),
        );
        for actor in [
            bound.registration.actor(),
            self.installed.fixture_sources.0,
            self.installed.fixture_sources.1,
        ] {
            eprintln!(
                "original independent Profile actor inputs: actor={actor:?}, actual={:?}",
                bound.runtime.input_inventory(actor, None, 8)
            );
            if let Ok(inputs) = bound.runtime.input_inventory(actor, None, 8) {
                let standing = bound.runtime.standing_input_inventory(actor, None, 32);
                for input in inputs {
                    eprintln!(
                        "original independent Profile input retirement: actor={actor:?}, input={:?}, preflight={:?}, header_authors={:?}, matching_standing={:?}",
                        input.input,
                        bound.runtime.input_retirement_status(actor, input.input),
                        bound.runtime.input_header_authors(actor, input.input, 8),
                        standing
                            .as_ref()
                            .ok()
                            .and_then(|rows| rows.iter().find(|row| {
                                Some(row.standing_input_id) == input.standing_input_id
                            })),
                    );
                }
            }
            if let Ok(batches) = bound.runtime.committed_batches(actor, None, 8) {
                for (ordinal, input) in batches {
                    eprintln!(
                        "original independent Profile batch effects: actor={actor:?}, ordinal={ordinal:?}, input={input:?}, actual={:?}",
                        bound.runtime.effect_inventory(actor, input, None, 16)
                    );
                }
            }
        }
    }
}

async fn drive_original_profile_source<F: Future>(
    fixture: &mut crate::fixtures::FrontendFixture,
    joint: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    clock: &crate::task_execution::clock::ManualClock,
    advance_indexed_clock: bool,
    future: F,
) -> F::Output {
    drive_original_profile_source_with_trace(
        fixture,
        joint,
        clock,
        advance_indexed_clock,
        future,
        None,
    )
    .await
}

async fn drive_original_profile_source_with_trace<F: Future>(
    fixture: &mut crate::fixtures::FrontendFixture,
    joint: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    clock: &crate::task_execution::clock::ManualClock,
    advance_indexed_clock: bool,
    future: F,
    trace: Option<OriginalProfileProgressTrace<'_>>,
) -> F::Output {
    use crate::fixtures::{ProgressBudget, ProgressSelection};
    use novarocks_coordination_contract::MonotonicClock;
    let mut future = Box::pin(future);
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            crate::query_execution::native_execution_adapter::source_cohort::tests::drive(joint);
            if let Poll::Ready(result) = futures::poll!(future.as_mut()) {
                return result;
            }
            let actual = fixture
                .progress(ProgressSelection::All, ProgressBudget::default())
                .await;
            if advance_indexed_clock
                && actual.actors == 0
                && actual.hosts == 0
                && let Some(next) = fixture.runtime().next_deadline()
                && std::time::Duration::from_nanos(next.0) > clock.now().since_origin()
            {
                clock.advance(Duration::from_nanos(
                    next.0 - u64::try_from(clock.now().since_origin().as_nanos()).unwrap(),
                ));
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    if result.is_err() {
        eprintln!(
            "original Profile progress wait expired: future={}, clock={:?}, indexed_deadline={:?}, advance_indexed_clock={}, unresolved_work={:?}, ledger_empty={}",
            std::any::type_name::<F>(),
            clock.now(),
            fixture.runtime().next_deadline(),
            advance_indexed_clock,
            fixture.ledger().unresolved_work(),
            fixture.ledger().is_empty(),
        );
        if let Some(trace) = trace {
            trace.report();
        }
    }
    result.unwrap_or_else(|_| {
        panic!("Profile component watchdog: same installed Profile Source progress")
    })
}

/// Same actual recipe/consumer/Root/Source for both original named tests.
/// The held transport is a component I/O stub, never a real BE/RPC claim.
async fn original_profile_source_at_its_frozen_deadline(ready_on_deadline: bool) {
    use crate::fixtures::native_recipe::{
        OriginalRecipeContext, OriginalRecipeDiagnostics, OriginalRecipeOperands,
        PreparedOriginalRecipeProcess, RecipeRoot, count_description,
        install_original_recipe_with_diagnostics, prepare_original_recipe_process_with_budgets,
    };
    use crate::native::{data_runtime::FrontendDataRuntime, task_transport::tests::Loopback};
    use novarocks_coordination_contract::MonotonicClock;
    use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
    use novarocks_workload_control::{
        ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
    };
    let probe = Arc::new(FinalInfoProbe::default());
    let (release, gate) = tokio::sync::oneshot::channel();
    let transport: Arc<dyn TaskResultTransport> = Arc::new(HeldOriginalFinalInfo {
        gate: Mutex::new(Some(gate)),
        probe: probe.clone(),
    });
    let mut data = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let mut peer = Loopback::start().await;
    let descriptor = peer.bounded_root_descriptor(novarocks_types::BackendProcessId::new_v7());
    let history = novarocks_control_runtime::history::HistoryCapture::new(32_768);
    // These are the original first-two fixture budgets, not defaults and not
    // the independent pumps diagnostic case's 10ms/60s policy.
    let diagnostic_budget = Duration::from_secs(1);
    let remote_budget = Duration::from_secs(5);
    let PreparedOriginalRecipeProcess {
        mut fixture,
        process,
        clock,
        mut joint,
        workload,
        mut supervisor,
        client,
        capacity,
        handle,
        descriptor,
    } = prepare_original_recipe_process_with_budgets(
        &data,
        descriptor,
        history,
        ResultCapacityConfig::V1,
        None,
        diagnostic_budget,
        remote_budget,
    );
    let root_work = workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root_work.owner.scope();
    let business = root_work.business;
    let window = capacity
        .try_acquire(&scope, ResultWindowClass::Internal)
        .unwrap();
    let (description, plan) = count_description();
    let (consumer, offers, output) = profile_computation_consumer(&description, plan).unwrap();
    let (mut logical, cohort, mut output) = install_original_recipe_with_diagnostics(
        OriginalRecipeContext {
            fixture: &mut fixture,
            joint: &mut joint,
            process: &process,
            data: &data,
            clock: &clock,
            handle,
            descriptor,
            client,
        },
        OriginalRecipeOperands {
            owner: root_work.owner,
            binding: QueryResultCapacityBinding::try_new(&scope, window.retain_alias()).unwrap(),
            description,
            plan,
            root: RecipeRoot::EmptyCountOnly,
            consumer,
            offers,
            output,
        },
        Some(OriginalRecipeDiagnostics { transport }),
    );
    logical
        .probe
        .graph_capture_requested
        .store(true, Ordering::Release);
    logical
        .probe
        .credentials_requested
        .store(true, Ordering::Release);
    let installed = fixture.activate_attempt(&logical, &mut joint, false).await;
    let graph = logical.probe.graph_capture.lock().unwrap().take().unwrap();
    let observer = logical
        .probe
        .profile_observer
        .lock()
        .unwrap()
        .take()
        .unwrap();
    let root = graph.root;
    let expected = graph
        .tasks
        .iter()
        .map(|task| task.identity())
        .collect::<Vec<_>>();
    assert!(!expected.is_empty());
    peer.enable_single_root_lifecycle(root, novarocks_result_contract::RootOutputKind::CountOnly);
    logical.probe.release_activation();
    let mut original = drive_original_profile_source(
        &mut fixture,
        &mut joint,
        clock.as_ref(),
        false,
        logical.ready.take().unwrap(),
    )
    .await
    .unwrap();
    let token = *original
        .ready_token()
        .expect("same actual Relay/consumer cut");
    let mut received = output
        .take_ready(token)
        .expect("receiving custody is staged before Ready returns");
    assert!(
        output.take_ready(token).is_err(),
        "one original receiving transfer only"
    );
    assert_eq!(received.ready_token(), token);
    assert_eq!(received.output_rows(), 0);
    assert_eq!(received.expected_tasks(), expected.as_slice());
    assert_eq!(
        token.activation(),
        logical.probe.activation.lock().unwrap().unwrap()
    );
    assert_eq!(token.root(), root);
    assert_eq!(
        token.diagnostic_deadline(),
        token.cut().saturating_add(diagnostic_budget)
    );
    assert_eq!(
        token.convergence_deadline(),
        token.cut().saturating_add(remote_budget)
    );
    let credentials = logical
        .probe
        .credentials
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap();
    assert!(Arc::ptr_eq(
        received.original_credentials().unwrap(),
        &credentials
    ));
    // Final AP starts diagnostic work only AFTER original Ready. Holding this
    // same real Source job cannot revoke Ready or consume the receiving offer.
    drive_original_profile_source(&mut fixture, &mut joint, clock.as_ref(), false, async {
        while !probe.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(*probe.requested.lock().unwrap(), Some(root));
    assert!(!probe.completed.load(Ordering::SeqCst));
    assert!(!probe.exited.load(Ordering::SeqCst));
    assert!(received.current_diagnostics().unwrap().is_none());
    assert!(
        !*installed.fixture_exit.observe_actual_exit().borrow(),
        "held original diagnostic work prevents an actual AP Scope exit"
    );
    let mut release = Some(release);
    if ready_on_deadline {
        release.take().unwrap().send(()).unwrap();
        // Current-thread execution: sending only wakes the SAME original job;
        // no await/poll occurs before advancing its original FE clock.
        assert!(!probe.completed.load(Ordering::SeqCst));
        assert!(!probe.exited.load(Ordering::SeqCst));
    }
    clock.advance(
        token
            .diagnostic_deadline()
            .saturating_duration_since(clock.now()),
    );
    assert_eq!(clock.now(), token.diagnostic_deadline());
    let first =
        drive_original_profile_source(&mut fixture, &mut joint, clock.as_ref(), false, async {
            loop {
                if let Some(snapshot) = received.current_diagnostics().unwrap() {
                    return snapshot;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
    assert_eq!(first.ready, token);
    assert_eq!(first.selected_at, token.diagnostic_deadline());
    assert_eq!(first.closure, ProfileDiagnosticClosure::ObservationDeadline);
    assert_eq!(first.tasks.len(), expected.len());
    assert!(first.tasks.iter().any(|(task, fact)| *task == root
        && matches!(
            fact,
            ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)
        )));
    assert!(!first.tasks.iter().any(|(task, fact)| *task == root
        && matches!(
            fact,
            ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::Unavailable(
                OperationOutcome::Gone
            ))
        )));
    let observed = received.wait_diagnostics().await.unwrap();
    assert!(Arc::ptr_eq(&first, &observed));
    if let Some(release) = release {
        assert!(!probe.completed.load(Ordering::SeqCst));
        assert!(!probe.exited.load(Ordering::SeqCst));
        assert!(!*installed.fixture_exit.observe_actual_exit().borrow());
        release.send(()).unwrap();
    }
    drive_original_profile_source(&mut fixture, &mut joint, clock.as_ref(), false, async {
        while !probe.completed.load(Ordering::SeqCst) || !probe.exited.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    // The raw future returned/dropped; later result, report and Join are still
    // independently retired by the installed Source, never by this probe.
    let after_read = received.current_diagnostics().unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &after_read));
    let mut exit = original
        .take_exit_observer()
        .or_else(|| logical.exit.take())
        .unwrap();
    drop((
        original,
        received,
        observed,
        after_read,
        output,
        credentials,
        graph,
    ));
    let receipt = drive_original_profile_source(
        &mut fixture,
        &mut joint,
        clock.as_ref(),
        true,
        std::future::poll_fn(|cx| exit.poll_exit(cx)),
    )
    .await
    .unwrap();
    assert!(*installed.fixture_exit.observe_actual_exit().borrow());
    // The Source's real result/Join/body/captures have now retired, not only
    // the test future's inner Drop probe. Its late Gone reply cannot replace
    // the immutable snapshot even after the publisher actually exits.
    let after_actual_exit = observer.current(token).unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &after_actual_exit));
    assert_eq!(after_actual_exit.selected_at, token.diagnostic_deadline());
    assert!(
        !after_actual_exit
            .tasks
            .iter()
            .any(|(task, fact)| *task == root
                && matches!(
                    fact,
                    ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::Unavailable(
                        OperationOutcome::Gone
                    ))
                ))
    );
    drop((observer, first, after_actual_exit));
    assert_eq!(peer.single_root_result_progress(), (true, 1));
    drop((receipt, exit, installed, logical, cohort, window, capacity));
    drive_original_profile_source(
        &mut fixture,
        &mut joint,
        clock.as_ref(),
        true,
        supervisor.shutdown_until(std::time::Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
    drop(supervisor);
    process.close_original_host_registration_for_fixture();
    fixture.finish().await;
    let history = crate::fixtures::history::CapturedHistory::drain(&fixture.history(), usize::MAX);
    history
        .check_completed_runtime(fixture.runtime(), fixture.ledger())
        .unwrap();
    drop(fixture);
    business.release();
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(&mut joint);
    tokio::time::timeout(Duration::from_secs(5), scope.wait_released())
        .await
        .unwrap();
    drop(scope);
    workload.close_admission();
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(&mut joint);
    assert!(workload.shutdown().is_ok());
    data.drain_original_apply_until(std::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    data.drain_native_outgoing_until(std::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(peer.settle().await);
    assert!(process.original_registries_empty_for_fixture());
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_profile_reaches_real_ready_while_original_final_info_is_held() {
    original_profile_source_at_its_frozen_deadline(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_profile_deadline_freezes_pending_before_the_original_rpc_returns_on_that_poll() {
    original_profile_source_at_its_frozen_deadline(true).await;
}

/// Explicit caller custody of one existing actual process/recipe. This groups
/// originals only; every installation/progress/exit uses the shared fixture.
struct IndependentProfileSource {
    data: crate::native::data_runtime::FrontendDataRuntime,
    peer: crate::native::task_transport::tests::Loopback,
    fixture: crate::fixtures::FrontendFixture,
    process: Arc<crate::control_runtime::FrontendAttemptRuntime>,
    clock: Arc<crate::task_execution::clock::ManualClock>,
    joint: novarocks_workload_control::joint_admission::JointProgressDriver,
    workload: novarocks_workload_control::WorkloadControl,
    supervisor: novarocks_query_application::coordination::LogicalExecutionSupervisor,
    capacity: novarocks_workload_control::ResultCapacityHandle,
    logical: crate::fixtures::native_recipe::ActualLogicalCase,
    installed: Arc<crate::task_execution::host::install::InstalledAttemptHost>,
    cohort: crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort,
    output: crate::query_execution::internal_consumer::profile::ProfileComputationOutput,
    consumer: Arc<dyn InternalComputationConsumer>,
    scope: novarocks_workload_control::WorkScope,
    business: novarocks_workload_control::BusinessPermit,
    window: novarocks_workload_control::ResultWindowGrant,
    root: TaskIdentity,
    expected: Vec<TaskIdentity>,
}
impl IndependentProfileSource {
    async fn new(
        namespace: novarocks_types::identity::QueryProcessNamespace,
        consumer_for_actual_source: Option<Arc<dyn InternalComputationConsumer>>,
    ) -> Self {
        use crate::fixtures::native_recipe::{
            OriginalRecipeContext, OriginalRecipeOperands, PreparedOriginalRecipeProcess,
            RecipeRoot, count_description, install_original_recipe,
            prepare_original_recipe_process_with_namespace,
        };
        use crate::native::{data_runtime::FrontendDataRuntime, task_transport::tests::Loopback};
        use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
        use novarocks_workload_control::{
            ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
        };
        let data = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let peer = Loopback::start().await;
        let descriptor = peer.bounded_root_descriptor(novarocks_types::BackendProcessId::new_v7());
        let history = novarocks_control_runtime::history::HistoryCapture::new(32_768);
        let PreparedOriginalRecipeProcess {
            mut fixture,
            process,
            clock,
            mut joint,
            workload,
            supervisor,
            client,
            capacity,
            handle,
            descriptor,
        } = prepare_original_recipe_process_with_namespace(
            &data,
            descriptor,
            history,
            ResultCapacityConfig::V1,
            None,
            namespace,
            (Duration::from_secs(1), Duration::from_secs(5)),
        );
        let original = workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = original.owner.scope();
        let window = capacity
            .try_acquire(&scope, ResultWindowClass::Internal)
            .unwrap();
        let (description, plan) = count_description();
        let (consumer, offers, output) = profile_computation_consumer(&description, plan).unwrap();
        let (logical, cohort, output) = install_original_recipe(
            OriginalRecipeContext {
                fixture: &mut fixture,
                joint: &mut joint,
                process: &process,
                data: &data,
                clock: &clock,
                handle,
                descriptor,
                client,
            },
            OriginalRecipeOperands {
                owner: original.owner,
                binding: QueryResultCapacityBinding::try_new(&scope, window.retain_alias())
                    .unwrap(),
                description,
                plan,
                root: RecipeRoot::EmptyCountOnly,
                consumer: consumer_for_actual_source.unwrap_or_else(|| consumer.clone()),
                offers,
                output,
            },
        );
        logical
            .probe
            .graph_capture_requested
            .store(true, Ordering::Release);
        logical
            .probe
            .credentials_requested
            .store(true, Ordering::Release);
        let installed = fixture.activate_attempt(&logical, &mut joint, false).await;
        let graph = logical.probe.graph_capture.lock().unwrap().take().unwrap();
        let root = graph.root;
        let expected = graph.tasks.iter().map(|task| task.identity()).collect();
        peer.enable_single_root_lifecycle(
            root,
            novarocks_result_contract::RootOutputKind::CountOnly,
        );
        Self {
            data,
            peer,
            fixture,
            process,
            clock,
            joint,
            workload,
            supervisor,
            capacity,
            logical,
            installed,
            cohort,
            output,
            consumer,
            scope,
            business: original.business,
            window,
            root,
            expected,
        }
    }
    async fn original_ready(
        &mut self,
    ) -> Result<novarocks_query_application::api::ExecutionOutputObligation, QueryExecutionError>
    {
        let ready = self.logical.ready.take().unwrap();
        drive_original_profile_source_with_trace(
            &mut self.fixture,
            &mut self.joint,
            self.clock.as_ref(),
            false,
            ready,
            Some(OriginalProfileProgressTrace {
                stage: "independent original Ready",
                binding: &self.logical.binding,
                installed: &self.installed,
                peer: &self.peer,
            }),
        )
        .await
    }
    async fn finish(
        mut self,
        mut output: Option<novarocks_query_application::api::ExecutionOutputObligation>,
        received: Option<ReadyProfileComputation>,
    ) {
        let successful = output.as_ref().is_some_and(|o| o.ready_token().is_some());
        let mut exit = output
            .as_mut()
            .and_then(|o| o.take_exit_observer())
            .or_else(|| self.logical.exit.take())
            .expect("same independent original exit observer");
        drop((output, received, self.output, self.consumer));
        let receipt = drive_original_profile_source_with_trace(
            &mut self.fixture,
            &mut self.joint,
            self.clock.as_ref(),
            true,
            std::future::poll_fn(|cx| exit.poll_exit(cx)),
            Some(OriginalProfileProgressTrace {
                stage: "independent original actual exit",
                binding: &self.logical.binding,
                installed: &self.installed,
                peer: &self.peer,
            }),
        )
        .await
        .unwrap();
        assert!(*self.installed.fixture_exit.observe_actual_exit().borrow());
        if successful {
            assert_eq!(self.peer.single_root_result_progress(), (true, 1));
        }
        drop((
            receipt,
            exit,
            self.installed,
            self.logical,
            self.cohort,
            self.window,
            self.capacity,
        ));
        drive_original_profile_source(
            &mut self.fixture,
            &mut self.joint,
            self.clock.as_ref(),
            true,
            self.supervisor
                .shutdown_until(std::time::Instant::now() + Duration::from_secs(5)),
        )
        .await
        .unwrap();
        drop(self.supervisor);
        self.process.close_original_host_registration_for_fixture();
        self.fixture.finish().await;
        let history =
            crate::fixtures::history::CapturedHistory::drain(&self.fixture.history(), usize::MAX);
        history
            .check_completed_runtime(self.fixture.runtime(), self.fixture.ledger())
            .unwrap();
        drop(self.fixture);
        self.business.release();
        crate::query_execution::native_execution_adapter::source_cohort::tests::drive(
            &mut self.joint,
        );
        watchdog(
            "same independent WC Scope really released",
            self.scope.wait_released(),
        )
        .await;
        drop(self.scope);
        self.workload.close_admission();
        crate::query_execution::native_execution_adapter::source_cohort::tests::drive(
            &mut self.joint,
        );
        assert!(self.workload.shutdown().is_ok());
        self.data
            .drain_original_apply_until(std::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        self.data
            .drain_native_outgoing_until(std::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert!(self.peer.settle().await);
        assert!(self.process.original_registries_empty_for_fixture());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_foreign_old_claim_and_cut_cannot_consume_new_profile_custody() {
    use novarocks_types::identity::QueryProcessNamespace;
    // Independent actual sources/issuers, as in the original three-actor
    // case. This is capability isolation and explicitly not recovery.
    let mut prior = IndependentProfileSource::new(QueryProcessNamespace::new(7101), None).await;
    prior.logical.probe.release_activation();
    let prior_original = prior.original_ready().await.unwrap();
    let prior_cut = *prior_original.ready_token().unwrap();
    let prior_received = prior.output.take_ready(prior_cut).unwrap();
    prior
        .finish(Some(prior_original), Some(prior_received))
        .await;

    let mut next = IndependentProfileSource::new(QueryProcessNamespace::new(7102), None).await;
    // Its original offer is already staged/bound, while the existing Native
    // release_activation gate prevents this actual source's consumer starting.
    let mut stale = IndependentProfileSource::new(
        QueryProcessNamespace::new(7103),
        Some(next.consumer.clone()),
    )
    .await;
    assert_ne!(
        stale.root.query_execution_id(),
        next.root.query_execution_id()
    );
    assert_ne!(
        prior_cut.activation(),
        *next
            .logical
            .probe
            .activation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
    );
    stale.logical.probe.release_activation();
    let rejected = stale.original_ready().await;
    assert!(
        rejected.is_err(),
        "the actual foreign source cannot claim the next original offer"
    );
    drop(rejected);
    stale.finish(None, None).await;

    next.logical.probe.release_activation();
    let next_original = next.original_ready().await.unwrap();
    let token = *next_original.ready_token().unwrap();
    assert!(
        next.output.take_ready(prior_cut).is_err(),
        "an old genuine cut cannot take new staged custody"
    );
    let received = next.output.take_ready(token).unwrap();
    assert!(next.output.take_ready(token).is_err());
    assert_eq!(received.ready_token().root(), next.root);
    assert_eq!(received.expected_tasks(), next.expected.as_slice());
    assert!(
        received
            .expected_tasks()
            .iter()
            .all(|task| task.query_execution_id() == next.root.query_execution_id())
    );
    let credentials = next
        .logical
        .probe
        .credentials
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap();
    assert!(Arc::ptr_eq(
        received.original_credentials().unwrap(),
        &credentials
    ));
    drop(credentials);
    next.finish(Some(next_original), Some(received)).await;
}

// One SAME production logical owner; no private QA test-driver replacement.
#[path = "profile_component_tests/production_recovery.rs"]
mod production_recovery;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_logical_profile_owner_replaces_before_visibility_and_receives_only_successor_diagnostics()
 {
    production_recovery::run().await;
}
