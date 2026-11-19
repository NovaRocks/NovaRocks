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

//! Actual original handoff through the existing QA/LP/AP recipe and consumer.
//! The owner-only Pending/duplicate/stale checks supply no Ready capability.
use super::super::*;
use crate::fixtures::native_recipe::{
    ActualLogicalCase, OriginalHandoffBindingGate, OriginalHandoffStage, OriginalRecipeContext,
    OriginalRecipeDiagnostics, OriginalRecipeOperands, PreparedOriginalRecipeProcess, RecipeRoot,
    count_description, install_original_recipe_with_diagnostics,
    prepare_original_recipe_process_with_budgets,
};
use crate::native::{data_runtime::FrontendDataRuntime, task_transport::tests::Loopback};
use crate::query_execution::internal_consumer::profile::{
    ProfileComputationOutput, ReadyProfileComputation,
    profile_computation_consumer_with_handoff_for_fixture,
};
use crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort;
use crate::task_execution::profile_diagnostics::ProfileDiagnosticObserver;
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{ComputationReadyToken, ExecutionOutputObligation},
    coordination::LogicalExecutionSupervisor,
};
use novarocks_types::BackendProcessId;
use novarocks_workload_control::{
    BusinessPermit, ResultCapacityConfig, ResultCapacityHandle, ResultWindowClass,
    ResultWindowGrant, WorkClass, WorkRequest, WorkScope, WorkloadControl,
    joint_admission::JointProgressDriver,
};
use std::{
    sync::{Arc, Mutex, OnceLock, Weak},
    task::Poll,
    time::{Duration, Instant},
};

struct RetainedHandoffCase(Option<Box<HandoffCase>>);
impl Drop for RetainedHandoffCase {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            static UNRESOLVED: OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
                OnceLock::new();
            UNRESOLVED
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .push(original);
        }
    }
}
struct HandoffCase {
    fixture: Option<crate::fixtures::FrontendFixture>,
    logical: Option<ActualLogicalCase>,
    process: Arc<crate::control_runtime::FrontendAttemptRuntime>,
    clock: Arc<crate::task_execution::clock::ManualClock>,
    joint: JointProgressDriver,
    workload: Option<WorkloadControl>,
    supervisor: Option<LogicalExecutionSupervisor>,
    scope: WorkScope,
    business: Option<BusinessPermit>,
    capacity: ResultCapacityHandle,
    window: Option<ResultWindowGrant>,
    cohort: Option<NativeSourceCohort>,
    output: Option<ProfileComputationOutput>,
    receiving: Option<ExecutionOutputObligation>,
    ready: Option<ReadyProfileComputation>,
    observer: Option<ProfileDiagnosticObserver>,
    installed: Option<Arc<crate::task_execution::host::install::InstalledAttemptHost>>,
    data: FrontendDataRuntime,
    peer: Option<Loopback>,
    history: novarocks_control_runtime::history::HistoryCapture,
    gate: Arc<OriginalHandoffBindingGate>,
    mailbox: Weak<Mailbox>,
    source_exited: bool,
}
impl HandoffCase {
    async fn install_retained(
        pair: (AttemptHandoffOfferPort, AttemptHandoffClaimPort),
    ) -> RetainedHandoffCase {
        let mailbox = Arc::downgrade(&pair.1.mailbox);
        let data = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let peer = Loopback::start().await;
        let descriptor = peer.bounded_root_descriptor(BackendProcessId::new_v7());
        let history = novarocks_control_runtime::history::HistoryCapture::new(65_536);
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
        } = prepare_original_recipe_process_with_budgets(
            &data,
            descriptor,
            history.clone(),
            ResultCapacityConfig::V1,
            None,
            Duration::from_secs(1),
            Duration::from_secs(5),
        );
        let root = workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let window = capacity
            .try_acquire(&scope, ResultWindowClass::Internal)
            .unwrap();
        let (description, plan) = count_description();
        let mut pair = Some(pair);
        let (consumer, offers, output) =
            profile_computation_consumer_with_handoff_for_fixture(&description, plan, &mut pair)
                .unwrap();
        assert!(pair.is_none());
        let (logical, cohort, output) = install_original_recipe_with_diagnostics(
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
                owner: root.owner,
                binding: QueryResultCapacityBinding::try_new(&scope, window.retain_alias())
                    .unwrap(),
                description,
                plan,
                root: RecipeRoot::EmptyCountOnly.with_three_task_protocol_chain(plan, [2, 3]),
                consumer,
                offers,
                output,
            },
            Some(OriginalRecipeDiagnostics {
                transport: Arc::new(super::UnavailableFinalInfo),
            }),
        );
        let gate = Arc::new(OriginalHandoffBindingGate::default());
        *logical.probe.handoff_binding_gate.lock().unwrap() = Some(gate.clone());
        logical
            .probe
            .graph_capture_requested
            .store(true, std::sync::atomic::Ordering::Release);
        logical
            .probe
            .credentials_requested
            .store(true, std::sync::atomic::Ordering::Release);
        RetainedHandoffCase(Some(Box::new(Self {
            fixture: Some(fixture),
            logical: Some(logical),
            process,
            clock,
            joint,
            workload: Some(workload),
            supervisor: Some(supervisor),
            scope,
            business: Some(root.business),
            capacity,
            window: Some(window),
            cohort: Some(cohort),
            output: Some(output),
            receiving: None,
            ready: None,
            observer: None,
            installed: None,
            data,
            peer: Some(peer),
            history,
            gate,
            mailbox,
            source_exited: false,
        })))
    }
    async fn staged_then_original_bind(
        &mut self,
        old: Option<ClaimKey>,
    ) -> (OriginalHandoffStage, Arc<Cell>) {
        let gate = self.gate.clone();
        let weak = self.mailbox.clone();
        let probe = self.logical.as_ref().unwrap().probe.clone();
        let activation = self.fixture.as_mut().unwrap().activate_attempt(
            self.logical.as_ref().unwrap(),
            &mut self.joint,
            false,
        );
        tokio::pin!(activation);
        let until = Instant::now() + Duration::from_secs(5);
        let stage = loop {
            if let Some(stage) = gate.entered() {
                break stage;
            }
            assert!(
                Instant::now() < until,
                "same original stage boundary was not reached"
            );
            assert!(
                futures::poll!(activation.as_mut()).is_pending(),
                "actual activation cannot finish before its original bind gate"
            );
            tokio::task::yield_now().await;
        };
        let mailbox = weak
            .upgrade()
            .expect("the fixed consumer still owns the original directory");
        let cell = mailbox.offer.lock().unwrap().as_ref().unwrap().clone();
        {
            let state = cell.state.lock().unwrap();
            let State::Staged(frozen) = &*state else {
                panic!("same resources must be staged, not bound");
            };
            assert_eq!(frozen.root, stage.root);
            assert_eq!(frozen.plan, stage.plan);
            let expected = probe.credentials.lock().unwrap().clone().unwrap();
            assert!(std::ptr::eq(
                Arc::as_ptr(frozen.resources.credentials.as_ref().unwrap()),
                expected.as_ptr()
            ));
            assert!(frozen.resources.profile.is_some());
        }
        if let Some(old) = old {
            assert!(
                claim_exact(mailbox.clone(), old).await.is_err(),
                "an old actual source cannot take the new staged original offer"
            );
            assert!(Arc::ptr_eq(
                mailbox.offer.lock().unwrap().as_ref().unwrap(),
                &cell
            ));
        }
        {
            let pending = claim_exact(
                mailbox.clone(),
                ClaimKey {
                    activation: stage.activation,
                    root: stage.root,
                    plan: stage.plan,
                },
            );
            tokio::pin!(pending);
            assert!(
                futures::poll!(pending.as_mut()).is_pending(),
                "actual staged resources do not grant the consumer a binding"
            );
            // Cancel only this owner-level borrow. The real producer and all
            // resources stay in the same Native slot for the actual consumer.
        }
        drop(mailbox);
        gate.release();
        self.installed = Some(activation.await);
        (stage, cell)
    }
    fn configure_original_root(&self, hold_end: bool) {
        let logical = self.logical.as_ref().unwrap();
        let root = logical.probe.root.lock().unwrap().unwrap();
        let graph = logical
            .probe
            .graph_capture
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        assert_eq!(graph.root, root);
        assert_eq!(graph.tasks.len(), 3);
        let tasks = graph
            .tasks
            .iter()
            .map(|task| task.identity())
            .collect::<Vec<_>>();
        self.peer.as_ref().unwrap().enable_single_root_lifecycle(
            root,
            novarocks_result_contract::RootOutputKind::CountOnly,
        );
        self.peer
            .as_ref()
            .unwrap()
            .enable_three_task_roster(tasks.try_into().unwrap())
            .unwrap();
        if hold_end {
            self.peer.as_ref().unwrap().defer_empty_root_end().unwrap();
        }
        logical.probe.release_activation();
    }
    async fn round(&mut self) {
        crate::fixtures::native_recipe::drive_typed(&mut self.joint);
        self.fixture
            .as_mut()
            .unwrap()
            .progress(
                crate::fixtures::ProgressSelection::All,
                crate::fixtures::ProgressBudget::default(),
            )
            .await;
        tokio::task::yield_now().await;
    }
    async fn wait_claimed_and_actual_read(&mut self, cell: &Cell) {
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            let claimed = matches!(*cell.state.lock().unwrap(), State::Claimed(_));
            if claimed
                && self
                    .peer
                    .as_ref()
                    .unwrap()
                    .single_root_first_result_waiting()
            {
                break;
            }
            assert!(
                Instant::now() < until,
                "same consumer did not claim and enter its original Root RPC"
            );
            self.round().await;
        }
        assert!(
            futures::poll!(
                self.logical
                    .as_mut()
                    .unwrap()
                    .ready
                    .as_mut()
                    .unwrap()
                    .as_mut()
            )
            .is_pending()
        );
        assert_eq!(
            self.peer.as_ref().unwrap().single_root_result_progress(),
            (false, 0)
        );
        assert!(
            !*self
                .installed
                .as_ref()
                .unwrap()
                .fixture_exit
                .observe_actual_exit()
                .borrow()
        );
    }
    async fn wait_ready(&mut self) -> ComputationReadyToken {
        let until = Instant::now() + Duration::from_secs(5);
        let mut original = loop {
            if let Poll::Ready(result) = futures::poll!(
                self.logical
                    .as_mut()
                    .unwrap()
                    .ready
                    .as_mut()
                    .unwrap()
                    .as_mut()
            ) {
                break result.unwrap();
            }
            assert!(
                Instant::now() < until,
                "same actual Profile consumer did not reach Ready"
            );
            self.round().await;
        };
        let token = *original.ready_token().unwrap();
        if let Some(exit) = original.take_exit_observer() {
            assert!(self.logical.as_ref().unwrap().exit.is_none());
            self.logical.as_mut().unwrap().exit = Some(exit);
        }
        self.receiving = Some(original);
        self.ready = Some(self.output.as_mut().unwrap().take_ready(token).unwrap());
        assert!(self.output.as_mut().unwrap().take_ready(token).is_err());
        let logical = self.logical.as_ref().unwrap();
        let graph = logical
            .probe
            .graph_capture
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        let expected = graph
            .tasks
            .iter()
            .map(|task| task.identity())
            .collect::<std::collections::BTreeSet<_>>();
        let ready = self.ready.as_ref().unwrap();
        assert_eq!(ready.ready_token(), token);
        assert_eq!(ready.output_rows(), 0);
        assert_eq!(
            ready
                .expected_tasks()
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );
        assert!(
            ready
                .expected_tasks()
                .iter()
                .all(|task| task.query_execution_id() == token.activation().execution())
        );
        let credentials = logical.probe.credentials.lock().unwrap().clone().unwrap();
        assert!(std::ptr::eq(
            Arc::as_ptr(ready.original_credentials().unwrap()),
            credentials.as_ptr()
        ));
        self.observer = Some(
            logical
                .probe
                .profile_observer
                .lock()
                .unwrap()
                .take()
                .unwrap(),
        );
        loop {
            if let Some(snapshot) = self.ready.as_ref().unwrap().current_diagnostics().unwrap() {
                assert_eq!(snapshot.ready, token);
                let same = self
                    .observer
                    .as_ref()
                    .unwrap()
                    .current(token)
                    .unwrap()
                    .unwrap();
                assert!(Arc::ptr_eq(&snapshot, &same));
                break;
            }
            if Instant::now() >= until {
                use novarocks_coordination_contract::MonotonicClock;
                let logical = self.logical.as_ref().unwrap();
                let bound = logical.binding.bound().unwrap();
                let installed = self.installed.as_ref().unwrap();
                let peer = self.peer.as_ref().unwrap();
                eprintln!(
                    "original handoff diagnostics wait expired: token={token:?}, clock={:?}, indexed_deadline={:?}, lp={:?}, ap={:?}, subscription={:?}, actual_ap_exit={}, peer_root_progress={:?}, peer_release={}, registered_convergence={:?}, original_failure={}, unresolved_work={:?}",
                    self.clock.now(),
                    self.process.actor_runtime().next_deadline(),
                    bound.runtime.snapshot(bound.registration.actor()),
                    bound.runtime.snapshot(installed.fixture_sources.0),
                    bound.runtime.snapshot(installed.fixture_sources.1),
                    *installed.fixture_exit.observe_actual_exit().borrow(),
                    peer.single_root_result_progress(),
                    peer.single_root_release_observed(),
                    logical.binding.original_convergence_for_fixture(token),
                    logical.binding.original_failure_for_fixture(),
                    self.process.ledger().unresolved_work(),
                );
            }
            assert!(
                Instant::now() < until,
                "same Source observer did not publish its original cut"
            );
            self.round().await;
            let now = novarocks_coordination_contract::MonotonicClock::now(self.clock.as_ref());
            if now < token.diagnostic_deadline()
                && self
                    .fixture
                    .as_ref()
                    .unwrap()
                    .runtime()
                    .next_deadline()
                    .is_some_and(|at| {
                        Duration::from_nanos(at.0) == token.diagnostic_deadline().since_origin()
                    })
            {
                // Only the SAME accepted token's already indexed deadline
                // advances this manual clock. The next ordinary round runs
                // the real timer wheel; no synthetic Timer input is injected.
                self.clock
                    .advance(token.diagnostic_deadline().since_origin() - now.since_origin());
            }
        }
        token
    }
    async fn finish_source_keep_ready(&mut self) {
        drop(self.receiving.take());
        let until = Instant::now() + Duration::from_secs(5);
        let receipt = loop {
            let polled = futures::poll!(std::future::poll_fn(|cx| self
                .logical
                .as_mut()
                .unwrap()
                .exit
                .as_mut()
                .unwrap()
                .poll_exit(cx)));
            if let Poll::Ready(receipt) = polled {
                break receipt.unwrap();
            }
            assert!(
                Instant::now() < until,
                "same handoff QA source did not actually exit"
            );
            self.round().await;
        };
        assert!(matches!(
            receipt.view().runtime_actor_result(),
            Some(novarocks_query_application::api::logical_runtime::LogicalExit::Completed)
        ));
        assert!(
            *self
                .installed
                .as_ref()
                .unwrap()
                .fixture_exit
                .observe_actual_exit()
                .borrow()
        );
        assert_eq!(
            self.peer.as_ref().unwrap().single_root_result_progress(),
            (true, 1)
        );
        assert!(self.peer.as_ref().unwrap().single_root_release_observed());
        self.source_exited = true;
        drop(receipt);
    }
    async fn finish_all(&mut self) {
        assert!(self.source_exited);
        let wait = Duration::from_secs(5);
        drop(self.ready.take());
        drop(self.observer.take());
        drop(self.output.take());
        drop(self.installed.take());
        drop(self.logical.take());
        drop(self.cohort.take());
        drop(self.window.take());
        assert_eq!(self.capacity.snapshot().held_positions, [0; 4]);
        let mut supervisor = self.supervisor.take().unwrap();
        {
            let mut shutdown = Box::pin(supervisor.shutdown_until(Instant::now() + wait));
            loop {
                if let Poll::Ready(result) = futures::poll!(shutdown.as_mut()) {
                    result.unwrap();
                    break;
                }
                self.round().await;
            }
        }
        drop(supervisor);
        self.process.close_original_host_registration_for_fixture();
        self.fixture.as_mut().unwrap().finish().await;
        drop(self.fixture.take());
        self.business.take().unwrap().release();
        crate::fixtures::native_recipe::drive_typed(&mut self.joint);
        let progress = self.workload.as_ref().unwrap().deadline_expiry_handle();
        tokio::time::timeout(wait, async {
            let released = self.scope.wait_released();
            tokio::pin!(released);
            loop {
                match self.joint.step() {
                    novarocks_workload_control::joint_admission::JointProgress::More => {
                        tokio::select! {
                            result = released.as_mut() => break result,
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                    novarocks_workload_control::joint_admission::JointProgress::Idle(revision) => {
                        tokio::select! {
                            result = released.as_mut() => break result,
                            _ = progress.wait_state_change(revision) => {}
                        }
                    }
                }
            }
        })
        .await
        .unwrap();
        self.workload.as_ref().unwrap().close_admission();
        crate::fixtures::native_recipe::drive_typed(&mut self.joint);
        if let Err(failure) = self.workload.take().unwrap().shutdown() {
            let (error, owner) = failure.into_parts();
            self.workload = Some(owner);
            panic!("same handoff Control did not drain: {error:?}");
        }
        self.data
            .drain_original_apply_until(Instant::now() + wait)
            .await
            .unwrap();
        self.data
            .drain_native_outgoing_until(Instant::now() + wait)
            .await
            .unwrap();
        assert!(self.peer.take().unwrap().settle().await);
        assert!(self.process.original_registries_empty_for_fixture());
        crate::fixtures::history::CapturedHistory::drain(&self.history, 65_536)
            .check_completed_runtime(self.process.actor_runtime(), self.process.ledger())
            .unwrap();
    }
}

pub(super) async fn staged_original_bundle() {
    let (offers, claims) = attempt_handoff_mailbox();
    let view = offers.reserve_view();
    // The original owner-level Claim endpoint keeps this same reference
    // directory for the duplicate check, as in the original component test.
    let directory = claims.mailbox.clone();
    let mut case = HandoffCase::install_retained((offers, claims)).await;
    let original = case.0.as_mut().unwrap();
    assert!(
        view.mailbox.upgrade().is_some(),
        "same reference directory remains after original offers moves"
    );
    let (stage, cell) = original.staged_then_original_bind(None).await;
    original.configure_original_root(false);
    let ready = original.wait_ready().await;
    assert_eq!(ready.activation(), stage.activation);
    assert_eq!(ready.root(), stage.root);
    assert!(matches!(*cell.state.lock().unwrap(), State::Claimed(_)));
    assert!(
        claim_exact(
            directory.clone(),
            ClaimKey {
                activation: stage.activation,
                root: stage.root,
                plan: stage.plan
            }
        )
        .await
        .is_err(),
        "same actual handoff is consumed once"
    );
    original.finish_source_keep_ready().await;
    assert!(matches!(*cell.state.lock().unwrap(), State::Claimed(_)));
    let expected = original
        .logical
        .as_ref()
        .unwrap()
        .probe
        .credentials
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    assert!(
        std::ptr::eq(
            Arc::as_ptr(
                original
                    .ready
                    .as_ref()
                    .unwrap()
                    .original_credentials()
                    .unwrap()
            ),
            expected.as_ptr()
        ),
        "claimed original storage survives the actual Source/producer destruction"
    );
    original.finish_all().await;
    drop(case.0.take());
}

pub(super) async fn claimed_original_bundle_across_new_source() {
    let (offers, claims) = attempt_handoff_mailbox();
    // The original test owner retains this ONE reference directory across two
    // actual Sources. It grants no QA identity, Source or additional position.
    let directory = claims.mailbox.clone();
    let mut old = HandoffCase::install_retained((offers, claims)).await;
    let old_case = old.0.as_mut().unwrap();
    let (old_stage, old_cell) = old_case.staged_then_original_bind(None).await;
    old_case.configure_original_root(true);
    old_case.wait_claimed_and_actual_read(&old_cell).await;
    let old_key = ClaimKey {
        activation: old_stage.activation,
        root: old_stage.root,
        plan: old_stage.plan,
    };
    let pair = (
        AttemptHandoffOfferPort {
            mailbox: directory.clone(),
        },
        AttemptHandoffClaimPort {
            mailbox: directory.clone(),
        },
    );
    let mut next = HandoffCase::install_retained(pair).await;
    let next_case = next.0.as_mut().unwrap();
    let (next_stage, next_cell) = next_case.staged_then_original_bind(Some(old_key)).await;
    assert!(matches!(*old_cell.state.lock().unwrap(), State::Claimed(_)));
    next_case.configure_original_root(false);
    let ready = next_case.wait_ready().await;
    assert_eq!(ready.activation(), next_stage.activation);
    let old_observer = old_case
        .logical
        .as_ref()
        .unwrap()
        .probe
        .profile_observer
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone_for_fixture();
    assert!(
        old_observer.current(ready).unwrap().is_none(),
        "old held Source cannot see the new snapshot"
    );
    let old_protocol = old_case
        .logical
        .as_ref()
        .unwrap()
        .probe
        .protocol
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    assert!(
        old_protocol
            .snapshot(next_stage.activation, next_stage.root, next_stage.plan)
            .is_err(),
        "old protocol receiver remains bound to its original source tuple"
    );
    let prior = old_protocol
        .snapshot(old_stage.activation, old_stage.root, old_stage.plan)
        .unwrap()
        .unwrap();
    assert_eq!(prior.tasks.len(), 3);
    assert!(
        prior
            .tasks
            .iter()
            .all(|(task, _)| task.query_execution_id() == old_stage.activation.execution())
    );
    let old_storage = old_case
        .logical
        .as_ref()
        .unwrap()
        .probe
        .credentials
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    assert!(
        old_storage.upgrade().is_some(),
        "the actual old consumer retains its claimed resources while Root I/O waits"
    );
    assert!(!std::ptr::eq(
        Arc::as_ptr(
            next_case
                .ready
                .as_ref()
                .unwrap()
                .original_credentials()
                .unwrap()
        ),
        old_storage.as_ptr()
    ));
    next_case.finish_source_keep_ready().await;
    assert!(matches!(
        *next_cell.state.lock().unwrap(),
        State::Claimed(_)
    ));
    assert!(
        old_case
            .peer
            .as_ref()
            .unwrap()
            .single_root_first_result_waiting()
    );
    assert!(
        !*old_case
            .installed
            .as_ref()
            .unwrap()
            .fixture_exit
            .observe_actual_exit()
            .borrow()
    );
    old_case
        .peer
        .as_ref()
        .unwrap()
        .release_single_root_first_result();
    let prior_ready = old_case.wait_ready().await;
    assert_eq!(prior_ready.activation(), old_stage.activation);
    assert!(std::ptr::eq(
        Arc::as_ptr(
            old_case
                .ready
                .as_ref()
                .unwrap()
                .original_credentials()
                .unwrap()
        ),
        old_storage.as_ptr()
    ));
    old_case.finish_source_keep_ready().await;
    assert!(matches!(*old_cell.state.lock().unwrap(), State::Claimed(_)));
    old_case.finish_all().await;
    next_case.finish_all().await;
    drop((old.0.take(), next.0.take()));
}
