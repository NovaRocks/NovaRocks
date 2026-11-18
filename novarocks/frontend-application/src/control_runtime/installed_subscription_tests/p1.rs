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

//! The same owned P=1 stage across actual Relay/consumer work and its Join.
//! Finite protocol peer; no Worker execution, Native M5 or extra Root is claimed.
//! The existing Statistics/CountOnly description exercises the common P1 owner;
//! Read/Client/Scalar carrier and class assertions remain separate QA tests.
use super::drive_until;
use crate::fixtures::native_recipe::{
    ActualLogicalCase, OriginalRecipeContext, OriginalRecipeOperands,
    PreparedOriginalRecipeProcess, ReadCloseGate, RecipeRoot, count_description,
    install_original_recipe, prepare_original_recipe_process_with_budgets,
};
use crate::native::{data_runtime::FrontendDataRuntime, task_transport::tests::Loopback};
use crate::query_execution::internal_consumer::attempt_handoff::{
    AttemptHandoffClaimPort, attempt_handoff_mailbox,
};
use novarocks_coordination_contract::ExecutionFailureSite;
use novarocks_execution_contract::{
    SafeDetail,
    failure::{FailureClass, FailureFact, FailureReason},
};
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{
        ComputationReadyToken, ExecutionOutputObligation, InternalComputationConsumer,
        InternalConsumerActivation, InternalConsumerFuture, InternalResultDelivery,
        QueryExecutionError, StatementNativeHealthFailure,
    },
    execution_error::ExecutionFailureInput,
};
use novarocks_workload_control::{
    ResultCapacityConfig, RootWork, WorkClass, WorkOwner, WorkRequest, WorkRetirementObserver,
    joint_admission as joint,
};
use std::{
    future::Future,
    num::NonZeroU64,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

const BYTES: usize = 8 * 1024 * 1024;
#[derive(Debug, Default)]
struct CountSignals {
    accepted: AtomicBool,
    frame_dropped: AtomicBool,
    ready: Mutex<Option<ComputationReadyToken>>,
    plan: Mutex<Option<novarocks_query_application::api::PlanSeal>>,
    returned: Mutex<Option<Result<(), QueryExecutionError>>>,
    release: tokio::sync::Notify,
}
struct ConsumerFrame(Arc<CountSignals>);
impl Drop for ConsumerFrame {
    fn drop(&mut self) {
        self.0.frame_dropped.store(true, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct HeldCountConsumer {
    signals: Arc<CountSignals>,
    domain_error: Option<QueryExecutionError>,
}
impl InternalComputationConsumer for HeldCountConsumer {
    fn begin_local_close(&self) {}
    fn poll_local_closed(
        &self,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        // This same original CountOnly consumer issues no local CPU jobs.
        Poll::Ready(Ok(()))
    }
    fn consume(&self, original: InternalConsumerActivation) -> InternalConsumerFuture {
        let signals = self.signals.clone();
        let error = self.domain_error.clone();
        Box::pin(async move {
            let _frame = ConsumerFrame(signals.clone());
            let (mut stream, source) = original.into_parts();
            let InternalResultDelivery::End(end) = stream.next().await?.expect("actual Root End")
            else {
                panic!("CountOnly cannot deliver a data segment");
            };
            let decoded = end.complete().await?;
            let activation = source.activation();
            assert!(
                signals
                    .plan
                    .lock()
                    .unwrap()
                    .replace(source.plan())
                    .is_none()
            );
            let accepted = source.accept(decoded).await?;
            assert_eq!(accepted.activation(), activation);
            assert!(signals.ready.lock().unwrap().replace(accepted).is_none());
            signals.accepted.store(true, Ordering::SeqCst);
            signals.release.notified().await;
            let result = error.map_or(Ok(()), Err);
            assert!(
                signals
                    .returned
                    .lock()
                    .unwrap()
                    .replace(result.clone())
                    .is_none()
            );
            result
        })
    }
}
struct ReleaseConsumer(Arc<CountSignals>);
impl Drop for ReleaseConsumer {
    fn drop(&mut self) {
        self.0.release.notify_one();
    }
}
#[derive(Default)]
struct PaidChildConstruction {
    pending: Option<joint::JointAdmission>,
    ready: Option<joint::JointGrantedBatch>,
    error: Option<joint::JointAdmissionFailure>,
}
// This complete record is actually instantiated before its own metadata quote.
// The process, recipe, consumer and Native work remain separate original owners.
#[derive(Default)]
struct P1Custody {
    root: Option<RootWork>,
    payload: Option<joint::RetainedPayloadGrant>,
    initial: Option<joint::RootDomainInitialAdmission>,
    first_stage: Option<WorkOwner>,
    first_retired: Option<WorkRetirementObserver>,
    child: PaidChildConstruction,
    first_pending: Option<joint::RootStageAdmission>,
    first_ready: Option<joint::RootStageReady>,
    first_error: Option<joint::JointAdmissionFailure>,
    computation: Option<joint::InternalComputationGrant>,
}
#[derive(Default)]
struct CaseState {
    custody: P1Custody,
    binding: Option<QueryResultCapacityBinding>,
    logical: Option<ActualLogicalCase>,
    claims: Option<AttemptHandoffClaimPort>,
    cohort:
        Option<crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort>,
    next_stage: Option<WorkOwner>,
    next: Option<joint::RootStageAdmission>,
    next_ready: Option<joint::RootStageReady>,
    next_error: Option<joint::JointAdmissionFailure>,
    next_compute: Option<joint::InternalComputationGrant>,
    receiving: Option<ExecutionOutputObligation>,
}
struct OriginalRun {
    data: FrontendDataRuntime,
    peer: Option<Loopback>,
    bootstrap: Option<PreparedOriginalRecipeProcess>,
    state: CaseState,
}
struct Retained(Option<Box<OriginalRun>>);
impl Drop for Retained {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            static UNKNOWN: OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> = OnceLock::new();
            UNKNOWN
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .push(original);
        }
    }
}
fn drive(driver: &mut joint::JointProgressDriver) {
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(driver);
}
fn paid_child(
    construction: &mut PaidChildConstruction,
    destination: &mut Option<WorkOwner>,
    scope: &novarocks_workload_control::WorkScope,
    driver: &mut joint::JointProgressDriver,
) {
    construction.pending = Some(scope.request_joint_child().unwrap());
    drive(driver);
    match Pin::new(construction.pending.as_mut().unwrap())
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(Ok(ready)) => construction.ready = Some(ready),
        Poll::Ready(Err(error)) => construction.error = Some(error),
        Poll::Pending => (),
    }
    assert!(
        construction.error.is_none() && construction.ready.is_some(),
        "same original paid child must be available"
    );
    construction
        .ready
        .as_mut()
        .unwrap()
        .adjustment
        .as_mut()
        .unwrap()
        .install_child_in(scope, destination)
        .unwrap()
        .notify();
    drop(construction.ready.take());
    drop(construction.pending.take());
    drive(driver);
    assert!(destination.is_some());
}
fn paid_originals(run: &mut OriginalRun) {
    // The returned bootstrap and this SAME record are already in the whole-run
    // guard before any new Root, paid constructor or positive grant exists.
    let bootstrap = run.bootstrap.as_mut().unwrap();
    let c = &mut run.state;
    c.custody.root = Some(
        bootstrap
            .workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap(),
    );
    let scope = c.custody.root.as_ref().unwrap().owner.scope();
    let domains = scope.prepare_root_domains().unwrap();
    let source = domains
        .initial_payload_source(&scope, joint::JointLane::Custody)
        .unwrap();
    let mut proposal = match source
        .try_begin(
            NonZeroU64::new(u64::try_from(std::mem::size_of::<P1Custody>()).unwrap()).unwrap(),
            NonZeroU64::new(u64::try_from(BYTES).unwrap()).unwrap(),
        )
        .unwrap()
    {
        joint::JointTryBegin::Started(proposal) => proposal,
        joint::JointTryBegin::Full(_) => panic!("same original Root has no previous constructor"),
    };
    proposal
        .push_dependency(&novarocks_resource_contract::SharedDependency::LocalHost)
        .unwrap();
    c.custody.initial = Some(proposal.submit().unwrap());
    drive(&mut bootstrap.joint);
    match c.custody.initial.as_mut().unwrap().poll_install_in(
        &mut Context::from_waker(Waker::noop()),
        &scope,
        &mut c.custody.payload,
    ) {
        Poll::Ready(Ok(notice)) => notice.notify(),
        _ => panic!("same original Root payload must install"),
    }
    drop(c.custody.initial.take());
    paid_child(
        &mut c.custody.child,
        &mut c.custody.first_stage,
        &scope,
        &mut bootstrap.joint,
    );
    c.custody.first_retired = Some(
        c.custody
            .first_stage
            .as_ref()
            .unwrap()
            .retirement_observer()
            .unwrap(),
    );
    let mut batch = c
        .custody
        .payload
        .as_ref()
        .unwrap()
        .root_stage_source(&scope, c.custody.first_stage.as_ref().unwrap())
        .unwrap()
        .begin(u64::try_from(BYTES).unwrap(), joint::JointLane::Foreground)
        .unwrap();
    batch.end_payload().unwrap();
    batch.end_stage().unwrap();
    c.custody.first_pending = Some(batch.submit().unwrap());
    drive(&mut bootstrap.joint);
    match Pin::new(c.custody.first_pending.as_mut().unwrap())
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(Ok(ready)) => c.custody.first_ready = Some(ready),
        Poll::Ready(Err(error)) => c.custody.first_error = Some(error),
        Poll::Pending => (),
    }
    assert!(
        c.custody.first_error.is_none() && c.custody.first_ready.is_some(),
        "same original P=1 stage"
    );
    c.custody
        .first_ready
        .as_mut()
        .unwrap()
        .install_in(
            &scope,
            c.custody.payload.as_ref().unwrap(),
            c.custody.first_stage.as_ref().unwrap(),
            &mut c.custody.computation,
        )
        .unwrap()
        .notify();
    drop(c.custody.first_ready.take());
    drop(c.custody.first_pending.take());
    QueryResultCapacityBinding::from_owned_internal_computation_in(
        &scope,
        c.custody.first_stage.as_ref().unwrap(),
        c.custody.payload.as_ref().unwrap(),
        &mut c.custody.computation,
        &mut c.binding,
    )
    .unwrap();
    assert!(
        c.custody.computation.is_none(),
        "one original positive grant moves into same activity cell"
    );
    drive(&mut bootstrap.joint);
    assert_eq!(
        scope.result_capacity().unwrap().snapshot().held_positions,
        [0, 0, 1, 0]
    );
}
fn original_domain_error() -> QueryExecutionError {
    // Preserve the original supervisor fixture's typed fact/site/detail.
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Unexpected,
            FailureReason::new(std::num::NonZeroU16::MAX, std::num::NonZeroU16::MIN),
        ),
        SafeDetail::truncating("ordinary fixture domain error after accepted Ready"),
        ExecutionFailureSite::Coordinator,
    ))
}
async fn run(domain_error: bool) {
    // Establish stable custody immediately after DataRuntime returns, then
    // put each returned original in that SAME guard before the next step.
    let mut retained = Retained(Some(Box::new(OriginalRun {
        data: FrontendDataRuntime::new(tokio::runtime::Handle::current()),
        peer: None,
        bootstrap: None,
        state: CaseState::default(),
    })));
    retained.0.as_mut().unwrap().peer = Some(Loopback::start().await);
    {
        let run = retained.0.as_mut().unwrap();
        let descriptor = run
            .peer
            .as_ref()
            .unwrap()
            .bounded_root_descriptor(novarocks_types::BackendProcessId::new_v7());
        let mut result = ResultCapacityConfig::V1;
        result.positions[2] = 1;
        result.all_objects_bytes[2] = u64::try_from(BYTES).unwrap();
        run.bootstrap = Some(prepare_original_recipe_process_with_budgets(
            &run.data,
            descriptor,
            novarocks_control_runtime::history::HistoryCapture::new(65_536),
            result,
            None,
            Duration::from_secs(1),
            Duration::from_secs(10),
        ));
        paid_originals(run);
    }
    let OriginalRun {
        data,
        peer,
        bootstrap,
        state: c,
    } = retained.0.as_mut().unwrap().as_mut();
    let bootstrap = bootstrap.as_mut().unwrap();
    let scope = c.custody.root.as_ref().unwrap().owner.scope();
    let binding = c.binding.as_ref().unwrap();
    let tail = binding.try_clone().unwrap();
    let old = binding.try_window_alias().unwrap();
    let old_manifest = old.internal_manifest_binding().unwrap().unwrap().clone();
    let old_authority = old.native_action_authority().unwrap();
    let quote = joint::NativeAsyncActionQuote::for_original_native_transport(
        NonZeroU64::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
    );
    let signals = Arc::new(CountSignals::default());
    let _release_consumer = ReleaseConsumer(signals.clone());
    let original_error = domain_error.then(original_domain_error);
    let consumer: Arc<dyn InternalComputationConsumer> = Arc::new(HeldCountConsumer {
        signals: signals.clone(),
        domain_error: original_error.clone(),
    });
    let (offers, claims) = attempt_handoff_mailbox();
    c.claims = Some(claims);
    let (description, plan) = count_description();
    let (logical, cohort, ()) = install_original_recipe(
        OriginalRecipeContext {
            fixture: &mut bootstrap.fixture,
            joint: &mut bootstrap.joint,
            process: &bootstrap.process,
            data,
            clock: &bootstrap.clock,
            handle: bootstrap.handle.clone(),
            descriptor: bootstrap.descriptor.clone(),
            client: bootstrap.client.clone(),
        },
        OriginalRecipeOperands {
            owner: c.custody.first_stage.take().unwrap(),
            binding: binding.clone(),
            description,
            plan,
            root: RecipeRoot::EmptyCountOnly,
            consumer,
            offers,
            output: (),
        },
    );
    // Shared installation transferred the SAME first owner through its own
    // original contract; stabilize returned pieces before any new assertion.
    c.logical = Some(logical);
    c.cohort = Some(cohort);
    let gate = Arc::new(ReadCloseGate::default());
    let _release_read = gate.release_guard();
    *c.logical
        .as_ref()
        .unwrap()
        .probe
        .read_close_gate
        .lock()
        .unwrap() = Some(gate.clone());
    paid_child(
        &mut c.custody.child,
        &mut c.next_stage,
        &scope,
        &mut bootstrap.joint,
    );
    let logical = c.logical.as_ref().unwrap();
    let installed = bootstrap
        .fixture
        .activate_attempt(logical, &mut bootstrap.joint, false)
        .await;
    let root = logical.probe.root.lock().unwrap().unwrap();
    let activation = logical.probe.activation.lock().unwrap().unwrap();
    peer.as_ref()
        .unwrap()
        .enable_single_root_lifecycle(root, novarocks_result_contract::RootOutputKind::CountOnly);
    logical.probe.release_activation();
    // Poll only a borrow of the same original response future. On a failed
    // wait it stays in the WholeRun guard; diagnostics do not cancel it.
    let ready_result = {
        let ready = c.logical.as_mut().unwrap().ready.as_mut().unwrap();
        drive_until(
            &bootstrap.process,
            &mut bootstrap.joint,
            ready.as_mut(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
    };
    if ready_result.is_none() {
        let logical = c.logical.as_ref().unwrap();
        let bound = logical.binding.bound();
        let lp = bound.as_ref().ok().map(|ports| {
            bootstrap
                .process
                .actor_runtime()
                .snapshot(ports.registration.actor())
        });
        let original_ready = bound
            .as_ref()
            .ok()
            .and_then(|ports| ports.original_consumer_for_fixture(activation).ok())
            .and_then(|weak| weak.upgrade())
            .map(|consumer| consumer.original_ready_promise(activation));
        // Only passive original observations. No Core read, new input, I/O,
        // clock advancement, release, retry, synthetic Ready or exit occurs.
        eprintln!(
            "original P1 Ready wait expired: clock={:?}, indexed_deadline={:?}, lp={lp:?}, ap={:?}, subscription={:?}, ledger_unresolved={:?}, ledger_empty={}, host_empty={}, original_ready={original_ready:?}, consumer_plan={:?}, consumer_ready={:?}, consumer_accepted={}, consumer_frame_dropped={}, consumer_returned={:?}, peer_root_progress={:?}, peer_first_reply_waiting={}, read_close_entered={}, read_close_calls={}, actual_ap_exit={}, positions={:?}, original_failure={}",
            bootstrap.process.clock.now(),
            bootstrap.process.actor_runtime().next_deadline(),
            bootstrap
                .process
                .actor_runtime()
                .snapshot(installed.fixture_sources.0),
            bootstrap
                .process
                .actor_runtime()
                .snapshot(installed.fixture_sources.1),
            bootstrap.process.ledger().unresolved_work(),
            bootstrap.process.ledger().is_empty(),
            bootstrap.process.host.is_empty(),
            *signals.plan.lock().unwrap(),
            *signals.ready.lock().unwrap(),
            signals.accepted.load(Ordering::SeqCst),
            signals.frame_dropped.load(Ordering::SeqCst),
            *signals.returned.lock().unwrap(),
            peer.as_ref().unwrap().single_root_result_progress(),
            peer.as_ref().unwrap().single_root_first_reply_waiting(),
            gate.entered(),
            gate.calls(),
            *installed.fixture_exit.observe_actual_exit().borrow(),
            scope
                .result_capacity()
                .map(|capacity| capacity.snapshot().held_positions),
            logical.binding.original_failure_for_fixture(),
        );
    }
    c.receiving = Some(ready_result.unwrap().unwrap());
    drop(c.logical.as_mut().unwrap().ready.take());
    assert!(
        c.receiving.as_mut().unwrap().take_output().is_none(),
        "CountOnly has no Rows output"
    );
    // Baseline observes the SAME consumer's completed accept before proposing
    // the next Root domain stage; publication of Ready does not poll that F.
    let accepted_observed = drive_until(
        &bootstrap.process,
        &mut bootstrap.joint,
        async {
            while !signals.accepted.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await;
    assert!(
        accepted_observed.is_some(),
        "the original consumer must observe its accepted Ready"
    );
    let token = *c.receiving.as_ref().unwrap().ready_token().unwrap();
    assert_eq!(token.activation(), activation);
    assert_eq!(token.root(), root);
    assert_eq!(*signals.plan.lock().unwrap(), Some(plan));
    assert_eq!(token.end().output_rows, 0);
    assert_eq!(*signals.ready.lock().unwrap(), Some(token));
    assert!(signals.accepted.load(Ordering::SeqCst));
    assert!(
        !signals.frame_dropped.load(Ordering::SeqCst),
        "Ready cannot retire the actual original consumer F"
    );
    // Match the original P1 test: genuine Ready/consumer accept precedes the
    // next Root-domain proposal. Submitting it before initial Native work
    // serializes that work behind the position it is itself waiting to free.
    let mut batch = c
        .custody
        .payload
        .as_ref()
        .unwrap()
        .root_stage_source(&scope, c.next_stage.as_ref().unwrap())
        .unwrap()
        .begin(u64::try_from(BYTES).unwrap(), joint::JointLane::Foreground)
        .unwrap();
    batch.end_payload().unwrap();
    batch.end_stage().unwrap();
    c.next = Some(batch.submit().unwrap());
    drive(&mut bootstrap.joint);
    match Pin::new(c.next.as_mut().unwrap()).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => c.next_ready = Some(ready),
        Poll::Ready(Err(error)) => c.next_error = Some(error),
        Poll::Pending => (),
    }
    assert!(
        c.next_ready.is_none() && c.next_error.is_none(),
        "SAME next stage initially waits behind original P=1"
    );
    let next = c.next.as_mut().unwrap();
    let premature = drive_until(
        &bootstrap.process,
        &mut bootstrap.joint,
        std::future::poll_fn(|cx| Pin::new(&mut *next).poll(cx)),
        Instant::now() + Duration::from_millis(20),
    )
    .await;
    if let Some(result) = premature {
        match result {
            Ok(ready) => c.next_ready = Some(ready),
            Err(error) => c.next_error = Some(error),
        }
    }
    assert!(
        c.next_ready.is_none() && c.next_error.is_none(),
        "SAME next stage stays pending while actual consumer F lives"
    );
    assert_eq!(
        scope.result_capacity().unwrap().snapshot().held_positions,
        [0, 0, 1, 0]
    );
    signals.release.notify_one();
    let next = c.next.as_mut().unwrap();
    let same = drive_until(
        &bootstrap.process,
        &mut bootstrap.joint,
        std::future::poll_fn(|cx| Pin::new(&mut *next).poll(cx)),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    match same {
        Ok(ready) => c.next_ready = Some(ready),
        Err(error) => c.next_error = Some(error),
    }
    assert!(c.next_error.is_none());
    assert!(signals.frame_dropped.load(Ordering::SeqCst));
    assert_eq!(
        *signals.returned.lock().unwrap(),
        Some(original_error.clone().map_or(Ok(()), Err))
    );
    c.next_ready
        .as_mut()
        .unwrap()
        .install_in(
            &scope,
            c.custody.payload.as_ref().unwrap(),
            c.next_stage.as_ref().unwrap(),
            &mut c.next_compute,
        )
        .unwrap()
        .notify();
    drop(c.next_ready.take());
    drop(c.next.take());
    assert_eq!(
        scope.result_capacity().unwrap().snapshot().held_positions,
        [0, 0, 1, 0]
    );
    let entered = drive_until(
        &bootstrap.process,
        &mut bootstrap.joint,
        async {
            while !gate.entered() {
                tokio::task::yield_now().await;
            }
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await;
    assert!(entered.is_some());
    assert_eq!(gate.calls(), 1);
    assert!(!*installed.fixture_exit.observe_actual_exit().borrow());
    let logical = c.logical.as_mut().unwrap();
    assert!(
        futures::poll!(std::future::poll_fn(|cx| logical
            .exit
            .as_mut()
            .unwrap()
            .poll_exit(cx)))
        .is_pending(),
        "actual Native cleanup remains independently held after SAME P=1 stage progressed"
    );
    assert_eq!(
        c.receiving.as_ref().unwrap().ready_token(),
        Some(&token),
        "late ordinary domain verdict cannot rewrite accepted Ready"
    );
    assert!(old_manifest.try_begin().is_err());
    assert!(
        old_authority
            .action_source(joint::JointLane::Foreground, quote)
            .is_err()
    );
    assert!(
        old_authority
            .action_source(joint::JointLane::Settlement, quote)
            .is_ok()
    );
    tail.try_window_alias()
        .unwrap()
        .check_backing_total(u64::try_from(BYTES).unwrap())
        .unwrap();
    gate.release();
    drop(c.receiving.take());
    let mut exit = c.logical.as_mut().unwrap().exit.take().unwrap();
    let actual = drive_until(
        &bootstrap.process,
        &mut bootstrap.joint,
        std::future::poll_fn(|cx| exit.poll_exit(cx)),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(
        actual.is_ok(),
        "ordinary domain refusal is not unproven physical exit"
    );
    assert!(*installed.fixture_exit.observe_actual_exit().borrow());
    drop((
        actual,
        exit,
        installed,
        tail,
        old,
        old_manifest,
        old_authority,
    ));
    drop((
        c.logical.take(),
        c.claims.take(),
        c.cohort.take(),
        c.binding.take(),
    ));
    let shutdown = drive_until(
        &bootstrap.process,
        &mut bootstrap.joint,
        bootstrap
            .supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(5)),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(shutdown.is_ok());
    bootstrap
        .process
        .close_original_host_registration_for_fixture();
    bootstrap.fixture.finish().await;
    drop(c.next_compute.take());
    c.next_stage.take().unwrap().complete();
    assert!(
        c.custody
            .first_retired
            .as_ref()
            .unwrap()
            .is_reclaimed()
            .unwrap()
    );
    drop(c.custody.payload.take());
    let root = c.custody.root.take().unwrap();
    root.business.release();
    root.owner.complete();
    drive(&mut bootstrap.joint);
    tokio::time::timeout(Duration::from_secs(5), async {
        let released = scope.wait_released();
        tokio::pin!(released);
        loop {
            drive(&mut bootstrap.joint);
            if let Poll::Ready(()) = futures::poll!(released.as_mut()) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    data.drain_original_apply_until(deadline).await.unwrap();
    data.drain_native_outgoing_until(deadline).await.unwrap();
    assert!(peer.as_mut().unwrap().settle().await);
    assert!(bootstrap.process.original_registries_empty_for_fixture());
    drop(scope);
    let OriginalRun { bootstrap, .. } = *retained.0.take().unwrap();
    let PreparedOriginalRecipeProcess {
        workload,
        mut joint,
        ..
    } = bootstrap.unwrap();
    workload.close_admission();
    drive(&mut joint);
    if let Err(failure) = workload.shutdown() {
        let (error, same_owner) = failure.into_parts();
        // Every original child/source above has actual exit; retain only the
        // unresolved original Control on a failed final disposition.
        static FINAL_UNKNOWN: OnceLock<Mutex<Vec<novarocks_workload_control::WorkloadControl>>> =
            OnceLock::new();
        FINAL_UNKNOWN
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .push(same_owner);
        panic!("the same original P1 Control did not drain: {error:?}");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn count_only_ready_retains_p1_until_actual_consumer_and_pump_exit() {
    run(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn count_only_late_domain_error_releases_p1_after_actual_consumer_exit() {
    run(true).await;
}
