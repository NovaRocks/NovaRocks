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

//! Real Profile consumers over the original handoff/TaskRound/QA fixture.

use super::*;
use crate::query_execution::internal_consumer::profile::{
    ReadyProfileComputation, profile_computation_consumer,
};
use crate::task_execution::profile_diagnostics::{
    ProfileDiagnosticClosure, ProfileDiagnosticMissing, ProfileTaskDiagnostic,
};
use novarocks_query_application::api::{InternalComputationConsumer, QueryExecutionError};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};

async fn watchdog<T>(stage: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("Profile component watchdog: {stage}"))
}

#[derive(Default)]
struct FinalInfoProbe {
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
        _: TaskIdentity,
    ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
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

    fn dynamic_filters(
        &self,
        _: TaskIdentity,
        _: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        panic!("Profile consumer must not fetch dynamic filters")
    }
}

/// The biased actor watch is observed before polling a now-ready consumer.
/// This deliberately exercises take_ready while source.accept's caller has
/// not resumed to publish any local completion marker.
async fn run_fixed_consumer_to_real_cut(
    attempt: &OriginalAttempt,
    consumer: Arc<dyn InternalComputationConsumer>,
    activation: InternalConsumerActivation,
    output: &mut crate::query_execution::internal_consumer::profile::ProfileComputationOutput,
    prior_cut: Option<ComputationReadyToken>,
) -> ReadyProfileComputation {
    let status = attempt
        .round
        .execution()
        .task(attempt.root.task_id())
        .unwrap()
        .status()
        .unwrap()
        .clone();
    let mut outcome = attempt.logical.original_internal_outcome();
    let consume = attempt
        .logical
        .run_registered_internal_consumer(consumer, activation);
    let deliver = attempt.logical.deliver_original_internal_end(
        attempt.root,
        status,
        RootResultEnd {
            sequence: NonZeroU64::MIN,
            output_rows: 0,
        },
    );
    tokio::pin!(consume, deliver);
    let mut consumed: Option<Result<(), QueryExecutionError>> = None;
    let mut delivered = false;
    let token = watchdog("original actor accepts fixed Profile proof", async {
        loop {
            if let Some(selected) = outcome.borrow().clone() {
                break selected.expect("the fixed Profile computation succeeds");
            }
            tokio::select! {
                biased;
                changed = outcome.changed() => changed.expect("original actor outcome remains live"),
                result = consume.as_mut(), if consumed.is_none() => {
                    consumed = Some(result);
                }
                result = deliver.as_mut(), if !delivered => {
                    result.expect("the original End is actually consumed");
                    delivered = true;
                }
            }
        }
    })
    .await;
    assert!(
        consumed.is_none(),
        "the actor cut must be receivable before the consumer resumes after source.accept"
    );
    if let Some(prior_cut) = prior_cut {
        assert!(output.take_ready(prior_cut).is_err());
    }
    let received = output
        .take_ready(token)
        .expect("staged custody and the actual actor cut transfer atomically");
    assert!(output.take_ready(token).is_err(), "one transfer only");
    if consumed.is_none() {
        watchdog("original consumer actual future exit", consume.as_mut())
            .await
            .unwrap();
    }
    if !delivered {
        watchdog("original End delivery receipt", deliver.as_mut())
            .await
            .unwrap();
    }
    received
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_profile_reaches_real_ready_while_original_final_info_is_held() {
    let probe = Arc::new(FinalInfoProbe::default());
    let (release, gate) = tokio::sync::oneshot::channel();
    let transport: Arc<dyn TaskResultTransport> = Arc::new(HeldOriginalFinalInfo {
        gate: Mutex::new(Some(gate)),
        probe: Arc::clone(&probe),
    });
    let (mut attempt, resources, activation) = watchdog(
        "build original actor and settle original Establish",
        OriginalAttempt::new_with_transport(1, transport),
    )
    .await;
    let (description, plan) = internal_count_description();
    let (consumer, offers, mut output) = profile_computation_consumer(&description, plan).unwrap();
    let mut producer = offers.reserve(attempt.root.query_execution_id()).unwrap();
    producer
        .stage(
            attempt.root.query_execution_id(),
            attempt.root,
            plan,
            resources,
        )
        .unwrap();
    attempt.bind_original_drive(&mut producer);
    let expected = attempt
        .round
        .execution()
        .graph()
        .tasks()
        .map(|task| task.identity())
        .collect::<Vec<_>>();
    // Start the actual original diagnostic request before proposing Ready.
    for _ in 0..=expected.len() {
        std::future::poll_fn(|cx| {
            attempt.diagnostics.poll_observe(&attempt.round, cx);
            Poll::Ready(())
        })
        .await;
    }
    assert!(probe.entered.load(Ordering::SeqCst));
    assert!(!probe.completed.load(Ordering::SeqCst));
    assert!(!probe.exited.load(Ordering::SeqCst));
    let mut received =
        run_fixed_consumer_to_real_cut(&attempt, consumer, activation, &mut output, None).await;
    let token = received.ready_token();
    assert_eq!(received.output_rows(), 0);
    assert_eq!(received.expected_tasks(), expected.as_slice());
    assert!(Arc::ptr_eq(
        received.original_credentials().unwrap(),
        &attempt.native_credentials
    ));
    assert!(received.current_diagnostics().unwrap().is_none());
    assert!(!probe.completed.load(Ordering::SeqCst));
    assert!(!probe.exited.load(Ordering::SeqCst));
    assert!(matches!(
        attempt.logical.actor_snapshot().await.unwrap().conclusion,
        Some(novarocks_query_application::coordination::LogicalConclusion::ComputationReady)
    ));
    attempt
        .diagnostics
        .begin_profile_observation(token)
        .unwrap();
    attempt.clock.advance(Duration::from_secs(1));
    std::future::poll_fn(|cx| {
        attempt.diagnostics.poll_profile_observation(
            &attempt.round,
            attempt.clock.now(),
            false,
            cx,
        );
        Poll::Ready(())
    })
    .await;
    let original = received.current_diagnostics().unwrap().unwrap();
    assert_eq!(original.ready, token);
    assert_eq!(
        original.closure,
        ProfileDiagnosticClosure::ObservationDeadline
    );
    assert_eq!(original.tasks.len(), expected.len());
    assert!(original.tasks.iter().any(|(_, fact)| matches!(
        fact,
        ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)
    )));
    let observed = watchdog(
        "read the original immutable diagnostic snapshot",
        received.wait_diagnostics(),
    )
    .await
    .unwrap();
    assert!(Arc::ptr_eq(&original, &observed));
    assert!(!attempt.diagnostics.is_closed());
    assert!(!probe.exited.load(Ordering::SeqCst));
    release.send(()).unwrap();
    watchdog("original held diagnostic RPC actually exits", async {
        while !attempt.diagnostics.is_closed() {
            std::future::poll_fn(|cx| {
                attempt.diagnostics.poll_profile_observation(
                    &attempt.round,
                    attempt.clock.now(),
                    false,
                    cx,
                );
                Poll::Ready(())
            })
            .await;
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(probe.completed.load(Ordering::SeqCst));
    assert!(probe.exited.load(Ordering::SeqCst));
    producer.retire_unclaimed();
    drop((producer, received, original, observed, output));
    watchdog("original TaskRound and actor drain", attempt.finish()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_profile_deadline_freezes_pending_before_the_original_rpc_returns_on_that_poll() {
    let probe = Arc::new(FinalInfoProbe::default());
    let (release, gate) = tokio::sync::oneshot::channel();
    let transport: Arc<dyn TaskResultTransport> = Arc::new(HeldOriginalFinalInfo {
        gate: Mutex::new(Some(gate)),
        probe: Arc::clone(&probe),
    });
    let (mut attempt, resources, activation) = watchdog(
        "build original actor for the deadline boundary",
        OriginalAttempt::new_with_transport(1, transport),
    )
    .await;
    let (description, plan) = internal_count_description();
    let (consumer, offers, mut output) = profile_computation_consumer(&description, plan).unwrap();
    let mut producer = offers.reserve(attempt.root.query_execution_id()).unwrap();
    producer
        .stage(
            attempt.root.query_execution_id(),
            attempt.root,
            plan,
            resources,
        )
        .unwrap();
    attempt.bind_original_drive(&mut producer);
    let pending_task = attempt.round.execution().accepted_terminal_tasks()[0];
    std::future::poll_fn(|cx| {
        attempt.diagnostics.poll_observe(&attempt.round, cx);
        Poll::Ready(())
    })
    .await;
    let initially_pending = probe.entered.load(Ordering::SeqCst)
        && !probe.completed.load(Ordering::SeqCst)
        && !probe.exited.load(Ordering::SeqCst)
        && !attempt.diagnostics.is_closed();
    let received =
        run_fixed_consumer_to_real_cut(&attempt, consumer, activation, &mut output, None).await;
    let token = received.ready_token();
    attempt
        .diagnostics
        .begin_profile_observation(token)
        .unwrap();
    // Sending only readies this original F; no other task polls it. Its first
    // return is deliberately the same poll that reaches the observation cut.
    let released = release.send(()).is_ok();
    let not_polled_after_release =
        !probe.completed.load(Ordering::SeqCst) && !probe.exited.load(Ordering::SeqCst);
    let remaining = token
        .diagnostic_deadline()
        .saturating_duration_since(attempt.clock.now());
    attempt.clock.advance(remaining);
    let at_exact_deadline = attempt.clock.now() == token.diagnostic_deadline();
    std::future::poll_fn(|cx| {
        attempt.diagnostics.poll_profile_observation(
            &attempt.round,
            attempt.clock.now(),
            false,
            cx,
        );
        Poll::Ready(())
    })
    .await;
    let original = received.current_diagnostics().unwrap();
    let original_read_exited = probe.completed.load(Ordering::SeqCst)
        && probe.exited.load(Ordering::SeqCst)
        && attempt.diagnostics.is_closed();
    let cutoff_closed = attempt.diagnostics.final_info_closed();
    let final_deadline = attempt.diagnostics.observation_deadline();
    // A second actual poll cannot replace the same immutable cut with the
    // original transport's now-received Unavailable(Gone) answer either.
    std::future::poll_fn(|cx| {
        attempt.diagnostics.poll_profile_observation(
            &attempt.round,
            attempt.clock.now(),
            false,
            cx,
        );
        Poll::Ready(())
    })
    .await;
    let late = received.current_diagnostics().unwrap();
    producer.retire_unclaimed();
    drop((producer, received, output));
    watchdog(
        "boundary original TaskRound and actor drain",
        attempt.finish(),
    )
    .await;

    assert!(initially_pending);
    assert!(released);
    assert!(not_polled_after_release);
    assert!(at_exact_deadline);
    assert!(original_read_exited);
    assert!(cutoff_closed);
    assert_eq!(final_deadline, None);
    let original = original.expect("the exact Ready deadline freezes the original snapshot");
    let late = late.expect("the original immutable cut remains visible");
    assert!(Arc::ptr_eq(&original, &late));
    assert_eq!(original.ready, token);
    assert_eq!(
        original.closure,
        ProfileDiagnosticClosure::ObservationDeadline
    );
    assert!(original.tasks.iter().any(|(task, fact)| {
        *task == pending_task
            && matches!(
                fact,
                ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)
            )
    }));
    assert!(!original.tasks.iter().any(|(task, fact)| {
        *task == pending_task
            && matches!(
                fact,
                ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::Unavailable(
                    OperationOutcome::Gone
                ))
            )
    }));
}

#[tokio::test(flavor = "current_thread")]
async fn actual_foreign_old_claim_and_cut_cannot_consume_new_profile_custody() {
    // These are independent actual actors, as in the original handoff tests.
    // This protects capability/cut isolation, not the QA recovery algorithm.
    let (mut prior, prior_resources, prior_activation) =
        watchdog("prior actor original Establish", OriginalAttempt::new(1)).await;
    drop(prior_resources);
    prior
        .round
        .bind_protocol_drive(&prior.logical.native_attempt_drive())
        .unwrap();
    let prior_cut = watchdog(
        "prior original actor cut",
        prior.accept_original_end(prior_activation),
    )
    .await;
    let (stale, stale_resources, stale_activation) =
        watchdog("stale actor original Establish", OriginalAttempt::new(1)).await;
    drop(stale_resources);
    let (mut next, resources, activation) =
        watchdog("next actor original Establish", OriginalAttempt::new(2)).await;
    let (description, plan) = internal_count_description();
    let (consumer, offers, mut output) = profile_computation_consumer(&description, plan).unwrap();
    let mut producer = offers.reserve(next.root.query_execution_id()).unwrap();
    producer
        .stage(next.root.query_execution_id(), next.root, plan, resources)
        .unwrap();
    next.bind_original_drive(&mut producer);
    let rejected = watchdog(
        "stale actual source cannot claim the next offer",
        stale
            .logical
            .run_registered_internal_consumer(Arc::clone(&consumer), stale_activation),
    )
    .await;
    assert!(rejected.is_err());
    // The failed foreign claim must not poison the receiving holder's first
    // actor fence. A real next source still reaches its actual cut.
    let received =
        run_fixed_consumer_to_real_cut(&next, consumer, activation, &mut output, Some(prior_cut))
            .await;
    assert_eq!(received.ready_token().root(), next.root);
    assert!(
        received
            .expected_tasks()
            .iter()
            .all(|task| task.query_execution_id() == next.root.query_execution_id())
    );
    assert!(Arc::ptr_eq(
        received.original_credentials().unwrap(),
        &next.native_credentials
    ));
    producer.retire_unclaimed();
    drop((producer, received, output));
    watchdog("prior actual actor drain", prior.finish()).await;
    watchdog("stale actual actor drain", stale.finish()).await;
    watchdog("next actual actor drain", next.finish()).await;
}

// The qualification fixture owns the successor's actual admission ACKs from
// its original TaskRound. It supplies neither an actor nor a Ready token.
#[derive(Debug)]
struct ProfileRecoveryFacts {
    failed: QueryContextRef,
    successor: QueryContextRef,
    admissions:
        Mutex<Option<Box<[novarocks_query_application::coordination::QualifiedWorkerAdmission]>>>,
    submitted: std::sync::atomic::AtomicUsize,
    isolation_exits: Arc<std::sync::atomic::AtomicUsize>,
}
#[derive(Debug)]
struct ProfileRecoveryPort {
    capacity: tokio::sync::watch::Sender<u64>,
    facts: Arc<ProfileRecoveryFacts>,
}
#[derive(Debug)]
struct ProfileRecoveryEffect(Arc<ProfileRecoveryFacts>);
impl novarocks_query_application::coordination::ReplacementQualificationEffectPort
    for ProfileRecoveryPort
{
    fn subscribe_capacity(&self) -> tokio::sync::watch::Receiver<u64> {
        self.capacity.subscribe()
    }
    fn try_reserve(
        &self,
        _: &novarocks_query_application::coordination::ReplacementQualificationRequest,
    ) -> novarocks_query_application::coordination::ReplacementQualificationEffectAdmission {
        novarocks_query_application::coordination::ReplacementQualificationEffectAdmission::Admitted(
            Box::new(ProfileRecoveryEffect(Arc::clone(&self.facts))),
        )
    }
}
impl novarocks_query_application::coordination::ReplacementQualificationEffectReservation
    for ProfileRecoveryEffect
{
    fn submit(
        self: Box<Self>,
        submission: novarocks_query_application::coordination::ReplacementQualificationEffectSubmission,
    ) {
        use novarocks_query_application::coordination as qa;
        let request = submission.request();
        if request.failed_contexts() != [self.0.failed]
            || request.replacement_contexts() != [self.0.successor]
        {
            submission.refused(qa::ReplacementQualificationFailure::InvalidReservation);
            return;
        }
        let admissions = self.0.admissions.lock().unwrap().take();
        let Some(admissions) = admissions else {
            submission.refused(qa::ReplacementQualificationFailure::InvalidReservation);
            return;
        };
        self.0.submitted.fetch_add(1, Ordering::SeqCst);
        let reservation = ProfileRecoveryIsolation {
            identity: request.identity(),
            failed: self.0.failed,
            admissions,
            exits: Arc::clone(&self.0.isolation_exits),
        };
        match qa::QualifiedReplacementReservation::try_new(request, Box::new(reservation)) {
            Ok(qualified) => submission.qualified(qualified).unwrap(),
            Err(error) => submission.refused(error),
        }
    }
}
#[derive(Debug)]
struct ProfileRecoveryIsolation {
    identity: novarocks_query_application::coordination::ReplacementQualificationIdentity,
    failed: QueryContextRef,
    admissions: Box<[novarocks_query_application::coordination::QualifiedWorkerAdmission]>,
    exits: Arc<std::sync::atomic::AtomicUsize>,
}
impl novarocks_query_application::coordination::AttemptIsolationReservation
    for ProfileRecoveryIsolation
{
    fn identity(
        &self,
    ) -> novarocks_query_application::coordination::ReplacementQualificationIdentity {
        self.identity
    }
    fn topology_revision(&self) -> u64 {
        1
    }
    fn admissions(&self) -> &[novarocks_query_application::coordination::QualifiedWorkerAdmission] {
        &self.admissions
    }
    fn validate_binding(
        &self,
        failed: &[QueryContextRef],
    ) -> Result<(), novarocks_query_application::coordination::ReplacementQualificationFailure>
    {
        (failed == [self.failed]).then_some(()).ok_or(novarocks_query_application::coordination::ReplacementQualificationFailure::InvalidReservation)
    }
    fn activate(
        self: Box<Self>,
    ) -> Result<
        Box<dyn novarocks_query_application::coordination::ActiveAttemptIsolationOwner>,
        novarocks_query_application::coordination::AttemptIsolationActivationFailure,
    > {
        Ok(Box::new(ProfileRecoveryActiveIsolation {
            execution: self.identity.replacement(),
            exits: Arc::clone(&self.exits),
        }))
    }
    fn abandon(self: Box<Self>) {
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct ProfileRecoveryActiveIsolation {
    execution: QueryExecutionId,
    exits: Arc<std::sync::atomic::AtomicUsize>,
}
impl novarocks_query_application::coordination::ActiveAttemptIsolationOwner
    for ProfileRecoveryActiveIsolation
{
    fn execution(&self) -> QueryExecutionId {
        self.execution
    }
    fn finish(self: Box<Self>) {
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
    fn abandon(self: Box<Self>) {
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}
struct ProfileRecoveryFinalInfo(
    Mutex<std::collections::BTreeMap<TaskIdentity, novarocks_execution_contract::FinalTaskInfo>>,
);
impl TaskResultTransport for ProfileRecoveryFinalInfo {
    fn final_task_info(
        &self,
        task: TaskIdentity,
    ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
        let info = self.0.lock().unwrap().remove(&task);
        Box::pin(async move {
            info.map(|info| FinalTaskInfoRead::Available(Box::new(info)))
                .ok_or_else(|| "the original successor final info was read twice".to_owned())
        })
    }
    fn dynamic_filters(
        &self,
        _: TaskIdentity,
        _: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        panic!("Profile recovery diagnostics must not fetch dynamic filters")
    }
}

#[tokio::test(flavor = "current_thread")]
async fn one_logical_profile_owner_replaces_before_visibility_and_receives_only_successor_diagnostics()
 {
    use crate::query_execution::internal_consumer::test_fixture::internal_profile_recovery_description;
    use novarocks_query_application::coordination as qa;
    use std::num::NonZeroU32;
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 10,
            per_scope_bytes: 1 << 18,
        },
    )
    .unwrap();
    let result_capacity = control
        .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let (description, plan) = watchdog(
        "complete the original SQL Profile declaration and CountOnly carrier",
        internal_profile_recovery_description(&scope),
    )
    .await;
    assert_eq!(
        description.kind(),
        novarocks_query_application::api::QueryExecutionKind::Read
    );
    assert_eq!(
        description.effect(),
        novarocks_coordination_contract::ExecutionEffect::None
    );
    assert_eq!(
        description.recovery(),
        novarocks_coordination_contract::RecoveryMode::RestartAttemptBeforeVisibility
    );
    let first_execution =
        QueryExecutionId::new(QueryId::new(0x92, 0x37), AttemptId::new(1).unwrap()).unwrap();
    let next_execution =
        QueryExecutionId::new(first_execution.query_id(), AttemptId::new(2).unwrap()).unwrap();
    let (mut first_round, first_acks, first_release, actor_clock, first_protocol, first_establish) =
        crate::task_execution::tests::manifest_normal_release_ack_with_protocol_fixture(
            first_execution,
            plan,
        );
    let (next_round, next_acks, next_release, next_clock, next_protocol, next_facts) =
        crate::task_execution::tests::manifest_normal_release_ack_with_protocol_fixture(
            next_execution,
            plan,
        );
    let first_root = first_round.root_task();
    let next_root = next_round.root_task();
    let first_context = *first_round.execution().graph().contexts().next().unwrap();
    let next_context = *next_round.execution().graph().contexts().next().unwrap();
    let mut admission = None;
    let mut establish = None;
    let mut qualified = Vec::new();
    for (intent, ack) in next_facts {
        assert_eq!(intent.operation_id(), ack.operation_id());
        match intent {
            OperationIntent::AcquireQueryContextAdmissionTicket(request) => {
                let AckPayload::AdmissionTicket(ticket) = ack.payload() else {
                    panic!("original replacement admission ACK")
                };
                qualified.push(
                    qa::QualifiedWorkerAdmission::try_new(next_execution, request, *ticket)
                        .unwrap(),
                );
                assert!(admission.replace(request).is_none());
            }
            OperationIntent::EstablishQueryContext(request) => {
                assert!(
                    establish
                        .replace((request, ack.operation_id(), ack.worker_outcome().unwrap()))
                        .is_none()
                );
            }
            _ => panic!("unrelated original successor fixture fact"),
        }
    }
    let isolation_exits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let facts = Arc::new(ProfileRecoveryFacts {
        failed: first_context,
        successor: next_context,
        admissions: Mutex::new(Some(qualified.into_boxed_slice())),
        submitted: std::sync::atomic::AtomicUsize::new(0),
        isolation_exits: Arc::clone(&isolation_exits),
    });
    let (capacity, _) = tokio::sync::watch::channel(1);
    let qualification = Arc::new(ProfileRecoveryPort {
        capacity,
        facts: Arc::clone(&facts),
    });
    let stage = scope
        .try_acquire(novarocks_workload_control::Stage::Execution)
        .unwrap();
    let (abort, abort_intake) =
        crate::task_execution::abort_effect::NativeAbortEffectAdapter::bounded(
            NonZeroUsize::new(2).unwrap(),
            Arc::new(CountingWake::default()),
        );
    first_round.install_abort_effect_intake(abort_intake);
    let config = LogicalExecutionTestHarness::internal_pre_visibility_recovery_config(
        first_execution,
        NonZeroUsize::new(8).unwrap(),
        vec![first_context],
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        NonZeroU32::new(2).unwrap(),
        qualification,
        root.owner,
        stage,
        Duration::from_secs(30),
    )
    .unwrap()
    .with_abort_query_context_effect_port(
        abort as Arc<dyn AbortQueryContextEffectPort>,
        NonZeroUsize::new(2).unwrap(),
    );
    let mut logical = LogicalExecutionTestHarness::install_internal(
        tokio::runtime::Handle::current(),
        config,
        &description,
        NonZeroUsize::MIN,
        Arc::clone(&actor_clock) as Arc<dyn MonotonicClock>,
        ComputationObservationBudgets::new(Duration::from_secs(1), Duration::from_secs(5)),
        first_execution,
        vec![first_context],
    )
    .unwrap();
    let first_activation = watchdog(
        "activate original Profile attempt",
        logical.activate_initial(),
    )
    .await
    .unwrap();
    watchdog(
        "settle first original Establish",
        crate::task_execution::tests::settle_original_actor_establish_facts(
            &logical,
            first_establish,
        ),
    )
    .await;
    let old_probe = Arc::new(FinalInfoProbe::default());
    let (release_old_read, old_gate) = tokio::sync::oneshot::channel();
    let transport: Arc<dyn TaskResultTransport> = Arc::new(HeldOriginalFinalInfo {
        gate: Mutex::new(Some(old_gate)),
        probe: Arc::clone(&old_probe),
    });
    let mut diagnostics = TaskFinalInfoCollector::new(
        transport,
        true,
        &first_round,
        Arc::new(CountingWake::default()),
    );
    let first_observer = diagnostics.enable_profile_observation().unwrap();
    let credentials = original_credentials(first_execution);
    let resources = AttemptHandoffResources::new(
        first_protocol,
        Some(Arc::clone(&credentials)),
        Some(first_observer),
    );
    let mut attempt = OriginalAttempt {
        round: first_round,
        acks: first_acks,
        release: Some(first_release),
        pending_upstream: None,
        scope,
        cancellation_requester: None,
        capacity: result_capacity,
        clock: actor_clock,
        plan,
        root: first_root,
        context: first_context,
        logical,
        native_credentials: credentials,
        diagnostics,
        _control: control,
        business: Some(root.business),
    };
    let (consumer, offers, mut output) = profile_computation_consumer(&description, plan).unwrap();
    let mut first_producer = offers.reserve(first_execution).unwrap();
    first_producer
        .stage(first_execution, first_root, plan, resources)
        .unwrap();
    attempt.bind_original_drive(&mut first_producer);
    std::future::poll_fn(|cx| {
        attempt.diagnostics.poll_observe(&attempt.round, cx);
        Poll::Ready(())
    })
    .await;
    let before = attempt.logical.actor_snapshot().await.unwrap();
    let old_read_pending =
        old_probe.entered.load(Ordering::SeqCst) && !old_probe.exited.load(Ordering::SeqCst);
    let next_activation = watchdog(
        "real same-owner pre-visibility replacement",
        attempt.logical.replace_original_running_attempt(
            novarocks_coordination_contract::AttemptFailureClass::RecoverableInfrastructure,
            next_execution,
            vec![next_context],
        ),
    )
    .await
    .unwrap();
    let (establish, operation, verdict) = establish.unwrap();
    watchdog(
        "settle qualified original successor Establish",
        attempt.logical.settle_original_qualified_establish(
            admission.unwrap(),
            establish,
            operation,
            verdict,
        ),
    )
    .await
    .unwrap();

    // Only the original per-attempt carrier fields change. The logical harness,
    // registry, WorkOwner, result capacity and receiving holder are the same.
    let mut old_round = std::mem::replace(&mut attempt.round, next_round);
    let old_acks = std::mem::replace(&mut attempt.acks, next_acks);
    let old_release = attempt.release.replace(next_release).unwrap();
    let old_context = std::mem::replace(&mut attempt.context, next_context);
    attempt.root = next_root;
    attempt.clock = next_clock;
    let next_credentials = original_credentials(next_execution);
    let old_credentials = std::mem::replace(
        &mut attempt.native_credentials,
        Arc::clone(&next_credentials),
    );
    let expected = attempt
        .round
        .execution()
        .graph()
        .tasks()
        .map(|task| task.identity())
        .collect::<Vec<_>>();
    let info = expected
        .iter()
        .map(|&identity| {
            let status = attempt
                .round
                .execution()
                .task(identity.task_id())
                .unwrap()
                .status()
                .unwrap()
                .clone();
            (
                identity,
                novarocks_execution_contract::FinalTaskInfo::try_new(
                    identity,
                    status,
                    Vec::new(),
                    false,
                )
                .unwrap(),
            )
        })
        .collect();
    let mut next_diagnostics = TaskFinalInfoCollector::new(
        Arc::new(ProfileRecoveryFinalInfo(Mutex::new(info))),
        true,
        &attempt.round,
        Arc::new(CountingWake::default()),
    );
    let next_observer = next_diagnostics.enable_profile_observation().unwrap();
    let mut old_diagnostics = std::mem::replace(&mut attempt.diagnostics, next_diagnostics);
    let resources = AttemptHandoffResources::new(
        next_protocol,
        Some(Arc::clone(&next_credentials)),
        Some(next_observer),
    );
    let mut next_producer = offers.reserve(next_execution).unwrap();
    next_producer
        .stage(next_execution, next_root, plan, resources)
        .unwrap();
    attempt.bind_original_drive(&mut next_producer);
    let activation = attempt
        .logical
        .internal_consumer_activation(next_root)
        .await
        .unwrap();
    let mut received =
        run_fixed_consumer_to_real_cut(&attempt, consumer, activation, &mut output, None).await;
    let ready = received.ready_token();
    let has_only_next_provenance = received.expected_tasks() == expected.as_slice();
    let has_original_next_credentials =
        Arc::ptr_eq(received.original_credentials().unwrap(), &next_credentials)
            && !Arc::ptr_eq(received.original_credentials().unwrap(), &old_credentials);
    let OperationIntent::ReleaseQueryContext(request) = attempt.release.as_ref().unwrap() else {
        unreachable!()
    };
    attempt.acks.publish(OperationAcknowledgement::new(
        request.envelope().operation_id(),
        OperationKind::ReleaseQueryContext,
        OperationOutcome::Accepted,
        AckPayload::Release {
            receipt: QueryContextReceipt::new(next_context, QueryContextState::TerminalRetained),
            outcome: ReleaseOutcome::Released,
            runtime_filter: None,
        },
    ));
    attempt.round.turn().unwrap();
    attempt.publish_original_snapshot(ready).await;
    let snapshot = watchdog(
        "same successful Profile diagnostic snapshot",
        received.wait_diagnostics(),
    )
    .await
    .unwrap();
    let released_old = release_old_read.send(()).is_ok();
    std::future::poll_fn(|cx| {
        old_diagnostics.poll_observe(&old_round, cx);
        Poll::Ready(())
    })
    .await;
    let old_read_exited = old_probe.completed.load(Ordering::SeqCst)
        && old_probe.exited.load(Ordering::SeqCst)
        && old_diagnostics.is_closed();
    let after_old_read = received.current_diagnostics().unwrap().unwrap();

    // Drive the original failed round and its exact context to real fixture
    // closure before retiring the one logical registry/actor.
    let OperationIntent::ReleaseQueryContext(request) = old_release else {
        unreachable!()
    };
    old_acks.publish(OperationAcknowledgement::new(
        request.envelope().operation_id(),
        OperationKind::ReleaseQueryContext,
        OperationOutcome::Accepted,
        AckPayload::Release {
            receipt: QueryContextReceipt::new(old_context, QueryContextState::TerminalRetained),
            outcome: ReleaseOutcome::Released,
            runtime_filter: None,
        },
    ));
    old_round.turn().unwrap();
    old_round
        .execution_mut()
        .apply_covered_entries(vec![ObservationIntakeEntry::Frame(Box::new(
            ObservationFrame::Covered {
                context: old_context,
                generation: 1,
                event: CoveredStatusStreamEvent {
                    fact: CoveredStatusStreamFact::ContextConvergence(
                        QueryContextConvergenceReceipt::new(
                            old_context,
                            QueryContextConvergenceVersion::FIRST,
                            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
                        ),
                    ),
                    source_revision: None,
                },
            },
        ))])
        .unwrap();
    let old_round_closed = old_round.attempt_drained()
        && old_round.pumps_closed()
        && old_round.execution().worker_contexts_stopped();
    attempt
        .logical
        .observe_worker_stopped_and_context_fenced(old_context)
        .await
        .unwrap();
    first_producer.retire_unclaimed();
    next_producer.retire_unclaimed();
    drop((
        first_producer,
        next_producer,
        received,
        output,
        old_diagnostics,
        old_round,
        old_credentials,
        next_credentials,
    ));
    watchdog(
        "both original attempts and one actor actual retirement",
        attempt.finish(),
    )
    .await;

    assert!(before.conclusion.is_none());
    assert!(!before.output_visible);
    assert!(old_read_pending);
    assert_eq!(first_activation.actor(), next_activation.actor());
    assert_eq!(
        first_activation.execution().query_id(),
        next_activation.execution().query_id()
    );
    assert!(first_activation.generation() < next_activation.generation());
    assert_eq!(ready.activation(), next_activation);
    assert_eq!(ready.root(), next_root);
    assert!(has_only_next_provenance);
    assert!(has_original_next_credentials);
    assert_eq!(facts.submitted.load(Ordering::SeqCst), 1);
    assert_eq!(isolation_exits.load(Ordering::SeqCst), 1);
    assert!(released_old && old_read_exited && old_round_closed);
    assert!(Arc::ptr_eq(&snapshot, &after_old_read));
    assert!(snapshot.complete());
    assert_eq!(
        snapshot
            .tasks
            .iter()
            .map(|(task, _)| *task)
            .collect::<Vec<_>>(),
        expected
    );
    assert!(snapshot.tasks.iter().all(|(task, fact)| task.query_execution_id() == next_execution && matches!(fact, ProfileTaskDiagnostic::Available(info) if info.final_status().identity() == *task)));
}
