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

//! Real immutable physical-request validation and original typed failures.
use super::native_all_frames::{consume_actual_inputs, retire_actual_work, start_generation};
use super::*;
use crate::{
    native::{
        data_runtime::FrontendDataRuntime, task_transport::tests::Loopback, transport::Client,
    },
    query_execution::native_execution_adapter::source_cohort::tests as wc,
    task_execution::host::{
        custody::tests::original_transport_join_id,
        subscriptions::{SubscriptionEnd, SubscriptionSource},
    },
};
use novarocks_attempt_protocol::{
    ids::IdSeed,
    input::{ObservationFailureKind, ReceivedObservation},
    observation::{
        SubscriptionRequest,
        controller::{
            CoveredSubscription, ObservationDisposition, SubscriptionDirective, SubscriptionStatus,
        },
    },
};
use novarocks_control_runtime::history::HistoryCapture;
use novarocks_execution_contract::{
    TaskConvergenceCursor, TaskConvergenceVersion, TaskOutputFacts, TaskState, TaskStatus,
    TaskStatusCursor, TaskStatusVersion,
};
use novarocks_failure_contract::FailureProjection;
use novarocks_proto_codec::FieldPath;
use novarocks_query_application::execution_error::ExecutionFailureSite;
use novarocks_task_codec::operation::{
    CoveredStatusStreamEvent, CoveredStatusStreamFact, decode_covered_subscribe_task_status,
    encode_covered_status_event,
};
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{future::poll_fn, time::Duration};
use tokio::runtime::Handle;

#[derive(Clone, Copy)]
enum Case {
    StatusUnchanged,
    ConvergenceUnchanged,
    ForeignProcess,
    ForeignCore,
    ForeignExecution,
}
async fn original_wire_failure(case: Case) {
    let capture: BatchCapture = Arc::new(Mutex::new(Vec::with_capacity(8)));
    let original_key = key();
    // The actual Source owns generation one in the Core-bound case. Do not
    // pre-register or model-open that generation in the neutral input fixture.
    let register_initial_stream = !matches!(case, Case::ForeignCore);
    let f = Fixture::make_with_initial_stream(
        4,
        8,
        register_initial_stream,
        Some(liveness::Trace {
            key: original_key,
            history: HistoryCapture::new(4096),
            capture: liveness::Capture::default(),
        }),
        Some(capture.clone()),
        register_initial_stream,
    );
    let mut peer = Loopback::start().await;
    peer.expect_covered_frames_then_hold(Vec::new());
    let task = f.input.root_binding().root();
    let participation = f.key.participation;
    let (control, mut driver, capacity) = wc::fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    wc::bind(&scope, &mut driver);
    let grant = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let cohort = wc::cohort(&scope, 1, &mut driver);
    let source =
        SubscriptionSource::from_cohort(participation, &scope, &cohort, f.input.clone()).unwrap();
    let source_scope = source.scope().unwrap();
    let generation = NonZeroU64::new(if matches!(case, Case::ForeignCore) {
        1
    } else {
        3
    })
    .unwrap();
    let mut policy = CoveredSubscription::new(participation, IdSeed::new(31, 1), 2).unwrap();
    policy.activate();
    assert_eq!(
        policy
            .drive(MonotonicInstant::ORIGIN, generation, None)
            .unwrap(),
        Some(SubscriptionDirective::Open { generation })
    );
    let request = Arc::new(SubscriptionRequest {
        participation,
        generation,
        status_cursors: Box::new([TaskStatusCursor::unobserved(task)]),
        task_convergence_cursors: Box::new([TaskConvergenceCursor::unobserved(task)]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([task]),
    });
    let mut native = FrontendDataRuntime::new(Handle::current());
    let client = Client::for_backend_in(
        Arc::from([peer.descriptor(participation.backend_process_id())]),
        0,
        native.clone(),
    );
    let mut actual = start_generation(&f, &source, request.clone(), client);
    let original_join = original_transport_join_id(&actual.role).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while peer.subscription_requests().len() != 1
            || peer.open_covered_subscriptions() != 1
            || f.input
                .read_state(
                    StreamKey {
                        participation,
                        generation,
                    },
                    token(8),
                    MonotonicInstant::ORIGIN,
                )
                .unwrap()
                .opened_at
                .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let physical = decode_covered_subscribe_task_status(
        &peer.subscription_requests()[0],
        FieldPath::root("immutable_original_request"),
    )
    .unwrap();
    assert_eq!(physical.participation, participation);
    assert_eq!(physical.generation, generation);
    assert_eq!(physical.status_cursors[0].current_version(), None);
    assert_eq!(physical.task_convergence_cursors[0].current_version(), None);
    let (fact, expected) = match case {
        Case::StatusUnchanged | Case::ConvergenceUnchanged => {
            let applied = Arc::new(SubscriptionRequest {
                status_cursors: Box::new([TaskStatusCursor::at(task, TaskStatusVersion::FIRST)]),
                task_convergence_cursors: Box::new([TaskConvergenceCursor::at(
                    task,
                    TaskConvergenceVersion::FIRST,
                )]),
                ..(*request).clone()
            });
            source.adjust_read_window(applied.clone()).unwrap();
            assert!(Arc::ptr_eq(
                &source.applied_request(generation).unwrap(),
                &applied
            ));
            assert_eq!(
                peer.subscription_requests().len(),
                1,
                "applied update cannot reopen the actual RPC"
            );
            (
                if matches!(case, Case::StatusUnchanged) {
                    CoveredStatusStreamFact::StatusUnchanged(task)
                } else {
                    CoveredStatusStreamFact::TaskConvergenceUnchanged(task)
                },
                ObservationFailureKind::ProtocolViolation,
            )
        }
        Case::ForeignProcess | Case::ForeignCore => {
            let foreign = novarocks_execution_contract::TaskIdentity::new(
                task.query_execution_id(),
                task.stage_id(),
                task.task_id(),
                BackendProcessId::new_v7(),
            );
            (
                CoveredStatusStreamFact::Status(
                    TaskStatus::try_new(
                        foreign,
                        TaskStatusVersion::FIRST,
                        TaskState::Running,
                        None,
                        TaskOutputFacts::default(),
                    )
                    .unwrap(),
                ),
                ObservationFailureKind::ProcessMismatch,
            )
        }
        Case::ForeignExecution => {
            let foreign_execution = QueryExecutionId::new(
                task.query_execution_id().query_id(),
                AttemptId::new(2).unwrap(),
            )
            .unwrap();
            let foreign = novarocks_execution_contract::TaskIdentity::new(
                foreign_execution,
                task.stage_id(),
                task.task_id(),
                task.backend_process_id(),
            );
            (
                CoveredStatusStreamFact::Status(
                    TaskStatus::try_new(
                        foreign,
                        TaskStatusVersion::FIRST,
                        TaskState::Running,
                        None,
                        TaskOutputFacts::default(),
                    )
                    .unwrap(),
                ),
                ObservationFailureKind::QueryMismatch,
            )
        }
    };
    peer.publish_held_covered_event(
        encode_covered_status_event(&CoveredStatusStreamEvent {
            fact,
            source_revision: None,
        })
        .unwrap(),
    )
    .await;
    let ended = tokio::time::timeout(
        Duration::from_secs(3),
        poll_fn(|cx| actual.role.poll_shared_completion(cx)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(ended.is_none());
    assert_eq!(
        original_transport_join_id(&actual.role),
        Some(original_join)
    );
    assert_eq!(
        *actual.result.lock().unwrap(),
        Some(SubscriptionEnd::Failed)
    );
    assert!(actual.failure.is_terminal_candidate());
    let mut observed: Vec<ReceivedObservation> = Vec::new();
    consume_actual_inputs(&f, &capture, &[&actual.phase], &mut observed);
    f.drain_progress();
    let observations = observed
        .iter()
        .filter(|row| row.generation == generation)
        .collect::<Vec<_>>();
    let [row] = observations.as_slice() else {
        panic!("the original rejected frame becomes exactly one failed observation");
    };
    assert_eq!(row.participation, participation);
    assert!(row.opening.opened_at.is_some());
    let ObservationEvent::Failed(failure) = &row.event else {
        panic!("foreign or versionless entity cannot be published");
    };
    assert_eq!(failure.kind, expected);
    assert_eq!(failure.cause.original, actual.failure.reference());
    if matches!(case, Case::ForeignCore) {
        // The actual paid H2 failure value, including its SAME original raw
        // failure reference, crosses the public Core boundary without a new
        // transport classifier or synthetic replacement evidence.
        assert_original_failure_reaches_core((**row).clone());
    }
    assert_eq!(
        policy.observe((**row).clone()).unwrap(),
        ObservationDisposition::Ignore
    );
    assert_eq!(policy.status(), SubscriptionStatus::Fatal(*failure));
    assert_eq!(
        policy
            .drive(MonotonicInstant::ORIGIN, generation, None)
            .unwrap(),
        Some(SubscriptionDirective::Close { generation })
    );
    source.close_generation(generation).unwrap();
    // This positive input follows the SAME actual body/captures/Join receipt above.
    policy.observe_actual_exit(generation).unwrap();
    assert_eq!(
        policy
            .drive(MonotonicInstant::ORIGIN, generation, None)
            .unwrap(),
        None
    );
    assert_eq!(policy.next_deadline(MonotonicInstant::ORIGIN), None);
    assert_eq!(
        failure.cause.site,
        novarocks_attempt_protocol::failure::AttemptFailureSite::Participation(participation)
    );
    let projected = actual.failure.project(failure.cause.original).unwrap();
    assert_eq!(
        projected.site(),
        match case {
            Case::ForeignProcess | Case::ForeignExecution | Case::ForeignCore => {
                ExecutionFailureSite::Coordinator
            }
            Case::StatusUnchanged | Case::ConvergenceUnchanged => ExecutionFailureSite::Backend {
                process: participation.backend_process_id(),
            },
        }
    );
    assert_eq!(
        projected.fact(),
        match case {
            Case::ForeignProcess | Case::ForeignExecution | Case::ForeignCore =>
                novarocks_query_application::execution_error::QueryFailureReason::PreparationProtocol.failure_fact(),
            Case::StatusUnchanged | Case::ConvergenceUnchanged =>
                novarocks_query_application::execution_error::QueryFailureReason::ObservationLost.failure_fact(),
        }
    );
    assert_eq!(projected.attempt_class(), failure.cause.class);
    let original = actual.failure.take(failure.cause.original).unwrap();
    assert_eq!(original.preparation_input(), projected);
    assert!(
        actual.failure.take(failure.cause.original).is_none(),
        "one original error transfer"
    );
    assert_eq!(
        peer.subscription_requests().len(),
        1,
        "a Native wire rejection never resubscribes"
    );
    assert!(
        peer.applied_requests().is_empty() && peer.control_requests() == 0,
        "receiving an observation cannot settle a request, Abort a Task, or touch its lease"
    );
    drop(observations);
    drop((original, observed));
    retire_actual_work(&f, &actual);
    let applied = source.reap_generation(generation).unwrap();
    match case {
        Case::StatusUnchanged | Case::ConvergenceUnchanged => {
            assert_eq!(
                applied.status_cursors[0].current_version(),
                Some(TaskStatusVersion::FIRST)
            );
            assert_eq!(
                applied.task_convergence_cursors[0].current_version(),
                Some(TaskConvergenceVersion::FIRST)
            );
        }
        _ => assert!(Arc::ptr_eq(&applied, &request)),
    }
    drop(applied);
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    tokio::time::timeout(Duration::from_secs(2), source_scope.wait_released())
        .await
        .unwrap();
    drop((
        actual,
        request,
        source,
        source_scope,
        cohort,
        grant,
        capacity,
        scope,
    ));
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    native.drain_original_apply_until(deadline).await.unwrap();
    native.drain_native_outgoing_until(deadline).await.unwrap();
    assert_eq!(peer.subscription_requests().len(), 1);
    assert!(peer.settle().await);
    wc::finish(control, root, driver);
    f.finish();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covered_unchanged_requires_a_version_in_the_physical_request() {
    original_wire_failure(Case::StatusUnchanged).await;
    original_wire_failure(Case::ConvergenceUnchanged).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_status_event_from_another_process_is_fatal() {
    original_wire_failure(Case::ForeignProcess).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_status_event_from_another_query_execution_is_fatal() {
    original_wire_failure(Case::ForeignExecution).await;
}

// Pure owner decisions are checked below the runtime carrier, after the SAME
// real H2 observation and actual paid input have been consumed above. The
// numerical graph/source metadata here grants no resources or exit authority.
fn assert_original_failure_reaches_core(original: ReceivedObservation) {
    use novarocks_attempt_protocol::error::TaskExecutionError;
    use novarocks_attempt_protocol::{
        AttemptCore, Core, InputSelection,
        capacity::{QueueStage, StepContext},
        config::DispatchLane,
        effect::{AttemptEffect, EffectBody, Publication},
        input::{
            AttemptInputClaim, AttemptSourceCut, ExecutorFact, ObservationBatch, PublicationApplied,
        },
        port_output::{AttemptTerminal, AttemptToLogicalBody},
    };
    use novarocks_coordination_contract::AttemptActionId;
    struct Units(usize);
    impl StepContext for Units {
        fn charge(&mut self, units: usize) -> bool {
            if units > self.0 {
                return false;
            }
            self.0 -= units;
            true
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            panic!("a fatal original observation cannot reserve a transport queue");
        }
    }
    let ObservationEvent::Failed(failure) = &original.event else {
        panic!("only the original H2 process mismatch reaches this owner probe");
    };
    assert_eq!(failure.kind, ObservationFailureKind::ProcessMismatch);
    let expected =
        AttemptFailure::TaskProtocol(Box::new(TaskExecutionError::NativeTransport(failure.cause)));
    let mut start = crate::task_execution::host::adapter::tests::start();
    let old_root = start.plan.graph.root;
    let mut task = start.plan.graph.tasks.remove(&old_root.task_id()).unwrap();
    let identity = novarocks_execution_contract::TaskIdentity::new(
        original.participation.query_execution_id(),
        old_root.stage_id(),
        old_root.task_id(),
        original.participation.backend_process_id(),
    );
    task.identity = identity;
    task.participation = original.participation;
    start.plan.graph.root = identity;
    start.plan.graph.tasks = [(identity.task_id(), task)].into();
    start.plan.graph.stages = [(identity.stage_id(), Box::<[_]>::from([identity]))].into();
    let epoch = *start.plan.admission_epochs.values().next().unwrap();
    let positions = *start.plan.preparation_positions.values().next().unwrap();
    let material = *start.plan.establish_material.values().next().unwrap();
    start.plan.admission_epochs = [(identity.backend_process_id(), epoch)].into();
    start.plan.preparation_positions = [(identity.backend_process_id(), positions)].into();
    start.plan.establish_material = [(original.participation, material)].into();
    start.activation.identity =
        AttemptActivationIdentity::from_parts(92, identity.query_execution_id(), 1);
    start.plan.authorization_limits.activation = start.activation.identity;
    let activation = start.activation.identity;
    let sources = start.sources;
    let units = start.minimum_core_step_units().unwrap();
    let mut core = Core::new(start).unwrap();
    let generation = core
        .open_subscription_for_test(
            original.participation,
            AttemptActionId::new(NonZeroU64::MIN),
            token(99),
            &mut Units(units),
        )
        .unwrap();
    assert_eq!(generation, original.generation);
    let mut next = 1u64;
    let mut drive = |core: &mut Core, mut source, mut input, expected_failure: bool| {
        let mut effects: Vec<AttemptEffect> = Vec::new();
        let mut failed_rounds = 0;
        for _ in 0..64 {
            let id =
                novarocks_attempt_protocol::input::AttemptInputId(NonZeroU64::new(next).unwrap());
            next = next.checked_add(1).unwrap();
            let result = core
                .handle(
                    MonotonicInstant::ORIGIN,
                    AttemptInputClaim {
                        source,
                        input: id,
                        native_cut: Some(AttemptSourceCut {
                            source: sources.native,
                            tail: None,
                        }),
                        selected_cut: Some(AttemptSourceCut {
                            source: sources.observation,
                            tail: (source == sources.observation).then_some(id),
                        }),
                    },
                    input,
                    &mut Units(units),
                )
                .unwrap();
            if let Some(Err(failure)) = &result.round_result {
                assert!(expected_failure);
                assert_eq!(failure, &expected);
                failed_rounds += 1;
            }
            let ack = result
                .effect
                .as_ref()
                .and_then(|effect| match &effect.body {
                    EffectBody::Publish(Publication::ProtocolBatch(_)) => Some((
                        effect.id,
                        PublicationApplied::ProtocolBatch { published: true },
                    )),
                    _ => None,
                });
            effects.extend(result.effect);
            effects.extend(result.abort_settlement);
            match (result.selection, ack) {
                (InputSelection::Only(selected), Some((effect, applied))) => {
                    assert_eq!(selected, sources.publication);
                    source = selected;
                    input = AttemptInput::Executor(ExecutorFact::PublicationApplied {
                        effect,
                        result: Ok(applied),
                    });
                }
                (InputSelection::Only(selected), None) if selected == sources.actor => {
                    source = selected;
                    input = AttemptInput::Timer;
                }
                _ => return (failed_rounds, effects),
            }
        }
        panic!("the bounded current owner probe did not finish its original round");
    };
    let (initial_failures, _) = drive(&mut core, sources.actor, AttemptInput::Bootstrap, false);
    assert_eq!(initial_failures, 0);
    let (failed_rounds, mut effects) = drive(
        &mut core,
        sources.observation,
        AttemptInput::Executor(ExecutorFact::Observations(
            ObservationBatch::new(vec![original]).unwrap(),
        )),
        true,
    );
    for _ in 0..4 {
        if effects.iter().any(|effect| {
            matches!(&effect.body, EffectBody::Port(port)
            if matches!(&port.body, AttemptToLogicalBody::AttemptTerminal(_)))
        }) {
            break;
        }
        effects.extend(drive(&mut core, sources.actor, AttemptInput::Timer, true).1);
    }
    assert_eq!(
        failed_rounds, 1,
        "same original identity failure owns the round result"
    );
    let terminal = AttemptTerminal::Failed(expected);
    assert_eq!(
        effects
            .iter()
            .filter(|effect| matches!(&effect.body,
        EffectBody::Port(port) if port.activation == activation
            && matches!(&port.body, AttemptToLogicalBody::AttemptTerminal(got)
                if got == &terminal)))
            .count(),
        1
    );
    assert!(effects.iter().all(|effect| !matches!(
        &effect.body,
        EffectBody::OpenSubscription(_)
            | EffectBody::SendBatch { .. }
            | EffectBody::ObserveProcess { .. }
    )));
    assert!(
        core.selected_task_failure_for_test().is_none(),
        "the foreign entity never becomes an accepted Task failure"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covered_foreign_status_process_reaches_actual_round_as_identity_violation() {
    original_wire_failure(Case::ForeignCore).await;
}
