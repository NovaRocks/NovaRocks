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

//! Explicit original generations over real H2; Native owns no retry policy.
use super::*;
use crate::{
    native::{
        data_runtime::FrontendDataRuntime, task_transport::tests::Loopback, transport::Client,
    },
    query_execution::native_execution_adapter::source_cohort::tests as wc,
    task_execution::host::{
        custody::{OriginalTransportJoinObligation, tests::original_transport_join_id},
        subscriptions::{
            SubscriptionEnd, SubscriptionFailureRecord, SubscriptionPhasePublisher,
            SubscriptionSource,
        },
    },
};
use novarocks_attempt_protocol::{input::ReceivedObservation, observation::SubscriptionRequest};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_work::{PanicSink, run_future},
    history::HistoryCapture,
    obligation::WorkRetirementObserver,
    reported_work::ReportedWork,
};
use novarocks_control_runtime_core::{completion::ConsumerKind, identity::ConsumerId};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{
    ParticipationConvergenceCursor, ParticipationConvergenceReceipt, ParticipationConvergenceState,
    ParticipationConvergenceVersion, ParticipationState, QuiesceParticipationReceipt,
    TaskConvergenceCursor, TaskConvergenceReceipt, TaskConvergenceVersion, TaskOutputFacts,
    TaskState, TaskStatus, TaskStatusCursor, TaskStatusVersion,
};
use novarocks_proto_codec::FieldPath;
use novarocks_task_codec::operation::{
    CoveredCatchUpComplete, CoveredObservationBookmark, CoveredStatusStreamEvent,
    CoveredStatusStreamFact, decode_covered_subscribe_task_status, encode_covered_status_event,
};
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{future::poll_fn, time::Duration};
use tokio::runtime::Handle;

pub(super) struct OriginalGeneration {
    pub(super) role: OriginalTransportJoinObligation,
    pub(super) phase: Arc<SubscriptionPhasePublisher<Recorder>>,
    header: WorkRetirementObserver,
    work: novarocks_control_runtime_core::identity::WorkId,
    pub(super) result: Arc<Mutex<Option<SubscriptionEnd>>>,
    pub(super) failure: Arc<SubscriptionFailureRecord>,
    panic: PanicSink,
}
pub(super) fn start_generation(
    f: &Fixture,
    source: &Arc<SubscriptionSource<Recorder>>,
    request: Arc<SubscriptionRequest>,
    client: Client,
) -> OriginalGeneration {
    let phase = f.phase(request.generation.get());
    let (work, mut payer) = f
        .scope
        .reserve_next(
            Generation::new(1).unwrap(),
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Cleanup,
            WorkAdmission::Derived(f.work.id()),
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let header = work.retirement_observer();
    let work_id = work.id();
    let observer = ConsumerId::new(1).unwrap();
    let report = ReportSlot::reserve_next(
        &mut payer,
        Generation::new(1).unwrap(),
        1,
        [(observer, ConsumerKind::Observer)],
    )
    .unwrap();
    let generation = source.prepare(request.clone(), &phase).unwrap();
    let panic = PanicSink::new();
    let panic_in = panic.clone();
    let (role, exit, handoff) = OriginalTransportJoinObligation::prepare(
        ReportedWork::try_new(work, payer, report).unwrap(),
        observer,
        panic.clone(),
    )
    .unwrap_or_else(|e| panic!("same original Native generation work: {}", e.error));
    assert!(handoff.owns_exit(&exit));
    let failure = SubscriptionFailureRecord::new(
        AttemptActionId::new(request.generation),
        StreamKey {
            participation: request.participation,
            generation: request.generation,
        },
    );
    let failure_in = failure.clone();
    let result = Arc::new(Mutex::new(None));
    let result_out = result.clone();
    handoff
        .spawn(&Handle::current(), exit, move |exit, _registration| {
            let running = run_future(exit, generation.run_native(client, failure_in), panic_in);
            async move {
                let Ok(Ok(end)) = running.await else {
                    panic!("same original Native subscription generation");
                };
                *result_out.lock().unwrap() = Some(end);
            }
        })
        .unwrap();
    OriginalGeneration {
        role,
        phase,
        header,
        work: work_id,
        result,
        failure,
        panic,
    }
}
pub(super) fn consume_actual_inputs(
    f: &Fixture,
    capture: &BatchCapture,
    phases: &[&SubscriptionPhasePublisher<Recorder>],
    observed: &mut Vec<ReceivedObservation>,
) {
    loop {
        let ran = f.runtime.poll_one(Tick(1)).unwrap();
        loop {
            let next = {
                let mut original = capture.lock().unwrap();
                if original.is_empty() {
                    None
                } else {
                    Some(original.remove(0))
                }
            };
            let Some((envelope, input)) = next else {
                break;
            };
            #[derive(Clone, Copy)]
            enum Returned {
                Phase(usize),
                Frame(StreamKey, usize),
                Progress,
            }
            let returned = match &input {
                AttemptInput::Executor(ExecutorFact::Observations(batch)) => {
                    assert_eq!(
                        (envelope.source, envelope.class),
                        (ActorId::new(2).unwrap(), InputClass::Lifecycle)
                    );
                    let original = batch.clone().into_items().into_vec();
                    let count = original.len();
                    let key = StreamKey {
                        participation: original[0].participation,
                        generation: original[0].generation,
                    };
                    assert!(
                        original
                            .iter()
                            .all(|row| (row.participation, row.generation)
                                == (key.participation, key.generation))
                    );
                    observed.extend(original);
                    Returned::Frame(key, count)
                }
                AttemptInput::Executor(ExecutorFact::SubscriptionProgress(snapshot)) => {
                    assert_eq!(
                        (envelope.source, envelope.class),
                        (ActorId::new(4).unwrap(), InputClass::Lifecycle)
                    );
                    let index = phases
                        .iter()
                        .position(|phase| phase.current_input().unwrap() == Some(envelope.input))
                        .unwrap();
                    assert!(
                        snapshot
                            .opened_at
                            .is_none_or(|opened| opened >= snapshot.opening_started_at)
                    );
                    Returned::Phase(index)
                }
                AttemptInput::Executor(ExecutorFact::ReadProgress(_)) => {
                    assert_eq!(
                        (envelope.source, envelope.class),
                        (ActorId::new(3).unwrap(), InputClass::Ordinary)
                    );
                    Returned::Progress
                }
                _ => panic!("no unrelated input or synthetic Root control"),
            };
            // The original captured input and its Arc payloads remain held
            // through the real ordinary-credit checks and retirement callback.
            if let Returned::Frame(key, count) = returned {
                let bank = f.bank.state.lock().unwrap();
                assert!(
                    bank.claims
                        .values()
                        .filter(|original| **original == Some(key))
                        .count()
                        >= count,
                    "even a zero-byte EOF keeps its original ordinary position until real retirement"
                );
            }
            f.runtime.retire_input(actor(), envelope.input).unwrap();
            match returned {
                Returned::Phase(index) => phases[index].input_retired(envelope.input).unwrap(),
                Returned::Frame(_, _) => f.input.input_retired(envelope.input).unwrap(),
                Returned::Progress => {
                    f.bank
                        .progress
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .after_retire()
                        .unwrap();
                }
            }
            drop(input);
        }
        if !ran {
            break;
        }
    }
}
pub(super) fn retire_actual_work(f: &Fixture, generation: &OriginalGeneration) {
    assert!(!generation.panic.has_panic());
    assert!(generation.role.take_join_failure().is_none());
    assert!(
        generation
            .failure
            .take(generation.failure.reference())
            .is_none()
    );
    assert!(generation.role.try_retire().unwrap());
    assert!(f.scope.reap_observed_header(&generation.header).unwrap());
    assert!(f.scope.work_lifecycle(generation.work).unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covered_stream_preserves_every_frame_and_reconnects_from_applied_cursors() {
    let capture: BatchCapture = Arc::new(Mutex::new(Vec::with_capacity(8)));
    let original_key = key();
    let history = HistoryCapture::new(4096);
    let f = Fixture::make_with_captures(
        16,
        32,
        true,
        Some(liveness::Trace {
            key: original_key,
            history: history.clone(),
            capture: liveness::Capture::default(),
        }),
        Some(capture.clone()),
    );
    let mut peer = Loopback::start().await;
    let task = f.input.root_binding().root();
    let participation = f.key.participation;
    let status = TaskStatus::try_new(
        task,
        TaskStatusVersion::FIRST,
        TaskState::Running,
        None,
        TaskOutputFacts::default(),
    )
    .unwrap();
    let participation_receipt = ParticipationConvergenceReceipt::new(
        participation,
        ParticipationConvergenceVersion::FIRST,
        ParticipationConvergenceState::WorkerStoppedAndParticipationFenced,
    );
    let task_receipt = TaskConvergenceReceipt::actual_stopped(task, TaskConvergenceVersion::FIRST);
    let quiesce = QuiesceParticipationReceipt::new(
        participation,
        1,
        vec![task],
        ParticipationState::Quiescing,
    );
    let frames = vec![
        CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Status(status.clone()),
            source_revision: Some(1),
        },
        CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::TaskConvergence(task_receipt),
            source_revision: Some(2),
        },
        CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::ParticipationConvergence(participation_receipt),
            source_revision: Some(3),
        },
        CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Quiesce(quiesce.clone()),
            source_revision: Some(4),
        },
        CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::StatusUnchanged(task),
            source_revision: None,
        },
        CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::TaskConvergenceUnchanged(task),
            source_revision: None,
        },
        CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Unknown(task),
            source_revision: None,
        },
        CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Gone(task),
            source_revision: Some(5),
        },
        CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                generation: 7,
                initial_cut: 4,
            }),
            source_revision: None,
        },
        CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                generation: 7,
                sequence: 1,
                covered_prefix: 4,
                source_cut: 5,
            }),
            source_revision: None,
        },
    ];
    let expected = vec![
        ObservationEvent::Fact {
            fact: ObservationFact::Status(status),
            source_revision: Some(1),
        },
        ObservationEvent::Fact {
            fact: ObservationFact::TaskConvergence(task_receipt),
            source_revision: Some(2),
        },
        ObservationEvent::Fact {
            fact: ObservationFact::ParticipationConvergence(participation_receipt),
            source_revision: Some(3),
        },
        ObservationEvent::Fact {
            fact: ObservationFact::Quiesce(Arc::new(quiesce)),
            source_revision: Some(4),
        },
        ObservationEvent::Fact {
            fact: ObservationFact::StatusUnchanged(task),
            source_revision: None,
        },
        ObservationEvent::Fact {
            fact: ObservationFact::TaskConvergenceUnchanged(task),
            source_revision: None,
        },
        ObservationEvent::Fact {
            fact: ObservationFact::Unknown(task),
            source_revision: None,
        },
        ObservationEvent::Fact {
            fact: ObservationFact::Gone(task),
            source_revision: Some(5),
        },
        ObservationEvent::Fact {
            fact: ObservationFact::CatchUpComplete {
                generation: 7,
                initial_cut: 4,
            },
            source_revision: None,
        },
        ObservationEvent::Fact {
            fact: ObservationFact::Bookmark {
                generation: 7,
                sequence: 1,
                covered_prefix: 4,
                source_cut: 5,
            },
            source_revision: None,
        },
    ];
    peer.expect_covered_frames_then_close(
        frames
            .iter()
            .map(|frame| encode_covered_status_event(frame).unwrap())
            .collect(),
    );
    peer.expect_covered_frames_then_hold(Vec::new());
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
    let mut native = FrontendDataRuntime::new(Handle::current());
    let client = Client::for_backend_in(
        Arc::from([peer.descriptor(participation.backend_process_id())]),
        0,
        native.clone(),
    );
    let first_request = Arc::new(SubscriptionRequest {
        participation,
        generation: NonZeroU64::new(7).unwrap(),
        status_cursors: Box::new([TaskStatusCursor::at(task, TaskStatusVersion::FIRST)]),
        task_convergence_cursors: Box::new([TaskConvergenceCursor::at(
            task,
            TaskConvergenceVersion::FIRST,
        )]),
        participation_cursor: Some(ParticipationConvergenceCursor::unobserved(participation)),
        quiesce_cursor: Some(novarocks_attempt_protocol::observation::QuiesceCursor {
            participation,
            fence_version: None,
        }),
        required_identities: Box::new([task]),
    });
    let mut first = start_generation(&f, &source, first_request.clone(), client.clone());
    let first_join = original_transport_join_id(&first.role).unwrap();
    let ended = tokio::time::timeout(
        Duration::from_secs(3),
        poll_fn(|cx| first.role.poll_shared_completion(cx)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(ended.is_none());
    assert_eq!(
        *first.result.lock().unwrap(),
        Some(SubscriptionEnd::RemoteClosed)
    );
    assert_eq!(original_transport_join_id(&first.role), Some(first_join));
    assert!(
        capture.lock().unwrap().is_empty(),
        "no entity input has been consumed by the actual actor"
    );
    assert!(
        source.reap_generation(first_request.generation).is_err(),
        "actual RPC/Join cannot retire ten held original frames/phase"
    );
    assert_eq!(
        peer.subscription_requests().len(),
        1,
        "Native Source has no automatic retry policy"
    );
    let applied = source.applied_request(first_request.generation).unwrap();
    assert!(Arc::ptr_eq(&applied, &first_request));
    // The caller supplies the next explicit AP generation from its original
    // applied snapshot. Receiving ten frames cannot mutate those cursors.
    let second_request = Arc::new(SubscriptionRequest {
        generation: NonZeroU64::new(8).unwrap(),
        ..(*applied).clone()
    });
    let mut second = start_generation(&f, &source, second_request.clone(), client);
    let second_join = original_transport_join_id(&second.role).unwrap();
    assert_ne!(first_join, second_join);
    tokio::time::timeout(Duration::from_secs(3), async {
        while peer.subscription_requests().len() != 2 || peer.open_covered_subscriptions() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let physical = peer.subscription_requests();
    let first_wire = decode_covered_subscribe_task_status(
        &physical[0],
        FieldPath::root("first_original_request"),
    )
    .unwrap();
    let second_wire = decode_covered_subscribe_task_status(
        &physical[1],
        FieldPath::root("second_original_request"),
    )
    .unwrap();
    assert_eq!(first_wire.generation.get(), 7);
    assert_eq!(second_wire.generation.get(), 8);
    assert_eq!(first_wire.required_identities, vec![task]);
    assert_eq!(second_wire.status_cursors, applied.status_cursors.to_vec());
    assert_eq!(
        second_wire.task_convergence_cursors,
        applied.task_convergence_cursors.to_vec()
    );
    assert_eq!(
        second_wire.status_cursors[0].current_version(),
        Some(TaskStatusVersion::FIRST)
    );
    assert_eq!(
        second_wire.task_convergence_cursors[0].current_version(),
        Some(TaskConvergenceVersion::FIRST)
    );
    assert_eq!(second_wire.quiesce_cursor.unwrap().fence_version, None);
    assert!(
        capture.lock().unwrap().is_empty(),
        "all ten original frames still precede actual receiver consumption"
    );
    assert!(source.reap_generation(first_request.generation).is_err());
    let mut observed = Vec::new();
    consume_actual_inputs(&f, &capture, &[&first.phase, &second.phase], &mut observed);
    let actual = observed
        .iter()
        .filter(|row| row.generation == first_request.generation)
        .collect::<Vec<_>>();
    assert_eq!(
        actual.len(),
        11,
        "ten original frames and their original EOF"
    );
    for (row, expected) in actual[..10].iter().zip(&expected) {
        assert_eq!(row.participation, participation);
        assert_eq!(row.generation.get(), 7);
        assert!(row.opening.opened_at.is_some());
        assert_eq!(&row.event, expected);
    }
    assert_eq!(actual[10].event, ObservationEvent::Closed);
    f.drain_progress();
    retire_actual_work(&f, &first);
    let same = source.reap_generation(first_request.generation).unwrap();
    assert!(Arc::ptr_eq(&same, &first_request));
    drop(same);
    assert_eq!(
        cohort.remaining(),
        0,
        "no second WC child is taken for the next original generation"
    );
    source.close_generation(second_request.generation).unwrap();
    let ended = tokio::time::timeout(
        Duration::from_secs(3),
        poll_fn(|cx| second.role.poll_shared_completion(cx)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(ended.is_none());
    assert_eq!(
        *second.result.lock().unwrap(),
        Some(SubscriptionEnd::CoreClosed)
    );
    consume_actual_inputs(&f, &capture, &[&first.phase, &second.phase], &mut observed);
    f.drain_progress();
    retire_actual_work(&f, &second);
    let same = source.reap_generation(second_request.generation).unwrap();
    assert!(Arc::ptr_eq(&same, &second_request));
    drop(same);
    drop(observed);
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    tokio::time::timeout(Duration::from_secs(2), source_scope.wait_released())
        .await
        .unwrap();
    drop((
        first,
        second,
        applied,
        first_request,
        second_request,
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
    assert!(peer.settle().await);
    wc::finish(control, root, driver);
    f.finish();
    let history = history.drain(4096);
    assert!(
        history.complete && history.remaining == 0,
        "actual retained history must be complete: {:?}",
        history.error
    );
}
