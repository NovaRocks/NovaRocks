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

//! Actual H2/read-credit pressure and explicit current AP policy. The removed
//! cross-query intake, lossy overflow and Native cursor/retry authors stay retired.
use super::native_all_frames::{
    OriginalGeneration, consume_actual_inputs, retire_actual_work, start_generation,
};
use super::*;
use crate::{
    native::{
        data_runtime::FrontendDataRuntime, task_transport::tests::Loopback, transport::Client,
    },
    query_execution::native_execution_adapter::source_cohort::{NativeSourceCohort, tests as wc},
    task_execution::host::subscriptions::{SubscriptionEnd, SubscriptionSource},
};
use novarocks_attempt_protocol::{
    ids::IdSeed,
    input::ReceivedObservation,
    observation::{
        SubscriptionRequest,
        controller::{
            CoveredSubscription, ObservationDisposition, SubscriptionDirective, SubscriptionStatus,
        },
    },
};
use novarocks_control_runtime::history::HistoryCapture;
use novarocks_execution_contract::{
    ParticipationConvergenceCursor, ParticipationConvergenceReceipt, ParticipationConvergenceState,
    ParticipationConvergenceVersion, TaskOutputFacts, TaskState, TaskStatus, TaskStatusCursor,
    TaskStatusVersion,
};
use novarocks_proto_codec::FieldPath;
use novarocks_task_codec::operation::{
    CoveredStatusStreamEvent, CoveredStatusStreamFact, decode_covered_subscribe_task_status,
    encode_covered_status_event,
};
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{future::poll_fn, time::Duration};
use tokio::runtime::Handle;

#[derive(Clone, Copy)]
enum Case {
    CompleteReceipt,
    AppliedCursor,
    ExplicitReopen,
    OnePendingFrame,
}
struct Pressure {
    f: Option<Fixture>,
    capture: BatchCapture,
    held: Option<(InputEnvelope, AttemptInput)>,
    peer: Option<Loopback>,
    native: Option<FrontendDataRuntime>,
    control: Option<novarocks_workload_control::WorkloadControl>,
    root: Option<novarocks_workload_control::RootWork>,
    driver: Option<novarocks_workload_control::joint_admission::JointProgressDriver>,
    capacity: Option<novarocks_workload_control::ResultCapacityHandle>,
    window: Option<novarocks_workload_control::ResultWindowGrant>,
    cohort: Option<NativeSourceCohort>,
    source: Option<Arc<SubscriptionSource<Recorder>>>,
    jobs: [Option<OriginalGeneration>; 2],
    observed: Vec<ReceivedObservation>,
}
struct Retained(Option<Box<Pressure>>);
impl Drop for Retained {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            // Keep the complete unresolved original F/Join/input/owner on an
            // assertion unwind; neither abort nor fragment Drop proves exit.
            static UNKNOWN: std::sync::OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
                std::sync::OnceLock::new();
            UNKNOWN
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .push(original);
        }
    }
}
impl Pressure {
    async fn take_first_frame(&mut self, policy: &mut CoveredSubscription) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let f = self.f.as_ref().unwrap();
                f.runtime.poll_one(Tick(1)).unwrap();
                let values = self.capture.lock().unwrap().drain(..).collect::<Vec<_>>();
                for (envelope, input) in values {
                    match &input {
                        AttemptInput::Executor(ExecutorFact::Observations(batch)) => {
                            assert_eq!(batch.items().len(), 1);
                            assert_eq!(envelope.source, ActorId::new(2).unwrap());
                            assert!(matches!(
                                policy.observe(batch.items()[0].clone()).unwrap(),
                                ObservationDisposition::ApplyEntity(_)
                            ));
                            assert!(self.held.replace((envelope, input)).is_none());
                        }
                        AttemptInput::Executor(ExecutorFact::SubscriptionProgress(snapshot)) => {
                            policy.observe_phase(*snapshot).unwrap();
                            f.runtime.retire_input(actor(), envelope.input).unwrap();
                            self.jobs[0]
                                .as_ref()
                                .unwrap()
                                .phase
                                .input_retired(envelope.input)
                                .unwrap();
                        }
                        AttemptInput::Executor(ExecutorFact::ReadProgress(progress)) => {
                            policy.observe_read_progress(*progress).unwrap();
                            f.runtime.retire_input(actor(), envelope.input).unwrap();
                            f.bank
                                .progress
                                .lock()
                                .unwrap()
                                .as_ref()
                                .unwrap()
                                .after_retire()
                                .unwrap();
                        }
                        _ => panic!("only actual original subscription inputs"),
                    }
                }
                if self.held.is_some() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    fn release_first_frame(&mut self) {
        let (envelope, input) = self.held.take().unwrap();
        let AttemptInput::Executor(ExecutorFact::Observations(batch)) = &input else {
            panic!("same original held frame");
        };
        self.observed.extend(batch.items().iter().cloned());
        let f = self.f.as_ref().unwrap();
        f.runtime.retire_input(actor(), envelope.input).unwrap();
        f.input.input_retired(envelope.input).unwrap();
        drop(input);
    }
    fn consume(&mut self, slot: usize) {
        let f = self.f.as_ref().unwrap();
        consume_actual_inputs(
            f,
            &self.capture,
            &[&self.jobs[slot].as_ref().unwrap().phase],
            &mut self.observed,
        );
        // All actual progress inputs went through the capture/retire callback
        // above. Never poll/drain a captured role a second time by numeric ID.
        let idle = f
            .bank
            .progress
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .in_flight()
            .unwrap()
            .is_none();
        if idle {
            assert!(
                !f.bank
                    .progress_failed
                    .load(std::sync::atomic::Ordering::SeqCst)
            );
            f.bank.progress_debts.lock().unwrap().clear();
        }
    }
    async fn close_job(&mut self, slot: usize, request: &Arc<SubscriptionRequest>) {
        let source = self.source.as_ref().unwrap();
        source.close_generation(request.generation).unwrap();
        let job = self.jobs[slot].as_mut().unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(3),
                poll_fn(|cx| job.role.poll_shared_completion(cx)),
            )
            .await
            .unwrap()
            .unwrap()
            .is_none()
        );
        assert_eq!(
            *job.result.lock().unwrap(),
            Some(SubscriptionEnd::CoreClosed)
        );
        self.consume(slot);
        let f = self.f.as_ref().unwrap();
        let job = self.jobs[slot].as_ref().unwrap();
        retire_actual_work(f, job);
        let same = self
            .source
            .as_ref()
            .unwrap()
            .reap_generation(request.generation)
            .unwrap();
        assert!(Arc::ptr_eq(&same, request));
        drop(same);
        drop(self.jobs[slot].take());
    }
    async fn finish(&mut self) {
        assert!(self.jobs.iter().all(Option::is_none));
        assert!(self.held.is_none());
        assert!(self.capture.lock().unwrap().is_empty());
        self.observed.clear();
        let source = self.source.take().unwrap();
        let child = source.scope().unwrap();
        source.close_new_jobs();
        source.close_after_jobs().unwrap();
        wc::drive(self.driver.as_mut().unwrap());
        tokio::time::timeout(Duration::from_secs(3), child.wait_released())
            .await
            .unwrap();
        drop((
            source,
            child,
            self.window.take(),
            self.cohort.take(),
            self.capacity.take(),
        ));
        let native = self.native.as_mut().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        native.drain_original_apply_until(deadline).await.unwrap();
        native.drain_native_outgoing_until(deadline).await.unwrap();
        assert!(self.peer.as_ref().unwrap().applied_requests().is_empty());
        assert_eq!(self.peer.as_ref().unwrap().control_requests(), 0);
        assert!(self.peer.as_mut().unwrap().settle().await);
        drop((self.native.take(), self.peer.take()));
        wc::finish(
            self.control.take().unwrap(),
            self.root.take().unwrap(),
            self.driver.take().unwrap(),
        );
        self.f.take().unwrap().finish();
    }
}

async fn actual_pressure(case: Case) {
    let capture: BatchCapture = Arc::new(Mutex::new(Vec::with_capacity(8)));
    let f = Fixture::make_with_captures(
        1,
        3,
        true,
        Some(liveness::Trace {
            key: key(),
            history: HistoryCapture::new(4096),
            capture: liveness::Capture::default(),
        }),
        Some(capture.clone()),
    );
    let participation = f.key.participation;
    let task = f.input.root_binding().root();
    let mut expected = Vec::new();
    let mut wire = Vec::new();
    let mut status_version = TaskStatusVersion::FIRST;
    let mut receipt_version = ParticipationConvergenceVersion::FIRST;
    for index in 0..3 {
        let fact = if matches!(case, Case::CompleteReceipt) {
            let receipt = ParticipationConvergenceReceipt::new(
                participation,
                receipt_version,
                ParticipationConvergenceState::WorkerStoppedAndParticipationFenced,
            );
            receipt_version = receipt_version.next().unwrap();
            expected.push(ObservationEvent::Fact {
                fact: ObservationFact::ParticipationConvergence(receipt),
                source_revision: Some(index + 1),
            });
            CoveredStatusStreamFact::ParticipationConvergence(receipt)
        } else {
            let status = if index == 0 && !matches!(case, Case::OnePendingFrame) {
                TaskStatus::created(task)
            } else {
                TaskStatus::try_new(
                    task,
                    status_version,
                    TaskState::Running,
                    None,
                    TaskOutputFacts::default(),
                )
                .unwrap()
            };
            status_version = status_version.next().unwrap();
            expected.push(ObservationEvent::Fact {
                fact: ObservationFact::Status(status.clone()),
                source_revision: Some(index + 1),
            });
            CoveredStatusStreamFact::Status(status)
        };
        wire.push(
            encode_covered_status_event(&CoveredStatusStreamEvent {
                fact,
                source_revision: Some(index + 1),
            })
            .unwrap(),
        );
    }
    let peer = Loopback::start().await;
    peer.expect_covered_frames_then_hold(wire);
    peer.expect_covered_frames_then_hold(Vec::new());
    let (control, mut driver, capacity) = wc::fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let parent = root.owner.scope();
    wc::bind(&parent, &mut driver);
    let window = capacity
        .try_acquire(&parent, ResultWindowClass::Client)
        .unwrap();
    let cohort = wc::cohort(&parent, 1, &mut driver);
    let source =
        SubscriptionSource::from_cohort(participation, &parent, &cohort, f.input.clone()).unwrap();
    let native = FrontendDataRuntime::new(Handle::current());
    let client = Client::for_backend_in(
        Arc::from([peer.descriptor(participation.backend_process_id())]),
        0,
        native.clone(),
    );
    let generation = NonZeroU64::new(3).unwrap();
    let request = Arc::new(SubscriptionRequest {
        participation,
        generation,
        status_cursors: if matches!(case, Case::CompleteReceipt) {
            Box::new([])
        } else {
            Box::new([TaskStatusCursor::unobserved(task)])
        },
        task_convergence_cursors: Box::new([]),
        participation_cursor: matches!(case, Case::CompleteReceipt)
            .then(|| ParticipationConvergenceCursor::unobserved(participation)),
        quiesce_cursor: None,
        required_identities: if matches!(case, Case::CompleteReceipt) {
            Box::new([])
        } else {
            Box::new([task])
        },
    });
    let mut policy = CoveredSubscription::new(participation, IdSeed::new(43, 1), 1).unwrap();
    policy.activate();
    assert_eq!(
        policy
            .drive(MonotonicInstant::ORIGIN, generation, None)
            .unwrap(),
        Some(SubscriptionDirective::Open { generation })
    );
    let mut retained = Retained(Some(Box::new(Pressure {
        f: Some(f),
        capture,
        held: None,
        peer: Some(peer),
        native: Some(native),
        control: Some(control),
        root: Some(root),
        driver: Some(driver),
        capacity: Some(capacity),
        window: Some(window),
        cohort: Some(cohort),
        source: Some(source),
        jobs: [None, None],
        observed: Vec::new(),
    })));
    let c = retained.0.as_mut().unwrap();
    c.jobs[0] = Some(start_generation(
        c.f.as_ref().unwrap(),
        c.source.as_ref().unwrap(),
        request.clone(),
        client.clone(),
    ));
    let stream = StreamKey {
        participation,
        generation,
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let paused = {
                let state = c.f.as_ref().unwrap().input.0.state.lock().unwrap();
                state
                    .streams
                    .get(&stream)
                    .is_some_and(|row| row.accepted_unretired == 2 && row.paused_from.is_some())
            };
            if paused && c.peer.as_ref().unwrap().subscription_requests().len() == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        c.capture.lock().unwrap().is_empty(),
        "no AP input consumed before pressure"
    );
    assert_eq!(
        c.f.as_ref()
            .unwrap()
            .bank
            .state
            .lock()
            .unwrap()
            .claims
            .values()
            .filter(|key| **key == Some(stream))
            .count(),
        2
    );
    assert!(Arc::ptr_eq(
        &c.source
            .as_ref()
            .unwrap()
            .applied_request(generation)
            .unwrap(),
        &request
    ));
    if matches!(case, Case::OnePendingFrame) {
        // Preserve the original wall-clock interval with NO AP consumption.
        // Original counting-wake callbacks are replaced by the same paid
        // Source's actual claim/queue/writer state, not synthetic wake counts.
        tokio::time::sleep(Duration::from_millis(5200)).await;
        let f = c.f.as_ref().unwrap();
        let state = f.input.0.state.lock().unwrap();
        let original = state.streams.get(&stream).unwrap();
        assert_eq!(original.accepted_unretired, 2);
        assert!(original.paused_from.is_some());
        assert_eq!(state.queue.len(), 1);
        assert!(state.in_flight.is_some());
        drop(state);
        assert!(c.capture.lock().unwrap().is_empty());
        assert_eq!(c.peer.as_ref().unwrap().subscription_requests().len(), 1);
        assert_eq!(c.peer.as_ref().unwrap().open_covered_subscriptions(), 1);
        assert_eq!(*c.jobs[0].as_ref().unwrap().result.lock().unwrap(), None);
        assert!(Arc::ptr_eq(
            &c.source
                .as_ref()
                .unwrap()
                .applied_request(generation)
                .unwrap(),
            &request
        ));
    }
    c.take_first_frame(&mut policy).await;
    let (envelope, held) = c.held.as_ref().unwrap();
    let AttemptInput::Executor(ExecutorFact::Observations(batch)) = held else {
        unreachable!()
    };
    assert_eq!(batch.items()[0].event, expected[0]);
    assert_eq!(
        c.f.as_ref()
            .unwrap()
            .input
            .0
            .state
            .lock()
            .unwrap()
            .in_flight
            .as_ref()
            .unwrap()
            .input,
        envelope.input
    );
    assert_eq!(
        c.f.as_ref()
            .unwrap()
            .bank
            .state
            .lock()
            .unwrap()
            .claims
            .values()
            .filter(|key| **key == Some(stream))
            .count(),
        2,
        "actual consumption alone cannot return either original credit"
    );
    assert!(
        c.source
            .as_ref()
            .unwrap()
            .reap_generation(generation)
            .is_err()
    );
    assert!(!c.jobs[0].as_ref().unwrap().role.try_retire().unwrap());
    let applied = Arc::new(SubscriptionRequest {
        status_cursors: if matches!(case, Case::CompleteReceipt) {
            Box::new([])
        } else {
            Box::new([TaskStatusCursor::at(task, TaskStatusVersion::FIRST)])
        },
        participation_cursor: matches!(case, Case::CompleteReceipt).then(|| {
            ParticipationConvergenceCursor::at(
                participation,
                ParticipationConvergenceVersion::FIRST,
            )
        }),
        ..(*request).clone()
    });
    // This is the explicit caller's applied snapshot, not a second Native
    // entity/cursor author. Actual QueryTaskExecution replay has its own test.
    c.source
        .as_ref()
        .unwrap()
        .adjust_read_window(applied.clone())
        .unwrap();
    for (millis, id) in [(100_000, 51), (200_000, 52)] {
        let f = c.f.as_ref().unwrap();
        f.clock.at(millis);
        let at = MonotonicInstant::from_origin(Duration::from_millis(millis));
        let Some(SubscriptionDirective::Confirm(confirm)) =
            policy.drive(at, generation, Some(token(id))).unwrap()
        else {
            panic!("same current AP policy requests an actual consistent read confirmation");
        };
        let snapshot = f
            .input
            .read_state(stream, confirm.token, confirm.requested_at)
            .unwrap();
        assert!(snapshot.reading_paused);
        assert!(!snapshot.closed);
        policy.observe_read_state(snapshot).unwrap();
        assert_eq!(policy.status(), SubscriptionStatus::Live);
        assert!(policy.last_failure().is_none());
        assert!(Arc::ptr_eq(
            &c.source
                .as_ref()
                .unwrap()
                .applied_request(generation)
                .unwrap(),
            &applied
        ));
        assert_eq!(c.peer.as_ref().unwrap().subscription_requests().len(), 1);
        assert_eq!(c.peer.as_ref().unwrap().open_covered_subscriptions(), 1);
        assert!(
            policy.drive(at, generation, None).unwrap().is_none(),
            "local Full cannot privately produce Close/Open or exhaust the one-error budget"
        );
        assert_eq!(*c.jobs[0].as_ref().unwrap().result.lock().unwrap(), None);
    }
    c.release_first_frame();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            c.consume(0);
            if c.observed.len() == 3 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        c.observed
            .iter()
            .map(|row| row.event.clone())
            .collect::<Vec<_>>(),
        expected,
        "same complete wire payloads survive actual capacity wait without loss or synthetic replay"
    );
    assert!(
        c.observed
            .iter()
            .all(|row| row.participation == participation && row.generation == generation)
    );
    assert!(
        Arc::ptr_eq(
            &c.source
                .as_ref()
                .unwrap()
                .applied_request(generation)
                .unwrap(),
            &applied
        ),
        "receive/retirement cannot advance the explicit caller's cursor"
    );
    let first_wire = decode_covered_subscribe_task_status(
        &c.peer.as_ref().unwrap().subscription_requests()[0],
        FieldPath::root("original_pressure_request"),
    )
    .unwrap();
    assert_eq!(first_wire.generation, generation);
    assert_eq!(
        first_wire.status_cursors,
        request.status_cursors.to_vec(),
        "applied metadata cannot mutate the active original RPC"
    );
    assert_eq!(c.peer.as_ref().unwrap().subscription_requests().len(), 1);
    assert!(c.peer.as_ref().unwrap().applied_requests().is_empty());
    assert_eq!(c.peer.as_ref().unwrap().control_requests(), 0);
    if matches!(case, Case::ExplicitReopen) {
        let at = MonotonicInstant::from_origin(Duration::from_secs(200));
        policy.reconcile(at);
        assert_eq!(
            policy.drive(at, generation, None).unwrap(),
            Some(SubscriptionDirective::Close { generation })
        );
        assert!(
            policy
                .drive(
                    at.saturating_add(Duration::from_secs(100)),
                    generation,
                    None
                )
                .unwrap()
                .is_none(),
            "explicit Close is not actual exit/new-generation authority"
        );
        c.close_job(0, &applied).await;
        assert_eq!(c.peer.as_ref().unwrap().subscription_requests().len(), 1);
        policy.observe_actual_exit(generation).unwrap();
        let Some(SubscriptionDirective::Open { generation: next }) =
            policy.drive(at, generation, None).unwrap()
        else {
            panic!("only the original AP author opens after actual exit");
        };
        assert_eq!(next.get(), generation.get() + 1);
        let next_request = Arc::new(SubscriptionRequest {
            generation: next,
            ..(*applied).clone()
        });
        c.jobs[1] = Some(start_generation(
            c.f.as_ref().unwrap(),
            c.source.as_ref().unwrap(),
            next_request.clone(),
            client.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            while c.peer.as_ref().unwrap().subscription_requests().len() != 2
                || c.peer.as_ref().unwrap().open_covered_subscriptions() != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let second_wire = decode_covered_subscribe_task_status(
            &c.peer.as_ref().unwrap().subscription_requests()[1],
            FieldPath::root("explicit_pressure_reopen"),
        )
        .unwrap();
        assert_eq!(second_wire.generation, next);
        assert_eq!(second_wire.status_cursors, applied.status_cursors.to_vec());
        assert_eq!(second_wire.status_cursors[0].identity(), task);
        assert_eq!(
            second_wire.status_cursors[0].current_version(),
            Some(TaskStatusVersion::FIRST)
        );
        assert_eq!(
            c.cohort.as_ref().unwrap().remaining(),
            0,
            "next generation uses the same paid stable Source child"
        );
        c.close_job(1, &next_request).await;
        drop(next_request);
    } else {
        c.close_job(0, &applied).await;
    }
    drop((client, request, applied, parent));
    c.finish().await;
    drop(retained.0.take());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn convergence_overflow_waits_for_capacity_without_spending_the_network_budget() {
    // The former cross-query global convergence intake is retired. This is
    // the actual per-AP Source's paid capacity and complete-receipt contract.
    actual_pressure(Case::CompleteReceipt).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn intake_overflow_preserves_the_last_enqueued_status_cursor() {
    actual_pressure(Case::AppliedCursor).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn intake_overflow_waits_for_task_round_resubscription_without_spending_error_budget() {
    // TaskRound is retired; current CoveredSubscription is the sole policy
    // author, while real old F/Join/report/input retirement precedes reopening.
    actual_pressure(Case::ExplicitReopen).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covered_stream_stops_reading_while_its_one_pending_frame_waits() {
    actual_pressure(Case::OnePendingFrame).await;
}
