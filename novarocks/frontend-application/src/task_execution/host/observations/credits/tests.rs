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

use super::*;
use novarocks_control_runtime::transaction::{RuntimeLedger, ScopeHandle};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::Generation,
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use std::{
    num::NonZeroU64,
    sync::atomic::{AtomicUsize, Ordering},
    task::Wake,
};

fn stream() -> StreamKey {
    StreamKey {
        participation: ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(2, 3), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        ),
        generation: NonZeroU64::new(1).unwrap(),
    }
}

fn pool(streams: &[StreamKey], data_bytes: usize) -> (Arc<ObservationCredits>, ScopeHandle) {
    let generation = Generation::new(1).unwrap();
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope = ledger.open_scope_next(None, generation).unwrap();
    let budget = ObservationBudget {
        queued_frames: 1,
        total_positions: 3,
        max_frame_bytes: 100,
        total_bytes: data_bytes + 10,
        control_bytes: 10,
    };
    let (mut work, mut reservation) = scope
        .reserve_next(
            generation,
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    inputs: budget.total_positions,
                    ..Default::default()
                },
            ),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    work.start().unwrap();
    let credits = ObservationCredits::bind(
        budget,
        streams.iter().map(|key| key.participation),
        &work,
        &mut reservation,
    )
    .unwrap();
    for key in streams {
        credits.register_stream(*key).unwrap();
    }
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    reservation.finish().unwrap();
    (credits, scope)
}

#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn ready_credit(polled: Poll<Result<ObservationCredit, ObservationError>>) -> ObservationCredit {
    match polled {
        Poll::Ready(Ok(credit)) => credit,
        Poll::Ready(Err(error)) => panic!("observation credit refused: {error:?}"),
        Poll::Pending => panic!("observation credit unexpectedly pending"),
    }
}

#[test]
fn data_bytes_and_positions_wake_reads_without_spending_the_control_share() {
    let first = stream();
    let second = stream();
    let third = stream();
    let (credits, scope) = pool(&[first, second, third], 160);
    let original_storage = scope.snapshot().unwrap().storage;
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);

    let mut first_credit = ready_credit(credits.poll_read(first, &mut cx));
    assert!(credits.poll_read(second, &mut cx).is_pending());
    let control = credits.try_control().unwrap();
    assert!(matches!(credits.try_control(), Err(ObservationError::Busy)));
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);

    first_credit.charge(40).unwrap();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    let mut second_credit = ready_credit(credits.poll_read(second, &mut cx));
    second_credit.charge(20).unwrap();
    assert!(credits.poll_read(third, &mut cx).is_pending());
    assert!(!credits.stream_claims_exited(third));
    drop(control);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);

    drop(first_credit);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
    assert!(credits.stream_claims_exited(first));
    let third_credit = ready_credit(credits.poll_read(third, &mut cx));
    assert_eq!(credits.retire_after_exit(), Err(ObservationError::Busy));
    drop((second_credit, third_credit));
    assert!(credits.stream_claims_exited(second));
    assert!(credits.stream_claims_exited(third));
    assert_eq!(scope.snapshot().unwrap().storage, original_storage);
    credits.retire_after_exit().unwrap();
    assert_eq!(scope.snapshot().unwrap().storage + 3, original_storage);
}

#[test]
fn slot_return_wakes_other_readers_when_a_wake_synchronously_reregisters() {
    struct ReentrantReader {
        credits: Arc<ObservationCredits>,
        stream: StreamKey,
        retry: Waker,
        held: Mutex<Option<ObservationCredit>>,
    }

    impl Wake for ReentrantReader {
        fn wake(self: Arc<Self>) {
            let mut cx = Context::from_waker(&self.retry);
            let credit = ready_credit(self.credits.poll_read(self.stream, &mut cx));
            assert!(self.credits.poll_read(self.stream, &mut cx).is_pending());
            assert!(self.held.lock().unwrap().replace(credit).is_none());
        }
    }

    let original = stream();
    let reentrant = StreamKey {
        generation: NonZeroU64::new(2).unwrap(),
        ..original
    };
    let later = StreamKey {
        generation: NonZeroU64::new(3).unwrap(),
        ..original
    };
    let (credits, _) = pool(&[original, reentrant, later], 100);
    let credit = ready_credit(credits.poll_read(original, &mut Context::from_waker(Waker::noop())));
    let retry = Arc::new(Wakes::default());
    let reentry = Arc::new(ReentrantReader {
        credits: credits.clone(),
        stream: reentrant,
        retry: Waker::from(retry.clone()),
        held: Mutex::new(None),
    });
    let later_wakes = Arc::new(Wakes::default());
    let reentry_waker = Waker::from(reentry.clone());
    let later_waker = Waker::from(later_wakes.clone());
    assert!(
        credits
            .poll_read(reentrant, &mut Context::from_waker(&reentry_waker))
            .is_pending()
    );
    assert!(
        credits
            .poll_read(later, &mut Context::from_waker(&later_waker))
            .is_pending()
    );

    drop(credit);
    assert_eq!(later_wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(retry.0.load(Ordering::SeqCst), 0);
    let credit = reentry.held.lock().unwrap().take().unwrap();
    drop(credit);
    assert_eq!(retry.0.load(Ordering::SeqCst), 1);
    credits.retire_after_exit().unwrap();
}

#[test]
fn retirement_requires_exact_generation_and_wait_cancellation() {
    let first = stream();
    let second = stream();
    let (credits, scope) = pool(&[first, second], 100);
    let mut cx = Context::from_waker(Waker::noop());
    let first_credit = ready_credit(credits.poll_read(first, &mut cx));
    assert!(credits.poll_read(second, &mut cx).is_pending());

    let replacement = StreamKey {
        generation: NonZeroU64::new(2).unwrap(),
        ..second
    };
    assert_eq!(credits.retire_stream(second), Err(ObservationError::Busy));
    credits.register_stream(replacement).unwrap();
    assert_eq!(
        credits.register_stream(second),
        Err(ObservationError::GenerationAlreadyRegistered)
    );
    assert_eq!(
        credits.register_stream(StreamKey {
            generation: NonZeroU64::new(3).unwrap(),
            ..second
        }),
        Err(ObservationError::Budget)
    );
    credits.cancel_read_wait(replacement);
    assert!(!credits.stream_claims_exited(second));
    credits.cancel_read_wait(second);
    assert!(credits.stream_claims_exited(second));
    credits.retire_stream(second).unwrap();

    assert!(matches!(
        credits.poll_read(second, &mut cx),
        Poll::Ready(Err(ObservationError::UnknownStream))
    ));
    assert!(credits.stream_claims_exited(replacement));
    drop(first_credit);
    let original_storage = scope.snapshot().unwrap().storage;
    credits.retire_after_exit().unwrap();
    assert_eq!(scope.snapshot().unwrap().storage + 3, original_storage);
}

#[test]
fn wake_panic_keeps_the_original_return_unacknowledged() {
    struct PanicWake;
    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("original waiter wake panic");
        }
    }

    let first = stream();
    let second = stream();
    let (credits, scope) = pool(&[first, second], 100);
    let credit = ready_credit(credits.poll_read(first, &mut Context::from_waker(Waker::noop())));
    let original_storage = scope.snapshot().unwrap().storage;
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(
        credits
            .poll_read(second, &mut Context::from_waker(&waker))
            .is_pending()
    );

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(credit)));
    assert!(result.is_err());
    assert!(!credits.stream_claims_exited(first));
    assert_eq!(credits.retire_stream(first), Err(ObservationError::Busy));
    assert_eq!(credits.retire_after_exit(), Err(ObservationError::Busy));
    assert_eq!(scope.snapshot().unwrap().storage, original_storage);
}

#[test]
fn read_progress_retires_only_the_generations_covered_by_each_actual_occurrence() {
    use crate::control_runtime::actor::{FrontendActor, FrontendEffect, FrontendInput, TestActor};
    use novarocks_attempt_protocol::input::{AttemptInput, ExecutorFact};
    use novarocks_control_runtime::{
        actor::{ActorConfig, ActorStep},
        actor_turn::ActorRuntime,
    };
    use novarocks_control_runtime_core::{
        identity::{ActorId, Tick},
        mailbox::{InputClass, InputEnvelope},
    };
    use novarocks_coordination_contract::MonotonicInstant;
    use std::time::Duration;

    struct Recorder(Arc<Mutex<Vec<ReadProgressSnapshot>>>);
    impl TestActor for Recorder {
        fn handle(
            &mut self,
            _: Tick,
            _: InputEnvelope,
            input: FrontendInput,
            _: &mut ActorStep<'_, FrontendEffect>,
        ) {
            let FrontendInput::Attempt(input) = input else {
                panic!("original read progress input");
            };
            let AttemptInput::Executor(ExecutorFact::ReadProgress(progress)) = *input else {
                panic!("original read progress snapshot");
            };
            self.0.lock().unwrap().push(progress);
        }
    }

    let old = stream();
    let replacement = StreamKey {
        generation: NonZeroU64::new(2).unwrap(),
        ..old
    };
    let (credits, scope) = pool(&[old, replacement], 300);
    let runtime = ActorRuntime::new();
    let actor = ActorId::new(1).unwrap();
    let source = ActorId::new(2).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let registration = runtime
        .register_bound(
            actor,
            scope.clone(),
            FrontendActor::Test(Box::new(Recorder(seen.clone()))),
            ActorConfig {
                mailbox: [4; 4],
                max_batches: 4,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 8,
                max_step_work: 1,
            },
        )
        .unwrap_or_else(|_| panic!("original read progress actor"));
    let (mut work, mut reservation) = scope
        .reserve_next(
            Generation::new(1).unwrap(),
            ReservationResources::work_only(),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    work.start().unwrap();
    let standing = runtime
        .reserve_admitted_input_for(&registration, source, InputClass::Ordinary, &work)
        .unwrap()
        .into_standing_input()
        .unwrap();
    let progress = ReadProgressInput::new(&registration, old.participation, standing);
    let report = |key: StreamKey, revision| ReadProgressSnapshot {
        participation: key.participation,
        generation: key.generation,
        resumed_at: MonotonicInstant::from_origin(Duration::from_millis(revision)),
        resume_revision: NonZeroU64::new(revision).unwrap(),
    };
    let mut cx = Context::from_waker(Waker::noop());
    let old_credit = ready_credit(credits.poll_read(old, &mut cx));
    credits.read_resumed(&progress, report(old, 1)).unwrap();
    drop(old_credit);

    let new_credit = ready_credit(credits.poll_read(replacement, &mut cx));
    credits
        .read_resumed(&progress, report(replacement, 1))
        .unwrap();
    drop(new_credit);
    let new_credit = ready_credit(credits.poll_read(replacement, &mut cx));
    credits
        .read_resumed(&progress, report(replacement, 2))
        .unwrap();
    drop(new_credit);

    assert!(!credits.stream_claims_exited(old));
    assert!(!credits.stream_claims_exited(replacement));
    assert!(!progress.retire_ready().unwrap());
    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert_eq!(*seen.lock().unwrap(), vec![report(old, 1)]);
    assert!(!credits.stream_claims_exited(old));
    let old_batch = runtime.committed_batches(actor, None, 4).unwrap();
    assert_eq!(old_batch.len(), 1);
    let (_, old_input) = old_batch[0];
    assert!(progress.retire_indexed(old_input).unwrap());
    assert_eq!(
        progress.retire_indexed(old_input),
        Err(novarocks_control_runtime::actor_turn::ActorTurnError::InputAuthorMismatch)
    );
    credits.retire_stream(old).unwrap();
    assert!(!credits.stream_claims_exited(replacement));

    assert!(runtime.poll_one(Tick(2)).unwrap());
    assert_eq!(
        *seen.lock().unwrap(),
        vec![report(old, 1), report(replacement, 2)]
    );
    assert_eq!(runtime.committed_batches(actor, None, 4).unwrap().len(), 1);
    assert!(progress.retire_ready().unwrap());
    assert!(credits.stream_claims_exited(replacement));
    progress.close_after_exit().unwrap();

    let new_credit = ready_credit(credits.poll_read(replacement, &mut cx));
    assert_eq!(
        credits.read_resumed(&progress, report(replacement, 3)),
        Err(ObservationError::Runtime(
            novarocks_control_runtime::actor_turn::ActorTurnError::Closed
        ))
    );
    drop(new_credit);
    assert!(!credits.stream_claims_exited(replacement));
    assert_eq!(credits.retire_after_exit(), Err(ObservationError::Busy));
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    reservation.finish().unwrap();
    scope.close_external().unwrap();
    scope.reap_ready(8).unwrap();
    runtime.close_actor_admission(actor).unwrap();
    while let Some(turn) = runtime.claim_host_actor() {
        turn.release(false).unwrap();
    }
    runtime.seal_actor(actor).unwrap();
    runtime.remove_actor(actor).unwrap();
    assert!(!scope.snapshot().unwrap().retirement_ready);
}
