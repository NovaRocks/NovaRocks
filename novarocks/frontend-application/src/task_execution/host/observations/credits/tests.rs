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
