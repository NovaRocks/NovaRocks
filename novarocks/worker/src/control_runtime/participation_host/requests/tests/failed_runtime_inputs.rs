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

//! A queued original R survives actual Core failure. Failed-input disposal
//! supplies abandonment and a wake, never its external delivery exit.

use super::*;
use crate::control_runtime::{
    actors::{WorkerMessage, tests::task},
    participation_host::{TIMER, failure::FailurePublicationError},
};
use novarocks_control_runtime::actor_turn::{FailureSourceState, InputPhase};
use novarocks_control_runtime_core::identity::ActionId;

#[test]
fn queued_submitted_request_is_woken_by_actual_failed_input_take_and_stays_paid_until_delivery_ends()
 {
    let f = original_participation(1);
    let ready = ready(&f, 1);
    let (fault_work, fault_header) = {
        let address = f.service.participation(f.identity).unwrap();
        let host = address.lock().unwrap();
        let original = &host.resources.metadata_resources[TIMER].work;
        (original.reference(), original.retirement_observer())
    };
    let unknown = task(f.identity, 999);
    // Fill, but do not poll, this actual Control input. The subsequent R is
    // filled into the same class, so the real mailbox FIFO encounters it later.
    let bad = {
        let address = f.service.participation(f.identity).unwrap();
        let host = address.lock().unwrap();
        f.runtime
            .reserve_admitted_input_for(
                host.registration(),
                ActorId::new(88).unwrap(),
                InputClass::Control,
                &host.resources.metadata_resources[TIMER].work,
            )
            .unwrap()
    }
    .fill(WorkerMessage::Participation(
        ParticipationMessage::Command {
            work: fault_work,
            request: None,
            command: ParticipationInput::Installed { task: unknown },
        },
    ))
    .unwrap();
    let (work, mut handle) = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .admit_request(
            &ready,
            ActorId::new(89).unwrap(),
            InputClass::Control,
            operation(151),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .unwrap();
    let request = handle.request();
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .submit_request(work, renew())
        .unwrap();
    f.runtime.poll_one(Tick(9)).unwrap();
    assert!(f.runtime.snapshot(participation_actor()).unwrap().failed);
    assert!(
        f.runtime
            .input_custody(participation_actor(), request.token)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::Filled),
        "the actual later R remains queued when the earlier Control input fails"
    );
    assert!(
        f.runtime
            .input_custody(participation_actor(), bad)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::AbandonedByOwnerFailure),
        "the failed business occurrence retains its own actual abandoned input"
    );
    let FailureSourceState::ClosedPending(failure_input) = f
        .runtime
        .failure_source_state(participation_actor())
        .unwrap()
    else {
        panic!("the actual builtin failure source must own its pending batch");
    };
    assert_ne!(failure_input, bad);
    let failure = f
        .runtime
        .take_effect(
            participation_actor(),
            failure_input,
            ActionId::new(1).unwrap(),
        )
        .unwrap();
    assert!(matches!(failure.effect, WorkerAction::OwnerFailed { .. }));
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(failure)
        .unwrap();
    assert!(matches!(
        f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_failed_runtime_inputs(1)
            .expect_err("publication drain is a real prerequisite"),
        FailurePublicationError::NotDrained
    ));
    let mut drained = false;
    for _ in 0..128 {
        if f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_failure_publications(1)
            .unwrap()
        {
            drained = true;
            break;
        }
    }
    assert!(drained);
    let delivery = handle
        .try_take()
        .expect("the original empty outbox receives the failed-owner reply");
    assert_eq!(delivery.request(), request);
    assert!(matches!(
        delivery.body(),
        ParticipationReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed))
    ));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);

    // Consume the original publication wake. A blocked R is still in its
    // real inventory; an empty ready queue proves only that this wake was used.
    let mut old_notices = 0;
    for _ in 0..4 {
        let Some(actual_work) = ready.next_participation_ready(&ready_owner(&f)) else {
            break;
        };
        assert_eq!(actual_work, work);
        assert!(
            !f.service
                .participation(f.identity)
                .unwrap()
                .lock()
                .unwrap()
                .advance_request(actual_work)
                .unwrap()
        );
        old_notices += 1;
    }
    assert!(old_notices > 0);
    assert!(ready.next_participation_ready(&ready_owner(&f)).is_none());
    assert!(
        f.runtime
            .input_custody(participation_actor(), request.token)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::Filled)
    );
    let zero = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_failed_runtime_inputs(0)
        .unwrap();
    assert_eq!(zero.units, 0);
    assert_eq!(zero.taken, 0);
    assert!(!zero.empty);
    assert!(
        f.runtime
            .input_custody(participation_actor(), request.token)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::Filled)
    );
    assert!(ready.next_participation_ready(&ready_owner(&f)).is_none());

    let mut took_original = false;
    for _ in 0..8 {
        let progress = f
            .service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_failed_runtime_inputs(1)
            .unwrap();
        assert!(progress.units <= 1);
        assert!(progress.taken <= progress.units);
        if f.runtime
            .input_custody(participation_actor(), request.token)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::AbandonedByOwnerFailure)
        {
            assert_eq!(progress.taken, 1);
            took_original = true;
            break;
        }
    }
    assert!(took_original);
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    let awakened = ready
        .next_participation_ready(&ready_owner(&f))
        .expect("taking this exact queued R input must notify its original address again");
    assert_eq!(awakened, work);
    assert!(
        !f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_request(work)
            .unwrap(),
        "abandoning the Core input does not end the real external delivery"
    );
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);

    drop((handle, delivery));
    let mut retired = false;
    for _ in 0..8 {
        let original = ready
            .next_participation_ready(&ready_owner(&f))
            .expect("the actual external delivery end wakes the same original R");
        assert_eq!(original, work);
        if f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_request(original)
            .unwrap()
        {
            retired = true;
            break;
        }
    }
    assert!(retired);
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert_eq!(ready.registered(), 0);
    let mut queues_empty = false;
    for _ in 0..4 {
        let progress = f
            .service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_failed_runtime_inputs(1)
            .unwrap();
        assert!(progress.units <= 1);
        assert_eq!(progress.taken, 0);
        if progress.empty {
            queues_empty = true;
            break;
        }
    }
    assert!(
        queues_empty,
        "one real empty round covers all four mailbox classes"
    );
    let final_observation = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_failed_runtime_inputs(0)
        .unwrap();
    assert!(final_observation.empty);
    assert_eq!((final_observation.units, final_observation.taken), (0, 0));
    assert!(ready.next_participation_ready(&ready_owner(&f)).is_none());
    // The failed ordinary occurrence and the builtin supervisor have distinct
    // actual headers. End each original input; neither is a synthetic cursor.
    f.runtime.retire_input(participation_actor(), bad).unwrap();
    assert!(
        f.runtime
            .input_custody(participation_actor(), bad)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.runtime
            .failure_source_state(participation_actor())
            .unwrap(),
        FailureSourceState::ClosedPending(failure_input)
    );
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .complete_failure_publications_for_test();
    assert_eq!(
        f.runtime
            .failure_source_state(participation_actor())
            .unwrap(),
        FailureSourceState::ClosedDrained
    );
    assert!(
        f.runtime
            .input_custody(participation_actor(), failure_input)
            .unwrap()
            .is_none()
    );
    finish(f);
    assert!(
        fault_header.is_retired(),
        "the original business holder's actual header must retire with its real fixture exits"
    );
}
