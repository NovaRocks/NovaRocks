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
    context_host::{TIMER, failure::FailurePublicationError},
};
use novarocks_control_runtime::driver::InputPhase;
use novarocks_control_runtime_core::identity::ActionId;

#[test]
fn queued_submitted_request_is_woken_by_actual_failed_input_take_and_stays_paid_until_delivery_ends()
 {
    let f = original_context(1, StorageNeed::default());
    let ready = ready(&f, 1);
    let scope = f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .id();
    let fault_work = f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .resources
        .roles[TIMER]
        .work
        .id();
    let unknown = task(f.identity, 999);
    // Fill, but do not poll, this actual Control input. The subsequent R is
    // filled into the same class, so the real mailbox FIFO encounters it later.
    let bad = f
        .runtime
        .reserve_input(
            context_actor(),
            ActorId::new(89).unwrap(),
            InputClass::Control,
        )
        .unwrap()
        .fill(WorkerMessage::Context(ContextMessage::Command {
            work: fault_work,
            request: None,
            command: ContextInput::Installed { task: unknown },
        }))
        .unwrap();
    let (work, mut handle) = f
        .service
        .context(f.identity)
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
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .submit_request(work, renew())
        .unwrap();
    f.runtime.poll_one(Tick(9)).unwrap();
    assert!(f.runtime.snapshot(context_actor()).unwrap().failed);
    assert!(
        f.runtime
            .input_custody(context_actor(), request.token)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::Filled),
        "the actual later R remains queued when the earlier Control input fails"
    );
    let failure = f
        .runtime
        .take_effect(context_actor(), bad, ActionId::new(1).unwrap())
        .unwrap();
    assert!(matches!(failure.effect, WorkerAction::OwnerFailed { .. }));
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(failure)
        .unwrap();
    assert!(matches!(
        f.service
            .context(f.identity)
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
            .context(f.identity)
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
        ContextReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed))
    ));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);

    // Consume the original publication wake. A blocked R is still in its
    // real inventory; an empty ready queue proves only that this wake was used.
    let mut old_notices = 0;
    for _ in 0..4 {
        let Some((actual_scope, actual_work)) = ready.next_context_ready() else {
            break;
        };
        assert_eq!((actual_scope, actual_work), (scope, work));
        assert!(
            !f.service
                .context(f.identity)
                .unwrap()
                .lock()
                .unwrap()
                .advance_request(actual_work)
                .unwrap()
        );
        old_notices += 1;
    }
    assert!(old_notices > 0);
    assert!(ready.next_context_ready().is_none());
    assert!(
        f.runtime
            .input_custody(context_actor(), request.token)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::Filled)
    );
    let zero = f
        .service
        .context(f.identity)
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
            .input_custody(context_actor(), request.token)
            .unwrap()
            .is_some_and(|input| input.phase == InputPhase::Filled)
    );
    assert!(ready.next_context_ready().is_none());

    let mut took_original = false;
    for _ in 0..8 {
        let progress = f
            .service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_failed_runtime_inputs(1)
            .unwrap();
        assert!(progress.units <= 1);
        assert!(progress.taken <= progress.units);
        if f.runtime
            .input_custody(context_actor(), request.token)
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
        .next_context_ready()
        .expect("taking this exact queued R input must notify its original address again");
    assert_eq!(awakened, (scope, work));
    assert!(
        !f.service
            .context(f.identity)
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
            .next_context_ready()
            .expect("the actual external delivery end wakes the same original R");
        assert_eq!(original, (scope, work));
        if f.service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_request(original.1)
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
            .context(f.identity)
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
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_failed_runtime_inputs(0)
        .unwrap();
    assert!(final_observation.empty);
    assert_eq!((final_observation.units, final_observation.taken), (0, 0));
    assert!(ready.next_context_ready().is_none());
    finish(f);
}
