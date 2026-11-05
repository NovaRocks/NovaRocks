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

//! Actual prepaid Participation continuation custody. Neutral close effects below
//! are component-model publications, never Native I/O or physical Task exits.

use super::*;
use crate::control_runtime::{
    actors::{WorkerMessage, tests::task},
    participation_host::{ParticipationInternalAdvance, failure::FailurePublicationError},
};
use novarocks_control_runtime::actor_turn::InputPhase;

fn take_wake(f: &mut Harness, input: InputId) -> EffectTicket<WorkerAction> {
    let mut wake = None;
    for action in f
        .runtime
        .ready_effects(
            participation_actor(),
            input,
            crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
        )
        .unwrap()
    {
        let ticket = f
            .runtime
            .take_effect(participation_actor(), input, action)
            .unwrap();
        if matches!(
            ticket.effect,
            WorkerAction::Participation(ParticipationEffect::WakeInternal)
        ) {
            assert!(
                wake.replace(ticket).is_none(),
                "one coalesced wake per real batch"
            );
        } else {
            assert!(matches!(
                ticket.effect,
                WorkerAction::Participation(
                    ParticipationEffect::CloseExternalAdmission { .. }
                        | ParticipationEffect::SealRoot { .. }
                )
            ));
            f.runtime.effect_completed(ticket).unwrap();
        }
    }
    wake.expect("original Core has a bounded close scan to resume")
}
fn handoff(f: &Harness, wake: EffectTicket<WorkerAction>) {
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .handoff_internal_wake(wake)
        .unwrap();
}
fn next(f: &Harness) -> ParticipationInternalAdvance {
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_internal()
        .unwrap()
}
fn seed(f: &mut Harness) -> EffectTicket<WorkerAction> {
    let input = model_fact(f, ParticipationInput::OwnerFenced, 2);
    take_wake(f, input)
}
fn begin_failure(f: &Harness, input: InputId) -> InputId {
    let failure = super::failure_publications::take_actual_failure(f, input);
    let failure_input = failure.input;
    assert!(matches!(failure.effect, WorkerAction::OwnerFailed { .. }));
    let address = f.service.participation(f.identity).unwrap();
    let mut host = address.lock().unwrap();
    host.begin_failure_publications(failure).unwrap();
    assert!(matches!(
        host.discard_failed_internal_hint(),
        Err(FailurePublicationError::NotDrained)
    ));
    let mut drained = false;
    for _ in 0..128 {
        if host.advance_failure_publications(1).unwrap() {
            drained = true;
            break;
        }
    }
    assert!(
        drained,
        "actual original failure publication session drains"
    );
    host.discard_failed_internal_hint().unwrap();
    failure_input
}
fn fault_input(f: &Harness) -> InputId {
    let work = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .resources
        .metadata_resources[TIMER]
        .work
        .reference();
    {
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
            work,
            request: None,
            command: ParticipationInput::Installed {
                task: task(f.identity, 999),
            },
        },
    ))
    .unwrap()
}

#[test]
fn coalesced_participation_hint_uses_original_prepaid_role_and_waits_for_its_real_old_batch() {
    let mut f = original_participation(1);
    let work_before = actual_component_inventory(&f);
    let inputs_before = actual_component_inventory(&f);
    let effects_before = actual_component_inventory(&f);
    let wake1 = seed(&mut f);
    let first_seed = wake1.input;
    let wake2 = seed(&mut f);
    let second_seed = wake2.input;
    handoff(&f, wake1);
    handoff(&f, wake2);
    f.runtime
        .retire_input(participation_actor(), first_seed)
        .unwrap();
    f.runtime
        .retire_input(participation_actor(), second_seed)
        .unwrap();
    let first = match next(&f) {
        ParticipationInternalAdvance::Submitted(input) => input,
        other => panic!("original paid hint: {other:?}"),
    };
    assert_eq!(next(&f), ParticipationInternalAdvance::Waiting(first));
    assert!(f.runtime.poll_one(Tick(3)).unwrap());
    assert_eq!(next(&f), ParticipationInternalAdvance::Waiting(first));
    complete_model_effects(&mut f, first);
    assert_eq!(next(&f), ParticipationInternalAdvance::Idle);
    assert_eq!(next(&f), ParticipationInternalAdvance::Idle);
    assert_eq!(actual_component_inventory(&f), work_before);
    assert_eq!(actual_component_inventory(&f), inputs_before);
    assert_eq!(actual_component_inventory(&f), effects_before);
    assert!(
        !f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .retire_internal_source()
            .unwrap(),
        "empty self-source cannot close while the parent may still issue work"
    );
    finish(f);
}

#[test]
fn pending_only_participation_hint_is_withdrawn_by_real_failure_stage_without_an_input_or_exit() {
    let mut f = original_participation(1);
    let wake = seed(&mut f);
    let input = wake.input;
    handoff(&f, wake);
    f.runtime
        .retire_input(participation_actor(), input)
        .unwrap();
    let before = actual_component_inventory(&f);
    let bad = fault_input(&f);
    assert!(f.runtime.poll_one(Tick(9)).unwrap());
    assert_eq!(
        next(&f),
        ParticipationInternalAdvance::AwaitingFailurePublication
    );
    let failure_input = begin_failure(&f, bad);
    assert_eq!(next(&f), ParticipationInternalAdvance::Idle);
    assert_eq!(
        actual_component_inventory(&f),
        before,
        "withdrawing a pure hint grants no Work exit"
    );
    super::failure_publications::finish_failed_fixture(f, failure_input);
}

#[test]
fn queued_internal_input_needs_actual_failed_abandonment_before_role_reuse_or_close() {
    let mut f = original_participation(1);
    let wake = seed(&mut f);
    let seed_input = wake.input;
    handoff(&f, wake);
    f.runtime
        .retire_input(participation_actor(), seed_input)
        .unwrap();
    let bad = fault_input(&f);
    let queued = match next(&f) {
        ParticipationInternalAdvance::Submitted(input) => input,
        other => panic!("queued original: {other:?}"),
    };
    assert!(f.runtime.poll_one(Tick(9)).unwrap());
    assert!(f.runtime.snapshot(participation_actor()).unwrap().failed);
    assert_eq!(
        f.runtime
            .input_custody(participation_actor(), queued)
            .unwrap()
            .unwrap()
            .phase,
        InputPhase::Filled
    );
    let failure_input = begin_failure(&f, bad);
    assert_eq!(next(&f), ParticipationInternalAdvance::Waiting(queued));
    let address = f.service.participation(f.identity).unwrap();
    let mut host = address.lock().unwrap();
    let mut taken = false;
    for _ in 0..16 {
        let progress = host.advance_failed_runtime_inputs(1).unwrap();
        taken |= progress.taken > 0;
        if progress.empty {
            break;
        }
    }
    assert!(taken);
    assert_eq!(
        f.runtime
            .input_custody(participation_actor(), queued)
            .unwrap()
            .unwrap()
            .phase,
        InputPhase::AbandonedByOwnerFailure
    );
    drop(host);
    assert_eq!(next(&f), ParticipationInternalAdvance::Idle);
    assert!(
        f.runtime
            .input_custody(participation_actor(), queued)
            .unwrap()
            .is_none()
    );
    super::failure_publications::finish_failed_fixture(f, failure_input);
}

#[test]
fn source_close_waits_for_an_unhanded_original_wake_even_after_other_metadata_sources_end() {
    let mut f = original_participation(1);
    let before = actual_component_inventory(&f);
    let wake = seed(&mut f);
    let seed_input = wake.input;
    {
        let address = f.service.participation(f.identity).unwrap();
        let mut host = address.lock().unwrap();
        // This serial component driver explicitly ends its original parent
        // source. The production Root authorization is tested by M/R/H cases.
        assert!(host.resources.owner_control.close_for_teardown().unwrap());
        assert!(
            !host.resources.internal.close_for_teardown().unwrap(),
            "the real unhanded Wake batch prevents an early last-role close"
        );
    }
    // The old original hint still transfers after future timer production ends.
    handoff(&f, wake);
    f.runtime
        .retire_input(participation_actor(), seed_input)
        .unwrap();
    let continuation = match next(&f) {
        ParticipationInternalAdvance::Submitted(input) => input,
        other => panic!("drain the actual prior hint: {other:?}"),
    };
    assert!(f.runtime.poll_one(Tick(3)).unwrap());
    complete_model_effects(&mut f, continuation);
    assert_eq!(next(&f), ParticipationInternalAdvance::Idle);
    assert!(
        f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .retire_internal_source()
            .unwrap()
    );
    assert_eq!(next(&f), ParticipationInternalAdvance::Closed);
    assert_eq!(
        actual_component_inventory(&f),
        before,
        "closing a metadata role does not exit its permanent author"
    );
    finish(f);
}
