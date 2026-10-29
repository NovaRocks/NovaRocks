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

//! Actual prepaid Context continuation custody. Neutral close effects below
//! are component-model publications, never Native I/O or physical Task exits.

use super::*;
use crate::control_runtime::{
    actors::{WorkerMessage, tests::task},
    context_host::{ContextInternalAdvance, failure::FailurePublicationError},
};
use novarocks_control_runtime::driver::InputPhase;
use novarocks_control_runtime_core::identity::ActionId;

fn take_wake(f: &mut Harness, input: InputId) -> EffectTicket<WorkerAction> {
    let mut wake = None;
    for action in f
        .runtime
        .ready_effects(
            context_actor(),
            input,
            crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
        )
        .unwrap()
    {
        let ticket = f
            .runtime
            .take_effect(context_actor(), input, action)
            .unwrap();
        if matches!(
            ticket.effect,
            WorkerAction::Context(ContextEffect::WakeInternal)
        ) {
            assert!(
                wake.replace(ticket).is_none(),
                "one coalesced wake per real batch"
            );
        } else {
            assert!(matches!(
                ticket.effect,
                WorkerAction::Context(
                    ContextEffect::CloseExternalAdmission { .. } | ContextEffect::SealRoot { .. }
                )
            ));
            f.runtime.effect_completed(ticket).unwrap();
        }
    }
    wake.expect("original Core has a bounded close scan to resume")
}
fn handoff(f: &Harness, wake: EffectTicket<WorkerAction>) {
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .handoff_internal_wake(wake)
        .unwrap();
}
fn next(f: &Harness) -> ContextInternalAdvance {
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_internal()
        .unwrap()
}
fn seed(f: &mut Harness) -> EffectTicket<WorkerAction> {
    let input = model_fact(f, ContextInput::OwnerFenced, 2);
    take_wake(f, input)
}
fn begin_failure(f: &Harness, input: InputId) {
    let failure = f
        .runtime
        .take_effect(context_actor(), input, ActionId::new(1).unwrap())
        .unwrap();
    assert!(matches!(failure.effect, WorkerAction::OwnerFailed { .. }));
    let address = f.service.context(f.identity).unwrap();
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
}
fn fault_input(f: &Harness) -> InputId {
    let work = f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .resources
        .roles[TIMER]
        .work
        .id();
    f.runtime
        .reserve_input(
            context_actor(),
            ActorId::new(88).unwrap(),
            InputClass::Control,
        )
        .unwrap()
        .fill(WorkerMessage::Context(ContextMessage::Command {
            work,
            request: None,
            command: ContextInput::Installed {
                task: task(f.identity, 999),
            },
        }))
        .unwrap()
}

#[test]
fn coalesced_context_hint_uses_original_prepaid_role_and_waits_for_its_real_old_batch() {
    let mut f = original_context(1, StorageNeed::default());
    let work_before = f.ledger.used(Dimension::Work.id());
    let inputs_before = f.ledger.used(Dimension::Inputs.id());
    let effects_before = f.ledger.used(Dimension::Effects.id());
    let wake1 = seed(&mut f);
    let first_seed = wake1.input;
    let wake2 = seed(&mut f);
    let second_seed = wake2.input;
    handoff(&f, wake1);
    handoff(&f, wake2);
    f.runtime.retire_input(context_actor(), first_seed).unwrap();
    f.runtime
        .retire_input(context_actor(), second_seed)
        .unwrap();
    let first = match next(&f) {
        ContextInternalAdvance::Submitted(input) => input,
        other => panic!("original paid hint: {other:?}"),
    };
    assert_eq!(next(&f), ContextInternalAdvance::Waiting(first));
    assert!(f.runtime.poll_one(Tick(3)).unwrap());
    assert_eq!(next(&f), ContextInternalAdvance::Waiting(first));
    complete_model_effects(&mut f, first);
    assert_eq!(next(&f), ContextInternalAdvance::Idle);
    assert_eq!(next(&f), ContextInternalAdvance::Idle);
    assert_eq!(f.ledger.used(Dimension::Work.id()), work_before);
    assert_eq!(f.ledger.used(Dimension::Inputs.id()), inputs_before);
    assert_eq!(f.ledger.used(Dimension::Effects.id()), effects_before);
    assert!(
        !f.service
            .context(f.identity)
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
fn pending_only_context_hint_is_withdrawn_by_real_failure_stage_without_an_input_or_exit() {
    let mut f = original_context(1, StorageNeed::default());
    let wake = seed(&mut f);
    let input = wake.input;
    handoff(&f, wake);
    f.runtime.retire_input(context_actor(), input).unwrap();
    let before = f.ledger.used(Dimension::Work.id());
    let bad = fault_input(&f);
    assert!(f.runtime.poll_one(Tick(9)).unwrap());
    assert_eq!(next(&f), ContextInternalAdvance::AwaitingFailurePublication);
    begin_failure(&f, bad);
    assert_eq!(next(&f), ContextInternalAdvance::Idle);
    assert_eq!(
        f.ledger.used(Dimension::Work.id()),
        before,
        "withdrawing a pure hint grants no Work exit"
    );
    finish(f);
}

#[test]
fn queued_internal_input_needs_actual_failed_abandonment_before_role_reuse_or_close() {
    let mut f = original_context(1, StorageNeed::default());
    let wake = seed(&mut f);
    let seed_input = wake.input;
    handoff(&f, wake);
    f.runtime.retire_input(context_actor(), seed_input).unwrap();
    let bad = fault_input(&f);
    let queued = match next(&f) {
        ContextInternalAdvance::Submitted(input) => input,
        other => panic!("queued original: {other:?}"),
    };
    assert!(f.runtime.poll_one(Tick(9)).unwrap());
    assert!(f.runtime.snapshot(context_actor()).unwrap().failed);
    assert_eq!(
        f.runtime
            .input_custody(context_actor(), queued)
            .unwrap()
            .unwrap()
            .phase,
        InputPhase::Filled
    );
    begin_failure(&f, bad);
    assert_eq!(next(&f), ContextInternalAdvance::Waiting(queued));
    let address = f.service.context(f.identity).unwrap();
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
            .input_custody(context_actor(), queued)
            .unwrap()
            .unwrap()
            .phase,
        InputPhase::AbandonedByOwnerFailure
    );
    drop(host);
    assert_eq!(next(&f), ContextInternalAdvance::Idle);
    assert!(
        f.runtime
            .input_custody(context_actor(), queued)
            .unwrap()
            .is_none()
    );
    finish(f);
}

#[test]
fn source_close_waits_for_an_unhanded_original_wake_even_after_other_metadata_sources_end() {
    let mut f = original_context(1, StorageNeed::default());
    let before = f.ledger.used(Dimension::Work.id());
    let wake = seed(&mut f);
    let seed_input = wake.input;
    {
        let address = f.service.context(f.identity).unwrap();
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
    f.runtime.retire_input(context_actor(), seed_input).unwrap();
    let continuation = match next(&f) {
        ContextInternalAdvance::Submitted(input) => input,
        other => panic!("drain the actual prior hint: {other:?}"),
    };
    assert!(f.runtime.poll_one(Tick(3)).unwrap());
    complete_model_effects(&mut f, continuation);
    assert_eq!(next(&f), ContextInternalAdvance::Idle);
    assert!(
        f.service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .retire_internal_source()
            .unwrap()
    );
    assert_eq!(next(&f), ContextInternalAdvance::Closed);
    assert_eq!(
        f.ledger.used(Dimension::Work.id()),
        before,
        "closing a metadata role does not exit its permanent author"
    );
    finish(f);
}
