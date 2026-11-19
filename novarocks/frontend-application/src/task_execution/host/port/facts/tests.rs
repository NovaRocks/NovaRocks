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

mod rescan;

use super::super::requests::tests::support::*;
use super::*;
use crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope;
use novarocks_attempt_protocol::effect::{AttemptEffect, EffectBody, EffectClass};
use novarocks_attempt_protocol::port_output::{
    AttemptTerminal as ApTerminal, AttemptToLogical as ApMessage, AttemptToLogicalBody as ApBody,
};
use novarocks_coordination_contract::logical_port::AttemptTerminal;
use std::{sync::atomic::Ordering, task::Waker};
fn terminal(n: u64) -> AttemptToLogical {
    AttemptToLogical {
        activation: activation(),
        action: action(n),
        body: AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Completed),
    }
}
fn terminal_effect(n: u64) -> FrontendEffect {
    FrontendEffect::Attempt(AttemptEffectEnvelope::new(AttemptEffect {
        id: action(n),
        class: EffectClass::Settlement,
        body: EffectBody::Port(ApMessage {
            activation: activation(),
            action: action(n),
            body: ApBody::AttemptTerminal(ApTerminal::Completed),
        }),
    }))
}
fn bank(f: &Fixture) -> (Arc<AttemptFactBank>, Arc<Probe>) {
    let wake = Arc::new(Probe::default());
    let bank = AttemptFactBank::reserve(
        &f.runtime,
        &f.logical.registration,
        &f.attempt.registration,
        activation(),
        &f.logical.work,
        Waker::from(Arc::clone(&wake)),
        Waker::noop().clone(),
    )
    .unwrap();
    (bank, wake)
}
fn finish(f: Fixture, bank: &Arc<AttemptFactBank>) {
    f.consume();
    bank.close_admission();
    assert!(bank.progress(3, true, false).unwrap().complete);
    assert!(bank.is_closed());
    f.finish();
}
#[test]
fn full_fact_position_keeps_original_ticket_and_resumes_in_order_after_actual_retirement() {
    let mut f = Fixture::new();
    let (bank, wake) = bank(&f);
    let first = f.emit(false, terminal_effect(17));
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    let original = loan.publish_now(&f.runtime, &first, terminal(17)).unwrap();
    drop(loan);
    assert!(
        f.logical.seen.lock().unwrap().is_empty(),
        "handoff must not await LP consumption"
    );
    f.runtime.effect_completed(first).unwrap();
    let second = f.emit(false, terminal_effect(18));
    assert!(bank.try_loan(FactRole::Lifecycle).is_none());
    f.runtime.validate_effect_ticket(&second).unwrap();
    let before = wake.0.load(Ordering::SeqCst);
    assert!(bank.progress(1, false, false).unwrap().progressed);
    assert_eq!(wake.0.load(Ordering::SeqCst), before + 1);
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    let next = loan.publish_now(&f.runtime, &second, terminal(18)).unwrap();
    assert_ne!(next, original);
    drop(loan);
    f.runtime.effect_completed(second).unwrap();
    f.consume();
    let seen = f.logical.seen.lock().unwrap();
    let actions = seen
        .iter()
        .filter_map(|input| match input {
            FrontendInput::Logical(input) => match &**input {
                LogicalInput::AttemptPort(message) => Some(message.action),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(actions, [action(17), action(18)]);
    drop(seen);
    bank.progress(3, false, false).unwrap();
    finish(f, &bank);
}
#[test]
fn original_producer_exit_after_intake_close_wakes_lp_then_actual_bank_close_wakes_ap() {
    let f = Fixture::new();
    let ap_wake = Arc::new(Probe::default());
    let lp_wake = Arc::new(Probe::default());
    let bank = AttemptFactBank::reserve(
        &f.runtime,
        &f.logical.registration,
        &f.attempt.registration,
        activation(),
        &f.logical.work,
        Waker::from(ap_wake.clone()),
        Waker::from(lp_wake.clone()),
    )
    .unwrap();
    bank.close_admission();
    assert!(!bank.progress(3, false, false).unwrap().complete);
    let before_lp = lp_wake.0.load(Ordering::SeqCst);
    let before_ap = ap_wake.0.load(Ordering::SeqCst);
    bank.original_producer_exited();
    let producer_woke_lp = lp_wake.0.load(Ordering::SeqCst) == before_lp + 1;
    assert!(
        !bank.is_closed(),
        "producer exit does not retire original positions"
    );
    bank.original_producer_exited();
    let duplicate_kept_lp = lp_wake.0.load(Ordering::SeqCst) == before_lp + 1;
    assert!(!bank.progress(0, true, false).unwrap().complete);
    let before_close_ap = ap_wake.0.load(Ordering::SeqCst);
    assert!(bank.progress(3, true, false).unwrap().complete);
    let close_woke_ap = ap_wake.0.load(Ordering::SeqCst) == before_close_ap + 1;
    assert!(bank.is_closed());
    bank.progress(3, true, false).unwrap();
    let duplicate_kept_ap = ap_wake.0.load(Ordering::SeqCst) == before_close_ap + 1;
    f.finish();
    assert!(producer_woke_lp && duplicate_kept_lp);
    assert_eq!(
        before_close_ap, before_ap,
        "no closure wake before actual retirement"
    );
    assert!(close_woke_ap && duplicate_kept_ap);
}

#[test]
fn failed_receiver_full_resumes_original_ap_only_after_actual_healthy_fact_retirement() {
    for unrecorded_fill in [false, true] {
        let mut f = Fixture::new();
        let (bank, wake) = bank(&f);
        let first = f.emit(false, terminal_effect(17));
        let second = f.emit(false, terminal_effect(18));
        let failure = stage_actual_logical_failure(&f);
        let mut original = bank.try_loan(FactRole::Lifecycle).unwrap();
        if unrecorded_fill {
            // Actual kernel acceptance before its caller records the receipt.
            // Do not synthesize an occurrence or alter the bank's cache.
            f.runtime
                .fill_from_effect(
                    &first,
                    original.position.as_ref().unwrap(),
                    FrontendInput::Logical(Box::new(LogicalInput::AttemptPort(Box::new(
                        terminal(17),
                    )))),
                )
                .unwrap();
            assert!(original.submitted.is_none());
        } else {
            original
                .publish_now(&f.runtime, &first, terminal(17))
                .unwrap();
        }
        drop(original);
        f.runtime.effect_completed(first).unwrap();
        f.runtime
            .poll_one(novarocks_control_runtime_core::identity::Tick(1))
            .unwrap();
        let proof = f
            .runtime
            .failure_proof_for(&f.logical.registration)
            .unwrap();
        bank.close_admission();
        let before = wake.0.load(Ordering::SeqCst);
        for _ in 0..2 {
            assert!(
                bank.try_failed_receiver_loan(&f.runtime, &proof, FactRole::Lifecycle)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                wake.0.load(Ordering::SeqCst),
                before,
                "Full must wait for an actual change"
            );
        }
        assert!(!bank.progress(0, false, true).unwrap().progressed);
        f.runtime.validate_effect_ticket(&second).unwrap();
        assert!(bank.progress(1, false, true).unwrap().progressed);
        let resumed = wake.0.load(Ordering::SeqCst) == before + 1;
        let mut next = bank
            .try_failed_receiver_loan(&f.runtime, &proof, FactRole::Lifecycle)
            .unwrap()
            .unwrap();
        let inputs = f
            .runtime
            .snapshot(f.logical.registration.actor())
            .unwrap()
            .inputs;
        next.publish_now(&f.runtime, &second, &proof, terminal(18))
            .unwrap();
        drop(next);
        assert_eq!(
            f.runtime
                .snapshot(f.logical.registration.actor())
                .unwrap()
                .inputs,
            inputs,
            "failed Core receives no invented input"
        );
        f.runtime.effect_completed(second).unwrap();
        settle_actual_logical_failure(&f, failure);
        let mut complete = false;
        // The earlier one-role visit left this original finite pass at role1.
        // Continue the same bounded cursor, including passive-cell retirement.
        for _ in 0..3 {
            if bank.progress(3, true, true).unwrap().complete {
                complete = true;
                break;
            }
        }
        assert!(complete);
        f.finish();
        assert!(
            resumed,
            "actual old fact retirement must wake the original AP waiter, unrecorded_fill={unrecorded_fill}"
        );
    }
}

#[test]
fn source_fence_returns_original_fact_and_does_not_publish_or_fake_retire() {
    let mut f = Fixture::new();
    let (bank, _) = bank(&f);
    let ticket = f.emit(false, terminal_effect(17));
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    f.attempt.scope.fail().unwrap();
    let refused = loan
        .publish_now(&f.runtime, &ticket, terminal(17))
        .unwrap_err();
    assert!(matches!(refused.error, FactError::Runtime(_)));
    assert_eq!(refused.message.action, action(17));
    drop(loan);
    f.runtime.effect_completed(ticket).unwrap();
    assert!(f.logical.seen.lock().unwrap().is_empty());
    bank.close_admission();
    assert!(!bank.progress(3, false, false).unwrap().complete);
    finish(f, &bank);
}
#[test]
fn independent_pre_ready_role_can_handoff_while_lifecycle_position_is_still_occupied() {
    let mut f = Fixture::new();
    let (bank, _) = bank(&f);
    let first = f.emit(false, terminal_effect(17));
    let mut fact = bank.try_loan(FactRole::Lifecycle).unwrap();
    fact.publish_now(&f.runtime, &first, terminal(17)).unwrap();
    drop(fact);
    f.runtime.effect_completed(first).unwrap();
    let ticket = f.emit(false, ap_effect(18));
    assert!(bank.try_loan(FactRole::Lifecycle).is_none());
    let mut pre_ready = bank.try_loan(FactRole::PreReady).unwrap();
    pre_ready
        .publish_now(&f.runtime, &ticket, request(18))
        .unwrap();
    drop(pre_ready);
    f.runtime.effect_completed(ticket).unwrap();
    f.consume();
    assert_eq!(f.logical.seen.lock().unwrap().len(), 2);
    bank.progress(3, false, false).unwrap();
    finish(f, &bank);
}
#[test]
fn loan_unwind_returns_same_position_and_close_waits_for_that_original_loan() {
    let f = Fixture::new();
    let (bank, _) = bank(&f);
    let loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    let original = loan.position.as_ref().unwrap().id();
    assert!(bank.try_loan(FactRole::Lifecycle).is_none());
    bank.close_admission();
    assert!(!bank.progress(3, true, false).unwrap().complete);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _same = loan;
        panic!("original handoff unwind");
    }));
    assert!(result.is_err());
    assert_eq!(
        bank.lock().slots[0].position.as_ref().unwrap().id(),
        original
    );
    assert!(bank.progress(3, true, false).unwrap().complete);
    f.finish();
}

fn original_evidence() -> Arc<AttemptConvergenceClosure> {
    let participation = novarocks_execution_contract::ParticipationRef::new(
        activation().execution(),
        novarocks_types::FrontendProcessId::try_from_bytes(
            novarocks_execution_contract::TaskOperationId::new_v7().to_bytes(),
        )
        .unwrap(),
        novarocks_types::BackendProcessId::new_v7(),
    );
    Arc::new(AttemptConvergenceClosure::Evidence(vec![(
        participation,
        novarocks_coordination_contract::AttemptParticipationConvergence::NeverEstablished,
    )]))
}
fn convergence(n: u64, closure: Option<Arc<AttemptConvergenceClosure>>) -> AttemptToLogical {
    AttemptToLogical {
        activation: activation(),
        action: action(n),
        body: AttemptToLogicalBody::ConvergenceProgress {
            local_exited: false,
            closure,
            lifecycle_runtime_filters: None,
        },
    }
}
fn convergence_effect(n: u64, closure: Option<Arc<AttemptConvergenceClosure>>) -> FrontendEffect {
    FrontendEffect::Attempt(AttemptEffectEnvelope::new(AttemptEffect {
        id: action(n),
        class: EffectClass::Reply,
        body: EffectBody::Port(ApMessage {
            activation: activation(),
            action: action(n),
            body: ApBody::ConvergenceProgress {
                local_exited: false,
                closure,
                lifecycle_runtime_filters: None,
            },
        }),
    }))
}
fn stage_actual_logical_failure(f: &Fixture) -> StandingInputObligation<FrontendActor> {
    let position = f
        .runtime
        .reserve_admitted_input_for(
            &f.logical.registration,
            f.logical.registration.actor(),
            InputClass::Lifecycle,
            &f.logical.work,
        )
        .unwrap()
        .into_standing_input()
        .unwrap();
    position
        .fill(FrontendInput::LogicalInfrastructureFailed)
        .unwrap();
    position
}
fn settle_actual_logical_failure(
    f: &Fixture,
    mut position: StandingInputObligation<FrontendActor>,
) {
    use novarocks_control_runtime::actor_turn::FailureSourceState;
    let actor = f.logical.registration.actor();
    let FailureSourceState::ClosedPending(supervisor) =
        f.runtime.failure_source_state(actor).unwrap()
    else {
        panic!("the original actual failure supervisor batch");
    };
    assert!(
        f.runtime
            .effect_inventory(actor, supervisor, None, 1)
            .unwrap()
            .is_empty()
    );
    f.runtime.retire_input(actor, supervisor).unwrap();
    assert!(position.retire_ready_after_exit().unwrap());
    position.close_after_exit().unwrap();
    assert!(f.runtime.take_failure_payload(actor).unwrap().is_some());
}
#[test]
fn original_completion_receipt_survives_failed_input_disposal_and_actual_bank_close() {
    let mut f = Fixture::new();
    let (bank, _) = bank(&f);
    let closure = original_evidence();
    let ticket = f.emit(false, convergence_effect(31, Some(closure.clone())));
    // The earlier same-class failure input runs before the AP fact; the fact
    // is nevertheless delivered while the original LP is still healthy.
    let failure = stage_actual_logical_failure(&f);
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    loan.publish_now(&f.runtime, &ticket, convergence(31, Some(closure.clone())))
        .unwrap();
    assert!(Arc::ptr_eq(
        bank.completion_receipt().unwrap().closure(),
        &closure
    ));
    drop(loan);
    f.runtime.effect_completed(ticket).unwrap();
    assert!(
        f.runtime
            .poll_one(novarocks_control_runtime_core::identity::Tick(1))
            .unwrap()
    );
    f.runtime
        .failure_proof_for(&f.logical.registration)
        .unwrap();
    assert!(f.logical.seen.lock().unwrap().is_empty());
    assert!(bank.progress(3, false, true).unwrap().progressed);
    let accepted = bank.completion_receipt().unwrap();
    assert_eq!(accepted.activation(), activation());
    assert_eq!(accepted.action(), action(31));
    assert!(Arc::ptr_eq(accepted.closure(), &closure));
    settle_actual_logical_failure(&f, failure);
    assert!(bank.progress(3, true, true).unwrap().complete);
    assert!(Arc::ptr_eq(
        bank.completion_receipt().unwrap().closure(),
        &closure
    ));
    f.finish();
}
#[test]
fn actual_fill_before_caller_unwind_recovers_prepared_receipt_before_failed_payload_disposal() {
    let mut f = Fixture::new();
    let (bank, _) = bank(&f);
    let closure = original_evidence();
    let ticket = f.emit(false, convergence_effect(32, Some(closure.clone())));
    let failure = stage_actual_logical_failure(&f);
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    let message = convergence(32, Some(closure.clone()));
    loan.prepare_completion(&f.runtime, &ticket, &message)
        .unwrap();
    assert!(bank.completion_receipt().is_none());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // This is the actual kernel commit, not a fabricated InputId or
        // boolean. Force the same cut as a notification unwind before the
        // publish caller records its returned occurrence.
        f.runtime
            .fill_from_effect(
                &ticket,
                loan.position.as_ref().unwrap(),
                FrontendInput::Logical(Box::new(LogicalInput::AttemptPort(Box::new(message)))),
            )
            .unwrap();
        assert!(loan.submitted.is_none());
        panic!("original caller unwind after actual fact fill");
    }));
    assert!(result.is_err());
    drop(loan);
    assert!(bank.completion_receipt().is_none());
    assert!(bank.lock().slots[0].prepared.is_some());
    assert!(
        bank.lock().slots[0]
            .position
            .as_ref()
            .unwrap()
            .in_flight()
            .unwrap()
            .is_some()
    );
    assert!(bank.try_loan(FactRole::Lifecycle).is_none());
    f.runtime.effect_completed(ticket).unwrap();
    assert!(
        f.runtime
            .poll_one(novarocks_control_runtime_core::identity::Tick(1))
            .unwrap()
    );
    f.runtime
        .failure_proof_for(&f.logical.registration)
        .unwrap();
    assert!(bank.progress(3, false, true).unwrap().progressed);
    assert!(bank.lock().slots[0].prepared.is_none());
    let accepted = bank.completion_receipt().unwrap();
    assert_eq!(accepted.action(), action(32));
    assert!(Arc::ptr_eq(accepted.closure(), &closure));
    assert!(f.logical.seen.lock().unwrap().is_empty());
    settle_actual_logical_failure(&f, failure);
    assert!(bank.progress(3, true, true).unwrap().complete);
    f.finish();
}
#[test]
fn completed_receipt_persists_across_closure_none_role_reuse_and_rejects_conflicting_original() {
    let mut f = Fixture::new();
    let (bank, _) = bank(&f);
    let closure = original_evidence();
    let first = f.emit(false, convergence_effect(33, Some(closure.clone())));
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    loan.publish_now(&f.runtime, &first, convergence(33, Some(closure.clone())))
        .unwrap();
    drop(loan);
    f.runtime.effect_completed(first).unwrap();
    f.consume();
    bank.progress(3, false, false).unwrap();
    let next = f.emit(false, convergence_effect(34, None));
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    loan.publish_now(&f.runtime, &next, convergence(34, None))
        .unwrap();
    drop(loan);
    f.runtime.effect_completed(next).unwrap();
    f.consume();
    bank.progress(3, false, false).unwrap();
    assert_eq!(bank.completion_receipt().unwrap().action(), action(33));
    let conflicting = original_evidence();
    let ticket = f.emit(false, convergence_effect(35, Some(conflicting.clone())));
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    let refused = loan
        .publish_now(&f.runtime, &ticket, convergence(35, Some(conflicting)))
        .unwrap_err();
    assert_eq!(refused.error, FactError::ConflictingCompletion);
    assert!(
        loan.position
            .as_ref()
            .unwrap()
            .in_flight()
            .unwrap()
            .is_none()
    );
    drop(loan);
    f.runtime.effect_completed(ticket).unwrap();
    assert!(Arc::ptr_eq(
        bank.completion_receipt().unwrap().closure(),
        &closure
    ));
    finish(f, &bank);
    assert_eq!(bank.completion_receipt().unwrap().action(), action(33));
}
#[test]
fn header_matching_projection_cannot_forge_a_convergence_body_or_replace_the_original_arc() {
    let mut f = Fixture::new();
    let (bank, _) = bank(&f);
    let ticket = f.emit(false, terminal_effect(36));
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    let refused = loan
        .publish_now(
            &f.runtime,
            &ticket,
            convergence(36, Some(original_evidence())),
        )
        .unwrap_err();
    assert_eq!(refused.error, FactError::WrongMessage);
    assert!(bank.completion_receipt().is_none());
    drop(loan);
    f.runtime.effect_completed(ticket).unwrap();
    let closure = original_evidence();
    let ticket = f.emit(false, convergence_effect(37, Some(closure.clone())));
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    let copied = Arc::new((*closure).clone());
    assert_eq!(*copied, *closure);
    assert!(!Arc::ptr_eq(&copied, &closure));
    let refused = loan
        .publish_now(&f.runtime, &ticket, convergence(37, Some(copied)))
        .unwrap_err();
    assert_eq!(refused.error, FactError::WrongMessage);
    let refused = loan
        .publish_now(&f.runtime, &ticket, terminal(37))
        .unwrap_err();
    assert_eq!(refused.error, FactError::WrongMessage);
    assert!(loan.prepared.is_none());
    assert!(
        loan.position
            .as_ref()
            .unwrap()
            .in_flight()
            .unwrap()
            .is_none()
    );
    loan.publish_now(&f.runtime, &ticket, convergence(37, Some(closure.clone())))
        .unwrap();
    assert!(Arc::ptr_eq(
        bank.completion_receipt().unwrap().closure(),
        &closure
    ));
    drop(loan);
    f.runtime.effect_completed(ticket).unwrap();
    finish(f, &bank);
}
#[test]
fn definitely_unsubmitted_failed_destination_clears_prepared_without_granting_a_receipt() {
    let mut f = Fixture::new();
    let (bank, _) = bank(&f);
    let closure = original_evidence();
    let ticket = f.emit(false, convergence_effect(38, Some(closure.clone())));
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    f.logical.scope.fail().unwrap();
    let refused = loan
        .publish_now(&f.runtime, &ticket, convergence(38, Some(closure)))
        .unwrap_err();
    assert_eq!(
        refused.error,
        FactError::Runtime(ActorTurnError::ActorFailed)
    );
    assert!(loan.prepared.is_none());
    assert!(
        loan.position
            .as_ref()
            .unwrap()
            .in_flight()
            .unwrap()
            .is_none()
    );
    assert!(bank.completion_receipt().is_none());
    drop(loan);
    f.runtime.effect_completed(ticket).unwrap();
    finish(f, &bank);
}
#[test]
fn passive_change_notification_is_persistent_and_idle_role_scans_do_not_signal_changes() {
    use std::{
        future::Future,
        task::{Context, Poll},
    };
    let mut f = Fixture::new();
    let (bank, _) = bank(&f);
    let wake = Arc::new(Probe::default());
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut pending = Box::pin(bank.changed());
    assert!(matches!(pending.as_mut().poll(&mut cx), Poll::Pending));
    bank.progress(3, false, false).unwrap();
    assert_eq!(wake.0.load(Ordering::SeqCst), 0);
    let closure = original_evidence();
    let ticket = f.emit(false, convergence_effect(39, Some(closure.clone())));
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    loan.publish_now(&f.runtime, &ticket, convergence(39, Some(closure)))
        .unwrap();
    assert_eq!(wake.0.load(Ordering::SeqCst), 1);
    assert!(matches!(pending.as_mut().poll(&mut cx), Poll::Ready(())));
    drop(pending);
    drop(loan);
    // A change occurring before its new observer is polled remains visible.
    let mut later = Box::pin(bank.changed());
    assert!(matches!(later.as_mut().poll(&mut cx), Poll::Ready(())));
    drop(later);
    f.runtime.effect_completed(ticket).unwrap();
    finish(f, &bank);
}

#[test]
fn original_remote_tracking_ended_receipt_preserves_unknown_instead_of_positive_closure() {
    let mut f = Fixture::new();
    let (bank, _) = bank(&f);
    let evidence = original_evidence();
    let AttemptConvergenceClosure::Evidence(rows) = evidence.as_ref() else {
        unreachable!();
    };
    let closure = Arc::new(AttemptConvergenceClosure::RemoteTrackingEnded {
        exact_participations: vec![rows[0].0].into(),
    });
    let ticket = f.emit(false, convergence_effect(40, Some(closure.clone())));
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    loan.publish_now(&f.runtime, &ticket, convergence(40, Some(closure.clone())))
        .unwrap();
    let receipt = bank.completion_receipt().unwrap();
    assert!(Arc::ptr_eq(receipt.closure(), &closure));
    assert!(
        matches!(receipt.closure().as_ref(), AttemptConvergenceClosure::RemoteTrackingEnded {
        exact_participations } if exact_participations.as_ref() == [rows[0].0])
    );
    drop(loan);
    f.runtime.effect_completed(ticket).unwrap();
    finish(f, &bank);
}

#[test]
fn observer_waker_panic_after_actual_acceptance_keeps_same_receipt_and_original_position() {
    use std::{
        future::Future,
        task::{Context, Poll, Wake},
    };
    struct PanicWake;
    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            panic!("original completion observer wake panic");
        }
    }
    let mut f = Fixture::new();
    let (bank, _) = bank(&f);
    let closure = original_evidence();
    let ticket = f.emit(false, convergence_effect(41, Some(closure.clone())));
    let waker = Waker::from(Arc::new(PanicWake));
    let mut observer = Box::pin(bank.changed());
    assert!(matches!(
        observer.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));
    let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
    let role = loan.position.as_ref().unwrap().id();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        loan.publish_now(&f.runtime, &ticket, convergence(41, Some(closure.clone())))
            .unwrap();
    }));
    assert!(panic.is_err());
    assert!(
        loan.position
            .as_ref()
            .unwrap()
            .in_flight()
            .unwrap()
            .is_some()
    );
    assert!(Arc::ptr_eq(
        bank.completion_receipt().unwrap().closure(),
        &closure
    ));
    drop(observer);
    drop(loan);
    assert_eq!(bank.lock().slots[0].position.as_ref().unwrap().id(), role);
    assert!(bank.lock().slots[0].prepared.is_none());
    f.runtime.effect_completed(ticket).unwrap();
    finish(f, &bank);
    assert_eq!(bank.completion_receipt().unwrap().action(), action(41));
}

#[test]
fn failed_receiver_retains_same_original_proof_with_zero_or_partial_paid_positions() {
    for original_positions in 0..=3 {
        let mut f = std::mem::ManuallyDrop::new(Fixture::new());
        let closure = original_evidence();
        let ticket = f.emit(false, convergence_effect(51, Some(closure.clone())));
        let mut positions = Vec::new();
        for class in [
            InputClass::Lifecycle,
            InputClass::Ordinary,
            InputClass::Ordinary,
        ]
        .into_iter()
        .take(original_positions)
        {
            positions.push(
                f.runtime
                    .reserve_admitted_input_for(
                        &f.logical.registration,
                        f.attempt.registration.actor(),
                        class,
                        &f.logical.work,
                    )
                    .unwrap()
                    .into_standing_input()
                    .unwrap(),
            );
        }
        let failure_position = stage_actual_logical_failure(&f);
        f.runtime
            .poll_one(novarocks_control_runtime_core::identity::Tick(1))
            .unwrap();
        let proof = f
            .runtime
            .failure_proof_for(&f.logical.registration)
            .unwrap();
        let before = f.runtime.snapshot(f.logical.registration.actor()).unwrap();
        let header = f.logical.work.retirement_observer();
        let pins = header.header_holders().unwrap();
        let closure_wake = Arc::new(Probe::default());
        let bank = AttemptFactBank::bind_failed_receiver(
            &f.runtime,
            &f.logical.registration,
            &f.attempt.registration,
            activation(),
            &f.logical.work,
            (Waker::from(closure_wake.clone()), Waker::noop().clone()),
            positions,
        )
        .unwrap();
        assert_eq!(header.header_holders().unwrap().0, pins.0 + 1);
        assert_eq!(
            f.runtime
                .snapshot(f.logical.registration.actor())
                .unwrap()
                .standing_inputs,
            before.standing_inputs,
            "absent original positions stay absent"
        );
        assert!(bank.try_loan(FactRole::Lifecycle).is_none());
        assert!(!bank.is_closed(), "no original producer exit was observed");
        let mut loan = bank
            .try_failed_receiver_loan(&f.runtime, &proof, FactRole::Lifecycle)
            .unwrap()
            .unwrap();
        loan.publish_now(
            &f.runtime,
            &ticket,
            &proof,
            convergence(51, Some(closure.clone())),
        )
        .unwrap();
        assert!(Arc::ptr_eq(
            bank.completion_receipt().unwrap().closure(),
            &closure
        ));
        assert!(
            bank.try_failed_receiver_loan(&f.runtime, &proof, FactRole::PreReady)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            f.runtime
                .snapshot(f.logical.registration.actor())
                .unwrap()
                .inputs,
            before.inputs,
            "passive publication never fills the failed receiver"
        );
        drop(loan);
        f.runtime.effect_completed(ticket).unwrap();
        assert!(!bank.progress(3, false, true).unwrap().complete);
        assert_eq!(
            header.header_holders().unwrap().0,
            pins.0 + 1,
            "a passive handoff does not retire the real producer's header"
        );
        let next = f.emit(false, convergence_effect(52, None));
        let mut loan = bank
            .try_failed_receiver_loan(&f.runtime, &proof, FactRole::Lifecycle)
            .unwrap()
            .unwrap();
        loan.publish_now(&f.runtime, &next, &proof, convergence(52, None))
            .unwrap();
        drop(loan);
        f.runtime.effect_completed(next).unwrap();
        assert!(Arc::ptr_eq(
            bank.completion_receipt().unwrap().closure(),
            &closure
        ));
        settle_actual_logical_failure(&f, failure_position);
        let before_close = closure_wake.0.load(Ordering::SeqCst);
        let mut complete = false;
        for _ in 0..4 {
            complete = bank.progress(3, true, true).unwrap().complete;
            if complete {
                break;
            }
        }
        assert!(complete && bank.is_closed());
        assert!(Arc::ptr_eq(
            bank.completion_receipt().unwrap().closure(),
            &closure
        ));
        let actual_close_wakes = closure_wake.0.load(Ordering::SeqCst) - before_close;
        bank.progress(3, true, true).unwrap();
        let repeated_close_wakes = closure_wake.0.load(Ordering::SeqCst) - before_close;
        std::mem::ManuallyDrop::into_inner(f).finish();
        assert_eq!(actual_close_wakes, 1, "paid positions={original_positions}");
        assert_eq!(
            repeated_close_wakes, 1,
            "paid positions={original_positions}"
        );
    }
}

#[test]
fn reserve_after_original_receiver_failure_uses_prepaid_work_without_allocating_new_roles() {
    let mut f = std::mem::ManuallyDrop::new(Fixture::new());
    let ticket = f.emit(false, terminal_effect(53));
    let failure_position = stage_actual_logical_failure(&f);
    f.runtime
        .poll_one(novarocks_control_runtime_core::identity::Tick(1))
        .unwrap();
    let proof = f
        .runtime
        .failure_proof_for(&f.logical.registration)
        .unwrap();
    let before = f.runtime.snapshot(f.logical.registration.actor()).unwrap();
    let (bank, _) = bank(&f);
    let after = f.runtime.snapshot(f.logical.registration.actor()).unwrap();
    assert_eq!(
        (after.inputs, after.standing_inputs),
        (before.inputs, before.standing_inputs)
    );
    assert!(!bank.is_closed());
    let mut loan = bank
        .try_failed_receiver_loan(&f.runtime, &proof, FactRole::Lifecycle)
        .unwrap()
        .unwrap();
    loan.publish_now(&f.runtime, &ticket, &proof, terminal(53))
        .unwrap();
    drop(loan);
    f.runtime.effect_completed(ticket).unwrap();
    assert!(
        bank.completion_receipt().is_none(),
        "Terminal is not positive remote closure"
    );
    assert!(!bank.progress(3, false, true).unwrap().complete);
    settle_actual_logical_failure(&f, failure_position);
    assert!(bank.progress(3, true, true).unwrap().complete);
    std::mem::ManuallyDrop::into_inner(f).finish();
}

#[test]
fn already_bound_bank_can_handoff_actual_late_proof_without_a_failed_core_input() {
    let mut f = std::mem::ManuallyDrop::new(Fixture::new());
    let (bank, _) = bank(&f);
    let closure = original_evidence();
    let ticket = f.emit(false, convergence_effect(54, Some(closure.clone())));
    let failure_position = stage_actual_logical_failure(&f);
    f.runtime
        .poll_one(novarocks_control_runtime_core::identity::Tick(1))
        .unwrap();
    let proof = f
        .runtime
        .failure_proof_for(&f.logical.registration)
        .unwrap();
    assert!(!bank.progress(3, false, true).unwrap().complete);
    let before = f.runtime.snapshot(f.logical.registration.actor()).unwrap();
    let mut loan = bank
        .try_failed_receiver_loan(&f.runtime, &proof, FactRole::Lifecycle)
        .unwrap()
        .unwrap();
    loan.publish_now(
        &f.runtime,
        &ticket,
        &proof,
        convergence(54, Some(closure.clone())),
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        bank.completion_receipt().unwrap().closure(),
        &closure
    ));
    // The passive read is available while the move-only publishing cell still
    // belongs to the original AP job. No loan return or fake input is needed.
    assert_eq!(
        f.runtime
            .snapshot(f.logical.registration.actor())
            .unwrap()
            .inputs,
        before.inputs
    );
    let pins = f
        .logical
        .work
        .retirement_observer()
        .header_holders()
        .unwrap();
    assert!(
        !bank.progress(3, true, true).unwrap().complete,
        "a producer-exit observation cannot retire a still-held original loan"
    );
    assert_eq!(
        f.logical
            .work
            .retirement_observer()
            .header_holders()
            .unwrap(),
        pins
    );
    assert_eq!(
        f.runtime
            .snapshot(f.logical.registration.actor())
            .unwrap()
            .standing_inputs,
        before.standing_inputs
    );
    drop(loan);
    f.runtime.effect_completed(ticket).unwrap();
    settle_actual_logical_failure(&f, failure_position);
    let mut complete = false;
    for _ in 0..3 {
        complete = bank.progress(3, true, true).unwrap().complete;
        if complete {
            break;
        }
    }
    assert!(complete);
    std::mem::ManuallyDrop::into_inner(f).finish();
}

#[test]
fn failed_receiver_waits_for_original_lifecycle_loan_and_is_notified_when_it_returns() {
    use std::{
        future::Future,
        task::{Context, Poll},
    };
    for target in [FactRole::Lifecycle, FactRole::PreReady] {
        let f = std::mem::ManuallyDrop::new(Fixture::new());
        let (bank, attempt_wake) = bank(&f);
        let original = bank.try_loan(FactRole::Lifecycle).unwrap();
        let failure_position = stage_actual_logical_failure(&f);
        f.runtime
            .poll_one(novarocks_control_runtime_core::identity::Tick(1))
            .unwrap();
        let proof = f
            .runtime
            .failure_proof_for(&f.logical.registration)
            .unwrap();
        bank.close_admission();
        let mut close_notice = Box::pin(bank.changed());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            close_notice.as_mut().poll(&mut cx),
            Poll::Ready(())
        ));
        drop(close_notice);

        // The real failure fence is independent of the original short loan's
        // return. Temporary absence of that anchor must remain backpressure.
        assert!(
            bank.try_failed_receiver_loan(&f.runtime, &proof, target)
                .unwrap()
                .is_none()
        );
        let mut changed = Box::pin(bank.changed());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(changed.as_mut().poll(&mut cx), Poll::Pending));
        assert!(!bank.progress(3, false, true).unwrap().complete);
        assert!(
            matches!(changed.as_mut().poll(&mut cx), Poll::Pending),
            "scanning unrelated original roles cannot consume this loan's waiter"
        );
        let before_return = attempt_wake.0.load(Ordering::SeqCst);
        drop(original);
        let resumed_attempt = attempt_wake.0.load(Ordering::SeqCst) == before_return + 1;
        assert!(matches!(changed.as_mut().poll(&mut cx), Poll::Ready(())));
        drop(changed);
        let loan = bank
            .try_failed_receiver_loan(&f.runtime, &proof, target)
            .unwrap()
            .unwrap();
        drop(loan);
        let before_idle = attempt_wake.0.load(Ordering::SeqCst);
        bank.progress(3, false, true).unwrap();
        let idle_stayed_quiet = attempt_wake.0.load(Ordering::SeqCst) == before_idle;
        settle_actual_logical_failure(&f, failure_position);
        bank.close_admission();
        let mut complete = false;
        for _ in 0..3 {
            complete = bank.progress(3, true, true).unwrap().complete;
            if complete {
                break;
            }
        }
        assert!(complete);
        std::mem::ManuallyDrop::into_inner(f).finish();
        assert!(
            resumed_attempt,
            "returning the original loan must wake the waiting AP source: {target:?}"
        );
        assert!(
            idle_stayed_quiet,
            "an idle scan after the actual loan return must not create another AP wake"
        );
    }
}

#[test]
fn bank_closure_waits_for_actual_prepaid_header_release_after_retirement_is_claimed() {
    let f = std::mem::ManuallyDrop::new(Fixture::new());
    let failure_position = stage_actual_logical_failure(&f);
    f.runtime
        .poll_one(novarocks_control_runtime_core::identity::Tick(1))
        .unwrap();
    let observer = f.logical.work.retirement_observer();
    let before = observer.header_holders().unwrap();
    let (bank, _) = bank(&f);
    assert_eq!(observer.header_holders().unwrap().0, before.0 + 1);
    assert!(!bank.progress(0, true, true).unwrap().complete);
    let original = bank.claim_failed_receiver_retirement().unwrap();
    // The real cell has moved out, but the checked retirement has not released
    // its original pin. Concurrent readers must still observe this duty live.
    assert!(!bank.is_closed());
    assert_eq!(observer.header_holders().unwrap().0, before.0 + 1);
    assert!(!bank.progress(3, true, true).unwrap().complete);
    original.retire().unwrap();
    assert_eq!(observer.header_holders().unwrap(), before);
    assert!(bank.is_closed());
    settle_actual_logical_failure(&f, failure_position);
    std::mem::ManuallyDrop::into_inner(f).finish();
}

#[test]
fn original_root_port_handoff_preserves_the_full_projection_in_both_receiver_states() {
    use novarocks_coordination_contract::{AttemptRootFailure, AttemptRootProjection};
    use novarocks_execution_contract::{TaskIdentity, TaskStatus};
    use novarocks_types::{BackendProcessId, StageId, TaskId};
    for failed in [false, true] {
        let mut f = std::mem::ManuallyDrop::new(Fixture::new());
        let (bank, _) = bank(&f);
        let root = TaskIdentity::new(
            activation().execution(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let original = Box::new(AttemptRootProjection {
            status: TaskStatus::created(root),
            failure: AttemptRootFailure::None,
        });
        let effect = FrontendEffect::Attempt(AttemptEffectEnvelope::new(AttemptEffect {
            id: action(61),
            class: EffectClass::Publication,
            body: EffectBody::Port(ApMessage {
                activation: activation(),
                action: action(61),
                body: ApBody::RootStatus(original.clone()),
            }),
        }));
        let ticket = f.emit(false, effect);
        let FrontendEffect::Attempt(original_effect) = &ticket.effect else {
            unreachable!()
        };
        let original_effect = original_effect.clone();
        let projected = AttemptToLogical {
            activation: activation(),
            action: action(61),
            body: AttemptToLogicalBody::RootStatus(original.clone()),
        };
        let mut wrong = original.clone();
        wrong.failure = AttemptRootFailure::DerivedPending;
        let bad = AttemptToLogical {
            body: AttemptToLogicalBody::RootStatus(wrong),
            ..projected.clone()
        };
        if failed {
            let failure_position = stage_actual_logical_failure(&f);
            f.runtime
                .poll_one(novarocks_control_runtime_core::identity::Tick(1))
                .unwrap();
            let proof = f
                .runtime
                .failure_proof_for(&f.logical.registration)
                .unwrap();
            let mut loan = bank
                .try_failed_receiver_loan(&f.runtime, &proof, FactRole::Lifecycle)
                .unwrap()
                .unwrap();
            assert_eq!(
                loan.publish_now(&f.runtime, &ticket, &proof, bad)
                    .unwrap_err()
                    .error,
                FactError::WrongMessage
            );
            let receipt = loan
                .publish_root_status_now(&f.runtime, &ticket, &proof, projected)
                .unwrap();
            let equivalent = AttemptEffectEnvelope::new(original_effect.protocol.clone());
            assert_eq!(
                receipt.confirm(activation(), &equivalent),
                Err(FactError::WrongMessage),
                "an equivalent wrapper cannot acknowledge the original publication"
            );
            drop(loan);
            assert!(bank.completion_receipt().is_none());
            f.runtime.effect_completed(ticket).unwrap();
            settle_actual_logical_failure(&f, failure_position);
            let mut complete = false;
            for _ in 0..4 {
                complete = bank.progress(3, true, true).unwrap().complete;
                if complete {
                    break;
                }
            }
            assert!(complete && bank.is_closed());
            std::mem::ManuallyDrop::into_inner(f).finish();
        } else {
            let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
            assert_eq!(
                loan.publish_now(&f.runtime, &ticket, bad)
                    .unwrap_err()
                    .error,
                FactError::WrongMessage
            );
            let receipt = loan
                .publish_root_status_now(&f.runtime, &ticket, projected.clone())
                .unwrap();
            receipt.confirm(activation(), &original_effect).unwrap();
            drop(loan);
            assert!(
                f.logical.seen.lock().unwrap().is_empty(),
                "handoff never waits for LP consumption"
            );
            assert!(
                bank.completion_receipt().is_none(),
                "root visibility is not participation closure"
            );
            f.runtime.effect_completed(ticket).unwrap();
            f.consume();
            assert!(
                f.logical
                    .seen
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|input| matches!(input,
                FrontendInput::Logical(input) if matches!(&**input,
                    LogicalInput::AttemptPort(message) if matches!(&message.body,
                        AttemptToLogicalBody::RootStatus(value) if value == &original))))
            );
            bank.progress(3, false, false).unwrap();
            finish(std::mem::ManuallyDrop::into_inner(f), &bank);
        }
    }
}

#[test]
fn current_ticket_prepaid_position_converts_after_receiver_failure_with_another_role_loaned() {
    for closing in [false, true] {
        let mut f = std::mem::ManuallyDrop::new(Fixture::new());
        let (bank, _) = bank(&f);
        let lifecycle = bank.try_loan(FactRole::Lifecycle).unwrap();
        let ticket = f.emit(false, ap_effect(62));
        let mut original = bank.try_loan(FactRole::PreReady).unwrap();
        let id = original.position.as_ref().unwrap().id();
        let failure_position = stage_actual_logical_failure(&f);
        f.runtime
            .poll_one(novarocks_control_runtime_core::identity::Tick(1))
            .unwrap();
        let proof = f
            .runtime
            .failure_proof_for(&f.logical.registration)
            .unwrap();
        if closing {
            bank.close_admission();
        }
        let refused = original
            .publish_now(&f.runtime, &ticket, request(62))
            .unwrap_err();
        assert!(matches!(
            refused.error,
            FactError::Closed | FactError::Runtime(ActorTurnError::ActorFailed)
        ));
        let before = f.runtime.snapshot(f.logical.registration.actor()).unwrap();
        let mut failed = original.into_failed_receiver(&f.runtime, &proof).unwrap_or_else(|_| panic!("same unsubmitted original position converts without waiting for Lifecycle reader"));
        let after = f.runtime.snapshot(f.logical.registration.actor()).unwrap();
        assert_eq!(
            (before.inputs, before.standing_inputs),
            (after.inputs, after.standing_inputs)
        );
        f.runtime.validate_effect_ticket(&ticket).unwrap();
        failed
            .publish_now(&f.runtime, &ticket, &proof, refused.message)
            .unwrap();
        drop(failed);
        assert_eq!(
            bank.lock().slots[FactRole::PreReady.index()]
                .position
                .as_ref()
                .unwrap()
                .id(),
            id
        );
        assert!(
            bank.completion_receipt().is_none(),
            "receiver failure grants no positive closure"
        );
        f.runtime.effect_completed(ticket).unwrap();
        drop(lifecycle);
        settle_actual_logical_failure(&f, failure_position);
        bank.close_admission();
        let mut complete = false;
        for _ in 0..4 {
            complete = bank.progress(3, true, true).unwrap().complete;
            if complete {
                break;
            }
        }
        assert!(complete && bank.is_closed());
        std::mem::ManuallyDrop::into_inner(f).finish();
    }
}

#[tokio::test]
async fn original_ap_failure_session_waits_for_its_own_credit_without_stealing_lp_notification() {
    use novarocks_control_runtime::{
        actor_turn::FailureSourceState,
        exit_custody::ExitCustody,
        exit_work::{PanicSink, run_future},
    };
    use std::{
        future::Future,
        task::{Context, Poll},
    };
    for receiver_failed in [false, true] {
        let (fixture, failure_work) = Fixture::with_attempt_failure_work();
        let mut f = std::mem::ManuallyDrop::new(fixture);
        let (bank, _) = bank(&f);
        let old = f.emit(false, terminal_effect(81));
        let mut loan = bank.try_loan(FactRole::Lifecycle).unwrap();
        loan.publish_now(&f.runtime, &old, terminal(81)).unwrap();
        drop(loan);
        f.runtime.effect_completed(old).unwrap();
        let marker = f
            .runtime
            .reserve_admitted_input_for(
                &f.attempt.registration,
                f.attempt.registration.actor(),
                InputClass::Lifecycle,
                &f.attempt.work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap();
        marker
            .fill(FrontendInput::LogicalInfrastructureFailed)
            .unwrap();
        let logical_failure = receiver_failed.then(|| stage_actual_logical_failure(&f));
        f.consume();
        let actor = f.attempt.registration.actor();
        let FailureSourceState::ClosedPending(input) =
            f.runtime.failure_source_state(actor).unwrap()
        else {
            panic!("same original AP failure batch")
        };
        let descriptor = f.runtime.effect_inventory(actor, input, None, 1).unwrap()[0]
            .0
            .id;
        let ticket = f.runtime.take_effect(actor, input, descriptor).unwrap();
        let header = failure_work.work().retirement_observer();
        let report = failure_work.report().clone();
        let (custody, exit) = ExitCustody::from_admitted(failure_work);
        let lost = AttemptToLogical {
            activation: activation(),
            action: action(descriptor.get()),
            body: AttemptToLogicalBody::AttemptLost {
                failure: novarocks_coordination_contract::ExecutionFailure::from_input(
                    novarocks_attempt_protocol::port_projection::owner_failure(),
                ),
                unknown_participations: Box::new([]),
                unresolved_actions: vec![action(81)].into_boxed_slice(),
            },
        };
        let cx = &mut Context::from_waker(Waker::noop());
        let mut prior_notice = Box::pin(bank.changed());
        assert!(matches!(prior_notice.as_mut().poll(cx), Poll::Ready(())));
        drop(prior_notice);
        let mut lp_notice = Box::pin(bank.changed());
        assert!(matches!(lp_notice.as_mut().poll(cx), Poll::Pending));
        let runtime = f.runtime.clone();
        let bank_view = bank.clone();
        let original_ticket = &ticket;
        let mut publisher = Box::pin(run_future(
            exit,
            async move {
                let mut session = runtime.claim_failure_publication(original_ticket).unwrap();
                bank_view
                    .publish_owner_lost(&runtime, &mut session, lost)
                    .await
                    .unwrap();
            },
            PanicSink::new(),
        ));
        assert!(
            matches!(publisher.as_mut().poll(cx), Poll::Pending),
            "the original occupied Lifecycle role remains Full"
        );
        assert!(
            matches!(lp_notice.as_mut().poll(cx), Poll::Pending),
            "AP failure publisher cannot consume or invent the LP notice"
        );
        let mut completed = false;
        for _ in 0..8 {
            bank.progress(3, false, receiver_failed).unwrap();
            match publisher.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    result.unwrap();
                    completed = true;
                    break;
                }
                Poll::Pending => f.consume(),
            }
        }
        assert!(
            completed,
            "original credit return resumes the SAME failure session"
        );
        assert!(
            matches!(lp_notice.as_mut().poll(cx), Poll::Ready(())),
            "both original observers receive independent notifications"
        );
        drop(lp_notice);
        drop(publisher);
        f.runtime.effect_completed(ticket).unwrap();
        f.runtime.retire_input(actor, input).unwrap();
        let mut marker = marker;
        assert!(marker.retire_ready_after_exit().unwrap());
        marker.close_after_exit().unwrap();
        assert!(f.runtime.take_failure_payload(actor).unwrap().is_some());
        report.consume(OBSERVER).unwrap();
        assert!(custody.try_retire().unwrap());
        assert!(f.attempt.scope.reap_observed_header(&header).unwrap());
        f.consume();
        if receiver_failed {
            settle_actual_logical_failure(&f, logical_failure.unwrap());
        } else {
            assert_eq!(f.logical.seen.lock().unwrap().iter().filter(|input| matches!(input, FrontendInput::Logical(input) if matches!(input.as_ref(), LogicalInput::AttemptPort(message) if matches!(&message.body, AttemptToLogicalBody::AttemptLost { unresolved_actions, .. } if unresolved_actions.as_ref() == [action(81)]) && message.action == action(descriptor.get())))).count(), 1);
        }
        assert!(
            bank.completion_receipt().is_none(),
            "AttemptLost does not create positive closure"
        );
        bank.close_admission();
        let mut complete = false;
        for _ in 0..4 {
            complete = bank.progress(3, true, receiver_failed).unwrap().complete;
            if complete {
                break;
            }
        }
        assert!(complete && bank.is_closed());
        std::mem::ManuallyDrop::into_inner(f).finish();
    }
}
