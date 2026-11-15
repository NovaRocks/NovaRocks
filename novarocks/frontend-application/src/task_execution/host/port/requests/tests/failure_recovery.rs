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
use novarocks_control_runtime::{
    actor_turn::{ActorFailureProof, FailureSourceState, StandingInputObligation},
    exit_work::ExitWorkError,
};
use novarocks_control_runtime_core::{completion::SlotState, mailbox::InputClass};
use std::panic::AssertUnwindSafe;

fn fail_attempt(f: &Fixture) -> (ActorFailureProof, StandingInputObligation<FrontendActor>) {
    let position = f
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
    position
        .fill(FrontendInput::LogicalInfrastructureFailed)
        .unwrap();
    f.consume();
    let proof = f
        .runtime
        .failure_proof_for(&f.attempt.registration)
        .unwrap();
    assert!(proof.failed_epoch() > proof.initial_epoch());
    assert!(
        !f.runtime
            .snapshot(f.logical.registration.actor())
            .unwrap()
            .failed
    );
    (proof, position)
}
fn settle_attempt_failure(f: &Fixture, mut position: StandingInputObligation<FrontendActor>) {
    let actor = f.attempt.registration.actor();
    let FailureSourceState::ClosedPending(supervisor) =
        f.runtime.failure_source_state(actor).unwrap()
    else {
        panic!("the actual original AP failure batch");
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

/// Test-only actual exit consumer. P12 must wire the same original job into
/// the AP Source's real launch/Join/report owner; this is not that integration.
async fn retire_failed_job(
    f: &Fixture,
    job: PreparedEffectJob,
    custody: ExitCustody,
    report: ReportSlot,
    observer: WorkRetirementObserver,
) {
    let PreparedEffectJob {
        effect,
        exit,
        mut result,
        captures,
        transport,
    } = job;
    assert!(
        transport.is_none(),
        "this fixture holds the actual report/custody directly"
    );
    assert!(!custody.body_exited() && !custody.captures_exited());
    let outcome = run_future_in_generation(
        exit,
        async move {
            drop((effect, captures));
        },
        PanicSink::new(),
        EPOCH,
    )
    .await;
    assert!(
        matches!(outcome, Err(ExitWorkError::NotStarted(_))),
        "the original failed-generation start is refused"
    );
    assert!(custody.body_exited() && custody.captures_exited());
    assert_eq!(
        report.publication_state().unwrap(),
        SlotState::RevokedUnused
    );
    let mut retired = false;
    for _ in 0..4 {
        if result.retire_ready_after_exit().unwrap() {
            retired = true;
            break;
        }
        if let Some(original) = result.take_failed_payload().unwrap() {
            original.dispose().unwrap();
        }
    }
    assert!(
        retired,
        "the exact original result occurrences retire without replay"
    );
    result.close_after_exit().unwrap();
    assert!(custody.try_retire().unwrap());
    assert!(f.attempt.scope.reap_observed_header(&observer).unwrap());
}

#[tokio::test]
async fn exact_failed_ap_proof_recovers_original_once_and_foreign_numeric_lookalike_is_refused() {
    let f = Fixture::new();
    let foreign = Fixture::new();
    let requests = table(&f);
    let drops = Arc::new(AtomicUsize::new(0));
    let (job, custody, report, observer) = f.job(71, Box::new(Capture(drops.clone())));
    requests.insert(&request(71), job).unwrap();
    let (foreign_proof, foreign_position) = fail_attempt(&foreign);
    assert_eq!(
        foreign.attempt.registration.actor(),
        f.attempt.registration.actor()
    );
    assert_eq!(
        foreign.attempt.registration.scope_id(),
        f.attempt.registration.scope_id()
    );
    assert_eq!(
        requests
            .take_one_after_attempt_failure(&foreign_proof)
            .err(),
        Some(RequestError::WrongFailureProof)
    );
    assert!(!requests.lock().closed && !requests.is_empty());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    settle_attempt_failure(&foreign, foreign_position);
    foreign.finish();
    let (proof, position) = fail_attempt(&f);
    let before = f
        .runtime
        .snapshot(f.attempt.registration.actor())
        .unwrap()
        .inputs;
    let recovered = requests
        .take_one_after_attempt_failure(&proof)
        .unwrap()
        .unwrap();
    assert_eq!(recovered.token, token(71));
    assert_eq!(recovered.action, action(71));
    assert!(recovered.reply_input.is_none());
    assert!(custody.owns_exit(&recovered.job.exit));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(!custody.body_exited() && !custody.captures_exited());
    assert_eq!(
        f.runtime
            .snapshot(f.attempt.registration.actor())
            .unwrap()
            .inputs,
        before
    );
    assert!(requests.is_empty() && !requests.has_capacity());
    assert!(
        requests
            .take_one_after_attempt_failure(&proof)
            .unwrap()
            .is_none()
    );
    retire_failed_job(&f, recovered.job, custody, report, observer).await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    settle_attempt_failure(&f, position);
    f.finish();
}

#[tokio::test]
async fn failed_ap_recovery_retains_reply_loan_and_scan_to_loan_race_requests_original_return_wake()
{
    let f = Fixture::new();
    let wake = Arc::new(Probe::default());
    let requests = AttemptRequests::new(
        activation(),
        f.attempt.registration.clone(),
        f.logical.registration.clone(),
        NonZeroUsize::new(1).unwrap(),
        Waker::from(wake.clone()),
    );
    let drops = Arc::new(AtomicUsize::new(0));
    let (job, custody, report, observer) = f.job(72, Box::new(Capture(drops.clone())));
    requests.insert(&request(72), job).unwrap();
    // Capture the exact descriptor as the publishing scan does, then let the
    // normal reply borrower win before its subsequent loan acquisition.
    let descriptor = requests.lock().rows.get(&token(72)).unwrap().descriptor;
    let loan = requests.loan(descriptor.token, descriptor.kind).unwrap();
    assert!(!requests.lock().host_waiting);
    assert_eq!(
        requests.loan(descriptor.token, descriptor.kind).err(),
        Some(RequestError::ReplyBorrowed)
    );
    assert!(
        requests.lock().host_waiting,
        "the scan-to-loan race must register its waiter"
    );
    let (proof, position) = fail_attempt(&f);
    assert!(
        requests
            .take_one_after_attempt_failure(&proof)
            .unwrap()
            .is_none()
    );
    assert!(!requests.is_empty() && drops.load(Ordering::SeqCst) == 0);
    let before = wake.0.load(Ordering::SeqCst);
    drop(loan);
    assert_eq!(wake.0.load(Ordering::SeqCst), before + 1);
    let recovered = requests
        .take_one_after_attempt_failure(&proof)
        .unwrap()
        .unwrap();
    assert_eq!(recovered.token, descriptor.token);
    assert_eq!(recovered.action, descriptor.action);
    assert!(recovered.reply_input.is_none());
    assert!(custody.owns_exit(&recovered.job.exit));
    retire_failed_job(&f, recovered.job, custody, report, observer).await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    settle_attempt_failure(&f, position);
    f.finish();
}

#[tokio::test]
async fn failed_ap_recovery_preserves_actual_pre_failure_reply_input_and_does_not_fill_again() {
    let mut f = Fixture::new();
    let requests = table(&f);
    let drops = Arc::new(AtomicUsize::new(0));
    let (job, custody, report, observer) = f.job(73, Box::new(Capture(drops.clone())));
    requests.insert(&request(73), job).unwrap();
    let message = reply(73, false);
    let effect = logical_effect(73, message.clone(), &f.logical.payer);
    let ticket = f.emit(true, effect);
    let input = requests
        .deliver_reply_now(&f.runtime, &ticket, message)
        .unwrap();
    f.runtime.effect_completed(ticket).unwrap();
    let (proof, position) = fail_attempt(&f);
    let before = f
        .runtime
        .snapshot(f.attempt.registration.actor())
        .unwrap()
        .inputs;
    let recovered = requests
        .take_one_after_attempt_failure(&proof)
        .unwrap()
        .unwrap();
    assert_eq!(recovered.reply_input, Some(input));
    assert_eq!(recovered.job.result.in_flight().unwrap()[1], Some(input));
    assert_eq!(
        f.runtime
            .snapshot(f.attempt.registration.actor())
            .unwrap()
            .inputs,
        before
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    retire_failed_job(&f, recovered.job, custody, report, observer).await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    settle_attempt_failure(&f, position);
    f.finish();
}

#[tokio::test]
async fn failed_ap_recovery_keeps_unobserved_actual_reply_after_post_commit_caller_unwind() {
    let mut f = Fixture::new();
    let requests = table(&f);
    let drops = Arc::new(AtomicUsize::new(0));
    let (job, custody, report, observer) = f.job(74, Box::new(Capture(drops.clone())));
    requests.insert(&request(74), job).unwrap();
    let message = reply(74, false);
    let effect = logical_effect(74, message.clone(), &f.logical.payer);
    let ticket = f.emit(true, effect);
    let mut actual = None;
    let interrupted =
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut loan = requests
                .loan(token(74), RequestKind::PreReadyReplacement)
                .unwrap();
            actual = Some(loan.job.as_mut().unwrap().result.fill_event_from_effect(
            &f.runtime, &ticket, None,
            FrontendInput::Attempt(Box::new(novarocks_attempt_protocol::input::AttemptInput::Port(
                novarocks_attempt_protocol::port_projection::project_logical_message(&message),
            ))),
        ).unwrap());
            panic!("original LP caller unwind before local reply receipt");
        }));
    assert!(interrupted.is_err());
    assert!(
        requests
            .lock()
            .rows
            .get(&token(74))
            .unwrap()
            .reply_input
            .is_none()
    );
    f.runtime.effect_completed(ticket).unwrap();
    let (proof, position) = fail_attempt(&f);
    let recovered = requests
        .take_one_after_attempt_failure(&proof)
        .unwrap()
        .unwrap();
    assert!(
        recovered.reply_input.is_none(),
        "no receipt is reconstructed from intent"
    );
    assert_eq!(recovered.job.result.in_flight().unwrap()[1], actual);
    assert!(actual.is_some());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    retire_failed_job(&f, recovered.job, custody, report, observer).await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    settle_attempt_failure(&f, position);
    f.finish();
}
