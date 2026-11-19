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

mod failure_recovery;
pub(crate) mod support;
use super::*;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::ExitCustody,
    exit_work::{PanicSink, run_future_in_generation},
    obligation::WorkRetirementObserver,
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    task::Waker,
};
use support::*;
struct Capture(Arc<AtomicUsize>);
impl Drop for Capture {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
async fn retire_job(
    f: &Fixture,
    job: PreparedEffectJob,
    custody: ExitCustody,
    report: ReportSlot,
    observer: WorkRetirementObserver,
) {
    let PreparedEffectJob {
        exit,
        mut result,
        captures,
        effect,
        transport,
    } = job;
    assert!(transport.is_none());
    drop(effect);
    run_future_in_generation(
        exit,
        async move {
            drop(captures);
        },
        PanicSink::new(),
        EPOCH,
    )
    .await
    .unwrap();
    assert!(custody.body_exited() && custody.captures_exited());
    f.consume();
    assert!(result.retire_ready_after_exit().unwrap());
    result.close_after_exit().unwrap();
    report.consume(OBSERVER).unwrap();
    assert!(custody.try_retire().unwrap());
    assert!(f.attempt.scope.reap_observed_header(&observer).unwrap());
}
fn table(f: &Fixture) -> Arc<AttemptRequests> {
    AttemptRequests::new(
        activation(),
        f.attempt.registration.clone(),
        f.logical.registration.clone(),
        NonZeroUsize::new(1).unwrap(),
        Waker::noop().clone(),
    )
}
#[tokio::test]
async fn original_job_result_and_exit_survive_until_one_exact_fenced_reply() {
    let mut f = Fixture::new();
    let requests = table(&f);
    let drops = Arc::new(AtomicUsize::new(0));
    let (job, custody, report, observer) = f.job(17, Box::new(Capture(Arc::clone(&drops))));
    requests.insert(&request(17), job).unwrap();
    assert!(!requests.is_empty() && drops.load(Ordering::SeqCst) == 0);
    let message = reply(17, false);
    let effect = logical_effect(17, message.clone(), &f.logical.payer);
    let ticket = f.emit(true, effect);
    let input = requests
        .deliver_reply_now(&f.runtime, &ticket, message.clone())
        .unwrap();
    assert!(
        f.attempt.seen.lock().unwrap().is_empty(),
        "synchronous reply handoff never waits for AP consumption"
    );
    assert_eq!(
        requests
            .deliver_reply_now(&f.runtime, &ticket, message)
            .unwrap_err()
            .error,
        RequestError::DuplicateReply
    );
    let replied = requests.take_replied(token(17)).unwrap();
    assert_eq!(replied.reply_input, input);
    assert_eq!(replied.token, token(17));
    assert!(custody.owns_exit(&replied.job.exit));
    assert!(!custody.body_exited() && !custody.captures_exited());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "a logical reply does not destroy original rich captures"
    );
    f.runtime.effect_completed(ticket).unwrap();
    retire_job(&f, replied.job, custody, report, observer).await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(requests.is_empty());
    f.finish();
}
#[tokio::test]
async fn changed_reply_payload_or_token_cannot_borrow_an_unrelated_current_ticket() {
    let mut f = Fixture::new();
    let requests = table(&f);
    let (job, custody, report, observer) = f.job(17, Box::new(()));
    requests.insert(&request(17), job).unwrap();
    let original = reply(17, false);
    let effect = logical_effect(17, original.clone(), &f.logical.payer);
    let ticket = f.emit(true, effect);
    assert_eq!(
        requests
            .deliver_reply_now(&f.runtime, &ticket, reply(17, true))
            .unwrap_err()
            .error,
        RequestError::WrongReply
    );
    assert_eq!(
        requests
            .deliver_reply_now(&f.runtime, &ticket, reply(18, false))
            .unwrap_err()
            .error,
        RequestError::WrongReply
    );
    requests
        .deliver_reply_now(&f.runtime, &ticket, original)
        .unwrap();
    let replied = requests.take_replied(token(17)).unwrap();
    f.runtime.effect_completed(ticket).unwrap();
    retire_job(&f, replied.job, custody, report, observer).await;
    f.finish();
}
#[tokio::test]
async fn source_scope_fence_refuses_reply_and_keeps_the_same_unfilled_original_job() {
    let mut f = Fixture::new();
    let requests = table(&f);
    let drops = Arc::new(AtomicUsize::new(0));
    let (job, custody, report, observer) = f.job(17, Box::new(Capture(Arc::clone(&drops))));
    requests.insert(&request(17), job).unwrap();
    let original = reply(17, false);
    let effect = logical_effect(17, original.clone(), &f.logical.payer);
    let ticket = f.emit(true, effect);
    f.logical.scope.fail().unwrap();
    assert!(matches!(
        requests
            .deliver_reply_now(&f.runtime, &ticket, original)
            .unwrap_err()
            .error,
        RequestError::Runtime(_)
    ));
    assert!(requests.take_replied(token(17)).is_none());
    let job = requests.take_unpublished(token(17)).unwrap();
    assert!(custody.owns_exit(&job.exit));
    assert_eq!(job.result.in_flight().unwrap(), [None, None]);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    f.runtime.effect_completed(ticket).unwrap();
    retire_job(&f, job, custody, report, observer).await;
    f.finish();
}
#[tokio::test]
async fn full_or_closed_request_table_returns_original_jobs_without_disposing_captures() {
    let f = Fixture::new();
    let wake = Arc::new(Probe::default());
    let requests = AttemptRequests::new(
        activation(),
        f.attempt.registration.clone(),
        f.logical.registration.clone(),
        NonZeroUsize::MIN,
        Waker::from(wake.clone()),
    );
    let drops = Arc::new(AtomicUsize::new(0));
    let (job, a, ar, ah) = f.job(17, Box::new(Capture(Arc::clone(&drops))));
    requests.insert(&request(17), job).unwrap();
    let (job, b, br, bh) = f.job(18, Box::new(Capture(Arc::clone(&drops))));
    let refused = requests.insert(&request(18), job).unwrap_err();
    assert_eq!(refused.error, RequestError::Full);
    assert!(b.owns_exit(&refused._job.exit));
    assert!(!requests.has_capacity());
    let before = wake.0.load(Ordering::SeqCst);
    let original = requests.take_unpublished(token(17)).unwrap();
    assert_eq!(
        wake.0.load(Ordering::SeqCst),
        before + 1,
        "actual capacity return wakes the original Full producer"
    );
    assert!(requests.has_capacity());
    requests.close_admission();
    assert!(!requests.has_capacity());
    let refused = requests.insert(&request(18), refused._job).unwrap_err();
    assert_eq!(refused.error, RequestError::Closed);
    assert!(b.owns_exit(&refused._job.exit));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    retire_job(&f, original, a, ar, ah).await;
    retire_job(&f, refused._job, b, br, bh).await;
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    f.finish();
}

#[tokio::test]
async fn pending_original_captures_are_retained_if_last_external_table_handle_drops() {
    let f = Fixture::new();
    let requests = table(&f);
    let weak = Arc::downgrade(&requests);
    let drops = Arc::new(AtomicUsize::new(0));
    let (job, custody, report, observer) = f.job(17, Box::new(Capture(Arc::clone(&drops))));
    requests.insert(&request(17), job).unwrap();
    drop(requests);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "original rich captures cannot fall onto an owner poller"
    );
    let original = weak
        .upgrade()
        .expect("pending original table retains its custody");
    let job = original.take_unpublished(token(17)).unwrap();
    drop(original);
    assert!(
        weak.upgrade().is_none(),
        "the neutral table releases its retained alias after the original row moves out"
    );
    assert!(custody.owns_exit(&job.exit));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    retire_job(&f, job, custody, report, observer).await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    f.finish();
}

#[tokio::test]
async fn failed_original_owner_replies_to_same_job_without_ordinary_source_ticket() {
    let f = std::mem::ManuallyDrop::new(Fixture::new());
    let requests = table(&f);
    let drops = Arc::new(AtomicUsize::new(0));
    let (job, custody, report, observer) = f.job(17, Box::new(Capture(drops.clone())));
    requests.insert(&request(17), job).unwrap();
    let (mut cleanup, mut payer) = f
        .attempt
        .scope
        .reserve_next(
            EPOCH,
            novarocks_control_runtime_core::capacity::ReservationResources::work_only(),
            novarocks_control_runtime_core::scope::WorkKind::Cleanup,
            novarocks_control_runtime_core::scope::WorkAdmission::External,
        )
        .unwrap()
        .bind(novarocks_control_runtime_core::scope::WorkKind::Cleanup)
        .unwrap();
    let mut failure = f
        .runtime
        .reserve_admitted_input_for(
            &f.logical.registration,
            f.logical.registration.actor(),
            novarocks_control_runtime_core::mailbox::InputClass::Lifecycle,
            &f.logical.work,
        )
        .unwrap()
        .into_standing_input()
        .unwrap();
    failure
        .fill(FrontendInput::LogicalInfrastructureFailed)
        .unwrap();
    assert!(
        f.runtime
            .poll_one(novarocks_control_runtime_core::identity::Tick(1))
            .unwrap()
    );
    let proof = f
        .runtime
        .failure_proof_for(&f.logical.registration)
        .unwrap();
    cleanup.start().unwrap();
    let mut session = f
        .runtime
        .claim_actor_failure_publication(&proof, &cleanup)
        .unwrap();
    let cause = novarocks_coordination_contract::ExecutionFailure::from_input(
        novarocks_attempt_protocol::port_projection::owner_failure(),
    );
    let input = requests
        .publish_one_owner_failed(&mut session, &cause)
        .unwrap()
        .unwrap();
    assert!(
        requests
            .publish_one_owner_failed(&mut session, &cause)
            .unwrap()
            .is_none()
    );
    assert!(!custody.body_exited() && !custody.captures_exited());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let replied = requests.take_replied(token(17)).unwrap();
    assert_eq!(replied.reply_input, input);
    retire_job(&f, replied.job, custody, report, observer).await;
    let seen = f.attempt.seen.lock().unwrap();
    assert!(
        matches!(seen.as_slice(), [FrontendInput::Attempt(message)] if matches!(message.as_ref(),
        novarocks_attempt_protocol::input::AttemptInput::Port(message)
        if matches!(&message.body, novarocks_coordination_contract::LogicalToAttemptBody::PreReadyReplacementOwnerFailed {
            token: actual, failure: actual_failure
        } if *actual == token(17) && actual_failure == &cause)))
    );
    drop(seen);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    session.finish().unwrap();
    cleanup.body_exited().unwrap();
    cleanup.captures_exited().unwrap();
    payer.finish().unwrap();
    assert!(
        f.attempt
            .scope
            .reap_observed_header(&cleanup.retirement_observer())
            .unwrap()
    );
    let novarocks_control_runtime::actor_turn::FailureSourceState::ClosedPending(supervisor) = f
        .runtime
        .failure_source_state(f.logical.registration.actor())
        .unwrap()
    else {
        panic!("same original empty failure batch");
    };
    assert!(
        f.runtime
            .effect_inventory(f.logical.registration.actor(), supervisor, None, 1)
            .unwrap()
            .is_empty()
    );
    f.runtime
        .retire_input(f.logical.registration.actor(), supervisor)
        .unwrap();
    assert!(failure.retire_ready_after_exit().unwrap());
    failure.close_after_exit().unwrap();
    drop(
        f.runtime
            .take_failure_payload(f.logical.registration.actor())
            .unwrap(),
    );
    std::mem::ManuallyDrop::into_inner(f).finish();
}
