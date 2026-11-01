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
use crate::control_runtime::process_control::RootRequestAdvanceError;

fn advance_until(
    f: &mut Fixture,
    handles: &EstablishExecutorHandles,
    mut done: impl FnMut(&Fixture) -> bool,
) -> RequestIoProgress {
    let deadline = Instant::now() + WATCHDOG;
    let mut total = RequestIoProgress::default();
    loop {
        let version = f.runtime.change_version();
        let progress = f.root.advance_ready_requests(handles, None, 8).unwrap();
        total.visited += progress.visited;
        total.dispatched += progress.dispatched;
        total.submitted += progress.submitted;
        total.retired += progress.retired;
        if done(f) {
            return total;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "original R owner turn watchdog expired"
        );
        f.runtime.wait_after_timeout(version, remaining);
    }
}

#[test]
fn original_ready_turn_submits_getter_return_and_disposes_rejected_source_before_retirement() {
    let mut f = fixture();
    let mut foreign = fixture();
    let pools = SourcePoolGuard::start(&mut f);
    let foreign_pools = SourcePoolGuard::start(&mut foreign);
    let handles = pools.handles.as_ref().unwrap().clone();
    let (raw, getters, drops) = request(&f, None, None, false);
    let (input, mut external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    f.root
        .stage_establish_getter(input, job)
        .unwrap_or_else(|(error, _original)| panic!("original R staging refused: {error:?}"));
    let fees = tests::original_fees(&f);
    let wrong_owner = matches!(
        f.root
            .advance_ready_requests(foreign_pools.handles.as_ref().unwrap(), None, 8),
        Err(RootRequestAdvanceError::WrongOwner)
    );
    let untouched = tests::original_fees(&f) == fees && getters.load(Ordering::SeqCst) == 0;
    let submitted = advance_until(&mut f, &handles, |f| {
        f.root.resources().requests[&input].submitted
    });
    // The owner consumed the actual return and filled the original input; no
    // test-side call to take_establish_ingress or submit_request is involved.
    f.runtime.poll_one(Tick(2)).unwrap();
    f.root.index_control_effects(64).unwrap();
    let body = f.next_control_body();
    let reference = crate::control_runtime::effect_dispatch::EffectRef {
        input: body.input,
        action: body.descriptor.id,
    };
    f.complete_body(body);
    f.root.control_completion_observed(reference).unwrap();
    let reply = f.next_control_body();
    f.root.publish_request_reply(reply).unwrap();
    let delivery = external.try_take().unwrap();
    let gone = delivery.body()
        == &ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::Gone));
    let held_for_delivery = f
        .root
        .advance_ready_requests(&handles, None, 8)
        .unwrap()
        .retired
        == 0
        && f.ledger.used(Dimension::Requests.id()) == 1;
    drop(delivery);
    drop(external);
    let retired = advance_until(&mut f, &handles, |f| f.root.resources().requests.is_empty());
    drop(handles);
    pools.finish();
    foreign_pools.finish();
    let fees_zero = f.ledger.used(Dimension::Requests.id()) == 0;
    f.finish();
    foreign.finish();
    assert!(wrong_owner && untouched && gone && held_for_delivery && fees_zero);
    assert_eq!(submitted.submitted, 1);
    assert_eq!(retired.retired, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

#[test]
fn a_self_requeued_lower_request_does_not_starve_another_ready_original() {
    let mut f = fixture();
    let (first, external1) = f
        .root
        .admit_request(
            ActorId::new(2).unwrap(),
            operation(),
            MonotonicInstant::from_origin(Duration::from_secs(100)),
        )
        .unwrap();
    let (second, external2) = f
        .root
        .admit_request(
            ActorId::new(2).unwrap(),
            operation(),
            MonotonicInstant::from_origin(Duration::from_secs(100)),
        )
        .unwrap();
    f.root.resources().requests[&first].note_progress();
    f.root.resources().requests[&second].note_progress();
    let first_ready = f.root.request_ready().next_ready();
    f.root.resources().requests[&first].note_progress();
    let next_ready = f.root.request_ready().next_ready();
    drop(external1);
    drop(external2);
    assert!(
        f.root
            .begin_unsubmitted_withdrawal(first)
            .unwrap()
            .is_none()
    );
    assert!(
        f.root
            .begin_unsubmitted_withdrawal(second)
            .unwrap()
            .is_none()
    );
    let retired = f.root.reap_ready_requests(16).unwrap();
    f.finish();
    assert_eq!(first_ready, Some(first));
    assert_eq!(next_ready, Some(second));
    assert_eq!(retired, 2);
}

#[test]
fn original_ready_turn_publishes_getter_panic_failure_without_claiming_raw_exit() {
    let mut f = fixture();
    let pools = SourcePoolGuard::start(&mut f);
    let handles = pools.handles().clone();
    let (raw, getters, drops) = owner_failure::symbolic_panicking_source(&f, None);
    let (input, mut external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    f.root
        .stage_establish_getter(input, job)
        .unwrap_or_else(|(error, _original)| {
            panic!("original panic getter stage refused: {error:?}")
        });
    let submitted = advance_until(&mut f, &handles, |f| {
        f.root.resources().requests[&input].submitted
    });
    let raw_still_owned =
        drops.load(Ordering::SeqCst) == 0 && f.ledger.used(Dimension::Requests.id()) == 1;
    f.runtime.poll_one(Tick(1)).unwrap();
    f.root.index_control_effects(8).unwrap();
    let reply = f.root.take_control_effect(8).unwrap().unwrap();
    let same_input = reply.input == input;
    f.root.publish_request_reply(reply).unwrap();
    let delivery = external.try_take().unwrap();
    let failed = delivery.body()
        == &ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed));
    drop(delivery);
    drop(external);
    let completed = advance_until(&mut f, &handles, |f| f.root.resources().requests.is_empty());
    drop(handles);
    pools.finish();
    f.finish();
    assert!(same_input && failed && raw_still_owned);
    assert_eq!(submitted.submitted, 1);
    assert_eq!(completed.retired, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

#[test]
fn a_cancelled_original_return_keeps_authority_for_actual_withdrawal_and_cleanup() {
    let mut f = fixture();
    let pools = SourcePoolGuard::start(&mut f);
    let handles = pools.handles().clone();
    let (raw, getters, drops) = request(&f, None, None, false);
    let (input, external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    job.cancel_before_start();
    let observed = matches!(f.root.advance_ready_requests(&handles, None, 8),
        Err(RootRequestAdvanceError::Request { input: actual, error: RequestIoError::Observed(EstablishIngressOutcome::CancelledBeforeStart) }) if actual == input);
    let cached = matches!(
        f.root.resources().requests[&input].ingress_outcome,
        Some(EstablishIngressOutcome::CancelledBeforeStart)
    );
    let disposal = f.root.begin_unsubmitted_withdrawal(input).unwrap().unwrap();
    f.root
        .stage_establish_disposal(input, disposal)
        .unwrap_or_else(|(error, _original)| {
            panic!("actual cancelled source staging refused: {error:?}")
        });
    drop(external);
    let completed = advance_until(&mut f, &handles, |f| f.root.resources().requests.is_empty());
    drop(handles);
    pools.finish();
    f.finish();
    assert!(observed && cached);
    assert_eq!(completed.retired, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}
