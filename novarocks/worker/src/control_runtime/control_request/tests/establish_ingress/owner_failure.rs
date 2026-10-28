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

//! Failure admission needs the original failed owner and its drained return,
//! rather than merely an admitted RPC or a running foreign getter.

use super::*;
use novarocks_control_runtime::exit_role::ExitRolePhase;
use novarocks_control_runtime_core::completion::ReturnState;

#[test]
fn a_live_getter_and_its_projection_cannot_submit_an_owner_failure() {
    let mut f = fixture(2);
    let getter = Gate::new();
    let (raw, getters, drops) = request(&f, Some(getter.clone()), None, false);
    let (input, external) = admit(&mut f);
    let before_begin_refused = matches!(
        f.root.submit_establish_ingress_failure(input),
        Err(ControlRequestError::WrongPhase)
    );
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    let worker = thread::spawn(move || host(job));
    getter.entered.wait();
    let while_running_refused = matches!(
        f.root.submit_establish_ingress_failure(input),
        Err(ControlRequestError::WrongPhase)
    );
    // Release and join the real getter before asserting its rejection result.
    getter.resume.wait();
    worker.join().unwrap();
    let projected = matches!(
        f.root.take_establish_ingress(input).unwrap(),
        Some(EstablishIngressOutcome::Projected(_))
    );
    let after_projection_refused = matches!(
        f.root.submit_establish_ingress_failure(input),
        Err(ControlRequestError::WrongPhase)
    );
    drop(external);
    let cleanup = f.root.begin_unsubmitted_withdrawal(input).unwrap().unwrap();
    let source_exited = thread::spawn(move || cleanup.run()).join().unwrap();
    let retired = f.root.reap_ready_requests(8).unwrap();
    f.finish();

    assert!(before_begin_refused);
    assert!(while_running_refused);
    assert!(projected);
    assert!(after_projection_refused);
    assert!(source_exited);
    assert_eq!(retired, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

#[test]
fn an_actual_stopped_rpc_start_is_not_an_unknown_getter_failure() {
    let mut f = fixture(2);
    let (raw, getters, drops) = request(&f, None, None, false);
    let (input, external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    // This stops the actual scope containing the original RPC Step. The
    // getter must receive the real Work::start refusal, without a fake role.
    f.root.resources().scope.request_stop().unwrap();
    host(job);
    let start_refused = matches!(
        f.root.take_establish_ingress(input).unwrap(),
        Some(EstablishIngressOutcome::StartRefused(_))
    );
    let failure_refused = matches!(
        f.root.submit_establish_ingress_failure(input),
        Err(ControlRequestError::WrongPhase)
    );
    drop(external);
    let cleanup = f.root.begin_unsubmitted_withdrawal(input).unwrap().unwrap();
    let source_exited = thread::spawn(move || cleanup.run()).join().unwrap();
    let retired = f.root.reap_ready_requests(8).unwrap();
    f.finish();

    assert!(start_refused);
    assert!(failure_refused);
    assert!(source_exited);
    assert_eq!(retired, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

// These symbolic opaque sources exercise real Worker custody and foreign
// invocation/Drop boundaries. They do not claim Native ingress integration.
fn symbolic_panicking_source(
    f: &Fixture,
    drop_gate: Option<Arc<Gate>>,
) -> (EstablishQueryContext, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let getters = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let content = |panic_getter, drop_gate| {
        Arc::new(Content {
            getters: getters.clone(),
            drops: drops.clone(),
            getter_gate: None,
            drop_gate,
            panic_drop: false,
            panic_getter,
        }) as Arc<dyn CodecOwnedContent>
    };
    (
        EstablishQueryContext::new(
            operation(),
            f.context,
            AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
            content(true, drop_gate),
            content(false, None),
            content(false, None),
            CredentialUpdate::new(
                CredentialLeaseId::new(1),
                CredentialEpoch::new(1).unwrap(),
                Arc::new(Credential {
                    drops: drops.clone(),
                    panic_drop: false,
                }),
            ),
            LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
        ),
        getters,
        drops,
    )
}

fn dispatch_owner_failure(
    f: &mut Fixture,
    input: InputId,
    external: &mut ControlRequestHandle,
    now: Tick,
) -> (
    Option<crate::control_runtime::establish_ingress::EstablishDisposalJob>,
    bool,
    bool,
) {
    f.runtime.poll_one(now).unwrap();
    f.root.index_control_effects(64).unwrap();
    let failure_reply = f.root.take_control_effect(64).unwrap().unwrap();
    let reply_uses_original_input = failure_reply.input == input;
    let disposal = f
        .root
        .begin_rejected_ingress_disposal(&failure_reply)
        .unwrap();
    f.root.publish_request_reply(failure_reply).unwrap();
    let delivery = external.try_take().unwrap();
    let owner_failed = delivery.body()
        == &ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed));
    drop(delivery);
    (disposal, reply_uses_original_input, owner_failed)
}

#[test]
fn an_observed_getter_panic_drains_one_return_and_reply_but_waits_for_actual_raw_drop() {
    let mut f = fixture(2);
    let dropping = Gate::new();
    let (raw, getters, drops) = symbolic_panicking_source(&f, Some(dropping.clone()));
    let (input, mut external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    host(job);
    let original_return_is_filled = f.root.resources().requests[&input]
        .report
        .owned_return_state()
        .unwrap()
        == ReturnState::Filled;
    let filled_refused = matches!(
        f.root.submit_establish_ingress_failure(input),
        Err(ControlRequestError::IngressPending)
    );
    let panicked = matches!(
        f.root.take_establish_ingress(input).unwrap(),
        Some(EstablishIngressOutcome::GetterPanicked)
    );
    let authority_still_started =
        f.root.resources().requests[&input].role.phase() == ExitRolePhase::Started;
    let withdrawal_refused = matches!(
        f.root.begin_unsubmitted_withdrawal(input),
        Err(ControlRequestError::WrongPhase)
    );
    f.root.submit_establish_ingress_failure(input).unwrap();
    let duplicate_refused = matches!(
        f.root.submit_establish_ingress_failure(input),
        Err(ControlRequestError::WrongPhase)
    );
    let (disposal, reply_uses_original_input, owner_failed) =
        dispatch_owner_failure(&mut f, input, &mut external, Tick(1));
    drop(external);
    let cleaner = thread::spawn(move || disposal.unwrap().run());
    dropping.entered.wait();
    let retired_while_dropping = f.root.reap_ready_requests(8).unwrap();
    let fee_while_dropping = f.ledger.used(Dimension::Replies.id());
    let still_registered = f.root.request_ready().registered();
    // Unblock and join the actual destructor before any behavioral assertion.
    dropping.resume.wait();
    let source_exited = cleaner.join().unwrap();
    let no_second_reply = f.root.take_control_effect(64).unwrap().is_none();
    let retired = f.root.reap_ready_requests(8).unwrap();
    let input_retired = f.runtime.input_custody(actor(), input).unwrap().is_none();
    let fee_after_exit = f.ledger.used(Dimension::Replies.id());
    let registered_after_exit = f.root.request_ready().registered();
    f.finish();

    assert!(original_return_is_filled);
    assert!(filled_refused);
    assert!(panicked);
    assert!(authority_still_started);
    assert!(withdrawal_refused);
    assert!(duplicate_refused);
    assert!(reply_uses_original_input);
    assert!(owner_failed);
    assert_eq!(retired_while_dropping, 0);
    assert_eq!(fee_while_dropping, 1);
    assert_eq!(still_registered, 1);
    assert!(source_exited);
    assert!(no_second_reply);
    assert!(input_retired);
    assert_eq!(retired, 1);
    assert_eq!(fee_after_exit, 0);
    assert_eq!(registered_after_exit, 0);
    assert_eq!(getters.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

#[test]
fn five_observed_getter_panics_reuse_one_paid_rpc_position_after_actual_retirement() {
    // R is exactly one: five independently admitted requests must each return
    // the same finite capacity before the next request can be admitted.
    let mut f = Fixture::with_scopes(
        1,
        1,
        2,
        AdmissionStorage {
            loading: ResourceVector::default(),
            accepted: ResourceVector::default(),
            context: ResourceVector::default(),
            ticket: ResourceVector::default(),
        },
        ESTABLISH_REQUEST_STORAGE
            .checked_sub(CONTROL_REQUEST_STORAGE)
            .unwrap()
            .resources()
            .unwrap(),
        2,
    );
    let mut inputs = Vec::new();
    let mut all_panicked = true;
    let mut all_replied = true;
    let mut all_sources_exited = true;
    let mut all_positions_released = true;
    let mut retired = 0;
    let mut getter_count = 0;
    let mut drop_count = 0;
    for index in 1..=5 {
        let (raw, getters, drops) = symbolic_panicking_source(&f, None);
        let (input, mut external) = admit(&mut f);
        inputs.push(input);
        let job = f
            .root
            .begin_establish_ingress(input, raw, PayloadId(index))
            .unwrap();
        host(job);
        all_panicked &= matches!(
            f.root.take_establish_ingress(input).unwrap(),
            Some(EstablishIngressOutcome::GetterPanicked)
        );
        f.root.submit_establish_ingress_failure(input).unwrap();
        let (disposal, same_input, owner_failed) =
            dispatch_owner_failure(&mut f, input, &mut external, Tick(index));
        all_replied &= same_input && owner_failed;
        drop(external);
        all_sources_exited &= thread::spawn(move || disposal.unwrap().run())
            .join()
            .unwrap();
        retired += f.root.reap_ready_requests(8).unwrap();
        all_positions_released &= f.root.request_ready().registered() == 0
            && f.ledger.used(Dimension::Replies.id()) == 0
            && f.runtime.input_custody(actor(), input).unwrap().is_none();
        getter_count += getters.load(Ordering::SeqCst);
        drop_count += drops.load(Ordering::SeqCst);
    }
    f.finish();

    assert_eq!(inputs.len(), 5);
    assert!(inputs.windows(2).all(|pair| pair[0] != pair[1]));
    assert!(all_panicked);
    assert!(all_replied);
    assert!(all_sources_exited);
    assert!(all_positions_released);
    assert_eq!(retired, 5);
    assert_eq!(getter_count, 5);
    assert_eq!(drop_count, 20);
}

#[test]
fn a_lost_original_job_remains_unknown_after_its_failure_reply_without_fake_raw_cleanup() {
    let mut f = fixture(2);
    let (raw, getters, drops) = request(&f, None, None, false);
    let (input, mut external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    // Drop the actual never-run original job. Its real Drop loses its unique
    // sender and authority; no getter result or source-exit fact is invented.
    drop(job);
    let return_unknown = f.root.resources().requests[&input]
        .report
        .owned_return_state()
        .unwrap()
        == ReturnState::Unknown;
    let original_unresolved =
        f.root.resources().requests[&input].role.phase() == ExitRolePhase::Unresolved;
    f.root.submit_establish_ingress_failure(input).unwrap();
    let (disposal, same_input, owner_failed) =
        dispatch_owner_failure(&mut f, input, &mut external, Tick(1));
    let no_disposal_authority = disposal.is_none();
    drop(disposal);
    drop(external);
    let retired = f.root.reap_ready_requests(8).unwrap();
    let input_retired = f.runtime.input_custody(actor(), input).unwrap().is_none();
    let fee = f.ledger.used(Dimension::Replies.id());
    let registered = f.root.request_ready().registered();
    // Only the real outbox/input/external legs drained. The untouched raw
    // fields and their source role remain in one bounded self-retained owner.
    // This is deliberate Unknown evidence, never a successful cleanup result.
    std::mem::forget(f);

    assert!(return_unknown);
    assert!(original_unresolved);
    assert!(same_input);
    assert!(owner_failed);
    assert!(no_disposal_authority);
    assert!(input_retired);
    assert_eq!(retired, 0);
    assert_eq!(fee, 1);
    assert_eq!(registered, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}
