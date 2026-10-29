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

//! A late zero-effect Root return must wake its original paid request.

use super::*;

#[test]
fn a_late_context_return_reawakens_the_original_timed_out_request_without_a_new_effect() {
    let mut f = RouteFixture::new();
    let (input, mut handle, getters, drops) = f.begin_route(op(12), 10, None);
    let runtime = f.f.runtime.clone();

    // The Context has decided, but its original effect author has not handed
    // back the answer. Let the real Root timer settle the RPC first.
    runtime.poll_one(Tick(3)).unwrap();
    let probe = {
        let host_address = f.f.root.service().context(f.f.context).unwrap();
        let mut host = host_address.lock().unwrap();
        host.dispatch.index_committed(&runtime, 64).unwrap();
        host.dispatch.take_ready(&runtime, 64).unwrap().unwrap()
    };
    let decided_conflict = matches!(
        &probe.effect,
        WorkerAction::Context(ContextEffect::EstablishProbed { request, result, .. })
            if request.token == input
                && matches!(result, EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ContextConflict)
    );
    let timer_selected = runtime.advance_time(Tick(10), 2).unwrap();
    runtime.poll_one(Tick(10)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let cancel = f.f.root.take_control_effect(64).unwrap().unwrap();
    let timer_input = cancel.input;
    let cancelled_original = matches!(
        &cancel.effect,
        WorkerAction::Control { effect: ControlEffect::CancelEstablishRoute { request, .. }, .. }
            if request.token == input
    );

    // The already admitted author fills a different real Root input. Its
    // Context input, captures, report and escrow retire before Root consumes
    // that original return occurrence.
    let context_retired = {
        let host_address = f.f.root.service().context(f.f.context).unwrap();
        let mut host = host_address.lock().unwrap();
        host.handoff_establish_probe(probe).unwrap();
        host.advance_establish_route(input).unwrap()
    };
    let returns: Vec<_> = runtime
        .input_inventory(actor(), None, 16)
        .unwrap()
        .into_iter()
        .filter(|entry| {
            entry.source == context_actor()
                && entry.phase == InputPhase::Filled
                && entry.role.is_some()
        })
        .collect();
    let return_input = returns.first().expect("actual admitted Root return").input;
    let distinct_return_occurrence = returns.len() == 1 && return_input != input;

    f.f.root.handoff_establish_route_cancel(cancel).unwrap();
    let reply = f.f.root.take_control_effect(64).unwrap().unwrap();
    let timeout_reply = matches!(
        &reply.effect,
        WorkerAction::ControlReply(reply)
            if reply.request.token == input && reply.result == RequestResult::TimeoutWhileAccepted
    );
    f.f.root.publish_request_reply(reply).unwrap();
    let delivery = handle.try_take().unwrap();
    let timeout_delivered = matches!(
        delivery.body(),
        ControlReplyBody::Verdict(RequestResult::TimeoutWhileAccepted)
    );
    drop(delivery);
    drop(handle);
    runtime.retire_input(actor(), timer_input).unwrap();
    let disposal =
        f.f.root
            .take_retired_route_disposal(input)
            .expect("actual Context exit permits the original raw disposal");
    let disposed = thread::spawn(move || disposal.run()).join().unwrap();

    // Drain every existing original-R signal while its callback is still only
    // Filled. This must retain R and both paid Replies, not guess Core exit.
    let before_return_reaped = f.f.root.reap_ready_requests(32).unwrap();
    let pending_before_return = f.f.root.resources().requests.contains_key(&input);
    let fees_before_return = f.f.ledger.used(Dimension::Replies.id());
    let filled_before_return = runtime
        .input_custody(actor(), return_input)
        .unwrap()
        .is_some_and(|entry| entry.phase == InputPhase::Filled);

    runtime.poll_one(Tick(10)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    // The pure Core correctly ignores the expired token and commits an empty
    // effect batch. Its real input progress must still address the original R.
    let no_new_effect = f.f.root.take_control_effect(64).unwrap().is_none();
    let committed_return = runtime
        .input_custody(actor(), return_input)
        .unwrap()
        .is_some_and(|entry| entry.phase == InputPhase::Committed);
    let unassisted_reaped = f.f.root.reap_ready_requests(32).unwrap();

    // Diagnostic cleanup only: preserve the first result, then use the actual
    // original custody's metadata wake so a red assertion cannot abandon bound
    // roles or obscure this liveness defect with a strict Drop abort.
    let assisted_reaped = if unassisted_reaped == 0 {
        f.f.root
            .resources()
            .requests
            .get(&input)
            .expect("the same original request remains")
            .note_progress();
        f.f.root.reap_ready_requests(32).unwrap()
    } else {
        0
    };
    let request_retired = !f.f.root.resources().requests.contains_key(&input);
    let fees_after_return = f.f.ledger.used(Dimension::Replies.id());
    let actual_getters = getters.load(Ordering::SeqCst);
    let actual_drops = drops.load(Ordering::SeqCst);
    f.finish_model_refusal();

    // Every semantic assertion follows the full real fixture teardown.
    assert_eq!(
        unassisted_reaped, 1,
        "a zero-effect late callback must wake its original R"
    );
    assert!(decided_conflict);
    assert_eq!(timer_selected, 1);
    assert!(cancelled_original);
    assert!(context_retired);
    assert!(distinct_return_occurrence);
    assert!(timeout_reply && timeout_delivered);
    assert!(disposed);
    assert_eq!(before_return_reaped, 0);
    assert!(pending_before_return);
    assert_eq!(fees_before_return, 2);
    assert!(filled_before_return);
    assert!(no_new_effect);
    assert!(committed_return);
    assert_eq!(assisted_reaped, 0);
    assert!(request_retired);
    assert_eq!(fees_after_return, 1);
    assert_eq!(actual_getters, 3);
    assert_eq!(actual_drops, 4);
}
