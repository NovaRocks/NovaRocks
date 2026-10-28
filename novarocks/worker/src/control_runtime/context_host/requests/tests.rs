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

//! Real Root Materialize/ScopeParent and direct Context R custody. The fixture
//! has not run Native materialization: Renew receives the actual Establishing
//! verdict. These tests prove neither RF loans nor full S8 recovery.

use super::*;
use crate::control_runtime::{
    actors::ContextMessage,
    context_host::requests::{ContextRequestError, ContextRequestPublishRefusal},
    request_ready::ControlRequestReady,
};
use novarocks_control_runtime::completion::ReportSlot;
use novarocks_control_runtime_core::{
    completion::{ConsumerKind, Consumption},
    identity::{ConsumerId, WorkId},
};
use novarocks_execution_contract::LeaseSequence;
use novarocks_worker_protocol::context::{
    ContextCommand, ContextEffect, ContextInput, ContextReplyBody,
};

fn original_context(requests: usize, extra: StorageNeed) -> Harness {
    let mut f = Harness::new_with_request_capacity(
        CONTEXT_BOOTSTRAP_STORAGE.checked_add(extra).unwrap(),
        requests,
    );
    let materialize = f.materialize();
    let binding = f.binding(context_actor(), &materialize);
    f.service.adopt_context(&materialize, binding).unwrap();
    f.complete_materialize_handoff(materialize);
    f
}
fn ready(f: &Harness, count: usize) -> ControlRequestReady {
    ControlRequestReady::new(NonZeroUsize::new(count).unwrap(), f.runtime.clone())
}
fn admit(
    f: &mut Harness,
    ready: &ControlRequestReady,
    operation: TaskOperationId,
) -> (
    WorkId,
    crate::control_runtime::context_host::requests::ContextRequestHandle,
) {
    f.service
        .context(f.identity)
        .unwrap()
        .admit_request(
            ready,
            ActorId::new(91).unwrap(),
            InputClass::Ordinary,
            operation,
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .unwrap()
}
fn renew() -> ContextCommand {
    ContextCommand::Renew {
        sequence: LeaseSequence::new(1),
        valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
    }
}
fn submit_renew(f: &mut Harness, work: WorkId) {
    f.service
        .context(f.identity)
        .unwrap()
        .submit_request(work, renew())
        .unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
}
fn reply(f: &Harness, input: InputId) -> EffectTicket<WorkerAction> {
    let effects = f
        .runtime
        .ready_effects(context_actor(), input, ContextCore::MAX_STEP_WORK)
        .unwrap();
    assert_eq!(effects.len(), 1, "one complete Core-authored Renew reply");
    f.runtime
        .take_effect(context_actor(), input, effects[0])
        .unwrap()
}
fn advance(f: &mut Harness, work: WorkId) -> bool {
    // The original reaper is bounded and may retire a sibling ready header
    // first. Do not alter protocol time or manufacture any exit observation.
    for _ in 0..8 {
        if f.service
            .context(f.identity)
            .unwrap()
            .advance_request(work)
            .unwrap()
        {
            return true;
        }
    }
    false
}
fn finish(mut f: Harness) {
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert!(
        f.service
            .context(f.identity)
            .unwrap()
            .resources
            .requests
            .is_empty()
    );
    assert!(
        f.service
            .context(f.identity)
            .unwrap()
            .resources
            .request_partials
            .is_empty()
    );
    f.dispose_service_bootstrap(context_actor());
    f.finish_model_refusal();
}

#[test]
fn original_delivery_keeps_shared_r_until_its_actual_drop_and_core_receipt_survives_input_retirement()
 {
    let mut f = original_context(1, StorageNeed::default());
    let ready = ready(&f, 1);
    let (work, mut handle) = admit(&mut f, &ready, operation(71));
    let request = handle.request();
    submit_renew(&mut f, work);
    let core_committed = f
        .runtime
        .input_custody(context_actor(), request.token)
        .unwrap()
        .is_some_and(|input| {
            input.phase == novarocks_control_runtime::driver::InputPhase::Committed
        });
    let before_reply = f
        .service
        .context(f.identity)
        .unwrap()
        .advance_request(work)
        .unwrap();
    let ticket = reply(&f, request.token);
    let decided = match &ticket.effect {
        WorkerAction::Reply(reply) => reply.body.clone(),
        _ => panic!("actual Core reply"),
    };
    f.service
        .context(f.identity)
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    let delivery = handle.try_take().unwrap();
    let same_body = delivery.body() == decided.as_ref() && delivery.request() == request;
    let establishing_verdict = matches!(
        decided.as_ref(),
        ContextReplyBody::Verdict(RequestResult::Outcome(
            OperationOutcome::ContextTerminalReceipt
        ))
    );
    drop(handle); // Actual delivery, not the old wait handle, now owns the leg.
    let while_delivering = f
        .service
        .context(f.identity)
        .unwrap()
        .advance_request(work)
        .unwrap();
    let input_retired = f
        .runtime
        .input_custody(context_actor(), request.token)
        .unwrap()
        .is_none();
    let charged = f.ledger.used(Dimension::Requests.id());
    let refused = f
        .service
        .context(f.identity)
        .unwrap()
        .admit_request(
            &ready,
            ActorId::new(92).unwrap(),
            InputClass::Control,
            operation(72),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .err()
        .expect("the original admission is refused");
    let full_without_partial = refused.partial.is_none();
    if let Some(partial) = refused.partial {
        assert!(advance(&mut f, partial));
    }
    drop(delivery);
    let retired = advance(&mut f, work);
    let refunded = f.ledger.used(Dimension::Requests.id()) == 0;
    finish(f);

    assert!(core_committed && !before_reply);
    assert!(same_body && establishing_verdict && !while_delivering && input_retired);
    assert_eq!(charged, 1);
    assert!(full_without_partial && retired && refunded);
}

#[test]
fn canceled_external_wait_does_not_revoke_the_already_owned_core_command() {
    let mut f = original_context(1, StorageNeed::default());
    let ready = ready(&f, 1);
    let (work, handle) = admit(&mut f, &ready, operation(73));
    let input = handle.request().token;
    f.service
        .context(f.identity)
        .unwrap()
        .submit_request(work, renew())
        .unwrap();
    drop(handle);
    let before_core = f
        .service
        .context(f.identity)
        .unwrap()
        .advance_request(work)
        .unwrap();
    let charged_before_core = f.ledger.used(Dimension::Requests.id());
    f.runtime.poll_one(Tick(2)).unwrap();
    let ticket = reply(&f, input);
    let exact_reply = matches!(&ticket.effect, WorkerAction::Reply(reply)
        if reply.request.token == input && reply.request.operation == operation(73)
            && reply.body.matches(reply.result));
    f.service
        .context(f.identity)
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    let retired = advance(&mut f, work);
    finish(f);

    assert!(!before_core && charged_before_core == 1);
    assert!(exact_reply && retired);
}

#[test]
fn matching_numeric_context_work_and_input_cannot_publish_a_foreign_runtime_ticket() {
    // Each Context came from its actual Root Materialize in a different real
    // runtime. Even matching ScopeId/WorkId/InputId and wire metadata do not
    // replace the private original runtime/ledger provenance.
    let mut a = original_context(1, StorageNeed::default());
    let mut b = original_context(1, StorageNeed::default());
    let ar = ready(&a, 1);
    let br = ready(&b, 1);
    let (aw, mut ah) = admit(&mut a, &ar, operation(74));
    let (bw, mut bh) = admit(&mut b, &br, operation(74));
    let same_numbers = aw == bw && ah.request().token == bh.request().token;
    submit_renew(&mut a, aw);
    submit_renew(&mut b, bw);
    let at = reply(&a, ah.request().token);
    let bt = reply(&b, bh.request().token);
    let at = match b
        .service
        .context(b.identity)
        .unwrap()
        .publish_request_reply(at)
    {
        Err(ContextRequestPublishRefusal::Publication { ticket, .. }) => ticket,
        Err(ContextRequestPublishRefusal::Rejected { ticket, .. }) => ticket,
        _ => panic!("foreign ticket must return its original custody"),
    };
    let bt = match a
        .service
        .context(a.identity)
        .unwrap()
        .publish_request_reply(bt)
    {
        Err(ContextRequestPublishRefusal::Publication { ticket, .. }) => ticket,
        Err(ContextRequestPublishRefusal::Rejected { ticket, .. }) => ticket,
        _ => panic!("foreign ticket must return its original custody"),
    };
    let neither_published = ah.try_take().is_none() && bh.try_take().is_none();
    a.service
        .context(a.identity)
        .unwrap()
        .publish_request_reply(at)
        .unwrap();
    b.service
        .context(b.identity)
        .unwrap()
        .publish_request_reply(bt)
        .unwrap();
    let ad = ah.try_take().unwrap();
    let bd = bh.try_take().unwrap();
    let same_decisions = ad.body() == bd.body();
    drop((ad, bd, ah, bh));
    let retired = advance(&mut a, aw) && advance(&mut b, bw);
    finish(a);
    finish(b);

    assert!(same_numbers && neither_published && same_decisions && retired);
}

#[test]
fn task_command_refusal_returns_the_original_and_the_unused_r_withdraws() {
    let mut f = original_context(1, StorageNeed::default());
    let ready = ready(&f, 1);
    let (work, handle) = admit(&mut f, &ready, operation(75));
    let input = handle.request().token;
    let task = crate::control_runtime::actors::tests::task(f.identity, 1);
    let command = ContextCommand::GetDynamicFilters {
        task,
        acknowledged: None,
    };
    let refused = f
        .service
        .context(f.identity)
        .unwrap()
        .submit_request(work, command)
        .expect_err("the original admission is refused");
    let original = matches!(refused.command,
        Some(ContextCommand::GetDynamicFilters { task: same, acknowledged: None }) if same == task);
    let typed = matches!(refused.error, ContextRequestError::TaskPredecessorRequired);
    let not_submitted = f
        .runtime
        .input_custody(context_actor(), input)
        .unwrap()
        .is_some_and(|input| {
            input.phase == novarocks_control_runtime::driver::InputPhase::Reserved
        });
    let retained = !f
        .service
        .context(f.identity)
        .unwrap()
        .advance_request(work)
        .unwrap();
    drop(handle);
    let withdrawn = advance(&mut f, work);
    finish(f);

    assert!(original && typed && not_submitted && retained && withdrawn);
}

#[test]
fn partial_input_header_refusal_keeps_the_original_then_cleans_only_its_unused_inventory() {
    // These unused foreign-capture-free header probes are paid by the actual
    // Context Materialize vector, independently of the one attempted R.
    let mut f = original_context(
        1,
        StorageNeed {
            inputs: 8,
            work: 1,
            ..StorageNeed::default()
        },
    );
    let ready = ready(&f, 1);
    // One additional original sibling header is paid by this Context's
    // Materialize vector. Its actual exit enters the common reaper before the
    // later partial R, without adding an RPC or a synthetic ready witness.
    let (mut sibling, mut sibling_capacity) = {
        let host = f.service.context(f.identity).unwrap();
        host.scope()
            .reserve_child_next(
                generation(),
                host.resources.roles[LIFETIME].capacity.id(),
                ResourceVector::default(),
                WorkKind::Cleanup,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap()
    };
    let sibling_retirement = sibling.retirement_observer();
    sibling.body_exited().unwrap();
    sibling.captures_exited().unwrap();
    let sibling_pending = !sibling_retirement.is_retired();
    let mut headers = Vec::new();
    {
        let host = f.service.context(f.identity).unwrap();
        while let Ok(permit) = f.runtime.reserve_admitted_input(
            context_actor(),
            ActorId::new(93).unwrap(),
            InputClass::Ordinary,
            &host.resources.roles[LIFETIME].work,
        ) {
            headers.push(permit);
        }
    }
    let refused = f
        .service
        .context(f.identity)
        .unwrap()
        .admit_request(
            &ready,
            ActorId::new(94).unwrap(),
            InputClass::Ordinary,
            operation(76),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .err()
        .expect("the original admission is refused");
    let typed = matches!(
        refused.error,
        ContextRequestError::Driver(DriverError::InputCapacity)
    );
    let partial = refused
        .partial
        .expect("original work/report remain in the host");
    let held = f.ledger.used(Dimension::Requests.id()) == 1
        && f.service
            .context(f.identity)
            .unwrap()
            .resources
            .request_partials
            .contains_key(&partial);
    for header in headers {
        header.revoke_unused().unwrap();
    }
    // Consume admission's coalesced Context mark. A later mark must therefore
    // come from this bounded cleanup turn, not an old mark or another RPC.
    let admitted_notice = f.service.next_host_ready().unwrap();
    let admission_mark_is_original = admitted_notice
        .as_ref()
        .is_some_and(|notice| f.service.context(f.identity).unwrap().matches_ready(notice));
    let admission_mark_drained = f.service.next_host_ready().unwrap().is_none();
    let first_retired = f
        .service
        .context(f.identity)
        .unwrap()
        .advance_request(partial)
        .unwrap();
    let sibling_reaped_by_first_turn = sibling_retirement.is_retired();
    let still_charged = f.ledger.used(Dimension::Requests.id()) == 1;
    let retry_notice = f.service.next_host_ready().unwrap();
    let same_original_ready = admitted_notice
        .as_ref()
        .zip(retry_notice.as_ref())
        .is_some_and(|(first, retry)| first.same_origin(retry) && retry.context() == f.identity);
    let retry_mark_drained = f.service.next_host_ready().unwrap().is_none();
    let cleaned = first_retired || advance(&mut f, partial);
    // Retire only already-exited original metadata. Release the sibling's
    // actual zero-resource header bundle, never an R/storage refund surrogate.
    let scope = f.service.context(f.identity).unwrap().scope().clone();
    f.ledger.reap_scope_ready(scope.id(), 8).unwrap();
    sibling_capacity
        .release(&sibling_capacity.resources().unwrap())
        .unwrap();
    drop((sibling, sibling_capacity, admitted_notice, retry_notice));
    finish(f);

    assert!(typed && held && cleaned);
    assert!(sibling_pending && sibling_reaped_by_first_turn);
    assert!(!first_retired && still_charged);
    assert!(admission_mark_is_original && admission_mark_drained);
    assert!(
        same_original_ready && retry_mark_drained,
        "a partial bounded reaper retry must requeue the same Context without another RPC"
    );
}

#[test]
fn partial_report_header_refusal_retains_and_retires_the_original_unstarted_work() {
    // Report probes have their actual prepaid Context storage and never attach
    // a callback, payload or foreign body. Their explicit unused exit is real.
    let mut f = original_context(
        1,
        StorageNeed {
            completions: 128,
            ..StorageNeed::default()
        },
    );
    let ready = ready(&f, 1);
    let mut reports = Vec::new();
    {
        let host = f.service.context(f.identity).unwrap();
        while let Ok(report) = ReportSlot::reserve_next(
            host.scope(),
            generation(),
            host.resources.roles[LIFETIME].work.id(),
            1,
            [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
        ) {
            reports.push(report);
        }
    }
    let refused = f
        .service
        .context(f.identity)
        .unwrap()
        .admit_request(
            &ready,
            ActorId::new(95).unwrap(),
            InputClass::Ordinary,
            operation(77),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .err()
        .expect("the original admission is refused");
    let partial = refused
        .partial
        .expect("original work/capacity remain in the host");
    let held = f.ledger.used(Dimension::Requests.id()) == 1;
    let probe_count = reports.len();
    let mut revoked_without_consumption = true;
    let mut requires_actual_exits = true;
    let mut reports_retired = true;
    for report in reports {
        // RevokedUnused is a distinct terminal production state. No consumer
        // ever received this report, so its Observer remains Pending; unused
        // revocation must not invent a positive consumption observation.
        report.revoke_unused().unwrap();
        revoked_without_consumption &=
            report.consumption(ConsumerId::new(1).unwrap()).unwrap() == Consumption::Pending;
        let before_exit = report.try_retire().unwrap();
        requires_actual_exits &= !before_exit;
        if before_exit {
            // A regression may already have removed the real metadata slot.
            // Do not issue nonexistent body/capture facts to repair it.
            continue;
        }
        report.body_exited().unwrap();
        let before_captures = report.try_retire().unwrap();
        requires_actual_exits &= !before_captures;
        if before_captures {
            continue;
        }
        report.captures_exited().unwrap();
        revoked_without_consumption &=
            report.consumption(ConsumerId::new(1).unwrap()).unwrap() == Consumption::Pending;
        reports_retired &= report.try_retire().unwrap();
    }
    let cleaned = advance(&mut f, partial);
    finish(f);

    assert!(held && cleaned);
    assert!(probe_count > 0 && revoked_without_consumption);
    assert!(requires_actual_exits && reports_retired);
}

#[test]
fn delayed_quiesce_reply_uses_its_original_token_after_the_initial_input_retires() {
    let mut f = original_context(1, StorageNeed::default());
    let ready = ready(&f, 1);
    let (work, mut handle) = admit(&mut f, &ready, operation(78));
    let request = handle.request();
    f.service
        .context(f.identity)
        .unwrap()
        .submit_request(work, ContextCommand::Quiesce)
        .unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
    let mut continue_seen = false;
    for action in f
        .runtime
        .ready_effects(context_actor(), request.token, ContextCore::MAX_STEP_WORK)
        .unwrap()
    {
        let ticket = f
            .runtime
            .take_effect(context_actor(), request.token, action)
            .unwrap();
        match &ticket.effect {
            WorkerAction::Context(ContextEffect::ContinueQuiesce) => continue_seen = true,
            WorkerAction::Context(ContextEffect::CloseExternalAdmission { .. }) => {
                f.runtime.close_actor_admission(context_actor()).unwrap();
            }
            _ => panic!("original Quiesce handoffs"),
        }
        f.runtime.effect_completed(ticket).unwrap();
    }
    let waiting = !f
        .service
        .context(f.identity)
        .unwrap()
        .advance_request(work)
        .unwrap();
    let original_input_retired = f
        .runtime
        .input_custody(context_actor(), request.token)
        .unwrap()
        .is_none();
    // Use the actual Context's already-paid serial metadata Step holder. This
    // exercises Core continuation/reply correlation, not a Native Q runner or
    // membership snapshot structural-funding acceptance claim.
    let continuation = {
        let host = f.service.context(f.identity).unwrap();
        f.runtime
            .reserve_admitted_input(
                context_actor(),
                ActorId::new(96).unwrap(),
                InputClass::Lifecycle,
                &host.resources.roles[TIMER].work,
            )
            .unwrap()
            .fill(WorkerMessage::Context(ContextMessage::Command {
                work: host.resources.roles[TIMER].work.id(),
                request: None,
                command: ContextInput::ContinueQuiesce,
            }))
            .unwrap()
    };
    f.runtime.poll_one(Tick(2)).unwrap();
    let ticket = reply(&f, continuation);
    let late_original_reply = matches!(&ticket.effect,
        WorkerAction::Reply(reply) if reply.request == request
            && matches!(reply.body.as_ref(), ContextReplyBody::Quiesce { .. }));
    f.service
        .context(f.identity)
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    f.runtime
        .retire_input(context_actor(), continuation)
        .unwrap();
    let delivery = handle.try_take().unwrap();
    let charged = f.ledger.used(Dimension::Requests.id()) == 1;
    drop((delivery, handle));
    let retired = advance(&mut f, work);
    finish(f);

    assert!(continue_seen && waiting && original_input_retired);
    assert!(continuation != request.token && late_original_reply && charged && retired);
}
