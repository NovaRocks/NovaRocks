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

#[path = "tests/task_prefix.rs"]
mod task_prefix;
#[path = "tests/task_routing.rs"]
mod task_routing;

use super::*;
use crate::control_runtime::{
    actors::ContextMessage,
    context_host::requests::{ContextRequestError, ContextRequestPublishRefusal},
    request_ready::ControlRequestReady,
};
use crate::control_runtime::{
    source_positions::SOURCE_SLOT_STORAGE, source_slot_issuer::SourceSlotIssuePartial,
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

fn task_funding_context(extra: StorageNeed) -> Harness {
    // The fixture supplies the actual Task-causal profile to the process
    // ledger. It does not borrow the Context bootstrap's paid inventory.
    let mut f = Harness::new_with_request_profile(
        CONTEXT_BOOTSTRAP_STORAGE.checked_add(extra).unwrap(),
        1,
        StorageNeed::role(true),
    );
    let materialize = f.materialize();
    let binding = f.binding(context_actor(), &materialize);
    f.service.adopt_context(&materialize, binding).unwrap();
    f.complete_materialize_handoff(materialize);
    f
}

#[test]
fn task_request_source_siblings_and_outer_keep_shared_r_after_the_original_author_retires() {
    use crate::control_runtime::admitted_role::{adopt_reported_work, dispose_unused_completion};
    use novarocks_control_runtime::exit_role::{ExitRole, ExitRolePhase};

    let mut f = task_funding_context(StorageNeed::default());
    let dimensions = [
        Dimension::Requests,
        Dimension::Inputs,
        Dimension::Batches,
        Dimension::Effects,
        Dimension::Cuts,
        Dimension::Replies,
        Dimension::Completions,
        Dimension::Captures,
        Dimension::Work,
    ];
    let baseline = dimensions.map(|dimension| f.ledger.used(dimension.id()));
    let mut all_authors_retired = true;
    let mut all_sources_still_live = true;
    let mut original_outer_preserved = true;
    let mut no_second_r = true;
    for _ in 0..3 {
        let funding = f
            .service
            .context(f.identity)
            .unwrap()
            .reserve_task_request_funding()
            .unwrap();
        let author = funding.author;
        let sources = funding.sources;
        let mut outer = funding.outer;
        let main_retirement = author.work.retirement_observer();
        let source_retirements = sources
            .each_ref()
            .map(|source| source.reported().work().retirement_observer());
        let (main, authority) = ExitRole::from_admitted(adopt_reported_work(
            author.work,
            author.capacity,
            author.report,
        ));
        // These holders have never been exposed to a Task, producer or RPC.
        // Only their own actual unused exit may be observed here.
        authority.withdraw_unpublished_and_exit().unwrap();
        let scope = f.service.context(f.identity).unwrap().scope().clone();
        f.ledger.reap_scope_ready(scope.id(), 8).unwrap();
        all_authors_retired &= main_retirement.is_retired()
            && main.try_retire().unwrap()
            && main.phase() == ExitRolePhase::Retired;
        all_sources_still_live &= source_retirements.iter().all(|source| !source.is_retired());
        let original_outer = outer.resources().unwrap();
        assert_eq!(original_outer.get(Dimension::Work.id()), 0);
        let original_source_payment = SOURCE_SLOT_STORAGE
            .checked_mul(2)
            .unwrap()
            .resources()
            .unwrap();
        let retained = original_outer
            .checked_add(&original_source_payment)
            .unwrap();
        for (index, dimension) in dimensions.iter().enumerate() {
            original_outer_preserved &=
                f.ledger.used(dimension.id()) == baseline[index] + retained.get(dimension.id());
        }
        let refusal = f
            .service
            .context(f.identity)
            .unwrap()
            .reserve_task_request_funding()
            .err()
            .expect("one actual process R remains occupied");
        no_second_r &= refusal.partial.is_none();
        if let Some(partial) = refusal.partial {
            assert!(advance(&mut f, partial));
        }
        let mut partial = SourceSlotIssuePartial::from_admissions(&scope, sources);
        let mut source_exit = false;
        for _ in 0..8 {
            if partial.try_retire(&f.ledger).unwrap() {
                source_exit = true;
                break;
            }
        }
        assert!(source_exit);
        f.ledger.reap_scope_ready(scope.id(), 8).unwrap();
        assert!(source_retirements.iter().all(|source| source.is_retired()));
        // Main Work has already gone; the same original outer is still the
        // unique paid reservation. It grants no new Work/derivation authority.
        outer.release(&original_outer).unwrap();
        for (index, dimension) in dimensions.iter().enumerate() {
            assert_eq!(f.ledger.used(dimension.id()), baseline[index]);
        }
    }
    finish(f);
    assert!(all_authors_retired && all_sources_still_live);
    assert!(original_outer_preserved && no_second_r);
}

#[test]
fn partial_task_source_report_admission_retains_original_outer_until_its_actual_headers_retire() {
    let mut f = task_funding_context(StorageNeed {
        completions: 128,
        ..StorageNeed::default()
    });
    let mut probes = Vec::new();
    {
        let host = f.service.context(f.identity).unwrap();
        while let Ok(report) = ReportSlot::reserve_next(
            host.scope(),
            generation(),
            host.resources.roles[LIFETIME].work.id(),
            1,
            [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
        ) {
            probes.push(report);
        }
    }
    // Leave exactly the original author's report and Source1's report free.
    // The second Source admission must return a real partial, not a fabricated
    // successful Source2 or an immediate refund of its occupied outer storage.
    assert!(probes.len() > 2);
    for report in probes.drain(probes.len() - 2..) {
        report.revoke_unused().unwrap();
        report.body_exited().unwrap();
        report.captures_exited().unwrap();
        assert!(report.try_retire().unwrap());
    }
    let before_admission = f
        .service
        .context(f.identity)
        .unwrap()
        .scope()
        .snapshot()
        .unwrap();
    let refusal = f
        .service
        .context(f.identity)
        .unwrap()
        .reserve_task_request_funding()
        .err()
        .expect("actual report inventory is full");
    let partial = refusal
        .partial
        .expect("actual author, Source1 and outer remain owned");
    let held = f.ledger.used(Dimension::Requests.id()) == 1;
    let before = f
        .service
        .context(f.identity)
        .unwrap()
        .scope()
        .snapshot()
        .unwrap();
    // Actual resident reports distinguish the retained author plus Source1
    // from a refusal that rolled every header back or merely copied counts.
    let held_original_partial =
        before.storage == before_admission.storage + 2 && before.work >= before_admission.work + 2;
    for report in probes {
        report.revoke_unused().unwrap();
        report.body_exited().unwrap();
        report.captures_exited().unwrap();
        assert!(report.try_retire().unwrap());
    }
    let cleaned = advance(&mut f, partial);
    let after = f
        .service
        .context(f.identity)
        .unwrap()
        .scope()
        .snapshot()
        .unwrap();
    let actual_headers_retired = after.work < before.work && after.storage < before.storage;
    finish(f);
    assert!(held_original_partial && held && cleaned && actual_headers_retired);
}

#[test]
fn task_funding_in_a_cleanup_epoch_keeps_the_original_core_consumer_generation() {
    use crate::control_runtime::admitted_role::{adopt_reported_work, dispose_unused_completion};
    use novarocks_control_runtime::exit_role::ExitRole;
    let mut f = task_funding_context(StorageNeed::default());
    let scope = f.service.context(f.identity).unwrap().scope().clone();
    let original_generation = scope.gates().unwrap().generation;
    let cleanup_generation = scope.fail().unwrap();
    assert!(cleanup_generation > original_generation);
    let funding = f
        .service
        .context(f.identity)
        .unwrap()
        .reserve_task_request_funding()
        .unwrap();
    let reports = [
        funding.author.report.clone(),
        funding.sources[0].reported().report().clone(),
        funding.sources[1].reported().report().clone(),
    ];
    let mut original_core_legs = true;
    for report in &reports {
        original_core_legs &= report
            .abandon_failed_owner(ConsumerId::new(1).unwrap(), &scope, original_generation)
            .is_ok();
    }
    // No report/body was published to the external leg. These exact unexposed
    // holders now take their real unused exit; abandoning Core did not sign it.
    let (main, authority) = ExitRole::from_admitted(adopt_reported_work(
        funding.author.work,
        funding.author.capacity,
        funding.author.report,
    ));
    let no_invented_physical_exit = !main.body_exited() && !main.captures_exited();
    authority.withdraw_unpublished_and_exit().unwrap();
    let mut partial = SourceSlotIssuePartial::from_admissions(&scope, funding.sources);
    let mut source_exit = false;
    for _ in 0..8 {
        if partial.try_retire(&f.ledger).unwrap() {
            source_exit = true;
            break;
        }
    }
    assert!(source_exit);
    f.ledger.reap_scope_ready(scope.id(), 8).unwrap();
    let retired = main.try_retire().unwrap();
    let mut outer = funding.outer;
    outer.release(&outer.resources().unwrap()).unwrap();
    finish(f);
    assert!(original_core_legs && no_invented_physical_exit && retired);
}

// This is the actual Context/Task/R owner graph with modeled neutral
// materialization and an unstarted loader. It claims no Native I/O or RF loan.
struct RpcTaskPending;
impl crate::task_port::TaskPendingNotice for RpcTaskPending {
    fn pending(&self) {}
}
fn model_fact(f: &mut Harness, fact: ContextInput, at: u64) -> InputId {
    let work = f.service.context(f.identity).unwrap().resources.roles[TIMER]
        .work
        .id();
    let input = f
        .runtime
        .reserve_input(
            context_actor(),
            ActorId::new(88).unwrap(),
            InputClass::Lifecycle,
        )
        .unwrap()
        .fill(crate::control_runtime::actors::WorkerMessage::Context(
            ContextMessage::Command {
                work,
                request: None,
                command: fact,
            },
        ))
        .unwrap();
    f.runtime.poll_one(Tick(at)).unwrap();
    input
}
fn complete_model_effects(f: &mut Harness, input: InputId) {
    complete_model_effects_with_preparation(f, input, None);
}
fn complete_model_effects_with_preparation(
    f: &mut Harness,
    input: InputId,
    mut preparation: Option<&mut Option<ContextMetadataRole>>,
) {
    use novarocks_control_runtime_core::effect::EffectState;
    loop {
        let inventory = f
            .runtime
            .effect_inventory(context_actor(), input, None, ContextCore::MAX_STEP_WORK)
            .unwrap();
        for (action, phase) in &inventory {
            if *phase == EffectState::Pending {
                for predecessor in &action.dependencies {
                    if inventory.iter().any(|(actual, state)| {
                        actual.id == *predecessor && *state == EffectState::Completed
                    }) {
                        f.runtime
                            .acknowledge_dependency(context_actor(), input, action.id, *predecessor)
                            .unwrap();
                    }
                }
            }
        }
        let ready = f
            .runtime
            .ready_effects(context_actor(), input, ContextCore::MAX_STEP_WORK)
            .unwrap();
        if ready.is_empty() {
            break;
        }
        for action in ready {
            let ticket = f
                .runtime
                .take_effect(context_actor(), input, action)
                .unwrap();
            if let WorkerAction::Context(ContextEffect::EnqueuePreparation {
                reservation, ..
            }) = &ticket.effect
            {
                let slot = preparation
                    .as_deref_mut()
                    .expect("the fixture must retain the actual preparation guard");
                let guard = f
                    .runtime
                    .take_step_reservation(&ticket, *reservation)
                    .unwrap();
                let (work, capacity) = guard.bind(WorkKind::Business).unwrap();
                assert!(
                    slot.replace(ContextMetadataRole { work, capacity })
                        .is_none()
                );
                f.runtime.effect_completed(ticket).unwrap();
            } else if matches!(ticket.effect, WorkerAction::Reply(_)) {
                f.service
                    .context(f.identity)
                    .unwrap()
                    .publish_request_reply(ticket)
                    .unwrap();
            } else {
                // Neutral component publications only. Never use this helper
                // to claim a Native callback, pin, body or opaque disposal exit.
                f.runtime.effect_completed(ticket).unwrap();
            }
        }
    }
}
fn pending_loading_task_fixture(
    identity: Option<QueryContextRef>,
) -> (
    Harness,
    TaskIdentity,
    WorkId,
    crate::control_runtime::context_host::requests::ContextRequestHandle,
    EffectTicket<WorkerAction>,
) {
    let mut f = Harness::new_with_context_identity(
        CONTEXT_BOOTSTRAP_STORAGE,
        1,
        StorageNeed::role(true),
        1,
        identity,
    );
    let materialize = f.materialize();
    let binding = f.binding(context_actor(), &materialize);
    f.service.adopt_context(&materialize, binding).unwrap();
    f.complete_materialize_handoff(materialize);
    let activated = model_fact(
        &mut f,
        ContextInput::MaterializationFinished {
            result: Ok(crate::control_runtime::actors::tests::initial_materialized()),
        },
        2,
    );
    complete_model_effects(&mut f, activated);
    f.runtime.retire_input(context_actor(), activated).unwrap();
    let task = crate::control_runtime::actors::tests::task(f.identity, 1);
    let ready = ready(&f, 1);
    let (work, create) = f
        .service
        .context(f.identity)
        .unwrap()
        .admit_request(
            &ready,
            ActorId::new(87).unwrap(),
            InputClass::Ordinary,
            operation(79),
            MonotonicInstant::from_origin(Duration::from_nanos(4)),
        )
        .unwrap();
    let input = create.request().token;
    f.service
        .context(f.identity)
        .unwrap()
        .submit_request(
            work,
            ContextCommand::Create {
                task,
                payload: novarocks_worker_protocol::creation::PayloadId(1),
            },
        )
        .unwrap();
    f.runtime.poll_one(Tick(3)).unwrap();
    let load_action = f
        .runtime
        .ready_effects(context_actor(), input, ContextCore::MAX_STEP_WORK)
        .unwrap()[0];
    let load = f
        .runtime
        .take_effect(context_actor(), input, load_action)
        .unwrap();
    assert!(matches!(
        load.effect,
        WorkerAction::Context(ContextEffect::LoadInput { .. })
    ));
    (f, task, work, create, load)
}
fn live_loading_task_fixture() -> (Harness, TaskIdentity) {
    let (mut f, task, work, create, load) = pending_loading_task_fixture(None);
    f.service
        .context(f.identity)
        .unwrap()
        .adopt_task(
            &f.runtime,
            &load,
            crate::control_runtime::task::TaskHostBinding {
                source_storage_model: false,
                context: f.identity,
                task_source: ActorId::new(86).unwrap(),
                protocol_generation: generation(),
                max_cuts: 4,
                pending: Arc::new(RpcTaskPending),
                health: Arc::new(
                    novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
                ),
            },
        )
        .unwrap();
    finish_create_waiter(&mut f, work, create, load);
    (f, task)
}
fn finish_create_waiter(
    f: &mut Harness,
    work: WorkId,
    mut create: crate::control_runtime::context_host::requests::ContextRequestHandle,
    load: EffectTicket<WorkerAction>,
) {
    let input = create.request().token;
    f.runtime.effect_completed(load).unwrap();
    assert!(
        !f.service
            .context(f.identity)
            .unwrap()
            .advance_request(work)
            .unwrap()
    );
    assert!(
        f.runtime
            .input_custody(context_actor(), input)
            .unwrap()
            .is_none()
    );
    // The Create waiter expires; its elected Task/loader responsibility stays.
    // No timeout or reply is used to claim that the loader actually exited.
    let tick = model_fact(f, ContextInput::Tick, 5);
    complete_model_effects(f, tick);
    f.runtime.retire_input(context_actor(), tick).unwrap();
    let delivered = create.try_take().expect("original Create timeout reply");
    assert!(matches!(
        delivered.body(),
        ContextReplyBody::Verdict(RequestResult::TimeoutWhileAccepted)
    ));
    drop((create, delivered));
    assert!(advance(f, work));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
}
fn finish_task_fixture(mut f: Harness, task: TaskIdentity) {
    use crate::task_proposal_ingress::PairRoute;
    let host = f.service.context(f.identity).unwrap();
    host.close_task_request_admission(task).unwrap();
    let owner = host
        .take_task(task)
        .expect("original requests and cuts have drained");
    let crate::control_runtime::task::TaskHostOwner::Ready(task) = owner else {
        panic!("same actual ready Task");
    };
    let mut resources = task.into_resources();
    assert!(resources.causal.is_empty());
    let ingress = resources.port.ingress().clone();
    ingress.seal_source_admission();
    for route in [PairRoute::Open, PairRoute::ConsumingOpen] {
        for holder in ingress
            .take_unused_pair(route)
            .unwrap()
            .into_iter()
            .flatten()
        {
            holder.exit();
        }
        for source in ingress.pair_inventory(route).unwrap() {
            assert!(source.source.is_none());
            assert!(source.custody.try_retire().unwrap());
        }
    }
    let scope = f.service.context(f.identity).unwrap().scope().clone();
    f.ledger.reap_scope_ready(scope.id(), 32).unwrap();
    for route in [PairRoute::Open, PairRoute::ConsumingOpen] {
        let reaped = ingress
            .prepare_pair_reap(route)
            .unwrap()
            .map(|probe| probe.observe_reaped().unwrap());
        let storage = ingress
            .release_closed_route_pair(route, reaped)
            .unwrap_or_else(|_| panic!("actual source and summary teardown"));
        for mut payment in storage {
            payment.release().unwrap();
        }
    }
    resources.lifetime.body_exited().unwrap();
    resources.lifetime.captures_exited().unwrap();
    if let Some(open) = &mut resources.open_summary {
        open.close_after_exit(&resources.lifetime).unwrap();
    }
    resources
        .active
        .release(&resources.active.resources().unwrap())
        .unwrap();
    f.ledger.reap_scope_ready(scope.id(), 32).unwrap();
    drop((resources, ingress));
    finish(f);
}

#[test]
fn actual_task_r_repeats_at_r_one_and_retains_the_original_delivery_and_real_delayed_token() {
    let (mut f, task) = live_loading_task_fixture();
    let ready = ready(&f, 1);
    let mut prior_input = None;
    for sequence in 0..3 {
        let operation = operation(80 + sequence);
        let (work, mut handle) = f
            .service
            .context(f.identity)
            .unwrap()
            .admit_task_request(
                &ready,
                task,
                InputClass::Control,
                operation,
                MonotonicInstant::from_origin(Duration::from_secs(5)),
            )
            .unwrap();
        assert_eq!(handle.request().operation, operation);
        f.service
            .context(f.identity)
            .unwrap()
            .submit_request(work, ContextCommand::GetFinalInfo { task })
            .unwrap();
        let delivery = f
            .service
            .context(f.identity)
            .unwrap()
            .deliver_task_event(task)
            .unwrap()
            .unwrap();
        assert_ne!(prior_input, Some(delivery.event));
        prior_input = Some(delivery.event);
        f.runtime.poll_one(Tick(6 + u64::from(sequence))).unwrap();
        let ticket = reply(&f, delivery.event);
        let original_token = match &ticket.effect {
            WorkerAction::Reply(reply) => {
                reply.request.token == delivery.event
                    && reply.request.operation == operation
                    && reply.task == Some(task)
            }
            _ => false,
        };
        f.service
            .context(f.identity)
            .unwrap()
            .publish_request_reply(ticket)
            .unwrap();
        let delivered = handle.try_take().unwrap();
        assert!(matches!(
            delivered.body(),
            ContextReplyBody::Verdict(RequestResult::Outcome(
                OperationOutcome::InvalidStateOrRequest
            ))
        ));
        drop(handle);
        assert!(
            !f.service
                .context(f.identity)
                .unwrap()
                .advance_request(work)
                .unwrap()
        );
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
        assert!(
            f.runtime
                .input_custody(context_actor(), delivery.event)
                .unwrap()
                .is_none()
        );
        assert!(original_token);
        drop(delivered);
        // Progress uses only the original real ready address, including the
        // retry emitted when a bounded reaper first visits another sibling.
        let mut retired = false;
        for _ in 0..8 {
            let (scope, actual) = ready
                .next_context_ready()
                .expect("same original R must remain ready");
            assert_eq!(scope, f.service.context(f.identity).unwrap().scope().id());
            assert_eq!(actual, work);
            if f.service
                .context(f.identity)
                .unwrap()
                .advance_request(actual)
                .unwrap()
            {
                retired = true;
                break;
            }
        }
        assert!(retired);
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
        assert_eq!(ready.registered(), 0);
    }
    finish_task_fixture(f, task);
}

#[test]
fn task_input_admission_pressure_keeps_the_original_unexposed_partial_and_refunds_without_task_exit()
 {
    let (mut f, task) = live_loading_task_fixture();
    let ready = ready(&f, 1);
    // Explicit unused mailbox probes occupy actual actor positions. They
    // attach no command/body and are revoked by this fixture owner below.
    let mut probes = Vec::new();
    while let Ok(permit) = f.runtime.reserve_input(
        context_actor(),
        ActorId::new(85).unwrap(),
        InputClass::Control,
    ) {
        probes.push(permit);
    }
    assert!(!probes.is_empty());
    let refusal = f
        .service
        .context(f.identity)
        .unwrap()
        .admit_task_request(
            &ready,
            task,
            InputClass::Control,
            operation(110),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .err()
        .expect("the actual Task causal input pool is occupied");
    let partial = refusal
        .partial
        .expect("same admitted main, Source2 and outer remain owned");
    let actual_input_refusal = matches!(refusal.error, ContextRequestError::Causal(_));
    assert_eq!(ready.registered(), 0);
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    assert!(
        f.service
            .context(f.identity)
            .unwrap()
            .task(task)
            .unwrap()
            .causal()
            .is_empty()
    );
    // Cleanup of those never-exposed originals does not wait for the mailbox
    // probes, another RPC, Task close, or an invented input consumption fact.
    let retired = advance(&mut f, partial);
    let original_task_still_owned = f.service.context(f.identity).unwrap().task(task).is_some();
    let refunded = f.ledger.used(Dimension::Requests.id()) == 0;
    for permit in probes {
        permit.revoke_unused().unwrap();
    }
    let (work, handle) = f
        .service
        .context(f.identity)
        .unwrap()
        .admit_task_request(
            &ready,
            task,
            InputClass::Control,
            operation(111),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .unwrap();
    drop(handle);
    assert!(advance(&mut f, work));
    finish_task_fixture(f, task);
    assert!(actual_input_refusal && retired && original_task_still_owned && refunded);
}

fn accepted_task_fixture() -> (Harness, TaskIdentity, ContextMetadataRole) {
    let (mut f, task) = live_loading_task_fixture();
    // This neutral model has no measured Native input. Its explicit cost is
    // accepted by the real synchronous port, which owns the actual P/bytes
    // reservation below. No state flag or fabricated Installed fact is used.
    let loaded = model_fact(
        &mut f,
        ContextInput::Loaded {
            task,
            result: Ok(novarocks_worker_protocol::creation::CreationCost {
                input_retained_bytes: 1,
                ingress_retained_bytes: 1,
                structural_bytes: 1,
                inbound_records: 0,
                inbound_bytes: 0,
            }),
        },
        6,
    );
    let mut preparation = None;
    complete_model_effects_with_preparation(&mut f, loaded, Some(&mut preparation));
    f.runtime.retire_input(context_actor(), loaded).unwrap();
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 1);
    assert_eq!(f.ledger.used(Dimension::PreparingBytes.id()), 3);
    (
        f,
        task,
        preparation.expect("original Accepted queue reservation"),
    )
}
fn finish_accepted_task_fixture(
    mut f: Harness,
    task: TaskIdentity,
    mut preparation: ContextMetadataRole,
) {
    // The test never starts this original queued preparation or captures a
    // host input/body. Its actual unused holder exits here before Task close.
    preparation.work.body_exited().unwrap();
    preparation.work.captures_exited().unwrap();
    let scope = f.service.context(f.identity).unwrap().scope().clone();
    f.ledger.reap_scope_ready(scope.id(), 32).unwrap();
    preparation
        .capacity
        .release(&preparation.capacity.resources().unwrap())
        .unwrap();
    drop(preparation);
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 0);
    assert_eq!(f.ledger.used(Dimension::PreparingBytes.id()), 0);
    finish_task_fixture(f, task);
}

#[test]
fn accepted_task_request_retires_its_own_r_without_refunding_the_still_queued_preparation() {
    let (mut f, task, preparation) = accepted_task_fixture();
    let ready = ready(&f, 1);
    let (work, mut handle) = f
        .service
        .context(f.identity)
        .unwrap()
        .admit_task_request(
            &ready,
            task,
            InputClass::Ordinary,
            operation(112),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .unwrap();
    f.service
        .context(f.identity)
        .unwrap()
        .submit_request(
            work,
            ContextCommand::GetDynamicFilters {
                task,
                acknowledged: None,
            },
        )
        .unwrap();
    let delivery = f
        .service
        .context(f.identity)
        .unwrap()
        .deliver_task_event(task)
        .unwrap()
        .unwrap();
    f.runtime.poll_one(Tick(7)).unwrap();
    let ticket = reply(&f, delivery.event);
    f.service
        .context(f.identity)
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    let delivered = handle.try_take().unwrap();
    assert!(matches!(
        delivered.body(),
        ContextReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::Idempotent))
    ));
    drop((handle, delivered));
    let mut retired = false;
    for _ in 0..8 {
        let (scope, actual) = ready
            .next_context_ready()
            .expect("actual R ready or bounded-reaper retry");
        assert_eq!(actual, work);
        assert_eq!(scope, f.service.context(f.identity).unwrap().scope().id());
        if f.service
            .context(f.identity)
            .unwrap()
            .advance_request(work)
            .unwrap()
        {
            retired = true;
            break;
        }
    }
    let own_r_returned = retired && f.ledger.used(Dimension::Requests.id()) == 0;
    let queued_preparation_still_paid = f.ledger.used(Dimension::PreparingTasks.id()) == 1
        && f.ledger.used(Dimension::PreparingBytes.id()) == 3
        && f.ledger.used(Dimension::ActiveTasks.id()) == 1;
    finish_accepted_task_fixture(f, task, preparation);
    assert!(own_r_returned && queued_preparation_still_paid);
}

#[test]
fn task_adoption_rejects_a_foreign_original_runtime_even_with_identical_wire_and_actor_identities()
{
    let mut owner = original_context(1, StorageNeed::default());
    let (mut original, task, work, create, load) =
        pending_loading_task_fixture(Some(owner.identity));
    assert_eq!(owner.identity, original.identity);
    let before = original
        .runtime
        .step_reservations(load.actor, load.input, None, ContextCore::MAX_STEP_WORK)
        .unwrap();
    assert_eq!(before.len(), 1);
    let binding = || crate::control_runtime::task::TaskHostBinding {
        source_storage_model: false,
        context: original.identity,
        task_source: ActorId::new(86).unwrap(),
        protocol_generation: generation(),
        max_cuts: 4,
        pending: Arc::new(RpcTaskPending),
        health: Arc::new(
            novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
        ),
    };
    let refused = owner.service.context(owner.identity).unwrap().adopt_task(
        &original.runtime,
        &load,
        binding(),
    );
    let wrong_runtime = matches!(refused, Err(ContextHostError::WrongRuntime));
    let guard_preserved = original
        .runtime
        .step_reservations(load.actor, load.input, None, ContextCore::MAX_STEP_WORK)
        .unwrap()
        == before;
    let foreign_not_owned = owner
        .service
        .context(owner.identity)
        .unwrap()
        .task(task)
        .is_none();
    // Teardown also remains honest if this origin regression reappears: move
    // that same wrongly adopted inventory back to its actual original Scope.
    // No replacement guard, admission, Work or physical-exit fact is invented.
    if refused.is_ok() {
        let actual = owner
            .service
            .context(owner.identity)
            .unwrap()
            .tasks
            .remove(&task)
            .unwrap();
        assert!(
            original
                .service
                .context(original.identity)
                .unwrap()
                .tasks
                .insert(task, actual)
                .is_none()
        );
    } else {
        original
            .service
            .context(original.identity)
            .unwrap()
            .adopt_task(&original.runtime, &load, binding())
            .unwrap();
    }
    finish_create_waiter(&mut original, work, create, load);
    finish_task_fixture(original, task);
    finish(owner);
    assert!(
        wrong_runtime && guard_preserved && foreign_not_owned,
        "wrong_runtime={wrong_runtime}, guard_preserved={guard_preserved}, foreign_not_owned={foreign_not_owned}"
    );
}
