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

//! Real Root Materialize/ScopeParent and direct Participation R custody. The fixture
//! has not run Native materialization: Renew returns the actual Establishing
//! Participation acknowledgement. These tests prove neither RF loans nor full S8 recovery.

#[path = "tests/task_prefix.rs"]
mod task_prefix;
#[path = "tests/task_routing.rs"]
mod task_routing;

use super::*;
use crate::control_runtime::{
    actors::ParticipationMessage,
    participation_host::requests::{ParticipationRequestError, ParticipationRequestPublishRefusal},
    request_ready::ControlRequestReady,
};
use crate::control_runtime::{
    source_positions::SOURCE_SLOT_STORAGE, source_slot_issuer::SourceSlotIssuePartial,
};
use novarocks_control_runtime::completion::ReportSlot;
use novarocks_control_runtime_core::{
    completion::{ConsumerKind, Consumption},
    identity::{ConsumerId, ScopedWorkRef},
};
use novarocks_execution_contract::LeaseSequence;
use novarocks_worker_protocol::participation::{
    ParticipationCommand, ParticipationEffect, ParticipationInput, ParticipationReplyBody,
};

fn original_participation(requests: usize) -> Harness {
    let mut f = Harness::new_with_request_capacity(PARTICIPATION_BOOTSTRAP_STORAGE, requests);
    let materialize = f.materialize();
    let binding = f.binding(participation_actor(), &materialize);
    f.service
        .adopt_participation(&materialize, binding)
        .unwrap();
    f.complete_materialize_handoff(materialize);
    f
}
fn ready_owner(f: &Harness) -> novarocks_control_runtime::actor_turn::ActorRegistration {
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .registration()
        .clone()
}
fn ready(f: &Harness, count: usize) -> ControlRequestReady {
    ControlRequestReady::new(NonZeroUsize::new(count).unwrap(), f.runtime.clone())
}
fn admit(
    f: &mut Harness,
    ready: &ControlRequestReady,
    operation: TaskOperationId,
) -> (
    ScopedWorkRef,
    crate::control_runtime::participation_host::requests::ParticipationRequestHandle,
) {
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
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
fn renew() -> ParticipationCommand {
    ParticipationCommand::Renew {
        sequence: LeaseSequence::new(1),
        valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
    }
}
fn submit_renew(f: &mut Harness, work: ScopedWorkRef) {
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .submit_request(work, renew())
        .unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
}
fn reply(f: &Harness, input: InputId) -> EffectTicket<WorkerAction> {
    let effects = f
        .runtime
        .ready_effects(
            participation_actor(),
            input,
            crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
        )
        .unwrap();
    let mut reply = None;
    for action in effects {
        let ticket = f
            .runtime
            .take_effect(participation_actor(), input, action)
            .unwrap();
        match &ticket.effect {
            WorkerAction::Reply(_) => {
                assert!(
                    reply.replace(ticket).is_none(),
                    "one complete Core-authored reply"
                );
            }
            WorkerAction::Participation(ParticipationEffect::CloseFanoutPrepared {
                targets: 0,
                ..
            }) => {
                // This component fixture has no Task fanout or physical host.
                // Consume the actual empty-scan fact, separately from its reply.
                f.runtime.effect_completed(ticket).unwrap();
            }
            _ => panic!("the original reply batch contains only its declared metadata"),
        }
    }
    reply.expect("one complete Core-authored reply")
}
fn advance(f: &mut Harness, work: ScopedWorkRef) -> bool {
    // The original reaper is bounded and may retire a sibling ready header
    // first. Do not alter protocol time or manufacture any exit observation.
    for _ in 0..8 {
        if f.service
            .participation(f.identity)
            .unwrap()
            .lock()
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
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .resources
            .requests
            .is_empty()
    );
    assert!(
        f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .resources
            .request_partials
            .is_empty()
    );
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .complete_failure_publications_for_test();
    f.dispose_service_bootstrap(participation_actor());
    f.finish_model_refusal();
}

#[test]
fn original_delivery_keeps_shared_r_until_its_actual_drop_and_core_receipt_survives_input_retirement()
 {
    let mut f = original_participation(1);
    let ready = ready(&f, 1);
    let (work, mut handle) = admit(&mut f, &ready, operation(71));
    let request = handle.request();
    submit_renew(&mut f, work);
    let core_committed = f
        .runtime
        .input_custody(participation_actor(), request.token)
        .unwrap()
        .is_some_and(|input| {
            input.phase == novarocks_control_runtime::actor_turn::InputPhase::Committed
        });
    let before_reply = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_request(work)
        .unwrap();
    let ticket = reply(&f, request.token);
    let decided = match &ticket.effect {
        WorkerAction::Reply(reply) => reply.body.clone(),
        _ => panic!("actual Core reply"),
    };
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    let delivery = handle.try_take().unwrap();
    let same_body = delivery.body() == decided.as_ref() && delivery.request() == request;
    let establishing_acknowledgement = matches!(
        decided.as_ref(),
        ParticipationReplyBody::Participation(ack)
            if ack.outcome == OperationOutcome::ParticipationTerminalReceipt
                && ack.receipt.participation() == f.identity
                && ack.receipt.state() == novarocks_execution_contract::ParticipationState::Establishing
                && ack.receipt.lease().is_some_and(|lease| lease.sequence() == LeaseSequence::INITIAL)
    );
    drop(handle); // Actual delivery, not the old wait handle, now owns the leg.
    let while_delivering = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_request(work)
        .unwrap();
    let input_retired = f
        .runtime
        .input_custody(participation_actor(), request.token)
        .unwrap()
        .is_none();
    let charged = f.ledger.used(Dimension::Requests.id());
    let refused = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
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
    assert!(same_body && establishing_acknowledgement && !while_delivering && input_retired);
    assert_eq!(charged, 1);
    assert!(full_without_partial && retired && refunded);
}

#[test]
fn canceled_external_wait_does_not_revoke_the_already_owned_core_command() {
    let mut f = original_participation(1);
    let ready = ready(&f, 1);
    let (work, handle) = admit(&mut f, &ready, operation(73));
    let input = handle.request().token;
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .submit_request(work, renew())
        .unwrap();
    drop(handle);
    let before_core = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
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
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    let retired = advance(&mut f, work);
    finish(f);

    assert!(!before_core && charged_before_core == 1);
    assert!(exact_reply && retired);
}

#[test]
fn matching_numeric_participation_work_and_input_cannot_publish_a_foreign_runtime_ticket() {
    // Each Participation came from its actual Root Materialize in a different real
    // runtime. Even matching ScopeId/WorkId/InputId and wire metadata do not
    // replace the private original runtime/ledger provenance.
    let mut a = original_participation(1);
    let mut b = original_participation(1);
    let ar = ready(&a, 1);
    let br = ready(&b, 1);
    let (aw, mut ah) = admit(&mut a, &ar, operation(74));
    let (bw, mut bh) = admit(&mut b, &br, operation(74));
    let same_numbers = aw == bw && ah.request().token == bh.request().token;
    submit_renew(&mut a, aw);
    submit_renew(&mut b, bw);
    let at = reply(&a, ah.request().token);
    let bt = reply(&b, bh.request().token);
    let original_a = match &at.effect {
        WorkerAction::Reply(reply) => reply.body.clone(),
        _ => panic!("the original A Core decision"),
    };
    let original_b = match &bt.effect {
        WorkerAction::Reply(reply) => reply.body.clone(),
        _ => panic!("the original B Core decision"),
    };
    let at = match b
        .service
        .participation(b.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(at)
    {
        Err(ParticipationRequestPublishRefusal::Publication { ticket, .. }) => ticket,
        Err(ParticipationRequestPublishRefusal::Rejected { ticket, .. }) => ticket,
        _ => panic!("foreign ticket must return its original custody"),
    };
    let bt = match a
        .service
        .participation(a.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(bt)
    {
        Err(ParticipationRequestPublishRefusal::Publication { ticket, .. }) => ticket,
        Err(ParticipationRequestPublishRefusal::Rejected { ticket, .. }) => ticket,
        _ => panic!("foreign ticket must return its original custody"),
    };
    let neither_published = ah.try_take().is_none() && bh.try_take().is_none();
    a.service
        .participation(a.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(at)
        .unwrap();
    b.service
        .participation(b.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(bt)
        .unwrap();
    let ad = ah.try_take().unwrap();
    let bd = bh.try_take().unwrap();
    // Participation acknowledgements carry their own exact Participation identity.
    // Each returned foreign ticket must preserve its own first decision.
    let same_decisions = ad.body() == original_a.as_ref()
        && bd.body() == original_b.as_ref()
        && matches!((ad.body(), bd.body()),
            (ParticipationReplyBody::Participation(a_ack), ParticipationReplyBody::Participation(b_ack))
                if a_ack.outcome == OperationOutcome::ParticipationTerminalReceipt
                    && b_ack.outcome == OperationOutcome::ParticipationTerminalReceipt
                    && a_ack.receipt.participation() == a.identity
                    && b_ack.receipt.participation() == b.identity
                    && a_ack.receipt.state() == b_ack.receipt.state());
    drop((ad, bd, ah, bh));
    let retired = advance(&mut a, aw) && advance(&mut b, bw);
    finish(a);
    finish(b);

    assert!(same_numbers && neither_published && same_decisions && retired);
}

#[test]
fn task_command_refusal_returns_the_original_and_the_unused_r_withdraws() {
    let mut f = original_participation(1);
    let ready = ready(&f, 1);
    let (work, handle) = admit(&mut f, &ready, operation(75));
    let input = handle.request().token;
    let task = crate::control_runtime::actors::tests::task(f.identity, 1);
    let command = ParticipationCommand::GetDynamicFilters {
        task,
        acknowledged: None,
    };
    let refused = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .submit_request(work, command)
        .expect_err("the original admission is refused");
    let original = matches!(refused.command,
        Some(ParticipationCommand::GetDynamicFilters { task: same, acknowledged: None }) if same == task);
    let typed = matches!(
        refused.error,
        ParticipationRequestError::TaskPredecessorRequired
    );
    let not_submitted = f
        .runtime
        .input_custody(participation_actor(), input)
        .unwrap()
        .is_some_and(|input| {
            input.phase == novarocks_control_runtime::actor_turn::InputPhase::Reserved
        });
    let retained = !f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_request(work)
        .unwrap();
    drop(handle);
    let withdrawn = advance(&mut f, work);
    finish(f);

    assert!(original && typed && not_submitted && retained && withdrawn);
}

fn unused_input_parcel(
    f: &Harness,
) -> (
    HostMetadataResources,
    novarocks_control_runtime::actor_turn::ReservedInputObligation<WorkerActor>,
) {
    let profile = StorageNeed {
        work: 1,
        inputs: 1,
        batches: 1,
        effects: crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
        ..StorageNeed::default()
    };
    let (mut work, capacity) = f
        .scope
        .reserve_next(
            generation(),
            profile.resources().unwrap(),
            WorkKind::Step,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Step)
        .unwrap();
    work.start().unwrap();
    let registration = ready_owner(f);
    let permit = f
        .runtime
        .reserve_admitted_input_for(
            &registration,
            ActorId::new(93).unwrap(),
            InputClass::Lifecycle,
            &work,
        )
        .unwrap();
    (HostMetadataResources { work, capacity }, permit)
}
fn retire_input_parcel(
    f: &Harness,
    mut owner: HostMetadataResources,
    permit: novarocks_control_runtime::actor_turn::ReservedInputObligation<WorkerActor>,
) {
    permit.revoke_unused().unwrap();
    let original = owner.work.retirement_observer();
    owner.work.body_exited().unwrap();
    owner.work.captures_exited().unwrap();
    f.ledger.reap_scope_ready(f.scope.id(), 16).unwrap();
    assert!(original.is_retired());
    finish_unused_bundle(&mut owner.capacity);
}
#[test]
fn unrelated_prepaid_lifecycle_position_does_not_borrow_the_original_rpc_inventory() {
    let mut f = original_participation(1);
    let (owner, permit) = unused_input_parcel(&f);
    let header = owner.work.retirement_observer();
    let ready = ready(&f, 1);
    let (work, handle) = admit(&mut f, &ready, operation(76));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    assert!(!header.is_retired());
    drop(handle);
    assert!(advance(&mut f, work));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert!(
        !header.is_retired(),
        "ending R cannot retire the unrelated original position owner"
    );
    retire_input_parcel(&f, owner, permit);
    finish(f);
}

fn exhausted_report_parcel(f: &Harness, count: usize) -> (HostMetadataResources, Vec<ReportSlot>) {
    let profile = StorageNeed {
        work: 1,
        completions: count,
        ..StorageNeed::default()
    };
    let (mut work, mut capacity) = f
        .scope
        .reserve_next(
            generation(),
            profile.resources().unwrap(),
            WorkKind::Step,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Step)
        .unwrap();
    work.start().unwrap();
    let reports = (0..count)
        .map(|_| {
            ReportSlot::reserve_next(
                &mut capacity,
                generation(),
                1,
                [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
            )
            .unwrap()
        })
        .collect();
    assert_eq!(capacity.resources().unwrap().owned_slots.completions, 0);
    (HostMetadataResources { work, capacity }, reports)
}
fn retire_report_parcel(f: &Harness, mut owner: HostMetadataResources, reports: Vec<ReportSlot>) {
    for report in reports {
        report.revoke_unused().unwrap();
        assert_eq!(
            report.consumption(ConsumerId::new(1).unwrap()).unwrap(),
            Consumption::Pending
        );
        assert!(!report.try_retire().unwrap());
        report.body_exited().unwrap();
        assert!(!report.try_retire().unwrap());
        report.captures_exited().unwrap();
        assert_eq!(
            report.consumption(ConsumerId::new(1).unwrap()).unwrap(),
            Consumption::Pending
        );
        assert!(report.try_retire().unwrap());
    }
    let header = owner.work.retirement_observer();
    owner.work.body_exited().unwrap();
    owner.work.captures_exited().unwrap();
    f.ledger.reap_scope_ready(f.scope.id(), 16).unwrap();
    assert!(header.is_retired());
    finish_unused_bundle(&mut owner.capacity);
}
#[test]
fn exhausted_original_report_parcel_does_not_borrow_the_independent_request_positions() {
    let mut f = original_participation(1);
    let (owner, reports) = exhausted_report_parcel(&f, 3);
    let ready = ready(&f, 1);
    let (work, mut handle) = admit(&mut f, &ready, operation(77));
    let request = handle.request();
    submit_renew(&mut f, work);
    let ticket = reply(&f, request.token);
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    let delivered = handle
        .try_take()
        .expect("the original R owns its distinct report/reply positions");
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    drop((handle, delivered));
    assert!(advance(&mut f, work));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    retire_report_parcel(&f, owner, reports);
    finish(f);
}

#[test]
fn delayed_quiesce_reply_uses_its_original_token_after_the_initial_input_retires() {
    let mut f = original_participation(1);
    let ready = ready(&f, 1);
    let (work, mut handle) = admit(&mut f, &ready, operation(78));
    let request = handle.request();
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .submit_request(work, ParticipationCommand::Quiesce)
        .unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
    let mut continue_seen = false;
    for action in f
        .runtime
        .ready_effects(
            participation_actor(),
            request.token,
            crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
        )
        .unwrap()
    {
        let ticket = f
            .runtime
            .take_effect(participation_actor(), request.token, action)
            .unwrap();
        match &ticket.effect {
            WorkerAction::Participation(ParticipationEffect::WakeInternal) => {
                continue_seen = true;
                f.service
                    .participation(f.identity)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .handoff_internal_wake(ticket)
                    .unwrap();
                continue;
            }
            WorkerAction::Participation(ParticipationEffect::CloseExternalAdmission { .. }) => {
                f.runtime
                    .close_actor_admission(participation_actor())
                    .unwrap();
            }
            _ => panic!("original Quiesce handoffs"),
        }
        f.runtime.effect_completed(ticket).unwrap();
    }
    let waiting = !f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_request(work)
        .unwrap();
    let original_input_retired = f
        .runtime
        .input_custody(participation_actor(), request.token)
        .unwrap()
        .is_none();
    // The actual Wake fills the Participation's original prepaid self-source. It
    // neither reserves another position nor borrows a second Work identity.
    let continuation = match f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_internal()
        .unwrap()
    {
        crate::control_runtime::participation_host::ParticipationInternalAdvance::Submitted(
            input,
        ) => input,
        other => panic!("the original quiesce continuation is ready: {other:?}"),
    };
    f.runtime.poll_one(Tick(2)).unwrap();
    let ticket = reply(&f, continuation);
    let late_original_reply = matches!(&ticket.effect,
        WorkerAction::Reply(reply) if reply.request == request
            && matches!(reply.body.as_ref(), ParticipationReplyBody::Quiesce { .. }));
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    f.runtime
        .retire_input(participation_actor(), continuation)
        .unwrap();
    let delivery = handle.try_take().unwrap();
    let charged = f.ledger.used(Dimension::Requests.id()) == 1;
    drop((delivery, handle));
    let retired = advance(&mut f, work);
    finish(f);

    assert!(continue_seen && waiting && original_input_retired);
    assert!(continuation != request.token && late_original_reply && charged && retired);
}

fn task_funding_participation() -> Harness {
    // The fixture supplies the actual Task-causal profile to the process
    // ledger. It does not borrow the Participation bootstrap's paid inventory.
    let mut f = Harness::new_with_request_capacity(PARTICIPATION_BOOTSTRAP_STORAGE, 1);
    let materialize = f.materialize();
    let binding = f.binding(participation_actor(), &materialize);
    f.service
        .adopt_participation(&materialize, binding)
        .unwrap();
    f.complete_materialize_handoff(materialize);
    f
}

#[test]
fn task_request_source_siblings_and_outer_keep_shared_r_after_the_original_author_retires() {
    use crate::control_runtime::reported_work_admission::adopt_reported_work;
    use novarocks_control_runtime::exit_custody::{ExitCustody, ExitCustodyPhase};

    let mut f = task_funding_participation();
    let dimensions = [Dimension::Requests];
    let baseline = dimensions.map(|dimension| f.ledger.used(dimension.id()));
    let mut all_authors_retired = true;
    let mut all_sources_still_live = true;
    let mut original_outer_preserved = true;
    let mut no_second_r = true;
    for _ in 0..3 {
        let funding = f
            .service
            .participation(f.identity)
            .unwrap()
            .lock()
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
        let (main, authority) = ExitCustody::from_admitted(adopt_reported_work(
            author.work,
            author.capacity,
            author.report,
        ));
        // These holders have never been exposed to a Task, producer or RPC.
        // Only their own actual unused exit may be observed here.
        authority.withdraw_unpublished_and_exit().unwrap();
        let scope = f.scope.clone();
        f.ledger.reap_scope_ready(scope.id(), 8).unwrap();
        // The Root-funded Sources are siblings of this original R. Its
        // header may already have reaped; still retire the actual ExitCustody.
        let main_role_retired = main.try_retire().unwrap();
        all_authors_retired &= main_retirement.is_retired()
            && main_role_retired
            && main.phase() == ExitCustodyPhase::Retired;
        all_sources_still_live &= source_retirements.iter().all(|source| !source.is_retired());
        let original_outer = outer.resources().unwrap();
        assert_eq!(original_outer.owned_slots.work_headers, 0);
        let original_source_payment = SOURCE_SLOT_STORAGE
            .checked_mul(2)
            .unwrap()
            .resources()
            .unwrap();
        let retained = original_outer
            .checked_add(&original_source_payment)
            .unwrap();
        for (index, dimension) in dimensions.iter().enumerate() {
            original_outer_preserved &= f.ledger.used(dimension.id())
                == baseline[index] + retained.quota.get(dimension.id());
        }
        let refusal = f
            .service
            .participation(f.identity)
            .unwrap()
            .lock()
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
        f.ledger.reap_scope_ready(scope.id(), 8).unwrap();
        assert!(main_retirement.is_retired());
        // Main Work has already gone; the same original outer is still the
        // unique paid reservation. It grants no new Work/derivation authority.
        outer.release_quota(&original_outer.quota).unwrap();
        outer.finish().unwrap();
        for (index, dimension) in dimensions.iter().enumerate() {
            assert_eq!(f.ledger.used(dimension.id()), baseline[index]);
        }
    }
    finish(f);
    assert!(all_authors_retired && all_sources_still_live);
    assert!(original_outer_preserved && no_second_r);
}

#[test]
fn exhausted_original_report_parcel_preserves_the_task_r_author_and_source_sibling_positions() {
    use crate::control_runtime::reported_work_admission::dispose_unused_completion;
    let f = task_funding_participation();
    let (owner, reports) = exhausted_report_parcel(&f, 3);
    let funding = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .reserve_task_request_funding()
        .unwrap();
    let financial_scope = funding.author.work.scope().clone();
    assert!(financial_scope.same_scope(&f.scope));
    let author_header = funding.author.work.retirement_observer();
    let source_headers = funding
        .sources
        .each_ref()
        .map(|s| s.reported().work().retirement_observer());
    let expected = SOURCE_SLOT_STORAGE.resources().unwrap().owned_slots;
    for source in &funding.sources {
        assert_eq!(source.reported().bound_storage_profile().unwrap(), expected);
    }
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    // Source2 are this original R's real children. They must leave first;
    // an unrelated report parcel neither supplies nor retires those holders.
    let mut partial = SourceSlotIssuePartial::from_admissions(&financial_scope, funding.sources);
    let mut exited = false;
    for _ in 0..8 {
        if partial.try_retire(&f.ledger).unwrap() {
            exited = true;
            break;
        }
    }
    assert!(exited);
    assert!(source_headers.iter().all(|source| source.is_retired()));
    assert!(!author_header.is_retired());
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    dispose_unused_completion(funding.author);
    financial_scope.reap_ready(16).unwrap();
    assert!(author_header.is_retired());
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    let mut outer = funding.outer;
    finish_unused_bundle(&mut outer);
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    retire_report_parcel(&f, owner, reports);
    finish(f);
}

#[test]
fn task_funding_in_a_cleanup_epoch_keeps_the_original_core_consumer_generation() {
    use crate::control_runtime::reported_work_admission::adopt_reported_work;
    use novarocks_control_runtime::exit_custody::ExitCustody;
    let f = task_funding_participation();
    let scope = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    let original_generation = scope.gates().unwrap().generation;
    let cleanup_generation = scope.fail().unwrap();
    assert!(cleanup_generation > original_generation);
    let funding = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
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
    let (main, authority) = ExitCustody::from_admitted(adopt_reported_work(
        funding.author.work,
        funding.author.capacity,
        funding.author.report,
    ));
    let no_invented_physical_exit = !main.body_exited() && !main.captures_exited();
    authority.withdraw_unpublished_and_exit().unwrap();
    let mut partial = SourceSlotIssuePartial::from_admissions(&f.scope, funding.sources);
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
    finish_unused_bundle(&mut outer);
    finish(f);
    assert!(original_core_legs && no_invented_physical_exit && retired);
}

// This is the actual Participation/Task/R owner graph with modeled neutral
// materialization and an unstarted loader. It claims no Native I/O or RF loan.
struct RpcTaskPending;
impl crate::task_port::TaskPendingNotice for RpcTaskPending {
    fn pending(&self) {}
}
pub(super) fn model_fact(f: &mut Harness, fact: ParticipationInput, at: u64) -> InputId {
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
    let input = {
        let address = f.service.participation(f.identity).unwrap();
        let host = address.lock().unwrap();
        f.runtime
            .reserve_admitted_input_for(
                host.registration(),
                ActorId::new(88).unwrap(),
                InputClass::Lifecycle,
                &host.resources.metadata_resources[TIMER].work,
            )
            .unwrap()
    }
    .fill(
        crate::control_runtime::actors::WorkerMessage::Participation(
            ParticipationMessage::Command {
                work,
                request: None,
                command: fact,
            },
        ),
    )
    .unwrap();
    f.runtime.poll_one(Tick(at)).unwrap();
    input
}
pub(super) fn complete_model_effects(f: &mut Harness, input: InputId) {
    complete_model_effects_with_preparation(f, input, None);
}
fn complete_model_effects_with_preparation(
    f: &mut Harness,
    input: InputId,
    mut preparation: Option<&mut Option<HostMetadataResources>>,
) {
    use novarocks_control_runtime_core::effect::EffectState;
    loop {
        let inventory = f
            .runtime
            .effect_inventory(
                participation_actor(),
                input,
                None,
                crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
            )
            .unwrap();
        for (action, phase) in &inventory {
            if *phase == EffectState::Pending {
                for predecessor in &action.dependencies {
                    if inventory.iter().any(|(actual, state)| {
                        actual.id == *predecessor && *state == EffectState::Completed
                    }) {
                        f.runtime
                            .acknowledge_dependency(
                                participation_actor(),
                                input,
                                action.id,
                                *predecessor,
                            )
                            .unwrap();
                    }
                }
            }
        }
        let ready = f
            .runtime
            .ready_effects(
                participation_actor(),
                input,
                crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
            )
            .unwrap();
        if ready.is_empty() {
            break;
        }
        for action in ready {
            let ticket = f
                .runtime
                .take_effect(participation_actor(), input, action)
                .unwrap();
            if let WorkerAction::Participation(ParticipationEffect::EnqueuePreparation {
                reservation,
                ..
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
                    slot.replace(HostMetadataResources { work, capacity })
                        .is_none()
                );
                f.runtime.effect_completed(ticket).unwrap();
            } else if let WorkerAction::Participation(ParticipationEffect::EstablishmentDecided {
                participation,
                decision,
                ..
            }) = &ticket.effect
            {
                assert_eq!(*participation, f.identity);
                let decision = decision.clone();
                assert!(
                    f.model_establishment_decision
                        .replace(decision.clone())
                        .is_none()
                );
                f.runtime.effect_completed(ticket).unwrap();
                assert!(
                    f.command(
                        ControlInput::EstablishmentDecided {
                            participation: f.identity,
                            decision
                        },
                        None
                    )
                    .is_none()
                );
            } else if let WorkerAction::Participation(
                ParticipationEffect::CloseExternalAdmission { generation, .. },
            ) = &ticket.effect
            {
                f.service
                    .participation(f.identity)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .resources
                    .execution_close_epoch = Some(*generation);
                f.runtime.effect_completed(ticket).unwrap();
            } else if matches!(ticket.effect, WorkerAction::Reply(_)) {
                f.service
                    .participation(f.identity)
                    .unwrap()
                    .lock()
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
    identity: Option<ParticipationRef>,
) -> (
    Harness,
    TaskIdentity,
    ScopedWorkRef,
    crate::control_runtime::participation_host::requests::ParticipationRequestHandle,
    EffectTicket<WorkerAction>,
) {
    pending_loading_task_fixture_with_submit(identity, |f, task, work| {
        f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .submit_request(
                work,
                ParticipationCommand::Create {
                    task,
                    payload: novarocks_worker_protocol::creation::PayloadId(1),
                },
            )
            .unwrap();
    })
}
fn pending_loading_task_fixture_with_submit(
    identity: Option<ParticipationRef>,
    submit: impl FnOnce(&mut Harness, TaskIdentity, ScopedWorkRef),
) -> (
    Harness,
    TaskIdentity,
    ScopedWorkRef,
    crate::control_runtime::participation_host::requests::ParticipationRequestHandle,
    EffectTicket<WorkerAction>,
) {
    let mut f =
        Harness::new_with_participation_identity(PARTICIPATION_BOOTSTRAP_STORAGE, 1, 1, identity);
    let materialize = f.materialize();
    let binding = f.binding(participation_actor(), &materialize);
    f.service
        .adopt_participation(&materialize, binding)
        .unwrap();
    f.complete_materialize_handoff(materialize);
    let activated = model_fact(
        &mut f,
        ParticipationInput::MaterializationFinished {
            result: Ok(crate::control_runtime::actors::tests::initial_materialized()),
        },
        2,
    );
    complete_model_effects(&mut f, activated);
    f.runtime
        .retire_input(participation_actor(), activated)
        .unwrap();
    let task = crate::control_runtime::actors::tests::task(f.identity, 1);
    let ready = ready(&f, 1);
    let (work, create) = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
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
    submit(&mut f, task, work);
    f.runtime.poll_one(Tick(3)).unwrap();
    let load_action = f
        .runtime
        .ready_effects(
            participation_actor(),
            input,
            crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
        )
        .unwrap()[0];
    let load = f
        .runtime
        .take_effect(participation_actor(), input, load_action)
        .unwrap();
    assert!(matches!(
        load.effect,
        WorkerAction::Participation(ParticipationEffect::LoadInput { .. })
    ));
    (f, task, work, create, load)
}
fn live_loading_task_fixture() -> (Harness, TaskIdentity) {
    let (mut f, task, work, create, load) = pending_loading_task_fixture(None);
    let registration = ready_owner(&f);
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .adopt_task(
            &f.runtime,
            &load,
            &crate::control_runtime::task::TaskHostBinding {
                registration: registration.clone(),
                participation: f.identity,
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
    work: ScopedWorkRef,
    mut create: crate::control_runtime::participation_host::requests::ParticipationRequestHandle,
    load: EffectTicket<WorkerAction>,
) {
    let input = create.request().token;
    f.runtime.effect_completed(load).unwrap();
    assert!(
        !f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_request(work)
            .unwrap()
    );
    assert!(
        f.runtime
            .input_custody(participation_actor(), input)
            .unwrap()
            .is_none()
    );
    // The Create waiter expires; its elected Task/loader responsibility stays.
    // No timeout or reply is used to claim that the loader actually exited.
    let tick = model_fact(f, ParticipationInput::Tick, 5);
    complete_model_effects(f, tick);
    f.runtime.retire_input(participation_actor(), tick).unwrap();
    let delivered = create.try_take().expect("original Create timeout reply");
    assert!(matches!(
        delivered.body(),
        ParticipationReplyBody::Verdict(RequestResult::TimeoutWhileAccepted)
    ));
    drop((create, delivered));
    assert!(advance(f, work));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
}
// These component fixtures never exposed the original disposal job. Its
// Available gate must revoke the same authority/report and reap its real
// Participation-lifetime sibling before Participation teardown can release that parent.
fn retire_unused_task_roles(
    f: &Harness,
    resources: &mut crate::control_runtime::task::TaskHostResources,
) {
    let mut retired = false;
    for _ in 0..64 {
        if resources.disposal.retire_unused(&f.ledger).unwrap() {
            retired = true;
            break;
        }
    }
    assert!(retired && resources.disposal.is_retired());
    if let Some(mut original) = resources.accepted_completion_storage.take() {
        assert!(original.resources().unwrap().quota.is_empty());
        original.finish().unwrap();
    }
}
// Reap each original Source through its actual financial owner. A rotated
// external RPC pair can belong to the Root even while this Task stays live.
fn reap_original_source_pair(
    ingress: &crate::task_proposal_ingress::TaskProposalIngress,
    route: crate::task_proposal_ingress::PairRoute,
) -> [crate::control_runtime::source_positions::ReapedPosition; 2] {
    for original in ingress.pair_inventory(route).unwrap() {
        let mut retired = original.custody.reusable();
        for _ in 0..64 {
            if retired {
                break;
            }
            assert!(original.custody.try_retire().unwrap());
            original.custody.financial_scope().reap_ready(1).unwrap();
            retired = original.custody.reusable();
        }
        assert!(
            retired,
            "the exact original Source header must really retire"
        );
    }
    ingress
        .prepare_pair_reap(route)
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap())
}
fn finish_task_fixture(f: Harness, task: TaskIdentity) {
    finish_task_fixture_with(f, task, |_| {});
}
fn finish_task_fixture_with(f: Harness, task: TaskIdentity, before_finish: impl FnOnce(&Harness)) {
    use crate::task_proposal_ingress::PairRoute;
    let host_address = f.service.participation(f.identity).unwrap();
    let mut host = host_address.lock().unwrap();
    host.close_task_request_admission(task).unwrap();
    let owner = host
        .take_task_for_teardown(task)
        .expect("original requests and cuts have drained");
    let crate::control_runtime::task::TaskHostOwner::Ready(task) = owner else {
        panic!("same actual ready Task");
    };
    drop(host);
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
    let scope = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    f.ledger.reap_scope_ready(scope.id(), 32).unwrap();
    for route in [PairRoute::Open, PairRoute::ConsumingOpen] {
        let reaped = reap_original_source_pair(&ingress, route);
        let storage = ingress
            .release_closed_route_pair(route, reaped)
            .unwrap_or_else(|_| panic!("actual source and summary teardown"));
        drop(storage); // Original reaped Source records carry no second payment.
    }
    crate::control_runtime::task::retire_unused_host_calls_for_test(
        &resources.host_calls,
        &f.ledger,
    );
    retire_unused_task_roles(&f, &mut resources);
    resources.lifetime.body_exited().unwrap();
    resources.lifetime.captures_exited().unwrap();
    if let Some(open) = &mut resources.open_summary {
        open.close_after_exit(&resources.lifetime).unwrap();
    }
    finish_unused_bundle(&mut resources.active);
    f.ledger.reap_scope_ready(scope.id(), 32).unwrap();
    drop((resources, ingress));
    before_finish(&f);
    finish(f);
}

#[test]
fn actual_task_r_repeats_at_r_one_and_retains_the_original_delivery_and_real_delayed_token() {
    let (f, task) = live_loading_task_fixture();
    let ready = ready(&f, 1);
    let mut prior_input = None;
    for sequence in 0..3 {
        let operation = operation(80 + sequence);
        let (work, mut handle) = f
            .service
            .participation(f.identity)
            .unwrap()
            .lock()
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
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .submit_request(work, ParticipationCommand::GetFinalInfo { task })
            .unwrap();
        let delivery = f
            .service
            .participation(f.identity)
            .unwrap()
            .lock()
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
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .publish_request_reply(ticket)
            .unwrap();
        let delivered = handle.try_take().unwrap();
        assert!(matches!(
            delivered.body(),
            ParticipationReplyBody::Verdict(RequestResult::Outcome(
                OperationOutcome::InvalidStateOrRequest
            ))
        ));
        drop(handle);
        assert!(
            !f.service
                .participation(f.identity)
                .unwrap()
                .lock()
                .unwrap()
                .advance_request(work)
                .unwrap()
        );
        assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
        assert!(
            f.runtime
                .input_custody(participation_actor(), delivery.event)
                .unwrap()
                .is_none()
        );
        assert!(original_token);
        drop(delivered);
        // Progress uses only the original real ready address, including the
        // retry emitted when a bounded reaper first visits another sibling.
        let mut retired = false;
        for _ in 0..8 {
            let actual = ready
                .next_participation_ready(&ready_owner(&f))
                .expect("same original R must remain ready");
            assert_eq!(actual, work);
            if f.service
                .participation(f.identity)
                .unwrap()
                .lock()
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
fn a_separate_original_lifecycle_position_does_not_refund_or_block_the_task_request_author() {
    let (mut f, task) = live_loading_task_fixture();
    let (owner, permit) = unused_input_parcel(&f);
    let ready = ready(&f, 1);
    let (work, handle) = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .admit_task_request(
            &ready,
            task,
            InputClass::Control,
            operation(110),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .unwrap();
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    assert_eq!(ready.registered(), 1);
    drop(handle);
    assert!(advance(&mut f, work));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert!(
        f.service
            .participation(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .task(task)
            .is_some()
    );
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    retire_input_parcel(&f, owner, permit);
    finish_task_fixture(f, task);
}

fn accepted_task_fixture() -> (Harness, TaskIdentity, HostMetadataResources) {
    let (mut f, task) = live_loading_task_fixture();
    // This neutral model has no measured Native input. Its explicit cost is
    // accepted by the real synchronous port, which owns the actual P/bytes
    // reservation below. No state flag or fabricated Installed fact is used.
    let loaded = model_fact(
        &mut f,
        ParticipationInput::Loaded {
            task,
            result: Ok(novarocks_worker_protocol::creation::CreationCost {
                input_retained_bytes: 1,
                ingress_retained_bytes: 1,
                structural_bytes: 1,
            }),
        },
        6,
    );
    let mut preparation = None;
    complete_model_effects_with_preparation(&mut f, loaded, Some(&mut preparation));
    f.runtime
        .retire_input(participation_actor(), loaded)
        .unwrap();
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 1);
    assert_eq!(f.ledger.used(Dimension::PreparingBytes.id()), 3);
    (
        f,
        task,
        preparation.expect("original Accepted queue reservation"),
    )
}
fn finish_accepted_task_fixture(
    f: Harness,
    task: TaskIdentity,
    preparation: HostMetadataResources,
) {
    finish_accepted_task_fixture_with(f, task, preparation, |_| {});
}
fn finish_accepted_task_fixture_with(
    f: Harness,
    task: TaskIdentity,
    mut preparation: HostMetadataResources,
    before_finish: impl FnOnce(&Harness),
) {
    // The test never starts this original queued preparation or captures a
    // host input/body. Its actual unused holder exits here before Task close.
    preparation.work.body_exited().unwrap();
    preparation.work.captures_exited().unwrap();
    let scope = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    f.ledger.reap_scope_ready(scope.id(), 32).unwrap();
    finish_unused_bundle(&mut preparation.capacity);
    drop(preparation);
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 0);
    assert_eq!(f.ledger.used(Dimension::PreparingBytes.id()), 0);
    finish_task_fixture_with(f, task, before_finish);
}

#[test]
fn accepted_task_request_retires_its_own_r_without_refunding_the_still_queued_preparation() {
    let (f, task, preparation) = accepted_task_fixture();
    let ready = ready(&f, 1);
    let (work, mut handle) = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
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
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .submit_request(
            work,
            ParticipationCommand::GetDynamicFilters {
                task,
                acknowledged: None,
            },
        )
        .unwrap();
    let delivery = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .deliver_task_event(task)
        .unwrap()
        .unwrap();
    f.runtime.poll_one(Tick(7)).unwrap();
    let ticket = reply(&f, delivery.event);
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    let delivered = handle.try_take().unwrap();
    assert!(matches!(
        delivered.body(),
        ParticipationReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::Idempotent))
    ));
    drop((handle, delivered));
    let mut retired = false;
    for _ in 0..8 {
        let actual = ready
            .next_participation_ready(&ready_owner(&f))
            .expect("actual R ready or bounded-reaper retry");
        assert_eq!(actual, work);
        if f.service
            .participation(f.identity)
            .unwrap()
            .lock()
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
    let owner = original_participation(1);
    let (mut original, task, work, create, load) =
        pending_loading_task_fixture(Some(owner.identity));
    assert_eq!(owner.identity, original.identity);
    let before = original
        .runtime
        .step_reservations(
            load.actor,
            load.input,
            None,
            crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
        )
        .unwrap();
    assert_eq!(before.len(), 1);
    let binding = || crate::control_runtime::task::TaskHostBinding {
        registration: original
            .service
            .participation(original.identity)
            .unwrap()
            .lock()
            .unwrap()
            .registration()
            .clone(),
        participation: original.identity,
        task_source: ActorId::new(86).unwrap(),
        protocol_generation: generation(),
        max_cuts: 4,
        pending: Arc::new(RpcTaskPending),
        health: Arc::new(
            novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
        ),
    };
    let foreign_binding = binding();
    let refused = owner
        .service
        .participation(owner.identity)
        .unwrap()
        .lock()
        .unwrap()
        .adopt_task(&original.runtime, &load, &foreign_binding);
    let wrong_runtime = matches!(refused, Err(ParticipationHostError::WrongRuntime));
    let guard_preserved = original
        .runtime
        .step_reservations(
            load.actor,
            load.input,
            None,
            crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
        )
        .unwrap()
        == before;
    let foreign_not_owned = owner
        .service
        .participation(owner.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .is_none();
    // Teardown also remains honest if this origin regression reappears: move
    // that same wrongly adopted inventory back to its actual original Scope.
    // No replacement guard, admission, Work or physical-exit fact is invented.
    if refused.is_ok() {
        let actual = owner
            .service
            .participation(owner.identity)
            .unwrap()
            .lock()
            .unwrap()
            .tasks
            .remove(&task)
            .unwrap();
        assert!(
            original
                .service
                .participation(original.identity)
                .unwrap()
                .lock()
                .unwrap()
                .tasks
                .insert(task, actual)
                .is_none()
        );
    } else {
        // Acquire the original registration before borrowing that same
        // Participation; evaluating the binding while locked would reenter it.
        let local_binding = binding();
        original
            .service
            .participation(original.identity)
            .unwrap()
            .lock()
            .unwrap()
            .adopt_task(&original.runtime, &load, &local_binding)
            .unwrap();
    }
    finish_create_waiter(&mut original, work, create, load);
    finish_task_fixture(original, task);
    finish(owner);
    assert!(
        wrong_runtime && guard_preserved && foreign_not_owned,
        "wrong_runtime={wrong_runtime}, guard_preserved={guard_preserved}, foreign_not_owned={foreign_not_owned}"
    );

    // A separate real fixture preserves the original foreign-origin and
    // healthy retry assertions above, then exercises the early failure cut.
    {
        use crate::control_runtime::{
            loading::TaskCreatePayload,
            task::{TaskHostBinding, TaskHostError, TaskHostOwner},
        };
        use novarocks_control_runtime_core::{
            completion::ReturnState,
            effect::{EffectError, EffectState},
        };
        use novarocks_execution::runtime::execution_health::ExecutionHealth;
        use novarocks_execution_contract::{
            CreateTask, CreationContent, ExchangeTopology, FrozenBytes, TaskCreationInput,
            TaskDescriptor,
        };
        use std::{
            any::Any,
            num::NonZeroUsize,
            sync::{
                Mutex,
                atomic::{AtomicUsize, Ordering},
            },
            thread::{self, ThreadId},
        };

        struct NoTaskEffects;
        impl crate::TaskProtocolObserver for NoTaskEffects {
            fn observe(&self, _: crate::TaskProtocolEvent) {
                panic!("a Reserved Task never publishes a Native Task event");
            }
        }
        impl crate::TaskResultLifecycle for NoTaskEffects {
            fn discard_task(&self, _: TaskIdentity) {
                panic!("a Reserved Task never owns a Task result");
            }
            fn retire_task_result(&self, _: TaskIdentity) {
                panic!("a Reserved Task never owns a Task result");
            }
        }
        impl crate::TaskExecutionMetrics for NoTaskEffects {
            fn record_task_created(&self) {
                panic!("a Reserved Task never reaches the Task creation gate");
            }
        }
        #[derive(Debug)]
        struct OriginalBacking {
            _permit: tokio::sync::OwnedSemaphorePermit,
            drops: Arc<AtomicUsize>,
        }
        impl crate::WorkerIngressBacking for OriginalBacking {}
        impl Drop for OriginalBacking {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        #[derive(Debug)]
        struct OriginalRaw {
            _backing: crate::WorkerIngressResponsibility,
            drops: Arc<AtomicUsize>,
            getters: Arc<AtomicUsize>,
            thread: Arc<Mutex<Option<ThreadId>>>,
        }
        impl CreationContent for OriginalRaw {
            fn encoded_len(&self) -> usize {
                self.getters.fetch_add(1, Ordering::SeqCst);
                1
            }
            fn retained_bytes(&self) -> usize {
                self.getters.fetch_add(1, Ordering::SeqCst);
                1
            }
            fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
                self.getters.fetch_add(1, Ordering::SeqCst);
                self
            }
        }
        impl Drop for OriginalRaw {
            fn drop(&mut self) {
                *self.thread.lock().unwrap() = Some(thread::current().id());
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }

        let raw_drops = Arc::new(AtomicUsize::new(0));
        let raw_getters = Arc::new(AtomicUsize::new(0));
        let backing_drops = Arc::new(AtomicUsize::new(0));
        let raw_thread = Arc::new(Mutex::new(None));
        // This is one actual component ingress position, retained inside the
        // original opaque capture. It does not claim Native permit geometry.
        let backing_gate = Arc::new(tokio::sync::Semaphore::new(1));
        let backing =
            crate::WorkerIngressResponsibility::from_admitted(Arc::new(OriginalBacking {
                _permit: backing_gate.clone().try_acquire_owned().unwrap(),
                drops: backing_drops.clone(),
            }));
        let health = Arc::new(ExecutionHealth::new());
        let raw = OriginalRaw {
            _backing: backing,
            drops: raw_drops.clone(),
            getters: raw_getters.clone(),
            thread: raw_thread.clone(),
        };
        let (mut failed, failed_task, failed_work, mut failed_create, failed_load) =
            pending_loading_task_fixture_with_submit(None, |f, task, work| {
                let registration = ready_owner(f);
                let descriptor = TaskDescriptor::try_new(
                    task,
                    novarocks_types::UniqueId::new(77, 88),
                    NonZeroUsize::new(1).unwrap(),
                    vec![],
                    ExchangeTopology::default(),
                )
                .unwrap();
                let request = Arc::new(
                    CreateTask::try_new(operation(79), f.identity, descriptor, vec![]).unwrap(),
                );
                let binding = TaskHostBinding {
                    registration,
                    participation: f.identity,
                    task_source: ActorId::new(86).unwrap(),
                    protocol_generation: generation(),
                    max_cuts: 4,
                    pending: Arc::new(RpcTaskPending),
                    health: health.clone(),
                };
                let payload = TaskCreatePayload {
                    id: PayloadId(1),
                    request,
                    input: TaskCreationInput::new(
                        FrozenBytes::freeze_encoded(vec![1]),
                        Box::new(raw),
                    ),
                    ingress_retained_bytes: 0,
                };
                f.service
                    .participation(f.identity)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .submit_task_create(work, payload, binding)
                    .unwrap_or_else(|_| panic!("the same original R captures its rich Create"));
            });
        let failed_input = failed_load.input;
        let task_work = failed_load.descriptor.work;
        let guard_before = failed
            .runtime
            .step_reservations(
                participation_actor(),
                failed_input,
                None,
                crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
            )
            .unwrap();
        assert_eq!(guard_before.len(), 1);
        assert_eq!(guard_before[0].1, task_work.work);
        let (participation_scope, r_header, root_registration) = {
            let address = failed.service.participation(failed.identity).unwrap();
            let host = address.lock().unwrap();
            let r = host.resources.requests[&failed_work]
                .observation_for_test()
                .unwrap();
            assert_eq!(r.return_state, ReturnState::Reserved);
            (
                host.scope().clone(),
                r.header,
                host.control_registration.clone(),
            )
        };
        assert_eq!(task_work.scope, participation_scope.id());
        assert_eq!(failed.ledger.used(Dimension::ActiveTasks.id()), 1);
        assert_eq!(failed.ledger.used(Dimension::Requests.id()), 1);
        assert!(
            participation_scope
                .work_lifecycle(task_work.work)
                .unwrap()
                .is_some()
        );
        assert!(!r_header.is_retired());
        // This is the actual healthy outer preflight, before the same Core
        // fails between that check and the inner original-guard handoff.
        failed.runtime.validate_effect_ticket(&failed_load).unwrap();
        let failure = failure_publications::actual_failure(&mut failed);
        let failure_input = failure.input;
        assert!(
            failed
                .runtime
                .effect_completion_observer(&failed_load)
                .is_ok()
        );
        assert!(
            failed
                .runtime
                .effect_inventory(participation_actor(), failed_input, None, 1)
                .unwrap()
                .iter()
                .any(|(_, state)| *state == EffectState::Started)
        );
        assert_eq!(
            failed
                .runtime
                .step_reservations(participation_actor(), failed_input, None, 1)
                .unwrap(),
            guard_before
        );
        let binding = TaskHostBinding {
            registration: ready_owner(&failed),
            participation: failed.identity,
            task_source: ActorId::new(86).unwrap(),
            protocol_generation: generation(),
            max_cuts: 4,
            pending: Arc::new(RpcTaskPending),
            health: health.clone(),
        };
        {
            let address = failed.service.participation(failed.identity).unwrap();
            let mut host = address.lock().unwrap();
            let error = host
                .adopt_task(&failed.runtime, &failed_load, &binding)
                .unwrap_err();
            assert!(matches!(
                error,
                ParticipationHostError::Task(TaskHostError::ActorTurn(
                    ActorTurnError::FailurePublicationRequired
                        | ActorTurnError::Effect(EffectError::StaleGeneration)
                ))
            ));
            assert!(matches!(host.tasks.get(&failed_task),
                Some(TaskHostOwner::Unpublished(owner)) if owner.matches_loading(&failed_load)));
            assert!(host.resources.pending_host_jobs.actor_effect.is_none());
            // Move the same Started ticket into the existing refusal custody;
            // no new dispatch, guard, report or permission is manufactured.
            host.resources.pending_host_jobs.actor_effect = Some(failed_load);
            host.begin_failure_publications(failure).unwrap();
        }
        assert!(
            failed
                .runtime
                .step_reservations(participation_actor(), failed_input, None, 1)
                .unwrap()
                .is_empty(),
            "the original guard moved out exactly once"
        );
        assert!(
            participation_scope
                .work_lifecycle(task_work.work)
                .unwrap()
                .is_some()
        );
        assert_eq!(failed.ledger.used(Dimension::ActiveTasks.id()), 1);
        assert_eq!(raw_drops.load(Ordering::SeqCst), 0);
        assert_eq!(raw_getters.load(Ordering::SeqCst), 0);
        assert_eq!(backing_drops.load(Ordering::SeqCst), 0);
        let (executor, mut ports) =
            failure_publications::original_lifecycle_executor(&failed.runtime, root_registration);
        let no_task = Arc::new(NoTaskEffects);
        ports.protocol_events = Some(crate::TaskExecutionPorts::new(
            no_task.clone(),
            no_task.clone(),
            no_task,
            health,
        ));
        {
            let address = failed.service.participation(failed.identity).unwrap();
            let mut host = address.lock().unwrap();
            assert!(matches!(
                host.advance_failed_task_cleanup(&ports, 0),
                Err(
                    crate::control_runtime::participation_host::ParticipationIoHandoffError::Failure(
                        failure::FailurePublicationError::NotDrained
                    )
                )
            ));
            assert!(
                participation_scope
                    .work_lifecycle(task_work.work)
                    .unwrap()
                    .is_some()
            );
            assert_eq!(failed.ledger.used(Dimension::ActiveTasks.id()), 1);
        }
        let (drained, supervisor_alive) =
            failure_publications::drain_one_unit_turns(&mut failed, failure_input);
        assert!(drained && supervisor_alive);
        let failed_delivery = failed_create
            .try_take()
            .expect("the original Create receives OwnerFailed");
        assert!(matches!(
            failed_delivery.body(),
            ParticipationReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed))
        ));
        let mut task_retired = false;
        for _ in 0..128 {
            let address = failed.service.participation(failed.identity).unwrap();
            let mut host = address.lock().unwrap();
            let runnable = host.advance_failed_task_cleanup(&ports, 1).unwrap();
            if !host.tasks.contains_key(&failed_task) {
                task_retired = true;
                break;
            }
            assert!(
                runnable,
                "the original unused metadata cannot park on an external R"
            );
        }
        assert!(task_retired);
        assert!(
            failed
                .runtime
                .input_custody(participation_actor(), failed_input)
                .unwrap()
                .is_none()
        );
        assert!(
            participation_scope
                .work_lifecycle(task_work.work)
                .unwrap()
                .is_none()
        );
        assert_eq!(failed.ledger.used(Dimension::ActiveTasks.id()), 0);
        assert_eq!(failed.ledger.used(Dimension::Requests.id()), 1);
        assert!(!r_header.is_retired());
        assert_eq!(raw_drops.load(Ordering::SeqCst), 0);
        assert_eq!(raw_getters.load(Ordering::SeqCst), 0);
        assert_eq!(backing_drops.load(Ordering::SeqCst), 0);
        assert_eq!(backing_gate.available_permits(), 0);
        {
            let address = failed.service.participation(failed.identity).unwrap();
            let mut host = address.lock().unwrap();
            host.advance_request_notification(failed_work, &ports)
                .unwrap();
            assert!(!host.advance_request(failed_work).unwrap());
        }
        assert_eq!(raw_drops.load(Ordering::SeqCst), 0);
        drop((failed_create, failed_delivery));
        failed
            .service
            .participation(failed.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_request_notification(failed_work, &ports)
            .unwrap();
        drop(
            executor
                .shutdown()
                .unwrap_or_else(|_| panic!("the original disposal host joins")),
        );
        assert_eq!(raw_drops.load(Ordering::SeqCst), 1);
        assert_eq!(raw_getters.load(Ordering::SeqCst), 0);
        assert_eq!(backing_drops.load(Ordering::SeqCst), 1);
        assert_eq!(backing_gate.available_permits(), 1);
        assert_ne!(raw_thread.lock().unwrap().unwrap(), thread::current().id());
        {
            let address = failed.service.participation(failed.identity).unwrap();
            let host = address.lock().unwrap();
            assert_eq!(
                host.resources.requests[&failed_work]
                    .observation_for_test()
                    .unwrap()
                    .return_state,
                ReturnState::Filled
            );
        }
        // Pure custody takes the actual physical return; no report is filled
        // or consumed by this test and no authority exit is synthesized.
        assert!(
            failed
                .service
                .participation(failed.identity)
                .unwrap()
                .lock()
                .unwrap()
                .advance_request(failed_work)
                .unwrap()
        );
        assert!(r_header.is_retired());
        assert_eq!(failed.ledger.used(Dimension::Requests.id()), 0);
        drop(ports);
        failure_publications::finish_failed_fixture(failed, failure_input);
    }
}

#[path = "tests/source_slot_lifetime.rs"]
mod source_slot_lifetime;

#[path = "tests/failure_publications.rs"]
mod failure_publications;

#[path = "tests/failed_runtime_inputs.rs"]
mod failed_runtime_inputs;

#[path = "tests/task_host_calls.rs"]
mod task_host_calls;

#[path = "tests/internal.rs"]
mod internal;
