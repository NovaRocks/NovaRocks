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

//! Actual common roles, original causal senders and real Core input receipts.
//! These empty-vector protocol fixtures prove no production R escrow budget,
//! Native getter/delivery exit, RF loan or S8 recovery qualification.

use super::*;
use crate::control_runtime::admitted_role::dispose_unused_completion;
use novarocks_control_runtime::exit_role::ExitRolePhase;

fn metadata() -> RequestMetadata {
    RequestMetadata {
        operation: operation(91),
        deadline: MonotonicInstant::from_origin(Duration::from_nanos(99)),
    }
}
fn admit_original(h: &Harness) -> (RpcAuthorAdmission, ReportSlot) {
    let generation = h.fixture.scope.gates().unwrap().generation;
    let mut guard = h
        .fixture
        .scope
        .reserve_child_next(
            generation,
            h.parent,
            novarocks_control_runtime_core::capacity::ReservationResources::new(
                ResourceVector::default(),
                novarocks_control_runtime_core::owned_storage::OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    inputs: 2,
                    batches: 2,
                    effects: 2 * crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
                    replies: 1,
                    ..novarocks_control_runtime_core::owned_storage::OwnedSlotNeed::default()
                },
            ),
            WorkKind::Cleanup,
        )
        .unwrap();
    let report = ReportSlot::reserve_next(
        &mut guard,
        generation,
        2,
        [
            (
                ConsumerId::new(1).unwrap(),
                ConsumerKind::Owner {
                    scope: h.fixture.scope.id(),
                    generation,
                },
            ),
            (ConsumerId::new(2).unwrap(), ConsumerKind::Observer),
        ],
    )
    .unwrap();
    let (work, capacity) = guard.bind(WorkKind::Cleanup).unwrap();
    let admitted = AdmittedCompletionRole {
        work,
        capacity,
        report,
    };
    let report = admitted.report.clone();
    let rpc = h
        .inbox
        .lock()
        .unwrap()
        .admit_rpc_author(
            admitted,
            metadata(),
            InputClass::Control,
            1,
            Arc::new(Pending),
            h.admission(),
        )
        .unwrap();
    (rpc, report)
}
fn main_retired(h: &Harness, role: &ExitRole) -> bool {
    h.fixture
        .ledger
        .reap_scope_ready(h.fixture.scope.id(), 256)
        .unwrap();
    role.try_retire().unwrap()
}
fn retire_cut(h: &Harness, cut: CutId) -> RetiredCut {
    h.inbox
        .lock()
        .unwrap()
        .retire_after_exit(cut, CausalSourceRetirement::Unclaimed)
        .unwrap()
}

fn complete_model_effects(h: &Harness, input: InputId) -> Vec<WorkerAction> {
    use novarocks_control_runtime_core::effect::EffectState;
    let mut effects = Vec::new();
    loop {
        let inventory = h
            .fixture
            .runtime
            .effect_inventory(
                actor(),
                input,
                None,
                crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
            )
            .unwrap();
        for (descriptor, state) in &inventory {
            if *state == EffectState::Pending {
                for predecessor in &descriptor.dependencies {
                    if inventory.iter().any(|(original, phase)| {
                        original.id == *predecessor && *phase == EffectState::Completed
                    }) {
                        h.fixture
                            .runtime
                            .acknowledge_dependency(actor(), input, descriptor.id, *predecessor)
                            .unwrap();
                    }
                }
            }
        }
        let ready = h
            .fixture
            .runtime
            .ready_effects(
                actor(),
                input,
                crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
            )
            .unwrap();
        if ready.is_empty() {
            break;
        }
        for action in ready {
            let ticket = h
                .fixture
                .runtime
                .take_effect(actor(), input, action)
                .unwrap();
            effects.push(ticket.effect.clone());
            h.fixture.runtime.effect_completed(ticket).unwrap();
        }
    }
    effects
}

#[test]
fn rpc_sender_return_and_core_receipt_do_not_exit_or_consume_the_long_lived_main_role() {
    let h = Harness::new();
    let (
        RpcAuthorAdmission {
            cut,
            role,
            mut authority,
            sender,
            input_author: _,
        },
        report,
    ) = admit_original(&h);
    authority.start().unwrap();
    let no_token_before_fill = h.inbox.lock().unwrap().rpc_delivery(cut).unwrap().is_none();
    let returned = sender
        .send_input(ContextInput::Request(ContextCommand::Cancel {
            task: h.task,
            reason: CancelReason::UpstreamNoLongerNeeded,
        }))
        .unwrap();
    h.inbox
        .lock()
        .unwrap()
        .observe_rpc_author_return(returned)
        .unwrap();
    let main_still_started = role.phase() == ExitRolePhase::Started;
    let before_event_pending =
        report.consumption(ConsumerId::new(1).unwrap()).unwrap() == Consumption::Pending;
    let delivery = h.inbox.lock().unwrap().deliver_next().unwrap().unwrap();
    let exact_receipt = h.inbox.lock().unwrap().rpc_delivery(cut).unwrap() == Some(delivery);
    h.fixture.runtime.poll_one(Tick(2)).unwrap();
    let observed = h.inbox.lock().unwrap().observe_core(cut).unwrap();
    let after_event_pending =
        report.consumption(ConsumerId::new(1).unwrap()).unwrap() == Consumption::Pending;
    if let Some(prefix) = delivery.prefix {
        complete_model_effects(&h, prefix);
    }
    let replies = complete_model_effects(&h, delivery.event);
    let exact_reply = replies.iter().any(|effect| {
        matches!(effect,
        WorkerAction::Reply(reply) if reply.request.token == delivery.event
            && reply.request.operation == metadata().operation)
    });

    // This test now actually ends its original author holder, after its actual
    // effects exit but BEFORE retiring its still-live input header. No Core
    // commit, sender return or header disappearance signs the authority for it.
    authority.exited().unwrap();
    report.consume(ConsumerId::new(1).unwrap()).unwrap();
    report.consume(ConsumerId::new(2).unwrap()).unwrap();
    let original_main_retired = main_retired(&h, &role);
    let header_still_present = h
        .fixture
        .runtime
        .input_custody(actor(), delivery.event)
        .unwrap()
        .is_some();
    let blocked_on_headers = h
        .inbox
        .lock()
        .unwrap()
        .retire_after_exit(cut, CausalSourceRetirement::Unclaimed)
        .is_err();
    if let Some(prefix) = delivery.prefix {
        h.fixture.runtime.retire_input(actor(), prefix).unwrap();
    }
    h.fixture
        .runtime
        .retire_input(actor(), delivery.event)
        .unwrap();
    let saved_delivery = h.inbox.lock().unwrap().rpc_delivery(cut).unwrap();
    let saved_event_consumed = h.inbox.lock().unwrap().event_core_consumed(cut).unwrap();
    let receipt_survives_header = saved_delivery == Some(delivery) && saved_event_consumed;
    h.inbox.lock().unwrap().observe_effects_exited(cut).unwrap();
    h.ingress.consume_cut(cut, 0).unwrap();
    let retired = retire_cut(&h, cut);
    h.dispose_pair(retired);
    h.finish();

    assert!(no_token_before_fill && exact_receipt && exact_reply);
    assert!(main_still_started && before_event_pending && after_event_pending);
    assert_eq!(observed, Consumption::Consumed);
    assert!(receipt_survives_header && header_still_present && blocked_on_headers);
    assert!(original_main_retired);
}

#[test]
fn unused_unique_rpc_return_rejects_another_inbox_and_never_fabricates_an_event() {
    let h = Harness::new();
    let other = Harness::new();
    let (
        RpcAuthorAdmission {
            cut,
            role,
            authority,
            sender,
            input_author: _,
        },
        report,
    ) = admit_original(&h);
    let returned = sender.withdraw();
    let (error, returned) = other
        .inbox
        .lock()
        .unwrap()
        .observe_rpc_author_return(returned)
        .expect_err("another original inbox cannot consume this return");
    h.inbox
        .lock()
        .unwrap()
        .observe_rpc_author_return(returned)
        .unwrap();
    let no_delivery = h.inbox.lock().unwrap().rpc_delivery(cut).unwrap().is_none();
    let no_event_consumption = h.inbox.lock().unwrap().event_core_consumed(cut).is_err();
    let no_event = no_delivery && no_event_consumption;
    let main_not_exited = role.phase() == ExitRolePhase::Admitted;
    let report_not_consumed =
        report.consumption(ConsumerId::new(1).unwrap()).unwrap() == Consumption::Pending;
    h.inbox.lock().unwrap().observe_effects_exited(cut).unwrap();
    let blocked_on_main = h
        .inbox
        .lock()
        .unwrap()
        .retire_after_exit(cut, CausalSourceRetirement::Unclaimed)
        .is_err();
    authority.withdraw_unpublished_and_exit().unwrap();
    let unused_still_not_consumed =
        report.consumption(ConsumerId::new(1).unwrap()).unwrap() == Consumption::Pending;
    let original_main_retired = main_retired(&h, &role);
    let retired = retire_cut(&h, cut);
    h.dispose_pair(retired);
    h.finish();
    other.finish();

    assert_eq!(error, CausalError::WrongTask);
    assert!(no_event && main_not_exited && report_not_consumed && unused_still_not_consumed);
    assert!(blocked_on_main && original_main_retired);
}

#[test]
fn foreign_ledger_whole_author_with_the_same_numeric_scope_and_work_returns_every_original() {
    let h = Harness::new();
    let other = Harness::new();
    let local = source_role(&h.fixture.scope, h.parent);
    let foreign = source_role(&other.fixture.scope, other.parent);
    let same_numbers = local.work.id() == foreign.work.id()
        && local.work.scope().id() == foreign.work.scope().id();
    let original_work = foreign.work.id();
    let original_capacity = foreign.capacity.id();
    let original_report = foreign.report.clone();
    let admission = h.admission();
    let CutSourceAdmission::Fresh(sources) = &admission else {
        unreachable!()
    };
    let original_sources = sources.each_ref().map(|source| {
        (
            source.reported().work().id(),
            source.reported().report().clone(),
        )
    });
    let actor_before = h.fixture.runtime.snapshot(actor()).unwrap();
    let refused = h.inbox.lock().unwrap().admit_rpc_author(
        foreign,
        metadata(),
        InputClass::Ordinary,
        1,
        Arc::new(Pending),
        admission,
    );
    let (error, admitted, admission) = match refused {
        Err(RpcAuthorAdmissionRefusal::BeforeCut {
            error,
            admitted,
            admission,
        }) => (error, admitted, admission),
        _ => panic!("a different ledger cannot lend an original author to this actor"),
    };
    let whole_unchanged = admitted.work.id() == original_work
        && admitted.capacity.id() == original_capacity
        && admitted.report.same_slot(&original_report);
    let untouched_actor = h.fixture.runtime.snapshot(actor()).unwrap() == actor_before
        && h.inbox.lock().unwrap().is_empty();
    let CutSourceAdmission::Fresh(sources) = admission else {
        unreachable!()
    };
    let source_unchanged =
        sources
            .each_ref()
            .iter()
            .zip(original_sources.iter())
            .all(|(source, (work, report))| {
                source.reported().work().id() == *work
                    && source.reported().report().same_slot(report)
            });
    dispose_unused_completion(admitted);
    dispose_unused_completion(local);
    let mut partial =
        crate::control_runtime::source_slot_issuer::SourceSlotIssuePartial::from_admissions(
            &h.fixture.scope,
            sources,
        );
    let mut partial_retired = false;
    for _ in 0..256 {
        if partial.try_retire(&h.fixture.ledger).unwrap() {
            partial_retired = true;
            break;
        }
    }
    assert!(partial_retired);
    h.fixture
        .ledger
        .reap_scope_ready(h.fixture.scope.id(), 256)
        .unwrap();
    other
        .fixture
        .ledger
        .reap_scope_ready(other.fixture.scope.id(), 256)
        .unwrap();
    h.finish();
    other.finish();

    assert!(same_numbers && whole_unchanged && untouched_actor && source_unchanged);
    assert!(matches!(
        error,
        RpcAuthorAdmissionError::Causal(CausalError::Driver(DriverError::InputAuthorMismatch))
    ));
}

/// End only the original producer positions. The independent, already-admitted
/// RPC still owns its original cut, Source2 and whole R author throughout.
fn retire_original_producer_pairs(h: &Harness) {
    use crate::task_proposal_ingress::PairRoute;

    h.ingress.seal_source_admission();
    assert!(h.ingress.open_summary_drained());
    for route in [PairRoute::Open, PairRoute::ConsumingOpen] {
        h.inbox
            .lock()
            .unwrap()
            .observe_closed_source_pair(route)
            .unwrap();
        h.fixture
            .ledger
            .reap_scope_ready(h.fixture.scope.id(), 256)
            .unwrap();
        let reaped = h
            .ingress
            .prepare_pair_reap(route)
            .unwrap()
            .map(|probe| probe.observe_reaped().unwrap());
        let storage = h
            .ingress
            .release_physically_closed_producer_pair(route, reaped)
            .unwrap();
        // These exact original Source headers/reports and backing storage have
        // really exited before the same admitted RPC sender is invoked.
        drop(storage);
    }
}

/// The two producer pairs were already physically detached by the actual
/// original API. Do not mint substitutes merely to use the ordinary wrapper.
fn finish_after_producer_storage_exit(mut h: Harness) {
    use crate::task_proposal_ingress::{IngressError, PairRoute};
    use novarocks_worker_protocol::proposal_summary::CutError;

    h.exit_author();
    assert!(h.inbox.lock().unwrap().is_empty());
    for route in [PairRoute::Open, PairRoute::ConsumingOpen] {
        assert!(matches!(
            h.ingress.pair_inventory(route),
            Err(IngressError::Cut(CutError::Unknown))
        ));
    }
    assert_eq!(h.ingress.retained_sources(), 0);
    drop(h.inbox);
    drop(h.ingress);
    h.fixture
        .ledger
        .reap_scope_ready(h.fixture.scope.id(), 256)
        .unwrap();
    h.capacity
        .release_quota(&h.capacity.resources().unwrap().quota)
        .unwrap();
    h.capacity.finish().unwrap();
    // This original fixture closes/removes the actual actor and Scope and
    // asserts that its complete real ledger is empty before returning.
    h.fixture.finish();
}

#[test]
fn admitted_rpc_sender_can_fill_after_original_producer_storage_physically_exits() {
    let h = Harness::new();
    let (
        RpcAuthorAdmission {
            cut,
            role,
            mut authority,
            sender,
            input_author,
        },
        report,
    ) = admit_original(&h);
    authority.start().unwrap();
    retire_original_producer_pairs(&h);
    let original_r_still_started = role.phase() == ExitRolePhase::Started;
    let original_cut_still_owned = h.inbox.lock().unwrap().last_cut() == Some(cut);
    let no_event_before_fill = h.inbox.lock().unwrap().rpc_delivery(cut).unwrap().is_none();
    let original_report_pending =
        report.consumption(ConsumerId::new(1).unwrap()).unwrap() == Consumption::Pending;

    let (sender_registered, original_input_returned, original_r_retired, core_replied) =
        match sender.send_input(ContextInput::Request(ContextCommand::Cancel {
            task: h.task,
            reason: CancelReason::UpstreamNoLongerNeeded,
        })) {
            Ok(returned) => {
                h.inbox
                    .lock()
                    .unwrap()
                    .observe_rpc_author_return(returned)
                    .unwrap();
                let delivery = h.inbox.lock().unwrap().deliver_next().unwrap().unwrap();
                h.fixture.runtime.poll_one(Tick(2)).unwrap();
                assert_eq!(
                    h.inbox.lock().unwrap().observe_core(cut).unwrap(),
                    Consumption::Consumed
                );
                if let Some(prefix) = delivery.prefix {
                    complete_model_effects(&h, prefix);
                }
                let effects = complete_model_effects(&h, delivery.event);
                let core_replied = effects.iter().any(|effect| {
                    matches!(effect,
                        WorkerAction::Reply(reply)
                            if reply.request.token == delivery.event
                                && reply.request.operation == metadata().operation)
                });
                authority.exited().unwrap();
                report.consume(ConsumerId::new(1).unwrap()).unwrap();
                report.consume(ConsumerId::new(2).unwrap()).unwrap();
                let original_r_retired = main_retired(&h, &role);
                if let Some(prefix) = delivery.prefix {
                    h.fixture.runtime.retire_input(actor(), prefix).unwrap();
                }
                h.fixture
                    .runtime
                    .retire_input(actor(), delivery.event)
                    .unwrap();
                h.inbox.lock().unwrap().observe_effects_exited(cut).unwrap();
                h.ingress.consume_cut(cut, 0).unwrap();
                (true, true, original_r_retired, core_replied)
            }
            Err(RpcInputRefusal { input, returned }) => {
                let original_input_returned = matches!(&input,
                    ContextInput::Request(ContextCommand::Cancel { task, reason })
                        if *task == h.task && *reason == CancelReason::UpstreamNoLongerNeeded);
                // The failed send retained the same input and unique return.
                // No publication occurred, so the original holder explicitly
                // revokes its still-reserved event rather than inventing a
                // Core receipt or dropping an unresolved author.
                drop(input);
                h.ingress.revoke_event(cut).unwrap();
                h.inbox
                    .lock()
                    .unwrap()
                    .observe_rpc_author_return(returned)
                    .unwrap();
                h.inbox.lock().unwrap().observe_effects_exited(cut).unwrap();
                authority.withdraw_unpublished_and_exit().unwrap();
                let original_r_retired = main_retired(&h, &role);
                (false, original_input_returned, original_r_retired, false)
            }
        };
    drop(input_author);
    let retired = retire_cut(&h, cut);
    h.dispose_pair(retired);
    finish_after_producer_storage_exit(h);
    eprintln!("RPC producer-storage fixture cleanup complete: ledger_empty=true");

    // Every expected-RED assertion follows original input/R/Source2 disposal
    // and the complete real actor/Scope/ledger cleanup above.
    assert!(original_r_still_started && original_cut_still_owned);
    assert!(no_event_before_fill && original_report_pending);
    assert!(original_input_returned && original_r_retired);
    assert!(
        sender_registered,
        "an already-admitted original RPC sender was refused after original producer storage exit"
    );
    assert!(core_replied);
}
