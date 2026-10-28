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

//! The real Context failure action owns the publication session. A failed
//! flag, Task projection or source exit alone never supplies its drained proof.
//! Materialization and protocol effects remain neutral component-model facts;
//! actual aliases, Work, reports, R delivery and fees use the real runtime.

use super::*;
use crate::{
    control_runtime::context_host::{FAILURE, failure::FailurePublicationError},
    task_proposal_ingress::PairRoute,
};
use novarocks_control_runtime_core::{effect::EffectState, identity::ActionId};
use novarocks_execution::runtime::fragment::ExecutionFailure;
use novarocks_execution_contract::{
    TaskConclusionProposal, TaskFailurePhase,
    failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
};
use std::{
    num::NonZeroU16,
    sync::atomic::{AtomicUsize, Ordering},
};

fn activate(f: &mut Harness) {
    let input = model_fact(
        f,
        ContextInput::MaterializationFinished {
            result: Ok(crate::control_runtime::actors::tests::initial_materialized()),
        },
        1,
    );
    complete_model_effects(f, input);
    f.runtime.retire_input(context_actor(), input).unwrap();
}
fn actual_failure(f: &mut Harness) -> EffectTicket<WorkerAction> {
    let unknown = crate::control_runtime::actors::tests::task(f.identity, 999);
    // The actual actor consumes this unknown Task input, its Core rejects it,
    // and the runtime installs its original failed-generation cleanup action.
    let input = model_fact(f, ContextInput::Installed { task: unknown }, 9);
    let ticket = f
        .runtime
        .take_effect(context_actor(), input, ActionId::new(1).unwrap())
        .unwrap();
    assert!(matches!(ticket.effect, WorkerAction::OwnerFailed { .. }));
    assert!(
        f.service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .scope()
            .snapshot()
            .unwrap()
            .failed
    );
    ticket
}
fn failure_is_still_started(f: &mut Harness, input: InputId) -> bool {
    let host_address = f.service.context(f.identity).unwrap();
    let host = host_address.lock().unwrap();
    host.resources.failure.is_some()
        && matches!(
            host.resources.roles[FAILURE].work.actual_exit_ready(),
            Ok(false)
        )
        && !host.resources.roles[FAILURE]
            .work
            .retirement_observer()
            .is_retired()
        && f.runtime
            .effect_inventory(context_actor(), input, None, 1)
            .is_ok_and(|inventory| {
                inventory.iter().any(|(effect, state)| {
                    effect.id == ActionId::new(1).unwrap() && *state == EffectState::Started
                })
            })
}
fn drain_one_unit_turns(f: &mut Harness, input: InputId) -> (bool, bool) {
    let mut alive = true;
    for _ in 0..128 {
        alive &= failure_is_still_started(f, input);
        let drained = f
            .service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_failure_publications(1)
            .unwrap();
        alive &= failure_is_still_started(f, input);
        if drained {
            return (true, alive);
        }
    }
    (false, alive)
}

#[test]
fn bounded_failure_publication_preserves_the_published_reply_and_fills_only_the_empty_outbox() {
    let mut f = original_context(2, StorageNeed::default());
    let ready = ready(&f, 2);
    let (published_work, mut published_handle) = admit(&mut f, &ready, operation(131));
    let published_input = published_handle.request().token;
    submit_renew(&mut f, published_work);
    let ticket = reply(&f, published_input);
    let decided = match &ticket.effect {
        WorkerAction::Reply(reply) => reply.body.clone(),
        _ => panic!("the original Core-authored reply"),
    };
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    let published_delivery = published_handle.try_take().unwrap();
    let original_body = published_delivery.body() == decided.as_ref()
        && !matches!(
            published_delivery.body(),
            ContextReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed))
        );
    let (empty_work, mut empty_handle) = admit(&mut f, &ready, operation(132));
    let failure = actual_failure(&mut f);
    let failure_input = failure.input;
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(failure)
        .unwrap();

    let zero_did_not_drain = !f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_failure_publications(0)
        .unwrap();
    let zero_did_not_publish = empty_handle.try_take().is_none();
    let one_did_not_scan_all = !f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_failure_publications(1)
        .unwrap()
        && empty_handle.try_take().is_none();
    let alive_at_zero_and_one = failure_is_still_started(&mut f, failure_input);
    let refusal = f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .admit_request(
            &ready,
            ActorId::new(91).unwrap(),
            InputClass::Ordinary,
            operation(133),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .err()
        .expect("the failed owner refuses new R before ordinary capacity checks");
    let new_r_closed =
        matches!(refusal.error, ContextRequestError::OwnerFailed) && refusal.partial.is_none();
    let refusal = f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .submit_request(empty_work, renew())
        .unwrap_err();
    let original_command_returned = matches!(refusal.error, ContextRequestError::OwnerFailed)
        && matches!(refusal.command, Some(ContextCommand::Renew { .. }));

    let (drained, failure_alive_through_drain) = drain_one_unit_turns(&mut f, failure_input);
    let failed_delivery = empty_handle
        .try_take()
        .expect("the empty original outbox receives OwnerFailed");
    let only_empty_failed = matches!(
        failed_delivery.body(),
        ContextReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed))
    );
    let already_published_preserved = published_delivery.body() == decided.as_ref();
    let physical_deliveries_still_paid = f.ledger.used(Dimension::Requests.id()) == 2;
    drop((
        published_handle,
        published_delivery,
        empty_handle,
        failed_delivery,
    ));
    let requests_retired = advance(&mut f, published_work) && advance(&mut f, empty_work);
    let supervision_still_owned = failure_is_still_started(&mut f, failure_input);
    finish(f);

    assert!(original_body && already_published_preserved && only_empty_failed);
    assert!(zero_did_not_drain && zero_did_not_publish && one_did_not_scan_all);
    assert!(new_r_closed && original_command_returned);
    assert!(drained && alive_at_zero_and_one && failure_alive_through_drain);
    assert!(physical_deliveries_still_paid && requests_retired && supervision_still_owned);
}

#[derive(Debug)]
struct QueuedSource(Arc<AtomicUsize>);
impl std::fmt::Display for QueuedSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original failed-owner queued source")
    }
}
impl std::error::Error for QueuedSource {}
impl FailureProjection for QueuedSource {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(domain::WORKER, NonZeroU16::new(65012).unwrap()),
        )
    }
}
impl Drop for QueuedSource {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn source_fees(f: &Harness) -> [u64; 3] {
    [Dimension::Work, Dimension::Completions, Dimension::Captures]
        .map(|dimension| f.ledger.used(dimension.id()))
}

#[test]
fn only_the_real_publication_drain_abandons_a_queued_source_and_its_alias_still_controls_refund() {
    let (mut f, task, preparation) = accepted_task_fixture();
    let ingress = f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .port()
        .ingress()
        .clone();
    let scope = f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    let original_generation = scope.gates().unwrap().generation;
    let drops = Arc::new(AtomicUsize::new(0));
    let cause = ExecutionFailure::from_domain(QueuedSource(drops.clone()));
    let early_alias = cause.clone();
    let handoff = ingress.submit(TaskConclusionProposal::Failed {
        cause,
        phase: TaskFailurePhase::Execution,
    });
    assert!(handoff.transferred && handoff.discarded.is_none() && handoff.error.is_none());
    let source = ingress
        .pair_inventory(PairRoute::Open)
        .unwrap()
        .into_iter()
        .find_map(|position| position.source)
        .unwrap();
    let custody = ingress.source_inventory(source).unwrap().custody;
    let fees = source_fees(&f);
    let not_started = matches!(
        f.service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .take_failed_task_open(task),
        Err(FailurePublicationError::NotStarted)
    );
    let failure = actual_failure(&mut f);
    let failure_input = failure.input;
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(failure)
        .unwrap();
    let not_drained = matches!(
        f.service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .take_failed_task_open(task),
        Err(FailurePublicationError::NotDrained)
    );
    let (drained, failure_alive) = drain_one_unit_turns(&mut f, failure_input);
    let original = f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .take_failed_task_open(task)
        .unwrap()
        .expect(
            "the actual queued open summary transfers without an input or Core acknowledgement",
        );
    let exact_summary = original.cut.is_none()
        && original.summary.candidate_sources().collect::<Vec<_>>() == vec![source];
    let (abandoned_not_consumed, never_filled) = {
        let address = f.service.context(f.identity).unwrap();
        let mut host = address.lock().unwrap();
        let open = host.task(task).unwrap().open_summary_mut().unwrap();
        (
            open.source_core_consumption(source).unwrap() == Consumption::AbandonedByOwnerFailure,
            open.source_input(source).unwrap().is_none(),
        )
    };
    let ordinal = ingress
        .pair_inventory(PairRoute::ConsumingOpen)
        .unwrap()
        .iter()
        .position(|position| position.source == Some(source))
        .unwrap();
    let retained = ingress
        .take_failed_source(PairRoute::ConsumingOpen, ordinal)
        .unwrap()
        .unwrap();
    let last_alias = retained.cause().clone();
    drop((retained, early_alias));
    let alias_still_live = drops.load(Ordering::SeqCst) == 0
        && !custody.actual_exit_ready().unwrap()
        && source_fees(&f) == fees;
    let no_report_exit = matches!(
        custody.report().consume(ConsumerId::new(1).unwrap()),
        Err(RuntimeError::Completion(
            novarocks_control_runtime_core::completion::CompletionError::NotFilled
        ))
    );
    drop(last_alias);
    let actual_source_exit =
        drops.load(Ordering::SeqCst) == 1 && custody.actual_exit_ready().unwrap();
    for holder in ingress
        .take_unused_pair(PairRoute::ConsumingOpen)
        .unwrap()
        .into_iter()
        .flatten()
    {
        holder.exit();
    }
    custody
        .report()
        .abandon_failed_owner(ConsumerId::new(1).unwrap(), &scope, original_generation)
        .unwrap();
    let original_core_leg_abandoned = custody
        .report()
        .consumption(ConsumerId::new(1).unwrap())
        .unwrap()
        == Consumption::AbandonedByOwnerFailure;
    custody
        .report()
        .consume(ConsumerId::new(2).unwrap())
        .unwrap();
    for original in ingress.pair_inventory(PairRoute::ConsumingOpen).unwrap() {
        assert!(original.custody.try_retire().unwrap());
    }
    f.ledger.reap_scope_ready(scope.id(), 32).unwrap();
    let reaped = ingress
        .prepare_pair_reap(PairRoute::ConsumingOpen)
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .open_summary_mut()
        .unwrap()
        .retire_after_reap(reaped)
        .unwrap_or_else(|_| panic!("the original abandoned pair actually reaped"));
    let after_generation = source_fees(&f);
    ingress.seal_source_admission();
    let reaped = ingress
        .prepare_pair_reap(PairRoute::ConsumingOpen)
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    let detached = ingress
        .release_closed_route_pair(PairRoute::ConsumingOpen, reaped)
        .unwrap_or_else(|_| panic!("the closed original source slots detach after true reaping"));
    let before_release = source_fees(&f);
    for mut original in detached {
        original.release().unwrap();
    }
    let after_release = source_fees(&f);
    let only_actual_slot_release_refunded = after_generation == fees
        && before_release == fees
        && before_release
            .iter()
            .zip(after_release)
            .all(|(before, after)| before.checked_sub(after) == Some(2));
    let failure_work_still_alive = failure_is_still_started(&mut f, failure_input);
    drop((original, custody, ingress));
    super::source_slot_lifetime::finish_after_consuming_pair_detached(f, task, preparation);

    assert!(not_started && not_drained && drained && failure_alive);
    assert!(exact_summary && abandoned_not_consumed && never_filled);
    assert!(alias_still_live && no_report_exit && actual_source_exit);
    assert!(
        original_core_leg_abandoned
            && only_actual_slot_release_refunded
            && failure_work_still_alive
    );
}

#[test]
fn a_foreign_same_number_failure_ticket_cannot_start_the_local_work_and_returns_to_its_actual_owner()
 {
    let mut local = original_context(0, StorageNeed::default());
    let mut foreign = original_context(0, StorageNeed::default());
    activate(&mut local);
    activate(&mut foreign);
    let local_ticket = actual_failure(&mut local);
    let foreign_ticket = actual_failure(&mut foreign);
    let local_input = local_ticket.input;
    let foreign_input = foreign_ticket.input;
    let same_numeric_identity = local_ticket.actor == foreign_ticket.actor
        && local_ticket.descriptor.work == foreign_ticket.descriptor.work
        && local_ticket.input == foreign_ticket.input
        && local
            .service
            .context(local.identity)
            .unwrap()
            .lock()
            .unwrap()
            .scope()
            .id()
            == foreign
                .service
                .context(foreign.identity)
                .unwrap()
                .lock()
                .unwrap()
                .scope()
                .id();
    let original_work = foreign_ticket.descriptor.work;
    let refusal = local
        .service
        .context(local.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(foreign_ticket)
        .unwrap_err();
    let rejected_origin = matches!(
        refusal.error,
        FailurePublicationError::Driver(DriverError::EffectTicketOriginMismatch)
    );
    let returned_original = refusal.ticket.actor == context_actor()
        && refusal.ticket.input == foreign_input
        && refusal.ticket.descriptor.work == original_work;
    // Common rejects start on an already Started lease. Successful use of the
    // local original ticket proves the foreign refusal left its Work Admitted;
    // successful foreign use proves the returned ticket retained its origin.
    local
        .service
        .context(local.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(local_ticket)
        .unwrap();
    foreign
        .service
        .context(foreign.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(refusal.ticket)
        .unwrap();
    let (local_drained, local_alive) = drain_one_unit_turns(&mut local, local_input);
    let (foreign_drained, foreign_alive) = drain_one_unit_turns(&mut foreign, foreign_input);
    finish(local);
    finish(foreign);

    assert!(same_numeric_identity && rejected_origin && returned_original);
    assert!(local_drained && foreign_drained && local_alive && foreign_alive);
}

#[test]
fn real_parent_fence_uses_one_prepaid_context_role_and_failed_owner_abandons_its_queued_input() {
    use crate::control_runtime::context_host::owner_control::OwnerFenceDelivery;
    use novarocks_control_runtime::driver::InputPhase;

    let mut f = original_context(1, StorageNeed::default());
    let root_fence_input = f
        .input
        .fill(WorkerMessage::Control(ControlMessage::Command {
            work: f.roles[2].work.id(),
            request: None,
            command: ControlInput::FenceFrontend {
                frontend: f.identity.frontend_process_id(),
            },
        }))
        .unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
    let wake_actions = f
        .runtime
        .ready_effects(
            control_actor(),
            root_fence_input,
            ControlCore::MAX_STEP_WORK,
        )
        .unwrap();
    assert_eq!(
        wake_actions.len(),
        1,
        "the real fence schedules its own continuation"
    );
    let wake = f
        .runtime
        .take_effect(control_actor(), root_fence_input, wake_actions[0])
        .unwrap();
    assert!(matches!(
        &wake.effect,
        WorkerAction::Control {
            effect: ControlEffect::WakeInternal,
            ..
        }
    ));
    // Consume this actual neutral wake before reusing the fixture's original
    // serial Control input role for Core-selected internal work. No new role,
    // synthetic token, or guessed ContinueFence operation is introduced.
    f.runtime.effect_completed(wake).unwrap();
    f.runtime
        .retire_input(control_actor(), root_fence_input)
        .unwrap();
    let root_input = f
        .input
        .fill(WorkerMessage::Control(ControlMessage::Command {
            work: f.roles[2].work.id(),
            request: None,
            command: ControlInput::NextInternal,
        }))
        .unwrap();
    f.runtime.poll_one(Tick(3)).unwrap();
    let actions = f
        .runtime
        .ready_effects(control_actor(), root_input, ControlCore::MAX_STEP_WORK)
        .unwrap();
    let mut abort = None;
    let mut route_published = false;
    for action in actions {
        let ticket = f
            .runtime
            .take_effect(control_actor(), root_input, action)
            .unwrap();
        match &ticket.effect {
            WorkerAction::Control {
                effect: ControlEffect::Route { context, state, .. },
                ..
            } => {
                assert_eq!(*context, f.identity);
                assert_eq!(*state, QueryContextState::Aborting);
                drop(
                    f.runtime
                        .commit_snapshot(&ticket, &f.route, Arc::new(*state))
                        .unwrap(),
                );
                f.runtime.effect_completed(ticket).unwrap();
                route_published = true;
            }
            WorkerAction::Control {
                effect: ControlEffect::AbortContext { context, .. },
                ..
            } => {
                assert_eq!(*context, f.identity);
                assert!(abort.replace(ticket).is_none());
            }
            other => {
                panic!("unexpected real Root fence effect requires its actual owner: {other:?}")
            }
        }
    }
    let abort = abort.expect("the real fenced Context has one original AbortContext effect");
    let directory = f.service.directory();
    let address = directory.context(f.identity).unwrap();
    let scope = address.lock().unwrap().scope().clone();
    let before_scope = scope.snapshot().unwrap();
    let before_actor = f.runtime.snapshot(context_actor()).unwrap();
    let before_roles = f.runtime.role_inventory(context_actor(), None, 16).unwrap();
    let paid = [
        Dimension::Inputs,
        Dimension::Batches,
        Dimension::Effects,
        Dimension::Work,
        Dimension::Completions,
        Dimension::Captures,
        Dimension::Requests,
    ]
    .map(|dimension| f.ledger.used(dimension.id()));
    let original_parent_role = before_roles
        .iter()
        .find(|role| role.source == control_actor() && role.class == InputClass::Control)
        .expect("bootstrap already admitted the original parent-control role")
        .role;
    let unused_is_not_retired = !address.lock().unwrap().retire_owner_control().unwrap();
    let delivered = directory.handoff_owner_fence(&abort).unwrap();
    let after_first = f.runtime.snapshot(context_actor()).unwrap();
    let duplicate = directory.handoff_owner_fence(&abort).unwrap();
    let after_duplicate = f.runtime.snapshot(context_actor()).unwrap();
    let input = f
        .runtime
        .role_inventory(context_actor(), None, 16)
        .unwrap()
        .into_iter()
        .find(|role| role.role == original_parent_role)
        .unwrap()
        .in_flight
        .expect("the same original parent role holds its one actual input");
    let original_queued = f
        .runtime
        .input_custody(context_actor(), input)
        .unwrap()
        .is_some_and(|original| {
            original.role == Some(original_parent_role)
                && original.source == control_actor()
                && original.class == InputClass::Control
                && original.phase == InputPhase::Filled
        });
    let queued_cannot_retire = !address.lock().unwrap().retire_owner_control().unwrap();
    let one_original_position = after_first.inputs == before_actor.inputs + 1
        && after_first.roles == before_actor.roles
        && after_first.batches == before_actor.batches
        && after_duplicate == after_first;
    let no_new_work_or_funding = scope.snapshot().unwrap() == before_scope
        && paid
            == [
                Dimension::Inputs,
                Dimension::Batches,
                Dimension::Effects,
                Dimension::Work,
                Dimension::Completions,
                Dimension::Captures,
                Dimension::Requests,
            ]
            .map(|dimension| f.ledger.used(dimension.id()));
    let no_rpc_capture = {
        let host = address.lock().unwrap();
        host.resources.requests.is_empty()
            && host.resources.request_partials.is_empty()
            && f.ledger.used(Dimension::Requests.id()) == 0
    };

    // Lifecycle input from another actual source wins class priority over
    // queued parent Control. The real Core panics on an unknown Task; it has
    // never run OwnerFenced's unassembled foreign close/stop host actions.
    let failure = actual_failure(&mut f);
    let failure_input = failure.input;
    let parent_still_queued = f
        .runtime
        .input_custody(context_actor(), input)
        .unwrap()
        .is_some_and(|original| original.phase == InputPhase::Filled);
    let failed_delivery = directory.handoff_owner_fence(&abort).unwrap();
    let no_refill_after_failure = f
        .runtime
        .input_custody(context_actor(), input)
        .unwrap()
        .is_some_and(|original| original.phase == InputPhase::Filled);
    let original_root_ticket_still_valid = f.runtime.validate_effect_ticket(&abort).is_ok();
    address
        .lock()
        .unwrap()
        .begin_failure_publications(failure)
        .unwrap();
    let (drained, failure_alive) = drain_one_unit_turns(&mut f, failure_input);
    let still_not_consumed_by_publication = f
        .runtime
        .input_custody(context_actor(), input)
        .unwrap()
        .is_some_and(|original| original.phase == InputPhase::Filled);
    let mut abandoned = false;
    for _ in 0..8 {
        let progress = address
            .lock()
            .unwrap()
            .advance_failed_runtime_inputs(1)
            .unwrap();
        assert!(progress.units <= 1 && progress.taken <= progress.units);
        if f.runtime
            .input_custody(context_actor(), input)
            .unwrap()
            .is_some_and(|original| original.phase == InputPhase::AbandonedByOwnerFailure)
        {
            abandoned = true;
            break;
        }
    }
    let retired_after_abandon = address.lock().unwrap().retire_owner_control().unwrap();
    let original_input_removed = f
        .runtime
        .input_custody(context_actor(), input)
        .unwrap()
        .is_none();
    let no_parent_role_after_retirement = !f
        .runtime
        .role_inventory(context_actor(), None, 16)
        .unwrap()
        .iter()
        .any(|role| role.role == original_parent_role);
    f.runtime.effect_completed(abort).unwrap();
    f.runtime.retire_input(control_actor(), root_input).unwrap();
    // Existing metadata-only teardown finishes the genuine failure session,
    // then the original Root refusal/retention/removal. It is not Native exit.
    finish(f);

    assert!(route_published && original_root_ticket_still_valid);
    assert_eq!(delivered, OwnerFenceDelivery::Delivered);
    assert_eq!(duplicate, OwnerFenceDelivery::AlreadyDelivered);
    assert!(unused_is_not_retired && original_queued && queued_cannot_retire);
    assert!(one_original_position && no_new_work_or_funding && no_rpc_capture);
    assert_eq!(failed_delivery, OwnerFenceDelivery::FailedOwner);
    assert!(parent_still_queued && no_refill_after_failure && still_not_consumed_by_publication);
    assert!(drained && failure_alive && abandoned);
    assert!(retired_after_abandon && original_input_removed && no_parent_role_after_retirement);
}
