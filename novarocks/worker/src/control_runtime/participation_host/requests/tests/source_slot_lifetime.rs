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

//! Real Participation/Task/R payments and original source aliases. Neutral Core
//! facts and effects remain model-world; these tests never start Native work,
//! synthesize physical exits or stand in for a failure-publication session.

use super::*;
use crate::{
    control_runtime::task::TaskHostOwner,
    task_proposal_ingress::{IngressError, PairRoute},
};
use novarocks_control_runtime::{actor_turn::InputPhase, exit_custody::ExitCustodyPhase};
use novarocks_execution::runtime::task::ExecutionFailure;
use novarocks_execution_contract::{
    TaskConclusionProposal, TaskFailurePhase,
    failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
};
use novarocks_worker_protocol::proposal_summary::CutError;
use std::{
    num::NonZeroU16,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
struct OriginalSource(Arc<AtomicUsize>);
impl std::fmt::Display for OriginalSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("actual source-slot lifetime probe")
    }
}
impl std::error::Error for OriginalSource {}
impl FailureProjection for OriginalSource {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(domain::WORKER, NonZeroU16::new(65011).unwrap()),
        )
    }
}
impl Drop for OriginalSource {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn source_fees(f: &Harness) -> [u64; 2] {
    actual_component_inventory(f)
}
// The first test already detached the consuming source pair to inspect the
// actual fee refund. Finish only the remaining original open pair, then the
// same unstarted preparation, Task lifetime and Participation inventories. This is
// the existing fixture's genuine unused teardown, not a Native stop report.
pub(super) fn finish_after_consuming_pair_detached(
    f: Harness,
    task: TaskIdentity,
    preparation: HostMetadataResources,
) {
    finish_after_consuming_pair_detached_with(f, task, preparation, |_| {});
}
pub(super) fn finish_after_consuming_pair_detached_with(
    f: Harness,
    task: TaskIdentity,
    mut preparation: HostMetadataResources,
    before_finish: impl FnOnce(&Harness),
) {
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

    let host_address = f.service.participation(f.identity).unwrap();
    let mut host = host_address.lock().unwrap();
    host.close_task_request_admission(task).unwrap();
    let TaskHostOwner::Ready(task) = host.take_task_for_teardown(task).unwrap() else {
        panic!("the original ready Task remains owned by this Participation");
    };
    drop(host);
    let mut resources = task.into_resources();
    assert!(resources.causal.is_empty());
    let ingress = resources.port.ingress().clone();
    assert!(matches!(
        ingress.pair_inventory(PairRoute::ConsumingOpen),
        Err(IngressError::Cut(CutError::Unknown))
    ));
    for holder in ingress
        .take_unused_pair(PairRoute::Open)
        .unwrap()
        .into_iter()
        .flatten()
    {
        holder.exit();
    }
    for original in ingress.pair_inventory(PairRoute::Open).unwrap() {
        assert!(original.source.is_none());
        assert!(original.custody.try_retire().unwrap());
    }
    f.ledger.reap_scope_ready(scope.id(), 32).unwrap();
    let reaped = reap_original_source_pair(&ingress, PairRoute::Open);
    let detached = ingress
        .release_closed_route_pair(PairRoute::Open, reaped)
        .unwrap_or_else(|_| panic!("the remaining original closed pair actually reaped"));
    drop(detached); // The original Source bundle finished at its actual ExitCustody retirement.
    crate::control_runtime::task::retire_unused_host_calls_for_test(
        &resources.host_calls,
        &f.ledger,
    );
    retire_unused_task_roles(&f, &mut resources);
    resources.lifetime.body_exited().unwrap();
    resources.lifetime.captures_exited().unwrap();
    resources
        .open_summary
        .as_mut()
        .unwrap()
        .close_after_exit(&resources.lifetime)
        .unwrap();
    finish_unused_bundle(&mut resources.active);
    f.ledger.reap_scope_ready(scope.id(), 32).unwrap();
    drop((resources, ingress));
    before_finish(&f);
    finish(f);
}

#[test]
fn a_later_source_keeps_its_original_slot_fees_after_the_r_that_supplied_them_has_really_returned()
{
    let (mut f, task, preparation) = accepted_task_fixture();
    let baseline = source_fees(&f);
    let ready = ready(&f, 1);
    let ingress = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .port()
        .ingress()
        .clone();
    let prefix_a = ingress
        .pair_inventory(PairRoute::Open)
        .unwrap()
        .map(|original| original.key);
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
            operation(121),
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
    let actual = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .deliver_task_event(task)
        .unwrap()
        .unwrap();
    assert!(actual.prefix.is_none());
    let future_b = ingress.pair_inventory(PairRoute::Open).unwrap();
    let future_keys = future_b.each_ref().map(|original| original.key.clone());
    assert!(
        future_keys
            .iter()
            .zip(&prefix_a)
            .all(|(new, old)| !new.same_original(old))
    );

    // This source is later than the original request's real cut. It captures
    // B while that request still owns its external delivery and R position.
    let drops = Arc::new(AtomicUsize::new(0));
    let cause = ExecutionFailure::from_domain(OriginalSource(drops.clone()));
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
        .find_map(|original| original.source)
        .unwrap();
    let source_custody = ingress.source_inventory(source).unwrap().custody;
    f.runtime.poll_one(Tick(7)).unwrap();
    let ticket = reply(&f, actual.event);
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
    drop(handle);
    let delivery_still_holds_r = !f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_request(work)
        .unwrap()
        && f.ledger.used(Dimension::Requests.id()) == 1;
    drop(delivered);
    let original_r_retired = advance(&mut f, work)
        && f.ledger.used(Dimension::Requests.id()) == 0
        && ready.registered() == 0;
    let source_outlives_r = drops.load(Ordering::SeqCst) == 0
        && source_custody.phase() == ExitCustodyPhase::Started
        && !source_custody.actual_exit_ready().unwrap();
    let fees_after_r = source_fees(&f);
    // The old initial A2 fee has exited, while B2 now occupies the same
    // aggregate budget. Counting baseline + B2 would double-charge that swap.
    let actual_replacement = fees_after_r == baseline
        && ingress
            .pair_inventory(PairRoute::Open)
            .unwrap()
            .iter()
            .zip(&future_keys)
            .all(|(current, original)| current.key.same_original(original));

    // R=1 is genuinely reusable while the unrelated source and alias live.
    // This next request never submits, so its unique sender/unused holders
    // withdraw on their actual return without changing the queued B summary.
    let (second, second_handle) = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .admit_task_request(
            &ready,
            task,
            InputClass::Control,
            operation(122),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .unwrap();
    drop(second_handle);
    let r_reusable = advance(&mut f, second)
        && f.ledger.used(Dimension::Requests.id()) == 0
        && source_fees(&f) == fees_after_r
        && !source_custody.actual_exit_ready().unwrap();

    let summary_input = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .open_summary_mut()
        .unwrap()
        .deliver_next(generation())
        .unwrap()
        .unwrap();
    f.runtime.poll_one(Tick(8)).unwrap();
    let core_receipt = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .open_summary_mut()
        .unwrap()
        .observe_core()
        .unwrap();
    complete_model_effects(&mut f, summary_input);
    f.runtime
        .retire_input(participation_actor(), summary_input)
        .unwrap();
    let retained = ingress.take_source(source).unwrap();
    let last_alias = retained.cause().clone();
    drop((retained, early_alias));
    let alias_still_holds_b = drops.load(Ordering::SeqCst) == 0
        && !source_custody.actual_exit_ready().unwrap()
        && source_fees(&f) == fees_after_r;
    let early_report = source_custody.report().consume(ConsumerId::new(1).unwrap());
    let no_false_report = matches!(
        early_report,
        Err(RuntimeError::Completion(
            novarocks_control_runtime_core::completion::CompletionError::NotFilled
        ))
    );
    drop(last_alias);
    let true_last_exit =
        drops.load(Ordering::SeqCst) == 1 && source_custody.actual_exit_ready().unwrap();
    for holder in ingress
        .take_unused_pair(PairRoute::ConsumingOpen)
        .unwrap()
        .into_iter()
        .flatten()
    {
        holder.exit();
    }
    for original in ingress.pair_inventory(PairRoute::ConsumingOpen).unwrap() {
        if let Some(source) = original.source {
            let receipt = f
                .service
                .participation(f.identity)
                .unwrap()
                .lock()
                .unwrap()
                .task(task)
                .unwrap()
                .open_summary_mut()
                .unwrap()
                .source_core_consumption(source)
                .unwrap();
            assert_eq!(receipt, Consumption::Consumed);
            original
                .custody
                .report()
                .consume(ConsumerId::new(1).unwrap())
                .unwrap();
            original
                .custody
                .report()
                .consume(ConsumerId::new(2).unwrap())
                .unwrap();
        }
        assert!(original.custody.try_retire().unwrap());
    }
    f.ledger
        .reap_scope_ready(
            f.service
                .participation(f.identity)
                .unwrap()
                .lock()
                .unwrap()
                .scope()
                .id(),
            32,
        )
        .unwrap();
    let reaped = reap_original_source_pair(&ingress, PairRoute::ConsumingOpen);
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .open_summary_mut()
        .unwrap()
        .retire_after_reap(reaped)
        .unwrap_or_else(|_| panic!("the exact B generation and reports actually reaped"));
    let after_generation = source_fees(&f);
    ingress.seal_source_admission();
    let reaped = reap_original_source_pair(&ingress, PairRoute::ConsumingOpen);
    let detached = ingress
        .release_closed_route_pair(PairRoute::ConsumingOpen, reaped)
        .unwrap_or_else(|_| panic!("the same closed B slot storage leaves the ingress"));
    let before_refund = source_fees(&f);
    drop(detached); // The original Source bundle finished at its actual ExitCustody retirement.
    let after_refund = source_fees(&f);
    let exact_slot_refund = fees_after_r
        .iter()
        .zip(after_generation)
        .all(|(before, after)| before.checked_sub(after) == Some(2))
        && before_refund == after_generation
        && after_refund == before_refund;
    drop((source_custody, ingress, future_b));
    finish_after_consuming_pair_detached(f, task, preparation);

    assert!(delivery_still_holds_r && original_r_retired && source_outlives_r);
    assert!(actual_replacement && r_reusable && alias_still_holds_b);
    assert_eq!(core_receipt, Consumption::Consumed);
    assert!(no_false_report && true_last_exit && exact_slot_refund);
}

#[test]
fn an_original_prefix_source_blocks_its_own_r_until_real_alias_exit_and_source_report_consumption()
{
    let (mut f, task, preparation) = accepted_task_fixture();
    let ready = ready(&f, 1);
    let ingress = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .port()
        .ingress()
        .clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let cause = ExecutionFailure::from_domain(OriginalSource(drops.clone()));
    let last_alias = cause.clone();
    let handoff = ingress.submit(TaskConclusionProposal::Failed {
        cause,
        phase: TaskFailurePhase::Execution,
    });
    assert!(handoff.transferred && handoff.error.is_none() && handoff.discarded.is_none());
    let source = ingress
        .pair_inventory(PairRoute::Open)
        .unwrap()
        .into_iter()
        .find_map(|original| original.source)
        .unwrap();
    let custody = ingress.source_inventory(source).unwrap().custody;
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
            operation(123),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .unwrap();
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .submit_request(work, ParticipationCommand::GetFinalInfo { task })
        .unwrap();
    let actual = f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .deliver_task_event(task)
        .unwrap()
        .unwrap();
    let prefix = actual.prefix.expect("the same source-bearing A prefix");
    for _ in 0..2 {
        f.runtime.poll_one(Tick(7)).unwrap();
        if f.runtime
            .input_custody(participation_actor(), actual.event)
            .unwrap()
            .is_some_and(|receipt| receipt.phase == InputPhase::Committed)
        {
            break;
        }
    }
    complete_model_effects(&mut f, prefix);
    let ticket = reply(&f, actual.event);
    f.service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .publish_request_reply(ticket)
        .unwrap();
    let delivered = handle.try_take().unwrap();
    drop((handle, delivered));
    let retained = ingress.take_source(source).unwrap();
    drop(retained);
    // Save the actual Core fact while the original role still resides in
    // the Task. advance_request may uniquely move it into the same R tail.
    let prefix_core_consumed = {
        let address = f.service.participation(f.identity).unwrap();
        let mut owner = address.lock().unwrap();
        let causal = owner.task(task).unwrap().causal();
        assert_eq!(
            causal.observe_core(actual.cut).unwrap(),
            Consumption::Consumed
        );
        causal.source_core_consumption(actual.cut, source).unwrap()
    };
    let own_prefix_waits = !f
        .service
        .participation(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .advance_request(work)
        .unwrap()
        && f.ledger.used(Dimension::Requests.id()) == 1
        && drops.load(Ordering::SeqCst) == 0
        && !custody.actual_exit_ready().unwrap();
    drop(last_alias);
    let actual_last_exit =
        drops.load(Ordering::SeqCst) == 1 && custody.actual_exit_ready().unwrap();
    for holder in ingress
        .take_unused_pair(PairRoute::Cut(actual.cut))
        .unwrap()
        .into_iter()
        .flatten()
    {
        holder.exit();
    }
    // The exact cached prefix receipt, actual rich source/capture exit and
    // real local observer allow the original report consumers to finish.
    custody
        .report()
        .consume(ConsumerId::new(1).unwrap())
        .unwrap();
    custody
        .report()
        .consume(ConsumerId::new(2).unwrap())
        .unwrap();
    let original_r_retired = advance(&mut f, work) && f.ledger.used(Dimension::Requests.id()) == 0;
    drop((custody, ingress));
    finish_accepted_task_fixture(f, task, preparation);

    assert!(own_prefix_waits && actual_last_exit && original_r_retired);
    assert_eq!(prefix_core_consumed, Consumption::Consumed);
}
