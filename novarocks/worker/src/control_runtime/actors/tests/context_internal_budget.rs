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

//! Actual Actor/batch and paid SourceSlot integration. Task physical facts
//! below are explicit protocol-budget model inputs, not Native executor exit.
//! Rich payload destruction, original reports, cut custody and Work reaping
//! are real; no numeric SourceId or replacement request is manufactured.

use super::*;
use crate::control_runtime::{
    causal::{CausalSourceRetirement, TaskCausalInbox},
    source_slot_issuer::{SourceSlotPayer, reserve_source_slots},
};
use crate::task_proposal_ingress::{CutSourceAdmission, PairRoute, TaskProposalIngress};
use novarocks_execution::runtime::fragment::ExecutionFailure;
use novarocks_execution_contract::{
    AbortCause, TaskConclusion, TaskConclusionProposal, TaskFailurePhase, TerminationDetail,
    failure::{FailureFact, FailureReason, domain},
};
use novarocks_worker_protocol::proposal_summary::Proposal;
use std::{
    num::NonZeroU16,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug)]
struct OriginalFailure {
    observation: bool,
    drops: Arc<AtomicUsize>,
}
impl std::fmt::Display for OriginalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original Actor budget source")
    }
}
impl std::error::Error for OriginalFailure {}
impl FailureProjection for OriginalFailure {
    fn failure_fact(&self) -> FailureFact {
        let reason = FailureReason::new(domain::WORKER, NonZeroU16::new(65002).unwrap());
        if self.observation {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Data,
                reason,
            )
        }
    }
}
impl Drop for OriginalFailure {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn a_paid_source_prefix_and_remaining_close_slice_publish_one_real_internal_wake() {
    let mut f = Fixture::with_policy_byte_limits(100);
    let policy = f.policy.unwrap();
    let task = task(f.identity, 1);
    let loading = f.run(
        ContextInput::Request(ContextCommand::Create {
            task,
            payload: PayloadId(1),
        }),
        Some(operation(1)),
    );
    let parent = loading
        .iter()
        .find_map(|effect| match effect {
            WorkerAction::Context(ContextEffect::LoadInput { reservation, .. }) => {
                Some(*reservation)
            }
            _ => None,
        })
        .unwrap();
    // Split existing Task.loading Source fees. This is the production paid
    // constructor, not the empty-vector protocol_model source fixture.
    let originals = reserve_source_slots::<4>(
        &f.scope,
        parent,
        generation(),
        SourceSlotPayer::Retained(&mut f.jobs.get_mut(&parent).unwrap().1),
    )
    .unwrap();
    let ingress = Arc::new(
        TaskProposalIngress::from_admitted(
            task,
            1,
            originals,
            Arc::new(CausalPending),
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
        )
        .unwrap_or_else(|_| panic!("the actual four paid original source slots")),
    );
    let inbox = TaskCausalInbox::new(
        f.runtime.clone(),
        actor(),
        ActorId::new(7001).unwrap(),
        task,
        generation(),
        ingress.clone(),
        Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
    );
    f.run(
        ContextInput::Loaded {
            task,
            result: Ok(CreationCost {
                input_retained_bytes: 1,
                ingress_retained_bytes: 1,
                structural_bytes: 1,
                inbound_records: 0,
                inbound_bytes: 0,
            }),
        },
        None,
    );
    f.run(ContextInput::Installed { task }, None);
    // These same model facts are used by the pure two-cut budget regression.
    // They must never be interpreted as exits of the Work/captures below.
    f.run(
        ContextInput::CompletionIntent {
            task,
            completed_output: None,
        },
        None,
    );
    f.run(ContextInput::CompletionExited { task }, None);
    for fact in [
        PhysicalFact::LoaderExited,
        PhysicalFact::PreparationBodyExited,
        PhysicalFact::PreparationExited,
        PhysicalFact::OutputReleased,
        PhysicalFact::RootIdle,
    ] {
        f.run(ContextInput::Physical { task, fact }, None);
    }
    let mut h = CausalHarness {
        fixture: f,
        task,
        ingress,
        inbox,
        source_parent: parent,
    };
    let work = h.work();
    let source_slots = reserve_source_slots::<2>(
        &h.fixture.scope,
        parent,
        generation(),
        SourceSlotPayer::Retained(&mut h.fixture.jobs.get_mut(&parent).unwrap().1),
    )
    .unwrap();
    let (cut, mut sender) = h
        .inbox
        .admit_settlement(
            1,
            Arc::new(CausalPending),
            CutSourceAdmission::Fresh(source_slots),
        )
        .unwrap();
    sender.bind_work(work);
    // A source-free original predecessor is legal and has no rich payload
    // whose consumption proof could be inferred from another prefix.
    h.fixture.run(
        ContextInput::Proposal {
            task,
            proposal: Proposal {
                conclusion: TaskConclusion::Terminated(TerminationDetail::Aborted(
                    AbortCause::PeerTaskFailed,
                )),
                evidence: None,
                source: None,
                diagnostic: None,
            },
        },
        None,
    );
    let drops = Arc::new(AtomicUsize::new(0));
    for observation in [true, false] {
        let handoff = h.ingress.submit(TaskConclusionProposal::Failed {
            cause: ExecutionFailure::from_domain(OriginalFailure {
                observation,
                drops: drops.clone(),
            }),
            phase: TaskFailurePhase::Execution,
        });
        assert!(handoff.transferred && handoff.discarded.is_none() && handoff.error.is_none());
    }
    sender.registered();
    let sources: Vec<_> = h
        .ingress
        .pair_inventory(PairRoute::Cut(cut))
        .unwrap()
        .into_iter()
        .filter_map(|row| row.source)
        .collect();
    let delivery = h.inbox.deliver_next().unwrap().unwrap();
    let prefix = delivery
        .prefix
        .expect("both rich candidates have the original prefix input");
    h.fixture.runtime.poll_one(Tick(31_000_000_000)).unwrap();
    let actor_failed = h.fixture.runtime.snapshot(actor()).unwrap().failed;
    let prefix_inventory = h
        .fixture
        .runtime
        .effect_inventory(actor(), prefix, None, ContextActor::MAX_STEP_ACTIONS)
        .unwrap();
    let prefix_count = prefix_inventory.len();
    let not_retired_while_effects_live = matches!(
        h.fixture.runtime.retire_input(actor(), prefix),
        Err(DriverError::EffectsRemain)
    );
    let core_receipt = h.inbox.observe_core(cut).unwrap();
    let prefix_effects = h.fixture.drain(prefix);
    let event_effects = h.fixture.drain(delivery.event);
    let wakes = prefix_effects
        .iter()
        .filter(|effect| matches!(effect, WorkerAction::Context(ContextEffect::WakeInternal)))
        .count();
    let leaked_hints = prefix_effects.iter().any(|effect| {
        matches!(
            effect,
            WorkerAction::Context(
                ContextEffect::ContinueTaskDomain { .. }
                    | ContextEffect::ContinueRequestReplies { .. }
                    | ContextEffect::ContinueReplies { .. }
                    | ContextEffect::ContinueQuiesce
                    | ContextEffect::ContinueClose { .. }
            )
        )
    });
    let disposed = prefix_effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                WorkerAction::Context(ContextEffect::DisposeSource { .. })
            )
        })
        .count();
    let retained = prefix_effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                WorkerAction::Context(ContextEffect::RetireTask { .. })
            )
        })
        .count();
    let publications = prefix_effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                WorkerAction::Context(ContextEffect::PublishTask { .. })
            )
        })
        .count();
    let no_reply = !prefix_effects
        .iter()
        .chain(&event_effects)
        .any(|effect| matches!(effect, WorkerAction::Reply(_)));
    let fees_before = [Dimension::Work, Dimension::Completions, Dimension::Captures]
        .map(|dimension| h.fixture.ledger.used(dimension.id()));
    let mut actual_source_receipts = true;
    for source in &sources {
        actual_source_receipts &=
            h.inbox.source_core_consumption(cut, *source).unwrap() == Consumption::Consumed;
        let retained_source = h.ingress.take_source(*source).unwrap();
        drop(retained_source); // Actual owned payload/capture exit on this host stack.
    }
    for row in h.ingress.pair_inventory(PairRoute::Cut(cut)).unwrap() {
        assert!(row.custody.actual_exit_ready().unwrap());
        row.custody
            .report()
            .consume(ConsumerId::new(1).unwrap())
            .unwrap();
        row.custody
            .report()
            .consume(ConsumerId::new(2).unwrap())
            .unwrap();
        assert!(row.custody.try_retire().unwrap());
    }
    h.fixture
        .ledger
        .reap_scope_ready(h.fixture.scope.id(), 256)
        .unwrap();
    let still_paid_after_source_reap =
        [Dimension::Work, Dimension::Completions, Dimension::Captures]
            .map(|dimension| h.fixture.ledger.used(dimension.id()))
            == fees_before;
    let reaped = h
        .ingress
        .prepare_pair_reap(PairRoute::Cut(cut))
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    // The actual neutral sender returned and both actual runtime batches
    // completed. This fixture owns no foreign callback/capture at this cut.
    h.ingress.observe_cut_exit(cut, true, true).unwrap();
    h.ingress.consume_cut(cut, 0).unwrap();
    let retired = h
        .inbox
        .retire_after_exit(cut, CausalSourceRetirement::Reaped(reaped))
        .unwrap();
    let detached = dispose_retired_pair(&h.fixture, &h.ingress, retired);
    let fees_after = [Dimension::Work, Dimension::Completions, Dimension::Captures]
        .map(|dimension| h.fixture.ledger.used(dimension.id()));
    // The Wake represented a real remaining close scan. Follow that existing
    // neutral cursor only after the original prefix's batch really retired.
    let next = h.fixture.run(ContextInput::NextInternal, None);
    let close_generation = next
        .iter()
        .find_map(|effect| match effect {
            WorkerAction::Context(ContextEffect::ApplyTaskClose {
                task: id,
                generation,
                ..
            }) if *id == task => Some(*generation),
            _ => None,
        })
        .expect("the actual remaining close cursor reaches this retained Task record");
    let applied = h.fixture.run(
        ContextInput::ApplyTaskClose {
            task,
            generation: close_generation,
            intent: novarocks_worker_protocol::context::TaskCloseIntent::Abort(
                novarocks_execution_contract::AbortCause::LeaseExpired,
            ),
        },
        None,
    );
    let no_duplicate_task_publication = !applied.iter().any(|effect| {
        matches!(
            effect,
            WorkerAction::Context(
                ContextEffect::StopTask { .. }
                    | ContextEffect::PublishTask { .. }
                    | ContextEffect::RetireTask { .. }
            )
        )
    });
    h.finish(); // Original Source routes, reports, jobs, metadata and Scope exit.

    assert!(!actor_failed);
    assert!(not_retired_while_effects_live);
    assert_eq!(core_receipt, Consumption::Consumed);
    assert_eq!(sources.len(), 2);
    assert!(actual_source_receipts);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_eq!(detached, 2);
    assert!(still_paid_after_source_reap);
    assert_eq!(fees_after, fees_before.map(|fee| fee - 2));
    assert_eq!(wakes, 1);
    assert!(!leaked_hints);
    assert!(
        no_reply,
        "the neutral continuation creates no second RPC/token"
    );
    assert_eq!(disposed, 1);
    assert_eq!(retained, 1);
    assert_eq!(publications, 3);
    // Body14 includes one original summary record. Removing its one pure
    // ContinueClose and adding one actual Wake leaves 13 actual output slots.
    // This is measured output, not an assertion that the pure upper15 is hit.
    assert_eq!(prefix_count, 13);
    assert_eq!(prefix_effects.len(), prefix_count);
    assert!(no_duplicate_task_publication);
    assert_eq!(policy.context_actor(1).unwrap().max_step_work, 22);
    assert_eq!(policy.context_actor(1).unwrap().max_effects_per_step, 16);
    assert_eq!(policy.context_actor(1).unwrap().turn_budget, 92);
}
