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
    // Bind the Task.loading inventory's actual Source roles from the same
    // original Work and payer, using the production atomic issuer.
    let (parent_work, payer) = f.jobs.get_mut(&parent).unwrap();
    let originals = reserve_source_slots::<4>(
        &f.scope,
        parent_work,
        (f.scope.id(), generation()),
        SourceSlotPayer::Retained(payer),
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
        f.registration.clone(),
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
    let (parent_work, payer) = h.fixture.jobs.get_mut(&parent).unwrap();
    let source_slots = reserve_source_slots::<2>(
        &h.fixture.scope,
        parent_work,
        (h.fixture.scope.id(), generation()),
        SourceSlotPayer::Retained(payer),
    )
    .unwrap();
    let producer = &h
        .fixture
        .roles
        .iter()
        .find(|(original, _)| original.id() == work)
        .unwrap()
        .0;
    let (cut, mut sender) = h
        .inbox
        .admit_settlement(
            producer,
            1,
            Arc::new(CausalPending),
            CutSourceAdmission::Fresh(source_slots),
        )
        .unwrap();
    sender.bind_work(producer.reference());
    // A source-free original predecessor is legal and has no rich payload
    // whose consumption proof could be inferred from another prefix.
    let peer_effects = h.fixture.run(
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
    assert_eq!(
        peer_effects
            .iter()
            .filter(|effect| matches!(
                effect,
                WorkerAction::Context(ContextEffect::StopTask { .. })
            ))
            .count(),
        0,
        "a buffered proposal is not its first actual adjudication"
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
    let original_sources: Vec<_> = h
        .ingress
        .pair_inventory(PairRoute::Cut(cut))
        .unwrap()
        .into_iter()
        .map(|row| row.custody)
        .collect();
    let headers_live_before = original_sources.iter().all(|source| !source.reusable());
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
    let actual_headers_reaped = original_sources.iter().all(|source| source.reusable());
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
    let before_detach = h.fixture.scope.snapshot().unwrap();
    let crate::task_proposal_ingress::RetiredCut { pair, funding, .. } = retired;
    let witnesses = h
        .ingress
        .prepare_owned_pair_reap(&pair)
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    let detached = h.ingress.release_owned_pair(&pair, witnesses).unwrap();
    let detached_count = detached.len();
    drop(detached); // Original neutral rows leave the ingress lock before Drop.
    drop((pair, funding));
    let detach_retires_no_work = h.fixture.scope.snapshot().unwrap() == before_detach;
    // The Wake represented a real remaining close scan. Follow that existing
    // neutral cursor only after the original prefix's batch really retired.
    let next = h.fixture.run(ContextInput::NextInternal, None);
    assert!(
        next.iter().any(|effect| matches!(
            effect,
            WorkerAction::Context(ContextEffect::CloseFanoutPrepared { targets: 0, .. })
        )),
        "the finite scan completes without reviving an already terminal Task"
    );
    let no_duplicate_task_publication = !next.iter().any(|effect| {
        matches!(
            effect,
            WorkerAction::Context(
                ContextEffect::ApplyTaskClose { .. }
                    | ContextEffect::StopTask { .. }
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
    assert_eq!(detached_count, 2);
    assert!(headers_live_before && actual_headers_reaped);
    assert!(detach_retires_no_work);
    assert_eq!(wakes, 1);
    assert!(!leaked_hints);
    assert!(
        no_reply,
        "the neutral continuation creates no second RPC/token"
    );
    assert_eq!(disposed, 1);
    assert_eq!(retained, 1);
    assert_eq!(publications, 3);
    let peer_publication = prefix_effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                WorkerAction::Context(ContextEffect::PublishConclusion {
                    conclusion: TaskConclusion::Terminated(TerminationDetail::Aborted(
                        AbortCause::PeerTaskFailed
                    )),
                    ..
                })
            )
        })
        .expect("the prefix first adjudicates its original buffered peer proposal");
    let origin_publication = prefix_effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                WorkerAction::Context(ContextEffect::PublishConclusion {
                    conclusion: TaskConclusion::Terminated(TerminationDetail::Failed(_)),
                    ..
                })
            )
        })
        .expect("the same prefix then adjudicates its retained original failure");
    assert!(peer_publication < origin_publication);
    assert_eq!(
        prefix_effects[peer_publication..origin_publication]
            .iter()
            .filter(|effect| matches!(
                effect,
                WorkerAction::Context(ContextEffect::StopTask { .. })
            ))
            .count(),
        1,
        "the first actual peer adjudication owns exactly one stop effect"
    );
    assert_eq!(
        prefix_effects[origin_publication..]
            .iter()
            .filter(|effect| matches!(
                effect,
                WorkerAction::Context(ContextEffect::StopTask { .. })
            ))
            .count(),
        0,
        "origin refinement publishes its state without issuing a second stop"
    );
    // The two adjudications and terminal publication path emit nine effects,
    // including the one first stop; a second stop would raise that total to ten.
    // With three lease-close effects and the summary receipt, this sequence
    // charges 14 pure units including its one original record visit.
    // The Actor removes ContinueClose and adds its one prepaid Wake; neither
    // operation removes that record visit from the output inventory twice.
    assert_eq!(
        prefix_effects
            .iter()
            .filter(|effect| matches!(
                effect,
                WorkerAction::Context(ContextEffect::CloseExternalAdmission { .. })
            ))
            .count(),
        1
    );
    assert_eq!(
        prefix_effects
            .iter()
            .filter(|effect| matches!(
                effect,
                WorkerAction::Context(ContextEffect::SealRoot { .. })
            ))
            .count(),
        1
    );
    assert_eq!(prefix_count, 3 + 5 + 4 + 1 - 1 + wakes);
    assert_eq!(prefix_effects.len(), prefix_count);
    assert!(no_duplicate_task_publication);
    let config = policy.context_actor(1).unwrap();
    assert_eq!(config.max_step_work, ContextCore::MAX_STEP_WORK + 1);
    assert_eq!(
        config.max_effects_per_step,
        ContextCore::MAX_STEP_ACTIONS + 1
    );
    assert_eq!(config.turn_budget, 4 * (config.max_step_work + 1));
}
