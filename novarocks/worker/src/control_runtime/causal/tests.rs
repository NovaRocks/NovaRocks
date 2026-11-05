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

//! Exact request envelopes run through the real Participation actor and original
//! causal positions. This component fixture has no Native callback or I/O body.

use super::*;
use crate::control_runtime::actors::{
    WorkerAction,
    tests::{Fixture, actor, generation, operation, task},
};
use crate::task_completion_set::AdmittedCompletionTransfer;
use crate::task_proposal_ingress::{CutSourceAdmission, RetiredCut};
use novarocks_control_runtime::{
    actor_turn::Clock, completion::ReportSlot, obligation::ReservationObligation,
    transaction::ScopeHandle,
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::{ConsumerKind, Consumption},
    identity::{ConsumerId, ReservationId, Tick},
    scope::WorkKind,
};
use novarocks_execution_contract::{
    AbortCause, CancelReason, OperationOutcome, TaskConclusion, TaskConclusionProposal,
    TerminationDetail,
};
use novarocks_worker_protocol::participation::ParticipationCommand;
use novarocks_worker_protocol::{
    creation::{CreationCost, PayloadId},
    lease::MonotonicInstant,
    participation::{ParticipationEffect, RequestResult},
};
use std::{
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Pending;
impl TaskPendingNotice for Pending {
    fn pending(&self) {}
}

fn source_role(scope: &ScopeHandle, parent: ReservationId) -> AdmittedCompletionTransfer {
    let g = scope.gates().unwrap().generation;
    let mut admitted = scope
        .reserve_child_next(
            g,
            parent,
            novarocks_control_runtime_core::capacity::ReservationResources::new(
                ResourceVector::default(),
                novarocks_control_runtime_core::owned_storage::OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..novarocks_control_runtime_core::owned_storage::OwnedSlotNeed::default()
                },
            ),
            WorkKind::Cleanup,
        )
        .unwrap();
    let report = ReportSlot::reserve_next(
        &mut admitted,
        g,
        2,
        [
            (
                ConsumerId::new(1).unwrap(),
                ConsumerKind::Owner {
                    scope: scope.id(),
                    generation: g,
                },
            ),
            (ConsumerId::new(2).unwrap(), ConsumerKind::Observer),
        ],
    )
    .unwrap();
    let (work, capacity) = admitted.bind(WorkKind::Cleanup).unwrap();
    AdmittedCompletionTransfer {
        work,
        capacity,
        report,
    }
}

fn model_source_slot(
    scope: &ScopeHandle,
    parent: ReservationId,
) -> crate::control_runtime::source_positions::SourceSlotAdmission {
    let admitted = source_role(scope, parent);
    crate::control_runtime::source_positions::SourceSlotAdmission::from_admitted(
        crate::control_runtime::reported_work_admission::adopt_reported_work(
            admitted.work,
            admitted.capacity,
            admitted.report,
        ),
    )
    .unwrap()
}
struct Harness {
    fixture: Fixture,
    task: TaskIdentity,
    ingress: Arc<TaskProposalIngress>,
    inbox: Arc<Mutex<TaskCausalInbox>>,
    parent: ReservationId,
    author: WorkObligation,
    capacity: ReservationObligation,
    author_exited: bool,
}
impl Harness {
    fn new() -> Self {
        let mut fixture = Fixture::new();
        let task = task(fixture.identity, 1);
        let create = fixture.send(
            ParticipationInput::Request(ParticipationCommand::Create {
                task,
                payload: PayloadId(1),
            }),
            Some(operation(1)),
            InputClass::Ordinary,
            2,
        );
        fixture.runtime.poll_one(Tick(1)).unwrap();
        let parent = fixture
            .drain(create)
            .iter()
            .find_map(|effect| match effect {
                WorkerAction::Participation(ParticipationEffect::LoadInput {
                    reservation, ..
                }) => Some(*reservation),
                _ => None,
            })
            .expect("the actual LoadInput adopted the original Task lifetime");
        let loaded = fixture.send(
            ParticipationInput::Loaded {
                task,
                result: Ok(CreationCost {
                    input_retained_bytes: 1,
                    ingress_retained_bytes: 1,
                    structural_bytes: 1,
                }),
            },
            None,
            InputClass::Ordinary,
            2,
        );
        fixture.runtime.poll_one(Tick(1)).unwrap();
        fixture.drain(loaded);
        let health = Arc::new(ExecutionHealth::default());
        let ingress = Arc::new(
            TaskProposalIngress::from_admitted(
                task,
                1,
                std::array::from_fn(|_| model_source_slot(&fixture.scope, parent)),
                Arc::new(Pending),
                health.clone(),
            )
            .unwrap_or_else(|_| panic!("four original source roles")),
        );
        let inbox = Arc::new(Mutex::new(TaskCausalInbox::new(
            fixture.runtime.clone(),
            fixture.registration.clone(),
            ActorId::new(7001).unwrap(),
            task,
            generation(),
            ingress.clone(),
            health,
        )));
        let (mut author, capacity) = fixture
            .scope
            .reserve_child_next(
                generation(),
                parent,
                novarocks_control_runtime_core::capacity::ReservationResources::new(
                    ResourceVector::default(),
                    novarocks_control_runtime_core::owned_storage::OwnedSlotNeed {
                        work_headers: 1,
                        completions: 0,
                        captures: 0,
                        inputs: 2,
                        batches: 2,
                        effects: 2
                            * crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
                        replies: 1,
                        ..novarocks_control_runtime_core::owned_storage::OwnedSlotNeed::default()
                    },
                ),
                WorkKind::Cleanup,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        author.start().unwrap();
        Self {
            fixture,
            task,
            ingress,
            inbox,
            parent,
            author,
            capacity,
            author_exited: false,
        }
    }
    fn exit_author(&mut self) {
        if !self.author_exited {
            self.author.body_exited().unwrap();
            self.author.captures_exited().unwrap();
            self.author_exited = true;
        }
    }
    fn admission(&self) -> CutSourceAdmission {
        CutSourceAdmission::Fresh(std::array::from_fn(|_| {
            model_source_slot(&self.fixture.scope, self.parent)
        }))
    }
    fn admit(&self, request: RequestMetadata, class: InputClass) -> (CutId, TaskCausalSender) {
        self.inbox
            .lock()
            .unwrap()
            .admit_request(
                TaskCausalRequest {
                    author: &self.author,
                    request,
                    class,
                },
                1,
                Arc::new(Pending),
                self.admission(),
            )
            .unwrap()
    }
    fn dispose_pair(&self, retired: RetiredCut) {
        for holder in self
            .ingress
            .take_unused_owned_pair(&retired.pair)
            .unwrap()
            .into_iter()
            .flatten()
        {
            holder.exit();
        }
        for observation in self.ingress.owned_pair_inventory(&retired.pair).unwrap() {
            assert!(observation.source.is_none());
            assert!(observation.custody.actual_exit_ready().unwrap());
            assert!(observation.custody.try_retire().unwrap());
        }
        self.fixture
            .ledger
            .reap_scope_ready(self.fixture.scope.id(), 256)
            .unwrap();
        let witnesses = self
            .ingress
            .prepare_owned_pair_reap(&retired.pair)
            .unwrap()
            .map(|probe| probe.observe_reaped().unwrap());
        for storage in self
            .ingress
            .release_owned_pair(&retired.pair, witnesses)
            .unwrap()
        {
            drop(storage);
        }
    }
    fn retire(&mut self, delivery: CausalDelivery) -> Vec<WorkerAction> {
        assert_eq!(
            self.inbox
                .lock()
                .unwrap()
                .observe_core(delivery.cut)
                .unwrap(),
            Consumption::Consumed
        );
        assert!(
            self.inbox
                .lock()
                .unwrap()
                .event_core_consumed(delivery.cut)
                .unwrap()
        );
        assert!(
            self.inbox
                .lock()
                .unwrap()
                .retire_after_exit(delivery.cut, CausalSourceRetirement::Unclaimed)
                .is_err(),
            "real input effects still retain the original positions"
        );
        let mut effects = Vec::new();
        if let Some(prefix) = delivery.prefix {
            effects.extend(self.fixture.drain(prefix));
        }
        effects.extend(self.fixture.drain(delivery.event));
        // Observe the same original RPC Work, not a synchronous return guess.
        // This is no claim about a Native callback, physical I/O or S8 recovery.
        self.exit_author();
        self.ingress
            .observe_cut_exit(delivery.cut, true, true)
            .unwrap();
        self.ingress.consume_cut(delivery.cut, 0).unwrap();
        let retired = self
            .inbox
            .lock()
            .unwrap()
            .retire_after_exit(delivery.cut, CausalSourceRetirement::Unclaimed)
            .unwrap();
        self.dispose_pair(retired);
        effects
    }
    fn finish(mut self) {
        self.exit_author();
        assert!(self.inbox.lock().unwrap().is_empty());
        drop(self.inbox);
        super::super::actors::tests::finish_source_routes(&self.fixture, &self.ingress);
        drop(self.ingress);
        self.fixture
            .ledger
            .reap_scope_ready(self.fixture.scope.id(), 256)
            .unwrap();
        self.capacity
            .release_quota(&self.capacity.resources().unwrap().quota)
            .unwrap();
        self.capacity.finish().unwrap();
        self.fixture.finish();
    }
}

#[test]
fn control_request_waits_for_its_actual_prefix_and_replies_to_its_event_token() {
    struct BetweenSteps {
        inbox: Arc<Mutex<TaskCausalInbox>>,
        cut: CutId,
        samples: AtomicUsize,
    }
    impl Clock for BetweenSteps {
        fn now(&self) -> Tick {
            if self.samples.fetch_add(1, Ordering::SeqCst) == 2 {
                let mut inbox = self.inbox.lock().unwrap();
                assert_eq!(inbox.observe_core(self.cut).unwrap(), Consumption::Consumed);
                assert!(
                    !inbox.event_core_consumed(self.cut).unwrap(),
                    "the committed prefix is not the high-priority request's receipt"
                );
            }
            Tick(2)
        }
    }
    let mut h = Harness::new();
    let request = RequestMetadata {
        operation: operation(61),
        deadline: MonotonicInstant::from_origin(Duration::from_nanos(99)),
    };
    let (cut, sender) = h.admit(request, InputClass::Control);
    let handoff = h
        .ingress
        .submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
    assert!(handoff.transferred && handoff.discarded.is_none() && handoff.error.is_none());
    sender
        .send_input(ParticipationInput::Request(ParticipationCommand::Cancel {
            task: h.task,
            reason: CancelReason::UpstreamNoLongerNeeded,
        }))
        .unwrap();
    let delivery = h.inbox.lock().unwrap().deliver_next().unwrap().unwrap();
    assert!(delivery.prefix.is_some());
    let clock = BetweenSteps {
        inbox: h.inbox.clone(),
        cut,
        samples: AtomicUsize::new(0),
    };
    h.fixture.runtime.poll_one_with_clock(&clock).unwrap();
    assert_eq!(
        clock.samples.load(Ordering::SeqCst),
        3,
        "the between-step oracle really ran"
    );
    drop(clock);
    let effects = h.retire(delivery);
    assert!(effects.iter().any(|effect| matches!(effect, WorkerAction::Participation(ParticipationEffect::PublishConclusion { conclusion, .. }) if *conclusion == TaskConclusion::Terminated(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)))));
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        WorkerAction::Participation(ParticipationEffect::PublishConclusion {
            conclusion: TaskConclusion::Terminated(TerminationDetail::Canceled(_)),
            ..
        })
    )));
    let replies = effects
        .iter()
        .filter_map(|effect| match effect {
            WorkerAction::Reply(reply) => Some(reply),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].request.token, delivery.event);
    assert_ne!(Some(replies[0].request.token), delivery.prefix);
    assert_eq!(replies[0].request.operation, request.operation);
    assert_eq!(replies[0].request.deadline, request.deadline);
    assert_eq!(
        replies[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    h.finish();
}

#[test]
fn ordinary_request_uses_its_frozen_deadline_and_actual_event_token() {
    let mut h = Harness::new();
    let request = RequestMetadata {
        operation: operation(62),
        deadline: MonotonicInstant::from_origin(Duration::from_nanos(1)),
    };
    let (_, sender) = h.admit(request, InputClass::Ordinary);
    sender
        .send_input(ParticipationInput::Request(
            ParticipationCommand::GetFinalInfo { task: h.task },
        ))
        .unwrap();
    let delivery = h.inbox.lock().unwrap().deliver_next().unwrap().unwrap();
    assert!(delivery.prefix.is_none());
    h.fixture.runtime.poll_one(Tick(2)).unwrap();
    let effects = h.retire(delivery);
    let replies = effects
        .iter()
        .filter_map(|effect| match effect {
            WorkerAction::Reply(reply) => Some(reply),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].request.token, delivery.event);
    assert_eq!(replies[0].request.operation, request.operation);
    assert_eq!(replies[0].request.deadline, request.deadline);
    assert_eq!(
        replies[0].result,
        RequestResult::Outcome(OperationOutcome::OperationTimedOut)
    );
    h.finish();
}

// Refusal must return the same two paid original roles, not two replacement
// reservations with numerically similar identities. Keep actual report and
// Work retirement observers across the attempted admission.
fn check_admission_refusal(
    h: &Harness,
    author: &WorkObligation,
    class: InputClass,
    expected: CausalError,
) {
    let admission = h.admission();
    let CutSourceAdmission::Fresh(roles) = &admission else {
        unreachable!()
    };
    let originals = roles.each_ref().map(|role| {
        (
            role.reported().work().id(),
            role.reported().work().scope().id(),
            role.reported().report().clone(),
            role.reported().work().retirement_observer(),
        )
    });
    let actor_before = h.fixture.runtime.snapshot(actor()).unwrap();
    let scope_before = h.fixture.scope.snapshot().unwrap();
    let refusal = h.inbox.lock().unwrap().admit_request(
        TaskCausalRequest {
            author,
            request: RequestMetadata {
                operation: operation(63),
                deadline: MonotonicInstant::from_origin(Duration::from_nanos(99)),
            },
            class,
        },
        1,
        Arc::new(Pending),
        admission,
    );
    let refusal = match refusal {
        Err(refusal) => refusal,
        Ok(_) => panic!("foreign or unsupported request author must be refused"),
    };
    assert_eq!(refusal.error, expected);
    // These are actual runtime custody snapshots; absence from a ready queue
    // is neither the admission oracle nor a consumption/exit receipt.
    assert_eq!(h.fixture.runtime.snapshot(actor()).unwrap(), actor_before);
    assert_eq!(h.fixture.scope.snapshot().unwrap(), scope_before);
    assert!(h.inbox.lock().unwrap().is_empty());
    let CutSourceAdmission::Fresh(roles) = refusal.admission else {
        panic!("return the original fresh Source2")
    };
    for (slot, (work, scope, report, retired)) in roles.into_iter().zip(originals) {
        assert_eq!(slot.reported().work().id(), work);
        assert_eq!(slot.reported().work().scope().id(), scope);
        assert!(slot.reported().report().same_slot(&report));
        assert!(!retired.is_retired());
        let role = slot.into_reported();
        let (custody, unused) =
            crate::control_runtime::source_custody::SourceCustody::from_reported(
                h.task,
                role,
                Arc::new(Pending),
                Arc::new(ExecutionHealth::default()),
            );
        // The original unused permit's actual Drop revokes its report and
        // observes this never-invoked body. No Core or fake callback is used.
        drop(unused);
        assert!(custody.actual_exit_ready().unwrap());
        assert!(custody.try_retire().unwrap());
        h.fixture
            .ledger
            .reap_scope_ready(h.fixture.scope.id(), 256)
            .unwrap();
        assert!(retired.is_retired());
        assert!(custody.reusable());
    }
}

#[test]
fn lifecycle_and_timer_request_classes_return_the_same_unspent_source_roles() {
    for class in [InputClass::Lifecycle, InputClass::Timer] {
        let h = Harness::new();
        check_admission_refusal(&h, &h.author, class, CausalError::InvalidRequestClass);
        h.finish();
    }
}

#[test]
fn mismatched_request_bodies_return_the_original_input_without_sealing_the_cut() {
    for mismatch in 0..5 {
        let mut h = Harness::new();
        let request = RequestMetadata {
            operation: operation(64),
            deadline: MonotonicInstant::from_origin(Duration::from_nanos(99)),
        };
        let (cut, sender) = h.admit(request, InputClass::Control);
        let actor_before = h.fixture.runtime.snapshot(actor()).unwrap();
        let wrong_task = task(h.fixture.identity, 2);
        let input = match mismatch {
            0 => ParticipationInput::Request(ParticipationCommand::Cancel {
                task: wrong_task,
                reason: CancelReason::UpstreamNoLongerNeeded,
            }),
            1 => ParticipationInput::Request(ParticipationCommand::Release),
            2 => ParticipationInput::Running { task: h.task },
            3 => ParticipationInput::Tick,
            4 => ParticipationInput::Request(ParticipationCommand::Create {
                task: h.task,
                payload: PayloadId(777),
            }),
            _ => unreachable!(),
        };
        let returned = sender.send_input(input).unwrap_err();
        match (mismatch, returned) {
            (0, ParticipationInput::Request(ParticipationCommand::Cancel { task: id, reason })) => {
                assert_eq!(id, wrong_task);
                assert_eq!(reason, CancelReason::UpstreamNoLongerNeeded);
            }
            (1, ParticipationInput::Request(ParticipationCommand::Release)) => {}
            (2, ParticipationInput::Running { task: id }) => assert_eq!(id, h.task),
            (3, ParticipationInput::Tick) => {}
            (
                4,
                ParticipationInput::Request(ParticipationCommand::Create { task: id, payload }),
            ) => {
                assert_eq!(id, h.task);
                assert_eq!(payload, PayloadId(777));
            }
            _ => panic!("refusal must return the same neutral request body"),
        }
        assert_eq!(h.fixture.runtime.snapshot(actor()).unwrap(), actor_before);
        assert_eq!(h.inbox.lock().unwrap().cut_inventory(None, 2), vec![cut]);
        h.exit_author();
        h.ingress.revoke_event(cut).unwrap();
        h.ingress.observe_cut_exit(cut, true, true).unwrap();
        let retired = h
            .inbox
            .lock()
            .unwrap()
            .retire_after_exit(cut, CausalSourceRetirement::Unclaimed)
            .expect("unused-cut retirement rejects a mistakenly sealed cut");
        h.dispose_pair(retired);
        h.finish();
    }
}

fn exercise_foreign_author() {
    use novarocks_control_runtime::transaction::RuntimeLedger;
    use novarocks_control_runtime_core::scope::WorkAdmission;
    let h = Harness::new();
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let mut authors = Vec::new();
    // Real original Works intentionally reach the same numeric ID; no explicit
    // fabricated identity or foreign Root numeric Work can authorize Participation.
    for _ in 0..h.author.id().get() {
        let (mut work, capacity) = scope
            .reserve_next(
                generation(),
                novarocks_control_runtime_core::capacity::ReservationResources::work_only(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        authors.push((work, capacity));
    }
    let author = &authors.last().unwrap().0;
    assert_eq!(author.id(), h.author.id());
    assert_eq!(scope.id(), h.fixture.scope.id());
    check_admission_refusal(
        &h,
        author,
        InputClass::Control,
        CausalError::ActorTurn(ActorTurnError::InputAuthorMismatch),
    );
    for (mut work, mut capacity) in authors {
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        ledger.reap_scope_ready(scope.id(), 256).unwrap();
        capacity
            .release_quota(&capacity.resources().unwrap().quota)
            .unwrap();
        capacity.finish().unwrap();
    }
    scope.close_external().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
    h.finish();
}

#[test]
fn a_foreign_ledger_with_the_same_scope_and_work_numbers_cannot_author_a_participation_request() {
    exercise_foreign_author();
}

mod rpc_author;
