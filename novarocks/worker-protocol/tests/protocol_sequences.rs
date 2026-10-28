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

//! Contract sequences use explicit identities, time and an inline reservation
//! observer. Driver synchronization and actual holder teardown are separate tests.
use novarocks_control_runtime_core::identity::{Generation, InputId, ReservationId};
use novarocks_execution_contract::{
    AbortCause, AcquireQueryContextAdmissionTicket, AdmissionTicketId, CancelReason, LeaseSequence,
    LeaseValidFor, OperationOutcome, QueryContextRef, QueryContextState, TaskConclusion,
    TaskIdentity, TaskOperationId, TaskOutputFacts, TaskState, TaskStatus, TaskStatusCursor,
    TaskStatusVersion, TerminationDetail,
};
use novarocks_types::{
    NativeCompatibilityId,
    identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    },
};
use novarocks_worker_protocol::{
    admission::{
        AdmissionCore, AdmissionTicketConfig, AdmissionTicketObservation, AdmissionTicketRedemption,
    },
    context::{
        ContextCommand, ContextConfig, ContextCore, ContextEffect, ContextEstablishment,
        ContextEstablishmentDecision, ContextInput, ContextMaterialized, ContextStep, PhysicalFact,
        RequestResult,
    },
    creation::{
        AdmissionNeed, AdmissionPort, AdmissionRefusal, CreationCost, PayloadId, RequestContext,
    },
    lease::{LeaseBounds, MonotonicInstant},
    observation::{
        CoveredObservationFact, CoveredObservationFrame, CoveredSubscriptionError,
        ObservationSource, SubscriptionId,
    },
    proposal_summary::Proposal,
};
use std::time::Duration;
fn bytes(n: u8) -> [u8; 16] {
    let mut bytes = [n; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes
}
fn at(n: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_secs(n))
}
fn operation(n: u8) -> TaskOperationId {
    TaskOperationId::try_from_bytes(bytes(n)).unwrap()
}
fn context() -> QueryContextRef {
    QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(7101, 7102), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(bytes(1)).unwrap(),
        BackendProcessId::try_from_bytes(bytes(2)).unwrap(),
    )
}
fn task(n: u32) -> TaskIdentity {
    let c = context();
    TaskIdentity::new(
        c.query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(n).unwrap(),
        c.backend_process_id(),
    )
}
fn core(max_tasks: usize, max_preparing: usize) -> ContextCore {
    core_with_initial_lease(max_tasks, max_preparing, at(0))
}
fn core_with_initial_lease(
    max_tasks: usize,
    max_preparing: usize,
    elected_at: MonotonicInstant,
) -> ContextCore {
    core_with_establishment(
        ContextEstablishment {
            operation: operation(2),
            ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
            payload: PayloadId(1),
            semantics: semantics(),
            elected_at,
        },
        max_tasks,
        max_preparing,
    )
}
fn core_with_establishment(
    establishment: ContextEstablishment,
    max_tasks: usize,
    max_preparing: usize,
) -> ContextCore {
    ContextCore::new(
        context(),
        Generation::new(1).unwrap(),
        establishment,
        ContextConfig {
            max_tasks,
            max_creation_waiters: 8,
            max_pending_requests: 64,
            max_preparing,
            max_task_bytes: 100,
            step_budget: ContextCore::MAX_STEP_WORK,
            lease_bounds: LeaseBounds::DEFAULT,
            metric_publish_interval: Duration::from_millis(100),
        },
    )
    .unwrap()
}
#[derive(Default)]
struct Port {
    calls: Vec<AdmissionNeed>,
    next: u64,
    reject: Option<OperationOutcome>,
    typed_reject: Option<AdmissionRefusal>,
    next_context_input: u64,
    context_tokens: std::collections::BTreeMap<TaskOperationId, InputId>,
}
impl AdmissionPort for Port {
    fn try_reserve(&mut self, need: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal> {
        self.calls.push(need);
        if let Some(refusal) = &self.typed_reject {
            return Err(refusal.clone());
        }
        if let Some(reject) = self.reject {
            return Err(reject.into());
        }
        self.next += 1;
        Ok(ReservationId::new(self.next).unwrap())
    }
}

#[derive(Debug)]
enum FixtureInput {
    Fact(Box<ContextInput>),
    Rpc(TaskOperationId, Box<ContextCommand>),
}
impl From<ContextInput> for FixtureInput {
    fn from(input: ContextInput) -> Self {
        Self::Fact(Box::new(input))
    }
}
fn rpc(operation: TaskOperationId, command: ContextCommand) -> FixtureInput {
    FixtureInput::Rpc(operation, Box::new(command))
}
impl Port {
    fn request_token(&self, operation: TaskOperationId) -> InputId {
        *self
            .context_tokens
            .get(&operation)
            .expect("the fixture issued this actual request")
    }
}
trait FixtureContext {
    fn apply_fixture(
        &mut self,
        now: MonotonicInstant,
        input: impl Into<FixtureInput>,
        port: &mut Port,
    ) -> Result<ContextStep, novarocks_worker_protocol::context::CoreError>;
}
impl FixtureContext for ContextCore {
    fn apply_fixture(
        &mut self,
        now: MonotonicInstant,
        input: impl Into<FixtureInput>,
        port: &mut Port,
    ) -> Result<ContextStep, novarocks_worker_protocol::context::CoreError> {
        match input.into() {
            FixtureInput::Fact(input) => self.handle(now, *input, port),
            FixtureInput::Rpc(operation, command) => {
                // The external fixture issues every token, even for the same wire operation.
                port.next_context_input += 1;
                let token = InputId::new(1_000_000 + port.next_context_input).unwrap();
                port.context_tokens.insert(operation, token);
                self.handle_request(
                    now,
                    RequestContext {
                        token,
                        operation,
                        deadline: at(10_000),
                    },
                    *command,
                    port,
                )
            }
        }
    }
}

fn cost() -> CreationCost {
    CreationCost {
        input_retained_bytes: 3,
        ingress_retained_bytes: 4,
        structural_bytes: 7,
        inbound_records: 2,
        inbound_bytes: 9,
    }
}
fn activate(core: &mut ContextCore, port: &mut Port) {
    core.handle(
        at(0),
        ContextInput::MaterializationFinished {
            result: Ok(ContextMaterialized {
                initial_credential_resource: 101,
            }),
        },
        port,
    )
    .unwrap();
}
fn create(core: &mut ContextCore, port: &mut Port, n: u8) {
    core.apply_fixture(
        at(0),
        rpc(
            operation(n),
            ContextCommand::Create {
                task: task(n.into()),
                payload: PayloadId(n.into()),
            },
        ),
        port,
    )
    .unwrap();
}
fn loaded(core: &mut ContextCore, port: &mut Port, n: u8) -> Vec<ContextEffect> {
    let mut all = Vec::new();
    let mut input = ContextInput::Loaded {
        task: task(n.into()),
        result: Ok(cost()),
    };
    loop {
        let step = core.handle(at(0), input, port).unwrap();
        assert!(step.charged_work() <= ContextCore::MAX_STEP_WORK);
        let more = step
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::ContinueReplies { .. }));
        all.extend(step.effects);
        if !more {
            return all;
        }
        input = ContextInput::ContinueReplies {
            task: task(n.into()),
        };
    }
}

fn proposal(conclusion: TaskConclusion) -> Proposal {
    Proposal {
        conclusion,
        evidence: None,
        source: None,
        diagnostic: None,
    }
}

#[test]
fn settlement_flushes_its_predecessors_without_freezing_later_conclusions_or_claiming_exit() {
    use novarocks_control_runtime_core::identity::WorkId;
    let mut core = core(4, 1);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    let admission_calls = port.calls.len();
    let first = core
        .handle(
            at(0),
            ContextInput::Settle {
                task: task(1),
                work: WorkId::new(1).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(
        first.effects,
        [ContextEffect::SettlementBarrier {
            task: task(1),
            work: WorkId::new(1).unwrap(),
        }]
    );
    assert!(core.task(task(1)).unwrap().conclusion.accepted.is_none());
    let cancellation = TaskConclusion::Terminated(TerminationDetail::Canceled(
        CancelReason::UpstreamNoLongerNeeded,
    ));
    let queued = core
        .handle(
            at(1),
            ContextInput::Proposal {
                task: task(1),
                proposal: proposal(cancellation.clone()),
            },
            &mut port,
        )
        .unwrap();
    assert!(queued.effects.is_empty());
    let second = core
        .handle(
            at(1),
            ContextInput::Settle {
                task: task(1),
                work: WorkId::new(2).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    assert!(second.charged_work() <= ContextCore::MAX_STEP_WORK);
    let publication = second
        .effects
        .iter()
        .position(|effect| {
            matches!(effect,
        ContextEffect::PublishConclusion { conclusion, .. } if conclusion == &cancellation)
        })
        .unwrap();
    let barrier = second
        .effects
        .iter()
        .position(|effect| {
            matches!(effect,
        ContextEffect::SettlementBarrier { work, .. } if work.get() == 2)
        })
        .unwrap();
    assert!(publication < barrier);
    let task = core.task(task(1)).unwrap();
    assert_eq!(task.conclusion.accepted, Some(cancellation));
    assert!(!task.actual_stopped);
    assert!(!task.output_released);
    assert!(!task.resources_converged);
    assert_eq!(
        port.calls.len(),
        admission_calls,
        "settlement consumes its original admission instead of reserving more capacity"
    );
}

#[test]
fn exact_create_replays_share_one_loader_and_reserve_the_complete_preparation_cost() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    core.apply_fixture(
        at(0),
        rpc(
            operation(2),
            ContextCommand::Create {
                task: task(1),
                payload: PayloadId(999),
            },
        ),
        &mut port,
    )
    .unwrap();
    assert_eq!(port.calls.len(), 1);
    let effects = loaded(&mut core, &mut port, 1);
    assert!(matches!(
        port.calls[1],
        AdmissionNeed::Preparation {
            bytes: 14,
            inbound_records: 2,
            inbound_bytes: 9,
            ..
        }
    ));
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e, ContextEffect::EnqueuePreparation { .. }))
            .count(),
        1
    );
    let published = effects
        .iter()
        .position(|e| matches!(e, ContextEffect::PublishTask { .. }))
        .unwrap();
    let enqueue = effects
        .iter()
        .position(|effect| matches!(effect, ContextEffect::EnqueuePreparation { .. }))
        .unwrap();
    assert!(
        published < enqueue,
        "preparation starts only after the Accepted publication commits"
    );
    let replies: Vec<_> = effects
        .iter()
        .enumerate()
        .filter_map(|(i, e)| {
            if let ContextEffect::Reply { request, outcome } = e {
                Some((i, *request, *outcome))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(replies.len(), 2);
    assert!(
        replies
            .iter()
            .all(|(i, _, outcome)| *i > enqueue && *outcome == OperationOutcome::Accepted)
    );
    assert_eq!(core.spent_identities(), 1);
}
#[test]
fn abort_predecessor_is_applied_before_a_late_loader_result() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    core.apply_fixture(
        at(0),
        rpc(
            operation(4),
            ContextCommand::Abort {
                cause: AbortCause::QueryFailed,
            },
        ),
        &mut port,
    )
    .unwrap();
    let effects = loaded(&mut core, &mut port, 1);
    assert_eq!(
        port.calls.len(),
        1,
        "late loader cannot reserve preparation after closure"
    );
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::CancelLoader { .. })),
        "only the Task's ordered close cut cancels its loader"
    );
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        ContextEffect::PublishTask { .. }
            | ContextEffect::PublishConclusion { .. }
            | ContextEffect::PublishTaskConvergence { .. }
    )));
    assert!(core.status(task(1)).is_none());
    assert!(effects.iter().any(|e| matches!(
        e,
        ContextEffect::Reply {
            outcome: OperationOutcome::ContextTerminalReceipt,
            ..
        }
    )));
    assert!(!core.task(task(1)).unwrap().loader_exited);
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, ContextEffect::RetireTask { .. }))
    );
    for input in [
        ContextInput::ContinueClose,
        ContextInput::ApplyTaskClose {
            task: task(1),
            generation: Generation::new(2).unwrap(),
        },
        ContextInput::Physical {
            task: task(1),
            fact: PhysicalFact::LoaderExited,
        },
    ] {
        let step = core.handle(at(0), input, &mut port).unwrap();
        assert!(!step.effects.iter().any(|effect| matches!(
            effect,
            ContextEffect::PublishTask { .. }
                | ContextEffect::PublishConclusion { .. }
                | ContextEffect::PublishTaskConvergence { .. }
        )));
    }
    assert!(core.status(task(1)).is_none());
}
#[test]
fn installed_and_body_exit_do_not_return_the_preparation_position() {
    let mut core = core(8, 1);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    core.handle(at(0), ContextInput::Installed { task: task(1) }, &mut port)
        .unwrap();
    core.handle(
        at(0),
        ContextInput::Physical {
            task: task(1),
            fact: PhysicalFact::PreparationBodyExited,
        },
        &mut port,
    )
    .unwrap();
    create(&mut core, &mut port, 2);
    assert!(loaded(&mut core, &mut port, 2).iter().any(|e| matches!(
        e,
        ContextEffect::Reply {
            outcome: OperationOutcome::PreparationBusy,
            ..
        }
    )));
    core.handle(
        at(0),
        ContextInput::Physical {
            task: task(1),
            fact: PhysicalFact::PreparationExited,
        },
        &mut port,
    )
    .unwrap();
    create(&mut core, &mut port, 3);
    assert!(loaded(&mut core, &mut port, 3).iter().any(|e| matches!(
        e,
        ContextEffect::Reply {
            outcome: OperationOutcome::Accepted,
            ..
        }
    )));
}
#[test]
fn release_waits_for_each_physical_fact_and_for_actual_host_release() {
    let mut core = core(8, 1);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    core.handle(
        at(0),
        ContextInput::Proposal {
            task: task(1),
            proposal: proposal(TaskConclusion::Terminated(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            ))),
        },
        &mut port,
    )
    .unwrap();
    core.handle(at(0), ContextInput::Adjudicate { task: task(1) }, &mut port)
        .unwrap();
    for fact in [
        PhysicalFact::LoaderExited,
        PhysicalFact::PreparationExited,
        PhysicalFact::ActualStopped,
        PhysicalFact::OutputReleased,
        PhysicalFact::RootIdle,
    ] {
        core.handle(
            at(0),
            ContextInput::Physical {
                task: task(1),
                fact,
            },
            &mut port,
        )
        .unwrap();
    }
    core.handle(at(0), ContextInput::Finish { task: task(1) }, &mut port)
        .unwrap();
    core.apply_fixture(at(0), rpc(operation(6), ContextCommand::Quiesce), &mut port)
        .unwrap();
    core.handle(at(0), ContextInput::ContinueQuiesce, &mut port)
        .unwrap();
    let refused = core
        .apply_fixture(at(0), rpc(operation(7), ContextCommand::Release), &mut port)
        .unwrap();
    assert!(refused.effects.iter().any(|e| matches!(
        e,
        ContextEffect::Reply {
            outcome: OperationOutcome::ReleaseNotReady,
            ..
        }
    )));
    let exited = core
        .handle(
            at(0),
            ContextInput::Physical {
                task: task(1),
                fact: PhysicalFact::ResourcesConverged,
            },
            &mut port,
        )
        .unwrap();
    assert!(
        exited
            .effects
            .iter()
            .any(|e| matches!(e, ContextEffect::RetireTask { .. }))
    );
    let releasing = core
        .apply_fixture(at(0), rpc(operation(7), ContextCommand::Release), &mut port)
        .unwrap();
    assert!(
        releasing
            .effects
            .iter()
            .any(|e| matches!(e, ContextEffect::ReleaseHost { .. }))
    );
    assert!(!releasing.effects.iter().any(|e| matches!(
        e,
        ContextEffect::Reply {
            outcome: OperationOutcome::Accepted,
            ..
        }
    )));
    let released = core
        .handle(at(0), ContextInput::HostReleased, &mut port)
        .unwrap();
    assert_eq!(core.state(), QueryContextState::TerminalRetained);
    assert!(released.effects.iter().any(|e|matches!(e,ContextEffect::Reply{request,outcome:OperationOutcome::Accepted} if *request == port.request_token(operation(7)))));
}
#[test]
fn current_input_time_expires_a_lease_before_a_delayed_timer_arrives() {
    let mut core = core(8, 1);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    let step = core
        .apply_fixture(
            at(30),
            rpc(
                operation(7),
                ContextCommand::Renew {
                    sequence: LeaseSequence::new(1),
                    valid_for: LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
                },
            ),
            &mut port,
        )
        .unwrap();
    assert_eq!(core.state(), QueryContextState::Aborting);
    assert_eq!(core.generation().get(), 2);
    assert!(step.effects.iter().any(|e| matches!(
        e,
        ContextEffect::Reply {
            outcome: OperationOutcome::LeaseExpired,
            ..
        }
    )));
    assert!(core.handle(at(29), ContextInput::Tick, &mut port).is_err());
}
#[test]
fn redemption_does_not_return_a_ticket_reservation_or_expire_an_established_context() {
    let mut tickets = AdmissionCore::new(AdmissionTicketConfig::DEFAULT, [1; 8]);
    let mut port = Port::default();
    let request = AcquireQueryContextAdmissionTicket::new(
        operation(5),
        context(),
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        tickets.capability(),
    );
    let id = AdmissionTicketId::try_from_bytes([4; 16]).unwrap();
    let grant = tickets.acquire(request, id, at(0), &mut port).unwrap();
    assert_eq!(
        tickets
            .acquire(
                request,
                AdmissionTicketId::try_from_bytes([5; 16]).unwrap(),
                at(1),
                &mut port
            )
            .unwrap()
            .receipt(),
        grant.receipt()
    );
    assert_eq!(port.calls.len(), 1);
    assert!(matches!(
        tickets.redeem(id, context(), at(1)),
        Ok(AdmissionTicketRedemption::Redeemed(_))
    ));
    assert!(tickets.advance(at(8), 8).is_empty());
    assert_eq!(
        tickets.observe(id, context(), at(8)),
        AdmissionTicketObservation::Redeemed
    );
    assert_eq!(
        tickets.close_context(context(), at(8)),
        ReservationId::new(1)
    );
    assert!(tickets.close_context(context(), at(8)).is_none());
}
fn status(version: u64) -> TaskStatus {
    TaskStatus::try_new(
        task(1),
        TaskStatusVersion::new(version).unwrap(),
        TaskState::Planned,
        None,
        TaskOutputFacts::new(false),
    )
    .unwrap()
}
#[test]
fn covered_selection_is_not_delivery_and_a_newer_selected_key_remains_owed() {
    let mut source = ObservationSource::new(context(), 8, 2, 1024, 8192);
    let id = SubscriptionId(1);
    source
        .publish(CoveredObservationFact::Status(status(1)), 100)
        .unwrap();
    source
        .begin_subscribe(id, 9, vec![TaskStatusCursor::unobserved(task(1))])
        .unwrap();
    assert!(source.continue_publication(1).complete);
    while !source.continue_subscribe(id, 1).unwrap().complete {}
    let initial = source.select_next(id).unwrap().unwrap();
    assert_eq!(source.bookmark(id).unwrap().covered_prefix, 0);
    assert_eq!(source.select_next(id).unwrap().unwrap(), initial);
    source.note_delivered(id, initial.delivery_id).unwrap();
    let complete = source.select_next(id).unwrap().unwrap();
    assert!(matches!(
        complete.frame,
        CoveredObservationFrame::CatchUpComplete { initial_cut: 1 }
    ));
    assert_eq!(
        source
            .note_delivered(id, complete.delivery_id)
            .unwrap()
            .covered_prefix,
        1
    );
    source
        .publish(CoveredObservationFact::Status(status(2)), 100)
        .unwrap();
    assert!(source.continue_publication(1).complete);
    let selected = source.select_next(id).unwrap().unwrap();
    source
        .publish(CoveredObservationFact::Status(status(3)), 100)
        .unwrap();
    assert!(source.continue_publication(1).complete);
    assert_eq!(source.bookmark(id).unwrap().covered_prefix, 1);
    assert_eq!(
        source
            .note_delivered(id, selected.delivery_id)
            .unwrap()
            .covered_prefix,
        2
    );
    let next = source.select_next(id).unwrap().unwrap();
    assert!(matches!(
        next.frame,
        CoveredObservationFrame::Live { revision: 3, .. }
    ));
    assert_eq!(
        source
            .note_delivered(id, next.delivery_id)
            .unwrap()
            .covered_prefix,
        3
    );
    assert_eq!(
        source.publish(CoveredObservationFact::Status(status(2)), 100),
        Err(CoveredSubscriptionError::SourceRegression)
    );
}

#[test]
fn catchup_freezes_initial_cut_before_incremental_validation_and_publication_fanout() {
    let mut source = ObservationSource::new(context(), 8, 2, 1024, 8192);
    source
        .publish(CoveredObservationFact::Status(status(1)), 100)
        .unwrap();
    assert!(source.continue_publication(1).complete);
    let id = SubscriptionId(7);
    assert_eq!(
        source
            .begin_subscribe(id, 1, vec![TaskStatusCursor::unobserved(task(1))])
            .unwrap(),
        1
    );
    source
        .publish(CoveredObservationFact::Status(status(2)), 100)
        .unwrap();
    assert_eq!(source.bookmark(id).unwrap().source_cut, 1);
    // Snapshot reads version 1 even before the pending update has visited this
    // subscriber; its pending old value is part of the frozen-cut lookup.
    assert_eq!(source.continue_subscribe(id, 1).unwrap().work, 1);
    assert!(source.continue_publication(1).complete);
    while !source.continue_subscribe(id, 1).unwrap().complete {}
    let first = source.select_next(id).unwrap().unwrap();
    assert_eq!(
        first.frame,
        CoveredObservationFrame::CatchUp(CoveredObservationFact::Status(status(1)))
    );
    source.note_delivered(id, first.delivery_id).unwrap();
    let complete = source.select_next(id).unwrap().unwrap();
    assert_eq!(
        complete.frame,
        CoveredObservationFrame::CatchUpComplete { initial_cut: 1 }
    );
    assert_eq!(
        source
            .note_delivered(id, complete.delivery_id)
            .unwrap()
            .covered_prefix,
        1
    );
    let live = source.select_next(id).unwrap().unwrap();
    assert_eq!(
        live.frame,
        CoveredObservationFrame::Live {
            revision: 2,
            fact: CoveredObservationFact::Status(status(2))
        }
    );
    assert_eq!(
        source
            .note_delivered(id, live.delivery_id)
            .unwrap()
            .covered_prefix,
        2
    );
}
#[test]
fn publication_does_not_advance_a_subscriber_cut_before_bounded_fanout_visits_it() {
    let mut source = ObservationSource::new(context(), 8, 2, 1024, 8192);
    let a = SubscriptionId(1);
    let b = SubscriptionId(2);
    for id in [a, b] {
        source.begin_subscribe(id, 1, vec![]).unwrap();
        while !source.continue_subscribe(id, 1).unwrap().complete {}
        let complete = source.select_next(id).unwrap().unwrap();
        source.note_delivered(id, complete.delivery_id).unwrap();
    }
    source
        .publish(CoveredObservationFact::Status(status(1)), 100)
        .unwrap();
    assert_eq!(
        source.publish(CoveredObservationFact::Status(status(2)), 100),
        Err(CoveredSubscriptionError::PublicationPending)
    );
    assert_eq!(
        source.continue_publication(1),
        novarocks_worker_protocol::observation::ObservationProgress {
            work: 1,
            complete: false
        }
    );
    assert_eq!(source.bookmark(a).unwrap().source_cut, 1);
    assert_eq!(source.bookmark(b).unwrap().source_cut, 0);
    assert!(source.continue_publication(1).complete);
    assert_eq!(source.bookmark(b).unwrap().source_cut, 1);
}

#[test]
fn absolute_request_deadline_settles_only_reply_and_keeps_accepted_loader_owned() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    let request = RequestContext {
        token: InputId::new(1).unwrap(),
        operation: operation(1),
        deadline: at(2),
    };
    let start = core
        .handle_request(
            at(0),
            request,
            ContextCommand::Create {
                task: task(1),
                payload: PayloadId(1),
            },
            &mut port,
        )
        .unwrap();
    assert!(start.requests.is_empty());
    assert_eq!(core.next_deadline(), Some(at(2)));
    assert_eq!(port.calls.len(), 1);
    let timeout = core.handle(at(2), ContextInput::Tick, &mut port).unwrap();
    assert_eq!(timeout.requests.len(), 1);
    assert_eq!(timeout.requests[0].request, request);
    assert_eq!(
        timeout.requests[0].result,
        RequestResult::TimeoutWhileAccepted
    );
    assert_eq!(core.state(), QueryContextState::Active);
    assert!(!core.task(task(1)).unwrap().loader_exited);
    assert!(
        !timeout
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::RetireTask { .. }))
    );
    let loaded = core
        .handle(
            at(3),
            ContextInput::Loaded {
                task: task(1),
                result: Ok(cost()),
            },
            &mut port,
        )
        .unwrap();
    assert!(loaded.requests.is_empty());
    assert_eq!(port.calls.len(), 2);
    assert!(
        core.task(task(1))
            .unwrap()
            .preparation_reservation
            .is_some()
    );
}
#[test]
fn a_late_request_never_starts_work_and_same_operation_tokens_share_only_the_created_entity() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    let command = || ContextCommand::Create {
        task: task(1),
        payload: PayloadId(1),
    };
    let late = core
        .handle_request(
            at(1),
            RequestContext {
                token: InputId::new(1).unwrap(),
                operation: operation(1),
                deadline: at(1),
            },
            command(),
            &mut port,
        )
        .unwrap();
    assert_eq!(
        late.requests[0].result,
        RequestResult::Outcome(OperationOutcome::OperationTimedOut)
    );
    assert!(port.calls.is_empty());
    for token in 2..=3 {
        core.handle_request(
            at(1),
            RequestContext {
                token: InputId::new(token).unwrap(),
                operation: operation(1),
                deadline: at(10),
            },
            command(),
            &mut port,
        )
        .unwrap();
    }
    assert_eq!(port.calls.len(), 1);
    let ready = core
        .handle(
            at(2),
            ContextInput::Loaded {
                task: task(1),
                result: Ok(cost()),
            },
            &mut port,
        )
        .unwrap();
    let more = core
        .handle(
            at(2),
            ContextInput::ContinueReplies { task: task(1) },
            &mut port,
        )
        .unwrap();
    let mut replies = ready.requests;
    replies.extend(more.requests);
    assert_eq!(replies.len(), 2);
    assert_eq!(
        replies
            .iter()
            .map(|reply| reply.request.token)
            .collect::<Vec<_>>(),
        vec![InputId::new(2).unwrap(), InputId::new(3).unwrap()],
        "the same wire operation settles each actual request separately"
    );
    assert!(
        replies
            .iter()
            .all(|reply| reply.request.operation == operation(1))
    );
    assert!(
        replies
            .iter()
            .all(|reply| reply.result == RequestResult::Outcome(OperationOutcome::Accepted))
    );
    assert_eq!(port.calls.len(), 2);
}

#[test]
fn preparation_publication_hold_defers_runtime_exit_without_requiring_the_normal_tail_to_exit() {
    use novarocks_worker_protocol::context::PreparationExitRole;
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    core.handle(
        at(0),
        ContextInput::PreparationExitHold {
            task: task(1),
            role: PreparationExitRole::Preparation,
            held: true,
        },
        &mut port,
    )
    .unwrap();
    for fact in [
        PhysicalFact::ActualStopped,
        PhysicalFact::ResourcesConverged,
    ] {
        core.handle(
            at(1),
            ContextInput::Physical {
                task: task(1),
                fact,
            },
            &mut port,
        )
        .unwrap();
    }
    assert!(!core.task(task(1)).unwrap().actual_stopped);
    assert!(!core.task(task(1)).unwrap().resources_converged);
    assert!(!core.task(task(1)).unwrap().preparation_exited);
    let exited = core
        .handle(
            at(2),
            ContextInput::PreparationExitHold {
                task: task(1),
                role: PreparationExitRole::Preparation,
                held: false,
            },
            &mut port,
        )
        .unwrap();
    assert!(core.task(task(1)).unwrap().actual_stopped);
    assert!(core.task(task(1)).unwrap().resources_converged);
    assert!(!core.task(task(1)).unwrap().preparation_exited);
    assert!(
        exited
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::PublishTaskConvergence { .. }))
    );
}
#[test]
fn preparation_failure_phase_is_chosen_at_the_installed_cut_by_the_core() {
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure, TaskFailurePhase,
        failure::{FailureClass, FailureFact, FailureReason, domain},
    };
    use novarocks_worker_protocol::proposal_summary::SourceId;
    use std::num::NonZeroU16;
    let fact = FailureFact::new(
        FailureClass::Data,
        FailureReason::new(domain::EXECUTION, NonZeroU16::new(1).unwrap()),
    );
    let failure = |phase| Proposal {
        conclusion: TaskConclusion::Terminated(TerminationDetail::Failed(
            TaskFailure::new_in_phase(fact, SafeDetail::truncating("body failed"), phase),
        )),
        evidence: None,
        source: Some(SourceId(1)),
        diagnostic: None,
    };
    for installed in [false, true] {
        let mut core = core(8, 2);
        let mut port = Port::default();
        activate(&mut core, &mut port);
        create(&mut core, &mut port, 1);
        loaded(&mut core, &mut port, 1);
        if installed {
            core.handle(at(0), ContextInput::Installed { task: task(1) }, &mut port)
                .unwrap();
        }
        core.handle(
            at(1),
            ContextInput::PreparationBodyFailure {
                task: task(1),
                preparing: failure(TaskFailurePhase::Preparation),
                executing: failure(TaskFailurePhase::Execution),
            },
            &mut port,
        )
        .unwrap();
        let Some(TaskConclusion::Terminated(TerminationDetail::Failed(failure))) =
            &core.task(task(1)).unwrap().conclusion.accepted
        else {
            panic!("body failure selected");
        };
        assert_eq!(
            failure.phase(),
            if installed {
                TaskFailurePhase::Execution
            } else {
                TaskFailurePhase::Preparation
            }
        );
    }
}
#[test]
fn resources_converged_cannot_precede_the_actual_runtime_stop_receipt() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    assert_eq!(
        core.handle(
            at(1),
            ContextInput::Physical {
                task: task(1),
                fact: PhysicalFact::ResourcesConverged
            },
            &mut port
        )
        .unwrap_err(),
        novarocks_worker_protocol::context::CoreError::IllegalFact
    );
    assert!(!core.task(task(1)).unwrap().resources_converged);
}

#[test]
fn a_losing_summary_releases_its_source_without_republishing_the_accepted_generation() {
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure,
        failure::{FailureClass, FailureFact, FailureReason, domain},
    };
    use novarocks_worker_protocol::proposal_summary::SourceId;
    use std::num::NonZeroU16;
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    let proposal = |source| Proposal {
        conclusion: TaskConclusion::Terminated(TerminationDetail::Failed(TaskFailure::new(
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::EXECUTION, NonZeroU16::new(1).unwrap()),
            ),
            SafeDetail::truncating("failure"),
        ))),
        evidence: None,
        source: Some(SourceId(source)),
        diagnostic: None,
    };
    core.handle(
        at(0),
        ContextInput::Proposal {
            task: task(1),
            proposal: proposal(1),
        },
        &mut port,
    )
    .unwrap();
    core.handle(at(0), ContextInput::Adjudicate { task: task(1) }, &mut port)
        .unwrap();
    let version = core.task(task(1)).unwrap().version;
    core.handle(
        at(1),
        ContextInput::Proposal {
            task: task(1),
            proposal: proposal(2),
        },
        &mut port,
    )
    .unwrap();
    let keep = core
        .handle(at(1), ContextInput::Adjudicate { task: task(1) }, &mut port)
        .unwrap();
    assert_eq!(core.task(task(1)).unwrap().version, version);
    assert_eq!(
        keep.effects,
        vec![ContextEffect::DisposeSource {
            task: task(1),
            source: SourceId(2)
        }]
    );
}

#[test]
fn source_disposal_is_addressed_to_the_exact_task_when_occurrence_ordinals_match() {
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure,
        failure::{FailureClass, FailureFact, FailureReason, domain},
    };
    use novarocks_worker_protocol::proposal_summary::{ProposalSummary, SourceId};
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    let proposal = |source| Proposal {
        conclusion: TaskConclusion::Terminated(TerminationDetail::Failed(TaskFailure::new(
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(65000).unwrap()),
            ),
            SafeDetail::truncating("source routing probe"),
        ))),
        evidence: None,
        source: Some(SourceId(source)),
        diagnostic: None,
    };
    for number in [1, 2] {
        create(&mut core, &mut port, number);
        loaded(&mut core, &mut port, number);
        for occurrence in [1, 2] {
            let mut summary = ProposalSummary::default();
            summary.push(proposal(occurrence));
            let step = core
                .handle(
                    at(0),
                    ContextInput::Summary {
                        task: task(u32::from(number)),
                        generation: Generation::new(1).unwrap(),
                        cut: None,
                        summary,
                    },
                    &mut port,
                )
                .unwrap();
            let disposed: Vec<_> = step
                .effects
                .into_iter()
                .filter(|effect| matches!(effect, ContextEffect::DisposeSource { .. }))
                .collect();
            if occurrence == 1 {
                assert!(disposed.is_empty());
            } else {
                assert_eq!(
                    disposed,
                    vec![ContextEffect::DisposeSource {
                        task: task(u32::from(number)),
                        source: SourceId(2),
                    }]
                );
            }
        }
    }
}

#[test]
fn expired_create_replay_checks_the_entity_before_claiming_nothing_was_created() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    let request = |token, operation_id| RequestContext {
        token: InputId::new(token).unwrap(),
        operation: operation(operation_id),
        deadline: at(1),
    };
    let command = || ContextCommand::Create {
        task: task(1),
        payload: PayloadId(99),
    };
    let creating = core
        .handle_request(at(1), request(1, 2), command(), &mut port)
        .unwrap();
    assert_eq!(
        creating.requests[0].result,
        RequestResult::TimeoutWhileAccepted
    );
    core.handle(
        at(1),
        ContextInput::Loaded {
            task: task(1),
            result: Ok(cost()),
        },
        &mut port,
    )
    .unwrap();
    let accepted = core
        .handle_request(at(1), request(2, 3), command(), &mut port)
        .unwrap();
    assert_eq!(
        accepted.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Idempotent)
    );
    assert_eq!(port.calls.len(), 2);
}

#[test]
fn metric_deadline_coalesces_facts_without_expiring_the_application_lease_or_erasing_writer_facts()
{
    use novarocks_execution_contract::{TaskResourceFacts, TaskWriterFacts};
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    let writer = TaskWriterFacts::empty().with_written(3, 99);
    core.handle(
        at(0),
        ContextInput::Metrics {
            task: task(1),
            facts: TaskResourceFacts::empty().with_running_drivers(1),
            writer: Some(writer),
        },
        &mut port,
    )
    .unwrap();
    let pending = core
        .handle(
            at(0),
            ContextInput::Metrics {
                task: task(1),
                facts: TaskResourceFacts::empty().with_running_drivers(2),
                writer: None,
            },
            &mut port,
        )
        .unwrap();
    assert!(pending.effects.is_empty());
    let metric_deadline = MonotonicInstant::from_origin(Duration::from_millis(100));
    assert_eq!(core.next_deadline(), Some(metric_deadline));
    let tick = core
        .handle(metric_deadline, ContextInput::Tick, &mut port)
        .unwrap();
    assert_eq!(core.state(), QueryContextState::Active);
    assert!(tick.effects.iter().any(|effect| matches!(effect, ContextEffect::PublishMetrics { facts, writer: Some(value), .. } if facts.running_drivers() == Some(2) && *value == writer)));
}
#[test]
fn pending_task_close_cuts_do_not_keep_an_unhandled_metric_timer_due() {
    use novarocks_execution_contract::TaskResourceFacts;
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    for running in [1, 2] {
        core.handle(
            at(0),
            ContextInput::Metrics {
                task: task(1),
                facts: TaskResourceFacts::empty().with_running_drivers(running),
                writer: None,
            },
            &mut port,
        )
        .unwrap();
    }
    let request = RequestContext {
        token: InputId::new(77).unwrap(),
        operation: operation(8),
        deadline: at(5),
    };
    core.handle_request(
        at(0),
        request,
        ContextCommand::Abort {
            cause: AbortCause::QueryFailed,
        },
        &mut port,
    )
    .unwrap();
    let scheduled = core
        .handle(at(0), ContextInput::ContinueClose, &mut port)
        .unwrap();
    let close_generation = scheduled
        .effects
        .iter()
        .find_map(|effect| match effect {
            ContextEffect::ApplyTaskClose {
                task: target,
                generation,
            } if *target == task(1) => Some(*generation),
            _ => None,
        })
        .expect("the original close cut is scheduled but has not been consumed");
    let due = MonotonicInstant::from_origin(Duration::from_millis(100));
    assert_eq!(core.next_deadline(), Some(due));
    let tick = core.handle(due, ContextInput::Tick, &mut port).unwrap();
    assert!(
        tick.effects
            .iter()
            .any(|e| matches!(e, ContextEffect::PublishMetrics { .. }))
    );
    assert_eq!(
        core.next_deadline(),
        Some(request.deadline),
        "the handled metric timer is gone; only the actual unfinished Abort reply remains"
    );
    assert_eq!(
        core.task(task(1)).unwrap().applied_close_generation,
        Generation::new(1).unwrap()
    );
    let applied = core
        .handle(
            due,
            ContextInput::ApplyTaskClose {
                task: task(1),
                generation: close_generation,
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(applied.requests.len(), 1);
    assert_eq!(applied.requests[0].request, request);
    assert_eq!(
        applied.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    assert_eq!(
        core.task(task(1)).unwrap().applied_close_generation,
        close_generation
    );
    assert!(core.next_deadline().is_none());
}
#[test]
fn final_info_freezes_the_real_status_writer_and_operator_facts() {
    use novarocks_execution_contract::{
        OperatorStatistics, SafeDetail, TaskResourceFacts, TaskWriterFacts,
    };
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    let writer = TaskWriterFacts::empty().with_written(7, 1234);
    core.handle(
        at(0),
        ContextInput::Metrics {
            task: task(1),
            facts: TaskResourceFacts::empty().with_cpu_time(Duration::from_millis(5)),
            writer: Some(writer),
        },
        &mut port,
    )
    .unwrap();
    let statistics =
        vec![OperatorStatistics::new(1, SafeDetail::truncating("scan")).with_output_rows(7)];
    core.handle(
        at(0),
        ContextInput::OperatorStatistics {
            task: task(1),
            statistics: statistics.clone(),
            truncated: true,
        },
        &mut port,
    )
    .unwrap();
    core.apply_fixture(
        at(0),
        rpc(
            operation(2),
            ContextCommand::Cancel {
                task: task(1),
                reason: CancelReason::UpstreamNoLongerNeeded,
            },
        ),
        &mut port,
    )
    .unwrap();
    core.handle(at(0), ContextInput::Finish { task: task(1) }, &mut port)
        .unwrap();
    let final_step = core
        .apply_fixture(
            at(0),
            rpc(operation(3), ContextCommand::GetFinalInfo { task: task(1) }),
            &mut port,
        )
        .unwrap();
    let ContextEffect::FinalInfoReply { info, .. } = &final_step.effects[0] else {
        panic!("frozen final information");
    };
    assert_eq!(info.final_status(), &core.task(task(1)).unwrap().published);
    assert_eq!(info.final_status().writer(), Some(writer));
    assert_eq!(info.operator_statistics(), statistics);
    assert!(info.operator_statistics_truncated());
    assert!(!core.task(task(1)).unwrap().actual_stopped);
}
fn control() -> novarocks_worker_protocol::control::ControlCore {
    control_with_pending_limit(16)
}
fn control_with_pending_limit(
    max_pending_requests: usize,
) -> novarocks_worker_protocol::control::ControlCore {
    use novarocks_worker_protocol::control::{ControlConfig, ControlCore};
    ControlCore::new(
        ControlConfig {
            max_contexts: 8,
            max_waiters: 8,
            max_pending_requests,
            step_budget: ControlCore::MAX_STEP_WORK,
            retained_contexts: 8,
            gone_fences: 8,
            retention: Duration::from_secs(10),
        },
        AdmissionTicketConfig::DEFAULT,
        [1; 8],
        at(0),
    )
    .unwrap()
}
fn semantics() -> novarocks_worker_protocol::control::EstablishSemanticFacts {
    use novarocks_execution_contract::{ContentFingerprint, CredentialEpoch, CredentialLeaseId};
    novarocks_worker_protocol::control::EstablishSemanticFacts {
        catalog: ContentFingerprint::from_bytes([1; 16]),
        shared_filter: ContentFingerprint::from_bytes([2; 16]),
        query_options: ContentFingerprint::from_bytes([3; 16]),
        credential_lease: CredentialLeaseId::new(1),
        credential_epoch: CredentialEpoch::new(1).unwrap(),
        initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
    }
}
fn grant(
    control: &mut novarocks_worker_protocol::control::ControlCore,
    port: &mut Port,
) -> AdmissionTicketId {
    use novarocks_worker_protocol::control::ControlInput;
    let ticket = AdmissionTicketId::try_from_bytes([4; 16]).unwrap();
    let request = AcquireQueryContextAdmissionTicket::new(
        operation(1),
        context(),
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        control.admission().capability(),
    );
    control
        .handle(at(0), ControlInput::Acquire { request, ticket }, port)
        .unwrap();
    ticket
}

fn another_control_context() -> QueryContextRef {
    QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(7301, 7302), AttemptId::new(1).unwrap()).unwrap(),
        context().frontend_process_id(),
        context().backend_process_id(),
    )
}
fn active_establish_input(
    ticket: AdmissionTicketId,
    payload: u64,
) -> novarocks_worker_protocol::control::ControlInput {
    novarocks_worker_protocol::control::ControlInput::Establish {
        request: operation(2),
        context: context(),
        ticket,
        payload: PayloadId(payload),
        semantics: semantics(),
        generation: Generation::new(1).unwrap(),
    }
}

fn routed_establish_request(
    step: &novarocks_worker_protocol::control::ControlStep,
) -> (
    QueryContextRef,
    novarocks_worker_protocol::context::EstablishReplayRequest,
) {
    let routes: Vec<_> = step
        .effects
        .iter()
        .filter_map(|effect| match effect {
            novarocks_worker_protocol::control::ControlEffect::RouteEstablish {
                context,
                request,
            } => Some((*context, *request)),
            _ => None,
        })
        .collect();
    assert_eq!(
        routes.len(),
        1,
        "one actual neutral route for this original R"
    );
    assert!(step.charged_work() <= novarocks_worker_protocol::control::ControlCore::MAX_STEP_WORK);
    assert!(step.requests.is_empty());
    routes[0]
}
fn finish_routed_establish(
    control: &mut novarocks_worker_protocol::control::ControlCore,
    port: &mut Port,
    route: &novarocks_worker_protocol::control::ControlStep,
    now: u64,
    decided_at: u64,
    result: Result<OperationOutcome, novarocks_worker_protocol::context::DomainRefusal>,
) -> novarocks_worker_protocol::control::ControlStep {
    let (context, request) = routed_establish_request(route);
    let step = control
        .handle(
            at(now),
            novarocks_worker_protocol::control::ControlInput::RoutedEstablishFinished {
                context,
                request: request.request,
                result,
                decided_at: at(decided_at),
            },
            port,
        )
        .unwrap();
    assert!(step.charged_work() <= control.max_step_work());
    step
}

struct ActiveControlFixture {
    control: novarocks_worker_protocol::control::ControlCore,
    port: Port,
    ticket: AdmissionTicketId,
}
impl ActiveControlFixture {
    fn new(max_pending_requests: usize, deadline: u64) -> Self {
        let mut control = control_with_pending_limit(max_pending_requests);
        let mut port = Port::default();
        let ticket = grant(&mut control, &mut port);
        let first = control
            .handle_request(
                at(0),
                RequestContext {
                    token: InputId::new(1).unwrap(),
                    operation: operation(2),
                    deadline: at(deadline),
                },
                active_establish_input(ticket, 1),
                &mut port,
            )
            .unwrap();
        assert!(first.charged_work() <= control.max_step_work());
        assert!(first.requests.is_empty());
        assert_eq!(port.calls.len(), 2);
        Self {
            control,
            port,
            ticket,
        }
    }
    fn request(
        &mut self,
        token: u64,
        deadline: u64,
        input: novarocks_worker_protocol::control::ControlInput,
    ) -> novarocks_worker_protocol::control::ControlStep {
        let step = self
            .control
            .handle_request(
                at(0),
                RequestContext {
                    token: InputId::new(token).unwrap(),
                    operation: operation(2),
                    deadline: at(deadline),
                },
                input,
                &mut self.port,
            )
            .unwrap();
        assert!(step.charged_work() <= self.control.max_step_work());
        step
    }
    fn reject(
        &mut self,
        token: u64,
        deadline: u64,
        input: novarocks_worker_protocol::control::ControlInput,
        outcome: OperationOutcome,
    ) {
        let calls = self.port.calls.len();
        let refused = self.request(token, deadline, input);
        assert!(
            refused.effects.is_empty(),
            "a conflict cannot act on the original operation"
        );
        assert_eq!(refused.requests.len(), 1);
        assert_eq!(
            refused.requests[0].request.token,
            InputId::new(token).unwrap()
        );
        assert_eq!(refused.requests[0].result, RequestResult::Outcome(outcome));
        assert_eq!(self.port.calls.len(), calls);
    }
    fn materialized(&mut self) -> novarocks_worker_protocol::control::ControlStep {
        let step = self
            .control
            .handle(
                at(0),
                novarocks_worker_protocol::control::ControlInput::EstablishmentDecided {
                    context: context(),
                    decision: active_establishment_decision(),
                },
                &mut self.port,
            )
            .unwrap();
        assert!(step.charged_work() <= self.control.max_step_work());
        step
    }
    fn flush(&mut self) -> novarocks_worker_protocol::control::ControlStep {
        let step = self
            .control
            .handle(
                at(0),
                novarocks_worker_protocol::control::ControlInput::ContinueRequestReplies {
                    operation: operation(2),
                },
                &mut self.port,
            )
            .unwrap();
        assert!(step.charged_work() <= self.control.max_step_work());
        step
    }
    fn finish_original(&mut self) {
        let completed = self.materialized();
        assert_eq!(completed.requests.len(), 1);
        assert_eq!(
            completed.requests[0].request.token,
            InputId::new(1).unwrap()
        );
        assert_eq!(
            completed.requests[0].result,
            RequestResult::Outcome(OperationOutcome::Accepted)
        );
        assert!(self.flush().requests.is_empty());
        assert_eq!(self.port.calls.len(), 2);
    }
}

#[test]
fn root_routes_existing_establish_fields_without_interpreting_entity_equality() {
    use novarocks_worker_protocol::control::ControlInput;
    for changed in 0..8 {
        let mut f = ActiveControlFixture::new(16, 100);
        let mut candidate = active_establish_input(f.ticket, 2);
        let ControlInput::Establish {
            ticket, semantics, ..
        } = &mut candidate
        else {
            unreachable!()
        };
        match changed {
            0 => *ticket = AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
            1 => {
                semantics.catalog =
                    novarocks_execution_contract::ContentFingerprint::from_bytes([9; 16])
            }
            2 => {
                semantics.shared_filter =
                    novarocks_execution_contract::ContentFingerprint::from_bytes([9; 16])
            }
            3 => {
                semantics.query_options =
                    novarocks_execution_contract::ContentFingerprint::from_bytes([9; 16])
            }
            4 => {
                semantics.credential_lease = novarocks_execution_contract::CredentialLeaseId::new(2)
            }
            5 => {
                semantics.credential_epoch =
                    novarocks_execution_contract::CredentialEpoch::new(2).unwrap()
            }
            6 => {
                semantics.initial_lease_valid_for =
                    LeaseValidFor::new(Duration::from_secs(29)).unwrap()
            }
            7 => {}
            _ => unreachable!(),
        }
        let ControlInput::Establish {
            ticket: expected_ticket,
            semantics: expected_semantics,
            ..
        } = &candidate
        else {
            unreachable!()
        };
        let expected_ticket = *expected_ticket;
        let expected_semantics = *expected_semantics;
        let routed = f.request(2, 100, candidate);
        let (target, actual) = routed_establish_request(&routed);
        assert_eq!(actual.ticket, expected_ticket);
        assert_eq!(actual.semantics, expected_semantics);
        assert_eq!(target, context());
        assert_eq!(actual.payload, PayloadId(2));
        let completed = finish_routed_establish(
            &mut f.control,
            &mut f.port,
            &routed,
            0,
            0,
            Ok(if changed == 7 {
                OperationOutcome::Idempotent
            } else {
                OperationOutcome::ContextConflict
            }),
        );
        assert_eq!(
            completed.requests[0].request.token,
            InputId::new(2).unwrap()
        );
        assert_eq!(
            completed.requests[0].result,
            RequestResult::Outcome(if changed == 7 {
                OperationOutcome::Idempotent
            } else {
                OperationOutcome::ContextConflict
            })
        );
        f.finish_original();
    }
}

#[test]
fn an_absent_other_context_cannot_reuse_a_live_creation_operation() {
    use novarocks_worker_protocol::control::ControlInput;
    let mut f = ActiveControlFixture::new(8, 100);
    let mut candidate = active_establish_input(f.ticket, 2);
    let ControlInput::Establish {
        context: target, ..
    } = &mut candidate
    else {
        unreachable!()
    };
    *target = another_control_context();
    f.reject(2, 100, candidate, OperationOutcome::ContextConflict);
    assert_eq!(
        f.control.route(another_control_context()),
        QueryContextState::Absent
    );
    f.finish_original();
}

#[test]
fn active_establish_cannot_be_joined_by_an_acquire_of_the_same_operation() {
    use novarocks_worker_protocol::control::ControlInput;
    let mut f = ActiveControlFixture::new(16, 100);
    let request = AcquireQueryContextAdmissionTicket::new(
        operation(2),
        another_control_context(),
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        f.control.admission().capability(),
    );
    f.reject(
        2,
        100,
        ControlInput::Acquire {
            request,
            ticket: AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
        },
        OperationOutcome::DomainConflict,
    );
    f.finish_original();
}

#[test]
fn same_operation_routed_requests_have_independent_results_and_settle_once() {
    let mut f = ActiveControlFixture::new(16, 100);
    let second = f.request(2, 80, active_establish_input(f.ticket, 20));
    let third = f.request(3, 90, active_establish_input(f.ticket, 30));
    assert_eq!(routed_establish_request(&second).1.payload, PayloadId(20));
    assert_eq!(routed_establish_request(&third).1.payload, PayloadId(30));
    let first = f.materialized();
    assert_eq!(first.requests.len(), 1);
    assert_eq!(first.requests[0].request.token, InputId::new(1).unwrap());
    assert_eq!(
        first.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    for (route, token, outcome) in [
        (third, 3, OperationOutcome::ContextConflict),
        (second, 2, OperationOutcome::Idempotent),
    ] {
        let completed =
            finish_routed_establish(&mut f.control, &mut f.port, &route, 0, 0, Ok(outcome));
        assert_eq!(completed.requests.len(), 1);
        assert_eq!(
            completed.requests[0].request.token,
            InputId::new(token).unwrap()
        );
        assert_eq!(
            completed.requests[0].result,
            RequestResult::Outcome(outcome)
        );
        assert!(
            finish_routed_establish(&mut f.control, &mut f.port, &route, 0, 0, Ok(outcome))
                .requests
                .is_empty()
        );
    }
    assert!(f.flush().requests.is_empty());
    assert_eq!(f.port.calls.len(), 2);
}

#[test]
fn a_different_operation_routes_its_own_payload_instead_of_joining_the_directory_body() {
    use novarocks_worker_protocol::control::ControlInput;
    let mut f = ActiveControlFixture::new(16, 100);
    let mut candidate = active_establish_input(f.ticket, 30);
    let ControlInput::Establish { request, .. } = &mut candidate else {
        unreachable!()
    };
    *request = operation(3);
    let routed = f
        .control
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(2).unwrap(),
                operation: operation(3),
                deadline: at(100),
            },
            candidate,
            &mut f.port,
        )
        .unwrap();
    assert_eq!(routed_establish_request(&routed).1.payload, PayloadId(30));
    let completed = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &routed,
        0,
        0,
        Ok(OperationOutcome::ContextConflict),
    );
    assert_eq!(completed.requests[0].request.operation, operation(3));
    assert_eq!(
        completed.requests[0].result,
        RequestResult::Outcome(OperationOutcome::ContextConflict)
    );
    f.finish_original();
}

#[test]
fn an_expired_existing_request_neither_routes_nor_replays_another_requests_body() {
    let mut f = ActiveControlFixture::new(16, 100);
    let timed_out = f.request(2, 0, active_establish_input(f.ticket, 2));
    assert_eq!(timed_out.requests.len(), 1);
    assert_eq!(
        timed_out.requests[0].request.token,
        InputId::new(2).unwrap()
    );
    assert_eq!(
        timed_out.requests[0].result,
        RequestResult::TimeoutWhileAccepted
    );
    assert!(timed_out.effects.is_empty());
    f.finish_original();
}

#[test]
fn a_routed_conflict_does_not_take_or_replace_the_original_creation_success() {
    let mut f = ActiveControlFixture::new(16, 100);
    let route = f.request(2, 100, active_establish_input(f.ticket, 2));
    let first = f.materialized();
    assert_eq!(first.requests.len(), 1);
    assert_eq!(first.requests[0].request.token, InputId::new(1).unwrap());
    assert_eq!(
        first.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    let conflict = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        0,
        0,
        Ok(OperationOutcome::ContextConflict),
    );
    assert_eq!(conflict.requests.len(), 1);
    assert_eq!(conflict.requests[0].request.token, InputId::new(2).unwrap());
    assert_eq!(
        conflict.requests[0].result,
        RequestResult::Outcome(OperationOutcome::ContextConflict)
    );
    assert!(f.flush().requests.is_empty());
    assert_eq!(
        first.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
}

#[test]
fn existing_routes_share_the_original_pending_request_bound() {
    let mut f = ActiveControlFixture::new(1, 100);
    f.reject(
        2,
        100,
        active_establish_input(f.ticket, 2),
        OperationOutcome::ResourceExhausted,
    );
    f.finish_original();
    let route = f.request(3, 100, active_establish_input(f.ticket, 3));
    assert_eq!(
        routed_establish_request(&route).1.request.token,
        InputId::new(3).unwrap()
    );
    f.reject(
        4,
        100,
        active_establish_input(f.ticket, 4),
        OperationOutcome::ResourceExhausted,
    );
    let completed = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        0,
        0,
        Ok(OperationOutcome::Idempotent),
    );
    assert_eq!(
        completed.requests[0].request.token,
        InputId::new(3).unwrap()
    );
    let next = f.request(5, 100, active_establish_input(f.ticket, 5));
    assert_eq!(
        routed_establish_request(&next).1.request.token,
        InputId::new(5).unwrap()
    );
    assert_eq!(f.port.calls.len(), 2);
}

#[test]
fn active_control_shape_dies_with_the_last_waiter_without_revoking_materialization() {
    use novarocks_worker_protocol::control::ControlInput;
    let mut f = ActiveControlFixture::new(1, 1);
    let expired = f
        .control
        .handle(at(1), ControlInput::Tick, &mut f.port)
        .unwrap();
    assert_eq!(expired.requests.len(), 1);
    assert_eq!(
        expired.requests[0].result,
        RequestResult::TimeoutWhileAccepted
    );
    assert_eq!(f.control.route(context()), QueryContextState::Establishing);
    let request = AcquireQueryContextAdmissionTicket::new(
        operation(2),
        another_control_context(),
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        f.control.admission().capability(),
    );
    let acquired = f
        .control
        .handle_request(
            at(1),
            RequestContext {
                token: InputId::new(2).unwrap(),
                operation: operation(2),
                deadline: at(100),
            },
            ControlInput::Acquire {
                request,
                ticket: AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
            },
            &mut f.port,
        )
        .unwrap();
    assert_eq!(acquired.requests.len(), 1);
    assert_eq!(
        acquired.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    assert_eq!(
        f.port.calls.len(),
        3,
        "no stale active shape is a permanent decision latch"
    );
    let completed = f
        .control
        .handle(
            at(1),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision: active_establishment_decision(),
            },
            &mut f.port,
        )
        .unwrap();
    assert!(
        completed.requests.is_empty(),
        "the expired original waiter stays retired"
    );
}

#[test]
fn acquire_replay_uses_the_existing_identity_author_and_not_new_driver_coordinates() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut control = control();
    let mut port = Port::default();
    let original = AcquireQueryContextAdmissionTicket::new(
        operation(1),
        context(),
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        control.admission().capability(),
    );
    let mut receipt = None;
    for (token, new_ticket, deadline) in [(1, 4, 100), (2, 9, 80)] {
        let capability = control.admission().capability();
        if token == 2 {
            assert_ne!(capability, original.admission_epoch_capability());
        }
        let request = AcquireQueryContextAdmissionTicket::new(
            operation(1),
            context(),
            original.valid_for(),
            original.native_compatibility_id(),
            capability,
        );
        let step = control
            .handle_request(
                at(0),
                RequestContext {
                    token: InputId::new(token).unwrap(),
                    operation: operation(1),
                    deadline: at(deadline),
                },
                ControlInput::Acquire {
                    request,
                    ticket: AdmissionTicketId::try_from_bytes([new_ticket; 16]).unwrap(),
                },
                &mut port,
            )
            .unwrap();
        assert!(step.charged_work() <= control.max_step_work());
        let actual = step
            .effects
            .iter()
            .find_map(|effect| match effect {
                ControlEffect::AdmissionReply {
                    result: Ok(actual), ..
                } => Some(*actual),
                _ => None,
            })
            .unwrap();
        if let Some(original_receipt) = receipt {
            assert_eq!(actual, original_receipt);
        }
        receipt = Some(actual);
        assert_eq!(step.requests.len(), 1);
        assert_eq!(step.requests[0].request.token, InputId::new(token).unwrap());
    }
    for changed in 0..3 {
        let request = AcquireQueryContextAdmissionTicket::new(
            operation(1),
            if changed == 0 {
                another_control_context()
            } else {
                context()
            },
            LeaseValidFor::new(Duration::from_secs(if changed == 1 { 4 } else { 5 })).unwrap(),
            NativeCompatibilityId::new([if changed == 2 { 9 } else { 3 }; 32]),
            control.admission().capability(),
        );
        let step = control
            .handle_request(
                at(0),
                RequestContext {
                    token: InputId::new(3 + changed).unwrap(),
                    operation: operation(1),
                    deadline: at(100),
                },
                ControlInput::Acquire {
                    request,
                    ticket: AdmissionTicketId::try_from_bytes([10; 16]).unwrap(),
                },
                &mut port,
            )
            .unwrap();
        assert_eq!(
            step.requests[0].result,
            RequestResult::Outcome(OperationOutcome::DomainConflict)
        );
    }
    assert_eq!(port.calls.len(), 1);
}

#[test]
fn a_new_establish_with_an_unknown_ticket_keeps_the_original_gone_verdict() {
    use novarocks_worker_protocol::control::ControlInput;
    let mut control = control();
    let mut port = Port::default();
    let step = control
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(1).unwrap(),
                operation: operation(2),
                deadline: at(100),
            },
            active_establish_input(AdmissionTicketId::try_from_bytes([9; 16]).unwrap(), 1),
            &mut port,
        )
        .unwrap();
    assert!(step.charged_work() <= control.max_step_work());
    assert_eq!(step.requests.len(), 1);
    assert_eq!(
        step.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Gone)
    );
    assert!(!step.effects.iter().any(|effect| matches!(
        effect,
        novarocks_worker_protocol::control::ControlEffect::Materialize { .. }
    )));
    assert_eq!(control.route(context()), QueryContextState::Absent);
    assert!(port.calls.is_empty());
    let no_waiter = control
        .handle(
            at(0),
            ControlInput::ContinueRequestReplies {
                operation: operation(2),
            },
            &mut port,
        )
        .unwrap();
    assert!(no_waiter.requests.is_empty());
}

fn establishment_decision(step: &ContextStep) -> (Generation, ContextEstablishmentDecision) {
    assert!(step.charged_work() <= ContextCore::MAX_STEP_WORK);
    let decisions: Vec<_> = step
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ContextEffect::EstablishmentDecided {
                context: identity,
                close_epoch,
                decision,
            } => {
                assert_eq!(*identity, context());
                Some((*close_epoch, decision.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(decisions.len(), 1, "one decision for the actual input");
    decisions[0].clone()
}

fn active_establishment_decision() -> ContextEstablishmentDecision {
    ContextEstablishmentDecision {
        state: QueryContextState::Active,
        outcome: Ok(OperationOutcome::Accepted),
    }
}
fn closed_establishment_decision(
    state: QueryContextState,
    outcome: OperationOutcome,
) -> ContextEstablishmentDecision {
    ContextEstablishmentDecision {
        state,
        outcome: Ok(outcome),
    }
}

#[test]
fn materialize_preserves_the_complete_original_establishment_for_the_context_core() {
    use novarocks_execution_contract::{ContentFingerprint, CredentialEpoch, CredentialLeaseId};
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut control = control();
    let mut port = Port::default();
    let ticket = selector_grant(&mut control, &mut port, context(), 71, 5);
    let original = ContextEstablishment {
        operation: operation(72),
        ticket,
        payload: PayloadId(901),
        semantics: novarocks_worker_protocol::control::EstablishSemanticFacts {
            catalog: ContentFingerprint::from_bytes([41; 16]),
            shared_filter: ContentFingerprint::from_bytes([42; 16]),
            query_options: ContentFingerprint::from_bytes([43; 16]),
            credential_lease: CredentialLeaseId::new(44),
            credential_epoch: CredentialEpoch::new(45).unwrap(),
            initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(20)).unwrap(),
        },
        elected_at: at(2),
    };
    let elected = control
        .handle(
            at(2),
            ControlInput::Establish {
                request: original.operation,
                context: context(),
                ticket: original.ticket,
                payload: original.payload,
                semantics: original.semantics,
                generation: Generation::new(1).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    assert!(elected.charged_work() <= control.max_step_work());
    let materializations: Vec<_> = elected
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ControlEffect::Materialize {
                context,
                establishment,
                generation,
                ..
            } => Some((*context, *establishment, *generation)),
            _ => None,
        })
        .collect();
    assert_eq!(
        materializations,
        vec![(context(), original, Generation::new(1).unwrap())]
    );
    let (identity, establishment, generation) = materializations[0];
    let mut context_core = core_with_establishment(establishment, 1, 1);
    assert_eq!(context_core.identity(), identity);
    assert_eq!(context_core.generation(), generation);
    assert_eq!(context_core.establishment(), original);
    assert_eq!(context_core.next_deadline(), Some(at(22)));
    assert_eq!(
        context_core
            .handle(at(1), ContextInput::Tick, &mut port)
            .unwrap_err(),
        novarocks_worker_protocol::context::CoreError::TimeRegressed
    );
    let finished = context_core
        .handle(
            at(4),
            ContextInput::MaterializationFinished {
                result: Ok(ContextMaterialized {
                    initial_credential_resource: 101,
                }),
            },
            &mut port,
        )
        .unwrap();
    assert!(finished.charged_work() <= context_core.max_step_work());
    assert_eq!(context_core.state(), QueryContextState::Active);
    assert_eq!(context_core.establishment(), original);
}

#[test]
fn matching_context_and_generation_do_not_replace_the_original_establish_header() {
    use novarocks_execution_contract::{ContentFingerprint, CredentialEpoch, CredentialLeaseId};
    let original = ContextEstablishment {
        operation: operation(2),
        ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
        payload: PayloadId(1),
        semantics: semantics(),
        elected_at: at(0),
    };
    let original_core = core_with_establishment(original, 1, 1);
    // Each original header field remains independently meaningful, even when
    // the routing identity and protocol generation happen to be identical.
    for changed in 0..10 {
        let mut candidate = original;
        match changed {
            0 => candidate.operation = operation(3),
            1 => candidate.ticket = AdmissionTicketId::try_from_bytes([5; 16]).unwrap(),
            2 => candidate.payload = PayloadId(2),
            3 => candidate.semantics.catalog = ContentFingerprint::from_bytes([9; 16]),
            4 => candidate.semantics.shared_filter = ContentFingerprint::from_bytes([9; 16]),
            5 => candidate.semantics.query_options = ContentFingerprint::from_bytes([9; 16]),
            6 => candidate.semantics.credential_lease = CredentialLeaseId::new(2),
            7 => candidate.semantics.credential_epoch = CredentialEpoch::new(2).unwrap(),
            8 => {
                candidate.semantics.initial_lease_valid_for =
                    LeaseValidFor::new(Duration::from_secs(31)).unwrap();
            }
            9 => candidate.elected_at = at(1),
            _ => unreachable!(),
        }
        assert_ne!(candidate, original);
        let candidate_core = core_with_establishment(candidate, 1, 1);
        assert_eq!(candidate_core.identity(), original_core.identity());
        assert_eq!(candidate_core.generation(), original_core.generation());
        assert_eq!(candidate_core.establishment(), candidate);
        assert_ne!(
            candidate_core.establishment(),
            original_core.establishment()
        );
        assert_eq!(original_core.establishment(), original);
    }
}

#[test]
fn materialize_carries_the_winning_step_time_without_resetting_the_initial_lease() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut control = control();
    let mut port = Port::default();
    let ticket = grant(&mut control, &mut port);
    let elected = control
        .handle(
            at(2),
            ControlInput::Establish {
                request: operation(2),
                context: context(),
                ticket,
                payload: PayloadId(1),
                semantics: semantics(),
                generation: Generation::new(1).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    assert!(elected.charged_work() <= control.max_step_work());
    let establishment = elected
        .effects
        .iter()
        .find_map(|effect| match effect {
            ControlEffect::Materialize { establishment, .. } => Some(*establishment),
            _ => None,
        })
        .expect("the actual elected Materialize effect");
    assert_eq!(establishment.elected_at, at(2));
    // The first observed driver input can be delayed; construction has no
    // independent clock or lease argument that could move the origin.
    let mut context_core = core_with_establishment(establishment, 1, 1);
    let delayed = context_core
        .handle(at(19), ContextInput::Tick, &mut port)
        .unwrap();
    assert!(delayed.charged_work() <= context_core.max_step_work());
    assert_eq!(context_core.next_deadline(), Some(at(32)));
    let established = context_core
        .handle(
            at(31),
            ContextInput::MaterializationFinished {
                result: Ok(ContextMaterialized {
                    initial_credential_resource: 101,
                }),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(
        establishment_decision(&established),
        (Generation::new(1).unwrap(), active_establishment_decision())
    );
    assert_eq!(context_core.next_deadline(), Some(at(32)));
    let duplicate = context_core
        .handle(
            at(31),
            ContextInput::MaterializationFinished {
                result: Ok(ContextMaterialized {
                    initial_credential_resource: 101,
                }),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(
        establishment_decision(&duplicate),
        establishment_decision(&established)
    );
    assert_eq!(context_core.next_deadline(), Some(at(32)));
    let expired = context_core
        .handle(
            at(32),
            ContextInput::MaterializationFinished {
                result: Ok(ContextMaterialized {
                    initial_credential_resource: 101,
                }),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(
        establishment_decision(&expired),
        (
            Generation::new(2).unwrap(),
            closed_establishment_decision(
                QueryContextState::Aborting,
                OperationOutcome::LeaseExpired
            )
        )
    );
    assert_eq!(context_core.state(), QueryContextState::Aborting);
    assert!(
        expired
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::CloseExternalAdmission { .. }))
    );
    assert!(!expired.effects.iter().any(|effect| matches!(
        effect,
        ContextEffect::EstablishmentDecided {
            decision: ContextEstablishmentDecision {
                state: QueryContextState::Active,
                ..
            },
            ..
        }
    )));
}

#[test]
fn a_late_first_establishment_reports_expiry_and_cannot_reopen_on_repetition() {
    let mut context_core = core_with_initial_lease(1, 1, at(2));
    let mut port = Port::default();
    for now in [at(32), at(33)] {
        let closed = context_core
            .handle(
                now,
                ContextInput::MaterializationFinished {
                    result: Ok(ContextMaterialized {
                        initial_credential_resource: 101,
                    }),
                },
                &mut port,
            )
            .unwrap();
        assert_eq!(
            establishment_decision(&closed),
            (
                Generation::new(2).unwrap(),
                closed_establishment_decision(
                    QueryContextState::Aborting,
                    OperationOutcome::LeaseExpired
                )
            )
        );
        assert_eq!(context_core.state(), QueryContextState::Aborting);
    }
    assert!(port.calls.is_empty());
}

#[test]
fn an_abort_before_establishment_keeps_its_original_closed_outcome() {
    let mut context_core = core(1, 1);
    let mut port = Port::default();
    let aborted = context_core
        .apply_fixture(
            at(1),
            rpc(
                operation(2),
                ContextCommand::Abort {
                    cause: AbortCause::QueryFailed,
                },
            ),
            &mut port,
        )
        .unwrap();
    assert!(aborted.charged_work() <= ContextCore::MAX_STEP_WORK);
    for now in [at(1), at(31)] {
        let closed = context_core
            .handle(
                now,
                ContextInput::MaterializationFinished {
                    result: Ok(ContextMaterialized {
                        initial_credential_resource: 101,
                    }),
                },
                &mut port,
            )
            .unwrap();
        assert_eq!(
            establishment_decision(&closed),
            (
                Generation::new(2).unwrap(),
                closed_establishment_decision(
                    QueryContextState::Aborting,
                    OperationOutcome::ContextTerminalReceipt
                )
            )
        );
        assert_eq!(context_core.state(), QueryContextState::Aborting);
    }
}

#[test]
fn quiesce_during_materialization_fences_late_establishment_and_settles_each_actual_token() {
    let mut context_core = core(1, 1);
    let mut port = Port::default();
    let quiesce = context_core
        .apply_fixture(at(1), rpc(operation(2), ContextCommand::Quiesce), &mut port)
        .unwrap();
    let first_token = port.request_token(operation(2));
    assert!(quiesce.charged_work() <= ContextCore::MAX_STEP_WORK);
    assert_eq!(context_core.state(), QueryContextState::Quiescing);
    assert!(
        quiesce
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::CloseExternalAdmission { .. }))
    );
    assert!(
        quiesce
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::ContinueQuiesce))
    );
    assert!(quiesce.requests.is_empty());
    let waiting = context_core
        .apply_fixture(at(1), rpc(operation(2), ContextCommand::Quiesce), &mut port)
        .unwrap();
    let second_token = port.request_token(operation(2));
    assert_ne!(first_token, second_token);
    assert!(waiting.charged_work() <= ContextCore::MAX_STEP_WORK);
    assert!(waiting.requests.is_empty());
    assert!(
        !waiting
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::QuiesceReply { .. }))
    );
    for now in [at(1), at(2)] {
        let late = context_core
            .handle(
                now,
                ContextInput::MaterializationFinished {
                    result: Ok(ContextMaterialized {
                        initial_credential_resource: 101,
                    }),
                },
                &mut port,
            )
            .unwrap();
        assert_eq!(
            establishment_decision(&late),
            (
                Generation::new(1).unwrap(),
                closed_establishment_decision(
                    QueryContextState::Quiescing,
                    OperationOutcome::ContextTerminalReceipt
                )
            )
        );
        assert!(
            !late
                .effects
                .iter()
                .any(|effect| matches!(effect, ContextEffect::ReleaseHost { .. }))
        );
    }
    // One entity scan runs once; each continuation settles at most one actual request.
    let completed = context_core
        .handle(at(2), ContextInput::ContinueQuiesce, &mut port)
        .unwrap();
    assert!(completed.charged_work() <= ContextCore::MAX_STEP_WORK);
    assert_eq!(completed.requests.len(), 1);
    assert_eq!(completed.requests[0].request.token, first_token);
    assert_eq!(
        completed.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    let first_receipt = completed
        .effects
        .iter()
        .find_map(|effect| match effect {
            ContextEffect::QuiesceReply {
                request, receipt, ..
            } if *request == first_token => Some(receipt.clone()),
            _ => None,
        })
        .expect("the first request retains its complete neutral fence receipt");
    let idempotent = context_core
        .handle(at(2), ContextInput::ContinueQuiesce, &mut port)
        .unwrap();
    assert!(idempotent.charged_work() <= ContextCore::MAX_STEP_WORK);
    assert_eq!(idempotent.requests.len(), 1);
    assert_eq!(idempotent.requests[0].request.token, second_token);
    assert_eq!(
        idempotent.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Idempotent)
    );
    let second_receipt = idempotent
        .effects
        .iter()
        .find_map(|effect| match effect {
            ContextEffect::QuiesceReply {
                request, receipt, ..
            } if *request == second_token => Some(receipt.clone()),
            _ => None,
        })
        .expect("a pending second token needs no host RPC replay");
    assert!(std::sync::Arc::ptr_eq(&first_receipt, &second_receipt));
    assert_eq!(first_receipt.state(), QueryContextState::Quiescing);
    assert_eq!(completed.requests[0].request.operation, operation(2));
    assert_eq!(idempotent.requests[0].request.operation, operation(2));
    for step in [&quiesce, &waiting, &completed, &idempotent] {
        assert!(
            !step
                .effects
                .iter()
                .any(|effect| matches!(effect, ContextEffect::ReleaseHost { .. }))
        );
    }
    assert_eq!(context_core.state(), QueryContextState::Quiescing);
    assert!(port.calls.is_empty());
}

#[test]
fn a_same_time_frontend_fence_defeats_either_context_establishment_delivery_order() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    for close_first in [false, true] {
        let mut control = control();
        let mut context_core = core(1, 1);
        let mut port = Port::default();
        let ticket = grant(&mut control, &mut port);
        control
            .handle(
                at(0),
                ControlInput::Establish {
                    request: operation(2),
                    context: context(),
                    ticket,
                    payload: PayloadId(1),
                    semantics: semantics(),
                    generation: Generation::new(1).unwrap(),
                },
                &mut port,
            )
            .unwrap();
        control
            .handle(
                at(1),
                ControlInput::FenceFrontend {
                    frontend: context().frontend_process_id(),
                },
                &mut port,
            )
            .unwrap();
        let fence = control
            .handle(at(1), ControlInput::ContinueFence, &mut port)
            .unwrap();
        assert!(fence.charged_work() <= control.max_step_work());
        let close = || {
            rpc(
                operation(3),
                ContextCommand::Abort {
                    cause: AbortCause::QueryFailed,
                },
            )
        };
        if close_first {
            context_core
                .apply_fixture(at(1), close(), &mut port)
                .unwrap();
        }
        let decided = context_core
            .handle(
                at(1),
                ContextInput::MaterializationFinished {
                    result: Ok(ContextMaterialized {
                        initial_credential_resource: 101,
                    }),
                },
                &mut port,
            )
            .unwrap();
        let (_, verdict) = establishment_decision(&decided);
        assert_eq!(
            verdict,
            if close_first {
                closed_establishment_decision(
                    QueryContextState::Aborting,
                    OperationOutcome::ContextTerminalReceipt,
                )
            } else {
                active_establishment_decision()
            }
        );
        if !close_first {
            context_core
                .apply_fixture(at(1), close(), &mut port)
                .unwrap();
        }

        let returned = control
            .handle(
                at(1),
                ControlInput::EstablishmentDecided {
                    context: context(),
                    decision: verdict,
                },
                &mut port,
            )
            .unwrap();
        assert!(returned.charged_work() <= control.max_step_work());
        assert_eq!(context_core.state(), QueryContextState::Aborting);
        assert_eq!(control.route(context()), QueryContextState::Aborting);
        assert!(!returned.effects.iter().any(|effect| matches!(
            effect,
            ControlEffect::Route {
                state: QueryContextState::Active,
                ..
            }
        )));
        assert!(returned.effects.iter().any(|effect| matches!(
            effect,
            ControlEffect::Reply {
                outcome: OperationOutcome::ContextTerminalReceipt,
                ..
            }
        )));
    }
}

#[test]
fn establish_reply_expiry_does_not_undo_the_redeemed_ticket_or_context_materialization() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut control = control();
    let mut port = Port::default();
    let ticket = grant(&mut control, &mut port);
    control
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(1).unwrap(),
                operation: operation(2),
                deadline: at(1),
            },
            ControlInput::Establish {
                request: operation(2),
                context: context(),
                ticket,
                payload: PayloadId(1),
                semantics: semantics(),
                generation: Generation::new(1).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    let expired = control
        .handle(at(1), ControlInput::Tick, &mut port)
        .unwrap();
    assert_eq!(
        expired.requests[0].result,
        RequestResult::TimeoutWhileAccepted
    );
    assert_eq!(control.route(context()), QueryContextState::Establishing);
    assert!(
        !expired
            .effects
            .iter()
            .any(|effect| matches!(effect, ControlEffect::RetireReservation(_)))
    );
    let materialized = control
        .handle(
            at(2),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision: active_establishment_decision(),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(control.route(context()), QueryContextState::Active);
    assert!(materialized.requests.is_empty());
    assert_eq!(port.calls.len(), 2);
}
#[test]
fn frontend_fence_blocks_late_materialization_and_new_entity_routing() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut f = ActiveControlFixture::new(8, 100);
    let route = f.request(2, 100, active_establish_input(f.ticket, 2));
    routed_establish_request(&route);
    f.control
        .handle(
            at(0),
            ControlInput::FenceFrontend {
                frontend: context().frontend_process_id(),
            },
            &mut f.port,
        )
        .unwrap();
    f.control
        .handle(at(0), ControlInput::ContinueFence, &mut f.port)
        .unwrap();
    assert_eq!(f.control.route(context()), QueryContextState::Aborting);
    let late = f.materialized();
    assert_eq!(f.control.route(context()), QueryContextState::Aborting);
    assert!(late.effects.iter().all(|effect| !matches!(
        effect,
        ControlEffect::Route {
            state: QueryContextState::Active,
            ..
        }
    )));
    f.reject(
        3,
        100,
        active_establish_input(f.ticket, 3),
        OperationOutcome::Gone,
    );
    let completed = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        0,
        0,
        Ok(OperationOutcome::InvalidStateOrRequest),
    );
    assert_eq!(
        completed.requests[0].request.token,
        InputId::new(2).unwrap()
    );
    assert_eq!(
        completed.requests[0].result,
        RequestResult::Outcome(OperationOutcome::InvalidStateOrRequest)
    );
    assert_eq!(f.port.calls.len(), 2);
}
#[test]
fn covered_reconnect_positions_validate_convergence_and_skip_known_context_receipt() {
    use novarocks_execution_contract::{
        QueryContextConvergenceCursor, QueryContextConvergenceReceipt,
        QueryContextConvergenceState, QueryContextConvergenceVersion, TaskConvergenceCursor,
        TaskConvergenceReceipt, TaskConvergenceVersion,
    };
    use novarocks_worker_protocol::observation::CoveredSubscriptionRequest;
    let mut source = ObservationSource::new(context(), 8, 2, 1024, 8192);
    let convergence =
        TaskConvergenceReceipt::actual_stopped(task(1), TaskConvergenceVersion::FIRST);
    let context_convergence = QueryContextConvergenceReceipt::new(
        context(),
        QueryContextConvergenceVersion::FIRST,
        QueryContextConvergenceState::WorkerStoppedAndContextFenced,
    );
    for fact in [
        CoveredObservationFact::Status(status(1)),
        CoveredObservationFact::TaskConvergence(convergence),
        CoveredObservationFact::ContextConvergence(context_convergence),
    ] {
        source.publish(fact, 100).unwrap();
        assert!(source.continue_publication(1).complete);
    }
    let id = SubscriptionId(1);
    source
        .begin_covered_subscription(
            id,
            1,
            CoveredSubscriptionRequest {
                status: vec![TaskStatusCursor::at(task(1), status(1).version())],
                convergence: vec![TaskConvergenceCursor::at(
                    task(1),
                    TaskConvergenceVersion::FIRST,
                )],
                context: Some(QueryContextConvergenceCursor::at(
                    context(),
                    QueryContextConvergenceVersion::FIRST,
                )),
                ..CoveredSubscriptionRequest::default()
            },
        )
        .unwrap();
    while !source.continue_subscribe(id, 1).unwrap().complete {}
    let mut facts = Vec::new();
    loop {
        let next = source.select_next(id).unwrap().unwrap();
        let complete = matches!(next.frame, CoveredObservationFrame::CatchUpComplete { .. });
        if let CoveredObservationFrame::CatchUp(fact) = &next.frame {
            facts.push(fact.clone());
        }
        source.note_delivered(id, next.delivery_id).unwrap();
        if complete {
            break;
        }
    }
    assert_eq!(
        facts,
        vec![
            CoveredObservationFact::StatusUnchanged(task(1)),
            CoveredObservationFact::TaskConvergenceUnchanged(task(1))
        ]
    );
    assert_eq!(source.bookmark(id).unwrap().covered_prefix, 3);
    let future = SubscriptionId(2);
    source
        .begin_covered_subscription(
            future,
            1,
            CoveredSubscriptionRequest {
                convergence: vec![TaskConvergenceCursor::at(
                    task(1),
                    TaskConvergenceVersion::new(2).unwrap(),
                )],
                ..CoveredSubscriptionRequest::default()
            },
        )
        .unwrap();
    assert_eq!(
        source.continue_subscribe(future, 1),
        Err(CoveredSubscriptionError::FutureTaskConvergenceCursor(task(
            1
        )))
    );
    assert!(
        source.bookmark(future).is_none(),
        "failed initialization cannot expose a partial snapshot"
    );
}

#[test]
fn retiring_subscriber_keeps_its_bounded_storage_slot_until_incremental_cleanup_exits() {
    let mut source = ObservationSource::new(context(), 8, 1, 1024, 8192);
    source
        .publish(CoveredObservationFact::Status(status(1)), 100)
        .unwrap();
    assert!(source.continue_publication(1).complete);
    let id = SubscriptionId(1);
    source
        .begin_subscribe(id, 1, vec![TaskStatusCursor::unobserved(task(1))])
        .unwrap();
    assert!(!source.continue_subscribe(id, 1).unwrap().complete);
    assert!(source.unsubscribe(id));
    assert!(source.bookmark(id).is_none());
    assert_eq!(
        source.begin_subscribe(SubscriptionId(2), 2, vec![]),
        Err(CoveredSubscriptionError::TargetLimitExceeded)
    );
    loop {
        let progress = source.continue_unsubscribe(id, 1).unwrap();
        assert_eq!(progress.work, 1);
        if progress.complete {
            break;
        }
    }
    source
        .begin_subscribe(SubscriptionId(2), 2, vec![])
        .unwrap();
}

#[test]
fn task_close_cuts_do_not_overtake_old_summaries_or_serialize_other_tasks() {
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure,
        failure::{FailureClass, FailureEvidence, FailureFact, FailureReason, domain},
    };
    use novarocks_worker_protocol::proposal_summary::{ProposalSummary, SourceId};
    use std::num::NonZeroU16;
    for refine_peer in [false, true] {
        let mut core = core(8, 2);
        let mut port = Port::default();
        activate(&mut core, &mut port);
        for n in [1, 2] {
            create(&mut core, &mut port, n);
            loaded(&mut core, &mut port, n);
            core.handle(
                at(0),
                ContextInput::Installed {
                    task: task(n.into()),
                },
                &mut port,
            )
            .unwrap();
        }
        let first = if refine_peer {
            TaskConclusion::Terminated(TerminationDetail::Aborted(AbortCause::PeerTaskFailed))
        } else {
            TaskConclusion::Terminated(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            ))
        };
        let mut preceding = ProposalSummary::default();
        preceding.push(proposal(first.clone()));
        // A's preceding segment was already admitted by its driver, but has
        // not reached the core yet. The context close only schedules its cut.
        let begin = core
            .apply_fixture(
                at(0),
                rpc(
                    operation(5),
                    ContextCommand::Abort {
                        cause: AbortCause::QueryFailed,
                    },
                ),
                &mut port,
            )
            .unwrap();
        assert!(!begin.effects.iter().any(|effect| matches!(effect, ContextEffect::Reply { request, .. } if *request == port.request_token(operation(5)))));
        let a = core
            .handle(at(0), ContextInput::ContinueClose, &mut port)
            .unwrap();
        assert!(a.effects.contains(&ContextEffect::ApplyTaskClose {
            task: task(1),
            generation: Generation::new(2).unwrap()
        }));
        assert_eq!(
            core.task(task(1)).unwrap().applied_close_generation.get(),
            1
        );
        let b = core.handle(at(0), ContextInput::Tick, &mut port).unwrap();
        assert!(b.effects.contains(&ContextEffect::ApplyTaskClose {
            task: task(2),
            generation: Generation::new(2).unwrap()
        }));
        let another_abort = core
            .apply_fixture(
                at(0),
                rpc(
                    operation(6),
                    ContextCommand::Abort {
                        cause: AbortCause::QueryFailed,
                    },
                ),
                &mut port,
            )
            .unwrap();
        assert!(
            !another_abort
                .effects
                .iter()
                .any(|effect| matches!(effect, ContextEffect::Reply { .. }))
        );
        let b_applied = core
            .handle(
                at(0),
                ContextInput::ApplyTaskClose {
                    task: task(2),
                    generation: Generation::new(2).unwrap(),
                },
                &mut port,
            )
            .unwrap();
        assert_eq!(
            core.task(task(2)).unwrap().applied_close_generation.get(),
            2
        );
        assert!(matches!(
            core.task(task(2)).unwrap().conclusion.accepted,
            Some(TaskConclusion::Terminated(TerminationDetail::Aborted(
                AbortCause::QueryFailed
            )))
        ));
        assert!(
            !b_applied
                .effects
                .iter()
                .any(|effect| matches!(effect, ContextEffect::Reply { .. }))
        );
        assert_eq!(
            core.task(task(1)).unwrap().applied_close_generation.get(),
            1
        );
        // Core consumption, rather than source/capture destruction, is the
        // predecessor required before the close action can apply.
        core.handle(
            at(0),
            ContextInput::Summary {
                task: task(1),
                generation: Generation::new(1).unwrap(),
                cut: None,
                summary: preceding,
            },
            &mut port,
        )
        .unwrap();
        assert_eq!(
            core.task(task(1)).unwrap().conclusion.accepted,
            Some(first.clone())
        );
        let a_applied = core
            .handle(
                at(0),
                ContextInput::ApplyTaskClose {
                    task: task(1),
                    generation: Generation::new(2).unwrap(),
                },
                &mut port,
            )
            .unwrap();
        assert_eq!(core.task(task(1)).unwrap().conclusion.accepted, Some(first));
        assert!(a_applied.effects.contains(&ContextEffect::Reply {
            request: port.request_token(operation(5)),
            outcome: OperationOutcome::Accepted
        }));
        assert!(!core.task(task(1)).unwrap().loader_exited);
        assert!(!core.task(task(1)).unwrap().actual_stopped);
        let replay = core
            .handle(at(0), ContextInput::ContinueClose, &mut port)
            .unwrap();
        assert!(replay.effects.contains(&ContextEffect::Reply {
            request: port.request_token(operation(6)),
            outcome: OperationOutcome::Idempotent
        }));
        let failure = TaskFailure::new(
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::EXECUTION, NonZeroU16::new(1).unwrap()),
            ),
            SafeDetail::truncating("origin failed"),
        );
        let mut later = ProposalSummary::default();
        later.push(Proposal {
            conclusion: TaskConclusion::Terminated(TerminationDetail::Failed(failure.clone())),
            evidence: Some(FailureEvidence::Originating),
            source: Some(SourceId(11)),
            diagnostic: None,
        });
        core.handle(
            at(0),
            ContextInput::Summary {
                task: task(1),
                generation: Generation::new(2).unwrap(),
                cut: None,
                summary: later,
            },
            &mut port,
        )
        .unwrap();
        if refine_peer {
            assert_eq!(
                core.task(task(1)).unwrap().conclusion.accepted,
                Some(TaskConclusion::Terminated(TerminationDetail::Failed(
                    failure
                )))
            );
        } else {
            assert!(matches!(
                core.task(task(1)).unwrap().conclusion.accepted,
                Some(TaskConclusion::Terminated(TerminationDetail::Canceled(_)))
            ));
        }
    }
}
#[test]
fn a_preparation_failure_before_its_ordered_close_is_not_preempted_by_context_abort() {
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure, TaskFailurePhase,
        failure::{FailureClass, FailureFact, FailureReason, domain},
    };
    use novarocks_worker_protocol::proposal_summary::SourceId;
    use std::num::NonZeroU16;
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    let fact = FailureFact::new(
        FailureClass::Data,
        FailureReason::new(domain::EXECUTION, NonZeroU16::new(1).unwrap()),
    );
    let projection = |phase| Proposal {
        conclusion: TaskConclusion::Terminated(TerminationDetail::Failed(
            TaskFailure::new_in_phase(fact, SafeDetail::truncating("prepare failed"), phase),
        )),
        evidence: None,
        source: Some(SourceId(12)),
        diagnostic: None,
    };
    core.apply_fixture(
        at(0),
        rpc(
            operation(5),
            ContextCommand::Abort {
                cause: AbortCause::QueryFailed,
            },
        ),
        &mut port,
    )
    .unwrap();
    core.handle(at(0), ContextInput::ContinueClose, &mut port)
        .unwrap();
    core.handle(
        at(0),
        ContextInput::PreparationBodyFailure {
            task: task(1),
            preparing: projection(TaskFailurePhase::Preparation),
            executing: projection(TaskFailurePhase::Execution),
        },
        &mut port,
    )
    .unwrap();
    assert_eq!(
        core.task(task(1)).unwrap().applied_close_generation.get(),
        1
    );
    core.handle(
        at(0),
        ContextInput::ApplyTaskClose {
            task: task(1),
            generation: Generation::new(2).unwrap(),
        },
        &mut port,
    )
    .unwrap();
    assert!(
        matches!(core.task(task(1)).unwrap().conclusion.accepted, Some(TaskConclusion::Terminated(TerminationDetail::Failed(ref failure))) if failure.phase() == TaskFailurePhase::Preparation)
    );
}
#[test]
fn an_applied_close_waits_for_core_consumption_but_not_the_failure_sources_actual_drop() {
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure,
        failure::{FailureClass, FailureFact, FailureReason, domain},
    };
    use novarocks_worker_protocol::proposal_summary::{ProposalSummary, SourceId};
    use std::num::NonZeroU16;
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    let mut preceding = ProposalSummary::default();
    preceding.push(Proposal {
        conclusion: TaskConclusion::Terminated(TerminationDetail::Failed(TaskFailure::new(
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::EXECUTION, NonZeroU16::new(1).unwrap()),
            ),
            SafeDetail::truncating("driver failed"),
        ))),
        evidence: None,
        source: Some(SourceId(17)),
        diagnostic: None,
    });
    core.apply_fixture(
        at(0),
        rpc(
            operation(5),
            ContextCommand::Abort {
                cause: AbortCause::QueryFailed,
            },
        ),
        &mut port,
    )
    .unwrap();
    core.handle(at(0), ContextInput::ContinueClose, &mut port)
        .unwrap();
    core.handle(
        at(0),
        ContextInput::Summary {
            task: task(1),
            generation: Generation::new(1).unwrap(),
            cut: None,
            summary: preceding,
        },
        &mut port,
    )
    .unwrap();
    let closed = core
        .handle(
            at(0),
            ContextInput::ApplyTaskClose {
                task: task(1),
                generation: Generation::new(2).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(
        core.task(task(1)).unwrap().conclusion.source,
        Some(SourceId(17))
    );
    assert!(!closed.effects.contains(&ContextEffect::DisposeSource {
        task: task(1),
        source: SourceId(17)
    }));
    assert!(closed.effects.contains(&ContextEffect::Reply {
        request: port.request_token(operation(5)),
        outcome: OperationOutcome::Accepted
    }));
    assert!(!core.task(task(1)).unwrap().actual_stopped);
    assert!(!core.task(task(1)).unwrap().resources_converged);
}
#[test]
fn expired_repeated_abort_reply_roles_do_not_leave_ghost_waiters_behind_a_slow_task_cut() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    core.handle_request(
        at(0),
        RequestContext {
            token: InputId::new(1).unwrap(),
            operation: operation(4),
            deadline: at(1000),
        },
        ContextCommand::Abort {
            cause: AbortCause::QueryFailed,
        },
        &mut port,
    )
    .unwrap();
    core.handle(at(0), ContextInput::ContinueClose, &mut port)
        .unwrap();
    for n in 0..71u8 {
        let now = u64::from(n) + 1;
        let op = operation(n + 10);
        let token = InputId::new(u64::from(n) + 2).unwrap();
        let admitted = core
            .handle_request(
                at(now),
                RequestContext {
                    token,
                    operation: op,
                    deadline: at(now + 1),
                },
                ContextCommand::Abort {
                    cause: AbortCause::QueryFailed,
                },
                &mut port,
            )
            .unwrap();
        assert!(
            admitted.requests.is_empty(),
            "live RPC admission owns the waiter; expired predecessors cannot exhaust it"
        );
        assert!(
            !admitted
                .effects
                .iter()
                .any(|effect| matches!(effect, ContextEffect::Reply { .. }))
        );
        let timeout = core
            .handle(at(now + 1), ContextInput::Tick, &mut port)
            .unwrap();
        assert_eq!(timeout.requests.len(), 1);
        assert_eq!(timeout.requests[0].request.token, token);
        assert_eq!(
            timeout.requests[0].result,
            RequestResult::TimeoutWhileAccepted
        );
        assert_eq!(
            core.task(task(1)).unwrap().applied_close_generation.get(),
            1
        );
    }
    let live = core
        .handle_request(
            at(100),
            RequestContext {
                token: InputId::new(100).unwrap(),
                operation: operation(200),
                deadline: at(1000),
            },
            ContextCommand::Abort {
                cause: AbortCause::QueryFailed,
            },
            &mut port,
        )
        .unwrap();
    assert!(live.requests.is_empty());
    let applied = core
        .handle(
            at(100),
            ContextInput::ApplyTaskClose {
                task: task(1),
                generation: Generation::new(2).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(applied.requests.len(), 1);
    assert_eq!(applied.requests[0].request.operation, operation(4));
    assert_eq!(
        applied.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    assert!(!core.task(task(1)).unwrap().actual_stopped);
    let remaining = core
        .handle(at(100), ContextInput::ContinueClose, &mut port)
        .unwrap();
    assert_eq!(remaining.requests.len(), 1);
    assert_eq!(remaining.requests[0].request.operation, operation(200));
    assert_eq!(
        remaining.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Idempotent)
    );
    assert!(
        core.handle(at(100), ContextInput::ContinueClose, &mut port)
            .unwrap()
            .effects
            .is_empty()
    );
}
#[test]
fn expired_creation_reply_positions_do_not_fill_the_waiter_bound_or_cancel_the_loader() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    core.handle_request(
        at(0),
        RequestContext {
            token: InputId::new(1).unwrap(),
            operation: operation(1),
            deadline: at(1),
        },
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(1),
        },
        &mut port,
    )
    .unwrap();
    core.handle(at(1), ContextInput::Tick, &mut port).unwrap();
    for n in 0..13u8 {
        let now = u64::from(n) + 1;
        let op = operation(n + 10);
        let admitted = core
            .handle_request(
                at(now),
                RequestContext {
                    token: InputId::new(u64::from(n) + 2).unwrap(),
                    operation: op,
                    deadline: at(now + 1),
                },
                ContextCommand::Create {
                    task: task(1),
                    payload: PayloadId(99),
                },
                &mut port,
            )
            .unwrap();
        assert!(admitted.requests.is_empty());
        core.handle(at(now + 1), ContextInput::Tick, &mut port)
            .unwrap();
        assert!(core.task(task(1)).unwrap().waiting_replies.is_empty());
        assert!(!core.task(task(1)).unwrap().loader_exited);
    }
    core.handle_request(
        at(20),
        RequestContext {
            token: InputId::new(100).unwrap(),
            operation: operation(50),
            deadline: at(25),
        },
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(99),
        },
        &mut port,
    )
    .unwrap();
    let loaded = core
        .handle(
            at(20),
            ContextInput::Loaded {
                task: task(1),
                result: Ok(cost()),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(loaded.requests.len(), 1);
    assert_eq!(loaded.requests[0].request.operation, operation(50));
    assert_eq!(
        loaded.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    assert_eq!(core.task(task(1)).unwrap().payload, PayloadId(1));
    assert!(!core.task(task(1)).unwrap().loader_exited);
    assert_eq!(
        port.calls.len(),
        2,
        "one original loader and one real preparation reservation"
    );
}
#[test]
fn expired_establish_routes_do_not_fill_the_request_bound_or_revoke_materialization() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut f = ActiveControlFixture::new(2, 1);
    let first = f
        .control
        .handle(at(1), ControlInput::Tick, &mut f.port)
        .unwrap();
    assert_eq!(first.requests[0].request.token, InputId::new(1).unwrap());
    for n in 0..13u64 {
        let now = n + 1;
        let routed = f
            .control
            .handle_request(
                at(now),
                RequestContext {
                    token: InputId::new(n + 2).unwrap(),
                    operation: operation(2),
                    deadline: at(now + 1),
                },
                active_establish_input(f.ticket, n + 99),
                &mut f.port,
            )
            .unwrap();
        let original = routed_establish_request(&routed).1.request;
        let timed = f
            .control
            .handle(at(now + 1), ControlInput::Tick, &mut f.port)
            .unwrap();
        assert_eq!(timed.requests[0].request, original);
        assert_eq!(
            timed.requests[0].result,
            RequestResult::TimeoutWhileAccepted
        );
        assert!(timed.effects.iter().any(|effect|matches!(effect,ControlEffect::CancelEstablishRoute{request,..} if *request==original)));
        assert_eq!(f.control.route(context()), QueryContextState::Establishing);
    }
    let route = f
        .control
        .handle_request(
            at(20),
            RequestContext {
                token: InputId::new(100).unwrap(),
                operation: operation(2),
                deadline: at(100),
            },
            active_establish_input(f.ticket, 100),
            &mut f.port,
        )
        .unwrap();
    let materialized = f
        .control
        .handle(
            at(20),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision: active_establishment_decision(),
            },
            &mut f.port,
        )
        .unwrap();
    assert!(
        materialized.requests.is_empty(),
        "the expired creator is not a live routed request"
    );
    let reply = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        20,
        20,
        Ok(OperationOutcome::Idempotent),
    );
    assert_eq!(reply.requests[0].request.token, InputId::new(100).unwrap());
    assert_eq!(
        reply.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Idempotent)
    );
    assert_eq!(f.control.route(context()), QueryContextState::Active);
    assert_eq!(f.port.calls.len(), 2);
}
#[test]
fn a_creation_wait_position_survives_until_its_last_real_reply_token_exits() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    for (token, deadline) in [(1, 1), (2, 10)] {
        core.handle_request(
            at(0),
            RequestContext {
                token: InputId::new(token).unwrap(),
                operation: operation(1),
                deadline: at(deadline),
            },
            ContextCommand::Create {
                task: task(1),
                payload: PayloadId(1),
            },
            &mut port,
        )
        .unwrap();
    }
    let expired = core.handle(at(1), ContextInput::Tick, &mut port).unwrap();
    assert_eq!(expired.requests[0].request.token, InputId::new(1).unwrap());
    assert!(
        core.task(task(1))
            .unwrap()
            .waiting_replies
            .contains(&InputId::new(2).unwrap())
    );
    let ready = core
        .handle(
            at(2),
            ContextInput::Loaded {
                task: task(1),
                result: Ok(cost()),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(ready.requests.len(), 1);
    assert_eq!(ready.requests[0].request.token, InputId::new(2).unwrap());
    assert_eq!(
        ready.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    assert!(core.task(task(1)).unwrap().waiting_replies.is_empty());
    assert_eq!(port.calls.len(), 2);
}
fn inbound_refusal() -> AdmissionRefusal {
    use novarocks_execution_contract::{
        OperationDomainFailure, SafeDetail,
        failure::{
            FailureClass, FailureFact, FailureReason, ResourceDimension, ResourceFacts,
            ResourceScope, domain,
        },
        operation::DomainFailureOutcome,
    };
    let fact = FailureFact::new(
        FailureClass::Resource,
        FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(73).unwrap()),
    )
    .with_resource(ResourceFacts {
        request_scope: Some(ResourceScope::Task),
        constraint_scope: Some(ResourceScope::Process),
        dimension: ResourceDimension::Bytes,
        requested: Some(9),
        required_growth: Some(9),
        limit: Some(10),
        live: None,
        committed: Some(8),
    });
    AdmissionRefusal::Domain(
        DomainFailureOutcome::ResourceExhausted,
        OperationDomainFailure::new(
            fact,
            SafeDetail::truncating("inbound normal-close byte capacity exhausted"),
        ),
    )
}
#[test]
fn inbound_typed_admission_refusal_survives_loader_exit_and_exact_entity_replay() {
    use novarocks_worker_protocol::context::DomainRefusal;
    let refusal = inbound_refusal();
    let AdmissionRefusal::Domain(outcome, failure) = refusal.clone() else {
        unreachable!();
    };
    let expected = DomainRefusal { outcome, failure };
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    core.handle_request(
        at(0),
        RequestContext {
            token: InputId::new(1).unwrap(),
            operation: operation(1),
            deadline: at(100),
        },
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(1),
        },
        &mut port,
    )
    .unwrap();
    port.typed_reject = Some(refusal.clone());
    let rejected = core
        .handle(
            at(1),
            ContextInput::Loaded {
                task: task(1),
                result: Ok(cost()),
            },
            &mut port,
        )
        .unwrap();
    assert!(
        rejected
            .effects
            .contains(&ContextEffect::DomainFailureReply {
                request: InputId::new(1).unwrap(),
                refusal: expected.clone()
            })
    );
    assert_eq!(
        rejected.requests[0].result,
        RequestResult::Outcome(outcome.outcome())
    );
    assert!(!rejected.effects.iter().any(|effect| matches!(
        effect,
        ContextEffect::EnqueuePreparation { .. }
            | ContextEffect::RetireTask { .. }
            | ContextEffect::PublishTask { .. }
            | ContextEffect::PublishTaskConvergence { .. }
    )));
    assert!(!core.task(task(1)).unwrap().loader_exited);
    assert!(
        core.task(task(1))
            .unwrap()
            .preparation_reservation
            .is_none()
    );
    assert_eq!(core.task(task(1)).unwrap().creation_refusal, Some(refusal));
    assert!(core.status(task(1)).is_none());
    let exited = core
        .handle(
            at(2),
            ContextInput::Physical {
                task: task(1),
                fact: PhysicalFact::LoaderExited,
            },
            &mut port,
        )
        .unwrap();
    assert!(exited.effects.iter().any(|effect| matches!(
        effect,
        ContextEffect::RetireTask {
            preparation: None,
            ..
        }
    )));
    assert_eq!(
        core.task(task(1)).unwrap().phase,
        novarocks_worker_protocol::task::CreationPhase::Retained
    );
    port.typed_reject = None;
    for (token, op, deadline) in [(2, 2, 100), (3, 3, 1)] {
        let replay = core
            .handle_request(
                at(3),
                RequestContext {
                    token: InputId::new(token).unwrap(),
                    operation: operation(op),
                    deadline: at(deadline),
                },
                ContextCommand::Create {
                    task: task(1),
                    payload: PayloadId(99),
                },
                &mut port,
            )
            .unwrap();
        assert_eq!(replay.requests.len(), 1);
        assert_eq!(
            replay.requests[0].request.token,
            InputId::new(token).unwrap()
        );
        assert_eq!(replay.requests[0].request.operation, operation(op));
        assert_eq!(replay.requests[0].task, Some(task(1)));
        assert!(matches!(replay.requests[0].body.as_ref(),
            novarocks_worker_protocol::context::ContextReplyBody::Domain(actual) if actual == &expected));
        if deadline <= 3 {
            assert!(
                replay.effects.is_empty(),
                "an expired unadmitted request publishes no host effect"
            );
        } else {
            assert!(replay.effects.contains(&ContextEffect::DomainFailureReply {
                request: InputId::new(token).unwrap(),
                refusal: expected.clone()
            }));
        }
        assert_eq!(
            replay.requests[0].result,
            RequestResult::Outcome(outcome.outcome())
        );
    }
    assert_eq!(
        port.calls.len(),
        2,
        "a retained refusal never starts preparation or re-reserves capacity on replay"
    );
}
#[test]
fn typed_loader_failure_and_active_task_capacity_failure_preserve_their_domain_body() {
    let refusal = inbound_refusal();
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    port.typed_reject = Some(refusal.clone());
    let first = core
        .apply_fixture(
            at(0),
            rpc(
                operation(1),
                ContextCommand::Create {
                    task: task(1),
                    payload: PayloadId(1),
                },
            ),
            &mut port,
        )
        .unwrap();
    assert!(
        matches!(&first.effects[0], ContextEffect::DomainFailureReply { refusal: value, .. } if value.outcome.outcome() == refusal.outcome())
    );
    assert!(
        core.task(task(1)).is_none(),
        "a failed active reservation cannot create an uncharged entity record"
    );
    port.typed_reject = None;
    create(&mut core, &mut port, 1);
    let loaded = core
        .handle(
            at(0),
            ContextInput::Loaded {
                task: task(1),
                result: Err(refusal.clone()),
            },
            &mut port,
        )
        .unwrap();
    assert!(matches!(
        &loaded.effects[0],
        ContextEffect::DomainFailureReply { .. }
    ));
    assert_eq!(core.task(task(1)).unwrap().creation_refusal, Some(refusal));
    assert_eq!(
        port.calls.len(),
        2,
        "loader failure never attempts a preparation reservation"
    );
}
#[test]
fn typed_ticket_capacity_refusal_is_replayed_without_losing_its_failure_facts() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput, ControlReplyBody};
    let refusal = inbound_refusal();
    let mut control = control();
    let mut port = Port {
        typed_reject: Some(refusal.clone()),
        ..Port::default()
    };
    let request = AcquireQueryContextAdmissionTicket::new(
        operation(1),
        context(),
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        control.admission().capability(),
    );
    let ticket = AdmissionTicketId::try_from_bytes([4; 16]).unwrap();
    for token in 1..=2 {
        let step = control
            .handle_request(
                at(0),
                RequestContext {
                    token: InputId::new(token).unwrap(),
                    operation: operation(1),
                    deadline: at(100),
                },
                ControlInput::Acquire { request, ticket },
                &mut port,
            )
            .unwrap();
        assert!(
            matches!(&step.effects[0], ControlEffect::DomainFailureReply { refusal: value, .. } if value.outcome.outcome() == refusal.outcome() && Some(&value.failure) == match &refusal { AdmissionRefusal::Domain(_, failure) => Some(failure), _ => None })
        );
        assert_eq!(
            step.requests[0].result,
            RequestResult::Outcome(refusal.outcome())
        );
        let AdmissionRefusal::Domain(outcome, failure) = &refusal else {
            unreachable!()
        };
        assert_eq!(
            step.requests[0].body,
            ControlReplyBody::Domain(novarocks_worker_protocol::context::DomainRefusal {
                outcome: *outcome,
                failure: failure.clone()
            })
        );
        assert_eq!(step.requests[0].request.token, InputId::new(token).unwrap());
        port.typed_reject = None;
    }
    assert_eq!(port.calls.len(), 1);
}
#[test]
fn every_control_reply_carries_its_original_body_when_a_later_decision_reuses_the_operation() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput, ControlReplyBody};
    let mut control = control();
    let mut port = Port::default();
    let request = AcquireQueryContextAdmissionTicket::new(
        operation(1),
        context(),
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        control.admission().capability(),
    );
    let first = control
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(1).unwrap(),
                operation: operation(1),
                deadline: at(100),
            },
            ControlInput::Acquire {
                request,
                ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    let receipt = first
        .effects
        .iter()
        .find_map(|effect| match effect {
            ControlEffect::AdmissionReply {
                result: Ok(receipt),
                ..
            } => Some(*receipt),
            _ => None,
        })
        .unwrap();
    // Keep the earlier output unconsumed while the Core decides a conflicting
    // later input. Neither host order nor a new operation cache is an oracle.
    let conflicting = AcquireQueryContextAdmissionTicket::new(
        operation(1),
        context(),
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([9; 32]),
        request.admission_epoch_capability(),
    );
    let next = control
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(2).unwrap(),
                operation: operation(1),
                deadline: at(100),
            },
            ControlInput::Acquire {
                request: conflicting,
                ticket: AdmissionTicketId::try_from_bytes([5; 16]).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(
        first.requests[0].body,
        ControlReplyBody::Admission(Ok(receipt))
    );
    assert_eq!(
        next.requests[0].body,
        ControlReplyBody::Admission(Err(OperationOutcome::DomainConflict))
    );
    assert!(first.requests[0].body.matches(first.requests[0].result));
    assert!(next.requests[0].body.matches(next.requests[0].result));
    assert_eq!(port.calls.len(), 1);
}
#[test]
fn a_single_task_byte_refusal_keeps_precedence_over_a_full_preparation_position() {
    let mut core = core(8, 1);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    create(&mut core, &mut port, 2);
    let mut oversized = cost();
    oversized.input_retained_bytes = 101;
    let rejected = core
        .handle(
            at(0),
            ContextInput::Loaded {
                task: task(2),
                result: Ok(oversized),
            },
            &mut port,
        )
        .unwrap();
    assert!(rejected.effects.contains(&ContextEffect::Reply {
        request: port.request_token(operation(2)),
        outcome: OperationOutcome::ResourceExhausted
    }));
    create(&mut core, &mut port, 3);
    let full = loaded(&mut core, &mut port, 3);
    assert!(full.contains(&ContextEffect::Reply {
        request: port.request_token(operation(3)),
        outcome: OperationOutcome::PreparationBusy
    }));
}
#[test]
fn typed_context_materialization_capacity_refusal_keeps_its_failure_projection() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let refusal = inbound_refusal();
    let mut control = control();
    let mut port = Port::default();
    let ticket = grant(&mut control, &mut port);
    port.typed_reject = Some(refusal.clone());
    let step = control
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(1).unwrap(),
                operation: operation(2),
                deadline: at(100),
            },
            ControlInput::Establish {
                request: operation(2),
                context: context(),
                ticket,
                payload: PayloadId(1),
                semantics: semantics(),
                generation: Generation::new(1).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    assert!(
        matches!(&step.effects[0], ControlEffect::DomainFailureReply { refusal: value, .. } if value.outcome.outcome() == refusal.outcome() && Some(&value.failure) == match &refusal { AdmissionRefusal::Domain(_, failure) => Some(failure), _ => None })
    );
    assert_eq!(
        step.requests[0].result,
        RequestResult::Outcome(refusal.outcome())
    );
    assert!(!step.effects.iter().any(|effect| matches!(
        effect,
        ControlEffect::Materialize { .. } | ControlEffect::RetireReservation(_)
    )));
    assert_eq!(control.route(context()), QueryContextState::Absent);
}
#[test]
fn aggregate_preparation_capacity_refusal_remains_preparation_busy() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    port.reject = Some(OperationOutcome::PreparationBusy);
    let step = core
        .handle(
            at(0),
            ContextInput::Loaded {
                task: task(1),
                result: Ok(cost()),
            },
            &mut port,
        )
        .unwrap();
    assert!(step.effects.contains(&ContextEffect::Reply {
        request: port.request_token(operation(1)),
        outcome: OperationOutcome::PreparationBusy
    }));
    assert!(!step.effects.iter().any(|effect| matches!(
        effect,
        ContextEffect::DomainFailureReply { .. } | ContextEffect::EnqueuePreparation { .. }
    )));
    assert!(!core.task(task(1)).unwrap().loader_exited);
    assert_eq!(port.calls.len(), 2);
}
#[test]
fn independent_creation_reply_tokens_follow_runtime_consumption_order_not_numeric_order() {
    use novarocks_worker_protocol::context::CoreError;
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    // The earlier issued role 10 belongs to another source. Runtime fairness
    // may consume role 20 first; neither token has been retired or reused.
    for (token, op, number) in [(20, 1, 1), (10, 2, 2)] {
        let step = core
            .handle_request(
                at(0),
                RequestContext {
                    token: InputId::new(token).unwrap(),
                    operation: operation(op),
                    deadline: at(100),
                },
                ContextCommand::Create {
                    task: task(number),
                    payload: PayloadId(u64::from(number)),
                },
                &mut port,
            )
            .unwrap();
        assert!(step.requests.is_empty());
    }
    let duplicate = core
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(20).unwrap(),
                operation: operation(3),
                deadline: at(100),
            },
            ContextCommand::Create {
                task: task(3),
                payload: PayloadId(3),
            },
            &mut port,
        )
        .unwrap_err();
    assert_eq!(duplicate, CoreError::WrongIdentity);
    let independent = core
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(30).unwrap(),
                operation: operation(4),
                deadline: at(100),
            },
            ContextCommand::Create {
                task: task(3),
                payload: PayloadId(3),
            },
            &mut port,
        )
        .unwrap();
    assert!(independent.requests.is_empty());
    assert!(core.task(task(3)).is_some());
    assert_eq!(
        port.calls.len(),
        3,
        "a new actual token may create another entity; only the duplicate was refused"
    );
    let mut replies = Vec::new();
    for number in [1, 2, 3] {
        replies.extend(
            core.handle(
                at(0),
                ContextInput::Loaded {
                    task: task(number),
                    result: Ok(cost()),
                },
                &mut port,
            )
            .unwrap()
            .requests,
        );
    }
    assert_eq!(replies.len(), 3);
    assert_eq!(replies[0].request.token, InputId::new(20).unwrap());
    assert_eq!(replies[1].request.token, InputId::new(10).unwrap());
    assert_eq!(replies[2].request.token, InputId::new(30).unwrap());
    assert_eq!(replies[2].request.operation, operation(4));
    assert_eq!(
        replies[2].result,
        RequestResult::Outcome(OperationOutcome::PreparationBusy),
        "the independent third loader reaches the original two-position preparation limit"
    );
    assert!(
        replies
            .iter()
            .take(2)
            .all(|reply| reply.result == RequestResult::Outcome(OperationOutcome::Accepted))
    );
    assert!(
        core.handle(at(0), ContextInput::Tick, &mut port)
            .unwrap()
            .requests
            .is_empty()
    );
}
#[test]
fn independent_establish_reply_tokens_accept_numeric_reordering_and_reject_live_duplicates() {
    use novarocks_worker_protocol::{context::CoreError, control::ControlInput};
    let mut control = control();
    let mut port = Port::default();
    let second = QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(7201, 7202), AttemptId::new(1).unwrap()).unwrap(),
        context().frontend_process_id(),
        context().backend_process_id(),
    );
    let ticket_a = AdmissionTicketId::try_from_bytes([4; 16]).unwrap();
    let ticket_b = AdmissionTicketId::try_from_bytes([5; 16]).unwrap();
    for (token, op, identity, ticket) in [(1, 1, context(), ticket_a), (2, 2, second, ticket_b)] {
        let request = AcquireQueryContextAdmissionTicket::new(
            operation(op),
            identity,
            LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
            NativeCompatibilityId::new([3; 32]),
            control.admission().capability(),
        );
        control
            .handle_request(
                at(0),
                RequestContext {
                    token: InputId::new(token).unwrap(),
                    operation: operation(op),
                    deadline: at(100),
                },
                ControlInput::Acquire { request, ticket },
                &mut port,
            )
            .unwrap();
    }
    let establish = |op, identity, ticket, payload| ControlInput::Establish {
        request: operation(op),
        context: identity,
        ticket,
        payload: PayloadId(payload),
        semantics: semantics(),
        generation: Generation::new(1).unwrap(),
    };
    for (token, op, identity, ticket, payload) in [
        (20, 3, context(), ticket_a, 1),
        (10, 4, second, ticket_b, 2),
    ] {
        let step = control
            .handle_request(
                at(0),
                RequestContext {
                    token: InputId::new(token).unwrap(),
                    operation: operation(op),
                    deadline: at(100),
                },
                establish(op, identity, ticket, payload),
                &mut port,
            )
            .unwrap();
        assert!(step.requests.is_empty());
    }
    let duplicate = control
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(20).unwrap(),
                operation: operation(5),
                deadline: at(100),
            },
            establish(5, second, ticket_b, 2),
            &mut port,
        )
        .unwrap_err();
    assert_eq!(duplicate, CoreError::WrongIdentity);
    let mismatch = control
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(30).unwrap(),
                operation: operation(6),
                deadline: at(100),
            },
            establish(7, second, ticket_b, 2),
            &mut port,
        )
        .unwrap_err();
    assert_eq!(mismatch, CoreError::WrongIdentity);
    assert_eq!(
        port.calls.len(),
        4,
        "two distinct actual context roles were admitted"
    );
    let mut replies = Vec::new();
    for identity in [context(), second] {
        replies.extend(
            control
                .handle(
                    at(0),
                    ControlInput::EstablishmentDecided {
                        context: identity,
                        decision: active_establishment_decision(),
                    },
                    &mut port,
                )
                .unwrap()
                .requests,
        );
    }
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0].request.token, InputId::new(20).unwrap());
    assert_eq!(replies[1].request.token, InputId::new(10).unwrap());
    assert!(
        replies
            .iter()
            .all(|reply| reply.result == RequestResult::Outcome(OperationOutcome::Accepted))
    );
    assert!(
        control
            .handle(at(0), ControlInput::Tick, &mut port)
            .unwrap()
            .requests
            .is_empty()
    );
}

#[test]
fn physically_retained_context_returns_the_active_ticket_before_metadata_eviction() {
    use novarocks_worker_protocol::control::{
        ControlConfig, ControlCore, ControlEffect, ControlInput,
    };
    let mut control = ControlCore::new(
        ControlConfig {
            max_contexts: 3,
            max_waiters: 8,
            max_pending_requests: 16,
            step_budget: ControlCore::MAX_STEP_WORK,
            retained_contexts: 2,
            gone_fences: 3,
            retention: Duration::from_secs(10),
        },
        AdmissionTicketConfig::new(1, Duration::from_secs(10), Duration::from_secs(120), 8)
            .unwrap(),
        [1; 8],
        at(0),
    )
    .unwrap();
    let mut port = Port::default();
    let ticket = grant(&mut control, &mut port);
    control
        .handle(
            at(0),
            ControlInput::Establish {
                request: operation(2),
                context: context(),
                ticket,
                payload: PayloadId(1),
                semantics: semantics(),
                generation: Generation::new(1).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    control
        .handle(
            at(1),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision: active_establishment_decision(),
            },
            &mut port,
        )
        .unwrap();
    let revoked = control
        .handle(
            at(1),
            ControlInput::RevokeTicket { context: context() },
            &mut port,
        )
        .unwrap();
    assert!(
        revoked.effects.is_empty(),
        "closing external admission cannot release a live redeemed context"
    );
    assert_eq!(
        control.admission().observe(ticket, context(), at(1)),
        AdmissionTicketObservation::Redeemed
    );
    let retained = control
        .handle(
            at(1),
            ControlInput::Retained { context: context() },
            &mut port,
        )
        .unwrap();
    assert!(
        retained.effects.contains(&ControlEffect::RetireReservation(
            ReservationId::new(1).unwrap()
        )),
        "the converged context's active ticket cannot be held throughout metadata retention"
    );
    assert_eq!(
        control.admission().observe(ticket, context(), at(1)),
        AdmissionTicketObservation::Closed
    );
    let replay = control
        .handle(
            at(1),
            ControlInput::Retained { context: context() },
            &mut port,
        )
        .unwrap();
    assert!(
        replay.effects.is_empty(),
        "the original physical receipt neither double-releases nor extends retention"
    );
    let c = context();
    let next = QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(7203, 7204), AttemptId::new(1).unwrap()).unwrap(),
        c.frontend_process_id(),
        c.backend_process_id(),
    );
    let next_ticket = AdmissionTicketId::try_from_bytes([5; 16]).unwrap();
    let request = AcquireQueryContextAdmissionTicket::new(
        operation(3),
        next,
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        control.admission().capability(),
    );
    let acquired = control
        .handle(
            at(1),
            ControlInput::Acquire {
                request,
                ticket: next_ticket,
            },
            &mut port,
        )
        .unwrap();
    assert!(acquired.effects.iter().any(|effect| matches!(effect,
        ControlEffect::AdmissionReply { result: Ok(receipt), .. } if receipt.ticket_id() == next_ticket)));
    let revoked = control
        .handle(
            at(1),
            ControlInput::RevokeTicket { context: next },
            &mut port,
        )
        .unwrap();
    assert_eq!(
        revoked.effects,
        vec![ControlEffect::RetireReservation(
            ReservationId::new(3).unwrap()
        )]
    );
    assert_eq!(
        control.admission().observe(next_ticket, next, at(1)),
        AdmissionTicketObservation::Closed
    );
    let removed = control
        .handle(at(2), ControlInput::HostRemoved { context: c }, &mut port)
        .unwrap();
    assert_eq!(
        removed.effects,
        vec![ControlEffect::RetireReservation(
            ReservationId::new(2).unwrap()
        )]
    );
}

#[test]
fn succeeded_proposal_requires_actual_stop_and_complete_output_before_handoff() {
    use novarocks_worker_protocol::context::CoreError;
    let mut core = core(4, 1);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    for complete_output in [false, true] {
        core.handle(
            at(0),
            ContextInput::OutputFacts {
                task: task(1),
                facts: TaskOutputFacts::new(complete_output),
            },
            &mut port,
        )
        .unwrap();
        assert_eq!(
            core.handle(
                at(0),
                ContextInput::Proposal {
                    task: task(1),
                    proposal: proposal(TaskConclusion::Succeeded),
                },
                &mut port
            )
            .unwrap_err(),
            CoreError::IllegalFact
        );
        assert!(core.task(task(1)).unwrap().conclusion.accepted.is_none());
        assert!(!core.task(task(1)).unwrap().actual_stopped);
    }
    let stopped = core
        .handle(
            at(0),
            ContextInput::Physical {
                task: task(1),
                fact: PhysicalFact::ActualStopped,
            },
            &mut port,
        )
        .unwrap();
    assert!(stopped.charged_work() <= ContextCore::MAX_STEP_WORK);
    let accepted = core
        .handle(
            at(0),
            ContextInput::Proposal {
                task: task(1),
                proposal: proposal(TaskConclusion::Succeeded),
            },
            &mut port,
        )
        .unwrap();
    assert!(accepted.charged_work() <= ContextCore::MAX_STEP_WORK);
    core.handle(at(0), ContextInput::Adjudicate { task: task(1) }, &mut port)
        .unwrap();
    assert_eq!(
        core.task(task(1)).unwrap().conclusion.accepted,
        Some(TaskConclusion::Succeeded)
    );
}

fn completion_fixture() -> (ContextCore, Port) {
    let mut core = core(4, 1);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    loaded(&mut core, &mut port, 1);
    for input in [
        ContextInput::Installed { task: task(1) },
        ContextInput::Running { task: task(1) },
    ] {
        completion_step(&mut core, &mut port, input);
    }
    (core, port)
}
fn completion_step(
    core: &mut ContextCore,
    port: &mut Port,
    input: impl Into<FixtureInput>,
) -> novarocks_worker_protocol::context::ContextStep {
    let input = input.into();
    let label = format!("{input:?}");
    let step = core.apply_fixture(at(0), input, port).unwrap();
    assert!(
        step.charged_work() <= ContextCore::MAX_STEP_WORK,
        "{label}: actual charged work {} exceeds {}: {:?}",
        step.charged_work(),
        ContextCore::MAX_STEP_WORK,
        step.effects
    );
    step
}
fn completion_failure(source: u64) -> Proposal {
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure,
        failure::{FailureClass, FailureEvidence, FailureFact, FailureReason, domain},
    };
    use novarocks_worker_protocol::proposal_summary::SourceId;
    Proposal {
        conclusion: TaskConclusion::Terminated(TerminationDetail::Failed(TaskFailure::new(
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::EXECUTION, std::num::NonZeroU16::new(51).unwrap()),
            ),
            SafeDetail::truncating("original completion failure"),
        ))),
        evidence: Some(FailureEvidence::Originating),
        source: Some(SourceId(source)),
        diagnostic: None,
    }
}

#[test]
fn completion_intent_waits_for_the_original_action_and_fact_exit_without_waiting_for_preparation() {
    let (mut core, mut port) = completion_fixture();
    let admitted = port.calls.len();
    let intent = completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: Some(TaskOutputFacts::new(true)),
        },
    );
    assert!(intent.effects.is_empty());
    let record = core.task(task(1)).unwrap();
    assert!(record.completion_success_pending);
    assert!(record.conclusion.accepted.is_none());
    assert!(!record.actual_stopped);
    assert!(!record.resources_converged);
    assert!(!record.preparation_body_exited);
    assert!(!record.preparation_exited);
    let exited = completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    let record = core.task(task(1)).unwrap();
    assert_eq!(record.state, TaskState::Finished);
    assert_eq!(record.conclusion.accepted, Some(TaskConclusion::Succeeded));
    assert!(record.actual_stopped && record.resources_converged);
    assert!(!record.completion_success_pending);
    assert!(!record.preparation_exited);
    assert!(!record.output_released && !record.root_idle);
    assert!(
        !exited
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::RetireTask { .. }))
    );
    let published = record.published.clone();
    assert!(
        completion_step(
            &mut core,
            &mut port,
            ContextInput::CompletionExited { task: task(1) }
        )
        .effects
        .is_empty()
    );
    assert_eq!(core.task(task(1)).unwrap().published, published);
    assert_eq!(
        port.calls.len(),
        admitted,
        "completion uses originally admitted responsibility"
    );
}

#[test]
fn completion_exit_can_precede_success_intent_and_later_output_facts_close_it() {
    use novarocks_worker_protocol::context::CoreError;
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    assert!(core.task(task(1)).unwrap().conclusion.accepted.is_none());
    assert_eq!(
        core.handle(
            at(0),
            ContextInput::Proposal {
                task: task(1),
                proposal: proposal(TaskConclusion::Succeeded),
            },
            &mut port
        )
        .unwrap_err(),
        CoreError::IllegalFact,
        "actual stop still does not grant missing output responsibility",
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: None,
        },
    );
    assert!(core.task(task(1)).unwrap().completion_success_pending);
    assert_eq!(core.task(task(1)).unwrap().state, TaskState::Running);
    completion_step(
        &mut core,
        &mut port,
        ContextInput::OutputFacts {
            task: task(1),
            facts: TaskOutputFacts::new(true),
        },
    );
    assert_eq!(core.task(task(1)).unwrap().state, TaskState::Finished);
    assert!(!core.task(task(1)).unwrap().preparation_exited);
}

#[test]
fn a_true_actual_stop_fact_closes_the_intent_without_inventing_resource_convergence() {
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: Some(TaskOutputFacts::new(true)),
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::Physical {
            task: task(1),
            fact: PhysicalFact::ActualStopped,
        },
    );
    let record = core.task(task(1)).unwrap();
    assert_eq!(record.state, TaskState::Finished);
    assert!(record.actual_stopped);
    assert!(!record.resources_converged);
}

#[test]
fn the_last_original_publication_hold_retries_success_without_waiting_for_whole_preparation() {
    use novarocks_worker_protocol::context::PreparationExitRole;
    let (mut core, mut port) = completion_fixture();
    for role in [
        PreparationExitRole::Admission,
        PreparationExitRole::Preparation,
    ] {
        completion_step(
            &mut core,
            &mut port,
            ContextInput::PreparationExitHold {
                task: task(1),
                role,
                held: true,
            },
        );
    }
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: Some(TaskOutputFacts::new(true)),
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::Physical {
            task: task(1),
            fact: PhysicalFact::PreparationBodyExited,
        },
    );
    assert!(core.task(task(1)).unwrap().runtime_actual_stopped_received);
    assert!(!core.task(task(1)).unwrap().actual_stopped);
    completion_step(
        &mut core,
        &mut port,
        ContextInput::PreparationExitHold {
            task: task(1),
            role: PreparationExitRole::Admission,
            held: false,
        },
    );
    assert!(core.task(task(1)).unwrap().conclusion.accepted.is_none());
    completion_step(
        &mut core,
        &mut port,
        ContextInput::PreparationExitHold {
            task: task(1),
            role: PreparationExitRole::Preparation,
            held: false,
        },
    );
    let record = core.task(task(1)).unwrap();
    assert_eq!(record.state, TaskState::Finished);
    assert!(record.actual_stopped && record.resources_converged);
    assert!(!record.preparation_exited);
}

#[test]
fn successful_root_production_waits_for_output_and_drain_reply_follows_the_terminal_publication() {
    use novarocks_worker_protocol::context::RootDrainVerdict;
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: None,
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::Flushing { task: task(1) },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    assert_eq!(core.task(task(1)).unwrap().state, TaskState::Flushing);
    assert!(core.task(task(1)).unwrap().completion_success_pending);
    let drained = completion_step(
        &mut core,
        &mut port,
        rpc(operation(10), ContextCommand::RootDrained { task: task(1) }),
    );
    let record = core.task(task(1)).unwrap();
    assert_eq!(record.state, TaskState::Finished);
    let terminal = drained.effects.iter().position(|effect| matches!(effect,
        ContextEffect::PublishTask { state: TaskState::Finished, snapshot, .. } if snapshot.output().responsibility_complete())).unwrap();
    let reply = drained.effects.iter().position(|effect| matches!(effect,
        ContextEffect::RootDrainReply { result: RootDrainVerdict::Published(version), .. } if *version == record.version)).unwrap();
    assert!(terminal < reply);
    assert!(record.output.responsibility_complete());
    assert!(
        !record.output_released,
        "consumer ACK is separate from output ownership release"
    );
}

#[test]
fn a_cancellation_between_success_intent_and_exit_keeps_its_conclusion_and_independent_output() {
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: Some(TaskOutputFacts::new(true)),
        },
    );
    completion_step(
        &mut core,
        &mut port,
        rpc(
            operation(10),
            ContextCommand::Cancel {
                task: task(1),
                reason: CancelReason::UpstreamNoLongerNeeded,
            },
        ),
    );
    assert_eq!(core.task(task(1)).unwrap().state, TaskState::Canceling);
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    let record = core.task(task(1)).unwrap();
    assert_eq!(record.state, TaskState::Canceled);
    assert!(record.output.responsibility_complete());
    assert!(!record.completion_success_pending);
}

#[test]
fn a_stopped_root_success_intent_is_completed_as_the_later_cancellation_without_drain() {
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: None,
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    assert_eq!(core.task(task(1)).unwrap().state, TaskState::Running);
    completion_step(
        &mut core,
        &mut port,
        rpc(
            operation(10),
            ContextCommand::Cancel {
                task: task(1),
                reason: CancelReason::UpstreamNoLongerNeeded,
            },
        ),
    );
    let record = core.task(task(1)).unwrap();
    assert_eq!(record.state, TaskState::Canceled);
    assert!(!record.output.responsibility_complete());
    assert!(!record.completion_success_pending);
}

#[test]
fn a_peer_placeholder_refines_to_the_original_failure_before_completion_exit_and_cannot_refine_after_sealing()
 {
    use novarocks_worker_protocol::proposal_summary::{ProposalSummary, SourceId};
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: None,
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::Proposal {
            task: task(1),
            proposal: proposal(TaskConclusion::Terminated(TerminationDetail::Aborted(
                AbortCause::PeerTaskFailed,
            ))),
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::Adjudicate { task: task(1) },
    );
    let first = completion_failure(17);
    let expected = first.conclusion.clone();
    let mut summary = ProposalSummary::default();
    summary.push(first);
    completion_step(
        &mut core,
        &mut port,
        ContextInput::Summary {
            task: task(1),
            generation: Generation::new(1).unwrap(),
            cut: None,
            summary,
        },
    );
    assert_eq!(
        core.task(task(1)).unwrap().conclusion.source,
        Some(SourceId(17))
    );
    assert_eq!(core.task(task(1)).unwrap().state, TaskState::Failing);
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    assert_eq!(core.task(task(1)).unwrap().state, TaskState::Failed);
    assert_eq!(
        core.task(task(1)).unwrap().conclusion.accepted,
        Some(expected.clone())
    );
    let mut late = ProposalSummary::default();
    late.push(completion_failure(18));
    let discarded = completion_step(
        &mut core,
        &mut port,
        ContextInput::Summary {
            task: task(1),
            generation: Generation::new(1).unwrap(),
            cut: None,
            summary: late,
        },
    );
    assert!(discarded.effects.contains(&ContextEffect::DisposeSource {
        task: task(1),
        source: SourceId(18)
    }));
    assert_eq!(
        core.task(task(1)).unwrap().conclusion.source,
        Some(SourceId(17))
    );
    assert_eq!(
        core.task(task(1)).unwrap().conclusion.accepted,
        Some(expected)
    );
}

#[test]
fn both_predecessor_summaries_are_adjudicated_before_a_pending_completion_can_seal_the_result() {
    use novarocks_worker_protocol::proposal_summary::{ProposalSummary, SourceId};
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: None,
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::Proposal {
            task: task(1),
            proposal: proposal(TaskConclusion::Terminated(TerminationDetail::Aborted(
                AbortCause::PeerTaskFailed,
            ))),
        },
    );
    let mut summary = ProposalSummary::default();
    summary.push(completion_failure(19));
    completion_step(
        &mut core,
        &mut port,
        ContextInput::Summary {
            task: task(1),
            generation: Generation::new(1).unwrap(),
            cut: None,
            summary,
        },
    );
    let record = core.task(task(1)).unwrap();
    assert_eq!(record.state, TaskState::Failed);
    assert_eq!(record.conclusion.source, Some(SourceId(19)));
    assert_eq!(record.conclusion.generation, 2);
}

#[test]
fn finishing_a_prior_termination_clears_the_waiting_intent_and_late_success_never_reopens_it() {
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: None,
        },
    );
    completion_step(
        &mut core,
        &mut port,
        rpc(
            operation(10),
            ContextCommand::Cancel {
                task: task(1),
                reason: CancelReason::UpstreamNoLongerNeeded,
            },
        ),
    );
    completion_step(&mut core, &mut port, ContextInput::Finish { task: task(1) });
    assert_eq!(core.task(task(1)).unwrap().state, TaskState::Canceled);
    assert!(!core.task(task(1)).unwrap().actual_stopped);
    assert!(!core.task(task(1)).unwrap().completion_success_pending);
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: None,
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    assert_eq!(core.task(task(1)).unwrap().state, TaskState::Canceled);
}

#[test]
fn completion_intent_rejects_unowned_uninstalled_or_incomplete_positive_output_facts() {
    use novarocks_worker_protocol::context::CoreError;
    let mut core = core(4, 1);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    for input in [
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: None,
        },
        ContextInput::CompletionExited { task: task(1) },
    ] {
        assert_eq!(
            core.handle(at(0), input, &mut port).unwrap_err(),
            CoreError::IllegalFact
        );
    }
    loaded(&mut core, &mut port, 1);
    assert_eq!(
        core.handle(
            at(0),
            ContextInput::CompletionIntent {
                task: task(1),
                completed_output: None
            },
            &mut port
        )
        .unwrap_err(),
        CoreError::IllegalFact
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::Installed { task: task(1) },
    );
    assert_eq!(
        core.handle(
            at(0),
            ContextInput::CompletionIntent {
                task: task(1),
                completed_output: Some(TaskOutputFacts::new(false))
            },
            &mut port
        )
        .unwrap_err(),
        CoreError::IllegalFact
    );
    assert!(!core.task(task(1)).unwrap().completion_success_pending);
    assert!(core.task(task(1)).unwrap().conclusion.accepted.is_none());
}

#[test]
fn completion_summary_with_lease_expiry_source_release_and_task_retirement_stays_in_the_fifteen_unit_budget()
 {
    use novarocks_execution_contract::failure::FailureEvidence;
    use novarocks_worker_protocol::proposal_summary::ProposalSummary;
    let candidate = |kind, source| match kind {
        0 => None,
        1 => Some(proposal(TaskConclusion::Terminated(
            TerminationDetail::Aborted(AbortCause::PeerTaskFailed),
        ))),
        2 => Some(proposal(TaskConclusion::Terminated(
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
        ))),
        3 => Some(completion_failure(source)),
        4 => {
            let mut failure = completion_failure(source);
            failure.evidence = Some(FailureEvidence::StopObservation);
            Some(failure)
        }
        _ => unreachable!(),
    };
    let mut maximum = 0;
    for a in 0..5 {
        for b in 0..5 {
            for c in 0..5 {
                for d in 0..5 {
                    let (mut core, mut port) = completion_fixture();
                    completion_step(
                        &mut core,
                        &mut port,
                        ContextInput::CompletionIntent {
                            task: task(1),
                            completed_output: None,
                        },
                    );
                    completion_step(
                        &mut core,
                        &mut port,
                        ContextInput::CompletionExited { task: task(1) },
                    );
                    for fact in [
                        PhysicalFact::LoaderExited,
                        PhysicalFact::PreparationBodyExited,
                        PhysicalFact::PreparationExited,
                        PhysicalFact::OutputReleased,
                        PhysicalFact::RootIdle,
                    ] {
                        completion_step(
                            &mut core,
                            &mut port,
                            ContextInput::Physical {
                                task: task(1),
                                fact,
                            },
                        );
                    }
                    for (kind, source) in [(a, 1), (b, 2)] {
                        if let Some(proposal) = candidate(kind, source) {
                            completion_step(
                                &mut core,
                                &mut port,
                                ContextInput::Proposal {
                                    task: task(1),
                                    proposal,
                                },
                            );
                        }
                    }
                    let mut summary = ProposalSummary::default();
                    for (kind, source) in [(c, 3), (d, 4)] {
                        if let Some(proposal) = candidate(kind, source) {
                            summary.push(proposal);
                        }
                    }
                    let step = core
                        .handle(
                            at(31),
                            ContextInput::Summary {
                                task: task(1),
                                generation: Generation::new(1).unwrap(),
                                cut: None,
                                summary,
                            },
                            &mut port,
                        )
                        .unwrap();
                    maximum = maximum.max(step.charged_work());
                    assert!(
                        step.charged_work() <= ContextCore::MAX_STEP_WORK,
                        "candidate sequence {a},{b},{c},{d} charges {}: {:?}",
                        step.charged_work(),
                        step.effects
                    );
                    let record = core.task(task(1)).unwrap();
                    if a + b + c + d != 0 {
                        assert!(record.state.is_terminal());
                        assert_eq!(
                            record.phase,
                            novarocks_worker_protocol::task::CreationPhase::Retained
                        );
                        assert!(!record.completion_success_pending);
                    }
                }
            }
        }
    }
    assert!(
        maximum >= 10,
        "the matrix reaches combined publication and source-release work"
    );
}

#[test]
fn task_close_retries_a_stopped_pending_success_as_the_existing_abort() {
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: None,
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    completion_step(
        &mut core,
        &mut port,
        rpc(
            operation(10),
            ContextCommand::Abort {
                cause: AbortCause::PeerTaskFailed,
            },
        ),
    );
    completion_step(&mut core, &mut port, ContextInput::ContinueClose);
    let closed = completion_step(
        &mut core,
        &mut port,
        ContextInput::ApplyTaskClose {
            task: task(1),
            generation: Generation::new(2).unwrap(),
        },
    );
    let record = core.task(task(1)).unwrap();
    assert_eq!(record.state, TaskState::Aborted);
    assert!(!record.completion_success_pending);
    assert!(!record.output.responsibility_complete());
    assert!(closed.effects.iter().any(|effect| matches!(
        effect,
        ContextEffect::PublishTask {
            state: TaskState::Aborted,
            ..
        }
    )));
}

#[test]
fn a_root_drain_applies_its_positive_output_before_finishing_a_buffered_peer_abort() {
    use novarocks_worker_protocol::context::RootDrainVerdict;
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: None,
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::Proposal {
            task: task(1),
            proposal: proposal(TaskConclusion::Terminated(TerminationDetail::Aborted(
                AbortCause::PeerTaskFailed,
            ))),
        },
    );
    let drained = completion_step(
        &mut core,
        &mut port,
        rpc(operation(10), ContextCommand::RootDrained { task: task(1) }),
    );
    let record = core.task(task(1)).unwrap();
    assert_eq!(record.state, TaskState::Aborted);
    assert!(
        record.published.output().responsibility_complete(),
        "the same operation's positive output must be in its terminal publication"
    );
    assert!(
        matches!(drained.effects.last(), Some(ContextEffect::RootDrainReply {
        result: RootDrainVerdict::Published(version), ..
    }) if *version == record.version)
    );
}

#[test]
fn one_finish_with_output_event_preserves_its_causal_failure_source_and_positive_output_without_claiming_exit()
 {
    use novarocks_worker_protocol::proposal_summary::SourceId;
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::Proposal {
            task: task(1),
            proposal: completion_failure(77),
        },
    );
    let finish = completion_step(
        &mut core,
        &mut port,
        ContextInput::FinishWithOutput {
            task: task(1),
            output: Some(TaskOutputFacts::new(true)),
        },
    );
    let record = core.task(task(1)).unwrap();
    assert_eq!(record.state, TaskState::Failed);
    assert_eq!(record.conclusion.source, Some(SourceId(77)));
    assert!(record.published.output().responsibility_complete());
    assert!(!record.actual_stopped && !record.resources_converged && !record.output_released);
    assert!(matches!(
        finish.effects.last(),
        Some(ContextEffect::PublishTask {
            state: TaskState::Failed,
            ..
        })
    ));
    completion_step(
        &mut core,
        &mut port,
        ContextInput::FinishWithOutput {
            task: task(1),
            output: None,
        },
    );
    assert!(
        core.task(task(1)).unwrap().output.responsibility_complete(),
        "absence of new output evidence cannot revoke a completed handoff"
    );
}

#[test]
fn actual_publications_carry_each_convergence_field_and_their_exact_versions() {
    use novarocks_worker_protocol::context::PreparationExitRole;
    let (mut core, mut port) = completion_fixture();
    completion_step(
        &mut core,
        &mut port,
        ContextInput::PreparationExitHold {
            task: task(1),
            role: PreparationExitRole::Preparation,
            held: true,
        },
    );
    let released = completion_step(
        &mut core,
        &mut port,
        ContextInput::Physical {
            task: task(1),
            fact: PhysicalFact::OutputReleased,
        },
    );
    let released_snapshot = released
        .effects
        .iter()
        .find_map(|effect| match effect {
            ContextEffect::PublishTaskConvergence {
                version, snapshot, ..
            } => {
                assert_eq!(*version, snapshot.version());
                Some(*snapshot)
            }
            _ => None,
        })
        .expect("an independent output release publishes even under a preparation hold");
    assert!(released_snapshot.output_released());
    assert!(!released_snapshot.actual_stopped());
    assert_eq!(
        released
            .effects
            .iter()
            .filter(|e| matches!(e, ContextEffect::PublishTaskConvergence { .. }))
            .count(),
        1
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: Some(TaskOutputFacts::new(true)),
        },
    );
    let deferred_exit = completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    assert!(
        !deferred_exit
            .effects
            .iter()
            .any(|e| matches!(e, ContextEffect::PublishTaskConvergence { .. }))
    );
    let terminal = completion_step(
        &mut core,
        &mut port,
        ContextInput::PreparationExitHold {
            task: task(1),
            role: PreparationExitRole::Preparation,
            held: false,
        },
    );
    let actual_exit = terminal
        .effects
        .iter()
        .find_map(|effect| match effect {
            ContextEffect::PublishTaskConvergence {
                version, snapshot, ..
            } => {
                assert_eq!(*version, snapshot.version());
                Some(*snapshot)
            }
            _ => None,
        })
        .expect("the real exit publishes the applied receipts");
    assert!(
        actual_exit.actual_stopped()
            && actual_exit.resources_converged()
            && actual_exit.output_released()
    );
    assert!(!actual_exit.conclusion_stable());
    assert!(actual_exit.version() > released_snapshot.version());
    let stable = terminal
        .effects
        .iter()
        .find_map(|effect| match effect {
            ContextEffect::PublishTask {
                state: TaskState::Finished,
                convergence,
                ..
            } => Some(*convergence),
            _ => None,
        })
        .expect("the actual terminal publication carries its final neutral convergence");
    assert!(stable.conclusion_stable() && stable.retirement_ready());
    assert!(stable.version() > actual_exit.version());
    for fact in [
        PhysicalFact::LoaderExited,
        PhysicalFact::PreparationBodyExited,
        PhysicalFact::PreparationExited,
    ] {
        completion_step(
            &mut core,
            &mut port,
            ContextInput::Physical {
                task: task(1),
                fact,
            },
        );
    }
    let retired = completion_step(
        &mut core,
        &mut port,
        ContextInput::Physical {
            task: task(1),
            fact: PhysicalFact::RootIdle,
        },
    );
    let final_snapshot = retired
        .effects
        .iter()
        .find_map(|effect| match effect {
            ContextEffect::RetireTask { convergence, .. } => Some(*convergence),
            _ => None,
        })
        .expect("actual retirement publishes the final monotonic view");
    assert!(final_snapshot.retired());
    assert!(!final_snapshot.retirement_ready());
    assert!(final_snapshot.version() > stable.version());
    assert_eq!(
        final_snapshot,
        core.task(task(1)).unwrap().convergence.snapshot()
    );
}

#[test]
fn summary_consumed_contains_the_actual_bounded_diagnostics_author_view() {
    use novarocks_execution_contract::{
        SafeDetail, TaskFailurePhase,
        failure::{FailureClass, FailureFact, FailureReason, domain},
    };
    use novarocks_worker_protocol::proposal_summary::{Diagnostic, ProposalSummary};
    let (mut core, mut port) = completion_fixture();
    let mut summary = ProposalSummary::default();
    for i in 0..7 {
        summary.cleanup(Diagnostic {
            phase: TaskFailurePhase::Execution,
            fact: FailureFact::new(
                FailureClass::Unexpected,
                FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(60001).unwrap()),
            ),
            detail: SafeDetail::truncating(&format!("cleanup {i}")),
            cleanup: true,
        });
    }
    let consumed = completion_step(
        &mut core,
        &mut port,
        ContextInput::Summary {
            task: task(1),
            generation: Generation::new(1).unwrap(),
            cut: None,
            summary,
        },
    );
    let actual = consumed
        .effects
        .iter()
        .find_map(|effect| match effect {
            ContextEffect::SummaryConsumed { diagnostics, .. } => Some(diagnostics),
            _ => None,
        })
        .expect("the existing summary receipt carries the author's bounded view");
    assert_eq!(actual, &core.task(task(1)).unwrap().diagnostics);
    assert_eq!(actual.entries.len(), 4);
    assert_eq!(actual.dropped, 3);
}

#[test]
fn a_shorter_operator_snapshot_replaces_counts_but_preserves_prior_truncation_in_frozen_final_info()
{
    use novarocks_execution_contract::{OperatorStatistics, SafeDetail};
    let (mut core, mut port) = completion_fixture();
    let statistics = |rows| {
        vec![OperatorStatistics::new(1, SafeDetail::truncating("scan")).with_output_rows(rows)]
    };
    completion_step(
        &mut core,
        &mut port,
        ContextInput::OperatorStatistics {
            task: task(1),
            statistics: statistics(10),
            truncated: true,
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::OperatorStatistics {
            task: task(1),
            statistics: statistics(2),
            truncated: false,
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionIntent {
            task: task(1),
            completed_output: Some(TaskOutputFacts::new(true)),
        },
    );
    completion_step(
        &mut core,
        &mut port,
        ContextInput::CompletionExited { task: task(1) },
    );
    let read = completion_step(
        &mut core,
        &mut port,
        rpc(
            operation(11),
            ContextCommand::GetFinalInfo { task: task(1) },
        ),
    );
    let info = read
        .effects
        .iter()
        .find_map(|effect| match effect {
            ContextEffect::FinalInfoReply { info, .. } => Some(info),
            _ => None,
        })
        .expect("real final information is frozen at the terminal publication");
    assert_eq!(
        info.operator_statistics(),
        statistics(2),
        "complete operator snapshots replace previous counters; they are not deltas"
    );
    assert!(
        info.operator_statistics_truncated(),
        "a later short snapshot cannot erase already observed truncation"
    );
    // Late producer observations cannot rewrite the immutable final export.
    completion_step(
        &mut core,
        &mut port,
        ContextInput::OperatorStatistics {
            task: task(1),
            statistics: statistics(99),
            truncated: false,
        },
    );
    let reread = completion_step(
        &mut core,
        &mut port,
        rpc(
            operation(12),
            ContextCommand::GetFinalInfo { task: task(1) },
        ),
    );
    let later = reread
        .effects
        .iter()
        .find_map(|effect| match effect {
            ContextEffect::FinalInfoReply { info, .. } => Some(info),
            _ => None,
        })
        .unwrap();
    assert_eq!(later, info);
}

fn selector_context(query: u64, frontend: u8) -> QueryContextRef {
    QueryContextRef::new(
        QueryExecutionId::new(
            QueryId::new(
                i64::try_from(query).unwrap(),
                i64::try_from(query + 1).unwrap(),
            ),
            AttemptId::new(1).unwrap(),
        )
        .unwrap(),
        FrontendProcessId::try_from_bytes(bytes(frontend)).unwrap(),
        context().backend_process_id(),
    )
}

fn selector_grant(
    control: &mut novarocks_worker_protocol::control::ControlCore,
    port: &mut Port,
    context: QueryContextRef,
    base: u8,
    valid_for: u64,
) -> AdmissionTicketId {
    use novarocks_worker_protocol::control::ControlInput;
    let ticket = AdmissionTicketId::try_from_bytes([base; 16]).unwrap();
    let request = AcquireQueryContextAdmissionTicket::new(
        operation(base),
        context,
        LeaseValidFor::new(Duration::from_secs(valid_for)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        control.admission().capability(),
    );
    let granted = control
        .handle(at(0), ControlInput::Acquire { request, ticket }, port)
        .unwrap();
    assert!(granted.charged_work() <= control.max_step_work());
    ticket
}

fn selector_establish(
    control: &mut novarocks_worker_protocol::control::ControlCore,
    port: &mut Port,
    context: QueryContextRef,
    base: u8,
) -> novarocks_worker_protocol::control::ControlStep {
    use novarocks_worker_protocol::control::ControlInput;
    let ticket = selector_grant(control, port, context, base, 5);
    let step = control
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(u64::from(base) * 10 + 1).unwrap(),
                operation: operation(base + 1),
                deadline: at(100),
            },
            ControlInput::Establish {
                request: operation(base + 1),
                context,
                ticket,
                payload: PayloadId(u64::from(base)),
                semantics: semantics(),
                generation: Generation::new(1).unwrap(),
            },
            port,
        )
        .unwrap();
    assert!(step.requests.is_empty());
    assert!(step.charged_work() <= control.max_step_work());
    step
}

#[test]
fn next_internal_fairly_services_fences_and_due_maintenance_without_taking_routed_replies() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut control = control();
    let mut port = Port::default();
    let contexts = [selector_context(8101, 1), selector_context(8201, 1)];
    let mut routes = Vec::new();
    for (index, identity) in contexts.into_iter().enumerate() {
        let base = 10 + index as u8 * 10;
        selector_establish(&mut control, &mut port, identity, base);
        let replay = control
            .handle_request(
                at(0),
                RequestContext {
                    token: InputId::new(u64::from(base) * 10 + 2).unwrap(),
                    operation: operation(base + 1),
                    deadline: at(100),
                },
                ControlInput::Establish {
                    request: operation(base + 1),
                    context: identity,
                    ticket: AdmissionTicketId::try_from_bytes([base; 16]).unwrap(),
                    payload: PayloadId(999),
                    semantics: semantics(),
                    generation: Generation::new(1).unwrap(),
                },
                &mut port,
            )
            .unwrap();
        routed_establish_request(&replay);
        routes.push(replay);
    }
    control
        .handle(
            at(0),
            ControlInput::FenceFrontend {
                frontend: contexts[0].frontend_process_id(),
            },
            &mut port,
        )
        .unwrap();
    selector_grant(&mut control, &mut port, selector_context(8301, 2), 30, 1);
    let due_reservation = ReservationId::new(port.next).unwrap();
    let calls = port.calls.len();
    let mut steps = Vec::new();
    for _ in 0..16 {
        let next = control
            .handle(at(1), ControlInput::NextInternal, &mut port)
            .unwrap();
        assert!(
            next.charged_work() + usize::from(control.has_internal_work())
                <= control.max_step_work()
        );
        assert!(
            next.requests.is_empty(),
            "the selector cannot decide a pending routed R"
        );
        steps.push(next);
        if !control.has_internal_work() {
            break;
        }
    }
    assert!(!control.has_internal_work());
    let retire_position=steps.iter().position(|step|step.effects.iter().any(|effect|matches!(effect,ControlEffect::RetireReservation(id) if *id==due_reservation))).unwrap();
    assert!(
        retire_position < 4,
        "due maintenance gets a turn while the fence still has members"
    );
    for identity in contexts {
        assert_eq!(steps.iter().flat_map(|step|&step.effects).filter(|effect|matches!(effect,ControlEffect::AbortContext {context,..} if *context==identity)).count(),1);
    }
    for route in &routes {
        let reply = finish_routed_establish(
            &mut control,
            &mut port,
            route,
            1,
            1,
            Ok(OperationOutcome::InvalidStateOrRequest),
        );
        assert_eq!(reply.requests.len(), 1);
        assert_eq!(
            reply.requests[0].request,
            routed_establish_request(route).1.request
        );
    }
    assert_eq!(port.calls.len(), calls);
    for _ in 0..3 {
        let idle = control
            .handle(at(1), ControlInput::NextInternal, &mut port)
            .unwrap();
        assert!(idle.effects.is_empty());
        assert!(idle.requests.is_empty());
        assert!(!control.has_internal_work());
    }
}

#[test]
fn a_routed_deadline_cancels_only_that_route_without_a_reply_selector_loop() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut f = ActiveControlFixture::new(4, 100);
    let route = f.request(2, 1, active_establish_input(f.ticket, 2));
    let original = routed_establish_request(&route).1.request;
    let timed = f
        .control
        .handle(at(1), ControlInput::Tick, &mut f.port)
        .unwrap();
    assert_eq!(timed.requests.len(), 1);
    assert_eq!(timed.requests[0].request, original);
    assert_eq!(
        timed.requests[0].result,
        RequestResult::TimeoutWhileAccepted
    );
    assert_eq!(timed.effects.iter().filter(|effect|matches!(effect,ControlEffect::CancelEstablishRoute{context:target,request} if *target==context() && *request==original)).count(),1);
    assert!(!f.control.has_internal_work());
    let idle = f
        .control
        .handle(at(1), ControlInput::NextInternal, &mut f.port)
        .unwrap();
    assert!(idle.effects.is_empty());
    assert!(idle.requests.is_empty());
    let late = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        1,
        0,
        Ok(OperationOutcome::Idempotent),
    );
    assert!(late.requests.is_empty());
    assert_eq!(f.control.route(context()), QueryContextState::Establishing);
}

#[test]
fn next_internal_serializes_two_frontend_fences_without_restarting_duplicates() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut control = control();
    let mut port = Port::default();
    let contexts = [
        selector_context(9101, 3),
        selector_context(9201, 3),
        selector_context(9301, 4),
    ];
    for (i, context) in contexts.into_iter().enumerate() {
        let base = 40 + i as u8 * 3;
        let ticket = selector_grant(&mut control, &mut port, context, base, 5);
        control
            .handle(
                at(0),
                ControlInput::Establish {
                    request: operation(base + 1),
                    context,
                    ticket,
                    payload: PayloadId(u64::from(base)),
                    semantics: semantics(),
                    generation: Generation::new(1).unwrap(),
                },
                &mut port,
            )
            .unwrap();
    }
    // Both ordinary FE loss facts can arrive before the first scan starts.
    for frontend in [
        contexts[0].frontend_process_id(),
        contexts[2].frontend_process_id(),
    ] {
        let queued = control
            .handle(at(0), ControlInput::FenceFrontend { frontend }, &mut port)
            .unwrap();
        assert_eq!(queued.effects, vec![ControlEffect::ContinueFence]);
        assert!(queued.charged_work() <= control.max_step_work());
    }
    let mut effects = Vec::new();
    for _ in 0..8 {
        let duplicate = control
            .handle(
                at(0),
                ControlInput::FenceFrontend {
                    frontend: contexts[0].frontend_process_id(),
                },
                &mut port,
            )
            .unwrap();
        assert!(duplicate.effects.is_empty());
        if !control.has_internal_work() {
            break;
        }
        let step = control
            .handle(at(0), ControlInput::NextInternal, &mut port)
            .unwrap();
        assert!(
            step.charged_work() + usize::from(control.has_internal_work())
                <= control.max_step_work()
        );
        effects.extend(step.effects);
    }
    assert!(!control.has_internal_work());
    for (i, context) in contexts.into_iter().enumerate() {
        assert_eq!(control.route(context), QueryContextState::Aborting);
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(effect,
                    ControlEffect::AbortContext { context: original, generation }
                        if *original == context && *generation == Generation::new(2).unwrap()
                ))
                .count(),
            1
        );
        let materialized = control
            .handle(
                at(0),
                ControlInput::EstablishmentDecided {
                    context,
                    decision: active_establishment_decision(),
                },
                &mut port,
            )
            .unwrap();
        assert!(materialized.effects.iter().any(|effect| matches!(effect,
            ControlEffect::Reply { request, outcome: OperationOutcome::ContextTerminalReceipt }
                if *request == operation(41 + i as u8 * 3)
        )));
        assert_eq!(control.route(context), QueryContextState::Aborting);
        assert!(materialized.charged_work() <= control.max_step_work());
    }
    let calls = port.calls.len();
    let refused_context = selector_context(9401, 4);
    let request = AcquireQueryContextAdmissionTicket::new(
        operation(60),
        refused_context,
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        control.admission().capability(),
    );
    let refused = control
        .handle(
            at(0),
            ControlInput::Acquire {
                request,
                ticket: AdmissionTicketId::try_from_bytes([60; 16]).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    assert!(refused.effects.iter().any(|effect| matches!(
        effect,
        ControlEffect::AdmissionReply {
            result: Err(OperationOutcome::Gone),
            ..
        }
    )));
    assert_eq!(port.calls.len(), calls);
}

#[test]
fn next_internal_empty_frontend_fences_each_take_one_step_and_preserve_the_fence_bound() {
    use novarocks_worker_protocol::control::{
        ControlConfig, ControlCore, ControlEffect, ControlInput,
    };
    let mut control = ControlCore::new(
        ControlConfig {
            max_contexts: 2,
            max_waiters: 2,
            max_pending_requests: 2,
            step_budget: ControlCore::MAX_STEP_WORK,
            retained_contexts: 2,
            gone_fences: 2,
            retention: Duration::from_secs(10),
        },
        AdmissionTicketConfig::DEFAULT,
        [1; 8],
        at(0),
    )
    .unwrap();
    let mut port = Port::default();
    let frontends = [3, 4, 5].map(|id| FrontendProcessId::try_from_bytes(bytes(id)).unwrap());
    for frontend in &frontends[..2] {
        control
            .handle(
                at(0),
                ControlInput::FenceFrontend {
                    frontend: *frontend,
                },
                &mut port,
            )
            .unwrap();
    }
    let first = control
        .handle(at(0), ControlInput::NextInternal, &mut port)
        .unwrap();
    assert_eq!(first.effects, vec![ControlEffect::ContinueFence]);
    assert!(control.has_internal_work());
    let second = control
        .handle(at(0), ControlInput::NextInternal, &mut port)
        .unwrap();
    assert!(second.effects.is_empty());
    assert!(!control.has_internal_work());
    let duplicate = control
        .handle(
            at(0),
            ControlInput::FenceFrontend {
                frontend: frontends[0],
            },
            &mut port,
        )
        .unwrap();
    assert!(duplicate.effects.is_empty());
    assert!(!control.has_internal_work());
    assert_eq!(
        control
            .handle(
                at(0),
                ControlInput::FenceFrontend {
                    frontend: frontends[2]
                },
                &mut port
            )
            .unwrap_err(),
        novarocks_worker_protocol::context::CoreError::InvalidConfiguration
    );
    assert!(port.calls.is_empty());
    for step in [first, second, duplicate] {
        assert!(step.charged_work() <= control.max_step_work());
    }
}

#[test]
fn a_routed_decision_uses_its_context_time_instead_of_root_delivery_time() {
    let mut f = ActiveControlFixture::new(4, 1);
    let route = f.request(2, 1, active_establish_input(f.ticket, 2));
    let first = f.materialized();
    assert_eq!(first.requests[0].request.token, InputId::new(1).unwrap());
    let late = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        2,
        0,
        Ok(OperationOutcome::Idempotent),
    );
    assert_eq!(late.requests[0].request.token, InputId::new(2).unwrap());
    assert_eq!(
        late.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Idempotent)
    );
    assert!(!f.control.has_internal_work());
}

fn materialization_refusal() -> AdmissionRefusal {
    use novarocks_execution_contract::{
        OperationDomainFailure, SafeDetail,
        failure::{FailureClass, FailureFact, FailureReason, domain},
        operation::DomainFailureOutcome,
    };
    AdmissionRefusal::Domain(
        DomainFailureOutcome::InvalidStateOrRequest,
        OperationDomainFailure::new(
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::EXECUTION, std::num::NonZeroU16::new(91).unwrap()),
            ),
            SafeDetail::truncating("context shared facts materialization rejected"),
        ),
    )
}

#[test]
fn typed_materialization_failure_is_original_and_does_not_decide_a_routed_request() {
    use novarocks_worker_protocol::{
        context::DomainRefusal,
        control::{ControlInput, ControlReplyBody},
    };
    let refusal = materialization_refusal();
    let AdmissionRefusal::Domain(outcome, failure) = refusal.clone() else {
        unreachable!()
    };
    let expected = DomainRefusal { outcome, failure };
    let mut f = ActiveControlFixture::new(4, 1);
    let route = f.request(2, 1, active_establish_input(f.ticket, 2));
    let mut context_core = core(1, 1);
    let finished = context_core
        .handle(
            at(0),
            ContextInput::MaterializationFinished {
                result: Err(refusal),
            },
            &mut f.port,
        )
        .unwrap();
    let (close_epoch, decision) = establishment_decision(&finished);
    assert_eq!(close_epoch, Generation::new(2).unwrap());
    assert_eq!(decision.outcome, Err(expected.clone()));
    let first = f
        .control
        .handle(
            at(0),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision,
            },
            &mut f.port,
        )
        .unwrap();
    assert_eq!(first.requests.len(), 1);
    assert_eq!(first.requests[0].request.token, InputId::new(1).unwrap());
    assert_eq!(first.requests[0].body, ControlReplyBody::Domain(expected));
    let duplicate = f
        .control
        .handle(
            at(0),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision: active_establishment_decision(),
            },
            &mut f.port,
        )
        .unwrap();
    assert!(duplicate.effects.is_empty());
    assert!(duplicate.requests.is_empty());
    let second = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        2,
        0,
        Ok(OperationOutcome::InvalidStateOrRequest),
    );
    assert_eq!(second.requests.len(), 1);
    assert_eq!(second.requests[0].request.token, InputId::new(2).unwrap());
    assert_eq!(
        second.requests[0].result,
        RequestResult::Outcome(OperationOutcome::InvalidStateOrRequest)
    );
    assert_eq!(f.control.route(context()), QueryContextState::Aborting);
}

#[test]
fn materialization_quiesce_completion_keeps_its_won_fence_until_a_tick_upgrades_it() {
    for failed in [false, true] {
        for now in [2, 30] {
            for tick_first in [false, true] {
                let mut context_core = core(1, 1);
                let mut port = Port::default();
                context_core
                    .apply_fixture(at(1), rpc(operation(2), ContextCommand::Quiesce), &mut port)
                    .unwrap();
                if tick_first {
                    context_core
                        .handle(at(now), ContextInput::Tick, &mut port)
                        .unwrap();
                }
                let finished = context_core
                    .handle(
                        at(now),
                        ContextInput::MaterializationFinished {
                            result: if failed {
                                Err(materialization_refusal())
                            } else {
                                Ok(ContextMaterialized {
                                    initial_credential_resource: 101,
                                })
                            },
                        },
                        &mut port,
                    )
                    .unwrap();
                let (_, decision) = establishment_decision(&finished);
                let expired = tick_first && now == 30;
                assert_eq!(
                    decision.state,
                    if expired {
                        QueryContextState::Aborting
                    } else {
                        QueryContextState::Quiescing
                    }
                );
                if expired && failed {
                    let AdmissionRefusal::Domain(outcome, failure) = materialization_refusal()
                    else {
                        unreachable!()
                    };
                    assert_eq!(
                        decision.outcome,
                        Err(novarocks_worker_protocol::context::DomainRefusal { outcome, failure })
                    );
                } else {
                    assert_eq!(
                        decision.outcome,
                        Ok(if expired {
                            OperationOutcome::LeaseExpired
                        } else {
                            OperationOutcome::ContextTerminalReceipt
                        })
                    );
                }
                assert!(!finished.effects.iter().any(|effect| matches!(
                    effect,
                    ContextEffect::EstablishmentDecided {
                        decision: ContextEstablishmentDecision {
                            state: QueryContextState::Active,
                            ..
                        },
                        ..
                    }
                )));
                if !expired {
                    // Finishing does not renew the lease. A following actual
                    // timer at its old deadline still upgrades the live fence.
                    assert_eq!(context_core.next_deadline(), Some(at(30)));
                    let tick = context_core
                        .handle(at(30), ContextInput::Tick, &mut port)
                        .unwrap();
                    assert!(tick.charged_work() <= ContextCore::MAX_STEP_WORK);
                    assert_eq!(context_core.state(), QueryContextState::Aborting);
                }
            }
        }
    }
}

#[test]
fn materialization_host_error_preserves_domain_facts_without_replacing_the_first_close_cause() {
    for expired in [false, true] {
        let mut context_core = core(1, 1);
        let mut port = Port::default();
        let now = if expired { 30 } else { 1 };
        if !expired {
            context_core
                .apply_fixture(
                    at(now),
                    rpc(
                        operation(2),
                        ContextCommand::Abort {
                            cause: AbortCause::QueryFailed,
                        },
                    ),
                    &mut port,
                )
                .unwrap();
        }
        let error = context_core
            .handle(
                at(now),
                ContextInput::MaterializationFinished {
                    result: Err(materialization_refusal()),
                },
                &mut port,
            )
            .unwrap();
        let (_, decision) = establishment_decision(&error);
        let AdmissionRefusal::Domain(outcome, failure) = materialization_refusal() else {
            unreachable!()
        };
        assert_eq!(
            decision.outcome,
            Err(novarocks_worker_protocol::context::DomainRefusal { outcome, failure })
        );
        assert_eq!(decision.state, QueryContextState::Aborting);
        let later = context_core
            .handle(
                at(31),
                ContextInput::MaterializationFinished {
                    result: Ok(ContextMaterialized {
                        initial_credential_resource: 101,
                    }),
                },
                &mut port,
            )
            .unwrap();
        assert_eq!(
            establishment_decision(&later).1.outcome,
            Ok(if expired {
                OperationOutcome::LeaseExpired
            } else {
                OperationOutcome::ContextTerminalReceipt
            })
        );
    }
}

#[test]
fn root_fence_blocks_late_active_without_erasing_typed_failure_or_lease_expiry() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput, ControlReplyBody};
    for (branch, scanned) in
        (0..3).flat_map(|branch| [false, true].map(|scanned| (branch, scanned)))
    {
        let mut f = ActiveControlFixture::new(4, 100);
        f.control
            .handle(
                at(0),
                ControlInput::FenceFrontend {
                    frontend: context().frontend_process_id(),
                },
                &mut f.port,
            )
            .unwrap();
        if scanned {
            f.control
                .handle(at(0), ControlInput::ContinueFence, &mut f.port)
                .unwrap();
        }
        let mut context_core = core(1, 1);
        let now = if branch == 2 { 30 } else { 0 };
        let finished = context_core
            .handle(
                at(now),
                ContextInput::MaterializationFinished {
                    result: if branch == 1 {
                        Err(materialization_refusal())
                    } else {
                        Ok(ContextMaterialized {
                            initial_credential_resource: 101,
                        })
                    },
                },
                &mut f.port,
            )
            .unwrap();
        let result = f
            .control
            .handle(
                at(now),
                ControlInput::EstablishmentDecided {
                    context: context(),
                    decision: establishment_decision(&finished).1,
                },
                &mut f.port,
            )
            .unwrap();
        assert_eq!(f.control.route(context()), QueryContextState::Aborting);
        assert_eq!(result.requests.len(), 1);
        assert_eq!(
            result.requests[0].result,
            RequestResult::Outcome(match branch {
                0 => OperationOutcome::ContextTerminalReceipt,
                1 => OperationOutcome::InvalidStateOrRequest,
                _ => OperationOutcome::LeaseExpired,
            })
        );
        assert_eq!(
            matches!(result.requests[0].body, ControlReplyBody::Domain(_)),
            branch == 1
        );
        assert!(!result.effects.iter().any(|effect| matches!(
            effect,
            ControlEffect::Route {
                state: QueryContextState::Active,
                ..
            }
        )));
        assert!(result.charged_work() <= f.control.max_step_work());
    }
}

fn release_while_materialization_is_outstanding(core: &mut ContextCore, port: &mut Port) {
    core.apply_fixture(at(0), rpc(operation(70), ContextCommand::Quiesce), port)
        .unwrap();
    core.handle(at(0), ContextInput::ContinueQuiesce, port)
        .unwrap();
    let releasing = core
        .apply_fixture(at(0), rpc(operation(71), ContextCommand::Release), port)
        .unwrap();
    assert_eq!(core.state(), QueryContextState::Releasing);
    assert!(
        releasing
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::ReleaseHost { .. }))
    );
}

#[test]
fn late_materialization_error_reports_actual_releasing_or_terminal_state_without_rollback() {
    use novarocks_worker_protocol::control::{ControlInput, ControlReplyBody};
    for host_released in [false, true] {
        let mut f = ActiveControlFixture::new(4, 100);
        let mut context_core = core(1, 1);
        release_while_materialization_is_outstanding(&mut context_core, &mut f.port);
        if host_released {
            // Logical host release is an environment fact. It does not assert
            // that the separate materialization Work/report has retired.
            context_core
                .handle(at(0), ContextInput::HostReleased, &mut f.port)
                .unwrap();
            f.control
                .handle(
                    at(0),
                    ControlInput::Retained { context: context() },
                    &mut f.port,
                )
                .unwrap();
        }
        let error = context_core
            .handle(
                at(1),
                ContextInput::MaterializationFinished {
                    result: Err(materialization_refusal()),
                },
                &mut f.port,
            )
            .unwrap();
        let (_, decision) = establishment_decision(&error);
        let state = if host_released {
            QueryContextState::TerminalRetained
        } else {
            QueryContextState::Releasing
        };
        assert_eq!(decision.state, state);
        assert_eq!(context_core.state(), state);
        assert!(!error.effects.iter().any(|effect| matches!(
            effect,
            ContextEffect::CloseExternalAdmission { .. } | ContextEffect::ReleaseHost { .. }
        )));
        let result = f
            .control
            .handle(
                at(1),
                ControlInput::EstablishmentDecided {
                    context: context(),
                    decision,
                },
                &mut f.port,
            )
            .unwrap();
        assert_eq!(f.control.route(context()), state);
        assert_eq!(result.requests.len(), 1);
        assert!(matches!(
            result.requests[0].body,
            ControlReplyBody::Domain(_)
        ));
        assert!(result.charged_work() <= f.control.max_step_work());
    }
}

#[test]
fn context_bootstrap_failure_keeps_the_original_reply_and_a_pending_route_is_independent() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput, ControlReplyBody};
    let mut f = ActiveControlFixture::new(4, 100);
    let route = f.request(2, 100, active_establish_input(f.ticket, 2));
    let calls = f.port.calls.len();
    let failure = f
        .control
        .handle(
            at(0),
            ControlInput::ContextBootstrapFailed {
                context: context(),
                refusal: materialization_refusal(),
            },
            &mut f.port,
        )
        .unwrap();
    assert_eq!(failure.requests.len(), 1);
    let original = failure.requests[0].body.clone();
    assert!(matches!(original, ControlReplyBody::Domain(_)));
    let duplicate = f
        .control
        .handle(
            at(0),
            ControlInput::ContextBootstrapFailed {
                context: context(),
                refusal: OperationOutcome::ResourceExhausted.into(),
            },
            &mut f.port,
        )
        .unwrap();
    assert!(duplicate.effects.is_empty());
    assert!(duplicate.requests.is_empty());
    assert!(f.flush().requests.is_empty());
    let refused = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        0,
        0,
        Ok(OperationOutcome::OwnerFailed),
    );
    assert_eq!(refused.requests[0].request.token, InputId::new(2).unwrap());
    assert_eq!(
        refused.requests[0].result,
        RequestResult::Outcome(OperationOutcome::OwnerFailed)
    );
    assert_eq!(failure.requests[0].body, original);
    assert_eq!(f.port.calls.len(), calls);
    assert!(!failure.effects.iter().any(|effect| matches!(
        effect,
        ControlEffect::Route {
            state: QueryContextState::Active,
            ..
        }
    )));
}

#[test]
fn a_routed_typed_decision_keeps_its_own_time_without_reusing_the_creation_body() {
    use novarocks_worker_protocol::{
        context::DomainRefusal,
        control::{ControlInput, ControlReplyBody},
    };
    let mut f = ActiveControlFixture::new(4, 1);
    let route = f.request(2, 1, active_establish_input(f.ticket, 2));
    let first = f.materialized();
    let AdmissionRefusal::Domain(outcome, failure) = materialization_refusal() else {
        unreachable!()
    };
    let expected = DomainRefusal { outcome, failure };
    let second = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        2,
        0,
        Err(expected.clone()),
    );
    assert_eq!(second.requests[0].request.token, InputId::new(2).unwrap());
    assert_eq!(second.requests[0].body, ControlReplyBody::Domain(expected));
    assert_eq!(
        first.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    assert!(
        f.control
            .handle(at(2), ControlInput::Tick, &mut f.port)
            .unwrap()
            .requests
            .is_empty()
    );
}

#[test]
fn terminal_establishment_and_retained_use_one_original_retirement_and_one_ttl_in_either_order() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    for retained_first in [false, true] {
        let mut f = ActiveControlFixture::new(4, 100);
        let mut context_core = core(1, 1);
        release_while_materialization_is_outstanding(&mut context_core, &mut f.port);
        context_core
            .handle(at(0), ContextInput::HostReleased, &mut f.port)
            .unwrap();
        let finished = context_core
            .handle(
                at(0),
                ContextInput::MaterializationFinished {
                    result: Err(materialization_refusal()),
                },
                &mut f.port,
            )
            .unwrap();
        let decision = establishment_decision(&finished).1;
        assert_eq!(decision.state, QueryContextState::TerminalRetained);
        let decided = ControlInput::EstablishmentDecided {
            context: context(),
            decision,
        };
        let retained = ControlInput::Retained { context: context() };
        let (first, second) = if retained_first {
            (retained, decided)
        } else {
            (decided, retained)
        };
        let first = f.control.handle(at(1), first, &mut f.port).unwrap();
        assert_eq!(
            f.control.route(context()),
            QueryContextState::TerminalRetained
        );
        assert_eq!(f.control.next_deadline(), Some(at(11)));
        let second = f.control.handle(at(2), second, &mut f.port).unwrap();
        assert_eq!(
            f.control.route(context()),
            QueryContextState::TerminalRetained
        );
        assert_eq!(
            f.control.next_deadline(),
            Some(at(11)),
            "the second fact cannot restart retention"
        );
        let effects: Vec<_> = first.effects.iter().chain(&second.effects).collect();
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(effect, ControlEffect::RetireReservation(_)))
                .count(),
            1
        );
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(
                    effect,
                    ControlEffect::Route {
                        state: QueryContextState::TerminalRetained,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert_eq!(first.requests.len() + second.requests.len(), 1);
        for step in [first, second] {
            assert!(
                step.charged_work() + usize::from(f.control.has_internal_work())
                    <= f.control.max_step_work()
            );
        }
        // The host has not supplied HostRemoved. No physical Work/report exit
        // follows from either of these terminal protocol inputs.
        let pending = f
            .control
            .handle(at(11), ControlInput::Tick, &mut f.port)
            .unwrap();
        assert!(pending.effects.iter().any(|effect| matches!(effect, ControlEffect::RemoveHost { context: original } if *original == context())));
        assert_eq!(
            f.control.route(context()),
            QueryContextState::TerminalRetained
        );
    }
}

#[test]
fn materialization_decisions_reject_impossible_shapes_and_unknown_contexts() {
    use novarocks_worker_protocol::{context::CoreError, control::ControlInput};
    let mut f = ActiveControlFixture::new(4, 100);
    let unknown = f
        .control
        .handle(
            at(0),
            ControlInput::EstablishmentDecided {
                context: another_control_context(),
                decision: active_establishment_decision(),
            },
            &mut f.port,
        )
        .unwrap_err();
    assert_eq!(unknown, CoreError::WrongIdentity);
    for (state, outcome) in [
        (
            QueryContextState::Establishing,
            Ok(OperationOutcome::Accepted),
        ),
        (
            QueryContextState::Active,
            Ok(OperationOutcome::ContextTerminalReceipt),
        ),
        (QueryContextState::Aborting, Ok(OperationOutcome::Accepted)),
    ] {
        assert_eq!(
            f.control
                .handle(
                    at(0),
                    ControlInput::EstablishmentDecided {
                        context: context(),
                        decision: ContextEstablishmentDecision { state, outcome },
                    },
                    &mut f.port
                )
                .unwrap_err(),
            CoreError::IllegalFact
        );
        assert_eq!(f.control.route(context()), QueryContextState::Establishing);
    }
    assert_eq!(
        f.control
            .handle(
                at(0),
                ControlInput::ContextBootstrapFailed {
                    context: context(),
                    refusal: OperationOutcome::Accepted.into(),
                },
                &mut f.port
            )
            .unwrap_err(),
        CoreError::IllegalFact
    );
    f.finish_original();
}

#[test]
fn a_quiesced_context_does_not_accept_a_success_disguised_as_host_refusal() {
    use novarocks_worker_protocol::context::CoreError;
    for outcome in [OperationOutcome::Accepted, OperationOutcome::Idempotent] {
        let mut context_core = core(1, 1);
        let mut port = Port::default();
        context_core
            .apply_fixture(at(1), rpc(operation(2), ContextCommand::Quiesce), &mut port)
            .unwrap();
        assert_eq!(
            context_core
                .handle(
                    at(2),
                    ContextInput::MaterializationFinished {
                        result: Err(AdmissionRefusal::Verdict(outcome)),
                    },
                    &mut port
                )
                .unwrap_err(),
            CoreError::IllegalFact
        );
        assert_eq!(context_core.state(), QueryContextState::Quiescing);
        assert!(port.calls.is_empty());
    }
}

#[test]
fn retained_state_without_a_completion_does_not_backdate_the_typed_establishment_decision() {
    use novarocks_worker_protocol::control::ControlInput;
    let mut f = ActiveControlFixture::new(4, 1);
    let retained = f
        .control
        .handle(
            at(0),
            ControlInput::Retained { context: context() },
            &mut f.port,
        )
        .unwrap();
    assert!(retained.requests.is_empty());
    let mut context_core = core(1, 1);
    release_while_materialization_is_outstanding(&mut context_core, &mut f.port);
    context_core
        .handle(at(0), ContextInput::HostReleased, &mut f.port)
        .unwrap();
    let finished = context_core
        .handle(
            at(0),
            ContextInput::MaterializationFinished {
                result: Err(materialization_refusal()),
            },
            &mut f.port,
        )
        .unwrap();
    let late = f
        .control
        .handle(
            at(2),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision: establishment_decision(&finished).1,
            },
            &mut f.port,
        )
        .unwrap();
    assert_eq!(late.requests.len(), 1);
    assert_eq!(
        late.requests[0].result,
        RequestResult::TimeoutWhileAccepted,
        "Root retained state at zero was not yet an operation decision"
    );
    assert_eq!(
        f.control.route(context()),
        QueryContextState::TerminalRetained
    );
    assert!(late.charged_work() <= f.control.max_step_work());
}

fn creation_and_routed_request_with_deadlines(first: u64, second: u64) -> ActiveControlFixture {
    use novarocks_worker_protocol::control::ControlInput;
    let mut f = ActiveControlFixture::new(8, first);
    let mut input = active_establish_input(f.ticket, 2);
    let ControlInput::Establish { request, .. } = &mut input else {
        unreachable!()
    };
    *request = operation(3);
    let route = f
        .control
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(2).unwrap(),
                operation: operation(3),
                deadline: at(second),
            },
            input,
            &mut f.port,
        )
        .unwrap();
    routed_establish_request(&route);
    f
}

fn terminal_typed_establishment_decision() -> ContextEstablishmentDecision {
    use novarocks_worker_protocol::context::DomainRefusal;
    let AdmissionRefusal::Domain(outcome, failure) = materialization_refusal() else {
        unreachable!()
    };
    ContextEstablishmentDecision {
        state: QueryContextState::TerminalRetained,
        outcome: Err(DomainRefusal { outcome, failure }),
    }
}

#[test]
fn retained_host_removal_preserves_an_independent_routed_reply_without_restarting_retention() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput, ControlReplyBody};
    let mut f = ActiveControlFixture::new(8, 100);
    let route = f.request(2, 100, active_establish_input(f.ticket, 2));
    let first = f
        .control
        .handle(
            at(0),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision: terminal_typed_establishment_decision(),
            },
            &mut f.port,
        )
        .unwrap();
    assert_eq!(first.requests.len(), 1);
    assert!(matches!(
        first.requests[0].body,
        ControlReplyBody::Domain(_)
    ));
    let due = f
        .control
        .handle(at(10), ControlInput::Tick, &mut f.port)
        .unwrap();
    assert_eq!(due.effects.iter().filter(|effect|matches!(effect,ControlEffect::RemoveHost{context:target} if *target==context())).count(),1);
    // The model reports actual Context physical removal only after its own
    // route leg exited; a Root backchannel may still be awaiting delivery.
    let gone = f
        .control
        .handle(
            at(10),
            ControlInput::HostRemoved { context: context() },
            &mut f.port,
        )
        .unwrap();
    assert_eq!(f.control.route(context()), QueryContextState::Gone);
    let second = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        11,
        1,
        Ok(OperationOutcome::ContextTerminalReceipt),
    );
    assert_eq!(second.requests[0].request.token, InputId::new(2).unwrap());
    assert_eq!(
        second.requests[0].result,
        RequestResult::Outcome(OperationOutcome::ContextTerminalReceipt)
    );
    assert!(matches!(
        first.requests[0].body,
        ControlReplyBody::Domain(_)
    ));
    for step in [first, due, gone, second] {
        assert!(step.charged_work() <= f.control.max_step_work());
    }
}

#[test]
fn an_expired_new_r_cannot_copy_a_creation_domain_body_or_consume_another_route() {
    use novarocks_worker_protocol::control::{ControlInput, ControlReplyBody};
    let mut f = ActiveControlFixture::new(8, 100);
    let route = f.request(2, 100, active_establish_input(f.ticket, 2));
    let first = f
        .control
        .handle(
            at(0),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision: terminal_typed_establishment_decision(),
            },
            &mut f.port,
        )
        .unwrap();
    let expired = f
        .control
        .handle_request(
            at(2),
            RequestContext {
                token: InputId::new(3).unwrap(),
                operation: operation(2),
                deadline: at(1),
            },
            active_establish_input(f.ticket, 3),
            &mut f.port,
        )
        .unwrap();
    assert_eq!(expired.requests.len(), 1);
    assert_eq!(expired.requests[0].request.token, InputId::new(3).unwrap());
    assert_eq!(
        expired.requests[0].result,
        RequestResult::TimeoutWhileAccepted
    );
    assert!(expired.effects.is_empty());
    let original = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        2,
        0,
        Ok(OperationOutcome::ContextTerminalReceipt),
    );
    assert_eq!(original.requests[0].request.token, InputId::new(2).unwrap());
    assert_eq!(
        original.requests[0].result,
        RequestResult::Outcome(OperationOutcome::ContextTerminalReceipt)
    );
    assert!(matches!(
        first.requests[0].body,
        ControlReplyBody::Domain(_)
    ));
}

#[test]
fn pending_retained_removal_and_a_routed_deadline_keep_separate_obligations() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut f = ActiveControlFixture::new(8, 100);
    let route = f.request(2, 11, active_establish_input(f.ticket, 2));
    let original = routed_establish_request(&route).1.request;
    f.control
        .handle(
            at(0),
            ControlInput::Retained { context: context() },
            &mut f.port,
        )
        .unwrap();
    let due = f
        .control
        .handle(at(10), ControlInput::Tick, &mut f.port)
        .unwrap();
    assert!(
        !due.effects
            .iter()
            .any(|effect| matches!(effect, ControlEffect::RemoveHost { .. }))
    );
    assert!(!f.control.has_internal_work());
    let decided = f
        .control
        .handle(
            at(11),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision: terminal_typed_establishment_decision(),
            },
            &mut f.port,
        )
        .unwrap();
    assert_eq!(decided.requests[0].request.token, InputId::new(1).unwrap());
    assert_eq!(decided.effects.iter().filter(|effect|matches!(effect,ControlEffect::RemoveHost{context:target} if *target==context())).count(),1);
    let timed = f
        .control
        .handle(at(11), ControlInput::Tick, &mut f.port)
        .unwrap();
    assert_eq!(timed.requests[0].request, original);
    assert_eq!(
        timed.requests[0].result,
        RequestResult::TimeoutWhileAccepted
    );
    assert!(timed.effects.iter().any(|effect|matches!(effect,ControlEffect::CancelEstablishRoute{request,..} if *request==original)));
    assert!(
        !timed
            .effects
            .iter()
            .any(|effect| matches!(effect, ControlEffect::RemoveHost { .. }))
    );
    assert!(
        f.control
            .next_deadline()
            .is_none_or(|deadline| deadline > at(21)),
        "neither M nor routed timeout restarts TTL"
    );
    let idle = f
        .control
        .handle(at(11), ControlInput::NextInternal, &mut f.port)
        .unwrap();
    assert!(idle.effects.is_empty());
    let late = finish_routed_establish(
        &mut f.control,
        &mut f.port,
        &route,
        11,
        10,
        Ok(OperationOutcome::ContextTerminalReceipt),
    );
    assert!(late.requests.is_empty());
}

#[test]
fn retained_capacity_eviction_preserves_a_routed_result_and_the_new_context_ttl() {
    use novarocks_worker_protocol::control::{
        ControlConfig, ControlCore, ControlEffect, ControlInput, ControlReplyBody,
    };
    let mut control = ControlCore::new(
        ControlConfig {
            max_contexts: 2,
            max_waiters: 8,
            max_pending_requests: 8,
            step_budget: ControlCore::MAX_STEP_WORK,
            retained_contexts: 1,
            gone_fences: 8,
            retention: Duration::from_secs(10),
        },
        AdmissionTicketConfig::DEFAULT,
        [1; 8],
        at(0),
    )
    .unwrap();
    let mut port = Port::default();
    let old = context();
    let new = another_control_context();
    let mut routed = None;
    for (identity, base, tokens) in [(old, 70, vec![(1, 71), (2, 72)]), (new, 80, vec![(3, 81)])] {
        let ticket = selector_grant(&mut control, &mut port, identity, base, 5);
        for (token, op) in tokens {
            let accepted = control
                .handle_request(
                    at(0),
                    RequestContext {
                        token: InputId::new(token).unwrap(),
                        operation: operation(op),
                        deadline: at(100),
                    },
                    ControlInput::Establish {
                        request: operation(op),
                        context: identity,
                        ticket,
                        payload: PayloadId(u64::from(base)),
                        semantics: semantics(),
                        generation: Generation::new(1).unwrap(),
                    },
                    &mut port,
                )
                .unwrap();
            assert!(accepted.requests.is_empty());
            assert!(accepted.charged_work() <= control.max_step_work());
            if token == 2 {
                routed_establish_request(&accepted);
                routed = Some(accepted);
            }
        }
    }
    let calls = port.calls.len();
    let first = control
        .handle(
            at(0),
            ControlInput::EstablishmentDecided {
                context: old,
                decision: terminal_typed_establishment_decision(),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(first.requests[0].request.token, InputId::new(1).unwrap());
    assert!(matches!(
        first.requests[0].body,
        ControlReplyBody::Domain(_)
    ));
    let evicted = control
        .handle(
            at(1),
            ControlInput::EstablishmentDecided {
                context: new,
                decision: terminal_typed_establishment_decision(),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(evicted.requests[0].request.token, InputId::new(3).unwrap());
    assert_eq!(
        evicted
            .effects
            .iter()
            .filter(|effect| matches!(effect,ControlEffect::RemoveHost{context} if *context==old))
            .count(),
        1
    );
    assert_eq!(control.next_deadline(), Some(at(11)));
    control
        .handle(at(1), ControlInput::HostRemoved { context: old }, &mut port)
        .unwrap();
    let tail = finish_routed_establish(
        &mut control,
        &mut port,
        &routed.unwrap(),
        2,
        1,
        Ok(OperationOutcome::ContextTerminalReceipt),
    );
    assert_eq!(tail.requests[0].request.token, InputId::new(2).unwrap());
    assert_eq!(
        tail.requests[0].result,
        RequestResult::Outcome(OperationOutcome::ContextTerminalReceipt)
    );
    assert_eq!(control.route(old), QueryContextState::Gone);
    assert_eq!(control.route(new), QueryContextState::TerminalRetained);
    assert_eq!(control.next_deadline(), Some(at(11)));
    let before = control
        .handle(at(10), ControlInput::Tick, &mut port)
        .unwrap();
    assert!(
        !before
            .effects
            .iter()
            .any(|effect| matches!(effect, ControlEffect::RemoveHost { .. }))
    );
    let due = control
        .handle(at(11), ControlInput::Tick, &mut port)
        .unwrap();
    assert_eq!(
        due.effects
            .iter()
            .filter(|effect| matches!(effect,ControlEffect::RemoveHost{context} if *context==new))
            .count(),
        1
    );
    assert_eq!(port.calls.len(), calls);
    for step in [first, evicted, tail, before, due] {
        assert!(step.charged_work() <= control.max_step_work());
    }
}

#[test]
fn a_retained_directory_with_no_waiters_still_waits_for_its_real_materialization_decision() {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut f = creation_and_routed_request_with_deadlines(1, 1);
    let retained = f
        .control
        .handle(
            at(0),
            ControlInput::Retained { context: context() },
            &mut f.port,
        )
        .unwrap();
    let first_timeout = f
        .control
        .handle(at(1), ControlInput::Tick, &mut f.port)
        .unwrap();
    let last_timeout = f
        .control
        .handle(at(1), ControlInput::Tick, &mut f.port)
        .unwrap();
    assert_eq!(first_timeout.requests.len(), 1);
    assert_eq!(last_timeout.requests.len(), 1);
    assert_eq!(
        first_timeout.requests[0].request.token,
        InputId::new(1).unwrap()
    );
    assert_eq!(
        last_timeout.requests[0].request.token,
        InputId::new(2).unwrap()
    );
    for result in [&first_timeout.requests[0], &last_timeout.requests[0]] {
        assert_eq!(result.result, RequestResult::TimeoutWhileAccepted);
    }
    let due = f
        .control
        .handle(at(10), ControlInput::Tick, &mut f.port)
        .unwrap();
    assert!(
        !due.effects
            .iter()
            .any(|effect| matches!(effect, ControlEffect::RemoveHost { .. })),
        "timeout is not a Context materialization completion"
    );
    assert!(!f.control.has_internal_work());
    for _ in 0..2 {
        let idle = f
            .control
            .handle(at(10), ControlInput::NextInternal, &mut f.port)
            .unwrap();
        assert!(idle.effects.is_empty());
        assert!(idle.requests.is_empty());
        assert!(
            !f.control.has_internal_work(),
            "unknown completion must not self-spin"
        );
        assert!(idle.charged_work() <= f.control.max_step_work());
    }
    let decided = f
        .control
        .handle(
            at(11),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision: terminal_typed_establishment_decision(),
            },
            &mut f.port,
        )
        .unwrap();
    assert!(
        decided.requests.is_empty(),
        "both original RPC roles already timed out"
    );
    assert_eq!(decided.effects.iter().filter(|effect| matches!(effect, ControlEffect::RemoveHost { context: original } if *original == context())).count(), 1);
    assert!(!f.control.has_internal_work());
    let idle = f
        .control
        .handle(at(11), ControlInput::NextInternal, &mut f.port)
        .unwrap();
    assert!(idle.effects.is_empty());
    let gone = f
        .control
        .handle(
            at(11),
            ControlInput::HostRemoved { context: context() },
            &mut f.port,
        )
        .unwrap();
    assert_eq!(f.control.route(context()), QueryContextState::Gone);
    for step in [
        retained,
        first_timeout,
        last_timeout,
        due,
        decided,
        idle,
        gone,
    ] {
        assert!(
            step.charged_work() + usize::from(f.control.has_internal_work())
                <= f.control.max_step_work()
        );
    }
}

#[test]
fn an_ingress_owner_failure_settles_only_its_original_token_without_business_admission() {
    use novarocks_worker_protocol::control::{ControlInput, ControlReplyBody};
    let mut control = control();
    let mut port = Port::default();
    control
        .handle(
            at(0),
            ControlInput::FenceFrontend {
                frontend: context().frontend_process_id(),
            },
            &mut port,
        )
        .unwrap();
    control
        .handle(at(0), ControlInput::NextInternal, &mut port)
        .unwrap();
    let request = RequestContext {
        token: InputId::new(1).unwrap(),
        operation: operation(90),
        deadline: at(0),
    };
    port.reject = Some(OperationOutcome::ResourceExhausted);
    let failed = control
        .handle_request(
            at(5),
            request,
            ControlInput::IngressOwnerFailed {
                request: request.operation,
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(failed.records_visited, 1);
    assert!(failed.effects.is_empty());
    assert_eq!(failed.requests.len(), 1);
    assert_eq!(failed.requests[0].request, request);
    assert_eq!(
        failed.requests[0].body,
        ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed))
    );
    assert!(port.calls.is_empty());
    assert!(!control.has_internal_work());
    assert_eq!(control.route(context()), QueryContextState::Absent);

    // The failed ingress did not leave an operation shape or directory row.
    // This other unfenced context can use the same operation for a genuine
    // independently paid acquisition, rather than joining a phantom failure.
    port.reject = None;
    let other = selector_context(15001, 2);
    let acquire = AcquireQueryContextAdmissionTicket::new(
        request.operation,
        other,
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        control.admission().capability(),
    );
    let accepted = control
        .handle_request(
            at(5),
            RequestContext {
                token: InputId::new(2).unwrap(),
                operation: request.operation,
                deadline: at(100),
            },
            ControlInput::Acquire {
                request: acquire,
                ticket: AdmissionTicketId::try_from_bytes([90; 16]).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(accepted.requests.len(), 1);
    assert!(matches!(
        accepted.requests[0].body,
        ControlReplyBody::Admission(Ok(_))
    ));
    assert_eq!(port.calls.len(), 1);
    assert!(failed.charged_work() <= control.max_step_work());
    assert!(accepted.charged_work() <= control.max_step_work());
}

#[test]
fn an_ingress_failure_cannot_consume_an_original_creation_or_independent_routed_result() {
    use novarocks_worker_protocol::control::{ControlInput, ControlReplyBody};
    for phase in 0..3 {
        let mut f = ActiveControlFixture::new(8, 100);
        let route = f.request(2, 1, active_establish_input(f.ticket, 2));
        let mut first = if phase == 0 {
            None
        } else {
            Some(
                f.control
                    .handle(
                        at(0),
                        ControlInput::EstablishmentDecided {
                            context: context(),
                            decision: terminal_typed_establishment_decision(),
                        },
                        &mut f.port,
                    )
                    .unwrap(),
            )
        };
        let calls = f.port.calls.len();
        let failed = f
            .control
            .handle_request(
                at(2),
                RequestContext {
                    token: InputId::new(3).unwrap(),
                    operation: operation(2),
                    deadline: at(0),
                },
                ControlInput::IngressOwnerFailed {
                    request: operation(2),
                },
                &mut f.port,
            )
            .unwrap();
        assert_eq!(failed.records_visited, 1);
        assert!(failed.effects.is_empty());
        assert_eq!(failed.requests.len(), 1);
        assert_eq!(failed.requests[0].request.token, InputId::new(3).unwrap());
        assert_eq!(
            failed.requests[0].body,
            ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::OwnerFailed))
        );
        assert_eq!(f.port.calls.len(), calls);
        if phase == 0 {
            assert_eq!(f.control.route(context()), QueryContextState::Establishing);
            first = Some(
                f.control
                    .handle(
                        at(2),
                        ControlInput::EstablishmentDecided {
                            context: context(),
                            decision: active_establishment_decision(),
                        },
                        &mut f.port,
                    )
                    .unwrap(),
            );
        }
        let first = first.unwrap();
        assert_eq!(first.requests.len(), 1);
        assert_eq!(first.requests[0].request.token, InputId::new(1).unwrap());
        let original_body = first.requests[0].body.clone();
        let routed_result = if phase == 0 {
            Ok(OperationOutcome::Idempotent)
        } else if phase == 1 {
            let AdmissionRefusal::Domain(outcome, failure) = materialization_refusal() else {
                unreachable!()
            };
            Err(novarocks_worker_protocol::context::DomainRefusal { outcome, failure })
        } else {
            Ok(OperationOutcome::ContextTerminalReceipt)
        };
        let second = finish_routed_establish(
            &mut f.control,
            &mut f.port,
            &route,
            2,
            0,
            routed_result.clone(),
        );
        assert_eq!(second.requests.len(), 1);
        assert_eq!(second.requests[0].request.token, InputId::new(2).unwrap());
        let expected = match routed_result {
            Ok(outcome) => ControlReplyBody::Verdict(RequestResult::Outcome(outcome)),
            Err(refusal) => ControlReplyBody::Domain(refusal),
        };
        assert_eq!(second.requests[0].body, expected);
        assert_eq!(first.requests[0].body, original_body);
        assert!(
            f.control
                .handle(at(2), ControlInput::Tick, &mut f.port)
                .unwrap()
                .requests
                .is_empty()
        );
    }
}

#[test]
fn an_ingress_failure_requires_a_valid_unique_rpc_token_and_monotonic_time() {
    use novarocks_worker_protocol::{context::CoreError, control::ControlInput};
    let mut f = ActiveControlFixture::new(4, 100);
    let calls = f.port.calls.len();
    assert_eq!(
        f.control
            .handle(
                at(0),
                ControlInput::IngressOwnerFailed {
                    request: operation(2),
                },
                &mut f.port
            )
            .unwrap_err(),
        CoreError::WrongIdentity
    );
    for (token, candidate_operation) in [(1, operation(2)), (2, operation(3))] {
        assert_eq!(
            f.control
                .handle_request(
                    at(0),
                    RequestContext {
                        token: InputId::new(token).unwrap(),
                        operation: candidate_operation,
                        deadline: at(100),
                    },
                    ControlInput::IngressOwnerFailed {
                        request: operation(2)
                    },
                    &mut f.port
                )
                .unwrap_err(),
            CoreError::WrongIdentity
        );
    }
    let failed = f
        .control
        .handle_request(
            at(1),
            RequestContext {
                token: InputId::new(2).unwrap(),
                operation: operation(2),
                deadline: at(0),
            },
            ControlInput::IngressOwnerFailed {
                request: operation(2),
            },
            &mut f.port,
        )
        .unwrap();
    assert_eq!(failed.requests[0].request.token, InputId::new(2).unwrap());
    assert_eq!(
        f.control
            .handle_request(
                at(0),
                RequestContext {
                    token: InputId::new(3).unwrap(),
                    operation: operation(2),
                    deadline: at(100),
                },
                ControlInput::IngressOwnerFailed {
                    request: operation(2)
                },
                &mut f.port
            )
            .unwrap_err(),
        CoreError::TimeRegressed
    );
    assert_eq!(f.port.calls.len(), calls);
    let original = f
        .control
        .handle(
            at(1),
            ControlInput::EstablishmentDecided {
                context: context(),
                decision: active_establishment_decision(),
            },
            &mut f.port,
        )
        .unwrap();
    assert_eq!(original.requests.len(), 1);
    assert_eq!(original.requests[0].request.token, InputId::new(1).unwrap());
    assert_eq!(
        original.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
}

#[test]
fn an_rpc_transport_wrapper_cannot_bypass_the_actual_request_envelope() {
    use novarocks_worker_protocol::context::CoreError;
    let mut core = core(4, 1);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    let command = || ContextCommand::Create {
        task: task(1),
        payload: PayloadId(1),
    };
    let refused = core.handle(at(0), ContextInput::Request(command()), &mut port);
    assert_eq!(refused.unwrap_err(), CoreError::WrongIdentity);
    assert!(port.calls.is_empty());
    assert!(core.task(task(1)).is_none());
    let request = RequestContext {
        token: InputId::new(71).unwrap(),
        operation: operation(9),
        deadline: at(100),
    };
    let admitted = core
        .handle_request(at(0), request, command(), &mut port)
        .unwrap();
    assert!(admitted.requests.is_empty());
    assert_eq!(port.calls.len(), 1);
    let loaded = core
        .handle(
            at(0),
            ContextInput::Loaded {
                task: task(1),
                result: Ok(cost()),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(loaded.requests.len(), 1);
    assert_eq!(loaded.requests[0].request, request);
    assert_eq!(loaded.requests[0].task, Some(task(1)));
    assert!(loaded.requests[0].body.matches(loaded.requests[0].result));
}

#[test]
fn the_same_wire_operation_does_not_join_distinct_tasks_or_replace_a_settled_body() {
    let mut core = core(4, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    let requests = [
        RequestContext {
            token: InputId::new(81).unwrap(),
            operation: operation(9),
            deadline: at(100),
        },
        RequestContext {
            token: InputId::new(82).unwrap(),
            operation: operation(9),
            deadline: at(100),
        },
    ];
    for (index, request) in requests.into_iter().enumerate() {
        let n = index as u32 + 1;
        core.handle_request(
            at(0),
            request,
            ContextCommand::Create {
                task: task(n),
                payload: PayloadId(u64::from(n)),
            },
            &mut port,
        )
        .unwrap();
    }
    assert_eq!(
        port.calls.len(),
        2,
        "each distinct entity owns its original loader"
    );
    let first = core
        .handle(
            at(0),
            ContextInput::Loaded {
                task: task(1),
                result: Ok(cost()),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(first.requests.len(), 1);
    assert_eq!(first.requests[0].request, requests[0]);
    assert_eq!(first.requests[0].task, Some(task(1)));
    let original_body = first.requests[0].body.clone();
    let second = core
        .handle(
            at(0),
            ContextInput::Loaded {
                task: task(2),
                result: Err(OperationOutcome::ResourceExhausted.into()),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(second.requests.len(), 1);
    assert_eq!(second.requests[0].request, requests[1]);
    assert_eq!(second.requests[0].task, Some(task(2)));
    assert_eq!(
        first.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    assert_eq!(
        second.requests[0].result,
        RequestResult::Outcome(OperationOutcome::ResourceExhausted)
    );
    assert!(std::sync::Arc::ptr_eq(
        &original_body,
        &first.requests[0].body
    ));
    assert!(!std::sync::Arc::ptr_eq(
        &original_body,
        &second.requests[0].body
    ));
    for reply in [&first.requests[0], &second.requests[0]] {
        assert!(reply.body.matches(reply.result));
    }
}
