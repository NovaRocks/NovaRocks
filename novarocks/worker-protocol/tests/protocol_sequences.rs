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
        ContextConfig, ContextCore, ContextEffect, ContextInput, PhysicalFact, RequestResult,
    },
    creation::{AdmissionNeed, AdmissionPort, CreationCost, PayloadId, RequestContext},
    lease::{InstalledLease, LeaseBounds, MonotonicInstant},
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
    ContextCore::new(
        context(),
        Generation::new(1).unwrap(),
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
        at(0),
        InstalledLease::install_initial(
            LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
            LeaseBounds::DEFAULT,
            at(0),
        ),
    )
    .unwrap()
}
#[derive(Default)]
struct Port {
    calls: Vec<AdmissionNeed>,
    next: u64,
    reject: Option<OperationOutcome>,
}
impl AdmissionPort for Port {
    fn try_reserve(&mut self, need: AdmissionNeed) -> Result<ReservationId, OperationOutcome> {
        self.calls.push(need);
        if let Some(reject) = self.reject {
            return Err(reject);
        }
        self.next += 1;
        Ok(ReservationId::new(self.next).unwrap())
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
    core.handle(at(0), ContextInput::Established, port).unwrap();
}
fn create(core: &mut ContextCore, port: &mut Port, n: u8) {
    core.handle(
        at(0),
        ContextInput::Create {
            request: operation(n),
            task: task(n.into()),
            payload: PayloadId(n.into()),
        },
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
fn exact_create_replays_share_one_loader_and_reserve_the_complete_preparation_cost() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    create(&mut core, &mut port, 1);
    core.handle(
        at(0),
        ContextInput::Create {
            request: operation(2),
            task: task(1),
            payload: PayloadId(999),
        },
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
    core.handle(
        at(0),
        ContextInput::Abort {
            request: operation(4),
            cause: AbortCause::QueryFailed,
        },
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
    core.handle(
        at(0),
        ContextInput::Quiesce {
            request: operation(6),
        },
        &mut port,
    )
    .unwrap();
    core.handle(at(0), ContextInput::ContinueQuiesce, &mut port)
        .unwrap();
    let refused = core
        .handle(
            at(0),
            ContextInput::Release {
                request: operation(7),
            },
            &mut port,
        )
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
        .handle(
            at(0),
            ContextInput::Release {
                request: operation(7),
            },
            &mut port,
        )
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
    assert!(released.effects.iter().any(|e|matches!(e,ContextEffect::Reply{request,outcome:OperationOutcome::Accepted} if *request==operation(7))));
}
#[test]
fn current_input_time_expires_a_lease_before_a_delayed_timer_arrives() {
    let mut core = core(8, 1);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    let step = core
        .handle(
            at(30),
            ContextInput::Renew {
                request: operation(7),
                sequence: LeaseSequence::new(1),
                valid_for: LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
            },
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
            ContextInput::Create {
                request: operation(1),
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
fn a_late_request_never_starts_work_and_an_exact_pending_replay_does_not_duplicate_loader() {
    let mut core = core(8, 2);
    let mut port = Port::default();
    activate(&mut core, &mut port);
    let command = || ContextInput::Create {
        request: operation(1),
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
            ContextInput::ContinueRequestReplies {
                operation: operation(1),
            },
            &mut port,
        )
        .unwrap();
    let mut replies = ready.requests;
    replies.extend(more.requests);
    assert_eq!(replies.len(), 2);
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
        vec![ContextEffect::DisposeSource(SourceId(2))]
    );
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
    let command = |operation_id| ContextInput::Create {
        request: operation(operation_id),
        task: task(1),
        payload: PayloadId(99),
    };
    let creating = core
        .handle_request(at(1), request(1, 2), command(2), &mut port)
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
        .handle_request(at(1), request(2, 3), command(3), &mut port)
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
    core.handle(
        at(0),
        ContextInput::Abort {
            request: operation(8),
            cause: AbortCause::QueryFailed,
        },
        &mut port,
    )
    .unwrap();
    let scheduled = core
        .handle(at(0), ContextInput::ContinueClose, &mut port)
        .unwrap();
    assert!(
        scheduled
            .effects
            .iter()
            .any(|e| matches!(e, ContextEffect::ApplyTaskClose { .. }))
    );
    let due = MonotonicInstant::from_origin(Duration::from_millis(100));
    assert_eq!(core.next_deadline(), Some(due));
    let tick = core.handle(due, ContextInput::Tick, &mut port).unwrap();
    assert!(
        tick.effects
            .iter()
            .any(|e| matches!(e, ContextEffect::PublishMetrics { .. }))
    );
    assert!(core.next_deadline().is_none());
    assert_eq!(
        core.task(task(1)).unwrap().applied_close_generation,
        Generation::new(1).unwrap()
    );
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
    core.handle(
        at(0),
        ContextInput::Cancel {
            request: operation(2),
            task: task(1),
            reason: CancelReason::UpstreamNoLongerNeeded,
        },
        &mut port,
    )
    .unwrap();
    core.handle(at(0), ContextInput::Finish { task: task(1) }, &mut port)
        .unwrap();
    let final_step = core
        .handle(
            at(0),
            ContextInput::GetFinalInfo {
                request: operation(3),
                task: task(1),
            },
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
    use novarocks_worker_protocol::control::{ControlConfig, ControlCore};
    ControlCore::new(
        ControlConfig {
            max_contexts: 8,
            max_waiters: 8,
            max_pending_requests: 16,
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
                compared_payload: None,
                same_credential: false,
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
            ControlInput::Materialized {
                context: context(),
                result: Ok(()),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(control.route(context()), QueryContextState::Active);
    assert!(materialized.requests.is_empty());
    assert_eq!(port.calls.len(), 2);
}
#[test]
fn frontend_fence_cannot_be_undone_by_late_materialization_and_replay_compares_exact_establish_facts()
 {
    use novarocks_worker_protocol::control::{ControlEffect, ControlInput};
    let mut control = control();
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
                compared_payload: None,
                same_credential: false,
                generation: Generation::new(1).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    // A semantic replay mismatch is fatal even if the first request is still materializing.
    let mut conflict = semantics();
    conflict.query_options = novarocks_execution_contract::ContentFingerprint::from_bytes([9; 16]);
    let refused = control
        .handle(
            at(0),
            ControlInput::Establish {
                request: operation(3),
                context: context(),
                ticket,
                payload: PayloadId(2),
                semantics: conflict,
                compared_payload: Some(PayloadId(1)),
                same_credential: true,
                generation: Generation::new(1).unwrap(),
            },
            &mut port,
        )
        .unwrap();
    assert!(refused.effects.iter().any(|effect| matches!(
        effect,
        ControlEffect::Reply {
            outcome: OperationOutcome::ContextConflict,
            ..
        }
    )));
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
        .handle(at(0), ControlInput::ContinueFence, &mut port)
        .unwrap();
    assert_eq!(control.route(context()), QueryContextState::Aborting);
    control
        .handle(
            at(0),
            ControlInput::Materialized {
                context: context(),
                result: Ok(()),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(control.route(context()), QueryContextState::Aborting);
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
            .handle(
                at(0),
                ContextInput::Abort {
                    request: operation(5),
                    cause: AbortCause::QueryFailed,
                },
                &mut port,
            )
            .unwrap();
        assert!(!begin.effects.iter().any(|effect| matches!(effect, ContextEffect::Reply { request, .. } if *request == operation(5))));
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
            .handle(
                at(0),
                ContextInput::Abort {
                    request: operation(6),
                    cause: AbortCause::QueryFailed,
                },
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
            request: operation(5),
            outcome: OperationOutcome::Accepted
        }));
        assert!(!core.task(task(1)).unwrap().loader_exited);
        assert!(!core.task(task(1)).unwrap().actual_stopped);
        let replay = core
            .handle(at(0), ContextInput::ContinueClose, &mut port)
            .unwrap();
        assert!(replay.effects.contains(&ContextEffect::Reply {
            request: operation(6),
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
    core.handle(
        at(0),
        ContextInput::Abort {
            request: operation(5),
            cause: AbortCause::QueryFailed,
        },
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
    core.handle(
        at(0),
        ContextInput::Abort {
            request: operation(5),
            cause: AbortCause::QueryFailed,
        },
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
    assert!(
        !closed
            .effects
            .contains(&ContextEffect::DisposeSource(SourceId(17)))
    );
    assert!(closed.effects.contains(&ContextEffect::Reply {
        request: operation(5),
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
        ContextInput::Abort {
            request: operation(4),
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
                ContextInput::Abort {
                    request: op,
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
            ContextInput::Abort {
                request: operation(200),
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
        ContextInput::Create {
            request: operation(1),
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
                ContextInput::Create {
                    request: op,
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
        ContextInput::Create {
            request: operation(50),
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
fn expired_establish_reply_positions_do_not_fill_the_waiter_bound_or_revoke_materialization() {
    use novarocks_worker_protocol::control::ControlInput;
    let mut control = control();
    let mut port = Port::default();
    let ticket = grant(&mut control, &mut port);
    let establish = |op: TaskOperationId, replay: bool| ControlInput::Establish {
        request: op,
        context: context(),
        ticket,
        payload: PayloadId(if replay { 99 } else { 1 }),
        semantics: semantics(),
        compared_payload: replay.then_some(PayloadId(1)),
        same_credential: replay,
        generation: Generation::new(1).unwrap(),
    };
    control
        .handle_request(
            at(0),
            RequestContext {
                token: InputId::new(1).unwrap(),
                operation: operation(2),
                deadline: at(1),
            },
            establish(operation(2), false),
            &mut port,
        )
        .unwrap();
    control
        .handle(at(1), ControlInput::Tick, &mut port)
        .unwrap();
    for n in 0..13u8 {
        let now = u64::from(n) + 1;
        let op = operation(n + 10);
        let admitted = control
            .handle_request(
                at(now),
                RequestContext {
                    token: InputId::new(u64::from(n) + 2).unwrap(),
                    operation: op,
                    deadline: at(now + 1),
                },
                establish(op, true),
                &mut port,
            )
            .unwrap();
        assert!(admitted.requests.is_empty());
        let timeout = control
            .handle(at(now + 1), ControlInput::Tick, &mut port)
            .unwrap();
        assert_eq!(
            timeout.requests[0].result,
            RequestResult::TimeoutWhileAccepted
        );
        assert_eq!(control.route(context()), QueryContextState::Establishing);
    }
    control
        .handle_request(
            at(20),
            RequestContext {
                token: InputId::new(100).unwrap(),
                operation: operation(50),
                deadline: at(100),
            },
            establish(operation(50), true),
            &mut port,
        )
        .unwrap();
    let materialized = control
        .handle(
            at(20),
            ControlInput::Materialized {
                context: context(),
                result: Ok(()),
            },
            &mut port,
        )
        .unwrap();
    assert_eq!(materialized.requests.len(), 1);
    assert_eq!(materialized.requests[0].request.operation, operation(50));
    assert_eq!(
        materialized.requests[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    assert_eq!(control.route(context()), QueryContextState::Active);
    assert_eq!(
        port.calls.len(),
        2,
        "the redeemed ticket and its one context materialization stay owned"
    );
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
            ContextInput::Create {
                request: operation(1),
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
            .contains(&operation(1))
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
