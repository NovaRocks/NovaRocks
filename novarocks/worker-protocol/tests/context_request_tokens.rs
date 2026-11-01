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

//! Pure request-token counterexamples. Explicit model host facts exercise the
//! actual protocol core; they prove no physical holder exit or Native routing.

use novarocks_control_runtime_core::identity::{
    Generation, InputId, ReservationId, ScopeId, ScopedWorkRef, WorkId,
};
use novarocks_execution_contract::{
    AdmissionTicketId, CancelReason, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
    DomainVersion, LeaseValidFor, OperationOutcome, QueryContextRef, TaskIdentity, TaskOperationId,
    TaskState,
};
use novarocks_types::identity::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use novarocks_worker_protocol::{
    context::{
        ContextCommand, ContextConfig, ContextCore, ContextEffect, ContextEstablishment,
        ContextInput, ContextMaterialized, ContextReplyBody, ContextRequestSettlement, ContextStep,
        RequestResult,
    },
    control::EstablishSemanticFacts,
    creation::{
        AdmissionNeed, AdmissionPort, AdmissionRefusal, CreationCost, PayloadId, RequestContext,
    },
    domain::ContextDomainInput,
    lease::{LeaseBounds, MonotonicInstant},
};
use std::{collections::VecDeque, sync::Arc, time::Duration};

fn bytes(n: u8) -> [u8; 16] {
    let mut bytes = [n; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes
}
fn operation(n: u8) -> TaskOperationId {
    TaskOperationId::try_from_bytes(bytes(n)).unwrap()
}
// One explicit model process owns these requests. The fixture's non-reused
// input issuance ordinal also names its original R Work; this is plain protocol
// metadata and cannot create runtime Work, capacity or an InputAuthor proof.
fn model_request_author(token: InputId) -> ScopedWorkRef {
    ScopedWorkRef {
        scope: ScopeId::new(1).expect("the model process Scope"),
        work: WorkId::new(token.get()).expect("the original model request ordinal"),
    }
}

fn at(seconds: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_secs(seconds))
}
fn context() -> QueryContextRef {
    QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(7511, 7512), AttemptId::new(1).unwrap()).unwrap(),
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
fn request(token: u64, operation: TaskOperationId, deadline: u64) -> RequestContext {
    RequestContext {
        author: model_request_author(InputId::new(token).unwrap()),
        token: InputId::new(token).unwrap(),
        operation,
        deadline: at(deadline),
    }
}
#[derive(Default)]
struct Port {
    calls: Vec<AdmissionNeed>,
    next: u64,
}
impl AdmissionPort for Port {
    fn try_reserve(&mut self, need: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal> {
        self.calls.push(need);
        self.next += 1;
        Ok(ReservationId::new(self.next).unwrap())
    }
}
struct Model {
    core: ContextCore,
    port: Port,
    now: MonotonicInstant,
}
impl Model {
    fn new() -> Self {
        let core = ContextCore::new(
            context(),
            Generation::new(1).unwrap(),
            ContextEstablishment {
                operation: operation(2),
                ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
                payload: PayloadId(1),
                elected_at: at(0),
                semantics: EstablishSemanticFacts {
                    catalog: ContentFingerprint::from_bytes([1; 16]),
                    shared_filter: ContentFingerprint::from_bytes([2; 16]),
                    query_options: ContentFingerprint::from_bytes([3; 16]),
                    credential_lease: CredentialLeaseId::new(1),
                    credential_epoch: CredentialEpoch::FIRST,
                    initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
                },
            },
            ContextConfig {
                max_tasks: 4,
                max_creation_waiters: 8,
                max_pending_requests: 16,
                max_preparing: 4,
                max_task_bytes: 100,
                step_budget: ContextCore::MAX_STEP_WORK,
                lease_bounds: LeaseBounds::DEFAULT,
                metric_publish_interval: Duration::from_millis(100),
                request_horizon: novarocks_worker_protocol::lease::RequestHorizon::DEFAULT,
            },
        )
        .unwrap();
        let mut m = Self {
            core,
            port: Port::default(),
            now: at(0),
        };
        m.apply(ContextInput::MaterializationFinished {
            result: Ok(ContextMaterialized {
                initial_credential_resource: 101,
            }),
        });
        m
    }
    fn check(step: &ContextStep) {
        assert!(
            step.requests.len() <= 1,
            "one bounded slice owns at most one reply: {step:?}"
        );
        assert!(
            step.charged_work() <= ContextCore::MAX_BODY_WORK,
            "every actual reply/continuation slice stays charged: {step:?}"
        );
    }
    fn apply(&mut self, input: ContextInput) -> ContextStep {
        let step = self.core.handle(self.now, input, &mut self.port).unwrap();
        Self::check(&step);
        step
    }
    fn command(&mut self, request: RequestContext, command: ContextCommand) -> ContextStep {
        let step = self
            .core
            .handle_request(self.now, request, command, &mut self.port)
            .unwrap();
        Self::check(&step);
        step
    }
    fn settle(&mut self, first: ContextStep) -> Vec<ContextRequestSettlement> {
        let mut pending = VecDeque::from([first]);
        let mut replies = Vec::new();
        // Follow only the Core's actually emitted reply continuations. This
        // fixed bound fails a livelock rather than manufacturing completion.
        for _ in 0..32 {
            let Some(mut step) = pending.pop_front() else {
                return replies;
            };
            replies.append(&mut step.requests);
            for effect in step.effects {
                let continuation = match effect {
                    ContextEffect::ContinueReplies { task } => {
                        Some(ContextInput::ContinueReplies { task })
                    }
                    ContextEffect::ContinueRequestReplies { operation } => {
                        Some(ContextInput::ContinueRequestReplies { operation })
                    }
                    _ => None,
                };
                if let Some(input) = continuation {
                    pending.push_back(self.apply(input));
                }
            }
        }
        panic!("reply continuations failed to finish within the fixture bound");
    }
    fn loaded_step(&mut self, task: TaskIdentity) -> ContextStep {
        self.apply(ContextInput::Loaded {
            task,
            result: Ok(CreationCost {
                input_retained_bytes: 1,
                ingress_retained_bytes: 1,
                structural_bytes: 1,
            }),
        })
    }
    fn loaded(&mut self, task: TaskIdentity) -> Vec<ContextRequestSettlement> {
        let step = self.loaded_step(task);
        self.settle(step)
    }
}
fn assert_reply(
    reply: &ContextRequestSettlement,
    original: RequestContext,
    task: Option<TaskIdentity>,
    result: RequestResult,
) {
    assert_eq!(reply.request, original);
    assert_eq!(reply.task, task);
    assert_eq!(reply.result, result);
    assert!(reply.body.matches(result));
}
fn loader_count(step: &ContextStep, task: TaskIdentity) -> usize {
    step.effects.iter().filter(|effect| matches!(effect, ContextEffect::LoadInput { task: original, .. } if *original == task)).count()
}

#[test]
fn equal_wire_operations_for_different_tasks_start_two_original_loaders() {
    let mut m = Model::new();
    let shared = operation(41);
    let first = request(1, shared, 20);
    let second = request(2, shared, 20);
    let a = m.command(
        first,
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(11),
        },
    );
    let b = m.command(
        second,
        ContextCommand::Create {
            task: task(2),
            payload: PayloadId(22),
        },
    );
    assert_eq!(loader_count(&a, task(1)), 1);
    assert_eq!(loader_count(&b, task(2)), 1);
    assert!(a.requests.is_empty() && b.requests.is_empty());
    assert_eq!(
        m.port.calls.len(),
        2,
        "wire operation equality cannot join different entities"
    );
    let a = m.loaded(task(1));
    let b = m.loaded(task(2));
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert_reply(
        &a[0],
        first,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    assert_reply(
        &b[0],
        second,
        Some(task(2)),
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
}

#[test]
fn equal_wire_operations_for_the_same_task_keep_one_loader_and_two_original_reply_tokens() {
    let mut m = Model::new();
    let shared = operation(42);
    let first = request(3, shared, 20);
    let second = request(4, shared, 20);
    let a = m.command(
        first,
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(31),
        },
    );
    // PayloadId addresses another captured carrier, not Task entity equality.
    let b = m.command(
        second,
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(999),
        },
    );
    assert_eq!(loader_count(&a, task(1)), 1);
    assert_eq!(loader_count(&b, task(1)), 0);
    assert_eq!(m.port.calls.len(), 1);
    assert!(a.requests.is_empty() && b.requests.is_empty());
    let replies = m.loaded(task(1));
    assert_eq!(
        replies.len(),
        2,
        "the entity has two independently admitted reply roles"
    );
    for original in [first, second] {
        let matching = replies
            .iter()
            .filter(|reply| reply.request.token == original.token)
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        assert_reply(
            matching[0],
            original,
            Some(task(1)),
            RequestResult::Outcome(OperationOutcome::Accepted),
        );
    }
    let replay = request(5, shared, 20);
    let step = m.command(
        replay,
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(1001),
        },
    );
    assert_eq!(loader_count(&step, task(1)), 0);
    let replies = m.settle(step);
    assert_eq!(replies.len(), 1);
    assert_reply(
        &replies[0],
        replay,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Idempotent),
    );
}

#[test]
fn equal_wire_operations_for_different_observations_keep_each_complete_typed_body() {
    let mut m = Model::new();
    let create = m.command(
        request(10, operation(50), 20),
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(10),
        },
    );
    assert_eq!(loader_count(&create, task(1)), 1);
    assert_eq!(m.loaded(task(1)).len(), 1);
    let version = DomainVersion::new(5).unwrap();
    let payload = PayloadId(55);
    m.apply(ContextInput::DynamicFilterPublished {
        task: task(1),
        version,
        payload,
        domain_count: 1,
    });
    let cancel = m.command(
        request(11, operation(51), 20),
        ContextCommand::Cancel {
            task: task(1),
            reason: CancelReason::UpstreamNoLongerNeeded,
        },
    );
    assert_eq!(m.settle(cancel).len(), 1);
    m.apply(ContextInput::Finish { task: task(1) });
    assert_eq!(m.core.task(task(1)).unwrap().state, TaskState::Canceled);

    let shared = operation(52);
    let filters = request(20, shared, 20);
    let final_info = request(21, shared, 20);
    let filters_step = m.command(
        filters,
        ContextCommand::GetDynamicFilters {
            task: task(1),
            acknowledged: None,
        },
    );
    let final_step = m.command(final_info, ContextCommand::GetFinalInfo { task: task(1) });
    assert!(filters_step.effects.iter().any(|effect| matches!(effect,
        ContextEffect::PrepareReadPayload { request, task: original_task, version: original_version, payload: original_payload }
            if *request == filters.token && *original_task == task(1) && *original_version == version && *original_payload == payload)));
    let filter_replies = m.settle(filters_step);
    let final_replies = m.settle(final_step);
    assert_eq!(filter_replies.len(), 1);
    assert_eq!(final_replies.len(), 1);
    assert_reply(
        &filter_replies[0],
        filters,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    assert!(
        matches!(filter_replies[0].body.as_ref(), ContextReplyBody::ReadPayload { version: original_version, payload: original_payload, outcome: OperationOutcome::Accepted } if *original_version == version && *original_payload == payload)
    );
    assert_reply(
        &final_replies[0],
        final_info,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    let ContextReplyBody::FinalInfo(info) = final_replies[0].body.as_ref() else {
        panic!("an equal wire operation cannot substitute the filter body")
    };
    assert_eq!(
        info.final_status(),
        &m.core.task(task(1)).unwrap().published
    );
    let original_info = info.clone();
    assert_eq!(info.final_status().identity(), task(1));
    assert_eq!(info.final_status().state(), TaskState::Canceled);

    let replay = request(22, shared, 20);
    let repeated = m.command(
        replay,
        ContextCommand::GetDynamicFilters {
            task: task(1),
            acknowledged: Some(version),
        },
    );
    let repeated = m.settle(repeated);
    assert_eq!(repeated.len(), 1);
    assert_reply(
        &repeated[0],
        replay,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Idempotent),
    );
    assert!(
        matches!(repeated[0].body.as_ref(), ContextReplyBody::ReadPayload { version: original_version, payload: original_payload, outcome: OperationOutcome::Idempotent } if *original_version == version && *original_payload == payload)
    );
    assert!(
        filter_replies[0]
            .body
            .matches(RequestResult::Outcome(OperationOutcome::Accepted)),
        "the earlier decided body is immutable"
    );
    let replay_info = request(23, shared, 20);
    let final_step = m.command(replay_info, ContextCommand::GetFinalInfo { task: task(1) });
    let repeated_info = m.settle(final_step);
    assert_eq!(repeated_info.len(), 1);
    assert_reply(
        &repeated_info[0],
        replay_info,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    let ContextReplyBody::FinalInfo(info) = repeated_info[0].body.as_ref() else {
        panic!("replay preserves the original final-info body")
    };
    assert!(Arc::ptr_eq(info, &original_info));
}

#[test]
fn an_expired_context_host_token_cannot_complete_a_new_create_with_the_same_wire_operation() {
    let mut m = Model::new();
    let shared = operation(61);
    let old = request(30, shared, 1);
    let pending = m.command(
        old,
        ContextCommand::UpdateContext {
            payload: PayloadId(91),
            update: ContextDomainInput::Catalog {
                version: DomainVersion::new(2).unwrap(),
                fingerprint: ContentFingerprint::from_bytes([9; 16]),
            },
        },
    );
    assert!(pending.requests.is_empty());
    assert!(pending.effects.iter().any(|effect| matches!(effect, ContextEffect::ApplyContextDomain { request, .. } if *request == old.token)));
    m.now = at(2);
    let expired = m.apply(ContextInput::Tick);
    let expired = m.settle(expired);
    assert_eq!(expired.len(), 1);
    assert_reply(&expired[0], old, None, RequestResult::TimeoutWhileAccepted);

    let new = request(31, shared, 20);
    let created = m.command(
        new,
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(92),
        },
    );
    assert_eq!(loader_count(&created, task(1)), 1);
    assert!(created.requests.is_empty());
    let host_returned = m.apply(ContextInput::ContextUpdateApplied {
        request: old.token,
        result: Ok(()),
    });
    assert!(
        m.settle(host_returned).is_empty(),
        "the old actual host return cannot answer a new original reply token"
    );
    let replies = m.loaded(task(1));
    assert_eq!(replies.len(), 1);
    assert_reply(
        &replies[0],
        new,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    assert!(matches!(
        replies[0].body.as_ref(),
        ContextReplyBody::Create { outcome: OperationOutcome::Accepted, receipt } if receipt.identity() == task(1)
    ));
}

#[test]
fn an_already_expired_new_token_does_not_join_an_old_pending_host_operation() {
    let mut m = Model::new();
    let shared = operation(71);
    let old = request(40, shared, 20);
    let pending = m.command(
        old,
        ContextCommand::UpdateContext {
            payload: PayloadId(101),
            update: ContextDomainInput::Catalog {
                version: DomainVersion::new(2).unwrap(),
                fingerprint: ContentFingerprint::from_bytes([10; 16]),
            },
        },
    );
    assert!(pending.requests.is_empty());
    assert!(pending.effects.iter().any(|effect| matches!(effect,
        ContextEffect::ApplyContextDomain { request, .. } if *request == old.token)));
    m.now = at(2);
    let expired = request(41, shared, 1);
    let refused = m.command(
        expired,
        ContextCommand::UpdateContext {
            payload: PayloadId(102),
            update: ContextDomainInput::Catalog {
                version: DomainVersion::new(3).unwrap(),
                fingerprint: ContentFingerprint::from_bytes([11; 16]),
            },
        },
    );
    assert!(!refused.effects.iter().any(|effect| matches!(
        effect,
        ContextEffect::ApplyContextDomain { .. } | ContextEffect::WaitContextDomain { .. }
    )));
    let replies = m.settle(refused);
    assert_eq!(replies.len(), 1);
    assert_reply(
        &replies[0],
        expired,
        None,
        RequestResult::Outcome(OperationOutcome::OperationTimedOut),
    );
    let original_returned = m.apply(ContextInput::ContextUpdateApplied {
        request: old.token,
        result: Ok(()),
    });
    let replies = m.settle(original_returned);
    assert_eq!(replies.len(), 1);
    assert_reply(
        &replies[0],
        old,
        None,
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    assert!(matches!(
        replies[0].body.as_ref(),
        ContextReplyBody::ContextDomain { .. }
    ));
}

#[test]
fn the_lifecycle_entry_cannot_execute_a_request_body_without_its_actual_request_context() {
    use novarocks_worker_protocol::context::CoreError;
    let mut m = Model::new();
    let refused = m.core.handle(
        m.now,
        ContextInput::Request(ContextCommand::Create {
            task: task(1),
            payload: PayloadId(111),
        }),
        &mut m.port,
    );
    assert_eq!(refused.unwrap_err(), CoreError::WrongIdentity);
    assert!(m.port.calls.is_empty());
    assert!(m.core.task(task(1)).is_none());
    let original = request(42, operation(72), 20);
    let admitted = m.command(
        original,
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(111),
        },
    );
    assert_eq!(loader_count(&admitted, task(1)), 1);
    assert_eq!(m.port.calls.len(), 1);
    let replies = m.loaded(task(1));
    assert_eq!(replies.len(), 1);
    assert_reply(
        &replies[0],
        original,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
}

#[test]
fn equal_wire_operations_for_two_filter_cuts_keep_both_original_body_versions() {
    let mut m = Model::new();
    let create = m.command(
        request(50, operation(80), 20),
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(121),
        },
    );
    assert_eq!(loader_count(&create, task(1)), 1);
    assert_eq!(m.loaded(task(1)).len(), 1);
    let shared = operation(81);
    let v1 = DomainVersion::FIRST;
    let p1 = PayloadId(122);
    m.apply(ContextInput::DynamicFilterPublished {
        task: task(1),
        version: v1,
        payload: p1,
        domain_count: 1,
    });
    let first = request(51, shared, 20);
    let step = m.command(
        first,
        ContextCommand::GetDynamicFilters {
            task: task(1),
            acknowledged: None,
        },
    );
    assert!(step.effects.iter().any(|effect| matches!(effect,
        ContextEffect::PrepareReadPayload { request, task: original_task, version, payload }
            if *request == first.token && *original_task == task(1) && *version == v1 && *payload == p1)));
    let first_replies = m.settle(step);
    assert_eq!(first_replies.len(), 1);
    let original_body = first_replies[0].body.clone();
    let v2 = DomainVersion::new(2).unwrap();
    let p2 = PayloadId(123);
    m.apply(ContextInput::DynamicFilterPublished {
        task: task(1),
        version: v2,
        payload: p2,
        domain_count: 2,
    });
    let second = request(52, shared, 20);
    let step = m.command(
        second,
        ContextCommand::GetDynamicFilters {
            task: task(1),
            acknowledged: None,
        },
    );
    assert!(step.effects.iter().any(|effect| matches!(effect,
        ContextEffect::PrepareReadPayload { request, task: original_task, version, payload }
            if *request == second.token && *original_task == task(1) && *version == v2 && *payload == p2)));
    let second_replies = m.settle(step);
    assert_eq!(second_replies.len(), 1);
    assert_reply(
        &first_replies[0],
        first,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    assert_reply(
        &second_replies[0],
        second,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    assert!(Arc::ptr_eq(&original_body, &first_replies[0].body));
    assert!(!Arc::ptr_eq(&original_body, &second_replies[0].body));
    assert!(
        matches!(original_body.as_ref(), ContextReplyBody::ReadPayload {
        version, payload, outcome: OperationOutcome::Accepted
    } if *version == v1 && *payload == p1)
    );
    assert!(
        matches!(second_replies[0].body.as_ref(), ContextReplyBody::ReadPayload {
        version, payload, outcome: OperationOutcome::Accepted
    } if *version == v2 && *payload == p2)
    );
}

#[test]
fn a_create_decision_before_the_deadline_survives_tick_before_its_second_fanout_slice() {
    let mut m = Model::new();
    let first = request(60, operation(91), 1);
    let second = request(61, operation(91), 1);
    m.command(
        first,
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(131),
        },
    );
    m.command(
        second,
        ContextCommand::Create {
            task: task(1),
            payload: PayloadId(132),
        },
    );
    let decided = m.loaded_step(task(1));
    assert_eq!(decided.requests.len(), 1);
    assert_reply(
        &decided.requests[0],
        first,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    assert!(decided.effects.iter().any(|effect| matches!(effect, ContextEffect::ContinueReplies { task: original } if *original == task(1))));
    m.now = at(2);
    let tick = m.apply(ContextInput::Tick);
    let replies = m.settle(tick);
    assert_eq!(replies.len(), 1);
    assert_reply(
        &replies[0],
        second,
        Some(task(1)),
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    let continuation = m.apply(ContextInput::ContinueReplies { task: task(1) });
    assert!(
        m.settle(continuation).is_empty(),
        "the original delayed continuation cannot reply twice"
    );
}

#[test]
fn a_quiesce_receipt_decided_before_the_deadline_survives_tick_before_waiter_fanout() {
    let mut m = Model::new();
    let first = request(62, operation(92), 20);
    let second = request(63, operation(92), 1);
    let begin = m.command(first, ContextCommand::Quiesce);
    assert!(
        begin
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::ContinueQuiesce))
    );
    assert!(
        m.command(second, ContextCommand::Quiesce)
            .requests
            .is_empty()
    );
    let decided = m.apply(ContextInput::ContinueQuiesce);
    assert_eq!(decided.requests.len(), 1);
    assert_reply(
        &decided.requests[0],
        first,
        None,
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    let ContextReplyBody::Quiesce {
        receipt: original_receipt,
        ..
    } = decided.requests[0].body.as_ref()
    else {
        panic!("the original entity scan froze this receipt")
    };
    assert!(
        decided
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::ContinueQuiesce))
    );
    m.now = at(2);
    let tick = m.apply(ContextInput::Tick);
    let replies = m.settle(tick);
    assert_eq!(replies.len(), 1);
    assert_reply(
        &replies[0],
        second,
        None,
        RequestResult::Outcome(OperationOutcome::Idempotent),
    );
    let ContextReplyBody::Quiesce { receipt, .. } = replies[0].body.as_ref() else {
        panic!("bounded fanout cannot replace a decided receipt with timeout")
    };
    assert!(Arc::ptr_eq(receipt, original_receipt));
    let continuation = m.apply(ContextInput::ContinueQuiesce);
    assert!(
        m.settle(continuation).is_empty(),
        "the original delayed continuation cannot reply twice"
    );
}

#[test]
fn entity_decision_at_the_exact_deadline_does_not_backdate_create_or_quiesce_replies() {
    for quiesce in [false, true] {
        let mut m = Model::new();
        let first = request(64, operation(93), 1);
        let second = request(65, operation(93), 1);
        let decided = if quiesce {
            let begin = m.command(first, ContextCommand::Quiesce);
            assert!(
                begin
                    .effects
                    .iter()
                    .any(|effect| matches!(effect, ContextEffect::ContinueQuiesce))
            );
            m.command(second, ContextCommand::Quiesce);
            m.now = at(1);
            m.apply(ContextInput::ContinueQuiesce)
        } else {
            m.command(
                first,
                ContextCommand::Create {
                    task: task(1),
                    payload: PayloadId(133),
                },
            );
            m.command(
                second,
                ContextCommand::Create {
                    task: task(1),
                    payload: PayloadId(134),
                },
            );
            m.now = at(1);
            m.loaded_step(task(1))
        };
        let task = (!quiesce).then(|| task(1));
        assert_eq!(decided.requests.len(), 1);
        assert_reply(
            &decided.requests[0],
            first,
            task,
            RequestResult::TimeoutWhileAccepted,
        );
        let tick = m.apply(ContextInput::Tick);
        let replies = m.settle(tick);
        assert_eq!(replies.len(), 1);
        assert_reply(
            &replies[0],
            second,
            task,
            RequestResult::TimeoutWhileAccepted,
        );
    }
}

#[test]
fn a_new_quiesce_token_after_abort_cannot_join_the_earlier_entity_scan() {
    use novarocks_execution_contract::AbortCause;
    let mut m = Model::new();
    let shared = operation(94);
    let original = request(66, shared, 20);
    let begin = m.command(original, ContextCommand::Quiesce);
    assert!(
        begin
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::ContinueQuiesce))
    );
    let abort = request(67, operation(95), 20);
    let aborting = m.command(
        abort,
        ContextCommand::Abort {
            cause: AbortCause::QueryFailed,
        },
    );
    assert!(
        aborting
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::ContinueClose { .. }))
    );
    let late = request(68, shared, 20);
    let refused = m.command(late, ContextCommand::Quiesce);
    let replies = m.settle(refused);
    assert_eq!(replies.len(), 1);
    assert_reply(
        &replies[0],
        late,
        None,
        RequestResult::Outcome(OperationOutcome::ContextTerminalReceipt),
    );
    assert!(matches!(
        replies[0].body.as_ref(),
        ContextReplyBody::Verdict(RequestResult::Outcome(
            OperationOutcome::ContextTerminalReceipt
        ))
    ));
    // This is the real continuation emitted before Abort. Its original
    // admitted request can finish, while the new closed-state token is done.
    let scanned = m.apply(ContextInput::ContinueQuiesce);
    assert!(
        !scanned
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::ContinueQuiesce))
    );
    let replies = m.settle(scanned);
    assert_eq!(replies.len(), 1);
    assert_reply(
        &replies[0],
        original,
        None,
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    let closing = m.apply(ContextInput::ContinueClose);
    let replies = m.settle(closing);
    assert_eq!(replies.len(), 1);
    assert_reply(
        &replies[0],
        abort,
        None,
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
}
