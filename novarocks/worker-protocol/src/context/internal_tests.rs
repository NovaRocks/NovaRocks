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

//! Pure protocol tests. Host results below are neutral model facts, never
//! physical executor/capture/Source exit or production quota evidence.
use super::*;
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use std::time::Duration;

fn at(seconds: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_secs(seconds))
}
fn bytes(n: u8) -> [u8; 16] {
    let mut bytes = [n; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes
}
fn identity() -> QueryContextRef {
    QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(9121, 9122), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(bytes(1)).unwrap(),
        BackendProcessId::try_from_bytes(bytes(2)).unwrap(),
    )
}
fn task(n: u32) -> TaskIdentity {
    TaskIdentity::new(
        identity().query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(n).unwrap(),
        identity().backend_process_id(),
    )
}
#[derive(Default)]
struct Port(u64);
impl AdmissionPort for Port {
    fn try_reserve(&mut self, _: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal> {
        self.0 += 1;
        Ok(ReservationId::new(self.0).unwrap())
    }
}
struct Model {
    core: ContextCore,
    port: Port,
    now: u64,
    token: u64,
}
impl Model {
    fn new() -> Self {
        let mut m = Self {
            core: ContextCore::new(
                identity(),
                Generation::new(1).unwrap(),
                ContextEstablishment {
                    operation: TaskOperationId::try_from_bytes(bytes(3)).unwrap(),
                    ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
                    payload: PayloadId(1),
                    elected_at: at(0),
                    semantics: EstablishSemanticFacts {
                        catalog: novarocks_execution_contract::ContentFingerprint::from_bytes(
                            [1; 16],
                        ),
                        shared_filter: novarocks_execution_contract::ContentFingerprint::from_bytes(
                            [2; 16],
                        ),
                        query_options: novarocks_execution_contract::ContentFingerprint::from_bytes(
                            [3; 16],
                        ),
                        credential_lease: novarocks_execution_contract::CredentialLeaseId::new(1),
                        credential_epoch: novarocks_execution_contract::CredentialEpoch::FIRST,
                        initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30))
                            .unwrap(),
                    },
                },
                ContextConfig {
                    max_tasks: 4,
                    max_creation_waiters: 8,
                    max_pending_requests: 32,
                    max_preparing: 4,
                    max_task_bytes: 100,
                    step_budget: ContextCore::MAX_STEP_WORK,
                    lease_bounds: LeaseBounds::DEFAULT,
                    metric_publish_interval: Duration::from_millis(100),
                },
            )
            .unwrap(),
            port: Port::default(),
            now: 0,
            token: 0,
        };
        m.fact(ContextInput::MaterializationFinished {
            result: Ok(ContextMaterialized {
                initial_credential_resource: 101,
            }),
        });
        m
    }
    fn check(step: &ContextStep) {
        assert!(
            step.charged_work() <= ContextCore::MAX_STEP_WORK,
            "{step:?}"
        );
        assert!(
            step.effects.len() + step.requests.len() <= ContextCore::MAX_STEP_ACTIONS,
            "{step:?}"
        );
    }
    fn fact(&mut self, input: ContextInput) -> ContextStep {
        let internal = matches!(input, ContextInput::NextInternal);
        let step = self
            .core
            .handle(at(self.now), input, &mut self.port)
            .unwrap();
        Self::check(&step);
        if !internal {
            assert!(step.charged_work() <= ContextCore::MAX_BODY_WORK);
        }
        step
    }
    fn command(&mut self, command: ContextCommand) -> (InputId, ContextStep) {
        self.token += 1;
        let token = InputId::new(self.token).unwrap();
        let step = self
            .core
            .handle_request(
                at(self.now),
                RequestContext {
                    token,
                    operation: TaskOperationId::try_from_bytes(bytes(
                        u8::try_from(self.token + 10).unwrap(),
                    ))
                    .unwrap(),
                    deadline: at(100),
                },
                command,
                &mut self.port,
            )
            .unwrap();
        Self::check(&step);
        assert!(step.charged_work() <= ContextCore::MAX_BODY_WORK);
        (token, step)
    }
    fn create(&mut self, id: TaskIdentity) -> InputId {
        self.command(ContextCommand::Create {
            task: id,
            payload: PayloadId(2),
        })
        .0
    }
    fn load(&mut self, id: TaskIdentity) -> ContextStep {
        self.fact(ContextInput::Loaded {
            task: id,
            result: Ok(CreationCost {
                input_retained_bytes: 1,
                ingress_retained_bytes: 1,
                structural_bytes: 1,
                inbound_records: 0,
                inbound_bytes: 0,
            }),
        })
    }
    fn accepted(&mut self, id: TaskIdentity) {
        self.create(id);
        self.fact(ContextInput::BindTaskShape {
            task: id,
            shape: TaskShape::default(),
        });
        self.load(id);
    }
    fn update(&mut self, id: TaskIdentity) -> InputId {
        self.command(ContextCommand::UpdateTask {
            task: id,
            payload: PayloadId(3),
            updates: vec![
                TaskDomainInput::Filter {
                    version: DomainVersion::FIRST,
                    fingerprint: novarocks_execution_contract::ContentFingerprint::from_bytes(
                        [7; 16]
                    ),
                };
                3
            ],
        })
        .0
    }
}

#[test]
fn an_idle_next_internal_charges_all_six_probes_without_creating_an_action() {
    let mut m = Model::new();
    assert!(!m.core.has_internal_work());
    let step = m.fact(ContextInput::NextInternal);
    assert_eq!(step.records_visited, 1 + ContextCore::INTERNAL_CLASSES);
    assert!(step.effects.is_empty());
    assert!(step.requests.is_empty());
    assert_eq!(m.core.generation(), Generation::new(1).unwrap());
    assert_eq!(ContextCore::MAX_BODY_WORK, 15);
    assert_eq!(ContextCore::MAX_STEP_ACTIONS, 15);
    assert_eq!(ContextCore::MAX_STEP_WORK, 21);
}

#[test]
fn domain_authors_park_at_the_host_boundary_and_resume_only_from_the_original_result() {
    let mut m = Model::new();
    m.accepted(task(1));
    let request = m.update(task(1));
    let mut applied = false;
    for _ in 0..32 {
        let step = m.fact(ContextInput::NextInternal);
        if step.effects.iter().any(|effect| {
            matches!(effect,
            ContextEffect::ApplyTaskDomains { request: token, task: id, .. }
                if *token == request && *id == task(1))
        }) {
            applied = true;
            break;
        }
    }
    assert!(applied);
    assert!(
        !m.core.has_internal_work(),
        "an admitted host job is not a runnable Core slice"
    );
    let (_, context_update) = m.command(ContextCommand::UpdateContext {
        payload: PayloadId(4),
        update: ContextDomainInput::Filter {
            version: DomainVersion::new(2).unwrap(),
            fingerprint: novarocks_execution_contract::ContentFingerprint::from_bytes([8; 16]),
        },
    });
    assert!(
        context_update
            .effects
            .iter()
            .any(|e| matches!(e, ContextEffect::ApplyContextDomain { .. }))
    );
    assert!(
        !m.core.has_internal_work(),
        "ContextDomain has no internal continuation while awaiting host"
    );
    let idle = m.fact(ContextInput::NextInternal);
    assert_eq!(idle.records_visited, 7);
    assert!(idle.effects.is_empty());
    m.fact(ContextInput::TaskUpdateApplied {
        request,
        task: task(1),
        result: Ok(vec![None; 3]),
    });
    assert!(m.core.has_internal_work());
    let mut reply = None;
    for _ in 0..32 {
        let step = m.fact(ContextInput::NextInternal);
        if let Some(settlement) = step
            .requests
            .into_iter()
            .find(|r| r.settlement.request.token == request)
        {
            reply = Some(settlement);
            break;
        }
    }
    assert!(reply.is_some());
    assert!(!m.core.has_internal_work());
}

#[test]
fn domain_members_and_quiesce_each_receive_a_bounded_fair_slice() {
    let mut m = Model::new();
    m.accepted(task(1));
    m.accepted(task(2));
    m.update(task(1));
    m.update(task(2));
    let (quiesce, _) = m.command(ContextCommand::Quiesce);
    let first = m.fact(ContextInput::NextInternal);
    assert_eq!(
        first.records_visited, 2,
        "first class probe and one original domain record"
    );
    assert!(
        first
            .effects
            .iter()
            .any(|e| matches!(e, ContextEffect::ContinueTaskDomain { task: id } if *id == task(1)))
    );
    let second = m.fact(ContextInput::NextInternal);
    assert_eq!(
        second.records_visited, 4,
        "three class probes and one membership record"
    );
    assert!(
        second
            .effects
            .iter()
            .any(|e| matches!(e, ContextEffect::ContinueQuiesce))
    );
    let third = m.fact(ContextInput::NextInternal);
    assert!(
        third
            .effects
            .iter()
            .any(|e| matches!(e, ContextEffect::ContinueTaskDomain { task: id } if *id == task(2))),
        "the next domain member rotates rather than restarting the lowest Task"
    );
    let mut quiesced = false;
    let mut domain_hosts = BTreeSet::new();
    for _ in 0..64 {
        if !m.core.has_internal_work() {
            break;
        }
        let step = m.fact(ContextInput::NextInternal);
        quiesced |= step
            .requests
            .iter()
            .any(|r| r.settlement.request.token == quiesce);
        for effect in step.effects {
            if let ContextEffect::ApplyTaskDomains { task, .. } = effect {
                domain_hosts.insert(task);
            }
        }
    }
    assert!(
        quiesced,
        "a repeatedly ready domain class cannot starve Quiesce"
    );
    assert_eq!(domain_hosts, [task(1), task(2)].into_iter().collect());
    assert!(
        !m.core.has_internal_work(),
        "only the two true host returns remain outstanding"
    );
}

#[test]
fn creation_replies_rotate_between_decided_tasks_without_selecting_a_loading_waiter() {
    let mut m = Model::new();
    m.create(task(1));
    let a1 = m.create(task(1));
    let a2 = m.create(task(1));
    m.create(task(2));
    let b1 = m.create(task(2));
    let b2 = m.create(task(2));
    m.create(task(3));
    assert!(
        !m.core.has_internal_work(),
        "Loading is not a creation decision"
    );
    m.load(task(1));
    m.load(task(2));
    let mut tokens = Vec::new();
    for probes_and_record in [5, 8, 8, 8] {
        let step = m.fact(ContextInput::NextInternal);
        assert_eq!(step.records_visited, probes_and_record);
        assert_eq!(step.requests.len(), 1);
        tokens.push(step.requests[0].settlement.request.token);
    }
    assert_eq!(tokens, [a1, b1, a2, b2]);
    assert!(!m.core.has_internal_work());
    assert_eq!(m.core.task(task(3)).unwrap().phase, CreationPhase::Loading);
}

#[test]
fn close_parks_until_its_real_task_fact_then_drains_abort_waiters() {
    let mut m = Model::new();
    m.accepted(task(1));
    m.fact(ContextInput::OwnerFenced);
    let (a, _) = m.command(ContextCommand::Abort {
        cause: AbortCause::QueryFailed,
    });
    let (b, _) = m.command(ContextCommand::Abort {
        cause: AbortCause::QueryFailed,
    });
    let scan = m.fact(ContextInput::NextInternal);
    let generation = scan
        .effects
        .iter()
        .find_map(|e| match e {
            ContextEffect::ApplyTaskClose {
                task: id,
                generation,
                ..
            } if *id == task(1) => Some(*generation),
            _ => None,
        })
        .unwrap();
    assert!(
        !m.core.has_internal_work(),
        "scan finished but Task close is still a real host/causal dependency"
    );
    let idle = m.fact(ContextInput::NextInternal);
    assert_eq!(idle.records_visited, 7);
    assert!(idle.effects.is_empty());
    let applied = m.fact(ContextInput::ApplyTaskClose {
        task: task(1),
        generation,
        intent: TaskCloseIntent::Abort(novarocks_execution_contract::AbortCause::QueryFailed),
    });
    assert!(applied.requests.is_empty());
    assert!(m.core.has_internal_work());
    let mut tokens = Vec::new();
    for _ in 0..2 {
        let step = m.fact(ContextInput::NextInternal);
        tokens.extend(
            step.requests
                .into_iter()
                .map(|r| r.settlement.request.token),
        );
    }
    assert_eq!(tokens, [a, b]);
    assert!(!m.core.has_internal_work());
    assert!(
        !m.core.host_releasing,
        "terminal metadata is not physical Task retirement"
    );
}

#[test]
fn maintenance_expires_one_original_request_and_the_lease_prelude_runs_once() {
    let mut m = Model::new();
    let request = m.create(task(1));
    m.now = 30;
    let close = m.fact(ContextInput::NextInternal);
    assert_eq!(m.core.generation(), Generation::new(2).unwrap());
    assert_eq!(
        close
            .effects
            .iter()
            .filter(|e| matches!(e, ContextEffect::CloseExternalAdmission { .. }))
            .count(),
        1
    );
    assert_eq!(
        close
            .effects
            .iter()
            .filter(|e| matches!(e, ContextEffect::SealRoot { .. }))
            .count(),
        1
    );
    assert_eq!(m.core.closing.unwrap().cause, AbortCause::LeaseExpired);
    assert!(
        !m.core.has_internal_work(),
        "the request's future deadline is not a wake"
    );
    m.now = 100;
    let maintenance = m.fact(ContextInput::NextInternal);
    assert_eq!(maintenance.requests.len(), 1);
    assert_eq!(maintenance.requests[0].settlement.request.token, request);
    assert_eq!(
        maintenance.requests[0].settlement.result,
        RequestResult::TimeoutWhileAccepted
    );
    assert!(!maintenance.effects.iter().any(|e| matches!(
        e,
        ContextEffect::CloseExternalAdmission { .. } | ContextEffect::SealRoot { .. }
    )));
    assert_eq!(m.core.generation(), Generation::new(2).unwrap());
    assert!(!m.core.has_internal_work());
}

#[test]
fn the_original_pending_reply_map_always_selects_its_lowest_ready_token() {
    let mut m = Model::new();
    let (low, _) = m.command(ContextCommand::Quiesce);
    let (first, _) = m.command(ContextCommand::Quiesce);
    let (high, _) = m.command(ContextCommand::Quiesce);
    // Exercise the Core's actual bounded mid-step author with its three
    // already admitted requests. This private unit boundary does not claim a
    // currently public input necessarily authors three decisions in one step.
    let body = Arc::new(ContextReplyBody::Verdict(RequestResult::Outcome(
        OperationOutcome::Accepted,
    )));
    let mut authored = ContextStep::default();
    m.core
        .complete_request_at(first, body.clone(), at(0), &mut authored);
    m.core
        .complete_request_at(high, body.clone(), at(0), &mut authored);
    m.core.complete_request_at(low, body, at(0), &mut authored);
    assert_eq!(authored.requests[0].settlement.request.token, first);
    assert_eq!(m.core.pending_replies.len(), 2);
    let step = m.fact(ContextInput::NextInternal);
    assert_eq!(step.requests[0].settlement.request.token, low);
    assert_eq!(m.core.pending_replies.first_key_value().unwrap().0, &high);
    let (later, _) = m.command(ContextCommand::Quiesce);
    m.core.complete_request_at(
        later,
        Arc::new(ContextReplyBody::Verdict(RequestResult::Outcome(
            OperationOutcome::Accepted,
        ))),
        at(0),
        &mut authored,
    );
    // The next class is a bounded Quiesce slice. The following request slice
    // must still select the older ready token despite a newer author arriving.
    let quiesce = m.fact(ContextInput::NextInternal);
    assert!(quiesce.requests.is_empty());
    let oldest = m.fact(ContextInput::NextInternal);
    assert_eq!(oldest.requests[0].settlement.request.token, high);
    assert_eq!(m.core.pending_replies.first_key_value().unwrap().0, &later);
}
