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
use novarocks_control_runtime_core::identity::{ScopeId, ScopedWorkRef, WorkId};
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use std::time::Duration;

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
fn bytes(n: u8) -> [u8; 16] {
    let mut bytes = [n; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes
}
fn identity() -> ParticipationRef {
    ParticipationRef::new(
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
    core: LiveParticipationCore,
    port: Port,
    now: u64,
    token: u64,
}
impl Model {
    fn new() -> Self {
        let mut m = Self {
            core: LiveParticipationCore::new(
                identity(),
                Generation::new(1).unwrap(),
                ParticipationEstablishment {
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
                        credential_domain_id: novarocks_execution_contract::CredentialDomainId::new(
                            1,
                        ),
                        credential_epoch: novarocks_execution_contract::CredentialEpoch::FIRST,
                        initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30))
                            .unwrap(),
                    },
                },
                ParticipationConfig {
                    max_tasks: 4,
                    max_creation_waiters: 8,
                    max_pending_requests: 32,
                    max_preparing: 4,
                    max_task_bytes: 100,
                    step_budget: ParticipationCore::MAX_STEP_WORK,
                    lease_bounds: LeaseBounds::DEFAULT,
                    metric_publish_interval: Duration::from_millis(100),
                    request_horizon: crate::lease::RequestHorizon::DEFAULT,
                },
            )
            .unwrap(),
            port: Port::default(),
            now: 0,
            token: 0,
        };
        m.fact(ParticipationInput::MaterializationFinished {
            result: Ok(ParticipationMaterialized {
                initial_credential_resource: 101,
            }),
        });
        m
    }
    fn check(step: &ParticipationStep) {
        assert!(
            step.charged_work() <= ParticipationCore::MAX_STEP_WORK,
            "{step:?}"
        );
        assert!(
            step.effects.len() + step.requests.len() <= ParticipationCore::MAX_STEP_ACTIONS,
            "{step:?}"
        );
    }
    fn fact(&mut self, input: ParticipationInput) -> ParticipationStep {
        let internal = matches!(input, ParticipationInput::NextInternal);
        let step = self
            .core
            .handle(at(self.now), input, &mut self.port)
            .unwrap();
        Self::check(&step);
        if !internal {
            assert!(step.charged_work() <= ParticipationCore::MAX_BODY_WORK);
        }
        step
    }
    fn command(&mut self, command: ParticipationCommand) -> (InputId, ParticipationStep) {
        self.token += 1;
        let token = InputId::new(self.token).unwrap();
        let step = self
            .core
            .handle_request(
                at(self.now),
                RequestReplyTag {
                    author: model_request_author(token),
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
        assert!(step.charged_work() <= ParticipationCore::MAX_BODY_WORK);
        (token, step)
    }
    fn create(&mut self, id: TaskIdentity) -> InputId {
        self.command(ParticipationCommand::Create {
            task: id,
            payload: PayloadId(2),
        })
        .0
    }
    fn load(&mut self, id: TaskIdentity) -> ParticipationStep {
        self.fact(ParticipationInput::Loaded {
            task: id,
            result: Ok(CreationCost {
                input_retained_bytes: 1,
                ingress_retained_bytes: 1,
                structural_bytes: 1,
            }),
        })
    }
    fn accepted(&mut self, id: TaskIdentity) {
        self.create(id);
        self.fact(ParticipationInput::BindTaskShape {
            task: id,
            shape: TaskShape::default(),
        });
        self.load(id);
    }
    fn update(&mut self, id: TaskIdentity) -> InputId {
        self.command(ParticipationCommand::UpdateTask {
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
fn an_idle_next_internal_charges_all_internal_probes_without_creating_an_action() {
    let mut m = Model::new();
    assert!(!m.core.has_internal_work());
    let step = m.fact(ParticipationInput::NextInternal);
    assert_eq!(
        step.records_visited,
        1 + ParticipationCore::INTERNAL_CLASSES
    );
    assert!(step.effects.is_empty());
    assert!(step.requests.is_empty());
    assert_eq!(m.core.generation, Generation::new(1).unwrap());
    assert_eq!(ParticipationCore::MAX_BODY_WORK, 15);
    assert_eq!(ParticipationCore::MAX_STEP_ACTIONS, 16);
    assert_eq!(ParticipationCore::MAX_STEP_WORK, 24);
}

#[test]
fn domain_authors_park_at_the_host_boundary_and_resume_only_from_the_original_result() {
    let mut m = Model::new();
    m.accepted(task(1));
    let request = m.update(task(1));
    let mut applied = false;
    for _ in 0..32 {
        let step = m.fact(ParticipationInput::NextInternal);
        if step.effects.iter().any(|effect| {
            matches!(effect,
            ParticipationEffect::ApplyTaskDomains { request: token, task: id, .. }
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
    let (_, participation_update) = m.command(ParticipationCommand::UpdateParticipation {
        payload: PayloadId(4),
        update: ParticipationDomainInput::Filter {
            version: DomainVersion::new(2).unwrap(),
            fingerprint: novarocks_execution_contract::ContentFingerprint::from_bytes([8; 16]),
        },
    });
    assert!(
        participation_update
            .effects
            .iter()
            .any(|e| matches!(e, ParticipationEffect::ApplyParticipationDomain { .. }))
    );
    assert!(
        !m.core.has_internal_work(),
        "ParticipationDomain has no internal continuation while awaiting host"
    );
    let idle = m.fact(ParticipationInput::NextInternal);
    assert_eq!(idle.records_visited, 8);
    assert!(idle.effects.is_empty());
    m.fact(ParticipationInput::TaskUpdateApplied {
        request,
        task: task(1),
        result: Ok(vec![None; 3]),
    });
    assert!(m.core.has_internal_work());
    let mut reply = None;
    for _ in 0..32 {
        let step = m.fact(ParticipationInput::NextInternal);
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
    assert!(
        m.core.has_internal_work(),
        "the original domain source still owns its completion notice"
    );
    let completed = m.fact(ParticipationInput::NextInternal);
    assert!(completed.effects.iter().any(|effect| matches!(effect,
        ParticipationEffect::DomainRequestCompleted { request: original, task: Some(identity), payload: PayloadId(3) }
            if *original == request && *identity == task(1))));
    assert!(!m.core.has_internal_work());
}

#[test]
fn domain_members_and_quiesce_each_receive_a_bounded_fair_slice() {
    let mut m = Model::new();
    m.accepted(task(1));
    m.accepted(task(2));
    m.update(task(1));
    m.update(task(2));
    let (quiesce, _) = m.command(ParticipationCommand::Quiesce);
    let first = m.fact(ParticipationInput::NextInternal);
    assert_eq!(
        first.records_visited, 2,
        "first class probe and one original domain record"
    );
    assert!(first.effects.iter().any(
        |e| matches!(e, ParticipationEffect::ContinueTaskDomain { task: id } if *id == task(1))
    ));
    let second = m.fact(ParticipationInput::NextInternal);
    assert_eq!(
        second.records_visited, 4,
        "three class probes and one membership record"
    );
    assert!(
        second
            .effects
            .iter()
            .any(|e| matches!(e, ParticipationEffect::ContinueQuiesce))
    );
    let third = m.fact(ParticipationInput::NextInternal);
    assert!(
        third.effects.iter().any(
            |e| matches!(e, ParticipationEffect::ContinueTaskDomain { task: id } if *id == task(2))
        ),
        "the next domain member rotates rather than restarting the lowest Task"
    );
    let mut quiesced = false;
    let mut domain_hosts = BTreeSet::new();
    for _ in 0..64 {
        if !m.core.has_internal_work() {
            break;
        }
        let step = m.fact(ParticipationInput::NextInternal);
        quiesced |= step
            .requests
            .iter()
            .any(|r| r.settlement.request.token == quiesce);
        for effect in step.effects {
            if let ParticipationEffect::ApplyTaskDomains { task, .. } = effect {
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
    for probes_and_record in [5, 9, 9, 9] {
        let step = m.fact(ParticipationInput::NextInternal);
        assert_eq!(step.records_visited, probes_and_record);
        assert_eq!(step.requests.len(), 1);
        tokens.push(step.requests[0].settlement.request.token);
    }
    assert_eq!(tokens, [a1, b1, a2, b2]);
    assert!(!m.core.has_internal_work());
    assert_eq!(
        m.core.tasks.get(&task(3)).unwrap().phase,
        CreationPhase::Loading
    );
}

#[test]
fn close_parks_until_its_real_task_fact_then_drains_abort_waiters() {
    let mut m = Model::new();
    m.accepted(task(1));
    m.fact(ParticipationInput::OwnerFenced);
    let (a, _) = m.command(ParticipationCommand::Abort {
        cause: AbortCause::QueryFailed,
    });
    let (b, _) = m.command(ParticipationCommand::Abort {
        cause: AbortCause::QueryFailed,
    });
    let scan = m.fact(ParticipationInput::NextInternal);
    let generation = scan
        .effects
        .iter()
        .find_map(|e| match e {
            ParticipationEffect::ApplyTaskClose {
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
    let idle = m.fact(ParticipationInput::NextInternal);
    assert_eq!(idle.records_visited, 8);
    assert!(idle.effects.is_empty());
    let applied = m.fact(ParticipationInput::ApplyTaskClose {
        task: task(1),
        generation,
        intent: TaskCloseIntent::Abort(novarocks_execution_contract::AbortCause::QueryFailed),
    });
    assert!(applied.requests.is_empty());
    assert!(m.core.has_internal_work());
    let mut tokens = Vec::new();
    for _ in 0..2 {
        let step = m.fact(ParticipationInput::NextInternal);
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

fn rollback_terminal_observation() -> TerminalObservation {
    TerminalObservation {
        intent: TerminalIntent::FinishExistingTermination { output: None },
        normal_close_validated: false,
        output_released: true,
        final_statistics: None,
    }
}

fn original_abort_cut(m: &mut Model, id: TaskIdentity) -> Generation {
    m.fact(ParticipationInput::NextInternal)
        .effects
        .iter()
        .find_map(|effect| match effect {
            ParticipationEffect::ApplyTaskClose {
                task, generation, ..
            } if *task == id => Some(*generation),
            _ => None,
        })
        .expect("the original close scan schedules this Task's causal cut")
}

#[test]
fn rollback_terminal_before_abort_cut_waits_without_selecting_success_in_both_exit_orders() {
    for exit_before_close in [false, true] {
        let mut m = Model::new();
        let id = task(1);
        m.accepted(id);
        m.command(ParticipationCommand::Abort {
            cause: AbortCause::QueryFailed,
        });
        let mut observation = rollback_terminal_observation();
        if exit_before_close {
            // Even an independently complete output cannot turn this
            // termination-only observation into a success intent.
            observation.intent = TerminalIntent::FinishExistingTermination {
                output: Some(TaskOutputFacts::new(true)),
            };
        }
        let observed = m.fact(ParticipationInput::TerminalObservation {
            task: id,
            observation,
        });
        let record = &m.core.tasks[&id];
        assert!(record.accepted() && record.terminal_observation_received);
        assert!(!record.installed && !record.state.is_terminal());
        assert!(record.conclusion.accepted.is_none());
        assert!(record.output_released && record.statistics_sealed);
        assert_eq!(record.output.responsibility_complete(), exit_before_close);
        assert!(!record.actual_stopped && !record.resources_converged);
        assert!(!observed.effects.iter().any(|effect| matches!(
            effect,
            ParticipationEffect::PublishConclusion { .. }
                | ParticipationEffect::FinalizeTaskExecution { .. }
                | ParticipationEffect::RetireTask { .. }
        )));
        if exit_before_close {
            m.fact(ParticipationInput::CompletionExited { task: id });
            let record = &m.core.tasks[&id];
            assert!(record.actual_stopped && record.resources_converged);
            assert!(!record.state.is_terminal());
            assert!(record.conclusion.accepted.is_none());
        }
        let generation = original_abort_cut(&mut m, id);
        let finished = m.fact(ParticipationInput::ApplyTaskClose {
            task: id,
            generation,
            intent: TaskCloseIntent::Abort(AbortCause::QueryFailed),
        });
        assert_eq!(m.core.tasks[&id].state, TaskState::Aborted);
        assert_eq!(
            m.core.tasks[&id].conclusion.accepted,
            Some(TaskConclusion::Terminated(TerminationDetail::Aborted(
                AbortCause::QueryFailed
            )))
        );
        assert_eq!(m.core.tasks[&id].conclusion.source, None);
        assert!(finished.effects.iter().any(|effect| matches!(effect,
            ParticipationEffect::FinalizeTaskExecution { task, .. } if *task == id)));
        assert!(
            !finished
                .effects
                .iter()
                .any(|effect| matches!(effect, ParticipationEffect::RetireTask { .. })),
            "business completion cannot sign loader, preparation or Root exit"
        );
        if !exit_before_close {
            assert!(!m.core.tasks[&id].actual_stopped);
            m.fact(ParticipationInput::CompletionExited { task: id });
        }
        assert!(!m.core.tasks[&id].loader_exited);
        assert!(!m.core.tasks[&id].preparation_exited);
        assert!(!m.core.tasks[&id].physical_exit_ready());
    }
}

#[test]
fn rollback_terminal_pending_close_keeps_the_original_generation_and_release_verdict() {
    for wrong_close in [true, false] {
        let mut m = Model::new();
        let id = task(1);
        m.accepted(id);
        m.command(ParticipationCommand::Abort {
            cause: AbortCause::QueryFailed,
        });
        m.fact(ParticipationInput::TerminalObservation {
            task: id,
            observation: rollback_terminal_observation(),
        });
        let (_, release) = m.command(ParticipationCommand::Release);
        assert!(matches!(
            release.requests[0].body.as_ref(),
            ParticipationReplyBody::Release {
                release: novarocks_execution_contract::ReleaseOutcome::NotReady,
                ..
            }
        ));
        assert!(
            !release
                .effects
                .iter()
                .any(|effect| matches!(effect, ParticipationEffect::ReleaseHost { .. }))
        );
        let generation = original_abort_cut(&mut m, id);
        if wrong_close {
            let wrong = m.core.handle(
                at(m.now),
                ParticipationInput::ApplyTaskClose {
                    task: id,
                    generation: generation.next().unwrap(),
                    intent: TaskCloseIntent::Abort(AbortCause::QueryFailed),
                },
                &mut m.port,
            );
            assert_eq!(wrong.unwrap_err(), CoreError::WrongIdentity);
            assert!(m.core.tasks[&id].conclusion.accepted.is_none());
            // An illegal input ends this model branch. Normal convergence
            // below uses a fresh Core, never a resumed failed owner.
            continue;
        }
        m.fact(ParticipationInput::CompletionExited { task: id });
        assert!(m.core.tasks[&id].conclusion.accepted.is_none());
        m.fact(ParticipationInput::ApplyTaskClose {
            task: id,
            generation,
            intent: TaskCloseIntent::Abort(AbortCause::QueryFailed),
        });
        assert_eq!(m.core.tasks[&id].state, TaskState::Aborted);
        assert_eq!(m.core.tasks[&id].applied_close_generation, generation);
    }
}

#[test]
fn rollback_terminal_pending_close_preserves_the_original_termination_and_failure_source() {
    use novarocks_execution_contract::failure::{
        FailureClass, FailureEvidence, FailureFact, FailureReason, domain,
    };
    use novarocks_execution_contract::{SafeDetail, TaskFailure};
    use std::num::NonZeroU16;
    let failure = TaskConclusion::Terminated(TerminationDetail::Failed(TaskFailure::new(
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(domain::EXECUTION, NonZeroU16::new(51).unwrap()),
        ),
        SafeDetail::truncating("original preparation failure"),
    )));
    for original in [
        TaskConclusion::Terminated(TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        )),
        failure,
    ] {
        let mut m = Model::new();
        let id = task(1);
        m.accepted(id);
        m.command(ParticipationCommand::Abort {
            cause: AbortCause::QueryFailed,
        });
        m.fact(ParticipationInput::TerminalObservation {
            task: id,
            observation: rollback_terminal_observation(),
        });
        let source = matches!(
            &original,
            TaskConclusion::Terminated(TerminationDetail::Failed(_))
        )
        .then_some(SourceId(41));
        let mut summary = ProposalSummary::default();
        summary.push(Proposal {
            conclusion: original.clone(),
            evidence: source.map(|_| FailureEvidence::Originating),
            source,
            diagnostic: None,
        });
        m.fact(ParticipationInput::Summary {
            task: id,
            generation: Generation::new(1).unwrap(),
            cut: None,
            summary,
        });
        assert_eq!(
            m.core.tasks[&id].conclusion.accepted,
            Some(original.clone())
        );
        assert!(!m.core.tasks[&id].state.is_terminal());
        let generation = original_abort_cut(&mut m, id);
        m.fact(ParticipationInput::ApplyTaskClose {
            task: id,
            generation,
            intent: TaskCloseIntent::Abort(AbortCause::QueryFailed),
        });
        let record = &m.core.tasks[&id];
        assert!(record.state.is_terminal());
        assert_eq!(record.conclusion.accepted, Some(original));
        assert_eq!(record.conclusion.source, source);
        assert!(!record.actual_stopped && !record.resources_converged);
    }
}

#[test]
fn rollback_terminal_without_a_real_unapplied_abort_remains_illegal() {
    for mode in 0..4 {
        let mut m = Model::new();
        let id = task(1);
        m.accepted(id);
        match mode {
            0 => {}
            1 => {
                m.command(ParticipationCommand::Quiesce);
            }
            2 => {
                m.command(ParticipationCommand::Abort {
                    cause: AbortCause::QueryFailed,
                });
                // Isolate the guard with an already-applied generation and
                // no conclusion. This is an invalid neutral model state,
                // never an actual runtime close or exit observation.
                m.core.tasks.get_mut(&id).unwrap().applied_close_generation = m.core.generation;
            }
            _ => {
                m.fact(ParticipationInput::Installed { task: id });
                m.fact(ParticipationInput::CompletionIntent {
                    task: id,
                    completed_output: Some(TaskOutputFacts::new(true)),
                });
                m.fact(ParticipationInput::CompletionExited { task: id });
                assert_eq!(
                    m.core.tasks[&id].conclusion.accepted,
                    Some(TaskConclusion::Succeeded)
                );
                m.command(ParticipationCommand::Abort {
                    cause: AbortCause::QueryFailed,
                });
            }
        }
        let before = m.core.tasks[&id].conclusion.clone();
        let refused = m.core.handle(
            at(m.now),
            ParticipationInput::TerminalObservation {
                task: id,
                observation: rollback_terminal_observation(),
            },
            &mut m.port,
        );
        assert_eq!(refused.unwrap_err(), CoreError::IllegalFact);
        assert!(!m.core.tasks[&id].terminal_observation_received);
        assert_eq!(m.core.tasks[&id].conclusion, before);
    }
}

#[test]
fn rollback_terminal_keeps_original_acceptance_output_statistics_and_once_only_guards() {
    for mode in 0..4 {
        let mut m = Model::new();
        let id = task(1);
        if mode == 0 {
            m.create(id);
        } else {
            m.accepted(id);
        }
        m.command(ParticipationCommand::Abort {
            cause: AbortCause::QueryFailed,
        });
        let mut observation = rollback_terminal_observation();
        match mode {
            1 => {
                observation.intent = TerminalIntent::FinishExistingTermination {
                    output: Some(TaskOutputFacts::new(false)),
                }
            }
            2 => {
                observation.final_statistics = Some(TerminalStatistics {
                    statistics: vec![
                        OperatorStatistics::new(
                            1,
                            novarocks_execution_contract::SafeDetail::truncating(
                                "original bounded sample"
                            )
                        );
                        FINAL_TASK_INFO_MAX_OPERATORS + 1
                    ],
                    truncated: true,
                })
            }
            3 => {
                m.fact(ParticipationInput::TerminalObservation {
                    task: id,
                    observation: observation.clone(),
                });
            }
            _ => {}
        }
        let refused = m.core.handle(
            at(m.now),
            ParticipationInput::TerminalObservation {
                task: id,
                observation,
            },
            &mut m.port,
        );
        assert_eq!(refused.unwrap_err(), CoreError::IllegalFact);
        assert_eq!(m.core.tasks[&id].terminal_observation_received, mode == 3);
        assert!(m.core.tasks[&id].conclusion.accepted.is_none());
        assert!(!m.core.tasks[&id].actual_stopped && !m.core.tasks[&id].resources_converged);
    }
}

#[test]
fn maintenance_expires_one_original_request_and_the_lease_prelude_runs_once() {
    let mut m = Model::new();
    let request = m.create(task(1));
    m.now = 30;
    let close = m.fact(ParticipationInput::NextInternal);
    assert_eq!(m.core.generation, Generation::new(2).unwrap());
    assert_eq!(
        close
            .effects
            .iter()
            .filter(|e| matches!(e, ParticipationEffect::CloseExternalAdmission { .. }))
            .count(),
        1
    );
    assert_eq!(
        close
            .effects
            .iter()
            .filter(|e| matches!(e, ParticipationEffect::SealRoot { .. }))
            .count(),
        1
    );
    assert_eq!(m.core.closing.unwrap().cause, AbortCause::LeaseExpired);
    assert!(
        !m.core.has_internal_work(),
        "the request's future deadline is not a wake"
    );
    m.now = 100;
    let maintenance = m.fact(ParticipationInput::NextInternal);
    assert_eq!(maintenance.requests.len(), 1);
    assert_eq!(maintenance.requests[0].settlement.request.token, request);
    assert_eq!(
        maintenance.requests[0].settlement.result,
        RequestResult::TimeoutWhileAccepted
    );
    assert!(!maintenance.effects.iter().any(|e| matches!(
        e,
        ParticipationEffect::CloseExternalAdmission { .. } | ParticipationEffect::SealRoot { .. }
    )));
    assert_eq!(m.core.generation, Generation::new(2).unwrap());
    assert!(!m.core.has_internal_work());
}

#[test]
fn the_original_pending_reply_map_always_selects_its_lowest_ready_token() {
    let mut m = Model::new();
    let (low, _) = m.command(ParticipationCommand::Quiesce);
    let (first, _) = m.command(ParticipationCommand::Quiesce);
    let (high, _) = m.command(ParticipationCommand::Quiesce);
    // Exercise the Core's actual bounded mid-step author with its three
    // already admitted requests. This private unit boundary does not claim a
    // currently public input necessarily authors three decisions in one step.
    let body = Arc::new(ParticipationReplyBody::Verdict(RequestResult::Outcome(
        OperationOutcome::Accepted,
    )));
    let mut authored = ParticipationStep::default();
    m.core
        .complete_request_at(first, body.clone(), at(0), &mut authored);
    m.core
        .complete_request_at(high, body.clone(), at(0), &mut authored);
    m.core.complete_request_at(low, body, at(0), &mut authored);
    assert_eq!(authored.requests[0].settlement.request.token, first);
    assert_eq!(m.core.replies.pending_replies.len(), 2);
    let step = m.fact(ParticipationInput::NextInternal);
    assert_eq!(step.requests[0].settlement.request.token, low);
    assert_eq!(
        m.core.replies.pending_replies.first_key_value().unwrap().0,
        &high
    );
    let (later, _) = m.command(ParticipationCommand::Quiesce);
    m.core.complete_request_at(
        later,
        Arc::new(ParticipationReplyBody::Verdict(RequestResult::Outcome(
            OperationOutcome::Accepted,
        ))),
        at(0),
        &mut authored,
    );
    // The next class is a bounded Quiesce slice. The following request slice
    // must still select the older ready token despite a newer author arriving.
    let quiesce = m.fact(ParticipationInput::NextInternal);
    assert!(quiesce.requests.is_empty());
    let oldest = m.fact(ParticipationInput::NextInternal);
    assert_eq!(oldest.requests[0].settlement.request.token, high);
    assert_eq!(
        m.core.replies.pending_replies.first_key_value().unwrap().0,
        &later
    );
}
