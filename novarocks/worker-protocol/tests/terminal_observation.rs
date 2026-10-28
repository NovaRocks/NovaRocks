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

//! Pure protocol sequences supply explicit model facts. They do not prove
//! physical host/capture exit, native transfer, or real failure-owner cleanup.

use novarocks_control_runtime_core::identity::{Generation, InputId, ReservationId};
use novarocks_execution_contract::{
    AbortCause, AdmissionTicketId, CancelReason, ContentFingerprint, CredentialEpoch,
    CredentialLeaseId, FinalTaskInfo, LeaseValidFor, OperatorStatistics, QueryContextRef,
    SafeDetail, TaskConclusion, TaskFailure, TaskIdentity, TaskOperationId, TaskOutputFacts,
    TaskState, TerminationDetail,
    failure::{FailureClass, FailureEvidence, FailureFact, FailureReason, domain},
};
use novarocks_types::identity::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use novarocks_worker_protocol::{
    context::{
        ContextCommand, ContextConfig, ContextCore, ContextEffect, ContextEstablishment,
        ContextInput, ContextMaterialized, ContextReplyBody, ContextStep, CoreError, PhysicalFact,
        TerminalIntent, TerminalObservation, TerminalStatistics,
    },
    control::EstablishSemanticFacts,
    creation::{
        AdmissionNeed, AdmissionPort, AdmissionRefusal, CreationCost, PayloadId, RequestContext,
    },
    lease::{LeaseBounds, MonotonicInstant},
    proposal_summary::{Proposal, SourceId},
};
use std::{num::NonZeroU16, sync::Arc, time::Duration};

fn bytes(n: u8) -> [u8; 16] {
    let mut value = [n; 16];
    value[6] = 0x70;
    value[8] = 0x80;
    value
}
fn operation(n: u8) -> TaskOperationId {
    TaskOperationId::try_from_bytes(bytes(n)).unwrap()
}
fn at(seconds: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_secs(seconds))
}
fn context() -> QueryContextRef {
    QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(7303, 7304), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(bytes(1)).unwrap(),
        BackendProcessId::try_from_bytes(bytes(2)).unwrap(),
    )
}
fn task() -> TaskIdentity {
    let context = context();
    TaskIdentity::new(
        context.query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        context.backend_process_id(),
    )
}
#[derive(Default)]
struct Port {
    next: u64,
}
impl AdmissionPort for Port {
    fn try_reserve(&mut self, _: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal> {
        self.next += 1;
        Ok(ReservationId::new(self.next).unwrap())
    }
}
struct Model {
    core: ContextCore,
    port: Port,
    now: MonotonicInstant,
    next_request: u64,
}
impl Model {
    fn new() -> Self {
        let semantics = EstablishSemanticFacts {
            catalog: ContentFingerprint::from_bytes([1; 16]),
            shared_filter: ContentFingerprint::from_bytes([2; 16]),
            query_options: ContentFingerprint::from_bytes([3; 16]),
            credential_lease: CredentialLeaseId::new(1),
            credential_epoch: CredentialEpoch::FIRST,
            initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
        };
        let core = ContextCore::new(
            context(),
            Generation::new(1).unwrap(),
            ContextEstablishment {
                operation: operation(2),
                ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
                payload: PayloadId(1),
                semantics,
                elected_at: at(0),
            },
            ContextConfig {
                max_tasks: 4,
                max_creation_waiters: 4,
                max_pending_requests: 16,
                max_preparing: 1,
                max_task_bytes: 100,
                step_budget: ContextCore::MAX_STEP_WORK,
                lease_bounds: LeaseBounds::DEFAULT,
                metric_publish_interval: Duration::from_millis(100),
            },
        )
        .unwrap();
        let mut model = Self {
            core,
            port: Port::default(),
            now: at(0),
            next_request: 1,
        };
        model.apply(ContextInput::MaterializationFinished {
            result: Ok(ContextMaterialized {
                initial_credential_resource: 101,
            }),
        });
        model.request(
            operation(3),
            ContextCommand::Create {
                task: task(),
                payload: PayloadId(3),
            },
        );
        let mut loaded = model.apply(ContextInput::Loaded {
            task: task(),
            result: Ok(CreationCost {
                input_retained_bytes: 1,
                ingress_retained_bytes: 1,
                structural_bytes: 1,
                inbound_records: 0,
                inbound_bytes: 0,
            }),
        });
        while loaded
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::ContinueReplies { .. }))
        {
            loaded = model.apply(ContextInput::ContinueReplies { task: task() });
        }
        model.apply(ContextInput::Installed { task: task() });
        model.apply(ContextInput::Running { task: task() });
        model
    }
    fn request(&mut self, operation: TaskOperationId, command: ContextCommand) -> ContextStep {
        let token = InputId::new(self.next_request).unwrap();
        self.next_request += 1;
        let step = self
            .core
            .handle_request(
                self.now,
                RequestContext {
                    token,
                    operation,
                    deadline: at(10_000),
                },
                command,
                &mut self.port,
            )
            .unwrap();
        assert!(step.charged_work() <= ContextCore::MAX_STEP_WORK);
        step
    }
    fn apply(&mut self, input: ContextInput) -> ContextStep {
        let label = format!("{input:?}");
        let step = self.core.handle(self.now, input, &mut self.port).unwrap();
        assert!(
            step.charged_work() <= ContextCore::MAX_STEP_WORK,
            "{label}: charged {}: {:?}",
            step.charged_work(),
            step.effects
        );
        step
    }
    fn observe(&mut self, observation: TerminalObservation) -> ContextStep {
        self.apply(ContextInput::TerminalObservation {
            task: task(),
            observation,
        })
    }
    fn physical(&mut self, fact: PhysicalFact) -> ContextStep {
        self.apply(ContextInput::Physical { task: task(), fact })
    }
    fn final_info(&mut self, request: u8) -> Arc<FinalTaskInfo> {
        self.request(
            operation(request),
            ContextCommand::GetFinalInfo { task: task() },
        )
        .requests
        .into_iter()
        .find_map(|reply| match reply.body.as_ref() {
            ContextReplyBody::FinalInfo(info) => {
                assert_eq!(reply.request.operation, operation(request));
                assert_eq!(reply.task, Some(task()));
                assert!(reply.body.matches(reply.result));
                Some(info.clone())
            }
            _ => None,
        })
        .expect("the model's terminal status froze final information")
    }
    fn other_physical_gates(&mut self) {
        // Explicit pure-world facts, not observations of any host work.
        for fact in [
            PhysicalFact::LoaderExited,
            PhysicalFact::PreparationBodyExited,
            PhysicalFact::PreparationExited,
            PhysicalFact::RootIdle,
        ] {
            self.physical(fact);
        }
    }
}
fn statistics(rows: u64) -> Vec<OperatorStatistics> {
    vec![OperatorStatistics::new(1, SafeDetail::truncating("scan")).with_output_rows(rows)]
}
fn success(output_released: bool) -> TerminalObservation {
    TerminalObservation {
        intent: TerminalIntent::SuccessfulProduction {
            completed_output: Some(TaskOutputFacts::new(true)),
        },
        output_released,
        final_statistics: Some(TerminalStatistics {
            statistics: statistics(7),
            truncated: false,
        }),
    }
}
fn finish(output_released: bool) -> TerminalObservation {
    TerminalObservation {
        intent: TerminalIntent::FinishExistingTermination {
            output: Some(TaskOutputFacts::new(true)),
        },
        output_released,
        final_statistics: Some(TerminalStatistics {
            statistics: statistics(7),
            truncated: true,
        }),
    }
}
fn failure(source: u64, evidence: FailureEvidence) -> Proposal {
    Proposal {
        conclusion: TaskConclusion::Terminated(TerminationDetail::Failed(TaskFailure::new(
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::EXECUTION, NonZeroU16::new(51).unwrap()),
            ),
            SafeDetail::truncating("original terminal failure"),
        ))),
        evidence: Some(evidence),
        source: Some(SourceId(source)),
        diagnostic: None,
    }
}
fn cancel() -> Proposal {
    Proposal {
        conclusion: TaskConclusion::Terminated(TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        )),
        evidence: None,
        source: None,
        diagnostic: None,
    }
}
fn has_retire(step: &ContextStep) -> bool {
    step.effects
        .iter()
        .any(|effect| matches!(effect, ContextEffect::RetireTask { .. }))
}

#[test]
fn a_grouped_success_waits_for_independent_exit_output_and_original_physical_gates() {
    let mut model = Model::new();
    let observed = model.observe(success(false));
    let record = model.core.task(task()).unwrap();
    assert_eq!(record.state, TaskState::Running);
    assert!(record.completion_success_pending);
    assert!(record.conclusion.accepted.is_none());
    assert!(!record.actual_stopped && !record.resources_converged && !record.output_released);
    assert!(!has_retire(&observed));
    assert!(!observed.effects.iter().any(|effect| matches!(
        effect,
        ContextEffect::PublishTask {
            state: TaskState::Finished,
            ..
        }
    )));
    let exited = model.apply(ContextInput::CompletionExited { task: task() });
    let record = model.core.task(task()).unwrap();
    assert_eq!(record.state, TaskState::Finished);
    assert!(record.actual_stopped && record.resources_converged);
    assert!(!record.output_released);
    assert!(!has_retire(&exited));
    model.other_physical_gates();
    assert!(
        !model
            .core
            .task(task())
            .unwrap()
            .convergence
            .snapshot()
            .retired()
    );
    let output = model.physical(PhysicalFact::OutputReleased);
    assert!(has_retire(&output));
    assert!(
        model
            .core
            .task(task())
            .unwrap()
            .convergence
            .snapshot()
            .retired()
    );
}

#[test]
fn final_statistics_are_sealed_while_success_is_pending_and_late_samples_cannot_replace_them() {
    let mut model = Model::new();
    model.apply(ContextInput::OperatorStatistics {
        task: task(),
        statistics: statistics(3),
        truncated: true,
    });
    model.observe(success(true));
    assert_eq!(model.core.task(task()).unwrap().state, TaskState::Running);
    assert!(model.core.task(task()).unwrap().statistics_sealed);
    model.apply(ContextInput::OperatorStatistics {
        task: task(),
        statistics: statistics(99),
        truncated: false,
    });
    assert_eq!(
        model.core.task(task()).unwrap().operator_statistics,
        statistics(7)
    );
    model.apply(ContextInput::CompletionExited { task: task() });
    let info = model.final_info(10);
    assert_eq!(info.operator_statistics(), statistics(7));
    assert!(info.operator_statistics_truncated());
    model.apply(ContextInput::OperatorStatistics {
        task: task(),
        statistics: statistics(123),
        truncated: false,
    });
    assert_eq!(model.final_info(11), info);
}

#[test]
fn one_observation_finishes_the_existing_cancel_or_original_failure_and_keeps_final_statistics() {
    for originating in [false, true] {
        let mut model = Model::new();
        let proposed = if originating {
            failure(77, FailureEvidence::Originating)
        } else {
            cancel()
        };
        let expected = proposed.conclusion.clone();
        model.apply(ContextInput::Proposal {
            task: task(),
            proposal: proposed,
        });
        let observed = model.observe(finish(true));
        let record = model.core.task(task()).unwrap();
        assert_eq!(record.conclusion.accepted.as_ref(), Some(&expected));
        assert_eq!(
            record.state,
            if originating {
                TaskState::Failed
            } else {
                TaskState::Canceled
            }
        );
        assert!(record.output_released);
        assert!(!record.actual_stopped && !record.resources_converged);
        assert!(!has_retire(&observed));
        if originating {
            assert_eq!(record.conclusion.source, Some(SourceId(77)));
        }
        let terminal = observed
            .effects
            .iter()
            .position(|effect| {
                matches!(
                    effect,
                    ContextEffect::PublishTask {
                        state: TaskState::Failed | TaskState::Canceled,
                        ..
                    }
                )
            })
            .unwrap();
        let output = observed.effects.iter().position(|effect| matches!(effect, ContextEffect::PublishTaskConvergence {snapshot, ..} if snapshot.output_released())).unwrap();
        assert!(terminal < output);
        let info = model.final_info(10);
        assert_eq!(info.operator_statistics(), statistics(7));
        assert!(info.operator_statistics_truncated());
    }
}

#[test]
fn incomplete_positive_output_and_finish_without_a_prior_termination_are_rejected() {
    for intent in [
        TerminalIntent::SuccessfulProduction {
            completed_output: Some(TaskOutputFacts::new(false)),
        },
        TerminalIntent::FinishExistingTermination {
            output: Some(TaskOutputFacts::new(false)),
        },
        TerminalIntent::FinishExistingTermination { output: None },
    ] {
        let mut model = Model::new();
        let refused = model.core.handle(
            at(0),
            ContextInput::TerminalObservation {
                task: task(),
                observation: TerminalObservation {
                    intent,
                    output_released: true,
                    final_statistics: None,
                },
            },
            &mut model.port,
        );
        assert_eq!(refused.unwrap_err(), CoreError::IllegalFact);
        let record = model.core.task(task()).unwrap();
        assert!(!record.terminal_observation_received);
        assert!(!record.statistics_sealed);
        assert!(!record.output_released && !record.actual_stopped);
    }
}

#[test]
fn an_original_terminal_observation_cannot_be_registered_twice() {
    let mut model = Model::new();
    model.observe(success(true));
    let refused = model.core.handle(
        at(0),
        ContextInput::TerminalObservation {
            task: task(),
            observation: success(true),
        },
        &mut model.port,
    );
    assert_eq!(refused.unwrap_err(), CoreError::IllegalFact);
    assert_eq!(model.core.task(task()).unwrap().state, TaskState::Running);
    assert!(
        model
            .core
            .task(task())
            .unwrap()
            .conclusion
            .accepted
            .is_none()
    );
}

#[test]
fn lease_expiry_and_two_failure_candidates_remain_inside_the_actual_step_budget() {
    for prior_cancel in [false, true] {
        for lease_expired in [false, true] {
            let mut model = Model::new();
            if prior_cancel {
                model.apply(ContextInput::Proposal {
                    task: task(),
                    proposal: cancel(),
                });
                model.apply(ContextInput::Adjudicate { task: task() });
            }
            model.other_physical_gates();
            model.physical(PhysicalFact::ActualStopped);
            model.physical(PhysicalFact::ResourcesConverged);
            model.apply(ContextInput::Proposal {
                task: task(),
                proposal: failure(11, FailureEvidence::StopObservation),
            });
            model.apply(ContextInput::Proposal {
                task: task(),
                proposal: failure(12, FailureEvidence::Originating),
            });
            assert_eq!(
                model.core.task(task()).unwrap().proposals.candidate_count(),
                2
            );
            if lease_expired {
                model.now = at(30);
            }
            let observed = model.observe(finish(true));
            assert!(has_retire(&observed));
            if lease_expired {
                assert!(
                    observed.effects.iter().any(|effect| matches!(
                        effect,
                        ContextEffect::CloseExternalAdmission { .. }
                    ))
                );
            }
            let released = observed
                .effects
                .iter()
                .filter(|effect| matches!(effect, ContextEffect::DisposeSource { .. }))
                .count();
            assert!(released >= 1);
            if prior_cancel {
                assert_eq!(released, 2);
            }
            assert_eq!(model.final_info(20).operator_statistics(), statistics(7));
        }
    }
}

#[test]
fn the_last_retirement_requests_host_release_without_inventing_exit_in_the_group() {
    let mut model = Model::new();
    let close = model.request(
        operation(30),
        ContextCommand::Abort {
            cause: AbortCause::PeerTaskFailed,
        },
    );
    assert!(
        !close
            .effects
            .iter()
            .any(|effect| matches!(effect, ContextEffect::ReleaseHost { .. }))
    );
    let scheduled = model.apply(ContextInput::ContinueClose);
    let generation = scheduled
        .effects
        .iter()
        .find_map(|effect| match effect {
            ContextEffect::ApplyTaskClose { generation, .. } => Some(*generation),
            _ => None,
        })
        .expect("the model schedules this original Task's close cut");
    model.apply(ContextInput::ApplyTaskClose {
        task: task(),
        generation,
    });
    model.other_physical_gates();
    model.physical(PhysicalFact::ActualStopped);
    model.physical(PhysicalFact::ResourcesConverged);
    let observed = model.observe(finish(true));
    let retire = observed
        .effects
        .iter()
        .position(|effect| matches!(effect, ContextEffect::RetireTask { .. }))
        .unwrap();
    let release = observed
        .effects
        .iter()
        .position(|effect| matches!(effect, ContextEffect::ReleaseHost { .. }))
        .unwrap();
    assert!(retire < release);
    assert!(
        model
            .core
            .task(task())
            .unwrap()
            .convergence
            .snapshot()
            .retired()
    );
}

#[test]
fn cancellation_between_the_grouped_success_intent_and_exit_keeps_its_winning_conclusion() {
    let mut model = Model::new();
    model.observe(success(true));
    model.apply(ContextInput::Proposal {
        task: task(),
        proposal: cancel(),
    });
    model.apply(ContextInput::Adjudicate { task: task() });
    assert_eq!(model.core.task(task()).unwrap().state, TaskState::Canceling);
    model.apply(ContextInput::CompletionExited { task: task() });
    let record = model.core.task(task()).unwrap();
    assert_eq!(record.state, TaskState::Canceled);
    assert!(matches!(
        record.conclusion.accepted,
        Some(TaskConclusion::Terminated(TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded
        )))
    ));
    assert!(!record.completion_success_pending);
    assert_eq!(model.final_info(40).operator_statistics(), statistics(7));
}

#[test]
fn oversized_final_statistics_are_rejected_before_observation_or_output_release() {
    let mut model = Model::new();
    let mut observation = success(true);
    observation.final_statistics = Some(TerminalStatistics {
        statistics: vec![
            OperatorStatistics::new(1, SafeDetail::truncating("scan"));
            novarocks_execution_contract::FINAL_TASK_INFO_MAX_OPERATORS + 1
        ],
        truncated: true,
    });
    let refused = model.core.handle(
        at(0),
        ContextInput::TerminalObservation {
            task: task(),
            observation,
        },
        &mut model.port,
    );
    assert_eq!(refused.unwrap_err(), CoreError::IllegalFact);
    let record = model.core.task(task()).unwrap();
    assert!(!record.terminal_observation_received && !record.statistics_sealed);
    assert!(!record.output_released && !record.actual_stopped);
    assert!(record.conclusion.accepted.is_none());
}
