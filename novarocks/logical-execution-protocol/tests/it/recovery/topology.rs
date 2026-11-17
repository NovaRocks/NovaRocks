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

use super::super::support::*;
use super::contract::*;
use novarocks_coordination_contract::{
    AttemptFailureClass, AttemptIssueSettlement, ExecutionFailure, ExecutionFailureInput,
    ExecutionFailureSite, PreReadyProcessReplacementWitness, QueryFailureReason,
    TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
};
use novarocks_execution_contract::{
    OperationKind, OperationOutcome, SafeDetail, TaskFailurePhase,
    failure::{FailureClass, FailureFact, FailureProjection},
};
use std::time::Duration;

// The expected topology is supplied by each contract case. This assertion
// helper does not classify the failure or derive a process from its Task.
fn assert_original_recovery(
    r: &RecoveryFixture,
    original: &ExecutionFailure,
    expected: SuccessorTopology,
) {
    let prepares =
        r.f.effects
            .iter()
            .filter(|effect| {
                matches!(
                    effect.body,
                    LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
                )
            })
            .collect::<Vec<_>>();
    assert_eq!(prepares.len(), 1);
    let LogicalEffectBody::Native(NativeEffect::PrepareSuccessor {
        failed,
        replacement,
        topology,
        deadline,
    }) = &prepares[0].body
    else {
        unreachable!()
    };
    assert_eq!(*failed, activation(1).execution());
    assert_eq!(*replacement, activation(2).execution());
    assert_eq!(*topology, expected);
    assert_eq!(*deadline, at(7).saturating_add(Duration::from_millis(50)));
    let facts = r.f.core.facts();
    assert_eq!(facts.conclusion(), None);
    assert!(facts.failure().is_none());
    assert!(facts.supervision_failure().is_none());
    assert_eq!(facts.attempts_started(), 1);
    assert!(!facts.output_visible());
    assert!(facts.ready_cut().is_none());
    let first = facts
        .attempts()
        .find(|a| a.activation == activation(1))
        .unwrap();
    assert_eq!(first.accepted_failure, Some(original.input()));
    assert!(first.native_run_failure.is_none());
    assert_eq!(first.participations, roster(1));
    assert!(!first.all_participations_closed);
    assert!(!first.cleanup_complete);
}

#[test]
fn ordinary_backend_loss_excludes_the_exact_reported_process_across_all_three_failure_ingresses() {
    for ingress in 0..3 {
        let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
        r.establish_initial(AttemptIssueSettlement::Worker(OperationOutcome::Accepted));
        let process = participant(1, 2).backend_process_id();
        assert_ne!(process, root(1).backend_process_id());
        let original = ExecutionFailure::from_input(ExecutionFailureInput::new(
            QueryFailureReason::BackendLost.failure_fact(),
            SafeDetail::truncating("original non-root backend connection disappeared"),
            ExecutionFailureSite::Backend { process },
        ));
        assert_eq!(
            original.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert!(original.pre_ready_replacement().is_none());
        r.f.now = at(7);
        match ingress {
            0 => r.fail(activation(1), original.clone()),
            1 => {
                r.f.relay(RelayRequest::DeliveryFailed(FailureReport::from_attempt(
                    &original,
                )));
            }
            2 => {
                let run = r.f.run.take().unwrap();
                r.f.result(
                    run,
                    ActionResult::Native(NativeFact::Failed {
                        execution: activation(1).execution(),
                        failure: FailureReport::from_attempt(&original),
                    }),
                );
            }
            _ => unreachable!(),
        }
        assert_original_recovery(&r, &original, SuccessorTopology::ExcludeProcess(process));
        r.f.now = at(9);
        r.fail(activation(1), original.clone());
        assert_original_recovery(&r, &original, SuccessorTopology::ExcludeProcess(process));
    }
}

#[test]
fn coordinator_root_read_and_task_reports_cannot_guess_a_backend_process_or_authorize_replacement()
{
    for site in [
        ExecutionFailureSite::Coordinator,
        ExecutionFailureSite::RootRead { root: root(1) },
        ExecutionFailureSite::Task {
            identity: root(1),
            phase: TaskFailurePhase::Execution,
        },
    ] {
        let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
        r.establish_initial(AttemptIssueSettlement::Worker(OperationOutcome::Accepted));
        let original = ExecutionFailure::from_input(ExecutionFailureInput::new(
            QueryFailureReason::BackendLost.failure_fact(),
            SafeDetail::truncating("original dependency report without a backend-process author"),
            site,
        ));
        // The original classifier requires an explicit Backend author. A Task
        // identity is not permission to infer its process or force recovery.
        assert_eq!(original.class(), AttemptFailureClass::ExecutionFailure);
        r.f.now = at(7);
        r.fail(activation(1), original.clone());
        let facts = r.f.core.facts();
        assert_eq!(facts.conclusion(), Some(LogicalConclusion::Failed));
        assert_eq!(facts.failure(), Some(original.error()));
        assert_eq!(facts.attempts_started(), 1);
        assert!(!facts.output_visible());
        assert_eq!(
            facts.attempts().next().unwrap().accepted_failure,
            Some(original.input())
        );
        assert!(!r.f.effects.iter().any(|effect| matches!(
            effect.body,
            LogicalEffectBody::Native(
                NativeEffect::PrepareSuccessor { .. }
                    | NativeEffect::DispatchQualification { .. }
                    | NativeEffect::ActivateReservation { .. }
            )
        )));
    }
}

#[test]
fn a_backend_site_does_not_make_nonrecoverable_transport_and_lease_causes_replaceable() {
    for reason in [
        QueryFailureReason::TransportDeadline,
        QueryFailureReason::TransportRefused,
        QueryFailureReason::ObservationLost,
        QueryFailureReason::TaskLeaseExpired,
    ] {
        let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
        r.establish_initial(AttemptIssueSettlement::Worker(OperationOutcome::Accepted));
        let original = ExecutionFailure::from_input(ExecutionFailureInput::new(
            reason.failure_fact(),
            SafeDetail::truncating("original bounded transport or lease failure"),
            ExecutionFailureSite::Backend {
                process: participant(1, 2).backend_process_id(),
            },
        ));
        assert_eq!(original.class(), AttemptFailureClass::ExecutionFailure);
        r.f.now = at(7);
        r.fail(activation(1), original.clone());
        let facts = r.f.core.facts();
        assert_eq!(facts.conclusion(), Some(LogicalConclusion::Failed));
        assert_eq!(facts.failure(), Some(original.error()));
        assert_eq!(facts.attempts_started(), 1);
        assert!(!facts.output_visible());
        assert_eq!(
            facts.attempts().next().unwrap().accepted_failure,
            Some(original.input())
        );
        assert!(!r.f.effects.iter().any(|effect| matches!(
            effect.body,
            LogicalEffectBody::Native(
                NativeEffect::PrepareSuccessor { .. }
                    | NativeEffect::DispatchQualification { .. }
                    | NativeEffect::ActivateReservation { .. }
            )
        )));
    }
}

#[test]
fn excluding_a_process_for_identity_refusal_still_requires_its_original_observed_parent() {
    for observed_parent in [false, true] {
        let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
        let p = participant(1, 1);
        let grant = r.grant(activation(1), p, 10);
        if observed_parent {
            let issued = r.issue(activation(1), p, grant, 10);
            r.settle(issued, AttemptIssueSettlement::TransportOwned)
                .unwrap();
            r.settle(issued, AttemptIssueSettlement::TransportUnknown)
                .unwrap();
        }
        let op = operation(6000);
        let input = ExecutionFailureInput::new(
            FailureFact::new(
                FailureClass::Invariant,
                TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
            ),
            SafeDetail::truncating("original CreateTask identity refusal needs its parent"),
            ExecutionFailureSite::Operation {
                participation: p,
                operation_id: op,
                kind: OperationKind::CreateTask,
            },
        );
        let witness = PreReadyProcessReplacementWitness::try_new(
            &input,
            p,
            op,
            participant(2, 3).backend_process_id(),
        )
        .unwrap();
        let mut original = ExecutionFailure::from_input(input);
        original.attach_pre_ready_replacement(witness).unwrap();
        assert_eq!(original.class(), AttemptFailureClass::ContractViolation);
        r.f.now = at(7);
        r.fail(activation(1), original.clone());
        if observed_parent {
            assert_original_recovery(
                &r,
                &original,
                SuccessorTopology::ExcludeProcess(p.backend_process_id()),
            );
        } else {
            assert_eq!(
                r.f.core.facts().conclusion(),
                Some(LogicalConclusion::Failed)
            );
            assert_eq!(r.f.core.facts().failure(), Some(original.error()));
            assert!(!r.f.effects.iter().any(|effect| matches!(
                effect.body,
                LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
            )));
            assert_eq!(
                r.f.core.facts().attempts().next().unwrap().accepted_failure,
                Some(original.input())
            );
            assert!(!r.f.core.facts().output_visible());
        }
    }
}
