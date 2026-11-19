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

//! Exact independent process-replacement evidence remains passive and correlated.

use novarocks_coordination_contract::{
    ExecutionFailure, ExecutionFailureInput, ExecutionFailureSite, IdSeed, OperationIds,
    PreReadyProcessReplacementWitness, TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
};
use novarocks_execution_contract::{
    OperationKind, ParticipationRef, SafeDetail, TaskOperationId,
    failure::{FailureClass, FailureFact},
};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};

fn operation(seed: u128) -> TaskOperationId {
    OperationIds::new(IdSeed::new(seed, 100)).issue()
}
fn backend(seed: u128) -> BackendProcessId {
    BackendProcessId::try_from_bytes(operation(seed).to_bytes()).unwrap()
}

#[test]
fn exact_replacement_evidence_rejects_foreign_operation_site_and_process() {
    let participation = ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(29, 31), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(operation(700).to_bytes()).unwrap(),
        backend(1),
    );
    let original_operation = operation(100);
    for kind in [
        OperationKind::UpdateParticipation,
        OperationKind::CreateTask,
    ] {
        let input = ExecutionFailureInput::new(
            FailureFact::new(
                FailureClass::Invariant,
                TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
            ),
            SafeDetail::truncating("backend process id mismatch"),
            ExecutionFailureSite::Operation {
                participation,
                operation_id: original_operation,
                kind,
            },
        );
        let witness = PreReadyProcessReplacementWitness::try_new(
            &input,
            participation,
            original_operation,
            backend(2),
        )
        .unwrap();
        assert!(witness.matches_input(&input));
        assert_eq!(
            PreReadyProcessReplacementWitness::refusal_operation(&input),
            Some((participation, original_operation, kind))
        );
        assert!(
            PreReadyProcessReplacementWitness::try_new(
                &input,
                participation,
                original_operation,
                backend(1)
            )
            .is_err()
        );
        assert!(
            PreReadyProcessReplacementWitness::try_new(
                &input,
                participation,
                operation(101),
                backend(2)
            )
            .is_err()
        );
        let foreign = ExecutionFailureInput::new(
            input.fact(),
            input.detail().clone(),
            ExecutionFailureSite::Backend {
                process: backend(1),
            },
        );
        assert!(
            PreReadyProcessReplacementWitness::try_new(
                &foreign,
                participation,
                original_operation,
                backend(2)
            )
            .is_err()
        );
        let other_kind = if kind == OperationKind::CreateTask {
            OperationKind::UpdateParticipation
        } else {
            OperationKind::CreateTask
        };
        let other = ExecutionFailureInput::new(
            input.fact(),
            input.detail().clone(),
            ExecutionFailureSite::Operation {
                participation,
                operation_id: original_operation,
                kind: other_kind,
            },
        );
        assert!(!witness.matches_input(&other));
        let mut failure = ExecutionFailure::from_input(input.clone());
        failure
            .attach_pre_ready_replacement(witness.clone())
            .unwrap();
        assert_eq!(failure.input(), &input);
        assert_eq!(failure.pre_ready_replacement(), Some(&witness));
        assert!(failure.attach_pre_ready_replacement(witness).is_err());
    }
}
