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

use super::super::AttemptEffectEnvelope;
use crate::control_runtime::host::purpose_tests::stopped_admission_fixture;
use novarocks_attempt_protocol::{
    effect::{AttemptEffect, EffectBody, EffectClass, OperationIntent, QueuedInputPayload},
    input::SendGeneration,
};
use novarocks_control_runtime::exit_work::HandoffState;
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{ParticipationRef, QuiesceParticipation, TaskOperationId};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use std::{num::NonZeroU64, sync::Arc};

#[test]
fn stopped_quiesce_admission_reaches_teardown_through_its_original_cell() {
    let participation = ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(1, 1), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    );
    let id = AttemptActionId::new(NonZeroU64::MIN);
    let operation = OperationIntent::QuiesceParticipation(QuiesceParticipation::new(
        TaskOperationId::new_v7(),
        participation,
    ));
    let lane = operation.lane();
    let admission = AttemptEffectEnvelope::new(AttemptEffect {
        id,
        class: EffectClass::Business,
        body: EffectBody::AdmitQueuedInput {
            operation,
            generation: SendGeneration::new(id),
            payload: QueuedInputPayload::AlreadyRetained,
            lane,
        },
    });
    assert_eq!(admission.handoff().unwrap().state(), HandoffState::Pending);
    let original = stopped_admission_fixture(admission.clone())
        .expect("stopped admission must reach its original unused teardown");
    assert!(Arc::ptr_eq(&original, &admission));
    assert_eq!(original.handoff().unwrap().state(), HandoffState::Withdrawn);
}
