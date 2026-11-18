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

use crate::recovery::contract::*;
use crate::support::*;
use novarocks_coordination_contract::{
    AttemptIssueSettlement, AttemptRootFailure, AttemptRootProjection, ExecutionFailure,
    ExecutionFailureInput, QueryFailureReason, SelectedTaskTermination,
};
use novarocks_execution_contract::{
    SafeDetail, TaskFailure, TerminationDetail, failure::FailureProjection,
};
use std::num::NonZeroU64;

fn internal(
    r: &mut RecoveryFixture,
    a: AttemptActivationIdentity,
    source: u64,
    id: u64,
    request: InternalRequest,
) -> LogicalRequestToken {
    let token = LogicalRequestToken::new(NonZeroU64::new(id).unwrap());
    r.f.send(LogicalInput::Internal {
        source: LogicalSourceId::new(NonZeroU64::new(source).unwrap()),
        token,
        activation: a,
        request,
    });
    token
}
fn rejected(r: &RecoveryFixture, token: LogicalRequestToken) -> bool {
    r.f.effects.iter().any(|e| {
        matches!(
            &e.body, LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
                token: held, response: InternalResponse::Rejected(_), ..
            }) if *held == token
        )
    })
}
#[test]
fn deferred_old_end_retirement_keeps_logical_outcome_open_until_original_successor_failure() {
    let mut r = RecoveryFixture::new(LogicalOutputMode::InternalResultStream);
    r.establish_initial(AttemptIssueSettlement::TransportUnknown);
    let status = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(3).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::new(false),
    )
    .unwrap();
    r.send_port(
        activation(1),
        AttemptToLogicalBody::RootStatus(Box::new(AttemptRootProjection {
            status,
            failure: AttemptRootFailure::None,
        })),
    );
    internal(
        &mut r,
        activation(1),
        201,
        1001,
        InternalRequest::RegisterSource {
            root: root(1),
            plan: PLAN,
            supervised_exit: true,
        },
    );
    r.relay(
        activation(1),
        RelayRequest::OpenDelivery {
            root: root(1),
            carrier: carrier(true),
        },
    );
    // The data owner consumed this exact original End, but has not supplied
    // EndDecoded or a Ready proposal. Actual End Drop/Join belongs to P14.
    r.relay(
        activation(1),
        RelayRequest::EndConsumed {
            root: root(1),
            end: end(0, 27),
            accepted_packets: 0,
            delivered_through: None,
        },
    );
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert!(r.f.core.facts().ready_cut().is_none());
    assert!(!r.f.core.facts().output_visible());
    let first = backend_failure(1, "original A failure after deferred internal End");
    r.fail(activation(1), first.clone());
    let (dispatch, _) = r.prepare(roster(2));
    let grants = dispatch
        .replacement
        .iter()
        .enumerate()
        .map(|(i, p)| admission(*p, 81 + i as u8, 1000))
        .collect::<Vec<_>>();
    r.f.now = at(10);
    r.qualify(&dispatch, grants, at(5));
    let (b, _) = r.activate(&dispatch);
    let registration = internal(
        &mut r,
        b,
        202,
        1002,
        InternalRequest::RegisterSource {
            root: root(2),
            plan: PLAN,
            supervised_exit: true,
        },
    );
    assert!(r.f.effects.iter().any(|e| matches!(
        &e.body, LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
            token, response: InternalResponse::SourceRegistered, ..
        }) if *token == registration
    )));
    let old_error = novarocks_logical_execution_protocol::failure::query_failure(
        QueryFailureReason::InternalConsumerPanicked,
        "old A consumer finally returned its own original error",
    );
    let old_exit = internal(
        &mut r,
        activation(1),
        201,
        1003,
        InternalRequest::ExitObserved {
            outcome: Some(Err(old_error)),
        },
    );
    let old_decoded = internal(
        &mut r,
        activation(1),
        201,
        1004,
        InternalRequest::EndDecoded {
            receipt: InternalEndReceiptId(NonZeroU64::MIN),
            end: end(0, 27),
        },
    );
    assert!(rejected(&r, old_exit));
    assert!(rejected(&r, old_decoded));
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert!(r.f.core.facts().failure().is_none());
    assert!(!r.f.core.facts().original_internal_consumer_exited());
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(
            ConsumerEffect::PublishFailure(_) | ConsumerEffect::PublishReady { .. }
        )
    )));
    let selected = SelectedTaskTermination::new(
        root(2),
        TerminationDetail::Failed(TaskFailure::new(
            QueryFailureReason::ResultProtocol.failure_fact(),
            SafeDetail::truncating("B consumer rejected its original replacement result"),
        )),
    );
    let input = ExecutionFailureInput::from_selected_task(&selected).unwrap();
    let failure = ExecutionFailure::from_input(input.clone());
    internal(
        &mut r,
        b,
        202,
        1005,
        InternalRequest::Failed(FailureReport::from_attempt(&failure)),
    );
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Failed)
    );
    assert_eq!(r.f.core.facts().failure(), Some(failure.error()));
    assert_eq!(
        r.f.core
            .facts()
            .attempts()
            .find(|a| a.activation == activation(1))
            .unwrap()
            .accepted_failure,
        Some(first.input())
    );
    assert_eq!(
        r.f.core
            .facts()
            .attempts()
            .find(|a| a.activation == b)
            .unwrap()
            .accepted_failure,
        Some(&input)
    );
    let published =
        r.f.effects
            .iter()
            .filter_map(|e| match &e.body {
                LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(error)) => Some(error),
                _ => None,
            })
            .collect::<Vec<_>>();
    assert_eq!(published, vec![failure.error()]);
    assert!(r.f.core.facts().ready_cut().is_none());
}
