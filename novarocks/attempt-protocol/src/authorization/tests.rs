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

use super::*;
use crate::input::{AckPayload, LocalDispatchResult, OperationReplyResult};
use novarocks_execution_contract::{
    AdmissionEpochCapability, AdmissionTicketId, LeaseValidFor, OperationOutcome,
    ParticipationAdmissionTicketReceipt, operation::OperationResult,
};
use novarocks_types::NativeCompatibilityId;
use std::{num::NonZeroU64, time::Duration};

fn token(n: u64) -> AttemptRequestToken {
    AttemptRequestToken::new(NonZeroU64::new(n).unwrap())
}
fn issue(n: u64) -> AttemptAuthorizationRef {
    AttemptAuthorizationRef::new(NonZeroU64::new(n).unwrap())
}
fn gate() -> AdmissionAuthorization {
    let (graph, _) = crate::test_support::graph_with_strict_input(false);
    let part = *graph.participations().next().unwrap();
    let mut ids = crate::ids::OperationIds::new(crate::ids::IdSeed::new(17, 200));
    AdmissionAuthorization::new(AcquireParticipationAdmissionTicket::new(
        ids.issue(),
        part,
        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        AdmissionEpochCapability::try_from_bytes([7; 16]).unwrap(),
    ))
}
fn begin(gate: &mut AdmissionAuthorization) {
    assert!(matches!(
        gate.authorize(token(1)),
        AdmissionGateDirective::Request(AttemptAuthorization::BeginAdmission(_))
    ));
    assert_eq!(
        gate.replied(
            token(1),
            Ok(AttemptAuthorizationDecision::AdmissionStarted(issue(11)))
        )
        .unwrap(),
        AdmissionGateReply::Authorized
    );
}
fn local(gate: &AdmissionAuthorization, result: LocalDispatchResult) -> OperationReply {
    OperationReply {
        operation_id: gate.request.envelope().operation_id(),
        kind: gate.request.envelope().kind(),
        result: OperationReplyResult::Local {
            result,
            failure: None,
            protocol_failure: None,
            detail: None,
        },
    }
}
fn failure() -> AttemptFailure {
    AttemptFailure::TaskProtocol(Box::new(TaskExecutionError::UnknownOperation))
}

#[test]
fn repeated_carrier_full_keeps_one_begin_and_same_original_request() {
    let mut gate = gate();
    let expected = gate.request;
    let AdmissionGateDirective::Request(AttemptAuthorization::BeginAdmission(request)) =
        gate.authorize(token(1))
    else {
        panic!("one original begin")
    };
    assert_eq!(
        request.envelope().operation_id(),
        expected.envelope().operation_id()
    );
    assert_eq!(request.participation(), expected.participation());
    assert_eq!(request.valid_for(), expected.valid_for());
    for n in 2..100 {
        assert!(matches!(
            gate.authorize(token(n)),
            AdmissionGateDirective::Waiting
        ));
        assert!(!gate.permits_protocol_turn());
    }
    assert_eq!(
        gate.replied(
            token(2),
            Ok(AttemptAuthorizationDecision::AdmissionStarted(issue(12)))
        )
        .unwrap(),
        AdmissionGateReply::Stale
    );
    assert!(!gate.permits_protocol_turn());
    gate.replied(
        token(1),
        Ok(AttemptAuthorizationDecision::AdmissionStarted(issue(11))),
    )
    .unwrap();
    for n in 100..200 {
        assert!(matches!(
            gate.authorize(token(n)),
            AdmissionGateDirective::Authorized
        ));
    }
    assert_eq!(gate.issue, Some(issue(11)));
}

#[test]
fn unknown_replay_reuses_the_issue_and_preworker_refusal_keeps_prior_unknown() {
    let mut gate = gate();
    begin(&mut gate);
    let unknown = local(&gate, LocalDispatchResult::TransportUnknown);
    assert!(matches!(
        gate.settle(token(2), &unknown).unwrap(),
        AdmissionSettlementDirective::TransportUnknown
    ));
    assert!(gate.permits_protocol_turn());
    assert!(matches!(
        gate.authorize(token(3)),
        AdmissionGateDirective::Authorized
    ));
    let rejected = local(&gate, LocalDispatchResult::NonWorkerRejected);
    let AdmissionSettlementDirective::Request(AttemptAuthorization::SettleAdmission {
        issue: selected,
        operation_id,
        prior_transport_unknown,
        outcome,
        receipt,
        ..
    }) = gate.settle(token(4), &rejected).unwrap()
    else {
        panic!("settle original issue")
    };
    assert_eq!(selected, issue(11));
    assert_eq!(operation_id, unknown.operation_id());
    assert!(prior_transport_unknown && outcome.is_none() && receipt.is_none());
    assert!(!gate.permits_protocol_turn());
    assert_eq!(
        gate.replied(
            token(4),
            Ok(AttemptAuthorizationDecision::AdmissionRetryable)
        )
        .unwrap(),
        AdmissionGateReply::Settled
    );
    assert!(matches!(
        gate.authorize(token(5)),
        AdmissionGateDirective::Retired
    ));
}

#[test]
fn granted_and_retryable_dispositions_do_not_create_gate_owned_ticket_or_retry_policy() {
    for disposition in [
        AttemptAuthorizationDecision::AdmissionGranted,
        AttemptAuthorizationDecision::AdmissionRetryable,
    ] {
        let mut gate = gate();
        begin(&mut gate);
        let accepted = matches!(disposition, AttemptAuthorizationDecision::AdmissionGranted);
        let receipt = ParticipationAdmissionTicketReceipt::new(
            AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
            gate.request.participation(),
            gate.request.valid_for(),
        );
        let reply = OperationReply {
            operation_id: gate.request.envelope().operation_id(),
            kind: gate.request.envelope().kind(),
            result: OperationReplyResult::Worker(OperationResult::Verdict {
                outcome: if accepted {
                    OperationOutcome::Accepted
                } else {
                    OperationOutcome::ResourceExhausted
                },
                detail: None,
                acknowledgement: if accepted {
                    Some(AckPayload::AdmissionTicket(receipt))
                } else {
                    None
                },
            }),
        };
        let AdmissionSettlementDirective::Request(AttemptAuthorization::SettleAdmission {
            receipt: selected,
            prior_transport_unknown,
            ..
        }) = gate.settle(token(2), &reply).unwrap()
        else {
            panic!("original actual receipt")
        };
        assert_eq!(selected, accepted.then_some(receipt));
        assert!(!prior_transport_unknown);
        assert_eq!(
            gate.replied(token(2), Ok(disposition)).unwrap(),
            AdmissionGateReply::Settled
        );
        assert!(gate.issue.is_none());
        assert!(matches!(
            gate.authorize(token(3)),
            AdmissionGateDirective::Retired
        ));
    }
}

#[test]
fn actual_settlement_error_retains_original_receipt_and_unknown_for_later_verdict() {
    let mut gate = gate();
    begin(&mut gate);
    let unknown = local(&gate, LocalDispatchResult::TransportUnknown);
    gate.settle(token(2), &unknown).unwrap();
    let rejected = local(&gate, LocalDispatchResult::NonWorkerRejected);
    gate.settle(token(3), &rejected).unwrap();
    assert_eq!(
        gate.replied(
            token(3),
            Err(AttemptAuthorizationFailure::Rejected(failure()))
        )
        .unwrap(),
        AdmissionGateReply::Failed {
            failure: failure(),
            retire_queued_authorization: true
        }
    );
    assert_eq!(gate.issue, Some(issue(11)));
    assert!(gate.prior_unknown);
    assert!(matches!(
        gate.authorize(token(4)),
        AdmissionGateDirective::Retired
    ));
    let AdmissionSettlementDirective::Request(AttemptAuthorization::SettleAdmission {
        issue: selected,
        prior_transport_unknown,
        ..
    }) = gate.settle(token(5), &rejected).unwrap()
    else {
        panic!("same receipt can settle later observed fact")
    };
    assert_eq!(selected, issue(11));
    assert!(prior_transport_unknown);
    gate.replied(
        token(5),
        Ok(AttemptAuthorizationDecision::AdmissionRetryable),
    )
    .unwrap();
    assert!(gate.issue.is_none());
}

#[test]
fn closing_during_begin_keeps_the_original_call_but_never_reopens_native_authorization() {
    let mut gate = gate();
    gate.authorize(token(1));
    gate.close();
    assert!(!gate.forget_closed_receipt());
    assert!(matches!(
        gate.authorize(token(2)),
        AdmissionGateDirective::Waiting
    ));
    gate.replied(
        token(1),
        Ok(AttemptAuthorizationDecision::AdmissionStarted(issue(11))),
    )
    .unwrap();
    assert!(matches!(
        gate.authorize(token(3)),
        AdmissionGateDirective::Retired
    ));
    assert!(gate.forget_closed_receipt());
    assert!(gate.issue.is_none() && gate.permits_protocol_turn());
}

#[test]
fn foreign_operation_and_wrong_reply_shape_cannot_consume_the_original_call() {
    let other = gate();
    let mut gate = gate();
    gate.authorize(token(1));
    assert!(
        gate.replied(
            token(1),
            Ok(AttemptAuthorizationDecision::EstablishAuthorized(issue(22)))
        )
        .is_err()
    );
    assert!(!gate.permits_protocol_turn());
    gate.replied(
        token(1),
        Ok(AttemptAuthorizationDecision::AdmissionStarted(issue(11))),
    )
    .unwrap();
    let mut reply = local(&gate, LocalDispatchResult::NonWorkerRejected);
    let mut ids = crate::ids::OperationIds::new(crate::ids::IdSeed::new(18, 200));
    reply.operation_id = ids.issue();
    assert!(gate.settle(token(2), &reply).is_err());
    assert_eq!(gate.issue, Some(issue(11)));
    assert!(gate.permits_protocol_turn());
    assert_eq!(other.issue, None);
}
