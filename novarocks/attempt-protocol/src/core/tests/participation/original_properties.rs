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

//! The original single-participation admission-to-Establish gate.

use super::*;
use crate::port_output::AttemptAuthorization;
use novarocks_execution_contract::{AdmissionTicketId, ParticipationAdmissionTicketReceipt};

#[test]
fn original_participation_waits_for_its_own_acquire_receipt_before_establish() {
    let mut fixture = Fixture::new(BeforeStop::Pristine, 128);
    assert_eq!(fixture.core.execution.graph().participations().count(), 1);
    let SentAdmission {
        request,
        issue,
        generation,
        ..
    } = send_fixture_admission(&mut fixture);
    let participation = fixture.participation;
    assert_eq!(request.participation(), participation);
    let operation = OperationIntent::AcquireParticipationAdmissionTicket(request);
    fixture.drain(|effect| panic!("Acquire has no Worker receipt yet: {effect:?}"));
    assert_eq!(
        fixture.core.execution.owner(participation).unwrap().state(),
        ParticipationState::Absent
    );
    let ticket = ParticipationAdmissionTicketReceipt::new(
        AdmissionTicketId::try_from_bytes([0x71; 16]).unwrap(),
        participation,
        request.valid_for(),
    );
    let sources = fixture.sources;
    let received = fixture.step(
        sources.native,
        AttemptInput::Executor(ExecutorFact::Operation(reply(
            &operation,
            generation,
            worker(AckPayload::AdmissionTicket(ticket)),
        ))),
    );
    assert!(
        received.effect.is_none(),
        "a Worker receipt is not a same-input Establish send"
    );
    let EffectBody::Port(port) = fixture.expect_effect().body else {
        panic!("the original Acquire settles before Establish authorization");
    };
    let AttemptToLogicalBody::AuthorizationRequest {
        token,
        request:
            AttemptAuthorization::SettleAdmission {
                issue: original_issue,
                participation: original_part,
                operation_id,
                outcome: Some(OperationOutcome::Accepted),
                receipt: Some(original_ticket),
                prior_transport_unknown: false,
            },
    } = port.body
    else {
        panic!("the exact original Worker ticket must settle");
    };
    assert_eq!(original_issue, issue);
    assert_eq!(original_part, participation);
    assert_eq!(operation_id, operation.operation_id());
    assert_eq!(original_ticket, ticket);
    fixture.drain(|effect| panic!("QA settlement is still pending: {effect:?}"));
    assert!(
        reply_fixture_authorization(
            &mut fixture,
            token,
            AttemptAuthorizationDecision::AdmissionGranted
        )
        .effect
        .is_none()
    );
    let EffectBody::EstablishAuthorization {
        operation: establish,
        request: port,
    } = fixture.expect_effect().body
    else {
        panic!("a separate original candidate turn asks for Establish");
    };
    let OperationIntent::EstablishParticipation {
        participation: actual,
        ticket: actual_ticket,
        ..
    } = &establish
    else {
        panic!("the next request is the original participation's Establish");
    };
    assert_eq!(*actual, participation);
    assert_eq!(*actual_ticket, ticket.ticket_id());
    assert!(
        matches!(port.body, AttemptToLogicalBody::AuthorizationRequest {
        request: AttemptAuthorization::Establish { participation: p, operation_id }, ..
    } if p == participation && operation_id == establish.operation_id())
    );
    assert_eq!(
        fixture.core.execution.dispatcher().in_flight_operations(),
        0
    );
    assert_eq!(fixture.core.execution.dispatcher().queued_items(), 0);
}
