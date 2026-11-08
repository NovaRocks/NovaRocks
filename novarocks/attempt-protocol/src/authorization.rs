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

//! The pure portion of the original serial QA gate. Canonical receipt/permit/
//! submission objects remain with their original credentialed bridge. Primary
//! protocol input cannot overtake an outstanding old inline QA call; its reply,
//! actual exit and original credit progress remain independently reachable.

pub(crate) mod establish;

#[cfg(test)]
mod tests;

use crate::{error::TaskExecutionError, failure::AttemptFailure, input::OperationReply};
use novarocks_coordination_contract::{
    AttemptAuthorization, AttemptAuthorizationDecision, AttemptAuthorizationFailure,
    AttemptAuthorizationRef, AttemptRequestToken,
};
use novarocks_execution_contract::AcquireParticipationAdmissionTicket;

#[derive(Clone, Copy, Debug)]
enum AdmissionCall {
    Begin,
    Settle,
}
#[derive(Clone, Copy, Debug)]
struct AdmissionFlight {
    token: AttemptRequestToken,
    call: AdmissionCall,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AdmissionGateDirective {
    Request(AttemptAuthorization),
    Waiting,
    Authorized,
    Retired,
}
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum AdmissionGateReply {
    Stale,
    Authorized,
    Settled,
    Failed {
        failure: AttemptFailure,
        // The old definitive ACK retires a queued replay's gate entry even
        // when the actual QA settlement returns an error.
        retire_queued_authorization: bool,
    },
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AdmissionSettlementDirective {
    Unowned,
    TransportUnknown,
    Request(AttemptAuthorization),
}
#[derive(Debug)]
pub(crate) struct AdmissionAuthorization {
    request: AcquireParticipationAdmissionTicket,
    issue: Option<AttemptAuthorizationRef>,
    prior_unknown: bool,
    flight: Option<AdmissionFlight>,
    retired: bool,
    closing: bool,
}
impl AdmissionAuthorization {
    /// One constant-sized row for this original immutable admission operation.
    /// Its protocol owner, not this gate, decides ticket expiry and reissue.
    pub(crate) fn new(request: AcquireParticipationAdmissionTicket) -> Self {
        Self {
            request,
            issue: None,
            prior_unknown: false,
            flight: None,
            retired: false,
            closing: false,
        }
    }
    pub(crate) fn permits_protocol_turn(&self) -> bool {
        self.flight.is_none()
    }
    /// Caller charges the one row before this operation. Source-credit Full
    /// retains this original request; it cannot create another begin call.
    pub(crate) fn authorize(&mut self, token: AttemptRequestToken) -> AdmissionGateDirective {
        if self.flight.is_some() {
            return AdmissionGateDirective::Waiting;
        }
        if self.closing || self.retired {
            return AdmissionGateDirective::Retired;
        }
        if self.issue.is_some() {
            return AdmissionGateDirective::Authorized;
        }
        self.flight = Some(AdmissionFlight {
            token,
            call: AdmissionCall::Begin,
        });
        AdmissionGateDirective::Request(AttemptAuthorization::BeginAdmission(self.request))
    }
    /// Only an ACK belonging to this original operation enters this row. The
    /// Core must first filter an old Unknown behind a queued exact replay,
    /// matching the old observer-before-domain-settlement order.
    pub(crate) fn settle(
        &mut self,
        token: AttemptRequestToken,
        reply: &OperationReply,
    ) -> Result<AdmissionSettlementDirective, TaskExecutionError> {
        if reply.operation_id() != self.request.envelope().operation_id()
            || reply.kind() != self.request.envelope().kind()
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        if self.flight.is_some() {
            return Err(TaskExecutionError::Schedule(
                "admission ACK overtook its original serial QA call".into(),
            ));
        }
        let Some(issue) = self.issue else {
            return Ok(AdmissionSettlementDirective::Unowned);
        };
        if reply.dispatch_result() == crate::operation::OperationDispatchResult::TransportUnknown {
            self.prior_unknown = true;
            return Ok(AdmissionSettlementDirective::TransportUnknown);
        }
        let receipt = match reply.payload() {
            crate::input::AckPayload::AdmissionTicket(receipt) => Some(*receipt),
            _ => None,
        };
        self.flight = Some(AdmissionFlight {
            token,
            call: AdmissionCall::Settle,
        });
        Ok(AdmissionSettlementDirective::Request(
            AttemptAuthorization::SettleAdmission {
                issue,
                participation: self.request.participation(),
                operation_id: reply.operation_id(),
                outcome: reply.worker_outcome(),
                receipt,
                prior_transport_unknown: reply.worker_outcome().is_none() && self.prior_unknown,
            },
        ))
    }
    /// The bridge has actually executed the original QA call. Handoff, grant
    /// identity and local job exit alone never settle this call.
    pub(crate) fn replied(
        &mut self,
        token: AttemptRequestToken,
        result: Result<AttemptAuthorizationDecision, AttemptAuthorizationFailure<AttemptFailure>>,
    ) -> Result<AdmissionGateReply, TaskExecutionError> {
        let Some(flight) = self.flight.filter(|flight| flight.token == token) else {
            return Ok(AdmissionGateReply::Stale);
        };
        match (flight.call, result) {
            (AdmissionCall::Begin, Ok(AttemptAuthorizationDecision::AdmissionStarted(issue))) => {
                self.issue = Some(issue);
                self.flight = None;
                Ok(AdmissionGateReply::Authorized)
            }
            (
                AdmissionCall::Settle,
                Ok(
                    AttemptAuthorizationDecision::AdmissionGranted
                    | AttemptAuthorizationDecision::AdmissionRetryable,
                ),
            ) => {
                // The original gate ignores Granted/Retryable; ParticipationOwner
                // still interprets the actual Worker receipt and original time.
                self.issue = None;
                self.prior_unknown = false;
                self.flight = None;
                self.retired = true;
                Ok(AdmissionGateReply::Settled)
            }
            (_, Err(AttemptAuthorizationFailure::Rejected(failure))) => {
                self.flight = None;
                self.retired = true;
                // On Settle error the old Copy receipt and unknown marker stay
                // owned for a later observed verdict or convergence cleanup.
                Ok(AdmissionGateReply::Failed {
                    failure,
                    retire_queued_authorization: matches!(flight.call, AdmissionCall::Settle),
                })
            }
            (_, Err(AttemptAuthorizationFailure::EstablishBudgetExhausted(_))) => {
                Err(TaskExecutionError::Schedule(
                    "Establish budget failure cannot answer an admission QA call".into(),
                ))
            }
            _ => Err(TaskExecutionError::Schedule(
                "admission QA reply differs from its original call".into(),
            )),
        }
    }
    /// Close new authorization, but keep a running original QA call reachable.
    /// An already-issued Native operation and QA residual responsibility are
    /// neither withdrawn nor declared exited by this flag.
    pub(crate) fn close(&mut self) {
        self.closing = true;
    }
    /// The old convergence path forgets only its local Copy receipt. The QA
    /// ledger keeps every possibly granted participation and Abort obligation.
    pub(crate) fn forget_closed_receipt(&mut self) -> bool {
        if !self.closing || self.flight.is_some() {
            return false;
        }
        self.issue = None;
        self.prior_unknown = false;
        self.retired = true;
        true
    }
}
