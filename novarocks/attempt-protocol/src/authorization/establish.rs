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

//! Pure correlation for one immutable Establish operation. The caller charges
//! each advance and uses the original post-round gate phase. Rich guards stay
//! in the original bridge; a ref and the historical M grant no send capacity.

#[cfg(test)]
mod tests;
#[cfg(test)]
use crate::operation::OperationDispatchResult;
use crate::{error::TaskExecutionError, failure::AttemptFailure, input::SendGeneration};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptAuthorization, AttemptAuthorizationDecision,
    AttemptAuthorizationFailure, AttemptAuthorizationLimits, AttemptAuthorizationRef,
    AttemptIssueSettlement, AttemptRequestToken,
};
use novarocks_execution_contract::{OperationOutcome, ParticipationRef, TaskOperationId};
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Directive {
    Request(AttemptAuthorization),
    /// Correlation only: emission must recheck this row and the global gate.
    IssueRecorded,
    Waiting,
    Retired,
}
#[derive(Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "the synchronous QA reply moves a typed Establish rejection directly to the gate without boxing it"
)]
pub(crate) enum AuthorizationReply {
    Stale,
    IssueRecorded(AttemptAuthorizationRef),
    Failed(AttemptFailure),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Step {
    Settle {
        token: AttemptRequestToken,
        authorization: AttemptAuthorizationRef,
        settlement: AttemptIssueSettlement,
    },
    /// Drop one original local late guard off the poller. QA retains the
    /// residual remote obligation. Wait for actual local disposal completion.
    DetachLate {
        token: AttemptRequestToken,
        authorization: AttemptAuthorizationRef,
    },
    Waiting,
    WaitingTransport,
    Ready,
}
#[derive(Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "the original settlement returns the optional first typed failure and exact retirement decision on the stack"
)]
pub(crate) enum SettlementReply {
    Stale,
    Progress,
    Complete {
        failure: Option<AttemptFailure>,
        retire_queued_authorization: bool,
    },
}
#[derive(Clone, Copy, Debug)]
struct Current {
    authorization: AttemptAuthorizationRef,
    emitted: Option<SendGeneration>,
}
#[derive(Clone, Copy, Debug)]
enum Selection {
    Unknown,
    PreWorker,
    Worker(OperationOutcome),
    ClosingUnsent,
}
#[derive(Debug)]
struct Pass {
    owns_acknowledgement: bool,
    selection: Selection,
    first_error: Option<AttemptFailure>,
}
#[derive(Clone, Copy, Debug)]
enum FlightKind {
    Settle(AttemptIssueSettlement),
    DetachLate,
}
#[derive(Clone, Copy, Debug)]
struct Flight {
    token: AttemptRequestToken,
    authorization: AttemptAuthorizationRef,
    kind: FlightKind,
}
#[derive(Debug)]
pub(crate) struct EstablishAuthorization {
    participation: ParticipationRef,
    operation_id: TaskOperationId,
    attempted: bool,
    current: Option<Current>,
    // Preallocate SAME actual QA M before registration. late+current <= M;
    // Unknown on generation M may make late alone reach M.
    late: VecDeque<AttemptAuthorizationRef>,
    late_entry: bool,
    limit: usize,
    authorization_flight: Option<AttemptRequestToken>,
    pass: Option<Pass>,
    flight: Option<Flight>,
    closing: bool,
    retired: bool,
}
/// Registration-time metadata storage. The immutable Establish operation does
/// not exist until its original admission receipt is accepted; preallocation
/// must not mint that operation early or resize this FIFO on a poller.
#[derive(Debug)]
pub(crate) struct EstablishAuthorizationStorage {
    participation: ParticipationRef,
    late: VecDeque<AttemptAuthorizationRef>,
    limit: usize,
}
impl EstablishAuthorizationStorage {
    pub(crate) fn new(
        activation: AttemptActivationIdentity,
        limits: AttemptAuthorizationLimits,
        participation: ParticipationRef,
    ) -> Result<Self, TaskExecutionError> {
        if limits.activation != activation {
            return Err(invalid("Establish limits belong to another activation"));
        }
        let limit = limits.establish_per_participation.get();
        Ok(Self {
            participation,
            late: VecDeque::with_capacity(limit),
            limit,
        })
    }
    /// Bind once to the original owner-generated operation. Moving this
    /// already priced storage allocates no row and issues no authorization.
    pub(crate) fn bind(self, operation_id: TaskOperationId) -> EstablishAuthorization {
        EstablishAuthorization {
            participation: self.participation,
            operation_id,
            attempted: false,
            current: None,
            late: self.late,
            limit: self.limit,
            late_entry: false,
            authorization_flight: None,
            pass: None,
            flight: None,
            closing: false,
            retired: false,
        }
    }
}
impl EstablishAuthorization {
    pub(crate) fn participation(&self) -> ParticipationRef {
        self.participation
    }

    pub(crate) fn permits_protocol_turn(&self) -> bool {
        self.authorization_flight.is_none() && self.pass.is_none() && self.flight.is_none()
    }
    pub(crate) fn authorize(&mut self, token: AttemptRequestToken) -> Directive {
        if !self.permits_protocol_turn() {
            return Directive::Waiting;
        }
        if self.closing || self.retired {
            return Directive::Retired;
        }
        if self.current.is_some() {
            return Directive::IssueRecorded;
        }
        let replay = self.attempted || !self.late.is_empty();
        // Before awaiting, including a subsequently cancelled original call.
        self.attempted = true;
        self.authorization_flight = Some(token);
        Directive::Request(if replay {
            AttemptAuthorization::ReauthorizeEstablish {
                participation: self.participation,
                operation_id: self.operation_id,
            }
        } else {
            AttemptAuthorization::Establish {
                participation: self.participation,
                operation_id: self.operation_id,
            }
        })
    }
    pub(crate) fn authorized(
        &mut self,
        token: AttemptRequestToken,
        result: Result<AttemptAuthorizationDecision, AttemptAuthorizationFailure<AttemptFailure>>,
    ) -> Result<AuthorizationReply, TaskExecutionError> {
        if self.authorization_flight != Some(token) {
            return Ok(AuthorizationReply::Stale);
        }
        let reply = match result {
            Ok(AttemptAuthorizationDecision::EstablishAuthorized(authorization)) => {
                if self.current.is_some() || self.late.len() >= self.limit {
                    return Err(invalid(
                        "Establish reply exceeds the original QA metadata bound",
                    ));
                }
                // Bridge validates freshness/identity against the actual guard.
                self.current = Some(Current {
                    authorization,
                    emitted: None,
                });
                AuthorizationReply::IssueRecorded(authorization)
            }
            Err(AttemptAuthorizationFailure::Rejected(failure)) => {
                AuthorizationReply::Failed(failure)
            }
            Err(AttemptAuthorizationFailure::EstablishBudgetExhausted(failure)) => {
                let failure = if self.late.is_empty() {
                    failure
                } else {
                    let AttemptFailure::Host(original) = failure else {
                        return Err(invalid("QA budget failure lacks its original host error"));
                    };
                    AttemptFailure::EstablishReplayExhausted {
                        original,
                        participation: self.participation,
                        operation_id: self.operation_id,
                    }
                };
                AuthorizationReply::Failed(failure)
            }
            _ => return Err(invalid("Establish QA reply differs from its original call")),
        };
        if matches!(reply, AuthorizationReply::Failed(_)) {
            // Original finish_authorization records a rejected carrier. A
            // repeated candidate must consume that rejection, never reissue QA.
            self.retired = true;
        }
        self.authorization_flight = None;
        Ok(reply)
    }
    pub(crate) fn available_for_emission(&self) -> Option<AttemptAuthorizationRef> {
        if self.closing || self.retired || !self.permits_protocol_turn() {
            return None;
        }
        self.current
            .filter(|current| current.emitted.is_none())
            .map(|current| current.authorization)
    }

    /// Original SendBatch emission, distinct from RPC first poll and exit.
    pub(crate) fn emitted(
        &mut self,
        authorization: AttemptAuthorizationRef,
        generation: SendGeneration,
    ) -> Result<(), TaskExecutionError> {
        if self.closing || self.retired || !self.permits_protocol_turn() {
            return Err(invalid("Establish emission crossed its authorization gate"));
        }
        let current = self
            .current
            .as_mut()
            .filter(|current| current.authorization == authorization && current.emitted.is_none())
            .ok_or_else(|| invalid("Establish emission has no unused matching current guard"))?;
        current.emitted = Some(generation);
        Ok(())
    }

    /// Accept only the matching generation's actual definitely-unsent result.
    /// Earlier transport-unknown guards stay in the original late FIFO.
    pub(crate) fn withdrawn(&mut self, generation: SendGeneration) -> bool {
        let Some(current) = self
            .current
            .as_mut()
            .filter(|current| current.emitted == Some(generation))
        else {
            return false;
        };
        current.emitted = None;
        true
    }
    /// Caller has already validated this exact op and filtered queued-replay
    /// old Unknown. Observer settlement follows the original round phases.
    #[cfg(test)]
    pub(crate) fn begin_settlement(
        &mut self,
        result: OperationDispatchResult,
    ) -> Result<(), TaskExecutionError> {
        self.begin_observed_settlement(super::observations::EstablishObservation {
            operation_id: self.operation_id,
            result: super::observations::ObservedDispatch::from_dispatch(result),
        })
    }
    pub(crate) fn begin_observed_settlement(
        &mut self,
        observed: super::observations::EstablishObservation,
    ) -> Result<(), TaskExecutionError> {
        if observed.operation_id != self.operation_id {
            return Err(TaskExecutionError::UnknownOperation);
        }
        if !self.permits_protocol_turn() {
            return Err(invalid(
                "Establish ACK overtook its original serial QA call",
            ));
        }
        use super::observations::ObservedDispatch;
        let selection = match observed.result {
            ObservedDispatch::Unknown => Selection::Unknown,
            ObservedDispatch::Worker(outcome) => Selection::Worker(outcome),
            ObservedDispatch::PreWorker => Selection::PreWorker,
        };
        self.pass = Some(Pass {
            owns_acknowledgement: self.current.is_some() || self.late_entry,
            selection,
            first_error: None,
        });
        Ok(())
    }
    /// Late guards remain reachable until the whole gate has finished
    /// handling all still-emitted transports.
    pub(crate) fn advance_closing_current(&mut self, token: AttemptRequestToken) -> Step {
        if self.current.is_none() && self.pass.is_none() && self.permits_protocol_turn() {
            Step::Ready
        } else {
            self.advance(token)
        }
    }

    /// One precharged selected original guard. Full retains this same flight;
    /// no next item can be selected until the actual guard result arrives.
    pub(crate) fn advance(&mut self, token: AttemptRequestToken) -> Step {
        if self.flight.is_some() || self.authorization_flight.is_some() {
            return Step::Waiting;
        }
        if let Some(pass) = &self.pass {
            let selected = match pass.selection {
                Selection::Unknown => self
                    .current
                    .take()
                    .map(|c| (c.authorization, AttemptIssueSettlement::TransportUnknown)),
                Selection::PreWorker => self
                    .current
                    .take()
                    .map(|c| c.authorization)
                    .or_else(|| self.late.pop_front())
                    .map(|authorization| {
                        (authorization, AttemptIssueSettlement::PreWorkerRejected)
                    }),
                Selection::Worker(outcome) => self
                    .late
                    .pop_front()
                    .or_else(|| self.current.take().map(|c| c.authorization))
                    .map(|authorization| (authorization, AttemptIssueSettlement::Worker(outcome))),
                Selection::ClosingUnsent => self
                    .current
                    .take()
                    .map(|c| (c.authorization, AttemptIssueSettlement::DefinitelyUnsent)),
            };
            if let Some((authorization, settlement)) = selected {
                self.flight = Some(Flight {
                    token,
                    authorization,
                    kind: FlightKind::Settle(settlement),
                });
                return Step::Settle {
                    token,
                    authorization,
                    settlement,
                };
            }
            return Step::Ready;
        }
        if !self.closing {
            return Step::Ready;
        }
        if let Some(current) = self.current {
            if current.emitted.is_some() {
                return Step::WaitingTransport;
            }
            self.pass = Some(Pass {
                owns_acknowledgement: true,
                selection: Selection::ClosingUnsent,
                first_error: None,
            });
            return self.advance(token);
        }
        if let Some(authorization) = self.late.pop_front() {
            self.flight = Some(Flight {
                token,
                authorization,
                kind: FlightKind::DetachLate,
            });
            return Step::DetachLate {
                token,
                authorization,
            };
        }
        Step::Ready
    }
    pub(crate) fn settled(
        &mut self,
        token: AttemptRequestToken,
        authorization: AttemptAuthorizationRef,
        result: Result<(), AttemptFailure>,
    ) -> Result<SettlementReply, TaskExecutionError> {
        let Some(flight) = self
            .flight
            .filter(|f| f.token == token && f.authorization == authorization)
        else {
            return Ok(SettlementReply::Stale);
        };
        let FlightKind::Settle(settlement) = flight.kind else {
            return Err(invalid("Establish settlement answered a local detach"));
        };
        let pass = self
            .pass
            .as_mut()
            .ok_or_else(|| invalid("Establish settlement lost its pass"))?;
        if settlement == AttemptIssueSettlement::TransportUnknown && result.is_ok() {
            // Removing the current guard left room in the same M allocation.
            self.late.push_back(authorization);
            self.late_entry = true;
        }
        if let Err(failure) = result {
            // Original prepare_convergence deliberately ignores this one error.
            if !matches!(pass.selection, Selection::ClosingUnsent) && pass.first_error.is_none() {
                pass.first_error = Some(failure);
            }
        }
        self.flight = None;
        if matches!(pass.selection, Selection::Worker(_))
            && (!self.late.is_empty() || self.current.is_some())
        {
            return Ok(SettlementReply::Progress);
        }
        self.finish_pass()
    }
    /// Finish a genuinely empty selection without inventing a guard result.
    pub(crate) fn finish_empty(&mut self) -> Result<SettlementReply, TaskExecutionError> {
        if self.flight.is_some() {
            return Err(invalid("Establish guard has not actually settled"));
        }
        let pass = self
            .pass
            .as_ref()
            .ok_or_else(|| invalid("Establish has no settlement pass"))?;
        let pending = match pass.selection {
            Selection::Unknown | Selection::ClosingUnsent => self.current.is_some(),
            Selection::PreWorker | Selection::Worker(_) => {
                self.current.is_some() || !self.late.is_empty()
            }
        };
        if pending {
            return Err(invalid("Establish settlement still has an original guard"));
        }
        self.finish_pass()
    }
    fn finish_pass(&mut self) -> Result<SettlementReply, TaskExecutionError> {
        let pass = self
            .pass
            .take()
            .ok_or_else(|| invalid("Establish has no settlement pass"))?;
        let definitive = pass.owns_acknowledgement
            && matches!(pass.selection, Selection::PreWorker | Selection::Worker(_));
        if matches!(pass.selection, Selection::Worker(_)) {
            self.late_entry = false;
        }
        if definitive {
            self.retired = true;
        }
        Ok(SettlementReply::Complete {
            failure: pass.first_error,
            retire_queued_authorization: definitive,
        })
    }
    pub(crate) fn close(&mut self) {
        self.closing = true;
    }
    /// Invoke only after the ORIGINAL whole gate has drained its ACKs and
    /// pending calls and accepted convergence preparation. Clearing this local
    /// correlation is not QA residual cleanup or physical exit. Do not clear
    /// merely when the last guard of this row has been detached.
    pub(crate) fn forget_closed_local_entry(&mut self) -> bool {
        if !self.closing
            || !self.permits_protocol_turn()
            || self.current.is_some()
            || !self.late.is_empty()
        {
            return false;
        }
        self.late_entry = false;
        true
    }
    /// Original local guard was actually dropped by its credentialed owner.
    /// This proves neither remote release nor that job/capture's actual exit.
    pub(crate) fn detached(
        &mut self,
        token: AttemptRequestToken,
        authorization: AttemptAuthorizationRef,
    ) -> Result<bool, TaskExecutionError> {
        let Some(flight) = self
            .flight
            .filter(|f| f.token == token && f.authorization == authorization)
        else {
            return Ok(false);
        };
        if !matches!(flight.kind, FlightKind::DetachLate) {
            return Err(invalid("Establish detach answered a settlement"));
        }
        self.flight = None;
        Ok(true)
    }
}
fn invalid(detail: &str) -> TaskExecutionError {
    TaskExecutionError::Schedule(detail.into())
}
