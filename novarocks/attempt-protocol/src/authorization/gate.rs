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

//! Select the original admission or Establish row only when the ordered ACK
//! journal reaches settlement. One actual QA reply may be outstanding.

use super::{
    AdmissionAuthorization, AdmissionGateDirective, AdmissionGateReply,
    AdmissionSettlementDirective,
    establish::{
        AuthorizationReply, Directive, EstablishAuthorization, EstablishAuthorizationStorage,
        SettlementReply, Step,
    },
    observations::ObserverDebt,
};
use crate::{
    effect::{AttemptEffect, EffectBody, EffectClass, OperationIntent},
    error::TaskExecutionError,
    failure::AttemptFailure,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptAuthorization, AttemptAuthorizationLimits,
    AttemptAuthorizationRef, AttemptRequestToken, AttemptToLogical, AttemptToLogicalBody,
    LogicalToAttemptBody,
};
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Row {
    Admission(TaskOperationId),
    Establish(TaskOperationId),
}

#[derive(Clone, Copy, Debug)]
struct Flight {
    token: AttemptRequestToken,
    row: Row,
    settling: bool,
}

#[derive(Debug)]
pub(crate) enum AuthorizationStep {
    Request(AttemptEffect),
    Ready,
    Waiting,
    Retired,
}

#[derive(Debug)]
pub(crate) enum ObserverStep {
    Request(AttemptEffect),
    Consumed { retire: Option<TaskOperationId> },
    Waiting,
}

#[derive(Debug)]
pub(crate) enum QaReply {
    Stale,
    Authorized(TaskOperationId),
    AuthorizationFailed {
        operation: TaskOperationId,
        failure: AttemptFailure,
    },
    ObserverProgress,
    ObserverComplete {
        retire: Option<TaskOperationId>,
    },
}

#[derive(Debug)]
pub(crate) struct AuthorizationGate {
    activation: AttemptActivationIdentity,
    admission_owners: BTreeMap<ParticipationRef, Option<TaskOperationId>>,
    admissions: BTreeMap<TaskOperationId, AdmissionAuthorization>,
    establish_storage: BTreeMap<ParticipationRef, EstablishAuthorizationStorage>,
    establishments: BTreeMap<TaskOperationId, EstablishAuthorization>,
    flight: Option<Flight>,
    selected_establish: Option<TaskOperationId>,
    first_observer_failure: Option<AttemptFailure>,
}

impl AuthorizationGate {
    pub(crate) fn new(
        activation: AttemptActivationIdentity,
        limits: AttemptAuthorizationLimits,
        participations: impl Iterator<Item = ParticipationRef>,
    ) -> Result<Self, TaskExecutionError> {
        let mut admission_owners = BTreeMap::new();
        let mut establish_storage = BTreeMap::new();
        for participation in participations {
            admission_owners.insert(participation, None);
            establish_storage.insert(
                participation,
                EstablishAuthorizationStorage::new(activation, limits, participation)?,
            );
        }
        Ok(Self {
            activation,
            admission_owners,
            admissions: BTreeMap::new(),
            establish_storage,
            establishments: BTreeMap::new(),
            flight: None,
            selected_establish: None,
            first_observer_failure: None,
        })
    }

    pub(crate) fn waiting_for_reply(&self) -> bool {
        self.flight.is_some()
    }

    /// The candidate has already been chosen at the old gate's authorization
    /// point and its outbound effect has been charged. Replays reuse the row.
    pub(crate) fn authorize(
        &mut self,
        intent: &OperationIntent,
        token: AttemptRequestToken,
        action: AttemptActionId,
    ) -> Result<AuthorizationStep, TaskExecutionError> {
        if self.flight.is_some() || self.selected_establish.is_some() {
            return Ok(AuthorizationStep::Waiting);
        }
        let operation = intent.operation_id();
        let (row, request) = match intent {
            OperationIntent::AcquireParticipationAdmissionTicket(request) => {
                let owner = self
                    .admission_owners
                    .get_mut(&request.participation())
                    .ok_or(TaskExecutionError::UnknownOperation)?;
                if *owner != Some(operation) {
                    if let Some(previous) = *owner {
                        let row = &self.admissions[&previous];
                        if !row.retired || row.issue.is_some() {
                            return Err(schedule(
                                "new admission overtook its original authorization",
                            ));
                        }
                        self.admissions.remove(&previous);
                    }
                    self.admissions
                        .insert(operation, AdmissionAuthorization::new(*request));
                    *owner = Some(operation);
                }
                let admission = self
                    .admissions
                    .get_mut(&operation)
                    .expect("registered admission");
                let frozen = admission.request;
                if frozen.envelope() != request.envelope()
                    || frozen.participation() != request.participation()
                    || frozen.valid_for() != request.valid_for()
                    || frozen.native_compatibility_id() != request.native_compatibility_id()
                    || frozen.admission_epoch_capability() != request.admission_epoch_capability()
                {
                    return Err(schedule("admission replay changed its frozen request"));
                }
                let request = match admission.authorize(token) {
                    AdmissionGateDirective::Request(request) => request,
                    AdmissionGateDirective::Waiting => return Ok(AuthorizationStep::Waiting),
                    AdmissionGateDirective::Authorized => return Ok(AuthorizationStep::Ready),
                    AdmissionGateDirective::Retired => return Ok(AuthorizationStep::Retired),
                };
                (Row::Admission(operation), request)
            }
            OperationIntent::EstablishParticipation { participation, .. } => {
                if !self.establishments.contains_key(&operation) {
                    let storage = self
                        .establish_storage
                        .remove(participation)
                        .ok_or(TaskExecutionError::UnknownOperation)?;
                    self.establishments
                        .insert(operation, storage.bind(operation));
                }
                let establish = self
                    .establishments
                    .get_mut(&operation)
                    .expect("registered Establish");
                if establish.participation() != *participation {
                    return Err(schedule("Establish replay changed its participation"));
                }
                let request = match establish.authorize(token) {
                    Directive::Request(request) => request,
                    Directive::Waiting => return Ok(AuthorizationStep::Waiting),
                    Directive::IssueRecorded(_) => return Ok(AuthorizationStep::Ready),
                    Directive::Retired => return Ok(AuthorizationStep::Retired),
                };
                (Row::Establish(operation), request)
            }
            _ => return Err(TaskExecutionError::UnknownOperation),
        };
        self.flight = Some(Flight {
            token,
            row,
            settling: false,
        });
        let port = self.port(
            action,
            AttemptToLogicalBody::AuthorizationRequest { token, request },
        );
        let body = if matches!(request, AttemptAuthorization::Establish { .. }) {
            EffectBody::EstablishAuthorization {
                operation: intent.clone(),
                request: port,
            }
        } else {
            EffectBody::Port(port)
        };
        Ok(AuthorizationStep::Request(AttemptEffect {
            id: action,
            class: EffectClass::Business,
            body,
        }))
    }

    /// One selected debt or one guard from the same debt. The caller charges
    /// before advancing and retains the current journal row while waiting.
    pub(crate) fn settle_observation(
        &mut self,
        debt: ObserverDebt,
        token: AttemptRequestToken,
        action: AttemptActionId,
    ) -> Result<ObserverStep, TaskExecutionError> {
        if self.flight.is_some() {
            return Ok(ObserverStep::Waiting);
        }
        let (row, body) = match debt {
            ObserverDebt::Admission(observation) => {
                let operation = observation.operation_id;
                let Some(admission) = self.admissions.get_mut(&operation) else {
                    return Ok(ObserverStep::Consumed { retire: None });
                };
                let AdmissionSettlementDirective::Request(request) =
                    admission.settle_observed(token, observation)?
                else {
                    return Ok(ObserverStep::Consumed { retire: None });
                };
                (
                    Row::Admission(operation),
                    AttemptToLogicalBody::AuthorizationRequest { token, request },
                )
            }
            ObserverDebt::Establish(observation) => {
                let operation = observation.operation_id;
                let Some(establish) = self.establishments.get_mut(&operation) else {
                    // Other UpdateParticipation shapes have no QA observer.
                    return Ok(ObserverStep::Consumed { retire: None });
                };
                match self.selected_establish {
                    None => {
                        establish.begin_observed_settlement(observation)?;
                        self.selected_establish = Some(operation);
                    }
                    Some(selected) if selected == operation => {}
                    Some(_) => {
                        return Err(schedule("Establish observer advanced to another operation"));
                    }
                }
                match establish.advance(token) {
                    Step::Settle {
                        authorization,
                        settlement,
                        ..
                    } => {
                        let body = AttemptToLogicalBody::EstablishSettlement {
                            token,
                            authorization,
                            participation: establish.participation(),
                            operation_id: operation,
                            settlement,
                        };
                        (Row::Establish(operation), body)
                    }
                    Step::Ready => {
                        let completed = establish.finish_empty()?;
                        let retire = self.complete_establish(operation, completed)?;
                        return Ok(ObserverStep::Consumed { retire });
                    }
                    Step::Waiting | Step::WaitingTransport | Step::DetachLate { .. } => {
                        return Err(schedule("unexpected Establish observer transition"));
                    }
                }
            }
        };
        self.flight = Some(Flight {
            token,
            row,
            settling: true,
        });
        Ok(ObserverStep::Request(AttemptEffect {
            id: action,
            class: EffectClass::Settlement,
            body: EffectBody::Port(self.port(action, body)),
        }))
    }

    pub(crate) fn replied(
        &mut self,
        body: LogicalToAttemptBody<AttemptFailure>,
    ) -> Result<QaReply, TaskExecutionError> {
        let Some(flight) = self.flight else {
            return Ok(QaReply::Stale);
        };
        let reply = match (flight.row, flight.settling, body) {
            (
                Row::Admission(operation),
                settling,
                LogicalToAttemptBody::AuthorizationReply { token, decision },
            ) if token == flight.token => {
                let admission = self
                    .admissions
                    .get_mut(&operation)
                    .expect("original admission QA row");
                match admission.replied(token, decision)? {
                    AdmissionGateReply::Authorized if !settling => QaReply::Authorized(operation),
                    AdmissionGateReply::Settled if settling => QaReply::ObserverComplete {
                        retire: Some(operation),
                    },
                    AdmissionGateReply::Failed {
                        failure,
                        retire_queued_authorization,
                    } if settling => {
                        self.first_observer_failure.get_or_insert(failure);
                        QaReply::ObserverComplete {
                            retire: retire_queued_authorization.then_some(operation),
                        }
                    }
                    AdmissionGateReply::Failed { failure, .. } => {
                        QaReply::AuthorizationFailed { operation, failure }
                    }
                    AdmissionGateReply::Stale => return Ok(QaReply::Stale),
                    _ => return Err(schedule("admission QA reply crossed the selected phase")),
                }
            }
            (
                Row::Establish(operation),
                false,
                LogicalToAttemptBody::AuthorizationReply { token, decision },
            ) if token == flight.token => {
                let establish = self
                    .establishments
                    .get_mut(&operation)
                    .expect("original Establish QA row");
                match establish.authorized(token, decision)? {
                    AuthorizationReply::Stale => return Ok(QaReply::Stale),
                    AuthorizationReply::IssueRecorded(_) => QaReply::Authorized(operation),
                    AuthorizationReply::Failed(failure) => {
                        QaReply::AuthorizationFailed { operation, failure }
                    }
                }
            }
            (
                Row::Establish(operation),
                true,
                LogicalToAttemptBody::EstablishSettlementReply {
                    token,
                    authorization,
                    result,
                },
            ) if token == flight.token => {
                let establish = self
                    .establishments
                    .get_mut(&operation)
                    .expect("original Establish observer row");
                match establish.settled(token, authorization, result)? {
                    SettlementReply::Stale => return Ok(QaReply::Stale),
                    SettlementReply::Progress => QaReply::ObserverProgress,
                    completed => QaReply::ObserverComplete {
                        retire: self.complete_establish(operation, completed)?,
                    },
                }
            }
            _ => return Ok(QaReply::Stale),
        };
        self.flight = None;
        Ok(reply)
    }

    fn complete_establish(
        &mut self,
        operation: TaskOperationId,
        completed: SettlementReply,
    ) -> Result<Option<TaskOperationId>, TaskExecutionError> {
        let SettlementReply::Complete {
            failure,
            retire_queued_authorization,
        } = completed
        else {
            return Err(schedule("Establish observer did not complete"));
        };
        self.selected_establish = None;
        if let Some(failure) = failure {
            self.first_observer_failure.get_or_insert(failure);
        }
        Ok(retire_queued_authorization.then_some(operation))
    }

    /// Called on the original queued prefix before taking its reservation or
    /// consuming a grant. The candidate driver queues only authorized rows.
    pub(crate) fn validate_send(&self, intent: &OperationIntent) -> Result<(), TaskExecutionError> {
        let operation = intent.operation_id();
        let available = match intent {
            OperationIntent::AcquireParticipationAdmissionTicket(_) => {
                self.admissions.get(&operation).is_some_and(|row| {
                    row.issue.is_some() && !row.retired && !row.closing && row.flight.is_none()
                })
            }
            OperationIntent::EstablishParticipation { participation, .. } => {
                self.establishments.get(&operation).is_some_and(|row| {
                    row.participation() == *participation && row.available_for_emission().is_some()
                })
            }
            _ => true,
        };
        if !available {
            return Err(schedule(
                "Native send has no matching original QA authorization",
            ));
        }
        Ok(())
    }

    /// Call only after validate_send accepted the entire immutable prefix.
    pub(crate) fn note_sent(
        &mut self,
        intent: &OperationIntent,
        generation: crate::input::SendGeneration,
    ) {
        if matches!(intent, OperationIntent::EstablishParticipation { .. }) {
            let row = self
                .establishments
                .get_mut(&intent.operation_id())
                .expect("validated original Establish authorization");
            let authorization = row
                .available_for_emission()
                .expect("validated original Establish guard");
            row.emitted(authorization, generation)
                .expect("validated original Establish emission");
        }
    }

    pub(crate) fn note_establish_withdrawn(
        &mut self,
        operation: TaskOperationId,
        participation: ParticipationRef,
        generation: crate::input::SendGeneration,
    ) -> Result<(), TaskExecutionError> {
        let Some(row) = self
            .establishments
            .get_mut(&operation)
            .filter(|row| row.participation() == participation)
        else {
            return Err(TaskExecutionError::UnknownOperation);
        };
        if !row.withdrawn(generation) {
            return Err(schedule(
                "withdrawal differs from the emitted Establish generation",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn establish_emitted(
        &mut self,
        operation: TaskOperationId,
        authorization: AttemptAuthorizationRef,
        generation: crate::input::SendGeneration,
    ) -> Result<(), TaskExecutionError> {
        self.establishments
            .get_mut(&operation)
            .ok_or(TaskExecutionError::UnknownOperation)?
            .emitted(authorization, generation)
    }

    pub(crate) fn take_observer_failure(&mut self) -> Option<AttemptFailure> {
        self.first_observer_failure.take()
    }

    fn port(
        &self,
        action: AttemptActionId,
        body: AttemptToLogicalBody<AttemptFailure>,
    ) -> AttemptToLogical<AttemptFailure> {
        AttemptToLogical {
            activation: self.activation,
            action,
            body,
        }
    }
}

fn schedule(detail: &str) -> TaskExecutionError {
    TaskExecutionError::Schedule(detail.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        authorization::observations::{EstablishObservation, ObservedDispatch},
        failure::{AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure},
        ids::{IdSeed, OperationIds},
        input::SendGeneration,
        start::PayloadRef,
    };
    use novarocks_coordination_contract::{
        AttemptAuthorizationDecision, AttemptFailureClass, AttemptIssueSettlement,
    };
    use novarocks_execution_contract::{
        AdmissionTicketId, LeaseSequence, LeaseValidFor, OperationEnvelope, OperationKind,
        OperationOutcome,
    };
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        time::Duration,
    };

    fn nz(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).unwrap()
    }

    fn token(n: u64) -> AttemptRequestToken {
        AttemptRequestToken::new(nz(n))
    }

    fn action(n: u64) -> AttemptActionId {
        AttemptActionId::new(nz(n))
    }

    fn issue(n: u64) -> AttemptAuthorizationRef {
        AttemptAuthorizationRef::new(nz(n))
    }

    fn settlement_effect(
        step: ObserverStep,
    ) -> (
        AttemptRequestToken,
        AttemptAuthorizationRef,
        AttemptIssueSettlement,
    ) {
        let ObserverStep::Request(AttemptEffect {
            body: EffectBody::Port(port),
            ..
        }) = step
        else {
            panic!("expected an actual QA settlement request");
        };
        let AttemptToLogicalBody::EstablishSettlement {
            token,
            authorization,
            settlement,
            ..
        } = port.body
        else {
            panic!("expected Establish guard settlement");
        };
        (token, authorization, settlement)
    }

    #[test]
    fn definitive_establish_settles_oldest_guard_then_current_before_consuming_debt() {
        let (graph, activation) = crate::test_support::graph_with_strict_input(false);
        let participation = *graph.participations().next().unwrap();
        let operation = OperationIds::new(IdSeed::new(20, 200)).issue();
        let mut gate = AuthorizationGate::new(
            activation,
            AttemptAuthorizationLimits {
                activation,
                establish_per_participation: NonZeroUsize::new(2).unwrap(),
            },
            graph.participations().copied(),
        )
        .unwrap();
        let intent = OperationIntent::EstablishParticipation {
            envelope: OperationEnvelope::with_default_wait(
                operation,
                OperationKind::UpdateParticipation,
            ),
            participation,
            ticket: AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
            initial_lease_sequence: LeaseSequence::INITIAL,
            initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
            material: PayloadRef(nz(4)),
            queued_bytes: 256,
        };
        let debt = |result| {
            ObserverDebt::Establish(EstablishObservation {
                operation_id: operation,
                result,
            })
        };
        let result = |n, guard, result| LogicalToAttemptBody::EstablishSettlementReply {
            token: token(n),
            authorization: issue(guard),
            result,
        };
        let authorize = |gate: &mut AuthorizationGate, n| {
            let AuthorizationStep::Request(effect) =
                gate.authorize(&intent, token(n), action(n)).unwrap()
            else {
                panic!("expected original authorization request");
            };
            assert!(match effect.body {
                EffectBody::EstablishAuthorization {
                    operation: original,
                    ..
                } if n == 1 => original.operation_id() == operation,
                EffectBody::Port(port) if n == 3 => matches!(
                    port.body, AttemptToLogicalBody::AuthorizationRequest {
                        request: AttemptAuthorization::ReauthorizeEstablish { operation_id, .. }, ..
                    } if operation_id == operation
                ),
                _ => false,
            });
            assert!(
                matches!(gate.replied(LogicalToAttemptBody::AuthorizationReply {
                token: token(n),
                decision: Ok(AttemptAuthorizationDecision::EstablishAuthorized(issue(n))),
            }).unwrap(), QaReply::Authorized(authorized) if authorized == operation)
            );
            gate.establish_emitted(operation, issue(n), SendGeneration::new(action(n)))
                .unwrap();
        };
        authorize(&mut gate, 1);
        let unknown = debt(ObservedDispatch::Unknown);
        assert_eq!(
            settlement_effect(
                gate.settle_observation(unknown, token(2), action(2))
                    .unwrap()
            ),
            (token(2), issue(1), AttemptIssueSettlement::TransportUnknown)
        );
        assert!(matches!(
            gate.replied(result(2, 1, Ok(()))).unwrap(),
            QaReply::ObserverComplete { retire: None }
        ));
        authorize(&mut gate, 3);

        let worker = debt(ObservedDispatch::Worker(OperationOutcome::Accepted));
        let applied = AttemptIssueSettlement::Worker(OperationOutcome::Accepted);
        assert_eq!(
            settlement_effect(
                gate.settle_observation(worker, token(4), action(4))
                    .unwrap()
            ),
            (token(4), issue(1), applied),
        );
        assert!(matches!(
            gate.replied(result(4, 3, Ok(()))).unwrap(),
            QaReply::Stale
        ));
        assert!(matches!(
            gate.settle_observation(worker, token(5), action(5))
                .unwrap(),
            ObserverStep::Waiting
        ));
        let original = AttemptFailure::Host(HostProjectedFailure {
            original: HostFailureRef(nz(90)),
            kind: HostFailureKind::EstablishAuthorization,
            class: AttemptFailureClass::ExecutionFailure,
            site: AttemptFailureSite::Attempt,
        });
        assert!(matches!(
            gate.replied(result(4, 1, Err(original.clone()))).unwrap(),
            QaReply::ObserverProgress
        ));
        assert_eq!(
            settlement_effect(
                gate.settle_observation(worker, token(5), action(5))
                    .unwrap()
            ),
            (token(5), issue(3), applied),
        );
        assert!(matches!(
            gate.replied(result(5, 3, Ok(()))).unwrap(),
            QaReply::ObserverComplete { retire: Some(retired) } if retired == operation
        ));
        assert_eq!(gate.take_observer_failure(), Some(original));
        assert!(matches!(
            gate.authorize(&intent, token(6), action(6)).unwrap(),
            AuthorizationStep::Retired
        ));
    }
}
