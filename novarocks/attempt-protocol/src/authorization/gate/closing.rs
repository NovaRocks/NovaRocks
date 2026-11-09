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

//! Finish the original local gate without claiming transport or remote exit.

use super::{AuthorizationGate, QaReply, schedule};
use crate::{
    authorization::establish::{SettlementReply, Step},
    capacity::StepContext,
    effect::{AttemptEffect, EffectBody, EffectClass},
    error::TaskExecutionError,
    failure::AttemptFailure,
    terminal::GatePreparation,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptAuthorizationRef, AttemptRequestToken, AttemptToLogicalBody,
    LogicalToAttemptBody,
};
use novarocks_execution_contract::TaskOperationId;
use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Unbounded},
};

#[derive(Clone, Copy, Debug, Default)]
enum Phase {
    #[default]
    Open,
    Admission(Option<TaskOperationId>),
    Establish(Option<TaskOperationId>),
    WaitingTransport,
    ForgetAdmission(Option<TaskOperationId>),
    Detach(Option<TaskOperationId>),
    Ready,
    Failed,
}

#[derive(Clone, Copy, Debug)]
struct ClosingFlight {
    operation: TaskOperationId,
    token: AttemptRequestToken,
    authorization: AttemptAuthorizationRef,
    detach: bool,
}

#[derive(Debug, Default)]
pub(super) struct ClosingGate {
    phase: Phase,
    waits_for_transport: bool,
    flight: Option<ClosingFlight>,
}

#[derive(Debug)]
pub(crate) enum CloseProgress {
    Yield,
    Request(AttemptEffect),
    AwaitReply,
    WaitingTransport,
    Ready,
}

impl ClosingGate {
    pub(super) fn waiting_for_reply(&self) -> bool {
        self.flight.is_some()
    }

    pub(super) fn is_closed_to_business(&self) -> bool {
        !matches!(self.phase, Phase::Open)
    }

    pub(super) fn transport_changed(&mut self) {
        if matches!(self.phase, Phase::WaitingTransport) {
            self.phase = Phase::Admission(None);
            self.waits_for_transport = false;
        }
    }
}

impl AuthorizationGate {
    pub(crate) fn request_close(&mut self) {
        if !self.close.is_closed_to_business() {
            self.close.phase = if self.admissions.is_empty() && self.establishments.is_empty() {
                Phase::Ready
            } else {
                Phase::Admission(None)
            };
        }
    }

    pub(crate) fn close_needs_drive(&self) -> bool {
        matches!(
            self.close.phase,
            Phase::Admission(_)
                | Phase::Establish(_)
                | Phase::ForgetAdmission(_)
                | Phase::Detach(_)
        )
    }

    pub(crate) fn preparation(&self) -> GatePreparation {
        match self.close.phase {
            Phase::Open => GatePreparation::Open,
            Phase::WaitingTransport => GatePreparation::Applied { ready: false },
            Phase::Ready => GatePreparation::Applied { ready: true },
            _ => GatePreparation::Applying,
        }
    }

    /// Only after the original ACK journal has drained. The tail first
    /// settles current unused guards; late guards remain reachable until
    /// the original gate has no transport-owned rows.
    pub(crate) fn advance_close(
        &mut self,
        action: AttemptActionId,
        token: AttemptRequestToken,
        context: &mut dyn StepContext,
    ) -> Result<CloseProgress, TaskExecutionError> {
        if self.waiting_for_reply() {
            return Ok(CloseProgress::AwaitReply);
        }
        if self.selected_establish.is_some() {
            return Err(schedule("closing overtook an original Establish observer"));
        }
        loop {
            match self.close.phase {
                Phase::Open => return Err(schedule("original authorization gate is still open")),
                Phase::WaitingTransport => return Ok(CloseProgress::WaitingTransport),
                Phase::Ready => return Ok(CloseProgress::Ready),
                Phase::Failed => {
                    return Err(schedule(
                        "original authorization guard could not be disposed",
                    ));
                }
                Phase::Admission(after) | Phase::ForgetAdmission(after) => {
                    let forget = matches!(self.close.phase, Phase::ForgetAdmission(_));
                    if !context.charge(1) {
                        return Ok(CloseProgress::Yield);
                    }
                    if let Some((&operation, row)) = next(&mut self.admissions, after) {
                        if forget {
                            if !row.forget_closed_receipt() {
                                return Err(schedule(
                                    "closed Admission retains its original transport",
                                ));
                            }
                            self.close.phase = Phase::ForgetAdmission(Some(operation));
                        } else {
                            row.close();
                            self.close.waits_for_transport |= row.emitted.is_some();
                            self.close.phase = Phase::Admission(Some(operation));
                        }
                    } else {
                        self.close.phase = if forget {
                            Phase::Detach(None)
                        } else {
                            Phase::Establish(None)
                        };
                    }
                }
                Phase::Establish(after) | Phase::Detach(after) => {
                    let detach = matches!(self.close.phase, Phase::Detach(_));
                    if !context.charge(1) {
                        return Ok(CloseProgress::Yield);
                    }
                    let Some((&operation, row)) = next(&mut self.establishments, after) else {
                        self.close.phase = if detach {
                            Phase::Ready
                        } else if self.close.waits_for_transport {
                            Phase::WaitingTransport
                        } else {
                            Phase::ForgetAdmission(None)
                        };
                        continue;
                    };
                    row.close();
                    let participation = row.participation();
                    let step = if detach {
                        row.advance(token)
                    } else {
                        row.advance_closing_current(token)
                    };
                    let (authorization, body, class) = match step {
                        Step::Settle {
                            authorization,
                            settlement,
                            ..
                        } if !detach => (
                            authorization,
                            AttemptToLogicalBody::EstablishSettlement {
                                token,
                                authorization,
                                participation,
                                operation_id: operation,
                                settlement,
                            },
                            EffectClass::Settlement,
                        ),
                        Step::DetachLate { authorization, .. } if detach => (
                            authorization,
                            AttemptToLogicalBody::DetachEstablish {
                                token,
                                authorization,
                                participation,
                                operation_id: operation,
                            },
                            EffectClass::Cleanup,
                        ),
                        Step::WaitingTransport if !detach => {
                            self.close.waits_for_transport = true;
                            self.close.phase = Phase::Establish(Some(operation));
                            continue;
                        }
                        Step::Ready => {
                            if detach {
                                if !row.forget_closed_local_entry() {
                                    return Err(schedule(
                                        "closed Establish retains its original guard",
                                    ));
                                }
                                self.close.phase = Phase::Detach(Some(operation));
                            } else {
                                self.close.phase = Phase::Establish(Some(operation));
                            }
                            continue;
                        }
                        _ => {
                            return Err(schedule(
                                "unexpected original authorization close transition",
                            ));
                        }
                    };
                    self.close.flight = Some(ClosingFlight {
                        operation,
                        token,
                        authorization,
                        detach,
                    });
                    return Ok(CloseProgress::Request(AttemptEffect {
                        id: action,
                        class,
                        body: EffectBody::Port(self.port(action, body)),
                    }));
                }
            }
        }
    }

    pub(super) fn close_replied(
        &mut self,
        body: LogicalToAttemptBody<AttemptFailure>,
    ) -> Result<QaReply, TaskExecutionError> {
        let flight = self.close.flight.expect("original closing flight");
        let row = self
            .establishments
            .get_mut(&flight.operation)
            .expect("original closing Establish row");
        match body {
            LogicalToAttemptBody::EstablishSettlementReply {
                token,
                authorization,
                result,
            } if !flight.detach
                && token == flight.token
                && authorization == flight.authorization =>
            {
                if !matches!(
                    row.settled(token, authorization, result)?,
                    SettlementReply::Complete { .. }
                ) {
                    return Err(schedule("original unused Establish did not settle"));
                }
            }
            LogicalToAttemptBody::EstablishDetachReply {
                token,
                authorization,
                result,
            } if flight.detach
                && token == flight.token
                && authorization == flight.authorization =>
            {
                if let Err(failure) = result {
                    self.close.phase = Phase::Failed;
                    self.close.flight = None;
                    return Ok(QaReply::ClosingFailed(failure));
                }
                if !row.detached(token, authorization)? {
                    return Err(schedule("original late Establish did not detach"));
                }
            }
            _ => return Ok(QaReply::Stale),
        }
        self.close.flight = None;
        Ok(QaReply::ClosingProgress)
    }
}

fn next<V>(
    rows: &mut BTreeMap<TaskOperationId, V>,
    after: Option<TaskOperationId>,
) -> Option<(&TaskOperationId, &mut V)> {
    match after {
        Some(after) => rows.range_mut((Excluded(after), Unbounded)).next(),
        None => rows.iter_mut().next(),
    }
}
