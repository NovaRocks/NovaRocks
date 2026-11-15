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

//! Fixed purpose routing; no participation count contributes source positions.
use super::{
    adapter::LogicalHostEffect,
    geometry::{ATTEMPT_PURPOSES, EXECUTION_PURPOSES},
};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_logical_execution_protocol::{
    effect::{
        ConsumerEffect as C, GovernanceEffect as G, LogicalEffectBody as E, NativeEffect as N,
        RelayEffect as R,
    },
    input::ObligationKind,
    start::LogicalStart,
};
use novarocks_types::QueryExecutionId;
use std::collections::BTreeMap;

pub(super) struct LogicalPurposes {
    identity: novarocks_types::QueryId,
    actor: novarocks_coordination_contract::LogicalExecutionActorId,
    max_attempts: usize,
    attempts: BTreeMap<QueryExecutionId, usize>,
}
impl LogicalPurposes {
    pub(super) fn new(start: &LogicalStart) -> Self {
        Self {
            identity: start.identity,
            actor: start.actor_instance,
            max_attempts: start.budgets.max_attempts.get() as usize,
            attempts: [(start.first_attempt.activation.execution(), 0)].into(),
        }
    }
    pub(super) fn activation(&mut self, a: AttemptActivationIdentity) -> Option<usize> {
        if a.actor() != self.actor {
            return None;
        }
        self.attempt(a.execution())
    }
    pub(super) fn attempt(&mut self, e: QueryExecutionId) -> Option<usize> {
        if e.query_id() != self.identity {
            return None;
        }
        if let Some(index) = self.attempts.get(&e) {
            return Some(*index);
        }
        let index = self.attempts.len();
        if index == self.max_attempts {
            return None;
        }
        self.attempts.insert(e, index);
        Some(index)
    }
    pub(super) fn effect(&mut self, body: &LogicalHostEffect) -> Option<usize> {
        let mut a = None;
        let mut e = None;
        let mut global = false;
        let n = match body {
            LogicalHostEffect::JoinReadiness(_) => {
                global = true;
                15
            }
            LogicalHostEffect::Bootstrap => {
                global = true;
                14
            }
            LogicalHostEffect::ClientCloseObservation => {
                global = true;
                16
            }
            LogicalHostEffect::PortDelivered(activation) => {
                a = Some(*activation);
                23
            }
            LogicalHostEffect::Protocol(effect) => match &effect.body {
                E::Native(effect) => match effect {
                    N::ActivateAttempt { activation, .. } => {
                        a = Some(*activation);
                        0
                    }
                    N::RunAttempt { activation } => {
                        a = Some(*activation);
                        1
                    }
                    N::FinishResources { activation } | N::AbandonResources { activation } => {
                        a = Some(*activation);
                        2
                    }
                    N::RecordTerminalCut { activation, .. } => {
                        a = Some(*activation);
                        3
                    }
                    N::RecordStreamingSuccess { activation } => {
                        a = Some(*activation);
                        4
                    }
                    N::RecordComputationReady { cut } => {
                        a = Some(cut.activation);
                        5
                    }
                    N::PrepareSuccessor { replacement, .. } => {
                        e = Some(*replacement);
                        6
                    }
                    N::DispatchQualification { identity, .. } => {
                        e = Some(identity.replacement);
                        7
                    }
                    N::ActivateReservation { identity, .. } => {
                        e = Some(identity.replacement);
                        8
                    }
                    N::CancelQualification { identity, .. } => {
                        e = Some(identity.replacement);
                        9
                    }
                    N::AbandonReservation { identity } => {
                        e = Some(identity.replacement);
                        10
                    }
                    N::ConvergeDormant { execution, .. } => {
                        e = Some(*execution);
                        11
                    }
                    N::RetirePreparation => {
                        global = true;
                        7
                    }
                },
                E::Relay(effect) => match effect {
                    R::Start { activation } => {
                        a = Some(*activation);
                        12
                    }
                    R::Stop { activation } => {
                        a = Some(*activation);
                        13
                    }
                    R::Reply { activation, .. } => {
                        a = Some(*activation);
                        14
                    }
                },
                E::Consumer(effect) => match effect {
                    C::WriteSchema { activation } => {
                        a = Some(*activation);
                        15
                    }
                    C::ReserveEndSlot { activation } => {
                        a = Some(*activation);
                        16
                    }
                    C::EnqueueEnd { activation } => {
                        a = Some(*activation);
                        17
                    }
                    C::PublishAttemptFailure { activation, .. } => {
                        a = Some(*activation);
                        18
                    }
                    C::ExposeOutput { .. } => {
                        global = true;
                        0
                    }
                    C::PublishFailure(_) => {
                        global = true;
                        1
                    }
                    C::PublishCompletion => {
                        global = true;
                        2
                    }
                    C::InternalReply { .. } => {
                        global = true;
                        3
                    }
                    C::PublishReady { .. } => {
                        global = true;
                        4
                    }
                    C::PublishDiagnostics { .. } => {
                        global = true;
                        5
                    }
                    C::PublishConvergence { .. } => {
                        global = true;
                        6
                    }
                },
                E::Governance(effect) => match effect {
                    G::RegisterObligation { activation, kind } => {
                        a = Some(*activation);
                        19 + kind_index(*kind)
                    }
                    G::ResolveObligation { activation, kind }
                    | G::EndRemoteTracking { activation, kind } => {
                        a = Some(*activation);
                        21 + kind_index(*kind)
                    }
                    G::ReleaseExecutionStage => {
                        global = true;
                        8
                    }
                    G::RequestShutdownCancellation | G::RequestClientCancellation => {
                        global = true;
                        9
                    }
                    G::CompleteWorkOwner => {
                        global = true;
                        10
                    }
                },
                E::AttemptPort(message) => {
                    a = Some(message.activation);
                    23
                }
                E::AttemptLifecycleDiagnostic(d) => {
                    a = Some(d.activation);
                    24
                }
                E::SnapshotReply { .. } => {
                    global = true;
                    11
                }
                E::RegistryJoinReply { .. } => {
                    global = true;
                    12
                }
                E::Retirable => {
                    global = true;
                    13
                }
                E::SupervisionFailed(_) => {
                    global = true;
                    11
                }
            },
        };
        if global {
            return (n < EXECUTION_PURPOSES).then_some(self.max_attempts * ATTEMPT_PURPOSES + n);
        }
        let index = if let Some(a) = a {
            self.activation(a)?
        } else {
            self.attempt(e?)?
        };
        (n < ATTEMPT_PURPOSES).then_some(index * ATTEMPT_PURPOSES + n)
    }
}
fn kind_index(kind: ObligationKind) -> usize {
    match kind {
        ObligationKind::RunningWork => 0,
        ObligationKind::RetiredAttempt => 1,
    }
}
