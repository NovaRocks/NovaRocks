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

//! Executes decisions on the original workload objects. The FE carrier owns
//! execution and reports result and actual capture exit separately.

use crate::{
    api::{QueryExecutionError, logical_runtime::LogicalFuture},
    execution_error::{ExecutionFailureInput, ExecutionFailureSite},
};
use novarocks_coordination_contract::{AttemptActivationIdentity, LogicalExecutionActorId};
use novarocks_execution_contract::{
    SafeDetail,
    failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
};
use novarocks_logical_execution_protocol::{
    effect::GovernanceEffect,
    input::{GovernanceFact, ObligationKind},
};
use novarocks_types::QueryId;
use novarocks_workload_control::{
    CancellationReason, Obligation, ObligationKey, Stage, StagePermit, WorkCancellationRequester,
    WorkError, WorkOwner, WorkScope,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Mutex, MutexGuard},
};

/// Construction refusal returns the unique original owners to the installer.
pub struct GovernanceInstallRefusal {
    pub error: QueryExecutionError,
    pub owner: WorkOwner,
    pub stage: StagePermit,
}

impl fmt::Debug for GovernanceInstallRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GovernanceInstallRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum End {
    Resolved,
    RemoteTrackingEnded,
}
struct OriginalObligation {
    original: Option<Obligation>,
    ended: Option<End>,
}
struct State {
    owner: Option<WorkOwner>,
    stage: Option<StagePermit>,
    obligations: BTreeMap<ObligationKey, OriginalObligation>,
    completed: bool,
    registering: BTreeSet<ObligationKey>,
    settling: BTreeSet<ObligationKey>,
}

pub struct GovernanceExecutor {
    identity: QueryId,
    actor: LogicalExecutionActorId,
    scope: WorkScope,
    cancellation: WorkCancellationRequester,
    state: Mutex<State>,
}

impl GovernanceExecutor {
    pub fn new(
        identity: QueryId,
        actor: LogicalExecutionActorId,
        owner: WorkOwner,
        stage: StagePermit,
    ) -> Result<Self, GovernanceInstallRefusal> {
        let scope = owner.scope();
        if let Err(error) = stage.check(&scope, Stage::Execution) {
            return Err(GovernanceInstallRefusal {
                error: work_failure(error),
                owner,
                stage,
            });
        }
        let cancellation = owner.cancellation_requester();
        Ok(Self {
            identity,
            actor,
            scope,
            cancellation,
            state: Mutex::new(State {
                owner: Some(owner),
                stage: Some(stage),
                obligations: BTreeMap::new(),
                completed: false,
                registering: BTreeSet::new(),
                settling: BTreeSet::new(),
            }),
        })
    }

    pub fn scope(&self) -> WorkScope {
        self.scope.clone()
    }

    pub fn execute(&self, effect: GovernanceEffect) -> LogicalFuture<'_, GovernanceFact> {
        Box::pin(async move { self.apply(effect).unwrap_or_else(GovernanceFact::Failed) })
    }

    fn apply(&self, effect: GovernanceEffect) -> Result<GovernanceFact, QueryExecutionError> {
        match effect {
            GovernanceEffect::RegisterObligation { activation, kind } => {
                let key = self.key(activation, kind)?;
                {
                    let mut state = self.lock();
                    if state.completed {
                        return Err(protocol_failure("work owner already completed"));
                    }
                    if state.obligations.contains_key(&key) {
                        return Ok(GovernanceFact::ObligationRegistered { activation, kind });
                    }
                    if !state.registering.insert(key) {
                        return Err(protocol_failure(
                            "original obligation registration is already in flight",
                        ));
                    }
                }
                // Workload operations may wake callers; never invoke them under
                // this executor's lock. A panic leaves the in-flight marker
                // retained rather than authoring a fabricated completion.
                let result = self.scope.register_obligation(key, wc_kind(kind));
                let mut state = self.lock();
                state.registering.remove(&key);
                let original = result.map_err(work_failure)?;
                state.obligations.insert(
                    key,
                    OriginalObligation {
                        original: Some(original),
                        ended: None,
                    },
                );
                Ok(GovernanceFact::ObligationRegistered { activation, kind })
            }
            GovernanceEffect::ResolveObligation { activation, kind } => {
                self.settle(activation, kind, End::Resolved)?;
                Ok(GovernanceFact::ObligationResolved { activation, kind })
            }
            GovernanceEffect::EndRemoteTracking { activation, kind } => {
                self.settle(activation, kind, End::RemoteTrackingEnded)?;
                Ok(GovernanceFact::RemoteTrackingEnded { activation, kind })
            }
            GovernanceEffect::ReleaseExecutionStage => {
                let stage = self.lock().stage.take();
                drop(stage);
                Ok(GovernanceFact::StageReleased)
            }
            GovernanceEffect::RequestShutdownCancellation => {
                self.cancellation
                    .request(CancellationReason::ServerShutdown)
                    .map_err(work_failure)?;
                Ok(GovernanceFact::CancellationRequested)
            }
            GovernanceEffect::RequestClientCancellation => {
                self.cancellation
                    .request(CancellationReason::ClientDisconnected)
                    .map_err(work_failure)?;
                Ok(GovernanceFact::CancellationRequested)
            }
            GovernanceEffect::CompleteWorkOwner => {
                let owner = {
                    let mut state = self.lock();
                    if state.stage.is_some()
                        || !state.registering.is_empty()
                        || !state.settling.is_empty()
                        || state.obligations.values().any(|o| o.ended.is_none())
                    {
                        return Err(protocol_failure(
                            "work completion still owns an execution stage or unsettled obligation",
                        ));
                    }
                    state.completed = true;
                    state.owner.take()
                };
                if let Some(owner) = owner {
                    owner.complete_after_terminal_cancel_settled();
                }
                Ok(GovernanceFact::WorkOwnerCompleted)
            }
        }
    }

    fn settle(
        &self,
        activation: AttemptActivationIdentity,
        kind: ObligationKind,
        end: End,
    ) -> Result<(), QueryExecutionError> {
        let key = self.key(activation, kind)?;
        let original = {
            let mut state = self.lock();
            let record = state.obligations.get(&key).ok_or_else(|| {
                protocol_failure("governance names an unregistered original obligation")
            })?;
            if let Some(old) = record.ended {
                return if old == end {
                    Ok(())
                } else {
                    Err(protocol_failure(
                        "governance cannot rewrite the original obligation end reason",
                    ))
                };
            }
            let original = record
                .original
                .as_ref()
                .ok_or_else(|| protocol_failure("original obligation is absent before settlement"))?
                .clone();
            if !state.settling.insert(key) {
                return Err(protocol_failure(
                    "original obligation settlement is already in flight",
                ));
            }
            original
        };
        let applied = match end {
            End::Resolved => original.resolve(),
            End::RemoteTrackingEnded => original.end_remote_tracking_unknown(),
        };
        let mut state = self.lock();
        state.settling.remove(&key);
        if !applied {
            return Err(responsibility_failure());
        }
        let record = state
            .obligations
            .get_mut(&key)
            .expect("original settlement retains its record");
        record.ended = Some(end);
        let retained = record.original.take();
        drop(state);
        drop(original);
        drop(retained);
        Ok(())
    }

    fn key(
        &self,
        activation: AttemptActivationIdentity,
        kind: ObligationKind,
    ) -> Result<ObligationKey, QueryExecutionError> {
        if activation.actor() != self.actor || activation.execution().query_id() != self.identity {
            return Err(protocol_failure(
                "governance activation belongs to another logical execution",
            ));
        }
        // Scope-qualified exact encoding: kind cannot alias its sibling. This
        // is process-local data, not a wire identity or a second ID issuer.
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&self.actor.get().to_le_bytes());
        bytes[8..16].copy_from_slice(&activation.execution().attempt_id().get().to_le_bytes());
        bytes[16..24].copy_from_slice(&activation.generation().to_le_bytes());
        bytes[24..].copy_from_slice(&kind_tag(kind).to_le_bytes());
        Ok(ObligationKey(bytes))
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

const fn kind_tag(kind: ObligationKind) -> u64 {
    match kind {
        ObligationKind::RunningWork => 1,
        ObligationKind::RetiredAttempt => 2,
    }
}
const fn wc_kind(kind: ObligationKind) -> novarocks_workload_control::ObligationKind {
    match kind {
        ObligationKind::RunningWork => novarocks_workload_control::ObligationKind::RunningWork,
        ObligationKind::RetiredAttempt => {
            novarocks_workload_control::ObligationKind::RetiredAttempt
        }
    }
}
fn work_failure(error: WorkError) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        error.failure_fact(),
        SafeDetail::truncating("original workload governance operation failed"),
        ExecutionFailureSite::Coordinator,
    ))
}
fn protocol_failure(message: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(84).unwrap()),
        ),
        SafeDetail::truncating(message),
        ExecutionFailureSite::Coordinator,
    ))
}
fn responsibility_failure() -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Unexpected,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(93).unwrap()),
        ),
        SafeDetail::truncating("stand-down residual responsibility could not be settled"),
        ExecutionFailureSite::Coordinator,
    ))
}

#[cfg(test)]
#[path = "governance/tests.rs"]
mod tests;
