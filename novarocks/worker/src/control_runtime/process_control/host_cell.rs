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

//! Original Root inventory cell for the shared host poller. Each Context
//! transfer is prepared under Root, then bound with that Root guard released.

use super::{ProcessControlOwner, RootRequestAdvanceError};
use crate::QueryContextHostFactory;
use crate::control_runtime::{
    actors::{WorkerAction, WorkerActor},
    context_host::ContextHostError,
    context_host_jobs::{ContextHostJob, ContextHostJobError, MaterializationCaptureRefusal},
    control_request::{ControlRequestError, RequestIoProgress},
    establish_executors::EstablishExecutorHandles,
    establish_route::EstablishRouteError,
    service::ContextHostAddress,
};
use novarocks_control_runtime::driver::{
    ActorHostTurn, ActorRegistration, DriverError, EffectCompletionRefusal, EffectTicket,
};
use novarocks_worker_protocol::control::ControlEffect;
use std::sync::{Arc, Mutex, MutexGuard};

pub(crate) struct RootHostCell {
    owner: Mutex<ProcessControlOwner>,
    registration: ActorRegistration,
}
#[derive(Debug, Default)]
pub(crate) struct RootHostProgress {
    pub routed: usize,
    pub stale_targets: usize,
    pub requests: RequestIoProgress,
    pub rejected_ingress: super::rejected_ingress::RejectedIngressProgress,
    /// Only this turn's route/source classes, not all Node effects or Tasks.
    pub runnable: bool,
}
#[derive(Debug)]
pub(crate) enum RootMaterializationError {
    Driver(DriverError),
    Context(ContextHostError),
    Control(ControlRequestError),
    WrongTarget,
}
impl std::fmt::Display for RootMaterializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Driver(error) => {
                write!(f, "Root materialization actor operation failed: {error}")
            }
            Self::Context(error) => {
                write!(f, "Root materialization Context handoff failed: {error}")
            }
            Self::Control(error) => {
                write!(f, "Root materialization request operation failed: {error}")
            }
            Self::WrongTarget => {
                f.write_str("Root materialization target does not match the original Context")
            }
        }
    }
}
impl std::error::Error for RootMaterializationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Driver(error) => Some(error),
            Self::Context(error) => Some(error),
            Self::Control(error) => Some(error),
            Self::WrongTarget => None,
        }
    }
}

/// Every refusal preserves the exclusive Root ticket. Captures which have
/// already moved to the original Context inventory are never copied back.
#[must_use = "Retain the original ticket and every refused capture"]
pub(crate) enum RootMaterializationRefusal {
    Rejected {
        error: RootMaterializationError,
        ticket: EffectTicket<WorkerAction>,
        _target: ContextHostAddress,
        _factory: Arc<dyn QueryContextHostFactory>,
    },
    Capture {
        ticket: EffectTicket<WorkerAction>,
        _target: ContextHostAddress,
        refusal: MaterializationCaptureRefusal,
    },
    Stage {
        error: ContextHostJobError,
        ticket: EffectTicket<WorkerAction>,
        _target: ContextHostAddress,
        _job: ContextHostJob,
    },
    Completion(EffectCompletionRefusal<WorkerAction>),
}
impl std::fmt::Debug for RootMaterializationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected { error, ticket, .. } => f
                .debug_struct("Rejected")
                .field("error", error)
                .field("input", &ticket.input)
                .finish_non_exhaustive(),
            Self::Capture {
                refusal, ticket, ..
            } => f
                .debug_struct("Capture")
                .field("error", &refusal.error)
                .field("input", &ticket.input)
                .finish_non_exhaustive(),
            Self::Stage { error, ticket, .. } => f
                .debug_struct("Stage")
                .field("error", error)
                .field("input", &ticket.input)
                .finish_non_exhaustive(),
            Self::Completion(refusal) => f.debug_tuple("Completion").field(refusal).finish(),
        }
    }
}
impl RootHostCell {
    pub(crate) fn new(owner: ProcessControlOwner) -> Self {
        let registration = owner.registration().clone();
        Self {
            owner: Mutex::new(owner),
            registration,
        }
    }
    pub(crate) fn lock(&self) -> MutexGuard<'_, ProcessControlOwner> {
        self.owner.lock().expect("original Root host inventory")
    }
    pub(crate) fn try_lock(&self) -> Option<MutexGuard<'_, ProcessControlOwner>> {
        self.owner.try_lock().ok()
    }
    #[cfg(test)]
    pub(crate) fn into_owner(self) -> ProcessControlOwner {
        self.owner
            .into_inner()
            .expect("original Root host inventory")
    }
    /// Move the original committed Materialize into its already-adopted
    /// Context. Root and Context inventory guards never overlap. The factory
    /// is captured only after M/R/H and both callback directions are admitted;
    /// no foreign call or destructor runs in this metadata handoff.
    pub(crate) fn handoff_materialization(
        &self,
        ticket: EffectTicket<WorkerAction>,
        target: ContextHostAddress,
        factory: Arc<dyn QueryContextHostFactory>,
    ) -> Result<(), RootMaterializationRefusal> {
        let (context, admission) = match &ticket.effect {
            WorkerAction::Control {
                effect:
                    ControlEffect::Materialize {
                        context,
                        reservation,
                        ..
                    },
                admitted: Some(original),
            } if original == reservation => (*context, *reservation),
            _ => {
                return Err(RootMaterializationRefusal::Rejected {
                    error: RootMaterializationError::WrongTarget,
                    ticket,
                    _target: target,
                    _factory: factory,
                });
            }
        };
        let preflight = {
            let owner = self.lock();
            let runtime = owner.runtime();
            if ticket.actor != self.registration.actor() || target.context() != context {
                Err(RootMaterializationError::WrongTarget)
            } else {
                runtime
                    .validate_effect_ticket(&ticket)
                    .and_then(|()| runtime.validate_registration(target.registration()))
                    .map(|()| runtime.clone())
                    .map_err(RootMaterializationError::Driver)
            }
        };
        let runtime = match preflight {
            Ok(runtime) => runtime,
            Err(error) => {
                return Err(RootMaterializationRefusal::Rejected {
                    error,
                    ticket,
                    _target: target,
                    _factory: factory,
                });
            }
        };

        // Preparing a target admits only metadata and original empty roles.
        // A partial remains in that Context; no source or factory moved yet.
        let prepared = {
            match target.lock() {
                Some(mut host)
                    if host.admission() == admission
                        && host.registration().same_registration(target.registration()) =>
                {
                    let prepared = if host.host_jobs().is_none() {
                        host.prepare_host_jobs()
                    } else {
                        Ok(())
                    };
                    prepared
                        .map_err(RootMaterializationError::Context)
                        .and_then(|()| {
                            host.host_jobs()
                                .cloned()
                                .ok_or(RootMaterializationError::WrongTarget)
                        })
                }
                _ => Err(RootMaterializationError::WrongTarget),
            }
        };
        let jobs = match prepared {
            Ok(jobs) => jobs,
            Err(error) => {
                return Err(RootMaterializationRefusal::Rejected {
                    error,
                    ticket,
                    _target: target,
                    _factory: factory,
                });
            }
        };
        let source = {
            let mut owner = self.lock();
            owner
                .bind_materialization_return(&ticket, jobs.clone())
                .and_then(|()| owner.take_materialization_source(&ticket))
        };
        let source = match source {
            Ok(source) => source,
            Err(error) => {
                return Err(RootMaterializationRefusal::Rejected {
                    error: RootMaterializationError::Control(error),
                    ticket,
                    _target: target,
                    _factory: factory,
                });
            }
        };

        // Whole source/factory refusal and every opaque Drop escape both
        // owner cells. Only the same already-paid Jobs custody is borrowed.
        let job = match jobs.capture_materialization(source, factory) {
            Ok(job) => job,
            Err(refusal) => {
                return Err(RootMaterializationRefusal::Capture {
                    ticket,
                    _target: target,
                    refusal,
                });
            }
        };
        let staged = match target.lock() {
            Some(mut host) => host.stage_materialization(job),
            None => Err((ContextHostJobError::WrongOwner, job)),
        };
        if let Err((error, job)) = staged {
            return Err(RootMaterializationRefusal::Stage {
                error,
                ticket,
                _target: target,
                _job: job,
            });
        }
        // This completion acknowledges captured ownership, not M's physical
        // execution or the Context Core's eventual establishment decision.
        let input = ticket.input;
        runtime
            .effect_completed_for(&self.registration, ticket)
            .map_err(RootMaterializationRefusal::Completion)?;
        self.lock().note_control_input_progress(input);
        Ok(())
    }
    /// The exclusive Common host turn remains claimed across all phases, but
    /// the Root mutex does not. No foreign getter, callback or Drop runs here.
    pub(crate) fn advance_ready(
        &self,
        turn: &ActorHostTurn<WorkerActor>,
        handles: &EstablishExecutorHandles,
        protocol_events: Option<&crate::TaskExecutionPorts>,
        budget: usize,
    ) -> Result<RootHostProgress, RootRequestAdvanceError> {
        if !turn.matches_registration(&self.registration)
            || !handles.belongs_to(self.lock().request_ready())
        {
            return Err(RootRequestAdvanceError::WrongOwner);
        }
        let mut progress = RootHostProgress::default();
        for _ in 0..budget {
            let original = match self.lock().take_ready_route_binding() {
                Ok(original) => original,
                Err(ControlRequestError::Route(EstablishRouteError::TargetRemoved)) => {
                    // The same actor incarnation cannot recover. Preserve the
                    // original pending R/escrow for its Core deadline/cancel;
                    // do not busy-requeue or turn a late target into fail-stop.
                    progress.stale_targets += 1;
                    continue;
                }
                Err(error) => return Err(RootRequestAdvanceError::Control(error)),
            };
            let Some(original) = original else {
                break;
            };
            // The temporary Root guard has ended before a target lock is taken.
            match original.bind() {
                Ok(()) => progress.routed += 1,
                Err(ControlRequestError::Route(EstablishRouteError::TargetRemoved)) => {
                    progress.stale_targets += 1;
                }
                Err(error) => return Err(RootRequestAdvanceError::Control(error)),
            }
        }
        let mut owner = self.lock();
        progress.requests = owner.advance_ready_requests(handles, protocol_events, budget)?;
        // Each source class has its own finite allowance. The rejected lane
        // visits at most budget retry addresses plus budget original owners.
        progress.rejected_ingress = owner.advance_rejected_ingress(handles, budget)?;
        progress.runnable = !owner.resources.ready_routes.is_empty()
            || owner.request_ready().has_control_ready()
            || progress.rejected_ingress.runnable;
        Ok(progress)
    }
}
