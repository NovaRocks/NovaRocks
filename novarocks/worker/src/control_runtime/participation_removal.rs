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

//! Expiry creates one short Root author and its actual forget child. The
//! original HostRemoved receipt outlives that child and the directory removal;
//! no Work or callable is prepaid throughout the retained Participation horizon.

use super::{
    actors::{WorkerAction, WorkerActor},
    control_internal::{ControlInternalError, ControlLifecycleReceipt},
    participation_forget::{ParticipationForgetError, ParticipationForgetOwner},
    participation_host::task_io::TaskIoHostJob,
    participation_ready::ParticipationReadyNotice,
    process_control::ProcessControlOwner,
};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket},
    obligation::{ReservationObligation, WorkObligation, WorkRetirementObserver},
    reported_work::ReportedWork,
    transaction::{DeclaredInput, RuntimeError},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::ConsumerId,
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::ParticipationRef;
use novarocks_worker_protocol::control::ControlEffect;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParticipationRemovalError {
    WrongOwner,
    ActorTurn(ActorTurnError),
    Runtime(RuntimeError),
    Forget(ParticipationForgetError),
    Source(ControlInternalError),
}
impl std::fmt::Display for ParticipationRemovalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner => {
                f.write_str("Participation removal does not belong to the original Root owner")
            }
            Self::ActorTurn(error) => {
                write!(f, "Participation removal actor operation failed: {error}")
            }
            Self::Runtime(error) => {
                write!(f, "Participation removal runtime operation failed: {error}")
            }
            Self::Forget(error) => write!(f, "Participation removal host forget failed: {error}"),
            Self::Source(error) => {
                write!(f, "Participation removal lifecycle source failed: {error}")
            }
        }
    }
}
impl std::error::Error for ParticipationRemovalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ActorTurn(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Forget(error) => Some(error),
            Self::Source(error) => Some(error),
            Self::WrongOwner => None,
        }
    }
}

impl From<ActorTurnError> for ParticipationRemovalError {
    fn from(error: ActorTurnError) -> Self {
        Self::ActorTurn(error)
    }
}
impl From<RuntimeError> for ParticipationRemovalError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl From<ParticipationForgetError> for ParticipationRemovalError {
    fn from(error: ParticipationForgetError) -> Self {
        Self::Forget(error)
    }
}

/// Never-filled partial originals have not invoked or captured the host. The
/// caller retains them through actual unused teardown or process failure.
#[must_use = "Retain every original partially admitted removal position"]
pub(crate) struct ParticipationRemovalPartial {
    pub work: WorkObligation,
    pub capacity: ReservationObligation,
    pub cleanup: Option<ReportedWork>,
}
#[must_use = "A refusal retains the host and all actual partially admitted owners"]
pub(crate) struct ParticipationRemovalRefusal {
    pub error: ParticipationRemovalError,
    pub participation: ParticipationRef,
    pub _notice: ParticipationReadyNotice,
    pub _host: Arc<dyn crate::TaskExecutionHost>,
    pub partial: Option<ParticipationRemovalPartial>,
}
impl std::fmt::Debug for ParticipationRemovalRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParticipationRemovalRefusal")
            .field("error", &self.error)
            .field("participation", &self.participation)
            .field("retains_partial", &self.partial.is_some())
            .finish_non_exhaustive()
    }
}

/// This unique owner remains at the Node until the original Root return is
/// consumed and the short Step header actually retires. Its host capture is
/// held only by the same forget child, whose foreign Drop runs on a host worker.
#[must_use = "Keep removal custody through the original HostRemoved input and Step exit"]
pub(crate) struct ParticipationRemovalOwner {
    runtime: ActorRuntime<WorkerActor>,
    root: ActorRegistration,
    participation: ParticipationRef,
    work: WorkObligation,
    capacity: ReservationObligation,
    retirement: WorkRetirementObserver,
    receipt: Option<ControlLifecycleReceipt>,
    forget: ParticipationForgetOwner,
    body_exited: bool,
    captures_exited: bool,
    retired: bool,
}
impl ParticipationRemovalOwner {
    /// Only a currently valid original Root RemoveHost effect can create this
    /// finite responsibility. The ticket stays with its caller until the whole
    /// owner and same job have transferred; adoption is not physical completion.
    pub(crate) fn from_actual_effect(
        runtime: ActorRuntime<WorkerActor>,
        root: ActorRegistration,
        actual_parent: &WorkObligation,
        ticket: &EffectTicket<WorkerAction>,
        participation: ParticipationRef,
        notice: ParticipationReadyNotice,
        host: Arc<dyn crate::TaskExecutionHost>,
    ) -> Result<(Self, TaskIoHostJob), ParticipationRemovalRefusal> {
        let checked = (|| -> Result<_, ParticipationRemovalError> {
            let source = runtime.registration_from_effect(ticket)?;
            if !source.same_registration(&root)
                || !root.matches_scope(actual_parent.scope())
                || notice.participation() != participation
                || !matches!(
                    &ticket.effect,
                    WorkerAction::Control {
                        effect: ControlEffect::RemoveHost { participation: target },
                        admitted: None,
                    } if *target == participation
                )
            {
                return Err(ParticipationRemovalError::WrongOwner);
            }
            runtime.validate_work_author(&root, actual_parent)?;
            Ok(actual_parent.scope().gates()?.generation)
        })();
        let generation = match checked {
            Ok(generation) => generation,
            Err(error) => {
                return Err(ParticipationRemovalRefusal {
                    error,
                    participation,
                    _notice: notice,
                    _host: host,
                    partial: None,
                });
            }
        };
        let scope = actual_parent.scope();
        // This original Step owns its receipt/capture and the actual forget
        // child. Root's existing internal Source pays for its own one input,
        // batch and effects; no Participation occupies a private idle Root input.
        let resources = ReservationResources {
            keyed: None,
            quota: ResourceVector::default(),
            owned_slots: OwnedSlotNeed {
                work_headers: 2,
                completions: 1,
                captures: 2,
                ..OwnedSlotNeed::default()
            },
        };
        let guard = match scope.reserve_next(
            generation,
            resources,
            WorkKind::Step,
            WorkAdmission::Derived(actual_parent.id()),
        ) {
            Ok(guard) => guard,
            Err(error) => {
                return Err(ParticipationRemovalRefusal {
                    error: error.into(),
                    participation,
                    _notice: notice,
                    _host: host,
                    partial: None,
                });
            }
        };
        let (work, capacity) = match guard.bind(WorkKind::Step) {
            Ok(original) => original,
            Err(error) => {
                return Err(ParticipationRemovalRefusal {
                    error: error.into(),
                    participation,
                    _notice: notice,
                    _host: host,
                    partial: None,
                });
            }
        };
        let mut partial = ParticipationRemovalPartial {
            work,
            capacity,
            cleanup: None,
        };
        macro_rules! refuse {
            ($error:expr) => {
                return Err(ParticipationRemovalRefusal {
                    error: $error.into(),
                    participation,
                    _notice: notice,
                    _host: host,
                    partial: Some(partial),
                })
            };
        }
        if let Err(error) = partial.work.start() {
            refuse!(error);
        }
        let observer = ConsumerId::new(1).expect("one original forget observer");
        partial.cleanup = match scope.reserve_child_from(
            &mut partial.capacity,
            &partial.work,
            DeclaredInput {
                generation,
                kind: WorkKind::Cleanup,
                resources: ReservationResources {
                    keyed: None,
                    quota: ResourceVector::default(),
                    owned_slots: OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..OwnedSlotNeed::default()
                    },
                },
                consumers: vec![(observer, ConsumerKind::Observer)],
            },
        ) {
            Ok(original) => Some(original),
            Err(error) => refuse!(error),
        };
        // reserve_child_from already moved and bound only the child's actual
        // 1/1/1. The remaining capture belongs solely to this Step's input owner.
        if let Err(error) = partial.work.bind_owned_captures(&mut partial.capacity, 1) {
            refuse!(error);
        }
        let (forget, job) = match ParticipationForgetOwner::from_admitted(
            scope,
            participation,
            notice,
            host,
            observer,
            partial.cleanup.take().expect("same original forget child"),
        ) {
            Ok(original) => original,
            Err(refusal) => {
                partial.cleanup = Some(refusal.original);
                return Err(ParticipationRemovalRefusal {
                    error: refusal.error.into(),
                    participation: refusal.participation,
                    _notice: refusal.notice,
                    _host: refusal.host,
                    partial: Some(partial),
                });
            }
        };
        let retirement = partial.work.retirement_observer();
        Ok((
            Self {
                runtime,
                root,
                participation,
                work: partial.work,
                capacity: partial.capacity,
                retirement,
                receipt: None,
                forget,
                body_exited: false,
                captures_exited: false,
                retired: false,
            },
            TaskIoHostJob::ParticipationForget(job),
        ))
    }
    pub(crate) fn participation(&self) -> ParticipationRef {
        self.participation
    }
    /// The original return, report consumer, body, captures and actual Cleanup
    /// header all must retire. Root's existing bounded reaper owns header work.
    pub(crate) fn forget_retired(&self) -> Result<bool, ParticipationRemovalError> {
        Ok(self.forget.try_retire()?)
    }
    /// Node calls this only after forget_retired and actual removal of this
    /// same Participation actor and directory cell. This owner cannot manufacture
    /// that external directory-removal proof from a missing Scope or phase.
    pub(crate) fn publish_host_removed(
        &mut self,
        root: &mut ProcessControlOwner,
    ) -> Result<bool, ParticipationRemovalError> {
        if self.receipt.is_some() {
            return Ok(true);
        }
        if !self.forget_retired()? {
            return Ok(false);
        }
        self.runtime.validate_registration(&self.root)?;
        let receipt = root
            .enqueue_host_removed(&self.work, self.participation)
            .map_err(ParticipationRemovalError::Source)?;
        if !receipt.matches_host_removed(&self.root, self.work.reference(), self.participation) {
            return Err(ParticipationRemovalError::WrongOwner);
        }
        self.receipt = Some(receipt);
        Ok(true)
    }
    /// Only the actual original Root batch can release this position. Closing
    /// publication and exiting the Step remain separate from the forget child
    /// and the directory. Callers keep driving Root's ordinary bounded reaper.
    pub(crate) fn retire_after_host_removed(&mut self) -> Result<bool, ParticipationRemovalError> {
        if self.retired {
            return Ok(true);
        }
        let Some(receipt) = &self.receipt else {
            return Ok(false);
        };
        if !receipt.matches_host_removed(&self.root, self.work.reference(), self.participation) {
            return Err(ParticipationRemovalError::WrongOwner);
        }
        if !receipt.is_retired() {
            return Ok(false);
        }
        self.runtime.validate_registration(&self.root)?;
        if !self.body_exited {
            self.work.body_exited()?;
            self.body_exited = true;
        }
        if !self.captures_exited {
            self.work.captures_exited()?;
            self.captures_exited = true;
        }
        if !self.retirement.is_retired() {
            return Ok(false);
        }
        self.capacity.finish()?;
        self.retired = true;
        Ok(true)
    }
}
