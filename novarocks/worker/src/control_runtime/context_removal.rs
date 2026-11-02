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
//! no Work or callable is prepaid throughout the retained Context horizon.

use super::{
    actors::{WorkerAction, WorkerActor},
    context_forget::{ContextForgetError, ContextForgetOwner},
    context_host::task_driver::TaskDriverHostJob,
    context_ready::ContextReadyNotice,
    control_internal::{ControlInternalError, RootLifecycleReceipt},
    process_control::ProcessControlOwner,
};
use novarocks_control_runtime::{
    driver::{ActorRegistration, ActorRuntime, DriverError, EffectTicket},
    lease::{ReservationBundle, WorkLease, WorkRetirementObserver},
    reported_work::ReportedWork,
    transaction::{RoleSpec, RuntimeError},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::ConsumerId,
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::QueryContextRef;
use novarocks_worker_protocol::control::ControlEffect;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContextRemovalError {
    WrongOwner,
    Driver(DriverError),
    Runtime(RuntimeError),
    Forget(ContextForgetError),
    Source(ControlInternalError),
}
impl std::fmt::Display for ContextRemovalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner => {
                f.write_str("Context removal does not belong to the original Root owner")
            }
            Self::Driver(error) => write!(f, "Context removal actor operation failed: {error}"),
            Self::Runtime(error) => write!(f, "Context removal runtime operation failed: {error}"),
            Self::Forget(error) => write!(f, "Context removal host forget failed: {error}"),
            Self::Source(error) => write!(f, "Context removal lifecycle source failed: {error}"),
        }
    }
}
impl std::error::Error for ContextRemovalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Driver(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Forget(error) => Some(error),
            Self::Source(error) => Some(error),
            Self::WrongOwner => None,
        }
    }
}

impl From<DriverError> for ContextRemovalError {
    fn from(error: DriverError) -> Self {
        Self::Driver(error)
    }
}
impl From<RuntimeError> for ContextRemovalError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl From<ContextForgetError> for ContextRemovalError {
    fn from(error: ContextForgetError) -> Self {
        Self::Forget(error)
    }
}

/// Never-filled partial originals have not invoked or captured the host. The
/// caller retains them through actual unused teardown or process failure.
#[must_use = "Retain every original partially admitted removal position"]
pub(crate) struct ContextRemovalPartial {
    pub work: WorkLease,
    pub capacity: ReservationBundle,
    pub cleanup: Option<ReportedWork>,
}
#[must_use = "A refusal retains the host and all actual partially admitted owners"]
pub(crate) struct ContextRemovalRefusal {
    pub error: ContextRemovalError,
    pub context: QueryContextRef,
    pub _notice: ContextReadyNotice,
    pub _host: Arc<dyn crate::TaskExecutionHost>,
    pub partial: Option<ContextRemovalPartial>,
}
impl std::fmt::Debug for ContextRemovalRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextRemovalRefusal")
            .field("error", &self.error)
            .field("context", &self.context)
            .field("retains_partial", &self.partial.is_some())
            .finish_non_exhaustive()
    }
}

/// This unique owner remains at the Node until the original Root return is
/// consumed and the short Step header actually retires. Its host capture is
/// held only by the same forget child, whose foreign Drop runs on a host worker.
#[must_use = "Keep removal custody through the original HostRemoved input and Step exit"]
pub(crate) struct ContextRemovalOwner {
    runtime: ActorRuntime<WorkerActor>,
    root: ActorRegistration,
    context: QueryContextRef,
    work: WorkLease,
    capacity: ReservationBundle,
    retirement: WorkRetirementObserver,
    receipt: Option<RootLifecycleReceipt>,
    forget: ContextForgetOwner,
    body_exited: bool,
    captures_exited: bool,
    retired: bool,
}
impl ContextRemovalOwner {
    /// Only a currently valid original Root RemoveHost effect can create this
    /// finite responsibility. The ticket stays with its caller until the whole
    /// owner and same job have transferred; adoption is not physical completion.
    pub(crate) fn from_actual_effect(
        runtime: ActorRuntime<WorkerActor>,
        root: ActorRegistration,
        actual_parent: &WorkLease,
        ticket: &EffectTicket<WorkerAction>,
        context: QueryContextRef,
        notice: ContextReadyNotice,
        host: Arc<dyn crate::TaskExecutionHost>,
    ) -> Result<(Self, TaskDriverHostJob), ContextRemovalRefusal> {
        let checked = (|| -> Result<_, ContextRemovalError> {
            let source = runtime.registration_from_effect(ticket)?;
            if !source.same_registration(&root)
                || !root.matches_scope(actual_parent.scope())
                || notice.context() != context
                || !matches!(
                    &ticket.effect,
                    WorkerAction::Control {
                        effect: ControlEffect::RemoveHost { context: target },
                        admitted: None,
                    } if *target == context
                )
            {
                return Err(ContextRemovalError::WrongOwner);
            }
            runtime.validate_work_author(&root, actual_parent)?;
            Ok(actual_parent.scope().gates()?.generation)
        })();
        let generation = match checked {
            Ok(generation) => generation,
            Err(error) => {
                return Err(ContextRemovalRefusal {
                    error,
                    context,
                    _notice: notice,
                    _host: host,
                    partial: None,
                });
            }
        };
        let scope = actual_parent.scope();
        // This original Step owns its receipt/capture and the actual forget
        // child. Root's existing internal Source pays for its own one input,
        // batch and effects; no Context occupies a private idle Root input.
        let resources = ReservationResources {
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
                return Err(ContextRemovalRefusal {
                    error: error.into(),
                    context,
                    _notice: notice,
                    _host: host,
                    partial: None,
                });
            }
        };
        let (work, capacity) = match guard.bind(WorkKind::Step) {
            Ok(original) => original,
            Err(error) => {
                return Err(ContextRemovalRefusal {
                    error: error.into(),
                    context,
                    _notice: notice,
                    _host: host,
                    partial: None,
                });
            }
        };
        let mut partial = ContextRemovalPartial {
            work,
            capacity,
            cleanup: None,
        };
        macro_rules! refuse {
            ($error:expr) => {
                return Err(ContextRemovalRefusal {
                    error: $error.into(),
                    context,
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
            RoleSpec {
                generation,
                kind: WorkKind::Cleanup,
                resources: ReservationResources {
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
        let (forget, job) = match ContextForgetOwner::from_admitted(
            scope,
            context,
            notice,
            host,
            observer,
            partial.cleanup.take().expect("same original forget child"),
        ) {
            Ok(original) => original,
            Err(refusal) => {
                partial.cleanup = Some(refusal.original);
                return Err(ContextRemovalRefusal {
                    error: refusal.error.into(),
                    context: refusal.context,
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
                context,
                work: partial.work,
                capacity: partial.capacity,
                retirement,
                receipt: None,
                forget,
                body_exited: false,
                captures_exited: false,
                retired: false,
            },
            TaskDriverHostJob::ContextForget(job),
        ))
    }
    pub(crate) fn context(&self) -> QueryContextRef {
        self.context
    }
    /// The original return, report consumer, body, captures and actual Cleanup
    /// header all must retire. Root's existing bounded reaper owns header work.
    pub(crate) fn forget_retired(&self) -> Result<bool, ContextRemovalError> {
        Ok(self.forget.try_retire()?)
    }
    /// Node calls this only after forget_retired and actual removal of this
    /// same Context actor and directory cell. This owner cannot manufacture
    /// that external directory-removal proof from a missing Scope or phase.
    pub(crate) fn publish_host_removed(
        &mut self,
        root: &mut ProcessControlOwner,
    ) -> Result<bool, ContextRemovalError> {
        if self.receipt.is_some() {
            return Ok(true);
        }
        if !self.forget_retired()? {
            return Ok(false);
        }
        self.runtime.validate_registration(&self.root)?;
        let receipt = root
            .enqueue_host_removed(&self.work, self.context)
            .map_err(ContextRemovalError::Source)?;
        if !receipt.matches_host_removed(&self.root, self.work.reference(), self.context) {
            return Err(ContextRemovalError::WrongOwner);
        }
        self.receipt = Some(receipt);
        Ok(true)
    }
    /// Only the actual original Root batch can release this position. Closing
    /// publication and exiting the Step remain separate from the forget child
    /// and the directory. Callers keep driving Root's ordinary bounded reaper.
    pub(crate) fn retire_after_host_removed(&mut self) -> Result<bool, ContextRemovalError> {
        if self.retired {
            return Ok(true);
        }
        let Some(receipt) = &self.receipt else {
            return Ok(false);
        };
        if !receipt.matches_host_removed(&self.root, self.work.reference(), self.context) {
            return Err(ContextRemovalError::WrongOwner);
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
