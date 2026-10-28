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

//! The Worker host owner holds the actual Context and Task inventories.
//! Protocol decisions and synchronous admission stay in the registered actors;
//! host executors borrow no mutable Core and never hold this owner while
//! invoking a provider. Transport facades and driver composition use this same
//! inventory. This module does not make bootstrap equivalent to Active or exit.

use super::{
    actors::{WorkerAction, WorkerActor},
    context_executors::{
        ContextExecutorHandles, ContextExecutorStartError, ContextExecutorStartRefusal,
        ContextExecutors,
    },
    context_host::{
        ContextHostBinding, ContextHostError, ContextHostProgress, ContextHostProgressError,
        ContextHostRecord, ContextHostRefusal,
    },
    context_host_jobs::ContextHostJobError,
    context_ready::{ContextReady, ContextReadyError, ContextReadyNotice},
    host_executor::HostExecutorHandle,
    loading::TaskCreatePayload,
    loading_events::{LoadingEventOutcome, LoadingJobReturned},
    loading_host_job::LoadingHostJob,
    process_control::RegisteredControl,
    task::{LoadingSubmission, TaskHostBinding, TaskHostRecord},
};
use novarocks_control_runtime::{
    driver::{ActorRuntime, DriverError, EffectTicket},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::identity::ActorId;
use novarocks_execution_contract::{QueryContextRef, TaskIdentity};
use novarocks_worker_protocol::{context::ContextEffect, control::ControlEffect};
use std::{collections::BTreeMap, num::NonZeroUsize};

#[derive(Debug)]
pub(crate) enum ServiceHostError {
    Driver(DriverError),
    WrongEffect,
    ContextAlreadyOwned,
    ActorAlreadyOwned,
    ContextTableFull,
    UnknownContext,
    TaskUnavailable,
    Context(ContextHostRefusal),
    Task(ContextHostError),
}
#[must_use = "A refused host return retains the exact original author receipt"]
pub(crate) struct ServiceLoadingReturnRefusal {
    pub error: ServiceHostError,
    pub returned: LoadingJobReturned,
}
impl std::fmt::Debug for ServiceLoadingReturnRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceLoadingReturnRefusal")
            .field("error", &self.error)
            .field("returned", &self.returned)
            .finish()
    }
}

/// One process-owned host inventory. Construction receives the same actual
/// runtime and ledger as its admitted Control actor; it allocates no quota,
/// issues no ticket and creates no second protocol decision path.
///
/// Scope/bootstrap completion precedes transport publication. The caller keeps
/// internal actor registration open until every admitted bootstrap and reliable
/// output role has transferred, even after external request intake closes.
/// Physical executor close follows producer drain, rather than replacing it.
pub(crate) struct WorkerService {
    runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    control: ActorId,
    contexts: BTreeMap<QueryContextRef, ContextHostRecord>,
    actors: BTreeMap<ActorId, QueryContextRef>,
    max_contexts: NonZeroUsize,
    request_positions: usize,
    context_ready: ContextReady,
    context_executors_started: bool,
}
impl WorkerService {
    pub(super) fn from_registered_control(registered: RegisteredControl) -> Self {
        let (runtime, ledger, control, max_contexts, requests) = registered.into_parts();
        Self::from_inventory(runtime, ledger, control, max_contexts, requests.get())
    }
    /// Component fixtures attach their separately paid Control inventories.
    /// Production construction requires the original registration evidence.
    #[cfg(test)]
    pub(crate) fn from_admitted_control(
        runtime: ActorRuntime<WorkerActor>,
        ledger: RuntimeLedger,
        control: ActorId,
        max_contexts: NonZeroUsize,
    ) -> Self {
        Self::from_inventory(runtime, ledger, control, max_contexts, 0)
    }
    fn from_inventory(
        runtime: ActorRuntime<WorkerActor>,
        ledger: RuntimeLedger,
        control: ActorId,
        max_contexts: NonZeroUsize,
        request_positions: usize,
    ) -> Self {
        Self {
            context_ready: ContextReady::new(runtime.clone(), max_contexts),
            context_executors_started: false,
            runtime,
            ledger,
            control,
            contexts: BTreeMap::new(),
            actors: BTreeMap::new(),
            max_contexts,
            request_positions,
        }
    }
    pub(super) fn ledger(&self) -> &RuntimeLedger {
        &self.ledger
    }
    pub(crate) fn runtime(&self) -> &ActorRuntime<WorkerActor> {
        &self.runtime
    }
    pub(crate) fn start_context_executors(
        &mut self,
        materialization_workers: NonZeroUsize,
        cleanup_workers: NonZeroUsize,
    ) -> Result<ContextExecutors, ContextExecutorStartRefusal> {
        if std::mem::replace(&mut self.context_executors_started, true) {
            return Err(ContextExecutorStartRefusal {
                error: ContextExecutorStartError::AlreadyStarted,
                owner: None,
            });
        }
        ContextExecutors::start(
            self.runtime.clone(),
            self.control,
            self.max_contexts,
            materialization_workers,
            cleanup_workers,
        )
    }

    /// Take only a real committed Materialize effect's original Context guard.
    /// A refusal after binding returns the same partial resource inventory;
    /// the caller must retain it in its actual failure/cleanup owner. Shared
    /// admission-binding invariants are process failures, not query refusals.
    pub(crate) fn adopt_context(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
        binding: ContextHostBinding,
    ) -> Result<(), ServiceHostError> {
        let context = match &ticket.effect {
            WorkerAction::Control {
                effect: ControlEffect::Materialize { context, .. },
                ..
            } if ticket.actor == self.control => *context,
            _ => return Err(ServiceHostError::WrongEffect),
        };
        if self.contexts.contains_key(&context) {
            return Err(ServiceHostError::ContextAlreadyOwned);
        }
        if self.actors.contains_key(&binding.actor) || binding.actor == self.control {
            return Err(ServiceHostError::ActorAlreadyOwned);
        }
        if self.contexts.len() == self.max_contexts.get() {
            return Err(ServiceHostError::ContextTableFull);
        }
        let mut host = ContextHostRecord::adopt_with_routes(
            &self.runtime,
            &self.ledger,
            ticket,
            binding,
            self.request_positions,
        )
        .map_err(ServiceHostError::Context)?;
        if let Err(error) = host.register_ready(&self.context_ready) {
            let (resources, tasks) = host.into_resources();
            assert!(tasks.is_empty(), "a bootstrap has not captured Task work");
            return Err(ServiceHostError::Context(ContextHostRefusal {
                error: ContextHostError::Ready(error),
                resources: Some(Box::new(resources)),
            }));
        }
        assert_eq!(host.identity(), context);
        assert!(self.actors.insert(host.actor(), context).is_none());
        assert!(self.contexts.insert(context, host).is_none());
        Ok(())
    }

    /// The same original loading/Task guard moves into its owning Context.
    /// This path does not call the host, publish Accepted, or mint a replacement
    /// source role in response to an execution-side notification.
    pub(crate) fn adopt_loading(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
        binding: TaskHostBinding,
    ) -> Result<(), ServiceHostError> {
        if !matches!(
            &ticket.effect,
            WorkerAction::Context(ContextEffect::LoadInput { .. })
        ) {
            return Err(ServiceHostError::WrongEffect);
        }
        let context = *self
            .actors
            .get(&ticket.actor)
            .ok_or(ServiceHostError::UnknownContext)?;
        self.contexts
            .get_mut(&context)
            .expect("actor index addresses its actual owner")
            .adopt_task(&self.runtime, ticket, binding)
            .map_err(ServiceHostError::Task)
    }
    pub(crate) fn context(&mut self, context: QueryContextRef) -> Option<&mut ContextHostRecord> {
        self.contexts.get_mut(&context)
    }
    pub(crate) fn next_host_ready(&self) -> Result<Option<ContextReadyNotice>, ContextReadyError> {
        self.context_ready.next_ready()
    }
    pub(crate) fn ready_context(
        &mut self,
        notice: &ContextReadyNotice,
    ) -> Option<&mut ContextHostRecord> {
        self.contexts
            .get_mut(&notice.context())
            .filter(|host| host.matches_ready(notice))
    }
    /// One already coalesced original address per bounded host turn. The
    /// driver retains this inventory on error; it cannot turn a submission
    /// failure into a parked retry without a corresponding capacity wake.
    pub(crate) fn advance_context_host(
        &mut self,
        notice: &ContextReadyNotice,
        handles: &ContextExecutorHandles,
    ) -> Result<ContextHostProgress, ContextHostProgressError> {
        self.ready_context(notice)
            .ok_or(ContextHostJobError::WrongOwner)?
            .advance_host_io(handles)
    }
    /// The real host job receives its fixed Loaded/LoaderExited authors before
    /// any request getter runs. An initializing/refused owner is retained for
    /// cleanup and cannot publish a port or enqueue a loader through this API.
    pub(crate) fn submit_loading(
        &mut self,
        context: QueryContextRef,
        payload: TaskCreatePayload,
        executor: &HostExecutorHandle<LoadingHostJob>,
    ) -> Result<LoadingSubmission, (ServiceHostError, TaskCreatePayload)> {
        let Some(host) = self.task(context, payload.request.identity()) else {
            return Err((ServiceHostError::TaskUnavailable, payload));
        };
        host.submit_loading(payload, executor)
            .map_err(|(error, payload)| {
                (
                    ServiceHostError::Task(ContextHostError::Task(error)),
                    payload,
                )
            })
    }
    pub(crate) fn retry_loading_submission(
        &mut self,
        context: QueryContextRef,
        task: TaskIdentity,
        executor: &HostExecutorHandle<LoadingHostJob>,
    ) -> Result<LoadingSubmission, ServiceHostError> {
        Ok(self
            .task(context, task)
            .ok_or(ServiceHostError::TaskUnavailable)?
            .retry_loading_submission(executor))
    }
    /// Called only after the physical job's outer send/install/notify call
    /// returned. Observation acknowledges this original author's captures,
    /// independently of Core, effect batches, preparation and Task capacity.
    /// Routing failure returns the same receipt to its live completion owner.
    pub(crate) fn observe_loading_return(
        &mut self,
        context: QueryContextRef,
        task: TaskIdentity,
        returned: LoadingJobReturned,
    ) -> Result<LoadingEventOutcome, ServiceLoadingReturnRefusal> {
        let Some(host) = self.task(context, task) else {
            return Err(ServiceLoadingReturnRefusal {
                error: ServiceHostError::TaskUnavailable,
                returned,
            });
        };
        host.observe_loading_job_return(returned)
            .map_err(|(error, returned)| ServiceLoadingReturnRefusal {
                error: ServiceHostError::Task(ContextHostError::Task(error)),
                returned,
            })
    }
    pub(crate) fn take_loading_return(
        &mut self,
        context: QueryContextRef,
        task: TaskIdentity,
    ) -> Result<Option<LoadingJobReturned>, ServiceHostError> {
        self.task(context, task)
            .ok_or(ServiceHostError::TaskUnavailable)?
            .take_loading_return()
            .map_err(|error| ServiceHostError::Task(ContextHostError::Task(error)))
    }
    pub(crate) fn task(
        &mut self,
        context: QueryContextRef,
        task: TaskIdentity,
    ) -> Option<&mut TaskHostRecord> {
        self.contexts.get_mut(&context)?.task(task)
    }

    /// Move an actual Removing Context to its original cleanup executor.
    /// No resources, Task snapshots or scope records are destroyed here.
    /// The returned owner retains every physical exit and publication duty.
    #[cfg(test)]
    pub(crate) fn take_removing_context(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<Option<ContextHostRecord>, ServiceHostError> {
        self.runtime
            .validate_effect_ticket(ticket)
            .map_err(ServiceHostError::Driver)?;
        let context = match &ticket.effect {
            WorkerAction::Control {
                effect: ControlEffect::RemoveHost { context },
                ..
            } if ticket.actor == self.control => *context,
            _ => return Err(ServiceHostError::WrongEffect),
        };
        let owner = self.contexts.remove(&context);
        if let Some(owner) = &owner {
            assert_eq!(self.actors.remove(&owner.actor()), Some(context));
        }
        Ok(owner)
    }
    /// Move the original owner only after its H/report/child Scope retirement
    /// and HostRemoved handoff. Before this point host completion notices must
    /// still resolve to this same Service record. Afterwards Root's two paid
    /// inputs and the original parent admission remain with the returned owner.
    pub(crate) fn take_physically_removed_context(
        &mut self,
        context: QueryContextRef,
    ) -> Result<Option<ContextHostRecord>, super::context_host_jobs::ContextHostJobError> {
        let Some(host) = self.contexts.get_mut(&context) else {
            return Ok(None);
        };
        if !host.publish_host_removed()? {
            return Ok(None);
        }
        let host = self
            .contexts
            .remove(&context)
            .expect("original physical Context owner");
        assert_eq!(self.actors.remove(&host.actor()), Some(context));
        Ok(Some(host))
    }
}
