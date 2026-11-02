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

mod directory;
pub(crate) use directory::{ContextDirectory, ContextHostAddress};

#[cfg(test)]
use novarocks_control_runtime::driver::DriverError;

use super::{
    actors::{WorkerAction, WorkerActor},
    context_executors::{ContextExecutorStartError, ContextExecutorStartRefusal, ContextExecutors},
    context_host::{ContextHostBinding, ContextHostError, ContextHostRecord, ContextHostRefusal},
    host_executor::HostExecutorLimits,
    process_control::RegisteredControl,
};
use novarocks_control_runtime::{
    driver::{ActorRuntime, EffectTicket},
    lease::WorkChildAuthority,
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::identity::ActorId;
use novarocks_execution_contract::QueryContextRef;
use novarocks_worker_protocol::control::ControlEffect;

#[derive(Debug)]
pub(crate) enum ServiceHostError {
    #[cfg(test)]
    Driver(DriverError),
    WrongEffect,
    ContextAlreadyOwned,
    ActorAlreadyOwned,
    Context(ContextHostRefusal),
}

impl std::fmt::Display for ServiceHostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(test)]
            Self::Driver(error) => write!(f, "Worker service driver failed: {error}"),
            Self::WrongEffect => f.write_str("Worker service received a different effect"),
            Self::ContextAlreadyOwned => f.write_str("Worker service already owns this Context"),
            Self::ActorAlreadyOwned => f.write_str("Worker service already owns this actor"),
            Self::Context(original) => write!(
                f,
                "Worker service Context admission failed: {}",
                original.error
            ),
        }
    }
}
impl std::error::Error for ServiceHostError {}

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
    process_source_parent: WorkChildAuthority,
    directory: ContextDirectory,
    request_positions: usize,
    context_executors_started: bool,
}
impl WorkerService {
    pub(super) fn from_registered_control(registered: RegisteredControl) -> Self {
        let (runtime, ledger, control, requests, process_source_parent) = registered.into_parts();
        Self::from_inventory(
            runtime,
            ledger,
            control,
            requests.get(),
            process_source_parent,
        )
    }
    /// Component fixtures attach their separately paid Control inventories.
    /// Production construction requires the original registration evidence.
    #[cfg(test)]
    pub(crate) fn from_admitted_control(
        runtime: ActorRuntime<WorkerActor>,
        ledger: RuntimeLedger,
        control: ActorId,
        actual_root_parent: &novarocks_control_runtime::lease::WorkLease,
    ) -> Self {
        let process_source_parent = actual_root_parent
            .child_authority()
            .expect("the fixture holds the original Root baseline Work");
        Self::from_inventory(runtime, ledger, control, 0, process_source_parent)
    }
    fn from_inventory(
        runtime: ActorRuntime<WorkerActor>,
        ledger: RuntimeLedger,
        control: ActorId,
        request_positions: usize,
        process_source_parent: WorkChildAuthority,
    ) -> Self {
        Self {
            context_executors_started: false,
            runtime,
            ledger,
            control,
            process_source_parent,
            directory: ContextDirectory::default(),
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
        materialization: HostExecutorLimits,
        cleanup: HostExecutorLimits,
        capacity: super::task_capacity::TaskCapacitySignal,
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
            materialization,
            cleanup,
            capacity,
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
        // Registration and publication are one directory transaction. A host
        // poller cannot resolve the newly registered actor before its original
        // Context cell has been published here.
        let mut directory = self.directory.state();
        if directory.contexts.contains_key(&context)
            || directory
                .executions
                .contains_key(&context.query_execution_id())
        {
            return Err(ServiceHostError::ContextAlreadyOwned);
        }
        if directory.actors.contains_key(&binding.actor) || binding.actor == self.control {
            return Err(ServiceHostError::ActorAlreadyOwned);
        }
        let mut host = ContextHostRecord::adopt_with_routes(
            &self.runtime,
            &self.ledger,
            ticket,
            binding,
            self.request_positions,
            self.process_source_parent.clone(),
        )
        .map_err(ServiceHostError::Context)?;
        if let Err(error) = host.register_ready() {
            let (resources, tasks) = host.into_resources();
            assert!(tasks.is_empty(), "a bootstrap has not captured Task work");
            return Err(ServiceHostError::Context(ContextHostRefusal {
                error: ContextHostError::Ready(error),
                resources: Some(Box::new(resources)),
            }));
        }
        assert_eq!(host.identity(), context);
        assert!(
            directory
                .executions
                .insert(context.query_execution_id(), context)
                .is_none()
        );
        assert!(directory.actors.insert(host.actor(), context).is_none());
        assert!(
            directory
                .contexts
                .insert(context, ContextHostAddress::new(host))
                .is_none()
        );
        Ok(())
    }

    pub(crate) fn directory(&self) -> ContextDirectory {
        self.directory.clone()
    }
    pub(crate) fn context(&self, context: QueryContextRef) -> Option<ContextHostAddress> {
        self.directory.context(context)
    }
    /// Move the original owner for component tests which directly hold its
    /// removal effect. The returned owner retains all physical obligations.
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
        let Some(address) = self.context(context) else {
            return Ok(None);
        };
        let owner = address.take();
        if owner.is_some() {
            self.directory.remove_original(&address);
        }
        Ok(owner)
    }
}
