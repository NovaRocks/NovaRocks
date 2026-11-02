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

//! The original process-funded execution-retirement callback and workless
//! lifecycle facts. Root input capacity is borrowed only when a fact is sent.

mod failed;

use super::{
    actors::{
        ContextActor, ContextMessage, ControlMessage, WorkerAction, WorkerActor, WorkerMessage,
    },
    context_host_jobs::ContextHostJobError,
    control_internal::{RootLifecycleFact, RootLifecycleReceipt},
};
use crate::control_runtime_policy::StorageNeed;
use novarocks_control_runtime::{
    driver::{
        ActorHostTurn, ActorRegistration, ActorRuntime, DriverError, InputPermit, InputRole,
        MetadataConsumption, MetadataTicket,
    },
    lease::WorkLease,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    identity::{BatchId, Generation, InputId, ScopedWorkRef},
    mailbox::InputClass,
};
use novarocks_execution_contract::QueryContextRef;
use novarocks_worker_protocol::{
    context::{ContextEffect, ContextInput},
    control::ControlInput,
    lease::{MonotonicInstant, RequestHorizon},
    retention::{ContextRetirementIdentity, RetentionOrigin},
};

pub(crate) const CONTEXT_LIFECYCLE_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 1,
    batches: 1,
    effects: ContextActor::MAX_STEP_ACTIONS,
    work: 0,
    completions: 0,
    captures: 0,
    cuts: 0,
    replies: 0,
};

/// This callback exists before any host capture. Its actual payer belongs to
/// the process, so delivering it never requires the removed execution Scope.
pub(crate) struct ContextLifecycle {
    runtime: ActorRuntime<WorkerActor>,
    root: ActorRegistration,
    actor: ActorRegistration,
    context: QueryContextRef,
    child: ScopeHandle,
    work: ScopedWorkRef,
    permit: Option<InputPermit<WorkerActor>>,
    callback: Option<InputRole<WorkerActor>>,
    horizon: RequestHorizon,
    retirement: Option<(ContextRetirementIdentity, RetentionOrigin)>,
    callback_published: bool,
    callback_closed: bool,
    outgoing: Option<MetadataTicket<WorkerAction>>,
    scan_after: Option<BatchId>,
    scan_input: Option<InputId>,
    retained: bool,
    // Frozen observation from the original committed compaction fact, never
    // reconstructed from a directory address or a failed Core.
    compact_inventory: Option<novarocks_worker_protocol::retention::RetainedContextInventory>,
    failed_custodian: bool,
    expired: bool,
    failed_intent: Option<RootLifecycleFact>,
    failed_receipt: Option<(RootLifecycleFact, RootLifecycleReceipt)>,
    failed_deadline_due: bool,
    failed_metadata_drained: bool,
}
impl ContextLifecycle {
    /// Identity of the original execution Scope, even after its removal.
    /// Retained request funding never derives Work from this handle.
    pub(in crate::control_runtime) fn original_execution_scope(&self) -> &ScopeHandle {
        &self.child
    }
    pub(in crate::control_runtime) fn retention_snapshot(
        &self,
    ) -> super::node_metrics::WorkerContextRetentionSnapshot {
        use super::node_metrics::WorkerContextRetentionSnapshot;
        let failed = self.failed_custodian && self.retirement.is_some();
        let plain_origin_bytes = if failed {
            std::mem::size_of::<(ContextRetirementIdentity, RetentionOrigin)>()
        } else {
            0
        };
        WorkerContextRetentionSnapshot {
            compact_contexts: usize::from(self.compact_inventory.is_some()),
            failed_custodians: usize::from(failed),
            decided_task_records: self
                .compact_inventory
                .map(|inventory| inventory.decided_task_records)
                .or_else(|| (!failed).then_some(0)),
            metadata_bytes_estimate: match self.compact_inventory {
                Some(inventory) => inventory
                    .metadata_bytes_estimate
                    .and_then(|n| n.checked_add(plain_origin_bytes)),
                None => Some(plain_origin_bytes),
            },
        }
    }
    pub(super) fn new(
        runtime: ActorRuntime<WorkerActor>,
        root: ActorRegistration,
        actor: ActorRegistration,
        context: QueryContextRef,
        child: ScopeHandle,
        author: &WorkLease,
        horizon: RequestHorizon,
    ) -> Self {
        Self {
            runtime,
            root,
            actor,
            context,
            child,
            work: author.reference(),
            permit: None,
            callback: None,
            horizon,
            retirement: None,
            callback_published: false,
            callback_closed: false,
            outgoing: None,
            scan_after: None,
            scan_input: None,
            retained: false,
            compact_inventory: None,
            failed_custodian: false,
            expired: false,
            failed_intent: None,
            failed_receipt: None,
            failed_deadline_due: false,
            failed_metadata_drained: false,
        }
    }
    pub(super) fn initialize(&mut self, author: &WorkLease) -> Result<(), ContextHostJobError> {
        if author.reference() != self.work || self.callback.is_some() || self.permit.is_some() {
            return Err(ContextHostJobError::WrongPhase);
        }
        self.runtime.validate_registration(&self.root)?;
        self.permit = Some(self.runtime.reserve_admitted_input_for(
            &self.actor,
            self.root.actor(),
            InputClass::Lifecycle,
            author,
        )?);
        match self
            .permit
            .take()
            .expect("original execution callback")
            .into_role()
        {
            Ok(role) => {
                self.callback = Some(role);
                Ok(())
            }
            Err((error, original)) => {
                self.permit = Some(original);
                Err(error.into())
            }
        }
    }
    /// Only actual ledger removal supplies this fact. The injected time is
    /// sampled by the Node after removal and is immutable on a repeated turn.
    pub(super) fn publish_execution_retired(
        &mut self,
        close_epoch: Generation,
        retired_at: MonotonicInstant,
    ) -> Result<(), ContextHostJobError> {
        let origin = RetentionOrigin::new(retired_at, self.horizon);
        if self.retirement.is_some_and(|original| {
            original != (ContextRetirementIdentity::Protocol { close_epoch }, origin)
        }) {
            return Err(ContextHostJobError::WrongPhase);
        }
        match self.child.gates() {
            Err(RuntimeError::UnknownScope) => {}
            Ok(_) => return Err(ContextHostJobError::WrongPhase),
            Err(error) => return Err(error.into()),
        }
        // Physical removal cannot be undone if the callback races failure.
        // Keep the one original instant before touching the failed-Core gate.
        self.retirement = Some((ContextRetirementIdentity::Protocol { close_epoch }, origin));
        if self.callback_published {
            return Ok(());
        }
        let result = self
            .callback
            .as_ref()
            .ok_or(ContextHostJobError::WrongPhase)?
            .fill(WorkerMessage::Context(ContextMessage::Command {
                work: self.work,
                request: None,
                command: ContextInput::ExecutionCustodyRetired {
                    context: self.context,
                    close_epoch,
                    origin,
                },
            }))
            .map_err(|(error, _original)| ContextHostJobError::Driver(error));
        if let Err(error) = result {
            if matches!(
                &error,
                ContextHostJobError::Driver(
                    DriverError::ActorFailed
                        | DriverError::FailurePublicationRequired
                        | DriverError::Closed
                )
            ) && self.runtime.snapshot(self.actor.actor())?.failed
            {
                // The trusted failure custodian retains this same fact. It
                // must never sample another retirement time or resume Core.
                return Ok(());
            }
            return Err(error);
        }
        self.callback_published = true;
        Ok(())
    }
    /// At most one original fact is in transit. A Full destination retains
    /// the same ticket and the runtime's real credit waiter, without spinning.
    pub(super) fn advance_metadata(
        &mut self,
        turn: &mut ActorHostTurn<WorkerActor>,
        budget: usize,
    ) -> Result<bool, ContextHostJobError> {
        self.failed_metadata_drained = false;
        if !turn.matches_registration(&self.actor) {
            return Err(ContextHostJobError::WrongOwner);
        }
        for _ in 0..budget {
            if self.outgoing.is_none() {
                if self.scan_input.is_none() {
                    let Some((ordinal, input)) = self
                        .runtime
                        .committed_batches(self.actor.actor(), self.scan_after, 1)?
                        .first()
                        .copied()
                    else {
                        break;
                    };
                    self.scan_after = Some(ordinal);
                    self.scan_input = Some(input);
                }
                let input = self.scan_input.expect("original scanned batch");
                self.outgoing = turn.take_metadata(input)?;
                if self.outgoing.is_none() {
                    self.scan_input = None;
                    continue;
                }
            }
            let ticket = self.outgoing.as_ref().expect("original lifecycle fact");
            let command = match ticket.value() {
                WorkerAction::Context(ContextEffect::RetainContext {
                    context,
                    close_epoch,
                    inventory,
                }) if *context == self.context => {
                    // Read only after the original source batch committed. The
                    // same ticket remains owned until Root consumes the fact.
                    self.compact_inventory.get_or_insert(*inventory);
                    ControlInput::ExecutionRetired {
                        context: *context,
                        retirement: ContextRetirementIdentity::Protocol {
                            close_epoch: *close_epoch,
                        },
                    }
                }
                WorkerAction::Context(ContextEffect::Expired {
                    context,
                    close_epoch,
                }) if *context == self.context => ControlInput::ContextExpired {
                    context: *context,
                    retirement: ContextRetirementIdentity::Protocol {
                        close_epoch: *close_epoch,
                    },
                },
                _ => return Err(ContextHostJobError::WrongOwner),
            };
            let delivery = match self.runtime.metadata_delivery_for(&self.actor, ticket)? {
                Some(original) => original,
                None => match self.runtime.forward_metadata_for(
                    &self.actor,
                    &self.root,
                    ticket,
                    InputClass::Lifecycle,
                    WorkerMessage::Control(ControlMessage::Metadata(command)),
                ) {
                    Ok(delivery) => delivery,
                    Err((DriverError::InputCapacity, _original)) => return Ok(false),
                    Err((error, _original)) => return Err(error.into()),
                },
            };
            match delivery.consumption() {
                MetadataConsumption::Pending => return Ok(false),
                MetadataConsumption::AbandonedByOwnerFailure => {
                    return Err(DriverError::ActorFailed.into());
                }
                MetadataConsumption::Consumed => {}
            }
            let original = self.outgoing.take().expect("same consumed fact");
            let retained = matches!(
                original.value(),
                WorkerAction::Context(ContextEffect::RetainContext { .. })
            );
            let expired = matches!(
                original.value(),
                WorkerAction::Context(ContextEffect::Expired { .. })
            );
            match self.runtime.metadata_completed_for(&self.actor, original) {
                Ok(()) => {}
                Err((error, original)) => {
                    self.outgoing = Some(original);
                    return Err(error.into());
                }
            }
            if retained {
                self.retained = true;
            }
            if expired {
                self.expired = true;
            }
        }
        // More bounded original batch positions may remain. Waits returned
        // above rely exclusively on an actual receipt or credit wake.
        Ok(self.scan_input.is_some())
    }
    pub(super) fn retire_callback(&mut self) -> Result<bool, ContextHostJobError> {
        if self.callback_closed {
            return Ok(true);
        }
        if self.retirement.is_none() || !self.retained {
            return Ok(false);
        }
        let callback = self
            .callback
            .as_mut()
            .ok_or(ContextHostJobError::WrongPhase)?;
        if let Some(input) = callback.in_flight()? {
            if self.scan_input == Some(input) {
                return Ok(false);
            }
            match self.runtime.retire_input(self.actor.actor(), input) {
                Ok(()) => {}
                Err(
                    DriverError::InputStillQueued
                    | DriverError::EffectsRemain
                    | DriverError::ReservationsRemain
                    | DriverError::MetadataStillPending,
                ) => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        }
        callback.close_after_exit()?;
        self.callback = None;
        self.callback_closed = true;
        Ok(true)
    }
    /// The serial cursor still borrows this exact source batch. Its original
    /// Timer input must stay resident until the next bounded scan observes
    /// that the batch contains no further metadata, including after an ACK.
    pub(super) fn scans_input(&self, input: InputId) -> bool {
        self.scan_input == Some(input)
    }
    #[cfg(test)]
    pub(crate) fn withdraw_unused(&mut self) -> Result<(), ContextHostJobError> {
        if self.retirement.is_some() || self.outgoing.is_some() {
            return Err(ContextHostJobError::WrongPhase);
        }
        if let Some(permit) = self.permit.take() {
            permit.revoke_unused()?;
        }
        if let Some(callback) = self.callback.as_mut() {
            callback.close_after_exit()?;
        }
        self.callback = None;
        self.callback_closed = true;
        Ok(())
    }
}
