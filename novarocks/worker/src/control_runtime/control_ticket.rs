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

//! Custody of the actual ticket metadata holders elected by the Control core.
//! The pure admission core remains the decision owner; this inventory only
//! moves its original guard and retires that holder on its original effect.

use super::actors::{WorkerAction, WorkerActor};
use novarocks_control_runtime::{
    driver::{ActorRuntime, DriverError, EffectTicket},
    lease::{CapacityBundle, WorkLease},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    identity::{ActorId, ReservationId},
    scope::WorkKind,
};
use novarocks_execution_contract::{AdmissionTicketId, QueryContextAdmissionTicketReceipt};
use novarocks_worker_protocol::control::ControlEffect;
use std::collections::BTreeMap;

#[derive(Debug)]
pub(crate) enum ControlTicketError {
    WrongActor,
    WrongEffect,
    WrongRootScope,
    DuplicateTicket,
    DuplicateReservation,
    UnknownTicket,
    UnknownReservation,
    ReceiptMismatch,
    Driver(DriverError),
    /// Taking a validated original guard cannot ordinarily fail to bind.
    /// The process supervisor must apply its shared-invariant failure policy.
    BindInvariant(RuntimeError),
    Runtime(RuntimeError),
}

struct ControlOrigin {
    runtime: ActorRuntime<WorkerActor>,
    actor: ActorId,
    // Structural metadata from the first real holder, not numeric authority.
    // The one process root has no supported hot-replacement/reset path.
    scope: ScopeHandle,
}

#[derive(Clone, Copy)]
enum ExitProgress {
    Held,
    BodyObserved,
    CapturesObserved,
}
struct TicketHolder {
    receipt: QueryContextAdmissionTicketReceipt,
    work: WorkLease,
    capacity: CapacityBundle,
    // Only records which calls on this same owned WorkLease succeeded, so a
    // partial ordinary error can retry without signing an exit twice.
    exit: ExitProgress,
}

/// There is one actual holder per admitted ticket, plus an address-only index.
/// No separate count, fee, grant decision or retirement acknowledgement lives
/// here. Unknown Drop uses the original WorkLease/CapacityBundle custody rules;
/// it never returns bound capacity as if a holder had actually exited.
#[must_use = "Retain the original ticket holders until their explicit retirement"]
pub(crate) struct ControlTicketInventory {
    origin: Option<ControlOrigin>,
    holders: BTreeMap<AdmissionTicketId, TicketHolder>,
    reservations: BTreeMap<ReservationId, AdmissionTicketId>,
}
impl ControlTicketInventory {
    pub(crate) fn new() -> Self {
        Self {
            origin: None,
            holders: BTreeMap::new(),
            reservations: BTreeMap::new(),
        }
    }
    fn validate(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        control: ActorId,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ControlTicketError> {
        runtime
            .validate_effect_ticket(ticket)
            .map_err(ControlTicketError::Driver)?;
        if ticket.actor != control {
            return Err(ControlTicketError::WrongActor);
        }
        if let Some(origin) = &self.origin {
            if control != origin.actor {
                return Err(ControlTicketError::WrongActor);
            }
            origin
                .runtime
                .validate_effect_ticket(ticket)
                .map_err(ControlTicketError::Driver)?;
        }
        Ok(())
    }
    /// Only a newly admitted grant transfers a guard. A pure replay must find
    /// its same actual receipt here and cannot reserve or bind another holder.
    pub(crate) fn adopt(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        control: ActorId,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<QueryContextAdmissionTicketReceipt, ControlTicketError> {
        self.validate(runtime, control, ticket)?;
        let (receipt, admitted) = match &ticket.effect {
            WorkerAction::Control {
                effect:
                    ControlEffect::AdmissionReply {
                        result: Ok(receipt),
                        ..
                    },
                admitted,
            } => (*receipt, *admitted),
            _ => return Err(ControlTicketError::WrongEffect),
        };
        let Some(reservation) = admitted else {
            let holder = self
                .holders
                .get(&receipt.ticket_id())
                .ok_or(ControlTicketError::UnknownTicket)?;
            if holder.receipt != receipt {
                return Err(ControlTicketError::ReceiptMismatch);
            }
            return Ok(holder.receipt);
        };
        if self.holders.contains_key(&receipt.ticket_id()) {
            return Err(ControlTicketError::DuplicateTicket);
        }
        if self.reservations.contains_key(&reservation) {
            return Err(ControlTicketError::DuplicateReservation);
        }
        let guard = runtime
            .take_step_reservation(ticket, reservation)
            .map_err(ControlTicketError::Driver)?;
        if self
            .origin
            .as_ref()
            .is_some_and(|origin| origin.scope.id() != guard.scope().id())
        {
            // Still pure and unbound: its guard may roll back. No previous
            // holder or bound fee is removed from the original inventory.
            return Err(ControlTicketError::WrongRootScope);
        }
        let (work, capacity) = guard
            .bind(WorkKind::Business)
            .map_err(ControlTicketError::BindInvariant)?;
        if self.origin.is_none() {
            self.origin = Some(ControlOrigin {
                runtime: runtime.clone(),
                actor: control,
                scope: work.scope().clone(),
            });
        }
        self.holders.insert(
            receipt.ticket_id(),
            TicketHolder {
                receipt,
                work,
                capacity,
                exit: ExitProgress::Held,
            },
        );
        self.reservations.insert(reservation, receipt.ticket_id());
        Ok(receipt)
    }
    /// The metadata DTO holds no callback or opaque destructor. Its own body
    /// and captures can end here; real children still block actual readiness.
    /// False leaves that same holder in place for a later bounded retry.
    pub(crate) fn retire(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        control: ActorId,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<bool, ControlTicketError> {
        self.validate(runtime, control, ticket)?;
        let reservation = match &ticket.effect {
            WorkerAction::Control {
                effect: ControlEffect::RetireReservation(reservation),
                admitted: None,
            } => *reservation,
            _ => return Err(ControlTicketError::WrongEffect),
        };
        let id = *self
            .reservations
            .get(&reservation)
            .ok_or(ControlTicketError::UnknownReservation)?;
        let holder = self
            .holders
            .get_mut(&id)
            .ok_or(ControlTicketError::UnknownTicket)?;
        if holder.capacity.id() != reservation {
            return Err(ControlTicketError::UnknownReservation);
        }
        if matches!(holder.exit, ExitProgress::Held) {
            holder
                .work
                .body_exited()
                .map_err(ControlTicketError::Runtime)?;
            holder.exit = ExitProgress::BodyObserved;
        }
        if matches!(holder.exit, ExitProgress::BodyObserved) {
            holder
                .work
                .captures_exited()
                .map_err(ControlTicketError::Runtime)?;
            holder.exit = ExitProgress::CapturesObserved;
        }
        if !holder
            .work
            .actual_exit_ready()
            .map_err(ControlTicketError::Runtime)?
        {
            return Ok(false);
        }
        if !holder.capacity.is_released() {
            let resources = holder
                .capacity
                .resources()
                .map_err(ControlTicketError::Runtime)?;
            holder
                .capacity
                .release(&resources)
                .map_err(ControlTicketError::Runtime)?;
        }
        // Every fallible transition ran while the actual holder remained in
        // its original map. Common header reaping is a separate later action.
        self.reservations.remove(&reservation);
        self.holders.remove(&id);
        Ok(true)
    }
    pub(crate) fn ticket_id_for_reservation(
        &self,
        reservation: ReservationId,
    ) -> Option<AdmissionTicketId> {
        self.reservations.get(&reservation).copied()
    }
    pub(crate) fn receipt(
        &self,
        id: AdmissionTicketId,
    ) -> Option<QueryContextAdmissionTicketReceipt> {
        self.holders.get(&id).map(|holder| holder.receipt)
    }
    pub(crate) fn len(&self) -> usize {
        self.holders.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.holders.is_empty()
    }
}
