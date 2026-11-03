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

//! One original Task-causal request admission. External R work belongs to the
//! process request scope. Its two original Sources derive independently from
//! the same process baseline, while the exact cut retains its own exit gates.
//! Core report consumption still belongs to the exact destination actor.

use super::*;
use crate::control_runtime::{
    source_positions::{SOURCE_SLOT_STORAGE, SourceSlotAdmission},
    source_slot_issuer::{
        SourceSlotPayer, reserve_source_slots, reserve_source_slots_from_authority,
    },
};
use crate::task_completion_set::AdmittedCompletionTransfer;
use novarocks_control_runtime_core::scope::WorkAdmission;

const AUTHOR_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 0,
    batches: 0,
    effects: 0,
    cuts: 0,
    replies: 0,
    completions: 1,
    captures: 4,
    work: 1,
};

/// These are the actual admitted originals, not a promise to fund Source2
/// after it captures a producer. The main owner retains `outer` through its
/// cut/input/delivery retirement, including after the author retires. Each
/// source admission carries its original bound positions into the bank.
#[must_use = "The original Task request must retain its outer funding and source holders"]
pub(crate) struct TaskRequestFunding {
    pub author: AdmittedCompletionTransfer,
    pub sources: [SourceSlotAdmission; 2],
    pub outer: ReservationObligation,
    // One unique subscription signed by this actual R Work. Deferred EOF
    // moves it to the R before the same funding later binds a causal cut.
    pub header_ready: Option<HeaderReadyRegistration>,
}
impl ParticipationHostRecord {
    /// Called by this Context's Task request factory before exposing a sender.
    /// External R and Source2 share the original process financial Scope.
    /// Source birth belongs to the independent Root baseline, so future
    /// Sources can outlive R without retaining Context execution Work. The
    /// exact cut's consumed prefix keeps its separate real retirement gates.
    pub(crate) fn reserve_task_request_funding(
        &mut self,
    ) -> Result<TaskRequestFunding, ParticipationRequestAdmissionRefusal> {
        match self.try_reserve_task_request_funding(true) {
            Ok(funding) => Ok(funding),
            Err((error, partial)) => Err(self.retain_request_partial(error, partial)),
        }
    }
    pub(super) fn try_reserve_task_request_funding(
        &self,
        reply: bool,
    ) -> Result<
        TaskRequestFunding,
        (
            ParticipationRequestError,
            Option<ParticipationRequestPartial>,
        ),
    > {
        let full = StorageNeed::causal_cut_storage(reply);
        let mut author_storage = AUTHOR_STORAGE;
        author_storage.captures = if reply { 4 } else { 1 };
        let resources = full
            .resources()
            .map_err(|e| (ParticipationRequestError::Policy(e), None))?;
        let source_storage = SOURCE_SLOT_STORAGE
            .checked_mul(2)
            .map_err(|e| (ParticipationRequestError::Policy(e), None))?;
        let source_resources = source_storage
            .resources()
            .map_err(|e| (ParticipationRequestError::Policy(e), None))?;
        let outer = full
            .checked_sub(author_storage)
            .and_then(|remaining| remaining.checked_sub(source_storage))
            .and_then(StorageNeed::resources)
            .map_err(|e| (ParticipationRequestError::Policy(e), None))?;
        let scope = if reply {
            self.request_scope()
        } else {
            self.scope()
        };
        let generation = scope
            .gates()
            .map_err(|e| (ParticipationRequestError::Runtime(e), None))?
            .generation;
        let admission = if reply {
            WorkAdmission::External
        } else {
            WorkAdmission::Derived(
                self.resources.metadata_resources[super::super::LIFETIME]
                    .work
                    .id(),
            )
        };
        let guard = scope
            .reserve_next(generation, resources, WorkKind::Settlement, admission)
            .map_err(|e| (ParticipationRequestError::Runtime(e), None))?;
        let (work, capacity) = guard
            .bind(WorkKind::Settlement)
            .unwrap_or_else(|_| std::process::abort());
        let work_retirement = work.retirement_observer();
        let mut partial = ParticipationRequestPartial {
            ledger: self.ledger.clone(),
            work,
            capacity,
            report: None,
            permit: None,
            work_retirement,
            report_revoked: false,
            exited: false,
            unresolved: false,
            outer: None,
            sources: None,
        };
        // During issuance this original partial retains the Source-only
        // parcel. The R outer remains in its original financial capacity until
        // Source2 is bound; every refusal therefore returns all originals.
        partial.outer = match partial.capacity.split_next(&source_resources) {
            Ok(parcel) => Some(parcel),
            Err(e) => return Err((ParticipationRequestError::Runtime(e), Some(partial))),
        };
        // External R's Source-only parcel is already in the Root financial
        // Scope. Its actual baseline capability selects the parent at birth;
        // neither that parcel nor an existing Work is reparented afterward.
        // Internal feedback retains the original live Context lineage.
        if !reply
            && let Err(error) = partial
                .outer
                .as_mut()
                .expect("original Source2 parcel")
                .transfer(&self.resources.metadata_resources[super::super::LIFETIME].work)
        {
            return Err((ParticipationRequestError::Runtime(error), Some(partial)));
        }
        // Issuance follows the current cleanup epoch, but the consumer is
        // still this Context's original Core. A failed owner must abandon
        // that exact original leg; no cleanup epoch creates another Core.
        partial.report = match ReportSlot::reserve_next(
            &mut partial.capacity,
            generation,
            2,
            [
                (
                    core_consumer(),
                    ConsumerKind::Owner {
                        scope: self.registration().scope_id(),
                        generation: self.registration().initial_epoch(),
                    },
                ),
                (external_consumer(), ConsumerKind::Observer),
            ],
        ) {
            Ok(report) => Some(report),
            Err(e) => return Err((ParticipationRequestError::Runtime(e), Some(partial))),
        };
        let core_owner = (
            self.registration().scope_id(),
            self.registration().initial_epoch(),
        );
        let payer =
            SourceSlotPayer::Retained(partial.outer.as_mut().expect("original Source2 parcel"));
        let issued = if reply {
            reserve_source_slots_from_authority::<2>(
                scope,
                &self.process_source_parent,
                core_owner,
                payer,
            )
        } else {
            reserve_source_slots::<2>(
                scope,
                &self.resources.metadata_resources[super::super::LIFETIME].work,
                core_owner,
                payer,
            )
        };
        let sources = match issued {
            Ok(slots) => slots,
            Err(refusal) => {
                partial.sources = Some(refusal.partial);
                return Err((
                    ParticipationRequestError::SourceSlots(refusal.error),
                    Some(partial),
                ));
            }
        };
        // Bound Source positions now belong to their own actual roles. This
        // carrier contains no R quota and only never-bound unused inventory.
        if let Err(error) = partial
            .outer
            .as_mut()
            .expect("original Source2 parcel")
            .finish()
        {
            partial.sources = Some(SourceSlotIssuePartial::from_admissions(scope, sources));
            return Err((ParticipationRequestError::Runtime(error), Some(partial)));
        }
        partial.outer = None;
        partial.outer = match partial.capacity.split_next(&outer) {
            Ok(outer) => Some(outer),
            Err(error) => {
                partial.sources = Some(SourceSlotIssuePartial::from_admissions(scope, sources));
                return Err((ParticipationRequestError::Runtime(error), Some(partial)));
            }
        };
        let header_ready = partial
            .work
            .header_readiness_registration()
            .unwrap_or_else(|_| std::process::abort());
        Ok(TaskRequestFunding {
            author: AdmittedCompletionTransfer {
                work: partial.work,
                capacity: partial.capacity,
                report: partial.report.expect("original R report"),
            },
            sources,
            outer: partial.outer.expect("original Task R outer escrow"),
            header_ready: Some(header_ready),
        })
    }
}
