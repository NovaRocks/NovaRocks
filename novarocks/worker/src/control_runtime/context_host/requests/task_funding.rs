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

//! One original Task-causal R payment. The main author, request outer storage
//! and two source-slot fees leave the original admission as separate owners.
//! Each source fee follows its actual bank slot across generations; a future
//! source may outlive this request without retaining its R position.

use super::*;
use crate::control_runtime::{
    source_positions::SourceSlotAdmission,
    source_slot_issuer::{SourceSlotPayer, reserve_source_slots},
};
use crate::task_completion_roles::AdmittedCompletionRole;

const AUTHOR_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 0,
    batches: 0,
    effects: 0,
    cuts: 0,
    replies: 0,
    completions: 1,
    captures: 1,
    work: 1,
};

/// These are the actual admitted originals, not a promise to fund Source2
/// after it captures a producer. The main owner retains `outer` through its
/// cut/input/delivery retirement, including after the author retires. Each
/// source admission separately carries its original slot payment into the bank.
#[must_use = "The original Task request must retain its outer funding and source holders"]
pub(crate) struct TaskRequestFunding {
    pub author: AdmittedCompletionRole,
    pub sources: [SourceSlotAdmission; 2],
    pub outer: CapacityBundle,
}
impl ContextHostRecord {
    /// Called by this Context's Task request factory before exposing a sender.
    /// All three Work holders are siblings under its actual lifetime; making
    /// the source holders children of the short author creates a retirement
    /// cycle because causal cut retirement itself waits for that author.
    pub(crate) fn reserve_task_request_funding(
        &mut self,
    ) -> Result<TaskRequestFunding, ContextRequestAdmissionRefusal> {
        match self.try_reserve_task_request_funding() {
            Ok(funding) => Ok(funding),
            Err((error, partial)) => Err(self.retain_request_partial(error, partial)),
        }
    }
    fn try_reserve_task_request_funding(
        &self,
    ) -> Result<TaskRequestFunding, (ContextRequestError, Option<ContextRequestPartial>)> {
        let full = StorageNeed::role(true);
        let resources = full
            .resources()
            .map_err(|e| (ContextRequestError::Policy(e), None))?;
        let outer = full
            .checked_sub(AUTHOR_STORAGE)
            .and_then(StorageNeed::resources)
            .map_err(|e| (ContextRequestError::Policy(e), None))?;
        let scope = self.scope();
        let generation = scope
            .gates()
            .map_err(|e| (ContextRequestError::Runtime(e), None))?
            .generation;
        let parent = self.resources.roles[super::super::LIFETIME].capacity.id();
        let guard = scope
            .reserve_child_next(generation, parent, resources, WorkKind::Settlement)
            .map_err(|e| (ContextRequestError::Runtime(e), None))?;
        let (work, capacity) = guard
            .bind(WorkKind::Settlement)
            .unwrap_or_else(|_| std::process::abort());
        let work_retirement = work.retirement_observer();
        let mut partial = ContextRequestPartial {
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
        partial.outer = match partial.capacity.split_next(&outer) {
            Ok(outer) => Some(outer),
            Err(e) => return Err((ContextRequestError::Runtime(e), Some(partial))),
        };
        // Issuance follows the current cleanup epoch, but the consumer is
        // still this Context's original Core. A failed owner must abandon
        // that exact original leg; no cleanup epoch creates another Core.
        partial.report = match ReportSlot::reserve_next(
            scope,
            generation,
            partial.work.id(),
            2,
            [
                (
                    core_consumer(),
                    ConsumerKind::Owner {
                        scope: scope.id(),
                        generation: self.initial_generation,
                    },
                ),
                (external_consumer(), ConsumerKind::Observer),
            ],
        ) {
            Ok(report) => Some(report),
            Err(e) => return Err((ContextRequestError::Runtime(e), Some(partial))),
        };
        let sources = match reserve_source_slots::<2>(
            scope,
            parent,
            self.initial_generation,
            SourceSlotPayer::Retained(partial.outer.as_mut().expect("original R outer")),
        ) {
            Ok(slots) => slots,
            Err(refusal) => {
                partial.sources = Some(refusal.partial);
                return Err((
                    ContextRequestError::SourceSlots(refusal.error),
                    Some(partial),
                ));
            }
        };
        Ok(TaskRequestFunding {
            author: AdmittedCompletionRole {
                work: partial.work,
                capacity: partial.capacity,
                report: partial.report.expect("original R report"),
            },
            sources,
            outer: partial.outer.expect("original Task R outer escrow"),
        })
    }
}
