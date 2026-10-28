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

//! The actual SourceSlot's original payment, separate from one generation's
//! Work/report. Admission moves already paid units; rearming does not refund
//! them. The bank must detach the real slot before releasing payment outside
//! its lock. This primitive alone supplies no detach or physical-exit proof.

use crate::control_runtime_policy::StorageNeed;
use novarocks_control_runtime::{
    lease::CapacityBundle,
    reported_work::ReportedWork,
    transaction::{RuntimeError, ScopeHandle},
};

pub(crate) const SOURCE_SLOT_STORAGE: StorageNeed = StorageNeed {
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

#[derive(Debug)]
pub(crate) enum SourceSlotStorageError {
    InvalidStorage,
    Runtime(RuntimeError),
}
#[must_use = "A refused admission keeps both original reported Work and payment"]
pub(crate) struct SourceSlotAdmissionRefusal {
    pub error: SourceSlotStorageError,
    pub reported: ReportedWork,
    pub storage: CapacityBundle,
}
impl std::fmt::Debug for SourceSlotAdmissionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceSlotAdmissionRefusal")
            .field("error", &self.error)
            .field("reported", &self.reported)
            .field("storage_reservation", &self.storage.id())
            .finish()
    }
}

#[must_use = "The bank accepts the whole original role and SourceSlot payment"]
pub(crate) struct SourceSlotAdmission {
    reported: ReportedWork,
    storage: SourceSlotStorage,
}
impl std::fmt::Debug for SourceSlotAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceSlotAdmission")
            .field("reported", &self.reported)
            .finish_non_exhaustive()
    }
}

// Only the bank can separate this unique storage owner from its validated
// generation. Other callers receive or return the whole admission.
pub(super) struct SourceSlotStorage {
    scope: ScopeHandle,
    payment: Payment,
}
enum Payment {
    Paid(Option<CapacityBundle>),
    // Explicit finite protocol fixtures have no production payment. This
    // variant is absent from production and proves no quota or byte bound.
    #[cfg(test)]
    Model,
}
impl SourceSlotAdmission {
    /// Borrow the validated original generation before handoff. This neither
    /// clones its permit nor changes storage, exit or consumer authority.
    pub(crate) fn reported(&self) -> &ReportedWork {
        &self.reported
    }
    pub(crate) fn from_paid(
        reported: ReportedWork,
        mut storage: CapacityBundle,
    ) -> Result<Self, SourceSlotAdmissionRefusal> {
        let resources = match storage.resources() {
            Ok(resources) => resources,
            Err(error) => {
                return Err(SourceSlotAdmissionRefusal {
                    error: SourceSlotStorageError::Runtime(error),
                    reported,
                    storage,
                });
            }
        };
        let expected = SOURCE_SLOT_STORAGE
            .resources()
            .expect("fixed source slot storage fits its dimensions");
        if resources != expected {
            return Err(SourceSlotAdmissionRefusal {
                error: SourceSlotStorageError::InvalidStorage,
                reported,
                storage,
            });
        }
        // The actual same-ledger transfer targets this validated original
        // generation, not a caller-supplied numeric scope or Work identity.
        if let Err(error) = storage.transfer(reported.work()) {
            return Err(SourceSlotAdmissionRefusal {
                error: SourceSlotStorageError::Runtime(error),
                reported,
                storage,
            });
        }
        let scope = reported.work().scope().clone();
        Ok(Self {
            reported,
            storage: SourceSlotStorage {
                scope,
                payment: Payment::Paid(Some(storage)),
            },
        })
    }
    pub(super) fn into_slot_parts(self) -> (ReportedWork, SourceSlotStorage) {
        (self.reported, self.storage)
    }
    /// Only an admission not yet transferred to the bank can return these
    /// original owners for real unused/partial cleanup. This grants no exit.
    pub(crate) fn into_originals(self) -> (ReportedWork, Option<CapacityBundle>) {
        (self.reported, self.storage.into_payment())
    }
    /// Explicitly unpaid, finite protocol model. Never use this constructor
    /// as production quota evidence or as an implicit missing-payment fallback.
    #[cfg(test)]
    pub(crate) fn protocol_model(reported: ReportedWork) -> Self {
        let scope = reported.work().scope().clone();
        Self {
            reported,
            storage: SourceSlotStorage {
                scope,
                payment: Payment::Model,
            },
        }
    }
}
impl SourceSlotStorage {
    /// A neutral origin comparison is safe under the bank fence. It does not
    /// lock the Common ledger or authorize a replacement generation.
    pub(super) fn matches_scope(&self, scope: &ScopeHandle) -> bool {
        self.scope.same_scope(scope)
    }
    /// The parent bank already detached the real slot and observed its exact
    /// exits. Idempotence releases no replacement fee; errors retain this same
    /// original bundle. No opaque destructor or callback runs here.
    pub(super) fn release(&mut self) -> Result<(), RuntimeError> {
        match &mut self.payment {
            Payment::Paid(payment) => {
                if let Some(original) = payment {
                    if !original.is_released() {
                        let resources = original.resources()?;
                        original.release(&resources)?;
                    }
                    *payment = None;
                }
            }
            #[cfg(test)]
            Payment::Model => {}
        }
        Ok(())
    }
    pub(super) fn into_payment(self) -> Option<CapacityBundle> {
        match self.payment {
            Payment::Paid(payment) => payment,
            #[cfg(test)]
            Payment::Model => None,
        }
    }
}

#[cfg(test)]
mod tests;
