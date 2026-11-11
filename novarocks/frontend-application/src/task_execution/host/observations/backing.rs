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

//! Structural observation backing admitted against the original Source Work.

use super::{
    MetadataRetirementRefusal, ObservationBudget, ObservationError, ObservationMetadataLayout,
    ObservationMetadataObligation,
    credits::ObservationCredits,
    host_turn::OrderedObservationHost,
    source::{BoundObservationSource, ObservationMetadataAuthor},
};
use crate::control_runtime::actor::FrontendActor;
use novarocks_control_runtime::obligation::{ReservationObligation, WorkObligation};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::DimensionId,
};
use novarocks_type_contract::owned_resources::{btree::node_layout, layout::arc_layout};
use std::{
    alloc::Layout,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

pub(crate) const OBSERVATION_BACKING_BYTES: DimensionId = DimensionId::new(13).unwrap();

pub(super) fn arc<T>() -> Result<Layout, ObservationError> {
    arc_layout(Layout::new::<T>()).map_err(|_| ObservationError::Budget)
}

pub(super) fn array<T>(len: usize) -> Result<usize, ObservationError> {
    Layout::array::<T>(len)
        .map(|layout| layout.size())
        .map_err(|_| ObservationError::Budget)
}

pub(super) fn sum<const N: usize>(parts: [usize; N]) -> Result<usize, ObservationError> {
    parts
        .into_iter()
        .try_fold(0usize, |total, bytes| total.checked_add(bytes))
        .ok_or(ObservationError::Budget)
}

pub(super) fn tree<K, V>(entries: usize) -> Result<usize, ObservationError> {
    let node = node_layout::<K, V>().map_err(|_| ObservationError::Budget)?;
    // Every retained node can be charged as an internal allocation. Add a
    // full possible split cascade and root above the retained inventory.
    let height = (usize::BITS - entries.leading_zeros()) as usize;
    entries
        .checked_add(height)
        .and_then(|nodes| nodes.checked_add(1))
        .and_then(|nodes| nodes.checked_mul(node.size()))
        .ok_or(ObservationError::Budget)
}

pub(crate) struct ObservationBackingQuote {
    layout: ObservationMetadataLayout,
    participants: usize,
    quota: ResourceVector,
}

impl ObservationBackingQuote {
    pub(crate) fn new(
        budget: ObservationBudget,
        participants: usize,
    ) -> Result<Self, ObservationError> {
        let layout = ObservationMetadataLayout::for_actor::<FrontendActor>(budget)?;
        let bytes = sum([
            layout.writer_bytes()?,
            ObservationCredits::backing_bytes(budget.total_positions, participants)?,
            BoundObservationSource::backing_bytes(participants)?,
            OrderedObservationHost::backing_bytes(participants)?,
            arc::<PaidObservationMetadata>()?.size(),
            arc::<BackingCharge>()?.size(),
            Layout::new::<ObservationBackingPin>().size(),
        ])?;
        let quota = ResourceVector::new([(
            OBSERVATION_BACKING_BYTES,
            u64::try_from(bytes).map_err(|_| ObservationError::Budget)?,
        )])
        .map_err(|_| ObservationError::Budget)?;
        Ok(Self {
            layout,
            participants,
            quota,
        })
    }

    pub(crate) fn quota(&self) -> &ResourceVector {
        &self.quota
    }

    /// Use the existing WC scope inventory for both the number of Sources
    /// and each Source's maximum frozen roster. This conservative envelope
    /// accommodates a closed Source still held by a physical alias.
    pub(crate) fn process_limit(
        budget: ObservationBudget,
        scopes: NonZeroUsize,
    ) -> Result<ResourceVector, ObservationError> {
        let per_source = Self::new(budget, scopes.get())?;
        let bytes = per_source
            .quota
            .get(OBSERVATION_BACKING_BYTES)
            .checked_mul(u64::try_from(scopes.get()).map_err(|_| ObservationError::Budget)?)
            .ok_or(ObservationError::Budget)?;
        ResourceVector::new([(OBSERVATION_BACKING_BYTES, bytes)])
            .map_err(|_| ObservationError::Budget)
    }
}

const UNPUBLISHED: u8 = 0;
const RETAINED: u8 = 1;
const EXITED: u8 = 2;

struct BackingCharge {
    reservation: ReservationObligation,
    quota: ResourceVector,
    phase: AtomicU8,
}

impl BackingCharge {
    fn mark_exited(&self, expected: u8) {
        self.phase
            .compare_exchange(expected, EXITED, Ordering::AcqRel, Ordering::Acquire)
            .expect("only the original observation owner proves exit");
    }
}

impl Drop for BackingCharge {
    fn drop(&mut self) {
        if *self.phase.get_mut() != EXITED {
            // A lost proof keeps the original reservation unresolved.
            return;
        }
        if let Err(error) = self
            .reservation
            .release_quota(&self.quota)
            .and_then(|()| self.reservation.finish())
        {
            // The active reservation records the unresolved row on Drop.
            tracing::error!(?error, "retire original observation backing");
        }
    }
}

/// Keep this after the actual backing in every holder. The last holder
/// releases the one prepaid partition after the explicit Source exit.
#[derive(Clone)]
pub(crate) struct ObservationBackingPin(Arc<BackingCharge>);

impl ObservationMetadataObligation for ObservationBackingPin {
    fn backing_pin(&self) -> Option<Self> {
        Some(self.clone())
    }

    fn retire_after_exit(self: Box<Self>) -> Result<(), MetadataRetirementRefusal> {
        self.0.mark_exited(RETAINED);
        Ok(())
    }
}

pub(crate) struct PaidObservationMetadata {
    layout: ObservationMetadataLayout,
    participants: usize,
    backing: ObservationBackingPin,
}

impl PaidObservationMetadata {
    pub(crate) fn take_from(
        quote: ObservationBackingQuote,
        work: &WorkObligation,
        positions: &mut ReservationObligation,
    ) -> Result<Arc<Self>, ObservationError> {
        if !positions.belongs_to_bound_work(work.scope(), work.id()) {
            return Err(ObservationError::Budget);
        }
        let reservation = positions
            .split_next(&ReservationResources::new(
                quote.quota.clone(),
                Default::default(),
            ))
            .map_err(|error| ObservationError::Runtime(error.into()))?;
        Ok(Arc::new(Self {
            layout: quote.layout,
            participants: quote.participants,
            backing: ObservationBackingPin(Arc::new(BackingCharge {
                reservation,
                quota: quote.quota,
                phase: AtomicU8::new(UNPUBLISHED),
            })),
        }))
    }

    pub(crate) fn pin(&self) -> ObservationBackingPin {
        self.backing.clone()
    }

    pub(crate) fn retire_unpublished(&self) {
        self.backing.0.mark_exited(UNPUBLISHED);
    }
}

impl ObservationMetadataAuthor for PaidObservationMetadata {
    fn retain(
        &self,
        required: ObservationMetadataLayout,
        participants: usize,
    ) -> Result<Box<dyn ObservationMetadataObligation>, ObservationError> {
        if self.layout != required || self.participants != participants {
            return Err(ObservationError::Budget);
        }
        self.backing
            .0
            .phase
            .compare_exchange(UNPUBLISHED, RETAINED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| ObservationError::Busy)?;
        Ok(Box::new(self.backing.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_control_runtime::transaction::RuntimeLedger;
    use novarocks_control_runtime_core::{
        identity::Generation,
        scope::{WorkAdmission, WorkKind},
    };

    #[test]
    fn dropping_the_exit_proof_leaves_the_original_backing_charge_unresolved() {
        let quote = ObservationBackingQuote::new(ObservationBudget::for_task_attempt(), 1).unwrap();
        let bytes = quote.quota().get(OBSERVATION_BACKING_BYTES);
        let layout = quote.layout;
        let ledger = RuntimeLedger::new(quote.quota().clone());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut work, mut reservation) = scope
            .reserve_next(
                generation,
                ReservationResources::for_work(quote.quota().clone()),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
        let metadata = PaidObservationMetadata::take_from(quote, &work, &mut reservation).unwrap();
        let proof = metadata.retain(layout, 1).unwrap();
        let alias = proof.backing_pin().unwrap();
        drop((metadata, proof, alias));
        assert_eq!(
            ledger
                .try_capacity_dimensions([OBSERVATION_BACKING_BYTES])
                .unwrap()
                .unwrap()
                .used,
            [bytes]
        );
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        reservation.finish().unwrap();
        scope.close_external().unwrap();
        assert!(!scope.join_ready().unwrap());
    }
}
