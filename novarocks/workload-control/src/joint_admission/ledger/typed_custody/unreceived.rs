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

//! Exact finite FE responsibility in the original paid ClaimRecord.
//! Pure metadata remains after its last frontend handle; Drop is never cleanup.
use super::*;
use std::num::{NonZeroU64, NonZeroUsize};

/// Actual occurrence count and exact serialized pure metadata allocation.
/// This does not describe unknown remote object bytes or grant publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnreceivedProductionQuote {
    records: NonZeroU64,
    facts_bytes: NonZeroUsize,
}
impl UnreceivedProductionQuote {
    pub fn new(records: NonZeroU64, facts_bytes: NonZeroUsize) -> Result<Self, WorkError> {
        u64::try_from(facts_bytes.get()).map_err(|_| WorkError::ArithmeticOverflow)?;
        Ok(Self {
            records,
            facts_bytes,
        })
    }
    pub fn facts_bytes(self) -> usize {
        self.facts_bytes.get()
    }
    pub fn records(self) -> u64 {
        self.records.get()
    }
    /// One actual attempt metadata record, containing every frozen occurrence.
    /// Its known publication responsibility is prepaid before writer effects.
    pub fn required_progress(self) -> JointResources {
        JointResources::ZERO
            .with(JointResource::ClaimRecords, 1)
            .with(JointResource::MetadataBytes, self.facts_bytes.get() as u64)
            .with(JointResource::UnreceivedProductions, self.records.get())
            .with(JointResource::Publications, 1)
    }
    pub fn record_inline_layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<ProductionRecord>()
    }
    pub fn receiving_handle_layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<UnreceivedProductionGrant>()
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Unknown,
    Received,
}
/// The original map owns only pure finite bytes and an existing origin Weak.
/// No Inner, session, credentials, callback, payload or deletion authority.
pub(in crate::joint_admission::ledger) struct ProductionRecord {
    origin: Weak<dyn Send + Sync>,
    facts: Box<[u8]>,
    records: u64,
    phase: Phase,
}
/// This request creates no output and permits no dispatch until its typed
/// registration is installed in the original receiving session.
#[must_use]
pub struct UnreceivedProductionAdmission {
    admission: JointAdmission,
    scope: WorkScope,
    origin: Weak<dyn Send + Sync>,
    quote: UnreceivedProductionQuote,
}
impl WorkScope {
    /// A closed delegated constructor: only these responsibility coordinates,
    /// the original active Node's complete class/D, and original paid Q.
    pub fn begin_unreceived_productions(
        &self,
        origin: Weak<dyn Send + Sync>,
        quote: UnreceivedProductionQuote,
    ) -> Result<UnreceivedProductionAdmission, WorkError> {
        if origin.strong_count() == 0 {
            return Err(WorkError::Released);
        }
        let handle = JointAdmissionHandle {
            inner: Arc::clone(&self.inner),
        };
        let mut proposal = handle.begin_paid_proposal_for_scope(self.clone())?;
        let facts = {
            let state = self.inner.state.lock().unwrap();
            Arc::clone(control_facts(&state, self)?)
        };
        proposal.begin_new(JointLane::Custody, quote.required_progress())?;
        // This closed constructor delegates the original active control's
        // exact canonical relationship, not just its local S. The existing
        // proof FactCapture expands S union Root D and preserves later Grow.
        // queue_request_mode authenticates this exact Root against our scope.
        proposal.current.as_mut().ok_or(WorkError::Conflict)?.origin = facts.key.origin;
        for dependency in facts.dependencies.iter() {
            proposal.push_dependency(&dependency.fact)?;
        }
        proposal.end_change()?;
        let admission = proposal.submit().map_err(|failure| failure.error)?;
        Ok(UnreceivedProductionAdmission {
            admission,
            scope: self.clone(),
            origin,
            quote,
        })
    }
}
fn control_facts<'a>(
    state: &'a State,
    scope: &WorkScope,
) -> Result<&'a Arc<ResponsibilityFacts>, WorkError> {
    validate_active_control_node(state, scope)?;
    let node = &state.nodes[&scope.id];
    let cell = &state.cancellations.slots[node.cancellation.0];
    let control = state
        .joint
        .as_ref()
        .unwrap()
        .claims
        .get(&cell.claim.ok_or(WorkError::NotReady)?)
        .ok_or(WorkError::Released)?;
    if cell.phase != SlotPhase::Node(scope.id)
        || cell.generation == 0
        || control.owner != ClaimOwner::ControlSlot(node.cancellation)
        || !control.custody.is_raw()
        || control.pending.is_some()
        || control.amounts != cell.base
        || control.active_amounts != cell.base
        || control.facts.key != control.active_facts.key
        || !root_domains::has_effective_domains(state, &control.active_facts, true)
        || !control.active_facts.key.source_kind.is_empty()
    {
        return Err(WorkError::Conflict);
    }
    Ok(&control.active_facts)
}
impl Future for UnreceivedProductionAdmission {
    type Output = Result<UnreceivedProductionReady, JointAdmissionFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.admission).poll(cx).map(|result| {
            result.map(|batch| UnreceivedProductionReady {
                batch,
                scope: self.scope.clone(),
                origin: self.origin.clone(),
                quote: self.quote,
            })
        })
    }
}
#[must_use]
pub struct UnreceivedProductionReady {
    batch: JointGrantedBatch,
    scope: WorkScope,
    origin: Weak<dyn Send + Sync>,
    quote: UnreceivedProductionQuote,
}
/// Unique receiving authority. Its ordinary destructor NEVER refunds either
/// unknown output responsibility or the later exact publication record.
pub struct UnreceivedProductionGrant {
    inner: Arc<Inner>,
    id: u64,
    epoch: u64,
    origin: Weak<dyn Send + Sync>,
}
impl UnreceivedProductionReady {
    /// Compare only original authority/origin identity; no grant or new pin.
    pub fn is_for_owner(&self, owner: &UnreceivedProductionOwner) -> bool {
        Weak::ptr_eq(&Arc::downgrade(&self.scope.inner), &owner.inner)
            && Weak::ptr_eq(&self.origin, &owner.origin)
    }
    /// Caller allocates/fills pure metadata only after this paid Ready exists.
    /// All fallible checks precede moves. Notify/Drop Ready outside all caller
    /// receiving locks, after the grant is installed in its stable destination.
    pub fn register_in(
        &mut self,
        facts: &mut Option<Box<[u8]>>,
        destination: &mut Option<UnreceivedProductionGrant>,
    ) -> Result<WorkloadProgressNotification, WorkError> {
        if destination.is_some()
            || self.origin.strong_count() == 0
            || facts
                .as_ref()
                .is_none_or(|facts| facts.len() != self.quote.facts_bytes())
            || self.batch.new_claims.len() != 1
            || self.batch.adjustment.is_some()
        {
            return Err(WorkError::Conflict);
        }
        let original = &mut self.batch.new_claims[0];
        if !Arc::ptr_eq(&original.inner, &self.scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let id = original.id.ok_or(WorkError::Released)?;
        let inner = Arc::clone(&self.scope.inner);
        inner.update_facts_silent(|state| {
            let full = control_facts(state, &self.scope)?;
            let ledger = state.joint.as_ref().unwrap();
            let row = ledger.claims.get(&id).ok_or(WorkError::Released)?;
            if !row.custody.is_raw()
                || row.production.is_some()
                || row.owner != ClaimOwner::PhysicalWork(self.scope.id)
                || row.pending.is_some()
                || row.amounts != self.quote.required_progress()
                || row.active_amounts != row.amounts
                || row.facts.key != row.active_facts.key
                || row.active_facts.class != full.class
                || row.active_facts.lane != JointLane::Custody
                || row.active_facts.key.origin != full.key.origin
                || row.active_facts.key.dependencies != full.key.dependencies
                || !row.active_facts.key.source_kind.is_empty()
            {
                return Err(WorkError::Conflict);
            }
            let epoch = row
                .structure_revision
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let row = state.joint.as_mut().unwrap().claims.get_mut(&id).unwrap();
            row.structure_revision = epoch;
            row.custody = ClaimCustody::Production { epoch };
            row.production = Some(ProductionRecord {
                origin: self.origin.clone(),
                facts: facts.take().unwrap(),
                records: self.quote.records(),
                phase: Phase::Unknown,
            });
            original.id = None;
            *destination = Some(UnreceivedProductionGrant {
                inner: Arc::clone(&inner),
                id,
                epoch,
                origin: self.origin.clone(),
            });
            Ok(())
        })?;
        Ok(WorkloadProgressNotification::new(inner))
    }
}
impl UnreceivedProductionGrant {
    pub fn owner(&self) -> UnreceivedProductionOwner {
        UnreceivedProductionOwner {
            inner: Arc::downgrade(&self.inner),
            origin: self.origin.clone(),
        }
    }
    fn check<'a>(&self, state: &'a State) -> Result<&'a ClaimRecord, WorkError> {
        let row = state
            .joint
            .as_ref()
            .and_then(|ledger| ledger.claims.get(&self.id))
            .ok_or(WorkError::Released)?;
        if !matches!(row.custody, ClaimCustody::Production { epoch } if epoch == self.epoch)
            || row
                .production
                .as_ref()
                .is_none_or(|record| record.phase != Phase::Unknown)
            || row.pending.is_some()
            || row.amounts != row.active_amounts
        {
            return Err(WorkError::Conflict);
        }
        row.release_revision
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(row)
    }
    /// Preflight only, before the session mutates its receiving union.
    pub fn check_receive(&self) -> Result<(), WorkError> {
        let state = self.inner.state.lock().unwrap();
        self.check(&state).map(|_| ())
    }
    /// Original session calls this only AFTER complete End/domain validation
    /// and installation of its exact set/retention in the stable union. It is
    /// not a BE seal receipt, object deletion permission or a publication grant.
    pub fn receive_in(&self) -> Result<WorkloadProgressNotification, WorkError> {
        self.inner.update_facts_silent(|state| {
            let row = self.check(state)?;
            let released = JointResources::ZERO.with(
                JointResource::UnreceivedProductions,
                row.production.as_ref().unwrap().records,
            );
            let active = row.active_amounts.checked_sub(released)?;
            let charged = row.amounts.checked_sub(released)?;
            let ledger = state.joint.as_mut().unwrap();
            let row = ledger.claims.get_mut(&self.id).unwrap();
            row.amounts = charged;
            row.active_amounts = active;
            row.release_revision += 1;
            row.production.as_mut().unwrap().phase = Phase::Received;
            ledger.released();
            Ok(())
        })?;
        Ok(WorkloadProgressNotification::new(Arc::clone(&self.inner)))
    }
}
/// Observation/terminal custody of the same existing session origin. No
/// identity is minted and no additional metadata registry is installed.
#[derive(Clone)]
pub struct UnreceivedProductionOwner {
    inner: Weak<Inner>,
    origin: Weak<dyn Send + Sync>,
}
impl UnreceivedProductionOwner {
    pub fn same_origin(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.inner, &other.inner) && Weak::ptr_eq(&self.origin, &other.origin)
    }
    pub fn pending_records(&self) -> Result<u64, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let state = inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
        ledger
            .claims
            .values()
            .filter_map(|row| row.production.as_ref())
            .filter(|record| {
                record.phase == Phase::Unknown && Weak::ptr_eq(&record.origin, &self.origin)
            })
            .try_fold(0u64, |sum, record| {
                sum.checked_add(record.records)
                    .ok_or(WorkError::ArithmeticOverflow)
            })
    }
    /// Pure diagnostic equality; bytes are retained even after every FE grant
    /// and session strong alias exits. The data itself grants no object access.
    pub fn retains_facts(&self, facts: &[u8]) -> Result<bool, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let state = inner.state.lock().unwrap();
        Ok(state
            .joint
            .as_ref()
            .ok_or(WorkError::NotReady)?
            .claims
            .values()
            .any(|row| {
                row.production.as_ref().is_some_and(|record| {
                    Weak::ptr_eq(&record.origin, &self.origin) && record.facts.as_ref() == facts
                })
            }))
    }
    /// Only the original Provider/publication terminal owner may call this
    /// after a positive, sufficiently scoped disposition. Unknown records
    /// refuse before any mutation. No timeout/Task terminal/Drop invokes it.
    pub fn settle_received_in(&self) -> Result<WorkloadProgressNotification, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        inner.update_facts_silent(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if ledger.claims.values().any(|row| {
                row.production.as_ref().is_some_and(|record| {
                    Weak::ptr_eq(&record.origin, &self.origin)
                        && (record.phase == Phase::Unknown || row.pending.is_some())
                })
            }) {
                return Err(WorkError::Conflict);
            }
            let mut after = std::ops::Bound::Unbounded;
            loop {
                let id = state
                    .joint
                    .as_ref()
                    .unwrap()
                    .claims
                    .range((after, std::ops::Bound::Unbounded))
                    .find_map(|(&id, row)| {
                        row.production
                            .as_ref()
                            .is_some_and(|record| {
                                record.phase == Phase::Received
                                    && Weak::ptr_eq(&record.origin, &self.origin)
                            })
                            .then_some(id)
                    });
                let Some(id) = id else {
                    break;
                };
                after = std::ops::Bound::Excluded(id);
                remove_claim(state, id);
            }
            Ok(())
        })?;
        Ok(WorkloadProgressNotification::new(inner))
    }
}

#[cfg(test)]
#[path = "unreceived/canonical_tests.rs"]
mod canonical_tests;
