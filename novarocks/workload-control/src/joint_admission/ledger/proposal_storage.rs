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

//! Original control-slot backing for request construction and active receipt borrows.

use super::*;
use crate::scope::ReclaimWakeOnExit;
use std::ops::Deref;

/// Immutable request-local facts. Installed claims never retain this body.
#[derive(Clone)]
pub(in crate::joint_admission) struct ChangeBody(Arc<Vec<ChangeRecord>>);
impl Deref for ChangeBody {
    type Target = [ChangeRecord];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProposalKey {
    slot: SlotId,
    serial: u64,
    generation: u64,
}

/// This local constructor pin is never stored in State. Its body field in
/// PreparedRequest is declared first and exits before this release author.
pub(crate) struct ProposalConstruction {
    inner: Weak<Inner>,
    owner: WorkId,
    pub(super) key: Option<ProposalKey>,
    pub(super) reserved_child: Option<SlotId>,
    pub(super) reserved_cohort: Vec<SlotId>,
}
impl Drop for ProposalConstruction {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else { return };
        if let Some(inner) = self.inner.upgrade() {
            inner.update(|state| {
                root_domains::release_construction(state, self.owner, key);
                if let Some(slot) = self.reserved_child.take() {
                    state.cancellations.withdraw_child_reservation(slot);
                }
                if !self.reserved_cohort.is_empty() {
                    let parent = state.nodes[&self.owner].cancellation;
                    state
                        .cancellations
                        .cell(parent)
                        .release_fixed_children(self.reserved_cohort.len());
                }
                for slot in self.reserved_cohort.drain(..) {
                    state.cancellations.withdraw_child_reservation(slot);
                }
                state.nodes.get_mut(&self.owner).unwrap().resource_waiters -= 1;
                state.collect(self.owner);
            });
        }
    }
}

/// A temporary unacquired constructor slot is distinct from every capacity
/// proof/configuration/backing error. Full owns no request ticket or body.
pub enum JointTryBegin<T, W = JointProposalSlotWait> {
    Started(T),
    Full(W),
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ProposalWaitReason {
    RootWriter(JointLane),
}

#[derive(Clone, Copy)]
pub(crate) struct ProposalSlotWaitTarget {
    reason: ProposalWaitReason,
    owner: WorkId,
    slot: SlotId,
    generation: u64,
}
pub(crate) enum ProposalReservation {
    Reserved(Option<ProposalConstruction>),
    Full(ProposalSlotWaitTarget),
}

/// Cheap same-source reobservation facts. This carries no heap wait, ticket,
/// grant or physical authority and can move with its original preparation.
#[derive(Clone)]
pub struct JointProposalSlotSignal {
    inner: Weak<Inner>,
    target: ProposalSlotWaitTarget,
}
impl JointProposalSlotSignal {
    pub(super) fn new(inner: Weak<Inner>, target: ProposalSlotWaitTarget) -> Self {
        Self { inner, target }
    }
    /// Only the original stable Source calls this to fill its one paid wait
    /// position. Per-request preparations retain this cheap signal instead.
    pub fn into_wait(self) -> JointProposalSlotWait {
        JointProposalSlotWait::new(self.inner, self.target)
    }
    pub fn is_same_source(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.inner, &other.inner)
            && self.target.owner == other.target.owner
            && self.target.slot == other.target.slot
            && self.target.generation == other.target.generation
            && self.target.reason == other.target.reason
    }
}

/// An event subscription in the original producer's existing fixed field.
/// Ready asks that SAME source to reobserve; it never reserves a Q cell.
pub struct JointProposalSlotWait {
    source: JointProposalSlotSignal,
    future: Pin<Box<dyn Future<Output = Result<(), WorkError>> + Send>>,
}
impl JointProposalSlotWait {
    pub(super) fn new(inner: Weak<Inner>, target: ProposalSlotWaitTarget) -> Self {
        Self {
            source: JointProposalSlotSignal::new(Weak::clone(&inner), target),
            future: Box::pin(wait_proposal_slot(inner, target)),
        }
    }
    pub fn is_for_signal(&self, signal: &JointProposalSlotSignal) -> bool {
        self.source.is_same_source(signal)
    }
    /// Inline layout is already included by the containing original row.
    /// Charge the second, actual named F pointee once per fixed wait position.
    pub fn metadata_layouts() -> (std::alloc::Layout, std::alloc::Layout) {
        fn layout<F: Future<Output = Result<(), WorkError>>>(
            _: fn(Weak<Inner>, ProposalSlotWaitTarget) -> F,
        ) -> std::alloc::Layout {
            std::alloc::Layout::new::<F>()
        }
        (
            std::alloc::Layout::new::<Self>(),
            layout(wait_proposal_slot),
        )
    }
}
impl Future for JointProposalSlotWait {
    type Output = Result<(), WorkError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}
async fn wait_proposal_slot(
    inner: Weak<Inner>,
    target: ProposalSlotWaitTarget,
) -> Result<(), WorkError> {
    let inner = inner.upgrade().ok_or(WorkError::Released)?;
    loop {
        let changed = inner.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let reobserve = {
            let state = inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            let node = state.nodes.get(&target.owner).ok_or(WorkError::Released)?;
            let slot = state
                .cancellations
                .slots
                .get(target.slot.0)
                .ok_or(WorkError::Released)?;
            if node.owner != crate::OwnerState::Active
                || node.cancellation != target.slot
                || slot.phase != SlotPhase::Node(target.owner)
                || slot.generation != target.generation
            {
                return Err(WorkError::Released);
            }
            match target.reason {
                ProposalWaitReason::RootWriter(lane) => {
                    if lane != JointLane::Settlement {
                        node.check(&state.cancellations)?;
                        if state.closed {
                            return Err(WorkError::Closed);
                        }
                    }
                    let domains = node.root_domains.as_ref().ok_or(WorkError::Conflict)?;
                    // A free Q does not make a busy original Root writable.
                    domains.construction.is_none() && domains.pending.is_none()
                }
            }
        };
        if reobserve {
            return Ok(());
        }
        changed.await;
    }
}

impl JointAdmissionHandle {
    pub(crate) fn reserve_proposal(
        &self,
        scope: &WorkScope,
    ) -> Result<Option<ProposalConstruction>, WorkError> {
        match self.try_reserve_proposal(scope)? {
            ProposalReservation::Reserved(custody) => Ok(custody),
            ProposalReservation::Full(_) => Err(WorkError::Capacity("Root writer is busy")),
        }
    }

    pub(crate) fn try_reserve_proposal(
        &self,
        scope: &WorkScope,
    ) -> Result<ProposalReservation, WorkError> {
        self.try_reserve_proposal_mode(scope, None)
    }

    pub(super) fn try_reserve_root_writer(
        &self,
        scope: &WorkScope,
        lane: JointLane,
        observed: Option<u64>,
    ) -> Result<ProposalReservation, WorkError> {
        self.try_reserve_proposal_mode(scope, Some((lane, observed)))
    }

    fn try_reserve_proposal_mode(
        &self,
        scope: &WorkScope,
        writer: Option<(JointLane, Option<u64>)>,
    ) -> Result<ProposalReservation, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let (result, changed) = {
            let mut state = self.inner.state.lock().unwrap();
            let result = (|| {
                let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
                if ledger.driver_closed {
                    return Err(WorkError::Closed);
                }
                let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
                if !state.cancellations.fixed {
                    return Ok(ProposalReservation::Reserved(None));
                }
                let slot = node.cancellation;
                if let Some((lane, observed)) = writer {
                    if node.parent.is_some() || node.owner != crate::OwnerState::Active {
                        return Err(WorkError::Conflict);
                    }
                    if lane != JointLane::Settlement {
                        node.check(&state.cancellations)?;
                        if state.closed {
                            return Err(WorkError::Closed);
                        }
                    }
                    let domains = node.root_domains.as_ref().ok_or(WorkError::Conflict)?;
                    if domains.construction.is_some()
                        || domains.pending.is_some()
                        || observed.is_some_and(|revision| revision != domains.writer_revision)
                    {
                        return Ok(ProposalReservation::Full(ProposalSlotWaitTarget {
                            reason: ProposalWaitReason::RootWriter(lane),
                            owner: scope.id,
                            slot,
                            generation: state.cancellations.slots[slot.0].generation,
                        }));
                    }
                    domains
                        .writer_revision
                        .checked_add(1)
                        .ok_or(WorkError::ArithmeticOverflow)?;
                }
                let waiters = node
                    .resource_waiters
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
                let generation = state.cancellations.slots[slot.0].generation;
                let key = ProposalKey {
                    slot,
                    generation,
                    serial: state.next_id()?,
                };
                let node = state.nodes.get_mut(&scope.id).unwrap();
                node.resource_waiters = waiters;
                if writer.is_some() {
                    node.root_domains.as_mut().unwrap().construction = Some(key);
                }
                Ok(ProposalReservation::Reserved(Some(ProposalConstruction {
                    inner: Arc::downgrade(&self.inner),
                    owner: scope.id,
                    reserved_child: None,
                    reserved_cohort: Vec::new(),
                    key: Some(key),
                })))
            })();
            let changed = matches!(&result, Ok(ProposalReservation::Reserved(Some(_))));
            if changed {
                crate::admission::dispatch(&mut state, &self.inner.config);
                crate::observation::fill_control(&mut state, &self.inner.config);
                state.advance_progress_revision();
            }
            (result, changed)
        };
        if changed {
            crate::admission::drain_wakes(&self.inner);
            self.inner.changed.notify_waiters();
        }
        result
    }

    pub(super) fn store_proposal(
        &self,
        custody: &Option<ProposalConstruction>,
        records: Vec<ChangeRecord>,
    ) -> Result<ChangeBody, WorkError> {
        if let Some(custody) = custody {
            let key = custody.key.ok_or(WorkError::Released)?;
            let state = self.inner.state.lock().unwrap();
            let node = state.nodes.get(&custody.owner).ok_or(WorkError::Released)?;
            let slot = &state.cancellations.slots[key.slot.0];
            if node.cancellation != key.slot || slot.generation != key.generation {
                return Err(WorkError::Conflict);
            }
        }
        Ok(ChangeBody(Arc::new(records)))
    }
}

pub(super) fn queue_proposal(
    state: &mut State,
    custody: &Option<ProposalConstruction>,
    _id: u64,
) -> Result<Option<ProposalKey>, WorkError> {
    let Some(custody) = custody else {
        return Ok(None);
    };
    let key = custody.key.ok_or(WorkError::Released)?;
    let node = state.nodes.get(&custody.owner).ok_or(WorkError::Released)?;
    if node.cancellation != key.slot
        || state.cancellations.slots[key.slot.0].generation != key.generation
    {
        return Err(WorkError::Conflict);
    }
    Ok(Some(key))
}

/// Owned installed claims, independent of temporary proposal construction.
/// Each guard retains its original ledger/Work custody; neither this Vec nor
/// its owning iterator borrows the request's ChangeRecord backing.
pub struct JointGrantedClaims {
    claims: Vec<JointClaim>,
    _exit: ReclaimWakeOnExit,
}
impl std::fmt::Debug for JointGrantedClaims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.claims.fmt(f)
    }
}
impl Deref for JointGrantedClaims {
    type Target = [JointClaim];
    fn deref(&self) -> &Self::Target {
        &self.claims
    }
}
impl JointGrantedClaims {
    pub fn pop(&mut self) -> Option<JointClaim> {
        self.claims.pop()
    }
    pub fn remove(&mut self, index: usize) -> JointClaim {
        self.claims.remove(index)
    }
    pub(super) fn new(claims: Vec<JointClaim>, body: ChangeBody, inner: &Arc<Inner>) -> Self {
        // Installed ClaimRecords own their active facts, and each JointClaim
        // owns its exact ledger identity. The consumed reply no longer reads
        // ChangeRecords. Release only this raw body alias: active proof/receipt
        // aliases still prevent the original cell from being reclaimed.
        // ChangeBody Drop never invokes callbacks, including under State.
        drop(body);
        Self {
            claims,
            _exit: ReclaimWakeOnExit::observe(&Arc::downgrade(inner)),
        }
    }
}

impl std::ops::DerefMut for JointGrantedClaims {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.claims
    }
}

pub struct JointGrantedClaimsIter {
    claims: std::vec::IntoIter<JointClaim>,
    _exit: ReclaimWakeOnExit,
}
impl IntoIterator for JointGrantedClaims {
    type Item = JointClaim;
    type IntoIter = JointGrantedClaimsIter;
    fn into_iter(self) -> Self::IntoIter {
        JointGrantedClaimsIter {
            claims: self.claims.into_iter(),
            _exit: self._exit,
        }
    }
}
impl Iterator for JointGrantedClaimsIter {
    type Item = JointClaim;
    fn next(&mut self) -> Option<Self::Item> {
        self.claims.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.claims.size_hint()
    }
}
impl DoubleEndedIterator for JointGrantedClaimsIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.claims.next_back()
    }
}
impl ExactSizeIterator for JointGrantedClaimsIter {}
impl std::iter::FusedIterator for JointGrantedClaimsIter {}

#[cfg(test)]
mod child_reservation_tests {
    use super::*;
    use crate::{CancellationReason, WorkClass, WorkRequest};

    fn drain(driver: &mut crate::joint_admission::JointProgressDriver) {
        for _ in 0..10000 {
            if matches!(
                driver.step(),
                crate::joint_admission::JointProgress::Idle(_)
            ) {
                return;
            }
        }
        panic!("the original driver must reach a finite idle pass");
    }

    #[test]
    fn exact_child_reservation_returns_on_constructor_drop_queue_drop_and_cancel() {
        let (control, handle, mut driver) =
            crate::scope::fixed_control_storage_tests::prototype_profile(3, 1, 1, 1);
        {
            let dependency = handle
                .register_dependency(novarocks_resource_contract::SharedDependency::Network(
                    novarocks_resource_contract::NetworkServiceOrigin::from_selected_url(
                        &url::Url::parse("https://original.example").unwrap(),
                    )
                    .unwrap(),
                ))
                .unwrap();
            let parent = control
                .try_begin_root(WorkRequest::new(WorkClass::Query))
                .unwrap();
            let scope = parent.owner.scope();
            let mut binding = handle
                .request(
                    &scope,
                    vec![JointClaimChange::FirstBind {
                        source: parent.owner.control_claim_view().unwrap(),
                        dependencies: vec![dependency],
                    }],
                )
                .unwrap();
            drain(&mut driver);
            let Poll::Ready(Ok(batch)) =
                Pin::new(&mut binding).poll(&mut Context::from_waker(Waker::noop()))
            else {
                panic!("the real first-binding proof must complete");
            };
            batch
                .adjustment
                .unwrap()
                .install_control(&parent.owner, &[])
                .unwrap();
            drain(&mut driver);
            let head = control
                .inner
                .state
                .lock()
                .unwrap()
                .cancellations
                .free_head
                .unwrap();
            let before = control
                .inner
                .state
                .lock()
                .unwrap()
                .joint
                .as_ref()
                .unwrap()
                .control_snapshot_for_test();
            let prepared = handle.prepare_child(&scope, None).unwrap();
            {
                let state = control.inner.state.lock().unwrap();
                assert_eq!(
                    state.cancellations.slots[head.0].phase,
                    SlotPhase::ChildReserved { parent: scope.id }
                );
                assert_eq!(state.nodes.len(), 1);
                assert_eq!(
                    state.joint.as_ref().unwrap().control_snapshot_for_test(),
                    before
                );
            }
            drop(prepared);
            assert_eq!(
                control.inner.state.lock().unwrap().cancellations.free_head,
                Some(head)
            );
            drain(&mut driver);
            let queued = handle
                .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
                .unwrap();
            {
                let state = control.inner.state.lock().unwrap();
                assert!(matches!(state.cancellations.slots[head.0].phase,
                    SlotPhase::ChildQueued { parent: owner, .. } if owner == scope.id));
            }
            drop(queued);
            assert_eq!(
                control.inner.state.lock().unwrap().cancellations.free_head,
                Some(head)
            );
            drain(&mut driver);
            let mut cancelled = handle
                .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
                .unwrap();
            parent.owner.cancel(CancellationReason::Requested);
            drain(&mut driver);
            // A refused proof has no real child payload or observer to retain.
            assert_eq!(
                control.inner.state.lock().unwrap().cancellations.free_head,
                Some(head)
            );
            assert!(matches!(
                Pin::new(&mut cancelled).poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(JointAdmissionFailure {
                    error: WorkError::Cancelled(_)
                }))
            ));
            parent.business.release();
            parent.owner.complete_after_terminal_cancel_settled();
        }
        drain(&mut driver);
        control.close_admission();
        drain(&mut driver);
        assert!(control.shutdown().is_ok());
    }
}
