// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Lane access ownership and allocation facts, independent of funding rights.
use super::{CoverageError, RecordOwner, RecordRef, ResponsibilityClass, SlotCore};
use crate::{
    Account, AccountId,
    sync::{Arc, Mutex, Ordering},
};

#[derive(Debug)]
pub(crate) struct LaneShared {
    pub record: RecordOwner,
    pub affiliation: Mutex<Account>,
    pub member: Mutex<Option<Arc<crate::membership::MemberNode>>>,
    pub query_origin: bool,
}
/// Sharing a lane retains access responsibility; it mints no authorization.
#[derive(Clone, Debug)]
pub struct Lane(pub(crate) Arc<LaneShared>);
impl Lane {
    pub(crate) fn new(
        account: &Account,
        region: super::RecordRegion,
    ) -> Result<Self, CoverageError> {
        let query = crate::account::Path::new(account)
            .nodes
            .iter()
            .flatten()
            .any(|node| {
                node.kind() == crate::AccountKind::Work
                    && node.0.retired.load(Ordering::Acquire) == 0
            });
        let record = account.0.shared.record_store.acquire(
            account.id().get(),
            if query {
                ResponsibilityClass::Query
            } else {
                ResponsibilityClass::Service
            },
            region,
        )?;
        Ok(Self(Arc::new(LaneShared {
            record,
            member: Mutex::new(None),
            query_origin: query,
            affiliation: Mutex::new(account.clone()),
        })))
    }
    pub fn reference(&self) -> RecordRef {
        self.0.record.reference()
    }
    pub fn record(&self) -> &super::LaneRecord {
        self.0.record.record()
    }
    pub fn origin(&self) -> AccountId {
        AccountId::new(self.record().origin.load(Ordering::Acquire))
    }
    pub fn affiliation(&self) -> Account {
        self.0.affiliation.lock().unwrap().clone()
    }
    pub(crate) fn set_affiliation(&self, account: Account) {
        let member = self.detach_member();
        *self.0.affiliation.lock().unwrap() = account;
        if let Some(member) = &member {
            self.affiliation().0.members.insert(member);
        }
        let query = crate::account::Path::new(&self.affiliation())
            .nodes
            .iter()
            .flatten()
            .any(|node| {
                node.kind() == crate::AccountKind::Work
                    && node.0.retired.load(Ordering::Acquire) == 0
            });
        self.record().set_class(if query {
            ResponsibilityClass::Query
        } else {
            ResponsibilityClass::Residual
        });
    }
    pub(crate) fn publish_member(&self, member: Arc<crate::membership::MemberNode>) {
        self.affiliation().0.members.insert(&member);
        *self.0.member.lock().unwrap() = Some(member);
    }
    pub(crate) fn detach_member(&self) -> Option<Arc<crate::membership::MemberNode>> {
        let member = self.0.member.lock().unwrap().clone();
        if let Some(member) = &member {
            self.affiliation().0.members.remove(member);
        }
        member
    }
    pub(crate) fn restore_member(&self, member: &Option<Arc<crate::membership::MemberNode>>) {
        if let Some(member) = member {
            self.affiliation().0.members.insert(member);
        }
    }
    pub fn seal(&self) {
        self.record().seal();
    }
    pub(crate) fn enter(&self) -> bool {
        let entered = self.record().enter();
        if !entered {
            self.store().faults.scope_refused();
        }
        entered
    }
    pub(crate) fn leave(&self) {
        self.record().leave();
    }
    pub fn stop_producing(&self) -> Result<(), CoverageError> {
        self.seal();
        if self.record().scope_active() {
            return Err(CoverageError::ScopeRefused);
        }
        self.record().stop();
        Ok(())
    }
    pub fn production_state(&self) -> super::ProductionState {
        self.record().production_state()
    }
    pub fn responsibility_class(&self) -> ResponsibilityClass {
        self.record().responsibility_class()
    }
    pub(crate) fn store(&self) -> &super::RecordStore {
        self.0.record.store()
    }
    pub(crate) fn bump_sequence(&self) {
        self.record().sequence.fetch_add(1, Ordering::Release);
    }
    pub fn live_bytes(&self) -> u64 {
        let record = self.record();
        let sum = i128::from(record.tagged.load(Ordering::Acquire))
            + i128::from(record.r1_small.load(Ordering::Acquire));
        sum.max(0) as u64
    }
    pub(crate) fn reclaimable(&self) -> bool {
        self.reclaimable_with_handles(1)
    }
    pub(crate) fn reclaimable_with_handles(&self, handles: usize) -> bool {
        let life = self.record().lifetime();
        life.outstanding() == 0 && life.pins() == 0 && Arc::strong_count(&self.0) == handles
    }
    /// Publishes one proven allocation that was not already published by the
    /// attribution wrapper/helper. The exact physical request size accompanies
    /// its one eventual release. Real Rust allocation Layout sizes fit i64.
    pub(crate) fn publish_fact(&self, bytes: u64) -> FactToken {
        let (tagged, small) = fact_bytes(bytes);
        // SAFETY: this strong lane handle retains RecordOwner through publication.
        unsafe { SlotCore::direct(self.store(), self.reference(), tagged, small, 1) };
        FactToken {
            reference: self.reference(),
            #[cfg(loom)]
            store: std::ptr::from_ref(self.store()),
        }
    }
}
fn fact_bytes(bytes: u64) -> (i64, i64) {
    // Proven underlying Rust allocation sizes satisfy Layout's isize bound.
    if bytes < super::ATTRIBUTION_THRESHOLD_BYTES as u64 {
        (0, bytes as i64)
    } else {
        (bytes as i64, 0)
    }
}
/// Pointer-free production identity of one successful allocation. A copied
/// value confers no additional access right or allocation obligation.
#[derive(Clone, Copy, Debug)]
pub struct FactToken {
    reference: RecordRef,
    // Model instances have independent storage. The model caller retains that
    // store until every allocation access ends; production uses the static store.
    #[cfg(loom)]
    store: *const super::RecordStore,
}
// SAFETY: the token is only an identity. Actual access requires the linear
// outstanding-allocation contract below; model backing is independently retained.
unsafe impl Send for FactToken {}
unsafe impl Sync for FactToken {}
impl FactToken {
    pub const fn reference(self) -> RecordRef {
        self.reference
    }
    fn store(&self) -> &super::RecordStore {
        #[cfg(not(loom))]
        {
            super::global_store()
        }
        #[cfg(loom)]
        // SAFETY: model callers retain their instance to the last token access.
        {
            unsafe { &*self.store }
        }
    }
    /// Releases one successful allocation after its actual underlying free.
    ///
    /// # Safety
    /// Use the exact issued token and size once, after underlying storage is
    /// freed. A copied token is not a second obligation. Do not access it after
    /// this release. Model callers must retain the instance store to this point;
    /// the production process-lifetime store does not depend on authority lifetime.
    pub unsafe fn record_deallocation(self, bytes: u64) {
        let (tagged, small) = fact_bytes(bytes);
        // SAFETY: the genuine allocation protects its final publication; direct
        // performs no record access after the final count decrement.
        unsafe { SlotCore::direct(self.store(), self.reference, -tagged, -small, -1) };
    }
    /// # Safety
    /// The allocation described by this token must still be outstanding.
    pub unsafe fn account_id(self) -> AccountId {
        let record = self
            .store()
            .resolve(self.reference)
            .expect("outstanding allocation identity");
        AccountId::new(record.origin.load(Ordering::Acquire))
    }
}
#[cfg(not(loom))]
const _: () = assert!(std::mem::size_of::<FactToken>() == 8);

impl Drop for LaneShared {
    fn drop(&mut self) {
        if let Some(member) = self.member.lock().unwrap().take() {
            self.affiliation.lock().unwrap().0.members.remove(&member);
        }
    }
}
impl Account {
    /// Creates attribution access only. This performs no capacity qualification,
    /// stock consumption or funding-ledger commitment.
    #[cfg(all(test, loom))]
    pub(crate) fn create_lane(&self) -> Result<Lane, CoverageError> {
        self.0.shared.prune_observations(16);
        let lane = Lane::new(self, super::RecordRegion::Observation)?;
        let member = crate::membership::MemberNode::lane(&lane.0, None);
        let path = crate::account::Path::new(self);
        let _gates = path.shared_gates();
        if path
            .nodes
            .iter()
            .flatten()
            .any(|a| a.0.closed.load(Ordering::Acquire) != 0)
        {
            return Err(CoverageError::AccountClosed);
        }
        lane.publish_member(member);
        self.0.shared.retain_observation(&lane);
        Ok(lane)
    }
}
