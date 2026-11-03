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

//! Coalesced bounded settlement. No background thread, workload callbacks or
//! FE policy is owned by this core. A caller drives continuation independently
//! of the allocating thread.
use crate::{
    account::Path,
    authority::{MemoryAuthority, Shared},
    domain::FundingDomain,
    error::{CapacityError, Refusal},
    sync::{Arc, Ordering},
};
use std::time::{Duration, Instant};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceReason {
    CapacityReduced,
    ShortageCandidate,
    ExplicitLocalReclaim,
}
#[derive(Debug, Default)]
pub(crate) struct MaintenanceState {
    pub epoch: u64,
    pub cursor: usize,
    pub upper: usize,
    pub domain_upper: usize,
    pub busy: bool,
    pub pending: bool,
    pub deferred_active: u64,
    pub membership_revision: u64,
    pub started_at: Option<Instant>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageReceipt {
    pub epoch: u64,
    pub scanned: usize,
    pub upper: usize,
    pub complete: bool,
    pub deferred_active: u64,
    pub capacity_revision: u64,
    pub root_revision: u64,
    pub membership_revision: u64,
    pub started_at: Instant,
    pub observed_at: Instant,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShortageReceipt {
    pub refusal: Refusal,
    pub coverage: CoverageReceipt,
    pub rechecked_at: Instant,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestOutcome {
    Granted(FundingDomain),
    SettlementPending(CoverageReceipt),
    SharedShortage(ShortageReceipt),
    Refused(CapacityError),
}
// FundingDomain identity equality never compares sampled allocation facts.
impl PartialEq for FundingDomain {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for FundingDomain {}

pub(crate) fn request(shared: &Shared, _reason: MaintenanceReason) -> u64 {
    let mut m = shared.maintenance.lock().unwrap();
    if !m.pending {
        m.epoch += 1;
        m.cursor = 0;
        m.domain_upper = shared.domains.lock().unwrap().upper;
        m.upper = m.domain_upper + shared.accounts.lock().unwrap().upper;
        m.pending = true;
        m.deferred_active = 0;
        m.membership_revision = shared.membership_revision.load(Ordering::Acquire);
        m.started_at = Some(Instant::now());
    }
    m.epoch
}
impl MemoryAuthority {
    pub fn request_maintenance(&self, reason: MaintenanceReason) -> u64 {
        request(&self.shared, reason)
    }
    pub fn maintain(&self, budget: usize) -> CoverageReceipt {
        // Independently driven maintenance starts a successor sweep after a
        // completed one, so late releases do not require the producer to wake.
        request(&self.shared, MaintenanceReason::ExplicitLocalReclaim);
        let (epoch, mut cursor, upper, domain_upper, mut deferred) = {
            let mut m = self.shared.maintenance.lock().unwrap();
            if m.busy {
                return self.coverage(&m, false);
            }
            m.busy = true;
            (
                m.epoch,
                m.cursor,
                m.upper,
                m.domain_upper,
                m.deferred_active,
            )
        };
        let mut scanned = 0;
        while cursor < upper && scanned < budget {
            if cursor < domain_upper {
                let record = self.shared.domains.lock().unwrap().records[cursor].clone();
                if let Some(record) = record {
                    let domain = FundingDomain(record);
                    // Capture before accepting facts: anything published after
                    // this cutoff invalidates actionable coverage until recheck.
                    let cutoff = domain.0.lane.record().sequence.load(Ordering::Acquire);
                    domain.settle();
                    if domain.0.state.lock().unwrap().active {
                        domain.0.state.lock().unwrap().drain_requested = true;
                        deferred += 1;
                        domain.0.covered_sequence.store(cutoff, Ordering::Relaxed);
                        domain.0.covered_epoch.store(epoch, Ordering::Release);
                    } else {
                        domain.drain_idle();
                        domain.0.covered_sequence.store(cutoff, Ordering::Relaxed);
                        domain.0.covered_epoch.store(epoch, Ordering::Release);
                        self.reclaim_record(domain, cursor);
                    }
                }
            } else {
                let index = cursor - domain_upper;
                let account = self.shared.accounts.lock().unwrap().records[index]
                    .as_ref()
                    .and_then(|a| a.upgrade())
                    .map(crate::Account);
                if let Some(account) = account {
                    account.return_slack(u64::MAX);
                }
            }
            cursor += 1;
            scanned += 1;
        }
        self.shared.prune_observations(budget);
        self.shared.record_store.store().reclaim(budget);
        let mut m = self.shared.maintenance.lock().unwrap();
        assert_eq!(
            m.epoch, epoch,
            "coalesced epoch remains owned until batch commit"
        );
        m.cursor = cursor;
        m.deferred_active = deferred;
        m.busy = false;
        m.pending = cursor != upper;
        self.coverage(&m, !m.pending)
    }
    fn reclaim_record(&self, domain: FundingDomain, index: usize) {
        loop {
            let affiliation = domain.affiliation();
            let path = Path::new(&affiliation);
            let _gates = path.exclusive_gates();
            let mut state = domain.0.state.lock().unwrap();
            if domain.affiliation().id() != affiliation.id() {
                continue;
            }
            if state.registered_active || state.external != 0 || !domain.0.lane.reclaimable() {
                return;
            }
            // A final free can occur after this batch's settle/idle sample but
            // before reclaimability becomes true. Accept its final facts while
            // the exact affiliation is pinned; removing just the metadata
            // would otherwise orphan the record's last payload commitment.
            let mut states = path.locks();
            assert_eq!(
                domain.0.lane.live_bytes(),
                0,
                "no allocations retain live bytes"
            );
            let final_payload = state.committed;
            let final_backing = state.authorized;
            let protected = affiliation.0.control.load(Ordering::Acquire) != 0;
            if protected {
                states[0].as_mut().unwrap().slack += final_backing;
                crate::settlement::adjust_commitment(
                    &path,
                    &mut states,
                    final_payload,
                    final_backing,
                );
            } else {
                crate::settlement::adjust_commitment(&path, &mut states, final_payload, 0);
            }
            state.authorized = 0;
            state.committed = 0;
            state.settled_live = 0;
            let member = domain.0.lane.detach_member();
            let removed = {
                let mut registry = self.shared.domains.lock().unwrap();
                // Recheck while index publication is excluded: a concurrent
                // observer may have pinned the record after the first sample.
                if Arc::strong_count(&domain.0) != 2 || !domain.0.lane.reclaimable() {
                    domain.0.lane.restore_member(&member);
                    return;
                }
                let removed = registry.records[index].take();
                registry.free_slots.push(index);
                registry.metadata -= domain.0.metadata;
                self.shared
                    .membership_revision
                    .fetch_add(1, Ordering::Release);
                removed
            };
            let metadata = domain.0.metadata;
            let reference = domain.0.lane.reference();
            drop(state);
            drop(removed);
            drop(domain);
            drop(member);
            assert!(
                self.shared.record_store.store().reclaim_exact(reference),
                "final funding owner must complete exact record reclamation"
            );
            // All affiliation lifecycle gates remain pinned and exclusive.
            // Real stable-record storage is freed before its charge is returned.
            if protected {
                states[0].as_mut().unwrap().slack += metadata;
            } else {
                crate::settlement::adjust_commitment(&path, &mut states, metadata, 0);
            }
            return;
        }
    }
    fn coverage(&self, m: &MaintenanceState, complete: bool) -> CoverageReceipt {
        CoverageReceipt {
            epoch: m.epoch,
            scanned: m.cursor,
            upper: m.upper,
            complete,
            deferred_active: m.deferred_active,
            capacity_revision: self.shared.capacity_revision.load(Ordering::Acquire),
            root_revision: self.root().0.ledger.lock().unwrap().revision,
            membership_revision: m.membership_revision,
            started_at: m.started_at.unwrap_or_else(Instant::now),
            observed_at: Instant::now(),
        }
    }
    /// Validate an observation interval before using shortage as a control fact.
    /// A newer publication, membership change, policy/ledger revision or age
    /// expiry requires another bounded settlement and exact request recheck.
    /// Decisions still recheck at their own commit boundary; no snapshot pins
    /// future allocation/free events.
    pub fn shortage_is_fresh(&self, receipt: &ShortageReceipt, max_age: Duration) -> bool {
        let accounts = self.shared.account_list();
        let _gate = self.root().0.gate.read().unwrap();
        let coverage = &receipt.coverage;
        if !coverage.complete
            || receipt.rechecked_at.elapsed() > max_age
            || coverage.capacity_revision != self.shared.capacity_revision.load(Ordering::Acquire)
            || coverage.membership_revision
                != self.shared.membership_revision.load(Ordering::Acquire)
            || coverage.root_revision != self.root().0.ledger.lock().unwrap().revision
        {
            return false;
        }
        let Some(constraint) = accounts
            .iter()
            .find(|a| a.id() == receipt.refusal.constraint_account)
        else {
            return false;
        };
        let state = constraint.0.ledger.lock().unwrap();
        if state.version != receipt.refusal.policy_revision
            || state.revision != receipt.refusal.ledger_revision
        {
            return false;
        }
        drop(state);
        let upper = self.shared.domains.lock().unwrap().upper;
        for index in 0..upper {
            let domain = self.shared.domains.lock().unwrap().records[index].clone();
            if let Some(domain) = domain
                && (domain.covered_epoch.load(Ordering::Acquire) != coverage.epoch
                    || domain.covered_sequence.load(Ordering::Acquire)
                        != domain.lane.record().sequence.load(Ordering::Acquire))
            {
                return false;
            }
        }
        true
    }
    /// Checks capability ownership only. This grants no funding domain.
    pub fn validate_account(&self, account: &crate::Account) -> Result<(), CapacityError> {
        if !Arc::ptr_eq(&self.shared, &account.0.shared) {
            return Err(CapacityError::Invalid {
                detail: "account belongs to another authority",
            });
        }
        Ok(())
    }
    /// No actionable shared shortage can escape before a bounded sweep and
    /// exact recheck under current target/policy qualification.
    pub fn request_domain(
        &self,
        account: &crate::Account,
        bytes: u64,
        budget: usize,
    ) -> RequestOutcome {
        if let Err(error) = self.validate_account(account) {
            return RequestOutcome::Refused(error);
        }
        match account.create_domain(bytes) {
            Ok(domain) => RequestOutcome::Granted(domain),
            Err(CapacityError::ShortageCandidate(_)) => {
                self.request_maintenance(MaintenanceReason::ShortageCandidate);
                let mut coverage = self.maintain(budget);
                if !coverage.complete {
                    return RequestOutcome::SettlementPending(coverage);
                }
                match account.create_domain(bytes) {
                    Ok(domain) => RequestOutcome::Granted(domain),
                    Err(CapacityError::ShortageCandidate(mut refusal)) => {
                        refusal.requested = bytes;
                        if refusal.capacity_revision != coverage.capacity_revision {
                            self.request_maintenance(MaintenanceReason::ShortageCandidate);
                            return RequestOutcome::SettlementPending(coverage);
                        }
                        coverage.root_revision = self.root().0.ledger.lock().unwrap().revision;
                        let receipt = ShortageReceipt {
                            refusal,
                            coverage,
                            rechecked_at: Instant::now(),
                        };
                        if !self.shortage_is_fresh(&receipt, Duration::from_secs(1)) {
                            self.request_maintenance(MaintenanceReason::ShortageCandidate);
                            return RequestOutcome::SettlementPending(receipt.coverage);
                        }
                        RequestOutcome::SharedShortage(receipt)
                    }
                    Err(error) => RequestOutcome::Refused(error),
                }
            }
            Err(error) => RequestOutcome::Refused(error),
        }
    }
}
impl FundingDomain {
    pub(crate) fn drain_idle(&self) -> u64 {
        self.trim_idle_to(0)
    }
    /// Revoke only inactive spare rights beyond the requested retention.
    /// Floor-funded rights return to their protected pool. An active lane's
    /// stock is never touched by this caller-owned control operation.
    pub fn trim_idle_to(&self, retained_free: u64) -> u64 {
        loop {
            let account = self.affiliation();
            let path = Path::new(&account);
            let _gates = path.exclusive_gates();
            let mut s = self.0.state.lock().unwrap();
            if self.affiliation().id() != account.id() {
                continue;
            }
            if s.active {
                return 0;
            }
            // Floor-backed lanes return idle to the protected account pool;
            // all other genuinely revoked rights reduce ancestor commitment.
            let live = self.0.lane.live_bytes();
            let retained_free = if s.drain_requested { 0 } else { retained_free };
            let obligation = live.checked_add(s.external).expect("valid live and bound");
            // The requested retention is a cap, so MAX means retain every
            // already funded right rather than an overflowing obligation.
            let retained_authorization = s.authorized.min(obligation.saturating_add(retained_free));
            let idle = s.authorized - retained_authorization;
            let desired = retained_authorization.max(obligation);
            let mut states = path.locks();
            if account.0.control.load(Ordering::Acquire) != 0 {
                states[0].as_mut().unwrap().slack += idle;
                // Returning funded idle is a local reclassification. Released
                // unbacked facts still lower total exposure; they never mint F.
                crate::settlement::adjust_commitment(
                    &path,
                    &mut states,
                    s.committed,
                    desired.checked_add(idle).expect("valid protected exposure"),
                );
            } else {
                crate::settlement::adjust_commitment(&path, &mut states, s.committed, desired);
            }
            s.authorized = retained_authorization;
            s.committed = desired;
            s.settled_live = live;
            s.drain_requested = false;
            return idle;
        }
    }
}
