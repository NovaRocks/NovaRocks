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

//! Owner-level observation access, atomically published as a complete group.
// Design: ADR-0197 (docs/adr/ADR-0197-observation-owner-shards-and-coverage-shadow.md)
use super::{
    CoverageError, LANE_SHARDS, Lane, ProductionState, RecordRef, RecordRegion, RecordSnapshot,
    ResponsibilityClass,
};
use crate::{
    Account,
    account::Path,
    membership::MemberNode,
    sync::{Arc, Ordering},
};

pub const NORMAL_PRUNE_BUDGET: usize = 16;
/// One failed creation advances this many registry slots and draining records.
pub const OBSERVATION_CATCH_UP_BUDGET: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageGap {
    ObservationExhausted,
    AccountClosed,
}
#[derive(Clone, Debug)]
pub struct LaneShards(Arc<ShardsState>);
#[derive(Debug)]
enum ShardsState {
    Covered([Lane; LANE_SHARDS]),
    Uncovered(CoverageGap),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardSnapshot {
    pub reference: RecordRef,
    pub record: RecordSnapshot,
    pub production: ProductionState,
    pub class: ResponsibilityClass,
}
/// Independently sampled facts, not a simultaneous heap snapshot. Record
/// identities confer no access responsibility after their owner is released.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardsSnapshot {
    pub coverage: Result<(), CoverageGap>,
    pub records: [Option<ShardSnapshot>; LANE_SHARDS],
    pub tagged_bytes: i128,
    pub r1_small_bytes: i128,
    pub outstanding: i128,
    pub slot_pins: u64,
}
struct Prepared {
    owner: LaneShards,
    members: [Arc<MemberNode>; LANE_SHARDS],
}
impl Prepared {
    fn new(account: &Account) -> Result<Self, CoverageError> {
        let mut lanes = Vec::with_capacity(LANE_SHARDS);
        for _ in 0..LANE_SHARDS {
            lanes.push(Lane::new(account, RecordRegion::Observation)?);
        }
        let lanes: [Lane; LANE_SHARDS] = lanes.try_into().expect("complete shard group");
        let members = std::array::from_fn(|i| MemberNode::lane(&lanes[i].0, None));
        // The shell and every member are allocated before acquiring any gate.
        Ok(Self {
            owner: LaneShards(Arc::new(ShardsState::Covered(lanes))),
            members,
        })
    }
    fn publish(&self, account: &Account) {
        let ShardsState::Covered(lanes) = &*self.owner.0 else {
            unreachable!()
        };
        for (lane, member) in lanes.iter().zip(&self.members) {
            lane.publish_member(member.clone());
        }
        account.0.shared.retain_observations(lanes);
    }
}
impl Account {
    /// Observation never qualifies or consumes funding. A coverage gap remains
    /// an owner handle whose scope shadows unrelated ambient owners.
    pub fn create_lane_shards(&self) -> LaneShards {
        let shared = &self.0.shared;
        shared.prune_observations(NORMAL_PRUNE_BUDGET);
        let prepared = match Prepared::new(self) {
            Ok(prepared) => prepared,
            Err(_) => {
                // Unpublished records use the existing draining protocol. No
                // identity escaped, and cleanup runs outside lifecycle gates.
                shared.prune_observations(OBSERVATION_CATCH_UP_BUDGET);
                shared
                    .record_store
                    .store()
                    .reclaim(OBSERVATION_CATCH_UP_BUDGET);
                match Prepared::new(self) {
                    Ok(prepared) => prepared,
                    Err(_) => {
                        return LaneShards(Arc::new(ShardsState::Uncovered(
                            CoverageGap::ObservationExhausted,
                        )));
                    }
                }
            }
        };
        let path = Path::new(self);
        let gates = path.shared_gates();
        if path
            .nodes
            .iter()
            .flatten()
            .any(|node| node.0.closed.load(Ordering::Acquire) != 0)
        {
            drop(gates);
            drop(prepared);
            shared.record_store.store().faults.binding_failed();
            return LaneShards(Arc::new(ShardsState::Uncovered(CoverageGap::AccountClosed)));
        }
        // Linking and indexed registry replacement only clone prebuilt Arcs.
        prepared.publish(self);
        drop(gates);
        prepared.owner
    }
}
impl LaneShards {
    pub fn coverage(&self) -> Result<(), CoverageGap> {
        match &*self.0 {
            ShardsState::Covered(_) => Ok(()),
            ShardsState::Uncovered(gap) => Err(*gap),
        }
    }
    #[cfg(not(loom))]
    fn selected(&self) -> Option<&Lane> {
        match &*self.0 {
            ShardsState::Covered(lanes) => {
                Some(&lanes[crate::attribution::tls::current_shard() % LANE_SHARDS])
            }
            ShardsState::Uncovered(_) => None,
        }
    }
    /// Runs one synchronous step; unavailable coverage shadows the ambient.
    #[cfg(not(loom))]
    pub fn run<T>(&self, step: impl FnOnce() -> T) -> T {
        match self.selected() {
            Some(lane) => lane.run_or_shadow(step),
            None => crate::attribution::scope::run_shadowed(step),
        }
    }
    #[cfg(not(loom))]
    pub fn run_observed<T>(
        &self,
        observation: &mut crate::attribution::scope::OwnerStepObservation,
        step: impl FnOnce() -> T,
    ) -> T {
        match self.selected() {
            Some(lane) => lane.run_or_shadow_observed(observation, step),
            None => crate::attribution::scope::run_shadowed_observed(observation, step),
        }
    }
    #[cfg(not(loom))]
    pub fn explicit_owner(&self) -> crate::attribution::explicit::ExplicitOwner {
        match self.selected() {
            Some(lane) => crate::attribution::explicit::ExplicitOwner::new(lane.clone()),
            None => crate::attribution::explicit::ExplicitOwner::uncovered(),
        }
    }
    pub fn stop_producing(&self) -> Result<(), CoverageError> {
        let ShardsState::Covered(lanes) = &*self.0 else {
            return Ok(());
        };
        // Seal the complete group even when an earlier shard is still active.
        for lane in lanes {
            lane.seal();
        }
        if lanes.iter().any(|lane| lane.record().scope_active()) {
            return Err(CoverageError::ScopeRefused);
        }
        for lane in lanes {
            lane.stop_producing()?;
        }
        Ok(())
    }
    pub fn snapshot(&self) -> ShardsSnapshot {
        let mut snapshot = ShardsSnapshot {
            coverage: self.coverage(),
            records: [None; LANE_SHARDS],
            tagged_bytes: 0,
            r1_small_bytes: 0,
            outstanding: 0,
            slot_pins: 0,
        };
        if let ShardsState::Covered(lanes) = &*self.0 {
            for (output, lane) in snapshot.records.iter_mut().zip(lanes) {
                let record = lane.record().snapshot();
                snapshot.tagged_bytes += i128::from(record.tagged_bytes);
                snapshot.r1_small_bytes += i128::from(record.r1_small_bytes);
                snapshot.outstanding += i128::from(record.outstanding);
                snapshot.slot_pins += u64::from(record.slot_pins);
                *output = Some(ShardSnapshot {
                    reference: lane.reference(),
                    record,
                    production: lane.production_state(),
                    class: lane.responsibility_class(),
                });
            }
        }
        snapshot
    }
}
