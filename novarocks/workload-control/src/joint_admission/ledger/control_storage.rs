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

//! Fixed storage geometry and original control-claim transitions.

use super::*;
use crate::control_storage::{ControlSlot, ControlStorageConfig};

/// The actual storage envelope used by configuration. This deliberately covers
/// fixed joint metadata service, not caller-built vectors, task/Waker graphs,
/// Provider URL input parsing or complete roles. No production activation.
pub(crate) fn control_storage_base(
    config: ControlStorageConfig<'_>,
) -> Result<JointResources, WorkError> {
    use crate::control_storage::{btree_peak_bytes, resources_for_bytes, storage_cell_bytes};
    let count = config.count()?;
    if count == 0 {
        return Err(WorkError::InvalidConfig("fixed control storage geometry"));
    }
    let mut bytes = storage_cell_bytes()?
        .checked_mul(count)
        .and_then(|n| n.checked_add(std::mem::size_of::<CancellationStorage>()))
        .ok_or(WorkError::ArithmeticOverflow)?;
    bytes = bytes
        .checked_add(crate::cancellation::shared_child_index_backing_bytes(
            count,
        )?)
        .and_then(|n| n.checked_add(std::mem::size_of::<JointLedger>()))
        .ok_or(WorkError::ArithmeticOverflow)?;
    // Five immutable identity labels, not five resource reservations.
    let empty_facts = crate::control_storage::arc_bytes::<ResponsibilityFacts>()?
        .checked_add(crate::control_storage::arc_bytes::<FactKey>()?)
        .and_then(|n| {
            n.checked_add(crate::control_storage::arc_bytes::<Vec<DependencyRef>>().ok()?)
        })
        .and_then(|n| n.checked_mul(5))
        .ok_or(WorkError::ArithmeticOverflow)?;
    bytes = bytes
        .checked_add(empty_facts)
        .and_then(|n| {
            n.checked_add(crate::control_storage::arc_bytes::<ResponsibilityConfig>().ok()?)
        })
        .ok_or(WorkError::ArithmeticOverflow)?;
    for role in config.poll_roles {
        bytes = bytes
            .checked_add(role.bytes(count)?)
            .ok_or(WorkError::ArithmeticOverflow)?;
    }
    bytes = bytes
        .checked_add(config.process_sources.bytes(count)?)
        .ok_or(WorkError::ArithmeticOverflow)?;
    for backing in [
        btree_peak_bytes::<WorkId, crate::scope::Node>(count)?,
        btree_peak_bytes::<u64, ClaimRecord>(count)?,
    ] {
        bytes = bytes
            .checked_add(backing)
            .ok_or(WorkError::ArithmeticOverflow)?;
    }
    let per_slot = bytes
        .checked_add(count - 1)
        .ok_or(WorkError::ArithmeticOverflow)?
        / count;
    resources_for_bytes(per_slot)
}

impl WorkloadControl {
    /// Prepay and install fixed local containers in this original authority.
    /// This validates storage and ledger limits only; the application owner
    /// must install its required services and complete profile before Ready.
    pub fn configure_control_storage(
        &self,
        config: ControlStorageConfig<'_>,
    ) -> Result<(), WorkError> {
        let count = config.count()?;
        let base = control_storage_base(config)?;
        if count != self.inner.config.scope_records_limit {
            return Err(WorkError::InvalidConfig("fixed control scope inventory"));
        }
        self.inner.update(|state| {
            if state.ready
                || state.root_admission_is_closed()
                || !state.nodes.is_empty()
                || !state.cancellations.slots.is_empty()
            {
                return Err(WorkError::Conflict);
            }
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if ledger.driver_closed || !ledger.claims.is_empty() || !ledger.requests.is_empty() {
                return Err(WorkError::Conflict);
            }
            state
                .next_id
                .checked_add(u64::try_from(count).map_err(|_| WorkError::ArithmeticOverflow)?)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let dependencies = DependencyRegistry::new();
            let shared_children = crate::cancellation::new_shared_child_index();
            let mut slots = Vec::new();
            slots
                .try_reserve_exact(count)
                .map_err(|_| WorkError::Capacity("fixed control inventory"))?;
            let mut claims = BTreeMap::new();
            let mut empty_facts = std::array::from_fn(|_| None);
            let classes = [
                WorkClass::Query,
                WorkClass::MaterializedView,
                WorkClass::Statistics,
                WorkClass::TableMaintenance,
                WorkClass::Management,
            ];
            for (class, _) in classes.iter().enumerate() {
                let facts = Arc::new(ResponsibilityFacts {
                    key: Arc::new(FactKey {
                        origin: GroupOrigin::Frozen,
                        class,
                        lane: JointLane::Custody as usize,
                        dependencies: Vec::new(),
                        source_kind: SourceKind::Plain,
                    }),
                    class: classes[class],
                    lane: JointLane::Custody,
                    dependencies: Arc::new(Vec::<DependencyRef>::new()),
                });
                empty_facts[class] = Some(Arc::clone(&facts));
            }
            let facts = Arc::clone(empty_facts[0].as_ref().unwrap());
            for offset in 0..count {
                let cell = crate::cancellation::Cancellation::preallocated_fixed(
                    Arc::clone(&shared_children),
                    slots.len(),
                    count - 1,
                );
                let claim = state.next_id()?;
                let id = SlotId(slots.len());
                let mut polls = Vec::new();
                polls
                    .try_reserve_exact(config.poll_roles.len())
                    .map_err(|_| WorkError::Capacity("fixed poll backing inventory"))?;
                for &role in config.poll_roles {
                    polls.push(crate::role_poll::PollBacking::new(
                        role,
                        SlotId(slots.len()),
                    ));
                }
                slots.push(ControlSlot {
                    retired_root_domains: None,
                    cell: Some(cell),
                    polls,
                    class: WorkClass::Query,
                    phase: SlotPhase::Free,
                    generation: 0,
                    claim: Some(claim),
                    base,
                    parent: None,
                    next_free: (offset + 1 < count).then_some(SlotId(id.0 + 1)),
                });
                claims.insert(
                    claim,
                    ClaimRecord {
                        internal_position: false,
                        computation_window: None,
                        domain_birth: None,
                        native_dispatch: None,
                        dispatch_staging: None,
                        custody: ClaimCustody::Raw,
                        production: None,
                        owner: ClaimOwner::ControlSlot(id),
                        facts: Arc::clone(&facts),
                        amounts: base,
                        active_facts: Arc::clone(&facts),
                        active_amounts: base,
                        pending: None,
                        structure_revision: 0,
                        release_revision: 0,
                    },
                );
            }
            let ledger = state.joint.as_mut().unwrap();
            ledger.dependencies = Some(dependencies);
            ledger.claims = claims;
            ledger.control_empty_facts = empty_facts;
            state.cancellations = CancellationStorage {
                slots,
                fixed: true,
                retired: 0,
                retired_polls: 0,
                free_head: (count != 0).then_some(SlotId(0)),
                legacy_vacant: None,
            };
            Ok(())
        })
    }
}

impl JointLedger {
    pub(crate) fn control_is_empty(&self, claim: u64) -> bool {
        self.claims.get(&claim).is_some_and(|record| {
            record.facts.key.origin == GroupOrigin::Frozen
                && record.active_facts.key.origin == GroupOrigin::Frozen
                && record.facts.dependencies.is_empty()
                && record.active_facts.dependencies.is_empty()
                && record.pending.is_none()
        })
    }

    pub(crate) fn activate_control_slot(
        &mut self,
        slot: SlotId,
        claim: u64,
        class: WorkClass,
    ) -> Result<(), WorkError> {
        let record = self.claims.get_mut(&claim).ok_or(WorkError::Released)?;
        if self.driver_closed
            || record.owner != ClaimOwner::ControlSlot(slot)
            || record.pending.is_some()
        {
            return Err(WorkError::Conflict);
        }
        let facts = Arc::clone(
            self.control_empty_facts[class_index(class)]
                .as_ref()
                .unwrap(),
        );
        let revision = record
            .structure_revision
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        record.facts = Arc::clone(&facts);
        record.active_facts = facts;
        record.structure_revision = revision;
        Ok(())
    }

    pub(crate) fn reset_control_slot(&mut self, slot: SlotId, claim: u64, base: JointResources) {
        let record = self.claims.get_mut(&claim).expect("original fixed claim");
        assert_eq!(record.owner, ClaimOwner::ControlSlot(slot));
        assert!(
            record.pending.is_none(),
            "collected control slot has no pending reply"
        );
        let from = Arc::clone(&record.facts);
        let to = Arc::clone(
            self.control_empty_facts[class_index(record.facts.class)]
                .as_ref()
                .unwrap(),
        );
        record.facts = Arc::clone(&to);
        record.active_facts = Arc::clone(&to);
        record.amounts = base;
        record.active_amounts = base;
        match record.structure_revision.checked_add(1) {
            Some(revision) => record.structure_revision = revision,
            None => self.driver_closed = true,
        }
        if from.key != to.key {
            self.layout_changed();
        }
        self.released();
    }
}

#[cfg(test)]
impl JointLedger {
    pub(crate) fn control_snapshot_for_test(&self) -> (usize, usize, JointResources, usize) {
        let mut amount = JointResources::ZERO;
        let mut records = 0;
        let mut bound = 0;
        for record in self.claims.values() {
            if matches!(record.owner, ClaimOwner::ControlSlot(_)) {
                amount = amount.checked_add(record.amounts).unwrap();
                records += 1;
                bound += usize::from(!record.facts.dependencies.is_empty());
            }
        }
        (records, self.claims.len(), amount, bound)
    }

    pub(crate) fn control_has_domains_for_test(
        &self,
        claim: u64,
        expected: &[SharedDependency],
    ) -> bool {
        let record = &self.claims[&claim];
        [&record.facts, &record.active_facts]
            .into_iter()
            .all(|facts| {
                facts.dependencies.len() == expected.len()
                    && expected.iter().all(|fact| {
                        facts
                            .dependencies
                            .iter()
                            .any(|dependency| &dependency.fact == fact)
                    })
            })
    }

    pub(crate) fn control_revision_for_test(&self, claim: u64) -> u64 {
        self.claims[&claim].structure_revision
    }

    /// Establish an existing domain-bound fixture without pretending that this
    /// test setup has driven the separate FirstBind protocol.
    pub(crate) fn bind_control_fixture(&mut self, slot: SlotId, claim: u64, ordinal: u64) {
        let record = &self.claims[&claim];
        assert_eq!(record.owner, ClaimOwner::ControlSlot(slot));
        assert!(record.facts.dependencies.is_empty());
        let from = Arc::clone(&record.facts);
        let registry = self.dependencies.as_mut().unwrap();
        let dependency = registry
            .lookup(&SharedDependency::LocalHost)
            .unwrap_or_else(|| {
                registry
                    .insert(SharedDependency::LocalHost, ordinal)
                    .unwrap()
            });
        let ordinal = dependency.ordinal;
        let to = Arc::new(ResponsibilityFacts {
            key: Arc::new(FactKey {
                origin: GroupOrigin::Frozen,
                class: class_index(from.class),
                lane: from.lane as usize,
                dependencies: vec![ordinal],
                source_kind: SourceKind::Plain,
            }),
            class: from.class,
            lane: from.lane,
            dependencies: Arc::new(vec![dependency]),
        });
        let record = self.claims.get_mut(&claim).unwrap();
        record.facts = Arc::clone(&to);
        record.active_facts = to;
        record.structure_revision += 1;
        self.layout_changed();
    }
}

#[cfg(test)]
mod canonical_fixture_tests {
    use super::*;

    #[test]
    fn fixture_reuses_the_actual_canonical_ordinal_in_its_profile_key() {
        let (control, handle, mut driver) =
            crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 1, 1);
        let canonical = handle
            .register_dependency(SharedDependency::LocalHost)
            .unwrap();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        {
            let mut state = control.inner.state.lock().unwrap();
            let slot = state.nodes[&work.owner.scope().id()].cancellation;
            let claim = state.cancellations.slots[slot.0].claim.unwrap();
            let later = state.next_id().unwrap();
            assert_ne!(canonical.record.ordinal, later);
            let ledger = state.joint.as_mut().unwrap();
            ledger.bind_control_fixture(slot, claim, later);
            let facts = &ledger.claims[&claim].facts;
            assert_eq!(facts.key.dependencies, vec![canonical.record.ordinal]);
            assert_eq!(facts.dependencies[0].ordinal, facts.key.dependencies[0]);
        }
        drop(canonical);
        work.business.release();
        work.owner.complete();
        for _ in 0..1000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                return;
            }
        }
        panic!("actual fixture graph exit must reach idle");
    }
}
