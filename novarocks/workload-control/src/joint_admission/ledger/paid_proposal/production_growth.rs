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

//! Every existing production responsibility follows its original control D
//! in the same paid request. No raw view or independent grant is exposed.
use super::*;

pub(in crate::joint_admission::ledger) fn production_change_count(
    state: &State,
    works: &[WorkId],
    base: usize,
) -> Result<usize, WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    ledger
        .claims
        .values()
        .filter(|row| {
            matches!(row.owner, ClaimOwner::PhysicalWork(work) if works.contains(&work))
                && matches!(row.custody, ClaimCustody::Production { .. })
        })
        .try_fold(base, |n, _| {
            n.checked_add(1).ok_or(WorkError::ArithmeticOverflow)
        })
}
impl WorkScope {
    /// Pure current-inventory fact, not a reservation or a production profile.
    /// The final queue cut rechecks membership; concurrent registration may
    /// require a fresh proposal. Never infer a configured maxL from this alone.
    pub fn control_binding_change_records(&self) -> Result<usize, WorkError> {
        let state = self.inner.state.lock().unwrap();
        validate_active_control_node(&state, self)?;
        production_change_count(&state, &[self.id], 1)
    }
}
impl JointProposal {
    pub(super) fn append_control_productions(&mut self) -> Result<(), WorkError> {
        let base = self.records.len();
        for index in 0..base {
            let change = &self.records[index];
            if root_domains::observation(change) {
                continue;
            }
            let Some(control) = change.source.as_ref() else {
                continue;
            };
            let ClaimSource::NodeControl { work, .. } = control.source else {
                continue;
            };
            let full = Arc::clone(&change.claim.facts);
            let mut after = std::ops::Bound::Unbounded;
            loop {
                // Capture only original scalar fences and existing Arc facts
                // under State. All allocation/paid body writes occur outside.
                let next = {
                    let state = self.scope.inner.state.lock().unwrap();
                    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
                    ledger
                        .claims
                        .range((after, std::ops::Bound::Unbounded))
                        .find_map(|(&id, row)| {
                            let ClaimCustody::Production { epoch } = row.custody else {
                                return None;
                            };
                            (row.owner == ClaimOwner::PhysicalWork(work)).then(|| {
                                (
                                    id,
                                    epoch,
                                    row.pending,
                                    row.structure_revision,
                                    row.release_revision,
                                    Arc::clone(&row.active_facts),
                                    row.active_amounts,
                                    row.amounts == row.active_amounts
                                        && row.facts.key == row.active_facts.key
                                        && row.production.is_some(),
                                )
                            })
                        })
                };
                let Some((id, epoch, pending, structure, release, original, amounts, whole)) = next
                else {
                    break;
                };
                after = std::ops::Bound::Excluded(id);
                if pending.is_some()
                    || !whole
                    || original.class != full.class
                    || original.lane != JointLane::Custody
                    || full.lane != JointLane::Custody
                    || !original.key.source_kind.is_empty()
                    || !full.key.source_kind.is_empty()
                    || original.dependencies.iter().any(|old| {
                        full.dependencies
                            .binary_search_by_key(&old.ordinal, |d| d.ordinal)
                            .is_err()
                    })
                    || self.records.iter().any(|change| change.old == Some(id))
                {
                    return Err(WorkError::Conflict);
                }
                structure
                    .checked_add(2)
                    .ok_or(WorkError::ArithmeticOverflow)?;
                let source = JointClaimView {
                    inner: Arc::downgrade(&self.scope.inner),
                    id,
                    source: ClaimSource::Production { work, epoch },
                    structure_revision: structure,
                    release_revision: release,
                };
                // Full class/lane/D facts are exactly shared with this control
                // change, without another Vec/Arc domain allocation.
                self.records.push(ChangeRecord {
                    old: Some(id),
                    original_amounts: Some(amounts),
                    original_facts: Some(original),
                    source: Some(source),
                    claim: ProposedClaim {
                        facts: Arc::clone(&full),
                        amounts,
                    },
                });
                self.started += 1;
            }
        }
        Ok(())
    }
}

/// Recheck exact membership, origin work and typed incarnation at queue,
/// proof and installation. No new registration can pass a pending control.
pub(in crate::joint_admission::ledger) fn validate_control_productions(
    state: &State,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    for control in changes.iter().filter(|change| {
        change
            .source
            .as_ref()
            .is_some_and(|source| matches!(source.source, ClaimSource::NodeControl { .. }))
    }) {
        if root_domains::observation(control) {
            continue;
        }
        let ClaimSource::NodeControl { work, .. } = control.source.as_ref().unwrap().source else {
            unreachable!()
        };
        // The internal author appends this work's sources in original ID order.
        // Merge the two finite inventories instead of searching L for every C.
        let mut expected = changes.iter().filter(|change| change.source.as_ref().is_some_and(|source| {
            matches!(source.source, ClaimSource::Production { work: original, .. } if original == work)
        }));
        for (&id, row) in &ledger.claims {
            let ClaimCustody::Production { epoch } = row.custody else {
                continue;
            };
            if row.owner != ClaimOwner::PhysicalWork(work) {
                continue;
            }
            let change = expected.next().ok_or(WorkError::Conflict)?;
            let source = change.source.as_ref().unwrap();
            if source.id != id
                || !matches!(source.source, ClaimSource::Production { epoch: original, .. } if original == epoch)
                || change.old != Some(id)
                || row.production.is_none()
                || change.original_amounts != Some(row.active_amounts)
                || change.claim.amounts != row.active_amounts
                || change
                    .original_facts
                    .as_ref()
                    .is_none_or(|old| old.key != row.active_facts.key)
                || change.claim.facts.key != control.claim.facts.key
                || !change.claim.facts.key.source_kind.is_empty()
            {
                return Err(WorkError::Conflict);
            }
        }
        if expected.next().is_some() {
            return Err(WorkError::Conflict);
        }
    }
    for change in changes {
        let Some(source) = &change.source else {
            continue;
        };
        let ClaimSource::Production { work, epoch } = source.source else {
            continue;
        };
        let row = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
        if !matches!(row.custody, ClaimCustody::Production { epoch: original } if epoch == original)
            || row.owner != ClaimOwner::PhysicalWork(work) || row.production.is_none()
            || !changes.iter().any(|control| control.source.as_ref().is_some_and(|source| matches!(source.source, ClaimSource::NodeControl { work: original, .. } if original == work)))
        { return Err(WorkError::Conflict); }
    }
    Ok(())
}
