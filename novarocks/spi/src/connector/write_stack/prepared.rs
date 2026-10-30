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

//! The bounded, complete result of one distributed write's data plane.
//!
//! A prepared write set is complete or it does not exist. There is no partial
//! set, no truncation, no spill, and no "commit what arrived": the frontend may
//! call the provider's finish only with a set that the execution graph closed.
//!
//! Two forms of the same facts exist, and both use the ledger in this module:
//!
//! * while aggregating, the root backend holds canonical encoded bytes it never
//!   interprets;
//! * after the frontend decodes those bytes on its exact control binding, it
//!   holds [`ConnectorPreparedWriteSet`], whose fragments are provider values.

use crate::connector::write_stack::limits::{
    MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES,
    MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES,
};
use crate::connector::write_stack::runtime::ConnectorCommitFragment;
use crate::connector::write_stack::target::WriteTargetOrdinal;
use crate::connector::{ConnectorError, ConnectorErrorKind};

/// Running budget for one prepared write set.
///
/// Both a byte budget and an entry budget are enforced, because the byte budget
/// alone cannot see the fixed per-entry bookkeeping a large number of tiny
/// fragments would cost.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PreparedWriteSetLedger {
    bytes: usize,
    entries: usize,
}

impl PreparedWriteSetLedger {
    pub const fn new() -> Self {
        Self {
            bytes: 0,
            entries: 0,
        }
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    pub const fn entries(&self) -> usize {
        self.entries
    }

    /// Charge one canonical commit fragment. Reaching a bound is legal;
    /// exceeding it is `ResourceExhausted` and never a truncation.
    pub fn reserve_fragment(&mut self, fragment_bytes: usize) -> Result<(), ConnectorError> {
        use super::failure::WriteSetRefusal;
        use novarocks_failure_contract::ResourceDimension;
        if fragment_bytes > MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES {
            return Err(WriteSetRefusal::Fragment {
                requested: fragment_bytes,
            }
            .into());
        }
        let entries = self.entries.checked_add(1).ok_or_else(|| {
            ConnectorError::from(WriteSetRefusal::Arithmetic {
                dimension: ResourceDimension::Items,
                live: self.entries,
                requested: 1,
            })
        })?;
        if entries > MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES {
            return Err(WriteSetRefusal::Entries { live: self.entries }.into());
        }
        let bytes = self.bytes.checked_add(fragment_bytes).ok_or_else(|| {
            ConnectorError::from(WriteSetRefusal::Arithmetic {
                dimension: ResourceDimension::Bytes,
                live: self.bytes,
                requested: fragment_bytes,
            })
        })?;
        if bytes > MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES {
            return Err(WriteSetRefusal::Bytes {
                live: self.bytes,
                requested: fragment_bytes,
            }
            .into());
        }
        // All constraints accepted this one fragment. No refused request
        // mutates either dimension of the complete prepared-write set.
        self.bytes = bytes;
        self.entries = entries;
        Ok(())
    }
}

/// Checked accumulation of the rows a distributed write actually accepted.
///
/// Overflow is a contract error, never a saturating counter: a wrong affected
/// row count would be reported to the SQL client as truth.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteRowCountAccumulator(u64);

/// Exact change to the target's visible cardinality, independent of the
/// number of input rows its writers accepted. Replacing a row contributes
/// one insertion and one deletion, and therefore changes no cardinality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriteRowChange {
    Replace(u64),
    Delta { inserted: u64, deleted: u64 },
}

impl ConnectorWriteRowChange {
    /// Apply only to the logical count of the exact admitted target version.
    /// An unknown prior count stays unknown until a complete replacement.
    pub fn apply(self, previous: Option<u64>) -> Result<Option<u64>, ConnectorError> {
        match self {
            Self::Replace(rows) => Ok(Some(rows)),
            Self::Delta { inserted, deleted } => previous
                .map(|rows| {
                    // Subtract first so a cardinality-preserving replacement
                    // cannot overflow an otherwise representable final count.
                    rows.checked_sub(deleted)
                        .and_then(|rows| rows.checked_add(inserted))
                        .ok_or_else(|| ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            "prepared logical row change exceeds the admitted target cardinality",
                        ))
                })
                .transpose(),
        }
    }
}

impl WriteRowCountAccumulator {
    pub const fn new() -> Self {
        Self(0)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn add(&mut self, rows: u64) -> Result<(), ConnectorError> {
        self.0 = self.0.checked_add(rows).ok_or_else(|| {
            ConnectorError::from(super::failure::WriteCountFailure::Total {
                left: self.0,
                right: rows,
            })
        })?;
        Ok(())
    }
}

/// The complete, immutable set the frontend hands to a provider's finish.
#[derive(Debug)]
pub struct ConnectorPreparedWriteSet {
    row_count: u64,
    fragments: Vec<(WriteTargetOrdinal, ConnectorCommitFragment)>,
}

impl ConnectorPreparedWriteSet {
    /// Build a prepared set from an already-complete aggregation.
    ///
    /// `expected_targets` is the target set this set is allowed to name. A
    /// fragment naming anything outside it means the plan and the session
    /// disagree, so it fails closed rather than committing a partial or foreign
    /// artifact.
    ///
    /// The expected set is checked for cardinality and duplicates, not for
    /// denseness from zero. Denseness belongs to the *session's* sealed set
    /// (`ConnectorWriteSessionPlan::try_new`), and restating it here would
    /// refuse a legitimate single-query set: a copy-on-write statement drives
    /// one query per rewritten file, and query `k` names only target `k`.
    /// Membership is therefore an exact set test rather than a comparison
    /// against the highest ordinal.
    pub fn try_new(
        row_count: u64,
        mut fragments: Vec<(WriteTargetOrdinal, ConnectorCommitFragment)>,
        expected_targets: &[WriteTargetOrdinal],
    ) -> Result<Self, ConnectorError> {
        Self::try_new_in(row_count, &mut fragments, expected_targets)
    }

    /// Validate every original fragment before moving its unique inventory.
    /// A refusal leaves the caller's receiving slot unchanged.
    pub fn try_new_in(
        row_count: u64,
        fragments: &mut Vec<(WriteTargetOrdinal, ConnectorCommitFragment)>,
        expected_targets: &[WriteTargetOrdinal],
    ) -> Result<Self, ConnectorError> {
        crate::connector::write_stack::target::validate_query_target_ordinals(expected_targets)?;
        if fragments.len() > MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector prepared write set exceeds the frozen entry budget",
            ));
        }
        let expected = expected_targets
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        for (target, _) in fragments.iter() {
            if !expected.contains(target) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector commit fragment names a write target outside the sealed set",
                ));
            }
        }
        Ok(Self {
            row_count,
            fragments: std::mem::take(fragments),
        })
    }

    /// The checked total of rows every writer accepted. It becomes the SQL
    /// statement's affected row count only after external commit is known to
    /// have succeeded.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn fragments(&self) -> &[(WriteTargetOrdinal, ConnectorCommitFragment)] {
        &self.fragments
    }

    pub fn into_fragments(self) -> Vec<(WriteTargetOrdinal, ConnectorCommitFragment)> {
        self.fragments
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_cardinality_uses_net_effect_and_preserves_unknown() {
        use ConnectorWriteRowChange::{Delta, Replace};
        assert_eq!(
            Delta {
                inserted: 1,
                deleted: 0
            }
            .apply(Some(2))
            .unwrap(),
            Some(3)
        );
        assert_eq!(
            Delta {
                inserted: 0,
                deleted: 2
            }
            .apply(Some(3))
            .unwrap(),
            Some(1)
        );
        assert_eq!(
            Delta {
                inserted: 1,
                deleted: 1
            }
            .apply(Some(u64::MAX))
            .unwrap(),
            Some(u64::MAX)
        );
        assert_eq!(
            Delta {
                inserted: 0,
                deleted: 0
            }
            .apply(Some(3))
            .unwrap(),
            Some(3)
        );
        assert_eq!(
            Delta {
                inserted: 1,
                deleted: 0
            }
            .apply(None)
            .unwrap(),
            None
        );
        assert_eq!(Replace(0).apply(None).unwrap(), Some(0));
        assert!(
            Delta {
                inserted: 0,
                deleted: 2
            }
            .apply(Some(1))
            .is_err()
        );
        assert!(
            Delta {
                inserted: 1,
                deleted: 0
            }
            .apply(Some(u64::MAX))
            .is_err()
        );
    }
    use crate::connector::write_stack::runtime::{ConnectorWriteBinding, OpaqueWritePayload};
    use crate::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderId,
    };

    fn binding() -> ConnectorWriteBinding {
        let instance_id = ConnectorInstanceId::parse("prepared_unit").expect("instance id");
        ConnectorWriteBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
                instance_id: instance_id.clone(),
            },
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([3; 32])),
        )
    }

    /// `count` decoded fragments all naming `target`, as a completed
    /// aggregation would hand them to the frontend.
    fn fragments(
        target: WriteTargetOrdinal,
        count: usize,
    ) -> Vec<(WriteTargetOrdinal, ConnectorCommitFragment)> {
        let binding = binding();
        (0..count)
            .map(|_| {
                (
                    target,
                    ConnectorCommitFragment::from_parts(
                        binding.clone(),
                        OpaqueWritePayload::new(()),
                    ),
                )
            })
            .collect()
    }

    #[test]
    fn actual_ledger_refusals_preserve_dimension_known_values_and_atomic_state() {
        use novarocks_failure_contract::{FailureClass, FailureProjection, ResourceDimension};
        let mut fragment = PreparedWriteSetLedger::new();
        let over = MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1;
        let error = fragment.reserve_fragment(over).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().code(), 43);
        let resource = fact.resource().unwrap();
        assert_eq!(
            (
                resource.requested,
                resource.limit,
                resource.live,
                resource.committed
            ),
            (
                Some(over as u64),
                Some(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES as u64),
                None,
                None
            )
        );
        assert_eq!(fragment, PreparedWriteSetLedger::new());
        let mut entries = PreparedWriteSetLedger::new();
        for _ in 0..MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES {
            entries.reserve_fragment(0).unwrap();
        }
        let settled = entries;
        let error = entries.reserve_fragment(0).unwrap_err();
        assert_eq!(entries, settled);
        let resource = error.failure_fact().resource().unwrap();
        assert_eq!(resource.dimension, ResourceDimension::Items);
        assert_eq!(
            (resource.requested, resource.live, resource.limit),
            (
                Some(1),
                Some(MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES as u64),
                Some(MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES as u64)
            )
        );
        let mut bytes = PreparedWriteSetLedger::new();
        for _ in 0..MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES / MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES {
            bytes
                .reserve_fragment(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES)
                .unwrap();
        }
        let settled = bytes;
        let error = bytes.reserve_fragment(1).unwrap_err();
        assert_eq!(bytes, settled);
        let resource = error.failure_fact().resource().unwrap();
        assert_eq!(resource.dimension, ResourceDimension::Bytes);
        assert_eq!(
            (resource.requested, resource.live, resource.limit),
            (
                Some(1),
                Some(MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES as u64),
                Some(MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES as u64)
            )
        );
    }
    #[test]
    fn ledger_accepts_the_exact_bounds_and_rejects_one_more() {
        let mut ledger = PreparedWriteSetLedger::new();
        assert!(
            ledger
                .reserve_fragment(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES)
                .is_ok()
        );
        assert_eq!(ledger.bytes(), MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES);
        assert_eq!(ledger.entries(), 1);

        let mut over = PreparedWriteSetLedger::new();
        assert_eq!(
            over.reserve_fragment(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1)
                .expect_err("single fragment budget")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(over, PreparedWriteSetLedger::new());
    }

    #[test]
    fn ledger_rejects_the_set_byte_budget() {
        let mut ledger = PreparedWriteSetLedger::new();
        let full = MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES / MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES;
        for _ in 0..full {
            ledger
                .reserve_fragment(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES)
                .expect("within the set byte budget");
        }
        assert_eq!(ledger.bytes(), MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES);
        assert_eq!(
            ledger
                .reserve_fragment(1)
                .expect_err("set byte budget")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn ledger_rejects_the_entry_budget_before_the_byte_budget() {
        let mut ledger = PreparedWriteSetLedger::new();
        for _ in 0..MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES {
            ledger.reserve_fragment(0).expect("within the entry budget");
        }
        assert_eq!(ledger.entries(), MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES);
        assert_eq!(ledger.bytes(), 0);
        assert_eq!(
            ledger.reserve_fragment(0).expect_err("entry budget").kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn row_count_overflow_is_a_contract_error_not_a_saturating_counter() {
        let mut rows = WriteRowCountAccumulator::new();
        rows.add(u64::MAX).expect("first add");
        assert_eq!(
            rows.add(1).expect_err("overflow").kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(rows.get(), u64::MAX);
    }

    #[test]
    fn prepared_set_rejects_a_fragment_outside_the_sealed_target_set() {
        let targets = [WriteTargetOrdinal::try_new(0).expect("bounded")];
        assert!(ConnectorPreparedWriteSet::try_new(0, Vec::new(), &targets).is_ok());
        assert!(ConnectorPreparedWriteSet::try_new(0, Vec::new(), &[]).is_err());
    }

    /// One query of a copy-on-write statement names exactly its own target,
    /// which is not target zero. That set is a query fact, not a session one,
    /// so it must be accepted here.
    #[test]
    fn prepared_set_accepts_a_single_target_query_set_at_a_non_zero_ordinal() {
        let targets = [WriteTargetOrdinal::try_new(2).expect("bounded")];
        assert!(ConnectorPreparedWriteSet::try_new(0, Vec::new(), &targets).is_ok());
    }

    /// The ledger bounds entries while the root aggregates; the constructor
    /// bounds them again on the set the frontend is about to commit. The
    /// constructor's own boundary is therefore a separate fact from the
    /// ledger's, and the exact bound has to be a set that exists.
    #[test]
    fn prepared_set_accepts_the_exact_entry_budget_and_refuses_one_more() {
        let target = WriteTargetOrdinal::try_new(0).expect("bounded");
        let targets = [target];

        let exact = ConnectorPreparedWriteSet::try_new(
            0,
            fragments(target, MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES),
            &targets,
        )
        .expect("the exact entry budget is legal");
        assert_eq!(
            exact.fragments().len(),
            MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES
        );

        let error = ConnectorPreparedWriteSet::try_new(
            0,
            fragments(target, MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES + 1),
            &targets,
        )
        .expect_err("one entry over the budget");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    /// A copy-on-write statement drives several queries against one session and
    /// commits once. Each query's set is complete for its own execution graph,
    /// so a per-query charge would accept every one of them while the frontend
    /// held their sum. The budgets bound that sum.
    #[test]
    fn the_set_byte_budget_bounds_the_union_of_a_statements_queries() {
        let per_query = MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES / 2;
        let fragments_per_query = per_query / MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES;

        // Either query alone is inside the byte budget with room to spare.
        for _ in 0..2 {
            let mut alone = PreparedWriteSetLedger::new();
            for _ in 0..fragments_per_query {
                alone
                    .reserve_fragment(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES)
                    .expect("one query is well inside the byte budget");
            }
            assert_eq!(alone.bytes(), per_query);
        }

        // Charged on the union, the second query fills the budget exactly and
        // the next fragment is refused.
        let mut union = PreparedWriteSetLedger::new();
        for _ in 0..(2 * fragments_per_query) {
            union
                .reserve_fragment(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES)
                .expect("the union is still inside the byte budget");
        }
        assert_eq!(union.bytes(), MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES);
        let settled = union;
        let mut over = settled;
        assert_eq!(
            over.reserve_fragment(1)
                .expect_err("over the union byte budget")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        // A refused fragment charges nothing: the ledger still describes the
        // set that was actually accepted.
        assert_eq!(over, settled);
    }

    /// The entry budget is a union fact too, and it is reached by fragments
    /// that cost no bytes at all -- which is exactly why it exists beside the
    /// byte budget.
    #[test]
    fn the_set_entry_budget_bounds_the_union_of_a_statements_queries() {
        let per_query = MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES / 2;

        let mut alone = PreparedWriteSetLedger::new();
        for _ in 0..per_query {
            alone
                .reserve_fragment(0)
                .expect("one query is well inside the entry budget");
        }
        assert_eq!(alone.entries(), per_query);

        let mut union = PreparedWriteSetLedger::new();
        for _ in 0..(2 * per_query) {
            union
                .reserve_fragment(0)
                .expect("the union is still inside the entry budget");
        }
        assert_eq!(union.entries(), MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES);
        let settled = union;
        let mut over = settled;
        assert_eq!(
            over.reserve_fragment(0)
                .expect_err("over the union entry budget")
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(over, settled);
    }

    /// Refusing the set byte budget must not leave the ledger describing a
    /// partially charged set: the frontend commits what the ledger accepted, so
    /// a half-charged fragment would be a half-committed write.
    #[test]
    fn a_refused_fragment_leaves_the_set_ledger_exactly_where_it_was() {
        let mut ledger = PreparedWriteSetLedger::new();
        let full = MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES / MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES;
        for _ in 0..full {
            ledger
                .reserve_fragment(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES)
                .expect("within the set byte budget");
        }
        let settled = ledger;

        // Over the single-fragment budget, over the set byte budget, and over
        // both at once: none of them may move the ledger.
        for over in [
            MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1,
            1,
            MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES + 1,
        ] {
            let mut attempt = settled;
            assert_eq!(
                attempt
                    .reserve_fragment(over)
                    .expect_err("over a frozen budget")
                    .kind(),
                ConnectorErrorKind::ResourceExhausted
            );
            assert_eq!(attempt, settled);
        }
    }
}
