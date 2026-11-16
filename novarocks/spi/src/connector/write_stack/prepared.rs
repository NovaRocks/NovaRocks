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
use crate::connector::write_stack::runtime::ConnectorCommitFragment;
use crate::connector::write_stack::target::WriteTargetOrdinal;
use crate::connector::{ConnectorError, ConnectorErrorKind};

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

/// Read-only membership in the original writer cohort after receiving its
/// complete result. A receipt still has to prove its own creation identity.
pub trait ConnectorWriteProductionProof: Send + Sync {
    fn admits(
        &self,
        target: WriteTargetOrdinal,
        physical: &super::ConnectorWriterPhysicalContext,
    ) -> Result<bool, ConnectorError>;
}

/// The complete, immutable set the frontend hands to a provider's finish.
pub struct ConnectorPreparedWriteSet {
    row_count: u64,
    fragments: Vec<(WriteTargetOrdinal, ConnectorCommitFragment)>,
    production: Option<std::sync::Arc<dyn ConnectorWriteProductionProof>>,
}

impl std::fmt::Debug for ConnectorPreparedWriteSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorPreparedWriteSet")
            .field("row_count", &self.row_count)
            .field("fragments", &self.fragments)
            .field("has_production", &self.production.is_some())
            .finish()
    }
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
    /// refuse a legitimate single-query set when query `k` names only target `k`.
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
            production: None,
        })
    }

    /// The checked total of rows every writer accepted. It becomes the SQL
    /// statement's affected row count only after external commit is known to
    /// have succeeded.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn with_received_production(
        mut self,
        proof: std::sync::Arc<dyn ConnectorWriteProductionProof>,
    ) -> Self {
        self.production = Some(proof);
        self
    }

    pub fn admits_creation(
        &self,
        target: WriteTargetOrdinal,
        physical: &super::ConnectorWriterPhysicalContext,
    ) -> Result<bool, ConnectorError> {
        match &self.production {
            Some(production) => production.admits(target, physical),
            None => Ok(false),
        }
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

    /// One query within a sealed session names exactly its own target,
    /// which is not target zero. That set is a query fact, not a session one,
    /// so it must be accepted here.
    #[test]
    fn prepared_set_accepts_a_single_target_query_set_at_a_non_zero_ordinal() {
        let targets = [WriteTargetOrdinal::try_new(2).expect("bounded")];
        assert!(ConnectorPreparedWriteSet::try_new(0, Vec::new(), &targets).is_ok());
    }

    #[test]
    fn prepared_set_accepts_more_than_16384_fragments_for_its_exact_target() {
        let target = WriteTargetOrdinal::try_new(0).expect("bounded");
        let count = 16_385;
        let prepared = ConnectorPreparedWriteSet::try_new(0, fragments(target, count), &[target])
            .expect("complete set for the expected target");
        assert_eq!(prepared.fragments().len(), count);
    }
}
