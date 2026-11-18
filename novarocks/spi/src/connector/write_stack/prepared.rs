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

//! Shared validation and row-count facts for a distributed write's results.
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

/// Read-only membership in the query's registered writer cohort. A receipt
/// still needs its own creation identity and the query must separately
/// confirm complete receiving before publication.
pub trait ConnectorWriteProductionProof: Send + Sync {
    fn admits(
        &self,
        target: WriteTargetOrdinal,
        physical: &super::ConnectorWriterPhysicalContext,
    ) -> Result<bool, ConnectorError>;
}

/// Validate every fragment against the query's frozen target set.
///
/// A query can use a sparse subset of its session's dense target set. This
/// check borrows the original fragments so a refusal leaves caller custody
/// unchanged.
pub fn validate_write_fragment_targets(
    fragments: &[(WriteTargetOrdinal, ConnectorCommitFragment)],
    expected_targets: &[WriteTargetOrdinal],
) -> Result<(), ConnectorError> {
    crate::connector::write_stack::target::validate_query_target_ordinals(expected_targets)?;
    let expected = expected_targets
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if fragments
        .iter()
        .any(|(target, _)| !expected.contains(target))
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector commit fragment names a write target outside the sealed set",
        ));
    }
    Ok(())
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
}
