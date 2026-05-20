use std::collections::BTreeSet;

use crate::engine::mv::partition::MvPartitionKey;

/// Set of MV target partitions affected by a signed aggregate delta batch.
///
/// `Unpartitioned` is the legitimate state for non-partitioned MV targets;
/// callers MUST NOT treat it as "no information available". A failed
/// derivation surfaces an [`AffectedPartitionError`] instead — the design
/// is strict fail-fast, no silent fallback (see spec §5 principle 2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedAggregateTargetPartitions {
    Unpartitioned,
    Known { partitions: BTreeSet<MvPartitionKey> },
}

impl AffectedAggregateTargetPartitions {
    pub(crate) fn known<I: IntoIterator<Item = MvPartitionKey>>(partitions: I) -> Self {
        Self::Known {
            partitions: partitions.into_iter().collect(),
        }
    }

    pub(crate) fn partitions(&self) -> Option<&BTreeSet<MvPartitionKey>> {
        match self {
            Self::Unpartitioned => None,
            Self::Known { partitions } => Some(partitions),
        }
    }
}

/// Reasons aggregate-delta partition derivation can refuse a delta batch.
/// Every variant carries enough context for the refresh error message to
/// name the failing field and / or value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedPartitionError {
    /// The schema contract has no `target.partition` but the caller expected
    /// a partitioned MV. Only raised when the layout reports group-key columns
    /// but the contract is unpartitioned (callers should treat `partition =
    /// None` as Unpartitioned, not as an error — this variant is reserved
    /// for drift between layout and contract).
    ContractMissing(String),
    /// Transform listed in the contract has no first-class derivation rule.
    TransformUnsupported { field: String, transform: String },
    /// Output column referenced by the partition field is not a pure column
    /// expression, OR resolves to a non-group-key column in the layout.
    OutputLineageNotPureColumn { field: String },
    /// Partition field references a target visible column that does not
    /// exist in the contract, or whose backing visible column is missing
    /// from the layout / from the delta chunk schema.
    GroupKeyColumnMissing { field: String, reason: String },
    /// Group-key column in the delta chunk has an Arrow type that the
    /// Iceberg transform function refuses.
    GroupKeyTypeMismatch {
        field: String,
        want: String,
        got: String,
    },
    /// `iceberg::transform::create_transform_function(...).transform(array)`
    /// itself returned an error.
    TransformFailed { field: String, source: String },
}

impl std::fmt::Display for AffectedPartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ContractMissing(reason) => write!(
                f,
                "aggregate target partition contract missing or inconsistent: {reason}"
            ),
            Self::TransformUnsupported { field, transform } => write!(
                f,
                "aggregate target partition field {field} uses unsupported transform {transform}"
            ),
            Self::OutputLineageNotPureColumn { field } => write!(
                f,
                "aggregate target partition field {field} requires row-evaluation fallback"
            ),
            Self::GroupKeyColumnMissing { field, reason } => {
                write!(f, "aggregate target partition field {field}: {reason}")
            }
            Self::GroupKeyTypeMismatch { field, want, got } => write!(
                f,
                "aggregate target partition field {field} delta column type mismatch: want {want}, got {got}"
            ),
            Self::TransformFailed { field, source } => write!(
                f,
                "aggregate target partition field {field} transform failed: {source}"
            ),
        }
    }
}

impl std::error::Error for AffectedPartitionError {}

/// Inputs required to derive the affected target partitions from an aggregate
/// delta batch. Populated by Tasks 2–3; this stub preserves the public
/// surface referenced by `mod.rs` re-exports until the full implementation
/// lands.
pub(crate) struct AggregateDeltaPartitionInput {
    // Fields added in Task 2.
    _priv: (),
}

/// Derive the set of target partitions touched by a signed aggregate delta
/// batch given the MV schema contract and the aggregate layout.
/// Implementation added in Task 2–4; the stub exists so that `mod.rs` can
/// re-export the symbol without breaking compilation.
pub(crate) fn derive_from_aggregate_delta(
    _input: &AggregateDeltaPartitionInput,
) -> Result<AffectedAggregateTargetPartitions, AffectedPartitionError> {
    unimplemented!("derive_from_aggregate_delta: implementation added in Task 2")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::mv::partition::{
        MvPartitionKey, MvPartitionKeyField, MvPartitionValue,
    };

    fn sample_key(value: &str) -> MvPartitionKey {
        MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn affected_aggregate_target_partitions_known_dedupes_and_sorts() {
        let result = AffectedAggregateTargetPartitions::known(
            [sample_key("b"), sample_key("a"), sample_key("a")],
        );
        let AffectedAggregateTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        assert_eq!(
            partitions.into_iter().collect::<Vec<_>>(),
            vec![sample_key("a"), sample_key("b")]
        );
    }

    #[test]
    fn affected_partition_error_display_includes_field_and_reason() {
        let err = AffectedPartitionError::TransformUnsupported {
            field: "region".to_string(),
            transform: "void".to_string(),
        };
        let message = format!("{err}");
        assert!(message.contains("region"), "{message}");
        assert!(message.contains("void"), "{message}");
    }

    #[test]
    fn affected_aggregate_target_partitions_unpartitioned_has_no_partitions() {
        let result = AffectedAggregateTargetPartitions::Unpartitioned;
        assert!(result.partitions().is_none());
    }
}
