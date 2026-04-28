//! Errors and (in later PRs) data structures for iceberg snapshot-lineage
//! change planning under IVM Phase 2. This file is the home of the new
//! `plan_changes` entrypoint that PR-2 will introduce; PR-1 only lands the
//! error enum so that CREATE-time PRIMARY KEY validation has a stable type
//! to return.

/// All failure modes the iceberg change-planning and IVM CREATE/REFRESH
/// paths can surface. STRICT fail-fast: every variant is a hard rejection,
/// not a fallback signal. Variants not constructed in this PR are reserved
/// for PR-2 (`plan_changes` lineage walk) and PR-3/4 (runtime checks).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ChangeError {
    /// `previous_snapshot` referenced by stored MV state is no longer
    /// reachable from the current snapshot's parent chain (e.g. expired).
    LineageBroken { previous_snapshot: i64 },

    /// Snapshot operation is not understood or not in scope for this phase
    /// (e.g. `overwrite`, vendor-specific ops).
    UnsupportedOperation { snapshot_id: i64, op: String },

    /// Equality-delete file encountered; only position-deletes are in scope.
    EqualityDeleteUnsupported { snapshot_id: i64 },

    /// Iceberg v3 deletion-vector file encountered; out of scope.
    DeletionVectorUnsupported { snapshot_id: i64 },

    /// Schema evolution between `previous_snapshot` and `current_snapshot`
    /// (or any unsupported schema-related rejection at CREATE time).
    SchemaEvolutionUnsupported { detail: String },

    /// REPLACE snapshot failed the compaction-only sanity checks (records
    /// changed / schema-id changed / no added or no removed files).
    ReplaceValidationFailed { snapshot_id: i64, reason: String },

    /// CREATE-time: PRIMARY KEY column does not exist on the iceberg base
    /// table.
    PrimaryKeyMissingFromBase { pk_col: String },

    /// CREATE-time: PRIMARY KEY column is nullable on the base table.
    PrimaryKeyNullable { pk_col: String },

    /// CREATE-time: PRIMARY KEY column has a non-hashable scalar type.
    PrimaryKeyTypeUnsupported { pk_col: String, ty: String },

    /// Runtime: PRIMARY KEY column observed NULL in a base row at refresh
    /// time. Not constructed in PR-1.
    PrimaryKeyValueNull { row_info: String },

    /// CREATE-time: iceberg base table is not format-version 2.
    IcebergFormatUnsupported { format_version: i32 },

    /// Catch-all for invariant violations the codebase should never hit;
    /// constructing one is a bug, not a user error.
    InternalInconsistency(String),
}

impl std::fmt::Display for ChangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeError::LineageBroken { previous_snapshot } => write!(
                f,
                "iceberg lineage broken: previous snapshot {previous_snapshot} is unreachable from current snapshot"
            ),
            ChangeError::UnsupportedOperation { snapshot_id, op } => write!(
                f,
                "iceberg snapshot {snapshot_id} has unsupported operation `{op}`"
            ),
            ChangeError::EqualityDeleteUnsupported { snapshot_id } => write!(
                f,
                "iceberg snapshot {snapshot_id} contains equality-delete files; not supported in this phase"
            ),
            ChangeError::DeletionVectorUnsupported { snapshot_id } => write!(
                f,
                "iceberg snapshot {snapshot_id} contains v3 deletion-vector files; not supported in this phase"
            ),
            ChangeError::SchemaEvolutionUnsupported { detail } => write!(
                f,
                "iceberg schema evolution not supported: {detail}"
            ),
            ChangeError::ReplaceValidationFailed { snapshot_id, reason } => write!(
                f,
                "iceberg REPLACE snapshot {snapshot_id} failed compaction validation: {reason}"
            ),
            ChangeError::PrimaryKeyMissingFromBase { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` does not exist on the iceberg base table"
            ),
            ChangeError::PrimaryKeyNullable { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` must be NOT NULL on the iceberg base table"
            ),
            ChangeError::PrimaryKeyTypeUnsupported { pk_col, ty } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` has unsupported type `{ty}`; only hashable scalar types are allowed"
            ),
            ChangeError::PrimaryKeyValueNull { row_info } => write!(
                f,
                "PRIMARY KEY value is NULL in base row: {row_info}"
            ),
            ChangeError::IcebergFormatUnsupported { format_version } => write!(
                f,
                "iceberg base table format-version {format_version} is not supported; IVM Phase 2 requires v2"
            ),
            ChangeError::InternalInconsistency(detail) => {
                write!(f, "internal inconsistency: {detail}")
            }
        }
    }
}

impl std::error::Error for ChangeError {}

#[cfg(test)]
mod tests {
    use super::ChangeError;

    #[test]
    fn display_primary_key_missing() {
        let e = ChangeError::PrimaryKeyMissingFromBase {
            pk_col: "order_id".to_string(),
        };
        let s = format!("{e}");
        assert!(s.contains("order_id"), "{s}");
        assert!(s.to_lowercase().contains("primary key"), "{s}");
    }

    #[test]
    fn display_iceberg_format_unsupported() {
        let e = ChangeError::IcebergFormatUnsupported { format_version: 1 };
        let s = format!("{e}");
        assert!(s.contains("format-version 1"), "{s}");
        assert!(s.to_lowercase().contains("v2"), "{s}");
    }
}
