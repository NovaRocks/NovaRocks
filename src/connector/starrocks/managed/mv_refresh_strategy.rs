use crate::connector::iceberg::changes::{
    ChangeError, IcebergChangePolicySignal, policy_signal_from_change_error,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MvRefreshPolicy {
    NoOp {
        current_snapshot_id: i64,
    },
    FullRefresh {
        target_snapshot_id: Option<i64>,
        reason: FullRefreshReason,
    },
    Incremental {
        previous_snapshot_id: i64,
        current_snapshot_id: i64,
    },
    Unsupported {
        reason: UnsupportedRefreshReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum FullRefreshReason {
    InitialRefresh,
    InsertOverwrite {
        snapshot_id: i64,
    },
    LineageExpired {
        previous_snapshot_id: i64,
    },
    BaseTableRecreated {
        previous_uuid: String,
        current_uuid: String,
    },
    SchemaEvolutionSafeFallback {
        detail: String,
    },
}

impl std::fmt::Display for FullRefreshReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FullRefreshReason::InitialRefresh => write!(f, "initial refresh"),
            FullRefreshReason::InsertOverwrite { snapshot_id } => {
                write!(f, "insert overwrite snapshot {snapshot_id}")
            }
            FullRefreshReason::LineageExpired {
                previous_snapshot_id,
            } => write!(f, "lineage expired after snapshot {previous_snapshot_id}"),
            FullRefreshReason::BaseTableRecreated {
                previous_uuid,
                current_uuid,
            } => write!(
                f,
                "base table recreated (previous uuid {previous_uuid}, current uuid {current_uuid})"
            ),
            FullRefreshReason::SchemaEvolutionSafeFallback { detail } => {
                write!(f, "schema evolution safe fallback: {detail}")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum UnsupportedRefreshReason {
    SchemaEvolution { detail: String },
    ReplaceValidationFailed { snapshot_id: i64, reason: String },
    InternalInconsistency { detail: String },
}

impl std::fmt::Display for UnsupportedRefreshReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnsupportedRefreshReason::SchemaEvolution { detail } => {
                write!(f, "schema evolution unsupported: {detail}")
            }
            UnsupportedRefreshReason::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => write!(
                f,
                "replace snapshot {snapshot_id} failed validation: {reason}"
            ),
            UnsupportedRefreshReason::InternalInconsistency { detail } => {
                write!(f, "internal inconsistency: {detail}")
            }
        }
    }
}

pub(crate) fn choose_snapshot_refresh_policy(
    previous_snapshot_id: Option<i64>,
    current_snapshot_id: Option<i64>,
) -> Result<MvRefreshPolicy, String> {
    match (previous_snapshot_id, current_snapshot_id) {
        (None, current) => Ok(MvRefreshPolicy::FullRefresh {
            target_snapshot_id: current,
            reason: FullRefreshReason::InitialRefresh,
        }),
        (Some(previous), Some(current)) if previous == current => Ok(MvRefreshPolicy::NoOp {
            current_snapshot_id: current,
        }),
        (Some(previous), Some(current)) => Ok(MvRefreshPolicy::Incremental {
            previous_snapshot_id: previous,
            current_snapshot_id: current,
        }),
        (Some(previous), None) => Err(format!(
            "materialized view refresh cannot advance from snapshot {previous}: base table has no current snapshot"
        )),
    }
}

pub(crate) fn policy_from_change_error(err: ChangeError) -> MvRefreshPolicy {
    match (policy_signal_from_change_error(&err), err) {
        (
            IcebergChangePolicySignal::FullRefresh { .. },
            ChangeError::UnsupportedOperation { snapshot_id, op },
        ) if op == "overwrite" => MvRefreshPolicy::FullRefresh {
            target_snapshot_id: Some(snapshot_id),
            reason: FullRefreshReason::InsertOverwrite { snapshot_id },
        },
        (
            IcebergChangePolicySignal::FullRefresh { .. },
            ChangeError::LineageBroken { previous_snapshot },
        ) => MvRefreshPolicy::FullRefresh {
            target_snapshot_id: None,
            reason: FullRefreshReason::LineageExpired {
                previous_snapshot_id: previous_snapshot,
            },
        },
        (IcebergChangePolicySignal::FullRefresh { reason }, _) => MvRefreshPolicy::FullRefresh {
            target_snapshot_id: None,
            reason: FullRefreshReason::SchemaEvolutionSafeFallback { detail: reason },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::SchemaEvolutionUnsupported { detail },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::SchemaEvolution { detail },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::ReplaceValidationFailed {
                snapshot_id,
                reason,
            },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::UnsupportedOperation { snapshot_id, op },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: format!("unsupported iceberg snapshot operation `{op}` in {snapshot_id}"),
            },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::InternalInconsistency(detail),
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency { detail },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::PrimaryKeyMissingFromBase { pk_col }
            | ChangeError::PrimaryKeyNullable { pk_col }
            | ChangeError::PrimaryKeyTypeUnsupported { pk_col, .. },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: format!(
                    "CREATE-time primary key validation reached refresh path: {pk_col}"
                ),
            },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::PrimaryKeyValueNull { row_info },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: format!("primary key value became NULL during refresh: {row_info}"),
            },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::IcebergFormatUnsupported { format_version },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: format!(
                    "unsupported Iceberg format reached refresh path: {format_version}"
                ),
            },
        },
        (IcebergChangePolicySignal::Unsupported { reason }, _) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency { detail: reason },
        },
        (IcebergChangePolicySignal::Incremental, _) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: "incremental signal is invalid for change planning errors".to_string(),
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::connector::iceberg::changes::ChangeError;

    use super::*;

    #[test]
    fn policy_is_full_for_initial_refresh() {
        assert_eq!(
            choose_snapshot_refresh_policy(None, Some(10)).expect("policy"),
            MvRefreshPolicy::FullRefresh {
                target_snapshot_id: Some(10),
                reason: FullRefreshReason::InitialRefresh,
            }
        );
    }

    #[test]
    fn policy_is_noop_for_same_snapshot() {
        assert_eq!(
            choose_snapshot_refresh_policy(Some(10), Some(10)).expect("policy"),
            MvRefreshPolicy::NoOp {
                current_snapshot_id: 10,
            }
        );
    }

    #[test]
    fn policy_is_incremental_for_advanced_snapshot() {
        assert_eq!(
            choose_snapshot_refresh_policy(Some(10), Some(12)).expect("policy"),
            MvRefreshPolicy::Incremental {
                previous_snapshot_id: 10,
                current_snapshot_id: 12,
            }
        );
    }

    #[test]
    fn replace_validation_error_maps_to_full_refresh() {
        assert_eq!(
            policy_from_change_error(ChangeError::ReplaceValidationFailed {
                snapshot_id: 22,
                reason: "records changed".to_string(),
            }),
            MvRefreshPolicy::FullRefresh {
                target_snapshot_id: None,
                reason: FullRefreshReason::SchemaEvolutionSafeFallback {
                    detail: "replace snapshot is not a provably safe compaction: records changed"
                        .to_string(),
                },
            }
        );
    }

    #[test]
    fn unsupported_operation_maps_to_unsupported() {
        assert!(matches!(
            policy_from_change_error(ChangeError::UnsupportedOperation {
                snapshot_id: 22,
                op: "vendor-op".to_string(),
            }),
            MvRefreshPolicy::Unsupported { .. }
        ));
    }
}
