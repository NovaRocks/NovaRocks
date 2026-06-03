use crate::engine::StatementResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BaseSnapshotPolicy {
    SingleBase,
    AllBasesRequired,
    JoinPairPartialInitialSkip,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BaseSnapshotStatus {
    pub(crate) fqn: String,
    pub(crate) previous_snapshot_id: Option<i64>,
    pub(crate) current_snapshot_id_before_pin: Option<i64>,
}

impl BaseSnapshotStatus {
    pub(crate) fn new(
        fqn: impl Into<String>,
        previous_snapshot_id: Option<i64>,
        current_snapshot_id_before_pin: Option<i64>,
    ) -> Self {
        Self {
            fqn: fqn.into(),
            previous_snapshot_id,
            current_snapshot_id_before_pin,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RefreshDecision {
    SkipEmpty,
    FirstRefresh,
    MetadataOnly,
    Incremental,
    FailFast { reason: String },
}

pub(crate) struct IcebergMvRefreshLifecycle;

impl IcebergMvRefreshLifecycle {
    pub(crate) fn run(
        decision: RefreshDecision,
        first_refresh: impl FnOnce() -> Result<StatementResult, String>,
        metadata_only: impl FnOnce() -> Result<StatementResult, String>,
        incremental: impl FnOnce() -> Result<StatementResult, String>,
    ) -> Result<StatementResult, String> {
        match decision {
            RefreshDecision::SkipEmpty => Ok(StatementResult::Ok),
            RefreshDecision::FirstRefresh => first_refresh(),
            RefreshDecision::MetadataOnly => metadata_only(),
            RefreshDecision::Incremental => incremental(),
            RefreshDecision::FailFast { reason } => Err(reason),
        }
    }
}

pub(crate) fn decide_refresh(
    policy: BaseSnapshotPolicy,
    statuses: &[BaseSnapshotStatus],
    label: &str,
) -> RefreshDecision {
    match policy {
        BaseSnapshotPolicy::SingleBase => decide_single_base_refresh(statuses, label),
        BaseSnapshotPolicy::AllBasesRequired => decide_all_bases_required_refresh(statuses, label),
        BaseSnapshotPolicy::JoinPairPartialInitialSkip => {
            decide_join_pair_partial_initial_skip_refresh(statuses, label)
        }
    }
}

fn decide_single_base_refresh(statuses: &[BaseSnapshotStatus], label: &str) -> RefreshDecision {
    let [status] = statuses else {
        return fail_fast(format!(
            "{label} refresh requires exactly one base snapshot status, got {}",
            statuses.len()
        ));
    };

    match (
        status.previous_snapshot_id,
        status.current_snapshot_id_before_pin,
    ) {
        (None, None) => RefreshDecision::SkipEmpty,
        (None, Some(_)) => RefreshDecision::FirstRefresh,
        (Some(_), None) => fail_fast(format!(
            "{label} refresh cannot continue: previously-refreshed base snapshot for {} is no longer reachable",
            status.fqn
        )),
        (Some(previous), Some(current)) if previous == current => RefreshDecision::MetadataOnly,
        (Some(_), Some(_)) => RefreshDecision::Incremental,
    }
}

fn decide_all_bases_required_refresh(
    statuses: &[BaseSnapshotStatus],
    label: &str,
) -> RefreshDecision {
    if let Some(decision) = reject_invalid_base_statuses(statuses, label) {
        return decision;
    }

    let has_previous = statuses
        .iter()
        .any(|status| status.previous_snapshot_id.is_some());
    let all_previous = statuses
        .iter()
        .all(|status| status.previous_snapshot_id.is_some());
    let any_current = statuses
        .iter()
        .any(|status| status.current_snapshot_id_before_pin.is_some());
    let all_current = statuses
        .iter()
        .all(|status| status.current_snapshot_id_before_pin.is_some());

    if has_previous && !all_previous {
        return fail_fast(format!(
            "{label} refresh has partial previous refresh snapshots; recreate the MV"
        ));
    }
    if !has_previous && !any_current {
        return RefreshDecision::SkipEmpty;
    }
    if !has_previous && !all_current {
        return fail_fast(format!(
            "{label} refresh cannot run first refresh because only some bases have current snapshots; load all bases or recreate the MV"
        ));
    }
    if has_previous && !all_current {
        return fail_fast(format!(
            "{label} refresh cannot continue: previously-refreshed base snapshot for {} is no longer reachable",
            missing_current_snapshot_fqn(statuses)
        ));
    }
    if !has_previous {
        return RefreshDecision::FirstRefresh;
    }
    if snapshots_are_unchanged(statuses) {
        RefreshDecision::MetadataOnly
    } else {
        RefreshDecision::Incremental
    }
}

fn decide_join_pair_partial_initial_skip_refresh(
    statuses: &[BaseSnapshotStatus],
    label: &str,
) -> RefreshDecision {
    if statuses.len() != 2 {
        return fail_fast(format!(
            "{label} refresh requires exactly two base snapshot statuses, got {}",
            statuses.len()
        ));
    }

    let has_previous = statuses
        .iter()
        .any(|status| status.previous_snapshot_id.is_some());
    let all_previous = statuses
        .iter()
        .all(|status| status.previous_snapshot_id.is_some());
    let any_current = statuses
        .iter()
        .any(|status| status.current_snapshot_id_before_pin.is_some());
    let all_current = statuses
        .iter()
        .all(|status| status.current_snapshot_id_before_pin.is_some());

    if has_previous && !all_previous {
        return fail_fast(format!(
            "{label} refresh has partial previous refresh snapshots; recreate the MV"
        ));
    }
    if !has_previous && !all_current {
        return RefreshDecision::SkipEmpty;
    }
    if has_previous && !all_current {
        return fail_fast(format!(
            "{label} refresh cannot continue: previously-refreshed base snapshot for {} is no longer reachable",
            missing_current_snapshot_fqn(statuses)
        ));
    }
    if !has_previous && any_current {
        return RefreshDecision::FirstRefresh;
    }
    if snapshots_are_unchanged(statuses) {
        RefreshDecision::MetadataOnly
    } else {
        RefreshDecision::Incremental
    }
}

fn reject_invalid_base_statuses(
    statuses: &[BaseSnapshotStatus],
    label: &str,
) -> Option<RefreshDecision> {
    if statuses.is_empty() {
        return Some(fail_fast(format!(
            "{label} refresh requires at least one base snapshot status"
        )));
    }
    None
}

fn snapshots_are_unchanged(statuses: &[BaseSnapshotStatus]) -> bool {
    statuses.iter().all(|status| {
        status.previous_snapshot_id == status.current_snapshot_id_before_pin
            && status.previous_snapshot_id.is_some()
    })
}

fn missing_current_snapshot_fqn(statuses: &[BaseSnapshotStatus]) -> &str {
    statuses
        .iter()
        .find(|status| status.current_snapshot_id_before_pin.is_none())
        .map(|status| status.fqn.as_str())
        .unwrap_or("<unknown>")
}

fn fail_fast(reason: String) -> RefreshDecision {
    RefreshDecision::FailFast { reason }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn single_base_empty_skips() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.t", None, None)],
            "projection/filter",
        );

        assert_eq!(decision, RefreshDecision::SkipEmpty);
    }

    #[test]
    fn single_base_first_refresh_when_current_snapshot_appears() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.t", None, Some(10))],
            "projection/filter",
        );

        assert_eq!(decision, RefreshDecision::FirstRefresh);
    }

    #[test]
    fn single_base_previous_snapshot_missing_current_fails() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.t", Some(10), None)],
            "projection/filter",
        );

        assert!(matches!(decision, RefreshDecision::FailFast { .. }));
    }

    #[test]
    fn single_base_unchanged_snapshot_is_metadata_only() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.t", Some(10), Some(10))],
            "projection/filter",
        );

        assert_eq!(decision, RefreshDecision::MetadataOnly);
    }

    #[test]
    fn single_base_changed_snapshot_is_incremental() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.t", Some(10), Some(11))],
            "projection/filter",
        );

        assert_eq!(decision, RefreshDecision::Incremental);
    }

    #[test]
    fn all_bases_required_all_empty_skips() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::AllBasesRequired,
            &[
                BaseSnapshotStatus::new("ice.db.t1", None, None),
                BaseSnapshotStatus::new("ice.db.t2", None, None),
            ],
            "fan-in aggregate",
        );

        assert_eq!(decision, RefreshDecision::SkipEmpty);
    }

    #[test]
    fn all_bases_required_partial_initial_current_fails() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::AllBasesRequired,
            &[
                BaseSnapshotStatus::new("ice.db.t1", None, Some(10)),
                BaseSnapshotStatus::new("ice.db.t2", None, None),
            ],
            "fan-in aggregate",
        );

        assert!(matches!(decision, RefreshDecision::FailFast { .. }));
    }

    #[test]
    fn all_bases_required_all_current_first_refreshes() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::AllBasesRequired,
            &[
                BaseSnapshotStatus::new("ice.db.t1", None, Some(10)),
                BaseSnapshotStatus::new("ice.db.t2", None, Some(20)),
            ],
            "fan-in aggregate",
        );

        assert_eq!(decision, RefreshDecision::FirstRefresh);
    }

    #[test]
    fn all_bases_required_partial_previous_metadata_fails() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::AllBasesRequired,
            &[
                BaseSnapshotStatus::new("ice.db.t1", Some(10), Some(10)),
                BaseSnapshotStatus::new("ice.db.t2", None, Some(20)),
            ],
            "fan-in aggregate",
        );

        assert!(matches!(decision, RefreshDecision::FailFast { .. }));
    }

    #[test]
    fn all_bases_required_previous_snapshot_missing_current_fails() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::AllBasesRequired,
            &[
                BaseSnapshotStatus::new("ice.db.t1", Some(10), Some(10)),
                BaseSnapshotStatus::new("ice.db.t2", Some(20), None),
            ],
            "fan-in aggregate",
        );

        assert!(matches!(decision, RefreshDecision::FailFast { .. }));
    }

    #[test]
    fn join_pair_partial_initial_current_skips() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::JoinPairPartialInitialSkip,
            &[
                BaseSnapshotStatus::new("ice.db.left", None, Some(10)),
                BaseSnapshotStatus::new("ice.db.right", None, None),
            ],
            "join aggregate",
        );

        assert_eq!(decision, RefreshDecision::SkipEmpty);
    }

    #[test]
    fn join_pair_all_current_first_refreshes() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::JoinPairPartialInitialSkip,
            &[
                BaseSnapshotStatus::new("ice.db.left", None, Some(10)),
                BaseSnapshotStatus::new("ice.db.right", None, Some(20)),
            ],
            "join aggregate",
        );

        assert_eq!(decision, RefreshDecision::FirstRefresh);
    }

    #[test]
    fn join_pair_partial_previous_metadata_fails() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::JoinPairPartialInitialSkip,
            &[
                BaseSnapshotStatus::new("ice.db.left", Some(10), Some(10)),
                BaseSnapshotStatus::new("ice.db.right", None, Some(20)),
            ],
            "join aggregate",
        );

        assert!(matches!(decision, RefreshDecision::FailFast { .. }));
    }

    #[test]
    fn join_pair_previous_snapshot_missing_current_fails() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::JoinPairPartialInitialSkip,
            &[
                BaseSnapshotStatus::new("ice.db.left", Some(10), Some(10)),
                BaseSnapshotStatus::new("ice.db.right", Some(20), None),
            ],
            "join aggregate",
        );

        assert!(matches!(decision, RefreshDecision::FailFast { .. }));
    }

    #[test]
    fn unchanged_snapshots_are_metadata_only() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::AllBasesRequired,
            &[
                BaseSnapshotStatus::new("ice.db.t1", Some(10), Some(10)),
                BaseSnapshotStatus::new("ice.db.t2", Some(20), Some(20)),
            ],
            "fan-in aggregate",
        );

        assert_eq!(decision, RefreshDecision::MetadataOnly);
    }

    #[test]
    fn changed_snapshots_are_incremental() {
        let decision = decide_refresh(
            BaseSnapshotPolicy::AllBasesRequired,
            &[
                BaseSnapshotStatus::new("ice.db.t1", Some(10), Some(11)),
                BaseSnapshotStatus::new("ice.db.t2", Some(20), Some(20)),
            ],
            "fan-in aggregate",
        );

        assert_eq!(decision, RefreshDecision::Incremental);
    }

    #[test]
    fn lifecycle_dispatches_first_refresh_closure() {
        let calls = RefCell::new(Vec::new());

        let result = IcebergMvRefreshLifecycle::run(
            RefreshDecision::FirstRefresh,
            || {
                calls.borrow_mut().push("first");
                Ok(crate::engine::StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("metadata");
                Ok(crate::engine::StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("incremental");
                Ok(crate::engine::StatementResult::Ok)
            },
        )
        .expect("first refresh closure should succeed");

        assert!(matches!(result, crate::engine::StatementResult::Ok));
        assert_eq!(*calls.borrow(), vec!["first"]);
    }

    #[test]
    fn lifecycle_dispatches_metadata_only_closure() {
        let calls = RefCell::new(Vec::new());

        let result = IcebergMvRefreshLifecycle::run(
            RefreshDecision::MetadataOnly,
            || {
                calls.borrow_mut().push("first");
                Ok(crate::engine::StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("metadata");
                Ok(crate::engine::StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("incremental");
                Ok(crate::engine::StatementResult::Ok)
            },
        )
        .expect("metadata-only closure should succeed");

        assert!(matches!(result, crate::engine::StatementResult::Ok));
        assert_eq!(*calls.borrow(), vec!["metadata"]);
    }

    #[test]
    fn lifecycle_dispatches_incremental_closure() {
        let calls = RefCell::new(Vec::new());

        let result = IcebergMvRefreshLifecycle::run(
            RefreshDecision::Incremental,
            || {
                calls.borrow_mut().push("first");
                Ok(crate::engine::StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("metadata");
                Ok(crate::engine::StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("incremental");
                Ok(crate::engine::StatementResult::Ok)
            },
        )
        .expect("incremental closure should succeed");

        assert!(matches!(result, crate::engine::StatementResult::Ok));
        assert_eq!(*calls.borrow(), vec!["incremental"]);
    }

    #[test]
    fn lifecycle_skip_empty_returns_ok_without_calling_closures() {
        let result = IcebergMvRefreshLifecycle::run(
            RefreshDecision::SkipEmpty,
            || panic!("first refresh closure must not run"),
            || panic!("metadata-only closure must not run"),
            || panic!("incremental closure must not run"),
        )
        .expect("skip-empty should succeed");

        assert!(matches!(result, crate::engine::StatementResult::Ok));
    }

    #[test]
    fn lifecycle_fail_fast_returns_reason_without_calling_closures() {
        let result = IcebergMvRefreshLifecycle::run(
            RefreshDecision::FailFast {
                reason: "missing snapshot".to_string(),
            },
            || panic!("first refresh closure must not run"),
            || panic!("metadata-only closure must not run"),
            || panic!("incremental closure must not run"),
        );

        assert_eq!(result.unwrap_err(), "missing snapshot");
    }
}
