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

use std::collections::BTreeMap;

use crate::mv::model::{AffectedTargetPartitions, MvStorageEngine, MvTarget, RefreshMode};
use crate::mv::refresh::snapshot::{
    BaseSnapshotPolicy, BaseSnapshotStatus, ExecutableRefreshDecision, decide_refresh,
};
use novarocks_catalog::identifier::TableIdentity;

pub(crate) struct RefreshPlanningInput<'a> {
    pub(crate) snapshot_policy: BaseSnapshotPolicy,
    pub(crate) base_snapshots: &'a [BaseSnapshotStatus],
    pub(crate) label: &'a str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshPlanningDecision {
    pub(crate) refresh: ExecutableRefreshDecision,
}

impl RefreshPlanningDecision {
    pub(crate) fn mode(&self) -> RefreshMode {
        self.refresh.mode()
    }
}

pub(crate) fn decide_refresh_plan(
    input: &RefreshPlanningInput<'_>,
) -> Result<RefreshPlanningDecision, String> {
    let refresh = ExecutableRefreshDecision::from_refresh_decision(decide_refresh(
        input.snapshot_policy,
        input.base_snapshots,
        input.label,
    ))?;
    Ok(RefreshPlanningDecision { refresh })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RefreshStateBaseline {
    Pinless,
    SnapshotBacked {
        previous_snapshot_ids: BTreeMap<String, i64>,
        previous_table_uuids: BTreeMap<String, String>,
        target_snapshot_id: Option<i64>,
        target_table_uuid: String,
        definition_fingerprint: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshPlanContract {
    pub(crate) mv_id: Option<i64>,
    pub(crate) target: MvTarget,
    pub(crate) storage_engine: MvStorageEngine,
    pub(crate) decision: ExecutableRefreshDecision,
    pub(crate) state_baseline: RefreshStateBaseline,
    pub(crate) base_refs: Vec<TableIdentity>,
    pub(crate) snapshot_pins: BTreeMap<String, Option<i64>>,
    pub(crate) affected_partitions: AffectedTargetPartitions,
}

impl RefreshPlanContract {
    pub(crate) fn mode(&self) -> RefreshMode {
        self.decision.mode()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::mv::model::{AffectedTargetPartitions, MvStorageEngine, MvTarget, RefreshMode};
    use crate::mv::refresh::snapshot::{
        BaseSnapshotPolicy, BaseSnapshotStatus, ExecutableRefreshDecision,
    };
    use novarocks_catalog::identifier::TableIdentity;

    const LABEL: &str = "iceberg MV test";

    fn decide(
        snapshot_policy: BaseSnapshotPolicy,
        base_snapshots: &[BaseSnapshotStatus],
    ) -> Result<RefreshPlanningDecision, String> {
        decide_refresh_plan(&RefreshPlanningInput {
            snapshot_policy,
            base_snapshots,
            label: LABEL,
        })
    }

    #[test]
    fn planning_preserves_skip_empty_vs_metadata_only() {
        let skip = decide(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.left", None, None)],
        )
        .unwrap();
        let metadata = decide(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.left", Some(10), Some(10))],
        )
        .unwrap();

        assert_eq!(skip.refresh, ExecutableRefreshDecision::SkipEmpty);
        assert_eq!(skip.mode(), RefreshMode::Noop);
        assert_eq!(metadata.refresh, ExecutableRefreshDecision::MetadataOnly);
        assert_eq!(metadata.mode(), RefreshMode::Noop);
    }

    #[test]
    fn planning_projects_first_and_incremental_modes() {
        let first = decide(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.left", None, Some(10))],
        )
        .unwrap();
        let incremental = decide(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.left", Some(10), Some(11))],
        )
        .unwrap();

        assert_eq!(first.refresh, ExecutableRefreshDecision::FirstRefresh);
        assert_eq!(first.mode(), RefreshMode::Full);
        assert_eq!(incremental.refresh, ExecutableRefreshDecision::Incremental);
        assert_eq!(incremental.mode(), RefreshMode::Incremental);
    }

    #[test]
    fn planning_preserves_fail_fast_reason() {
        let error = decide(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.left", Some(10), None)],
        )
        .unwrap_err();

        assert_eq!(
            error,
            "iceberg MV test refresh cannot continue: previously-refreshed base snapshot for ice.db.left is no longer reachable"
        );
    }

    #[test]
    fn planning_uses_explicit_snapshot_policy() {
        let statuses = [
            BaseSnapshotStatus::new("ice.db.left", None, Some(10)),
            BaseSnapshotStatus::new("ice.db.right", None, None),
        ];

        assert_eq!(
            decide(BaseSnapshotPolicy::AllBasesRequired, &statuses).unwrap_err(),
            "iceberg MV test refresh cannot run first refresh because only some bases have current snapshots; load all bases or recreate the MV"
        );
        let partial = decide(BaseSnapshotPolicy::JoinPairPartialInitialSkip, &statuses).unwrap();
        assert_eq!(partial.refresh, ExecutableRefreshDecision::SkipEmpty);
        assert_eq!(partial.mode(), RefreshMode::Noop);
    }

    #[test]
    fn plan_contract_preserves_common_fields() {
        let target = MvTarget {
            catalog: Some("ice".to_string()),
            database: "db".to_string(),
            name: "mv".to_string(),
        };
        let base_refs = vec![
            TableIdentity::new("ice", "db", "left"),
            TableIdentity::new("ice", "db", "right"),
        ];
        let snapshot_pins = BTreeMap::from([
            ("ice.db.left".to_string(), Some(10)),
            ("ice.db.right".to_string(), Some(20)),
        ]);
        let affected_partitions = AffectedTargetPartitions::not_derived("join planning");
        let state_baseline = RefreshStateBaseline::SnapshotBacked {
            previous_snapshot_ids: BTreeMap::from([("ice.db.left".to_string(), 9)]),
            previous_table_uuids: BTreeMap::from([(
                "ice.db.left".to_string(),
                "uuid-left".to_string(),
            )]),
            target_snapshot_id: Some(30),
            target_table_uuid: "uuid-target".to_string(),
            definition_fingerprint: "definition-v1".to_string(),
        };

        let contract = RefreshPlanContract {
            mv_id: Some(42),
            target: target.clone(),
            storage_engine: MvStorageEngine::Iceberg,
            decision: ExecutableRefreshDecision::Incremental,
            state_baseline: state_baseline.clone(),
            base_refs: base_refs.clone(),
            snapshot_pins: snapshot_pins.clone(),
            affected_partitions: affected_partitions.clone(),
        };

        assert_eq!(contract.mv_id, Some(42));
        assert_eq!(contract.target, target);
        assert_eq!(contract.storage_engine, MvStorageEngine::Iceberg);
        assert_eq!(contract.mode(), RefreshMode::Incremental);
        assert_eq!(contract.state_baseline, state_baseline);
        assert_eq!(contract.base_refs, base_refs);
        assert_eq!(contract.snapshot_pins, snapshot_pins);
        assert_eq!(contract.affected_partitions, affected_partitions);
    }
}
