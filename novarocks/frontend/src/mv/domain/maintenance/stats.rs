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

//! Collects per-table maintenance facts through the neutral connector surface:
//! snapshot list, current-snapshot summary counters, typed maintenance policy
//! facts, references, the provider-signed compaction count, and the
//! downstream-consumer floor that protects incremental MV lineage.
//!
//! Nothing here interprets a storage format. The provider signs every fact; the
//! frontend owns every policy decision made from them.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorTableIdentity, ConnectorTableResolution,
};

use crate::mv::domain::persistence::definition::StoredMvDefinition;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

/// Provider facts only. Frontend owns every policy decision and retry state.
#[derive(Clone, Debug, Default)]
pub struct TableMaintenanceStats {
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Vec<SnapshotInfo>,
    pub total_data_files: Option<u64>,
    pub max_compactable_data_files: Option<u64>,
    pub total_files_size_bytes: Option<u64>,
    pub total_delete_files: Option<u64>,
    /// Typed maintenance policy facts declared by the table. `None` means the
    /// table declares no usable value for that key; defaults and clamping are
    /// policy and belong to the frontend, never to this fact layer.
    pub maintenance_enabled: Option<bool>,
    pub expire_max_snapshot_age_ms: Option<i64>,
    pub expire_min_snapshots_to_keep: Option<u32>,
    pub target_file_size_bytes: Option<i64>,
    pub non_default_reference_count: usize,
    pub downstream_floor_ts_ms: Option<i64>,
    pub downstream_floor_unknown: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DownstreamFloor {
    pub(crate) floor_ts_ms: Option<i64>,
    pub(crate) unknown: bool,
}

/// Minimum consumed-snapshot timestamp across all MV definitions that read
/// `table_fqn` incrementally (committed positions and in-flight pins). A
/// consumer pointing at a snapshot we cannot resolve marks the floor unknown,
/// which blocks expire for safety.
pub(crate) fn downstream_floor(
    definitions: &[StoredMvDefinition],
    table_fqn: &str,
    snapshot_ts_by_id: &BTreeMap<i64, i64>,
) -> DownstreamFloor {
    let mut floor_ts: Option<i64> = None;
    let mut unknown = false;
    let mut consider = |snapshot_id: i64| match snapshot_ts_by_id.get(&snapshot_id) {
        Some(ts) => floor_ts = Some(floor_ts.map_or(*ts, |f| f.min(*ts))),
        None => unknown = true,
    };
    for definition in definitions {
        if let Some(id) = definition.last_refresh_snapshots.get(table_fqn) {
            consider(*id);
        }
        if let Some(id) = definition.refresh_target_snapshots.get(table_fqn) {
            consider(*id);
        }
    }
    DownstreamFloor {
        floor_ts_ms: floor_ts,
        unknown,
    }
}

/// Read one MV storage table's maintenance facts through the neutral surface.
/// `definitions` is the full MV list from the same pass, used for the floor.
///
/// The compaction observation runs first on purpose. Answering it forces the
/// provider to discard its cached table and re-read the catalog, and the
/// provider repopulates that cache with what it read, so the metadata load
/// below observes the very table version the count was taken from. Reversing
/// the two would let the projected facts describe an older table than the
/// count, and would drop the forced refresh this pass has always performed.
/// Read one MV storage table's maintenance facts through explicit frontend
/// control and observation ports. Background policy must retain only these
/// leaves, never the aggregate standalone engine state.
pub fn collect_table_stats_with_ports(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    catalog: &str,
    namespace: &str,
    table: &str,
    definitions: &[StoredMvDefinition],
) -> Result<TableMaintenanceStats, String> {
    let context = novarocks::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let identity = ConnectorTableIdentity {
        instance_id: instance_id.clone(),
        namespace: Arc::from(namespace),
        table: Arc::from(table),
    };

    let max_compactable_data_files =
        novarocks::connector::metadata_maintenance::read_max_compactable_data_files(
            connector_control,
            &instance_id,
            identity,
            context.clone(),
        )
        .map_err(|error| {
            format!(
                "observe {catalog}.{namespace}.{table} compaction groups for maintenance: {error}"
            )
        })?;

    let exact_lease =
        novarocks::connector::acquire_metadata_planning_lease(connector_control, catalog)?;
    let metadata = novarocks::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        context.clone(),
        namespace,
        table,
        ConnectorTableResolution::StrictBaseTable,
    )?;
    let observed = crate::mv::domain::storage_observation::observe_maintenance_metadata(
        storage_observation,
        &exact_lease,
        &metadata,
        context,
    )
    .map_err(|error| {
        format!("observe {catalog}.{namespace}.{table} maintenance metadata: {error}")
    })?;

    let snapshots: Vec<SnapshotInfo> = observed
        .snapshots()
        .iter()
        .map(|snapshot| SnapshotInfo {
            snapshot_id: snapshot.snapshot_id,
            timestamp_ms: snapshot.timestamp_ms,
        })
        .collect();
    let snapshot_ts_by_id: BTreeMap<i64, i64> = snapshots
        .iter()
        .map(|snapshot| (snapshot.snapshot_id, snapshot.timestamp_ms))
        .collect();

    let fqn = format!("{catalog}.{namespace}.{table}");
    let floor = downstream_floor(definitions, &fqn, &snapshot_ts_by_id);
    let policy = *observed.policy();

    Ok(TableMaintenanceStats {
        current_snapshot_id: observed.current_snapshot_id(),
        snapshots,
        total_data_files: observed.total_data_files(),
        max_compactable_data_files,
        total_files_size_bytes: observed.total_files_size_bytes(),
        total_delete_files: observed.total_delete_files(),
        maintenance_enabled: policy.maintenance_enabled,
        expire_max_snapshot_age_ms: policy.expire_max_snapshot_age_ms,
        expire_min_snapshots_to_keep: policy.expire_min_snapshots_to_keep,
        target_file_size_bytes: policy.target_file_size_bytes,
        non_default_reference_count: observed.non_default_reference_count(),
        downstream_floor_ts_ms: floor.floor_ts_ms,
        downstream_floor_unknown: floor.unknown,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::persistence::definition::{StoredMvDefinition, StoredMvRefreshPolicy};
    use std::collections::BTreeMap;

    fn definition_with_consumed(fqn: &str, snapshot_id: i64) -> StoredMvDefinition {
        let mut last_refresh_snapshots = BTreeMap::new();
        last_refresh_snapshots.insert(fqn.to_string(), snapshot_id);
        StoredMvDefinition {
            mv_id: 1,
            select_sql: "SELECT 1".to_string(),
            base_table_refs: vec![fqn.to_string()],
            primary_key_columns: vec![],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("analytics".to_string()),
            target_table: Some("mv_x".to_string()),
            schema_contract: None,
            partition_spec: None,
            partition_state_complete: false,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots,
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            refresh_policy: StoredMvRefreshPolicy::Manual,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 0,
        }
    }

    #[test]
    fn floor_is_min_consumed_snapshot_timestamp() {
        let mut ts_by_id = BTreeMap::new();
        ts_by_id.insert(10, 1_000);
        ts_by_id.insert(20, 2_000);
        let defs = vec![
            definition_with_consumed("ice.sales.t", 20),
            definition_with_consumed("ice.sales.t", 10),
        ];
        let floor = downstream_floor(&defs, "ice.sales.t", &ts_by_id);
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: Some(1_000),
                unknown: false
            }
        );
    }

    #[test]
    fn floor_is_none_without_consumers() {
        let defs = vec![definition_with_consumed("ice.sales.other", 10)];
        let floor = downstream_floor(&defs, "ice.sales.t", &BTreeMap::new());
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: None,
                unknown: false
            }
        );
    }

    #[test]
    fn floor_unknown_when_consumed_snapshot_missing_from_metadata() {
        let defs = vec![definition_with_consumed("ice.sales.t", 99)];
        let floor = downstream_floor(&defs, "ice.sales.t", &BTreeMap::new());
        assert!(floor.unknown);
    }

    #[test]
    fn floor_considers_in_progress_refresh_pins() {
        let mut ts_by_id = BTreeMap::new();
        ts_by_id.insert(10, 1_000);
        let mut def = definition_with_consumed("ice.sales.other", 1);
        def.refresh_target_snapshots
            .insert("ice.sales.t".to_string(), 10);
        let floor = downstream_floor(&[def], "ice.sales.t", &ts_by_id);
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: Some(1_000),
                unknown: false
            }
        );
    }

    #[test]
    fn floor_takes_min_across_both_maps_for_same_table() {
        let mut ts_by_id = BTreeMap::new();
        ts_by_id.insert(10, 1_000);
        ts_by_id.insert(20, 2_000);
        // One consumer: committed at snapshot 20, with an in-flight refresh
        // pinned at the older snapshot 10. The floor must reflect the older pin.
        let mut def = definition_with_consumed("ice.sales.t", 20);
        def.refresh_target_snapshots
            .insert("ice.sales.t".to_string(), 10);
        let floor = downstream_floor(&[def], "ice.sales.t", &ts_by_id);
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: Some(1_000),
                unknown: false
            }
        );
    }
}
