//! Collects per-table maintenance facts from a single Iceberg metadata load:
//! snapshot list, current-snapshot summary counters, table properties, refs,
//! and the downstream-consumer floor that protects incremental MV lineage.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::meta::repository::mv::StoredMvDefinition;

use super::policy::{SnapshotInfo, TableMaintenanceStats};

/// Iceberg snapshot-summary keys (string literals: the constants in
/// vendor/iceberg-0.9.0/src/spec/snapshot_summary.rs are private).
const TOTAL_DATA_FILES_KEY: &str = "total-data-files";
const TOTAL_DELETE_FILES_KEY: &str = "total-delete-files";
const TOTAL_FILES_SIZE_KEY: &str = "total-files-size";
const MAIN_BRANCH: &str = "main";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DownstreamFloor {
    pub(crate) floor_ts_ms: Option<i64>,
    pub(crate) unknown: bool,
}

pub(crate) fn summary_u64(props: &HashMap<String, String>, key: &str) -> Option<u64> {
    props.get(key).and_then(|v| v.trim().parse::<u64>().ok())
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

/// Load fresh metadata for one MV storage table and assemble stats.
/// `definitions` is the full MV list from the same pass, used for the floor.
pub(crate) fn collect_table_stats(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
    table: &str,
    definitions: &[StoredMvDefinition],
) -> Result<TableMaintenanceStats, String> {
    let (iceberg_catalog, table_ident, _object_store) =
        crate::engine::iceberg_maintenance::resolve_maintenance_catalog(
            state, catalog, namespace, table,
        )?;
    let loaded = crate::connector::iceberg::catalog::registry::block_on_iceberg(async move {
        iceberg_catalog.load_table(&table_ident).await
    })?
    .map_err(|e| {
        format!("load iceberg table {catalog}.{namespace}.{table} for maintenance failed: {e}")
    })?;
    let metadata = loaded.metadata();

    let snapshots: Vec<SnapshotInfo> = metadata
        .snapshots()
        .map(|s| SnapshotInfo {
            snapshot_id: s.snapshot_id(),
            timestamp_ms: s.timestamp_ms(),
        })
        .collect();
    let snapshot_ts_by_id: BTreeMap<i64, i64> = snapshots
        .iter()
        .map(|s| (s.snapshot_id, s.timestamp_ms))
        .collect();

    let summary = metadata
        .current_snapshot()
        .map(|s| s.summary().additional_properties.clone())
        .unwrap_or_default();

    let fqn = format!("{catalog}.{namespace}.{table}");
    let floor = downstream_floor(definitions, &fqn, &snapshot_ts_by_id);

    let non_main_ref_count = metadata
        .refs()
        .keys()
        .filter(|name| name.as_str() != MAIN_BRANCH)
        .count();

    Ok(TableMaintenanceStats {
        current_snapshot_id: metadata.current_snapshot_id(),
        snapshots,
        total_data_files: summary_u64(&summary, TOTAL_DATA_FILES_KEY),
        total_files_size_bytes: summary_u64(&summary, TOTAL_FILES_SIZE_KEY),
        total_delete_files: summary_u64(&summary, TOTAL_DELETE_FILES_KEY),
        properties: metadata.properties().clone(),
        non_main_ref_count,
        downstream_floor_ts_ms: floor.floor_ts_ms,
        downstream_floor_unknown: floor.unknown,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::repository::mv::{StoredMvDefinition, StoredMvRefreshPolicy};
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

    #[test]
    fn summary_u64_parses_and_rejects() {
        let mut props = std::collections::HashMap::new();
        props.insert("total-data-files".to_string(), "42".to_string());
        props.insert("bad".to_string(), "x".to_string());
        assert_eq!(summary_u64(&props, "total-data-files"), Some(42));
        assert_eq!(summary_u64(&props, "bad"), None);
        assert_eq!(summary_u64(&props, "absent"), None);
    }
}
