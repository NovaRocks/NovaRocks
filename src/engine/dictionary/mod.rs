pub(crate) mod maintenance;
pub(crate) mod model;
pub(crate) mod rebuild;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::DataType;

use crate::engine::dictionary::model::{
    DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue, DictionaryWatermark,
};
use crate::meta::repository::dictionary::{
    DICTIONARY_STATE_ACTIVE, DICTIONARY_STATE_DROPPED, DICTIONARY_STATE_STALE,
    DictionaryMetaRepository, StoredDictionarySnapshot, StoredDictionaryValue,
};
use crate::sql::catalog::{ScanSource, TableDef};
use crate::sql::optimizer::rewrite::context::QueryDictionaryProvider;

#[derive(Clone, Default)]
pub(crate) struct DictionaryManager {
    repo: Arc<DictionaryMetaRepository>,
}

impl DictionaryManager {
    pub(crate) fn repo(&self) -> &DictionaryMetaRepository {
        &self.repo
    }

    /// Load the active snapshot for `(owner, column_name)` from the metadata
    /// provider. Returns `Ok(None)` either when there is no metadata provider
    /// configured (test/embedding modes) or when there is no active snapshot
    /// for the requested owner/column triple.
    pub(crate) fn load_active_snapshot(
        &self,
        state: &crate::engine::StandaloneState,
        owner: &DictionaryOwner,
        column_name: &str,
    ) -> Result<Option<DictionarySnapshot>, String> {
        let Some(provider) = state.metadata_provider.as_ref() else {
            return Ok(None);
        };
        let txn = provider
            .begin_read()
            .map_err(|e| format!("open dictionary read txn failed: {e}"))?;
        let stored = self
            .repo
            .load_active(txn.as_ref(), owner.kind(), &owner.stable_key(), column_name)
            .map_err(|e| format!("load active dictionary snapshot failed: {e}"))?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        let snapshot = stored_to_snapshot(stored)?;
        Ok(Some(snapshot))
    }

    /// Persist `snapshot` as the active dictionary for its owner/column. Fails
    /// fast when no metadata provider is configured.
    pub(crate) fn upsert_snapshot(
        &self,
        state: &crate::engine::StandaloneState,
        snapshot: DictionarySnapshot,
    ) -> Result<(), String> {
        let provider = state.metadata_provider.as_ref().ok_or_else(|| {
            "dictionary upsert requires a metadata provider but none is configured".to_string()
        })?;
        let stored = snapshot_to_stored(snapshot)?;
        let mut txn = provider
            .begin_write("upsert dictionary snapshot")
            .map_err(|e| format!("open dictionary write txn failed: {e}"))?;
        self.repo
            .upsert_snapshot(txn.as_mut(), &stored)
            .map_err(|e| format!("upsert dictionary snapshot failed: {e}"))?;
        txn.commit()
            .map_err(|e| format!("commit dictionary snapshot upsert failed: {e}"))?;
        Ok(())
    }
}

fn dictionary_state_to_str(state: &DictionaryState) -> &'static str {
    match state {
        DictionaryState::Active => DICTIONARY_STATE_ACTIVE,
        DictionaryState::Stale => DICTIONARY_STATE_STALE,
        DictionaryState::Dropped => DICTIONARY_STATE_DROPPED,
    }
}

fn dictionary_state_from_str(value: &str) -> Result<DictionaryState, String> {
    match value {
        DICTIONARY_STATE_ACTIVE => Ok(DictionaryState::Active),
        DICTIONARY_STATE_STALE => Ok(DictionaryState::Stale),
        DICTIONARY_STATE_DROPPED => Ok(DictionaryState::Dropped),
        other => Err(format!("unknown dictionary state `{other}`")),
    }
}

/// Canonical string form for the supported Arrow column types.
fn data_type_to_canonical_str(ty: &DataType) -> Result<&'static str, String> {
    match ty {
        DataType::Utf8 => Ok("UTF8"),
        DataType::LargeUtf8 => Ok("LARGEUTF8"),
        DataType::Binary => Ok("BINARY"),
        DataType::LargeBinary => Ok("LARGEBINARY"),
        other => Err(format!(
            "dictionary column type {other:?} is not supported (expected string/binary)"
        )),
    }
}

fn canonical_str_to_data_type(value: &str) -> Result<DataType, String> {
    match value.to_ascii_uppercase().as_str() {
        "UTF8" => Ok(DataType::Utf8),
        "LARGEUTF8" => Ok(DataType::LargeUtf8),
        "BINARY" => Ok(DataType::Binary),
        "LARGEBINARY" => Ok(DataType::LargeBinary),
        _ => Err(value.to_string()),
    }
}

fn snapshot_to_stored(snapshot: DictionarySnapshot) -> Result<StoredDictionarySnapshot, String> {
    let DictionarySnapshot {
        dictionary_id,
        owner,
        column_id,
        column_name,
        data_type,
        version,
        watermark,
        values,
        null_id,
        state,
        order_preserving,
    } = snapshot;
    let now_ms = current_millis();
    let data_type_str = data_type_to_canonical_str(&data_type)?.to_string();
    let owner_kind = owner.kind().to_string();
    let owner_key = owner.stable_key();
    let watermark_json = watermark.stable_json();
    let stored_values = values
        .into_iter()
        .map(|value| StoredDictionaryValue {
            id: value.id,
            bytes: value.bytes,
        })
        .collect();
    Ok(StoredDictionarySnapshot {
        dictionary_id,
        owner_kind,
        owner_key,
        column_id,
        column_name,
        data_type: data_type_str,
        version,
        watermark: watermark_json,
        values: stored_values,
        null_id,
        state: dictionary_state_to_str(&state).to_string(),
        order_preserving,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    })
}

fn stored_to_snapshot(stored: StoredDictionarySnapshot) -> Result<DictionarySnapshot, String> {
    let StoredDictionarySnapshot {
        dictionary_id,
        owner_kind,
        owner_key,
        column_id,
        column_name,
        data_type,
        version,
        watermark,
        values,
        null_id,
        state,
        order_preserving,
        created_at_ms: _,
        updated_at_ms: _,
    } = stored;
    let owner = parse_owner(&owner_kind, &owner_key)?;
    let data_type = canonical_str_to_data_type(&data_type).map_err(|raw| {
        format!("dictionary snapshot {dictionary_id} has unsupported data type {raw}")
    })?;
    let watermark = serde_json::from_str::<DictionaryWatermark>(&watermark).map_err(|e| {
        format!("dictionary snapshot {dictionary_id} has invalid watermark JSON: {e}")
    })?;
    let state = dictionary_state_from_str(&state)?;
    let values = values
        .into_iter()
        .map(|value| DictionaryValue {
            id: value.id,
            bytes: value.bytes,
        })
        .collect();
    Ok(DictionarySnapshot {
        dictionary_id,
        owner,
        column_id,
        column_name,
        data_type,
        version,
        watermark,
        values,
        null_id,
        state,
        order_preserving,
    })
}

fn parse_owner(owner_kind: &str, owner_key: &str) -> Result<DictionaryOwner, String> {
    let fields = parse_key_fields(owner_key);
    match owner_kind {
        "starrocks_table" => {
            let database = fields
                .get("db")
                .cloned()
                .ok_or_else(|| format!("owner key missing `db`: {owner_key}"))?;
            let table = fields
                .get("table")
                .cloned()
                .ok_or_else(|| format!("owner key missing `table`: {owner_key}"))?;
            let db_id = fields
                .get("db_id")
                .ok_or_else(|| format!("owner key missing `db_id`: {owner_key}"))?
                .parse::<i64>()
                .map_err(|e| format!("invalid `db_id` in owner key `{owner_key}`: {e}"))?;
            let table_id = fields
                .get("table_id")
                .ok_or_else(|| format!("owner key missing `table_id`: {owner_key}"))?
                .parse::<i64>()
                .map_err(|e| format!("invalid `table_id` in owner key `{owner_key}`: {e}"))?;
            Ok(DictionaryOwner::StarRocksTable {
                database,
                table,
                db_id,
                table_id,
            })
        }
        "iceberg_table" => {
            let catalog = fields
                .get("catalog")
                .cloned()
                .ok_or_else(|| format!("owner key missing `catalog`: {owner_key}"))?;
            let namespace = fields
                .get("namespace")
                .cloned()
                .ok_or_else(|| format!("owner key missing `namespace`: {owner_key}"))?;
            let table = fields
                .get("table")
                .cloned()
                .ok_or_else(|| format!("owner key missing `table`: {owner_key}"))?;
            let uuid_raw = fields.get("uuid").cloned().unwrap_or_default();
            let table_uuid = if uuid_raw.is_empty() {
                None
            } else {
                Some(uuid_raw)
            };
            Ok(DictionaryOwner::IcebergTable {
                catalog,
                namespace,
                table,
                table_uuid,
            })
        }
        other => Err(format!("unknown dictionary owner kind `{other}`")),
    }
}

fn parse_key_fields(key: &str) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    for part in key.split(';') {
        if let Some((k, v)) = part.split_once('=') {
            out.insert(k.to_string(), v.to_string());
        }
    }
    out
}

fn current_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `QueryDictionaryProvider` implementation backed by the standalone
/// engine state. Resolves the active dictionary snapshot for a column
/// by mapping the `TableDef` into a `DictionaryOwner` and delegating to
/// `DictionaryManager::load_active_snapshot`. Returns `Ok(None)` for
/// table sources that do not support dictionaries (metadata tables,
/// IVM delta scans, etc.) and for missing snapshots.
pub(crate) struct DictionaryQueryProvider {
    state: Arc<crate::engine::StandaloneState>,
}

impl DictionaryQueryProvider {
    pub(crate) fn new(state: Arc<crate::engine::StandaloneState>) -> Self {
        Self { state }
    }

    fn owner_for(
        &self,
        table: &TableDef,
        database: &str,
    ) -> Result<Option<DictionaryOwner>, String> {
        match &table.source {
            // Lock-free: (db_id, table_id) live in the plan node, populated
            // when the StarRocks table was registered via
            // `InMemoryCatalog::register_starrocks_table`. We do NOT take
            // `state.starrocks_table.read()` here — every Scan column of every
            // SELECT calls this method, and that lock is contended with
            // INSERT / DROP DATABASE writers under parallel sql-tests.
            ScanSource::StarRocks { db_id, table_id } => {
                Ok(Some(DictionaryOwner::StarRocksTable {
                    database: database.to_string(),
                    table: table.name.clone(),
                    db_id: *db_id,
                    table_id: *table_id,
                }))
            }
            ScanSource::IcebergDataFiles { table: info, .. } => {
                Ok(Some(DictionaryOwner::IcebergTable {
                    catalog: info.catalog.clone(),
                    namespace: info.namespace.clone(),
                    table: info.table.clone(),
                    table_uuid: info.table_uuid.clone(),
                }))
            }
            // Metadata tables, IVM delta scans, IMV pinned-version placeholders,
            // and IMV target-state scans never participate in dictionary rewriting.
            ScanSource::IcebergMetadataTable { .. }
            | ScanSource::IcebergDeltaTable { .. }
            | ScanSource::IcebergVersionTable { .. }
            | ScanSource::IcebergMvTargetState { .. } => Ok(None),
        }
    }
}

impl QueryDictionaryProvider for DictionaryQueryProvider {
    fn load_active_snapshot(
        &self,
        table: &TableDef,
        database: &str,
        column_name: &str,
    ) -> Result<Option<DictionarySnapshot>, String> {
        let Some(owner) = self.owner_for(table, database)? else {
            return Ok(None);
        };
        let Some(snapshot) =
            self.state
                .dictionary_manager
                .load_active_snapshot(&self.state, &owner, column_name)?
        else {
            return Ok(None);
        };
        // Iceberg snapshot-watermark staleness: a dictionary built by ANALYZE
        // FULL is pinned to the table snapshot + schema it scanned (its
        // watermark). Any later commit (INSERT / OVERWRITE / DELETE / schema
        // change) advances the table's current snapshot or schema id, after
        // which the dictionary may no longer cover the visible string set. A
        // watermark mismatch therefore means the snapshot is stale and must
        // NOT drive the rewrite (encoding an uncovered value would silently
        // map it to the null/0 id). This is conservative per the
        // low-cardinality design — we do not try to prove a delete-only commit
        // is safe; ANALYZE FULL must be re-run to refresh the dictionary.
        // (StarRocks tables use the tablet-version watermark maintained by
        // their own write path and are unaffected here.)
        if let ScanSource::IcebergDataFiles { table: info, .. } = &table.source
            && let DictionaryWatermark::Iceberg {
                snapshot_id,
                schema_id,
            } = &snapshot.watermark
            && (*snapshot_id != info.current_snapshot_id || *schema_id != info.schema_id)
        {
            return Ok(None);
        }
        Ok(Some(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::StandaloneState;
    use crate::engine::dictionary::model::StarRocksTabletWatermark;
    use crate::meta::SqliteMetaStoreProvider;

    fn open_state() -> (tempfile::TempDir, StandaloneState) {
        let dir = tempfile::tempdir().expect("tempdir");
        let provider = SqliteMetaStoreProvider::open(dir.path().join("dictionary.sqlite"))
            .expect("open provider");
        let state = StandaloneState {
            metadata_provider: Some(Arc::new(provider)),
            ..StandaloneState::default()
        };
        (dir, state)
    }

    fn sample_owner() -> DictionaryOwner {
        DictionaryOwner::StarRocksTable {
            database: "demo".to_string(),
            table: "t1".to_string(),
            db_id: 100,
            table_id: 200,
        }
    }

    fn sample_snapshot(id: i64, state: DictionaryState) -> DictionarySnapshot {
        DictionarySnapshot {
            dictionary_id: id,
            owner: sample_owner(),
            column_id: Some(300),
            column_name: "s".to_string(),
            data_type: DataType::Utf8,
            version: 1,
            watermark: DictionaryWatermark::StarRocks {
                schema_id: 7,
                tablets: vec![StarRocksTabletWatermark {
                    tablet_id: 11,
                    partition_id: 12,
                    visible_version: 13,
                }],
            },
            values: vec![
                DictionaryValue {
                    id: 1,
                    bytes: b"a".to_vec(),
                },
                DictionaryValue {
                    id: 2,
                    bytes: b"b".to_vec(),
                },
            ],
            null_id: 0,
            state,
            order_preserving: true,
        }
    }

    #[test]
    fn upsert_then_load_active_snapshot_round_trips() {
        let (_dir, state) = open_state();
        let manager = DictionaryManager::default();
        let snapshot = sample_snapshot(1, DictionaryState::Active);
        manager
            .upsert_snapshot(&state, snapshot.clone())
            .expect("upsert snapshot");
        let loaded = manager
            .load_active_snapshot(&state, &snapshot.owner, &snapshot.column_name)
            .expect("load active snapshot");
        let loaded = loaded.expect("snapshot should be active");
        assert_eq!(loaded.dictionary_id, snapshot.dictionary_id);
        assert_eq!(loaded.column_name, snapshot.column_name);
        assert_eq!(loaded.data_type, snapshot.data_type);
        assert_eq!(loaded.values.len(), snapshot.values.len());
        assert!(loaded.order_preserving);
        assert!(matches!(loaded.state, DictionaryState::Active));
        assert!(matches!(
            loaded.watermark,
            DictionaryWatermark::StarRocks { .. }
        ));
    }

    #[test]
    fn load_active_snapshot_returns_none_without_provider() {
        let state = StandaloneState::default();
        let manager = DictionaryManager::default();
        let owner = sample_owner();
        assert!(
            manager
                .load_active_snapshot(&state, &owner, "s")
                .expect("load")
                .is_none()
        );
    }

    #[test]
    fn upsert_snapshot_requires_provider() {
        let state = StandaloneState::default();
        let manager = DictionaryManager::default();
        let err = manager
            .upsert_snapshot(&state, sample_snapshot(1, DictionaryState::Active))
            .expect_err("upsert without provider must fail");
        assert!(err.contains("metadata provider"), "unexpected error: {err}");
    }

    #[test]
    fn load_active_returns_none_for_stale_snapshot() {
        let (_dir, state) = open_state();
        let manager = DictionaryManager::default();
        let snapshot = sample_snapshot(2, DictionaryState::Stale);
        manager
            .upsert_snapshot(&state, snapshot.clone())
            .expect("upsert stale snapshot");
        let loaded = manager
            .load_active_snapshot(&state, &snapshot.owner, &snapshot.column_name)
            .expect("load active snapshot");
        assert!(loaded.is_none());
    }

    /// Lock-free owner-lookup contract: `DictionaryQueryProvider::owner_for`
    /// must derive `(db_id, table_id)` from `ScanSource::StarRocks` directly,
    /// without consulting `state.starrocks_table`. This test leaves
    /// `state.starrocks_table` empty (no runtime registered) but registers a
    /// snapshot owned by `(db_id=100, table_id=200)`; if the provider still
    /// tries to look up the runtime in the catalog, it would return `Ok(None)`
    /// and `load_active_snapshot` would miss the snapshot. After the fix,
    /// identity flows from the plan node and the snapshot is found.
    #[test]
    fn dictionary_provider_owner_for_starrocks_reads_identity_from_plan_node() {
        use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let (_dir, state) = open_state();
        let state = Arc::new(state);

        // sample_owner() / sample_snapshot() use db_id=100, table_id=200,
        // database="demo", table="t1", column_name="s".
        let snapshot = sample_snapshot(1, DictionaryState::Active);
        state
            .dictionary_manager
            .upsert_snapshot(&state, snapshot.clone())
            .expect("upsert sample snapshot");

        // Construct a Scan-level TableDef carrying the SAME (db_id, table_id)
        // in ScanSource. Note: state.starrocks_table is empty — no runtime
        // is registered. The lock-free provider must consult ScanSource
        // directly to resolve the owner.
        let table = TableDef {
            name: "t1".to_string(),
            columns: vec![ColumnDef {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 100,
                table_id: 200,
            },
        };

        let provider = DictionaryQueryProvider::new(state);
        let loaded = provider
            .load_active_snapshot(&table, "demo", "s")
            .expect("load_active_snapshot returns Ok");

        assert!(
            loaded.is_some(),
            "lock-free owner_for must resolve identity from ScanSource::StarRocks {{ db_id, table_id }} payload, not from state.starrocks_table",
        );
    }

    /// Iceberg scans now participate in the low-cardinality dictionary rewrite
    /// because Option A (iceberg scan execution-layer dictionary-encode support)
    /// has landed. `DictionaryQueryProvider::owner_for` maps `IcebergDataFiles`
    /// to a `DictionaryOwner::IcebergTable`, so when an Active iceberg
    /// dictionary snapshot exists in the metadata store,
    /// `load_active_snapshot` must return it (not `None`).
    #[test]
    fn dictionary_provider_loads_iceberg_data_files_snapshot() {
        use crate::sql::catalog::{
            ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
        };
        use arrow::datatypes::DataType;
        use std::collections::BTreeMap;
        use std::sync::Arc;

        let (_dir, state) = open_state();
        let state = Arc::new(state);

        // Persist an Active iceberg dictionary snapshot that matches the
        // iceberg table identity used below. If owner_for still mapped
        // IcebergDataFiles to an iceberg owner, this snapshot would be found.
        let owner = DictionaryOwner::IcebergTable {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
        };
        let snapshot = DictionarySnapshot {
            dictionary_id: 99,
            owner,
            column_id: None,
            column_name: "s".to_string(),
            data_type: DataType::Utf8,
            version: 1,
            watermark: DictionaryWatermark::Iceberg {
                snapshot_id: Some(7),
                schema_id: 1,
            },
            values: vec![DictionaryValue {
                id: 1,
                bytes: b"a".to_vec(),
            }],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving: true,
        };
        state
            .dictionary_manager
            .upsert_snapshot(&state, snapshot)
            .expect("upsert iceberg snapshot");

        let iceberg = IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///tmp/test_table".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
        };
        let table = TableDef {
            name: "test_table".to_string(),
            columns: vec![ColumnDef {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg,
                files: vec![],
                cloud_properties: BTreeMap::new(),
            },
        };

        let provider = DictionaryQueryProvider::new(state);
        let loaded = provider
            .load_active_snapshot(&table, "test_db", "s")
            .expect("load_active_snapshot returns Ok");
        let snapshot =
            loaded.expect("iceberg scans support dict execution (Option A); snapshot must load");
        assert_eq!(snapshot.column_name, "s");
        assert_eq!(snapshot.dictionary_id, 99);
    }

    /// Iceberg snapshot-watermark staleness: a dictionary built for snapshot 7
    /// must NOT be used once the table's current snapshot advances (e.g. after
    /// an INSERT), even though the persisted snapshot is still `Active`. This
    /// keeps a post-write query from driving the rewrite off a stale dict
    /// (which would encode the new, uncovered values to the null/0 id).
    #[test]
    fn dictionary_provider_skips_stale_iceberg_snapshot_after_table_advances() {
        use crate::sql::catalog::{
            ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
        };
        use arrow::datatypes::DataType;
        use std::collections::BTreeMap;
        use std::sync::Arc;

        let (_dir, state) = open_state();
        let state = Arc::new(state);

        let owner = DictionaryOwner::IcebergTable {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
        };
        let snapshot = DictionarySnapshot {
            dictionary_id: 99,
            owner,
            column_id: None,
            column_name: "s".to_string(),
            data_type: DataType::Utf8,
            version: 1,
            // Built for snapshot 7.
            watermark: DictionaryWatermark::Iceberg {
                snapshot_id: Some(7),
                schema_id: 1,
            },
            values: vec![DictionaryValue {
                id: 1,
                bytes: b"a".to_vec(),
            }],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving: true,
        };
        state
            .dictionary_manager
            .upsert_snapshot(&state, snapshot)
            .expect("upsert iceberg snapshot");

        let iceberg = IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            // Table has since advanced to snapshot 8 (e.g. an INSERT committed).
            current_snapshot_id: Some(8),
            schema_id: 1,
            location: "file:///tmp/test_table".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
        };
        let table = TableDef {
            name: "test_table".to_string(),
            columns: vec![ColumnDef {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg,
                files: vec![],
                cloud_properties: BTreeMap::new(),
            },
        };

        let provider = DictionaryQueryProvider::new(state);
        let loaded = provider
            .load_active_snapshot(&table, "test_db", "s")
            .expect("load_active_snapshot returns Ok");
        assert!(
            loaded.is_none(),
            "dict built for snapshot 7 must be stale once the table advances to snapshot 8",
        );
    }
}
