//! Dictionary rebuild path used by ANALYZE FULL.
//!
//! ANALYZE FULL on a StarRocks or Iceberg table runs the per-column distinct
//! value scan against the standalone query engine and persists the sorted,
//! null-excluded result as an Active dictionary snapshot. ANALYZE SAMPLE does
//! not rebuild dictionaries.

use std::sync::Arc;

use arrow::array::{Array, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
use arrow::datatypes::DataType;

use crate::engine::StandaloneState;
use crate::engine::catalog::normalize_identifier;
use crate::engine::dictionary::model::{
    DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue, DictionaryWatermark,
    StarRocksTabletWatermark,
};
use crate::meta::repository::id_scopes;
use crate::runtime::query_result::QueryResult;
use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};

/// Rebuild active dictionary snapshots for `database.table`'s string-typed
/// columns. Returns the number of snapshots that were persisted. Both
/// StarRocks and Iceberg backends are handled; any other backend (or no
/// metadata provider configured) results in `Ok(0)`.
///
/// This path is best-effort with respect to concurrent writes: a write that
/// commits between the per-column `SELECT DISTINCT` scan and `upsert_snapshot`
/// may leave a freshly-persisted Active snapshot that already observes a
/// pre-write watermark. The next successful write invokes `mark_table_stale`,
/// which flips the snapshot into STALE so subsequent queries see it as
/// missing.
pub(crate) fn rebuild_for_analyze_full(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    columns: Option<&[String]>,
) -> Result<usize, String> {
    if state.metadata_provider.is_none() {
        return Ok(0);
    }

    let table_def = match state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .get(database, table)
    {
        Ok(def) => def,
        Err(_) => return Ok(0),
    };

    let owner = match build_owner(state, database, table, &table_def)? {
        Some(owner) => owner,
        None => return Ok(0),
    };

    let watermark = match &table_def.source {
        ScanSource::StarRocks { .. } => build_starrocks_watermark(state, database, table)?,
        ScanSource::IcebergDataFiles { table: info, .. } => DictionaryWatermark::Iceberg {
            snapshot_id: info.current_snapshot_id,
            schema_id: info.schema_id,
        },
        _ => return Ok(0),
    };

    let selected_columns = select_string_columns(&table_def, columns);
    if selected_columns.is_empty() {
        return Ok(0);
    }

    let mut built = 0_usize;
    for column in selected_columns {
        let distinct = collect_distinct_values(state, database, table, &column)?;
        let dictionary_id = allocate_dictionary_id(state)?;
        let snapshot = DictionarySnapshot {
            dictionary_id,
            owner: owner.clone(),
            column_id: None,
            column_name: column.name.clone(),
            data_type: column.data_type.clone(),
            version: 1,
            watermark: watermark.clone(),
            values: distinct,
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving: true,
        };
        state.dictionary_manager.upsert_snapshot(state, snapshot)?;
        built += 1;
    }
    Ok(built)
}

fn build_owner(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    table_def: &TableDef,
) -> Result<Option<DictionaryOwner>, String> {
    match &table_def.source {
        ScanSource::StarRocks { .. } => {
            let starrocks = state
                .starrocks_table
                .read()
                .expect("standalone StarRocks table read lock");
            let runtime = starrocks.table(database, table)?;
            Ok(Some(DictionaryOwner::StarRocksTable {
                database: runtime.database_name.clone(),
                table: runtime.table.name.clone(),
                db_id: runtime.table.db_id,
                table_id: runtime.table.table_id,
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
        _ => Ok(None),
    }
}

fn build_starrocks_watermark(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
) -> Result<DictionaryWatermark, String> {
    let starrocks = state
        .starrocks_table
        .read()
        .expect("standalone StarRocks table read lock");
    let runtime = starrocks.table(database, table)?;
    let partition_versions: std::collections::HashMap<i64, i64> = runtime
        .partitions
        .iter()
        .map(|partition| (partition.partition_id, partition.visible_version))
        .collect();
    let tablets = runtime
        .tablets
        .iter()
        .map(|tablet| StarRocksTabletWatermark {
            tablet_id: tablet.tablet_id,
            partition_id: tablet.partition_id,
            visible_version: partition_versions
                .get(&tablet.partition_id)
                .copied()
                .unwrap_or(1),
        })
        .collect();
    Ok(DictionaryWatermark::StarRocks {
        schema_id: runtime.table.current_schema_id,
        tablets,
    })
}

fn select_string_columns(table_def: &TableDef, columns: Option<&[String]>) -> Vec<ColumnDef> {
    let allowed: Option<std::collections::HashSet<String>> = columns.map(|cols| {
        cols.iter()
            .filter_map(|raw| normalize_identifier(raw).ok())
            .collect()
    });
    table_def
        .columns
        .iter()
        .filter(|column| is_string_or_binary(&column.data_type))
        .filter(|column| match &allowed {
            None => true,
            Some(set) => normalize_identifier(&column.name)
                .ok()
                .as_ref()
                .map(|name| set.contains(name))
                .unwrap_or(false),
        })
        .cloned()
        .collect()
}

fn is_string_or_binary(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

/// Escape every backtick in `s` by doubling it, matching MySQL's identifier
/// quoting rules. The caller is responsible for wrapping the result in
/// backticks.
fn quote_backticks(s: &str) -> String {
    s.replace('`', "``")
}

/// Issue `SELECT DISTINCT <column> FROM <db>.<table> WHERE <column> IS NOT
/// NULL ORDER BY <column>` against the standalone engine and assign monotonic
/// ids starting at 1 in result order.
fn collect_distinct_values(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    column: &ColumnDef,
) -> Result<Vec<DictionaryValue>, String> {
    let escaped_column = quote_backticks(&column.name);
    let sql = format!(
        "SELECT DISTINCT `{}` FROM `{}`.`{}` WHERE `{}` IS NOT NULL ORDER BY `{}`",
        escaped_column,
        quote_backticks(database),
        quote_backticks(table),
        escaped_column,
        escaped_column,
    );
    let statement = crate::sql::parser::parse_normalized_sql_raw(&sql)
        .map_err(|e| format!("dictionary distinct parse failed: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("dictionary distinct did not parse as query".to_string());
    };
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let result = crate::engine::execute_query(
        &query,
        &catalog_snapshot,
        &connectors_snapshot,
        database,
        state.exchange_port,
        None,
    )?;
    materialize_dictionary_values(&result)
}

fn materialize_dictionary_values(result: &QueryResult) -> Result<Vec<DictionaryValue>, String> {
    let mut values: Vec<DictionaryValue> = Vec::new();
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut next_id: i32 = 1;
    for chunk in &result.chunks {
        let batch = &chunk.batch;
        if batch.num_columns() == 0 {
            continue;
        }
        let array = batch.column(0);
        let bytes_iter = column_to_bytes(array)?;
        for entry in bytes_iter {
            match entry {
                Some(bytes) => {
                    if !seen.insert(bytes.clone()) {
                        continue;
                    }
                    values.push(DictionaryValue { id: next_id, bytes });
                    next_id = next_id.checked_add(1).ok_or_else(|| {
                        "dictionary id overflow while building distinct values".to_string()
                    })?;
                }
                None => continue,
            }
        }
    }
    Ok(values)
}

fn column_to_bytes(array: &arrow::array::ArrayRef) -> Result<Vec<Option<Vec<u8>>>, String> {
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).as_bytes().to_vec())
                }
            })
            .collect());
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).as_bytes().to_vec())
                }
            })
            .collect());
    }
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_vec())
                }
            })
            .collect());
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_vec())
                }
            })
            .collect());
    }
    Err(format!(
        "dictionary distinct result column has unsupported Arrow type {:?}",
        array.data_type()
    ))
}

fn allocate_dictionary_id(state: &Arc<StandaloneState>) -> Result<i64, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "dictionary id allocation requires a metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("allocate dictionary id")
        .map_err(|e| format!("open dictionary id txn failed: {e}"))?;
    let id = txn
        .allocate_id(id_scopes::dictionary_snapshot())
        .map_err(|e| format!("allocate dictionary id failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit dictionary id allocation failed: {e}"))?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::StandaloneState;
    use crate::engine::dictionary::model::DictionaryWatermark;
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

    #[test]
    fn rebuild_returns_zero_when_table_missing() {
        let (_dir, state) = open_state();
        let state = Arc::new(state);
        let count = rebuild_for_analyze_full(&state, "no_db", "no_table", None)
            .expect("rebuild missing table");
        assert_eq!(count, 0);
    }

    #[test]
    fn rebuild_returns_zero_without_metadata_provider() {
        let state = Arc::new(StandaloneState::default());
        let count =
            rebuild_for_analyze_full(&state, "db", "t", None).expect("rebuild without provider");
        assert_eq!(count, 0);
    }

    #[test]
    fn materialize_dictionary_values_assigns_sequential_ids() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};
        use arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let array =
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("batch");
        let chunk = crate::engine::record_batch_to_chunk(batch).expect("chunk");
        let result = QueryResult {
            columns: vec![crate::runtime::query_result::QueryResultColumn {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                logical_type: None,
            }],
            chunks: vec![chunk],
        };
        let values = materialize_dictionary_values(&result).expect("values");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].id, 1);
        assert_eq!(values[0].bytes, b"a");
        assert_eq!(values[1].id, 2);
        assert_eq!(values[1].bytes, b"b");
    }

    #[test]
    fn materialize_dictionary_values_deduplicates_bytes() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};
        use arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let array = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("a"),
            Some("b"),
            Some("c"),
        ])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("batch");
        let chunk = crate::engine::record_batch_to_chunk(batch).expect("chunk");
        let result = QueryResult {
            columns: vec![crate::runtime::query_result::QueryResultColumn {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                logical_type: None,
            }],
            chunks: vec![chunk],
        };

        let values = materialize_dictionary_values(&result).expect("values");

        assert_eq!(values.len(), 3);
        assert_eq!(values[0].id, 1);
        assert_eq!(values[0].bytes, b"a");
        assert_eq!(values[1].id, 2);
        assert_eq!(values[1].bytes, b"b");
        assert_eq!(values[2].id, 3);
        assert_eq!(values[2].bytes, b"c");
    }

    #[test]
    fn empty_state_has_no_snapshots() {
        // A freshly-opened metadata store should return no active snapshot for
        // an arbitrary owner; this guards against accidental snapshots leaking
        // into the store on initialization.
        let (_dir, state) = open_state();
        let manager = &state.dictionary_manager;
        let owner = DictionaryOwner::StarRocksTable {
            database: "demo".to_string(),
            table: "t1".to_string(),
            db_id: 1,
            table_id: 2,
        };
        let snapshot = manager
            .load_active_snapshot(&state, &owner, "s")
            .expect("load");
        assert!(snapshot.is_none());
    }

    #[test]
    fn watermark_round_trips_through_metadata_repo() {
        let (_dir, state) = open_state();
        let manager = &state.dictionary_manager;
        let owner = DictionaryOwner::IcebergTable {
            catalog: "cat".to_string(),
            namespace: "ns".to_string(),
            table: "tbl".to_string(),
            table_uuid: Some("uuid-1".to_string()),
        };
        let snapshot = DictionarySnapshot {
            dictionary_id: 42,
            owner: owner.clone(),
            column_id: None,
            column_name: "s".to_string(),
            data_type: DataType::Utf8,
            version: 1,
            watermark: DictionaryWatermark::Iceberg {
                snapshot_id: Some(7),
                schema_id: 3,
            },
            values: vec![],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving: true,
        };
        manager
            .upsert_snapshot(&state, snapshot.clone())
            .expect("upsert");
        let loaded = manager
            .load_active_snapshot(&state, &owner, "s")
            .expect("load")
            .expect("present");
        match loaded.watermark {
            DictionaryWatermark::Iceberg {
                snapshot_id,
                schema_id,
            } => {
                assert_eq!(snapshot_id, Some(7));
                assert_eq!(schema_id, 3);
            }
            other => panic!("unexpected watermark variant: {other:?}"),
        }
    }
}
