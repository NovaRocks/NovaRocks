use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DictionaryOwner {
    StarRocksTable {
        database: String,
        table: String,
        db_id: i64,
        table_id: i64,
    },
    IcebergTable {
        catalog: String,
        namespace: String,
        table: String,
        table_uuid: Option<String>,
    },
}

impl DictionaryOwner {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            DictionaryOwner::StarRocksTable { .. } => "starrocks_table",
            DictionaryOwner::IcebergTable { .. } => "iceberg_table",
        }
    }

    pub(crate) fn stable_key(&self) -> String {
        match self {
            DictionaryOwner::StarRocksTable {
                database,
                table,
                db_id,
                table_id,
            } => format!("db={database};table={table};db_id={db_id};table_id={table_id}"),
            DictionaryOwner::IcebergTable {
                catalog,
                namespace,
                table,
                table_uuid,
            } => format!(
                "catalog={catalog};namespace={namespace};table={table};uuid={}",
                table_uuid.as_deref().unwrap_or("")
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DictionaryWatermark {
    StarRocks {
        schema_id: i64,
        tablets: Vec<StarRocksTabletWatermark>,
    },
    Iceberg {
        snapshot_id: Option<i64>,
        schema_id: i32,
    },
}

impl DictionaryWatermark {
    pub(crate) fn stable_json(&self) -> String {
        serde_json::to_string(self).expect("dictionary watermark serializes")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StarRocksTabletWatermark {
    pub(crate) tablet_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) visible_version: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DictionaryState {
    Active,
    Stale,
    Dropped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DictionaryValue {
    pub(crate) id: i32,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct DictionarySnapshot {
    pub(crate) dictionary_id: i64,
    pub(crate) owner: DictionaryOwner,
    pub(crate) column_id: Option<i64>,
    pub(crate) column_name: String,
    pub(crate) data_type: DataType,
    pub(crate) version: i64,
    pub(crate) watermark: DictionaryWatermark,
    pub(crate) values: Vec<DictionaryValue>,
    pub(crate) null_id: i32,
    pub(crate) state: DictionaryState,
    pub(crate) order_preserving: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct QueryDictionarySelection {
    pub(crate) base_dictionaries: BTreeMap<String, DictionarySnapshot>,
}
