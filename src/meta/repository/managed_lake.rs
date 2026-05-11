use serde::{Deserialize, Serialize};

use crate::meta::keys::{NS_MANAGED, normalize_lookup_name};
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_json_payload, encode_json_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaRevision, MetaWriteTxn,
};

const MANAGED_DATABASE_KIND: &str = "managed.database";
const MANAGED_DATABASE_NAME_KIND: &str = "managed.database_name";
const MANAGED_TABLE_KIND: &str = "managed.table";
const MANAGED_TABLE_NAME_KIND: &str = "managed.table_name";
const MANAGED_SCHEMA_KIND: &str = "managed.schema";
const MANAGED_COLUMN_KIND: &str = "managed.column";
const MANAGED_PARTITION_KIND: &str = "managed.partition";
const MANAGED_INDEX_KIND: &str = "managed.index";
const MANAGED_TABLET_KIND: &str = "managed.tablet";

const MANAGED_DATABASE_SCHEMA_VERSION: i32 = 1;
const MANAGED_DATABASE_NAME_SCHEMA_VERSION: i32 = 1;
const MANAGED_TABLE_SCHEMA_VERSION: i32 = 1;
const MANAGED_TABLE_NAME_SCHEMA_VERSION: i32 = 1;
const MANAGED_SCHEMA_SCHEMA_VERSION: i32 = 1;
const MANAGED_COLUMN_SCHEMA_VERSION: i32 = 1;
const MANAGED_PARTITION_SCHEMA_VERSION: i32 = 1;
const MANAGED_INDEX_SCHEMA_VERSION: i32 = 1;
const MANAGED_TABLET_SCHEMA_VERSION: i32 = 1;

#[derive(Default)]
pub struct ManagedLakeMetaRepository;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManagedLakeSnapshot {
    pub databases: Vec<StoredManagedDatabase>,
    pub tables: Vec<StoredManagedTable>,
    pub schemas: Vec<StoredManagedSchema>,
    pub columns: Vec<StoredManagedColumn>,
    pub partitions: Vec<StoredManagedPartition>,
    pub indexes: Vec<StoredManagedIndex>,
    pub tablets: Vec<StoredManagedTablet>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedDatabase {
    pub db_id: i64,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedTable {
    pub table_id: i64,
    pub db_id: i64,
    pub name: String,
    pub keys_type: String,
    pub bucket_num: i64,
    pub current_schema_id: i64,
    pub state: ManagedTableState,
    pub kind: ManagedTableKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedSchema {
    pub schema_id: i64,
    pub table_id: i64,
    pub schema_version: i64,
    pub tablet_schema_pb: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedColumn {
    pub schema_id: i64,
    pub ordinal: i64,
    pub column_name: String,
    pub logical_type: String,
    pub nullable: bool,
    pub visible: bool,
    pub is_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedPartition {
    pub partition_id: i64,
    pub table_id: i64,
    pub name: String,
    pub visible_version: i64,
    pub next_version: i64,
    pub state: ManagedPartitionState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedIndex {
    pub index_id: i64,
    pub table_id: i64,
    pub partition_id: i64,
    pub index_type: String,
    pub state: ManagedIndexState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedTablet {
    pub tablet_id: i64,
    pub partition_id: i64,
    pub index_id: i64,
    pub bucket_seq: i64,
    pub tablet_root_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ManagedPartitionState {
    Creating,
    Active,
    Retired,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ManagedTableState {
    Creating,
    Active,
    Dropping,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ManagedTableKind {
    Table,
    MaterializedView,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ManagedIndexState {
    Creating,
    Active,
    Retired,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateManagedDatabaseRequest {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateManagedTableRequest {
    pub db_id: i64,
    pub name: String,
    pub keys_type: String,
    pub bucket_num: i64,
    pub current_schema_id: i64,
    pub state: ManagedTableState,
    pub kind: ManagedTableKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IdLookup {
    id: i64,
}

impl ManagedLakeMetaRepository {
    pub fn create_database(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateManagedDatabaseRequest,
    ) -> RepositoryResult<StoredManagedDatabase> {
        let lookup_key = key_database_name(&req.name)?;
        if let Some(record) = txn.get(&lookup_key)? {
            let _: IdLookup = decode_record_payload(
                &record,
                MANAGED_DATABASE_NAME_KIND,
                MANAGED_DATABASE_NAME_SCHEMA_VERSION,
            )?;
            return Err(RepositoryError::conflict(format!(
                "managed database {} already exists",
                req.name
            )));
        }

        let database = StoredManagedDatabase {
            db_id: txn.allocate_id(id_scopes::managed_db())?,
            name: req.name,
        };
        txn.put(MetaRecordPut::new(
            key_database(database.db_id)?,
            record_kind(MANAGED_DATABASE_KIND)?,
            ExpectedRevision::NotExists,
            encode_json_payload(MANAGED_DATABASE_SCHEMA_VERSION, &database)?,
        ))?;
        txn.put(MetaRecordPut::new(
            lookup_key,
            record_kind(MANAGED_DATABASE_NAME_KIND)?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                MANAGED_DATABASE_NAME_SCHEMA_VERSION,
                &IdLookup { id: database.db_id },
            )?,
        ))?;
        Ok(database)
    }

    pub fn create_table(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateManagedTableRequest,
    ) -> RepositoryResult<StoredManagedTable> {
        let lookup_key = key_table_name(req.db_id, &req.name)?;
        if let Some(record) = txn.get(&lookup_key)? {
            let _: IdLookup = decode_record_payload(
                &record,
                MANAGED_TABLE_NAME_KIND,
                MANAGED_TABLE_NAME_SCHEMA_VERSION,
            )?;
            return Err(RepositoryError::conflict(format!(
                "managed table {} already exists",
                req.name
            )));
        }

        let table = StoredManagedTable {
            table_id: txn.allocate_id(id_scopes::managed_table())?,
            db_id: req.db_id,
            name: req.name,
            keys_type: req.keys_type,
            bucket_num: req.bucket_num,
            current_schema_id: req.current_schema_id,
            state: req.state,
            kind: req.kind,
        };
        txn.put(MetaRecordPut::new(
            key_table(table.table_id)?,
            record_kind(MANAGED_TABLE_KIND)?,
            ExpectedRevision::NotExists,
            encode_json_payload(MANAGED_TABLE_SCHEMA_VERSION, &table)?,
        ))?;
        txn.put(MetaRecordPut::new(
            lookup_key,
            record_kind(MANAGED_TABLE_NAME_KIND)?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                MANAGED_TABLE_NAME_SCHEMA_VERSION,
                &IdLookup { id: table.table_id },
            )?,
        ))?;
        Ok(table)
    }

    pub fn create_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
        name: &str,
        visible_version: i64,
    ) -> RepositoryResult<StoredManagedPartition> {
        let partition = StoredManagedPartition {
            partition_id: txn.allocate_id(id_scopes::managed_partition())?,
            table_id,
            name: name.to_string(),
            visible_version,
            next_version: visible_version + 1,
            state: ManagedPartitionState::Active,
        };
        put_partition(txn, &partition, ExpectedRevision::NotExists)?;
        Ok(partition)
    }

    pub fn load_snapshot(&self, txn: &dyn MetaReadTxn) -> RepositoryResult<ManagedLakeSnapshot> {
        let mut snapshot = ManagedLakeSnapshot {
            databases: scan_values(
                txn,
                "database",
                MANAGED_DATABASE_KIND,
                MANAGED_DATABASE_SCHEMA_VERSION,
            )?,
            tables: scan_values(
                txn,
                "table",
                MANAGED_TABLE_KIND,
                MANAGED_TABLE_SCHEMA_VERSION,
            )?,
            schemas: scan_values(
                txn,
                "schema",
                MANAGED_SCHEMA_KIND,
                MANAGED_SCHEMA_SCHEMA_VERSION,
            )?,
            columns: scan_values(
                txn,
                "column",
                MANAGED_COLUMN_KIND,
                MANAGED_COLUMN_SCHEMA_VERSION,
            )?,
            partitions: scan_values(
                txn,
                "partition",
                MANAGED_PARTITION_KIND,
                MANAGED_PARTITION_SCHEMA_VERSION,
            )?,
            indexes: scan_values(
                txn,
                "index",
                MANAGED_INDEX_KIND,
                MANAGED_INDEX_SCHEMA_VERSION,
            )?,
            tablets: scan_values(
                txn,
                "tablet",
                MANAGED_TABLET_KIND,
                MANAGED_TABLET_SCHEMA_VERSION,
            )?,
        };
        snapshot.databases.sort_by_key(|value| value.db_id);
        snapshot.tables.sort_by_key(|value| value.table_id);
        snapshot.schemas.sort_by_key(|value| value.schema_id);
        snapshot
            .columns
            .sort_by_key(|value| (value.schema_id, value.ordinal));
        snapshot.partitions.sort_by_key(|value| value.partition_id);
        snapshot.indexes.sort_by_key(|value| value.index_id);
        snapshot.tablets.sort_by_key(|value| value.tablet_id);
        Ok(snapshot)
    }

    pub fn load_partition(
        &self,
        txn: &dyn MetaReadTxn,
        partition_id: i64,
    ) -> RepositoryResult<Option<StoredManagedPartition>> {
        Ok(self
            .load_versioned_partition(txn, partition_id)?
            .map(|(_, partition)| partition))
    }

    pub fn load_versioned_partition(
        &self,
        txn: &dyn MetaReadTxn,
        partition_id: i64,
    ) -> RepositoryResult<Option<(MetaRevision, StoredManagedPartition)>> {
        txn.get(&key_partition(partition_id)?)?
            .map(|record| {
                let revision = record.revision.clone();
                let partition = decode_record_payload(
                    &record,
                    MANAGED_PARTITION_KIND,
                    MANAGED_PARTITION_SCHEMA_VERSION,
                )?;
                Ok((revision, partition))
            })
            .transpose()
    }

    pub fn update_partition_exact(
        &self,
        txn: &mut dyn MetaWriteTxn,
        partition: &StoredManagedPartition,
        expected: MetaRevision,
    ) -> RepositoryResult<()> {
        put_partition(txn, partition, ExpectedRevision::Exact(expected))
    }
}

fn put_partition(
    txn: &mut dyn MetaWriteTxn,
    partition: &StoredManagedPartition,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_partition(partition.partition_id)?,
        record_kind(MANAGED_PARTITION_KIND)?,
        expected,
        encode_json_payload(MANAGED_PARTITION_SCHEMA_VERSION, partition)?,
    ))?;
    Ok(())
}

fn scan_values<T>(
    txn: &dyn MetaReadTxn,
    path: &str,
    expected_kind: &str,
    expected_schema_version: i32,
) -> RepositoryResult<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let prefix = MetaKeyPrefix::new(NS_MANAGED, [path.to_string()])?;
    txn.scan(&prefix, None)?
        .into_iter()
        .map(|record| decode_record_payload(&record, expected_kind, expected_schema_version))
        .collect()
}

fn decode_record_payload<T>(
    record: &MetaRecord,
    expected_kind: &str,
    expected_schema_version: i32,
) -> RepositoryResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    if record.kind.as_str() != expected_kind {
        return Err(RepositoryError::provider(format!(
            "metadata record {} has kind {}, expected {expected_kind}",
            record.key.canonical_path(),
            record.kind.as_str()
        )));
    }
    if record.payload.schema_version != expected_schema_version {
        return Err(RepositoryError::provider(format!(
            "metadata record {} has schema version {}, expected {expected_schema_version}",
            record.key.canonical_path(),
            record.payload.schema_version
        )));
    }
    decode_json_payload(&record.payload)
}

fn record_kind(value: &str) -> RepositoryResult<MetaRecordKind> {
    Ok(MetaRecordKind::new(value)?)
}

fn key_database(db_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        ["database".to_string(), db_id.to_string()],
    )?)
}

fn key_database_name(name: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        ["database-name".to_string(), normalize_lookup_name(name)],
    )?)
}

fn key_table(table_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        ["table".to_string(), table_id.to_string()],
    )?)
}

fn key_table_name(db_id: i64, name: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        [
            "table-name".to_string(),
            db_id.to_string(),
            normalize_lookup_name(name),
        ],
    )?)
}

fn key_partition(partition_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        ["partition".to_string(), partition_id.to_string()],
    )?)
}
