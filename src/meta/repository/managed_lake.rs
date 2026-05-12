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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateManagedColumnRequest {
    pub column_name: String,
    pub logical_type: String,
    pub nullable: bool,
    pub visible: bool,
    pub is_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateManagedTableLayoutRequest {
    pub db_id: i64,
    pub table_name: String,
    pub keys_type: String,
    pub bucket_num: i64,
    pub kind: ManagedTableKind,
    pub schema_version: i64,
    pub tablet_schema_pb: Vec<u8>,
    pub columns: Vec<CreateManagedColumnRequest>,
    pub partition_name: String,
    pub warehouse_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedManagedTableLayout {
    pub table: StoredManagedTable,
    pub schema: StoredManagedSchema,
    pub columns: Vec<StoredManagedColumn>,
    pub partition: StoredManagedPartition,
    pub index: StoredManagedIndex,
    pub tablets: Vec<StoredManagedTablet>,
    pub partition_root_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageManagedTruncateRequest {
    pub table_id: i64,
    pub db_id: i64,
    pub bucket_num: i64,
    pub partition_name: String,
    pub warehouse_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedManagedTruncate {
    pub partition_id: i64,
    pub index_id: i64,
    pub tablet_ids: Vec<i64>,
    pub partition_root_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageManagedMvRefreshRequest {
    pub table_id: i64,
    pub db_id: i64,
    pub bucket_num: i64,
    pub partition_name: String,
    pub warehouse_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedManagedMvRefresh {
    pub partition_id: i64,
    pub index_id: i64,
    pub tablet_ids: Vec<i64>,
    pub partition_root_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IdLookup {
    id: i64,
}

impl ManagedLakeMetaRepository {
    pub fn get_or_create_database(
        &self,
        txn: &mut dyn MetaWriteTxn,
        name: &str,
    ) -> RepositoryResult<StoredManagedDatabase> {
        if let Some(database) = self.load_database_by_name(txn, name)? {
            return Ok(database);
        }
        self.create_database(
            txn,
            CreateManagedDatabaseRequest {
                name: name.to_string(),
            },
        )
    }

    pub fn load_database_by_name(
        &self,
        txn: &dyn MetaReadTxn,
        name: &str,
    ) -> RepositoryResult<Option<StoredManagedDatabase>> {
        let Some(record) = txn.get(&key_database_name(name)?)? else {
            return Ok(None);
        };
        let lookup: IdLookup = decode_record_payload(
            &record,
            MANAGED_DATABASE_NAME_KIND,
            MANAGED_DATABASE_NAME_SCHEMA_VERSION,
        )?;
        self.load_database(txn, lookup.id)
    }

    pub fn load_database(
        &self,
        txn: &dyn MetaReadTxn,
        db_id: i64,
    ) -> RepositoryResult<Option<StoredManagedDatabase>> {
        txn.get(&key_database(db_id)?)?
            .map(|record| {
                decode_record_payload(
                    &record,
                    MANAGED_DATABASE_KIND,
                    MANAGED_DATABASE_SCHEMA_VERSION,
                )
            })
            .transpose()
    }

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

    pub fn create_table_layout(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateManagedTableLayoutRequest,
    ) -> RepositoryResult<CreatedManagedTableLayout> {
        if req.bucket_num <= 0 {
            return Err(RepositoryError::invalid(format!(
                "managed table bucket_num must be positive, got {}",
                req.bucket_num
            )));
        }
        self.load_database(txn, req.db_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("managed database {} not found", req.db_id))
        })?;

        let lookup_key = key_table_name(req.db_id, &req.table_name)?;
        if let Some(record) = txn.get(&lookup_key)? {
            let _: IdLookup = decode_record_payload(
                &record,
                MANAGED_TABLE_NAME_KIND,
                MANAGED_TABLE_NAME_SCHEMA_VERSION,
            )?;
            return Err(RepositoryError::conflict(format!(
                "managed table {} already exists",
                req.table_name
            )));
        }

        let table_id = txn.allocate_id(id_scopes::managed_table())?;
        let schema_id = table_id;
        let partition_id = txn.allocate_id(id_scopes::managed_partition())?;
        let index_id = txn.allocate_id(id_scopes::managed_index())?;
        let partition_root_path =
            tablet_root_path(&req.warehouse_uri, req.db_id, table_id, partition_id);

        let table = StoredManagedTable {
            table_id,
            db_id: req.db_id,
            name: req.table_name,
            keys_type: req.keys_type,
            bucket_num: req.bucket_num,
            current_schema_id: schema_id,
            state: ManagedTableState::Active,
            kind: req.kind,
        };
        put_table(txn, &table, ExpectedRevision::NotExists)?;
        txn.put(MetaRecordPut::new(
            lookup_key,
            record_kind(MANAGED_TABLE_NAME_KIND)?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                MANAGED_TABLE_NAME_SCHEMA_VERSION,
                &IdLookup { id: table.table_id },
            )?,
        ))?;

        let schema = StoredManagedSchema {
            schema_id,
            table_id,
            schema_version: req.schema_version,
            tablet_schema_pb: req.tablet_schema_pb,
        };
        put_schema(txn, &schema, ExpectedRevision::NotExists)?;

        let columns = req
            .columns
            .into_iter()
            .enumerate()
            .map(|(ordinal, column)| StoredManagedColumn {
                schema_id,
                ordinal: ordinal as i64,
                column_name: column.column_name,
                logical_type: column.logical_type,
                nullable: column.nullable,
                visible: column.visible,
                is_key: column.is_key,
            })
            .collect::<Vec<_>>();
        for column in &columns {
            put_column(txn, column, ExpectedRevision::NotExists)?;
        }

        let partition = StoredManagedPartition {
            partition_id,
            table_id,
            name: req.partition_name,
            visible_version: 1,
            next_version: 2,
            state: ManagedPartitionState::Active,
        };
        put_partition(txn, &partition, ExpectedRevision::NotExists)?;

        let index = StoredManagedIndex {
            index_id,
            table_id,
            partition_id,
            index_type: "BASE".to_string(),
            state: ManagedIndexState::Active,
        };
        put_index(txn, &index, ExpectedRevision::NotExists)?;

        let mut tablets = Vec::new();
        for bucket_seq in 0..req.bucket_num {
            let tablet = StoredManagedTablet {
                tablet_id: txn.allocate_id(id_scopes::managed_tablet())?,
                partition_id,
                index_id,
                bucket_seq,
                tablet_root_path: partition_root_path.clone(),
            };
            put_tablet(txn, &tablet, ExpectedRevision::NotExists)?;
            tablets.push(tablet);
        }

        Ok(CreatedManagedTableLayout {
            table,
            schema,
            columns,
            partition,
            index,
            tablets,
            partition_root_path,
        })
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

    pub fn update_schema_payload(
        &self,
        txn: &mut dyn MetaWriteTxn,
        schema_id: i64,
        tablet_schema_pb: Vec<u8>,
    ) -> RepositoryResult<()> {
        let Some((revision, mut schema)) = load_versioned_schema(txn, schema_id)? else {
            return Err(RepositoryError::not_found(format!(
                "managed schema {schema_id} not found"
            )));
        };
        schema.tablet_schema_pb = tablet_schema_pb;
        put_schema(txn, &schema, ExpectedRevision::Exact(revision))
    }

    pub fn mark_table_dropping(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
    ) -> RepositoryResult<()> {
        let (table_revision, mut table) =
            self.load_versioned_table(txn, table_id)?.ok_or_else(|| {
                RepositoryError::not_found(format!("managed table {table_id} not found"))
            })?;
        if table.state == ManagedTableState::Dropping {
            return Ok(());
        }
        if table.state != ManagedTableState::Active {
            return Err(RepositoryError::conflict(format!(
                "managed table {table_id} is {:?}, expected Active",
                table.state
            )));
        }
        if self.load_snapshot(txn)?.partitions.iter().any(|partition| {
            partition.table_id == table_id && partition.state == ManagedPartitionState::Creating
        }) {
            return Err(RepositoryError::conflict(format!(
                "cannot drop table {table_id}: refresh in progress"
            )));
        }

        table.state = ManagedTableState::Dropping;
        put_table(txn, &table, ExpectedRevision::Exact(table_revision))?;

        for (revision, mut partition) in self.load_versioned_partitions_for_table(txn, table_id)? {
            if partition.state == ManagedPartitionState::Active {
                partition.state = ManagedPartitionState::Retired;
                put_partition(txn, &partition, ExpectedRevision::Exact(revision))?;
            }
        }
        for (revision, mut index) in self.load_versioned_indexes_for_table(txn, table_id)? {
            if index.state == ManagedIndexState::Active {
                index.state = ManagedIndexState::Retired;
                put_index(txn, &index, ExpectedRevision::Exact(revision))?;
            }
        }
        Ok(())
    }

    pub fn stage_truncate_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: StageManagedTruncateRequest,
    ) -> RepositoryResult<StagedManagedTruncate> {
        if req.bucket_num <= 0 {
            return Err(RepositoryError::invalid(format!(
                "managed table bucket_num must be positive, got {}",
                req.bucket_num
            )));
        }
        let table = self.load_table(txn, req.table_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("managed table {} not found", req.table_id))
        })?;
        if table.state != ManagedTableState::Active {
            return Err(RepositoryError::conflict(format!(
                "managed table {} is {:?}, expected Active",
                req.table_id, table.state
            )));
        }

        let partition_id = txn.allocate_id(id_scopes::managed_partition())?;
        let index_id = txn.allocate_id(id_scopes::managed_index())?;
        let partition_root_path =
            tablet_root_path(&req.warehouse_uri, req.db_id, req.table_id, partition_id);

        let partition = StoredManagedPartition {
            partition_id,
            table_id: req.table_id,
            name: req.partition_name,
            visible_version: 1,
            next_version: 2,
            state: ManagedPartitionState::Creating,
        };
        put_partition(txn, &partition, ExpectedRevision::NotExists)?;

        let index = StoredManagedIndex {
            index_id,
            table_id: req.table_id,
            partition_id,
            index_type: "BASE".to_string(),
            state: ManagedIndexState::Creating,
        };
        put_index(txn, &index, ExpectedRevision::NotExists)?;

        let mut tablet_ids = Vec::new();
        for bucket_seq in 0..req.bucket_num {
            let tablet_id = txn.allocate_id(id_scopes::managed_tablet())?;
            let tablet = StoredManagedTablet {
                tablet_id,
                partition_id,
                index_id,
                bucket_seq,
                tablet_root_path: partition_root_path.clone(),
            };
            put_tablet(txn, &tablet, ExpectedRevision::NotExists)?;
            tablet_ids.push(tablet_id);
        }

        Ok(StagedManagedTruncate {
            partition_id,
            index_id,
            tablet_ids,
            partition_root_path,
        })
    }

    pub fn activate_truncate_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
        old_partition_id: i64,
        new_partition_id: i64,
        new_index_id: i64,
    ) -> RepositoryResult<()> {
        let partitions = self.load_versioned_partitions_for_table(txn, table_id)?;
        let mut saw_old = false;
        let mut saw_new = false;
        for (revision, mut partition) in partitions {
            if partition.partition_id == new_partition_id {
                if partition.state != ManagedPartitionState::Creating {
                    return Err(RepositoryError::conflict(format!(
                        "managed partition {new_partition_id} is {:?}, expected Creating",
                        partition.state
                    )));
                }
                partition.state = ManagedPartitionState::Active;
                partition.visible_version = 1;
                partition.next_version = 2;
                saw_new = true;
                put_partition(txn, &partition, ExpectedRevision::Exact(revision))?;
            } else if partition.partition_id == old_partition_id {
                if partition.state == ManagedPartitionState::Active {
                    partition.state = ManagedPartitionState::Retired;
                    put_partition(txn, &partition, ExpectedRevision::Exact(revision))?;
                }
                saw_old = true;
            }
        }
        if !saw_old {
            return Err(RepositoryError::not_found(format!(
                "managed partition {old_partition_id} not found"
            )));
        }
        if !saw_new {
            return Err(RepositoryError::not_found(format!(
                "managed partition {new_partition_id} not found"
            )));
        }

        let mut saw_new_index = false;
        for (revision, mut index) in self.load_versioned_indexes_for_table(txn, table_id)? {
            if index.index_id == new_index_id {
                if index.state != ManagedIndexState::Creating {
                    return Err(RepositoryError::conflict(format!(
                        "managed index {new_index_id} is {:?}, expected Creating",
                        index.state
                    )));
                }
                index.state = ManagedIndexState::Active;
                saw_new_index = true;
                put_index(txn, &index, ExpectedRevision::Exact(revision))?;
            } else if index.partition_id == old_partition_id
                && index.state == ManagedIndexState::Active
            {
                index.state = ManagedIndexState::Retired;
                put_index(txn, &index, ExpectedRevision::Exact(revision))?;
            }
        }
        if !saw_new_index {
            return Err(RepositoryError::not_found(format!(
                "managed index {new_index_id} not found"
            )));
        }
        Ok(())
    }

    pub fn stage_mv_refresh_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: StageManagedMvRefreshRequest,
    ) -> RepositoryResult<StagedManagedMvRefresh> {
        if req.bucket_num <= 0 {
            return Err(RepositoryError::invalid(format!(
                "managed materialized view bucket_num must be positive, got {}",
                req.bucket_num
            )));
        }
        let table = self.load_table(txn, req.table_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("managed table {} not found", req.table_id))
        })?;
        if table.kind != ManagedTableKind::MaterializedView {
            return Err(RepositoryError::conflict(format!(
                "table {} is not a materialized view",
                req.table_id
            )));
        }
        if table.state != ManagedTableState::Active {
            return Err(RepositoryError::conflict(format!(
                "materialized view {} is {:?}, expected Active",
                req.table_id, table.state
            )));
        }
        if self.load_snapshot(txn)?.partitions.iter().any(|partition| {
            partition.table_id == req.table_id && partition.state == ManagedPartitionState::Creating
        }) {
            return Err(RepositoryError::conflict(format!(
                "cannot refresh materialized view {}: refresh already in progress",
                req.table_id
            )));
        }

        let partition_id = txn.allocate_id(id_scopes::managed_partition())?;
        let index_id = txn.allocate_id(id_scopes::managed_index())?;
        let partition_root_path =
            tablet_root_path(&req.warehouse_uri, req.db_id, req.table_id, partition_id);

        let partition = StoredManagedPartition {
            partition_id,
            table_id: req.table_id,
            name: req.partition_name,
            visible_version: 1,
            next_version: 2,
            state: ManagedPartitionState::Creating,
        };
        put_partition(txn, &partition, ExpectedRevision::NotExists)?;

        let index = StoredManagedIndex {
            index_id,
            table_id: req.table_id,
            partition_id,
            index_type: "BASE".to_string(),
            state: ManagedIndexState::Creating,
        };
        put_index(txn, &index, ExpectedRevision::NotExists)?;

        let mut tablet_ids = Vec::new();
        for bucket_seq in 0..req.bucket_num {
            let tablet_id = txn.allocate_id(id_scopes::managed_tablet())?;
            let tablet = StoredManagedTablet {
                tablet_id,
                partition_id,
                index_id,
                bucket_seq,
                tablet_root_path: partition_root_path.clone(),
            };
            put_tablet(txn, &tablet, ExpectedRevision::NotExists)?;
            tablet_ids.push(tablet_id);
        }

        Ok(StagedManagedMvRefresh {
            partition_id,
            index_id,
            tablet_ids,
            partition_root_path,
        })
    }

    pub fn activate_mv_refresh_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
        old_partition_id: i64,
        new_partition_id: i64,
        new_index_id: i64,
    ) -> RepositoryResult<()> {
        let partitions = self.load_versioned_partitions_for_table(txn, table_id)?;
        let mut saw_old = false;
        let mut saw_new = false;
        for (revision, mut partition) in partitions {
            if partition.partition_id == new_partition_id {
                match partition.state {
                    ManagedPartitionState::Creating => {
                        partition.state = ManagedPartitionState::Active;
                        partition.visible_version = 2;
                        partition.next_version = 3;
                        put_partition(txn, &partition, ExpectedRevision::Exact(revision))?;
                    }
                    ManagedPartitionState::Active => {
                        if partition.visible_version != 2 || partition.next_version != 3 {
                            return Err(RepositoryError::conflict(format!(
                                "managed partition {new_partition_id} active versions are {}/{}, expected 2/3",
                                partition.visible_version, partition.next_version
                            )));
                        }
                    }
                    _ => {
                        return Err(RepositoryError::conflict(format!(
                            "managed partition {new_partition_id} is {:?}, expected Creating",
                            partition.state
                        )));
                    }
                }
                saw_new = true;
            } else if partition.partition_id == old_partition_id {
                if partition.state == ManagedPartitionState::Active {
                    partition.state = ManagedPartitionState::Retired;
                    put_partition(txn, &partition, ExpectedRevision::Exact(revision))?;
                }
                saw_old = true;
            }
        }
        if !saw_old {
            return Err(RepositoryError::not_found(format!(
                "managed partition {old_partition_id} not found"
            )));
        }
        if !saw_new {
            return Err(RepositoryError::not_found(format!(
                "managed partition {new_partition_id} not found"
            )));
        }

        let mut saw_new_index = false;
        for (revision, mut index) in self.load_versioned_indexes_for_table(txn, table_id)? {
            if index.index_id == new_index_id {
                match index.state {
                    ManagedIndexState::Creating => {
                        index.state = ManagedIndexState::Active;
                        saw_new_index = true;
                        put_index(txn, &index, ExpectedRevision::Exact(revision))?;
                    }
                    ManagedIndexState::Active => {
                        saw_new_index = true;
                    }
                    _ => {
                        return Err(RepositoryError::conflict(format!(
                            "managed index {new_index_id} is {:?}, expected Creating",
                            index.state
                        )));
                    }
                }
            } else if index.partition_id == old_partition_id
                && index.state == ManagedIndexState::Active
            {
                index.state = ManagedIndexState::Retired;
                put_index(txn, &index, ExpectedRevision::Exact(revision))?;
            }
        }
        if !saw_new_index {
            return Err(RepositoryError::not_found(format!(
                "managed index {new_index_id} not found"
            )));
        }
        Ok(())
    }

    pub fn delete_creating_partition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        partition_id: i64,
    ) -> RepositoryResult<()> {
        let Some((partition_revision, partition)) =
            self.load_versioned_partition(txn, partition_id)?
        else {
            return Ok(());
        };
        if partition.state != ManagedPartitionState::Creating {
            return Ok(());
        }
        for (revision, tablet) in load_versioned_tablets_for_partition(txn, partition_id)? {
            txn.delete(
                &key_tablet(tablet.tablet_id)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
        for (revision, index) in load_versioned_indexes_for_partition(txn, partition_id)? {
            if index.state == ManagedIndexState::Creating {
                txn.delete(
                    &key_index(index.index_id)?,
                    ExpectedRevision::Exact(revision),
                )?;
            }
        }
        txn.delete(
            &key_partition(partition_id)?,
            ExpectedRevision::Exact(partition_revision),
        )?;
        Ok(())
    }

    pub fn fail_creating_tables(&self, txn: &mut dyn MetaWriteTxn) -> RepositoryResult<Vec<i64>> {
        let mut failed = Vec::new();
        for (revision, mut table) in load_versioned_tables(txn)? {
            if table.state == ManagedTableState::Creating {
                table.state = ManagedTableState::Failed;
                failed.push(table.table_id);
                put_table(txn, &table, ExpectedRevision::Exact(revision))?;
            }
        }
        Ok(failed)
    }

    pub fn delete_all_creating_partitions(
        &self,
        txn: &mut dyn MetaWriteTxn,
    ) -> RepositoryResult<Vec<i64>> {
        let partition_ids = self
            .load_snapshot(txn)?
            .partitions
            .into_iter()
            .filter(|partition| partition.state == ManagedPartitionState::Creating)
            .map(|partition| partition.partition_id)
            .collect::<Vec<_>>();
        for partition_id in &partition_ids {
            self.delete_creating_partition(txn, *partition_id)?;
        }
        Ok(partition_ids)
    }

    pub fn drop_database_entry(
        &self,
        txn: &mut dyn MetaWriteTxn,
        database_name: &str,
    ) -> RepositoryResult<bool> {
        let lookup_key = key_database_name(database_name)?;
        let Some(record) = txn.get(&lookup_key)? else {
            return Ok(false);
        };
        let lookup: IdLookup = decode_record_payload(
            &record,
            MANAGED_DATABASE_NAME_KIND,
            MANAGED_DATABASE_NAME_SCHEMA_VERSION,
        )?;
        let Some(database_record) = txn.get(&key_database(lookup.id)?)? else {
            txn.delete(&lookup_key, ExpectedRevision::Exact(record.revision))?;
            return Ok(true);
        };
        txn.delete(
            &key_database(lookup.id)?,
            ExpectedRevision::Exact(database_record.revision),
        )?;
        txn.delete(&lookup_key, ExpectedRevision::Exact(record.revision))?;
        Ok(true)
    }

    pub fn purge_dropping_table_for_reuse(
        &self,
        txn: &mut dyn MetaWriteTxn,
        db_id: i64,
        table_name: &str,
    ) -> RepositoryResult<Vec<i64>> {
        let target = normalize_lookup_name(table_name);
        let table_ids = self
            .load_snapshot(txn)?
            .tables
            .into_iter()
            .filter(|table| {
                table.db_id == db_id
                    && table.state == ManagedTableState::Dropping
                    && normalize_lookup_name(&table.name) == target
            })
            .map(|table| table.table_id)
            .collect::<Vec<_>>();
        for table_id in &table_ids {
            purge_table_owned_metadata(self, txn, *table_id, false)?;
        }
        Ok(table_ids)
    }

    pub fn purge_retired_table_metadata(
        &self,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
    ) -> RepositoryResult<()> {
        purge_table_owned_metadata(self, txn, table_id, true)
    }

    pub fn purge_retired_partition_metadata(
        &self,
        txn: &mut dyn MetaWriteTxn,
        partition_id: i64,
    ) -> RepositoryResult<()> {
        let Some((partition_revision, partition)) =
            self.load_versioned_partition(txn, partition_id)?
        else {
            return Ok(());
        };
        if partition.state != ManagedPartitionState::Retired {
            return Err(RepositoryError::conflict(format!(
                "cannot purge managed partition {partition_id}: partition is not retired"
            )));
        }
        for (revision, tablet) in load_versioned_tablets_for_partition(txn, partition_id)? {
            txn.delete(
                &key_tablet(tablet.tablet_id)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
        for (revision, index) in load_versioned_indexes_for_partition(txn, partition_id)? {
            txn.delete(
                &key_index(index.index_id)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
        txn.delete(
            &key_partition(partition_id)?,
            ExpectedRevision::Exact(partition_revision),
        )?;
        Ok(())
    }

    pub fn load_table(
        &self,
        txn: &dyn MetaReadTxn,
        table_id: i64,
    ) -> RepositoryResult<Option<StoredManagedTable>> {
        Ok(self
            .load_versioned_table(txn, table_id)?
            .map(|(_, table)| table))
    }

    pub fn load_versioned_table(
        &self,
        txn: &dyn MetaReadTxn,
        table_id: i64,
    ) -> RepositoryResult<Option<(MetaRevision, StoredManagedTable)>> {
        txn.get(&key_table(table_id)?)?
            .map(|record| {
                let revision = record.revision.clone();
                let table = decode_record_payload(
                    &record,
                    MANAGED_TABLE_KIND,
                    MANAGED_TABLE_SCHEMA_VERSION,
                )?;
                Ok((revision, table))
            })
            .transpose()
    }

    fn load_versioned_partitions_for_table(
        &self,
        txn: &dyn MetaReadTxn,
        table_id: i64,
    ) -> RepositoryResult<Vec<(MetaRevision, StoredManagedPartition)>> {
        load_versioned_partitions(txn).map(|partitions| {
            partitions
                .into_iter()
                .filter(|(_, partition)| partition.table_id == table_id)
                .collect()
        })
    }

    fn load_versioned_indexes_for_table(
        &self,
        txn: &dyn MetaReadTxn,
        table_id: i64,
    ) -> RepositoryResult<Vec<(MetaRevision, StoredManagedIndex)>> {
        load_versioned_indexes(txn).map(|indexes| {
            indexes
                .into_iter()
                .filter(|(_, index)| index.table_id == table_id)
                .collect()
        })
    }
}

fn purge_table_owned_metadata(
    repo: &ManagedLakeMetaRepository,
    txn: &mut dyn MetaWriteTxn,
    table_id: i64,
    require_dropping: bool,
) -> RepositoryResult<()> {
    let Some((table_revision, table)) = repo.load_versioned_table(txn, table_id)? else {
        return Ok(());
    };
    if require_dropping && table.state != ManagedTableState::Dropping {
        return Err(RepositoryError::conflict(format!(
            "cannot purge managed table {table_id}: table is not dropping"
        )));
    }

    let schema_ids = load_versioned_schemas(txn)?
        .into_iter()
        .filter(|(_, schema)| schema.table_id == table_id)
        .collect::<Vec<_>>();
    for (revision, column) in load_versioned_columns(txn)? {
        if schema_ids
            .iter()
            .any(|(_, schema)| schema.schema_id == column.schema_id)
        {
            txn.delete(
                &key_column(column.schema_id, column.ordinal)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
    }
    for (revision, schema) in schema_ids {
        txn.delete(
            &key_schema(schema.schema_id)?,
            ExpectedRevision::Exact(revision),
        )?;
    }

    let partition_ids = load_versioned_partitions(txn)?
        .into_iter()
        .filter(|(_, partition)| partition.table_id == table_id)
        .collect::<Vec<_>>();
    for (revision, tablet) in load_versioned_tablets(txn)? {
        if partition_ids
            .iter()
            .any(|(_, partition)| partition.partition_id == tablet.partition_id)
        {
            txn.delete(
                &key_tablet(tablet.tablet_id)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
    }
    for (revision, index) in load_versioned_indexes(txn)? {
        if index.table_id == table_id {
            txn.delete(
                &key_index(index.index_id)?,
                ExpectedRevision::Exact(revision),
            )?;
        }
    }
    for (revision, partition) in partition_ids {
        txn.delete(
            &key_partition(partition.partition_id)?,
            ExpectedRevision::Exact(revision),
        )?;
    }

    delete_table_name_lookup_if_matches(txn, table.db_id, &table.name, table_id)?;
    txn.delete(
        &key_table(table_id)?,
        ExpectedRevision::Exact(table_revision),
    )?;
    Ok(())
}

fn delete_table_name_lookup_if_matches(
    txn: &mut dyn MetaWriteTxn,
    db_id: i64,
    table_name: &str,
    table_id: i64,
) -> RepositoryResult<()> {
    let lookup_key = key_table_name(db_id, table_name)?;
    let Some(record) = txn.get(&lookup_key)? else {
        return Ok(());
    };
    let lookup: IdLookup = decode_record_payload(
        &record,
        MANAGED_TABLE_NAME_KIND,
        MANAGED_TABLE_NAME_SCHEMA_VERSION,
    )?;
    if lookup.id == table_id {
        txn.delete(&lookup_key, ExpectedRevision::Exact(record.revision))?;
    }
    Ok(())
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

fn put_table(
    txn: &mut dyn MetaWriteTxn,
    table: &StoredManagedTable,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_table(table.table_id)?,
        record_kind(MANAGED_TABLE_KIND)?,
        expected,
        encode_json_payload(MANAGED_TABLE_SCHEMA_VERSION, table)?,
    ))?;
    Ok(())
}

fn put_schema(
    txn: &mut dyn MetaWriteTxn,
    schema: &StoredManagedSchema,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_schema(schema.schema_id)?,
        record_kind(MANAGED_SCHEMA_KIND)?,
        expected,
        encode_json_payload(MANAGED_SCHEMA_SCHEMA_VERSION, schema)?,
    ))?;
    Ok(())
}

fn put_column(
    txn: &mut dyn MetaWriteTxn,
    column: &StoredManagedColumn,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_column(column.schema_id, column.ordinal)?,
        record_kind(MANAGED_COLUMN_KIND)?,
        expected,
        encode_json_payload(MANAGED_COLUMN_SCHEMA_VERSION, column)?,
    ))?;
    Ok(())
}

fn put_index(
    txn: &mut dyn MetaWriteTxn,
    index: &StoredManagedIndex,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_index(index.index_id)?,
        record_kind(MANAGED_INDEX_KIND)?,
        expected,
        encode_json_payload(MANAGED_INDEX_SCHEMA_VERSION, index)?,
    ))?;
    Ok(())
}

fn put_tablet(
    txn: &mut dyn MetaWriteTxn,
    tablet: &StoredManagedTablet,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_tablet(tablet.tablet_id)?,
        record_kind(MANAGED_TABLET_KIND)?,
        expected,
        encode_json_payload(MANAGED_TABLET_SCHEMA_VERSION, tablet)?,
    ))?;
    Ok(())
}

fn load_versioned_tables(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredManagedTable)>> {
    scan_versioned_values(
        txn,
        "table",
        MANAGED_TABLE_KIND,
        MANAGED_TABLE_SCHEMA_VERSION,
    )
}

fn load_versioned_schemas(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredManagedSchema)>> {
    scan_versioned_values(
        txn,
        "schema",
        MANAGED_SCHEMA_KIND,
        MANAGED_SCHEMA_SCHEMA_VERSION,
    )
}

fn load_versioned_schema(
    txn: &dyn MetaReadTxn,
    schema_id: i64,
) -> RepositoryResult<Option<(MetaRevision, StoredManagedSchema)>> {
    txn.get(&key_schema(schema_id)?)?
        .map(|record| {
            let revision = record.revision.clone();
            let schema =
                decode_record_payload(&record, MANAGED_SCHEMA_KIND, MANAGED_SCHEMA_SCHEMA_VERSION)?;
            Ok((revision, schema))
        })
        .transpose()
}

fn load_versioned_columns(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredManagedColumn)>> {
    scan_versioned_values(
        txn,
        "column",
        MANAGED_COLUMN_KIND,
        MANAGED_COLUMN_SCHEMA_VERSION,
    )
}

fn load_versioned_partitions(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredManagedPartition)>> {
    scan_versioned_values(
        txn,
        "partition",
        MANAGED_PARTITION_KIND,
        MANAGED_PARTITION_SCHEMA_VERSION,
    )
}

fn load_versioned_indexes(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredManagedIndex)>> {
    scan_versioned_values(
        txn,
        "index",
        MANAGED_INDEX_KIND,
        MANAGED_INDEX_SCHEMA_VERSION,
    )
}

fn load_versioned_indexes_for_partition(
    txn: &dyn MetaReadTxn,
    partition_id: i64,
) -> RepositoryResult<Vec<(MetaRevision, StoredManagedIndex)>> {
    load_versioned_indexes(txn).map(|indexes| {
        indexes
            .into_iter()
            .filter(|(_, index)| index.partition_id == partition_id)
            .collect()
    })
}

fn load_versioned_tablets(
    txn: &dyn MetaReadTxn,
) -> RepositoryResult<Vec<(MetaRevision, StoredManagedTablet)>> {
    scan_versioned_values(
        txn,
        "tablet",
        MANAGED_TABLET_KIND,
        MANAGED_TABLET_SCHEMA_VERSION,
    )
}

fn load_versioned_tablets_for_partition(
    txn: &dyn MetaReadTxn,
    partition_id: i64,
) -> RepositoryResult<Vec<(MetaRevision, StoredManagedTablet)>> {
    load_versioned_tablets(txn).map(|tablets| {
        tablets
            .into_iter()
            .filter(|(_, tablet)| tablet.partition_id == partition_id)
            .collect()
    })
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

fn scan_versioned_values<T>(
    txn: &dyn MetaReadTxn,
    path: &str,
    expected_kind: &str,
    expected_schema_version: i32,
) -> RepositoryResult<Vec<(MetaRevision, T)>>
where
    T: for<'de> Deserialize<'de>,
{
    let prefix = MetaKeyPrefix::new(NS_MANAGED, [path.to_string()])?;
    txn.scan(&prefix, None)?
        .into_iter()
        .map(|record| {
            let revision = record.revision.clone();
            decode_record_payload(&record, expected_kind, expected_schema_version)
                .map(|value| (revision, value))
        })
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

fn key_schema(schema_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        ["schema".to_string(), schema_id.to_string()],
    )?)
}

fn key_column(schema_id: i64, ordinal: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        [
            "column".to_string(),
            schema_id.to_string(),
            ordinal.to_string(),
        ],
    )?)
}

fn key_index(index_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        ["index".to_string(), index_id.to_string()],
    )?)
}

fn key_tablet(tablet_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MANAGED,
        ["tablet".to_string(), tablet_id.to_string()],
    )?)
}

fn tablet_root_path(warehouse_uri: &str, db_id: i64, table_id: i64, partition_id: i64) -> String {
    format!(
        "{}/db_{}/table_{}/partition_{}",
        warehouse_uri.trim_end_matches('/'),
        db_id,
        table_id,
        partition_id
    )
}
