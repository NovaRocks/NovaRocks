use serde::{Deserialize, Serialize};

use crate::meta::repository::managed_lake::ManagedLakeMetaRepository;
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_json_payload, encode_json_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaReadTxn, MetaRecord, MetaRecordKind, MetaRecordPut,
    MetaRevision, MetaWriteTxn,
};

const MANAGED_TXN_KIND: &str = "managed.txn";
const MANAGED_TXN_SCHEMA_VERSION: i32 = 1;
const NS_MANAGED_TXN: &str = "managed.txn";

#[derive(Default)]
pub struct ManagedLakeTxnRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredManagedTxn {
    pub txn_id: i64,
    pub table_id: i64,
    pub partition_id: i64,
    pub base_version: i64,
    pub commit_version: i64,
    pub state: ManagedTxnState,
    pub retry_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ManagedTxnState {
    Prepared,
    Written,
    Visible,
    Aborted,
}

impl ManagedTxnState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Prepared => "PREPARED",
            Self::Written => "WRITTEN",
            Self::Visible => "VISIBLE",
            Self::Aborted => "ABORTED",
        }
    }
}

impl ManagedLakeTxnRepository {
    pub fn prepare(
        &self,
        meta_repo: &ManagedLakeMetaRepository,
        txn: &mut dyn MetaWriteTxn,
        table_id: i64,
        partition_id: i64,
    ) -> RepositoryResult<StoredManagedTxn> {
        let partition = meta_repo
            .load_partition(txn, partition_id)?
            .ok_or_else(|| {
                RepositoryError::not_found(format!("partition {partition_id} not found"))
            })?;
        if partition.table_id != table_id {
            return Err(RepositoryError::conflict(format!(
                "partition {partition_id} belongs to table {}, expected {table_id}",
                partition.table_id
            )));
        }

        let base_version = partition.visible_version;
        let stored = StoredManagedTxn {
            txn_id: txn.allocate_id(id_scopes::managed_txn())?,
            table_id,
            partition_id,
            base_version,
            commit_version: base_version + 1,
            state: ManagedTxnState::Prepared,
            retry_at_ms: None,
            updated_at_ms: 0,
        };
        put_txn(txn, &stored, ExpectedRevision::NotExists)?;
        Ok(stored)
    }

    pub fn load(
        &self,
        txn: &dyn MetaReadTxn,
        txn_id: i64,
    ) -> RepositoryResult<Option<StoredManagedTxn>> {
        Ok(load_versioned_txn(txn, txn_id)?.map(|versioned| versioned.value))
    }

    pub fn mark_written(&self, txn: &mut dyn MetaWriteTxn, txn_id: i64) -> RepositoryResult<()> {
        let mut stored = load_required_txn(txn, txn_id)?;
        if stored.value.state != ManagedTxnState::Prepared {
            return Err(RepositoryError::conflict(format!(
                "managed txn {txn_id} is {}, expected {}",
                stored.value.state.as_str(),
                ManagedTxnState::Prepared.as_str()
            )));
        }
        stored.value.state = ManagedTxnState::Written;
        put_txn(
            txn,
            &stored.value,
            ExpectedRevision::Exact(stored.record_revision),
        )
    }

    pub fn mark_visible(
        &self,
        meta_repo: &ManagedLakeMetaRepository,
        txn: &mut dyn MetaWriteTxn,
        txn_id: i64,
    ) -> RepositoryResult<()> {
        let mut stored = load_required_txn(txn, txn_id)?;
        if stored.value.state != ManagedTxnState::Written {
            return Err(RepositoryError::conflict(format!(
                "managed txn {txn_id} is {}, expected {}",
                stored.value.state.as_str(),
                ManagedTxnState::Written.as_str()
            )));
        }

        let (partition_revision, mut partition) = meta_repo
            .load_versioned_partition(txn, stored.value.partition_id)?
            .ok_or_else(|| {
                RepositoryError::not_found(format!(
                    "partition {} not found",
                    stored.value.partition_id
                ))
            })?;
        if partition.visible_version != stored.value.base_version {
            return Err(RepositoryError::conflict(format!(
                "partition {} visible version is {}, expected {}",
                stored.value.partition_id, partition.visible_version, stored.value.base_version
            )));
        }

        partition.visible_version = stored.value.commit_version;
        partition.next_version = stored.value.commit_version + 1;
        meta_repo.update_partition_exact(txn, &partition, partition_revision)?;

        stored.value.state = ManagedTxnState::Visible;
        put_txn(
            txn,
            &stored.value,
            ExpectedRevision::Exact(stored.record_revision),
        )
    }

    pub fn mark_aborted(&self, txn: &mut dyn MetaWriteTxn, txn_id: i64) -> RepositoryResult<()> {
        let mut stored = load_required_txn(txn, txn_id)?;
        match stored.value.state {
            ManagedTxnState::Prepared | ManagedTxnState::Written => {
                stored.value.state = ManagedTxnState::Aborted;
                put_txn(
                    txn,
                    &stored.value,
                    ExpectedRevision::Exact(stored.record_revision),
                )
            }
            ManagedTxnState::Aborted => Ok(()),
            ManagedTxnState::Visible => Err(RepositoryError::conflict(format!(
                "managed txn {txn_id} is {}, cannot abort",
                ManagedTxnState::Visible.as_str()
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VersionedManagedTxn {
    record_revision: MetaRevision,
    value: StoredManagedTxn,
}

fn load_required_txn(txn: &dyn MetaReadTxn, txn_id: i64) -> RepositoryResult<VersionedManagedTxn> {
    load_versioned_txn(txn, txn_id)?
        .ok_or_else(|| RepositoryError::not_found(format!("managed txn {txn_id} not found")))
}

fn load_versioned_txn(
    txn: &dyn MetaReadTxn,
    txn_id: i64,
) -> RepositoryResult<Option<VersionedManagedTxn>> {
    txn.get(&key_txn(txn_id)?)?
        .map(|record| {
            let value =
                decode_record_payload(&record, MANAGED_TXN_KIND, MANAGED_TXN_SCHEMA_VERSION)?;
            Ok(VersionedManagedTxn {
                record_revision: record.revision,
                value,
            })
        })
        .transpose()
}

fn put_txn(
    txn: &mut dyn MetaWriteTxn,
    stored: &StoredManagedTxn,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_txn(stored.txn_id)?,
        record_kind(MANAGED_TXN_KIND)?,
        expected,
        encode_json_payload(MANAGED_TXN_SCHEMA_VERSION, stored)?,
    ))?;
    Ok(())
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

fn key_txn(txn_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(NS_MANAGED_TXN, [txn_id.to_string()])?)
}
