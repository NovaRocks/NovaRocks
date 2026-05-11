use serde::{Deserialize, Serialize};

use crate::meta::keys::NS_JOB;
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_json_payload, encode_json_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaReadTxn, MetaRecord, MetaRecordKind, MetaRecordPut,
    MetaRevision, MetaWriteTxn,
};

const ERASE_JOB_KIND: &str = "job.erase";
const ERASE_JOB_SCHEMA_VERSION: i32 = 1;

#[derive(Default)]
pub struct JobMetaRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredEraseJob {
    pub job_id: i64,
    pub table_id: i64,
    pub partition_id: Option<i64>,
    pub root_path: String,
    pub state: JobState,
    pub retry_at_ms: Option<i64>,
    pub updated_at_ms: i64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobState {
    Pending,
    Running,
    Failed,
    Finished,
}

impl JobState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Failed => "FAILED",
            Self::Finished => "FINISHED",
        }
    }
}

pub struct CreateEraseJobRequest {
    pub table_id: i64,
    pub partition_id: Option<i64>,
    pub root_path: String,
    pub now_ms: i64,
}

impl JobMetaRepository {
    pub fn create_erase_job(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateEraseJobRequest,
    ) -> RepositoryResult<StoredEraseJob> {
        let stored = StoredEraseJob {
            job_id: txn.allocate_id(id_scopes::erase_job())?,
            table_id: req.table_id,
            partition_id: req.partition_id,
            root_path: req.root_path,
            state: JobState::Pending,
            retry_at_ms: None,
            updated_at_ms: req.now_ms,
            last_error: None,
        };
        put_erase_job(txn, &stored, ExpectedRevision::NotExists)?;
        Ok(stored)
    }

    pub fn load_erase_job(
        &self,
        txn: &dyn MetaReadTxn,
        job_id: i64,
    ) -> RepositoryResult<Option<StoredEraseJob>> {
        Ok(load_versioned_erase_job(txn, job_id)?.map(|versioned| versioned.value))
    }

    pub fn claim_erase_job(
        &self,
        txn: &mut dyn MetaWriteTxn,
        job_id: i64,
        now_ms: i64,
    ) -> RepositoryResult<bool> {
        let Some(mut stored) = load_versioned_erase_job(txn, job_id)? else {
            return Ok(false);
        };
        match stored.value.state {
            JobState::Pending | JobState::Failed => {
                stored.value.state = JobState::Running;
                stored.value.retry_at_ms = None;
                stored.value.updated_at_ms = now_ms;
                stored.value.last_error = None;
                put_erase_job(
                    txn,
                    &stored.value,
                    ExpectedRevision::Exact(stored.record_revision),
                )?;
                Ok(true)
            }
            JobState::Running | JobState::Finished => Ok(false),
        }
    }

    pub fn finish_erase_job(
        &self,
        txn: &mut dyn MetaWriteTxn,
        job_id: i64,
        now_ms: i64,
    ) -> RepositoryResult<()> {
        let mut stored = load_required_erase_job(txn, job_id)?;
        let state = stored.value.state.clone();
        match state {
            JobState::Running => {
                stored.value.state = JobState::Finished;
                stored.value.retry_at_ms = None;
                stored.value.updated_at_ms = now_ms;
                stored.value.last_error = None;
                put_erase_job(
                    txn,
                    &stored.value,
                    ExpectedRevision::Exact(stored.record_revision),
                )
            }
            JobState::Pending | JobState::Failed | JobState::Finished => {
                Err(RepositoryError::conflict(format!(
                    "erase job {job_id} is {}, expected {}",
                    state.as_str(),
                    JobState::Running.as_str()
                )))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VersionedEraseJob {
    record_revision: MetaRevision,
    value: StoredEraseJob,
}

fn load_required_erase_job(
    txn: &dyn MetaReadTxn,
    job_id: i64,
) -> RepositoryResult<VersionedEraseJob> {
    load_versioned_erase_job(txn, job_id)?
        .ok_or_else(|| RepositoryError::not_found(format!("erase job {job_id} not found")))
}

fn load_versioned_erase_job(
    txn: &dyn MetaReadTxn,
    job_id: i64,
) -> RepositoryResult<Option<VersionedEraseJob>> {
    txn.get(&key_erase_job(job_id)?)?
        .map(|record| {
            let value = decode_record_payload(&record, ERASE_JOB_KIND, ERASE_JOB_SCHEMA_VERSION)?;
            Ok(VersionedEraseJob {
                record_revision: record.revision,
                value,
            })
        })
        .transpose()
}

fn put_erase_job(
    txn: &mut dyn MetaWriteTxn,
    stored: &StoredEraseJob,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_erase_job(stored.job_id)?,
        record_kind(ERASE_JOB_KIND)?,
        expected,
        encode_json_payload(ERASE_JOB_SCHEMA_VERSION, stored)?,
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

fn key_erase_job(job_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_JOB,
        ["erase".to_string(), job_id.to_string()],
    )?)
}
