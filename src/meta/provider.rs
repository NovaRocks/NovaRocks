use crate::meta::{
    ExpectedRevision, IdScope, MetaError, MetaKey, MetaKeyPrefix, MetaRecord, MetaRecordPut,
};

pub trait MetaStoreProvider: Send + Sync {
    fn provider_name(&self) -> &'static str;
    fn capabilities(&self) -> MetaStoreCapabilities;
    fn begin_read(&self) -> Result<Box<dyn MetaReadTxn>, MetaError>;
    fn begin_write(&self, purpose: &str) -> Result<Box<dyn MetaWriteTxn>, MetaError>;
}

pub trait MetaReadTxn {
    fn get(&self, key: &MetaKey) -> Result<Option<MetaRecord>, MetaError>;
    fn scan(
        &self,
        prefix: &MetaKeyPrefix,
        limit: Option<usize>,
    ) -> Result<Vec<MetaRecord>, MetaError>;
}

pub trait MetaWriteTxn: MetaReadTxn {
    fn put(&mut self, record: MetaRecordPut) -> Result<(), MetaError>;
    fn delete(&mut self, key: &MetaKey, expected: ExpectedRevision) -> Result<(), MetaError>;
    fn allocate_id(&mut self, scope: IdScope) -> Result<i64, MetaError>;
    fn commit(self: Box<Self>) -> Result<MetaCommitOutcome, MetaError>;
    fn abort(self: Box<Self>) -> Result<(), MetaError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaStoreCapabilities {
    pub snapshot_read: bool,
    pub atomic_write: bool,
    pub single_writer: bool,
    pub optimistic_concurrency: bool,
    pub monotonic_id_allocation: bool,
    pub commit_unknown_reporting: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaCommitOutcome {
    pub provider_revision: Option<i64>,
    pub committed_at_ms: i64,
}
