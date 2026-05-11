pub mod error;
pub mod id;
pub mod keys;
pub mod payload;
pub mod provider;
pub mod record;
pub mod repository;
pub mod sqlite;

pub use error::{MetaError, MetaErrorKind};
pub use id::IdScope;
pub use provider::{
    MetaCommitOutcome, MetaReadTxn, MetaStoreCapabilities, MetaStoreProvider, MetaWriteTxn,
};
pub use record::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaPayload, MetaPayloadEncoding, MetaRecord,
    MetaRecordKind, MetaRecordPut, MetaRevision,
};
pub use sqlite::SqliteMetaStoreProvider;
