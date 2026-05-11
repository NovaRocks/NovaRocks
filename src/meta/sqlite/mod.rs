mod schema;
mod txn;

use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::Connection;

use crate::meta::{MetaError, MetaReadTxn, MetaStoreCapabilities, MetaStoreProvider, MetaWriteTxn};

#[derive(Clone, Debug)]
pub struct SqliteMetaStoreProvider {
    path: PathBuf,
}

impl SqliteMetaStoreProvider {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MetaError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                MetaError::new(
                    crate::meta::MetaErrorKind::Transient,
                    format!(
                        "create metadata provider directory {} failed: {e}",
                        parent.display()
                    ),
                )
            })?;
        }
        let provider = Self { path };
        provider.init_schema()?;
        Ok(provider)
    }

    fn connection(&self) -> Result<Connection, MetaError> {
        let conn = Connection::open(&self.path).map_err(txn::sqlite_error)?;
        conn.busy_timeout(Duration::from_secs(5))
            .map_err(txn::sqlite_error)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(txn::sqlite_error)?;
        Ok(conn)
    }

    fn init_schema(&self) -> Result<(), MetaError> {
        schema::init_schema(&self.connection()?)
    }
}

impl MetaStoreProvider for SqliteMetaStoreProvider {
    fn provider_name(&self) -> &'static str {
        "sqlite"
    }

    fn capabilities(&self) -> MetaStoreCapabilities {
        MetaStoreCapabilities {
            snapshot_read: true,
            atomic_write: true,
            single_writer: true,
            optimistic_concurrency: true,
            monotonic_id_allocation: true,
            commit_unknown_reporting: false,
        }
    }

    fn begin_read(&self) -> Result<Box<dyn MetaReadTxn>, MetaError> {
        Ok(Box::new(txn::SqliteReadTxn::begin(self.connection()?)?))
    }

    fn begin_write(&self, purpose: &str) -> Result<Box<dyn MetaWriteTxn>, MetaError> {
        Ok(Box::new(txn::SqliteWriteTxn::begin(
            self.connection()?,
            purpose,
        )?))
    }
}
