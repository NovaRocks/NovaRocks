// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::mem::size_of;
use std::sync::Arc;

use novarocks_spi::connector::read_stack::SchemaTableName;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorResourceReservation};
use paimon::catalog::Identifier;
use paimon::io::{FileIO, RetainedRead};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};

use crate::io::PaimonHostFileIo;
use crate::metadata::{PaimonFrozenRead, freeze_table};
use crate::resources::PaimonRequestResources;
use crate::sdk_control::PaimonSdkReadControl;

pub const MAX_PAIMON_CATALOG_ENTRIES: usize = 65_536;
pub const MAX_PAIMON_CATALOG_LISTING_BYTES: u64 = 16 * 1024 * 1024;

/// A catalog listing that keeps its metadata charge until its caller drops it.
pub struct PaimonCatalogEntries {
    entries: Vec<String>,
    _reservation: ConnectorResourceReservation,
}

impl PaimonCatalogEntries {
    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    /// Build a replacement owner while this listing's metadata reservation is
    /// still live. This prevents an unaccounted interval during identity
    /// materialization.
    pub fn map<T>(self, transform: impl FnOnce(Vec<String>) -> T) -> T {
        let Self {
            entries,
            _reservation,
        } = self;
        let output = transform(entries);
        drop(_reservation);
        output
    }
}

impl std::fmt::Debug for PaimonCatalogEntries {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaimonCatalogEntries")
            .field("entries", &self.entries)
            .finish_non_exhaustive()
    }
}

/// FE-only, read-only filesystem catalog. The raw SDK catalog is never exposed,
/// so callers cannot reach its mutation methods.
#[derive(Clone)]
pub struct PaimonFileSystemCatalog {
    inner: FileSystemCatalog,
    resources: PaimonRequestResources,
}

impl PaimonFileSystemCatalog {
    pub fn try_new(
        warehouse: impl AsRef<str>,
        host_io: PaimonHostFileIo,
        resources: PaimonRequestResources,
    ) -> Result<Self, ConnectorError> {
        resources.checkpoint()?;
        let warehouse = warehouse.as_ref();
        if warehouse.is_empty() || warehouse.len() > 16 * 1024 {
            return Err(invalid(
                "Paimon warehouse location must be non-empty and bounded",
            ));
        }
        let control = PaimonSdkReadControl::new(resources.clone());
        let file_io = FileIO::from_read_only(Arc::new(host_io), Arc::new(control));
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let inner = FileSystemCatalog::with_file_io(options, file_io).map_err(map_sdk_error)?;
        Ok(Self { inner, resources })
    }

    pub fn warehouse(&self) -> &str {
        self.inner.warehouse()
    }

    pub async fn list_databases(&self) -> Result<PaimonCatalogEntries, ConnectorError> {
        self.resources.checkpoint()?;
        let entries = self
            .inner
            .list_databases_retained()
            .await
            .map_err(map_sdk_error)?;
        self.retain_listing(entries)
    }

    pub async fn list_tables(
        &self,
        database: &str,
    ) -> Result<PaimonCatalogEntries, ConnectorError> {
        self.resources.checkpoint()?;
        let entries = self
            .inner
            .list_tables_retained(database)
            .await
            .map_err(map_sdk_error)?;
        self.retain_listing(entries)
    }

    /// Load and freeze one table. Snapshot discovery happens exactly once in
    /// `freeze_table`; later split planning uses only its exact SDK table copy.
    pub async fn prepare_read(
        &self,
        name: &SchemaTableName,
    ) -> Result<Arc<PaimonFrozenRead>, ConnectorError> {
        self.resources.checkpoint()?;
        let identifier = Identifier::new(name.schema_name(), name.table_name());
        let table = self
            .inner
            .get_table(&identifier)
            .await
            .map_err(map_sdk_error)?;
        freeze_table(table, name.clone(), self.resources.clone())
            .await
            .map(Arc::new)
    }

    fn retain_listing(
        &self,
        entries: RetainedRead<Vec<String>>,
    ) -> Result<PaimonCatalogEntries, ConnectorError> {
        entries.try_map(|entries| {
            if entries.len() > MAX_PAIMON_CATALOG_ENTRIES {
                return Err(exhausted("Paimon catalog listing exceeds the entry limit"));
            }
            let bytes = entries.iter().try_fold(
                (entries.len() * size_of::<String>()) as u64,
                |total, entry| {
                    if entry.is_empty() || entry.len() > 1_024 {
                        return Err(invalid("Paimon catalog entry name is invalid or unbounded"));
                    }
                    total
                        .checked_add(entry.len() as u64)
                        .ok_or_else(|| exhausted("Paimon catalog listing size overflow"))
                },
            )?;
            if bytes > MAX_PAIMON_CATALOG_LISTING_BYTES {
                return Err(exhausted("Paimon catalog listing exceeds the byte limit"));
            }
            let reservation = self.resources.reserve_metadata(bytes.max(1))?;
            self.resources.checkpoint()?;
            Ok(PaimonCatalogEntries {
                entries,
                _reservation: reservation,
            })
        })
    }
}

impl std::fmt::Debug for PaimonFileSystemCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaimonFileSystemCatalog")
            .field("warehouse", &self.inner.warehouse())
            .finish_non_exhaustive()
    }
}

pub(crate) fn map_sdk_error(error: paimon::Error) -> ConnectorError {
    if let paimon::Error::UnexpectedError {
        source: Some(source),
        ..
    } = &error
    {
        if let Some(host_error) = source.downcast_ref::<ConnectorError>() {
            return host_error.clone();
        }
        if let Some(file_error) = source.downcast_ref::<novarocks_fs::FileError>() {
            return crate::io::connector_error_from_file_error(file_error);
        }
    }
    let kind = match &error {
        paimon::Error::DatabaseNotExist { .. }
        | paimon::Error::TableNotExist { .. }
        | paimon::Error::ViewNotExist { .. }
        | paimon::Error::FunctionNotExist { .. }
        | paimon::Error::ColumnNotExist { .. } => ConnectorErrorKind::NotFound,
        paimon::Error::Unsupported { .. } | paimon::Error::IoUnsupported { .. } => {
            ConnectorErrorKind::Unsupported
        }
        paimon::Error::DataInvalid { .. }
        | paimon::Error::DataTypeInvalid { .. }
        | paimon::Error::FileIndexFormatInvalid { .. } => ConnectorErrorKind::CorruptData,
        paimon::Error::ConfigInvalid { .. } | paimon::Error::IdentifierInvalid { .. } => {
            ConnectorErrorKind::InvalidRequest
        }
        paimon::Error::IoUnexpected { .. } | paimon::Error::RestApi { .. } => {
            ConnectorErrorKind::Unavailable
        }
        _ => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, format!("Paimon SDK rejected read metadata: {error}"))
}

fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn exhausted(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorRequestResources, ConnectorResourceCheckpoint,
        ConnectorResourceClass, ConnectorResourceLease, ConnectorResourceLedger,
    };
    use paimon::io::{FileStatus, FileStatusStream, ReadControl, ReadOnlyFileIO};

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct LedgerState {
        current: u64,
        limit: u64,
        events: Vec<String>,
    }

    #[derive(Clone, Debug)]
    struct ObservedLedger {
        state: Arc<Mutex<LedgerState>>,
    }

    impl ObservedLedger {
        fn new(limit: u64) -> Self {
            Self {
                state: Arc::new(Mutex::new(LedgerState {
                    current: 0,
                    limit,
                    events: Vec::new(),
                })),
            }
        }

        fn current(&self) -> u64 {
            self.state.lock().unwrap().current
        }

        fn events(&self) -> Vec<String> {
            self.state.lock().unwrap().events.clone()
        }

        fn reserve(&self, class: ConnectorResourceClass, bytes: u64) -> Result<(), ConnectorError> {
            let mut state = self.state.lock().unwrap();
            let next = state
                .current
                .checked_add(bytes)
                .ok_or_else(|| exhausted("test resource accounting overflowed"))?;
            if next > state.limit {
                return Err(exhausted("test resource budget exhausted"));
            }
            state.current = next;
            state.events.push(format!("reserve:{class:?}:{bytes}"));
            Ok(())
        }
    }

    struct ObservedLease {
        ledger: ObservedLedger,
        class: ConnectorResourceClass,
        bytes: u64,
    }

    impl ConnectorResourceLease for ObservedLease {
        fn bytes(&self) -> u64 {
            self.bytes
        }

        fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
            self.ledger.reserve(self.class, additional)?;
            self.bytes += additional;
            Ok(())
        }

        fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
            if bytes > self.bytes {
                return Err(invalid("test resource lease cannot grow through shrink"));
            }
            let released = self.bytes - bytes;
            let mut state = self.ledger.state.lock().unwrap();
            state.current -= released;
            state
                .events
                .push(format!("release:{:?}:{released}", self.class));
            self.bytes = bytes;
            Ok(())
        }
    }

    impl Drop for ObservedLease {
        fn drop(&mut self) {
            let mut state = self.ledger.state.lock().unwrap();
            state.current -= self.bytes;
            state
                .events
                .push(format!("release:{:?}:{}", self.class, self.bytes));
        }
    }

    impl ConnectorResourceLedger for ObservedLedger {
        fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
            Ok(ConnectorResourceCheckpoint::new(0))
        }

        fn try_reserve(
            &self,
            class: ConnectorResourceClass,
            bytes: u64,
        ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
            self.reserve(class, bytes)?;
            Ok(Box::new(ObservedLease {
                ledger: self.clone(),
                class,
                bytes,
            }))
        }
    }

    #[derive(Debug)]
    struct DatabaseListingIo {
        warehouse: String,
        database_path: String,
    }

    #[async_trait::async_trait]
    impl ReadOnlyFileIO for DatabaseListingIo {
        async fn stat(&self, _path: &str) -> paimon::Result<FileStatus> {
            Err(paimon::Error::IoUnsupported {
                message: "test backend has no stat path".to_string(),
            })
        }

        async fn exists(&self, _path: &str) -> paimon::Result<bool> {
            Ok(false)
        }

        async fn read(&self, _path: &str, _range: Range<u64>) -> paimon::Result<Bytes> {
            Err(paimon::Error::IoUnsupported {
                message: "test backend has no read path".to_string(),
            })
        }

        async fn list(&self, path: &str, _recursive: bool) -> paimon::Result<FileStatusStream> {
            let statuses = if path == self.warehouse {
                vec![FileStatus {
                    size: 0,
                    is_dir: true,
                    path: format!("{}/", self.database_path),
                    last_modified: None,
                }]
            } else {
                Vec::new()
            };
            Ok(Box::pin(futures::stream::iter(
                statuses.into_iter().map(Ok),
            )))
        }
    }

    fn observed_catalog(limit: u64) -> (PaimonFileSystemCatalog, ObservedLedger, u64) {
        let warehouse = "s3://bucket/warehouse";
        let database_path = format!("{warehouse}/db.db");
        let sdk_bytes = (std::mem::size_of::<FileStatus>() + database_path.len() + 1) as u64;
        let ledger = ObservedLedger::new(limit);
        let request_resources = ConnectorRequestResources::new(Arc::new(ledger.clone()));
        let resources = PaimonRequestResources::new(
            request_resources,
            Arc::new(NeverCancelled),
            Instant::now() + Duration::from_secs(60),
        );
        let control: Arc<dyn ReadControl> = Arc::new(PaimonSdkReadControl::new(resources.clone()));
        let backend = DatabaseListingIo {
            warehouse: warehouse.to_string(),
            database_path,
        };
        let file_io = FileIO::from_read_only(Arc::new(backend), control);
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let inner = FileSystemCatalog::with_file_io(options, file_io).unwrap();
        (
            PaimonFileSystemCatalog { inner, resources },
            ledger,
            sdk_bytes,
        )
    }

    #[tokio::test]
    async fn catalog_adopts_sdk_listing_before_releasing_sdk_reservation() {
        let (catalog, ledger, _) = observed_catalog(u64::MAX);

        let entries = catalog.list_databases().await.unwrap();
        assert_eq!(entries.entries(), &["db".to_string()]);
        assert!(ledger.current() > 0);
        let events = ledger.events();
        let sdk_reserve = events
            .iter()
            .position(|event| event.starts_with("reserve:ReaderState"))
            .unwrap();
        let provider_reserve = events
            .iter()
            .position(|event| event.starts_with("reserve:Metadata"))
            .unwrap();
        let sdk_release = events
            .iter()
            .position(|event| event.starts_with("release:ReaderState"))
            .unwrap();
        assert!(sdk_reserve < provider_reserve && provider_reserve < sdk_release);

        let identities = entries.map(|entries| {
            assert!(ledger.current() > 0);
            entries
                .into_iter()
                .map(Arc::<str>::from)
                .collect::<Vec<_>>()
        });
        assert_eq!(identities, vec![Arc::<str>::from("db")]);
        assert_eq!(ledger.current(), 0);
    }

    #[tokio::test]
    async fn failed_catalog_adoption_releases_sdk_listing_reservation() {
        let (_, _, sdk_bytes) = observed_catalog(u64::MAX);
        let (catalog, ledger, _) = observed_catalog(sdk_bytes);

        let error = catalog.list_databases().await.unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(ledger.current(), 0);
        let events = ledger.events();
        assert!(
            events
                .iter()
                .any(|event| event.starts_with("release:ReaderState"))
        );
    }
}
