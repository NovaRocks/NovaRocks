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

use std::sync::Arc;

use novarocks_spi::connector::read_stack::SchemaTableName;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use paimon::catalog::Identifier;
use paimon::io::FileIO;
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};

use crate::io::PaimonHostFileIo;
use crate::metadata::{PaimonFrozenRead, PaimonFrozenReadRecipe, freeze_table, rebind_table};
use crate::resources::PaimonRequestControl;
use crate::sdk_control::PaimonSdkReadControl;

/// FE-owned catalog entries. The vector owns its elements until materialized or dropped.
pub struct PaimonCatalogEntries {
    entries: Vec<String>,
}

impl PaimonCatalogEntries {
    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    pub fn map<T>(self, transform: impl FnOnce(Vec<String>) -> T) -> T {
        transform(self.entries)
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
    control: PaimonRequestControl,
}

impl PaimonFileSystemCatalog {
    pub fn try_new(
        warehouse: impl AsRef<str>,
        host_io: PaimonHostFileIo,
        control: PaimonRequestControl,
    ) -> Result<Self, ConnectorError> {
        control.checkpoint()?;
        let warehouse = warehouse.as_ref();
        if warehouse.is_empty() {
            return Err(invalid("Paimon warehouse location must be non-empty"));
        }
        let sdk_control = PaimonSdkReadControl::new(control.clone());
        let file_io = FileIO::from_read_only(Arc::new(host_io), Arc::new(sdk_control));
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let inner = FileSystemCatalog::with_file_io(options, file_io).map_err(map_sdk_error)?;
        Ok(Self { inner, control })
    }

    pub fn warehouse(&self) -> &str {
        self.inner.warehouse()
    }

    pub async fn list_databases(&self) -> Result<PaimonCatalogEntries, ConnectorError> {
        self.control.checkpoint()?;
        let entries = self
            .inner
            .list_databases_plain()
            .await
            .map_err(map_sdk_error)?;
        self.retain_listing(entries)
    }

    pub async fn list_tables(
        &self,
        database: &str,
    ) -> Result<PaimonCatalogEntries, ConnectorError> {
        self.control.checkpoint()?;
        let entries = self
            .inner
            .list_tables_plain(database)
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
        self.control.checkpoint()?;
        let identifier = Identifier::new(name.schema_name(), name.table_name());
        let table = self
            .inner
            .get_table(&identifier)
            .await
            .map_err(map_sdk_error)?;
        freeze_table(table, name.clone(), self.control.clone())
            .await
            .map(Arc::new)
    }

    /// Rebind one already-frozen semantic recipe to this request's FileIO and
    /// operation control. No catalog-current or latest-snapshot lookup occurs.
    pub(crate) fn rebind_read(
        &self,
        recipe: &PaimonFrozenReadRecipe,
    ) -> Result<Arc<PaimonFrozenRead>, ConnectorError> {
        rebind_table(self.inner.file_io().clone(), recipe, self.control.clone()).map(Arc::new)
    }

    fn retain_listing(&self, entries: Vec<String>) -> Result<PaimonCatalogEntries, ConnectorError> {
        if entries.iter().any(|entry| entry.is_empty()) {
            return Err(invalid("Paimon catalog entry name is empty"));
        }
        self.control.checkpoint()?;
        Ok(PaimonCatalogEntries { entries })
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

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks_spi::connector::ConnectorErrorKind;
    use paimon::io::{FileIO, FileStatus, FileStatusStream, ReadOnlyFileIO};
    use paimon::{CatalogOptions, FileSystemCatalog, Options};

    use super::*;

    #[derive(Debug)]
    struct DatabaseListingIo {
        warehouse: String,
        database_count: usize,
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

        async fn read(
            &self,
            _path: &str,
            _range: Range<u64>,
            _known_size: Option<u64>,
        ) -> paimon::Result<Bytes> {
            Err(paimon::Error::IoUnsupported {
                message: "test backend has no read path".to_string(),
            })
        }

        async fn list(&self, path: &str, _recursive: bool) -> paimon::Result<FileStatusStream> {
            let count = if path == self.warehouse {
                self.database_count
            } else {
                0
            };
            let warehouse = self.warehouse.clone();
            Ok(Box::pin(futures::stream::iter((0..count).map(
                move |index| {
                    let name = if index == 0 {
                        "db".to_string()
                    } else {
                        format!("db{index}")
                    };
                    Ok(FileStatus {
                        size: 0,
                        is_dir: true,
                        path: format!("{warehouse}/{name}.db/"),
                        last_modified: None,
                    })
                },
            ))))
        }
    }

    fn catalog(
        cancellation: Arc<novarocks_spi::connector::ConnectorStopOwner>,
        database_count: usize,
    ) -> PaimonFileSystemCatalog {
        let warehouse = "s3://bucket/warehouse";
        let control = PaimonRequestControl::new(
            cancellation.view(),
            Instant::now() + Duration::from_secs(60),
        );
        let file_io = FileIO::from_read_only(
            Arc::new(DatabaseListingIo {
                warehouse: warehouse.to_string(),
                database_count,
            }),
            Arc::new(PaimonSdkReadControl::new(control.clone())),
        );
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let inner = FileSystemCatalog::with_file_io(options, file_io).unwrap();
        PaimonFileSystemCatalog { inner, control }
    }

    #[tokio::test]
    async fn fe_listing_is_plain_owned_and_materializes() {
        let catalog = catalog(
            Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
            1,
        );
        let entries = catalog.list_databases().await.unwrap();
        assert_eq!(entries.entries(), &["db".to_string()]);
        let identities = entries.map(|entries| {
            entries
                .into_iter()
                .map(Arc::<str>::from)
                .collect::<Vec<_>>()
        });
        assert_eq!(identities, vec![Arc::<str>::from("db")]);
    }

    #[tokio::test]
    async fn fe_listing_crosses_the_old_entry_limit() {
        let catalog = catalog(
            Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
            65_537,
        );
        let entries = catalog.list_databases().await.unwrap();
        assert_eq!(entries.entries().len(), 65_537);
        assert!(entries.entries().contains(&"db65536".to_string()));
    }

    #[tokio::test]
    async fn fe_listing_still_observes_request_cancellation() {
        let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let catalog = catalog(Arc::clone(&cancellation), 1);
        cancellation.request_stop();
        let error = catalog.list_databases().await.unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    }
}
