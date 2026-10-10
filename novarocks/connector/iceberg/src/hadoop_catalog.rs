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

/// HadoopFileSystemCatalog — a Hadoop-catalog-compatible implementation of the
/// iceberg `Catalog` trait.
///
/// Differences from `MemoryCatalog`:
/// - Metadata files are written as `v{N}.metadata.json` (Hadoop convention).
/// - `version-hint.text` is maintained alongside each metadata directory so
///   that StarRocks FE, Spark, and Trino can discover the current version.
/// - `update_table` manually applies requirements/updates instead of delegating
///   to `TableCommit::apply()`, which calls `MetadataLocation::from_str()` and
///   only accepts the `{version}-{uuid}.metadata.json` format.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{TableMetadata, TableMetadataBuilder};
use crate::iceberg::table::Table;
use crate::iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use async_trait::async_trait;
use bytes::Bytes;
#[cfg(test)]
use novarocks_fs::FileError;
use novarocks_fs::{ConditionalCreateOutcome, FileCancellation, FileErrorKind};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

// A native local conditional create can expose the newly reserved path before
// its payload is fully visible to a competing reader. Retry only the
// authoritative reread; never replay the create request after its fence point.
const AUTHORITATIVE_V1_READ_ATTEMPTS: usize = 3;
const AUTHORITATIVE_V1_READ_RETRY_DELAY: Duration = Duration::from_millis(10);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HadoopCreateAttemptFacts {
    pub(crate) operation_id: String,
    pub(crate) table_uuid: String,
    pub(crate) metadata_location: String,
    pub(crate) metadata_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HadoopCreateDisposition {
    Created,
    Existing,
}

#[derive(Debug)]
pub(crate) struct HadoopCreateResult {
    pub(crate) disposition: HadoopCreateDisposition,
    /// Kept for the fault tests, which assert the published attempt's identity
    /// straight off the client result rather than through the catalog owner.
    #[allow(dead_code)]
    pub(crate) facts: HadoopCreateAttemptFacts,
    pub(crate) authoritative_table_uuid: String,
    pub(crate) authoritative_metadata_digest: String,
    pub(crate) table: Table,
    /// Read by the fault tests only. A finalization failure never downgrades a
    /// commit the client already proved, so no production caller consults it.
    #[allow(dead_code)]
    pub(crate) finalization_failure: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HadoopCreateFailureKind {
    Invalid,
    Unsupported,
    Uncommitted,
    Unknown,
}

#[derive(Debug)]
pub(crate) struct HadoopCreateFailure {
    pub(crate) kind: HadoopCreateFailureKind,
    pub(crate) facts: Option<HadoopCreateAttemptFacts>,
    pub(crate) message: String,
}

#[derive(Debug)]
pub(crate) struct HadoopCreateAttempt {
    ident: TableIdent,
    facts: HadoopCreateAttemptFacts,
    table_location: String,
    metadata: TableMetadata,
    metadata_bytes: Bytes,
}

impl HadoopCreateAttempt {
    pub(crate) fn facts(&self) -> &HadoopCreateAttemptFacts {
        &self.facts
    }
}

#[derive(Debug)]
pub(crate) enum HadoopCreateReconciliation {
    Committed {
        finalization_failure: Option<String>,
    },
    Absent,
    Foreign,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HadoopCatalogTestFault {
    BeforeConditionalRequest,
    AfterConditionalResponseLoss,
    BeforeHintWrite,
    AfterHintWriteResponseLoss,
    AuthoritativeV1Read,
}

#[derive(Debug)]
pub struct HadoopFileSystemCatalog {
    file_io: FileIO,
    warehouse_location: String,
    /// The exact provider-owned storage capability admitted for this catalog.
    /// It is process local and never serialized with the catalog client.
    binding: Option<crate::access_binding::IcebergReadBinding>,
    /// Maps `"namespace/table"` to the current metadata file location.
    tables: Mutex<HashMap<String, String>>,
    #[cfg(test)]
    test_faults: std::sync::Mutex<Vec<HadoopCatalogTestFault>>,
}

impl HadoopFileSystemCatalog {
    /// Compatibility constructor for storage-free tests. Production callers
    /// must use [`Self::new_with_binding`]. Any operation requiring a
    /// provider filesystem capability fails closed when constructed here.
    pub fn new(file_io: FileIO, warehouse_location: String) -> Self {
        Self {
            file_io,
            warehouse_location: warehouse_location.trim_end_matches('/').to_string(),
            binding: None,
            tables: Mutex::new(HashMap::new()),
            #[cfg(test)]
            test_faults: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Create a catalog whose filesystem operations are bound to one admitted
    /// catalog capability. This is the only production constructor.
    pub fn new_with_binding(
        file_io: FileIO,
        warehouse_location: String,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Self {
        Self {
            file_io,
            warehouse_location: warehouse_location.trim_end_matches('/').to_string(),
            binding: Some(binding),
            tables: Mutex::new(HashMap::new()),
            #[cfg(test)]
            test_faults: std::sync::Mutex::new(Vec::new()),
        }
    }

    #[cfg(test)]
    fn inject_test_fault(&self, fault: HadoopCatalogTestFault) {
        self.test_faults
            .lock()
            .expect("Hadoop catalog test fault lock")
            .push(fault);
    }

    #[cfg(test)]
    fn take_test_fault(&self, fault: HadoopCatalogTestFault) -> bool {
        let mut faults = self
            .test_faults
            .lock()
            .expect("Hadoop catalog test fault lock");
        let Some(position) = faults.iter().position(|candidate| *candidate == fault) else {
            return false;
        };
        faults.remove(position);
        true
    }

    // -----------------------------------------------------------------------
    // Path helpers (pub(crate) for unit tests)
    // -----------------------------------------------------------------------

    /// Returns the table root location derived from the warehouse location and
    /// the table identifier, e.g. `oss://bucket/warehouse/ns/table`.
    pub fn table_location(&self, ident: &TableIdent) -> String {
        let namespace = ident.namespace().join("/");
        format!("{}/{}/{}", self.warehouse_location, namespace, ident.name())
    }

    fn namespace_marker_location(&self, namespace: &NamespaceIdent) -> String {
        format!(
            "{}/{}/.novarocks_namespace",
            self.warehouse_location,
            namespace.join("/")
        )
    }

    fn namespace_location(&self, namespace: &NamespaceIdent) -> String {
        format!("{}/{}", self.warehouse_location, namespace.join("/"))
    }

    fn listing_binding(&self) -> Result<crate::access_binding::IcebergReadBinding> {
        self.binding.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::FeatureUnsupported,
                "Bounded Hadoop listing requires the provider-owned filesystem binding",
            )
        })
    }

    fn check_listing_location(&self, namespace: Option<&NamespaceIdent>) -> Result<()> {
        let components = namespace.map(|namespace| namespace.as_ref());
        let bytes = components
            .into_iter()
            .flatten()
            .try_fold(self.warehouse_location.len(), |bytes, component| {
                bytes.checked_add(component.len())?.checked_add(1)
            })
            .ok_or_else(crate::fs_io::listing_refusal)?;
        // Include the longest suffix used by the listing's existence probes.
        crate::fs_io::check_listing_path_size(
            bytes
                .checked_add(32)
                .ok_or_else(crate::fs_io::listing_refusal)?,
        )
    }

    async fn external_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.list_tables_for_read(
            namespace,
            self.listing_binding()?,
            novarocks_spi::connector::ConnectorListingBound::V1,
        )
        .await
    }

    /// Both listing and existence use the same complete directory/probe walk.
    /// Existence keeps only a bool rather than cloning unused TableIdent values.
    async fn scan_external_tables(
        &self,
        namespace: &NamespaceIdent,
        binding: crate::access_binding::IcebergReadBinding,
        bound: novarocks_spi::connector::ConnectorListingBound,
        workspace_bytes: usize,
        retain_tables: bool,
    ) -> Result<(Vec<TableIdent>, bool)> {
        self.check_listing_location(Some(namespace))?;
        let control = binding.operation_control();
        check_catalog_read_control(&control)?;
        let namespace_location = self.namespace_location(namespace);
        let file_io = crate::fs_io::build_bounded_file_io_for_location(
            &namespace_location,
            binding,
            bound,
            workspace_bytes,
        )?;
        let children = file_io.list_directories(&namespace_location).await?;
        check_directory_listing_bound(&children, bound)?;
        let source_bytes = directory_heap_bytes(&children)?;
        let qualifier_bytes = namespace_clone_heap_bytes(namespace)?;
        let mut retained_qualifiers = 0usize;
        let mut tables = Vec::new();
        let mut found = false;
        for table in children {
            check_catalog_read_control(&control)?;
            let path_bytes = namespace_location
                .len()
                .checked_add(table.len())
                .and_then(|bytes| bytes.checked_add(32))
                .ok_or_else(crate::fs_io::listing_refusal)?;
            crate::fs_io::check_listing_path_size(path_bytes)?;
            if file_io
                .exists(Self::version_hint_path(&format!(
                    "{namespace_location}/{table}"
                )))
                .await?
            {
                check_catalog_read_control(&control)?;
                found = true;
                if retain_tables {
                    let next_qualifiers = retained_qualifiers
                        .checked_add(qualifier_bytes)
                        .ok_or_else(crate::fs_io::listing_refusal)?;
                    let other_bytes = source_bytes
                        .checked_add(next_qualifiers)
                        .ok_or_else(crate::fs_io::listing_refusal)?;
                    crate::fs_io::grow_listing_vec(
                        &mut tables,
                        other_bytes,
                        workspace_bytes,
                        bound.entries,
                    )?;
                    tables.push(TableIdent::new(namespace.clone(), table));
                    retained_qualifiers = next_qualifiers;
                }
            }
        }
        tables.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        tables.dedup();
        check_catalog_read_control(&control)?;
        Ok((tables, found))
    }

    async fn list_namespaces_bounded(
        &self,
        parent: Option<&NamespaceIdent>,
        binding: crate::access_binding::IcebergReadBinding,
        bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<NamespaceIdent>> {
        self.check_listing_location(parent)?;
        let workspace_bytes = hadoop_listing_workspace();
        let control = binding.operation_control();
        check_catalog_read_control(&control)?;
        let location = parent
            .map(|parent| self.namespace_location(parent))
            .unwrap_or_else(|| self.warehouse_location.clone());
        let file_io = crate::fs_io::build_bounded_file_io_for_location(
            &location,
            binding.clone(),
            bound,
            workspace_bytes,
        )?;
        let children = file_io.list_directories(&location).await?;
        check_directory_listing_bound(&children, bound)?;
        let source_bytes = directory_heap_bytes(&children)?;
        let parent_bytes = parent
            .map(namespace_clone_heap_bytes)
            .transpose()?
            .unwrap_or(0);
        let mut namespace_bytes = 0usize;
        let mut namespaces = Vec::new();
        for child in children {
            check_catalog_read_control(&control)?;
            if child.starts_with('.') {
                continue;
            }
            let next_namespace_bytes = namespace_bytes
                .checked_add(parent_bytes)
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<String>()))
                .ok_or_else(crate::fs_io::listing_refusal)?;
            let other_bytes = source_bytes
                .checked_add(next_namespace_bytes)
                .ok_or_else(crate::fs_io::listing_refusal)?;
            crate::fs_io::grow_listing_vec(
                &mut namespaces,
                other_bytes,
                workspace_bytes,
                bound.entries,
            )?;
            let namespace = match parent {
                Some(parent) => {
                    // Build the exact component backing after its whole request
                    // was admitted; to_vec followed by push could grow twice.
                    let mut components = Vec::with_capacity(parent.as_ref().len() + 1);
                    components.extend(parent.as_ref().iter().cloned());
                    components.push(child);
                    NamespaceIdent::from_vec(components)?
                }
                None => NamespaceIdent::new(child),
            };
            let held_bytes = namespaces
                .capacity()
                .checked_mul(std::mem::size_of::<NamespaceIdent>())
                .and_then(|slots| other_bytes.checked_add(slots))
                .ok_or_else(crate::fs_io::listing_refusal)?;
            let remaining = workspace_bytes
                .checked_sub(held_bytes)
                .ok_or_else(crate::fs_io::listing_refusal)?;
            if self
                .namespace_exists_bounded(&namespace, binding.clone(), bound, remaining)
                .await?
            {
                namespaces.push(namespace);
                namespace_bytes = next_namespace_bytes;
            }
        }
        namespaces.sort_unstable();
        namespaces.dedup();
        check_catalog_read_control(&control)?;
        Ok(namespaces)
    }

    /// Refuse the complete root source before probing any child. The internal
    /// existence source uses V1 and the workspace left by its parent.
    pub(crate) async fn list_namespaces_for_read(
        &self,
        binding: crate::access_binding::IcebergReadBinding,
        bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<NamespaceIdent>> {
        self.list_namespaces_bounded(None, binding, bound).await
    }

    async fn namespace_exists_bounded(
        &self,
        namespace: &NamespaceIdent,
        binding: crate::access_binding::IcebergReadBinding,
        bound: novarocks_spi::connector::ConnectorListingBound,
        workspace_bytes: usize,
    ) -> Result<bool> {
        self.check_listing_location(Some(namespace))?;
        let control = binding.operation_control();
        check_catalog_read_control(&control)?;
        let file_io = crate::fs_io::build_bounded_file_io_for_location(
            &self.warehouse_location,
            binding.clone(),
            bound,
            workspace_bytes,
        )?;
        let exists = file_io
            .exists(self.namespace_marker_location(namespace))
            .await?;
        check_catalog_read_control(&control)?;
        if exists {
            return Ok(true);
        }
        // The caller's bound describes the namespace output, not the table
        // directories needed to prove that this namespace exists. The latter
        // are a separate source under V1 and the same remaining workspace.
        self.scan_external_tables(
            namespace,
            binding,
            novarocks_spi::connector::ConnectorListingBound::V1,
            workspace_bytes,
            false,
        )
        .await
        .map(|(_, found)| found)
    }

    pub(crate) async fn namespace_exists_for_read(
        &self,
        namespace: &NamespaceIdent,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<bool> {
        self.namespace_exists_bounded(
            namespace,
            binding,
            novarocks_spi::connector::ConnectorListingBound::V1,
            hadoop_listing_workspace(),
        )
        .await
    }

    pub(crate) async fn list_tables_for_read(
        &self,
        namespace: &NamespaceIdent,
        binding: crate::access_binding::IcebergReadBinding,
        bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<TableIdent>> {
        self.scan_external_tables(namespace, binding, bound, hadoop_listing_workspace(), true)
            .await
            .map(|(tables, _)| tables)
    }

    pub(crate) async fn table_exists_for_read(
        &self,
        table: &TableIdent,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<bool> {
        match self.load_table_for_read(table, binding).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == ErrorKind::TableNotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Returns the path to the `vN.metadata.json` file for a given table location
    /// and version number.
    pub fn metadata_path(table_location: &str, version: u32) -> String {
        let base = table_location.trim_end_matches('/');
        format!("{}/metadata/v{}.metadata.json", base, version)
    }

    /// Returns the path to the `version-hint.text` file for a given table location.
    pub fn version_hint_path(table_location: &str) -> String {
        let base = table_location.trim_end_matches('/');
        format!("{}/metadata/version-hint.text", base)
    }

    /// Read the current version stored in `version-hint.text`. Returns `0` if
    /// the file does not exist or cannot be parsed.
    async fn read_version_hint(&self, table_location: &str) -> u32 {
        Self::read_version_hint_with_io(&self.file_io, table_location).await
    }

    async fn read_version_hint_with_io(file_io: &FileIO, table_location: &str) -> u32 {
        let path = Self::version_hint_path(table_location);
        let Ok(input) = file_io.new_input(&path) else {
            return 0;
        };
        let Ok(bytes) = input.read().await else {
            return 0;
        };
        let s = String::from_utf8_lossy(&bytes);
        s.trim().parse::<u32>().unwrap_or(0)
    }

    /// Write `version-hint.text` with the given version number.
    async fn write_version_hint(&self, table_location: &str, version: u32) -> Result<()> {
        #[cfg(test)]
        if self.take_test_fault(HadoopCatalogTestFault::BeforeHintWrite) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "injected failure before Hadoop version-hint write",
            ));
        }
        let path = Self::version_hint_path(table_location);
        let output = self.file_io.new_output(&path)?;
        let result = output.write(format!("{}\n", version).into()).await;
        #[cfg(test)]
        if result.is_ok()
            && self.take_test_fault(HadoopCatalogTestFault::AfterHintWriteResponseLoss)
        {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "injected response loss after Hadoop version-hint write",
            ));
        }
        result
    }

    /// Persist table metadata at `v{version}.metadata.json` and update
    /// `version-hint.text`.
    async fn write_metadata(
        &self,
        table_location: &str,
        metadata: &TableMetadata,
        version: u32,
    ) -> Result<String> {
        let metadata_path = Self::metadata_path(table_location, version);
        metadata
            .write_to(&self.file_io, &metadata_path)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("write metadata to {}: {}", metadata_path, e),
                )
            })?;
        self.write_version_hint(table_location, version).await?;
        Ok(metadata_path)
    }

    /// Build a `Table` value from metadata and a metadata location.
    fn build_table(
        &self,
        ident: TableIdent,
        metadata: TableMetadata,
        metadata_location: String,
    ) -> Result<Table> {
        Table::builder()
            .file_io(self.file_io.clone())
            .metadata(Arc::new(metadata))
            .identifier(ident)
            .metadata_location(metadata_location)
            .build()
    }

    /// Use one request-bound FileIO for every read needed to resolve the
    /// catalog pointer and metadata. The generation's FileIO remains reusable
    /// by later requests and by mutation recovery.
    pub(crate) async fn load_table_for_read(
        &self,
        table: &TableIdent,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<Table> {
        let control = binding.operation_control();
        let check_active = || -> Result<()> {
            if let Some(control) = &control {
                control.check_active().map_err(|error| {
                    Error::new(ErrorKind::Unexpected, "Hadoop catalog read stopped")
                        .with_source(error)
                })?;
            }
            Ok(())
        };
        check_active()?;
        let table_location = self.table_location(table);
        let file_io = crate::fs_io::build_file_io_for_location(&table_location, binding);
        self.load_table_using_io(table, file_io, check_active, false)
            .await
    }

    pub(crate) async fn load_table_for_commit(
        &self,
        table: &TableIdent,
        file_io: FileIO,
    ) -> Result<Table> {
        self.load_table_using_io(table, file_io, || Ok(()), true)
            .await
    }

    async fn load_table_using_io<F>(
        &self,
        table: &TableIdent,
        file_io: FileIO,
        check_active: F,
        strict_hint_read: bool,
    ) -> Result<Table>
    where
        F: Fn() -> Result<()> + Send + Sync,
    {
        check_active()?;
        let table_location = self.table_location(table);
        let version = if strict_hint_read {
            let path = Self::version_hint_path(&table_location);
            if file_io.exists(&path).await? {
                let bytes = file_io.new_input(&path)?.read().await?;
                String::from_utf8_lossy(&bytes)
                    .trim()
                    .parse::<u32>()
                    .unwrap_or(0)
            } else {
                0
            }
        } else {
            Self::read_version_hint_with_io(&file_io, &table_location).await
        };
        check_active()?;
        let metadata_location = if version == 0 {
            let v1 = Self::metadata_path(&table_location, 1);
            if !file_io.exists(&v1).await? {
                check_active()?;
                return Err(Error::new(
                    ErrorKind::TableNotFound,
                    format!("table not found: {}", Self::table_key(table)),
                ));
            }
            v1
        } else {
            let hinted = Self::metadata_path(&table_location, version);
            if !file_io.exists(&hinted).await? {
                check_active()?;
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Hadoop catalog version hint points to missing metadata: {hinted}"),
                ));
            }
            hinted
        };
        check_active()?;
        let metadata = TableMetadata::read_from(&file_io, &metadata_location)
            .await
            .map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("read metadata from {metadata_location}: {error}"),
                )
                .with_source(error)
            })?;
        check_active()?;
        Table::builder()
            .file_io(file_io)
            .metadata(Arc::new(metadata))
            .identifier(table.clone())
            .metadata_location(metadata_location)
            .build()
    }

    /// Return the table key used as the key in the `tables` map.
    fn table_key(ident: &TableIdent) -> String {
        let namespace = ident.namespace().join("/");
        format!("{}/{}", namespace, ident.name())
    }

    /// Read the current version hint from the filesystem and, if valid, insert
    /// the resolved metadata location into the in-memory cache.
    ///
    /// Returns `Some(metadata_location)` when the table exists on disk, or
    /// `None` when `version-hint.text` is absent or unparseable.  Both
    /// `load_table` and `table_exists` delegate to this helper so that every
    /// filesystem probe also populates the cache, making subsequent calls
    /// cache-hit fast.
    async fn try_cache_existing_table(&self, table: &TableIdent) -> Result<Option<String>> {
        let table_location = self.table_location(table);
        let version = self.read_version_hint(&table_location).await;
        let metadata_location = if version == 0 {
            let v1 = Self::metadata_path(&table_location, 1);
            if !self.file_io.exists(&v1).await? {
                return Ok(None);
            }
            TableMetadata::read_from(&self.file_io, &v1)
                .await
                .map_err(|error| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("read canonical Hadoop table metadata {v1}: {error}"),
                    )
                })?;
            if let Err(error) = self.write_version_hint(&table_location, 1).await {
                tracing::warn!(
                    "failed to repair Hadoop catalog version hint from canonical v1 metadata: {error}"
                );
            }
            v1
        } else {
            let hinted = Self::metadata_path(&table_location, version);
            if !self.file_io.exists(&hinted).await? {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Hadoop catalog version hint points to missing metadata: {hinted}"),
                ));
            }
            TableMetadata::read_from(&self.file_io, &hinted)
                .await
                .map_err(|error| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("read hinted Hadoop table metadata {hinted}: {error}"),
                    )
                })?;
            hinted
        };
        let key = Self::table_key(table);
        self.tables
            .lock()
            .await
            .insert(key, metadata_location.clone());
        Ok(Some(metadata_location))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Create-attempt errors are propagated unchanged to preserve the catalog error contract."
    )]
    pub(crate) fn prepare_create_attempt(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
        operation_id: String,
    ) -> std::result::Result<HadoopCreateAttempt, HadoopCreateFailure> {
        let ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let table_location = creation
            .location
            .clone()
            .unwrap_or_else(|| self.table_location(&ident));
        let creation_with_location = TableCreation {
            location: Some(table_location.clone()),
            ..creation
        };
        let build_result = TableMetadataBuilder::from_table_creation(creation_with_location)
            .map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Invalid,
                facts: None,
                message: format!("build metadata from creation: {error}"),
            })?
            .build()
            .map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Invalid,
                facts: None,
                message: format!("build metadata: {error}"),
            })?;
        let metadata = build_result.metadata;
        let metadata_bytes =
            serde_json::to_vec(&metadata).map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Invalid,
                facts: None,
                message: format!("serialize Hadoop table metadata: {error}"),
            })?;
        let metadata_location = Self::metadata_path(&table_location, 1);
        let facts = HadoopCreateAttemptFacts {
            operation_id,
            table_uuid: metadata.uuid().to_string(),
            metadata_location,
            metadata_digest: hex_digest(&metadata_bytes),
        };
        Ok(HadoopCreateAttempt {
            ident,
            facts,
            table_location,
            metadata,
            metadata_bytes: metadata_bytes.into(),
        })
    }

    pub(crate) async fn create_table_fenced(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
        operation_id: String,
    ) -> std::result::Result<HadoopCreateResult, HadoopCreateFailure> {
        let attempt = self.prepare_create_attempt(namespace, creation, operation_id)?;
        self.publish_create_attempt(attempt).await
    }

    pub(crate) async fn publish_create_attempt(
        &self,
        attempt: HadoopCreateAttempt,
    ) -> std::result::Result<HadoopCreateResult, HadoopCreateFailure> {
        let binding = self.binding.as_ref().ok_or_else(|| HadoopCreateFailure {
            kind: HadoopCreateFailureKind::Invalid,
            facts: Some(attempt.facts.clone()),
            message: "Hadoop catalog has no admitted filesystem capability".to_string(),
        })?;
        let access =
            crate::fs_io::resolve_access_for_location(&attempt.facts.metadata_location, binding)
                .map_err(|message| HadoopCreateFailure {
                    kind: HadoopCreateFailureKind::Invalid,
                    facts: Some(attempt.facts.clone()),
                    message: format!("resolve Hadoop metadata fence: {message}"),
                })?;

        // Capability validation precedes directory creation so an unsupported
        // binding fails before any metadata-side storage mutation.
        if !access.supports_conditional_create() {
            return Err(HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Unsupported,
                facts: Some(attempt.facts.clone()),
                message: "Hadoop catalog storage does not support native conditional create"
                    .to_string(),
            });
        }
        access
            .ensure_parent_directory()
            .await
            .map_err(|message| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Uncommitted,
                facts: Some(attempt.facts.clone()),
                message: format!("create Hadoop metadata directory: {message}"),
            })?;

        #[cfg(test)]
        if self.take_test_fault(HadoopCatalogTestFault::BeforeConditionalRequest) {
            return Err(HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Uncommitted,
                facts: Some(attempt.facts.clone()),
                message: "injected failure before Hadoop conditional create request".to_string(),
            });
        }

        let cancellation = FileCancellation::new();
        let conditional = access
            .handle()
            .create_if_absent(0, attempt.metadata_bytes.clone(), &cancellation)
            .await;
        #[cfg(test)]
        let conditional = match conditional {
            Ok(ConditionalCreateOutcome::Created)
                if self.take_test_fault(HadoopCatalogTestFault::AfterConditionalResponseLoss) =>
            {
                Err(FileError::new(
                    FileErrorKind::Transient,
                    "injected response loss after Hadoop conditional create",
                ))
            }
            result => result,
        };
        let (disposition, metadata, authoritative_metadata_digest) = match conditional {
            Ok(ConditionalCreateOutcome::Created) => (
                HadoopCreateDisposition::Created,
                attempt.metadata.clone(),
                attempt.facts.metadata_digest.clone(),
            ),
            Ok(ConditionalCreateOutcome::AlreadyExists) => {
                self.classify_existing_v1(&attempt).await?
            }
            Err(error) if error.kind() == FileErrorKind::Unsupported => {
                return Err(HadoopCreateFailure {
                    kind: HadoopCreateFailureKind::Unsupported,
                    facts: Some(attempt.facts.clone()),
                    message: error.to_string(),
                });
            }
            Err(error) => match self.classify_existing_v1(&attempt).await {
                Ok(classified) => classified,
                Err(_) => {
                    return Err(HadoopCreateFailure {
                        kind: HadoopCreateFailureKind::Unknown,
                        facts: Some(attempt.facts.clone()),
                        message: format!(
                            "conditionally create Hadoop v1 metadata and authoritative reread failed: {error}"
                        ),
                    });
                }
            },
        };

        let mut finalization_failure = None;
        if disposition == HadoopCreateDisposition::Created
            && let Err(error) = self.write_version_hint(&attempt.table_location, 1).await
        {
            finalization_failure = Some(format!(
                "publish Hadoop catalog version hint after committed v1 metadata: {error}"
            ));
        }

        let key = Self::table_key(&attempt.ident);
        self.tables
            .lock()
            .await
            .insert(key, attempt.facts.metadata_location.clone());
        let table = self
            .build_table(
                attempt.ident,
                metadata,
                attempt.facts.metadata_location.clone(),
            )
            .map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Unknown,
                facts: Some(attempt.facts.clone()),
                message: format!("build committed Hadoop table: {error}"),
            })?;
        Ok(HadoopCreateResult {
            disposition,
            facts: attempt.facts,
            authoritative_table_uuid: table.metadata().uuid().to_string(),
            authoritative_metadata_digest,
            table,
            finalization_failure,
        })
    }

    async fn classify_existing_v1(
        &self,
        attempt: &HadoopCreateAttempt,
    ) -> std::result::Result<(HadoopCreateDisposition, TableMetadata, String), HadoopCreateFailure>
    {
        #[cfg(test)]
        if self.take_test_fault(HadoopCatalogTestFault::AuthoritativeV1Read) {
            return Err(HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Unknown,
                facts: Some(attempt.facts.clone()),
                message: "injected authoritative Hadoop v1 reread failure".to_string(),
            });
        }
        for read_attempt in 0..AUTHORITATIVE_V1_READ_ATTEMPTS {
            let read_result = async {
                let input = self.file_io.new_input(&attempt.facts.metadata_location)?;
                let bytes = input.read().await?;
                let metadata = serde_json::from_slice::<TableMetadata>(&bytes)?;
                Ok::<_, Error>((metadata, hex_digest(&bytes)))
            }
            .await;
            match read_result {
                Ok((metadata, authoritative_digest)) => {
                    let same_owner = metadata.uuid().to_string() == attempt.facts.table_uuid
                        && authoritative_digest == attempt.facts.metadata_digest;
                    return Ok((
                        if same_owner {
                            HadoopCreateDisposition::Created
                        } else {
                            HadoopCreateDisposition::Existing
                        },
                        metadata,
                        authoritative_digest,
                    ));
                }
                Err(error) if read_attempt + 1 < AUTHORITATIVE_V1_READ_ATTEMPTS => {
                    tokio::time::sleep(AUTHORITATIVE_V1_READ_RETRY_DELAY).await;
                    let _ = error;
                }
                Err(error) => {
                    return Err(HadoopCreateFailure {
                        kind: HadoopCreateFailureKind::Unknown,
                        facts: Some(attempt.facts.clone()),
                        message: format!("read authoritative Hadoop v1 metadata: {error}"),
                    });
                }
            }
        }
        unreachable!("authoritative reread either succeeds or returns its final failure")
    }

    pub(crate) async fn reconcile_create_attempt(
        &self,
        namespace: &str,
        table: &str,
        expected_uuid: &str,
        expected_metadata_location: &str,
        expected_metadata_digest: &str,
    ) -> std::result::Result<HadoopCreateReconciliation, String> {
        let ident = TableIdent::from_strs([namespace, table])
            .map_err(|error| format!("build Hadoop table identity: {error}"))?;
        let table_location = self.table_location(&ident);
        let canonical_v1 = Self::metadata_path(&table_location, 1);
        if canonical_v1 != expected_metadata_location {
            return Ok(HadoopCreateReconciliation::Foreign);
        }
        if !self
            .file_io
            .exists(&canonical_v1)
            .await
            .map_err(|error| format!("probe authoritative Hadoop v1 metadata: {error}"))?
        {
            return Ok(HadoopCreateReconciliation::Absent);
        }
        let bytes = self
            .file_io
            .new_input(&canonical_v1)
            .map_err(|error| format!("open authoritative Hadoop v1 metadata: {error}"))?
            .read()
            .await
            .map_err(|error| format!("read authoritative Hadoop v1 metadata: {error}"))?;
        let metadata: TableMetadata = serde_json::from_slice(&bytes)
            .map_err(|error| format!("decode authoritative Hadoop v1 metadata: {error}"))?;
        if metadata.uuid().to_string() != expected_uuid
            || hex_digest(&bytes) != expected_metadata_digest
        {
            return Ok(HadoopCreateReconciliation::Foreign);
        }
        let finalization_failure = self
            .write_version_hint(&table_location, 1)
            .await
            .err()
            .map(|error| format!("repair committed Hadoop catalog version hint: {error}"));
        Ok(HadoopCreateReconciliation::Committed {
            finalization_failure,
        })
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

// Eight bounded path temporaries cover namespace joins, probe paths, and
// provider-relative paths while the directory/result containers coexist.
fn hadoop_listing_workspace() -> usize {
    crate::fs_io::HADOOP_LISTING_WORKSPACE_BYTES
        - 8 * novarocks_spi::connector::ConnectorListingBound::V1.name_bytes
}

fn directory_heap_bytes(children: &Vec<String>) -> Result<usize> {
    children.iter().try_fold(
        children
            .capacity()
            .checked_mul(std::mem::size_of::<String>())
            .ok_or_else(crate::fs_io::listing_refusal)?,
        |bytes, child| {
            bytes
                .checked_add(child.capacity())
                .ok_or_else(crate::fs_io::listing_refusal)
        },
    )
}

fn namespace_clone_heap_bytes(namespace: &NamespaceIdent) -> Result<usize> {
    namespace.as_ref().iter().try_fold(
        namespace
            .as_ref()
            .len()
            .checked_mul(std::mem::size_of::<String>())
            .ok_or_else(crate::fs_io::listing_refusal)?,
        |bytes, component| {
            bytes
                .checked_add(component.len())
                .ok_or_else(crate::fs_io::listing_refusal)
        },
    )
}

/// Refuse a directory listing that exceeds the caller's listing bound. The
/// refusal is carried as the typed source so the catalog owner can keep its
/// `ResourceExhausted` classification.
fn check_directory_listing_bound(
    children: &[String],
    bound: novarocks_spi::connector::ConnectorListingBound,
) -> Result<()> {
    bound.check_complete_listing(children).map_err(|refusal| {
        Error::new(
            ErrorKind::Unexpected,
            "Hadoop catalog directory listing refused by its listing bound",
        )
        .with_source(refusal)
    })
}

fn check_catalog_read_control(
    control: &Option<Arc<dyn novarocks_spi::connector::ConnectorOperationControl>>,
) -> Result<()> {
    if let Some(control) = control {
        control.check_active().map_err(|error| {
            Error::new(ErrorKind::Unexpected, "Hadoop catalog read stopped").with_source(error)
        })?;
    }
    Ok(())
}

#[async_trait]
impl Catalog for HadoopFileSystemCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.list_namespaces_bounded(
            parent,
            self.listing_binding()?,
            novarocks_spi::connector::ConnectorListingBound::V1,
        )
        .await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let marker = self.namespace_marker_location(namespace);
        self.file_io
            .new_output(&marker)?
            .write(Vec::new().into())
            .await?;
        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        Ok(Namespace::new(namespace.clone()))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        if let Some(binding) = &self.binding {
            return self
                .namespace_exists_for_read(namespace, binding.clone())
                .await;
        }
        // The compatibility constructor may answer its exact marker probe,
        // but has no receipt for a custom Storage's listing allocations.
        self.check_listing_location(Some(namespace))?;
        if self
            .file_io
            .exists(self.namespace_marker_location(namespace))
            .await?
        {
            return Ok(true);
        }
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Bounded Hadoop namespace discovery requires the provider-owned filesystem binding",
        ))
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Ok(())
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.file_io
            .delete(self.namespace_marker_location(namespace))
            .await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.external_tables(namespace).await
    }

    /// Create a table: write `v1.metadata.json` and `version-hint.text=1`.
    ///
    /// If `creation.location` is `None` the table location is inferred from
    /// the warehouse location and the table identifier.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let result = self
            .create_table_fenced(namespace, creation, uuid::Uuid::now_v7().to_string())
            .await
            .map_err(|failure| {
                let kind = match failure.kind {
                    HadoopCreateFailureKind::Invalid => ErrorKind::DataInvalid,
                    HadoopCreateFailureKind::Unsupported => ErrorKind::FeatureUnsupported,
                    HadoopCreateFailureKind::Uncommitted => ErrorKind::Unexpected,
                    HadoopCreateFailureKind::Unknown => ErrorKind::Unexpected,
                };
                Error::new(kind, failure.message)
            })?;
        if result.disposition == HadoopCreateDisposition::Existing {
            return Err(Error::new(
                ErrorKind::TableAlreadyExists,
                format!("table already exists: {}", result.table.identifier()),
            ));
        }
        Ok(result.table)
    }

    /// Load a table through the filesystem-owned catalog pointer.
    ///
    /// The in-memory registry is an accelerator only. A different Hadoop
    /// catalog client can replace or drop the table, so serving its cached
    /// location without first resolving `version-hint.text` would make this
    /// client observe a stale table identity.
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let key = Self::table_key(table);
        let metadata_location = match self.try_cache_existing_table(table).await? {
            Some(location) => location,
            None => {
                self.tables.lock().await.remove(&key);
                return Err(Error::new(
                    ErrorKind::TableNotFound,
                    format!("table not found: {}", key),
                ));
            }
        };

        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("read metadata from {}: {}", metadata_location, e),
                )
            })?;

        self.build_table(table.clone(), metadata, metadata_location)
    }

    /// Drop the table from the catalog.
    ///
    /// For a filesystem catalog the catalog entry *is* storage: existence
    /// resolves through `version-hint.text` and, failing that, the canonical
    /// `v1.metadata.json`. Removing those two is therefore the catalog
    /// operation, mirroring ADR-0077, where writing `v1.metadata.json` is what
    /// makes a table exist. Data files and superseded metadata are objects, and
    /// they are not touched here.
    ///
    /// This used to prefix-delete the whole table directory, which was wrong in
    /// four ways at once: it ignored the caller's data disposition, so a drop
    /// asking to retain data destroyed it anyway; it gave concurrent readers no
    /// window, so a reader that had just resolved this table lost its files
    /// mid-scan; it matched a lexical prefix rather than the table's exact
    /// object identity; and it swallowed its own failures, so a partial delete
    /// still reported success. Object deletion now belongs to the post-commit
    /// cleanup handoff, which runs only after the drop is proven committed,
    /// only against exact identity, and only once the age window has passed.
    /// See ADR-0118.
    ///
    /// The order matters. The hint goes first, so a failure between the two
    /// steps leaves the table resolvable through `v1.metadata.json` and the
    /// hint self-repairs on the next read: the drop simply did not commit, and
    /// retrying is safe. Removing `v1.metadata.json` is the commit point.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let key = Self::table_key(table);
        self.tables.lock().await.remove(&key);

        let table_location = self.table_location(table);
        let hint = Self::version_hint_path(&table_location);
        if self.file_io.exists(&hint).await? {
            self.file_io.delete(&hint).await?;
        }
        let canonical = Self::metadata_path(&table_location, 1);
        if self.file_io.exists(&canonical).await? {
            self.file_io.delete(&canonical).await?;
        }
        Ok(())
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        let key = Self::table_key(table);
        // The filesystem pointer is the catalog authority. Do not let a local
        // cache hide an external drop or replacement made by another client.
        let exists = self.try_cache_existing_table(table).await?.is_some();
        if !exists {
            self.tables.lock().await.remove(&key);
        }
        Ok(exists)
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let src_key = Self::table_key(src);
        let dest_key = Self::table_key(dest);
        let mut guard = self.tables.lock().await;
        if let Some(loc) = guard.remove(&src_key) {
            guard.insert(dest_key, loc);
        }
        Ok(())
    }

    /// Register an existing table that already has metadata written at
    /// `metadata_location`.
    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("read metadata from {}: {}", metadata_location, e),
                )
            })?;

        let key = Self::table_key(table);
        self.tables
            .lock()
            .await
            .insert(key, metadata_location.clone());

        self.build_table(table.clone(), metadata, metadata_location)
    }

    /// Apply a table commit (requirements + updates) and write a new versioned
    /// metadata file.
    ///
    /// This method bypasses `TableCommit::apply()` which internally calls
    /// `MetadataLocation::from_str()`. That function rejects the Hadoop
    /// `vN.metadata.json` naming convention, so we manually apply requirements
    /// and updates here.
    async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
        let ident = commit.identifier().clone();

        // Load the current metadata.
        let current_table = self.load_table(&ident).await?;
        let current_metadata_location = current_table
            .metadata_location()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "no metadata location for table: {}",
                        Self::table_key(&ident)
                    ),
                )
            })?
            .to_string();
        let current_metadata = current_table.metadata();

        // Check all requirements against the current metadata.
        for requirement in commit.take_requirements() {
            requirement.check(Some(current_metadata))?;
        }

        // Apply all updates to produce new metadata.
        let mut builder = current_metadata
            .clone()
            .into_builder(Some(current_metadata_location));
        for update in commit.take_updates() {
            builder = update.apply(builder)?;
        }
        let new_metadata = builder.build()?.metadata;

        // Determine the next version number.
        let table_location = current_metadata.location().to_string();
        let current_version = self.read_version_hint(&table_location).await;
        let next_version = current_version + 1;

        // Write the new metadata and update version-hint.text.
        let new_metadata_location = self
            .write_metadata(&table_location, &new_metadata, next_version)
            .await?;

        // Update the in-memory registry.
        let key = Self::table_key(&ident);
        self.tables
            .lock()
            .await
            .insert(key, new_metadata_location.clone());

        self.build_table(ident, new_metadata, new_metadata_location)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;
    use std::time::Instant;

    use crate::iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorRequestContext,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::*;

    fn local_test_binding() -> crate::access_binding::IcebergReadBinding {
        // The binding stores handles into this runtime. Keep its owner alive for
        // the whole test process: constructing and immediately dropping a
        // runtime is illegal from these `#[tokio::test]` bodies.
        static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        let runtime = RUNTIME
            .get_or_init(|| tokio::runtime::Runtime::new().expect("build local test runtime"));
        crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        )
    }

    fn test_catalog(location: &str) -> HadoopFileSystemCatalog {
        let binding = local_test_binding();
        let file_io = crate::fs_io::build_file_io_for_location(location, binding.clone());
        HadoopFileSystemCatalog::new_with_binding(file_io, location.to_string(), binding)
    }

    fn listing_error_kind(error: &Error) -> ConnectorErrorKind {
        std::error::Error::source(error)
            .and_then(|source| source.downcast_ref::<ConnectorError>())
            .expect("typed listing refusal")
            .kind()
    }

    #[tokio::test]
    async fn bounded_hadoop_listing_preserves_marker_fallback_and_hierarchical_names() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        for path in [
            "external/table/metadata",
            "external/not-a-table",
            "marked",
            ".hidden",
        ] {
            std::fs::create_dir_all(warehouse.path().join(path)).expect("directory");
        }
        std::fs::write(
            warehouse
                .path()
                .join("external/table/metadata/version-hint.text"),
            b"1",
        )
        .expect("external table hint");
        std::fs::write(warehouse.path().join("marked/.novarocks_namespace"), b"")
            .expect("namespace marker");
        std::fs::create_dir_all(warehouse.path().join("parent/child")).expect("child");
        std::fs::write(
            warehouse.path().join("parent/child/.novarocks_namespace"),
            b"",
        )
        .expect("child marker");
        let location = warehouse.path().to_string_lossy().to_string();
        let catalog = test_catalog(&location);
        let namespaces = catalog
            .list_namespaces(None)
            .await
            .expect("plain bounded namespaces");
        assert_eq!(
            namespaces,
            [
                NamespaceIdent::new("external".into()),
                NamespaceIdent::new("marked".into())
            ]
        );
        let parent = NamespaceIdent::new("parent".into());
        assert_eq!(
            catalog
                .list_namespaces(Some(&parent))
                .await
                .expect("hierarchical listing"),
            [NamespaceIdent::from_vec(vec!["parent".into(), "child".into()]).expect("namespace")]
        );
        assert!(
            catalog
                .namespace_exists(&NamespaceIdent::new("external".into()))
                .await
                .expect("fallback")
        );
        let bound = novarocks_spi::connector::ConnectorListingBound {
            entries: 2,
            page_entries: 1,
            ..novarocks_spi::connector::ConnectorListingBound::V1
        };
        let tables = catalog
            .list_tables_for_read(
                &NamespaceIdent::new("external".into()),
                local_test_binding(),
                bound,
            )
            .await
            .expect("actual caller bound");
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name(), "table");
        assert_eq!(
            listing_error_kind(
                &catalog
                    .list_tables_for_read(
                        &NamespaceIdent::new("external".into()),
                        local_test_binding(),
                        novarocks_spi::connector::ConnectorListingBound {
                            entries: 1,
                            ..bound
                        },
                    )
                    .await
                    .expect_err("whole source exceeds caller bound")
            ),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[tokio::test]
    async fn bounded_hadoop_scan_precharges_qualifier_and_result_backing() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        std::fs::create_dir_all(warehouse.path().join("n/t/metadata")).expect("table");
        std::fs::write(
            warehouse.path().join("n/t/metadata/version-hint.text"),
            b"1",
        )
        .expect("hint");
        let location = warehouse.path().to_string_lossy().to_string();
        let catalog = test_catalog(&location);
        let namespace = NamespaceIdent::new("n".into());
        let bound = novarocks_spi::connector::ConnectorListingBound {
            entries: 1,
            ..novarocks_spi::connector::ConnectorListingBound::V1
        };
        let source_bytes = std::mem::size_of::<String>() + 1;
        let result_bytes = source_bytes
            + namespace_clone_heap_bytes(&namespace).expect("qualifier")
            + std::mem::size_of::<TableIdent>();
        assert_eq!(
            listing_error_kind(
                &catalog
                    .scan_external_tables(
                        &namespace,
                        local_test_binding(),
                        bound,
                        result_bytes - 1,
                        true,
                    )
                    .await
                    .expect_err("clone/result before allocation")
            ),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(
            catalog
                .scan_external_tables(&namespace, local_test_binding(), bound, result_bytes, true)
                .await
                .expect("exact result boundary")
                .0
                .len(),
            1
        );
        // The existence fallback walks the same complete source and all probes
        // while retaining no TableIdent namespace clones.
        assert!(
            catalog
                .scan_external_tables(&namespace, local_test_binding(), bound, source_bytes, false)
                .await
                .expect("existence under remaining workspace")
                .1
        );
    }

    #[tokio::test]
    async fn bounded_hadoop_namespace_output_does_not_limit_fallback_table_source() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        for table in ["first", "second"] {
            let metadata = warehouse.path().join("n").join(table).join("metadata");
            std::fs::create_dir_all(&metadata).expect("table directory");
            std::fs::write(metadata.join("version-hint.text"), b"1").expect("table hint");
        }
        let location = warehouse.path().to_string_lossy().to_string();
        let catalog = test_catalog(&location);
        let bound = novarocks_spi::connector::ConnectorListingBound {
            entries: 1,
            name_bytes: 1,
            total_name_bytes: 1,
            ..novarocks_spi::connector::ConnectorListingBound::V1
        };
        assert_eq!(
            catalog
                .list_namespaces_for_read(local_test_binding(), bound)
                .await
                .expect("one namespace with two longer table names"),
            [NamespaceIdent::new("n".into())]
        );
    }

    #[tokio::test]
    async fn bounded_hadoop_listing_refuses_custom_storage_without_owner_binding() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = HadoopFileSystemCatalog::new(
            FileIO::new_with_fs(),
            warehouse.path().to_string_lossy().to_string(),
        );
        assert_eq!(
            catalog
                .list_namespaces(None)
                .await
                .expect_err("no owner proof")
                .kind(),
            ErrorKind::FeatureUnsupported
        );
        assert_eq!(
            catalog
                .list_tables(&NamespaceIdent::new("n".into()))
                .await
                .expect_err("no owner proof")
                .kind(),
            ErrorKind::FeatureUnsupported
        );
    }

    fn read_request(
        cancellation: Arc<novarocks_spi::connector::ConnectorStopOwner>,
        deadline: Instant,
    ) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            deadline,
            cancellation.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request")
    }

    #[tokio::test]
    async fn request_bound_hadoop_load_does_not_poison_the_catalog_generation() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let ident = TableIdent::new(namespace.clone(), "events".to_string());
        let catalog = test_catalog(&location);
        catalog
            .create_table_fenced(
                &namespace,
                test_creation("events"),
                "read-control".to_string(),
            )
            .await
            .expect("create table");

        let cancelled = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let first = catalog
            .load_table_for_read(
                &ident,
                local_test_binding().for_request(read_request(
                    cancelled.clone(),
                    Instant::now() + Duration::from_secs(30),
                )),
            )
            .await
            .expect("first load");
        cancelled.request_stop();
        let metadata_path = first.metadata_location().expect("metadata path");
        let stopped = first
            .file_io()
            .new_input(metadata_path)
            .expect("input")
            .read()
            .await
            .err()
            .expect("old request must stop");
        let source = std::error::Error::source(&stopped)
            .and_then(|source| source.downcast_ref::<ConnectorError>())
            .expect("typed cancellation");
        assert_eq!(source.kind(), ConnectorErrorKind::Cancelled);

        let cancelled_binding = local_test_binding().for_request(read_request(
            cancelled,
            Instant::now() + Duration::from_secs(30),
        ));
        let stopped_kind = |error: &Error| {
            std::error::Error::source(error)
                .and_then(|source| source.downcast_ref::<ConnectorError>())
                .expect("typed catalog read stop")
                .kind()
        };
        assert_eq!(
            stopped_kind(
                &catalog
                    .list_namespaces_for_read(
                        cancelled_binding.clone(),
                        novarocks_spi::connector::ConnectorListingBound::V1,
                    )
                    .await
                    .expect_err("cancelled namespace list"),
            ),
            ConnectorErrorKind::Cancelled,
        );
        assert_eq!(
            stopped_kind(
                &catalog
                    .namespace_exists_for_read(&namespace, cancelled_binding.clone())
                    .await
                    .expect_err("cancelled namespace existence"),
            ),
            ConnectorErrorKind::Cancelled,
        );
        assert_eq!(
            stopped_kind(
                &catalog
                    .list_tables_for_read(
                        &namespace,
                        cancelled_binding.clone(),
                        novarocks_spi::connector::ConnectorListingBound::V1,
                    )
                    .await
                    .expect_err("cancelled table list"),
            ),
            ConnectorErrorKind::Cancelled,
        );
        assert_eq!(
            stopped_kind(
                &catalog
                    .table_exists_for_read(&ident, cancelled_binding)
                    .await
                    .expect_err("cancelled table existence"),
            ),
            ConnectorErrorKind::Cancelled,
        );

        catalog
            .load_table_for_read(
                &ident,
                local_test_binding().for_request(read_request(
                    Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
                    Instant::now() + Duration::from_secs(30),
                )),
            )
            .await
            .expect("later request can load");
        catalog
            .load_table(&ident)
            .await
            .expect("generation client remains usable");
    }

    #[tokio::test]
    async fn commit_reload_uses_operation_io_and_fresh_hint_without_replacing_catalog_io() {
        use crate::commit::model::ArtifactWriter;
        let directory = tempfile::tempdir().unwrap();
        let warehouse = directory.path().to_string_lossy().to_string();
        let catalog = test_catalog(&warehouse);
        let stop = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let operation = crate::commit::operation::IcebergCommitOperation::new(
            crate::commit::model::OperationToken::from_write(
                novarocks_spi::connector::ConnectorWriteOperationId::from_bytes([7; 16]),
            ),
            warehouse.clone(),
            local_test_binding(),
            read_request(stop.clone(), Instant::now() + Duration::from_secs(30)),
            crate::resources::IcebergCatalogRuntime::new(tokio::runtime::Handle::current()),
            crate::commit::operation::OperationLimits::default(),
        )
        .unwrap();
        let namespace = NamespaceIdent::new("analytics".into());
        let ident = TableIdent::new(namespace.clone(), "events".into());
        catalog
            .create_table_fenced(&namespace, test_creation("events"), "commit-reload".into())
            .await
            .unwrap();
        let original = catalog.load_table(&ident).await.unwrap();
        let mut json = serde_json::to_value(original.metadata()).unwrap();
        json["properties"]["external-change"] = "visible-on-reload".into();
        let table_location = catalog.table_location(&ident);
        let v2 = HadoopFileSystemCatalog::metadata_path(&table_location, 2);
        std::fs::write(&v2, serde_json::to_vec(&json).unwrap()).unwrap();
        std::fs::write(
            HadoopFileSystemCatalog::version_hint_path(&table_location),
            b"2\n",
        )
        .unwrap();
        let attempt = operation.begin_attempt().unwrap();
        let reloaded = catalog
            .load_table_for_commit(&ident, attempt.file_io().clone())
            .await
            .unwrap();
        assert_eq!(reloaded.metadata_location(), Some(v2.as_str()));
        assert_eq!(
            reloaded
                .metadata()
                .properties()
                .get("external-change")
                .unwrap(),
            "visible-on-reload"
        );
        assert!(
            original
                .metadata()
                .properties()
                .get("external-change")
                .is_none()
        );
        stop.request_stop();
        let error = catalog
            .load_table_for_commit(&ident, attempt.file_io().clone())
            .await
            .unwrap_err();
        let mut source: &dyn std::error::Error = &error;
        loop {
            if let Some(stop) = source.downcast_ref::<ConnectorError>() {
                assert_eq!(stop.kind(), ConnectorErrorKind::Cancelled);
                break;
            }
            source = source
                .source()
                .expect("typed operation cancellation is preserved");
        }
        catalog
            .load_table(&ident)
            .await
            .expect("catalog generation remains usable");
    }

    fn test_creation(name: &str) -> TableCreation {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        TableCreation::builder()
            .name(name.to_string())
            .schema(schema)
            .format_version(FormatVersion::V2)
            .build()
    }

    #[test]
    fn test_metadata_path() {
        assert_eq!(
            HadoopFileSystemCatalog::metadata_path("oss://bucket/warehouse/db/tbl", 1),
            "oss://bucket/warehouse/db/tbl/metadata/v1.metadata.json"
        );
        assert_eq!(
            HadoopFileSystemCatalog::metadata_path("file:///tmp/wh/db/tbl", 3),
            "file:///tmp/wh/db/tbl/metadata/v3.metadata.json"
        );
    }

    #[test]
    fn test_version_hint_path() {
        assert_eq!(
            HadoopFileSystemCatalog::version_hint_path("oss://bucket/warehouse/db/tbl"),
            "oss://bucket/warehouse/db/tbl/metadata/version-hint.text"
        );
    }

    #[test]
    fn test_table_location() {
        let catalog = test_catalog("oss://bucket/warehouse");
        let ident = TableIdent::from_strs(["ns1", "my_table"]).unwrap();
        assert_eq!(
            catalog.table_location(&ident),
            "oss://bucket/warehouse/ns1/my_table"
        );
    }

    #[tokio::test]
    async fn namespace_marker_survives_catalog_reconstruction() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());

        let catalog = test_catalog(&location);
        assert!(!catalog.namespace_exists(&namespace).await.unwrap());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        assert!(catalog.namespace_exists(&namespace).await.unwrap());
        assert_eq!(
            catalog.list_namespaces(None).await.unwrap(),
            vec![namespace.clone()]
        );

        let restored = test_catalog(&location);
        assert!(restored.namespace_exists(&namespace).await.unwrap());
        assert_eq!(
            restored.list_namespaces(None).await.unwrap(),
            vec![namespace.clone()]
        );
        restored.drop_namespace(&namespace).await.unwrap();
        assert!(!restored.namespace_exists(&namespace).await.unwrap());
    }

    #[tokio::test]
    async fn external_table_directory_establishes_namespace_without_private_marker() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("spark_created".to_string());
        let metadata_dir = warehouse
            .path()
            .join("spark_created")
            .join("orders")
            .join("metadata");
        std::fs::create_dir_all(&metadata_dir).expect("create Spark-style metadata directory");
        std::fs::write(metadata_dir.join("version-hint.text"), b"1\n")
            .expect("write Spark-style version hint");

        let catalog = test_catalog(&location);
        assert!(catalog.namespace_exists(&namespace).await.unwrap());
        assert_eq!(
            catalog.list_namespaces(None).await.unwrap(),
            vec![namespace.clone()]
        );
        assert_eq!(
            catalog.list_tables(&namespace).await.unwrap(),
            vec![TableIdent::new(namespace, "orders".to_string())]
        );
    }

    #[tokio::test]
    async fn independent_catalog_clients_share_one_v1_owner() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let first = test_catalog(&location);
        let second = test_catalog(&location);

        let (left, right) = tokio::join!(
            first.create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-left".to_string(),
            ),
            second.create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-right".to_string(),
            ),
        );
        let left = left.expect("left create result");
        let right = right.expect("right create result");

        assert_eq!(
            [left.disposition, right.disposition]
                .into_iter()
                .filter(|disposition| *disposition == HadoopCreateDisposition::Created)
                .count(),
            1
        );
        assert_eq!(
            [left.disposition, right.disposition]
                .into_iter()
                .filter(|disposition| *disposition == HadoopCreateDisposition::Existing)
                .count(),
            1
        );
        assert_eq!(left.table.metadata().uuid(), right.table.metadata().uuid());
        assert_eq!(
            left.table.metadata_location(),
            right.table.metadata_location()
        );
    }

    /// A drop removes the catalog entry and nothing else.
    ///
    /// Data files outlive it deliberately: they are objects, and objects are
    /// reclaimed by the age-gated, identity-checked cleanup handoff, never by
    /// the catalog deleting a path prefix out from under a live reader.
    #[tokio::test]
    async fn drop_removes_the_catalog_pointer_and_leaves_objects_for_collection() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let ident = TableIdent::new(namespace.clone(), "events".to_string());
        let catalog = test_catalog(&location);
        catalog
            .create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-create".to_string(),
            )
            .await
            .expect("create table");

        let table_location = catalog.table_location(&ident);
        // Stand in for a data file the table owns.
        let data_file = format!("{table_location}/data/part-0.parquet");
        catalog
            .file_io
            .new_output(&data_file)
            .expect("output")
            .write(bytes::Bytes::from_static(b"rows"))
            .await
            .expect("write data file");

        catalog.drop_table(&ident).await.expect("drop table");

        assert!(
            !catalog.table_exists(&ident).await.expect("existence"),
            "the catalog entry is gone"
        );
        // A fresh client must agree: the pointer, not just the cache, was removed.
        let restored = test_catalog(&location);
        assert!(!restored.table_exists(&ident).await.expect("existence"));

        assert!(
            catalog.file_io.exists(&data_file).await.expect("stat"),
            "objects survive the drop and are left to identity-aware collection"
        );
    }

    #[tokio::test]
    async fn external_drop_invalidates_a_cached_table_existence() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let ident = TableIdent::new(namespace.clone(), "events".to_string());
        let server_catalog = test_catalog(&location);
        let external_catalog = test_catalog(&location);

        server_catalog
            .create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-create".to_string(),
            )
            .await
            .expect("create table");
        assert!(
            server_catalog
                .table_exists(&ident)
                .await
                .expect("cache table existence")
        );

        external_catalog
            .drop_table(&ident)
            .await
            .expect("drop table through an external client");

        assert!(
            !server_catalog
                .table_exists(&ident)
                .await
                .expect("observe external drop")
        );
        assert!(matches!(
            server_catalog.load_table(&ident).await,
            Err(error) if error.kind() == ErrorKind::TableNotFound
        ));
    }

    #[tokio::test]
    async fn canonical_v1_recovers_table_when_hint_is_missing() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let ident = TableIdent::new(namespace.clone(), "events".to_string());
        let catalog = test_catalog(&location);
        let created = catalog
            .create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-create".to_string(),
            )
            .await
            .expect("create table");
        let hint = HadoopFileSystemCatalog::version_hint_path(&catalog.table_location(&ident));
        catalog.file_io.delete(&hint).await.expect("remove hint");

        let restored = test_catalog(&location);
        assert!(restored.table_exists(&ident).await.expect("table exists"));
        let loaded = restored.load_table(&ident).await.expect("load from v1");
        assert_eq!(created.table.metadata().uuid(), loaded.metadata().uuid());
        assert!(restored.file_io.exists(&hint).await.expect("repaired hint"));
    }

    #[tokio::test]
    async fn fault_before_conditional_request_leaves_no_v1() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let ident = TableIdent::new(namespace.clone(), "events".to_string());
        let catalog = test_catalog(&location);
        let v1 = HadoopFileSystemCatalog::metadata_path(&catalog.table_location(&ident), 1);
        let hint = HadoopFileSystemCatalog::version_hint_path(&catalog.table_location(&ident));
        catalog.inject_test_fault(HadoopCatalogTestFault::BeforeConditionalRequest);

        let failure = catalog
            .create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-before-request".to_string(),
            )
            .await
            .expect_err("injected pre-request failure");

        assert_eq!(failure.kind, HadoopCreateFailureKind::Uncommitted);
        assert!(!catalog.file_io.exists(&v1).await.expect("probe v1"));
        assert!(!catalog.file_io.exists(&hint).await.expect("probe hint"));
    }

    #[tokio::test]
    async fn lost_conditional_response_is_attributed_by_authoritative_v1() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let catalog = test_catalog(&location);
        catalog.inject_test_fault(HadoopCatalogTestFault::AfterConditionalResponseLoss);

        let created = catalog
            .create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-response-loss".to_string(),
            )
            .await
            .expect("authoritative reread attributes committed v1");

        assert_eq!(created.disposition, HadoopCreateDisposition::Created);
        assert_eq!(created.facts.table_uuid, created.authoritative_table_uuid);
        assert_eq!(
            created.facts.metadata_digest,
            created.authoritative_metadata_digest
        );
        assert!(created.finalization_failure.is_none());
        assert!(
            catalog
                .file_io
                .exists(&created.facts.metadata_location)
                .await
                .expect("probe committed v1")
        );
    }

    #[tokio::test]
    async fn failure_before_hint_write_is_committed_and_v1_recovers() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let ident = TableIdent::new(namespace.clone(), "events".to_string());
        let catalog = test_catalog(&location);
        catalog.inject_test_fault(HadoopCatalogTestFault::BeforeHintWrite);

        let created = catalog
            .create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-hint-before".to_string(),
            )
            .await
            .expect("v1 commit survives hint failure");
        let hint = HadoopFileSystemCatalog::version_hint_path(&catalog.table_location(&ident));
        assert_eq!(created.disposition, HadoopCreateDisposition::Created);
        assert!(created.finalization_failure.is_some());
        assert!(
            catalog
                .file_io
                .exists(&created.facts.metadata_location)
                .await
                .expect("probe committed v1")
        );
        assert!(!catalog.file_io.exists(&hint).await.expect("probe hint"));

        let restored = test_catalog(&location);
        let loaded = restored
            .load_table(&ident)
            .await
            .expect("recover table from canonical v1");
        assert_eq!(
            loaded.metadata().uuid().to_string(),
            created.facts.table_uuid
        );
        assert!(restored.file_io.exists(&hint).await.expect("repaired hint"));
    }

    #[tokio::test]
    async fn lost_hint_response_remains_committed_with_durable_hint() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let ident = TableIdent::new(namespace.clone(), "events".to_string());
        let catalog = test_catalog(&location);
        catalog.inject_test_fault(HadoopCatalogTestFault::AfterHintWriteResponseLoss);

        let created = catalog
            .create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-hint-response-loss".to_string(),
            )
            .await
            .expect("v1 commit survives lost hint response");
        let hint = HadoopFileSystemCatalog::version_hint_path(&catalog.table_location(&ident));
        assert_eq!(created.disposition, HadoopCreateDisposition::Created);
        assert!(created.finalization_failure.is_some());
        assert!(catalog.file_io.exists(&hint).await.expect("durable hint"));

        let restored = test_catalog(&location);
        let loaded = restored
            .load_table(&ident)
            .await
            .expect("load through durable hint");
        assert_eq!(
            loaded.metadata().uuid().to_string(),
            created.facts.table_uuid
        );
    }

    #[tokio::test]
    async fn failed_authoritative_reread_after_response_loss_stays_unknown() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let ident = TableIdent::new(namespace.clone(), "events".to_string());
        let catalog = test_catalog(&location);
        let attempt = catalog
            .prepare_create_attempt(
                &namespace,
                test_creation("events"),
                "operation-reread-failure".to_string(),
            )
            .expect("prepare create attempt");
        let facts = attempt.facts.clone();
        catalog.inject_test_fault(HadoopCatalogTestFault::AfterConditionalResponseLoss);
        catalog.inject_test_fault(HadoopCatalogTestFault::AuthoritativeV1Read);

        let failure = catalog
            .publish_create_attempt(attempt)
            .await
            .expect_err("unreadable authoritative v1 cannot be guessed committed");
        let hint = HadoopFileSystemCatalog::version_hint_path(&catalog.table_location(&ident));
        assert_eq!(failure.kind, HadoopCreateFailureKind::Unknown);
        assert!(
            catalog
                .file_io
                .exists(&facts.metadata_location)
                .await
                .expect("v1 was durably created")
        );
        assert!(!catalog.file_io.exists(&hint).await.expect("probe hint"));
    }

    #[tokio::test]
    async fn failed_hint_repair_never_deletes_committed_v1() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let location = warehouse.path().to_string_lossy().to_string();
        let namespace = NamespaceIdent::new("analytics".to_string());
        let ident = TableIdent::new(namespace.clone(), "events".to_string());
        let creator = test_catalog(&location);
        creator.inject_test_fault(HadoopCatalogTestFault::BeforeHintWrite);
        let created = creator
            .create_table_fenced(
                &namespace,
                test_creation("events"),
                "operation-repair".to_string(),
            )
            .await
            .expect("commit v1 without hint");
        let original_v1 = creator
            .file_io
            .new_input(&created.facts.metadata_location)
            .expect("open committed v1")
            .read()
            .await
            .expect("read committed v1");

        let restored = test_catalog(&location);
        restored.inject_test_fault(HadoopCatalogTestFault::BeforeHintWrite);
        assert!(
            restored
                .table_exists(&ident)
                .await
                .expect("v1 remains authoritative when hint repair fails")
        );
        let after_repair = restored
            .file_io
            .new_input(&created.facts.metadata_location)
            .expect("open v1 after failed repair")
            .read()
            .await
            .expect("read v1 after failed repair");
        let hint = HadoopFileSystemCatalog::version_hint_path(&restored.table_location(&ident));
        assert_eq!(after_repair, original_v1);
        assert!(!restored.file_io.exists(&hint).await.expect("probe hint"));
        let loaded = restored
            .load_table(&ident)
            .await
            .expect("cache populated from v1 despite failed hint repair");
        assert_eq!(
            loaded.metadata().uuid().to_string(),
            created.facts.table_uuid
        );
    }
}
