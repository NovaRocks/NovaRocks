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

mod create_publication;
mod drop_publication;
/// HadoopFileSystemCatalog — a Hadoop-catalog-compatible implementation of the
/// iceberg `Catalog` trait.
///
/// Differences from `MemoryCatalog`:
/// - Metadata files are written as `v{N}.metadata.json` (Hadoop convention).
/// - `version-hint.text` is maintained alongside each metadata directory so
///   that StarRocks FE, Spark, and Trino can discover the current version.
/// - Prepared publication applies frozen requirements/updates and installs a
///   complete new version without replacing an existing slot.
mod publication;
pub(crate) mod versions;

use std::collections::HashMap;
use std::sync::Arc;
#[cfg(test)]
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
#[cfg(test)]
use novarocks_fs::FileErrorKind;
use novarocks_fs::{ConditionalCreateOutcome, FileCancellation};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HadoopCreateAttemptFacts {
    pub(crate) operation_id: String,
    pub(crate) table_uuid: String,
    pub(crate) metadata_location: String,
    pub(crate) metadata_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HadoopCreateFailureKind {
    Invalid,
}

#[derive(Debug)]
pub(crate) struct HadoopCreateFailure {
    pub(crate) kind: HadoopCreateFailureKind,
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
    pub(crate) fn identifier(&self) -> &TableIdent {
        &self.ident
    }
    pub(crate) fn metadata_bytes(&self) -> &[u8] {
        &self.metadata_bytes
    }
    pub(crate) fn validate_frozen_identity(&self, operation_id: [u8; 16]) -> Result<()> {
        if self.facts.operation_id != uuid::Uuid::from_bytes(operation_id).simple().to_string()
            || self.facts.table_uuid != self.metadata.uuid().to_string()
            || self.facts.metadata_location
                != HadoopFileSystemCatalog::metadata_path(&self.table_location, 1)
            || self.facts.metadata_digest != hex_digest(&self.metadata_bytes)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Frozen Hadoop create identity differs from its exact metadata request",
            ));
        }
        Ok(())
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
    BeforeConditionalUnsupported,
    AfterConditionalResponseLoss,
    AfterConditionalUnsupported,
    AfterExistingConditionalResponseLoss,
    BeforeConditionalReadEvidenceExpiry,
    BeforeConditionalReadDelete,
    BeforeCanonicalDrop,
    AfterCanonicalDropResponseLoss,
    AfterDropHintEvidenceExpiry,
    BeforeDropHintDelete,
    AfterDropHintResponseLoss,
    BeforeHintWrite,
    AfterHintWriteResponseLoss,
    AuthoritativeV1Read,
}

#[derive(Debug)]
pub struct HadoopFileSystemCatalog {
    file_io: FileIO,
    runtime: crate::iceberg::Runtime,
    warehouse_location: String,
    /// The exact provider-owned storage capability admitted for this catalog.
    /// It is process local and never serialized with the catalog client.
    binding: Option<crate::access_binding::IcebergReadBinding>,
    #[cfg(test)]
    test_faults: std::sync::Mutex<Vec<HadoopCatalogTestFault>>,
}

impl HadoopFileSystemCatalog {
    /// Compatibility constructor for storage-free tests. Production callers
    /// must use [`Self::new_with_binding`]. Any operation requiring a
    /// provider filesystem capability fails closed when constructed here.
    pub fn new(
        file_io: FileIO,
        warehouse_location: String,
        runtime: crate::iceberg::Runtime,
    ) -> Self {
        Self {
            file_io,
            runtime,
            warehouse_location: warehouse_location.trim_end_matches('/').to_string(),
            binding: None,
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
        runtime: crate::iceberg::Runtime,
    ) -> Self {
        Self {
            file_io,
            runtime,
            warehouse_location: warehouse_location.trim_end_matches('/').to_string(),
            binding: Some(binding),
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
        let children = crate::fs_io::list_directories(
            &namespace_location,
            binding.clone(),
            bound,
            workspace_bytes,
        )
        .await?;
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
            if versions::physical_exists(&format!("{namespace_location}/{table}"), binding.clone())
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
        let children =
            crate::fs_io::list_directories(&location, binding.clone(), bound, workspace_bytes)
                .await?;
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
        _bound: novarocks_spi::connector::ConnectorListingBound,
        workspace_bytes: usize,
    ) -> Result<bool> {
        self.check_listing_location(Some(namespace))?;
        let control = binding.operation_control();
        check_catalog_read_control(&control)?;
        let file_io =
            crate::fs_io::build_file_io_for_location(&self.warehouse_location, binding.clone());
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
        Ok(!self
            .version_inventory(table, binding)
            .await?
            .files
            .is_empty())
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

    // A phase grant bounds starting each step, never discards its result.
    fn check_publication_budget(
        budget: Option<crate::catalog::publication::EvidenceContext>,
    ) -> Result<()> {
        if budget.is_some_and(|budget| budget.expired()) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Hadoop publication evidence deadline elapsed",
            )
            .with_source(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::DeadlineExceeded,
                "Hadoop publication evidence deadline elapsed",
            )));
        }
        Ok(())
    }

    /// All read callers share physical version discovery without repairing hints.
    pub(crate) async fn load_table_for_read(
        &self,
        table: &TableIdent,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<Table> {
        self.load_discovered_table(table, binding).await
    }

    pub(crate) async fn load_table_for_commit(
        &self,
        table: &TableIdent,
        file_io: FileIO,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<Table> {
        let current = self
            .discover_current_version(table, binding, None)
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::TableNotFound,
                    "Hadoop commit target does not exist",
                )
            })?;
        Table::builder()
            .runtime(self.runtime.clone())
            .file_io(file_io)
            .metadata(Arc::new(current.metadata))
            .identifier(table.clone())
            .metadata_location(current.file.path)
            .build()
    }

    /// Canonical table name for diagnostics.
    fn table_key(ident: &TableIdent) -> String {
        let namespace = ident.namespace().join("/");
        format!("{}/{}", namespace, ident.name())
    }

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
        self.validate_table_root(&ident, &table_location)
            .map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Invalid,
                message: error.to_string(),
            })?;
        if creation.properties.contains_key("write.metadata.path") {
            return Err(HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Invalid,
                message: "Hadoop catalog does not support write.metadata.path".into(),
            });
        }
        let creation_with_location = TableCreation {
            location: Some(table_location.clone()),
            ..creation
        };
        let build_result = TableMetadataBuilder::from_table_creation(creation_with_location)
            .map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Invalid,
                message: format!("build metadata from creation: {error}"),
            })?
            .build()
            .map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Invalid,
                message: format!("build metadata: {error}"),
            })?;
        let metadata = build_result.metadata;
        let metadata_bytes =
            serde_json::to_vec(&metadata).map_err(|error| HadoopCreateFailure {
                kind: HadoopCreateFailureKind::Invalid,
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

    /// Local fixtures publish through the real owner and the original binding.
    #[cfg(test)]
    pub(crate) async fn create_fixture_table(
        self: &Arc<Self>,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        assert_eq!(
            namespace.as_ref().len(),
            1,
            "Fixture requires one exact namespace component"
        );
        let binding = self
            .binding
            .clone()
            .expect("fixture original storage binding");
        let owner = crate::catalog::factory::NovaRocksCatalogFactory::adopt_hadoop_for_test(
            Arc::clone(self),
        );
        Ok(
            crate::catalog::admission_test_support::create_static_fixture_table_with_owner(
                owner,
                binding,
                novarocks_spi::connector::ProviderBindingEpoch::from_bytes([0x71; 16]),
                crate::catalog::CatalogNamespaceName::new(namespace.as_ref()[0].clone()),
                creation,
            )
            .await,
        )
    }

    #[cfg(test)]
    async fn publish_fixture_catalog(
        self: &Arc<Self>,
        request: crate::catalog::catalog_request::CatalogPublicationRequest,
    ) -> crate::catalog::error::CatalogOutcome<crate::catalog::transaction::CommitProof> {
        let binding = self
            .binding
            .clone()
            .expect("fixture original storage binding");
        let owner = crate::catalog::factory::NovaRocksCatalogFactory::adopt_hadoop_for_test(
            Arc::clone(self),
        );
        crate::catalog::admission_test_support::publish_catalog_fixture_with_owner(
            owner,
            binding,
            novarocks_spi::connector::ProviderBindingEpoch::from_bytes([0x71; 16]),
            request,
        )
        .await
    }

    pub(crate) async fn reconcile_create_attempt_for_read(
        &self,
        namespace: &str,
        table: &str,
        expected_uuid: &str,
        expected_metadata_location: &str,
        expected_metadata_digest: &str,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> std::result::Result<HadoopCreateReconciliation, novarocks_spi::connector::ConnectorError>
    {
        use novarocks_spi::connector::{
            ConnectorError, ConnectorErrorKind, ConnectorOperationControl,
        };
        let context = binding.request_context().cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Conditional-create observation requires the original request context",
            )
        })?;
        context.check_active()?;
        let ident = TableIdent::from_strs([namespace, table]).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("build Hadoop table identity: {error}"),
            )
        })?;
        let file_io =
            crate::fs_io::build_file_io_for_location(&self.table_location(&ident), binding);
        self.reconcile_create_using_io(
            namespace,
            table,
            expected_uuid,
            expected_metadata_location,
            expected_metadata_digest,
            &file_io,
            || context.check_active(),
            |error| {
                context
                    .check_active()
                    .err()
                    .unwrap_or_else(|| crate::catalog::error::map_read_error(error))
            },
        )
        .await
    }

    async fn reconcile_create_using_io(
        &self,
        namespace: &str,
        table: &str,
        expected_uuid: &str,
        expected_metadata_location: &str,
        expected_metadata_digest: &str,
        file_io: &FileIO,
        check: impl Fn() -> std::result::Result<(), novarocks_spi::connector::ConnectorError>,
        map_read_failure: impl Fn(&Error) -> novarocks_spi::connector::ConnectorError,
    ) -> std::result::Result<HadoopCreateReconciliation, novarocks_spi::connector::ConnectorError>
    {
        let ident = TableIdent::from_strs([namespace, table]).map_err(|error| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                format!("build Hadoop table identity: {error}"),
            )
        })?;
        let table_location = self.table_location(&ident);
        let canonical_v1 = Self::metadata_path(&table_location, 1);
        if canonical_v1 != expected_metadata_location {
            return Ok(HadoopCreateReconciliation::Foreign);
        }
        check()?;
        let exists = file_io
            .exists(&canonical_v1)
            .await
            .map_err(|error| map_read_failure(&error))?;
        check()?;
        if !exists {
            return Ok(HadoopCreateReconciliation::Absent);
        }
        let bytes = file_io
            .new_input(&canonical_v1)
            .map_err(|error| map_read_failure(&error))?
            .read()
            .await
            .map_err(|error| map_read_failure(&error))?;
        check()?;
        let metadata: TableMetadata =
            crate::metadata_json::from_slice(&bytes).map_err(|error| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                    format!("decode authoritative Hadoop v1 metadata: {error}"),
                )
            })?;
        if metadata.uuid().to_string() != expected_uuid
            || hex_digest(&bytes) != expected_metadata_digest
        {
            return Ok(HadoopCreateReconciliation::Foreign);
        }
        // Observation never repairs hints or acquires a second mutation grant.
        // A journal-owned finalizer may repair only after retaining this proof.
        Ok(HadoopCreateReconciliation::Committed {
            finalization_failure: None,
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
    /// SDK mutations cannot enter catalog publication.
    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Catalog mutation must use its prepared publication journal",
        ))
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
    /// SDK mutations cannot enter catalog publication.
    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Catalog mutation must use its prepared publication journal",
        ))
    }
    /// SDK mutations cannot enter catalog publication.
    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Catalog mutation must use its prepared publication journal",
        ))
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.external_tables(namespace).await
    }
    /// SDK mutations cannot enter catalog publication.
    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Catalog mutation must use its prepared publication journal",
        ))
    }

    /// Resolve the authoritative physical version before loading its metadata.
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        self.load_discovered_table(table, self.listing_binding()?)
            .await
    }
    /// SDK mutations cannot enter catalog publication.
    async fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Catalog mutation must use its prepared publication journal",
        ))
    }

    async fn purge_table(&self, _table: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Hadoop catalog does not support purging tables",
        ))
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        Ok(!self
            .version_inventory(table, self.listing_binding()?)
            .await?
            .files
            .is_empty())
    }
    /// SDK mutations cannot enter catalog publication.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Catalog mutation must use its prepared publication journal",
        ))
    }
    /// SDK mutations cannot enter catalog publication.
    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Catalog mutation must use its prepared publication journal",
        ))
    }
    /// SDK mutations cannot enter catalog publication.
    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Catalog mutation must use its prepared publication journal",
        ))
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

    /// Test calls retain the same joining bridge as production publication.
    pub(super) fn on_bridge<F: std::future::Future + Send + 'static>(future: F) -> F::Output
    where
        F::Output: Send + 'static,
    {
        let runtime = local_test_runtime();
        let bridge = crate::resources::IcebergCatalogRuntime::new(runtime.handle().clone());
        bridge.block_on(future).expect("catalog bridge exits")
    }

    fn local_test_runtime() -> &'static tokio::runtime::Runtime {
        static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("build local test runtime"))
    }

    fn local_test_binding() -> crate::access_binding::IcebergReadBinding {
        let runtime = local_test_runtime();
        crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        )
    }

    pub(super) fn test_catalog(location: &str) -> HadoopFileSystemCatalog {
        let binding = local_test_binding();
        let file_io = crate::fs_io::build_file_io_for_location(location, binding.clone());
        HadoopFileSystemCatalog::new_with_binding(
            file_io,
            location.to_string(),
            binding,
            crate::resources::IcebergCatalogRuntime::new(local_test_runtime().handle().clone())
                .iceberg_runtime()
                .clone(),
        )
    }

    fn listing_error_kind(error: &Error) -> ConnectorErrorKind {
        std::error::Error::source(error)
            .and_then(|source| source.downcast_ref::<ConnectorError>())
            .expect("typed listing refusal")
            .kind()
    }

    #[test]
    fn bounded_hadoop_listing_preserves_marker_fallback_and_hierarchical_names() {
        crate::hadoop_catalog::tests::on_bridge(async move {
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
                    .join("external/table/metadata/v1.metadata.json"),
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
            let catalog = Arc::new(test_catalog(&location));
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
                [
                    NamespaceIdent::from_vec(vec!["parent".into(), "child".into()])
                        .expect("namespace")
                ]
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
        });
    }

    #[test]
    fn bounded_hadoop_scan_precharges_qualifier_and_result_backing() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            let warehouse = tempfile::tempdir().expect("warehouse");
            std::fs::create_dir_all(warehouse.path().join("n/t/metadata")).expect("table");
            std::fs::write(warehouse.path().join("n/t/metadata/v1.metadata.json"), b"1")
                .expect("hint");
            let location = warehouse.path().to_string_lossy().to_string();
            let catalog = Arc::new(test_catalog(&location));
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
                    .scan_external_tables(
                        &namespace,
                        local_test_binding(),
                        bound,
                        result_bytes,
                        true
                    )
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
                    .scan_external_tables(
                        &namespace,
                        local_test_binding(),
                        bound,
                        source_bytes,
                        false
                    )
                    .await
                    .expect("existence under remaining workspace")
                    .1
            );
        });
    }

    #[test]
    fn bounded_hadoop_namespace_output_does_not_limit_fallback_table_source() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            let warehouse = tempfile::tempdir().expect("warehouse");
            for table in ["first", "second"] {
                let metadata = warehouse.path().join("n").join(table).join("metadata");
                std::fs::create_dir_all(&metadata).expect("table directory");
                std::fs::write(
                    metadata.join("v1.metadata.json"),
                    b"invalid JSON still establishes presence",
                )
                .expect("version file");
            }
            let location = warehouse.path().to_string_lossy().to_string();
            let catalog = Arc::new(test_catalog(&location));
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
        });
    }

    #[test]
    fn bounded_hadoop_listing_refuses_custom_storage_without_owner_binding() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            let warehouse = tempfile::tempdir().expect("warehouse");
            let catalog = HadoopFileSystemCatalog::new(
                FileIO::new_with_fs(),
                warehouse.path().to_string_lossy().to_string(),
                crate::iceberg::Runtime::try_current().expect("test runtime"),
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
        });
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

    #[test]
    fn request_bound_hadoop_load_does_not_poison_the_catalog_generation() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            let warehouse = tempfile::tempdir().expect("warehouse");
            let location = warehouse.path().to_string_lossy().to_string();
            let namespace = NamespaceIdent::new("analytics".to_string());
            let ident = TableIdent::new(namespace.clone(), "events".to_string());
            let catalog = Arc::new(test_catalog(&location));
            catalog
                .create_fixture_table(&namespace, test_creation("events"))
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
                .expect_err("old request must stop");
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
        });
    }

    #[test]
    fn commit_reload_uses_operation_io_and_fresh_hint_without_replacing_catalog_io() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            use crate::commit::model::ArtifactWriter;
            let directory = tempfile::tempdir().unwrap();
            let warehouse = directory.path().to_string_lossy().to_string();
            let catalog = Arc::new(test_catalog(&warehouse));
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
                .create_fixture_table(&namespace, test_creation("events"))
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
                .load_table_for_commit(&ident, attempt.file_io().clone(), attempt.read_binding())
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
                .load_table_for_commit(&ident, attempt.file_io().clone(), attempt.read_binding())
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
        });
    }

    pub(super) fn test_creation(name: &str) -> TableCreation {
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
        let catalog = Arc::new(test_catalog("oss://bucket/warehouse"));
        let ident = TableIdent::from_strs(["ns1", "my_table"]).unwrap();
        assert_eq!(
            catalog.table_location(&ident),
            "oss://bucket/warehouse/ns1/my_table"
        );
    }

    #[test]
    fn namespace_marker_survives_catalog_reconstruction() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            let warehouse = tempfile::tempdir().expect("warehouse");
            let location = warehouse.path().to_string_lossy().to_string();
            let namespace = NamespaceIdent::new("analytics".to_string());

            let catalog = Arc::new(test_catalog(&location));
            assert!(!catalog.namespace_exists(&namespace).await.unwrap());
            let outcome = catalog
                .publish_fixture_catalog(
                    crate::catalog::catalog_request::CatalogPublicationRequest::CreateNamespace {
                        namespace: crate::catalog::CatalogNamespaceName::new("analytics"),
                        properties: HashMap::new(),
                        no_op_if_exists: false,
                    },
                )
                .await;
            assert!(
                matches!(
                    outcome,
                    crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
                ),
                "{outcome:?}"
            );
            assert!(catalog.namespace_exists(&namespace).await.unwrap());
            assert_eq!(
                catalog.list_namespaces(None).await.unwrap(),
                vec![namespace.clone()]
            );

            let restored = Arc::new(test_catalog(&location));
            assert!(restored.namespace_exists(&namespace).await.unwrap());
            assert_eq!(
                restored.list_namespaces(None).await.unwrap(),
                vec![namespace.clone()]
            );
            let outcome = restored
                .publish_fixture_catalog(
                    crate::catalog::catalog_request::CatalogPublicationRequest::DropNamespace {
                        namespace: crate::catalog::CatalogNamespaceName::new("analytics"),
                        no_op_if_missing: false,
                    },
                )
                .await;
            assert!(
                matches!(
                    outcome,
                    crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
                ),
                "{outcome:?}"
            );
            assert!(!restored.namespace_exists(&namespace).await.unwrap());
        });
    }

    #[test]
    fn external_table_directory_establishes_namespace_without_private_marker() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            let warehouse = tempfile::tempdir().expect("warehouse");
            let location = warehouse.path().to_string_lossy().to_string();
            let namespace = NamespaceIdent::new("spark_created".to_string());
            let metadata_dir = warehouse
                .path()
                .join("spark_created")
                .join("orders")
                .join("metadata");
            std::fs::create_dir_all(&metadata_dir).expect("create Spark-style metadata directory");
            std::fs::write(
                metadata_dir.join("v1.metadata.json"),
                b"not needed for physical existence",
            )
            .expect("write Spark-style version hint");

            let catalog = Arc::new(test_catalog(&location));
            assert!(catalog.namespace_exists(&namespace).await.unwrap());
            assert_eq!(
                catalog.list_namespaces(None).await.unwrap(),
                vec![namespace.clone()]
            );
            assert_eq!(
                catalog.list_tables(&namespace).await.unwrap(),
                vec![TableIdent::new(namespace, "orders".to_string())]
            );
        });
    }

    /// A drop removes the catalog entry and nothing else.
    ///
    /// Data files outlive it deliberately: they are objects, and objects are
    /// reclaimed by the age-gated, identity-checked cleanup handoff, never by
    /// the catalog deleting a path prefix out from under a live reader.
    #[test]
    fn drop_removes_the_catalog_pointer_and_leaves_objects_for_collection() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            let warehouse = tempfile::tempdir().expect("warehouse");
            let location = warehouse.path().to_string_lossy().to_string();
            let namespace = NamespaceIdent::new("analytics".to_string());
            let ident = TableIdent::new(namespace.clone(), "events".to_string());
            let catalog = Arc::new(test_catalog(&location));
            catalog
                .create_fixture_table(&namespace, test_creation("events"))
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

            let outcome = catalog
                .publish_fixture_catalog(
                    crate::catalog::catalog_request::CatalogPublicationRequest::DropTable {
                        table: crate::catalog::CatalogTableName::from_identifier(&ident),
                        no_op_if_missing: false,
                        purge: false,
                        capture: None,
                    },
                )
                .await;
            assert!(
                matches!(
                    outcome,
                    crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
                ),
                "{outcome:?}"
            );

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
        });
    }

    #[test]
    fn external_drop_invalidates_a_cached_table_existence() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            let warehouse = tempfile::tempdir().expect("warehouse");
            let location = warehouse.path().to_string_lossy().to_string();
            let namespace = NamespaceIdent::new("analytics".to_string());
            let ident = TableIdent::new(namespace.clone(), "events".to_string());
            let server_catalog = Arc::new(test_catalog(&location));
            let external_catalog = Arc::new(test_catalog(&location));

            server_catalog
                .create_fixture_table(&namespace, test_creation("events"))
                .await
                .expect("create table");
            assert!(
                server_catalog
                    .table_exists(&ident)
                    .await
                    .expect("cache table existence")
            );

            let outcome = external_catalog
                .publish_fixture_catalog(
                    crate::catalog::catalog_request::CatalogPublicationRequest::DropTable {
                        table: crate::catalog::CatalogTableName::from_identifier(&ident),
                        no_op_if_missing: false,
                        purge: false,
                        capture: None,
                    },
                )
                .await;
            assert!(
                matches!(
                    outcome,
                    crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
                ),
                "{outcome:?}"
            );

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
        });
    }

    #[test]
    fn canonical_v1_recovers_table_when_hint_is_missing() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            let warehouse = tempfile::tempdir().expect("warehouse");
            let location = warehouse.path().to_string_lossy().to_string();
            let namespace = NamespaceIdent::new("analytics".to_string());
            let ident = TableIdent::new(namespace.clone(), "events".to_string());
            let catalog = Arc::new(test_catalog(&location));
            let created = catalog
                .create_fixture_table(&namespace, test_creation("events"))
                .await
                .expect("create table");
            let hint = HadoopFileSystemCatalog::version_hint_path(&catalog.table_location(&ident));
            catalog.file_io.delete(&hint).await.expect("remove hint");

            let restored = test_catalog(&location);
            assert!(restored.table_exists(&ident).await.expect("table exists"));
            let loaded = restored.load_table(&ident).await.expect("load from v1");
            assert_eq!(created.metadata().uuid(), loaded.metadata().uuid());
            assert!(
                !restored
                    .file_io
                    .exists(&hint)
                    .await
                    .expect("reads do not repair hint")
            );
        });
    }
}
