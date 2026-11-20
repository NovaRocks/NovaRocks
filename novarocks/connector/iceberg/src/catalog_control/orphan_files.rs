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

//! Trino-compatible orphan enumeration for the original explicit cleanup Source.

use std::collections::HashSet;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_fs::FsScheme;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorListingPageSize,
    ConnectorMaintenanceRetention, ConnectorOperationControl, ConnectorOriginalResultScope,
    ConnectorOrphanCleanupMetrics, ConnectorRequestContext, ConnectorTableObjectId,
    LakePublicationDisposition, ProviderBindingEpoch, ProviderMetadataCpuPort,
};
use novarocks_type_contract::owned_resources::{
    hashmap::fresh_table_layout, vec::original_resize_growth,
};

use super::maintenance_admission::{RetentionProcedure, validate_retention_procedure};
use super::maintenance_error::{filesystem_failure, invalid_metadata};
use super::statistics::scoped_cpu::{IcebergMetadataCpuOriginal, run_provider_metadata};
use crate::access_binding::IcebergReadBinding;
use crate::catalog::listing_admission::ListingAdmission;
use crate::catalog_config::IcebergCatalogMaintenanceConfig;
use crate::catalog_metadata_root::is_catalog_metadata_root;
use crate::fs_io::{
    IcebergFsAccess, format_resolved_location, resolve_request_access_for_location,
};
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{Manifest, ManifestList, TableMetadataRef};
use crate::iceberg::table::Table;
use crate::opendal::{Error as StorageError, Operator, raw::Timestamp};

const DELETE_BATCH_SIZE: usize = 1000;
const VERSION_HINT: &str = "version-hint.text";

/// The same Source exposes only original, observed deletion evidence. An
/// unanswered request is conservative even if an earlier batch was confirmed.
#[derive(Default)]
pub(super) struct Deletions(Mutex<DeleteEvidence>);

#[derive(Default)]
struct DeleteEvidence {
    dispatched: bool,
    uncertain: bool,
    files: u64,
    bytes: u64,
}

impl Deletions {
    fn begin(&self) {
        let mut evidence = self.0.lock().unwrap_or_else(|p| p.into_inner());
        evidence.dispatched = true;
        evidence.uncertain = true;
    }

    fn confirmed(&self, files: u64, bytes: u64) -> Result<(), ConnectorError> {
        let mut evidence = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let count = evidence.files.checked_add(files).ok_or_else(resource)?;
        let length = evidence.bytes.checked_add(bytes).ok_or_else(resource)?;
        evidence.files = count;
        evidence.bytes = length;
        evidence.uncertain = false;
        Ok(())
    }

    fn counts(&self) -> (u64, u64) {
        let evidence = self.0.lock().unwrap_or_else(|p| p.into_inner());
        (evidence.files, evidence.bytes)
    }

    pub(super) fn publication_fact(&self) -> (Option<LakePublicationDisposition>, bool) {
        let evidence = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let disposition = if evidence.uncertain {
            Some(LakePublicationDisposition::CommitUnknown)
        } else if evidence.dispatched {
            Some(LakePublicationDisposition::KnownCommitted)
        } else {
            None
        };
        (disposition, evidence.files != 0)
    }
}

/// The CPU port accepts the normal exact-generation metadata source. The
/// payload remains in that source across a borrowed worker poll or unwind.
pub(super) enum DecodeInput {
    List(Bytes, TableMetadataRef),
    Manifest(Bytes),
}

pub(super) enum Decoded {
    List(ManifestList),
    Manifest(Manifest),
}

pub(super) struct DecodeWork {
    pub(super) input: DecodeInput,
    pub(super) output: Option<crate::iceberg::Result<Decoded>>,
}

impl DecodeWork {
    pub(super) fn run(&mut self) {
        self.output = Some(match &self.input {
            DecodeInput::List(bytes, metadata) => {
                crate::manifest::decode_manifest_list(bytes, metadata).map(Decoded::List)
            }
            DecodeInput::Manifest(bytes) => {
                // File validity requires the physical entry status and path only.
                crate::manifest::parse_manifest_checked(bytes).map(Decoded::Manifest)
            }
        });
    }
}

pub(super) struct OrphanScan<'a> {
    pub(super) context: &'a ConnectorRequestContext,
    pub(super) scope: &'a ConnectorOriginalResultScope,
    pub(super) binding: &'a IcebergReadBinding,
    pub(super) admission: &'a ListingAdmission,
    pub(super) cpu: &'a Arc<dyn ProviderMetadataCpuPort>,
    pub(super) descriptor: &'a ConnectorInstanceDescriptor,
    pub(super) incarnation: ProviderBindingEpoch,
    pub(super) table_name: &'a str,
    pub(super) retention: ConnectorMaintenanceRetention,
    pub(super) configuration: IcebergCatalogMaintenanceConfig,
    pub(super) deletions: &'a Deletions,
}

impl OrphanScan<'_> {
    // Design: ADR-0243 (docs/adr/ADR-0243-statement-authorized-iceberg-metadata-maintenance.md)
    pub(super) async fn execute(
        &self,
        table: &Table,
        expected_object: &ConnectorTableObjectId,
        older_than_ms: i64,
    ) -> Result<Option<ConnectorOrphanCleanupMetrics>, ConnectorError> {
        self.check_active()?;
        let metadata = table.metadata();
        crate::document_storage::observation::validate_expected_object(expected_object, metadata)?;
        validate_retention_procedure(
            RetentionProcedure::RemoveOrphanFiles,
            metadata.format_version() as u8,
            metadata
                .properties()
                .get("write.location-provider.impl")
                .map(String::as_str),
            self.table_name,
            self.retention,
            self.configuration,
        )?;
        if metadata.current_snapshot().is_none() {
            return Ok(None);
        }

        let _retention = self.context.retain_owned_metadata()?;
        let mut budget = Budget::new(self.context);
        let root = metadata.location();
        let access = resolve_request_access_for_location(root, self.binding)
            .await
            .map_err(|error| self.file_error(error))?;
        let storage = ScanStorage::new(access, self.table_name, &mut budget).await?;
        let cutoff = Timestamp::from_millisecond(older_than_ms)
            .map_err(|_| filesystem_error(self.table_name))?;
        let metadata_location = table.metadata_location().ok_or_else(|| {
            invalid_metadata("Iceberg table has no current metadata file location")
        })?;

        let mut valid = FileNames::default();
        let mut processed_manifests = 0u64;
        // Read every dependency before establishing the first delete obligation.
        self.protect_snapshot_files(table, &mut valid, &mut processed_manifests, &mut budget)
            .await?;
        for path in std::iter::once(metadata_location)
            .chain(
                metadata
                    .metadata_log()
                    .iter()
                    .map(|log| log.metadata_file.as_str()),
            )
            .chain(
                metadata
                    .statistics_iter()
                    .map(|stats| stats.statistics_path.as_str()),
            )
            .chain(
                metadata
                    .partition_statistics_iter()
                    .map(|stats| stats.statistics_path.as_str()),
            )
        {
            self.check_active()?;
            valid.insert(file_name(path), &mut budget).await?;
        }
        let active_files =
            (valid.names.len() - usize::from(valid.names.contains(VERSION_HINT))) as u64;
        let scanned_files = self
            .scan_storage(&storage, cutoff, &valid, &mut budget)
            .await?;
        let (deleted_files, deleted_bytes) = self.deletions.counts();
        Ok(Some(ConnectorOrphanCleanupMetrics {
            processed_manifests,
            active_files,
            scanned_files,
            deleted_files,
            deleted_bytes,
        }))
    }

    async fn protect_snapshot_files(
        &self,
        table: &Table,
        valid: &mut FileNames,
        processed_manifests: &mut u64,
        budget: &mut Budget<'_>,
    ) -> Result<(), ConnectorError> {
        let mut visited = FileNames::default();
        for snapshot in table.metadata().snapshots() {
            self.check_active()?;
            let path = snapshot.manifest_list();
            valid.insert(file_name(path), budget).await?;
            let (bytes, list_bytes) = self.read_object(table.file_io(), path, budget).await?;
            let Decoded::List(list) = self
                .decode(DecodeInput::List(bytes, table.metadata_ref()), path)
                .await?
            else {
                return Err(internal("orphan CPU returned a manifest for a list"));
            };
            let list_decoded =
                crate::commit::manifest_encoding::descriptor_owned_bytes(list.entries())
                    .ok()
                    .and_then(|size| usize::try_from(size).ok())
                    .ok_or_else(resource)?;
            budget.retain(list_decoded).await?;
            budget.release(list_bytes);
            for descriptor in list.entries() {
                self.check_active()?;
                let path = &descriptor.manifest_path;
                if !visited.insert(path, budget).await? {
                    continue;
                }
                valid.insert(file_name(path), budget).await?;
                let (bytes, manifest_bytes) =
                    self.read_object(table.file_io(), path, budget).await?;
                let Decoded::Manifest(manifest) =
                    self.decode(DecodeInput::Manifest(bytes), path).await?
                else {
                    return Err(internal("orphan CPU returned a list for a manifest"));
                };
                let manifest_decoded = usize::try_from(
                    crate::manifest::target_query::raw_manifest_bytes(&manifest, descriptor),
                )
                .map_err(|_| resource())?;
                budget.retain(manifest_decoded).await?;
                budget.release(manifest_bytes);
                for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                    self.check_active()?;
                    valid.insert(file_name(entry.file_path()), budget).await?;
                }
                drop(manifest);
                budget.release(manifest_decoded);
                *processed_manifests = processed_manifests.checked_add(1).ok_or_else(resource)?;
            }
            drop(list);
            budget.release(list_decoded);
        }
        budget.release(visited.backing);
        Ok(())
    }

    async fn read_object(
        &self,
        io: &FileIO,
        path: &str,
        budget: &mut Budget<'_>,
    ) -> Result<(Bytes, usize), ConnectorError> {
        self.check_active()?;
        let input = io
            .new_input(path)
            .map_err(|error| manifest_error(path, error))?;
        let metadata = input
            .metadata()
            .await
            .map_err(|error| manifest_error(path, error))?;
        let size = usize::try_from(metadata.size).map_err(|_| resource())?;
        budget.retain(size).await?;
        let bytes = input
            .read()
            .await
            .map_err(|error| manifest_error(path, error))?;
        if bytes.len() != size {
            return Err(manifest_filesystem_error(path));
        }
        Ok((bytes, size))
    }

    async fn decode(&self, input: DecodeInput, path: &str) -> Result<Decoded, ConnectorError> {
        self.check_active()?;
        let source = IcebergMetadataCpuOriginal::orphan_decode(
            self.descriptor.clone(),
            self.incarnation,
            DecodeWork {
                input,
                output: None,
            },
        );
        run_provider_metadata(self.cpu, &source).await?;
        source
            .take_orphan_decode()
            .map_err(|error| manifest_error(path, error))
    }

    async fn scan_storage(
        &self,
        storage: &ScanStorage<'_>,
        cutoff: Timestamp,
        valid: &FileNames,
        budget: &mut Budget<'_>,
    ) -> Result<u64, ConnectorError> {
        let operator = storage.access.operator();
        let mut directories = Vec::new();
        budget.retain(storage.prefix.len()).await?;
        let mut directory = storage.prefix.as_str().to_owned();
        let mut candidates = Vec::new();
        let mut scanned = 0u64;
        loop {
            let mut entries = self
                .admission
                .run_wait_for_exit(self.context, async {
                    self.check_active()?;
                    let _io = self.binding.admit_non_http_io(&storage.full_root).await?;
                    operator
                        .lister_with(&directory)
                        .recursive(storage.recursive)
                        .limit(ConnectorListingPageSize::DEFAULT.get())
                        .await
                        .map_err(|error| self.storage_error(error))
                })
                .await?;
            while let Some(entry) = self
                .admission
                .run_wait_for_exit(self.context, async {
                    self.check_active()?;
                    let _io = self.binding.admit_non_http_io(&storage.full_root).await?;
                    entries
                        .next()
                        .await
                        .transpose()
                        .map_err(|error| self.storage_error(error))
                })
                .await?
            {
                let (path, metadata) = entry.into_parts();
                // Local listing includes the requested directory itself.
                if metadata.is_dir() && path == directory {
                    continue;
                }
                storage.validate_path(&directory, &path)?;
                if metadata.is_dir() {
                    if !storage.recursive {
                        budget
                            .push(&mut directories, path, |value| value.capacity())
                            .await?;
                    }
                    continue;
                }
                if !metadata.is_file() {
                    continue;
                }
                scanned = scanned.checked_add(1).ok_or_else(resource)?;
                let (modified, size) = required_file_facts(&metadata, self.table_name)?;
                if modified >= cutoff
                    || file_name(&path) == VERSION_HINT
                    || valid.names.contains(file_name(&path))
                {
                    continue;
                }
                let full_backing = storage.location_backing(path.len())?;
                budget.retain(full_backing).await?;
                let full = format_resolved_location(storage.access.handle(), &path)
                    .map_err(|_| filesystem_error(self.table_name))?;
                if storage.is_protected(&path, &full)? {
                    drop(full);
                    budget.release(full_backing);
                    continue;
                }
                let entry = Candidate {
                    path,
                    full,
                    full_backing,
                    size,
                };
                budget
                    .push(&mut candidates, entry, |value| value.path.capacity())
                    .await?;
                if candidates.len() == storage.batch_size {
                    self.delete_candidates(storage, &operator, &candidates)
                        .await?;
                    budget.clear_paths(&mut candidates);
                }
            }
            drop(entries);
            budget.release(directory.capacity());
            let Some(next) = directories.pop() else {
                break;
            };
            directory = next;
        }
        if !candidates.is_empty() {
            self.delete_candidates(storage, &operator, &candidates)
                .await?;
        }
        Ok(scanned)
    }

    async fn delete_candidates(
        &self,
        storage: &ScanStorage<'_>,
        operator: &Operator,
        candidates: &[Candidate],
    ) -> Result<(), ConnectorError> {
        // Recheck the exact full location at the dispatch boundary.
        for candidate in candidates {
            if storage.is_protected(&candidate.path, &candidate.full)? {
                return Err(filesystem_error(self.table_name));
            }
        }
        #[cfg(test)]
        crate::fs_io::note_test_storage_request("orphan_delete_batch", &storage.full_root);
        if storage.native_batch {
            let bytes = candidates.iter().try_fold(0u64, |sum, file| {
                sum.checked_add(file.size).ok_or_else(resource)
            })?;
            self.delete(storage, candidates.len() as u64, bytes, async {
                let mut deleter = operator.deleter().await?;
                for file in candidates {
                    deleter.delete(file.path.as_str()).await?;
                }
                deleter.close().await
            })
            .await
        } else {
            for candidate in candidates {
                self.delete(storage, 1, candidate.size, async {
                    operator.delete(&candidate.path).await
                })
                .await?;
            }
            Ok(())
        }
    }

    async fn delete(
        &self,
        storage: &ScanStorage<'_>,
        files: u64,
        bytes: u64,
        call: impl Future<Output = Result<(), StorageError>>,
    ) -> Result<(), ConnectorError> {
        self.admission
            .run_wait_for_exit(self.context, async {
                self.check_active()?;
                let _io = self.binding.admit_non_http_io(&storage.full_root).await?;
                self.check_active()?;
                self.context.publication_obligation_started()?;
                self.deletions.begin();
                // Preserve the receipt inside the original future, before its outer
                // actual-exit wait can project a simultaneous stop or deadline.
                call.await.map_err(|error| self.storage_error(error))?;
                self.deletions.confirmed(files, bytes)
            })
            .await
    }

    fn check_active(&self) -> Result<(), ConnectorError> {
        self.context.check_active()?;
        self.scope
            .check_active()
            .map_err(super::cleanup_maintenance::async_original::scope_error)
    }

    fn storage_error(&self, error: StorageError) -> ConnectorError {
        self.file_error(original_storage_failure(&error).unwrap_or_else(|| {
            ConnectorError::from(novarocks_fs::map_object_store_listing_error(error))
        }))
    }

    fn file_error(&self, error: ConnectorError) -> ConnectorError {
        if is_control_or_resource(&error) {
            error
        } else {
            filesystem_error(self.table_name)
        }
    }
}

struct ScanStorage<'a> {
    access: IcebergFsAccess,
    prefix: String,
    full_root: String,
    recursive: bool,
    native_batch: bool,
    batch_size: usize,
    table_name: &'a str,
}

impl<'a> ScanStorage<'a> {
    async fn new(
        access: IcebergFsAccess,
        table_name: &'a str,
        budget: &mut Budget<'_>,
    ) -> Result<Self, ConnectorError> {
        let operator = access.operator();
        let info = operator.info();
        let capability = info.capability();
        let supported = match (access.handle().scheme(), info.scheme()) {
            (FsScheme::Local, "fs") | (FsScheme::Hdfs, "hdfs-native") => true,
            (FsScheme::ObjectStore, "s3") => capability.list_with_recursive,
            _ => false,
        };
        if !(supported && capability.list && capability.delete) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Orphan cleanup requires storage listing with file modification times and sizes",
            ));
        }
        let recursive = access.handle().scheme() == FsScheme::ObjectStore;
        let native_batch = recursive && capability.delete_max_size.is_some_and(|size| size > 1);
        let batch_size = if native_batch {
            DELETE_BATCH_SIZE.min(capability.delete_max_size.unwrap())
        } else {
            DELETE_BATCH_SIZE
        };
        let root = access
            .single_relative_path()
            .map_err(|_| filesystem_error(table_name))?;
        let root = root.trim_matches('/');
        // Keep both forms, including local path-join scratch and URI overhead.
        let path_backing =
            location_backing(&access, root.len().checked_add(1).ok_or_else(resource)?)?
                .checked_add(root.len())
                .and_then(|size| size.checked_add(1))
                .ok_or_else(resource)?;
        budget.retain(path_backing).await?;
        let prefix = if root.is_empty() {
            "/".into()
        } else {
            format!("{root}/")
        };
        let full_root = format_resolved_location(access.handle(), &prefix)
            .map_err(|_| filesystem_error(table_name))?;
        Ok(Self {
            access,
            prefix,
            full_root,
            recursive,
            native_batch,
            batch_size,
            table_name,
        })
    }

    fn validate_path(&self, directory: &str, path: &str) -> Result<(), ConnectorError> {
        let relative = path.strip_prefix(directory.trim_start_matches('/'));
        let child = relative.map(|path| path.strip_suffix('/').unwrap_or(path));
        if child.is_some_and(|path| {
            !path.is_empty()
                && (self.recursive || !path.contains('/'))
                && path.split('/').all(|part| !matches!(part, "" | "." | ".."))
        }) {
            Ok(())
        } else {
            Err(filesystem_error(self.table_name))
        }
    }

    fn location_backing(&self, path_bytes: usize) -> Result<usize, ConnectorError> {
        location_backing(&self.access, path_bytes)
    }

    fn is_protected(&self, path: &str, full: &str) -> Result<bool, ConnectorError> {
        let relative = path.strip_prefix(self.prefix.trim_start_matches('/'));
        if !relative.is_some_and(|path| {
            !path.is_empty() && path.split('/').all(|part| !matches!(part, "" | "." | ".."))
        }) {
            return Err(filesystem_error(self.table_name));
        }
        if full.strip_prefix(&self.full_root) != relative {
            return Err(filesystem_error(self.table_name));
        }
        Ok(is_catalog_metadata_root(full))
    }
}

fn location_backing(access: &IcebergFsAccess, path_bytes: usize) -> Result<usize, ConnectorError> {
    // format_resolved_location can build an intermediate local PathBuf before
    // returning its URI String. Account for both during serialization.
    path_bytes
        .checked_add(access.handle().root().map_or(0, str::len))
        .and_then(|size| size.checked_add(access.handle().authority().map_or(0, str::len)))
        .and_then(|size| size.checked_add(32))
        .and_then(|size| size.checked_mul(4))
        .ok_or_else(resource)
}

struct Candidate {
    path: String,
    full: String,
    full_backing: usize,
    size: u64,
}

#[derive(Default)]
struct FileNames {
    names: HashSet<String>,
    backing: usize,
}

impl FileNames {
    async fn insert(
        &mut self,
        name: &str,
        budget: &mut Budget<'_>,
    ) -> Result<bool, ConnectorError> {
        if name.is_empty() || self.names.contains(name) {
            return Ok(false);
        }
        let grows = self.names.len() == self.names.capacity();
        let proposed = if grows {
            table_bytes(self.names.len().checked_add(1).ok_or_else(resource)?)?
        } else {
            0
        };
        budget
            .retain(proposed.checked_add(name.len()).ok_or_else(resource)?)
            .await?;
        if grows {
            let previous = table_bytes(self.names.capacity())?;
            self.names.try_reserve(1).map_err(|_| resource())?;
            budget.release(previous);
            self.backing -= previous;
        }
        self.names.insert(name.to_owned());
        self.backing += proposed + name.len();
        Ok(true)
    }
}

fn table_bytes(count: usize) -> Result<usize, ConnectorError> {
    fresh_table_layout::<String, ()>(count)
        .map(|facts| facts.request_bytes_upper_bound)
        .map_err(|_| resource())
}

struct Budget<'a> {
    context: &'a ConnectorRequestContext,
    retained: usize,
    admitted: usize,
}

impl<'a> Budget<'a> {
    fn new(context: &'a ConnectorRequestContext) -> Self {
        Self {
            context,
            retained: 0,
            admitted: 0,
        }
    }

    async fn retain(&mut self, bytes: usize) -> Result<(), ConnectorError> {
        let required = self.retained.checked_add(bytes).ok_or_else(resource)?;
        if required > self.admitted {
            let total = u64::try_from(required)
                .ok()
                .and_then(NonZeroU64::new)
                .ok_or_else(resource)?;
            self.context.reserve_owned_metadata(total).await?;
            self.admitted = required;
        }
        self.retained = required;
        Ok(())
    }

    fn release(&mut self, bytes: usize) {
        self.retained -= bytes;
    }

    async fn push<T: Send>(
        &mut self,
        values: &mut Vec<T>,
        value: T,
        backing: impl FnOnce(&T) -> usize,
    ) -> Result<(), ConnectorError> {
        let target = values.len().checked_add(1).ok_or_else(resource)?;
        let growth = original_resize_growth::<T>(values.len(), values.capacity(), target)
            .map_err(|_| resource())?;
        let bytes = backing(&value);
        let new_backing = growth.requested_backing.map_or(0, |layout| layout.size());
        self.retain(bytes.checked_add(new_backing).ok_or_else(resource)?)
            .await?;
        if new_backing != 0 {
            values.try_reserve(1).map_err(|_| resource())?;
            self.release(growth.old_backing.map_or(0, |layout| layout.size()));
        }
        values.push(value);
        Ok(())
    }

    fn clear_paths(&mut self, files: &mut Vec<Candidate>) {
        for file in files.drain(..) {
            self.release(file.path.capacity() + file.full_backing);
        }
    }
}

fn required_file_facts(
    metadata: &crate::opendal::Metadata,
    table_name: &str,
) -> Result<(Timestamp, u64), ConnectorError> {
    let modified = metadata
        .last_modified()
        .ok_or_else(|| filesystem_error(table_name))?;
    // All admitted built-in file listers author content length on each entry.
    // The OpenDAL public getter represents an empty file with a length of zero.
    Ok((modified, metadata.content_length()))
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn filesystem_error(table_name: &str) -> ConnectorError {
    filesystem_failure(format!(
        "Failed removing orphan files for table: {table_name}"
    ))
}

fn manifest_filesystem_error(path: &str) -> ConnectorError {
    filesystem_failure(format!("Unable to list manifest file content from {path}"))
}

fn is_control_or_resource(error: &ConnectorError) -> bool {
    matches!(
        error.failure_fact().class(),
        FailureClass::Control | FailureClass::Resource
    )
}

fn original_storage_failure(error: &(dyn std::error::Error + 'static)) -> Option<ConnectorError> {
    let mut source = Some(error);
    while let Some(cause) = source {
        if let Some(error) = cause.downcast_ref::<ConnectorError>() {
            return Some(error.clone());
        }
        source = cause.source();
    }
    None
}

fn manifest_error(path: &str, error: crate::iceberg::Error) -> ConnectorError {
    if let Some(error) = original_storage_failure(&error) {
        if is_control_or_resource(&error) {
            return error;
        }
        if error.kind() == ConnectorErrorKind::NotFound {
            return invalid_metadata(format!("Manifest file does not exist: {path}"));
        }
    }
    manifest_filesystem_error(path)
}

fn internal(detail: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, detail)
}

fn resource() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "orphan scan metadata capacity exceeded",
    )
}

#[cfg(test)]
mod tests;
