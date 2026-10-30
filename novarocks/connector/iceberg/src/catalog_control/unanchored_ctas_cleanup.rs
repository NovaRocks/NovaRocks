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

//! Catalog-wide collection for CTAS roots that have no published table anchor.
//!
//! Every candidate is discovered beneath one fixed warehouse prefix, then is
//! independently re-read immediately before its exact prefix is removed. A
//! malformed sidecar, a target that now exists, or any uncertain observation
//! is retained: crash-only recovery must never infer authority from a name.

use std::future::IntoFuture;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use novarocks_fs::{FsLocation, FsScheme};
use novarocks_spi::connector::{
    ConnectorCtasUnanchoredCleanupOutcome, ConnectorCtasUnanchoredCleanupRequest,
    ConnectorCtasUnanchoredDiscoveryRequest, ConnectorCtasUnanchoredProvenance, ConnectorError,
    ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorListingBound, ConnectorListingBudget,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorProviderBindingKey,
    ConnectorRequestContext, ConnectorUnanchoredCtasCleanup, ProviderBindingEpoch,
};

use super::staged_create::{
    ctas_staging_location, decode_unanchored_ctas_provenance, unanchored_ctas_provenance_location,
};
use crate::metadata_context::IcebergMetadataContext;

const CTAS_STAGING_NAMESPACE: &str = "_novarocks/ctas-staging/v1";
const CTAS_UNANCHORED_PROVENANCE_FILE: &str = "_novarocks.ctas.provenance.v1.json";

#[derive(Clone)]
pub(crate) struct IcebergUnanchoredCtasCleanupAdapter {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    runtime: Arc<IcebergMetadataContext>,
}

impl IcebergUnanchoredCtasCleanupAdapter {
    /// Attach a sweeper for this generation's unanchored CTAS staging root.
    ///
    /// This deliberately does not ask whether the catalog can run a CTAS. A
    /// catalog that cannot has never staged anything unanchored, so the sweep
    /// finds nothing and deletes nothing -- and gating here would replace the
    /// catalog's own explanation of why CTAS is impossible with a generic
    /// "no cleanup capability" from the lease derivation. The refusal belongs
    /// where the reason is known.
    pub(crate) fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Result<Self, ConnectorError> {
        let warehouse = runtime.control_state().configuration().warehouse_uri.trim();
        if warehouse.is_empty() {
            return Err(unsupported(
                "unanchored CTAS cleanup requires an explicit warehouse URI",
            ));
        }
        let parsed =
            FsLocation::parse(warehouse).map_err(|error| unavailable(error.to_string()))?;
        if !matches!(
            parsed.scheme(),
            FsScheme::Local | FsScheme::ObjectStore | FsScheme::Hdfs
        ) {
            return Err(unsupported(
                "unanchored CTAS cleanup warehouse has no list/stat/delete prefix support",
            ));
        }
        if !matches!(parsed.scheme(), FsScheme::Local)
            && !runtime
                .resources()
                .planning_binding()
                .requires_request_storage_resolver()
        {
            let access = crate::fs_io::resolve_access_for_location(
                warehouse,
                runtime.resources().planning_binding(),
            )
            .map_err(unavailable)?;
            Self::validate_cleanup_capabilities(&access)?;
        }
        Ok(Self {
            descriptor,
            incarnation,
            runtime,
        })
    }

    fn owner(&self) -> ConnectorProviderBindingKey {
        ConnectorProviderBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        }
    }

    fn warehouse_root(&self) -> &str {
        self.runtime
            .control_state()
            .configuration()
            .warehouse_uri
            .trim_end_matches('/')
    }

    fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
        if context.is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "unanchored CTAS cleanup request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "unanchored CTAS cleanup request deadline elapsed",
            ));
        }
        Ok(())
    }

    fn validate_warehouse(&self, requested: &str) -> Result<(), ConnectorError> {
        if requested.trim_end_matches('/') != self.warehouse_root() {
            return Err(invalid(
                "unanchored CTAS cleanup request warehouse differs from its control generation",
            ));
        }
        Ok(())
    }

    fn validate_cleanup_capabilities(
        access: &crate::fs_io::IcebergFsAccess,
    ) -> Result<(), ConnectorError> {
        let capability = access.operator().info().capability();
        if !capability.list
            || !capability.list_with_recursive
            || !capability.stat
            || !capability.read
            || !capability.delete
        {
            return Err(unsupported(
                "unanchored CTAS cleanup warehouse lacks recursive list, stat, read, or exact-delete support",
            ));
        }
        Ok(())
    }

    fn action_access(
        &self,
        location: &str,
        context: &ConnectorRequestContext,
    ) -> Result<crate::fs_io::IcebergFsAccess, ConnectorError> {
        let access = crate::fs_io::resolve_access_for_location(
            location,
            &self
                .runtime
                .resources()
                .planning_binding()
                .for_request(context.clone()),
        )
        .map_err(unavailable)?;
        Self::validate_cleanup_capabilities(&access)?;
        Ok(access)
    }

    fn file_io(
        &self,
        location: &str,
        context: &ConnectorRequestContext,
    ) -> Result<crate::iceberg::io::FileIO, ConnectorError> {
        let parsed = FsLocation::parse(location).map_err(|error| unavailable(error.to_string()))?;
        if !matches!(parsed.scheme(), FsScheme::Local) {
            self.action_access(location, context)?;
        }
        Ok(crate::fs_io::build_file_io_for_location(
            location,
            self.runtime
                .resources()
                .planning_binding()
                .for_request(context.clone()),
        ))
    }

    fn read_optional(
        &self,
        location: &str,
        context: &ConnectorRequestContext,
    ) -> Result<Option<Bytes>, ConnectorError> {
        let file_io = self.file_io(location, context)?;
        let input = file_io
            .new_input(location)
            .map_err(|error| unavailable(error.to_string()))?;
        let exists = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { input.exists().await })
            .map_err(unavailable)?
            .map_err(|error| unavailable(error.to_string()))?;
        if !exists {
            return Ok(None);
        }
        let input = file_io
            .new_input(location)
            .map_err(|error| unavailable(error.to_string()))?;
        let bytes = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { input.read().await })
            .map_err(unavailable)?
            .map_err(|error| unavailable(error.to_string()))?;
        Ok(Some(bytes))
    }

    fn root_for(
        &self,
        publication: novarocks_spi::connector::LakePublicationId,
    ) -> Result<String, ConnectorError> {
        // `try_new` already proved this generation has an explicit warehouse,
        // so the only arm this can take is unreachable here. Report it as the
        // refusal it is rather than as a transient failure a caller might retry.
        let table = ctas_staging_location(self.warehouse_root(), publication)
            .map_err(|_| unsupported("derive unanchored CTAS staging location"))?;
        table
            .strip_suffix("/table")
            .map(ToOwned::to_owned)
            .ok_or_else(|| invalid("CTAS staging location does not end in /table"))
    }

    fn sidecar_for(
        &self,
        publication: novarocks_spi::connector::LakePublicationId,
    ) -> Result<String, ConnectorError> {
        // See `root_for`: unreachable, and a refusal rather than a retryable
        // failure if it ever became reachable.
        let table = ctas_staging_location(self.warehouse_root(), publication)
            .map_err(|_| unsupported("derive unanchored CTAS staging location"))?;
        unanchored_ctas_provenance_location(&table)
    }

    fn sidecar_locations(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, ConnectorError> {
        let warehouse = self.warehouse_root();
        let root = format!("{warehouse}/{CTAS_STAGING_NAMESPACE}");
        let parsed = FsLocation::parse(&root).map_err(|error| unavailable(error.to_string()))?;
        match parsed.scheme() {
            FsScheme::Local => {
                let root_path = parsed.path().to_owned();
                let control = context.clone();
                let admission = self.runtime.novarocks_catalog().listing_admission();
                self.runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(async move {
                        admission
                            .run(&control, async {
                                let mut locations = Vec::new();
                                let mut budget =
                                    ConnectorListingBudget::new(ConnectorListingBound::V1)?;
                                let mut retained =
                                    ConnectorListingBudget::new(ConnectorListingBound::V1)?;
                                let root_path = std::path::Path::new(&root_path);
                                let mut entries = match tokio::fs::read_dir(root_path).await {
                                    Ok(entries) => entries,
                                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                                        return Ok(locations);
                                    }
                                    Err(error) => {
                                        return Err(unavailable(format!(
                                            "list unanchored CTAS roots: {error}"
                                        )));
                                    }
                                };
                                while let Some(entry) =
                                    entries.next_entry().await.map_err(|error| {
                                        unavailable(format!("read unanchored CTAS root: {error}"))
                                    })?
                                {
                                    Self::validate_context(&control)?;
                                    let entry_path = entry.path();
                                    let entry_name = entry_path.to_str().ok_or_else(|| {
                                        invalid("unanchored CTAS root path is not UTF-8")
                                    })?;
                                    budget.admit_names(std::iter::once(entry_name))?;
                                    entry_name
                                        .len()
                                        .checked_add(
                                            1 + CTAS_UNANCHORED_PROVENANCE_FILE.len()
                                                + "file://".len(),
                                        )
                                        .filter(|bytes| {
                                            *bytes <= ConnectorListingBound::V1.name_bytes
                                        })
                                        .ok_or_else(|| {
                                            ConnectorError::new(
                                                ConnectorErrorKind::ResourceExhausted,
                                                "unanchored CTAS sidecar name bound exceeded",
                                            )
                                        })?;
                                    let path = entry_path.join(CTAS_UNANCHORED_PROVENANCE_FILE);
                                    let is_file = match tokio::fs::metadata(&path).await {
                                        Ok(metadata) => metadata.is_file(),
                                        Err(error)
                                            if error.kind() == std::io::ErrorKind::NotFound =>
                                        {
                                            false
                                        }
                                        Err(error) => {
                                            return Err(unavailable(format!(
                                                "stat unanchored CTAS sidecar: {error}"
                                            )));
                                        }
                                    };
                                    if is_file {
                                        let location_bytes = path
                                            .to_str()
                                            .ok_or_else(|| {
                                                invalid("unanchored CTAS sidecar path is not UTF-8")
                                            })?
                                            .len()
                                            + "file://".len();
                                        if location_bytes > ConnectorListingBound::V1.name_bytes {
                                            return Err(ConnectorError::new(
                                                ConnectorErrorKind::ResourceExhausted,
                                                "unanchored CTAS sidecar name bound exceeded",
                                            ));
                                        }
                                        let path_name = path.to_str().ok_or_else(|| {
                                            invalid("unanchored CTAS sidecar path is not UTF-8")
                                        })?;
                                        retained.admit_qualified_names(
                                            "file://",
                                            std::iter::once(path_name),
                                        )?;
                                        let location = format!("file://{}", path.display());
                                        locations.push(location);
                                    }
                                }
                                locations.sort();
                                Ok(locations)
                            })
                            .await
                    })
                    .map_err(unavailable)?
            }
            FsScheme::ObjectStore | FsScheme::Hdfs => {
                let access = self.action_access(&root, context)?;
                let path = access
                    .single_relative_path()
                    .map_err(unavailable)?
                    .trim_matches('/')
                    .to_string();
                let prefix = format!("{path}/");
                let operator = access.operator();
                let location_prefix = crate::fs_io::format_resolved_location(access.handle(), "")
                    .map_err(unavailable)?;
                let control = context.clone();
                let admission = self.runtime.novarocks_catalog().listing_admission();
                self.runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(async move {
                        admission
                            .run(&control, async {
                                let mut entries = until_context(
                                    &control,
                                    operator
                                        .lister_with(&prefix)
                                        .recursive(true)
                                        .limit(ConnectorListingBound::V1.page_entries)
                                        .into_future(),
                                )
                                .await?
                                .map_err(|error| {
                                    ConnectorError::from(
                                        novarocks_fs::map_object_store_listing_error(error),
                                    )
                                })?;
                                let mut budget =
                                    ConnectorListingBudget::new(ConnectorListingBound::V1)?;
                                let mut retained =
                                    ConnectorListingBudget::new(ConnectorListingBound::V1)?;
                                let mut locations = Vec::new();
                                while let Some(entry) =
                                    until_context(&control, entries.next()).await?
                                {
                                    let entry = entry.map_err(|error| {
                                        ConnectorError::from(
                                            novarocks_fs::map_object_store_listing_error(error),
                                        )
                                    })?;
                                    budget.admit_names(std::iter::once(entry.path()))?;
                                    if !entry.path().ends_with(CTAS_UNANCHORED_PROVENANCE_FILE) {
                                        continue;
                                    }
                                    let relative_path = entry.path().trim_start_matches('/');
                                    location_prefix
                                        .len()
                                        .checked_add(relative_path.len())
                                        .filter(|bytes| {
                                            *bytes <= ConnectorListingBound::V1.name_bytes
                                        })
                                        .ok_or_else(|| {
                                            ConnectorError::new(
                                                ConnectorErrorKind::ResourceExhausted,
                                                "unanchored CTAS sidecar name bound exceeded",
                                            )
                                        })?;
                                    retained.admit_qualified_names(
                                        &location_prefix,
                                        std::iter::once(relative_path),
                                    )?;
                                    let location = crate::fs_io::format_resolved_location(
                                        access.handle(),
                                        entry.path(),
                                    )
                                    .map_err(unavailable)?;
                                    locations.push(location);
                                }
                                locations.sort_unstable();
                                locations.dedup();
                                Ok(locations)
                            })
                            .await
                    })
                    .map_err(unavailable)?
            }
        }
    }
}

fn cleanup_future_layout<A, B, C, F: std::future::Future>(
    _: fn(A, B, C) -> F,
) -> std::alloc::Layout {
    std::alloc::Layout::new::<F>()
}
async fn discover_unanchored_ctas_entry(
    owner: IcebergUnanchoredCtasCleanupAdapter,
    request: ConnectorCtasUnanchoredDiscoveryRequest,
    context: ConnectorRequestContext,
) -> Result<Vec<ConnectorCtasUnanchoredProvenance>, ConnectorError> {
    owner
        .discover_unanchored_ctas_async_original(request, context)
        .await
}
async fn inspect_then_delete_unanchored_ctas_entry(
    owner: IcebergUnanchoredCtasCleanupAdapter,
    request: ConnectorCtasUnanchoredCleanupRequest,
    context: ConnectorRequestContext,
) -> Result<ConnectorCtasUnanchoredCleanupOutcome, ConnectorError> {
    owner
        .inspect_then_delete_unanchored_ctas_async_original(request, context)
        .await
}

impl IcebergUnanchoredCtasCleanupAdapter {
    async fn read_optional_async(
        &self,
        location: &str,
        context: &ConnectorRequestContext,
    ) -> Result<Option<Bytes>, ConnectorError> {
        let file_io = self.file_io(location, context)?;
        let input = file_io
            .new_input(location)
            .map_err(|error| unavailable(error.to_string()))?;
        let exists = input
            .exists()
            .await
            .map_err(|error| unavailable(error.to_string()))?;
        if !exists {
            return Ok(None);
        }
        let input = file_io
            .new_input(location)
            .map_err(|error| unavailable(error.to_string()))?;
        let bytes = input
            .read()
            .await
            .map_err(|error| unavailable(error.to_string()))?;
        Ok(Some(bytes))
    }

    async fn sidecar_locations_async(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, ConnectorError> {
        let warehouse = self.warehouse_root();
        let root = format!("{warehouse}/{CTAS_STAGING_NAMESPACE}");
        let parsed = FsLocation::parse(&root).map_err(|error| unavailable(error.to_string()))?;
        match parsed.scheme() {
            FsScheme::Local => {
                let root_path = parsed.path().to_owned();
                let control = context.clone();
                let admission = self.runtime.novarocks_catalog().listing_admission();
                async move {
                    admission
                        .run(&control, async {
                            let mut locations = Vec::new();
                            let mut budget =
                                ConnectorListingBudget::new(ConnectorListingBound::V1)?;
                            let mut retained =
                                ConnectorListingBudget::new(ConnectorListingBound::V1)?;
                            let root_path = std::path::Path::new(&root_path);
                            let mut entries = match tokio::fs::read_dir(root_path).await {
                                Ok(entries) => entries,
                                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                                    return Ok(locations);
                                }
                                Err(error) => {
                                    return Err(unavailable(format!(
                                        "list unanchored CTAS roots: {error}"
                                    )));
                                }
                            };
                            while let Some(entry) = entries.next_entry().await.map_err(|error| {
                                unavailable(format!("read unanchored CTAS root: {error}"))
                            })? {
                                Self::validate_context(&control)?;
                                let entry_path = entry.path();
                                let entry_name = entry_path.to_str().ok_or_else(|| {
                                    invalid("unanchored CTAS root path is not UTF-8")
                                })?;
                                budget.admit_names(std::iter::once(entry_name))?;
                                entry_name
                                    .len()
                                    .checked_add(
                                        1 + CTAS_UNANCHORED_PROVENANCE_FILE.len() + "file://".len(),
                                    )
                                    .filter(|bytes| *bytes <= ConnectorListingBound::V1.name_bytes)
                                    .ok_or_else(|| {
                                        ConnectorError::new(
                                            ConnectorErrorKind::ResourceExhausted,
                                            "unanchored CTAS sidecar name bound exceeded",
                                        )
                                    })?;
                                let path = entry_path.join(CTAS_UNANCHORED_PROVENANCE_FILE);
                                let is_file = match tokio::fs::metadata(&path).await {
                                    Ok(metadata) => metadata.is_file(),
                                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                                        false
                                    }
                                    Err(error) => {
                                        return Err(unavailable(format!(
                                            "stat unanchored CTAS sidecar: {error}"
                                        )));
                                    }
                                };
                                if is_file {
                                    let location_bytes = path
                                        .to_str()
                                        .ok_or_else(|| {
                                            invalid("unanchored CTAS sidecar path is not UTF-8")
                                        })?
                                        .len()
                                        + "file://".len();
                                    if location_bytes > ConnectorListingBound::V1.name_bytes {
                                        return Err(ConnectorError::new(
                                            ConnectorErrorKind::ResourceExhausted,
                                            "unanchored CTAS sidecar name bound exceeded",
                                        ));
                                    }
                                    let path_name = path.to_str().ok_or_else(|| {
                                        invalid("unanchored CTAS sidecar path is not UTF-8")
                                    })?;
                                    retained.admit_qualified_names(
                                        "file://",
                                        std::iter::once(path_name),
                                    )?;
                                    let location = format!("file://{}", path.display());
                                    locations.push(location);
                                }
                            }
                            locations.sort();
                            Ok(locations)
                        })
                        .await
                }
                .await
            }
            FsScheme::ObjectStore | FsScheme::Hdfs => {
                let access = self.action_access(&root, context)?;
                let path = access
                    .single_relative_path()
                    .map_err(unavailable)?
                    .trim_matches('/')
                    .to_string();
                let prefix = format!("{path}/");
                let operator = access.operator();
                let location_prefix = crate::fs_io::format_resolved_location(access.handle(), "")
                    .map_err(unavailable)?;
                let control = context.clone();
                let admission = self.runtime.novarocks_catalog().listing_admission();
                async move {
                    admission
                        .run(&control, async {
                            let mut entries = until_context(
                                &control,
                                operator
                                    .lister_with(&prefix)
                                    .recursive(true)
                                    .limit(ConnectorListingBound::V1.page_entries)
                                    .into_future(),
                            )
                            .await?
                            .map_err(|error| {
                                ConnectorError::from(novarocks_fs::map_object_store_listing_error(
                                    error,
                                ))
                            })?;
                            let mut budget =
                                ConnectorListingBudget::new(ConnectorListingBound::V1)?;
                            let mut retained =
                                ConnectorListingBudget::new(ConnectorListingBound::V1)?;
                            let mut locations = Vec::new();
                            while let Some(entry) = until_context(&control, entries.next()).await? {
                                let entry = entry.map_err(|error| {
                                    ConnectorError::from(
                                        novarocks_fs::map_object_store_listing_error(error),
                                    )
                                })?;
                                budget.admit_names(std::iter::once(entry.path()))?;
                                if !entry.path().ends_with(CTAS_UNANCHORED_PROVENANCE_FILE) {
                                    continue;
                                }
                                let relative_path = entry.path().trim_start_matches('/');
                                location_prefix
                                    .len()
                                    .checked_add(relative_path.len())
                                    .filter(|bytes| *bytes <= ConnectorListingBound::V1.name_bytes)
                                    .ok_or_else(|| {
                                        ConnectorError::new(
                                            ConnectorErrorKind::ResourceExhausted,
                                            "unanchored CTAS sidecar name bound exceeded",
                                        )
                                    })?;
                                retained.admit_qualified_names(
                                    &location_prefix,
                                    std::iter::once(relative_path),
                                )?;
                                let location = crate::fs_io::format_resolved_location(
                                    access.handle(),
                                    entry.path(),
                                )
                                .map_err(unavailable)?;
                                locations.push(location);
                            }
                            locations.sort_unstable();
                            locations.dedup();
                            Ok(locations)
                        })
                        .await
                }
                .await
            }
        }
    }

    async fn discover_unanchored_ctas_async_original(
        &self,
        request: ConnectorCtasUnanchoredDiscoveryRequest,
        context: ConnectorRequestContext,
    ) -> Result<Vec<ConnectorCtasUnanchoredProvenance>, ConnectorError> {
        Self::validate_context(&context)?;
        if request.owner != self.owner() {
            return Err(invalid("unanchored CTAS discovery has a foreign owner"));
        }
        self.validate_warehouse(&request.warehouse_root)?;
        let mut candidates = Vec::new();
        for sidecar in self.sidecar_locations_async(&context).await? {
            Self::validate_context(&context)?;
            let Some(bytes) = self.read_optional_async(&sidecar, &context).await? else {
                continue;
            };
            let Ok(provenance) = decode_unanchored_ctas_provenance(&bytes) else {
                continue;
            };
            if provenance.target.instance_id != self.descriptor.instance_id
                || provenance.created_at_ms >= request.cutoff_ms
            {
                continue;
            }
            let expected_sidecar = self.sidecar_for(provenance.publication_id)?;
            if sidecar != expected_sidecar {
                continue;
            }
            candidates.push(provenance);
        }
        candidates.sort_by_key(|candidate| candidate.publication_id);
        candidates.dedup_by_key(|candidate| candidate.publication_id);
        Ok(candidates)
    }

    async fn inspect_then_delete_unanchored_ctas_async_original(
        &self,
        request: ConnectorCtasUnanchoredCleanupRequest,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorCtasUnanchoredCleanupOutcome, ConnectorError> {
        Self::validate_context(&context)?;
        if request.owner != self.owner() {
            return Err(invalid("unanchored CTAS cleanup has a foreign owner"));
        }
        self.validate_warehouse(&request.warehouse_root)?;
        if request.provenance.created_at_ms >= request.cutoff_ms {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        }
        let sidecar = self.sidecar_for(request.provenance.publication_id)?;
        let Some(bytes) = self.read_optional_async(&sidecar, &context).await? else {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        };
        let Ok(observed) = decode_unanchored_ctas_provenance(&bytes) else {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        };
        if observed != request.provenance {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        }
        // The current target must be a fresh exact lookup. A matching UUID is
        // the successful publication and pins this root. A different UUID is
        // an ambiguous drop/recreate observation: the old staging root may be
        // shared or otherwise still operator-relevant, so it also leaks. Only
        // a definite NotFound target permits unanchored-root deletion.
        self.runtime
            .control_state()
            .invalidate_table(&observed.target.namespace, &observed.target.table);
        match self
            .runtime
            .observe_table_classified_for_request_with_snapshots_async(
                &observed.target.namespace,
                &observed.target.table,
                &context,
                crate::catalog::SnapshotLoading::All,
            )
            .await
        {
            Ok(_physical) => {
                let _expected_uuid = observed.staged_table_uuid.ok_or_else(|| {
                    invalid("unanchored CTAS cleanup provenance lacks staged table UUID")
                })?;
                return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
            }
            Err((ConnectorErrorKind::NotFound, _)) => {}
            Err((kind, message)) => return Err(ConnectorError::new(kind, message)),
        }
        let root = self.root_for(observed.publication_id)?;
        let access = self.action_access(&root, &context)?;
        let path = access
            .single_relative_path()
            .map_err(unavailable)?
            .to_owned();
        let admission = self.runtime.novarocks_catalog().listing_admission();
        let delete_started = Arc::new(AtomicBool::new(false));
        let started = Arc::clone(&delete_started);
        let delete = async move {
            admission
                .run(&context, async {
                    delete_prefix_bounded_tracking(
                        access.operator(),
                        &path,
                        &context,
                        ConnectorListingBound::V1,
                        &started,
                    )
                    .await
                })
                .await
        }
        .await;
        cleanup_delete_outcome(Ok(delete), delete_started.load(Ordering::Acquire))
    }
}

impl ConnectorUnanchoredCtasCleanup for IcebergUnanchoredCtasCleanupAdapter {
    fn discover_unanchored_ctas_async(
        &self,
        request: ConnectorCtasUnanchoredDiscoveryRequest,
        context: ConnectorRequestContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Vec<ConnectorCtasUnanchoredProvenance>, ConnectorError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(discover_unanchored_ctas_entry(
            self.clone(),
            request,
            context,
        ))
    }
    fn inspect_then_delete_unanchored_ctas_async(
        &self,
        request: ConnectorCtasUnanchoredCleanupRequest,
        context: ConnectorRequestContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ConnectorCtasUnanchoredCleanupOutcome, ConnectorError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(inspect_then_delete_unanchored_ctas_entry(
            self.clone(),
            request,
            context,
        ))
    }

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }

    fn warehouse_root(&self) -> Result<Arc<str>, ConnectorError> {
        Ok(Arc::from(self.warehouse_root()))
    }

    fn discover_unanchored_ctas(
        &self,
        request: ConnectorCtasUnanchoredDiscoveryRequest,
        context: ConnectorRequestContext,
    ) -> Result<Vec<ConnectorCtasUnanchoredProvenance>, ConnectorError> {
        Self::validate_context(&context)?;
        if request.owner != self.owner() {
            return Err(invalid("unanchored CTAS discovery has a foreign owner"));
        }
        self.validate_warehouse(&request.warehouse_root)?;
        let mut candidates = Vec::new();
        for sidecar in self.sidecar_locations(&context)? {
            Self::validate_context(&context)?;
            let Some(bytes) = self.read_optional(&sidecar, &context)? else {
                continue;
            };
            let Ok(provenance) = decode_unanchored_ctas_provenance(&bytes) else {
                continue;
            };
            if provenance.target.instance_id != self.descriptor.instance_id
                || provenance.created_at_ms >= request.cutoff_ms
            {
                continue;
            }
            let expected_sidecar = self.sidecar_for(provenance.publication_id)?;
            if sidecar != expected_sidecar {
                continue;
            }
            candidates.push(provenance);
        }
        candidates.sort_by_key(|candidate| candidate.publication_id);
        candidates.dedup_by_key(|candidate| candidate.publication_id);
        Ok(candidates)
    }

    fn inspect_then_delete_unanchored_ctas(
        &self,
        request: ConnectorCtasUnanchoredCleanupRequest,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorCtasUnanchoredCleanupOutcome, ConnectorError> {
        Self::validate_context(&context)?;
        if request.owner != self.owner() {
            return Err(invalid("unanchored CTAS cleanup has a foreign owner"));
        }
        self.validate_warehouse(&request.warehouse_root)?;
        if request.provenance.created_at_ms >= request.cutoff_ms {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        }
        let sidecar = self.sidecar_for(request.provenance.publication_id)?;
        let Some(bytes) = self.read_optional(&sidecar, &context)? else {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        };
        let Ok(observed) = decode_unanchored_ctas_provenance(&bytes) else {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        };
        if observed != request.provenance {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        }
        // The current target must be a fresh exact lookup. A matching UUID is
        // the successful publication and pins this root. A different UUID is
        // an ambiguous drop/recreate observation: the old staging root may be
        // shared or otherwise still operator-relevant, so it also leaks. Only
        // a definite NotFound target permits unanchored-root deletion.
        self.runtime
            .control_state()
            .invalidate_table(&observed.target.namespace, &observed.target.table);
        match self
            .runtime
            .load_table_classified(&observed.target.namespace, &observed.target.table)
        {
            Ok(_physical) => {
                let _expected_uuid = observed.staged_table_uuid.ok_or_else(|| {
                    invalid("unanchored CTAS cleanup provenance lacks staged table UUID")
                })?;
                return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
            }
            Err((ConnectorErrorKind::NotFound, _)) => {}
            Err((kind, message)) => return Err(ConnectorError::new(kind, message)),
        }
        let root = self.root_for(observed.publication_id)?;
        let access = self.action_access(&root, &context)?;
        let path = access
            .single_relative_path()
            .map_err(unavailable)?
            .to_owned();
        let admission = self.runtime.novarocks_catalog().listing_admission();
        let delete_started = Arc::new(AtomicBool::new(false));
        let started = Arc::clone(&delete_started);
        let delete = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                admission
                    .run(&context, async {
                        delete_prefix_bounded_tracking(
                            access.operator(),
                            &path,
                            &context,
                            ConnectorListingBound::V1,
                            &started,
                        )
                        .await
                    })
                    .await
            });
        cleanup_delete_outcome(delete, delete_started.load(Ordering::Acquire))
    }
}

fn cleanup_delete_outcome(
    delete: Result<Result<(), ConnectorError>, String>,
    delete_started: bool,
) -> Result<ConnectorCtasUnanchoredCleanupOutcome, ConnectorError> {
    match delete {
        Ok(Err(error)) if !delete_started => Err(error),
        Err(error) if !delete_started => Err(unavailable(error)),
        Ok(Ok(())) => Ok(ConnectorCtasUnanchoredCleanupOutcome::Deleted),
        Ok(Err(error)) => Ok(ConnectorCtasUnanchoredCleanupOutcome::CommitUnknown {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                format!("delete unanchored CTAS root: {error}"),
            ),
        }),
        Err(error) => Ok(ConnectorCtasUnanchoredCleanupOutcome::CommitUnknown {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                format!("run unanchored CTAS root delete: {error}"),
            ),
        }),
    }
}

/// Drop pending SDK IO at the request's original deadline or stop signal.
async fn until_context<T>(
    context: &ConnectorRequestContext,
    future: impl std::future::Future<Output = T>,
) -> Result<T, ConnectorError> {
    IcebergUnanchoredCtasCleanupAdapter::validate_context(context)?;
    tokio::select! {
        biased;
        _ = context.stop().stopped() => Err(ConnectorError::new(ConnectorErrorKind::Cancelled,
            "unanchored CTAS cleanup request was cancelled")),
        _ = tokio::time::sleep_until(context.deadline().into()) => Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded, "unanchored CTAS cleanup deadline elapsed")),
        result = future => Ok(result),
    }
}

#[cfg(test)]
async fn delete_prefix_bounded(
    operator: opendal::Operator,
    path: &str,
    context: &ConnectorRequestContext,
    bound: ConnectorListingBound,
) -> Result<(), ConnectorError> {
    delete_prefix_bounded_tracking(operator, path, context, bound, &AtomicBool::new(false)).await
}

/// Preserve source deletion order, retaining at most one small batch.
/// Only a pass that began a delete can have an unknown destructive outcome.
async fn delete_prefix_bounded_tracking(
    operator: opendal::Operator,
    path: &str,
    context: &ConnectorRequestContext,
    bound: ConnectorListingBound,
    delete_started: &AtomicBool,
) -> Result<(), ConnectorError> {
    let mut budget = ConnectorListingBudget::new(bound)?;
    let normalized_root = path.trim_end_matches('/');
    normalized_root
        .len()
        .checked_add(1 + CTAS_UNANCHORED_PROVENANCE_FILE.len())
        .filter(|bytes| *bytes <= bound.name_bytes)
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "unanchored CTAS recovery marker name bound exceeded",
            )
        })?;
    let recovery_marker = format!("{normalized_root}/{CTAS_UNANCHORED_PROVENANCE_FILE}");
    let mut entries = until_context(
        context,
        operator
            .lister_with(path)
            .recursive(true)
            .limit(bound.page_entries)
            .into_future(),
    )
    .await?
    .map_err(|error| ConnectorError::from(novarocks_fs::map_object_store_listing_error(error)))?;
    let mut batch = Vec::with_capacity(bound.page_entries);
    // Keep the recovery marker until every listed child has been deleted.
    let mut provenance = None;
    let mut root_directory = None;
    while let Some(entry) = until_context(context, entries.next()).await? {
        let entry = entry.map_err(|error| {
            ConnectorError::from(novarocks_fs::map_object_store_listing_error(error))
        })?;
        if entry.path().trim_end_matches('/') == path.trim_end_matches('/') {
            root_directory = Some(entry.path().to_owned());
            continue;
        }
        budget.admit_names(std::iter::once(entry.path()))?;
        if entry.path().trim_end_matches('/') == recovery_marker {
            provenance = Some(entry.path().to_owned());
            continue;
        }
        if crate::catalog_metadata_root::is_catalog_metadata_root(entry.path()) {
            return Err(unsupported(
                "unanchored CTAS cleanup encountered a protected catalog metadata root",
            ));
        }
        batch.push(entry.path().to_owned());
        if batch.len() == bound.page_entries {
            delete_started.store(true, Ordering::Release);
            until_context(context, operator.delete_iter(std::mem::take(&mut batch)))
                .await?
                .map_err(|error| unavailable(error.to_string()))?;
        }
    }
    if !batch.is_empty() {
        delete_started.store(true, Ordering::Release);
        until_context(context, operator.delete_iter(batch))
            .await?
            .map_err(|error| unavailable(error.to_string()))?;
    }
    for last in provenance.into_iter().chain(root_directory) {
        delete_started.store(true, Ordering::Release);
        until_context(context, operator.delete(&last))
            .await?
            .map_err(|error| unavailable(error.to_string()))?;
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message.into())
}

pub(crate) fn async_source_backing_bytes() -> Result<usize, ConnectorError> {
    let layouts = [
        cleanup_future_layout(discover_unanchored_ctas_entry),
        cleanup_future_layout(inspect_then_delete_unanchored_ctas_entry),
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            AtomicBool,
        >())
        .map_err(|error| invalid(error.to_string()))?,
    ];
    layouts.into_iter().try_fold(0usize, |bytes, layout| {
        bytes
            .checked_add(layout.size())
            .ok_or_else(|| invalid("CTAS cleanup source quote overflow"))
    })
}

#[cfg(test)]
mod listing_tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorStopOwner, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };
    use std::time::Duration;

    fn context(stop: &ConnectorStopOwner) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(10),
            stop.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn cleanup_deletes_multiple_finite_batches_without_collecting_the_prefix() {
        let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
        for index in 0..9 {
            operator
                .write(&format!("root/file{index}"), "data")
                .await
                .unwrap();
        }
        operator.write("neighbor/keep", "data").await.unwrap();
        let stop = ConnectorStopOwner::new();
        delete_prefix_bounded(
            operator.clone(),
            "root/",
            &context(&stop),
            ConnectorListingBound {
                page_entries: 2,
                ..ConnectorListingBound::V1
            },
        )
        .await
        .unwrap();
        assert!(operator.list("root/").await.unwrap().is_empty());
        assert!(operator.exists("neighbor/keep").await.unwrap());
    }

    #[tokio::test]
    async fn cleanup_entry_bound_refuses_the_destructive_pass_without_truncation_success() {
        let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
        for index in 0..3 {
            operator
                .write(&format!("root/file{index}"), "data")
                .await
                .unwrap();
        }
        let stop = ConnectorStopOwner::new();
        let started = AtomicBool::new(false);
        let error = delete_prefix_bounded_tracking(
            operator.clone(),
            "root/",
            &context(&stop),
            ConnectorListingBound {
                entries: 2,
                page_entries: 1,
                ..ConnectorListingBound::V1
            },
            &started,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert!(started.load(Ordering::Acquire));
        assert!(matches!(
            cleanup_delete_outcome(Ok(Err(error)), true).unwrap(),
            ConnectorCtasUnanchoredCleanupOutcome::CommitUnknown { .. }
        ));
        assert!(!operator.list("root/").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cleanup_local_nested_directories_stream_and_preserve_marker_on_refusal() {
        let directory = tempfile::tempdir().unwrap();
        let operator = opendal::Operator::new(
            opendal::services::Fs::default().root(directory.path().to_str().unwrap()),
        )
        .unwrap();
        operator.write("root/table/data/a", "data").await.unwrap();
        operator.write("root/table/data/b", "data").await.unwrap();
        operator
            .write(&format!("root/{CTAS_UNANCHORED_PROVENANCE_FILE}"), "marker")
            .await
            .unwrap();
        let stop = ConnectorStopOwner::new();
        let bound = ConnectorListingBound {
            page_entries: 1,
            entries: 1,
            ..ConnectorListingBound::V1
        };
        assert_eq!(
            delete_prefix_bounded(operator.clone(), "root/", &context(&stop), bound)
                .await
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert!(
            operator
                .exists(&format!("root/{CTAS_UNANCHORED_PROVENANCE_FILE}"))
                .await
                .unwrap()
        );
        delete_prefix_bounded(
            operator.clone(),
            "root/",
            &context(&stop),
            ConnectorListingBound {
                page_entries: 1,
                ..ConnectorListingBound::V1
            },
        )
        .await
        .unwrap();
        assert!(!directory.path().join("root").exists());
    }

    #[tokio::test]
    async fn cleanup_listing_refusal_before_first_delete_keeps_exact_error_and_files() {
        let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
        for index in 0..3 {
            operator
                .write(&format!("root/file{index}"), "data")
                .await
                .unwrap();
        }
        let stop = ConnectorStopOwner::new();
        let started = AtomicBool::new(false);
        let error = delete_prefix_bounded_tracking(
            operator.clone(),
            "root/",
            &context(&stop),
            ConnectorListingBound {
                entries: 2,
                page_entries: 256,
                ..ConnectorListingBound::V1
            },
            &started,
        )
        .await
        .unwrap_err();
        assert_eq!(
            cleanup_delete_outcome(Ok(Err(error)), started.load(Ordering::Acquire))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert!(!started.load(Ordering::Acquire));
        for index in 0..3 {
            assert!(operator.exists(&format!("root/file{index}")).await.unwrap());
        }
    }

    #[tokio::test]
    async fn cleanup_expired_admission_keeps_exact_deadline_without_starting_delete() {
        let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
        operator.write("root/keep", "data").await.unwrap();
        let stop = ConnectorStopOwner::new();
        let expired = ConnectorRequestContext::try_new(
            Instant::now() - Duration::from_secs(1),
            stop.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap();
        let admission = crate::catalog::listing_admission::ListingAdmission::default();
        let started = AtomicBool::new(false);
        let error = admission
            .run(&expired, async {
                delete_prefix_bounded_tracking(
                    operator.clone(),
                    "root/",
                    &expired,
                    ConnectorListingBound::V1,
                    &started,
                )
                .await
            })
            .await
            .unwrap_err();
        assert_eq!(
            cleanup_delete_outcome(Ok(Err(error)), started.load(Ordering::Acquire))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::DeadlineExceeded
        );
        assert!(!started.load(Ordering::Acquire));
        assert!(operator.exists("root/keep").await.unwrap());
    }

    #[tokio::test]
    async fn cleanup_stop_is_refused_before_first_delete() {
        let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
        operator.write("root/keep", "data").await.unwrap();
        let stop = ConnectorStopOwner::new();
        let context = context(&stop);
        stop.request_stop();
        let started = AtomicBool::new(false);
        assert_eq!(
            delete_prefix_bounded_tracking(
                operator.clone(),
                "root/",
                &context,
                ConnectorListingBound::V1,
                &started,
            )
            .await
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::Cancelled
        );
        assert!(!started.load(Ordering::Acquire));
        assert!(operator.exists("root/keep").await.unwrap());
    }
}
