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

//! Runtime-private state for one Iceberg control generation.
//!
//! A control generation owns precisely one catalog client and the physical
//! caches derived from its parsed configuration.  The frontend owns the map
//! of generations; this value deliberately has no catalog-name registry and
//! never falls back to a process-global Tokio runtime.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
    ConnectorVendedCredentialLeaseCollectionPort, ConnectorVendedS3CredentialLeaseRefresher,
    CredentialLoadTableDelegation, CredentialRenewalPath, MaterializationContext,
};
use novarocks_types::naming::normalize_identifier;

use crate::catalog_control::IcebergCatalogControlState;
use crate::iceberg::{NamespaceIdent, TableIdent};
use crate::loaded_table::{
    IcebergAttemptTableAccess, IcebergPhysicalTable, IcebergRestLoadTableVendedS3LeaseRefresher,
    IcebergRestVendedS3LeaseRefresher, IcebergVendedCredentialLeaseSeed,
    IcebergVendedS3RenewalCapability,
};
use crate::resources::IcebergMetadataResources;

#[cfg(test)]
fn original_classified_error((kind, message): (ConnectorErrorKind, String)) -> ConnectorError {
    ConnectorError::new(kind, message)
}
fn classified_original_error(error: ConnectorError) -> (ConnectorErrorKind, String) {
    (error.kind(), error.to_string())
}

static NEXT_ATTEMPT_METADATA_CACHE_OWNER: AtomicU64 = AtomicU64::new(1);

/// Provider-private, attempt-local successful metadata observation cache. Only
/// resource-free frozen facts may enter it: retaining a request-bound FileIO
/// here would keep its own `ConnectorRequestScope` alive through a strong cycle.
#[derive(Default)]
struct AttemptMetadataTableCache {
    entries: Mutex<HashMap<AttemptMetadataTableKey, Arc<AttemptMetadataTableEntry>>>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct AttemptMetadataTableKey {
    owner: u64,
    namespace: String,
    table: String,
}

struct AttemptMetadataTableEntry {
    result: Mutex<Option<Result<IcebergAttemptTableAccess, ConnectorError>>>,
    ready: Condvar,
    async_ready: tokio::sync::Notify,
}

impl AttemptMetadataTableEntry {
    fn loading() -> Self {
        Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
            async_ready: tokio::sync::Notify::new(),
        }
    }
}

impl AttemptMetadataTableCache {
    #[cfg(test)]
    fn get_or_load(
        &self,
        key: AttemptMetadataTableKey,
        load: impl FnOnce() -> Result<IcebergAttemptTableAccess, (ConnectorErrorKind, String)>,
    ) -> Result<IcebergAttemptTableAccess, (ConnectorErrorKind, String)> {
        self.get_or_load_original(key, || load().map_err(original_classified_error))
            .map_err(|error| (error.kind(), error.message().to_owned()))
    }
    #[cfg(test)]
    async fn get_or_load_async<F>(
        &self,
        key: AttemptMetadataTableKey,
        load: F,
    ) -> Result<IcebergAttemptTableAccess, (ConnectorErrorKind, String)>
    where
        F: std::future::Future<
                Output = Result<IcebergAttemptTableAccess, (ConnectorErrorKind, String)>,
            >,
    {
        self.get_or_load_original_async(key, async {
            load.await.map_err(original_classified_error)
        })
        .await
        .map_err(|error| (error.kind(), error.message().to_owned()))
    }
    fn get_or_load_original(
        &self,
        key: AttemptMetadataTableKey,
        load: impl FnOnce() -> Result<IcebergAttemptTableAccess, ConnectorError>,
    ) -> Result<IcebergAttemptTableAccess, ConnectorError> {
        let (entry, loader) = {
            let mut entries = self.entries.lock().expect("attempt metadata cache lock");
            match entries.get(&key) {
                Some(entry) => (Arc::clone(entry), false),
                None => {
                    let entry = Arc::new(AttemptMetadataTableEntry::loading());
                    entries.insert(key.clone(), Arc::clone(&entry));
                    (entry, true)
                }
            }
        };
        if loader {
            let result = load();
            // A request can observe absence, create the object, then load it
            // again for publication or bootstrap. Keep the single-flight
            // result for existing waiters, but do not let a failed observation
            // become a stale negative cache across that external effect.
            if result.is_err() {
                let mut entries = self.entries.lock().expect("attempt metadata cache lock");
                if entries
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(current, &entry))
                {
                    entries.remove(&key);
                }
            }
            let mut stored = entry.result.lock().expect("attempt metadata entry lock");
            debug_assert!(stored.is_none(), "attempt metadata entry completes once");
            *stored = Some(result.clone());
            drop(stored);
            entry.ready.notify_all();
            entry.async_ready.notify_waiters();
            return result;
        }

        let mut stored = entry.result.lock().expect("attempt metadata entry lock");
        while stored.is_none() {
            stored = entry
                .ready
                .wait(stored)
                .expect("attempt metadata entry wait");
        }
        stored
            .as_ref()
            .expect("attempt metadata entry completed")
            .clone()
    }
    async fn get_or_load_original_async<F>(
        &self,
        key: AttemptMetadataTableKey,
        load: F,
    ) -> Result<IcebergAttemptTableAccess, ConnectorError>
    where
        F: std::future::Future<Output = Result<IcebergAttemptTableAccess, ConnectorError>>,
    {
        let (entry, loader) = {
            let mut entries = self.entries.lock().expect("attempt metadata cache lock");
            match entries.get(&key) {
                Some(entry) => (Arc::clone(entry), false),
                None => {
                    let entry = Arc::new(AttemptMetadataTableEntry::loading());
                    entries.insert(key.clone(), Arc::clone(&entry));
                    (entry, true)
                }
            }
        };
        if !loader {
            loop {
                let ready = entry.async_ready.notified();
                tokio::pin!(ready);
                ready.as_mut().enable();
                let observed = {
                    entry
                        .result
                        .lock()
                        .expect("attempt metadata entry lock")
                        .clone()
                };
                if let Some(result) = observed {
                    return result;
                }
                ready.await;
            }
        }
        let mut publication = AttemptMetadataLoad {
            cache: self,
            key,
            entry,
            complete: false,
        };
        let result = load.await;
        if result.is_err() {
            publication.remove_failed_entry();
        }
        *publication
            .entry
            .result
            .lock()
            .expect("attempt metadata entry lock") = Some(result.clone());
        publication.complete = true;
        publication.entry.ready.notify_all();
        publication.entry.async_ready.notify_waiters();
        result
    }
}

// An async loader can be cancelled before publishing. Existing sync and async
// waiters receive its real cancellation; a later attempt may load a new entry.
struct AttemptMetadataLoad<'a> {
    cache: &'a AttemptMetadataTableCache,
    key: AttemptMetadataTableKey,
    entry: Arc<AttemptMetadataTableEntry>,
    complete: bool,
}
impl AttemptMetadataLoad<'_> {
    fn remove_failed_entry(&self) {
        let mut entries = self
            .cache
            .entries
            .lock()
            .expect("attempt metadata cache lock");
        if entries
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.entry))
        {
            entries.remove(&self.key);
        }
    }
}
impl Drop for AttemptMetadataLoad<'_> {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        self.remove_failed_entry();
        *self
            .entry
            .result
            .lock()
            .expect("attempt metadata entry lock") = Some(Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Iceberg metadata loader exited before publishing its observation",
        )));
        self.entry.ready.notify_all();
        self.entry.async_ready.notify_waiters();
    }
}

/// Everything one Iceberg control generation owns.
///
/// The generation holds exactly one catalog, and every operation family reaches
/// it through [`IcebergMetadataContext::novarocks_catalog`]. There is no second
/// handle and no concrete-kind slot: a generation that held two clients would
/// have two pieces of in-memory state that disagree about the same lake.
///
/// Publication callers hand a complete frozen request to this owner; the
/// journal retains its single dispatch conclusion and independent finalization.
#[derive(Clone)]
pub struct IcebergMetadataContext {
    control_state: IcebergCatalogControlState,
    resources: IcebergMetadataResources,
    /// The one semantic owner of catalog behavior for this generation.
    novarocks_catalog: Arc<dyn crate::catalog::NovaRocksCatalog>,
    /// Proven-committed drops awaiting collection. Generation-local and
    /// bounded; retired with the generation.
    drop_cleanup: Arc<crate::catalog_control::drop_cleanup::DropCleanupQueue>,
    /// Opaque per-generation cache identity. It is local-only and prevents
    /// two catalog generations in one query scope from sharing a table view.
    attempt_metadata_cache_owner: u64,
    table_coordination: Arc<crate::catalog::table_coordination::IcebergTableCoordination>,
}

#[allow(dead_code)]
impl IcebergMetadataContext {
    /// Construct one fully local provider generation.  REST and HMS client
    /// initialization is polled only through the runtime injected by server
    /// composition, so factory construction remains deterministic in every
    /// frontend role.
    pub fn try_new(
        control_state: IcebergCatalogControlState,
        resources: IcebergMetadataResources,
    ) -> Result<Self, String> {
        Self::try_new_with_rest_access_delegation(
            control_state,
            resources,
            crate::catalog_runtime::RestAccessDelegationMode::Static,
        )
    }

    /// Construct a generation from its explicit typed credential mode.  This
    /// mode comes from `CatalogProperties`, never from a REST response.
    pub(crate) fn try_new_with_rest_access_delegation(
        control_state: IcebergCatalogControlState,
        resources: IcebergMetadataResources,
        rest_access_delegation: crate::catalog_runtime::RestAccessDelegationMode,
    ) -> Result<Self, String> {
        Self::try_new_with_rest_access_delegation_and_control(
            control_state,
            resources,
            rest_access_delegation,
            None,
        )
    }

    pub(crate) fn try_new_with_rest_access_delegation_and_control(
        control_state: IcebergCatalogControlState,
        resources: IcebergMetadataResources,
        rest_access_delegation: crate::catalog_runtime::RestAccessDelegationMode,
        control: Option<MaterializationContext>,
    ) -> Result<Self, String> {
        if let Some(control) = &control {
            ConnectorOperationControl::check_active(control).map_err(|error| error.to_string())?;
        }
        let configuration = control_state.configuration().clone();
        let binding = resources.planning_binding().clone();
        let build_control = control.clone();
        let iceberg_runtime = resources.catalog_runtime().iceberg_runtime().clone();
        let activity = resources.catalog_runtime().activity().cloned();
        let catalog = resources
            .catalog_runtime()
            .block_on(async move {
                if let Some(control) = &build_control {
                    ConnectorOperationControl::check_active(control)
                        .map_err(|error| error.to_string())?;
                }
                let catalog =
                    crate::catalog_runtime::build_catalog_client_with_rest_access_delegation(
                        &configuration,
                        binding,
                        rest_access_delegation,
                        iceberg_runtime,
                        activity,
                    )
                    .await?;
                if let Some(control) = &build_control {
                    ConnectorOperationControl::check_active(control)
                        .map_err(|error| error.to_string())?;
                }
                Ok::<_, String>(catalog)
            })?
            .map_err(|error| format!("build Iceberg control-generation catalog: {error}"))?;
        if let Some(control) = &control {
            ConnectorOperationControl::check_active(control).map_err(|error| error.to_string())?;
        }
        // Every handle below is derived from this one client. Building a second
        // one for the owner would give the generation two clients with separate
        // in-memory state, and they would disagree about the same lake -- a
        // table dropped through one still resolving through the other.
        let novarocks_catalog = crate::catalog::factory::NovaRocksCatalogFactory::adopt(
            control_state.configuration(),
            &catalog,
        )?;
        Ok(Self {
            control_state,
            resources,
            novarocks_catalog,
            drop_cleanup: Arc::new(crate::catalog_control::drop_cleanup::DropCleanupQueue::new()),
            table_coordination: Arc::new(
                crate::catalog::table_coordination::IcebergTableCoordination::default(),
            ),
            attempt_metadata_cache_owner: NEXT_ATTEMPT_METADATA_CACHE_OWNER
                .fetch_add(1, Ordering::Relaxed),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_catalog_for_test(
        control_state: IcebergCatalogControlState,
        resources: IcebergMetadataResources,
        novarocks_catalog: Arc<dyn crate::catalog::NovaRocksCatalog>,
    ) -> Self {
        Self {
            control_state,
            resources,
            novarocks_catalog,
            drop_cleanup: Arc::new(crate::catalog_control::drop_cleanup::DropCleanupQueue::new()),
            table_coordination: Arc::new(
                crate::catalog::table_coordination::IcebergTableCoordination::default(),
            ),
            attempt_metadata_cache_owner: NEXT_ATTEMPT_METADATA_CACHE_OWNER
                .fetch_add(1, Ordering::Relaxed),
        }
    }

    pub(crate) fn with_table_coordination(
        mut self,
        coordination: Arc<crate::catalog::table_coordination::IcebergTableCoordination>,
    ) -> Self {
        self.table_coordination = coordination;
        self
    }

    pub(crate) fn table_coordination(
        &self,
    ) -> &Arc<crate::catalog::table_coordination::IcebergTableCoordination> {
        &self.table_coordination
    }

    /// Pure facts of the current original generation. Actual requests may
    /// discover further vended/configured routes and must extend the same Root
    /// before those routes' first credential/network dispatch.
    pub(crate) fn metadata_action_initial_dependencies(
        &self,
    ) -> Result<Vec<novarocks_spi::connector::SharedDependency>, ConnectorError> {
        use novarocks_spi::connector::{NetworkServiceOrigin, SharedDependency};
        let config = self.control_state.configuration();
        let mut dependencies = Vec::new();
        match config.kind {
            crate::catalog_config::IcebergCatalogKind::Rest => {
                let uri = config.rest_uri.as_ref().ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "REST generation has no original URI",
                    )
                })?;
                let selected = url::Url::parse(uri).map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "invalid original REST URI",
                    )
                })?;
                let fact = SharedDependency::Network(
                    NetworkServiceOrigin::from_selected_url(&selected).map_err(|_| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "REST origin exceeds dependency profile",
                        )
                    })?,
                );
                dependencies.push(fact);
            }
            crate::catalog_config::IcebergCatalogKind::Hadoop => {
                dependencies.push(
                    self.resources
                        .planning_binding()
                        .selected_io_dependency(&config.warehouse_uri)?,
                );
            }
            crate::catalog_config::IcebergCatalogKind::Hive => {
                // The existing catalog author permanently refuses these two
                // mutations. This does not remove any HMS read capability.
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "Hive Metastore catalog is a read-only compatibility entry",
                ));
            }
        }
        Ok(dependencies)
    }

    pub(crate) fn read_planning_initial_dependencies(
        &self,
    ) -> Result<Vec<novarocks_spi::connector::SharedDependency>, ConnectorError> {
        let config = self.control_state.configuration();
        if config.kind != crate::catalog_config::IcebergCatalogKind::Hive {
            return self.metadata_action_initial_dependencies();
        }
        // Catalog configuration already selected the first real HMS address.
        // The vendored Thrift builder consumes this identical host:port;
        // framed/buffered codecs share this same external service domain.
        let address = config.hms_uris.as_deref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "HMS read generation has no selected address",
            )
        })?;
        let selected =
            novarocks_spi::connector::SelectedThriftServiceOrigin::from_selected_address(address)
                .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "HMS selected service address exceeds the dependency contract",
                )
            })?;
        Ok(vec![novarocks_spi::connector::SharedDependency::Thrift(
            selected.into_owned(),
        )])
    }

    pub(crate) fn control_state(&self) -> &IcebergCatalogControlState {
        &self.control_state
    }

    /// The generation's single catalog owner.
    pub(crate) fn novarocks_catalog(&self) -> &Arc<dyn crate::catalog::NovaRocksCatalog> {
        &self.novarocks_catalog
    }

    /// Proven-committed drops awaiting their collection pass.
    pub(crate) fn drop_cleanup(
        &self,
    ) -> &Arc<crate::catalog_control::drop_cleanup::DropCleanupQueue> {
        &self.drop_cleanup
    }

    pub(crate) fn resources(&self) -> &IcebergMetadataResources {
        &self.resources
    }

    pub(crate) fn load_table(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<IcebergPhysicalTable, String> {
        self.load_table_classified(namespace, table)
            .map_err(|(_, message)| message)
    }

    /// Load a selected snapshot scope without reusing complete metadata for
    /// refs-only observations, or caching refs-only responses as full loads.
    pub(crate) fn load_table_with_snapshots(
        &self,
        namespace: &str,
        table: &str,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<IcebergPhysicalTable, String> {
        if snapshots == crate::catalog::SnapshotLoading::All {
            return self.load_table(namespace, table);
        }
        let namespace = normalize_identifier(namespace)?;
        let table = normalize_identifier(table)?;
        self.observe_table_classified_with_snapshots(&namespace, &table, None, None, snapshots)
            .map_err(|(_, message)| message)
    }

    /// Load one table for an admitted request. A vended response is immediately
    /// rebound to this request's storage resolver and is never cached.
    pub(crate) fn load_table_for_request(
        &self,
        namespace: &str,
        table: &str,
        request_context: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, String> {
        self.load_table_classified_with_credential_lease_collection(
            namespace,
            table,
            request_context.vended_credential_lease_collection(),
            Some(request_context),
        )
        .map_err(|(_, message)| message)
    }

    /// Async counterpart of the same request-local cache author. It shares the
    /// exact entry with synchronous consumers and preserves failed-load eviction.
    pub(crate) async fn load_table_for_request_async(
        &self,
        namespace: &str,
        table: &str,
        request: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, String> {
        self.load_table_classified_for_request_async(namespace, table, request)
            .await
            .map_err(|(_, message)| message)
    }

    /// Preserve absence and control classification at the typed read boundary.
    pub(crate) async fn load_table_classified_for_request_async(
        &self,
        namespace: &str,
        table: &str,
        request: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        self.load_table_original_for_request_async(namespace, table, request)
            .await
            .map_err(classified_original_error)
    }

    pub(crate) async fn load_table_original_for_request_async(
        &self,
        namespace: &str,
        table: &str,
        request: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        let load = async {
            ConnectorOperationControl::check_active(request)?;
            let namespace = normalize_identifier(namespace).map_err(|message| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
            })?;
            let table = normalize_identifier(table).map_err(|message| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
            })?;
            let collection = request.vended_credential_lease_collection();
            if request.fresh_catalog_observation_required()
                || (request.vended_credential_lease_sink().is_none()
                    && request.storage_resolver().is_some())
            {
                return self
                    .observe_table_original_for_request_with_snapshots_async(
                        &namespace,
                        &table,
                        request,
                        crate::catalog::SnapshotLoading::All,
                    )
                    .await;
            }
            let cache =
                request.request_scope_extension_or_insert_with(AttemptMetadataTableCache::default);
            let frozen = cache
                .get_or_load_original_async(
                    AttemptMetadataTableKey {
                        owner: self.attempt_metadata_cache_owner,
                        namespace: namespace.clone(),
                        table: table.clone(),
                    },
                    async {
                        let loaded = if collection.is_none() {
                            self.control_state
                                .physical_table_cache()
                                .get(&namespace, &table)
                                .map_err(|message| {
                                    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
                                })?
                        } else {
                            None
                        };
                        let loaded = match loaded {
                            Some(loaded) => loaded,
                            None => {
                                self.observe_table_original_with_snapshots_async(
                                    &namespace,
                                    &table,
                                    collection,
                                    Some(request),
                                    crate::catalog::SnapshotLoading::All,
                                )
                                .await?
                            }
                        };
                        Ok(IcebergAttemptTableAccess::freeze(loaded))
                    },
                )
                .await?;
            ConnectorOperationControl::check_active(request)?;
            frozen.reacquired_request_scoped(
                self.resources
                    .planning_binding()
                    .for_request(request.clone()),
                self.resources.catalog_runtime().iceberg_runtime().clone(),
            )
        };
        load.await
    }

    pub(crate) async fn load_table_classified_async(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        let namespace = normalize_identifier(namespace).map_err(invalid_request)?;
        let table = normalize_identifier(table).map_err(invalid_request)?;
        if let Some(cached) = self
            .control_state
            .physical_table_cache()
            .get(&namespace, &table)
            .map_err(unavailable)?
        {
            return Ok(cached);
        }
        self.observe_table_classified_with_snapshots_async(
            &namespace,
            &table,
            None,
            None,
            crate::catalog::SnapshotLoading::All,
        )
        .await
    }

    /// Load an explicit REST snapshot scope without allowing a partial
    /// observation into the complete-metadata caches.
    pub(crate) fn load_table_for_request_with_snapshots(
        &self,
        namespace: &str,
        table: &str,
        request_context: &ConnectorRequestContext,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<IcebergPhysicalTable, String> {
        match snapshots {
            crate::catalog::SnapshotLoading::All => {
                self.load_table_for_request(namespace, table, request_context)
            }
            crate::catalog::SnapshotLoading::Refs => self.observe_table_for_request_with_snapshots(
                namespace,
                table,
                request_context,
                snapshots,
            ),
        }
    }

    /// Probe the UUID, wait in the logical catalog's domain, then establish a
    /// fresh attempt base under the lock. Both observations retain the exact
    /// request's original control and generation-specific credential binding.
    pub(crate) fn coordinate_table_for_request(
        &self,
        namespace: &str,
        table: &str,
        request: &ConnectorRequestContext,
    ) -> Result<
        (
            crate::catalog::table_coordination::IcebergTableCoordinationGuard,
            IcebergPhysicalTable,
        ),
        novarocks_spi::connector::ConnectorError,
    > {
        self.coordinate_table_for_request_with_snapshots(
            namespace,
            table,
            request,
            crate::catalog::SnapshotLoading::All,
        )
    }

    pub(crate) fn coordinate_table_for_request_with_snapshots(
        &self,
        namespace: &str,
        table: &str,
        request: &ConnectorRequestContext,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<
        (
            crate::catalog::table_coordination::IcebergTableCoordinationGuard,
            IcebergPhysicalTable,
        ),
        novarocks_spi::connector::ConnectorError,
    > {
        use novarocks_spi::connector::ConnectorError;
        let observed = self
            .observe_table_classified_for_request_with_snapshots(
                namespace, table, request, snapshots,
            )
            .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
        let uuid = observed.table.metadata().uuid();
        let coordination = Arc::clone(self.table_coordination());
        let wait_request = request.clone();
        let activity = self.resources.catalog_runtime().activity().cloned();
        let guard = self
            .resources
            .catalog_runtime()
            .block_on(async move {
                let _waiting = activity.as_ref().map(|owner| {
                    owner.enter(novarocks_spi::connector::CatalogActivityPhase::TableCoordination)
                });
                coordination.acquire(uuid, &wait_request).await
            })
            .map_err(|message| ConnectorError::new(ConnectorErrorKind::Unavailable, message))??;
        let fresh = self
            .observe_table_classified_for_request_with_snapshots(
                namespace, table, request, snapshots,
            )
            .map_err(|(kind, message)| {
                if kind == ConnectorErrorKind::NotFound {
                    ConnectorError::table_object_binding(
                        novarocks_spi::connector::ConnectorTableObjectBindingFailure::Missing,
                        message,
                    )
                } else {
                    ConnectorError::new(kind, message)
                }
            })?;
        if fresh.table.metadata().uuid() != uuid {
            return Err(ConnectorError::table_object_binding(
                novarocks_spi::connector::ConnectorTableObjectBindingFailure::Replaced,
                "Iceberg table UUID changed while waiting for catalog coordination",
            ));
        }
        Ok((guard, fresh))
    }

    pub(crate) async fn coordinate_table_for_request_async(
        &self,
        namespace: &str,
        table: &str,
        request: &ConnectorRequestContext,
    ) -> Result<
        (
            crate::catalog::table_coordination::IcebergTableCoordinationGuard,
            IcebergPhysicalTable,
        ),
        novarocks_spi::connector::ConnectorError,
    > {
        use novarocks_spi::connector::ConnectorError;
        let observed = self
            .observe_table_classified_for_request_with_snapshots_async(
                namespace,
                table,
                request,
                crate::catalog::SnapshotLoading::All,
            )
            .await
            .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
        let uuid = observed.table.metadata().uuid();
        let guard = {
            let _waiting = self.resources.catalog_runtime().activity().map(|owner| {
                owner.enter(novarocks_spi::connector::CatalogActivityPhase::TableCoordination)
            });
            self.table_coordination.acquire(uuid, request).await?
        };
        let fresh = self
            .observe_table_classified_for_request_with_snapshots_async(
                namespace,
                table,
                request,
                crate::catalog::SnapshotLoading::All,
            )
            .await
            .map_err(|(kind, message)| {
                if kind == ConnectorErrorKind::NotFound {
                    ConnectorError::table_object_binding(
                        novarocks_spi::connector::ConnectorTableObjectBindingFailure::Missing,
                        message,
                    )
                } else {
                    ConnectorError::new(kind, message)
                }
            })?;
        if fresh.table.metadata().uuid() != uuid {
            return Err(ConnectorError::table_object_binding(
                novarocks_spi::connector::ConnectorTableObjectBindingFailure::Replaced,
                "Iceberg table UUID changed while waiting for catalog coordination",
            ));
        }
        Ok((guard, fresh))
    }

    /// Observe a mutation admission frontier without reusing planning caches.
    /// Keep the original deadline, stop view and credential collector for this attempt.
    pub(crate) fn observe_table_for_request(
        &self,
        namespace: &str,
        table: &str,
        request_context: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, String> {
        self.observe_table_for_request_with_snapshots(
            namespace,
            table,
            request_context,
            crate::catalog::SnapshotLoading::All,
        )
    }

    pub(crate) fn observe_table_for_request_with_snapshots(
        &self,
        namespace: &str,
        table: &str,
        request_context: &ConnectorRequestContext,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<IcebergPhysicalTable, String> {
        self.observe_table_classified_for_request_with_snapshots(
            namespace,
            table,
            request_context,
            snapshots,
        )
        .map_err(|(_, message)| message)
    }

    pub(crate) fn observe_table_classified_for_request(
        &self,
        namespace: &str,
        table: &str,
        request_context: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        self.observe_table_classified_for_request_with_snapshots(
            namespace,
            table,
            request_context,
            crate::catalog::SnapshotLoading::All,
        )
    }

    pub(crate) fn observe_table_classified_for_request_with_snapshots(
        &self,
        namespace: &str,
        table: &str,
        request_context: &ConnectorRequestContext,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        let control_error =
            |error: novarocks_spi::connector::ConnectorError| (error.kind(), error.to_string());
        ConnectorOperationControl::check_active(request_context).map_err(control_error)?;
        let namespace = normalize_identifier(namespace).map_err(invalid_request)?;
        let table = normalize_identifier(table).map_err(invalid_request)?;
        let loaded = self.observe_table_classified_with_snapshots(
            &namespace,
            &table,
            request_context.vended_credential_lease_collection(),
            Some(request_context),
            snapshots,
        )?;
        self.scope_observed_table(loaded, request_context)
    }

    pub(crate) async fn observe_table_classified_for_request_with_snapshots_async(
        &self,
        namespace: &str,
        table: &str,
        request_context: &ConnectorRequestContext,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        self.observe_table_original_for_request_with_snapshots_async(
            namespace,
            table,
            request_context,
            snapshots,
        )
        .await
        .map_err(classified_original_error)
    }

    pub(crate) async fn observe_table_original_for_request_with_snapshots_async(
        &self,
        namespace: &str,
        table: &str,
        request_context: &ConnectorRequestContext,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        ConnectorOperationControl::check_active(request_context)?;
        let namespace = normalize_identifier(namespace)
            .map_err(|message| ConnectorError::new(ConnectorErrorKind::InvalidRequest, message))?;
        let table = normalize_identifier(table)
            .map_err(|message| ConnectorError::new(ConnectorErrorKind::InvalidRequest, message))?;
        let loaded = self
            .observe_table_original_with_snapshots_async(
                &namespace,
                &table,
                request_context.vended_credential_lease_collection(),
                Some(request_context),
                snapshots,
            )
            .await?;
        self.scope_original_observed_table(loaded, request_context)
    }

    fn scope_observed_table(
        &self,
        loaded: IcebergPhysicalTable,
        request_context: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        self.scope_original_observed_table(loaded, request_context)
            .map_err(classified_original_error)
    }

    fn scope_original_observed_table(
        &self,
        loaded: IcebergPhysicalTable,
        request_context: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        ConnectorOperationControl::check_active(request_context)?;
        let renewal = loaded.attempt_access_if_vended();
        let binding = self
            .resources
            .planning_binding()
            .for_request(request_context.clone());
        let mut scoped = IcebergPhysicalTable::request_scoped(
            &loaded.table,
            binding,
            self.resources.catalog_runtime().iceberg_runtime().clone(),
        )?;
        if let Some(renewal) = renewal {
            scoped = scoped.with_attempt_access(renewal);
        }
        Ok(scoped)
    }

    /// Request-scoped variant retaining provider error classification for the
    /// typed read boundary, where `NotFound` remains distinct from a control
    /// failure. Vended materialization still stays request-local and bypasses
    /// the physical-table cache.
    pub(crate) fn load_table_classified_for_request(
        &self,
        namespace: &str,
        table: &str,
        request_context: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        self.load_table_classified_with_credential_lease_collection(
            namespace,
            table,
            request_context.vended_credential_lease_collection(),
            Some(request_context),
        )
    }

    /// Reacquire one attempt's vended storage capability for an immutable
    /// table binding.
    ///
    /// `load_table` acquires this attempt's vended lease. The returned table
    /// must name the same UUID, but its metadata location and snapshot are
    /// ignored; the result is rebuilt from `frozen` with this attempt's
    /// request-local resolver.
    pub(crate) fn reacquire_table_access_for_request(
        &self,
        namespace: &str,
        table: &str,
        access: &IcebergAttemptTableAccess,
        request_context: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        let namespace = normalize_identifier(namespace).map_err(invalid_request)?;
        let table = normalize_identifier(table).map_err(invalid_request)?;
        let ident = TableIdent::from_strs([namespace.as_str(), table.as_str()])
            .map_err(|error| invalid_request(format!("build Iceberg table identity: {error}")))?;
        if access.identifier() != &ident {
            return Err((
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "reacquire Iceberg table {namespace}.{table}: target does not match the frozen table identity"
                ),
            ));
        }
        if request_context.is_cancelled() {
            return Err((
                ConnectorErrorKind::Cancelled,
                "reacquire Iceberg table access was cancelled".to_string(),
            ));
        }
        if std::time::Instant::now() >= request_context.deadline() {
            return Err((
                ConnectorErrorKind::DeadlineExceeded,
                "reacquire Iceberg table access deadline elapsed".to_string(),
            ));
        }
        let Some(capability) = access.renewal() else {
            let request_binding = self
                .resources
                .planning_binding()
                .for_request(request_context.clone());
            return access
                .reacquired_request_scoped(
                    request_binding,
                    self.resources.catalog_runtime().iceberg_runtime().clone(),
                )
                .map_err(|error| (error.kind(), error.to_string()));
        };
        let frozen = access;
        let collection = request_context
            .vended_credential_lease_collection()
            .ok_or_else(|| {
                (
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "reacquire Iceberg table {namespace}.{table}: missing attempt credential collection"
                    ),
                )
            })?;
        let target =
            crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        let owner = Arc::clone(self.novarocks_catalog());
        let loaded = self
            .resources
            .catalog_runtime()
            .block_on(async move { owner.load_table(target).await })
            .map_err(unavailable)?;
        ConnectorOperationControl::check_active(request_context)
            .map_err(|error| (error.kind(), error.to_string()))?;
        let loaded = loaded.map_err(|error| {
            (
                error.kind(),
                format!("reacquire Iceberg table {namespace}.{table}: {error}"),
            )
        })?;
        let (materialization, access_delegation) = loaded.into_parts();
        let seed = access_delegation.into_vended_lease_seed().ok_or_else(|| {
            (
                ConnectorErrorKind::Unsupported,
                format!(
                    "reacquire Iceberg table {namespace}.{table}: catalog did not return vended access delegation"
                ),
            )
        })?;
        let metadata_binding = self.resources.planning_binding().clone();
        let observed = materialization
            .materialize_for_request(metadata_binding)
            .map_err(|error| (error.kind(), error.to_string()))?;
        let contribution = match capability {
            IcebergVendedS3RenewalCapability::CredentialsEndpoint(_) => {
                let attempt_scope = frozen
                    .validate_credentials_endpoint_attempt_acquisition(&observed, &seed)
                    .map_err(|error| (error.kind(), error.to_string()))?;
                let catalog = self
                    .novarocks_catalog
                    .vended_credential_refresh_catalog()
                    .ok_or_else(|| {
                        (
                            ConnectorErrorKind::Unsupported,
                            format!(
                                "reacquire Iceberg table {namespace}.{table}: vended access has no REST catalog owner"
                            ),
                        )
                    })?;
                // The same routed path a fresh observation would announce: an
                // execution node acquiring for itself must be told it (L15).
                let announcement = self.vended_renewal_path(
                    &seed.renewal_capability(),
                    &ident,
                    frozen.metadata().uuid(),
                )?;
                seed.into_vended_s3_credential_lease_contribution()
                    .and_then(|contribution| {
                        contribution.with_refresher(
                            catalog.acquisition_owner(),
                            Arc::new(IcebergRestVendedS3LeaseRefresher::new(
                                catalog,
                                self.resources.catalog_runtime().clone(),
                                attempt_scope,
                            )),
                        )
                    })
                    .map(|contribution| contribution.with_renewal_path(announcement))
                    .map_err(|error| (error.kind(), error.to_string()))?
            }
            IcebergVendedS3RenewalCapability::LoadTableDelegation(_) => {
                frozen
                    .validate_attempt_reacquisition(&observed, &seed)
                    .map_err(|error| (error.kind(), error.to_string()))?;
                self.vended_attempt_contribution(seed, ident, frozen.metadata().uuid())?
            }
        };
        collection
            .offer_vended_s3_credential_lease(contribution)
            .map_err(|error| {
                (
                    error.kind(),
                    format!(
                        "reacquire Iceberg table {namespace}.{table}: collect vended credentials: {error}"
                    ),
                )
            })?;
        ConnectorOperationControl::check_active(request_context)
            .map_err(|error| (error.kind(), error.to_string()))?;
        let request_binding = self
            .resources
            .planning_binding()
            .for_request(request_context.clone());
        frozen
            .reacquired_request_scoped(
                request_binding,
                self.resources.catalog_runtime().iceberg_runtime().clone(),
            )
            .map_err(|error| (error.kind(), error.to_string()))
    }

    pub(crate) async fn reacquire_table_access_for_request_async(
        &self,
        namespace: &str,
        table: &str,
        access: &IcebergAttemptTableAccess,
        request_context: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        let namespace = normalize_identifier(namespace).map_err(invalid_request)?;
        let table = normalize_identifier(table).map_err(invalid_request)?;
        let ident = TableIdent::from_strs([namespace.as_str(), table.as_str()])
            .map_err(|error| invalid_request(format!("build Iceberg table identity: {error}")))?;
        if access.identifier() != &ident {
            return Err((
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "reacquire Iceberg table {namespace}.{table}: target does not match the frozen table identity"
                ),
            ));
        }
        if request_context.is_cancelled() {
            return Err((
                ConnectorErrorKind::Cancelled,
                "reacquire Iceberg table access was cancelled".to_string(),
            ));
        }
        if std::time::Instant::now() >= request_context.deadline() {
            return Err((
                ConnectorErrorKind::DeadlineExceeded,
                "reacquire Iceberg table access deadline elapsed".to_string(),
            ));
        }
        let Some(capability) = access.renewal() else {
            let request_binding = self
                .resources
                .planning_binding()
                .for_request(request_context.clone());
            return access
                .reacquired_request_scoped(
                    request_binding,
                    self.resources.catalog_runtime().iceberg_runtime().clone(),
                )
                .map_err(|error| (error.kind(), error.to_string()));
        };
        let frozen = access;
        let collection = request_context
            .vended_credential_lease_collection()
            .ok_or_else(|| {
                (
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "reacquire Iceberg table {namespace}.{table}: missing attempt credential collection"
                    ),
                )
            })?;
        let target =
            crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        let owner = Arc::clone(self.novarocks_catalog());
        let loaded = owner.load_table(target).await;
        ConnectorOperationControl::check_active(request_context)
            .map_err(|error| (error.kind(), error.to_string()))?;
        let loaded = loaded.map_err(|error| {
            (
                error.kind(),
                format!("reacquire Iceberg table {namespace}.{table}: {error}"),
            )
        })?;
        let (materialization, access_delegation) = loaded.into_parts();
        let seed = access_delegation.into_vended_lease_seed().ok_or_else(|| {
            (
                ConnectorErrorKind::Unsupported,
                format!(
                    "reacquire Iceberg table {namespace}.{table}: catalog did not return vended access delegation"
                ),
            )
        })?;
        let metadata_binding = self.resources.planning_binding().clone();
        let observed = materialization
            .materialize_for_request(metadata_binding)
            .map_err(|error| (error.kind(), error.to_string()))?;
        let contribution = match capability {
            IcebergVendedS3RenewalCapability::CredentialsEndpoint(_) => {
                let attempt_scope = frozen
                    .validate_credentials_endpoint_attempt_acquisition(&observed, &seed)
                    .map_err(|error| (error.kind(), error.to_string()))?;
                let catalog = self
                    .novarocks_catalog
                    .vended_credential_refresh_catalog()
                    .ok_or_else(|| {
                        (
                            ConnectorErrorKind::Unsupported,
                            format!(
                                "reacquire Iceberg table {namespace}.{table}: vended access has no REST catalog owner"
                            ),
                        )
                    })?;
                // The same routed path a fresh observation would announce: an
                // execution node acquiring for itself must be told it (L15).
                let announcement = self.vended_renewal_path(
                    &seed.renewal_capability(),
                    &ident,
                    frozen.metadata().uuid(),
                )?;
                seed.into_vended_s3_credential_lease_contribution()
                    .and_then(|contribution| {
                        contribution.with_refresher(
                            catalog.acquisition_owner(),
                            Arc::new(IcebergRestVendedS3LeaseRefresher::new(
                                catalog,
                                self.resources.catalog_runtime().clone(),
                                attempt_scope,
                            )),
                        )
                    })
                    .map(|contribution| contribution.with_renewal_path(announcement))
                    .map_err(|error| (error.kind(), error.to_string()))?
            }
            IcebergVendedS3RenewalCapability::LoadTableDelegation(_) => {
                frozen
                    .validate_attempt_reacquisition(&observed, &seed)
                    .map_err(|error| (error.kind(), error.to_string()))?;
                self.vended_attempt_contribution(seed, ident, frozen.metadata().uuid())?
            }
        };
        collection
            .offer_vended_s3_credential_lease(contribution)
            .map_err(|error| {
                (
                    error.kind(),
                    format!(
                        "reacquire Iceberg table {namespace}.{table}: collect vended credentials: {error}"
                    ),
                )
            })?;
        ConnectorOperationControl::check_active(request_context)
            .map_err(|error| (error.kind(), error.to_string()))?;
        let request_binding = self
            .resources
            .planning_binding()
            .for_request(request_context.clone());
        frozen
            .reacquired_request_scoped(
                request_binding,
                self.resources.catalog_runtime().iceberg_runtime().clone(),
            )
            .map_err(|error| (error.kind(), error.to_string()))
    }

    /// Load a table while keeping the catalog's own error classification.
    ///
    /// The string-returning `load_table` erases it, but the metadata SPI has to
    /// keep absence distinguishable from a transport failure: callers drive
    /// `CREATE ... IF NOT EXISTS` and MV target creation off
    /// `ConnectorErrorKind::NotFound`, and an absent table reported as
    /// `Unavailable` turns those into hard errors.
    pub(crate) fn load_table_classified(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        self.load_table_classified_with_credential_lease_collection(namespace, table, None, None)
    }

    /// Load a table through either an observation or attempt context.
    ///
    /// Observation uses the generation's FE metadata principal and retains
    /// only the non-secret renewal capability. Attempt instantiation transfers
    /// a REST-vended response into its request-local collector. A context that
    /// advertises an attempt sink without a matching collection port fails
    /// closed, and no vended response enters the physical table cache.
    pub(crate) fn load_table_classified_with_credential_lease_collection(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        self.load_table_original_with_credential_lease_collection(
            namespace,
            table,
            credential_lease_collection,
            request_context,
        )
        .map_err(classified_original_error)
    }

    pub(crate) fn load_table_original_with_credential_lease_collection(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        if let Some(request_context) = request_context {
            novarocks_spi::connector::ConnectorOperationControl::check_active(request_context)?;
        }
        let loaded = self.load_table_original_unbound(
            namespace,
            table,
            credential_lease_collection,
            request_context,
        );
        if let Some(request_context) = request_context {
            novarocks_spi::connector::ConnectorOperationControl::check_active(request_context)?;
        }
        let loaded = loaded?;
        let Some(request_context) = request_context else {
            return Ok(loaded);
        };
        // A control-generation cache hit retains its generation FileIO. Rebuild
        // only this operation's table view before any manifest read can use it.
        let renewal = loaded.attempt_access_if_vended();
        let binding = self
            .resources
            .planning_binding()
            .for_request(request_context.clone());
        let mut scoped = IcebergPhysicalTable::request_scoped(
            &loaded.table,
            binding,
            self.resources.catalog_runtime().iceberg_runtime().clone(),
        )?;
        if let Some(renewal) = renewal {
            scoped = scoped.with_attempt_access(renewal);
        }
        Ok(scoped)
    }

    fn load_table_original_unbound(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        let namespace = normalize_identifier(namespace)
            .map_err(|message| ConnectorError::new(ConnectorErrorKind::InvalidRequest, message))?;
        let table = normalize_identifier(table)
            .map_err(|message| ConnectorError::new(ConnectorErrorKind::InvalidRequest, message))?;
        let Some(request_context) = request_context else {
            return self.load_table_original_uncached(
                &namespace,
                &table,
                credential_lease_collection,
                None,
            );
        };
        // The request cache freezes every read-only planning observation in
        // one request scope, including the metadata/statistics/typed-scan
        // paths before an attempt collector exists. A terminal write context
        // deliberately drops that collector and retains a replacement storage
        // resolver; it must reload through that resolver rather than reuse the
        // completed attempt's observation.
        //
        // Terminal reloads must also reach the catalog rather than the
        // control-state table cache: a commit decides against that current
        // observation, so a cached table could compute replacements from a
        // snapshot the branch has already moved past.
        if request_context.fresh_catalog_observation_required()
            || (request_context.vended_credential_lease_sink().is_none()
                && request_context.storage_resolver().is_some())
        {
            return self.observe_table_original(
                &namespace,
                &table,
                credential_lease_collection,
                Some(request_context),
            );
        }
        let cache = request_context
            .request_scope_extension_or_insert_with(AttemptMetadataTableCache::default);
        let frozen = cache.get_or_load_original(
            AttemptMetadataTableKey {
                owner: self.attempt_metadata_cache_owner,
                namespace: namespace.clone(),
                table: table.clone(),
            },
            || {
                self.load_table_original_uncached(
                    &namespace,
                    &table,
                    credential_lease_collection,
                    Some(request_context),
                )
                .map(IcebergAttemptTableAccess::freeze)
            },
        )?;
        frozen.reacquired_request_scoped(
            self.resources
                .planning_binding()
                .for_request(request_context.clone()),
            self.resources.catalog_runtime().iceberg_runtime().clone(),
        )
    }

    /// Perform the one physical catalog observation for a cache miss. The
    /// caller owns normalization and, when applicable, the attempt-local
    /// single-flight entry that memoizes a successful materialization.
    /// Load past the attempt-local cache, but still accept a control-state
    /// cached table when no vended credential collection forces a fresh
    /// observation.
    ///
    /// This is a read-side reuse. A caller whose result decides an external
    /// effect must use [`Self::observe_table_classified`] instead: the name of
    /// this one says only that it skips the *attempt* cache.
    fn load_table_original_uncached(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        if credential_lease_collection.is_none()
            && let Some(table) = self
                .control_state
                .physical_table_cache()
                .get(namespace, table)
                .map_err(|message| ConnectorError::new(ConnectorErrorKind::Unavailable, message))?
        {
            return Ok(table);
        }
        self.observe_table_original(
            namespace,
            table,
            credential_lease_collection,
            request_context,
        )
    }

    /// Observe the catalog now, reusing no cache at any level.
    fn observe_table_classified(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        self.observe_table_original(
            namespace,
            table,
            credential_lease_collection,
            request_context,
        )
        .map_err(classified_original_error)
    }

    fn observe_table_original(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        self.observe_table_original_with_snapshots(
            namespace,
            table,
            credential_lease_collection,
            request_context,
            crate::catalog::SnapshotLoading::All,
        )
    }

    fn observe_table_classified_with_snapshots(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        self.observe_table_original_with_snapshots(
            namespace,
            table,
            credential_lease_collection,
            request_context,
            snapshots,
        )
        .map_err(classified_original_error)
    }

    fn observe_table_original_with_snapshots(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        let ident = TableIdent::from_strs([namespace, table]).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("build Iceberg table identity: {error}"),
            )
        })?;
        let owner = Arc::clone(self.novarocks_catalog());
        let target =
            crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        let read_binding = request_context.map(|request| {
            self.resources
                .planning_binding()
                .for_request(request.clone())
        });
        let loaded = self
            .resources
            .catalog_runtime()
            .block_on(Self::read_observation(
                owner,
                target,
                read_binding,
                snapshots,
            ))
            .map_err(|message| ConnectorError::new(ConnectorErrorKind::Unavailable, message))??;
        self.materialize_original_observation(
            ident,
            loaded,
            (namespace, table),
            credential_lease_collection,
            request_context,
            snapshots,
        )
    }

    async fn observe_table_classified_with_snapshots_async(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        self.observe_table_original_with_snapshots_async(
            namespace,
            table,
            credential_lease_collection,
            request_context,
            snapshots,
        )
        .await
        .map_err(classified_original_error)
    }

    async fn observe_table_original_with_snapshots_async(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        let ident = TableIdent::from_strs([namespace, table]).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("build Iceberg table identity: {error}"),
            )
        })?;
        let owner = Arc::clone(self.novarocks_catalog());
        let target =
            crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        let read_binding = request_context.map(|request| {
            self.resources
                .planning_binding()
                .for_request(request.clone())
        });
        let loaded = Self::read_observation(owner, target, read_binding, snapshots).await?;
        self.materialize_original_observation(
            ident,
            loaded,
            (namespace, table),
            credential_lease_collection,
            request_context,
            snapshots,
        )
    }

    async fn read_observation(
        catalog: Arc<dyn crate::catalog::NovaRocksCatalog>,
        target: crate::catalog::CatalogTableName,
        binding: Option<crate::access_binding::IcebergReadBinding>,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, novarocks_spi::connector::ConnectorError>
    {
        match binding {
            Some(binding) => {
                catalog
                    .load_table_for_read_with_snapshots(target, binding, snapshots)
                    .await
            }
            None => catalog.load_table_with_snapshots(target, snapshots).await,
        }
    }

    fn materialize_original_observation(
        &self,
        ident: TableIdent,
        loaded: crate::loaded_table::IcebergLoadedTable,
        target: (&str, &str),
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        let (namespace, table) = target;
        let (materialization, access_delegation) = loaded.into_parts();
        if let Some(seed) = access_delegation.into_vended_lease_seed() {
            let request_binding = request_context.map_or_else(
                || self.resources.planning_binding().clone(),
                |context| {
                    self.resources
                        .planning_binding()
                        .for_request(context.clone())
                },
            );
            let observed_table =
                materialization.materialize_for_request(request_binding.clone())?;
            seed.validate_table_access(&observed_table)?;
            let attempt_access = seed.renewal_capability();
            if let Some(collection) = credential_lease_collection {
                let contribution = self.vended_original_attempt_contribution(
                    seed,
                    ident,
                    observed_table.metadata().uuid(),
                )?;
                collection.offer_vended_s3_credential_lease(contribution)?;
            } else if request_context
                .is_some_and(|context| context.vended_credential_lease_sink().is_some())
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    format!(
                        "load Iceberg table {namespace}.{table}: vended REST credentials require a query-attempt lease consumer"
                    ),
                ));
            }
            return Ok(
                IcebergPhysicalTable::new(observed_table).with_attempt_access(attempt_access)
            );
        }
        let loaded_table = materialization.into_static_table()?;
        let physical = IcebergPhysicalTable::new(loaded_table);
        // The Hadoop read may have built this table with a request-bound
        // FileIO. Only its resource-free facts may enter the request cache;
        // the complete table may enter the generation cache only without a
        // request binding.
        if request_context.is_none() && snapshots == crate::catalog::SnapshotLoading::All {
            self.control_state
                .physical_table_cache()
                .insert(namespace, table, physical.clone())
                .map_err(|message| ConnectorError::new(ConnectorErrorKind::Unavailable, message))?;
        }
        Ok(physical)
    }

    /// The routed acquisition path for one vended observation (L15), or the
    /// refusal that keeps a query an execution node could not serve from
    /// being dispatched at all (L25).
    pub(crate) fn vended_renewal_path(
        &self,
        renewal: &IcebergVendedS3RenewalCapability,
        table: &TableIdent,
        expected_table_uuid: uuid::Uuid,
    ) -> Result<CredentialRenewalPath, (ConnectorErrorKind, String)> {
        self.vended_original_renewal_path(renewal, table, expected_table_uuid)
            .map_err(classified_original_error)
    }

    pub(crate) fn vended_original_renewal_path(
        &self,
        renewal: &IcebergVendedS3RenewalCapability,
        table: &TableIdent,
        expected_table_uuid: uuid::Uuid,
    ) -> Result<CredentialRenewalPath, ConnectorError> {
        use crate::rest_protocol::routing::{RouteTarget, route_for_lease};
        let endpoints = self
            .novarocks_catalog
            .vended_credential_refresh_catalog()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "vended REST credential acquisition has no catalog owner",
                )
            })?;
        let binding = endpoints.established_binding().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "vended REST credentials were observed without an established session",
            )
        })?;
        let admission = endpoints.route_admission();
        match renewal {
            IcebergVendedS3RenewalCapability::CredentialsEndpoint(scope) => {
                let routing = route_for_lease(
                    &binding,
                    &admission,
                    RouteTarget::CredentialsEndpoint(scope.endpoint()),
                )?;
                Ok(CredentialRenewalPath::CredentialsEndpoint(routing))
            }
            IcebergVendedS3RenewalCapability::LoadTableDelegation(_) => {
                let routing = route_for_lease(&binding, &admission, RouteTarget::LoadTable(table))?;
                let delegation = CredentialLoadTableDelegation::try_new(
                    table
                        .namespace()
                        .as_ref()
                        .iter()
                        .map(|level| Arc::from(level.as_str()))
                        .collect(),
                    Arc::from(table.name()),
                    Arc::from(expected_table_uuid.to_string().as_str()),
                )?;
                Ok(CredentialRenewalPath::LoadTableDelegation {
                    routing,
                    table: delegation,
                })
            }
        }
    }

    fn vended_attempt_contribution(
        &self,
        seed: IcebergVendedCredentialLeaseSeed,
        table: TableIdent,
        expected_table_uuid: uuid::Uuid,
    ) -> Result<
        novarocks_spi::connector::VendedS3CredentialLeaseContribution,
        (ConnectorErrorKind, String),
    > {
        self.vended_original_attempt_contribution(seed, table, expected_table_uuid)
            .map_err(classified_original_error)
    }

    fn vended_original_attempt_contribution(
        &self,
        seed: IcebergVendedCredentialLeaseSeed,
        table: TableIdent,
        expected_table_uuid: uuid::Uuid,
    ) -> Result<novarocks_spi::connector::VendedS3CredentialLeaseContribution, ConnectorError> {
        let renewal = seed.renewal_capability();
        let contribution = seed.into_vended_s3_credential_lease_contribution()?;
        let catalog = self
            .novarocks_catalog
            .vended_credential_refresh_catalog()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "vended REST credential acquisition has no catalog owner",
                )
            })?;
        // The announcement and the refresher are two projections of one
        // observation. They are built together so a consumer that must acquire
        // for itself is told exactly the path this coordinator would have used
        // (CAD-1 D2), on this generation's one session (D15).
        let announcement =
            self.vended_original_renewal_path(&renewal, &table, expected_table_uuid)?;
        let acquisition_owner = catalog.acquisition_owner();
        let refresher: Arc<dyn ConnectorVendedS3CredentialLeaseRefresher> = match renewal {
            IcebergVendedS3RenewalCapability::CredentialsEndpoint(scope) => {
                Arc::new(IcebergRestVendedS3LeaseRefresher::new(
                    catalog,
                    self.resources.catalog_runtime().clone(),
                    scope,
                ))
            }
            IcebergVendedS3RenewalCapability::LoadTableDelegation(scope) => {
                Arc::new(IcebergRestLoadTableVendedS3LeaseRefresher::new(
                    catalog,
                    self.resources.catalog_runtime().clone(),
                    table,
                    expected_table_uuid,
                    scope,
                ))
            }
        };
        contribution
            .with_refresher(acquisition_owner, refresher)
            .map(|contribution| contribution.with_renewal_path(announcement))
    }

    /// Enumerate namespaces for one request, refused as a whole when the
    /// listing would exceed `bound`. The catalog's classification survives,
    /// so a bound refusal stays `ResourceExhausted`.
    pub(crate) fn list_namespaces_for_request(
        &self,
        request: &ConnectorRequestContext,
        bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<String>, (ConnectorErrorKind, String)> {
        let owner = Arc::clone(self.novarocks_catalog());
        let binding = self
            .resources
            .planning_binding()
            .for_request(request.clone());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.list_namespaces_for_read(binding, bound).await })
            .map_err(|error| (ConnectorErrorKind::Unavailable, error))?
            .map_err(|error| {
                (
                    error.kind(),
                    format!("list Iceberg namespaces: {}", error.message()),
                )
            })
    }

    pub(crate) async fn list_namespaces_for_request_async(
        &self,
        request: &ConnectorRequestContext,
        bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<String>, (ConnectorErrorKind, String)> {
        let owner = Arc::clone(self.novarocks_catalog());
        let binding = self
            .resources
            .planning_binding()
            .for_request(request.clone());
        owner
            .list_namespaces_for_read(binding, bound)
            .await
            .map_err(|error| {
                (
                    error.kind(),
                    format!("list Iceberg namespaces: {}", error.message()),
                )
            })
    }

    pub(crate) fn namespace_exists(&self, namespace: &str) -> Result<bool, String> {
        let namespace = NamespaceIdent::new(normalize_identifier(namespace)?);
        let namespace_label = namespace.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target = crate::catalog::CatalogNamespaceName::new(namespace.to_url_string());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.namespace_exists(target).await })?
            .map_err(|error| format!("check Iceberg namespace {namespace_label}: {error}"))
    }

    pub(crate) fn namespace_exists_for_request(
        &self,
        namespace: &str,
        request: &ConnectorRequestContext,
    ) -> Result<bool, String> {
        let namespace = NamespaceIdent::new(normalize_identifier(namespace)?);
        let namespace_label = namespace.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target = crate::catalog::CatalogNamespaceName::new(namespace.to_url_string());
        let binding = self
            .resources
            .planning_binding()
            .for_request(request.clone());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.namespace_exists_for_read(target, binding).await })?
            .map_err(|error| format!("check Iceberg namespace {namespace_label}: {error}"))
    }

    pub(crate) async fn namespace_exists_for_request_async(
        &self,
        namespace: &str,
        request: &ConnectorRequestContext,
    ) -> Result<bool, String> {
        let namespace = NamespaceIdent::new(normalize_identifier(namespace)?);
        let namespace_label = namespace.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target = crate::catalog::CatalogNamespaceName::new(namespace.to_url_string());
        let binding = self
            .resources
            .planning_binding()
            .for_request(request.clone());
        owner
            .namespace_exists_for_read(target, binding)
            .await
            .map_err(|error| format!("check Iceberg namespace {namespace_label}: {error}"))
    }

    /// Enumerate one namespace's tables for one request, refused as a whole
    /// when the listing would exceed `bound`. The catalog's classification
    /// survives, so a bound refusal stays `ResourceExhausted`.
    pub(crate) fn list_tables_for_request(
        &self,
        namespace: &str,
        request: &ConnectorRequestContext,
        bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<String>, (ConnectorErrorKind, String)> {
        let namespace =
            NamespaceIdent::new(normalize_identifier(namespace).map_err(invalid_request)?);
        let namespace_label = namespace.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target = crate::catalog::CatalogNamespaceName::new(namespace.to_url_string());
        let binding = self
            .resources
            .planning_binding()
            .for_request(request.clone());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.list_tables_for_read(target, binding, bound).await })
            .map_err(|error| (ConnectorErrorKind::Unavailable, error))?
            .map_err(|error| {
                (
                    error.kind(),
                    format!(
                        "list Iceberg tables in {namespace_label}: {}",
                        error.message()
                    ),
                )
            })
    }

    pub(crate) async fn list_tables_for_request_async(
        &self,
        namespace: &str,
        request: &ConnectorRequestContext,
        bound: novarocks_spi::connector::ConnectorListingBound,
    ) -> Result<Vec<String>, (ConnectorErrorKind, String)> {
        let namespace =
            NamespaceIdent::new(normalize_identifier(namespace).map_err(invalid_request)?);
        let namespace_label = namespace.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target = crate::catalog::CatalogNamespaceName::new(namespace.to_url_string());
        let binding = self
            .resources
            .planning_binding()
            .for_request(request.clone());
        owner
            .list_tables_for_read(target, binding, bound)
            .await
            .map_err(|error| {
                (
                    error.kind(),
                    format!(
                        "list Iceberg tables in {namespace_label}: {}",
                        error.message()
                    ),
                )
            })
    }

    pub(crate) fn table_exists(&self, namespace: &str, table: &str) -> Result<bool, String> {
        let ident = TableIdent::new(
            NamespaceIdent::new(normalize_identifier(namespace)?),
            normalize_identifier(table)?,
        );
        let ident_label = ident.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target =
            crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.table_exists(target).await })?
            .map_err(|error| format!("check Iceberg table {ident_label}: {error}"))
    }

    pub(crate) fn table_exists_for_request(
        &self,
        namespace: &str,
        table: &str,
        request: &ConnectorRequestContext,
    ) -> Result<bool, String> {
        let ident = TableIdent::new(
            NamespaceIdent::new(normalize_identifier(namespace)?),
            normalize_identifier(table)?,
        );
        let ident_label = ident.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target =
            crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        let binding = self
            .resources
            .planning_binding()
            .for_request(request.clone());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.table_exists_for_read(target, binding).await })?
            .map_err(|error| format!("check Iceberg table {ident_label}: {error}"))
    }

    pub(crate) async fn table_exists_for_request_async(
        &self,
        namespace: &str,
        table: &str,
        request: &ConnectorRequestContext,
    ) -> Result<bool, String> {
        let ident = TableIdent::new(
            NamespaceIdent::new(normalize_identifier(namespace)?),
            normalize_identifier(table)?,
        );
        let ident_label = ident.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target =
            crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        let binding = self
            .resources
            .planning_binding()
            .for_request(request.clone());
        owner
            .table_exists_for_read(target, binding)
            .await
            .map_err(|error| format!("check Iceberg table {ident_label}: {error}"))
    }
}

fn invalid_request(message: String) -> (ConnectorErrorKind, String) {
    (ConnectorErrorKind::InvalidRequest, message)
}

fn unavailable(message: String) -> (ConnectorErrorKind, String) {
    (ConnectorErrorKind::Unavailable, message)
}

impl std::fmt::Debug for IcebergMetadataContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergMetadataContext")
            .field("control_state", &"<provider catalog state>")
            .field("resources", &self.resources)
            .field("catalog", &"<provider catalog client>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::ConnectorRequestScope;
    use serde::{Deserialize, Serialize};

    use crate::iceberg::io::{FileIOBuilder, Storage, StorageConfig, StorageFactory};
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    };

    use super::*;

    struct ScopeDropProbe(Arc<AtomicUsize>);

    impl Drop for ScopeDropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Serialize, Deserialize)]
    struct ScopeRetainingFileIoFactory {
        #[serde(skip, default)]
        scope: ConnectorRequestScope,
        #[serde(skip, default)]
        drops: Arc<AtomicUsize>,
    }

    impl std::fmt::Debug for ScopeRetainingFileIoFactory {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ScopeRetainingFileIoFactory")
        }
    }

    impl Drop for ScopeRetainingFileIoFactory {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[typetag::serde]
    impl StorageFactory for ScopeRetainingFileIoFactory {
        fn build(&self, _config: &StorageConfig) -> crate::iceberg::Result<Arc<dyn Storage>> {
            // A metadata-only freeze must never need to instantiate storage.
            let _ = &self.scope;
            Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::Unexpected,
                "metadata cache test must not perform storage I/O",
            ))
        }
    }

    fn scope_retaining_table(
        scope: ConnectorRequestScope,
        drops: Arc<AtomicUsize>,
        runtime: crate::iceberg::Runtime,
    ) -> IcebergPhysicalTable {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let spec = PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(0)
            .build()
            .expect("partition spec");
        let metadata = TableMetadataBuilder::new(
            schema,
            spec,
            SortOrder::builder().build_unbound().expect("sort"),
            "file:///tmp/attempt-cache".to_string(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        IcebergPhysicalTable::new(
            crate::iceberg::table::Table::builder()
                .runtime(runtime)
                .identifier(TableIdent::from_strs(["analytics", "frozen"]).expect("identity"))
                .metadata(Arc::new(metadata))
                .metadata_location("file:///tmp/attempt-cache/v1.metadata.json".to_string())
                .readonly(true)
                .file_io(
                    FileIOBuilder::new(Arc::new(ScopeRetainingFileIoFactory { scope, drops }))
                        .build(),
                )
                .build()
                .expect("table"),
        )
    }

    #[test]
    fn successful_request_cache_releases_file_io_and_its_own_scope() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let sdk_runtime = crate::resources::IcebergCatalogRuntime::new(runtime.handle().clone());
        let scope = ConnectorRequestScope::new();
        let scope_drops = Arc::new(AtomicUsize::new(0));
        let file_io_drops = Arc::new(AtomicUsize::new(0));
        scope.extension_or_insert_with(|| ScopeDropProbe(Arc::clone(&scope_drops)));
        let cache = scope.extension_or_insert_with(AttemptMetadataTableCache::default);
        let key = AttemptMetadataTableKey {
            owner: 1,
            namespace: "analytics".to_string(),
            table: "frozen".to_string(),
        };
        let table = scope_retaining_table(
            scope.clone(),
            Arc::clone(&file_io_drops),
            sdk_runtime.iceberg_runtime().clone(),
        );
        let metadata = table.table.metadata_ref();
        let frozen = cache
            .get_or_load(key.clone(), || Ok(IcebergAttemptTableAccess::freeze(table)))
            .expect("freeze");
        assert_eq!(file_io_drops.load(Ordering::SeqCst), 1);
        drop(scope);
        assert_eq!(scope_drops.load(Ordering::SeqCst), 1);
        // Even an independently retained cache must own no request runtime.
        let cached = cache
            .get_or_load(key, || panic!("successful metadata is loaded only once"))
            .expect("cached metadata");
        assert!(std::ptr::eq(cached.metadata(), metadata.as_ref()));
        assert!(std::ptr::eq(frozen.metadata(), cached.metadata()));

        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let rebuilt = cached
            .reacquired_request_scoped(binding, sdk_runtime.iceberg_runtime().clone())
            .expect("rebuild");
        assert!(Arc::ptr_eq(&metadata, &rebuilt.table.metadata_ref()));
        assert_eq!(rebuilt.table.identifier(), frozen.identifier());
        assert_eq!(
            rebuilt.table.metadata_location(),
            Some("file:///tmp/attempt-cache/v1.metadata.json")
        );
        assert!(rebuilt.table.readonly());
    }

    #[test]
    fn successful_request_observation_is_single_flight_across_concurrent_readers() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let sdk_runtime = crate::resources::IcebergCatalogRuntime::new(runtime.handle().clone());
        let cache = AttemptMetadataTableCache::default();
        let key = AttemptMetadataTableKey {
            owner: 1,
            namespace: "analytics".to_string(),
            table: "frozen".to_string(),
        };
        let frozen = IcebergAttemptTableAccess::freeze(scope_retaining_table(
            ConnectorRequestScope::new(),
            Arc::new(AtomicUsize::new(0)),
            sdk_runtime.iceberg_runtime().clone(),
        ));
        let calls = AtomicUsize::new(0);
        let readers = std::sync::Barrier::new(8);
        std::thread::scope(|threads| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    threads.spawn(|| {
                        readers.wait();
                        cache
                            .get_or_load(key.clone(), || {
                                calls.fetch_add(1, Ordering::SeqCst);
                                Ok(frozen.clone())
                            })
                            .expect("observation")
                    })
                })
                .collect();
            for handle in handles {
                let observed = handle.join().expect("reader");
                assert!(std::ptr::eq(observed.metadata(), frozen.metadata()));
            }
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn failed_request_observation_is_not_retained_after_a_create_boundary() {
        let cache = AttemptMetadataTableCache::default();
        let key = AttemptMetadataTableKey {
            owner: 1,
            namespace: "analytics".to_string(),
            table: "created_later".to_string(),
        };
        let calls = AtomicUsize::new(0);

        let first = cache.get_or_load(key.clone(), || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err((ConnectorErrorKind::NotFound, "target is absent".to_string()))
        });
        assert!(matches!(first, Err((ConnectorErrorKind::NotFound, _))));

        let second = cache.get_or_load(key, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err((ConnectorErrorKind::NotFound, "target is absent".to_string()))
        });
        assert!(matches!(second, Err((ConnectorErrorKind::NotFound, _))));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn generation_runtime_keeps_one_explicit_catalog_client() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let control = IcebergMetadataResources::new(binding, runtime.handle().clone());
        let generation = IcebergMetadataContext::try_new(
            IcebergCatalogControlState::new(configuration),
            control,
        )
        .expect("generation runtime");

        assert_eq!(generation.control_state().properties().len(), 2);

        // Every operation reaches this generation's one semantic owner. The
        // following real UUID/cache test independently checks its reads.
        let first = Arc::clone(generation.novarocks_catalog());
        let second = Arc::clone(generation.novarocks_catalog());
        assert!(
            Arc::ptr_eq(&first, &second),
            "a generation must hold exactly one catalog owner"
        );
    }
    // The oracle is a UUID returned by the real Hadoop create, independent of
    // both cache implementations. The local warehouse/runtime belong to this
    // test; every guard is dropped before the runtime and warehouse exit.
    #[test]
    fn coordinated_observation_bypasses_request_and_generation_caches() {
        use novarocks_spi::connector::ConnectorStopOwner;
        use std::time::{Duration, Instant};

        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let context = IcebergMetadataContext::try_new(
            IcebergCatalogControlState::new(configuration),
            IcebergMetadataResources::new(binding, runtime.handle().clone()),
        )
        .expect("generation");
        let created = runtime.block_on(
            crate::catalog::admission_test_support::create_static_fixture_table(
                &context,
                novarocks_spi::connector::ProviderBindingEpoch::from_bytes([9; 16]),
                crate::catalog::CatalogNamespaceName::new("analytics"),
                crate::iceberg::TableCreation::builder()
                    .name("frozen".to_string())
                    .schema(
                        Schema::builder()
                            .with_fields(vec![Arc::new(NestedField::required(
                                1,
                                "id",
                                Type::Primitive(PrimitiveType::Long),
                            ))])
                            .build()
                            .unwrap(),
                    )
                    .build(),
            ),
        );
        let actual_uuid = created.metadata().uuid();
        let stale = scope_retaining_table(
            ConnectorRequestScope::new(),
            Arc::new(AtomicUsize::new(0)),
            context
                .resources
                .catalog_runtime()
                .iceberg_runtime()
                .clone(),
        );
        assert_ne!(actual_uuid, stale.table.metadata().uuid());
        context
            .control_state
            .physical_table_cache()
            .insert("analytics", "frozen", stale.clone())
            .expect("generation cache");
        let stop = ConnectorStopOwner::new();
        let request = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            stop.view(),
            1024,
            1024,
        )
        .unwrap();
        let cache =
            request.request_scope_extension_or_insert_with(AttemptMetadataTableCache::default);
        cache
            .get_or_load(
                AttemptMetadataTableKey {
                    owner: context.attempt_metadata_cache_owner,
                    namespace: "analytics".to_string(),
                    table: "frozen".to_string(),
                },
                || Ok(IcebergAttemptTableAccess::freeze(stale.clone())),
            )
            .unwrap();
        let cached = context
            .load_table_classified_for_request("analytics", "frozen", &request)
            .unwrap();
        assert_eq!(
            cached.table.metadata().uuid(),
            stale.table.metadata().uuid()
        );
        let (guard, fresh) = context
            .coordinate_table_for_request("analytics", "frozen", &request)
            .expect("fresh coordinated observation");
        assert_eq!(fresh.table.metadata().uuid(), actual_uuid);
        // A same-table caller cannot enter before the observation's holder
        // releases, while a different UUID continues immediately.
        runtime.block_on(async {
            let pending = context.table_coordination().acquire(actual_uuid, &request);
            tokio::pin!(pending);
            assert!(futures::poll!(&mut pending).is_pending());
            let other = context
                .table_coordination()
                .acquire(stale.table.metadata().uuid(), &request)
                .await
                .expect("different UUID progresses");
            drop(other);
        });
        drop(guard);
    }
    #[tokio::test]
    async fn cancelled_async_cache_loader_wakes_existing_waiter_and_does_not_cache_absence() {
        let cache = AttemptMetadataTableCache::default();
        let key = AttemptMetadataTableKey {
            owner: 1,
            namespace: "analytics".into(),
            table: "created_later".into(),
        };
        let mut loader = Box::pin(cache.get_or_load_async(key.clone(), std::future::pending()));
        assert!(futures::poll!(&mut loader).is_pending());
        let mut waiter = Box::pin(cache.get_or_load_async(key.clone(), async {
            panic!("the original active entry already owns the physical observation")
        }));
        assert!(futures::poll!(&mut waiter).is_pending());
        drop(loader);
        assert!(matches!(
            waiter.await,
            Err((ConnectorErrorKind::Cancelled, _))
        ));
        let calls = AtomicUsize::new(0);
        let first = cache
            .get_or_load_async(key.clone(), async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err((ConnectorErrorKind::NotFound, "target is absent".into()))
            })
            .await;
        assert!(matches!(first, Err((ConnectorErrorKind::NotFound, _))));
        let second = cache.get_or_load(key, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err((ConnectorErrorKind::NotFound, "target is absent".into()))
        });
        assert!(matches!(second, Err((ConnectorErrorKind::NotFound, _))));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn original_cache_waiter_receives_same_typed_cause_without_negative_caching() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use std::error::Error;
        let cache = AttemptMetadataTableCache::default();
        let key = AttemptMetadataTableKey {
            owner: 17,
            namespace: "original".into(),
            table: "typed_failure".into(),
        };
        let error = ConnectorError::from_domain_static(
            ConnectorErrorKind::Cancelled,
            novarocks_type_contract::CompileControlError::Cancelled,
            "original metadata control failure",
        );
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap()
            .clone();
        let fact = error.failure_fact();
        let (release, gate) = tokio::sync::oneshot::channel();
        let original = error.clone();
        let mut loader = Box::pin(cache.get_or_load_original_async(key.clone(), async move {
            gate.await.unwrap();
            Err(original)
        }));
        assert!(futures::poll!(&mut loader).is_pending());
        let mut waiter = Box::pin(cache.get_or_load_original_async(key.clone(), async {
            panic!("the same original in-flight entry must not load twice")
        }));
        assert!(futures::poll!(&mut waiter).is_pending());
        release.send(()).unwrap();
        let first = loader.await.err().expect("original loader failure");
        let second = waiter.await.err().expect("same original waiter failure");
        for received in [&first, &second] {
            assert_eq!(received.failure_fact(), fact);
            assert_eq!(
                received
                    .source()
                    .unwrap()
                    .downcast_ref::<SharedFailureCause>(),
                Some(&source)
            );
            assert!(matches!(
                received
                    .source()
                    .unwrap()
                    .downcast_ref::<SharedFailureCause>()
                    .unwrap()
                    .downcast_ref::<novarocks_type_contract::CompileControlError>(),
                Some(novarocks_type_contract::CompileControlError::Cancelled)
            ));
        }
        assert!(
            cache.entries.lock().unwrap().is_empty(),
            "failure is not a stale negative cache"
        );
        let calls = AtomicUsize::new(0);
        let retry = cache
            .get_or_load_original(key, || {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(error.clone())
            })
            .err()
            .expect("same typed producer failure");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            retry.source().unwrap().downcast_ref::<SharedFailureCause>(),
            Some(&source)
        );
        assert_eq!(retry.failure_fact(), fact);
    }
}
