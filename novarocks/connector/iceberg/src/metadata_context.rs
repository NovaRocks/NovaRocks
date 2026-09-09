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
    ConnectorErrorKind, ConnectorRequestContext, ConnectorVendedCredentialLeaseCollectionPort,
    ConnectorVendedS3CredentialLeaseRefresher,
};
use novarocks_types::naming::normalize_identifier;

use crate::catalog_control::IcebergCatalogControlState;
use crate::iceberg::{NamespaceIdent, TableIdent};
use crate::loaded_table::{
    IcebergPhysicalTable, IcebergRestLoadTableVendedS3LeaseRefresher,
    IcebergRestVendedS3LeaseRefresher, IcebergVendedCredentialLeaseSeed,
    IcebergVendedS3RenewalCapability, parse_vended_access_delegation,
};
use crate::resources::IcebergMetadataResources;

static NEXT_ATTEMPT_METADATA_CACHE_OWNER: AtomicU64 = AtomicU64::new(1);

/// Provider-private, attempt-local table materialization cache.  It is stored
/// inside `ConnectorRequestScope`, so neither a process-global cache nor a
/// query plan can retain a request-bound FileIO or response-local secret.
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
    result: Mutex<Option<Result<IcebergPhysicalTable, (ConnectorErrorKind, String)>>>,
    ready: Condvar,
}

impl AttemptMetadataTableEntry {
    fn loading() -> Self {
        Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
        }
    }
}

impl AttemptMetadataTableCache {
    fn get_or_load(
        &self,
        key: AttemptMetadataTableKey,
        load: impl FnOnce() -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)>,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        let (entry, loader) = {
            let mut entries = self.entries.lock().expect("attempt metadata cache lock");
            match entries.get(&key) {
                Some(entry) => (Arc::clone(entry), false),
                None => {
                    let entry = Arc::new(AttemptMetadataTableEntry::loading());
                    entries.insert(key, Arc::clone(&entry));
                    (entry, true)
                }
            }
        };
        if loader {
            let result = load();
            let mut stored = entry.result.lock().expect("attempt metadata entry lock");
            debug_assert!(stored.is_none(), "attempt metadata entry completes once");
            *stored = Some(result.clone());
            entry.ready.notify_all();
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
}

/// Everything one Iceberg control generation owns.
///
/// The generation holds exactly one catalog, and every operation family reaches
/// it through [`IcebergMetadataContext::novarocks_catalog`]. There is no second
/// handle and no concrete-kind slot: a generation that held two clients would
/// have two pieces of in-memory state that disagree about the same lake.
///
/// Callers that need the vendored client -- the commit machinery, which submits
/// through `Catalog::update_table` -- derive it from the owner rather than from
/// a field of their own.
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
        let configuration = control_state.configuration().clone();
        let binding = resources.planning_binding().clone();
        let catalog = resources
            .catalog_runtime()
            .block_on(async move {
                crate::catalog_runtime::build_catalog_client_with_rest_access_delegation(
                    &configuration,
                    binding,
                    rest_access_delegation,
                )
                .await
            })?
            .map_err(|error| format!("build Iceberg control-generation catalog: {error}"))?;
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
            attempt_metadata_cache_owner: NEXT_ATTEMPT_METADATA_CACHE_OWNER
                .fetch_add(1, Ordering::Relaxed),
        })
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
    /// `load_table` is used only as an authorization endpoint. The returned
    /// table must name the same UUID, but its metadata location and snapshot
    /// are ignored; the result is rebuilt from `frozen` with this attempt's
    /// request-local resolver.
    pub(crate) fn reacquire_table_access_for_request(
        &self,
        namespace: &str,
        table: &str,
        frozen: &IcebergPhysicalTable,
        request_context: &ConnectorRequestContext,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        let namespace = normalize_identifier(namespace).map_err(invalid_request)?;
        let table = normalize_identifier(table).map_err(invalid_request)?;
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
        let ident = TableIdent::from_strs([namespace.as_str(), table.as_str()])
            .map_err(|error| invalid_request(format!("build Iceberg table identity: {error}")))?;
        if frozen.table.identifier() != &ident {
            return Err((
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "reacquire Iceberg table {namespace}.{table}: target does not match the frozen table identity"
                ),
            ));
        }
        let capability = frozen
            .attempt_access()
            .map_err(|error| (error.kind(), error.to_string()))?;
        let rest_catalog = self
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
        if let IcebergVendedS3RenewalCapability::CredentialsEndpoint(scope) = capability {
            let catalog = Arc::clone(&rest_catalog);
            let endpoint = scope.endpoint().to_string();
            let delegation = self
                .resources
                .catalog_runtime()
                .block_on(async move {
                    catalog
                        .load_credentials_with_access_delegation(&endpoint)
                        .await
                })
                .map_err(unavailable)?
                .map_err(|error| {
                    (
                        ConnectorErrorKind::Unavailable,
                        format!("reacquire Iceberg table {namespace}.{table} credentials: {error}"),
                    )
                })?;
            let seed = parse_vended_access_delegation(&delegation)
                .map_err(|error| (error.kind(), error.to_string()))?
                .into_vended_lease_seed()
                .ok_or_else(|| {
                    (
                        ConnectorErrorKind::Unsupported,
                        format!(
                            "reacquire Iceberg table {namespace}.{table}: credentials endpoint returned static access"
                        ),
                    )
                })?;
            if !scope.matches_seed(&seed) {
                return Err((
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "reacquire Iceberg table {namespace}.{table}: credentials endpoint changed prefix scope or endpoint"
                    ),
                ));
            }
            seed.validate_table_access(&frozen.table)
                .map_err(|error| (error.kind(), error.to_string()))?;
            let contribution = seed
                .into_vended_s3_credential_lease_contribution()
                .and_then(|contribution| {
                    contribution.with_refresher(Arc::new(IcebergRestVendedS3LeaseRefresher::new(
                        rest_catalog,
                        self.resources.catalog_runtime().clone(),
                        scope,
                    )))
                })
                .map_err(|error| (error.kind(), error.to_string()))?;
            collection
                .offer_vended_s3_credential_lease(contribution)
                .map_err(|error| (error.kind(), error.to_string()))?;
            let request_binding = self
                .resources
                .planning_binding()
                .for_request(request_context.clone());
            return IcebergPhysicalTable::reacquired_request_scoped(frozen, request_binding)
                .map_err(|error| (error.kind(), error.to_string()));
        }
        let target =
            crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        let owner = Arc::clone(self.novarocks_catalog());
        let loaded = self
            .resources
            .catalog_runtime()
            .block_on(async move { owner.load_table(target).await })
            .map_err(unavailable)?
            .map_err(|error| {
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
        let request_binding = self
            .resources
            .planning_binding()
            .for_request(request_context.clone());
        let observed = materialization
            .materialize_for_request(request_binding.clone())
            .map_err(|error| (error.kind(), error.to_string()))?;
        IcebergPhysicalTable::validate_attempt_reacquisition(frozen, &observed, &seed)
            .map_err(|error| (error.kind(), error.to_string()))?;
        let contribution =
            self.vended_attempt_contribution(seed, ident, frozen.table.metadata().uuid())?;
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
        IcebergPhysicalTable::reacquired_request_scoped(frozen, request_binding)
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

    /// Load a table and immediately hand a REST-vended credential response to
    /// the request-local query-attempt collector. A missing collector remains
    /// fail-closed, and no vended response is ever inserted into the physical
    /// table cache.
    pub(crate) fn load_table_classified_with_credential_lease_collection(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        let namespace = normalize_identifier(namespace).map_err(invalid_request)?;
        let table = normalize_identifier(table).map_err(invalid_request)?;
        let Some(request_context) = request_context else {
            return self.load_table_classified_uncached(
                &namespace,
                &table,
                credential_lease_collection,
                None,
            );
        };
        // The cache is an admission-only freeze. A terminal write context has
        // deliberately dropped the collector and carries a replacement
        // terminal-only resolver; it must reload through that resolver rather
        // than reuse a FileIO that was bound to the active attempt lease.
        //
        // It must also reach the catalog rather than the control-state table
        // cache: this is the observation a commit decides against, so a cached
        // table would let it compute replacements from a snapshot the branch
        // has already moved past.
        if request_context.vended_credential_lease_sink().is_none() {
            return self.observe_table_classified(
                &namespace,
                &table,
                credential_lease_collection,
                Some(request_context),
            );
        }
        let cache = request_context
            .request_scope_extension_or_insert_with(AttemptMetadataTableCache::default);
        cache.get_or_load(
            AttemptMetadataTableKey {
                owner: self.attempt_metadata_cache_owner,
                namespace: namespace.clone(),
                table: table.clone(),
            },
            || {
                self.load_table_classified_uncached(
                    &namespace,
                    &table,
                    credential_lease_collection,
                    Some(request_context),
                )
            },
        )
    }

    /// Perform the one physical catalog observation for a cache miss. The
    /// caller owns normalization and, when applicable, the attempt-local
    /// single-flight entry that memoizes both its value and failure.
    /// Load past the attempt-local cache, but still accept a control-state
    /// cached table when no vended credential collection forces a fresh
    /// observation.
    ///
    /// This is a read-side reuse. A caller whose result decides an external
    /// effect must use [`Self::observe_table_classified`] instead: the name of
    /// this one says only that it skips the *attempt* cache.
    fn load_table_classified_uncached(
        &self,
        namespace: &str,
        table: &str,
        credential_lease_collection: Option<&ConnectorVendedCredentialLeaseCollectionPort>,
        request_context: Option<&ConnectorRequestContext>,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        if credential_lease_collection.is_none() {
            if let Some(table) = self
                .control_state
                .physical_table_cache()
                .get(&namespace, &table)
                .map_err(unavailable)?
            {
                return Ok(table);
            }
        }
        self.observe_table_classified(
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
        let ident = TableIdent::from_strs([namespace, table])
            .map_err(|error| invalid_request(format!("build Iceberg table identity: {error}")))?;
        let owner = Arc::clone(self.novarocks_catalog());
        let target =
            crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        let loaded = self
            .resources
            .catalog_runtime()
            .block_on(async move { owner.load_table(target).await })
            .map_err(unavailable)?
            .map_err(|error| {
                (
                    error.kind(),
                    format!("load Iceberg table {namespace}.{table}: {error}"),
                )
            })?;
        let (materialization, access_delegation) = loaded.into_parts();
        if let Some(seed) = access_delegation.into_vended_lease_seed() {
            let request_context = request_context.ok_or_else(|| {
                (
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "load Iceberg table {namespace}.{table}: vended REST credentials require a request-scoped storage resolver"
                    ),
                )
            })?;
            if credential_lease_collection.is_none() && request_context.storage_resolver().is_none()
            {
                return Err((
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "load Iceberg table {namespace}.{table}: vended REST terminal reload requires an admitted storage resolver"
                    ),
                ));
            }
            let request_binding = self
                .resources
                .planning_binding()
                .for_request(request_context.clone());
            let observed_table = materialization
                .materialize_for_request(request_binding.clone())
                .map_err(|error| (error.kind(), error.to_string()))?;
            seed.validate_table_access(&observed_table)
                .map_err(|error| (error.kind(), error.to_string()))?;
            let attempt_access = seed.renewal_capability();
            if let Some(collection) = credential_lease_collection {
                let contribution = self.vended_attempt_contribution(
                    seed,
                    ident,
                    observed_table.metadata().uuid(),
                )?;
                collection
                    .offer_vended_s3_credential_lease(contribution)
                    .map_err(|error| {
                        (
                            error.kind(),
                            format!(
                                "load Iceberg table {namespace}.{table}: collect vended credentials: {error}"
                            ),
                        )
                    })?;
            } else if request_context.vended_credential_lease_sink().is_some() {
                return Err((
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
        let loaded_table = materialization
            .into_static_table()
            .map_err(|error| (error.kind(), error.to_string()))?;
        let physical = IcebergPhysicalTable::new(loaded_table);
        self.control_state
            .physical_table_cache()
            .insert(&namespace, &table, physical.clone())
            .map_err(unavailable)?;
        Ok(physical)
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
        let renewal = seed.renewal_capability();
        let contribution = seed
            .into_vended_s3_credential_lease_contribution()
            .map_err(|error| (error.kind(), error.to_string()))?;
        let catalog = self
            .novarocks_catalog
            .vended_credential_refresh_catalog()
            .ok_or_else(|| {
                (
                    ConnectorErrorKind::Unsupported,
                    "vended REST credential acquisition has no catalog owner".to_string(),
                )
            })?;
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
            .with_refresher(refresher)
            .map_err(|error| (error.kind(), error.to_string()))
    }

    pub(crate) fn list_namespaces(&self) -> Result<Vec<String>, String> {
        let owner = Arc::clone(self.novarocks_catalog());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.list_namespaces().await })?
            .map_err(|error| format!("list Iceberg namespaces: {error}"))
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

    pub(crate) fn list_tables(&self, namespace: &str) -> Result<Vec<String>, String> {
        let namespace = NamespaceIdent::new(normalize_identifier(namespace)?);
        let namespace_label = namespace.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target = crate::catalog::CatalogNamespaceName::new(namespace.to_url_string());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.list_tables(target).await })?
            .map_err(|error| format!("list Iceberg tables in {namespace_label}: {error}"))
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
    use std::sync::Arc;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    use super::*;

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

        // Every vendored handle the generation hands out is the same
        // allocation. Two clients built from one configuration would have
        // separate in-memory state and disagree about the same lake -- a table
        // dropped through one would still resolve through the other. That held
        // once during development and cost a passing test to find, which is why
        // the context now has no second handle to get wrong.
        let first = generation.novarocks_catalog().vendored_client();
        let second = generation.novarocks_catalog().vendored_client();
        assert!(
            Arc::ptr_eq(&first, &second),
            "a generation must hold exactly one catalog client"
        );
    }
}
