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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, ResolvedQueryOptions,
};
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_proto_codec::catalog::CatalogSet;
use novarocks_proto_codec::lifecycle::{QueryExecutionId, QueryOptions as ProtocolQueryOptions};
use novarocks_query_application::api::LiveBackendTarget;
use novarocks_spi::connector::{
    CatalogCredentialMode, CatalogCredentialPurpose, CatalogNonSecretProperty, CatalogProperties,
    CatalogStorageAccessDomainInput, ConnectorControlPlanningLease, ConnectorError,
    ConnectorErrorKind, ConnectorProviderId, ConnectorStorageResolver,
    ConnectorVendedCredentialLeaseSink, CredentialConsumerRole, CredentialLeaseDescriptor,
    CredentialLeaseId, CredentialLeaseProvider, CredentialLeaseSecretEnvelope,
    ResolvedVendedS3Access, StorageAccessRequest, StorageCredentialScopePrefix,
    VendedS3CredentialLeaseContribution, VendedS3SeedMaterial,
};
use novarocks_types::NativeCompatibilityId;
use sha2::{Digest, Sha256};

const ATTEMPT_CREDENTIAL_LEASE_ID_DOMAIN: &[u8] = b"novarocks.attempt-credential-lease-id.v1";

/// Secret-free indirection for one attempt's FE-local storage authority. It
/// follows the move-only lease owner from candidate planning into the admitted
/// lifecycle control, but never owns a secret envelope itself.
struct AttemptCredentialLeaseStorageRoute {
    owner: Mutex<AttemptCredentialLeaseStorageRouteOwner>,
}

enum AttemptCredentialLeaseStorageRouteOwner {
    Planning(Weak<AttemptCredentialLeaseCollector>),
    Admitted(Weak<AttemptCredentialStorage>),
    Revoked,
}

/// A single in-flight resolve's original owner projection. It may outlive
/// the route lock while the collector atomically seals its table.
enum CurrentAttemptStorageOwner {
    Planning(Arc<AttemptCredentialLeaseCollector>),
    Frozen(Arc<AttemptCredentialStorage>),
}
impl CurrentAttemptStorageOwner {
    fn resolve(
        self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        match self {
            Self::Planning(owner) => owner.resolve_retained_original(request),
            Self::Frozen(owner) => owner.resolve_vended_s3(request),
        }
    }
}

impl AttemptCredentialLeaseStorageRoute {
    fn new_planning(owner: Weak<AttemptCredentialLeaseCollector>) -> Self {
        Self {
            owner: Mutex::new(AttemptCredentialLeaseStorageRouteOwner::Planning(owner)),
        }
    }

    fn adopt(&self, owner: &Arc<AttemptCredentialStorage>) {
        *self.owner.lock().expect("attempt credential storage route") =
            AttemptCredentialLeaseStorageRouteOwner::Admitted(Arc::downgrade(owner));
    }

    fn revoke(&self) {
        *self.owner.lock().expect("attempt credential storage route") =
            AttemptCredentialLeaseStorageRouteOwner::Revoked;
    }

    fn current_owner(&self) -> Result<CurrentAttemptStorageOwner, ConnectorError> {
        let owner = self.owner.lock().expect("attempt credential storage route");
        match &*owner {
            AttemptCredentialLeaseStorageRouteOwner::Planning(owner) => {
                owner.upgrade().map(CurrentAttemptStorageOwner::Planning)
            }
            AttemptCredentialLeaseStorageRouteOwner::Admitted(owner) => {
                owner.upgrade().map(CurrentAttemptStorageOwner::Frozen)
            }
            AttemptCredentialLeaseStorageRouteOwner::Revoked => None,
        }
        .ok_or_else(vended_storage_access_denied)
    }
}

impl ConnectorStorageResolver for AttemptCredentialLeaseStorageRoute {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        // Sealing may have happened after upgrading the original weak
        // planning owner. The route mutex is released before resolving the
        // exact current table under the collector's own transition lock.
        self.current_owner()?.resolve(request)
    }
}

/// Secret-free contribution capability for provider calls that may outlive
/// their attempt actor. The route never keeps the collector alive; an offer
/// arriving after cancellation fails closed instead of extending credential
/// lifetime through an uninterruptible Connector call.
struct AttemptCredentialLeaseSinkRoute {
    owner: Weak<AttemptCredentialLeaseCollector>,
}

impl ConnectorVendedCredentialLeaseSink for AttemptCredentialLeaseSinkRoute {
    fn offer_vended_s3_credential_lease(
        &self,
        catalog_properties: &CatalogProperties,
        contribution: VendedS3CredentialLeaseContribution,
    ) -> Result<(), ConnectorError> {
        self.owner
            .upgrade()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "attempt credential collector is no longer available",
                )
            })?
            .offer_vended_s3_credential_lease(catalog_properties, contribution)
    }
}

/// Move-only FE collection state for credentials obtained during one candidate
/// attempt's metadata observation. The SPI sees only the sink trait; table,
/// plan, cache, and Core request types cannot recover these values.
pub(crate) struct AttemptCredentialLeaseCollector {
    execution_id: QueryExecutionId,
    state: Mutex<AttemptCredentialLeaseCollectorState>,
    frozen: OnceLock<Arc<AttemptCredentialStorage>>,
    route: Arc<AttemptCredentialLeaseStorageRoute>,
    sink_route: Arc<AttemptCredentialLeaseSinkRoute>,
}

struct AttemptCredentialLeaseCollectorState {
    scopes: BTreeSet<(
        novarocks_spi::connector::CatalogHandle,
        StorageCredentialScopePrefix,
    )>,
    leases: Vec<QueryCredentialLease>,
    phase: AttemptCredentialCollectionPhase,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum AttemptCredentialCollectionPhase {
    Collecting,
    NativeDrained,
    TerminalOnly,
}

impl AttemptCredentialLeaseCollector {
    pub(crate) fn new(execution_id: QueryExecutionId) -> Arc<Self> {
        Arc::new_cyclic(|collector| Self {
            execution_id,
            state: Mutex::new(AttemptCredentialLeaseCollectorState {
                scopes: BTreeSet::new(),
                leases: Vec::new(),
                phase: AttemptCredentialCollectionPhase::Collecting,
            }),
            frozen: OnceLock::new(),
            route: Arc::new(AttemptCredentialLeaseStorageRoute::new_planning(
                collector.clone(),
            )),
            sink_route: Arc::new(AttemptCredentialLeaseSinkRoute {
                owner: collector.clone(),
            }),
        })
    }

    pub(crate) fn has_collected_leases(&self) -> bool {
        !self
            .state
            .lock()
            .expect("attempt credential collector lock")
            .leases
            .is_empty()
    }

    /// Claims the one Native transfer right. A terminal-only seal never
    /// grants that right, even though both paths share the same frozen Arc.
    pub(crate) fn drain_credential_leases(
        &self,
    ) -> Result<QueryCredentialLeases, DistributedQueryError> {
        let mut state = self
            .state
            .lock()
            .expect("attempt credential collector lock");
        if state.phase != AttemptCredentialCollectionPhase::Collecting {
            return Err(contract_error(
                "attempt credential lease collector has no unclaimed Native transfer",
            ));
        }
        let storage =
            self.seal_locked(&mut state, AttemptCredentialCollectionPhase::NativeDrained)?;
        Ok(QueryCredentialLeases { storage })
    }

    /// Explicit terminal retirement freezes existing material and refuses all
    /// later offers. Repeating this, including sealed-empty, returns the same
    /// original table. It never turns failure into a Native startup grant.
    pub(crate) fn seal_for_terminal(
        &self,
    ) -> Result<Arc<AttemptCredentialStorage>, DistributedQueryError> {
        let mut state = self
            .state
            .lock()
            .expect("attempt credential collector lock");
        if state.phase == AttemptCredentialCollectionPhase::Collecting {
            return self.seal_locked(&mut state, AttemptCredentialCollectionPhase::TerminalOnly);
        }
        Ok(Arc::clone(self.frozen.get().expect(
            "a retired collector published its original frozen table",
        )))
    }

    fn verify_native_storage(
        &self,
        execution: QueryExecutionId,
        storage: &Arc<AttemptCredentialStorage>,
    ) -> Result<(), DistributedQueryError> {
        let state = self
            .state
            .lock()
            .expect("attempt credential collector lock");
        if self.execution_id != execution
            || state.phase != AttemptCredentialCollectionPhase::NativeDrained
            || !self
                .frozen
                .get()
                .is_some_and(|original| Arc::ptr_eq(original, storage))
        {
            return Err(contract_error(
                "Native credential handoff does not retain the original drained attempt table",
            ));
        }
        Ok(())
    }

    fn seal_locked(
        &self,
        state: &mut AttemptCredentialLeaseCollectorState,
        phase: AttemptCredentialCollectionPhase,
    ) -> Result<Arc<AttemptCredentialStorage>, DistributedQueryError> {
        debug_assert!(state.phase == AttemptCredentialCollectionPhase::Collecting);
        let validation = canonicalize_credential_leases(&mut state.leases);
        let storage = Arc::new(AttemptCredentialStorage {
            leases: std::mem::take(&mut state.leases),
            route: Some(Arc::clone(&self.route)),
        });
        // Every read and write of the collection/frozen transition holds the
        // same state lock. No resolver can observe drained without this Arc.
        assert!(self.frozen.set(Arc::clone(&storage)).is_ok());
        // A malformed Native contribution still closes collection. It must
        // never reopen startup or accept late material after a failed seal.
        state.phase = if validation.is_ok() {
            phase
        } else {
            AttemptCredentialCollectionPhase::TerminalOnly
        };
        state.scopes.clear();
        self.route.adopt(&storage);
        validation?;
        Ok(storage)
    }

    /// A terminal responsibility owner may retain the original collector
    /// before planning has finished. Ordinary wrapper Drop only releases that
    /// custody; failure/retirement must call seal_for_terminal explicitly.
    pub(crate) fn terminal_storage_resolver(
        self: &Arc<Self>,
    ) -> Arc<AttemptTerminalCredentialResolver> {
        Arc::new(AttemptTerminalCredentialResolver {
            collector: Arc::clone(self),
        })
    }

    fn resolve_retained_original(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        let state = self
            .state
            .lock()
            .expect("attempt credential collector lock");
        if state.phase == AttemptCredentialCollectionPhase::Collecting {
            return resolve_vended_s3_access(&state.leases, request);
        }
        let storage = Arc::clone(
            self.frozen
                .get()
                .expect("a retired collector published its original frozen table"),
        );
        drop(state);
        storage.resolve_vended_s3(request)
    }

    pub(crate) fn sink(&self) -> Arc<dyn ConnectorVendedCredentialLeaseSink> {
        Arc::clone(&self.sink_route) as Arc<dyn ConnectorVendedCredentialLeaseSink>
    }

    /// The collector is also the FE-local storage authority while planning is
    /// still observing metadata. It deliberately stops resolving after its
    /// one-way hand-off to the lifecycle plan, so a stale planning context
    /// cannot retain a credential capability past attempt admission.
    pub(crate) fn storage_resolver(self: &Arc<Self>) -> Arc<dyn ConnectorStorageResolver> {
        Arc::clone(&self.route) as Arc<dyn ConnectorStorageResolver>
    }
}

impl ConnectorStorageResolver for AttemptCredentialLeaseCollector {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        let state = self
            .state
            .lock()
            .expect("attempt credential collector lock");
        if state.phase != AttemptCredentialCollectionPhase::Collecting {
            return Err(vended_storage_access_denied());
        }
        resolve_vended_s3_access(&state.leases, request)
    }
}

impl ConnectorVendedCredentialLeaseSink for AttemptCredentialLeaseCollector {
    fn offer_vended_s3_credential_lease(
        &self,
        catalog_properties: &CatalogProperties,
        contribution: VendedS3CredentialLeaseContribution,
    ) -> Result<(), ConnectorError> {
        let binding = catalog_properties
            .credential_bindings()
            .iter()
            .find(|binding| {
                binding.purpose() == CatalogCredentialPurpose::ObjectStoreData
                    && binding.consumer_role() == CredentialConsumerRole::Backend
                    && matches!(binding.mode(), CatalogCredentialMode::Vended)
            })
            .cloned()
            .ok_or_else(|| collector_error("catalog has no vended object-store data binding"))?;
        if catalog_properties.provider_id().as_str() != "iceberg" {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "vended S3 credential collection currently supports Iceberg catalogs only",
            ));
        }
        let provider_id = ConnectorProviderId::parse("iceberg")
            .map_err(|error| collector_error(&format!("parse Iceberg provider id: {error}")))?;
        let non_secret_properties = catalog_properties
            .execution_properties()
            .iter()
            .map(|property| CatalogNonSecretProperty::try_new(property.key(), property.value()))
            .collect::<Result<Vec<_>, _>>()?;
        // The acquisition path is non-secret -- the coordinator's frozen
        // routing context and, for load-table, the table identity -- and it is
        // the one fact a consumer needs to acquire for itself, so it rides the
        // descriptor beside the scope it applies to (CAD-1 D1, D2; IRU-13a L15).
        let (entries, renewal_path, provider_refresher) = contribution.into_parts_with_refresher();
        let mut state = self
            .state
            .lock()
            .expect("attempt credential collector lock");
        if state.phase != AttemptCredentialCollectionPhase::Collecting {
            return Err(collector_error(
                "vended credential contribution arrived after attempt collector consumption",
            ));
        }
        for entry in entries {
            let (prefix, not_after_unix_ms, access_key_id, secret_access_key, session_token) =
                entry.into_parts();
            let owner = catalog_properties.handle().clone();
            if !state.scopes.insert((owner.clone(), prefix.clone())) {
                // The first response-local value is the sole retained value
                // for this exact attempt scope. The duplicate is dropped here,
                // never compared, logged, cached, or exported.
                continue;
            }
            let access_domain = CatalogStorageAccessDomainInput::try_new(
                provider_id.clone(),
                owner.catalog_name().clone(),
                catalog_properties.config_format_version(),
                non_secret_properties.clone(),
                binding.clone(),
                vec![prefix.clone()],
            )
            .map(|input| input.derive_access_domain())?;
            let lease_id = credential_lease_id(self.execution_id, &owner, &prefix)?;
            let refresh_capable = provider_refresher.is_some();
            let descriptor = CredentialLeaseDescriptor::try_new(
                lease_id,
                1,
                owner,
                CredentialLeaseProvider::S3,
                vec![prefix],
                not_after_unix_ms,
                refresh_capable,
                renewal_path.clone(),
                access_domain,
            )?;
            let envelope = CredentialLeaseSecretEnvelope::try_new(
                lease_id,
                1,
                access_key_id,
                secret_access_key,
                session_token,
                not_after_unix_ms,
            )
            .map_err(|error| {
                collector_error(&format!("build vended credential envelope: {error}"))
            })?;
            state.leases.push(
                QueryCredentialLease::try_new(descriptor, envelope)
                    .map_err(|error| collector_error(error.message()))?
                    .with_provider(provider_refresher.clone()),
            );
        }
        Ok(())
    }
}

fn credential_lease_id(
    execution_id: QueryExecutionId,
    owner: &novarocks_spi::connector::CatalogHandle,
    prefix: &StorageCredentialScopePrefix,
) -> Result<CredentialLeaseId, ConnectorError> {
    let mut digest = Sha256::new();
    digest.update(ATTEMPT_CREDENTIAL_LEASE_ID_DOMAIN);
    digest.update(execution_id.query_id().high().to_be_bytes());
    digest.update(execution_id.query_id().low().to_be_bytes());
    digest.update(execution_id.attempt_id().get().to_be_bytes());
    digest.update(owner.catalog_name().as_str().as_bytes());
    digest.update(owner.version().as_bytes());
    digest.update(prefix.as_str().as_bytes());
    let digest = digest.finalize();
    let bytes: [u8; 16] = digest[..16]
        .try_into()
        .expect("SHA-256 digest always contains a credential lease id");
    CredentialLeaseId::try_from_bytes(bytes)
}

fn collector_error(message: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

/// One query-attempt lease frozen before any participant Init is dispatched.
/// Values remain FE-local until `materialize` forms the TLS-only Init side
/// channel; manifests receive only the descriptor.
pub(crate) struct QueryCredentialLease {
    descriptor: CredentialLeaseDescriptor,
    envelope: CredentialLeaseSecretEnvelope,
    seed_grant: novarocks_spi::connector::VendedS3SeedGrantId,
    /// How this consumer renews.
    ///
    /// The coordinator is a consumer of this material too, and its own storage
    /// authority acquires through this. It is the only refresh path left: the
    /// rotation this used to sit beside is gone (CAD-1 C14).
    provider: Option<novarocks_spi::connector::VendedS3InProcessAcquisition>,
}

impl QueryCredentialLease {
    pub(crate) fn try_new(
        descriptor: CredentialLeaseDescriptor,
        envelope: CredentialLeaseSecretEnvelope,
    ) -> Result<Self, DistributedQueryError> {
        if !envelope.matches_descriptor(&descriptor) {
            return Err(contract_error(
                "query credential lease initial envelope does not match descriptor",
            ));
        }
        Ok(Self {
            descriptor,
            envelope,
            // One admitted seed envelope authorizes this exact grant instance.
            // Resolver calls copy the identity; they never mint another grant.
            seed_grant: novarocks_spi::connector::VendedS3SeedGrantId::new(),
            provider: None,
        })
    }

    /// Retain the provider capability this lease's rotation adapter wraps.
    pub(crate) fn with_provider(
        mut self,
        provider: Option<novarocks_spi::connector::VendedS3InProcessAcquisition>,
    ) -> Self {
        self.provider = provider;
        self
    }

    pub(crate) const fn descriptor(&self) -> &CredentialLeaseDescriptor {
        &self.descriptor
    }

    pub(crate) const fn envelope(&self) -> &CredentialLeaseSecretEnvelope {
        &self.envelope
    }

    pub(crate) fn provider(
        &self,
    ) -> Option<&novarocks_spi::connector::VendedS3InProcessAcquisition> {
        self.provider.as_ref()
    }
}

/// Canonical secret-bearing lease contribution retained by the FE attempt.
/// The unique table is not copied: Native materialization borrows this exact
/// frozen Arc, and terminal storage later receives that same allocation.
pub(crate) struct QueryCredentialLeases {
    storage: Arc<AttemptCredentialStorage>,
}

fn canonicalize_credential_leases(
    leases: &mut [QueryCredentialLease],
) -> Result<(), DistributedQueryError> {
    leases.sort_by_key(|lease| lease.descriptor().lease_id());
    if leases
        .windows(2)
        .any(|pair| pair[0].descriptor().lease_id() == pair[1].descriptor().lease_id())
    {
        return Err(contract_error(
            "query credential lease contribution repeats a lease id",
        ));
    }
    Ok(())
}

impl QueryCredentialLeases {
    pub(crate) fn empty() -> Self {
        Self {
            storage: Arc::new(AttemptCredentialStorage {
                leases: Vec::new(),
                route: None,
            }),
        }
    }

    pub(crate) fn leases(&self) -> &[QueryCredentialLease] {
        &self.storage.leases
    }

    /// Move the same table even when empty: identity-sensitive handoff must
    /// retain the original allocation rather than replacing it with absence.
    pub(crate) fn into_storage(self) -> Arc<AttemptCredentialStorage> {
        self.storage
    }
}

/// One immutable original attempt credential table. Provider-local refresh
/// capabilities remain attached to the original leases; they do not mutate
/// this table or replace its material during Native/terminal handoff.
pub(crate) struct AttemptCredentialStorage {
    leases: Vec<QueryCredentialLease>,
    route: Option<Arc<AttemptCredentialLeaseStorageRoute>>,
}

impl ConnectorStorageResolver for AttemptCredentialStorage {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        resolve_vended_s3_access(&self.leases, request)
    }
}

impl Drop for AttemptCredentialStorage {
    /// The original weak route is revoked only when the final owner of this
    /// frozen table exits, never because one observer or Native waiter drops.
    fn drop(&mut self) {
        if let Some(route) = &self.route {
            route.revoke();
        }
    }
}

/// Strong custody reserved for the terminal responsibility owner. Planning
/// routes and contribution sinks deliberately remain weak. This wrapper is
/// not adopted by the route and cannot grant or replay Native initialization.
pub(crate) struct AttemptTerminalCredentialResolver {
    collector: Arc<AttemptCredentialLeaseCollector>,
}

impl AttemptTerminalCredentialResolver {
    /// Pure observation of the original Native transfer. This never seals a
    /// collector or grants startup to a terminal-only table.
    pub(crate) fn verify_native_storage(
        &self,
        execution: QueryExecutionId,
        storage: &Arc<AttemptCredentialStorage>,
    ) -> Result<(), DistributedQueryError> {
        self.collector.verify_native_storage(execution, storage)
    }

    pub(crate) fn seal_for_terminal(
        &self,
    ) -> Result<Arc<AttemptCredentialStorage>, DistributedQueryError> {
        self.collector.seal_for_terminal()
    }

    fn resolve_scope(
        &self,
        request: &StorageAccessRequest,
        selection: &TerminalScopeSelection,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        let state = self
            .collector
            .state
            .lock()
            .expect("attempt credential collector lock");
        if state.phase == AttemptCredentialCollectionPhase::Collecting {
            return resolve_terminal_scope(&state.leases, request, selection);
        }
        let frozen = Arc::clone(self.collector.frozen.get().expect("retired original table"));
        drop(state);
        resolve_terminal_scope(&frozen.leases, request, selection)
    }
}

impl ConnectorStorageResolver for AttemptTerminalCredentialResolver {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        self.collector.resolve_retained_original(request)
    }
}

// A scope token pins its original collector and exact lease, not an index in
// the collecting Vec that canonicalization may later reorder.
struct TerminalScopeSelection {
    lease_id: CredentialLeaseId,
    prefix: StorageCredentialScopePrefix,
}
struct SelectedTerminalScope {
    resolver: Arc<AttemptTerminalCredentialResolver>,
    selection: TerminalScopeSelection,
}
impl SelectedTerminalScope {
    fn resolve(
        self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        self.resolver.resolve_scope(request, &self.selection)
    }
}

struct IndexedTerminalScope {
    owner: novarocks_spi::connector::CatalogHandle,
    original: SelectedTerminalScope,
}
#[derive(Default)]
struct SessionTerminalCredentials {
    entries: Vec<Arc<AttemptTerminalCredentialResolver>>,
    by_execution: HashMap<QueryExecutionId, usize>,
    frozen: Option<Result<Arc<[IndexedTerminalScope]>, ConnectorError>>,
}

#[derive(Debug)]
struct TerminalCredentialSealFailure {
    original: DistributedQueryError,
    fact: novarocks_failure_contract::FailureFact,
}
impl std::fmt::Display for TerminalCredentialSealFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.original, f)
    }
}
impl std::error::Error for TerminalCredentialSealFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}
impl novarocks_failure_contract::FailureProjection for TerminalCredentialSealFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.fact
    }
}

/// One Session keeps all original stage capabilities until its real terminal
/// holders exit. Selection grants no Native startup or computation position.
#[derive(Default)]
pub(crate) struct SessionTerminalCredentialRouter {
    original: Mutex<SessionTerminalCredentials>,
    #[cfg(test)]
    test_resolver: Option<Arc<dyn ConnectorStorageResolver>>,
}
impl SessionTerminalCredentialRouter {
    pub(crate) fn register(
        &self,
        credentials: &super::logical_execution::InitialComputationCredentials,
    ) -> Result<(), ConnectorError> {
        self.register_original(
            credentials.execution(),
            credentials.terminal_storage_resolver(),
        )
    }

    fn register_original(
        &self,
        execution: QueryExecutionId,
        resolver: Arc<AttemptTerminalCredentialResolver>,
    ) -> Result<(), ConnectorError> {
        if resolver.collector.execution_id != execution {
            return Err(collector_error(
                "terminal credentials have a foreign original execution",
            ));
        }
        let mut state = self
            .original
            .lock()
            .map_err(|_| collector_error("terminal credential router is poisoned"))?;
        if let Some(index) = state.by_execution.get(&execution) {
            return if Arc::ptr_eq(&state.entries[*index], &resolver) {
                Ok(())
            } else {
                Err(collector_error(
                    "terminal credentials changed within one original execution",
                ))
            };
        }
        if state.frozen.is_some() {
            return Err(collector_error(
                "terminal credential registration is already frozen",
            ));
        }
        let index = state.entries.len();
        state.entries.push(resolver);
        state.by_execution.insert(execution, index);
        Ok(())
    }

    /// Only the original terminal Context frontier invokes this. Successful
    /// stages already drained their table for Native; a failed unstarted stage
    /// closes collection through the same terminal-only author. Neither proves
    /// a worker exit or admits another Native start.
    pub(crate) fn freeze(&self) -> Result<(), ConnectorError> {
        #[cfg(test)]
        if self.test_resolver.is_some() {
            return Ok(());
        }
        let mut state = self
            .original
            .lock()
            .map_err(|_| collector_error("terminal credential router is poisoned"))?;
        if let Some(frozen) = &state.frozen {
            return frozen.as_ref().map(|_| ()).map_err(Clone::clone);
        }
        // Local original transition locks only: no Provider or dyn resolver
        // callback runs while registration and its scope snapshot are frozen.
        let frozen = Self::freeze_entries(&state.entries);
        let result = frozen.as_ref().map(|_| ()).map_err(Clone::clone);
        state.frozen = Some(frozen);
        result
    }

    fn freeze_entries(
        entries: &[Arc<AttemptTerminalCredentialResolver>],
    ) -> Result<Arc<[IndexedTerminalScope]>, ConnectorError> {
        let mut scopes = BTreeMap::new();
        let mut failure = None;
        for entry in entries {
            let storage = match entry.seal_for_terminal() {
                Ok(storage) => storage,
                Err(original) => {
                    if failure.is_none() {
                        let fact = original.clone().into_execution_failure_input().fact();
                        failure = Some(ConnectorError::from_domain(
                            ConnectorErrorKind::Internal,
                            TerminalCredentialSealFailure { original, fact },
                        ));
                    }
                    // Close every original collection even after the first
                    // ordinary validation refusal. This is not physical exit.
                    continue;
                }
            };
            if failure.is_some() {
                continue;
            }
            for lease in &storage.leases {
                let descriptor = lease.descriptor();
                if descriptor.provider() != CredentialLeaseProvider::S3 {
                    continue;
                }
                for prefix in descriptor.prefixes() {
                    // Same attempt responses already deduplicated their exact
                    // scope. Across real stages, latest authorization wins an
                    // equal scope, without inspecting expiry or failure.
                    scopes.insert(
                        (descriptor.owner().clone(), prefix.clone()),
                        IndexedTerminalScope {
                            owner: descriptor.owner().clone(),
                            original: SelectedTerminalScope {
                                resolver: Arc::clone(entry),
                                selection: TerminalScopeSelection {
                                    lease_id: descriptor.lease_id(),
                                    prefix: prefix.clone(),
                                },
                            },
                        },
                    );
                }
            }
        }
        if let Some(failure) = failure {
            return Err(failure);
        }
        Ok(scopes.into_values().collect::<Vec<_>>().into())
    }

    fn select(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<SelectedTerminalScope, ConnectorError> {
        let scopes = {
            let state = self
                .original
                .lock()
                .map_err(|_| collector_error("terminal credential router is poisoned"))?;
            state
                .frozen
                .as_ref()
                .ok_or_else(|| collector_error("terminal credential scopes are not frozen"))?
                .as_ref()
                .map(Arc::clone)
                .map_err(Clone::clone)?
        };
        // One O(1) snapshot pin and a scan of distinct scopes, not a Vec clone
        // and collector scan for every stage on every object-store request.
        let mut selected: Option<&IndexedTerminalScope> = None;
        for scope in scopes.iter() {
            let prefix = &scope.original.selection.prefix;
            if &scope.owner == request.owner()
                && request.location().starts_with(prefix.as_str())
                && selected.is_none_or(|old| {
                    prefix.as_str().len() > old.original.selection.prefix.as_str().len()
                })
            {
                selected = Some(scope);
            }
        }
        let selected = selected.ok_or_else(vended_storage_access_denied)?;
        Ok(SelectedTerminalScope {
            resolver: Arc::clone(&selected.original.resolver),
            selection: TerminalScopeSelection {
                lease_id: selected.original.selection.lease_id,
                prefix: selected.original.selection.prefix.clone(),
            },
        })
    }

    // The original terminal lifetime fixtures deliberately supply their own
    // finite observer. Production registration never accepts a dyn resolver.
    #[cfg(test)]
    pub(crate) fn for_test(resolver: Arc<dyn ConnectorStorageResolver>) -> Self {
        Self {
            original: Mutex::new(SessionTerminalCredentials::default()),
            test_resolver: Some(resolver),
        }
    }
}
impl ConnectorStorageResolver for SessionTerminalCredentialRouter {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        #[cfg(test)]
        if let Some(resolver) = &self.test_resolver {
            return resolver.resolve_vended_s3(request);
        }
        self.select(request)?.resolve(request)
    }
}

fn resolve_terminal_scope(
    leases: &[QueryCredentialLease],
    request: &StorageAccessRequest,
    selection: &TerminalScopeSelection,
) -> Result<ResolvedVendedS3Access, ConnectorError> {
    let mut exact = leases
        .iter()
        .filter(|lease| lease.descriptor().lease_id() == selection.lease_id);
    let lease = exact.next().ok_or_else(vended_storage_access_denied)?;
    if exact.next().is_some()
        || lease.descriptor().provider() != CredentialLeaseProvider::S3
        || lease.descriptor().owner() != request.owner()
        || !lease.descriptor().prefixes().contains(&selection.prefix)
        || !request.location().starts_with(selection.prefix.as_str())
    {
        return Err(vended_storage_access_denied());
    }
    Ok(resolve_original_vended_lease(lease, &selection.prefix))
}

pub(crate) fn resolve_vended_s3_access(
    leases: &[QueryCredentialLease],
    request: &StorageAccessRequest,
) -> Result<ResolvedVendedS3Access, ConnectorError> {
    let (lease, matched_prefix) =
        select_vended_scope(leases, request).ok_or_else(vended_storage_access_denied)?;
    Ok(resolve_original_vended_lease(lease, matched_prefix))
}

// Both ordinary attempt lookup and the terminal router use this exact scope
// author. Expiry never influences which original lease is selected.
fn select_vended_scope<'a>(
    leases: &'a [QueryCredentialLease],
    request: &StorageAccessRequest,
) -> Option<(&'a QueryCredentialLease, &'a StorageCredentialScopePrefix)> {
    let mut selected: Option<(&QueryCredentialLease, &StorageCredentialScopePrefix)> = None;
    for lease in leases {
        let descriptor = lease.descriptor();
        if descriptor.provider() != CredentialLeaseProvider::S3
            || descriptor.owner() != request.owner()
        {
            continue;
        }
        for prefix in descriptor.prefixes() {
            if request.location().starts_with(prefix.as_str())
                && selected
                    .is_none_or(|(_, current)| prefix.as_str().len() > current.as_str().len())
            {
                selected = Some((lease, prefix));
            }
        }
    }
    selected
}

fn resolve_original_vended_lease(
    lease: &QueryCredentialLease,
    matched_prefix: &StorageCredentialScopePrefix,
) -> ResolvedVendedS3Access {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let selected = ResolvedVendedS3Access::new(
        lease.descriptor().storage_access_domain_id(),
        lease.descriptor().lease_id(),
        lease.envelope().epoch(),
        matched_prefix.clone(),
        lease.descriptor().renewal_path().cloned(),
        Some(lease.seed_grant),
        // Scope and acquisition capability survive material expiry. Retain
        // the original selection; never try a broader or another source's
        // scope merely because this seed can no longer sign a request.
        (lease.envelope().session_token_expires_at_unix_ms() > now).then(|| {
            VendedS3SeedMaterial::new(
                lease.envelope().session_token_expires_at_unix_ms(),
                lease.envelope().access_key_id().clone(),
                lease.envelope().secret_access_key().clone(),
                lease.envelope().session_token().clone(),
            )
        }),
    );
    // ... and it renews through that same provider rather than waiting for the
    // rotation the pump drives, so removing the pump takes nothing away from
    // this side (CAD-1 C13).
    match lease.provider() {
        Some(provider) => selected.with_provider(provider.clone()),
        None => selected,
    }
}

fn vended_storage_access_denied() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "vended storage access is unavailable for this query attempt",
    )
}

pub struct QueryInitOptions {
    execution_id: QueryExecutionId,
    /// Execution-owned options retained solely for sealed native fragment
    /// submission. They are not the lifecycle wire carrier.
    native_submission_options: QueryOptions,
    query_options: ProtocolQueryOptions,
    catalog_set: CatalogSet,
    credential_leases: QueryCredentialLeases,
}

impl QueryInitOptions {
    pub fn new(
        execution_id: QueryExecutionId,
        native_compatibility_id: NativeCompatibilityId,
        live_backends: Vec<LiveBackendTarget>,
        native_submission_options: &ResolvedQueryOptions,
        query_options: ProtocolQueryOptions,
    ) -> Result<Self, DistributedQueryError> {
        if live_backends.is_empty() {
            return Err(contract_error(
                "query initialization requires at least one live backend",
            ));
        }
        let mut backend_indices = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for target in &live_backends {
            target
                .process_id()
                .map_err(|error| contract_error(error.to_string()))?;
            let backend_compatibility_id = target.descriptor().native_compatibility_id();
            if backend_compatibility_id != native_compatibility_id {
                return Err(contract_error(format!(
                    "query initialization live snapshot contains backend {} from another compatibility island",
                    target.backend_idx()
                )));
            }
            if !backend_indices.insert(target.backend_idx()) {
                return Err(contract_error(format!(
                    "query initialization live snapshot repeats backend {}",
                    target.backend_idx()
                )));
            }
            let endpoint = target
                .endpoint()
                .map_err(|error| contract_error(error.to_string()))?;
            if !endpoints.insert(endpoint.clone()) {
                return Err(contract_error(format!(
                    "query initialization live snapshot repeats endpoint {}",
                    endpoint
                )));
            }
        }
        Ok(Self {
            execution_id,
            native_submission_options: native_submission_options.runtime_options().clone(),
            query_options,
            catalog_set: CatalogSet::new([]).expect("the empty catalog set is valid"),
            credential_leases: QueryCredentialLeases::empty(),
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    /// Frozen runtime options carried from Init through the sealed native
    /// submission view.  They are read-only encoder input, never a route to
    /// reacquire lifecycle or topology state.
    pub fn native_submission_options(&self) -> &QueryOptions {
        &self.native_submission_options
    }

    /// The exact validated protocol options frozen into every participant
    /// manifest. Core does not project execution options into this carrier.
    pub const fn query_options(&self) -> &ProtocolQueryOptions {
        &self.query_options
    }

    /// Freezes the query-wide catalog contribution that is copied unchanged
    /// into every participant's existing Init request. Query assembly owns
    /// choosing this set; lifecycle only preserves its exact validated value.
    pub fn with_catalog_set(mut self, catalog_set: CatalogSet) -> Self {
        self.catalog_set = catalog_set;
        self
    }

    pub fn catalog_set(&self) -> &CatalogSet {
        &self.catalog_set
    }

    /// Attaches exact FE-owned confidential material after planning has
    /// collected every table-operation requirement. The material is consumed
    /// once by Init materialization and never becomes part of the manifest.
    pub(crate) fn with_credential_leases(mut self, leases: QueryCredentialLeases) -> Self {
        self.credential_leases = leases;
        self
    }

    pub(crate) fn credential_leases(&self) -> &QueryCredentialLeases {
        &self.credential_leases
    }

    /// Takes the frozen credential table out of these options.
    ///
    /// Used once, after an establish has copied the material into wire form,
    /// so the attempt keeps exactly one owner of the secrets rather than two.
    pub(crate) fn take_credential_leases(&mut self) -> QueryCredentialLeases {
        std::mem::replace(&mut self.credential_leases, QueryCredentialLeases::empty())
    }
}

/// FE-local ownership retained for every catalog dependency frozen into one
/// query Init contribution.  The catalog set is immutable once this lease is
/// constructed; the control leases keep the exact FE runtimes that produced
/// those artifacts alive until the attempt reaches a terminal path.
///
/// It is deliberately held by the attempt's own prepared-execution value rather
/// than by a transport or fragment carrier.  Catalog materialization is already
/// frozen when the attempt is prepared, while control-runtime lifetime remains
/// a Frontend-local concern.
pub(crate) struct QueryCatalogLease {
    catalog_set: CatalogSet,
    #[allow(
        dead_code,
        reason = "Drop retains these exact FE control leases until the attempt is done with them."
    )]
    control_leases: Vec<ConnectorControlPlanningLease>,
}

impl QueryCatalogLease {
    pub(crate) fn new(
        catalog_set: CatalogSet,
        control_leases: Vec<ConnectorControlPlanningLease>,
    ) -> Self {
        Self {
            catalog_set,
            control_leases,
        }
    }

    pub(crate) fn catalog_set(&self) -> &CatalogSet {
        &self.catalog_set
    }

    #[cfg(test)]
    pub(crate) fn control_lease_count(&self) -> usize {
        self.control_leases.len()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::{AttemptCredentialLeaseCollector, QueryCatalogLease, QueryInitOptions};
    use crate::query_execution::contract::ResolvedQueryOptions;
    use novarocks_execution_contract::{BackendProcessDescriptor, RuntimeEndpoint};
    use novarocks_proto_codec::catalog::CatalogSet;
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId, QueryOptions};
    use novarocks_proto_models::novarocks;
    use novarocks_query_application::api::LiveBackendTarget;
    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogVersion, ConnectorControlPlanningLease, ConnectorInstanceId,
        ConnectorProviderId, ConnectorStorageResolver, ConnectorVendedCredentialLeaseSink,
        ConnectorVendedS3CredentialLeaseRefresher, CredentialConsumerRole, StorageAccessRequest,
        StorageCredentialScopePrefix, VendedS3CredentialLeaseContribution,
        VendedS3CredentialLeaseEntry, VendedS3CredentialLeaseRefresh,
    };
    use novarocks_types::BackendProcessId;
    use novarocks_types::QueryId;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(41, 73),
            AttemptId::new(7).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn wire_query_options() -> QueryOptions {
        QueryOptions::from_proto(novarocks::QueryOptions::default())
    }

    fn catalog_set() -> CatalogSet {
        CatalogSet::new([CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("catalog.analytics").expect("catalog name"),
                CatalogVersion::from_bytes([0x23; 32]),
            ),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![],
            vec![],
        )
        .expect("catalog properties")])
        .expect("catalog set")
    }

    fn vended_catalog_properties() -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("catalog.vended").expect("catalog name"),
                CatalogVersion::from_bytes([0x24; 32]),
            ),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::Backend,
                    CatalogCredentialMode::Vended,
                )
                .expect("vended binding"),
            ],
        )
        .expect("catalog properties")
    }

    fn vended_contribution(canary: &str) -> VendedS3CredentialLeaseContribution {
        VendedS3CredentialLeaseContribution::try_new(
            vec![
                VendedS3CredentialLeaseEntry::try_new(
                    StorageCredentialScopePrefix::try_from_normalized("s3://warehouse/data")
                        .expect("prefix"),
                    // The lease uses Unix milliseconds, not Unix seconds.
                    // This fixture is for ownership/handoff behavior, so keep
                    // its synthetic credential valid independently of wall
                    // clock progress.
                    u64::MAX,
                    SecretValue::new(format!("access-{canary}")),
                    SecretValue::new(format!("secret-{canary}")),
                    SecretValue::new(format!("token-{canary}")),
                )
                .expect("entry"),
            ],
            None,
        )
        .expect("contribution")
    }

    struct ProviderLocalRefresher;

    impl ConnectorVendedS3CredentialLeaseRefresher for ProviderLocalRefresher {
        fn refresh_vended_s3_credentials(
            &self,
            _policy: novarocks_spi::connector::VendedS3CredentialRefreshCallPolicy,
        ) -> Result<VendedS3CredentialLeaseRefresh, novarocks_spi::connector::ConnectorError>
        {
            panic!("the capability is not invoked by this collection test")
        }
    }

    struct DropTrackedRefresher {
        dropped: Arc<AtomicBool>,
    }

    impl Drop for DropTrackedRefresher {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl ConnectorVendedS3CredentialLeaseRefresher for DropTrackedRefresher {
        fn refresh_vended_s3_credentials(
            &self,
            _policy: novarocks_spi::connector::VendedS3CredentialRefreshCallPolicy,
        ) -> Result<VendedS3CredentialLeaseRefresh, novarocks_spi::connector::ConnectorError>
        {
            panic!("the drop-tracked refresher is never invoked")
        }
    }

    #[test]
    fn expired_original_material_retains_renewal_after_each_real_collection_cut() {
        for native_drained in [false, true] {
            let collector = AttemptCredentialLeaseCollector::new(execution_id());
            let terminal = collector.terminal_storage_resolver();
            let planning = collector.storage_resolver();
            let properties = vended_catalog_properties();
            let provider: Arc<dyn ConnectorVendedS3CredentialLeaseRefresher> =
                Arc::new(ProviderLocalRefresher);
            let path = novarocks_spi::connector::CredentialRenewalPath::CredentialsEndpoint(
                novarocks_spi::connector::CredentialRoutingContext::try_new(
                    "https://catalog.example/credentials",
                    [],
                )
                .unwrap(),
            );
            // A fresh, broader grant must not replace this exact expired
            // selection. Both enter through the real contribution receiver.
            collector
                .offer_vended_s3_credential_lease(&properties, vended_contribution("broader"))
                .unwrap();
            let contribution = VendedS3CredentialLeaseContribution::try_new(
                vec![
                    VendedS3CredentialLeaseEntry::try_new(
                        StorageCredentialScopePrefix::try_from_normalized(
                            "s3://warehouse/data/exact",
                        )
                        .unwrap(),
                        1,
                        SecretValue::new("expired-access"),
                        SecretValue::new("expired-secret"),
                        SecretValue::new("expired-token"),
                    )
                    .unwrap(),
                ],
                Some(path.clone()),
            )
            .unwrap()
            .with_refresher(
                novarocks_spi::connector::CredentialAcquisitionOwnerId::new(),
                Arc::clone(&provider),
            )
            .unwrap();
            collector
                .offer_vended_s3_credential_lease(&properties, contribution)
                .unwrap();
            let request = StorageAccessRequest::try_new(
                properties.handle().clone(),
                "s3://warehouse/data/exact/file.parquet",
            )
            .unwrap();
            let collecting = planning.resolve_vended_s3(&request).unwrap();
            assert!(collecting.seed().is_none());
            assert!(Arc::ptr_eq(
                collecting.provider().unwrap().refresher(),
                &provider
            ));
            let storage = if native_drained {
                collector.drain_credential_leases().unwrap().into_storage()
            } else {
                terminal.seal_for_terminal().unwrap()
            };
            assert_eq!(
                terminal
                    .verify_native_storage(execution_id(), &storage)
                    .is_ok(),
                native_drained,
                "terminal retirement must not mint a Native handoff"
            );
            drop(collector);
            for resolver in [
                &planning,
                &(terminal.clone() as Arc<dyn ConnectorStorageResolver>),
            ] {
                let resolved = resolver.resolve_vended_s3(&request).unwrap();
                assert!(resolved.seed().is_none());
                assert_eq!(
                    resolved.matched_prefix().as_str(),
                    "s3://warehouse/data/exact"
                );
                assert_eq!(resolved.lease_id(), collecting.lease_id());
                assert_eq!(resolved.seed_grant(), collecting.seed_grant());
                assert!(resolved.seed_grant().is_some());
                assert_eq!(
                    resolved.provider().unwrap().identity(),
                    collecting.provider().unwrap().identity()
                );
                assert_eq!(resolved.renewal_path(), Some(&path));
                assert!(Arc::ptr_eq(
                    resolved.provider().unwrap().refresher(),
                    &provider
                ));
            }
            assert!(Arc::ptr_eq(
                &storage,
                &terminal.seal_for_terminal().unwrap()
            ));
        }
    }

    fn terminal_stage(
        attempt: u64,
    ) -> (
        super::QueryExecutionId,
        Arc<AttemptCredentialLeaseCollector>,
    ) {
        let execution =
            QueryExecutionId::new(QueryId::new(41, 73), AttemptId::new(attempt).unwrap()).unwrap();
        (execution, AttemptCredentialLeaseCollector::new(execution))
    }

    fn offer_terminal_scope(
        collector: &AttemptCredentialLeaseCollector,
        prefix: &str,
        expiration: u64,
    ) {
        collector
            .offer_vended_s3_credential_lease(
                &vended_catalog_properties(),
                VendedS3CredentialLeaseContribution::try_new(
                    vec![
                        VendedS3CredentialLeaseEntry::try_new(
                            StorageCredentialScopePrefix::try_from_normalized(prefix).unwrap(),
                            expiration,
                            SecretValue::new("router-access"),
                            SecretValue::new("router-secret"),
                            SecretValue::new("router-token"),
                        )
                        .unwrap(),
                    ],
                    None,
                )
                .unwrap(),
            )
            .unwrap();
    }

    #[test]
    fn session_terminal_router_retains_distinct_stages_and_rejects_changed_original() {
        let router = super::SessionTerminalCredentialRouter::default();
        let (first_id, first) = terminal_stage(41);
        let (second_id, second) = terminal_stage(42);
        offer_terminal_scope(&first, "s3://warehouse/data", u64::MAX);
        offer_terminal_scope(&second, "s3://warehouse/other", u64::MAX);
        let first_terminal = first.terminal_storage_resolver();
        let second_terminal = second.terminal_storage_resolver();
        let weak_first = Arc::downgrade(&first);
        let weak_second = Arc::downgrade(&second);
        router
            .register_original(first_id, first_terminal.clone())
            .unwrap();
        router
            .register_original(first_id, first_terminal.clone())
            .unwrap();
        router
            .register_original(second_id, second_terminal.clone())
            .unwrap();
        assert!(
            router
                .register_original(first_id, first.terminal_storage_resolver())
                .is_err()
        );
        assert!(
            router
                .register_original(second_id, first_terminal.clone())
                .is_err()
        );
        let request = |location| {
            StorageAccessRequest::try_new(vended_catalog_properties().handle().clone(), location)
                .unwrap()
        };
        let expected_first = first_terminal
            .resolve_vended_s3(&request("s3://warehouse/data/file"))
            .unwrap();
        let expected_second = second_terminal
            .resolve_vended_s3(&request("s3://warehouse/other/file"))
            .unwrap();
        router.freeze().unwrap();
        router
            .register_original(first_id, first_terminal.clone())
            .unwrap();
        let (late_id, late) = terminal_stage(43);
        offer_terminal_scope(&late, "s3://warehouse/data", u64::MAX);
        assert!(
            router
                .register_original(late_id, late.terminal_storage_resolver())
                .is_err()
        );
        drop((first_terminal, second_terminal, first, second));
        assert!(weak_first.upgrade().is_some());
        assert!(weak_second.upgrade().is_some());
        assert_eq!(
            router
                .resolve_vended_s3(&request("s3://warehouse/data/file"))
                .unwrap()
                .lease_id(),
            expected_first.lease_id()
        );
        assert_eq!(
            router
                .resolve_vended_s3(&request("s3://warehouse/other/file"))
                .unwrap()
                .lease_id(),
            expected_second.lease_id()
        );
        let foreign_owner = CatalogHandle::new(
            ConnectorInstanceId::parse("foreign").unwrap(),
            CatalogVersion::from_bytes([2; 32]),
        );
        let foreign =
            StorageAccessRequest::try_new(foreign_owner, "s3://warehouse/data/file").unwrap();
        assert!(router.resolve_vended_s3(&foreign).is_err());
        drop(router);
        assert!(weak_first.upgrade().is_none());
        assert!(weak_second.upgrade().is_none());
    }

    #[test]
    fn session_terminal_router_selects_latest_exact_scope_without_expiry_fallback() {
        let request = StorageAccessRequest::try_new(
            vended_catalog_properties().handle().clone(),
            "s3://warehouse/data/file",
        )
        .unwrap();
        for (old_expiration, latest_expiration) in [(1, u64::MAX), (u64::MAX, 1)] {
            let router = super::SessionTerminalCredentialRouter::default();
            let (old_id, old) = terminal_stage(51);
            let (new_id, new) = terminal_stage(52);
            offer_terminal_scope(&old, "s3://warehouse/data", old_expiration);
            offer_terminal_scope(&new, "s3://warehouse/data", latest_expiration);
            // Same-attempt first response remains the sole original value.
            offer_terminal_scope(&new, "s3://warehouse/data", old_expiration);
            let old_terminal = old.terminal_storage_resolver();
            let new_terminal = new.terminal_storage_resolver();
            let old_lease = old_terminal.resolve_vended_s3(&request).unwrap();
            let newest_lease = new_terminal.resolve_vended_s3(&request).unwrap();
            router
                .register_original(old_id, old_terminal.clone())
                .unwrap();
            router.register_original(new_id, new_terminal).unwrap();
            // Retrying the old registration cannot promote it behind a new stage.
            router
                .register_original(old_id, old_terminal.clone())
                .unwrap();
            router.freeze().unwrap();
            router.register_original(old_id, old_terminal).unwrap();
            let token = router.select(&request).unwrap();
            let (late_id, late) = terminal_stage(53);
            offer_terminal_scope(&late, "s3://warehouse/data", u64::MAX);
            assert!(
                router
                    .register_original(late_id, late.terminal_storage_resolver())
                    .is_err()
            );
            let actual = token.resolve(&request).unwrap();
            assert_eq!(actual.lease_id(), newest_lease.lease_id());
            assert_ne!(actual.lease_id(), old_lease.lease_id());
            assert_eq!(actual.seed_grant(), newest_lease.seed_grant());
            assert_eq!(actual.seed().is_some(), latest_expiration == u64::MAX);
            assert!(actual.provider().is_none());
            assert!(
                actual.renewal_path().is_none(),
                "an expired selection never falls back to an older valid seed"
            );
        }
    }

    #[test]
    fn session_terminal_router_keeps_longest_prefix_and_exact_lease_across_freeze() {
        let router = super::SessionTerminalCredentialRouter::default();
        let (old_id, old) = terminal_stage(61);
        let owner = vended_catalog_properties();
        let mut prefixes = ["s3://warehouse/data/exact", "s3://warehouse/unrelated"];
        prefixes.sort_by_key(|prefix| {
            std::cmp::Reverse(
                super::credential_lease_id(
                    old_id,
                    owner.handle(),
                    &StorageCredentialScopePrefix::try_from_normalized(prefix).unwrap(),
                )
                .unwrap(),
            )
        });
        for prefix in prefixes {
            offer_terminal_scope(&old, prefix, 1);
        }
        let old_terminal = old.terminal_storage_resolver();
        router
            .register_original(old_id, old_terminal.clone())
            .unwrap();
        let request =
            StorageAccessRequest::try_new(owner.handle().clone(), "s3://warehouse/data/exact/file")
                .unwrap();
        let collecting = old_terminal.resolve_vended_s3(&request).unwrap();
        let before_order = old
            .state
            .lock()
            .unwrap()
            .leases
            .iter()
            .map(|lease| lease.descriptor().lease_id())
            .collect::<Vec<_>>();
        let storage = old.drain_credential_leases().unwrap().into_storage();
        let after_order = storage
            .leases
            .iter()
            .map(|lease| lease.descriptor().lease_id())
            .collect::<Vec<_>>();
        assert_ne!(
            before_order, after_order,
            "real canonical sorting changes indices"
        );
        let (new_id, new) = terminal_stage(62);
        offer_terminal_scope(&new, "s3://warehouse/data", u64::MAX);
        router
            .register_original(new_id, new.terminal_storage_resolver())
            .unwrap();
        router.freeze().unwrap();
        old_terminal
            .verify_native_storage(old_id, &storage)
            .unwrap();
        let actual = router.resolve_vended_s3(&request).unwrap();
        assert_eq!(actual.lease_id(), collecting.lease_id());
        assert_eq!(actual.seed_grant(), collecting.seed_grant());
        assert_eq!(
            actual.matched_prefix().as_str(),
            "s3://warehouse/data/exact"
        );
        assert!(
            actual.seed().is_none(),
            "a newer broader valid seed cannot replace a longer original scope"
        );
    }

    #[test]
    fn session_terminal_router_caches_the_original_freeze_failure() {
        use novarocks_failure_contract::FailureProjection;
        let router = Arc::new(super::SessionTerminalCredentialRouter::default());
        let (execution, original) = terminal_stage(71);
        let duplicate = AttemptCredentialLeaseCollector::new(execution);
        for collector in [&original, &duplicate] {
            offer_terminal_scope(collector, "s3://warehouse/data", u64::MAX);
        }
        let material = duplicate.drain_credential_leases().unwrap();
        drop(duplicate);
        let mut material = Arc::try_unwrap(material.storage)
            .ok()
            .expect("one original table");
        original
            .state
            .lock()
            .unwrap()
            .leases
            .append(&mut material.leases);
        router
            .register_original(execution, original.terminal_storage_resolver())
            .unwrap();
        let (later_execution, later) = terminal_stage(72);
        offer_terminal_scope(&later, "s3://warehouse/other", u64::MAX);
        let later_sink = later.sink();
        let weak_first = Arc::downgrade(&original);
        let weak_later = Arc::downgrade(&later);
        router
            .register_original(later_execution, later.terminal_storage_resolver())
            .unwrap();
        let first = router.freeze().unwrap_err();
        let late = later_sink
            .offer_vended_s3_credential_lease(
                &vended_catalog_properties(),
                vended_contribution("later-cannot-contribute"),
            )
            .unwrap_err();
        assert!(
            late.message()
                .contains("after attempt collector consumption")
        );
        assert!(
            later.drain_credential_leases().is_err(),
            "the later valid stage must not retain a Native transfer right"
        );
        let repeated = router.freeze().unwrap_err();
        assert_eq!(
            first, repeated,
            "the same cached original cause survives an idempotent table getter"
        );
        assert_eq!(first.failure_fact(), repeated.failure_fact());
        assert!(
            original.seal_for_terminal().is_ok(),
            "the original getter is now idempotent, not renewed validation"
        );
        let request = StorageAccessRequest::try_new(
            vended_catalog_properties().handle().clone(),
            "s3://warehouse/data/file",
        )
        .unwrap();
        assert_eq!(router.resolve_vended_s3(&request).unwrap_err(), first);
        assert!(
            original
                .sink()
                .offer_vended_s3_credential_lease(
                    &vended_catalog_properties(),
                    vended_contribution("late")
                )
                .is_err()
        );
        let last_alias = Arc::clone(&router);
        drop((original, later, router));
        assert!(weak_first.upgrade().is_some());
        assert!(weak_later.upgrade().is_some());
        drop(last_alias);
        assert!(weak_first.upgrade().is_none());
        assert!(weak_later.upgrade().is_none());
    }

    #[test]
    fn attempt_collector_deduplicates_scope_and_drains_once() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let properties = vended_catalog_properties();
        collector
            .offer_vended_s3_credential_lease(&properties, vended_contribution("first-canary"))
            .expect("first contribution");
        collector
            .offer_vended_s3_credential_lease(&properties, vended_contribution("second-canary"))
            .expect("duplicate scope is dropped without retaining its value");

        let leases = collector.drain_credential_leases().expect("one-time drain");
        assert_eq!(leases.leases().len(), 1);
        assert!(
            !leases.leases()[0].descriptor().refresh_capable(),
            "an endpoint-free contribution without a provider source is not refreshable"
        );
        assert!(collector.drain_credential_leases().is_err());
    }

    #[test]
    fn provider_sink_does_not_keep_a_cancelled_attempt_collector_alive() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let weak_collector = Arc::downgrade(&collector);
        let sink = collector.sink();
        let refresher_dropped = Arc::new(AtomicBool::new(false));
        let contribution = vended_contribution("accepted-before-cancel")
            .with_refresher(
                novarocks_spi::connector::CredentialAcquisitionOwnerId::new(),
                Arc::new(DropTrackedRefresher {
                    dropped: Arc::clone(&refresher_dropped),
                }),
            )
            .expect("drop-tracked provider refresher");
        sink.offer_vended_s3_credential_lease(&vended_catalog_properties(), contribution)
            .expect("credential is collected before cancellation");
        drop(collector);

        assert!(
            weak_collector.upgrade().is_none(),
            "a provider-held sink must not extend the attempt collector lifetime"
        );
        assert!(
            refresher_dropped.load(Ordering::SeqCst),
            "credential-owned provider state must be released with the cancelled attempt"
        );
        let error = sink
            .offer_vended_s3_credential_lease(
                &vended_catalog_properties(),
                vended_contribution("late-provider"),
            )
            .expect_err("late contribution fails after its attempt is gone");
        assert!(error.to_string().contains("no longer available"), "{error}");
    }

    #[test]
    fn provider_local_refresh_capability_does_not_require_a_public_endpoint() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let properties = vended_catalog_properties();
        let contribution = vended_contribution("load-table")
            .with_refresher(
                novarocks_spi::connector::CredentialAcquisitionOwnerId::new(),
                Arc::new(ProviderLocalRefresher),
            )
            .expect("provider-local refresher");
        collector
            .offer_vended_s3_credential_lease(&properties, contribution)
            .expect("provider-local contribution");

        let leases = collector.drain_credential_leases().expect("one-time drain");
        assert!(leases.leases()[0].descriptor().refresh_capable());
        assert!(leases.leases()[0].provider().is_some());
    }

    #[test]
    fn attempt_collector_resolves_only_before_lifecycle_handoff() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let properties = vended_catalog_properties();
        collector
            .offer_vended_s3_credential_lease(&properties, vended_contribution("resolver-canary"))
            .expect("contribution");
        let resolver = collector.storage_resolver();
        let request = StorageAccessRequest::try_new(
            properties.handle().clone(),
            "s3://warehouse/data/file.parquet",
        )
        .expect("storage request");
        let resolved = resolver
            .resolve_vended_s3(&request)
            .expect("collector resolves its current attempt scope");
        assert_eq!(resolved.matched_prefix().as_str(), "s3://warehouse/data");

        let leases = collector
            .drain_credential_leases()
            .expect("one-way lifecycle handoff");
        assert!(
            novarocks_spi::connector::ConnectorStorageResolver::resolve_vended_s3(
                collector.as_ref(),
                &request,
            )
            .is_err(),
            "the original planning collector refuses after its one-way drain"
        );
        assert!(resolver.resolve_vended_s3(&request).is_ok());
        drop(collector);
        assert!(resolver.resolve_vended_s3(&request).is_ok());
        drop(leases);
        assert!(resolver.resolve_vended_s3(&request).is_err());
    }

    #[test]
    fn terminal_custody_retains_original_material_when_planning_fails_before_native_drain() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let terminal = collector.terminal_storage_resolver();
        let weak_collector = Arc::downgrade(&collector);
        let sink = collector.sink();
        let properties = vended_catalog_properties();
        let failed_preparation = (|| {
            sink.offer_vended_s3_credential_lease(
                &properties,
                vended_contribution("pre-seal-failure"),
            )?;
            Err::<(), _>(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                "injected preparation failure after the original contribution",
            ))
        })();
        assert!(failed_preparation.is_err());
        let request = StorageAccessRequest::try_new(
            properties.handle().clone(),
            "s3://warehouse/data/file.parquet",
        )
        .unwrap();
        assert_eq!(
            terminal
                .resolve_vended_s3(&request)
                .unwrap()
                .matched_prefix()
                .as_str(),
            "s3://warehouse/data"
        );
        // The terminal owner was registered before the failed planning path
        // releases its collector. Cancellation is not an actual-exit receipt.
        drop(collector);
        assert!(weak_collector.upgrade().is_some());
        let frozen = terminal.seal_for_terminal().unwrap();
        let again = terminal.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(&frozen, &again));
        assert!(
            sink.offer_vended_s3_credential_lease(
                &properties,
                vended_contribution("late-after-failure")
            )
            .is_err()
        );
        let retained = weak_collector.upgrade().unwrap();
        assert!(retained.drain_credential_leases().is_err());
        assert!(retained.resolve_vended_s3(&request).is_err());
        assert!(terminal.resolve_vended_s3(&request).is_ok());
        let foreign_scope = StorageAccessRequest::try_new(
            properties.handle().clone(),
            "s3://warehouse/other/file.parquet",
        )
        .unwrap();
        assert!(terminal.resolve_vended_s3(&foreign_scope).is_err());
        let foreign_owner = StorageAccessRequest::try_new(
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("catalog.foreign").unwrap(),
                CatalogVersion::from_bytes([0x24; 32]),
            ),
            "s3://warehouse/data/file.parquet",
        )
        .unwrap();
        assert!(terminal.resolve_vended_s3(&foreign_owner).is_err());
        drop((retained, frozen, again, terminal));
        assert!(weak_collector.upgrade().is_none());
    }

    #[test]
    fn native_and_terminal_handoffs_share_one_frozen_arc_until_the_final_owner_exits() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let weak_collector = Arc::downgrade(&collector);
        let terminal = collector.terminal_storage_resolver();
        let route = collector.storage_resolver();
        let raw_route = Arc::clone(&collector.route);
        let dropped = Arc::new(AtomicBool::new(false));
        let properties = vended_catalog_properties();
        collector
            .offer_vended_s3_credential_lease(
                &properties,
                vended_contribution("native-same-original")
                    .with_refresher(
                        novarocks_spi::connector::CredentialAcquisitionOwnerId::new(),
                        Arc::new(DropTrackedRefresher {
                            dropped: Arc::clone(&dropped),
                        }),
                    )
                    .unwrap(),
            )
            .unwrap();
        let request = StorageAccessRequest::try_new(
            properties.handle().clone(),
            "s3://warehouse/data/file.parquet",
        )
        .unwrap();
        let leases = collector.drain_credential_leases().unwrap();
        let original = terminal.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(&leases.storage, &original));
        assert!(std::ptr::eq(
            leases.leases().as_ptr(),
            original.leases.as_ptr()
        ));
        assert!(collector.drain_credential_leases().is_err());
        assert!(collector.resolve_vended_s3(&request).is_err());
        assert!(!leases.leases().is_empty());
        let native = leases.into_storage();
        assert!(Arc::ptr_eq(&native, &original));
        let repeated = collector.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(&repeated, &native));
        let weak_storage = Arc::downgrade(&native);
        assert!(route.resolve_vended_s3(&request).is_ok());
        drop(collector);
        assert!(weak_collector.upgrade().is_some());
        drop(terminal);
        assert!(weak_collector.upgrade().is_none());
        assert!(route.resolve_vended_s3(&request).is_ok());
        drop((original, repeated));
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(route.resolve_vended_s3(&request).is_ok());
        drop(native);
        assert!(weak_storage.upgrade().is_none());
        assert!(dropped.load(Ordering::SeqCst));
        assert!(route.resolve_vended_s3(&request).is_err());
        assert!(matches!(
            &*raw_route.owner.lock().unwrap(),
            super::AttemptCredentialLeaseStorageRouteOwner::Revoked
        ));
    }

    #[test]
    fn sealed_empty_is_idempotent_and_never_reopens_collection_or_native_startup() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let terminal = collector.terminal_storage_resolver();
        let first = terminal.seal_for_terminal().unwrap();
        let second = collector.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(first.leases.is_empty());
        assert!(collector.drain_credential_leases().is_err());
        assert!(
            collector
                .sink()
                .offer_vended_s3_credential_lease(
                    &vended_catalog_properties(),
                    vended_contribution("late-empty"),
                )
                .is_err()
        );
        let native_collector = AttemptCredentialLeaseCollector::new(execution_id());
        let native = native_collector.drain_credential_leases().unwrap();
        let frozen_empty = native_collector.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(&native.storage, &frozen_empty));
        let native = native.into_storage();
        assert!(Arc::ptr_eq(&native, &frozen_empty));
        assert!(native.leases.is_empty());
        drop(native);
        assert!(native_collector.drain_credential_leases().is_err());
        assert!(Arc::ptr_eq(
            &frozen_empty,
            &native_collector.seal_for_terminal().unwrap()
        ));
    }

    #[test]
    fn native_storage_verification_retains_exact_phase_and_arc_including_empty() {
        for has_material in [false, true] {
            let collector = AttemptCredentialLeaseCollector::new(execution_id());
            let terminal = collector.terminal_storage_resolver();
            let unrelated_empty = super::QueryCredentialLeases::empty().into_storage();
            assert!(
                terminal
                    .verify_native_storage(execution_id(), &unrelated_empty)
                    .is_err()
            );
            assert!(
                collector.frozen.get().is_none(),
                "verification cannot seal the collector"
            );
            if has_material {
                collector
                    .offer_vended_s3_credential_lease(
                        &vended_catalog_properties(),
                        vended_contribution("original-native"),
                    )
                    .unwrap();
            }
            let original = collector.drain_credential_leases().unwrap().into_storage();
            assert!(Arc::ptr_eq(collector.frozen.get().unwrap(), &original));
            assert!(
                terminal
                    .verify_native_storage(execution_id(), &original)
                    .is_ok()
            );
            assert!(
                terminal
                    .verify_native_storage(execution_id(), &unrelated_empty)
                    .is_err()
            );
            let other_execution = QueryExecutionId::new(
                execution_id().query_id(),
                novarocks_types::AttemptId::new(execution_id().attempt_id().get() + 1).unwrap(),
            )
            .unwrap();
            assert!(
                terminal
                    .verify_native_storage(other_execution, &original)
                    .is_err()
            );
            let other_collector = AttemptCredentialLeaseCollector::new(execution_id());
            let foreign = other_collector
                .drain_credential_leases()
                .unwrap()
                .into_storage();
            assert!(
                terminal
                    .verify_native_storage(execution_id(), &foreign)
                    .is_err()
            );

            let terminal_only = AttemptCredentialLeaseCollector::new(execution_id());
            if has_material {
                terminal_only
                    .offer_vended_s3_credential_lease(
                        &vended_catalog_properties(),
                        vended_contribution("original-terminal"),
                    )
                    .unwrap();
            }
            let terminal = terminal_only.terminal_storage_resolver();
            let sealed = terminal.seal_for_terminal().unwrap();
            assert!(Arc::ptr_eq(terminal_only.frozen.get().unwrap(), &sealed));
            assert!(
                terminal
                    .verify_native_storage(execution_id(), &sealed)
                    .is_err()
            );
            assert!(terminal_only.drain_credential_leases().is_err());
        }
    }

    #[test]
    fn dropping_a_terminal_wrapper_does_not_retire_collection_or_native_startup() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let terminal = collector.terminal_storage_resolver();
        drop(terminal);
        assert!(collector.frozen.get().is_none());
        collector
            .offer_vended_s3_credential_lease(
                &vended_catalog_properties(),
                vended_contribution("collect-after-wrapper-drop"),
            )
            .unwrap();
        assert_eq!(
            collector.drain_credential_leases().unwrap().leases().len(),
            1
        );
        assert!(collector.drain_credential_leases().is_err());
    }

    #[test]
    fn original_planning_route_projection_survives_sealing_before_its_resolve() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let properties = vended_catalog_properties();
        collector
            .offer_vended_s3_credential_lease(&properties, vended_contribution("route-cut"))
            .unwrap();
        let request = StorageAccessRequest::try_new(
            properties.handle().clone(),
            "s3://warehouse/data/file.parquet",
        )
        .unwrap();
        // Capture exactly what a concurrent resolve upgraded before seal.
        // The real route releases its lock before invoking this projection.
        let selected = collector.route.current_owner().unwrap();
        let leases = collector.drain_credential_leases().unwrap();
        assert!(collector.resolve_vended_s3(&request).is_err());
        assert!(selected.resolve(&request).is_ok());
        assert!(!leases.leases().is_empty());
        let original = leases.into_storage();
        let route = collector.storage_resolver();
        drop(collector);
        assert!(route.resolve_vended_s3(&request).is_ok());
        drop(original);
        assert!(route.resolve_vended_s3(&request).is_err());
    }

    #[test]
    fn malformed_native_seal_retires_collection_but_keeps_original_terminal_material() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let duplicate = AttemptCredentialLeaseCollector::new(execution_id());
        let properties = vended_catalog_properties();
        for original in [&collector, &duplicate] {
            original
                .offer_vended_s3_credential_lease(&properties, vended_contribution("duplicate-id"))
                .unwrap();
        }
        let material = duplicate.drain_credential_leases().unwrap();
        drop(duplicate);
        let mut material = Arc::try_unwrap(material.storage)
            .ok()
            .expect("one original test table");
        collector
            .state
            .lock()
            .unwrap()
            .leases
            .append(&mut material.leases);
        assert!(collector.drain_credential_leases().is_err());
        assert!(collector.drain_credential_leases().is_err());
        assert!(
            collector
                .sink()
                .offer_vended_s3_credential_lease(&properties, vended_contribution("late-invalid"))
                .is_err()
        );
        let frozen = collector.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(
            &frozen,
            &collector.seal_for_terminal().unwrap()
        ));
        let request = StorageAccessRequest::try_new(
            properties.handle().clone(),
            "s3://warehouse/data/file.parquet",
        )
        .unwrap();
        assert!(
            collector
                .terminal_storage_resolver()
                .resolve_vended_s3(&request)
                .is_ok()
        );
        assert!(collector.resolve_vended_s3(&request).is_err());
    }

    #[test]
    fn terminal_resolver_has_no_drained_gap_during_concurrent_native_sealing() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let properties = vended_catalog_properties();
        collector
            .offer_vended_s3_credential_lease(&properties, vended_contribution("sealing-race"))
            .unwrap();
        let terminal = collector.terminal_storage_resolver();
        let request = StorageAccessRequest::try_new(
            properties.handle().clone(),
            "s3://warehouse/data/file.parquet",
        )
        .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let resolving = {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..1024 {
                    assert!(terminal.resolve_vended_s3(&request).is_ok());
                }
            })
        };
        barrier.wait();
        let native = collector.drain_credential_leases().unwrap();
        resolving
            .join()
            .expect("the original resolver thread actually exits");
        let frozen = collector.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(&native.storage, &frozen));
    }

    /// The control leases a query froze drain exactly once, when the attempt
    /// that holds them lets go.
    ///
    /// The attempt owns the lease directly, so the drain point is the lease's
    /// own drop rather than a separate termination call. That is what keeps a
    /// planning runtime alive for as long as, and no longer than, the attempt
    /// that froze artifacts from it.
    #[test]
    fn query_catalog_lease_drains_its_control_leases_exactly_once_when_dropped() {
        let releases = Arc::new(AtomicUsize::new(0));
        let release_counter = Arc::clone(&releases);
        let planning_lease = ConnectorControlPlanningLease::new(
            crate::connector::scan_model::planned_files_fixture_binding(
                "catalog.lease",
                HashMap::new(),
                None,
            )
            .into(),
            move || {
                release_counter.fetch_add(1, Ordering::SeqCst);
            },
        );
        let catalog_lease = QueryCatalogLease::new(catalog_set(), vec![planning_lease]);
        assert_eq!(catalog_lease.control_lease_count(), 1);
        assert_eq!(releases.load(Ordering::SeqCst), 0);

        drop(catalog_lease);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn query_init_options_reject_other_island_target_before_manifest_construction() {
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let other_island = LiveBackendTarget::new(
            0,
            BackendProcessDescriptor::try_new(
                BackendProcessId::new_v7(),
                RuntimeEndpoint::new("127.0.0.1", 19040).expect("valid endpoint"),
                RuntimeEndpoint::new("control-0.test.invalid", 19061)
                    .expect("valid control endpoint"),
                "test-deployment",
                "different-build",
                novarocks_types::NativeCompatibilityId::new([0x72; 32]),
                4096,
            )
            .expect("valid descriptor"),
            novarocks_execution::task_execution::AdmissionEpochCapability::try_from_bytes(
                [0x61; 16],
            )
            .expect("nonzero epoch"),
        );

        let error = match QueryInitOptions::new(
            execution_id(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            vec![other_island],
            &resolved,
            wire_query_options(),
        ) {
            Ok(_) => panic!("other-island target must not produce a participant manifest"),
            Err(error) => error,
        };

        assert!(
            error.message().contains("another compatibility island"),
            "{error}"
        );
    }
}
