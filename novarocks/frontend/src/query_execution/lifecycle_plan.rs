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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::common::backend_topology::{CoordinatorReportEndpoint, LiveBackendTarget};
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, ResolvedQueryOptions,
};
use crate::query_execution::schedule::FragmentLifecycleProjection;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_proto_codec::catalog::CatalogSet;
use novarocks_proto_codec::lifecycle::{
    CredentialLeaseSecretEnvelope, ParticipantAttemptRef, ParticipantBackendIdentity,
    ParticipantManifest, ParticipantManifestDigest, QueryControlEndpoint, QueryExecutionId,
    QueryOptions as ProtocolQueryOptions, RuntimeFilterContribution,
};
use novarocks_proto_models::common;
use novarocks_proto_models::novarocks;
use novarocks_spi::connector::{
    CatalogCredentialMode, CatalogCredentialPurpose, CatalogNonSecretProperty, CatalogProperties,
    CatalogProviderKind, CatalogStorageAccessDomainInput, ConnectorControlPlanningLease,
    ConnectorError, ConnectorErrorKind, ConnectorProviderId, ConnectorStorageResolver,
    ConnectorVendedCredentialLeaseSink, ConnectorVendedS3CredentialLeaseRefresher,
    CredentialConsumerRole, CredentialLeaseDescriptor, CredentialLeaseId, CredentialLeaseProvider,
    ResolvedVendedS3Access, StorageAccessRequest, StorageCredentialScopePrefix,
    VendedS3CredentialLeaseContribution,
};
use novarocks_types::BackendProcessId;
use novarocks_types::NativeCompatibilityId;
use sha2::{Digest, Sha256};

use crate::query_execution::launch::StageParticipantBinding;
use crate::query_execution::terminal_set::QueryTerminalSet;

const ATTEMPT_CREDENTIAL_LEASE_ID_DOMAIN: &[u8] = b"novarocks.attempt-credential-lease-id.v1";

/// Secret-free indirection for one attempt's FE-local storage authority. It
/// follows the move-only lease owner from candidate planning into the admitted
/// lifecycle control, but never owns a secret envelope itself.
struct AttemptCredentialLeaseStorageRoute {
    owner: Mutex<AttemptCredentialLeaseStorageRouteOwner>,
}

enum AttemptCredentialLeaseStorageRouteOwner {
    Planning(Weak<AttemptCredentialLeaseCollector>),
    Admitted(Weak<dyn ConnectorStorageResolver>),
    Revoked,
}

impl AttemptCredentialLeaseStorageRoute {
    fn new_planning(owner: Weak<AttemptCredentialLeaseCollector>) -> Self {
        Self {
            owner: Mutex::new(AttemptCredentialLeaseStorageRouteOwner::Planning(owner)),
        }
    }

    fn adopt(&self, owner: Arc<dyn ConnectorStorageResolver>) {
        *self.owner.lock().expect("attempt credential storage route") =
            AttemptCredentialLeaseStorageRouteOwner::Admitted(Arc::downgrade(&owner));
    }

    fn revoke(&self) {
        *self.owner.lock().expect("attempt credential storage route") =
            AttemptCredentialLeaseStorageRouteOwner::Revoked;
    }
}

impl ConnectorStorageResolver for AttemptCredentialLeaseStorageRoute {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        let owner = {
            let owner = self.owner.lock().expect("attempt credential storage route");
            match &*owner {
                AttemptCredentialLeaseStorageRouteOwner::Planning(owner) => owner
                    .upgrade()
                    .map(|owner| Arc::clone(&owner) as Arc<dyn ConnectorStorageResolver>),
                AttemptCredentialLeaseStorageRouteOwner::Admitted(owner) => owner.upgrade(),
                AttemptCredentialLeaseStorageRouteOwner::Revoked => None,
            }
        };
        owner
            .ok_or_else(vended_storage_access_denied)?
            .resolve_vended_s3(request)
    }
}

/// Move-only FE collection state for credentials obtained during one candidate
/// attempt's metadata observation. The SPI sees only the sink trait; table,
/// plan, cache, and Core request types cannot recover these values.
pub(crate) struct AttemptCredentialLeaseCollector {
    execution_id: QueryExecutionId,
    state: Mutex<AttemptCredentialLeaseCollectorState>,
    route: Arc<AttemptCredentialLeaseStorageRoute>,
}

struct AttemptCredentialLeaseCollectorState {
    scopes: BTreeSet<(
        novarocks_spi::connector::CatalogHandle,
        StorageCredentialScopePrefix,
    )>,
    leases: Vec<QueryCredentialLease>,
    drained: bool,
}

impl AttemptCredentialLeaseCollector {
    pub(crate) fn new(execution_id: QueryExecutionId) -> Arc<Self> {
        Arc::new_cyclic(|collector| Self {
            execution_id,
            state: Mutex::new(AttemptCredentialLeaseCollectorState {
                scopes: BTreeSet::new(),
                leases: Vec::new(),
                drained: false,
            }),
            route: Arc::new(AttemptCredentialLeaseStorageRoute::new_planning(
                collector.clone(),
            )),
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

    pub(crate) fn into_credential_leases(
        &self,
    ) -> Result<QueryCredentialLeases, DistributedQueryError> {
        let mut state = self
            .state
            .lock()
            .expect("attempt credential collector lock");
        if state.drained {
            return Err(contract_error(
                "attempt credential lease collector was consumed more than once",
            ));
        }
        state.drained = true;
        state.scopes.clear();
        QueryCredentialLeases::try_new_with_storage_route(
            std::mem::take(&mut state.leases),
            Arc::clone(&self.route),
        )
    }

    pub(crate) fn sink(self: &Arc<Self>) -> Arc<dyn ConnectorVendedCredentialLeaseSink> {
        Arc::clone(self) as Arc<dyn ConnectorVendedCredentialLeaseSink>
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
        if state.drained {
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
                    && binding.consumer_role() == CredentialConsumerRole::FrontendAndBackend
                    && matches!(binding.mode(), CatalogCredentialMode::Vended)
            })
            .cloned()
            .ok_or_else(|| collector_error("catalog has no vended object-store data binding"))?;
        if catalog_properties.provider_kind() != CatalogProviderKind::Iceberg {
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
        let (entries, refresh_endpoint, provider_refresher) =
            contribution.into_parts_with_refresher();
        if refresh_endpoint.is_some() != provider_refresher.is_some() {
            return Err(collector_error(
                "vended S3 credential refresh endpoint and provider source differ",
            ));
        }
        let mut state = self
            .state
            .lock()
            .expect("attempt credential collector lock");
        if state.drained {
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
            let refresh_capable = refresh_endpoint.is_some();
            let descriptor = CredentialLeaseDescriptor::try_new(
                lease_id,
                1,
                owner,
                CredentialLeaseProvider::S3,
                vec![prefix],
                not_after_unix_ms,
                refresh_capable,
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
            let refresher = provider_refresher.as_ref().map(|provider| {
                Arc::new(ProviderVendedS3LeaseRefresher {
                    provider: Arc::clone(provider),
                }) as Arc<dyn QueryCredentialLeaseRefresher>
            });
            state.leases.push(
                QueryCredentialLease::try_new(descriptor, envelope, refresher)
                    .map_err(|error| collector_error(error.message()))?,
            );
        }
        Ok(())
    }
}

/// Adapts a provider-owned, FE-local credential source to this attempt's
/// immutable lease identity. The provider returns one complete refreshed
/// response; this adapter consumes only the exact existing prefix and rejects
/// any scope drift before forming the next confidential epoch.
struct ProviderVendedS3LeaseRefresher {
    provider: Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>,
}

impl QueryCredentialLeaseRefresher for ProviderVendedS3LeaseRefresher {
    fn refresh(
        &self,
        current: &CredentialLeaseDescriptor,
    ) -> Result<QueryCredentialLeaseRefresh, String> {
        if current.provider() != CredentialLeaseProvider::S3 || current.prefixes().len() != 1 {
            return Err("vended S3 credential refresh has an invalid existing scope".to_string());
        }
        let target_prefix = &current.prefixes()[0];
        let entry = self
            .provider
            .refresh_vended_s3_credentials()
            .map_err(|_| "provider vended S3 credential refresh failed".to_string())?
            .into_entries()
            .into_iter()
            .find(|entry| entry.prefix() == target_prefix)
            .ok_or_else(|| {
                "provider vended S3 credential refresh changed prefix scope".to_string()
            })?;
        let (prefix, not_after_unix_ms, access_key_id, secret_access_key, session_token) =
            entry.into_parts();
        let epoch = current
            .epoch()
            .checked_add(1)
            .ok_or_else(|| "vended S3 credential lease epoch overflow".to_string())?;
        let descriptor = CredentialLeaseDescriptor::try_new(
            current.lease_id(),
            epoch,
            current.owner().clone(),
            CredentialLeaseProvider::S3,
            vec![prefix],
            not_after_unix_ms,
            true,
            current.storage_access_domain_id(),
        )
        .map_err(|_| "build refreshed vended S3 credential descriptor failed".to_string())?;
        let envelope = CredentialLeaseSecretEnvelope::try_new(
            current.lease_id(),
            epoch,
            access_key_id,
            secret_access_key,
            session_token,
            not_after_unix_ms,
        )
        .map_err(|_| "build refreshed vended S3 credential envelope failed".to_string())?;
        QueryCredentialLeaseRefresh::try_new(descriptor, envelope)
            .map_err(|_| "refreshed vended S3 credential lease violates its contract".to_string())
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

/// Frozen target selected from one live backend snapshot.
///
/// This is coordinator orchestration state, not a native lifecycle message.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryLifecycleTarget {
    backend_idx: usize,
    endpoint: RuntimeEndpoint,
    process_id: BackendProcessId,
}

impl QueryLifecycleTarget {
    pub fn new(
        backend_idx: usize,
        endpoint: RuntimeEndpoint,
        process_id: BackendProcessId,
    ) -> Self {
        Self {
            backend_idx,
            endpoint,
            process_id,
        }
    }

    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }

    pub const fn process_id(&self) -> BackendProcessId {
        self.process_id
    }
}

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

fn protocol_contract_error(error: novarocks_proto_codec::ProtocolError) -> DistributedQueryError {
    contract_error(error.to_string())
}

fn protocol_report_endpoint(
    endpoint: CoordinatorReportEndpoint,
) -> Result<QueryControlEndpoint, DistributedQueryError> {
    let endpoint = endpoint.into_runtime_endpoint();
    let port = u32::try_from(endpoint.port())
        .map_err(|_| contract_error("report endpoint port is outside u32 range"))?;
    QueryControlEndpoint::parse(novarocks::QueryControlEndpoint {
        host: endpoint.host().to_string(),
        port,
    })
    .map_err(protocol_contract_error)
}

fn protocol_backend_identity(
    target: LiveBackendTarget,
) -> Result<ParticipantBackendIdentity, DistributedQueryError> {
    let endpoint = target.endpoint().map_err(protocol_contract_error)?;
    let process_id = target.process_id().map_err(protocol_contract_error)?;
    ParticipantBackendIdentity::parse(novarocks::ParticipantBackendIdentity {
        endpoint: Some(novarocks::QueryControlEndpoint {
            host: endpoint.host().to_string(),
            port: u32::try_from(endpoint.port())
                .map_err(|_| contract_error("backend endpoint port is outside u32 range"))?,
        }),
        process_id: Some(novarocks::BackendProcessId {
            value: process_id.to_bytes().to_vec(),
        }),
    })
    .map_err(protocol_contract_error)
}

fn protocol_unique_id(id: novarocks_types::UniqueId) -> common::UniqueId {
    common::UniqueId {
        hi: id.high(),
        lo: id.low(),
    }
}

fn protocol_exchange_route(
    route: &novarocks_proto_codec::lifecycle::ExchangeRouteManifest,
) -> novarocks::ExchangeRouteManifest {
    *route.as_proto()
}

fn duration_millis(duration: Duration) -> Result<u64, DistributedQueryError> {
    duration.as_millis().try_into().map_err(|_| {
        contract_error("query initialization pre-start timeout must fit in u64 milliseconds")
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct QueryInitPlanHeader {
    execution_id: QueryExecutionId,
    query_deadline_unix_ms: u64,
}

/// FE-owned source for one already-admitted vended credential refresh.
///
/// The source is intentionally attempt-local and is never represented on the
/// native wire.  In particular, a Backend cannot recover it after a stream
/// loss and cannot use it to mint a credential independently.
pub(crate) trait QueryCredentialLeaseRefresher: Send + Sync + 'static {
    fn refresh(
        &self,
        current: &CredentialLeaseDescriptor,
    ) -> Result<QueryCredentialLeaseRefresh, String>;
}

/// A next epoch obtained by the FE from the provider-private refresh source.
/// The secret envelope has a redacted Debug implementation in the codec and
/// therefore must not be exposed by this type either.
pub(crate) struct QueryCredentialLeaseRefresh {
    descriptor: CredentialLeaseDescriptor,
    envelope: CredentialLeaseSecretEnvelope,
}

impl QueryCredentialLeaseRefresh {
    pub(crate) fn try_new(
        descriptor: CredentialLeaseDescriptor,
        envelope: CredentialLeaseSecretEnvelope,
    ) -> Result<Self, DistributedQueryError> {
        if !envelope.matches_descriptor(&descriptor) {
            return Err(contract_error(
                "query credential lease refresh envelope does not match descriptor",
            ));
        }
        Ok(Self {
            descriptor,
            envelope,
        })
    }

    pub(crate) const fn descriptor(&self) -> &CredentialLeaseDescriptor {
        &self.descriptor
    }

    pub(crate) const fn envelope(&self) -> &CredentialLeaseSecretEnvelope {
        &self.envelope
    }
}

/// One query-attempt lease frozen before any participant Init is dispatched.
/// Values remain FE-local until `materialize` forms the TLS-only Init side
/// channel; manifests receive only the descriptor.
pub(crate) struct QueryCredentialLease {
    descriptor: CredentialLeaseDescriptor,
    envelope: CredentialLeaseSecretEnvelope,
    refresher: Option<Arc<dyn QueryCredentialLeaseRefresher>>,
}

impl QueryCredentialLease {
    pub(crate) fn try_new(
        descriptor: CredentialLeaseDescriptor,
        envelope: CredentialLeaseSecretEnvelope,
        refresher: Option<Arc<dyn QueryCredentialLeaseRefresher>>,
    ) -> Result<Self, DistributedQueryError> {
        if !envelope.matches_descriptor(&descriptor) {
            return Err(contract_error(
                "query credential lease initial envelope does not match descriptor",
            ));
        }
        if descriptor.refresh_capable() != refresher.is_some() {
            return Err(contract_error(
                "query credential lease refresh capability and FE refresher differ",
            ));
        }
        Ok(Self {
            descriptor,
            envelope,
            refresher,
        })
    }

    pub(crate) const fn descriptor(&self) -> &CredentialLeaseDescriptor {
        &self.descriptor
    }

    pub(crate) const fn envelope(&self) -> &CredentialLeaseSecretEnvelope {
        &self.envelope
    }

    pub(crate) fn refresher(&self) -> Option<&Arc<dyn QueryCredentialLeaseRefresher>> {
        self.refresher.as_ref()
    }
}

/// Canonical secret-bearing lease contribution retained by the FE attempt.
/// It is deliberately not `Debug` or `Clone`: retries reuse this exact
/// in-memory material and code outside the lifecycle owner cannot copy it.
pub(crate) struct QueryCredentialLeases {
    leases: Vec<QueryCredentialLease>,
    storage_route: Option<Arc<AttemptCredentialLeaseStorageRoute>>,
}

impl QueryCredentialLeases {
    pub(crate) fn empty() -> Self {
        Self {
            leases: Vec::new(),
            storage_route: None,
        }
    }

    pub(crate) fn try_new(
        mut leases: Vec<QueryCredentialLease>,
    ) -> Result<Self, DistributedQueryError> {
        leases.sort_by_key(|lease| lease.descriptor().lease_id());
        if leases
            .windows(2)
            .any(|pair| pair[0].descriptor().lease_id() == pair[1].descriptor().lease_id())
        {
            return Err(contract_error(
                "query credential lease contribution repeats a lease id",
            ));
        }
        Ok(Self {
            leases,
            storage_route: None,
        })
    }

    fn try_new_with_storage_route(
        leases: Vec<QueryCredentialLease>,
        storage_route: Arc<AttemptCredentialLeaseStorageRoute>,
    ) -> Result<Self, DistributedQueryError> {
        let mut leases = Self::try_new(leases)?;
        leases.storage_route = Some(storage_route);
        Ok(leases)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.leases.is_empty()
    }

    pub(crate) fn descriptors(&self) -> impl Iterator<Item = &CredentialLeaseDescriptor> {
        self.leases.iter().map(QueryCredentialLease::descriptor)
    }

    pub(crate) fn leases(&self) -> &[QueryCredentialLease] {
        &self.leases
    }

    pub(crate) fn lease(&self, id: CredentialLeaseId) -> Option<&QueryCredentialLease> {
        self.leases
            .binary_search_by_key(&id, |lease| lease.descriptor().lease_id())
            .ok()
            .map(|index| &self.leases[index])
    }

    pub(crate) fn refreshable(&self) -> Vec<(CredentialLeaseId, u64)> {
        self.leases
            .iter()
            .filter(|lease| lease.refresher().is_some())
            .map(|lease| {
                (
                    lease.descriptor().lease_id(),
                    lease.descriptor().not_after_unix_ms(),
                )
            })
            .collect()
    }

    pub(crate) fn replace(
        &mut self,
        descriptor: CredentialLeaseDescriptor,
        envelope: CredentialLeaseSecretEnvelope,
    ) -> Result<(), DistributedQueryError> {
        let id = descriptor.lease_id();
        let index = self
            .leases
            .binary_search_by_key(&id, |lease| lease.descriptor().lease_id())
            .map_err(|_| {
                contract_error("query credential lease refresh references an unknown lease")
            })?;
        let lease = &mut self.leases[index];
        if !lease.descriptor.has_same_refresh_scope(&descriptor)
            || descriptor.epoch() <= lease.descriptor.epoch()
            || !envelope.matches_descriptor(&descriptor)
        {
            return Err(contract_error(
                "query credential lease refresh changed immutable scope or epoch",
            ));
        }
        lease.descriptor = descriptor;
        lease.envelope = envelope;
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.leases.clear();
    }

    fn storage_route(&self) -> Option<Arc<AttemptCredentialLeaseStorageRoute>> {
        self.storage_route.clone()
    }

    pub(crate) fn adopt_storage_resolver(&self, owner: Arc<dyn ConnectorStorageResolver>) {
        if let Some(storage_route) = &self.storage_route {
            storage_route.adopt(owner);
        }
    }

    pub(crate) fn revoke_storage_resolver(&self) {
        if let Some(storage_route) = &self.storage_route {
            storage_route.revoke();
        }
    }

    /// Turns this attempt's frozen table into the storage capability its own
    /// commit reads through.
    ///
    /// The table is moved because the caller must be finished contributing it:
    /// on the task protocol the establish has already copied the material into
    /// wire form, and a second owner of the same secrets would be a second
    /// place it could outlive the attempt.
    ///
    /// The route connectors were handed during planning is re-pointed at the
    /// returned owner, so one capability serves both that route and the
    /// frontend's terminal commit. The route holds it weakly, so the caller
    /// must keep the returned value alive for as long as either may resolve.
    pub(crate) fn into_attempt_storage_resolver(self) -> Option<Arc<AttemptCredentialStorage>> {
        if self.leases.is_empty() {
            return None;
        }
        let owner = Arc::new(AttemptCredentialStorage {
            leases: self.leases,
            route: self.storage_route.clone(),
        });
        if let Some(route) = &self.storage_route {
            route.adopt(Arc::clone(&owner) as Arc<dyn ConnectorStorageResolver>);
        }
        Some(owner)
    }
}

/// One attempt's frozen credential table, as a storage resolver.
///
/// It is the task protocol's counterpart of the old lifecycle's terminal
/// credential capability. There is no attempt state to gate on here because
/// there is none to consult: the frontend holds the table itself, and the call
/// site that hands this to a write session is the one that already proved the
/// attempt completed.
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
    /// Revokes the planning route rather than leaving it pointing at a dead
    /// weak reference: a revoked route denies access, an expired one would
    /// deny it with a message about the wrong thing.
    fn drop(&mut self) {
        if let Some(route) = &self.route {
            route.revoke();
        }
    }
}

pub(crate) fn resolve_vended_s3_access(
    leases: &[QueryCredentialLease],
    request: &StorageAccessRequest,
) -> Result<ResolvedVendedS3Access, ConnectorError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let mut selected: Option<(&QueryCredentialLease, &StorageCredentialScopePrefix)> = None;
    for lease in leases {
        let descriptor = lease.descriptor();
        if descriptor.provider() != CredentialLeaseProvider::S3
            || descriptor.owner() != request.owner()
            || lease.envelope().session_token_expires_at_unix_ms() <= now
        {
            continue;
        }
        for prefix in descriptor.prefixes() {
            if !request.location().starts_with(prefix.as_str()) {
                continue;
            }
            if selected.is_none_or(|(_, current)| prefix.as_str().len() > current.as_str().len()) {
                selected = Some((lease, prefix));
            }
        }
    }
    let (lease, matched_prefix) = selected.ok_or_else(vended_storage_access_denied)?;
    Ok(ResolvedVendedS3Access::new(
        lease.descriptor().storage_access_domain_id(),
        lease.descriptor().lease_id(),
        lease.envelope().epoch(),
        matched_prefix.clone(),
        lease.envelope().session_token_expires_at_unix_ms(),
        lease.envelope().access_key_id().clone(),
        lease.envelope().secret_access_key().clone(),
        lease.envelope().session_token().clone(),
    ))
}

fn vended_storage_access_denied() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "vended storage access is unavailable for this query attempt",
    )
}

impl QueryInitPlanHeader {
    const fn new(execution_id: QueryExecutionId, query_deadline_unix_ms: u64) -> Self {
        Self {
            execution_id,
            query_deadline_unix_ms,
        }
    }

    pub(crate) const fn execution_id(self) -> QueryExecutionId {
        self.execution_id
    }
}

pub struct QueryInitOptions {
    execution_id: QueryExecutionId,
    native_compatibility_id: NativeCompatibilityId,
    live_backends: Vec<LiveBackendTarget>,
    /// Execution-owned options retained solely for sealed native fragment
    /// submission. They are not the lifecycle wire carrier.
    native_submission_options: QueryOptions,
    query_options: ProtocolQueryOptions,
    query_deadline_unix_ms: u64,
    pre_start_timeout: Duration,
    report_endpoint: QueryControlEndpoint,
    catalog_set: CatalogSet,
    credential_leases: QueryCredentialLeases,
}

impl QueryInitOptions {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        execution_id: QueryExecutionId,
        native_compatibility_id: NativeCompatibilityId,
        live_backends: Vec<LiveBackendTarget>,
        native_submission_options: &ResolvedQueryOptions,
        query_options: ProtocolQueryOptions,
        query_deadline_unix_ms: u64,
        pre_start_timeout: Duration,
        report_endpoint: CoordinatorReportEndpoint,
    ) -> Result<Self, DistributedQueryError> {
        if live_backends.is_empty() {
            return Err(contract_error(
                "query initialization requires at least one live backend",
            ));
        }
        if query_deadline_unix_ms == 0 {
            return Err(contract_error(
                "query initialization deadline must be nonzero",
            ));
        }
        if pre_start_timeout.is_zero() {
            return Err(contract_error(
                "query initialization pre-start timeout must be nonzero",
            ));
        }
        let mut backend_indices = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for target in &live_backends {
            target.process_id().map_err(protocol_contract_error)?;
            let backend_compatibility_id = target
                .descriptor()
                .native_compatibility_id()
                .map_err(protocol_contract_error)?;
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
            let endpoint = target.endpoint().map_err(protocol_contract_error)?;
            if !endpoints.insert(endpoint.clone()) {
                return Err(contract_error(format!(
                    "query initialization live snapshot repeats endpoint {}",
                    endpoint
                )));
            }
        }
        let report_endpoint = protocol_report_endpoint(report_endpoint).map_err(|error| {
            contract_error(format!(
                "query initialization report endpoint is invalid: {error}"
            ))
        })?;
        Ok(Self {
            execution_id,
            native_compatibility_id,
            live_backends,
            native_submission_options: native_submission_options.runtime_options().clone(),
            query_options,
            query_deadline_unix_ms,
            pre_start_timeout,
            report_endpoint,
            catalog_set: CatalogSet::new([]).expect("the empty catalog set is valid"),
            credential_leases: QueryCredentialLeases::empty(),
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn native_compatibility_id(&self) -> NativeCompatibilityId {
        self.native_compatibility_id
    }

    pub fn live_backends(&self) -> &[LiveBackendTarget] {
        &self.live_backends
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

pub struct QueryInitPlan {
    execution_id: QueryExecutionId,
    #[allow(
        dead_code,
        reason = "Retained for staged query-execution contract and lifecycle integration."
    )]
    query_deadline_unix_ms: u64,
    participants: Vec<QueryInitParticipant>,
    credential_leases: QueryCredentialLeases,
}

impl QueryInitPlan {
    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    #[allow(
        dead_code,
        reason = "Retained for staged query-execution contract and lifecycle integration."
    )]
    pub(crate) const fn query_deadline_unix_ms(&self) -> u64 {
        self.query_deadline_unix_ms
    }

    pub fn participant_count(&self) -> usize {
        self.participants.len()
    }

    pub fn backend_ids(&self) -> Vec<usize> {
        self.participants
            .iter()
            .map(QueryInitParticipant::backend_idx)
            .collect()
    }

    pub fn participant(&self, backend_idx: usize) -> Option<&QueryInitParticipant> {
        self.participants
            .iter()
            .find(|participant| participant.backend_idx() == backend_idx)
    }

    pub fn into_participants(self) -> Vec<QueryInitParticipant> {
        self.participants
    }

    pub(crate) fn into_parts(self) -> (Vec<QueryInitParticipant>, QueryCredentialLeases) {
        (self.participants, self.credential_leases)
    }

    /// Captures participant facts that must outlive consumption of the Init
    /// plan by the control-ready barrier. QLC-3 never re-resolves topology for
    /// Stage/Start after this point.
    pub fn stage_participant_bindings(
        &self,
    ) -> Result<Vec<StageParticipantBinding>, DistributedQueryError> {
        self.participants
            .iter()
            .map(|participant| {
                let endpoint = participant
                    .backend()
                    .endpoint()
                    .map_err(protocol_contract_error)?;
                let process_id = participant
                    .backend()
                    .process_id()
                    .map_err(protocol_contract_error)?;
                StageParticipantBinding::new(
                    QueryLifecycleTarget::new(
                        participant.backend_idx(),
                        RuntimeEndpoint::new(endpoint.host(), i32::from(endpoint.port()))
                            .map_err(|error| contract_error(error.to_string()))?,
                        process_id,
                    ),
                    ParticipantAttemptRef::new(self.execution_id, process_id)
                        .map_err(protocol_contract_error)?,
                    participant
                        .manifest()
                        .expected_fragment_instance_ids()
                        .into_iter()
                        .map(|id| novarocks_types::UniqueId::new(id.hi, id.lo)),
                )
                .map_err(|error| {
                    contract_error(format!(
                        "query stage participant {} is invalid: {error}",
                        participant.backend_idx()
                    ))
                })
            })
            .collect()
    }

    #[cfg(test)]
    pub fn from_manifests_for_contract_test(
        execution_id: QueryExecutionId,
        manifests: impl IntoIterator<Item = (usize, ParticipantManifest)>,
    ) -> Result<Self, DistributedQueryError> {
        let mut process_ids = BTreeSet::new();
        let mut participants = manifests
            .into_iter()
            .map(|(backend_idx, manifest)| {
                if manifest.execution_id().map_err(protocol_contract_error)? != execution_id {
                    return Err(contract_error(
                        "contract-test participant execution id differs from query init plan",
                    ));
                }
                let backend = manifest.backend().map_err(protocol_contract_error)?;
                let process_id = backend.process_id().map_err(protocol_contract_error)?;
                if !process_ids.insert(process_id) {
                    return Err(contract_error(
                        "contract-test query init plan repeats a backend process identity",
                    ));
                }
                let digest = manifest.digest().map_err(protocol_contract_error)?;
                Ok(QueryInitParticipant {
                    backend_idx,
                    backend,
                    manifest,
                    digest,
                })
            })
            .collect::<Result<Vec<_>, DistributedQueryError>>()?;
        participants.sort_by_key(QueryInitParticipant::backend_idx);
        if participants
            .windows(2)
            .any(|pair| pair[0].backend_idx() == pair[1].backend_idx())
        {
            return Err(contract_error(
                "contract-test query init plan repeats a backend index",
            ));
        }
        if participants.is_empty() {
            return Err(contract_error(
                "contract-test query init plan requires a participant",
            ));
        }
        Ok(Self {
            execution_id,
            query_deadline_unix_ms: participants[0].manifest().query_deadline_unix_ms(),
            participants,
            credential_leases: QueryCredentialLeases::empty(),
        })
    }
}

pub struct QueryInitParticipant {
    backend_idx: usize,
    backend: ParticipantBackendIdentity,
    manifest: ParticipantManifest,
    digest: ParticipantManifestDigest,
}

impl QueryInitParticipant {
    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn backend(&self) -> &ParticipantBackendIdentity {
        &self.backend
    }

    pub const fn manifest(&self) -> &ParticipantManifest {
        &self.manifest
    }

    pub const fn digest(&self) -> ParticipantManifestDigest {
        self.digest
    }

    pub fn into_parts(
        self,
    ) -> (
        usize,
        ParticipantBackendIdentity,
        ParticipantManifest,
        ParticipantManifestDigest,
    ) {
        (self.backend_idx, self.backend, self.manifest, self.digest)
    }
}

pub trait QueryInitBarrier: Send + Sync + 'static {
    fn initialize_all(
        &self,
        plan: QueryInitPlan,
    ) -> Result<QueryLifecycleLease, DistributedQueryError>;
}

/// The fail-closed result of aborting an attempt that had already entered
/// Running.  The original error is never replaced by terminal delivery
/// cleanup; a completed terminal set is supplemental evidence only.
#[derive(Clone, Debug)]
pub struct QueryLifecycleAbortOutcome {
    primary_error: String,
    terminal_set: Option<QueryTerminalSet>,
}

impl QueryLifecycleAbortOutcome {
    pub fn new(primary_error: impl Into<String>, terminal_set: Option<QueryTerminalSet>) -> Self {
        Self {
            primary_error: primary_error.into(),
            terminal_set,
        }
    }

    pub fn primary_error(&self) -> &str {
        &self.primary_error
    }

    pub fn terminal_set(&self) -> Option<&QueryTerminalSet> {
        self.terminal_set.as_ref()
    }

    pub fn into_primary_error(self) -> String {
        self.primary_error
    }
}

pub trait QueryLifecycleLeaseGuard: Send + 'static {
    fn finalize(self: Box<Self>) -> Result<QueryTerminalSet, DistributedQueryError>;

    /// Returns a FE-local, terminal-only storage capability before finalization
    /// consumes the lifecycle guard. The capability may outlive a successful
    /// native terminal only while a connector write still needs its exact
    /// commit or reconciliation decision; its owner revokes the underlying
    /// lease material when the capability is dropped.
    fn retain_terminal_storage_resolver(&self) -> Option<Arc<dyn ConnectorStorageResolver>> {
        None
    }

    fn abort_preserving(self: Box<Self>, primary_error: String) -> QueryLifecycleAbortOutcome;
}

/// FE-local ownership retained for every catalog dependency frozen into one
/// query Init contribution.  The catalog set is immutable once this lease is
/// constructed; the control leases keep the exact FE runtimes that produced
/// those artifacts alive until the attempt reaches a terminal path.
///
/// This is deliberately attached to [`QueryLifecycleLease`] instead of a
/// transport or fragment carrier.  Catalog materialization is already frozen
/// in Init, while control-runtime lifetime remains a Frontend-local concern.
pub(crate) struct QueryCatalogLease {
    catalog_set: CatalogSet,
    #[allow(
        dead_code,
        reason = "Drop retains these exact FE control leases until lifecycle termination."
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

#[must_use = "query lifecycle must be finalized or aborted"]
pub struct QueryLifecycleLease {
    guard: Option<Box<dyn QueryLifecycleLeaseGuard>>,
    catalog_lease: Option<QueryCatalogLease>,
}

impl QueryLifecycleLease {
    pub fn new(guard: Box<dyn QueryLifecycleLeaseGuard>) -> Self {
        Self {
            guard: Some(guard),
            catalog_lease: None,
        }
    }

    /// Attach the query-wide FE catalog ownership after the exact Init plan
    /// has been accepted by the barrier.  Finalize, explicit abort, and the
    /// drop-time abort path all consume this wrapper, so the control leases
    /// cannot drain while an attempt is still live.
    pub(crate) fn with_catalog_lease(mut self, catalog_lease: QueryCatalogLease) -> Self {
        debug_assert!(self.catalog_lease.is_none());
        self.catalog_lease = Some(catalog_lease);
        self
    }

    pub fn finalize(mut self) -> Result<QueryTerminalSet, DistributedQueryError> {
        self.guard
            .take()
            .expect("query lifecycle lease is consumed exactly once")
            .finalize()
    }

    /// Obtain the narrowly-scoped FE terminal capability before consuming the
    /// lifecycle lease. It is only meaningful for a successful connector-write
    /// terminal; ordinary result paths never request it.
    pub(crate) fn retain_terminal_storage_resolver(
        &self,
    ) -> Option<Arc<dyn ConnectorStorageResolver>> {
        self.guard
            .as_ref()
            .and_then(|guard| guard.retain_terminal_storage_resolver())
    }

    pub fn abort_with_outcome(mut self, primary_error: String) -> QueryLifecycleAbortOutcome {
        self.guard
            .take()
            .expect("query lifecycle lease is consumed exactly once")
            .abort_preserving(primary_error)
    }

    pub fn abort_preserving(self, primary_error: String) -> String {
        self.abort_with_outcome(primary_error).into_primary_error()
    }
}

impl Drop for QueryLifecycleLease {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            let _ = guard
                .abort_preserving("query lifecycle lease dropped before completion".to_string());
        }
    }
}

pub(crate) fn compile_query_init_plan(
    fragments: &FragmentLifecycleProjection,
    runtime_filters: Vec<(usize, RuntimeFilterContribution)>,
    options: &QueryInitOptions,
) -> Result<QueryInitPlan, DistributedQueryError> {
    let live_by_backend = options
        .live_backends
        .iter()
        .map(|target| (target.backend_idx(), target.clone()))
        .collect::<BTreeMap<_, _>>();
    if live_by_backend != fragments.frozen_live_backends {
        return Err(contract_error(
            "frozen schedule topology differs from query initialization snapshot",
        ));
    }
    for (&backend_idx, endpoint) in &fragments.endpoints_by_backend {
        let target = live_by_backend.get(&backend_idx).ok_or_else(|| {
            contract_error(format!(
                "scheduled backend {backend_idx} is absent from query initialization live snapshot"
            ))
        })?;
        let target_endpoint = target.endpoint().map_err(protocol_contract_error)?;
        if target_endpoint != *endpoint {
            return Err(contract_error(format!(
                "scheduled backend {backend_idx} endpoint {} differs from query initialization snapshot endpoint {}",
                endpoint.as_host_port(),
                target_endpoint
            )));
        }
    }

    let mut runtime_filter_by_backend = BTreeMap::new();
    for (backend_idx, contribution) in runtime_filters {
        if !live_by_backend.contains_key(&backend_idx) {
            return Err(contract_error(format!(
                "runtime filter backend {backend_idx} is absent from query initialization live snapshot"
            )));
        }
        if runtime_filter_by_backend
            .insert(backend_idx, contribution)
            .is_some()
        {
            return Err(contract_error(format!(
                "runtime filter contribution repeats backend {backend_idx}"
            )));
        }
    }

    let participant_ids = fragments
        .instances_by_backend
        .keys()
        .copied()
        .chain(runtime_filter_by_backend.keys().copied())
        .collect::<BTreeSet<_>>();
    let mut participants = Vec::with_capacity(participant_ids.len());
    for backend_idx in participant_ids {
        let target = live_by_backend
            .get(&backend_idx)
            .ok_or_else(|| {
                contract_error(format!(
                    "query initialization participant backend {backend_idx} is not live"
                ))
            })?
            .clone();
        let backend = protocol_backend_identity(target)?;
        let expected_instances = fragments
            .instances_by_backend
            .get(&backend_idx)
            .cloned()
            .unwrap_or_default();
        let runtime_filter = runtime_filter_by_backend.remove(&backend_idx);
        let manifest = ParticipantManifest::parse(novarocks::ParticipantManifest {
            execution_id: Some(novarocks_proto_codec::lifecycle::encode_query_execution_id(
                options.execution_id,
            )),
            backend: Some(backend.as_proto().clone()),
            native_compatibility_id: Some(novarocks::NativeCompatibilityId {
                value: options.native_compatibility_id.as_bytes().to_vec(),
            }),
            expected_fragment_instance_ids: expected_instances
                .into_iter()
                .map(protocol_unique_id)
                .collect(),
            query_options: Some(*options.query_options.as_proto()),
            query_deadline_unix_ms: options.query_deadline_unix_ms,
            exchange_routes: fragments
                .exchange_routes
                .iter()
                .map(protocol_exchange_route)
                .collect(),
            runtime_filter: runtime_filter.map(|contribution| contribution.as_proto().clone()),
            pre_start_timeout_ms: duration_millis(options.pre_start_timeout)?,
            report_endpoint: Some(options.report_endpoint.as_proto().clone()),
            catalog_set: Some(options.catalog_set.as_proto().clone()),
            credential_lease_descriptors: options
                .credential_leases
                .descriptors()
                .map(novarocks_proto_codec::lifecycle::encode_credential_lease_descriptor)
                .collect(),
        })
        .map_err(|error| {
            contract_error(format!(
                "query initialization participant manifest is invalid: {error}"
            ))
        })?;
        let digest = manifest.digest().map_err(protocol_contract_error)?;
        participants.push(QueryInitParticipant {
            backend_idx,
            backend,
            manifest,
            digest,
        });
    }
    let header = QueryInitPlanHeader::new(options.execution_id, options.query_deadline_unix_ms);
    fragments.freeze_query_init_header(header)?;
    let leases = options
        .credential_leases
        .leases
        .iter()
        .map(|lease| {
            QueryCredentialLease::try_new(
                lease.descriptor.clone(),
                lease.envelope.clone(),
                lease.refresher.clone(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let credential_leases = match options.credential_leases.storage_route() {
        Some(route) => QueryCredentialLeases::try_new_with_storage_route(leases, route)?,
        None => QueryCredentialLeases::try_new(leases)?,
    };
    Ok(QueryInitPlan {
        execution_id: options.execution_id,
        query_deadline_unix_ms: header.query_deadline_unix_ms,
        participants,
        credential_leases,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;

    use super::{
        AttemptCredentialLeaseCollector, QueryCatalogLease, QueryInitOptions,
        QueryLifecycleAbortOutcome, QueryLifecycleLease, QueryLifecycleLeaseGuard,
        compile_query_init_plan,
    };
    use crate::common::backend_topology::{CoordinatorReportEndpoint, LiveBackendTarget};
    use crate::query_execution::contract::{QueryId, ResolvedQueryOptions};
    use crate::query_execution::schedule::FragmentLifecycleProjection;
    use novarocks_proto_codec::catalog::CatalogSet;
    use novarocks_proto_codec::lifecycle::{
        AttemptId, QueryExecutionId, QueryOptions, RuntimeFilterContribution,
    };
    use novarocks_proto_codec::membership::BackendProcessDescriptor;
    use novarocks_proto_models::novarocks;
    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogProviderKind, CatalogVersion, ConnectorControlPlanningLease,
        ConnectorInstanceId, ConnectorVendedCredentialLeaseSink, CredentialConsumerRole,
        StorageAccessRequest, StorageCredentialScopePrefix, VendedS3CredentialLeaseContribution,
        VendedS3CredentialLeaseEntry,
    };
    use novarocks_types::{BackendProcessId, UniqueId};

    struct CountingLifecycleGuard;

    impl QueryLifecycleLeaseGuard for CountingLifecycleGuard {
        fn finalize(
            self: Box<Self>,
        ) -> Result<
            crate::query_execution::terminal_set::QueryTerminalSet,
            crate::query_execution::contract::DistributedQueryError,
        > {
            Ok(
                crate::query_execution::terminal_set::QueryTerminalSet::new(Vec::new())
                    .expect("empty terminal set"),
            )
        }

        fn abort_preserving(self: Box<Self>, primary_error: String) -> QueryLifecycleAbortOutcome {
            QueryLifecycleAbortOutcome::new(primary_error, None)
        }
    }

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(41, 73),
            AttemptId::new(7).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn backend(backend_idx: usize) -> LiveBackendTarget {
        static PROCESS_IDS: OnceLock<[BackendProcessId; 3]> = OnceLock::new();
        let process_ids = PROCESS_IDS.get_or_init(|| {
            [
                BackendProcessId::new_v7(),
                BackendProcessId::new_v7(),
                BackendProcessId::new_v7(),
            ]
        });
        LiveBackendTarget::new(
            backend_idx,
            BackendProcessDescriptor::new(
                process_ids[backend_idx],
                novarocks_proto_codec::lifecycle::QueryControlEndpoint::new(
                    "127.0.0.1",
                    u16::try_from(19040 + backend_idx).expect("valid port"),
                )
                .expect("valid endpoint"),
                "test-deployment",
                "test-build",
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            )
            .expect("valid descriptor"),
        )
    }

    fn runtime_filter(backend_idx: usize) -> (usize, RuntimeFilterContribution) {
        let participant_id = u32::try_from(backend_idx + 1).expect("participant");
        let contribution = RuntimeFilterContribution::parse(novarocks::RuntimeFilterContribution {
            participant_id,
            ..Default::default()
        })
        .expect("valid opaque contribution");
        (backend_idx, contribution)
    }

    fn wire_query_options() -> QueryOptions {
        QueryOptions::parse(novarocks::QueryOptions::default()).expect("valid wire query options")
    }

    fn catalog_set() -> CatalogSet {
        CatalogSet::new([CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("catalog.analytics").expect("catalog name"),
                CatalogVersion::from_bytes([0x23; 32]),
            ),
            CatalogProviderKind::Iceberg,
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
            CatalogProviderKind::Iceberg,
            1,
            vec![],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::FrontendAndBackend,
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

        let leases = collector.into_credential_leases().expect("one-time drain");
        assert_eq!(leases.leases().len(), 1);
        assert!(collector.into_credential_leases().is_err());
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

        collector
            .into_credential_leases()
            .expect("one-way lifecycle handoff");
        assert!(resolver.resolve_vended_s3(&request).is_err());
    }

    #[test]
    fn query_catalog_lease_drains_only_after_lifecycle_finalize_or_abort() {
        let releases = Arc::new(AtomicUsize::new(0));
        let binding = crate::connector::scan_model::planned_files_fixture_binding(
            "catalog.lease",
            HashMap::new(),
            None,
        );
        let release_counter = Arc::clone(&releases);
        let planning_lease = ConnectorControlPlanningLease::new(binding.into(), move || {
            release_counter.fetch_add(1, Ordering::SeqCst);
        });
        let catalog_lease = QueryCatalogLease::new(catalog_set(), vec![planning_lease]);
        assert_eq!(catalog_lease.control_lease_count(), 1);

        QueryLifecycleLease::new(Box::new(CountingLifecycleGuard))
            .with_catalog_lease(catalog_lease)
            .finalize()
            .expect("lifecycle finalizes");
        assert_eq!(releases.load(Ordering::SeqCst), 1);

        let release_counter = Arc::clone(&releases);
        let planning_lease = ConnectorControlPlanningLease::new(
            crate::connector::scan_model::planned_files_fixture_binding(
                "catalog.abort",
                HashMap::new(),
                None,
            )
            .into(),
            move || {
                release_counter.fetch_add(1, Ordering::SeqCst);
            },
        );
        let catalog_lease = QueryCatalogLease::new(catalog_set(), vec![planning_lease]);
        drop(
            QueryLifecycleLease::new(Box::new(CountingLifecycleGuard))
                .with_catalog_lease(catalog_lease),
        );
        assert_eq!(releases.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn query_init_plan_unions_fragment_and_runtime_filter_participants() {
        let fragment_zero = UniqueId::new(10, 1);
        let fragment_one = UniqueId::new(10, 2);
        let fragments = FragmentLifecycleProjection::new(
            BTreeMap::from([
                (0, BTreeSet::from([fragment_zero])),
                (1, BTreeSet::from([fragment_one])),
            ]),
            BTreeMap::from([
                (0, backend(0).endpoint().expect("valid endpoint")),
                (1, backend(1).endpoint().expect("valid endpoint")),
            ]),
            Vec::new(),
        )
        .with_frozen_live_backends(vec![backend(0), backend(1), backend(2)])
        .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let options = QueryInitOptions::new(
            execution_id(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            vec![backend(0), backend(1), backend(2)],
            &resolved,
            wire_query_options(),
            1_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid init options")
        .with_catalog_set(catalog_set());

        let plan = compile_query_init_plan(
            &fragments,
            vec![runtime_filter(1), runtime_filter(2)],
            &options,
        )
        .expect("valid init plan");

        assert_eq!(plan.backend_ids(), vec![0, 1, 2]);
        assert_eq!(
            plan.participant(2)
                .expect("service-only participant")
                .manifest()
                .expected_fragment_instance_ids(),
            Vec::new()
        );
        assert!(
            plan.participant(2)
                .expect("service-only participant")
                .manifest()
                .runtime_filter()
                .expect("validated contribution")
                .is_some()
        );
        let expected_catalogs = options.catalog_set().as_proto().clone();
        for backend_idx in plan.backend_ids() {
            assert_eq!(
                plan.participant(backend_idx)
                    .expect("participant")
                    .manifest()
                    .catalog_set()
                    .expect("catalog set")
                    .as_proto(),
                &expected_catalogs,
                "every Init manifest carries the same frozen query-wide catalog set"
            );
        }
    }

    #[test]
    fn runtime_filter_contribution_is_carried_opaquely() {
        let fragments =
            FragmentLifecycleProjection::new(BTreeMap::new(), BTreeMap::new(), Vec::new())
                .with_frozen_live_backends(vec![backend(2)])
                .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let options = QueryInitOptions::new(
            execution_id(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            vec![backend(2)],
            &resolved,
            wire_query_options(),
            1_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid init options");

        let plan = compile_query_init_plan(&fragments, vec![runtime_filter(2)], &options)
            .expect("valid init plan");
        let contribution = plan
            .participant(2)
            .expect("runtime filter participant")
            .manifest()
            .runtime_filter()
            .expect("validated runtime filter")
            .expect("runtime filter contribution");

        assert_eq!(contribution.participant_id(), 3);
        assert_eq!(contribution.as_proto(), runtime_filter(2).1.as_proto());
    }

    #[test]
    fn query_init_plan_rejects_backend_restart_at_the_same_endpoint() {
        let fragments = FragmentLifecycleProjection::new(
            BTreeMap::from([(0, BTreeSet::from([UniqueId::new(10, 1)]))]),
            BTreeMap::from([(0, backend(0).endpoint().expect("valid endpoint"))]),
            Vec::new(),
        )
        .with_frozen_live_backends(vec![backend(0)])
        .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let restarted = LiveBackendTarget::new(
            backend(0).backend_idx(),
            BackendProcessDescriptor::new(
                BackendProcessId::new_v7(),
                novarocks_proto_codec::lifecycle::QueryControlEndpoint::new("127.0.0.1", 19040)
                    .expect("valid endpoint"),
                "test-deployment",
                "test-build",
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            )
            .expect("valid descriptor"),
        );
        let options = QueryInitOptions::new(
            execution_id(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            vec![restarted],
            &resolved,
            wire_query_options(),
            1_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid restarted snapshot");

        let error = match compile_query_init_plan(&fragments, Vec::new(), &options) {
            Ok(_) => panic!("same endpoint with a new start epoch must invalidate the schedule"),
            Err(error) => error,
        };

        assert!(
            error
                .message()
                .contains("frozen schedule topology differs from query initialization snapshot")
        );
    }

    #[test]
    fn query_init_options_reject_other_island_target_before_manifest_construction() {
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let other_island = LiveBackendTarget::new(
            0,
            BackendProcessDescriptor::new(
                BackendProcessId::new_v7(),
                novarocks_proto_codec::lifecycle::QueryControlEndpoint::new("127.0.0.1", 19040)
                    .expect("valid endpoint"),
                "test-deployment",
                "different-build",
                novarocks_types::NativeCompatibilityId::new([0x72; 32]),
            )
            .expect("valid descriptor"),
        );

        let error = match QueryInitOptions::new(
            execution_id(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            vec![other_island],
            &resolved,
            wire_query_options(),
            1_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        ) {
            Ok(_) => panic!("other-island target must not produce a participant manifest"),
            Err(error) => error,
        };

        assert!(
            error.message().contains("another compatibility island"),
            "{error}"
        );
    }

    #[test]
    fn query_init_plan_rejects_duplicate_runtime_filter_backend() {
        let fragments =
            FragmentLifecycleProjection::new(BTreeMap::new(), BTreeMap::new(), Vec::new())
                .with_frozen_live_backends(vec![backend(1)])
                .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let options = QueryInitOptions::new(
            execution_id(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            vec![backend(1)],
            &resolved,
            wire_query_options(),
            9_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid init options");

        let error = match compile_query_init_plan(
            &fragments,
            vec![runtime_filter(1), runtime_filter(1)],
            &options,
        ) {
            Ok(_) => panic!("one backend must have at most one runtime-filter contribution"),
            Err(error) => error,
        };

        assert!(
            error
                .message()
                .contains("runtime filter contribution repeats backend 1")
        );
    }

    #[test]
    fn query_init_plan_freezes_deadline() {
        let fragments =
            FragmentLifecycleProjection::new(BTreeMap::new(), BTreeMap::new(), Vec::new())
                .with_frozen_live_backends(vec![backend(2)])
                .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let options = QueryInitOptions::new(
            execution_id(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            vec![backend(2)],
            &resolved,
            wire_query_options(),
            9_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid init options");

        let plan = compile_query_init_plan(&fragments, vec![runtime_filter(2)], &options)
            .expect("valid init plan");

        assert_eq!(plan.query_deadline_unix_ms(), 9_000);

        let changed_deadline = QueryInitOptions::new(
            execution_id(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            vec![backend(2)],
            &resolved,
            wire_query_options(),
            9_001,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid changed options");
        let deadline_error =
            match compile_query_init_plan(&fragments, vec![runtime_filter(2)], &changed_deadline) {
                Ok(_) => panic!("one schedule cannot rebuild the same QEI with a new deadline"),
                Err(error) => error,
            };
        assert!(
            deadline_error
                .message()
                .contains("query initialization header differs")
        );
    }
}
