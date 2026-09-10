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

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::backend_topology::LiveBackendTarget;
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, ResolvedQueryOptions,
};
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_proto_codec::catalog::CatalogSet;
use novarocks_proto_codec::lifecycle::{
    CredentialLeaseSecretEnvelope, QueryExecutionId, QueryOptions as ProtocolQueryOptions,
};
use novarocks_proto_codec::lifecycle::{
    encode_credential_lease_descriptor, encode_credential_lease_secret_envelope,
};
use novarocks_spi::connector::{
    CatalogCredentialMode, CatalogCredentialPurpose, CatalogNonSecretProperty, CatalogProperties,
    CatalogStorageAccessDomainInput, ConnectorControlPlanningLease, ConnectorError,
    ConnectorErrorKind, ConnectorProviderId, ConnectorStorageResolver,
    ConnectorVendedCredentialLeaseSink, ConnectorVendedS3CredentialLeaseRefresher,
    CredentialConsumerRole, CredentialLeaseDescriptor, CredentialLeaseId, CredentialLeaseProvider,
    ResolvedVendedS3Access, StorageAccessRequest, StorageCredentialScopePrefix,
    VendedS3CredentialLeaseContribution,
};
use novarocks_task_codec::domain::WireCredential;
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
    route: Arc<AttemptCredentialLeaseStorageRoute>,
    sink_route: Arc<AttemptCredentialLeaseSinkRoute>,
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
        let (entries, _refresh_endpoint, provider_refresher) =
            contribution.into_parts_with_refresher();
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
            let refresh_capable = provider_refresher.is_some();
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

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

fn protocol_contract_error(error: novarocks_proto_codec::ProtocolError) -> DistributedQueryError {
    contract_error(error.to_string())
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
            leases: Mutex::new(self.leases),
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
/// The table is behind a lock because a long query rotates it in place: the
/// frontend is the credential principal, so it is this table that must hold the
/// current epoch of every vended lease -- both for its own reads and for the
/// material each rotation hands to the backends.
pub(crate) struct AttemptCredentialStorage {
    leases: Mutex<Vec<QueryCredentialLease>>,
    route: Option<Arc<AttemptCredentialLeaseStorageRoute>>,
}

impl AttemptCredentialStorage {
    /// Every lease that can be rotated, with the instant it stops being usable.
    pub(crate) fn refreshable(&self) -> Vec<(CredentialLeaseId, u64)> {
        self.leases
            .lock()
            .unwrap_or_else(|error| error.into_inner())
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

    /// The current descriptor of one lease and the source that can refresh it.
    ///
    /// The descriptor is what the provider is asked to advance from, so it is
    /// read under the same lock that a rotation writes: asking from a stale
    /// descriptor would produce an epoch this table would then refuse.
    pub(crate) fn refresh_source(
        &self,
        lease_id: CredentialLeaseId,
    ) -> Option<(
        CredentialLeaseDescriptor,
        Arc<dyn QueryCredentialLeaseRefresher>,
    )> {
        let leases = self
            .leases
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let index = leases
            .binary_search_by_key(&lease_id, |lease| lease.descriptor().lease_id())
            .ok()?;
        let lease = &leases[index];
        Some((lease.descriptor().clone(), Arc::clone(lease.refresher()?)))
    }

    /// Installs one refreshed lease, keeping every immutable fact of it.
    ///
    /// The rules are the ones the lifecycle owner applied: the same refresh
    /// scope, the exact next provider epoch, a later expiry, and an envelope
    /// that matches its own descriptor. Anything else is refused rather than
    /// installed, because a table that accepted a re-scoped lease would hand
    /// out access nobody vended.
    pub(crate) fn apply_refresh(
        &self,
        refreshed: &QueryCredentialLeaseRefresh,
    ) -> Result<(), DistributedQueryError> {
        let descriptor = refreshed.descriptor();
        let mut leases = self
            .leases
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let index = leases
            .binary_search_by_key(&descriptor.lease_id(), |lease| {
                lease.descriptor().lease_id()
            })
            .map_err(|_| {
                contract_error("query credential lease refresh references an unknown lease")
            })?;
        let lease = &mut leases[index];
        if !lease.descriptor.has_same_refresh_scope(descriptor)
            || descriptor.epoch() != lease.descriptor.epoch().saturating_add(1)
            || descriptor.not_after_unix_ms() <= lease.descriptor.not_after_unix_ms()
            || !refreshed.envelope().matches_descriptor(descriptor)
        {
            return Err(contract_error(
                "query credential lease refresh changed immutable scope or epoch",
            ));
        }
        lease.descriptor = descriptor.clone();
        lease.envelope = refreshed.envelope().clone();
        Ok(())
    }

    /// The whole table as the wire material one rotation carries.
    ///
    /// The whole table, not the lease that moved: a credential domain epoch
    /// counts rotations of the batch, and a backend installs what it is handed,
    /// so a partial batch would revoke the leases it left out.
    pub(crate) fn freeze_material(&self) -> Result<WireCredential, DistributedQueryError> {
        let leases = self
            .leases
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut descriptors = Vec::with_capacity(leases.len());
        let mut envelopes = Vec::with_capacity(leases.len());
        for lease in leases.iter() {
            descriptors.push(encode_credential_lease_descriptor(lease.descriptor()));
            envelopes.push(encode_credential_lease_secret_envelope(lease.envelope()));
        }
        // The codec's own message carries no secret, and the material never
        // reaches a rendering.
        WireCredential::decode(
            &descriptors,
            &envelopes,
            novarocks_proto_codec::FieldPath::root("rotated_credential"),
        )
        .map_err(|error| {
            contract_error(format!(
                "rotated credential contribution is not installable: {error}"
            ))
        })
    }
}

impl ConnectorStorageResolver for AttemptCredentialStorage {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        resolve_vended_s3_access(
            &self
                .leases
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
            request,
        )
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

pub struct QueryInitOptions {
    execution_id: QueryExecutionId,
    native_compatibility_id: NativeCompatibilityId,
    live_backends: Vec<LiveBackendTarget>,
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
        Ok(Self {
            execution_id,
            native_compatibility_id,
            live_backends,
            native_submission_options: native_submission_options.runtime_options().clone(),
            query_options,
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{AttemptCredentialLeaseCollector, QueryCatalogLease, QueryInitOptions};
    use crate::common::backend_topology::LiveBackendTarget;
    use crate::query_execution::contract::{QueryId, ResolvedQueryOptions};
    use novarocks_proto_codec::catalog::CatalogSet;
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId, QueryOptions};
    use novarocks_proto_codec::membership::BackendProcessDescriptor;
    use novarocks_proto_models::novarocks;
    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogVersion, ConnectorControlPlanningLease, ConnectorInstanceId,
        ConnectorProviderId, ConnectorVendedCredentialLeaseSink,
        ConnectorVendedS3CredentialLeaseRefresher, CredentialConsumerRole, StorageAccessRequest,
        StorageCredentialScopePrefix, VendedS3CredentialLeaseContribution,
        VendedS3CredentialLeaseEntry, VendedS3CredentialLeaseRefresh,
    };
    use novarocks_types::BackendProcessId;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(41, 73),
            AttemptId::new(7).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
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
        ) -> Result<VendedS3CredentialLeaseRefresh, novarocks_spi::connector::ConnectorError>
        {
            panic!("the capability is not invoked by this collection test")
        }
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
        assert!(
            !leases.leases()[0].descriptor().refresh_capable(),
            "an endpoint-free contribution without a provider source is not refreshable"
        );
        assert!(collector.into_credential_leases().is_err());
    }

    #[test]
    fn provider_sink_does_not_keep_a_cancelled_attempt_collector_alive() {
        let collector = AttemptCredentialLeaseCollector::new(execution_id());
        let weak_collector = Arc::downgrade(&collector);
        let sink = collector.sink();
        drop(collector);

        assert!(
            weak_collector.upgrade().is_none(),
            "a provider-held sink must not extend the attempt collector lifetime"
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
            .with_refresher(Arc::new(ProviderLocalRefresher))
            .expect("provider-local refresher");
        collector
            .offer_vended_s3_credential_lease(&properties, contribution)
            .expect("provider-local contribution");

        let leases = collector.into_credential_leases().expect("one-time drain");
        assert!(leases.leases()[0].descriptor().refresh_capable());
        assert!(leases.leases()[0].refresher().is_some());
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
            BackendProcessDescriptor::new(
                BackendProcessId::new_v7(),
                novarocks_proto_codec::lifecycle::QueryControlEndpoint::new("127.0.0.1", 19040)
                    .expect("valid endpoint"),
                "test-deployment",
                "different-build",
                novarocks_types::NativeCompatibilityId::new([0x72; 32]),
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
