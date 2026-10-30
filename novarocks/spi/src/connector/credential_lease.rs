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

//! Secret-free query-attempt credential lease metadata.
//!
//! A descriptor is immutable attempt metadata and may be included in the
//! participant-manifest digest. Credential values intentionally do not appear
//! here: the native lifecycle confidential carrier owns their short-lived
//! transport and execution-side installation.

use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::Duration;

use novarocks_secret::SecretValue;

use super::{
    CatalogHandle, CatalogProperties, ConnectorError, ConnectorErrorKind, StorageAccessDomainId,
    StorageCredentialScopePrefix,
};

/// Identity minted once by an existing process-local acquisition owner.
/// It is neither an authorization grant nor an endpoint fault-domain key.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct CredentialAcquisitionOwnerId(uuid::Uuid);
impl CredentialAcquisitionOwnerId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}
impl Default for CredentialAcquisitionOwnerId {
    fn default() -> Self {
        Self::new()
    }
}
impl std::fmt::Debug for CredentialAcquisitionOwnerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CredentialAcquisitionOwnerId { .. }")
    }
}

/// One original seed authorization instance, independent of query/lease ids.
/// Copies of this value name the same grant; a new grant gets a fresh identity.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct VendedS3SeedGrantId(uuid::Uuid);
impl VendedS3SeedGrantId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}
impl Default for VendedS3SeedGrantId {
    fn default() -> Self {
        Self::new()
    }
}
impl std::fmt::Debug for VendedS3SeedGrantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VendedS3SeedGrantId { .. }")
    }
}

/// The original acquisition capability's non-secret, immutable cache key.
/// The full scope is retained because the refresher validates the whole set.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct VendedS3InProcessAcquisitionIdentity {
    owner: CredentialAcquisitionOwnerId,
    renewal_path: Option<CredentialRenewalPath>,
    prefixes: Arc<[StorageCredentialScopePrefix]>,
}
impl VendedS3InProcessAcquisitionIdentity {
    pub fn try_new(
        owner: CredentialAcquisitionOwnerId,
        renewal_path: Option<CredentialRenewalPath>,
        mut prefixes: Vec<StorageCredentialScopePrefix>,
    ) -> Result<Self, ConnectorError> {
        if prefixes.is_empty() || prefixes.len() > MAX_CREDENTIAL_LEASE_PREFIXES {
            return Err(exhausted("in-process credential acquisition prefix set"));
        }
        prefixes.sort();
        if prefixes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid(
                "duplicate in-process credential acquisition prefix",
            ));
        }
        Ok(Self {
            owner,
            renewal_path,
            prefixes: prefixes.into(),
        })
    }
    pub fn renewal_path(&self) -> Option<&CredentialRenewalPath> {
        self.renewal_path.as_ref()
    }
    pub fn prefixes(&self) -> &[StorageCredentialScopePrefix] {
        &self.prefixes
    }
}

/// The exact capability and its identity travel together inside this process.
#[derive(Clone)]
pub struct VendedS3InProcessAcquisition {
    identity: VendedS3InProcessAcquisitionIdentity,
    refresher: Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>,
}
impl VendedS3InProcessAcquisition {
    pub fn identity(&self) -> &VendedS3InProcessAcquisitionIdentity {
        &self.identity
    }
    pub fn refresher(&self) -> &Arc<dyn ConnectorVendedS3CredentialLeaseRefresher> {
        &self.refresher
    }
}

pub const MAX_CREDENTIAL_LEASES_PER_QUERY: usize = 64;
pub const MAX_CREDENTIAL_LEASE_PREFIXES: usize = 64;
/// Bounded like every other wire string here: an address a server advertises
/// is still input, and an unbounded one is a way to make a descriptor large.
pub const MAX_CREDENTIALS_ENDPOINT_BYTES: usize = 2 * 1024;
/// Upper bound on the public routing headers one acquisition path carries.
pub const MAX_CREDENTIAL_ROUTING_HEADERS: usize = 16;
/// Upper bound on the summed bytes of routing header names and values.
pub const MAX_CREDENTIAL_ROUTING_HEADER_BYTES: usize = 4 * 1024;
/// Namespace depth a load-table acquisition path may name.
pub const MAX_CREDENTIAL_NAMESPACE_LEVELS: usize = 16;
/// Byte bound on one namespace level, table name or table uuid.
pub const MAX_CREDENTIAL_IDENTIFIER_BYTES: usize = 512;
pub const MAX_CREDENTIAL_LEASE_ID_BYTES: usize = 16;
pub const MAX_CREDENTIAL_LEASE_SECRET_SCALAR_BYTES: usize = 8 * 1024;
pub const MAX_CREDENTIAL_LEASE_SECRET_ENVELOPE_BYTES: usize = 256 * 1024;

/// Stable, non-secret identity for one query-attempt credential lease.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CredentialLeaseId([u8; MAX_CREDENTIAL_LEASE_ID_BYTES]);

impl CredentialLeaseId {
    pub fn try_from_bytes(
        bytes: [u8; MAX_CREDENTIAL_LEASE_ID_BYTES],
    ) -> Result<Self, ConnectorError> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(invalid("credential lease id"));
        }
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; MAX_CREDENTIAL_LEASE_ID_BYTES] {
        &self.0
    }
}

/// Closed provider family accepted by the M2 v1 confidential lease carrier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CredentialLeaseProvider {
    S3,
}

/// One move-only S3 credential entry contributed by a provider while it still
/// owns a vended metadata response.
///
/// The entry deliberately has no `Clone`, `Debug`, or serialization trait.
/// Secret values move only into the query-attempt collector that owns
/// confidential lifecycle installation.
pub struct VendedS3CredentialLeaseEntry {
    prefix: StorageCredentialScopePrefix,
    not_after_unix_ms: u64,
    access_key_id: SecretValue,
    secret_access_key: SecretValue,
    session_token: SecretValue,
}

impl VendedS3CredentialLeaseEntry {
    pub fn try_new(
        prefix: StorageCredentialScopePrefix,
        not_after_unix_ms: u64,
        access_key_id: SecretValue,
        secret_access_key: SecretValue,
        session_token: SecretValue,
    ) -> Result<Self, ConnectorError> {
        if not_after_unix_ms == 0 {
            return Err(invalid("vended S3 credential expiration"));
        }
        Ok(Self {
            prefix,
            not_after_unix_ms,
            access_key_id,
            secret_access_key,
            session_token,
        })
    }

    pub fn prefix(&self) -> &StorageCredentialScopePrefix {
        &self.prefix
    }

    pub const fn not_after_unix_ms(&self) -> u64 {
        self.not_after_unix_ms
    }

    /// Transfer the secret scalars to the sole query-attempt lease owner.
    /// Callers must not retain this entry after invoking this method.
    pub fn into_parts(
        self,
    ) -> (
        StorageCredentialScopePrefix,
        u64,
        SecretValue,
        SecretValue,
        SecretValue,
    ) {
        (
            self.prefix,
            self.not_after_unix_ms,
            self.access_key_id,
            self.secret_access_key,
            self.session_token,
        )
    }
}

/// Provider-neutral, response-local contribution for one vended S3 lease
/// scope.
///
/// This value is deliberately move-only. It may travel only from a provider's
/// metadata-response adapter to an in-process query-attempt collector; it is
/// never a table attribute, cache value, SQL plan field, or native wire value.
pub struct VendedS3CredentialLeaseContribution {
    entries: Vec<VendedS3CredentialLeaseEntry>,
    /// What the provider observed about how this scope can be re-acquired.
    ///
    /// It is announced to consumers rather than kept private because a
    /// consumer that must acquire for itself cannot derive it: the path is the
    /// catalog's statement, not a canonical route a client may assume
    /// (CAD-1 D2).
    renewal_path: Option<CredentialRenewalPath>,
    refresher: Option<(
        CredentialAcquisitionOwnerId,
        Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>,
    )>,
}

impl VendedS3CredentialLeaseContribution {
    pub fn try_new(
        mut entries: Vec<VendedS3CredentialLeaseEntry>,
        renewal_path: Option<CredentialRenewalPath>,
    ) -> Result<Self, ConnectorError> {
        if entries.is_empty() || entries.len() > MAX_CREDENTIAL_LEASE_PREFIXES {
            return Err(exhausted("vended S3 credential entry set"));
        }
        entries.sort_by(|left, right| left.prefix.cmp(&right.prefix));
        if entries
            .windows(2)
            .any(|pair| pair[0].prefix == pair[1].prefix)
        {
            return Err(invalid("duplicate vended S3 credential prefix"));
        }
        Ok(Self {
            entries,
            renewal_path,
            refresher: None,
        })
    }

    /// Attach the provider-owned, FE-local source for a later refresh. The
    /// source is a capability, not a wire field or a table property; it can be
    /// consumed only by the query-attempt collector that receives this
    /// response-local contribution.
    pub fn with_refresher(
        mut self,
        owner: CredentialAcquisitionOwnerId,
        refresher: Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>,
    ) -> Result<Self, ConnectorError> {
        self.refresher = Some((owner, refresher));
        Ok(self)
    }

    pub fn entries(&self) -> &[VendedS3CredentialLeaseEntry] {
        &self.entries
    }

    pub fn renewal_path(&self) -> Option<&CredentialRenewalPath> {
        self.renewal_path.as_ref()
    }

    pub fn refresh_endpoint(&self) -> Option<&str> {
        match &self.renewal_path {
            Some(CredentialRenewalPath::CredentialsEndpoint(routing)) => Some(routing.url()),
            _ => None,
        }
    }

    /// Replace the announced acquisition path with the one the provider
    /// resolved for this exact response.
    ///
    /// A provider that also builds a refresher knows more than the response
    /// alone says — which table a load-table acquisition names, for instance —
    /// and the announcement must carry the same fact the refresher acts on.
    pub fn with_renewal_path(mut self, renewal_path: CredentialRenewalPath) -> Self {
        self.renewal_path = Some(renewal_path);
        self
    }

    pub fn refresher(&self) -> Option<&Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>> {
        self.refresher.as_ref().map(|(_, refresher)| refresher)
    }

    /// Transfer the complete response-local contribution to the sole
    /// query-attempt collector.
    pub fn into_parts(
        self,
    ) -> (
        Vec<VendedS3CredentialLeaseEntry>,
        Option<CredentialRenewalPath>,
    ) {
        (self.entries, self.renewal_path)
    }

    /// Transfer both the confidential entries and the provider refresh source
    /// to the only query-attempt collector. Existing callers that do not own a
    /// refresher should use [`Self::into_parts`].
    pub fn into_parts_with_refresher(
        self,
    ) -> (
        Vec<VendedS3CredentialLeaseEntry>,
        Option<CredentialRenewalPath>,
        Option<VendedS3InProcessAcquisition>,
    ) {
        let acquisition = self
            .refresher
            .map(|(owner, refresher)| VendedS3InProcessAcquisition {
                identity: VendedS3InProcessAcquisitionIdentity {
                    owner,
                    renewal_path: self.renewal_path.clone(),
                    prefixes: self
                        .entries
                        .iter()
                        .map(|entry| entry.prefix.clone())
                        .collect(),
                },
                refresher,
            });
        (self.entries, self.renewal_path, acquisition)
    }
}

/// Provider-neutral, move-only values returned by one FE-owned vended S3
/// refresh. The provider retains all HTTP/catalog identity; the lifecycle
/// owner receives only another closed set of response-local credential values.
pub struct VendedS3CredentialLeaseRefresh {
    entries: Vec<VendedS3CredentialLeaseEntry>,
}

impl VendedS3CredentialLeaseRefresh {
    pub fn try_new(mut entries: Vec<VendedS3CredentialLeaseEntry>) -> Result<Self, ConnectorError> {
        if entries.is_empty() || entries.len() > MAX_CREDENTIAL_LEASE_PREFIXES {
            return Err(exhausted("refreshed vended S3 credential entry set"));
        }
        entries.sort_by(|left, right| left.prefix.cmp(&right.prefix));
        if entries
            .windows(2)
            .any(|pair| pair[0].prefix == pair[1].prefix)
        {
            return Err(invalid("duplicate refreshed vended S3 credential prefix"));
        }
        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[VendedS3CredentialLeaseEntry] {
        &self.entries
    }

    /// Transfer all refreshed values to the FE lifecycle owner. It is
    /// responsible for selecting the existing lease's exact scope and forming
    /// the next confidential epoch.
    pub fn into_entries(self) -> Vec<VendedS3CredentialLeaseEntry> {
        self.entries
    }
}

/// Secret-free observation used immediately before a provider refresh dispatch.
///
/// This is intentionally a process-local capability rather than a serialized
/// cancellation token. The provider only learns whether another external
/// request remains permitted; it cannot obtain an attempt identity, a
/// credential, or a runtime registry from this port.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VendedS3CredentialRefreshDispatch {
    Permitted,
    Fenced,
}

/// Attempt-local authority over provider credential-refresh dispatches.
///
/// Providers must check this before every external dispatch, including a
/// retry after a retryable response. A `Fenced` result means that no new
/// request may be started; it never claims that an already-issued request was
/// forcibly cancelled.
pub trait VendedS3CredentialRefreshDispatchGuard: Send + Sync {
    fn provider_dispatch(&self) -> VendedS3CredentialRefreshDispatch;
}

/// Secret-free, FE-local bound for one provider credential-refresh call.
///
/// The provider starts one monotonic deadline when it receives this policy.
/// Every attempt, its transport setup, response read, and retry wait must fit
/// inside that one budget.  This is deliberately a synchronous call policy:
/// it does not claim that a caller can forcibly abort an already-issued I/O
/// operation.
#[derive(Clone)]
pub struct VendedS3CredentialRefreshCallPolicy {
    remaining: Duration,
    max_attempts: NonZeroU8,
    retry_backoff: Duration,
    dispatch_guard: Arc<dyn VendedS3CredentialRefreshDispatchGuard>,
}

impl std::fmt::Debug for VendedS3CredentialRefreshCallPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VendedS3CredentialRefreshCallPolicy")
            .field("remaining", &self.remaining)
            .field("max_attempts", &self.max_attempts)
            .field("retry_backoff", &self.retry_backoff)
            .field("dispatch_guard", &"<process-local>")
            .finish()
    }
}

impl VendedS3CredentialRefreshCallPolicy {
    pub fn try_new(
        remaining: Duration,
        max_attempts: NonZeroU8,
        retry_backoff: Duration,
        dispatch_guard: Arc<dyn VendedS3CredentialRefreshDispatchGuard>,
    ) -> Result<Self, ConnectorError> {
        if remaining.is_zero() {
            return Err(invalid("vended S3 credential refresh remaining budget"));
        }
        if max_attempts.get() > 1 && retry_backoff.is_zero() {
            return Err(invalid("vended S3 credential refresh retry backoff"));
        }
        Ok(Self {
            remaining,
            max_attempts,
            retry_backoff,
            dispatch_guard,
        })
    }

    pub const fn remaining(&self) -> Duration {
        self.remaining
    }

    pub const fn max_attempts(&self) -> NonZeroU8 {
        self.max_attempts
    }

    pub const fn retry_backoff(&self) -> Duration {
        self.retry_backoff
    }

    /// Reads the same attempt-local dispatch authority used by the caller.
    /// The result must be checked at each actual provider request boundary.
    pub fn provider_dispatch(&self) -> VendedS3CredentialRefreshDispatch {
        self.dispatch_guard.provider_dispatch()
    }
}

/// FE-local provider capability for refreshing an already-admitted vended S3
/// lease. Implementations must use their own catalog identity and may never
/// be serialized or attached to a BE request.
pub trait ConnectorVendedS3CredentialLeaseRefresher: Send + Sync {
    fn refresh_vended_s3_credentials(
        &self,
        policy: VendedS3CredentialRefreshCallPolicy,
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError>;

    /// Explicit async acquisition for the original governed request. The old
    /// synchronous capability is never used as a fallback on this path.
    fn refresh_vended_s3_credentials_async<'a>(
        &'a self,
        _policy: VendedS3CredentialRefreshCallPolicy,
        _request: super::ConnectorRequestContext,
    ) -> futures::future::BoxFuture<'a, Result<VendedS3CredentialLeaseRefresh, ConnectorError>>
    {
        Box::pin(async {
            Err(ConnectorError::new(
                super::ConnectorErrorKind::Unsupported,
                "async vended credential refresh requires an explicit provider author",
            ))
        })
    }
}

/// FE-local receiver for provider vended-credential contributions.
///
/// The trait object is carried only by the request context. It is not
/// serializable and must never be captured by table, plan, fragment, or cache
/// state.
pub trait ConnectorVendedCredentialLeaseSink: Send + Sync {
    fn offer_vended_s3_credential_lease(
        &self,
        catalog_properties: &CatalogProperties,
        contribution: VendedS3CredentialLeaseContribution,
    ) -> Result<(), ConnectorError>;
}

/// A request-local, query-wide vended-credential sink attachment.
///
/// A query can touch multiple catalog generations, so each metadata call
/// clones the query-wide context and decorates its own collection port with
/// the exact catalog properties before invoking the provider.
#[derive(Clone)]
pub struct ConnectorVendedCredentialLeaseCollectionPort {
    catalog_properties: CatalogProperties,
    sink: Arc<dyn ConnectorVendedCredentialLeaseSink>,
}

/// Confidential, in-process material for one query-attempt credential lease.
///
/// This value deliberately belongs to SPI rather than a wire codec: it owns
/// secret wrappers and exposes values only at the concrete protocol or storage
/// consumer boundary. It is never manifest/digest material.
#[derive(Clone, Eq, PartialEq)]
pub struct CredentialLeaseSecretEnvelope {
    lease_id: CredentialLeaseId,
    epoch: u64,
    access_key_id: SecretValue,
    secret_access_key: SecretValue,
    session_token: SecretValue,
    session_token_expires_at_unix_ms: u64,
}

impl std::fmt::Debug for CredentialLeaseSecretEnvelope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialLeaseSecretEnvelope")
            .field("lease_id", &self.lease_id)
            .field("epoch", &self.epoch)
            .field("secret_scalar_count", &3)
            .field(
                "session_token_expires_at_unix_ms",
                &self.session_token_expires_at_unix_ms,
            )
            .field("material", &"[REDACTED]")
            .finish()
    }
}

impl CredentialLeaseSecretEnvelope {
    pub fn try_new(
        lease_id: CredentialLeaseId,
        epoch: u64,
        access_key_id: SecretValue,
        secret_access_key: SecretValue,
        session_token: SecretValue,
        session_token_expires_at_unix_ms: u64,
    ) -> Result<Self, ConnectorError> {
        validate_secret_scalar(access_key_id.expose_secret())?;
        validate_secret_scalar(secret_access_key.expose_secret())?;
        validate_secret_scalar(session_token.expose_secret())?;
        if epoch == 0 || session_token_expires_at_unix_ms == 0 {
            return Err(invalid("credential lease epoch or expiration"));
        }
        Ok(Self {
            lease_id,
            epoch,
            access_key_id,
            secret_access_key,
            session_token,
            session_token_expires_at_unix_ms,
        })
    }

    /// Wraps plain scalars a coordinator-local provider response produced.
    ///
    /// No longer a wire boundary: nothing decodes material from the native
    /// transport any more. It remains because the provider hands its response
    /// over as strings (CAD-1 C07b).
    pub fn try_new_from_scalars(
        lease_id: CredentialLeaseId,
        epoch: u64,
        access_key_id: String,
        secret_access_key: String,
        session_token: String,
        session_token_expires_at_unix_ms: u64,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            lease_id,
            epoch,
            SecretValue::new(access_key_id),
            SecretValue::new(secret_access_key),
            SecretValue::new(session_token),
            session_token_expires_at_unix_ms,
        )
    }

    pub const fn lease_id(&self) -> CredentialLeaseId {
        self.lease_id
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn session_token_expires_at_unix_ms(&self) -> u64 {
        self.session_token_expires_at_unix_ms
    }

    pub const fn access_key_id(&self) -> &SecretValue {
        &self.access_key_id
    }

    pub const fn secret_access_key(&self) -> &SecretValue {
        &self.secret_access_key
    }

    pub const fn session_token(&self) -> &SecretValue {
        &self.session_token
    }

    pub fn matches_descriptor(&self, descriptor: &CredentialLeaseDescriptor) -> bool {
        self.lease_id == descriptor.lease_id()
            && self.epoch == descriptor.epoch()
            && self.session_token_expires_at_unix_ms == descriptor.not_after_unix_ms()
    }
}

impl ConnectorVendedCredentialLeaseCollectionPort {
    pub fn new(
        catalog_properties: CatalogProperties,
        sink: Arc<dyn ConnectorVendedCredentialLeaseSink>,
    ) -> Self {
        Self {
            catalog_properties,
            sink,
        }
    }

    pub const fn catalog_properties(&self) -> &CatalogProperties {
        &self.catalog_properties
    }

    /// Immediately transfer one provider response into the attached
    /// query-attempt collector. The exact catalog-generation properties are
    /// attached by query materialization before this provider call starts.
    pub fn offer_vended_s3_credential_lease(
        &self,
        contribution: VendedS3CredentialLeaseContribution,
    ) -> Result<(), ConnectorError> {
        self.sink
            .offer_vended_s3_credential_lease(&self.catalog_properties, contribution)
    }
}

/// Immutable, secret-free lease metadata frozen for one query participant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialLeaseDescriptor {
    lease_id: CredentialLeaseId,
    epoch: u64,
    owner: CatalogHandle,
    provider: CredentialLeaseProvider,
    prefixes: Vec<StorageCredentialScopePrefix>,
    not_after_unix_ms: u64,
    refresh_capable: bool,
    /// How a consumer of this lease acquires for itself, when the server
    /// advertised a path at all.
    ///
    /// Non-secret on purpose: an address or a table identity, never material,
    /// and it belongs on the descriptor rather than beside the secret because
    /// that is what lets a consumer acquire for itself. The client never
    /// constructs this path — the spec has the server advertise it, and a
    /// client that guessed the canonical route would be asserting a capability
    /// the deployment may not have (CAD-1 D2).
    renewal_path: Option<CredentialRenewalPath>,
    storage_access_domain_id: StorageAccessDomainId,
}

/// The closed set of acquisition paths a catalog can advertise.
///
/// Two variants because the REST specification has two: a catalog may serve a
/// scope's credentials from its own address, or it may vend them only inside a
/// load-table response. A consumer selects one and never probes the other
/// after a failure (CAD-1 D2, D11).
///
/// Both carry the public routing context the coordinator froze from its one
/// catalog session, so a consumer reaches exactly the service and catalog the
/// coordinator observed, never a session of its own (IRU-13a D15).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CredentialRenewalPath {
    CredentialsEndpoint(CredentialRoutingContext),
    LoadTableDelegation {
        routing: CredentialRoutingContext,
        table: CredentialLoadTableDelegation,
    },
}

impl CredentialRenewalPath {
    /// Where and with which public headers a consumer sends its acquisition.
    pub fn routing(&self) -> &CredentialRoutingContext {
        match self {
            Self::CredentialsEndpoint(routing) | Self::LoadTableDelegation { routing, .. } => {
                routing
            }
        }
    }
}

/// One public request header an acquisition sends to reach its catalog.
///
/// Public by construction (a persistent catalog header or an explicitly
/// declared server-supplied one), yet still request data rather than a
/// diagnostic: `Debug` prints the name only.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CredentialRoutingHeader {
    name: Arc<str>,
    value: Arc<str>,
}

impl CredentialRoutingHeader {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl std::fmt::Debug for CredentialRoutingHeader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialRoutingHeader")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// The public facts that decide which service and catalog an acquisition
/// reaches: the complete request address and the public routing headers. It
/// never carries authentication material; each node authenticates with its
/// own identity.
///
/// Headers are kept in canonical order (by lowercase name, names unique) so
/// that equal contexts encode identically and key the same authority.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct CredentialRoutingContext {
    url: Arc<str>,
    headers: Arc<[CredentialRoutingHeader]>,
}

impl CredentialRoutingContext {
    pub fn try_new(
        url: impl Into<Arc<str>>,
        headers: impl IntoIterator<Item = (Arc<str>, Arc<str>)>,
    ) -> Result<Self, ConnectorError> {
        let url = url.into();
        if url.is_empty()
            || url.len() > MAX_CREDENTIALS_ENDPOINT_BYTES
            || !(url.starts_with("https://") || url.starts_with("http://"))
            || url.bytes().any(|byte| {
                byte.is_ascii_control() || byte.is_ascii_whitespace() || !byte.is_ascii()
            })
        {
            return Err(invalid("credential routing address"));
        }
        let mut headers = headers
            .into_iter()
            .map(|(name, value)| CredentialRoutingHeader { name, value })
            .collect::<Vec<_>>();
        if headers.len() > MAX_CREDENTIAL_ROUTING_HEADERS {
            return Err(exhausted("credential routing header set"));
        }
        let mut total = 0_usize;
        for header in &headers {
            if !is_routing_header_name(&header.name) || !is_routing_header_value(&header.value) {
                return Err(invalid("credential routing header"));
            }
            total = total.saturating_add(header.name.len() + header.value.len());
        }
        if total > MAX_CREDENTIAL_ROUTING_HEADER_BYTES {
            return Err(exhausted("credential routing header bytes"));
        }
        headers.sort();
        if headers.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(invalid("duplicate credential routing header"));
        }
        Ok(Self {
            url,
            headers: headers.into(),
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn headers(&self) -> &[CredentialRoutingHeader] {
        &self.headers
    }
}

impl std::fmt::Debug for CredentialRoutingContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialRoutingContext")
            .field("url", &self.url)
            .field("headers", &self.headers)
            .finish()
    }
}

/// A lowercase HTTP token (RFC 9110 `tchar`).
fn is_routing_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"!#$%&'*+-.^_`|~".contains(&byte)
        })
}

/// Visible ASCII, spaces and tabs only: no control character can split a
/// header (RFC 9110 field value, ASCII subset).
fn is_routing_header_value(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte == b'\t' || (b' '..=b'~').contains(&byte))
}

/// The exact table a load-table acquisition names.
///
/// The uuid travels with the identity rather than being re-derived: a later
/// response that answered for a different table would otherwise install
/// material for the wrong authority (CAD-1 D11b).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CredentialLoadTableDelegation {
    namespace: Vec<Arc<str>>,
    table: Arc<str>,
    table_uuid: Arc<str>,
}

impl CredentialLoadTableDelegation {
    pub fn try_new(
        namespace: Vec<Arc<str>>,
        table: Arc<str>,
        table_uuid: Arc<str>,
    ) -> Result<Self, ConnectorError> {
        if namespace.is_empty() || namespace.len() > MAX_CREDENTIAL_NAMESPACE_LEVELS {
            return Err(exhausted("credential lease load-table namespace"));
        }
        if namespace
            .iter()
            .any(|level| level.is_empty() || level.len() > MAX_CREDENTIAL_IDENTIFIER_BYTES)
        {
            return Err(invalid("credential lease load-table namespace level"));
        }
        if table.is_empty() || table.len() > MAX_CREDENTIAL_IDENTIFIER_BYTES {
            return Err(invalid("credential lease load-table name"));
        }
        if table_uuid.is_empty() || table_uuid.len() > MAX_CREDENTIAL_IDENTIFIER_BYTES {
            return Err(invalid("credential lease load-table uuid"));
        }
        Ok(Self {
            namespace,
            table,
            table_uuid,
        })
    }

    pub fn namespace(&self) -> &[Arc<str>] {
        &self.namespace
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }
}

impl CredentialLeaseDescriptor {
    pub fn try_new(
        lease_id: CredentialLeaseId,
        epoch: u64,
        owner: CatalogHandle,
        provider: CredentialLeaseProvider,
        mut prefixes: Vec<StorageCredentialScopePrefix>,
        not_after_unix_ms: u64,
        refresh_capable: bool,
        renewal_path: Option<CredentialRenewalPath>,
        storage_access_domain_id: StorageAccessDomainId,
    ) -> Result<Self, ConnectorError> {
        if epoch == 0 {
            return Err(invalid("credential lease epoch"));
        }
        if prefixes.is_empty() || prefixes.len() > MAX_CREDENTIAL_LEASE_PREFIXES {
            return Err(exhausted("credential lease prefix set"));
        }
        prefixes.sort();
        if prefixes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("duplicate credential lease prefix"));
        }
        if not_after_unix_ms == 0 {
            return Err(invalid("credential lease expiration"));
        }
        Ok(Self {
            lease_id,
            epoch,
            owner,
            provider,
            prefixes,
            not_after_unix_ms,
            refresh_capable,
            renewal_path,
            storage_access_domain_id,
        })
    }

    /// The advertised acquisition path for this scope, when there is one.
    ///
    /// Its absence is meaningful rather than incidental: a catalog that
    /// advertises no path offers no renewal, and a consumer holding such a
    /// lease is the seeded-without-renewal shape (CAD-1 D11).
    pub fn renewal_path(&self) -> Option<&CredentialRenewalPath> {
        self.renewal_path.as_ref()
    }

    /// The advertised acquisition address, when that is the path this lease
    /// carries.
    pub fn credentials_endpoint(&self) -> Option<&str> {
        match &self.renewal_path {
            Some(CredentialRenewalPath::CredentialsEndpoint(routing)) => Some(routing.url()),
            _ => None,
        }
    }

    pub const fn lease_id(&self) -> CredentialLeaseId {
        self.lease_id
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn owner(&self) -> &CatalogHandle {
        &self.owner
    }

    pub const fn provider(&self) -> CredentialLeaseProvider {
        self.provider
    }

    pub fn prefixes(&self) -> &[StorageCredentialScopePrefix] {
        &self.prefixes
    }

    pub const fn not_after_unix_ms(&self) -> u64 {
        self.not_after_unix_ms
    }

    pub const fn refresh_capable(&self) -> bool {
        self.refresh_capable
    }

    pub const fn storage_access_domain_id(&self) -> StorageAccessDomainId {
        self.storage_access_domain_id
    }

    /// Refresh may advance only epoch and expiration. Any owner, provider,
    /// scope, or access-domain change is a new attempt, never a refresh.
    pub fn has_same_refresh_scope(&self, other: &Self) -> bool {
        self.lease_id == other.lease_id
            && self.owner == other.owner
            && self.provider == other.provider
            && self.prefixes == other.prefixes
            && self.refresh_capable == other.refresh_capable
            && self.storage_access_domain_id == other.storage_access_domain_id
    }
}

fn invalid(subject: &'static str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        format!("invalid {subject}"),
    )
}

fn exhausted(subject: &'static str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        format!("{subject} exceeds configured bounds"),
    )
}

fn validate_secret_scalar(value: &str) -> Result<(), ConnectorError> {
    if value.is_empty() || value.len() > MAX_CREDENTIAL_LEASE_SECRET_SCALAR_BYTES {
        return Err(invalid("credential lease secret scalar"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU8;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{
        ConnectorVendedCredentialLeaseCollectionPort, ConnectorVendedCredentialLeaseSink,
        ConnectorVendedS3CredentialLeaseRefresher, CredentialAcquisitionOwnerId,
        CredentialLeaseDescriptor, CredentialLeaseId, CredentialLeaseProvider,
        CredentialLoadTableDelegation, CredentialRenewalPath, CredentialRoutingContext,
        MAX_CREDENTIAL_LEASE_PREFIXES, MAX_CREDENTIAL_ROUTING_HEADER_BYTES,
        MAX_CREDENTIAL_ROUTING_HEADERS, MAX_CREDENTIALS_ENDPOINT_BYTES,
        VendedS3CredentialLeaseContribution, VendedS3CredentialLeaseEntry,
        VendedS3CredentialLeaseRefresh, VendedS3CredentialRefreshCallPolicy,
        VendedS3CredentialRefreshDispatch, VendedS3CredentialRefreshDispatchGuard,
    };
    use crate::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogVersion, ConnectorError, ConnectorInstanceId,
        ConnectorProviderId, CredentialConsumerRole, StorageAccessDomainId,
        StorageCredentialScopePrefix,
    };
    use novarocks_secret::SecretValue;

    fn owner() -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::parse("warehouse").expect("catalog"),
            CatalogVersion::from_bytes([7; 32]),
        )
    }

    fn prefix(value: &str) -> StorageCredentialScopePrefix {
        StorageCredentialScopePrefix::try_from_normalized(value).expect("prefix")
    }

    struct ProviderLocalRefresher;

    struct PermittedDispatch;

    impl VendedS3CredentialRefreshDispatchGuard for PermittedDispatch {
        fn provider_dispatch(&self) -> VendedS3CredentialRefreshDispatch {
            VendedS3CredentialRefreshDispatch::Permitted
        }
    }

    impl ConnectorVendedS3CredentialLeaseRefresher for ProviderLocalRefresher {
        fn refresh_vended_s3_credentials(
            &self,
            _policy: VendedS3CredentialRefreshCallPolicy,
        ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError> {
            panic!("the capability is not invoked by this construction test")
        }
    }

    fn descriptor(
        epoch: u64,
        prefixes: Vec<StorageCredentialScopePrefix>,
    ) -> CredentialLeaseDescriptor {
        CredentialLeaseDescriptor::try_new(
            CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
            epoch,
            owner(),
            CredentialLeaseProvider::S3,
            prefixes,
            10,
            true,
            None,
            StorageAccessDomainId::from_bytes([9; 32]),
        )
        .expect("descriptor")
    }

    #[test]
    fn refresh_call_policy_requires_a_live_budget_and_nonbusy_retry() {
        assert!(
            VendedS3CredentialRefreshCallPolicy::try_new(
                Duration::ZERO,
                NonZeroU8::new(1).expect("nonzero attempts"),
                Duration::ZERO,
                Arc::new(PermittedDispatch),
            )
            .is_err()
        );
        assert!(
            VendedS3CredentialRefreshCallPolicy::try_new(
                Duration::from_millis(1),
                NonZeroU8::new(2).expect("nonzero attempts"),
                Duration::ZERO,
                Arc::new(PermittedDispatch),
            )
            .is_err()
        );

        let policy = VendedS3CredentialRefreshCallPolicy::try_new(
            Duration::from_millis(20),
            NonZeroU8::new(2).expect("nonzero attempts"),
            Duration::from_millis(1),
            Arc::new(PermittedDispatch),
        )
        .expect("valid bounded policy");
        assert_eq!(policy.max_attempts().get(), 2);
    }

    #[test]
    fn descriptor_canonicalizes_prefixes_and_refresh_scope_excludes_epoch_and_expiry() {
        let first = descriptor(1, vec![prefix("s3://bucket/z"), prefix("s3://bucket/a")]);
        assert_eq!(first.prefixes()[0].as_str(), "s3://bucket/a");
        let second = CredentialLeaseDescriptor::try_new(
            first.lease_id(),
            2,
            owner(),
            CredentialLeaseProvider::S3,
            first.prefixes().to_vec(),
            20,
            true,
            None,
            StorageAccessDomainId::from_bytes([9; 32]),
        )
        .expect("refresh descriptor");
        assert!(first.has_same_refresh_scope(&second));
    }

    #[test]
    fn a_provider_local_refresher_does_not_require_a_public_endpoint() {
        let contribution = VendedS3CredentialLeaseContribution::try_new(
            vec![
                VendedS3CredentialLeaseEntry::try_new(
                    prefix("s3://bucket/table"),
                    10,
                    SecretValue::new("access"),
                    SecretValue::new("secret"),
                    SecretValue::new("token"),
                )
                .expect("entry"),
            ],
            None,
        )
        .expect("contribution")
        .with_refresher(
            CredentialAcquisitionOwnerId::new(),
            Arc::new(ProviderLocalRefresher),
        )
        .expect("provider-local refresh needs no fabricated endpoint");

        assert!(contribution.refresh_endpoint().is_none());
        assert!(contribution.refresher().is_some());
    }

    #[test]
    fn descriptor_rejects_empty_duplicate_and_overbound_prefix_sets() {
        assert!(
            CredentialLeaseDescriptor::try_new(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                1,
                owner(),
                CredentialLeaseProvider::S3,
                vec![],
                10,
                false,
                None,
                StorageAccessDomainId::from_bytes([9; 32]),
            )
            .is_err()
        );
        assert!(
            CredentialLeaseDescriptor::try_new(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                1,
                owner(),
                CredentialLeaseProvider::S3,
                vec![prefix("s3://bucket/a"), prefix("s3://bucket/a")],
                10,
                false,
                None,
                StorageAccessDomainId::from_bytes([9; 32]),
            )
            .is_err()
        );
        let prefixes = (0..=MAX_CREDENTIAL_LEASE_PREFIXES)
            .map(|index| prefix(&format!("s3://bucket/{index}")))
            .collect();
        assert!(
            CredentialLeaseDescriptor::try_new(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                1,
                owner(),
                CredentialLeaseProvider::S3,
                prefixes,
                10,
                false,
                None,
                StorageAccessDomainId::from_bytes([9; 32]),
            )
            .is_err()
        );
    }

    struct RecordingVendedSink {
        seen: Mutex<Vec<(CatalogHandle, usize, Option<String>)>>,
    }

    impl ConnectorVendedCredentialLeaseSink for RecordingVendedSink {
        fn offer_vended_s3_credential_lease(
            &self,
            catalog_properties: &CatalogProperties,
            contribution: VendedS3CredentialLeaseContribution,
        ) -> Result<(), ConnectorError> {
            let (entries, renewal_path) = contribution.into_parts();
            self.seen.lock().expect("record sink").push((
                catalog_properties.handle().clone(),
                entries.len(),
                match renewal_path {
                    Some(CredentialRenewalPath::CredentialsEndpoint(routing)) => {
                        Some(routing.url().to_string())
                    }
                    Some(CredentialRenewalPath::LoadTableDelegation { table, .. }) => {
                        Some(format!("load-table:{}", table.table()))
                    }
                    None => None,
                },
            ));
            Ok(())
        }
    }

    fn vended_catalog_properties() -> CatalogProperties {
        CatalogProperties::new(
            owner(),
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

    #[test]
    fn collection_port_forwards_exact_catalog_properties_and_move_only_contribution() {
        let sink = Arc::new(RecordingVendedSink {
            seen: Mutex::new(Vec::new()),
        });
        let port = ConnectorVendedCredentialLeaseCollectionPort::new(
            vended_catalog_properties(),
            sink.clone(),
        );
        let contribution = VendedS3CredentialLeaseContribution::try_new(
            vec![
                VendedS3CredentialLeaseEntry::try_new(
                    prefix("s3://bucket/table"),
                    100,
                    SecretValue::new("access-canary"),
                    SecretValue::new("secret-canary"),
                    SecretValue::new("token-canary"),
                )
                .expect("entry"),
            ],
            Some(CredentialRenewalPath::CredentialsEndpoint(routing(
                "https://catalog.example.test/v1/credentials",
                &[],
            ))),
        )
        .expect("contribution");

        port.offer_vended_s3_credential_lease(contribution)
            .expect("offer contribution");

        let seen = sink.seen.lock().expect("record sink");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, owner());
        assert_eq!(seen[0].1, 1);
        assert_eq!(
            seen[0].2.as_deref(),
            Some("https://catalog.example.test/v1/credentials")
        );
    }

    fn header(name: &str, value: &str) -> (Arc<str>, Arc<str>) {
        (Arc::from(name), Arc::from(value))
    }

    fn routing(url: &str, headers: &[(&str, &str)]) -> CredentialRoutingContext {
        CredentialRoutingContext::try_new(
            url,
            headers.iter().map(|(name, value)| header(name, value)),
        )
        .expect("routing context")
    }

    #[test]
    fn routing_headers_are_kept_in_canonical_order_so_equal_contexts_compare_equal() {
        let first = routing(
            "https://catalog.example.test/v1/p1/credentials",
            &[("x-project-id", "p1"), ("polaris-realm", "r1")],
        );
        let second = routing(
            "https://catalog.example.test/v1/p1/credentials",
            &[("polaris-realm", "r1"), ("x-project-id", "p1")],
        );
        let names = first
            .headers()
            .iter()
            .map(|header| header.name())
            .collect::<Vec<_>>();
        assert_eq!(names, ["polaris-realm", "x-project-id"]);
        assert_eq!(first.headers()[0].value(), "r1");
        assert_eq!(first, second);
        assert_eq!(
            first.url(),
            "https://catalog.example.test/v1/p1/credentials"
        );

        // Any single routing fact makes a different acquisition.
        assert_ne!(
            first,
            routing(
                "https://catalog.example.test/v1/p1/credentials",
                &[("x-project-id", "p2"), ("polaris-realm", "r1")],
            )
        );
        assert_ne!(
            first,
            routing(
                "https://catalog.example.test/v1/p2/credentials",
                &[("x-project-id", "p1"), ("polaris-realm", "r1")],
            )
        );
    }

    #[test]
    fn routing_context_refuses_malformed_and_duplicate_headers() {
        let url = "https://catalog.example.test/v1/credentials";
        for (name, value) in [
            ("", "v"),
            ("X-Project-Id", "v"),
            ("x project", "v"),
            ("x-project:", "v"),
            ("x-project", "line\r\nbreak"),
            ("x-project", "caf\u{e9}"),
        ] {
            assert!(
                CredentialRoutingContext::try_new(url, [header(name, value)]).is_err(),
                "header {name:?} must be refused"
            );
        }
        assert!(
            CredentialRoutingContext::try_new(
                url,
                [header("x-project-id", "a"), header("x-project-id", "b")],
            )
            .is_err()
        );
        // A tab and a space are legal inside a field value.
        assert!(CredentialRoutingContext::try_new(url, [header("x-tag", "a b\tc")]).is_ok());
    }

    #[test]
    fn routing_context_bounds_header_count_and_bytes() {
        let url = "https://catalog.example.test/v1/credentials";
        let at_bound = (0..MAX_CREDENTIAL_ROUTING_HEADERS)
            .map(|index| header(&format!("x-h{index}"), "v"))
            .collect::<Vec<_>>();
        assert_eq!(
            CredentialRoutingContext::try_new(url, at_bound)
                .expect("count bound is inclusive")
                .headers()
                .len(),
            MAX_CREDENTIAL_ROUTING_HEADERS
        );
        let over_count = (0..=MAX_CREDENTIAL_ROUTING_HEADERS)
            .map(|index| header(&format!("x-h{index}"), "v"))
            .collect::<Vec<_>>();
        assert!(CredentialRoutingContext::try_new(url, over_count).is_err());

        // Names and values are summed together.
        let name = "x-big";
        let fits = "v".repeat(MAX_CREDENTIAL_ROUTING_HEADER_BYTES - name.len());
        assert!(CredentialRoutingContext::try_new(url, [header(name, &fits)]).is_ok());
        let over = "v".repeat(MAX_CREDENTIAL_ROUTING_HEADER_BYTES - name.len() + 1);
        assert!(CredentialRoutingContext::try_new(url, [header(name, &over)]).is_err());
        let half = "v".repeat(MAX_CREDENTIAL_ROUTING_HEADER_BYTES / 2);
        assert!(
            CredentialRoutingContext::try_new(url, [header("x-a", &half), header("x-b", &half)])
                .is_err()
        );
    }

    #[test]
    fn routing_address_must_be_a_bounded_http_url() {
        for url in [
            "",
            "catalog.example.test/v1/credentials",
            "ftp://catalog.example.test/v1/credentials",
            "https://catalog.example.test/v1/cred entials",
            "https://catalog.example.test/v1/\ncredentials",
            "https://catalog.example.test/v1/cr\u{e9}dentials",
        ] {
            assert!(
                CredentialRoutingContext::try_new(url, []).is_err(),
                "address {url:?} must be refused"
            );
        }
        assert!(
            CredentialRoutingContext::try_new("http://127.0.0.1:8181/v1/credentials", []).is_ok()
        );

        let base = "https://catalog.example.test/";
        let at_bound = format!(
            "{base}{}",
            "a".repeat(MAX_CREDENTIALS_ENDPOINT_BYTES - base.len())
        );
        assert!(CredentialRoutingContext::try_new(at_bound.as_str(), []).is_ok());
        let over = format!("{at_bound}a");
        assert!(CredentialRoutingContext::try_new(over.as_str(), []).is_err());
    }

    #[test]
    fn debug_of_a_renewal_path_never_prints_a_routing_header_value() {
        let routing = routing(
            "https://catalog.example.test/v1/credentials",
            &[("x-project-id", "value-canary")],
        );
        let delegation = CredentialRenewalPath::LoadTableDelegation {
            routing: routing.clone(),
            table: CredentialLoadTableDelegation::try_new(
                vec![Arc::from("sales")],
                Arc::from("orders"),
                Arc::from("8f1d0c6e-0000-4000-8000-000000000001"),
            )
            .expect("delegation"),
        };
        for rendered in [
            format!("{routing:?}"),
            format!("{:?}", routing.headers()[0]),
            format!("{delegation:?}"),
            format!(
                "{:?}",
                CredentialRenewalPath::CredentialsEndpoint(routing.clone())
            ),
        ] {
            assert!(rendered.contains("x-project-id"), "{rendered}");
            assert!(!rendered.contains("value-canary"), "{rendered}");
        }
        assert_eq!(delegation.routing(), &routing);
    }

    #[test]
    fn the_endpoint_accessors_answer_only_for_the_credentials_endpoint_path() {
        let endpoint = routing("https://catalog.example.test/v1/credentials", &[]);
        let announced = CredentialLeaseDescriptor::try_new(
            CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
            1,
            owner(),
            CredentialLeaseProvider::S3,
            vec![prefix("s3://bucket/a")],
            10,
            true,
            Some(CredentialRenewalPath::CredentialsEndpoint(endpoint.clone())),
            StorageAccessDomainId::from_bytes([9; 32]),
        )
        .expect("descriptor");
        assert_eq!(
            announced.credentials_endpoint(),
            Some("https://catalog.example.test/v1/credentials")
        );
        let delegated = CredentialLeaseDescriptor::try_new(
            CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
            1,
            owner(),
            CredentialLeaseProvider::S3,
            vec![prefix("s3://bucket/a")],
            10,
            true,
            Some(CredentialRenewalPath::LoadTableDelegation {
                routing: endpoint,
                table: CredentialLoadTableDelegation::try_new(
                    vec![Arc::from("sales")],
                    Arc::from("orders"),
                    Arc::from("8f1d0c6e-0000-4000-8000-000000000001"),
                )
                .expect("delegation"),
            }),
            StorageAccessDomainId::from_bytes([9; 32]),
        )
        .expect("descriptor");
        assert_eq!(delegated.credentials_endpoint(), None);
        assert_eq!(
            delegated.renewal_path().map(|path| path.routing().url()),
            Some("https://catalog.example.test/v1/credentials")
        );
    }
}
