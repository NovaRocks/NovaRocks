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

use std::sync::Arc;

use novarocks_secret::SecretValue;

use super::{
    CatalogHandle, CatalogProperties, ConnectorError, ConnectorErrorKind, StorageAccessDomainId,
    StorageCredentialScopePrefix,
};

pub const MAX_CREDENTIAL_LEASES_PER_QUERY: usize = 64;
pub const MAX_CREDENTIAL_LEASE_PREFIXES: usize = 64;
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
    #[allow(clippy::too_many_arguments)]
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
    refresh_endpoint: Option<Arc<str>>,
    refresher: Option<Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>>,
}

impl VendedS3CredentialLeaseContribution {
    pub fn try_new(
        mut entries: Vec<VendedS3CredentialLeaseEntry>,
        refresh_endpoint: Option<Arc<str>>,
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
        if refresh_endpoint
            .as_deref()
            .is_some_and(|endpoint| endpoint.is_empty())
        {
            return Err(invalid("vended S3 credential refresh endpoint"));
        }
        Ok(Self {
            entries,
            refresh_endpoint,
            refresher: None,
        })
    }

    /// Attach the provider-owned, FE-local source for a later refresh. The
    /// source is a capability, not a wire field or a table property; it can be
    /// consumed only by the query-attempt collector that receives this
    /// response-local contribution.
    pub fn with_refresher(
        mut self,
        refresher: Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>,
    ) -> Result<Self, ConnectorError> {
        self.refresher = Some(refresher);
        Ok(self)
    }

    pub fn entries(&self) -> &[VendedS3CredentialLeaseEntry] {
        &self.entries
    }

    pub fn refresh_endpoint(&self) -> Option<&str> {
        self.refresh_endpoint.as_deref()
    }

    pub fn refresher(&self) -> Option<&Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>> {
        self.refresher.as_ref()
    }

    /// Transfer the complete response-local contribution to the sole
    /// query-attempt collector.
    pub fn into_parts(self) -> (Vec<VendedS3CredentialLeaseEntry>, Option<Arc<str>>) {
        (self.entries, self.refresh_endpoint)
    }

    /// Transfer both the confidential entries and the provider refresh source
    /// to the only query-attempt collector. Existing callers that do not own a
    /// refresher should use [`Self::into_parts`].
    pub fn into_parts_with_refresher(
        self,
    ) -> (
        Vec<VendedS3CredentialLeaseEntry>,
        Option<Arc<str>>,
        Option<Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>>,
    ) {
        (self.entries, self.refresh_endpoint, self.refresher)
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

/// FE-local provider capability for refreshing an already-admitted vended S3
/// lease. Implementations must use their own catalog identity and may never
/// be serialized or attached to a BE request.
pub trait ConnectorVendedS3CredentialLeaseRefresher: Send + Sync {
    fn refresh_vended_s3_credentials(
        &self,
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError>;
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

    /// Wraps decoded wire scalars before they can leave the confidential
    /// protocol boundary as ordinary strings.
    pub fn try_new_from_wire_scalars(
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

    /// Exposes the scalar values only to the codec that writes the TLS-only
    /// confidential lifecycle carrier.
    pub fn s3_secret_scalars(&self) -> (&str, &str, &str) {
        (
            self.access_key_id.expose_secret(),
            self.secret_access_key.expose_secret(),
            self.session_token.expose_secret(),
        )
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
    storage_access_domain_id: StorageAccessDomainId,
}

impl CredentialLeaseDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        lease_id: CredentialLeaseId,
        epoch: u64,
        owner: CatalogHandle,
        provider: CredentialLeaseProvider,
        mut prefixes: Vec<StorageCredentialScopePrefix>,
        not_after_unix_ms: u64,
        refresh_capable: bool,
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
            storage_access_domain_id,
        })
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
    use std::sync::{Arc, Mutex};

    use super::{
        ConnectorVendedCredentialLeaseCollectionPort, ConnectorVendedCredentialLeaseSink,
        ConnectorVendedS3CredentialLeaseRefresher, CredentialLeaseDescriptor, CredentialLeaseId,
        CredentialLeaseProvider, MAX_CREDENTIAL_LEASE_PREFIXES,
        VendedS3CredentialLeaseContribution, VendedS3CredentialLeaseEntry,
        VendedS3CredentialLeaseRefresh,
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

    impl ConnectorVendedS3CredentialLeaseRefresher for ProviderLocalRefresher {
        fn refresh_vended_s3_credentials(
            &self,
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
            StorageAccessDomainId::from_bytes([9; 32]),
        )
        .expect("descriptor")
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
        .with_refresher(Arc::new(ProviderLocalRefresher))
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
            let (entries, refresh_endpoint) = contribution.into_parts();
            self.seen.lock().expect("record sink").push((
                catalog_properties.handle().clone(),
                entries.len(),
                refresh_endpoint.map(|endpoint| endpoint.to_string()),
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
            Some(Arc::from("https://catalog.example.test/v1/credentials")),
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
}
