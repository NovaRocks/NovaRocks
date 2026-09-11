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

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use novarocks_secret::SecretValue;

use super::{
    CatalogHandle, CatalogProperties, ConnectorError, ConnectorErrorKind,
    ConnectorRequestResources, ConnectorVendedCredentialLeaseCollectionPort,
    ConnectorVendedCredentialLeaseSink, CredentialLeaseId, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, MAX_STORAGE_CREDENTIAL_SCOPE_PREFIX_BYTES,
    StorageAccessDomainId, StorageCredentialScopePrefix,
};

pub trait ConnectorCancellation: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

/// One non-wire, attempt-local sidecar shared by every clone of an admitted
/// request context. Providers may retain private frozen planning state here,
/// but the sidecar itself has no serialization or Debug path and is released
/// with the final owning request context.
#[derive(Clone, Default)]
pub struct ConnectorRequestScope {
    extensions: Arc<Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>>,
}

impl ConnectorRequestScope {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the one provider-private extension of type `T` for this attempt.
    /// Construction runs while holding only this short in-memory registry lock;
    /// extensions must not perform I/O in their constructors.
    pub fn extension_or_insert_with<T>(&self, make: impl FnOnce() -> T) -> Arc<T>
    where
        T: Any + Send + Sync,
    {
        let mut extensions = self
            .extensions
            .lock()
            .expect("connector request scope lock");
        if let Some(existing) = extensions.get(&TypeId::of::<T>()) {
            return Arc::clone(existing)
                .downcast::<T>()
                .expect("connector request scope extension type");
        }
        let extension = Arc::new(make());
        extensions.insert(
            TypeId::of::<T>(),
            Arc::clone(&extension) as Arc<dyn Any + Send + Sync>,
        );
        extension
    }
}

/// A non-secret target presented to the query-scoped storage capability.
///
/// The owner is the exact catalog generation already carried by scan/writer
/// handles. It deliberately contains neither a credential reference, an
/// access domain, nor any query identity: the query attempt and its lease
/// domain are selected only inside the process-local resolver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageAccessRequest {
    owner: CatalogHandle,
    location: Arc<str>,
}

impl StorageAccessRequest {
    pub fn try_new(
        owner: CatalogHandle,
        location: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        let location = location.as_ref();
        if location.is_empty()
            || location.len() > MAX_STORAGE_CREDENTIAL_SCOPE_PREFIX_BYTES
            || !location.is_ascii()
            || location.bytes().any(|byte| byte.is_ascii_whitespace())
            || location
                .bytes()
                .any(|byte| matches!(byte, b'?' | b'#' | b'\\'))
        {
            return Err(invalid_storage_route());
        }
        let parsed = url::Url::parse(location).map_err(|_| invalid_storage_route())?;
        if parsed.scheme() != "s3"
            || parsed.host_str().is_none_or(str::is_empty)
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.as_str() != location
        {
            return Err(invalid_storage_route());
        }
        Ok(Self {
            owner,
            location: Arc::from(location),
        })
    }

    pub fn owner(&self) -> &CatalogHandle {
        &self.owner
    }

    pub fn location(&self) -> &str {
        &self.location
    }
}

/// Process-local, query-attempt-bound storage access authority.
///
/// The native wire never contains this trait object. A request context receives
/// it only after the BE has admitted the exact query attempt and captured its
/// lifecycle lease state.
pub trait ConnectorStorageResolver: Send + Sync {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError>;
}

/// One successful vended S3 selection. The material is redacted from Debug
/// and never derives a serialization trait.
#[derive(Clone)]
pub struct ResolvedVendedS3Access {
    storage_access_domain_id: StorageAccessDomainId,
    lease_id: CredentialLeaseId,
    epoch: u64,
    matched_prefix: StorageCredentialScopePrefix,
    not_after_unix_ms: u64,
    access_key_id: SecretValue,
    secret_access_key: SecretValue,
    session_token: SecretValue,
}

impl ResolvedVendedS3Access {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_access_domain_id: StorageAccessDomainId,
        lease_id: CredentialLeaseId,
        epoch: u64,
        matched_prefix: StorageCredentialScopePrefix,
        not_after_unix_ms: u64,
        access_key_id: SecretValue,
        secret_access_key: SecretValue,
        session_token: SecretValue,
    ) -> Self {
        Self {
            storage_access_domain_id,
            lease_id,
            epoch,
            matched_prefix,
            not_after_unix_ms,
            access_key_id,
            secret_access_key,
            session_token,
        }
    }

    pub const fn storage_access_domain_id(&self) -> StorageAccessDomainId {
        self.storage_access_domain_id
    }

    pub const fn lease_id(&self) -> CredentialLeaseId {
        self.lease_id
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn matched_prefix(&self) -> &StorageCredentialScopePrefix {
        &self.matched_prefix
    }

    pub const fn not_after_unix_ms(&self) -> u64 {
        self.not_after_unix_ms
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
}

impl fmt::Debug for ResolvedVendedS3Access {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedVendedS3Access")
            .field("storage_access_domain_id", &self.storage_access_domain_id)
            .field("lease_id", &self.lease_id)
            .field("epoch", &self.epoch)
            .field("matched_prefix", &self.matched_prefix)
            .field("not_after_unix_ms", &self.not_after_unix_ms)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnectorRequestContext {
    deadline: Instant,
    cancellation: Arc<dyn ConnectorCancellation>,
    max_handle_payload_bytes: usize,
    max_total_payload_bytes: usize,
    storage_resolver: Option<Arc<dyn ConnectorStorageResolver>>,
    vended_credential_lease_sink: Option<Arc<dyn ConnectorVendedCredentialLeaseSink>>,
    vended_credential_lease_collection: Option<ConnectorVendedCredentialLeaseCollectionPort>,
    request_scope: ConnectorRequestScope,
    resources: Option<ConnectorRequestResources>,
}

/// Connector context for metadata observation and scan negotiation.
///
/// Planning never owns an execution-attempt credential collector.  Providers
/// may use the host-installed FE metadata access capability, but they cannot
/// contribute credentials to a future BE attempt through this context.
#[derive(Clone)]
pub struct ConnectorPlanningContext {
    request: ConnectorRequestContext,
}

impl ConnectorPlanningContext {
    pub fn try_from_request(request: ConnectorRequestContext) -> Result<Self, ConnectorError> {
        if request.storage_resolver.is_some()
            || request.vended_credential_lease_sink.is_some()
            || request.vended_credential_lease_collection.is_some()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Connector planning context cannot own attempt storage or credential capabilities",
            ));
        }
        Ok(Self { request })
    }

    pub const fn request(&self) -> &ConnectorRequestContext {
        &self.request
    }
}

/// Connector context for instantiating one execution attempt from frozen scan
/// semantics.  Attempt-only SPI accepts this type instead of a planning
/// context, so a provider cannot use reacquisition to renegotiate metadata,
/// projection, predicates, limits, or snapshots.
#[derive(Clone)]
pub struct ConnectorAttemptContext {
    request: ConnectorRequestContext,
}

impl ConnectorAttemptContext {
    pub fn from_admitted_request(request: ConnectorRequestContext) -> Self {
        Self { request }
    }

    pub const fn request(&self) -> &ConnectorRequestContext {
        &self.request
    }
}

impl ConnectorRequestContext {
    pub fn try_new(
        deadline: Instant,
        cancellation: Arc<dyn ConnectorCancellation>,
        max_handle_payload_bytes: usize,
        max_total_payload_bytes: usize,
    ) -> Result<Self, ConnectorError> {
        if max_handle_payload_bytes == 0
            || max_total_payload_bytes == 0
            || max_handle_payload_bytes > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES
            || max_total_payload_bytes > MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES
            || max_total_payload_bytes < max_handle_payload_bytes
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "invalid connector payload budget",
            ));
        }
        Ok(Self {
            deadline,
            cancellation,
            max_handle_payload_bytes,
            max_total_payload_bytes,
            storage_resolver: None,
            vended_credential_lease_sink: None,
            vended_credential_lease_collection: None,
            request_scope: ConnectorRequestScope::new(),
            resources: None,
        })
    }

    /// Bind this context to the reservation-owned attempt sidecar. A retry
    /// receives a newly minted scope, so no request-bound provider state can
    /// cross an attempt boundary.
    pub fn with_request_scope(mut self, request_scope: ConnectorRequestScope) -> Self {
        self.request_scope = request_scope;
        self
    }

    /// Installs a local capability after query admission. It is intentionally
    /// a builder step rather than a wire constructor argument.
    pub fn with_storage_resolver(
        mut self,
        storage_resolver: Arc<dyn ConnectorStorageResolver>,
    ) -> Self {
        self.storage_resolver = Some(storage_resolver);
        self
    }

    /// Installs the host ledger after this query or task has been admitted.
    pub fn with_resources(mut self, resources: ConnectorRequestResources) -> Self {
        self.resources = Some(resources);
        self
    }

    /// Installs the FE-local collector that owns vended metadata-response
    /// credentials for one query attempt. This remains a request-local
    /// capability and is never encoded on a wire. Query materialization later
    /// decorates its cloned context with each exact catalog generation.
    pub fn with_vended_credential_lease_sink(
        mut self,
        sink: Arc<dyn ConnectorVendedCredentialLeaseSink>,
    ) -> Self {
        self.vended_credential_lease_sink = Some(sink);
        self.vended_credential_lease_collection = None;
        self
    }

    /// Decorate one metadata call with the exact catalog properties acquired
    /// by query materialization. Calling this without the query-wide sink is
    /// invalid; a vended provider otherwise remains fail-closed.
    pub fn with_vended_credential_lease_collection(
        mut self,
        catalog_properties: CatalogProperties,
    ) -> Result<Self, ConnectorError> {
        let sink = self.vended_credential_lease_sink.clone().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "vended credential lease collection requires a query-attempt sink",
            )
        })?;
        self.vended_credential_lease_collection = Some(
            ConnectorVendedCredentialLeaseCollectionPort::new(catalog_properties, sink),
        );
        Ok(self)
    }

    /// Retain only the already-admitted storage resolver for a terminal
    /// operation. The terminal path may reload catalog metadata, but it must
    /// not ingest a second vended response after the attempt's descriptor set
    /// has been frozen and sent over Init.
    pub fn without_vended_credential_lease_sink(mut self) -> Self {
        self.vended_credential_lease_sink = None;
        self.vended_credential_lease_collection = None;
        self
    }

    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn cancellation(&self) -> &Arc<dyn ConnectorCancellation> {
        &self.cancellation
    }

    pub const fn max_handle_payload_bytes(&self) -> usize {
        self.max_handle_payload_bytes
    }

    pub const fn max_total_payload_bytes(&self) -> usize {
        self.max_total_payload_bytes
    }

    pub fn storage_resolver(&self) -> Option<&Arc<dyn ConnectorStorageResolver>> {
        self.storage_resolver.as_ref()
    }

    /// Runtime readers fail closed when the host did not install a ledger;
    /// absence never means an unlimited or untracked request.
    pub fn resources(&self) -> Result<&ConnectorRequestResources, ConnectorError> {
        self.resources.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector request resources were not installed after admission",
            )
        })
    }

    /// The base query-attempt sink before a metadata call decorates it with
    /// one exact catalog generation.
    pub fn vended_credential_lease_sink(
        &self,
    ) -> Option<&Arc<dyn ConnectorVendedCredentialLeaseSink>> {
        self.vended_credential_lease_sink.as_ref()
    }

    /// The vended credential contribution port, when this request was created
    /// for an admitted distributed query attempt.
    pub fn vended_credential_lease_collection(
        &self,
    ) -> Option<&ConnectorVendedCredentialLeaseCollectionPort> {
        self.vended_credential_lease_collection.as_ref()
    }

    /// Obtain provider-private state shared by all clones of this admitted
    /// attempt context. This is local process state only; it must not be put
    /// into connector handles, plans, or any transport payload.
    pub fn request_scope_extension_or_insert_with<T>(&self, make: impl FnOnce() -> T) -> Arc<T>
    where
        T: Any + Send + Sync,
    {
        self.request_scope.extension_or_insert_with(make)
    }
}

fn invalid_storage_route() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "invalid vended storage access route",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::{
        ConnectorCancellation, ConnectorPlanningContext, ConnectorRequestContext,
        ConnectorStorageResolver, ResolvedVendedS3Access, StorageAccessRequest,
    };
    use crate::connector::{
        CatalogHandle, CatalogProperties, CatalogVersion, ConnectorError, ConnectorInstanceId,
        ConnectorRequestResources, ConnectorResourceCheckpoint, ConnectorResourceClass,
        ConnectorResourceLease, ConnectorResourceLedger, ConnectorVendedCredentialLeaseSink,
        CredentialLeaseId, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        StorageAccessDomainId, StorageCredentialScopePrefix, VendedS3CredentialLeaseContribution,
    };
    use novarocks_secret::SecretValue;

    struct Active;

    impl ConnectorCancellation for Active {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct EmptyLedger;

    struct RejectingSink;

    struct RejectingResolver;

    impl ConnectorStorageResolver for RejectingResolver {
        fn resolve_vended_s3(
            &self,
            _request: &StorageAccessRequest,
        ) -> Result<ResolvedVendedS3Access, ConnectorError> {
            unreachable!("planning context construction must reject the resolver")
        }
    }

    impl ConnectorVendedCredentialLeaseSink for RejectingSink {
        fn offer_vended_s3_credential_lease(
            &self,
            _catalog_properties: &CatalogProperties,
            _contribution: VendedS3CredentialLeaseContribution,
        ) -> Result<(), ConnectorError> {
            unreachable!("planning context construction must reject the sink")
        }
    }

    impl ConnectorResourceLedger for EmptyLedger {
        fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
            Ok(ConnectorResourceCheckpoint::new(1))
        }

        fn try_reserve(
            &self,
            _class: ConnectorResourceClass,
            _bytes: u64,
        ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
            unreachable!("this test only checks resource installation")
        }
    }

    #[test]
    fn storage_request_rejects_noncanonical_or_credentialed_location() {
        let owner = CatalogHandle::new(
            ConnectorInstanceId::parse("request-test").expect("catalog"),
            CatalogVersion::from_bytes([1; 32]),
        );
        assert!(
            StorageAccessRequest::try_new(owner.clone(), "s3://bucket/table/data.parquet").is_ok()
        );
        for location in [
            "https://bucket/table/data.parquet",
            "s3://user:secret@bucket/table/data.parquet",
            "s3://bucket/table/data.parquet?token=secret",
            "s3://bucket/table/../other",
        ] {
            assert!(
                StorageAccessRequest::try_new(owner.clone(), location).is_err(),
                "{location}"
            );
        }
    }

    #[test]
    fn resolved_access_debug_redacts_secret_material() {
        let access = ResolvedVendedS3Access::new(
            StorageAccessDomainId::from_bytes([1; 32]),
            CredentialLeaseId::try_from_bytes([2; 16]).expect("lease"),
            1,
            StorageCredentialScopePrefix::try_from_normalized("s3://bucket/table").expect("prefix"),
            42,
            SecretValue::new("access-canary"),
            SecretValue::new("secret-canary"),
            SecretValue::new("token-canary"),
        );
        let rendered = format!("{access:?}");
        assert!(!rendered.contains("access-canary"));
        assert!(!rendered.contains("secret-canary"));
        assert!(!rendered.contains("token-canary"));
    }

    #[test]
    fn request_resources_are_absent_until_the_host_installs_them() {
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(Active),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap();
        assert!(context.resources().is_err());
        let context = context.with_resources(ConnectorRequestResources::new(Arc::new(EmptyLedger)));
        assert_eq!(
            context
                .resources()
                .unwrap()
                .checkpoint()
                .unwrap()
                .sequence(),
            1
        );
    }

    #[test]
    fn planning_context_rejects_attempt_credential_collection() {
        let base = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(Active),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap();
        assert!(ConnectorPlanningContext::try_from_request(base.clone()).is_ok());
        let resolver_decorated = base
            .clone()
            .with_storage_resolver(Arc::new(RejectingResolver));
        assert!(ConnectorPlanningContext::try_from_request(resolver_decorated).is_err());
        let attempt_decorated = base.with_vended_credential_lease_sink(Arc::new(RejectingSink));
        assert!(ConnectorPlanningContext::try_from_request(attempt_decorated).is_err());
    }
}
