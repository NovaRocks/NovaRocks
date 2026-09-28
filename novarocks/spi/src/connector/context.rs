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

use super::read_stack::ConnectorSourceOperations;
use super::{
    CatalogHandle, CatalogProperties, ConnectorError, ConnectorErrorKind, ConnectorStopView,
    ConnectorVendedCredentialLeaseCollectionPort, ConnectorVendedCredentialLeaseSink,
    ConnectorVendedS3CredentialLeaseRefresher, CredentialLeaseId,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    MAX_STORAGE_CREDENTIAL_SCOPE_PREFIX_BYTES, StorageAccessDomainId, StorageCredentialScopePrefix,
};

/// A clonable operation owner may expose liveness to provider I/O without
/// exposing authorization, request resources or a mutable lifecycle handle.
pub trait ConnectorOperationControl: Send + Sync {
    fn check_active(&self) -> Result<(), ConnectorError>;
}

/// Runtime-only identity for fair scan I/O scheduling. This does not belong in
/// a connector handle or a frozen plan: placement and attempt identity are
/// known only when the backend binds the scan node.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorRangeScope {
    query_high: i64,
    query_low: i64,
    attempt: u64,
    fragment_high: i64,
    fragment_low: i64,
    node_id: i32,
}

impl ConnectorRangeScope {
    pub fn try_new(
        query_high: i64,
        query_low: i64,
        attempt: u64,
        fragment_high: i64,
        fragment_low: i64,
        node_id: i32,
    ) -> Result<Self, ConnectorError> {
        if (query_high == 0 && query_low == 0)
            || attempt == 0
            || (fragment_high == 0 && fragment_low == 0)
            || node_id < 0
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "scan range scheduling requires an exact query attempt, fragment instance and node",
            ));
        }
        Ok(Self {
            query_high,
            query_low,
            attempt,
            fragment_high,
            fragment_low,
            node_id,
        })
    }

    pub const fn parts(self) -> (i64, i64, u64, i64, i64, i32) {
        (
            self.query_high,
            self.query_low,
            self.attempt,
            self.fragment_high,
            self.fragment_low,
            self.node_id,
        )
    }
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

/// Material one consumer was handed rather than acquired.
///
/// It exists only where the resolver and the consumer are the same process:
/// the coordinator resolves against leases its own provider already produced.
/// A consumer on another node is never seeded — material does not travel — so
/// this is absent there and the authority acquires for itself (CAD-1 D1).
#[derive(Clone)]
pub struct VendedS3SeedMaterial {
    not_after_unix_ms: u64,
    access_key_id: SecretValue,
    secret_access_key: SecretValue,
    session_token: SecretValue,
}

impl VendedS3SeedMaterial {
    pub const fn new(
        not_after_unix_ms: u64,
        access_key_id: SecretValue,
        secret_access_key: SecretValue,
        session_token: SecretValue,
    ) -> Self {
        Self {
            not_after_unix_ms,
            access_key_id,
            secret_access_key,
            session_token,
        }
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

impl fmt::Debug for VendedS3SeedMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VendedS3SeedMaterial")
            .field("not_after_unix_ms", &self.not_after_unix_ms)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

/// One successful vended S3 selection.
///
/// It names a scope and how to acquire for it. Material rides along only when
/// the resolver is in the consumer's own process, and is redacted from Debug.
#[derive(Clone)]
pub struct ResolvedVendedS3Access {
    storage_access_domain_id: StorageAccessDomainId,
    lease_id: CredentialLeaseId,
    epoch: u64,
    matched_prefix: StorageCredentialScopePrefix,
    /// How this consumer acquires for itself, when the catalog advertised a
    /// path. Absent means this lease cannot renew (CAD-1 D11).
    renewal_path: Option<crate::connector::CredentialRenewalPath>,
    /// Present only when the resolver is in the consumer's own process.
    seed: Option<VendedS3SeedMaterial>,
    /// The provider capability this consumer already holds, when the resolver
    /// and the consumer share a process.
    ///
    /// The coordinator planned the query, so the catalog client that observed
    /// the response is here; it renews through that rather than by
    /// authenticating an announced path. An execution node never has one --
    /// it is a capability, not a value, and capabilities do not travel
    /// (CAD-1 D1).
    provider: Option<Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>>,
}

impl ResolvedVendedS3Access {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_access_domain_id: StorageAccessDomainId,
        lease_id: CredentialLeaseId,
        epoch: u64,
        matched_prefix: StorageCredentialScopePrefix,
        renewal_path: Option<crate::connector::CredentialRenewalPath>,
        seed: Option<VendedS3SeedMaterial>,
    ) -> Self {
        Self {
            storage_access_domain_id,
            lease_id,
            epoch,
            matched_prefix,
            renewal_path,
            seed,
            provider: None,
        }
    }

    /// Attach the in-process provider capability this consumer already holds.
    ///
    /// Only a resolver running in the consumer's own process may call this.
    pub fn with_provider(
        mut self,
        provider: Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>,
    ) -> Self {
        self.provider = Some(provider);
        self
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

    /// The acquisition path for this selection, when one was advertised.
    ///
    /// Its absence is a fact about the catalog, not a missing value: a consumer
    /// holding such a selection is seeded and cannot renew (CAD-1 D11).
    pub fn renewal_path(&self) -> Option<&crate::connector::CredentialRenewalPath> {
        self.renewal_path.as_ref()
    }

    /// Material handed over with this selection, when the resolver is in the
    /// consumer's own process.
    pub const fn seed(&self) -> Option<&VendedS3SeedMaterial> {
        self.seed.as_ref()
    }

    /// The in-process provider capability, when this consumer holds one.
    pub fn provider(&self) -> Option<&Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>> {
        self.provider.as_ref()
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
            .field("seed", &self.seed)
            .field("provider", &self.provider.is_some())
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnectorRequestContext {
    // Design: ADR-0156. Operation control and authorized access do not imply
    // task memory admission; the BE factory receives that separately.
    deadline: Instant,
    stop: ConnectorStopView,
    max_handle_payload_bytes: usize,
    max_total_payload_bytes: usize,
    storage_resolver: Option<Arc<dyn ConnectorStorageResolver>>,
    vended_credential_lease_sink: Option<Arc<dyn ConnectorVendedCredentialLeaseSink>>,
    vended_credential_lease_collection: Option<ConnectorVendedCredentialLeaseCollectionPort>,
    request_scope: ConnectorRequestScope,
    execution_source: Option<ExecutionSource>,
    fresh_catalog_observation_required: bool,
}

/// The one execution source an attempt request reads for: the exact scope
/// its I/O is scheduled under and the registry its operations are admitted
/// to. Installed together, so no request schedules I/O for a source whose
/// operations nobody can stop.
#[derive(Clone)]
struct ExecutionSource {
    range_scope: ConnectorRangeScope,
    operations: ConnectorSourceOperations,
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
            || request.execution_source.is_some()
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
        stop: ConnectorStopView,
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
            stop,
            max_handle_payload_bytes,
            max_total_payload_bytes,
            storage_resolver: None,
            vended_credential_lease_sink: None,
            vended_credential_lease_collection: None,
            request_scope: ConnectorRequestScope::new(),
            execution_source: None,
            fresh_catalog_observation_required: false,
        })
    }

    /// Bind this context to the reservation-owned attempt sidecar. A retry
    /// receives a newly minted scope, so no request-bound provider state can
    /// cross an attempt boundary.
    pub fn with_request_scope(mut self, request_scope: ConnectorRequestScope) -> Self {
        self.request_scope = request_scope;
        self
    }

    /// Binds this attempt request to the one execution source it reads for:
    /// its I/O is scheduled under `range_scope`, and every operation it
    /// starts is admitted to `operations` first, so closing the source stops
    /// them and observes their exit.
    pub fn with_execution_source(
        mut self,
        range_scope: ConnectorRangeScope,
        operations: ConnectorSourceOperations,
    ) -> Self {
        self.execution_source = Some(ExecutionSource {
            range_scope,
            operations,
        });
        self
    }

    pub fn range_scope(&self) -> Option<ConnectorRangeScope> {
        self.execution_source
            .as_ref()
            .map(|source| source.range_scope)
    }

    /// The execution source's operations; see [`Self::with_execution_source`].
    pub fn source_operations(&self) -> Option<&ConnectorSourceOperations> {
        self.execution_source
            .as_ref()
            .map(|source| &source.operations)
    }

    /// Add a child operation's independent stop without replacing the
    /// request's original cancellation authority or absolute deadline.
    pub fn with_additional_stop(mut self, stop: ConnectorStopView) -> Self {
        self.stop = ConnectorStopView::any_of(self.stop, [stop]);
        self
    }

    /// Retain a host-owned stop relay on the view itself, so file operations
    /// keep its wakeup alive even after the request object is released.
    pub fn with_stop_lifetime<T: Any + Send + Sync>(mut self, lifetime: Arc<T>) -> Self {
        self.stop = self.stop.with_lifetime(lifetime);
        self
    }

    /// Derive a post-commit observation context without carrying read-only
    /// provider materializations across an external effect. The deadline,
    /// cancellation, and installed capabilities remain the
    /// same; only provider-private request-scope state is fresh.
    pub fn after_external_effect(mut self) -> Self {
        self.request_scope = ConnectorRequestScope::new();
        self.fresh_catalog_observation_required = true;
        self
    }

    /// Whether the caller crossed a known external-effect boundary and the
    /// next provider metadata observation must not reuse a generation cache.
    pub const fn fresh_catalog_observation_required(&self) -> bool {
        self.fresh_catalog_observation_required
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

    /// Project an admitted request onto the planning boundary. Planning may
    /// retain deadlines and cancellation, but
    /// it cannot resolve attempt storage or collect credentials for a future
    /// execution attempt.
    pub fn without_attempt_capabilities(mut self) -> Self {
        self.storage_resolver = None;
        self.vended_credential_lease_sink = None;
        self.vended_credential_lease_collection = None;
        self.execution_source = None;
        self
    }

    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn stop(&self) -> &ConnectorStopView {
        &self.stop
    }

    pub fn is_cancelled(&self) -> bool {
        self.stop.is_stopped()
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

impl ConnectorOperationControl for ConnectorRequestContext {
    fn check_active(&self) -> Result<(), ConnectorError> {
        if self.is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector operation was cancelled",
            ));
        }
        if Instant::now() >= self.deadline {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector operation deadline elapsed",
            ));
        }
        Ok(())
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use super::{
        ConnectorOperationControl, ConnectorPlanningContext, ConnectorRangeScope,
        ConnectorRequestContext, ConnectorStorageResolver, ResolvedVendedS3Access,
        StorageAccessRequest, VendedS3SeedMaterial,
    };
    use crate::connector::{
        CatalogHandle, CatalogProperties, CatalogVersion, ConnectorError, ConnectorErrorKind,
        ConnectorInstanceId, ConnectorProviderId, ConnectorStopOwner,
        ConnectorVendedCredentialLeaseSink, CredentialLeaseId, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, StorageAccessDomainId, StorageCredentialScopePrefix,
        VendedS3CredentialLeaseContribution,
    };
    use novarocks_secret::SecretValue;

    use crate::connector::read_stack::ConnectorSourceOperations;

    #[test]
    fn admitted_stop_reaches_every_request_clone_without_changing_deadline() {
        let owner = ConnectorStopOwner::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        let request = ConnectorRequestContext::try_new(
            deadline,
            owner.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request");
        let clone = request.clone();
        owner.request_stop();
        assert!(clone.is_cancelled());
        assert_eq!(clone.deadline(), deadline);
        assert_eq!(
            clone.check_active().expect_err("stopped request").kind(),
            ConnectorErrorKind::Cancelled
        );
    }

    #[test]
    fn file_stop_view_retains_signal_relay_after_request_release() {
        struct RelayLifetime(Arc<AtomicBool>);
        impl Drop for RelayLifetime {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let released = Arc::new(AtomicBool::new(false));
        let request = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request")
        .with_stop_lifetime(Arc::new(RelayLifetime(Arc::clone(&released))));
        let file_view = request.stop().clone();
        drop(request);
        assert!(!released.load(Ordering::Acquire));
        drop(file_view);
        assert!(released.load(Ordering::Acquire));
    }

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

    #[test]
    fn range_scope_is_exact_and_does_not_cross_the_planning_boundary() {
        for parts in [
            (0, 0, 1, 3, 4, 5),
            (1, 2, 0, 3, 4, 5),
            (1, 2, 1, 0, 0, 5),
            (1, 2, 1, 3, 4, -1),
        ] {
            assert!(
                ConnectorRangeScope::try_new(parts.0, parts.1, parts.2, parts.3, parts.4, parts.5,)
                    .is_err()
            );
        }
        let scope = ConnectorRangeScope::try_new(1, 2, 3, 4, 5, 6).expect("exact scope");
        let request = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request")
        .with_execution_source(scope, ConnectorSourceOperations::new());
        assert_eq!(request.clone().range_scope(), Some(scope));
        let ticket = request
            .source_operations()
            .expect("source operations")
            .admit(Arc::new(|| {}))
            .expect("admitted");
        assert_eq!(
            request
                .clone()
                .source_operations()
                .expect("source operations")
                .live_operations(),
            1,
            "every clone of the request shares the source's operations"
        );
        drop(ticket);
        assert!(ConnectorPlanningContext::try_from_request(request.clone()).is_err());
        let planning = request.without_attempt_capabilities();
        assert_eq!(planning.range_scope(), None);
        assert!(planning.source_operations().is_none());
        ConnectorPlanningContext::try_from_request(planning).expect("planning projection");
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
            None,
            Some(VendedS3SeedMaterial::new(
                42,
                SecretValue::new("access-canary"),
                SecretValue::new("secret-canary"),
                SecretValue::new("token-canary"),
            )),
        );
        let rendered = format!("{access:?}");
        assert!(!rendered.contains("access-canary"));
        assert!(!rendered.contains("secret-canary"));
        assert!(!rendered.contains("token-canary"));
    }

    #[test]
    fn post_effect_observation_uses_a_fresh_provider_scope() {
        struct ProviderMarker;

        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap();
        let before = context.request_scope_extension_or_insert_with(|| ProviderMarker);
        let after = context.clone().after_external_effect();
        let observed = after.request_scope_extension_or_insert_with(|| ProviderMarker);

        assert!(after.fresh_catalog_observation_required());
        assert!(!Arc::ptr_eq(&before, &observed));
        assert_eq!(context.deadline(), after.deadline());
    }

    #[test]
    fn planning_context_rejects_attempt_credential_collection() {
        let base = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            ConnectorStopOwner::new().view(),
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

        let catalog_properties = CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("planning-projection").expect("catalog"),
                CatalogVersion::from_bytes([2; 32]),
            ),
            ConnectorProviderId::parse("test").expect("provider"),
            1,
            Vec::new(),
            Vec::new(),
        )
        .expect("properties");
        let projected = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap()
        .with_storage_resolver(Arc::new(RejectingResolver))
        .with_vended_credential_lease_sink(Arc::new(RejectingSink))
        .with_vended_credential_lease_collection(catalog_properties)
        .expect("collection")
        .without_attempt_capabilities();
        assert!(ConnectorPlanningContext::try_from_request(projected).is_ok());
    }
}
