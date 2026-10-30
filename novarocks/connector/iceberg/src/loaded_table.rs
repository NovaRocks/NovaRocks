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

//! Provider-private physical table state captured by a catalog load.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use novarocks_fs::{ObjectStoreSecretMaterial, SecretValue};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorVendedS3CredentialLeaseRefresher,
    StorageCredentialScopePrefix, VendedS3CredentialLeaseContribution,
    VendedS3CredentialLeaseEntry, VendedS3CredentialLeaseRefresh,
    VendedS3CredentialRefreshCallPolicy, VendedS3CredentialRefreshDispatch,
};
use novarocks_types::naming::normalize_identifier;

const S3_ACCESS_KEY_ID: &str = "s3.access-key-id";
const S3_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
const S3_SESSION_TOKEN: &str = "s3.session-token";
const S3_SESSION_TOKEN_EXPIRES_AT_MS: &str = "s3.session-token-expires-at-ms";
const CLIENT_REFRESH_CREDENTIALS_ENABLED: &str = "client.refresh-credentials-enabled";
const CLIENT_REFRESH_CREDENTIALS_ENDPOINT: &str = "client.refresh-credentials-endpoint";

/// Response-local access delegation from one REST load/create result.
///
/// The vended arm is deliberately move-only.  It is neither a table attribute
/// nor a cache entry: a later query-attempt owner must consume it before any
/// table/FileIO/plan/handle can outlive this response.
pub(crate) enum IcebergAccessDelegation {
    Static,
    Vended(IcebergVendedCredentialLeaseSeed),
}

impl IcebergAccessDelegation {
    pub(crate) fn static_binding() -> Self {
        Self::Static
    }

    pub(crate) fn into_vended_lease_seed(self) -> Option<IcebergVendedCredentialLeaseSeed> {
        match self {
            Self::Static => None,
            Self::Vended(seed) => Some(seed),
        }
    }
}

/// A table materialized by the catalog plus response-local access facts.
///
/// This owns the delegation until a query-attempt lease source takes it.  It
/// intentionally has no `Clone`, and its table accessor never embeds the
/// delegation in Iceberg's `Table`/`FileIO` values.
pub(crate) struct IcebergLoadedTable {
    materialization: IcebergLoadedTableMaterialization,
    access_delegation: IcebergAccessDelegation,
}

/// The one place a catalog-load response can defer FileIO construction.
///
/// Static catalogs already own a fully materialized table.  A REST-vended
/// response intentionally does not: its companion lease must enter the
/// query-attempt collector before a request-scoped storage resolver can build
/// FileIO.  This enum is provider-private and move-only so neither branch can
/// leak into a physical-table cache.
pub(crate) enum IcebergLoadedTableMaterialization {
    Materialized(crate::iceberg::table::Table),
    DeferredRest(Box<crate::rest_protocol::deferred::FrozenLoadTable>),
}

impl IcebergLoadedTable {
    pub(crate) fn new(
        table: crate::iceberg::table::Table,
        access_delegation: IcebergAccessDelegation,
    ) -> Self {
        Self {
            materialization: IcebergLoadedTableMaterialization::Materialized(table),
            access_delegation,
        }
    }

    pub(crate) fn deferred_rest(
        materialization: crate::rest_protocol::deferred::FrozenLoadTable,
        access_delegation: IcebergAccessDelegation,
    ) -> Self {
        Self {
            materialization: IcebergLoadedTableMaterialization::DeferredRest(Box::new(
                materialization,
            )),
            access_delegation,
        }
    }

    /// Catalog control may inspect immutable identity without collecting any
    /// storage delegation or constructing request-scoped FileIO.
    pub(crate) fn metadata(&self) -> &crate::iceberg::spec::TableMetadata {
        match &self.materialization {
            IcebergLoadedTableMaterialization::Materialized(table) => table.metadata(),
            IcebergLoadedTableMaterialization::DeferredRest(table) => table.metadata(),
        }
    }
    pub(crate) fn metadata_location(&self) -> Option<&str> {
        match &self.materialization {
            IcebergLoadedTableMaterialization::Materialized(table) => table.metadata_location(),
            IcebergLoadedTableMaterialization::DeferredRest(table) => table.metadata_location(),
        }
    }

    pub(crate) fn into_parts(self) -> (IcebergLoadedTableMaterialization, IcebergAccessDelegation) {
        (self.materialization, self.access_delegation)
    }
}

impl Debug for IcebergLoadedTable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        // Test helpers require `Result<IcebergLoadedTable, _>: Debug`; keep
        // the confidential companion opaque even on that diagnostic path.
        formatter
            .debug_struct("IcebergLoadedTable")
            .field(
                "materialization",
                &match &self.materialization {
                    IcebergLoadedTableMaterialization::Materialized(_) => "materialized",
                    IcebergLoadedTableMaterialization::DeferredRest(_) => "deferred-rest",
                },
            )
            .field("access_delegation", &"<confidential>")
            .finish()
    }
}

impl IcebergLoadedTableMaterialization {
    /// Bind a table's immutable response metadata to the exact request-local
    /// resolver after its vended lease has entered the attempt collector.
    pub(crate) fn materialize_for_request(
        self,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<crate::iceberg::table::Table, ConnectorError> {
        match self {
            Self::Materialized(table) => Ok(table),
            Self::DeferredRest(frozen) => frozen
                .materialize(crate::fs_io::build_file_io_for_location("", binding))
                .map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("materialize request-scoped REST Iceberg table: {error}"),
                    )
                }),
        }
    }

    /// Materialize a static catalog response. A deferred REST response must
    /// never take this path: it has no catalog-global FileIO fallback.
    pub(crate) fn into_static_table(self) -> Result<crate::iceberg::table::Table, ConnectorError> {
        match self {
            Self::Materialized(table) => Ok(table),
            Self::DeferredRest(_) => Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "REST-vended Iceberg table requires a query-attempt storage resolver",
            )),
        }
    }
}

/// Provider-private, redacted source for one vended S3 query lease.
///
/// Secret values can be consumed only by the in-process lifecycle/FS owner.
/// No raw REST map, `Clone`, `Serialize`, or `Debug` implementation is
/// available here.
pub(crate) struct IcebergVendedCredentialLeaseSeed {
    credentials: Vec<IcebergVendedS3Credential>,
    refresh_endpoint: Option<Arc<str>>,
}

/// Non-secret, immutable scope that an Iceberg REST refresh must preserve.
/// It is intentionally private to the provider and contains no credential
/// material, table metadata, or catalog identity.
#[derive(Clone)]
pub(crate) struct IcebergVendedS3RefreshScope {
    prefixes: Vec<StorageCredentialScopePrefix>,
    endpoint: Arc<str>,
}

/// Non-secret scope retained when the declared REST generation has no
/// independent credentials endpoint and must renew through `load_table`.
///
/// The exact table identity is kept by the refresher itself. This value only
/// proves that a later response did not expand or retarget storage authority.
#[derive(Clone)]
pub(crate) struct IcebergVendedS3LoadTableScope {
    prefixes: Vec<StorageCredentialScopePrefix>,
}

/// Closed renewal paths supported by one parsed REST delegation response.
/// Callers select exactly one path and never probe another after failure.
#[derive(Clone)]
pub(crate) enum IcebergVendedS3RenewalCapability {
    CredentialsEndpoint(IcebergVendedS3RefreshScope),
    LoadTableDelegation(IcebergVendedS3LoadTableScope),
}

impl IcebergVendedCredentialLeaseSeed {
    /// Transfer this response-local seed directly into the provider-neutral
    /// query-attempt collection contribution. The move prevents its secret
    /// material from reaching table, FileIO, cache, or plan state.
    pub(crate) fn into_vended_s3_credential_lease_contribution(
        self,
    ) -> Result<VendedS3CredentialLeaseContribution, ConnectorError> {
        let entries = self
            .credentials
            .into_iter()
            .map(|credential| {
                let ObjectStoreSecretMaterial {
                    access_key_id,
                    access_key_secret,
                    session_token,
                } = credential.material;
                let session_token = session_token
                    .ok_or_else(|| invalid("vended S3 credential is missing a session token"))?;
                VendedS3CredentialLeaseEntry::try_new(
                    credential.prefix,
                    credential.not_after_unix_ms,
                    access_key_id,
                    access_key_secret,
                    session_token,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        // The announced path is routed through the generation's session
        // binding by the caller that forms the lease (L15); the bare endpoint
        // is not a path an execution node may use on its own.
        VendedS3CredentialLeaseContribution::try_new(entries, None)
    }

    /// Preserve exactly the provider-neutral facts against which a later
    /// credentials response is checked. A refresh cannot add, remove, or
    /// retarget prefix authority within an admitted query attempt.
    pub(crate) fn refresh_scope(&self) -> Option<IcebergVendedS3RefreshScope> {
        self.refresh_endpoint
            .as_ref()
            .map(|endpoint| IcebergVendedS3RefreshScope {
                prefixes: self
                    .credentials
                    .iter()
                    .map(|entry| entry.prefix.clone())
                    .collect(),
                endpoint: Arc::clone(endpoint),
            })
    }

    pub(crate) fn load_table_refresh_scope(&self) -> IcebergVendedS3LoadTableScope {
        IcebergVendedS3LoadTableScope {
            prefixes: self
                .credentials
                .iter()
                .map(|entry| entry.prefix.clone())
                .collect(),
        }
    }

    pub(crate) fn renewal_capability(&self) -> IcebergVendedS3RenewalCapability {
        match self.refresh_scope() {
            Some(scope) => IcebergVendedS3RenewalCapability::CredentialsEndpoint(scope),
            None => IcebergVendedS3RenewalCapability::LoadTableDelegation(
                self.load_table_refresh_scope(),
            ),
        }
    }

    /// A staged target has no stable table endpoint until publication. It can
    /// therefore continue only when this response declares an independent
    /// credentials endpoint.
    pub(crate) fn staged_refresh_scope(
        &self,
    ) -> Result<IcebergVendedS3RefreshScope, ConnectorError> {
        match self.renewal_capability() {
            IcebergVendedS3RenewalCapability::CredentialsEndpoint(scope) => Ok(scope),
            IcebergVendedS3RenewalCapability::LoadTableDelegation(_) => Err(invalid(
                "REST staged create vended credentials have no independent renewal endpoint",
            )),
        }
    }

    /// Prove that this response authorizes the immutable table facts an
    /// attempt is about to use. The catalog may have returned a newer metadata
    /// location; callers pass the already-bound table when reacquiring an
    /// attempt so the check remains against the original resources.
    pub(crate) fn validate_table_access(
        &self,
        table: &crate::iceberg::table::Table,
    ) -> Result<(), ConnectorError> {
        self.select_for_location(table.metadata().location())?;
        if let Some(metadata_location) = table.metadata_location() {
            self.select_for_location(metadata_location)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn not_after_unix_ms(&self) -> u64 {
        self.credentials
            .iter()
            .map(|credential| credential.not_after_unix_ms)
            .min()
            .expect("vended credential seed is non-empty")
    }

    #[cfg(test)]
    pub(crate) fn refresh_capable(&self) -> bool {
        self.refresh_endpoint.is_some()
    }

    pub(crate) fn refresh_endpoint(&self) -> Option<&str> {
        self.refresh_endpoint.as_deref()
    }

    /// Scope equality deliberately excludes material and expiration so a
    /// refresh can replace values without changing the query's access domain.
    #[cfg(test)]
    pub(crate) fn has_same_scope(&self, other: &Self) -> bool {
        self.refresh_endpoint == other.refresh_endpoint
            && self.credentials.len() == other.credentials.len()
            && self
                .credentials
                .iter()
                .zip(&other.credentials)
                .all(|(left, right)| left.prefix == right.prefix)
    }

    /// Select exactly one matching credential by longest canonical prefix.
    /// A same-length tie is rejected even if a future prefix normalizer makes
    /// it possible; choosing a response-order winner would expand authority.
    pub(crate) fn select_for_location(
        &self,
        location: &str,
    ) -> Result<&IcebergVendedS3Credential, ConnectorError> {
        let location = canonical_s3_location(location)?;
        let mut selected = None::<&IcebergVendedS3Credential>;
        for credential in &self.credentials {
            if !prefix_matches(credential.prefix.as_str(), &location) {
                continue;
            }
            match selected {
                None => selected = Some(credential),
                Some(current)
                    if credential.prefix.as_str().len() > current.prefix.as_str().len() =>
                {
                    selected = Some(credential)
                }
                Some(current)
                    if credential.prefix.as_str().len() == current.prefix.as_str().len() =>
                {
                    return Err(invalid("ambiguous vended S3 credential prefix match"));
                }
                Some(_) => {}
            }
        }
        selected.ok_or_else(|| invalid("vended S3 credential has no matching prefix"))
    }
}

impl IcebergVendedS3RefreshScope {
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn matches_seed(&self, seed: &IcebergVendedCredentialLeaseSeed) -> bool {
        // The credentials endpoint is capability carried by the initial table
        // response.  A standard refresh response need only rotate the
        // credentials themselves (Trino's provider retains the endpoint from
        // its original FileIO properties), so an omitted endpoint preserves
        // the already-admitted authority.  An explicitly different endpoint
        // remains a scope change and is rejected.
        seed.refresh_endpoint
            .as_deref()
            .is_none_or(|endpoint| endpoint == self.endpoint())
            && seed.credentials.len() == self.prefixes.len()
            && seed
                .credentials
                .iter()
                .zip(&self.prefixes)
                .all(|(credential, prefix)| credential.prefix == *prefix)
    }

    /// Bind a newly acquired attempt to the declared credentials endpoint.
    ///
    /// A new attempt proves coverage of its frozen resources independently;
    /// it does not inherit the preceding attempt's prefix set. The endpoint
    /// remains the declared acquisition path, while the fresh response becomes
    /// the scope that later renewals for this attempt must preserve.
    pub(crate) fn reacquired_for_seed(
        &self,
        seed: &IcebergVendedCredentialLeaseSeed,
    ) -> Result<Self, ConnectorError> {
        if seed
            .refresh_endpoint
            .as_deref()
            .is_some_and(|endpoint| endpoint != self.endpoint())
        {
            return Err(invalid(
                "vended REST credential reacquisition changed the credentials endpoint",
            ));
        }
        Ok(Self {
            prefixes: seed
                .credentials
                .iter()
                .map(|entry| entry.prefix.clone())
                .collect(),
            endpoint: Arc::clone(&self.endpoint),
        })
    }
}

impl IcebergVendedS3LoadTableScope {
    fn matches_seed(&self, seed: &IcebergVendedCredentialLeaseSeed) -> bool {
        seed.refresh_endpoint.is_none()
            && seed.credentials.len() == self.prefixes.len()
            && seed
                .credentials
                .iter()
                .zip(&self.prefixes)
                .all(|(credential, prefix)| credential.prefix == *prefix)
    }
}

/// FE-local source for refreshing one already-admitted REST vended scope.
/// It retains the generation's REST endpoints, which share the session that
/// observed the initial response, and the explicit runtime; no Backend, table
/// cache, or native message can recover either capability.
pub(crate) struct IcebergRestVendedS3LeaseRefresher {
    endpoints: Arc<crate::rest_protocol::endpoints::RestEndpoints>,
    runtime: crate::resources::IcebergCatalogRuntime,
    scope: IcebergVendedS3RefreshScope,
}

impl IcebergRestVendedS3LeaseRefresher {
    pub(crate) fn new(
        endpoints: Arc<crate::rest_protocol::endpoints::RestEndpoints>,
        runtime: crate::resources::IcebergCatalogRuntime,
        scope: IcebergVendedS3RefreshScope,
    ) -> Self {
        Self {
            endpoints,
            runtime,
            scope,
        }
    }
}

impl ConnectorVendedS3CredentialLeaseRefresher for IcebergRestVendedS3LeaseRefresher {
    fn refresh_vended_s3_credentials(
        &self,
        policy: VendedS3CredentialRefreshCallPolicy,
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError> {
        let endpoints = Arc::clone(&self.endpoints);
        let endpoint = Arc::clone(&self.scope.endpoint);
        let delegation = run_vended_renewal_with_policy(
            &self.runtime,
            policy,
            "Iceberg REST credential refresh",
            move || {
                let endpoints = Arc::clone(&endpoints);
                let endpoint = Arc::clone(&endpoint);
                async move { endpoints.load_credentials(endpoint.as_ref()).await }
            },
        )?;
        self.project_refresh(delegation)
    }

    fn refresh_vended_s3_credentials_async<'a>(
        &'a self,
        policy: VendedS3CredentialRefreshCallPolicy,
        request: novarocks_spi::connector::ConnectorRequestContext,
    ) -> futures::future::BoxFuture<'a, Result<VendedS3CredentialLeaseRefresh, ConnectorError>>
    {
        Box::pin(async move {
            let endpoints = Arc::clone(&self.endpoints);
            let endpoint = Arc::clone(&self.scope.endpoint);
            let context = crate::rest_protocol::http::RequestContext::from_connector(&request);
            let delegation = run_vended_renewal_with_policy_async(
                policy,
                "Iceberg REST credential refresh",
                move || {
                    let endpoints = Arc::clone(&endpoints);
                    let endpoint = Arc::clone(&endpoint);
                    let context = context.clone();
                    async move {
                        endpoints
                            .load_credentials_in(endpoint.as_ref(), &context)
                            .await
                    }
                },
            )
            .await?;
            self.project_refresh(delegation)
        })
    }
}

/// FE-local renewal path for REST catalogs that vend credentials only from
/// `load_table` responses.
///
/// Only the table UUID and the credentials are decoded from the response, and
/// the UUID serves solely to verify the exact object identity. No metadata
/// location or snapshot ever replaces the logical execution's frozen binding.
pub(crate) struct IcebergRestLoadTableVendedS3LeaseRefresher {
    endpoints: Arc<crate::rest_protocol::endpoints::RestEndpoints>,
    runtime: crate::resources::IcebergCatalogRuntime,
    table: crate::iceberg::TableIdent,
    expected_table_uuid: uuid::Uuid,
    scope: IcebergVendedS3LoadTableScope,
}

impl IcebergRestLoadTableVendedS3LeaseRefresher {
    pub(crate) fn new(
        endpoints: Arc<crate::rest_protocol::endpoints::RestEndpoints>,
        runtime: crate::resources::IcebergCatalogRuntime,
        table: crate::iceberg::TableIdent,
        expected_table_uuid: uuid::Uuid,
        scope: IcebergVendedS3LoadTableScope,
    ) -> Self {
        Self {
            endpoints,
            runtime,
            table,
            expected_table_uuid,
            scope,
        }
    }
}

impl ConnectorVendedS3CredentialLeaseRefresher for IcebergRestLoadTableVendedS3LeaseRefresher {
    fn refresh_vended_s3_credentials(
        &self,
        policy: VendedS3CredentialRefreshCallPolicy,
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError> {
        let endpoints = Arc::clone(&self.endpoints);
        let table = self.table.clone();
        let (observed_uuid, delegation) = run_vended_renewal_with_policy(
            &self.runtime,
            policy,
            "Iceberg REST load-table credential refresh",
            move || {
                let endpoints = Arc::clone(&endpoints);
                let table = table.clone();
                async move { endpoints.load_table_credentials(&table).await }
            },
        )?;
        self.project_refresh((observed_uuid, delegation))
    }

    fn refresh_vended_s3_credentials_async<'a>(
        &'a self,
        policy: VendedS3CredentialRefreshCallPolicy,
        request: novarocks_spi::connector::ConnectorRequestContext,
    ) -> futures::future::BoxFuture<'a, Result<VendedS3CredentialLeaseRefresh, ConnectorError>>
    {
        Box::pin(async move {
            let endpoints = Arc::clone(&self.endpoints);
            let table = self.table.clone();
            let context = crate::rest_protocol::http::RequestContext::from_connector(&request);
            let observed = run_vended_renewal_with_policy_async(
                policy,
                "Iceberg REST load-table credential refresh",
                move || {
                    let endpoints = Arc::clone(&endpoints);
                    let table = table.clone();
                    let context = context.clone();
                    async move { endpoints.load_table_credentials_in(&table, &context).await }
                },
            )
            .await?;
            self.project_refresh(observed)
        })
    }
}

/// Execute an idempotent provider call under one call-local deadline.
///
/// The timeout owns the actual future, rather than an outer blocking-job
/// handle. Dropping that future therefore stops waiting for its request and
/// response body before the synchronous bridge returns its Connector permit.
/// The call classifies its own failures: only an error marked retryable before
/// progress is retried.
pub(crate) fn run_vended_renewal_with_policy<T, F, Fut>(
    runtime: &crate::resources::IcebergCatalogRuntime,
    policy: VendedS3CredentialRefreshCallPolicy,
    operation: &'static str,
    call: F,
) -> Result<T, ConnectorError>
where
    T: Send + 'static,
    F: Fn() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<T, ConnectorError>> + Send + 'static,
{
    runtime
        .block_on(run_vended_renewal_with_policy_async(
            policy, operation, call,
        ))
        .map_err(|error| unavailable(format!("run {operation}: {error}")))?
}

fn refresh_deadline_exhausted(operation: &'static str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::DeadlineExceeded,
        format!("{operation} exhausted its provider-call deadline"),
    )
}

fn refresh_dispatch_fenced(operation: &'static str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Cancelled,
        format!("{operation} retry dispatch was fenced by its terminated attempt"),
    )
}

pub(crate) struct IcebergVendedS3Credential {
    prefix: StorageCredentialScopePrefix,
    material: ObjectStoreSecretMaterial,
    not_after_unix_ms: u64,
}

/// Translate a NovaRocks-decoded delegation into the provider's closed S3
/// schema. The raw values live only inside this adapter.
pub(crate) fn parse_rest_delegation(
    delegation: crate::rest_protocol::deferred::RestDelegation,
) -> Result<IcebergAccessDelegation, ConnectorError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("system clock before Unix epoch"))?
        .as_millis();
    let now = u64::try_from(now).map_err(|_| invalid("system clock exceeds Unix milliseconds"))?;
    parse_rest_delegation_at(delegation, now)
}

fn parse_rest_delegation_at(
    delegation: crate::rest_protocol::deferred::RestDelegation,
    now_unix_ms: u64,
) -> Result<IcebergAccessDelegation, ConnectorError> {
    use crate::rest_protocol::minimal_view::StorageCredentials;
    let credentials = match delegation.into_credentials() {
        StorageCredentials::Present(credentials) if !credentials.is_empty() => credentials,
        _ => {
            return Err(invalid(
                "vended REST Iceberg response has no storage credentials",
            ));
        }
    };
    let inputs = credentials
        .into_iter()
        .map(|credential| {
            let (prefix, config) = credential.into_parts();
            RestCredentialInput { prefix, config }
        })
        .collect();
    parse_vended_s3_credentials_at(inputs, now_unix_ms)
}

/// Raw REST values exist only while this adapter translates them into the
/// closed, redacted seed.  This type is intentionally private and has neither
/// `Debug` nor any accessor returning its map.
struct RestCredentialInput {
    prefix: String,
    config: std::collections::BTreeMap<String, String>,
}

fn parse_vended_s3_credentials_at(
    inputs: Vec<RestCredentialInput>,
    now_unix_ms: u64,
) -> Result<IcebergAccessDelegation, ConnectorError> {
    if inputs.is_empty() {
        return Err(invalid(
            "vended REST Iceberg response has no storage credentials",
        ));
    }
    let mut credentials = Vec::with_capacity(inputs.len());
    let mut refresh = None::<Option<Arc<str>>>;
    for input in inputs {
        let prefix = canonical_s3_prefix(&input.prefix)?;
        if input
            .config
            .keys()
            .any(|key| !is_supported_s3_vended_key(key))
        {
            return Err(invalid(
                "vended S3 credential contains an unsupported config key",
            ));
        }
        let access_key_id = required_config_value(&input, S3_ACCESS_KEY_ID)?;
        let access_key_secret = required_config_value(&input, S3_SECRET_ACCESS_KEY)?;
        let session_token = required_config_value(&input, S3_SESSION_TOKEN)?;
        let expiration = required_config_value(&input, S3_SESSION_TOKEN_EXPIRES_AT_MS)?;
        let not_after_unix_ms = expiration
            .parse::<u64>()
            .map_err(|_| invalid("vended S3 credential expiration"))?;
        if not_after_unix_ms <= now_unix_ms {
            return Err(invalid("vended S3 credential is expired"));
        }
        let credential_refresh = parse_refresh_capability(&input)?;
        match refresh.as_ref() {
            None => refresh = Some(credential_refresh),
            Some(existing) if existing == &credential_refresh => {}
            Some(_) => {
                return Err(invalid(
                    "vended S3 credentials disagree on refresh capability",
                ));
            }
        }
        credentials.push(IcebergVendedS3Credential {
            prefix,
            material: ObjectStoreSecretMaterial {
                access_key_id: SecretValue::new(access_key_id),
                access_key_secret: SecretValue::new(access_key_secret),
                session_token: Some(SecretValue::new(session_token)),
            },
            not_after_unix_ms,
        });
    }
    credentials.sort_by(|left, right| left.prefix.cmp(&right.prefix));
    if credentials
        .windows(2)
        .any(|pair| pair[0].prefix == pair[1].prefix)
    {
        return Err(invalid("duplicate vended S3 credential prefix"));
    }
    Ok(IcebergAccessDelegation::Vended(
        IcebergVendedCredentialLeaseSeed {
            credentials,
            refresh_endpoint: refresh.flatten(),
        },
    ))
}

/// Iceberg REST vended credentials use the standard client capability pair.
/// Like Iceberg AWS/Trino, refresh is enabled by default when an endpoint is
/// present; an explicit `false` disables it. This preserves the standard
/// contract while every prefix still has to agree on the exact capability.
fn parse_refresh_capability(
    input: &RestCredentialInput,
) -> Result<Option<Arc<str>>, ConnectorError> {
    let enabled = match input.config.get(CLIENT_REFRESH_CREDENTIALS_ENABLED) {
        None => true,
        Some(value) if value == "true" => true,
        Some(value) if value == "false" => false,
        Some(_) => return Err(invalid("vended S3 credential refresh capability")),
    };
    let endpoint = input
        .config
        .get(CLIENT_REFRESH_CREDENTIALS_ENDPOINT)
        .map(String::as_str)
        .map(parse_refresh_endpoint)
        .transpose()?;
    match (enabled, endpoint) {
        (true, Some(endpoint)) => Ok(Some(endpoint)),
        (true, None) => Ok(None),
        (false, None) => Ok(None),
        (false, Some(_)) => Ok(None),
    }
}

fn required_config_value<'a>(
    input: &'a RestCredentialInput,
    key: &str,
) -> Result<&'a str, ConnectorError> {
    input
        .config
        .get(key)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("vended S3 credential is missing a required config value"))
}

fn is_supported_s3_vended_key(key: &str) -> bool {
    matches!(
        key,
        S3_ACCESS_KEY_ID
            | S3_SECRET_ACCESS_KEY
            | S3_SESSION_TOKEN
            | S3_SESSION_TOKEN_EXPIRES_AT_MS
            | CLIENT_REFRESH_CREDENTIALS_ENABLED
            | CLIENT_REFRESH_CREDENTIALS_ENDPOINT
    )
}

fn canonical_s3_prefix(value: &str) -> Result<StorageCredentialScopePrefix, ConnectorError> {
    let canonical = canonical_s3_location(value)?;
    StorageCredentialScopePrefix::try_from_normalized(&canonical)
}

fn canonical_s3_location(value: &str) -> Result<String, ConnectorError> {
    let parsed = url::Url::parse(value).map_err(|_| invalid("vended S3 credential prefix"))?;
    if parsed.scheme() != "s3"
        || parsed.host_str().is_none_or(str::is_empty)
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte == b'\\')
    {
        return Err(invalid("vended S3 credential prefix"));
    }
    let bucket = parsed
        .host_str()
        .expect("checked S3 bucket")
        .to_ascii_lowercase();
    let path = parsed.path();
    if path
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return Err(invalid("vended S3 credential prefix"));
    }
    Ok(format!("s3://{bucket}{path}"))
}

fn parse_refresh_endpoint(value: &str) -> Result<Arc<str>, ConnectorError> {
    let parsed =
        url::Url::parse(value).map_err(|_| invalid("vended S3 credential refresh endpoint"))?;
    if !matches!(parsed.scheme(), "https" | "http")
        || parsed.host_str().is_none_or(str::is_empty)
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(invalid("vended S3 credential refresh endpoint"));
    }
    Ok(Arc::from(value))
}

fn prefix_matches(prefix: &str, location: &str) -> bool {
    if prefix == location {
        return true;
    }
    let Some(suffix) = location.strip_prefix(prefix) else {
        return false;
    };
    prefix.ends_with('/') || suffix.starts_with('/')
}

fn invalid(subject: &'static str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        format!("invalid {subject}"),
    )
}

fn unavailable(message: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
}

#[derive(Clone)]
pub struct IcebergPhysicalTable {
    pub table: crate::iceberg::table::Table,
    attempt_access: Option<IcebergVendedS3RenewalCapability>,
}

/// Provider-private, resource-free retry recipe. It retains the exact table
/// identity and metadata selected during preparation, but never that request's
/// FileIO, cancellation scope, storage resolver, or resource ledger.
#[derive(Clone)]
pub(crate) struct IcebergAttemptTableAccess {
    identifier: crate::iceberg::TableIdent,
    metadata: crate::iceberg::spec::TableMetadataRef,
    metadata_location: Option<String>,
    readonly: bool,
    renewal: Option<IcebergVendedS3RenewalCapability>,
}

impl IcebergAttemptTableAccess {
    pub(crate) fn freeze(table: IcebergPhysicalTable) -> Self {
        Self {
            identifier: table.table.identifier().clone(),
            metadata: table.table.metadata_ref(),
            metadata_location: table.table.metadata_location().map(str::to_owned),
            readonly: table.table.readonly(),
            renewal: table.attempt_access_if_vended(),
        }
    }

    /// The COW author borrows the loaded SDK table and shares its metadata.
    /// The original operation remains active while its access facts are frozen.
    pub(crate) fn freeze_with_original_scope(
        table: &IcebergPhysicalTable,
        scope: &crate::commit::write_stack::control::cow_begin::CowBeginScope<'_>,
    ) -> Result<Arc<Self>, novarocks_spi::connector::ConnectorCowBeginCause> {
        scope.active()?;
        let id = table.table.identifier();
        Ok(Arc::new(Self {
            identifier: id.clone(),
            metadata: table.table.metadata_ref(),
            metadata_location: table.table.metadata_location().map(str::to_owned),
            readonly: table.table.readonly(),
            renewal: table.attempt_access_if_vended(),
        }))
    }

    pub(crate) fn identifier(&self) -> &crate::iceberg::TableIdent {
        &self.identifier
    }

    pub(crate) fn renewal(&self) -> Option<IcebergVendedS3RenewalCapability> {
        self.renewal.clone()
    }

    pub(crate) fn metadata(&self) -> &crate::iceberg::spec::TableMetadata {
        &self.metadata
    }

    /// Non-secret identity of the frozen metadata view used to scope one
    /// attempt-local physical-table cache entry.
    ///
    /// The location is already an immutable fact of the frozen table access;
    /// it does not carry an access capability or a credential. Two references
    /// to one table may share request-local FileIO only when they name this
    /// same frozen metadata view and request-local reacquisition result.
    pub(crate) fn request_cache_identity(&self) -> String {
        self.metadata_location
            .clone()
            .unwrap_or_else(|| self.metadata.location().to_owned())
    }

    pub(crate) fn validate_table_access(
        &self,
        seed: &IcebergVendedCredentialLeaseSeed,
    ) -> Result<(), ConnectorError> {
        seed.select_for_location(self.metadata.location())?;
        if let Some(metadata_location) = &self.metadata_location {
            seed.select_for_location(metadata_location)?;
        }
        Ok(())
    }

    pub(crate) fn reacquired_request_scoped(
        &self,
        binding: crate::access_binding::IcebergReadBinding,
        runtime: crate::iceberg::Runtime,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        let file_io_location = self
            .metadata_location
            .as_deref()
            .unwrap_or_else(|| self.metadata.location());
        let mut builder = crate::iceberg::table::Table::builder()
            .runtime(runtime)
            .identifier(self.identifier.clone())
            .metadata(Arc::clone(&self.metadata))
            .readonly(self.readonly)
            .file_io(crate::fs_io::build_file_io_for_location(
                file_io_location,
                binding,
            ));
        if let Some(metadata_location) = &self.metadata_location {
            builder = builder.metadata_location(metadata_location.clone());
        }
        builder
            .build()
            .map(IcebergPhysicalTable::new)
            .map(|table| match &self.renewal {
                Some(renewal) => table.with_attempt_access(renewal.clone()),
                None => table,
            })
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("rebuild request-scoped Iceberg table: {error}"),
                )
            })
    }

    pub(crate) fn validate_attempt_reacquisition(
        &self,
        observed: &crate::iceberg::table::Table,
        delegation: &IcebergVendedCredentialLeaseSeed,
    ) -> Result<(), ConnectorError> {
        if observed.metadata().uuid() != self.metadata.uuid() {
            return Err(invalid(
                "vended REST load-table delegation returned a different table UUID",
            ));
        }
        let Some(IcebergVendedS3RenewalCapability::LoadTableDelegation(_)) = self.renewal() else {
            return Err(invalid(
                "frozen table requires credentials-endpoint attempt access",
            ));
        };
        if delegation.refresh_endpoint().is_some() {
            return Err(invalid(
                "vended REST load-table delegation changed its acquisition path",
            ));
        }
        self.validate_table_access(delegation)
    }

    /// Validate an attempt's initial credentials-endpoint acquisition.
    ///
    /// The catalog response supplies this attempt's secret lease, while the
    /// frozen metadata remains the only execution metadata authority. A
    /// later response may widen the credential prefixes, but it must retain
    /// the original endpoint and cover every frozen resource.
    pub(crate) fn validate_credentials_endpoint_attempt_acquisition(
        &self,
        observed: &crate::iceberg::table::Table,
        delegation: &IcebergVendedCredentialLeaseSeed,
    ) -> Result<IcebergVendedS3RefreshScope, ConnectorError> {
        if observed.metadata().uuid() != self.metadata.uuid() {
            return Err(invalid(
                "vended REST credentials acquisition returned a different table UUID",
            ));
        }
        let Some(IcebergVendedS3RenewalCapability::CredentialsEndpoint(scope)) = self.renewal()
        else {
            return Err(invalid(
                "frozen table requires load-table delegation attempt access",
            ));
        };
        self.validate_table_access(delegation)?;
        scope.reacquired_for_seed(delegation)
    }
}

impl Debug for IcebergPhysicalTable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergPhysicalTable")
            .field("table", &self.table)
            .field(
                "attempt_access",
                &self
                    .attempt_access
                    .as_ref()
                    .map(|capability| match capability {
                        IcebergVendedS3RenewalCapability::CredentialsEndpoint(_) => {
                            "credentials-endpoint"
                        }
                        IcebergVendedS3RenewalCapability::LoadTableDelegation(_) => {
                            "load-table-delegation"
                        }
                    }),
            )
            .finish()
    }
}

impl IcebergPhysicalTable {
    pub fn new(table: crate::iceberg::table::Table) -> Self {
        Self {
            table,
            attempt_access: None,
        }
    }

    pub(crate) fn with_attempt_access(
        mut self,
        capability: IcebergVendedS3RenewalCapability,
    ) -> Self {
        self.attempt_access = Some(capability);
        self
    }

    pub(crate) fn attempt_access_if_vended(&self) -> Option<IcebergVendedS3RenewalCapability> {
        self.attempt_access.clone()
    }

    pub fn into_table(self) -> crate::iceberg::table::Table {
        self.table
    }

    /// Rebuild a REST-vended table's FileIO from the request-local resolver.
    ///
    /// This copies only immutable metadata identity, never the source table's
    /// FileIO. A later action can therefore derive its table from persisted
    /// staged metadata without cloning or reusing the prepare action's
    /// request-local object-store capability.
    pub(crate) fn request_scoped(
        table: &crate::iceberg::table::Table,
        binding: crate::access_binding::IcebergReadBinding,
        runtime: crate::iceberg::Runtime,
    ) -> Result<Self, ConnectorError> {
        let metadata_location = table.metadata_location().map(str::to_owned);
        let file_io_location = metadata_location
            .as_deref()
            .unwrap_or_else(|| table.metadata().location());
        let mut builder = crate::iceberg::table::Table::builder()
            .runtime(runtime)
            .identifier(table.identifier().clone())
            .metadata(table.metadata_ref())
            .readonly(table.readonly())
            .file_io(crate::fs_io::build_file_io_for_location(
                file_io_location,
                binding,
            ));
        if let Some(metadata_location) = metadata_location {
            builder = builder.metadata_location(metadata_location);
        }
        builder.build().map(Self::new).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("rebuild request-scoped Iceberg table: {error}"),
            )
        })
    }
}

/// Per-control-generation cache of provider-private physical table state.
/// SQL table projections deliberately do not enter this cache.
#[derive(Clone, Default)]
pub struct IcebergPhysicalTableCache {
    entries: Arc<RwLock<HashMap<(String, String), IcebergPhysicalTable>>>,
}

impl IcebergPhysicalTableCache {
    pub fn get(
        &self,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<Option<IcebergPhysicalTable>, String> {
        let key = cache_key(namespace_name, table_name)?;
        let entries = match self.entries.read() {
            Ok(entries) => entries,
            Err(poisoned) => {
                let message = format!("table cache lock: {poisoned}");
                drop(poisoned.into_inner());
                self.entries.clear_poison();
                if let Ok(mut entries) = self.entries.write() {
                    entries.clear();
                }
                return Err(message);
            }
        };
        Ok(entries.get(&key).cloned())
    }

    pub fn insert(
        &self,
        namespace_name: &str,
        table_name: &str,
        physical: IcebergPhysicalTable,
    ) -> Result<(), String> {
        let key = cache_key(namespace_name, table_name)?;
        let mut entries = self
            .entries
            .write()
            .map_err(|error| format!("table cache lock: {error}"))?;
        entries.insert(key, physical);
        Ok(())
    }

    pub fn invalidate(&self, namespace_name: &str, table_name: &str) {
        let Ok(key) = cache_key(namespace_name, table_name) else {
            return;
        };
        if let Ok(mut entries) = self.entries.write() {
            entries.remove(&key);
        }
    }

    /// Test-only fault injection used by Core integration coverage. This is
    /// intentionally provider-owned: production composition never invokes it.
    #[doc(hidden)]
    pub fn poison_for_test(&self) {
        let entries = Arc::clone(&self.entries);
        let _ = std::thread::spawn(move || {
            let _guard = entries.write().expect("table cache write lock");
            panic!("injected table cache failure");
        })
        .join();
    }
}

fn cache_key(namespace_name: &str, table_name: &str) -> Result<(String, String), String> {
    Ok((
        normalize_identifier(namespace_name)?,
        normalize_identifier(table_name)?,
    ))
}

impl IcebergRestVendedS3LeaseRefresher {
    fn project_refresh(
        &self,
        delegation: crate::rest_protocol::deferred::RestDelegation,
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError> {
        let refreshed = match parse_rest_delegation(delegation)? {
            IcebergAccessDelegation::Vended(seed) => seed,
            IcebergAccessDelegation::Static => {
                return Err(invalid(
                    "vended REST credential refresh returned static access delegation",
                ));
            }
        };
        if !self.scope.matches_seed(&refreshed) {
            return Err(invalid(
                "vended REST credential refresh changed prefix scope or endpoint",
            ));
        }
        let (entries, _) = refreshed
            .into_vended_s3_credential_lease_contribution()?
            .into_parts();
        VendedS3CredentialLeaseRefresh::try_new(entries)
    }
}

impl IcebergRestLoadTableVendedS3LeaseRefresher {
    fn project_refresh(
        &self,
        (observed_uuid, delegation): (
            Option<String>,
            crate::rest_protocol::deferred::RestDelegation,
        ),
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError> {
        let observed_uuid = observed_uuid
            .as_deref()
            .and_then(|uuid| uuid::Uuid::parse_str(uuid).ok())
            .ok_or_else(|| invalid("vended REST load-table refresh returned no table UUID"))?;
        if observed_uuid != self.expected_table_uuid {
            return Err(invalid(
                "vended REST load-table refresh returned a different table UUID",
            ));
        }
        let refreshed = match parse_rest_delegation(delegation)? {
            IcebergAccessDelegation::Vended(seed) => seed,
            IcebergAccessDelegation::Static => {
                return Err(invalid(
                    "vended REST load-table refresh returned static access delegation",
                ));
            }
        };
        if !self.scope.matches_seed(&refreshed) {
            return Err(invalid(
                "vended REST load-table refresh changed prefix scope or acquisition path",
            ));
        }
        let (entries, _) = refreshed
            .into_vended_s3_credential_lease_contribution()?
            .into_parts();
        VendedS3CredentialLeaseRefresh::try_new(entries)
    }
}

pub(crate) async fn run_vended_renewal_with_policy_async<T, F, Fut>(
    policy: VendedS3CredentialRefreshCallPolicy,
    operation: &'static str,
    call: F,
) -> Result<T, ConnectorError>
where
    F: Fn() -> Fut + Send,
    Fut: std::future::Future<Output = Result<T, ConnectorError>> + Send,
{
    let deadline = tokio::time::Instant::now() + policy.remaining();
    let mut attempts = 0_u8;
    loop {
        if policy.provider_dispatch() == VendedS3CredentialRefreshDispatch::Fenced {
            return Err(refresh_dispatch_fenced(operation));
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(refresh_deadline_exhausted(operation));
        }
        attempts = attempts.saturating_add(1);
        let outcome = tokio::time::timeout(remaining, call()).await;
        match outcome {
            Err(_) => return Err(refresh_deadline_exhausted(operation)),
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => {
                if !error.retryable_before_progress() || attempts >= policy.max_attempts().get() {
                    return Err(error);
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining <= policy.retry_backoff() {
                    return Err(refresh_deadline_exhausted(operation));
                }
                tokio::time::sleep(policy.retry_backoff()).await;
                if policy.provider_dispatch() == VendedS3CredentialRefreshDispatch::Fenced {
                    return Err(refresh_dispatch_fenced(operation));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::num::NonZeroU8;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use super::{
        CLIENT_REFRESH_CREDENTIALS_ENABLED, CLIENT_REFRESH_CREDENTIALS_ENDPOINT,
        IcebergAccessDelegation, RestCredentialInput, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY,
        S3_SESSION_TOKEN, S3_SESSION_TOKEN_EXPIRES_AT_MS, parse_vended_s3_credentials_at,
        run_vended_renewal_with_policy, unavailable,
    };
    use novarocks_spi::connector::ConnectorError;
    use novarocks_spi::connector::{
        ConnectorErrorKind, VendedS3CredentialRefreshCallPolicy, VendedS3CredentialRefreshDispatch,
        VendedS3CredentialRefreshDispatchGuard,
    };

    /// Drive the renewal policy loop with an upstream-client call, the way
    /// the loop's deadline and retry behavior is easiest to exercise: an
    /// `Unexpected` (request-layer) error is retryable before progress.
    fn run_vended_refresh_with_policy<T, F, Fut>(
        runtime: &crate::resources::IcebergCatalogRuntime,
        policy: VendedS3CredentialRefreshCallPolicy,
        operation: &'static str,
        call: F,
    ) -> Result<T, ConnectorError>
    where
        T: Send + 'static,
        F: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = crate::iceberg::Result<T>> + Send + 'static,
    {
        run_vended_renewal_with_policy(runtime, policy, operation, move || {
            let call = call();
            async move {
                call.await.map_err(|error| {
                    let retryable = matches!(error.kind(), crate::iceberg::ErrorKind::Unexpected);
                    let error = unavailable(format!("{operation}: {error}"));
                    if retryable {
                        error.with_retryable_before_progress()
                    } else {
                        error
                    }
                })
            }
        })
    }

    struct PermittedDispatch;

    impl VendedS3CredentialRefreshDispatchGuard for PermittedDispatch {
        fn provider_dispatch(&self) -> VendedS3CredentialRefreshDispatch {
            VendedS3CredentialRefreshDispatch::Permitted
        }
    }

    struct ToggleDispatch(Arc<AtomicBool>);

    impl VendedS3CredentialRefreshDispatchGuard for ToggleDispatch {
        fn provider_dispatch(&self) -> VendedS3CredentialRefreshDispatch {
            if self.0.load(Ordering::SeqCst) {
                VendedS3CredentialRefreshDispatch::Permitted
            } else {
                VendedS3CredentialRefreshDispatch::Fenced
            }
        }
    }

    #[derive(Clone, Copy)]
    enum LoopbackResponse {
        ServiceUnavailable,
        Hold,
    }

    struct LoopbackHttpServer {
        address: SocketAddr,
        accepted: Arc<AtomicBool>,
        requests: Arc<AtomicU8>,
        shutdown: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl LoopbackHttpServer {
        fn start(response: LoopbackResponse) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback HTTP server");
            listener
                .set_nonblocking(true)
                .expect("make loopback HTTP server nonblocking");
            let address = listener.local_addr().expect("read loopback HTTP address");
            let accepted = Arc::new(AtomicBool::new(false));
            let requests = Arc::new(AtomicU8::new(0));
            let shutdown = Arc::new(AtomicBool::new(false));
            let thread_accepted = Arc::clone(&accepted);
            let thread_requests = Arc::clone(&requests);
            let thread_shutdown = Arc::clone(&shutdown);
            let thread = std::thread::spawn(move || {
                while !thread_shutdown.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            thread_accepted.store(true, Ordering::SeqCst);
                            // Accepted sockets inherit the nonblocking listener
                            // mode on the supported platforms, and a read timeout
                            // means nothing on a nonblocking socket. Left
                            // inherited, the request read below returns WouldBlock
                            // before the client has written anything, this server
                            // drops the connection, and the caller observes a
                            // transport failure instead of the response behavior
                            // under test.
                            let _ = stream.set_nonblocking(false);
                            let _ = stream.set_read_timeout(Some(Duration::from_millis(20)));
                            let mut request = [0_u8; 1024];
                            if stream.read(&mut request).unwrap_or(0) == 0 {
                                continue;
                            }
                            thread_requests.fetch_add(1, Ordering::SeqCst);
                            match response {
                                LoopbackResponse::ServiceUnavailable => {
                                    stream
                                        .write_all(
                                            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                        )
                                        .expect("write retryable loopback response");
                                }
                                LoopbackResponse::Hold => {
                                    while !thread_shutdown.load(Ordering::SeqCst) {
                                        std::thread::sleep(Duration::from_millis(1));
                                    }
                                }
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => panic!("accept loopback HTTP connection: {error}"),
                    }
                }
            });
            Self {
                address,
                accepted,
                requests,
                shutdown,
                thread: Some(thread),
            }
        }

        fn endpoint(&self) -> String {
            format!("http://{}/credentials", self.address)
        }
    }

    impl Drop for LoopbackHttpServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.address);
            if let Some(thread) = self.thread.take() {
                // A server thread that panicked is worth reporting, but never
                // from a destructor that is already unwinding. Rust turns a
                // panic raised there into a non-unwinding panic and aborts the
                // test binary, which destroys every other test's result and
                // buries the failure being reported.
                assert!(
                    thread.join().is_ok() || std::thread::panicking(),
                    "join loopback HTTP server"
                );
            }
        }
    }

    async fn retryable_loopback_get(
        client: reqwest::Client,
        endpoint: String,
    ) -> crate::iceberg::Result<()> {
        let response = client.get(endpoint).send().await.map_err(|error| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::Unexpected,
                format!("loopback credential request failed: {error}"),
            )
        })?;
        if response.status().is_server_error() {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::Unexpected,
                "loopback credential request received retryable HTTP status",
            ));
        }
        Ok(())
    }

    fn input(prefix: &str, expiration: u64, suffix: &str) -> RestCredentialInput {
        RestCredentialInput {
            prefix: prefix.to_string(),
            config: BTreeMap::from([
                (S3_ACCESS_KEY_ID.to_string(), format!("access-{suffix}")),
                (S3_SECRET_ACCESS_KEY.to_string(), format!("secret-{suffix}")),
                (S3_SESSION_TOKEN.to_string(), format!("token-{suffix}")),
                (
                    S3_SESSION_TOKEN_EXPIRES_AT_MS.to_string(),
                    expiration.to_string(),
                ),
                (
                    CLIENT_REFRESH_CREDENTIALS_ENABLED.to_string(),
                    "true".to_string(),
                ),
                (
                    CLIENT_REFRESH_CREDENTIALS_ENDPOINT.to_string(),
                    "https://catalog.example.test/v1/credentials".to_string(),
                ),
            ]),
        }
    }

    fn seed(inputs: Vec<RestCredentialInput>) -> super::IcebergVendedCredentialLeaseSeed {
        match parse_vended_s3_credentials_at(inputs, 100).expect("valid vended response") {
            IcebergAccessDelegation::Vended(seed) => seed,
            IcebergAccessDelegation::Static => panic!("vended response must create a seed"),
        }
    }

    fn physical_table(
        metadata_location: &str,
        owner: &tokio::runtime::Runtime,
        runtime: crate::iceberg::Runtime,
    ) -> super::IcebergPhysicalTable {
        let binding = fresh_binding(owner);
        let schema = crate::iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
                1,
                "id",
                crate::iceberg::spec::Type::Primitive(crate::iceberg::spec::PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "s3://warehouse/data/table".to_string(),
            crate::iceberg::spec::FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = crate::iceberg::table::Table::builder()
            .runtime(runtime)
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).expect("identifier"))
            .file_io(crate::fs_io::build_file_io_for_location(
                "s3://warehouse/data/table",
                binding,
            ))
            .metadata(metadata)
            .metadata_location(metadata_location.to_string())
            .build()
            .expect("table");
        super::IcebergPhysicalTable::new(table)
    }

    fn fresh_binding(
        runtime: &tokio::runtime::Runtime,
    ) -> crate::access_binding::IcebergReadBinding {
        let handle = runtime.handle().clone();
        crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle)),
        )
    }

    fn table_with_metadata_location(
        source: &super::IcebergPhysicalTable,
        metadata_location: &str,
        runtime: crate::iceberg::Runtime,
    ) -> crate::iceberg::table::Table {
        crate::iceberg::table::Table::builder()
            .runtime(runtime)
            .identifier(source.table.identifier().clone())
            .file_io(source.table.file_io().clone())
            .metadata(source.table.metadata_ref())
            .metadata_location(metadata_location.to_string())
            .build()
            .expect("table")
    }

    fn catalog_runtime() -> (
        tokio::runtime::Runtime,
        crate::resources::IcebergCatalogRuntime,
    ) {
        let owner = tokio::runtime::Runtime::new().expect("runtime");
        let runtime = crate::resources::IcebergCatalogRuntime::new(owner.handle().clone());
        (owner, runtime)
    }

    #[test]
    fn vended_refresh_retries_one_unavailable_read_inside_its_call_budget() {
        let (_owner, runtime) = catalog_runtime();
        let attempts = Arc::new(AtomicU8::new(0));
        let observed = Arc::clone(&attempts);
        let policy = VendedS3CredentialRefreshCallPolicy::try_new(
            Duration::from_millis(100),
            NonZeroU8::new(2).expect("nonzero attempts"),
            Duration::from_millis(1),
            Arc::new(PermittedDispatch),
        )
        .expect("bounded policy");

        let value = run_vended_refresh_with_policy(&runtime, policy, "test refresh", move || {
            let observed = Arc::clone(&observed);
            async move {
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::Unexpected,
                        "transient read failure",
                    ))
                } else {
                    Ok(7_u8)
                }
            }
        })
        .expect("second GET succeeds inside the budget");

        assert_eq!(value, 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn vended_refresh_times_out_the_provider_future_not_its_outer_worker() {
        let (_owner, runtime) = catalog_runtime();
        let policy = VendedS3CredentialRefreshCallPolicy::try_new(
            Duration::from_millis(10),
            NonZeroU8::new(1).expect("nonzero attempts"),
            Duration::ZERO,
            Arc::new(PermittedDispatch),
        )
        .expect("single-attempt policy permits no retry delay");

        let error = run_vended_refresh_with_policy::<(), _, _>(
            &runtime,
            policy,
            "test refresh",
            || async { std::future::pending::<crate::iceberg::Result<()>>().await },
        )
        .expect_err("the provider future must obey its own deadline");

        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
    }

    #[test]
    fn vended_refresh_deadline_covers_real_loopback_connect_failures_and_retries() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
        let endpoint = format!(
            "http://{}/credentials",
            listener.local_addr().expect("read reserved loopback port")
        );
        drop(listener);

        let (_owner, runtime) = catalog_runtime();
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("build loopback HTTP client");
        let policy = VendedS3CredentialRefreshCallPolicy::try_new(
            Duration::from_millis(50),
            NonZeroU8::new(u8::MAX).expect("nonzero attempts"),
            Duration::from_millis(5),
            Arc::new(PermittedDispatch),
        )
        .expect("bounded policy");

        let error = run_vended_refresh_with_policy(&runtime, policy, "test refresh", move || {
            retryable_loopback_get(client.clone(), endpoint.clone())
        })
        .expect_err("connect failures must consume the call-local deadline");

        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
    }

    #[test]
    fn vended_refresh_deadline_covers_a_real_response_read_hold() {
        let server = LoopbackHttpServer::start(LoopbackResponse::Hold);
        let endpoint = server.endpoint();
        let (_owner, runtime) = catalog_runtime();
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("build loopback HTTP client");
        let policy = VendedS3CredentialRefreshCallPolicy::try_new(
            Duration::from_millis(50),
            NonZeroU8::new(1).expect("nonzero attempts"),
            Duration::ZERO,
            Arc::new(PermittedDispatch),
        )
        .expect("single-attempt policy");

        let error = run_vended_refresh_with_policy(&runtime, policy, "test refresh", move || {
            retryable_loopback_get(client.clone(), endpoint.clone())
        })
        .expect_err("a response read hold must not outlive the call-local deadline");

        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
        assert!(server.accepted.load(Ordering::SeqCst));
        assert_eq!(server.requests.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn vended_refresh_deadline_covers_real_retryable_http_responses() {
        let server = LoopbackHttpServer::start(LoopbackResponse::ServiceUnavailable);
        let endpoint = server.endpoint();
        let (_owner, runtime) = catalog_runtime();
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("build loopback HTTP client");
        let policy = VendedS3CredentialRefreshCallPolicy::try_new(
            Duration::from_millis(50),
            NonZeroU8::new(u8::MAX).expect("nonzero attempts"),
            Duration::from_millis(5),
            Arc::new(PermittedDispatch),
        )
        .expect("bounded policy");

        let error = run_vended_refresh_with_policy(&runtime, policy, "test refresh", move || {
            retryable_loopback_get(client.clone(), endpoint.clone())
        })
        .expect_err("retryable HTTP responses must not escape the call-local deadline");

        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
        assert!(server.requests.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn vended_refresh_does_not_retry_a_nonretryable_provider_read_failure() {
        let (_owner, runtime) = catalog_runtime();
        let attempts = Arc::new(AtomicU8::new(0));
        let observed = Arc::clone(&attempts);
        let policy = VendedS3CredentialRefreshCallPolicy::try_new(
            Duration::from_millis(100),
            NonZeroU8::new(2).expect("nonzero attempts"),
            Duration::from_millis(1),
            Arc::new(PermittedDispatch),
        )
        .expect("bounded policy");

        let error = run_vended_refresh_with_policy::<(), _, _>(
            &runtime,
            policy,
            "test refresh",
            move || {
                let observed = Arc::clone(&observed);
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Err(crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "malformed credential response",
                    ))
                }
            },
        )
        .expect_err("nonretryable response must stop at one call");

        assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
        assert!(!error.retryable_before_progress());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn vended_refresh_fences_retry_after_the_first_provider_request_returns_retryable() {
        let (_owner, runtime) = catalog_runtime();
        let attempts = Arc::new(AtomicU8::new(0));
        let observed = Arc::clone(&attempts);
        let permit = Arc::new(AtomicBool::new(true));
        let revoked = Arc::clone(&permit);
        let policy = VendedS3CredentialRefreshCallPolicy::try_new(
            Duration::from_millis(100),
            NonZeroU8::new(2).expect("nonzero attempts"),
            Duration::from_millis(1),
            Arc::new(ToggleDispatch(Arc::clone(&permit))),
        )
        .expect("bounded policy");

        let error = run_vended_refresh_with_policy::<(), _, _>(
            &runtime,
            policy,
            "test refresh",
            move || {
                let observed = Arc::clone(&observed);
                let revoked = Arc::clone(&revoked);
                async move {
                    let call = observed.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        revoked.store(false, Ordering::SeqCst);
                        Err(crate::iceberg::Error::new(
                            crate::iceberg::ErrorKind::Unexpected,
                            "first request is retryable",
                        ))
                    } else {
                        Ok(())
                    }
                }
            },
        )
        .expect_err("a terminal attempt must fence the retry dispatch");

        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn vended_seed_selects_the_longest_matching_prefix() {
        let seed = seed(vec![
            input("s3://warehouse/", 600, "root-canary"),
            input("s3://warehouse/data", 700, "data-canary"),
            input("s3://warehouse/data/delete", 800, "delete-canary"),
        ]);

        let selected = seed
            .select_for_location("s3://warehouse/data/delete/part-0.parquet")
            .expect("longest prefix");
        assert_eq!(selected.prefix.as_str(), "s3://warehouse/data/delete");
        assert_eq!(seed.not_after_unix_ms(), 600);
        assert!(seed.refresh_capable());
        // `IcebergVendedCredentialLeaseSeed` has no `Debug` implementation,
        // so this provider-private source cannot be formatted into logs.
    }

    #[test]
    fn vended_seed_rejects_missing_expired_and_ambiguous_prefixes() {
        let mut missing = input("s3://warehouse/data", 600, "missing");
        missing.config.remove(S3_SESSION_TOKEN);
        assert!(parse_vended_s3_credentials_at(vec![missing], 100).is_err());

        assert!(parse_vended_s3_credentials_at(
            vec![input("s3://warehouse/data", 100, "expired")],
            100,
        )
        .is_err());

        assert!(
            parse_vended_s3_credentials_at(
                vec![
                    input("s3://WAREHOUSE/data", 600, "first"),
                    input("s3://warehouse/data", 700, "second"),
                ],
                100,
            )
            .is_err()
        );
    }

    #[test]
    fn vended_refresh_uses_the_standard_default_enabled_contract() {
        let mut no_endpoint = input("s3://warehouse/data", 600, "no-endpoint");
        no_endpoint
            .config
            .remove(CLIENT_REFRESH_CREDENTIALS_ENDPOINT);
        let IcebergAccessDelegation::Vended(no_endpoint) =
            parse_vended_s3_credentials_at(vec![no_endpoint], 100).expect("no refresh endpoint")
        else {
            panic!("vended delegation");
        };
        assert!(!no_endpoint.refresh_capable());

        let mut disabled_with_endpoint = input("s3://warehouse/data", 600, "disabled");
        disabled_with_endpoint.config.insert(
            CLIENT_REFRESH_CREDENTIALS_ENABLED.to_string(),
            "false".to_string(),
        );
        let IcebergAccessDelegation::Vended(disabled_with_endpoint) =
            parse_vended_s3_credentials_at(vec![disabled_with_endpoint], 100)
                .expect("disabled refresh")
        else {
            panic!("vended delegation");
        };
        assert!(!disabled_with_endpoint.refresh_capable());

        let mut disabled = input("s3://warehouse/data", 600, "no-refresh");
        disabled.config.insert(
            CLIENT_REFRESH_CREDENTIALS_ENABLED.to_string(),
            "false".to_string(),
        );
        disabled.config.remove(CLIENT_REFRESH_CREDENTIALS_ENDPOINT);
        assert!(!seed(vec![disabled]).refresh_capable());
    }

    #[test]
    fn load_table_refresh_is_a_closed_acquisition_path() {
        let mut load_only = input("s3://warehouse/data", 600, "load-only");
        load_only.config.remove(CLIENT_REFRESH_CREDENTIALS_ENDPOINT);
        let load_only = seed(vec![load_only]);
        let scope = load_only.load_table_refresh_scope();
        assert!(scope.matches_seed(&load_only));
        assert!(matches!(
            load_only.renewal_capability(),
            super::IcebergVendedS3RenewalCapability::LoadTableDelegation(_)
        ));
        assert!(load_only.staged_refresh_scope().is_err());

        let endpoint = seed(vec![input("s3://warehouse/data", 700, "endpoint-refresh")]);
        assert!(!scope.matches_seed(&endpoint));
        assert!(matches!(
            endpoint.renewal_capability(),
            super::IcebergVendedS3RenewalCapability::CredentialsEndpoint(_)
        ));
        assert!(endpoint.staged_refresh_scope().is_ok());
    }

    #[test]
    fn load_table_reacquisition_keeps_the_frozen_metadata_binding() {
        let (owner, runtime) = catalog_runtime();
        let mut delegation = input("s3://warehouse/data", 600, "same-object");
        delegation
            .config
            .remove(CLIENT_REFRESH_CREDENTIALS_ENDPOINT);
        let delegation = seed(vec![delegation]);
        let frozen = physical_table(
            "s3://warehouse/data/table/metadata/v1.json",
            &owner,
            runtime.iceberg_runtime().clone(),
        )
        .with_attempt_access(delegation.renewal_capability());
        let observed = table_with_metadata_location(
            &frozen,
            "s3://warehouse/data/table/metadata/v2.json",
            runtime.iceberg_runtime().clone(),
        );
        let access = super::IcebergAttemptTableAccess::freeze(frozen.clone());

        access
            .validate_attempt_reacquisition(&observed, &delegation)
            .expect("same UUID and original resource scope");
        assert_eq!(
            frozen.table.metadata_location(),
            Some("s3://warehouse/data/table/metadata/v1.json")
        );

        let different_object = physical_table(
            "s3://warehouse/data/table/metadata/v3.json",
            &owner,
            runtime.iceberg_runtime().clone(),
        );
        let error = access
            .validate_attempt_reacquisition(&different_object.table, &delegation)
            .expect_err("a same-name table with a different UUID must be rejected");
        assert!(error.to_string().contains("different table UUID"));

        let mut broader_scope = input("s3://warehouse/", 600, "broader-scope");
        broader_scope
            .config
            .remove(CLIENT_REFRESH_CREDENTIALS_ENDPOINT);
        let broader_scope = seed(vec![broader_scope]);
        access
            .validate_attempt_reacquisition(&observed, &broader_scope)
            .expect("a later attempt may receive a different scope that covers frozen resources");

        let mut outside_scope = input("s3://other/data", 600, "wrong-scope");
        outside_scope
            .config
            .remove(CLIENT_REFRESH_CREDENTIALS_ENDPOINT);
        let outside_scope = seed(vec![outside_scope]);
        let error = access
            .validate_attempt_reacquisition(&observed, &outside_scope)
            .expect_err("delegation must cover the original bound resources");
        assert!(error.to_string().contains("no matching prefix"));
    }

    #[test]
    fn credentials_endpoint_reacquisition_adopts_new_covering_prefixes() {
        let initial = seed(vec![input("s3://warehouse/data/", 600, "initial")]);
        let original_scope = initial.refresh_scope().expect("initial refresh scope");
        let mut broader = input("s3://warehouse/", 900, "replacement");
        broader.config.remove(CLIENT_REFRESH_CREDENTIALS_ENDPOINT);
        let broader = seed(vec![broader]);

        assert!(!original_scope.matches_seed(&broader));
        let replacement_scope = original_scope
            .reacquired_for_seed(&broader)
            .expect("replacement attempt scope");
        assert!(replacement_scope.matches_seed(&broader));
        assert_eq!(
            replacement_scope.endpoint(),
            "https://catalog.example.test/v1/credentials"
        );

        let mut changed_endpoint = input("s3://warehouse/", 900, "changed-endpoint");
        changed_endpoint.config.insert(
            CLIENT_REFRESH_CREDENTIALS_ENDPOINT.to_string(),
            "https://other.example.test/v1/credentials".to_string(),
        );
        assert!(
            replacement_scope
                .reacquired_for_seed(&seed(vec![changed_endpoint]))
                .is_err()
        );
    }

    #[test]
    fn credentials_endpoint_attempt_acquisition_keeps_frozen_metadata() {
        let (owner, runtime) = catalog_runtime();
        let initial = seed(vec![input("s3://warehouse/data/", 600, "initial")]);
        let frozen = physical_table(
            "s3://warehouse/data/table/metadata/v1.json",
            &owner,
            runtime.iceberg_runtime().clone(),
        )
        .with_attempt_access(initial.renewal_capability());
        let observed = table_with_metadata_location(
            &frozen,
            "s3://warehouse/data/table/metadata/v2.json",
            runtime.iceberg_runtime().clone(),
        );
        let access = super::IcebergAttemptTableAccess::freeze(frozen);
        let replacement = seed(vec![input("s3://warehouse/", 900, "replacement")]);

        let scope = access
            .validate_credentials_endpoint_attempt_acquisition(&observed, &replacement)
            .expect("the current attempt may acquire a broader lease for frozen metadata");
        assert!(scope.matches_seed(&replacement));

        let different_table = physical_table(
            "s3://warehouse/data/table/metadata/v3.json",
            &owner,
            runtime.iceberg_runtime().clone(),
        );
        assert!(
            access
                .validate_credentials_endpoint_attempt_acquisition(
                    &different_table.table,
                    &replacement
                )
                .is_err()
        );
    }

    #[test]
    fn reacquired_request_scope_rebinds_access_without_changing_frozen_metadata() {
        let (owner, runtime) = catalog_runtime();
        let mut delegation = input("s3://warehouse/data", 600, "first-attempt");
        delegation
            .config
            .remove(CLIENT_REFRESH_CREDENTIALS_ENDPOINT);
        let delegation = seed(vec![delegation]);
        let frozen = physical_table(
            "s3://warehouse/data/table/metadata/v1.json",
            &owner,
            runtime.iceberg_runtime().clone(),
        )
        .with_attempt_access(delegation.renewal_capability());
        let old_file_io = frozen.table.file_io() as *const _;
        let access = super::IcebergAttemptTableAccess::freeze(frozen.clone());

        let reacquired = access
            .reacquired_request_scoped(fresh_binding(&owner), runtime.iceberg_runtime().clone())
            .expect("a new request scope can bind the frozen object");

        assert_eq!(
            reacquired.table.metadata_location(),
            frozen.table.metadata_location()
        );
        assert_eq!(
            reacquired.table.metadata().uuid(),
            frozen.table.metadata().uuid()
        );
        assert_eq!(
            reacquired.table.metadata().current_snapshot_id(),
            frozen.table.metadata().current_snapshot_id()
        );
        assert_ne!(
            reacquired.table.file_io() as *const _,
            old_file_io,
            "the replacement attempt must own a newly rebound FileIO"
        );
    }

    #[test]
    fn vended_scope_equality_ignores_rotated_material_and_expiration() {
        let first = seed(vec![input("s3://warehouse/data", 600, "first")]);
        let second = seed(vec![input("s3://warehouse/data", 900, "second")]);
        assert!(first.has_same_scope(&second));
        assert_eq!(
            second.refresh_endpoint(),
            Some("https://catalog.example.test/v1/credentials")
        );
    }

    #[test]
    fn vended_refresh_scope_retains_initial_endpoint_when_response_omits_it() {
        let initial = seed(vec![input("s3://warehouse/data", 600, "initial")]);
        let scope = initial.refresh_scope().expect("initial refresh scope");

        let mut omitted_endpoint = input("s3://warehouse/data", 900, "rotated");
        omitted_endpoint
            .config
            .remove(CLIENT_REFRESH_CREDENTIALS_ENDPOINT);
        let omitted_endpoint = seed(vec![omitted_endpoint]);
        assert!(scope.matches_seed(&omitted_endpoint));

        let mut changed_endpoint = input("s3://warehouse/data", 900, "changed");
        changed_endpoint.config.insert(
            CLIENT_REFRESH_CREDENTIALS_ENDPOINT.to_string(),
            "https://other.example.test/v1/credentials".to_string(),
        );
        let changed_endpoint = seed(vec![changed_endpoint]);
        assert!(!scope.matches_seed(&changed_endpoint));
    }

    #[test]
    fn vended_seed_moves_directly_into_a_query_lease_contribution() {
        let contribution = seed(vec![
            input("s3://warehouse/", 600, "root"),
            input("s3://warehouse/data", 700, "data"),
        ])
        .into_vended_s3_credential_lease_contribution()
        .expect("contribution");

        assert_eq!(contribution.entries().len(), 2);
        // The renewal path is routed through the generation's session by the
        // lease former (L15); a bare endpoint is never announced.
        assert_eq!(contribution.renewal_path(), None);
        // The only consuming API moves every secret scalar into the collector;
        // there is no Clone, Debug, or serialization path for this payload.
        let (entries, _) = contribution.into_parts();
        assert_eq!(entries.len(), 2);
    }

    /// FE renewal through the generation's REST endpoints (L27): both paths
    /// acquire with NovaRocks requests on the generation's one session, and
    /// a 401 stays a retryable request-layer failure (L17).
    mod rest_renewal {
        use std::collections::{BTreeMap, HashMap};
        use std::num::NonZeroU8;
        use std::sync::Arc;
        use std::time::Duration;

        use novarocks_spi::connector::{
            ConnectorErrorKind, ConnectorVendedS3CredentialLeaseRefresher,
            VendedS3CredentialRefreshCallPolicy,
        };

        use super::PermittedDispatch;
        use crate::loaded_table::{
            IcebergAccessDelegation, IcebergRestLoadTableVendedS3LeaseRefresher,
            IcebergRestVendedS3LeaseRefresher, parse_rest_delegation,
        };
        use crate::rest_protocol::deferred::RestDelegation;
        use crate::rest_protocol::endpoints::RestEndpoints;
        use crate::rest_protocol::minimal_view::{RawStorageCredential, StorageCredentials};
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};

        const PREFIX: &str = "s3://warehouse/db/t";
        const TABLE_UUID: &str = "6f0b1b2e-9c4a-4d2e-8f6a-0a1b2c3d4e5f";

        fn far_future_ms() -> String {
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                + 3_600_000)
                .to_string()
        }

        fn credential_config(refresh_endpoint: Option<&str>) -> BTreeMap<String, String> {
            let mut config = BTreeMap::from([
                ("s3.access-key-id".to_string(), "AKIA-renewed".to_string()),
                ("s3.secret-access-key".to_string(), "secret".to_string()),
                ("s3.session-token".to_string(), "session".to_string()),
                (
                    "s3.session-token-expires-at-ms".to_string(),
                    far_future_ms(),
                ),
            ]);
            if let Some(endpoint) = refresh_endpoint {
                config.insert(
                    "client.refresh-credentials-endpoint".to_string(),
                    endpoint.to_string(),
                );
            }
            config
        }

        fn seed(
            refresh_endpoint: Option<&str>,
        ) -> crate::loaded_table::IcebergVendedCredentialLeaseSeed {
            let delegation = RestDelegation::new(StorageCredentials::Present(vec![
                RawStorageCredential::from_decoded(
                    PREFIX.to_string(),
                    credential_config(refresh_endpoint),
                ),
            ]));
            match parse_rest_delegation(delegation).expect("vended seed") {
                IcebergAccessDelegation::Vended(seed) => seed,
                IcebergAccessDelegation::Static => panic!("static delegation"),
            }
        }

        fn credentials_json(refresh_endpoint: Option<&str>) -> serde_json::Value {
            serde_json::json!([{"prefix": PREFIX, "config": credential_config(refresh_endpoint)}])
        }

        fn policy() -> VendedS3CredentialRefreshCallPolicy {
            VendedS3CredentialRefreshCallPolicy::try_new(
                Duration::from_secs(5),
                NonZeroU8::new(2).expect("nonzero attempts"),
                Duration::from_millis(1),
                Arc::new(PermittedDispatch),
            )
            .expect("bounded policy")
        }

        fn generation(owner: &tokio::runtime::Runtime, server: &TestServer) -> Arc<RestEndpoints> {
            owner.block_on(async {
                crate::catalog_runtime::build_vended_rest_generation_for_test(
                    HashMap::from([("uri".to_string(), server.url())]),
                    None,
                    crate::iceberg::Runtime::try_current().expect("runtime"),
                )
                .await
                .expect("REST generation")
            })
        }

        #[test]
        fn separate_refresher_instances_keep_the_original_generation_acquisition_identity() {
            let server = TestServer::start(Script::new().on(
                "GET",
                "/v1/config",
                Reply::config(&[], &[], None),
            ));
            let endpoint = format!("{}/credentials", server.url());
            let (owner, runtime) = super::catalog_runtime();
            let endpoints = generation(&owner, &server);
            // A production lease follows a read on this original session.
            // Establish that same session in the fixture before routing its lease.
            let binding = owner
                .block_on(endpoints.binding())
                .expect("original REST session");
            let routed = crate::rest_protocol::routing::route_for_lease(
                &binding,
                &endpoints.route_admission(),
                crate::rest_protocol::routing::RouteTarget::CredentialsEndpoint(&endpoint),
            )
            .unwrap();
            let original_path =
                novarocks_spi::connector::CredentialRenewalPath::CredentialsEndpoint(routed);
            let make = || {
                let observed = seed(Some(&endpoint));
                let scope = observed.refresh_scope().unwrap();
                let contribution = observed
                    .into_vended_s3_credential_lease_contribution()
                    .unwrap()
                    .with_renewal_path(original_path.clone())
                    .with_refresher(
                        endpoints.acquisition_owner(),
                        Arc::new(IcebergRestVendedS3LeaseRefresher::new(
                            Arc::clone(&endpoints),
                            runtime.clone(),
                            scope,
                        )),
                    )
                    .unwrap();
                contribution.into_parts_with_refresher().2.unwrap()
            };
            let first = make();
            let second = make();
            assert!(!Arc::ptr_eq(first.refresher(), second.refresher()));
            assert_eq!(first.identity(), second.identity());
            assert_eq!(first.identity().renewal_path(), Some(&original_path));
            assert_eq!(first.identity().prefixes().len(), 1);
            assert_eq!(first.identity().prefixes()[0].as_str(), PREFIX);
            assert_eq!(
                endpoints.acquisition_owner(),
                Arc::clone(&endpoints).acquisition_owner()
            );
            let another_generation_owner = generation(&owner, &server);
            assert_ne!(
                endpoints.acquisition_owner(),
                another_generation_owner.acquisition_owner()
            );
            assert_eq!(
                server.count("GET", "/credentials"),
                0,
                "forming keys does not acquire"
            );
        }

        #[test]
        fn credentials_endpoint_renewal_uses_the_generation_session() {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on(
                        "GET",
                        "/credentials",
                        Reply::json(
                            200,
                            &serde_json::json!({"storage-credentials": credentials_json(None)}),
                        ),
                    ),
            );
            let endpoint = format!("{}/credentials", server.url());
            let (owner, runtime) = super::catalog_runtime();
            let endpoints = generation(&owner, &server);
            let scope = seed(Some(&endpoint))
                .refresh_scope()
                .expect("refresh scope");
            let refresher = IcebergRestVendedS3LeaseRefresher::new(endpoints, runtime, scope);

            refresher
                .refresh_vended_s3_credentials(policy())
                .expect("renewed");

            let renewals = server.requests_to("GET", "/credentials");
            assert_eq!(renewals.len(), 1);
            assert_eq!(renewals[0].header("x-iceberg-access-delegation"), None);
            assert_eq!(server.count("GET", "/v1/config"), 1);
        }

        #[test]
        fn load_table_renewal_checks_the_uuid_from_the_minimal_view() {
            let metadata = serde_json::json!({"table-uuid": TABLE_UUID, "snapshots": []});
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on(
                        "GET",
                        "/v1/namespaces/db/tables/t",
                        Reply::json(
                            200,
                            &serde_json::json!({
                                "metadata-location": "s3://warehouse/db/t/metadata/1.json",
                                "metadata": metadata,
                                "storage-credentials": credentials_json(None),
                            }),
                        ),
                    ),
            );
            let (owner, runtime) = super::catalog_runtime();
            let endpoints = generation(&owner, &server);
            let table = crate::iceberg::TableIdent::new(
                crate::iceberg::NamespaceIdent::new("db".to_string()),
                "t".to_string(),
            );
            let scope = seed(None).load_table_refresh_scope();

            IcebergRestLoadTableVendedS3LeaseRefresher::new(
                Arc::clone(&endpoints),
                runtime.clone(),
                table.clone(),
                uuid::Uuid::parse_str(TABLE_UUID).unwrap(),
                scope,
            )
            .refresh_vended_s3_credentials(policy())
            .expect("renewed");
            let Err(other) = IcebergRestLoadTableVendedS3LeaseRefresher::new(
                endpoints,
                runtime,
                table,
                uuid::Uuid::new_v4(),
                seed(None).load_table_refresh_scope(),
            )
            .refresh_vended_s3_credentials(policy()) else {
                panic!("another table incarnation must be refused");
            };
            assert!(!other.retryable_before_progress());

            let loads = server.requests_to("GET", "/v1/namespaces/db/tables/t");
            assert_eq!(loads.len(), 2);
            assert!(loads.iter().all(|request| {
                request.header("x-iceberg-access-delegation") == Some("vended-credentials")
            }));
        }

        #[test]
        fn a_401_renewal_is_retried_and_stays_a_request_layer_failure() {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on("GET", "/credentials", Reply::status(401)),
            );
            let endpoint = format!("{}/credentials", server.url());
            let (owner, runtime) = super::catalog_runtime();
            let endpoints = generation(&owner, &server);
            let scope = seed(Some(&endpoint))
                .refresh_scope()
                .expect("refresh scope");

            let Err(error) = IcebergRestVendedS3LeaseRefresher::new(endpoints, runtime, scope)
                .refresh_vended_s3_credentials(policy())
            else {
                panic!("a 401 renewal must fail");
            };

            assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
            assert!(error.retryable_before_progress());
            assert_eq!(
                server.count("GET", "/credentials"),
                2,
                "retried within the policy"
            );
        }
    }
}
