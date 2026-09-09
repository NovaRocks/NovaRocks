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
    DeferredRest(crate::iceberg_catalog_rest::DeferredRestTableMaterialization),
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
        materialization: crate::iceberg_catalog_rest::DeferredRestTableMaterialization,
        access_delegation: IcebergAccessDelegation,
    ) -> Self {
        Self {
            materialization: IcebergLoadedTableMaterialization::DeferredRest(materialization),
            access_delegation,
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
            Self::DeferredRest(materialization) => materialization
                .materialize_with_file_io(crate::fs_io::build_file_io_for_location("", binding))
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

    /// Inspect only the immutable object identity from a fresh Catalog
    /// response. The placeholder FileIO is never used for data or metadata I/O;
    /// it lets the vendored value complete its move-only table construction
    /// without retaining an admitted request's resolver in a lease refresher.
    fn into_table_uuid_for_identity_check(self) -> Result<uuid::Uuid, ConnectorError> {
        match self {
            Self::Materialized(table) => Ok(table.metadata().uuid()),
            Self::DeferredRest(materialization) => materialization
                .materialize_with_file_io(crate::iceberg::io::FileIO::new_with_memory())
                .map(|table| table.metadata().uuid())
                .map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("inspect REST Iceberg table identity: {error}"),
                    )
                }),
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
        VendedS3CredentialLeaseContribution::try_new(entries, self.refresh_endpoint)
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

    pub(crate) fn not_after_unix_ms(&self) -> u64 {
        self.credentials
            .iter()
            .map(IcebergVendedS3Credential::not_after_unix_ms)
            .min()
            .expect("vended credential seed is non-empty")
    }

    pub(crate) fn refresh_capable(&self) -> bool {
        self.refresh_endpoint.is_some()
    }

    pub(crate) fn refresh_endpoint(&self) -> Option<&str> {
        self.refresh_endpoint.as_deref()
    }

    pub(crate) fn prefixes(&self) -> impl Iterator<Item = &StorageCredentialScopePrefix> {
        self.credentials
            .iter()
            .map(IcebergVendedS3Credential::prefix)
    }

    /// Scope equality deliberately excludes material and expiration so a
    /// refresh can replace values without changing the query's access domain.
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
/// It retains the same catalog client allocation and explicit runtime that
/// observed the initial response; no Backend, table cache, or native message
/// can recover either capability.
pub(crate) struct IcebergRestVendedS3LeaseRefresher {
    catalog: Arc<crate::iceberg_catalog_rest::RestCatalog>,
    runtime: crate::resources::IcebergCatalogRuntime,
    scope: IcebergVendedS3RefreshScope,
}

impl IcebergRestVendedS3LeaseRefresher {
    pub(crate) fn new(
        catalog: Arc<crate::iceberg_catalog_rest::RestCatalog>,
        runtime: crate::resources::IcebergCatalogRuntime,
        scope: IcebergVendedS3RefreshScope,
    ) -> Self {
        Self {
            catalog,
            runtime,
            scope,
        }
    }
}

impl ConnectorVendedS3CredentialLeaseRefresher for IcebergRestVendedS3LeaseRefresher {
    fn refresh_vended_s3_credentials(
        &self,
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError> {
        let catalog = Arc::clone(&self.catalog);
        let endpoint = Arc::clone(&self.scope.endpoint);
        let delegation = self
            .runtime
            .block_on(async move {
                catalog
                    .load_credentials_with_access_delegation(endpoint.as_ref())
                    .await
            })
            .map_err(|error| unavailable(format!("run Iceberg REST credential refresh: {error}")))?
            .map_err(|error| unavailable(format!("load Iceberg REST credentials: {error}")))?;
        let refreshed = match parse_vended_access_delegation(&delegation)? {
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

/// FE-local renewal path for REST catalogs that vend credentials only from
/// `load_table` responses.
///
/// The response table is used solely to verify the exact object identity. Its
/// metadata location and snapshot never replace the logical execution's
/// frozen binding.
pub(crate) struct IcebergRestLoadTableVendedS3LeaseRefresher {
    catalog: Arc<crate::iceberg_catalog_rest::RestCatalog>,
    runtime: crate::resources::IcebergCatalogRuntime,
    table: crate::iceberg::TableIdent,
    expected_table_uuid: uuid::Uuid,
    scope: IcebergVendedS3LoadTableScope,
}

impl IcebergRestLoadTableVendedS3LeaseRefresher {
    pub(crate) fn new(
        catalog: Arc<crate::iceberg_catalog_rest::RestCatalog>,
        runtime: crate::resources::IcebergCatalogRuntime,
        table: crate::iceberg::TableIdent,
        expected_table_uuid: uuid::Uuid,
        scope: IcebergVendedS3LoadTableScope,
    ) -> Self {
        Self {
            catalog,
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
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError> {
        let catalog = Arc::clone(&self.catalog);
        let table = self.table.clone();
        let response = self
            .runtime
            .block_on(async move {
                catalog
                    .load_table_deferred_with_access_delegation(&table)
                    .await
            })
            .map_err(|error| {
                unavailable(format!(
                    "run Iceberg REST load-table credential refresh: {error}"
                ))
            })?
            .map_err(|error| {
                unavailable(format!("load Iceberg REST table credentials: {error}"))
            })?;
        let (materialization, delegation) = response.into_parts();
        let refreshed = match parse_vended_access_delegation(&delegation)? {
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
        let observed_uuid = IcebergLoadedTableMaterialization::DeferredRest(materialization)
            .into_table_uuid_for_identity_check()?;
        if observed_uuid != self.expected_table_uuid {
            return Err(invalid(
                "vended REST load-table refresh returned a different table UUID",
            ));
        }
        let (entries, _) = refreshed
            .into_vended_s3_credential_lease_contribution()?
            .into_parts();
        VendedS3CredentialLeaseRefresh::try_new(entries)
    }
}

pub(crate) struct IcebergVendedS3Credential {
    prefix: StorageCredentialScopePrefix,
    material: ObjectStoreSecretMaterial,
    not_after_unix_ms: u64,
}

impl IcebergVendedS3Credential {
    pub(crate) fn prefix(&self) -> &StorageCredentialScopePrefix {
        &self.prefix
    }

    pub(crate) fn not_after_unix_ms(&self) -> u64 {
        self.not_after_unix_ms
    }

    pub(crate) fn secret_material(&self) -> &ObjectStoreSecretMaterial {
        &self.material
    }
}

/// Translate the vendor's redacted views directly into the provider's closed
/// S3 schema.  This is the only adapter allowed to read REST config values.
pub(crate) fn parse_vended_access_delegation(
    delegation: &crate::iceberg_catalog_rest::RestAccessDelegation,
) -> Result<IcebergAccessDelegation, ConnectorError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("system clock before Unix epoch"))?
        .as_millis();
    let now = u64::try_from(now).map_err(|_| invalid("system clock exceeds Unix milliseconds"))?;
    parse_vended_access_delegation_at(delegation, now)
}

fn parse_vended_access_delegation_at(
    delegation: &crate::iceberg_catalog_rest::RestAccessDelegation,
    now_unix_ms: u64,
) -> Result<IcebergAccessDelegation, ConnectorError> {
    if !delegation.is_present() || delegation.is_empty() {
        return Err(invalid(
            "vended REST Iceberg response has no storage credentials",
        ));
    }

    let mut credentials = Vec::new();
    for credential in delegation.credentials() {
        let mut config = std::collections::BTreeMap::new();
        for key in credential.config_keys() {
            // Keep the vendor's raw view in this short-lived adapter only.
            // A missing value violates that view's contract and is refused,
            // rather than silently becoming an empty credential field.
            let value = credential
                .config_value(key)
                .ok_or_else(|| invalid("vended REST credential config value"))?;
            config.insert(key.to_string(), value.to_string());
        }
        credentials.push(RestCredentialInput {
            prefix: credential.prefix().to_string(),
            config,
        });
    }
    parse_vended_s3_credentials_at(credentials, now_unix_ms)
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

    pub(crate) fn attempt_access(
        &self,
    ) -> Result<IcebergVendedS3RenewalCapability, ConnectorError> {
        self.attempt_access
            .clone()
            .ok_or_else(|| invalid("frozen table has no vended attempt access capability"))
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
    ) -> Result<Self, ConnectorError> {
        let metadata_location = table.metadata_location().map(str::to_owned);
        let file_io_location = metadata_location
            .as_deref()
            .unwrap_or_else(|| table.metadata().location());
        let mut builder = crate::iceberg::table::Table::builder()
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

    pub(crate) fn reacquired_request_scoped(
        frozen: &Self,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<Self, ConnectorError> {
        let capability = frozen.attempt_access()?;
        Self::request_scoped(&frozen.table, binding)
            .map(|physical| physical.with_attempt_access(capability))
    }

    /// Rebuild a REST-vended table's FileIO from the request-local resolver.
    /// The source table is consumed so it cannot be retained by a generation
    /// cache or reused by another attempt.
    pub(crate) fn into_request_scoped(
        self,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<Self, ConnectorError> {
        Self::request_scoped(&self.table, binding)
    }

    /// Validate a `load_table` delegation response against an already-frozen
    /// logical input. A matching UUID proves object identity; the response's
    /// metadata location and snapshot deliberately do not replace `frozen`.
    pub(crate) fn validate_attempt_reacquisition(
        frozen: &Self,
        observed: &crate::iceberg::table::Table,
        delegation: &IcebergVendedCredentialLeaseSeed,
    ) -> Result<(), ConnectorError> {
        if observed.metadata().uuid() != frozen.table.metadata().uuid() {
            return Err(invalid(
                "vended REST load-table delegation returned a different table UUID",
            ));
        }
        let IcebergVendedS3RenewalCapability::LoadTableDelegation(expected_scope) =
            frozen.attempt_access()?
        else {
            return Err(invalid(
                "frozen table requires credentials-endpoint attempt access",
            ));
        };
        if !expected_scope.matches_seed(delegation) {
            return Err(invalid(
                "vended REST load-table delegation changed prefix scope or acquisition path",
            ));
        }
        delegation.validate_table_access(&frozen.table)
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use super::{
        CLIENT_REFRESH_CREDENTIALS_ENABLED, CLIENT_REFRESH_CREDENTIALS_ENDPOINT,
        IcebergAccessDelegation, RestCredentialInput, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY,
        S3_SESSION_TOKEN, S3_SESSION_TOKEN_EXPIRES_AT_MS, parse_vended_s3_credentials_at,
    };

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

    fn physical_table(metadata_location: &str) -> super::IcebergPhysicalTable {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let handle = runtime.handle().clone();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle)),
        );
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

    fn table_with_metadata_location(
        source: &super::IcebergPhysicalTable,
        metadata_location: &str,
    ) -> crate::iceberg::table::Table {
        crate::iceberg::table::Table::builder()
            .identifier(source.table.identifier().clone())
            .file_io(source.table.file_io().clone())
            .metadata(source.table.metadata_ref())
            .metadata_location(metadata_location.to_string())
            .build()
            .expect("table")
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
        assert_eq!(selected.prefix().as_str(), "s3://warehouse/data/delete");
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
        let mut delegation = input("s3://warehouse/data", 600, "same-object");
        delegation
            .config
            .remove(CLIENT_REFRESH_CREDENTIALS_ENDPOINT);
        let delegation = seed(vec![delegation]);
        let frozen = physical_table("s3://warehouse/data/table/metadata/v1.json")
            .with_attempt_access(delegation.renewal_capability());
        let observed =
            table_with_metadata_location(&frozen, "s3://warehouse/data/table/metadata/v2.json");

        super::IcebergPhysicalTable::validate_attempt_reacquisition(
            &frozen,
            &observed,
            &delegation,
        )
        .expect("same UUID and original resource scope");
        assert_eq!(
            frozen.table.metadata_location(),
            Some("s3://warehouse/data/table/metadata/v1.json")
        );

        let different_object = physical_table("s3://warehouse/data/table/metadata/v3.json");
        let error = super::IcebergPhysicalTable::validate_attempt_reacquisition(
            &frozen,
            &different_object.table,
            &delegation,
        )
        .expect_err("a same-name table with a different UUID must be rejected");
        assert!(error.to_string().contains("different table UUID"));

        let mut outside_scope = input("s3://other/data", 600, "wrong-scope");
        outside_scope
            .config
            .remove(CLIENT_REFRESH_CREDENTIALS_ENDPOINT);
        let outside_scope = seed(vec![outside_scope]);
        let error = super::IcebergPhysicalTable::validate_attempt_reacquisition(
            &frozen,
            &observed,
            &outside_scope,
        )
        .expect_err("delegation must cover the original bound resources");
        assert!(error.to_string().contains("changed prefix scope"));
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
        assert_eq!(
            contribution.refresh_endpoint(),
            Some("https://catalog.example.test/v1/credentials")
        );
        // The only consuming API moves every secret scalar into the collector;
        // there is no Clone, Debug, or serialization path for this payload.
        let (entries, _) = contribution.into_parts();
        assert_eq!(entries.len(), 2);
    }
}
