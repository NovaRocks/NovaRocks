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

//! This module contains the iceberg REST catalog implementation.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};
use iceberg::spec::{ViewMetadata, ViewVersion};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit,
    TableCreation, TableIdent, TableUpdate, ViewCommit, ViewCreation,
};
use itertools::Itertools;
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, {self},
};
use reqwest::{Client, Method, StatusCode, Url};
use tokio::sync::OnceCell;
use typed_builder::TypedBuilder;

use crate::client::{
    HttpClient, deserialize_catalog_response, deserialize_unexpected_catalog_error,
};
use crate::types::{
    CatalogConfig, CommitTableRequest, CommitTableResponse, CommitViewRequest,
    CreateNamespaceRequest, CreateTableRequest, CreateViewRequest, ListNamespaceResponse,
    ListTablesResponse, ListViewsResponse, LoadCredentialsResult, LoadTableResult, LoadViewResult,
    NamespaceResponse, RegisterTableRequest, RenameTableRequest,
};

/// REST catalog URI
pub const REST_CATALOG_PROP_URI: &str = "uri";
/// REST catalog warehouse location
pub const REST_CATALOG_PROP_WAREHOUSE: &str = "warehouse";
/// Disable header redaction in error logs (defaults to false for security)
pub const REST_CATALOG_PROP_DISABLE_HEADER_REDACTION: &str = "disable-header-redaction";
/// Header used by a caller that requests REST storage-credential delegation.
///
/// The header is deliberately only installed by the parallel access-delegation
/// APIs below. The upstream [`Catalog`] trait keeps its existing behavior.
pub const REST_CATALOG_HEADER_ACCESS_DELEGATION: &str = "x-iceberg-access-delegation";
/// Value for [`REST_CATALOG_HEADER_ACCESS_DELEGATION`] requesting vended credentials.
pub const REST_CATALOG_ACCESS_DELEGATION_VENDED_CREDENTIALS: &str = "vended-credentials";

const ICEBERG_REST_SPEC_VERSION: &str = "0.14.1";
const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const PATH_V1: &str = "v1";

/// Builder for [`RestCatalog`].
#[derive(Debug)]
pub struct RestCatalogBuilder {
    config: RestCatalogConfig,
    storage_factory: Option<Arc<dyn StorageFactory>>,
}

impl Default for RestCatalogBuilder {
    fn default() -> Self {
        Self {
            config: RestCatalogConfig {
                name: None,
                uri: "".to_string(),
                warehouse: None,
                props: HashMap::new(),
                client: None,
            },
            storage_factory: None,
        }
    }
}

impl CatalogBuilder for RestCatalogBuilder {
    type C = RestCatalog;

    fn with_storage_factory(mut self, storage_factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(storage_factory);
        self
    }

    fn load(
        mut self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send {
        self.config.name = Some(name.into());

        if props.contains_key(REST_CATALOG_PROP_URI) {
            self.config.uri = props
                .get(REST_CATALOG_PROP_URI)
                .cloned()
                .unwrap_or_default();
        }

        if props.contains_key(REST_CATALOG_PROP_WAREHOUSE) {
            self.config.warehouse = props.get(REST_CATALOG_PROP_WAREHOUSE).cloned()
        }

        // Collect other remaining properties
        self.config.props = props
            .into_iter()
            .filter(|(k, _)| k != REST_CATALOG_PROP_URI && k != REST_CATALOG_PROP_WAREHOUSE)
            .collect();

        let result = {
            if self.config.name.is_none() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog name is required",
                ))
            } else if self.config.uri.is_empty() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog uri is required",
                ))
            } else {
                Ok(RestCatalog::new(self.config, self.storage_factory))
            }
        };

        std::future::ready(result)
    }
}

impl RestCatalogBuilder {
    /// Configures the catalog with a custom HTTP client.
    pub fn with_client(mut self, client: Client) -> Self {
        self.config.client = Some(client);
        self
    }
}

/// Rest catalog configuration.
#[derive(Clone, Debug, TypedBuilder)]
pub(crate) struct RestCatalogConfig {
    #[builder(default, setter(strip_option))]
    name: Option<String>,

    uri: String,

    #[builder(default, setter(strip_option(fallback = warehouse_opt)))]
    warehouse: Option<String>,

    #[builder(default)]
    props: HashMap<String, String>,

    #[builder(default)]
    client: Option<Client>,
}

impl RestCatalogConfig {
    fn url_prefixed(&self, parts: &[&str]) -> String {
        [&self.uri, PATH_V1]
            .into_iter()
            .chain(self.props.get("prefix").map(|s| &**s))
            .chain(parts.iter().cloned())
            .join("/")
    }

    fn config_endpoint(&self) -> String {
        [&self.uri, PATH_V1, "config"].join("/")
    }

    pub(crate) fn get_token_endpoint(&self) -> String {
        if let Some(oauth2_uri) = self.props.get("oauth2-server-uri") {
            oauth2_uri.to_string()
        } else {
            [&self.uri, PATH_V1, "oauth", "tokens"].join("/")
        }
    }

    fn namespaces_endpoint(&self) -> String {
        self.url_prefixed(&["namespaces"])
    }

    fn namespace_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string()])
    }

    fn tables_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "tables"])
    }

    fn rename_table_endpoint(&self) -> String {
        self.url_prefixed(&["tables", "rename"])
    }

    fn register_table_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "register"])
    }

    fn table_endpoint(&self, table: &TableIdent) -> String {
        self.url_prefixed(&[
            "namespaces",
            &table.namespace.to_url_string(),
            "tables",
            &table.name,
        ])
    }

    fn views_endpoint(&self, ns: &NamespaceIdent) -> String {
        self.url_prefixed(&["namespaces", &ns.to_url_string(), "views"])
    }

    fn view_endpoint(&self, view: &TableIdent) -> String {
        self.url_prefixed(&[
            "namespaces",
            &view.namespace.to_url_string(),
            "views",
            &view.name,
        ])
    }

    /// Get the client from the config.
    pub(crate) fn client(&self) -> Option<Client> {
        self.client.clone()
    }

    /// Get the token from the config.
    ///
    /// The client can use this token to send requests.
    pub(crate) fn token(&self) -> Option<String> {
        self.props.get("token").cloned()
    }

    /// Get the credentials from the config. The client can use these credentials to fetch a new
    /// token.
    ///
    /// ## Output
    ///
    /// - `None`: No credential is set.
    /// - `Some(None, client_secret)`: No client_id is set, use client_secret directly.
    /// - `Some(Some(client_id), client_secret)`: Both client_id and client_secret are set.
    pub(crate) fn credential(&self) -> Option<(Option<String>, String)> {
        let cred = self.props.get("credential")?;

        match cred.split_once(':') {
            Some((client_id, client_secret)) => {
                Some((Some(client_id.to_string()), client_secret.to_string()))
            }
            None => Some((None, cred.to_string())),
        }
    }

    /// Get the extra headers from config, which includes:
    ///
    /// - `content-type`
    /// - `x-client-version`
    /// - `user-agent`
    /// - All headers specified by `header.xxx` in props.
    pub(crate) fn extra_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                HeaderName::from_static("x-client-version"),
                HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
            ),
            (
                header::USER_AGENT,
                HeaderValue::from_str(&format!("iceberg-rs/{CARGO_PKG_VERSION}")).unwrap(),
            ),
        ]);

        for (key, value) in self
            .props
            .iter()
            .filter_map(|(k, v)| k.strip_prefix("header.").map(|k| (k, v)))
        {
            headers.insert(
                HeaderName::from_str(key).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid header name: {key}"),
                    )
                    .with_source(e)
                })?,
                HeaderValue::from_str(value).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid header value: {value}"),
                    )
                    .with_source(e)
                })?,
            );
        }

        Ok(headers)
    }

    /// Get the optional OAuth headers from the config.
    pub(crate) fn extra_oauth_params(&self) -> HashMap<String, String> {
        let mut params = HashMap::new();

        if let Some(scope) = self.props.get("scope") {
            params.insert("scope".to_string(), scope.to_string());
        } else {
            params.insert("scope".to_string(), "catalog".to_string());
        }

        let optional_params = ["audience", "resource"];
        for param_name in optional_params {
            if let Some(value) = self.props.get(param_name) {
                params.insert(param_name.to_string(), value.to_string());
            }
        }

        params
    }

    /// Check if header redaction is disabled in error logs.
    ///
    /// Returns true if the `disable-header-redaction` property is set to "true".
    /// Defaults to false for security (headers are redacted by default).
    pub(crate) fn disable_header_redaction(&self) -> bool {
        self.props
            .get(REST_CATALOG_PROP_DISABLE_HEADER_REDACTION)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Merge the `RestCatalogConfig` with the a [`CatalogConfig`] (fetched from the REST server).
    pub(crate) fn merge_with_config(mut self, mut config: CatalogConfig) -> Self {
        if let Some(uri) = config.overrides.remove("uri") {
            self.uri = uri;
        }

        let mut props = config.defaults;
        props.extend(self.props);
        props.extend(config.overrides);

        self.props = props;
        self
    }
}

#[derive(Debug)]
struct RestContext {
    client: HttpClient,
    /// Runtime config is fetched from rest server and stored here.
    ///
    /// It's could be different from the user config.
    config: RestCatalogConfig,
    /// Properties advertised by `/v1/config`, excluding all user-supplied
    /// properties. Extension capability gates must only consult this map.
    server_properties: HashMap<String, String>,
}

/// Rest catalog implementation.
#[derive(Debug)]
pub struct RestCatalog {
    /// User config is stored as-is and never be changed.
    ///
    /// It could be different from the config fetched from the server and used at runtime.
    user_config: RestCatalogConfig,
    ctx: OnceCell<RestContext>,
    /// Storage factory for creating FileIO instances.
    storage_factory: Option<Arc<dyn StorageFactory>>,
}

/// A REST staged-create result and the authoritative initialization updates
/// required by its first `assert-create` commit.
#[derive(Debug)]
pub struct StagedTableCreate {
    table: Table,
    initialization_updates: Vec<TableUpdate>,
}

/// A redacted view of one prefix-scoped REST storage credential.
///
/// Credential values remain private. Consumers can inspect only the keys they
/// understand and retrieve a value by its exact key while translating this
/// response into their own closed credential type.
pub struct StorageCredentialDelegation<'a> {
    credential: &'a crate::types::StorageCredential,
}

impl StorageCredentialDelegation<'_> {
    /// Prefix to which this credential applies.
    pub fn prefix(&self) -> &str {
        &self.credential.prefix
    }

    /// Return the value for one known provider configuration key.
    pub fn config_value(&self, key: &str) -> Option<&str> {
        self.credential.config.get(key).map(String::as_str)
    }

    /// Return the available provider configuration key names, never values.
    pub fn config_keys(&self) -> impl Iterator<Item = &str> {
        self.credential.config.keys().map(String::as_str)
    }
}

impl Debug for StorageCredentialDelegation<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageCredentialDelegation")
            .field("prefix", &self.credential.prefix)
            .field(
                "config_keys",
                &self.credential.config.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Storage delegation facts received from one REST table response.
///
/// This is intentionally not serializable and has no accessor that returns a
/// raw configuration map. It is a short-lived handoff from the vendored REST
/// client to the provider-owned parser.
pub struct RestAccessDelegation {
    storage_credentials: Option<Vec<crate::types::StorageCredential>>,
}

impl RestAccessDelegation {
    fn new(storage_credentials: Option<Vec<crate::types::StorageCredential>>) -> Self {
        Self {
            storage_credentials,
        }
    }

    /// Whether the REST response carried the `storage-credentials` member.
    pub fn is_present(&self) -> bool {
        self.storage_credentials.is_some()
    }

    /// Number of prefix-scoped credentials in the response.
    pub fn len(&self) -> usize {
        self.storage_credentials.as_ref().map_or(0, Vec::len)
    }

    /// Whether no prefix-scoped credentials were returned.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over redacted prefix-scoped credential views.
    pub fn credentials(&self) -> impl Iterator<Item = StorageCredentialDelegation<'_>> {
        self.storage_credentials
            .iter()
            .flatten()
            .map(|credential| StorageCredentialDelegation { credential })
    }
}

impl Debug for RestAccessDelegation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestAccessDelegation")
            .field("present", &self.is_present())
            .field("credential_count", &self.len())
            .finish()
    }
}

/// One table materialized from a REST response plus its access delegation.
pub struct RestTableWithAccessDelegation {
    table: Table,
    access_delegation: RestAccessDelegation,
}

impl RestTableWithAccessDelegation {
    /// Borrow the materialized Iceberg table.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Borrow the response-local, redacted delegation facts.
    pub fn access_delegation(&self) -> &RestAccessDelegation {
        &self.access_delegation
    }

    /// Consume the response into its table and delegation facts.
    pub fn into_parts(self) -> (Table, RestAccessDelegation) {
        (self.table, self.access_delegation)
    }
}

impl Debug for RestTableWithAccessDelegation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestTableWithAccessDelegation")
            .field("table", &self.table)
            .field("access_delegation", &self.access_delegation)
            .finish()
    }
}

/// A table response whose immutable metadata has not yet been associated with
/// a [`FileIO`].
///
/// This is for providers that must consume response-local credentials before
/// they choose the request-scoped FileIO.  It deliberately exposes neither
/// the response's configuration map nor its metadata: callers can only
/// materialize the already-frozen table with a FileIO they own.
pub struct DeferredRestTableMaterialization {
    table_ident: TableIdent,
    metadata_location: Option<String>,
    metadata: iceberg::spec::TableMetadata,
    // Keep ordinary REST table config private so the catalog can retain its
    // legacy materialization path without exposing a second raw config API.
    config: HashMap<String, String>,
}

impl DeferredRestTableMaterialization {
    /// Materialize this frozen response with an explicitly supplied FileIO.
    ///
    /// A request-scoped provider calls this only after it has converted the
    /// companion access delegation into its own sealed credential capability.
    pub fn materialize_with_file_io(self, file_io: FileIO) -> Result<Table> {
        let table_builder = Table::builder()
            .identifier(self.table_ident)
            .file_io(file_io)
            .metadata(self.metadata);
        match self.metadata_location {
            Some(metadata_location) => table_builder.metadata_location(metadata_location).build(),
            None => table_builder.build(),
        }
    }
}

impl Debug for DeferredRestTableMaterialization {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredRestTableMaterialization")
            .field("table", &self.table_ident)
            .field("has_metadata_location", &self.metadata_location.is_some())
            .field("config_keys", &self.config.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// A REST table response with response-local access delegation that has not
/// yet selected a FileIO.
pub struct DeferredRestTableWithAccessDelegation {
    materialization: DeferredRestTableMaterialization,
    access_delegation: RestAccessDelegation,
}

impl DeferredRestTableWithAccessDelegation {
    /// Consume this response into its opaque table materialization and
    /// response-local delegation facts.
    pub fn into_parts(self) -> (DeferredRestTableMaterialization, RestAccessDelegation) {
        (self.materialization, self.access_delegation)
    }
}

impl Debug for DeferredRestTableWithAccessDelegation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredRestTableWithAccessDelegation")
            .field("materialization", &self.materialization)
            .field("access_delegation", &self.access_delegation)
            .finish()
    }
}

/// A staged table plus response-local storage delegation facts.
pub struct StagedTableCreateWithAccessDelegation {
    table: Table,
    initialization_updates: Vec<TableUpdate>,
    access_delegation: RestAccessDelegation,
}

/// A staged table response whose FileIO is intentionally deferred until a
/// provider has consumed response-local access delegation.
pub struct DeferredStagedTableCreateWithAccessDelegation {
    materialization: DeferredRestTableMaterialization,
    initialization_updates: Vec<TableUpdate>,
    access_delegation: RestAccessDelegation,
}

impl DeferredStagedTableCreateWithAccessDelegation {
    /// Consume this staged response into opaque table materialization, the
    /// server-derived initialization updates, and response-local delegation.
    pub fn into_parts(
        self,
    ) -> (
        DeferredRestTableMaterialization,
        Vec<TableUpdate>,
        RestAccessDelegation,
    ) {
        (
            self.materialization,
            self.initialization_updates,
            self.access_delegation,
        )
    }
}

impl Debug for DeferredStagedTableCreateWithAccessDelegation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredStagedTableCreateWithAccessDelegation")
            .field("materialization", &self.materialization)
            .field(
                "initialization_update_count",
                &self.initialization_updates.len(),
            )
            .field("access_delegation", &self.access_delegation)
            .finish()
    }
}

impl StagedTableCreateWithAccessDelegation {
    /// Return the staged table.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Return the initialization updates required by its first commit.
    pub fn initialization_updates(&self) -> &[TableUpdate] {
        &self.initialization_updates
    }

    /// Borrow the response-local, redacted delegation facts.
    pub fn access_delegation(&self) -> &RestAccessDelegation {
        &self.access_delegation
    }

    /// Consume this result into all of its provider-private facts.
    pub fn into_parts(self) -> (Table, Vec<TableUpdate>, RestAccessDelegation) {
        (
            self.table,
            self.initialization_updates,
            self.access_delegation,
        )
    }
}

impl Debug for StagedTableCreateWithAccessDelegation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedTableCreateWithAccessDelegation")
            .field("table", &self.table)
            .field(
                "initialization_update_count",
                &self.initialization_updates.len(),
            )
            .field("access_delegation", &self.access_delegation)
            .finish()
    }
}

/// Dispatch certainty for the REST staged-create request. Downstream saga
/// owners must never infer this boundary from an error string or a generic
/// `Unexpected` kind.
#[derive(Debug)]
pub enum StagedCreateError {
    /// The server rejected the staged create because the target exists.
    Conflict(Error),
    /// The request was not sent to the staged-create endpoint.
    KnownNotDispatched(Error),
    /// The request may have reached the server and needs reconciliation.
    PossiblyDispatched(Error),
}

/// Dispatch certainty for the assert-create publication request.
#[derive(Debug)]
pub enum StagedCommitError {
    /// The assert-create requirement rejected a concurrent visible table.
    Conflict(Error),
    /// The publication request was not sent.
    KnownNotDispatched(Error),
    /// The publication request may have reached the server.
    PossiblyDispatched(Error),
    /// The server confirmed success but the response could not be finalized.
    CommittedResponseInvalid(Error),
}

impl StagedTableCreate {
    /// Return the staged table. Its metadata location is absent because the
    /// table has not been published yet.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Return initialization updates reconstructed from the staged response.
    pub fn initialization_updates(&self) -> &[TableUpdate] {
        &self.initialization_updates
    }

    /// Consume this result into the staged table and initialization updates.
    pub fn into_parts(self) -> (Table, Vec<TableUpdate>) {
        (self.table, self.initialization_updates)
    }
}

impl RestCatalog {
    /// Creates a `RestCatalog` from a [`RestCatalogConfig`].
    fn new(config: RestCatalogConfig, storage_factory: Option<Arc<dyn StorageFactory>>) -> Self {
        Self {
            user_config: config,
            ctx: OnceCell::new(),
            storage_factory,
        }
    }

    /// Gets the [`RestContext`] from the catalog.
    async fn context(&self) -> Result<&RestContext> {
        self.ctx
            .get_or_try_init(|| async {
                let client = HttpClient::new(&self.user_config)?;
                let catalog_config = RestCatalog::load_config(&client, &self.user_config).await?;
                let server_properties = catalog_config.merged_properties();
                let config = self.user_config.clone().merge_with_config(catalog_config);
                let client = client.update_with(&config)?;

                Ok(RestContext {
                    config,
                    client,
                    server_properties,
                })
            })
            .await
    }

    /// Fetch exactly one REST list-tables page without accumulating later
    /// pages in memory. The token is opaque and must be replayed unchanged.
    pub async fn list_tables_page(
        &self,
        namespace: &NamespaceIdent,
        page_token: Option<&str>,
        page_size: usize,
    ) -> Result<ListTablesResponse> {
        if page_size == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "REST list-tables page size must be non-zero",
            ));
        }
        let context = self.context().await?;
        let endpoint = context.config.tables_endpoint(namespace);
        let page_size = page_size.to_string();
        let mut request = context
            .client
            .request(Method::GET, endpoint)
            .query(&[("pageSize", page_size.as_str())]);
        if let Some(token) = page_token {
            request = request.query(&[("pageToken", token)]);
        }
        let http_response = context.client.query_catalog(request.build()?).await?;
        match http_response.status() {
            StatusCode::OK => {
                deserialize_catalog_response::<ListTablesResponse>(http_response).await
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to list tables of a namespace that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Fetch one REST list-namespaces page, preserving the opaque continuation.
    /// The caller owns pagination; this method never fetches a later page.
    pub async fn list_namespaces_page(
        &self,
        parent: Option<&NamespaceIdent>,
        page_token: Option<&str>,
        page_size: usize,
    ) -> Result<ListNamespaceResponse> {
        if page_size == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "REST list-namespaces page size must be non-zero",
            ));
        }
        let context = self.context().await?;
        let page_size = page_size.to_string();
        let mut request = context
            .client
            .request(Method::GET, context.config.namespaces_endpoint())
            .query(&[("pageSize", page_size.as_str())]);
        if let Some(parent) = parent {
            request = request.query(&[("parent", parent.to_url_string())]);
        }
        if let Some(token) = page_token {
            request = request.query(&[("pageToken", token)]);
        }
        let response = context.client.query_catalog(request.build()?).await?;
        match response.status() {
            StatusCode::OK => deserialize_catalog_response::<ListNamespaceResponse>(response).await,
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "The parent parameter of the namespace provided does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Fetch one REST list-views page, preserving the opaque continuation.
    /// The caller owns pagination; this method never fetches a later page.
    pub async fn list_views_page(
        &self,
        namespace: &NamespaceIdent,
        page_token: Option<&str>,
        page_size: usize,
    ) -> Result<ListViewsResponse> {
        if page_size == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "REST list-views page size must be non-zero",
            ));
        }
        let context = self.context().await?;
        let page_size = page_size.to_string();
        let mut request = context
            .client
            .request(Method::GET, context.config.views_endpoint(namespace))
            .query(&[("pageSize", page_size.as_str())]);
        if let Some(token) = page_token {
            request = request.query(&[("pageToken", token)]);
        }
        let response = context.client.query_catalog(request.build()?).await?;
        match response.status() {
            StatusCode::OK => deserialize_catalog_response::<ListViewsResponse>(response).await,
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::NamespaceNotFound,
                "Tried to list views under a namespace that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Load the runtime config from the server by `user_config`.
    ///
    /// It's required for a REST catalog to update its config after creation.
    async fn load_config(
        client: &HttpClient,
        user_config: &RestCatalogConfig,
    ) -> Result<CatalogConfig> {
        let mut request_builder = client.request(Method::GET, user_config.config_endpoint());

        if let Some(warehouse_location) = &user_config.warehouse {
            request_builder = request_builder.query(&[("warehouse", warehouse_location)]);
        }

        let request = request_builder.build()?;

        let http_response = client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => deserialize_catalog_response(http_response).await,
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn load_file_io(
        &self,
        metadata_location: Option<&str>,
        extra_config: Option<HashMap<String, String>>,
    ) -> Result<FileIO> {
        let mut props = self.context().await?.config.props.clone();
        if let Some(config) = extra_config {
            props.extend(config);
        }

        // If the warehouse is a logical identifier instead of a URL we don't want
        // to raise an exception
        let warehouse_path = match self.context().await?.config.warehouse.as_deref() {
            Some(url) if Url::parse(url).is_ok() => Some(url),
            Some(_) => None,
            None => None,
        };

        if metadata_location.or(warehouse_path).is_none() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Unable to load file io, neither warehouse nor metadata location is set!",
            ));
        }

        // Require a StorageFactory to be provided
        let factory = self
            .storage_factory
            .clone()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "StorageFactory must be provided for RestCatalog. Use `with_storage_factory` to configure it.",
                )
            })?;

        let file_io = FileIOBuilder::new(factory).with_props(props).build();

        Ok(file_io)
    }

    /// Ask the REST service to initialize, but not publish, a table.
    ///
    /// The returned initialization updates are derived from the service's
    /// authoritative metadata and must precede the transaction's snapshot
    /// updates in one `assert-create` table commit.
    pub async fn stage_create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<StagedTableCreate> {
        let table = self
            .create_table_with_stage(namespace, creation, true, false)
            .await?;
        let (table, _) = table.into_parts();
        let initialization_updates = table.metadata().staged_create_initialization_updates()?;
        Ok(StagedTableCreate {
            table,
            initialization_updates,
        })
    }

    /// Stage a table while requesting and retaining REST storage delegation.
    ///
    /// This is a parallel API for provider-owned credential handling. It does
    /// not alter the upstream [`Catalog`] trait or its normal request shape.
    pub async fn stage_create_table_with_access_delegation(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<StagedTableCreateWithAccessDelegation> {
        let table = self
            .create_table_with_stage(namespace, creation, true, true)
            .await?;
        let (table, access_delegation) = table.into_parts();
        let initialization_updates = table.metadata().staged_create_initialization_updates()?;
        Ok(StagedTableCreateWithAccessDelegation {
            table,
            initialization_updates,
            access_delegation,
        })
    }

    /// Create a table while requesting and retaining REST storage delegation.
    ///
    /// The returned delegation is response-local and deliberately separate
    /// from the `Table`'s FileIO configuration.
    pub async fn create_table_with_access_delegation(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<RestTableWithAccessDelegation> {
        self.create_table_with_stage(namespace, creation, false, true)
            .await
    }

    /// Load a table while requesting and retaining REST storage delegation.
    ///
    /// Provider code should use this rather than the upstream [`Catalog`]
    /// trait method when the catalog definition selected vended credentials.
    pub async fn load_table_with_access_delegation(
        &self,
        table_ident: &TableIdent,
    ) -> Result<RestTableWithAccessDelegation> {
        let response = self
            .load_table_deferred_with_access_delegation(table_ident)
            .await?;
        self.materialize_deferred_table_response(response).await
    }

    /// Load a table while retaining its response-local delegation before any
    /// FileIO is constructed.
    ///
    /// This is intentionally separate from [`Self::load_table_with_access_delegation`].
    /// The latter preserves the historical catalog-owned FileIO behavior;
    /// providers using vended credentials must choose a request-scoped FileIO
    /// only after they consume the companion delegation.
    pub async fn load_table_deferred_with_access_delegation(
        &self,
        table_ident: &TableIdent,
    ) -> Result<DeferredRestTableWithAccessDelegation> {
        let context = self.context().await?;
        let request = context
            .client
            .request(Method::GET, context.config.table_endpoint(table_ident))
            .header(
                REST_CATALOG_HEADER_ACCESS_DELEGATION,
                REST_CATALOG_ACCESS_DELEGATION_VENDED_CREDENTIALS,
            )
            .build()?;
        let http_response = context.client.query_catalog(request).await?;
        let response = match http_response.status() {
            StatusCode::OK | StatusCode::NOT_MODIFIED => {
                deserialize_catalog_response::<LoadTableResult>(http_response).await?
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::TableNotFound,
                    "Tried to load a table that does not exist",
                ));
            }
            _ => {
                return Err(deserialize_unexpected_catalog_error(
                    http_response,
                    context.client.disable_header_redaction(),
                )
                .await);
            }
        };
        Ok(self.defer_table_response(table_ident.clone(), response))
    }

    /// Load a fresh set of storage credentials from an endpoint advertised by
    /// a previous vended table response.
    ///
    /// The request deliberately reuses this catalog's initialized context and
    /// authenticated HTTP client. It is not a table load and therefore cannot
    /// observe a later schema or snapshot. The returned facts stay in the
    /// existing response-local redacted wrapper until a provider translates
    /// them into its closed credential type.
    pub async fn load_credentials_with_access_delegation(
        &self,
        credentials_endpoint: &str,
    ) -> Result<RestAccessDelegation> {
        let context = self.context().await?;
        let request = context
            .client
            .request(Method::GET, credentials_endpoint)
            .build()?;
        let http_response = context.client.query_catalog(request).await?;
        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<LoadCredentialsResult>(http_response).await?;
                Ok(RestAccessDelegation::new(response.storage_credentials))
            }
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Typed staged-create variant for durable application sagas.
    pub async fn stage_create_table_typed(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> std::result::Result<StagedTableCreate, StagedCreateError> {
        self.stage_create_table_typed_with_access_delegation(namespace, creation)
            .await
            .map(|response| {
                let (table, initialization_updates, _) = response.into_parts();
                StagedTableCreate {
                    table,
                    initialization_updates,
                }
            })
    }

    /// Typed staged-create variant that retains REST storage delegation.
    ///
    /// Its dispatch certainty matches [`Self::stage_create_table_typed`]; the
    /// only additional fact is the response-local delegation handoff.
    pub async fn stage_create_table_typed_with_access_delegation(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> std::result::Result<StagedTableCreateWithAccessDelegation, StagedCreateError> {
        let staged = self
            .stage_create_table_typed_deferred_with_access_delegation(namespace, creation)
            .await?;
        let (materialization, initialization_updates, access_delegation) = staged.into_parts();
        let table = self
            .materialize_deferred_table_response(DeferredRestTableWithAccessDelegation {
                materialization,
                access_delegation,
            })
            .await
            .map_err(StagedCreateError::PossiblyDispatched)?;
        let (table, access_delegation) = table.into_parts();
        Ok(StagedTableCreateWithAccessDelegation {
            table,
            initialization_updates,
            access_delegation,
        })
    }

    /// Typed staged-create variant that retains its response-local delegation
    /// before constructing a FileIO.
    ///
    /// Vended providers must use this form, collect the sealed credential
    /// contribution, and then materialize with the attempt-scoped resolver.
    pub async fn stage_create_table_typed_deferred_with_access_delegation(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> std::result::Result<DeferredStagedTableCreateWithAccessDelegation, StagedCreateError> {
        let context = self
            .context()
            .await
            .map_err(StagedCreateError::KnownNotDispatched)?;
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let request = context
            .client
            .request(Method::POST, context.config.tables_endpoint(namespace))
            .header(
                REST_CATALOG_HEADER_ACCESS_DELEGATION,
                REST_CATALOG_ACCESS_DELEGATION_VENDED_CREDENTIALS,
            )
            .json(&CreateTableRequest {
                name: creation.name,
                location: creation.location,
                schema: creation.schema,
                partition_spec: creation.partition_spec,
                write_order: creation.sort_order,
                stage_create: Some(true),
                properties: creation.properties,
            })
            .build()
            .map_err(|error| StagedCreateError::KnownNotDispatched(error.into()))?;
        let http_response = context
            .client
            .query_catalog(request)
            .await
            .map_err(StagedCreateError::PossiblyDispatched)?;
        let status = http_response.status();
        let response = match status {
            StatusCode::OK => deserialize_catalog_response::<LoadTableResult>(http_response)
                .await
                .map_err(StagedCreateError::PossiblyDispatched)?,
            StatusCode::CONFLICT => {
                return Err(StagedCreateError::Conflict(Error::new(
                    ErrorKind::TableAlreadyExists,
                    "The table already exists",
                )));
            }
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => {
                return Err(StagedCreateError::PossiblyDispatched(
                    deserialize_unexpected_catalog_error(
                        http_response,
                        context.client.disable_header_redaction(),
                    )
                    .await,
                ));
            }
            _ => {
                return Err(StagedCreateError::KnownNotDispatched(
                    deserialize_unexpected_catalog_error(
                        http_response,
                        context.client.disable_header_redaction(),
                    )
                    .await,
                ));
            }
        };
        let initialization_updates = response
            .metadata
            .staged_create_initialization_updates()
            .map_err(StagedCreateError::PossiblyDispatched)?;
        let DeferredRestTableWithAccessDelegation {
            materialization,
            access_delegation,
        } = self.defer_table_response(table_ident, response);
        Ok(DeferredStagedTableCreateWithAccessDelegation {
            materialization,
            initialization_updates,
            access_delegation,
        })
    }

    /// Materialize the standard REST staged-table result returned inside the
    /// fenced CTAS extension without creating another client or dispatching a
    /// second stage request.
    pub async fn materialize_ctas_staged_table(
        &self,
        table_ident: TableIdent,
        staged_table: serde_json::Value,
    ) -> Result<StagedTableCreate> {
        let response: LoadTableResult = serde_json::from_value(staged_table).map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "Invalid staged table result in CTAS extension response",
            )
            .with_source(error)
        })?;
        let config = response
            .config
            .into_iter()
            .chain(self.user_config.props.clone())
            .collect();
        let file_io_location = response
            .metadata_location
            .as_deref()
            .unwrap_or_else(|| response.metadata.location());
        let file_io = self
            .load_file_io(Some(file_io_location), Some(config))
            .await?;
        let table_builder = Table::builder()
            .identifier(table_ident)
            .file_io(file_io)
            .metadata(response.metadata);
        let table = match response.metadata_location {
            Some(metadata_location) => table_builder.metadata_location(metadata_location).build(),
            None => table_builder.build(),
        }?;
        let initialization_updates = table.metadata().staged_create_initialization_updates()?;
        Ok(StagedTableCreate {
            table,
            initialization_updates,
        })
    }

    /// Encode one standard REST staged-create request as a bounded extension
    /// payload. The fence-aware catalog owns dispatch and linearization; this
    /// helper only reuses the exact request shape of ordinary REST staging.
    pub async fn encode_ctas_stage_provider_payload(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<String> {
        let body = serde_json::to_value(CreateTableRequest {
            name: creation.name,
            location: creation.location,
            schema: creation.schema,
            partition_spec: creation.partition_spec,
            write_order: creation.sort_order,
            stage_create: Some(true),
            properties: creation.properties,
        })?;
        encode_ctas_downstream_action(
            Method::POST,
            self.context().await?.config.tables_endpoint(namespace),
            body,
        )
    }

    /// Encode one standard REST assert-create commit for execution inside the
    /// fence-aware catalog transaction. No downstream request is sent here.
    pub async fn encode_ctas_publish_provider_payload(
        &self,
        mut commit: TableCommit,
    ) -> Result<String> {
        let identifier = commit.identifier().clone();
        let body = serde_json::to_value(CommitTableRequest {
            identifier: Some(identifier.clone()),
            requirements: commit.take_requirements(),
            updates: commit.take_updates(),
        })?;
        encode_ctas_downstream_action(
            Method::POST,
            self.context().await?.config.table_endpoint(&identifier),
            body,
        )
    }

    /// Submit one staged table commit without reloading the invisible table.
    /// The typed result preserves conflict, pre-dispatch, uncertain-dispatch,
    /// and committed-but-unreadable response states.
    pub async fn commit_staged_table_typed(
        &self,
        commit: TableCommit,
    ) -> std::result::Result<Table, StagedCommitError> {
        let materialization = self.commit_staged_table_typed_deferred(commit).await?;
        let metadata_location = materialization
            .metadata_location
            .clone()
            .expect("staged commit response always has a metadata location");
        let file_io = self
            .load_file_io(Some(&metadata_location), None)
            .await
            .map_err(StagedCommitError::CommittedResponseInvalid)?;
        materialization
            .materialize_with_file_io(file_io)
            .map_err(StagedCommitError::CommittedResponseInvalid)
    }

    /// Submit one staged table commit while leaving FileIO selection to the
    /// caller that owns the response-local storage capability.
    ///
    /// A vended-credential provider cannot use [`Self::commit_staged_table_typed`]:
    /// the REST response proves the catalog mutation but does not carry another
    /// storage delegation, while the table response still needs a FileIO. The
    /// provider must retain its already-admitted request lease and materialize
    /// this opaque result with that request-scoped FileIO.
    pub async fn commit_staged_table_typed_deferred(
        &self,
        mut commit: TableCommit,
    ) -> std::result::Result<DeferredRestTableMaterialization, StagedCommitError> {
        let context = self
            .context()
            .await
            .map_err(StagedCommitError::KnownNotDispatched)?;
        let request = context
            .client
            .request(
                Method::POST,
                context.config.table_endpoint(commit.identifier()),
            )
            .json(&CommitTableRequest {
                identifier: Some(commit.identifier().clone()),
                requirements: commit.take_requirements(),
                updates: commit.take_updates(),
            })
            .build()
            .map_err(|error| StagedCommitError::KnownNotDispatched(error.into()))?;
        let http_response = context
            .client
            .query_catalog(request)
            .await
            .map_err(StagedCommitError::PossiblyDispatched)?;
        let status = http_response.status();
        let response: CommitTableResponse = match status {
            StatusCode::OK => deserialize_catalog_response(http_response)
                .await
                .map_err(StagedCommitError::CommittedResponseInvalid)?,
            StatusCode::CONFLICT => {
                return Err(StagedCommitError::Conflict(
                    Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        "CatalogCommitConflicts, one or more requirements failed.",
                    )
                    .with_retryable(true),
                ));
            }
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => {
                return Err(StagedCommitError::PossiblyDispatched(
                    deserialize_unexpected_catalog_error(
                        http_response,
                        context.client.disable_header_redaction(),
                    )
                    .await,
                ));
            }
            _ => {
                return Err(StagedCommitError::KnownNotDispatched(
                    deserialize_unexpected_catalog_error(
                        http_response,
                        context.client.disable_header_redaction(),
                    )
                    .await,
                ));
            }
        };
        Ok(DeferredRestTableMaterialization {
            table_ident: commit.identifier().clone(),
            metadata_location: Some(response.metadata_location),
            metadata: response.metadata,
            config: HashMap::new(),
        })
    }

    /// Submit one staged table commit using a caller-owned FileIO for the
    /// committed response.
    ///
    /// This is the vended counterpart to [`Self::commit_staged_table_typed`].
    /// The caller must supply an already-admitted request-scoped FileIO; this
    /// method never falls back to the catalog-global storage factory.
    pub async fn commit_staged_table_typed_with_file_io(
        &self,
        commit: TableCommit,
        file_io: FileIO,
    ) -> std::result::Result<Table, StagedCommitError> {
        self.commit_staged_table_typed_deferred(commit)
            .await?
            .materialize_with_file_io(file_io)
            .map_err(StagedCommitError::CommittedResponseInvalid)
    }

    async fn create_table_with_stage(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
        stage_create: bool,
        request_access_delegation: bool,
    ) -> Result<RestTableWithAccessDelegation> {
        let context = self.context().await?;
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());

        let request = context
            .client
            .request(Method::POST, context.config.tables_endpoint(namespace));
        let request = if request_access_delegation {
            request.header(
                REST_CATALOG_HEADER_ACCESS_DELEGATION,
                REST_CATALOG_ACCESS_DELEGATION_VENDED_CREDENTIALS,
            )
        } else {
            request
        }
        .json(&CreateTableRequest {
            name: creation.name,
            location: creation.location,
            schema: creation.schema,
            partition_spec: creation.partition_spec,
            write_order: creation.sort_order,
            stage_create: Some(stage_create),
            properties: creation.properties,
        })
        .build()?;

        let http_response = context.client.query_catalog(request).await?;
        let response = match http_response.status() {
            StatusCode::OK => {
                deserialize_catalog_response::<LoadTableResult>(http_response).await?
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::NamespaceNotFound,
                    "Tried to create a table under a namespace that does not exist",
                ));
            }
            StatusCode::CONFLICT => {
                return Err(Error::new(
                    ErrorKind::TableAlreadyExists,
                    "The table already exists",
                ));
            }
            _ => {
                return Err(deserialize_unexpected_catalog_error(
                    http_response,
                    context.client.disable_header_redaction(),
                )
                .await);
            }
        };

        if !stage_create && response.metadata_location.is_none() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Metadata location missing in `create_table` response!",
            ));
        }

        self.materialize_table_response(table_ident, response).await
    }

    async fn materialize_table_response(
        &self,
        table_ident: TableIdent,
        response: LoadTableResult,
    ) -> Result<RestTableWithAccessDelegation> {
        self.materialize_deferred_table_response(self.defer_table_response(table_ident, response))
            .await
    }

    async fn materialize_deferred_table_response(
        &self,
        response: DeferredRestTableWithAccessDelegation,
    ) -> Result<RestTableWithAccessDelegation> {
        let (materialization, access_delegation) = response.into_parts();
        let file_io_location = materialization
            .metadata_location
            .as_deref()
            .unwrap_or_else(|| materialization.metadata.location());
        let file_io = self
            .load_file_io(Some(file_io_location), Some(materialization.config.clone()))
            .await?;
        let table = materialization.materialize_with_file_io(file_io)?;
        Ok(RestTableWithAccessDelegation {
            table,
            access_delegation,
        })
    }

    fn defer_table_response(
        &self,
        table_ident: TableIdent,
        response: LoadTableResult,
    ) -> DeferredRestTableWithAccessDelegation {
        let LoadTableResult {
            metadata_location,
            metadata,
            config,
            storage_credentials,
        } = response;
        DeferredRestTableWithAccessDelegation {
            materialization: DeferredRestTableMaterialization {
                table_ident,
                metadata_location,
                metadata,
                config,
            },
            access_delegation: RestAccessDelegation::new(storage_credentials),
        }
    }

    /// Invalidate the current token without generating a new one. On the next request, the client
    /// will attempt to generate a new token.
    pub async fn invalidate_token(&self) -> Result<()> {
        self.context().await?.client.invalidate_token().await
    }

    /// Invalidate the current token and set a new one. Generates a new token before invalidating
    /// the current token, meaning the old token will be used until this function acquires the lock
    /// and overwrites the token.
    ///
    /// If credential is invalid, or the request fails, this method will return an error and leave
    /// the current token unchanged.
    pub async fn regenerate_token(&self) -> Result<()> {
        self.context().await?.client.regenerate_token().await
    }
}

fn encode_ctas_downstream_action(
    method: Method,
    absolute_url: String,
    body: serde_json::Value,
) -> Result<String> {
    let url = Url::parse(&absolute_url).map_err(|error| {
        Error::new(
            ErrorKind::DataInvalid,
            "Invalid REST endpoint for CTAS downstream action",
        )
        .with_source(error)
    })?;
    let mut path = url.path().to_string();
    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }
    serde_json::to_string(&serde_json::json!({
        "method": method.as_str(),
        "path": path,
        "body": body,
    }))
    .map_err(|error| {
        Error::new(
            ErrorKind::DataInvalid,
            "Failed to encode CTAS downstream REST action",
        )
        .with_source(error)
    })
}

/// All requests and expected responses are derived from the REST catalog API spec:
/// https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
#[async_trait]
impl Catalog for RestCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let context = self.context().await?;
        let endpoint = context.config.namespaces_endpoint();
        let mut namespaces = Vec::new();
        let mut next_token = None;

        loop {
            let mut request = context.client.request(Method::GET, endpoint.clone());

            // Filter on `parent={namespace}` if a parent namespace exists.
            if let Some(ns) = parent {
                request = request.query(&[("parent", ns.to_url_string())]);
            }

            if let Some(token) = next_token {
                request = request.query(&[("pageToken", token)]);
            }

            let http_response = context.client.query_catalog(request.build()?).await?;

            match http_response.status() {
                StatusCode::OK => {
                    let response =
                        deserialize_catalog_response::<ListNamespaceResponse>(http_response)
                            .await?;

                    namespaces.extend(response.namespaces);

                    match response.next_page_token {
                        Some(token) => next_token = Some(token),
                        None => break,
                    }
                }
                StatusCode::NOT_FOUND => {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "The parent parameter of the namespace provided does not exist",
                    ));
                }
                _ => {
                    return Err(deserialize_unexpected_catalog_error(
                        http_response,
                        context.client.disable_header_redaction(),
                    )
                    .await);
                }
            }
        }

        Ok(namespaces)
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::POST, context.config.namespaces_endpoint())
            .json(&CreateNamespaceRequest {
                namespace: namespace.clone(),
                properties,
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<NamespaceResponse>(http_response).await?;
                Ok(Namespace::from(response))
            }
            StatusCode::CONFLICT => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to create a namespace that already exists",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::GET, context.config.namespace_endpoint(namespace))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<NamespaceResponse>(http_response).await?;
                Ok(Namespace::from(response))
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to get a namespace that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn namespace_exists(&self, ns: &NamespaceIdent) -> Result<bool> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::HEAD, context.config.namespace_endpoint(ns))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating namespace not supported yet!",
        ))
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::DELETE, context.config.namespace_endpoint(namespace))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to drop a namespace that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let context = self.context().await?;
        let endpoint = context.config.tables_endpoint(namespace);
        let mut identifiers = Vec::new();
        let mut next_token = None;

        loop {
            let mut request = context.client.request(Method::GET, endpoint.clone());

            if let Some(token) = next_token {
                request = request.query(&[("pageToken", token)]);
            }

            let http_response = context.client.query_catalog(request.build()?).await?;

            match http_response.status() {
                StatusCode::OK => {
                    let response =
                        deserialize_catalog_response::<ListTablesResponse>(http_response).await?;

                    identifiers.extend(response.identifiers);

                    match response.next_page_token {
                        Some(token) => next_token = Some(token),
                        None => break,
                    }
                }
                StatusCode::NOT_FOUND => {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "Tried to list tables of a namespace that does not exist",
                    ));
                }
                _ => {
                    return Err(deserialize_unexpected_catalog_error(
                        http_response,
                        context.client.disable_header_redaction(),
                    )
                    .await);
                }
            }
        }

        Ok(identifiers)
    }

    /// Create a new table inside the namespace.
    ///
    /// In the resulting table, if there are any config properties that
    /// are present in both the response from the REST server and the
    /// config provided when creating this `RestCatalog` instance then
    /// the value provided locally to the `RestCatalog` will take precedence.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.create_table_with_stage(namespace, creation, false, false)
            .await
            .map(|response| response.into_parts().0)
    }

    /// Load table from the catalog.
    ///
    /// If there are any config properties that are present in both the response from the REST
    /// server and the config provided when creating this `RestCatalog` instance, then the value
    /// provided locally to the `RestCatalog` will take precedence.
    async fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::GET, context.config.table_endpoint(table_ident))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        let response = match http_response.status() {
            StatusCode::OK | StatusCode::NOT_MODIFIED => {
                deserialize_catalog_response::<LoadTableResult>(http_response).await?
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::TableNotFound,
                    "Tried to load a table that does not exist",
                ));
            }
            _ => {
                return Err(deserialize_unexpected_catalog_error(
                    http_response,
                    context.client.disable_header_redaction(),
                )
                .await);
            }
        };

        self.materialize_table_response(table_ident.clone(), response)
            .await
            .map(|response| response.into_parts().0)
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::DELETE, context.config.table_endpoint(table))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to drop a table that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::HEAD, context.config.table_endpoint(table))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::POST, context.config.rename_table_endpoint())
            .json(&RenameTableRequest {
                source: src.clone(),
                destination: dest.clone(),
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to rename a table that does not exist (is the namespace correct?)",
            )),
            StatusCode::CONFLICT => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to rename a table to a name that already exists",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    async fn register_table(
        &self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        let context = self.context().await?;

        let request = context
            .client
            .request(
                Method::POST,
                context
                    .config
                    .register_table_endpoint(table_ident.namespace()),
            )
            .json(&RegisterTableRequest {
                name: table_ident.name.clone(),
                metadata_location: metadata_location.clone(),
                overwrite: Some(false),
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        let response: LoadTableResult = match http_response.status() {
            StatusCode::OK => {
                deserialize_catalog_response::<LoadTableResult>(http_response).await?
            }
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::NamespaceNotFound,
                    "The namespace specified does not exist.",
                ));
            }
            StatusCode::CONFLICT => {
                return Err(Error::new(
                    ErrorKind::TableAlreadyExists,
                    "The given table already exists.",
                ));
            }
            _ => {
                return Err(deserialize_unexpected_catalog_error(
                    http_response,
                    context.client.disable_header_redaction(),
                )
                .await);
            }
        };

        let metadata_location = response.metadata_location.as_ref().ok_or(Error::new(
            ErrorKind::DataInvalid,
            "Metadata location missing in `register_table` response!",
        ))?;

        let file_io = self.load_file_io(Some(metadata_location), None).await?;

        Table::builder()
            .identifier(table_ident.clone())
            .file_io(file_io)
            .metadata(response.metadata)
            .metadata_location(metadata_location.clone())
            .build()
    }

    async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
        let context = self.context().await?;

        let request = context
            .client
            .request(
                Method::POST,
                context.config.table_endpoint(commit.identifier()),
            )
            .json(&CommitTableRequest {
                identifier: Some(commit.identifier().clone()),
                requirements: commit.take_requirements(),
                updates: commit.take_updates(),
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        let response: CommitTableResponse = match http_response.status() {
            StatusCode::OK => deserialize_catalog_response(http_response).await?,
            StatusCode::NOT_FOUND => {
                return Err(Error::new(
                    ErrorKind::TableNotFound,
                    "Tried to update a table that does not exist",
                ));
            }
            StatusCode::CONFLICT => {
                return Err(Error::new(
                    ErrorKind::CatalogCommitConflicts,
                    "CatalogCommitConflicts, one or more requirements failed. The client may retry.",
                )
                .with_retryable(true));
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "An unknown server-side problem occurred; the commit state is unknown.",
                ));
            }
            StatusCode::BAD_GATEWAY => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "A gateway or proxy received an invalid response from the upstream server; the commit state is unknown.",
                ));
            }
            StatusCode::GATEWAY_TIMEOUT => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "A server-side gateway timeout occurred; the commit state is unknown.",
                ));
            }
            _ => {
                return Err(deserialize_unexpected_catalog_error(
                    http_response,
                    context.client.disable_header_redaction(),
                )
                .await);
            }
        };

        let file_io = self
            .load_file_io(Some(&response.metadata_location), None)
            .await?;

        Table::builder()
            .identifier(commit.identifier().clone())
            .file_io(file_io)
            .metadata(response.metadata)
            .metadata_location(response.metadata_location)
            .build()
    }

    /// Create a new view inside the namespace.
    async fn create_view(
        &self,
        namespace: &NamespaceIdent,
        creation: ViewCreation,
    ) -> Result<ViewMetadata> {
        let context = self.context().await?;

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("system clock before epoch: {e}"),
                )
            })?
            .as_millis() as i64;
        let view_version = ViewVersion::builder()
            .with_version_id(1)
            .with_schema_id(creation.schema.schema_id())
            .with_timestamp_ms(timestamp_ms)
            .with_summary(creation.summary)
            .with_representations(creation.representations)
            .with_default_catalog(creation.default_catalog)
            .with_default_namespace(creation.default_namespace)
            .build();

        let request = context
            .client
            .request(Method::POST, context.config.views_endpoint(namespace))
            .json(&CreateViewRequest {
                name: creation.name,
                location: creation.location,
                schema: creation.schema,
                view_version,
                properties: creation.properties,
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<LoadViewResult>(http_response).await?;
                Ok(response.metadata)
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::NamespaceNotFound,
                "Tried to create a view under a namespace that does not exist",
            )),
            StatusCode::CONFLICT => {
                Err(Error::new(ErrorKind::Unexpected, "The view already exists"))
            }
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Load a view's metadata from the catalog.
    async fn load_view(&self, view: &TableIdent) -> Result<ViewMetadata> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::GET, context.config.view_endpoint(view))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<LoadViewResult>(http_response).await?;
                Ok(response.metadata)
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to load a view that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Commit updates to an existing view.
    async fn update_view(&self, mut commit: ViewCommit) -> Result<ViewMetadata> {
        let context = self.context().await?;

        let request = context
            .client
            .request(
                Method::POST,
                context.config.view_endpoint(commit.identifier()),
            )
            .json(&CommitViewRequest {
                identifier: Some(commit.identifier().clone()),
                requirements: commit.take_requirements(),
                updates: commit.take_updates(),
            })
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK => {
                let response =
                    deserialize_catalog_response::<LoadViewResult>(http_response).await?;
                Ok(response.metadata)
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to update a view that does not exist",
            )),
            StatusCode::CONFLICT => Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "View commit failed due to a conflicting update",
            )
            .with_retryable(true)),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Drop a view from the catalog.
    async fn drop_view(&self, view: &TableIdent) -> Result<()> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::DELETE, context.config.view_endpoint(view))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::Unexpected,
                "Tried to drop a view that does not exist",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// Check if a view exists in the catalog.
    async fn view_exists(&self, view: &TableIdent) -> Result<bool> {
        let context = self.context().await?;

        let request = context
            .client
            .request(Method::HEAD, context.config.view_endpoint(view))
            .build()?;

        let http_response = context.client.query_catalog(request).await?;

        match http_response.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(deserialize_unexpected_catalog_error(
                http_response,
                context.client.disable_header_redaction(),
            )
            .await),
        }
    }

    /// List views in the namespace.
    async fn list_views(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let context = self.context().await?;
        let endpoint = context.config.views_endpoint(namespace);
        let mut identifiers = Vec::new();
        let mut next_token = None;

        loop {
            let mut request = context.client.request(Method::GET, endpoint.clone());

            if let Some(token) = next_token {
                request = request.query(&[("pageToken", token)]);
            }

            let http_response = context.client.query_catalog(request.build()?).await?;

            match http_response.status() {
                StatusCode::OK => {
                    let response =
                        deserialize_catalog_response::<ListViewsResponse>(http_response).await?;

                    identifiers.extend(response.identifiers);

                    match response.next_page_token {
                        Some(token) => next_token = Some(token),
                        None => break,
                    }
                }
                StatusCode::NOT_FOUND => {
                    return Err(Error::new(
                        ErrorKind::NamespaceNotFound,
                        "Tried to list views under a namespace that does not exist",
                    ));
                }
                _ => {
                    return Err(deserialize_unexpected_catalog_error(
                        http_response,
                        context.client.disable_header_redaction(),
                    )
                    .await);
                }
            }
        }

        Ok(identifiers)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use iceberg::TableRequirement;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::spec::{
        FormatVersion, NestedField, NullOrder, Operation, PrimitiveType, Schema, Snapshot,
        SnapshotLog, SnapshotReference, SnapshotRetention, SortDirection, SortField, SortOrder,
        Summary, TableMetadata, TableMetadataBuilder, Transform, Type, UnboundPartitionField,
        UnboundPartitionSpec,
    };
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use mockito::{Matcher, Mock, Server, ServerGuard};
    use serde_json::json;
    use uuid::uuid;

    use super::*;

    #[tokio::test]
    async fn test_update_config() {
        let mut server = Server::new_async().await;

        let config_mock = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(
                r#"{
                "overrides": {
                    "warehouse": "s3://iceberg-catalog"
                },
                "defaults": {}
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        assert_eq!(
            catalog
                .context()
                .await
                .unwrap()
                .config
                .props
                .get("warehouse"),
            Some(&"s3://iceberg-catalog".to_string())
        );

        config_mock.assert_async().await;
    }

    async fn create_config_mock(server: &mut ServerGuard) -> Mock {
        server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(
                r#"{
                "overrides": {
                    "warehouse": "s3://iceberg-catalog"
                },
                "defaults": {}
            }"#,
            )
            .create_async()
            .await
    }

    async fn create_oauth_mock(server: &mut ServerGuard) -> Mock {
        create_oauth_mock_with_path(server, "/v1/oauth/tokens", "ey000000000000", 200).await
    }

    async fn create_oauth_mock_with_path(
        server: &mut ServerGuard,
        path: &str,
        token: &str,
        status: usize,
    ) -> Mock {
        let body = format!(
            r#"{{
                "access_token": "{token}",
                "token_type": "Bearer",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "expires_in": 86400
            }}"#
        );
        server
            .mock("POST", path)
            .with_status(status)
            .with_body(body)
            .expect(1)
            .create_async()
            .await
    }

    #[tokio::test]
    async fn test_oauth() {
        let mut server = Server::new_async().await;
        let oauth_mock = create_oauth_mock(&mut server).await;
        let config_mock = create_config_mock(&mut server).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_oauth_with_optional_param() {
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());
        props.insert("scope".to_string(), "custom_scope".to_string());
        props.insert("audience".to_string(), "custom_audience".to_string());
        props.insert("resource".to_string(), "custom_resource".to_string());

        let mut server = Server::new_async().await;
        let oauth_mock = server
            .mock("POST", "/v1/oauth/tokens")
            .match_body(mockito::Matcher::Regex("scope=custom_scope".to_string()))
            .match_body(mockito::Matcher::Regex(
                "audience=custom_audience".to_string(),
            ))
            .match_body(mockito::Matcher::Regex(
                "resource=custom_resource".to_string(),
            ))
            .with_status(200)
            .with_body(
                r#"{
                "access_token": "ey000000000000",
                "token_type": "Bearer",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "expires_in": 86400
                }"#,
            )
            .expect(1)
            .create_async()
            .await;

        let config_mock = create_config_mock(&mut server).await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let token = catalog.context().await.unwrap().client.token().await;

        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_invalidate_token() {
        let mut server = Server::new_async().await;
        let oauth_mock = create_oauth_mock(&mut server).await;
        let config_mock = create_config_mock(&mut server).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));

        let oauth_mock =
            create_oauth_mock_with_path(&mut server, "/v1/oauth/tokens", "ey000000000001", 200)
                .await;
        catalog.invalidate_token().await.unwrap();
        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000001".to_string()));
    }

    #[tokio::test]
    async fn test_invalidate_token_failing_request() {
        let mut server = Server::new_async().await;
        let oauth_mock = create_oauth_mock(&mut server).await;
        let config_mock = create_config_mock(&mut server).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));

        let oauth_mock =
            create_oauth_mock_with_path(&mut server, "/v1/oauth/tokens", "ey000000000001", 500)
                .await;
        catalog.invalidate_token().await.unwrap();
        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        assert_eq!(token, None);
    }

    #[tokio::test]
    async fn test_regenerate_token() {
        let mut server = Server::new_async().await;
        let oauth_mock = create_oauth_mock(&mut server).await;
        let config_mock = create_config_mock(&mut server).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));

        let oauth_mock =
            create_oauth_mock_with_path(&mut server, "/v1/oauth/tokens", "ey000000000001", 200)
                .await;
        catalog.regenerate_token().await.unwrap();
        oauth_mock.assert_async().await;
        let token = catalog.context().await.unwrap().client.token().await;
        assert_eq!(token, Some("ey000000000001".to_string()));
    }

    #[tokio::test]
    async fn test_regenerate_token_failing_request() {
        let mut server = Server::new_async().await;
        let oauth_mock = create_oauth_mock(&mut server).await;
        let config_mock = create_config_mock(&mut server).await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let token = catalog.context().await.unwrap().client.token().await;
        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));

        let oauth_mock =
            create_oauth_mock_with_path(&mut server, "/v1/oauth/tokens", "ey000000000001", 500)
                .await;
        let invalidate_result = catalog.regenerate_token().await;
        assert!(invalidate_result.is_err());
        oauth_mock.assert_async().await;
        let token = catalog.context().await.unwrap().client.token().await;

        // original token is left intact
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_http_headers() {
        let server = Server::new_async().await;
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());

        let config = RestCatalogConfig::builder()
            .uri(server.url())
            .props(props)
            .build();
        let headers: HeaderMap = config.extra_headers().unwrap();

        let expected_headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                HeaderName::from_static("x-client-version"),
                HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
            ),
            (
                header::USER_AGENT,
                HeaderValue::from_str(&format!("iceberg-rs/{CARGO_PKG_VERSION}")).unwrap(),
            ),
        ]);
        assert_eq!(headers, expected_headers);
    }

    #[tokio::test]
    async fn test_http_headers_with_custom_headers() {
        let server = Server::new_async().await;
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());
        props.insert(
            "header.content-type".to_string(),
            "application/yaml".to_string(),
        );
        props.insert(
            "header.customized-header".to_string(),
            "some/value".to_string(),
        );

        let config = RestCatalogConfig::builder()
            .uri(server.url())
            .props(props)
            .build();
        let headers: HeaderMap = config.extra_headers().unwrap();

        let expected_headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/yaml"),
            ),
            (
                HeaderName::from_static("x-client-version"),
                HeaderValue::from_static(ICEBERG_REST_SPEC_VERSION),
            ),
            (
                header::USER_AGENT,
                HeaderValue::from_str(&format!("iceberg-rs/{CARGO_PKG_VERSION}")).unwrap(),
            ),
            (
                HeaderName::from_static("customized-header"),
                HeaderValue::from_static("some/value"),
            ),
        ]);
        assert_eq!(headers, expected_headers);
    }

    #[tokio::test]
    async fn test_oauth_with_oauth2_server_uri() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;

        let mut auth_server = Server::new_async().await;
        let auth_server_path = "/some/path";
        let oauth_mock =
            create_oauth_mock_with_path(&mut auth_server, auth_server_path, "ey000000000000", 200)
                .await;

        let mut props = HashMap::new();
        props.insert("credential".to_string(), "client1:secret1".to_string());
        props.insert(
            "oauth2-server-uri".to_string(),
            format!("{}{}", auth_server.url(), auth_server_path).to_string(),
        );

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(props)
                .build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let token = catalog.context().await.unwrap().client.token().await;

        oauth_mock.assert_async().await;
        config_mock.assert_async().await;
        assert_eq!(token, Some("ey000000000000".to_string()));
    }

    #[tokio::test]
    async fn test_config_override() {
        let mut server = Server::new_async().await;
        let mut redirect_server = Server::new_async().await;
        let new_uri = redirect_server.url();

        let config_mock = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(
                json!(
                    {
                        "overrides": {
                            "uri": new_uri,
                            "warehouse": "s3://iceberg-catalog",
                            "prefix": "ice/warehouses/my"
                        },
                        "defaults": {},
                    }
                )
                .to_string(),
            )
            .create_async()
            .await;

        let list_ns_mock = redirect_server
            .mock("GET", "/v1/ice/warehouses/my/namespaces")
            .with_body(
                r#"{
                    "namespaces": []
                }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let _namespaces = catalog.list_namespaces(None).await.unwrap();

        config_mock.assert_async().await;
        list_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let list_ns_mock = server
            .mock("GET", "/v1/namespaces")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns1", "ns11"],
                    ["ns2"]
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let namespaces = catalog.list_namespaces(None).await.unwrap();

        let expected_ns = vec![
            NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns2".to_string()]).unwrap(),
        ];

        assert_eq!(expected_ns, namespaces);

        config_mock.assert_async().await;
        list_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_namespace_with_pagination() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let list_ns_mock_page1 = server
            .mock("GET", "/v1/namespaces")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns1", "ns11"],
                    ["ns2"]
                ],
                "next-page-token": "token123"
            }"#,
            )
            .create_async()
            .await;

        let list_ns_mock_page2 = server
            .mock("GET", "/v1/namespaces?pageToken=token123")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns3"],
                    ["ns4", "ns41"]
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let namespaces = catalog.list_namespaces(None).await.unwrap();

        let expected_ns = vec![
            NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns2".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns3".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns4".to_string(), "ns41".to_string()]).unwrap(),
        ];

        assert_eq!(expected_ns, namespaces);

        config_mock.assert_async().await;
        list_ns_mock_page1.assert_async().await;
        list_ns_mock_page2.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_namespace_with_multiple_pages() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        // Page 1
        let list_ns_mock_page1 = server
            .mock("GET", "/v1/namespaces")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns1", "ns11"],
                    ["ns2"]
                ],
                "next-page-token": "page2"
            }"#,
            )
            .create_async()
            .await;

        // Page 2
        let list_ns_mock_page2 = server
            .mock("GET", "/v1/namespaces?pageToken=page2")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns3"],
                    ["ns4", "ns41"]
                ],
                "next-page-token": "page3"
            }"#,
            )
            .create_async()
            .await;

        // Page 3
        let list_ns_mock_page3 = server
            .mock("GET", "/v1/namespaces?pageToken=page3")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns5", "ns51", "ns511"]
                ],
                "next-page-token": "page4"
            }"#,
            )
            .create_async()
            .await;

        // Page 4
        let list_ns_mock_page4 = server
            .mock("GET", "/v1/namespaces?pageToken=page4")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns6"],
                    ["ns7"]
                ],
                "next-page-token": "page5"
            }"#,
            )
            .create_async()
            .await;

        // Page 5 (final page)
        let list_ns_mock_page5 = server
            .mock("GET", "/v1/namespaces?pageToken=page5")
            .with_body(
                r#"{
                "namespaces": [
                    ["ns8", "ns81"]
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let namespaces = catalog.list_namespaces(None).await.unwrap();

        let expected_ns = vec![
            NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns2".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns3".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns4".to_string(), "ns41".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec![
                "ns5".to_string(),
                "ns51".to_string(),
                "ns511".to_string(),
            ])
            .unwrap(),
            NamespaceIdent::from_vec(vec!["ns6".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns7".to_string()]).unwrap(),
            NamespaceIdent::from_vec(vec!["ns8".to_string(), "ns81".to_string()]).unwrap(),
        ];

        assert_eq!(expected_ns, namespaces);

        // Verify all page requests were made
        config_mock.assert_async().await;
        list_ns_mock_page1.assert_async().await;
        list_ns_mock_page2.assert_async().await;
        list_ns_mock_page3.assert_async().await;
        list_ns_mock_page4.assert_async().await;
        list_ns_mock_page5.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let create_ns_mock = server
            .mock("POST", "/v1/namespaces")
            .with_body(
                r#"{
                "namespace": [ "ns1", "ns11"],
                "properties" : {
                    "key1": "value1"
                }
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let namespaces = catalog
            .create_namespace(
                &NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
                HashMap::from([("key1".to_string(), "value1".to_string())]),
            )
            .await
            .unwrap();

        let expected_ns = Namespace::with_properties(
            NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap(),
            HashMap::from([("key1".to_string(), "value1".to_string())]),
        );

        assert_eq!(expected_ns, namespaces);

        config_mock.assert_async().await;
        create_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let get_ns_mock = server
            .mock("GET", "/v1/namespaces/ns1")
            .with_body(
                r#"{
                "namespace": [ "ns1"],
                "properties" : {
                    "key1": "value1"
                }
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let namespaces = catalog
            .get_namespace(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        let expected_ns = Namespace::with_properties(
            NamespaceIdent::new("ns1".to_string()),
            HashMap::from([("key1".to_string(), "value1".to_string())]),
        );

        assert_eq!(expected_ns, namespaces);

        config_mock.assert_async().await;
        get_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn check_namespace_exists() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let get_ns_mock = server
            .mock("HEAD", "/v1/namespaces/ns1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        assert!(
            catalog
                .namespace_exists(&NamespaceIdent::new("ns1".to_string()))
                .await
                .unwrap()
        );

        config_mock.assert_async().await;
        get_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_drop_namespace() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let drop_ns_mock = server
            .mock("DELETE", "/v1/namespaces/ns1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        catalog
            .drop_namespace(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        config_mock.assert_async().await;
        drop_ns_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_tables() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let list_tables_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table1"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table2"
                    }
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let tables = catalog
            .list_tables(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        let expected_tables = vec![
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table1".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table2".to_string()),
        ];

        assert_eq!(tables, expected_tables);

        config_mock.assert_async().await;
        list_tables_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_tables_page_preserves_the_opaque_continuation() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;
        let first_page_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables?pageSize=1")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [{"namespace": ["ns1"], "name": "table1"}],
                "next-page-token": "opaque-token"
            }"#,
            )
            .create_async()
            .await;
        let second_page_mock = server
            .mock(
                "GET",
                "/v1/namespaces/ns1/tables?pageSize=1&pageToken=opaque-token",
            )
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [{"namespace": ["ns1"], "name": "table2"}]
            }"#,
            )
            .create_async()
            .await;
        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );
        let namespace = NamespaceIdent::new("ns1".to_string());

        let first = catalog.list_tables_page(&namespace, None, 1).await.unwrap();
        assert_eq!(first.identifiers.len(), 1);
        assert_eq!(first.identifiers[0].name, "table1");
        assert_eq!(first.next_page_token.as_deref(), Some("opaque-token"));

        let second = catalog
            .list_tables_page(&namespace, first.next_page_token.as_deref(), 1)
            .await
            .unwrap();
        assert_eq!(second.identifiers.len(), 1);
        assert_eq!(second.identifiers[0].name, "table2");
        assert_eq!(second.next_page_token, None);

        config_mock.assert_async().await;
        first_page_mock.assert_async().await;
        second_page_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_namespaces_page_preserves_token_and_size_without_accumulating() {
        let mut server = Server::new_async().await;
        let config = create_config_mock(&mut server).await;
        let first_mock = server
            .mock("GET", "/v1/namespaces?pageSize=1")
            .with_status(200)
            .with_body(r#"{"namespaces":[["ns1"]],"next-page-token":"opaque-token"}"#)
            .create_async()
            .await;
        let second_mock = server
            .mock("GET", "/v1/namespaces?pageSize=1&pageToken=opaque-token")
            .with_status(200)
            .with_body(r#"{"namespaces":[["ns2"]]}"#)
            .create_async()
            .await;
        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let first = catalog.list_namespaces_page(None, None, 1).await.unwrap();
        assert_eq!(first.namespaces.len(), 1);
        assert_eq!(first.next_page_token.as_deref(), Some("opaque-token"));
        // The second mock has not been called: the SDK owns no page loop.
        assert!(!second_mock.matched_async().await);
        let second = catalog
            .list_namespaces_page(None, first.next_page_token.as_deref(), 1)
            .await
            .unwrap();
        assert_eq!(second.namespaces.len(), 1);
        assert_eq!(second.next_page_token, None);
        config.assert_async().await;
        first_mock.assert_async().await;
        second_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_namespaces_page_preserves_parent_filter() {
        let mut server = Server::new_async().await;
        let config = create_config_mock(&mut server).await;
        let page = server
            .mock(
                "GET",
                "/v1/namespaces?pageSize=1&parent=ns1&pageToken=opaque-token",
            )
            .with_status(200)
            .with_body(r#"{"namespaces":[["ns1","child"]],"next-page-token":"next"}"#)
            .create_async()
            .await;
        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );
        let parent = NamespaceIdent::new("ns1".to_string());
        let response = catalog
            .list_namespaces_page(Some(&parent), Some("opaque-token"), 1)
            .await
            .unwrap();
        assert_eq!(response.namespaces.len(), 1);
        assert_eq!(response.namespaces[0].as_ref(), &["ns1", "child"]);
        assert_eq!(response.next_page_token.as_deref(), Some("next"));
        config.assert_async().await;
        page.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_views_page_preserves_token_and_size_without_accumulating() {
        let mut server = Server::new_async().await;
        let config = create_config_mock(&mut server).await;
        let first_mock = server.mock("GET", "/v1/namespaces/ns1/views?pageSize=1")
            .with_status(200).with_body(r#"{"identifiers":[{"namespace":["ns1"],"name":"v1"}],"next-page-token":"opaque-token"}"#).create_async().await;
        let second_mock = server
            .mock(
                "GET",
                "/v1/namespaces/ns1/views?pageSize=1&pageToken=opaque-token",
            )
            .with_status(200)
            .with_body(r#"{"identifiers":[{"namespace":["ns1"],"name":"v2"}]}"#)
            .create_async()
            .await;
        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );
        let namespace = NamespaceIdent::new("ns1".to_string());
        let first = catalog.list_views_page(&namespace, None, 1).await.unwrap();
        assert_eq!(first.identifiers.len(), 1);
        assert_eq!(first.next_page_token.as_deref(), Some("opaque-token"));
        // The second mock has not been called: the SDK owns no page loop.
        assert!(!second_mock.matched_async().await);
        let second = catalog
            .list_views_page(&namespace, first.next_page_token.as_deref(), 1)
            .await
            .unwrap();
        assert_eq!(second.identifiers.len(), 1);
        assert_eq!(second.next_page_token, None);
        config.assert_async().await;
        first_mock.assert_async().await;
        second_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_tables_with_pagination() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let list_tables_mock_page1 = server
            .mock("GET", "/v1/namespaces/ns1/tables")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table1"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table2"
                    }
                ],
                "next-page-token": "token456"
            }"#,
            )
            .create_async()
            .await;

        let list_tables_mock_page2 = server
            .mock("GET", "/v1/namespaces/ns1/tables?pageToken=token456")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table3"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table4"
                    }
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let tables = catalog
            .list_tables(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        let expected_tables = vec![
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table1".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table2".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table3".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table4".to_string()),
        ];

        assert_eq!(tables, expected_tables);

        config_mock.assert_async().await;
        list_tables_mock_page1.assert_async().await;
        list_tables_mock_page2.assert_async().await;
    }

    #[tokio::test]
    async fn test_list_tables_with_multiple_pages() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        // Page 1
        let list_tables_mock_page1 = server
            .mock("GET", "/v1/namespaces/ns1/tables")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table1"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table2"
                    }
                ],
                "next-page-token": "page2"
            }"#,
            )
            .create_async()
            .await;

        // Page 2
        let list_tables_mock_page2 = server
            .mock("GET", "/v1/namespaces/ns1/tables?pageToken=page2")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table3"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table4"
                    }
                ],
                "next-page-token": "page3"
            }"#,
            )
            .create_async()
            .await;

        // Page 3
        let list_tables_mock_page3 = server
            .mock("GET", "/v1/namespaces/ns1/tables?pageToken=page3")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table5"
                    }
                ],
                "next-page-token": "page4"
            }"#,
            )
            .create_async()
            .await;

        // Page 4
        let list_tables_mock_page4 = server
            .mock("GET", "/v1/namespaces/ns1/tables?pageToken=page4")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table6"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table7"
                    }
                ],
                "next-page-token": "page5"
            }"#,
            )
            .create_async()
            .await;

        // Page 5 (final page)
        let list_tables_mock_page5 = server
            .mock("GET", "/v1/namespaces/ns1/tables?pageToken=page5")
            .with_status(200)
            .with_body(
                r#"{
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table8"
                    }
                ]
            }"#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let tables = catalog
            .list_tables(&NamespaceIdent::new("ns1".to_string()))
            .await
            .unwrap();

        let expected_tables = vec![
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table1".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table2".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table3".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table4".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table5".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table6".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table7".to_string()),
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table8".to_string()),
        ];

        assert_eq!(tables, expected_tables);

        // Verify all page requests were made
        config_mock.assert_async().await;
        list_tables_mock_page1.assert_async().await;
        list_tables_mock_page2.assert_async().await;
        list_tables_mock_page3.assert_async().await;
        list_tables_mock_page4.assert_async().await;
        list_tables_mock_page5.assert_async().await;
    }

    #[tokio::test]
    async fn test_drop_tables() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let delete_table_mock = server
            .mock("DELETE", "/v1/namespaces/ns1/tables/table1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        catalog
            .drop_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "table1".to_string(),
            ))
            .await
            .unwrap();

        config_mock.assert_async().await;
        delete_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_check_table_exists() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let check_table_exists_mock = server
            .mock("HEAD", "/v1/namespaces/ns1/tables/table1")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        assert!(
            catalog
                .table_exists(&TableIdent::new(
                    NamespaceIdent::new("ns1".to_string()),
                    "table1".to_string(),
                ))
                .await
                .unwrap()
        );

        config_mock.assert_async().await;
        check_table_exists_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_rename_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let rename_table_mock = server
            .mock("POST", "/v1/tables/rename")
            .with_status(204)
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        catalog
            .rename_table(
                &TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table1".to_string()),
                &TableIdent::new(NamespaceIdent::new("ns1".to_string()), "table2".to_string()),
            )
            .await
            .unwrap();

        config_mock.assert_async().await;
        rename_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_load_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let rename_table_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "load_table_response.json"
            ))
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "test1".to_string(),
            ))
            .await
            .unwrap();

        assert_eq!(
            &TableIdent::from_strs(vec!["ns1", "test1"]).unwrap(),
            table.identifier()
        );
        assert_eq!(
            "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
            table.metadata_location().unwrap()
        );
        assert_eq!(FormatVersion::V1, table.metadata().format_version());
        assert_eq!("s3://warehouse/database/table", table.metadata().location());
        assert_eq!(
            uuid!("b55d9dda-6561-423a-8bfc-787980ce421f"),
            table.metadata().uuid()
        );
        assert_eq!(
            Utc.timestamp_millis_opt(1646787054459).unwrap(),
            table.metadata().last_updated_timestamp().unwrap()
        );
        assert_eq!(
            vec![&Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()
                    .unwrap()
            )],
            table.metadata().schemas_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            &HashMap::from([
                ("owner".to_string(), "bryan".to_string()),
                (
                    "write.metadata.compression-codec".to_string(),
                    "gzip".to_string()
                )
            ]),
            table.metadata().properties()
        );
        assert_eq!(vec![&Arc::new(Snapshot::builder()
            .with_snapshot_id(3497810964824022504)
            .with_timestamp_ms(1646787054459)
            .with_manifest_list("s3://warehouse/database/table/metadata/snap-3497810964824022504-1-c4f68204-666b-4e50-a9df-b10c34bf6b82.avro")
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter([
                    ("spark.app.id", "local-1646787004168"),
                    ("added-data-files", "1"),
                    ("added-records", "1"),
                    ("added-files-size", "697"),
                    ("changed-partition-count", "1"),
                    ("total-records", "1"),
                    ("total-files-size", "697"),
                    ("total-data-files", "1"),
                    ("total-delete-files", "0"),
                    ("total-position-deletes", "0"),
                    ("total-equality-deletes", "0")
                ].iter().map(|p| (p.0.to_string(), p.1.to_string()))),
            }).build()
        )], table.metadata().snapshots().collect::<Vec<_>>());
        assert_eq!(
            &[SnapshotLog {
                timestamp_ms: 1646787054459,
                snapshot_id: 3497810964824022504,
            }],
            table.metadata().history()
        );
        assert_eq!(
            vec![&Arc::new(SortOrder {
                order_id: 0,
                fields: vec![],
            })],
            table.metadata().sort_orders_iter().collect::<Vec<_>>()
        );

        config_mock.assert_async().await;
        rename_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn access_delegation_is_opt_in_and_preserves_redacted_facts() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;
        let response = std::fs::read_to_string(format!(
            "{}/testdata/load_table_response.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
        .replacen(
            "\n}",
            ",\n  \"storage-credentials\": [{\n    \"prefix\": \"s3://warehouse/database/\",\n    \"config\": {\"s3.access-key-id\": \"access-canary\", \"s3.secret-access-key\": \"secret-canary\"}\n  }]\n}",
            1,
        );
        let ordinary = server
            .mock("GET", "/v1/namespaces/ns1/tables/ordinary")
            .match_header(REST_CATALOG_HEADER_ACCESS_DELEGATION, Matcher::Missing)
            .with_status(200)
            .with_body(response.clone())
            .create_async()
            .await;
        let delegated = server
            .mock("GET", "/v1/namespaces/ns1/tables/delegated")
            .match_header(
                REST_CATALOG_HEADER_ACCESS_DELEGATION,
                REST_CATALOG_ACCESS_DELEGATION_VENDED_CREDENTIALS,
            )
            .with_status(200)
            .with_body(response)
            .create_async()
            .await;
        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let ordinary_table = catalog
            .load_table(&TableIdent::from_strs(["ns1", "ordinary"]).unwrap())
            .await
            .unwrap();
        assert_eq!(ordinary_table.identifier().name, "ordinary");
        let delegated_response = catalog
            .load_table_deferred_with_access_delegation(
                &TableIdent::from_strs(["ns1", "delegated"]).unwrap(),
            )
            .await
            .unwrap();
        let (materialization, delegation) = delegated_response.into_parts();
        assert!(delegation.is_present());
        assert_eq!(delegation.len(), 1);
        let credential = delegation.credentials().next().unwrap();
        assert_eq!(credential.prefix(), "s3://warehouse/database/");
        assert_eq!(
            credential.config_value("s3.access-key-id"),
            Some("access-canary")
        );
        assert!(format!("{credential:?}").contains("s3.access-key-id"));
        assert!(!format!("{credential:?}").contains("secret-canary"));
        assert!(!format!("{materialization:?}").contains("secret-canary"));
        let request_scoped_table = materialization
            .materialize_with_file_io(FileIO::new_with_memory())
            .expect("materialize deferred response with caller-owned FileIO");
        assert_eq!(request_scoped_table.identifier().name, "delegated");

        config_mock.assert_async().await;
        ordinary.assert_async().await;
        delegated.assert_async().await;
    }

    #[tokio::test]
    async fn credential_refresh_uses_catalog_authentication_and_redacts_values() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;
        let credentials = server
            .mock("GET", "/credentials")
            .match_header("authorization", "Bearer refresh-token")
            .with_status(200)
            .with_body(
                r#"{
                    "storage-credentials": [{
                        "prefix": "s3://warehouse/data/",
                        "config": {
                            "s3.access-key-id": "access-canary",
                            "s3.secret-access-key": "secret-canary"
                        }
                    }]
                }"#,
            )
            .create_async()
            .await;
        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri(server.url())
                .props(HashMap::from([(
                    "token".to_string(),
                    "refresh-token".to_string(),
                )]))
                .build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let delegation = catalog
            .load_credentials_with_access_delegation(&format!("{}/credentials", server.url()))
            .await
            .expect("refresh response");
        assert!(delegation.is_present());
        assert_eq!(delegation.len(), 1);
        let credential = delegation.credentials().next().expect("credential");
        assert_eq!(credential.prefix(), "s3://warehouse/data/");
        assert_eq!(
            credential.config_value("s3.access-key-id"),
            Some("access-canary")
        );
        assert!(!format!("{credential:?}").contains("secret-canary"));
        assert!(!format!("{delegation:?}").contains("secret-canary"));

        config_mock.assert_async().await;
        credentials.assert_async().await;
    }

    #[tokio::test]
    async fn test_load_table_404() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let rename_table_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .with_status(404)
            .with_body(r#"
{
    "error": {
        "message": "Table does not exist: ns1.test1 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
        "type": "NoSuchNamespaceErrorException",
        "code": 404
    }
}
            "#)
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("ns1".to_string()),
                "test1".to_string(),
            ))
            .await;

        assert!(table.is_err());
        assert!(table.err().unwrap().message().contains("does not exist"));

        config_mock.assert_async().await;
        rename_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let create_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "create_table_response.json"
            ))
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let table_creation = TableCreation::builder()
            .name("test1".to_string())
            .schema(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(1)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap(),
            )
            .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
            .partition_spec(
                UnboundPartitionSpec::builder()
                    .add_partition_fields(vec![
                        UnboundPartitionField::builder()
                            .source_id(1)
                            .transform(Transform::Truncate(3))
                            .name("id".to_string())
                            .build(),
                    ])
                    .unwrap()
                    .build(),
            )
            .sort_order(
                SortOrder::builder()
                    .with_sort_field(
                        SortField::builder()
                            .source_id(2)
                            .transform(Transform::Identity)
                            .direction(SortDirection::Ascending)
                            .null_order(NullOrder::First)
                            .build(),
                    )
                    .build_unbound()
                    .unwrap(),
            )
            .build();

        let table = catalog
            .create_table(&NamespaceIdent::from_strs(["ns1"]).unwrap(), table_creation)
            .await
            .unwrap();

        assert_eq!(
            &TableIdent::from_strs(vec!["ns1", "test1"]).unwrap(),
            table.identifier()
        );
        assert_eq!(
            "s3://warehouse/database/table/metadata.json",
            table.metadata_location().unwrap()
        );
        assert_eq!(FormatVersion::V1, table.metadata().format_version());
        assert_eq!("s3://warehouse/database/table", table.metadata().location());
        assert_eq!(
            uuid!("bf289591-dcc0-4234-ad4f-5c3eed811a29"),
            table.metadata().uuid()
        );
        assert_eq!(
            1657810967051,
            table
                .metadata()
                .last_updated_timestamp()
                .unwrap()
                .timestamp_millis()
        );
        assert_eq!(
            vec![&Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(0)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap()
            )],
            table.metadata().schemas_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            &HashMap::from([
                (
                    "write.delete.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
                (
                    "write.metadata.compression-codec".to_string(),
                    "gzip".to_string()
                ),
                (
                    "write.summary.partition-limit".to_string(),
                    "100".to_string()
                ),
                (
                    "write.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
            ]),
            table.metadata().properties()
        );
        assert!(table.metadata().current_snapshot().is_none());
        assert!(table.metadata().history().is_empty());
        assert_eq!(
            vec![&Arc::new(SortOrder {
                order_id: 0,
                fields: vec![],
            })],
            table.metadata().sort_orders_iter().collect::<Vec<_>>()
        );

        config_mock.assert_async().await;
        create_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_stage_create_and_commit_authoritative_metadata_with_snapshot_updates() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;

        let mut staged_response: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/testdata/create_table_response.json"
        )))
        .unwrap();
        staged_response["metadata-location"] = serde_json::Value::Null;

        let stage_create_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables")
            .match_body(mockito::Matcher::PartialJson(json!({
                "name": "test1",
                "stage-create": true,
                "properties": {"owner": "request-owner"}
            })))
            .with_status(200)
            .with_body(staged_response.to_string())
            .expect(1)
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );
        let request_schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "request_field", Type::Primitive(PrimitiveType::String))
                    .into(),
            ])
            .with_schema_id(77)
            .build()
            .unwrap();
        let staged = catalog
            .stage_create_table(
                &NamespaceIdent::from_strs(["ns1"]).unwrap(),
                TableCreation::builder()
                    .name("test1".to_string())
                    .schema(request_schema)
                    .properties(HashMap::from([(
                        "owner".to_string(),
                        "request-owner".to_string(),
                    )]))
                    .build(),
            )
            .await
            .unwrap();

        assert_eq!(None, staged.table().metadata_location());
        assert_eq!(
            uuid!("bf289591-dcc0-4234-ad4f-5c3eed811a29"),
            staged.table().metadata().uuid()
        );
        assert_eq!(
            "s3://warehouse/database/table",
            staged.table().metadata().location()
        );
        assert_eq!(0, staged.table().metadata().current_schema_id());
        assert_eq!(0, staged.table().metadata().default_partition_spec_id());
        assert_eq!(0, staged.table().metadata().default_sort_order_id());
        assert_eq!(
            Some(&"zstd".to_string()),
            staged
                .table()
                .metadata()
                .properties()
                .get("write.parquet.compression-codec")
        );
        assert!(!staged.table().metadata().properties().contains_key("owner"));

        let initialization_json = serde_json::to_value(staged.initialization_updates()).unwrap();
        let initialization_json = initialization_json.as_array().unwrap();
        assert_eq!(9, initialization_json.len());
        assert_eq!(
            json!({
                "action": "assign-uuid",
                "uuid": "bf289591-dcc0-4234-ad4f-5c3eed811a29"
            }),
            initialization_json[0]
        );
        assert_eq!(
            json!({
                "action": "set-location",
                "location": "s3://warehouse/database/table"
            }),
            initialization_json[1]
        );
        assert_eq!(0, initialization_json[2]["schema"]["schema-id"]);
        assert_eq!(3, initialization_json[2]["last-column-id"]);
        assert_eq!(0, initialization_json[3]["schema-id"]);
        assert_eq!(0, initialization_json[4]["spec"]["spec-id"]);
        assert_eq!(0, initialization_json[5]["spec-id"]);
        assert_eq!(0, initialization_json[6]["sort-order"]["order-id"]);
        assert_eq!(0, initialization_json[7]["sort-order-id"]);
        assert_eq!(
            "zstd",
            initialization_json[8]["updates"]["write.parquet.compression-codec"]
        );

        let ident = staged.table().identifier().clone();
        let (_, mut updates) = staged.into_parts();
        let snapshot_id = 3055729675574597000;
        updates.push(TableUpdate::AddSnapshot {
            snapshot: Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_timestamp_ms(1657810968051)
                .with_sequence_number(0)
                .with_manifest_list("s3://warehouse/database/table/metadata/snap-proof.avro")
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build(),
        });
        updates.push(TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        });

        let expected_request = CommitTableRequest {
            identifier: Some(ident.clone()),
            requirements: vec![TableRequirement::NotExist],
            updates: updates.clone(),
        };
        let commit_response = json!({
            "metadata-location": "s3://warehouse/database/table/metadata/v1.metadata.json",
            "metadata": staged_response["metadata"].clone()
        });
        let commit_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1")
            .match_body(mockito::Matcher::Json(
                serde_json::to_value(&expected_request).unwrap(),
            ))
            .with_status(200)
            .with_body(commit_response.to_string())
            .expect(1)
            .create_async()
            .await;

        let committed = catalog
            .update_table(
                TableCommit::builder()
                    .ident(ident)
                    .requirements(vec![TableRequirement::NotExist])
                    .updates(updates)
                    .build(),
            )
            .await
            .unwrap();
        assert_eq!(
            Some("s3://warehouse/database/table/metadata/v1.metadata.json"),
            committed.metadata_location()
        );

        config_mock.assert_async().await;
        stage_create_mock.assert_async().await;
        commit_mock.assert_async().await;
    }

    fn typed_stage_creation() -> TableCreation {
        TableCreation::builder()
            .name("test1".to_string())
            .schema(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    ])
                    .build()
                    .unwrap(),
            )
            .build()
    }

    fn typed_staged_commit() -> TableCommit {
        TableCommit::builder()
            .ident(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .requirements(vec![TableRequirement::NotExist])
            .updates(Vec::new())
            .build()
    }

    fn catalog_error_body(status: u16) -> String {
        json!({
            "error": {
                "message": "typed staged operation failure",
                "type": "TestException",
                "code": status
            }
        })
        .to_string()
    }

    #[tokio::test]
    async fn typed_deferred_stage_create_keeps_vended_credentials_out_of_file_io() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;
        let response = std::fs::read_to_string(format!(
            "{}/testdata/create_table_response.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
        .replacen(
            "\n}",
            ",\n  \"storage-credentials\": [{\n    \"prefix\": \"s3://warehouse/database/\",\n    \"config\": {\"s3.access-key-id\": \"access-canary\", \"s3.secret-access-key\": \"secret-canary\"}\n  }]\n}",
            1,
        );
        let stage_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables")
            .match_header(
                REST_CATALOG_HEADER_ACCESS_DELEGATION,
                REST_CATALOG_ACCESS_DELEGATION_VENDED_CREDENTIALS,
            )
            .with_status(200)
            .with_body(response)
            .create_async()
            .await;
        // No StorageFactory is installed. A successful deferred response
        // proves the method did not select a catalog-global FileIO first.
        let catalog =
            RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build(), None);

        let staged = catalog
            .stage_create_table_typed_deferred_with_access_delegation(
                &NamespaceIdent::from_strs(["ns1"]).unwrap(),
                typed_stage_creation(),
            )
            .await
            .expect("deferred stage response");
        let (materialization, initialization_updates, delegation) = staged.into_parts();
        assert!(!initialization_updates.is_empty());
        assert!(delegation.is_present());
        assert!(!format!("{materialization:?}").contains("secret-canary"));
        assert!(!format!("{delegation:?}").contains("secret-canary"));
        let table = materialization
            .materialize_with_file_io(FileIO::new_with_memory())
            .expect("caller-owned request FileIO materializes staged table");
        assert_eq!(table.identifier().name, "test1");

        config_mock.assert_async().await;
        stage_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_typed_stage_create_preserves_conflict_and_dispatch_uncertainty() {
        for (status, expected_conflict) in [(409, true), (503, false)] {
            let mut server = Server::new_async().await;
            let config_mock = create_config_mock(&mut server).await;
            let stage_mock = server
                .mock("POST", "/v1/namespaces/ns1/tables")
                .with_status(status)
                .with_body(catalog_error_body(status as u16))
                .expect(1)
                .create_async()
                .await;
            let catalog = RestCatalog::new(
                RestCatalogConfig::builder().uri(server.url()).build(),
                Some(Arc::new(LocalFsStorageFactory)),
            );
            let error = catalog
                .stage_create_table_typed(
                    &NamespaceIdent::from_strs(["ns1"]).unwrap(),
                    typed_stage_creation(),
                )
                .await
                .unwrap_err();
            assert_eq!(
                expected_conflict,
                matches!(error, StagedCreateError::Conflict(_))
            );
            assert_eq!(
                !expected_conflict,
                matches!(error, StagedCreateError::PossiblyDispatched(_))
            );
            config_mock.assert_async().await;
            stage_mock.assert_async().await;
        }
    }

    #[tokio::test]
    async fn test_typed_stage_create_request_build_failure_is_known_not_dispatched() {
        let catalog = RestCatalog::new(
            RestCatalogConfig::builder()
                .uri("://invalid".to_string())
                .build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );
        let error = catalog
            .stage_create_table_typed(
                &NamespaceIdent::from_strs(["ns1"]).unwrap(),
                typed_stage_creation(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, StagedCreateError::KnownNotDispatched(_)));
    }

    #[tokio::test]
    async fn test_typed_staged_commit_preserves_conflict_and_response_uncertainty() {
        for (status, expected) in [(409, "conflict"), (503, "dispatch"), (200, "response")] {
            let mut server = Server::new_async().await;
            let config_mock = create_config_mock(&mut server).await;
            let body = if status == 200 {
                "not-json".to_string()
            } else {
                catalog_error_body(status)
            };
            let commit_mock = server
                .mock("POST", "/v1/namespaces/ns1/tables/test1")
                .with_status(status as usize)
                .with_body(body)
                .expect(1)
                .create_async()
                .await;
            let catalog = RestCatalog::new(
                RestCatalogConfig::builder().uri(server.url()).build(),
                Some(Arc::new(LocalFsStorageFactory)),
            );
            let error = catalog
                .commit_staged_table_typed(typed_staged_commit())
                .await
                .unwrap_err();
            match expected {
                "conflict" => assert!(matches!(error, StagedCommitError::Conflict(_))),
                "dispatch" => assert!(matches!(error, StagedCommitError::PossiblyDispatched(_))),
                "response" => {
                    assert!(matches!(
                        error,
                        StagedCommitError::CommittedResponseInvalid(_)
                    ))
                }
                _ => unreachable!(),
            }
            config_mock.assert_async().await;
            commit_mock.assert_async().await;
        }
    }

    #[tokio::test]
    async fn typed_staged_commit_with_caller_file_io_needs_no_catalog_storage_factory() {
        let mut server = Server::new_async().await;
        let config_mock = create_config_mock(&mut server).await;
        let response = json!({
            "metadata-location": "s3://warehouse/database/table/metadata/v1.metadata.json",
            "metadata": staged_metadata_json(),
        });
        let commit_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body(response.to_string())
            .expect(1)
            .create_async()
            .await;
        // The caller-owned FileIO is the only storage capability. A vended
        // catalog must not need a catalog-global StorageFactory to finalize a
        // successful staged commit response.
        let catalog =
            RestCatalog::new(RestCatalogConfig::builder().uri(server.url()).build(), None);

        let table = catalog
            .commit_staged_table_typed_with_file_io(
                typed_staged_commit(),
                FileIO::new_with_memory(),
            )
            .await
            .expect("caller-owned FileIO materializes committed staged response");
        assert_eq!(table.identifier().name, "test1");

        config_mock.assert_async().await;
        commit_mock.assert_async().await;
    }

    fn staged_metadata_json() -> serde_json::Value {
        let response: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/testdata/create_table_response.json"
        )))
        .unwrap();
        response["metadata"].clone()
    }

    #[test]
    fn test_staged_create_initialization_updates_preserve_v2_and_v3_format() {
        for expected in [FormatVersion::V2, FormatVersion::V3] {
            let metadata = TableMetadataBuilder::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    ])
                    .build()
                    .unwrap(),
                UnboundPartitionSpec::default(),
                SortOrder::unsorted_order(),
                "s3://warehouse/database/versioned".to_string(),
                expected,
                HashMap::new(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            let updates = metadata.staged_create_initialization_updates().unwrap();
            assert!(updates.contains(&TableUpdate::UpgradeFormatVersion {
                format_version: expected,
            }));
        }
    }

    #[test]
    fn test_staged_create_initialization_updates_reject_snapshot_state() {
        let mut metadata_json = staged_metadata_json();
        let snapshot_id = 3055729675574597000i64;
        metadata_json["current-snapshot-id"] = json!(snapshot_id);
        metadata_json["snapshots"] = json!([{
            "snapshot-id": snapshot_id,
            "timestamp-ms": 1657810968051i64,
            "summary": {"operation": "append"},
            "manifest-list": "s3://warehouse/database/table/metadata/snap-proof.avro"
        }]);
        metadata_json["refs"] = json!({
            "main": {"snapshot-id": snapshot_id, "type": "branch"}
        });
        let metadata: TableMetadata = serde_json::from_value(metadata_json).unwrap();
        let error = metadata.staged_create_initialization_updates().unwrap_err();
        assert_eq!(ErrorKind::DataInvalid, error.kind());
        assert!(error.message().contains("cannot be represented"));
    }

    #[test]
    fn test_staged_create_initialization_updates_reject_unrepresented_partition_watermark() {
        let mut metadata_json = staged_metadata_json();
        metadata_json["last-partition-id"] = json!(1000);
        let metadata: TableMetadata = serde_json::from_value(metadata_json).unwrap();
        let error = metadata.staged_create_initialization_updates().unwrap_err();
        assert_eq!(ErrorKind::DataInvalid, error.kind());
        assert!(error.message().contains("partition high-watermark"));
    }

    #[tokio::test]
    async fn test_create_table_409() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let create_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables")
            .with_status(409)
            .with_body(r#"
{
    "error": {
        "message": "Table already exists: ns1.test1 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
        "type": "AlreadyExistsException",
        "code": 409
    }
}
            "#)
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let table_creation = TableCreation::builder()
            .name("test1".to_string())
            .schema(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(1)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap(),
            )
            .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
            .build();

        let table_result = catalog
            .create_table(&NamespaceIdent::from_strs(["ns1"]).unwrap(), table_creation)
            .await;

        assert!(table_result.is_err());
        assert!(
            table_result
                .err()
                .unwrap()
                .message()
                .contains("already exists")
        );

        config_mock.assert_async().await;
        create_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_update_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let load_table_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .expect(1)
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "load_table_response.json"
            ))
            .create_async()
            .await;

        let update_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "update_table_response.json"
            ))
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let table1 = {
            let file = File::open(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "create_table_response.json"
            ))
            .unwrap();
            let reader = BufReader::new(file);
            let resp = serde_json::from_reader::<_, LoadTableResult>(reader).unwrap();

            Table::builder()
                .metadata(resp.metadata)
                .metadata_location(resp.metadata_location.unwrap())
                .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
                .file_io(FileIO::new_with_fs())
                .build()
                .unwrap()
        };

        let tx = Transaction::new(&table1);
        let table = tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V2)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        assert_eq!(
            &TableIdent::from_strs(vec!["ns1", "test1"]).unwrap(),
            table.identifier()
        );
        assert_eq!(
            "s3://warehouse/database/table/metadata.json",
            table.metadata_location().unwrap()
        );
        assert_eq!(FormatVersion::V2, table.metadata().format_version());
        assert_eq!("s3://warehouse/database/table", table.metadata().location());
        assert_eq!(
            uuid!("bf289591-dcc0-4234-ad4f-5c3eed811a29"),
            table.metadata().uuid()
        );
        assert_eq!(
            1657810967051,
            table
                .metadata()
                .last_updated_timestamp()
                .unwrap()
                .timestamp_millis()
        );
        assert_eq!(
            vec![&Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean))
                            .into(),
                    ])
                    .with_schema_id(0)
                    .with_identifier_field_ids(vec![2])
                    .build()
                    .unwrap()
            )],
            table.metadata().schemas_iter().collect::<Vec<_>>()
        );
        assert_eq!(
            &HashMap::from([
                (
                    "write.delete.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
                (
                    "write.metadata.compression-codec".to_string(),
                    "gzip".to_string()
                ),
                (
                    "write.summary.partition-limit".to_string(),
                    "100".to_string()
                ),
                (
                    "write.parquet.compression-codec".to_string(),
                    "zstd".to_string()
                ),
            ]),
            table.metadata().properties()
        );
        assert!(table.metadata().current_snapshot().is_none());
        assert!(table.metadata().history().is_empty());
        assert_eq!(
            vec![&Arc::new(SortOrder {
                order_id: 0,
                fields: vec![],
            })],
            table.metadata().sort_orders_iter().collect::<Vec<_>>()
        );

        config_mock.assert_async().await;
        update_table_mock.assert_async().await;
        load_table_mock.assert_async().await
    }

    #[tokio::test]
    async fn test_update_table_404() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let load_table_mock = server
            .mock("GET", "/v1/namespaces/ns1/tables/test1")
            .expect(1)
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "load_table_response.json"
            ))
            .create_async()
            .await;

        let update_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/tables/test1")
            .with_status(404)
            .with_body(
                r#"
{
    "error": {
        "message": "The given table does not exist",
        "type": "NoSuchTableException",
        "code": 404
    }
}
            "#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let table1 = {
            let file = File::open(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "create_table_response.json"
            ))
            .unwrap();
            let reader = BufReader::new(file);
            let resp = serde_json::from_reader::<_, LoadTableResult>(reader).unwrap();

            Table::builder()
                .metadata(resp.metadata)
                .metadata_location(resp.metadata_location.unwrap())
                .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
                .file_io(FileIO::new_with_fs())
                .build()
                .unwrap()
        };

        let tx = Transaction::new(&table1);
        let table_result = tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V2)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await;

        assert!(table_result.is_err());
        assert!(
            table_result
                .err()
                .unwrap()
                .message()
                .contains("does not exist")
        );

        config_mock.assert_async().await;
        update_table_mock.assert_async().await;
        load_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_register_table() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let register_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/register")
            .with_status(200)
            .with_body_from_file(format!(
                "{}/testdata/{}",
                env!("CARGO_MANIFEST_DIR"),
                "load_table_response.json"
            ))
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );
        let table_ident =
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "test1".to_string());
        let metadata_location = String::from(
            "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
        );

        let table = catalog
            .register_table(&table_ident, metadata_location)
            .await
            .unwrap();

        assert_eq!(
            &TableIdent::from_strs(vec!["ns1", "test1"]).unwrap(),
            table.identifier()
        );
        assert_eq!(
            "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
            table.metadata_location().unwrap()
        );

        config_mock.assert_async().await;
        register_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_register_table_404() {
        let mut server = Server::new_async().await;

        let config_mock = create_config_mock(&mut server).await;

        let register_table_mock = server
            .mock("POST", "/v1/namespaces/ns1/register")
            .with_status(404)
            .with_body(
                r#"
{
    "error": {
        "message": "The namespace specified does not exist",
        "type": "NoSuchNamespaceErrorException",
        "code": 404
    }
}
            "#,
            )
            .create_async()
            .await;

        let catalog = RestCatalog::new(
            RestCatalogConfig::builder().uri(server.url()).build(),
            Some(Arc::new(LocalFsStorageFactory)),
        );

        let table_ident =
            TableIdent::new(NamespaceIdent::new("ns1".to_string()), "test1".to_string());
        let metadata_location = String::from(
            "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
        );
        let table = catalog
            .register_table(&table_ident, metadata_location)
            .await;

        assert!(table.is_err());
        assert!(table.err().unwrap().message().contains("does not exist"));

        config_mock.assert_async().await;
        register_table_mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_create_rest_catalog() {
        let builder = RestCatalogBuilder::default().with_client(Client::new());

        let catalog = builder
            .load(
                "test",
                HashMap::from([
                    (
                        REST_CATALOG_PROP_URI.to_string(),
                        "http://localhost:8080".to_string(),
                    ),
                    ("a".to_string(), "b".to_string()),
                ]),
            )
            .await;

        assert!(catalog.is_ok());

        let catalog_config = catalog.unwrap().user_config;
        assert_eq!(catalog_config.name.as_deref(), Some("test"));
        assert_eq!(catalog_config.uri, "http://localhost:8080");
        assert_eq!(catalog_config.warehouse, None);
        assert!(catalog_config.client.is_some());

        assert_eq!(catalog_config.props.get("a"), Some(&"b".to_string()));
        assert!(!catalog_config.props.contains_key(REST_CATALOG_PROP_URI));
    }

    #[tokio::test]
    async fn test_create_rest_catalog_no_uri() {
        let builder = RestCatalogBuilder::default();

        let catalog = builder
            .load(
                "test",
                HashMap::from([(
                    REST_CATALOG_PROP_WAREHOUSE.to_string(),
                    "s3://warehouse".to_string(),
                )]),
            )
            .await;

        assert!(catalog.is_err());
        if let Err(err) = catalog {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert_eq!(err.message(), "Catalog uri is required");
        }
    }
}
