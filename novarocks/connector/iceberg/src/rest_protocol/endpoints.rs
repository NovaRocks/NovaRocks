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

//! The NovaRocks-issued REST read endpoints of one catalog generation.
//!
//! Every request uses the one session binding captured while upstream
//! established its session (L27): single-page listing, the delegated table
//! load and baseline re-read, table existence in both modes, the credentials
//! endpoint, view reads, and the load-table credential acquisition. Reads keep
//! today's error classes (L17): a load's 404 is `NotFound`, an existence
//! check's 404 is `false`, everything else is `Unavailable`. Non-success
//! bodies are never read.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use reqwest::Method;
use reqwest::header::HeaderValue;
use serde::de::DeserializeOwned;

use crate::iceberg::io::StorageFactory;
use crate::iceberg::spec::ViewMetadata;
use crate::iceberg::{NamespaceIdent, Runtime, TableIdent};
use crate::rest_protocol::attempt::{AttemptFailure, SessionGate, ensure_session};
use crate::rest_protocol::auth::RestAuthOwner;
use crate::rest_protocol::binding::SessionBinding;
use crate::rest_protocol::deferred::{FrozenLoadTable, RestDelegation};
use crate::rest_protocol::evidence::DispatchStage;
use crate::rest_protocol::http::{RequestContext, ResponseBody, RestRequest};
use crate::rest_protocol::json::{JsonScanner, ViewSink};
use crate::rest_protocol::minimal_view::{BackendAcquisitionSink, CredentialsSink};
use crate::rest_protocol::wire::{
    ListNamespaceResponse, ListTablesResponse, LoadTableResult, NamespaceResponse,
};

/// The value the client sends to ask for vended storage credentials.
pub(crate) const VENDED_CREDENTIALS: HeaderValue = HeaderValue::from_static("vended-credentials");

/// `GET .../views/{view}` response. NovaRocks owns this wire shape: the
/// upstream REST crate has no view API.
#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LoadViewResult {
    #[allow(dead_code)]
    metadata_location: String,
    metadata: ViewMetadata,
}

/// One catalog generation's REST session authority and read endpoints.
pub(crate) struct RestEndpoints {
    acquisition_owner: novarocks_spi::connector::CredentialAcquisitionOwnerId,
    pub(crate) publication_generation: crate::catalog::publication::PublicationGeneration,
    properties: Arc<std::collections::HashMap<String, String>>,
    owner: Arc<RestAuthOwner>,
    gate: Arc<SessionGate>,
    runtime: Runtime,
    /// The static generation's storage factory; `None` in the vended mode,
    /// which never materializes with a catalog-global FileIO.
    storage_factory: Option<Arc<dyn StorageFactory>>,
    declared_routing_headers: BTreeSet<String>,
    /// Whether the catalog definition declares the execution nodes'
    /// `DataCredentialVending` identity (L25).
    vending_declared: bool,
}

impl fmt::Debug for RestEndpoints {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print the upstream client (L29).
        formatter
            .debug_struct("RestEndpoints")
            .field("owner", &self.owner)
            .field("established", &self.gate.ready().is_some())
            .field("declared_routing_headers", &self.declared_routing_headers)
            .finish_non_exhaustive()
    }
}

/// Typed conditional result; callers own the representation binding and may
/// reuse only the exact complete metadata validated by this ETag.
pub(crate) enum ConditionalTableLoad {
    Complete {
        table: Box<FrozenLoadTable>,
        delegation: RestDelegation,
        etag: Option<HeaderValue>,
    },
    NotModified,
}

/// A validator for exactly one complete representation from one immutable
/// catalog generation/session/auth owner. Fields never leave this module.
#[derive(Clone)]
pub(crate) struct CatalogMetadataValidator {
    endpoints: Arc<RestEndpoints>,
    binding: Arc<SessionBinding>,
    url: String,
    etag: HeaderValue,
}
impl fmt::Debug for CatalogMetadataValidator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CatalogMetadataValidator { .. }")
    }
}

/// Where a NovaRocks read ended.
enum Answer {
    /// A success status with its unread body.
    Success { code: u16, body: ResponseBody },
    /// Any other status; the body was not read.
    Status(u16),
}

impl RestEndpoints {
    pub(crate) const fn acquisition_owner(
        &self,
    ) -> novarocks_spi::connector::CredentialAcquisitionOwnerId {
        self.acquisition_owner
    }

    pub(crate) fn new(
        properties: Arc<std::collections::HashMap<String, String>>,
        owner: Arc<RestAuthOwner>,
        runtime: Runtime,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        declared_routing_headers: BTreeSet<String>,
        vending_declared: bool,
    ) -> Self {
        Self {
            acquisition_owner: novarocks_spi::connector::CredentialAcquisitionOwnerId::new(),
            publication_generation: Default::default(),
            properties,
            owner,
            gate: Arc::new(SessionGate::default()),
            runtime,
            storage_factory,
            declared_routing_headers,
            vending_declared,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_gate(
        properties: Arc<std::collections::HashMap<String, String>>,
        owner: Arc<RestAuthOwner>,
        runtime: Runtime,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        declared_routing_headers: BTreeSet<String>,
        vending_declared: bool,
        gate: Arc<SessionGate>,
    ) -> Self {
        let mut endpoints = Self::new(
            properties,
            owner,
            runtime,
            storage_factory,
            declared_routing_headers,
            vending_declared,
        );
        endpoints.gate = gate;
        endpoints
    }

    /// The binding of an already established session, without establishing
    /// one. A vended lease is formed only after a NovaRocks read on this
    /// session, so its binding always exists by then.
    pub(crate) fn established_binding(&self) -> Option<Arc<SessionBinding>> {
        self.gate.ready()
    }

    /// The admission facts every routing context of this generation is
    /// checked against (L25).
    pub(crate) fn route_admission(&self) -> crate::rest_protocol::routing::RouteAdmission<'_> {
        crate::rest_protocol::routing::RouteAdmission {
            vending_declared: self.vending_declared,
            declared_headers: &self.declared_routing_headers,
        }
    }

    /// Whether this generation vends storage credentials (it has no
    /// catalog-global storage factory).
    pub(crate) fn is_vended(&self) -> bool {
        self.storage_factory.is_none()
    }

    /// A FileIO from the static generation's storage factory, or `None` in
    /// the vended mode.
    pub(crate) fn static_file_io(&self) -> Option<crate::iceberg::io::FileIO> {
        self.storage_factory
            .as_ref()
            .map(|factory| crate::iceberg::io::FileIOBuilder::new(Arc::clone(factory)).build())
    }

    /// The establishment attempt's own result, for callers that classify a
    /// failed step (mutations: `NotWritten(Binding(step))`).
    pub(crate) async fn session(&self) -> Result<Arc<SessionBinding>, AttemptFailure> {
        ensure_session(&self.owner, &self.gate, &self.properties).await
    }

    pub(crate) async fn session_in(
        &self,
        context: &RequestContext,
    ) -> Result<Arc<SessionBinding>, AttemptFailure> {
        crate::rest_protocol::attempt::ensure_session_in(
            &self.owner,
            &self.gate,
            &self.properties,
            context,
        )
        .await
    }

    /// The generation's session binding, establishing the session when needed
    /// (L2). Concurrent callers share one establishment attempt.
    pub(crate) async fn binding(&self) -> Result<Arc<SessionBinding>, ConnectorError> {
        self.binding_in(&RequestContext::for_read()).await
    }
    pub(crate) async fn binding_in(
        &self,
        context: &RequestContext,
    ) -> Result<Arc<SessionBinding>, ConnectorError> {
        crate::rest_protocol::attempt::ensure_session_in(
            &self.owner,
            &self.gate,
            &self.properties,
            context,
        )
        .await
        .map_err(|failure| {
            failure.into_connector(|step| {
                ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    format!("Iceberg REST session could not be established: {step:?}"),
                )
            })
        })
    }

    pub(crate) fn owner(&self) -> &Arc<RestAuthOwner> {
        &self.owner
    }

    pub(crate) fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub(crate) fn declared_routing_headers(&self) -> &BTreeSet<String> {
        &self.declared_routing_headers
    }

    /// One page of namespaces. `parent` is never sent (L27).
    pub(crate) async fn list_namespaces_page(
        &self,
        page_token: Option<&str>,
        page_size: usize,
    ) -> Result<ListNamespaceResponse, ConnectorError> {
        const OPERATION: &str = "list namespaces";
        let binding = self.binding().await?;
        let url = page_url(binding.namespaces_url(), page_token, page_size, OPERATION)?;
        match self.get(OPERATION, &binding, url, None, &[200]).await? {
            Answer::Success { body, .. } => decode_document(OPERATION, body).await,
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// One page of tables in `namespace`.
    pub(crate) async fn list_tables_page(
        &self,
        namespace: &NamespaceIdent,
        page_token: Option<&str>,
        page_size: usize,
    ) -> Result<ListTablesResponse, ConnectorError> {
        const OPERATION: &str = "list tables";
        let binding = self.binding().await?;
        let url = page_url(
            binding.tables_url(namespace),
            page_token,
            page_size,
            OPERATION,
        )?;
        match self.get(OPERATION, &binding, url, None, &[200]).await? {
            Answer::Success { body, .. } => decode_document(OPERATION, body).await,
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// One page of views in `namespace`; a missing namespace is `NotFound`.
    pub(crate) async fn list_views_page(
        &self,
        namespace: &NamespaceIdent,
        page_token: Option<&str>,
        page_size: usize,
    ) -> Result<ListTablesResponse, ConnectorError> {
        const OPERATION: &str = "list views";
        let binding = self.binding().await?;
        let url = page_url(
            binding.views_url(namespace),
            page_token,
            page_size,
            OPERATION,
        )?;
        match self.get(OPERATION, &binding, url, None, &[200]).await? {
            Answer::Success { body, .. } => decode_document(OPERATION, body).await,
            Answer::Status(404) => Err(not_found(OPERATION)),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// One governed listing page's original bytes. Parsing/collector work is
    /// borrowed only by the same Provider's closed metadata CPU author.
    pub(crate) async fn list_views_page_bytes_in(
        &self,
        namespace: &NamespaceIdent,
        page_token: Option<&str>,
        page_size: usize,
        context: &RequestContext,
    ) -> Result<Vec<u8>, ConnectorError> {
        const OPERATION: &str = "list views";
        let binding = self.binding_in(context).await?;
        let url = page_url(
            binding.views_url(namespace),
            page_token,
            page_size,
            OPERATION,
        )?;
        match self
            .get_in(OPERATION, &binding, url, None, &[200], context)
            .await?
        {
            Answer::Success { body, .. } => read_document_bytes(OPERATION, body).await,
            Answer::Status(404) => Err(not_found(OPERATION)),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// Complete metadata or a conditionally validated representation. A 304
    /// never decodes a body or contributes storage credentials.
    pub(crate) async fn load_table_conditional(
        &self,
        table: &TableIdent,
        delegated: bool,
        etag: Option<&HeaderValue>,
    ) -> Result<ConditionalTableLoad, ConnectorError> {
        let binding = self.binding().await?;
        self.load_table_conditional_request(
            table,
            delegated,
            etag,
            &binding,
            binding.table_url(table),
            &RequestContext::for_read(),
        )
        .await
    }

    async fn load_table_conditional_request(
        &self,
        table: &TableIdent,
        delegated: bool,
        etag: Option<&HeaderValue>,
        binding: &SessionBinding,
        url: String,
        context: &RequestContext,
    ) -> Result<ConditionalTableLoad, ConnectorError> {
        const OPERATION: &str = "load table";
        let mut headers = binding.request_headers(delegated.then_some(VENDED_CREDENTIALS));
        if let Some(etag) = etag {
            headers.insert(reqwest::header::IF_NONE_MATCH, etag.clone());
        }
        self.owner
            .authorize_in(&mut headers, context)
            .await
            .map_err(|failure| {
                failure.into_connector(|_| {
                    unavailable("Iceberg REST load table failed: no catalog token".into())
                })
            })?;
        let (stage, body) = self
            .owner
            .http()
            .send_once(
                Ok(RestRequest::new(Method::GET, url, headers)),
                &[200, 304],
                context,
            )
            .await?;
        match (stage, body) {
            (DispatchStage::Status { code: 200, .. }, Some(body)) => {
                let etag = body.header(reqwest::header::ETAG).cloned();
                let response: LoadTableResult = decode_document(OPERATION, body).await?;
                let (table, delegation) =
                    FrozenLoadTable::from_response(table.clone(), response, self.runtime.clone());
                Ok(ConditionalTableLoad::Complete {
                    table: Box::new(table),
                    delegation,
                    etag,
                })
            }
            (DispatchStage::Status { code: 304, .. }, _) if etag.is_some() => {
                Ok(ConditionalTableLoad::NotModified)
            }
            (DispatchStage::Status { code: 304, .. }, _) => Err(unavailable(
                "Iceberg REST load table returned 304 without a conditional request".into(),
            )),
            (DispatchStage::Status { code: 404, .. }, _) => Err(not_found(OPERATION)),
            (DispatchStage::Status { code, .. }, _) => Err(status_error(OPERATION, code)),
            _ => Err(unavailable(
                "Iceberg REST load table failed without a complete response".into(),
            )),
        }
    }

    /// GC always asks for all valid snapshots. Conditional validation never
    /// crosses endpoint generation, session/auth owner, route or query shape.
    pub(crate) async fn load_complete_metadata(
        self: &Arc<Self>,
        table: &TableIdent,
        validator: Option<&CatalogMetadataValidator>,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<crate::catalog::CatalogMetadataLoad, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        context.check_active()?;
        if validator.is_some_and(|value| !Arc::ptr_eq(&value.endpoints, self)) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Metadata validator belongs to another catalog generation",
            ));
        }
        let request_context = RequestContext::from_connector(context);
        let binding = self.binding_in(&request_context).await?;
        context.check_active()?;
        let mut url = reqwest::Url::parse(&binding.table_url(table)).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Complete metadata route is invalid",
            )
        })?;
        if url.query_pairs().any(|(name, _)| name == "snapshots") {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Complete metadata route already defines a snapshot representation",
            ));
        }
        url.query_pairs_mut().append_pair("snapshots", "all");
        let url = url.to_string();
        if validator.is_some_and(|value| !Arc::ptr_eq(&value.binding, &binding) || value.url != url)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Metadata validator belongs to another complete representation",
            ));
        }

        let loaded = self
            .load_table_conditional_request(
                table,
                false,
                validator.map(|value| &value.etag),
                &binding,
                url.clone(),
                &request_context,
            )
            .await?;
        context.check_active()?;
        match loaded {
            ConditionalTableLoad::Complete {
                table,
                delegation: _,
                etag,
            } => {
                let validator = etag.map(|etag| CatalogMetadataValidator {
                    endpoints: Arc::clone(self),
                    binding,
                    url,
                    etag,
                });
                crate::catalog::metadata_read::complete_metadata(
                    table.metadata().clone(),
                    table.metadata_location(),
                    validator,
                )
            }
            ConditionalTableLoad::NotModified => {
                Ok(crate::catalog::CatalogMetadataLoad::NotModified)
            }
        }
    }

    pub(crate) async fn load_table(
        &self,
        table: &TableIdent,
        delegated: bool,
    ) -> Result<(FrozenLoadTable, RestDelegation), ConnectorError> {
        self.load_table_with_snapshots(table, delegated, crate::catalog::SnapshotLoading::All)
            .await
    }

    pub(crate) async fn load_table_with_snapshots(
        &self,
        table: &TableIdent,
        delegated: bool,
        snapshots: crate::catalog::SnapshotLoading,
    ) -> Result<(FrozenLoadTable, RestDelegation), ConnectorError> {
        self.load_table_with_snapshots_in(table, delegated, snapshots, &RequestContext::for_read())
            .await
    }

    pub(crate) async fn load_table_with_snapshots_in(
        &self,
        table: &TableIdent,
        delegated: bool,
        snapshots: crate::catalog::SnapshotLoading,
        context: &RequestContext,
    ) -> Result<(FrozenLoadTable, RestDelegation), ConnectorError> {
        let binding = self.binding_in(context).await?;
        match self
            .load_table_conditional_request(
                table,
                delegated,
                None,
                &binding,
                binding.load_table_url(table, snapshots),
                context,
            )
            .await?
        {
            ConditionalTableLoad::Complete {
                table, delegation, ..
            } => Ok((*table, delegation)),
            ConditionalTableLoad::NotModified => unreachable!("unconditional load rejects 304"),
        }
    }

    /// Namespace existence is a catalog fact and never requests storage authority.
    pub(crate) async fn namespace_exists(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<bool, ConnectorError> {
        const OPERATION: &str = "check namespace existence";
        let binding = self.binding().await?;
        if !binding.namespace_head_advertised() {
            return match self.load_namespace(namespace).await {
                Ok(_) => Ok(true),
                Err(error) if error.kind() == ConnectorErrorKind::NotFound => Ok(false),
                Err(error) => Err(error),
            };
        }
        match self
            .send(
                OPERATION,
                &binding,
                Method::HEAD,
                binding.namespace_url(namespace),
                None,
                &[200, 204],
            )
            .await?
        {
            Answer::Success { .. } => Ok(true),
            Answer::Status(404) => Ok(false),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    pub(crate) async fn load_namespace(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<crate::iceberg::Namespace, ConnectorError> {
        const OPERATION: &str = "load namespace";
        let binding = self.binding().await?;
        match self
            .get(
                OPERATION,
                &binding,
                binding.namespace_url(namespace),
                None,
                &[200],
            )
            .await?
        {
            Answer::Success { body, .. } => decode_document::<NamespaceResponse>(OPERATION, body)
                .await
                .map(Into::into),
            Answer::Status(404) => Err(not_found(OPERATION)),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// Whether `table` exists, in both access-delegation modes (L27): `HEAD`
    /// when the server advertised it, otherwise a `GET` without the
    /// delegation header. Only the status is consulted; no body is read and
    /// no FileIO or table object is built.
    pub(crate) async fn table_exists(&self, table: &TableIdent) -> Result<bool, ConnectorError> {
        const OPERATION: &str = "check table existence";
        let binding = self.binding().await?;
        let (method, success): (Method, &[u16]) = if binding.table_head_advertised() {
            (Method::HEAD, &[200, 204])
        } else {
            (Method::GET, &[200])
        };
        match self
            .send(
                OPERATION,
                &binding,
                method,
                binding.table_url(table),
                None,
                success,
            )
            .await?
        {
            // Dropped unread: existence is the status alone.
            Answer::Success { .. } => Ok(true),
            Answer::Status(404) => Ok(false),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// Load one view's metadata; a missing view is `NotFound`.
    pub(crate) async fn load_view(
        &self,
        view: &TableIdent,
    ) -> Result<ViewMetadata, ConnectorError> {
        const OPERATION: &str = "load view";
        let binding = self.binding().await?;
        match self
            .get(OPERATION, &binding, binding.view_url(view), None, &[200])
            .await?
        {
            Answer::Success { body, .. } => {
                let response: LoadViewResult = decode_document(OPERATION, body).await?;
                Ok(response.metadata)
            }
            Answer::Status(404) => Err(not_found(OPERATION)),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// Whether `view` exists (`HEAD`).
    pub(crate) async fn view_exists(&self, view: &TableIdent) -> Result<bool, ConnectorError> {
        const OPERATION: &str = "check view existence";
        let binding = self.binding().await?;
        match self
            .send(
                OPERATION,
                &binding,
                Method::HEAD,
                binding.view_url(view),
                None,
                &[200, 204],
            )
            .await?
        {
            Answer::Success { .. } => Ok(true),
            Answer::Status(404) => Ok(false),
            Answer::Status(code) => Err(status_error(OPERATION, code)),
        }
    }

    /// Renew through the server-provided credentials endpoint: authenticated,
    /// without the delegation header, decoding `storage-credentials` only.
    /// Every request-layer failure is retryable before progress (L17).
    pub(crate) async fn load_credentials(
        &self,
        endpoint: &str,
    ) -> Result<RestDelegation, ConnectorError> {
        self.load_credentials_in(endpoint, &RequestContext::for_read())
            .await
    }

    pub(crate) async fn load_credentials_in(
        &self,
        endpoint: &str,
        context: &RequestContext,
    ) -> Result<RestDelegation, ConnectorError> {
        const OPERATION: &str = "load credentials";
        let binding = self.binding_in(context).await.map_err(retryable)?;
        match self
            .get_in(
                OPERATION,
                &binding,
                endpoint.to_string(),
                None,
                &[200],
                context,
            )
            .await
            .map_err(retryable)?
        {
            Answer::Success { body, .. } => {
                let view = scan_body(OPERATION, body, CredentialsSink::new())
                    .await
                    .map_err(retryable)?;
                Ok(RestDelegation::new(view.into_credentials()))
            }
            Answer::Status(code) => Err(retryable(status_error(OPERATION, code))),
        }
    }

    /// Acquire credentials through a delegated table load, decoding only the
    /// table UUID and `storage-credentials`. Every request-layer failure is
    /// retryable before progress (L17).
    pub(crate) async fn load_table_credentials(
        &self,
        table: &TableIdent,
    ) -> Result<(Option<String>, RestDelegation), ConnectorError> {
        self.load_table_credentials_in(table, &RequestContext::for_read())
            .await
    }

    pub(crate) async fn load_table_credentials_in(
        &self,
        table: &TableIdent,
        context: &RequestContext,
    ) -> Result<(Option<String>, RestDelegation), ConnectorError> {
        const OPERATION: &str = "acquire table credentials";
        let binding = self.binding_in(context).await.map_err(retryable)?;
        match self
            .get_in(
                OPERATION,
                &binding,
                binding.table_url(table),
                Some(VENDED_CREDENTIALS),
                &[200],
                context,
            )
            .await
            .map_err(retryable)?
        {
            Answer::Success { body, .. } => {
                let view = scan_body(OPERATION, body, BackendAcquisitionSink::new())
                    .await
                    .map_err(retryable)?;
                let (uuid, credentials) = view.into_parts();
                Ok((uuid, RestDelegation::new(credentials)))
            }
            Answer::Status(code) => Err(retryable(status_error(OPERATION, code))),
        }
    }

    async fn get(
        &self,
        operation: &'static str,
        binding: &SessionBinding,
        url: String,
        delegation: Option<HeaderValue>,
        success: &[u16],
    ) -> Result<Answer, ConnectorError> {
        self.get_in(
            operation,
            binding,
            url,
            delegation,
            success,
            &RequestContext::for_read(),
        )
        .await
    }

    async fn get_in(
        &self,
        operation: &'static str,
        binding: &SessionBinding,
        url: String,
        delegation: Option<HeaderValue>,
        success: &[u16],
        context: &RequestContext,
    ) -> Result<Answer, ConnectorError> {
        self.send_in(
            operation,
            binding,
            Method::GET,
            url,
            delegation,
            success,
            context,
        )
        .await
    }

    /// Send one NovaRocks read with the session's headers and this role's
    /// `Authorization`, exactly once.
    async fn send(
        &self,
        operation: &'static str,
        binding: &SessionBinding,
        method: Method,
        url: String,
        delegation: Option<HeaderValue>,
        success: &[u16],
    ) -> Result<Answer, ConnectorError> {
        self.send_in(
            operation,
            binding,
            method,
            url,
            delegation,
            success,
            &RequestContext::for_read(),
        )
        .await
    }

    async fn send_in(
        &self,
        operation: &'static str,
        binding: &SessionBinding,
        method: Method,
        url: String,
        delegation: Option<HeaderValue>,
        success: &[u16],
        context: &RequestContext,
    ) -> Result<Answer, ConnectorError> {
        let mut headers = binding.request_headers(delegation);
        self.owner
            .authorize_in(&mut headers, context)
            .await
            .map_err(|failure| {
                failure.into_connector(|step| {
                    unavailable(format!(
                        "Iceberg REST {operation} failed: no catalog token ({step:?})"
                    ))
                })
            })?;
        let (stage, body) = self
            .owner
            .http()
            .send_once(Ok(RestRequest::new(method, url, headers)), success, context)
            .await?;
        match (stage, body) {
            (DispatchStage::Status { code, .. }, Some(body)) => Ok(Answer::Success { code, body }),
            (DispatchStage::Status { code, .. }, None) => Ok(Answer::Status(code)),
            (DispatchStage::NotWritten(cause), _) => Err(unavailable(format!(
                "Iceberg REST {operation} failed before the request was written ({cause:?})"
            ))),
            (DispatchStage::WrittenNoStatus(cause), _) => Err(unavailable(format!(
                "Iceberg REST {operation} failed without a response ({cause:?})"
            ))),
        }
    }
}

/// An execution node's acquisition along a frozen routing context (spec 5.4).
///
/// It never opens a session: no `/v1/config`, no address of its own. The
/// request carries the protocol headers, the routing headers, the delegation
/// header on the load-table path, and the node's own `Authorization`. Only the
/// table UUID (load-table path) and `storage-credentials` are decoded. Every
/// request-layer failure is retryable before progress (L17).
pub(crate) async fn acquire_routed(
    owner: &RestAuthOwner,
    routing: &novarocks_spi::connector::CredentialRoutingContext,
    load_table: bool,
) -> Result<(Option<String>, RestDelegation), ConnectorError> {
    acquire_routed_in(owner, routing, load_table, &RequestContext::for_read()).await
}

pub(crate) async fn acquire_routed_in(
    owner: &RestAuthOwner,
    routing: &novarocks_spi::connector::CredentialRoutingContext,
    load_table: bool,
    context: &RequestContext,
) -> Result<(Option<String>, RestDelegation), ConnectorError> {
    const OPERATION: &str = "execution-node credential acquisition";
    let mut headers = crate::rest_protocol::binding::protocol_headers();
    for header in routing.headers() {
        let name = reqwest::header::HeaderName::from_bytes(header.name().as_bytes());
        let value = HeaderValue::from_str(header.value());
        let (Ok(name), Ok(value)) = (name, value) else {
            return Err(unavailable(format!(
                "Iceberg REST {OPERATION} has an unusable routing header"
            )));
        };
        headers.insert(name, value);
    }
    if load_table {
        headers.insert(
            crate::rest_protocol::binding::X_ICEBERG_ACCESS_DELEGATION,
            VENDED_CREDENTIALS,
        );
    }
    owner
        .authorize_in(&mut headers, context)
        .await
        .map_err(|failure| {
            failure.into_connector(|step| {
                retryable(unavailable(format!(
                    "Iceberg REST {OPERATION} failed: no catalog token ({step:?})"
                )))
            })
        })?;
    let (stage, body) = owner
        .http()
        .send_once(
            Ok(RestRequest::new(
                Method::GET,
                routing.url().to_string(),
                headers,
            )),
            &[200],
            context,
        )
        .await?;
    let body = match (stage, body) {
        (DispatchStage::Status { .. }, Some(body)) => body,
        (DispatchStage::Status { code, .. }, None) => {
            return Err(retryable(status_error(OPERATION, code)));
        }
        (DispatchStage::NotWritten(cause), _) => {
            return Err(retryable(unavailable(format!(
                "Iceberg REST {OPERATION} failed before the request was written ({cause:?})"
            ))));
        }
        (DispatchStage::WrittenNoStatus(cause), _) => {
            return Err(retryable(unavailable(format!(
                "Iceberg REST {OPERATION} failed without a response ({cause:?})"
            ))));
        }
    };
    if load_table {
        let view = scan_body(OPERATION, body, BackendAcquisitionSink::new())
            .await
            .map_err(retryable)?;
        let (uuid, credentials) = view.into_parts();
        Ok((uuid, RestDelegation::new(credentials)))
    } else {
        let view = scan_body(OPERATION, body, CredentialsSink::new())
            .await
            .map_err(retryable)?;
        Ok((None, RestDelegation::new(view.into_credentials())))
    }
}

/// Append the single-page parameters exactly as upstream encodes query pairs.
fn page_url(
    base: String,
    page_token: Option<&str>,
    page_size: usize,
    operation: &'static str,
) -> Result<String, ConnectorError> {
    if page_size == 0 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("Iceberg REST {operation} page size must be non-zero"),
        ));
    }
    let mut url = url::Url::parse(&base).map_err(|_| {
        unavailable(format!(
            "Iceberg REST {operation} failed: invalid catalog URL"
        ))
    })?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("pageSize", &page_size.to_string());
        if let Some(token) = page_token {
            pairs.append_pair("pageToken", token);
        }
    }
    Ok(url.into())
}

/// Read a complete document and decode it once (L8). The decoder's error is
/// withheld: it can quote response values.
async fn decode_document<T: DeserializeOwned>(
    operation: &'static str,
    body: ResponseBody,
) -> Result<T, ConnectorError> {
    let bytes = read_document_bytes(operation, body).await?;
    serde_json::from_slice(&bytes).map_err(|_| {
        unavailable(format!(
            "Iceberg REST {operation} returned a response that could not be decoded"
        ))
    })
}

async fn read_document_bytes(
    operation: &'static str,
    body: ResponseBody,
) -> Result<Vec<u8>, ConnectorError> {
    body.read_to_end().await.map_err(|error| {
        unavailable(format!(
            "Iceberg REST {operation} failed while reading the response ({error:?})"
        ))
    })
}

/// Scan a body chunk by chunk into a minimal view, never holding the raw
/// document (L8).
async fn scan_body<S: ViewSink>(
    operation: &'static str,
    mut body: ResponseBody,
    sink: S,
) -> Result<S::View, ConnectorError> {
    let mut scanner = JsonScanner::new(sink);
    while let Some(chunk) = body.next_chunk().await.map_err(|error| {
        unavailable(format!(
            "Iceberg REST {operation} failed while reading the response ({error:?})"
        ))
    })? {
        scanner.feed(&chunk).map_err(|error| {
            unavailable(format!(
                "Iceberg REST {operation} returned an invalid response ({error:?})"
            ))
        })?;
    }
    scanner.finish().map_err(|error| {
        unavailable(format!(
            "Iceberg REST {operation} returned an invalid response ({error:?})"
        ))
    })
}

fn status_error(operation: &'static str, code: u16) -> ConnectorError {
    unavailable(format!("Iceberg REST {operation} failed with HTTP {code}"))
}

fn not_found(operation: &'static str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::NotFound,
        format!("Iceberg REST {operation} failed: the object does not exist"),
    )
}

fn unavailable(message: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
}

fn retryable(error: ConnectorError) -> ConnectorError {
    error.with_retryable_before_progress()
}

#[cfg(test)]
mod tests {
    //! NovaRocks read endpoints against a scripted server, through the real
    //! provider-owned session: request split (L27), one session per generation (L2,
    //! L28), the delegation header per call site (L23) and read error classes
    //! (L17). Each test owns its loopback server; Drop joins it.
    use std::collections::HashMap;
    use std::sync::Arc;

    use novarocks_spi::connector::ConnectorErrorKind;

    use super::{ConditionalTableLoad, RestEndpoints};
    use crate::iceberg::{NamespaceIdent, TableIdent};
    use crate::rest_protocol::test_server::{
        RecordedRequest, Reply, Script, TestServer, oauth2_identity,
    };

    const DELEGATION: &str = "x-iceberg-access-delegation";
    const TABLE_HEAD: &str = "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}";

    async fn generation(
        server: &TestServer,
        extra: &[(&str, &str)],
        identity: Option<crate::access_binding::IcebergRestAuthMaterial>,
    ) -> Arc<RestEndpoints> {
        let mut properties: HashMap<String, String> = extra
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        properties.insert("uri".to_string(), server.url());
        crate::catalog_runtime::build_vended_rest_generation_for_test(
            properties,
            identity,
            crate::iceberg::Runtime::try_current().expect("test runtime"),
        )
        .await
        .expect("REST generation")
    }

    fn table(name: &str) -> TableIdent {
        TableIdent::new(NamespaceIdent::new("db".to_string()), name.to_string())
    }

    fn table_metadata_json(properties: &[(&str, &str)]) -> serde_json::Value {
        use crate::iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder,
            Type, UnboundPartitionSpec,
        };
        let metadata = TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
            UnboundPartitionSpec::default(),
            SortOrder::unsorted_order(),
            "s3://warehouse/db/t".into(),
            FormatVersion::V2,
            properties
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        serde_json::to_value(metadata).unwrap()
    }

    fn load_response(credentials: bool) -> serde_json::Value {
        let mut response = serde_json::json!({
            "metadata-location": "s3://warehouse/db/t/metadata/00001.metadata.json",
            "metadata": table_metadata_json(&[]),
        });
        if credentials {
            response["storage-credentials"] = serde_json::json!([{
                "prefix": "s3://warehouse/db/t",
                "config": {"s3.access-key-id": "AKIA-canary"},
            }]);
        }
        response
    }

    fn non_session(requests: Vec<RecordedRequest>) -> Vec<RecordedRequest> {
        requests
            .into_iter()
            .filter(|request| {
                request.path() != "/v1/config" && request.path() != "/v1/oauth/tokens"
            })
            .collect()
    }

    /// Refs responses legitimately retain snapshot-log entries whose snapshots
    /// are omitted. The wire fixture deliberately supplies those independent
    /// facts instead of deriving expectations through the metadata builder.
    #[tokio::test]
    async fn refs_load_uses_the_query_parameter_and_accepts_omitted_historical_snapshots() {
        use crate::catalog::SnapshotLoading;
        let mut response = load_response(false);
        let metadata = &mut response["metadata"];
        metadata["last-updated-ms"] = serde_json::json!(2000);
        metadata["last-sequence-number"] = serde_json::json!(2);
        metadata["current-snapshot-id"] = serde_json::json!(20);
        metadata["snapshots"] = serde_json::json!([{
            "snapshot-id": 20, "parent-snapshot-id": 10, "sequence-number": 2,
            "timestamp-ms": 2000, "manifest-list": "s3://warehouse/db/t/metadata/s20.avro",
            "summary": {"operation": "append"}, "schema-id": 0,
        }]);
        metadata["snapshot-log"] = serde_json::json!([
            {"timestamp-ms": 1000, "snapshot-id": 10},
            {"timestamp-ms": 2000, "snapshot-id": 20},
        ]);
        metadata["refs"] = serde_json::json!({"main": {"snapshot-id": 20, "type": "branch"}});
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/t",
                    Reply::json(200, &response),
                ),
        );
        let endpoints = generation(&server, &[], None).await;
        assert_eq!(SnapshotLoading::default(), SnapshotLoading::All);
        for delegated in [false, true] {
            let (frozen, _) = endpoints
                .load_table_with_snapshots(&table("t"), delegated, SnapshotLoading::Refs)
                .await
                .expect("refs metadata parses without historical snapshots");
            assert_eq!(frozen.metadata().snapshots().len(), 1);
            assert!(frozen.metadata().snapshot_by_id(10).is_none());
            assert_eq!(frozen.metadata().history().len(), 2);
            assert_eq!(frozen.metadata().current_snapshot_id(), Some(20));
            endpoints
                .load_table(&table("t"), delegated)
                .await
                .expect("default full load");
        }
        let requests = non_session(server.requests());
        assert_eq!(requests.len(), 4);
        for (index, request) in requests.iter().enumerate() {
            assert_eq!(
                request.query(),
                if index % 2 == 0 {
                    Some("snapshots=refs")
                } else {
                    None
                }
            );
            assert_eq!(
                request.header(DELEGATION),
                if index < 2 {
                    None
                } else {
                    Some("vended-credentials")
                }
            );
        }
    }

    #[tokio::test]
    async fn the_first_novarocks_request_establishes_the_session_without_a_probe_request() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on("POST", "/v1/oauth/tokens", Reply::token("fe-token"))
                .on("GET", "/v1/namespaces/db/tables/t", Reply::status(404)),
        );
        let endpoints =
            generation(&server, &[], Some(oauth2_identity("fe-control", "secret"))).await;

        assert!(
            !endpoints
                .table_exists(&table("t"))
                .await
                .expect("existence")
        );

        let requests = server.requests();
        let lines: Vec<_> = requests
            .iter()
            .map(|request| format!("{} {}", request.method, request.path()))
            .collect();
        assert_eq!(
            lines,
            [
                "POST /v1/oauth/tokens",
                "GET /v1/config",
                "GET /v1/namespaces/db/tables/t"
            ],
            "the probe is vetoed and only the session handshake precedes the read"
        );
        assert!(
            requests
                .iter()
                .all(|request| !request.target.contains("__novarocks_probe_"))
        );
        let existence = &requests[2];
        assert_eq!(existence.header("authorization"), Some("Bearer fe-token"));
        assert_eq!(existence.header(DELEGATION), None);
    }

    #[tokio::test]
    async fn table_existence_uses_head_only_when_advertised_and_never_delegates() {
        for advertised in [false, true] {
            let endpoints_list: &[&str] = if advertised { &[TABLE_HEAD] } else { &[] };
            let server = TestServer::start(
                Script::new()
                    .on(
                        "GET",
                        "/v1/config",
                        Reply::config(&[], &[], advertised.then_some(endpoints_list)),
                    )
                    .on("HEAD", "/v1/namespaces/db/tables/t", Reply::status(204))
                    // A body no client could decode: existence must not read it.
                    .on(
                        "GET",
                        "/v1/namespaces/db/tables/t",
                        Reply::bytes(200, b"<html>not metadata</html>".to_vec()),
                    ),
            );
            let endpoints = generation(&server, &[], None).await;

            assert!(
                endpoints
                    .table_exists(&table("t"))
                    .await
                    .expect("existence")
            );

            let reads = non_session(server.requests());
            assert_eq!(reads.len(), 1, "{advertised}");
            assert_eq!(reads[0].method, if advertised { "HEAD" } else { "GET" });
            assert_eq!(reads[0].header(DELEGATION), None);
            assert_eq!(
                reads[0].header("authorization"),
                None,
                "no identity, no token"
            );
        }
    }

    #[tokio::test]
    async fn existence_of_a_table_whose_metadata_needs_kms_is_a_status_fact() {
        // A v3 table with `encryption.key-id` cannot become a table object
        // without a KMS; existence must not need one (L27).
        let mut response = load_response(false);
        response["metadata"] = table_metadata_json(&[("encryption.key-id", "kms-key")]);
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/t",
                    Reply::json(200, &response),
                ),
        );
        let endpoints = generation(&server, &[], None).await;

        assert!(
            endpoints
                .table_exists(&table("t"))
                .await
                .expect("existence")
        );
    }

    #[tokio::test]
    async fn delegated_loads_send_the_delegation_header_and_other_reads_do_not() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/t",
                    Reply::json(200, &load_response(true)),
                )
                .on(
                    "GET",
                    "/credentials",
                    Reply::json(
                        200,
                        &serde_json::json!({"storage-credentials": [{
                            "prefix": "s3://warehouse/db/t",
                            "config": {"s3.access-key-id": "AKIA-canary"},
                        }]}),
                    ),
                ),
        );
        let endpoints = generation(&server, &[], None).await;

        let (frozen, delegation) = endpoints.load_table(&table("t"), true).await.expect("load");
        assert_eq!(delegation.len(), 1);
        assert!(!format!("{frozen:?}{delegation:?}").contains("AKIA-canary"));
        endpoints
            .load_table(&table("t"), false)
            .await
            .expect("plain load");
        let credentials = endpoints
            .load_credentials(&format!("{}/credentials", server.url()))
            .await
            .expect("credentials");
        assert_eq!(credentials.len(), 1);
        let (uuid, acquired) = endpoints
            .load_table_credentials(&table("t"))
            .await
            .expect("acquisition");
        assert!(uuid.is_some());
        assert_eq!(acquired.len(), 1);

        let reads = non_session(server.requests());
        let delegated: Vec<_> = reads
            .iter()
            .map(|request| {
                (
                    request.path().to_string(),
                    request.header(DELEGATION).map(str::to_string),
                )
            })
            .collect();
        assert_eq!(
            delegated,
            [
                (
                    "/v1/namespaces/db/tables/t".to_string(),
                    Some("vended-credentials".to_string())
                ),
                ("/v1/namespaces/db/tables/t".to_string(), None),
                ("/credentials".to_string(), None),
                (
                    "/v1/namespaces/db/tables/t".to_string(),
                    Some("vended-credentials".to_string())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn listing_sends_one_page_with_page_size_and_never_a_parent() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces",
                    Reply::json(200, &serde_json::json!({"namespaces": [["db"]]})),
                )
                .on(
                    "GET",
                    "/v1/namespaces/db/tables",
                    Reply::json(200, &serde_json::json!({"identifiers": []})),
                )
                .on(
                    "GET",
                    "/v1/namespaces/db/views",
                    Reply::json(200, &serde_json::json!({"identifiers": []})),
                ),
        );
        let endpoints = generation(&server, &[], None).await;
        let db = NamespaceIdent::new("db".to_string());

        endpoints
            .list_namespaces_page(Some("a b&c"), 10)
            .await
            .expect("namespaces");
        endpoints
            .list_tables_page(&db, None, 7)
            .await
            .expect("tables");
        endpoints
            .list_views_page(&db, Some("t1"), 3)
            .await
            .expect("views");

        let queries: Vec<_> = non_session(server.requests())
            .iter()
            .map(|request| request.query().map(str::to_string))
            .collect();
        assert_eq!(
            queries,
            [
                Some("pageSize=10&pageToken=a+b%26c".to_string()),
                Some("pageSize=7".to_string()),
                Some("pageSize=3&pageToken=t1".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn read_error_classes_stay_as_today_and_view_404_is_not_found() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on("GET", "/v1/namespaces/db/views/v", Reply::status(404))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/missing",
                    Reply::status(404),
                )
                .on("GET", "/v1/namespaces/db/tables/t", Reply::status(503))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/moved",
                    Reply::redirect(307, "/elsewhere"),
                ),
        );
        let endpoints = generation(&server, &[], None).await;

        let view = endpoints
            .load_view(&table("v"))
            .await
            .expect_err("missing view");
        assert_eq!(view.kind(), ConnectorErrorKind::NotFound);
        let missing = endpoints
            .load_table(&table("missing"), true)
            .await
            .expect_err("missing");
        assert_eq!(missing.kind(), ConnectorErrorKind::NotFound);
        let unavailable = endpoints
            .load_table(&table("t"), true)
            .await
            .expect_err("503");
        assert_eq!(unavailable.kind(), ConnectorErrorKind::Unavailable);
        assert!(!unavailable.retryable_before_progress());
        let moved = endpoints
            .load_table(&table("moved"), true)
            .await
            .expect_err("307");
        assert_eq!(moved.kind(), ConnectorErrorKind::Unavailable);
        assert_eq!(
            server.count("GET", "/elsewhere"),
            0,
            "redirects are never followed"
        );
    }

    #[tokio::test]
    async fn renewal_request_failures_are_retryable_before_progress() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on("GET", "/credentials", Reply::status(401))
                .on("GET", "/v1/namespaces/db/tables/t", Reply::status(401)),
        );
        let endpoints = generation(&server, &[], None).await;

        let credentials = endpoints
            .load_credentials(&format!("{}/credentials", server.url()))
            .await
            .expect_err("401");
        assert_eq!(credentials.kind(), ConnectorErrorKind::Unavailable);
        assert!(credentials.retryable_before_progress());
        let acquisition = endpoints
            .load_table_credentials(&table("t"))
            .await
            .expect_err("401");
        assert!(acquisition.retryable_before_progress());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_session_serves_namespace_and_table_reads_after_config_changes() {
        let server = TestServer::start(
            Script::new()
                .on_sequence(
                    "GET",
                    "/v1/config",
                    vec![
                        Reply::config(&[("prefix", "p1")], &[], None),
                        Reply::config(&[("prefix", "p2")], &[], None),
                    ],
                )
                .on(
                    "GET",
                    "/v1/p1/namespaces/db",
                    Reply::json(
                        200,
                        &serde_json::json!({"namespace": ["db"], "properties": {}}),
                    ),
                )
                .on("GET", "/v1/p1/namespaces/db/tables/t", Reply::status(404))
                .on(
                    "GET",
                    "/v1/p1/namespaces/db/tables",
                    Reply::json(200, &serde_json::json!({"identifiers": []})),
                ),
        );
        let endpoints = generation(&server, &[], None).await;
        let db = NamespaceIdent::new("db".to_string());

        // Cold session: namespace and table reads race through the same gate.
        let upstream_read = async { endpoints.namespace_exists(&db).await.expect("namespace") };
        let probe_table = table("t");
        let (namespace, exists) = tokio::join!(upstream_read, endpoints.table_exists(&probe_table));
        assert!(namespace);
        assert!(!exists.expect("existence"));
        // Later reads keep the first binding.
        endpoints
            .list_tables_page(&db, None, 5)
            .await
            .expect("tables");
        assert!(endpoints.namespace_exists(&db).await.expect("namespace"));

        assert_eq!(server.count("GET", "/v1/config"), 1);
        let paths: Vec<_> = non_session(server.requests())
            .iter()
            .map(|request| request.path().to_string())
            .collect();
        assert!(!paths.is_empty());
        assert!(
            paths.iter().all(|path| path.starts_with("/v1/p1/")),
            "{paths:?}"
        );
    }

    #[tokio::test]
    async fn novarocks_and_upstream_loads_are_the_same_request_but_for_delegation() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on("POST", "/v1/oauth/tokens", Reply::token("fe-token"))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/t",
                    Reply::json(200, &load_response(false)),
                ),
        );
        let endpoints = generation(
            &server,
            &[("header.x-tenant", "t1")],
            Some(oauth2_identity("fe-control", "secret")),
        )
        .await;

        endpoints
            .load_table(&table("t"), true)
            .await
            .expect("NovaRocks load");
        // The independent SDK comparator is dev-only and owns its own
        // handshake. It cannot participate in the production session.
        use crate::iceberg::CatalogBuilder;
        let upstream = crate::iceberg_catalog_rest::RestCatalogBuilder::default()
            .with_client(endpoints.owner().http().reqwest_client().clone())
            .load(
                "independent-upstream",
                HashMap::from([
                    ("uri".to_string(), server.url()),
                    ("header.x-tenant".to_string(), "t1".to_string()),
                    ("token".to_string(), "fe-token".to_string()),
                ]),
            )
            .await
            .unwrap();
        let _ = crate::iceberg::Catalog::load_table(&upstream, &table("t")).await;

        let loads = server.requests_to("GET", "/v1/namespaces/db/tables/t");
        assert_eq!(loads.len(), 2);
        let mut novarocks = loads[0].header_map();
        let upstream = loads[1].header_map();
        assert_eq!(
            novarocks.remove(DELEGATION).as_deref(),
            Some("vended-credentials")
        );
        for name in [
            "authorization",
            "x-tenant",
            "content-type",
            "x-client-version",
            "user-agent",
        ] {
            assert_eq!(novarocks.get(name), upstream.get(name), "{name}");
        }
        assert_eq!(loads[0].target, loads[1].target);
        assert_eq!(server.count("POST", "/v1/oauth/tokens"), 1);
    }
    #[tokio::test]
    async fn conditional_load_preserves_raw_etag_and_304_has_no_document_or_credentials() {
        let response = load_response(true);
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on_sequence(
                    "GET",
                    "/v1/namespaces/db/tables/t",
                    vec![
                        Reply::json(200, &response).with_header("etag", "W/\"opaque-value\""),
                        Reply::bytes(304, b"invalid-json-with-credential-canary".to_vec()),
                        Reply::status(304),
                    ],
                ),
        );
        let endpoints = generation(&server, &[], None).await;
        let validator = match endpoints
            .load_table_conditional(&table("t"), true, None)
            .await
            .unwrap()
        {
            ConditionalTableLoad::Complete {
                etag: Some(etag), ..
            } => etag,
            _ => panic!("complete load retains the server ETag"),
        };
        assert_eq!(validator, "W/\"opaque-value\"");
        assert!(matches!(
            endpoints
                .load_table_conditional(&table("t"), false, Some(&validator))
                .await
                .unwrap(),
            ConditionalTableLoad::NotModified
        ));
        let loads = server.requests_to("GET", "/v1/namespaces/db/tables/t");
        assert_eq!(loads.len(), 2);
        assert_eq!(loads[0].header("if-none-match"), None);
        assert_eq!(loads[1].header("if-none-match"), Some("W/\"opaque-value\""));
        assert_eq!(loads[1].header("x-iceberg-access-delegation"), None);
        assert_eq!(server.count("GET", "/v1/config"), 1);
        assert!(
            endpoints
                .load_table(&table("t"), false)
                .await
                .unwrap_err()
                .message()
                .contains("304 without a conditional")
        );
    }

    #[tokio::test]
    async fn complete_load_without_etag_is_usable_and_does_not_invent_a_validator() {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces/db/tables/t",
                    Reply::json(200, &load_response(false)),
                ),
        );
        let endpoints = generation(&server, &[], None).await;
        assert!(matches!(
            endpoints
                .load_table_conditional(&table("t"), false, None)
                .await
                .unwrap(),
            ConditionalTableLoad::Complete { etag: None, .. }
        ));
    }

    #[tokio::test]
    async fn invalid_optional_capability_keeps_reads_available_and_never_sends_a_key() {
        use super::super::configuration::IdempotencyCapability;
        for lifetime in [
            serde_json::Value::Null,
            serde_json::json!(7),
            serde_json::json!("PT0S"),
            serde_json::json!("invalid"),
        ] {
            let config = serde_json::json!({"defaults":{}, "overrides":{}, "idempotency-key-lifetime": lifetime});
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::json(200, &config))
                    .on("GET", "/v1/namespaces/db/tables/t", Reply::status(404)),
            );
            let endpoints = generation(&server, &[], None).await;
            assert!(!endpoints.table_exists(&table("t")).await.unwrap());
            assert!(matches!(
                endpoints.binding().await.unwrap().idempotency_capability(),
                IdempotencyCapability::Invalid { .. }
            ));
            assert_eq!(server.count("GET", "/v1/config"), 1);
            assert!(
                server
                    .requests()
                    .iter()
                    .all(|request| request.header("idempotency-key").is_none())
            );
        }
    }
}
