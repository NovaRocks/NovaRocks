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

//! The per-role auth owner (L16): the only component that sets
//! `Authorization` on a REST request of its role.
//!
//! The frontend owner authenticates the one configuration handshake and all
//! later requests of its generation. The backend owner authorizes only its
//! routed credential requests and never establishes a catalog session. Token
//! exchange is shared per attempt and successful tokens are never refreshed.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use futures::FutureExt;
use novarocks_fs::SecretValue;
use reqwest::Method;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};

use super::attempt::{AttemptCell, AttemptFailure, BindingStepError};
use super::binding::{is_authentication_header, is_protocol_header, protocol_headers};
use super::evidence::{BindingFailure, DispatchStage};
use super::http::{RequestContext, RestHttpClient, RestRequest};
use crate::access_binding::IcebergRestAuthMaterial;

const PROP_URI: &str = "uri";
const PROP_OAUTH2_SERVER_URI: &str = "oauth2-server-uri";
const PROP_SCOPE: &str = "scope";
const PROP_AUDIENCE: &str = "audience";
const PROP_RESOURCE: &str = "resource";
const DEFAULT_SCOPE: &str = "catalog";
const HEADER_PROPERTY_PREFIX: &str = "header.";
/// Properties that carry or select authentication; the persistent
/// definition never holds them (L5).
const AUTHENTICATION_PROPERTIES: [&str; 3] = ["token", "credential", "rest.auth.type"];

/// The persistent, non-secret REST catalog definition the owner derives every
/// request from: the configured `uri`, its public `header.*` and the OAuth2
/// token request. Server-sent configuration never changes these (L16).
#[derive(Clone)]
pub(crate) struct PersistentRestConfig {
    uri: String,
    headers: Vec<(HeaderName, HeaderValue)>,
    token_endpoint: String,
    scope: String,
    audience: Option<String>,
    resource: Option<String>,
}

impl fmt::Debug for PersistentRestConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistentRestConfig")
            .field(
                "header_names",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl PersistentRestConfig {
    /// Projects the persistent catalog properties (as handed to upstream:
    /// `uri` included, `type` excluded). Fails fast on authentication
    /// material, authentication-class or protocol-class `header.*` (L5, L26)
    /// and on an empty explicit token endpoint.
    pub(crate) fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let uri = properties
            .get(PROP_URI)
            .filter(|uri| !uri.is_empty())
            .cloned()
            .ok_or_else(|| "REST catalog definition is missing `uri`".to_string())?;
        for key in AUTHENTICATION_PROPERTIES {
            if properties.contains_key(key) {
                return Err(format!(
                    "REST catalog property `{key}` is not supported: authentication comes only \
                     from the CatalogControl or DataCredentialVending identity"
                ));
            }
        }
        let mut headers = Vec::new();
        for (key, value) in properties {
            let Some(name) = key.strip_prefix(HEADER_PROPERTY_PREFIX) else {
                continue;
            };
            if is_authentication_header(name) {
                return Err(format!(
                    "REST catalog header `{name}` is not supported: authentication comes only \
                     from the CatalogControl or DataCredentialVending identity"
                ));
            }
            if is_protocol_header(name) {
                return Err(format!(
                    "REST catalog header `{name}` is set by the client and cannot be configured"
                ));
            }
            let name = HeaderName::from_str(name)
                .map_err(|_| format!("REST catalog header name `{name}` is invalid"))?;
            let value = HeaderValue::from_str(value)
                .map_err(|_| format!("REST catalog header `{name}` has an invalid value"))?;
            if headers.iter().any(|(existing, _)| *existing == name) {
                return Err(format!(
                    "REST catalog header `{name}` is configured more than once"
                ));
            }
            headers.push((name, value));
        }
        headers.sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
        let token_endpoint = match properties.get(PROP_OAUTH2_SERVER_URI) {
            Some(endpoint) if endpoint.is_empty() => {
                return Err("REST catalog property `oauth2-server-uri` is empty".to_string());
            }
            Some(endpoint) => endpoint.clone(),
            None => [uri.as_str(), "v1", "oauth", "tokens"].join("/"),
        };
        Ok(Self {
            uri,
            headers,
            token_endpoint,
            scope: properties
                .get(PROP_SCOPE)
                .cloned()
                .unwrap_or_else(|| DEFAULT_SCOPE.to_string()),
            audience: properties.get(PROP_AUDIENCE).cloned(),
            resource: properties.get(PROP_RESOURCE).cloned(),
        })
    }

    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    /// The persistent public headers, sorted by name.
    pub(crate) fn headers(&self) -> &[(HeaderName, HeaderValue)] {
        &self.headers
    }

    /// The persistent value of an OAuth2 token-request property, used to
    /// report which server-sent values were ignored.
    pub(crate) fn token_property(&self, key: &str) -> Option<&str> {
        match key {
            PROP_OAUTH2_SERVER_URI => Some(&self.token_endpoint),
            PROP_SCOPE => Some(&self.scope),
            PROP_AUDIENCE => self.audience.as_deref(),
            PROP_RESOURCE => self.resource.as_deref(),
            _ => None,
        }
    }

    /// Headers of the token request: the protocol headers upstream sends
    /// with a form content type, plus the persistent public `header.*`.
    fn token_request_headers(&self) -> HeaderMap {
        let mut headers = protocol_headers();
        for (name, value) in &self.headers {
            headers.insert(name.clone(), value.clone());
        }
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        headers
    }
}

/// The OAuth2 `client_credentials` token response; only `access_token` is
/// used. Deliberately no `Debug`.
#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    #[allow(dead_code)]
    token_type: String,
    #[allow(dead_code)]
    expires_in: Option<u64>,
    #[allow(dead_code)]
    issued_token_type: Option<String>,
}

/// Exchanges the role's client credential at the persistent token endpoint,
/// with the same form and headers upstream's `OAuth2Manager` sends.
async fn exchange_token(
    http: RestHttpClient,
    persistent: Arc<PersistentRestConfig>,
    client_id: String,
    client_secret: SecretValue,
    context: RequestContext,
) -> Result<HeaderValue, BindingStepError> {
    let mut form = vec![
        ("grant_type", "client_credentials"),
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.expose_secret()),
        ("scope", persistent.scope.as_str()),
    ];
    if let Some(audience) = &persistent.audience {
        form.push(("audience", audience));
    }
    if let Some(resource) = &persistent.resource {
        form.push(("resource", resource));
    }
    let request = RestRequest::new(
        Method::POST,
        persistent.token_endpoint.clone(),
        persistent.token_request_headers(),
    )
    .with_form_body(form);
    let (stage, body) = http
        .send_once(Ok(request), &[200], &context)
        .await
        .map_err(|original| {
            BindingStepError::from_original(BindingFailure::TokenUnavailable, original)
        })?;
    match (stage, body) {
        (DispatchStage::Status { .. }, Some(body)) => {
            let bytes = body
                .read_to_end()
                .await
                .map_err(|_| BindingFailure::TokenUnavailable)?;
            let response: TokenResponse =
                serde_json::from_slice(&bytes).map_err(|_| BindingFailure::TokenUnavailable)?;
            bearer_value(response.access_token.as_str())
                .ok_or_else(|| BindingFailure::TokenUnavailable.into())
        }
        (DispatchStage::Status { code, .. }, None) if (400..500).contains(&code) => {
            Err(BindingFailure::TokenRejected.into())
        }
        _ => Err(BindingFailure::TokenUnavailable.into()),
    }
}

/// The role identity as the owner uses it.
enum Credential {
    /// No identity: requests carry no `Authorization`.
    Anonymous,
    /// A configured bearer token, already a sensitive header value.
    Bearer(HeaderValue),
    /// An OAuth2 client credential exchanged at the token endpoint.
    ClientCredentials {
        client_id: String,
        client_secret: SecretValue,
    },
}

/// The auth owner of one role.
pub(crate) struct RestAuthOwner {
    credential: Credential,
    persistent: Arc<PersistentRestConfig>,
    http: RestHttpClient,
    /// The exchanged `Authorization` value: one exchange per attempt, kept
    /// after the first success and never refreshed.
    token: AttemptCell<HeaderValue, AttemptFailure>,
    /// Frontend only: state of the current establishment attempt.
    frontend: bool,
    activity: Option<Arc<crate::activity::GenerationActivity>>,
}

impl fmt::Debug for RestAuthOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let identity = match &self.credential {
            Credential::Anonymous => "none",
            Credential::Bearer(_) => "bearer",
            Credential::ClientCredentials { .. } => "oauth2",
        };
        let role = if self.frontend { "frontend" } else { "backend" };
        formatter
            .debug_struct("RestAuthOwner")
            .field("role", &role)
            .field("identity", &identity)
            .field("persistent", &self.persistent)
            .finish_non_exhaustive()
    }
}

impl RestAuthOwner {
    /// The frontend role's sole authentication authority.
    pub(crate) fn frontend(
        identity: Option<IcebergRestAuthMaterial>,
        persistent: PersistentRestConfig,
        http: RestHttpClient,
    ) -> Result<Arc<Self>, String> {
        Self::frontend_observed(identity, persistent, http, None)
    }
    pub(crate) fn frontend_observed(
        identity: Option<IcebergRestAuthMaterial>,
        persistent: PersistentRestConfig,
        http: RestHttpClient,
        activity: Option<Arc<crate::activity::GenerationActivity>>,
    ) -> Result<Arc<Self>, String> {
        let mut owner = Self::new(identity, persistent, http, true)?;
        owner.activity = activity;
        Ok(Arc::new(owner))
    }
    pub(crate) fn activity(&self) -> Option<&Arc<crate::activity::GenerationActivity>> {
        self.activity.as_ref()
    }

    /// The backend owner; it never builds an upstream client.
    pub(crate) fn backend(
        identity: Option<IcebergRestAuthMaterial>,
        persistent: PersistentRestConfig,
        http: RestHttpClient,
    ) -> Result<Arc<Self>, String> {
        Ok(Arc::new(Self::new(identity, persistent, http, false)?))
    }

    fn new(
        identity: Option<IcebergRestAuthMaterial>,
        persistent: PersistentRestConfig,
        http: RestHttpClient,
        frontend: bool,
    ) -> Result<Self, String> {
        let credential = match identity {
            None => Credential::Anonymous,
            Some(IcebergRestAuthMaterial::Bearer { token }) => Credential::Bearer(
                bearer_value(token.expose_secret())
                    .ok_or_else(|| "REST bearer token is not a valid header value".to_string())?,
            ),
            Some(IcebergRestAuthMaterial::Oauth2 {
                client_id,
                client_secret,
            }) => Credential::ClientCredentials {
                client_id,
                client_secret,
            },
        };
        Ok(Self {
            credential,
            persistent: Arc::new(persistent),
            http,
            token: AttemptCell::default(),
            frontend,
            activity: None,
        })
    }

    pub(crate) fn is_anonymous(&self) -> bool {
        matches!(self.credential, Credential::Anonymous)
    }

    pub(crate) fn http(&self) -> &RestHttpClient {
        &self.http
    }

    pub(crate) fn persistent(&self) -> &PersistentRestConfig {
        &self.persistent
    }

    /// The role's `Authorization` value: `None` without an identity, the
    /// configured token for a bearer identity, otherwise the exchanged token.
    /// Concurrent callers share one exchange; a failure is not kept.
    pub(crate) async fn authorization(&self) -> Result<Option<HeaderValue>, AttemptFailure> {
        self.authorization_in(&RequestContext::for_read()).await
    }
    pub(crate) async fn authorization_in(
        &self,
        context: &RequestContext,
    ) -> Result<Option<HeaderValue>, AttemptFailure> {
        let (client_id, client_secret) = match &self.credential {
            Credential::Anonymous => return Ok(None),
            Credential::Bearer(value) => return Ok(Some(value.clone())),
            Credential::ClientCredentials {
                client_id,
                client_secret,
            } => (client_id, client_secret),
        };
        self.token
            .get_or_attempt(|attempt| {
                let http = self.http.clone();
                let persistent = Arc::clone(&self.persistent);
                let client_id = client_id.clone();
                let client_secret = client_secret.clone();
                let activity = self.activity.clone();
                let context=context.clone();
                async move {
                    let _initializer = activity.as_ref().map(|owner| {
                        owner.enter(
                            novarocks_spi::connector::CatalogActivityPhase::AuthorizationInitializer,
                        )
                    });
                    exchange_token(http, persistent, client_id, client_secret, context)
                        .await
                        .map_err(|cause| AttemptFailure::from_cause(attempt, cause))
                }
                .boxed()
            })
            .await
            .map(Some)
    }

    /// Writes `Authorization` for a NovaRocks request of this role.
    pub(crate) async fn authorize(&self, headers: &mut HeaderMap) -> Result<(), AttemptFailure> {
        self.authorize_in(headers, &RequestContext::for_read())
            .await
    }
    pub(crate) async fn authorize_in(
        &self,
        headers: &mut HeaderMap,
        context: &RequestContext,
    ) -> Result<(), AttemptFailure> {
        if let Some(value) = self.authorization_in(context).await? {
            headers.insert(AUTHORIZATION, value);
        }
        Ok(())
    }
}

fn bearer_value(token: &str) -> Option<HeaderValue> {
    let mut value = HeaderValue::from_str(&format!("Bearer {token}")).ok()?;
    value.set_sensitive(true);
    Some(value)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
    use crate::rest_protocol::test_server::{
        Reply, Script, TestServer, bearer_identity, oauth2_identity,
    };

    fn client() -> RestHttpClient {
        RestHttpClient::with_timeouts(Duration::from_secs(2), Duration::from_secs(5)).unwrap()
    }

    fn properties(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn backend(
        server: &TestServer,
        extra: &[(&str, &str)],
        identity: Option<IcebergRestAuthMaterial>,
    ) -> Arc<RestAuthOwner> {
        let mut definition = properties(extra);
        definition.insert("uri".to_string(), server.url());
        let persistent = PersistentRestConfig::from_properties(&definition).unwrap();
        RestAuthOwner::backend(identity, persistent, client()).unwrap()
    }

    #[tokio::test]
    async fn the_token_request_equals_upstream_oauth2_manager() {
        let server = TestServer::start(
            Script::new()
                .on("POST", "/v1/oauth/tokens", Reply::token("exchanged-token"))
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on("GET", "/v1/namespaces/db", Reply::status(404)),
        );
        let shared = [
            ("scope", "catalog:read"),
            ("audience", "the-audience"),
            ("resource", "urn:the-resource"),
            ("header.x-tenant", "tenant-1"),
        ];
        // Upstream's own OAuth2Manager, selected from the same configuration.
        let mut upstream_properties = properties(&shared);
        upstream_properties.insert("uri".to_string(), server.url());
        upstream_properties.insert(
            "credential".to_string(),
            "client-id:client-secret".to_string(),
        );
        let upstream = crate::iceberg_catalog_rest::RestCatalogBuilder::default()
            .with_client(client().reqwest_client().clone())
            .load("rest", upstream_properties)
            .await
            .unwrap();
        let namespace = NamespaceIdent::new("db".to_string());
        assert!(!upstream.namespace_exists(&namespace).await.unwrap());
        let upstream_request = server.requests_to("POST", "/v1/oauth/tokens").remove(0);

        let owner = backend(
            &server,
            &shared,
            Some(oauth2_identity("client-id", "client-secret")),
        );
        let mut headers = HeaderMap::new();
        owner.authorize(&mut headers).await.unwrap();
        assert_eq!(headers[AUTHORIZATION], "Bearer exchanged-token");
        let requests = server.requests_to("POST", "/v1/oauth/tokens");
        assert_eq!(requests.len(), 2);
        let ours = &requests[1];

        assert_eq!(ours.target, upstream_request.target);
        assert_eq!(ours.header_map(), upstream_request.header_map());
        assert_eq!(ours.form(), upstream_request.form());
        assert_eq!(
            ours.form(),
            vec![
                ("audience".to_string(), "the-audience".to_string()),
                ("client_id".to_string(), "client-id".to_string()),
                ("client_secret".to_string(), "client-secret".to_string()),
                ("grant_type".to_string(), "client_credentials".to_string()),
                ("resource".to_string(), "urn:the-resource".to_string()),
                ("scope".to_string(), "catalog:read".to_string()),
            ]
        );
        assert_eq!(ours.header("authorization"), None);
        assert_eq!(ours.header("x-tenant"), Some("tenant-1"));
    }

    #[tokio::test]
    async fn an_explicit_token_endpoint_is_used_as_configured() {
        let server =
            TestServer::start(Script::new().on("POST", "/idp/token", Reply::token("idp-token")));
        let endpoint = format!("{}/idp/token", server.url());
        let owner = backend(
            &server,
            &[("oauth2-server-uri", &endpoint)],
            Some(oauth2_identity("id", "secret")),
        );
        assert_eq!(
            owner.authorization().await.unwrap().unwrap(),
            "Bearer idp-token"
        );
        assert_eq!(server.count("POST", "/idp/token"), 1);
        let form = server.requests_to("POST", "/idp/token")[0].form();
        assert!(form.contains(&("scope".to_string(), "catalog".to_string())));
    }

    #[tokio::test]
    async fn concurrent_backend_acquisitions_share_one_exchange_and_its_failure() {
        let server = TestServer::start(Script::new().on_sequence(
            "POST",
            "/v1/oauth/tokens",
            vec![
                Reply::status(500).with_delay(Duration::from_millis(200)),
                Reply::token("second-token"),
            ],
        ));
        let owner = backend(&server, &[], Some(oauth2_identity("id", "secret")));
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let owner = Arc::clone(&owner);
            tasks.push(tokio::spawn(async move {
                let mut headers = HeaderMap::new();
                owner.authorize(&mut headers).await
            }));
        }
        let mut failures = Vec::new();
        for task in tasks {
            failures.push(task.await.unwrap().unwrap_err());
        }
        assert!(failures.iter().all(|failure| *failure == failures[0]));
        assert_eq!(failures[0].step, BindingFailure::TokenUnavailable);
        assert_eq!(server.count("POST", "/v1/oauth/tokens"), 1);

        // Only a later call exchanges again; success is then kept.
        for _ in 0..3 {
            let mut headers = HeaderMap::new();
            owner.authorize(&mut headers).await.unwrap();
            assert_eq!(headers[AUTHORIZATION], "Bearer second-token");
        }
        assert_eq!(server.count("POST", "/v1/oauth/tokens"), 2);
    }

    #[tokio::test]
    async fn token_endpoint_failures_are_classified_by_status_only() {
        for (reply, expected) in [
            (Reply::status(401), BindingFailure::TokenRejected),
            (Reply::status(400), BindingFailure::TokenRejected),
            (Reply::status(503), BindingFailure::TokenUnavailable),
            (Reply::status(201), BindingFailure::TokenUnavailable),
            (
                Reply::redirect(307, "http://127.0.0.1:9/elsewhere"),
                BindingFailure::TokenUnavailable,
            ),
            (
                Reply::bytes(200, b"not json".to_vec()),
                BindingFailure::TokenUnavailable,
            ),
            (Reply::CloseAfterRequest, BindingFailure::TokenUnavailable),
        ] {
            let server = TestServer::start(Script::new().on("POST", "/v1/oauth/tokens", reply));
            let owner = backend(&server, &[], Some(oauth2_identity("id", "secret")));
            assert_eq!(owner.authorization().await.unwrap_err().step, expected);
        }
    }

    #[tokio::test]
    async fn bearer_and_anonymous_identities_exchange_nothing() {
        let server = TestServer::start(Script::new());
        let bearer = backend(&server, &[], Some(bearer_identity("configured-token")));
        let mut headers = HeaderMap::new();
        bearer.authorize(&mut headers).await.unwrap();
        assert_eq!(headers[AUTHORIZATION], "Bearer configured-token");
        assert!(headers[AUTHORIZATION].is_sensitive());

        let anonymous = backend(&server, &[], None);
        let mut headers = HeaderMap::new();
        anonymous.authorize(&mut headers).await.unwrap();
        assert!(headers.is_empty());
        assert!(server.requests().is_empty());
    }

    #[test]
    fn the_persistent_definition_rejects_authentication_and_protocol_headers() {
        for (key, value) in [
            ("header.Authorization", "Bearer x"),
            ("header.x-api-token", "x"),
            ("header.User-Agent", "custom"),
            ("header.x-iceberg-access-delegation", "vended-credentials"),
            ("rest.auth.type", "oauth2"),
            ("token", "x"),
            ("credential", "id:secret"),
            ("oauth2-server-uri", ""),
        ] {
            let definition = properties(&[("uri", "http://catalog"), (key, value)]);
            assert!(
                PersistentRestConfig::from_properties(&definition).is_err(),
                "{key}"
            );
        }
        assert!(PersistentRestConfig::from_properties(&properties(&[])).is_err());
        let definition = properties(&[("uri", "http://catalog/api"), ("header.x-tenant", "t")]);
        let persistent = PersistentRestConfig::from_properties(&definition).unwrap();
        assert_eq!(
            persistent.token_property("oauth2-server-uri"),
            Some("http://catalog/api/v1/oauth/tokens")
        );
        assert_eq!(persistent.token_property("scope"), Some("catalog"));
    }

    #[test]
    fn debug_output_carries_no_principal_or_secret() {
        let definition = properties(&[("uri", "http://catalog"), ("header.x-tenant", "t-canary")]);
        let persistent = PersistentRestConfig::from_properties(&definition).unwrap();
        let owner = RestAuthOwner::frontend(
            Some(oauth2_identity("principal-canary", "secret-canary")),
            persistent.clone(),
            client(),
        )
        .unwrap();
        let bearer =
            RestAuthOwner::backend(Some(bearer_identity("token-canary")), persistent, client())
                .unwrap();
        for debug in [format!("{owner:?}"), format!("{bearer:?}")] {
            for canary in [
                "principal-canary",
                "secret-canary",
                "token-canary",
                "t-canary",
            ] {
                assert!(!debug.contains(canary), "{debug}");
            }
        }
    }

    #[test]
    fn an_unusable_bearer_token_fails_construction() {
        let definition = properties(&[("uri", "http://catalog")]);
        let persistent = PersistentRestConfig::from_properties(&definition).unwrap();
        assert!(
            RestAuthOwner::backend(Some(bearer_identity("bad\ntoken")), persistent, client())
                .is_err()
        );
    }
}
