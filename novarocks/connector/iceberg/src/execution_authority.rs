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

//! The execution node's own acquisition path for vended data credentials.
//!
//! CAD-1 D1 puts the catalog identity on the node that consumes the material:
//! an execution node authenticates to the REST control plane as itself and
//! exchanges that identity for storage credentials, instead of waiting for a
//! coordinator to push material it cannot renew.
//!
//! The node never opens a catalog session of its own (D15): it sends exactly
//! the request the coordinator froze into the routing context, with only its
//! own `Authorization` added. The acquisition budget, retry shape and failure
//! classification are deliberately *not* duplicated: they belong to
//! [`crate::authority_source`], which wraps this refresher.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorVendedS3CredentialLeaseRefresher,
    CredentialRoutingContext, VendedS3CredentialLeaseRefresh, VendedS3CredentialRefreshCallPolicy,
};

use crate::access_binding::IcebergRestAuthMaterial;
use crate::catalog_config::{IcebergCatalogKind, parse_catalog_configuration};
use crate::loaded_table::{IcebergAccessDelegation, parse_rest_delegation};
use crate::resources::IcebergCatalogRuntime;
use crate::rest_protocol::auth::{PersistentRestConfig, RestAuthOwner};

/// Which of the two advertised shapes this node acquires along.
///
/// A consumer selects one and never probes the other after a failure: the
/// catalog stated which path it serves, and trying the other would assert a
/// capability the deployment may not have (CAD-1 D2). Both carry the routing
/// context the coordinator froze.
#[derive(Clone, Debug)]
pub(crate) enum ExecutionNodeAcquisitionPath {
    /// The scope's credentials are served from this address.
    CredentialsEndpoint { routing: CredentialRoutingContext },
    /// The catalog vends only inside a load-table response for this table.
    LoadTableDelegation {
        routing: CredentialRoutingContext,
        table: crate::iceberg::TableIdent,
        expected_table_uuid: uuid::Uuid,
    },
}

impl ExecutionNodeAcquisitionPath {
    /// Project one announced path into the provider's own vocabulary.
    ///
    /// Fallible on purpose, and resolved before the authority is formed: an
    /// announcement this node cannot parse is a disagreement about the table,
    /// and an acquisition that proceeded with a degraded identity would have
    /// nothing meaningful left to compare a response against.
    pub(crate) fn project(
        announced: &novarocks_spi::connector::CredentialRenewalPath,
    ) -> Result<Self, ConnectorError> {
        let routing = announced.routing().clone();
        // The coordinator validated the address; an address this node would
        // not send is refused rather than repaired.
        crate::rest_protocol::routing::canonical_route_url(routing.url()).and_then(
            |canonical| {
                (canonical == routing.url()).then_some(()).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "announced routing address is not in canonical form",
                    )
                })
            },
        )?;
        match announced {
            novarocks_spi::connector::CredentialRenewalPath::CredentialsEndpoint(_) => {
                Ok(Self::CredentialsEndpoint { routing })
            }
            novarocks_spi::connector::CredentialRenewalPath::LoadTableDelegation {
                table: delegation,
                ..
            } => {
                let namespace = crate::iceberg::NamespaceIdent::from_vec(
                    delegation
                        .namespace()
                        .iter()
                        .map(|level| level.as_ref().to_string())
                        .collect(),
                )
                .map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("announced load-table namespace is not usable: {error}"),
                    )
                })?;
                let expected_table_uuid =
                    uuid::Uuid::parse_str(delegation.table_uuid()).map_err(|error| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            format!("announced load-table uuid is not a uuid: {error}"),
                        )
                    })?;
                Ok(Self::LoadTableDelegation {
                    routing,
                    table: crate::iceberg::TableIdent::new(
                        namespace,
                        delegation.table().to_string(),
                    ),
                    expected_table_uuid,
                })
            }
        }
    }

    pub(crate) fn routing(&self) -> &CredentialRoutingContext {
        match self {
            Self::CredentialsEndpoint { routing } | Self::LoadTableDelegation { routing, .. } => {
                routing
            }
        }
    }
}

/// Acquires data credentials along one advertised path using this process's
/// own catalog principal.
///
/// The auth owner is built once and retained: a process-lived authority that
/// rebuilt it on every renewal would also repeat its OAuth2 token exchange,
/// turning one storage refresh into two remote round trips.
pub(crate) struct ExecutionNodeCredentialsEndpointRefresher {
    catalog_name: String,
    non_secret_properties: Vec<(String, String)>,
    principal: IcebergRestAuthMaterial,
    runtime: IcebergCatalogRuntime,
    path: ExecutionNodeAcquisitionPath,
    owner: Mutex<Option<Arc<RestAuthOwner>>>,
}

impl std::fmt::Debug for ExecutionNodeCredentialsEndpointRefresher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut rendered = formatter.debug_struct("ExecutionNodeCredentialsEndpointRefresher");
        rendered.field("catalog_name", &self.catalog_name);
        match &self.path {
            ExecutionNodeAcquisitionPath::CredentialsEndpoint { routing } => {
                rendered.field("routing", routing);
            }
            ExecutionNodeAcquisitionPath::LoadTableDelegation { routing, table, .. } => {
                rendered.field("routing", routing);
                rendered.field("load_table", &table.name());
            }
        }
        rendered.finish_non_exhaustive()
    }
}

impl ExecutionNodeCredentialsEndpointRefresher {
    pub(crate) fn new(
        catalog_name: String,
        non_secret_properties: Vec<(String, String)>,
        principal: IcebergRestAuthMaterial,
        runtime: IcebergCatalogRuntime,
        path: ExecutionNodeAcquisitionPath,
    ) -> Self {
        Self {
            catalog_name,
            non_secret_properties,
            principal,
            runtime,
            path,
            owner: Mutex::new(None),
        }
    }

    /// The persistent, non-secret catalog definition the token request is
    /// derived from (L16). The principal never comes from it.
    fn persistent_properties(&self) -> Result<HashMap<String, String>, ConnectorError> {
        let configuration = parse_catalog_configuration(
            &self.catalog_name,
            &self.non_secret_properties,
        )
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("parse Iceberg catalog definition for credential acquisition: {error}"),
            )
        })?;
        if configuration.kind != IcebergCatalogKind::Rest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "credential acquisition requires a REST Iceberg catalog definition",
            ));
        }
        let uri = configuration.rest_uri.clone().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "REST Iceberg catalog definition has no uri for credential acquisition",
            )
        })?;
        let mut properties = configuration
            .properties
            .iter()
            .filter(|(key, _)| key != "type")
            .cloned()
            .collect::<HashMap<_, _>>();
        properties.insert(
            crate::rest_protocol::wire::REST_CATALOG_PROP_URI.to_string(),
            uri,
        );
        Ok(properties)
    }

    fn owner(&self) -> Result<Arc<RestAuthOwner>, ConnectorError> {
        let mut slot = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(owner) = slot.as_ref() {
            return Ok(Arc::clone(owner));
        }
        let persistent = PersistentRestConfig::from_properties(&self.persistent_properties()?)
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))?;
        let http = crate::rest_protocol::http::RestHttpClient::new()
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))?;
        let owner = RestAuthOwner::backend(Some(self.principal.clone()), persistent, http)
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))?;
        *slot = Some(Arc::clone(&owner));
        Ok(owner)
    }
}

impl ConnectorVendedS3CredentialLeaseRefresher for ExecutionNodeCredentialsEndpointRefresher {
    fn refresh_vended_s3_credentials(
        &self,
        policy: VendedS3CredentialRefreshCallPolicy,
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError> {
        let owner = self.owner()?;
        let routing = self.path.routing().clone();
        let load_table = matches!(
            self.path,
            ExecutionNodeAcquisitionPath::LoadTableDelegation { .. }
        );
        let (observed_uuid, delegation) = crate::loaded_table::run_vended_renewal_with_policy(
            &self.runtime,
            policy,
            "Iceberg execution-node credential acquisition",
            move || {
                let owner = Arc::clone(&owner);
                let routing = routing.clone();
                async move {
                    crate::rest_protocol::endpoints::acquire_routed(&owner, &routing, load_table)
                        .await
                }
            },
        )?;
        self.project_refresh(observed_uuid, delegation)
    }

    fn refresh_vended_s3_credentials_async<'a>(
        &'a self,
        policy: VendedS3CredentialRefreshCallPolicy,
        request: novarocks_spi::connector::ConnectorRequestContext,
    ) -> futures::future::BoxFuture<'a, Result<VendedS3CredentialLeaseRefresh, ConnectorError>>
    {
        Box::pin(async move {
            let owner = self.owner()?;
            let routing = self.path.routing().clone();
            let load_table = matches!(
                self.path,
                ExecutionNodeAcquisitionPath::LoadTableDelegation { .. }
            );
            let context = crate::rest_protocol::http::RequestContext::from_connector(&request);
            let (uuid, delegation) = crate::loaded_table::run_vended_renewal_with_policy_async(
                policy,
                "Iceberg execution-node credential acquisition",
                move || {
                    let owner = Arc::clone(&owner);
                    let routing = routing.clone();
                    let context = context.clone();
                    async move {
                        crate::rest_protocol::endpoints::acquire_routed_in(
                            &owner, &routing, load_table, &context,
                        )
                        .await
                    }
                },
            )
            .await?;
            self.project_refresh(uuid, delegation)
        })
    }
}

impl ExecutionNodeCredentialsEndpointRefresher {
    fn project_refresh(
        &self,
        observed_uuid: Option<String>,
        delegation: crate::rest_protocol::deferred::RestDelegation,
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError> {
        // D11b: only the table identity is read from the response, and only to
        // compare it; no metadata reaches any plan, cache or table state here.
        if let ExecutionNodeAcquisitionPath::LoadTableDelegation {
            expected_table_uuid,
            ..
        } = &self.path
        {
            let observed = observed_uuid
                .as_deref()
                .and_then(|uuid| uuid::Uuid::parse_str(uuid).ok());
            if observed != Some(*expected_table_uuid) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::PermissionDenied,
                    "load-table credential acquisition answered for a different table",
                ));
            }
        }
        if !delegation.is_present() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::PermissionDenied,
                "credential acquisition returned static access delegation",
            ));
        }
        // No prefix-scope equality check here, unlike the coordinator's
        // refresher. That check exists to keep one admitted attempt's authority
        // from being widened mid-flight; this node holds one authority per
        // scope and `IcebergAuthorityMaterialSource` already refuses a response
        // that does not cover the exact scope it owns (CAD-1 D7).
        let seed = match parse_rest_delegation(delegation)? {
            IcebergAccessDelegation::Vended(seed) => seed,
            IcebergAccessDelegation::Static => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::PermissionDenied,
                    "credential acquisition returned static access delegation",
                ));
            }
        };
        let (entries, _) = seed
            .into_vended_s3_credential_lease_contribution()?
            .into_parts();
        VendedS3CredentialLeaseRefresh::try_new(entries)
    }
}

#[cfg(test)]
mod tests {
    //! The execution node's acquisition along a frozen routing context
    //! (spec 5.4, L17): its own identity, the coordinator's exact route, no
    //! `/v1/config`, only the UUID and credentials decoded. Each test owns a
    //! loopback scripted server; Drop joins it.
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use novarocks_spi::connector::{
        CredentialLoadTableDelegation, CredentialRenewalPath, CredentialRoutingContext,
        VendedS3CredentialRefreshDispatch, VendedS3CredentialRefreshDispatchGuard,
    };

    use super::*;
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};

    const TABLE_UUID: &str = "8f1d0c6e-0000-4000-8000-000000000001";
    const OTHER_TABLE_UUID: &str = "8f1d0c6e-0000-4000-8000-0000000000ff";
    const TABLE_PATH: &str = "/v1/p1/namespaces/sales/tables/orders";

    struct PermittedDispatch;

    impl VendedS3CredentialRefreshDispatchGuard for PermittedDispatch {
        fn provider_dispatch(&self) -> VendedS3CredentialRefreshDispatch {
            VendedS3CredentialRefreshDispatch::Permitted
        }
    }

    fn not_after() -> String {
        (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("wall clock")
            .as_millis()
            + 3_600_000)
            .to_string()
    }

    fn credentials() -> serde_json::Value {
        serde_json::json!([{
            "prefix": "s3://warehouse/sales/orders",
            "config": {
                "s3.access-key-id": "AKIA-node",
                "s3.secret-access-key": "secret",
                "s3.session-token": "session",
                "s3.session-token-expires-at-ms": not_after(),
            },
        }])
    }

    fn load_response(uuid: &str) -> Reply {
        Reply::json(
            200,
            &serde_json::json!({
                "metadata-location": "s3://warehouse/sales/orders/metadata/1.json",
                "metadata": {"table-uuid": uuid, "snapshots": []},
                "storage-credentials": credentials(),
            }),
        )
    }

    fn routing(server: &TestServer, path: &str) -> CredentialRoutingContext {
        CredentialRoutingContext::try_new(
            format!("{}{path}", server.url()),
            [(Arc::from("x-tenant"), Arc::from("t1"))],
        )
        .expect("routing context")
    }

    fn load_table_path(server: &TestServer, uuid: &str) -> ExecutionNodeAcquisitionPath {
        ExecutionNodeAcquisitionPath::project(&CredentialRenewalPath::LoadTableDelegation {
            routing: routing(server, TABLE_PATH),
            table: CredentialLoadTableDelegation::try_new(
                vec![Arc::from("sales")],
                Arc::from("orders"),
                Arc::from(uuid),
            )
            .expect("delegation"),
        })
        .expect("projected path")
    }

    fn endpoint_path(server: &TestServer) -> ExecutionNodeAcquisitionPath {
        ExecutionNodeAcquisitionPath::project(&CredentialRenewalPath::CredentialsEndpoint(routing(
            server,
            "/credentials",
        )))
        .expect("projected path")
    }

    fn refresher(
        server: &TestServer,
        runtime: &tokio::runtime::Runtime,
        principal: IcebergRestAuthMaterial,
        path: ExecutionNodeAcquisitionPath,
    ) -> ExecutionNodeCredentialsEndpointRefresher {
        ExecutionNodeCredentialsEndpointRefresher::new(
            "warehouse".to_string(),
            vec![
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), server.url()),
            ],
            principal,
            IcebergCatalogRuntime::new(runtime.handle().clone()),
            path,
        )
    }

    fn bearer() -> IcebergRestAuthMaterial {
        IcebergRestAuthMaterial::Bearer {
            token: novarocks_fs::SecretValue::new("execution-node-token"),
        }
    }

    fn policy(attempts: u8) -> VendedS3CredentialRefreshCallPolicy {
        VendedS3CredentialRefreshCallPolicy::try_new(
            Duration::from_secs(10),
            std::num::NonZeroU8::new(attempts).expect("attempts"),
            Duration::from_millis(1),
            Arc::new(PermittedDispatch),
        )
        .expect("policy")
    }

    #[test]
    fn a_cold_node_acquires_along_the_frozen_load_table_route_without_a_session() {
        // `/v1/config` would now name another prefix; the node never asks.
        let server = TestServer::start(
            Script::new()
                .on(
                    "GET",
                    "/v1/config",
                    Reply::config(&[("prefix", "p2")], &[], None),
                )
                .on("GET", TABLE_PATH, load_response(TABLE_UUID)),
        );
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let refresher = refresher(
            &server,
            &runtime,
            bearer(),
            load_table_path(&server, TABLE_UUID),
        );

        let Ok(refresh) = refresher.refresh_vended_s3_credentials(policy(1)) else {
            panic!("the node acquires on first use");
        };
        drop(refresh);

        assert_eq!(server.count("GET", "/v1/config"), 0);
        let loads = server.requests_to("GET", TABLE_PATH);
        assert_eq!(loads.len(), 1);
        assert_eq!(
            loads[0].header("authorization"),
            Some("Bearer execution-node-token")
        );
        assert_eq!(loads[0].header("x-tenant"), Some("t1"));
        assert_eq!(
            loads[0].header("x-iceberg-access-delegation"),
            Some("vended-credentials")
        );
    }

    #[test]
    fn the_endpoint_route_uses_an_exchanged_token_and_no_delegation_header() {
        let server = TestServer::start(
            Script::new()
                .on("POST", "/v1/oauth/tokens", Reply::token("node-token"))
                .on(
                    "GET",
                    "/credentials",
                    Reply::json(
                        200,
                        &serde_json::json!({"storage-credentials": credentials()}),
                    ),
                ),
        );
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let principal = IcebergRestAuthMaterial::Oauth2 {
            client_id: "executor".to_string(),
            client_secret: novarocks_fs::SecretValue::new("secret"),
        };
        let refresher = refresher(&server, &runtime, principal, endpoint_path(&server));

        assert!(refresher.refresh_vended_s3_credentials(policy(1)).is_ok());
        assert!(refresher.refresh_vended_s3_credentials(policy(1)).is_ok());

        assert_eq!(server.count("POST", "/v1/oauth/tokens"), 1, "token kept");
        let renewals = server.requests_to("GET", "/credentials");
        assert_eq!(renewals.len(), 2);
        assert!(renewals.iter().all(|request| {
            request.header("authorization") == Some("Bearer node-token")
                && request.header("x-iceberg-access-delegation").is_none()
        }));
    }

    #[test]
    fn a_response_for_another_table_or_without_credentials_is_refused() {
        for (reply, expected) in [
            (load_response(OTHER_TABLE_UUID), "different table"),
            (
                Reply::json(
                    200,
                    &serde_json::json!({
                        "metadata-location": "s3://warehouse/sales/orders/metadata/1.json",
                        "metadata": {"table-uuid": TABLE_UUID},
                    }),
                ),
                "static access delegation",
            ),
        ] {
            let server = TestServer::start(Script::new().on("GET", TABLE_PATH, reply));
            let runtime = tokio::runtime::Runtime::new().expect("runtime");
            let refresher = refresher(
                &server,
                &runtime,
                bearer(),
                load_table_path(&server, TABLE_UUID),
            );
            let Err(error) = refresher.refresh_vended_s3_credentials(policy(1)) else {
                panic!("{expected} must be refused");
            };
            assert_eq!(
                error.kind(),
                ConnectorErrorKind::PermissionDenied,
                "{expected}"
            );
            assert!(error.message().contains(expected), "{}", error.message());
        }
    }

    #[test]
    fn request_layer_failures_are_retryable_and_never_a_denial() {
        // A 401 after the node's token expired must not close the authority
        // while its material is still valid (L17).
        let server = TestServer::start(Script::new().on("GET", TABLE_PATH, Reply::status(401)));
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let refresher = refresher(
            &server,
            &runtime,
            bearer(),
            load_table_path(&server, TABLE_UUID),
        );
        let Err(error) = refresher.refresh_vended_s3_credentials(policy(2)) else {
            panic!("a 401 is not material");
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
        assert!(error.retryable_before_progress());
        assert_eq!(server.requests_to("GET", TABLE_PATH).len(), 2, "retried");

        let closed = TestServer::start(Script::new());
        let unreachable = load_table_path(&closed, TABLE_UUID);
        drop(closed.shut_down());
        let refresher = ExecutionNodeCredentialsEndpointRefresher::new(
            "warehouse".to_string(),
            vec![
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), "http://127.0.0.1:9".to_string()),
            ],
            bearer(),
            IcebergCatalogRuntime::new(runtime.handle().clone()),
            unreachable,
        );
        let Err(error) = refresher.refresh_vended_s3_credentials(policy(1)) else {
            panic!("an unreachable catalog yields no material");
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
        assert!(error.retryable_before_progress());
    }

    #[test]
    fn a_non_canonical_announced_address_is_refused() {
        let routing = CredentialRoutingContext::try_new(
            "http://127.0.0.1:1/v1/namespaces/a/tables/b?x=1",
            std::iter::empty(),
        )
        .expect("syntactically valid");
        let error = ExecutionNodeAcquisitionPath::project(
            &CredentialRenewalPath::CredentialsEndpoint(routing),
        )
        .expect_err("a query string is not a route");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }
}
