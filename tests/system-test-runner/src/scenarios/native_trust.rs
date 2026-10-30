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

//! Process-boundary Native trust acceptance scenarios.
//!
//! These scenarios deliberately use the existing cross-process harness rather
//! than constructing a second mini-cluster. Raw HTTP/2 probes exercise the
//! listener-wide admission layer before routing, while public SQL proves the
//! normal FE-to-BE production path continues through the same 1FE+3BE launch.

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use crate::scenarios::native_sql_controls::SqlIntentOwner;
use crate::scenarios::task_evidence;

#[path = "native_trust_controls.rs"]
mod native_trust_controls;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use h2::client;
use http::{Request, header};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture;
use novarocks_cluster_harness::vended_rest_catalog::{
    VendedRefreshBehavior, VendedRestCatalogConfig, VendedRestCatalogFixture, VendedS3Credential,
};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, NativeTrustFixture,
    NativeTrustFixtureMode, QueryLifecycleStructuredSnapshot, ServerHandle,
};
use novarocks_native_trust::{NativeEndpointConnector, NativeTrust};
use novarocks_proto_models::novarocks as proto;
use novarocks_secret::SecretValue;
use novarocks_types::NativeEndpoint;
use prost::Message;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

const REQUIRED_BACKENDS: usize = 3;
const GRPC_UNAUTHENTICATED: u16 = 16;
const GRPC_UNIMPLEMENTED: u16 = 12;
const UNKNOWN_NATIVE_PATH: &str = "/novarocks.NovaRocksGrpc/Nwt3Unknown";
const HEARTBEAT_PATH: &str = "/novarocks.NovaRocksGrpc/Heartbeat";
const APPLY_TASK_OPERATIONS_PATH: &str = "/novarocks.NovaRocksGrpc/ApplyTaskOperations";
const VENDED_METADATA_CREDENTIAL_NAME: &str = "native-trust-vended-metadata";
const VENDED_METADATA_CREDENTIAL_GENERATION: &str = "v1";
const VENDED_METADATA_ACCESS_KEY_ENV: &str = "NOVAROCKS_NATIVE_TRUST_METADATA_ACCESS_KEY_ID";
const VENDED_METADATA_SECRET_KEY_ENV: &str = "NOVAROCKS_NATIVE_TRUST_METADATA_SECRET_ACCESS_KEY";
/// Each execution node's own catalog identity: a vended read is admitted only
/// when execution nodes can acquire storage credentials themselves (L25).
const VENDED_EXECUTION_CREDENTIAL_NAME: &str = "native-trust-vended-execution";
const VENDED_EXECUTION_CREDENTIAL_GENERATION: &str = "v1";
const VENDED_EXECUTION_BEARER_ENV_PREFIX: &str = "NOVAROCKS_NATIVE_TRUST_EXECUTION_BEARER_BE_";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(NativeTrustPositive {
            name: "native-trust/plaintext-ip",
            fixture: NativeTrustFixture::plaintext_ip(),
            intent: SqlIntentOwner::default(),
        }),
        Box::new(NativeTrustPositive {
            name: "native-trust/automatic-dns",
            fixture: NativeTrustFixture::automatic_dns(),
            intent: SqlIntentOwner::default(),
        }),
        Box::new(NativeTrustPositive {
            name: "native-trust/pem-ip",
            fixture: NativeTrustFixture::pem_ip(),
            intent: SqlIntentOwner::default(),
        }),
        Box::new(NativeTrustNegative::domain_mismatch()),
        Box::new(NativeTrustNegative::plaintext_tls_mismatch()),
        Box::new(NativeTrustNegative::automatic_pem_mismatch()),
        Box::new(VendedCredentialTransportParity::plaintext()),
        Box::new(VendedCredentialTransportParity::automatic()),
        Box::new(VendedCredentialTransportParity::pem()),
    ]
}

struct NativeTrustPositive {
    name: &'static str,
    fixture: NativeTrustFixture,
    intent: SqlIntentOwner,
}

impl Scenario for NativeTrustPositive {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_trust",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            native_trust_fixture: self.fixture.clone(),
            ..Default::default()
        })
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        self.intent
            .freeze(
                native_trust_controls::baseline_statements(),
                &[],
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        ensure!(
            context.handle().native_trust_mode() == self.fixture.mode(),
            "Native trust harness launched a different transport profile"
        );
        let data_endpoint = context.handle().native_be_endpoint(0)?;
        let control_endpoint = context.handle().native_be_control_endpoint(0)?;
        let trust = context.handle().native_probe_trust()?;

        for (endpoint, path) in [
            (&data_endpoint, APPLY_TASK_OPERATIONS_PATH),
            (&control_endpoint, HEARTBEAT_PATH),
        ] {
            assert_authentication_order(context, endpoint, &trust, self.fixture.mode(), path)?;
        }
        context.action(
            "proved missing/invalid/valid JWT ordering on both real Native BE Data and Control listeners",
        );

        let mut connection = mysql_actor::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        let mut snapshots = Vec::new();
        let mut participating = BTreeSet::new();
        for index in 0..3 {
            let ordinal = index + 1;
            let selected = self.intent.take(index)?;
            let (rows, snapshot) = native_trust_controls::observed_query(
                context,
                &selected,
                &mut connection,
                native_trust_controls::BASELINE_SQL,
                &format!("distributed Native trust acceptance query {ordinal}"),
            )?;
            ensure!(
                rows == vec![1, 2],
                "Native trust acceptance query {ordinal} returned unexpected rows: {rows:?}"
            );
            let backends =
                assert_query_crossed_trust_boundary(context, &snapshot, "Native trust query")?;
            participating.extend(backends);
            snapshots.push(snapshot);
        }
        // Replaces a per-backend `NOVAROCKS_QUERY_INIT_APPLIED` loop, whose
        // subject -- the retired InitQuery -- no production query sends any
        // more. Its stated property was FE-to-BE admission across every BE,
        // and the task protocol cannot restate that through queries: a query
        // context is established only where the scheduler placed a task, and
        // the constant query above places tasks on one backend. Reading the
        // frontend's own registry proves the same reach without depending on
        // placement: a backend is eligible-live only once its authenticated
        // announce and the FE-pull exact heartbeat agree, and both are Native
        // RPCs over the transport profile under test, so a BE this fixture
        // could not authenticate to could not appear here.
        let topology = context
            .handle()
            .frontend_backend_topology()
            .context("read the frontend backend registry over the Native trust profile")?;
        ensure!(
            topology.len() == REQUIRED_BACKENDS
                && topology.iter().all(|row| row.is_eligible_live()),
            "frontend registry does not hold {REQUIRED_BACKENDS} eligible-live backends over transport={:?}: {topology:?}",
            self.fixture.mode()
        );
        context.action(format!(
            "proved real 1FE+3BE topology, authenticated FE-to-BE reach on every BE, and a completed FE/BE query round trip with transport={:?}; terminal snapshots={}, participating backends={participating:?}",
            self.fixture.mode()
            , snapshots.len()
        ));
        connection.finish()?;
        Ok(())
    }
}

struct NativeTrustNegative {
    name: &'static str,
    fixture: NativeTrustFixture,
    probe_mode: NativeTrustFixtureMode,
    wrong_reference: bool,
}

impl NativeTrustNegative {
    fn domain_mismatch() -> Self {
        Self {
            name: "native-trust/reject-jwt-domain-mismatch",
            fixture: NativeTrustFixture::automatic_dns(),
            probe_mode: NativeTrustFixtureMode::Automatic,
            wrong_reference: true,
        }
    }

    fn plaintext_tls_mismatch() -> Self {
        Self {
            name: "native-trust/reject-plaintext-tls-mismatch",
            fixture: NativeTrustFixture::plaintext_ip(),
            probe_mode: NativeTrustFixtureMode::Automatic,
            wrong_reference: false,
        }
    }

    fn automatic_pem_mismatch() -> Self {
        Self {
            name: "native-trust/reject-automatic-pem-mismatch",
            fixture: NativeTrustFixture::automatic_dns(),
            probe_mode: NativeTrustFixtureMode::Pem,
            wrong_reference: false,
        }
    }
}

impl Scenario for NativeTrustNegative {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_trust",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            native_trust_fixture: self.fixture.clone(),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let endpoint = if self.wrong_reference {
            NativeEndpoint::from_host_port("127.0.0.1", context.handle().runtime().be[0].grpc)
                .map_err(anyhow::Error::msg)
                .context("construct intentionally wrong automatic TLS reference")?
        } else {
            context.handle().native_be_endpoint(0)?
        };
        let connector = context
            .handle()
            .native_probe_connector(endpoint, self.probe_mode)?;
        let failure = connect_probe(connector).expect_err("mismatched Native transport must fail");
        let diagnostic = format!("{failure:#}");
        ensure!(
            !diagnostic.contains("Bearer "),
            "mismatched Native transport error leaked an authorization value"
        );
        context.action(format!(
            "rejected Native mismatch fixture={:?} probe={:?} before any authenticated RPC dispatch",
            self.fixture.mode(),
            self.probe_mode
        ));
        Ok(())
    }
}

/// Runs one real REST-vended catalog definition over every Native transport
/// profile and requires the same outcome from each.
///
/// This used to be a gate. The plaintext variant proved that h2c refused a
/// vended definition, because the query-attempt lease carried secret material
/// across the FE/BE boundary and only TLS could protect it in flight. CAD-1
/// took the material off that boundary -- the node that consumes a credential
/// now acquires it under its own catalog identity (D1) -- so there is nothing
/// left for a transport gate to protect, and the gate was removed with it.
///
/// Parity is what replaces it: the same definition must be admitted, reach a
/// backend, and return the same rows on h2c, automatic TLS and PEM TLS alike.
/// A transport-dependent difference here would mean material had found its way
/// back onto the wire.
struct VendedCredentialTransportParity {
    name: &'static str,
    fixture: NativeTrustFixture,
    rest: Mutex<Option<VendedCredentialParityFixture>>,
    intent: SqlIntentOwner,
}

struct VendedCredentialParityFixture {
    rest: IsolatedIcebergRestFixture,
    proxy: VendedRestCatalogFixture,
}

impl VendedCredentialTransportParity {
    fn plaintext() -> Self {
        Self::new(
            "native-trust/vended-credential-transport-parity",
            NativeTrustFixture::plaintext_ip(),
        )
    }

    fn automatic() -> Self {
        Self::new(
            "native-trust/vended-credential-transport-parity-automatic",
            NativeTrustFixture::automatic_dns(),
        )
    }

    fn pem() -> Self {
        Self::new(
            "native-trust/vended-credential-transport-parity-pem",
            NativeTrustFixture::pem_ip(),
        )
    }

    fn new(name: &'static str, fixture: NativeTrustFixture) -> Self {
        Self {
            name,
            fixture,
            rest: Mutex::new(None),
            intent: SqlIntentOwner::default(),
        }
    }

    fn fixture_endpoints(&self) -> Result<(String, String, String)> {
        let fixture = self
            .rest
            .lock()
            .map_err(|_| anyhow::anyhow!("vended transport parity fixture lock poisoned"))?;
        let fixture = fixture
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("vended transport parity fixture is missing"))?;
        Ok((
            fixture.proxy.uri().to_owned(),
            fixture.rest.endpoints().rest_warehouse.clone(),
            fixture.rest.endpoints().minio_endpoint.clone(),
        ))
    }

    fn table_loads(&self) -> Result<u64> {
        let fixture = self
            .rest
            .lock()
            .map_err(|_| anyhow::anyhow!("vended transport parity fixture lock poisoned"))?;
        fixture
            .as_ref()
            .map(|fixture| fixture.proxy.audit().table_loads)
            .ok_or_else(|| anyhow::anyhow!("vended transport parity fixture is missing"))
    }
}

impl Scenario for VendedCredentialTransportParity {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_trust",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config_with_fixture_run(
        &self,
        scenario_root: &std::path::Path,
        budget: &novarocks_cluster_harness::fixture_run::RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        let mut rest = IsolatedIcebergRestFixture::start_with_shared_minio(scenario_root, budget)
            .context("start isolated REST fixture for vended transport parity")?;
        rest.provision_empty_table("vended_parity_db", "vended_parity_data")
            .context("provision isolated vended transport parity source table")?;
        let endpoints = rest.endpoints().clone();
        let metadata_identity = rest.static_s3_identity();
        let identities = rest
            .provision_vended_s3_identities()
            .context("provision isolated vended transport parity S3 identities")?;
        let proxy = VendedRestCatalogFixture::start(VendedRestCatalogConfig {
            downstream: endpoints.rest_uri.clone(),
            scope_prefix: format!("{}/", endpoints.rest_warehouse.trim_end_matches('/')),
            initial: VendedS3Credential::new(
                identities.initial.access_key_id,
                SecretValue::new(identities.initial.secret_access_key),
                SecretValue::new(identities.initial.session_token),
            )
            .and_then(|credential| {
                credential.with_not_after_unix_ms(identities.initial.not_after_unix_ms)
            })
            .context("build initial vended transport parity S3 credential")?,
            rotated: VendedS3Credential::new(
                identities.rotated.access_key_id,
                SecretValue::new(identities.rotated.secret_access_key),
                SecretValue::new(identities.rotated.session_token),
            )
            .and_then(|credential| {
                credential.with_not_after_unix_ms(identities.rotated.not_after_unix_ms)
            })
            .context("build rotated vended transport parity S3 credential")?,
            initial_ttl: Duration::from_secs(60),
            refresh_ttl: Duration::from_secs(60),
            refresh_behavior: VendedRefreshBehavior::IssueRotatedCredential,
            table_commit_response_behavior: Default::default(),
            hold_first_table_commit_response: false,
        })
        .context("start vended transport parity REST proxy")?;
        let mut fixture = self
            .rest
            .lock()
            .map_err(|_| anyhow::anyhow!("vended transport parity fixture lock poisoned"))?;
        ensure!(
            fixture.is_none(),
            "vended transport parity fixture was initialized more than once"
        );
        *fixture = Some(VendedCredentialParityFixture { rest, proxy });
        let mut child_environment = CrossProcessChildEnvironment::default();
        child_environment.fe.insert(
            VENDED_METADATA_ACCESS_KEY_ENV.to_string(),
            metadata_identity.access_key_id,
        );
        child_environment.fe.insert(
            VENDED_METADATA_SECRET_KEY_ENV.to_string(),
            metadata_identity.secret_access_key,
        );
        let metadata_registry = format!(
            r#"
[[connector.credentials]]
purpose = "object-store-metadata"
name = "{VENDED_METADATA_CREDENTIAL_NAME}"
generation = "{VENDED_METADATA_CREDENTIAL_GENERATION}"
kind = "s3"
access_key_id = "${{ENV:{VENDED_METADATA_ACCESS_KEY_ENV}}}"
access_key_secret = "${{ENV:{VENDED_METADATA_SECRET_KEY_ENV}}}"
"#
        );
        let mut config_overlay = CrossProcessConfigOverlay {
            fe: Some(metadata_registry),
            ..Default::default()
        };
        for backend_index in 0..REQUIRED_BACKENDS {
            let environment_name = format!("{VENDED_EXECUTION_BEARER_ENV_PREFIX}{backend_index}");
            child_environment
                .be_by_index
                .entry(backend_index)
                .or_default()
                .insert(
                    environment_name.clone(),
                    format!("native-trust-execution-node-be-{backend_index}"),
                );
            config_overlay.be_by_index.insert(
                backend_index,
                format!(
                    r#"
[[connector.credentials]]
purpose = "data-credential-vending"
name = "{VENDED_EXECUTION_CREDENTIAL_NAME}"
generation = "{VENDED_EXECUTION_CREDENTIAL_GENERATION}"
kind = "iceberg-rest-bearer"
token = "${{ENV:{environment_name}}}"
"#
                ),
            );
        }
        Ok(ScenarioLaunchConfig {
            native_trust_fixture: self.fixture.clone(),
            child_environment,
            config_overlay,
            ..Default::default()
        })
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        self.intent
            .freeze(
                vec![native_trust_controls::grpc_statement(
                    native_trust_controls::PARITY_SQL,
                )],
                &[],
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        ensure!(
            context.handle().native_trust_mode() == self.fixture.mode(),
            "vended transport parity launched a different Native transport profile"
        );
        assert_credential_domain_task_ingress(context, self.fixture.mode())?;

        let (proxy_uri, warehouse, minio_endpoint) = self.fixture_endpoints()?;
        let mut connection = mysql_actor::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        const CATALOG: &str = "vended_parity";
        connection
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{proxy_uri}\",\"iceberg.catalog.warehouse\"=\"{warehouse}\",\"aws.s3.endpoint\"=\"{minio_endpoint}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{VENDED_METADATA_CREDENTIAL_NAME}\",\"credential.object-store-metadata.generation\"=\"{VENDED_METADATA_CREDENTIAL_GENERATION}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"vended\",\"credential.data-credential-vending.consumer-role\"=\"backend\",\"credential.data-credential-vending.mode\"=\"static\",\"credential.data-credential-vending.name\"=\"{VENDED_EXECUTION_CREDENTIAL_NAME}\",\"credential.data-credential-vending.generation\"=\"{VENDED_EXECUTION_CREDENTIAL_GENERATION}\")"
            ))
            .context("create real REST-vended catalog for Native transport parity")?;

        let query = native_trust_controls::PARITY_SQL;
        let establish_before = (0..REQUIRED_BACKENDS)
            .map(|index| {
                context
                    .handle()
                    .be_log_count(index, task_evidence::CONTEXT_ESTABLISH_APPLIED)
            })
            .collect::<Result<Vec<_>>>()?;
        let selected = self.intent.take(0)?;
        let (rows, snapshot) = native_trust_controls::observed_query(
            context,
            &selected,
            &mut connection,
            query,
            &format!(
                "real REST-vended query over {:?} Native transport",
                self.fixture.mode()
            ),
        )?;
        assert_query_crossed_trust_boundary(context, &snapshot, "vended transport parity query")?;
        ensure!(
            rows == vec![0],
            "vended catalog query returned unexpected rows on {:?}: {rows:?}",
            self.fixture.mode()
        );
        // The retired gate proved a negative on h2c: no BE context may exist,
        // because the statement never got past FE admission. Parity needs the
        // opposite evidence, and needs it on every profile -- a query answered
        // without reaching a backend would satisfy the row assertion above
        // while proving nothing about the transport.
        let establish_after = (0..REQUIRED_BACKENDS)
            .map(|index| {
                context
                    .handle()
                    .be_log_count(index, task_evidence::CONTEXT_ESTABLISH_APPLIED)
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            establish_after
                .iter()
                .zip(&establish_before)
                .any(|(after, before)| after > before),
            "vended catalog query established no BE context on {:?}: before={establish_before:?} after={establish_after:?}",
            self.fixture.mode()
        );
        context.action(format!(
            "admitted the real vended definition through FE and BE lifecycle over {:?} Native transport",
            self.fixture.mode()
        ));
        ensure!(
            self.table_loads()? > 0,
            "vended transport parity did not observe a REST table response carrying a lease"
        );
        connection.finish()?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .rest
            .lock()
            .map_err(|_| anyhow::anyhow!("vended transport parity fixture lock poisoned"))?
            .take();
        let Some(VendedCredentialParityFixture { mut rest, proxy }) = fixture else {
            return Ok(());
        };
        drop(proxy);
        rest.shutdown()
            .context("shutdown isolated vended transport parity REST fixture")
    }
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let count = context.handle().be_count();
    ensure!(
        count == REQUIRED_BACKENDS,
        "{} requires a real 1FE+3BE Native cluster, got 1FE+{count}BE",
        context.name()
    );
    context.action("verified real independent-process 1FE+3BE Native topology");
    Ok(())
}

fn assert_authentication_order(
    context: &mut ScenarioContext,
    endpoint: &NativeEndpoint,
    trust: &NativeTrust,
    mode: NativeTrustFixtureMode,
    representative_path: &str,
) -> Result<()> {
    let missing = raw_grpc_probe(
        context
            .handle()
            .native_probe_connector(endpoint.clone(), mode)?,
        UNKNOWN_NATIVE_PATH,
        None,
        None,
    )?;
    ensure!(
        missing.http_status == 200 && missing.grpc_status == Some(GRPC_UNAUTHENTICATED),
        "missing JWT must fail listener admission before unknown-path fallback, got {missing:?}"
    );
    let invalid = raw_grpc_probe(
        context
            .handle()
            .native_probe_connector(endpoint.clone(), mode)?,
        UNKNOWN_NATIVE_PATH,
        Some("Bearer invalid.native.token"),
        None,
    )?;
    ensure!(
        invalid.http_status == 200 && invalid.grpc_status == Some(GRPC_UNAUTHENTICATED),
        "invalid JWT must fail listener admission before unknown-path fallback, got {invalid:?}"
    );
    let authorization = authorization_header(trust)?;
    let valid_unknown = raw_grpc_probe(
        context
            .handle()
            .native_probe_connector(endpoint.clone(), mode)?,
        UNKNOWN_NATIVE_PATH,
        Some(&authorization),
        None,
    )?;
    ensure!(
        valid_unknown.http_status == 200 && valid_unknown.grpc_status == Some(GRPC_UNIMPLEMENTED),
        "valid JWT must reach the Native unknown-path fallback, got {valid_unknown:?}"
    );
    let valid_rpc = raw_grpc_probe(
        context
            .handle()
            .native_probe_connector(endpoint.clone(), mode)?,
        representative_path,
        Some(&authorization),
        // An unterminated protobuf varint is invalid for either request schema.
        Some(&[0, 0, 0, 0, 1, 0xff]),
    )?;
    ensure!(
        valid_rpc.grpc_status == Some(tonic::Code::Internal as u16),
        "valid JWT must reach representative Native RPC protobuf decode on its exact endpoint, got {valid_rpc:?}"
    );
    Ok(())
}

/// Sends a credential-domain establish directly to one real BE listener.
///
/// This used to prove a transport gate: on h2c the request was refused because
/// it carried confidential material, and on TLS the same bytes were admitted
/// and then rejected structurally. There is no such gate any more -- material
/// never crosses this boundary, because the node that consumes it acquires it
/// under its own catalog identity (CAD-1 D1).
///
/// What is still worth proving is that the announcement is treated identically
/// on both transports and that raw ingress still refuses a malformed request.
/// The request deliberately omits its operation envelope, so every transport
/// must answer with the same structural rejection.
fn assert_credential_domain_task_ingress(
    context: &mut ScenarioContext,
    mode: NativeTrustFixtureMode,
) -> Result<()> {
    let request = proto::ApplyTaskOperationsRequest {
        operations: vec![proto::TaskOperation {
            envelope: None,
            operation: Some(proto::task_operation::Operation::UpdateQueryContext(
                proto::UpdateQueryContextRequest {
                    command: Some(proto::update_query_context_request::Command::Establish(
                        proto::EstablishQueryContextRequest {
                            initial_credential: Some(proto::QueryContextCredentialDomain {
                                lease_id: 1,
                                epoch: 1,
                                descriptors: Vec::new(),
                            }),
                            ..Default::default()
                        },
                    )),
                },
            )),
        }],
    };
    let mut payload = Vec::new();
    request
        .encode(&mut payload)
        .context("encode direct credential-domain task-ingress probe")?;
    let mut frame = Vec::with_capacity(payload.len() + 5);
    frame.push(0);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);

    let endpoint = context.handle().native_be_endpoint(0)?;
    let connector = context.handle().native_probe_connector(endpoint, mode)?;
    let authorization = authorization_header(&context.handle().native_probe_trust()?)?;
    let response = raw_grpc_probe(
        connector,
        APPLY_TASK_OPERATIONS_PATH,
        Some(&authorization),
        Some(&frame),
    )?;
    ensure!(
        response.grpc_status == Some(tonic::Code::InvalidArgument as u16),
        "direct credential-domain task ingress returned unexpected status: {response:?}"
    );
    let diagnostic = response.grpc_message.as_deref().unwrap_or_default();
    ensure!(
        !diagnostic.contains("confidential"),
        "raw ingress still speaks of a transport gate that no longer exists: {response:?}"
    );
    ensure!(
        diagnostic.contains("envelope"),
        "raw ingress did not reach structural decode: {response:?}"
    );
    context.action(format!(
        "proved raw BE ApplyTaskOperations treats a credential announcement identically on {mode:?}, with no transport gate in front of it",
    ));
    Ok(())
}

/// Confirms native execution and convergence across the trust boundary using
/// this query's exact Task evidence.
fn assert_query_crossed_trust_boundary(
    context: &mut ScenarioContext,
    snapshot: &QueryLifecycleStructuredSnapshot,
    subject: &str,
) -> Result<BTreeSet<usize>> {
    task_evidence::assert_query_completed_across_boundary(context, snapshot, subject)
}

#[derive(Debug, serde::Serialize)]
pub(super) struct GrpcProbe {
    pub(super) http_status: u16,
    pub(super) grpc_status: Option<u16>,
    pub(super) grpc_message: Option<String>,
    // Only the existing bounded mode captures data, after its original cap check.
    // None means unobserved; Some(empty) proves a physically empty response body.
    pub(super) response_body: Option<Vec<u8>>,
}

/// An authenticated plaintext/IP probe for malformed, metadata-only requests. The
/// authorization value is neither returned nor included in its observation.
pub(super) fn bounded_authenticated_probe(
    context: &mut ScenarioContext,
    path: &str,
    frame: &[u8],
) -> Result<GrpcProbe> {
    ensure!(
        frame.len() <= 4096,
        "raw Native probe request exceeds its bound"
    );
    let endpoint = context.handle().native_be_endpoint(0)?;
    let mode = context.handle().native_trust_mode();
    ensure!(
        mode == NativeTrustFixtureMode::Plaintext
            && endpoint.host().parse::<std::net::IpAddr>().is_ok(),
        "bounded malformed-request probe requires plaintext and an IP endpoint"
    );
    let connector = context.handle().native_probe_connector(endpoint, mode)?;
    let authorization = authorization_header(&context.handle().native_probe_trust()?)?;
    let deadline = context
        .remaining("bounded authenticated Native probe")?
        .min(Duration::from_secs(10));
    raw_grpc_probe_with_deadline(
        connector,
        path,
        Some(&authorization),
        Some(frame),
        Some(deadline),
    )
}

fn authorization_header(trust: &NativeTrust) -> Result<String> {
    let mut request = tonic::Request::new(());
    trust
        .apply_client_authorization(request.metadata_mut())
        .map_err(anyhow::Error::msg)
        .context("issue valid Native trust test JWT")?;
    request
        .metadata()
        .get("authorization")
        .context("Native trust interceptor did not add authorization metadata")?
        .to_str()
        .context("Native trust authorization metadata was not ASCII")
        .map(ToOwned::to_owned)
}

fn connect_probe(connector: NativeEndpointConnector) -> Result<()> {
    tokio::runtime::Runtime::new()
        .context("create Native mismatch probe runtime")?
        .block_on(async move {
            let stream = connector
                .connect()
                .await
                .map_err(anyhow::Error::msg)
                .context("connect mismatched Native transport")?;
            let (_sender, connection) = client::handshake(stream)
                .await
                .context("perform HTTP/2 handshake over mismatched Native transport")?;
            drop(connection);
            Ok(())
        })
}

fn raw_grpc_probe(
    connector: NativeEndpointConnector,
    path: &str,
    authorization: Option<&str>,
    body: Option<&[u8]>,
) -> Result<GrpcProbe> {
    raw_grpc_probe_with_deadline(connector, path, authorization, body, None)
}

fn raw_grpc_probe_with_deadline(
    connector: NativeEndpointConnector,
    path: &str,
    authorization: Option<&str>,
    body: Option<&[u8]>,
    deadline: Option<Duration>,
) -> Result<GrpcProbe> {
    tokio::runtime::Runtime::new()
        .context("create Native raw probe runtime")?
        .block_on(async move {
            let mut driver = None;
            let probe = async {
                let stream = connector
                    .connect()
                    .await
                    .map_err(anyhow::Error::msg)
                    .context("connect Native raw probe")?;
                let (mut sender, connection) = client::handshake(stream)
                    .await
                    .context("perform Native raw probe HTTP/2 handshake")?;
                driver = Some(tokio::spawn(connection));
                let mut request = Request::builder()
                    .method("POST")
                    .uri(path)
                    .header(header::CONTENT_TYPE, "application/grpc")
                    .header("te", "trailers");
                if let Some(authorization) = authorization {
                    request = request.header(header::AUTHORIZATION, authorization);
                }
                let request = request.body(()).context("build Native raw probe request")?;
                let end_of_stream = body.is_none();
                let (response, mut send_stream) = sender
                    .send_request(request, end_of_stream)
                    .context("send Native raw probe request")?;
                if let Some(body) = body {
                    send_stream
                        .send_data(Bytes::copy_from_slice(body), true)
                        .context("send Native raw probe gRPC frame")?;
                }
                let response = response
                    .await
                    .context("receive Native raw probe response")?;
                let http_status = response.status().as_u16();
                let header_status = grpc_status(response.headers());
                let header_message = grpc_message(response.headers());
                let mut body = response.into_body();
                let mut response_bytes = 0usize;
                let mut response_body = deadline.map(|_| Vec::new());
                while let Some(bytes) = body
                    .data()
                    .await
                    .transpose()
                    .context("read Native raw probe body")?
                {
                    response_bytes = response_bytes
                        .checked_add(bytes.len())
                        .context("Native probe response byte overflow")?;
                    ensure!(
                        deadline.is_none() || response_bytes <= 4096,
                        "malformed-request Native response exceeds probe bound"
                    );
                    if let Some(captured) = response_body.as_mut() {
                        captured.extend_from_slice(&bytes);
                    }
                }
                let trailer_status = body
                    .trailers()
                    .await
                    .context("read Native raw probe trailers")?;
                let grpc_status =
                    header_status.or_else(|| trailer_status.as_ref().and_then(grpc_status));
                let grpc_message =
                    header_message.or_else(|| trailer_status.as_ref().and_then(grpc_message));
                Ok(GrpcProbe {
                    http_status,
                    grpc_status,
                    grpc_message,
                    response_body,
                })
            };
            let result = match deadline {
                Some(deadline) => match tokio::time::timeout(deadline, probe).await {
                    Ok(result) => result,
                    Err(error) => Err(anyhow::Error::from(error)
                        .context("absolute Native probe deadline exceeded")),
                },
                None => probe.await,
            };
            // Response failure or timeout must also abort and actually join
            // the owned HTTP/2 driver. Peer keep-alive is not its exit signal.
            let joined = native_trust_controls::join_probe_driver(driver).await;
            native_trust_controls::with_cleanup(result, joined)
        })
}

fn grpc_status(headers: &http::HeaderMap) -> Option<u16> {
    headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn grpc_message(headers: &http::HeaderMap) -> Option<String> {
    headers
        .get("grpc-message")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_the_full_native_trust_transport_matrix() {
        let names = scenarios()
            .into_iter()
            .map(|scenario| scenario.name())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "native-trust/plaintext-ip",
                "native-trust/automatic-dns",
                "native-trust/pem-ip",
                "native-trust/reject-jwt-domain-mismatch",
                "native-trust/reject-plaintext-tls-mismatch",
                "native-trust/reject-automatic-pem-mismatch",
                "native-trust/vended-credential-transport-parity",
                "native-trust/vended-credential-transport-parity-automatic",
                "native-trust/vended-credential-transport-parity-pem",
            ]
        );
    }

    #[test]
    fn negative_transport_scenarios_have_no_test_session_authority() {
        let deadline = Instant::now() + Duration::from_secs(5);
        for scenario in [
            NativeTrustNegative::domain_mismatch(),
            NativeTrustNegative::plaintext_tls_mismatch(),
            NativeTrustNegative::automatic_pem_mismatch(),
        ] {
            assert!(
                scenario
                    .test_session_plan(
                        &ScenarioLaunchConfig::default(),
                        Path::new("/unused"),
                        REQUIRED_BACKENDS,
                        Path::new("/unused/primary"),
                        deadline,
                    )
                    .unwrap()
                    .is_none()
            );
        }
    }
}
