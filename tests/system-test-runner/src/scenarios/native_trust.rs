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
use crate::scenarios::task_evidence;
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
    NativeTrustFixture, NativeTrustFixtureMode, QueryLifecycleStructuredSnapshot, ServerHandle,
};
use novarocks_native_trust::{NativeEndpointConnector, NativeTrust};
use novarocks_secret::SecretValue;
use novarocks_types::NativeEndpoint;
use prost::Message;
use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::Duration;

const REQUIRED_BACKENDS: usize = 3;
const GRPC_UNAUTHENTICATED: u16 = 16;
const GRPC_UNIMPLEMENTED: u16 = 12;
const UNKNOWN_NATIVE_PATH: &str = "/novarocks.NovaRocksGrpc/Nwt3Unknown";
const HEARTBEAT_PATH: &str = "/novarocks.NovaRocksGrpc/Heartbeat";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(NativeTrustPositive {
            name: "native-trust/plaintext-ip",
            fixture: NativeTrustFixture::plaintext_ip(),
        }),
        Box::new(NativeTrustPositive {
            name: "native-trust/automatic-dns",
            fixture: NativeTrustFixture::automatic_dns(),
        }),
        Box::new(NativeTrustPositive {
            name: "native-trust/pem-ip",
            fixture: NativeTrustFixture::pem_ip(),
        }),
        Box::new(NativeTrustNegative::domain_mismatch()),
        Box::new(NativeTrustNegative::plaintext_tls_mismatch()),
        Box::new(NativeTrustNegative::automatic_pem_mismatch()),
        Box::new(VendedCredentialTlsGate::plaintext()),
        Box::new(VendedCredentialTlsGate::automatic()),
        Box::new(VendedCredentialTlsGate::pem()),
    ]
}

struct NativeTrustPositive {
    name: &'static str,
    fixture: NativeTrustFixture,
}

impl Scenario for NativeTrustPositive {
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
        ensure!(
            context.handle().native_trust_mode() == self.fixture.mode(),
            "Native trust harness launched a different transport profile"
        );
        let endpoint = context.handle().native_be_endpoint(0)?;
        let trust = context.handle().native_probe_trust()?;

        assert_authentication_order(context, &endpoint, &trust, self.fixture.mode())?;
        context.action(
            "proved listener-wide missing/invalid/valid JWT ordering on a real Native BE listener",
        );

        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect Native trust acceptance MySQL client")?,
        )?;
        let mut previous_execution = context
            .handle()
            .query_lifecycle_structured_snapshot()?
            .and_then(|snapshot| snapshot.execution_id);
        let mut snapshots = Vec::new();
        let mut participating = BTreeSet::new();
        let deadline = context.deadline();
        for ordinal in 1..=3 {
            let rows: Vec<i64> = connection
                .query("SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v")
                .with_context(|| {
                    format!("run distributed Native trust acceptance query {ordinal}")
                })?;
            ensure!(
                rows == vec![1, 2],
                "Native trust acceptance query {ordinal} returned unexpected rows: {rows:?}"
            );
            let snapshot = context
                .handle()
                .await_query_lifecycle_structured_snapshot_after(
                    previous_execution.as_deref(),
                    deadline,
                )
                .with_context(|| {
                    format!("read FE lifecycle snapshot for Native trust query {ordinal}")
                })?;
            previous_execution = snapshot.execution_id.clone();
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

/// Exercises the confidential query-attempt lease transport boundary with one
/// real REST-vended catalog definition. The plaintext variant proves both
/// independently-owned h2c rejections, while the TLS variants prove the same
/// definition is admitted over automatic and PEM Native TLS.
struct VendedCredentialTlsGate {
    name: &'static str,
    fixture: NativeTrustFixture,
    rest: Mutex<Option<VendedCredentialTlsFixture>>,
}

struct VendedCredentialTlsFixture {
    rest: IsolatedIcebergRestFixture,
    proxy: VendedRestCatalogFixture,
}

impl VendedCredentialTlsGate {
    fn plaintext() -> Self {
        Self::new(
            "native-trust/vended-credential-tls-gate",
            NativeTrustFixture::plaintext_ip(),
        )
    }

    fn automatic() -> Self {
        Self::new(
            "native-trust/vended-credential-tls-gate-automatic",
            NativeTrustFixture::automatic_dns(),
        )
    }

    fn pem() -> Self {
        Self::new(
            "native-trust/vended-credential-tls-gate-pem",
            NativeTrustFixture::pem_ip(),
        )
    }

    fn new(name: &'static str, fixture: NativeTrustFixture) -> Self {
        Self {
            name,
            fixture,
            rest: Mutex::new(None),
        }
    }

    fn fixture_endpoints(&self) -> Result<(String, String, String)> {
        let fixture = self
            .rest
            .lock()
            .map_err(|_| anyhow::anyhow!("vended TLS gate fixture lock poisoned"))?;
        let fixture = fixture
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("vended TLS gate fixture is missing"))?;
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
            .map_err(|_| anyhow::anyhow!("vended TLS gate fixture lock poisoned"))?;
        fixture
            .as_ref()
            .map(|fixture| fixture.proxy.audit().table_loads)
            .ok_or_else(|| anyhow::anyhow!("vended TLS gate fixture is missing"))
    }
}

impl Scenario for VendedCredentialTlsGate {
    fn name(&self) -> &'static str {
        self.name
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut rest = IsolatedIcebergRestFixture::start(scenario_root)
            .context("start isolated REST fixture for vended TLS gate")?;
        rest.provision_empty_table("vended_tls_db", "vended_tls_data")
            .context("provision isolated vended TLS gate source table")?;
        let endpoints = rest.endpoints().clone();
        let identities = rest
            .provision_vended_s3_identities()
            .context("provision isolated vended TLS gate S3 identities")?;
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
            .context("build initial vended TLS gate S3 credential")?,
            rotated: VendedS3Credential::new(
                identities.rotated.access_key_id,
                SecretValue::new(identities.rotated.secret_access_key),
                SecretValue::new(identities.rotated.session_token),
            )
            .and_then(|credential| {
                credential.with_not_after_unix_ms(identities.rotated.not_after_unix_ms)
            })
            .context("build rotated vended TLS gate S3 credential")?,
            initial_ttl: Duration::from_secs(60),
            refresh_ttl: Duration::from_secs(60),
            refresh_behavior: VendedRefreshBehavior::IssueRotatedCredential,
            table_commit_response_behavior: Default::default(),
            hold_first_table_commit_response: false,
        })
        .context("start vended TLS gate REST proxy")?;
        let mut fixture = self
            .rest
            .lock()
            .map_err(|_| anyhow::anyhow!("vended TLS gate fixture lock poisoned"))?;
        ensure!(
            fixture.is_none(),
            "vended TLS gate fixture was initialized more than once"
        );
        *fixture = Some(VendedCredentialTlsFixture { rest, proxy });
        Ok(ScenarioLaunchConfig {
            native_trust_fixture: self.fixture.clone(),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        ensure!(
            context.handle().native_trust_mode() == self.fixture.mode(),
            "vended TLS gate launched a different Native transport profile"
        );
        // # The retired direct-ingress probe
        //
        // This step used to dial `InitQuery` on BE[0] with a confidential
        // `CredentialLeaseSecretEnvelope` and require h2c to refuse it while
        // TLS carried it into later validation. `InitQuery` is gone, and the
        // envelope's successor carrier is the task protocol's
        // `QueryContextCredentialDomain` on establish -- but the transport gate
        // for it, `refuse_confidential_material_in_the_clear`
        // (`novarocks/proto-codec/src/task_execution/domain.rs`), has no
        // production caller, so no backend ingress refuses confidential
        // material in the clear today and a probe would observe nothing.
        // Restoring this step needs that gate wired into the apply ingress
        // first; asserting it now would assert a refusal no code produces.

        let (proxy_uri, warehouse, minio_endpoint) = self.fixture_endpoints()?;
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect vended TLS gate MySQL client")?,
        )?;
        const CATALOG: &str = "vended_tls_gate";
        connection
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{proxy_uri}\",\"iceberg.catalog.warehouse\"=\"{warehouse}\",\"aws.s3.endpoint\"=\"{minio_endpoint}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-data.consumer-role\"=\"frontend-and-backend\",\"credential.object-store-data.mode\"=\"vended\")"
            ))
            .context("create real REST-vended catalog for Native TLS gate")?;

        let query = format!("SELECT count(*) FROM {CATALOG}.vended_tls_db.vended_tls_data");
        match self.fixture.mode() {
            NativeTrustFixtureMode::Plaintext => {
                let init_counts = (0..REQUIRED_BACKENDS)
                    .map(|index| {
                        context
                            .handle()
                            .be_log_count(index, task_evidence::CONTEXT_ESTABLISH_APPLIED)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let error = connection
                    .query::<i64, _>(&query)
                    .expect_err("plaintext vended catalog query must fail at FE admission");
                let diagnostic = error.to_string();
                ensure!(
                    diagnostic.contains(
                        "vended credential lease admission requires TLS Native transport"
                    ),
                    "plaintext vended catalog query did not expose the typed FE TLS rejection: {diagnostic}"
                );
                for (index, before) in init_counts.into_iter().enumerate() {
                    ensure!(
                        context
                            .handle()
                            .be_log_count(index, task_evidence::CONTEXT_ESTABLISH_APPLIED)?
                            == before,
                        "plaintext vended catalog query established a context on BE[{index}] after FE rejection"
                    );
                }
                context.action("proved h2c rejects the real vended definition at FE admission before any BE context establish");
            }
            NativeTrustFixtureMode::Automatic | NativeTrustFixtureMode::Pem => {
                let rows: Vec<i64> = connection
                    .query(&query)
                    .context("run real REST-vended query over Native TLS")?;
                ensure!(
                    rows == vec![0],
                    "TLS vended catalog query returned unexpected rows: {rows:?}"
                );
                context.action(format!(
                    "admitted the real vended definition through FE and BE lifecycle over {:?} Native TLS",
                    self.fixture.mode()
                ));
            }
        }
        ensure!(
            self.table_loads()? > 0,
            "vended TLS gate did not observe a REST table response carrying a lease"
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .rest
            .lock()
            .map_err(|_| anyhow::anyhow!("vended TLS gate fixture lock poisoned"))?
            .take();
        let Some(VendedCredentialTlsFixture { mut rest, proxy }) = fixture else {
            return Ok(());
        };
        drop(proxy);
        rest.shutdown()
            .context("shutdown isolated vended TLS gate REST fixture")
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
    let valid_heartbeat = raw_grpc_probe(
        context
            .handle()
            .native_probe_connector(endpoint.clone(), mode)?,
        HEARTBEAT_PATH,
        Some(&authorization),
        Some(&[0, 0, 0, 0, 2, 0x08, 0x01]),
    )?;
    ensure!(
        valid_heartbeat.grpc_status != Some(GRPC_UNAUTHENTICATED),
        "valid JWT must reach representative Native RPC dispatch, got {valid_heartbeat:?}"
    );
    Ok(())
}

/// Confirms one acceptance query really crossed the trust boundary and
/// finished there.
///
/// Named for the boundary rather than for a lifecycle, because the retired
/// protocol that owned that word is not what carries these queries any more.
///
/// This scenario is about JWT and TLS, not about lifecycle bookkeeping: the
/// retired assertions here -- a non-empty participant outcome list, no error
/// source, and every outcome a positive proof -- were only how it confirmed
/// that a query had run end to end over the transport under test. None of the
/// three has a subject on the task protocol:
///
/// * Participant outcomes are gone as a concept. The task protocol mints no
///   `ParticipantTerminalOutcome`, and the frontend publishes an empty list
///   rather than fabricating proofs (ADR-0135), so both the "not empty" and
///   the "all proofs" assertions are unsatisfiable rather than merely false.
/// * `error_source` is structurally `None` here. A task round publishes its
///   convergence evidence only after the client-visible answer is already
///   linearized as a success -- a failed attempt aborts its contexts and
///   publishes nothing -- so the assertion could not fail, and an assertion
///   that cannot fail is worse than no assertion.
///
/// What replaces them is the same confirmation built from the task protocol's
/// own evidence, and it is deliberately stronger in one respect: it is scoped
/// to this query's execution identity, where the retired backend-log
/// assertions matched any query in the run.
fn assert_query_crossed_trust_boundary(
    context: &mut ScenarioContext,
    snapshot: &QueryLifecycleStructuredSnapshot,
    subject: &str,
) -> Result<BTreeSet<usize>> {
    task_evidence::assert_query_completed_across_boundary(context, snapshot, subject)
}

#[derive(Debug)]
struct GrpcProbe {
    http_status: u16,
    grpc_status: Option<u16>,
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
    tokio::runtime::Runtime::new()
        .context("create Native raw probe runtime")?
        .block_on(async move {
            let stream = connector
                .connect()
                .await
                .map_err(anyhow::Error::msg)
                .context("connect Native raw probe")?;
            let (mut sender, connection) = client::handshake(stream)
                .await
                .context("perform Native raw probe HTTP/2 handshake")?;
            let driver = tokio::spawn(async move { connection.await });
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
            let mut body = response.into_body();
            while body
                .data()
                .await
                .transpose()
                .context("read Native raw probe body")?
                .is_some()
            {}
            let trailer_status = body
                .trailers()
                .await
                .context("read Native raw probe trailers")?
                .as_ref()
                .and_then(grpc_status);
            // Native listeners are long-lived. The probe has received the
            // complete response, so waiting for the peer to close would turn
            // a successful keep-alive into a scenario timeout.
            driver.abort();
            let _ = driver.await;
            Ok(GrpcProbe {
                http_status,
                grpc_status: header_status.or(trailer_status),
            })
        })
}

fn grpc_status(headers: &http::HeaderMap) -> Option<u16> {
    headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
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
                "native-trust/vended-credential-tls-gate",
                "native-trust/vended-credential-tls-gate-automatic",
                "native-trust/vended-credential-tls-gate-pem",
            ]
        );
    }
}
