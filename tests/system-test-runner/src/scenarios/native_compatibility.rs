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

//! Real process-boundary compatibility-island admission scenarios.

use crate::actors::mysql as mysql_actor;
use crate::scenario::{
    Scenario, ScenarioBinary, ScenarioBinaryLayout, ScenarioContext, ScenarioLaunchConfig,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use h2::client;
use http::{Request, header};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;
use novarocks_native_trust::{NativeEndpointConnector, NativeTrust};
use novarocks_proto_models::{catalog, common, novarocks as proto};
use novarocks_types::BackendProcessId;
use novarocks_version::{
    NativeCarrierDeclaration, derive_repository_native_compatibility_material,
};
use prost::Message;
use std::collections::BTreeSet;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const REQUIRED_BACKENDS: usize = 3;
const BASELINE_QUERY: &str = "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(MixedBuildSameIsland),
        Box::new(OtherIslandHardCut),
        Box::new(IslandDrainAndReplacement),
        Box::new(TargetIslandCutover),
    ]
}

struct MixedBuildSameIsland;

impl Scenario for MixedBuildSameIsland {
    fn name(&self) -> &'static str {
        "native-compatibility/mixed-build-same-island"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            binary_layout: ScenarioBinaryLayout {
                frontend: ScenarioBinary::Primary,
                backends: vec![
                    ScenarioBinary::Primary,
                    ScenarioBinary::Compatible,
                    ScenarioBinary::Compatible,
                ],
            },
            expected_eligible_backend_count: Some(REQUIRED_BACKENDS),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let rows = context.handle().frontend_backend_topology()?;
        let live = rows
            .iter()
            .filter(|row| row.is_eligible_live())
            .collect::<Vec<_>>();
        ensure!(
            live.len() == REQUIRED_BACKENDS,
            "same-island launch expected 3 eligible BEs, rows={rows:?}"
        );
        let build_ids = live
            .iter()
            .map(|row| row.build_identity.as_str())
            .collect::<BTreeSet<_>>();
        let compatibility_ids = live
            .iter()
            .map(|row| row.native_compatibility_id.as_str())
            .collect::<BTreeSet<_>>();
        ensure!(
            build_ids.len() == 2,
            "same island must admit two BuildIdentity values, got {build_ids:?}"
        );
        ensure!(
            compatibility_ids.len() == 1,
            "same island must retain one compatibility ID, got {compatibility_ids:?}"
        );
        assert_island_ready(context, 200)?;
        run_distributed_queries(context, &[0, 1, 2])?;
        context.action(format!("admitted 3 BEs with builds={build_ids:?} inside compatibility_ids={compatibility_ids:?}"));
        Ok(())
    }
}

struct OtherIslandHardCut;
struct IslandDrainAndReplacement;
struct TargetIslandCutover;

impl Scenario for OtherIslandHardCut {
    fn name(&self) -> &'static str {
        "native-compatibility/other-island-hard-cut"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            binary_layout: ScenarioBinaryLayout {
                frontend: ScenarioBinary::Primary,
                backends: vec![
                    ScenarioBinary::Primary,
                    ScenarioBinary::Compatible,
                    ScenarioBinary::OtherIsland,
                ],
            },
            expected_eligible_backend_count: Some(2),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let rows = context.handle().frontend_backend_topology()?;
        let live = rows
            .iter()
            .filter(|row| row.is_eligible_live())
            .collect::<Vec<_>>();
        ensure!(
            live.len() == 2,
            "mixed-island launch expected 2 eligible BEs, rows={rows:?}"
        );
        let other = rows
            .iter()
            .filter(|row| !row.is_eligible_live())
            .collect::<Vec<_>>();
        ensure!(
            other.len() == 1,
            "mixed-island launch expected exactly one excluded BE, rows={rows:?}"
        );
        ensure!(
            other[0]
                .status_detail
                .contains("other compatibility island"),
            "excluded BE must explain OtherIsland, row={:?}",
            other[0]
        );
        assert_island_ready(context, 200)?;
        run_distributed_queries(context, &[0, 1])?;
        context
            .handle()
            .assert_be_log(2, super::task_evidence::CONTEXT_ESTABLISH_APPLIED)
            .expect_err("OtherIsland BE must never be given a query context by the FE");
        assert_raw_ingress_hard_cuts(context)?;
        context.action(
            "excluded epoch-2 BE remained OtherIsland while SQL admitted only compatible BEs",
        );
        Ok(())
    }
}

impl Scenario for TargetIslandCutover {
    fn name(&self) -> &'static str {
        "native-compatibility/target-island-cutover"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            binary_layout: ScenarioBinaryLayout {
                frontend: ScenarioBinary::Primary,
                backends: vec![
                    ScenarioBinary::Primary,
                    ScenarioBinary::Compatible,
                    ScenarioBinary::Compatible,
                ],
            },
            expected_eligible_backend_count: Some(REQUIRED_BACKENDS),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        assert_island_ready(context, 200)?;
        let mut target = context.launch_peer_cluster(
            "target-island",
            ScenarioLaunchConfig {
                binary_layout: ScenarioBinaryLayout {
                    frontend: ScenarioBinary::OtherIsland,
                    backends: vec![ScenarioBinary::OtherIsland; REQUIRED_BACKENDS],
                },
                expected_eligible_backend_count: Some(REQUIRED_BACKENDS),
                ..Default::default()
            },
        )?;
        let result = (|| -> Result<()> {
            let target_island =
                target.frontend_management_get("/island-readyz", Duration::from_secs(5))?;
            ensure!(
                target_island.status == 200,
                "target island is not ready: {target_island:?}"
            );
            let source_user = context.mysql_user().to_string();
            let source_port = context.mysql_port();
            let (sender, receiver) = mpsc::sync_channel(1);
            thread::spawn(move || {
                let result = (|| -> Result<Vec<i64>> {
                    let mut connection =
                        mysql_actor::connect(&source_user, source_port, Duration::from_secs(30))?;
                    connection
                        .query("SELECT sleep(2)")
                        .context("run source pre-cutover query")
                })();
                let _ = sender.send(result);
            });
            thread::sleep(Duration::from_millis(200));
            context
                .handle()
                .begin_fe_drain()
                .context("begin source FE drain after target island readiness")?;
            let target_rows: Vec<i64> = mysql_actor::connect(
                target.mysql_user(),
                target.runtime().fe_mysql_port,
                context.remaining("connect target post-cutover client")?,
            )?
            .query(BASELINE_QUERY)
            .context("run new query on target island")?;
            ensure!(
                target_rows == vec![1, 2],
                "target island query returned {target_rows:?}"
            );
            let source_rows = receiver
                .recv_timeout(context.remaining("wait for source pre-cutover query")?)
                .map_err(|error| {
                    anyhow::anyhow!("source pre-cutover query did not finish: {error}")
                })??;
            ensure!(
                source_rows.len() == 1,
                "source pre-cutover query returned {source_rows:?}"
            );
            let deadline = context.deadline();
            context.handle().wait_fe_exit_until(deadline)?;
            context.action("target island became ready before source drain; new query went to target while source attempt completed locally");
            Ok(())
        })();
        let cleanup = ServerHandle::shutdown(&mut target);
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Err(error), Err(cleanup)) => {
                Err(error.context(format!("target cleanup also failed: {cleanup:#}")))
            }
        }
    }
}

impl Scenario for IslandDrainAndReplacement {
    fn name(&self) -> &'static str {
        "native-compatibility/island-drain-and-replacement"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            binary_layout: ScenarioBinaryLayout {
                frontend: ScenarioBinary::Primary,
                backends: vec![
                    ScenarioBinary::Primary,
                    ScenarioBinary::Compatible,
                    ScenarioBinary::OtherIsland,
                ],
            },
            expected_eligible_backend_count: Some(2),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        context
            .handle()
            .begin_be_drain(0)
            .context("begin SIGTERM drain for first compatible BE")?;
        wait_for_eligible_count(context, 1)?;
        context
            .handle()
            .begin_be_drain(1)
            .context("begin SIGTERM drain for second compatible BE")?;
        wait_for_eligible_count(context, 0)?;
        assert_base_and_island_readiness(context, 200, 503)?;
        context.action("drained both compatible BEs: base readiness remained true while island readiness became false");

        let compatible = context.compatible_binary()?;
        let deadline = context.deadline();
        context
            .handle()
            .restart_be_with_binary_until(2, compatible, 1, deadline)
            .context("replace OtherIsland BE with compatible binary")?;
        wait_for_eligible_count(context, 1)?;
        assert_base_and_island_readiness(context, 200, 200)?;
        run_distributed_queries(context, &[2])?;
        context.action("replaced the OtherIsland BE with a same-island binary and recovered distributed query admission");
        Ok(())
    }
}

fn assert_raw_ingress_hard_cuts(context: &mut ScenarioContext) -> Result<()> {
    let endpoint = context.handle().native_be_endpoint(2)?;
    let mode = context.handle().native_trust_mode();
    let connector = context.handle().native_probe_connector(endpoint, mode)?;
    let trust = context.handle().native_probe_trust()?;
    let authorization = authorization_header(&trust)?;
    let material = derive_repository_native_compatibility_material([
        NativeCarrierDeclaration::try_new("iceberg", 1)?,
        NativeCarrierDeclaration::try_new("starrocks", 1)?,
    ])?;
    let init = proto::InitQueryRequest {
        credential_lease_envelopes: vec![],
        manifest: Some(proto::ParticipantManifest {
            execution_id: Some(proto::QueryExecutionId {
                query_id: Some(common::UniqueId { hi: 1, lo: 1 }),
                attempt_id: 1,
            }),
            backend: Some(proto::ParticipantBackendIdentity {
                endpoint: Some(proto::QueryControlEndpoint {
                    host: "127.0.0.1".to_string(),
                    port: 1,
                }),
                process_id: Some(proto::BackendProcessId {
                    value: BackendProcessId::new_v7().to_bytes().to_vec(),
                }),
            }),
            native_compatibility_id: Some(proto::NativeCompatibilityId {
                value: material.id().as_bytes().to_vec(),
            }),
            expected_fragment_instance_ids: vec![common::UniqueId { hi: 2, lo: 2 }],
            query_options: Some(proto::QueryOptions::default()),
            query_deadline_unix_ms: 1,
            pre_start_timeout_ms: 1,
            report_endpoint: Some(proto::QueryControlEndpoint {
                host: "127.0.0.1".to_string(),
                port: 1,
            }),
            catalog_set: Some(catalog::CatalogSet::default()),
            ..Default::default()
        }),
    };
    let init_response: proto::InitQueryResponse = raw_unary(
        connector.clone(),
        "/novarocks.NovaRocksGrpc/InitQuery",
        &authorization,
        init,
    )?;
    ensure!(
        init_response.outcome
            == proto::QueryInitOutcome::QueryInitRejectedCompatibilityMismatch as i32,
        "raw InitQuery must receive typed compatibility mismatch, got {:?}",
        init_response
    );
    let exchange = proto::ExchangeRequest {
        finst_id_hi: 11,
        finst_id_lo: 12,
        node_id: 0,
        sender_id: 0,
        be_number: 0,
        eos: true,
        sequence: 1,
        payload: vec![0xff],
        source_finst_id_hi: 21,
        source_finst_id_lo: 22,
        sender_ordinal: 0,
        sender_count: 1,
    };
    let exchange_response: proto::ExchangeResponse = raw_unary(
        connector,
        "/novarocks.NovaRocksGrpc/ExchangeUnary",
        &authorization,
        exchange,
    )?;
    let status = exchange_response
        .status
        .context("ExchangeUnary response missing status")?;
    ensure!(
        status.code != 0 && status.message.contains("exchange ingress route rejected"),
        "unknown malformed Exchange must reject before decode, got {status:?}"
    );
    context.action("authenticated raw InitQuery returned typed compatibility mismatch and malformed unknown Exchange route rejected before decode");
    Ok(())
}

fn authorization_header(trust: &NativeTrust) -> Result<String> {
    let mut request = tonic::Request::new(());
    trust
        .apply_client_authorization(request.metadata_mut())
        .map_err(anyhow::Error::msg)?;
    request
        .metadata()
        .get("authorization")
        .context("Native trust omitted authorization")?
        .to_str()
        .context("Native trust authorization is not ASCII")
        .map(ToOwned::to_owned)
}

fn raw_unary<M: Message, R: Message + Default>(
    connector: NativeEndpointConnector,
    path: &str,
    authorization: &str,
    message: M,
) -> Result<R> {
    let mut payload = Vec::new();
    message.encode(&mut payload)?;
    let mut frame = Vec::with_capacity(payload.len() + 5);
    frame.push(0);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    tokio::runtime::Runtime::new()?.block_on(async move {
        let stream = connector.connect().await.map_err(anyhow::Error::msg)?;
        let (mut sender, connection) = client::handshake(stream).await?;
        let driver = tokio::spawn(async move { connection.await });
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header(header::CONTENT_TYPE, "application/grpc")
            .header("te", "trailers")
            .header(header::AUTHORIZATION, authorization)
            .body(())?;
        let (response, mut send_stream) = sender.send_request(request, false)?;
        send_stream.send_data(Bytes::from(frame), true)?;
        let response = response.await?;
        ensure!(
            response.status().as_u16() == 200,
            "raw RPC returned HTTP {}",
            response.status()
        );
        let header_status = grpc_status(response.headers());
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.data().await {
            bytes.extend_from_slice(&chunk?);
        }
        let trailers = body.trailers().await?;
        ensure!(
            header_status.or_else(|| trailers.as_ref().and_then(grpc_status)) == Some(0),
            "raw RPC returned non-OK grpc-status header={header_status:?} trailers={trailers:?}"
        );
        driver.abort();
        let _ = driver.await;
        ensure!(
            bytes.len() >= 5 && bytes[0] == 0,
            "raw RPC response lacks uncompressed gRPC frame"
        );
        let length =
            u32::from_be_bytes(bytes[1..5].try_into().expect("frame header width")) as usize;
        ensure!(
            bytes.len() == length + 5,
            "raw RPC response frame length mismatch"
        );
        R::decode(&bytes[5..]).context("decode raw gRPC response")
    })
}

fn grpc_status(headers: &http::HeaderMap) -> Option<u16> {
    headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    ensure!(
        context.handle().be_count() == REQUIRED_BACKENDS,
        "{} requires 1FE+3BE",
        context.name()
    );
    Ok(())
}

fn assert_island_ready(context: &mut ScenarioContext, expected_status: u16) -> Result<()> {
    let timeout = context.remaining("query island readiness")?;
    let response = context
        .handle()
        .frontend_management_get("/island-readyz", timeout)?;
    ensure!(
        response.status == expected_status,
        "/island-readyz expected {expected_status}, got {} body={}",
        response.status,
        response.body
    );
    Ok(())
}

fn assert_base_and_island_readiness(
    context: &mut ScenarioContext,
    base_status: u16,
    island_status: u16,
) -> Result<()> {
    let timeout = context.remaining("query FE readiness")?;
    let base = context
        .handle()
        .frontend_management_get("/readyz", timeout)?;
    ensure!(
        base.status == base_status,
        "/readyz expected {base_status}, got {}",
        base.status
    );
    assert_island_ready(context, island_status)
}

fn wait_for_eligible_count(context: &mut ScenarioContext, expected: usize) -> Result<()> {
    loop {
        let rows = context.handle().frontend_backend_topology()?;
        if rows.iter().filter(|row| row.is_eligible_live()).count() == expected {
            return Ok(());
        }
        if context
            .remaining("wait for compatibility island count")
            .is_err()
        {
            anyhow::bail!(
                "timed out waiting for compatible eligible backend count {expected}; rows={rows:?}"
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

fn run_distributed_queries(
    context: &mut ScenarioContext,
    expected_backends: &[usize],
) -> Result<()> {
    let mut connection = mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining("connect compatibility-island MySQL client")?,
    )?;
    for ordinal in 1..=3 {
        let rows: Vec<i64> = connection
            .query(BASELINE_QUERY)
            .with_context(|| format!("run compatibility-island query {ordinal}"))?;
        ensure!(
            rows == vec![1, 2],
            "compatibility-island query {ordinal} returned {rows:?}"
        );
    }
    for &backend in expected_backends {
        context
            .handle()
            .assert_be_log(backend, super::task_evidence::CONTEXT_ESTABLISH_APPLIED)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_status_accepts_initial_response_headers() {
        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", http::HeaderValue::from_static("0"));

        assert_eq!(grpc_status(&headers), Some(0));
    }
}
