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
use novarocks_proto_models::{common, novarocks as proto, plan};
use novarocks_types::identity::{BackendProcessId, FrontendProcessId};
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
        Box::new(RawEstablishCompatibilityAdmission),
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
struct RawEstablishCompatibilityAdmission;
struct IslandDrainAndReplacement;
struct TargetIslandCutover;

impl Scenario for RawEstablishCompatibilityAdmission {
    fn name(&self) -> &'static str {
        "native-compatibility/raw-establish-compatibility-admission"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let target_port = context.handle().runtime().be[0].grpc;
        let rows = context.handle().frontend_backend_topology()?;
        let target = rows
            .iter()
            .find(|row| row.grpc_port == target_port)
            .context("SHOW BACKENDS omitted raw Establish target BE")?;
        ensure!(
            target.is_eligible_live(),
            "raw Establish target BE must be eligible and live, row={target:?}"
        );
        let backend = target
            .process_id
            .parse::<BackendProcessId>()
            .context("parse target backend process identity")?;
        let native_compatibility_id = decode_hex_32(&target.native_compatibility_id)?;
        let query_context = raw_query_context(backend);
        let endpoint = context.handle().native_be_endpoint(0)?;
        let mode = context.handle().native_trust_mode();
        let connector = context.handle().native_probe_connector(endpoint, mode)?;
        let trust = context.handle().native_probe_trust()?;
        let authorization = authorization_header(&trust)?;
        let establish_applied_before = context
            .handle()
            .be_log_count(0, super::task_evidence::CONTEXT_ESTABLISH_APPLIED)?;

        for (label, compatibility_id) in [
            ("missing", None),
            ("31-byte", Some(vec![0xa1; 31])),
            ("33-byte", Some(vec![0xa2; 33])),
        ] {
            let response = raw_apply_task_operations(
                &connector,
                &authorization,
                vec![raw_establish(query_context.clone(), compatibility_id)],
            )?;
            ensure!(
                response.grpc_status == tonic::Code::InvalidArgument as u16,
                "{label} native compatibility identity must return invalid_argument, got status={} message={:?}",
                response.grpc_status,
                response.grpc_message
            );
            ensure!(
                response.message.is_none(),
                "{label} malformed Establish must not return an operation receipt"
            );
        }
        context.action(
            "raw Establish rejected missing, 31-byte, and 33-byte compatibility identities as invalid_argument before dispatch",
        );

        let mut foreign = native_compatibility_id;
        foreign[0] ^= 0xff;
        let foreign_response = raw_apply_task_operations(
            &connector,
            &authorization,
            vec![raw_establish(query_context.clone(), Some(foreign.to_vec()))],
        )?;
        let foreign_receipt = only_successful_receipt(foreign_response, "foreign Establish")?;
        ensure!(
            proto::TaskOperationOutcome::try_from(foreign_receipt.outcome)
                == Ok(proto::TaskOperationOutcome::CompatibilityMismatch),
            "foreign Establish must return CompatibilityMismatch, got {foreign_receipt:?}"
        );
        ensure!(
            foreign_receipt.ack.is_none(),
            "compatibility mismatch must not carry an acknowledgement"
        );
        ensure!(
            context
                .handle()
                .be_log_count(0, super::task_evidence::CONTEXT_ESTABLISH_APPLIED)?
                == establish_applied_before,
            "foreign Establish must not emit an applied marker"
        );

        let create_response = raw_apply_task_operations(
            &connector,
            &authorization,
            vec![raw_create_task(query_context.clone())],
        )?;
        let create_receipt =
            only_successful_receipt(create_response, "CreateTask after foreign Establish")?;
        ensure!(
            proto::TaskOperationOutcome::try_from(create_receipt.outcome)
                == Ok(proto::TaskOperationOutcome::OperationTimedOut),
            "CreateTask after foreign Establish must time out on the context creation gate, got {create_receipt:?}"
        );
        ensure!(
            create_receipt.ack.is_none(),
            "CreateTask against an absent context must not carry an acknowledgement"
        );
        ensure!(
            context
                .handle()
                .be_log_count(0, super::task_evidence::CONTEXT_ESTABLISH_APPLIED)?
                == establish_applied_before,
            "refused CreateTask must not establish the context"
        );
        context.action(
            "foreign 32-byte compatibility identity returned CompatibilityMismatch; a following valid CreateTask timed out on the untouched context creation gate",
        );

        let matching_response = raw_apply_task_operations(
            &connector,
            &authorization,
            vec![raw_establish(
                query_context,
                Some(native_compatibility_id.to_vec()),
            )],
        )?;
        let matching_receipt = only_successful_receipt(matching_response, "matching Establish")?;
        ensure!(
            proto::TaskOperationOutcome::try_from(matching_receipt.outcome)
                == Ok(proto::TaskOperationOutcome::Accepted),
            "matching Establish must be accepted, got {matching_receipt:?}"
        );
        ensure!(
            matches!(
                matching_receipt.ack,
                Some(proto::task_operation_receipt::Ack::QueryContext(_))
            ),
            "accepted matching Establish must carry a query-context acknowledgement"
        );
        ensure!(
            context
                .handle()
                .be_log_count(0, super::task_evidence::CONTEXT_ESTABLISH_APPLIED)?
                == establish_applied_before + 1,
            "matching Establish must emit exactly one applied marker"
        );
        context.action(
            "exact backend compatibility identity accepted the same raw query context and emitted the establish-applied marker",
        );
        Ok(())
    }
}

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

/// Proves the malformed-exchange raw-ingress cut owned by this backend.
///
/// Raw Establish compatibility admission has its own scenario because it must
/// also prove the registry stays untouched after a well-formed foreign-island
/// request.
fn assert_raw_ingress_hard_cuts(context: &mut ScenarioContext) -> Result<()> {
    let endpoint = context.handle().native_be_endpoint(2)?;
    let mode = context.handle().native_trust_mode();
    let connector = context.handle().native_probe_connector(endpoint, mode)?;
    let trust = context.handle().native_probe_trust()?;
    let authorization = authorization_header(&trust)?;
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
    context.action("malformed unknown Exchange route rejected before decode");
    Ok(())
}

fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
    ensure!(
        value.len() == 64,
        "native compatibility identity must be 64 hex characters, got {}",
        value.len()
    );
    let mut decoded = [0_u8; 32];
    for (index, byte) in decoded.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .with_context(|| format!("decode native compatibility byte {index}"))?;
    }
    Ok(decoded)
}

fn raw_query_context(backend: BackendProcessId) -> proto::QueryContextRef {
    proto::QueryContextRef {
        query_execution_id: Some(proto::QueryExecutionId {
            query_id: Some(common::UniqueId { hi: 91, lo: 92 }),
            attempt_id: 1,
        }),
        frontend_process_id: Some(proto::FrontendProcessId {
            value: FrontendProcessId::new_v7().to_bytes().to_vec(),
        }),
        backend_process_id: Some(proto::BackendProcessId {
            value: backend.to_bytes().to_vec(),
        }),
    }
}

fn raw_operation_envelope(max_wait_millis: u64) -> proto::TaskOperationEnvelope {
    proto::TaskOperationEnvelope {
        operation_id: Some(proto::TaskOperationId {
            value: FrontendProcessId::new_v7().to_bytes().to_vec(),
        }),
        max_wait_millis,
    }
}

fn raw_query_options() -> proto::QueryOptions {
    proto::QueryOptions {
        pipeline_dop: 2,
        ..Default::default()
    }
}

fn raw_establish(
    query_context: proto::QueryContextRef,
    native_compatibility_id: Option<Vec<u8>>,
) -> proto::TaskOperation {
    proto::TaskOperation {
        envelope: Some(raw_operation_envelope(15_000)),
        operation: Some(proto::task_operation::Operation::UpdateQueryContext(
            proto::UpdateQueryContextRequest {
                command: Some(proto::update_query_context_request::Command::Establish(
                    proto::EstablishQueryContextRequest {
                        query_context: Some(query_context),
                        catalog_set: Some(Default::default()),
                        initial_runtime_filter: Some(Default::default()),
                        initial_credential: Some(proto::QueryContextCredentialDomain {
                            lease_id: 1,
                            epoch: 1,
                            descriptors: Vec::new(),
                            envelopes: Vec::new(),
                        }),
                        initial_lease: Some(proto::QueryExecutionLeaseGrant {
                            sequence: 0,
                            valid_for_millis: 30_000,
                        }),
                        query_options: Some(raw_query_options()),
                        native_compatibility_id: native_compatibility_id
                            .map(|value| proto::NativeCompatibilityId { value }),
                    },
                )),
            },
        )),
    }
}

fn raw_create_task(query_context: proto::QueryContextRef) -> proto::TaskOperation {
    let execution = query_context
        .query_execution_id
        .clone()
        .expect("raw query context carries an execution identity");
    let backend = query_context
        .backend_process_id
        .clone()
        .expect("raw query context carries a backend identity");
    let query_id = execution
        .query_id
        .expect("raw execution identity carries a query id");
    let fragment_instance_id = common::UniqueId { hi: 93, lo: 94 };
    proto::TaskOperation {
        envelope: Some(raw_operation_envelope(50)),
        operation: Some(proto::task_operation::Operation::CreateTask(
            proto::CreateTaskRequest {
                query_context: Some(query_context),
                descriptor: Some(proto::TaskDescriptor {
                    identity: Some(proto::TaskIdentity {
                        query_execution_id: Some(execution),
                        stage_id: 1,
                        task_id: 1,
                        backend_process_id: Some(backend),
                    }),
                    fragment_instance_id: Some(fragment_instance_id),
                    pipeline_dop: 2,
                    split_plan_nodes: Vec::new(),
                    topology: Some(Default::default()),
                    fragment: Some(proto::TaskFragmentPlan {
                        plan: Some(plan::PlanFragment {
                            fragment_id: 1,
                            sink: Some(plan::DataSink {
                                kind: Some(plan::data_sink::Kind::Result(true)),
                            }),
                            ..Default::default()
                        }),
                        instance_params: Some(proto::InstanceParams {
                            query_id: Some(query_id),
                            fragment_instance_id: Some(fragment_instance_id),
                            query_options: Some(raw_query_options()),
                            typed_result_sink: true,
                            ..Default::default()
                        }),
                    }),
                }),
                initial_domains: Vec::new(),
            },
        )),
    }
}

fn only_successful_receipt(
    response: RawUnaryResponse<proto::ApplyTaskOperationsResponse>,
    subject: &str,
) -> Result<proto::TaskOperationReceipt> {
    ensure!(
        response.grpc_status == tonic::Code::Ok as u16,
        "{subject} returned grpc-status={} message={:?}",
        response.grpc_status,
        response.grpc_message
    );
    let mut receipts = response
        .message
        .with_context(|| format!("{subject} returned no response message"))?
        .receipts;
    ensure!(
        receipts.len() == 1,
        "{subject} must return exactly one receipt, got {}",
        receipts.len()
    );
    Ok(receipts.remove(0))
}

fn raw_apply_task_operations(
    connector: &NativeEndpointConnector,
    authorization: &str,
    operations: Vec<proto::TaskOperation>,
) -> Result<RawUnaryResponse<proto::ApplyTaskOperationsResponse>> {
    raw_unary_response(
        connector,
        "/novarocks.NovaRocksGrpc/ApplyTaskOperations",
        authorization,
        proto::ApplyTaskOperationsRequest { operations },
    )
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
    let response = raw_unary_response(&connector, path, authorization, message)?;
    ensure!(
        response.grpc_status == tonic::Code::Ok as u16,
        "raw RPC returned non-OK grpc-status={} message={:?}",
        response.grpc_status,
        response.grpc_message
    );
    response
        .message
        .context("raw RPC returned no response message")
}

struct RawUnaryResponse<R> {
    grpc_status: u16,
    grpc_message: Option<String>,
    message: Option<R>,
}

fn raw_unary_response<M: Message, R: Message + Default>(
    connector: &NativeEndpointConnector,
    path: &str,
    authorization: &str,
    message: M,
) -> Result<RawUnaryResponse<R>> {
    let mut payload = Vec::new();
    message.encode(&mut payload)?;
    let mut frame = Vec::with_capacity(payload.len() + 5);
    frame.push(0);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    let connector = connector.clone();
    let path = path.to_owned();
    let authorization = authorization.to_owned();
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
        let header_message = grpc_message(response.headers());
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.data().await {
            bytes.extend_from_slice(&chunk?);
        }
        let trailers = body.trailers().await?;
        let status = header_status
            .or_else(|| trailers.as_ref().and_then(grpc_status))
            .context("raw RPC omitted grpc-status")?;
        let message = header_message.or_else(|| trailers.as_ref().and_then(grpc_message));
        driver.abort();
        let _ = driver.await;
        let decoded = if bytes.is_empty() {
            None
        } else {
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
            Some(R::decode(&bytes[5..]).context("decode raw gRPC response")?)
        };
        Ok(RawUnaryResponse {
            grpc_status: status,
            grpc_message: message,
            message: decoded,
        })
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
