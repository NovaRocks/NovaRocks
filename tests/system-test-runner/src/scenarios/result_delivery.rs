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

use super::result_delivery_support::await_idle;
use crate::actors::mysql_stream::{AsyncMysqlStream, TextColumnObservation, TextResultObservation};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use crate::scenarios::native_sql_controls::{SqlIntentOwner, StatementSpec};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{QueryLifecycleStatementObservation, TestClusterOwner};
use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason, domain};
use novarocks_proto_models::{novarocks as proto, result as result_proto};
use prost::Message;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    topology: String,
    segment_bytes: u64,
    mysql_u24_payload_bytes: u64,
    mysql_client_max_packet_bytes: u32,
    scope: String,
    cases: Vec<WireCase>,
}

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct WireCase {
    name: String,
    sql: String,
    expected_columns: u64,
    expected_schema: Vec<TextColumnObservation>,
    expected_rows: u64,
    expected_row_payload_bytes: u64,
    expected_packets: u64,
    expected_row_sha256: String,
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(WireBoundary(
            "result-delivery/many-small-rows-cross-segment",
        )),
        Box::new(WireBoundary("result-delivery/large-row-cross-u24")),
        Box::new(RootReadRefusal),
        Box::new(ParticipationRootRetention::new(RootProtocolMode::None)),
        Box::new(ParticipationRootRetention::new(RootProtocolMode::ZeroAck)),
        Box::new(ParticipationRootRetention::new(RootProtocolMode::FinalAck)),
        Box::new(ParticipationRootRetention::new(RootProtocolMode::OneByte)),
        Box::new(ParticipationRootRetention::new(
            RootProtocolMode::MinimumNewRow,
        )),
    ]
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RefusalManifest {
    schema_version: u32,
    topology: String,
    scope: String,
    path: String,
    cases: Vec<RefusalCase>,
    health: HealthQuery,
    identity_refusal: IdentityRefusalFreeze,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityRefusalFreeze {
    class: String,
    domain: u16,
    reason: u16,
    evidence: String,
    safe_detail: String,
    ack_receipt: bool,
    resource_facts: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, serde::Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum RefusalResponseContract {
    TypedIdentityRefusal,
    MalformedNoBody,
}

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct RefusalCase {
    name: String,
    profile_id: u32,
    kind: String,
    wanted_sequence: u64,
    grpc_status: u16,
    response_contract: RefusalResponseContract,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthQuery {
    sql: String,
    expected_row_sha256: String,
}

fn foreign_root_task() -> proto::TaskIdentity {
    proto::TaskIdentity {
        query_execution_id: Some(proto::QueryExecutionId {
            query_id: Some(novarocks_proto_models::common::UniqueId { hi: 1, lo: 2 }),
            attempt_id: 1,
        }),
        stage_id: 1,
        task_id: 1,
        // Deliberately foreign, but structurally valid. A valid V1 read
        // must reach exact-process refusal; each malformed field below
        // must instead be rejected before that same route lookup.
        backend_process_id: Some(proto::BackendProcessId {
            value: novarocks_types::BackendProcessId::new_v7()
                .to_bytes()
                .to_vec(),
        }),
    }
}

fn refusal_request(
    case: &RefusalCase,
    root_task: &proto::TaskIdentity,
) -> Result<proto::FetchRootResultRequest> {
    use result_proto::root_output_kind::Kind;
    let kind = match case.kind.as_str() {
        "client_rows_true" => Some(Kind::ClientRows(true)),
        "client_rows_false" => Some(Kind::ClientRows(false)),
        "absent" => None,
        "unknown_domain_999" => Some(Kind::InternalFacts(999)),
        _ => anyhow::bail!("unknown frozen root refusal case"),
    };
    Ok(proto::FetchRootResultRequest {
        root_task: Some(root_task.clone()),
        profile_id: case.profile_id,
        output_kind: kind.map(|kind| result_proto::RootOutputKind { kind: Some(kind) }),
        wanted_sequence: Some(case.wanted_sequence),
        consumed_sequence: 0,
        max_wait_millis: 100,
    })
}

// This independent response oracle does not call the Root response decoder.
// Transport status alone cannot prove the owning channel's typed refusal.
fn assert_root_refusal_response(
    case: &RefusalCase,
    request: &proto::FetchRootResultRequest,
    response: &super::native_trust::GrpcProbe,
) -> Result<()> {
    ensure!(
        response.http_status == 200 && response.grpc_status == Some(case.grpc_status),
        "root refusal transport status differs from its frozen case"
    );
    let body = response
        .response_body
        .as_deref()
        .context("root refusal requires the original bounded response body observation")?;
    ensure!(
        body.len() <= 4096,
        "root refusal response exceeds the original probe cap"
    );
    match case.response_contract {
        RefusalResponseContract::MalformedNoBody => {
            ensure!(
                case.grpc_status == 3 && body.is_empty(),
                "malformed request must return grpc3 without a business response"
            );
        }
        RefusalResponseContract::TypedIdentityRefusal => {
            ensure!(
                case.grpc_status == 0,
                "a legal request uses its typed owner refusal"
            );
            ensure!(
                body.len() >= 5 && body[0] == 0,
                "root refusal requires one complete uncompressed gRPC frame"
            );
            let length = u32::from_be_bytes(body[1..5].try_into()?) as usize;
            ensure!(
                body[5..].len() == length,
                "root refusal has a truncated or additional frame"
            );
            let decoded = proto::FetchRootResultResponse::decode(&body[5..])?;
            ensure!(
                decoded.root_task == request.root_task
                    && decoded.profile_id == request.profile_id
                    && decoded.output_kind == request.output_kind,
                "typed refusal changed the original exact root/profile/purpose binding"
            );
            let Some(proto::fetch_root_result_response::Result::Refused(refused)) = decoded.result
            else {
                anyhow::bail!(
                    "legal foreign-root request must return Refused, without a reply/ACK or deferral"
                );
            };
            let fact = novarocks_task_codec::status::decode_failure_fact(
                refused
                    .fact
                    .as_ref()
                    .context("typed root refusal omitted its domain fact")?,
                novarocks_proto_codec::FieldPath::root("independent_root_refusal").field("fact"),
            )?;
            let expected = FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(43).unwrap()),
            );
            ensure!(
                fact == expected,
                "owning channel identity refusal lost its exact originating fact or invented resources: {fact:?}"
            );
            ensure!(
                refused.safe_detail == "root read was refused by its owning channel",
                "owning channel refusal changed its fixed safe description"
            );
        }
    }
    Ok(())
}

struct RootReadRefusal;

impl Scenario for RootReadRefusal {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "result-delivery/root-read-profile-kind-refusal"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "root refusal requires native 1FE+3BE"
        );
        let manifest: RefusalManifest = serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/root-read-refusal-freeze-v2.json"
        ))?;
        ensure!(
            manifest.schema_version == 2
                && manifest.topology == "1FE+3BE"
                && !manifest.scope.is_empty()
                && manifest.cases.len() == 7
                && manifest
                    .cases
                    .iter()
                    .filter(|case| case.response_contract
                        == RefusalResponseContract::TypedIdentityRefusal)
                    .count()
                    == 1
                && manifest.identity_refusal.class == "Data"
                && manifest.identity_refusal.domain == domain::WORKER.get()
                && manifest.identity_refusal.reason == 43
                && manifest.identity_refusal.evidence == "Originating"
                && manifest.identity_refusal.safe_detail
                    == "root read was refused by its owning channel"
                && !manifest.identity_refusal.ack_receipt
                && !manifest.identity_refusal.resource_facts
                && manifest.path == "/novarocks.NovaRocksGrpc/FetchRootResult",
            "unsupported root refusal manifest"
        );
        let epoch = Instant::now();
        await_idle(context, "root-refusal", "before", epoch)?;
        let root_task = foreign_root_task();
        let mut observations = Vec::new();
        for case in &manifest.cases {
            let request = refusal_request(case, &root_task)?;
            let payload = request.encode_to_vec();
            let mut frame = vec![0];
            frame.extend_from_slice(&u32::try_from(payload.len())?.to_be_bytes());
            frame.extend_from_slice(&payload);
            let response =
                super::native_trust::bounded_authenticated_probe(context, &manifest.path, &frame)?;
            let checked = assert_root_refusal_response(case, &request, &response);
            observations.push(serde_json::json!({"expected":case,"response":response,"request_bytes":frame.len()}));
            std::fs::write(
                context
                    .scenario_root()
                    .join("root-read-refusal-observations.json"),
                serde_json::to_vec_pretty(&observations)?,
            )?;
            ensure!(
                checked.is_ok(),
                "root read refusal differs from frozen case {}: {:?}",
                case.name,
                checked.err()
            );
        }
        let before = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<Vec<_>>>()?;
        let timeout = context
            .remaining("root refusal follow-up query")?
            .min(Duration::from_secs(10));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let health = runtime.block_on(async {
            let mut stream =
                AsyncMysqlStream::connect(context.mysql_user(), context.mysql_port(), timeout)
                    .await?;
            Ok::<_, anyhow::Error>(
                stream
                    .observe_text_query(&manifest.health.sql, Duration::ZERO)
                    .await,
            )
        })?;
        std::fs::write(
            context.scenario_root().join("root-refusal-follow-up.json"),
            serde_json::to_vec_pretty(&health)?,
        )?;
        ensure!(
            health.error.is_none()
                && health.rows == 1
                && health.row_payload_bytes == 5
                && health.packets == 5
                && health.row_sha256 == manifest.health.expected_row_sha256
                && health.schema
                    == [TextColumnObservation {
                        name: "total".to_owned(),
                        mysql_type: 8
                    }],
            "native follow-up query failed after root refusals"
        );
        let after = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            before
                .iter()
                .zip(&after)
                .all(|(a, b)| a.is_finite() && b.is_finite() && b >= a)
                && before.iter().zip(&after).any(|(a, b)| b > a),
            "follow-up never created a native task"
        );
        await_idle(context, "root-refusal", "after", epoch)?;
        context.record_phase_observation(
            "root-read-refusal",
            1,
            1,
            1,
            "authenticated-native-root",
            7,
            "passed",
            BTreeMap::from([("refusal_probes", 7), ("health_rows", health.rows)]),
        )?;
        context.action("verified seven frozen authenticated root request refusals, exact native follow-up rows and public owner convergence");
        Ok(())
    }
}

struct WireBoundary(&'static str);

impl Scenario for WireBoundary {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        self.0
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "wire boundary requires native 1FE+3BE"
        );
        let manifest: Manifest = serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/result-delivery-wire-boundary-v4.json"
        ))?;
        ensure!(
            manifest.schema_version == 1
                && manifest.topology == "1FE+3BE"
                && manifest.segment_bytes == 1_048_576
                && manifest.mysql_u24_payload_bytes == 0x00ff_ffff
                && manifest.mysql_client_max_packet_bytes == 67_108_864
                && !manifest.scope.is_empty()
                && manifest.cases.len() == 2,
            "unsupported frozen wire boundary manifest"
        );
        let case = manifest
            .cases
            .iter()
            .find(|case| case.name == self.name())
            .ok_or_else(|| anyhow::anyhow!("wire boundary case missing from frozen manifest"))?;
        ensure!(
            case.expected_row_payload_bytes > manifest.segment_bytes,
            "wire boundary must cross a native segment"
        );
        ensure!(
            case.expected_schema.len() as u64 == case.expected_columns,
            "frozen schema count mismatch"
        );
        let epoch = Instant::now();
        await_idle(context, "wire-boundary", "before", epoch)?;
        let before: Vec<_> = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<_>>()?;
        let timeout = context
            .remaining("wire boundary query")?
            .min(Duration::from_secs(30));
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let observation = runtime.block_on(async {
            let mut stream = AsyncMysqlStream::connect_with_max_packet_bytes(
                &user,
                port,
                timeout,
                manifest.mysql_client_max_packet_bytes,
            )
            .await?;
            Ok::<_, anyhow::Error>(stream.observe_text_query(&case.sql, Duration::ZERO).await)
        })?;
        std::fs::write(
            context
                .scenario_root()
                .join("wire-boundary-observation.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version":1,"expected":case,"observation":observation
            }))?,
        )?;
        ensure!(
            observation.error.is_none(),
            "wire query failed: {:?}",
            observation.error
        );
        ensure!(
            observation.columns == case.expected_columns
                && observation.schema == case.expected_schema
                && observation.rows == case.expected_rows,
            "wire result schema or row count disagrees with independent oracle"
        );
        ensure!(
            observation.row_payload_bytes == case.expected_row_payload_bytes
                && observation.packets == case.expected_packets,
            "wire payload length or U24 packet count disagrees with independent oracle"
        );
        ensure!(
            observation.row_sha256 == case.expected_row_sha256,
            "wire row bytes or row order disagree with independent oracle"
        );
        let after: Vec<_> = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<_>>()?;
        let mut created = 0_u64;
        for (before, after) in before.iter().zip(&after) {
            ensure!(
                before.is_finite() && after.is_finite() && after >= before,
                "invalid native task creation counter"
            );
            created += (after - before) as u64;
        }
        ensure!(
            created > 0,
            "wire query never executed a native backend task"
        );
        context.record_phase_observation(
            "wire-correctness",
            1,
            1,
            1,
            "public-mysql-native-root",
            1,
            "passed",
            BTreeMap::from([
                ("rows", observation.rows),
                ("row_payload_bytes", observation.row_payload_bytes),
                ("packets", observation.packets),
                ("native_tasks_created", created),
            ]),
        )?;
        context.action("verified native row bytes, order, packet sequence, one schema and terminal success against an independent frozen oracle");
        await_idle(context, "wire-boundary", "after", epoch)?;
        context.action("observed two consecutive idle FE governance/window and BE reservation/ingress snapshots after writer exit");
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionManifest {
    schema_version: u32,
    topology: String,
    scope: String,
    sql: String,
    segment_bytes: u64,
    native_row_bytes: u64,
    client_receive_buffer_bytes: u32,
    max_applied_receive_buffer_bytes: u32,
    mysql_client_max_packet_bytes: u32,
    query_observation_ms: u64,
    metadata_wait_ms: u64,
    held_phase_ms: u64,
    phase_sample_interval_ms: u64,
    phase_max_samples: usize,
    expected_columns: u64,
    expected_rows: u64,
    expected_row_payload_bytes: u64,
    expected_packets: u64,
    expected_schema: Vec<TextColumnObservation>,
    expected_row_sha256: String,
    #[serde(default)]
    expected_packet_sequences: Option<Vec<u8>>,
}

const ROOT_RESOURCES: [&str; 14] = [
    "channels",
    "terminal_task_records",
    "producers_running",
    "producers_exited",
    "ends_published",
    "ends_acknowledged",
    "sealed",
    "data_positions",
    "payload_bytes",
    "segments",
    "deliveries",
    "retained_reservations",
    "metadata_holders",
    "metadata_bytes",
];

pub(super) fn root_census(rows: &[serde_json::Value]) -> Result<Option<BTreeMap<String, u64>>> {
    use super::result_delivery_support::metric;
    let available = metric(
        rows,
        "novarocks_backend_root_ownership_snapshot_available",
        &[],
    )?;
    ensure!(available <= 1, "invalid root census availability");
    if available == 0 {
        ensure!(
            !rows
                .iter()
                .any(|row| row["tags"]["metric"] == "novarocks_backend_root_ownership"),
            "unavailable root census published stale ownership"
        );
        return Ok(None);
    }
    ROOT_RESOURCES
        .iter()
        .map(|name| {
            Ok((
                (*name).to_owned(),
                metric(
                    rows,
                    "novarocks_backend_root_ownership",
                    &[("resource", name)],
                )?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()
        .map(Some)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RootProtocolMode {
    None,
    ZeroAck,
    FinalAck,
    OneByte,
    MinimumNewRow,
}
struct ParticipationRootRetention(RootProtocolMode, SqlIntentOwner);

impl Scenario for ParticipationRootRetention {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        match self.0 {
            RootProtocolMode::None => "result-delivery/producer-exit-participation-retention",
            RootProtocolMode::OneByte => "result-delivery/one-byte-continuation-normal-wire",
            RootProtocolMode::MinimumNewRow => "result-delivery/minimum-new-row-normal-wire",
            RootProtocolMode::ZeroAck => "result-delivery/installed-root-zero-ack-normal-wire",
            RootProtocolMode::FinalAck => {
                "result-delivery/installed-root-replay-final-ack-interference"
            }
        }
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        if self.0 == RootProtocolMode::None {
            return Ok(None);
        }
        let manifest = self.retention_manifest()?;
        Ok(Some(self.1.freeze(
            vec![StatementSpec {
                sql: manifest.sql,
                faults: vec![],
                observations: (0..cluster_size)
                    .map(|index| (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), None))
                    .collect(),
            }],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let mut observation = None;
        let result = self.run_retention(context, &mut observation);
        let cleanup = observation.as_ref().map_or(Ok(()), |observation| {
            context
                .handle()
                .drain_query_lifecycle_statement_observation(observation)
        });
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Err(cleanup)) => {
                anyhow::bail!("{primary:#}; root observation cleanup: {cleanup:#}")
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }
}

impl ParticipationRootRetention {
    fn new(mode: RootProtocolMode) -> Self {
        Self(mode, SqlIntentOwner::default())
    }

    fn retention_manifest(&self) -> Result<RetentionManifest> {
        Ok(serde_json::from_str(
            if self.0 == RootProtocolMode::MinimumNewRow {
                include_str!(
                    "../../../../docs/testing/mem-1-m07/inputs/minimum-new-row-retention-freeze-v1.json"
                )
            } else if self.0 == RootProtocolMode::OneByte {
                include_str!(
                    "../../../../docs/testing/mem-1-m07/inputs/one-byte-continuation-retention-freeze-v1.json"
                )
            } else {
                include_str!(
                    "../../../../docs/testing/mem-1-m07/inputs/root-context-retention-freeze-v1.json"
                )
            },
        )?)
    }

    fn run_retention(
        &self,
        context: &mut ScenarioContext,
        observation: &mut Option<QueryLifecycleStatementObservation>,
    ) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "root retention requires native 1FE+3BE"
        );
        let manifest = self.retention_manifest()?;
        ensure!(
            manifest.schema_version == 1
                && manifest.topology == "1FE+3BE"
                && !manifest.scope.is_empty()
                && manifest.segment_bytes == 1048576
                && manifest.expected_rows
                    == if self.0 == RootProtocolMode::MinimumNewRow {
                        2
                    } else {
                        1
                    }
                && manifest.native_row_bytes
                    == manifest.expected_row_payload_bytes + 4 * manifest.expected_rows
                && manifest.native_row_bytes > manifest.segment_bytes
                && manifest.native_row_bytes <= 2 * manifest.segment_bytes
                && manifest.query_observation_ms == 20000
                && manifest.held_phase_ms == 5000
                && manifest.metadata_wait_ms == 5000
                && manifest.phase_sample_interval_ms == 100
                && manifest.phase_max_samples == 51,
            "unsupported root retention freeze"
        );
        if self.0 == RootProtocolMode::MinimumNewRow {
            ensure!(
                manifest.expected_columns == 1
                    && manifest.expected_packets == 6
                    && manifest.expected_packet_sequences.as_deref()
                        == Some(&[1, 2, 3, 4, 5, 6][..])
                    && manifest.expected_schema
                        == vec![TextColumnObservation {
                            name: "payload".into(),
                            mysql_type: 253
                        }],
                "unsupported minimum-new-row wire oracle"
            );
        }
        let epoch = Instant::now();
        await_idle(context, "root-retention", "before", epoch)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let mut stream = runtime.block_on(AsyncMysqlStream::connect_with_receive_buffer(
            context.mysql_user(),
            context.mysql_port(),
            Duration::from_millis(manifest.query_observation_ms),
            manifest.mysql_client_max_packet_bytes,
            manifest.client_receive_buffer_bytes,
        ))?;
        let connection_id = stream.connection_id()?;
        // Task-create observations are emitted on stdout. The diagnostic view
        // combines two streams and therefore cannot serve as an append cursor.
        let before_stdout = (0..3)
            .map(|index| context.handle().be_stdout_contents(index, usize::MAX))
            .collect::<Result<Vec<_>>>()?;
        let applied = stream
            .receive_buffer_bytes()
            .context("missing applied client receive buffer")?;
        ensure!(
            applied > 0 && applied <= manifest.max_applied_receive_buffer_bytes,
            "OS client receive window exceeds frozen probe bound"
        );
        let (ready, metadata_ready) = tokio::sync::oneshot::channel();
        let (resume, resume_read) = tokio::sync::oneshot::channel();
        let sql = manifest.sql.clone();
        if self.0 != RootProtocolMode::None {
            *observation = Some(self.1.take(0)?.arm(context, connection_id, &sql)?);
        }
        let job = runtime.spawn(async move {
            stream
                .observe_text_query_with_metadata_pause(
                    &sql,
                    Duration::ZERO,
                    Some((ready, resume_read)),
                )
                .await
        });
        let mut samples = Vec::new();
        let held = (|| -> Result<()> {
            runtime
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_millis(manifest.metadata_wait_ms),
                        metadata_ready,
                    )
                    .await
                })
                .context("root retention metadata deadline")??;
            let deadline = Instant::now() + Duration::from_millis(manifest.held_phase_ms);
            let ports: Vec<_> = context
                .handle()
                .runtime()
                .be
                .iter()
                .map(|be| be.http)
                .collect();
            let mut consecutive = 0;
            loop {
                context.remaining("held root census")?;
                ensure!(
                    Instant::now() < deadline && samples.len() < manifest.phase_max_samples,
                    "root producer never retired with two context-held Data positions and End"
                );
                ensure!(
                    !job.is_finished(),
                    "client actor exited before held-root observation"
                );
                let mut roots = Vec::new();
                for port in &ports {
                    let client = reqwest::blocking::Client::builder()
                        .timeout(
                            deadline
                                .saturating_duration_since(Instant::now())
                                .min(Duration::from_secs(1)),
                        )
                        .build()?;
                    let rows: serde_json::Value = client
                        .get(format!("http://127.0.0.1:{port}/metrics?type=json"))
                        .send()?
                        .error_for_status()?
                        .json()?;
                    roots.push(root_census(
                        rows.as_array()
                            .context("invalid root census metric array")?,
                    )?);
                }
                let occupied: Vec<_> = roots
                    .iter()
                    .filter_map(Option::as_ref)
                    .filter(|root| root["channels"] != 0)
                    .collect();
                let matches = roots.iter().all(Option::is_some)
                    && occupied.len() == 1
                    && occupied[0]["channels"] == 1
                    && occupied[0]["terminal_task_records"] == 1
                    && occupied[0]["producers_running"] == 0
                    && occupied[0]["producers_exited"] == 1
                    && occupied[0]["ends_published"] == 1
                    && occupied[0]["ends_acknowledged"] == 0
                    && occupied[0]["sealed"] == 0
                    && occupied[0]["data_positions"] == 2
                    && occupied[0]["payload_bytes"] == manifest.native_row_bytes
                    && occupied[0]["segments"] == 2
                    && occupied[0]["metadata_bytes"] > 0;
                samples.push(
                    serde_json::json!({"elapsed_micros":epoch.elapsed().as_micros(),
                    "applied_receive_buffer_bytes":applied,"roots":roots,"matches":matches}),
                );
                std::fs::write(
                    context
                        .scenario_root()
                        .join("root-context-held-census.json"),
                    serde_json::to_vec_pretty(&samples)?,
                )?;
                ensure!(
                    Instant::now() < deadline,
                    "held root census exceeded absolute phase deadline"
                );
                consecutive = if matches { consecutive + 1 } else { 0 };
                if consecutive == 2 {
                    if self.0 != RootProtocolMode::None {
                        let occupied_be = roots
                            .iter()
                            .position(|root| {
                                root.as_ref().is_some_and(|root| root["channels"] == 1)
                            })
                            .context("held root has no exact backend")?;
                        installed_root_protocol(
                            context,
                            &before_stdout,
                            occupied_be,
                            self.0,
                            &job,
                        )?;
                        if self.0 == RootProtocolMode::FinalAck {
                            let kill_deadline = Instant::now()
                                + context
                                    .remaining("interference KILL")?
                                    .min(Duration::from_secs(2));
                            context
                                .handle()
                                .kill_query_until(connection_id, kill_deadline)?;
                            ensure!(
                                Instant::now() < kill_deadline,
                                "interference KILL exceeded deadline"
                            );
                        }
                    }
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(manifest.phase_sample_interval_ms));
            }
        })();
        // Resume even a failed probe, then join and preserve the real wire
        // outcome before returning its failure. No background actor is hidden.
        let _ = resume.send(());
        let observation = runtime.block_on(job).context("join paused root client")?;
        std::fs::write(
            context
                .scenario_root()
                .join("root-context-retention-wire.json"),
            serde_json::to_vec_pretty(&observation)?,
        )?;
        held?;
        if self.0 != RootProtocolMode::FinalAck {
            ensure!(
                observation.error.is_none()
                    && observation.columns == manifest.expected_columns
                    && observation.schema == manifest.expected_schema
                    && observation.rows == manifest.expected_rows
                    && observation.row_payload_bytes == manifest.expected_row_payload_bytes
                    && observation.packets == manifest.expected_packets
                    && observation.row_sha256 == manifest.expected_row_sha256,
                "resumed client bytes differ from independent frozen oracle"
            );
        } else {
            ensure!(
                observation.columns == manifest.expected_columns
                    && observation.schema == manifest.expected_schema,
                "protocol interference wire metadata changed"
            );
        }
        await_idle(context, "root-retention", "after", epoch)?;
        context.record_phase_observation(
            "producer-retired-context-held-root",
            1,
            1,
            1,
            "public-mysql-native-root",
            1,
            "passed",
            BTreeMap::from([
                ("rows", observation.rows),
                ("held_data_positions", 2),
                ("census_samples", u64::try_from(samples.len())?),
            ]),
        )?;
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledProtocolManifest {
    schema_version: u32,
    topology: String,
    scope: String,
    retention_input_sha256: String,
    phase_ms: u64,
    max_wait_millis: u64,
    correction_of: BTreeMap<String, String>,
    request_frame_bytes: usize,
    response_data_bytes: usize,
    max_identity_candidates: usize,
    data1_sha256: String,
    data2_sha256: String,
    data1_bytes: usize,
    data2_bytes: usize,
    end_sequence: u64,
    output_rows: u64,
    operations: Vec<String>,
    cases: Vec<String>,
}

fn installed_root_request(
    task: &proto::TaskIdentity,
    wanted_sequence: Option<u64>,
    consumed_sequence: u64,
    max_wait_millis: u64,
) -> proto::FetchRootResultRequest {
    proto::FetchRootResultRequest {
        root_task: Some(task.clone()),
        profile_id: 1,
        output_kind: Some(result_proto::RootOutputKind {
            kind: Some(result_proto::root_output_kind::Kind::ClientRows(true)),
        }),
        wanted_sequence,
        consumed_sequence,
        max_wait_millis,
    }
}

fn installed_root_protocol(
    context: &mut ScenarioContext,
    before_stdout: &[String],
    backend: usize,
    mode: RootProtocolMode,
    job: &tokio::task::JoinHandle<TextResultObservation>,
) -> Result<()> {
    use novarocks_execution_contract::root_result::RootReadOutcome;
    use sha2::{Digest, Sha256};
    let one_byte = mode == RootProtocolMode::OneByte;
    let minimum_new_row = mode == RootProtocolMode::MinimumNewRow;
    let typed_rows = one_byte || minimum_new_row;
    let (protocol_json, retention_bytes, parent_path, parent_bytes) = if minimum_new_row {
        (
            include_str!("../../../../docs/testing/mem-1-m07/inputs/minimum-new-row-protocol-freeze-v1.json"),
            include_bytes!("../../../../docs/testing/mem-1-m07/inputs/minimum-new-row-retention-freeze-v1.json").as_slice(),
            "docs/testing/mem-1-m07/inputs/one-byte-continuation-protocol-freeze-v1.json",
            include_bytes!("../../../../docs/testing/mem-1-m07/inputs/one-byte-continuation-protocol-freeze-v1.json").as_slice(),
        )
    } else if one_byte {
        (
            include_str!("../../../../docs/testing/mem-1-m07/inputs/one-byte-continuation-protocol-freeze-v1.json"),
            include_bytes!("../../../../docs/testing/mem-1-m07/inputs/one-byte-continuation-retention-freeze-v1.json").as_slice(),
            "docs/testing/mem-1-m07/inputs/installed-root-protocol-freeze-v3.json",
            include_bytes!("../../../../docs/testing/mem-1-m07/inputs/installed-root-protocol-freeze-v3.json").as_slice(),
        )
    } else {
        (
            include_str!(
                "../../../../docs/testing/mem-1-m07/inputs/installed-root-protocol-freeze-v3.json"
            ),
            include_bytes!(
                "../../../../docs/testing/mem-1-m07/inputs/root-context-retention-freeze-v1.json"
            )
            .as_slice(),
            "docs/testing/mem-1-m07/inputs/installed-root-protocol-freeze-v2.json",
            include_bytes!(
                "../../../../docs/testing/mem-1-m07/inputs/installed-root-protocol-freeze-v2.json"
            )
            .as_slice(),
        )
    };
    let freeze: InstalledProtocolManifest = serde_json::from_str(protocol_json)?;
    let expected_cases: &[&str] = if minimum_new_row {
        &["result-delivery/minimum-new-row-normal-wire"]
    } else if one_byte {
        &["result-delivery/one-byte-continuation-normal-wire"]
    } else {
        &[
            "result-delivery/installed-root-zero-ack-normal-wire",
            "result-delivery/installed-root-replay-final-ack-interference",
        ]
    };
    let expected_operations: &[&str] = if typed_rows {
        &["ack0", "ack0", "fetch1", "fetch2", "fetch-end3"]
    } else {
        &[
            "ack0",
            "ack0",
            "fetch1",
            "fetch2",
            "fetch-end3",
            "replay1",
            "ack3",
            "ack3",
            "retired1",
        ]
    };
    ensure!(
        freeze.schema_version == if typed_rows { 1 } else { 3 }
            && freeze.topology == "1FE+3BE"
            && !freeze.scope.is_empty()
            && freeze.retention_input_sha256 == format!("{:x}", Sha256::digest(retention_bytes))
            && freeze.phase_ms == 5000
            && freeze.max_wait_millis == 100
            && freeze.correction_of.len() == 3
            && freeze.correction_of["path"] == parent_path
            && !freeze.correction_of["reason"].is_empty()
            && freeze.correction_of["sha256"] == format!("{:x}", Sha256::digest(parent_bytes))
            && freeze.max_identity_candidates == 8
            && freeze.request_frame_bytes == 4096
            && freeze.response_data_bytes == 1048576 + 4096
            && freeze.data1_bytes == 1048576
            && freeze.data2_bytes
                == if minimum_new_row {
                    5
                } else if one_byte {
                    1
                } else {
                    8
                }
            && freeze.end_sequence == 3
            && freeze.output_rows == if minimum_new_row { 2 } else { 1 }
            && freeze.cases == expected_cases
            && freeze.operations == expected_operations,
        "unsupported installed-root protocol freeze"
    );
    let deadline = Instant::now() + Duration::from_millis(freeze.phase_ms);
    let after_stdout = (0..3)
        .map(|index| context.handle().be_stdout_contents(index, usize::MAX))
        .collect::<Result<Vec<_>>>()?;
    let candidates = super::result_delivery_root_protocol::parse_created_task_candidates(
        before_stdout,
        &after_stdout,
        backend,
    )?;
    ensure!(
        candidates.len() <= freeze.max_identity_candidates,
        "root identity candidates exceeded freeze"
    );
    let mut observations = Vec::new();
    let mut installed = None;
    for candidate in candidates {
        ensure!(
            Instant::now() < deadline && !job.is_finished(),
            "root discovery expired or actor exited"
        );
        let request = installed_root_request(&candidate, None, 0, freeze.max_wait_millis);
        let (reply, mut observation) = super::result_delivery_root_protocol::probe_candidate(
            context, backend, &request, 0, deadline,
        )?;
        observation["operation"] = serde_json::json!("discover-exact-root-by-zero-ack");
        observations.push(observation);
        std::fs::write(
            context.scenario_root().join("installed-root-protocol.json"),
            serde_json::to_vec_pretty(&observations)?,
        )?;
        if let Some(reply) = reply {
            ensure!(
                matches!(reply.outcome, RootReadOutcome::AckOnly) && reply.accepted_consumed == 0,
                "root discovery changed or did not acknowledge the zero frontier"
            );
            ensure!(installed.is_none(), "more than one actual task owns a root");
            installed = Some(candidate);
        }
    }
    let task = installed.context("no actual task identity routes to the held installed root")?;
    let mut proven = 0;
    let mut delivered_data = 0u8;
    let mut cursor = novarocks_result_contract::ClientRowStreamCursor::new();
    let row_profile = novarocks_result_contract::ClientRowProfile::try_new(
        freeze.data1_bytes,
        novarocks_result_contract::RootProfileV1::ROW_PAYLOAD_BYTES,
    )?;
    let count = if mode == RootProtocolMode::ZeroAck {
        2
    } else {
        freeze.operations.len()
    };
    for (index, operation) in freeze.operations.iter().take(count).enumerate() {
        ensure!(
            Instant::now() < deadline && !job.is_finished(),
            "installed-root protocol phase expired or actor exited"
        );
        let (wanted, consumed) = match operation.as_str() {
            "ack0" => (None, 0),
            "fetch1" | "replay1" => (Some(1), 0),
            "fetch2" => (Some(2), 0),
            "fetch-end3" => (Some(3), 0),
            "ack3" => (None, proven),
            "retired1" => (Some(1), proven),
            _ => anyhow::bail!("unknown frozen root operation"),
        };
        let request = installed_root_request(&task, wanted, consumed, freeze.max_wait_millis);
        let (reply, mut observation) = super::result_delivery_root_protocol::probe(
            context, backend, &request, proven, deadline,
        )?;
        let mut matches = match (&reply.outcome, operation.as_str()) {
            (RootReadOutcome::AckOnly, "ack0") => reply.accepted_consumed == 0,
            (RootReadOutcome::AckOnly, "ack3") => {
                proven == freeze.end_sequence && reply.accepted_consumed == proven
            }
            (RootReadOutcome::Retired, "retired1") => {
                proven == freeze.end_sequence && reply.accepted_consumed == proven
            }
            (RootReadOutcome::Data(data), "fetch1" | "replay1") => {
                let digest = format!("{:x}", Sha256::digest(data.body()));
                data.sequence().get() == 1
                    && data.end_after_data().is_none()
                    && data.body().len() == freeze.data1_bytes
                    && digest == freeze.data1_sha256
                    && reply.accepted_consumed == 0
            }
            (RootReadOutcome::Data(data), "fetch2") => {
                data.sequence().get() == 2
                    && match data.end_after_data() {
                        None => true,
                        Some(end) => {
                            typed_rows
                                && end.sequence.get() == freeze.end_sequence
                                && end.output_rows == freeze.output_rows
                        }
                    }
                    && data.body().len() == freeze.data2_bytes
                    && format!("{:x}", Sha256::digest(data.body())) == freeze.data2_sha256
                    && reply.accepted_consumed == 0
            }
            (RootReadOutcome::End(end), "fetch-end3") => {
                let matches = delivered_data == 3
                    && end.sequence.get() == freeze.end_sequence
                    && end.output_rows == freeze.output_rows
                    && reply.accepted_consumed == 0;
                if matches {
                    proven = end.sequence.get();
                }
                matches
            }
            _ => false,
        };
        if one_byte
            && matches
            && matches!(operation.as_str(), "fetch1" | "fetch2")
            && let RootReadOutcome::Data(data) = &reply.outcome
        {
            match cursor.validate_body(row_profile, data.body()) {
                Ok(validated) => {
                    let after = validated.after();
                    let continuation = operation == "fetch2";
                    let spans = validated.payload_spans().collect::<Vec<_>>();
                    let exact_span = !continuation
                        || (spans.len() == 1
                            && spans[0].starts_row.is_none()
                            && spans[0].bytes == b"x"
                            && spans[0].completes_row);
                    matches = after.remaining() == u32::from(!continuation)
                        && after.completed_rows() == u64::from(continuation)
                        && exact_span;
                    observation["client_row_cursor"] = serde_json::json!({
                        "before_remaining": cursor.remaining(), "after_remaining": after.remaining(),
                        "completed_rows": after.completed_rows(), "spans": spans.len(),
                        "one_byte_continuation": continuation && exact_span,
                    });
                    if matches {
                        cursor = after;
                    }
                }
                Err(error) => {
                    observation["client_row_cursor_error"] = serde_json::json!(error.to_string());
                    matches = false;
                }
            }
        }
        if minimum_new_row
            && matches
            && matches!(operation.as_str(), "fetch1" | "fetch2")
            && let RootReadOutcome::Data(data) = &reply.outcome
        {
            match cursor.validate_body(row_profile, data.body()) {
                Ok(validated) => {
                    let after = validated.after();
                    let second_row = operation == "fetch2";
                    let spans = validated.payload_spans().collect::<Vec<_>>();
                    let expected_payload = if second_row {
                        1
                    } else {
                        freeze.data1_bytes - 4
                    };
                    let exact_span = spans.len() == 1
                        && spans[0].starts_row.map(|length| length.get() as usize)
                            == Some(expected_payload)
                        && spans[0].bytes.len() == expected_payload
                        && spans[0].completes_row
                        && (!second_row || spans[0].bytes == [0]);
                    matches = cursor.remaining() == 0
                        && cursor.completed_rows() == u64::from(second_row)
                        && after.remaining() == 0
                        && after.completed_rows() == 1 + u64::from(second_row)
                        && exact_span
                        && (!second_row || data.body().as_ref() == &[1, 0, 0, 0, 0][..]);
                    observation["client_row_cursor"] = serde_json::json!({
                        "before_remaining": cursor.remaining(), "after_remaining": after.remaining(),
                        "before_completed_rows": cursor.completed_rows(), "completed_rows": after.completed_rows(),
                        "spans": spans.len(), "starts_row_payload_bytes": spans.first().and_then(|span| span.starts_row).map(|length| length.get()),
                        "minimum_new_row": second_row && exact_span,
                    });
                    if matches {
                        cursor = after;
                    }
                }
                Err(error) => {
                    observation["client_row_cursor_error"] = serde_json::json!(error.to_string());
                    matches = false;
                }
            }
        }
        if typed_rows && operation == "fetch-end3" {
            matches &=
                cursor.validate_end().is_ok() && cursor.completed_rows() == freeze.output_rows;
        }
        if matches && operation == "fetch1" {
            delivered_data |= 1;
        }
        if matches && operation == "fetch2" {
            delivered_data |= 2;
        }
        observation["operation"] = serde_json::json!(operation);
        observation["matches"] = serde_json::json!(matches);
        observation["proven_delivered_consumed"] = serde_json::json!(proven);
        observations.push(observation);
        std::fs::write(
            context.scenario_root().join("installed-root-protocol.json"),
            serde_json::to_vec_pretty(&observations)?,
        )?;
        ensure!(
            matches,
            "installed root operation {operation} differs from frozen oracle"
        );
        // Each typed reply owns only client-side decoded bytes. Release it
        // before another RPC; it never stands in for a backend holder receipt.
        drop(reply);
        let client = reqwest::blocking::Client::builder()
            .timeout(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_secs(1)),
            )
            .build()?;
        let rows: serde_json::Value = client
            .get(format!(
                "http://127.0.0.1:{}/metrics?type=json",
                context.handle().runtime().be[backend].http
            ))
            .send()?
            .error_for_status()?
            .json()?;
        let census = root_census(rows.as_array().context("invalid installed-root census")?)?
            .context("installed-root census unavailable")?;
        let retired = matches!(operation.as_str(), "ack3" | "retired1");
        ensure!(
            census["channels"] == 1
                && census["terminal_task_records"] == 1
                && census["producers_exited"] == 1
                && census["producers_running"] == 0
                && census["ends_published"] == 1
                && census["sealed"] == 0
                && census["ends_acknowledged"] == u64::from(retired)
                && census["data_positions"] == if retired { 0 } else { 2 }
                && census["payload_bytes"]
                    == if retired {
                        0
                    } else {
                        u64::try_from(freeze.data1_bytes + freeze.data2_bytes)?
                    },
            "installed root changed outside its proven ACK frontier"
        );
        let census_path = context
            .scenario_root()
            .join(format!("installed-root-census-{index}.json"));
        std::fs::write(census_path, serde_json::to_vec_pretty(&census)?)?;
        ensure!(
            Instant::now() < deadline && !job.is_finished(),
            "installed-root chain exceeded deadline or paused actor exited"
        );
    }
    Ok(())
}

#[cfg(test)]
mod census_tests {
    #[test]
    fn minimum_new_row_frozen_literal_oracles_and_requests_are_exact() {
        use novarocks_result_contract::{ClientRowProfile, ClientRowStreamCursor, RootProfileV1};
        use sha2::{Digest, Sha256};
        let retention_bytes = include_bytes!(
            "../../../../docs/testing/mem-1-m07/inputs/minimum-new-row-retention-freeze-v1.json"
        );
        let retention: super::RetentionManifest = serde_json::from_slice(retention_bytes).unwrap();
        let freeze: super::InstalledProtocolManifest = serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/minimum-new-row-protocol-freeze-v1.json"
        ))
        .unwrap();
        let value_bytes = 1048568usize;
        let mut first_row = vec![0xfd, 0xf8, 0xff, 0x0f];
        first_row.extend(std::iter::repeat_n(b'x', value_bytes));
        let second_row = [0u8];
        let mut wire_hash = Sha256::new();
        wire_hash.update(&first_row);
        wire_hash.update((first_row.len() as u64).to_le_bytes());
        wire_hash.update(second_row);
        wire_hash.update(1u64.to_le_bytes());
        assert_eq!(
            format!("{:x}", wire_hash.finalize()),
            retention.expected_row_sha256
        );
        assert_eq!(retention.expected_rows, 2);
        assert_eq!(retention.expected_columns, 1);
        assert_eq!(
            retention.expected_row_payload_bytes,
            (first_row.len() + 1) as u64
        );
        assert_eq!(retention.expected_packets, 6);
        assert_eq!(
            retention.expected_packet_sequences.as_deref(),
            Some(&[1, 2, 3, 4, 5, 6][..])
        );
        assert_eq!(
            retention.expected_schema,
            vec![super::TextColumnObservation {
                name: "payload".into(),
                mysql_type: 253
            }]
        );
        assert_eq!(
            retention.sql,
            "SELECT payload FROM (SELECT 0 AS ordinal, REPEAT('x', 1048568) AS payload UNION ALL SELECT 1 AS ordinal, '' AS payload) ordered_rows ORDER BY ordinal ASC"
        );
        let mut data1 = (first_row.len() as u32).to_le_bytes().to_vec();
        data1.extend_from_slice(&first_row);
        let data2 = [1u8, 0, 0, 0, 0];
        assert_eq!(data1.len(), RootProfileV1::SEGMENT_BYTES);
        assert_eq!(freeze.data1_bytes, data1.len());
        assert_eq!(freeze.data2_bytes, data2.len());
        assert_eq!(freeze.end_sequence, 3);
        assert_eq!(freeze.output_rows, 2);
        assert_eq!(format!("{:x}", Sha256::digest(&data1)), freeze.data1_sha256);
        assert_eq!(format!("{:x}", Sha256::digest(data2)), freeze.data2_sha256);
        assert_eq!(
            format!("{:x}", Sha256::digest(retention_bytes)),
            freeze.retention_input_sha256
        );
        let profile = ClientRowProfile::try_new(
            RootProfileV1::SEGMENT_BYTES,
            RootProfileV1::ROW_PAYLOAD_BYTES,
        )
        .unwrap();
        let first = ClientRowStreamCursor::new()
            .validate_body(profile, &data1)
            .unwrap();
        assert_eq!(first.after().remaining(), 0);
        assert_eq!(first.after().completed_rows(), 1);
        let second = first.after().validate_body(profile, &data2).unwrap();
        let spans = second.payload_spans().collect::<Vec<_>>();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].starts_row.unwrap().get(), 1);
        assert_eq!(spans[0].bytes, [0]);
        assert!(spans[0].completes_row);
        assert_eq!(second.after().remaining(), 0);
        assert_eq!(second.after().completed_rows(), 2);
        second.after().validate_end().unwrap();
        let task = super::foreign_root_task();
        for operation in &freeze.operations {
            let wanted = match operation.as_str() {
                "ack0" => None,
                "fetch1" => Some(1),
                "fetch2" => Some(2),
                "fetch-end3" => Some(3),
                other => panic!("unexpected frozen operation {other}"),
            };
            let request = super::installed_root_request(&task, wanted, 0, freeze.max_wait_millis);
            novarocks_task_codec::root_result::decode_read(
                &request,
                novarocks_proto_codec::FieldPath::root("minimum_new_row"),
            )
            .unwrap();
        }
    }

    #[test]
    fn frozen_installed_root_requests_pass_the_production_decoder() {
        let freeze: super::InstalledProtocolManifest = serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/installed-root-protocol-freeze-v3.json"
        ))
        .unwrap();
        let task = super::foreign_root_task();
        for (wanted, consumed) in [
            (None, 0),
            (Some(1), 0),
            (Some(2), 0),
            (Some(3), 0),
            (None, 3),
            (Some(1), 3),
        ] {
            let request =
                super::installed_root_request(&task, wanted, consumed, freeze.max_wait_millis);
            assert!(
                novarocks_task_codec::root_result::decode_read(
                    &request,
                    novarocks_proto_codec::FieldPath::root("frozen_probe")
                )
                .is_ok()
            );
        }
        let invalid = super::installed_root_request(&task, None, 0, 0);
        assert!(
            novarocks_task_codec::root_result::decode_read(
                &invalid,
                novarocks_proto_codec::FieldPath::root("invalid_zero_wait")
            )
            .is_err()
        );
    }

    use super::*;
    #[test]
    fn census_missing_partial_duplicate_stale_and_invalid_samples_are_refused() {
        use serde_json::json;
        let available = json!({"tags":{"metric":"novarocks_backend_root_ownership_snapshot_available"},"value":1});
        assert!(root_census(&[]).is_err());
        assert!(root_census(std::slice::from_ref(&available)).is_err());
        let mut rows = vec![available.clone()];
        rows.extend(ROOT_RESOURCES.iter().map(|name| json!({"tags":{"metric":"novarocks_backend_root_ownership","resource":name},"value":0})));
        assert!(root_census(&rows).unwrap().is_some());
        rows.push(rows[1].clone());
        assert!(root_census(&rows).is_err());
        rows.pop();
        rows[0]["value"] = json!(0);
        assert!(root_census(&rows).is_err());
        assert_eq!(root_census(&rows[..1]).unwrap(), None);
        rows[0] = available;
        rows[1]["value"] = json!(-1);
        assert!(root_census(&rows).is_err());
    }
}

#[cfg(test)]
mod root_refusal_v2_tests {
    use super::*;

    fn freeze() -> RefusalManifest {
        serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/root-read-refusal-freeze-v2.json"
        ))
        .unwrap()
    }
    fn captured(message: proto::FetchRootResultResponse) -> super::super::native_trust::GrpcProbe {
        let payload = message.encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        frame.extend_from_slice(&payload);
        super::super::native_trust::GrpcProbe {
            http_status: 200,
            grpc_status: Some(0),
            grpc_message: None,
            response_body: Some(frame),
        }
    }
    fn refused(request: &proto::FetchRootResultRequest) -> proto::FetchRootResultResponse {
        proto::FetchRootResultResponse {
            root_task: request.root_task.clone(),
            profile_id: request.profile_id,
            output_kind: request.output_kind,
            result: Some(proto::fetch_root_result_response::Result::Refused(
                proto::RootReadRefusal {
                    fact: Some(novarocks_task_codec::status::encode_failure_fact(
                        FailureFact::new(
                            FailureClass::Data,
                            FailureReason::new(
                                domain::WORKER,
                                std::num::NonZeroU16::new(43).unwrap(),
                            ),
                        ),
                    )),
                    safe_detail: "root read was refused by its owning channel".into(),
                },
            )),
        }
    }
    #[test]
    fn legal_foreign_root_requires_complete_original_typed_identity_refusal() {
        let freeze = freeze();
        assert_eq!(freeze.schema_version, 2);
        let case = &freeze.cases[0];
        assert_eq!(
            case.response_contract,
            RefusalResponseContract::TypedIdentityRefusal
        );
        let request = refusal_request(case, &foreign_root_task()).unwrap();
        let original = refused(&request);
        assert!(assert_root_refusal_response(case, &request, &captured(original.clone())).is_ok());
        for mode in 0..6 {
            let mut changed = original.clone();
            match mode {
                0 => changed.profile_id = 2,
                1 => changed.root_task.as_mut().unwrap().task_id += 1,
                2 => changed.result = None,
                3 => {
                    let Some(proto::fetch_root_result_response::Result::Refused(value)) =
                        changed.result.as_mut()
                    else {
                        unreachable!()
                    };
                    value.fact = None;
                }
                4 => {
                    let Some(proto::fetch_root_result_response::Result::Refused(value)) =
                        changed.result.as_mut()
                    else {
                        unreachable!()
                    };
                    value.fact.as_mut().unwrap().reason.as_mut().unwrap().code = 44;
                }
                _ => {
                    let Some(proto::fetch_root_result_response::Result::Refused(value)) =
                        changed.result.as_mut()
                    else {
                        unreachable!()
                    };
                    value.safe_detail = "foreign Display is not the safe owner summary".into();
                }
            }
            assert!(
                assert_root_refusal_response(case, &request, &captured(changed)).is_err(),
                "accepted mutation {mode}"
            );
        }
        let mut unknown = captured(original.clone());
        unknown.response_body = None;
        assert!(assert_root_refusal_response(case, &request, &unknown).is_err());
        let mut empty = captured(original.clone());
        empty.response_body = Some(Vec::new());
        assert!(assert_root_refusal_response(case, &request, &empty).is_err());
        let mut suffix = captured(original);
        suffix
            .response_body
            .as_mut()
            .unwrap()
            .extend_from_slice(&[0, 0, 0, 0, 0]);
        assert!(assert_root_refusal_response(case, &request, &suffix).is_err());
    }
    #[test]
    fn all_six_malformed_requests_keep_grpc3_and_no_business_body() {
        let freeze = freeze();
        for case in &freeze.cases[1..] {
            assert_eq!(
                case.response_contract,
                RefusalResponseContract::MalformedNoBody
            );
            assert_eq!(case.grpc_status, 3);
            let request = refusal_request(case, &foreign_root_task()).unwrap();
            let mut response = super::super::native_trust::GrpcProbe {
                http_status: 200,
                grpc_status: Some(3),
                grpc_message: None,
                response_body: Some(Vec::new()),
            };
            assert!(assert_root_refusal_response(case, &request, &response).is_ok());
            response.response_body = captured(refused(&request)).response_body;
            assert!(assert_root_refusal_response(case, &request, &response).is_err());
        }
    }
}
