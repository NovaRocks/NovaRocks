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

use super::native_creation::receipt_verdict;
#[path = "compatibility_sql_intent.rs"]
mod compatibility_sql_intent;
use crate::scenario::{
    Scenario, ScenarioBinary, ScenarioBinaryLayout, ScenarioContext, ScenarioLaunchConfig,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use compatibility_sql_intent::{CompatibilityIntent, CompatibilitySql};
use h2::client;
use http::{Request, header};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_native_trust::{NativeEndpointConnector, NativeTrust};
use novarocks_proto_models::{common, novarocks as proto};
use novarocks_types::identity::{BackendProcessId, FrontendProcessId};
use prost::Message;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const REQUIRED_BACKENDS: usize = 3;
// Observation pacing matches the existing task-evidence polling cadence.
// It never decides expiry or extends the original request/scenario deadline.
const ADMISSION_OBSERVATION_INTERVAL: Duration = Duration::from_millis(25);
const BASELINE_QUERY: &str = "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v";
pub(super) const HEARTBEAT_PATH: &str = "/novarocks.NovaRocksGrpc/Heartbeat";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(MixedBuildSameIsland::default()),
        Box::new(OtherIslandHardCut::default()),
        Box::new(RawEstablishCompatibilityAdmission::default()),
        Box::new(IslandDrainAndReplacement::default()),
        Box::new(TargetIslandCutover::default()),
    ]
}

#[derive(Default)]
struct MixedBuildSameIsland {
    intent: CompatibilityIntent,
}

impl Scenario for MixedBuildSameIsland {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_compatibility",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn is_compatibility_stage(&self) -> bool {
        true
    }
    fn binary_requirements(&self) -> &'static [ScenarioBinary] {
        &[ScenarioBinary::Primary, ScenarioBinary::Compatible]
    }

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

    fn test_session_plan(
        &self,
        launch: &ScenarioLaunchConfig,
        _: &std::path::Path,
        cluster_size: usize,
        _: &std::path::Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        ensure!(
            cluster_size == 3,
            "compatibility observations require native 1FE+3BE"
        );
        let artifacts = launch
            .resolved_artifacts
            .as_ref()
            .context("compatibility role artifacts were not resolved before freezing")?;
        Ok(Some(
            self.intent.freeze(
                &artifacts.frontend,
                &artifacts.backends,
                None,
                (0..3)
                    .map(|_| CompatibilitySql {
                        sql: BASELINE_QUERY.into(),
                        backends: vec![0, 1, 2],
                    })
                    .collect(),
                &[],
                deadline,
            )?,
        ))
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
        compatibility_sql_intent::run_baselines(context, &self.intent, BASELINE_QUERY, &[0, 1, 2])?;
        context.action(format!("admitted 3 BEs with builds={build_ids:?} inside compatibility_ids={compatibility_ids:?}"));
        Ok(())
    }
}

#[derive(Default)]
struct OtherIslandHardCut {
    intent: CompatibilityIntent,
}
#[derive(Default)]
struct RawEstablishCompatibilityAdmission {
    intent: CompatibilityIntent,
}
#[derive(Default)]
struct IslandDrainAndReplacement {
    intent: CompatibilityIntent,
}
#[derive(Default)]
struct TargetIslandCutover {
    source: CompatibilityIntent,
    target: CompatibilityIntent,
    peer:
        std::sync::Mutex<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>>,
}

impl Scenario for RawEstablishCompatibilityAdmission {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_compatibility",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn is_compatibility_stage(&self) -> bool {
        true
    }
    fn binary_requirements(&self) -> &'static [ScenarioBinary] {
        &[ScenarioBinary::Primary]
    }

    fn name(&self) -> &'static str {
        "native-compatibility/raw-establish-compatibility-admission"
    }

    fn test_session_plan(
        &self,
        launch: &ScenarioLaunchConfig,
        _: &std::path::Path,
        cluster_size: usize,
        _: &std::path::Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        ensure!(
            cluster_size == 3,
            "raw compatibility observation requires native 1FE+3BE"
        );
        let artifacts = launch
            .resolved_artifacts
            .as_ref()
            .context("raw compatibility artifacts were not resolved before freezing")?;
        Ok(Some(self.intent.freeze(
            &artifacts.frontend,
            &artifacts.backends,
            None,
            vec![],
            &[0],
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let target_port = context.handle().runtime().be[0].grpc;
        let rows = context.handle().frontend_backend_topology()?;
        let target = rows
            .iter()
            .find(|row| row.grpc_port == target_port)
            .context("backend topology omitted raw Establish target BE")?;
        ensure!(
            target.is_eligible_live(),
            "raw Establish target BE must be eligible and live, row={target:?}"
        );
        let backend = target
            .process_id
            .parse::<BackendProcessId>()
            .context("parse target backend process identity")?;
        let native_compatibility_id = decode_hex_32(&target.native_compatibility_id)?;
        let target_participation = raw_participation(backend);
        let endpoint = context.handle().native_be_endpoint(0)?;
        let mode = context.handle().native_trust_mode();
        let connector = context.handle().native_probe_connector(endpoint, mode)?;
        let control_endpoint = context.handle().native_be_control_endpoint(0)?;
        let control_connector = context
            .handle()
            .native_probe_connector(control_endpoint, mode)?;
        let trust = context.handle().native_probe_trust()?;
        let authorization = authorization_header(&trust)?;
        let heartbeat: proto::HeartbeatResponse = raw_unary(
            control_connector,
            HEARTBEAT_PATH,
            &authorization,
            proto::HeartbeatRequest {
                expected_process_id: Some(proto::BackendProcessId {
                    value: backend.to_bytes().to_vec(),
                }),
            },
            context.deadline(),
        )?;
        let admission_epoch_capability = heartbeat
            .admission_epoch_capability
            .context("raw Establish target heartbeat omitted its admission epoch capability")?;
        let acquisition = raw_apply_task_operations(
            &connector,
            &authorization,
            vec![raw_acquire_admission_ticket(
                target_participation.clone(),
                native_compatibility_id,
                admission_epoch_capability.clone(),
            )],
            context.deadline(),
        )?;
        let acquisition_receipt =
            only_successful_receipt(acquisition, "admission ticket acquisition")?;
        ensure!(
            proto::TaskOperationOutcome::try_from(receipt_verdict(&acquisition_receipt)?.outcome)
                == Ok(proto::TaskOperationOutcome::Accepted),
            "admission ticket acquisition must be accepted, got {acquisition_receipt:?}"
        );
        let Some(proto::task_operation_verdict::Ack::ParticipationAdmissionTicket(ticket)) =
            receipt_verdict(&acquisition_receipt)?.ack.clone()
        else {
            anyhow::bail!("accepted admission acquisition omitted its ticket acknowledgement");
        };
        ensure!(
            ticket.participation.as_ref() == Some(&target_participation)
                && ticket.valid_for_millis == 10_000,
            "admission ticket acknowledgement does not bind the exact raw request: {ticket:?}"
        );
        let admission_ticket_id = ticket
            .ticket_id
            .context("admission ticket acknowledgement omitted its ticket id")?;
        let metrics_client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;
        let metrics_port = context.handle().runtime().be[0].http;
        let mut admission_sample = 0usize;
        let observation_deadline = context
            .deadline()
            .min(Instant::now() + Duration::from_secs(15));
        let issued = loop {
            if let Some(sample) = admission_observation(
                &metrics_client,
                context,
                metrics_port,
                &mut admission_sample,
            )? {
                break sample;
            }
            ensure!(
                Instant::now() < observation_deadline,
                "original admission observation remained unavailable"
            );
            thread::sleep(
                ADMISSION_OBSERVATION_INTERVAL
                    .min(observation_deadline.saturating_duration_since(Instant::now())),
            );
        };
        ensure!(
            issued.logical_live_grants == 1.0,
            "the issued raw ticket must retain its original logical grant: {issued:?}"
        );
        let establish_applied_before = context
            .handle()
            .be_log_count(0, super::task_evidence::PARTICIPATION_ESTABLISH_APPLIED)?;

        for (label, compatibility_id) in [
            ("missing", None),
            ("31-byte", Some(vec![0xa1; 31])),
            ("33-byte", Some(vec![0xa2; 33])),
        ] {
            let response = raw_apply_task_operations(
                &connector,
                &authorization,
                vec![raw_establish(
                    target_participation.clone(),
                    admission_ticket_id.clone(),
                    compatibility_id,
                )],
                context.deadline(),
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
            vec![raw_establish(
                target_participation.clone(),
                admission_ticket_id.clone(),
                Some(foreign.to_vec()),
            )],
            context.deadline(),
        )?;
        let foreign_receipt = only_successful_receipt(foreign_response, "foreign Establish")?;
        ensure!(
            proto::TaskOperationOutcome::try_from(receipt_verdict(&foreign_receipt)?.outcome)
                == Ok(proto::TaskOperationOutcome::CompatibilityMismatch),
            "foreign Establish must return CompatibilityMismatch, got {foreign_receipt:?}"
        );
        ensure!(
            receipt_verdict(&foreign_receipt)?.ack.is_none(),
            "compatibility mismatch must not carry an acknowledgement"
        );
        ensure!(
            context
                .handle()
                .be_log_count(0, super::task_evidence::PARTICIPATION_ESTABLISH_APPLIED)?
                == establish_applied_before,
            "foreign Establish must not emit an applied marker"
        );

        let create_response = raw_apply_task_operations(
            &connector,
            &authorization,
            vec![raw_create_task(target_participation.clone())],
            context.deadline(),
        )?;
        let create_receipt =
            only_successful_receipt(create_response, "CreateTask after foreign Establish")?;
        ensure!(
            proto::TaskOperationOutcome::try_from(receipt_verdict(&create_receipt)?.outcome)
                == Ok(proto::TaskOperationOutcome::NotReady),
            "CreateTask after foreign Establish must immediately require an active context, got {create_receipt:?}"
        );
        ensure!(
            receipt_verdict(&create_receipt)?.ack.is_none(),
            "CreateTask against an absent context must not carry an acknowledgement"
        );
        ensure!(
            context
                .handle()
                .be_log_count(0, super::task_evidence::PARTICIPATION_ESTABLISH_APPLIED)?
                == establish_applied_before,
            "refused CreateTask must not establish the context"
        );
        context.action(
            "foreign 32-byte compatibility identity returned CompatibilityMismatch; a following valid CreateTask returned NotReady without establishing the context",
        );

        // The absent-context CreateTask does not consume the admission ticket.
        // Observe both the committed logical terminal transition and actual fee
        // return before a new acquisition; rejected acquisitions remain replayable.
        wait_for_admission_ticket_expiry(
            &metrics_client,
            context,
            metrics_port,
            &mut admission_sample,
        )?;
        context.action(
            "target BE original logical grant and actual ticket charge both moved from 1 to 0 before renewed acquisition",
        );
        let renewed_acquisition = raw_apply_task_operations(
            &connector,
            &authorization,
            vec![raw_acquire_admission_ticket(
                target_participation.clone(),
                native_compatibility_id,
                admission_epoch_capability,
            )],
            context.deadline(),
        )?;
        let renewed_receipt =
            only_successful_receipt(renewed_acquisition, "renewed admission ticket acquisition")?;
        ensure!(
            proto::TaskOperationOutcome::try_from(receipt_verdict(&renewed_receipt)?.outcome)
                == Ok(proto::TaskOperationOutcome::Accepted),
            "renewed admission ticket acquisition must be accepted, got {renewed_receipt:?}"
        );
        let Some(proto::task_operation_verdict::Ack::ParticipationAdmissionTicket(renewed_ticket)) =
            receipt_verdict(&renewed_receipt)?.ack.clone()
        else {
            anyhow::bail!("renewed admission acquisition omitted its ticket acknowledgement");
        };
        let renewed_ticket_id = renewed_ticket
            .ticket_id
            .context("renewed admission acknowledgement omitted its ticket id")?;

        let matching_response = raw_apply_task_operations(
            &connector,
            &authorization,
            vec![raw_establish(
                target_participation,
                renewed_ticket_id,
                Some(native_compatibility_id.to_vec()),
            )],
            context.deadline(),
        )?;
        let matching_receipt = only_successful_receipt(matching_response, "matching Establish")?;
        ensure!(
            proto::TaskOperationOutcome::try_from(receipt_verdict(&matching_receipt)?.outcome)
                == Ok(proto::TaskOperationOutcome::Accepted),
            "matching Establish must be accepted, got {matching_receipt:?}"
        );
        ensure!(
            matches!(
                receipt_verdict(&matching_receipt)?.ack.clone(),
                Some(proto::task_operation_verdict::Ack::Participation(_))
            ),
            "accepted matching Establish must carry a query-context acknowledgement"
        );
        ensure!(
            context
                .handle()
                .be_log_count(0, super::task_evidence::PARTICIPATION_ESTABLISH_APPLIED)?
                == establish_applied_before + 1,
            "matching Establish must emit exactly one applied marker"
        );
        context.action(
            "exact backend compatibility identity accepted the same raw query context and emitted the establish-applied marker",
        );
        Ok(())
    }
}

fn wait_for_admission_ticket_expiry(
    client: &reqwest::blocking::Client,
    context: &ScenarioContext,
    port: u16,
    sample_index: &mut usize,
) -> Result<()> {
    let deadline = context
        .deadline()
        .min(Instant::now() + Duration::from_secs(15));
    loop {
        let sample = admission_observation(client, context, port, sample_index)?;
        if let Some(sample) = sample
            && sample.logical_live_grants == 0.0
        {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "Worker did not close the original grant before the scenario deadline; sample={sample:?}"
        );
        thread::sleep(
            ADMISSION_OBSERVATION_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct AdmissionObservation {
    logical_live_grants: f64,
}

fn admission_observation(
    client: &reqwest::blocking::Client,
    context: &ScenarioContext,
    port: u16,
    sample_index: &mut usize,
) -> Result<Option<AdmissionObservation>> {
    let url = format!("http://127.0.0.1:{port}/metrics?type=json");
    let response = client.get(&url).send()?;
    let status = response.status();
    let body = response.bytes()?;
    let stem = format!("admission-owner-sample-{:04}", *sample_index);
    *sample_index = sample_index
        .checked_add(1)
        .context("admission sample index exhausted")?;
    // Preserve the actual response before parsing or reporting a missing field.
    std::fs::write(context.scenario_root().join(format!("{stem}.body")), &body)?;
    std::fs::write(
        context.scenario_root().join(format!("{stem}-http.json")),
        serde_json::to_vec_pretty(
            &serde_json::json!({"url":url,"status":status.as_u16(),"bytes":body.len()}),
        )?,
    )?;
    ensure!(
        status.is_success(),
        "original admission metrics HTTP failed: {status}"
    );
    let rows: serde_json::Value = serde_json::from_slice(&body)?;
    let rows = rows
        .as_array()
        .context("BE metrics JSON was not an array")?;
    let read = |metric: &str, resource: Option<&str>| -> Result<f64> {
        let mut samples = rows.iter().filter(|row| {
            row["tags"]["metric"] == metric
                && resource.is_none_or(|resource| row["tags"]["resource"] == resource)
        });
        let sample = samples
            .next()
            .with_context(|| format!("BE metrics omitted {metric} {resource:?}"))?;
        ensure!(
            samples.next().is_none(),
            "BE metrics duplicated {metric} {resource:?}"
        );
        let value = sample["value"]
            .as_f64()
            .context("original admission metric was not numeric")?;
        ensure!(
            value.is_finite()
                && value.fract() == 0.0
                && (0.0..=9_007_199_254_740_991.0).contains(&value),
            "original admission metric is not an exact nonnegative integer"
        );
        Ok(value)
    };
    let available = read(
        "novarocks_backend_worker_admission_snapshot_available",
        None,
    )?;
    ensure!(
        available == 0.0 || available == 1.0,
        "invalid admission availability"
    );
    if available == 0.0 {
        ensure!(
            !rows
                .iter()
                .any(|row| row["tags"]["metric"] == "novarocks_backend_worker_admission"),
            "unavailable admission sample retained stale fields"
        );
        return Ok(None);
    }
    Ok(Some(AdmissionObservation {
        logical_live_grants: read(
            "novarocks_backend_worker_admission",
            Some("logical_live_grants"),
        )?,
    }))
}

impl Scenario for OtherIslandHardCut {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_compatibility",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn is_compatibility_stage(&self) -> bool {
        true
    }
    fn binary_requirements(&self) -> &'static [ScenarioBinary] {
        &[
            ScenarioBinary::Primary,
            ScenarioBinary::Compatible,
            ScenarioBinary::OtherIsland,
        ]
    }

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

    fn test_session_plan(
        &self,
        launch: &ScenarioLaunchConfig,
        _: &std::path::Path,
        cluster_size: usize,
        _: &std::path::Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        ensure!(
            cluster_size == 3,
            "compatibility observations require native 1FE+3BE"
        );
        let artifacts = launch
            .resolved_artifacts
            .as_ref()
            .context("compatibility role artifacts were not resolved before freezing")?;
        Ok(Some(
            self.intent.freeze(
                &artifacts.frontend,
                &artifacts.backends,
                None,
                (0..3)
                    .map(|_| CompatibilitySql {
                        sql: BASELINE_QUERY.into(),
                        backends: vec![0, 1],
                    })
                    .collect(),
                &[2],
                deadline,
            )?,
        ))
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
        // The other-island binary is either this build at the test-alternate
        // epoch or a real earlier release; the evidence names which one.
        let excluded_build = other[0].build_identity.clone();
        let primary_compatibility_id = decode_hex_32(&live[0].native_compatibility_id)?;
        assert_island_ready(context, 200)?;
        compatibility_sql_intent::run_baselines(context, &self.intent, BASELINE_QUERY, &[0, 1])?;
        context
            .handle()
            .assert_be_log(2, super::task_evidence::PARTICIPATION_ESTABLISH_APPLIED)
            .expect_err("OtherIsland BE must never be given a query context by the FE");
        assert_other_island_admission_cut(context, primary_compatibility_id)?;
        assert_raw_ingress_hard_cuts(context)?;
        context.action(format!(
            "excluded BE (build {excluded_build}) remained OtherIsland while SQL admitted only compatible BEs"
        ));
        Ok(())
    }
}

impl Scenario for TargetIslandCutover {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_compatibility",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn is_compatibility_stage(&self) -> bool {
        true
    }
    fn binary_requirements(&self) -> &'static [ScenarioBinary] {
        &[
            ScenarioBinary::Primary,
            ScenarioBinary::Compatible,
            ScenarioBinary::OtherIsland,
        ]
    }

    fn name(&self) -> &'static str {
        "native-compatibility/target-island-cutover"
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

    fn test_session_plan(
        &self,
        launch: &ScenarioLaunchConfig,
        _: &std::path::Path,
        cluster_size: usize,
        _: &std::path::Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        ensure!(
            cluster_size == 3,
            "compatibility cutover requires native 1FE+3BE"
        );
        let artifacts = launch
            .resolved_artifacts
            .as_ref()
            .context("cutover artifacts were not resolved before freezing")?;
        let other = artifacts
            .other_island
            .as_deref()
            .context("cutover requires --other-island-binary")?;
        let peer = self.target.freeze(
            other,
            &vec![other.to_owned(); 3],
            None,
            vec![CompatibilitySql {
                sql: BASELINE_QUERY.into(),
                backends: vec![],
            }],
            &[],
            deadline,
        )?;
        *self
            .peer
            .lock()
            .map_err(|_| anyhow::anyhow!("cutover peer intent poisoned"))? = Some(peer);
        Ok(Some(self.source.freeze(
            &artifacts.frontend,
            &artifacts.backends,
            None,
            vec![CompatibilitySql {
                sql: "SELECT sleep(2)".into(),
                backends: vec![],
            }],
            &[],
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        assert_island_ready(context, 200)?;
        let peer = self
            .peer
            .lock()
            .map_err(|_| anyhow::anyhow!("cutover peer intent poisoned"))?
            .take()
            .context("target cutover plan was not frozen before source launch")?;
        let mut target = context.launch_peer_cluster_with_test_session(
            "target-island",
            ScenarioLaunchConfig {
                binary_layout: ScenarioBinaryLayout {
                    frontend: ScenarioBinary::OtherIsland,
                    backends: vec![ScenarioBinary::OtherIsland; REQUIRED_BACKENDS],
                },
                expected_eligible_backend_count: Some(REQUIRED_BACKENDS),
                ..Default::default()
            },
            peer,
        )?;
        let mut source = None;
        let primary = (|| -> Result<()> {
            let target_island = target.frontend_management_get(
                "/island-readyz",
                context
                    .remaining("observe target island readiness")?
                    .min(Duration::from_secs(5)),
            )?;
            ensure!(
                target_island.status == 200,
                "target island is not ready: {target_island:?}"
            );
            source = Some(compatibility_sql_intent::SourceQuery::start(
                context,
                self.source.take(0)?,
            )?);
            thread::sleep(
                Duration::from_millis(200)
                    .min(context.remaining("source cutover admission interval")?),
            );
            context
                .handle()
                .begin_fe_drain()
                .context("begin source FE drain after target island readiness")?;
            compatibility_sql_intent::run_peer_baseline(
                &mut target,
                self.target.take(0)?,
                BASELINE_QUERY,
                context.deadline(),
            )?;
            Ok(())
        })();
        // Even a target failure joins the original source before cleanup. FE
        // session/process close invalidates its source observation on drain.
        let source_exit = source.map_or(Ok(()), compatibility_sql_intent::SourceQuery::join);
        let result = compatibility_sql_intent::combine(primary, source_exit).and_then(|()| {
            let deadline = context.deadline();
            context.handle().wait_fe_exit_until(deadline)?;
            context.action("target island became ready before source drain; new query went to target while source attempt completed locally");
            Ok(())
        });
        let cleanup = TestClusterOwner::shutdown(&mut target);
        compatibility_sql_intent::combine(result, cleanup)
    }
}

impl Scenario for IslandDrainAndReplacement {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_compatibility",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn is_compatibility_stage(&self) -> bool {
        true
    }
    fn binary_requirements(&self) -> &'static [ScenarioBinary] {
        &[
            ScenarioBinary::Primary,
            ScenarioBinary::Compatible,
            ScenarioBinary::OtherIsland,
        ]
    }

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

    fn test_session_plan(
        &self,
        launch: &ScenarioLaunchConfig,
        _: &std::path::Path,
        cluster_size: usize,
        _: &std::path::Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        ensure!(
            cluster_size == 3,
            "compatibility observations require native 1FE+3BE"
        );
        let artifacts = launch
            .resolved_artifacts
            .as_ref()
            .context("compatibility role artifacts were not resolved before freezing")?;
        Ok(Some(
            self.intent.freeze(
                &artifacts.frontend,
                &artifacts.backends,
                Some((
                    2,
                    artifacts
                        .compatible
                        .as_deref()
                        .context("compatibility replacement requires --compatible-binary")?,
                )),
                (0..3)
                    .map(|_| CompatibilitySql {
                        sql: BASELINE_QUERY.into(),
                        backends: vec![2],
                    })
                    .collect(),
                &[],
                deadline,
            )?,
        ))
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
        compatibility_sql_intent::run_baselines(context, &self.intent, BASELINE_QUERY, &[2])?;
        context.action("replaced the OtherIsland BE with a same-island binary and recovered distributed query admission");
        Ok(())
    }
}

/// Proves the malformed-exchange raw-ingress cut owned by this backend.
///
/// Raw Establish compatibility admission has its own scenario because it must
/// also prove the registry stays untouched after a well-formed foreign-island
/// request.
fn assert_other_island_admission_cut(
    context: &mut ScenarioContext,
    primary_compatibility_id: [u8; 32],
) -> Result<()> {
    let endpoint = context.handle().native_be_endpoint(2)?;
    let mode = context.handle().native_trust_mode();
    let connector = context.handle().native_probe_connector(endpoint, mode)?;
    let control_endpoint = context.handle().native_be_control_endpoint(2)?;
    let control_connector = context
        .handle()
        .native_probe_connector(control_endpoint, mode)?;
    let trust = context.handle().native_probe_trust()?;
    let authorization = authorization_header(&trust)?;
    let rows = context.handle().frontend_backend_topology()?;
    let target_port = context.handle().runtime().be[2].grpc;
    let target = rows
        .iter()
        .find(|row| row.grpc_port == target_port)
        .context("backend topology omitted OtherIsland admission target")?;
    let backend = target.process_id.parse::<BackendProcessId>()?;
    let heartbeat: proto::HeartbeatResponse = raw_unary(
        control_connector,
        HEARTBEAT_PATH,
        &authorization,
        proto::HeartbeatRequest {
            expected_process_id: Some(proto::BackendProcessId {
                value: backend.to_bytes().to_vec(),
            }),
        },
        context.deadline(),
    )?;
    let admission_epoch_capability = heartbeat
        .admission_epoch_capability
        .context("OtherIsland heartbeat omitted its admission epoch capability")?;
    let metrics_client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let metrics_port = context.handle().runtime().be[2].http;
    let mut sample_index = 0;
    let mut observe_admission = || -> Result<AdmissionObservation> {
        loop {
            if let Some(sample) =
                admission_observation(&metrics_client, context, metrics_port, &mut sample_index)?
            {
                return Ok(sample);
            }
            ensure!(
                Instant::now() < context.deadline(),
                "OtherIsland admission observation remained unavailable"
            );
            thread::sleep(
                ADMISSION_OBSERVATION_INTERVAL
                    .min(context.deadline().saturating_duration_since(Instant::now())),
            );
        }
    };
    let reservations_before = observe_admission()?;
    let response = raw_apply_task_operations(
        &connector,
        &authorization,
        vec![raw_acquire_admission_ticket(
            raw_participation(backend),
            primary_compatibility_id,
            admission_epoch_capability,
        )],
        context.deadline(),
    )?;
    let receipt = only_successful_receipt(response, "OtherIsland admission acquisition")?;
    ensure!(
        proto::TaskOperationOutcome::try_from(receipt_verdict(&receipt)?.outcome)
            == Ok(proto::TaskOperationOutcome::CompatibilityMismatch),
        "OtherIsland admission must reject the primary island identity, got {receipt:?}"
    );
    ensure!(
        receipt_verdict(&receipt)?.ack.is_none(),
        "rejected admission must not issue a ticket"
    );
    ensure!(
        observe_admission()? == reservations_before,
        "OtherIsland rejection must not acquire a Worker reservation"
    );
    context
        .handle()
        .assert_be_log(2, super::task_evidence::PARTICIPATION_ESTABLISH_APPLIED)
        .expect_err("rejected OtherIsland admission must not establish a query context");
    context.action("OtherIsland BE rejected the exact primary compatibility identity before ticket reservation or query-context establishment");
    Ok(())
}

fn assert_raw_ingress_hard_cuts(context: &mut ScenarioContext) -> Result<()> {
    let rows = context.handle().frontend_backend_topology()?;
    let target_port = context.handle().runtime().be[2].grpc;
    let backend = rows
        .iter()
        .find(|row| row.grpc_port == target_port)
        .context("backend topology omitted raw Exchange ingress target")?
        .process_id
        .parse::<BackendProcessId>()?;
    let endpoint = context.handle().native_be_endpoint(2)?;
    let mode = context.handle().native_trust_mode();
    let connector = context.handle().native_probe_connector(endpoint, mode)?;
    let trust = context.handle().native_backend_probe_trust()?;
    let authorization = authorization_header(&trust)?;
    // An internally complete fixture identity reaches the unknown-route cut;
    // malformed payload must still be rejected before any decoder runs.
    let query = novarocks_types::QueryExecutionId::new(
        novarocks_types::QueryId::new(11, 12),
        novarocks_types::AttemptId::new(1)?,
    )?;
    let process = match trust.local_process_identity() {
        Some(novarocks_native_trust::NativeProcessIdentity::Backend(process)) => process,
        _ => anyhow::bail!("Exchange probe requires its authenticated backend process"),
    };
    let source = novarocks_execution_contract::TaskIdentity::new(
        query,
        novarocks_types::StageId::new(2)?,
        novarocks_types::TaskId::new(1)?,
        process,
    );
    let destination = novarocks_execution_contract::TaskIdentity::new(
        query,
        novarocks_types::StageId::new(1)?,
        novarocks_types::TaskId::new(1)?,
        backend,
    );
    let stream = novarocks_execution_contract::ExchangeStreamIdentity::try_new(
        novarocks_execution_contract::ExchangeSource::new(
            source,
            novarocks_types::UniqueId::new(21, 22),
            0,
        ),
        destination,
        novarocks_types::UniqueId::new(11, 12),
        novarocks_execution_contract::FragmentNodeId::new(0),
    )?;
    use novarocks_execution_contract::ordered_retained_stream::exchange::{
        ExchangeUnitDemand, ExchangeUnitItem, ExchangeUnitSequence,
    };
    use novarocks_task_codec::exchange_unit_codec as codec;
    let sequence = ExchangeUnitSequence::FIRST;
    let exchange = codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {
        stream,
        sender_count: 1,
        sender_id: 0,
        backend_number: 0,
        item: ExchangeUnitItem::Unit {
            demand: ExchangeUnitDemand {
                sequence,
                wire_bytes: 1,
            },
            end_after_unit: true,
        },
        payload: Bytes::from_static(&[0xff]),
        next_demand: None,
    });
    let response: proto::DeliverExchangeUnitResponse = raw_unary(
        connector,
        "/novarocks.NovaRocksGrpc/DeliverExchangeUnit",
        &authorization,
        exchange,
        context.deadline(),
    )?;
    let outcome = codec::decode_delivery_response(
        &response,
        stream,
        sequence,
        novarocks_proto_codec::FieldPath::root("unknown_exchange_probe"),
    )?;
    ensure!(
        matches!(outcome,codec::DecodedExchangeDeliveryOutcome::Refused(fact) if fact.reason().code()==460),
        "unknown malformed Exchange Unit must reject at the frozen route before decode, got {outcome:?}"
    );
    context.action("malformed unknown Exchange route rejected before decode");
    Ok(())
}

pub(super) fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
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

pub(super) fn raw_participation(backend: BackendProcessId) -> proto::ParticipationRef {
    proto::ParticipationRef {
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

pub(super) fn raw_operation_envelope(max_wait_millis: u64) -> proto::TaskOperationEnvelope {
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

pub(super) fn raw_establish(
    target_participation: proto::ParticipationRef,
    admission_ticket_id: proto::AdmissionTicketId,
    native_compatibility_id: Option<Vec<u8>>,
) -> proto::TaskOperation {
    proto::TaskOperation {
        envelope: Some(raw_operation_envelope(15_000)),
        operation: Some(proto::task_operation::Operation::UpdateParticipation(
            proto::UpdateParticipationRequest {
                command: Some(proto::update_participation_request::Command::Establish(
                    proto::EstablishParticipationRequest {
                        participation: Some(target_participation),
                        catalog_set: Some(Default::default()),
                        initial_runtime_filter: Some(Default::default()),
                        initial_credential: Some(proto::ParticipationCredentialDomain {
                            credential_domain_id: 1,
                            epoch: 1,
                            descriptors: Vec::new(),
                        }),
                        initial_lease: Some(proto::ParticipationLeaseGrant {
                            sequence: 0,
                            valid_for_millis: 30_000,
                        }),
                        query_options: Some(raw_query_options()),
                        native_compatibility_id: native_compatibility_id
                            .map(|value| proto::NativeCompatibilityId { value }),
                        admission_ticket_id: Some(admission_ticket_id),
                    },
                )),
            },
        )),
    }
}

pub(super) fn raw_acquire_admission_ticket(
    target_participation: proto::ParticipationRef,
    native_compatibility_id: [u8; 32],
    admission_epoch_capability: proto::AdmissionEpochCapability,
) -> proto::TaskOperation {
    proto::TaskOperation {
        envelope: Some(raw_operation_envelope(15_000)),
        operation: Some(
            proto::task_operation::Operation::AcquireParticipationAdmissionTicket(
                proto::AcquireParticipationAdmissionTicketRequest {
                    participation: Some(target_participation),
                    valid_for_millis: 10_000,
                    native_compatibility_id: Some(proto::NativeCompatibilityId {
                        value: native_compatibility_id.to_vec(),
                    }),
                    admission_epoch_capability: Some(admission_epoch_capability),
                },
            ),
        ),
    }
}

fn raw_create_task(target_participation: proto::ParticipationRef) -> proto::TaskOperation {
    let execution = target_participation
        .query_execution_id
        .expect("raw query context carries an execution identity");
    let backend = target_participation
        .backend_process_id
        .clone()
        .expect("raw query context carries a backend identity");
    let task_key = common::UniqueId { hi: 93, lo: 94 };
    let frozen_fragment = super::native_creation::compiled_fragment(
        1,
        2,
        None,
        super::native_creation::RawSink::Result,
        &[],
    );
    // Each creation fact has one owner: the descriptor owns identity, kernel
    // key, parallelism and topology, the context owns every query-wide option,
    // and the assignment owns only the instance ordinal, the initial scan
    // ranges and the sink bindings. This root task has no scan and no sink
    // edge, so its assignment is just its ordinal.
    let creation_metadata = proto::CreationMetadata {
        participation: Some(target_participation),
        descriptor: Some(proto::TaskDescriptor {
            identity: Some(proto::TaskIdentity {
                query_execution_id: Some(execution),
                stage_id: 1,
                task_id: 1,
                backend_process_id: Some(backend),
            }),
            task_key: Some(task_key),
            pipeline_dop: 2,
            split_plan_nodes: Vec::new(),
            topology: Some(Default::default()),
        }),
        initial_domains: Vec::new(),
        assignment: Some(proto::TaskAssignment {
            instance_ordinal: 0,
            initial_scan_ranges: Vec::new(),
            sink_edge_ids: Vec::new(),
        }),
    };
    proto::TaskOperation {
        envelope: Some(raw_operation_envelope(50)),
        operation: Some(proto::task_operation::Operation::CreateTask(
            proto::CreateTaskRequest {
                frozen_fragment: frozen_fragment.encode_to_vec().into(),
                creation_metadata: creation_metadata.encode_to_vec().into(),
            },
        )),
    }
}

pub(super) fn only_successful_receipt(
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
    let receipt = receipts.remove(0);
    novarocks_task_codec::operation::decode_receipt_header(
        &receipt,
        novarocks_proto_codec::FieldPath::root("scenario_receipt"),
    )?;
    Ok(receipt)
}

pub(super) fn raw_apply_task_operations(
    connector: &NativeEndpointConnector,
    authorization: &str,
    operations: Vec<proto::TaskOperation>,
    deadline: std::time::Instant,
) -> Result<RawUnaryResponse<proto::ApplyTaskOperationsResponse>> {
    raw_apply_task_operations_until(connector, authorization, operations, deadline)
}

pub(super) fn raw_apply_task_operations_until(
    connector: &NativeEndpointConnector,
    authorization: &str,
    operations: Vec<proto::TaskOperation>,
    deadline: std::time::Instant,
) -> Result<RawUnaryResponse<proto::ApplyTaskOperationsResponse>> {
    raw_unary_response_until(
        connector,
        "/novarocks.NovaRocksGrpc/ApplyTaskOperations",
        authorization,
        proto::ApplyTaskOperationsRequest { operations },
        deadline,
    )
}

pub(super) fn authorization_header(trust: &NativeTrust) -> Result<String> {
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

pub(super) fn raw_unary<M: Message, R: Message + Default>(
    connector: NativeEndpointConnector,
    path: &str,
    authorization: &str,
    message: M,
    deadline: std::time::Instant,
) -> Result<R> {
    raw_unary_until(connector, path, authorization, message, deadline)
}

pub(super) fn raw_unary_until<M: Message, R: Message + Default>(
    connector: NativeEndpointConnector,
    path: &str,
    authorization: &str,
    message: M,
    deadline: std::time::Instant,
) -> Result<R> {
    let response = raw_unary_response_until(&connector, path, authorization, message, deadline)?;
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

pub(super) struct RawUnaryResponse<R> {
    pub(super) grpc_status: u16,
    pub(super) grpc_message: Option<String>,
    pub(super) message: Option<R>,
}

pub(super) fn raw_unary_response<M: Message, R: Message + Default>(
    connector: &NativeEndpointConnector,
    path: &str,
    authorization: &str,
    message: M,
    deadline: std::time::Instant,
) -> Result<RawUnaryResponse<R>> {
    raw_unary_response_until(connector, path, authorization, message, deadline)
}

/// One explicit fixture-owned physical connection. Cloned clients multiplex
/// streams on this connection; they never create another peer incarnation.
pub(super) struct RawUnaryConnection {
    runtime: Arc<tokio::runtime::Runtime>,
    sender: Option<client::SendRequest<Bytes>>,
    connection_job: Option<tokio::task::JoinHandle<Result<(), h2::Error>>>,
    close_failure: Option<String>,
    original_deadline: Option<std::time::Instant>,
}

#[derive(Clone)]
pub(super) struct RawUnaryClient {
    runtime: Arc<tokio::runtime::Runtime>,
    sender: client::SendRequest<Bytes>,
    original_deadline: Option<std::time::Instant>,
}

impl RawUnaryConnection {
    pub(super) fn connect_until(
        connector: &NativeEndpointConnector,
        deadline: std::time::Instant,
    ) -> Result<Self> {
        Self::connect_with_deadline(connector, Some(deadline))
    }
    fn connect_with_deadline(
        connector: &NativeEndpointConnector,
        deadline: Option<std::time::Instant>,
    ) -> Result<Self> {
        if let Some(deadline) = deadline {
            ensure!(
                std::time::Instant::now() < deadline,
                "raw Native original connect deadline expired"
            );
        }
        let runtime = Arc::new(tokio::runtime::Runtime::new()?);
        let connect = async {
            let stream = connector.connect().await.map_err(anyhow::Error::msg)?;
            let (sender, connection) = client::handshake(stream).await?;
            let connection_job = tokio::spawn(connection);
            Ok::<_, anyhow::Error>((sender, connection_job))
        };
        let (sender, connection_job) = runtime.block_on(async {
            match deadline {
                Some(deadline) => {
                    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), connect)
                        .await
                        .context("raw Native original TCP/TLS/H2 deadline expired")?
                }
                None => connect.await,
            }
        })?;
        let mut connection = Self {
            runtime,
            sender: Some(sender),
            connection_job: Some(connection_job),
            close_failure: None,
            original_deadline: deadline,
        };
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return combine_raw_cleanup(
                Err(anyhow::anyhow!(
                    "raw Native connection completed after its original deadline"
                )),
                connection.close(),
            );
        }
        Ok(connection)
    }

    pub(super) fn client(&self) -> RawUnaryClient {
        RawUnaryClient {
            runtime: Arc::clone(&self.runtime),
            sender: self.sender.as_ref().expect("live raw H2 fixture").clone(),
            original_deadline: self.original_deadline,
        }
    }

    /// Cancellation is explicit and joined: dropping a task handle alone would
    /// detach the IO connection job and leave a physical closing generation alive.
    pub(super) fn close(&mut self) -> Result<()> {
        drop(self.sender.take());
        if let Some(connection_job) = self.connection_job.take() {
            connection_job.abort();
            let failure = match self.runtime.block_on(connection_job) {
                Ok(Ok(())) => None,
                Err(error) if error.is_cancelled() => None,
                Ok(Err(error)) => Some(format!(
                    "original raw Native H2 connection job failed during actual join: {error}"
                )),
                Err(error) => Some(format!(
                    "original raw Native H2 connection job panicked during actual join: {error}"
                )),
            };
            if failure.is_some() {
                self.close_failure = failure;
            }
        }
        match &self.close_failure {
            Some(failure) => Err(anyhow::anyhow!(failure.clone())),
            None => Ok(()),
        }
    }
}

impl Drop for RawUnaryConnection {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            eprintln!("Raw Native actual exit failed: {error:#}");
        }
    }
}

impl RawUnaryClient {
    /// Header-only rejection on this same physical connection and its original
    /// deadline. The caller closes/joins the original owner on either outcome.
    pub(super) fn header_rejection_until(
        &self,
        path: &str,
        authorization: &str,
        extra_name: &str,
        extra_value: &str,
        original_deadline: std::time::Instant,
    ) -> Result<tonic::Status> {
        let deadline = self
            .original_deadline
            .map_or(original_deadline, |original| {
                original.min(original_deadline)
            });
        ensure!(
            std::time::Instant::now() < deadline,
            "raw Native original header deadline expired"
        );
        let mut sender = self.sender.clone();
        let outcome = self.runtime.block_on(async move {
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async move {
                let request = Request::builder()
                    .method("POST")
                    .uri(path)
                    .header(header::CONTENT_TYPE, "application/grpc")
                    .header("te", "trailers")
                    .header(header::AUTHORIZATION, authorization)
                    .header(extra_name, extra_value)
                    .body(())?;
                sender = sender.ready().await?;
                let (response, unwritten_body) = sender.send_request(request, false)?;
                let response = response.await?;
                let status = tonic::Status::from_header_map(response.headers())
                    .context("early Native ingress rejection omitted grpc-status")?;
                drop(unwritten_body);
                Ok::<_, anyhow::Error>(status)
            })
            .await
            .context("raw Native original ready/header deadline expired")?
        })?;
        ensure!(
            std::time::Instant::now() < deadline,
            "raw Native header response completed after its original deadline"
        );
        Ok(outcome)
    }
    pub(super) fn response_with_hold_until<M: Message, R: Message + Default>(
        &self,
        path: &str,
        authorization: &str,
        message: M,
        hold_token: Option<&str>,
        original_deadline: std::time::Instant,
    ) -> Result<RawUnaryResponse<R>> {
        self.response_with_deadline(
            path,
            authorization,
            message,
            hold_token,
            Some(original_deadline),
        )
    }
    fn response_with_deadline<M: Message, R: Message + Default>(
        &self,
        path: &str,
        authorization: &str,
        message: M,
        hold_token: Option<&str>,
        deadline: Option<std::time::Instant>,
    ) -> Result<RawUnaryResponse<R>> {
        let deadline = match (self.original_deadline, deadline) {
            (Some(original), Some(request)) => Some(original.min(request)),
            (original, request) => original.or(request),
        };
        if let Some(deadline) = deadline {
            ensure!(
                std::time::Instant::now() < deadline,
                "raw Native original request deadline expired"
            );
        }
        let mut payload = Vec::new();
        message.encode(&mut payload)?;
        let mut frame = Vec::with_capacity(payload.len() + 5);
        frame.push(0);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        let mut sender = self.sender.clone();
        let response = async move {
            let mut request = Request::builder()
                .method("POST")
                .uri(path)
                .header(header::CONTENT_TYPE, "application/grpc")
                .header("te", "trailers")
                .header(header::AUTHORIZATION, authorization);
            if let Some(token) = hold_token {
                request = request.header("x-novarocks-ingress-hold-token", token);
            }
            let request = request.body(())?;
            // Readiness belongs to each sender handle, including concurrent
            // streams approaching the peer's advertised stream limit.
            sender = sender.ready().await?;
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
                let chunk = chunk?;
                bytes.extend_from_slice(&chunk);
                body.flow_control().release_capacity(chunk.len())?;
            }
            let trailers = body.trailers().await?;
            let status = header_status
                .or_else(|| trailers.as_ref().and_then(grpc_status))
                .context("raw RPC omitted grpc-status")?;
            let message = header_message.or_else(|| trailers.as_ref().and_then(grpc_message));
            let decoded = if bytes.is_empty() {
                None
            } else {
                ensure!(
                    bytes.len() >= 5 && bytes[0] == 0,
                    "raw RPC response lacks uncompressed gRPC frame"
                );
                let length = u32::from_be_bytes(bytes[1..5].try_into().expect("frame header width"))
                    as usize;
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
        };
        let result = self.runtime.block_on(async move {
            match deadline {
                Some(deadline) => {
                    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), response)
                        .await
                        .context(
                            "raw Native original ready/response/body/trailers deadline expired",
                        )?
                }
                None => response.await,
            }
        })?;
        ensure!(
            deadline.is_none_or(|deadline| std::time::Instant::now() < deadline),
            "raw Native response completed after its original deadline"
        );
        Ok(result)
    }
}

pub(super) fn raw_unary_response_until<M: Message, R: Message + Default>(
    connector: &NativeEndpointConnector,
    path: &str,
    authorization: &str,
    message: M,
    original_deadline: std::time::Instant,
) -> Result<RawUnaryResponse<R>> {
    raw_unary_response_with_hold_until(
        connector,
        path,
        authorization,
        message,
        None,
        original_deadline,
    )
}
pub(super) fn raw_unary_response_with_hold_until<M: Message, R: Message + Default>(
    connector: &NativeEndpointConnector,
    path: &str,
    authorization: &str,
    message: M,
    hold_token: Option<&str>,
    original_deadline: std::time::Instant,
) -> Result<RawUnaryResponse<R>> {
    let mut connection = RawUnaryConnection::connect_until(connector, original_deadline)?;
    let primary = connection.client().response_with_hold_until(
        path,
        authorization,
        message,
        hold_token,
        original_deadline,
    );
    combine_raw_cleanup(primary, connection.close())
}

pub(super) fn combine_raw_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(primary), Err(cleanup)) => {
            anyhow::bail!("{primary:#}; raw Native actual exit: {cleanup:#}")
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
    }
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

#[cfg(test)]
mod tests {
    #[test]
    fn raw_connection_job_actual_exit_retains_failure_and_panic_across_repeated_close() {
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        for panic in [false, true] {
            let connection_job = runtime.spawn(async move {
                if panic {
                    panic!("intentional raw connection job regression panic");
                }
                Err(h2::Error::from(h2::Reason::INTERNAL_ERROR))
            });
            runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(2), async {
                    while !connection_job.is_finished() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            });
            let mut connection = RawUnaryConnection {
                runtime: Arc::clone(&runtime),
                sender: None,
                connection_job: Some(connection_job),
                close_failure: None,
                original_deadline: Some(Instant::now() + Duration::from_secs(2)),
            };
            let original = connection.close().unwrap_err().to_string();
            assert!(original.contains(if panic { "panicked" } else { "failed" }));
            assert!(connection.connection_job.is_none());
            assert_eq!(connection.close().unwrap_err().to_string(), original);
            let combined = combine_raw_cleanup::<()>(
                Err(anyhow::anyhow!("original RPC failed")),
                connection.close(),
            )
            .unwrap_err()
            .to_string();
            assert!(combined.contains("original RPC failed") && combined.contains(&original));
        }
    }
    #[test]
    fn raw_connection_job_cancellation_is_actually_joined_and_has_no_failure_verdict() {
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let connection_job = runtime.spawn(std::future::pending::<Result<(), h2::Error>>());
        let mut connection = RawUnaryConnection {
            runtime,
            sender: None,
            connection_job: Some(connection_job),
            close_failure: None,
            original_deadline: Some(Instant::now() + Duration::from_secs(2)),
        };
        connection.close().unwrap();
        assert!(connection.connection_job.is_none());
        connection.close().unwrap();
    }

    use super::*;

    #[test]
    fn grpc_status_accepts_initial_response_headers() {
        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", http::HeaderValue::from_static("0"));

        assert_eq!(grpc_status(&headers), Some(0));
    }
}
