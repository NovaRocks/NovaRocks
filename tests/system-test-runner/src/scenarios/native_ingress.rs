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

//! Native ingress checks against the real 1FE+3BE process boundary.

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use h2::client;
use http::{Request, header};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;
use novarocks_native_trust::NativeEndpointConnector;
use novarocks_proto_models::novarocks as proto;
use novarocks_types::BackendProcessId;
use prost::Message;
use reqwest::blocking::Client;
use serde_json::Value;
use std::time::Duration;
use std::{
    fs,
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::PathBuf,
    sync::mpsc,
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    actors::mysql as mysql_actor,
    scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig},
};
use novarocks_cluster_harness::LaunchProfile;
use novarocks_cluster_harness::process_resources::ProcessResourceSampler;

use super::native_compatibility::{
    RawUnaryResponse, authorization_header, raw_acquire_admission_ticket, raw_establish,
    raw_operation_envelope, raw_query_context, raw_unary_response, raw_unary_response_with_hold,
};

const REQUIRED_BACKENDS: usize = 3;
const HEARTBEAT_PATH: &str = "/novarocks.NovaRocksGrpc/Heartbeat";
const ORDINARY_PATH: &str = "/novarocks.NovaRocksGrpc/ApplyTaskOperations";
const CONTROL_PATH: &str = "/novarocks.NovaRocksGrpc/ApplyTaskControlOperations";
const ORDINARY_BATCH_MAX: usize = 48 * 1024 * 1024;
const ORDINARY_FRAME_MAX: usize = 64 * 1024 * 1024;
const CONTROL_FRAME_MAX: usize = 1024 * 1024;

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(OuterPreflightRejection),
        Box::new(QueryMessageBounds),
        Box::new(CurrentGateObservation),
        Box::new(BlockingSaturationControl),
        Box::new(ResidentEnvelopeCalibration),
        Box::new(AsyncSchedulingPressure),
        Box::new(PartialBodyDeadline),
        Box::new(RegistryContentionControl),
    ]
}

struct OuterPreflightRejection;
struct QueryMessageBounds;
struct CurrentGateObservation;
struct BlockingSaturationControl;
struct ResidentEnvelopeCalibration;
struct AsyncSchedulingPressure;
struct PartialBodyDeadline;
struct RegistryContentionControl;

#[derive(Clone, PartialEq, Message)]
struct PaddedOperations {
    #[prost(message, repeated, tag = "1")]
    operations: Vec<proto::TaskOperation>,
    #[prost(bytes, tag = "2047")]
    padding: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct PaddedControls {
    #[prost(message, repeated, tag = "1")]
    operations: Vec<proto::TaskControlOperation>,
    #[prost(bytes, tag = "2047")]
    padding: Vec<u8>,
}

impl Scenario for OuterPreflightRejection {
    fn name(&self) -> &'static str {
        "native-ingress/outer-preflight-rejection"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let (connector, authorization, _, _) = probe(context)?;
        let bad_timeout = header_rejection(
            &connector,
            HEARTBEAT_PATH,
            &authorization,
            "grpc-timeout",
            "invalid",
        )?;
        ensure!(
            bad_timeout.code() == tonic::Code::InvalidArgument,
            "malformed grpc-timeout must fail before body read, got {bad_timeout}"
        );
        context.action("malformed grpc-timeout rejected without sending a request body");

        let over_control = header_rejection(
            &connector,
            CONTROL_PATH,
            &authorization,
            "content-length",
            &(CONTROL_FRAME_MAX + 6).to_string(),
        )?;
        ensure!(
            over_control.code() == tonic::Code::ResourceExhausted,
            "oversized control Content-Length must fail before body read, got {over_control}"
        );
        ensure!(
            over_control.metadata().get("x-novarocks-ingress-rejection")
                == Some(&tonic::metadata::MetadataValue::from_static("body_limit")),
            "control body limit rejection lost its typed ingress category"
        );
        context.action("control method Content-Length rejected without sending a request body");

        let over_ordinary = header_rejection(
            &connector,
            ORDINARY_PATH,
            &authorization,
            "content-length",
            &(ORDINARY_FRAME_MAX + 6).to_string(),
        )?;
        ensure!(
            over_ordinary.code() == tonic::Code::ResourceExhausted,
            "oversized ordinary Content-Length must fail before body read, got {over_ordinary}"
        );
        ensure!(
            over_ordinary
                .metadata()
                .get("x-novarocks-ingress-rejection")
                == Some(&tonic::metadata::MetadataValue::from_static("body_limit")),
            "ordinary body limit rejection lost its typed ingress category"
        );
        context.action("ordinary method 64 MiB Content-Length rejected before reading DATA");

        let response: RawUnaryResponse<proto::ApplyTaskOperationsResponse> = raw_unary_response(
            &connector,
            ORDINARY_PATH,
            &authorization,
            proto::ApplyTaskOperationsRequest {
                operations: vec![proto::TaskOperation::default(); 33],
            },
        )?;
        ensure!(
            response.grpc_status == tonic::Code::ResourceExhausted as u16
                && response.message.is_none(),
            "33 operations must fail at codec resource preflight before typed dispatch: status={} detail={:?}",
            response.grpc_status,
            response.grpc_message
        );
        context.action("33-item batch rejected by the raw codec resource preflight");
        let rows = read_backend_metrics(context, 0)?;
        for class in ["ordinary", "control"] {
            let header_rejections = metric(
                &rows,
                "novarocks_backend_native_ingress_rejections_total",
                &[("class", class), ("reason", "body_limit")],
            )?;
            ensure!(
                header_rejections >= 1.0,
                "{class} header-stage body gate did not record rejection"
            );
        }
        let worker_used = metric(
            &rows,
            "novarocks_backend_worker_context_reservations",
            &[("dimension", "used")],
        )?;
        let tasks_created = metric(
            &rows,
            "novarocks_backend_task_execution_tasks_created_total",
            &[],
        )?;
        ensure!(
            worker_used == 0.0 && tasks_created == 0.0,
            "preflight rejection escaped into Worker: reservations={worker_used} created={tasks_created}"
        );
        context.action("Tower body_limit counters advanced; Worker reservations and created tasks remained zero");
        Ok(())
    }
}

impl Scenario for QueryMessageBounds {
    fn name(&self) -> &'static str {
        "native-ingress/query-message-bounds"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let (connector, authorization, backend, compatibility_id) = probe(context)?;
        let heartbeat: RawUnaryResponse<proto::HeartbeatResponse> = raw_unary_response(
            &connector,
            HEARTBEAT_PATH,
            &authorization,
            proto::HeartbeatRequest {
                expected_process_id: Some(proto::BackendProcessId {
                    value: backend.to_bytes().to_vec(),
                }),
            },
        )?;
        ensure!(
            heartbeat.grpc_status == 0,
            "heartbeat failed before bounds probe"
        );
        let epoch = heartbeat
            .message
            .context("heartbeat omitted response")?
            .admission_epoch_capability
            .context("heartbeat omitted admission epoch")?;
        let ordinary =
            raw_acquire_admission_ticket(raw_query_context(backend), compatibility_id, epoch);
        for (size, accepted) in [(ORDINARY_BATCH_MAX, true), (ORDINARY_BATCH_MAX + 1, false)] {
            let request = pad_ordinary(ordinary.clone(), size)?;
            let response: RawUnaryResponse<proto::ApplyTaskOperationsResponse> =
                raw_unary_response(&connector, ORDINARY_PATH, &authorization, request)?;
            if accepted {
                ensure!(
                    response.grpc_status == 0
                        && response
                            .message
                            .as_ref()
                            .is_some_and(|body| body.receipts.len() == 1),
                    "legal near-limit ordinary request failed: status={} detail={:?}",
                    response.grpc_status,
                    response.grpc_message
                );
            } else {
                ensure!(
                    response.grpc_status == tonic::Code::ResourceExhausted as u16,
                    "over-limit ordinary request was not rejected: status={} detail={:?}",
                    response.grpc_status,
                    response.grpc_message
                );
            }
            context.action(format!(
                "ordinary batch encoded_bytes={size} accepted={accepted} grpc_status={}",
                response.grpc_status
            ));
        }

        let control = proto::TaskControlOperation {
            envelope: Some(raw_operation_envelope(15_000)),
            control: Some(proto::task_control_operation::Control::CancelTask(
                proto::CancelTaskRequest {
                    identity: Some(proto::TaskIdentity {
                        query_execution_id: raw_query_context(backend).query_execution_id,
                        stage_id: 1,
                        task_id: 1,
                        backend_process_id: Some(proto::BackendProcessId {
                            value: backend.to_bytes().to_vec(),
                        }),
                    }),
                    reason: proto::TaskCancelReason::UpstreamNoLongerNeeded as i32,
                },
            )),
        };
        for (size, accepted) in [(CONTROL_FRAME_MAX, true), (CONTROL_FRAME_MAX + 1, false)] {
            let request = pad_control(control.clone(), size)?;
            let response: RawUnaryResponse<proto::ApplyTaskOperationsResponse> =
                raw_unary_response(&connector, CONTROL_PATH, &authorization, request)?;
            if accepted {
                ensure!(
                    response.grpc_status == 0
                        && response
                            .message
                            .as_ref()
                            .is_some_and(|body| body.receipts.len() == 1),
                    "legal near-limit control request failed: status={} detail={:?}",
                    response.grpc_status,
                    response.grpc_message
                );
            } else {
                ensure!(
                    response.grpc_status == tonic::Code::OutOfRange as u16,
                    "over-limit control request was not rejected: status={} detail={:?}",
                    response.grpc_status,
                    response.grpc_message
                );
            }
            context.action(format!(
                "control batch encoded_bytes={size} accepted={accepted} grpc_status={}",
                response.grpc_status
            ));
        }
        let streamed_status = streamed_control_over_limit(&connector, &authorization)?;
        ensure!(
            streamed_status.code() == tonic::Code::ResourceExhausted
                && streamed_status
                    .metadata()
                    .get("x-novarocks-ingress-rejection")
                    == Some(&tonic::metadata::MetadataValue::from_static("body_limit")),
            "control DATA body gate did not return its typed ingress rejection: {streamed_status}"
        );
        context.action(format!(
            "control over-limit DATA with legal frame length returned grpc_status={streamed_status}"
        ));
        let rows = read_backend_metrics(context, 0)?;
        let limited = metric(
            &rows,
            "novarocks_backend_native_ingress_rejections_total",
            &[("class", "control"), ("reason", "body_limit")],
        )?;
        ensure!(
            limited >= 1.0,
            "control DATA size rejection did not reach the Tower body gate"
        );
        context.action(format!(
            "control DATA over-limit recorded by Tower body gate: body_limit_rejections={limited}"
        ));
        Ok(())
    }
}

impl Scenario for CurrentGateObservation {
    fn name(&self) -> &'static str {
        "native-ingress/current-gate-observation"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        _manifest: Option<&std::path::Path>,
    ) -> Result<()> {
        ensure!(
            launch_profile == LaunchProfile::FaultScenario,
            "dynamic gate observation requires --launch-profile fault-scenario"
        );
        Ok(())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "current gate observation requires a real 1FE+3BE cluster"
        );
        for index in 0..REQUIRED_BACKENDS {
            let rows = read_backend_metrics(context, index)?;
            let rows = rows.as_slice();
            for class in ["ordinary", "control"] {
                for phase in ["running", "waiting"] {
                    let used = metric(
                        rows,
                        "novarocks_backend_native_ingress_slots",
                        &[("class", class), ("phase", phase), ("dimension", "used")],
                    )?;
                    let limit = metric(
                        rows,
                        "novarocks_backend_native_ingress_slots",
                        &[("class", class), ("phase", phase), ("dimension", "limit")],
                    )?;
                    ensure!(
                        used == 0.0 && limit > 0.0,
                        "BE[{index}] {class} {phase} slots invalid: used={used} limit={limit}"
                    );
                }
                for kind in ["body", "data_backing"] {
                    let retained = metric(
                        rows,
                        "novarocks_backend_native_response_backings",
                        &[("class", class), ("kind", kind)],
                    )?;
                    ensure!(
                        retained == 0.0,
                        "BE[{index}] {class} {kind} backing remained at rest: {retained}"
                    );
                }
            }
            let worker_used = metric(
                rows,
                "novarocks_backend_worker_context_reservations",
                &[("dimension", "used")],
            )?;
            let worker_limit = metric(
                rows,
                "novarocks_backend_worker_context_reservations",
                &[("dimension", "limit")],
            )?;
            ensure!(
                worker_used == 0.0 && worker_limit > 0.0,
                "BE[{index}] Worker reservation readout invalid: used={worker_used} limit={worker_limit}"
            );
            for (source, expected) in [
                ("native_ingress", 1.0),
                ("worker_context_reservation", 1.0),
                ("exchange_slot", 0.0),
                ("memory_charge", 0.0),
            ] {
                let available = metric(
                    rows,
                    "novarocks_backend_saturation_source_available",
                    &[("source", source)],
                )?;
                ensure!(
                    available == expected,
                    "BE[{index}] {source} availability={available}, expected={expected}"
                );
            }
            context.action(format!("BE[{index}] current Native slots and Worker reservations exposed at rest; future Exchange/MEM readouts explicitly unavailable"));
        }
        let (connector, authorization, backend, compatibility_id) = probe(context)?;
        let operation = ticket_operation(&connector, &authorization, backend, compatibility_id)?;
        let mut held = HeldOrdinary::start(&connector, &authorization, operation, backend, 1)?;
        held.await_entered(context.deadline())?;
        wait_ingress_slot(context, "ordinary", "running", 1.0)?;
        context.action("BE[0] ordinary running slot rose to one during a real held Worker closure");
        held.release_and_join()?;
        wait_ingress_slot(context, "ordinary", "running", 0.0)?;
        context.action("BE[0] ordinary running slot returned to zero after closure release");
        Ok(())
    }
}

impl Scenario for BlockingSaturationControl {
    fn name(&self) -> &'static str {
        "native-ingress/blocking-saturation-control"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        _uea1_workload_manifest: Option<&std::path::Path>,
    ) -> Result<()> {
        ensure!(
            launch_profile == LaunchProfile::FaultScenario,
            "blocking saturation requires --launch-profile fault-scenario"
        );
        Ok(())
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut launch = ScenarioLaunchConfig::default();
        launch.config_overlay.be = Some(
            "[runtime.native_ingress]\nmax_blocking_threads = 8\nordinary_running = 8\nordinary_waiting = 8\ncontrol_worker_threads = 4\ncontrol_running = 4\ncontrol_waiting = 4\n"
                .to_string(),
        );
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let (connector, authorization, backend, compatibility_id) = probe(context)?;
        let releasable =
            establish_releasable_context(&connector, &authorization, backend, compatibility_id)?;
        let operation = ticket_operation(&connector, &authorization, backend, compatibility_id)?;
        let running_limit = metric(
            &read_backend_metrics(context, 0)?,
            "novarocks_backend_native_ingress_slots",
            &[
                ("class", "ordinary"),
                ("phase", "running"),
                ("dimension", "limit"),
            ],
        )?;
        ensure!(
            running_limit == 8.0,
            "expected eight ordinary slots, got {running_limit}"
        );
        let mut held =
            HeldOrdinary::start(&connector, &authorization, operation.clone(), backend, 8)?;
        held.await_entered(context.deadline())?;
        wait_ingress_slot(context, "ordinary", "running", 8.0)?;
        context.action("BE[0] eight real ordinary Worker closures held; ordinary blocking pool and running gate full");

        let queued_connector = connector.clone();
        let queued_authorization = authorization.clone();
        let queued_operation = operation.clone();
        let queued = thread::spawn(move || {
            raw_unary_response::<_, proto::ApplyTaskOperationsResponse>(
                &queued_connector,
                ORDINARY_PATH,
                &queued_authorization,
                proto::ApplyTaskOperationsRequest {
                    operations: vec![queued_operation],
                },
            )
        });
        wait_ingress_slot(context, "ordinary", "waiting", 1.0)?;
        context.action("ninth ordinary request visibly waited at Native gate");

        // No task was created in this context. A typed Worker refusal proves
        // control-lane progress without claiming that a real task was canceled.
        let cancel_request = control_cancel(backend);
        let cancel_id = cancel_request.operations[0]
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.operation_id.clone())
            .context("Cancel probe omitted operation id")?;
        let control = raw_unary_response(&connector, CONTROL_PATH, &authorization, cancel_request)?;
        let receipt = exact_probe_receipt(
            control,
            &cancel_id,
            proto::TaskOperationOutcome::InvalidStateOrRequest,
            "Cancel",
        )?;
        ensure!(
            receipt.ack.is_none()
                && receipt.safe_field_path.is_none()
                && receipt.safe_detail == "cancel names a task this backend does not own",
            "task-free Cancel did not return its Worker-owned refusal: {receipt:?}"
        );
        context.action(
            "task-free Cancel returned the exact Worker refusal while eight ordinary closures and one waiter were present",
        );

        let quiesce_request = control_quiesce(releasable.clone());
        let quiesce_id = quiesce_request.operations[0]
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.operation_id.clone())
            .context("Quiesce probe omitted operation id")?;
        let quiesce =
            raw_unary_response(&connector, CONTROL_PATH, &authorization, quiesce_request)?;
        let receipt = exact_probe_receipt(
            quiesce,
            &quiesce_id,
            proto::TaskOperationOutcome::Accepted,
            "Quiesce",
        )?;
        let Some(proto::task_operation_receipt::Ack::QuiesceQueryContext(ack)) = &receipt.ack
        else {
            anyhow::bail!("Quiesce control omitted its typed acknowledgement: {receipt:?}");
        };
        ensure!(
            ack.query_context.as_ref() == Some(&releasable)
                && ack.fence_version == 1
                && ack.accepted_tasks.is_empty()
                && proto::QueryContextState::try_from(ack.state)
                    == Ok(proto::QueryContextState::Quiescing),
            "task-free Quiesce did not freeze the exact empty accepted-task set: {ack:?}"
        );
        context.action("Quiesce installed fence version one with an empty accepted-task set under ordinary saturation");

        let release_request = control_release(releasable.clone());
        let release_id = release_request.operations[0]
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.operation_id.clone())
            .context("Release probe omitted operation id")?;
        let release =
            raw_unary_response(&connector, CONTROL_PATH, &authorization, release_request)?;
        let receipt = exact_probe_receipt(
            release,
            &release_id,
            proto::TaskOperationOutcome::Accepted,
            "Release",
        )?;
        let Some(proto::task_operation_receipt::Ack::ReleaseQueryContext(ack)) = &receipt.ack
        else {
            anyhow::bail!("Release control omitted its typed acknowledgement: {receipt:?}");
        };
        ensure!(
            ack.query_context.as_ref() == Some(&releasable)
                && proto::ReleaseQueryContextOutcome::try_from(ack.outcome)
                    == Ok(proto::ReleaseQueryContextOutcome::Released)
                && proto::QueryContextState::try_from(ack.state)
                    == Ok(proto::QueryContextState::TerminalRetained)
                && ack.termination_cause.is_none(),
            "task-free context did not release normally under ordinary saturation: {ack:?}"
        );
        context.action(
            "Release of the exact quiesced, task-free context returned Released and TerminalRetained under ordinary saturation",
        );

        held.release_and_join()?;
        let queued = queued
            .join()
            .map_err(|_| anyhow::anyhow!("queued ordinary probe panicked"))??;
        ensure!(
            queued.grpc_status == 0,
            "queued ordinary probe failed after release: {:?}",
            queued.grpc_message
        );
        wait_ingress_slot(context, "ordinary", "running", 0.0)?;
        wait_ingress_slot(context, "ordinary", "waiting", 0.0)?;
        context.action("ordinary running/waiting returned to zero after release and queue drain");
        Ok(())
    }
}

impl Scenario for ResidentEnvelopeCalibration {
    fn name(&self) -> &'static str {
        "native-ingress/resident-envelope-calibration"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        manifest: Option<&std::path::Path>,
    ) -> Result<()> {
        BlockingSaturationControl.validate_runner_inputs(launch_profile, manifest)
    }

    fn launch_config(&self, scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        BlockingSaturationControl.launch_config(scenario_root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let (connector, authorization, backend, compatibility_id) = probe(context)?;
        let operation = ticket_operation(&connector, &authorization, backend, compatibility_id)?;
        let mut sampler = ProcessResourceSampler::from_identities(
            context.process_resource_identities()?,
            context.name(),
        )?;
        sampler.sample_cluster()?;
        let before = sample_rss(&sampler, "be-0")?;
        let mut held =
            HeldOrdinary::start_with_near_limit(&connector, &authorization, operation, backend, 8)?;
        held.await_entered(context.deadline())?;
        wait_ingress_slot(context, "ordinary", "running", 8.0)?;
        sampler.sample_cluster()?;
        let during = sample_rss(&sampler, "be-0")?;
        held.release_and_join()?;
        wait_ingress_slot(context, "ordinary", "running", 0.0)?;
        sampler.sample_cluster()?;
        let after = sample_rss(&sampler, "be-0")?;
        let path = context.scenario_root().join("process-resources.json");
        sampler.write_json(&path)?;
        if let (Some(before), Some(during)) = (before, during) {
            ensure!(
                during.saturating_sub(before) < 1024 * 1024 * 1024,
                "one near-limit and seven small held Native requests caused an obvious >1 GiB BE RSS jump"
            );
        }
        context.action(format!(
            "BE[0] RSS MiB before/during-one-48MiB-ordinary-plus-seven-small-held/after={}/{}/{}; outer padding is not a frozen-plan carrier; process samples at {}",
            rss_mib(before),
            rss_mib(during),
            rss_mib(after),
            path.display()
        ));
        Ok(())
    }
}

impl Scenario for AsyncSchedulingPressure {
    fn name(&self) -> &'static str {
        "native-ingress/async-scheduling-pressure"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let (connector, authorization, backend, _) = probe(context)?;
        let before = total_shuffle_bytes(context)?;
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let mut workers = Vec::new();
        for _ in 0..4 {
            let user = user.clone();
            let started_tx = started_tx.clone();
            let done_tx = done_tx.clone();
            workers.push(thread::spawn(move || {
                let result = (|| -> Result<()> {
                    let mut connection =
                        mysql_actor::connect(&user, port, Duration::from_secs(60))?;
                    started_tx
                        .send(())
                        .context("publish Exchange query start")?;
                    connection.query_drop(
                        "SELECT sleep(2), COUNT(*) FROM (\
                           SELECT generate_series AS x FROM TABLE(generate_series(1, 100000)) \
                           UNION ALL \
                           SELECT generate_series AS x FROM TABLE(generate_series(100001, 200000))\
                         ) t",
                    )?;
                    Ok(())
                })();
                let _ = done_tx.send(result);
            }));
        }
        drop(started_tx);
        drop(done_tx);
        for _ in 0..4 {
            started_rx.recv_timeout(Duration::from_secs(10))?;
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        let (active_shuffle, ordinary_running, ordinary_waiting) = loop {
            let active_shuffle = total_shuffle_bytes(context)?;
            let rows = read_backend_metrics(context, 0)?;
            let ordinary_running = metric(
                &rows,
                "novarocks_backend_native_ingress_slots",
                &[
                    ("class", "ordinary"),
                    ("phase", "running"),
                    ("dimension", "used"),
                ],
            )?;
            let ordinary_waiting = metric(
                &rows,
                "novarocks_backend_native_ingress_slots",
                &[
                    ("class", "ordinary"),
                    ("phase", "waiting"),
                    ("dimension", "used"),
                ],
            )?;
            if active_shuffle > before {
                break (active_shuffle, ordinary_running, ordinary_waiting);
            }
            ensure!(
                Instant::now() < deadline,
                "distributed query produced no observable Native/Exchange activity"
            );
            thread::yield_now();
        };
        let mut completed = 0;
        while let Ok(result) = done_rx.try_recv() {
            result?;
            completed += 1;
        }
        ensure!(
            completed < 4,
            "all Exchange workload queries completed before control probe"
        );
        context.action(format!(
            "distributed query active before control: shuffle_bytes={active_shuffle}, ordinary_running={ordinary_running}, ordinary_waiting={ordinary_waiting}"
        ));
        let control: RawUnaryResponse<proto::ApplyTaskOperationsResponse> = raw_unary_response(
            &connector,
            CONTROL_PATH,
            &authorization,
            control_cancel(backend),
        )?;
        ensure!(
            control.grpc_status == 0
                && control
                    .message
                    .as_ref()
                    .is_some_and(|body| body.receipts.len() == 1),
            "small Cancel did not progress during distributed Exchange work: status={} detail={:?}",
            control.grpc_status,
            control.grpc_message
        );
        context.action(
            "small Cancel returned a real control receipt while distributed queries were active",
        );
        for _ in completed..4 {
            done_rx.recv_timeout(Duration::from_secs(60))??;
        }
        for worker in workers {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("Exchange workload actor panicked"))?;
        }
        let after = total_shuffle_bytes(context)?;
        ensure!(
            after > before,
            "distributed workload produced no Exchange shuffle bytes"
        );
        context.action(format!("four distributed queries completed; Exchange shuffle bytes before/after={before}/{after}"));
        Ok(())
    }
}

impl Scenario for PartialBodyDeadline {
    fn name(&self) -> &'static str {
        "native-ingress/partial-body-deadline"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let (connector, authorization, backend, _) = probe(context)?;
        let (outcome, elapsed) = partial_body_deadline(&connector, &authorization)?;
        ensure!(
            elapsed < Duration::from_secs(3),
            "stalled partial body exceeded coarse termination bound: {elapsed:?}"
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let rows = read_backend_metrics(context, 0)?;
            let rejected = rows.iter().any(|row| {
                row["tags"]["metric"] == "novarocks_backend_native_ingress_rejections_total"
                    && row["tags"]["class"] == "ordinary"
                    && row["tags"]["reason"] == "running_deadline"
                    && row["value"].as_f64().is_some_and(|value| value >= 1.0)
            });
            if rejected {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "stalled partial body did not publish running_deadline rejection"
            );
            thread::yield_now();
        }
        context.action(format!("ordinary half-open DATA ended after {:?} with {:?}; running_deadline count advanced (RST has no readable gRPC status)", elapsed, outcome));
        wait_ingress_slot(context, "ordinary", "running", 0.0)?;
        let control: RawUnaryResponse<proto::ApplyTaskOperationsResponse> = raw_unary_response(
            &connector,
            CONTROL_PATH,
            &authorization,
            control_cancel(backend),
        )?;
        ensure!(
            control.grpc_status == 0
                && control
                    .message
                    .as_ref()
                    .is_some_and(|body| body.receipts.len() == 1),
            "small control did not progress after partial-body timeout: {:?}",
            control.grpc_message
        );
        context.action("partial-body holder returned and a later small Cancel completed");
        Ok(())
    }
}

impl Scenario for RegistryContentionControl {
    fn name(&self) -> &'static str {
        "native-ingress/registry-contention-control"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        manifest: Option<&std::path::Path>,
    ) -> Result<()> {
        BlockingSaturationControl.validate_runner_inputs(launch_profile, manifest)
    }

    fn launch_config(&self, scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        fs::create_dir_all(scenario_root)?;
        let token = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        fs::write(scenario_root.join("registry-hold-token"), &token)?;
        let mut launch = ScenarioLaunchConfig::default();
        launch
            .child_environment
            .be_by_index
            .entry(0)
            .or_default()
            .insert(
                "NOVAROCKS_SQL_TEST_NATIVE_REGISTRY_HOLD_TOKEN".to_owned(),
                token,
            );
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let token = fs::read_to_string(context.scenario_root().join("registry-hold-token"))?;
        let mut rendezvous = RegistryRendezvous::bind(&token)?;
        let (connector, authorization, backend, compatibility_id) = probe(context)?;
        let operation = ticket_operation(&connector, &authorization, backend, compatibility_id)?;
        let before = read_backend_metrics(context, 0)?;
        let control_started_before = metric_or_zero(
            &before,
            "novarocks_backend_native_control_queue_wait_seconds_count",
            &[("outcome", "started")],
        )?;
        let registry_wait_before = metric(
            &before,
            "novarocks_backend_worker_registry_lock_observation",
            &[("phase", "wait"), ("statistic", "samples")],
        )?;
        let acquire_connector = connector.clone();
        let acquire_authorization = authorization.clone();
        let acquire = thread::spawn(move || {
            raw_unary_response::<_, proto::ApplyTaskOperationsResponse>(
                &acquire_connector,
                ORDINARY_PATH,
                &acquire_authorization,
                proto::ApplyTaskOperationsRequest {
                    operations: vec![operation],
                },
            )
        });
        rendezvous.await_entered(backend, context.deadline())?;
        context.action("BE[0] first Acquire held the real Worker registry mutex before mutation");

        let control_connector = connector.clone();
        let control_authorization = authorization.clone();
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let control_thread = thread::spawn(move || {
            let result = raw_unary_response::<_, proto::ApplyTaskOperationsResponse>(
                &control_connector,
                CONTROL_PATH,
                &control_authorization,
                control_cancel(backend),
            );
            let _ = done_tx.send(result);
        });
        let deadline = context
            .deadline()
            .min(Instant::now() + Duration::from_secs(10));
        loop {
            let rows = read_backend_metrics(context, 0)?;
            let started = metric_or_zero(
                &rows,
                "novarocks_backend_native_control_queue_wait_seconds_count",
                &[("outcome", "started")],
            )?;
            let running = metric(
                &rows,
                "novarocks_backend_native_ingress_slots",
                &[
                    ("class", "control"),
                    ("phase", "running"),
                    ("dimension", "used"),
                ],
            )?;
            if started > control_started_before && running == 1.0 {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "control executor did not start while registry lock was held: started={started} running={running}"
            );
            thread::yield_now();
        }
        ensure!(
            matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "control completed before Worker registry lock was released"
        );
        context.action("control executor started and held one control-running slot, but its receipt waited for the Worker mutex");
        rendezvous.release()?;
        let acquisition = acquire
            .join()
            .map_err(|_| anyhow::anyhow!("registry Acquire actor panicked"))??;
        ensure!(
            acquisition.grpc_status == 0,
            "registry Acquire failed after release: {:?}",
            acquisition.grpc_message
        );
        let control = done_rx.recv_timeout(Duration::from_secs(10))??;
        control_thread
            .join()
            .map_err(|_| anyhow::anyhow!("registry control actor panicked"))?;
        ensure!(
            control.grpc_status == 0
                && control
                    .message
                    .as_ref()
                    .is_some_and(|body| body.receipts.len() == 1),
            "control did not complete after registry release: {:?}",
            control.grpc_message
        );
        wait_ingress_slot(context, "control", "running", 0.0)?;
        let after = read_backend_metrics(context, 0)?;
        let registry_wait_after = metric(
            &after,
            "novarocks_backend_worker_registry_lock_observation",
            &[("phase", "wait"), ("statistic", "samples")],
        )?;
        ensure!(
            registry_wait_after > registry_wait_before,
            "Worker lock wait samples did not advance after true contention"
        );
        context.action(format!("registry lock released; control receipt completed, lock wait samples before/after={registry_wait_before}/{registry_wait_after}"));
        Ok(())
    }
}

fn metric_or_zero(rows: &[Value], name: &str, labels: &[(&str, &str)]) -> Result<f64> {
    if rows.iter().any(|row| {
        row["tags"]["metric"] == name
            && labels
                .iter()
                .all(|(key, value)| row["tags"][*key] == *value)
    }) {
        metric(rows, name, labels)
    } else {
        Ok(0.0)
    }
}

struct RegistryRendezvous {
    path: PathBuf,
    listener: UnixListener,
    stream: Option<UnixStream>,
    token: String,
}

impl RegistryRendezvous {
    fn bind(token: &str) -> Result<Self> {
        let path = novarocks_failpoint::native_registry_hold_socket_path(token)
            .map_err(anyhow::Error::msg)?;
        let listener = UnixListener::bind(&path)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            path,
            listener,
            stream: None,
            token: token.to_owned(),
        })
    }

    fn await_entered(
        &mut self,
        backend: BackendProcessId,
        scenario_deadline: Instant,
    ) -> Result<()> {
        let deadline = scenario_deadline.min(Instant::now() + Duration::from_secs(10));
        let (mut stream, _) = loop {
            match self.listener.accept() {
                Ok(accepted) => break accepted,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    ensure!(
                        Instant::now() < deadline,
                        "Worker registry lock rendezvous did not arrive"
                    );
                    thread::yield_now();
                }
                Err(error) => return Err(error.into()),
            }
        };
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let mut marker = String::new();
        stream.read_to_string(&mut marker)?;
        ensure!(
            marker == format!("NIR1 0 {backend} {}\n", self.token),
            "unexpected Worker registry marker: {marker:?}"
        );
        self.stream = Some(stream);
        Ok(())
    }

    fn release(&mut self) -> Result<()> {
        self.stream
            .as_mut()
            .context("registry hold has not entered")?
            .write_all(b"R")?;
        self.stream = None;
        Ok(())
    }
}

impl Drop for RegistryRendezvous {
    fn drop(&mut self) {
        if let Some(stream) = &mut self.stream {
            let _ = stream.write_all(b"R");
        }
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Debug)]
enum PartialBodyOutcome {
    GrpcDeadline,
    TransportAbort,
}

fn partial_body_deadline(
    connector: &NativeEndpointConnector,
    authorization: &str,
) -> Result<(PartialBodyOutcome, Duration)> {
    let connector = connector.clone();
    let authorization = authorization.to_owned();
    tokio::runtime::Runtime::new()?.block_on(async move {
        let stream = connector.connect().await.map_err(anyhow::Error::msg)?;
        let (mut sender, connection) = client::handshake(stream).await?;
        let driver = tokio::spawn(async move { connection.await });
        let request = Request::builder()
            .method("POST")
            .uri(ORDINARY_PATH)
            .header(header::CONTENT_TYPE, "application/grpc")
            .header("te", "trailers")
            .header(header::AUTHORIZATION, authorization)
            .header("grpc-timeout", "100m")
            .body(())?;
        let (response, mut send_stream) = sender.send_request(request, false)?;
        send_stream.send_data(Bytes::from_static(&[0, 0, 0, 0, 100, 0]), false)?;
        let started = Instant::now();
        let response = tokio::time::timeout(Duration::from_secs(3), response)
            .await
            .context("partial-body request was not rejected within a coarse 3-second bound")?;
        let response = match response {
            Ok(response) => response,
            Err(_) => {
                driver.abort();
                let _ = driver.await;
                return Ok((PartialBodyOutcome::TransportAbort, started.elapsed()));
            }
        };
        let status = response
            .headers()
            .get("grpc-status")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u16>().ok());
        let mut body = response.into_body();
        while let Some(chunk) = body.data().await {
            if chunk.is_err() {
                driver.abort();
                let _ = driver.await;
                return Ok((PartialBodyOutcome::TransportAbort, started.elapsed()));
            }
        }
        let trailers = match body.trailers().await {
            Ok(trailers) => trailers,
            Err(_) => {
                driver.abort();
                let _ = driver.await;
                return Ok((PartialBodyOutcome::TransportAbort, started.elapsed()));
            }
        };
        let status = status
            .or_else(|| {
                trailers
                    .as_ref()
                    .and_then(|headers| headers.get("grpc-status"))
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u16>().ok())
            })
            .context("partial-body rejection omitted grpc-status")?;
        driver.abort();
        let _ = driver.await;
        ensure!(
            status == tonic::Code::DeadlineExceeded as u16,
            "partial-body grpc-status={status}"
        );
        Ok((PartialBodyOutcome::GrpcDeadline, started.elapsed()))
    })
}

fn total_shuffle_bytes(context: &mut ScenarioContext) -> Result<f64> {
    let mut total = 0.0;
    for index in 0..REQUIRED_BACKENDS {
        let rows = read_backend_metrics(context, index)?;
        total += metric(&rows, "novarocks_exchange_shuffle_bytes_total", &[])?;
    }
    Ok(total)
}

fn sample_rss(sampler: &ProcessResourceSampler, role: &str) -> Result<Option<u64>> {
    sampler
        .samples()
        .iter()
        .rev()
        .find(|sample| sample.role == role)
        .map(|sample| sample.rss_bytes)
        .with_context(|| format!("missing {role} process sample"))
}

fn rss_mib(bytes: Option<u64>) -> String {
    bytes.map_or_else(
        || "unavailable".to_owned(),
        |bytes| format!("{:.1}", bytes as f64 / 1048576.0),
    )
}

fn read_backend_metrics(context: &mut ScenarioContext, index: usize) -> Result<Vec<Value>> {
    let port = context
        .handle()
        .runtime()
        .be
        .get(index)
        .context("BE metrics target index out of range")?
        .http;
    let url = format!("http://127.0.0.1:{port}/metrics?type=json");
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let rows: Value = client.get(&url).send()?.error_for_status()?.json()?;
    rows.as_array()
        .cloned()
        .context("BE metrics JSON was not an array")
}

fn wait_ingress_slot(
    context: &mut ScenarioContext,
    class: &str,
    phase: &str,
    expected: f64,
) -> Result<()> {
    let deadline = context
        .deadline()
        .min(Instant::now() + Duration::from_secs(10));
    loop {
        let rows = read_backend_metrics(context, 0)?;
        let used = metric(
            &rows,
            "novarocks_backend_native_ingress_slots",
            &[("class", class), ("phase", phase), ("dimension", "used")],
        )?;
        if used == expected {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "{class} {phase} slot count did not reach {expected}; last={used}"
        );
        thread::yield_now();
    }
}

fn ticket_operation(
    connector: &NativeEndpointConnector,
    authorization: &str,
    backend: BackendProcessId,
    compatibility_id: [u8; 32],
) -> Result<proto::TaskOperation> {
    let heartbeat: RawUnaryResponse<proto::HeartbeatResponse> = raw_unary_response(
        connector,
        HEARTBEAT_PATH,
        authorization,
        proto::HeartbeatRequest {
            expected_process_id: Some(proto::BackendProcessId {
                value: backend.to_bytes().to_vec(),
            }),
        },
    )?;
    ensure!(
        heartbeat.grpc_status == 0,
        "heartbeat failed before held ordinary probe"
    );
    let epoch = heartbeat
        .message
        .context("heartbeat omitted response")?
        .admission_epoch_capability
        .context("heartbeat omitted admission epoch")?;
    Ok(raw_acquire_admission_ticket(
        raw_query_context(backend),
        compatibility_id,
        epoch,
    ))
}

fn establish_releasable_context(
    connector: &NativeEndpointConnector,
    authorization: &str,
    backend: BackendProcessId,
    compatibility_id: [u8; 32],
) -> Result<proto::QueryContextRef> {
    let acquisition = ticket_operation(connector, authorization, backend, compatibility_id)?;
    let context = match acquisition.operation.as_ref() {
        Some(proto::task_operation::Operation::AcquireQueryContextAdmissionTicket(request)) => {
            request
                .query_context
                .clone()
                .context("ticket operation omitted context")?
        }
        _ => anyhow::bail!("ticket helper produced wrong operation"),
    };
    let acquisition_id = acquisition
        .envelope
        .as_ref()
        .and_then(|envelope| envelope.operation_id.clone())
        .context("ticket acquisition omitted operation id")?;
    let response = raw_unary_response(
        connector,
        ORDINARY_PATH,
        authorization,
        proto::ApplyTaskOperationsRequest {
            operations: vec![acquisition],
        },
    )?;
    let receipt = exact_probe_receipt(
        response,
        &acquisition_id,
        proto::TaskOperationOutcome::Accepted,
        "Admission ticket",
    )?;
    let Some(proto::task_operation_receipt::Ack::QueryContextAdmissionTicket(ticket)) = receipt.ack
    else {
        anyhow::bail!("ticket acquisition omitted ticket acknowledgement");
    };
    ensure!(
        ticket.query_context.as_ref() == Some(&context) && ticket.valid_for_millis == 10_000,
        "admission ticket did not bind the exact context and requested validity: {ticket:?}"
    );
    let ticket_id = ticket
        .ticket_id
        .context("ticket acknowledgement omitted id")?;
    let operation = raw_establish(context.clone(), ticket_id, Some(compatibility_id.to_vec()));
    let establish_id = operation
        .envelope
        .as_ref()
        .and_then(|envelope| envelope.operation_id.clone())
        .context("Establish probe omitted operation id")?;
    let establish = raw_unary_response(
        connector,
        ORDINARY_PATH,
        authorization,
        proto::ApplyTaskOperationsRequest {
            operations: vec![operation],
        },
    )?;
    let receipt = exact_probe_receipt(
        establish,
        &establish_id,
        proto::TaskOperationOutcome::Accepted,
        "Establish",
    )?;
    let Some(proto::task_operation_receipt::Ack::QueryContext(ack)) = receipt.ack else {
        anyhow::bail!("Establish omitted its typed context acknowledgement");
    };
    let lease = ack
        .lease
        .as_ref()
        .context("Establish omitted its initial lease receipt")?;
    ensure!(
        ack.query_context.as_ref() == Some(&context)
            && proto::QueryContextState::try_from(ack.state)
                == Ok(proto::QueryContextState::Active)
            && ack.termination_cause.is_none()
            && ack.accepted_domains.is_empty()
            && lease.sequence == 0
            && lease.requested_valid_for_millis == 30_000
            && lease.effective_valid_for_millis > 0
            && lease.effective_valid_for_millis <= lease.requested_valid_for_millis,
        "Establish did not install the exact active context and initial facts: {ack:?}"
    );
    Ok(context)
}

fn control_cancel(backend: BackendProcessId) -> proto::ApplyTaskControlOperationsRequest {
    proto::ApplyTaskControlOperationsRequest {
        operations: vec![proto::TaskControlOperation {
            envelope: Some(raw_operation_envelope(15_000)),
            control: Some(proto::task_control_operation::Control::CancelTask(
                proto::CancelTaskRequest {
                    identity: Some(proto::TaskIdentity {
                        query_execution_id: raw_query_context(backend).query_execution_id,
                        stage_id: 1,
                        task_id: 1,
                        backend_process_id: Some(proto::BackendProcessId {
                            value: backend.to_bytes().to_vec(),
                        }),
                    }),
                    reason: proto::TaskCancelReason::UpstreamNoLongerNeeded as i32,
                },
            )),
        }],
    }
}

fn exact_probe_receipt(
    response: RawUnaryResponse<proto::ApplyTaskOperationsResponse>,
    operation_id: &proto::TaskOperationId,
    expected: proto::TaskOperationOutcome,
    label: &str,
) -> Result<proto::TaskOperationReceipt> {
    ensure!(
        response.grpc_status == 0,
        "{label} failed at transport: status={} detail={:?}",
        response.grpc_status,
        response.grpc_message
    );
    let mut body = response
        .message
        .context("Native control probe omitted response")?;
    ensure!(
        body.receipts.len() == 1,
        "{label} expected exactly one receipt, got {}",
        body.receipts.len()
    );
    let receipt = body.receipts.remove(0);
    ensure!(
        receipt.operation_id.as_ref() == Some(operation_id)
            && proto::TaskOperationOutcome::try_from(receipt.outcome) == Ok(expected),
        "{label} did not return the exact operation and expected owner outcome: {receipt:?}"
    );
    Ok(receipt)
}

fn control_quiesce(context: proto::QueryContextRef) -> proto::ApplyTaskControlOperationsRequest {
    proto::ApplyTaskControlOperationsRequest {
        operations: vec![proto::TaskControlOperation {
            envelope: Some(raw_operation_envelope(15_000)),
            control: Some(proto::task_control_operation::Control::QuiesceQueryContext(
                proto::QuiesceQueryContextRequest {
                    query_context: Some(context),
                },
            )),
        }],
    }
}

fn control_release(context: proto::QueryContextRef) -> proto::ApplyTaskControlOperationsRequest {
    proto::ApplyTaskControlOperationsRequest {
        operations: vec![proto::TaskControlOperation {
            envelope: Some(raw_operation_envelope(15_000)),
            control: Some(proto::task_control_operation::Control::ReleaseQueryContext(
                proto::ReleaseQueryContextRequest {
                    query_context: Some(context),
                },
            )),
        }],
    }
}

struct HeldCall {
    token: String,
    path: PathBuf,
    listener: UnixListener,
    stream: Option<UnixStream>,
    response:
        Option<thread::JoinHandle<Result<RawUnaryResponse<proto::ApplyTaskOperationsResponse>>>>,
}

struct HeldOrdinary {
    calls: Vec<HeldCall>,
    backend: BackendProcessId,
}

impl HeldOrdinary {
    fn start(
        connector: &NativeEndpointConnector,
        authorization: &str,
        operation: proto::TaskOperation,
        backend: BackendProcessId,
        count: usize,
    ) -> Result<Self> {
        Self::start_inner(connector, authorization, operation, backend, count, false)
    }

    fn start_with_near_limit(
        connector: &NativeEndpointConnector,
        authorization: &str,
        operation: proto::TaskOperation,
        backend: BackendProcessId,
        count: usize,
    ) -> Result<Self> {
        Self::start_inner(connector, authorization, operation, backend, count, true)
    }

    fn start_inner(
        connector: &NativeEndpointConnector,
        authorization: &str,
        operation: proto::TaskOperation,
        backend: BackendProcessId,
        count: usize,
        near_limit_first: bool,
    ) -> Result<Self> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mut calls = Vec::with_capacity(count);
        for index in 0..count {
            let token = format!("{}-{nonce}-{index}", std::process::id());
            let path = novarocks_failpoint::native_ingress_hold_socket_path(&token)
                .map_err(anyhow::Error::msg)?;
            let listener = UnixListener::bind(&path)
                .with_context(|| format!("bind Native hold rendezvous {}", path.display()))?;
            listener.set_nonblocking(true)?;
            let connector = connector.clone();
            let authorization = authorization.to_owned();
            let request = if near_limit_first && index == 0 {
                Some(pad_ordinary(operation.clone(), ORDINARY_BATCH_MAX)?)
            } else {
                None
            };
            let small_request = proto::ApplyTaskOperationsRequest {
                operations: vec![operation.clone()],
            };
            let token_for_request = token.clone();
            let response = thread::spawn(move || {
                if let Some(request) = request {
                    raw_unary_response_with_hold::<_, proto::ApplyTaskOperationsResponse>(
                        &connector,
                        ORDINARY_PATH,
                        &authorization,
                        request,
                        Some(&token_for_request),
                    )
                } else {
                    raw_unary_response_with_hold::<_, proto::ApplyTaskOperationsResponse>(
                        &connector,
                        ORDINARY_PATH,
                        &authorization,
                        small_request,
                        Some(&token_for_request),
                    )
                }
            });
            calls.push(HeldCall {
                token,
                path,
                listener,
                stream: None,
                response: Some(response),
            });
        }
        Ok(Self { calls, backend })
    }

    fn await_entered(&mut self, scenario_deadline: Instant) -> Result<()> {
        let deadline = scenario_deadline.min(Instant::now() + Duration::from_secs(10));
        for call in &mut self.calls {
            let (mut stream, _) = loop {
                match call.listener.accept() {
                    Ok(accepted) => break accepted,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        ensure!(
                            Instant::now() < deadline,
                            "ordinary closure did not enter token={}",
                            call.token
                        );
                        thread::yield_now();
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            stream.set_nonblocking(false)?;
            stream.set_read_timeout(Some(Duration::from_secs(5)))?;
            let mut marker = String::new();
            stream.read_to_string(&mut marker)?;
            ensure!(
                marker == format!("NIH1 0 {} {}\n", self.backend, call.token),
                "unexpected Native hold marker: {marker:?}"
            );
            call.stream = Some(stream);
        }
        Ok(())
    }

    fn release_and_join(&mut self) -> Result<()> {
        for call in &mut self.calls {
            if let Some(stream) = &mut call.stream {
                stream.write_all(b"R")?;
            }
        }
        for call in &mut self.calls {
            let response = call
                .response
                .take()
                .context("held ordinary response already joined")?
                .join()
                .map_err(|_| anyhow::anyhow!("held ordinary probe panicked"))??;
            ensure!(
                response.grpc_status == 0
                    && response
                        .message
                        .as_ref()
                        .is_some_and(|body| body.receipts.len() == 1),
                "held ordinary probe failed: status={} detail={:?}",
                response.grpc_status,
                response.grpc_message
            );
        }
        Ok(())
    }
}

impl Drop for HeldOrdinary {
    fn drop(&mut self) {
        for call in &mut self.calls {
            if let Some(stream) = &mut call.stream {
                let _ = stream.write_all(b"R");
            }
            let _ = fs::remove_file(&call.path);
        }
    }
}

fn metric(rows: &[Value], name: &str, labels: &[(&str, &str)]) -> Result<f64> {
    let mut matches = rows.iter().filter(|row| {
        row["tags"]["metric"] == name
            && labels
                .iter()
                .all(|(key, value)| row["tags"][*key] == *value)
    });
    let row = matches
        .next()
        .with_context(|| format!("missing BE metric {name} {labels:?}"))?;
    ensure!(
        matches.next().is_none(),
        "duplicate BE metric {name} {labels:?}"
    );
    row["value"]
        .as_f64()
        .with_context(|| format!("non-numeric BE metric {name} {labels:?}"))
}

fn probe(
    context: &mut ScenarioContext,
) -> Result<(NativeEndpointConnector, String, BackendProcessId, [u8; 32])> {
    ensure!(
        context.handle().be_count() == REQUIRED_BACKENDS,
        "{} requires a real 1FE+3BE cluster",
        context.name()
    );
    let port = context.handle().runtime().be[0].grpc;
    let rows = context.handle().frontend_backend_topology()?;
    let row = rows
        .iter()
        .find(|row| row.grpc_port == port)
        .context("SHOW BACKENDS omitted the Native ingress target")?;
    ensure!(
        row.is_eligible_live(),
        "Native ingress target is not eligible-live"
    );
    let backend = row.process_id.parse::<BackendProcessId>()?;
    let compatibility_id = hex_32(&row.native_compatibility_id)?;
    let endpoint = context.handle().native_be_endpoint(0)?;
    let trust_mode = context.handle().native_trust_mode();
    let connector = context
        .handle()
        .native_probe_connector(endpoint, trust_mode)?;
    let authorization = authorization_header(&context.handle().native_probe_trust()?)?;
    Ok((connector, authorization, backend, compatibility_id))
}

fn hex_32(value: &str) -> Result<[u8; 32]> {
    ensure!(value.len() == 64, "expected 64 hex digits");
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(bytes)
}

fn pad_ordinary(operation: proto::TaskOperation, target: usize) -> Result<PaddedOperations> {
    let mut request = PaddedOperations {
        operations: vec![operation],
        padding: Vec::new(),
    };
    let length = padding_len(target, request.encoded_len())?;
    request.padding.resize(length, 0);
    ensure!(
        request.encoded_len() == target,
        "ordinary padding is inexact"
    );
    Ok(request)
}

fn pad_control(operation: proto::TaskControlOperation, target: usize) -> Result<PaddedControls> {
    let mut request = PaddedControls {
        operations: vec![operation],
        padding: Vec::new(),
    };
    let length = padding_len(target, request.encoded_len())?;
    request.padding.resize(length, 0);
    ensure!(
        request.encoded_len() == target,
        "control padding is inexact"
    );
    Ok(request)
}

fn padding_len(target: usize, base: usize) -> Result<usize> {
    ensure!(
        target >= base + 8,
        "target frame is smaller than the request"
    );
    // Field 2047 has a two-byte key. A nonempty bytes field also has a
    // varint length. Solve that small self-reference without allocating the
    // potentially 48 MiB padding on each candidate.
    let mut length = target - base - 3;
    for _ in 0..3 {
        let actual = base + 2 + varint_len(length) + length;
        if actual == target {
            return Ok(length);
        }
        length = if actual < target {
            length + target - actual
        } else {
            length - (actual - target)
        };
    }
    anyhow::bail!("padding length did not converge")
}

fn varint_len(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 128 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

fn header_rejection(
    connector: &NativeEndpointConnector,
    path: &str,
    authorization: &str,
    extra_name: &str,
    extra_value: &str,
) -> Result<tonic::Status> {
    let connector = connector.clone();
    let path = path.to_owned();
    let authorization = authorization.to_owned();
    let extra_name = extra_name.to_owned();
    let extra_value = extra_value.to_owned();
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
            .header(extra_name, extra_value)
            .body(())?;
        let (response, _unwritten_body) = sender.send_request(request, false)?;
        let response = tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .context("Native ingress did not reject headers before reading the body")??;
        let status = tonic::Status::from_header_map(response.headers())
            .context("early Native ingress rejection omitted grpc-status")?;
        driver.abort();
        let _ = driver.await;
        Ok(status)
    })
}

fn streamed_control_over_limit(
    connector: &NativeEndpointConnector,
    authorization: &str,
) -> Result<tonic::Status> {
    let connector = connector.clone();
    let authorization = authorization.to_owned();
    tokio::runtime::Runtime::new()?.block_on(async move {
        let stream = connector.connect().await.map_err(anyhow::Error::msg)?;
        let (mut sender, connection) = client::handshake(stream).await?;
        let driver = tokio::spawn(async move { connection.await });
        let request = Request::builder()
            .method("POST")
            .uri(CONTROL_PATH)
            .header(header::CONTENT_TYPE, "application/grpc")
            .header("te", "trailers")
            .header(header::AUTHORIZATION, authorization)
            .body(())?;
        let (response, mut send_stream) = sender.send_request(request, false)?;
        let mut bytes = vec![0; CONTROL_FRAME_MAX + 6];
        bytes[1..5].copy_from_slice(&(CONTROL_FRAME_MAX as u32).to_be_bytes());
        send_stream.send_data(Bytes::from(bytes), true)?;
        let response = tokio::time::timeout(Duration::from_secs(10), response).await??;
        let header_status = tonic::Status::from_header_map(response.headers());
        let mut body = response.into_body();
        while let Some(chunk) = body.data().await {
            let _ = chunk?;
        }
        let trailers = body.trailers().await?;
        let status = header_status
            .or_else(|| trailers.as_ref().and_then(tonic::Status::from_header_map))
            .context("streaming size rejection omitted grpc-status")?;
        driver.abort();
        let _ = driver.await;
        Ok(status)
    })
}
