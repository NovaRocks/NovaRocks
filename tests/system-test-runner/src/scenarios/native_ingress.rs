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

use super::native_creation::{
    RawBackendSession, RawCreate, await_installed, ensure_accepted,
    observe_all_canceled_and_stopped, outcome, quiesce_session, raw_create_identity,
    receipt_verdict, session_control,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use h2::client;
use http::{Request, header};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_native_trust::NativeEndpointConnector;
use novarocks_proto_models::novarocks as proto;
use novarocks_types::BackendProcessId;
use prost::Message;
use reqwest::blocking::Client;
use serde_json::Value;
use std::time::Duration;
use std::{
    fs,
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::Instant,
};

use crate::{
    actors::mysql as mysql_actor,
    scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig},
};
use novarocks_cluster_harness::LaunchProfile;

use super::native_direct_controls::{
    DirectIntent, DirectIntentOwner, close_hold, read_marker, release_hold,
};

use super::native_compatibility::{
    RawUnaryConnection, RawUnaryResponse, authorization_header, raw_acquire_admission_ticket,
    raw_establish, raw_operation_envelope, raw_participation, raw_unary_response,
    raw_unary_response_until,
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
        Box::new(CurrentGateObservation::default()),
        Box::new(BlockingSaturationControl::default()),
        Box::new(AsyncSchedulingPressure),
        Box::new(PartialBodyDeadline),
        Box::new(RegistryContentionControl::default()),
    ]
}

struct OuterPreflightRejection;
struct QueryMessageBounds;
#[derive(Default)]
struct CurrentGateObservation {
    intent: DirectIntentOwner,
}
#[derive(Default)]
struct BlockingSaturationControl {
    intent: DirectIntentOwner,
}
struct AsyncSchedulingPressure;
struct PartialBodyDeadline;
#[derive(Default)]
struct RegistryContentionControl {
    intent: DirectIntentOwner,
}

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
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_ingress",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "native-ingress/outer-preflight-rejection"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let (connector, control_connector, authorization, _, _) = probe(context)?;
        let before_worker = node_hot_resources(&read_backend_metrics(context, 0)?)?;
        let bad_timeout = header_rejection(
            &control_connector,
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
            &control_connector,
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
            context.deadline(),
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
        let after_worker = node_hot_resources(&rows)?;
        let tasks_created = metric(
            &rows,
            "novarocks_backend_task_execution_tasks_created_total",
            &[],
        )?;
        ensure!(
            after_worker == before_worker && tasks_created == 0.0,
            "preflight rejection escaped into Worker: before={before_worker:?} after={after_worker:?} created={tasks_created}"
        );
        context.action("Tower body_limit counters advanced; Worker reservations and created tasks remained zero");
        Ok(())
    }
}

impl Scenario for QueryMessageBounds {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_ingress",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "native-ingress/query-message-bounds"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let (connector, control_connector, authorization, backend, compatibility_id) =
            probe(context)?;
        let heartbeat: RawUnaryResponse<proto::HeartbeatResponse> = raw_unary_response(
            &control_connector,
            HEARTBEAT_PATH,
            &authorization,
            proto::HeartbeatRequest {
                expected_process_id: Some(proto::BackendProcessId {
                    value: backend.to_bytes().to_vec(),
                }),
            },
            context.deadline(),
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
        let binary =
            raw_acquire_admission_ticket(raw_participation(backend), compatibility_id, epoch);
        for (size, accepted) in [(ORDINARY_BATCH_MAX, true), (ORDINARY_BATCH_MAX + 1, false)] {
            let request = pad_ordinary(binary.clone(), size)?;
            let response: RawUnaryResponse<proto::ApplyTaskOperationsResponse> =
                raw_unary_response(
                    &connector,
                    ORDINARY_PATH,
                    &authorization,
                    request,
                    context.deadline(),
                )?;
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
                        query_execution_id: raw_participation(backend).query_execution_id,
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
                raw_unary_response(
                    &control_connector,
                    CONTROL_PATH,
                    &authorization,
                    request,
                    context.deadline(),
                )?;
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
        let streamed_status = streamed_control_over_limit(&control_connector, &authorization)?;
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
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_ingress",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "native-ingress/current-gate-observation"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, launch_profile: LaunchProfile) -> Result<()> {
        ensure!(
            launch_profile == LaunchProfile::FaultScenario,
            "dynamic gate observation requires --launch-profile fault-scenario"
        );
        Ok(())
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let targets = (0..1).map(|query| (0, query, None)).collect::<Vec<_>>();
        Ok(Some(self.intent.freeze(
            "ingress-hold",
            1,
            &targets,
            false,
            cluster_size,
            binary,
            deadline,
        )?))
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
            let available = metric(
                rows,
                "novarocks_backend_worker_node_snapshot_available",
                &[],
            )?;
            ensure!(
                available == 1.0,
                "BE[{index}] original Node resource snapshot is unavailable"
            );
            for resource in [
                "active_tasks",
                "preparing_tasks",
                "preparing_bytes",
                "requests",
            ] {
                let used = metric(
                    rows,
                    "novarocks_backend_worker_node_resources",
                    &[("resource", resource)],
                )?;
                ensure!(
                    used == 0.0,
                    "BE[{index}] original Node {resource} was live at rest: {used}"
                );
            }
            for (source, expected) in [
                ("native_ingress", 1.0),
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
            context.action(format!("BE[{index}] current Native slots and actual Node charged resources exposed at rest; future Exchange/MEM readouts explicitly unavailable"));
        }
        let (connector, control_connector, authorization, backend, compatibility_id) =
            probe(context)?;
        let intent = self.intent.get()?;
        intent.verify_backend(0, backend)?;
        ensure!(
            context.deadline() == intent.deadline,
            "Native ingress deadline changed after freeze"
        );
        let operations = held_operations(
            &intent,
            &control_connector,
            &authorization,
            backend,
            compatibility_id,
        )?;
        let mut held = HeldOrdinary::start(
            &connector,
            &authorization,
            operations,
            backend,
            intent.deadline,
        )?;
        let primary = (|| -> Result<()> {
            held.await_entered(context.deadline())?;
            wait_ingress_slot(context, "ordinary", "running", 1.0)?;
            context.action(
                "BE[0] ordinary running slot rose to one during its original held asynchronous Native request",
            );
            held.release_and_join()?;
            wait_ingress_slot(context, "ordinary", "running", 0.0)?;
            context.action("BE[0] ordinary running slot returned to zero after the original Native request exited");
            Ok(())
        })();
        held.finish_after(primary)
    }
}

impl Scenario for BlockingSaturationControl {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_ingress",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "native-ingress/blocking-saturation-control"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, launch_profile: LaunchProfile) -> Result<()> {
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

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let targets = (0..8).map(|query| (0, query, None)).collect::<Vec<_>>();
        Ok(Some(self.intent.freeze(
            "ingress-hold",
            10,
            &targets,
            false,
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let (connector, control_connector, authorization, backend, compatibility_id) =
            probe(context)?;
        let intent = self.intent.get()?;
        intent.verify_backend(0, backend)?;
        ensure!(
            context.deadline() == intent.deadline,
            "Native ingress deadline changed after freeze"
        );
        // Positively observe the initial real owner inventory; do not assume
        // that a freshly launched process has no active Participation custody.
        wait_execution_custody(context, 0.0)?;
        let operations = held_operations(
            &intent,
            &control_connector,
            &authorization,
            backend,
            compatibility_id,
        )?;
        let operation = ticket_operation(
            &control_connector,
            &authorization,
            backend,
            compatibility_id,
            context.deadline(),
        )?;
        let releasable_query = intent
            .queries
            .get(8)
            .context("missing declared control Participation")?;
        let (releasable, _) = establish_declared_participation(
            &connector,
            &control_connector,
            &authorization,
            backend,
            compatibility_id,
            (releasable_query.participation(backend), None),
            context.deadline(),
        )?;
        let expiring_query = intent
            .queries
            .get(9)
            .context("missing declared timer Participation")?;
        let (expiring, expiring_lease) = establish_declared_participation(
            &connector,
            &control_connector,
            &authorization,
            backend,
            compatibility_id,
            (expiring_query.participation(backend), Some(1_000)),
            context.deadline(),
        )?;
        let before_expiry = wait_execution_custody(context, 2.0)?;
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
        let mut held = HeldOrdinary::start(
            &connector,
            &authorization,
            operations,
            backend,
            intent.deadline,
        )?;
        let primary = (|| -> Result<()> {
            held.await_entered(context.deadline())?;
            wait_ingress_slot(context, "ordinary", "running", 8.0)?;
            context.action("BE[0] eight original asynchronous Native requests retained their actual ordinary running permits before Worker transfer; the ordinary running gate is full");

            held.start_waiter(&authorization, operation.clone())?;
            wait_ingress_slot(context, "ordinary", "waiting", 1.0)?;
            context.action("ninth ordinary request visibly waited at Native gate");

            // Management sampling cannot deliver a Worker input to this
            // Participation. Its actual execution Scope must disappear BEFORE a
            // typed RPC probe could itself observe time and expire the lease.
            let after_expiry = wait_execution_custody(context, 1.0)?;
            ensure!(
                metric(
                    &after_expiry,
                    "novarocks_backend_native_ingress_slots",
                    &[
                        ("class", "ordinary"),
                        ("phase", "running"),
                        ("dimension", "used")
                    ]
                )? == 8.0
                    && metric(
                        &after_expiry,
                        "novarocks_backend_native_ingress_slots",
                        &[
                            ("class", "ordinary"),
                            ("phase", "waiting"),
                            ("dimension", "used")
                        ]
                    )? == 1.0,
                "original ordinary saturation ended before the independent timer observation"
            );
            let probe = proto::ApplyTaskControlOperationsRequest {
                operations: vec![proto::TaskControlOperation {
                    envelope: Some(raw_operation_envelope(15_000)),
                    control: Some(proto::task_control_operation::Control::RenewLease(
                        proto::RenewParticipationLeaseRequest {
                            participation: Some(expiring.clone()),
                            lease: Some(proto::ParticipationLeaseGrant {
                                sequence: expiring_lease
                                    .sequence
                                    .checked_add(1)
                                    .context("timer Participation lease sequence overflow")?,
                                valid_for_millis: expiring_lease.requested_valid_for_millis,
                            }),
                        },
                    )),
                }],
            };
            let probe_id = probe.operations[0]
                .envelope
                .as_ref()
                .and_then(|envelope| envelope.operation_id.clone())
                .context("timer probe omitted operation id")?;
            let timer = exact_probe_receipt(
                raw_unary_response_until(
                    &control_connector,
                    CONTROL_PATH,
                    &authorization,
                    probe,
                    intent.deadline,
                )?,
                &probe_id,
                proto::TaskOperationOutcome::LeaseExpired,
                "original timer Participation",
            )?;
            let Some(proto::task_operation_verdict::Ack::Participation(ack)) =
                &receipt_verdict(&timer)?.ack
            else {
                anyhow::bail!("timer Participation omitted its typed Participation receipt");
            };
            ensure!(
                ack.participation.as_ref() == Some(&expiring)
                    && matches!(
                        proto::ParticipationState::try_from(ack.state),
                        Ok(proto::ParticipationState::Aborting
                            | proto::ParticipationState::TerminalRetained)
                    ),
                "the already-retired timer Participation changed identity/state: {ack:?}"
            );
            fs::write(
                context
                    .scenario_root()
                    .join("ordinary-full-original-timer.json"),
                serde_json::to_vec_pretty(
                    &serde_json::json!({"participation":format!("{expiring:?}"),
                    "initial_sequence":expiring_lease.sequence,
                    "requested_valid_for_millis":expiring_lease.requested_valid_for_millis,
                    "effective_valid_for_millis":expiring_lease.effective_valid_for_millis,
                    "before_probe_execution_custody":after_expiry,
                    "before_expiry_execution_custody":before_expiry,
                    "outcome":"LeaseExpired"}),
                )?,
            )?;
            context.action("the original short-lease Participation's actual execution custody fell from two to one while all eight ordinary permits remained held, before any Worker probe; its exact later control receipt confirmed LeaseExpired");

            // No task was created in this participation. A typed Worker refusal proves
            // control-lane progress without claiming that a real task was canceled.
            let cancel_request = control_cancel(backend);
            let cancel_id = cancel_request.operations[0]
                .envelope
                .as_ref()
                .and_then(|envelope| envelope.operation_id.clone())
                .context("Cancel probe omitted operation id")?;
            let control = raw_unary_response_until(
                &control_connector,
                CONTROL_PATH,
                &authorization,
                cancel_request,
                intent.deadline,
            )?;
            let receipt = exact_probe_receipt(
                control,
                &cancel_id,
                proto::TaskOperationOutcome::InvalidStateOrRequest,
                "Cancel",
            )?;
            ensure!(
                receipt_verdict(&receipt)?.ack.is_none()
                    && receipt_verdict(&receipt)?.safe_field_path.is_none()
                    && receipt_verdict(&receipt)?.safe_detail
                        == "cancel names a task this backend does not own",
                "task-free Cancel did not return its Worker-owned refusal: {receipt:?}"
            );
            context.action(
            "task-free Cancel returned the exact Worker refusal while eight original ordinary requests and one waiter were present",
        );

            let quiesce_request = control_quiesce(releasable.clone());
            let quiesce_id = quiesce_request.operations[0]
                .envelope
                .as_ref()
                .and_then(|envelope| envelope.operation_id.clone())
                .context("Quiesce probe omitted operation id")?;
            let quiesce = raw_unary_response_until(
                &control_connector,
                CONTROL_PATH,
                &authorization,
                quiesce_request,
                intent.deadline,
            )?;
            let receipt = exact_probe_receipt(
                quiesce,
                &quiesce_id,
                proto::TaskOperationOutcome::Accepted,
                "Quiesce",
            )?;
            let Some(proto::task_operation_verdict::Ack::QuiesceParticipation(ack)) =
                &receipt_verdict(&receipt)?.ack
            else {
                anyhow::bail!("Quiesce control omitted its typed acknowledgement: {receipt:?}");
            };
            ensure!(
                ack.participation.as_ref() == Some(&releasable)
                    && ack.fence_version == 1
                    && ack.accepted_tasks.is_empty()
                    && proto::ParticipationState::try_from(ack.state)
                        == Ok(proto::ParticipationState::Quiescing),
                "task-free Quiesce did not freeze the exact empty accepted-task set: {ack:?}"
            );
            context.action("Quiesce installed fence version one with an empty accepted-task set under ordinary saturation");

            let release_request = control_release(releasable.clone());
            let release_id = release_request.operations[0]
                .envelope
                .as_ref()
                .and_then(|envelope| envelope.operation_id.clone())
                .context("Release probe omitted operation id")?;
            let release = raw_unary_response_until(
                &control_connector,
                CONTROL_PATH,
                &authorization,
                release_request,
                intent.deadline,
            )?;
            let receipt = exact_probe_receipt(
                release,
                &release_id,
                proto::TaskOperationOutcome::Accepted,
                "Release",
            )?;
            let Some(proto::task_operation_verdict::Ack::ReleaseParticipation(ack)) =
                &receipt_verdict(&receipt)?.ack
            else {
                anyhow::bail!("Release control omitted its typed acknowledgement: {receipt:?}");
            };
            ensure!(
                ack.participation.as_ref() == Some(&releasable)
                    && proto::ReleaseParticipationOutcome::try_from(ack.outcome)
                        == Ok(proto::ReleaseParticipationOutcome::Released)
                    && proto::ParticipationState::try_from(ack.state)
                        == Ok(proto::ParticipationState::TerminalRetained)
                    && ack.termination_cause.is_none(),
                "task-free participation did not release normally under ordinary saturation: {ack:?}"
            );
            context.action(
            "Release of the exact quiesced, task-free participation returned Released and TerminalRetained under ordinary saturation",
        );

            let still_full = read_backend_metrics(context, 0)?;
            ensure!(
                metric(
                    &still_full,
                    "novarocks_backend_native_ingress_slots",
                    &[
                        ("class", "ordinary"),
                        ("phase", "running"),
                        ("dimension", "used")
                    ]
                )? == 8.0
                    && metric(
                        &still_full,
                        "novarocks_backend_native_ingress_slots",
                        &[
                            ("class", "ordinary"),
                            ("phase", "waiting"),
                            ("dimension", "used")
                        ]
                    )? == 1.0,
                "original ordinary saturation ended before control completion"
            );
            held.release_and_join()?;
            let queued = held.join_waiter()?;
            ensure!(
                queued.grpc_status == 0,
                "queued ordinary probe failed after release: {:?}",
                queued.grpc_message
            );
            wait_ingress_slot(context, "ordinary", "running", 0.0)?;
            wait_ingress_slot(context, "ordinary", "waiting", 0.0)?;
            context
                .action("ordinary running/waiting returned to zero after release and queue drain");
            Ok(())
        })();
        held.finish_after(primary)
    }
}

impl Scenario for AsyncSchedulingPressure {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_ingress",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "native-ingress/async-scheduling-pressure"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let (_connector, control_connector, authorization, backend, _) = probe(context)?;
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
            &control_connector,
            CONTROL_PATH,
            &authorization,
            control_cancel(backend),
            context.deadline(),
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
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_ingress",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "native-ingress/partial-body-deadline"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let (connector, control_connector, authorization, backend, _) = probe(context)?;
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
            &control_connector,
            CONTROL_PATH,
            &authorization,
            control_cancel(backend),
            context.deadline(),
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
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_ingress",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "native-ingress/registry-contention-control"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, launch_profile: LaunchProfile) -> Result<()> {
        BlockingSaturationControl::default().validate_runner_inputs(launch_profile)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        // One original Participation's already Accepted preparation is the held
        // host action. The second Participation uses the same live backend/poller.
        Ok(Some(self.intent.freeze(
            "preparation-hold",
            2,
            &[(0, 0, Some(1))],
            false,
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let epoch = Instant::now();
        let intent = self.intent.get()?;
        ensure!(
            context.deadline() == intent.deadline,
            "Native host-action deadline changed after freeze"
        );
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "host-action isolation requires the original native 1FE+3BE cluster"
        );
        let mut rendezvous = HostActionRendezvous::bind(intent.token(0)?)?;
        let query = intent
            .queries
            .first()
            .context("missing held Participation identity")?;
        let held =
            RawBackendSession::establish_declared(context, 0, query.query_id, query.frontend)?;
        intent.verify_backend(0, held.backend)?;
        let mut create = RawCreate::values(&held, 1);
        // Reuse the existing capacity receiver shape: it remains live until
        // its own normal fence, rather than completing an unrelated query.
        create.pipeline_dop = 1;
        create.exchange_input = true;
        ensure_accepted(&held, &create)?;
        rendezvous.await_entered(&held, &create, context.deadline())?;
        context.action("one Participation's exact Accepted preparation entered its supervised Native host action; no poller or registry lock is held");

        let query = intent
            .queries
            .get(1)
            .context("missing sibling Participation identity")?;
        let sibling =
            RawBackendSession::establish_declared(context, 0, query.query_id, query.frontend)?;
        intent.verify_backend(0, sibling.backend)?;
        ensure!(
            held.backend == sibling.backend && held.query_id != sibling.query_id,
            "host-action isolation must use two actual Participations on the same backend"
        );
        let original = sibling
            .establish_request
            .as_ref()
            .context("sibling lost original Establish")?;
        let Some(proto::task_operation::Operation::UpdateParticipation(update)) =
            &original.operation
        else {
            anyhow::bail!("sibling original operation is not a Participation update");
        };
        let Some(proto::update_participation_request::Command::Establish(establish)) =
            &update.command
        else {
            anyhow::bail!("sibling original update is not Establish");
        };
        let mut lease = establish
            .initial_lease
            .context("sibling omitted its actual initial lease")?;
        lease.sequence = lease
            .sequence
            .checked_add(1)
            .context("sibling lease sequence overflow")?;
        let renewed = session_control(
            &sibling,
            proto::task_control_operation::Control::RenewLease(
                proto::RenewParticipationLeaseRequest {
                    participation: Some(sibling.participation_under(sibling.frontend, 1)),
                    lease: Some(lease),
                },
            ),
        )?;
        ensure!(
            outcome(&renewed)? == proto::TaskOperationOutcome::Accepted,
            "same-backend sibling renewal did not complete while the host action was held: {renewed:?}"
        );
        let Some(proto::task_operation_verdict::Ack::Participation(ack)) =
            &receipt_verdict(&renewed)?.ack
        else {
            anyhow::bail!("sibling renewal omitted its exact Participation receipt");
        };
        let receipt = ack
            .lease
            .as_ref()
            .context("sibling renewal omitted its lease receipt")?;
        ensure!(
            ack.participation.as_ref() == Some(&sibling.participation_under(sibling.frontend, 1))
                && ack.state == proto::ParticipationState::Active as i32
                && receipt.sequence == lease.sequence
                && receipt.requested_valid_for_millis == lease.valid_for_millis
                && receipt.effective_valid_for_millis > 0
                && receipt.effective_valid_for_millis <= lease.valid_for_millis,
            "sibling renewal changed its original Participation or lease: {ack:?}"
        );
        let sibling_fence = quiesce_session(&sibling)?;
        ensure!(
            sibling_fence.fence_version != 0 && sibling_fence.accepted_tasks.is_empty(),
            "sibling fence invented Task membership: {sibling_fence:?}"
        );
        ensure!(
            sibling.release_when_ready(context)? == proto::ReleaseParticipationOutcome::Released,
            "same-backend sibling did not actually release while the host action was held"
        );
        context.action("the independent same-BE Participation established, renewed its exact next lease, fenced and actually Released before the held host action was released");

        let replay = held.apply(
            create.operation(),
            "observe the still-held original preparation",
        )?;
        ensure!(
            outcome(&replay)? == proto::TaskOperationOutcome::Idempotent,
            "held Task replay changed original ownership: {replay:?}"
        );
        let Some(proto::task_operation_verdict::Ack::CreateTask(ack)) =
            &receipt_verdict(&replay)?.ack
        else {
            anyhow::bail!("held Task replay omitted its exact Create acknowledgement");
        };
        let status = ack
            .current_status
            .as_ref()
            .context("held Task omitted its status")?;
        ensure!(
            status.installed == Some(false) && status.state == proto::TaskState::Planned as i32,
            "held host action unexpectedly installed or stopped before release: {status:?}"
        );
        rendezvous.release()?;
        await_installed(context, &held, &create)?;
        let fence = quiesce_session(&held)?;
        let identities = vec![raw_create_identity(&create)];
        ensure!(
            fence.fence_version != 0 && fence.accepted_tasks == identities,
            "held Participation fence changed original Task membership: {fence:?}"
        );
        observe_all_canceled_and_stopped(context, &held, &fence, &identities)?;
        ensure!(
            held.release_when_ready(context)? == proto::ReleaseParticipationOutcome::Released,
            "held Participation did not actually release after its host action exited"
        );
        super::result_delivery_support::await_idle(
            context,
            "participation-host-action",
            "after-original-release",
            epoch,
        )?;
        context.action("the original held host action exited, installed and normally stopped its exact Task, and both Participations released with the existing owner-idle barrier");
        Ok(())
    }
}

struct HostActionRendezvous {
    deadline: Option<Instant>,
    path: PathBuf,
    listener: UnixListener,
    stream: Option<UnixStream>,
    token: String,
}

impl HostActionRendezvous {
    fn bind(token: &str) -> Result<Self> {
        let path = novarocks_failpoint::native_registry_hold_socket_path(token)
            .map_err(anyhow::Error::msg)?;
        let listener = UnixListener::bind(&path)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            deadline: None,
            path,
            listener,
            stream: None,
            token: token.to_owned(),
        })
    }

    fn await_entered(
        &mut self,
        session: &RawBackendSession,
        create: &RawCreate,
        scenario_deadline: Instant,
    ) -> Result<()> {
        let deadline = scenario_deadline.min(Instant::now() + Duration::from_secs(10));
        let (mut stream, _) = loop {
            match self.listener.accept() {
                Ok(accepted) => break accepted,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    ensure!(
                        Instant::now() < deadline,
                        "the original preparation host-action rendezvous did not arrive"
                    );
                    thread::yield_now();
                }
                Err(error) => return Err(error.into()),
            }
        };
        let marker = read_marker(&mut stream, deadline)?;
        self.deadline = Some(scenario_deadline);
        ensure!(
            marker
                == format!(
                    "NTP1 {} {} {} 1 1 {} {}\n",
                    session.backend,
                    session.query_id.hi,
                    session.query_id.lo,
                    create.task_id,
                    self.token
                ),
            "unexpected original preparation host-action marker: {marker:?}"
        );
        self.stream = Some(stream);
        Ok(())
    }

    fn release(&mut self) -> Result<()> {
        release_hold(
            self.stream
                .as_mut()
                .context("original host action has not entered")?,
            self.deadline
                .context("original host action lacks its original deadline")?,
        )?;
        self.stream = None;
        Ok(())
    }
}

impl Drop for HostActionRendezvous {
    fn drop(&mut self) {
        if let Some(stream) = &mut self.stream {
            close_hold(stream);
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
        let connection_job = tokio::spawn(connection);
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
                connection_job.abort();
                let _ = connection_job.await;
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
                connection_job.abort();
                let _ = connection_job.await;
                return Ok((PartialBodyOutcome::TransportAbort, started.elapsed()));
            }
        }
        let trailers = match body.trailers().await {
            Ok(trailers) => trailers,
            Err(_) => {
                connection_job.abort();
                let _ = connection_job.await;
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
        connection_job.abort();
        let _ = connection_job.await;
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

fn read_backend_metrics(context: &mut ScenarioContext, index: usize) -> Result<Vec<Value>> {
    let port = context
        .handle()
        .runtime()
        .be
        .get(index)
        .context("BE metrics target index out of range")?
        .http;
    let url = format!("http://127.0.0.1:{port}/metrics?type=json");
    let client = Client::builder()
        .timeout(
            context
                .remaining("read Native ingress metrics")?
                .min(Duration::from_secs(5)),
        )
        .build()?;
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

fn held_operations(
    intent: &DirectIntent,
    connector: &NativeEndpointConnector,
    authorization: &str,
    backend: BackendProcessId,
    compatibility_id: [u8; 32],
) -> Result<Vec<(proto::TaskOperation, String)>> {
    let heartbeat: RawUnaryResponse<proto::HeartbeatResponse> = raw_unary_response_until(
        connector,
        HEARTBEAT_PATH,
        authorization,
        proto::HeartbeatRequest {
            expected_process_id: Some(proto::BackendProcessId {
                value: backend.to_bytes().to_vec(),
            }),
        },
        intent.deadline,
    )?;
    ensure!(
        heartbeat.grpc_status == 0,
        "Native held query heartbeat failed"
    );
    let epoch = heartbeat
        .message
        .context("heartbeat omitted response")?
        .admission_epoch_capability
        .context("heartbeat omitted epoch")?;
    intent
        .holds
        .iter()
        .map(|hold| {
            ensure!(
                hold.backend_index == 0 && hold.task_id.is_none(),
                "invalid ingress hold target"
            );
            let query = intent
                .queries
                .get(hold.query_index)
                .context("hold query outside frozen set")?;
            Ok((
                raw_acquire_admission_ticket(
                    query.participation(backend),
                    compatibility_id,
                    epoch.clone(),
                ),
                hold.token.clone(),
            ))
        })
        .collect()
}

fn ticket_operation(
    control_connector: &NativeEndpointConnector,
    authorization: &str,
    backend: BackendProcessId,
    compatibility_id: [u8; 32],
    deadline: Instant,
) -> Result<proto::TaskOperation> {
    let heartbeat: RawUnaryResponse<proto::HeartbeatResponse> = raw_unary_response_until(
        control_connector,
        HEARTBEAT_PATH,
        authorization,
        proto::HeartbeatRequest {
            expected_process_id: Some(proto::BackendProcessId {
                value: backend.to_bytes().to_vec(),
            }),
        },
        deadline,
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
        raw_participation(backend),
        compatibility_id,
        epoch,
    ))
}

fn establish_declared_participation(
    connector: &NativeEndpointConnector,
    control_connector: &NativeEndpointConnector,
    authorization: &str,
    backend: BackendProcessId,
    compatibility_id: [u8; 32],
    declared: (proto::ParticipationRef, Option<u64>),
    deadline: Instant,
) -> Result<(proto::ParticipationRef, proto::ParticipationLeaseReceipt)> {
    let (declared_participation, lease_duration_millis) = declared;
    let mut acquisition = ticket_operation(
        control_connector,
        authorization,
        backend,
        compatibility_id,
        deadline,
    )?;
    match acquisition.operation.as_mut() {
        Some(proto::task_operation::Operation::AcquireParticipationAdmissionTicket(request)) => {
            request.participation = Some(declared_participation.clone());
        }
        _ => anyhow::bail!("ticket helper produced wrong operation"),
    }
    let target_participation = declared_participation;
    let acquisition_id = acquisition
        .envelope
        .as_ref()
        .and_then(|envelope| envelope.operation_id.clone())
        .context("ticket acquisition omitted operation id")?;
    let response = raw_unary_response_until(
        connector,
        ORDINARY_PATH,
        authorization,
        proto::ApplyTaskOperationsRequest {
            operations: vec![acquisition],
        },
        deadline,
    )?;
    let receipt = exact_probe_receipt(
        response,
        &acquisition_id,
        proto::TaskOperationOutcome::Accepted,
        "Admission ticket",
    )?;
    let Some(proto::task_operation_verdict::Ack::ParticipationAdmissionTicket(ticket)) =
        receipt_verdict(&receipt)?.ack.clone()
    else {
        anyhow::bail!("ticket acquisition omitted ticket acknowledgement");
    };
    ensure!(
        ticket.participation.as_ref() == Some(&target_participation)
            && ticket.valid_for_millis == 10_000,
        "admission ticket did not bind the exact participation and requested validity: {ticket:?}"
    );
    let ticket_id = ticket
        .ticket_id
        .context("ticket acknowledgement omitted id")?;
    let mut operation = raw_establish(
        target_participation.clone(),
        ticket_id,
        Some(compatibility_id.to_vec()),
    );
    let Some(proto::task_operation::Operation::UpdateParticipation(update)) =
        operation.operation.as_mut()
    else {
        anyhow::bail!("original Establish factory produced the wrong operation");
    };
    let Some(proto::update_participation_request::Command::Establish(establish)) =
        update.command.as_mut()
    else {
        anyhow::bail!("original Establish factory produced the wrong command");
    };
    let initial = establish
        .initial_lease
        .as_mut()
        .context("original Establish omitted its lease grant")?;
    if let Some(millis) = lease_duration_millis {
        initial.valid_for_millis = millis;
    }
    let requested_sequence = initial.sequence;
    let requested_duration = initial.valid_for_millis;
    ensure!(
        establish.catalog_set.is_some() && establish.initial_runtime_filter.is_some(),
        "original Establish omitted its catalog or runtime-filter domain"
    );
    let credential = establish
        .initial_credential
        .as_ref()
        .context("original Establish omitted its credential domain")?;
    // Establish installs these actual three initial domains. Even empty
    // catalog/filter content receives its first-version Apply receipt.
    let expected_domains = vec![
        proto::ParticipationDomainReceipt {
            progression: proto::AcceptedDomainProgression::Apply as i32,
            receipt: Some(
                proto::participation_domain_receipt::Receipt::CatalogBinding(
                    proto::ScalarDomainReceipt {
                        accepted_version: novarocks_execution_contract::DomainVersion::FIRST.get(),
                    },
                ),
            ),
        },
        proto::ParticipationDomainReceipt {
            progression: proto::AcceptedDomainProgression::Apply as i32,
            receipt: Some(
                proto::participation_domain_receipt::Receipt::SharedDynamicFilter(
                    proto::ScalarDomainReceipt {
                        accepted_version: novarocks_execution_contract::DomainVersion::FIRST.get(),
                    },
                ),
            ),
        },
        proto::ParticipationDomainReceipt {
            progression: proto::AcceptedDomainProgression::Apply as i32,
            receipt: Some(proto::participation_domain_receipt::Receipt::Credential(
                proto::ParticipationCredentialReceipt {
                    credential_domain_id: credential.credential_domain_id,
                    accepted_epoch: credential.epoch,
                },
            )),
        },
    ];
    let establish_id = operation
        .envelope
        .as_ref()
        .and_then(|envelope| envelope.operation_id.clone())
        .context("Establish probe omitted operation id")?;
    let establish = raw_unary_response_until(
        connector,
        ORDINARY_PATH,
        authorization,
        proto::ApplyTaskOperationsRequest {
            operations: vec![operation],
        },
        deadline,
    )?;
    let receipt = exact_probe_receipt(
        establish,
        &establish_id,
        proto::TaskOperationOutcome::Accepted,
        "Establish",
    )?;
    let Some(proto::task_operation_verdict::Ack::Participation(ack)) =
        receipt_verdict(&receipt)?.ack.clone()
    else {
        anyhow::bail!("Establish omitted its typed participation acknowledgement");
    };
    let lease = ack
        .lease
        .as_ref()
        .context("Establish omitted its initial lease receipt")?;
    ensure!(
        ack.participation.as_ref() == Some(&target_participation)
            && proto::ParticipationState::try_from(ack.state)
                == Ok(proto::ParticipationState::Active)
            && ack.termination_cause.is_none()
            && ack.accepted_domains == expected_domains
            && lease.sequence == requested_sequence
            && lease.requested_valid_for_millis == requested_duration
            && lease.effective_valid_for_millis > 0
            && lease.effective_valid_for_millis <= lease.requested_valid_for_millis,
        "Establish did not install the exact active participation and initial facts: {ack:?}"
    );
    Ok((
        target_participation,
        proto::ParticipationLeaseReceipt {
            sequence: lease.sequence,
            requested_valid_for_millis: lease.requested_valid_for_millis,
            effective_valid_for_millis: lease.effective_valid_for_millis,
        },
    ))
}

/// Read management-only original execution custody. A timer proof must be
/// recorded before any application input could itself advance that Participation.
fn node_hot_resources(rows: &[Value]) -> Result<[f64; 4]> {
    ensure!(
        metric(
            rows,
            "novarocks_backend_worker_node_snapshot_available",
            &[]
        )? == 1.0,
        "original Node resource snapshot is unavailable"
    );
    let mut values = [0.0; 4];
    for (index, resource) in [
        "requests",
        "active_tasks",
        "preparing_tasks",
        "preparing_bytes",
    ]
    .into_iter()
    .enumerate()
    {
        values[index] = metric(
            rows,
            "novarocks_backend_worker_node_resources",
            &[("resource", resource)],
        )?;
    }
    Ok(values)
}

fn wait_execution_custody(context: &mut ScenarioContext, expected: f64) -> Result<Vec<Value>> {
    let deadline = context
        .deadline()
        .min(Instant::now() + Duration::from_secs(10));
    loop {
        let rows = read_backend_metrics(context, 0)?;
        let available = metric(
            &rows,
            "novarocks_backend_execution_quiescence_snapshot_available",
            &[],
        )?;
        ensure!(
            available == 0.0 || available == 1.0,
            "invalid execution custody availability: {available}"
        );
        if available == 1.0 {
            let actual = metric(
                &rows,
                "novarocks_backend_execution_quiescence",
                &[("resource", "participations_with_execution_custody")],
            )?;
            if actual == expected {
                return Ok(rows);
            }
        }
        ensure!(
            Instant::now() < deadline,
            "actual execution custody did not reach {expected}"
        );
        thread::sleep(
            context
                .remaining("observe original execution custody")?
                .min(Duration::from_millis(10)),
        );
    }
}

fn control_cancel(backend: BackendProcessId) -> proto::ApplyTaskControlOperationsRequest {
    proto::ApplyTaskControlOperationsRequest {
        operations: vec![proto::TaskControlOperation {
            envelope: Some(raw_operation_envelope(15_000)),
            control: Some(proto::task_control_operation::Control::CancelTask(
                proto::CancelTaskRequest {
                    identity: Some(proto::TaskIdentity {
                        query_execution_id: raw_participation(backend).query_execution_id,
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
            && proto::TaskOperationOutcome::try_from(receipt_verdict(&receipt)?.outcome)
                == Ok(expected),
        "{label} did not return the exact operation and expected owner outcome: {receipt:?}"
    );
    Ok(receipt)
}

fn control_quiesce(
    target_participation: proto::ParticipationRef,
) -> proto::ApplyTaskControlOperationsRequest {
    proto::ApplyTaskControlOperationsRequest {
        operations: vec![proto::TaskControlOperation {
            envelope: Some(raw_operation_envelope(15_000)),
            control: Some(
                proto::task_control_operation::Control::QuiesceParticipation(
                    proto::QuiesceParticipationRequest {
                        participation: Some(target_participation),
                    },
                ),
            ),
        }],
    }
}

fn control_release(
    target_participation: proto::ParticipationRef,
) -> proto::ApplyTaskControlOperationsRequest {
    proto::ApplyTaskControlOperationsRequest {
        operations: vec![proto::TaskControlOperation {
            envelope: Some(raw_operation_envelope(15_000)),
            control: Some(
                proto::task_control_operation::Control::ReleaseParticipation(
                    proto::ReleaseParticipationRequest {
                        participation: Some(target_participation),
                    },
                ),
            ),
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
    deadline: Instant,
    connection: RawUnaryConnection,
    waiter:
        Option<thread::JoinHandle<Result<RawUnaryResponse<proto::ApplyTaskOperationsResponse>>>>,
}

impl HeldOrdinary {
    fn start(
        connector: &NativeEndpointConnector,
        authorization: &str,
        operations: Vec<(proto::TaskOperation, String)>,
        backend: BackendProcessId,
        deadline: Instant,
    ) -> Result<Self> {
        let mut held = Self {
            calls: Vec::with_capacity(operations.len()),
            backend,
            connection: RawUnaryConnection::connect_until(connector, deadline)?,
            waiter: None,
            deadline,
        };
        for (operation, token) in operations {
            let path = novarocks_failpoint::native_ingress_hold_socket_path(&token)
                .map_err(anyhow::Error::msg)?;
            let listener = UnixListener::bind(&path)
                .with_context(|| format!("bind Native hold rendezvous {}", path.display()))?;
            listener.set_nonblocking(true)?;
            let client = held.connection.client();
            let authorization = authorization.to_owned();
            let token_for_request = token.clone();
            let response = thread::spawn(move || {
                client.response_with_hold_until::<_, proto::ApplyTaskOperationsResponse>(
                    ORDINARY_PATH,
                    &authorization,
                    proto::ApplyTaskOperationsRequest {
                        operations: vec![operation],
                    },
                    Some(&token_for_request),
                    deadline,
                )
            });
            held.calls.push(HeldCall {
                token,
                path,
                listener,
                stream: None,
                response: Some(response),
            });
        }
        Ok(held)
    }

    fn start_waiter(&mut self, authorization: &str, operation: proto::TaskOperation) -> Result<()> {
        ensure!(self.waiter.is_none(), "ordinary waiter already started");
        let client = self.connection.client();
        let authorization = authorization.to_owned();
        let deadline = self.deadline;
        self.waiter = Some(thread::spawn(move || {
            client.response_with_hold_until(
                ORDINARY_PATH,
                &authorization,
                proto::ApplyTaskOperationsRequest {
                    operations: vec![operation],
                },
                None,
                deadline,
            )
        }));
        Ok(())
    }

    fn join_waiter(&mut self) -> Result<RawUnaryResponse<proto::ApplyTaskOperationsResponse>> {
        self.waiter
            .take()
            .context("ordinary waiter not started")?
            .join()
            .map_err(|_| anyhow::anyhow!("queued ordinary probe panicked"))?
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
                            "ordinary asynchronous request did not enter token={}",
                            call.token
                        );
                        thread::yield_now();
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            let marker = read_marker(&mut stream, deadline)?;
            ensure!(
                marker == format!("NIH1 0 {} {}\n", self.backend, call.token),
                "unexpected Native hold marker: {marker:?}"
            );
            call.stream = Some(stream);
        }
        Ok(())
    }

    fn release_and_join(&mut self) -> Result<()> {
        let mut result = Ok(());
        for call in &mut self.calls {
            if let Some(stream) = &mut call.stream {
                result = super::native_compatibility::combine_raw_cleanup(
                    result,
                    release_hold(stream, self.deadline),
                );
            }
        }
        for call in &mut self.calls {
            if let Some(response) = call.response.take() {
                let joined = response
                    .join()
                    .map_err(|_| anyhow::anyhow!("held ordinary probe panicked"))
                    .and_then(|response| response)
                    .and_then(|response| {
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
                        Ok(())
                    });
                result = super::native_compatibility::combine_raw_cleanup(result, joined);
            }
        }
        result
    }

    fn finish_after<T>(&mut self, primary: Result<T>) -> Result<T> {
        let mut cleanup = Ok(());
        for call in &mut self.calls {
            if let Some(stream) = &mut call.stream {
                close_hold(stream);
            }
            while let Ok((mut stream, _)) = call.listener.accept() {
                close_hold(&mut stream);
            }
            let removed = fs::remove_file(&call.path);
            let removed = match removed {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => {
                    Err(anyhow::Error::from(error).context("remove owned Native hold socket"))
                }
            };
            cleanup = super::native_compatibility::combine_raw_cleanup(cleanup, removed);
        }
        cleanup =
            super::native_compatibility::combine_raw_cleanup(cleanup, self.connection.close());
        for call in &mut self.calls {
            if let Some(response) = call.response.take() {
                let joined = response
                    .join()
                    .map_err(|_| anyhow::anyhow!("held ordinary probe panicked during cleanup"))
                    .and_then(|response| response)
                    .map(|_| ());
                cleanup = super::native_compatibility::combine_raw_cleanup(cleanup, joined);
            }
        }
        if let Some(waiter) = self.waiter.take() {
            let joined = waiter
                .join()
                .map_err(|_| anyhow::anyhow!("ordinary waiter panicked during cleanup"))
                .and_then(|response| response)
                .map(|_| ());
            cleanup = super::native_compatibility::combine_raw_cleanup(cleanup, joined);
        }
        super::native_compatibility::combine_raw_cleanup(primary, cleanup)
    }
}

impl Drop for HeldOrdinary {
    fn drop(&mut self) {
        if let Err(error) = self.finish_after(Ok(())) {
            eprintln!("Native held ordinary cleanup failed: {error:#}");
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
) -> Result<(
    NativeEndpointConnector,
    NativeEndpointConnector,
    String,
    BackendProcessId,
    [u8; 32],
)> {
    ensure!(
        context.handle().be_count() == REQUIRED_BACKENDS,
        "{} requires a real 1FE+3BE cluster",
        context.name()
    );
    let endpoint = context.handle().native_be_endpoint(0)?;
    let control_endpoint = context.handle().native_be_control_endpoint(0)?;
    ensure!(
        endpoint != control_endpoint,
        "Native Data and Control endpoints must be distinct"
    );
    let port = endpoint.port();
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
    let trust_mode = context.handle().native_trust_mode();
    let connector = context
        .handle()
        .native_probe_connector(endpoint, trust_mode)?;
    let control_connector = context
        .handle()
        .native_probe_connector(control_endpoint, trust_mode)?;
    let authorization = authorization_header(&context.handle().native_probe_trust()?)?;
    Ok((
        connector,
        control_connector,
        authorization,
        backend,
        compatibility_id,
    ))
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
        let connection_job = tokio::spawn(connection);
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
        connection_job.abort();
        let _ = connection_job.await;
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
        let connection_job = tokio::spawn(connection);
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
        connection_job.abort();
        let _ = connection_job.await;
        Ok(status)
    })
}
