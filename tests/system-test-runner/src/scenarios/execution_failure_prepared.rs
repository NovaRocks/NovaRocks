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

//! A real prepared kernel remains owned after its original failure is accepted.
#![cfg(unix)]

use super::native_creation::{
    RawBackendSession, RawCreate, RawSink, ensure_accepted, observe_task_terminal_and_stopped,
    quiesce_session, raw_create_identity,
};
use super::native_direct_controls::{DirectIntentOwner, close_hold, read_marker, release_hold};
use super::preparation_capacity::await_snapshot_at;
use super::query_lifecycle::{await_resource_convergence, resource_snapshot};
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{QueryExecutionResourceSnapshot, TestClusterOwner};
use novarocks_execution_contract::{
    failure::{FailureClass, FailureEvidence, domain},
    task_execution::status::{
        TaskFailure, TaskFailurePhase, TaskState, TaskStatus, TerminationDetail,
    },
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::operation::{CoveredStatusStreamFact, decode_covered_status_event};
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

const ACTION: &str = "prepared-failure-exit-hold";
// Local Display is diagnostic; the native boundary carries the neutral summary.
const SAFE_DETAIL: &str = "Unexpected failure (reason 13:20)";
const HEALTHY_SQL: &str = "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 1) t ORDER BY v";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(AcceptedPreparedFailureDelayedExit::default()),
        Box::new(participation_owner_panic::ParticipationOwnerPanicIsolation::default()),
        Box::new(
            participation_owner_panic::ParticipationOwnerPanicIsolation::with_prepared_holder(),
        ),
        Box::new(participation_owner_panic::RetainedParticipationOwnerPanicIsolation::default()),
        Box::new(
            participation_owner_panic::RetainedParticipationOwnerPanicIsolation::with_expiry(),
        ),
    ]
}
#[derive(Default)]
struct AcceptedPreparedFailureDelayedExit {
    intent: DirectIntentOwner,
}
impl Scenario for AcceptedPreparedFailureDelayedExit {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        super::declarations::for_module(
            "execution_failure_prepared",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }
    fn name(&self) -> &'static str {
        "execution-failure/accepted-prepared-failure-delayed-exit"
    }
    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        ensure!(cluster_size == 3, "prepared exit requires native 1FE+3BE");
        Ok(Some(self.intent.freeze(
            ACTION,
            2,
            &[(0, 0, Some(1))],
            false,
            cluster_size,
            binary,
            deadline,
        )?))
    }
    fn launch_config(&self, _root: &Path) -> Result<ScenarioLaunchConfig> {
        let mut launch = ScenarioLaunchConfig::default();
        launch.config_overlay.fe = Some("[runtime]\ntask_dispatch_create_permits = 2\n".into());
        launch.config_overlay.be = Some("[runtime]\ntask_preparation_max_tasks_per_participation = 2\ntask_preparation_max_tasks = 3\ntask_preparation_max_bytes = 67108864\ntask_preparation_max_workers = 1\n".into());
        Ok(launch)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "prepared exit requires native 1FE+3BE"
        );
        let intent = self.intent.get()?;
        ensure!(
            intent.deadline == context.deadline(),
            "prepared action original deadline changed"
        );
        let baseline = resource_snapshot(context)?;
        let processes = context.recheck_live_process_launch_identities()?;
        let token = intent.token(0)?;
        // Bind before Create; the original action owns the other end. This
        // RAII gate opens by EOF on every unsuccessful path, before cleanup.
        let mut gate = PreparedExitGate::bind(token)?;
        let mut sessions = Vec::new();
        let result = (|| -> Result<()> {
            let query = &intent.queries[0];
            let session =
                RawBackendSession::establish_declared(context, 0, query.query_id, query.frontend)?;
            sessions.push((session, proto::TaskState::Failed));
            intent.verify_backend(0, sessions[0].0.backend)?;
            let sibling_query = &intent.queries[1];
            let sibling = RawBackendSession::establish_declared(
                context,
                0,
                sibling_query.query_id,
                sibling_query.frontend,
            )?;
            sessions.push((sibling, proto::TaskState::Finished));
            intent.verify_backend(0, sessions[1].0.backend)?;
            let session = &sessions[0].0;
            let sibling = &sessions[1].0;
            let mut failed = RawCreate::values(session, 1);
            failed.pipeline_dop = 1;
            failed.sink = RawSink::CountOnly;
            exercise(context, session, sibling, &failed, &mut gate, &baseline)
        })();
        drop(gate);

        // Quiesce/Release remain the original context owner. A result error
        // cannot skip real child exit or the independent accepted sibling.
        let mut cleanup_failures = Vec::new();
        for (owner, expected) in &sessions {
            match quiesce_session(owner) {
                Ok(fence) => {
                    for identity in &fence.accepted_tasks {
                        if let Err(error) =
                            observe_task_terminal_and_stopped(context, owner, identity, *expected)
                        {
                            cleanup_failures.push(error);
                        }
                    }
                }
                Err(error) => cleanup_failures.push(error),
            }
            match owner.release_when_ready(context) {
                Ok(proto::ReleaseParticipationOutcome::Released) => {}
                Ok(other) => cleanup_failures.push(anyhow::anyhow!(
                    "prepared scenario participation release did not complete: {other:?}"
                )),
                Err(error) => cleanup_failures.push(error),
            }
        }
        if let Err(error) = await_snapshot_at(context, 0, 0, 0, 0) {
            cleanup_failures.push(error);
        }
        if let Err(error) = await_resource_convergence(context, &baseline) {
            cleanup_failures.push(error);
        }
        match context.recheck_live_process_launch_identities() {
            Ok(after) if after == processes => {}
            Ok(_) => cleanup_failures.push(anyhow::anyhow!(
                "prepared failure replaced or stopped a live role process"
            )),
            Err(error) => cleanup_failures.push(error),
        }
        let cleanup = if cleanup_failures.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "prepared exit cleanup failed: {cleanup_failures:#?}"
            ))
        };
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Err(cleanup)) => {
                Err(primary.context(format!("prepared exit cleanup also failed: {cleanup:#}")))
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }
}

fn exercise(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    sibling_session: &RawBackendSession,
    failed: &RawCreate,
    gate: &mut PreparedExitGate,
    baseline: &QueryExecutionResourceSnapshot,
) -> Result<()> {
    ensure_accepted(session, failed)?;
    let marker = gate.await_entered(context, session, failed)?;
    write_artifact(
        context,
        "prepared-exit-marker.json",
        &json!({
            "marker_sha256": format!("{:x}", Sha256::digest(marker.as_bytes())),
            "marker_bytes": marker.len(), "task": raw_create_identity(failed).encode_to_vec(),
        }),
    )?;
    let before = observe_prepared_status(context, session, failed, ExitPhase::Held)?;
    let original = nominal_failure(&before.status)?.clone();
    write_artifact(context, "prepared-failing-covered.json", &before.json())?;
    let charged = await_snapshot_at(context, 0, 1, 0, 0)?;
    ensure!(
        charged.participation_positions == 1
            && charged.bytes > 0
            && charged.participation_limit == 2
            && charged.position_limit == 3
            && charged.worker_limit == 1
            && charged.byte_limit == 67108864,
        "held prepared kernel changed its actual ledger: {charged:?}"
    );
    let retained = resource_snapshot(context)?;
    ensure!(
        active_fragments(&retained, 0)? == active_fragments(baseline, 0)? + 1.0,
        "held kernel lost its original active fragment registration"
    );
    write_artifact(
        context,
        "prepared-held-ledger.json",
        &json!({
            "positions": charged.positions, "participation_positions": charged.participation_positions,
            "bytes": charged.bytes, "workers": charged.workers, "queued": charged.queued,
            "active_fragments": active_fragments(&retained, 0)?,
        }),
    )?;

    // This second exact task uses the same BE and physical preparation worker.
    // A real Finished plus actual-stop receipt proves it was not merely queued.
    let mut sibling = RawCreate::values(sibling_session, 2);
    sibling.pipeline_dop = 1;
    ensure_accepted(sibling_session, &sibling)?;
    observe_task_terminal_and_stopped(
        context,
        sibling_session,
        &raw_create_identity(&sibling),
        proto::TaskState::Finished,
    )?;
    let connection = mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?;
    let rows: Vec<i64> = connection.run(|connection| {
        connection
            .query(HEALTHY_SQL)
            .context("execute healthy public query while prepared failure is retained")
    })?;
    ensure!(
        rows == [1, 1],
        "healthy query changed complete rows while prepared failure waited: {rows:?}"
    );
    let still_held = observe_prepared_status(context, session, failed, ExitPhase::Held)?;
    ensure!(
        nominal_failure(&still_held.status)? == &original,
        "independent work changed the already accepted failure"
    );
    let after_sibling = await_snapshot_at(context, 0, 1, 0, 0)?;
    ensure!(
        after_sibling.bytes == charged.bytes && after_sibling.participation_positions == 1,
        "healthy sibling returned the held kernel's preparation charges"
    );
    let still_retained = await_retained_fragment(context, baseline)?;
    write_artifact(
        context,
        "prepared-held-after-health.json",
        &json!({
            "covered": still_held.json(), "positions": after_sibling.positions,
            "participation_positions": after_sibling.participation_positions, "bytes": after_sibling.bytes,
            "active_fragments": active_fragments(&still_retained, 0)?, "healthy_rows": rows,
        }),
    )?;
    context.action("covered Failing retained the original Preparation NATIVE:20 without actual-stop; P/count/bytes and original prepared fragment remained live while a same-BE sibling actually stopped and a healthy public query completed");

    gate.release(context.deadline())?;
    await_explicit_release(context, session, failed, &gate.token)?;
    let after = observe_prepared_status(context, session, failed, ExitPhase::Exited)?;
    ensure!(
        nominal_failure(&after.status)? == &original,
        "actual prepared drain replaced its original failure/phase/detail"
    );
    write_artifact(
        context,
        "prepared-failed-stopped-covered.json",
        &after.json(),
    )?;
    let released = await_snapshot_at(context, 0, 0, 0, 0)?;
    let exited_resources = resource_snapshot(context)?;
    ensure!(
        active_fragments(&exited_resources, 0)? == active_fragments(baseline, 0)?,
        "actual-stop did not release the original fragment registration"
    );
    write_artifact(
        context,
        "prepared-exit-returned-charges.json",
        &json!({
            "positions": released.positions, "participation_positions": released.participation_positions,
            "bytes": released.bytes, "workers": released.workers, "queued": released.queued,
            "active_fragments": active_fragments(&exited_resources, 0)?,
        }),
    )?;
    // No raw Create replay is used to establish failure: the current creation
    // cell may legally carry DomainFailure. Covered Task status is authoritative.
    let fence = quiesce_session(session)?;
    ensure!(
        fence.accepted_tasks == [raw_create_identity(failed)],
        "prepared exit changed the exact accepted task set"
    );
    context.action("explicit R opened the original prepared drain; covered Failed plus nonzero actual-stop preserved the original TaskFailure, and actual preparation positions/bytes returned");
    Ok(())
}

#[derive(Clone, Copy)]
enum ExitPhase {
    Held,
    Exited,
}
struct CoveredPrepared {
    status: TaskStatus,
    status_wire: Vec<u8>,
    stopped_version: Option<u64>,
    initial_cut: u64,
    covered_cut: u64,
    bookmark_sequence: u64,
}
impl CoveredPrepared {
    fn json(&self) -> Value {
        let failure = match self.status.termination() {
            Some(TerminationDetail::Failed(failure)) => Some(failure),
            _ => None,
        };
        json!({
            "identity": self.status.identity().to_string(), "status_version": self.status.version().get(),
            "status": format!("{:?}", self.status.state()), "installed": self.status.installed(),
            "status_wire_hex": hex(&self.status_wire), "actual_stopped_version": self.stopped_version,
            "initial_cut": self.initial_cut, "covered_cut": self.covered_cut,
            "bookmark_sequence": self.bookmark_sequence,
            "failure": failure.map(|failure| json!({
                "domain": failure.fact().reason().domain(), "reason": failure.fact().reason().code(),
                "class": format!("{:?}", failure.fact().class()),
                "evidence": format!("{:?}", failure.fact().evidence()),
                "phase": failure.phase().as_str(), "safe_detail": failure.detail().as_str(),
                "resource": failure.fact().resource().map(|value| format!("{value:?}")),
                "representation": failure.fact().representation().map(|value| format!("{value:?}")),
                "row_position": failure.fact().row_position().map(|value| format!("{value:?}")),
            })),
        })
    }
}
fn nominal_failure(status: &TaskStatus) -> Result<&TaskFailure> {
    ensure!(!status.installed(), "prepared failure was marked Installed");
    let Some(TerminationDetail::Failed(failure)) = status.termination() else {
        bail!("prepared action did not preserve an originating Task failure: {status:?}");
    };
    let fact = failure.fact();
    ensure!(
        fact.class() == FailureClass::Unexpected
            && fact.evidence() == FailureEvidence::Originating
            && fact.reason().domain() == domain::NATIVE.get()
            && fact.reason().code() == 20
            && fact.resource().is_none()
            && fact.representation().is_none()
            && fact.row_position().is_none()
            && failure.phase() == TaskFailurePhase::Preparation
            && failure.detail().as_str() == SAFE_DETAIL,
        "prepared failure changed its real domain/phase/safe detail: {failure:?}"
    );
    Ok(failure)
}

/// The same authenticated covered subscription used by native_creation. This
/// one-task observation also records a closed initial cut while the gate holds.
/// Missing TaskConvergence is only a negative fact after that complete cut.
fn observe_prepared_status(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    create: &RawCreate,
    phase: ExitPhase,
) -> Result<CoveredPrepared> {
    let deadline = context.deadline();
    let expected_wire = raw_create_identity(create);
    let expected = novarocks_task_codec::identity::decode_task_identity(
        &expected_wire,
        FieldPath::root("prepared_expected"),
    )?;
    let request = proto::SubscribeTaskStatusRequest {
        participation: Some(session.participation_under(session.frontend, 1)),
        generation: 1,
        required_identities: vec![expected_wire],
        ..Default::default()
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (mut sender, connection) = tokio::time::timeout_at(deadline.into(), async {
            let stream = session
                .connector
                .connect()
                .await
                .map_err(anyhow::Error::msg)?;
            Ok::<_, anyhow::Error>(h2::client::handshake(stream).await?)
        })
        .await
        .context("original prepared subscription connect deadline expired")??;
        let connection_job = tokio::spawn(connection);
        let result = tokio::time::timeout_at(deadline.into(), async {
            sender = sender.ready().await?;
            let headers = http::Request::builder()
                .method("POST")
                .uri("/novarocks.NovaRocksGrpc/SubscribeTaskStatus")
                .header(http::header::CONTENT_TYPE, "application/grpc")
                .header("te", "trailers")
                .header(http::header::AUTHORIZATION, &session.authorization)
                .body(())?;
            let (response, mut send) = sender.send_request(headers, false)?;
            let payload = request.encode_to_vec();
            let mut frame = vec![0];
            frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            frame.extend_from_slice(&payload);
            send.send_data(bytes::Bytes::from(frame), true)?;
            let response = response.await?;
            ensure!(
                response.status().as_u16() == 200,
                "prepared covered stream returned HTTP {}",
                response.status()
            );
            let mut body = response.into_body();
            let mut pending = Vec::new();
            let mut status = None;
            let mut status_wire = Vec::new();
            let mut stopped_version = None;
            let mut initial_cut = None;
            let mut last_sequence = 0;
            while let Some(chunk) = body.data().await {
                let chunk = chunk?;
                body.flow_control().release_capacity(chunk.len())?;
                pending.extend_from_slice(&chunk);
                while pending.len() >= 5 {
                    ensure!(pending[0] == 0, "prepared status frame was compressed");
                    let length =
                        u32::from_be_bytes(pending[1..5].try_into().expect("gRPC header width"))
                            as usize;
                    ensure!(
                        length <= 8 * 1024 * 1024,
                        "prepared status frame exceeded existing probe bound"
                    );
                    if pending.len() < length + 5 {
                        break;
                    }
                    let wire = proto::TaskStatusStreamEvent::decode(&pending[5..length + 5])?;
                    pending.drain(..length + 5);
                    let event =
                        decode_covered_status_event(&wire, FieldPath::root("prepared_covered"))?;
                    match event.fact {
                        CoveredStatusStreamFact::Status(current) => {
                            ensure!(
                                current.identity() == expected,
                                "covered stream supplied a foreign Task"
                            );
                            nominal_failure(&current)?;
                            ensure!(
                                matches!(current.state(), TaskState::Failing | TaskState::Failed),
                                "prepared failure left its termination family"
                            );
                            if matches!(phase, ExitPhase::Held) {
                                ensure!(
                                    current.state() == TaskState::Failing,
                                    "prepared kernel was terminal before release"
                                );
                            }
                            let Some(proto::task_status_stream_event::Event::TaskStatus(raw)) =
                                wire.event
                            else {
                                unreachable!("decoded status variant")
                            };
                            status_wire = raw.encode_to_vec();
                            status = Some(current);
                        }
                        CoveredStatusStreamFact::TaskConvergence(receipt) => {
                            ensure!(
                                receipt.identity() == expected,
                                "actual-stop named a foreign Task"
                            );
                            ensure!(
                                !matches!(phase, ExitPhase::Held),
                                "prepared Task actually stopped while its kernel gate remained held"
                            );
                            stopped_version = Some(receipt.version().get());
                        }
                        CoveredStatusStreamFact::CatchUpComplete(receipt) => {
                            ensure!(
                                receipt.generation == 1 && initial_cut.is_none(),
                                "covered initial generation/cut repeated or changed"
                            );
                            initial_cut = Some(receipt.initial_cut);
                        }
                        CoveredStatusStreamFact::Bookmark(receipt) => {
                            ensure!(
                                receipt.generation == 1 && receipt.sequence > last_sequence,
                                "covered bookmark changed generation or regressed"
                            );
                            last_sequence = receipt.sequence;
                            if let Some(initial) = initial_cut {
                                ensure!(
                                    receipt.source_cut >= initial,
                                    "covered source cut regressed"
                                );
                                let ready = status.as_ref().is_some_and(|current| match phase {
                                    ExitPhase::Held => {
                                        current.state() == TaskState::Failing
                                            && stopped_version.is_none()
                                    }
                                    ExitPhase::Exited => {
                                        current.state() == TaskState::Failed
                                            && stopped_version.is_some()
                                    }
                                });
                                if ready && receipt.covered_prefix == receipt.source_cut {
                                    return Ok(CoveredPrepared {
                                        status: status.take().expect("checked status"),
                                        status_wire,
                                        stopped_version,
                                        initial_cut: initial,
                                        covered_cut: receipt.source_cut,
                                        bookmark_sequence: receipt.sequence,
                                    });
                                }
                            }
                        }
                        CoveredStatusStreamFact::Gone(_)
                        | CoveredStatusStreamFact::Unknown(_)
                        | CoveredStatusStreamFact::StatusUnchanged(_)
                        | CoveredStatusStreamFact::TaskConvergenceUnchanged(_) => bail!(
                            "required zero-cursor prepared Task evidence was absent or reclaimed"
                        ),
                        CoveredStatusStreamFact::ParticipationConvergence(_)
                        | CoveredStatusStreamFact::Quiesce(_) => {}
                    }
                }
            }
            bail!(
                "prepared covered stream ended before its exact cut; trailers={:?}",
                body.trailers().await?
            )
        })
        .await
        .context("original prepared status/cut deadline expired")
        .and_then(|result| result);
        connection_job.abort();
        let joined = connection_job.await;
        let cleanup = match joined {
            Ok(Ok(())) => Ok(()),
            Err(error) if error.is_cancelled() => Ok(()),
            Ok(Err(error)) => Err(anyhow::anyhow!(
                "prepared subscription original H2 connection job exit failed: {error}"
            )),
            Err(error) => Err(anyhow::anyhow!(
                "prepared subscription original H2 connection job panicked: {error}"
            )),
        };
        match (result, cleanup) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(primary), Err(cleanup)) => Err(primary.context(format!(
                "covered connection cleanup also failed: {cleanup:#}"
            ))),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    })
}

struct PreparedExitGate {
    path: PathBuf,
    listener: UnixListener,
    stream: Option<UnixStream>,
    token: String,
}
impl PreparedExitGate {
    fn bind(token: &str) -> Result<Self> {
        let path = novarocks_failpoint::native_registry_hold_socket_path(token)
            .map_err(anyhow::Error::msg)?;
        let listener = UnixListener::bind(&path)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            path,
            listener,
            stream: None,
            token: token.into(),
        })
    }
    fn await_entered(
        &mut self,
        context: &mut ScenarioContext,
        session: &RawBackendSession,
        create: &RawCreate,
    ) -> Result<String> {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    self.stream = Some(stream);
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(
                        context
                            .remaining("wait original prepared exit gate")?
                            .min(Duration::from_millis(1)),
                    );
                }
                Err(error) => return Err(error.into()),
            }
        }
        let marker = read_marker(
            self.stream.as_mut().expect("accepted original gate"),
            context.deadline(),
        )?;
        ensure!(
            marker
                == format!(
                    "NPE1 {} {} {} 1 1 {} {}\n",
                    session.backend,
                    session.query_id.hi,
                    session.query_id.lo,
                    create.task_id,
                    self.token
                ),
            "prepared exit marker changed its exact frozen Task/action"
        );
        Ok(marker)
    }
    fn release(&mut self, deadline: Instant) -> Result<()> {
        release_hold(
            self.stream
                .as_mut()
                .context("prepared exit gate was not entered")?,
            deadline,
        )?;
        drop(self.stream.take());
        Ok(())
    }
}
impl Drop for PreparedExitGate {
    fn drop(&mut self) {
        if let Some(stream) = &mut self.stream {
            close_hold(stream);
        }
        let _ = fs::remove_file(&self.path);
    }
}
fn await_explicit_release(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    create: &RawCreate,
    token: &str,
) -> Result<()> {
    let expected = format!(
        "{} backend={} execution_id={}:{}:1 stage=1 task={} token={} reason=release",
        novarocks_failpoint::observation_marker::PREPARED_FAILURE_EXIT_RELEASED,
        session.backend,
        session.query_id.hi,
        session.query_id.lo,
        create.task_id,
        token
    );
    loop {
        let log = context.handle().be_log_contents(session.index)?;
        if log.lines().any(|line| line.trim_end() == expected) {
            return Ok(());
        }
        thread::sleep(
            context
                .remaining("observe explicit original prepared release")?
                .min(Duration::from_millis(10)),
        );
    }
}
fn active_fragments(snapshot: &QueryExecutionResourceSnapshot, index: usize) -> Result<f64> {
    let backend = snapshot
        .backends
        .iter()
        .find(|backend| backend.index == index)
        .context("prepared resource snapshot omitted original BE")?;
    ensure!(
        snapshot.fe_running && backend.process_running,
        "prepared scenario role stopped"
    );
    backend
        .resources
        .get("native_query_active_tasks")
        .copied()
        .context("actual active-fragment measurement is unavailable")
}
fn await_retained_fragment(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
) -> Result<QueryExecutionResourceSnapshot> {
    loop {
        let snapshot = resource_snapshot(context)?;
        if active_fragments(&snapshot, 0)? == active_fragments(baseline, 0)? + 1.0 {
            return Ok(snapshot);
        }
        thread::sleep(
            context
                .remaining("await only the original held prepared fragment")?
                .min(Duration::from_millis(10)),
        );
    }
}
fn write_artifact(context: &ScenarioContext, name: &str, value: &Value) -> Result<()> {
    fs::write(
        context.scenario_root().join(name),
        serde_json::to_vec_pretty(value)?,
    )
    .with_context(|| format!("write prepared-exit evidence {name}"))
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

// S8 is a separate owner oracle. The existing prepared-failure scenario above
// remains unchanged: a typed host failure is not a Context actor panic.
mod participation_owner_panic {
    use super::*;
    use crate::scenarios::native_compatibility::{
        RawUnaryResponse, only_successful_receipt, raw_operation_envelope, raw_unary_response,
    };
    use crate::scenarios::native_creation::{await_installed, outcome};

    const OWNER_ACTION: &str = "participation-owner-panic-on-renew";
    const REMOVED: &str = "Worker Participation execution custody retired";

    fn owner_launch_config(root: &Path) -> Result<ScenarioLaunchConfig> {
        let mut launch = ScenarioLaunchConfig::default();
        // Preserve the original application writer, not a synthetic state
        // source or a replacement cleanup command.
        let log = root.join("owner-application-log");
        launch.config_overlay.be = Some(format!(
            "sys_log_dir = {:?}\nlog_filter = \"info,novarocks_native_adapter::backend_application=debug,novarocks_worker::control_runtime::node_bootstrap=debug,novarocks_worker::control_runtime::node=debug,novarocks_worker::control_runtime::task=debug,novarocks_worker::control_runtime::participation_host::requests=debug,novarocks_worker::control_runtime::causal=debug,novarocks_worker::control_runtime::participation_host::task_io=debug,novarocks_worker::control_runtime::participation_host::teardown=debug,novarocks_worker::control_runtime::participation_host::failure=debug,novarocks_worker::control_runtime::process_control::rejected_ingress=debug\"\n",
            log.to_string_lossy(),
        ));
        Ok(launch)
    }

    #[derive(Default)]
    pub(super) struct ParticipationOwnerPanicIsolation {
        intent: DirectIntentOwner,
        held_prepared: bool,
    }
    impl ParticipationOwnerPanicIsolation {
        pub(super) fn with_prepared_holder() -> Self {
            Self {
                intent: DirectIntentOwner::default(),
                held_prepared: true,
            }
        }
    }
    impl Scenario for ParticipationOwnerPanicIsolation {
        fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
            super::super::declarations::for_module(
                "execution_failure_prepared",
                self.name(),
                self.is_explicit_stage(),
                self.is_compatibility_stage(),
            )
        }
        fn name(&self) -> &'static str {
            if self.held_prepared {
                "execution-failure/participation-owner-panic-held-prepared-isolation"
            } else {
                "execution-failure/participation-owner-panic-isolation"
            }
        }
        fn is_explicit_stage(&self) -> bool {
            true
        }
        fn test_session_plan(
            &self,
            _launch: &ScenarioLaunchConfig,
            _root: &Path,
            cluster_size: usize,
            binary: &Path,
            deadline: Instant,
        ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>>
        {
            ensure!(
                cluster_size == 3,
                "Participation isolation requires native 1FE+3BE"
            );
            if self.held_prepared {
                return Ok(Some(self.intent.freeze_actions(
                    3,
                    &[(OWNER_ACTION, 0, 0, None), (ACTION, 0, 0, Some(1))],
                    false,
                    cluster_size,
                    binary,
                    deadline,
                )?));
            }
            Ok(Some(self.intent.freeze(
                OWNER_ACTION,
                3,
                &[(0, 0, None)],
                false,
                cluster_size,
                binary,
                deadline,
            )?))
        }
        fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
            owner_launch_config(root)
        }
        fn run(&self, context: &mut ScenarioContext) -> Result<()> {
            ensure!(
                context.handle().be_count() == 3,
                "Participation isolation requires native 1FE+3BE"
            );
            let intent = self.intent.get()?;
            ensure!(
                intent.deadline == context.deadline(),
                "original isolation deadline changed"
            );
            let baseline = resource_snapshot(context)?;
            let processes = context.recheck_live_process_launch_identities()?;
            let a = &intent.queries[0];
            let failed = RawBackendSession::establish_declared(context, 0, a.query_id, a.frontend)?;
            intent.verify_backend(0, failed.backend)?;
            let b = &intent.queries[1];
            let sibling =
                RawBackendSession::establish_declared(context, 0, b.query_id, b.frontend)?;
            let mut receiver = RawCreate::values(&failed, 1);
            receiver.pipeline_dop = 1;
            receiver.exchange_input = true;
            let mut prepared_gate = if self.held_prepared {
                receiver.sink = RawSink::CountOnly;
                let hold = intent
                    .holds
                    .iter()
                    .find(|hold| hold.task_id == Some(1))
                    .context("original prepared holder grant missing")?;
                Some(PreparedExitGate::bind(&hold.token)?)
            } else {
                None
            };
            ensure_accepted(&failed, &receiver)?;
            let held_bytes = if let Some(gate) = &mut prepared_gate {
                let marker = gate.await_entered(context, &failed, &receiver)?;
                let before = observe_prepared_status(context, &failed, &receiver, ExitPhase::Held)?;
                nominal_failure(&before.status)?;
                let charged = await_snapshot_at(context, 0, 1, 0, 0)?;
                ensure!(
                    charged.bytes > 0 && charged.participation_positions == 1,
                    "the original prepared holder lost its physical charges: {charged:?}"
                );
                await_retained_fragment(context, &baseline)?;
                write_artifact(
                    context,
                    "owner-held-prepared-before-panic.json",
                    &json!({
                        "marker_sha256":format!("{:x}",Sha256::digest(marker.as_bytes())),
                        "covered":before.json(),"preparation_bytes":charged.bytes,
                        "scope":"real prepared kernel/charges remain live; does not prove an old Stop/DisposeSource ticket is pending"
                    }),
                )?;
                context.action("the existing exact prepared-failure-exit gate retained the real original kernel, covered Failing with no actual-stop and nonzero preparation charges before the Participation actor panic; no old cleanup-ticket phase is inferred");
                Some(charged.bytes)
            } else {
                await_installed(context, &failed, &receiver)?;
                context.action("installed an original live receiver on BE0 with the sole paid inbound row before the declared Participation failure");
                None
            };

            // A different operation on the SAME Context is not the grant.
            let normal = renew(
                &failed,
                raw_operation_envelope(15_000),
                1,
                "unselected same-Participation Renew",
            )?;
            ensure!(
                outcome(&normal)? == proto::TaskOperationOutcome::Accepted,
                "unselected Renew did not preserve its normal decision: {normal:?}"
            );
            let trigger = intent
                .holds
                .first()
                .and_then(|hold| hold.trigger_operation)
                .context("declared original trigger operation missing")?;
            let mut envelope = raw_operation_envelope(15_000);
            envelope.operation_id = Some(proto::TaskOperationId {
                value: trigger.to_vec(),
            });
            let receipt = renew(
                &failed,
                envelope,
                2,
                "declared original Participation owner panic",
            )?;
            ensure!(
                outcome(&receipt)? == proto::TaskOperationOutcome::OwnerFailed,
                "actual owner panic did not return the original R OwnerFailed: {receipt:?}"
            );
            ensure!(
                context
                    .handle()
                    .be_log_count(0, "declared Participation owner panic participation=")?
                    == 1,
                "the original actor turn did not report exactly one declared panic"
            );
            context.action("unselected same-Participation Renew remained normal; the exact declared operation panicked inside its original Participation Actor turn and Common returned OwnerFailed through that same R");

            let mut healthy = RawCreate::values(&sibling, 2);
            healthy.pipeline_dop = 1;
            ensure_accepted(&sibling, &healthy)?;
            observe_task_terminal_and_stopped(
                context,
                &sibling,
                &raw_create_identity(&healthy),
                proto::TaskState::Finished,
            )?;
            let connection = mysql_actor::connect_until(
                context.mysql_user(),
                context.mysql_port(),
                context.deadline(),
            )?;
            let rows: Vec<i64> = connection.run(|connection| {
                connection
                    .query(HEALTHY_SQL)
                    .context("healthy public SQL after actual Participation owner failure")
            })?;
            ensure!(
                rows == [1, 1],
                "healthy SQL rows changed after owner isolation: {rows:?}"
            );
            ensure!(
                context.recheck_live_process_launch_identities()? == processes,
                "owner failure replaced a role process instead of isolating its Participation"
            );
            ensure!(
                sibling.release_when_ready(context)?
                    == proto::ReleaseParticipationOutcome::Released,
                "the independent same-BE Participation did not really release"
            );
            context.action("an independent Participation on the SAME BE Finished and actually stopped, public SQL returned exact rows, and its original Release completed without replacing a role");

            if let Some(gate) = &mut prepared_gate {
                // This is still the exact un-released physical holder. Do not
                // mistake logical OwnerFailed or the missing Core for its exit.
                let charged = await_snapshot_at(context, 0, 1, 0, 0)?;
                ensure!(
                    Some(charged.bytes) == held_bytes && charged.participation_positions == 1,
                    "owner failure or sibling progress prematurely refunded the prepared holder: {charged:?}"
                );
                await_retained_fragment(context, &baseline)?;
                let owner_log = fs::read_to_string(
                    context
                        .scenario_root()
                        .join("owner-application-log/novarocks.INFO"),
                )?;
                ensure!(
                    !owner_log.lines().any(|line| line.contains(REMOVED)
                        && line.contains(&format!("query_high={}", a.query_id.hi))
                        && line.contains(&format!("query_low={} ", a.query_id.lo))
                        && line.contains("attempt=1")),
                    "original Scope disappeared before its prepared kernel holder exited"
                );
                context.action("after actual OwnerFailed the same prepared gate remained closed with its original charges and fragment, while the independent same-BE Task/SQL/Release completed; original failed Scope has not physically disappeared");
                let token = intent
                    .holds
                    .iter()
                    .find(|hold| hold.task_id == Some(1))
                    .context("same original prepared holder grant missing")?
                    .token
                    .clone();
                gate.release(context.deadline())?;
                await_explicit_release(context, &failed, &receiver, &token)?;
                context.action("the original scenario owner explicitly released the same prepared-holder rendezvous after OwnerFailed; real gate release is observed without signing physical exit or cleanup-ticket completion");
            }

            // This original transition is emitted only AFTER actual Scope
            // removal. No missing directory row or terminal bool proves it.
            let path = context
                .scenario_root()
                .join("owner-application-log/novarocks.INFO");
            let high = format!("query_high={}", a.query_id.hi);
            let low = format!("query_low={} ", a.query_id.lo);
            loop {
                let log = match fs::read_to_string(&path) {
                    Ok(log) => log,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
                    Err(error) => {
                        return Err(error)
                            .context("read original Participation removal writer output");
                    }
                };
                if let Some(line) = log.lines().find(|line| {
                    line.contains(REMOVED)
                        && line.contains(&high)
                        && line.contains(&low)
                        && line.contains("attempt=1")
                }) {
                    write_artifact(
                        context,
                        "owner-scope-removed.json",
                        &json!({"original_line":line}),
                    )?;
                    break;
                }
                thread::sleep(
                    context
                        .remaining("wait original failed Participation Scope removal")?
                        .min(Duration::from_millis(50)),
                );
            }
            await_resource_convergence(context, &baseline)?;
            let c = &intent.queries[2];
            let recovered =
                RawBackendSession::establish_declared(context, 0, c.query_id, c.frontend)?;
            let mut replacement = RawCreate::values(&recovered, 3);
            replacement.pipeline_dop = 1;
            replacement.exchange_input = true;
            ensure_accepted(&recovered, &replacement)?;
            await_installed(context, &recovered, &replacement)?;
            ensure!(
                recovered.release_when_ready(context)?
                    == proto::ReleaseParticipationOutcome::Released,
                "recovered receiver did not complete its real normal release"
            );
            await_resource_convergence(context, &baseline)?;
            ensure!(
                context.recheck_live_process_launch_identities()? == processes,
                "role process changed before actual failure cleanup/recovered admission"
            );
            ensure!(
                context
                    .handle()
                    .be_log_count(0, "declared Participation owner panic participation=")?
                    == 1,
                "the once-grant produced another actor panic"
            );
            write_artifact(
                context,
                "owner-isolation-oracle.json",
                &json!({
                    "owner_failed_original_R":true,"same_BE_healthy_actual_stop":true,
                    "failed_scope_physically_removed":true,"fresh_receiver_accepted_and_installed":true,
                    "healthy_rows":rows,"role_processes_unchanged":true,
                    "prepared_holder_kept_through_owner_failure":self.held_prepared,
                    "old_stop_or_source_ticket_pending":"NOT_PROVED; this scene covers the actual prepared holder only",
                    "final_required_audit":"original FE+3BE monitor Finals all raw0; no lifetime expiry/owner loss; actual Node joins and cluster/fixture cleanup. Scenario return alone is insufficient."
                }),
            )?;
            context.action("the original failed Participation Scope physically disappeared, original physical resources converged, and a fresh receiver was Accepted, Installed, then normally Released; final four raw exits remain independently required");
            Ok(())
        }
    }
    #[derive(Default)]
    pub(super) struct RetainedParticipationOwnerPanicIsolation {
        intent: DirectIntentOwner,
        with_expiry: bool,
    }
    impl RetainedParticipationOwnerPanicIsolation {
        pub(super) fn with_expiry() -> Self {
            Self {
                intent: DirectIntentOwner::default(),
                with_expiry: true,
            }
        }
    }
    impl Scenario for RetainedParticipationOwnerPanicIsolation {
        fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
            super::super::declarations::for_module(
                "execution_failure_prepared",
                self.name(),
                self.is_explicit_stage(),
                self.is_compatibility_stage(),
            )
        }
        fn name(&self) -> &'static str {
            if self.with_expiry {
                "execution-failure/participation-owner-panic-retained-expiry"
            } else {
                "execution-failure/participation-owner-panic-retained-isolation"
            }
        }
        fn is_explicit_stage(&self) -> bool {
            true
        }
        fn test_session_plan(
            &self,
            _launch: &ScenarioLaunchConfig,
            _root: &Path,
            cluster_size: usize,
            binary: &Path,
            deadline: Instant,
        ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>>
        {
            ensure!(
                cluster_size == 3,
                "retained Participation isolation requires native 1FE+3BE"
            );
            Ok(Some(self.intent.freeze(
                OWNER_ACTION,
                2,
                &[(0, 0, None)],
                false,
                cluster_size,
                binary,
                deadline,
            )?))
        }
        fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
            owner_launch_config(root)
        }
        fn run(&self, context: &mut ScenarioContext) -> Result<()> {
            ensure!(
                context.handle().be_count() == 3,
                "retained Participation isolation requires native 1FE+3BE"
            );
            let intent = self.intent.get()?;
            ensure!(
                intent.deadline == context.deadline(),
                "original retained isolation deadline changed"
            );
            let baseline = resource_snapshot(context)?;
            let processes = context.recheck_live_process_launch_identities()?;
            let original = &intent.queries[0];
            let retained = RawBackendSession::establish_declared(
                context,
                0,
                original.query_id,
                original.frontend,
            )?;
            intent.verify_backend(0, retained.backend)?;
            let mut completed = RawCreate::values(&retained, 1);
            completed.pipeline_dop = 1;
            ensure_accepted(&retained, &completed)?;
            observe_task_terminal_and_stopped(
                context,
                &retained,
                &raw_create_identity(&completed),
                proto::TaskState::Finished,
            )?;
            ensure!(
                retained.release_when_ready(context)?
                    == proto::ReleaseParticipationOutcome::Released,
                "the original completed Participation did not normally release before its cold panic"
            );
            let retired_line = loop {
                let lines = execution_retirement_lines(context, &retained)?;
                ensure!(
                    lines.len() <= 1,
                    "the original Participation retired execution custody more than once"
                );
                if let Some(line) = lines.into_iter().next() {
                    break line;
                }
                thread::sleep(
                    context
                        .remaining("observe original released Participation execution retirement")?
                        .min(Duration::from_millis(50)),
                );
            };
            await_resource_convergence(context, &baseline)?;
            // The same original retained route must answer normally before
            // the exact operation selects its failure grant.
            let normal = renew(
                &retained,
                raw_operation_envelope(15_000),
                1,
                "unselected original retained Participation Renew",
            )?;
            ensure!(
                outcome(&normal)? == proto::TaskOperationOutcome::ParticipationTerminalReceipt,
                "the normally released retained Participation changed its terminal decision: {normal:?}"
            );
            ensure!(
                context
                    .handle()
                    .be_log_count(0, "declared Participation owner panic participation=")?
                    == 0,
                "an unselected operation panicked the retained actor"
            );
            write_artifact(
                context,
                "retained-owner-before-panic.json",
                &json!({
                    "original_execution_scope_retirement_line":retired_line,
                    "normal_task_finished_and_actual_stopped":true,
                    "normal_release":"Released",
                    "unselected_retained_renew":"ParticipationTerminalReceipt",
                    "resources_returned_to_baseline":true
                }),
            )?;
            context.action("the original Task normally Finished and actually stopped, its Participation returned Released, the original application writer recorded execution-Scope retirement, resources converged, and the same retained route answered an unselected Renew with its terminal receipt");

            let trigger = intent
                .holds
                .first()
                .and_then(|hold| hold.trigger_operation)
                .context("original retained actor trigger operation missing")?;
            let mut envelope = raw_operation_envelope(15_000);
            envelope.operation_id = Some(proto::TaskOperationId {
                value: trigger.to_vec(),
            });
            let failed = renew(
                &retained,
                envelope,
                2,
                "declared original retained Participation owner panic",
            )?;
            ensure!(
                outcome(&failed)? == proto::TaskOperationOutcome::OwnerFailed,
                "the original cold R did not receive OwnerFailed: {failed:?}"
            );
            ensure!(
                context
                    .handle()
                    .be_log_count(0, "declared Participation owner panic participation=")?
                    == 1,
                "the retained actor did not report exactly one declared original panic"
            );
            let fresh = renew(
                &retained,
                raw_operation_envelope(15_000),
                3,
                "fresh control request after retained Participation owner failure",
            )?;
            ensure!(
                outcome(&fresh)? == proto::TaskOperationOutcome::OwnerFailed,
                "fresh failed-Participation control request lost its typed OwnerFailed verdict: {fresh:?}"
            );
            let rich_ingress = exercise_fresh_failed_rich_ingress(context, &retained, &completed)?;
            let healthy = &intent.queries[1];
            let sibling = RawBackendSession::establish_declared(
                context,
                0,
                healthy.query_id,
                healthy.frontend,
            )?;
            intent.verify_backend(0, sibling.backend)?;
            ensure!(
                sibling.backend == retained.backend,
                "the recovery Participation moved to another BE process"
            );
            let mut task = RawCreate::values(&sibling, 2);
            task.pipeline_dop = 1;
            ensure_accepted(&sibling, &task)?;
            observe_task_terminal_and_stopped(
                context,
                &sibling,
                &raw_create_identity(&task),
                proto::TaskState::Finished,
            )?;
            ensure!(
                sibling.release_when_ready(context)?
                    == proto::ReleaseParticipationOutcome::Released,
                "the same-BE sibling Participation did not normally release after the cold panic"
            );
            let connection = mysql_actor::connect_until(
                context.mysql_user(),
                context.mysql_port(),
                context.deadline(),
            )?;
            let rows: Vec<i64> = connection.run(|connection| {
                connection
                    .query(HEALTHY_SQL)
                    .context("healthy SQL after retained Participation panic")
            })?;
            ensure!(
                rows == [1, 1],
                "healthy SQL changed after retained isolation: {rows:?}"
            );
            await_resource_convergence(context, &baseline)?;
            // Unlike resource counters, this original barrier also waits for
            // the actual failed-owner inventory to retire before shutdown.
            // It does not replace the earlier exact execution-Scope event.
            crate::scenarios::result_delivery_support::await_idle(
                context,
                "retained-owner-panic",
                "after-cleanup",
                Instant::now(),
            )?;
            let expiry = if self.with_expiry {
                Some(await_retained_participation_expiry(context)?)
            } else {
                None
            };
            ensure!(
                context.recheck_live_process_launch_identities()? == processes,
                "a role process changed instead of isolating the original retained actor"
            );
            let after = execution_retirement_lines(context, &retained)?;
            ensure!(
                after == [retired_line.clone()],
                "cold cleanup published another original execution retirement: {after:?}"
            );
            ensure!(
                context
                    .handle()
                    .be_log_count(0, "declared Participation owner panic participation=")?
                    == 1,
                "the consumed cold grant caused another actor panic"
            );
            context.action("the exact retained-Participation Renew panicked its original actor and its original admitted R received OwnerFailed; another Participation on that same BE and public SQL completed normally, resources converged, original role identities stayed fixed, and the original execution-retirement record remained unique");
            // The short variant exercises process-close cleanup. The expiry
            // variant first observes the real directory empty under the same
            // process; neither changes H or invents a RetentionOrigin reading.
            context
                .shutdown()
                .context("join the original native roles after retained actor failure")?;
            write_artifact(
                context,
                "retained-owner-isolation-oracle.json",
                &json!({
                    "original_execution_scope_retirement_line":retired_line,
                    "cold_original_R_owner_failed":true,
                    "fresh_failed_participation_renew_owner_failed":true,
                    "fresh_failed_participation_rich_ingress":rich_ingress,
                    "failed_owner_inventory_converged_before_shutdown":true,
                    "same_BE_sibling_finished_actual_stopped_and_released":true,
                    "healthy_rows":rows,
                    "resources_returned_to_baseline":true,
                    "role_processes_unchanged_before_shutdown":true,
                    "original_scope_retirement_record_count":after.len(),
                    "explicit_original_cluster_shutdown_joined":true,
                    "horizon_directory_expiry":expiry,
                    "horizon_expiry_and_normal_close_forget":if self.with_expiry {
                        "Observed backend0 available participation_records reach zero before shutdown with no new work; exact original-origin non-restart and individual normal-close forget are not directly observed by this counter"
                    } else {
                        "NOT_PROVED: no exact original RetentionOrigin Native read surface; this scene exercises real process-close cleanup without waiting for H"
                    },
                    "independent_new_execution_scope_allocation_census":"NOT_PROVED: the original retirement record remained unique; this is not a scope-allocation counter",
                    "independent_cleanup_Work_profile":"NOT_PROVED: actual process cleanup is exercised through the original production path, with no synthetic Work/exit receipts",
                    "raw_long_running_role_exit_codes":"independent original process audit required; generic shutdown success does not prove every raw code is zero"
                }),
            )?;
            if self.with_expiry {
                context.action("backend0's available original directory census reached zero after real retention expiry with no new work; the same original roles and unique execution-retirement line were then rechecked before actual shutdown and joins; exact H origin and individual normal-close facts remain separate evidence");
            } else {
                context.action("the original FE and three BE owners were explicitly stopped and joined without waiting for retained H; exact original-origin expiry, normal-close forget and raw role codes remain separately scoped verification");
            }
            Ok(())
        }
    }

    /// Exercise the three actual rich entrypoints after the original actor
    /// failed. Wire legality is independent of a terminal Task's business gate.
    fn exercise_fresh_failed_rich_ingress(
        context: &mut ScenarioContext,
        retained: &RawBackendSession,
        completed: &RawCreate,
    ) -> Result<Value> {
        let establish = retained
            .establish_request
            .as_ref()
            .context("retained session lost its original Establish")?;
        let Some(proto::task_operation::Operation::UpdateParticipation(update)) =
            establish.operation.as_ref()
        else {
            bail!("original Establish is not a participation update");
        };
        let Some(proto::update_participation_request::Command::Establish(establish)) =
            update.command.as_ref()
        else {
            bail!("original participation update is not Establish");
        };
        let original_participation = establish
            .participation
            .as_ref()
            .context("original Establish lost its exact Participation")?;
        ensure!(
            original_participation == &completed.participation,
            "completed Task disagrees with the original established Participation"
        );
        let mut credential = establish
            .initial_credential
            .clone()
            .context("original Establish lost its credential domain")?;
        let previous_epoch = credential.epoch;
        credential.epoch = previous_epoch
            .checked_add(1)
            .context("original credential epoch cannot advance")?;
        let credential_shape = json!({
            "credential_domain_id":credential.credential_domain_id,
            "previous_epoch":previous_epoch,
            "requested_epoch":credential.epoch,
            "descriptor_count":credential.descriptors.len(),
            "descriptor_source":"unchanged original Establish descriptors; material omitted"
        });
        let requests = [
            ("create", RawCreate::values(retained, 3).operation()),
            (
                "task_domain",
                proto::TaskOperation {
                    envelope: Some(raw_operation_envelope(15_000)),
                    operation: Some(proto::task_operation::Operation::UpdateTask(
                        proto::UpdateTaskRequest {
                            identity: Some(raw_create_identity(completed)),
                            domains: vec![proto::TaskDomainUpdate {
                                domain: Some(
                                    proto::task_domain_update::Domain::OpenExchangeEdges(
                                        proto::OpenExchangeEdgesDomain {
                                            version: 1,
                                            edge_ids: vec![7],
                                        },
                                    ),
                                ),
                            }],
                        },
                    )),
                },
            ),
            (
                "participation_domain",
                proto::TaskOperation {
                    envelope: Some(raw_operation_envelope(15_000)),
                    operation: Some(proto::task_operation::Operation::UpdateParticipation(
                        proto::UpdateParticipationRequest {
                            command: Some(
                                proto::update_participation_request::Command::AdvanceDomain(
                                    proto::AdvanceParticipationDomainRequest {
                                        participation: Some(original_participation.clone()),
                                        domain: Some(proto::ParticipationDomainUpdate {
                                            domain: Some(
                                                proto::participation_domain_update::Domain::Credential(
                                                    credential,
                                                ),
                                            ),
                                        }),
                                    },
                                ),
                            ),
                        },
                    )),
                },
            ),
        ];
        let neutral_participation = novarocks_task_codec::identity::decode_participation_ref(
            original_participation,
            FieldPath::root("retained_failed_rich_participation"),
        )?;
        let participation_field = format!("participation={neutral_participation:?}");
        let mut operation_ids = std::collections::BTreeSet::new();
        let mut evidence = Vec::new();
        for (kind, request) in requests {
            let operation = request
                .envelope
                .as_ref()
                .and_then(|envelope| envelope.operation_id.as_ref())
                .context("fresh rich request lost its original operation")?;
            let operation = novarocks_task_codec::identity::decode_task_operation_id(
                operation,
                FieldPath::root("retained_failed_rich_operation"),
            )?
            .to_string();
            ensure!(
                operation_ids.insert(operation.clone()),
                "fresh rich requests reused an operation identity"
            );
            let artifact = format!("retained-owner-fresh-rich-{kind}.json");
            let mut item = json!({
                "kind":kind,"operation":operation,"participation":format!("{neutral_participation:?}"),
                "phase":"original request prepared; response not yet observed",
                "wire_legality":match kind {
                    "create"=>"RawCreate::values frozen package for a fresh Task identity",
                    "task_domain"=>"nonempty OpenExchangeEdges version 1 / edge 7; no claim that this terminal Task's frozen graph contains edge 7",
                    _=>"exact original credential lease/descriptors with checked next epoch"
                }
            });
            write_artifact(context, &artifact, &item)?;
            let receipt = retained.apply(
                request,
                &format!("fresh {kind} after retained Participation owner failure"),
            )?;
            ensure!(
                outcome(&receipt)? == proto::TaskOperationOutcome::OwnerFailed,
                "fresh failed-Participation {kind} did not reach its typed OwnerFailed verdict: {receipt:?}"
            );
            item["phase"] = json!("OwnerFailed observed; original physical cleanup pending");
            item["outcome"] = json!("OwnerFailed");
            write_artifact(context, &artifact, &item)?;
            let cleanup =
                await_rejected_ingress_retirement(context, &participation_field, &operation, kind)?;
            item["phase"] = json!("same original cleanup disposed and retired");
            item["cleanup"] = cleanup;
            write_artifact(context, &artifact, &item)?;
            evidence.push(item);
        }
        context.action("three fresh wire/codec-legal rich requests reached Worker after the retained Participation failed and each returned OwnerFailed; each original operation recorded positive disposal and retirement of the same scoped cleanup Work, before the healthy same-BE sibling");
        Ok(json!({
            "requests":evidence,"credential_shape":credential_shape,
            "native_handler_body_frame_and_marker_exit":"NOT_PROVED by cleanup log lines; independent original role/transport exit audit required",
            "general_R_ingress_geometry":"NOT_PROVED; these fresh failed requests use the original Native-backed pre-R cleanup path"
        }))
    }

    fn await_rejected_ingress_retirement(
        context: &ScenarioContext,
        participation_field: &str,
        operation: &str,
        kind: &str,
    ) -> Result<Value> {
        const DISPOSED: &str = "Worker original rejected ingress disposed";
        const RETIRED: &str = "Worker original rejected ingress retired";
        let path = context
            .scenario_root()
            .join("owner-application-log/novarocks.INFO");
        let operation_field = format!("operation={operation}");
        let kind_field = format!("kind=\"{kind}\"");
        let artifact = format!("retained-owner-fresh-rich-{kind}-cleanup.json");
        let mut last_observation = None;
        loop {
            let log = match fs::read_to_string(&path) {
                Ok(log) => log,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
                Err(error) => {
                    return Err(error).context("read original rich-ingress cleanup writer");
                }
            };
            let matching = |marker: &str| {
                log.lines()
                    .filter(|line| {
                        line.contains(marker)
                            && line.contains(participation_field)
                            && line.contains(&operation_field)
                            && line.contains(&kind_field)
                    })
                    .collect::<Vec<_>>()
            };
            let disposed = matching(DISPOSED);
            let retired = matching(RETIRED);
            let observation = json!({
                "operation":operation,"kind":kind,"participation_field":participation_field,
                "disposed_lines":disposed,"retired_lines":retired,
                "scope":"Worker cleanup facts; Native handler/marker exit remains independent"
            });
            if last_observation.as_ref() != Some(&observation) {
                write_artifact(context, &artifact, &observation)?;
                last_observation = Some(observation);
            }
            ensure!(
                disposed.len() <= 1 && retired.len() <= 1,
                "original rich-ingress {kind} operation {operation} emitted duplicate disposal/retirement records"
            );
            if let (Some(disposed), Some(retired)) = (disposed.first(), retired.first()) {
                let original_work = rejected_ingress_work_field(disposed)?;
                ensure!(
                    original_work == rejected_ingress_work_field(retired)?,
                    "rich-ingress disposal and retirement changed their actual scoped Work"
                );
                return Ok(json!({
                    "operation":operation,"kind":kind,"participation_field":participation_field,
                    "scoped_work":original_work,"disposed_line":disposed,"retired_line":retired,
                    "body_frame_and_native_marker_exit":"separate from these Worker cleanup facts"
                }));
            }
            thread::sleep(
                context
                    .remaining("await same original rich-ingress disposal and retirement")?
                    .min(Duration::from_millis(25)),
            );
        }
    }

    fn rejected_ingress_work_field(line: &str) -> Result<&str> {
        let (_, field) = line
            .split_once("work=")
            .context("original cleanup log has no scoped Work field")?;
        ensure!(
            field.starts_with("ScopedWorkRef { scope: ScopeId("),
            "original cleanup log did not record an actual ScopedWorkRef"
        );
        let end = field
            .find('}')
            .context("original cleanup scoped Work field is incomplete")?;
        let work = &field[..=end];
        ensure!(
            work.contains(", work: WorkId("),
            "original cleanup scoped Work has no actual WorkId"
        );
        Ok(work)
    }

    fn await_retained_participation_expiry(context: &mut ScenarioContext) -> Result<Value> {
        use crate::scenarios::result_delivery_support::metric;

        let port = context
            .handle()
            .runtime()
            .be
            .first()
            .context("retained expiry requires the original backend0")?
            .http;
        let url = format!("http://127.0.0.1:{port}/metrics?type=json");
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .context("build original backend retention observer")?;
        let epoch = Instant::now();
        let mut previous_start = None;
        let mut samples = Vec::new();
        let mut saw_positive = false;
        let outcome = (|| -> Result<()> {
            loop {
                let remaining =
                    context.remaining("observe original backend retained directory expiry")?;
                let started = Instant::now();
                let since_previous =
                    previous_start.map(|previous| started.duration_since(previous).as_micros());
                previous_start = Some(started);
                let sample_tag = format!("retained-owner-expiry-sample-{:03}", samples.len());
                let response = client
                    .get(&url)
                    .timeout(remaining.min(Duration::from_secs(1)))
                    .send()
                    .context("read original backend retention metrics")?;
                let status = response.status();
                write_artifact(
                    context,
                    &format!("{sample_tag}-http.json"),
                    &json!({
                        "url":url,"status":status.as_u16(),
                        "request_started_after_micros":started.duration_since(epoch).as_micros(),
                        "since_previous_request_start_micros":since_previous
                    }),
                )?;
                let body = response
                    .bytes()
                    .context("read original retention HTTP body")?;
                fs::write(
                    context.scenario_root().join(format!("{sample_tag}.body")),
                    &body,
                )
                .context("preserve original retention HTTP body")?;
                ensure!(
                    status.is_success(),
                    "retention observation returned HTTP {status}"
                );
                let rows: Value =
                    serde_json::from_slice(&body).context("decode original retention metrics")?;
                let rows = rows
                    .as_array()
                    .context("retention metrics are not an array")?;
                let available = metric(
                    rows,
                    "novarocks_backend_execution_quiescence_snapshot_available",
                    &[],
                )?;
                ensure!(
                    available <= 1,
                    "invalid original retention snapshot availability"
                );
                let records =
                    if available == 1 {
                        Some(metric(
                            rows,
                            "novarocks_backend_execution_quiescence",
                            &[("resource", "participation_records")],
                        )?)
                    } else {
                        ensure!(
                            !rows.iter().any(|row| row["tags"]["metric"]
                                == "novarocks_backend_execution_quiescence"),
                            "unavailable retention snapshot exposed stale inventory"
                        );
                        None
                    };
                samples.push(json!({
                    "sample":sample_tag,"snapshot_available":available,
                    "participation_records":records,
                    "request_started_after_micros":started.duration_since(epoch).as_micros(),
                    "request_finished_after_micros":epoch.elapsed().as_micros(),
                    "since_previous_request_start_micros":since_previous
                }));
                if let Some(records) = records {
                    saw_positive |= records != 0;
                    if records == 0 {
                        ensure!(
                            saw_positive,
                            "expiry observer never sampled the original nonempty retained directory"
                        );
                        return Ok(());
                    }
                }
                // Poll an actual condition under the unchanged scene deadline;
                // no wall-clock sleep is used as a substitute for H expiry.
                thread::sleep(
                    context
                        .remaining("wait next original retention census")?
                        .min(Duration::from_millis(500)),
                );
            }
        })();
        let evidence = json!({
            "backend_index":0,"url":url,"elapsed_micros":epoch.elapsed().as_micros(),
            "poll_interval_millis":500,"original_scenario_deadline_unchanged":true,
            "snapshot_family":"novarocks_backend_execution_quiescence",
            "resource":"participation_records","observed_nonempty_retained_directory":saw_positive,
            "all_original_backend0_participation_records_removed_before_shutdown":outcome.is_ok(),
            "error":outcome.as_ref().err().map(|error| format!("{error:#}")),
            "samples":samples,
            "scope_exit":"independently witnessed earlier by the original execution-retirement event",
            "origin_contract":"the same immutable RetentionOrigin and runtime first-deadline contract apply; this HTTP census does not directly expose retired_at or prove no restart",
            "normal_close_forget":"not an independent per-stream normal-close observation"
        });
        write_artifact(
            context,
            "retained-owner-expiry-observations.json",
            &evidence,
        )?;
        outcome?;
        context.action("with all original work already converged and no new work issued, available backend0 participation_records decreased from a positive actual directory inventory to zero before shutdown; every HTTP body, status and actual sampling interval was preserved");
        Ok(evidence)
    }

    fn execution_retirement_lines(
        context: &ScenarioContext,
        session: &RawBackendSession,
    ) -> Result<Vec<String>> {
        let path = context
            .scenario_root()
            .join("owner-application-log/novarocks.INFO");
        let log = match fs::read_to_string(&path) {
            Ok(log) => log,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => {
                return Err(error)
                    .context("read original retained Participation retirement writer");
            }
        };
        let high = format!("query_high={}", session.query_id.hi);
        let low = format!("query_low={} ", session.query_id.lo);
        Ok(log
            .lines()
            .filter(|line| {
                line.contains(REMOVED)
                    && line.contains(&high)
                    && line.contains(&low)
                    && line.contains("attempt=1")
            })
            .map(str::to_owned)
            .collect())
    }

    fn renew(
        session: &RawBackendSession,
        envelope: proto::TaskOperationEnvelope,
        sequence: u64,
        subject: &str,
    ) -> Result<proto::TaskOperationReceipt> {
        let response: RawUnaryResponse<proto::ApplyTaskOperationsResponse> = raw_unary_response(
            &session.control_connector,
            "/novarocks.NovaRocksGrpc/ApplyTaskControlOperations",
            &session.authorization,
            proto::ApplyTaskControlOperationsRequest {
                operations: vec![proto::TaskControlOperation {
                    envelope: Some(envelope),
                    control: Some(proto::task_control_operation::Control::RenewLease(
                        proto::RenewParticipationLeaseRequest {
                            participation: Some(session.participation_under(session.frontend, 1)),
                            lease: Some(proto::ParticipationLeaseGrant {
                                sequence,
                                valid_for_millis: 30_000,
                            }),
                        },
                    )),
                }],
            },
            session.original_deadline,
        )?;
        only_successful_receipt(response, subject)
    }
}
