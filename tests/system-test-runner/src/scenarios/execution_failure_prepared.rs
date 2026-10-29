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
use novarocks_cluster_harness::{QueryExecutionResourceSnapshot, ServerHandle};
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
        Box::new(context_owner_panic::ContextOwnerPanicIsolation::default()),
        Box::new(context_owner_panic::ContextOwnerPanicIsolation::with_prepared_holder()),
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
        launch.config_overlay.be = Some("[runtime]\ntask_preparation_max_tasks_per_context = 2\ntask_preparation_max_tasks = 3\ntask_preparation_max_bytes = 67108864\ntask_preparation_max_workers = 1\n".into());
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
                Ok(proto::ReleaseQueryContextOutcome::Released) => {}
                Ok(other) => cleanup_failures.push(anyhow::anyhow!(
                    "prepared scenario context release did not complete: {other:?}"
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
        charged.context_positions == 1
            && charged.bytes > 0
            && charged.context_limit == 2
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
            "positions": charged.positions, "context_positions": charged.context_positions,
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
        after_sibling.bytes == charged.bytes && after_sibling.context_positions == 1,
        "healthy sibling returned the held kernel's preparation charges"
    );
    let still_retained = await_retained_fragment(context, baseline)?;
    write_artifact(
        context,
        "prepared-held-after-health.json",
        &json!({
            "covered": still_held.json(), "positions": after_sibling.positions,
            "context_positions": after_sibling.context_positions, "bytes": after_sibling.bytes,
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
            "positions": released.positions, "context_positions": released.context_positions,
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
        query_context: Some(session.context_under(session.frontend, 1)),
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
        let driver = tokio::spawn(connection);
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
                        CoveredStatusStreamFact::ContextConvergence(_)
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
        driver.abort();
        let joined = driver.await;
        let cleanup = match joined {
            Ok(Ok(())) => Ok(()),
            Err(error) if error.is_cancelled() => Ok(()),
            Ok(Err(error)) => Err(anyhow::anyhow!(
                "prepared subscription original H2 driver exit failed: {error}"
            )),
            Err(error) => Err(anyhow::anyhow!(
                "prepared subscription original H2 driver panicked: {error}"
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
        "NOVAROCKS_PREPARED_FAILURE_EXIT_RELEASED backend={} execution_id={}:{}:1 stage=1 task={} token={} reason=release",
        session.backend, session.query_id.hi, session.query_id.lo, create.task_id, token
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
        .get("native_query_active_fragments")
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
mod context_owner_panic {
    use super::*;
    use crate::scenarios::native_compatibility::{
        RawUnaryResponse, only_successful_receipt, raw_operation_envelope, raw_unary_response,
    };
    use crate::scenarios::native_creation::{await_installed, outcome};

    const OWNER_ACTION: &str = "context-owner-panic-on-renew";
    const REMOVED: &str = "Original Worker Context Scope removed";

    #[derive(Default)]
    pub(super) struct ContextOwnerPanicIsolation {
        intent: DirectIntentOwner,
        held_prepared: bool,
    }
    impl ContextOwnerPanicIsolation {
        pub(super) fn with_prepared_holder() -> Self {
            Self {
                intent: DirectIntentOwner::default(),
                held_prepared: true,
            }
        }
    }
    impl Scenario for ContextOwnerPanicIsolation {
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
                "execution-failure/context-owner-panic-held-prepared-isolation"
            } else {
                "execution-failure/context-owner-panic-isolation"
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
                "Context isolation requires native 1FE+3BE"
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
            let mut launch = ScenarioLaunchConfig::default();
            // The log is this scene's actual application writer output. It is
            // not a synthetic state source or a replacement cleanup command.
            let log = root.join("owner-application-log");
            launch.config_overlay.be = Some(format!(
                "sys_log_dir = {:?}\nlog_filter = \"info,novarocks_native_adapter::backend_application=debug,novarocks_worker::control_runtime::node_bootstrap=debug,novarocks_worker::control_runtime::node=debug,novarocks_worker::control_runtime::task=debug,novarocks_worker::control_runtime::context_host::requests=debug,novarocks_worker::control_runtime::causal=debug,novarocks_worker::control_runtime::context_host::task_driver=debug,novarocks_worker::control_runtime::context_host::teardown=debug\"\n[runtime]\ntask_normal_close_max_records = 1\ntask_normal_close_max_bytes = 67108864\n",
                log.to_string_lossy(),
            ));
            Ok(launch)
        }
        fn run(&self, context: &mut ScenarioContext) -> Result<()> {
            ensure!(
                context.handle().be_count() == 3,
                "Context isolation requires native 1FE+3BE"
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
                    charged.bytes > 0 && charged.context_positions == 1,
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
                context.action("the existing exact prepared-failure-exit gate retained the real original kernel, covered Failing with no actual-stop and nonzero preparation charges before the Context actor panic; no old cleanup-ticket phase is inferred");
                Some(charged.bytes)
            } else {
                await_installed(context, &failed, &receiver)?;
                context.action("installed an original live receiver on BE0 with the sole paid inbound row before the declared Context failure");
                None
            };

            // A different operation on the SAME Context is not the grant.
            let normal = renew(
                &failed,
                raw_operation_envelope(15_000),
                1,
                "unselected same-Context Renew",
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
                "declared original Context owner panic",
            )?;
            ensure!(
                outcome(&receipt)? == proto::TaskOperationOutcome::OwnerFailed,
                "actual owner panic did not return the original R OwnerFailed: {receipt:?}"
            );
            ensure!(
                context
                    .handle()
                    .be_log_count(0, "declared Context owner panic context=")?
                    == 1,
                "the original actor turn did not report exactly one declared panic"
            );
            context.action("unselected same-Context Renew remained normal; the exact declared operation panicked inside its original Context Actor turn and Common returned OwnerFailed through that same R");

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
                    .context("healthy public SQL after actual Context owner failure")
            })?;
            ensure!(
                rows == [1, 1],
                "healthy SQL rows changed after owner isolation: {rows:?}"
            );
            ensure!(
                context.recheck_live_process_launch_identities()? == processes,
                "owner failure replaced a role process instead of isolating its Context"
            );
            ensure!(
                sibling.release_when_ready(context)? == proto::ReleaseQueryContextOutcome::Released,
                "the independent same-BE Context did not really release"
            );
            context.action("an independent Context on the SAME BE Finished and actually stopped, public SQL returned exact rows, and its original Release completed without replacing a role");

            if let Some(gate) = &mut prepared_gate {
                // This is still the exact un-released physical holder. Do not
                // mistake logical OwnerFailed or the missing Core for its exit.
                let charged = await_snapshot_at(context, 0, 1, 0, 0)?;
                ensure!(
                    Some(charged.bytes) == held_bytes && charged.context_positions == 1,
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
                        return Err(error).context("read original Context removal writer output");
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
                        .remaining("wait original failed Context Scope removal")?
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
                    == proto::ReleaseQueryContextOutcome::Released,
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
                    .be_log_count(0, "declared Context owner panic context=")?
                    == 1,
                "the once-grant produced another actor panic"
            );
            write_artifact(
                context,
                "owner-isolation-oracle.json",
                &json!({
                    "owner_failed_original_R":true,"same_BE_healthy_actual_stop":true,
                    "failed_scope_physically_removed":true,"fresh_receiver_installed_at_record_limit":1,
                    "healthy_rows":rows,"role_processes_unchanged":true,
                    "prepared_holder_kept_through_owner_failure":self.held_prepared,
                    "old_stop_or_source_ticket_pending":"NOT_PROVED; this scene covers the actual prepared holder only",
                    "final_required_audit":"original FE+3BE monitor Finals all raw0; no lifetime expiry/owner loss; actual Node joins and cluster/fixture cleanup. Scenario return alone is insufficient."
                }),
            )?;
            context.action("the original failed Context Scope physically disappeared, original quota recovered at the unchanged one-row bound, and a fresh receiver Installed then normally Released; final four raw exits remain independently required");
            Ok(())
        }
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
                        proto::RenewQueryExecutionLeaseRequest {
                            query_context: Some(session.context_under(session.frontend, 1)),
                            lease: Some(proto::QueryExecutionLeaseGrant {
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
