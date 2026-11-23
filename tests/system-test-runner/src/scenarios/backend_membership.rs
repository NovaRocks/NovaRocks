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

//! Process-boundary backend self-registration acceptance.

use super::distributed_writer::{assert_commit_never_invoked, publication_terminals};
use super::native_sql_controls::{SqlIntentOwner, StatementSpec};
use super::trino_protocol::require_error;
use crate::actors::sql::{self, SqlSession, StatementFailure, StatementResult, TrinoValue};
use crate::actors::trino_protocol::TrinoClient;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_failpoint::target_binding::StatementExecutionBinding;
use novarocks_types::{BackendProcessId, FrontendProcessId};
use serde::Deserialize;
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};

const REQUIRED_BACKENDS: usize = 3;

/// The task protocol's restart rendezvous: it holds one applied
/// `EstablishParticipation` open so the harness can replace that exact process.
/// It replaces the retired protocol's `restart-after-init-ack` because
/// establish is the task protocol's first per-backend admission point.
const RESTART_AFTER_ESTABLISH_PARTICIPATION: &str = "restart-after-establish-participation";

/// Runner-side half of the establish-before-restart handoff.
///
/// The listener exists before the query starts. The backend sends the exact
/// fault token after it applies EstablishParticipation and keeps that RPC
/// parked until the runner completes process replacement. A log marker proves
/// the same event but cannot synchronize it: a polling runner can see the
/// marker only after the backend has consumed the request boundary.
struct RestartAfterEstablishRendezvous {
    path: PathBuf,
    token: String,
    listener: UnixListener,
}

impl RestartAfterEstablishRendezvous {
    const CANCEL: &'static [u8] = b"scenario-cancelled";

    fn bind(token: &str) -> Result<Self> {
        let path = novarocks_failpoint::restart_after_establish_rendezvous_socket_path(token)
            .map_err(anyhow::Error::msg)?;
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("remove stale establish rendezvous {}", path.display()))?;
        }
        let listener = UnixListener::bind(&path)
            .with_context(|| format!("bind establish rendezvous {}", path.display()))?;
        Ok(Self {
            path,
            token: token.to_owned(),
            listener,
        })
    }

    /// Receives the backend's exact token before the runner kills the old
    /// process. On timeout, connect once with a local cancellation payload so
    /// the accept worker can finish before cleanup removes the listener.
    fn wait_for_trigger(&self, deadline: Instant) -> Result<UnixStream> {
        let listener = self
            .listener
            .try_clone()
            .context("clone establish rendezvous listener")?;
        let token = self.token.clone();
        std::thread::scope(|scope| -> Result<UnixStream> {
            let (sender, receiver) = mpsc::sync_channel(1);
            scope.spawn(move || {
                let result = (|| -> Result<UnixStream> {
                    let (mut stream, _) =
                        listener.accept().context("accept establish rendezvous")?;
                    let mut payload = Vec::new();
                    stream
                        .read_to_end(&mut payload)
                        .context("read establish rendezvous token")?;
                    if payload == token.as_bytes() {
                        return Ok(stream);
                    }
                    if payload == Self::CANCEL {
                        bail!("scenario ended before the establish rendezvous fired");
                    }
                    bail!("establish rendezvous received an unexpected token");
                })();
                let _ = sender.send(result);
            });
            match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(result) => result,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    Self::cancel(&self.path);
                    let _ = receiver.recv();
                    bail!("timed out waiting for establish-before-restart rendezvous");
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    bail!("establish rendezvous worker disconnected before an arrival");
                }
            }
        })
    }

    fn cancel(path: &Path) {
        let Ok(mut stream) = UnixStream::connect(path) else {
            return;
        };
        let _ = stream.write_all(Self::CANCEL);
        let _ = stream.shutdown(Shutdown::Write);
    }
}

impl Drop for RestartAfterEstablishRendezvous {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(BackendSelfRegistration),
        Box::new(PreReadyReplan::default()),
        Box::new(PreReadyDmlNoRecovery::default()),
    ]
}

struct BackendSelfRegistration;

impl Scenario for BackendSelfRegistration {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        let mut declaration = crate::scenarios::declarations::for_module(
            "backend_membership",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        );
        declaration.impact.paths.extend([
            "novarocks/query-application/src/sql/trino_admission.rs".into(),
            "novarocks/query-application/src/system_tables/**".into(),
            "novarocks/query-application/src/query_control/**".into(),
            "tests/system-test-runner/src/actors/trino_protocol.rs".into(),
        ]);
        declaration
    }

    fn name(&self) -> &'static str {
        "membership/self-registration"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "membership acceptance requires exactly {REQUIRED_BACKENDS} BEs"
        );
        let initial_process_ids = (0..REQUIRED_BACKENDS)
            .map(|index| context.handle().backend_process_id(index))
            .collect::<Result<Vec<_>>>()?;
        // A parsed process identity is valid by construction, so the fact
        // worth asserting is that the three backends are three *different*
        // processes — a shared identity would mean the topology is not what
        // this acceptance claims to exercise.
        let distinct = initial_process_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        ensure!(
            distinct.len() == initial_process_ids.len(),
            "self-registered backends share a process identity: {initial_process_ids:?}"
        );
        context.action(format!(
            "observed {} eligible self-registered backend process identities",
            initial_process_ids.len()
        ));

        let actor = membership_http_actor(context, "original")?;
        query_one(context, "before FE restart")?;
        let original_fe = require_nodes(context, &actor, "initial", &initial_process_ids)?;
        let deadline = context.deadline();
        context
            .handle()
            .drain_be_until(REQUIRED_BACKENDS - 1, deadline)
            .context("gracefully drain one BE through SIGTERM")?;
        query_one(context, "while one BE is draining")?;
        let after_drain_fe = require_nodes(
            context,
            &actor,
            "after-single-drain",
            &initial_process_ids[..REQUIRED_BACKENDS - 1],
        )?;
        ensure!(
            after_drain_fe == original_fe,
            "BE drain changed the FE identity"
        );
        context.action(
            "proved SIGTERM removes a BE from future eligibility while the remaining BEs serve queries",
        );
        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(REQUIRED_BACKENDS - 1, deadline)
            .context("replace drained BE and wait for a new eligible process identity")?;
        let live_process_ids = (0..REQUIRED_BACKENDS)
            .map(|index| context.handle().backend_process_id(index))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            live_process_ids[REQUIRED_BACKENDS - 1] != initial_process_ids[REQUIRED_BACKENDS - 1],
            "replacement after drain must receive a new BackendProcessId"
        );
        ensure!(
            require_nodes(
                context,
                &actor,
                "after-drained-replacement",
                &live_process_ids
            )? == original_fe,
            "BE replacement changed FE identity",
        );

        let deadline = context.deadline();
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart FE and wait for BE renew announce plus exact heartbeat")?;
        let after_fe_restart = (0..REQUIRED_BACKENDS)
            .map(|index| context.handle().backend_process_id(index))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            after_fe_restart == live_process_ids,
            "FE restart must rebuild membership from the same live BE process identities"
        );
        let actor = membership_http_actor(context, "restarted")?;
        let restarted_fe = require_nodes(context, &actor, "after-fe-restart", &after_fe_restart)?;
        ensure!(
            restarted_fe != original_fe,
            "FE restart reused the original process identity"
        );
        context.action("proved FE restart rebuilds only from BE renew announce and heartbeat");

        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(0, deadline)
            .context("restart BE[0] and wait for replacement identity eligibility")?;
        let replacement_process_id = context.handle().backend_process_id(0)?;
        ensure!(
            replacement_process_id != initial_process_ids[0],
            "same endpoint replacement must receive a new BackendProcessId"
        );
        query_one(context, "after endpoint replacement")?;
        let live_after_same_endpoint = (0..REQUIRED_BACKENDS)
            .map(|index| context.handle().backend_process_id(index))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            require_nodes(
                context,
                &actor,
                "after-endpoint-replacement",
                &live_after_same_endpoint
            )? == restarted_fe,
            "same-endpoint replacement changed the current FE identity",
        );
        context.action(
            "proved endpoint replacement cannot inherit the prior process identity and remains queryable only after re-verification",
        );
        no_available_backends(context, &actor)
    }
}

fn membership_http_actor(context: &ScenarioContext, launch: &str) -> Result<TrinoClient> {
    TrinoClient::new(
        context.trino_port(),
        context.sql_user(),
        context.deadline(),
        &context
            .scenario_root()
            .join(format!("trino-membership-http-{launch}")),
    )
}

#[derive(Debug, Deserialize)]
struct ObservedBackend {
    process_id: String,
    build_identity: String,
    reported_state: String,
    eligible: bool,
    lease_valid: bool,
    identity_verified: bool,
    endpoint_owned: bool,
}

impl ObservedBackend {
    fn expected_node_state(&self) -> &str {
        if self.eligible {
            "active"
        } else if self.reported_state == "Draining"
            && self.lease_valid
            && self.identity_verified
            && self.endpoint_owned
        {
            "draining"
        } else {
            "inactive"
        }
    }
}

fn management_backends(
    context: &mut ScenarioContext,
    phase: &str,
) -> Result<BTreeMap<String, ObservedBackend>> {
    context.remaining("read management topology")?;
    let response = context
        .handle()
        .frontend_management_get("/v1/frontend/backends", Duration::from_secs(1))?;
    fs::write(
        context
            .scenario_root()
            .join(format!("membership-{phase}.json")),
        &response.body,
    )?;
    ensure!(
        response.status == 200,
        "{phase}: management topology returned HTTP {}",
        response.status
    );
    let document: serde_json::Value = serde_json::from_str(&response.body)?;
    ensure!(
        document["schema_version"] == 1,
        "{phase}: unexpected topology document {document}"
    );
    let rows: Vec<ObservedBackend> = serde_json::from_value(document["backends"].clone())?;
    let mut backends = BTreeMap::new();
    for row in rows {
        ensure!(
            backends.insert(row.process_id.clone(), row).is_none(),
            "{phase}: management reported a duplicate BE process",
        );
    }
    Ok(backends)
}

fn require_nodes(
    context: &mut ScenarioContext,
    actor: &TrinoClient,
    phase: &str,
    live_backends: &[BackendProcessId],
) -> Result<FrontendProcessId> {
    let before = management_backends(context, &format!("{phase}-before"))?;
    let info = actor.get(&format!("{}/v1/info", actor.base()))?;
    ensure!(info.status == 200, "{phase}: FE ServerInfo is unavailable");
    let info = info.json()?;
    let fe_version = info["nodeVersion"]["version"]
        .as_str()
        .filter(|value| !value.is_empty())
        .context("ServerInfo omitted original FE version")?;
    let pages = actor.execute(
        "SELECT node_id, http_uri, node_version, coordinator, state FROM system.runtime.nodes ORDER BY node_id",
    ).with_context(|| format!("observe SQL nodes at {phase}"))?;
    let mut metadata = None;
    let mut rows = Vec::new();
    for page in &pages {
        let body = page.assert_success()?;
        if let Some(columns) = body["columns"].as_array() {
            if let Some(prior) = &metadata {
                ensure!(
                    prior == columns,
                    "{phase}: node column signatures changed while paging"
                );
            } else {
                metadata = Some(columns.clone());
            }
        }
        if let Some(data) = body["data"].as_array() {
            rows.extend(data.iter().cloned());
        }
        if page.next_uri()?.is_none() {
            ensure!(
                body["stats"]["state"] == "FINISHED",
                "{phase}: invalid nodes terminal page"
            );
        }
    }
    let columns = metadata.context("nodes response omitted column declarations")?;
    let after = management_backends(context, &format!("{phase}-after"))?;
    let expected_columns = [
        ("node_id", "varchar"),
        ("http_uri", "varchar"),
        ("node_version", "varchar"),
        ("coordinator", "boolean"),
        ("state", "varchar"),
    ];
    ensure!(
        columns.len() == expected_columns.len()
            && columns
                .iter()
                .zip(expected_columns)
                .all(|(column, (name, ty))| {
                    column["name"] == name && column["typeSignature"]["rawType"] == ty
                }),
        "{phase}: unexpected nodes declaration: {columns:?}"
    );
    let mut frontend = None;
    let mut observed = BTreeSet::new();
    let mut active = BTreeSet::new();
    for row in &rows {
        use serde_json::Value;
        let Some(
            [
                Value::String(id),
                uri,
                Value::String(version),
                Value::Bool(coordinator),
                Value::String(state),
            ],
        ) = row.as_array().map(Vec::as_slice)
        else {
            bail!("{phase}: wrong nodes row shape: {row:?}");
        };
        ensure!(
            observed.insert(id.as_str()),
            "{phase}: repeated node ID {id}"
        );
        if *coordinator {
            let uri = uri
                .as_str()
                .context("FE has an HTTP listener but its SQL row has no URI")?;
            let uri = reqwest::Url::parse(uri)?;
            ensure!(
                uri.scheme() == "http"
                    && uri.port() == Some(context.trino_port())
                    && version == fe_version
                    && state == "active",
                "{phase}: coordinator projection differs from its original HTTP listener: {row:?}"
            );
            let id = id
                .parse::<FrontendProcessId>()
                .map_err(anyhow::Error::msg)?;
            ensure!(
                frontend.replace(id).is_none(),
                "{phase}: expected exactly one FE row"
            );
        } else {
            ensure!(
                uri.is_null(),
                "{phase}: native BE endpoint advertised as HTTP"
            );
            let matches_management =
                before.get(id).into_iter().chain(after.get(id)).any(|fact| {
                    version == &fact.build_identity && state == fact.expected_node_state()
                });
            ensure!(
                matches_management,
                "{phase}: SQL node changed original topology facts: {row:?}"
            );
            if state == "active" {
                active.insert(id.parse::<BackendProcessId>().map_err(anyhow::Error::msg)?);
            }
        }
    }
    for id in before.keys().filter(|id| after.contains_key(*id)) {
        ensure!(
            observed.contains(id.as_str()),
            "{phase}: retained BE omitted from nodes: {id}"
        );
    }
    let expected_active = live_backends.iter().copied().collect::<BTreeSet<_>>();
    ensure!(
        active == expected_active,
        "{phase}: active SQL processes {active:?} != {expected_active:?}"
    );
    context.action(format!(
        "checked FE version and HTTP endpoint, BE topology facts and exact active process IDs at {phase}",
    ));
    frontend.with_context(|| format!("{phase}: missing FE node"))
}

fn no_available_backends(context: &mut ScenarioContext, actor: &TrinoClient) -> Result<()> {
    for index in 0..REQUIRED_BACKENDS {
        let deadline = context.deadline();
        context
            .handle()
            .drain_be_until(index, deadline)
            .with_context(|| format!("drain BE[{index}] before the FE-only control check"))?;
    }
    let backends = management_backends(context, "zero-available")?;
    ensure!(
        backends.values().all(|row| !row.eligible),
        "a BE remains eligible: {backends:?}"
    );

    let remaining = actor.execute("SELECT * FROM system.runtime.nodes")?;
    let end = remaining
        .last()
        .context("metadata SELECT omitted its terminal response")?
        .json()?;
    ensure!(
        end.get("error").is_some()
            && end.get("nextUri").is_none()
            && remaining
                .iter()
                .all(|page| page.json().is_ok_and(|body| body.get("data").is_none())),
        "ordinary metadata SELECT did not preserve its BE admission requirement: {end}",
    );

    let post = actor.submit("SELECT BIGINT '234'")?;
    let initial = post.assert_success()?;
    let uri = post
        .next_uri()?
        .context("zero-BE POST omitted original URI")?;
    let control = (|| -> Result<()> {
        let id = initial["id"]
            .as_str()
            .context("zero-BE POST omitted query ID")?;
        ensure!(
            initial["stats"]["state"] == "QUEUED",
            "target was not queued: {initial}"
        );
        let pages = actor.execute(&format!(
            "CALL system.runtime.kill_query(query_id => '{id}', message => 'zero eligible backends')"
        ))?;
        for page in &pages {
            page.assert_success()?;
        }
        let call = pages
            .last()
            .context("FE-only cancellation omitted its terminal page")?
            .json()?;
        ensure!(
            call["stats"]["state"] == "FINISHED",
            "FE-only cancellation did not finish: {call}"
        );
        let killed = require_error(
            &actor.get(&uri)?,
            (38, "ADMINISTRATIVELY_KILLED", "USER_ERROR"),
        )?;
        ensure!(
            killed["id"] == id
                && killed["error"]["message"]
                    == format!(
                        "Query killed by user '{}'. Message: zero eligible backends",
                        context.sql_user(),
                    ),
            "wrong original target or management receipt: {killed}",
        );
        Ok(())
    })();
    if control.is_err() {
        let cleanup = actor.delete(&uri).and_then(|response| {
            ensure!(
                response.status == 204 || response.status == 404,
                "target cleanup HTTP {}",
                response.status
            );
            Ok(())
        });
        return membership_cleanup(control, cleanup);
    }

    context.action("at zero eligible BEs verified management topology, ordinary metadata admission refusal and the original FE-only cancellation receipt");
    Ok(())
}

fn query_one(context: &mut ScenarioContext, phase: &str) -> Result<()> {
    let mut connection = {
        context.remaining(&format!("connect {phase}"))?;
        SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?
    };
    let rows = sql::integer_column(
        &connection
            .execute("SELECT 1")
            .with_context(|| format!("run distributed query {phase}"))?,
    )?;
    ensure!(
        rows == vec![1],
        "distributed query {phase} returned {rows:?}"
    );
    Ok(())
}

const REPLAN_SQL: &str = "SELECT v FROM pre_ready_replan.ns.probe ORDER BY v";
const DML_SQL: &str = "INSERT INTO pre_ready_dml.ns.orders VALUES (1, 10), (2, 20)";

// The original submitted query and its physical reader remain one owner.
struct MembershipActor {
    result: mpsc::Receiver<std::result::Result<StatementResult, StatementFailure>>,
    worker: Option<thread::JoinHandle<Result<()>>>,
    cancel: novarocks_test_trino_client::CancelHandle,
    deadline: Instant,
    terminal: Arc<AtomicBool>,
}
impl MembershipActor {
    fn start(
        mut session: SqlSession,
        pending: novarocks_test_trino_client::PendingStatement,
        deadline: Instant,
    ) -> Result<Self> {
        let cancel = pending.cancel_handle();
        let terminal = Arc::new(AtomicBool::new(false));
        let saved = terminal.clone();
        let (sender, result) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("membership-original-sql".into())
            .spawn(move || {
                let response = session.run_pending(pending);
                saved.store(
                    matches!(&response, Ok(_) | Err(StatementFailure::Trino(_))),
                    Ordering::Release,
                );
                match sender.send(response) {
                    Ok(()) => Ok(()),
                    Err(mpsc::SendError(Err(primary))) => Err(anyhow::Error::new(primary)
                        .context("original membership SQL result receiver closed")),
                    Err(mpsc::SendError(Ok(_))) => {
                        bail!("original membership SQL result receiver closed")
                    }
                }
            });
        match worker {
            Ok(worker) => Ok(Self {
                result,
                worker: Some(worker),
                cancel,
                deadline,
                terminal,
            }),
            Err(primary) => membership_cleanup(
                Err(primary.into()),
                cancel.delete(deadline).map_err(Into::into),
            ),
        }
    }
    fn finish_after<T>(mut self, context: &mut ScenarioContext, primary: Result<T>) -> Result<T> {
        let attempted = primary.is_err() && !self.terminal.load(Ordering::Acquire);
        let mut cleanup = if attempted {
            self.cancel
                .delete(self.deadline)
                .map_err(anyhow::Error::from)
        } else {
            Ok(())
        };
        if cleanup.is_err() {
            cleanup = membership_cleanup(cleanup, context.shutdown());
        }
        if let Some(worker) = self.worker.take() {
            cleanup = membership_cleanup(
                cleanup,
                worker
                    .join()
                    .map_err(|payload| {
                        anyhow::Error::new(MembershipActorPanic(Mutex::new(payload)))
                    })
                    .and_then(|result| result),
            );
        }
        if !attempted && !self.terminal.load(Ordering::Acquire) {
            let cancelled = self
                .cancel
                .delete(self.deadline)
                .map_err(anyhow::Error::from);
            let shutdown = if cancelled.is_err() {
                context.shutdown()
            } else {
                Ok(())
            };
            cleanup = membership_cleanup(cleanup, membership_cleanup(cancelled, shutdown));
        }
        if let Ok(Err(error)) = self.result.try_recv() {
            cleanup = membership_cleanup(cleanup, Err(error.into()));
        }
        membership_cleanup(primary, cleanup)
    }
}
impl Drop for MembershipActor {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            let cancelled = if !self.terminal.load(Ordering::Acquire) {
                self.cancel
                    .delete(self.deadline)
                    .map_err(anyhow::Error::from)
            } else {
                Ok(())
            };
            let joined = worker
                .join()
                .map_err(|payload| anyhow::Error::new(MembershipActorPanic(Mutex::new(payload))))
                .and_then(|result| result);
            let pending = match self.result.try_recv() {
                Ok(Err(primary)) => Err(anyhow::Error::new(primary)
                    .context("original SQL result failed during actor Drop")),
                _ => Ok(()),
            };
            if let Err(error) = membership_cleanup(membership_cleanup(cancelled, joined), pending) {
                eprintln!("Original membership SQL actor cleanup failed: {error:#}");
            }
        }
    }
}
fn membership_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(MembershipActorCleanup { primary, cleanup }.into()),
    }
}
fn membership_bigint_rows(result: &StatementResult) -> Result<Vec<i64>> {
    ensure!(
        result.is_result_set()
            && result.columns.len() == 1
            && result.columns[0].type_signature.raw_type == "bigint"
            && result.columns[0].type_signature.arguments.is_empty(),
        "membership scan lost its declared BIGINT column"
    );
    sql::integer_column(result)
}
fn membership_order_rows(result: &StatementResult) -> Result<Vec<(i32, i64)>> {
    ensure!(
        result.is_result_set()
            && result.columns.len() == 2
            && result.columns[0].type_signature.raw_type == "integer"
            && result.columns[1].type_signature.raw_type == "bigint"
            && result
                .columns
                .iter()
                .all(|column| column.type_signature.arguments.is_empty()),
        "membership orders lost their declared INT/BIGINT columns"
    );
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Integer(k1), TrinoValue::Integer(v2)] => Ok((i32::try_from(*k1)?, *v2)),
            row => bail!("membership orders require non-null INT/BIGINT cells: {row:?}"),
        })
        .collect()
}

fn membership_statement(sql: &str) -> StatementSpec {
    StatementSpec {
        sql: sql.into(),
        faults: vec![(0, RESTART_AFTER_ESTABLISH_PARTICIPATION)],
        observations: vec![(
            0,
            novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(),
            Some(1),
        )],
    }
}

#[derive(Default)]
struct PreReadyReplan {
    intent: SqlIntentOwner,
}

impl Scenario for PreReadyReplan {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "backend_membership",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "membership/pre-ready-replan"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![membership_statement(REPLAN_SQL)],
            &[0],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "pre-ready replan acceptance requires exactly {REQUIRED_BACKENDS} BEs"
        );
        // The subject is a backend replaced after it admitted the attempt and
        // before the attempt became ready, so the query has to place a task on
        // that exact backend. A constant-relation query does not: a fragment
        // with no scan gets one instance, and the scheduler puts it on a
        // backend chosen from the query id. A connector scan is what fans a
        // fragment out to every live backend, which is why this fixture is a
        // real table rather than a UNION ALL of literals.
        let catalog = "pre_ready_replan";
        let warehouse = context.runtime_dir().join("pre-ready-replan-warehouse");
        fs::create_dir_all(&warehouse)
            .with_context(|| format!("create replan warehouse {}", warehouse.display()))?;
        let mut setup = {
            context.remaining("connect replan setup client")?;
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?
        };
        super::connector::create_catalog(&mut setup, catalog, &warehouse)
            .context("create replan Hadoop Iceberg catalog")?;
        setup
            .execute(format!("CREATE SCHEMA {catalog}.ns"))
            .context("create replan Iceberg namespace")?;
        setup
            .execute(format!("USE {catalog}.ns"))
            .context("select replan Iceberg catalog and namespace")?;
        setup
            .execute("CREATE TABLE probe (v BIGINT)")
            .context("create replan scan table")?;
        setup
            .execute("INSERT INTO probe VALUES (1), (2)")
            .context("seed replan scan table")?;
        drop(setup);

        let target = 0;
        let old_process_id = context.handle().backend_process_id(target)?;
        let selected = self.intent.take(0)?;
        let connection =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let pending = connection.submit(REPLAN_SQL)?;
        context.action(format!(
            "submitted exact replan client_query_id={}",
            pending.query_id
        ));
        let cancel = pending.cancel_handle();
        let (pending, observation) = selected.arm_pending(context, pending, REPLAN_SQL)?;
        let token = selected.token.clone();
        let rendezvous = match RestartAfterEstablishRendezvous::bind(&token) {
            Ok(rendezvous) => rendezvous,
            Err(primary) => {
                let cancelled = cancel
                    .delete(context.deadline())
                    .map_err(anyhow::Error::from);
                let drained = {
                    let original_deadline = context.deadline();
                    context
                        .handle()
                        .drain_query_lifecycle_statement_binding(&observation, original_deadline)
                };
                return membership_cleanup(Err(primary), membership_cleanup(cancelled, drained));
            }
        };
        context.action(format!(
            "armed token-scoped restart after BE[{target}] EstablishParticipation; old_process_id={old_process_id}"
        ));
        let actor = match MembershipActor::start(connection, pending, context.deadline()) {
            Ok(actor) => actor,
            Err(primary) => {
                let drained = {
                    let original_deadline = context.deadline();
                    context
                        .handle()
                        .drain_query_lifecycle_statement_binding(&observation, original_deadline)
                };
                return membership_cleanup(Err(primary), drained);
            }
        };
        let mut observation_drained = false;
        let phase = (|| -> Result<()> {
            let restart_ack = rendezvous.wait_for_trigger(context.deadline())?;
            // The canonical owner validates the original session/process receipt.
            // FE publishes this exact first-attempt binding before Establish runs.
            let binding = context
                .handle()
                .selected_native_observation_binding(
                    target,
                    novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                        .as_str(),
                    &token,
                    Some(1),
                )?
                .context("fired restart lacks its declared first-attempt binding")?;
            let expected_attempt = bound_replan_attempt(&binding, &token, target)?;

            let deadline = context.deadline();
            context
                .handle()
                .restart_be_until(target, deadline)
                .context("replace BE immediately after token-scoped EstablishParticipation")?;
            let replacement_process_id = context.handle().backend_process_id(target)?;
            ensure!(
                replacement_process_id != old_process_id,
                "pre-ready replacement must create a new BackendProcessId"
            );
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("clear pre-ready restart trigger")?;
            drop(restart_ack);

            let remaining = context.remaining("await re-planned query")?;
            let result = actor.result.recv_timeout(remaining).map_err(|error| {
                anyhow::anyhow!("pre-ready query did not return before deadline: {error}")
            })??;
            let rows = membership_bigint_rows(&result)?;
            ensure!(
                rows == vec![1, 2],
                "pre-ready re-planned query returned unexpected rows: {rows:?}"
            );
            let deadline = context.deadline();
            let terminal = context
                .handle()
                .await_query_lifecycle_statement_observation(&observation, true, deadline)
                .context("read terminal snapshot for re-planned statement")?;
            ensure!(
                terminal.attempt_id == 2
                    && terminal.process_namespace == expected_attempt.process_namespace
                    && terminal.local_sequence == expected_attempt.local_sequence,
                "pre-ready replacement snapshot mismatch: candidate={terminal:?}, target={expected_attempt:?}"
            );
            context
                .handle()
                .drain_query_lifecycle_statement_binding(&observation, deadline)?;
            observation_drained = true;
            context.action(format!(
            "replaced BE[{target}] after EstablishParticipation and observed successful statement attempt=2 completion with new_process_id={replacement_process_id}"
        ));
            Ok(())
        })();
        let cleared = if phase.is_err() {
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("clear original membership fault during failure cleanup")
        } else {
            Ok(())
        };
        let phase = membership_cleanup(phase, cleared);
        let primary = actor.finish_after(context, phase);
        let drained = if observation_drained {
            Ok(())
        } else {
            {
                let original_deadline = context.deadline();
                context
                    .handle()
                    .drain_query_lifecycle_statement_binding(&observation, original_deadline)
            }
        };
        membership_cleanup(primary, drained)
    }
}

fn bound_replan_attempt(
    binding: &StatementExecutionBinding,
    token: &str,
    backend_index: usize,
) -> Result<novarocks_cluster_harness::QueryLifecycleAttemptTarget> {
    binding.validate()?;
    ensure!(
        binding.statement_run_token == token
            && binding.backend_index == backend_index
            && binding.attempt_id == 1,
        "restart binding must name the exact fired token/backend and first attempt"
    );
    let local_sequence =
        u64::try_from(binding.query_id_lo).context("restart binding local sequence is negative")?;
    ensure!(local_sequence > 0, "restart binding local sequence is zero");
    Ok(novarocks_cluster_harness::QueryLifecycleAttemptTarget {
        process_namespace: u64::from_ne_bytes(binding.query_id_hi.to_ne_bytes()),
        local_sequence,
        attempt_id: 2,
    })
}

#[derive(Default)]
struct PreReadyDmlNoRecovery {
    intent: SqlIntentOwner,
}

impl Scenario for PreReadyDmlNoRecovery {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "backend_membership",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "membership/pre-ready-dml-no-recovery"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![membership_statement(DML_SQL)],
            &[0],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "pre-ready DML no-recovery acceptance requires exactly {REQUIRED_BACKENDS} BEs"
        );
        let catalog = "pre_ready_dml";
        let warehouse = context.runtime_dir().join("pre-ready-dml-warehouse");
        fs::create_dir_all(&warehouse)
            .with_context(|| format!("create DML warehouse {}", warehouse.display()))?;
        let mut setup = {
            context.remaining("connect DML setup client")?;
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?
        };
        super::connector::create_catalog(&mut setup, catalog, &warehouse)
            .context("create DML Hadoop Iceberg catalog")?;
        setup
            .execute(format!("CREATE SCHEMA {catalog}.ns"))
            .context("create DML Iceberg namespace")?;
        setup
            .execute(format!("USE {catalog}.ns"))
            .context("select DML Iceberg catalog and namespace")?;
        setup
            .execute("CREATE TABLE orders (k1 INT, v2 BIGINT)")
            .context("create DML target table")?;
        drop(setup);

        // This scenario owns a fresh FE and runs no concurrent DML between the
        // snapshots. The exact token-scoped observation below binds this one
        // statement; the publication labels independently prove no commit.
        let publication_before = publication_terminals(context, "write")?;
        let target = 0;
        let old_process_id = context.handle().backend_process_id(target)?;
        let selected = self.intent.take(0)?;
        let connection =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let pending = connection.submit(DML_SQL)?;
        context.action(format!(
            "submitted exact DML client_query_id={}",
            pending.query_id
        ));
        let cancel = pending.cancel_handle();
        let (pending, observation) = selected.arm_pending(context, pending, DML_SQL)?;
        let token = selected.token.clone();
        let rendezvous = match RestartAfterEstablishRendezvous::bind(&token) {
            Ok(rendezvous) => rendezvous,
            Err(primary) => {
                let cancelled = cancel
                    .delete(context.deadline())
                    .map_err(anyhow::Error::from);
                let drained = {
                    let original_deadline = context.deadline();
                    context
                        .handle()
                        .drain_query_lifecycle_statement_binding(&observation, original_deadline)
                };
                return membership_cleanup(Err(primary), membership_cleanup(cancelled, drained));
            }
        };
        context.action(format!(
            "armed token-scoped restart after BE[{target}] DML EstablishParticipation; old_process_id={old_process_id}"
        ));
        let actor = match MembershipActor::start(connection, pending, context.deadline()) {
            Ok(actor) => actor,
            Err(primary) => {
                let drained = {
                    let original_deadline = context.deadline();
                    context
                        .handle()
                        .drain_query_lifecycle_statement_binding(&observation, original_deadline)
                };
                return membership_cleanup(Err(primary), drained);
            }
        };
        let mut observation_drained = false;
        let phase = (|| -> Result<()> {
            let restart_ack = rendezvous.wait_for_trigger(context.deadline())?;
            let deadline = context.deadline();
            context
                .handle()
                .restart_be_until(target, deadline)
                .context("replace BE immediately after token-scoped DML EstablishParticipation")?;
            let replacement_process_id = context.handle().backend_process_id(target)?;
            ensure!(
                replacement_process_id != old_process_id,
                "pre-ready DML replacement must create a new BackendProcessId"
            );
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("clear pre-ready DML restart trigger")?;
            drop(restart_ack);
            let error = actor
                .result
                .recv_timeout(context.remaining("await no-recovery DML")?)
                .map_err(|error| {
                    anyhow::anyhow!("pre-ready DML did not return before deadline: {error}")
                })?
                .expect_err("a pre-ready external-effect execution must not auto-replan");
            let StatementFailure::Trino(_) = &error else {
                return Err(error.into());
            };
            let error_message = format!("{error:#}");
            let verified = (|| -> Result<()> {
                // KnownUncommitted is a typed publication sidecar, not a suffix of
                // the canonical public execution error (SEP-1).
                let publication_after = publication_terminals(context, "write")?;
                assert_commit_never_invoked(
                    publication_before,
                    publication_after,
                    "pre-ready DML without recovery",
                )
                .with_context(|| format!("verify failed DML publication: {error_message}"))?;

                let deadline = context.deadline();
                let terminal = context
                    .handle()
                    .await_query_lifecycle_statement_observation(&observation, false, deadline)?;
                ensure!(
                    terminal.attempt_id == 1,
                    "pre-ready DML must retain its original attempt without auto-recovery, got: {terminal:?}"
                );
                context
                    .handle()
                    .drain_query_lifecycle_statement_binding(&observation, deadline)?;
                observation_drained = true;

                let mut verify = {
                    context.remaining("connect DML verification client")?;
                    SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?
                };
                let rows = membership_order_rows(
                    &verify
                        .execute("SELECT k1, v2 FROM pre_ready_dml.ns.orders ORDER BY k1")
                        .context("read DML target after no-recovery statement")?,
                )?;
                ensure!(
                    rows.is_empty(),
                    "the rejected pre-ready DML must not publish data, got {rows:?}"
                );
                verify
                    .execute("INSERT INTO pre_ready_dml.ns.orders VALUES (1, 10), (2, 20)")
                    .context("retry the entire DML statement after KnownUncommitted")?;
                let rows = membership_order_rows(
                    &verify
                        .execute("SELECT k1, v2 FROM pre_ready_dml.ns.orders ORDER BY k1")
                        .context("read DML target after client retry")?,
                )?;
                ensure!(
                    rows == vec![(1, 10), (2, 20)],
                    "the client statement retry must publish exactly once, got {rows:?}"
                );
                context.action(format!(
            "replaced BE[{target}] after DML EstablishParticipation; the first attempt returned KnownUncommitted without published data, then an explicit client statement retry published once; new_process_id={replacement_process_id}"
        ));
                Ok(())
            })();
            match verified {
                Ok(()) => Ok(()),
                Err(observed) => Err(anyhow::Error::new(error).context(format!(
                    "original no-recovery DML assertion failed: {observed:#}"
                ))),
            }
        })();
        let cleared = if phase.is_err() {
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("clear original membership fault during failure cleanup")
        } else {
            Ok(())
        };
        let phase = membership_cleanup(phase, cleared);
        let primary = actor.finish_after(context, phase);
        let drained = if observation_drained {
            Ok(())
        } else {
            {
                let original_deadline = context.deadline();
                context
                    .handle()
                    .drain_query_lifecycle_statement_binding(&observation, original_deadline)
            }
        };
        membership_cleanup(primary, drained)
    }
}

struct MembershipActorCleanup {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl std::fmt::Debug for MembershipActorCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for MembershipActorCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original SQL operation failed: {:#}; actual actor cleanup failed: {:#}",
            self.primary, self.cleanup
        )
    }
}
impl std::error::Error for MembershipActorCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
struct MembershipActorPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Debug for MembershipActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for MembershipActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.0;
        f.write_str("original SQL actor panicked; payload retained")
    }
}
impl std::error::Error for MembershipActorPanic {}

#[cfg(test)]
mod binding_tests {
    use super::*;

    #[test]
    fn replan_binding_uses_the_exact_fired_token_backend_and_signed_namespace() {
        let binding = StatementExecutionBinding {
            schema_version: 1,
            target_session_id: "original-session".into(),
            statement_run_token: "fired".into(),
            backend_index: 1,
            backend_process_id: novarocks_types::BackendProcessId::new_v7().to_string(),
            query_id_hi: -11,
            query_id_lo: 12,
            attempt_id: 1,
        };
        let target = bound_replan_attempt(&binding, "fired", 1).expect("exact binding");
        assert_eq!(
            target.process_namespace,
            u64::from_ne_bytes((-11_i64).to_ne_bytes())
        );
        assert_eq!(target.local_sequence, 12);
        assert_eq!(target.attempt_id, 2);
        assert!(bound_replan_attempt(&binding, "fired", 0).is_err());
        assert!(bound_replan_attempt(&binding, "unrelated", 1).is_err());
        let mut later = binding.clone();
        later.attempt_id = 2;
        assert!(bound_replan_attempt(&later, "fired", 1).is_err());
    }

    #[test]
    fn membership_plans_reserve_the_original_fault_and_only_an_ordinary_successor() {
        let binary = std::env::current_exe().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        for sql in [REPLAN_SQL, DML_SQL] {
            let owner = SqlIntentOwner::default();
            let plan = owner
                .freeze(vec![membership_statement(sql)], &[0], 3, &binary, deadline)
                .unwrap();
            plan.validate_selected_intents(3).unwrap();
            let selected = owner.take(0).unwrap();
            assert_eq!(selected.sql, sql);
            assert_eq!(
                selected.faults,
                vec![(0, RESTART_AFTER_ESTABLISH_PARTICIPATION)]
            );
            assert_eq!(selected.deadline, deadline);
            assert!(owner.take(0).is_err());
        }
    }
}
