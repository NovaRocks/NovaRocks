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

//! Native Exchange acceptance uses the real Unit path and the original owners.
//! Position metrics include granted positions not yet delivered; they are not
//! a payload-byte census. A missing or contended snapshot never means zero.

use super::connector::{
    await_resource_convergence, connector_launch_config, create_catalog, create_warehouse,
    require_three_backends, resource_baseline,
};
use super::native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec};
use crate::actors::sql::{self, SqlSession, StatementFailure, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::native_root_reply_fault::{
    ExchangeFaultAction, ExchangeFaultCandidate, ExchangeFaultObservation, ExchangeFaultSelector,
    RootReplyFaultBounds, RootReplyFaultControl,
};
use novarocks_cluster_harness::{
    CrossProcessNativeFaultProxyConfig, CrossProcessRootReplyFaultConfig, LaunchProfile,
    QueryLifecycleStatementObservation, TestClusterOwner,
};
use novarocks_test_trino_client::{CancelHandle, TrinoError};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const CATALOG: &str = "exchange_flow";
const DATABASE: &str = "native_flow";
const TABLE: &str = "exchange_flow.native_flow.rows";
const FILES: i64 = 6;
const ROWS_PER_FILE: i64 = 4096;
const ROWS: i64 = FILES * ROWS_PER_FILE;
const TRANSPORT_MILLIS: u64 = 1000;
const HOLD: Duration = Duration::from_millis(2500);
const POLL: Duration = Duration::from_millis(20);

#[derive(Clone, Debug, Serialize)]
pub(super) struct FlowSnapshot {
    pub(super) backend_index: usize,
    pub(super) in_use: u64,
    pub(super) decoding: u64,
    pub(super) pending_demands: u64,
    pub(super) live_instances: u64,
    pub(super) max_instance_in_use: u64,
    pub(super) floating_in_use: u64,
    pub(super) closed_instances: u64,
    pub(super) instance_floor: u64,
    pub(super) instance_max: u64,
    pub(super) floating: u64,
    pub(super) stream_window: u64,
    pub(super) transport_timeout_ms: u64,
    pub(super) replays: u64,
    pub(super) calls: u64,
    pub(super) bytes: u64,
    pub(super) waiting_streams: u64,
}

impl FlowSnapshot {
    fn idle(&self) -> bool {
        self.in_use == 0
            && self.decoding == 0
            && self.pending_demands == 0
            && self.floating_in_use == 0
            && self.calls == 0
            && self.bytes == 0
            && self.waiting_streams == 0
    }
}

fn metric(text: &str, name: &str, label: Option<(&str, &str)>) -> Result<u64> {
    let mut found = None;
    for line in text.lines().filter(|line| !line.starts_with('#')) {
        let mut parts = line.split_whitespace();
        let Some(identity) = parts.next() else {
            continue;
        };
        let (family, labels) = identity
            .split_once('{')
            .map_or((identity, None), |(family, labels)| {
                (family, Some(labels.trim_end_matches('}')))
            });
        if family != name {
            continue;
        }
        let selected = match (label, labels) {
            (None, None) => true,
            (Some((key, value)), Some(labels)) => labels
                .split(',')
                .any(|entry| entry == format!("{key}=\"{value}\"")),
            _ => false,
        };
        if !selected {
            continue;
        }
        ensure!(
            found.is_none(),
            "duplicate Exchange metric {name} {label:?}"
        );
        let value = parts
            .next()
            .with_context(|| format!("Exchange metric {name} has no value"))?
            .parse::<f64>()?;
        ensure!(
            value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value < u64::MAX as f64,
            "invalid Exchange metric {name}={value}"
        );
        found = Some(value as u64);
    }
    found.with_context(|| format!("missing Exchange metric {name} {label:?}"))
}

/// None preserves the original author's explicitly unavailable snapshot.
pub(super) fn flow_snapshots(context: &mut ScenarioContext) -> Result<Option<Vec<FlowSnapshot>>> {
    let mut snapshots = Vec::new();
    for index in 0..context.handle().be_count() {
        let text = context.handle().backend_exchange_flow_metrics(index)?;
        match metric(
            &text,
            "novarocks_backend_exchange_flow_snapshot_available",
            None,
        )? {
            0 => return Ok(None),
            1 => {}
            other => bail!("invalid Exchange snapshot availability: {other}"),
        }
        let position = |state| {
            metric(
                &text,
                "novarocks_backend_exchange_flow_positions",
                Some(("state", state)),
            )
        };
        let limit = |name| {
            metric(
                &text,
                "novarocks_backend_exchange_flow_limits",
                Some(("limit", name)),
            )
        };
        let activity = |name| {
            metric(
                &text,
                "novarocks_backend_exchange_send_activity",
                Some(("resource", name)),
            )
        };
        let snapshot = FlowSnapshot {
            backend_index: index,
            in_use: position("in_use")?,
            decoding: position("decoding")?,
            pending_demands: position("pending_demands")?,
            live_instances: position("live_instances")?,
            max_instance_in_use: position("max_instance_in_use")?,
            floating_in_use: position("floating_in_use")?,
            closed_instances: position("closed_instances")?,
            instance_floor: limit("instance_floor")?,
            instance_max: limit("instance_max")?,
            floating: limit("floating")?,
            stream_window: limit("stream_window")?,
            transport_timeout_ms: limit("transport_timeout_ms")?,
            replays: metric(&text, "novarocks_backend_exchange_unit_replays_total", None)?,
            calls: activity("calls")?,
            bytes: activity("bytes")?,
            waiting_streams: activity("waiting_streams")?,
        };
        ensure!(
            snapshot.instance_floor > 0
                && snapshot.instance_floor <= snapshot.instance_max
                && snapshot.stream_window > 0
                && snapshot.transport_timeout_ms > 0,
            "invalid actual Exchange limits: {snapshot:?}"
        );
        // Each inequality belongs to this actual owner's one snapshot. Do not
        // assemble a total capacity claim from different processes or times.
        ensure!(
            snapshot.max_instance_in_use <= snapshot.instance_max
                && snapshot.floating_in_use <= snapshot.floating,
            "actual Exchange positions exceed their frozen limits: {snapshot:?}"
        );
        snapshots.push(snapshot);
    }
    Ok(Some(snapshots))
}

pub(super) fn await_flow_idle(context: &mut ScenarioContext) -> Result<Vec<FlowSnapshot>> {
    loop {
        context.remaining("await actual Exchange positions and call guards exiting")?;
        if let Some(snapshots) = flow_snapshots(context)?
            && snapshots.iter().all(FlowSnapshot::idle)
        {
            return Ok(snapshots);
        }
        thread::park_timeout(POLL);
    }
}

fn retain(context: &ScenarioContext, name: &str, value: &impl Serialize) -> Result<()> {
    std::fs::write(
        context.scenario_root().join(name),
        serde_json::to_vec_pretty(value)?,
    )?;
    Ok(())
}

fn combine<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(ExchangeFailure { primary, cleanup }.into()),
    }
}
struct ExchangeFailure {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl std::fmt::Display for ExchangeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}; original Exchange scenario cleanup: {}",
            self.primary, self.cleanup
        )
    }
}
impl std::fmt::Debug for ExchangeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for ExchangeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}

/// The scenario owns and joins this original query job on every path.
struct QueryJob {
    client_query_id: String,
    cancel: CancelHandle,
    selected: SelectedStatement,
    observation: QueryLifecycleStatementObservation,
    result: Option<mpsc::Receiver<std::result::Result<StatementResult, StatementFailure>>>,
    actor: Option<JoinHandle<Result<()>>>,
    deadline: Instant,
    settled: bool,
}

impl QueryJob {
    fn start(
        context: &mut ScenarioContext,
        selected: SelectedStatement,
        settings: Vec<String>,
    ) -> Result<Self> {
        let deadline = context.deadline();
        let mut session = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
        for setting in settings {
            session.execute(setting)?;
        }
        // This exact query is registered before any GET. Save its cancellation
        // owner immediately; dropping a Pending is not server cancellation.
        let pending = session.submit(&selected.sql)?;
        let cancel = pending.cancel_handle();
        let client_query_id = pending.query_id.clone();
        let (pending, observation) = selected.arm_pending(context, pending, &selected.sql)?;
        let (result_tx, result) = mpsc::sync_channel(1);
        let actor = match thread::Builder::new()
            .name("exchange-flow-selected-sql".into())
            .spawn(move || {
                let outcome = session.run_pending(pending);
                match result_tx.send(outcome) {
                    Ok(()) => Ok(()),
                    Err(mpsc::SendError(Err(actual))) => Err(actual.into()),
                    Err(mpsc::SendError(Ok(_))) => {
                        bail!("original Exchange SQL result owner closed before delivery")
                    }
                }
            }) {
            Ok(actor) => actor,
            Err(primary) => {
                let cancelled = cancel.delete(deadline).map_err(Into::into);
                let drained = context
                    .handle()
                    .drain_query_lifecycle_statement_binding(&observation, deadline);
                return combine(Err(primary.into()), combine(cancelled, drained));
            }
        };
        Ok(Self {
            client_query_id,
            cancel,
            selected,
            observation,
            result: Some(result),
            actor: Some(actor),
            deadline,
            settled: false,
        })
    }

    fn cancel(&self) -> Result<()> {
        self.cancel.delete(self.deadline).map_err(Into::into)
    }

    fn finish(&mut self, context: &mut ScenarioContext, expected: Outcome) -> Result<Value> {
        let receiver = self
            .result
            .take()
            .context("Exchange SQL outcome already consumed")?;
        let mut sql_terminal = false;
        let mut consumed = false;
        let primary = (|| -> Result<Value> {
            let actual = receiver
                .recv_timeout(context.remaining("receive original Exchange SQL outcome")?)?;
            consumed = true;
            sql_terminal = matches!(&actual, Ok(_) | Err(StatementFailure::Trino(_)));
            match (expected, actual) {
                (Outcome::Rows, Ok(result)) => {
                    let mut rows = sql::integer_column(&result)?;
                    rows.sort_unstable();
                    ensure!(
                        rows.len() == ROWS as usize,
                        "Exchange result row cardinality changed"
                    );
                    ensure!(
                        rows.iter().copied().eq(1..=ROWS),
                        "Exchange result contains a missing, repeated or wrong row"
                    );
                    Ok(
                        json!({"outcome":"rows","client_query_id":self.client_query_id,"rows":ROWS,"independent_oracle":"every integer 1..=24576 exactly once"}),
                    )
                }
                (Outcome::Deadline, Err(failure)) => {
                    let error = expected_failure(failure, Outcome::Deadline)?;
                    Ok(
                        json!({"outcome":"query_deadline","client_query_id":self.client_query_id,
                        "trino_error_code":error.error_code,"trino_error_name":error.error_name,
                        "trino_error_type":error.error_type,"message":error.message}),
                    )
                }
                (Outcome::Cancelled, Err(failure)) => {
                    let error = expected_failure(failure, Outcome::Cancelled)?;
                    // Direct DELETE owns a typed protocol cancellation. Its
                    // original public message has no promised NovaCode prefix;
                    // the same-query Native observation proves owner exit.
                    Ok(
                        json!({"outcome":"query_cancelled","client_query_id":self.client_query_id,
                        "trino_error_code":error.error_code,"trino_error_name":error.error_name,
                        "trino_error_type":error.error_type,"message":error.message}),
                    )
                }
                (_, Err(error)) => Err(error.into()),
                (_, Ok(_)) => {
                    bail!("selected Exchange SQL succeeded instead of its declared terminal")
                }
            }
        })();
        let mut cleanup = Ok(());
        if primary.is_err() && !sql_terminal {
            cleanup = self.cancel();
            if cleanup.is_err() {
                cleanup = combine(cleanup, context.shutdown());
            }
        }
        if let Some(actor) = self.actor.take() {
            cleanup = combine(cleanup, join_actor(actor));
        }
        // Keep a late original result even when the original wait failed.
        // It remains cleanup evidence after that earlier primary failure.
        if !consumed && let Ok(Err(actual)) = receiver.try_recv() {
            cleanup = combine(cleanup, Err(actual.into()));
        }
        let terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(
                &self.observation,
                matches!(expected, Outcome::Rows) && primary.is_ok(),
                self.deadline,
            );
        cleanup = combine(cleanup, terminal.map(|_| ()));
        cleanup = combine(
            cleanup,
            context
                .handle()
                .drain_query_lifecycle_statement_binding(&self.observation, self.deadline),
        );
        self.settled = true;
        combine(primary, cleanup)
    }

    fn settle(&mut self, context: &mut ScenarioContext) -> Result<()> {
        if self.settled {
            return Ok(());
        }
        let mut cleanup = Ok(());
        // A finished local worker may still have an unknown HTTP/transport
        // outcome. The original handle cancels only its known continuation;
        // completed typed results have already cleared that URI.
        cleanup = combine(cleanup, self.cancel());
        if cleanup.is_err() {
            cleanup = combine(cleanup, context.shutdown());
        }
        if let Some(actor) = self.actor.take() {
            cleanup = combine(cleanup, join_actor(actor));
        }
        if let Some(receiver) = self.result.take()
            && let Ok(Err(actual)) = receiver.try_recv()
        {
            cleanup = combine(cleanup, Err(actual.into()));
        }
        cleanup = combine(
            cleanup,
            context
                .handle()
                .drain_query_lifecycle_statement_binding(&self.observation, self.deadline),
        );
        self.settled = true;
        cleanup
    }
}

impl Drop for QueryJob {
    fn drop(&mut self) {
        if let Some(actor) = self.actor.take() {
            if !self.settled {
                let _ = self.cancel();
            }
            let _ = join_actor(actor);
        }
    }
}

fn join_actor(actor: JoinHandle<Result<()>>) -> Result<()> {
    actor
        .join()
        .map_err(|payload| anyhow::Error::new(ExchangePanic(std::sync::Mutex::new(payload))))?
}

struct ExchangePanic(std::sync::Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Display for ExchangePanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.0;
        f.write_str("original Exchange SQL actor panicked; payload retained")
    }
}
impl std::fmt::Debug for ExchangePanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for ExchangePanic {}

fn expected_failure(failure: StatementFailure, expected: Outcome) -> Result<TrinoError> {
    match failure {
        StatementFailure::Trino(error)
            if match expected {
                Outcome::Deadline => {
                    error.error_code == 131075
                        && error.error_name == "EXCEEDED_TIME_LIMIT"
                        && error.error_type == "INSUFFICIENT_RESOURCES"
                        && error
                            .message
                            .starts_with("[query.execution.deadline_exceeded] ")
                }
                Outcome::Cancelled => {
                    error.error_code == 3
                        && error.error_name == "USER_CANCELED"
                        && error.error_type == "USER_ERROR"
                }
                Outcome::Rows => false,
            } =>
        {
            Ok(error)
        }
        actual => Err(anyhow::Error::new(actual)
            .context("selected Exchange SQL did not return its original declared terminal")),
    }
}

#[derive(Clone, Copy)]
enum Outcome {
    Rows,
    Deadline,
    Cancelled,
}

type FileGeometryRow = (i32, String, String, i64, i64);

fn file_geometry(result: &StatementResult) -> Result<Vec<FileGeometryRow>> {
    ensure!(
        result.is_result_set() && result.columns.len() == 5,
        "Exchange file geometry must be the original five-column result set"
    );
    let names = result
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    ensure!(
        names
            == [
                "content",
                "file_path",
                "file_format",
                "record_count",
                "file_size_in_bytes"
            ],
        "Exchange file geometry declaration changed: {names:?}"
    );
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [
                TrinoValue::Integer(content),
                TrinoValue::Text(path),
                TrinoValue::Text(format),
                TrinoValue::Integer(records),
                TrinoValue::Integer(bytes),
            ] => Ok((
                i32::try_from(*content)?,
                path.clone(),
                format.clone(),
                *records,
                *bytes,
            )),
            _ => bail!("Exchange file geometry has NULL, wrong types or width: {row:?}"),
        })
        .collect()
}
fn plan_text(result: &StatementResult) -> Result<String> {
    ensure!(
        result.is_result_set() && result.columns.len() == 1,
        "Exchange EXPLAIN must retain its one text column"
    );
    ensure!(
        matches!(
            result.columns[0].type_signature.raw_type.as_str(),
            "varchar" | "char"
        ),
        "Exchange EXPLAIN column is not text"
    );
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Text(text)] => Ok(text.clone()),
            _ => bail!("Exchange EXPLAIN changed text/NULL/cardinality: {row:?}"),
        })
        .collect::<Result<Vec<_>>>()
        .map(|lines| lines.join("\n"))
}

fn seed(context: &mut ScenarioContext) -> Result<()> {
    let warehouse = create_warehouse(context, "exchange-flow")?;
    let mut control =
        SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
    create_catalog(&mut control, CATALOG, &warehouse)?;
    control.execute(format!("CREATE DATABASE {CATALOG}.{DATABASE}"))?;
    control.execute(format!("CREATE TABLE {TABLE} (k BIGINT NOT NULL)"))?;
    // Independent committed files make the scan eligible on the live BE
    // roster. Actual selected participants are checked, never guessed.
    for file in 0..FILES {
        let first = file * ROWS_PER_FILE + 1;
        let last = first + ROWS_PER_FILE - 1;
        control.execute(format!(
            "INSERT INTO {TABLE} SELECT generate_series FROM TABLE(generate_series({first},{last}))"
        ))?;
    }
    let files = file_geometry(&control.execute(format!(
        "SELECT content,file_path,file_format,record_count,file_size_in_bytes FROM {TABLE}$files ORDER BY file_path"
    )).context("read original Exchange fixture file geometry")?)?;
    retain(context, "exchange-seed-file-geometry.json", &files)?;
    // All six original files share Iceberg's minimum split weight (0.05 of
    // its 128 MiB target). The initial 256-split pump and least-total-weight
    // assignment therefore give each of the three original scan Tasks two
    // files. A Parquet page belongs to one split and never joins two files.
    ensure!(
        files.len() == FILES as usize
            && files
                .iter()
                .all(|(content, _, format, rows, bytes)| *content == 0
                    && format == "PARQUET"
                    && *rows == ROWS_PER_FILE
                    && *bytes > 0
                    && *bytes <= 128 * 1024 * 1024 / 20),
        "Exchange fixture requires six original 4096-row, minimum-weight Parquet files: {files:?}"
    );
    Ok(())
}

fn gather_sql() -> String {
    // A plain scan can deliver several root streams without an Exchange.
    // The original global ordering property requires a real singleton input.
    format!("SELECT k FROM {TABLE} ORDER BY k")
}

fn join_sql() -> String {
    format!("SELECT p.k FROM {TABLE} p JOIN {TABLE} b ON p.k=b.k")
}

fn settings(partitioned: bool, seconds: u64) -> Vec<String> {
    vec![
        format!("SET query_timeout = {seconds}"),
        format!(
            "SET cbo_broadcast_node_mem_budget_bytes = {}",
            if partitioned { 0 } else { 10_737_418_240_u64 }
        ),
        // Exercise actual ordered join inputs, independently of optional RF
        // admission. The affected V3 RF suites cover that separate contract.
        "SET enable_global_runtime_filter = false".into(),
    ]
}

#[derive(Clone, Copy)]
enum Case {
    Bounded,
    CrossQuery,
    Replay,
    Rebuild,
    LongWait,
    Control,
}

struct ExchangeFlow {
    case: Case,
    intent: SqlIntentOwner,
}

pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    [
        Case::Bounded,
        Case::CrossQuery,
        Case::Replay,
        Case::Rebuild,
        Case::LongWait,
        Case::Control,
    ]
    .into_iter()
    .map(|case| {
        Box::new(ExchangeFlow {
            case,
            intent: SqlIntentOwner::default(),
        }) as Box<dyn Scenario>
    })
    .collect()
}

impl ExchangeFlow {
    fn sqls(&self) -> Vec<String> {
        match self.case {
            Case::Bounded => vec![
                gather_sql(),
                join_sql(),
                join_sql(),
                gather_sql(),
                gather_sql(),
                gather_sql(),
            ],
            Case::CrossQuery | Case::Control => vec![join_sql(), join_sql()],
            Case::LongWait => vec![join_sql(), join_sql()],
            Case::Replay | Case::Rebuild => vec![gather_sql()],
        }
    }
}

impl Scenario for ExchangeFlow {
    fn name(&self) -> &'static str {
        match self.case {
            Case::Bounded => "exchange/bounded-backpressure",
            Case::CrossQuery => "exchange/cross-query-probe-build",
            Case::Replay => "exchange/unit-replay",
            Case::Rebuild => "exchange/grant-subscription-rebuild",
            Case::LongWait => "exchange/long-wait-without-idle-failure",
            Case::Control => "exchange/control-liveness",
        }
    }

    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        super::declarations::for_module("exchange_flow", self.name(), false, false)
    }

    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        ensure!(
            profile == LaunchProfile::FaultScenario,
            "Exchange acceptance requires the authenticated fault-scenario profile"
        );
        Ok(())
    }

    fn launch_config(&self, _: &Path) -> Result<ScenarioLaunchConfig> {
        let mut config = connector_launch_config();
        let original = config
            .config_overlay
            .be
            .take()
            .context("connector runtime overlay is absent")?;
        ensure!(
            original.contains("[runtime]\n"),
            "connector overlay has no runtime table"
        );
        config.config_overlay.be = Some(original.replacen("[runtime]\n", &format!(
            "[runtime]\nexchange_instance_floor_slots = 1\nexchange_instance_max_slots = 2\nexchange_floating_slots = 1\nexchange_stream_window = 2\nexchange_transport_timeout_ms = {TRANSPORT_MILLIS}\nexchange_io_max_inflight_bytes = 262144\nexchange_max_transmit_batched_bytes = 8192\n"
        ), 1));
        config.native_root_reply_fault = Some(CrossProcessRootReplyFaultConfig {
            backend_indices: (0..3).collect(),
            bounds: RootReplyFaultBounds::default(),
        });
        config.native_fault_proxies = CrossProcessNativeFaultProxyConfig {
            // Keep the existing Root actor's separate Control TCP budget.
            backend_retained_byte_limits: (0..3).map(|index| (index, 1024 * 1024)).collect(),
        };
        Ok(config)
    }

    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        ensure!(
            cluster_size == 3,
            "Exchange acceptance requires real native 1FE+3BE"
        );
        let specs = self
            .sqls()
            .into_iter()
            .map(|sql| StatementSpec {
                sql,
                faults: vec![],
                observations: (0..cluster_size)
                    .flat_map(|index| {
                        [
                            (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), Some(1)),
                            (index, novarocks_failpoint::observation_marker::ObservationAction::ExchangeSnapshot.as_str(), Some(1)),
                        ]
                    })
                    .collect(),
            })
            .collect();
        Ok(Some(self.intent.freeze(
            specs,
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        context.recheck_live_process_launch_identities()?;
        // Bind the authenticated live BE pairs before ordinary seed SQL can
        // open long-lived grant subscriptions. No fault is captured here.
        let fault = context.handle().native_exchange_fault(0)?;
        let baseline = resource_baseline(context)?;
        seed(context)?;
        await_resource_convergence(context, &baseline, "ordinary private Exchange seed SQL")?;
        let idle = await_flow_idle(context)?;
        ensure!(
            idle.len() == context.handle().be_count(),
            "Exchange backend roster changed"
        );
        for snapshot in &idle {
            ensure!(
                snapshot.instance_floor == 1
                    && snapshot.instance_max == 2
                    && snapshot.floating == 1
                    && snapshot.stream_window == 2
                    && snapshot.transport_timeout_ms == TRANSPORT_MILLIS,
                "actual Exchange test configuration changed: {snapshot:?}"
            );
        }
        retain(context, "exchange-initial-flow.json", &idle)?;
        let mut jobs = Vec::new();
        let mut evidence = json!({"schema_version":1,"scenario":self.name(),"rows":ROWS,
            "oracle":"full unique integer domain, independent of received rows", "phases":[]});
        let primary = match self.case {
            Case::Bounded => self.bounded(context, &fault, &mut jobs, &mut evidence),
            Case::CrossQuery => self.cross_query(context, &fault, &mut jobs, &mut evidence),
            Case::Replay => self.replay(context, &fault, &mut jobs, &mut evidence),
            Case::Rebuild => self.rebuild(context, &fault, &mut jobs, &mut evidence),
            Case::LongWait => self.long_wait(context, &fault, &mut jobs, &mut evidence),
            Case::Control => self.control(context, &fault, &mut jobs, &mut evidence),
        };
        // Release the original fault owner before joining failure cleanup.
        // Every job is retained until its original query thread actually exits.
        fault.release_exchange();
        let mut cleanup = Ok(());
        for job in &mut jobs {
            cleanup = combine(cleanup, job.settle(context));
        }
        let result = combine(primary, cleanup);
        let result = combine(
            result,
            await_fault_exit(context, &fault).map(|observation| {
                evidence["final_fault"] = json!(observation);
            }),
        );
        let result = combine(
            result,
            await_flow_idle(context).map(|snapshots| {
                evidence["final_flow"] = json!(snapshots);
            }),
        );
        let result = combine(
            result,
            await_resource_convergence(context, &baseline, self.name()),
        );
        let result = combine(
            result,
            retain(context, "exchange-flow-evidence.json", &evidence),
        );
        // Actual role exit remains the existing managed process owner's job;
        // the runner records and joins it on success and failure alike.
        result
    }
}

#[derive(Serialize)]
struct ExplainedPlan {
    physical_tree: String,
    contract: String,
}

impl ExplainedPlan {
    fn new(physical_tree: String, contract: String) -> Result<Self> {
        ensure!(
            physical_tree
                .lines()
                .any(|line| line.starts_with("PLAN FRAGMENT ")),
            "EXPLAIN VERBOSE did not return the original physical tree"
        );
        ensure!(
            contract.starts_with("PHYSICAL PLAN version=") && !plan_nodes(&contract)?.is_empty(),
            "EXPLAIN CONTRACT did not return original frozen node identities"
        );
        Ok(Self {
            physical_tree,
            contract,
        })
    }

    fn gather_receiver(&self) -> Result<ExplainedReceiver> {
        ensure!(
            self.physical_tree.lines().any(|line| {
                let label = line.trim_start().split_once(':').map(|(_, label)| label);
                label.is_some_and(|label| label.starts_with("SORT BY "))
            }) && self.physical_tree.lines().any(|line| {
                line.trim_start()
                    .split_once(':')
                    .is_some_and(|(_, label)| label == "GATHER" || label.starts_with("GATHER "))
            }),
            "gather physical tree has no actual global Sort and Gather: {}",
            self.physical_tree
        );
        receiver_contract(&self.contract, singleton_receiver_node(&self.contract)?)
    }

    fn build_receiver(&self, partitioned: bool) -> Result<ExplainedReceiver> {
        let label = if partitioned {
            "HASH JOIN (PARTITIONED,"
        } else {
            "HASH JOIN (BROADCAST,"
        };
        ensure!(
            self.physical_tree.contains(label) && self.contract.contains(label),
            "join distribution differs from the required physical tree/Contract: {}",
            self.physical_tree
        );
        receiver_contract(&self.contract, build_receiver_node(&self.contract)?)
    }
}

#[derive(Debug, Serialize)]
struct ExplainedReceiver {
    destination_fragment: u32,
    node: i32,
    edge: u32,
    source_fragment: u32,
}

fn receiver_contract(plan: &str, node: i32) -> Result<ExplainedReceiver> {
    let nodes = plan_nodes(plan)?;
    // The transport selector accepts a node within an actual Task. It cannot
    // reconstruct the scheduler's StageId from a display fragment ordinal.
    // This fixed corpus must therefore have one unambiguous receiver with
    // this original node ID before using that selector across its Tasks.
    let receivers: Vec<_> = nodes
        .iter()
        .filter(|actual| actual.node == node && actual.header.contains("(EXCHANGE ID:"))
        .collect();
    ensure!(
        receivers.len() == 1,
        "original receiver node is ambiguous across frozen fragments"
    );
    let receiver = receivers[0];
    let edge = number_after::<u32>(receiver.header, "(EXCHANGE ID: ")?;
    let edges: Vec<_> = plan
        .lines()
        .filter(|line| line.trim_start().starts_with(&format!("edge{edge} kind=")))
        .collect();
    ensure!(
        edges.len() == 1,
        "selected receiver has no unique original edge"
    );
    let (source, destination) = edges[0]
        .split_once(" destination={")
        .context("edge has no frozen destination")?;
    let destination_fragment = number_after(destination, "fragment=f")?;
    ensure!(
        destination_fragment == receiver.fragment
            && number_after::<i32>(destination, "node=n")? == node,
        "receiver node and frozen edge destination disagree"
    );
    Ok(ExplainedReceiver {
        destination_fragment,
        node,
        edge,
        source_fragment: number_after(source, "source={fragment=f")?,
    })
}

fn explain(context: &mut ScenarioContext, sql: &str, partitioned: bool) -> Result<ExplainedPlan> {
    let mut control =
        SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
    for setting in settings(partitioned, 120) {
        control.execute(setting)?;
    }
    let physical_tree = plan_text(&control.execute(format!("EXPLAIN VERBOSE {sql}"))?)?;
    let contract = plan_text(&control.execute(format!("EXPLAIN CONTRACT {sql}"))?)?;
    // Retain both original replies even if the subsequent format/identity
    // checks reject the fixture. No display ordinal is used as a wire ID.
    let input = format!("{partitioned}:{sql}");
    let digest = format!("{:x}", Sha256::digest(input.as_bytes()));
    retain(
        context,
        &format!("exchange-explain-{digest}.json"),
        &json!({"sql":sql,"settings":settings(partitioned,120),
            "physical_tree":physical_tree,"contract":contract}),
    )?;
    ExplainedPlan::new(physical_tree, contract)
}

struct PlanNode<'a> {
    header: &'a str,
    line: usize,
    fragment: u32,
    node: i32,
    depth: usize,
}

fn plan_nodes(plan: &str) -> Result<Vec<PlanNode<'_>>> {
    plan.lines()
        .enumerate()
        .filter(|(_, header)| header.contains("[fragment="))
        .map(|(line, header)| {
            Ok(PlanNode {
                header,
                line,
                fragment: number_after(header, "[fragment=")?,
                node: number_after(header, "node=")?,
                depth: number_after(header, "depth=")?,
            })
        })
        .collect()
}

fn singleton_receiver_node(plan: &str) -> Result<i32> {
    let destinations = plan
        .lines()
        .filter_map(|line| line.split_once(" destination={").map(|(_, value)| value))
        .filter(|destination| {
            destination
                .split_once("distribution=")
                .is_some_and(|(_, value)| value.starts_with("singleton,"))
        })
        .map(|destination| {
            Ok((
                number_after::<u32>(destination, "fragment=f")?,
                number_after::<i32>(destination, "node=n")?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        destinations.len() == 1,
        "gather case requires one exact singleton Exchange edge: {plan}"
    );
    let (fragment, node) = destinations[0];
    let nodes = plan_nodes(plan)?;
    let receivers: Vec<_> = nodes
        .iter()
        .enumerate()
        .filter(|(_, actual)| {
            actual.fragment == fragment
                && actual.node == node
                && actual.header.contains("EXCHANGE (EXCHANGE ID:")
        })
        .collect();
    ensure!(
        receivers.len() == 1,
        "singleton edge has no exact receiver node"
    );
    let (receiver_index, receiver) = receivers[0];
    ensure!(
        nodes[..receiver_index]
            .iter()
            .enumerate()
            .any(|(index, ancestor)| {
                ancestor.fragment == fragment
                    && ancestor.depth < receiver.depth
                    && ancestor.header.trim_start().starts_with("SORT ")
                    && nodes[index + 1..receiver_index].iter().all(|between| {
                        between.fragment == fragment && between.depth > ancestor.depth
                    })
            }),
        "singleton receiver is not the actual global Sort input"
    );
    Ok(node)
}

fn explain_gather(context: &mut ScenarioContext, evidence_file: &str) -> Result<ExplainedReceiver> {
    let sql = gather_sql();
    let plan = explain(context, &sql, false)?;
    let receiver = plan.gather_receiver()?;
    retain(
        context,
        evidence_file,
        &json!({"sql":sql,"plan":plan,"receiver":receiver}),
    )?;
    Ok(receiver)
}

fn number_after<T: std::str::FromStr>(text: &str, prefix: &str) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    let value = text
        .split_once(prefix)
        .with_context(|| format!("missing plan field {prefix}"))?
        .1;
    let value = value
        .split(|ch: char| !ch.is_ascii_digit())
        .next()
        .unwrap_or("");
    value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid plan field {prefix}: {error}"))
}

/// EXPLAIN is an independent shape oracle; the actor later binds the actual
/// Task/query/BE identities. A node number alone never supplies Task authority.
fn build_receiver_node(plan: &str) -> Result<i32> {
    let lines: Vec<_> = plan.lines().collect();
    let nodes = plan_nodes(plan)?;
    let joins: Vec<_> = nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| node.header.contains("HASH JOIN ("))
        .collect();
    ensure!(
        joins.len() == 1,
        "expected exactly one actual hash join: {plan}"
    );
    let (join_index, join) = joins[0];
    let next = nodes
        .get(join_index + 1)
        .context("hash join has no actual inputs")?;
    let detail = lines[join.line + 1..next.line].join("\n");
    let ordinal = if detail.contains("build-side=right,") {
        1
    } else {
        ensure!(
            detail.contains("build-side=left,"),
            "hash join omitted its build side"
        );
        0
    };
    let children: Vec<_> = nodes[join_index + 1..]
        .iter()
        .enumerate()
        .take_while(|(_, node)| node.fragment == join.fragment && node.depth > join.depth)
        .filter(|(_, node)| node.depth == join.depth + 1)
        .collect();
    ensure!(children.len() == 2, "hash join has no exact two-input tree");
    let branch_index = join_index + 1 + children[ordinal].0;
    let branch = &nodes[branch_index];
    let exchanges: Vec<_> = nodes[branch_index..]
        .iter()
        .enumerate()
        .take_while(|(offset, node)| {
            *offset == 0 || (node.fragment == branch.fragment && node.depth > branch.depth)
        })
        .filter(|(_, node)| node.header.contains("(EXCHANGE ID:"))
        .map(|(_, node)| node.node)
        .collect();
    ensure!(
        exchanges.len() == 1,
        "build input is not one actual remote Exchange: {plan}"
    );
    Ok(exchanges[0])
}

fn capture(
    context: &mut ScenarioContext,
    fault: &RootReplyFaultControl,
    action: ExchangeFaultAction,
    node: Option<i32>,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    ensure!(
        deadline < context.deadline(),
        "original scenario budget cannot cover capture"
    );
    fault.begin_exchange_capture_on(
        action,
        ExchangeFaultSelector {
            destination_node: node,
            ..Default::default()
        },
        deadline,
    )
}

fn bind_candidate(
    context: &mut ScenarioContext,
    fault: &RootReplyFaultControl,
    job: &QueryJob,
    receiver: &ExplainedReceiver,
) -> Result<ExchangeFaultCandidate> {
    loop {
        context
            .remaining("bind actual Exchange candidate to nominated query and live BE roster")?;
        capture_healthy(fault)?;
        if let Some(candidate) = fault.exchange_candidate() {
            let stream = candidate.target.stream();
            ensure!(
                stream.destination_node().get() == receiver.node,
                "fault candidate differs from the actual explained receiver node"
            );
            let source = stream.source().task();
            let destination = stream.destination();
            ensure!(
                source.query_execution_id() == destination.query_execution_id(),
                "fault stream crosses attempts"
            );
            ensure!(
                candidate.caller == source.backend_process_id(),
                "signed caller differs from original stream source"
            );
            ensure!(
                context
                    .handle()
                    .backend_process_id(candidate.backend_index)?
                    == destination.backend_process_id(),
                "candidate listener differs from exact destination BE"
            );
            let mut source_found = false;
            let mut destination_found = false;
            for index in 0..context.handle().be_count() {
                let process = context.handle().backend_process_id(index)?;
                if process != source.backend_process_id()
                    && process != destination.backend_process_id()
                {
                    continue;
                }
                let Some(binding) = context.handle().selected_native_observation_binding(
                    index,
                    novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                        .as_str(),
                    &job.selected.token,
                    Some(1),
                )?
                else {
                    continue;
                };
                ensure!(
                    binding.execution()? == source.query_execution_id()
                        && binding.process()? == process,
                    "candidate is not the exact nominated statement attempt"
                );
                let snapshot = context.handle().selected_native_observation_binding(
                    index,
                    novarocks_failpoint::observation_marker::ObservationAction::ExchangeSnapshot
                        .as_str(),
                    &job.selected.token,
                    Some(1),
                )?;
                ensure!(
                    snapshot.as_ref() == Some(&binding),
                    "source observation and Task nomination differ"
                );
                source_found |= process == source.backend_process_id();
                destination_found |= process == destination.backend_process_id();
            }
            if source_found && destination_found {
                fault.arm_exchange_exact(&candidate)?;
                let token_digest = format!("{:x}", Sha256::digest(job.selected.token.as_bytes()));
                retain(
                    context,
                    &format!("exchange-armed-candidate-{token_digest}.json"),
                    &json!({
                        "schema_version": 1,
                        "statement_run_token": job.selected.token,
                        "receiver": receiver,
                        "candidate": candidate,
                        "armed": true,
                    }),
                )?;
                return Ok(candidate);
            }
        }
        thread::park_timeout(POLL);
    }
}

fn await_fault_exit(
    context: &mut ScenarioContext,
    fault: &RootReplyFaultControl,
) -> Result<ExchangeFaultObservation> {
    loop {
        context.remaining("await original target H2 stream future exits")?;
        let observation = fault.exchange_observation();
        if observation.active_target_streams == 0
            && observation
                .attempts
                .iter()
                .all(|attempt| attempt.stream_future_exited_at_millis.is_some())
        {
            let transport = fault.observation();
            ensure!(
                transport.failures.is_empty() && !transport.failure_overflow,
                "original bounded fault actor failed: {transport:?}"
            );
            return Ok(observation);
        }
        thread::park_timeout(POLL);
    }
}

fn await_pressure(
    context: &mut ScenarioContext,
    fault: &RootReplyFaultControl,
) -> Result<Vec<FlowSnapshot>> {
    loop {
        context.remaining("observe actual ordered positions and pending demand competition")?;
        capture_healthy(fault)?;
        let observed = fault.exchange_observation();
        ensure!(
            original_still_held(&observed),
            "held Unit was dispatched before release"
        );
        if applied_successor(&observed)
            && let Some(snapshots) = flow_snapshots(context)?
            && snapshots.iter().any(|snapshot| {
                snapshot.in_use > 0
                    && snapshot.max_instance_in_use == snapshot.instance_max
                    && snapshot.floating_in_use == snapshot.floating
                    && snapshot.pending_demands > 0
            })
        {
            return Ok(snapshots);
        }
        thread::park_timeout(POLL);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Park {
    driver: i32,
    epoch: u64,
    generation: u64,
    line: String,
}

fn scalar<T: std::str::FromStr>(line: &str, name: &str) -> Result<T> {
    let prefix = format!("{name}=");
    let values: Vec<_> = line
        .split_whitespace()
        .filter_map(|word| word.strip_prefix(&prefix))
        .collect();
    ensure!(values.len() == 1, "exchange event omits or repeats {name}");
    values[0]
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid exchange event {name}"))
}

fn parked(
    context: &mut ScenarioContext,
    candidate: &ExchangeFaultCandidate,
) -> Result<Option<Park>> {
    let task_key = candidate.target.stream().destination_task_key();
    let identity = format!("task_key={:?}", Some((task_key.high(), task_key.low())));
    let source = format!(
        "source_name=\"EXCHANGE_SOURCE (id={})\"",
        candidate.target.stream().destination_node().get()
    );
    let text = context
        .handle()
        .be_stdout_contents(candidate.backend_index, usize::MAX)?;
    current_park(&text, &identity, &source)
}

fn driver_wakes(text: &str, identity: &str) -> Result<BTreeMap<(i32, u64), (u64, u64)>> {
    let mut wakes = BTreeMap::new();
    for line in text.lines().filter(|line| {
        line.contains(novarocks_failpoint::observation_marker::EXCHANGE_SNAPSHOT)
            && line.contains(identity)
            && line.contains("event=driver_wake ")
    }) {
        let key = (scalar(line, "driver_id")?, scalar(line, "block_epoch")?);
        let generations = (
            scalar(line, "blocked_generation")?,
            scalar(line, "current_generation")?,
        );
        ensure!(
            wakes.insert(key, generations).is_none(),
            "duplicate actual driver wake epoch"
        );
    }
    Ok(wakes)
}

fn current_park(text: &str, identity: &str, source: &str) -> Result<Option<Park>> {
    // A dispatcher may publish wake before the registering thread flushes
    // its park line. Pair by the actual epoch, never by log arrival order.
    let wakes = driver_wakes(text, identity)?;
    let mut latest = None;
    for line in text.lines().filter(|line| {
        line.contains(novarocks_failpoint::observation_marker::EXCHANGE_SNAPSHOT)
            && line.contains(identity)
    }) {
        if line.contains("event=driver_park ")
            && line.contains(source)
            && line.contains("reason=InputEmpty ")
        {
            let park = Park {
                driver: scalar(line, "driver_id")?,
                epoch: scalar(line, "block_epoch")?,
                generation: scalar(line, "frozen_generation")?,
                line: line.to_owned(),
            };
            let current: u64 = scalar(line, "current_generation")?;
            ensure!(
                park.driver >= 0 && park.epoch > 0,
                "invalid actual driver park identity"
            );
            if let Some((blocked, _)) = wakes.get(&(park.driver, park.epoch)) {
                ensure!(
                    *blocked == park.generation,
                    "actual wake disagrees with its original parked generation"
                );
            } else if park.generation > 0 && current == park.generation {
                // A zero initial frontier or an already changed endpoint is
                // not evidence for the requested stable nonzero parked epoch.
                latest = Some(park);
            }
        }
    }
    Ok(latest)
}

fn hold_parked(
    context: &mut ScenarioContext,
    fault: &RootReplyFaultControl,
    candidate: &ExchangeFaultCandidate,
) -> Result<Park> {
    let mut stable: Option<(Park, Instant)> = None;
    loop {
        context
            .remaining("observe the same actual Exchange source parked beyond transport timeout")?;
        capture_healthy(fault)?;
        let observation = fault.exchange_observation();
        ensure!(
            original_still_held(&observation),
            "long wait did not precede actual upstream admission"
        );
        if let Some(park) = parked(context, candidate)? {
            if stable.as_ref().is_none_or(|(old, _)| old != &park) {
                stable = Some((park, Instant::now()));
            }
            if let Some((park, since)) = &stable
                && since.elapsed() >= HOLD
            {
                return Ok(park.clone());
            }
        } else {
            stable = None;
        }
        let _ = flow_snapshots(context)?;
        thread::park_timeout(POLL);
    }
}

fn phase(evidence: &mut Value, value: Value) {
    evidence["phases"]
        .as_array_mut()
        .expect("phase evidence array")
        .push(value);
}

fn capture_healthy(fault: &RootReplyFaultControl) -> Result<()> {
    let original = fault.observation();
    ensure!(
        original.failures.is_empty() && !original.failure_overflow,
        "original authenticated fault actor failed: {original:?}"
    );
    ensure!(
        fault.exchange_observation().elapsed_millis
            < RootReplyFaultBounds::default().maximum_capture_millis,
        "original absolute Exchange fault capture deadline expired"
    );
    Ok(())
}

fn original_still_held(observation: &ExchangeFaultObservation) -> bool {
    let Some(candidate) = observation.candidate.as_ref() else {
        return false;
    };
    let original = || {
        observation
            .attempts
            .iter()
            .filter(|attempt| attempt.candidate.target == candidate.target)
    };
    original().any(|attempt| attempt.data_forward_held_at_millis.is_some())
        && original().all(|attempt| attempt.request_forwarded_at_millis.is_none())
}

fn applied_successor(observation: &ExchangeFaultObservation) -> bool {
    let Some(candidate) = observation.candidate.as_ref() else {
        return false;
    };
    let Some(first) = candidate.target.sequence() else {
        return false;
    };
    let Ok(next) = first.checked_next() else {
        return false;
    };
    // The actor admits only real payload Units into its successor positions,
    // never an End. Applied with no close/stop follows decode and queue commit.
    // Custody is a contiguous prefix: it cannot jump over the missing first Unit.
    observation.attempts.iter().any(|attempt| {
        attempt.candidate.target.stream() == candidate.target.stream()
            && attempt.candidate.target.sequence() == Some(next)
            && attempt.verdict == Some("Applied")
            && attempt.receiver_closed == Some(false)
            && attempt.sender_stopped == Some(false)
            && attempt
                .custody_through
                .is_some_and(|custody| custody < first.get())
    })
}

fn short_query(context: &mut ScenarioContext) -> Result<()> {
    let mut control =
        SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
    let actual = sql::integer_column(&control.execute("SELECT 17")?)?;
    ensure!(
        actual == [17],
        "short query failed while Exchange positions were held"
    );
    Ok(())
}

fn require_wake(
    context: &mut ScenarioContext,
    candidate: &ExchangeFaultCandidate,
    park: &Park,
) -> Result<()> {
    let task_key = candidate.target.stream().destination_task_key();
    let identity = format!("task_key={:?}", Some((task_key.high(), task_key.low())));
    let text = context
        .handle()
        .be_stdout_contents(candidate.backend_index, usize::MAX)?;
    let wakes = driver_wakes(&text, &identity)?;
    let (blocked, current) = wakes
        .get(&(park.driver, park.epoch))
        .context("held Source's original driver/block epoch never actually woke")?;
    ensure!(
        *blocked == park.generation && *current > park.generation,
        "actual Source wake did not advance the original parked driver frontier generation"
    );
    Ok(())
}

impl ExchangeFlow {
    fn bounded(
        &self,
        context: &mut ScenarioContext,
        fault: &RootReplyFaultControl,
        jobs: &mut Vec<QueryJob>,
        evidence: &mut Value,
    ) -> Result<()> {
        for ordinal in 0..3 {
            let sql = self.sqls()[ordinal].clone();
            let partitioned = ordinal == 2;
            let plan = explain(context, &sql, partitioned)?;
            let receiver = if ordinal == 0 {
                plan.gather_receiver()?
            } else {
                plan.build_receiver(partitioned)?
            };
            let node = receiver.node;
            retain(
                context,
                &format!("exchange-shape-{ordinal}.json"),
                &json!({"sql":sql,"plan":plan,"receiver":receiver}),
            )?;
            capture(
                context,
                fault,
                ExchangeFaultAction::HoldDataBeforeForward,
                Some(node),
            )?;
            let target = jobs.len();
            jobs.push(QueryJob::start(
                context,
                self.intent.take(ordinal)?,
                settings(partitioned, 120),
            )?);
            let candidate = bind_candidate(context, fault, &jobs[target], &receiver)?;
            let pressure = await_pressure(context, fault)?;
            let concurrent = jobs.len();
            jobs.push(QueryJob::start(
                context,
                self.intent.take(ordinal + 3)?,
                settings(false, 120),
            )?);
            let healthy = jobs[concurrent].finish(context, Outcome::Rows)?;
            ensure!(
                jobs[target]
                    .actor
                    .as_ref()
                    .is_some_and(|job| !job.is_finished()),
                "held query completed without its required original Unit"
            );
            ensure!(
                original_still_held(&fault.exchange_observation()),
                "required Unit was released before concurrent progress"
            );
            fault.release_exchange();
            let rows = jobs[target].finish(context, Outcome::Rows)?;
            let receipt = await_fault_exit(context, fault)?;
            let idle = await_flow_idle(context)?;
            phase(
                evidence,
                json!({"shape": (["gather","broadcast","shuffle"][ordinal]),"candidate":candidate,
                "actual_pressure":pressure,"concurrent_result":healthy,"result":rows,"fault":receipt,"idle":idle}),
            );
        }
        Ok(())
    }

    fn cross_query(
        &self,
        context: &mut ScenarioContext,
        fault: &RootReplyFaultControl,
        jobs: &mut Vec<QueryJob>,
        evidence: &mut Value,
    ) -> Result<()> {
        let plan = explain(context, &join_sql(), true)?;
        let receiver = plan.build_receiver(true)?;
        let node = receiver.node;
        retain(
            context,
            "exchange-probe-build-plan.json",
            &json!({"sql":join_sql(),"plan":plan,"receiver":receiver}),
        )?;
        capture(
            context,
            fault,
            ExchangeFaultAction::HoldDataBeforeForward,
            Some(node),
        )?;
        jobs.push(QueryJob::start(
            context,
            self.intent.take(0)?,
            settings(true, 120),
        )?);
        let candidate = bind_candidate(context, fault, &jobs[0], &receiver)?;
        let pressure = await_pressure(context, fault)?;
        jobs.push(QueryJob::start(
            context,
            self.intent.take(1)?,
            settings(true, 120),
        )?);
        let healthy = jobs[1].finish(context, Outcome::Rows)?;
        ensure!(
            original_still_held(&fault.exchange_observation())
                && jobs[0].actor.as_ref().is_some_and(|job| !job.is_finished()),
            "other query progress was measured after releasing the held build"
        );
        fault.release_exchange();
        let first = jobs[0].finish(context, Outcome::Rows)?;
        phase(
            evidence,
            json!({"candidate":candidate,"pressure":pressure,"second_query_finished_before_release":healthy,"first_query":first}),
        );
        Ok(())
    }

    fn replay(
        &self,
        context: &mut ScenarioContext,
        fault: &RootReplyFaultControl,
        jobs: &mut Vec<QueryJob>,
        evidence: &mut Value,
    ) -> Result<()> {
        let receiver = explain_gather(context, "exchange-replay-plan.json")?;
        let node = receiver.node;
        let before = await_flow_idle(context)?;
        capture(
            context,
            fault,
            ExchangeFaultAction::DropAcceptedReply,
            Some(node),
        )?;
        jobs.push(QueryJob::start(
            context,
            self.intent.take(0)?,
            settings(false, 120),
        )?);
        let candidate = bind_candidate(context, fault, &jobs[0], &receiver)?;
        let sequence = candidate
            .target
            .sequence()
            .context("replay selected a non-Unit target")?
            .get();
        loop {
            context.remaining(
                "observe original admitted Unit reply loss and actual idempotent replay",
            )?;
            capture_healthy(fault)?;
            let observation = fault.exchange_observation();
            if observation.replies_dropped == 1 && observation.replay_idempotent > 0 {
                ensure!(
                    observation
                        .attempts
                        .iter()
                        .any(|attempt| attempt.reply_dropped
                            && attempt
                                .custody_through
                                .is_some_and(|custody| custody >= sequence)
                            && attempt.admission_observed_at_millis.is_some()),
                    "first reply was not lost after real accepted custody"
                );
                ensure!(
                    observation.attempts.iter().any(|attempt| attempt.replay
                        && attempt.verdict == Some("Idempotent")
                        && attempt.candidate.target == candidate.target
                        && attempt.candidate.request_sha256 == candidate.request_sha256),
                    "replay changed its exact original stream, sequence or bytes"
                );
                phase(
                    evidence,
                    json!({"candidate":candidate,"lost_reply_and_replay":observation}),
                );
                break;
            }
            thread::park_timeout(POLL);
        }
        fault.release_exchange();
        let rows = jobs[0].finish(context, Outcome::Rows)?;
        let after = await_flow_idle(context)?;
        ensure!(
            after[candidate.backend_index].replays > before[candidate.backend_index].replays,
            "native reply replay did not reach the original receiver admission author"
        );
        phase(
            evidence,
            json!({"result":rows,"receiver_before":before,"receiver_after":after}),
        );
        Ok(())
    }

    fn rebuild(
        &self,
        context: &mut ScenarioContext,
        fault: &RootReplyFaultControl,
        jobs: &mut Vec<QueryJob>,
        evidence: &mut Value,
    ) -> Result<()> {
        let receiver = explain_gather(context, "exchange-rebuild-plan.json")?;
        let node = receiver.node;
        capture(
            context,
            fault,
            ExchangeFaultAction::CutGrantSubscription,
            Some(node),
        )?;
        jobs.push(QueryJob::start(
            context,
            self.intent.take(0)?,
            settings(false, 120),
        )?);
        let candidate = bind_candidate(context, fault, &jobs[0], &receiver)?;
        let original = candidate
            .target
            .generation()
            .context("grant rebuild selected a non-subscription target")?
            .get();
        loop {
            context.remaining(
                "observe exact grant subscription cut and complete new-generation snapshot",
            )?;
            capture_healthy(fault)?;
            let observation = fault.exchange_observation();
            if observation.subscriptions_cut == 1 && observation.rebuilt_snapshot_complete {
                ensure!(
                    observation
                        .rebuilt_generation
                        .is_some_and(|generation| generation != original && generation > 0),
                    "grant subscription did not change its generation"
                );
                ensure!(
                    observation
                        .rebuilt_stream
                        .is_some_and(|target| target.stream() == candidate.target.stream()),
                    "rebuilt subscription changed its original demand stream"
                );
                ensure!(
                    observation
                        .rebuilt_stream_granted_through
                        .is_some_and(|sequence| sequence > 0),
                    "new snapshot omitted the original stream's real grant"
                );
                phase(
                    evidence,
                    json!({"candidate":candidate,"rebuilt":observation}),
                );
                break;
            }
            thread::park_timeout(POLL);
        }
        fault.release_exchange();
        let result = jobs[0].finish(context, Outcome::Rows)?;
        phase(evidence, json!({"result":result}));
        Ok(())
    }

    fn long_wait(
        &self,
        context: &mut ScenarioContext,
        fault: &RootReplyFaultControl,
        jobs: &mut Vec<QueryJob>,
        evidence: &mut Value,
    ) -> Result<()> {
        let plan = explain(context, &join_sql(), true)?;
        let receiver = plan.build_receiver(true)?;
        let node = receiver.node;
        retain(
            context,
            "exchange-long-wait-plan.json",
            &json!({"sql":join_sql(),"plan":plan,"receiver":receiver}),
        )?;
        capture(
            context,
            fault,
            ExchangeFaultAction::HoldDataBeforeForward,
            Some(node),
        )?;
        jobs.push(QueryJob::start(
            context,
            self.intent.take(0)?,
            settings(true, 120),
        )?);
        let candidate = bind_candidate(context, fault, &jobs[0], &receiver)?;
        let pressure = await_pressure(context, fault)?;
        let park = hold_parked(context, fault, &candidate)?;
        ensure!(
            HOLD.as_millis() > u128::from(TRANSPORT_MILLIS),
            "long-wait interval does not exceed actual transport deadline"
        );
        fault.release_exchange();
        let success = jobs[0].finish(context, Outcome::Rows)?;
        require_wake(context, &candidate, &park)?;
        let first_fault = await_fault_exit(context, fault)?;
        let first_idle = await_flow_idle(context)?;
        phase(
            evidence,
            json!({"candidate":candidate,"pressure":pressure,"actual_park":park,"continuous_park_millis":HOLD.as_millis(),
            "transport_millis":TRANSPORT_MILLIS,"result":success,"actual_wake_same_epoch":true,"fault":first_fault,"idle":first_idle}),
        );

        // The second statement retains its own original query deadline. The
        // transport rule remains held until that QueryOwner has terminated it.
        capture(
            context,
            fault,
            ExchangeFaultAction::HoldDataBeforeForward,
            Some(node),
        )?;
        jobs.push(QueryJob::start(
            context,
            self.intent.take(1)?,
            settings(true, 1),
        )?);
        let deadline_candidate = bind_candidate(context, fault, &jobs[1], &receiver)?;
        let deadline = jobs[1].finish(context, Outcome::Deadline)?;
        let held = fault.exchange_observation();
        ensure!(
            original_still_held(&held),
            "query deadline was measured after data release"
        );
        fault.release_exchange();
        phase(
            evidence,
            json!({"short_deadline_candidate":deadline_candidate,"query_owner":deadline,"still_held_at_query_terminal":held}),
        );
        Ok(())
    }

    fn control(
        &self,
        context: &mut ScenarioContext,
        fault: &RootReplyFaultControl,
        jobs: &mut Vec<QueryJob>,
        evidence: &mut Value,
    ) -> Result<()> {
        let plan = explain(context, &join_sql(), true)?;
        let receiver = plan.build_receiver(true)?;
        let node = receiver.node;
        retain(
            context,
            "exchange-control-plan.json",
            &json!({"sql":join_sql(),"plan":plan,"receiver":receiver}),
        )?;
        capture(
            context,
            fault,
            ExchangeFaultAction::HoldDataBeforeForward,
            Some(node),
        )?;
        jobs.push(QueryJob::start(
            context,
            self.intent.take(0)?,
            settings(true, 120),
        )?);
        let candidate = bind_candidate(context, fault, &jobs[0], &receiver)?;
        let pressure = await_pressure(context, fault)?;
        short_query(context)?;
        jobs.push(QueryJob::start(
            context,
            self.intent.take(1)?,
            settings(true, 120),
        )?);
        let other = jobs[1].finish(context, Outcome::Rows)?;
        ensure!(
            original_still_held(&fault.exchange_observation()),
            "control work ran only after releasing the held stream"
        );
        jobs[0].cancel()?;
        let cancelled = jobs[0].finish(context, Outcome::Cancelled)?;
        // Cancellation does not release the actor's original target lease.
        // Observe its real exit separately before releasing the rule itself.
        let exited = await_fault_exit(context, fault)?;
        ensure!(
            exited.peer_cancels > 0,
            "client query cancellation did not reach the original held RPC owner"
        );
        fault.release_exchange();
        short_query(context)?;
        phase(
            evidence,
            json!({"candidate":candidate,"actual_pressure":pressure,"short_query_before_release":true,
            "healthy_query":other,"client_cancel_query_owner":cancelled,"actual_rpc_exits_before_rule_release":exited}),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_mismatch_keeps_original_wire_error_and_transport_object() {
        let wire = r#"{"message":"source","errorCode":3,"errorName":"USER_CANCELED","errorType":"INTERNAL_ERROR","ownerExtra":{"original":true}}"#;
        let actual = StatementFailure::Trino(serde_json::from_str(wire).unwrap());
        let error = expected_failure(actual, Outcome::Cancelled).unwrap_err();
        let actual = error.downcast_ref::<StatementFailure>().unwrap();
        let StatementFailure::Trino(actual) = actual else {
            panic!("typed source replaced");
        };
        assert_eq!(serde_json::to_string(actual).unwrap(), wire);
        let error = expected_failure(
            StatementFailure::Transport("original transport".into()),
            Outcome::Deadline,
        )
        .unwrap_err();
        assert!(
            matches!(error.downcast_ref::<StatementFailure>(), Some(StatementFailure::Transport(text)) if text == "original transport")
        );
    }
    #[test]
    fn multiple_failures_keep_both_actual_error_objects_in_order() {
        let failure = combine::<()>(
            Err(StatementFailure::Transport("original wait".into()).into()),
            Err(StatementFailure::Deadline.into()),
        )
        .unwrap_err();
        let pair = failure.downcast_ref::<ExchangeFailure>().unwrap();
        assert!(
            matches!(pair.primary.downcast_ref::<StatementFailure>(), Some(StatementFailure::Transport(text)) if text == "original wait")
        );
        assert!(matches!(
            pair.cleanup.downcast_ref::<StatementFailure>(),
            Some(StatementFailure::Deadline)
        ));
    }
    fn typed_result(fields: &[(&str, &str)], rows: Vec<Vec<TrinoValue>>) -> StatementResult {
        use novarocks_test_trino_client::{Column, TypeSignature};
        StatementResult {
            columns: fields
                .iter()
                .map(|(name, ty)| Column {
                    name: (*name).into(),
                    type_name: (*ty).into(),
                    type_signature: TypeSignature::simple(*ty),
                })
                .collect(),
            rows,
            query_id: "local-typed-fixture".into(),
            pages: 1,
            update_type: None,
            update_count: None,
        }
    }
    #[test]
    fn typed_file_geometry_preserves_original_values_and_rejects_null_or_overflow() {
        let names = [
            ("content", "integer"),
            ("file_path", "varchar"),
            ("file_format", "varchar"),
            ("record_count", "bigint"),
            ("file_size_in_bytes", "bigint"),
        ];
        let valid = vec![
            TrinoValue::Integer(0),
            TrinoValue::Text("original.parquet".into()),
            TrinoValue::Text("PARQUET".into()),
            TrinoValue::Integer(4096),
            TrinoValue::Integer(1024),
        ];
        let result = typed_result(&names, vec![valid.clone()]);
        assert_eq!(
            file_geometry(&result).unwrap(),
            vec![(0, "original.parquet".into(), "PARQUET".into(), 4096, 1024)]
        );
        let mut invalid = valid.clone();
        invalid[3] = TrinoValue::Null;
        assert!(file_geometry(&typed_result(&names, vec![invalid])).is_err());
        let mut invalid = valid;
        invalid[0] = TrinoValue::Integer(i64::from(i32::MAX) + 1);
        assert!(file_geometry(&typed_result(&names, vec![invalid])).is_err());
    }
    #[test]
    fn typed_explain_preserves_original_first_line_and_rejects_nontext() {
        let result = typed_result(
            &[("plan", "varchar")],
            vec![
                vec![TrinoValue::Text("PLAN FRAGMENT 0".into())],
                vec![TrinoValue::Text("  original plan".into())],
            ],
        );
        assert_eq!(
            plan_text(&result).unwrap(),
            "PLAN FRAGMENT 0\n  original plan"
        );
        assert!(
            plan_text(&typed_result(
                &[("plan", "varchar")],
                vec![vec![TrinoValue::Null]]
            ))
            .is_err()
        );
        assert!(
            plan_text(&typed_result(
                &[("plan", "bigint")],
                vec![vec![TrinoValue::Integer(1)]]
            ))
            .is_err()
        );
    }

    #[test]
    fn explicit_flow_overlay_preserves_the_original_connector_configuration() {
        let base = connector_launch_config();
        let mut expected: toml::Value = base.config_overlay.be.unwrap().parse().unwrap();
        let configured = ExchangeFlow {
            case: Case::Bounded,
            intent: SqlIntentOwner::default(),
        }
        .launch_config(Path::new("unused"))
        .unwrap();
        let mut actual: toml::Value = configured.config_overlay.be.unwrap().parse().unwrap();
        let fields = [
            ("exchange_instance_floor_slots", 1),
            ("exchange_instance_max_slots", 2),
            ("exchange_floating_slots", 1),
            ("exchange_stream_window", 2),
            ("exchange_transport_timeout_ms", 1000),
            ("exchange_io_max_inflight_bytes", 262144),
            ("exchange_max_transmit_batched_bytes", 8192),
        ];
        for (name, value) in fields {
            assert_eq!(
                actual["runtime"]
                    .as_table_mut()
                    .unwrap()
                    .remove(name)
                    .unwrap()
                    .as_integer(),
                Some(value)
            );
            expected["runtime"].as_table_mut().unwrap().remove(name);
        }
        assert_eq!(actual, expected);
        assert_eq!(
            configured.native_root_reply_fault.unwrap().backend_indices,
            configured
                .native_fault_proxies
                .backend_retained_byte_limits
                .into_keys()
                .collect()
        );
    }

    #[test]
    fn physical_tree_display_ids_never_supply_frozen_receiver_identity() {
        // The physical tree is the gather shape captured by p10-native03.
        // Its global display ordinal 1 is not an original fragment node ID.
        let tree = "PLAN FRAGMENT 0\n  OUTPUT EXPRS: *\n  STAGE PLACEMENT: singleton\n  DRIVER DOMAIN: min=1, max=5, power-of-two=false\n  PARTITION: UNPARTITIONED\n0:SORT BY [k ASC NULLS FIRST] stats={rows=24576}\n  1:GATHER stats={rows=24576}\nPLAN FRAGMENT 1\n  OUTPUT EXPRS: *\n  STAGE PLACEMENT: scan-work\n  DRIVER DOMAIN: min=1, max=5, power-of-two=false\n  PARTITION: UNPARTITIONED\n  STREAM DATA SINK\n    EXCHANGE ID: 1\n    PARTITION: UNPARTITIONED\n2:PROJECT [k] stats={rows=24576}\n  3:SCAN native_flow.rows stats={rows=24576}\n";
        // CONTRACT uses the separate completed.rs renderer's original IDs,
        // original edge identity and explicit fragment/depth fields.
        let contract = "PHYSICAL PLAN version=01, contract-revision=1\nEDGE GRAPH\n  edge7 kind=relational source={fragment=f1, projection=[v1], distribution=unconstrained, multiplicity=single-copy} destination={fragment=f0, node=n0, mapping=[f1.v1->f0.v2], distribution=singleton, multiplicity=single-copy}\nSORT [fragment=0, node=1, depth=0]\n  EXCHANGE (EXCHANGE ID: 7) [fragment=0, node=0, depth=1]\n    edge=7, imports=[v1->v2]\nSCAN native_flow.rows [fragment=1, node=0, depth=0]\n";
        let plan = ExplainedPlan::new(tree.into(), contract.into()).unwrap();
        let selected = plan.gather_receiver().unwrap();
        assert_eq!(selected.node, 0);
        assert_eq!(
            (
                selected.destination_fragment,
                selected.edge,
                selected.source_fragment
            ),
            (0, 7, 1)
        );
        assert!(ExplainedPlan::new(tree.into(), tree.into()).is_err());
        assert!(ExplainedPlan::new(contract.into(), tree.into()).is_err());
        assert!(
            ExplainedPlan::new(
                tree.into(),
                "PHYSICAL PLAN version=01, contract-revision=1".into()
            )
            .is_err()
        );
        assert!(
            ExplainedPlan::new(tree.into(), contract.replace("edge7 kind=", "edge8 kind="))
                .unwrap()
                .gather_receiver()
                .is_err()
        );
        assert!(
            ExplainedPlan::new(
                tree.replace("1:GATHER", "1:BROADCAST EXCHANGE"),
                contract.into()
            )
            .unwrap()
            .gather_receiver()
            .is_err()
        );
    }

    #[test]
    fn contract_build_selection_preserves_original_side_edge_and_fragment() {
        // This is the partitioned tree captured by p10-native03. It contains
        // no frozen build-side field and cannot supply a protocol identity.
        let tree = "PLAN FRAGMENT 0\n0:GATHER stats={rows=24648}\nPLAN FRAGMENT 1\n  STREAM DATA SINK\n    EXCHANGE ID: 0\n    PARTITION: UNPARTITIONED\n1:PROJECT [p.k] stats={rows=24648}\n  2:HASH JOIN (PARTITIONED, INNER, eq: [p.k = b.k]) stats={rows=24648}\n    3:HASH EXCHANGE\n    4:HASH EXCHANGE\nPLAN FRAGMENT 2\n  STREAM DATA SINK\n    EXCHANGE ID: 3\n    PARTITION: HASH_PARTITIONED (p.k)\n5:SCAN native_flow.rows (alias=p) stats={rows=24576}\nPLAN FRAGMENT 3\n  STREAM DATA SINK\n    EXCHANGE ID: 4\n    PARTITION: HASH_PARTITIONED (b.k)\n6:SCAN native_flow.rows (alias=b) stats={rows=24576}\n";
        let contract = "PHYSICAL PLAN version=01, contract-revision=1\nEDGE GRAPH\n edge10 kind=relational source={fragment=f1, projection=[v1]} destination={fragment=f0, node=n0, distribution=singleton, multiplicity=single-copy}\n edge11 kind=relational source={fragment=f2, projection=[v1]} destination={fragment=f1, node=n0, distribution=hash, multiplicity=single-copy}\n edge12 kind=relational source={fragment=f3, projection=[v1]} destination={fragment=f1, node=n1, distribution=hash, multiplicity=single-copy}\nEXCHANGE (EXCHANGE ID: 10) [fragment=0, node=0, depth=0]\nPROJECT [fragment=1, node=3, depth=0]\n HASH JOIN (PARTITIONED, INNER) [fragment=1, node=2, depth=1]\n  kind=INNER, build-side=right, distribution=PARTITIONED\n  HASH EXCHANGE (EXCHANGE ID: 11) [fragment=1, node=0, depth=2]\n  HASH EXCHANGE (EXCHANGE ID: 12) [fragment=1, node=1, depth=2]\nSCAN native_flow.rows [fragment=2, node=0, depth=0]\nSCAN native_flow.rows [fragment=3, node=0, depth=0]\n";
        let plan = ExplainedPlan::new(tree.into(), contract.into()).unwrap();
        let selected = plan.build_receiver(true).unwrap();
        assert_eq!(
            (
                selected.destination_fragment,
                selected.node,
                selected.edge,
                selected.source_fragment
            ),
            (1, 1, 12, 3)
        );
        assert!(plan.build_receiver(false).is_err());
        assert!(build_receiver_node(tree).is_err());
        assert!(
            ExplainedPlan::new(
                tree.into(),
                contract.replace("build-side=right", "missing-side=right")
            )
            .unwrap()
            .build_receiver(true)
            .is_err()
        );
        // Node 0 exists in two receiver fragments. A local node selector is
        // insufficient here; do not guess StageId from either display view.
        assert!(
            ExplainedPlan::new(
                tree.into(),
                contract.replace("build-side=right", "build-side=left")
            )
            .unwrap()
            .build_receiver(true)
            .is_err()
        );
        let left = contract
            .replace("build-side=right", "build-side=left")
            .replace("fragment=f0, node=n0", "fragment=f0, node=n9")
            .replace(
                "[fragment=0, node=0, depth=0]",
                "[fragment=0, node=9, depth=0]",
            );
        let selected = ExplainedPlan::new(tree.into(), left)
            .unwrap()
            .build_receiver(true)
            .unwrap();
        assert_eq!(
            (
                selected.destination_fragment,
                selected.node,
                selected.edge,
                selected.source_fragment
            ),
            (1, 0, 11, 2)
        );
    }

    #[test]
    fn gather_selection_requires_the_exact_singleton_edge_and_global_sort_input() {
        let edge = "edge7 kind=relational source={fragment=f1, projection=[v1], distribution=unconstrained, multiplicity=single-copy} destination={fragment=f0, node=n42, mapping=[f1.v1->f0.v2], distribution=singleton, multiplicity=single-copy}\n";
        let tree = "SORT [fragment=0, node=9, depth=0]\n EXCHANGE (EXCHANGE ID: 7) [fragment=0, node=42, depth=1]\nSCAN native_flow.rows [fragment=1, node=1, depth=0]\n";
        let plan = format!("{edge}{tree}");
        assert_eq!(singleton_receiver_node(&plan).unwrap(), 42);
        // A singleton source is not authority for a broadcast destination.
        let broadcast = edge
            .replace("distribution=unconstrained", "distribution=singleton")
            .replace(
                "mapping=[f1.v1->f0.v2], distribution=singleton",
                "mapping=[f1.v1->f0.v2], distribution=broadcast",
            );
        assert!(singleton_receiver_node(&format!("{broadcast}{tree}")).is_err());
        assert!(singleton_receiver_node(&plan.replace("node=n42", "node=n43")).is_err());
        assert!(
            singleton_receiver_node(
                &plan.replace("fragment=f0, node=n42", "fragment=f2, node=n42")
            )
            .is_err()
        );
        assert!(singleton_receiver_node(&plan.replace("SORT [", "PROJECT [")).is_err());
        assert!(singleton_receiver_node(&format!("{edge}{plan}")).is_err());
        assert!(singleton_receiver_node("PROJECT [fragment=0, node=0, depth=0]\nSCAN native_flow.rows [fragment=0, node=1, depth=1]\n").is_err());
        let unrelated = tree.replace(
            "SORT [fragment=0, node=9, depth=0]\n",
            "PROJECT [fragment=0, node=8, depth=0]\n SORT [fragment=0, node=9, depth=1]\n PROJECT [fragment=0, node=10, depth=1]\n",
        ).replace("node=42, depth=1", "node=42, depth=2");
        assert!(singleton_receiver_node(&format!("{edge}{unrelated}")).is_err());
    }

    #[test]
    fn receiver_selection_uses_the_declared_join_side_and_never_guesses_a_remote_input() {
        let plan = "HASH JOIN (PARTITIONED, INNER) [fragment=2, node=9, depth=0]\n kind=INNER, build-side=right, distribution=PARTITIONED\n PROJECT [fragment=2, node=1, depth=1]\n HASH EXCHANGE (EXCHANGE ID: 3) [fragment=2, node=41, depth=2]\n PROJECT [fragment=2, node=2, depth=1]\n HASH EXCHANGE (EXCHANGE ID: 4) [fragment=2, node=42, depth=2]\n";
        assert_eq!(build_receiver_node(plan).unwrap(), 42);
        assert_eq!(
            build_receiver_node(&plan.replace("build-side=right", "build-side=left")).unwrap(),
            41
        );
        assert!(
            build_receiver_node(&plan.replace("build-side=right", "omitted-side=right")).is_err()
        );
        assert!(
            build_receiver_node(&format!(
                "{plan}HASH EXCHANGE (EXCHANGE ID: 5) [fragment=2, node=43, depth=2]\n"
            ))
            .is_err()
        );
    }

    #[test]
    fn observation_parser_rejects_missing_duplicate_and_non_finite_samples() {
        assert_eq!(metric("m 0\n", "m", None).unwrap(), 0);
        assert!(metric("", "m", None).is_err());
        assert!(metric("m 0\nm 1\n", "m", None).is_err());
        assert!(metric("m NaN\n", "m", None).is_err());
        assert!(metric("m -1\n", "m", None).is_err());
    }

    #[test]
    fn source_park_parser_preserves_actual_driver_generation_and_epoch() {
        let identity = "task_key=Some((11, 22))";
        let source = "source_name=\"EXCHANGE_SOURCE (id=6)\"";
        let park = &*format!(
            "{} event=driver_park task_key=Some((11, 22)) driver_id=9 reason=InputEmpty frozen_generation=7 current_generation=7 block_epoch=41 source_name=\"EXCHANGE_SOURCE (id=6)\"\n",
            novarocks_failpoint::observation_marker::EXCHANGE_SNAPSHOT
        );
        let wake = &*format!(
            "{} event=driver_wake task_key=Some((11, 22)) driver_id=9 blocked_generation=7 current_generation=8 block_epoch=41\n",
            novarocks_failpoint::observation_marker::EXCHANGE_SNAPSHOT
        );
        let actual = current_park(park, identity, source).unwrap().unwrap();
        assert_eq!((actual.driver, actual.epoch, actual.generation), (9, 41, 7));
        assert!(
            current_park(&format!("{wake}{park}"), identity, source)
                .unwrap()
                .is_none()
        );
        assert!(
            current_park(&format!("{park}{wake}"), identity, source)
                .unwrap()
                .is_none()
        );
        assert!(
            current_park(park, identity, "source_name=\"EXCHANGE_SOURCE (id=5)\"")
                .unwrap()
                .is_none()
        );
        assert!(
            current_park(
                &park.replace("frozen_generation=7", "frozen_generation=Some(7)"),
                identity,
                source
            )
            .is_err()
        );
        assert!(
            current_park(
                &format!(
                    "{park}{}",
                    wake.replace("blocked_generation=7", "blocked_generation=6")
                ),
                identity,
                source
            )
            .is_err()
        );
        assert!(
            current_park(
                &park.replace("generation=7", "generation=0"),
                identity,
                source
            )
            .unwrap()
            .is_none()
        );
        assert!(
            current_park(
                &park.replace("current_generation=7", "current_generation=8"),
                identity,
                source
            )
            .unwrap()
            .is_none()
        );
    }
}
