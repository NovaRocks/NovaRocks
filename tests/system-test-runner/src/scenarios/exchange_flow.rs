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
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::native_root_reply_fault::{
    ExchangeFaultAction, ExchangeFaultCandidate, ExchangeFaultObservation, ExchangeFaultSelector,
    RootReplyFaultBounds, RootReplyFaultControl,
};
use novarocks_cluster_harness::{
    CrossProcessRootReplyFaultConfig, LaunchProfile, QueryLifecycleStatementObservation,
    ServerHandle,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeSet;
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
        (Err(primary), Err(cleanup)) => {
            bail!("{primary:#}; original Exchange scenario cleanup: {cleanup:#}")
        }
    }
}

/// The scenario owns and joins this original socket job on every path.
struct QueryJob {
    connection_id: u32,
    selected: SelectedStatement,
    observation: QueryLifecycleStatementObservation,
    result: Option<mpsc::Receiver<mysql::Result<Vec<i64>>>>,
    actor: Option<JoinHandle<Result<()>>>,
    user: String,
    port: u16,
    deadline: Instant,
    settled: bool,
}

impl QueryJob {
    fn start(
        context: &mut ScenarioContext,
        selected: SelectedStatement,
        settings: Vec<String>,
    ) -> Result<Self> {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (submit_tx, submit_rx) = mpsc::sync_channel(1);
        let (result_tx, result) = mpsc::sync_channel(1);
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        let deadline = context.deadline();
        let actor_user = user.clone();
        let sql = selected.sql.clone();
        let actor = thread::Builder::new()
            .name("exchange-flow-selected-sql".into())
            .spawn(move || {
                mysql_actor::connect_for_cancellation_until(&actor_user, port, deadline)?.run(
                    |connection| {
                        for setting in settings {
                            connection.query_drop(setting)?;
                        }
                        ready_tx.send(connection.connection_id())?;
                        match submit_rx
                            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                        {
                            Ok(()) => {}
                            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                            Err(error) => return Err(error.into()),
                        }
                        let outcome = connection.query::<i64, _>(&sql);
                        match result_tx.send(outcome) {
                            Ok(()) => Ok(()),
                            Err(mpsc::SendError(Err(actual))) => Err(actual.into()),
                            Err(mpsc::SendError(Ok(_))) => {
                                bail!("original Exchange SQL result owner closed before delivery")
                            }
                        }
                    },
                )
            })?;
        let ready = (|| -> Result<(u32, QueryLifecycleStatementObservation)> {
            let id = ready_rx
                .recv_timeout(context.remaining("receive original Exchange SQL connection")?)?;
            let observation = selected.arm(context, id, &selected.sql)?;
            Ok((id, observation))
        })();
        let (connection_id, observation) = match ready {
            Ok(value) => value,
            Err(primary) => {
                drop(submit_tx);
                return combine(Err(primary), join_actor(actor));
            }
        };
        if let Err(primary) = submit_tx.send(()) {
            let cleanup = combine(
                join_actor(actor),
                context
                    .handle()
                    .drain_query_lifecycle_statement_observation(&observation),
            );
            return combine(Err(primary.into()), cleanup);
        }
        Ok(Self {
            connection_id,
            selected,
            observation,
            result: Some(result),
            actor: Some(actor),
            user,
            port,
            deadline,
            settled: false,
        })
    }

    fn kill(&self) -> Result<()> {
        mysql_actor::connect_until(&self.user, self.port, self.deadline)?.run(|control| {
            control.query_drop(format!("KILL QUERY {}", self.connection_id))?;
            Ok(())
        })
    }

    fn finish(&mut self, context: &mut ScenarioContext, expected: Outcome) -> Result<Value> {
        let receiver = self
            .result
            .take()
            .context("Exchange SQL outcome already consumed")?;
        let mut sql_completed = false;
        let primary = (|| -> Result<Value> {
            let actual = receiver
                .recv_timeout(context.remaining("receive original Exchange SQL outcome")?)?;
            sql_completed = true;
            match (expected, actual) {
                (Outcome::Rows, Ok(mut rows)) => {
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
                        json!({"outcome":"rows","rows":ROWS,"independent_oracle":"every integer 1..=24576 exactly once"}),
                    )
                }
                (Outcome::Deadline, Err(mysql::Error::MySqlError(error))) => {
                    ensure!(
                        error.code == 1317
                            && error.state == "70100"
                            && error
                                .message
                                .starts_with("[query.execution.deadline_exceeded] "),
                        "expected original QueryOwner deadline, got {error}"
                    );
                    Ok(json!({"outcome":"query_deadline","code":error.code,"sqlstate":error.state}))
                }
                (Outcome::Cancelled, Err(mysql::Error::MySqlError(error))) => {
                    ensure!(
                        error.code == 1317
                            && error.state == "70100"
                            && error.message.starts_with("[query.execution.cancelled] "),
                        "expected original QueryOwner cancellation, got {error}"
                    );
                    Ok(
                        json!({"outcome":"query_cancelled","code":error.code,"sqlstate":error.state}),
                    )
                }
                (_, Err(error)) => Err(error.into()),
                (_, Ok(_)) => {
                    bail!("selected Exchange SQL succeeded instead of its declared terminal")
                }
            }
        })();
        let mut cleanup = Ok(());
        if self
            .actor
            .as_ref()
            .is_some_and(|actor| !actor.is_finished())
            && primary.is_err()
            && !sql_completed
        {
            cleanup = self.kill();
            if cleanup.is_err() {
                cleanup = combine(cleanup, context.shutdown());
            }
        }
        if let Some(actor) = self.actor.take() {
            cleanup = combine(cleanup, join_actor(actor));
        }
        let terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(
                &self.observation,
                matches!(expected, Outcome::Rows),
                self.deadline,
            );
        cleanup = combine(cleanup, terminal.map(|_| ()));
        cleanup = combine(
            cleanup,
            context
                .handle()
                .drain_query_lifecycle_statement_observation(&self.observation),
        );
        self.settled = true;
        combine(primary, cleanup)
    }

    fn settle(&mut self, context: &mut ScenarioContext) -> Result<()> {
        if self.settled {
            return Ok(());
        }
        let mut cleanup = Ok(());
        if self
            .actor
            .as_ref()
            .is_some_and(|actor| !actor.is_finished())
        {
            cleanup = self.kill();
            if cleanup.is_err() {
                cleanup = combine(cleanup, context.shutdown());
            }
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
            if !actor.is_finished() {
                let _ = self.kill();
            }
            let _ = join_actor(actor);
        }
    }
}

fn join_actor(actor: JoinHandle<Result<()>>) -> Result<()> {
    actor
        .join()
        .map_err(|_| anyhow::anyhow!("original Exchange SQL actor panicked"))?
}

#[derive(Clone, Copy)]
enum Outcome {
    Rows,
    Deadline,
    Cancelled,
}

fn seed(context: &mut ScenarioContext) -> Result<()> {
    let warehouse = create_warehouse(context, "exchange-flow")?;
    let deadline = context.deadline();
    mysql_actor::connect_until(context.mysql_user(), context.mysql_port(), deadline)?.run(|control| {
        create_catalog(control, CATALOG, &warehouse)?;
        control.query_drop(format!("CREATE DATABASE {CATALOG}.{DATABASE}"))?;
        control.query_drop(format!("CREATE TABLE {TABLE} (k BIGINT NOT NULL)"))?;
        // Independent committed files make the scan eligible on the live BE
        // roster. Actual selected participants are checked, never guessed.
        for file in 0..FILES {
            let first = file * ROWS_PER_FILE + 1;
            let last = first + ROWS_PER_FILE - 1;
            control.query_drop(format!("INSERT INTO {TABLE} SELECT generate_series FROM TABLE(generate_series({first},{last}))"))?;
        }
        Ok(())
    })
}

fn gather_sql() -> String {
    format!("SELECT k FROM {TABLE}")
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
            "[runtime]\nexchange_instance_floor_slots = 1\nexchange_instance_max_slots = 2\nexchange_floating_slots = 1\nexchange_stream_window = 2\nexchange_transport_timeout_ms = {TRANSPORT_MILLIS}\nexchange_io_max_inflight_bytes = 262144\nexchange_max_transmit_batched_bytes = 32768\n"
        ), 1));
        config.native_root_reply_fault = Some(CrossProcessRootReplyFaultConfig {
            backend_indices: (0..3).collect(),
            bounds: RootReplyFaultBounds::default(),
        });
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
                            (index, "grpc-fragment", Some(1)),
                            (index, "exchange-snapshot", Some(1)),
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
        let fault = context.handle().native_exchange_fault(0)?;
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
        // Every job is retained until its original socket thread actually exits.
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

fn explain(context: &mut ScenarioContext, sql: &str, partitioned: bool) -> Result<String> {
    mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?
    .run(|control| {
        for setting in settings(partitioned, 120) {
            control.query_drop(setting)?;
        }
        let lines: Vec<String> = control.query(format!("EXPLAIN VERBOSE {sql}"))?;
        Ok(lines.join("\n"))
    })
}

struct PlanNode<'a> {
    header: &'a str,
    line: usize,
    fragment: u32,
    node: i32,
    depth: usize,
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
    let nodes = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.contains("[fragment="))
        .map(|(line, header)| {
            Ok(PlanNode {
                header,
                line,
                fragment: number_after(header, "[fragment=")?,
                node: number_after(header, "node=")?,
                depth: number_after(header, "depth=")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
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
) -> Result<ExchangeFaultCandidate> {
    loop {
        context
            .remaining("bind actual Exchange candidate to nominated query and live BE roster")?;
        capture_healthy(fault)?;
        if let Some(candidate) = fault.exchange_candidate() {
            let stream = candidate.target.stream();
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
                    "grpc-fragment",
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
                    "exchange-snapshot",
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
    let finst = candidate.target.stream().destination_fragment_instance_id();
    let identity = format!("finst={:?}", Some((finst.high(), finst.low())));
    let source = format!(
        "source_name=\"EXCHANGE_SOURCE (id={})\"",
        candidate.target.stream().destination_node().get()
    );
    let text = context
        .handle()
        .be_stdout_contents(candidate.backend_index, usize::MAX)?;
    // A dispatcher may publish wake before the registering thread flushes
    // its park line. Pair by the actual epoch, never by log arrival order.
    let wakes: BTreeSet<_> = text
        .lines()
        .filter(|line| {
            line.contains("NOVAROCKS_EXCHANGE_SNAPSHOT")
                && line.contains(&identity)
                && line.contains("event=driver_wake ")
        })
        .map(|line| {
            Ok((
                scalar::<i32>(line, "driver_id")?,
                scalar::<u64>(line, "block_epoch")?,
            ))
        })
        .collect::<Result<_>>()?;
    let mut latest = None;
    for line in text
        .lines()
        .filter(|line| line.contains("NOVAROCKS_EXCHANGE_SNAPSHOT") && line.contains(&identity))
    {
        if line.contains("event=driver_park ")
            && line.contains(&source)
            && line.contains("reason=InputEmpty ")
        {
            let park = Park {
                driver: scalar(line, "driver_id")?,
                epoch: scalar(line, "block_epoch")?,
                line: line.to_owned(),
            };
            if !wakes.contains(&(park.driver, park.epoch)) {
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
    mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?
    .run(|control| {
        let actual: Vec<i64> = control.query("SELECT 17")?;
        ensure!(
            actual == [17],
            "short query failed while Exchange positions were held"
        );
        Ok(())
    })
}

fn require_wake(
    context: &mut ScenarioContext,
    candidate: &ExchangeFaultCandidate,
    park: &Park,
) -> Result<()> {
    let finst = candidate.target.stream().destination_fragment_instance_id();
    let identity = format!("finst={:?}", Some((finst.high(), finst.low())));
    let text = context
        .handle()
        .be_stdout_contents(candidate.backend_index, usize::MAX)?;
    ensure!(
        text.lines()
            .any(|line| line.contains("NOVAROCKS_EXCHANGE_SNAPSHOT")
                && line.contains("event=driver_wake ")
                && line.contains(&identity)
                && scalar::<i32>(line, "driver_id").ok() == Some(park.driver)
                && scalar::<u64>(line, "block_epoch").ok() == Some(park.epoch)),
        "held Source's original driver/block epoch never actually woke"
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
            let node = if ordinal == 0 {
                ensure!(
                    plan.contains("destination={") && plan.contains("distribution=singleton"),
                    "gather case has no real singleton Exchange: {plan}"
                );
                None
            } else {
                ensure!(
                    plan.contains(if partitioned {
                        "HASH JOIN (PARTITIONED,"
                    } else {
                        "HASH JOIN (BROADCAST,"
                    }),
                    "join Exchange shape changed: {plan}"
                );
                Some(build_receiver_node(&plan)?)
            };
            retain(
                context,
                &format!("exchange-shape-{ordinal}.json"),
                &json!({"sql":sql,"plan":plan,"held_build_node":node}),
            )?;
            capture(
                context,
                fault,
                ExchangeFaultAction::HoldDataBeforeForward,
                node,
            )?;
            let target = jobs.len();
            jobs.push(QueryJob::start(
                context,
                self.intent.take(ordinal)?,
                settings(partitioned, 120),
            )?);
            let candidate = bind_candidate(context, fault, &jobs[target])?;
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
                json!({"shape":["gather","broadcast","shuffle"][ordinal],"candidate":candidate,
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
        ensure!(
            plan.contains("HASH JOIN (PARTITIONED,"),
            "cross-query case lost shuffle: {plan}"
        );
        let node = build_receiver_node(&plan)?;
        retain(
            context,
            "exchange-probe-build-plan.json",
            &json!({"sql":join_sql(),"plan":plan,"held_build_node":node}),
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
        let candidate = bind_candidate(context, fault, &jobs[0])?;
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
        let before = await_flow_idle(context)?;
        capture(context, fault, ExchangeFaultAction::DropAcceptedReply, None)?;
        jobs.push(QueryJob::start(
            context,
            self.intent.take(0)?,
            settings(false, 120),
        )?);
        let candidate = bind_candidate(context, fault, &jobs[0])?;
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
        capture(
            context,
            fault,
            ExchangeFaultAction::CutGrantSubscription,
            None,
        )?;
        jobs.push(QueryJob::start(
            context,
            self.intent.take(0)?,
            settings(false, 120),
        )?);
        let candidate = bind_candidate(context, fault, &jobs[0])?;
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
        let node = build_receiver_node(&plan)?;
        retain(
            context,
            "exchange-long-wait-plan.json",
            &json!({"sql":join_sql(),"plan":plan,"held_build_node":node}),
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
        let candidate = bind_candidate(context, fault, &jobs[0])?;
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
        let deadline_candidate = bind_candidate(context, fault, &jobs[1])?;
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
        let node = build_receiver_node(&plan)?;
        retain(
            context,
            "exchange-control-plan.json",
            &json!({"sql":join_sql(),"plan":plan,"held_build_node":node}),
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
        let candidate = bind_candidate(context, fault, &jobs[0])?;
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
        jobs[0].kill()?;
        let cancelled = jobs[0].finish(context, Outcome::Cancelled)?;
        // Cancellation does not release the actor's original target lease.
        // Observe its real exit separately before releasing the rule itself.
        let exited = await_fault_exit(context, fault)?;
        ensure!(
            exited.peer_cancels > 0,
            "KILL did not reach the original held RPC owner"
        );
        fault.release_exchange();
        short_query(context)?;
        phase(
            evidence,
            json!({"candidate":candidate,"actual_pressure":pressure,"short_query_before_release":true,
            "healthy_query":other,"kill_query_owner":cancelled,"actual_rpc_exits_before_rule_release":exited}),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            ("exchange_max_transmit_batched_bytes", 32768),
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
        assert!(configured.native_root_reply_fault.is_some());
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
}
