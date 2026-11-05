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

//! Native acceptance for connector scans their pipeline driver polls.
//!
//! A typed scan's CPU work runs on the driver that polls its page stream;
//! while the stream waits on an object read it returns `Pending`, and the
//! driver's worker is free for other work (ADR-0159). On a real 1FE+3BE
//! deployment:
//!
//! * `uea4/scan-producer-readiness`: every BE runs one driver worker. While
//!   a query's scan waits on a held object read -- its Iceberg footer or a
//!   Paimon data file -- an independent query whose only split lands on the
//!   same backend completes before the hold is released. A scan whose one
//!   row group decodes into more pages than a turn allows yields its turn on
//!   its backend. A scan waiting for a split that has not come is not held
//!   here: the frontend enumerates a table's splits before it creates the
//!   tasks, so no backend task exists while its enumeration reads are held.
//! * `uea4/scan-producer-cancel`: `KILL QUERY` while the scan's object read
//!   is held. The query reports its cancellation, and every frontend and
//!   backend query resource converges while the read is still held: a
//!   scan's close stops and observes its reads instead of waiting for their
//!   bytes.
//!
//! Placement is structural, not incidental: every data scan has one task on
//! every backend, and a single split goes to the backend that sorts first by
//! endpoint, so two one-split tables share a backend. The split-assignment
//! marker names the backend each split actually reached.
//!
//! The scenarios read Iceberg tables they create through this worktree's REST
//! catalog and object store, and Paimon tables from the READY fixture named by
//! `NOVAROCKS_PAIMON_FIXTURE_MANIFEST`. Run them with that fixture's
//! `base-server.toml` as the base config: it defines the static object-store
//! credential both providers' catalogs use.

use super::connector::{
    await_resource_convergence, connector_reader_environment, require_three_backends,
    resource_baseline,
};
use super::native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec};
use super::paimon::scan_statement_actor::{ScanActor, combine};
use super::paimon::{Fixture as PaimonFixture, create_paimon_catalog, load_shared_fixture};
use super::uea4_catalog_planning::await_frontend_local_exit;
use crate::actors::sql::{self, SqlSession, StatementFailure, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::delayed_s3::{
    DelayedS3Config, DelayedS3ConnectionEventKind, DelayedS3EventKind, DelayedS3Proxy,
    DelayedS3ReadHold,
};
use novarocks_cluster_harness::{CrossProcessConfigOverlay, LaunchProfile, TestClusterOwner};
use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const DATABASE: &str = "uea4a3_scan";
const DIRECT_ICEBERG: &str = "uea4a3_ice_direct";
const HELD_ICEBERG: &str = "uea4a3_ice_held";
const DIRECT_PAIMON: &str = "uea4a3_paimon_direct";
const HELD_PAIMON: &str = "uea4a3_paimon_held";
const PAIMON_DATABASE: &str = "fixture";
/// Append-only fixture tables first: their reads touch data files directly.
const PAIMON_CANDIDATES: [&str; 5] = [
    "append_none",
    "append_snappy",
    "append_lz4",
    "pk_default",
    "pk_sequence",
];
const SPLIT_MARKER: &str = novarocks_failpoint::observation_marker::TASK_SPLIT_ASSIGNMENT_ACCEPTED;
const HOLD_ENTRY_TIMEOUT: Duration = Duration::from_secs(30);
const MARKER_TIMEOUT: Duration = Duration::from_secs(15);
const MARKER_POLL: Duration = Duration::from_millis(25);
/// Bounds the independent query: a scan that kept its driver's only worker
/// would hold it here, and the query would time out instead of finishing.
const INDEPENDENT_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const HELD_QUERY_EXIT_TIMEOUT: Duration = Duration::from_secs(30);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
/// Rows of the one-file table whose single row group decodes into many more
/// pages than one turn's budget allows.
const LONG_TABLE_ROWS: u64 = 400_000;
/// Caches off, so every read of a file under test is a real object read that
/// the proxy can hold, even after another catalog read the same file.
const NO_CACHE_OVERLAY: &str = "[runtime.cache]\npage_cache_enable = false\nparquet_meta_cache_enable = false\nparquet_page_cache_enable = false\ndatacache_enable = false\n";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(ScanProducerReadiness::default()),
        Box::new(ScanProducerCancel::default()),
    ]
}

/// Backends read data files under the fixture's static credential; the base
/// config defines only the frontend's metadata credential, which the harness
/// keeps off backends.
fn launch_config(driver_workers: usize) -> Result<ScenarioLaunchConfig> {
    let environment = Environment::load()?;
    let (credential, generation) = environment.paimon.credential();
    Ok(ScenarioLaunchConfig {
        child_environment: connector_reader_environment(),
        config_overlay: CrossProcessConfigOverlay {
            be: Some(format!(
                "[runtime]\npipeline_exec_thread_pool_thread_num = {driver_workers}\n{NO_CACHE_OVERLAY}\
                 [[connector.credentials]]\npurpose = \"object-store-data\"\n\
                 name = \"{credential}\"\ngeneration = \"{generation}\"\nkind = \"s3\"\n\
                 access_key_id = \"${{ENV:AWS_S3_ACCESS_KEY_ID}}\"\n\
                 access_key_secret = \"${{ENV:AWS_S3_SECRET_ACCESS_KEY}}\"\n"
            )),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn validate_environment(launch_profile: LaunchProfile) -> Result<()> {
    ensure!(
        launch_profile == LaunchProfile::FaultScenario,
        "UEA-4A-3 scan scenarios require fault-scenario composition and finite reader grants"
    );
    Environment::load().map(|_| ())
}

/// Object-store and catalog endpoints of this worktree's test environment.
struct Environment {
    rest_uri: String,
    warehouse: String,
    s3_endpoint: String,
    paimon: PaimonFixture,
}

impl Environment {
    fn load() -> Result<Self> {
        let required = |name: &str| {
            env::var(name).with_context(|| {
                format!("{name} is required; source docker/iceberg-rest/runtime/current/env.sh")
            })
        };
        Ok(Self {
            rest_uri: required("NOVAROCKS_ICEBERG_REST_URI")?,
            warehouse: required("NOVAROCKS_ICEBERG_REST_WAREHOUSE")?,
            s3_endpoint: required("AWS_S3_ENDPOINT")?,
            paimon: load_shared_fixture("schema")?,
        })
    }

    fn iceberg_catalog_sql(&self, name: &str, s3_endpoint: &str) -> String {
        let (credential, generation) = self.paimon.credential();
        format!(
            "CREATE EXTERNAL CATALOG {name} PROPERTIES(\"type\"=\"iceberg\",\
             \"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{}\",\
             \"iceberg.catalog.warehouse\"=\"{}\",\
             \"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"{}\",\
             \"aws.s3.enable_path_style_access\"=\"true\",\
             \"credential.object-store-metadata.consumer-role\"=\"frontend\",\
             \"credential.object-store-metadata.mode\"=\"static\",\
             \"credential.object-store-metadata.name\"=\"{credential}\",\
             \"credential.object-store-metadata.generation\"=\"{generation}\",\
             \"credential.object-store-data.consumer-role\"=\"backend\",\
             \"credential.object-store-data.mode\"=\"static\",\
             \"credential.object-store-data.name\"=\"{credential}\",\
             \"credential.object-store-data.generation\"=\"{generation}\")",
            sql_string(&self.rest_uri),
            sql_string(&self.warehouse),
            sql_string(s3_endpoint),
            sql_string(self.paimon.region()),
        )
    }
}

/// The catalogs of one scenario run: each provider is reachable directly and
/// through the proxy that can hold its object reads.
struct Catalogs {
    proxy: DelayedS3Proxy,
}

impl Catalogs {
    fn create(context: &mut ScenarioContext, with_paimon: bool) -> Result<Self> {
        let environment = Environment::load()?;
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: environment.s3_endpoint.clone(),
            delay: Duration::ZERO,
        })?;
        let mut control = connect(context, "connect catalog setup session")?;
        for (name, endpoint) in [
            (DIRECT_ICEBERG, environment.s3_endpoint.clone()),
            (HELD_ICEBERG, proxy.endpoint().to_owned()),
        ] {
            control
                .execute(format!("DROP CATALOG IF EXISTS {name}"))
                .map(|_| ())
                .with_context(|| format!("remove stale catalog {name}"))?;
            control
                .execute(environment.iceberg_catalog_sql(name, &endpoint))
                .map(|_| ())
                .with_context(|| format!("create Iceberg catalog {name}"))?;
        }
        control
            .execute(format!(
                "CREATE DATABASE IF NOT EXISTS {DIRECT_ICEBERG}.{DATABASE}"
            ))
            .map(|_| ())
            .context("create the scenario's Iceberg namespace")?;
        if with_paimon {
            create_paimon_catalog(&mut control, DIRECT_PAIMON, &environment.paimon)?;
            let mut held = environment.paimon.clone();
            held.endpoint = proxy.endpoint().to_owned();
            create_paimon_catalog(&mut control, HELD_PAIMON, &held)?;
        }
        Ok(Self { proxy })
    }
}

/// One Iceberg table of exactly one data file, created through the direct
/// catalog and read through either catalog.
struct IcebergTable {
    name: &'static str,
    rows: u64,
    /// Object path tail of the table's one data file.
    data_file_suffix: String,
}

impl IcebergTable {
    fn create(control: &mut SqlSession, name: &'static str, rows: u64) -> Result<Self> {
        let qualified = format!("{DIRECT_ICEBERG}.{DATABASE}.{name}");
        control
            .execute(format!("DROP TABLE IF EXISTS {qualified}"))
            .map(|_| ())
            .with_context(|| format!("drop stale {qualified}"))?;
        control
            .execute(format!("CREATE TABLE {qualified} (v BIGINT)"))
            .map(|_| ())
            .with_context(|| format!("create {qualified}"))?;
        // The writer starts a file for every chunk it is given. A sort is
        // gathered onto one driver and emits its input as one chunk, so the
        // ORDER BY is what makes this one data file of one row group.
        control
            .execute(format!(
                "INSERT INTO {qualified} SELECT generate_series FROM \
                 TABLE(generate_series(1, {rows})) ORDER BY generate_series"
            ))
            .map(|_| ())
            .with_context(|| format!("write {qualified}"))?;
        let files: Vec<String> = control
            .execute(format!(
                "SELECT file_path FROM {qualified}$files WHERE content = 0"
            ))
            .and_then(file_paths)
            .with_context(|| format!("list {qualified} data files"))?;
        let [file] = files.as_slice() else {
            bail!(
                "{qualified} must be one data file so its read is one split; found {}",
                files.len()
            );
        };
        Ok(Self {
            name,
            rows,
            data_file_suffix: path_tail(file)?,
        })
    }

    fn qualified(&self, catalog: &str) -> String {
        format!("{catalog}.{DATABASE}.{}", self.name)
    }

    fn count_and_sum_sql(&self, catalog: &str) -> String {
        format!("SELECT COUNT(*), SUM(v) FROM {}", self.qualified(catalog))
    }

    fn expected_count_and_sum(&self) -> Rows {
        let rows = i64::try_from(self.rows).expect("fixture rows fit i64");
        Rows {
            signatures: vec![novarocks_test_trino_client::TypeSignature::simple("bigint"); 2],
            rows: vec![vec![
                TrinoValue::Integer(rows),
                TrinoValue::Integer(rows * (rows + 1) / 2),
            ]],
        }
    }
}

/// A Paimon fixture table whose read is exactly one split. Its query selects
/// every column, so it reads the table's data files.
struct PaimonTable {
    name: &'static str,
    expected: Rows,
}

impl PaimonTable {
    fn sql(name: &str, catalog: &str) -> String {
        format!("SELECT * FROM {catalog}.{PAIMON_DATABASE}.{name}")
    }
}

/// The `/<file name>` tail an object request path ends with.
fn path_tail(location: &str) -> Result<String> {
    location
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .map(|name| format!("/{name}"))
        .with_context(|| format!("object location {location} has no file name"))
}

fn connect(context: &ScenarioContext, operation: &str) -> Result<SqlSession> {
    context.remaining(operation)?;
    SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())
        .map_err(Into::into)
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Preserve actual type declarations, NULL and typed values. Catalog names are
/// intentionally excluded; unordered comparison retains duplicate rows.
#[derive(Clone, Debug, PartialEq)]
struct Rows {
    signatures: Vec<novarocks_test_trino_client::TypeSignature>,
    rows: Vec<Vec<TrinoValue>>,
}
impl Rows {
    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    fn len(&self) -> usize {
        self.rows.len()
    }
}
fn value_order(left: &TrinoValue, right: &TrinoValue) -> std::cmp::Ordering {
    use TrinoValue::*;
    let tag = |value: &TrinoValue| match value {
        Null => 0,
        Boolean(_) => 1,
        Integer(_) => 2,
        Real(_) => 3,
        Double(_) => 4,
        Text(_) => 5,
        Binary(_) => 6,
        Array(_) => 7,
        Row(_) => 8,
        Map(_) => 9,
        Variant(_) => 10,
    };
    match (left, right) {
        (Null, Null) => std::cmp::Ordering::Equal,
        (Boolean(a), Boolean(b)) => a.cmp(b),
        (Integer(a), Integer(b)) => a.cmp(b),
        (Real(a), Real(b)) => a.total_cmp(b),
        (Double(a), Double(b)) => a.total_cmp(b),
        (Text(a), Text(b)) | (Variant(a), Variant(b)) => a.cmp(b),
        (Binary(a), Binary(b)) => a.cmp(b),
        (Array(a), Array(b)) | (Row(a), Row(b)) => row_order(a, b),
        (Map(a), Map(b)) => a
            .iter()
            .zip(b)
            .map(|((ak, av), (bk, bv))| value_order(ak, bk).then_with(|| value_order(av, bv)))
            .find(|order| !order.is_eq())
            .unwrap_or_else(|| a.len().cmp(&b.len())),
        _ => tag(left).cmp(&tag(right)),
    }
}
fn row_order(left: &[TrinoValue], right: &[TrinoValue]) -> std::cmp::Ordering {
    left.iter()
        .zip(right)
        .map(|(a, b)| value_order(a, b))
        .find(|order| !order.is_eq())
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}
fn query_rows(result: StatementResult) -> std::result::Result<Rows, StatementFailure> {
    if !result.is_result_set() {
        return Err(StatementFailure::Protocol(format!(
            "expected scan result set, got {result:?}"
        )));
    }
    let mut rows = result.rows;
    rows.sort_by(|a, b| row_order(a, b));
    Ok(Rows {
        signatures: result
            .columns
            .into_iter()
            .map(|column| column.type_signature)
            .collect(),
        rows,
    })
}
fn file_paths(result: StatementResult) -> std::result::Result<Vec<String>, StatementFailure> {
    if !result.is_result_set()
        || result.columns.len() != 1
        || result.columns[0].type_signature.raw_type != "varchar"
    {
        return Err(StatementFailure::Protocol(format!(
            "expected one text file_path column, got {result:?}"
        )));
    }
    result
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Text(path)] => Ok(path.clone()),
            other => Err(StatementFailure::Protocol(format!(
                "expected non-null file_path, got {other:?}"
            ))),
        })
        .collect()
}
/// Arm the original POST before its first GET; retain its exact cancellation
/// capability even when the client has already returned a local reader failure.
fn selected_result(
    context: &mut ScenarioContext,
    session: &mut SqlSession,
    selected: SelectedStatement,
    sql: &str,
) -> Result<StatementResult> {
    let pending = session.submit(sql)?;
    let cancel = pending.cancel_handle();
    let (pending, observation) = selected.arm_pending(context, pending, sql)?;
    let result = session.run_pending(pending);
    let cancelled = if matches!(&result, Ok(_) | Err(StatementFailure::Trino(_))) {
        Ok(())
    } else {
        cancel
            .delete(context.deadline())
            .map_err(anyhow::Error::from)
    };
    let result = combine(result.map_err(anyhow::Error::from), cancelled);
    let deadline = context.deadline();
    let drained = context
        .handle()
        .drain_query_lifecycle_statement_binding(&observation, deadline);
    combine(result, drained)
}

// ------------------------------------------------------------------ evidence

fn log_offsets(context: &mut ScenarioContext) -> Result<Vec<usize>> {
    let backends = context.handle().be_count();
    (0..backends)
        .map(|index| {
            context
                .handle()
                .be_log_contents(index)
                .map(|log| log.len())
                .with_context(|| format!("read BE[{index}] log"))
        })
        .collect()
}

/// Backend log lines written since `offsets`, tagged with their backend.
fn lines_since(context: &mut ScenarioContext, offsets: &[usize]) -> Result<Vec<(usize, String)>> {
    let mut lines = Vec::new();
    for (index, offset) in offsets.iter().enumerate() {
        let log = context
            .handle()
            .be_log_contents(index)
            .with_context(|| format!("read BE[{index}] log"))?;
        // A rotated log restarts below the offset: everything in it is new.
        let tail = log.get(*offset..).unwrap_or(log.as_str());
        lines.extend(tail.lines().map(|line| (index, line.to_owned())));
    }
    Ok(lines)
}

fn marker_field<'a>(line: &'a str, marker: &str, field: &str) -> Option<&'a str> {
    let (_, rest) = line.split_once(marker)?;
    rest.split_whitespace()
        .find_map(|token| token.strip_prefix(field)?.strip_prefix('='))
}

/// Where the splits of executions that started after `offsets` went: for
/// each execution, the backends that were given at least one split and how
/// many.
fn split_placements(lines: &[(usize, String)]) -> BTreeMap<String, BTreeMap<usize, u64>> {
    let mut placements: BTreeMap<String, BTreeMap<usize, u64>> = BTreeMap::new();
    for (backend, line) in lines {
        let (Some(execution), Some(enqueued)) = (
            marker_field(line, SPLIT_MARKER, "execution_id"),
            marker_field(line, SPLIT_MARKER, "enqueued").and_then(|n| n.parse::<u64>().ok()),
        ) else {
            continue;
        };
        if enqueued > 0 {
            *placements
                .entry(execution.to_owned())
                .or_default()
                .entry(*backend)
                .or_default() += enqueued;
        }
    }
    placements
}

#[derive(Clone, Debug, Serialize)]
struct SplitPlacement {
    execution_id: String,
    backend: usize,
}

/// Waits until exactly one execution that started after `offsets` has been
/// given splits, and returns the one backend its one split reached.
fn await_single_split(
    context: &mut ScenarioContext,
    offsets: &[usize],
    subject: &str,
) -> Result<SplitPlacement> {
    let deadline = Instant::now() + MARKER_TIMEOUT;
    loop {
        let placements = split_placements(&lines_since(context, offsets)?);
        if let Some((execution, backends)) = placements.iter().next() {
            ensure!(
                placements.len() == 1,
                "{subject}: expected one execution to receive splits, saw {placements:?}"
            );
            let total: u64 = backends.values().sum();
            ensure!(
                backends.len() == 1 && total == 1,
                "{subject}: expected exactly one split on one backend, saw {backends:?}"
            );
            let backend = *backends.keys().next().expect("one backend");
            return Ok(SplitPlacement {
                execution_id: execution.clone(),
                backend,
            });
        }
        ensure!(
            Instant::now() < deadline,
            "{subject}: no split assignment reached a backend within {MARKER_TIMEOUT:?}"
        );
        thread::sleep(MARKER_POLL);
    }
}

// ------------------------------------------------------------- held queries

/// A query running on its own connection while one of its object reads is
/// held.
type RunningQuery = ScanActor<Rows>;

fn still_running(held: &RunningQuery) -> Result<bool> {
    match held.done.try_recv() {
        Err(mpsc::TryRecvError::Empty) => Ok(true),
        Err(mpsc::TryRecvError::Disconnected) => {
            bail!("held query actor exited without a result")
        }
        Ok(Err(error)) => {
            Err(anyhow::Error::new(error).context("held query failed while its read was held"))
        }
        Ok(Ok(rows)) => bail!("held query ended while its read was held: {rows:?}"),
    }
}

/// Runs `sql` on a fresh connection whose every read is bounded, so a query
/// that cannot make progress fails instead of hanging the scenario.
fn run_independent(
    context: &mut ScenarioContext,
    sql: &str,
    selected: Option<SelectedStatement>,
) -> Result<(Rows, Duration)> {
    let started = Instant::now();
    let deadline = started
        .checked_add(INDEPENDENT_QUERY_TIMEOUT)
        .context("independent query deadline overflow")?
        .min(context.deadline());
    let mut connection = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
    let result = if let Some(selected) = selected {
        selected_result(context, &mut connection, selected, sql)?
    } else {
        connection.execute(sql)?
    };
    let rows = query_rows(result)
        .with_context(|| format!("independent query `{sql}` did not complete"))?;
    Ok((rows, started.elapsed()))
}

/// The request the proxy is holding: arrived, and not yet started upstream.
fn held_request_connection(proxy: &DelayedS3Proxy) -> Option<u64> {
    let events = proxy.event_log();
    let started: std::collections::BTreeSet<u64> = events
        .iter()
        .filter(|event| event.kind == DelayedS3EventKind::UpstreamStarted)
        .map(|event| event.request_id)
        .collect();
    let mut held = events
        .iter()
        .filter(|event| {
            event.kind == DelayedS3EventKind::Arrived && !started.contains(&event.request_id)
        })
        .map(|event| event.connection_id);
    let connection = held.next()?;
    held.next().is_none().then_some(connection)
}

fn connection_closed(proxy: &DelayedS3Proxy, connection: u64) -> bool {
    proxy.connection_log().iter().any(|event| {
        event.connection_id == connection && event.kind == DelayedS3ConnectionEventKind::Closed
    })
}

fn write_report(context: &ScenarioContext, file: &str, report: &impl Serialize) -> Result<()> {
    fs::write(
        context.scenario_root().join(file),
        serde_json::to_vec_pretty(report)?,
    )
    .with_context(|| format!("write {file}"))
}

// ------------------------------------------------------------- readiness

#[derive(Default)]
struct ScanProducerReadiness {
    intent: SqlIntentOwner,
}

const HELD_ICE_STATEMENT: usize = PAIMON_CANDIDATES.len();
const HELD_PAIMON_STATEMENTS: usize = HELD_ICE_STATEMENT + 1;
const INDEPENDENT_ICE_STATEMENT: usize = HELD_PAIMON_STATEMENTS + PAIMON_CANDIDATES.len();
const INDEPENDENT_PAIMON_STATEMENT: usize = INDEPENDENT_ICE_STATEMENT + 1;
const BUDGET_YIELD_STATEMENT: usize = INDEPENDENT_PAIMON_STATEMENT + 1;

fn candidate_sqls() -> Vec<String> {
    PAIMON_CANDIDATES
        .into_iter()
        .map(|name| PaimonTable::sql(name, DIRECT_PAIMON))
        .collect()
}
fn readiness_sqls() -> Vec<String> {
    let mut sqls = candidate_sqls();
    sqls.push(format!(
        "SELECT COUNT(*), SUM(v) FROM {HELD_ICEBERG}.{DATABASE}.t_open"
    ));
    sqls.extend(
        PAIMON_CANDIDATES
            .into_iter()
            .map(|name| PaimonTable::sql(name, HELD_PAIMON)),
    );
    let independent = format!("SELECT COUNT(*), SUM(v) FROM {DIRECT_ICEBERG}.{DATABASE}.t_peer");
    // The same source is executed in two distinct readiness phases.
    sqls.extend([independent.clone(), independent]);
    sqls.push(format!(
        "SELECT COUNT(*) FROM {DIRECT_ICEBERG}.{DATABASE}.t_long WHERE v * 3 = 1"
    ));
    sqls
}
fn scan_specs(sqls: Vec<String>, cluster_size: usize) -> Vec<StatementSpec> {
    sqls.into_iter()
        .map(|sql| StatementSpec {
            sql,
            faults: vec![],
            observations: (0..cluster_size)
                .map(|index| {
                    (
                        index,
                        novarocks_failpoint::observation_marker::ObservationAction::ReaderMarker
                            .as_str(),
                        None,
                    )
                })
                .collect(),
        })
        .collect()
}

#[derive(Serialize)]
struct ReadinessPhase {
    phase: &'static str,
    /// The waiting scan's one split and the backend it runs on.
    held: SplitPlacement,
    independent: SplitPlacement,
    independent_millis: u128,
}

#[derive(Serialize)]
struct BudgetYieldEvidence {
    placement: SplitPlacement,
    budget_yields_before: f64,
    budget_yields_after: f64,
    waits_before: f64,
    waits_after: f64,
}

#[derive(Serialize)]
struct ReadinessReport {
    driver_workers_per_backend: usize,
    phases: Vec<ReadinessPhase>,
    budget_yield: BudgetYieldEvidence,
    paimon_table: &'static str,
}

enum Hold<'a> {
    /// The next read of this object path tail.
    Object(&'a str),
    /// The next read of any object with this extension.
    Extension(&'static str),
}

impl Hold<'_> {
    fn arm(&self, proxy: &DelayedS3Proxy) -> Result<DelayedS3ReadHold> {
        match self {
            Self::Object(suffix) => proxy.hold_next_read_with_path_suffix(suffix),
            Self::Extension(extension) => proxy.hold_next_read_with_suffix(extension),
        }
    }
}

struct ReadinessCase<'a> {
    phase: &'static str,
    hold: Hold<'a>,
    held_sql: String,
    held_expected: Rows,
    independent_sql: String,
    independent_expected: Rows,
    held_statement: SelectedStatement,
    independent_statement: SelectedStatement,
}

fn run_readiness_case(
    context: &mut ScenarioContext,
    proxy: &DelayedS3Proxy,
    case: ReadinessCase<'_>,
) -> Result<ReadinessPhase> {
    let phase = case.phase;
    context.action(format!(
        "{phase}: hold the scan's object read, then run an independent scan on the same backend"
    ));
    let offsets = log_offsets(context)?;
    let hold = case.hold.arm(proxy)?;
    let held = RunningQuery::start(
        context,
        case.held_sql,
        Some(case.held_statement),
        query_rows,
    )?;
    let operation = (|| -> Result<ReadinessPhase> {
        hold.wait_until_entered(HOLD_ENTRY_TIMEOUT)
            .with_context(|| format!("{phase}: the held query's object read never arrived"))?;
        let held_split = await_single_split(context, &offsets, phase)?;
        ensure!(
            still_running(&held)?,
            "{phase}: the held query is not running"
        );
        let independent_offsets = log_offsets(context)?;
        let (observed, elapsed) = run_independent(
            context,
            &case.independent_sql,
            Some(case.independent_statement),
        )?;
        ensure!(
            observed == case.independent_expected,
            "{phase}: independent query returned {observed:?}, expected {:?}",
            case.independent_expected
        );
        let independent = await_single_split(context, &independent_offsets, phase)?;
        ensure!(
            independent.backend == held_split.backend,
            "{phase}: the independent split reached BE[{}], not the held scan's BE[{}]",
            independent.backend,
            held_split.backend
        );
        ensure!(
            still_running(&held)?,
            "{phase}: the held query finished before its read was released"
        );
        context.action(format!(
            "{phase}: independent scan on BE[{}] finished in {} ms while the held read waited",
            independent.backend,
            elapsed.as_millis()
        ));
        Ok(ReadinessPhase {
            phase,
            held: held_split,
            independent,
            independent_millis: elapsed.as_millis(),
        })
    })();
    hold.release();
    let completion = (|| -> Result<()> {
        let observed = held
            .done
            .recv_timeout(HELD_QUERY_EXIT_TIMEOUT)
            .context("held query did not end after its read was released")?
            .with_context(|| format!("{phase}: held query failed after release"))?;
        ensure!(
            observed == case.held_expected,
            "{phase}: held query returned {observed:?}, expected {:?}",
            case.held_expected
        );
        Ok(())
    })();
    held.settle(context, combine(operation, completion))
}

/// Picks a Paimon fixture table whose read is one split, reading it through
/// the direct catalog.
fn one_split_paimon_table(
    context: &mut ScenarioContext,
    intent: &SqlIntentOwner,
) -> Result<PaimonTable> {
    let mut control = connect(context, "connect Paimon probe session")?;
    for (index, name) in PAIMON_CANDIDATES.into_iter().enumerate() {
        let offsets = log_offsets(context)?;
        let sql = PaimonTable::sql(name, DIRECT_PAIMON);
        let expected = query_rows(selected_result(
            context,
            &mut control,
            intent.take(index)?,
            &sql,
        )?)
        .with_context(|| format!("probe Paimon fixture table {name}"))?;
        // Only the probe runs, so every placement seen belongs to it.
        thread::sleep(Duration::from_millis(200));
        let placements = split_placements(&lines_since(context, &offsets)?);
        let splits: u64 = placements
            .values()
            .flat_map(|backends| backends.values())
            .sum();
        if !expected.is_empty() && placements.len() == 1 && splits == 1 {
            context.action(format!(
                "Paimon fixture table {name} reads as one split ({} rows)",
                expected.len()
            ));
            return Ok(PaimonTable { name, expected });
        }
    }
    bail!("no Paimon fixture table in {PAIMON_CANDIDATES:?} reads as exactly one split")
}

impl Scenario for ScanProducerReadiness {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "uea4_scan_readiness",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "uea4/scan-producer-readiness"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, launch_profile: LaunchProfile) -> Result<()> {
        validate_environment(launch_profile)
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        launch_config(1)
    }

    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            scan_specs(readiness_sqls(), cluster_size),
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalogs = Catalogs::create(context, true)?;
        let proxy = &catalogs.proxy;
        let mut control = connect(context, "connect table setup session")?;
        let open = IcebergTable::create(&mut control, "t_open", 1_000)?;
        let peer = IcebergTable::create(&mut control, "t_peer", 2_000)?;
        let long = IcebergTable::create(&mut control, "t_long", LONG_TABLE_ROWS)?;
        drop(control);
        let paimon = one_split_paimon_table(context, &self.intent)?;
        let paimon_index = PAIMON_CANDIDATES
            .iter()
            .position(|name| *name == paimon.name)
            .context("selected Paimon table is outside the finite candidate list")?;
        let independent_sql = peer.count_and_sum_sql(DIRECT_ICEBERG);
        let independent_expected = peer.expected_count_and_sum();

        let phases = vec![
            run_readiness_case(
                context,
                proxy,
                ReadinessCase {
                    phase: "iceberg-footer",
                    hold: Hold::Object(&open.data_file_suffix),
                    held_sql: open.count_and_sum_sql(HELD_ICEBERG),
                    held_expected: open.expected_count_and_sum(),
                    independent_sql: independent_sql.clone(),
                    independent_expected: independent_expected.clone(),
                    held_statement: self.intent.take(HELD_ICE_STATEMENT)?,
                    independent_statement: self.intent.take(INDEPENDENT_ICE_STATEMENT)?,
                },
            )?,
            run_readiness_case(
                context,
                proxy,
                ReadinessCase {
                    phase: "paimon-data",
                    hold: Hold::Extension(".parquet"),
                    held_sql: PaimonTable::sql(paimon.name, HELD_PAIMON),
                    held_expected: paimon.expected.clone(),
                    independent_sql,
                    independent_expected,
                    held_statement: self.intent.take(HELD_PAIMON_STATEMENTS + paimon_index)?,
                    independent_statement: self.intent.take(INDEPENDENT_PAIMON_STATEMENT)?,
                },
            )?,
        ];

        context.action("run an all-filtered scan of one large row group and count its yields");
        let offsets = log_offsets(context)?;
        let backends = context.handle().be_count();
        let mut before = Vec::with_capacity(backends);
        for index in 0..backends {
            before.push((
                context
                    .handle()
                    .backend_scan_stream_pending(index, "budget_yield")?,
                context
                    .handle()
                    .backend_scan_stream_pending(index, "wait")?,
            ));
        }
        let mut control = connect(context, "connect budget-yield session")?;
        // `v * 3 = 1` holds for no integer and no statistic can prune it, so
        // every page is decoded and filtered to nothing.
        let sql = format!(
            "SELECT COUNT(*) FROM {} WHERE v * 3 = 1",
            long.qualified(DIRECT_ICEBERG)
        );
        let result = selected_result(
            context,
            &mut control,
            self.intent.take(BUDGET_YIELD_STATEMENT)?,
            &sql,
        )?;
        let matched = Some(sql::single_integer(&result)?);
        ensure!(
            matched == Some(0),
            "all-filtered scan matched {matched:?} rows"
        );
        let placement = await_single_split(context, &offsets, "budget-yield scan")?;
        let after = (
            context
                .handle()
                .backend_scan_stream_pending(placement.backend, "budget_yield")?,
            context
                .handle()
                .backend_scan_stream_pending(placement.backend, "wait")?,
        );
        let (yields_before, waits_before) = before[placement.backend];
        ensure!(
            after.0 > yields_before,
            "BE[{}] counted no budget yield for a scan of {LONG_TABLE_ROWS} rows in one row group",
            placement.backend
        );
        context.action(format!(
            "BE[{}] counted {} budget yields for the all-filtered scan",
            placement.backend,
            after.0 - yields_before
        ));

        write_report(
            context,
            "uea4a3-scan-readiness.json",
            &ReadinessReport {
                driver_workers_per_backend: 1,
                phases,
                budget_yield: BudgetYieldEvidence {
                    placement,
                    budget_yields_before: yields_before,
                    budget_yields_after: after.0,
                    waits_before,
                    waits_after: after.1,
                },
                paimon_table: paimon.name,
            },
        )
    }
}

// ------------------------------------------------------------------ cancel

#[derive(Default)]
struct ScanProducerCancel {
    intent: SqlIntentOwner,
}

#[derive(Serialize)]
struct CancelPhase {
    phase: &'static str,
    kill_to_error_millis: u128,
    converged_while_held: bool,
    independent_millis: u128,
    /// Auxiliary: the proxy saw the held request's connection close before
    /// the read was released. The owners' convergence above is the proof.
    held_connection_closed_before_release: Option<bool>,
}

fn run_cancel_case(
    context: &mut ScenarioContext,
    proxy: &DelayedS3Proxy,
    phase: &'static str,
    hold: Hold<'_>,
    held_sql: String,
    independent_sql: &str,
    independent_expected: &Rows,
) -> Result<CancelPhase> {
    context.action(format!(
        "{phase}: hold the scan's object read, KILL the query, converge while held"
    ));
    let baseline = resource_baseline(context)?;
    let hold = hold.arm(proxy)?;
    let held = RunningQuery::start(context, held_sql, None, query_rows)?;
    let operation = (|| -> Result<CancelPhase> {
        hold.wait_until_entered(HOLD_ENTRY_TIMEOUT)
            .with_context(|| format!("{phase}: the held query's object read never arrived"))?;
        let held_connection = held_request_connection(proxy);
        ensure!(
            still_running(&held)?,
            "{phase}: the held query is not running"
        );
        let killed = Instant::now();
        held.cancel()
            .with_context(|| format!("{phase}: DELETE original query"))?;
        let result = held
            .done
            .recv_timeout(HELD_QUERY_EXIT_TIMEOUT)
            .with_context(|| {
                format!("{phase}: the cancelled query did not end while its read was held")
            })?;
        let kill_to_error = killed.elapsed();
        let error = result
            .err()
            .context("cancelled query unexpectedly succeeded while its read was held")?;
        if !matches!(&error, StatementFailure::Trino(error) if error.error_code == 3 && error.error_name == "USER_CANCELED" && error.error_type == "USER_ERROR")
        {
            return Err(anyhow::Error::new(error).context(format!(
                "{phase}: the cancelled query did not report its cancellation"
            )));
        }
        // The read is still held: every owner must finish without its bytes.
        await_frontend_local_exit(context, &format!("{phase} while held"))?;
        let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
        context
            .handle()
            .await_query_execution_resource_convergence(&baseline, deadline)
            .with_context(|| {
                format!("{phase}: query resources did not converge while the read was held")
            })?;
        let (observed, elapsed) = run_independent(context, independent_sql, None)?;
        ensure!(
            &observed == independent_expected,
            "{phase}: independent query returned {observed:?}, expected {independent_expected:?}"
        );
        let closed = held_connection.map(|connection| connection_closed(proxy, connection));
        context.action(format!(
            "{phase}: cancelled in {} ms and converged while the read was held",
            kill_to_error.as_millis()
        ));
        Ok(CancelPhase {
            phase,
            kill_to_error_millis: kill_to_error.as_millis(),
            converged_while_held: true,
            independent_millis: elapsed.as_millis(),
            held_connection_closed_before_release: closed,
        })
    })();
    hold.release();
    let report = held.settle(context, operation)?;
    await_resource_convergence(context, &baseline, &format!("{phase} after release"))?;
    Ok(report)
}

impl Scenario for ScanProducerCancel {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "uea4_scan_readiness",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "uea4/scan-producer-cancel"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, launch_profile: LaunchProfile) -> Result<()> {
        validate_environment(launch_profile)
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        launch_config(1)
    }

    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            scan_specs(candidate_sqls(), cluster_size),
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalogs = Catalogs::create(context, true)?;
        let proxy = &catalogs.proxy;
        let mut control = connect(context, "connect table setup session")?;
        let cancel = IcebergTable::create(&mut control, "t_cancel", 1_000)?;
        let peer = IcebergTable::create(&mut control, "t_peer", 2_000)?;
        drop(control);
        let paimon = one_split_paimon_table(context, &self.intent)?;
        let independent_sql = peer.count_and_sum_sql(DIRECT_ICEBERG);
        let independent_expected = peer.expected_count_and_sum();

        let phases = vec![
            run_cancel_case(
                context,
                proxy,
                "iceberg-footer",
                Hold::Object(&cancel.data_file_suffix),
                cancel.count_and_sum_sql(HELD_ICEBERG),
                &independent_sql,
                &independent_expected,
            )?,
            run_cancel_case(
                context,
                proxy,
                "paimon-data",
                Hold::Extension(".parquet"),
                PaimonTable::sql(paimon.name, HELD_PAIMON),
                &independent_sql,
                &independent_expected,
            )?,
        ];
        write_report(context, "uea4a3-scan-cancel.json", &phases)
    }
}

// ------------------------------------------------------------------ fanout

// Retained while Paimon-dependent scenario tests are suspended.
#[cfg(all(test, any()))]
mod tests {
    use super::*;

    #[test]
    fn readiness_grants_fit_one_role_and_distinguish_repeated_peer_queries() {
        let binary = std::env::current_exe().unwrap();
        let owner = SqlIntentOwner::default();
        let sqls = readiness_sqls();
        assert_eq!(sqls.len(), 14);
        let plan = owner
            .freeze(
                scan_specs(sqls, 3),
                &[],
                3,
                &binary,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        for launch in plan.planned_launch_summaries().as_array().unwrap() {
            let grants = launch["grants"].as_array().unwrap();
            assert!(grants.len() <= novarocks_failpoint::session::MAX_GRANTS);
            if launch["role"] == "fe" {
                assert_eq!(grants.len(), 43);
            } else {
                assert_eq!(grants.len(), 14);
                assert!(grants.iter().all(|grant| {
                    grant["action"]
                        == novarocks_failpoint::observation_marker::ObservationAction::ReaderMarker
                            .as_str()
                }));
            }
        }
        let first = owner.take(INDEPENDENT_ICE_STATEMENT).unwrap();
        let second = owner.take(INDEPENDENT_PAIMON_STATEMENT).unwrap();
        assert_eq!(first.sql, second.sql);
        assert_ne!(first.token, second.token);
    }

    #[test]
    fn cancel_scene_observes_only_five_bounded_candidate_probes() {
        let binary = std::env::current_exe().unwrap();
        let owner = SqlIntentOwner::default();
        let plan = owner
            .freeze(
                scan_specs(candidate_sqls(), 3),
                &[],
                3,
                &binary,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        for launch in plan.planned_launch_summaries().as_array().unwrap() {
            let grants = launch["grants"].as_array().unwrap();
            if launch["role"] == "fe" {
                assert_eq!(grants.len(), 16);
            } else {
                assert_eq!(grants.len(), 5);
            }
        }
        assert!(owner.take(HELD_ICE_STATEMENT).is_err());
    }

    fn line(backend: usize, text: &str) -> (usize, String) {
        (backend, format!("2026-09-25 12:00:00 {text}"))
    }

    #[test]
    fn split_placements_count_only_enqueued_splits_per_execution() {
        let lines = [
            line(
                0,
                &*format!(
                    "{} execution_id=-1:7:1 task_key=1:2 plan_node=3 enqueued=1 duplicate=0 accepted_through=1",
                    novarocks_failpoint::observation_marker::TASK_SPLIT_ASSIGNMENT_ACCEPTED
                ),
            ),
            line(
                1,
                &*format!(
                    "{} execution_id=-1:7:1 task_key=1:3 plan_node=3 enqueued=0 duplicate=0 accepted_through=0",
                    novarocks_failpoint::observation_marker::TASK_SPLIT_ASSIGNMENT_ACCEPTED
                ),
            ),
            line(
                2,
                &*format!(
                    "{} execution_id=-1:7:1 task_key=1:4 plan_node=3",
                    novarocks_failpoint::observation_marker::TASK_SPLIT_NO_MORE
                ),
            ),
        ];
        let placements = split_placements(&lines);
        assert_eq!(placements.len(), 1);
        assert_eq!(placements["-1:7:1"], BTreeMap::from([(0, 1)]));
    }

    #[test]
    fn marker_fields_come_from_the_marker_line_only() {
        let text = &*format!(
            "prefix enqueued=9 {} execution_id=a enqueued=2",
            novarocks_failpoint::observation_marker::TASK_SPLIT_ASSIGNMENT_ACCEPTED
        );
        assert_eq!(marker_field(text, SPLIT_MARKER, "enqueued"), Some("2"));
        assert_eq!(
            marker_field("no marker enqueued=1", SPLIT_MARKER, "enqueued"),
            None
        );
    }
}
