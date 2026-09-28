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
//! * `uea4/scan-producer-fanout`: with several driver workers, a scan whose
//!   downstream is CPU-bound uses more than one core on its backend once the
//!   pipeline is wider than one, because the scan hands its chunks to
//!   consumer drivers instead of running the downstream itself.
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
use super::paimon::{Fixture as PaimonFixture, create_paimon_catalog, load_shared_fixture};
use super::uea4_catalog_planning::await_frontend_local_exit;
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::delayed_s3::{
    DelayedS3Config, DelayedS3ConnectionEventKind, DelayedS3EventKind, DelayedS3Proxy,
    DelayedS3ReadHold,
};
use novarocks_cluster_harness::process_resources::ProcessResourceSampler;
use novarocks_cluster_harness::{CrossProcessConfigOverlay, LaunchProfile, ServerHandle};
use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
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
const SPLIT_MARKER: &str = "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED";
const HOLD_ENTRY_TIMEOUT: Duration = Duration::from_secs(30);
const MARKER_TIMEOUT: Duration = Duration::from_secs(15);
const MARKER_POLL: Duration = Duration::from_millis(25);
/// Bounds the independent query: a scan that kept its driver's only worker
/// would hold it here, and the query would time out instead of finishing.
const INDEPENDENT_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const HELD_QUERY_EXIT_TIMEOUT: Duration = Duration::from_secs(30);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
const MYSQL_QUERY_INTERRUPTED: u16 = 1317;
/// Rows of the one-file table whose single row group decodes into many more
/// pages than one turn's budget allows.
const LONG_TABLE_ROWS: u64 = 400_000;
const FANOUT_TABLE_ROWS: u64 = 1_000_000;
const FANOUT_WORKERS: usize = 4;
const FANOUT_DOP: usize = 4;
/// Caches off, so every read of a file under test is a real object read that
/// the proxy can hold, even after another catalog read the same file.
const NO_CACHE_OVERLAY: &str = "[runtime.cache]\npage_cache_enable = false\nparquet_meta_cache_enable = false\nparquet_page_cache_enable = false\ndatacache_enable = false\n";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(ScanProducerReadiness),
        Box::new(ScanProducerCancel),
        Box::new(ScanProducerFanout),
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
        "UEA-4A-3 scan scenarios read backend markers, which only the fault-scenario profile emits"
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
                .query_drop(format!("DROP CATALOG IF EXISTS {name}"))
                .with_context(|| format!("remove stale catalog {name}"))?;
            control
                .query_drop(environment.iceberg_catalog_sql(name, &endpoint))
                .with_context(|| format!("create Iceberg catalog {name}"))?;
        }
        control
            .query_drop(format!(
                "CREATE DATABASE IF NOT EXISTS {DIRECT_ICEBERG}.{DATABASE}"
            ))
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
    fn create(control: &mut mysql::Conn, name: &'static str, rows: u64) -> Result<Self> {
        let qualified = format!("{DIRECT_ICEBERG}.{DATABASE}.{name}");
        control
            .query_drop(format!("DROP TABLE IF EXISTS {qualified}"))
            .with_context(|| format!("drop stale {qualified}"))?;
        control
            .query_drop(format!("CREATE TABLE {qualified} (v BIGINT)"))
            .with_context(|| format!("create {qualified}"))?;
        // The writer starts a file for every chunk it is given. A sort is
        // gathered onto one driver and emits its input as one chunk, so the
        // ORDER BY is what makes this one data file of one row group.
        control
            .query_drop(format!(
                "INSERT INTO {qualified} SELECT generate_series FROM \
                 TABLE(generate_series(1, {rows})) ORDER BY generate_series"
            ))
            .with_context(|| format!("write {qualified}"))?;
        let files: Vec<String> = control
            .query(format!(
                "SELECT file_path FROM {qualified}$files WHERE content = 0"
            ))
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
        vec![vec![rows.to_string(), (rows * (rows + 1) / 2).to_string()]]
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

fn connect(context: &ScenarioContext, operation: &str) -> Result<mysql::Conn> {
    mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining(operation)?,
    )
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// A result set as sorted rows of column texts, so results read through
/// different catalogs compare as values.
type Rows = Vec<Vec<String>>;

fn value_text(value: &mysql::Value) -> String {
    match value {
        mysql::Value::NULL => "NULL".to_owned(),
        mysql::Value::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        mysql::Value::Int(value) => value.to_string(),
        mysql::Value::UInt(value) => value.to_string(),
        mysql::Value::Float(value) => value.to_string(),
        mysql::Value::Double(value) => value.to_string(),
        other => other.as_sql(false),
    }
}

fn query_rows(connection: &mut mysql::Conn, sql: &str) -> mysql::Result<Rows> {
    let mut rows: Rows = connection
        .query::<mysql::Row, _>(sql)?
        .into_iter()
        .map(|row| row.unwrap().iter().map(value_text).collect())
        .collect();
    rows.sort();
    Ok(rows)
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
struct RunningQuery {
    connection_id: u32,
    done: mpsc::Receiver<mysql::Result<Rows>>,
    actor: JoinHandle<Result<()>>,
}

impl RunningQuery {
    fn start(context: &ScenarioContext, sql: String) -> Result<Self> {
        let (id_tx, id_rx) = mpsc::sync_channel(1);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        let timeout = context.remaining("connect held scan query")?;
        let actor = thread::spawn(move || -> Result<()> {
            // No read timeout: the query legitimately waits as long as its
            // read is held.
            let mut connection = mysql_actor::connect_for_cancellation(&user, port, timeout)?;
            id_tx.send(connection.connection_id())?;
            done_tx.send(query_rows(&mut connection, &sql))?;
            Ok(())
        });
        let connection_id = id_rx
            .recv_timeout(context.remaining("receive held query connection ID")?)
            .context("held query did not publish its connection ID")?;
        Ok(Self {
            connection_id,
            done: done_rx,
            actor,
        })
    }

    fn still_running(&self) -> Result<bool> {
        match self.done.try_recv() {
            Err(mpsc::TryRecvError::Empty) => Ok(true),
            Err(mpsc::TryRecvError::Disconnected) => {
                bail!("held query actor exited without a result")
            }
            Ok(result) => bail!("held query ended while its read was held: {result:?}"),
        }
    }

    fn finish(self, timeout: Duration) -> Result<mysql::Result<Rows>> {
        let result = self
            .done
            .recv_timeout(timeout)
            .context("held query did not end after its read was released")?;
        self.actor
            .join()
            .map_err(|_| anyhow::anyhow!("held query actor panicked"))??;
        Ok(result)
    }
}

/// Runs `sql` on a fresh connection whose every read is bounded, so a query
/// that cannot make progress fails instead of hanging the scenario.
fn run_independent(context: &ScenarioContext, sql: &str) -> Result<(Rows, Duration)> {
    let started = Instant::now();
    let mut connection = mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        INDEPENDENT_QUERY_TIMEOUT,
    )?;
    let rows = query_rows(&mut connection, sql)
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

fn run_id(context: &ScenarioContext) -> String {
    context.name().replace('/', "-")
}

// ------------------------------------------------------------- readiness

struct ScanProducerReadiness;

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
    let held = RunningQuery::start(context, case.held_sql)?;
    let operation = (|| -> Result<ReadinessPhase> {
        hold.wait_until_entered(HOLD_ENTRY_TIMEOUT)
            .with_context(|| format!("{phase}: the held query's object read never arrived"))?;
        let held_split = await_single_split(context, &offsets, phase)?;
        ensure!(
            held.still_running()?,
            "{phase}: the held query is not running"
        );
        let independent_offsets = log_offsets(context)?;
        let (observed, elapsed) = run_independent(context, &case.independent_sql)?;
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
            held.still_running()?,
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
    let result = held.finish(HELD_QUERY_EXIT_TIMEOUT)?;
    let report = operation?;
    let observed = result.with_context(|| format!("{phase}: held query failed after release"))?;
    ensure!(
        observed == case.held_expected,
        "{phase}: held query returned {observed:?}, expected {:?}",
        case.held_expected
    );
    Ok(report)
}

/// Picks a Paimon fixture table whose read is one split, reading it through
/// the direct catalog.
fn one_split_paimon_table(context: &mut ScenarioContext) -> Result<PaimonTable> {
    let mut control = connect(context, "connect Paimon probe session")?;
    for name in PAIMON_CANDIDATES {
        let offsets = log_offsets(context)?;
        let expected = query_rows(&mut control, &PaimonTable::sql(name, DIRECT_PAIMON))
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
    fn name(&self) -> &'static str {
        "uea4/scan-producer-readiness"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        _workload: Option<&Path>,
    ) -> Result<()> {
        validate_environment(launch_profile)
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        launch_config(1)
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
        let paimon = one_split_paimon_table(context)?;
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
        let matched: Option<i64> = control
            .query_first(format!(
                "SELECT COUNT(*) FROM {} WHERE v * 3 = 1",
                long.qualified(DIRECT_ICEBERG)
            ))
            .context("run the all-filtered long scan")?;
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

struct ScanProducerCancel;

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
    let held = RunningQuery::start(context, held_sql)?;
    let operation = (|| -> Result<CancelPhase> {
        hold.wait_until_entered(HOLD_ENTRY_TIMEOUT)
            .with_context(|| format!("{phase}: the held query's object read never arrived"))?;
        let held_connection = held_request_connection(proxy);
        ensure!(
            held.still_running()?,
            "{phase}: the held query is not running"
        );
        let killed = Instant::now();
        let mut control = connect(context, "connect KILL QUERY session")?;
        control
            .query_drop(format!("KILL QUERY {}", held.connection_id))
            .with_context(|| format!("{phase}: KILL QUERY"))?;
        let result = held
            .done
            .recv_timeout(HELD_QUERY_EXIT_TIMEOUT)
            .with_context(|| {
                format!("{phase}: the cancelled query did not end while its read was held")
            })?;
        let kill_to_error = killed.elapsed();
        ensure!(
            matches!(result, Err(mysql::Error::MySqlError(ref error)) if error.code == MYSQL_QUERY_INTERRUPTED),
            "{phase}: the cancelled query did not report its cancellation: {result:?}"
        );
        // The read is still held: every owner must finish without its bytes.
        await_frontend_local_exit(context, &format!("{phase} while held"))?;
        let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
        context
            .handle()
            .await_query_execution_resource_convergence(&baseline, deadline)
            .with_context(|| {
                format!("{phase}: query resources did not converge while the read was held")
            })?;
        let (observed, elapsed) = run_independent(context, independent_sql)?;
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
    held.actor
        .join()
        .map_err(|_| anyhow::anyhow!("{phase}: held query actor panicked"))??;
    let report = operation?;
    await_resource_convergence(context, &baseline, &format!("{phase} after release"))?;
    Ok(report)
}

impl Scenario for ScanProducerCancel {
    fn name(&self) -> &'static str {
        "uea4/scan-producer-cancel"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        _workload: Option<&Path>,
    ) -> Result<()> {
        validate_environment(launch_profile)
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        launch_config(1)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalogs = Catalogs::create(context, true)?;
        let proxy = &catalogs.proxy;
        let mut control = connect(context, "connect table setup session")?;
        let cancel = IcebergTable::create(&mut control, "t_cancel", 1_000)?;
        let peer = IcebergTable::create(&mut control, "t_peer", 2_000)?;
        drop(control);
        let paimon = one_split_paimon_table(context)?;
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

struct ScanProducerFanout;

#[derive(Serialize)]
struct FanoutRun {
    pipeline_dop: usize,
    wall_millis: u128,
    backend_cpu_millis: u128,
    backend_cores: f64,
    result: i64,
}

#[derive(Serialize)]
struct FanoutReport {
    driver_workers_per_backend: usize,
    backend: usize,
    rows: u64,
    runs: Vec<FanoutRun>,
}

fn backend_cpu_nanos(sampler: &ProcessResourceSampler, backend: usize) -> Result<u64> {
    let role = format!("be-{backend}");
    let sample = sampler
        .samples()
        .iter()
        .rev()
        .find(|sample| sample.role == role)
        .with_context(|| format!("no resource sample for {role}"))?;
    match (sample.cpu_user_nanos, sample.cpu_system_nanos) {
        (Some(user), Some(system)) => Ok(user + system),
        _ => bail!(
            "{role} CPU time is unavailable: {:?}",
            sample.unavailable_reason
        ),
    }
}

impl Scenario for ScanProducerFanout {
    fn name(&self) -> &'static str {
        "uea4/scan-producer-fanout"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        _workload: Option<&Path>,
    ) -> Result<()> {
        validate_environment(launch_profile)
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        launch_config(FANOUT_WORKERS)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let _catalogs = Catalogs::create(context, false)?;
        let mut control = connect(context, "connect fanout setup session")?;
        let table = IcebergTable::create(&mut control, "t_fanout", FANOUT_TABLE_ROWS)?;
        // Two SHA-512 rounds per row keep the downstream CPU-bound, so its
        // parallelism, not the scan's decoding, decides the cores used.
        let sql = format!(
            "SELECT SUM(LENGTH(sha2(sha2(CAST(v AS VARCHAR), 512), 512))) FROM {}",
            table.qualified(DIRECT_ICEBERG)
        );
        let mut runs = Vec::new();
        let mut backend = None;
        for dop in [1, FANOUT_DOP] {
            context.action(format!(
                "run the CPU-bound downstream at pipeline_dop={dop}"
            ));
            control
                .query_drop(format!("SET pipeline_dop = {dop}"))
                .context("set pipeline_dop")?;
            let offsets = log_offsets(context)?;
            let mut sampler = ProcessResourceSampler::from_identities(
                context.process_resource_identities()?,
                run_id(context),
            )?;
            sampler.sample_cluster()?;
            let started = Instant::now();
            let result: Option<i64> = control
                .query_first(sql.as_str())
                .with_context(|| format!("run the fanout query at pipeline_dop={dop}"))?;
            let wall = started.elapsed();
            sampler.sample_cluster()?;
            let placement = await_single_split(context, &offsets, "fanout scan")?;
            let expected_backend = *backend.get_or_insert(placement.backend);
            ensure!(
                placement.backend == expected_backend,
                "the fanout table's split moved from BE[{expected_backend}] to BE[{}]",
                placement.backend
            );
            let cpu_nanos = {
                let samples = sampler.samples();
                let role = format!("be-{}", placement.backend);
                let first = samples
                    .iter()
                    .find(|sample| sample.role == role)
                    .and_then(|sample| Some(sample.cpu_user_nanos? + sample.cpu_system_nanos?))
                    .with_context(|| format!("no initial CPU sample for {role}"))?;
                backend_cpu_nanos(&sampler, placement.backend)?.saturating_sub(first)
            };
            let cores = cpu_nanos as f64 / wall.as_nanos().max(1) as f64;
            context.action(format!(
                "pipeline_dop={dop}: BE[{}] used {cores:.2} cores over {} ms",
                placement.backend,
                wall.as_millis()
            ));
            runs.push(FanoutRun {
                pipeline_dop: dop,
                wall_millis: wall.as_millis(),
                backend_cpu_millis: u128::from(cpu_nanos / 1_000_000),
                backend_cores: cores,
                result: result.context("fanout query returned no row")?,
            });
        }
        let [narrow, wide] = runs.as_slice() else {
            bail!("expected one narrow and one wide fanout run");
        };
        ensure!(
            narrow.result == wide.result,
            "fanout changed the result: {} at dop 1, {} at dop {FANOUT_DOP}",
            narrow.result,
            wide.result
        );
        ensure!(
            wide.backend_cores >= 1.5 && wide.backend_cores >= narrow.backend_cores * 1.3,
            "a {FANOUT_DOP}-wide pipeline used {:.2} cores against {:.2} at dop 1: the scan's \
             downstream did not run on several drivers",
            wide.backend_cores,
            narrow.backend_cores
        );
        write_report(
            context,
            "uea4a3-scan-fanout.json",
            &FanoutReport {
                driver_workers_per_backend: FANOUT_WORKERS,
                backend: backend.expect("two runs"),
                rows: table.rows,
                runs,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(backend: usize, text: &str) -> (usize, String) {
        (backend, format!("2026-09-25 12:00:00 {text}"))
    }

    #[test]
    fn split_placements_count_only_enqueued_splits_per_execution() {
        let lines = [
            line(
                0,
                "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED execution_id=-1:7:1 finst=1:2 plan_node=3 enqueued=1 duplicate=0 accepted_through=1",
            ),
            line(
                1,
                "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED execution_id=-1:7:1 finst=1:3 plan_node=3 enqueued=0 duplicate=0 accepted_through=0",
            ),
            line(
                2,
                "NOVAROCKS_TASK_SPLIT_NO_MORE execution_id=-1:7:1 finst=1:4 plan_node=3",
            ),
        ];
        let placements = split_placements(&lines);
        assert_eq!(placements.len(), 1);
        assert_eq!(placements["-1:7:1"], BTreeMap::from([(0, 1)]));
    }

    #[test]
    fn marker_fields_come_from_the_marker_line_only() {
        let text =
            "prefix enqueued=9 NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED execution_id=a enqueued=2";
        assert_eq!(marker_field(text, SPLIT_MARKER, "enqueued"), Some("2"));
        assert_eq!(
            marker_field("no marker enqueued=1", SPLIT_MARKER, "enqueued"),
            None
        );
    }
}
