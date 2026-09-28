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

//! One measurement run of `uea4/scan-producer-performance`.
//!
//! `tests/benchmarks/uea4a3/README.md` fixes the protocol and
//! `tests/benchmarks/uea4a3/workload.json` the inputs; `run.py` launches this
//! scenario once per side (B0 or candidate) and configuration group, and
//! `compare.py` recomputes every statistic from the raw reports written here.
//! This scenario only measures: it runs each workload's closed-loop clients
//! for the warm-up and every window, records each query's submit and
//! terminal time, samples the processes' resources, and reads the backends'
//! dispatch and scan-stream metrics where the binary has them.

use super::connector::require_three_backends;
use super::paimon::{Fixture as PaimonFixture, create_paimon_catalog, load_shared_fixture};
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::delayed_s3::{DelayedS3Config, DelayedS3Proxy};
use novarocks_cluster_harness::process_resources::{
    ProcessResourceMonitor, ProcessResourceSampler,
};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, LaunchProfile, ServerHandle,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const SCENARIO: &str = "uea4/scan-producer-performance";
const WORKLOAD_ENV: &str = "NOVAROCKS_UEA4A3_WORKLOAD_MANIFEST";
const GROUP_ENV: &str = "NOVAROCKS_UEA4A3_CONFIG_GROUP";
const SIDE_ENV: &str = "NOVAROCKS_UEA4A3_SIDE";
/// The worktree environment's object-store secrets.
const ACCESS_KEY: &str = "AWS_S3_ACCESS_KEY_ID";
const SECRET_KEY: &str = "AWS_S3_SECRET_ACCESS_KEY";
/// The performance profile clears child environments and admits only typed
/// secret names, so every process receives the secrets under these.
const CHILD_ACCESS_KEY: &str = "NOVAROCKS_UEA4A3_S3_ACCESS_KEY_ID";
const CHILD_SECRET_KEY: &str = "NOVAROCKS_UEA4A3_S3_SECRET_ACCESS_KEY";
const DIRECT_ICEBERG: &str = "uea4a3_perf_ice";
const DELAYED_ICEBERG: &str = "uea4a3_perf_ice_delayed";
const DIRECT_PAIMON: &str = "uea4a3_perf_paimon";
const DISPATCH_METRIC: &str = "novarocks_driver_dispatch_latency_seconds";
const SCAN_PENDING_METRIC: &str = "novarocks_scan_stream_pending_total";
const INGRESS_REQUESTS_METRIC: &str = "novarocks_backend_native_ingress_request_body_bytes_count";
const INGRESS_WAIT_METRIC: &str = "novarocks_backend_native_ingress_wait_microseconds_total";
const INGRESS_REJECTIONS_METRIC: &str = "novarocks_backend_native_ingress_rejections_total";
const QUERY_INTERRUPTED: u16 = 1317;
/// Background load settles before the control samples start.
const CONTROL_RAMP: Duration = Duration::from_secs(5);
const KILL_RESULT_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Socket bound beyond the server's query timeout, so the server's timeout
/// error reaches the client before the socket gives up.
const IO_GRACE: Duration = Duration::from_secs(30);

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(ScanProducerPerformance)]
}

// ---------------------------------------------------------------- manifest

#[derive(Clone, Deserialize, Serialize)]
struct Manifest {
    schema_version: u32,
    scenario: String,
    frozen_on: String,
    measurement: Measurement,
    fixture: FixtureSpec,
    config_groups: Vec<ConfigGroup>,
    workloads: Vec<WorkloadSpec>,
    control: ControlSpec,
    gates: serde_json::Value,
}

#[derive(Clone, Deserialize, Serialize)]
struct Measurement {
    repetitions: usize,
    window_seconds: u64,
    warmup_seconds: u64,
    query_timeout_seconds: u64,
    resource_sample_interval_ms: u64,
}

#[derive(Clone, Deserialize, Serialize)]
struct FixtureSpec {
    database: String,
    wide_table: String,
    wide_files: usize,
    wide_rows_per_file: u64,
    small_table: String,
    small_files: usize,
    small_rows_per_file: u64,
    /// One file, so the control plane's short scan is short by construction.
    point_table: String,
    point_rows: u64,
    paimon_tables: Vec<String>,
    delayed_io_ms: u64,
}

#[derive(Clone, Deserialize, Serialize)]
struct ConfigGroup {
    name: String,
    driver_workers: Option<usize>,
    workloads: Vec<String>,
    control: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct WorkloadSpec {
    name: String,
    catalog: String,
    clients: usize,
    session: Vec<String>,
    sql: String,
    gated: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct ControlSpec {
    background_workload: String,
    background_clients: usize,
    window_seconds: u64,
    short_query_sql: String,
    short_query_samples: usize,
    short_query_interval_ms: u64,
    kill_query_sql: String,
    kill_samples: usize,
    kill_after_ms: u64,
}

impl Manifest {
    fn load() -> Result<(Self, String)> {
        let path = required_env(WORKLOAD_ENV)?;
        let bytes =
            fs::read(&path).with_context(|| format!("read UEA-4A-3 workload manifest {path}"))?;
        let manifest: Self =
            serde_json::from_slice(&bytes).context("decode UEA-4A-3 workload manifest")?;
        ensure!(
            manifest.schema_version == 1 && manifest.scenario == SCENARIO,
            "{path} is not a {SCENARIO} manifest"
        );
        for group in &manifest.config_groups {
            for workload in &group.workloads {
                manifest.workload(workload)?;
            }
        }
        manifest.workload(&manifest.control.background_workload)?;
        Ok((manifest, sha256_hex(&bytes)))
    }

    fn group(&self, name: &str) -> Result<&ConfigGroup> {
        self.config_groups
            .iter()
            .find(|group| group.name == name)
            .with_context(|| format!("manifest has no configuration group {name}"))
    }

    fn workload(&self, name: &str) -> Result<&WorkloadSpec> {
        self.workloads
            .iter()
            .find(|workload| workload.name == name)
            .with_context(|| format!("manifest has no workload {name}"))
    }

    fn render(&self, sql: &str, catalog: &str) -> Result<String> {
        let iceberg = match catalog {
            "direct" => DIRECT_ICEBERG,
            "delayed" => DELAYED_ICEBERG,
            other => bail!("unknown workload catalog {other}"),
        };
        Ok(sql
            .replace("{iceberg}", iceberg)
            .replace("{paimon}", DIRECT_PAIMON)
            .replace("{database}", &self.fixture.database)
            .replace("{wide}", &self.fixture.wide_table)
            .replace("{small}", &self.fixture.small_table)
            .replace("{point}", &self.fixture.point_table))
    }

    fn uses_delayed_catalog(&self, group: &ConfigGroup) -> Result<bool> {
        for name in &group.workloads {
            if self.workload(name)?.catalog == "delayed" {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn required_env(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn connect(context: &ScenarioContext, operation: &str) -> Result<mysql::Conn> {
    mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining(operation)?,
    )
}

fn load_average() -> String {
    Command::new("sysctl")
        .args(["-n", "vm.loadavg"])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|text| text.trim().to_owned())
        .unwrap_or_default()
}

// ----------------------------------------------------------------- catalogs

struct RestEnvironment {
    uri: String,
    warehouse: String,
    s3_endpoint: String,
}

impl RestEnvironment {
    fn load() -> Result<Self> {
        Ok(Self {
            uri: required_env("NOVAROCKS_ICEBERG_REST_URI")?,
            warehouse: required_env("NOVAROCKS_ICEBERG_REST_WAREHOUSE")?,
            s3_endpoint: required_env("AWS_S3_ENDPOINT")?,
        })
    }
}

fn create_iceberg_catalog(
    control: &mut mysql::Conn,
    name: &str,
    rest: &RestEnvironment,
    s3_endpoint: &str,
    paimon: &PaimonFixture,
) -> Result<()> {
    let (credential, generation) = paimon.credential();
    control
        .query_drop(format!("DROP CATALOG IF EXISTS {name}"))
        .with_context(|| format!("remove stale catalog {name}"))?;
    control
        .query_drop(format!(
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
            sql_string(&rest.uri),
            sql_string(&rest.warehouse),
            sql_string(s3_endpoint),
            sql_string(paimon.region()),
        ))
        .with_context(|| format!("create Iceberg catalog {name}"))
}

// ------------------------------------------------------------------ fixture

/// Creates a table of `files` data files of `rows_per_file` rows each unless
/// it already has exactly that shape, and returns its files' digest input.
fn ensure_table(
    control: &mut mysql::Conn,
    qualified: &str,
    columns: &str,
    files: usize,
    rows_per_file: u64,
    insert: impl Fn(u64, u64) -> String,
) -> Result<Vec<(String, i64, i64)>> {
    let expected_rows = rows_per_file * files as u64;
    let shape: Option<(Option<i64>, Option<i64>)> = control
        .query_first(format!(
            "SELECT COUNT(*), SUM(record_count) FROM {qualified}$files WHERE content = 0"
        ))
        .ok()
        .flatten();
    let matches = shape.is_some_and(|(count, rows)| {
        count == Some(files as i64) && rows == Some(expected_rows as i64)
    });
    if !matches {
        control
            .query_drop(format!("DROP TABLE IF EXISTS {qualified}"))
            .with_context(|| format!("drop stale {qualified}"))?;
        control
            .query_drop(format!("CREATE TABLE {qualified} ({columns})"))
            .with_context(|| format!("create {qualified}"))?;
        for file in 0..files as u64 {
            let start = file * rows_per_file + 1;
            control
                .query_drop(insert(start, start + rows_per_file - 1))
                .with_context(|| format!("write file {file} of {qualified}"))?;
        }
    }
    let mut listed: Vec<(String, i64, i64)> = control
        .query(format!(
            "SELECT file_path, record_count, file_size_in_bytes FROM {qualified}$files WHERE content = 0"
        ))
        .with_context(|| format!("list {qualified} data files"))?;
    listed.sort();
    ensure!(
        listed.len() == files
            && listed
                .iter()
                .all(|(_, rows, _)| *rows == rows_per_file as i64),
        "{qualified} is not {files} files of {rows_per_file} rows each"
    );
    Ok(listed)
}

fn ensure_fixture(control: &mut mysql::Conn, spec: &FixtureSpec) -> Result<String> {
    control
        .query_drop(format!(
            "CREATE DATABASE IF NOT EXISTS {DIRECT_ICEBERG}.{}",
            spec.database
        ))
        .context("create the performance namespace")?;
    let wide = format!("{DIRECT_ICEBERG}.{}.{}", spec.database, spec.wide_table);
    let small = format!("{DIRECT_ICEBERG}.{}.{}", spec.database, spec.small_table);
    let point = format!("{DIRECT_ICEBERG}.{}.{}", spec.database, spec.point_table);
    // The writer starts a data file for every chunk it is given; the ORDER BY
    // gathers each insert onto one driver whose sort emits one chunk, so each
    // insert is one file of one row group.
    let wide_files = ensure_table(
        control,
        &wide,
        "c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 VARCHAR(64), c5 BIGINT",
        spec.wide_files,
        spec.wide_rows_per_file,
        |start, end| {
            format!(
                "INSERT INTO {wide} SELECT generate_series, generate_series % 1000, \
                 (generate_series * 2654435761) % 1000003, CAST(generate_series AS VARCHAR), \
                 generate_series * 3 FROM TABLE(generate_series({start}, {end})) \
                 ORDER BY generate_series"
            )
        },
    )?;
    let small_files = ensure_table(
        control,
        &small,
        "v BIGINT",
        spec.small_files,
        spec.small_rows_per_file,
        |start, end| {
            format!(
                "INSERT INTO {small} SELECT generate_series FROM \
                 TABLE(generate_series({start}, {end})) ORDER BY generate_series"
            )
        },
    )?;
    let point_files = ensure_table(
        control,
        &point,
        "v BIGINT",
        1,
        spec.point_rows,
        |start, end| {
            format!(
                "INSERT INTO {point} SELECT generate_series FROM \
             TABLE(generate_series({start}, {end})) ORDER BY generate_series"
            )
        },
    )?;
    let mut digest = Sha256::new();
    for (path, rows, bytes) in wide_files.iter().chain(&small_files).chain(&point_files) {
        digest.update(format!("{path}\t{rows}\t{bytes}\n"));
    }
    Ok(format!("{:x}", digest.finalize()))
}

// ------------------------------------------------------------ closed loops

/// Where the measurement's clients connect. Plain values, so client threads
/// share it without borrowing the scenario context.
#[derive(Clone)]
struct Endpoint {
    user: String,
    port: u16,
    query_timeout: Duration,
}

impl Endpoint {
    fn of(context: &ScenarioContext, manifest: &Manifest) -> Self {
        Self {
            user: context.mysql_user().to_owned(),
            port: context.mysql_port(),
            query_timeout: Duration::from_secs(manifest.measurement.query_timeout_seconds),
        }
    }

    /// A session whose statements the server bounds by the frozen query
    /// timeout. The socket bound adds a grace, so the server's own timeout
    /// reaches the client first and counts as a timed-out query.
    fn session(&self, statements: &[String]) -> Result<mysql::Conn> {
        let mut connection = mysql_actor::connect_with_io_timeout(
            &self.user,
            self.port,
            CONNECT_TIMEOUT,
            self.query_timeout + IO_GRACE,
        )?;
        self.bound(&mut connection)?;
        for statement in statements {
            connection
                .query_drop(statement)
                .with_context(|| format!("apply `{statement}`"))?;
        }
        Ok(connection)
    }

    /// A session another session cancels. No socket read timeout: on macOS
    /// the client maps one that fires while it awaits the cancellation to
    /// EAGAIN. The server's query timeout still bounds the statement.
    fn cancellable(&self) -> Result<mysql::Conn> {
        let mut connection =
            mysql_actor::connect_for_cancellation(&self.user, self.port, CONNECT_TIMEOUT)?;
        self.bound(&mut connection)?;
        Ok(connection)
    }

    fn bound(&self, connection: &mut mysql::Conn) -> Result<()> {
        connection
            .query_drop(format!(
                "SET query_timeout = {}",
                self.query_timeout.as_secs()
            ))
            .context("apply the frozen query timeout")
    }
}

#[derive(Clone, Serialize)]
struct QuerySample {
    client: usize,
    submitted_micros: u128,
    finished_micros: u128,
    rows: Option<u64>,
    /// Leading hex of the sha256 over the result's sorted row texts, so both
    /// sides can be shown to return the same result.
    result_digest: Option<String>,
    /// The row count and first sorted rows of a result, recorded the first
    /// time a client sees its digest, so a divergent result shows its values.
    result_preview: Option<String>,
    error: Option<String>,
}

/// Bounds a result preview: the first sorted rows, at most this many bytes.
const RESULT_PREVIEW_ROWS: usize = 3;
const RESULT_PREVIEW_BYTES: usize = 512;

/// One complete result: its row count, digest and bounded preview.
struct QueryResult {
    rows: u64,
    digest: String,
    preview: String,
}

/// Runs one statement to its complete result. The result is kept as the
/// server sent it until the terminal time is taken, so digesting it adds no
/// latency.
fn execute(connection: &mut mysql::Conn, sql: &str) -> (Duration, Result<QueryResult>) {
    let started = Instant::now();
    let rows = connection
        .query_iter(sql)
        .and_then(|result| result.collect::<mysql::Result<Vec<mysql::Row>>>());
    let elapsed = started.elapsed();
    let outcome = rows.map_err(anyhow::Error::from).map(|rows| {
        let mut texts: Vec<String> = rows
            .into_iter()
            .map(|row| {
                row.unwrap()
                    .iter()
                    .map(value_text)
                    .collect::<Vec<_>>()
                    .join("\t")
            })
            .collect();
        texts.sort();
        let mut digest = Sha256::new();
        for text in &texts {
            digest.update(text.as_bytes());
            digest.update(b"\n");
        }
        let digest = format!("{:x}", digest.finalize());
        let mut preview = format!("{} rows: ", texts.len());
        preview.push_str(&texts[..texts.len().min(RESULT_PREVIEW_ROWS)].join(" | "));
        if preview.len() > RESULT_PREVIEW_BYTES {
            let mut end = RESULT_PREVIEW_BYTES;
            while !preview.is_char_boundary(end) {
                end -= 1;
            }
            preview.truncate(end);
        }
        QueryResult {
            rows: texts.len() as u64,
            digest: digest[..16].to_owned(),
            preview,
        }
    });
    (elapsed, outcome)
}

fn value_text(value: &mysql::Value) -> String {
    match value {
        mysql::Value::NULL => "NULL".to_owned(),
        mysql::Value::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => other.as_sql(false),
    }
}

/// Runs `clients` closed-loop sessions of `sql` that submit until `duration`
/// has passed and then drain their last query: every query submitted inside
/// the window belongs to it, however late it ends.
fn run_closed_loop(
    endpoint: &Endpoint,
    workload: &WorkloadSpec,
    sql: &str,
    clients: usize,
    duration: Duration,
) -> Result<Vec<QuerySample>> {
    run_closed_loop_until(
        endpoint,
        workload,
        sql,
        clients,
        duration,
        Arc::new(AtomicBool::new(true)),
    )
}

/// As [`run_closed_loop`], but the clients keep submitting past `duration`
/// until `released` is set, so a load that other samples need lasts until
/// they are taken.
fn run_closed_loop_until(
    endpoint: &Endpoint,
    workload: &WorkloadSpec,
    sql: &str,
    clients: usize,
    duration: Duration,
    released: Arc<AtomicBool>,
) -> Result<Vec<QuerySample>> {
    let started = Instant::now();
    let deadline = started + duration;
    let mut actors = Vec::with_capacity(clients);
    for client in 0..clients {
        let endpoint = endpoint.clone();
        let sql = sql.to_owned();
        let session = workload.session.clone();
        let released = Arc::clone(&released);
        actors.push(thread::spawn(move || -> Result<Vec<QuerySample>> {
            let mut connection = endpoint.session(&session)?;
            let mut samples = Vec::new();
            let mut seen = std::collections::BTreeSet::new();
            while Instant::now() < deadline || !released.load(Ordering::Acquire) {
                let submitted = started.elapsed();
                let (elapsed, outcome) = execute(&mut connection, &sql);
                let failed = outcome.is_err();
                let (rows, result_digest, result_preview, error) = match outcome {
                    Ok(result) => {
                        let preview = seen.insert(result.digest.clone()).then_some(result.preview);
                        (Some(result.rows), Some(result.digest), preview, None)
                    }
                    Err(error) => (None, None, None, Some(format!("{error:#}"))),
                };
                samples.push(QuerySample {
                    client,
                    submitted_micros: submitted.as_micros(),
                    finished_micros: (submitted + elapsed).as_micros(),
                    rows,
                    result_digest,
                    result_preview,
                    error,
                });
                if failed {
                    // A failed statement can leave the session unusable; a
                    // fresh one keeps the client in the loop.
                    connection = endpoint.session(&session)?;
                }
            }
            Ok(samples)
        }));
    }
    let mut samples = Vec::new();
    for actor in actors {
        samples.extend(
            actor
                .join()
                .map_err(|_| anyhow::anyhow!("closed-loop client panicked"))??,
        );
    }
    Ok(samples)
}

#[derive(Serialize)]
struct LoopSummary {
    successes: usize,
    errors: usize,
    first_errors: Vec<String>,
    elapsed_micros: u128,
    throughput_qps: f64,
    p50_ms: Option<f64>,
    p95_ms: Option<f64>,
    p99_ms: Option<f64>,
    distinct_result_digests: Vec<String>,
}

fn nearest_rank(sorted: &[f64], quantile: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    Some(sorted[rank.clamp(1, sorted.len()) - 1])
}

fn summarize(samples: &[QuerySample]) -> LoopSummary {
    let first = samples
        .iter()
        .map(|s| s.submitted_micros)
        .min()
        .unwrap_or(0);
    let last = samples.iter().map(|s| s.finished_micros).max().unwrap_or(0);
    let elapsed = last.saturating_sub(first);
    let mut latencies: Vec<f64> = samples
        .iter()
        .filter(|sample| sample.error.is_none())
        .map(|sample| (sample.finished_micros - sample.submitted_micros) as f64 / 1000.0)
        .collect();
    latencies.sort_by(f64::total_cmp);
    let successes = latencies.len();
    let mut digests: Vec<String> = samples
        .iter()
        .filter_map(|sample| sample.result_digest.clone())
        .collect();
    digests.sort_unstable();
    digests.dedup();
    LoopSummary {
        successes,
        errors: samples.len() - successes,
        first_errors: samples
            .iter()
            .filter_map(|sample| sample.error.clone())
            .take(5)
            .collect(),
        elapsed_micros: elapsed,
        throughput_qps: if elapsed == 0 {
            0.0
        } else {
            successes as f64 / (elapsed as f64 / 1_000_000.0)
        },
        p50_ms: nearest_rank(&latencies, 0.50),
        p95_ms: nearest_rank(&latencies, 0.95),
        p99_ms: nearest_rank(&latencies, 0.99),
        distinct_result_digests: digests,
    }
}

// --------------------------------------------------------------- resources

#[derive(Serialize)]
struct RoleResources {
    role: String,
    peak_rss_bytes: Option<u64>,
    /// B0 keeps its scan thread pool and the candidate has none, so the two
    /// sides' thread counts differ by design and are recorded, not gated.
    peak_threads: Option<u64>,
    cpu_millis: Option<u64>,
}

fn role_resources(sampler: &ProcessResourceSampler) -> Vec<RoleResources> {
    let mut roles: BTreeMap<&str, Vec<_>> = BTreeMap::new();
    for sample in sampler.samples() {
        roles.entry(sample.role.as_str()).or_default().push(sample);
    }
    roles
        .into_iter()
        .map(|(role, samples)| {
            let cpu =
                |sample: &&novarocks_cluster_harness::process_resources::ProcessResourceSample| {
                    Some(sample.cpu_user_nanos? + sample.cpu_system_nanos?)
                };
            let cpu_millis = match (samples.first().and_then(cpu), samples.last().and_then(cpu)) {
                (Some(first), Some(last)) => Some(last.saturating_sub(first) / 1_000_000),
                _ => None,
            };
            RoleResources {
                role: role.to_owned(),
                peak_rss_bytes: samples.iter().filter_map(|sample| sample.rss_bytes).max(),
                peak_threads: samples.iter().filter_map(|sample| sample.threads).max(),
                cpu_millis,
            }
        })
        .collect()
}

/// `name{labels} value` samples of one metric family from Prometheus text.
fn metric_samples(text: &str, family: &str) -> Vec<(String, BTreeMap<String, String>, f64)> {
    let mut samples = Vec::new();
    for line in text.lines() {
        if line.starts_with('#') || !line.starts_with(family) {
            continue;
        }
        let (series, value) = match line.rsplit_once(' ') {
            Some(parts) => parts,
            None => continue,
        };
        let Ok(value) = value.parse::<f64>() else {
            continue;
        };
        let (name, labels) = match series.split_once('{') {
            Some((name, rest)) => (name, rest.trim_end_matches('}')),
            None => (series, ""),
        };
        let labels = labels
            .split(',')
            .filter_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                Some((key.to_owned(), value.trim_matches('"').to_owned()))
            })
            .collect();
        samples.push((name.to_owned(), labels, value));
    }
    samples
}

#[derive(Serialize)]
struct DispatchTransition {
    transition: String,
    samples: f64,
    /// Upper bucket bounds holding the quantile, in microseconds.
    p50_micros: Option<f64>,
    p95_micros: Option<f64>,
    p99_micros: Option<f64>,
}

#[derive(Serialize)]
struct BackendMetrics {
    backend: usize,
    /// Absent when the binary does not have the metric.
    dispatch: Option<Vec<DispatchTransition>>,
    scan_pending: Option<BTreeMap<String, f64>>,
    /// Native ingress over the window, per class: requests completed,
    /// microseconds spent waiting for a running slot, and rejections by
    /// reason. Both sides have these, so a control-plane saturation shows on
    /// the side it happens on.
    ingress_requests: BTreeMap<String, f64>,
    ingress_wait_micros: BTreeMap<String, f64>,
    ingress_rejections: BTreeMap<String, f64>,
}

fn histogram_quantile(buckets: &[(f64, f64)], total: f64, quantile: f64) -> Option<f64> {
    if total <= 0.0 {
        return None;
    }
    let target = quantile * total;
    buckets
        .iter()
        .find(|(_, cumulative)| *cumulative >= target)
        .map(|(bound, _)| bound * 1_000_000.0)
}

fn backend_metrics(before: &str, after: &str, backend: usize) -> BackendMetrics {
    let bucket_family = format!("{DISPATCH_METRIC}_bucket");
    let delta = |family: &str, key: &dyn Fn(&BTreeMap<String, String>) -> Option<String>| {
        let mut values: BTreeMap<String, f64> = BTreeMap::new();
        for (name, labels, value) in metric_samples(after, family) {
            if name == family
                && let Some(key) = key(&labels)
            {
                *values.entry(key).or_default() += value;
            }
        }
        for (name, labels, value) in metric_samples(before, family) {
            if name == family
                && let Some(key) = key(&labels)
            {
                *values.entry(key).or_default() -= value;
            }
        }
        values
    };
    let buckets = delta(&bucket_family, &|labels| {
        Some(format!(
            "{}\t{}",
            labels.get("transition")?,
            labels.get("le")?
        ))
    });
    let counts = delta(&format!("{DISPATCH_METRIC}_count"), &|labels| {
        labels.get("transition").cloned()
    });
    let dispatch = (!counts.is_empty()).then(|| {
        counts
            .iter()
            .map(|(transition, total)| {
                let mut series: Vec<(f64, f64)> = buckets
                    .iter()
                    .filter_map(|(key, cumulative)| {
                        let (name, le) = key.split_once('\t')?;
                        (name == transition).then(|| {
                            let bound = if le == "+Inf" {
                                f64::INFINITY
                            } else {
                                le.parse().unwrap_or(f64::INFINITY)
                            };
                            (bound, *cumulative)
                        })
                    })
                    .collect();
                series.sort_by(|a, b| a.0.total_cmp(&b.0));
                DispatchTransition {
                    transition: transition.clone(),
                    samples: *total,
                    p50_micros: histogram_quantile(&series, *total, 0.50),
                    p95_micros: histogram_quantile(&series, *total, 0.95),
                    p99_micros: histogram_quantile(&series, *total, 0.99),
                }
            })
            .collect()
    });
    let pending = delta(SCAN_PENDING_METRIC, &|labels| labels.get("reason").cloned());
    BackendMetrics {
        backend,
        dispatch,
        scan_pending: (!pending.is_empty()).then_some(pending),
        ingress_requests: delta(INGRESS_REQUESTS_METRIC, &|labels| {
            labels.get("class").cloned()
        }),
        ingress_wait_micros: delta(INGRESS_WAIT_METRIC, &|labels| labels.get("class").cloned()),
        ingress_rejections: delta(INGRESS_REJECTIONS_METRIC, &|labels| {
            Some(format!(
                "{}/{}",
                labels.get("class")?,
                labels.get("reason")?
            ))
        }),
    }
}

fn backend_texts(context: &mut ScenarioContext) -> Result<Vec<String>> {
    let backends = context.handle().be_count();
    (0..backends)
        .map(|index| context.handle().backend_prometheus_text(index))
        .collect()
}

// ---------------------------------------------------------------- workloads

#[derive(Serialize)]
struct WindowReport {
    repetition: usize,
    summary: LoopSummary,
    resources: Vec<RoleResources>,
    backends: Vec<BackendMetrics>,
    samples: Vec<QuerySample>,
}

#[derive(Serialize)]
struct WorkloadReport {
    name: String,
    sql: String,
    clients: usize,
    session: Vec<String>,
    gated: bool,
    warmup: LoopSummary,
    windows: Vec<WindowReport>,
}

fn measure_workload(
    context: &mut ScenarioContext,
    manifest: &Manifest,
    workload: &WorkloadSpec,
) -> Result<WorkloadReport> {
    let sql = manifest.render(&workload.sql, &workload.catalog)?;
    let endpoint = Endpoint::of(context, manifest);
    context.action(format!("{}: warm up", workload.name));
    let warmup = run_closed_loop(
        &endpoint,
        workload,
        &sql,
        workload.clients,
        Duration::from_secs(manifest.measurement.warmup_seconds),
    )?;
    let mut windows = Vec::new();
    for repetition in 1..=manifest.measurement.repetitions {
        context.action(format!("{}: window {repetition}", workload.name));
        let before = backend_texts(context)?;
        let monitor = ProcessResourceMonitor::start_with_identities(
            context.process_resource_identities()?,
            format!("{}-{repetition}", workload.name),
            Duration::from_millis(manifest.measurement.resource_sample_interval_ms),
        )?;
        let samples = run_closed_loop(
            &endpoint,
            workload,
            &sql,
            workload.clients,
            Duration::from_secs(manifest.measurement.window_seconds),
        );
        let sampler = monitor.finish(
            &context
                .scenario_root()
                .join(format!("resources-{}-{repetition}.json", workload.name)),
        )?;
        let samples = samples?;
        let after = backend_texts(context)?;
        windows.push(WindowReport {
            repetition,
            summary: summarize(&samples),
            resources: role_resources(&sampler),
            backends: before
                .iter()
                .zip(&after)
                .enumerate()
                .map(|(backend, (before, after))| backend_metrics(before, after, backend))
                .collect(),
            samples,
        });
    }
    Ok(WorkloadReport {
        name: workload.name.clone(),
        sql,
        clients: workload.clients,
        session: workload.session.clone(),
        gated: workload.gated,
        warmup: summarize(&warmup),
        windows,
    })
}

// ------------------------------------------------------------------ control

#[derive(Serialize)]
struct ControlSamples {
    /// Short scan: submit to complete result, in milliseconds.
    short_scan_ms: Vec<f64>,
    short_scan_errors: Vec<String>,
    /// KILL QUERY: the KILL statement's round trip, in milliseconds.
    kill_ack_ms: Vec<f64>,
    /// KILL QUERY sent until the cancelled query reported its cancellation.
    kill_delivery_ms: Vec<f64>,
    /// Samples whose query ended before its KILL or with another outcome.
    kill_invalid: Vec<String>,
}

#[derive(Serialize)]
struct ControlReport {
    /// The KILL sample query run to completion once on the idle cluster: a
    /// sample is valid only if the query is still running when KILL arrives.
    kill_query_uncancelled_ms: f64,
    idle: ControlSamples,
    loaded: ControlSamples,
    /// What the background load kept busy while the loaded samples ran.
    loaded_resources: Vec<RoleResources>,
    background: LoopSummary,
    background_samples: Vec<QuerySample>,
    heartbeat: &'static str,
    runtime_filter: &'static str,
}

fn sample_control(
    endpoint: &Endpoint,
    control: &ControlSpec,
    short_sql: &str,
    kill_sql: &str,
) -> Result<ControlSamples> {
    let mut samples = ControlSamples {
        short_scan_ms: Vec::new(),
        short_scan_errors: Vec::new(),
        kill_ack_ms: Vec::new(),
        kill_delivery_ms: Vec::new(),
        kill_invalid: Vec::new(),
    };
    let interval = Duration::from_millis(control.short_query_interval_ms);
    let mut short = endpoint.session(&[])?;
    for _ in 0..control.short_query_samples {
        let tick = Instant::now();
        match execute(&mut short, short_sql) {
            (elapsed, Ok(_)) => samples.short_scan_ms.push(millis(elapsed)),
            (_, Err(error)) => {
                samples.short_scan_errors.push(format!("{error:#}"));
                short = endpoint.session(&[])?;
            }
        }
        if let Some(rest) = interval.checked_sub(tick.elapsed()) {
            thread::sleep(rest);
        }
    }
    let mut killer = endpoint.session(&[])?;
    for _ in 0..control.kill_samples {
        let (id_tx, id_rx) = mpsc::sync_channel(1);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let actor_endpoint = endpoint.clone();
        let sql = kill_sql.to_owned();
        let actor = thread::spawn(move || -> Result<()> {
            let mut connection = actor_endpoint.cancellable()?;
            id_tx.send(connection.connection_id())?;
            let (_, outcome) = execute(&mut connection, &sql);
            done_tx.send((Instant::now(), outcome))?;
            Ok(())
        });
        let connection_id = id_rx
            .recv_timeout(CONNECT_TIMEOUT)
            .context("KILL sample query did not publish its connection ID")?;
        thread::sleep(Duration::from_millis(control.kill_after_ms));
        let sent = Instant::now();
        let kill = killer.query_drop(format!("KILL QUERY {connection_id}"));
        let acked = sent.elapsed();
        let (ended, outcome) = done_rx
            .recv_timeout(KILL_RESULT_TIMEOUT)
            .context("KILL sample query did not end")?;
        actor
            .join()
            .map_err(|_| anyhow::anyhow!("KILL sample actor panicked"))??;
        let interrupted = outcome.as_ref().err().is_some_and(|error| {
            matches!(
                error.downcast_ref::<mysql::Error>(),
                Some(mysql::Error::MySqlError(error)) if error.code == QUERY_INTERRUPTED
            )
        });
        match kill {
            Ok(()) if interrupted => {
                samples.kill_ack_ms.push(millis(acked));
                samples
                    .kill_delivery_ms
                    .push(millis(ended.saturating_duration_since(sent)));
            }
            kill => samples.kill_invalid.push(format!(
                "kill={:?} query={}",
                kill.err(),
                match outcome {
                    Ok(result) => format!("completed with {} rows", result.rows),
                    Err(error) => format!("{error:#}"),
                }
            )),
        }
    }
    Ok(samples)
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn measure_control(context: &mut ScenarioContext, manifest: &Manifest) -> Result<ControlReport> {
    let control = &manifest.control;
    let endpoint = Endpoint::of(context, manifest);
    let short_sql = manifest.render(&control.short_query_sql, "direct")?;
    let kill_sql = manifest.render(&control.kill_query_sql, "direct")?;
    context.action("control: run the KILL sample query to completion once");
    let (uncancelled, outcome) = execute(&mut endpoint.session(&[])?, &kill_sql);
    outcome.context("run the KILL sample query uncancelled")?;
    let kill_query_uncancelled_ms = millis(uncancelled);
    ensure!(
        kill_query_uncancelled_ms >= 2.0 * control.kill_after_ms as f64,
        "the KILL sample query completes in {kill_query_uncancelled_ms:.0} ms on the idle \
         cluster, too close to the {} ms KILL delay for its samples to be valid; the manifest \
         must be refrozen",
        control.kill_after_ms
    );
    context.action("control: idle short scans and KILL samples");
    let idle = sample_control(&endpoint, control, &short_sql, &kill_sql)?;
    context.action("control: the same samples under a saturating scan load");
    let background = manifest.workload(&control.background_workload)?.clone();
    let background_sql = manifest.render(&background.sql, &background.catalog)?;
    let window = Duration::from_secs(control.window_seconds);
    let released = Arc::new(AtomicBool::new(false));
    let monitor = ProcessResourceMonitor::start_with_identities(
        context.process_resource_identities()?,
        "control-loaded",
        Duration::from_millis(manifest.measurement.resource_sample_interval_ms),
    )?;
    let (loaded, background_samples) = thread::scope(|scope| {
        let load = scope.spawn(|| {
            run_closed_loop_until(
                &endpoint,
                &background,
                &background_sql,
                control.background_clients,
                window,
                Arc::clone(&released),
            )
        });
        thread::sleep(CONTROL_RAMP);
        let loaded = sample_control(&endpoint, control, &short_sql, &kill_sql);
        released.store(true, Ordering::Release);
        let background = load
            .join()
            .map_err(|_| anyhow::anyhow!("background load panicked"));
        (loaded, background)
    });
    let sampler = monitor.finish(
        &context
            .scenario_root()
            .join("resources-control-loaded.json"),
    )?;
    let background_samples = background_samples??;
    Ok(ControlReport {
        kill_query_uncancelled_ms,
        idle,
        loaded: loaded?,
        loaded_resources: role_resources(&sampler),
        background: summarize(&background_samples),
        background_samples,
        heartbeat: "not measured: no FE-to-BE heartbeat latency entry point",
        runtime_filter: "not measured: no runtime-filter publication latency entry point",
    })
}

// ------------------------------------------------------------------ scenario

#[derive(Serialize)]
struct PerformanceReport {
    schema_version: u32,
    side: String,
    group: String,
    manifest_sha256: String,
    frozen_on: String,
    binary: PathBuf,
    binary_sha256: String,
    fixture_sha256: String,
    driver_workers: Option<usize>,
    load_average_before: String,
    load_average_after: String,
    workloads: Vec<WorkloadReport>,
    control: Option<ControlReport>,
}

struct ScanProducerPerformance;

impl Scenario for ScanProducerPerformance {
    fn name(&self) -> &'static str {
        SCENARIO
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        _workload: Option<&Path>,
    ) -> Result<()> {
        ensure!(
            launch_profile == LaunchProfile::Performance,
            "{SCENARIO} requires the native performance launch profile"
        );
        let (manifest, _) = Manifest::load()?;
        manifest.group(&required_env(GROUP_ENV)?)?;
        required_env(SIDE_ENV)?;
        required_env(ACCESS_KEY)?;
        required_env(SECRET_KEY)?;
        RestEnvironment::load()?;
        load_shared_fixture("schema").map(|_| ())
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (manifest, _) = Manifest::load()?;
        let group = manifest.group(&required_env(GROUP_ENV)?)?;
        let paimon = load_shared_fixture("schema")?;
        let (credential, generation) = paimon.credential();
        // Each role's credential list replaces the base config's: the
        // frontend reads metadata and the backends read data files, both under
        // the fixture's static credential and the typed secret names.
        let credential_overlay = |purpose: &str| {
            format!(
                "[[connector.credentials]]\npurpose = \"{purpose}\"\nname = \"{credential}\"\n\
                 generation = \"{generation}\"\nkind = \"s3\"\n\
                 access_key_id = \"${{ENV:{CHILD_ACCESS_KEY}}}\"\n\
                 access_key_secret = \"${{ENV:{CHILD_SECRET_KEY}}}\"\n"
            )
        };
        let mut be = String::new();
        if let Some(workers) = group.driver_workers {
            be.push_str(&format!(
                "[runtime]\npipeline_exec_thread_pool_thread_num = {workers}\n"
            ));
        }
        be.push_str(&credential_overlay("object-store-data"));
        let mut child_environment = CrossProcessChildEnvironment::default();
        let access = required_env(ACCESS_KEY)?;
        let secret = required_env(SECRET_KEY)?;
        for values in [&mut child_environment.fe, &mut child_environment.be] {
            values.insert(CHILD_ACCESS_KEY.to_owned(), access.clone());
            values.insert(CHILD_SECRET_KEY.to_owned(), secret.clone());
        }
        Ok(ScenarioLaunchConfig {
            child_environment,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(credential_overlay("object-store-metadata")),
                be: Some(be),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let (manifest, manifest_sha256) = Manifest::load()?;
        let group = manifest.group(&required_env(GROUP_ENV)?)?.clone();
        let side = required_env(SIDE_ENV)?;
        let rest = RestEnvironment::load()?;
        let paimon = load_shared_fixture("schema")?;
        let proxy = if manifest.uses_delayed_catalog(&group)? {
            Some(DelayedS3Proxy::start(DelayedS3Config {
                downstream: rest.s3_endpoint.clone(),
                delay: Duration::from_millis(manifest.fixture.delayed_io_ms),
            })?)
        } else {
            None
        };
        let mut control = connect(context, "connect performance setup session")?;
        create_iceberg_catalog(
            &mut control,
            DIRECT_ICEBERG,
            &rest,
            &rest.s3_endpoint,
            &paimon,
        )?;
        if let Some(proxy) = &proxy {
            create_iceberg_catalog(
                &mut control,
                DELAYED_ICEBERG,
                &rest,
                proxy.endpoint(),
                &paimon,
            )?;
        }
        create_paimon_catalog(&mut control, DIRECT_PAIMON, &paimon)?;
        context.action("ensure the fixed Iceberg inputs");
        let fixture_sha256 = ensure_fixture(&mut control, &manifest.fixture)?;
        for table in &manifest.fixture.paimon_tables {
            let rows: Vec<mysql::Row> = control
                .query(format!("SELECT * FROM {DIRECT_PAIMON}.fixture.{table}"))
                .with_context(|| format!("read Paimon fixture table {table}"))?;
            ensure!(!rows.is_empty(), "Paimon fixture table {table} is empty");
        }
        drop(control);

        let load_average_before = load_average();
        let mut workloads = Vec::new();
        for name in &group.workloads {
            let workload = manifest.workload(name)?.clone();
            workloads.push(measure_workload(context, &manifest, &workload)?);
        }
        let control = if group.control {
            Some(measure_control(context, &manifest)?)
        } else {
            None
        };
        let binary = context.primary_binary().to_path_buf();
        let binary_sha256 =
            sha256_hex(&fs::read(&binary).with_context(|| format!("read {}", binary.display()))?);
        let report = PerformanceReport {
            schema_version: 1,
            side,
            group: group.name.clone(),
            manifest_sha256,
            frozen_on: manifest.frozen_on.clone(),
            binary,
            binary_sha256,
            fixture_sha256,
            driver_workers: group.driver_workers,
            load_average_before,
            load_average_after: load_average(),
            workloads,
            control,
        };
        fs::write(
            context.scenario_root().join("uea4a3-performance.json"),
            serde_json::to_vec_pretty(&report)?,
        )
        .context("write the performance report")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_follows_ceil_of_p_times_n() {
        let sorted = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        assert_eq!(nearest_rank(&sorted, 0.95), Some(10.0));
        assert_eq!(nearest_rank(&sorted, 0.50), Some(5.0));
        assert_eq!(nearest_rank(&[], 0.5), None);
    }

    #[test]
    fn dispatch_quantiles_come_from_the_window_delta() {
        let before = "novarocks_driver_dispatch_latency_seconds_bucket{transition=\"wake_to_enqueue\",le=\"0.000001\"} 5\n\
                      novarocks_driver_dispatch_latency_seconds_bucket{transition=\"wake_to_enqueue\",le=\"0.000004\"} 5\n\
                      novarocks_driver_dispatch_latency_seconds_bucket{transition=\"wake_to_enqueue\",le=\"+Inf\"} 5\n\
                      novarocks_driver_dispatch_latency_seconds_count{transition=\"wake_to_enqueue\"} 5\n";
        let after = "novarocks_driver_dispatch_latency_seconds_bucket{transition=\"wake_to_enqueue\",le=\"0.000001\"} 6\n\
                     novarocks_driver_dispatch_latency_seconds_bucket{transition=\"wake_to_enqueue\",le=\"0.000004\"} 15\n\
                     novarocks_driver_dispatch_latency_seconds_bucket{transition=\"wake_to_enqueue\",le=\"+Inf\"} 15\n\
                     novarocks_driver_dispatch_latency_seconds_count{transition=\"wake_to_enqueue\"} 15\n\
                     novarocks_scan_stream_pending_total{reason=\"wait\"} 3\n";
        let metrics = backend_metrics(before, after, 0);
        let dispatch = metrics.dispatch.expect("dispatch metrics");
        assert_eq!(dispatch.len(), 1);
        assert_eq!(dispatch[0].samples, 10.0);
        assert_eq!(dispatch[0].p50_micros, Some(4.0));
        assert_eq!(
            metrics.scan_pending,
            Some(BTreeMap::from([("wait".to_owned(), 3.0)]))
        );
        let absent = backend_metrics("", "", 1);
        assert!(absent.dispatch.is_none() && absent.scan_pending.is_none());
    }

    #[test]
    fn the_checked_in_manifest_is_consistent() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/benchmarks/uea4a3/workload.json");
        let bytes = fs::read(&path).expect("read the checked-in manifest");
        let manifest: Manifest = serde_json::from_slice(&bytes).expect("decode manifest");
        assert_eq!(manifest.scenario, SCENARIO);
        for group in &manifest.config_groups {
            for workload in &group.workloads {
                manifest.workload(workload).expect("known workload");
            }
        }
        for workload in &manifest.workloads {
            manifest
                .render(&workload.sql, &workload.catalog)
                .expect("renders");
        }
    }
}
