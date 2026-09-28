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

//! Short real-provider planning regression check for UEA-4A-4.
//!
//! The same scenario and workload run against B0 and candidate binaries. This
//! coarse check only rejects large regressions; it is not a formal benchmark.

use super::connector::{await_resource_convergence, require_three_backends, resource_baseline};
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::delayed_s3::{DelayedS3Config, DelayedS3Proxy};
use novarocks_cluster_harness::process_resources::ProcessResourceMonitor;
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, LaunchProfile,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier, Mutex, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const WORKLOAD_ENV: &str = "NOVAROCKS_UEA4A4_WORKLOAD_MANIFEST";
const ICEBERG_FIXTURE_ENV: &str = "NOVAROCKS_UEA4A4_ICEBERG_FIXTURE_MANIFEST";
const PAIMON_FIXTURE_ENV: &str = "NOVAROCKS_UEA4A4_PAIMON_FIXTURE_MANIFEST";
const ACCESS_KEY_ENV: &str = "NOVAROCKS_UEA4A4_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "NOVAROCKS_UEA4A4_S3_SECRET_ACCESS_KEY";
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Workload {
    schema_version: u32,
    purpose: String,
    warehouse_concurrency_limit: usize,
    clients: usize,
    warmup_ms: u64,
    duration_ms: u64,
    repetitions: usize,
    slow_s3_delay_ms: u64,
    minimum_data_files: u64,
    iceberg_query: String,
    paimon_query: String,
}

impl Workload {
    fn load() -> Result<(Self, String)> {
        let path = env::var_os(WORKLOAD_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR")).join("../benchmarks/uea4a4/workload.json")
            });
        let bytes = fs::read(&path)
            .with_context(|| format!("read UEA-4A-4 workload {}", path.display()))?;
        let workload: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode UEA-4A-4 workload {}", path.display()))?;
        ensure!(
            workload.schema_version == 1,
            "unsupported UEA-4A-4 workload version"
        );
        ensure!(
            workload.purpose == "short-regression",
            "UEA-4A-4 requires the short-regression workload"
        );
        ensure!(
            workload.duration_ms == 20_000
                && workload.repetitions == 1
                && workload.warmup_ms == 3_000,
            "UEA-4A-4 short regression requires one 20-second window per provider and mode with a three-second warmup"
        );
        ensure!(
            (1..=64).contains(&workload.clients)
                && (1..=64).contains(&workload.warehouse_concurrency_limit)
                && workload.clients > workload.warehouse_concurrency_limit
                && workload.slow_s3_delay_ms > 0
                && workload.minimum_data_files >= 16,
            "UEA-4A-4 short workload lacks a saturated real-provider fixture"
        );
        for sql in [&workload.iceberg_query, &workload.paimon_query] {
            ensure!(
                sql.starts_with("SELECT ") && sql.contains("${table}") && !sql.contains(';'),
                "UEA-4A-4 workload query must be one SELECT with a table placeholder"
            );
        }
        Ok((workload, sha256(&bytes)))
    }
}

#[derive(Clone, Debug, Deserialize)]
struct ProviderFixture {
    schema_version: u32,
    fixture_kind: String,
    warehouse_uri: String,
    s3_endpoint: String,
    region: String,
    credential_name: String,
    credential_generation: String,
    database: String,
    table: String,
    snapshot_id: serde_json::Value,
    schema_id: serde_json::Value,
    data_file_count: u64,
    row_count: u64,
    objects_sha256: String,
    #[serde(default)]
    rest_uri: Option<String>,
}

#[derive(Clone)]
struct CheckedFixture {
    provider: &'static str,
    facts: ProviderFixture,
    manifest_sha256: String,
}

impl CheckedFixture {
    fn load(variable: &str, provider: &'static str, minimum_data_files: u64) -> Result<Self> {
        let path = PathBuf::from(
            env::var(variable)
                .with_context(|| format!("{variable} must name a published fixture"))?,
        );
        let bytes = fs::read(&path)
            .with_context(|| format!("read {provider} fixture manifest {}", path.display()))?;
        let manifest_sha256 = sha256(&bytes);
        let root = path
            .parent()
            .context("fixture manifest has no parent directory")?;
        let ready = fs::read_to_string(root.join("READY"))
            .with_context(|| format!("read {provider} fixture READY"))?;
        ensure!(
            ready.trim() == format!("sha256:{manifest_sha256}"),
            "{provider} fixture READY does not bind the exact manifest"
        );
        let facts: ProviderFixture = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode {provider} fixture manifest"))?;
        ensure!(
            facts.schema_version == 1,
            "unsupported {provider} fixture version"
        );
        ensure!(
            facts.fixture_kind == format!("uea4a4-{provider}-performance-v1"),
            "unexpected {provider} fixture kind"
        );
        ensure!(
            facts.data_file_count >= minimum_data_files && facts.row_count > 0,
            "{provider} fixture is too small for short planning regression"
        );
        for identifier in [&facts.database, &facts.table] {
            ensure!(
                !identifier.is_empty()
                    && identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
                "{provider} fixture contains an invalid database or table identifier"
            );
        }
        for identifier in [&facts.credential_name, &facts.credential_generation] {
            ensure!(
                !identifier.is_empty()
                    && identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'),
                "{provider} fixture contains an invalid identifier"
            );
        }
        ensure!(
            !facts.snapshot_id.is_null()
                && !facts.schema_id.is_null()
                && facts.snapshot_id.as_str().is_none_or(|id| !id.is_empty())
                && facts.schema_id.as_str().is_none_or(|id| !id.is_empty()),
            "{provider} fixture omitted snapshot or schema identity"
        );
        ensure!(
            facts.s3_endpoint.starts_with("http://127.0.0.1:")
                || facts.s3_endpoint.starts_with("http://localhost:"),
            "{provider} fixture must use a loopback HTTP S3 endpoint"
        );
        ensure!(
            provider != "iceberg" || facts.rest_uri.as_deref().is_some_and(|uri| !uri.is_empty()),
            "Iceberg fixture omitted REST Catalog endpoint"
        );
        let objects = fs::read(root.join("objects.json"))
            .with_context(|| format!("read {provider} fixture object inventory"))?;
        ensure!(
            sha256(&objects) == facts.objects_sha256,
            "{provider} fixture object inventory hash mismatch"
        );
        Ok(Self {
            provider,
            facts,
            manifest_sha256,
        })
    }

    fn catalog_name(&self, slow: bool) -> String {
        format!(
            "uea4a4_{}_{}",
            self.provider,
            if slow { "slow" } else { "normal" }
        )
    }

    fn table_name(&self, slow: bool) -> String {
        format!(
            "{}.{}.{}",
            self.catalog_name(slow),
            self.facts.database,
            self.facts.table
        )
    }

    fn catalog_sql(&self, slow: bool, s3_endpoint: &str) -> String {
        let name = self.catalog_name(slow);
        let properties = if self.provider == "iceberg" {
            format!(
                "\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{}\",\"iceberg.catalog.warehouse\"=\"{}\"",
                sql_string(self.facts.rest_uri.as_deref().unwrap_or_default()),
                sql_string(&self.facts.warehouse_uri)
            )
        } else {
            format!(
                "\"paimon.catalog.type\"=\"filesystem\",\"warehouse\"=\"{}\"",
                sql_string(&self.facts.warehouse_uri)
            )
        };
        format!(
            "CREATE EXTERNAL CATALOG {name} PROPERTIES(\"type\"=\"{}\",{properties},\
             \"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"{}\",\
             \"aws.s3.enable_path_style_access\"=\"true\",\
             \"credential.object-store-metadata.consumer-role\"=\"frontend\",\
             \"credential.object-store-metadata.mode\"=\"static\",\
             \"credential.object-store-metadata.name\"=\"{}\",\
             \"credential.object-store-metadata.generation\"=\"{}\",\
             \"credential.object-store-data.consumer-role\"=\"backend\",\
             \"credential.object-store-data.mode\"=\"static\",\
             \"credential.object-store-data.name\"=\"{}\",\
             \"credential.object-store-data.generation\"=\"{}\")",
            self.provider,
            sql_string(s3_endpoint),
            sql_string(&self.facts.region),
            self.facts.credential_name,
            self.facts.credential_generation,
            self.facts.credential_name,
            self.facts.credential_generation,
        )
    }
}

struct LiveFixtures {
    iceberg: CheckedFixture,
    paimon: CheckedFixture,
    iceberg_slow: DelayedS3Proxy,
    paimon_slow: DelayedS3Proxy,
}

struct CatalogPlanningPerformance {
    fixtures: Mutex<Option<LiveFixtures>>,
    smoke_only: bool,
}

impl CatalogPlanningPerformance {
    fn new(smoke_only: bool) -> Self {
        Self {
            fixtures: Mutex::new(None),
            smoke_only,
        }
    }
}

impl Scenario for CatalogPlanningPerformance {
    fn name(&self) -> &'static str {
        if self.smoke_only {
            "uea4/catalog-planning-smoke"
        } else {
            "uea4/catalog-planning-performance"
        }
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        _uea1_workload_manifest: Option<&Path>,
    ) -> Result<()> {
        ensure!(
            launch_profile == LaunchProfile::Performance,
            "UEA-4A-4 performance requires --launch-profile performance"
        );
        let (workload, _) = Workload::load()?;
        CheckedFixture::load(ICEBERG_FIXTURE_ENV, "iceberg", workload.minimum_data_files)?;
        CheckedFixture::load(PAIMON_FIXTURE_ENV, "paimon", workload.minimum_data_files)?;
        Ok(())
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (workload, _) = Workload::load()?;
        let iceberg =
            CheckedFixture::load(ICEBERG_FIXTURE_ENV, "iceberg", workload.minimum_data_files)?;
        let paimon =
            CheckedFixture::load(PAIMON_FIXTURE_ENV, "paimon", workload.minimum_data_files)?;
        let delay = Duration::from_millis(workload.slow_s3_delay_ms);
        let iceberg_slow = DelayedS3Proxy::start(DelayedS3Config {
            downstream: iceberg.facts.s3_endpoint.clone(),
            delay,
        })?;
        let paimon_slow = DelayedS3Proxy::start(DelayedS3Config {
            downstream: paimon.facts.s3_endpoint.clone(),
            delay,
        })?;
        let access_key = env::var("AWS_S3_ACCESS_KEY_ID")
            .context("UEA-4A-4 performance requires AWS_S3_ACCESS_KEY_ID")?;
        let secret_key = env::var("AWS_S3_SECRET_ACCESS_KEY")
            .context("UEA-4A-4 performance requires AWS_S3_SECRET_ACCESS_KEY")?;
        ensure!(
            !access_key.is_empty() && !secret_key.is_empty(),
            "UEA-4A-4 fixture credentials must be nonempty"
        );
        let mut child_environment = CrossProcessChildEnvironment::default();
        for values in [&mut child_environment.fe, &mut child_environment.be] {
            values.insert(ACCESS_KEY_ENV.to_owned(), access_key.clone());
            values.insert(SECRET_KEY_ENV.to_owned(), secret_key.clone());
        }
        let mut fe = format!(
            "[runtime.frontend_workload]\nconcurrency_limit = {}\nwaiting_limit = 512\ncapacity_wait_timeout_ms = 30000\n",
            workload.warehouse_concurrency_limit
        );
        let mut be = String::new();
        let mut credentials = std::collections::BTreeSet::new();
        for fixture in [&iceberg, &paimon] {
            credentials.insert((
                fixture.facts.credential_name.as_str(),
                fixture.facts.credential_generation.as_str(),
            ));
        }
        for (name, generation) in credentials {
            fe.push_str(&credential_overlay(
                "object-store-metadata",
                name,
                generation,
            ));
            be.push_str(&credential_overlay("object-store-data", name, generation));
        }
        *self
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))? = Some(LiveFixtures {
            iceberg,
            paimon,
            iceberg_slow,
            paimon_slow,
        });
        Ok(ScenarioLaunchConfig {
            child_environment,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(fe),
                be: Some(be),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        ensure!(
            context.launch_profile() == LaunchProfile::Performance,
            "UEA-4A-4 performance requires the native performance launch profile"
        );
        let (workload, workload_sha256) = Workload::load()?;
        let fixtures = self
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))?;
        let fixtures = fixtures
            .as_ref()
            .context("UEA-4A-4 fixture was not prepared")?;
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect UEA-4A-4 setup session")?,
        )?;
        for (fixture, proxy) in [
            (&fixtures.iceberg, &fixtures.iceberg_slow),
            (&fixtures.paimon, &fixtures.paimon_slow),
        ] {
            for (slow, endpoint) in [
                (false, fixture.facts.s3_endpoint.as_str()),
                (true, proxy.endpoint()),
            ] {
                connection
                    .query_drop(fixture.catalog_sql(slow, endpoint))
                    .with_context(|| format!("create {} benchmark catalog", fixture.provider))?;
                let table = fixture.table_name(slow);
                let count: Option<u64> = connection
                    .query_first(format!("SELECT COUNT(*) FROM {table}"))
                    .with_context(|| format!("verify {} benchmark table", fixture.provider))?;
                ensure!(
                    count == Some(fixture.facts.row_count),
                    "{} benchmark table row count does not match its published fixture",
                    fixture.provider
                );
            }
        }
        drop(connection);

        if self.smoke_only {
            context.action("verified real Iceberg and Paimon catalogs with identical performance configuration");
            return Ok(());
        }

        let monitor = ProcessResourceMonitor::start_with_identities(
            context.process_resource_identities()?,
            context.name(),
            SAMPLE_INTERVAL,
        )?;
        let execution = (|| -> Result<Vec<WindowReport>> {
            let mut reports = Vec::with_capacity(workload.repetitions * 4);
            for (fixture, proxy, query) in [
                (
                    &fixtures.iceberg,
                    &fixtures.iceberg_slow,
                    &workload.iceberg_query,
                ),
                (
                    &fixtures.paimon,
                    &fixtures.paimon_slow,
                    &workload.paimon_query,
                ),
            ] {
                for slow in [false, true] {
                    let sql = query.replace("${table}", &fixture.table_name(slow));
                    for repetition in 0..workload.repetitions {
                        context.action(format!(
                            "start short UEA-4A-4 window provider={} mode={} repetition={}",
                            fixture.provider,
                            if slow { "slow-remote" } else { "normal" },
                            repetition
                        ));
                        reports.push(run_window(
                            context,
                            &workload,
                            fixture.provider,
                            slow,
                            repetition,
                            &sql,
                            proxy,
                        )?);
                    }
                }
            }
            Ok(reports)
        })();
        let resource_path = context.scenario_root().join("process-resources.json");
        let resources = monitor.finish(&resource_path)?;
        let windows = execution?;
        ensure!(
            !resources.samples().is_empty(),
            "UEA-4A-4 collected no process samples"
        );
        let mut peak_rss_bytes_by_role = BTreeMap::new();
        for sample in resources.samples() {
            if let Some(rss) = sample.rss_bytes {
                let peak = peak_rss_bytes_by_role
                    .entry(sample.role.clone())
                    .or_insert(0_u64);
                *peak = (*peak).max(rss);
            }
        }
        ensure!(
            peak_rss_bytes_by_role.len() == 4,
            "UEA-4A-4 requires RSS samples for the FE and all three BEs"
        );
        let report = PerformanceReport {
            schema_version: 1,
            status: "short-regression-observed",
            server_binary_sha256: sha256_file(context.primary_binary())?,
            workload_sha256,
            iceberg_fixture_sha256: fixtures.iceberg.manifest_sha256.clone(),
            paimon_fixture_sha256: fixtures.paimon.manifest_sha256.clone(),
            effective_config_sha256: context
                .effective_launch_config_evidence()
                .semantics_sha256()
                .to_owned(),
            peak_rss_bytes_by_role,
            windows,
        };
        let path = context.scenario_root().join("uea4a4-performance.json");
        fs::write(&path, serde_json::to_vec_pretty(&report)?)
            .with_context(|| format!("write UEA-4A-4 raw report {}", path.display()))?;
        context.action(format!(
            "wrote short UEA-4A-4 regression evidence to {}",
            path.display()
        ));
        ensure!(
            report
                .windows
                .iter()
                .all(|window| window.errors == 0 && window.completed > 0),
            "UEA-4A-4 short regression had query errors or an empty window"
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))?
            .take();
        Ok(())
    }
}

fn credential_overlay(purpose: &str, name: &str, generation: &str) -> String {
    format!(
        "[[connector.credentials]]\npurpose = \"{purpose}\"\nname = \"{name}\"\ngeneration = \"{generation}\"\nkind = \"s3\"\naccess_key_id = \"${{ENV:{ACCESS_KEY_ENV}}}\"\naccess_key_secret = \"${{ENV:{SECRET_KEY_ENV}}}\"\n"
    )
}

#[derive(Serialize)]
struct QueryLatency {
    total_micros: u128,
    completed_after_window: bool,
}

#[derive(Serialize)]
struct WindowReport {
    provider: &'static str,
    mode: &'static str,
    repetition: usize,
    duration_ms: u64,
    warmup_micros: u128,
    completed: usize,
    throughput_per_second: f64,
    p95_micros: u128,
    errors: usize,
    slow_gets: u64,
    slow_heads: u64,
    drain_micros: u128,
    samples: Vec<QueryLatency>,
}

#[derive(Serialize)]
struct PerformanceReport {
    schema_version: u32,
    status: &'static str,
    server_binary_sha256: String,
    workload_sha256: String,
    iceberg_fixture_sha256: String,
    paimon_fixture_sha256: String,
    effective_config_sha256: String,
    peak_rss_bytes_by_role: BTreeMap<String, u64>,
    windows: Vec<WindowReport>,
}

fn run_window(
    context: &mut ScenarioContext,
    workload: &Workload,
    provider: &'static str,
    slow: bool,
    repetition: usize,
    sql: &str,
    proxy: &DelayedS3Proxy,
) -> Result<WindowReport> {
    let timeout = context.remaining("connect UEA-4A-4 workload clients")?;
    let mut connections = Vec::with_capacity(workload.clients);
    for _ in 0..workload.clients {
        connections.push(mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            timeout,
        )?);
    }
    let (connections, warmup_micros) = warm_connections(
        connections,
        sql,
        workload.warmup_ms,
        workload.warehouse_concurrency_limit,
    )?;
    let before = proxy.snapshot();
    let gate = Arc::new(Barrier::new(workload.clients + 1));
    let deadline = Arc::new(OnceLock::new());
    let mut workers = Vec::with_capacity(workload.clients);
    for mut connection in connections {
        let gate = Arc::clone(&gate);
        let deadline = Arc::clone(&deadline);
        let sql = sql.to_owned();
        workers.push(thread::spawn(
            move || -> Result<(Vec<QueryLatency>, usize)> {
                gate.wait();
                let end = *deadline.get().context("UEA-4A-4 window deadline missing")?;
                let mut samples = Vec::new();
                let mut errors = 0;
                while Instant::now() < end {
                    let started = Instant::now();
                    match execute_query(&mut connection, &sql) {
                        Ok(_) => samples.push(QueryLatency {
                            total_micros: started.elapsed().as_micros(),
                            completed_after_window: Instant::now() > end,
                        }),
                        Err(_) => {
                            errors += 1;
                            break;
                        }
                    }
                }
                Ok((samples, errors))
            },
        ));
    }
    let duration = Duration::from_millis(workload.duration_ms);
    let end = Instant::now() + duration;
    deadline
        .set(end)
        .map_err(|_| anyhow::anyhow!("UEA-4A-4 window deadline already set"))?;
    gate.wait();
    while Instant::now() < end {
        thread::sleep(SAMPLE_INTERVAL.min(end.saturating_duration_since(Instant::now())));
    }
    let mut samples = Vec::new();
    let mut errors = 0;
    for worker in workers {
        let (worker_samples, worker_errors) = worker
            .join()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 client panicked"))??;
        samples.extend(worker_samples);
        errors += worker_errors;
    }
    let drain_micros = Instant::now().saturating_duration_since(end).as_micros();
    let after = proxy.snapshot();
    ensure!(
        after.upstream_errors == before.upstream_errors,
        "UEA-4A-4 delayed S3 proxy observed an upstream error"
    );
    let gets = after.gets.saturating_sub(before.gets);
    let heads = after.heads.saturating_sub(before.heads);
    if slow {
        ensure!(
            gets + heads > 0,
            "slow {provider} window never used delayed S3 GET/HEAD"
        );
    } else {
        ensure!(
            gets + heads == 0,
            "normal {provider} window used the delayed S3 endpoint"
        );
    }
    let mut latencies = samples
        .iter()
        .map(|sample| sample.total_micros)
        .collect::<Vec<_>>();
    let completed = samples
        .iter()
        .filter(|sample| !sample.completed_after_window)
        .count();
    Ok(WindowReport {
        provider,
        mode: if slow { "slow-remote" } else { "normal" },
        repetition,
        duration_ms: workload.duration_ms,
        warmup_micros,
        completed,
        throughput_per_second: completed as f64 / duration.as_secs_f64(),
        p95_micros: if latencies.is_empty() {
            0
        } else {
            percentile(&mut latencies, 95)
        },
        errors,
        slow_gets: gets,
        slow_heads: heads,
        drain_micros,
        samples,
    })
}

fn warm_connections(
    mut connections: Vec<mysql::Conn>,
    sql: &str,
    warmup_ms: u64,
    active_clients: usize,
) -> Result<(Vec<mysql::Conn>, u128)> {
    ensure!(
        active_clients > 0 && active_clients <= connections.len(),
        "UEA-4A-4 warmup client count exceeds the admitted slot count"
    );
    let idle = connections.split_off(active_clients);
    let gate = Arc::new(Barrier::new(connections.len() + 1));
    let deadline = Arc::new(OnceLock::new());
    let mut workers = Vec::with_capacity(connections.len());
    for mut connection in connections {
        let gate = Arc::clone(&gate);
        let deadline = Arc::clone(&deadline);
        let sql = sql.to_owned();
        workers.push(thread::spawn(move || -> Result<mysql::Conn> {
            gate.wait();
            let end = *deadline.get().context("UEA-4A-4 warmup deadline missing")?;
            while Instant::now() < end {
                execute_query(&mut connection, &sql).context("warm real UEA-4A-4 provider")?;
            }
            Ok(connection)
        }));
    }
    let start = Instant::now();
    deadline
        .set(start + Duration::from_millis(warmup_ms))
        .map_err(|_| anyhow::anyhow!("UEA-4A-4 warmup deadline already set"))?;
    gate.wait();
    let mut warmed = Vec::with_capacity(workers.len());
    for worker in workers {
        warmed.push(
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("UEA-4A-4 warmup client panicked"))??,
        );
    }
    warmed.extend(idle);
    let elapsed = start.elapsed().as_micros();
    ensure!(
        elapsed >= Duration::from_millis(warmup_ms).as_micros(),
        "UEA-4A-4 warmup did not run for its fixed duration"
    );
    Ok((warmed, elapsed))
}

fn execute_query(connection: &mut mysql::Conn, sql: &str) -> Result<u128> {
    let started = Instant::now();
    let mut result = connection
        .query_iter(sql)
        .context("execute UEA-4A-4 provider SQL")?;
    let mut first_row = None;
    let mut rows = 0;
    for row in result.by_ref() {
        row.context("read UEA-4A-4 provider row")?;
        first_row.get_or_insert_with(|| started.elapsed().as_micros());
        rows += 1;
    }
    ensure!(rows > 0, "UEA-4A-4 provider query returned no rows");
    Ok(first_row.context("UEA-4A-4 provider query has no first row")?)
}

fn percentile(samples: &mut [u128], percentile: usize) -> u128 {
    samples.sort_unstable();
    samples[(samples.len() - 1) * percentile / 100]
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut reader =
        BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

/// Hold a real Iceberg object request while KILL QUERY remains responsive.
/// The SDK manifest and native file reader are exercised separately because
/// they have different cancellation wiring and may run on different roles.
struct IcebergHeldReadCancellation {
    setup: CatalogPlanningPerformance,
}

/// Exercises the FE planning and admitted BE read paths with both real
/// providers. The published small fixture cannot cross the former Paimon
/// item/byte limits; that separate boundary is reported as uncovered.
struct CatalogPlanningNoFeQuota {
    setup: CatalogPlanningPerformance,
}

/// Verifies that an FE metadata read remains owned after query cancellation:
/// the next provider query can use the released warehouse slot while the old
/// object request remains held, and the old query eventually exits cancelled.
struct CatalogPlanningResponsibility {
    setup: CatalogPlanningPerformance,
}

impl CatalogPlanningResponsibility {
    fn new() -> Self {
        Self {
            setup: CatalogPlanningPerformance::new(true),
        }
    }
}

impl Scenario for CatalogPlanningResponsibility {
    fn name(&self) -> &'static str {
        "uea4/catalog-planning-responsibility"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        workload: Option<&Path>,
    ) -> Result<()> {
        self.setup.validate_runner_inputs(launch_profile, workload)
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let mut config = self.setup.launch_config(scenario_root)?;
        let (workload, _) = Workload::load()?;
        let frontend = config
            .config_overlay
            .fe
            .as_mut()
            .context("responsibility scenario has no FE workload config")?;
        let original = format!(
            "concurrency_limit = {}",
            workload.warehouse_concurrency_limit
        );
        ensure!(
            frontend.contains(&original),
            "FE workload limit was not rendered"
        );
        *frontend = frontend.replacen(&original, "concurrency_limit = 1", 1);
        Ok(config)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixtures = self
            .setup
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))?;
        let fixtures = fixtures
            .as_ref()
            .context("UEA-4A-4 fixture was not prepared")?;
        let mut control = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect UEA-4A-4 responsibility setup")?,
        )?;
        for fixture in [&fixtures.iceberg, &fixtures.paimon] {
            control
                .query_drop(fixture.catalog_sql(
                    true,
                    if fixture.provider == "iceberg" {
                        fixtures.iceberg_slow.endpoint()
                    } else {
                        fixtures.paimon_slow.endpoint()
                    },
                ))
                .with_context(|| format!("create {} held-read catalog", fixture.provider))?;
            control
                .query_drop(fixture.catalog_sql(false, &fixture.facts.s3_endpoint))
                .with_context(|| format!("create {} independent catalog", fixture.provider))?;
        }
        drop(control);

        let iceberg_sql = format!(
            "SELECT COUNT(*) FROM {} WHERE id BETWEEN 1 AND 1000",
            fixtures.iceberg.table_name(true)
        );
        let paimon_sql = format!(
            "SELECT COUNT(*) FROM {} WHERE id BETWEEN 1 AND 1000",
            fixtures.paimon.table_name(true)
        );
        let iceberg_followup = format!(
            "SELECT COUNT(*) FROM {}",
            fixtures.iceberg.table_name(false)
        );
        let paimon_followup = format!("SELECT COUNT(*) FROM {}", fixtures.paimon.table_name(false));
        let paimon_snapshot_path = format!(
            "/{}.db/{}/snapshot/LATEST",
            fixtures.paimon.facts.database, fixtures.paimon.facts.table
        );

        context.action("hold Iceberg SDK manifest, cancel Q1, admit independent Paimon Q2 before releasing Q1 read");
        run_held_provider_read(
            context,
            &fixtures.iceberg_slow,
            &iceberg_sql,
            &paimon_followup,
            fixtures.paimon.facts.row_count,
            "Iceberg",
            "sdk-manifest",
            Some(".avro"),
            true,
        )?;
        context.action("hold Paimon snapshot/LATEST metadata, cancel Q1, admit independent Iceberg Q2 before releasing Q1 read");
        run_held_provider_read(
            context,
            &fixtures.paimon_slow,
            &paimon_sql,
            &iceberg_followup,
            fixtures.iceberg.facts.row_count,
            "Paimon",
            "snapshot-metadata",
            Some(&paimon_snapshot_path),
            true,
        )?;
        context
            .action("hold Iceberg native data read and verify cancellation at its next checkpoint");
        run_held_provider_read(
            context,
            &fixtures.iceberg_slow,
            &iceberg_sql,
            &paimon_followup,
            fixtures.paimon.facts.row_count,
            "Iceberg",
            "native-data",
            Some(".parquet"),
            false,
        )?;
        context.action("hold Iceberg SDK manifest until its one-second statement deadline expires without KILL QUERY");
        run_iceberg_held_deadline(
            context,
            &fixtures.iceberg_slow,
            &iceberg_sql,
            &paimon_followup,
            fixtures.paimon.facts.row_count,
        )?;
        let mut control = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect independent catalog owner")?,
        )?;
        for fixture in [&fixtures.iceberg, &fixtures.paimon] {
            let table = fixture.table_name(false);
            let mut rows = control
                .query_iter(format!("EXPLAIN SELECT COUNT(*) FROM {table}"))
                .with_context(|| {
                    format!(
                        "EXPLAIN {} through provider metadata planning",
                        fixture.provider
                    )
                })?;
            let mut row_count = 0;
            for row in rows.by_ref() {
                row.with_context(|| format!("read {} EXPLAIN row", fixture.provider))?;
                row_count += 1;
            }
            ensure!(
                row_count > 0,
                "{} EXPLAIN returned no plan rows",
                fixture.provider
            );
            context.action(format!(
                "{} non-SELECT EXPLAIN returned {row_count} plan rows",
                fixture.provider
            ));
        }
        let report = serde_json::json!({
            "schema_version": 1,
            "acceptance_status": "partial",
            "binary_sha256": sha256_file(context.primary_binary())?,
            "iceberg_fixture_sha256": fixtures.iceberg.manifest_sha256,
            "paimon_fixture_sha256": fixtures.paimon.manifest_sha256,
            "native_topology": "1FE+3BE",
            "covered": ["Iceberg SDK and FS held reads", "Paimon snapshot/LATEST metadata read held", "KILL QUERY while read held", "Iceberg held SDK statement deadline without KILL", "independent query before old read is released", "held deadline read fully forwarded after release", "both providers EXPLAIN", "independent CREATE EXTERNAL CATALOG"],
            "uncovered": ["DML or maintenance owner in this scenario", "direct native observation of ReadAccessSink late deposit", "native file-layer DeadlineExceeded type (covered by separate FS and SDK unit tests)"],
        });
        fs::write(
            context
                .scenario_root()
                .join("uea4a4-responsibility-coverage.json"),
            serde_json::to_vec_pretty(&report)?,
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.setup.teardown()
    }
}

impl CatalogPlanningNoFeQuota {
    fn new() -> Self {
        Self {
            setup: CatalogPlanningPerformance::new(true),
        }
    }
}

impl Scenario for CatalogPlanningNoFeQuota {
    fn name(&self) -> &'static str {
        "uea4/catalog-planning-no-fe-quota"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        workload: Option<&Path>,
    ) -> Result<()> {
        self.setup.validate_runner_inputs(launch_profile, workload)
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        self.setup.launch_config(scenario_root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixtures = self
            .setup
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))?;
        let fixtures = fixtures
            .as_ref()
            .context("UEA-4A-4 fixture was not prepared")?;
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect UEA-4A-4 no-quota session")?,
        )?;
        for fixture in [&fixtures.iceberg, &fixtures.paimon] {
            connection
                .query_drop(fixture.catalog_sql(false, &fixture.facts.s3_endpoint))
                .with_context(|| format!("create {} no-quota catalog", fixture.provider))?;
            let table = fixture.table_name(false);
            let count: Option<u64> = connection
                .query_first(format!("SELECT COUNT(*) FROM {table}"))
                .with_context(|| format!("read {} through admitted execution", fixture.provider))?;
            ensure!(
                count == Some(fixture.facts.row_count),
                "{} native row count differs from its published fixture",
                fixture.provider
            );
            context.action(format!(
                "{} FE catalog/planning and BE admitted read returned {} rows from {} published files",
                fixture.provider, fixture.facts.row_count, fixture.facts.data_file_count
            ));
        }
        let baseline = resource_baseline(context)?;
        let paimon_table = fixtures.paimon.table_name(false);
        let budget_error = connection
            .query::<mysql::Row, _>(format!(
                "SELECT /*+ SET_VAR(query_mem_limit=1) */ * FROM {paimon_table}"
            ))
            .expect_err("one-byte query memory budget must reject the admitted Paimon read");
        ensure!(
            budget_error
                .to_string()
                .to_ascii_lowercase()
                .contains("memory"),
            "low-budget Paimon read failed for another reason: {budget_error}"
        );
        await_resource_convergence(context, &baseline, "Paimon BE capacity refusal")?;
        let recovered_rows: Option<u64> = connection
            .query_first(format!("SELECT COUNT(*) FROM {paimon_table}"))
            .context("read Paimon after BE capacity refusal")?;
        ensure!(
            recovered_rows == Some(fixtures.paimon.facts.row_count),
            "Paimon read after BE capacity refusal differed from its published fixture"
        );
        await_resource_convergence(context, &baseline, "Paimon BE capacity recovery")?;
        context.action(format!(
            "Paimon BE rejected a one-byte query memory budget and a normal-budget read recovered with {} rows",
            fixtures.paimon.facts.row_count
        ));
        let report = serde_json::json!({
            "schema_version": 1,
            "acceptance_status": "partial",
            "binary_sha256": sha256_file(context.primary_binary())?,
            "iceberg_fixture_sha256": fixtures.iceberg.manifest_sha256,
            "paimon_fixture_sha256": fixtures.paimon.manifest_sha256,
            "native_topology": "1FE+3BE",
            "covered": ["real Iceberg FE planning and BE read", "real Paimon FE planning and BE read", "Paimon BE query memory capacity refusal and recovery"],
            "uncovered": ["Paimon 256 MiB FE ledger", "Paimon catalog entries above 65536", "Paimon planned splits above 1000000", "Paimon planned files above 4000000", "Paimon listing above 16 MiB", "Paimon frozen metadata above 32 MiB", "Paimon split metadata above 256 MiB"],
        });
        fs::write(
            context
                .scenario_root()
                .join("uea4a4-no-fe-quota-coverage.json"),
            serde_json::to_vec_pretty(&report)?,
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.setup.teardown()
    }
}

impl IcebergHeldReadCancellation {
    fn new() -> Self {
        Self {
            setup: CatalogPlanningPerformance::new(true),
        }
    }
}

impl Scenario for IcebergHeldReadCancellation {
    fn name(&self) -> &'static str {
        "uea4/iceberg-held-read-cancellation"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        workload: Option<&Path>,
    ) -> Result<()> {
        self.setup.validate_runner_inputs(launch_profile, workload)
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let mut config = self.setup.launch_config(scenario_root)?;
        let (workload, _) = Workload::load()?;
        let frontend = config
            .config_overlay
            .fe
            .as_mut()
            .context("held-read scenario has no FE workload config")?;
        let original = format!(
            "concurrency_limit = {}",
            workload.warehouse_concurrency_limit
        );
        ensure!(
            frontend.contains(&original),
            "FE workload limit was not rendered"
        );
        *frontend = frontend.replacen(&original, "concurrency_limit = 1", 1);
        Ok(config)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixtures = self
            .setup
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))?;
        let fixtures = fixtures
            .as_ref()
            .context("UEA-4A-4 fixture was not prepared")?;
        let fixture = &fixtures.iceberg;
        let proxy = &fixtures.iceberg_slow;
        let mut control = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect Iceberg held-read setup session")?,
        )?;
        control
            .query_drop(fixture.catalog_sql(true, proxy.endpoint()))
            .context("create Iceberg held-read catalog")?;
        control
            .query_drop(
                fixtures
                    .paimon
                    .catalog_sql(false, &fixtures.paimon.facts.s3_endpoint),
            )
            .context("create independent Paimon follow-up catalog")?;
        let table = fixture.table_name(true);
        let read_sql = format!("SELECT COUNT(*) FROM {table} WHERE id BETWEEN 1 AND 1000");
        let followup_sql = format!("SELECT COUNT(*) FROM {}", fixtures.paimon.table_name(false));
        let followup_count = fixtures.paimon.facts.row_count;
        drop(control);

        for (owner, suffix) in [("sdk-manifest", ".avro"), ("native-data", ".parquet")] {
            context.action(format!(
                "hold real Iceberg {owner} GET/HEAD, cancel its query, then release the read"
            ));
            run_held_provider_read(
                context,
                proxy,
                &read_sql,
                &followup_sql,
                followup_count,
                "Iceberg",
                owner,
                Some(suffix),
                owner == "sdk-manifest",
            )?;
        }
        let mut control = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect post-cancellation Iceberg session")?,
        )?;
        let count: Option<u64> = control
            .query_first(format!("SELECT COUNT(*) FROM {table}"))
            .context("read Iceberg table after held cancellations")?;
        ensure!(
            count == Some(fixture.facts.row_count),
            "Iceberg post-cancellation row count differs from fixture"
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.setup.teardown()
    }
}

fn frontend_governance_snapshot(context: &mut ScenarioContext) -> Result<String> {
    let state = frontend_workload_state(context)?;
    Ok(format!(
        "active={} governance={}",
        state
            .pointer("/workload/active/statement")
            .unwrap_or(&serde_json::Value::Null),
        state
            .pointer("/workload/governance")
            .unwrap_or(&serde_json::Value::Null)
    ))
}

fn frontend_workload_state(context: &mut ScenarioContext) -> Result<serde_json::Value> {
    let timeout = context
        .remaining("read FE workload state")?
        .min(Duration::from_secs(2));
    let response = context
        .handle()
        .frontend_management_get("/v1/frontend/state", timeout)?;
    ensure!(
        response.status == 200,
        "FE workload HTTP {}",
        response.status
    );
    serde_json::from_str(&response.body).context("decode FE workload state")
}

pub(super) fn await_frontend_local_exit(
    context: &mut ScenarioContext,
    operation: &str,
) -> Result<()> {
    let deadline = Instant::now() + context.remaining(operation)?.min(Duration::from_secs(45));
    loop {
        let state = frontend_workload_state(context)?;
        let counts = [
            "/workload/active/statement",
            "/workload/governance/root_responsibilities",
            "/workload/governance/admitted_queries",
            "/workload/governance/preparation",
            "/workload/governance/execution",
            "/workload/governance/obligations",
            "/workload/governance/control_inflight",
        ];
        if counts
            .iter()
            .all(|path| state.pointer(path).and_then(serde_json::Value::as_u64) == Some(0))
        {
            context.action(format!(
                "{operation}: FE query and remote observation owners exited"
            ));
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "{operation} did not converge after the held object returned: active={} governance={}",
            state
                .pointer("/workload/active/statement")
                .unwrap_or(&serde_json::Value::Null),
            state
                .pointer("/workload/governance")
                .unwrap_or(&serde_json::Value::Null)
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn run_iceberg_held_deadline(
    context: &mut ScenarioContext,
    proxy: &DelayedS3Proxy,
    sql: &str,
    followup_sql: &str,
    followup_count: u64,
) -> Result<()> {
    let baseline = resource_baseline(context)?;
    let hold = proxy.hold_next_read_with_suffix(".avro")?;
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let user = context.mysql_user().to_owned();
    let port = context.mysql_port();
    let timeout = context.remaining("connect held Iceberg deadline query")?;
    let query = sql.to_owned();
    let query_thread = thread::spawn(move || -> Result<()> {
        let mut connection = mysql_actor::connect_for_cancellation(&user, port, timeout)?;
        connection
            .query_drop("SET query_timeout = 1")
            .context("set one-second statement deadline")?;
        ready_tx.send(())?;
        done_tx.send(connection.query::<u64, _>(query))?;
        Ok(())
    });
    let operation = (|| -> Result<()> {
        ready_rx
            .recv_timeout(context.remaining("await Iceberg deadline query start")?)
            .context("Iceberg deadline query did not start")?;
        hold.wait_until_entered(
            context
                .remaining("observe held Iceberg deadline manifest read")?
                .min(Duration::from_secs(30)),
        )?;
        let before = proxy.snapshot();
        let result = done_rx
            .recv_timeout(
                context
                    .remaining("await Iceberg statement deadline while S3 is held")?
                    .min(Duration::from_secs(10)),
            )
            .context("held Iceberg query did not reach its statement deadline")?;
        ensure!(
            matches!(result, Err(mysql::Error::MySqlError(ref error)) if error.message.to_ascii_lowercase().contains("timed out")),
            "held Iceberg query did not return a deadline terminal: {result:?}"
        );
        context.action(
            "Q1 returned a statement-timeout terminal while its SDK manifest read remained held",
        );
        let state = frontend_workload_state(context)?;
        ensure!(
            state
                .pointer("/workload/governance/admitted_queries")
                .and_then(serde_json::Value::as_u64)
                == Some(0),
            "Q1 deadline terminal retained the sole warehouse permit: {}",
            state
        );
        context.action(format!(
            "after Q1 deadline, FE workload active={} admitted=0 waiting={} preparation={}",
            state
                .pointer("/workload/active/statement")
                .unwrap_or(&serde_json::Value::Null),
            state
                .pointer("/workload/governance/waiting_records")
                .unwrap_or(&serde_json::Value::Null),
            state
                .pointer("/workload/governance/preparation")
                .unwrap_or(&serde_json::Value::Null),
        ));

        let (followup_tx, followup_rx) = mpsc::sync_channel(1);
        let followup_user = context.mysql_user().to_owned();
        let followup_port = context.mysql_port();
        let followup_query = followup_sql.to_owned();
        let followup_connect_timeout = context
            .remaining("connect Q2 after Iceberg deadline terminal")?
            .min(Duration::from_secs(10));
        let followup_thread = thread::spawn(move || {
            let result = (|| -> Result<Option<u64>> {
                let mut connection = mysql_actor::connect_for_cancellation(
                    &followup_user,
                    followup_port,
                    followup_connect_timeout,
                )?;
                connection
                    .query_first(followup_query)
                    .context("read Q2 after Iceberg deadline")
            })();
            let _ = followup_tx.send(result);
        });
        let observed = match followup_rx.recv_timeout(
            context
                .remaining("await Q2 while expired Iceberg read remains held")?
                .min(Duration::from_secs(10)),
        ) {
            Ok(result) => result?,
            Err(error) => {
                let snapshot = frontend_governance_snapshot(context)
                    .unwrap_or_else(|state_error| format!("unavailable: {state_error:#}"));
                hold.release();
                let after_release = followup_rx
                    .recv_timeout(Duration::from_secs(10))
                    .map(|result| format!("{result:?}"))
                    .unwrap_or_else(|release_error| format!("unavailable: {release_error}"));
                anyhow::bail!(
                    "Q2 did not complete before the expired Q1 read was released: {error}; FE workload: {snapshot}; Q2 after release: {after_release}"
                );
            }
        };
        followup_thread
            .join()
            .map_err(|_| anyhow::anyhow!("Iceberg deadline follow-up actor panicked"))?;
        ensure!(
            observed == Some(followup_count),
            "Q2 returned {observed:?} while the expired Q1 read was held"
        );
        context.action("independent Q2 completed before the expired Q1 object read was released");

        hold.release();
        hold.wait_until_forwarded(
            context
                .remaining("await released Iceberg deadline read forwarding")?
                .min(Duration::from_secs(30)),
        )?;
        let after = proxy.snapshot();
        ensure!(
            after.gets + after.heads >= before.gets + before.heads
                && after.upstream_errors == before.upstream_errors,
            "released Iceberg deadline manifest read did not complete cleanly"
        );
        context
            .action("released Q1 SDK manifest object read finished forwarding from the real store");
        Ok(())
    })();
    hold.release();
    if operation.is_ok() {
        query_thread
            .join()
            .map_err(|_| anyhow::anyhow!("Iceberg deadline query actor panicked"))??;
        await_frontend_local_exit(context, "Iceberg held deadline")?;
        await_resource_convergence(context, &baseline, "Iceberg held deadline")?;
    }
    operation
}

fn run_held_provider_read(
    context: &mut ScenarioContext,
    proxy: &DelayedS3Proxy,
    sql: &str,
    followup_sql: &str,
    followup_count: u64,
    provider: &str,
    owner: &str,
    suffix: Option<&str>,
    expect_independent_followup: bool,
) -> Result<()> {
    let baseline = resource_baseline(context)?;
    let hold = match suffix {
        Some(suffix) if suffix.starts_with('/') => proxy.hold_next_read_with_path_suffix(suffix)?,
        Some(suffix) => proxy.hold_next_read_with_suffix(suffix)?,
        None => proxy.hold_next_read()?,
    };
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let user = context.mysql_user().to_owned();
    let port = context.mysql_port();
    let timeout = context.remaining("connect held provider query")?;
    let query = sql.to_owned();
    let query_thread = thread::spawn(move || -> Result<()> {
        let mut connection = mysql_actor::connect_for_cancellation(&user, port, timeout)?;
        ready_tx.send(connection.connection_id())?;
        done_tx.send(connection.query::<u64, _>(query))?;
        Ok(())
    });
    let operation = (|| -> Result<()> {
        let connection_id = ready_rx
            .recv_timeout(context.remaining("receive held provider connection ID")?)
            .with_context(|| format!("held {provider} query did not publish its connection ID"))?;
        hold.wait_until_entered(
            context
                .remaining("observe real held provider object read")?
                .min(Duration::from_secs(30)),
        )?;
        let held_snapshot = proxy.snapshot();
        let (kill_tx, kill_rx) = mpsc::sync_channel(1);
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        let kill_timeout = context.remaining("connect held provider KILL QUERY session")?;
        let kill_thread = thread::spawn(move || -> Result<()> {
            let mut control = mysql_actor::connect(&user, port, kill_timeout)?;
            kill_tx.send(control.query_drop(format!("KILL QUERY {connection_id}")))?;
            Ok(())
        });
        let kill_result = kill_rx
            .recv_timeout(
                context
                    .remaining("await held provider KILL QUERY control")?
                    .min(Duration::from_secs(10)),
            )
            .with_context(|| {
                format!("{owner} KILL QUERY did not respond while S3 read was held")
            })?;
        kill_result.with_context(|| format!("cancel held {provider} {owner} query"))?;
        let early_result = match done_rx.try_recv() {
            Ok(result) => Some(result),
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                anyhow::bail!("{provider} {owner} query actor exited without a result")
            }
        };
        if expect_independent_followup {
            // This is the FE metadata/source-open case: cancellation releases
            // the only warehouse slot while the old local I/O still runs.
            // A held BE data reader belongs to task cleanup and has a
            // different terminal boundary.
            let mut followup = mysql_actor::connect(
                context.mysql_user(),
                context.mysql_port(),
                context
                    .remaining("connect follow-up query while provider read is held")?
                    .min(Duration::from_secs(10)),
            )?;
            let observed: Option<u64> = followup.query_first(followup_sql).with_context(|| {
                format!("independent query did not complete while {owner} read was held")
            })?;
            ensure!(
                observed == Some(followup_count),
                "independent query returned {observed:?} while {owner} read was held"
            );
        }
        hold.release();
        hold.wait_until_forwarded(
            context
                .remaining("await held provider object read forwarding")?
                .min(Duration::from_secs(30)),
        )?;
        context.action(format!(
            "{provider} {owner} held object read finished forwarding after release"
        ));
        kill_thread
            .join()
            .map_err(|_| anyhow::anyhow!("{provider} {owner} KILL QUERY actor panicked"))??;
        let result = match early_result {
            Some(result) => result,
            None => done_rx
                .recv_timeout(context.remaining("await held provider query exit")?)
                .with_context(|| {
                    format!("{provider} {owner} query did not exit after S3 release")
                })?,
        };
        ensure!(
            matches!(result, Err(mysql::Error::MySqlError(ref error)) if error.code == 1317),
            "{provider} {owner} query did not report cancellation after S3 release: {result:?}"
        );
        let after = proxy.snapshot();
        ensure!(
            after.gets + after.heads >= held_snapshot.gets + held_snapshot.heads
                && after.upstream_errors == held_snapshot.upstream_errors,
            "{provider} {owner} S3 proxy did not preserve the held read"
        );
        Ok(())
    })();
    hold.release();
    if operation.is_ok() {
        query_thread
            .join()
            .map_err(|_| anyhow::anyhow!("{provider} {owner} query actor panicked"))??;
        await_frontend_local_exit(context, &format!("{provider} {owner} held read"))?;
        await_resource_convergence(context, &baseline, &format!("{provider} {owner} held read"))?;
    }
    operation
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(CatalogPlanningPerformance::new(true)),
        Box::new(CatalogPlanningPerformance::new(false)),
        Box::new(IcebergHeldReadCancellation::new()),
        Box::new(CatalogPlanningNoFeQuota::new()),
        Box::new(CatalogPlanningResponsibility::new()),
    ]
}
