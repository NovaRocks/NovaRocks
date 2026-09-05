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

use crate::benchmark_bootstrap::{
    BenchmarkBootstrapOptions, ReadyBenchmarkFixture, ensure_benchmark_data,
};
use crate::cluster::{ClusterMode, launch_server};
use crate::config::{
    TestLane, build_suite_configs, load_runner_config, resolve_config_path, resolve_repo_root,
};
use crate::{Cli, Mode, RecordFrom, run_cli};
use anyhow::{Context, Result, bail};
use clap::{ArgAction, Parser};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const SCENARIO_FILE: &str = "scenario.toml";
const WARMUP_RUNS: u32 = 1;
const MEASURED_RUNS: u32 = 5;

#[derive(Debug, Parser)]
#[command(
    name = "novarocks-sql-benchmark",
    about = "Run release SQL benchmarks from tests/sql/benchmarks/"
)]
struct BenchmarkCli {
    /// Benchmark workload name(s), comma-separated. Use "all" for every workload.
    #[arg(long, required_unless_present = "list_suites")]
    suite: Option<String>,
    /// Print the deterministic benchmark workload names and exit.
    #[arg(long, action = ArgAction::SetTrue)]
    list_suites: bool,
    #[arg(long)]
    config: Option<String>,
    /// Select only these query IDs, comma-separated.
    #[arg(long)]
    only: Option<String>,
    #[arg(long)]
    query_timeout: Option<u64>,
    /// Directory for immutable benchmark reports. A timestamped child is created per run.
    #[arg(long, default_value = "reports/sql-benchmarks")]
    output_dir: String,
    /// Rebuild a fixture explicitly. ReadyInvalid still fails closed.
    #[arg(long, action = ArgAction::SetTrue)]
    rebuild: bool,
    /// Number of BE processes for this cross-process benchmark run (>= 1).
    #[arg(long, default_value_t = 1, value_parser = parse_backend_count)]
    backend_count: usize,
    /// Optional JSON object supplied by the external benchmark controller.
    #[arg(long, value_name = "JSON_OBJECT", value_parser = parse_controller_environment)]
    controller_environment: Option<Value>,
    /// Optional comparison identity supplied by the external benchmark controller.
    #[arg(long, value_parser = parse_comparison_key)]
    comparison_key: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BenchmarkScenario {
    schema_version: u32,
    scenario: String,
    workload: String,
    scale: String,
    distribution: String,
    file_layout: String,
    cache_mode: String,
    query_order: String,
    warmup_runs: u32,
    measured_runs: u32,
    concurrency: u32,
}

#[derive(Debug, Serialize)]
struct BenchmarkTopology {
    mode: &'static str,
    frontend_count: usize,
    backend_count: usize,
}

#[derive(Debug, Serialize)]
struct WorkloadReport<'a> {
    workload: &'a str,
    scenario: &'a BenchmarkScenario,
    fixture: &'a ReadyBenchmarkFixture,
    verification: &'static str,
    warmup_runs: u32,
    measured_runs: u32,
    concurrency: u32,
    topology: BenchmarkTopology,
    binary: String,
    controller_environment: Option<&'a Value>,
    comparison_key: Option<&'a str>,
}

fn parse_controller_environment(raw: &str) -> std::result::Result<Value, String> {
    let value: Value = serde_json::from_str(raw)
        .map_err(|error| format!("--controller-environment must be a JSON object: {error}"))?;
    if !value.is_object() {
        return Err("--controller-environment must be a JSON object".to_owned());
    }
    Ok(value)
}

fn parse_backend_count(raw: &str) -> std::result::Result<usize, String> {
    let count = raw
        .parse::<usize>()
        .map_err(|error| format!("--backend-count must be an integer >= 1: {error}"))?;
    if count == 0 {
        return Err("--backend-count must be >= 1".to_owned());
    }
    Ok(count)
}

fn parse_comparison_key(raw: &str) -> std::result::Result<String, String> {
    if raw.trim().is_empty() {
        return Err("--comparison-key must not be empty".to_owned());
    }
    Ok(raw.to_owned())
}

pub fn run() -> Result<()> {
    let cli = BenchmarkCli::parse();
    let base_dir = resolve_repo_root()?;
    let workloads = build_suite_configs(&base_dir, TestLane::Benchmark)?;
    if workloads.is_empty() {
        bail!("no benchmark workload directories found under tests/sql/benchmarks");
    }
    if cli.list_suites {
        for name in workloads.keys() {
            println!("{name}");
        }
        return Ok(());
    }
    if cfg!(debug_assertions) {
        bail!("benchmark runner must be built with cargo --release");
    }
    let release_binary = require_release_binary(&base_dir)?;
    let names = select_workloads(
        cli.suite.as_deref().expect("clap requires --suite"),
        &workloads,
    )?;
    let config_path = resolve_config_path(cli.config.as_deref(), &base_dir);
    let runner_config = load_runner_config(config_path.as_deref())?;
    let run_root = create_run_root(&base_dir, &cli.output_dir)?;
    for workload in names {
        let scenario = load_scenario(&base_dir, &workload)?;
        validate_scenario(&scenario, &workload)?;
        let fixture = ensure_fixture(&runner_config, &base_dir, &workload, &scenario, cli.rebuild)?;
        let workload_root = run_root.join(&workload);
        fs::create_dir_all(&workload_root)?;
        run_workload_protocol(
            &base_dir,
            &runner_config,
            &workload,
            &cli,
            &fixture,
            &workload_root,
        )?;
        write_samples_csv(&workload_root)?;
        let report = WorkloadReport {
            workload: &workload,
            scenario: &scenario,
            fixture: &fixture,
            verification: "passed",
            warmup_runs: WARMUP_RUNS,
            measured_runs: MEASURED_RUNS,
            concurrency: 1,
            topology: BenchmarkTopology {
                mode: "cross-process",
                frontend_count: 1,
                backend_count: cli.backend_count,
            },
            binary: release_binary.display().to_string(),
            controller_environment: cli.controller_environment.as_ref(),
            comparison_key: cli.comparison_key.as_deref(),
        };
        fs::write(
            workload_root.join("run.json"),
            serde_json::to_vec_pretty(&report)?,
        )?;
        fs::write(workload_root.join("SUMMARY.md"), render_summary(&report)?)?;
    }
    println!("benchmark reports written to {}", run_root.display());
    Ok(())
}

fn require_release_binary(base_dir: &Path) -> Result<PathBuf> {
    let binary = PathBuf::from(env::var_os("NOVAROCKS_BIN").context(
        "benchmark runner requires NOVAROCKS_BIN to name a target/release/novarocks binary",
    )?)
    .canonicalize()
    .context("canonicalize NOVAROCKS_BIN")?;
    let release_root = base_dir
        .join("target/release")
        .canonicalize()
        .with_context(|| {
            format!(
                "canonicalize expected release directory {}",
                base_dir.join("target/release").display()
            )
        })?;
    if !binary.starts_with(&release_root)
        || binary.file_name().is_none_or(|name| name != "novarocks")
    {
        bail!(
            "benchmark runner requires NOVAROCKS_BIN under {}/novarocks, got {}",
            release_root.display(),
            binary.display()
        );
    }
    Ok(binary)
}

fn select_workloads(
    raw: &str,
    workloads: &BTreeMap<String, crate::types::SuiteConfig>,
) -> Result<Vec<String>> {
    if raw == "all" {
        return Ok(workloads.keys().cloned().collect());
    }
    let selected = raw
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if selected.is_empty() {
        bail!("benchmark --suite must name a workload or all");
    }
    for name in &selected {
        if !workloads.contains_key(name) {
            bail!("unknown benchmark workload {name}; use --list-suites");
        }
    }
    Ok(selected)
}

fn load_scenario(base_dir: &Path, workload: &str) -> Result<BenchmarkScenario> {
    let path = TestLane::Benchmark
        .suite_root(base_dir)
        .join(workload)
        .join(SCENARIO_FILE);
    let source =
        fs::read_to_string(&path).with_context(|| format!("read scenario {}", path.display()))?;
    toml::from_str(&source).with_context(|| format!("parse scenario {}", path.display()))
}

fn validate_scenario(scenario: &BenchmarkScenario, workload: &str) -> Result<()> {
    if scenario.schema_version != 1
        || scenario.workload != workload
        || scenario.scale.trim().is_empty()
        || scenario.scenario.trim().is_empty()
        || scenario.distribution.trim().is_empty()
        || scenario.file_layout.trim().is_empty()
        || scenario.cache_mode.trim().is_empty()
    {
        bail!("benchmark scenario for {workload} is incomplete or has an unsupported schema");
    }
    if scenario.query_order != "lexical"
        || scenario.warmup_runs != WARMUP_RUNS
        || scenario.measured_runs != MEASURED_RUNS
        || scenario.concurrency != 1
    {
        bail!(
            "benchmark scenario for {workload} must use lexical order, one warmup, five measured runs, and concurrency 1"
        );
    }
    Ok(())
}

fn ensure_fixture(
    runner_config: &crate::types::RunnerConfig,
    base_dir: &Path,
    workload: &str,
    scenario: &BenchmarkScenario,
    rebuild: bool,
) -> Result<ReadyBenchmarkFixture> {
    let mut scales = BTreeMap::new();
    scales.insert(workload.to_owned(), scenario.scale.clone());
    ensure_benchmark_data(
        &BenchmarkBootstrapOptions {
            enabled: true,
            rebuild,
            scales,
        },
        runner_config,
        base_dir,
        workload,
    )?
    .ok_or_else(|| anyhow::anyhow!("benchmark workload {workload} has no fixture lifecycle"))
}

struct BenchmarkClusterTarget {
    host: String,
    port: String,
}

fn run_workload_protocol(
    base_dir: &Path,
    runner_config: &crate::types::RunnerConfig,
    workload: &str,
    benchmark: &BenchmarkCli,
    fixture: &ReadyBenchmarkFixture,
    workload_root: &Path,
) -> Result<()> {
    let mut server = launch_server(
        ClusterMode::CrossProcess,
        benchmark.backend_count,
        base_dir,
        runner_config,
        false,
        false,
    )?;
    let target = BenchmarkClusterTarget {
        host: server
            .target_host()
            .unwrap_or("127.0.0.1")
            .to_owned(),
        port: server
            .target_port()
            .context("benchmark cluster did not expose a MySQL port")?
            .to_string(),
    };
    let protocol_run_id = format!(
        "sqlb_{workload}_{:x}_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        std::process::id()
    );

    let primary = (|| -> Result<()> {
        run_pass(
            workload,
            benchmark,
            fixture,
            &target,
            &protocol_run_id,
            "verify",
            &workload_root.join("verify.csv"),
            None,
            false,
            true,
            false,
        )?;
        run_pass(
            workload,
            benchmark,
            fixture,
            &target,
            &protocol_run_id,
            "warmup-1",
            &workload_root.join("warmup-1.csv"),
            None,
            true,
            true,
            false,
        )?;
        for run in 1..=MEASURED_RUNS {
            run_pass(
                workload,
                benchmark,
                fixture,
                &target,
                &protocol_run_id,
                &format!("measured-{run}"),
                &workload_root.join(format!("measured-{run}.csv")),
                None,
                true,
                true,
                false,
            )?;
        }
        run_pass(
            workload,
            benchmark,
            fixture,
            &target,
            &protocol_run_id,
            "profile",
            &workload_root.join("profile.csv"),
            Some(workload_root.join("profiles")),
            true,
            false,
            false,
        )
    })();

    let cleanup_result = if primary.is_err() {
        run_pass(
            workload,
            benchmark,
            fixture,
            &target,
            &protocol_run_id,
            "cleanup-after-failure",
            &workload_root.join("cleanup-after-failure.csv"),
            None,
            true,
            false,
            true,
        )
    } else {
        Ok(())
    };
    let shutdown_result = server.shutdown();
    let residual = server.residual_process_ids();

    match (primary, cleanup_result, shutdown_result, residual.is_empty()) {
        (Ok(()), Ok(()), Ok(()), true) => Ok(()),
        (Err(primary), Ok(()), Ok(()), true) => Err(primary),
        (primary, cleanup, shutdown, no_residual) => {
            let mut errors = Vec::new();
            if let Err(error) = primary {
                errors.push(format!("benchmark phase failed: {error:#}"));
            }
            if let Err(error) = cleanup {
                errors.push(format!("suite cleanup failed: {error:#}"));
            }
            if let Err(error) = shutdown {
                errors.push(format!("cluster shutdown failed: {error:#}"));
            }
            if !no_residual {
                errors.push(format!("residual benchmark server processes: {residual:?}"));
            }
            bail!(errors.join("; "))
        }
    }
}

fn run_pass(
    workload: &str,
    benchmark: &BenchmarkCli,
    fixture: &ReadyBenchmarkFixture,
    target: &BenchmarkClusterTarget,
    protocol_run_id: &str,
    phase: &str,
    timing_path: &Path,
    profile_dir: Option<PathBuf>,
    skip_init: bool,
    skip_cleanup: bool,
    cleanup_only: bool,
) -> Result<()> {
    println!("[{workload}] benchmark phase={phase}");
    let cli = Cli {
        suite: Some(workload.to_owned()),
        list_extensions: false,
        list_suites: false,
        config: benchmark.config.clone(),
        mode: Mode::Verify,
        record_from: RecordFrom::Reference,
        sql_dir: None,
        result_dir: None,
        sql_glob: None,
        mysql: None,
        host: Some(target.host.clone()),
        port: Some(target.port.clone()),
        user: None,
        password: None,
        ref_mysql: None,
        ref_host: None,
        ref_port: None,
        ref_user: None,
        ref_password: None,
        query_timeout: benchmark.query_timeout,
        verify: phase_requires_result_verification(phase),
        no_verify: !phase_requires_result_verification(phase),
        update_expected: false,
        write_actual_dir: None,
        failure_artifact_dir: None,
        only: benchmark.only.clone(),
        skip: None,
        limit: None,
        order_sensitive_default: false,
        float_epsilon: None,
        preview_lines: 3,
        cluster_mode: ClusterMode::CrossProcess,
        cluster_size: Some(benchmark.backend_count),
        target_session_sql: Vec::new(),
        rewrite_explain_contains_as_not_contains: Vec::new(),
        dry_run: false,
        fail_fast: true,
        case_timing_output: Some(timing_path.display().to_string()),
        benchmark_warehouse: Some(fixture.exact_warehouse.clone()),
        benchmark_profile_dir: profile_dir.map(|path| path.display().to_string()),
        benchmark_external_cluster: true,
        benchmark_skip_init: skip_init,
        benchmark_skip_cleanup: skip_cleanup,
        benchmark_cleanup_only: cleanup_only,
        benchmark_run_id: Some(protocol_run_id.to_owned()),
        jobs: 1,
    };
    if run_cli(cli, TestLane::Benchmark, "benchmark")? != 0 {
        bail!("benchmark {workload} failed during {phase}");
    }
    Ok(())
}

fn phase_requires_result_verification(phase: &str) -> bool {
    phase != "profile"
}

fn render_summary(report: &WorkloadReport<'_>) -> Result<String> {
    let topology = &report.topology;
    let environment = match report.controller_environment {
        Some(environment) => format!(
            "\n- Controller environment:\n\n```json\n{}\n```\n",
            serde_json::to_string_pretty(environment)?
        ),
        None => "\n- Controller environment: not provided\n".to_owned(),
    };
    let comparison_key = report.comparison_key.unwrap_or("not provided");
    Ok(format!(
        "# {} benchmark\n\n- Verification: {}\n- Topology: {} ({}FE+{}BE)\n- Binary: `{}`\n- Fixture: {}\n- Warmup runs: {}\n- Measured runs: {}\n- Concurrency: {}\n- Query order: {}\n- Comparison key: {}\n{}\nSee `samples.csv`, `run.json`, and `profiles/`.\n",
        report.workload,
        report.verification,
        topology.mode,
        topology.frontend_count,
        topology.backend_count,
        report.binary,
        report.fixture.publication_identity,
        report.warmup_runs,
        report.measured_runs,
        report.concurrency,
        report.scenario.query_order,
        comparison_key,
        environment,
    ))
}

fn create_run_root(base_dir: &Path, output_dir: &str) -> Result<PathBuf> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let root = base_dir.join(output_dir).join(format!("run-{millis}"));
    fs::create_dir_all(&root)?;
    Ok(root)
}

fn write_samples_csv(workload_root: &Path) -> Result<()> {
    let mut out = String::from("run,suite,case,status,elapsed_seconds\n");
    for run in 1..=MEASURED_RUNS {
        for line in fs::read_to_string(workload_root.join(format!("measured-{run}.csv")))?
            .lines()
            .skip(1)
        {
            out.push_str(&format!("{run},{line}\n"));
        }
    }
    fs::write(workload_root.join("samples.csv"), out).context("write benchmark samples")
}

#[cfg(test)]
mod tests {
    use super::{
        BenchmarkCli, BenchmarkScenario, BenchmarkTopology, WorkloadReport,
        parse_controller_environment, phase_requires_result_verification, render_summary,
        validate_scenario,
    };
    use crate::benchmark_bootstrap::ReadyBenchmarkFixture;
    use clap::{CommandFactory, Parser};
    use std::collections::BTreeMap;
    #[test]
    fn benchmark_help_mentions_only_the_benchmark_root() {
        let help = BenchmarkCli::command().render_long_help().to_string();
        assert!(help.contains("tests/sql/benchmarks/"));
        assert!(!help.contains("tests/sql/correctness/"));
        assert!(help.contains("--backend-count"));
        assert!(help.contains("--controller-environment"));
        assert!(help.contains("--comparison-key"));
    }
    #[test]
    fn benchmark_scenario_rejects_non_serial_protocol() {
        let scenario = BenchmarkScenario {
            schema_version: 1,
            scenario: "default".to_string(),
            workload: "ssb".to_string(),
            scale: "1".to_string(),
            distribution: "standard".to_string(),
            file_layout: "standard".to_string(),
            cache_mode: "warm".to_string(),
            query_order: "lexical".to_string(),
            warmup_runs: 1,
            measured_runs: 4,
            concurrency: 1,
        };
        assert!(validate_scenario(&scenario, "ssb").is_err());
    }

    #[test]
    fn benchmark_cli_rejects_zero_backend_count_and_non_object_environment() {
        assert!(
            BenchmarkCli::try_parse_from(["benchmark", "--suite", "ssb", "--backend-count", "0",])
                .is_err()
        );
        assert!(
            BenchmarkCli::try_parse_from([
                "benchmark",
                "--suite",
                "ssb",
                "--controller-environment",
                "[\"not-an-object\"]",
            ])
            .is_err()
        );
    }

    #[test]
    fn profile_phase_disables_expected_result_comparison() {
        assert!(phase_requires_result_verification("verify"));
        assert!(phase_requires_result_verification("measured-5"));
        assert!(!phase_requires_result_verification("profile"));
    }

    #[test]
    fn report_preserves_controller_metadata_and_selected_topology() {
        let scenario = BenchmarkScenario {
            schema_version: 1,
            scenario: "sf1".to_owned(),
            workload: "ssb".to_owned(),
            scale: "1".to_owned(),
            distribution: "standard".to_owned(),
            file_layout: "standard".to_owned(),
            cache_mode: "warm".to_owned(),
            query_order: "lexical".to_owned(),
            warmup_runs: 1,
            measured_runs: 5,
            concurrency: 1,
        };
        let fixture = ReadyBenchmarkFixture {
            dataset_key: BTreeMap::new(),
            exact_warehouse: "s3://fixture".to_owned(),
            manifest_uri: "s3://fixture/manifest".to_owned(),
            ready_uri: "s3://fixture/READY".to_owned(),
            publication_identity: "publication-1".to_owned(),
            publication_etag: "etag-1".to_owned(),
            reused: true,
            built: false,
        };
        let environment = parse_controller_environment(r#"{"machine":"controller-owned"}"#)
            .expect("environment object");
        let report = WorkloadReport {
            workload: "ssb",
            scenario: &scenario,
            fixture: &fixture,
            verification: "passed",
            warmup_runs: 1,
            measured_runs: 5,
            concurrency: 1,
            topology: BenchmarkTopology {
                mode: "cross-process",
                frontend_count: 1,
                backend_count: 2,
            },
            binary: "target/release/novarocks".to_owned(),
            controller_environment: Some(&environment),
            comparison_key: Some("pool-a"),
        };
        let json = serde_json::to_value(&report).expect("serialize report");
        assert_eq!(json["topology"]["backend_count"], 2);
        assert_eq!(json["controller_environment"], environment);
        assert_eq!(json["comparison_key"], "pool-a");
        let summary = render_summary(&report).expect("render summary");
        assert!(summary.contains("cross-process (1FE+2BE)"));
        assert!(summary.contains("Comparison key: pool-a"));
        assert!(summary.contains(r#""machine": "controller-owned""#));
    }
}
