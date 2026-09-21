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

//! Bounded native concurrency evidence. One sleeping row keeps each admitted
//! driver in a cancellation-aware query without an unbounded CPU or memory
//! allocation. Every session uses a five-second statement deadline, so timeout
//! cancellation stops both active and queued roots; single-query KILL QUERY
//! protocol conformance is covered by its dedicated scenarios.

use crate::actors::mysql as mysql_actor;
use crate::actors::mysql_stream::{AsyncMysqlStream, MysqlStream};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::process_resources::{
    ProcessResourceMonitor, ProcessResourceSampler,
};
use novarocks_cluster_harness::{
    CrossProcessConfigOverlay, CrossProcessNativeFaultProxyConfig, QueryExecutionResourceSnapshot,
    ServerHandle,
    native_fault_proxy::{NativeFaultProxyControl, ProxyDirection, ProxyMode},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::sync::{Arc, Barrier as StdBarrier, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime};
use tokio::sync::Barrier;
use tokio::task::{JoinHandle, JoinSet};

const REQUIRED_BACKENDS: usize = 3;
const TIERS: [usize; 3] = [16, 64, 256];
const CONCURRENCY_LIMIT: usize = 3;
const CONTROL_READY_LIMIT: usize = 256;
const WAITING_LIMIT: usize = 512;
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
// The role can start a small bounded worker wave as its network fan-in grows.
// Keep a platform-safe absolute allowance while asserting that the 256-root
// tier does not grow proportionally to client roots.
const FIXED_RUNTIME_THREAD_ALLOWANCE: u64 = 12;
const IO_TIMEOUT_CAP: Duration = Duration::from_secs(10);
const QUERY_TIMEOUT_SECONDS: u64 = 5;
const TERMINAL_TIMEOUT_SECONDS: u64 = 1;
const REMOTE_CLEANUP_TIMEOUT_MS: u64 = 500;
// This scenario must prove 256 logical MySQL roots, not allocate one OS
// thread per root. Sixteen I/O workers keep the 256-socket fan-in making
// progress while preserving a fixed, platform-safe harness bound.
const CLIENT_RUNTIME_THREADS: usize = 16;
// `sleep` evaluates in the BE after the one-row source is distributed. It is
// deliberately a one-row source so each of the three admitted drivers uses
// bounded memory while queued roots demonstrate the actual governance limit.
const HELD_QUERY: &str = "SELECT sleep(10) FROM TABLE(generate_series(1, 1))";
const PERFORMANCE_WINDOW: Duration = Duration::from_secs(120);
const PERFORMANCE_REPETITIONS: usize = 3;
const PERFORMANCE_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const PERFORMANCE_NORMAL_QUERY: &str = "SELECT SUM(i) FROM TABLE(generate_series(1, 1000)) AS t(i)";
const PERFORMANCE_SATURATED_QUERY: &str = "SELECT sleep(1) FROM TABLE(generate_series(1, 1))";
const PERFORMANCE_NORMAL_THINK_TIME: Duration = Duration::from_millis(20);
const PERFORMANCE_SATURATED_CLIENTS: usize = CONCURRENCY_LIMIT + 1;

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(QueryConcurrency),
        Box::new(WarehouseAdmission),
        Box::new(TerminalReleasesSlot),
        Box::new(RemoteCleanupRetires),
        Box::new(LateWorkLeaseExpiry),
        Box::new(Uea4a1BaselinePerformance),
        Box::new(Uea4a1CandidatePerformance),
    ]
}

struct QueryConcurrency;

/// A compact, functional companion to the 16/64/256 fan-in measurement.
///
/// It uses the same public MySQL and native 1FE+3BE path, but stops once the
/// warehouse queue has been observed. This makes the single warehouse permit
/// an independently runnable product acceptance case rather than an inferred
/// property of the larger thread-growth experiment.
struct WarehouseAdmission;

impl Scenario for WarehouseAdmission {
    fn name(&self) -> &'static str {
        "query-concurrency/warehouse-admission"
    }

    fn launch_config(&self, scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        QueryConcurrency.launch_config(scenario_root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let monitor = ProcessResourceMonitor::start_with_identities(
            context.process_resource_identities()?,
            context.name(),
            SAMPLE_INTERVAL,
        )?;
        let result = run_tier(context, &monitor, CONCURRENCY_LIMIT + 1);
        let resource_path = context.scenario_root().join("process-resources.json");
        let _resources = monitor.finish(&resource_path)?;
        result?;
        context.action(format!(
            "observed one warehouse queue behind {CONCURRENCY_LIMIT} admitted queries in native 1FE+3BE"
        ));
        Ok(())
    }
}

/// A query's terminal result must wake the next logical query even when the
/// first query's Worker has not yet received its stop request.
struct TerminalReleasesSlot;

impl Scenario for TerminalReleasesSlot {
    fn name(&self) -> &'static str {
        "query-concurrency/terminal-releases-slot"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(fault_launch_config(1, REMOTE_CLEANUP_TIMEOUT_MS))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline_resources = query_resources(context)?;
        let creates = task_create_counts(context)?;
        let mut first = start_timed_hold(context)?;
        let target = await_fresh_task_create(context, &creates)?;
        let process_id = context.process_ids().backends[target];
        let proxy = native_proxy(context, target)?;
        await_proxy_connection(context, &proxy, target)?;
        pause_native(&proxy);
        await_resource_activity(context, &baseline_resources)?;
        context.action(format!(
            "paused an admitted q1 on BE[{target}] while retaining process identity {process_id}"
        ));

        let second = start_follow_up_query(context)?;
        await_frontend_state(context, "observe q2 waiting behind q1", |state| {
            state.workload.active.statement == 2
                && state.workload.governance.admitted_queries == 1
                && state.workload.governance.waiting_records > 0
        })?;
        context.action("observed q2 in the sole warehouse admission queue");

        assert_timeout_terminal(first.read_timeout_query_error()?, "q1")?;
        let state =
            await_frontend_state(context, "observe q1 terminal releases its slot", |state| {
                state.workload.governance.admitted_queries == 1
                    && state.workload.governance.waiting_records == 0
            })?;
        ensure!(
            query_resources(context)? != baseline_resources,
            "q1 Worker resources disappeared before q2 could use q1's terminal release: {}",
            state.diagnostic()
        );
        context.action(
            "received q1 terminal and observed q2 hold the released permit while q1 BE work remained live",
        );

        let rows = second
            .done
            .recv_timeout(context.remaining("await q2 after q1 terminal")?)
            .context("q2 did not start after q1 released the warehouse slot")??;
        second
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("q2 actor panicked"))??;
        ensure!(rows == vec![1], "q2 returned unexpected rows: {rows:?}");
        ensure!(
            query_resources(context)? != baseline_resources,
            "q1 physical work ended before the scenario could prove the terminal/physical overlap"
        );

        resume_native(&proxy);
        await_resource_convergence(context, &baseline_resources)?;
        ensure!(
            context.process_ids().backends[target] == process_id,
            "BE[{target}] was replaced while terminal-release scenario only partitioned it"
        );
        context.action(
            "q2 completed before q1's delayed Worker cleanup; restoring Native transport then converged real resources",
        );
        Ok(())
    }
}

/// The Frontend may end its local remote-observation record after the cleanup
/// deadline, but it must not fabricate a Worker stop or require endpoint
/// replacement to make that local retirement possible.
struct RemoteCleanupRetires;

impl Scenario for RemoteCleanupRetires {
    fn name(&self) -> &'static str {
        "query-concurrency/remote-cleanup-retires"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(fault_launch_config(1, REMOTE_CLEANUP_TIMEOUT_MS))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline_resources = query_resources(context)?;
        let baseline_governance = frontend_state(context)?.workload.governance;
        let creates = task_create_counts(context)?;
        let mut stream = start_timed_hold(context)?;
        let target = await_fresh_task_create(context, &creates)?;
        let process_id = context.process_ids().backends[target];
        let proxy = native_proxy(context, target)?;
        await_proxy_connection(context, &proxy, target)?;
        pause_native(&proxy);
        await_resource_activity(context, &baseline_resources)?;
        assert_timeout_terminal(stream.read_timeout_query_error()?, "partitioned query")?;

        let retired = await_frontend_state(
            context,
            "await finite remote tracking retirement",
            |state| {
                let governance = &state.workload.governance;
                governance.admitted_queries == 0
                    && governance.obligations <= baseline_governance.obligations
                    && governance.old_attempts <= baseline_governance.old_attempts
                    && governance.unknown_creates <= baseline_governance.unknown_creates
                    && governance.obligations_tracking_ended_remote_unknown
                        > baseline_governance.obligations_tracking_ended_remote_unknown
            },
        )?;
        ensure!(
            query_resources(context)? != baseline_resources,
            "remote cleanup reported tracking-ended only after BE resources had already converged: {}",
            retired.diagnostic()
        );
        ensure!(
            context.process_ids().backends[target] == process_id,
            "finite FE retirement required replacing BE[{target}]"
        );
        context.action(format!(
            "retired q1 remote observation with obligations={} old_attempts={} unknown_creates={} and remote_unknown_endings={} while BE[{target}] remained live",
            retired.workload.governance.obligations,
            retired.workload.governance.old_attempts,
            retired.workload.governance.unknown_creates,
            retired.workload.governance.obligations_tracking_ended_remote_unknown,
        ));

        await_backend_revoked_for_future_admission(context, target, process_id)?;
        execute_follow_up_query(context)?;
        context.action("completed a new query after finite FE retirement without replacing the unreachable endpoint");

        resume_native(&proxy);
        await_resource_convergence(context, &baseline_resources)
    }
}

/// A BE can accept already-issued work after the FE has become terminal. The
/// FE must not renew that work: the last valid Worker lease remains the final
/// authority and eventually releases the real local resources.
struct LateWorkLeaseExpiry;

/// The B0 controller deliberately runs the pre-4A-1 binary with the legacy
/// stage settings. It is the same client, queries, fixture and BE topology as
/// the candidate controller; only the documented capacity policy differs.
struct Uea4a1BaselinePerformance;

/// The candidate controller records both a normal one-client query stream and
/// a four-client saturated stream behind the warehouse concurrency limit.
struct Uea4a1CandidatePerformance;

impl Scenario for LateWorkLeaseExpiry {
    fn name(&self) -> &'static str {
        "query-concurrency/late-work-lease-expiry"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(fault_launch_config(1, REMOTE_CLEANUP_TIMEOUT_MS))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline_resources = query_resources(context)?;
        let baseline_governance = frontend_state(context)?.workload.governance;
        let creates = task_create_counts(context)?;
        let mut stream = start_timed_hold(context)?;
        let target = await_fresh_task_create(context, &creates)?;
        let process_id = context.process_ids().backends[target];
        let proxy = native_proxy(context, target)?;
        await_proxy_connection(context, &proxy, target)?;
        pause_native(&proxy);
        await_resource_activity(context, &baseline_resources)?;
        assert_timeout_terminal(stream.read_timeout_query_error()?, "late-work query")?;
        await_frontend_state(
            context,
            "await FE retirement before lease expiry",
            |state| {
                let governance = &state.workload.governance;
                governance.admitted_queries == 0
                    && governance.obligations_tracking_ended_remote_unknown
                        > baseline_governance.obligations_tracking_ended_remote_unknown
            },
        )?;
        ensure!(
            query_resources(context)? != baseline_resources,
            "BE resources converged before the scenario observed the post-terminal lease window"
        );
        context.action(
            "kept both Native directions paused after FE terminal so no further lease renewal can be issued",
        );

        await_resource_convergence(context, &baseline_resources)?;
        ensure!(
            context.process_ids().backends[target] == process_id,
            "BE[{target}] exited or was replaced instead of ending orphaned work through its lease"
        );
        context.action(
            "observed BE resource convergence while its FE Native endpoint remained paused, proving the final valid lease bounded orphaned work",
        );

        resume_native(&proxy);
        let state = frontend_state(context)?;
        ensure!(
            state.workload.active.statement == 0 && state.workload.governance.admitted_queries == 0,
            "late Native traffic revived a retired query: {}",
            state.diagnostic()
        );
        execute_follow_up_query(context)?;
        context.action("restored delayed Native traffic and completed a fresh query without reviving the retired attempt");
        Ok(())
    }
}

impl Scenario for QueryConcurrency {
    fn name(&self) -> &'static str {
        "query-concurrency/16-64-256-governance"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(format!(
                    "[runtime.frontend_workload]\n\
                     concurrency_limit = {CONCURRENCY_LIMIT}\n\
                     waiting_limit = {WAITING_LIMIT}\n\
                     control_inflight_limit = {CONCURRENCY_LIMIT}\n\
                     control_ready_limit = {CONTROL_READY_LIMIT}\n\
                     logical_context_admission_issue_capacity = {CONCURRENCY_LIMIT}\n\
                     logical_context_establish_capacity = {CONCURRENCY_LIMIT}\n"
                )),
                // One cooperative driver per BE keeps the production 1FE+3BE
                // boundary while ensuring this is a queue/credit test, not a
                // host CPU saturation benchmark.
                be: Some(
                    "[runtime]\n\
                     pipeline_scan_thread_pool_thread_num = 1\n\
                     pipeline_exec_thread_pool_thread_num = 1\n\
                     exchange_io_threads = 1\n"
                        .to_string(),
                ),
                ..CrossProcessConfigOverlay::default()
            },
            ..ScenarioLaunchConfig::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "{} requires native 1FE+3BE",
            context.name()
        );
        let timeout = bounded_io_timeout(context, "connect concurrency control client")?;
        let mut control =
            mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
        control.query_drop("SELECT 1")?;
        let monitor = ProcessResourceMonitor::start_with_identities(
            context.process_resource_identities()?,
            context.name(),
            SAMPLE_INTERVAL,
        )?;
        let execution = (|| {
            let mut windows = Vec::with_capacity(TIERS.len());
            for tier in TIERS {
                windows.push(run_tier(context, &monitor, tier)?);
                control.query_drop("SELECT 1").with_context(|| {
                    format!("prove control query remains live after tier {tier}")
                })?;
            }
            Ok::<_, anyhow::Error>(windows)
        })();
        let resource_path = context.scenario_root().join("process-resources.json");
        let resources = monitor.finish(&resource_path)?;
        let windows = execution?;
        assert_thread_growth(&resources, &windows)?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Uea4a1PerformanceProfile {
    Baseline,
    Candidate,
}

impl Uea4a1PerformanceProfile {
    const fn name(self) -> &'static str {
        match self {
            Self::Baseline => "b0",
            Self::Candidate => "candidate",
        }
    }
}

impl Scenario for Uea4a1BaselinePerformance {
    fn name(&self) -> &'static str {
        "query-concurrency/uea4a1-b0-performance"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(uea4a1_performance_launch_config(
            Uea4a1PerformanceProfile::Baseline,
        ))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_uea4a1_performance(context, Uea4a1PerformanceProfile::Baseline)
    }
}

impl Scenario for Uea4a1CandidatePerformance {
    fn name(&self) -> &'static str {
        "query-concurrency/uea4a1-candidate-performance"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(uea4a1_performance_launch_config(
            Uea4a1PerformanceProfile::Candidate,
        ))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_uea4a1_performance(context, Uea4a1PerformanceProfile::Candidate)
    }
}

fn uea4a1_performance_launch_config(profile: Uea4a1PerformanceProfile) -> ScenarioLaunchConfig {
    let fe = match profile {
        // This is the recorded B0 policy: it has no whole-query concurrency
        // gate. The legacy stage limits remain visible so a B0 result cannot
        // be misread as a run under the candidate policy.
        Uea4a1PerformanceProfile::Baseline => "[runtime.frontend_workload]\n\
             root_limit = 256\n\
             business_limit = 256\n\
             preparation_limit = 16\n\
             execution_limit = 64\n\
             waiting_limit = 1024\n\
             logical_start_capacity = 256\n\
             logical_context_admission_issue_capacity = 1024\n\
             logical_context_establish_capacity = 1024\n"
            .to_owned(),
        Uea4a1PerformanceProfile::Candidate => format!(
            "[runtime.frontend_workload]\n\
             concurrency_limit = {CONCURRENCY_LIMIT}\n\
             waiting_limit = {WAITING_LIMIT}\n\
             capacity_wait_timeout_ms = 30000\n\
             logical_context_admission_issue_capacity = 1024\n\
             logical_context_establish_capacity = 1024\n\
             logical_remote_cleanup_timeout_ms = 5000\n\
             planning_idle_keepalive_ms = 60000\n"
        ),
    };
    ScenarioLaunchConfig {
        config_overlay: CrossProcessConfigOverlay {
            fe: Some(fe),
            // The benchmark deliberately holds at most one cooperative driver
            // per BE. Its saturation is a warehouse-query admission test, not
            // a host CPU benchmark.
            be: Some(
                "[runtime]\n\
                 pipeline_scan_thread_pool_thread_num = 1\n\
                 pipeline_exec_thread_pool_thread_num = 1\n\
                 exchange_io_threads = 1\n"
                    .to_owned(),
            ),
            ..CrossProcessConfigOverlay::default()
        },
        ..ScenarioLaunchConfig::default()
    }
}

#[derive(Serialize)]
struct Uea4a1PerformanceReport {
    schema_version: u32,
    profile: &'static str,
    fixture: &'static str,
    normal_query: &'static str,
    saturated_query: &'static str,
    window_duration_ms: u64,
    repetitions: usize,
    normal_clients: usize,
    saturated_clients: usize,
    warehouse_concurrency_limit: usize,
    windows: Vec<Uea4a1PerformanceWindow>,
}

#[derive(Serialize)]
struct Uea4a1PerformanceWindow {
    workload: &'static str,
    repetition: usize,
    clients: usize,
    client_think_time_ms: u64,
    started_unix_millis: u128,
    ended_unix_millis: u128,
    completed: usize,
    throughput_per_second: f64,
    p50_micros: u128,
    p95_micros: u128,
    p99_micros: u128,
    control_p99_micros: u128,
    peak_active_statements: usize,
    peak_admitted_queries: usize,
    peak_waiting_records: usize,
    peak_obligations: usize,
}

struct Uea4a1LatencySample {
    total_micros: u128,
}

fn run_uea4a1_performance(
    context: &mut ScenarioContext,
    profile: Uea4a1PerformanceProfile,
) -> Result<()> {
    require_three_backends(context)?;
    ensure!(
        context.launch_profile() == novarocks_cluster_harness::LaunchProfile::Performance,
        "{} requires --launch-profile performance",
        context.name()
    );
    let monitor = ProcessResourceMonitor::start_with_identities(
        context.process_resource_identities()?,
        context.name(),
        PERFORMANCE_SAMPLE_INTERVAL,
    )?;
    let execution = (|| {
        let mut windows = Vec::with_capacity(PERFORMANCE_REPETITIONS * 2);
        for repetition in 0..PERFORMANCE_REPETITIONS {
            windows.push(run_uea4a1_performance_window(
                context,
                "normal",
                repetition,
                1,
                PERFORMANCE_NORMAL_QUERY,
                PERFORMANCE_NORMAL_THINK_TIME,
            )?);
        }
        for repetition in 0..PERFORMANCE_REPETITIONS {
            let window = run_uea4a1_performance_window(
                context,
                "saturated",
                repetition,
                PERFORMANCE_SATURATED_CLIENTS,
                PERFORMANCE_SATURATED_QUERY,
                Duration::ZERO,
            )?;
            if matches!(profile, Uea4a1PerformanceProfile::Candidate) {
                ensure!(
                    window.peak_admitted_queries <= CONCURRENCY_LIMIT,
                    "candidate exceeded warehouse concurrency limit: {} > {CONCURRENCY_LIMIT}",
                    window.peak_admitted_queries
                );
                ensure!(
                    window.peak_waiting_records > 0,
                    "candidate saturated window did not observe the warehouse admission queue"
                );
            }
            windows.push(window);
        }
        if matches!(profile, Uea4a1PerformanceProfile::Candidate) {
            let normal_control_p99 = windows
                .iter()
                .filter(|window| window.workload == "normal")
                .map(|window| window.control_p99_micros)
                .max()
                .context("candidate performance did not record normal control samples")?;
            for window in windows
                .iter()
                .filter(|window| window.workload == "saturated")
            {
                ensure!(
                    window.control_p99_micros <= 2_000_000,
                    "candidate saturated control p99 exceeded two seconds: {}us",
                    window.control_p99_micros
                );
                ensure!(
                    window.control_p99_micros <= normal_control_p99.saturating_mul(3),
                    "candidate saturated control p99 exceeded three times the no-fault p99: {}us > {}us",
                    window.control_p99_micros,
                    normal_control_p99.saturating_mul(3)
                );
            }
        }
        Ok::<_, anyhow::Error>(windows)
    })();
    let resource_path = context.scenario_root().join("process-resources.json");
    let resources = monitor.finish(&resource_path)?;
    let windows = execution?;
    ensure!(
        !resources.samples().is_empty(),
        "{} collected no FE/BE thread samples",
        context.name()
    );
    let report = Uea4a1PerformanceReport {
        schema_version: 1,
        profile: profile.name(),
        fixture: "TABLE(generate_series(1, 1000)) and TABLE(generate_series(1, 1))",
        normal_query: PERFORMANCE_NORMAL_QUERY,
        saturated_query: PERFORMANCE_SATURATED_QUERY,
        window_duration_ms: PERFORMANCE_WINDOW.as_millis() as u64,
        repetitions: PERFORMANCE_REPETITIONS,
        normal_clients: 1,
        saturated_clients: PERFORMANCE_SATURATED_CLIENTS,
        warehouse_concurrency_limit: CONCURRENCY_LIMIT,
        windows,
    };
    let report_path = context.scenario_root().join("uea4a1-performance.json");
    fs::write(&report_path, serde_json::to_vec_pretty(&report)?)
        .with_context(|| format!("write 4A-1 performance report {}", report_path.display()))?;
    context.action(format!(
        "completed {} frozen 4A-1 performance windows; raw report={}",
        report.windows.len(),
        report_path.display()
    ));
    Ok(())
}

fn run_uea4a1_performance_window(
    context: &mut ScenarioContext,
    workload: &'static str,
    repetition: usize,
    clients: usize,
    query: &'static str,
    client_think_time: Duration,
) -> Result<Uea4a1PerformanceWindow> {
    let timeout = bounded_io_timeout(context, "connect 4A-1 performance clients")?;
    let mut connections = Vec::with_capacity(clients);
    for _ in 0..clients {
        let mut connection =
            mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
        execute_uea4a1_performance_query(&mut connection, query)
            .with_context(|| format!("warm 4A-1 {workload} performance query"))?;
        connections.push(connection);
    }
    let start = Arc::new(StdBarrier::new(clients + 1));
    let deadline = Instant::now() + PERFORMANCE_WINDOW;
    let started_unix_millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let mut workers = Vec::with_capacity(clients);
    for mut connection in connections {
        let start = Arc::clone(&start);
        workers.push(thread::spawn(
            move || -> Result<Vec<Uea4a1LatencySample>> {
                start.wait();
                let mut samples = Vec::new();
                while Instant::now() < deadline {
                    let started = Instant::now();
                    execute_uea4a1_performance_query(&mut connection, query)
                        .with_context(|| format!("run 4A-1 {workload} performance query"))?;
                    samples.push(Uea4a1LatencySample {
                        total_micros: started.elapsed().as_micros(),
                    });
                    thread::sleep(client_think_time);
                }
                Ok(samples)
            },
        ));
    }
    start.wait();
    let mut peak_active_statements = 0;
    let mut peak_admitted_queries = 0;
    let mut peak_waiting_records = 0;
    let mut peak_obligations = 0;
    let mut control_samples = Vec::new();
    while Instant::now() < deadline {
        let control_started = Instant::now();
        let state = frontend_state(context)?;
        control_samples.push(control_started.elapsed().as_micros());
        peak_active_statements = peak_active_statements.max(state.workload.active.statement);
        peak_admitted_queries =
            peak_admitted_queries.max(state.workload.governance.admitted_queries);
        peak_waiting_records = peak_waiting_records.max(state.workload.governance.waiting_records);
        peak_obligations = peak_obligations.max(state.workload.governance.obligations);
        thread::sleep(PERFORMANCE_SAMPLE_INTERVAL);
    }
    let mut samples = Vec::new();
    for worker in workers {
        samples.extend(
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("4A-1 performance client panicked"))??,
        );
    }
    await_convergence(context)?;
    ensure!(
        !samples.is_empty(),
        "4A-1 {workload} performance window {repetition} completed no queries"
    );
    let ended_unix_millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let elapsed_seconds = PERFORMANCE_WINDOW.as_secs_f64();
    Ok(Uea4a1PerformanceWindow {
        workload,
        repetition,
        clients,
        client_think_time_ms: client_think_time.as_millis() as u64,
        started_unix_millis,
        ended_unix_millis,
        completed: samples.len(),
        throughput_per_second: samples.len() as f64 / elapsed_seconds,
        p50_micros: percentile_micros(&mut samples, 50),
        p95_micros: percentile_micros(&mut samples, 95),
        p99_micros: percentile_micros(&mut samples, 99),
        control_p99_micros: percentile_values(&mut control_samples, 99),
        peak_active_statements,
        peak_admitted_queries,
        peak_waiting_records,
        peak_obligations,
    })
}

fn execute_uea4a1_performance_query(connection: &mut mysql::Conn, query: &str) -> Result<()> {
    let mut result = connection.query_iter(query)?;
    let mut rows = 0;
    for row in result.by_ref() {
        row?;
        rows += 1;
    }
    ensure!(rows > 0, "4A-1 performance query returned no rows");
    Ok(())
}

fn percentile_micros(samples: &mut [Uea4a1LatencySample], percentile: usize) -> u128 {
    let mut values = samples
        .iter()
        .map(|sample| sample.total_micros)
        .collect::<Vec<_>>();
    percentile_values(&mut values, percentile)
}

fn percentile_values(values: &mut [u128], percentile: usize) -> u128 {
    assert!(!values.is_empty(), "percentile requires one sample");
    values.sort_unstable();
    let index = ((values.len() - 1) * percentile) / 100;
    values[index]
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FrontendState {
    workload: FrontendWorkload,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FrontendWorkload {
    active: ActiveWorkloads,
    governance: Governance,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ActiveWorkloads {
    statement: usize,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Governance {
    root_responsibilities: usize,
    admitted_queries: usize,
    preparation: usize,
    execution: usize,
    old_attempts: usize,
    unknown_creates: usize,
    obligations: usize,
    obligations_settled_with_evidence: usize,
    obligations_tracking_ended_remote_unknown: usize,
    waiting_records: usize,
    peak_waiting_records: usize,
    resource_limit_bytes: u64,
    held_bytes: u64,
    peak_held_bytes: u64,
    result_credit_held_bytes: u64,
    control_ready: usize,
    control_inflight: usize,
}

impl FrontendState {
    fn diagnostic(&self) -> String {
        let workload = &self.workload;
        let governance = &workload.governance;
        format!(
            "active_statement={} roots={} admitted={} preparation={} execution={} obligations={} old_attempts={} unknown_creates={} evidence_endings={} remote_unknown_endings={} waiting={} held_bytes={} control_ready={} control_inflight={}",
            workload.active.statement,
            governance.root_responsibilities,
            governance.admitted_queries,
            governance.preparation,
            governance.execution,
            governance.obligations,
            governance.old_attempts,
            governance.unknown_creates,
            governance.obligations_settled_with_evidence,
            governance.obligations_tracking_ended_remote_unknown,
            governance.waiting_records,
            governance.held_bytes,
            governance.control_ready,
            governance.control_inflight,
        )
    }
}

struct TierWindow {
    tier: usize,
    start_millis: u128,
    end_millis: u128,
}

fn run_tier(
    context: &mut ScenarioContext,
    monitor: &ProcessResourceMonitor,
    tier: usize,
) -> Result<TierWindow> {
    let timeout = bounded_io_timeout(context, "connect held query clients")?;
    let runtime = bounded_client_runtime()?;
    let readers = runtime.block_on(start_held_query_readers(
        context.mysql_user().to_owned(),
        context.mysql_port(),
        timeout,
        tier,
    ))?;
    let start_millis = monitor.elapsed_millis();
    let observed = await_governance(context, tier, &readers)?;
    assert_bounds(&observed, tier)?;
    context.action(format!(
        "tier {tier} reached exact root admission with execution={} waiting={} peak_waiting={} peak_held_bytes={}",
        observed.execution, observed.waiting_records, observed.peak_waiting_records, observed.peak_held_bytes
    ));
    let outcomes = runtime.block_on(read_timeout_outcomes(readers))?;
    let mut terminal_failures = Vec::new();
    for (index, (_connection, outcome)) in outcomes.iter().enumerate() {
        match outcome {
            Ok(error) if is_deadline_terminal(error) => {}
            Ok(error) => terminal_failures.push(format!(
                "client {index} returned a non-deadline terminal response: {error}"
            )),
            Err(error) => terminal_failures.push(format!(
                "client {index} failed to read its deadline terminal: {error}"
            )),
        }
    }
    if !terminal_failures.is_empty() {
        // Retain every client socket until this diagnostic read. Dropping the
        // sockets first would turn an absent deadline terminal into a client
        // disconnect and conceal the FE owner state that must be investigated.
        let state = frontend_state(context)
            .map(|state| state.diagnostic())
            .unwrap_or_else(|error| format!("unavailable: {error:#}"));
        bail!(
            "tier {tier} did not deliver deadline terminals: {}; FE workload state: {state}",
            terminal_failures.join("; ")
        );
    }
    context.action(format!(
        "tier {tier} deadline terminals received: {}",
        outcomes.len()
    ));
    // The raw readers run continuously while these roots are live. Once all
    // of them have observed the expected deadline terminal, governance still
    // owns the separate responsibility/queue/byte convergence assertion.
    await_convergence(context)?;
    Ok(TierWindow {
        tier,
        start_millis,
        end_millis: monitor.elapsed_millis(),
    })
}

fn bounded_client_runtime() -> Result<TokioRuntime> {
    TokioRuntimeBuilder::new_multi_thread()
        .worker_threads(CLIENT_RUNTIME_THREADS)
        .enable_io()
        .enable_time()
        .build()
        .context("build bounded async concurrency client runtime")
}

async fn start_held_query_readers(
    user: String,
    port: u16,
    timeout: Duration,
    tier: usize,
) -> Result<Vec<JoinHandle<(AsyncMysqlStream, Result<String>)>>> {
    let mut connects = JoinSet::new();
    for _ in 0..tier {
        let user = user.clone();
        connects.spawn(async move {
            let mut connection = AsyncMysqlStream::connect(&user, port, timeout).await?;
            connection
                .send_query(&format!("SET query_timeout = {QUERY_TIMEOUT_SECONDS}"))
                .await?;
            connection.expect_ok_packet("SET query_timeout").await?;
            Ok::<_, anyhow::Error>(connection)
        });
    }
    let mut connections = Vec::with_capacity(tier);
    while let Some(result) = connects.join_next().await {
        connections.push(result.context("concurrent held-query client task panicked")??);
    }
    ensure!(
        connections.len() == tier,
        "expected {tier} held-query clients, connected {}",
        connections.len()
    );

    let gate = Arc::new(Barrier::new(tier + 1));
    let reads = connections
        .into_iter()
        .map(|mut connection| {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                gate.wait().await;
                let outcome = async {
                    connection.send_query(HELD_QUERY).await?;
                    connection.read_timeout_query_error().await
                }
                .await;
                (connection, outcome)
            })
        })
        .collect();
    // Keep the original actor's causality: every client starts receiving its
    // statement terminal immediately after it sends the query. Waiting for
    // every write to finish before creating readers can strand roots behind
    // the FE control queue even though their deadline has elapsed.
    gate.wait().await;
    Ok(reads)
}

async fn read_timeout_outcomes(
    reads: Vec<JoinHandle<(AsyncMysqlStream, Result<String>)>>,
) -> Result<Vec<(AsyncMysqlStream, Result<String>)>> {
    let mut outcomes = Vec::with_capacity(reads.len());
    for read in reads {
        outcomes.push(
            read.await
                .context("concurrent held-query read task panicked")?,
        );
    }
    Ok(outcomes)
}

fn frontend_state(context: &mut ScenarioContext) -> Result<FrontendState> {
    let timeout = bounded_io_timeout(context, "read Frontend workload state")?;
    let response = context
        .handle()
        .frontend_management_get("/v1/frontend/state", timeout)?;
    ensure!(
        response.status == 200,
        "Frontend workload state returned HTTP {}",
        response.status
    );
    Ok(serde_json::from_str(&response.body)?)
}

fn await_governance(
    context: &mut ScenarioContext,
    tier: usize,
    readers: &[JoinHandle<(AsyncMysqlStream, Result<String>)>],
) -> Result<Governance> {
    let mut peak_active = 0;
    let mut peak_roots = 0;
    loop {
        let state = frontend_state(context).with_context(|| {
            format!(
                "observe exact {tier}-root admission (peak_active={peak_active}, peak_roots={peak_roots}, finished_readers={})",
                readers.iter().filter(|reader| reader.is_finished()).count()
            )
        })?;
        peak_active = peak_active.max(state.workload.active.statement);
        peak_roots = peak_roots.max(state.workload.governance.root_responsibilities);
        if state.workload.active.statement == tier
            && state.workload.governance.root_responsibilities == tier
        {
            return Ok(state.workload.governance);
        }
        let finished_readers = readers.iter().filter(|reader| reader.is_finished()).count();
        ensure!(
            finished_readers == 0,
            "tier {tier} reached a client terminal before exact root admission: finished_readers={finished_readers}, peak_active={peak_active}, peak_roots={peak_roots}, FE workload state: {}",
            state.diagnostic()
        );
        thread::sleep(
            context
                .remaining("observe exact governed query roots")?
                .min(Duration::from_millis(25)),
        );
    }
}

fn assert_bounds(snapshot: &Governance, tier: usize) -> Result<()> {
    ensure!(
        snapshot.admitted_queries <= CONCURRENCY_LIMIT,
        "tier {tier} exceeded warehouse concurrency limit: {}",
        snapshot.admitted_queries
    );
    ensure!(
        snapshot.waiting_records <= WAITING_LIMIT && snapshot.peak_waiting_records <= WAITING_LIMIT,
        "tier {tier} exceeded waiting-record bound"
    );
    ensure!(
        snapshot.peak_waiting_records > 0,
        "tier {tier} created no governed wait"
    );
    ensure!(
        snapshot.held_bytes <= snapshot.resource_limit_bytes
            && snapshot.peak_held_bytes <= snapshot.resource_limit_bytes
            && snapshot.result_credit_held_bytes <= snapshot.held_bytes,
        "tier {tier} violated local resource authority bound"
    );
    Ok(())
}

fn await_convergence(context: &mut ScenarioContext) -> Result<()> {
    loop {
        let state = frontend_state(context)?;
        if state.workload.active.statement == 0
            && state.workload.governance.root_responsibilities == 0
            && state.workload.governance.waiting_records == 0
            && state.workload.governance.held_bytes == 0
        {
            return Ok(());
        }
        let remaining = match context.remaining("await workload convergence after cancellation") {
            Ok(remaining) => remaining,
            Err(error) => {
                context.action(format!(
                    "cancellation convergence timed out with Frontend workload state: {:?}",
                    state.workload
                ));
                return Err(error).context("await workload convergence after cancellation");
            }
        };
        thread::sleep(remaining.min(Duration::from_millis(25)));
    }
}

fn assert_thread_growth(resources: &ProcessResourceSampler, windows: &[TierWindow]) -> Result<()> {
    let first = windows.first().context("missing 16-query sample window")?;
    let mut roles = resources
        .samples()
        .iter()
        .map(|sample| sample.role.as_str())
        .collect::<Vec<_>>();
    roles.sort_unstable();
    roles.dedup();
    ensure!(
        !roles.is_empty(),
        "process resource monitor recorded no process identities"
    );
    for role in roles {
        let baseline = peak_threads(resources, role, first)?;
        for window in windows.iter().skip(1) {
            ensure!(
                peak_threads(resources, role, window)? <= baseline + FIXED_RUNTIME_THREAD_ALLOWANCE,
                "{role} thread peak grew beyond bound at tier {}",
                window.tier
            );
        }
    }
    Ok(())
}

fn peak_threads(
    resources: &ProcessResourceSampler,
    role: &str,
    window: &TierWindow,
) -> Result<u64> {
    resources
        .samples()
        .iter()
        .filter(|sample| {
            sample.role == role
                && sample.elapsed_millis >= window.start_millis
                && sample.elapsed_millis <= window.end_millis
        })
        .filter_map(|sample| sample.threads)
        .max()
        .context("missing process thread sample")
}

fn bounded_io_timeout(context: &ScenarioContext, operation: &str) -> Result<Duration> {
    Ok(context.remaining(operation)?.min(IO_TIMEOUT_CAP))
}

fn fault_launch_config(concurrency_limit: usize, cleanup_timeout_ms: u64) -> ScenarioLaunchConfig {
    ScenarioLaunchConfig {
        config_overlay: CrossProcessConfigOverlay {
            fe: Some(format!(
                "[runtime.frontend_workload]\n\
                 concurrency_limit = {concurrency_limit}\n\
                 waiting_limit = {WAITING_LIMIT}\n\
                 control_inflight_limit = {concurrency_limit}\n\
                 control_ready_limit = {CONTROL_READY_LIMIT}\n\
                 logical_context_admission_issue_capacity = {concurrency_limit}\n\
                 logical_context_establish_capacity = {concurrency_limit}\n\
                 logical_remote_cleanup_timeout_ms = {cleanup_timeout_ms}\n"
            )),
            be: Some(
                "[runtime]\n\
                 pipeline_scan_thread_pool_thread_num = 1\n\
                 pipeline_exec_thread_pool_thread_num = 1\n\
                 exchange_io_threads = 1\n"
                    .to_string(),
            ),
            ..CrossProcessConfigOverlay::default()
        },
        native_fault_proxies: CrossProcessNativeFaultProxyConfig {
            backend_retained_byte_limits: BTreeMap::from([
                (0, 64 * 1024),
                (1, 64 * 1024),
                (2, 64 * 1024),
            ]),
        },
        ..ScenarioLaunchConfig::default()
    }
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let actual = context.handle().be_count();
    ensure!(
        actual == REQUIRED_BACKENDS,
        "{} requires native 1FE+{REQUIRED_BACKENDS}BE, but runner launched 1FE+{actual}BE",
        context.name()
    );
    context.action("verified native 1FE+3BE topology");
    Ok(())
}

fn query_resources(context: &mut ScenarioContext) -> Result<QueryExecutionResourceSnapshot> {
    context
        .handle()
        .query_execution_resource_snapshot()?
        .context("cross-process harness did not expose the query-resource oracle")
}

fn await_resource_activity(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
) -> Result<()> {
    loop {
        if query_resources(context)? != *baseline {
            return Ok(());
        }
        thread::sleep(
            context
                .remaining("observe live Worker resources")?
                .min(Duration::from_millis(25)),
        );
    }
}

fn await_resource_convergence(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
) -> Result<()> {
    let deadline = context.deadline();
    context
        .handle()
        .await_query_execution_resource_convergence(baseline, deadline)
        .context("await real Worker resource convergence")?;
    context.action("observed real Worker resource convergence");
    Ok(())
}

fn task_create_counts(context: &mut ScenarioContext) -> Result<Vec<usize>> {
    (0..context.handle().be_count())
        .map(|index| {
            context
                .handle()
                .be_log_count(index, "NOVAROCKS_TASK_CREATE_APPLIED")
        })
        .collect()
}

fn await_fresh_task_create(context: &mut ScenarioContext, before: &[usize]) -> Result<usize> {
    loop {
        for (index, count) in before.iter().copied().enumerate() {
            if context
                .handle()
                .be_log_count(index, "NOVAROCKS_TASK_CREATE_APPLIED")?
                > count
            {
                return Ok(index);
            }
        }
        thread::sleep(
            context
                .remaining("observe an admitted Native TaskCreate")?
                .min(Duration::from_millis(25)),
        );
    }
}

fn native_proxy(context: &mut ScenarioContext, target: usize) -> Result<NativeFaultProxyControl> {
    context
        .handle()
        .native_fault_proxy(target)
        .with_context(|| format!("obtain native fault proxy for BE[{target}]"))
}

fn await_proxy_connection(
    context: &ScenarioContext,
    proxy: &NativeFaultProxyControl,
    target: usize,
) -> Result<()> {
    loop {
        if proxy.active_connections() > 0 {
            return Ok(());
        }
        thread::sleep(
            context
                .remaining(&format!(
                    "observe Native connection through BE[{target}] proxy"
                ))?
                .min(Duration::from_millis(25)),
        );
    }
}

fn pause_native(proxy: &NativeFaultProxyControl) {
    proxy.set_mode(ProxyDirection::ClientToUpstream, ProxyMode::Paused);
    proxy.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Paused);
}

fn resume_native(proxy: &NativeFaultProxyControl) {
    proxy.set_mode(ProxyDirection::ClientToUpstream, ProxyMode::Forward);
    proxy.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Forward);
}

fn start_timed_hold(context: &ScenarioContext) -> Result<MysqlStream> {
    let mut stream = MysqlStream::connect(
        context.mysql_user(),
        context.mysql_port(),
        bounded_io_timeout(context, "connect timed hold client")?,
    )?;
    stream.send_query(&format!("SET query_timeout = {TERMINAL_TIMEOUT_SECONDS}"))?;
    stream.expect_ok_packet("SET query_timeout for timed hold")?;
    stream.send_query(HELD_QUERY)?;
    Ok(stream)
}

struct PendingFollowUpQuery {
    thread: thread::JoinHandle<Result<()>>,
    done: mpsc::Receiver<Result<Vec<i64>>>,
}

fn start_follow_up_query(context: &ScenarioContext) -> Result<PendingFollowUpQuery> {
    let (done_tx, done) = mpsc::sync_channel(1);
    let user = context.mysql_user().to_owned();
    let port = context.mysql_port();
    let timeout = bounded_io_timeout(context, "connect q2 waiting client")?;
    let thread = thread::Builder::new()
        .name("query-concurrency-follow-up".to_string())
        .spawn(move || -> Result<()> {
            let mut connection = mysql_actor::connect_for_cancellation(&user, port, timeout)?;
            let rows = connection
                .query("SELECT sleep(1) FROM TABLE(generate_series(1, 1))")
                .context("run q2 after q1 terminal")?;
            done_tx.send(Ok(rows)).context("publish q2 rows")
        })
        .context("start q2 waiting client")?;
    Ok(PendingFollowUpQuery { thread, done })
}

fn execute_follow_up_query(context: &ScenarioContext) -> Result<()> {
    let mut connection = mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        bounded_io_timeout(context, "connect post-retirement query")?,
    )?;
    let rows: Vec<i64> = connection
        .query("SELECT sleep(1) FROM TABLE(generate_series(1, 1))")
        .context("run post-retirement distributed query")?;
    ensure!(
        rows == vec![1],
        "post-retirement query returned unexpected rows: {rows:?}"
    );
    Ok(())
}

fn assert_timeout_terminal(error: String, query: &str) -> Result<()> {
    ensure!(
        is_deadline_terminal(&error),
        "{query} returned an unexpected terminal instead of timeout: {error}"
    );
    Ok(())
}

fn is_deadline_terminal(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    normalized.contains("timed out")
        || normalized.contains("timeout")
        || normalized.contains("deadline")
}

fn await_frontend_state(
    context: &mut ScenarioContext,
    operation: &str,
    mut predicate: impl FnMut(&FrontendState) -> bool,
) -> Result<FrontendState> {
    loop {
        let state = frontend_state(context)?;
        if predicate(&state) {
            return Ok(state);
        }
        thread::sleep(context.remaining(operation)?.min(Duration::from_millis(25)));
    }
}

fn await_backend_revoked_for_future_admission(
    context: &mut ScenarioContext,
    target: usize,
    expected_process_id: u32,
) -> Result<()> {
    let advertised_port = context.handle().native_be_endpoint(target)?.port();
    loop {
        ensure!(
            context.process_ids().backends[target] == expected_process_id,
            "BE[{target}] process identity changed while its endpoint was permanently unreachable"
        );
        let eligible = context
            .handle()
            .frontend_backend_topology()?
            .iter()
            .any(|row| row.grpc_port == advertised_port && row.is_eligible_live());
        if !eligible {
            return Ok(());
        }
        thread::sleep(
            context
                .remaining(&format!(
                    "observe BE[{target}] revoked for future admission"
                ))?
                .min(Duration::from_millis(25)),
        );
    }
}
