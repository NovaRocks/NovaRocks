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
use crate::actors::mysql_stream::AsyncMysqlStream;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::process_resources::{
    ProcessResourceMonitor, ProcessResourceSampler,
};
use novarocks_cluster_harness::{CrossProcessConfigOverlay, ServerHandle};
use serde::Deserialize;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime};
use tokio::sync::Barrier;
use tokio::task::{JoinHandle, JoinSet};

const REQUIRED_BACKENDS: usize = 3;
const TIERS: [usize; 3] = [16, 64, 256];
const ROOT_LIMIT: usize = 257;
const EXECUTION_LIMIT: usize = 3;
const WAITING_LIMIT: usize = 512;
const WAITING_BYTES_LIMIT: u64 = 64 * 1024 * 1024;
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
// The role can start a small bounded worker wave as its network fan-in grows.
// Keep a platform-safe absolute allowance while asserting that the 256-root
// tier does not grow proportionally to client roots.
const FIXED_RUNTIME_THREAD_ALLOWANCE: u64 = 12;
const IO_TIMEOUT_CAP: Duration = Duration::from_secs(10);
const QUERY_TIMEOUT_SECONDS: u64 = 5;
// This scenario must prove 256 logical MySQL roots, not allocate one OS
// thread per root. Sixteen I/O workers keep the 256-socket fan-in making
// progress while preserving a fixed, platform-safe harness bound.
const CLIENT_RUNTIME_THREADS: usize = 16;
// `sleep` evaluates in the BE after the one-row source is distributed. It is
// deliberately a one-row source so each of the three admitted drivers uses
// bounded memory while queued roots demonstrate the actual governance limit.
const HELD_QUERY: &str = "SELECT sleep(10) FROM TABLE(generate_series(1, 1))";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(QueryConcurrency)]
}

struct QueryConcurrency;

impl Scenario for QueryConcurrency {
    fn name(&self) -> &'static str {
        "query-concurrency/16-64-256-governance"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(format!(
                    "[runtime.frontend_workload]\n\
                     root_limit = {ROOT_LIMIT}\n\
                     business_limit = {ROOT_LIMIT}\n\
                     preparation_limit = {EXECUTION_LIMIT}\n\
                     execution_limit = {EXECUTION_LIMIT}\n\
                     waiting_limit = {WAITING_LIMIT}\n\
                     waiting_bytes = {WAITING_BYTES_LIMIT}\n\
                     control_inflight_limit = {EXECUTION_LIMIT}\n\
                     control_ready_limit = {ROOT_LIMIT}\n\
                     logical_start_capacity = {EXECUTION_LIMIT}\n\
                     logical_context_admission_issue_capacity = {EXECUTION_LIMIT}\n\
                     logical_context_establish_capacity = {EXECUTION_LIMIT}\n"
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

#[derive(Debug, Deserialize)]
struct FrontendState {
    workload: FrontendWorkload,
}

#[derive(Debug, Deserialize)]
struct FrontendWorkload {
    active: ActiveWorkloads,
    governance: Governance,
}

#[derive(Debug, Deserialize)]
struct ActiveWorkloads {
    statement: usize,
}

#[derive(Debug, Deserialize)]
struct Governance {
    root_responsibilities: usize,
    preparation: usize,
    execution: usize,
    waiting_records: usize,
    peak_waiting_records: usize,
    waiting_bytes: u64,
    peak_waiting_bytes: u64,
    resource_limit_bytes: u64,
    held_bytes: u64,
    peak_held_bytes: u64,
    result_credit_held_bytes: u64,
    control_ready: usize,
    control_inflight: usize,
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
    let observed = await_governance(context, tier)?;
    assert_bounds(&observed, tier)?;
    context.action(format!(
        "tier {tier} reached exact root admission with execution={} waiting={} peak_waiting={} peak_held_bytes={}",
        observed.execution, observed.waiting_records, observed.peak_waiting_records, observed.peak_held_bytes
    ));
    let outcomes = runtime.block_on(read_timeout_outcomes(readers))?;
    let mut terminal_failures = Vec::new();
    for (index, (_connection, outcome)) in outcomes.iter().enumerate() {
        match outcome {
            Ok(error)
                if error.contains("timed out")
                    || error.contains("timeout")
                    || error.contains("deadline") => {}
            Ok(error) => terminal_failures.push(format!(
                "client {index} returned a non-timeout terminal response: {error}"
            )),
            Err(error) => terminal_failures.push(format!(
                "client {index} failed to read its timeout response: {error}"
            )),
        }
    }
    if !terminal_failures.is_empty() {
        // Retain every client socket until this diagnostic read. Dropping the
        // sockets first would turn an absent deadline terminal into a client
        // disconnect and conceal the FE owner state that must be investigated.
        let state = frontend_state(context)
            .map(|state| format!("{state:?}"))
            .unwrap_or_else(|error| format!("unavailable: {error:#}"));
        bail!(
            "tier {tier} did not deliver deadline terminals: {}; FE workload state: {state}",
            terminal_failures.join("; ")
        );
    }
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

fn await_governance(context: &mut ScenarioContext, tier: usize) -> Result<Governance> {
    loop {
        let state = frontend_state(context)?;
        if state.workload.active.statement == tier
            && state.workload.governance.root_responsibilities == tier
        {
            return Ok(state.workload.governance);
        }
        thread::sleep(
            context
                .remaining("observe exact governed query roots")?
                .min(Duration::from_millis(25)),
        );
    }
}

fn assert_bounds(snapshot: &Governance, tier: usize) -> Result<()> {
    ensure!(
        snapshot.execution <= EXECUTION_LIMIT,
        "tier {tier} exceeded execution limit: {}",
        snapshot.execution
    );
    ensure!(
        snapshot.waiting_records <= WAITING_LIMIT && snapshot.peak_waiting_records <= WAITING_LIMIT,
        "tier {tier} exceeded waiting-record bound"
    );
    ensure!(
        snapshot.waiting_bytes <= WAITING_BYTES_LIMIT
            && snapshot.peak_waiting_bytes <= WAITING_BYTES_LIMIT,
        "tier {tier} exceeded waiting-byte bound"
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
