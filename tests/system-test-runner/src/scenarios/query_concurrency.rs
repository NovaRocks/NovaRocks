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

use super::native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec};
use crate::actors::sql::{
    self, PendingStatement, SqlSession, StatementFailure, StatementResult, TrinoValue,
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::process_resources::{
    ProcessResourceMonitor, ProcessResourceSampler,
};
use novarocks_cluster_harness::{
    CrossProcessConfigOverlay, CrossProcessNativeFaultProxyConfig, QueryExecutionResourceSnapshot,
    TestClusterOwner,
    native_fault_proxy::{NativeFaultProxyControl, ProxyDirection, ProxyMode},
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};
#[path = "query_concurrency_controls.rs"]
mod concurrency_controls;
#[path = "query_concurrency_trino.rs"]
mod concurrency_trino;
use concurrency_controls::{JoinedThread, created_for};
use concurrency_trino::TierClient;
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime};
use tokio::sync::Barrier;
use tokio::task::JoinHandle;

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
// One bounded async runtime drives every client through its first GET and
// continuation pages while preserving the original sixteen-worker bound.
const CLIENT_RUNTIME_THREADS: usize = 16;
// `sleep` evaluates in the BE after the one-row source is distributed. It is
// deliberately a one-row source so each of the three admitted drivers uses
// bounded memory while queued roots demonstrate the actual governance limit.
const HELD_QUERY: &str = "SELECT sleep(10) FROM TABLE(generate_series(1, 1))";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(QueryConcurrency),
        Box::new(WarehouseAdmission),
        Box::new(TerminalReleasesSlot::default()),
        Box::new(RemoteCleanupRetires::default()),
        Box::new(LateParticipationLeaseExpiry::default()),
    ]
}

struct QueryConcurrency;

/// A compact, functional companion to the 16/64/256 fan-in measurement.
///
/// It uses the same public Trino and native 1FE+3BE path, but stops once the
/// warehouse queue has been observed. This makes the single warehouse permit
/// an independently runnable product acceptance case rather than an inferred
/// property of the larger thread-growth experiment.
struct WarehouseAdmission;

impl Scenario for WarehouseAdmission {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_concurrency",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

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
#[derive(Default)]
struct TerminalReleasesSlot {
    intent: SqlIntentOwner,
}

impl Scenario for TerminalReleasesSlot {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_concurrency",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-concurrency/terminal-releases-slot"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(fault_launch_config(1, REMOTE_CLEANUP_TIMEOUT_MS))
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let statement = StatementSpec {
            sql: HELD_QUERY.into(),
            faults: vec![],
            observations: (0..REQUIRED_BACKENDS)
                .map(|index| {
                    (
                        index,
                        novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                            .as_str(),
                        None,
                    )
                })
                .collect(),
        };
        Ok(Some(self.intent.freeze(
            vec![statement],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline_resources = query_resources(context)?;
        let creates = task_create_counts(context)?;
        let selected = self.intent.take(0)?;
        let (first, observation) = start_timed_hold(context, &selected)?;
        let target = await_fresh_task_create(context, &creates, &selected)?;
        let process_id = context.process_ids().backends[target];
        let proxy = native_proxy(context, target)?;
        await_proxy_connection(context, &proxy, target)?;
        pause_native(&proxy);
        let restore_native = RestoreNative(proxy.clone());
        await_resource_activity(context, &baseline_resources)?;
        context.action(format!(
            "paused an admitted q1 on BE[{target}] while retaining process identity {process_id}"
        ));

        let second = start_follow_up_query(context)?;
        let _restore_native = restore_native;
        await_frontend_state(context, "observe q2 waiting behind q1", |state| {
            state.workload.active.statement == 2
                && state.workload.governance.admitted_queries == 1
                && state.workload.governance.waiting_records > 0
        })?;
        context.action("observed q2 in the sole warehouse admission queue");

        assert_trino_timeout(first.finish(context)?, "q1")?;
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

        assert_follow_up(&second.finish(context)??)?;
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
        drain_timed_hold(context, &observation)?;
        Ok(())
    }
}

/// The Frontend may end its local remote-observation record after the cleanup
/// deadline, but it must not fabricate a Worker stop or require endpoint
/// replacement to make that local retirement possible.
#[derive(Default)]
struct RemoteCleanupRetires {
    intent: SqlIntentOwner,
}

impl Scenario for RemoteCleanupRetires {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_concurrency",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-concurrency/remote-cleanup-retires"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(fault_launch_config(1, REMOTE_CLEANUP_TIMEOUT_MS))
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let statement = StatementSpec {
            sql: HELD_QUERY.into(),
            faults: vec![],
            observations: (0..REQUIRED_BACKENDS)
                .map(|index| {
                    (
                        index,
                        novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                            .as_str(),
                        None,
                    )
                })
                .collect(),
        };
        Ok(Some(self.intent.freeze(
            vec![statement],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline_resources = query_resources(context)?;
        let baseline_governance = frontend_state(context)?.workload.governance;
        let creates = task_create_counts(context)?;
        let selected = self.intent.take(0)?;
        let (query, observation) = start_timed_hold(context, &selected)?;
        let target = await_fresh_task_create(context, &creates, &selected)?;
        let process_id = context.process_ids().backends[target];
        let proxy = native_proxy(context, target)?;
        await_proxy_connection(context, &proxy, target)?;
        pause_native(&proxy);
        let _restore_native = RestoreNative(proxy.clone());
        await_resource_activity(context, &baseline_resources)?;
        assert_trino_timeout(query.finish(context)?, "partitioned query")?;

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
        drain_timed_hold(context, &observation)?;
        await_resource_convergence(context, &baseline_resources)
    }
}

/// A BE can accept already-issued work after the FE has become terminal. The
/// FE must not renew that work: the last valid Worker lease remains the final
/// authority and eventually releases the real local resources.
#[derive(Default)]
struct LateParticipationLeaseExpiry {
    intent: SqlIntentOwner,
}

impl Scenario for LateParticipationLeaseExpiry {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_concurrency",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-concurrency/late-participation-lease-expiry"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(fault_launch_config(1, REMOTE_CLEANUP_TIMEOUT_MS))
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let statement = StatementSpec {
            sql: HELD_QUERY.into(),
            faults: vec![],
            observations: (0..REQUIRED_BACKENDS)
                .map(|index| {
                    (
                        index,
                        novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                            .as_str(),
                        None,
                    )
                })
                .collect(),
        };
        Ok(Some(self.intent.freeze(
            vec![statement],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline_resources = query_resources(context)?;
        let baseline_governance = frontend_state(context)?.workload.governance;
        let creates = task_create_counts(context)?;
        let selected = self.intent.take(0)?;
        let (query, observation) = start_timed_hold(context, &selected)?;
        let target = await_fresh_task_create(context, &creates, &selected)?;
        let process_id = context.process_ids().backends[target];
        let proxy = native_proxy(context, target)?;
        await_proxy_connection(context, &proxy, target)?;
        pause_native(&proxy);
        let _restore_native = RestoreNative(proxy.clone());
        await_resource_activity(context, &baseline_resources)?;
        assert_trino_timeout(query.finish(context)?, "late-work query")?;
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
        drain_timed_hold(context, &observation)?;
        Ok(())
    }
}

impl Scenario for QueryConcurrency {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_concurrency",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

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
                     logical_participation_admission_issue_capacity = {CONCURRENCY_LIMIT}\n\
                     logical_participation_establish_capacity = {CONCURRENCY_LIMIT}\n"
                )),
                // One cooperative driver per BE keeps the production 1FE+3BE
                // boundary while ensuring this is a queue/credit test, not a
                // host CPU saturation benchmark.
                be: Some(
                    "[runtime]\n\
                     pipeline_exec_thread_pool_thread_num = 1\n"
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
        let mut control =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        ensure!(
            sql::single_integer(&control.execute("SELECT 1")?)? == 1,
            "invalid control query"
        );
        let monitor = ProcessResourceMonitor::start_with_identities(
            context.process_resource_identities()?,
            context.name(),
            SAMPLE_INTERVAL,
        )?;
        let execution = (|| {
            let mut windows = Vec::with_capacity(TIERS.len());
            for tier in TIERS {
                windows.push(run_tier(context, &monitor, tier)?);
                ensure!(
                    sql::single_integer(&control.execute("SELECT 1")?)? == 1,
                    "control query did not return 1 after tier {tier}"
                );
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
    background: usize,
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
    result_window_positions: Option<[usize; 4]>,
    control_ready: usize,
    control_inflight: usize,
}

impl FrontendState {
    fn diagnostic(&self) -> String {
        let workload = &self.workload;
        let governance = &workload.governance;
        format!(
            "active_statement={} active_background={} roots={} admitted={} preparation={} execution={} obligations={} old_attempts={} unknown_creates={} evidence_endings={} remote_unknown_endings={} waiting={} result_window_positions={:?} control_ready={} control_inflight={}",
            workload.active.statement,
            workload.active.background,
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
            governance.result_window_positions,
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
        context.sql_user(),
        context.trino_port(),
        timeout,
        context.deadline(),
        context.scenario_root().join(format!("tier-{tier}-http")),
        tier,
    ))?;
    let start_millis = monitor.elapsed_millis();
    let admission = await_governance(context, tier, &readers).and_then(|observed| {
        assert_bounds(&observed, tier)?;
        context.action(format!(
            "tier {tier} reached exact root admission with execution={} waiting={} peak_waiting={} result_window_positions={:?}",
            observed.execution, observed.waiting_records, observed.peak_waiting_records, observed.result_window_positions
        ));
        Ok(())
    });
    let outcomes = runtime.block_on(read_timeout_outcomes(readers));
    let mut terminal_failures = Vec::new();
    for (index, outcome) in outcomes.iter().enumerate() {
        if let Err(error) = outcome {
            terminal_failures.push(format!(
                "client {index} did not observe a typed deadline terminal: {error:#}"
            ));
        }
    }
    if let Err(error) = admission {
        terminal_failures.push(format!("exact admission failed: {error:#}"));
    }

    if !terminal_failures.is_empty() {
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

type TierReader = JoinHandle<(TierClient, Result<()>)>;

async fn start_held_query_readers(
    user: &str,
    port: u16,
    timeout: Duration,
    deadline: Instant,
    root: std::path::PathBuf,
    tier: usize,
) -> Result<Vec<TierReader>> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .connect_timeout(timeout)
        .build()?;
    let mut clients = Vec::with_capacity(tier);
    // Per-client SET receipts are preparation. Complete them before the
    // workload barrier so they do not add concurrent control roots to this
    // fixture's exact query-root tier.
    for index in 0..tier {
        let mut actor = TierClient::new(
            client.clone(),
            port,
            user.to_owned(),
            timeout,
            deadline,
            root.join(format!("{index:03}")),
        )?;
        let result = actor.prepare(QUERY_TIMEOUT_SECONDS).await;
        let result = settle_tier_actor(&mut actor, result).await;
        super::paimon::scan_statement_actor::combine(result, actor.persist())?;
        clients.push(actor);
    }

    let gate = Arc::new(Barrier::new(tier + 1));
    let reads = clients
        .into_iter()
        .map(|mut actor| {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                gate.wait().await;
                let result = actor.run_timeout(HELD_QUERY, QUERY_TIMEOUT_SECONDS).await;
                let result = settle_tier_actor(&mut actor, result).await;
                (actor, result)
            })
        })
        .collect();
    gate.wait().await;
    Ok(reads)
}

async fn settle_tier_actor(actor: &mut TierClient, primary: Result<()>) -> Result<()> {
    if primary.is_ok() {
        return Ok(());
    }
    let cancelled = actor.cancel().await;
    super::paimon::scan_statement_actor::combine(
        primary,
        super::paimon::scan_statement_actor::combine(cancelled, actor.persist()),
    )
}

async fn read_timeout_outcomes(reads: Vec<TierReader>) -> Vec<Result<()>> {
    let mut outcomes = Vec::with_capacity(reads.len());
    for read in reads {
        outcomes.push(match read.await {
            Ok((actor, result)) => {
                super::paimon::scan_statement_actor::combine(result, actor.persist())
            }
            Err(error) => {
                Err(anyhow::Error::new(error).context("concurrent Trino reader did not join"))
            }
        });
    }
    outcomes
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
    readers: &[TierReader],
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
        // Root responsibilities include background scopes. Table maintenance
        // can briefly hold one while probing an empty job queue, so account
        // for those roots explicitly while requiring every query root.
        if state.workload.active.statement == tier
            && state.workload.governance.root_responsibilities
                == tier + state.workload.active.background
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
    let positions = snapshot
        .result_window_positions
        .context("FE must expose actual result-window positions")?;
    // This scenario runs the frozen V1 host profile, not an allocation wallet.
    for (class, (held, limit)) in positions.into_iter().zip([320, 16, 4, 64]).enumerate() {
        ensure!(
            held <= limit,
            "tier {tier} exceeded V1 result class {class}: {held} > {limit}"
        );
    }
    Ok(())
}

fn await_convergence(context: &mut ScenarioContext) -> Result<()> {
    loop {
        let state = frontend_state(context)?;
        if state.workload.active.statement == 0
            && state.workload.governance.root_responsibilities == 0
            && state.workload.governance.waiting_records == 0
            && state.workload.governance.result_window_positions == Some([0; 4])
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
                 logical_participation_admission_issue_capacity = {concurrency_limit}\n\
                 logical_participation_establish_capacity = {concurrency_limit}\n\
                 logical_remote_cleanup_timeout_ms = {cleanup_timeout_ms}\n"
            )),
            be: Some(
                "[runtime]\n\
                 pipeline_exec_thread_pool_thread_num = 1\n"
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
            context.handle().be_log_count(
                index,
                novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED,
            )
        })
        .collect()
}

fn await_fresh_task_create(
    context: &mut ScenarioContext,
    before: &[usize],
    selected: &SelectedStatement,
) -> Result<usize> {
    loop {
        for (index, count) in before.iter().copied().enumerate() {
            let Some(binding) = context.handle().selected_native_observation_binding(
                index,
                novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(),
                &selected.token,
                None,
            )?
            else {
                continue;
            };
            let log = context.handle().be_log_contents(index)?;
            let mut total = 0;
            let mut exact = false;
            for line in log.lines() {
                if line.contains(novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED) {
                    total += 1;
                    if total > count && created_for(line, &binding)? {
                        exact = true;
                    }
                }
            }
            if exact {
                return Ok(index);
            }
        }
        thread::sleep(
            context
                .remaining("observe q1 exact admitted Native TaskCreate")?
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

struct RestoreNative(NativeFaultProxyControl);
impl Drop for RestoreNative {
    fn drop(&mut self) {
        resume_native(&self.0);
    }
}

fn start_timed_hold(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
) -> Result<(
    PendingConcurrencyQuery,
    novarocks_cluster_harness::QueryLifecycleStatementObservation,
)> {
    let deadline = context.deadline();
    let mut session = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
    let completion = session.execute(format!("SET query_timeout = {TERMINAL_TIMEOUT_SECONDS}"))?;
    ensure!(
        completion.update_type.as_deref() == Some("SET SESSION")
            && completion.update_count.is_none()
            && completion.columns.is_empty()
            && completion.rows.is_empty()
            && session
                .session()
                .properties
                .get("query_timeout")
                .is_some_and(|value| value == &TERMINAL_TIMEOUT_SECONDS.to_string(),),
        "timed hold omitted its exact query_timeout SessionEffect"
    );
    let pending = session.submit(HELD_QUERY)?;
    let (pending, observation) = selected.arm_pending(context, pending, HELD_QUERY)?;
    Ok((
        PendingConcurrencyQuery::start(session, pending, deadline)?,
        observation,
    ))
}

fn drain_timed_hold(
    context: &mut ScenarioContext,
    observation: &novarocks_cluster_harness::QueryLifecycleStatementObservation,
) -> Result<()> {
    let deadline = context.deadline();
    context
        .handle()
        .drain_query_lifecycle_statement_binding(observation, deadline)
}

type QueryResponse = std::result::Result<StatementResult, StatementFailure>;

struct PendingConcurrencyQuery {
    thread: Option<JoinedThread<Result<()>>>,
    done: mpsc::Receiver<QueryResponse>,
    cancel: novarocks_test_trino_client::CancelHandle,
    deadline: Instant,
}

impl PendingConcurrencyQuery {
    fn start(
        mut session: SqlSession,
        pending: PendingStatement,
        deadline: Instant,
    ) -> Result<Self> {
        use super::paimon::scan_statement_actor::combine;

        let cancel = pending.cancel_handle();
        let (done_tx, done) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("query-concurrency-trino".to_owned())
            .spawn(move || -> Result<()> {
                match done_tx.send(session.run_pending(pending)) {
                    Ok(()) => Ok(()),
                    Err(mpsc::SendError(Err(error))) => Err(error.into()),
                    Err(mpsc::SendError(Ok(_))) => bail!("Trino result receiver closed"),
                }
            });
        match worker {
            Ok(worker) => Ok(Self {
                thread: Some(JoinedThread::new(worker)),
                done,
                cancel,
                deadline,
            }),
            Err(error) => combine(
                Err(error.into()),
                cancel.delete(deadline).map_err(Into::into),
            ),
        }
    }

    fn finish(mut self, context: &ScenarioContext) -> Result<QueryResponse> {
        use super::paimon::scan_statement_actor::combine;

        let received = self
            .done
            .recv_timeout(context.remaining("await original Trino query")?)
            .context("original Trino query did not return a result");
        let cancelled = if !matches!(&received, Ok(Ok(_) | Err(StatementFailure::Trino(_)))) {
            self.cancel
                .delete(self.deadline)
                .map_err(anyhow::Error::from)
        } else {
            Ok(())
        };
        combine(received, combine(cancelled, self.join()))
    }

    fn join(&mut self) -> Result<()> {
        self.thread
            .take()
            .expect("Trino query thread exists")
            .join()
            .map_err(|_| anyhow::anyhow!("Trino query actor panicked"))?
    }
}

impl Drop for PendingConcurrencyQuery {
    fn drop(&mut self) {
        if self.thread.is_some() {
            if let Err(error) = self.cancel.delete(self.deadline) {
                eprintln!("Cancel original concurrency query failed: {error}");
            }
            if let Err(error) = self.join() {
                eprintln!("Join original concurrency query failed: {error:#}");
            }
        }
    }
}

const FOLLOW_UP_QUERY: &str = "SELECT sleep(1) FROM TABLE(generate_series(1, 1))";

fn start_follow_up_query(context: &ScenarioContext) -> Result<PendingConcurrencyQuery> {
    let deadline = context.deadline();
    let session = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
    let pending = session.submit(FOLLOW_UP_QUERY)?;
    PendingConcurrencyQuery::start(session, pending, deadline)
}

fn execute_follow_up_query(context: &ScenarioContext) -> Result<()> {
    assert_follow_up(&start_follow_up_query(context)?.finish(context)??)
}

fn assert_follow_up(result: &StatementResult) -> Result<()> {
    ensure!(
        result.is_result_set()
            && result.columns.len() == 1
            && result.columns[0].type_signature.raw_type == "boolean"
            && result.columns[0].type_signature.arguments.is_empty()
            && result.rows == [vec![TrinoValue::Boolean(true)]],
        "follow-up query returned unexpected typed result: {result:?}"
    );
    Ok(())
}

fn assert_trino_timeout(response: QueryResponse, query: &str) -> Result<()> {
    match response {
        Err(StatementFailure::Trino(error))
            if error.error_code == 131075
                && error.error_name == "EXCEEDED_TIME_LIMIT"
                && error.error_type == "INSUFFICIENT_RESOURCES" =>
        {
            Ok(())
        }
        Err(error) => Err(anyhow::Error::new(error).context(format!(
            "{query} returned an unexpected terminal instead of timeout"
        ))),
        Ok(result) => bail!("{query} completed instead of timing out: {result:?}"),
    }
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
