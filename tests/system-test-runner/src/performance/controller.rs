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

use super::business::{self, BusinessSample, MixedFixtureBinding};
use super::fixture_identity::PreparedWindowIdentity;
use super::fixture_realization::write_fixture_realization;
use super::manifest::{
    ManifestPurpose, MixedWorkload, SlowOutputWorkload, Uea1WorkloadManifest, Window,
};
use super::metrics::{
    MeasurementWindow, PerformanceDiagnosticResponse, PerformanceReportInput,
    PreparationDiagnosticFrame, PreparationEvent, QuerySample, write_report,
};
use super::provenance::{
    RunManifestKind, begin_run_manifest, sha256_file, write_run_completion_marker,
};
use crate::actors::mysql as mysql_actor;
use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::LaunchProfile;
use novarocks_cluster_harness::process_resources::ProcessResourceMonitor;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub enum PerformanceScenario {
    ShortConcurrent,
    Mixed,
    SlowOutput,
}

impl PerformanceScenario {
    pub fn name(self) -> &'static str {
        match self {
            Self::ShortConcurrent => "performance/uea1-short-concurrent",
            Self::Mixed => "performance/uea1-mixed",
            Self::SlowOutput => "performance/uea1-slow-output",
        }
    }
}

pub fn run(
    scenario: PerformanceScenario,
    context: &mut ScenarioContext,
    manifest: &Uea1WorkloadManifest,
    mixed_fixture: Option<&MixedFixtureBinding>,
) -> Result<()> {
    ensure!(
        context.launch_profile() == LaunchProfile::Performance,
        "{} requires --launch-profile performance",
        scenario.name()
    );
    if manifest.purpose == ManifestPurpose::Formal {
        ensure!(
            context.process_ids().backends.len() == 3,
            "formal UEA-1 performance requires native 1FE+3BE"
        );
        ensure!(
            context
                .primary_binary()
                .components()
                .any(|component| component.as_os_str() == "release"),
            "formal UEA-1 performance requires a checkout-local release binary"
        );
    }
    if matches!(scenario, PerformanceScenario::Mixed) {
        ensure!(
            mixed_fixture.is_some(),
            "mixed performance requires a scenario-owned isolated catalog fixture"
        );
    }
    let fixture_spec = serde_json::to_vec(&(scenario.name(), manifest))
        .context("serialize UEA-1 fixture specification")?;
    let workload_manifest_bytes = std::fs::read(
        context
            .uea1_workload_manifest()
            .context("UEA-1 performance workload manifest path is missing")?,
    )
    .context("read frozen UEA-1 workload manifest for the run artifact")?;
    let run_manifest = begin_run_manifest(
        context,
        scenario.name(),
        &manifest.sha256,
        &workload_manifest_bytes,
        &fixture_spec,
        manifest.purpose == ManifestPurpose::Formal,
        RunManifestKind::Performance,
    )?;
    let monitor = ProcessResourceMonitor::start_with_identities(
        context.process_resource_identities()?,
        run_manifest.run_id(),
        Duration::from_millis(100),
    )?;
    let timeline = MonotonicTimeline::new(&monitor);
    let execution = (|| {
        let diagnostic = run_diagnostic_prelude(
            scenario,
            context,
            manifest,
            mixed_fixture,
            run_manifest.run_id(),
            timeline,
        )?;
        let (samples, measurement_windows, timed_fixture_identities) = match scenario {
            PerformanceScenario::ShortConcurrent => {
                let (samples, windows) = run_short(context, manifest, &monitor, timeline)?;
                (samples, windows, Vec::new())
            }
            PerformanceScenario::Mixed => run_mixed(
                context,
                &manifest.mixed,
                mixed_fixture.context("mixed performance fixture is missing")?,
                manifest.purpose == ManifestPurpose::Formal,
                &monitor,
                timeline,
            )?,
            PerformanceScenario::SlowOutput => {
                let (samples, windows) =
                    run_slow_output(context, &manifest.slow_output, &monitor, timeline)?;
                (samples, windows, Vec::new())
            }
        };
        Ok::<_, anyhow::Error>((
            diagnostic,
            samples,
            measurement_windows,
            timed_fixture_identities,
        ))
    })();
    let resource_path = context.scenario_root().join("process-resources.json");
    let resources = monitor.finish(&resource_path);
    let (diagnostic, samples, measurement_windows, timed_fixture_identities, resources) = match (
        execution, resources,
    ) {
        (Ok((diagnostic, samples, windows, identities)), Ok(resources)) => {
            (diagnostic, samples, windows, identities, resources)
        }
        (Err(execution_error), Ok(_)) => {
            return Err(execution_error).with_context(|| {
                format!(
                    "partial process resources were preserved at {}",
                    resource_path.display()
                )
            });
        }
        (Ok(_), Err(resource_error)) => return Err(resource_error),
        (Err(execution_error), Err(resource_error)) => bail!(
            "performance execution failed: {execution_error:#}; process resource collection also failed: {resource_error:#}"
        ),
    };
    let resources_sha256 = sha256_file(&resource_path)?;
    ensure!(
        !resources.samples().is_empty(),
        "{} collected no process resource samples",
        scenario.name()
    );
    let fixture_realization = write_fixture_realization(
        context.scenario_root(),
        scenario.name(),
        mixed_fixture.and_then(MixedFixtureBinding::provider_runtime),
        diagnostic.fixture_identity.as_ref(),
        &timed_fixture_identities,
    )?;
    ensure!(
        measurement_windows
            .iter()
            .all(|window| window.started_elapsed_millis.saturating_mul(1000)
                >= diagnostic.frame.ended_elapsed_micros),
        "{} timed measurement overlapped its diagnostic prelude",
        scenario.name()
    );
    ensure!(
        !samples.is_empty() && samples.iter().any(|sample| sample.rows > 0),
        "{} produced no valid work",
        scenario.name()
    );
    let run_manifest = run_manifest.finish_performance(
        &resources_sha256,
        &fixture_realization.artifact_sha256,
        &fixture_realization.semantics_sha256,
    )?;
    let performance_sha256 = write_report(PerformanceReportInput {
        root: context.scenario_root(),
        run_id: &run_manifest.run_id,
        run_manifest_sha256: &run_manifest.sha256,
        resources_sha256: &resources_sha256,
        effective_launch_config_sha256: &run_manifest.effective_launch_config_sha256,
        effective_launch_config_semantics_sha256: &run_manifest
            .effective_launch_config_semantics_sha256,
        fixture_realization_sha256: run_manifest
            .fixture_realization_sha256
            .as_deref()
            .context("completed performance manifest omitted fixture realization")?,
        fixture_realization_semantics_sha256: run_manifest
            .fixture_realization_semantics_sha256
            .as_deref()
            .context("completed performance manifest omitted fixture semantics")?,
        manifest_sha256: &manifest.sha256,
        scenario: scenario.name(),
        samples: &samples,
        measurement_windows: &measurement_windows,
        preparation_diagnostic: &diagnostic.frame,
        preparation_events: &diagnostic.events,
    })?;
    write_run_completion_marker(
        context.scenario_root(),
        scenario.name(),
        &run_manifest,
        &performance_sha256,
        &resources_sha256,
    )?;
    context.action(format!(
        "{} completed {} valid samples using manifest {}",
        scenario.name(),
        samples.len(),
        manifest.sha256
    ));
    Ok(())
}

#[derive(Clone, Copy)]
struct MonotonicTimeline {
    anchor: Instant,
    anchor_elapsed_micros: u128,
}

impl MonotonicTimeline {
    fn new(monitor: &ProcessResourceMonitor) -> Self {
        Self {
            anchor: Instant::now(),
            anchor_elapsed_micros: monitor.elapsed_millis().saturating_mul(1000),
        }
    }

    fn elapsed_micros(self) -> u128 {
        self.anchor_elapsed_micros + self.anchor.elapsed().as_micros()
    }

    fn elapsed_millis(self) -> u128 {
        self.elapsed_micros() / 1000
    }
}

struct CollectedDiagnosticPrelude {
    frame: PreparationDiagnosticFrame,
    events: Vec<PreparationEvent>,
    fixture_identity: Option<PreparedWindowIdentity>,
}

fn run_diagnostic_prelude(
    scenario: PerformanceScenario,
    context: &ScenarioContext,
    manifest: &Uea1WorkloadManifest,
    mixed_fixture: Option<&MixedFixtureBinding>,
    run_token: &str,
    timeline: MonotonicTimeline,
) -> Result<CollectedDiagnosticPrelude> {
    let secret = context
        .uea1_preparation_diagnostic_secret()
        .context("UEA-1 preparation diagnostic control secret is missing")?;
    post_preparation_diagnostic_control(
        context,
        "/v1/diagnostics/preparation/arm",
        secret,
        run_token,
    )?;
    let started_elapsed_micros = timeline.elapsed_micros();
    let prelude_result = execute_diagnostic_work(scenario, context, manifest, mixed_fixture);
    let drained = post_preparation_diagnostic_control(
        context,
        "/v1/diagnostics/preparation/drain",
        secret,
        run_token,
    );
    let ended_elapsed_micros = timeline.elapsed_micros();
    let fixture_identity = prelude_result?;
    let response = drained?.context("preparation diagnostic drain returned no document")?;
    ensure!(
        response.schema_version == 1 && response.run_token == run_token,
        "preparation diagnostic drain did not return the exact armed run"
    );
    ensure!(
        !response.events.is_empty(),
        "preparation diagnostic prelude produced no events"
    );
    Ok(CollectedDiagnosticPrelude {
        frame: PreparationDiagnosticFrame {
            schema_version: 1,
            run_token: response.run_token,
            started_elapsed_micros,
            ended_elapsed_micros,
        },
        events: response.events,
        fixture_identity,
    })
}

fn execute_diagnostic_work(
    scenario: PerformanceScenario,
    context: &ScenarioContext,
    manifest: &Uea1WorkloadManifest,
    mixed_fixture: Option<&MixedFixtureBinding>,
) -> Result<Option<PreparedWindowIdentity>> {
    let timeout = context.remaining("run UEA-1 preparation diagnostic prelude")?;
    let mut connection = mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
    match scenario {
        PerformanceScenario::ShortConcurrent => {
            for query in &manifest.short.queries {
                connection
                    .query_drop(query)
                    .context("execute short preparation diagnostic query")?;
            }
            Ok(None)
        }
        PerformanceScenario::SlowOutput => {
            connection
                .query_drop(&manifest.slow_output.control_query)
                .context("execute slow-output control diagnostic query")?;
            connection
                .query_drop(&manifest.slow_output.foreground_query)
                .context("execute slow-output foreground diagnostic query")?;
            connection
                .query_drop(format!(
                    "SELECT * FROM ({}) AS uea1_slow_diagnostic LIMIT 1",
                    manifest.slow_output.query
                ))
                .context("execute bounded slow-output preparation diagnostic query")?;
            Ok(None)
        }
        PerformanceScenario::Mixed => {
            let binding = mixed_fixture.context("mixed diagnostic fixture is missing")?;
            let mut diagnostic_workload = manifest.mixed.clone();
            for producer in &mut diagnostic_workload.producers {
                producer.jobs = 1;
                producer.minimum_completions = 1;
            }
            let prepared = business::prepare_window(
                &mut connection,
                binding,
                &diagnostic_workload,
                diagnostic_workload.repetitions,
            )?;
            let fixture_identity = prepared.fixture_identity.clone();
            let result = (|| {
                business::assert_rows(
                    &mut connection,
                    &prepared.foreground,
                    prepared.foreground_rows,
                    business::sequence_sum(prepared.foreground_rows)?,
                )?;
                for jobs in prepared.jobs.values() {
                    let job = jobs
                        .first()
                        .context("diagnostic producer has no prepared job")?;
                    business::execute_job(
                        &mut connection,
                        job,
                        diagnostic_workload.repetitions,
                        Instant::now() + Duration::from_millis(diagnostic_workload.job_timeout_ms),
                        Duration::from_millis(diagnostic_workload.job_timeout_ms),
                        Duration::from_millis(diagnostic_workload.poll_interval_ms),
                    )?;
                }
                Ok::<(), anyhow::Error>(())
            })();
            if result.is_ok() {
                prepared.cleanup(&mut connection)?;
            }
            result?;
            Ok(Some(fixture_identity))
        }
    }
}

fn post_preparation_diagnostic_control(
    context: &ScenarioContext,
    path: &str,
    secret: &str,
    run_token: &str,
) -> Result<Option<PerformanceDiagnosticResponse>> {
    let body = serde_json::to_vec(&serde_json::json!({"run_token": run_token}))?;
    let address = SocketAddr::from(([127, 0, 0, 1], context.fe_http_port()));
    let timeout = context.remaining("call preparation diagnostic management control")?;
    let mut stream = TcpStream::connect_timeout(&address, timeout)
        .context("connect preparation diagnostic management control")?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {secret}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("diagnostic management response has no header terminator")?;
    let headers = std::str::from_utf8(&response[..split])?;
    let status = headers
        .lines()
        .next()
        .context("diagnostic management response has no status line")?;
    ensure!(
        status.contains(" 204 ") || status.contains(" 200 "),
        "diagnostic management control failed: {status}"
    );
    if status.contains(" 204 ") {
        return Ok(None);
    }
    let payload = &response[split + 4..];
    serde_json::from_slice(payload)
        .context("decode preparation diagnostic drain response")
        .map(Some)
}

fn run_short(
    context: &ScenarioContext,
    manifest: &Uea1WorkloadManifest,
    monitor: &ProcessResourceMonitor,
    timeline: MonotonicTimeline,
) -> Result<(Vec<QuerySample>, Vec<MeasurementWindow>)> {
    let timeout = context.remaining("run short-query performance windows")?;
    let mut connection = mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
    for query in &manifest.short.queries {
        let explain = format!("EXPLAIN {query}");
        let text = connection
            .query_iter(explain)
            .context("explain fixed short performance query")?
            .map(|row| row.map(|row| format!("{row:?}")))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .join("\n");
        for expected in &manifest.short.plan_contains {
            ensure!(
                text.contains(expected),
                "fixed short query plan omitted required fragment {expected:?}: {text}"
            );
        }
    }
    drop(connection);

    let mut all = Vec::new();
    let mut measurement_windows = Vec::new();
    let mut window_index = 0;
    for window in &manifest.short.windows {
        for _ in 0..window.repetitions {
            let (samples, measurement_window) = run_query_window(
                QueryWindowInput {
                    user: context.mysql_user(),
                    port: context.mysql_port(),
                    queries: &manifest.short.queries,
                    window,
                    window_index,
                    timeout,
                    workload: "short",
                },
                monitor,
                timeline,
            )?;
            all.extend(samples);
            measurement_windows.push(measurement_window);
            window_index += 1;
        }
    }
    Ok((all, measurement_windows))
}

fn run_mixed(
    context: &ScenarioContext,
    workload: &MixedWorkload,
    binding: &MixedFixtureBinding,
    require_sustained_producers: bool,
    monitor: &ProcessResourceMonitor,
    timeline: MonotonicTimeline,
) -> Result<(
    Vec<QuerySample>,
    Vec<MeasurementWindow>,
    Vec<PreparedWindowIdentity>,
)> {
    let mut all = Vec::new();
    let mut measurement_windows = Vec::new();
    let mut business_samples = Vec::new();
    let mut fixture_identities = Vec::new();
    for window_index in 0..workload.repetitions {
        let timeout = context.remaining("prepare private mixed performance window")?;
        let mut setup = mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
        let prepared = business::prepare_window(&mut setup, binding, workload, window_index)?;
        let fixture_path = context
            .scenario_root()
            .join(format!("mixed-fixture-{window_index}.json"));
        std::fs::write(fixture_path, serde_json::to_vec_pretty(&prepared)?)?;
        let result = run_mixed_window(
            context,
            workload,
            &prepared,
            window_index,
            require_sustained_producers,
            monitor,
            timeline,
        );
        match result {
            Ok((queries, completed, measurement_window)) => {
                all.extend(queries);
                business_samples.extend(completed);
                measurement_windows.push(measurement_window);
                fixture_identities.push(prepared.fixture_identity.clone());
                std::fs::write(
                    context.scenario_root().join("mixed-business.json"),
                    serde_json::to_vec_pretty(&business_samples)?,
                )?;
                prepared.cleanup(&mut setup)?;
            }
            Err(error) => {
                // A failed/unknown job may still own work. The scenario stops
                // the cluster before destroying its private REST/S3 fixture.
                return Err(error)
                    .context("mixed window invalid; retain fixture until cluster shutdown");
            }
        }
    }
    Ok((all, measurement_windows, fixture_identities))
}

enum MixedSample {
    Query(QuerySample),
    Business(BusinessSample),
}

fn run_mixed_window(
    context: &ScenarioContext,
    workload: &MixedWorkload,
    prepared: &business::PreparedWindow,
    window_index: usize,
    require_sustained_producers: bool,
    monitor: &ProcessResourceMonitor,
    timeline: MonotonicTimeline,
) -> Result<(Vec<QuerySample>, Vec<BusinessSample>, MeasurementWindow)> {
    let timeout = Duration::from_millis(workload.job_timeout_ms)
        .min(context.remaining("run mixed performance window")?);
    let configured_concurrency = workload.foreground_clients + workload.producers.len();
    // Connect before releasing the start barrier. A failed connection cannot
    // leave the other clients parked forever on an unreachable barrier count.
    let mut connections = Vec::with_capacity(configured_concurrency);
    for _ in 0..configured_concurrency {
        let mut connection =
            mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
        connection.query_drop(format!(
            "SET query_timeout = {}",
            timeout.as_millis().div_ceil(1000).max(1)
        ))?;
        connections.push(connection);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(configured_concurrency + 1));
    let window_deadline = Arc::new(OnceLock::new());
    let (sender, receiver) = mpsc::sync_channel(1024);
    let mut workers = Vec::new();
    for client in 0..workload.foreground_clients {
        let stop = Arc::clone(&stop);
        let start = Arc::clone(&start);
        let window_deadline = Arc::clone(&window_deadline);
        let sender = sender.clone();
        let mut connection = connections.pop().context("mixed foreground connection")?;
        let table = prepared.foreground.clone();
        let expected_rows = prepared.foreground_rows;
        workers.push(thread::spawn(move || -> Result<()> {
            start.wait();
            let deadline = *window_deadline
                .get()
                .expect("coordinator set window deadline");
            let result = (|| {
                while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
                    let started = Instant::now();
                    let started_elapsed_micros = timeline.elapsed_micros();
                    business::assert_rows(
                        &mut connection,
                        &table,
                        expected_rows,
                        business::sequence_sum(expected_rows)?,
                    )?;
                    let elapsed = started.elapsed().as_micros();
                    let ended_elapsed_micros = timeline.elapsed_micros();
                    sender.send(MixedSample::Query(QuerySample {
                        workload: "mixed-foreground".into(),
                        window_index,
                        configured_concurrency,
                        client,
                        started_elapsed_micros,
                        ended_elapsed_micros,
                        first_row_micros: elapsed,
                        total_micros: elapsed,
                        rows: expected_rows,
                        bytes_read: None,
                        outcome: completion_outcome(Instant::now(), deadline).into(),
                    }))?;
                }
                Ok(())
            })();
            if result.is_err() {
                stop.store(true, Ordering::Release);
            }
            result
        }));
    }
    for producer in workload.producers.iter().cloned() {
        let stop = Arc::clone(&stop);
        let start = Arc::clone(&start);
        let window_deadline = Arc::clone(&window_deadline);
        let sender = sender.clone();
        let mut connection = connections.pop().context("mixed business connection")?;
        let jobs = prepared
            .jobs
            .get(&producer.kind)
            .context("prepared producer jobs are missing")?
            .clone();
        let poll_interval = Duration::from_millis(workload.poll_interval_ms);
        workers.push(thread::spawn(move || -> Result<()> {
            start.wait();
            let deadline = *window_deadline.get().expect("coordinator set window deadline");
            let result = (|| {
                let mut completed = 0_u64;
                let mut consumed = 0;
                for job in jobs {
                    if stop.load(Ordering::Acquire) || Instant::now() >= deadline { break; }
                    let sample = business::execute_job(&mut connection, &job, window_index, deadline, timeout, poll_interval)?;
                    completed += u64::from(sample.completed_in_window);
                    consumed += 1;
                    sender.send(MixedSample::Business(sample))?;
                }
                ensure!(!require_sustained_producers || consumed < producer.jobs || Instant::now() >= deadline,
                    "{} producer exhausted its finite fixture before the formal window ended", producer.kind.name());
                ensure!(completed >= producer.minimum_completions,
                    "{} producer completed {completed} proven jobs inside the window, expected at least {}",
                    producer.kind.name(), producer.minimum_completions);
                Ok(())
            })();
            if result.is_err() { stop.store(true, Ordering::Release); }
            result
        }));
    }
    drop(sender);
    let started_elapsed_millis = timeline.elapsed_millis();
    window_deadline
        .set(Instant::now() + Duration::from_millis(workload.duration_ms))
        .map_err(|_| anyhow::anyhow!("mixed window deadline was initialized twice"))?;
    start.wait();
    let mut queries = Vec::new();
    let mut business = Vec::new();
    for sample in receiver {
        match sample {
            MixedSample::Query(sample) => queries.push(sample),
            MixedSample::Business(sample) => business.push(sample),
        }
    }
    stop.store(true, Ordering::Release);
    let mut failure = None;
    for worker in workers {
        if let Err(error) = worker
            .join()
            .map_err(|_| anyhow::anyhow!("mixed performance worker panicked"))
            .and_then(|result| result)
        {
            failure.get_or_insert(error);
        }
    }
    std::fs::write(
        context
            .scenario_root()
            .join(format!("mixed-business-{window_index}.json")),
        serde_json::to_vec_pretty(&business)?,
    )?;
    std::fs::write(
        context
            .scenario_root()
            .join(format!("mixed-query-{window_index}.json")),
        serde_json::to_vec_pretty(&queries)?,
    )?;
    if let Some(error) = failure {
        return Err(error);
    }
    ensure!(!queries.is_empty(), "mixed window has no foreground work");
    Ok((
        queries,
        business,
        MeasurementWindow {
            workload: "mixed".into(),
            window_index,
            configured_concurrency,
            started_elapsed_millis,
            ended_elapsed_millis: started_elapsed_millis + u128::from(workload.duration_ms),
            drain_ended_elapsed_millis: monitor.elapsed_millis(),
        },
    ))
}

struct QueryWindowInput<'a> {
    user: &'a str,
    port: u16,
    queries: &'a [String],
    window: &'a Window,
    window_index: usize,
    timeout: Duration,
    workload: &'a str,
}

fn run_query_window(
    input: QueryWindowInput<'_>,
    monitor: &ProcessResourceMonitor,
    timeline: MonotonicTimeline,
) -> Result<(Vec<QuerySample>, MeasurementWindow)> {
    let QueryWindowInput {
        user,
        port,
        queries,
        window,
        window_index,
        timeout,
        workload,
    } = input;
    let concurrency = window.concurrency;
    let warmup_ms = window.warmup_ms;
    let duration_ms = window.duration_ms;

    // Complete connection and warmup as a separate phase. A failure here is
    // joined normally and cannot strand measured workers on their start gate.
    let mut warmup_workers = Vec::with_capacity(concurrency);
    for client in 0..concurrency {
        let connection = mysql_actor::connect(user, port, timeout)?;
        let queries = queries.to_vec();
        let workload = workload.to_string();
        warmup_workers.push(thread::spawn(
            move || -> Result<(usize, mysql::Conn, usize)> {
                let mut connection = connection;
                let warmup_deadline = Instant::now() + Duration::from_millis(warmup_ms);
                let mut index = 0;
                while Instant::now() < warmup_deadline {
                    let _ = measure_query(
                        &mut connection,
                        &format!("{workload}-warmup"),
                        window_index,
                        concurrency,
                        client,
                        &queries[index % queries.len()],
                        timeline,
                    )?;
                    index += 1;
                }
                Ok((client, connection, index))
            },
        ));
    }
    let mut warmed = Vec::with_capacity(concurrency);
    for worker in warmup_workers {
        warmed.push(
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("short warmup worker panicked"))??,
        );
    }
    warmed.sort_by_key(|(client, _, _)| *client);

    let start = Arc::new(Barrier::new(concurrency + 1));
    let window_deadline = Arc::new(OnceLock::new());
    let (sender, receiver) = mpsc::channel();
    let mut workers = Vec::with_capacity(concurrency);
    for (client, mut connection, mut index) in warmed {
        let queries = queries.to_vec();
        let start = Arc::clone(&start);
        let window_deadline = Arc::clone(&window_deadline);
        let sender = sender.clone();
        let workload = workload.to_string();
        workers.push(thread::spawn(move || -> Result<()> {
            start.wait();
            let deadline = *window_deadline
                .get()
                .expect("coordinator set short window deadline");
            while Instant::now() < deadline {
                let sample = measure_query(
                    &mut connection,
                    &workload,
                    window_index,
                    concurrency,
                    client,
                    &queries[index % queries.len()],
                    timeline,
                )?;
                let mut sample = sample;
                sample.outcome = completion_outcome(Instant::now(), deadline).to_string();
                sender.send(sample)?;
                index += 1;
            }
            Ok(())
        }));
    }
    drop(sender);
    let started_elapsed_millis = timeline.elapsed_millis();
    window_deadline
        .set(Instant::now() + Duration::from_millis(duration_ms))
        .map_err(|_| anyhow::anyhow!("short window deadline was initialized twice"))?;
    start.wait();
    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("short performance worker panicked"))??;
    }
    Ok((
        receiver.into_iter().collect(),
        MeasurementWindow {
            workload: workload.to_string(),
            window_index,
            configured_concurrency: concurrency,
            started_elapsed_millis,
            ended_elapsed_millis: started_elapsed_millis + u128::from(duration_ms),
            drain_ended_elapsed_millis: monitor.elapsed_millis(),
        },
    ))
}

fn measure_query(
    connection: &mut mysql::Conn,
    workload: &str,
    window_index: usize,
    configured_concurrency: usize,
    client: usize,
    sql: &str,
    timeline: MonotonicTimeline,
) -> Result<QuerySample> {
    let started = Instant::now();
    let started_elapsed_micros = timeline.elapsed_micros();
    let mut result = connection
        .query_iter(sql)
        .with_context(|| format!("run performance query {workload}"))?;
    let mut first_row = None;
    let mut rows = 0_u64;
    for row in result.by_ref() {
        row.with_context(|| format!("read performance query row {workload}"))?;
        first_row.get_or_insert_with(|| started.elapsed());
        rows += 1;
    }
    ensure!(rows > 0, "performance query {workload} returned no rows");
    let ended_elapsed_micros = timeline.elapsed_micros();
    Ok(QuerySample {
        workload: workload.to_string(),
        window_index,
        configured_concurrency,
        client,
        started_elapsed_micros,
        ended_elapsed_micros,
        first_row_micros: first_row.expect("row count is positive").as_micros(),
        total_micros: started.elapsed().as_micros(),
        rows,
        bytes_read: None,
        outcome: "success".to_string(),
    })
}

fn run_slow_output(
    context: &ScenarioContext,
    workload: &SlowOutputWorkload,
    monitor: &ProcessResourceMonitor,
    timeline: MonotonicTimeline,
) -> Result<(Vec<QuerySample>, Vec<MeasurementWindow>)> {
    let timeout = context.remaining("run slow-output performance window")?;
    let mut samples = Vec::new();
    let mut windows = Vec::new();
    for window_index in 0..workload.repetitions {
        let mut slow = connect_raw_mysql(context.mysql_user(), context.mysql_port(), timeout)?;
        send_query(&mut slow, &workload.query)?;
        let mut unread = connect_raw_mysql(context.mysql_user(), context.mysql_port(), timeout)?;
        send_query(&mut unread, &workload.query)?;
        let started = Instant::now();
        let started_elapsed_micros = timeline.elapsed_micros();
        let started_elapsed_millis = started_elapsed_micros / 1000;
        let deadline = started + Duration::from_millis(workload.duration_ms);
        let ended_elapsed_micros = started_elapsed_micros
            .saturating_add(u128::from(workload.duration_ms).saturating_mul(1000));
        let interval = Duration::from_millis(workload.interval_ms);
        let mut probe_workers = Vec::new();
        for (client, name, sql) in [
            (2, "slow-output-control", workload.control_query.clone()),
            (
                3,
                "slow-output-foreground",
                workload.foreground_query.clone(),
            ),
        ] {
            let mut connection =
                mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
            probe_workers.push(thread::spawn(move || -> Result<Vec<QuerySample>> {
                let mut probe_samples = Vec::new();
                while Instant::now() < deadline {
                    let interval_deadline = Instant::now() + interval;
                    let mut sample = measure_query(
                        &mut connection,
                        name,
                        window_index,
                        4,
                        client,
                        &sql,
                        timeline,
                    )?;
                    sample.outcome = completion_outcome_micros(
                        sample.ended_elapsed_micros,
                        ended_elapsed_micros,
                    )
                    .to_string();
                    probe_samples.push(sample);
                    thread::sleep(interval_deadline.saturating_duration_since(Instant::now()));
                }
                Ok(probe_samples)
            }));
        }

        slow.set_read_timeout(Some(interval.min(timeout)))?;
        let mut bytes = 0_u64;
        let mut first_byte = None;
        let mut buffer = vec![0_u8; workload.bytes_per_interval.min(64 * 1024)];
        while Instant::now() < deadline {
            let interval_deadline = Instant::now() + interval;
            let mut remaining = workload.bytes_per_interval;
            while remaining > 0 {
                let read_limit = remaining.min(buffer.len());
                let count = match slow.read(&mut buffer[..read_limit]) {
                    Ok(0) => break,
                    Ok(count) => count,
                    Err(error)
                        if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
                    {
                        break;
                    }
                    Err(error) => return Err(error).context("read throttled MySQL output"),
                };
                remaining -= count;
                bytes += count as u64;
                first_byte.get_or_insert_with(|| started.elapsed());
            }
            thread::sleep(interval_deadline.saturating_duration_since(Instant::now()));
        }
        let mut probe_samples = Vec::new();
        for worker in probe_workers {
            probe_samples.extend(
                worker
                    .join()
                    .map_err(|_| anyhow::anyhow!("slow-output probe worker panicked"))??,
            );
        }
        ensure!(
            has_workload_progress_in_window(
                &probe_samples,
                "slow-output-control",
                started_elapsed_micros,
                ended_elapsed_micros,
            ),
            "control query made no progress while slow readers were active"
        );
        ensure!(
            has_workload_progress_in_window(
                &probe_samples,
                "slow-output-foreground",
                started_elapsed_micros,
                ended_elapsed_micros,
            ),
            "foreground query made no progress while slow readers were active"
        );
        ensure!(bytes > 0, "slow-output client read no bytes");
        drop(unread);
        drop(slow);
        samples.extend(probe_samples);
        samples.push(QuerySample {
            workload: "slow-output-client".to_string(),
            window_index,
            configured_concurrency: 4,
            client: 0,
            started_elapsed_micros,
            ended_elapsed_micros: timeline.elapsed_micros(),
            first_row_micros: first_byte
                .context("slow-output client read no first byte")?
                .as_micros(),
            total_micros: started.elapsed().as_micros(),
            rows: bytes,
            bytes_read: Some(bytes),
            outcome: "window-observation".to_string(),
        });
        windows.push(MeasurementWindow {
            workload: "slow-output".to_string(),
            window_index,
            configured_concurrency: 4,
            started_elapsed_millis,
            ended_elapsed_millis: ended_elapsed_micros / 1000,
            drain_ended_elapsed_millis: monitor.elapsed_millis(),
        });
    }
    Ok((samples, windows))
}

fn completion_outcome(completed: Instant, deadline: Instant) -> &'static str {
    if completed <= deadline {
        "success"
    } else {
        "drained-after-window"
    }
}

fn completion_outcome_micros(completed: u128, deadline: u128) -> &'static str {
    if completed <= deadline {
        "success"
    } else {
        "drained-after-window"
    }
}

fn has_workload_progress_in_window(
    samples: &[QuerySample],
    workload: &str,
    window_started_micros: u128,
    window_ended_micros: u128,
) -> bool {
    samples.iter().any(|sample| {
        sample.workload == workload
            && sample.outcome == "success"
            && sample.started_elapsed_micros >= window_started_micros
            && sample.ended_elapsed_micros <= window_ended_micros
    })
}

fn connect_raw_mysql(user: &str, port: u16, timeout: Duration) -> Result<TcpStream> {
    const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
    const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
    const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
    const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
    const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
    const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = TcpStream::connect_timeout(&address, timeout)
        .with_context(|| format!("connect raw public MySQL client at {address}"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let handshake = read_packet(&mut stream)?;
    ensure!(
        handshake.first().copied() == Some(10),
        "expected MySQL protocol v10 handshake"
    );
    let flags = CLIENT_LONG_PASSWORD
        | CLIENT_LONG_FLAG
        | CLIENT_PROTOCOL_41
        | CLIENT_TRANSACTIONS
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH;
    let mut response = Vec::with_capacity(user.len() + 64);
    response.extend_from_slice(&flags.to_le_bytes());
    response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
    response.push(45);
    response.extend_from_slice(&[0_u8; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.push(0);
    response.extend_from_slice(b"mysql_native_password");
    response.push(0);
    write_packet(&mut stream, 1, &response)?;
    let auth = read_packet(&mut stream)?;
    if auth.first().copied() == Some(0xff) {
        bail!("raw MySQL authentication failed");
    }
    ensure!(
        auth.first().copied() == Some(0),
        "unexpected raw MySQL authentication response"
    );
    Ok(stream)
}

fn send_query(stream: &mut TcpStream, sql: &str) -> Result<()> {
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.push(0x03);
    payload.extend_from_slice(sql.as_bytes());
    write_packet(stream, 0, &payload)
}

fn read_packet(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header)?;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    ensure!(
        length <= 16 * 1024 * 1024,
        "MySQL packet exceeds test bound"
    );
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) -> Result<()> {
    let length = u32::try_from(payload.len()).context("MySQL packet length fits u32")?;
    ensure!(length <= 0x00ff_ffff, "MySQL packet exceeds protocol limit");
    let header = [
        (length & 0xff) as u8,
        ((length >> 8) & 0xff) as u8,
        ((length >> 16) & 0xff) as u8,
        sequence,
    ];
    stream.write_all(&header)?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_after_window_is_not_success() {
        assert_eq!(completion_outcome_micros(100, 100), "success");
        assert_eq!(completion_outcome_micros(101, 100), "drained-after-window");
    }

    #[test]
    fn slow_output_control_must_complete_inside_the_active_window() {
        let sample = QuerySample {
            workload: "slow-output-control".to_string(),
            window_index: 0,
            configured_concurrency: 3,
            client: 2,
            started_elapsed_micros: 101,
            ended_elapsed_micros: 199,
            first_row_micros: 10,
            total_micros: 98,
            rows: 1,
            bytes_read: None,
            outcome: "success".to_string(),
        };
        assert!(has_workload_progress_in_window(
            std::slice::from_ref(&sample),
            "slow-output-control",
            100,
            200,
        ));
        assert!(!has_workload_progress_in_window(
            &[sample],
            "slow-output-control",
            102,
            200,
        ));
    }
}
