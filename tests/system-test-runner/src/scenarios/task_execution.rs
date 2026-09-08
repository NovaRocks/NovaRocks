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

//! Startup latency baseline for the native distributed query path.
//!
//! This exists to make one comparison possible: the same fixtures, the same
//! binary, and the same 1FE+3BE topology, measured before and after the task
//! protocol cutover. It is deliberately a measurement scenario rather than an
//! assertion scenario — it fails only when the cluster or the fixture is
//! wrong, never on a latency number, because a threshold is meaningless until
//! there is a recorded baseline to compare against.
//!
//! Time to first row is measured client-side. `Conn::query_iter` yields rows
//! lazily, so stopping the clock on the first row needs no engine
//! instrumentation, which is what keeps this a test-only artifact.

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext};

const REQUIRED_BACKENDS: usize = 3;

/// Warm-up runs whose numbers are recorded but excluded from the summary.
const WARMUP_RUNS: usize = 3;

/// Measured runs per fixture.
const MEASURED_RUNS: usize = 20;

const IO_TIMEOUT_CAP: Duration = Duration::from_secs(30);

/// A one-exchange-level plan: a union feeding a single gather.
const SHALLOW_QUERY: &str =
    "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3) t ORDER BY v";

/// A closed fixture identity and SQL selected before B0/candidate comparison.
/// Both sides execute this exact SQL; a planner change may change the reported
/// plan hash, but cannot silently cause the harness to select a different query.
const DEEP_FIXTURE_ID: &str = "shuffle-join-chain-v1";
const DEEP_QUERY: &str = "SELECT COUNT(*) AS total FROM \
           (SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 3) a \
           JOIN (SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 3) b ON a.k = b.k \
           JOIN (SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 3) c ON b.k = c.k";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(StartupBaseline)]
}

struct StartupBaseline;

/// One measured execution of one fixture.
#[derive(Clone, Copy)]
struct RunSample {
    first_row: Duration,
    total: Duration,
}

/// What one fixture produced across its measured runs.
struct FixtureReport {
    name: &'static str,
    plan_fragments: usize,
    exchange_levels: usize,
    query_sha256: String,
    plan_sha256: String,
    warmup: Vec<RunSample>,
    measured: Vec<RunSample>,
}

impl Scenario for StartupBaseline {
    fn name(&self) -> &'static str {
        "task-execution/startup-baseline"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let fixture_spec = serde_json::to_vec(&(DEEP_FIXTURE_ID, SHALLOW_QUERY, DEEP_QUERY))?;
        let fixture_sha256 = format!("{:x}", Sha256::digest(&fixture_spec));
        let run_manifest = crate::performance::provenance::begin_run_manifest(
            context,
            self.name(),
            &fixture_sha256,
            &fixture_spec,
            &fixture_spec,
            true,
        )?;
        require_backends(context)?;

        let connect_timeout = bounded_timeout(context, "connect the baseline MySQL client")?;
        let mut connection =
            mysql_actor::connect(context.mysql_user(), context.mysql_port(), connect_timeout)?;
        context.action("connected through the public MySQL protocol");

        let shallow_shape = measure_plan_shape(&mut connection, SHALLOW_QUERY)
            .context("explain the shallow fixture")?;

        let deep_shape = measure_plan_shape(&mut connection, DEEP_QUERY)
            .with_context(|| format!("explain fixed deep fixture {DEEP_FIXTURE_ID}"))?;

        let mut reports = Vec::new();
        for (name, query, shape) in [
            ("shallow", SHALLOW_QUERY, shallow_shape),
            (DEEP_FIXTURE_ID, DEEP_QUERY, deep_shape),
        ] {
            let report = measure_fixture(context, &mut connection, name, query, shape)?;
            context.action(format!(
                "measured {name}: {} plan fragments, {} exchange levels, median first row {:?}, median total {:?}",
                report.plan_fragments,
                report.exchange_levels,
                median(&report.measured, |sample| sample.first_row),
                median(&report.measured, |sample| sample.total),
            ));
            reports.push(report);
        }

        // The whole point of the pair is that one is deeper than the other. If
        // the planner ever flattens the deep fixture, the comparison silently
        // stops measuring what it claims to, so this is the one latency-shaped
        // fact worth failing on.
        let shallow = &reports[0];
        let deep = &reports[1];
        ensure!(
            deep.exchange_levels > shallow.exchange_levels,
            "no deep candidate stacks more exchange levels than the shallow fixture \
             (fixed fixture was {} with {} levels against {}), so the frozen fixture no longer \
             measures plan depth. Revise the fixture as a new explicit version before measuring.",
            deep.name,
            deep.exchange_levels,
            shallow.exchange_levels
        );

        let resources = context.handle().query_execution_resource_snapshot()?;
        let backend_count = resources.map_or(0, |snapshot| snapshot.backends.len());

        let run_manifest = run_manifest.finish_success()?;
        write_report(
            context.scenario_root(),
            &reports,
            backend_count,
            &run_manifest.run_id,
            &run_manifest.sha256,
        )?;
        context.action(format!(
            "wrote startup-baseline.csv and startup-baseline.md under {}",
            context.scenario_root().display()
        ));
        Ok(())
    }
}

fn require_backends(context: &mut ScenarioContext) -> Result<()> {
    let topology = context.handle().frontend_backend_topology()?;
    ensure!(
        topology.len() == REQUIRED_BACKENDS,
        "startup baseline requires exactly {REQUIRED_BACKENDS} backends, saw {}",
        topology.len()
    );
    context.action(format!(
        "verified native 1FE+{REQUIRED_BACKENDS}BE topology"
    ));
    Ok(())
}

fn bounded_timeout(context: &ScenarioContext, operation: &str) -> Result<Duration> {
    Ok(context.remaining(operation)?.min(IO_TIMEOUT_CAP))
}

/// The plan shape of one fixture, read from `EXPLAIN`.
///
/// Recording the measured shape beats asserting a guessed one: the numbers go
/// into the report so a later comparison can prove it ran the same shape,
/// without this scenario having to hardcode a plan the optimizer owns.
struct PlanShape {
    plan_fragments: usize,
    exchange_levels: usize,
    plan_sha256: String,
}

fn measure_plan_shape(connection: &mut mysql::Conn, query: &str) -> Result<PlanShape> {
    // Only the detailed levels label fragments; plain `EXPLAIN` prints the
    // operator tree without the `PLAN FRAGMENT` headers this counts.
    let lines: Vec<String> = connection
        .query(format!("EXPLAIN VERBOSE {query}"))
        .context("EXPLAIN VERBOSE the fixture")?;
    let plan_fragments = lines
        .iter()
        .filter(|line| line.contains("PLAN FRAGMENT"))
        .count();
    let exchange_levels = lines
        .iter()
        .filter(|line| line.contains("EXCHANGE"))
        .count();
    let plan_sha256 = format!("{:x}", Sha256::digest(lines.join("\n").as_bytes()));
    ensure!(
        plan_fragments > 0,
        "EXPLAIN VERBOSE produced no plan fragments, so the fixture is not \
         distributed and there is no startup to measure. Plan was:\n{}",
        lines
            .iter()
            .take(40)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    Ok(PlanShape {
        plan_fragments,
        exchange_levels,
        plan_sha256,
    })
}

fn measure_fixture(
    context: &mut ScenarioContext,
    connection: &mut mysql::Conn,
    name: &'static str,
    query: &str,
    shape: PlanShape,
) -> Result<FixtureReport> {
    let mut warmup = Vec::with_capacity(WARMUP_RUNS);
    for run in 0..WARMUP_RUNS {
        context.remaining(&format!("{name} warm-up run {run}"))?;
        warmup.push(measure_one(connection, query)?);
    }
    let mut measured = Vec::with_capacity(MEASURED_RUNS);
    for run in 0..MEASURED_RUNS {
        context.remaining(&format!("{name} measured run {run}"))?;
        measured.push(measure_one(connection, query)?);
    }
    Ok(FixtureReport {
        name,
        plan_fragments: shape.plan_fragments,
        exchange_levels: shape.exchange_levels,
        query_sha256: format!("{:x}", Sha256::digest(query.as_bytes())),
        plan_sha256: shape.plan_sha256,
        warmup,
        measured,
    })
}

/// Runs one query, stopping the clock twice: when the first row arrives and
/// when the result is fully drained.
fn measure_one(connection: &mut mysql::Conn, query: &str) -> Result<RunSample> {
    let started = Instant::now();
    let mut rows = connection
        .query_iter(query)
        .context("dispatch the fixture query")?;
    let mut first_row = None;
    let mut observed = 0usize;
    for row in rows.by_ref() {
        let _ = row.context("read a fixture result row")?;
        if first_row.is_none() {
            first_row = Some(started.elapsed());
        }
        observed += 1;
    }
    let total = started.elapsed();
    ensure!(
        observed > 0,
        "the fixture returned no rows, so there is no first-row latency to measure"
    );
    Ok(RunSample {
        first_row: first_row.expect("a row was observed"),
        total,
    })
}

fn median(samples: &[RunSample], select: impl Fn(&RunSample) -> Duration) -> Duration {
    percentile(samples, &select, 50)
}

fn percentile(
    samples: &[RunSample],
    select: &impl Fn(&RunSample) -> Duration,
    percent: usize,
) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    let mut values: Vec<Duration> = samples.iter().map(select).collect();
    values.sort_unstable();
    // Nearest-rank: with 20 samples p95 is the 19th, which is what a small
    // sample can honestly report.
    let rank = (values.len() * percent).div_ceil(100).max(1) - 1;
    values[rank.min(values.len() - 1)]
}

fn write_report(
    root: &Path,
    reports: &[FixtureReport],
    backend_count: usize,
    run_id: &str,
    run_manifest_sha256: &str,
) -> Result<()> {
    let mut csv = String::from("fixture,phase,run,first_row_micros,total_micros\n");
    for report in reports {
        for (index, sample) in report.warmup.iter().enumerate() {
            csv.push_str(&format!(
                "{},warmup,{},{},{}\n",
                report.name,
                index,
                sample.first_row.as_micros(),
                sample.total.as_micros()
            ));
        }
        for (index, sample) in report.measured.iter().enumerate() {
            csv.push_str(&format!(
                "{},measured,{},{},{}\n",
                report.name,
                index,
                sample.first_row.as_micros(),
                sample.total.as_micros()
            ));
        }
    }
    fs::write(root.join("startup-baseline.csv"), csv)
        .context("write the startup baseline samples")?;

    #[derive(Serialize)]
    struct FixtureIdentity<'a> {
        fixture_id: &'a str,
        query_sha256: &'a str,
        plan_sha256: &'a str,
        plan_fragments: usize,
        exchange_levels: usize,
    }
    #[derive(Serialize)]
    struct StartupIdentity<'a> {
        schema_version: u32,
        run_id: &'a str,
        run_manifest_sha256: &'a str,
        fixtures: Vec<FixtureIdentity<'a>>,
    }
    let identity = StartupIdentity {
        schema_version: 1,
        run_id,
        run_manifest_sha256,
        fixtures: reports
            .iter()
            .map(|report| FixtureIdentity {
                fixture_id: report.name,
                query_sha256: &report.query_sha256,
                plan_sha256: &report.plan_sha256,
                plan_fragments: report.plan_fragments,
                exchange_levels: report.exchange_levels,
            })
            .collect(),
    };
    fs::write(
        root.join("startup-baseline-fixtures.json"),
        serde_json::to_vec_pretty(&identity)?,
    )
    .context("write startup fixture identities")?;

    let mut summary = String::from("# Native distributed startup baseline\n\n");
    summary.push_str(&format!(
        "Topology: 1 FE + {backend_count} BE, separate processes.\n\
         Runs per fixture: {WARMUP_RUNS} warm-up (recorded, excluded) + {MEASURED_RUNS} measured.\n\
         Time to first row is measured client-side on a lazy row iterator.\n\
         Run identity: `{run_id}`; run manifest SHA256: `{run_manifest_sha256}`.\n\n"
    ));
    summary.push_str("| fixture | SQL SHA256 | plan SHA256 | plan fragments | exchange levels | median first row | p95 first row | median total | p95 total |\n");
    summary.push_str("|---|---|---|---:|---:|---:|---:|---:|---:|\n");
    for report in reports {
        summary.push_str(&format!(
            "| {} | `{}` | `{}` | {} | {} | {:?} | {:?} | {:?} | {:?} |\n",
            report.name,
            report.query_sha256,
            report.plan_sha256,
            report.plan_fragments,
            report.exchange_levels,
            median(&report.measured, |sample| sample.first_row),
            percentile(&report.measured, &|sample: &RunSample| sample.first_row, 95),
            median(&report.measured, |sample| sample.total),
            percentile(&report.measured, &|sample: &RunSample| sample.total, 95),
        ));
    }
    summary.push_str(
        "\n## What this does not measure\n\n\
         Per-backend creation and update queue depth, in-flight peaks, and frontend CPU and heap are\n\
         not recorded here. The exposed backend gauges are instantaneous rather than high-water\n\
         marks, and these fixtures are far too small for a 50 ms poll to catch a peak, so any number\n\
         this scenario emitted for them would be an artifact of the sampling rather than a\n\
         measurement. Sample those separately against a fixture large enough to hold a queue.\n\n\
         Startup is also not broken down by phase. The frontend publishes lifecycle latency for only\n\
         two phases, so the pre-cutover path cannot report a per-phase startup split; the first-row\n\
         figure above is the whole control plane plus the first batch, and it is comparable across the\n\
         cutover only as that whole.\n",
    );
    fs::write(root.join("startup-baseline.md"), summary)
        .context("write the startup baseline summary")?;
    Ok(())
}
