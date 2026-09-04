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

//! Native acceptance for the distributed write data plane.
//!
//! These scenarios exist because the interesting claims are all about process
//! boundaries: writers running on several backends, one root aggregating them,
//! and a frontend that is the only thing allowed to commit. None of that is
//! observable in a single process, and an all-in-one run would prove only that
//! the code compiles into one binary.
//!
//! Assertions read published metrics rather than log strings. A backend's
//! tracing output goes to its own log files rather than the stdout the harness
//! captures, so grepping for a log line would assert on a formatting detail;
//! the counters are the contract.
//!
//! Two published families carry almost everything asserted here.
//!
//! The backend's `novarocks_backend_connector_write_*` family says who wrote.
//! Its writer counters are cumulative, so a delta across one statement is
//! exact. Its root prepared-set gauge is a process-wide high-water mark, so a
//! delta across one statement is *not*: a smaller later write leaves it
//! untouched. Every use of it below therefore establishes a precondition on
//! the peak first and then asserts an exact expected value, never "it grew".
//!
//! The frontend's `novarocks_dml_publication_terminal_total` family says how
//! the statement's external publication ended. Its `phase` label is the one
//! fact a write scenario cannot obtain any other way: `pre_dispatch` means the
//! terminal was assigned before the statement could possibly have asked a
//! provider to commit. Every terminal reachable *after* a commit call carries
//! `dispatch_possible`, so "all four `dispatch_possible` counters are
//! unchanged" is the process-boundary form of "the provider's commit was
//! invoked exactly zero times".

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{CrossProcessChildEnvironment, ServerHandle};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use super::connector::{
    await_resource_convergence, connector_launch_config, connector_reader_environment,
    create_catalog, create_warehouse, mysql_endpoint, require_three_backends, resource_baseline,
};

const WRITER_OPENS: &str = "novarocks_backend_connector_write_writer_opens_total";
const WRITER_TOTALS: &str = "novarocks_backend_connector_write_writer_totals";
const ROOT_PEAK: &str = "novarocks_backend_connector_write_root_prepared_set_peak";

/// `LakePublicationFamily::Write` — INSERT and INSERT OVERWRITE.
const WRITE_FAMILY: &str = "write";
/// `LakePublicationFamily::DataMutation` — DELETE, UPDATE and MERGE.
const MUTATION_FAMILY: &str = "data_mutation";

/// Rows per seeded source file. Deliberately small: a seeding statement runs
/// at one driver and rolls few files, which keeps the root prepared-set peak
/// it leaves behind well below what the measured write will reach. The
/// scenarios still derive that ceiling from the metrics rather than assume it.
const SEED_ROWS_PER_FILE: i64 = 2_000;
/// Two seeded files per backend, so the scan fragment reaches every backend
/// and every driver of every instance has its own split to write from.
const SEED_FILES: i64 = 6;
const SEED_ROWS: i64 = SEED_ROWS_PER_FILE * SEED_FILES;
/// `SEED_ROWS * (SEED_ROWS + 1) / 2`, the sum of `1..=SEED_ROWS`.
const SEED_SUM: i64 = SEED_ROWS * (SEED_ROWS + 1) / 2;

/// One sleeping row per file for the abort case's source, and how long each
/// row sleeps.
///
/// Three files so the scan reaches more than one backend, and five seconds so
/// the write stays in flight long enough for a task-creation marker to be
/// observed and a KILL QUERY to be delivered, without adding a wait the
/// scenario's budget would notice. The rows sleep in parallel across splits,
/// so the statement runs for about one delay rather than three.
const ABORT_SOURCE_FILES: i64 = 3;
const ABORT_DELAY_S: i64 = 5;

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(DistributedWriterDataflow),
        Box::new(DistributedWriterOverwrite),
        Box::new(DistributedWriterRowLevel),
        Box::new(DistributedWriterFaults),
    ]
}

/// One backend-count-wide observation of the write data plane.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct WriteCounters {
    opens: f64,
    rows: f64,
    commit_fragments: f64,
    root_peak_entries: f64,
}

fn write_counters(context: &mut ScenarioContext, index: usize) -> Result<WriteCounters> {
    let handle = context.handle();
    Ok(WriteCounters {
        opens: handle.backend_connector_write_metric(index, WRITER_OPENS, "outcome", "opened")?,
        rows: handle.backend_connector_write_metric(index, WRITER_TOTALS, "unit", "rows")?,
        commit_fragments: handle.backend_connector_write_metric(
            index,
            WRITER_TOTALS,
            "unit",
            "commit_fragments",
        )?,
        root_peak_entries: handle.backend_connector_write_metric(
            index,
            ROOT_PEAK,
            "dimension",
            "entries",
        )?,
    })
}

fn all_write_counters(context: &mut ScenarioContext) -> Result<Vec<WriteCounters>> {
    let count = context.handle().be_count();
    (0..count)
        .map(|index| write_counters(context, index))
        .collect()
}

/// What one statement did to the write data plane, per backend.
#[derive(Debug)]
struct WriteDelta {
    opens: Vec<f64>,
    rows: f64,
    commit_fragments: f64,
    /// Backends whose writer-open counter moved. A row count alone cannot
    /// distinguish a distributed write from one backend doing all of it.
    writing_backends: Vec<usize>,
}

fn write_delta(before: &[WriteCounters], after: &[WriteCounters]) -> WriteDelta {
    WriteDelta {
        opens: before
            .iter()
            .zip(after)
            .map(|(before, after)| after.opens - before.opens)
            .collect(),
        rows: before
            .iter()
            .zip(after)
            .map(|(before, after)| after.rows - before.rows)
            .sum(),
        commit_fragments: before
            .iter()
            .zip(after)
            .map(|(before, after)| after.commit_fragments - before.commit_fragments)
            .sum(),
        writing_backends: before
            .iter()
            .zip(after)
            .enumerate()
            .filter(|(_, (before, after))| after.opens > before.opens)
            .map(|(index, _)| index)
            .collect(),
    }
}

/// Identify the one backend that aggregated this statement's prepared write
/// set, using the root's peak-entry gauge.
///
/// The gauge is a process-wide high-water mark, so it cannot be read as a
/// delta. `peak_ceiling` is the caller's established precondition: the highest
/// entry count any *earlier* write on this cluster could have left behind. The
/// statement under test must have produced strictly more entries than that,
/// which makes "the backends now above the ceiling" exactly the set of roots
/// for this statement — and there must be exactly one.
fn identify_root(
    after: &[WriteCounters],
    peak_ceiling: f64,
    expected_entries: f64,
    label: &str,
) -> Result<usize> {
    if expected_entries <= peak_ceiling {
        bail!(
            "{label} aggregated {expected_entries} prepared-set entries, which does not exceed the \
             established peak ceiling {peak_ceiling}; the root gauge is a high-water mark and \
             cannot identify a root that did not raise it"
        );
    }
    let roots = after
        .iter()
        .enumerate()
        .filter(|(_, counters)| counters.root_peak_entries > peak_ceiling)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let [root] = roots.as_slice() else {
        bail!(
            "{label} aggregated on {} root backends (above peak ceiling {peak_ceiling}); expected \
             exactly one. Two roots would each hold a partial set and each believe it complete. \
             counters={after:?}",
            roots.len()
        );
    };
    // The root accepted every fragment the cluster's writers produced. This is
    // what "the frontend received a complete prepared write set" means on the
    // backend side, and it is an exact equality rather than a growth check.
    if (after[*root].root_peak_entries - expected_entries).abs() > f64::EPSILON {
        bail!(
            "{label} root BE[{root}] peaked at {} prepared-set entries but the cluster's writers \
             produced {expected_entries} commit fragments",
            after[*root].root_peak_entries
        );
    }
    Ok(*root)
}

/// The highest root prepared-set peak any earlier write on this cluster left
/// behind. It is the ceiling [`identify_root`] measures the next write against.
fn peak_ceiling(before: &[WriteCounters]) -> f64 {
    before
        .iter()
        .map(|counters| counters.root_peak_entries)
        .fold(0.0_f64, f64::max)
}

/// The frontend's published record of how DML publications ended.
///
/// Only label combinations the frontend pre-creates on every scrape are read,
/// so an absent sample means the frontend changed its vocabulary and is
/// reported rather than silently treated as zero.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct PublicationTerminals {
    pre_dispatch_uncommitted: f64,
    pre_dispatch_unknown: f64,
    pre_dispatch_committed_succeeded: f64,
    pre_dispatch_committed_failed: f64,
    dispatch_uncommitted: f64,
    dispatch_unknown: f64,
    dispatch_committed_succeeded: f64,
    dispatch_committed_failed: f64,
}

impl PublicationTerminals {
    /// Terminals only reachable once the statement has actually called the
    /// provider's commit. `NoOp` also passes through `dispatch_possible`, so
    /// this is an over-approximation in the safe direction: if every one of
    /// these is unchanged, no commit call happened.
    const fn post_commit_total(self) -> f64 {
        self.dispatch_uncommitted
            + self.dispatch_unknown
            + self.dispatch_committed_succeeded
            + self.dispatch_committed_failed
    }
}

fn publication_terminals(
    context: &mut ScenarioContext,
    family: &str,
) -> Result<PublicationTerminals> {
    let handle = context.handle();
    let read = |phase: &str, disposition: &str, finalization: &str| {
        handle.frontend_dml_publication_terminal(family, phase, disposition, finalization)
    };
    Ok(PublicationTerminals {
        pre_dispatch_uncommitted: read("pre_dispatch", "known_uncommitted", "not_applicable")?,
        pre_dispatch_unknown: read("pre_dispatch", "commit_unknown", "not_applicable")?,
        pre_dispatch_committed_succeeded: read("pre_dispatch", "known_committed", "succeeded")?,
        pre_dispatch_committed_failed: read("pre_dispatch", "known_committed", "failed")?,
        dispatch_uncommitted: read("dispatch_possible", "known_uncommitted", "not_applicable")?,
        dispatch_unknown: read("dispatch_possible", "commit_unknown", "not_applicable")?,
        dispatch_committed_succeeded: read("dispatch_possible", "known_committed", "succeeded")?,
        dispatch_committed_failed: read("dispatch_possible", "known_committed", "failed")?,
    })
}

/// Assert that exactly one statement committed exactly one external snapshot.
fn assert_committed_once(
    before: PublicationTerminals,
    after: PublicationTerminals,
    label: &str,
) -> Result<()> {
    let committed = after.dispatch_committed_succeeded - before.dispatch_committed_succeeded;
    if (committed - 1.0).abs() > f64::EPSILON {
        bail!(
            "{label} recorded {committed} committed publication terminals; expected exactly one. \
             before={before:?} after={after:?}"
        );
    }
    let other = after.post_commit_total() - before.post_commit_total() - committed;
    if other.abs() > f64::EPSILON {
        bail!(
            "{label} also recorded {other} non-committed post-dispatch publication terminals; a \
             successful write must reach exactly one terminal. before={before:?} after={after:?}"
        );
    }
    Ok(())
}

/// Assert the connector's commit was never invoked for this statement.
///
/// The frontend can only call `finish_write` after it has marked dispatch
/// possible, and every terminal it can assign from that point carries
/// `phase="dispatch_possible"`. So all four post-dispatch counters staying put
/// *is* an observed commit-invocation count of zero — and it is an exact
/// expected value rather than a comparison that quietly stops discriminating.
/// The statement must additionally have settled as known-uncommitted before
/// dispatch, which is the disposition that tells a client the retry is safe.
fn assert_commit_never_invoked(
    before: PublicationTerminals,
    after: PublicationTerminals,
    case: &str,
) -> Result<f64> {
    let post_commit = after.post_commit_total() - before.post_commit_total();
    if post_commit.abs() > f64::EPSILON {
        bail!(
            "{case}: the frontend recorded {post_commit} post-dispatch publication terminal(s), so \
             the connector's commit was invoked. before={before:?} after={after:?}"
        );
    }
    for (name, delta) in [
        (
            "pre_dispatch commit_unknown",
            after.pre_dispatch_unknown - before.pre_dispatch_unknown,
        ),
        (
            "pre_dispatch known_committed/succeeded",
            after.pre_dispatch_committed_succeeded - before.pre_dispatch_committed_succeeded,
        ),
        (
            "pre_dispatch known_committed/failed",
            after.pre_dispatch_committed_failed - before.pre_dispatch_committed_failed,
        ),
    ] {
        if delta.abs() > f64::EPSILON {
            bail!("{case}: unexpected {name} publication terminal delta {delta}");
        }
    }
    let uncommitted = after.pre_dispatch_uncommitted - before.pre_dispatch_uncommitted;
    if (uncommitted - 1.0).abs() > f64::EPSILON {
        bail!(
            "{case}: the frontend recorded {uncommitted} pre-dispatch known-uncommitted \
             publication terminals; a failed write must settle as exactly one. before={before:?} \
             after={after:?}"
        );
    }
    println!("{case}: observed connector commit invocations = 0 (post-dispatch terminals delta=0)");
    Ok(post_commit)
}

/// Wait until the frontend holds no live query lifecycle attempt.
///
/// The backend resource oracle proves the writers, the root's buffers and the
/// catalog leases were released. This proves the other end: the frontend's own
/// attempt -- and with it the write session that is the only thing allowed to
/// commit -- is gone too. Both halves are needed before "the write left nothing
/// behind" is a statement about the cluster rather than about one role.
fn await_frontend_attempts_drained(context: &mut ScenarioContext, operation: &str) -> Result<()> {
    let deadline = context.deadline();
    loop {
        let latest = context
            .handle()
            .frontend_query_lifecycle_active_attempts()
            .with_context(|| format!("read FE active attempts after {operation}"))?;
        if latest == 0.0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "the frontend still holds {latest} live query lifecycle attempt(s) after \
                 {operation}"
            );
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// The degree of parallelism a write fragment runs at on this host.
///
/// `SET pipeline_dop` is accepted by the session but does not reach a write
/// fragment's pipeline, so the degree is the backend's automatic one:
/// `available_parallelism() / 2`, floored at one. The system harness runs the
/// backends on this same host, so the runner can derive the exact expected
/// value rather than guess at it -- and can say plainly when the host is too
/// small for the claim under test instead of failing an assertion that reads
/// like a product bug.
fn require_multi_driver_host() -> Result<f64> {
    let cores = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    let dop = (cores / 2).max(1);
    if dop < 2 {
        bail!(
            "proving that degree of parallelism opens independent writers needs a host whose \
             automatic write DOP is above one, but available_parallelism()={cores} resolves it to \
             {dop}"
        );
    }
    Ok(dop as f64)
}

fn snapshot_count(control: &mut mysql::Conn, table: &str) -> Result<i64> {
    control
        .query_first::<i64, _>(format!("SELECT count(*) FROM {table}$snapshots"))
        .with_context(|| format!("read {table} snapshot count"))?
        .with_context(|| format!("{table}$snapshots returned no row"))
}

fn row_count(control: &mut mysql::Conn, table: &str) -> Result<i64> {
    control
        .query_first::<i64, _>(format!("SELECT count(*) FROM {table}"))
        .with_context(|| format!("count rows in {table}"))?
        .with_context(|| format!("{table} row count returned no row"))
}

/// Create the catalog and a source table with two committed data files per
/// backend.
///
/// The seeded shape matters twice. `generate_series` is a whole-relation
/// source, so a write reading it is scheduled onto a single instance at one
/// driver no matter how many backends exist -- which is why the seeding writes
/// each leave a one-entry root prepared-set peak behind, and why they cannot
/// themselves demonstrate anything about distribution. Only a scan whose work
/// arrives as runtime splits fans a write out across the cluster, which is
/// what these scenarios are about.
fn seed_source_files(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    source: &str,
    warehouse_name: &str,
) -> Result<()> {
    let warehouse = create_warehouse(context, warehouse_name)?;
    create_catalog(control, catalog, &warehouse)?;
    control
        .query_drop(format!("CREATE DATABASE {catalog}.{database}"))
        .with_context(|| format!("create {catalog}.{database}"))?;
    control
        .query_drop(format!(
            "CREATE TABLE {catalog}.{database}.{source} (v BIGINT)"
        ))
        .with_context(|| format!("create {catalog}.{database}.{source}"))?;
    for file in 0..SEED_FILES {
        let low = file * SEED_ROWS_PER_FILE + 1;
        let high = (file + 1) * SEED_ROWS_PER_FILE;
        control
            .query_drop(format!(
                "INSERT INTO {catalog}.{database}.{source} \
                 SELECT generate_series FROM TABLE(generate_series({low}, {high}))"
            ))
            .with_context(|| format!("seed {catalog}.{database}.{source} rows {low}..{high}"))?;
    }
    Ok(())
}

/// Seeds the one-row-per-file source the abort case writes from.
///
/// Separate from `seed_source_files` because it exists for its delay, not its
/// data. The matrix's own source is twelve thousand rows across six files and
/// a write of it finishes in well under a second, so a KILL QUERY aimed at it
/// would race the commit: the case would pass or fail on scheduling luck
/// rather than on whether an aborted attempt can commit. One row per file
/// gives one sleeping row per split, so the delay is bounded by the sleep
/// rather than multiplied by the row count -- the same shape
/// `distributed-resilience`'s cancellation cases use.
///
/// Written with one statement per row so each row lands in its own file, and
/// therefore its own split: a single statement would leave one file, one
/// split, and a write that reaches one backend.
fn seed_delay_source_files(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    source: &str,
) -> Result<()> {
    control
        .query_drop(format!(
            "CREATE TABLE {catalog}.{database}.{source} (v BIGINT, delay_s BIGINT)"
        ))
        .with_context(|| format!("create {catalog}.{database}.{source}"))?;
    for row in 1..=ABORT_SOURCE_FILES {
        control
            .query_drop(format!(
                "INSERT INTO {catalog}.{database}.{source} VALUES ({row}, {ABORT_DELAY_S})"
            ))
            .with_context(|| format!("seed {catalog}.{database}.{source} row {row}"))?;
    }
    Ok(())
}

/// Proves the shape the design is actually about: writers on more than one
/// backend, more than one writer per backend once the plan runs above one
/// driver, exactly one root aggregating every fragment they produced, and a
/// single snapshot from a frontend that is the only thing that can commit.
struct DistributedWriterDataflow;

impl Scenario for DistributedWriterDataflow {
    fn name(&self) -> &'static str {
        "connector/distributed-writer-dataflow"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let expected_dop = require_multi_driver_host()?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect distributed writer control session")?,
        )?;

        const CATALOG: &str = "distributed_writer";
        const DATABASE: &str = "distributed_writer_db";
        const SOURCE: &str = "distributed_writer_source";
        const TABLE: &str = "distributed_writer_data";
        let target = format!("{CATALOG}.{DATABASE}.{TABLE}");

        context.action("seed one committed source file per backend");
        seed_source_files(
            context,
            &mut control,
            CATALOG,
            DATABASE,
            SOURCE,
            "distributed-writer",
        )?;
        control
            .query_drop(format!(
                "CREATE TABLE {CATALOG}.{DATABASE}.{TABLE} (v BIGINT)"
            ))
            .context("create distributed writer table")?;

        // The root gauge below is a high-water mark, so the measured write can
        // only be attributed to a root if it aggregates strictly more entries
        // than any earlier write on this cluster left behind. Capture that
        // ceiling from the seeding writes rather than assuming one.
        let before = all_write_counters(context)?;
        let ceiling = peak_ceiling(&before);
        let before_terminals = publication_terminals(context, WRITE_FAMILY)?;
        let snapshots_before = snapshot_count(&mut control, &target)?;

        context.action("append through the distributed write dataflow");
        control
            .query_drop(format!(
                "INSERT INTO {target} SELECT v FROM {CATALOG}.{DATABASE}.{SOURCE}"
            ))
            .context("distributed append through the write dataflow")?;

        let after = all_write_counters(context)?;
        let after_terminals = publication_terminals(context, WRITE_FAMILY)?;
        let delta = write_delta(&before, &after);

        // Writers ran on more than one backend. A single-backend write would
        // still produce correct data, so row counts alone cannot prove the
        // plan actually distributed.
        if delta.writing_backends.len() < 2 {
            bail!(
                "distributed append opened writers on {} backend(s); before={before:?} \
                 after={after:?}",
                delta.writing_backends.len()
            );
        }

        // Degree of parallelism opens independent writers rather than sharing
        // one. Each driver of a writer pipeline owns its own connector writer,
        // so a backend running the plan at DOP N must have opened N of them.
        for &index in &delta.writing_backends {
            let opens = delta.opens[index];
            if (opens - expected_dop).abs() > f64::EPSILON {
                bail!(
                    "BE[{index}] opened {opens} writers for a plan running at DOP {expected_dop}; \
                     each driver must open its own writer. opens={:?}",
                    delta.opens
                );
            }
        }

        // Exactly one backend aggregated, and it accepted every fragment the
        // cluster's writers produced.
        let root = identify_root(
            &after,
            ceiling,
            delta.commit_fragments,
            "distributed writer dataflow",
        )?;

        // The root is an ordinary scheduled placement, not a co-located
        // sidecar of a writer. The scheduler pins the single finish instance to
        // `query_id.low() % backend_count`, so it frequently shares a backend
        // with one writer; what must never be true is that it shares one with
        // *every* writer, because then nothing crossed a process boundary.
        if !delta.writing_backends.iter().any(|&index| index != root) {
            bail!(
                "distributed append aggregated on BE[{root}] and every writer ran there too, so no \
                 commit fragment crossed a process boundary; writers={:?}",
                delta.writing_backends
            );
        }

        // Every row a writer accepted is accounted for across the cluster.
        if (delta.rows - SEED_ROWS as f64).abs() > f64::EPSILON {
            bail!("writers accepted {} rows; expected {SEED_ROWS}", delta.rows);
        }
        if delta.commit_fragments < delta.writing_backends.len() as f64 {
            bail!(
                "writers produced {} commit fragments across {} writing backends; expected at \
                 least one each",
                delta.commit_fragments,
                delta.writing_backends.len()
            );
        }
        // Only the frontend commits, and it committed once.
        assert_committed_once(
            before_terminals,
            after_terminals,
            "distributed writer dataflow",
        )?;
        let snapshots_after = snapshot_count(&mut control, &target)?;
        if snapshots_after != snapshots_before + 1 {
            bail!(
                "distributed append moved {target} from {snapshots_before} to {snapshots_after} \
                 snapshots; one statement must publish exactly one"
            );
        }

        let total: Option<i64> = control
            .query_first(format!("SELECT SUM(v) FROM {target}"))
            .context("read back the distributed append")?;
        if total != Some(SEED_SUM) {
            bail!("distributed append read back {total:?}; expected Some({SEED_SUM})");
        }

        // Resource convergence after consecutive writes: the seeding writes,
        // the measured write, and the read-backs above all ran on this
        // cluster, and every heavy per-query resource -- catalog leases held
        // by writer bindings, fragment controls holding root buffers, and
        // native query contexts -- must be back at the pre-write baseline.
        await_frontend_attempts_drained(context, "the distributed writer dataflow")?;
        await_resource_convergence(context, &baseline, "distributed writer dataflow")?;
        Ok(())
    }
}

/// The same dataflow claims for a full-table overwrite.
///
/// An overwrite is the statement most likely to be special-cased into "delete
/// then append", so it gets its own end-to-end evidence that it distributes,
/// aggregates on one root, and publishes one snapshot.
struct DistributedWriterOverwrite;

impl Scenario for DistributedWriterOverwrite {
    fn name(&self) -> &'static str {
        "connector/distributed-writer-overwrite"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let expected_dop = require_multi_driver_host()?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect distributed overwrite control session")?,
        )?;

        const CATALOG: &str = "distributed_overwrite";
        const DATABASE: &str = "distributed_overwrite_db";
        const SOURCE: &str = "distributed_overwrite_source";
        const TABLE: &str = "distributed_overwrite_data";
        let target = format!("{CATALOG}.{DATABASE}.{TABLE}");

        context.action("seed one committed source file per backend and a stale target");
        seed_source_files(
            context,
            &mut control,
            CATALOG,
            DATABASE,
            SOURCE,
            "distributed-writer-overwrite",
        )?;
        control
            .query_drop(format!(
                "CREATE TABLE {CATALOG}.{DATABASE}.{TABLE} (v BIGINT)"
            ))
            .context("create distributed overwrite table")?;
        // The stale content the overwrite must replace. It is written at one
        // driver so it, like the seeding writes, leaves a one-entry root peak.
        control
            .query_drop(format!(
                "INSERT INTO {target} SELECT generate_series FROM TABLE(generate_series(1, 7))"
            ))
            .context("seed the distributed overwrite target with stale rows")?;

        let before = all_write_counters(context)?;
        let ceiling = peak_ceiling(&before);
        let before_terminals = publication_terminals(context, WRITE_FAMILY)?;
        let snapshots_before = snapshot_count(&mut control, &target)?;

        context.action("replace the table through the distributed write dataflow");
        control
            .query_drop(format!(
                "INSERT OVERWRITE {target} SELECT v FROM {CATALOG}.{DATABASE}.{SOURCE}"
            ))
            .context("distributed overwrite through the write dataflow")?;

        let after = all_write_counters(context)?;
        let after_terminals = publication_terminals(context, WRITE_FAMILY)?;
        let delta = write_delta(&before, &after);

        if delta.writing_backends.len() < 2 {
            bail!(
                "distributed overwrite opened writers on {} backend(s); before={before:?} \
                 after={after:?}",
                delta.writing_backends.len()
            );
        }
        for &index in &delta.writing_backends {
            let opens = delta.opens[index];
            if (opens - expected_dop).abs() > f64::EPSILON {
                bail!(
                    "BE[{index}] opened {opens} overwrite writers for a plan running at DOP \
                     {expected_dop}; opens={:?}",
                    delta.opens
                );
            }
        }
        let root = identify_root(
            &after,
            ceiling,
            delta.commit_fragments,
            "distributed writer overwrite",
        )?;
        if !delta.writing_backends.iter().any(|&index| index != root) {
            bail!(
                "distributed overwrite aggregated on BE[{root}] and every writer ran there too; \
                 writers={:?}",
                delta.writing_backends
            );
        }
        if (delta.rows - SEED_ROWS as f64).abs() > f64::EPSILON {
            bail!(
                "overwrite writers accepted {} rows; expected {SEED_ROWS}",
                delta.rows
            );
        }

        assert_committed_once(
            before_terminals,
            after_terminals,
            "distributed writer overwrite",
        )?;
        let snapshots_after = snapshot_count(&mut control, &target)?;
        if snapshots_after != snapshots_before + 1 {
            bail!(
                "distributed overwrite moved {target} from {snapshots_before} to \
                 {snapshots_after} snapshots; one statement must publish exactly one"
            );
        }

        // The stale rows are gone and only the overwritten relation remains.
        let rows = row_count(&mut control, &target)?;
        if rows != SEED_ROWS {
            bail!("overwritten {target} has {rows} rows; expected {SEED_ROWS}");
        }
        let total: Option<i64> = control
            .query_first(format!("SELECT SUM(v) FROM {target}"))
            .context("read back the distributed overwrite")?;
        if total != Some(SEED_SUM) {
            bail!("distributed overwrite read back {total:?}; expected Some({SEED_SUM})");
        }

        await_frontend_attempts_drained(context, "the distributed writer overwrite")?;
        await_resource_convergence(context, &baseline, "distributed writer overwrite")?;
        Ok(())
    }
}

/// Row-level mutations write more than one branch of one relation.
///
/// A merge-on-read DELETE stages deletion vectors, a copy-on-write UPDATE
/// stages rewritten data files, and a MERGE stages both. Each is still one
/// statement, one prepared write set, and one snapshot -- which is the claim
/// that makes a multi-branch mutation atomic rather than two publications that
/// happen to run next to each other.
///
/// The second DELETE is the load-bearing one. A deletion vector replaces the
/// previous vector for its data file rather than accumulating beside it, so if
/// the backend had not read and merged the exact references the frontend
/// froze, the first DELETE's rows would come back. The frontend independently
/// refuses a commit whose artifact superseded a different reference set, so a
/// second DELETE that both commits and preserves the first one's effect is
/// end-to-end evidence that the merge used exactly the frozen references.
struct DistributedWriterRowLevel;

impl Scenario for DistributedWriterRowLevel {
    fn name(&self) -> &'static str {
        "connector/distributed-writer-row-level"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect row-level mutation control session")?,
        )?;

        const CATALOG: &str = "row_level_writer";
        const DATABASE: &str = "row_level_writer_db";
        let warehouse = create_warehouse(context, "distributed-writer-row-level")?;
        create_catalog(&mut control, CATALOG, &warehouse)?;
        control
            .query_drop(format!("CREATE DATABASE {CATALOG}.{DATABASE}"))
            .context("create row-level mutation database")?;

        // A merge-on-read v3 table: DELETE writes deletion vectors.
        let dv = format!("{CATALOG}.{DATABASE}.row_level_dv");
        control
            .query_drop(format!(
                "CREATE TABLE {dv} (id BIGINT, v BIGINT) TBLPROPERTIES \
                 (\"format-version\" = \"3\", \"write.row-lineage\" = \"true\", \
                 \"novarocks.update.mode\" = \"merge-on-read\")"
            ))
            .context("create deletion-vector mutation table")?;
        for file in 0..3 {
            let base = file * 4;
            control
                .query_drop(format!(
                    "INSERT INTO {dv} VALUES ({}, {}), ({}, {}), ({}, {}), ({}, {})",
                    base + 1,
                    base + 1,
                    base + 2,
                    base + 2,
                    base + 3,
                    base + 3,
                    base + 4,
                    base + 4
                ))
                .with_context(|| format!("seed deletion-vector file {file}"))?;
        }

        context.action("delete rows through the deletion-vector branch");
        let before = all_write_counters(context)?;
        let before_terminals = publication_terminals(context, MUTATION_FAMILY)?;
        let dv_snapshots_before = snapshot_count(&mut control, &dv)?;
        control
            .query_drop(format!("DELETE FROM {dv} WHERE id IN (1, 5, 9)"))
            .context("first deletion-vector DELETE")?;
        let after = all_write_counters(context)?;
        let after_terminals = publication_terminals(context, MUTATION_FAMILY)?;
        let delta = write_delta(&before, &after);
        if delta.writing_backends.len() < 2 {
            bail!(
                "deletion-vector DELETE opened writers on {} backend(s); a row-level branch must \
                 distribute like any other write. before={before:?} after={after:?}",
                delta.writing_backends.len()
            );
        }
        if delta.commit_fragments < 1.0 {
            bail!("deletion-vector DELETE produced no commit fragment: {delta:?}");
        }
        assert_committed_once(before_terminals, after_terminals, "deletion-vector DELETE")?;
        let dv_snapshots_after = snapshot_count(&mut control, &dv)?;
        if dv_snapshots_after != dv_snapshots_before + 1 {
            bail!(
                "deletion-vector DELETE moved {dv} from {dv_snapshots_before} to \
                 {dv_snapshots_after} snapshots; expected exactly one"
            );
        }
        let remaining: Vec<i64> = control
            .query(format!("SELECT id FROM {dv} ORDER BY id"))
            .context("read the table after the first deletion-vector DELETE")?;
        if remaining != vec![2, 3, 4, 6, 7, 8, 10, 11, 12] {
            bail!("first deletion-vector DELETE left {remaining:?}");
        }

        context.action("merge a second delete into the existing deletion vectors");
        let before_terminals = publication_terminals(context, MUTATION_FAMILY)?;
        control
            .query_drop(format!("DELETE FROM {dv} WHERE v > 10"))
            .context("second deletion-vector DELETE")?;
        let after_terminals = publication_terminals(context, MUTATION_FAMILY)?;
        assert_committed_once(
            before_terminals,
            after_terminals,
            "merged deletion-vector DELETE",
        )?;
        // If the backend had merged anything other than the exact references
        // the frontend froze, either the frontend would have refused this
        // commit or ids 1, 5 and 9 would be back.
        let remaining: Vec<i64> = control
            .query(format!("SELECT id FROM {dv} ORDER BY id"))
            .context("read the table after the merged deletion-vector DELETE")?;
        if remaining != vec![2, 3, 4, 6, 7, 8, 10] {
            bail!(
                "merged deletion-vector DELETE left {remaining:?}; the second delete must union \
                 with the first rather than replace it"
            );
        }

        // A format-version 2 merge-on-read table takes the position-delete
        // branch instead of the deletion-vector branch.
        let positional = format!("{CATALOG}.{DATABASE}.row_level_positional");
        control
            .query_drop(format!(
                "CREATE TABLE {positional} (id BIGINT, v BIGINT) TBLPROPERTIES \
                 (\"format-version\" = \"2\", \"novarocks.update.mode\" = \"merge-on-read\")"
            ))
            .context("create position-delete mutation table")?;
        for file in 0..3 {
            let base = file * 4;
            control
                .query_drop(format!(
                    "INSERT INTO {positional} VALUES ({}, {}), ({}, {}), ({}, {}), ({}, {})",
                    base + 1,
                    base + 1,
                    base + 2,
                    base + 2,
                    base + 3,
                    base + 3,
                    base + 4,
                    base + 4
                ))
                .with_context(|| format!("seed position-delete file {file}"))?;
        }
        context.action("delete rows through the position-delete branch");
        let before = all_write_counters(context)?;
        let before_terminals = publication_terminals(context, MUTATION_FAMILY)?;
        let positional_snapshots_before = snapshot_count(&mut control, &positional)?;
        control
            .query_drop(format!("DELETE FROM {positional} WHERE id IN (1, 5, 9)"))
            .context("position-delete DELETE")?;
        let after = all_write_counters(context)?;
        let after_terminals = publication_terminals(context, MUTATION_FAMILY)?;
        let delta = write_delta(&before, &after);
        if delta.writing_backends.len() < 2 {
            bail!(
                "position-delete DELETE opened writers on {} backend(s); before={before:?} \
                 after={after:?}",
                delta.writing_backends.len()
            );
        }
        assert_committed_once(before_terminals, after_terminals, "position-delete DELETE")?;
        let positional_snapshots_after = snapshot_count(&mut control, &positional)?;
        if positional_snapshots_after != positional_snapshots_before + 1 {
            bail!(
                "position-delete DELETE moved {positional} from {positional_snapshots_before} to \
                 {positional_snapshots_after} snapshots; expected exactly one"
            );
        }
        let remaining: Vec<i64> = control
            .query(format!("SELECT id FROM {positional} ORDER BY id"))
            .context("read the table after the position-delete DELETE")?;
        if remaining != vec![2, 3, 4, 6, 7, 8, 10, 11, 12] {
            bail!("position-delete DELETE left {remaining:?}");
        }

        // A copy-on-write UPDATE takes the data branch: it rewrites data files
        // rather than staging a delete artifact.
        let cow = format!("{CATALOG}.{DATABASE}.row_level_cow");
        control
            .query_drop(format!(
                "CREATE TABLE {cow} (id BIGINT, v BIGINT) TBLPROPERTIES \
                 (\"format-version\" = \"3\", \"write.row-lineage\" = \"true\", \
                 \"novarocks.update.mode\" = \"copy-on-write\")"
            ))
            .context("create copy-on-write mutation table")?;
        for file in 0..3 {
            let base = file * 4;
            control
                .query_drop(format!(
                    "INSERT INTO {cow} VALUES ({}, {}), ({}, {}), ({}, {}), ({}, {})",
                    base + 1,
                    base + 1,
                    base + 2,
                    base + 2,
                    base + 3,
                    base + 3,
                    base + 4,
                    base + 4
                ))
                .with_context(|| format!("seed copy-on-write file {file}"))?;
        }
        context.action("update rows through the copy-on-write data branch");
        let before = all_write_counters(context)?;
        let before_terminals = publication_terminals(context, MUTATION_FAMILY)?;
        let cow_snapshots_before = snapshot_count(&mut control, &cow)?;
        control
            .query_drop(format!(
                "UPDATE {cow} SET v = v + 100 WHERE id IN (1, 5, 9)"
            ))
            .context("copy-on-write UPDATE")?;
        let after = all_write_counters(context)?;
        let after_terminals = publication_terminals(context, MUTATION_FAMILY)?;
        let delta = write_delta(&before, &after);
        if delta.writing_backends.is_empty() {
            bail!("copy-on-write UPDATE opened no writer: before={before:?} after={after:?}");
        }
        if delta.commit_fragments < 1.0 {
            bail!("copy-on-write UPDATE produced no commit fragment: {delta:?}");
        }
        assert_committed_once(before_terminals, after_terminals, "copy-on-write UPDATE")?;
        let cow_snapshots_after = snapshot_count(&mut control, &cow)?;
        if cow_snapshots_after != cow_snapshots_before + 1 {
            bail!(
                "copy-on-write UPDATE moved {cow} from {cow_snapshots_before} to \
                 {cow_snapshots_after} snapshots; expected exactly one"
            );
        }
        let updated: Vec<i64> = control
            .query(format!("SELECT v FROM {cow} ORDER BY id"))
            .context("read the table after the copy-on-write UPDATE")?;
        if updated != vec![101, 2, 3, 4, 105, 6, 7, 8, 109, 10, 11, 12] {
            bail!("copy-on-write UPDATE left {updated:?}");
        }

        // MERGE folds a delete branch and a data branch into one statement.
        // Both must publish under one snapshot or the statement was not atomic.
        let merge_source = format!("{CATALOG}.{DATABASE}.row_level_merge_source");
        control
            .query_drop(format!(
                "CREATE TABLE {merge_source} (id BIGINT, v BIGINT) TBLPROPERTIES \
                 (\"format-version\" = \"3\", \"write.row-lineage\" = \"true\")"
            ))
            .context("create merge source table")?;
        control
            .query_drop(format!(
                "INSERT INTO {merge_source} VALUES (2, 2000), (99, 9900)"
            ))
            .context("seed merge source rows")?;
        context.action("merge matched and unmatched rows in one statement");
        let before = all_write_counters(context)?;
        let before_terminals = publication_terminals(context, MUTATION_FAMILY)?;
        let merge_snapshots_before = snapshot_count(&mut control, &dv)?;
        control
            .query_drop(format!(
                "MERGE INTO {dv} AS t USING {merge_source} AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = s.v \
                 WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
            ))
            .context("MERGE across the delete and data branches")?;
        let after = all_write_counters(context)?;
        let after_terminals = publication_terminals(context, MUTATION_FAMILY)?;
        let delta = write_delta(&before, &after);
        if delta.commit_fragments < 2.0 {
            bail!(
                "MERGE produced {} commit fragments; a matched update and an unmatched insert \
                 must stage at least one artifact each. delta={delta:?}",
                delta.commit_fragments
            );
        }
        assert_committed_once(before_terminals, after_terminals, "row-level MERGE")?;
        let merge_snapshots_after = snapshot_count(&mut control, &dv)?;
        if merge_snapshots_after != merge_snapshots_before + 1 {
            bail!(
                "MERGE moved {dv} from {merge_snapshots_before} to {merge_snapshots_after} \
                 snapshots; a delete branch and a data branch must fold into exactly one"
            );
        }
        let merged: Vec<(i64, i64)> = control
            .query(format!("SELECT id, v FROM {dv} ORDER BY id"))
            .context("read the table after the MERGE")?;
        if merged
            != vec![
                (2, 2000),
                (3, 3),
                (4, 4),
                (6, 6),
                (7, 7),
                (8, 8),
                (10, 10),
                (99, 9900),
            ]
        {
            bail!("MERGE left {merged:?}");
        }

        await_frontend_attempts_drained(context, "the distributed row-level writes")?;
        await_resource_convergence(context, &baseline, "distributed row-level writes")?;
        Ok(())
    }
}

/// One entry in the fault matrix: how the write data plane is broken, and how
/// the scenario undoes the arming afterwards.
///
/// # The retired sixth case
///
/// This matrix used to carry `CompleteSetTerminalFailure`: every writer
/// finished, the root's prepared set was complete, and one lifecycle
/// participant never reported its terminal outcome, so the write had to fail
/// on the second half of the barrier alone. It is deliberately gone rather
/// than ported, for two independent reasons.
///
/// It has no fault to arm. `TerminalOutcomeSuppress` is claimed only by
/// `QueryLifecycleRegistry`, which no production query reaches, and the
/// outcome it suppressed does not exist: `publish_task_round_convergence`
/// publishes an empty participant-outcome list because the task protocol's
/// per-domain receipts and termination latch replaced that funnel (ADR-0134).
///
/// And its property is one the task protocol deliberately inverts. A lost
/// terminal acknowledgement is *recovered* here, by replaying the exact
/// request -- which is precisely what
/// `tests/sql/correctness/distributed-resilience/sql/task-update-ack-loss.sql`
/// asserts. "Suppress the terminal and the write must fail" would therefore
/// assert the opposite of the design.
///
/// The state it described also cannot reach the commit barrier any more. In
/// `novarocks/frontend/src/coordinator/execution.rs` the task round's drain
/// loop keeps turning past its `client_visible_completion` gate while any
/// declared writer or the root finish task has not published `FINISHED`, and
/// a latched failure cause breaks that loop with an error propagated before
/// the barrier is built. "Complete set, participant silent" is therefore a
/// wait that ends at the statement deadline rather than a commit decision --
/// and the barrier's own refusal of it is covered by the frontend unit tests
/// in `query_execution/write_barrier.rs`. Keeping one matrix case honestly
/// retired beats keeping one that asserts the opposite of the protocol.
enum WriteFault {
    /// A writer fails at commit-fragment egress, after it already staged.
    WriterEgress,
    /// The root rejects a commit-fragment carrier at validation.
    RootValidation,
    /// One backend's participant task is failed after it published RUNNING.
    /// Its writers were already opened while the task was prepared, but the
    /// drivers never run, so the stream it owes the root aggregation never
    /// carries a row and never reaches end-of-stream: the gather cannot close.
    SeveredWriterStream,
    /// The frontend's attempt is aborted while its tasks are running, so it
    /// never fetches the root's complete prepared write set.
    FetchAbort,
}

/// The backend the severed-stream case arms. Fixed so the scenario can name
/// the process its evidence must come from; a backend index, never a count.
const SEVERED_BACKEND: usize = 2;

/// The task protocol's evidence that one task really failed on the backend the
/// fault was armed on.
///
/// It replaces `NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED`, which is
/// emitted only by `NativeFragmentService`'s Stage/Start ingress: the task
/// protocol runs its fragments from `NativeTaskExecutionHost`, so no
/// production query reaches that emitter.
const TASK_EXECUTION_FAILURE: &str = "task-execution-failure";
const TASK_EXECUTION_FAILURE_INJECTED: &str = "NOVAROCKS_TASK_EXECUTION_FAILURE_INJECTED";

/// One task was admitted on a backend, which is this protocol's earliest
/// evidence that an attempt is actually running out there.
const TASK_CREATE_APPLIED: &str = "NOVAROCKS_TASK_CREATE_APPLIED";

/// One backend applied this query's abort. Client cancellation reaches a
/// backend as this operation, so it is what a case about aborting a running
/// write has to count -- the retired chain's own abort and phase markers have
/// no live emitter.
const TASK_CONTEXT_ABORT_APPLIED: &str = "NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED";

impl WriteFault {
    /// How many times this fault has left evidence in a backend process.
    ///
    /// A fault that is armable but never bound to the attempt is inert, and an
    /// inert fault reads exactly like a fault that did not reproduce: the
    /// statement fails for some unrelated reason and the scenario still passes.
    /// Every case therefore has to show that its own injection fired.
    fn injection_marker_count(&self, context: &mut ScenarioContext) -> Result<usize> {
        match self.injection_evidence() {
            InjectionEvidence::Frontend(marker) => {
                let log = context
                    .handle()
                    .fe_log_contents()
                    .context("read FE log for fault-injection evidence")?;
                Ok(log.matches(marker).count())
            }
            InjectionEvidence::Backend(index, marker) => context
                .handle()
                .be_log_count(index, marker)
                .with_context(|| format!("count {marker} on BE[{index}]")),
            InjectionEvidence::EveryBackend(marker) => backend_marker_total(context, marker),
        }
    }

    /// Where this case's own injection leaves its evidence.
    const fn injection_evidence(&self) -> InjectionEvidence {
        match self {
            Self::WriterEgress => InjectionEvidence::Frontend(
                "NOVAROCKS_QUERY_FAULT_BOUND kind=connector-write-writer-failure ",
            ),
            Self::RootValidation => InjectionEvidence::Frontend(
                "NOVAROCKS_QUERY_FAULT_BOUND kind=connector-write-root-failure ",
            ),
            Self::SeveredWriterStream => {
                InjectionEvidence::Backend(SEVERED_BACKEND, TASK_EXECUTION_FAILURE_INJECTED)
            }
            // Summed across the cluster rather than pinned to one backend: an
            // abort reaches the backends the scheduler placed this attempt on,
            // which is a property of the plan and of the splits. What the case
            // needs is that the abort was delivered somewhere at all, because
            // the alternative -- a client error with no abort behind it -- is
            // exactly what a timeout looks like.
            Self::FetchAbort => InjectionEvidence::EveryBackend(TASK_CONTEXT_ABORT_APPLIED),
        }
    }

    /// The text the injected failure puts in the statement's own error, where
    /// the injection is a typed connector rejection rather than a lifecycle
    /// event. It proves the fault fired rather than merely having been bound.
    const fn injected_error_text(&self) -> Option<&'static str> {
        match self {
            Self::WriterEgress => Some("injected connector write writer failure"),
            Self::RootValidation => Some("injected connector write root failure"),
            Self::SeveredWriterStream | Self::FetchAbort => None,
        }
    }

    /// Which of the scenario's two write statements this case runs.
    const fn statement<'a>(&self, statements: &WriteStatements<'a>) -> &'a str {
        match self {
            Self::FetchAbort => statements.delayed,
            Self::WriterEgress | Self::RootValidation | Self::SeveredWriterStream => {
                statements.full
            }
        }
    }

    /// The evidence only this case can produce.
    fn assert_case_evidence(&self, delta: &WriteDelta, case: &str) -> Result<()> {
        match self {
            Self::SeveredWriterStream => {
                // Writers opened on the other backends, so the root really did
                // lose a live sender rather than never having had one.
                if delta.writing_backends.len() < 2 {
                    bail!(
                        "{case}: writers opened on {} backend(s), so no stream into the root was \
                         severed",
                        delta.writing_backends.len()
                    );
                }
                Ok(())
            }
            Self::WriterEgress | Self::RootValidation | Self::FetchAbort => Ok(()),
        }
    }

    const fn case(&self) -> &'static str {
        match self {
            Self::WriterEgress => "a writer that fails at commit-fragment egress",
            Self::RootValidation => "a root that rejects a commit-fragment carrier",
            Self::SeveredWriterStream => "a writer stream that never reaches the root aggregation",
            Self::FetchAbort => "an aborted attempt that never fetches the root's result",
        }
    }
}

/// Where one case's injection leaves the evidence that it actually fired.
enum InjectionEvidence {
    /// In the frontend's own log, because the fault is bound there.
    Frontend(&'static str),
    /// On the one backend the fault was armed on.
    Backend(usize, &'static str),
    /// Anywhere across the backends, summed, because which backends the
    /// marker reaches is a property of the plan rather than of the cluster.
    EveryBackend(&'static str),
}

/// The two write statements the fault matrix runs.
struct WriteStatements<'a> {
    /// The whole seeded source. Every case that does not have to act on a
    /// running write uses it, so the case's own fault is the only reason the
    /// statement takes any time at all.
    full: &'a str,
    /// The sleeping source, for the one case that has to abort a write while
    /// it is still running.
    delayed: &'a str,
}

/// A failed write must leave nothing behind.
///
/// This is the claim the whole dual barrier exists to make: whatever goes wrong
/// in the data plane or in the lifecycle, the connector is never asked to
/// commit, so no snapshot appears. Asserting it needs a real cluster because
/// the failure has to happen on a backend while the frontend is deciding.
///
/// The matrix walks the write path end to end. A writer can fail on its way out
/// of the data plane; the root can refuse what arrives; a writer's stream can
/// stop arriving at all; the frontend can be aborted before it fetches the
/// root's result; and -- the case that separates the two halves of the barrier
/// -- the data plane can close completely while the lifecycle still fails.
/// Every case ends with the same published fact: zero commit invocations.
struct DistributedWriterFaults;

impl Scenario for DistributedWriterFaults {
    fn name(&self) -> &'static str {
        "connector/distributed-writer-faults"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect distributed writer fault control session")?,
        )?;

        const CATALOG: &str = "distributed_writer_faults";
        const DATABASE: &str = "distributed_writer_fault_db";
        const SOURCE: &str = "distributed_writer_fault_source";
        const DELAY_SOURCE: &str = "distributed_writer_fault_delay";
        const TABLE: &str = "distributed_writer_fault_data";
        let target = format!("{CATALOG}.{DATABASE}.{TABLE}");

        context.action("seed one committed source file per backend");
        seed_source_files(
            context,
            &mut control,
            CATALOG,
            DATABASE,
            SOURCE,
            "distributed-writer-faults",
        )?;
        seed_delay_source_files(&mut control, CATALOG, DATABASE, DELAY_SOURCE)?;
        control
            .query_drop(format!(
                "CREATE TABLE {CATALOG}.{DATABASE}.{TABLE} (v BIGINT)"
            ))
            .context("create distributed writer fault table")?;

        let snapshots_before = snapshot_count(&mut control, &target)?;
        let insert = format!("INSERT INTO {target} SELECT v FROM {CATALOG}.{DATABASE}.{SOURCE}");
        // One sleeping row per file, so the write is genuinely in flight while
        // the scenario acts on it. `sleep` is on the engine's
        // non-deterministic list, so it survives constant folding.
        let delayed_insert = format!(
            "INSERT INTO {target} SELECT v FROM {CATALOG}.{DATABASE}.{DELAY_SOURCE} \
             WHERE sleep(delay_s)"
        );
        let statements = WriteStatements {
            full: &insert,
            delayed: &delayed_insert,
        };

        // A committed row would prove the frontend committed despite a broken
        // write, so seed nothing into the target and require it to stay empty.
        for fault in [
            WriteFault::WriterEgress,
            WriteFault::RootValidation,
            WriteFault::SeveredWriterStream,
            WriteFault::FetchAbort,
        ] {
            let case = fault.case();
            context.action(format!("inject {case} and require no snapshot"));
            let before_terminals = publication_terminals(context, WRITE_FAMILY)?;
            let before = all_write_counters(context)?;
            let before_injections = fault.injection_marker_count(context)?;

            let outcome = run_faulted_write(context, &fault, &user, port, &statements)?;
            let Err(error) = outcome else {
                bail!("{case} did not fail the write");
            };
            let error = format!("{error}");
            if let Some(expected) = fault.injected_error_text()
                && !error.contains(expected)
            {
                bail!("{case} failed with {error:?}, which does not carry {expected:?}");
            }

            let after_terminals = publication_terminals(context, WRITE_FAMILY)?;
            let after = all_write_counters(context)?;
            let delta = write_delta(&before, &after);
            let commits = assert_commit_never_invoked(before_terminals, after_terminals, case)?;
            println!(
                "distributed-writer-faults case={case:?} commit_invocations={commits} \
                 writer_opens_delta={:?} commit_fragments_delta={}",
                delta.opens, delta.commit_fragments
            );
            let after_injections = fault.injection_marker_count(context)?;
            if after_injections <= before_injections {
                bail!(
                    "{case} produced no fault-injection evidence on its target backend \
                     ({before_injections} -> {after_injections}); the write may have failed for an \
                     unrelated reason"
                );
            }
            fault.assert_case_evidence(&delta, case)?;

            let rows = row_count(&mut control, &target)?;
            if rows != 0 {
                bail!("{case} left {rows} committed rows; expected none");
            }
            let snapshots = snapshot_count(&mut control, &target)?;
            if snapshots != snapshots_before {
                bail!(
                    "{case} moved {target} from {snapshots_before} to {snapshots} snapshots; a \
                     failed write must publish none"
                );
            }
        }

        // The cluster is still able to write after the whole matrix, which is
        // what makes "nothing was left behind" a statement about resources
        // rather than only about the catalog.
        context.action("write successfully after the fault matrix");
        control
            .query_drop(&insert)
            .context("write after the distributed writer fault matrix")?;
        let rows = row_count(&mut control, &target)?;
        if rows != SEED_ROWS {
            bail!("post-fault write left {rows} rows; expected {SEED_ROWS}");
        }

        await_frontend_attempts_drained(context, "the distributed writer fault matrix")?;
        await_resource_convergence(context, &baseline, "distributed writer faults")?;
        Ok(())
    }
}

/// Arm one fault, run the write, and undo the arming.
///
/// The two cases that hold a process open -- a fragment failure that blocks
/// until the runner releases it, and a frontend that blocks at a lifecycle
/// phase until the runner acts -- run the write on its own connection so the
/// scenario thread stays free to release them.
fn run_faulted_write(
    context: &mut ScenarioContext,
    fault: &WriteFault,
    user: &str,
    port: u16,
    statements: &WriteStatements<'_>,
) -> Result<Result<(), mysql::Error>> {
    let case = fault.case();
    let insert = fault.statement(statements);
    match fault {
        WriteFault::WriterEgress | WriteFault::RootValidation => {
            let kind = match fault {
                WriteFault::WriterEgress => "connector-write-writer-failure",
                _ => "connector-write-root-failure",
            };
            for index in 0..context.handle().be_count() {
                context
                    .handle()
                    .arm_query_lifecycle_fault(index, kind)
                    .with_context(|| format!("arm {kind} on BE[{index}]"))?;
            }
            let outcome = run_write_on_own_connection(context, user, port, insert, case)?;
            context
                .handle()
                .clear_query_lifecycle_faults()
                .with_context(|| format!("clear {kind} tokens"))?;
            Ok(outcome)
        }
        WriteFault::SeveredWriterStream => {
            // One task on this backend is failed after it published RUNNING,
            // so the root's gather keeps a sender that never reaches
            // end-of-stream.
            //
            // This replaced `arm_fragment_executor_failure`, whose injection
            // point is `NativeFragmentService`'s Stage/Start ingress: the task
            // protocol runs its fragments from `NativeTaskExecutionHost`, so
            // that trigger is never consumed, the write succeeds untouched,
            // and the case waits out its budget for a marker with no emitter.
            // `TaskExecutionFailure` is the successor with the same shape --
            // the task is genuinely admitted and genuinely started before it
            // fails -- and it needs no release, because it is an injection
            // rather than a rendezvous.
            //
            // It is still the closest injection the fault channel offers to a
            // transport-level loss between a writer and the root. There is no
            // failpoint inside the exchange itself, and the exchange ingress
            // handler has no query execution identity to scope one to, so a
            // transport fault would have to be a second, unscoped channel.
            //
            // The fault is not one of the kinds that pin a single-instance
            // fragment, so the root finish task may land on this backend and
            // be the task that fails. The case's own evidence covers that: it
            // requires writers to have opened on at least two backends, so a
            // root that failed still failed with live senders behind it.
            context
                .handle()
                .arm_query_lifecycle_fault(SEVERED_BACKEND, TASK_EXECUTION_FAILURE)
                .context("arm the severed writer task failure")?;
            let outcome = run_write_on_own_connection(context, user, port, insert, case)?;
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("clear the severed writer task failure")?;
            Ok(outcome)
        }
        WriteFault::FetchAbort => {
            // The attempt is aborted while its tasks are running, so the
            // frontend never fetches the root's complete prepared write set.
            //
            // The retired chain gave this case a frontend rendezvous: the
            // runner armed a phase trigger and the coordinator parked at
            // `running` until the kill landed. That barrier is
            // `FrontendQueryLifecycleBarrier::start_all`, which only ANALYZE
            // reaches now, so waiting on its marker waits out the whole
            // budget. The task protocol's anchor is a task actually being
            // created on a backend -- the same trigger
            // `distributed-resilience`'s `task-kill-query` case uses -- and
            // the sleeping source is what keeps the write in flight long
            // enough for the kill to land inside it rather than after it.
            let created_baseline = backend_marker_total(context, TASK_CREATE_APPLIED)?;
            let write = start_write_on_own_connection(user, port, insert)?;
            let connection_id = write
                .connection_id
                .recv_timeout(context.remaining("receive aborted write connection id")?)
                .context("aborted write session ended before publishing its connection id")?;
            await_backend_marker_above(
                context,
                TASK_CREATE_APPLIED,
                created_baseline,
                "a task admitted for the write this case aborts",
            )?;
            let deadline = context.deadline();
            context
                .handle()
                .kill_query_until(connection_id, deadline)
                .context("abort the in-flight distributed write")?;
            let outcome = write
                .done
                .recv_timeout(context.remaining("await the aborted write result")?)
                .context("aborted write session did not finish")?;
            write
                .thread
                .join()
                .map_err(|_| anyhow::anyhow!("aborted write session thread panicked"))??;
            Ok(outcome)
        }
    }
}

struct PendingWrite {
    thread: thread::JoinHandle<Result<()>>,
    connection_id: mpsc::Receiver<u32>,
    done: mpsc::Receiver<Result<(), mysql::Error>>,
}

/// Run one faulted write on its own connection.
///
/// Each case gets a fresh session so a broken attempt cannot leave session
/// state behind for the next one, and so the control session stays usable for
/// the read-backs that follow.
fn run_write_on_own_connection(
    context: &mut ScenarioContext,
    user: &str,
    port: u16,
    insert: &str,
    case: &str,
) -> Result<Result<(), mysql::Error>> {
    let mut connection = mysql_actor::connect(
        user,
        port,
        context.remaining(&format!("connect the {case} write session"))?,
    )?;
    Ok(connection.query_drop(insert))
}

fn start_write_on_own_connection(user: &str, port: u16, insert: &str) -> Result<PendingWrite> {
    let (id_tx, connection_id) = mpsc::sync_channel(1);
    let (done_tx, done) = mpsc::sync_channel(1);
    let user = user.to_string();
    let insert = insert.to_string();
    let thread = thread::Builder::new()
        .name("distributed-writer-abort".to_string())
        .spawn(move || -> Result<()> {
            // This session's statement is deliberately left hanging until
            // another session cancels it, so it uses the connection form whose
            // wait is bounded by the scenario rather than by a socket read
            // timeout.
            let mut connection =
                mysql_actor::connect_for_cancellation(&user, port, Duration::from_secs(30))?;
            id_tx
                .send(connection.connection_id())
                .context("publish the aborted write connection id")?;
            let outcome = connection.query_drop(insert);
            done_tx
                .send(outcome)
                .context("publish the aborted write result")
        })
        .context("start the aborted write session")?;
    Ok(PendingWrite {
        thread,
        connection_id,
        done,
    })
}

/// How many times `marker` appears across every Backend log.
///
/// Summed rather than counted per Backend: which Backends an attempt reaches
/// is a property of its plan and its splits, so a per-Backend expectation
/// would be asserting the scheduler's current choice. Every use below cares
/// only that the total moved.
fn backend_marker_total(context: &mut ScenarioContext, marker: &str) -> Result<usize> {
    let mut total = 0;
    for index in 0..context.handle().be_count() {
        total += context
            .handle()
            .be_log_count(index, marker)
            .with_context(|| format!("count {marker} on BE[{index}]"))?;
    }
    Ok(total)
}

/// Waits until `marker` has appeared somewhere it had not before.
fn await_backend_marker_above(
    context: &mut ScenarioContext,
    marker: &str,
    baseline: usize,
    subject: &str,
) -> Result<()> {
    let deadline = context.deadline();
    loop {
        if backend_marker_total(context, marker)? > baseline {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for {subject}: {marker} stayed at {baseline}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}
