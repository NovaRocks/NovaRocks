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
use anyhow::{Context, Result, anyhow, bail};
use mysql::prelude::Queryable;
use mysql::{Row, Value};
use novarocks_cluster_harness::{
    QueryLifecycleFailureSite, QueryLifecycleStructuredSnapshot, ServerHandle,
};
use novarocks_connector_iceberg::iceberg::puffin::APACHE_DATASKETCHES_THETA_V1;
use novarocks_connector_iceberg::iceberg::spec::TableMetadata;
use novarocks_execution_contract::{
    TaskFailurePhase,
    failure::{FailureClass, FailureFact, FailureReason, domain},
};
use std::collections::BTreeSet;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use super::native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec};
#[path = "writer_statement_actor.rs"]
mod writer_statement_actor;
use writer_statement_actor::{WriterActor, retain_write_result};
#[path = "distributed_writer_row_level.rs"]
mod row_level_contract;

use super::connector::{
    await_resource_convergence, connector_launch_config, create_catalog, create_warehouse,
    mysql_endpoint, require_three_backends, resource_baseline,
};

const WRITER_OPENS: &str = "novarocks_backend_connector_write_writer_opens_total";
const WRITER_TOTALS: &str = "novarocks_backend_connector_write_writer_totals";
const WRITER_ABORTS: &str = "novarocks_backend_connector_write_writer_aborts_total";
const WRITE_DEBUG_FAULTS: &str = "novarocks_backend_connector_write_debug_faults_total";
const ROOT_PEAK: &str = "novarocks_backend_connector_write_root_prepared_set_peak";
const RESULT_TERMINALS: &str = "novarocks_backend_fragment_result_terminals_total";
const WRITER_PARTIAL_TOTALS: &str = "novarocks_table_writer_partial_totals";

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
const WIDE_STATISTICS_CHANNELS: usize = 1_024;
const WIDE_SEED_FILES: i64 = 12;
// Compact Theta v3 stores 16 + 8*n bytes for exact n > 1. Sixteen
// distinct inputs produce 144 bytes, crossing the IPC 64-byte payload bucket.
// This keeps the 1024-channel case a real sparse-packing fixture when an
// all-valid IPC bitmap is omitted, without changing the packet limit.
const WIDE_ROWS_PER_FILE: i64 = 16;
const WIDE_ROWS: i64 = WIDE_ROWS_PER_FILE * WIDE_SEED_FILES;
const STATISTICS_PACKET_BYTES: usize = 320 * 1024;

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
        Box::new(DistributedWriterConcurrentProgress::default()),
        Box::new(DistributedStatisticsDataflow),
        Box::new(DistributedWriterOverwrite),
        Box::new(DistributedWriterRowLevel::default()),
        Box::new(DistributedWriterFaults::default()),
    ]
}

/// One backend-count-wide observation of the write data plane.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct WriteCounters {
    opens: f64,
    rows: f64,
    commit_fragments: f64,
    abort_succeeded: f64,
    abort_failed: f64,
    append_holds: f64,
    result_finished: f64,
    result_aborted: f64,
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
        abort_succeeded: handle.backend_connector_write_metric(
            index,
            WRITER_ABORTS,
            "outcome",
            "succeeded",
        )?,
        abort_failed: handle.backend_connector_write_metric(
            index,
            WRITER_ABORTS,
            "outcome",
            "failed",
        )?,
        append_holds: handle.backend_connector_write_metric(
            index,
            WRITE_DEBUG_FAULTS,
            "kind",
            "append_hold",
        )?,
        result_finished: handle.backend_connector_write_metric(
            index,
            RESULT_TERMINALS,
            "terminal",
            "finished",
        )?,
        result_aborted: handle.backend_connector_write_metric(
            index,
            RESULT_TERMINALS,
            "terminal",
            "aborted",
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

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct WriterPartialCounters {
    batches: f64,
    rows: f64,
    bytes: f64,
    non_null_channels: f64,
    sparse_rows: f64,
}

fn writer_partial_counters(
    context: &mut ScenarioContext,
    index: usize,
) -> Result<WriterPartialCounters> {
    let handle = context.handle();
    let metric =
        |unit| handle.backend_connector_write_metric(index, WRITER_PARTIAL_TOTALS, "unit", unit);
    Ok(WriterPartialCounters {
        batches: metric("batches")?,
        rows: metric("rows")?,
        bytes: metric("bytes")?,
        non_null_channels: metric("non_null_channels")?,
        sparse_rows: metric("sparse_rows")?,
    })
}

fn all_writer_partial_counters(
    context: &mut ScenarioContext,
) -> Result<Vec<WriterPartialCounters>> {
    let count = context.handle().be_count();
    (0..count)
        .map(|index| writer_partial_counters(context, index))
        .collect()
}

fn writer_partial_delta(
    before: &[WriterPartialCounters],
    after: &[WriterPartialCounters],
) -> Vec<WriterPartialCounters> {
    before
        .iter()
        .zip(after)
        .map(|(before, after)| WriterPartialCounters {
            batches: after.batches - before.batches,
            rows: after.rows - before.rows,
            bytes: after.bytes - before.bytes,
            non_null_channels: after.non_null_channels - before.non_null_channels,
            sparse_rows: after.sparse_rows - before.sparse_rows,
        })
        .collect()
}

/// What one statement did to the write data plane, per backend.
#[derive(Debug)]
struct WriteDelta {
    opens: Vec<f64>,
    rows: f64,
    commit_fragments: f64,
    abort_succeeded: f64,
    abort_failed: f64,
    append_holds: Vec<f64>,
    result_finished: f64,
    result_aborted: f64,
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
        abort_succeeded: before
            .iter()
            .zip(after)
            .map(|(before, after)| after.abort_succeeded - before.abort_succeeded)
            .sum(),
        abort_failed: before
            .iter()
            .zip(after)
            .map(|(before, after)| after.abort_failed - before.abort_failed)
            .sum(),
        append_holds: before
            .iter()
            .zip(after)
            .map(|(before, after)| after.append_holds - before.append_holds)
            .collect(),
        result_finished: before
            .iter()
            .zip(after)
            .map(|(before, after)| after.result_finished - before.result_finished)
            .sum(),
        result_aborted: before
            .iter()
            .zip(after)
            .map(|(before, after)| after.result_aborted - before.result_aborted)
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
pub(super) struct PublicationTerminals {
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

pub(super) fn publication_terminals(
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
pub(super) fn assert_commit_never_invoked(
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
        (
            "dispatch_possible known_uncommitted",
            after.dispatch_uncommitted - before.dispatch_uncommitted,
        ),
        (
            "dispatch_possible commit_unknown",
            after.dispatch_unknown - before.dispatch_unknown,
        ),
        (
            "dispatch_possible known_committed/succeeded",
            after.dispatch_committed_succeeded - before.dispatch_committed_succeeded,
        ),
        (
            "dispatch_possible known_committed/failed",
            after.dispatch_committed_failed - before.dispatch_committed_failed,
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

// Three normal DMLs share one real Blocking worker. Internal positions and
// retained payload use the existing Result budget; transport values below are
// the original Native defaults, not another admission model or a heap quote.
const CONCURRENT_CAPACITY_OVERLAY: &str = r#"
query_blocking_worker_threads = 1
query_blocking_queue_capacity = 64
task_dispatch_create_permits = 16
task_dispatch_update_permits = 12
task_dispatch_lifecycle_permits = 4
task_dispatch_control_permits = 4
task_operation_max_batch_items = 32
task_operation_max_batch_encoded_bytes = 50331648
task_descriptor_max_encoded_bytes = 16777216
task_query_backend_max_queued_operations = 4096
task_query_backend_max_queued_bytes = 268435456
task_backend_max_queued_operations = 16384
task_backend_max_queued_bytes = 536870912
[runtime.frontend_workload]
concurrency_limit = 3
internal_result_positions = 3
internal_result_all_objects_bytes = 1073741824
scope_records_limit = 8192
"#;
// Keep the original BE root/process byte budgets explicit in the actual launch
// config. They retain Arrow input plus encoded output until their real exit.
const CONCURRENT_BACKEND_RESULT_OVERLAY: &str = r#"
result_retained_bytes_per_root = 16777216
result_retained_bytes_per_process = 4294967296
"#;
const CONCURRENT_CAPACITY_EXPECTED: &[(&[&str], i64)] = &[
    (&["runtime", "query_blocking_worker_threads"], 1),
    (&["runtime", "query_blocking_queue_capacity"], 64),
    (&["runtime", "task_dispatch_create_permits"], 16),
    (&["runtime", "task_dispatch_update_permits"], 12),
    (&["runtime", "task_dispatch_lifecycle_permits"], 4),
    (&["runtime", "task_dispatch_control_permits"], 4),
    (&["runtime", "task_operation_max_batch_items"], 32),
    (
        &["runtime", "task_operation_max_batch_encoded_bytes"],
        50331648,
    ),
    (&["runtime", "task_descriptor_max_encoded_bytes"], 16777216),
    (
        &["runtime", "task_query_backend_max_queued_operations"],
        4096,
    ),
    (
        &["runtime", "task_query_backend_max_queued_bytes"],
        268435456,
    ),
    (&["runtime", "task_backend_max_queued_operations"], 16384),
    (&["runtime", "task_backend_max_queued_bytes"], 536870912),
    (&["runtime", "frontend_workload", "concurrency_limit"], 3),
    (
        &[
            "runtime",
            "frontend_workload",
            "internal_result_all_objects_bytes",
        ],
        1073741824,
    ),
    (
        &["runtime", "frontend_workload", "scope_records_limit"],
        8192,
    ),
];
const CONCURRENT_BACKEND_RESULT_EXPECTED: &[(&[&str], i64)] = &[
    (&["runtime", "result_retained_bytes_per_root"], 16777216),
    (
        &["runtime", "result_retained_bytes_per_process"],
        4294967296,
    ),
];
const CONCURRENT_WRITERS: usize = 3;
const CONCURRENT_APPEND_HOLD: &str = "connector-write-append-hold";
const CONCURRENT_CATALOG: &str = "distributed_writer_concurrent";
const CONCURRENT_DATABASE: &str = "distributed_writer_concurrent_db";
const CONCURRENT_SOURCE: &str = "distributed_writer_concurrent_source";

#[derive(Default)]
struct DistributedWriterConcurrentProgress {
    intent: SqlIntentOwner,
}

struct ConcurrentWriter {
    ordinal: usize,
    selected: SelectedStatement,
    actor: WriterActor,
    early_outcome: Option<mysql::Result<()>>,
}

fn concurrent_target(ordinal: usize) -> String {
    format!("{CONCURRENT_CATALOG}.{CONCURRENT_DATABASE}.target_{ordinal}")
}

fn concurrent_insert(ordinal: usize) -> String {
    format!(
        "INSERT INTO {} SELECT v FROM {CONCURRENT_CATALOG}.{CONCURRENT_DATABASE}.{CONCURRENT_SOURCE}",
        concurrent_target(ordinal)
    )
}

fn require_concurrent_effective_config(context: &ScenarioContext) -> Result<usize> {
    let evidence: serde_json::Value =
        serde_json::from_slice(context.effective_launch_config_evidence().artifact_bytes())?;
    let roles = evidence["semantics"]["roles"]
        .as_array()
        .context("missing effective roles for concurrent writer scenario")?;
    let frontend = roles
        .iter()
        .filter(|role| role["role"] == "fe")
        .collect::<Vec<_>>();
    anyhow::ensure!(
        frontend.len() == 1,
        "concurrent writer scenario requires one effective FE config"
    );
    let require_values = |role: &serde_json::Value, expected: &[(&[&str], i64)]| -> Result<()> {
        for &(path, expected_value) in expected {
            let mut value = &role["effective_config"];
            for key in path {
                anyhow::ensure!(
                    value["type"] == "table",
                    "effective {} config has no table before {}",
                    role["role"],
                    path.join(".")
                );
                value = &value["value"][*key];
            }
            anyhow::ensure!(
                value["type"] == "integer" && value["value"].as_i64() == Some(expected_value),
                "concurrent writer effective {} {} must be exactly {expected_value}",
                role["role"],
                path.join(".")
            );
        }
        Ok(())
    };
    require_values(frontend[0], CONCURRENT_CAPACITY_EXPECTED)?;
    let backends = roles
        .iter()
        .filter(|role| role["role"] == "be")
        .collect::<Vec<_>>();
    anyhow::ensure!(
        backends.len() == CONCURRENT_WRITERS,
        "concurrent writer scenario requires three effective BE configs"
    );
    for backend in backends {
        require_values(backend, CONCURRENT_BACKEND_RESULT_EXPECTED)?;
    }
    let config = &frontend[0]["effective_config"];
    let integer = |path: &[&str]| -> Result<usize> {
        let mut value = config;
        for key in path {
            value = &value["value"][*key];
        }
        usize::try_from(
            value["value"]
                .as_i64()
                .with_context(|| format!("effective {} is not an integer", path.join(".")))?,
        )
        .with_context(|| format!("effective {} is not nonnegative", path.join(".")))
    };
    // The immutable prelaunch test-session plan nominates N=3 exact SQLs.
    // Require enough real positions without deriving an execution limit from
    // the Blocking worker count or changing the frozen three nominations.
    let workers = integer(&["runtime", "query_blocking_worker_threads"])?;
    let writers = integer(&["runtime", "frontend_workload", "internal_result_positions"])?;
    anyhow::ensure!(
        workers == 1 && writers >= CONCURRENT_WRITERS,
        "concurrent progress requires one actual Blocking worker and at least {CONCURRENT_WRITERS} Internal positions: positions={writers}, workers={workers}"
    );
    Ok(CONCURRENT_WRITERS)
}

/// Read external data only before admission or after EVERY original writer
/// actually exits. Readback can need the same warehouse query admission;
/// adding it while the three writers are held would contaminate this proof.
fn require_concurrent_source_data(control: &mut mysql::Conn) -> Result<()> {
    let source = format!("{CONCURRENT_CATALOG}.{CONCURRENT_DATABASE}.{CONCURRENT_SOURCE}");
    let rows = row_count(control, &source)?;
    let total: Option<i64> = control
        .query_first(format!("SELECT SUM(v) FROM {source}"))
        .context("read concurrent writer source value sum")?;
    anyhow::ensure!(
        rows == SEED_ROWS && total == Some(SEED_SUM),
        "concurrent source changed: rows={rows}, sum={total:?}; expected rows={SEED_ROWS}, sum={SEED_SUM}"
    );
    println!("concurrent-writer source rows={rows} sum={total:?}");
    Ok(())
}

fn require_concurrent_targets_unpublished(
    control: &mut mysql::Conn,
    snapshots_before: &[i64],
) -> Result<()> {
    for (ordinal, &before) in snapshots_before.iter().enumerate() {
        let target = concurrent_target(ordinal);
        let rows = row_count(control, &target)?;
        let snapshots = snapshot_count(control, &target)?;
        anyhow::ensure!(
            rows == 0 && snapshots == before,
            "concurrent target {ordinal} published unexpected data: rows={rows}, snapshots={before}->{snapshots}; expected empty target and no new snapshot"
        );
        println!("concurrent-writer target={ordinal} rows={rows} snapshots={before}->{snapshots}");
    }
    Ok(())
}

impl Scenario for DistributedWriterConcurrentProgress {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        let mut declaration = crate::scenarios::declarations::for_module(
            "distributed_writer",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        );
        declaration.impact.paths.extend([
            "novarocks/workload-control/**".into(),
            "novarocks/result-contract/**".into(),
            "novarocks/type-contract/**".into(),
            "novarocks-server/**".into(),
            "tests/system-test-runner/src/scenarios/writer_statement_actor.rs".into(),
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs".into(),
        ]);
        declaration
    }

    fn name(&self) -> &'static str {
        "connector/distributed-writer-concurrent-progress"
    }

    fn launch_config(&self, _root: &Path) -> Result<ScenarioLaunchConfig> {
        let capacity = CONCURRENT_CAPACITY_OVERLAY;
        let mut launch = connector_launch_config();
        launch.config_overlay.fe = Some(format!(
            "{}\n[runtime]\n{capacity}\n",
            launch.config_overlay.fe.take().unwrap_or_default()
        ));
        // connector_launch_config owns this existing runtime table and its
        // reader-cache settings. Insert within it instead of repeating a TOML
        // table or replacing the original backend configuration.
        let backend = launch
            .config_overlay
            .be
            .take()
            .context("original connector BE overlay")?;
        anyhow::ensure!(
            backend.matches("[runtime]\n").count() == 1,
            "original connector BE overlay must have one runtime table"
        );
        launch.config_overlay.be = Some(backend.replacen(
            "[runtime]\n",
            &format!("[runtime]\n{CONCURRENT_BACKEND_RESULT_OVERLAY}\n"),
            1,
        ));
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        anyhow::ensure!(
            cluster_size == CONCURRENT_WRITERS,
            "concurrent writer scenario requires native 1FE+3BE"
        );
        // The existing harness admits one selected token per (BE, fault kind).
        // Distinct BE targets allow all three original statements to stay held
        // concurrently without ending an earlier token or widening authority.
        let statements = (0..CONCURRENT_WRITERS)
            .map(|ordinal| StatementSpec {
                sql: concurrent_insert(ordinal),
                faults: vec![(ordinal, CONCURRENT_APPEND_HOLD)],
                observations: vec![],
            })
            .collect();
        Ok(Some(self.intent.freeze(
            statements,
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let concurrent_writers = require_concurrent_effective_config(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        seed_source_files(
            context,
            &mut control,
            CONCURRENT_CATALOG,
            CONCURRENT_DATABASE,
            CONCURRENT_SOURCE,
            "distributed-writer-concurrent-progress",
        )?;
        for ordinal in 0..concurrent_writers {
            control.query_drop(format!(
                "CREATE TABLE {} (v BIGINT)",
                concurrent_target(ordinal)
            ))?;
        }
        require_concurrent_source_data(&mut control)?;
        let snapshots_before = (0..concurrent_writers)
            .map(|ordinal| snapshot_count(&mut control, &concurrent_target(ordinal)))
            .collect::<Result<Vec<_>>>()?;
        require_concurrent_targets_unpublished(&mut control, &snapshots_before)?;
        control.finish()?;
        await_resource_convergence(context, &baseline, "concurrent writer fixture setup")?;

        // Reserve time from the original scenario deadline for cancellation,
        // actor joins and resource convergence; do not renew a timeout per SQL.
        let progress_budget = context
            .remaining("start concurrent held writers")?
            .checked_sub(Duration::from_secs(10))
            .context("concurrent writers have no time left for actual cleanup")?
            .min(Duration::from_secs(30));
        anyhow::ensure!(
            !progress_budget.is_zero(),
            "no concurrent writer progress budget remains"
        );
        let progress_deadline = Instant::now() + progress_budget;
        let mut writers = Vec::with_capacity(concurrent_writers);
        let progress = (|| -> Result<()> {
            // No required FE Blocking action is held here. A permanently
            // stuck shared worker would legitimately prevent this progress;
            // this scenario promises normal async concurrency, not isolation
            // from an operation that never actually exits.
            context.action("submit three independent INSERTs with one FE blocking worker");
            for ordinal in 0..concurrent_writers {
                let selected = self.intent.take(ordinal)?;
                let actor = WriterActor::start(
                    context,
                    &concurrent_insert(ordinal),
                    selected.clone(),
                    None,
                )?;
                writers.push(ConcurrentWriter {
                    ordinal,
                    selected,
                    actor,
                    early_outcome: None,
                });
            }
            await_concurrent_append_holds(context, &mut writers, progress_deadline)?;
            context.action("all three exact INSERTs reached append hold before any KILL");
            Ok(())
        })();

        // Do not return early after one cancellation or join fails: every actor
        // admitted above retains its original connection and requires settlement.
        let mut result = progress;
        let deadline = context.deadline();
        for writer in &writers {
            if writer.early_outcome.is_none() {
                let cancelled = context
                    .handle()
                    .kill_query_until(writer.actor.connection_id, deadline)
                    .with_context(|| {
                        format!("cancel original concurrent writer {}", writer.ordinal)
                    });
                result = writer_statement_actor::combine(result, cancelled);
            }
        }
        for writer in writers {
            let ordinal = writer.ordinal;
            let outcome =
                match writer.early_outcome {
                    Some(outcome) => Ok(outcome),
                    None => context
                        .remaining("receive original concurrent writer cancellation")
                        .and_then(|remaining| {
                            writer.actor.done.recv_timeout(remaining).with_context(|| {
                                format!("concurrent writer {ordinal} did not finish")
                            })
                        }),
                };
            let settled = writer.actor.settle(context, outcome).and_then(|outcome| {
                match outcome {
                    Err(mysql::Error::MySqlError(error))
                        if error.code == 1317 && error.state == "70100" => Ok(()),
                    other => bail!(
                        "concurrent writer {ordinal} must return its public KILL cancellation: {other:?}"
                    ),
                }
            });
            result = writer_statement_actor::combine(result, settled);
        }
        let faults = context.handle().clear_query_lifecycle_faults();
        result = writer_statement_actor::combine(result, faults);
        let convergence =
            await_resource_convergence(context, &baseline, "all original concurrent writers");
        result = writer_statement_actor::combine(result, convergence);

        // These are independent committed-data oracles, after real actor exit.
        // The BE resource oracle above covers contexts/fragments/catalog leases;
        // neither it nor empty tables proves unknown sealed objects disposed.
        let external_effects = mysql_actor::connect_until(&user, port, context.deadline())
            .and_then(|control| {
                control.run(|control| {
                    require_concurrent_targets_unpublished(control, &snapshots_before)?;
                    require_concurrent_source_data(control)
                })
            });
        result = writer_statement_actor::combine(result, external_effects);
        let readback_cleanup =
            await_resource_convergence(context, &baseline, "concurrent writer external readbacks");
        writer_statement_actor::combine(result, readback_cleanup)
    }
}

fn await_concurrent_append_holds(
    context: &mut ScenarioContext,
    writers: &mut [ConcurrentWriter],
    deadline: Instant,
) -> Result<()> {
    let mut hit = vec![false; writers.len()];
    loop {
        for writer in writers.iter_mut() {
            match writer.actor.done.try_recv() {
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Ok(outcome) => {
                    let detail = format!("{outcome:?}");
                    writer.early_outcome = Some(outcome);
                    bail!(
                        "concurrent writer {} became terminal before all three held: {detail}",
                        writer.ordinal
                    );
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    bail!(
                        "concurrent writer {} disconnected before all held",
                        writer.ordinal
                    );
                }
            }
            hit[writer.ordinal] = context.handle().selected_query_lifecycle_fault_hit(
                writer.ordinal,
                CONCURRENT_APPEND_HOLD,
                &writer.selected.token,
            )?;
        }
        if hit.iter().all(|value| *value) {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "three INSERTs did not independently reach append hold with one blocking worker; exact hits={hit:?}"
        );
        thread::sleep(
            Duration::from_millis(20).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

/// Proves the shape the design is actually about: writers on more than one
/// backend, more than one writer per backend once the plan runs above one
/// driver, exactly one root aggregating every fragment they produced, and a
/// single snapshot from a frontend that is the only thing that can commit.
struct DistributedWriterDataflow;

impl Scenario for DistributedWriterDataflow {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "distributed_writer",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/distributed-writer-dataflow"
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
        await_resource_convergence(context, &baseline, "distributed writer dataflow")?;
        Ok(())
    }
}

/// Native acceptance for the ordinary-aggregate statistics data plane.
///
/// This intentionally combines a full-table ANALYZE with a collect-on-write
/// INSERT in one fresh cluster. ANALYZE proves that an ordinary scan/aggregate
/// plan reaches every backend. The INSERT then proves that rows written by all
/// three backend processes contribute to one current-snapshot Theta artifact,
/// while exactly one root aggregates the prepared set and the frontend records
/// exactly one provider publication.
///
/// The Hadoop catalog's version hint is part of the external catalog format.
/// Requiring it to advance by one is the process-independent assertion that
/// data and SetStatistics were committed by one catalog transaction rather
/// than by a data commit followed by a statistics commit.
struct DistributedStatisticsDataflow;

impl Scenario for DistributedStatisticsDataflow {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "distributed_writer",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "statistics/native-aggregate-and-collect-on-write"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut config = connector_launch_config();
        let backend = config.config_overlay.be.get_or_insert_with(String::new);
        let runtime_cache = backend.find("[runtime.cache]").ok_or_else(|| {
            anyhow::anyhow!("connector backend overlay has no runtime cache table")
        })?;
        backend.insert_str(
            runtime_cache,
            &format!("exchange_max_transmit_batched_bytes = {STATISTICS_PACKET_BYTES}\n"),
        );
        Ok(config)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect native statistics control session")?,
        )?;

        const CATALOG: &str = "native_statistics";
        const DATABASE: &str = "native_statistics_db";
        const SOURCE: &str = "native_statistics_source";
        const TABLE: &str = "native_statistics_target";
        let warehouse = create_warehouse(context, "native-statistics-dataflow")?;
        let source = format!("{CATALOG}.{DATABASE}.{SOURCE}");
        let target = format!("{CATALOG}.{DATABASE}.{TABLE}");

        context.action("seed six source files for a three-backend statistics scan");
        create_catalog(&mut control, CATALOG, &warehouse)?;
        control
            .query_drop(format!("CREATE DATABASE {CATALOG}.{DATABASE}"))
            .context("create native statistics database")?;
        control
            .query_drop(format!(
                "CREATE TABLE {source} (v BIGINT) TBLPROPERTIES \
                 ('novarocks.statistics.collect-on-write' = 'false')"
            ))
            .context("create native statistics source")?;
        for file in 0..SEED_FILES {
            let low = file * SEED_ROWS_PER_FILE + 1;
            let high = (file + 1) * SEED_ROWS_PER_FILE;
            control
                .query_drop(format!(
                    "INSERT INTO {source} SELECT generate_series FROM \
                     TABLE(generate_series({low}, {high}))"
                ))
                .with_context(|| format!("seed native statistics rows {low}..{high}"))?;
        }

        let source_root = hadoop_table_root(&warehouse, DATABASE, SOURCE);
        let source_metadata_before = hadoop_metadata_version(&source_root)?;
        let source_snapshots_before = snapshot_count(&mut control, &source)?;
        let analyze_tasks_before = task_admission_counts(context)?;

        context.action("run binary ANALYZE across all three backend processes");
        control
            .query_drop(format!("ANALYZE TABLE {source}"))
            .context("run native distributed ANALYZE")?;
        wait_for_statistics_job_success(context, &mut control, CATALOG, DATABASE, SOURCE)?;

        let analyze_tasks_after = task_admission_counts(context)?;
        let analyze_task_delta = assert_every_backend_accepted_task(
            &analyze_tasks_before,
            &analyze_tasks_after,
            "native distributed ANALYZE",
        )?;
        let source_metadata_after = hadoop_metadata_version(&source_root)?;
        if source_metadata_after != source_metadata_before + 1 {
            bail!(
                "ANALYZE advanced Hadoop metadata from v{source_metadata_before} to \
                 v{source_metadata_after}; one statistics publication must create exactly one \
                 metadata version"
            );
        }
        let source_snapshots_after = snapshot_count(&mut control, &source)?;
        if source_snapshots_after != source_snapshots_before {
            bail!(
                "ANALYZE changed {source} snapshot count from {source_snapshots_before} to \
                 {source_snapshots_after}; statistics publication must attach to the existing \
                 snapshot"
            );
        }
        let analyze_ndv = assert_current_theta_metadata(
            &source_root,
            SEED_ROWS as f64,
            "native distributed ANALYZE",
        )?;
        assert_show_theta_statistics(
            &mut control,
            &source,
            "v",
            SEED_ROWS as f64,
            "native distributed ANALYZE",
        )?;

        control
            .query_drop(format!(
                "CREATE TABLE {target} (v BIGINT) TBLPROPERTIES \
                 ('novarocks.statistics.collect-on-write' = 'true')"
            ))
            .context("create collect-on-write target")?;
        let target_root = hadoop_table_root(&warehouse, DATABASE, TABLE);
        let metadata_before = hadoop_metadata_version(&target_root)?;
        let snapshots_before = snapshot_count(&mut control, &target)?;
        let before = all_write_counters(context)?;
        let ceiling = peak_ceiling(&before);
        let terminals_before = publication_terminals(context, WRITE_FAMILY)?;
        let tasks_before = task_admission_counts(context)?;

        context.action("collect statistics from the same pages written on all three backends");
        control
            .query_drop(format!("INSERT INTO {target} SELECT v FROM {source}"))
            .context("run native collect-on-write INSERT")?;

        let after = all_write_counters(context)?;
        let delta = write_delta(&before, &after);
        if delta.writing_backends.len() != 3 {
            bail!(
                "collect-on-write opened writers on {:?}; native 1FE+3BE acceptance requires \
                 all three backends",
                delta.writing_backends
            );
        }
        if (delta.rows - SEED_ROWS as f64).abs() > f64::EPSILON {
            bail!(
                "collect-on-write writers accepted {} rows; expected {SEED_ROWS}",
                delta.rows
            );
        }
        if delta.commit_fragments < 3.0 {
            bail!(
                "collect-on-write produced {} commit fragments across three writing backends",
                delta.commit_fragments
            );
        }
        let root = identify_root(
            &after,
            ceiling,
            delta.commit_fragments,
            "native collect-on-write",
        )?;
        if !delta.writing_backends.iter().any(|&index| index != root) {
            bail!(
                "collect-on-write root BE[{root}] was also the only writer; no writer result \
                 crossed a process boundary"
            );
        }
        let tasks_after = task_admission_counts(context)?;
        let write_task_delta = assert_every_backend_accepted_task(
            &tasks_before,
            &tasks_after,
            "native collect-on-write INSERT",
        )?;

        let terminals_after = publication_terminals(context, WRITE_FAMILY)?;
        assert_committed_once(terminals_before, terminals_after, "native collect-on-write")?;
        let snapshots_after = snapshot_count(&mut control, &target)?;
        if snapshots_after != snapshots_before + 1 {
            bail!(
                "collect-on-write moved {target} from {snapshots_before} to {snapshots_after} \
                 snapshots; data and statistics must publish as one snapshot"
            );
        }
        let metadata_after = hadoop_metadata_version(&target_root)?;
        if metadata_after != metadata_before + 1 {
            bail!(
                "collect-on-write advanced Hadoop metadata from v{metadata_before} to \
                 v{metadata_after}; data and SetStatistics must share one catalog commit"
            );
        }
        let write_ndv = assert_current_theta_metadata(
            &target_root,
            SEED_ROWS as f64,
            "native collect-on-write",
        )?;
        assert_show_theta_statistics(
            &mut control,
            &target,
            "v",
            SEED_ROWS as f64,
            "native collect-on-write",
        )?;
        let rows = row_count(&mut control, &target)?;
        if rows != SEED_ROWS {
            bail!("collect-on-write target contains {rows} rows; expected {SEED_ROWS}");
        }

        const WIDE_SOURCE: &str = "native_statistics_wide_source";
        const WIDE_TARGET: &str = "native_statistics_wide_target";
        let wide_source = format!("{CATALOG}.{DATABASE}.{WIDE_SOURCE}");
        let wide_target = format!("{CATALOG}.{DATABASE}.{WIDE_TARGET}");
        let wide_schema = (0..WIDE_STATISTICS_CHANNELS)
            .map(|index| format!("v{index} BIGINT"))
            .collect::<Vec<_>>()
            .join(", ");
        let wide_column_names = (0..WIDE_STATISTICS_CHANNELS)
            .map(|index| format!("v{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let wide_values = (0..WIDE_STATISTICS_CHANNELS)
            .map(|index| format!("generate_series + {}", index * 1_000_000))
            .collect::<Vec<_>>()
            .join(", ");

        context.action("seed a 1024-field source with twelve independently splittable files");
        control
            .query_drop(format!(
                "CREATE TABLE {wide_source} ({wide_schema}) TBLPROPERTIES \
                 ('novarocks.statistics.collect-on-write' = 'false')"
            ))
            .context("create wide native statistics source")?;
        for file in 0..WIDE_SEED_FILES {
            let low = file * WIDE_ROWS_PER_FILE + 1;
            let high = (file + 1) * WIDE_ROWS_PER_FILE;
            control
                .query_drop(format!(
                    "INSERT INTO {wide_source} SELECT {wide_values} FROM \
                     TABLE(generate_series({low}, {high}))"
                ))
                .with_context(|| format!("seed wide native statistics rows {low}..{high}"))?;
        }
        control
            .query_drop(format!(
                "CREATE TABLE {wide_target} ({wide_schema}) TBLPROPERTIES \
                 ('novarocks.statistics.collect-on-write' = 'true')"
            ))
            .context("create wide collect-on-write target")?;

        let wide_target_root = hadoop_table_root(&warehouse, DATABASE, WIDE_TARGET);
        let wide_metadata_before = hadoop_metadata_version(&wide_target_root)?;
        let wide_snapshots_before = snapshot_count(&mut control, &wide_target)?;
        let wide_write_before = all_write_counters(context)?;
        let wide_peak_ceiling = peak_ceiling(&wide_write_before);
        let wide_partial_before = all_writer_partial_counters(context)?;
        let wide_terminals_before = publication_terminals(context, WRITE_FAMILY)?;

        context.action(
            "collect 1024 writer aggregates through sparse multi-batch output on all backends",
        );
        control
            .query_drop(format!(
                "INSERT INTO {wide_target} SELECT {wide_column_names} FROM {wide_source}"
            ))
            .context("run wide native collect-on-write INSERT")?;

        let wide_write_after = all_write_counters(context)?;
        let wide_write_delta = write_delta(&wide_write_before, &wide_write_after);
        if wide_write_delta.writing_backends.len() != 3 {
            bail!(
                "wide collect-on-write opened writers on {:?}; expected all three backends",
                wide_write_delta.writing_backends
            );
        }
        if (wide_write_delta.rows - WIDE_ROWS as f64).abs() > f64::EPSILON {
            bail!(
                "wide collect-on-write writers accepted {} rows; expected {WIDE_ROWS}",
                wide_write_delta.rows
            );
        }
        let wide_root = identify_root(
            &wide_write_after,
            wide_peak_ceiling,
            wide_write_delta.commit_fragments,
            "wide native collect-on-write",
        )?;

        let wide_partial_after = all_writer_partial_counters(context)?;
        let wide_partial_delta = writer_partial_delta(&wide_partial_before, &wide_partial_after);
        assert_wide_writer_partials(&wide_write_delta.opens, &wide_partial_delta)?;

        let wide_terminals_after = publication_terminals(context, WRITE_FAMILY)?;
        assert_committed_once(
            wide_terminals_before,
            wide_terminals_after,
            "wide native collect-on-write",
        )?;
        let wide_snapshots_after = snapshot_count(&mut control, &wide_target)?;
        if wide_snapshots_after != wide_snapshots_before + 1 {
            bail!(
                "wide collect-on-write moved {wide_target} from {wide_snapshots_before} to \
                 {wide_snapshots_after} snapshots; expected one"
            );
        }
        let wide_metadata_after = hadoop_metadata_version(&wide_target_root)?;
        if wide_metadata_after != wide_metadata_before + 1 {
            bail!(
                "wide collect-on-write advanced Hadoop metadata from v{wide_metadata_before} to \
                 v{wide_metadata_after}; data and 1024 statistics artifacts must share one commit"
            );
        }
        assert_current_theta_metadata_count(
            &wide_target_root,
            WIDE_ROWS as f64,
            WIDE_STATISTICS_CHANNELS,
            "wide native collect-on-write",
        )?;
        let wide_rows = row_count(&mut control, &wide_target)?;
        if wide_rows != WIDE_ROWS {
            bail!("wide collect-on-write target contains {wide_rows} rows; expected {WIDE_ROWS}");
        }

        println!(
            "native-statistics-dataflow topology=1FE+3BE \
             analyze_task_delta={analyze_task_delta:?} analyze_ndv={analyze_ndv} \
             write_task_delta={write_task_delta:?} writing_backends={:?} root_be={} \
             commit_fragments={} write_ndv={} metadata_versions={}=>{} \
             wide_channels={} wide_root_be={} wide_partial_delta={wide_partial_delta:?}",
            delta.writing_backends,
            root,
            delta.commit_fragments,
            write_ndv,
            metadata_before,
            metadata_after,
            WIDE_STATISTICS_CHANNELS,
            wide_root,
        );

        await_resource_convergence(context, &baseline, "native statistics dataflow")?;
        Ok(())
    }
}

type TableStatisticsRow = (
    String,
    Option<String>,
    String,
    String,
    String,
    String,
    String,
);

/// Wait for the accepted ANALYZE job's business conclusion before observing
/// effects that belong to its background attempt. `ANALYZE` itself only proves
/// submission; a terminal `SUCCEEDED` record is the semantic precondition for
/// inspecting BE task admission and provider publication evidence.
fn wait_for_statistics_job_success(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<()> {
    context.action("wait for the submitted ANALYZE job to reach SUCCEEDED");
    loop {
        let rows: Vec<Row> = control
            .query("SHOW ANALYZE JOBS")
            .context("observe submitted native distributed ANALYZE job")?;
        let mut matches = Vec::new();
        for row in rows {
            if analyze_job_field(&row, "catalog")?.as_deref() == Some(catalog)
                && analyze_job_field(&row, "namespace")?.as_deref() == Some(namespace)
                && analyze_job_field(&row, "table")?.as_deref() == Some(table)
            {
                matches.push(row);
            }
        }
        let Some(job) = matches.pop() else {
            context.remaining("wait for submitted ANALYZE job to become observable")?;
            thread::sleep(Duration::from_millis(20));
            continue;
        };
        if !matches.is_empty() {
            bail!(
                "multiple ANALYZE jobs matched {catalog}.{namespace}.{table}; exact job identity is ambiguous"
            );
        }
        let job_id = analyze_job_field(&job, "job_id")?
            .filter(|value| !value.is_empty())
            .context("ANALYZE job observation has no job_id")?;
        let state = analyze_job_field(&job, "state")?
            .filter(|value| !value.is_empty())
            .context("ANALYZE job observation has no state")?;
        match state.as_str() {
            "SUCCEEDED" => return Ok(()),
            "SUBMITTED" | "PREPARING" | "COLLECTING" | "PUBLISHING" => {
                context.remaining(&format!(
                    "wait for ANALYZE job {job_id} to reach its business conclusion"
                ))?;
                thread::sleep(Duration::from_millis(20));
            }
            _ => {
                let detail = analyze_job_field(&job, "error_message")?.unwrap_or_default();
                bail!(
                    "ANALYZE job {job_id} for {catalog}.{namespace}.{table} reached {state}: {detail}"
                );
            }
        }
    }
}

fn analyze_job_field(row: &Row, name: &str) -> Result<Option<String>> {
    let index = row
        .columns_ref()
        .iter()
        .position(|column| column.name_str().eq_ignore_ascii_case(name))
        .with_context(|| format!("ANALYZE job observation omitted column {name}"))?;
    match row
        .as_ref(index)
        .context("ANALYZE job observation value is missing")?
    {
        Value::NULL => Ok(None),
        Value::Bytes(bytes) => Ok(Some(
            String::from_utf8(bytes.clone()).context("ANALYZE job observation is not UTF-8")?,
        )),
        Value::Int(value) => Ok(Some(value.to_string())),
        Value::UInt(value) => Ok(Some(value.to_string())),
        Value::Float(value) => Ok(Some(value.to_string())),
        Value::Double(value) => Ok(Some(value.to_string())),
        _ => bail!("ANALYZE job observation column {name} has an unsupported value type"),
    }
}

fn assert_show_theta_statistics(
    control: &mut mysql::Conn,
    table: &str,
    column: &str,
    expected_ndv: f64,
    label: &str,
) -> Result<()> {
    let rows: Vec<TableStatisticsRow> = control
        .query(format!("SHOW TABLE STATS {table}"))
        .with_context(|| format!("read {label} statistics through the provider"))?;
    let metric = format!("theta_ndv:{column}");
    let Some((_, value, status, basis_version, source, numeric_nature, basis_relation)) =
        rows.into_iter().find(|row| row.0 == metric)
    else {
        bail!("{label} did not expose {metric} through SHOW TABLE STATS");
    };
    let value = value
        .with_context(|| format!("{label} {metric} is AVAILABLE but has no scalar value"))?
        .parse::<f64>()
        .with_context(|| format!("parse {label} {metric} value"))?;
    assert_theta_estimate(value, expected_ndv, label)?;
    if status != "AVAILABLE"
        || basis_version != "SAME"
        || source != "PROVIDER_ARTIFACT"
        || numeric_nature != "APPROXIMATE"
        || basis_relation != "IDENTICAL"
    {
        bail!(
            "{label} {metric} has unexpected provider evidence: status={status:?} \
             basis_version={basis_version:?} source={source:?} \
             numeric_nature={numeric_nature:?} basis_relation={basis_relation:?}"
        );
    }
    Ok(())
}

fn assert_wide_writer_partials(
    writer_opens: &[f64],
    partials: &[WriterPartialCounters],
) -> Result<()> {
    if writer_opens.len() != 3 || partials.len() != 3 {
        bail!("wide writer partial evidence is not from exactly three backends");
    }
    for (index, (opens, partial)) in writer_opens.iter().zip(partials).enumerate() {
        if *opens <= 0.0 {
            bail!("wide collect-on-write opened no writer on BE[{index}]");
        }
        let expected_channels = *opens * WIDE_STATISTICS_CHANNELS as f64;
        if partial.non_null_channels != expected_channels {
            bail!(
                "BE[{index}] emitted {} non-null writer partial channels for {opens} writers; \
                 expected {expected_channels}",
                partial.non_null_channels
            );
        }
        if partial.batches != partial.rows || partial.rows <= *opens {
            bail!(
                "BE[{index}] writer partial delta is {partial:?}; the {opens} writers must each \
                 split their 1024 channels into more than one single-row batch"
            );
        }
        if partial.sparse_rows <= 0.0 || partial.sparse_rows > partial.rows {
            bail!(
                "BE[{index}] writer partial delta is {partial:?}; at least one row must be sparse \
                 under the {STATISTICS_PACKET_BYTES}-byte packet budget"
            );
        }
        if partial.bytes <= 0.0 {
            bail!("BE[{index}] emitted writer partial rows without encoded bytes");
        }
    }
    Ok(())
}

fn hadoop_table_root(warehouse: &Path, database: &str, table: &str) -> std::path::PathBuf {
    warehouse.join(database).join(table)
}

fn hadoop_metadata_version(table_root: &Path) -> Result<u32> {
    let hint = table_root.join("metadata/version-hint.text");
    std::fs::read_to_string(&hint)
        .with_context(|| format!("read Hadoop version hint {}", hint.display()))?
        .trim()
        .parse::<u32>()
        .with_context(|| format!("parse Hadoop version hint {}", hint.display()))
}

fn assert_current_theta_metadata(table_root: &Path, expected_ndv: f64, label: &str) -> Result<f64> {
    let values = assert_current_theta_metadata_count(table_root, expected_ndv, 1, label)?;
    Ok(values[0])
}

fn assert_current_theta_metadata_count(
    table_root: &Path,
    expected_ndv: f64,
    expected_blobs: usize,
    label: &str,
) -> Result<Vec<f64>> {
    let version = hadoop_metadata_version(table_root)?;
    let path = table_root
        .join("metadata")
        .join(format!("v{version}.metadata.json"));
    let bytes = std::fs::read(&path)
        .with_context(|| format!("read {label} metadata {}", path.display()))?;
    let metadata: TableMetadata = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode {label} Iceberg metadata {}", path.display()))?;
    let current = metadata
        .current_snapshot()
        .with_context(|| format!("{label} table has no current snapshot"))?;
    let statistics = metadata
        .statistics_iter()
        .filter(|statistics| statistics.snapshot_id == current.snapshot_id())
        .collect::<Vec<_>>();
    let [statistics] = statistics.as_slice() else {
        bail!(
            "{label} has {} StatisticsFiles for current snapshot {}; expected exactly one",
            statistics.len(),
            current.snapshot_id()
        );
    };
    let theta = statistics
        .blob_metadata
        .iter()
        .filter(|blob| blob.r#type == APACHE_DATASKETCHES_THETA_V1)
        .collect::<Vec<_>>();
    if theta.len() != expected_blobs {
        bail!(
            "{label} current StatisticsFile has {} Theta blobs; expected {expected_blobs}",
            theta.len(),
        );
    }
    let mut fields = BTreeSet::new();
    let mut values = Vec::with_capacity(theta.len());
    for blob in theta {
        if blob.snapshot_id != current.snapshot_id()
            || blob.sequence_number != current.sequence_number()
            || blob.fields.len() != 1
        {
            bail!(
                "{label} Theta provenance does not match the current snapshot: \
                 snapshot={} sequence={} fields={:?}, expected snapshot={} sequence={}",
                blob.snapshot_id,
                blob.sequence_number,
                blob.fields,
                current.snapshot_id(),
                current.sequence_number()
            );
        }
        if !fields.insert(blob.fields[0]) {
            bail!("{label} repeats Theta field identity {}", blob.fields[0]);
        }
        let ndv = blob
            .properties
            .get("ndv")
            .with_context(|| format!("{label} Theta blob has no ndv property"))?
            .parse::<f64>()
            .with_context(|| format!("parse {label} Theta ndv property"))?;
        assert_theta_estimate(ndv, expected_ndv, label)?;
        values.push(ndv);
    }
    let puffin_location = statistics
        .statistics_path
        .strip_prefix("file://")
        .ok_or_else(|| {
            anyhow!(
                "{label} expected a local file URI for Puffin verification, got {}",
                statistics.statistics_path
            )
        })?;
    let puffin_path = Path::new(puffin_location);
    if !puffin_path.is_file() || statistics.file_size_in_bytes <= 0 {
        bail!(
            "{label} published missing or empty Puffin file {} (metadata size={})",
            puffin_path.display(),
            statistics.file_size_in_bytes
        );
    }
    Ok(values)
}

fn assert_theta_estimate(actual: f64, expected: f64, label: &str) -> Result<()> {
    // The default nominal entries give a much tighter interval at this scale;
    // 15% deliberately avoids freezing one DataSketches sampling realization
    // while still rejecting the loss of any one of three balanced BE inputs.
    let tolerance = expected * 0.15;
    if (actual - expected).abs() > tolerance {
        bail!(
            "{label} Theta NDV {actual} is outside {expected} +/- {tolerance}; a current-snapshot \
             artifact must include the disjoint values written or scanned by every backend"
        );
    }
    Ok(())
}

fn task_admission_counts(context: &mut ScenarioContext) -> Result<Vec<f64>> {
    (0..context.handle().be_count())
        .map(|index| {
            context
                .handle()
                .backend_task_execution_tasks_created(index)
                .with_context(|| format!("read accepted EES tasks on BE[{index}]"))
        })
        .collect()
}

fn assert_every_backend_accepted_task(
    before: &[f64],
    after: &[f64],
    label: &str,
) -> Result<Vec<f64>> {
    if before.len() != 3 || after.len() != 3 {
        bail!("{label} task-admission evidence is not from exactly three backends");
    }
    let mut delta = Vec::with_capacity(before.len());
    for (index, (before, after)) in before.iter().zip(after).enumerate() {
        if after < before {
            bail!(
                "{label} BE[{index}] EES task-admission metric regressed from {before} to \
                 {after}; backend restart or metric-owner loss invalidates the evidence"
            );
        }
        delta.push(after - before);
    }
    if delta.contains(&0.0) {
        bail!(
            "{label} accepted-task deltas are {delta:?}; every backend in native 1FE+3BE \
             must accept at least one EES task from this statement"
        );
    }
    Ok(delta)
}

/// The same dataflow claims for a full-table overwrite.
///
/// An overwrite is the statement most likely to be special-cased into "delete
/// then append", so it gets its own end-to-end evidence that it distributes,
/// aggregates on one root, and publishes one snapshot.
struct DistributedWriterOverwrite;

impl Scenario for DistributedWriterOverwrite {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "distributed_writer",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/distributed-writer-overwrite"
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
#[derive(Default)]
struct DistributedWriterRowLevel {
    observations: SqlIntentOwner,
}

impl Scenario for DistributedWriterRowLevel {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "distributed_writer",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/distributed-writer-row-level"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.observations.freeze(
            row_level_contract::observation_specs(cluster_size),
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
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

        const CATALOG: &str = row_level_contract::CATALOG;
        const DATABASE: &str = row_level_contract::DATABASE;
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
        row_level_contract::assert_affected(&control, 3, "first deletion-vector DELETE")?;
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
        row_level_contract::assert_affected(&control, 2, "second deletion-vector DELETE")?;
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
        row_level_contract::assert_affected(&control, 3, "position-delete DELETE")?;
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
        row_level_contract::assert_affected(&control, 3, "copy-on-write UPDATE")?;
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
        row_level_contract::assert_affected(
            &control,
            2,
            "MERGE across the delete and data branches",
        )?;
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

        row_level_contract::run(context, &mut control, CATALOG, DATABASE, &self.observations)?;

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
/// It has no fault to arm. `TerminalOutcomeSuppress` was claimed only by the
/// retired protocol's backend query-lifecycle registry, and both that fault
/// kind and the outcome it suppressed are gone: `publish_task_round_convergence`
/// publishes an empty participant-outcome list because the task protocol's
/// per-domain receipts and termination latch replaced that funnel (ADR-0135).
///
/// And its property is one the task protocol deliberately inverts. A lost
/// terminal acknowledgement is *recovered* here, by replaying the exact
/// request -- which is precisely what
/// `tests/sql/correctness/distributed-resilience/sql/task-update-ack-loss.sql`
/// asserts. "Suppress the terminal and the write must fail" would therefore
/// assert the opposite of the design.
///
/// The state it described also cannot reach the commit barrier any more. In
/// `novarocks/frontend-application/src/coordinator/execution.rs` the task round's drain
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
    /// One writer rejects the ordinary partial aggregate update for its input
    /// page, after writer capacity was reserved but before either side accepts
    /// the page.
    PartialAggregateUpdate,
    /// One writer rejects ordinary partial aggregate finalization at EOS.
    PartialAggregateFinalize,
    /// The single root rejects merging a writer partial into its ordinary
    /// final aggregate.
    FinalAggregateMerge,
    /// The single root rejects ordinary final aggregate finalization at EOS.
    FinalAggregateFinalize,
    /// One backend's participant task is failed after it published RUNNING.
    /// Its writers were already opened while the task was prepared, but the
    /// drivers never run, so the stream it owes the root aggregation never
    /// carries a row and never reaches end-of-stream: the gather cannot close.
    SeveredWriterStream,
    /// The frontend's attempt is aborted while its tasks are running, so it
    /// never fetches the root's complete prepared write set.
    FetchAbort,
    /// The session's own deadline fires after every backend has entered a live
    /// writer append. Cancellation must abort those provider writers and must
    /// not publish a successful Root EOF.
    DeadlineAbort,
    /// All writer inputs reached Root EOS, but its first real Data never leaves BE.
    SealedBeforeRootData,
    /// FE consumed the real first Root segment, but receives no second Data/End.
    SealedAfterRootPrefix,
}

/// The task protocol's evidence that one task really failed on the backend the
/// fault was armed on.
///
/// It replaces `NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED`, which was
/// emitted only by the retired protocol's Stage/Start fragment ingress: the
/// task protocol runs its fragments from `NativeTaskExecutionHost`, and that
/// ingress no longer exists.
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
    /// Incomplete delivery cuts own exclusive targets; the recovery matrix
    /// and its healthy write share the base target.
    const fn target_table(&self) -> &'static str {
        match self {
            Self::SealedBeforeRootData => "distributed_writer_fault_before_root_data",
            Self::SealedAfterRootPrefix => "distributed_writer_fault_after_root_prefix",
            _ => "distributed_writer_fault_data",
        }
    }

    /// How many times this fault has left evidence in a backend process.
    ///
    /// A fault that is nominated but never hit by its exact owner is inert, and an
    /// inert fault reads exactly like a fault that did not reproduce: the
    /// statement fails for some unrelated reason and the scenario still passes.
    /// Every case therefore has to show that its own injection fired.
    fn injection_marker_count(
        &self,
        context: &mut ScenarioContext,
        selected: &SelectedStatement,
    ) -> Result<usize> {
        match self.injection_evidence() {
            InjectionEvidence::NativeFault => {
                selected.faults.iter().try_fold(0, |total, &(index, kind)| {
                    Ok(total
                        + usize::from(context.handle().selected_query_lifecycle_fault_hit(
                            index,
                            kind,
                            &selected.token,
                        )?))
                })
            }
            InjectionEvidence::EveryBackend(marker) => backend_marker_total(context, marker),
            InjectionEvidence::AppendHolds => Ok(all_write_counters(context)?
                .iter()
                .map(|counters| counters.append_holds as usize)
                .sum()),
        }
    }

    /// Where this case's own injection leaves its evidence.
    const fn injection_evidence(&self) -> InjectionEvidence {
        match self {
            Self::WriterEgress
            | Self::RootValidation
            | Self::PartialAggregateUpdate
            | Self::PartialAggregateFinalize
            | Self::FinalAggregateMerge
            | Self::FinalAggregateFinalize
            | Self::SealedBeforeRootData
            | Self::SealedAfterRootPrefix => InjectionEvidence::NativeFault,
            Self::SeveredWriterStream => {
                // The exact held Task is checked before reentry below; its
                // backend is selected from the original frozen writer graph.
                InjectionEvidence::EveryBackend(TASK_EXECUTION_FAILURE_INJECTED)
            }
            // Summed across the cluster rather than pinned to one backend: an
            // abort reaches the backends the scheduler placed this attempt on,
            // which is a property of the plan and of the splits. What the case
            // needs is that the abort was delivered somewhere at all, because
            // the alternative -- a client error with no abort behind it -- is
            // exactly what a timeout looks like.
            Self::FetchAbort => InjectionEvidence::EveryBackend(TASK_CONTEXT_ABORT_APPLIED),
            Self::DeadlineAbort => InjectionEvidence::AppendHolds,
        }
    }

    fn originating_connector_fact(&self) -> Option<FailureFact> {
        let (class, code) = match self {
            Self::RootValidation => (FailureClass::Data, 9),
            Self::WriterEgress
            | Self::PartialAggregateUpdate
            | Self::PartialAggregateFinalize
            | Self::FinalAggregateMerge
            | Self::FinalAggregateFinalize => (FailureClass::Unexpected, 10),
            Self::SeveredWriterStream
            | Self::FetchAbort
            | Self::DeadlineAbort
            | Self::SealedBeforeRootData
            | Self::SealedAfterRootPrefix => return None,
        };
        Some(FailureFact::new(
            class,
            FailureReason::new(domain::CONNECTOR, std::num::NonZeroU16::new(code).unwrap()),
        ))
    }

    /// Which of the scenario's two write statements this case runs.
    const fn statement<'a>(&self, statements: &WriteStatements<'a>) -> &'a str {
        match self {
            Self::FetchAbort => statements.delayed,
            Self::WriterEgress
            | Self::RootValidation
            | Self::PartialAggregateUpdate
            | Self::PartialAggregateFinalize
            | Self::FinalAggregateMerge
            | Self::FinalAggregateFinalize
            | Self::SeveredWriterStream
            | Self::DeadlineAbort
            | Self::SealedBeforeRootData
            | Self::SealedAfterRootPrefix => statements.full,
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
            Self::DeadlineAbort => {
                if delta.append_holds.len() != 3
                    || delta.append_holds.iter().any(|holds| *holds != 1.0)
                {
                    bail!(
                        "{case}: the query-scoped append hold must fire exactly once on each BE; \
                         holds={:?}",
                        delta.append_holds
                    );
                }
                for (index, (opens, holds)) in
                    delta.opens.iter().zip(&delta.append_holds).enumerate()
                {
                    if *opens < *holds {
                        bail!(
                            "{case}: BE[{index}] fired {holds} append hold(s) after opening only \
                             {opens} writer(s)"
                        );
                    }
                }
                let held = delta.append_holds.iter().sum::<f64>();
                if delta.abort_succeeded < held {
                    bail!(
                        "{case}: {held} live provider writers were held but only {} provider abort \
                         calls completed successfully",
                        delta.abort_succeeded
                    );
                }
                if delta.abort_failed != 0.0 {
                    bail!(
                        "{case}: {} provider writer abort call(s) failed",
                        delta.abort_failed
                    );
                }
                if delta.result_finished != 0.0 {
                    bail!(
                        "{case}: the native Root result owner published {} successful EOF \
                         terminal(s) after the deadline",
                        delta.result_finished
                    );
                }
                if delta.result_aborted != 1.0 {
                    bail!(
                        "{case}: the native Root result owner published {} abort terminal(s); \
                         expected exactly one",
                        delta.result_aborted
                    );
                }
                Ok(())
            }
            Self::WriterEgress
            | Self::RootValidation
            | Self::PartialAggregateUpdate
            | Self::PartialAggregateFinalize
            | Self::FinalAggregateMerge
            | Self::FinalAggregateFinalize
            | Self::FetchAbort
            | Self::SealedBeforeRootData
            | Self::SealedAfterRootPrefix => Ok(()),
        }
    }

    const fn case(&self) -> &'static str {
        match self {
            Self::WriterEgress => "a writer that fails at commit-fragment egress",
            Self::RootValidation => "a root that rejects a commit-fragment carrier",
            Self::PartialAggregateUpdate => "a writer partial aggregate that rejects input",
            Self::PartialAggregateFinalize => "a writer partial aggregate that rejects EOS",
            Self::FinalAggregateMerge => "a root final aggregate that rejects a partial",
            Self::FinalAggregateFinalize => "a root final aggregate that rejects EOS",
            Self::SeveredWriterStream => "a writer stream that never reaches the root aggregation",
            Self::FetchAbort => "an aborted attempt that never fetches the root's result",
            Self::DeadlineAbort => "a deadline that fires after provider writers enter append",
            Self::SealedBeforeRootData => "sealed objects before the first Root Data is offered",
            Self::SealedAfterRootPrefix => {
                "sealed objects after an accepted Root prefix without End"
            }
        }
    }
}

/// Where one case's injection leaves the evidence that it actually fired.
enum InjectionEvidence {
    /// Actual owner hit for this selected SQL, separately from nomination.
    NativeFault,
    /// Anywhere across the backends, summed, because which backends the
    /// marker reaches is a property of the plan rather than of the cluster.
    EveryBackend(&'static str),
    /// The provider-owned append-hold counter across every backend.
    AppendHolds,
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
#[derive(Default)]
struct DistributedWriterFaults {
    intent: SqlIntentOwner,
}

impl Scenario for DistributedWriterFaults {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "distributed_writer",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/distributed-writer-faults"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let selected = WRITE_FAULTS
            .iter()
            .position(|fault| matches!(fault, WriteFault::SeveredWriterStream))
            .context("writer fault matrix has no remote sender case")?;
        Ok(Some(self.intent.freeze_remote_writer_sender(
            writer_fault_specs(cluster_size),
            selected,
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;

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
                "CREATE TABLE {CATALOG}.{DATABASE}.{TABLE} (v BIGINT) \
                 TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'true')"
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
        let mut retained_objects = Vec::new();
        for (ordinal, fault) in WRITE_FAULTS.into_iter().enumerate() {
            let case = fault.case();
            let selected = self.intent.take(ordinal)?;
            let case_target = format!("{CATALOG}.{DATABASE}.{}", fault.target_table());
            let sealed_cut = matches!(
                fault,
                WriteFault::SealedBeforeRootData | WriteFault::SealedAfterRootPrefix
            );
            let case_snapshots_before = if sealed_cut {
                // This owned table has exactly one future writer, the frozen
                // original INSERT below. No old case can contribute a file.
                control
                    .query_drop(format!(
                        "CREATE TABLE {case_target} (v BIGINT) \
                     TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'true')"
                    ))
                    .context("create the original isolated sealed-object target")?;
                let table_root = hadoop_table_root(
                    &context
                        .runtime_dir()
                        .join("warehouses/distributed-writer-faults"),
                    DATABASE,
                    fault.target_table(),
                );
                anyhow::ensure!(
                    owned_root_parquet_paths(context, &table_root)?.is_empty(),
                    "fresh sealed-object target already contains data files"
                );
                snapshot_count(&mut control, &case_target)?
            } else {
                snapshots_before
            };
            let case_insert = if sealed_cut {
                format!("INSERT INTO {case_target} SELECT v FROM {CATALOG}.{DATABASE}.{SOURCE}")
            } else {
                insert.clone()
            };
            let case_statements = WriteStatements {
                full: &case_insert,
                delayed: statements.delayed,
            };
            context.action(format!("inject {case} and require no snapshot"));
            // Select the original finite grants before reading their hit baseline.
            // The SQL execution remains unbound until selected.arm below.
            for &(index, kind) in &selected.faults {
                context.handle().arm_selected_query_lifecycle_fault(
                    index,
                    kind,
                    &selected.token,
                )?;
            }
            let before_terminals = publication_terminals(context, WRITE_FAMILY)?;
            let before = all_write_counters(context)?;
            let before_injections = fault.injection_marker_count(context, &selected)?;

            let (outcome, accepted, original_window) = run_faulted_write(
                context,
                &fault,
                &user,
                port,
                &case_statements,
                selected.clone(),
                capture_original_write_window,
            )?;
            if let Some(expected) = fault.originating_connector_fact() {
                assert_connector_failure(context, &outcome, accepted.as_ref(), expected, case)?;
            }
            if matches!(
                fault,
                WriteFault::SealedBeforeRootData | WriteFault::SealedAfterRootPrefix
            ) {
                assert_sealed_root_read_failure(
                    context,
                    &fault,
                    &outcome,
                    accepted.as_ref(),
                    &selected,
                )?;
                let table_root = hadoop_table_root(
                    &context
                        .runtime_dir()
                        .join("warehouses/distributed-writer-faults"),
                    DATABASE,
                    fault.target_table(),
                );
                // The original statement's binding has drained and its worker
                // contexts exited. Only its fresh owned target's actual data
                // location is observed; other cases cannot contribute files.
                retained_objects.push(capture_sealed_root_objects(context, &table_root)?);
            }
            let Err(error) = outcome else {
                bail!("{case} did not fail the write");
            };
            if matches!(&fault, WriteFault::DeadlineAbort) {
                let mysql::Error::MySqlError(error) = &error else {
                    bail!(
                        "{case} did not return its complete public SQL deadline error: {error:?}"
                    );
                };
                anyhow::ensure!(
                    error.code == 1317
                        && error.state == "70100"
                        && exact_uncommitted_write_publication_message(
                            &error.message,
                            "[query.execution.deadline_exceeded] logical execution deadline expired before success EOF",
                        ),
                    "{case} lost its original deadline descriptor, errno, SQLSTATE or safe detail: {error:?}"
                );
            }

            // SELECT 1 reentry has now completed independently. Its successful
            // Root is outside this captured original-writer observation.
            let after_terminals = original_window.publication;
            let delta = write_delta(&before, &original_window.counters);
            let commits = assert_commit_never_invoked(before_terminals, after_terminals, case)?;
            println!(
                "distributed-writer-faults case={case:?} commit_invocations={commits} \
                 writer_opens_delta={:?} commit_fragments_delta={} append_holds_delta={:?} \
                 provider_abort_succeeded_delta={} provider_abort_failed_delta={} \
                 root_result_finished_delta={} root_result_aborted_delta={}",
                delta.opens,
                delta.commit_fragments,
                delta.append_holds,
                delta.abort_succeeded,
                delta.abort_failed,
                delta.result_finished,
                delta.result_aborted
            );
            let after_injections = fault.injection_marker_count(context, &selected)?;
            if after_injections <= before_injections {
                bail!(
                    "{case} produced no fault-injection evidence on its target backend \
                     ({before_injections} -> {after_injections}); the write may have failed for an \
                     unrelated reason"
                );
            }
            if !selected.faults.is_empty() {
                let hits = selected
                    .faults
                    .iter()
                    .try_fold(0usize, |total, &(index, kind)| {
                        Ok::<_, anyhow::Error>(
                            total
                                + usize::from(
                                    context.handle().selected_query_lifecycle_fault_hit(
                                        index,
                                        kind,
                                        &selected.token,
                                    )?,
                                ),
                        )
                    })?;
                if hits == 0 {
                    bail!("{case} left no actual fault hit for its selected statement");
                }
            }
            fault.assert_case_evidence(&delta, case)?;

            let rows = row_count(&mut control, &case_target)?;
            if rows != 0 {
                bail!("{case} left {rows} committed rows; expected none");
            }
            let snapshots = snapshot_count(&mut control, &case_target)?;
            if snapshots != case_snapshots_before {
                bail!(
                    "{case} moved {case_target} from {case_snapshots_before} to {snapshots} snapshots; a \
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

        await_resource_convergence(context, &baseline, "distributed writer faults")?;
        for objects in retained_objects {
            for object in objects {
                context.remaining("recheck the original sealed objects after native close")?;
                anyhow::ensure!(
                    sealed_object_digest(&object.path)? == (object.bytes, object.sha256),
                    "sealed output disappeared or changed after original abort/Task/context retirement: {}",
                    object.path.display()
                );
            }
        }
        Ok(())
    }
}

/// Exact original test controls, owned after the writer so unwind releases
/// every parked Create before joining the client. This grants no new action.
struct SeveredWriterRendezvous<'a> {
    context: &'a mut ScenarioContext,
    listener: Option<crate::scenarios::native_create_hold::HoldListener>,
    held: Vec<crate::scenarios::native_create_hold::HeldCreate>,
    token: String,
    hold_backends: Vec<usize>,
    closed: bool,
}
impl SeveredWriterRendezvous<'_> {
    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        let mut released = Ok(());
        for create in &mut self.held {
            released = writer_statement_actor::combine(released, create.release());
        }
        // End only the hold, keeping this query's task failure active. A
        // replay or late connection cannot park after the observed releases.
        let mut ended = Ok(());
        for &backend in &self.hold_backends {
            let result = match self
                .context
                .handle()
                .armed_query_lifecycle_fault_token(backend, "create-task-before-worker-hold")
            {
                Ok(Some(token)) if token == self.token => {
                    self.context.handle().end_selected_query_lifecycle_fault(
                        backend,
                        "create-task-before-worker-hold",
                        &self.token,
                    )
                }
                Ok(None) => Ok(()),
                Ok(Some(_)) => Err(anyhow::anyhow!(
                    "severed writer cleanup encountered a foreign hold token"
                )),
                Err(error) => Err(error),
            };
            ended = writer_statement_actor::combine(ended, result);
        }
        self.closed = ended.is_ok();
        drop(self.listener.take());
        writer_statement_actor::combine(released, ended)
    }
}
impl Drop for SeveredWriterRendezvous<'_> {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            eprintln!("severed writer rendezvous cleanup failed: {error:#}");
        }
    }
}

/// A closed observation interval for one writer, before any reentry SQL.
struct OriginalWriteWindow {
    publication: PublicationTerminals,
    counters: Vec<WriteCounters>,
}
fn capture_original_write_window(context: &mut ScenarioContext) -> Result<OriginalWriteWindow> {
    Ok(OriginalWriteWindow {
        publication: publication_terminals(context, WRITE_FAMILY)?,
        counters: all_write_counters(context)?,
    })
}

/// Observe the original SQL while its same connection remains idle. Regardless
/// of observation failure, settle releases the single gate and joins the actor.
fn settle_observed_writer(
    write: WriterActor,
    context: &mut ScenarioContext,
    primary: Result<mysql::Result<()>>,
    before_reentry: impl FnOnce(&mut ScenarioContext) -> Result<OriginalWriteWindow>,
) -> Result<(
    mysql::Result<()>,
    Option<QueryLifecycleStructuredSnapshot>,
    OriginalWriteWindow,
)> {
    let (window, observed) = if primary.is_ok() {
        match before_reentry(context) {
            Ok(window) => (Some(window), Ok(())),
            Err(error) => (None, Err(error)),
        }
    } else {
        (None, Ok(()))
    };
    let primary = retain_write_result(primary, observed);
    write.settle(context, primary).map(|outcome| {
        (
            outcome,
            None,
            window.expect("a successful observation precedes reentry"),
        )
    })
}

/// Run only the selected write after its actual idle connection is bound.
fn run_faulted_write(
    context: &mut ScenarioContext,
    fault: &WriteFault,
    user: &str,
    port: u16,
    statements: &WriteStatements<'_>,
    selected: SelectedStatement,
    before_reentry: impl FnOnce(&mut ScenarioContext) -> Result<OriginalWriteWindow>,
) -> Result<(
    Result<(), mysql::Error>,
    Option<QueryLifecycleStructuredSnapshot>,
    OriginalWriteWindow,
)> {
    let insert = fault.statement(statements);
    match fault {
        WriteFault::SeveredWriterStream => {
            let baseline = all_write_counters(context)?;
            let required_healthy = baseline
                .len()
                .checked_sub(1)
                .filter(|count| *count >= 2)
                .context("severed writer fixture needs its original three or more backends")?;
            let listener = crate::scenarios::native_create_hold::HoldListener::bind(
                &selected.token,
                context.process_ids().backends.len(),
            )?;
            let write = WriterActor::start(context, insert, selected.clone(), None)?;
            // Move the already-bound listener behind the actual writer in
            // declaration order. This guard releases accepted/unobserved and
            // future holds before WriterActor can cancel/join during unwind.
            let mut gate = SeveredWriterRendezvous {
                context,
                listener: Some(listener),
                held: Vec::new(),
                token: selected.token.clone(),
                hold_backends: selected
                    .faults
                    .iter()
                    .filter_map(|&(backend, kind)| {
                        (kind == "create-task-before-worker-hold").then_some(backend)
                    })
                    .collect(),
                closed: false,
            };
            let primary = (|| {
                loop {
                    while let Some(create) = gate
                        .listener
                        .as_ref()
                        .unwrap()
                        .try_receive(&selected.token, gate.context.deadline())?
                    {
                        anyhow::ensure!(
                            create.outbound_edges > 0
                                && create.process
                                    == gate
                                        .context
                                        .handle()
                                        .backend_process_id(create.backend)?
                                        .to_string()
                                && create.execution.ends_with(":1")
                                && gate.held.iter().all(|old| old.execution == create.execution
                                    && old.stage == create.stage
                                    && old.task == create.task
                                    && old.backend == create.backend
                                    && old.process == create.process),
                            "severed writer hold is not its exact original remote sender: {}",
                            create.payload
                        );
                        gate.held.push(create);
                    }
                    let current = all_write_counters(gate.context)?;
                    let delta = write_delta(&baseline, &current);
                    let healthy = gate.held.first().map_or(0, |sender| {
                        delta
                            .writing_backends
                            .iter()
                            .filter(|index| **index != sender.backend)
                            .count()
                    });
                    if !gate.held.is_empty() && healthy >= required_healthy {
                        gate.context.action(format!("{healthy} healthy writer backends opened before releasing original sender {}", gate.held[0].identity_marker()));
                        break;
                    }
                    match write.done.try_recv() {
                        Err(std::sync::mpsc::TryRecvError::Empty) => {}
                        Ok(outcome) => bail!(
                            "severed writer became terminal before its live-sender fixture existed: {outcome:?}"
                        ),
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            bail!("severed writer actor disconnected before its held sender")
                        }
                    }
                    thread::sleep(
                        gate.context
                            .remaining(
                                "observe original held sender and two healthy writer backends",
                            )?
                            .min(Duration::from_millis(20)),
                    );
                }
                gate.close()?;
                let result = write
                    .done
                    .recv_timeout(gate.context.remaining("await the severed writer result")?)
                    .context("severed writer session did not finish")?;
                let sender = gate
                    .held
                    .first()
                    .context("original remote sender was not held")?;
                let exact = format!(
                    "{TASK_EXECUTION_FAILURE_INJECTED} {} backend_index={} token={}",
                    sender.identity_marker(),
                    sender.backend,
                    selected.token
                );
                anyhow::ensure!(
                    gate.context.handle().be_log_count(sender.backend, &exact)? == 1,
                    "selected remote writer Task did not supply its exact once failure: {exact}"
                );
                let execution = format!(
                    "{TASK_EXECUTION_FAILURE_INJECTED} execution_id={} ",
                    sender.execution
                );
                anyhow::ensure!(
                    backend_marker_total(gate.context, &execution)? == 1,
                    "another Task of the same original execution also received the selected failure"
                );
                Ok(result)
            })();
            let cleanup = gate.close();
            drop(gate);
            settle_observed_writer(
                write,
                context,
                retain_write_result(primary, cleanup),
                before_reentry,
            )
        }
        WriteFault::FetchAbort => {
            let created_baseline = backend_marker_total(context, TASK_CREATE_APPLIED)?;
            let write = WriterActor::start(context, insert, selected, None)?;
            let primary = (|| {
                await_backend_marker_above(
                    context,
                    TASK_CREATE_APPLIED,
                    created_baseline,
                    "a task admitted for the write this case aborts",
                )?;
                let deadline = context.deadline();
                context
                    .handle()
                    .kill_query_until(write.connection_id, deadline)
                    .context("abort the in-flight distributed write")?;
                write
                    .done
                    .recv_timeout(context.remaining("await the aborted write result")?)
                    .context("aborted write session did not finish")
            })();
            settle_observed_writer(write, context, primary, before_reentry)
        }
        WriteFault::DeadlineAbort => {
            const QUERY_TIMEOUT_SECS: u64 = 10;
            let writer_baseline = all_write_counters(context)?;
            let write = WriterActor::start(context, insert, selected, Some(QUERY_TIMEOUT_SECS))?;
            let primary = (|| {
                await_append_holds(context, &writer_baseline)?;
                match write.done.try_recv() {
                    Err(std::sync::mpsc::TryRecvError::Empty) => {}
                    Ok(outcome) => bail!(
                        "deadline write became terminal before its three live append holds: {outcome:?}"
                    ),
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        bail!("deadline write actor disconnected before its own deadline")
                    }
                }
                let outcome = write
                    .done
                    .recv_timeout(context.remaining("await the deadline write result")?)
                    .context("deadline write session did not finish")?;
                await_deadline_abort_terminals(context, &writer_baseline)?;
                Ok(outcome)
            })();
            settle_observed_writer(write, context, primary, before_reentry)
        }
        _ => {
            let deadline = context.deadline();
            let mut connection = mysql_actor::connect_until(user, port, deadline)?;
            let observation = match selected.arm(context, connection.connection_id(), insert) {
                Ok(observation) => observation,
                Err(primary) => {
                    return writer_statement_actor::combine(Err(primary), connection.finish());
                }
            };
            let outcome = connection.query_drop(insert);
            let mut accepted = None;
            let terminal = match &outcome {
                Ok(()) => context
                    .handle()
                    .await_query_lifecycle_statement_observation(&observation, true, deadline)
                    .map(|_| ()),
                Err(mysql::Error::MySqlError(_)) => context
                    .handle()
                    .await_query_lifecycle_statement_observation(&observation, false, deadline)
                    .map(|snapshot| accepted = Some(snapshot)),
                Err(_) => Ok(()),
            };
            // Keep the original connection idle until its writer-only facts
            // are frozen. Do not subtract an assumed Root for later SELECT 1.
            let original_window = terminal.and_then(|_| before_reentry(context));
            let reentry = if matches!(&outcome, Err(mysql::Error::MySqlError(_))) {
                writer_statement_actor::require_public_error_reentry(&mut connection)
            } else {
                Ok(())
            };
            let drain = if outcome.is_ok() {
                context
                    .handle()
                    .drain_query_lifecycle_statement_observation(&observation)
            } else {
                context
                    .handle()
                    .drain_query_lifecycle_statement_binding(&observation, deadline)
            };
            let (original_window, observed) = match original_window {
                Ok(window) => (Some(window), Ok(())),
                Err(error) => (None, Err(error)),
            };
            let primary = match outcome {
                Err(error) if !matches!(&error, mysql::Error::MySqlError(_)) => Err(error.into()),
                outcome => Ok(outcome),
            };
            let cleanup = writer_statement_actor::combine(
                observed,
                writer_statement_actor::combine(reentry, drain),
            );
            retain_write_result(retain_write_result(primary, cleanup), connection.finish()).map(
                |outcome| {
                    (
                        outcome,
                        accepted,
                        original_window.expect("a successful observation precedes reentry"),
                    )
                },
            )
        }
    }
}

/// Both actions are ordinary original Root read failures, not Task failures.
/// Their exact role hit is separately required by the matrix below.
fn assert_sealed_root_read_failure(
    context: &mut ScenarioContext,
    fault: &WriteFault,
    outcome: &mysql::Result<()>,
    snapshot: Option<&QueryLifecycleStructuredSnapshot>,
    selected: &SelectedStatement,
) -> Result<()> {
    let (detail, action) = match fault {
        WriteFault::SealedBeforeRootData => (
            "injected write Root read failure before first Data",
            "connector-write-root-read-before-data-failure",
        ),
        WriteFault::SealedAfterRootPrefix => (
            "injected write Root read failure after accepted prefix",
            "connector-write-root-read-after-prefix-failure",
        ),
        _ => bail!("object oracle requires its original Root read action"),
    };
    // The native convergence snapshot observes only the Task protocol's
    // terminal, not the logical actor's independent Root read failure. Check
    // that failure at its actual public boundary before inspecting the snapshot.
    let Err(mysql::Error::MySqlError(error)) = outcome else {
        bail!("Root read action did not return its original SQL error: {outcome:?}");
    };
    anyhow::ensure!(
        error.code == 1105
            && error.state == "HY000"
            && exact_uncommitted_write_publication_message(
                &error.message,
                &format!("[query.execution.unexpected] {detail}")
            ),
        "Root read failure lost its exact public error/KnownUncommitted publication: {error:?}"
    );
    let snapshot =
        snapshot.context("Root read action has no original Native convergence snapshot")?;
    let execution = novarocks_types::QueryExecutionId::new(
        novarocks_types::QueryId::new(
            i64::from_ne_bytes(snapshot.process_namespace.to_ne_bytes()),
            i64::from_ne_bytes(snapshot.local_sequence.to_ne_bytes()),
        ),
        novarocks_types::AttemptId::new(snapshot.attempt_id)
            .map_err(|_| anyhow!("original statement has an invalid attempt identity"))?,
    )
    .map_err(|_| anyhow!("original statement has an invalid execution identity"))?;
    anyhow::ensure!(
        snapshot.attempt_id == 1
            && snapshot.worker_contexts_converged
            && snapshot.execution_id.as_deref()
                == Some(
                    format!(
                        "{}:{}:{}",
                        execution.query_id().high(),
                        execution.query_id().low(),
                        execution.attempt_id().get()
                    )
                    .as_str()
                ),
        "Root read action changed its original execution or physical context exit: {snapshot:?}"
    );
    // Only the actual Root reader can hit either action. The existing owner
    // validates its immutable binding against this statement's original grant,
    // backend process and launch; do not invent a Task site from a read refusal.
    let mut hits = 0;
    for &(index, kind) in &selected.faults {
        anyhow::ensure!(
            kind == action,
            "sealed Root read has a foreign finite action"
        );
        hits += usize::from(context.handle().selected_query_lifecycle_fault_hit(
            index,
            kind,
            &selected.token,
        )?);
    }
    anyhow::ensure!(
        hits == 1,
        "sealed Root read requires its one actual Root-role hit, got {hits}"
    );
    Ok(())
}

struct SealedRootObject {
    path: std::path::PathBuf,
    bytes: u64,
    sha256: [u8; 32],
}

impl SealedRootObject {
    fn observe(path: std::path::PathBuf) -> Result<Self> {
        let (bytes, sha256) = sealed_object_digest(&path)?;
        Ok(Self {
            path,
            bytes,
            sha256,
        })
    }
}

fn sealed_object_digest(path: &Path) -> Result<(u64, [u8; 32])> {
    use sha2::{Digest, Sha256};
    let metadata = std::fs::symlink_metadata(path)?;
    anyhow::ensure!(
        metadata.file_type().is_file() && metadata.len() != 0,
        "sealed output is not an existing nonempty regular file: {}",
        path.display()
    );
    let bytes = std::fs::read(path)?;
    Ok((metadata.len(), Sha256::digest(bytes).into()))
}

fn owned_root_parquet_paths(
    context: &mut ScenarioContext,
    table_root: &Path,
) -> Result<Vec<std::path::PathBuf>> {
    // Hadoop publishes a canonical absolute location even when the runner's
    // artifact root is relative or uses a filesystem alias.
    let owned = table_root.canonicalize()?;
    let table_root = owned.as_path();
    let version = hadoop_metadata_version(table_root)?;
    let metadata: TableMetadata = serde_json::from_slice(&std::fs::read(
        table_root
            .join("metadata")
            .join(format!("v{version}.metadata.json")),
    )?)?;
    // Follow the actual table metadata, not a guessed SDK filename or a
    // deletion-writer staging path. Each new target belongs to this one run.
    let data = metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", metadata.location().trim_end_matches('/')));
    let data = Path::new(data.strip_prefix("file://").unwrap_or(&data));
    anyhow::ensure!(
        data.is_absolute() && data.starts_with(table_root),
        "object oracle refuses data outside its original local table"
    );
    // Fresh CREATE need not create a data directory before any append.
    match std::fs::symlink_metadata(data) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
        Ok(metadata) => anyhow::ensure!(
            metadata.file_type().is_dir(),
            "object oracle refuses a redirected data directory"
        ),
    }
    let data = data.canonicalize()?;
    anyhow::ensure!(
        data.starts_with(&owned),
        "object oracle refuses a redirected data root"
    );
    let mut pending = vec![data];
    let mut objects = Vec::new();
    while let Some(directory) = pending.pop() {
        context.remaining("observe the original sealed writer objects")?;
        anyhow::ensure!(
            std::fs::symlink_metadata(&directory)?.file_type().is_dir(),
            "object oracle refuses a redirected directory"
        );
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            anyhow::ensure!(
                !kind.is_symlink(),
                "object oracle refuses a redirected object"
            );
            if kind.is_dir() {
                pending.push(entry.path());
            } else if entry.path().extension().is_some_and(|ext| ext == "parquet") {
                anyhow::ensure!(kind.is_file(), "original data object is not a regular file");
                objects.push(entry.path());
            }
        }
    }
    objects.sort();
    Ok(objects)
}

fn capture_sealed_root_objects(
    context: &mut ScenarioContext,
    table_root: &Path,
) -> Result<Vec<SealedRootObject>> {
    let paths = owned_root_parquet_paths(context, table_root)?;
    anyhow::ensure!(
        !paths.is_empty(),
        "real sealed writer collection has no surviving objects"
    );
    let objects = paths
        .into_iter()
        .map(SealedRootObject::observe)
        .collect::<Result<Vec<_>>>()?;
    assert_sealed_root_row_bag(&objects)?;
    Ok(objects)
}

/// Independent original input: seed_source_files writes each integer in
/// 1..=SEED_ROWS exactly once. Nonempty files alone could hide a deleted peer.
fn assert_sealed_root_row_bag(objects: &[SealedRootObject]) -> Result<()> {
    use arrow_array::{Array, Int64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut values = Vec::new();
    for object in objects {
        let path = &object.path;
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?.build()?;
        let mut file_rows = 0;
        for batch in reader {
            let batch = batch?;
            anyhow::ensure!(
                batch.num_columns() == 1 && batch.schema().field(0).name() == "v",
                "sealed data file lost the original single v column: {}",
                path.display()
            );
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("sealed original v column is not BIGINT")?;
            anyhow::ensure!(
                column.null_count() == 0,
                "sealed original v column acquired NULLs: {}",
                path.display()
            );
            file_rows += column.len();
            values.extend(column.values().iter().copied());
        }
        anyhow::ensure!(
            file_rows > 0,
            "sealed original data file has no data rows: {}",
            path.display()
        );
    }
    values.sort_unstable();
    let expected = (1..=SEED_ROWS).collect::<Vec<_>>();
    anyhow::ensure!(
        values == expected,
        "sealed original object collection lost, duplicated or changed the complete seeded row bag: rows={}",
        values.len()
    );
    Ok(())
}

// SQL receives the Task owner's neutral summary, never Connector Display.
// The optional suffix is exactly the original driver-owned operator location.
fn connector_neutral_detail(detail: &str, expected: FailureFact) -> bool {
    let base = format!(
        "{:?} failure (reason {})",
        expected.class(),
        expected.reason()
    );
    if detail == base {
        return true;
    }
    let Some(location) = detail.strip_prefix(&format!("{base} at operator ")) else {
        return false;
    };
    let Some((ordinal, operation)) = location.split_once(" during ") else {
        return false;
    };
    ordinal
        .parse::<usize>()
        .is_ok_and(|value| value.to_string() == ordinal)
        && matches!(
            operation,
            "Control" | "Preparation" | "Push" | "Pull" | "Finishing" | "Admission"
        )
}

fn exact_uncommitted_write_publication_message(message: &str, public_message: &str) -> bool {
    let prefix = format!("{public_message} (lake publication id=");
    let Some(publication) = message
        .strip_prefix(prefix.as_str())
        .and_then(|rest| rest.strip_suffix(" family=write disposition=KnownUncommitted)"))
        .and_then(|id| {
            id.parse::<novarocks_spi::connector::LakePublicationId>()
                .ok()
        })
    else {
        return false;
    };
    // The typed UUIDv7 parser plus complete round-trip equality rejects extra
    // fields, noncanonical IDs, changed descriptors and any trailing text.
    message
        == format!(
            "{public_message} (lake publication id={publication} family=write disposition=KnownUncommitted)"
        )
}

fn assert_connector_failure(
    context: &mut ScenarioContext,
    outcome: &Result<(), mysql::Error>,
    snapshot: Option<&QueryLifecycleStructuredSnapshot>,
    expected: FailureFact,
    case: &str,
) -> Result<()> {
    let snapshot = snapshot.context("connector fault has no exact accepted statement snapshot")?;
    anyhow::ensure!(
        snapshot.failure_fact == Some(expected)
            && snapshot.attempt_id == 1
            && snapshot.worker_contexts_converged,
        "{case}: original Connector fact or physical context convergence changed: {snapshot:?}"
    );
    let Some(QueryLifecycleFailureSite::Task { identity, phase }) = &snapshot.failure_site else {
        bail!("{case}: Connector failure lost its actual Task site");
    };
    anyhow::ensure!(
        *phase == TaskFailurePhase::Execution,
        "{case}: Task phase changed: {phase:?}"
    );
    let execution = identity.query_execution_id();
    let execution_text = format!(
        "{}:{}:{}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get()
    );
    anyhow::ensure!(
        execution.attempt_id().get() == 1
            && u64::from_ne_bytes(execution.query_id().high().to_ne_bytes())
                == snapshot.process_namespace
            && execution.query_id().low() as u64 == snapshot.local_sequence
            && snapshot.execution_id.as_deref() == Some(execution_text.as_str()),
        "{case}: accepted Task belongs to another immutable execution"
    );
    let processes = (0..context.handle().be_count())
        .map(|index| context.handle().backend_process_id(index))
        .collect::<Result<Vec<_>>>()?;
    anyhow::ensure!(
        processes.contains(&identity.backend_process_id()),
        "{case}: accepted Task belongs to another backend process"
    );
    let detail = snapshot
        .failure_detail
        .as_deref()
        .context("accepted Connector failure has no safe detail")?;
    anyhow::ensure!(
        connector_neutral_detail(detail, expected),
        "{case}: accepted detail is not its exact neutral fact/location summary: {detail:?}"
    );
    let Err(mysql::Error::MySqlError(error)) = outcome else {
        bail!("{case}: expected the original complete public SQL execution error, got {outcome:?}");
    };
    let descriptor = match expected.class() {
        FailureClass::Unexpected => "query.execution.unexpected",
        FailureClass::Data => "query.execution.invalid_data",
        class => bail!("{case}: no declared SQL descriptor for fixture Connector class {class:?}"),
    };
    let public_message = format!("[{descriptor}] {detail}");
    anyhow::ensure!(
        error.code == 1105
            && error.state == "HY000"
            && exact_uncommitted_write_publication_message(&error.message, &public_message),
        "{case}: SQL code/state/detail differs from the accepted Connector failure: {error:?}"
    );
    Ok(())
}

const WRITE_FAULTS: [WriteFault; 11] = [
    WriteFault::WriterEgress,
    WriteFault::RootValidation,
    WriteFault::PartialAggregateUpdate,
    WriteFault::PartialAggregateFinalize,
    WriteFault::FinalAggregateMerge,
    WriteFault::FinalAggregateFinalize,
    WriteFault::SeveredWriterStream,
    WriteFault::FetchAbort,
    WriteFault::DeadlineAbort,
    WriteFault::SealedBeforeRootData,
    WriteFault::SealedAfterRootPrefix,
];

fn writer_fault_specs(cluster_size: usize) -> Vec<StatementSpec> {
    let target =
        "distributed_writer_faults.distributed_writer_fault_db.distributed_writer_fault_data";
    let full = format!(
        "INSERT INTO {target} SELECT v FROM distributed_writer_faults.distributed_writer_fault_db.distributed_writer_fault_source"
    );
    let delayed = format!(
        "INSERT INTO {target} SELECT v FROM distributed_writer_faults.distributed_writer_fault_db.distributed_writer_fault_delay WHERE sleep(delay_s)"
    );
    WRITE_FAULTS
        .into_iter()
        .map(|fault| {
            let case_full = if matches!(fault, WriteFault::SealedBeforeRootData | WriteFault::SealedAfterRootPrefix) {
                format!("INSERT INTO distributed_writer_faults.distributed_writer_fault_db.{} SELECT v FROM distributed_writer_faults.distributed_writer_fault_db.distributed_writer_fault_source", fault.target_table())
            } else { full.clone() };
            let statements = WriteStatements { full: &case_full, delayed: &delayed };
            let kind = match fault {
                WriteFault::WriterEgress => Some("connector-write-writer-failure"),
                WriteFault::RootValidation => Some("connector-write-root-failure"),
                WriteFault::SealedBeforeRootData => {
                    Some("connector-write-root-read-before-data-failure")
                }
                WriteFault::SealedAfterRootPrefix => {
                    Some("connector-write-root-read-after-prefix-failure")
                }
                WriteFault::PartialAggregateUpdate => {
                    Some("connector-write-partial-update-failure")
                }
                WriteFault::PartialAggregateFinalize => {
                    Some("connector-write-partial-finalize-failure")
                }
                WriteFault::FinalAggregateMerge => Some("connector-write-final-merge-failure"),
                WriteFault::FinalAggregateFinalize => {
                    Some("connector-write-final-finalize-failure")
                }
                WriteFault::SeveredWriterStream => Some(TASK_EXECUTION_FAILURE),
                WriteFault::FetchAbort => None,
                WriteFault::DeadlineAbort => Some("connector-write-append-hold"),
            };
            StatementSpec {
                sql: fault.statement(&statements).to_owned(),
                faults: kind
                    .map(|kind| {
                        if matches!(fault, WriteFault::SeveredWriterStream) {
                            (0..cluster_size)
                                .flat_map(|index| {
                                    [(index, kind), (index, "create-task-before-worker-hold")]
                                })
                                .collect()
                        } else {
                            (0..cluster_size).map(|index| (index, kind)).collect()
                        }
                    })
                    .unwrap_or_default(),
                observations: if matches!(fault, WriteFault::FetchAbort) {
                    (0..cluster_size)
                        .map(|index| (index, "grpc-fragment", None))
                        .collect()
                } else {
                    vec![]
                },
            }
        })
        .collect()
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

fn await_append_holds(context: &mut ScenarioContext, baseline: &[WriteCounters]) -> Result<()> {
    let deadline = context.deadline();
    loop {
        let current = all_write_counters(context)?;
        if current.len() == baseline.len()
            && current
                .iter()
                .zip(baseline)
                .all(|(current, before)| current.append_holds > before.append_holds)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let observed = current
                .iter()
                .zip(baseline)
                .map(|(current, before)| current.append_holds - before.append_holds)
                .collect::<Vec<_>>();
            bail!(
                "timed out waiting for one query-scoped writer append hold on every BE; \
                 deltas={observed:?}"
            );
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn await_deadline_abort_terminals(
    context: &mut ScenarioContext,
    baseline: &[WriteCounters],
) -> Result<()> {
    let deadline = context.deadline();
    loop {
        let current = all_write_counters(context)?;
        let abort_succeeded = current
            .iter()
            .zip(baseline)
            .map(|(current, before)| current.abort_succeeded - before.abort_succeeded)
            .sum::<f64>();
        let result_aborted = current
            .iter()
            .zip(baseline)
            .map(|(current, before)| current.result_aborted - before.result_aborted)
            .sum::<f64>();
        if abort_succeeded >= baseline.len() as f64 && result_aborted >= 1.0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for deadline cleanup at its real owners; \
                 provider_abort_succeeded={abort_succeeded} root_result_aborted={result_aborted}"
            );
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(test)]
mod controlled_writer_tests {
    use super::*;

    #[test]
    fn original_parquet_row_bag_rejects_a_missing_or_changed_sealed_peer() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        fn write(path: &Path, values: &[i64]) {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(values.to_vec()))],
            )
            .unwrap();
            let mut writer =
                ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let directory = tempfile::tempdir().unwrap();
        let left = directory.path().join("left.parquet");
        let right = directory.path().join("right.parquet");
        let rows = (1..=SEED_ROWS).collect::<Vec<_>>();
        let split = rows.len() / 2;
        write(&left, &rows[..split]);
        write(&right, &rows[split..]);
        let objects = vec![
            SealedRootObject::observe(left.clone()).unwrap(),
            SealedRootObject::observe(right.clone()).unwrap(),
        ];
        assert_sealed_root_row_bag(&objects).unwrap();
        // The directory stays nonempty and every surviving file is real.
        // Neither condition may stand in for the complete original inputs.
        assert!(assert_sealed_root_row_bag(&objects[..1]).is_err());
        let mut changed = rows[split..].to_vec();
        changed[0] = rows[0];
        write(&right, &changed);
        assert!(assert_sealed_root_row_bag(&objects).is_err());
        write(&right, &[]);
        assert!(assert_sealed_root_row_bag(&objects).is_err());
    }

    #[test]
    fn connector_failure_requires_the_complete_canonical_publication_message() {
        let id = "018f1f07-1234-7000-8000-00000000000a";
        for public in [
            "[query.execution.unexpected] Unexpected failure (reason 4:10)",
            "[query.execution.invalid_data] Invalid data (reason 4:9)",
            "[query.execution.deadline_exceeded] logical execution deadline expired before success EOF",
        ] {
            let complete = format!(
                "{public} (lake publication id={id} family=write disposition=KnownUncommitted)"
            );
            assert!(exact_uncommitted_write_publication_message(
                &complete, public
            ));
            for wrong in [
                public.to_owned(),
                complete.replace("family=write", "family=data_mutation"),
                complete.replace("KnownUncommitted", "CommitUnknown"),
                complete.replace("KnownUncommitted", "CommittedSucceeded"),
                complete.replace(id, "not-a-publication-id"),
                complete.replace(id, "018f1f07-1234-4000-8000-00000000000a"),
                complete.replace(id, &id.to_uppercase()),
                complete.replace(" family=write", " extra=true family=write"),
                complete.replace(" family=write", "  family=write"),
                format!("{complete} trailing"),
                format!("prefix {complete}"),
            ] {
                assert!(!exact_uncommitted_write_publication_message(&wrong, public));
            }
            assert!(!exact_uncommitted_write_publication_message(
                &complete,
                "[query.execution.unexpected] Unexpected failure (reason 4:9)"
            ));
        }
    }

    #[test]
    fn pre_dispatch_uncommitted_requires_one_terminal_and_all_other_labels_unchanged() {
        let before = PublicationTerminals {
            pre_dispatch_uncommitted: 10.0,
            pre_dispatch_unknown: 10.0,
            pre_dispatch_committed_succeeded: 10.0,
            pre_dispatch_committed_failed: 10.0,
            dispatch_uncommitted: 10.0,
            dispatch_unknown: 10.0,
            dispatch_committed_succeeded: 10.0,
            dispatch_committed_failed: 10.0,
        };
        let mut after = before;
        after.pre_dispatch_uncommitted += 1.0;
        assert!(assert_commit_never_invoked(before, after, "one original attempt").is_ok());
        assert!(assert_commit_never_invoked(before, before, "missing terminal").is_err());
        let mut duplicate = after;
        duplicate.pre_dispatch_uncommitted += 1.0;
        assert!(assert_commit_never_invoked(before, duplicate, "duplicate terminal").is_err());

        let changes: [fn(&mut PublicationTerminals); 7] = [
            |value| value.pre_dispatch_unknown += 1.0,
            |value| value.pre_dispatch_committed_succeeded += 1.0,
            |value| value.pre_dispatch_committed_failed += 1.0,
            |value| value.dispatch_uncommitted += 1.0,
            |value| value.dispatch_unknown += 1.0,
            |value| value.dispatch_committed_succeeded += 1.0,
            |value| value.dispatch_committed_failed += 1.0,
        ];
        for change in changes {
            let mut wrong = after;
            change(&mut wrong);
            assert!(assert_commit_never_invoked(before, wrong, "another terminal").is_err());
        }
        // An unchanged sum must not hide individual label changes.
        let mut balanced = after;
        balanced.dispatch_uncommitted += 1.0;
        balanced.dispatch_unknown -= 1.0;
        assert!(assert_commit_never_invoked(before, balanced, "balanced labels").is_err());
    }

    #[test]
    fn fault_matrix_has_only_targeted_write_permissions() {
        let specs = writer_fault_specs(3);
        assert_eq!(specs.len(), 11);
        assert!(specs.iter().all(|spec| {
            spec.sql
                .starts_with("INSERT INTO distributed_writer_faults.")
        }));
        assert!(specs.iter().all(|spec| {
            spec.observations
                .iter()
                .all(|(_, action, _)| *action == "grpc-fragment")
        }));
        assert_eq!(
            specs.iter().map(|spec| spec.faults.len()).sum::<usize>(),
            33
        );
        assert_eq!(
            specs
                .iter()
                .map(|spec| spec.observations.len())
                .sum::<usize>(),
            3
        );
        assert_eq!(
            specs[6].faults,
            (0..3)
                .flat_map(|index| [
                    (index, TASK_EXECUTION_FAILURE),
                    (index, "create-task-before-worker-hold"),
                ])
                .collect::<Vec<_>>()
        );
        assert!(specs[7].faults.is_empty());
        assert!(specs[7].sql.ends_with("WHERE sleep(delay_s)"));
        assert!(specs.iter().flat_map(|spec| &spec.faults).all(|(_, kind)| {
            novarocks_failpoint::QueryLifecycleFaultKind::parse(kind).is_some()
        }));
        // This is 37 FE grants: the statement owner plus 33 binds and three
        // Task observations. Seed/setup and the healthy post-matrix write
        // deliberately receive no permission.
        assert_eq!(
            1 + specs
                .iter()
                .map(|spec| spec.faults.len() + spec.observations.len())
                .sum::<usize>(),
            37
        );
    }
}
