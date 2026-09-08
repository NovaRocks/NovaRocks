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

//! Test-owned fixture recipes and business completion observations.
//!
//! The scenario owns the REST/S3 services and creates a normal private catalog.
//! This module uses only public SQL against that catalog. No application owner,
//! fault marker, or controller label can manufacture a completed business job.

use super::fixture_identity::{
    BusinessInitialState, FixtureTableIdentity, PartitionShape, PreparedJobIdentity,
    PreparedWindowIdentity, RawFileFact, RawSnapshotFact, freeze_table_identity,
};
use super::manifest::{BusinessKind, MixedWorkload};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use mysql::{Conn, Row, Value};
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestRuntimeIdentity;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static NEXT_NAMESPACE: AtomicU64 = AtomicU64::new(1);

/// A catalog created and exclusively owned by the calling scenario.
///
/// Construction does not start services or register a catalog. The scenario
/// must keep its isolated REST/S3 fixture alive through cluster shutdown.
#[derive(Debug, Clone)]
pub struct MixedFixtureBinding {
    catalog: String,
    provider_runtime: Option<IsolatedIcebergRestRuntimeIdentity>,
}

impl MixedFixtureBinding {
    pub fn new(catalog: String) -> Result<Self> {
        ensure!(
            valid_identifier(&catalog),
            "invalid private fixture catalog identifier"
        );
        Ok(Self {
            catalog,
            provider_runtime: None,
        })
    }

    pub fn with_provider_runtime(
        catalog: String,
        provider_runtime: IsolatedIcebergRestRuntimeIdentity,
    ) -> Result<Self> {
        let mut binding = Self::new(catalog)?;
        binding.provider_runtime = Some(provider_runtime);
        Ok(binding)
    }

    pub(crate) fn provider_runtime(&self) -> Option<&IsolatedIcebergRestRuntimeIdentity> {
        self.provider_runtime.as_ref()
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct PreparedJob {
    pub kind: BusinessKind,
    pub ordinal: usize,
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub source: String,
    pub expected_rows: u64,
    pub expected_sum: u64,
    pub input_files: u64,
    pub input_artifacts: u64,
    pub input_data_paths: BTreeSet<String>,
    pub input_delete_paths: BTreeSet<String>,
    pub input_snapshots: BTreeSet<i64>,
}

impl PreparedJob {
    pub fn target(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }
}

#[derive(Debug, Serialize)]
pub(super) struct PreparedWindow {
    pub catalog: String,
    pub namespace: String,
    pub foreground: String,
    pub foreground_rows: u64,
    pub jobs: BTreeMap<BusinessKind, Vec<PreparedJob>>,
    pub fixture_identity: PreparedWindowIdentity,
    #[serde(skip)]
    cleanup: Vec<String>,
}

impl PreparedWindow {
    pub fn cleanup(&self, connection: &mut Conn) -> Result<()> {
        connection.query_drop(format!("SET CATALOG {}", self.catalog))?;
        connection.query_drop(format!("USE {}", self.namespace))?;
        for statement in self.cleanup.iter().rev() {
            connection
                .query_drop(statement)
                .context("clean private mixed fixture object")?;
        }
        connection
            .query_drop(format!("DROP DATABASE {}.{}", self.catalog, self.namespace))
            .context("drop private mixed fixture namespace")
    }
}

pub(super) fn prepare_window(
    connection: &mut Conn,
    binding: &MixedFixtureBinding,
    workload: &MixedWorkload,
    window_index: usize,
) -> Result<PreparedWindow> {
    let namespace = format!(
        "uea1_w{window_index}_{}",
        NEXT_NAMESPACE.fetch_add(1, Ordering::Relaxed)
    );
    let catalog = binding.catalog.clone();
    // Deliberately omit IF NOT EXISTS: a pre-existing object is not this run's
    // fixture, and must not be silently reused or reclaimed.
    connection.query_drop(format!("CREATE DATABASE {catalog}.{namespace}"))?;
    let (rows_per_file, files_per_table, foreground_rows) = workload.fixture.dimensions();
    let foreground = format!("{catalog}.{namespace}.foreground");
    // On setup failure retain the partially prepared namespace. The scenario
    // tears down its exact isolated storage fixture after stopping the cluster.
    seed_table(connection, &foreground, foreground_rows, 1)?;
    let foreground_sum = sequence_sum(foreground_rows)?;
    let foreground_identity = collect_table_identity(
        connection,
        &foreground,
        format!("window/{window_index}/foreground"),
        window_index,
        None,
        None,
        foreground_rows,
        foreground_sum,
    )?;
    let mut cleanup = vec![format!("DROP TABLE {foreground}")];
    let mut jobs_by_kind = BTreeMap::new();
    let mut job_identities = Vec::new();
    let base_rows = rows_per_file
        .checked_mul(files_per_table as u64)
        .context("fixture row overflow")?;
    for producer in &workload.producers {
        let mut jobs = Vec::with_capacity(producer.jobs);
        for ordinal in 0..producer.jobs {
            let table = format!("{}_{}", producer.kind.name().replace('-', "_"), ordinal);
            let source_table = if producer.kind == BusinessKind::MvRefresh {
                format!("{table}_source")
            } else {
                table.clone()
            };
            let source = format!("{catalog}.{namespace}.{source_table}");
            seed_table(connection, &source, rows_per_file, files_per_table)?;
            cleanup.push(format!("DROP TABLE {source}"));
            let (expected_rows, expected_sum) = if producer.kind == BusinessKind::Optimize {
                // Exercise the production-proven v3 rewrite path with live
                // deletion vectors as well as multiple small data files.
                for file in 1..=files_per_table as u64 {
                    let value = file
                        .checked_mul(rows_per_file)
                        .context("fixture delete value overflow")?;
                    connection.query_drop(format!("DELETE FROM {source} WHERE v = {value}"))?;
                }
                let deleted_rows = files_per_table as u64;
                let deleted_sum = rows_per_file
                    .checked_mul(
                        deleted_rows
                            .checked_mul(deleted_rows + 1)
                            .context("fixture deleted sum overflow")?
                            / 2,
                    )
                    .context("fixture deleted sum overflow")?;
                (
                    base_rows
                        .checked_sub(deleted_rows)
                        .context("fixture deleted every row")?,
                    sequence_sum(base_rows)?
                        .checked_sub(deleted_sum)
                        .context("fixture deleted sum exceeds total")?,
                )
            } else {
                (base_rows, sequence_sum(base_rows)?)
            };
            assert_rows(connection, &source, expected_rows, expected_sum)?;
            let input_identity = collect_table_identity(
                connection,
                &source,
                format!("window/{window_index}/{}/{}", producer.kind.name(), ordinal),
                window_index,
                Some(producer.kind),
                Some(ordinal),
                expected_rows,
                expected_sum,
            )?;
            let input_files = input_identity
                .raw
                .files
                .iter()
                .filter(|file| file.content == 0)
                .count() as u64;
            ensure!(
                input_files >= 2,
                "mixed job fixture must contain multiple actual data files"
            );
            let input_snapshots = input_identity
                .raw
                .snapshots
                .iter()
                .map(|snapshot| snapshot.snapshot_id)
                .collect::<BTreeSet<_>>();
            let input_artifacts = input_identity.raw.files.len() as u64;
            let input_data_paths = input_identity
                .raw
                .files
                .iter()
                .filter(|file| file.content == 0)
                .map(|file| file.file_path.clone())
                .collect::<BTreeSet<_>>();
            let input_delete_paths = input_identity
                .raw
                .files
                .iter()
                .filter(|file| file.content == 1)
                .map(|file| file.file_path.clone())
                .collect::<BTreeSet<_>>();
            if producer.kind == BusinessKind::Optimize {
                ensure!(
                    !input_delete_paths.is_empty(),
                    "mixed OPTIMIZE fixture has no live deletion vector"
                );
            }
            let live_delete_files = input_delete_paths.len();
            ensure!(
                !input_snapshots.is_empty(),
                "mixed job fixture has no committed input snapshot"
            );
            let job = PreparedJob {
                kind: producer.kind,
                ordinal,
                catalog: catalog.clone(),
                namespace: namespace.clone(),
                table,
                source,
                expected_rows,
                expected_sum,
                input_files,
                input_artifacts,
                input_data_paths,
                input_delete_paths,
                input_snapshots,
            };
            let initial_state = if job.kind == BusinessKind::MvRefresh {
                connection.query_drop(format!("SET CATALOG {}", job.catalog))?;
                connection.query_drop(format!("USE {}", job.namespace))?;
                connection.query_drop(format!(
                    "CREATE MATERIALIZED VIEW {} DISTRIBUTED BY HASH(v) BUCKETS 3 AS SELECT v FROM {}",
                    job.table, job.source,
                ))?;
                cleanup.push(format!(
                    "DROP MATERIALIZED VIEW {}.{}",
                    job.namespace, job.table
                ));
                let observed = mv_observation(connection, &job)?;
                BusinessInitialState::MvRefresh {
                    refresh_time: observed.refresh_time,
                    refresh_rows: observed.rows,
                }
            } else if job.kind == BusinessKind::Analyze {
                BusinessInitialState::Analyze {
                    matching_jobs: analyze_jobs(connection, &job)?.len(),
                    theta_statistics_available: has_theta_statistics(connection, &job)?,
                }
            } else {
                BusinessInitialState::Optimize {
                    matching_jobs: optimize_jobs(connection, &job)?.len(),
                    live_delete_files,
                }
            };
            initial_state.validate_for(job.kind)?;
            job_identities.push(PreparedJobIdentity {
                kind: job.kind,
                ordinal: job.ordinal,
                input: input_identity,
                initial_state,
            });
            jobs.push(job);
        }
        jobs_by_kind.insert(producer.kind, jobs);
    }
    let fixture_identity =
        PreparedWindowIdentity::try_new(window_index, foreground_identity, job_identities)?;
    Ok(PreparedWindow {
        catalog,
        namespace,
        foreground,
        foreground_rows,
        jobs: jobs_by_kind,
        fixture_identity,
        cleanup,
    })
}

fn seed_table(connection: &mut Conn, table: &str, rows_per_file: u64, files: usize) -> Result<()> {
    connection.query_drop(format!(
        "CREATE TABLE {table} (v BIGINT) TBLPROPERTIES \
         ('format-version' = '3', 'write.row-lineage' = 'true', \
          'novarocks.statistics.collect-on-write' = 'false')"
    ))?;
    for file in 0..files as u64 {
        let low = file
            .checked_mul(rows_per_file)
            .and_then(|value| value.checked_add(1))
            .context("fixture range overflow")?;
        let high = (file + 1)
            .checked_mul(rows_per_file)
            .context("fixture range overflow")?;
        connection.query_drop(format!(
            "INSERT INTO {table} SELECT generate_series FROM TABLE(generate_series({low}, {high}))"
        ))?;
    }
    Ok(())
}

pub(super) fn sequence_sum(rows: u64) -> Result<u64> {
    rows.checked_mul(rows.checked_add(1).context("fixture sum overflow")?)
        .map(|value| value / 2)
        .context("fixture sum overflow")
}

pub(super) fn assert_rows(
    connection: &mut Conn,
    table: &str,
    expected_rows: u64,
    expected_sum: u64,
) -> Result<()> {
    let observed: Option<(u64, Option<u64>)> = connection.query_first(format!(
        "SELECT COUNT(*), CAST(SUM(v) AS BIGINT) FROM {table}"
    ))?;
    ensure!(
        expected_rows > 0 && observed == Some((expected_rows, Some(expected_sum))),
        "mixed fixture/result aggregate differs from its frozen non-empty oracle"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_table_identity(
    connection: &mut Conn,
    table: &str,
    symbol: String,
    window_index: usize,
    business_kind: Option<BusinessKind>,
    job_ordinal: Option<usize>,
    expected_rows: u64,
    expected_sum: u64,
) -> Result<FixtureTableIdentity> {
    let aggregate: Option<(u64, Option<u64>)> = connection.query_first(format!(
        "SELECT COUNT(*), CAST(SUM(v) AS BIGINT) FROM {table}"
    ))?;
    let (rows, sum) = aggregate
        .and_then(|(rows, sum)| sum.map(|sum| (rows, sum)))
        .context("mixed fixture identity aggregate is empty")?;
    ensure!(
        rows == expected_rows && sum == expected_sum,
        "mixed fixture identity aggregate differs from its frozen oracle"
    );

    // The closed benchmark fixture is created without a partition transform.
    // Read only the scalar `$files` facts needed for identity; selecting the
    // optional nested `partition` column would make an unpartitioned fixture
    // depend on an unrelated MySQL nested-value representation.
    let file_rows: Vec<Row> = connection.query(format!(
        "SELECT content, file_path, file_format, spec_id, record_count, file_size_in_bytes FROM {table}$files"
    ))?;
    ensure!(
        !file_rows.is_empty(),
        "mixed fixture identity has no `$files` rows"
    );
    let mut files = Vec::with_capacity(file_rows.len());
    for row in file_rows {
        let content = required_number::<i32>(&row, "content")?;
        let spec_id = required_number::<i32>(&row, "spec_id")?;
        let record_count = u64::try_from(required_number::<i64>(&row, "record_count")?)
            .context("mixed `$files` record_count is negative")?;
        let file_size_in_bytes = u64::try_from(required_number::<i64>(&row, "file_size_in_bytes")?)
            .context("mixed `$files` file_size_in_bytes is negative")?;
        files.push(RawFileFact {
            content,
            file_path: required(&row, "file_path")?,
            file_format: required(&row, "file_format")?,
            spec_id,
            record_count,
            file_size_in_bytes,
            partition_shape: PartitionShape::Unpartitioned,
        });
    }

    let snapshot_rows: Vec<Row> = connection.query(format!(
        "SELECT snapshot_id, parent_id, operation FROM {table}$snapshots"
    ))?;
    ensure!(
        !snapshot_rows.is_empty(),
        "mixed fixture identity has no `$snapshots` rows"
    );
    let snapshots = snapshot_rows
        .iter()
        .map(|row| {
            Ok(RawSnapshotFact {
                snapshot_id: required_number(row, "snapshot_id")?,
                parent_id: number(row, "parent_id")?,
                operation: required(row, "operation")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    freeze_table_identity(
        table.to_string(),
        symbol,
        window_index,
        business_kind,
        job_ordinal,
        rows,
        sum,
        files,
        snapshots,
    )
}

fn required_number<T>(row: &Row, name: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    required(row, name)?
        .parse()
        .with_context(|| format!("parse mixed fixture identity column {name}"))
}

fn snapshots(connection: &mut Conn, table: &str) -> Result<BTreeSet<i64>> {
    let values: Vec<i64> =
        connection.query(format!("SELECT snapshot_id FROM {table}$snapshots"))?;
    Ok(values.into_iter().collect())
}

fn data_files(connection: &mut Conn, table: &str) -> Result<u64> {
    connection
        .query_first(format!(
            "SELECT COUNT(*) FROM {table}$files WHERE content = 0"
        ))?
        .context("data file observation returned no row")
}

fn physical_files(connection: &mut Conn, table: &str) -> Result<u64> {
    connection
        .query_first(format!("SELECT COUNT(*) FROM {table}$files"))?
        .context("physical file observation returned no row")
}

fn file_paths(connection: &mut Conn, table: &str, content: u8) -> Result<BTreeSet<String>> {
    let paths: Vec<String> = connection.query(format!(
        "SELECT file_path FROM {table}$files WHERE content = {content}"
    ))?;
    Ok(paths.into_iter().collect())
}

fn snapshot_parent(connection: &mut Conn, table: &str, snapshot: i64) -> Result<Option<i64>> {
    let parent: Option<Option<i64>> = connection.query_first(format!(
        "SELECT parent_id FROM {table}$snapshots WHERE snapshot_id = {snapshot}"
    ))?;
    parent.context("published OPTIMIZE snapshot is absent from snapshot history")
}

fn snapshot_operation(connection: &mut Conn, table: &str, snapshot: i64) -> Result<String> {
    connection
        .query_first(format!(
            "SELECT operation FROM {table}$snapshots WHERE snapshot_id = {snapshot}"
        ))?
        .context("published OPTIMIZE snapshot has no operation")
}

#[derive(Debug, Serialize)]
pub(super) struct BusinessSample {
    pub kind: BusinessKind,
    pub window_index: usize,
    pub ordinal: usize,
    pub target: String,
    pub submitted_micros: u128,
    pub total_micros: u128,
    pub completed_in_window: bool,
    pub effective_rows: u64,
    pub input_data_files: u64,
    pub input_artifacts: u64,
    pub output_data_files: Option<u64>,
    pub output_artifacts: Option<u64>,
    pub reported_output_data_files: Option<u64>,
    pub input_data_paths: BTreeSet<String>,
    pub output_data_paths: Option<BTreeSet<String>>,
    pub input_delete_paths: BTreeSet<String>,
    pub output_delete_paths: Option<BTreeSet<String>>,
    pub source_snapshots: BTreeSet<i64>,
    pub published_snapshot: Option<i64>,
    pub job_id: Option<String>,
    pub publication_marker: String,
}

/// Every job starts from a distinct pre-seeded target and is dispatched once.
/// A timeout is an error with remaining responsibility, never a completion.
pub(super) fn execute_job(
    connection: &mut Conn,
    job: &PreparedJob,
    window_index: usize,
    window_deadline: Instant,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<BusinessSample> {
    if job.kind == BusinessKind::MvRefresh {
        connection.query_drop(format!("SET CATALOG {}", job.catalog))?;
        connection.query_drop(format!("USE {}", job.namespace))?;
    }
    let started = Instant::now();
    let deadline = started
        .checked_add(timeout)
        .context("mixed job deadline overflow")?;
    let statement = match job.kind {
        BusinessKind::MvRefresh => {
            format!("REFRESH MATERIALIZED VIEW {} WITH SYNC MODE", job.table)
        }
        BusinessKind::Analyze => format!("ANALYZE TABLE {}", job.target()),
        BusinessKind::Optimize => format!("ALTER TABLE {} OPTIMIZE", job.target()),
    };
    connection
        .query_drop(statement)
        .with_context(|| format!("submit {} job", job.kind.name()))?;
    let submitted_micros = started.elapsed().as_micros();
    let mut sample = BusinessSample {
        kind: job.kind,
        window_index,
        ordinal: job.ordinal,
        target: job.target(),
        submitted_micros,
        total_micros: 0,
        completed_in_window: false,
        effective_rows: job.expected_rows,
        input_data_files: job.input_files,
        input_artifacts: job.input_artifacts,
        output_data_files: None,
        output_artifacts: None,
        reported_output_data_files: None,
        input_data_paths: job.input_data_paths.clone(),
        output_data_paths: None,
        input_delete_paths: job.input_delete_paths.clone(),
        output_delete_paths: None,
        source_snapshots: job.input_snapshots.clone(),
        published_snapshot: None,
        job_id: None,
        publication_marker: String::new(),
    };
    match job.kind {
        BusinessKind::MvRefresh => {
            let observed = mv_observation(connection, job)?;
            validate_mv_completion(&observed, job.expected_rows)?;
            assert_rows(
                connection,
                &job.target(),
                job.expected_rows,
                job.expected_sum,
            )?;
            ensure!(
                snapshots(connection, &job.source)? == job.input_snapshots,
                "MV job input changed during refresh"
            );
            sample.publication_marker =
                observed.refresh_time.context("completed MV refresh time")?;
        }
        BusinessKind::Analyze => {
            let mut exact_job_id = None;
            let completed = loop {
                check_deadline(deadline)?;
                if let Some(observed) =
                    observe_exact_job(analyze_jobs(connection, job)?, &mut exact_job_id)?
                {
                    if terminal_success(&observed.state, "SUCCEEDED")? {
                        break observed;
                    }
                }
                thread::sleep(
                    poll_interval.min(deadline.saturating_duration_since(Instant::now())),
                );
            };
            ensure!(
                snapshots(connection, &job.source)? == job.input_snapshots,
                "ANALYZE changed the frozen data snapshots"
            );
            ensure!(
                has_theta_statistics(connection, job)?,
                "ANALYZE succeeded without a matching provider statistics artifact"
            );
            assert_rows(connection, &job.source, job.expected_rows, job.expected_sum)?;
            sample.job_id = Some(completed.id);
            sample.publication_marker = completed
                .operation
                .context("ANALYZE completion has no operation identity")?;
        }
        BusinessKind::Optimize => {
            let mut exact_job_id = None;
            let completed = loop {
                check_deadline(deadline)?;
                if let Some(observed) =
                    observe_exact_job(optimize_jobs(connection, job)?, &mut exact_job_id)?
                {
                    if terminal_success(&observed.state, "FINISHED")? {
                        break observed;
                    }
                }
                thread::sleep(
                    poll_interval.min(deadline.saturating_duration_since(Instant::now())),
                );
            };
            let output_data_files = data_files(connection, &job.source)?;
            let output_artifacts = physical_files(connection, &job.source)?;
            let output_data_paths = file_paths(connection, &job.source, 0)?;
            let output_delete_paths = file_paths(connection, &job.source, 1)?;
            validate_optimize_completion(&completed, &job.input_snapshots)?;
            let published = completed
                .target_snapshot
                .context("OPTIMIZE target snapshot is absent")?;
            let after = snapshots(connection, &job.source)?;
            ensure!(
                after.contains(&published)
                    && after
                        .difference(&job.input_snapshots)
                        .copied()
                        .collect::<Vec<_>>()
                        == [published],
                "OPTIMIZE did not publish exactly its observed target snapshot"
            );
            ensure!(
                snapshot_parent(connection, &job.source, published)? == completed.base_snapshot,
                "OPTIMIZE target snapshot is not a direct replacement of its frozen base"
            );
            ensure!(
                snapshot_operation(connection, &job.source, published)? == "replace",
                "OPTIMIZE target snapshot is not a replace operation"
            );
            ensure!(
                !output_data_paths.is_empty()
                    && output_data_paths.is_disjoint(&job.input_data_paths)
                    && output_delete_paths.is_empty()
                    && output_artifacts < job.input_artifacts,
                "OPTIMIZE did not replace data paths and remove live deletion vectors"
            );
            assert_rows(connection, &job.source, job.expected_rows, job.expected_sum)?;
            sample.job_id = Some(completed.id);
            sample.published_snapshot = Some(published);
            sample.output_data_files = Some(output_data_files);
            sample.output_artifacts = Some(output_artifacts);
            sample.reported_output_data_files = completed.output_files;
            sample.output_data_paths = Some(output_data_paths);
            sample.output_delete_paths = Some(output_delete_paths);
            sample.publication_marker = published.to_string();
        }
    }
    check_deadline(deadline)?;
    sample.total_micros = started.elapsed().as_micros();
    sample.completed_in_window = Instant::now() <= window_deadline;
    Ok(sample)
}

fn check_deadline(deadline: Instant) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "mixed business completion deadline exceeded; job outcome is not proven"
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct JobObservation {
    id: String,
    state: String,
    operation: Option<String>,
    base_snapshot: Option<i64>,
    target_snapshot: Option<i64>,
    input_files: Option<u64>,
    output_files: Option<u64>,
}

fn require_single_job(mut observations: Vec<JobObservation>) -> Result<Option<JobObservation>> {
    ensure!(
        observations.len() <= 1,
        "private mixed target has multiple jobs; exact job identity is ambiguous"
    );
    Ok(observations.pop())
}

fn observe_exact_job(
    observations: Vec<JobObservation>,
    expected: &mut Option<String>,
) -> Result<Option<JobObservation>> {
    let observed = require_single_job(observations)?;
    match (&observed, expected.as_ref()) {
        (Some(job), Some(id)) => ensure!(
            &job.id == id,
            "mixed business job identity changed while awaiting completion"
        ),
        (Some(job), None) => *expected = Some(job.id.clone()),
        (None, Some(_)) => bail!("mixed business job disappeared before a proven completion"),
        (None, None) => {}
    }
    Ok(observed)
}

fn terminal_success(state: &str, success: &str) -> Result<bool> {
    if state == success {
        return Ok(true);
    }
    match state {
        "SUBMITTED" | "PENDING" | "PREPARING" | "RUNNING" | "PUBLISHING" => Ok(false),
        _ => bail!("mixed business reached non-success state {state}"),
    }
}

fn analyze_jobs(connection: &mut Conn, job: &PreparedJob) -> Result<Vec<JobObservation>> {
    let rows: Vec<Row> = connection.query("SHOW ANALYZE JOBS")?;
    let mut jobs = Vec::new();
    for row in rows {
        if field(&row, "catalog")?.as_deref() != Some(&job.catalog)
            || field(&row, "namespace")?.as_deref() != Some(&job.namespace)
            || field(&row, "table")?.as_deref() != Some(&job.table)
        {
            continue;
        }
        jobs.push(JobObservation {
            id: required(&row, "job_id")?,
            state: required(&row, "state")?,
            operation: field(&row, "operation_id")?,
            base_snapshot: None,
            target_snapshot: None,
            input_files: None,
            output_files: None,
        });
    }
    Ok(jobs)
}

fn optimize_jobs(connection: &mut Conn, job: &PreparedJob) -> Result<Vec<JobObservation>> {
    let rows: Vec<Row> = connection.query(format!(
        "SHOW ALTER TABLE OPTIMIZE FROM {}.{} WHERE TableName = '{}' ORDER BY CreateTime DESC",
        job.catalog, job.namespace, job.table,
    ))?;
    rows.into_iter()
        .map(|row| {
            Ok(JobObservation {
                id: required(&row, "JobId")?,
                state: required(&row, "State")?,
                operation: None,
                base_snapshot: number(&row, "BaseSnapshotId")?,
                target_snapshot: number(&row, "TargetSnapshotId")?,
                input_files: number(&row, "InputDataFiles")?,
                output_files: number(&row, "OutputDataFiles")?,
            })
        })
        .collect()
}

fn validate_optimize_completion(job: &JobObservation, before: &BTreeSet<i64>) -> Result<()> {
    ensure!(job.state == "FINISHED", "OPTIMIZE is not successful");
    let base = job.base_snapshot.context("OPTIMIZE has no base snapshot")?;
    let target = job
        .target_snapshot
        .context("OPTIMIZE has no target snapshot")?;
    ensure!(
        before.contains(&base) && !before.contains(&target) && base != target,
        "OPTIMIZE did not advance its frozen input snapshot"
    );
    ensure!(
        job.input_files.is_some_and(|files| files >= 2),
        "OPTIMIZE completed without an input rewrite: {job:?}"
    );
    Ok(())
}

struct MvObservation {
    refresh_time: Option<String>,
    rows: Option<u64>,
}

fn mv_observation(connection: &mut Conn, job: &PreparedJob) -> Result<MvObservation> {
    connection.query_drop(format!("SET CATALOG {}", job.catalog))?;
    let rows: Vec<Row> =
        connection.query(format!("SHOW MATERIALIZED VIEWS FROM {}", job.namespace))?;
    let mut matched = Vec::new();
    for row in rows {
        if field(&row, "Name")?.as_deref() == Some(&job.table) {
            matched.push(row);
        }
    }
    ensure!(
        matched.len() == 1,
        "private MV observation is missing or ambiguous"
    );
    Ok(MvObservation {
        refresh_time: field(&matched[0], "LastRefreshTime")?,
        rows: number(&matched[0], "LastRefreshRows")?,
    })
}

fn validate_mv_completion(observed: &MvObservation, expected_rows: u64) -> Result<()> {
    ensure!(
        observed
            .refresh_time
            .as_ref()
            .is_some_and(|value| !value.is_empty())
            && expected_rows > 0
            && observed.rows == Some(expected_rows),
        "MV did not publish the expected non-empty refresh"
    );
    Ok(())
}

fn has_theta_statistics(connection: &mut Conn, job: &PreparedJob) -> Result<bool> {
    let rows: Vec<Row> = connection.query(format!("SHOW TABLE STATS {}", job.target()))?;
    for row in rows {
        if field(&row, "metric")?.as_deref() != Some("theta_ndv:v") {
            continue;
        }
        if field(&row, "status")?.as_deref() != Some("AVAILABLE") {
            return Ok(false);
        }
        ensure!(
            required(&row, "source")? == "PROVIDER_ARTIFACT"
                && required(&row, "basis_version")? == "SAME"
                && required(&row, "basis_relation")? == "IDENTICAL",
            "statistics artifact does not describe the fixed input"
        );
        let value: f64 = required(&row, "value")?
            .parse()
            .context("parse provider Theta estimate")?;
        ensure!(
            value.is_finite()
                && value > 0.0
                && (value - job.expected_rows as f64).abs() <= job.expected_rows as f64 * 0.15,
            "statistics artifact does not cover the non-empty seeded relation"
        );
        return Ok(true);
    }
    Ok(false)
}

fn field(row: &Row, name: &str) -> Result<Option<String>> {
    let index = row
        .columns_ref()
        .iter()
        .position(|column| column.name_str().eq_ignore_ascii_case(name))
        .with_context(|| format!("business observation omitted column {name}"))?;
    match row
        .as_ref(index)
        .context("business observation value is missing")?
    {
        Value::NULL => Ok(None),
        Value::Bytes(bytes) => Ok(Some(
            String::from_utf8(bytes.clone()).context("business observation is not UTF-8")?,
        )),
        Value::Int(value) => Ok(Some(value.to_string())),
        Value::UInt(value) => Ok(Some(value.to_string())),
        Value::Float(value) => Ok(Some(value.to_string())),
        Value::Double(value) => Ok(Some(value.to_string())),
        _ => bail!("business observation column {name} has an unsupported value type"),
    }
}

fn required(row: &Row, name: &str) -> Result<String> {
    field(row, name)?
        .filter(|value| !value.is_empty())
        .with_context(|| format!("business observation {name} is empty"))
}

fn number<T: std::str::FromStr>(row: &Row, name: &str) -> Result<Option<T>> {
    field(row, name)?
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse()
                .map_err(|_| anyhow::anyhow!("business observation {name} is not numeric"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn optimize() -> JobObservation {
        JobObservation {
            id: "1".into(),
            state: "FINISHED".into(),
            operation: None,
            base_snapshot: Some(10),
            target_snapshot: Some(11),
            input_files: Some(3),
            output_files: Some(1),
        }
    }

    #[test]
    fn submitted_and_failed_jobs_never_count_as_success() {
        assert!(!terminal_success("SUBMITTED", "SUCCEEDED").unwrap());
        assert!(!terminal_success("RUNNING", "FINISHED").unwrap());
        for state in ["FAILED", "TARGET_REPLACED", "CANCELLED", "COMMIT_UNKNOWN"] {
            assert!(terminal_success(state, "FINISHED").is_err());
        }
        assert!(terminal_success("SUCCEEDED", "SUCCEEDED").unwrap());
    }

    #[test]
    fn optimize_requires_real_files_and_a_new_snapshot() {
        let before = BTreeSet::from([10]);
        validate_optimize_completion(&optimize(), &before).unwrap();
        let mut empty = optimize();
        empty.input_files = Some(0);
        assert!(validate_optimize_completion(&empty, &before).is_err());
        let mut unchanged = optimize();
        unchanged.target_snapshot = Some(10);
        assert!(validate_optimize_completion(&unchanged, &before).is_err());
        let mut wrong = optimize();
        wrong.base_snapshot = Some(99);
        assert!(validate_optimize_completion(&wrong, &before).is_err());
    }

    #[test]
    fn unrelated_or_duplicate_job_cannot_satisfy_completion() {
        assert!(require_single_job(Vec::new()).unwrap().is_none());
        assert!(require_single_job(vec![optimize(), optimize()]).is_err());
        let mut identity = None;
        observe_exact_job(vec![optimize()], &mut identity).unwrap();
        let mut replaced = optimize();
        replaced.id = "2".into();
        assert!(observe_exact_job(vec![replaced], &mut identity).is_err());
        assert!(observe_exact_job(Vec::new(), &mut identity).is_err());
    }

    #[test]
    fn mv_requires_publication_and_effective_rows() {
        assert!(
            validate_mv_completion(
                &MvObservation {
                    refresh_time: None,
                    rows: Some(8)
                },
                8
            )
            .is_err()
        );
        assert!(
            validate_mv_completion(
                &MvObservation {
                    refresh_time: Some("123".into()),
                    rows: Some(0)
                },
                8
            )
            .is_err()
        );
        validate_mv_completion(
            &MvObservation {
                refresh_time: Some("123".into()),
                rows: Some(8),
            },
            8,
        )
        .unwrap();
    }

    #[test]
    fn fixture_binding_rejects_arbitrary_sql() {
        assert!(MixedFixtureBinding::new("private_catalog".into()).is_ok());
        for value in ["", "a.b", "a; DROP TABLE t", "../shared"] {
            assert!(MixedFixtureBinding::new(value.into()).is_err());
        }
    }
}
