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

//! Frozen UEA-4G native latency samples. Comparison and reversed run order are
//! owned by the external experiment driver, never by adaptive scenario logic.

use super::connector::{await_resource_convergence, require_three_backends, resource_baseline};
use super::iceberg_delete_applicability::{
    CATALOG, Fixture, IcebergDeleteApplicability, create_catalog_sql,
    credential_overlay_with_cache, immutable_artifacts, table_name,
};
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::LaunchProfile;
use novarocks_cluster_harness::delayed_s3::DelayedS3Proxy;
use novarocks_cluster_harness::process_resources::ProcessResourceMonitor;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CACHE_ENV: &str = "NOVAROCKS_UEA4G_PERF_CACHE";
const SIDE_ENV: &str = "NOVAROCKS_UEA4G_PERF_SIDE";
const PASS_ENV: &str = "NOVAROCKS_UEA4G_PERF_PASS";
const WARMUP_RUNS: usize = 2;
const MEASURED_RUNS: usize = 9;
const CONCURRENCIES: [usize; 2] = [1, 4];
const RESOURCE_SAMPLE_INTERVAL_MS: u64 = 50;

#[derive(Default)]
pub(super) struct IcebergDeletePerformance {
    inner: IcebergDeleteApplicability,
}

#[derive(Clone)]
struct Settings {
    warm_cache: bool,
    side: String,
    pass: u64,
}
impl Settings {
    fn load() -> Result<Self> {
        let warm_cache = match env::var(CACHE_ENV).as_deref() {
            Ok("cold") => false,
            Ok("warm") => true,
            _ => bail!("{CACHE_ENV} must be cold or warm"),
        };
        let side = env::var(SIDE_ENV).context("performance side label is missing")?;
        ensure!(
            matches!(side.as_str(), "baseline" | "candidate"),
            "performance side must be baseline or candidate"
        );
        let pass = env::var(PASS_ENV)
            .context("performance pass is missing")?
            .parse::<u64>()?;
        ensure!(matches!(pass, 1 | 2), "performance pass must be 1 or 2");
        Ok(Self {
            warm_cache,
            side,
            pass,
        })
    }
    fn cache_name(&self) -> &'static str {
        if self.warm_cache { "warm" } else { "cold" }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct Aggregate {
    row_count: u64,
    sum_row_id: i64,
    sum_eq_key: i64,
    sum_partition_key: i64,
    sum_payload: i64,
}
impl Aggregate {
    fn from_oracle(case: &Value) -> Result<Self> {
        let oracle = &case["independent_oracle"];
        let int = |key: &str| {
            oracle[key]
                .as_i64()
                .with_context(|| format!("missing oracle {key}"))
        };
        Ok(Self {
            row_count: oracle["row_count"]
                .as_u64()
                .context("missing oracle row count")?,
            sum_row_id: int("sum_row_id")?,
            sum_eq_key: int("sum_eq_key")?,
            sum_partition_key: int("sum_partition_key")?,
            sum_payload: int("sum_payload")?,
        })
    }
    fn digest(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(self.row_count.to_be_bytes());
        for value in [
            self.sum_row_id,
            self.sum_eq_key,
            self.sum_partition_key,
            self.sum_payload,
        ] {
            hash.update(value.to_be_bytes());
        }
        format!("{:x}", hash.finalize())
    }
}

#[derive(Serialize)]
struct Sample {
    actor: usize,
    query_elapsed_ns: u128,
    aggregate: Option<Aggregate>,
    aggregate_sha256: Option<String>,
    matched: bool,
    error_class: Option<&'static str>,
}

impl Scenario for IcebergDeletePerformance {
    fn name(&self) -> &'static str {
        "connector/iceberg-delete-performance"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        _workload: Option<&Path>,
    ) -> Result<()> {
        ensure!(
            launch_profile == LaunchProfile::Performance,
            "UEA-4G samples require --launch-profile performance"
        );
        Settings::load()?;
        Ok(())
    }
    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let settings = Settings::load()?;
        let mut launch = self.inner.launch_config(scenario_root)?;
        for name in [
            "NOVAROCKS_SQL_TEST_EMIT_GRPC_FRAGMENT_MARKER",
            "NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER",
            "NOVAROCKS_SQL_TEST_EMIT_CATALOG_MATERIALIZATION_MARKER",
        ] {
            launch.child_environment.fe.remove(name);
            launch.child_environment.be.remove(name);
        }
        launch.config_overlay = credential_overlay_with_cache(settings.warm_cache);
        Ok(launch)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.launch_profile() == LaunchProfile::Performance,
            "performance profile changed after validation"
        );
        require_three_backends(context)?;
        let settings = Settings::load()?;
        let baseline = resource_baseline(context)?;
        let slot = self
            .inner
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("fixture lock poisoned"))?;
        let fixture = slot
            .as_ref()
            .context("performance fixture is not prepared")?;
        // Content verification happens outside every timed interval. The same
        // immutable inventory and input digest apply to baseline and candidate.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        drop(runtime.block_on(immutable_artifacts(fixture))?);
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("create performance catalog")?,
        )?;
        connection.query_drop(create_catalog_sql(fixture))?;
        let root = context.scenario_root().to_path_buf();
        let samples_path = root.join("delete-performance-samples.jsonl");
        let mut samples = BufWriter::new(File::create(&samples_path)?);
        let resource_started_wall_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let resource_run_id = format!(
            "uea4g-{}-{}-{}",
            settings.pass,
            settings.cache_name(),
            settings.side
        );
        let monitor = ProcessResourceMonitor::start_with_identities(
            context.process_resource_identities()?,
            resource_run_id.as_str(),
            Duration::from_millis(RESOURCE_SAMPLE_INTERVAL_MS),
        )?;
        let mut queries_finished_ms = None;
        let mut convergence_finished_ms = None;
        let stop = AtomicBool::new(false);
        let (experiment, object_log) = thread::scope(|scope| {
            let recorder = scope.spawn(|| record_objects(&fixture.proxy, &stop, &root));
            let queries = run_experiment(context, fixture, &settings, &monitor, &mut samples);
            queries_finished_ms = Some(monitor.elapsed_millis());
            let result = queries.and_then(|summary| {
                await_resource_convergence(context, &baseline, "delete performance queries")?;
                convergence_finished_ms = Some(monitor.elapsed_millis());
                Ok(summary)
            });
            stop.store(true, Ordering::Release);
            let log = recorder
                .join()
                .map_err(|_| anyhow::anyhow!("performance object recorder panicked"))
                .and_then(|r| r);
            (result, log)
        });
        // Explicitly settle the sampler on every experiment result, before any
        // fallible evidence processing or native process teardown. Do not rely
        // on Drop, which cannot report sampling errors.
        let resources = finish_resources(
            monitor,
            &root,
            &resource_run_id,
            resource_started_wall_ns,
            queries_finished_ms,
            convergence_finished_ms,
        );
        samples.flush()?;
        let sample_bytes = fs::read(&samples_path)?;
        let accepted = experiment
            .as_ref()
            .is_ok_and(|summary| accepts_aggregates(&settings, summary));
        let evidence = json!({
            "manifest_sha256":fixture.manifest_sha256,
            "corpus_sha256":fixture.manifest.corpus_sha256,
            "scale_sha256":fixture.manifest.scale_sha256,
            "scale_artifacts_sha256":fixture.manifest.scale_artifacts_sha256,
            "binary_sha256":format!("{:x}",Sha256::digest(fs::read(context.primary_binary())?)),
            "cache":settings.cache_name(),"side":settings.side,"pass":settings.pass,
            "warmup_runs":WARMUP_RUNS,"measured_runs":MEASURED_RUNS,"concurrencies":CONCURRENCIES,
            "timing":"query_elapsed_ns excludes connection setup and aggregate verification; wave_elapsed_ns spans barrier release through all actor joins",
            "cold_definition":"data/page/parquet caches disabled; each query opens a fresh reader; no claim that operating-system or object-store caches were flushed",
            "warm_definition":"normal data/page/parquet caches enabled; two unmeasured repetitions of the exact query precede nine measurements",
            "aggregate_checksum_definition":"SHA-256 over five big-endian 64-bit count/sum fields; this is not an exact row-bag hash",
            "samples_sha256":format!("{:x}",Sha256::digest(&sample_bytes)),
            "object_evidence":object_log.as_ref().ok(),
            "resource_evidence":resources.as_ref().ok(),
            "resource_error":resources.as_ref().err().map(|error| format!("{error:#}")),
            "summary":experiment.as_ref().ok(),"succeeded":experiment.is_ok() && object_log.is_ok() && resources.is_ok() && accepted,
            "comparison_owner":"external two-pass baseline/candidate driver; no ratios or relaxed gates computed here",
        });
        fs::write(
            root.join("delete-performance.json"),
            serde_json::to_vec_pretty(&evidence)?,
        )?;
        resources?;
        let summary = experiment?;
        object_log?;
        connection.query_drop(format!("DROP CATALOG {CATALOG}"))?;
        ensure!(
            accepted,
            "performance aggregate acceptance failed: candidate requires every case correct; baseline requires all no-delete and equality_m1 controls correct; wrong-case timings are noncomparable"
        );
        context.action(format!("recorded the complete fixed scale matrix; all_input_aggregates_match={}; incorrect non-control baseline cases are retained as noncomparable",summary["all_input_aggregates_match"]));
        Ok(())
    }
    fn teardown(&self) -> Result<()> {
        self.inner.teardown()
    }
}

fn run_experiment(
    context: &mut ScenarioContext,
    fixture: &Fixture,
    settings: &Settings,
    monitor: &ProcessResourceMonitor,
    output: &mut impl Write,
) -> Result<Value> {
    let cases = fixture.scale["cases"]
        .as_array()
        .context("scale cases absent")?;
    ensure!(
        cases.len() == 16,
        "performance matrix requires all 16 frozen scale observations"
    );
    let mut groups = Vec::new();
    let mut all_input_aggregates_match = true;
    let mut baseline_controls_match = true;
    for case in cases {
        ensure!(
            case["exact_bag_checked"] == true && case["java_oracle"] == case["independent_oracle"],
            "scale case lacks an independently checked row bag"
        );
        let name = case["case"].as_str().context("scale case name absent")?;
        let table = table_name(case["table"].as_str().context("scale table absent")?)?;
        let snapshot = case["snapshot"].as_i64().context("scale snapshot absent")?;
        let expected = Aggregate::from_oracle(case)?;
        let query = format!(
            "SELECT COUNT(*),COALESCE(SUM(row_id),0),COALESCE(SUM(eq_key),0),COALESCE(SUM(partition_key),0),COALESCE(SUM(payload),0) FROM {table} FOR VERSION AS OF {snapshot}"
        );
        let control = case["configuration"]["family"] == "no_delete" || name == "equality_m1";
        for concurrency in CONCURRENCIES {
            let mut correct = true;
            let mut incorrect_actor_samples = 0;
            let mut measured_ns = Vec::new();
            let mut wave_ns = Vec::new();
            for run in 0..WARMUP_RUNS + MEASURED_RUNS {
                let warmup = run < WARMUP_RUNS;
                let timeout = context.remaining("performance query wave")?;
                // Prepare all connections before creating the start barrier, so
                // a failed connection cannot strand the other actors at it.
                let mut connections = Vec::new();
                for _ in 0..concurrency {
                    let mut connection =
                        mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
                    connection.query_drop(format!(
                        "SET enable_scan_datacache = {}",
                        settings.warm_cache
                    ))?;
                    connections.push(connection);
                }
                let barrier = Arc::new(Barrier::new(concurrency + 1));
                // These coarse resource timestamps enclose the existing timed
                // wave; neither clock read is inside the query/wave timers.
                let resource_window_start_ms = monitor.elapsed_millis();
                let (elapsed, samples) = thread::scope(|scope| {
                    let handles = connections
                        .into_iter()
                        .enumerate()
                        .map(|(actor, mut connection)| {
                            let barrier = barrier.clone();
                            let query = &query;
                            scope.spawn(move || {
                                barrier.wait();
                                let started = Instant::now();
                                let result =
                                    connection.query::<(u64, i64, i64, i64, i64), _>(query);
                                let elapsed = started.elapsed().as_nanos();
                                match result {
                                    Ok(rows) if rows.len() == 1 => {
                                        let (
                                            row_count,
                                            sum_row_id,
                                            sum_eq_key,
                                            sum_partition_key,
                                            sum_payload,
                                        ) = rows[0];
                                        let aggregate = Aggregate {
                                            row_count,
                                            sum_row_id,
                                            sum_eq_key,
                                            sum_partition_key,
                                            sum_payload,
                                        };
                                        Sample {
                                            actor,
                                            query_elapsed_ns: elapsed,
                                            aggregate: Some(aggregate),
                                            aggregate_sha256: Some(aggregate.digest()),
                                            matched: aggregate == expected,
                                            error_class: if aggregate == expected {
                                                None
                                            } else {
                                                Some("aggregate_mismatch")
                                            },
                                        }
                                    }
                                    Ok(_) => Sample {
                                        actor,
                                        query_elapsed_ns: elapsed,
                                        aggregate: None,
                                        aggregate_sha256: None,
                                        matched: false,
                                        error_class: Some("aggregate_shape"),
                                    },
                                    Err(_) => Sample {
                                        actor,
                                        query_elapsed_ns: elapsed,
                                        aggregate: None,
                                        aggregate_sha256: None,
                                        matched: false,
                                        error_class: Some("query_failed"),
                                    },
                                }
                            })
                        })
                        .collect::<Vec<_>>();
                    let started = Instant::now();
                    barrier.wait();
                    let samples = handles
                        .into_iter()
                        .map(|handle| {
                            handle
                                .join()
                                .map_err(|_| anyhow::anyhow!("performance actor panicked"))
                        })
                        .collect::<Result<Vec<_>>>();
                    (started.elapsed().as_nanos(), samples)
                });
                let resource_window_end_ms = monitor.elapsed_millis();
                let samples = samples?;
                let matched = samples.iter().all(|sample| sample.matched);
                let record = json!({"case":name,"snapshot":snapshot,"metadata":case["metadata"],"cache":settings.cache_name(),"side":settings.side,"pass":settings.pass,"concurrency":concurrency,"run":run,"warmup":warmup,"wave_elapsed_ns":elapsed,"resource_window_start_ms":resource_window_start_ms,"resource_window_end_ms":resource_window_end_ms,"expected":expected,"expected_aggregate_sha256":expected.digest(),"samples":samples});
                serde_json::to_writer(&mut *output, &record)?;
                output.write_all(b"\n")?;
                output.flush()?;
                correct &= matched;
                incorrect_actor_samples += samples.iter().filter(|sample| !sample.matched).count();
                if !warmup {
                    measured_ns.extend(samples.iter().map(|sample| sample.query_elapsed_ns));
                    wave_ns.push(elapsed);
                }
            }
            all_input_aggregates_match &= correct;
            if control {
                baseline_controls_match &= correct;
            }
            groups.push(json!({"case":name,"cache":settings.cache_name(),"concurrency":concurrency,
                "correct":correct,"comparable":correct,"baseline_control":control,
                "incorrect_actor_samples":incorrect_actor_samples,
                "query_sample_count":measured_ns.len(),"query_ns":summarize(&measured_ns)?,"wave_sample_count":wave_ns.len(),"wave_ns":summarize(&wave_ns)?}));
        }
    }
    Ok(
        json!({"groups":groups,"all_input_aggregates_match":all_input_aggregates_match,
        "baseline_controls_match":baseline_controls_match,
        "wrong_case_policy":"retained timings are noncomparable and cannot support a latency improvement claim"}),
    )
}

fn accepts_aggregates(settings: &Settings, summary: &Value) -> bool {
    if settings.side == "candidate" {
        summary["all_input_aggregates_match"] == true
    } else {
        summary["baseline_controls_match"] == true
    }
}

fn finish_resources(
    monitor: ProcessResourceMonitor,
    root: &Path,
    run_id: &str,
    started_wall_ns: u128,
    queries_finished_ms: Option<u128>,
    convergence_finished_ms: Option<u128>,
) -> Result<Value> {
    let envelope_path = root.join("delete-performance-resources.json");
    let mut sampler = monitor.finish(&envelope_path)?;
    // Add an explicit final reading after convergence and worker settlement.
    // This is outside every query timer and is not an RSS-based release oracle.
    let final_reading = sampler.sample_cluster();
    sampler.write_json(&envelope_path)?;
    let ended_wall_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let samples_path = root.join("delete-performance-resources.jsonl");
    let mut output = BufWriter::new(File::create(&samples_path)?);
    for sample in sampler.samples() {
        serde_json::to_writer(&mut output, sample)?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    final_reading?;
    let mut roles = Vec::new();
    for role in ["fe", "be-0", "be-1", "be-2"] {
        let samples = sampler
            .samples()
            .iter()
            .filter(|sample| sample.role == role)
            .collect::<Vec<_>>();
        ensure!(
            samples.len() >= 2,
            "insufficient process resource samples for {role}"
        );
        for sample in &samples {
            ensure!(
                sample.unavailable_reason.is_none()
                    && sample.rss_bytes.is_some()
                    && sample.cpu_user_nanos.is_some()
                    && sample.cpu_system_nanos.is_some(),
                "process resource counters unavailable for {role}: {:?}",
                sample.unavailable_reason
            );
        }
        for pair in samples.windows(2) {
            ensure!(
                pair[0].elapsed_millis <= pair[1].elapsed_millis
                    && pair[0].cpu_user_nanos <= pair[1].cpu_user_nanos
                    && pair[0].cpu_system_nanos <= pair[1].cpu_system_nanos,
                "process resource counters moved backwards for {role}"
            );
        }
        let first = samples[0];
        let last = samples[samples.len() - 1];
        roles.push(json!({
            "role":role,"pid":first.pid,"process_start_token":first.process_start_token,
            "sample_count":samples.len(),"first_sample_elapsed_ms":first.elapsed_millis,
            "last_sample_elapsed_ms":last.elapsed_millis,
            "max_sample_gap_ms":samples.windows(2).map(|pair| pair[1].elapsed_millis-pair[0].elapsed_millis).max(),
            "rss_high_water_bytes":samples.iter().filter_map(|sample| sample.rss_bytes).max(),
            "threads_high_water":samples.iter().filter_map(|sample| sample.threads).max(),
            "cpu_user_delta_nanos":last.cpu_user_nanos.unwrap()-first.cpu_user_nanos.unwrap(),
            "cpu_system_delta_nanos":last.cpu_system_nanos.unwrap()-first.cpu_system_nanos.unwrap(),
        }));
    }
    Ok(json!({
        "schema_version":1,"run_id":run_id,"sample_interval_ms":RESOURCE_SAMPLE_INTERVAL_MS,
        "clock_resolution_ms":1,"started_wall_ns":started_wall_ns,"ended_wall_ns":ended_wall_ns,
        "queries_finished_ms":queries_finished_ms,"convergence_finished_ms":convergence_finished_ms,
        "envelope_sha256":format!("{:x}",Sha256::digest(fs::read(envelope_path)?)),
        "samples_sha256":format!("{:x}",Sha256::digest(fs::read(samples_path)?)),
        "sample_count":sampler.samples().len(),"roles":roles,
        "scope":"Whole native FE/BE process RSS and cumulative CPU, including concurrent process work; not standalone delete-index memory or per-query CPU. Short waves may have no internal sample. RSS decrease is not a resource-release proof.",
        "sampling":"50 ms worker sleep after each four-role sweep; actual per-role gaps are retained. Per-role OS counter reads and before/after birth-identity checks have an observation cost on both sides. Wave timestamps are floored monotonic milliseconds from this monitor; CPU counters have OS-dependent resolution despite nanosecond units. Final samples follow convergence and monitor join."
    }))
}

fn summarize(values: &[u128]) -> Result<Value> {
    ensure!(
        !values.is_empty(),
        "cannot summarize zero performance samples"
    );
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mean = sorted.iter().map(|v| *v as f64).sum::<f64>() / sorted.len() as f64;
    let variance = sorted
        .iter()
        .map(|v| (*v as f64 - mean).powi(2))
        .sum::<f64>()
        / sorted.len() as f64;
    let p95 = (95 * sorted.len()).div_ceil(100).saturating_sub(1);
    let median = (sorted[(sorted.len() - 1) / 2] as f64 + sorted[sorted.len() / 2] as f64) / 2.0;
    Ok(
        json!({"min":sorted[0],"median":median,"p95_nearest_rank":sorted[p95],"max":sorted[sorted.len()-1],"mean":mean,"coefficient_of_variation":if mean==0.0 {0.0} else {variance.sqrt()/mean},"all_samples_retained":true}),
    )
}

fn record_objects(proxy: &DelayedS3Proxy, stop: &AtomicBool, root: &Path) -> Result<Value> {
    let path = root.join("delete-performance-objects.jsonl");
    let mut output = BufWriter::new(File::create(&path)?);
    let mut count = 0_u64;
    loop {
        for event in proxy.take_event_log()? {
            let record = json!({"record":"object","kind":format!("{:?}",event.kind),"request_id":event.request_id,"connection_id":event.connection_id,"elapsed_ms":event.elapsed_millis,"method":event.method.as_str(),"object_id":event.object_id,"range":event.range,"bytes":event.bytes});
            serde_json::to_writer(&mut output, &record)?;
            output.write_all(b"\n")?;
            count += 1;
        }
        for event in proxy.take_connection_log()? {
            serde_json::to_writer(
                &mut output,
                &json!({"record":"connection","kind":format!("{:?}",event.kind),"connection_id":event.connection_id,"elapsed_ms":event.elapsed_millis}),
            )?;
            output.write_all(b"\n")?;
        }
        output.flush()?;
        if stop.load(Ordering::Acquire) {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    let counters = proxy.snapshot();
    ensure!(
        counters.event_overflow == 0 && counters.upstream_errors == 0,
        "object evidence overflowed or object reads failed"
    );
    Ok(
        json!({"sha256":format!("{:x}",Sha256::digest(fs::read(&path)?)),"object_event_count":count,"gets":counters.gets,"heads":counters.heads,"upstream_bytes_read":counters.upstream_bytes_read,"completed_response_bytes":counters.completed_response_bytes,"peak_inflight_reads":counters.peak_inflight_reads,"event_overflow":counters.event_overflow}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wrong_baseline_noncontrols_never_make_candidate_or_control_errors_acceptable() {
        let mut settings = Settings {
            warm_cache: false,
            side: "baseline".into(),
            pass: 1,
        };
        let noncontrol_error =
            json!({"all_input_aggregates_match":false,"baseline_controls_match":true});
        assert!(accepts_aggregates(&settings, &noncontrol_error));
        settings.side = "candidate".into();
        assert!(!accepts_aggregates(&settings, &noncontrol_error));
        settings.side = "baseline".into();
        assert!(!accepts_aggregates(
            &settings,
            &json!({"all_input_aggregates_match":false,"baseline_controls_match":false})
        ));
    }
    #[test]
    fn raw_sample_summary_keeps_fixed_nearest_rank_and_count_sum_digest() {
        let values = [9, 1, 8, 2, 7, 3, 6, 4, 5];
        let summary = summarize(&values).unwrap();
        assert_eq!(summary["median"], 5.0);
        assert_eq!(summary["p95_nearest_rank"], 9);
        let aggregate = Aggregate {
            row_count: 1,
            sum_row_id: 2,
            sum_eq_key: 3,
            sum_partition_key: 4,
            sum_payload: 5,
        };
        let mut different = aggregate;
        different.sum_eq_key = 4;
        assert_ne!(aggregate.digest(), different.digest());
        assert!(summarize(&[]).is_err());
    }
}
