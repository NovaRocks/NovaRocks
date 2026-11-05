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

//! Native process observation; physical query/R1 attribution remains unwired.
//! Real account lifecycle lane records exist independently of physical attribution.
use crate::actors::{mysql as mysql_actor, mysql_stream::MysqlStream};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use crate::scenarios::native_sql_controls::{SqlIntentOwner, StatementSpec};
use crate::scenarios::{query_lifecycle, task_evidence};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_memory::lane::{MAX_RECORDS, REGION_LABELS, UNATTRIBUTED_RECORDS};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    thread,
    time::{Duration, Instant},
};
const OBSERVATION_QUERIES: [&str; 2] = [
    "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v",
    "SELECT SUM(i) FROM TABLE(generate_series(1, 1000)) AS t(i)",
];

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(ObservationFamilies::default())]
}
#[derive(Default)]
struct ObservationFamilies {
    intent: SqlIntentOwner,
}
impl Scenario for ObservationFamilies {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "memory_attribution",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "memory-attribution/observation-families"
    }
    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let statements = OBSERVATION_QUERIES
            .iter()
            .map(|sql| StatementSpec {
                sql: (*sql).into(),
                faults: vec![],
                observations: (0..cluster_size)
                    .map(|index| (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), None))
                    .collect(),
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
        ensure!(
            context.handle().be_count() == 3,
            "memory attribution acceptance requires native 1FE+3BE"
        );
        let ids = context.process_ids();
        ensure!(
            ids.backends.iter().copied().collect::<BTreeSet<_>>().len() == 3,
            "backend observation must come from three independent processes"
        );
        let baseline = query_lifecycle::resource_snapshot(context)?;
        let mut connection = mysql_actor::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        for (ordinal, (sql, expected)) in [
            (OBSERVATION_QUERIES[0], vec![1, 2]),
            (OBSERVATION_QUERIES[1], vec![500500]),
        ]
        .into_iter()
        .enumerate()
        {
            let observation =
                self.intent
                    .take(ordinal)?
                    .arm(context, connection.connection_id(), sql)?;
            let rows: Result<Vec<i64>> = connection
                .query(sql)
                .context("execute distributed observation query");
            let result = (|| -> Result<()> {
                let rows = rows?;
                ensure!(
                    rows == expected,
                    "observation workload returned unexpected rows: {rows:?}"
                );
                let deadline = context.deadline();
                let terminal = context
                    .handle()
                    .await_query_lifecycle_statement_observation(&observation, true, deadline)?;
                task_evidence::assert_query_completed_across_boundary(
                    context,
                    &terminal,
                    "memory observation workload",
                )?;
                Ok(())
            })();
            let cleanup = context
                .handle()
                .drain_query_lifecycle_statement_observation(&observation);
            match (result, cleanup) {
                (Ok(()), Ok(())) => {}
                (Err(primary), Err(cleanup)) => {
                    anyhow::bail!("{primary:#}; memory statement cleanup: {cleanup:#}")
                }
                (Err(error), _) | (_, Err(error)) => return Err(error),
            }
        }
        connection.finish()?;
        query_lifecycle::await_resource_convergence(context, &baseline)?;
        let stream = MysqlStream::query(
            context.mysql_user(),
            context.mysql_port(),
            "SELECT v FROM (SELECT sleep(10) AS v UNION ALL SELECT sleep(10)) t ORDER BY v",
            context
                .remaining("start cancellation workload")?
                .min(Duration::from_secs(10)),
        )?;
        loop {
            if query_lifecycle::resource_snapshot(context)? != baseline {
                break;
            }
            thread::sleep(
                context
                    .remaining("observe active cancellation workload")?
                    .min(Duration::from_millis(50)),
            );
        }
        let during = scrape_cluster(context)?;
        stream.shutdown()?;
        context.action("canceled an active distributed observation query by closing its public MySQL connection");
        query_lifecycle::await_resource_convergence(context, &baseline)?;
        let after = scrape_cluster(context)?;
        fs::write(
            context.scenario_root().join("memory-attribution.json"),
            serde_json::to_vec_pretty(
                &serde_json::json!({"schema_version":2,"physical_attribution_wiring":false,"during_query":during,"after_cancel":after}),
            )?,
        )?;
        context.action("verified both process bands, finite lifecycle lane records, zero unwired query/residual/service physical attribution and lifecycle faults on each independent BE");
        Ok(())
    }
}
#[derive(Serialize)]
struct Sample {
    backend_index: usize,
    process_id: u32,
    http_port: u16,
    values: BTreeMap<String, f64>,
}
fn scrape_cluster(context: &mut ScenarioContext) -> Result<Vec<Sample>> {
    let ports = context
        .handle()
        .runtime()
        .be
        .iter()
        .map(|be| be.http)
        .collect::<Vec<_>>();
    ensure!(
        ports.iter().copied().collect::<BTreeSet<_>>().len() == 3,
        "backend metrics endpoints must be independent"
    );
    let ids = context.process_ids();
    let client = reqwest::blocking::Client::builder()
        .timeout(
            context
                .remaining("scrape attribution metrics")?
                .min(Duration::from_secs(3)),
        )
        .build()?;
    ports
        .into_iter()
        .enumerate()
        .map(|(index, port)| {
            let body = client
                .get(format!("http://127.0.0.1:{port}/metrics"))
                .send()?
                .error_for_status()?
                .text()?;
            let mut values = BTreeMap::new();
            let mut check = |name: &str, labels: &[(&str, &str)], zero: bool| -> Result<f64> {
                let value = sample_value(&body, name, labels)?;
                ensure!(
                    value.is_finite() && value >= 0.0,
                    "invalid observation sample {name}: {value}"
                );
                if zero {
                    ensure!(
                        value == 0.0,
                        "unwired/lifecycle sample {name} {labels:?} must be zero, got {value}"
                    );
                }
                values.insert(format!("{name}{labels:?}"), value);
                Ok(value)
            };
            for band in ["small", "tagged"] {
                check(
                    "novarocks_backend_process_counted_live_bytes",
                    &[("band", band)],
                    false,
                )?;
                for kind in ["alloc", "dealloc", "realloc", "failure"] {
                    check(
                        "novarocks_backend_process_counted_operations_total",
                        &[("band", band), ("kind", kind)],
                        false,
                    )?;
                }
                for flow in ["allocated", "deallocated"] {
                    check(
                        "novarocks_backend_process_counted_requested_bytes_total",
                        &[("band", band), ("flow", flow)],
                        false,
                    )?;
                }
            }
            for class in ["query", "residual", "service"] {
                for band in ["tagged", "r1_small"] {
                    check(
                        "novarocks_backend_memory_attributed_bytes",
                        &[("band", band), ("class", class)],
                        true,
                    )?;
                }
                // Domain creation owns a real lifecycle record even when no
                // physical allocation is attributed through that lane. These
                // bounded-prefix samples are not a quiescent leak oracle.
                for production in ["producing", "sealed", "stopped"] {
                    check(
                        "novarocks_backend_memory_lane_records",
                        &[("class", class), ("production", production)],
                        false,
                    )?;
                }
            }
            for production in ["producing", "sealed", "stopped"] {
                let count = check(
                    "novarocks_backend_memory_lane_records",
                    &[("class", "unattributed"), ("production", production)],
                    false,
                )?;
                ensure!(
                    count
                        == if production == "producing" {
                            f64::from(UNATTRIBUTED_RECORDS)
                        } else {
                            0.0
                        },
                    "immortal unattributed record states changed"
                );
            }
            let unattr = check(
                "novarocks_backend_memory_unattributed_bytes",
                &[("role", "unknown")],
                false,
            )?;
            ensure!(
                unattr > 0.0,
                "active wrapped backend allocations must report unattributed bytes"
            );
            for kind in [
                "binding_failure",
                "record_exhaustion",
                "orphan",
                "residual_growth",
                "scope_refusal",
                "reclaim_nonzero",
                "generation_exhaustion",
            ] {
                check(
                    "novarocks_backend_memory_attribution_faults_total",
                    &[("kind", kind)],
                    true,
                )?;
            }
            let capacity = check("novarocks_backend_memory_lane_record_capacity", &[], false)?;
            ensure!(
                capacity == f64::from(MAX_RECORDS),
                "record capacity changed from frozen manifest"
            );
            let mut dynamic_capacity = 0.0;
            for region in REGION_LABELS {
                dynamic_capacity += check(
                    "novarocks_backend_memory_lane_record_region_capacity",
                    &[("region", region)],
                    false,
                )?;
                for state in ["active", "residual_retained", "pending_reclaim"] {
                    check(
                        "novarocks_backend_memory_lane_record_region",
                        &[("region", region), ("state", state)],
                        false,
                    )?;
                }
            }
            ensure!(
                dynamic_capacity + f64::from(UNATTRIBUTED_RECORDS) == capacity,
                "isolated dynamic regions and immortal records must account for all record positions"
            );
            ensure!(
                check("novarocks_backend_memory_batch_threshold_bytes", &[], false)? == 1048576.0,
                "batch threshold changed from frozen manifest"
            );
            for name in [
                "lane_record_high_water",
                "lane_records_draining",
                "lane_record_segment_requested_bytes",
                "observation_metadata_bytes",
                "batch_pinned_slots",
                "batch_slot_balance_estimate_bytes",
                "attribution_sample_unixtime_seconds",
                "attribution_sequence_sum",
            ] {
                check(&format!("novarocks_backend_memory_{name}"), &[], false)?;
            }
            // Reconciliation and blind-spot samples are signed, independently read
            // fields. Concurrent in-flight observations cannot require equality.
            for name in [
                "novarocks_backend_memory_attribution_reconcile_bytes",
                "novarocks_backend_memory_ledger_blind_spot_bytes",
            ] {
                let value = sample_value(&body, name, &[])?;
                ensure!(value.is_finite(), "nonfinite signed observation");
                values.insert(name.to_string(), value);
            }
            for region in REGION_LABELS {
                for band in ["tagged", "r1_small"] {
                    let name = "novarocks_backend_memory_lane_record_region_residual_bytes";
                    let labels = [("band", band), ("region", region)];
                    let value = sample_value(&body, name, &labels)?;
                    ensure!(value.is_finite(), "nonfinite signed residual observation");
                    values.insert(format!("{name}{labels:?}"), value);
                }
            }
            Ok(Sample {
                backend_index: index,
                process_id: ids.backends[index],
                http_port: port,
                values,
            })
        })
        .collect()
}
fn sample_value(body: &str, name: &str, labels: &[(&str, &str)]) -> Result<f64> {
    let mut matches = body
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (key, value) = line.split_once(' ')?;
            let metric = key.split('{').next()?;
            if metric != name {
                return None;
            }
            let encoded = key.strip_prefix(name).unwrap_or_default();
            let actual = if encoded.is_empty() {
                BTreeMap::new()
            } else {
                encoded
                    .strip_prefix('{')?
                    .strip_suffix('}')?
                    .split(',')
                    .map(|pair| {
                        let (key, value) = pair.split_once('=')?;
                        Some((key, value.strip_prefix('"')?.strip_suffix('"')?))
                    })
                    .collect::<Option<BTreeMap<_, _>>>()?
            };
            if actual.len() == labels.len()
                && labels
                    .iter()
                    .all(|(label, value)| actual.get(label) == Some(value))
            {
                Some(value.trim().parse::<f64>())
            } else {
                None
            }
        });
    let value = matches
        .next()
        .with_context(|| format!("missing sample {name} {labels:?}"))??;
    ensure!(
        matches.next().is_none(),
        "ambiguous sample {name} {labels:?}"
    );
    Ok(value)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selectors_match_exact_family_and_all_requested_labels() {
        let body = "# HELP x help\nx{band=\"tagged\",kind=\"allocation\"} 2\nx{band=\"small\",kind=\"allocation\"} 3\ny 7\n";
        assert_eq!(
            sample_value(body, "x", &[("kind", "allocation"), ("band", "small")]).unwrap(),
            3.0
        );
        assert!(sample_value(body, "x", &[]).is_err());
        assert!(sample_value(body, "missing", &[]).is_err());
        assert!(sample_value("x{source_band=\"small\"} 1", "x", &[("band", "small")]).is_err());
        assert!(
            sample_value(
                "x{band=\"small\",query_id=\"1\"} 1",
                "x",
                &[("band", "small")]
            )
            .is_err()
        );
    }
}
