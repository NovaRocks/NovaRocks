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

use super::result_delivery::root_census;
use super::result_delivery_support::await_idle;
use crate::actors::mysql_stream::{AsyncMysqlStream, TextColumnObservation};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    topology: String,
    scope: String,
    segment_bytes: u64,
    client_receive_buffer_bytes: u32,
    max_applied_receive_buffer_bytes: u32,
    mysql_client_max_packet_bytes: u32,
    query_observation_ms: u64,
    metadata_wait_ms: u64,
    held_phase_ms: u64,
    closing_phase_ms: u64,
    phase_sample_interval_ms: u64,
    phase_max_samples: usize,
    expected_mysql_error_code: u16,
    health: Health,
    cases: Vec<Case>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Health {
    sql: String,
    expected_row_sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    mode: String,
    sql: String,
    native_row_bytes: u64,
    expected_columns: u64,
    expected_schema: Vec<TextColumnObservation>,
    expected_rows: u64,
    expected_row_payload_bytes: u64,
    expected_packets: u64,
    expected_row_sha256: String,
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(CancelRows(
            "result-delivery/unread-row-cancel-closing-window",
        )),
        Box::new(CancelRows(
            "result-delivery/partial-large-row-cancel-poisons-socket",
        )),
    ]
}
struct CancelRows(&'static str);

impl Scenario for CancelRows {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery_closing",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        self.0
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "row cancellation requires native 1FE+3BE"
        );
        let manifest: Manifest = serde_json::from_str(include_str!(
            "../../../../docs/testing/mem-1-m07/inputs/root-cancel-closing-freeze-v1.json"
        ))?;
        ensure!(
            manifest.schema_version == 1
                && manifest.topology == "1FE+3BE"
                && !manifest.scope.is_empty()
                && manifest.segment_bytes == 1048576
                && manifest.query_observation_ms == 20000
                && manifest.metadata_wait_ms == 5000
                && manifest.held_phase_ms == 5000
                && manifest.closing_phase_ms == 2000
                && manifest.phase_sample_interval_ms == 100
                && manifest.phase_max_samples == 51
                && manifest.expected_mysql_error_code == 1317,
            "unsupported cancellation freeze"
        );
        let case = manifest
            .cases
            .iter()
            .find(|case| case.name == self.0)
            .context("missing frozen cancel case")?;
        let resident = case.mode == "resident";
        ensure!(
            resident || case.mode == "missing-tail",
            "unknown cancellation mode"
        );
        ensure!(
            case.native_row_bytes == case.expected_row_payload_bytes + 4,
            "invalid frozen native row length"
        );
        let epoch = Instant::now();
        await_idle(context, "row-cancel", "before", epoch)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let mut stream = runtime.block_on(AsyncMysqlStream::connect_with_receive_buffer(
            context.mysql_user(),
            context.mysql_port(),
            Duration::from_millis(manifest.query_observation_ms),
            manifest.mysql_client_max_packet_bytes,
            manifest.client_receive_buffer_bytes,
        ))?;
        let connection = stream.connection_id()?;
        let applied = stream
            .receive_buffer_bytes()
            .context("missing applied client receive buffer")?;
        ensure!(
            applied > 0 && applied <= manifest.max_applied_receive_buffer_bytes,
            "invalid applied receive buffer"
        );
        let (ready, metadata_ready) = tokio::sync::oneshot::channel();
        let (resume, resume_read) = tokio::sync::oneshot::channel();
        let sql = case.sql.clone();
        let job = runtime.spawn(async move {
            let observation = stream
                .observe_text_query_with_metadata_pause(
                    &sql,
                    Duration::ZERO,
                    Some((ready, resume_read)),
                )
                .await;
            (stream, observation)
        });
        let mut samples = Vec::new();
        let mut timing = serde_json::Map::new();
        let held = (|| -> Result<()> {
            runtime
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_millis(manifest.metadata_wait_ms),
                        metadata_ready,
                    )
                    .await
                })
                .context("cancel metadata deadline")??;
            let deadline = Instant::now() + Duration::from_millis(manifest.held_phase_ms);
            let ports: Vec<_> = context
                .handle()
                .runtime()
                .be
                .iter()
                .map(|be| be.http)
                .collect();
            let mut consecutive = 0;
            loop {
                context.remaining("cancel held root census")?;
                ensure!(
                    Instant::now() < deadline && samples.len() < manifest.phase_max_samples,
                    "root did not reach frozen held cancellation phase"
                );
                ensure!(
                    !job.is_finished(),
                    "client actor exited before root cancellation"
                );
                let mut roots = Vec::new();
                for port in &ports {
                    let client = reqwest::blocking::Client::builder()
                        .timeout(
                            deadline
                                .saturating_duration_since(Instant::now())
                                .min(Duration::from_secs(1)),
                        )
                        .build()?;
                    let rows: serde_json::Value = client
                        .get(format!("http://127.0.0.1:{port}/metrics?type=json"))
                        .send()?
                        .error_for_status()?
                        .json()?;
                    roots.push(root_census(
                        rows.as_array().context("invalid root census array")?,
                    )?);
                }
                let occupied: Vec<_> = roots
                    .iter()
                    .filter_map(Option::as_ref)
                    .filter(|root| root["channels"] != 0)
                    .collect();
                let common = roots.iter().all(Option::is_some)
                    && occupied.len() == 1
                    && occupied[0]["channels"] == 1
                    && occupied[0]["data_positions"] == 2
                    && occupied[0]["ends_acknowledged"] == 0
                    && occupied[0]["sealed"] == 0;
                let matches = common
                    && if resident {
                        occupied[0]["terminal_task_records"] == 1
                            && occupied[0]["producers_exited"] == 1
                            && occupied[0]["producers_running"] == 0
                            && occupied[0]["ends_published"] == 1
                            && occupied[0]["payload_bytes"] == case.native_row_bytes
                            && occupied[0]["segments"] == 2
                    } else {
                        occupied[0]["producers_running"] == 1
                            && occupied[0]["producers_exited"] == 0
                            && occupied[0]["ends_published"] == 0
                            && occupied[0]["payload_bytes"] == 2 * manifest.segment_bytes
                            && occupied[0]["segments"] >= 2
                    };
                samples.push(serde_json::json!({"elapsed_micros":epoch.elapsed().as_micros(),
                    "connection_id":connection,"applied_receive_buffer_bytes":applied,"roots":roots,"matches":matches}));
                std::fs::write(
                    context.scenario_root().join("cancel-held-root-census.json"),
                    serde_json::to_vec_pretty(&samples)?,
                )?;
                ensure!(
                    Instant::now() < deadline,
                    "cancel census exceeded absolute deadline"
                );
                consecutive = if matches { consecutive + 1 } else { 0 };
                if consecutive == 2 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(manifest.phase_sample_interval_ms));
            }
            let kill_deadline = Instant::now()
                + context
                    .remaining("cancel exact connection")?
                    .min(Duration::from_secs(2));
            timing.insert(
                "kill_started_micros".into(),
                serde_json::json!(epoch.elapsed().as_micros()),
            );
            let kill_result = context.handle().kill_query_until(connection, kill_deadline);
            timing.insert(
                "kill_returned_micros".into(),
                serde_json::json!(epoch.elapsed().as_micros()),
            );
            std::fs::write(
                context.scenario_root().join("cancel-timing.json"),
                serde_json::to_vec_pretty(&timing)?,
            )?;
            kill_result?;
            ensure!(
                Instant::now() < kill_deadline,
                "KILL QUERY returned after the frozen observation deadline"
            );
            if resident {
                let deadline = Instant::now() + Duration::from_millis(manifest.closing_phase_ms);
                let mut closing = Vec::new();
                loop {
                    context.remaining("observe independent Closing position")?;
                    ensure!(
                        Instant::now() < deadline && closing.len() < 21,
                        "Closing position was never held independently of Client"
                    );
                    ensure!(
                        !job.is_finished(),
                        "paused actor exited before Closing observation"
                    );
                    let response = context.handle().frontend_management_get(
                        "/v1/frontend/state",
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_secs(1)),
                    )?;
                    ensure!(response.status == 200, "closing state observation failed");
                    let state: serde_json::Value = serde_json::from_str(&response.body)?;
                    let positions = state["workload"]["governance"]["result_window_positions"]
                        .as_array()
                        .context("missing result window positions")?;
                    ensure!(positions.len() == 4, "invalid result window dimensions");
                    let positions = positions
                        .iter()
                        .map(|value| value.as_u64().context("invalid result window position"))
                        .collect::<Result<Vec<_>>>()?;
                    let matches = positions == [0, 0, 0, 1]; // Client, Local, Internal, Closing.
                    closing.push(serde_json::json!({"elapsed_micros":epoch.elapsed().as_micros(),"positions":positions,"matches":matches}));
                    std::fs::write(
                        context.scenario_root().join("cancel-closing-window.json"),
                        serde_json::to_vec_pretty(&closing)?,
                    )?;
                    ensure!(
                        Instant::now() < deadline,
                        "Closing observation exceeded absolute deadline"
                    );
                    if matches {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(manifest.phase_sample_interval_ms));
                }
            }
            Ok(())
        })();
        timing.insert(
            "resume_sent_micros".into(),
            serde_json::json!(epoch.elapsed().as_micros()),
        );
        let _ = resume.send(());
        let timing_write = serde_json::to_vec_pretty(&timing)
            .map_err(anyhow::Error::from)
            .and_then(|bytes| {
                std::fs::write(context.scenario_root().join("cancel-timing.json"), bytes)
                    .map_err(anyhow::Error::from)
            });
        let (mut stream, observation) =
            runtime.block_on(job).context("join canceled row client")?;
        std::fs::write(
            context.scenario_root().join("cancel-row-wire.json"),
            serde_json::to_vec_pretty(&observation)?,
        )?;
        timing_write?;
        held?;
        ensure!(
            observation.columns == case.expected_columns
                && observation.schema == case.expected_schema,
            "canceled row schema differs from frozen oracle"
        );
        if resident {
            ensure!(
                observation.rows == case.expected_rows
                    && observation.row_payload_bytes == case.expected_row_payload_bytes
                    && observation.packets == case.expected_packets
                    && observation.row_sha256 == case.expected_row_sha256
                    && observation
                        .error
                        .as_deref()
                        .is_some_and(|error| error.starts_with("server result error code 1317:")),
                "unread row did not complete before exact cancellation ERR"
            );
        } else {
            ensure!(
                observation.rows == 0
                    && observation.row_payload_bytes > 0
                    && observation
                        .error
                        .as_deref()
                        .is_some_and(|error| error.contains("truncated server response")
                            || error.contains("Connection reset by peer")),
                "incomplete row was not physically truncated and socket poisoned"
            );
        }
        if !resident {
            await_idle(context, "row-cancel", "after-target", epoch)?;
        }
        let before = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<Vec<_>>>()?;
        let health =
            runtime.block_on(stream.observe_text_query(&manifest.health.sql, Duration::ZERO));
        std::fs::write(
            context
                .scenario_root()
                .join("cancel-same-socket-follow-up.json"),
            serde_json::to_vec_pretty(&health)?,
        )?;
        await_idle(context, "row-cancel", "after-follow-up", epoch)?;
        let after = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<Vec<_>>>()?;
        std::fs::write(
            context
                .scenario_root()
                .join("cancel-follow-up-native-tasks.json"),
            serde_json::to_vec_pretty(&serde_json::json!({"before": before, "after": after}))?,
        )?;
        ensure!(
            before
                .iter()
                .zip(&after)
                .all(|(a, b)| a.is_finite() && b.is_finite() && b >= a),
            "invalid follow-up native task counters"
        );
        if resident {
            ensure!(
                before.iter().zip(&after).any(|(a, b)| b > a),
                "reused socket did not create a native task"
            );
            ensure!(
                health.error.is_none()
                    && health.rows == 1
                    && health.row_payload_bytes == 5
                    && health.packets == 5
                    && health.row_sha256 == manifest.health.expected_row_sha256
                    && health.schema
                        == [TextColumnObservation {
                            name: "total".to_owned(),
                            mysql_type: 8
                        }],
                "same socket was not reusable after closing cancellation ERR"
            );
        } else {
            ensure!(
                transport_closed(health.error.as_deref())
                    && health.rows == 0
                    && health.columns == 0
                    && health.schema.is_empty()
                    && health.packets == 0
                    && health.row_payload_bytes == 0
                    && health.wire_bytes == 0
                    && before == after,
                "poisoned socket follow-up was not refused before native task creation"
            );
        }
        drop(stream);
        await_idle(context, "row-cancel", "after", epoch)?;
        context.record_phase_observation(
            "native-row-cancel",
            1,
            1,
            1,
            "public-mysql-native-root",
            1,
            "passed",
            BTreeMap::from([
                ("rows", observation.rows),
                ("same_socket_reused", u64::from(resident)),
                ("held_census_samples", u64::try_from(samples.len())?),
            ]),
        )?;
        Ok(())
    }
}

// Accept only physical EOF/socket errors, never a protocol or probe deadline error.
fn transport_closed(error: Option<&str>) -> bool {
    error.is_some_and(|error| {
        [
            "truncated server response",
            "Connection reset by peer",
            "Broken pipe",
            "Socket is not connected",
        ]
        .iter()
        .any(|reason| error.contains(reason))
    })
}

#[cfg(test)]
mod tests {
    use super::transport_closed;

    #[test]
    fn follow_up_requires_a_physical_transport_failure() {
        for error in [
            "write async MySQL COM_QUERY packet: Broken pipe (os error 32)",
            "read async MySQL packet header: truncated server response",
            "Connection reset by peer",
        ] {
            assert!(transport_closed(Some(error)));
        }
        for error in [
            "absolute query deadline exceeded",
            "invalid metadata EOF",
            "server result error code 1317: interrupted",
        ] {
            assert!(!transport_closed(Some(error)));
        }
        assert!(!transport_closed(None));
    }
}
