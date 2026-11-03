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

//! Independent requests progress while a real result consumer holds a full Data position window.

use super::result_delivery::root_census;
use super::result_delivery_concurrency::{
    assert_single_column_rows, expected_single_column_rows, retain_failure,
};
use super::result_delivery_support::await_idle;
use crate::actors::mysql_stream::{AsyncMysqlStream, ControlledPayloadReads, PayloadReadPermit};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_result_contract::RootProfileV1;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

const RECEIVE_BUFFER: u32 = 4096;
const MAX_APPLIED_RECEIVE_BUFFER: u32 = 65536;
const ROWS: u64 = 8192;
const BASE_CELL_BYTES: usize = 1024;

#[derive(Clone, Copy)]
enum Mode {
    Slow,
    Unread,
}

struct ClientProgress(Mode);

pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(ClientProgress(Mode::Slow)),
        Box::new(ClientProgress(Mode::Unread)),
    ]
}

impl Scenario for ClientProgress {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery_progress",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        match self.0 {
            Mode::Slow => "result-delivery/slow-client-progress",
            Mode::Unread => "result-delivery/unread-client-progress",
        }
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "client progress requires native 1FE+3BE"
        );
        let epoch = Instant::now();
        await_idle(context, "client-progress", "before", epoch)?;
        // This is a hang-prevention budget, never a response-time criterion.
        let deadline = Instant::now()
            + context
                .remaining("held client progress")?
                .min(Duration::from_secs(90));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let mut stream = runtime.block_on(async {
            tokio::time::timeout_at(
                deadline.into(),
                AsyncMysqlStream::connect_with_receive_buffer(
                    context.mysql_user(),
                    context.mysql_port(),
                    deadline.saturating_duration_since(Instant::now()),
                    64 * 1024 * 1024,
                    RECEIVE_BUFFER,
                ),
            )
            .await
            .context("held client connect deadline")?
        })?;
        let connection = stream.connection_id()?;
        let applied = stream
            .receive_buffer_bytes()
            .context("client receive buffer was not observed")?;
        ensure!(
            applied > 0 && applied <= MAX_APPLIED_RECEIVE_BUFFER,
            "OS receive buffer does not enforce a small client window"
        );
        let (ready, metadata_ready) = tokio::sync::oneshot::channel();
        let (resume_metadata, resume_read) = tokio::sync::oneshot::channel();
        let mut resume_metadata = Some(resume_metadata);
        let (permits, permit_receiver) = tokio::sync::mpsc::channel(2);
        let (progress, mut payload_progress) = tokio::sync::mpsc::unbounded_channel();
        let mode = self.0;
        let sql = format!(
            "SELECT REPEAT('q', {BASE_CELL_BYTES} + generate_series) AS payload FROM generate_series(1, {ROWS}) ORDER BY generate_series"
        );
        let job = runtime.spawn(async move {
            match mode {
                Mode::Slow => {
                    stream
                        .observe_text_query_with_controlled_reads(
                            &sql,
                            Some((ready, resume_read)),
                            ControlledPayloadReads {
                                permits: permit_receiver,
                                progress,
                            },
                            deadline,
                        )
                        .await
                }
                Mode::Unread => {
                    stream
                        .observe_text_query_owned_until(
                            &sql,
                            Duration::ZERO,
                            Some((ready, resume_read)),
                            deadline,
                        )
                        .await
                }
            }
        });
        let mut census = Vec::new();
        let mut independent = Vec::new();
        let mut actual_payload_bytes = 0u64;
        let mut control_completed = false;
        let mut causal = (|| -> Result<()> {
            runtime.block_on(async {
                tokio::time::timeout_at(deadline.into(), metadata_ready).await.context("held client metadata deadline")?
                    .context("held client exited before metadata pause")
            })?;
            await_saturated_root(context, deadline, epoch, "metadata-paused", &mut census, || !job.is_finished())?;
            if matches!(mode, Mode::Slow) {
                resume_metadata.take().context("metadata gate already released")?.send(())
                    .map_err(|_| anyhow::anyhow!("slow actor exited before metadata release"))?;
                runtime.block_on(async {
                    permits.send(PayloadReadPermit::Chunk).await.context("slow payload controller disappeared")?;
                    actual_payload_bytes = tokio::time::timeout_at(deadline.into(), payload_progress.recv()).await
                        .context("slow controlled payload read deadline")?.context("slow actor did not report real payload progress")?;
                    Ok::<_, anyhow::Error>(())
                })?;
                ensure!(actual_payload_bytes > 0 && actual_payload_bytes <= 65536,
                    "slow client did not perform exactly one bounded real row read");
                await_saturated_root(context, deadline, epoch, "payload-paused", &mut census, || !job.is_finished())?;
            }
            for (sql, expected) in [
                ("SELECT 1 AS value", "1"),
                ("SELECT SUM(generate_series) AS value FROM generate_series(1, 100)", "5050"),
            ] {
                let observed = runtime.block_on(async {
                    let mut client = tokio::time::timeout_at(deadline.into(), AsyncMysqlStream::connect(
                        context.mysql_user(), context.mysql_port(), deadline.saturating_duration_since(Instant::now()),
                    )).await.context("independent query connect deadline")??;
                    Ok::<_, anyhow::Error>(client.observe_text_query_owned_until(sql, Duration::ZERO, None, deadline).await)
                })?;
                let actual_failure = observed.actual_failure.as_ref().map(|error| format!("{error:#}"));
                let check = assert_single_column_rows(&observed.observation, "value", [expected.to_owned()]);
                independent.push(json!({"sql":sql,"expected":expected,"wire":observed.observation,"actual_failure":actual_failure}));
                check?;
                await_saturated_root(context, deadline, epoch, "independent-query-completed", &mut census, || !job.is_finished())?;
            }
            // The control request is itself independent of the held result socket.
            context.handle().kill_query_until(connection, deadline)?;
            control_completed = true;
            Ok(())
        })().err();
        let mut cleanup = Vec::new();
        if !control_completed
            && !job.is_finished()
            && let Err(error) = context.handle().kill_query_until(connection, deadline)
        {
            cleanup.push(format!("held client cleanup KILL: {error:#}"));
        }
        // Every gate is released before join, even if a census/probe/control failed.
        if let Some(resume) = resume_metadata.take() {
            let _ = resume.send(());
        }
        if matches!(mode, Mode::Slow)
            && let Err(error) = permits.try_send(PayloadReadPermit::Drain)
        {
            // Closed means the actor already exited; Full is a cleanup defect.
            if matches!(error, tokio::sync::mpsc::error::TrySendError::Full(_)) {
                cleanup.push("payload Drain command could not be published".to_owned());
            }
        }
        drop(permits);
        let mut wire = Value::Null;
        let mut actual_wire_failure = None;
        match runtime.block_on(job) {
            Ok(observed) => {
                actual_wire_failure = observed
                    .actual_failure
                    .as_ref()
                    .map(|error| format!("{error:#}"));
                let physical_close = actual_wire_failure.as_deref().is_some_and(|error| {
                    [
                        "truncated server response",
                        "Connection reset by peer",
                        "Broken pipe",
                        "Socket is not connected",
                    ]
                    .iter()
                    .any(|reason| error.contains(reason))
                });
                let check = (|| -> Result<()> {
                    ensure!(
                        observed.observation.columns == 1
                            && observed.observation.schema.len() == 1
                            && observed.observation.schema[0].name == "payload"
                            && observed.observation.schema[0].mysql_type == 253,
                        "held client metadata differs from declared query"
                    );
                    ensure!(
                        control_completed
                            && (observed.server_result_error_code == Some(1317) || physical_close),
                        "held actor did not physically close or receive ERR1317 after independent KILL: {actual_wire_failure:?}"
                    );
                    ensure!(
                        observed.observation.rows < ROWS,
                        "held client unexpectedly completed all declared rows before cancellation"
                    );
                    let (complete_rows, complete_bytes, prefix_digest) =
                        expected_single_column_rows(
                            (1..=observed.observation.rows)
                                .map(|row| "q".repeat(BASE_CELL_BYTES + row as usize)),
                        );
                    ensure!(
                        observed.observation.rows == complete_rows
                            && observed.observation.row_payload_bytes >= complete_bytes
                            && observed.observation.row_sha256 == prefix_digest,
                        "cancelled client complete-row prefix differs from independent declaration"
                    );
                    if matches!(mode, Mode::Slow) {
                        ensure!(
                            observed.observation.row_payload_bytes >= actual_payload_bytes,
                            "final wire observation lost controlled payload prefix"
                        );
                    }
                    Ok(())
                })();
                if let Err(error) = check {
                    retain_failure(&mut causal, &mut cleanup, error);
                }
                wire = serde_json::to_value(observed.observation)?;
            }
            Err(error) => retain_failure(&mut causal, &mut cleanup, error.into()),
        }
        if let Err(error) = await_idle(context, "client-progress", "after-cancel", epoch) {
            retain_failure(&mut causal, &mut cleanup, error);
        }
        // New request after socket exit independently checks the cluster remains healthy.
        let health = runtime.block_on(async {
            let timeout = context
                .remaining("post-cancel health")?
                .min(Duration::from_secs(15));
            let mut client =
                AsyncMysqlStream::connect(context.mysql_user(), context.mysql_port(), timeout)
                    .await?;
            Ok::<_, anyhow::Error>(
                client
                    .observe_text_query("SELECT 1 AS value", Duration::ZERO)
                    .await,
            )
        });
        let mut health_wire = Value::Null;
        match health {
            Ok(observed) => {
                if let Err(error) = assert_single_column_rows(&observed, "value", ["1".to_owned()])
                {
                    retain_failure(&mut causal, &mut cleanup, error);
                }
                health_wire = serde_json::to_value(observed)?;
            }
            Err(error) => retain_failure(&mut causal, &mut cleanup, error),
        }
        if let Err(error) = await_idle(context, "client-progress", "after-health", epoch) {
            retain_failure(&mut causal, &mut cleanup, error);
        }
        std::fs::write(
            context.scenario_root().join("client-progress.json"),
            serde_json::to_vec_pretty(&json!({
                "schema_version":1,"mode":if matches!(mode,Mode::Slow) {"slow"} else {"unread"},
            "execution_budget_seconds":90,"state_observation_budget_seconds":30,
                "connection_id":connection,"requested_receive_buffer_bytes":RECEIVE_BUFFER,"applied_receive_buffer_bytes":applied,
                "declared_rows":ROWS,"base_cell_bytes":BASE_CELL_BYTES,
            "row_formula":"q repeated (1024 + row), row=1..8192 in ascending order",
            "controlled_row_payload_bytes":actual_payload_bytes,
                "root_capacity":{"data_positions":RootProfileV1::DATA_POSITIONS,"segment_bytes":RootProfileV1::SEGMENT_BYTES,
                    "payload_bytes_ceiling":RootProfileV1::DATA_POSITIONS*RootProfileV1::SEGMENT_BYTES},
                "census":census,"independent_queries":independent,"control_completed":control_completed,
                "actor_wire":wire,"actor_actual_failure":actual_wire_failure,"post_cancel_health":health_wire,
                "causal_failure":causal.as_ref().map(|error| format!("{error:#}")),"cleanup_failures":cleanup,
            }))?,
        )?;
        if let Some(error) = causal {
            return Err(error);
        }
        ensure!(
            cleanup.is_empty(),
            "client progress cleanup failed: {cleanup:?}"
        );
        context.action("held root Data position window was full with actual segment reservations and positive retained bytes while independent arithmetic queries and KILL completed; client exited and owners drained");
        Ok(())
    }
}

fn await_saturated_root(
    context: &mut ScenarioContext,
    deadline: Instant,
    epoch: Instant,
    phase: &str,
    samples: &mut Vec<Value>,
    actor_running: impl Fn() -> bool,
) -> Result<()> {
    // Leave the original actor clock available to release gates and capture its
    // actual terminal response when state observation cannot reach the trigger.
    let deadline = deadline.min(Instant::now() + Duration::from_secs(30));
    let ports = context
        .handle()
        .runtime()
        .be
        .iter()
        .map(|be| be.http)
        .collect::<Vec<_>>();
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?;
    let mut consecutive = 0;
    loop {
        context.remaining("held root saturation")?;
        ensure!(
            Instant::now() < deadline,
            "root window never saturated before test execution deadline"
        );
        ensure!(
            actor_running(),
            "held actor exited before saturation observation"
        );
        let roots = ports
            .iter()
            .map(|port| -> Result<_> {
                let rows: Value = client
                    .get(format!("http://127.0.0.1:{port}/metrics?type=json"))
                    .send()?
                    .error_for_status()?
                    .json()?;
                root_census(rows.as_array().context("invalid root metric array")?)
            })
            .collect::<Result<Vec<_>>>()?;
        let occupied = roots
            .iter()
            .filter_map(Option::as_ref)
            .filter(|root| root["channels"] != 0)
            .collect::<Vec<_>>();
        // RootChannel refuses another builder when the Data position window is
        // full. Completed rows legally flush partial segments, so retained bytes
        // need not reach the independent byte ceiling. A live producer without
        // End distinguishes this held state from ordinary completed output.
        let saturated = roots.iter().all(Option::is_some)
            && occupied.len() == 1
            && occupied[0]["channels"] == 1
            && occupied[0]["data_positions"] == RootProfileV1::DATA_POSITIONS as u64
            && occupied[0]["payload_bytes"] > 0
            && occupied[0]["payload_bytes"]
                <= (RootProfileV1::DATA_POSITIONS * RootProfileV1::SEGMENT_BYTES) as u64
            && occupied[0]["segments"] >= RootProfileV1::DATA_POSITIONS as u64
            && occupied[0]["retained_reservations"] >= RootProfileV1::DATA_POSITIONS as u64
            && occupied[0]["producers_running"] == 1
            && occupied[0]["producers_exited"] == 0
            && occupied[0]["ends_published"] == 0
            && occupied[0]["ends_acknowledged"] == 0
            && occupied[0]["sealed"] == 0;
        samples.push(json!({"phase":phase,"elapsed_micros":epoch.elapsed().as_micros(),"roots":roots,"saturated":saturated}));
        std::fs::write(
            context
                .scenario_root()
                .join("client-progress-root-census.json"),
            serde_json::to_vec_pretty(samples)?,
        )?;
        consecutive = if saturated { consecutive + 1 } else { 0 };
        if consecutive == 2 {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
