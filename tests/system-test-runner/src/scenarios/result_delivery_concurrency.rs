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

//! Finite concurrent results with an oracle independent of engine output.

use super::result_delivery_support::await_idle;
use crate::actors::mysql_stream::{AsyncMysqlStream, TextResultObservation};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const CLIENTS: usize = 4;
const ROWS: usize = 4096;
const SUFFIX_BYTES: usize = 512;

pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(ConcurrentResults)]
}

struct ConcurrentResults;

impl Scenario for ConcurrentResults {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery_concurrency",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "result-delivery/concurrent-results-drain"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "concurrent results require native 1FE+3BE"
        );
        let epoch = Instant::now();
        await_idle(context, "concurrent-results", "before", epoch)?;
        let deadline = Instant::now()
            + context
                .remaining("concurrent result actors")?
                .min(Duration::from_secs(90));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()?;
        // All four sockets are connected before any query starts.
        let mut connected = Vec::new();
        for client in 0..CLIENTS {
            let stream = runtime.block_on(async {
                tokio::time::timeout_at(
                    deadline.into(),
                    AsyncMysqlStream::connect(
                        context.mysql_user(),
                        context.mysql_port(),
                        deadline.saturating_duration_since(Instant::now()),
                    ),
                )
                .await
                .context("concurrent client connect deadline")?
            })?;
            connected.push((client, stream.connection_id()?, stream));
        }
        let mut actors = Vec::new();
        let mut readiness = Vec::new();
        let mut resumes = Vec::new();
        for (client, connection, mut stream) in connected {
            let sql = format!(
                "SELECT CONCAT('client-{client}:', CAST(generate_series AS VARCHAR), ':', REPEAT('x', {SUFFIX_BYTES})) AS payload FROM generate_series(1, {ROWS}) ORDER BY generate_series"
            );
            let (ready, reached) = tokio::sync::oneshot::channel();
            let (resume, resume_read) = tokio::sync::oneshot::channel();
            readiness.push(reached);
            resumes.push(resume);
            actors.push((
                client,
                connection,
                runtime.spawn(async move {
                    stream
                        .observe_text_query_owned_until(
                            &sql,
                            Duration::ZERO,
                            Some((ready, resume_read)),
                            deadline,
                        )
                        .await
                }),
            ));
        }
        let mut causal = runtime
            .block_on(async {
                for reached in readiness {
                    tokio::time::timeout_at(deadline.into(), reached)
                        .await
                        .context("concurrent metadata barrier deadline")?
                        .context("concurrent actor exited before metadata barrier")?;
                }
                Ok::<_, anyhow::Error>(())
            })
            .err();
        let metadata_barrier_completed = causal.is_none();
        let mut cleanup = Vec::new();
        // Cancel failed setup while actors are still held, before any join.
        if causal.is_some() {
            for (_, connection, job) in &actors {
                if !job.is_finished()
                    && let Err(error) = context.handle().kill_query_until(*connection, deadline)
                {
                    cleanup.push(format!("cancel client {connection}: {error:#}"));
                }
            }
        }
        for resume in resumes {
            let _ = resume.send(());
        }
        let mut observations = Vec::new();
        let mut actors: VecDeque<_> = actors.into();
        while let Some((client, connection, job)) = actors.pop_front() {
            let actor_failure = match runtime.block_on(job) {
                Ok(owned) => {
                    let actual = owned
                        .actual_failure
                        .as_ref()
                        .map(|error| format!("{error:#}"));
                    let check = (|| -> Result<()> {
                        ensure!(
                            owned.actual_failure.is_none(),
                            "concurrent client {client} failed: {actual:?}"
                        );
                        ensure!(
                            owned.observation.schema.len() == 1
                                && owned.observation.schema[0].mysql_type == 253,
                            "concurrent string metadata differs from declared SQL"
                        );
                        let expected = (1..=ROWS).map(|row| {
                            format!("client-{client}:{row}:{}", "x".repeat(SUFFIX_BYTES))
                        });
                        assert_single_column_rows(&owned.observation, "payload", expected)?;
                        ensure!(
                            owned.observation.row_payload_bytes > 1024 * 1024,
                            "concurrent actor did not cross segment boundary"
                        );
                        Ok(())
                    })();
                    observations.push(json!({"client":client,"connection_id":connection,"wire":owned.observation,"actual_failure":actual}));
                    check.err()
                }
                Err(error) => Some(error.into()),
            };
            if let Some(error) = actor_failure {
                let first_failure = causal.is_none();
                retain_failure(&mut causal, &mut cleanup, error);
                if first_failure {
                    // A wire/oracle failure cancels every still-live peer before its join.
                    for (_, connection, job) in &actors {
                        if !job.is_finished()
                            && let Err(error) =
                                context.handle().kill_query_until(*connection, deadline)
                        {
                            cleanup.push(format!("cancel peer {connection}: {error:#}"));
                        }
                    }
                }
            }
        }
        if let Err(error) = await_idle(context, "concurrent-results", "after", epoch) {
            retain_failure(&mut causal, &mut cleanup, error);
        }
        let health = runtime.block_on(async {
            let timeout = context
                .remaining("concurrent post-drain health")?
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
        let mut health_wire = serde_json::Value::Null;
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
        if let Err(error) = await_idle(context, "concurrent-results", "after-health", epoch) {
            retain_failure(&mut causal, &mut cleanup, error);
        }
        std::fs::write(
            context.scenario_root().join("concurrent-results.json"),
            serde_json::to_vec_pretty(&json!({
                "schema_version":1,"clients":CLIENTS,"rows_per_client":ROWS,"suffix_bytes":SUFFIX_BYTES,
                "all_connected_before_queries":true,"metadata_barrier_completed":metadata_barrier_completed,
                "observations":observations,"post_drain_health":health_wire,"causal_failure":causal.as_ref().map(|error| format!("{error:#}")),"cleanup_failures":cleanup,
            }))?,
        )?;
        if let Some(error) = causal {
            return Err(error);
        }
        ensure!(cleanup.is_empty(), "concurrent cleanup failed: {cleanup:?}");
        context.action("four native clients reached metadata together, each independently matched its complete ordered rows and success EOF, and all owners drained");
        Ok(())
    }
}

pub(super) fn retain_failure(
    causal: &mut Option<anyhow::Error>,
    cleanup: &mut Vec<String>,
    error: anyhow::Error,
) {
    if causal.is_none() {
        *causal = Some(error);
    } else {
        cleanup.push(format!("{error:#}"));
    }
}

/// Construct the full MySQL text-row digest from declared values, including row lengths.
/// The actual observer hashes all row bytes and only commits a complete validated row.
pub(super) fn assert_single_column_rows(
    actual: &TextResultObservation,
    name: &str,
    expected: impl IntoIterator<Item = String>,
) -> Result<()> {
    let (rows, bytes, digest) = expected_single_column_rows(expected);
    ensure!(
        actual.error.is_none(),
        "text result failed: {:?}",
        actual.error
    );
    ensure!(
        actual.columns == 1 && actual.schema.len() == 1 && actual.schema[0].name == name,
        "single-column metadata differs from independent oracle"
    );
    ensure!(
        actual.rows == rows && actual.row_payload_bytes == bytes && actual.row_sha256 == digest,
        "complete text rows differ from independently declared values"
    );
    Ok(())
}

pub(super) fn expected_single_column_rows(
    expected: impl IntoIterator<Item = String>,
) -> (u64, u64, String) {
    let mut digest = Sha256::new();
    let mut rows = 0u64;
    let mut bytes = 0u64;
    for cell in expected {
        let length = cell.len();
        let prefix = if length < 251 {
            vec![length as u8]
        } else if length <= u16::MAX as usize {
            let number = (length as u16).to_le_bytes();
            vec![0xfc, number[0], number[1]]
        } else if length <= 0x00ff_ffff {
            let number = (length as u32).to_le_bytes();
            vec![0xfd, number[0], number[1], number[2]]
        } else {
            let mut prefix = vec![0xfe];
            prefix.extend_from_slice(&(length as u64).to_le_bytes());
            prefix
        };
        let row_bytes = (prefix.len() + length) as u64;
        digest.update(&prefix);
        digest.update(cell.as_bytes());
        digest.update(row_bytes.to_le_bytes());
        bytes += row_bytes;
        rows += 1;
    }
    (rows, bytes, format!("{:x}", digest.finalize()))
}
