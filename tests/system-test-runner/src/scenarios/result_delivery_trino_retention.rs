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

//! Native retained Data/End and exact ACK facts under the public Trino query.
//! The proxy holds the original captured response; its copy is not a Worker
//! encoded-response alias. The independent held-late-ACK scene owns that proof.
use super::independent_root_target::{FrontendIdentitySource, IndependentRootObserver};
use super::native_sql_controls::{SqlIntentOwner, StatementSpec};
use super::result_delivery::root_census;
use super::result_delivery_inputs::{CAPTURE, MAX_SAMPLES, QUERY, SAMPLE_INTERVAL};
use super::result_delivery_support::await_idle;
use super::result_delivery_trino_protocol::{ProtocolMode, installed_root_protocol};
use crate::actors::sql::{SqlSession, StatementFailure, StatementResult, TrinoValue, expect_error};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::native_root_reply_fault::{
    RootReplyFaultBounds, RootReplyFaultControl, RootReplyFaultObservation,
    RootReplyOriginalDisposition, RootReplyOriginalIdentity,
};
use novarocks_cluster_harness::{
    CrossProcessRootReplyFaultConfig, QueryLifecycleStatementObservation, TestClusterOwner,
};
use novarocks_test_trino_client::{CancelHandle, TypeArgument};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, thread, time::Instant};

pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    [
        ProtocolMode::Retention,
        ProtocolMode::ZeroAck,
        ProtocolMode::FinalAckInterference,
        ProtocolMode::OneByte,
        ProtocolMode::MinimumNewRow,
    ]
    .into_iter()
    .map(|mode| {
        Box::new(RetainedRoot {
            mode,
            intent: SqlIntentOwner::default(),
        }) as Box<dyn Scenario>
    })
    .collect()
}
struct RetainedRoot {
    mode: ProtocolMode,
    intent: SqlIntentOwner,
}
impl Scenario for RetainedRoot {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }
    fn name(&self) -> &'static str {
        match self.mode {
            ProtocolMode::Retention => "result-delivery/producer-exit-participation-retention",
            ProtocolMode::ZeroAck => "result-delivery/installed-root-zero-ack-normal-wire",
            ProtocolMode::FinalAckInterference => {
                "result-delivery/installed-root-replay-final-ack-interference"
            }
            ProtocolMode::OneByte => "result-delivery/one-byte-continuation-normal-wire",
            ProtocolMode::MinimumNewRow => "result-delivery/minimum-new-row-normal-wire",
        }
    }
    fn launch_config(&self, _: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            native_root_reply_fault: Some(CrossProcessRootReplyFaultConfig {
                backend_indices: (0..3).collect(),
                bounds: RootReplyFaultBounds::default(),
            }),
            native_fault_proxies: novarocks_cluster_harness::CrossProcessNativeFaultProxyConfig {
                backend_retained_byte_limits: (0..3).map(|index| (index, 1024 * 1024)).collect(),
            },
            ..ScenarioLaunchConfig::default()
        })
    }
    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        backends: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze_root_observation(
            vec![StatementSpec {
            sql: self.mode.shape().sql().into(), faults: vec![],
            observations: (0..backends).map(|index| (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), None)).collect(),
        }],
            backends,
            binary,
            deadline,
        )?))
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "retained Root requires native 1FE+3BE"
        );
        // Bind the real roster before any SQL; ordinary Exchange traffic keeps
        // the original authenticated cancellation classification.
        for backend in 0..3 {
            context.handle().native_exchange_fault(backend)?;
        }
        let control = context.handle().native_root_reply_fault(0)?;
        let epoch = Instant::now();
        let shape = self.mode.shape();
        let mut observation: Option<QueryLifecycleStatementObservation> = None;
        let mut cancel: Option<(CancelHandle, Instant)> = None;
        let mut worker = None;
        let mut released: Option<RootReplyOriginalIdentity> = None;
        let mut delete_started: Option<Instant> = None;
        let mut evidence = json!({"schema_version":1,"scenario":self.name(),"scope":"Native retained Data/End and actual original proxy response; excludes encoded Worker response-holder lifetime","sql":shape.sql(),"expected_value_lengths":shape.value_lengths(),"expected_native_bytes":shape.native_bytes()});
        let primary = (|| -> Result<()> {
            await_idle(context, "root-retention", "before", epoch)?;
            let query_deadline = context.deadline().min(Instant::now() + QUERY);
            let frontend = context
                .handle()
                .original_root_observation_frontend_identity(query_deadline)?;
            let original = IndependentRootObserver::capture_baseline_until(
                context,
                frontend,
                query_deadline,
                FrontendIdentitySource::RootObservation,
            )?;
            let mut session =
                SqlSession::new(context.sql_user(), context.trino_port(), query_deadline)?;
            let pending = session.submit(shape.sql())?;
            cancel = Some((pending.cancel_handle(), query_deadline));
            let client_query_id = pending.query_id.clone();
            let (pending, binding) =
                self.intent
                    .take(0)?
                    .arm_pending(context, pending, shape.sql())?;
            observation = Some(binding);
            let deadline = query_deadline.min(Instant::now() + CAPTURE);
            control.begin_capture(deadline)?;
            worker = Some(
                thread::Builder::new()
                    .name("root-retention-trino".into())
                    .spawn(move || session.run_pending(pending))?,
            );
            let installed = context
                .handle()
                .await_statement_root_installed(observation.as_ref().unwrap(), deadline)?;
            let target = original.observe_ready_until(context, deadline)?;
            ensure!(
                target.frontend == frontend && target.root == installed.root,
                "FE Installed author differs from original BE/FE/source observations"
            );
            evidence["source"] = original.source_evidence(&target)?;
            evidence["installed_root"] = json!({"task":installed.root.to_string(),"status_version":installed.status_version.get(),"client_query_id":client_query_id});
            let mut samples = 0usize;
            loop {
                ensure!(
                    Instant::now() < deadline
                        && samples < MAX_SAMPLES
                        && !worker.as_ref().unwrap().is_finished(),
                    "exact Root capture expired or original actor exited"
                );
                samples += 1;
                if let Some(candidate) = control.candidate() {
                    ensure!(
                        candidate.read.root_task() == target.root
                            && candidate.caller == frontend
                            && candidate.backend_index == target.root_backend_index
                            && candidate.read.profile().get() == 1
                            && candidate.read.kind()
                                == novarocks_result_contract::RootOutputKind::ClientRows
                            && candidate.read.consumed() == 0
                            && candidate.read.wanted().map(|n| n.get()) == Some(1),
                        "captured response is not the independently installed exact Root"
                    );
                    control.arm_exact_hold(target.root, frontend, target.root_backend_index)?;
                    break;
                }
                sleep(deadline);
            }
            let mut held = None;
            let mut census_samples = Vec::new();
            let mut consecutive = 0;
            loop {
                ensure!(
                    Instant::now() < deadline
                        && census_samples.len() < MAX_SAMPLES
                        && !worker.as_ref().unwrap().is_finished(),
                    "producer exit/retained Root original phase expired"
                );
                if held.is_none() {
                    held = control.observation().original_hold;
                }
                let roots = census(context, deadline)?;
                let matches = held.as_ref().is_some_and(|h| {
                    h.disposition == RootReplyOriginalDisposition::Held
                        && h.identity.root == target.root
                        && h.identity.caller == frontend
                        && h.identity.backend_index == target.root_backend_index
                }) && roots.iter().enumerate().all(|(index, root)| {
                    root.as_ref().is_some_and(|r| {
                        if index != target.root_backend_index {
                            return r.values().all(|n| *n == 0);
                        }
                        r["channels"] == 1
                            && r["terminal_task_records"] == 1
                            && r["producers_running"] == 0
                            && r["producers_exited"] == 1
                            && r["ends_published"] == 1
                            && r["ends_acknowledged"] == 0
                            && r["sealed"] == 0
                            && r["data_positions"] == 2
                            && r["segments"] == 2
                            && r["payload_bytes"] == shape.native_bytes() as u64
                            && r["metadata_bytes"] > 0
                    })
                });
                census_samples.push(json!({"elapsed_micros":epoch.elapsed().as_micros(),"roots":roots,"matches":matches}));
                evidence["retained_census"] = json!(census_samples);
                write(context, &evidence)?;
                consecutive = if matches { consecutive + 1 } else { 0 };
                if consecutive == 2 {
                    break;
                }
                sleep(deadline);
            }
            installed_root_protocol(
                context,
                target.root_backend_index,
                target.root,
                self.mode,
                deadline,
                || !worker.as_ref().unwrap().is_finished(),
            )?;
            if self.mode == ProtocolMode::FinalAckInterference {
                // The independent probe has already ACKed End and observed
                // Retired. Cancel this same query before releasing its response.
                let before_delete = control
                    .observation()
                    .original_hold
                    .context("original hold disappeared before DELETE")?;
                ensure!(
                    before_delete.disposition == RootReplyOriginalDisposition::Held
                        && before_delete.downstream_reset.is_none(),
                    "original target response reset or settled before exact query DELETE"
                );
                control.expect_exact_original_query_cancellation(&before_delete.identity)?;
                let started = Instant::now();
                delete_started = Some(started);
                evidence["delete_started_monotonic"] = json!(format!("{started:?}"));
                cancel.as_ref().unwrap().0.delete(query_deadline)?;
                evidence["exact_query_delete_after_probe_retired"] = json!(true);
            }
            let identity = held
                .context("exact original response never became held")?
                .identity;
            control.release_exact_original(&identity)?;
            released = Some(identity.clone());
            // Public cancellation can complete before this independently
            // scheduled Native response. Observe its actual settlement under
            // the same capture clock before verification or cluster shutdown.
            loop {
                let actual = control.observation();
                let held = actual
                    .original_hold
                    .context("released original response observation disappeared")?;
                ensure!(
                    held.identity == identity,
                    "released original RPC identity changed"
                );
                evidence["original_after_release"] = serde_json::to_value(&held)?;
                ensure!(
                    actual.failures.is_empty(),
                    "original response actor failed before settlement: {:?}",
                    actual.failures
                );
                if held.settled_at.is_some() {
                    ensure!(
                        matches!(
                            held.disposition,
                            RootReplyOriginalDisposition::ForwardedOriginal
                                | RootReplyOriginalDisposition::ExactDownstreamCancelled
                                | RootReplyOriginalDisposition::JoinedOriginalConnectionExit
                        ),
                        "released original response has an invalid settlement"
                    );
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "released original response did not settle within its original capture clock"
                );
                sleep(deadline);
            }
            Ok(())
        })();
        let interrupted = if primary.is_err() {
            let cancelled = cancel.as_ref().map_or(Ok(()), |(handle, deadline)| {
                handle.delete(*deadline).map_err(anyhow::Error::from)
            });
            control.disarm();
            cancelled
        } else {
            Ok(())
        };
        // The same original worker is always joined, including failed capture,
        // cancellation, partial publication and failed release paths.
        let joined = worker.take().map(|worker| {
            worker.join().map_err(|payload| {
                anyhow::Error::new(OriginalQueryPanic(std::sync::Mutex::new(payload)))
            })
        });
        evidence["client_terminal"] = match &joined {
            Some(Ok(Ok(result))) => {
                json!({"kind":"success","query_id":result.query_id,"pages":result.pages,"rows":result.rows.len()})
            }
            Some(Ok(Err(StatementFailure::Trino(error)))) => {
                json!({"kind":"Trino","code":error.error_code,"name":error.error_name,"type":error.error_type})
            }
            Some(Ok(Err(_))) => json!({"kind":"non-Trino-failure","actual_source_retained":true}),
            Some(Err(_)) => json!({"kind":"original-worker-panic","actual_source_retained":true}),
            None => json!({"kind":"not-started"}),
        };
        let verified = if primary.is_ok() {
            joined
                .context("successful held phase has no original query worker")
                .and_then(|joined| {
                    verify_terminal(
                        self.mode,
                        joined,
                        &control,
                        released.as_ref(),
                        delete_started,
                    )
                })
        } else {
            match joined {
                Some(Err(error)) => Err(error),
                Some(Ok(Err(error))) if !matches!(error, StatementFailure::Trino(_)) => {
                    Err(error.into())
                }
                _ => Ok(()),
            }
        };
        let actual_idle = await_idle(context, "root-retention", "after", epoch);
        let drained = observation.as_ref().map_or(Ok(()), |observation| {
            let deadline = context.deadline();
            context
                .handle()
                .drain_query_lifecycle_statement_binding(observation, deadline)
        });
        let before_shutdown = serde_json::to_value(control.observation());
        let shutdown = context.shutdown();
        let before_serialized = before_shutdown
            .map(|value| {
                evidence["proxy_before_shutdown"] = value;
            })
            .map_err(anyhow::Error::from);
        let proxy = control.observation();
        evidence["proxy_after_shutdown"] = serde_json::to_value(&proxy)?;
        let owners = check_joined_proxy(&proxy, self.mode);
        let owners = combine(owners, before_serialized);
        let recorded = write(context, &evidence);
        let result = combine(
            combine(
                combine(
                    combine(
                        combine(combine(primary, interrupted), verified),
                        actual_idle,
                    ),
                    drained,
                ),
                shutdown,
            ),
            owners,
        );
        let result = combine(result, recorded);
        if result.is_err() {
            context.retain_artifacts();
        }
        result?;
        context.record_phase_observation(
            "producer-retired-context-held-root",
            1,
            1,
            1,
            "public-trino-native-root",
            1,
            "passed",
            BTreeMap::from([
                ("rows", shape.value_lengths().len() as u64),
                ("held_data_positions", 2),
            ]),
        )?;
        Ok(())
    }
}
fn verify_terminal(
    mode: ProtocolMode,
    joined: Result<std::result::Result<StatementResult, StatementFailure>>,
    control: &RootReplyFaultControl,
    released: Option<&RootReplyOriginalIdentity>,
    delete_started: Option<Instant>,
) -> Result<()> {
    let response = joined?;
    let proxy = control.observation();
    let held = proxy
        .original_hold
        .context("original held response observation missing")?;
    ensure!(
        Some(&held.identity) == released,
        "original held response changed after release"
    );
    if mode == ProtocolMode::FinalAckInterference {
        let started =
            delete_started.context("interference has no actual exact-query DELETE attempt")?;
        ensure!(
            held.held_at <= started
                && held
                    .release_requested_at
                    .is_some_and(|released| released >= started)
                && held.settled_at.is_some_and(|settled| settled >= started),
            "held original response release/settle preceded exact DELETE"
        );
        if held.disposition == RootReplyOriginalDisposition::ExactDownstreamCancelled {
            let reset = held
                .downstream_reset
                .as_ref()
                .context("exact cancellation has no actual watched stream reset")?;
            ensure!(
                reset.observed_at >= started,
                "watched target reset preceded the exact query DELETE attempt"
            );
        }
        if held.disposition == RootReplyOriginalDisposition::JoinedOriginalConnectionExit {
            let exit = held
                .connection_exit
                .as_ref()
                .context("original connection exit is absent")?;
            ensure!(
                held.expected_query_cancellation_at
                    .is_some_and(|prepared| prepared <= started)
                    && exit.permitted_peer_exit
                    && exit.joined_at >= started
                    && exit.all_children_joined
                    && !exit.cleanup_failed
                    && exit.remaining_connection_bytes == 0
                    && !exit.owner_stop_observed
                    && !exit.generation_change_observed
                    && held.downstream_reset.is_none(),
                "original connection exit is not the exact declared cancellation phase and real Join"
            );
        }
        let error = response
            .err()
            .context("probe-consumed query unexpectedly completed instead of exact cancellation")?;
        let error = expect_error(&error, 3, "USER_CANCELED")?;
        ensure!(
            error.error_type == "USER_ERROR",
            "exact cancellation has another error class"
        );
        ensure!(
            matches!(
                held.disposition,
                RootReplyOriginalDisposition::ForwardedOriginal
                    | RootReplyOriginalDisposition::ExactDownstreamCancelled
                    | RootReplyOriginalDisposition::JoinedOriginalConnectionExit
            ),
            "held interference response did not actually settle"
        );
    } else {
        held.require_forwarded_original()?;
        let result = response?;
        ensure!(
            result.is_result_set()
                && result.columns.len() == 1
                && result.columns[0].name == "payload"
                && result.columns[0].type_name == "varchar"
                && result.columns[0].type_signature.raw_type == "varchar"
                && result.columns[0].type_signature.arguments
                    == [TypeArgument::Long(2_147_483_647)]
                && result.rows.len() == mode.shape().value_lengths().len(),
            "Root result metadata/row count differs from independent String contract"
        );
        for (row, length) in result.rows.iter().zip(mode.shape().value_lengths()) {
            let [TrinoValue::Text(value)] = row.as_slice() else {
                anyhow::bail!("Root row is not one non-null String");
            };
            ensure!(
                value.len() == *length && value.bytes().all(|byte| byte == b'x'),
                "Root String value differs from independent length/content formula"
            );
        }
    }
    Ok(())
}
fn check_joined_proxy(proxy: &RootReplyFaultObservation, mode: ProtocolMode) -> Result<()> {
    let held = proxy
        .original_hold
        .as_ref()
        .context("actual held original response missing at join")?;
    let emitted = u64::from(held.disposition == RootReplyOriginalDisposition::ForwardedOriginal);
    ensure!(
        proxy.shutdown_joined
            && proxy.active_listeners == 0
            && proxy.joined_listeners == 3
            && proxy.active_connections == 0
            && proxy.active_streams == 0
            && proxy.connection_positions == 0
            && proxy.stream_positions == 0
            && proxy.target_slots == 0
            && proxy.owned_buffer_bytes == 0
            && proxy.failures.is_empty()
            && !proxy.failure_overflow
            && proxy.first_failure.is_none()
            && proxy.claimed == 1
            && proxy.emitted_messages == emitted
            && proxy.mutated.is_none()
            && proxy.not_ready_observed == proxy.not_ready_forwarded
            && proxy.joined_children > 0
            && proxy.joined_connections > 0,
        "original proxy listener/child/connection/byte owners did not join cleanly"
    );
    if mode == ProtocolMode::FinalAckInterference {
        ensure!(
            proxy.target_consumed_positive_requests == 0,
            "interference target was ACKed by the held FE reader"
        );
    } else {
        ensure!(
            proxy.target_consumed_positive_requests > 0,
            "normal FE reader never advanced its actual Native ACK frontier"
        );
    }
    Ok(())
}
fn census(
    context: &mut ScenarioContext,
    deadline: Instant,
) -> Result<Vec<Option<BTreeMap<String, u64>>>> {
    let ports = context
        .handle()
        .runtime()
        .be
        .iter()
        .map(|be| be.http)
        .collect::<Vec<_>>();
    ports
        .into_iter()
        .map(|port| {
            let timeout = deadline
                .saturating_duration_since(Instant::now())
                .min(std::time::Duration::from_secs(1));
            ensure!(
                !timeout.is_zero(),
                "original Root census phase deadline expired"
            );
            let rows: Value = reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()?
                .get(format!("http://127.0.0.1:{port}/metrics?type=json"))
                .send()?
                .error_for_status()?
                .json()?;
            root_census(rows.as_array().context("Root census is not an array")?)
        })
        .collect()
}
fn sleep(deadline: Instant) {
    thread::sleep(SAMPLE_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
}
fn write(context: &ScenarioContext, value: &Value) -> Result<()> {
    std::fs::write(
        context.scenario_root().join("root-context-retention.json"),
        serde_json::to_vec_pretty(value)?,
    )
    .map_err(Into::into)
}
fn combine<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(OriginalCleanupFailure { primary, cleanup }.into()),
    }
}
struct OriginalQueryPanic(std::sync::Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Debug for OriginalQueryPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.0;
        f.write_str("OriginalQueryPanic { payload_retained: true }")
    }
}
impl std::fmt::Display for OriginalQueryPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original Trino query worker panicked; payload retained")
    }
}
impl std::error::Error for OriginalQueryPanic {}

// Retain both actual error objects. A rendered cleanup note cannot replace
// either original query/actor or original proxy/resource/IO source.
struct OriginalCleanupFailure {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl std::fmt::Display for OriginalCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Root retention failed: {:#}; actual cleanup also failed: {:#}",
            self.primary, self.cleanup
        )
    }
}
impl std::fmt::Debug for OriginalCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for OriginalCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
