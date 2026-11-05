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

//! Native FE semantic refusal after one bounded authenticated Root reply fault.
//! Public Trino errors and rows are observed independently of the Native actor.

use super::native_sql_controls::{SqlIntentOwner, StatementSpec};
use super::result_delivery::root_census;
use super::result_delivery_root_protocol::parse_created_task_candidates;
use super::result_delivery_support::await_idle;
use crate::actors::sql::{SqlSession, StatementResult, TrinoValue, single_integer};
use crate::actors::trino_protocol::TrinoClient;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::native_root_reply_fault::{
    RootReplyFaultBounds, RootReplyFaultObservation, RootReplyMutation,
};
use novarocks_cluster_harness::{
    CrossProcessRootReplyFaultConfig, LaunchProfile, TestClusterOwner,
};
use novarocks_execution_contract::identity::TaskIdentity;
use novarocks_proto_codec::FieldPath;
use novarocks_task_codec::budget::TransportBudget;
use novarocks_task_codec::identity::decode_task_identity;
use novarocks_test_trino_client::{Column, TrinoError, TypeArgument};
use novarocks_types::{BackendProcessId, FrontendProcessId};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const CAPTURE: Duration = Duration::from_secs(5);
const CLIENT: Duration = Duration::from_secs(20);
const LOG_CAP: usize = 2 * 1024 * 1024;
const METRIC_CAP: usize = 1024 * 1024;
const CREATE: &str = novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED;
const ESTABLISH: &str = novarocks_failpoint::observation_marker::PARTICIPATION_ESTABLISH_APPLIED;
const PREPARED_ROOT: &str = novarocks_failpoint::observation_marker::TASK_PREPARED_CLIENT_ROOT;
// Observation storage bound only; this does not change TaskGraph or admission caps.
const MARKERS_PER_BACKEND: usize = 8;
const SQL: &str = "SELECT REPEAT('x',64) AS payload";
const HEALTH: &str = "SELECT SUM(generate_series) AS total FROM generate_series(1, 100)";

#[derive(Clone, Copy)]
enum Case {
    Profile,
    Kind,
    Prefix,
    PrefixSuffix,
}
impl Case {
    fn mutation(self) -> RootReplyMutation {
        match self {
            Self::Profile => RootReplyMutation::ProfileTwo,
            Self::Kind => RootReplyMutation::ClientRowsFalse,
            Self::Prefix => RootReplyMutation::FourBytePrefixOnly,
            Self::PrefixSuffix => RootReplyMutation::ValidRowPrefixOnlySuffix,
        }
    }
    fn freeze(self) -> &'static str {
        match self {
            Self::Profile => {
                include_str!("../../../../docs/testing/trc-3/root-reply-profile-v2.json")
            }
            Self::Kind => include_str!("../../../../docs/testing/trc-3/root-reply-kind-v2.json"),
            Self::Prefix => {
                include_str!("../../../../docs/testing/trc-3/root-reply-prefix-only-v2.json")
            }
            Self::PrefixSuffix => include_str!(
                "../../../../docs/testing/trc-3/root-reply-valid-row-prefix-suffix-v2.json"
            ),
        }
    }
}
struct NativeRootReplyRefusal {
    case: Case,
    intent: SqlIntentOwner,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Freeze {
    schema_version: u32,
    contract: String,
    scenario: String,
    topology: String,
    sql: String,
    mutation: String,
    contract_reason: String,
    capture_millis: u64,
    maximum_target_attempts: u64,
    forward_millis: u64,
    expected_native_body_bytes: usize,
    expected_native_body_sha256: String,
    expected_value_length: usize,
    trino_error_code: i64,
    trino_error_name: String,
    trino_error_type: String,
    trino_message: String,
    health_sql: String,
    health_type: String,
    health_value: i64,
    #[serde(default)]
    expected_mutated_body_bytes: Option<usize>,
    #[serde(default)]
    expected_mutated_body_sha256: Option<String>,
}
pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    [Case::Profile, Case::Kind, Case::Prefix, Case::PrefixSuffix]
        .into_iter()
        .map(|case| {
            Box::new(NativeRootReplyRefusal {
                case,
                intent: SqlIntentOwner::default(),
            }) as Box<dyn Scenario>
        })
        .collect()
}
#[derive(serde::Serialize)]
struct NegativeClientAttempt {
    pages: u64,
    rows: u64,
    columns: Option<Vec<Column>>,
    terminal_error: TrinoError,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NegativePage {
    id: String,
    columns: Option<Vec<Column>>,
    data: Option<Vec<Value>>,
    error: Option<TrinoError>,
    next_uri: Option<String>,
    update_type: Option<String>,
    update_count: Option<u64>,
    stats: NegativeStats,
}
#[derive(Deserialize)]
struct NegativeStats {
    state: String,
}
fn check_negative_page(page: &NegativePage, query_id: &str) -> Result<()> {
    ensure!(
        page.id == query_id && page.update_type.is_none() && page.update_count.is_none(),
        "native refusal lost its original query or became a command completion"
    );
    ensure!(
        page.data.as_ref().is_none_or(Vec::is_empty),
        "native malformed response published a client row before refusal"
    );
    if let Some(columns) = &page.columns {
        check_payload_columns(columns)?;
    }
    if page.error.is_some() {
        ensure!(
            page.next_uri.is_none() && page.stats.state == "FAILED",
            "native refusal is not the actual terminal failed Trino page"
        );
    } else {
        ensure!(
            page.next_uri.is_some(),
            "native fault unexpectedly completed successfully"
        );
    }
    Ok(())
}
fn negative_client(
    actor: TrinoClient,
    query_id: String,
    first_uri: String,
    live: Arc<Mutex<Option<String>>>,
) -> Result<NegativeClientAttempt> {
    let mut uri = first_uri;
    let mut pages = 0u64;
    let mut columns = None;
    loop {
        let raw = actor.get(&uri)?;
        ensure!(
            raw.status == 200,
            "native refusal returned HTTP {} instead of a typed SQL error",
            raw.status
        );
        let page: NegativePage = serde_json::from_slice(&raw.body)?;
        // The raw actor retains every original body and repeated header. Only
        // one complete page is decoded here; no full-result accumulator.
        *live
            .lock()
            .map_err(|_| anyhow::anyhow!("original query URI owner poisoned"))? =
            page.next_uri.clone();
        check_negative_page(&page, &query_id)?;
        pages = pages
            .checked_add(1)
            .context("original Trino page count overflow")?;
        if let Some(actual) = page.columns {
            if let Some(previous) = &columns {
                ensure!(
                    previous == &actual,
                    "native refusal metadata changed between pages"
                );
            }
            columns = Some(actual);
        }
        if let Some(error) = page.error {
            return Ok(NegativeClientAttempt {
                pages,
                rows: 0,
                columns,
                terminal_error: error,
            });
        }
        uri = page
            .next_uri
            .context("nonterminal original query has no continuation")?;
        *live
            .lock()
            .map_err(|_| anyhow::anyhow!("original query URI owner poisoned"))? = Some(uri.clone());
    }
}
fn check_payload_columns(columns: &[Column]) -> Result<()> {
    ensure!(
        columns.len() == 1
            && columns[0].name == "payload"
            && columns[0].type_name == "varchar"
            && columns[0].type_signature.raw_type == "varchar"
            && columns[0].type_signature.arguments == [TypeArgument::Long(2_147_483_647)],
        "native query lost its independent VARCHAR payload declaration"
    );
    Ok(())
}
fn check_trino(observed: &NegativeClientAttempt, freeze: &Freeze) -> Result<()> {
    let error = &observed.terminal_error;
    ensure!(
        observed.pages > 0
            && observed.rows == 0
            && error.error_code == freeze.trino_error_code
            && error.error_name == freeze.trino_error_name
            && error.error_type == freeze.trino_error_type
            && error.message == freeze.trino_message,
        "real FE did not report the exact typed semantic refusal with zero published rows"
    );
    if let Some(columns) = &observed.columns {
        check_payload_columns(columns)?;
    }
    Ok(())
}
fn join_client(
    job: std::thread::JoinHandle<Result<NegativeClientAttempt>>,
) -> Result<NegativeClientAttempt> {
    job.join()
        .map_err(|payload| anyhow::Error::new(OriginalClientPanic(Mutex::new(payload))))?
}
#[derive(Debug)]
struct OriginalClientPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Display for OriginalClientPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _original = &self.0;
        f.write_str("original Trino refusal reader panicked; original payload retained")
    }
}
impl std::error::Error for OriginalClientPanic {}
fn with_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(RootReplyCleanupFailure { primary, cleanup }.into()),
    }
}
#[derive(Debug)]
struct RootReplyCleanupFailure {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl std::fmt::Display for RootReplyCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:#}; actual original Root reply cleanup failed: {:#}",
            self.primary, self.cleanup
        )
    }
}
impl std::error::Error for RootReplyCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}

impl Scenario for NativeRootReplyRefusal {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery_root_reply",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        match self.case {
            Case::Profile => "result-delivery/native-root-reply-profile-refusal",
            Case::Kind => "result-delivery/native-root-reply-kind-refusal",
            Case::Prefix => "result-delivery/native-root-reply-prefix-only-refusal",
            Case::PrefixSuffix => {
                "result-delivery/native-root-reply-valid-row-prefix-suffix-refusal"
            }
        }
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        ensure!(
            profile == LaunchProfile::FaultScenario,
            "native reply refusal requires task observation FaultScenario profile"
        );
        Ok(())
    }
    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze_root_observation(
            vec![StatementSpec {
                sql: SQL.into(),
                faults: Vec::new(),
                observations: (0..cluster_size)
                    .map(|index| (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), Some(1)))
                    .collect(),
            }],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn launch_config(&self, _: &Path) -> Result<ScenarioLaunchConfig> {
        // Select the bounded RootReply actor explicitly for these scenarios.
        // Ordinary scenarios keep the existing TCP forwarding path.
        Ok(ScenarioLaunchConfig {
            native_root_reply_fault: Some(CrossProcessRootReplyFaultConfig {
                backend_indices: (0..3).collect(),
                bounds: RootReplyFaultBounds::default(),
            }),
            // Existing Control TCP path remains in Forward state with its usual cap.
            native_fault_proxies: novarocks_cluster_harness::CrossProcessNativeFaultProxyConfig {
                backend_retained_byte_limits: (0..3).map(|i| (i, 1024 * 1024)).collect(),
            },
            ..ScenarioLaunchConfig::default()
        })
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "native root reply refusal requires 1FE+3BE"
        );
        let freeze: Freeze = serde_json::from_str(self.case.freeze())?;
        ensure!(
            freeze.schema_version == 2
                && freeze.contract == "TRC-3 spec3 P4; approved plan4 P04"
                && freeze.scenario == self.name()
                && freeze.topology == "1FE+3BE"
                && freeze.sql == SQL
                && freeze.mutation == format!("{:?}", self.case.mutation())
                && freeze.capture_millis == 5000
                && freeze.maximum_target_attempts == 32
                && freeze.forward_millis == 20000
                && freeze.expected_native_body_bytes == 72
                && freeze.expected_value_length == 64
                && freeze.health_sql == HEALTH
                && freeze.health_type == "bigint"
                && freeze.health_value == 5050,
            "unsupported independent Trino negative contract"
        );
        validate_suffix_freeze(&freeze, self.case)?;
        for backend in 0..3 {
            context.handle().native_exchange_fault(backend)?;
        }
        let control = context.handle().native_root_reply_fault(0)?;
        let epoch = Instant::now();
        let mut job = None;
        let mut observation = None;
        let mut cancel_actor = None::<TrinoClient>;
        let live = Arc::new(Mutex::new(None::<String>));
        let mut evidence = json!({"schema_version":2,"scenario":self.name(),"input":serde_json::from_str::<Value>(self.case.freeze())?,
            "scope":"same Native semantic refusal and independent logical Trino metadata/values; MySQL packet ordering and historical wire hashes retired"});
        let primary = (|| -> Result<()> {
            check_effective(context, &mut evidence)?;
            context.recheck_live_process_launch_identities()?;
            await_idle(context, "root-reply", "before-baseline", epoch)?;
            zero_roots(context, "before-baseline", &mut evidence)?;
            let baseline = positive_query(context, SQL)?;
            check_payload_columns(&baseline.columns)?;
            ensure!(
                baseline.is_result_set()
                    && baseline.rows == [vec![TrinoValue::Text("x".repeat(64))]],
                "same-SQL native baseline differs from independent logical scalar input"
            );
            let [TrinoValue::Text(value)] = baseline.rows[0].as_slice() else {
                anyhow::bail!("baseline has no actual String value");
            };
            evidence["metadata_baseline"] = json!({"columns":baseline.columns,"rows":baseline.rows.len(),"actual_value":value,"pages":baseline.pages});
            await_idle(context, "root-reply", "before-capture", epoch)?;
            zero_roots(context, "before-capture", &mut evidence)?;
            let before = logs(context)?;
            let processes = (0..3)
                .map(|index| context.handle().backend_process_id(index))
                .collect::<Result<Vec<_>>>()?;
            ensure!(
                context.remaining("absolute capture and original Trino join")? >= CLIENT,
                "insufficient original budget for bounded query join"
            );
            let started = Instant::now();
            let query_deadline = started + CLIENT;
            let deadline = started + CAPTURE;
            let frontend = context
                .handle()
                .original_root_observation_frontend_identity(query_deadline)?;
            let actor = TrinoClient::new(
                context.trino_port(),
                context.sql_user(),
                query_deadline,
                &context.scenario_root().join("root-reply-trino-http"),
            )?
            .with_session(&[("x-trino-time-zone", "Asia/Shanghai")]);
            cancel_actor = Some(actor.clone());
            let pending = actor.submit(SQL)?;
            ensure!(
                pending.status == 200,
                "original Trino POST did not return a pending query"
            );
            let pending_page: NegativePage = serde_json::from_slice(&pending.body)?;
            check_negative_page(&pending_page, &pending_page.id)?;
            ensure!(
                pending_page.error.is_none() && pending_page.stats.state == "QUEUED",
                "Native statement executed before its exact Trino arm"
            );
            let query_id = pending_page.id;
            let uri = pending_page
                .next_uri
                .context("original pending query has no continuation")?;
            *live
                .lock()
                .map_err(|_| anyhow::anyhow!("original query URI owner poisoned"))? =
                Some(uri.clone());
            observation = Some(
                self.intent
                    .take(0)?
                    .arm_client_query(context, &query_id, SQL)?,
            );
            control.begin_capture(deadline)?;
            let original_live = live.clone();
            let original_query_id = query_id.clone();
            job = Some(
                std::thread::Builder::new()
                    .name("native-refusal-trino".into())
                    .spawn(move || negative_client(actor, original_query_id, uri, original_live))?,
            );
            let installed = context
                .handle()
                .await_statement_root_installed(observation.as_ref().unwrap(), deadline)?;
            let mut snapshots = Vec::with_capacity(51);
            loop {
                ensure!(
                    Instant::now() < deadline && snapshots.len() < 51,
                    "exact Native refusal proof exceeded original capture clock"
                );
                ensure!(
                    !job.as_ref()
                        .context("missing original Trino reader")?
                        .is_finished(),
                    "original Trino reader exited before exact Native mutation arm"
                );
                let roots = census(context, deadline)?;
                snapshots.push(json!({"epoch_micros":epoch.elapsed().as_micros(),"roots":roots}));
                evidence["capture_samples"] = json!(snapshots);
                if roots.iter().all(Option::is_some) {
                    let occupied = roots
                        .iter()
                        .enumerate()
                        .filter_map(|(index, root)| {
                            (root.as_ref().unwrap()["channels"] != 0).then_some(index)
                        })
                        .collect::<Vec<_>>();
                    ensure!(
                        occupied.len() <= 1,
                        "multiple fresh roots cannot establish exact fault target"
                    );
                    if let [backend] = occupied.as_slice() {
                        let root = roots[*backend].as_ref().unwrap();
                        if root["channels"] == 1
                            && root["terminal_task_records"] == 1
                            && root["producers_running"] == 0
                            && root["producers_exited"] == 1
                            && root["ends_published"] == 1
                            && root["ends_acknowledged"] == 0
                            && root["sealed"] == 0
                            && root["data_positions"] == 1
                            && root["segments"] == 1
                            && root["payload_bytes"] == 72
                            && roots.iter().enumerate().all(|(index, root)| {
                                index == *backend
                                    || root.as_ref().unwrap().values().all(|value| *value == 0)
                            })
                        {
                            let after = logs(context)?;
                            let (task, observed_frontend) =
                                independent_target(&before, &after, *backend, &processes)?;
                            ensure!(
                                task == installed.root && observed_frontend == frontend,
                                "original FE Installed identity differs from independent Task/backend/frontend facts"
                            );
                            context.recheck_live_process_launch_identities()?;
                            if let Some(candidate) = control.candidate() {
                                ensure!(
                                    candidate.backend_index == *backend
                                        && candidate.caller == frontend
                                        && candidate.read.root_task() == task
                                        && candidate.read.profile().get() == 1
                                        && candidate.read.kind()
                                            == novarocks_result_contract::RootOutputKind::ClientRows
                                        && candidate.read.consumed() == 0
                                        && candidate.read.wanted().map(|n| n.get()) == Some(1),
                                    "candidate differs from original Installed Task/frontend/root"
                                );
                                evidence["exact_target"] = json!({"backend_index":backend,"backend_process":processes[*backend].to_string(),
                                    "task":task.to_string(),"frontend_process":frontend.to_string(),"client_query_id":query_id,
                                    "installed_status_version":installed.status_version.get(),"independent_markers":appended_markers(&before,&after)?,
                                    "known_root_profile":1,"known_root_kind":"ClientRows","independent_value_length":64,
                                    "candidate_request_sha256":candidate.request_sha256});
                                control.arm_exact(
                                    task,
                                    frontend,
                                    *backend,
                                    self.case.mutation(),
                                )?;
                                break;
                            }
                        }
                    }
                }
                std::thread::sleep(
                    Duration::from_millis(100)
                        .min(deadline.saturating_duration_since(Instant::now())),
                );
            }
            let observed = join_client(
                job.take()
                    .context("original Trino reader missing at join")?,
            )?;
            evidence["negative_client"] = serde_json::to_value(&observed)?;
            check_trino(&observed, &freeze)?;
            check_fault(&control.observation(), &freeze, self.case)?;
            evidence["fault_complete"] = serde_json::to_value(control.observation())?;
            control.disarm();
            await_idle(context, "root-reply", "after-refusal", epoch)?;
            zero_roots(context, "after-refusal", &mut evidence)?;
            let health = positive_query(context, HEALTH)?;
            ensure!(
                health.is_result_set()
                    && health.columns.len() == 1
                    && health.columns[0].name == "total"
                    && health.columns[0].type_name == "bigint"
                    && health.columns[0].type_signature.raw_type == "bigint"
                    && health.columns[0].type_signature.arguments.is_empty()
                    && health.rows == [vec![TrinoValue::Integer(5050)]],
                "ordinary Native Trino query failed after exact fault closed"
            );
            evidence["healthy_trino"] = json!({"columns":health.columns,"rows":health.rows.len(),"actual_total":single_integer(&health)?,"pages":health.pages});
            await_idle(context, "root-reply", "after-health", epoch)?;
            zero_roots(context, "after-health", &mut evidence)?;
            check_fault(&control.observation(), &freeze, self.case)?;
            context.recheck_live_process_launch_identities()?;
            Ok(())
        })();
        // A failed capture must still cancel this exact public query, release
        // the original proxy slot and join its original HTTP reader.
        control.disarm();
        let cancelled = (|| -> Result<()> {
            if primary.is_err() {
                let uri = live
                    .lock()
                    .map_err(|_| anyhow::anyhow!("original query URI owner poisoned"))?
                    .clone();
                if let Some(uri) = uri {
                    let actor = cancel_actor
                        .as_ref()
                        .context("live original query has no cancel actor")?;
                    let response = actor.delete(&uri)?;
                    ensure!(
                        matches!(response.status, 200 | 204),
                        "same-query Trino cleanup DELETE failed"
                    );
                }
            }
            Ok(())
        })();
        let cleanup_join = job
            .take()
            .map_or(Ok(()), |job| join_client(job).map(|_| ()));
        let cleanup_owners = if primary.is_err() {
            await_idle(context, "root-reply", "failure-cleanup", epoch)
                .and_then(|_| zero_roots(context, "failure-cleanup", &mut evidence))
        } else {
            Ok(())
        };
        let cleanup_observation = observation.as_ref().map_or(Ok(()), |observation| {
            let deadline = context.deadline();
            context
                .handle()
                .drain_query_lifecycle_statement_binding(observation, deadline)
        });
        evidence["failure_summaries"] = json!({"primary":failure_summary("scene-contract",primary.as_ref().err()),
            "cancel":failure_summary("same-query-cancel",cancelled.as_ref().err()),
            "client_join":failure_summary("client-join",cleanup_join.as_ref().err()),
            "owner_cleanup":failure_summary("owner-cleanup",cleanup_owners.as_ref().err()),
            "statement_observation":failure_summary("statement-observation",cleanup_observation.as_ref().err())});
        evidence["actor_before_shutdown"] = serde_json::to_value(control.observation())?;
        if primary.is_err()
            || cancelled.is_err()
            || cleanup_join.is_err()
            || cleanup_owners.is_err()
            || cleanup_observation.is_err()
        {
            context.retain_artifacts();
        }
        let shutdown = context.shutdown();
        let post = control.observation();
        let joined = check_shutdown(&post);
        evidence["actor_after_shutdown"] = serde_json::to_value(post)?;
        evidence["outcome"] = json!({"contract_and_recovery_ok":primary.is_ok(),"client_join_ok":cleanup_join.is_ok(),
            "cluster_shutdown_ok":shutdown.is_ok(),"actor_join_ok":joined.is_ok(),"failure_owner_cleanup_ok":cleanup_owners.is_ok(),
            "statement_observation_cleanup_ok":cleanup_observation.is_ok(),"same_query_cancel_ok":cancelled.is_ok()});
        let recorded = std::fs::write(
            context
                .scenario_root()
                .join("native-root-reply-observations.json"),
            serde_json::to_vec_pretty(&evidence)?,
        )
        .map_err(anyhow::Error::from);
        let result = with_cleanup(
            with_cleanup(
                with_cleanup(
                    with_cleanup(
                        with_cleanup(with_cleanup(primary, cancelled), cleanup_join),
                        cleanup_owners,
                    ),
                    cleanup_observation,
                ),
                shutdown,
            ),
            joined,
        );
        with_cleanup(result, recorded)?;
        context.action("verified exact Native RootReply semantic refusal, zero public rows/target ACK, real reader/actor Join and Native Trino recovery");
        Ok(())
    }
}

// Independent ASCII JSON input, not a production renderer or candidate output.
fn payload_literal() -> Vec<u8> {
    let mut body = vec![68, 0, 0, 0, b'[', b'"'];
    body.extend_from_slice(&[b'x'; 64]);
    body.extend_from_slice(b"\"]");
    body
}
fn validate_suffix_freeze(freeze: &Freeze, case: Case) -> Result<()> {
    let mut body = payload_literal();
    ensure!(
        body.len() == 72
            && freeze.expected_native_body_bytes == 72
            && format!("{:x}", Sha256::digest(&body)) == freeze.expected_native_body_sha256,
        "independent Trino Native input does not match its literal"
    );
    let (code, name, kind, message) = match case {
        Case::Profile | Case::Kind => (
            65536,
            "GENERIC_INTERNAL_ERROR",
            "INTERNAL_ERROR",
            "[query.execution.invariant] root response violates its contract",
        ),
        Case::Prefix | Case::PrefixSuffix => (
            0,
            "GENERIC_USER_ERROR",
            "USER_ERROR",
            "[query.execution.invalid_data] client-row prefix has no payload in the same body",
        ),
    };
    ensure!(
        freeze.trino_error_code == code
            && freeze.trino_error_name == name
            && freeze.trino_error_type == kind
            && freeze.trino_message == message,
        "independent Trino semantic error contract changed"
    );
    if matches!(case, Case::PrefixSuffix) {
        body.extend_from_slice(&[1, 0, 0, 0]);
        ensure!(
            freeze.expected_mutated_body_bytes == Some(76)
                && freeze.expected_mutated_body_sha256.as_deref()
                    == Some(format!("{:x}", Sha256::digest(&body)).as_str())
                && freeze.contract_reason
                    == "[query.execution.invalid_data] client-row prefix has no payload in the same body",
            "unsupported valid-row prefix-only suffix input"
        );
    } else {
        ensure!(
            freeze.expected_mutated_body_bytes.is_none()
                && freeze.expected_mutated_body_sha256.is_none(),
            "refusal fixture unexpectedly acquired a suffix input"
        );
    }
    Ok(())
}
fn positive_query(context: &mut ScenarioContext, sql: &str) -> Result<StatementResult> {
    let before = (0..3)
        .map(|index| context.handle().backend_task_execution_tasks_created(index))
        .collect::<Result<Vec<_>>>()?;
    let deadline = context.deadline().min(Instant::now() + CLIENT);
    let observed =
        SqlSession::new(context.sql_user(), context.trino_port(), deadline)?.execute(sql)?;
    let after = (0..3)
        .map(|index| context.handle().backend_task_execution_tasks_created(index))
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        before
            .iter()
            .zip(&after)
            .all(|(a, b)| a.is_finite() && b.is_finite() && *a >= 0.0 && b >= a)
            && before.iter().zip(&after).any(|(a, b)| b > a),
        "ordinary Trino query never created a Native Task"
    );
    Ok(observed)
}
fn check_fault(obs: &RootReplyFaultObservation, freeze: &Freeze, case: Case) -> Result<()> {
    ensure!(
        obs.claimed == 1
            && obs.emitted_messages == 1
            && obs.target_consumed_positive_requests == 0
            && (1..=32).contains(&obs.target_attempts)
            && obs.not_ready_observed == obs.not_ready_forwarded
            && obs.failures.is_empty()
            && !obs.failure_overflow
            && obs.first_failure.is_none()
            && obs.overflow_failures == 0,
        "native fault incomplete, overflowed, failed, or target result acknowledged"
    );
    let old = obs
        .original
        .as_ref()
        .context("no original real reply evidence")?;
    let new = obs
        .mutated
        .as_ref()
        .context("no emitted mutation evidence")?;
    ensure!(
        old["profile"] == 1
            && old["kind"] == "ClientRows"
            && old["sequence"] == 1
            && old["accepted_consumed"] == 0
            && old["body_bytes"] == 72
            && old["body_sha256"] == freeze.expected_native_body_sha256
            && new["mutation"] == format!("{:?}", case.mutation())
            && new["task"] == old["task"]
            && new["sequence"] == old["sequence"]
            && new["accepted_consumed"] == old["accepted_consumed"]
            && new["end_after_data"] == old["end_after_data"],
        "native mutation lost frozen immutable reply facts"
    );
    if !old["end_after_data"].is_null() {
        ensure!(
            old["end_after_data"]["sequence"] == 2 && old["end_after_data"]["output_rows"] == 1,
            "real optional Data1 piggyback End differs from scalar result contract"
        );
    }
    match case {
        Case::Profile => ensure!(
            new["profile"] == 2
                && new["output_kind"]
                    .as_str()
                    .is_some_and(|s| s.contains("ClientRows(true)"))
                && new["body_sha256"] == old["body_sha256"],
            "profile mutation differs"
        ),
        Case::Kind => ensure!(
            new["profile"] == 1
                && new["output_kind"]
                    .as_str()
                    .is_some_and(|s| s.contains("ClientRows(false)"))
                && new["body_sha256"] == old["body_sha256"],
            "kind mutation differs"
        ),
        Case::Prefix => ensure!(
            new["profile"] == 1
                && new["output_kind"]
                    .as_str()
                    .is_some_and(|s| s.contains("ClientRows(true)"))
                && new["body_bytes"] == 4
                && new["body_hex"] == "01000000",
            "prefix-only mutation differs"
        ),
        Case::PrefixSuffix => {
            validate_suffix_freeze(freeze, case)?;
            ensure!(
                new["profile"] == 1
                    && new["output_kind"]
                        .as_str()
                        .is_some_and(|s| s.contains("ClientRows(true)"))
                    && new["body_bytes"] == 76
                    && new["body_sha256"].as_str()
                        == freeze.expected_mutated_body_sha256.as_deref(),
                "valid-row prefix-only suffix mutation differs"
            );
        }
    }
    Ok(())
}
fn check_shutdown(obs: &RootReplyFaultObservation) -> Result<()> {
    ensure!(
        obs.shutdown_joined
            && obs.active_listeners == 0
            && obs.joined_listeners == 3
            && obs.active_connections == 0
            && obs.active_streams == 0
            && obs.connection_positions == 0
            && obs.stream_positions == 0
            && obs.target_slots == 0
            && obs.owned_buffer_bytes == 0
            && obs.failures.is_empty()
            && !obs.failure_overflow
            && obs.first_failure.is_none()
            && obs.target_consumed_positive_requests == 0
            && obs.claimed == 1
            && obs.emitted_messages == 1
            && obs.joined_children > 0
            && obs.joined_connections > 0,
        "actor descendants or owned-buffer aliases survived shutdown, or wholefailure occurred"
    );
    Ok(())
}

fn logs(context: &mut ScenarioContext) -> Result<Vec<String>> {
    // Read the original process stdout owner, not its diagnostic locator or
    // human combined-stream rendering. Launch identities remain checked
    // before/after capture; this scene never restarts a backend.
    (0..3)
        .map(|i| context.handle().be_stdout_contents(i, LOG_CAP))
        .collect()
}
fn appended_markers(before: &[String], after: &[String]) -> Result<Vec<Vec<String>>> {
    ensure!(
        before.len() == 3 && after.len() == 3,
        "task log inventory is not three BEs"
    );
    before
        .iter()
        .zip(after)
        .map(|(before, after)| {
            ensure!(
                after.starts_with(before),
                "task log was replaced or truncated"
            );
            ensure!(
                before.is_empty() || before.ends_with('\n'),
                "task log baseline ends inside a line"
            );
            let delta = &after[before.len()..];
            ensure!(
                delta.is_empty() || delta.ends_with('\n'),
                "partial appended task observation"
            );
            let mut markers = Vec::with_capacity(MARKERS_PER_BACKEND);
            for line in delta.lines().filter(|l| {
                l.contains(CREATE) || l.contains(ESTABLISH) || l.contains(PREPARED_ROOT)
            }) {
                ensure!(
                    markers.len() < MARKERS_PER_BACKEND && line.len() <= 384,
                    "task marker inventory overflow"
                );
                markers.push(line.to_owned());
            }
            Ok(markers)
        })
        .collect()
}
// Only the closed marker name changes for the existing canonical parser.
// Every exact identity field is retained and decoded at the production boundary.
fn decode_prepared_root_marker(line: &str) -> Result<TaskIdentity> {
    let suffix = line
        .strip_prefix(PREPARED_ROOT)
        .filter(|suffix| suffix.starts_with(' '))
        .context("prepared root marker has unexpected prefix")?;
    let normalized = format!("{CREATE}{suffix}\n");
    let candidates = parse_created_task_candidates(&[String::new()], &[normalized], 0)?;
    ensure!(
        candidates.len() == 1,
        "prepared root identity is not unique"
    );
    Ok(decode_task_identity(
        &candidates[0],
        FieldPath::root("independent_prepared_client_root"),
    )?)
}
fn independent_target(
    before: &[String],
    after: &[String],
    occupied: usize,
    processes: &[BackendProcessId],
) -> Result<(TaskIdentity, FrontendProcessId)> {
    ensure!(
        processes.len() == 3 && occupied < processes.len(),
        "actual backend process inventory is not three BEs"
    );
    let markers = appended_markers(before, after)?;
    let marker_logs: Vec<_> = markers
        .iter()
        .map(|lines| {
            let mut log = lines.join("\n");
            if !log.is_empty() {
                log.push('\n');
            }
            log
        })
        .collect();
    let created_count = markers
        .iter()
        .flatten()
        .filter(|line| line.contains(CREATE))
        .count();
    ensure!(
        created_count == 2,
        "whole three-BE set is not exactly two fresh SQL tasks"
    );
    let empty_logs = vec![String::new(); 3];
    let mut tasks = Vec::with_capacity(2);
    let mut participating = [false; 3];
    for (be, lines) in markers.iter().enumerate() {
        if !lines.iter().any(|line| line.contains(CREATE)) {
            continue;
        }
        participating[be] = true;
        for candidate in parse_created_task_candidates(&empty_logs, &marker_logs, be)? {
            let task = decode_task_identity(&candidate, FieldPath::root("independent_sql_task"))?;
            ensure!(
                task.backend_process_id() == processes[be],
                "created task backend UUID differs from actual process/descriptor"
            );
            tasks.push((be, task));
        }
    }
    ensure!(tasks.len() == 2, "two unique SQL tasks were not decoded");
    let execution = tasks[0].1.query_execution_id();
    ensure!(
        tasks
            .iter()
            .all(|(_, task)| task.query_execution_id() == execution),
        "fresh SQL tasks span multiple executions"
    );
    let roots: Vec<_> = markers
        .iter()
        .enumerate()
        .flat_map(|(be, lines)| {
            lines
                .iter()
                .filter(|line| line.contains(PREPARED_ROOT))
                .map(move |line| (be, line))
        })
        .collect();
    ensure!(
        roots.len() == 1 && roots[0].0 == occupied,
        "unique prepared ClientRows root differs from occupied census backend"
    );
    let task = decode_prepared_root_marker(roots[0].1)?;
    ensure!(
        task.backend_process_id() == processes[occupied] && tasks.contains(&(occupied, task)),
        "prepared root is not an exact created task on its actual backend"
    );
    let participation_markers: Vec<_> = markers
        .iter()
        .enumerate()
        .flat_map(|(be, lines)| {
            lines
                .iter()
                .filter(|line| line.contains(ESTABLISH))
                .map(move |line| (be, line))
        })
        .collect();
    ensure!(
        participation_markers.len()
            == participating
                .iter()
                .filter(|participates| **participates)
                .count(),
        "participating backends lack exactly one fresh participation each"
    );
    let expected = format!(
        "execution_id={}:{}:{}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get()
    );
    let mut seen_participations = [false; 3];
    let mut frontend = None;
    for (be, line) in participation_markers {
        ensure!(
            participating[be] && !seen_participations[be],
            "duplicate participation or participation outside participating backend"
        );
        seen_participations[be] = true;
        let fields: Vec<_> = line.split(' ').collect();
        ensure!(
            fields.len() == 4 && fields[0] == ESTABLISH && fields[1] == expected,
            "participation establish execution differs from SQL task execution"
        );
        let frontend_text = fields[2]
            .strip_prefix("frontend=")
            .context("missing canonical frontend identity")?;
        let observed_frontend: FrontendProcessId = frontend_text.parse()?;
        ensure!(
            observed_frontend.to_string() == frontend_text
                && fields[3] == format!("backend={}", processes[be]),
            "participation establish has noncanonical frontend or different backend UUID"
        );
        if let Some(frontend) = frontend {
            ensure!(
                frontend == observed_frontend,
                "fresh participations span multiple frontend UUIDs"
            );
        } else {
            frontend = Some(observed_frontend);
        }
    }
    ensure!(
        seen_participations == participating,
        "fresh participation membership differs from task membership"
    );
    Ok((
        task,
        frontend.context("missing independent frontend participation identity")?,
    ))
}
fn census(
    context: &mut ScenarioContext,
    deadline: Instant,
) -> Result<Vec<Option<BTreeMap<String, u64>>>> {
    let ports: Vec<_> = context
        .handle()
        .runtime()
        .be
        .iter()
        .map(|be| be.http)
        .collect();
    ports
        .into_iter()
        .map(|port| {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(
                !remaining.is_zero(),
                "root census absolute deadline elapsed"
            );
            let client = reqwest::blocking::Client::builder()
                .timeout(remaining.min(Duration::from_millis(500)))
                .build()?;
            let mut bytes = Vec::with_capacity(METRIC_CAP + 1);
            client
                .get(format!("http://127.0.0.1:{port}/metrics?type=json"))
                .send()?
                .error_for_status()?
                .take((METRIC_CAP + 1) as u64)
                .read_to_end(&mut bytes)?;
            ensure!(
                bytes.len() <= METRIC_CAP,
                "metric response exceeds 1 MiB cap"
            );
            let rows: Value = serde_json::from_slice(&bytes)?;
            root_census(rows.as_array().context("invalid root metric array")?)
        })
        .collect()
}
fn zero_roots(context: &mut ScenarioContext, phase: &str, evidence: &mut Value) -> Result<()> {
    let deadline = context.deadline().min(Instant::now() + CAPTURE);
    let mut snapshots = Vec::with_capacity(51);
    let mut consecutive = 0;
    loop {
        ensure!(
            Instant::now() < deadline && snapshots.len() < 51,
            "root owners failed bounded zero barrier"
        );
        let roots = census(context, deadline)?;
        let zero = roots
            .iter()
            .all(|r| r.as_ref().is_some_and(|r| r.values().all(|n| *n == 0)));
        consecutive = if zero { consecutive + 1 } else { 0 };
        snapshots.push(json!(roots));
        evidence[format!("root_zero_{phase}")] = json!(snapshots);
        if consecutive == 2 {
            return Ok(());
        }
        std::thread::sleep(
            Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}
fn check_effective(context: &ScenarioContext, evidence: &mut Value) -> Result<()> {
    let actual: Value =
        serde_json::from_slice(context.effective_launch_config_evidence().artifact_bytes())?;
    let actor = &actual["semantics"]["native_proxy"]["root_reply_message_fault"];
    ensure!(
        actor["backend_indices"] == json!([0, 1, 2])
            && actor["actor"]["target_slots"] == 1
            && actor["actor"]["not_ready_transparency"] == true
            && actor["actor"]["capture_deadline"] == "single-absolute-never-reset"
            && actor["actor"]["bounds"]["maximum_connections"] == 32
            && actor["actor"]["bounds"]["maximum_active_streams"] == 256
            && actor["actor"]["bounds"]["maximum_owned_buffer_bytes"] == 16 * 1024 * 1024
            && actor["actor"]["bounds"]["handshake_timeout_millis"] == 2000
            && actor["actor"]["maximum_target_attempts"] == 32
            && actor["actor"]["bounds"]["maximum_capture_millis"] == 5000
            && actor["actor"]["bounds"]["forward_timeout_millis"] == 20000,
        "effective launch did not enable exact actor v2 bounds on all three Data listeners"
    );
    let roles = actual["semantics"]["roles"]
        .as_array()
        .context("missing effective role configs")?;
    let fe: Vec<_> = roles.iter().filter(|r| r["role"] == "fe").collect();
    ensure!(fe.len() == 1, "effective FE role config is not unique");
    let config = &fe[0]["effective_config"];
    ensure!(
        config["type"] == "table",
        "effective FE config is not canonical table"
    );
    let runtime = &config["value"]["runtime"];
    ensure!(
        runtime.is_null() || runtime["type"] == "table",
        "effective FE runtime is not canonical table"
    );
    let queue = &runtime["value"]["task_operation_queue_residence_ms"];
    let (millis, source) = if queue.is_null() {
        // Server serde default is derived from this public budget constant.
        (
            u64::try_from(
                TransportBudget::DEFAULT
                    .frontend_queue_residence()
                    .as_millis(),
            )?,
            "server-serde-default",
        )
    } else {
        ensure!(
            queue["type"] == "integer",
            "effective queue residence has wrong type"
        );
        (
            queue["value"]
                .as_u64()
                .context("effective queue residence is not positive millis")?,
            "effective-explicit-config",
        )
    };
    ensure!(
        millis >= 5000,
        "effective FE RPC queue grace cannot cover original five-second capture"
    );
    evidence["effective_launch"] = actual;
    evidence["effective_capture_grace"] =
        json!({"queue_residence_millis":millis,"source":source,"capture_millis":5000});
    Ok(())
}

// Hash arbitrary cause formatting incrementally; store no unbounded full text.
struct FailureDigestWriter {
    hash: Sha256,
    bytes: usize,
    length_overflow: bool,
}
impl std::fmt::Write for FailureDigestWriter {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.hash.update(text.as_bytes());
        if let Some(length) = self.bytes.checked_add(text.len()) {
            self.bytes = length;
        } else {
            self.length_overflow = true;
            self.bytes = usize::MAX;
        }
        Ok(())
    }
}
fn failure_summary(class: &str, error: Option<&anyhow::Error>) -> Value {
    use std::fmt::Write as _;
    error.map_or(Value::Null, |error| {
        let mut sink = FailureDigestWriter {
            hash: Sha256::new(),
            bytes: 0,
            length_overflow: false,
        };
        let format_failed = write!(&mut sink, "{error:#}").is_err();
        json!({"class":class,"bytes":sink.bytes,"sha256":format!("{:x}",sink.hash.finalize()),
            "length_overflow":sink.length_overflow,"format_failed":format_failed})
    })
}

#[cfg(test)]
mod root_reply_predicate_tests {
    use super::*;

    fn identity_oracle_fixture(
        split_tasks: bool,
    ) -> (Vec<String>, Vec<String>, Vec<BackendProcessId>) {
        let processes: Vec<BackendProcessId> = [
            "01900000-0000-7000-8000-000000000001",
            "01900000-0000-7000-8000-000000000002",
            "01900000-0000-7000-8000-000000000003",
        ]
        .into_iter()
        .map(|text| text.parse().expect("v7 backend"))
        .collect();
        let mut after = vec![String::new(); 3];
        let upstream = if split_tasks { 2 } else { 1 };
        after[upstream].push_str(&format!(
            "{CREATE} execution_id=-7:23:2 stage=1 task=1 backend={}\n",
            processes[upstream]
        ));
        after[1].push_str(&format!(
            "{CREATE} execution_id=-7:23:2 stage=2 task=2 backend={}\n\
             {PREPARED_ROOT} execution_id=-7:23:2 stage=2 task=2 backend={}\n",
            processes[1], processes[1]
        ));
        for be in 0..3 {
            if !after[be].is_empty() {
                after[be].push_str(&format!(
                    "{ESTABLISH} execution_id=-7:23:2 frontend=01900000-0000-7000-8000-000000000004 backend={}\n",
                    processes[be]
                ));
            }
        }
        (vec![String::new(); 3], after, processes)
    }
    fn replace_marker(after: &[String], prefix: &str, from: &str, to: &str) -> Vec<String> {
        after
            .iter()
            .map(|log| {
                log.lines()
                    .map(|line| {
                        if line.starts_with(prefix) {
                            line.replace(from, to)
                        } else {
                            line.to_owned()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
                    + if log.is_empty() { "" } else { "\n" }
            })
            .collect()
    }
    #[test]
    fn two_created_tasks_resolve_only_the_unique_prepared_root() {
        for split in [false, true] {
            let (before, after, processes) = identity_oracle_fixture(split);
            let (root, frontend) =
                independent_target(&before, &after, 1, &processes).expect("independent root");
            assert_eq!(root.stage_id().get(), 2);
            assert_eq!(root.task_id().get(), 2);
            assert_eq!(root.backend_process_id(), processes[1]);
            assert_eq!(frontend.to_string(), "01900000-0000-7000-8000-000000000004");
            if !split {
                // Root selection follows an exact marker even for a lower stage.
                let lower =
                    replace_marker(&after, PREPARED_ROOT, "stage=2 task=2", "stage=1 task=1");
                let (root, _) = independent_target(&before, &lower, 1, &processes)
                    .expect("explicit lower root");
                assert_eq!(root.stage_id().get(), 1);
            }
        }
    }
    #[test]
    fn missing_or_multiple_prepared_roots_fail_identity_oracle() {
        let (before, mut after, processes) = identity_oracle_fixture(false);
        let root = after[1]
            .lines()
            .find(|line| line.starts_with(PREPARED_ROOT))
            .expect("root")
            .to_owned();
        after[1] = after[1].replace(&(root.clone() + "\n"), "");
        assert!(independent_target(&before, &after, 1, &processes).is_err());
        after[1].push_str(&(root.clone() + "\n"));
        after[1].push_str(&(root + "\n"));
        assert!(independent_target(&before, &after, 1, &processes).is_err());
    }
    #[test]
    fn extra_or_duplicate_created_tasks_fail_identity_oracle() {
        let (before, after, processes) = identity_oracle_fixture(false);
        for task in [1, 3] {
            let mut changed = after.clone();
            changed[1].push_str(&format!(
                "{CREATE} execution_id=-7:23:2 stage=1 task={task} backend={}\n",
                processes[1]
            ));
            assert!(independent_target(&before, &changed, 1, &processes).is_err());
        }
        // Exactly two lines with a duplicate tuple also fail the typed parser.
        let duplicate = replace_marker(&after, CREATE, "stage=2 task=2", "stage=1 task=1");
        assert!(independent_target(&before, &duplicate, 1, &processes).is_err());
    }
    #[test]
    fn other_execution_and_noncanonical_root_fields_fail_identity_oracle() {
        let (before, after, processes) = identity_oracle_fixture(false);
        for (from, to) in [
            ("-7:23:2", "-7:24:2"),
            ("stage=2 task=2", "stage=2 task=+2"),
            ("stage=2 task=2", "task=2 stage=2"),
            ("stage=2 task=2", "stage=2 task=2 extra=1"),
            ("stage=2 task=2", "stage=2 task=3"),
        ] {
            let changed = replace_marker(&after, PREPARED_ROOT, from, to);
            assert!(independent_target(&before, &changed, 1, &processes).is_err());
        }
        let mut changed = after.clone();
        changed[1] = changed[1].replacen("-7:23:2", "-7:24:2", 1);
        assert!(independent_target(&before, &changed, 1, &processes).is_err());
    }
    #[test]
    fn backend_and_occupied_census_mismatches_fail_identity_oracle() {
        let (before, after, processes) = identity_oracle_fixture(false);
        for prefix in [CREATE, PREPARED_ROOT, ESTABLISH] {
            let changed = replace_marker(
                &after,
                prefix,
                &processes[1].to_string(),
                &processes[0].to_string(),
            );
            assert!(independent_target(&before, &changed, 1, &processes).is_err());
        }
        assert!(independent_target(&before, &after, 0, &processes).is_err());
    }
    #[test]
    fn participation_membership_execution_and_full_frontend_must_match() {
        let (before, after, processes) = identity_oracle_fixture(true);
        for mode in 0..5 {
            let mut changed = after.clone();
            let participation_marker = changed[2]
                .lines()
                .find(|line| line.starts_with(ESTABLISH))
                .expect("participation marker")
                .to_owned();
            match mode {
                0 => changed[2] = changed[2].replace(&(participation_marker.clone() + "\n"), ""),
                1 => changed[2].push_str(&(participation_marker.clone() + "\n")),
                2 => changed[0].push_str(
                    &(participation_marker
                        .replace(&processes[2].to_string(), &processes[0].to_string())
                        + "\n"),
                ),
                3 => changed[2] = changed[2].replace("8000-000000000004", "8000-000000000005"),
                _ => changed = replace_marker(&after, ESTABLISH, "-7:23:2", "-7:24:2"),
            }
            assert!(independent_target(&before, &changed, 1, &processes).is_err());
        }
    }
    #[test]
    fn marker_slots_line_bound_and_full_newline_fail_closed() {
        let (before, after, processes) = identity_oracle_fixture(false);
        for prefix in [CREATE, ESTABLISH, PREPARED_ROOT] {
            let mut overflow = after.clone();
            overflow[0] = (0..=MARKERS_PER_BACKEND)
                .map(|_| format!("{prefix} invalid\n"))
                .collect();
            assert!(appended_markers(&before, &overflow).is_err());
        }
        let mut long = after.clone();
        long[0] = format!("{PREPARED_ROOT} {}\n", "x".repeat(384));
        assert!(appended_markers(&before, &long).is_err());
        let mut partial = after.clone();
        partial[1].pop();
        assert!(appended_markers(&before, &partial).is_err());
        let mut midline = before.clone();
        midline[1] = "partial".to_owned();
        let mut appended = after.clone();
        appended[1] = "partial".to_owned() + &appended[1];
        assert!(appended_markers(&midline, &appended).is_err());
        assert!(independent_target(&before, &after, 1, &processes[..2]).is_err());
    }

    #[test]
    fn raw_trino_refusal_rejects_success_cancel_transport_shape_and_partial_rows() {
        let freeze: Freeze = serde_json::from_str(Case::Profile.freeze()).unwrap();
        let original = json!({"id":"original-query","stats":{"state":"FAILED"},
            "error":{"message":freeze.trino_message,"errorCode":65536,"errorName":"GENERIC_INTERNAL_ERROR","errorType":"INTERNAL_ERROR"}});
        let page: NegativePage = serde_json::from_value(original.clone()).unwrap();
        check_negative_page(&page, "original-query").unwrap();
        check_trino(
            &NegativeClientAttempt {
                pages: 1,
                rows: 0,
                columns: None,
                terminal_error: page.error.unwrap(),
            },
            &freeze,
        )
        .unwrap();
        for change in [
            "query",
            "row",
            "completion",
            "nonterminal",
            "success",
            "cancel",
        ] {
            let mut value = original.clone();
            match change {
                "query" => value["id"] = json!("foreign-query"),
                "row" => value["data"] = json!([["already-published"]]),
                "completion" => value["updateType"] = json!("SELECT"),
                "nonterminal" => value["nextUri"] = json!("http://127.0.0.1:1/unexpected"),
                "success" => {
                    value.as_object_mut().unwrap().remove("error");
                    value["stats"]["state"] = json!("FINISHED");
                }
                "cancel" => {
                    value["error"]["errorCode"] = json!(3);
                    value["error"]["errorName"] = json!("USER_CANCELED");
                }
                _ => unreachable!(),
            }
            let page: NegativePage = serde_json::from_value(value).unwrap();
            if change == "cancel" {
                assert!(
                    check_trino(
                        &NegativeClientAttempt {
                            pages: 1,
                            rows: 0,
                            columns: None,
                            terminal_error: page.error.unwrap()
                        },
                        &freeze
                    )
                    .is_err()
                );
            } else {
                assert!(
                    check_negative_page(&page, "original-query").is_err(),
                    "accepted {change}"
                );
            }
        }
        assert!(
            serde_json::from_str::<NegativePage>(
                r#"{"id":"original-query","stats":{"state":"FAILED"},"error":{"message":"cut"}"#
            )
            .is_err()
        );
    }

    // These immutable observations test the runner predicate only. They do
    // not assert that a real FE consumed native bytes or establish identity.
    fn data_fixture(end: Value, freeze: &Freeze) -> RootReplyFaultObservation {
        let original = json!({"task":"test-only-task","profile":1,"kind":"ClientRows","sequence":1,
            "accepted_consumed":0,"body_bytes":72,"body_sha256":freeze.expected_native_body_sha256,"end_after_data":end});
        let mutated = json!({"task":"test-only-task","profile":2,"output_kind":"Some(ClientRows(true))","sequence":1,
            "accepted_consumed":0,"mutation":"ProfileTwo","body_sha256":freeze.expected_native_body_sha256,
            "end_after_data":original["end_after_data"]});
        RootReplyFaultObservation {
            active_listeners: 3,
            joined_listeners: 0,
            active_connections: 1,
            active_streams: 0,
            connection_positions: 1,
            stream_positions: 0,
            target_slots: 0,
            owned_buffer_bytes: 0,
            peak_owned_buffer_bytes: 72,
            claimed: 1,
            emitted_messages: 1,
            frozen_root_task: Some("test-only-task".to_owned()),
            frozen_request: None,
            target_requests: 1,
            target_consumed_positive_requests: 0,
            joined_connections: 0,
            joined_children: 0,
            pressure_listeners: 0,
            idle_drains_started: 0,
            idle_drains_completed: 0,
            idle_drain_yields: 0,
            connection_bound_buffer_charges: 0,
            connection_bound_buffer_drops: 0,
            standalone_buffer_charges: 0,
            shutdown_joined: false,
            failures: vec![],
            failure_overflow: false,
            first_failure: None,
            first_overflow: None,
            last_overflow: None,
            overflow_failures: 0,
            target_attempts: 1,
            not_ready_observed: 0,
            not_ready_forwarded: 0,
            non_target_peer_cancels: 0,
            preapplication_backend_indices: [Some(0), Some(1), Some(2)],
            preapplication_peer_closes: [0; 3],
            status_subscription_peer_cancels: 0,
            last_status_subscription_peer_cancel: None,
            not_ready_replies: vec![],
            target_attempt_requests: vec![],
            original: Some(original),
            mutated: Some(mutated),
            original_hold: None,
        }
    }
    #[test]
    fn frozen_suffix_literal_and_optional_end_are_exact() {
        let freeze: Freeze = serde_json::from_str(Case::PrefixSuffix.freeze()).expect("freeze");
        validate_suffix_freeze(&freeze, Case::PrefixSuffix).unwrap();
        let mut literal = payload_literal();
        assert_eq!(
            format!("{:x}", Sha256::digest(&literal)),
            freeze.expected_native_body_sha256
        );
        literal.extend_from_slice(&[1, 0, 0, 0]);
        let digest = format!("{:x}", Sha256::digest(&literal));
        assert_eq!(literal.len(), freeze.expected_mutated_body_bytes.unwrap());
        assert_eq!(
            Some(digest.as_str()),
            freeze.expected_mutated_body_sha256.as_deref()
        );
        for end in [Value::Null, json!({"sequence":2,"output_rows":1})] {
            let mut observed = data_fixture(end, &freeze);
            let mutated = observed.mutated.as_mut().unwrap();
            mutated["mutation"] = json!("ValidRowPrefixOnlySuffix");
            mutated["profile"] = json!(1);
            mutated["output_kind"] = json!("ClientRows(true)");
            mutated["body_bytes"] = json!(76);
            mutated["body_sha256"] = json!(digest);
            check_fault(&observed, &freeze, Case::PrefixSuffix).unwrap();
            observed.mutated.as_mut().unwrap()["body_sha256"] =
                json!(freeze.expected_native_body_sha256);
            assert!(check_fault(&observed, &freeze, Case::PrefixSuffix).is_err());
        }
        let mut changed: Freeze = serde_json::from_str(Case::PrefixSuffix.freeze()).unwrap();
        changed.expected_mutated_body_bytes = Some(72);
        assert!(validate_suffix_freeze(&changed, Case::PrefixSuffix).is_err());
        for case in [Case::Profile, Case::Kind, Case::Prefix] {
            let old: Freeze = serde_json::from_str(case.freeze()).unwrap();
            validate_suffix_freeze(&old, case).unwrap();
        }
    }

    #[test]
    fn data_without_piggyback_end_is_legal_but_some_end_is_exact_and_immutable() {
        let freeze: Freeze = serde_json::from_str(Case::Profile.freeze()).expect("freeze");
        for end in [Value::Null, json!({"sequence":2,"output_rows":1})] {
            check_fault(&data_fixture(end, &freeze), &freeze, Case::Profile)
                .expect("legal real Data1 form");
        }
        for end in [
            json!({"sequence":3,"output_rows":1}),
            json!({"sequence":2,"output_rows":2}),
            json!({"sequence":2,"output_rows":0}),
        ] {
            assert!(
                check_fault(&data_fixture(end, &freeze), &freeze, Case::Profile).is_err(),
                "invalid piggyback End accepted"
            );
        }
        let mut changed = data_fixture(Value::Null, &freeze);
        changed.mutated.as_mut().expect("mutated")["end_after_data"] =
            json!({"sequence":2,"output_rows":1});
        assert!(
            check_fault(&changed, &freeze, Case::Profile).is_err(),
            "mutation fabricated End"
        );
        let mut removed = data_fixture(json!({"sequence":2,"output_rows":1}), &freeze);
        removed.mutated.as_mut().expect("mutated")["end_after_data"] = Value::Null;
        assert!(
            check_fault(&removed, &freeze, Case::Profile).is_err(),
            "mutation removed real End"
        );
    }
}
