// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One original S+8 Trino query, a real held Native replay and two sealed ACKs.
//! Ordinary explicit registry entry; the original Native driver owns its actual
//! response/credit and Join. The FE proxy copy is not that encoded response.
use super::independent_root_target::{
    FrontendIdentitySource, IndependentRootObserver, IndependentRootTarget,
};
use super::native_sql_controls::{SqlIntentOwner, StatementSpec};
use super::result_delivery_root_protocol::{
    held_response::{HeldObservation, HeldRootResponse, JoinedOriginalConnection, ProvenReplayOne},
    probe_owned,
};
use super::result_delivery_support::{
    OriginalRootCensus as Census, await_original_root_idle as await_idle,
    check_original_root_deadline as check, original_root_census as census,
    sleep_original_root_sample as sleep_until_sample,
};
use crate::actors::sql::{SqlSession, TrinoValue, single_integer};
use crate::actors::trino_protocol::TrinoClient;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::native_root_reply_fault::{
    RootReplyFaultBounds, RootReplyFaultObservation, RootReplyOriginalDisposition,
    RootReplyOriginalIdentity,
};
use novarocks_cluster_harness::process_resources::ProcessLaunchIdentity;
use novarocks_cluster_harness::{
    CrossProcessRootReplyFaultConfig, LaunchProfile, TestClusterOwner,
};
use novarocks_execution_contract::{TaskIdentity, root_result::RootResultRead};
use novarocks_proto_codec::FieldPath;
use novarocks_result_contract::{RootOutputKind, RootProfileId};
use novarocks_task_codec::root_result::{decode_read, encode_read};
use novarocks_test_trino_client::{Column, TrinoError, TypeArgument};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    num::NonZeroU64,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::runtime::Runtime;

const NAME: &str = "result-delivery/held-response-late-ack";
const SQL: &str = "SELECT REPEAT('x', 1048576) AS payload FROM generate_series(1, 1)";
const HEALTH_SQL: &str = "SELECT SUM(generate_series) AS total FROM generate_series(1, 100)";
const S: u64 = 1_048_576;
const SAMPLES: usize = 51;
const RECEIPT_BYTES: usize = 131_072;
const WHOLE: Duration = Duration::from_secs(20);
const PROTOCOL: Duration = Duration::from_secs(5);
const ROOT_FIELDS: [&str; 14] = [
    "channels",
    "terminal_task_records",
    "producers_running",
    "producers_exited",
    "ends_published",
    "ends_acknowledged",
    "sealed",
    "data_positions",
    "payload_bytes",
    "segments",
    "deliveries",
    "retained_reservations",
    "metadata_holders",
    "metadata_bytes",
];

pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(HeldLateAck::default())]
}
#[derive(Default)]
struct HeldLateAck {
    clock: Mutex<Option<Instant>>,
    intent: SqlIntentOwner,
}
impl HeldLateAck {
    fn clock(&self) -> Result<Instant> {
        self.clock
            .lock()
            .map_err(|_| anyhow::anyhow!("original Root clock poisoned"))?
            .context("original Root clock was not captured before role launch")
    }
}
impl Scenario for HeldLateAck {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        super::declarations::for_module("result_delivery_held_late_ack", self.name(), true, false)
    }
    fn name(&self) -> &'static str {
        NAME
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        ensure!(
            profile == LaunchProfile::FaultScenario,
            "held response requires fault-scenario profile"
        );
        Ok(())
    }
    fn launch_config(&self, _: &Path) -> Result<ScenarioLaunchConfig> {
        let mut clock = self
            .clock
            .lock()
            .map_err(|_| anyhow::anyhow!("original Root clock poisoned"))?;
        ensure!(clock.is_none(), "original prelaunch clock already captured");
        *clock = Some(Instant::now() + WHOLE);
        Ok(ScenarioLaunchConfig {
            native_root_reply_fault: Some(CrossProcessRootReplyFaultConfig {
                backend_indices: (0..3).collect(),
                bounds: RootReplyFaultBounds::default(),
            }),
            native_fault_proxies: novarocks_cluster_harness::CrossProcessNativeFaultProxyConfig {
                backend_retained_byte_limits: (0..3).map(|index| (index, 1_048_576)).collect(),
            },
            ..ScenarioLaunchConfig::default()
        })
    }
    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        ensure!(
            deadline == self.clock()?,
            "held declaration changed its original prelaunch clock"
        );
        Ok(Some(self.intent.freeze_root_observation(
            vec![StatementSpec {
                    sql: SQL.into(),
                    faults: vec![],
                    observations: (0..cluster_size)
                        .map(|index| (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), Some(1)))
                        .collect(),
                }],
            cluster_size,
            binary,
            deadline,
        )?))
    }
    fn root_observation_deadline(&self) -> Result<Option<Instant>> {
        Ok(Some(self.clock()?))
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let deadline = self.clock()?;
        check(deadline)?;
        ensure!(
            context.deadline() == deadline && context.handle().be_count() == 3,
            "held scene lost its original 1FE+3BE clock"
        );
        for index in 0..3 {
            context.handle().native_exchange_fault(index)?;
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        context.retain_artifacts();
        run_owned(context, &runtime, deadline, &self.intent)
    }
    fn teardown(&self) -> Result<()> {
        check(self.clock()?)
    }
}

#[derive(Default)]
struct Facts {
    samples: Vec<Value>,
    actor: Option<HeldObservation>,
    root: Option<Value>,
    target_source: Option<Value>,
    closed_acks: Vec<Value>,
    original_prelaunch: Option<Instant>,
    health_before_tasks_created: Option<[u64; 3]>,
    health_after_tasks_created: Option<[u64; 3]>,
    protocol_complete: bool,
    actor_actual_join: bool,
    query_actual_join: bool,
    delete_attempted: bool,
    delete_returned: bool,
    protocol_started_from_original_prelaunch_us: Option<u128>,
    delete_started_from_original_prelaunch_us: Option<u128>,
    delete_returned_from_original_prelaunch_us: Option<u128>,
    settled_from_original_prelaunch_us: Option<u128>,
    trino_terminal: Option<TrinoError>,
    trino_pages: u64,
    health: Option<Value>,
    expected_server_err_retained: bool,
    expected_connection_cancel_retained: bool,
}
struct OwnedFailure {
    errors: [Option<anyhow::Error>; 12],
}
impl std::fmt::Debug for OwnedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldLateAckFailure")
            .field("failed_slots", &self.errors.each_ref().map(Option::is_some))
            .finish()
    }
}
impl std::fmt::Display for OwnedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("held late ACK failed; original primary and cleanup sources retained")
    }
}
impl std::error::Error for OwnedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.errors
            .iter()
            .flatten()
            .next()
            .map(|error| error.as_ref())
    }
}
#[derive(Debug)]
struct SecondaryFailure {
    primary: anyhow::Error,
    secondary: anyhow::Error,
}
impl std::fmt::Display for SecondaryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _retained = &self.secondary;
        f.write_str("original held primary and cleanup errors retained")
    }
}
impl std::error::Error for SecondaryFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
fn retain_secondary(slot: &mut Option<anyhow::Error>, secondary: anyhow::Error) {
    *slot = Some(match slot.take() {
        Some(primary) => SecondaryFailure { primary, secondary }.into(),
        None => secondary,
    });
}
struct PublicTerminal {
    error: TrinoError,
    pages: u64,
}
type PublicJob = std::thread::JoinHandle<Result<PublicTerminal>>;
fn public_query(
    actor: TrinoClient,
    query: String,
    first: String,
    live: Arc<Mutex<Option<String>>>,
) -> Result<PublicTerminal> {
    let mut uri = first;
    let mut pages = 0u64;
    loop {
        let raw = actor.get(&uri)?;
        ensure!(
            raw.status == 200,
            "original public query returned HTTP failure instead of Trino terminal error"
        );
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Page {
            id: String,
            next_uri: Option<String>,
            error: Option<TrinoError>,
            data: Option<Vec<Value>>,
            columns: Option<Vec<Column>>,
            stats: Value,
            update_type: Option<String>,
            update_count: Option<u64>,
        }
        let page: Page = serde_json::from_slice(&raw.body)?;
        *live
            .lock()
            .map_err(|_| anyhow::anyhow!("original continuation owner poisoned"))? =
            page.next_uri.clone();
        ensure!(
            page.id == query
                && page.update_type.is_none()
                && page.update_count.is_none()
                && page.data.as_ref().is_none_or(Vec::is_empty),
            "held original query changed identity or published data before its held first Root response"
        );
        if let Some(columns) = page.columns {
            ensure!(
                columns.len() == 1
                    && columns[0].name == "payload"
                    && columns[0].type_name == "varchar"
                    && columns[0].type_signature.raw_type == "varchar"
                    && columns[0].type_signature.arguments == [TypeArgument::Long(2_147_483_647)],
                "held original Trino declaration differs"
            );
        }
        pages = pages
            .checked_add(1)
            .context("original public page count overflow")?;
        if let Some(error) = page.error {
            ensure!(
                page.next_uri.is_none() && page.stats["state"] == "FAILED",
                "original query error was not terminal"
            );
            return Ok(PublicTerminal { error, pages });
        }
        uri = page
            .next_uri
            .context("held query unexpectedly succeeded before exact cancellation")?;
    }
}
#[derive(Debug)]
struct OriginalPublicPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Display for OriginalPublicPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _original = &self.0;
        f.write_str("original public held-query thread panicked; actual payload retained")
    }
}
impl std::error::Error for OriginalPublicPanic {}
fn join_public(job: PublicJob) -> Result<PublicTerminal> {
    job.join()
        .map_err(|payload| anyhow::Error::new(OriginalPublicPanic(Mutex::new(payload))))?
}

fn request(root: TaskIdentity, ack: bool) -> Result<RootResultRead> {
    let read = RootResultRead::try_new(
        root,
        RootProfileId::V1,
        RootOutputKind::ClientRows,
        if ack { None } else { NonZeroU64::new(1) },
        if ack { 1 } else { 0 },
        Duration::from_millis(100),
    )?;
    // Validate the actual submitted DTO with the production decoder, not shape only.
    ensure!(
        decode_read(
            &encode_read(&read),
            FieldPath::root("held_late_ack_request")
        )? == read,
        "actual submitted root request changed typed contract"
    );
    Ok(read)
}
fn identity(root: TaskIdentity) -> Value {
    // Existing encoder defines every exact identity field, including signed query bits.
    let execution = root.query_execution_id();
    json!({"query_hi":execution.query_id().high(),"query_lo":execution.query_id().low(),
        "attempt":execution.attempt_id().get(),"stage":root.stage_id().get(),
        "task":root.task_id().get(),"backend":root.backend_process_id().to_string()})
}
fn roots_match(
    roots: &[BTreeMap<String, u64>; 3],
    backend: usize,
    sealed: bool,
    positive_holder: bool,
) -> Result<bool> {
    ensure!(backend < 3, "selected backend outside original inventory");
    for root in roots {
        ensure!(
            root.len() == ROOT_FIELDS.len()
                && ROOT_FIELDS.iter().all(|name| root.contains_key(*name)),
            "root census shape differs from actual fixed projection"
        );
    }
    if roots
        .iter()
        .enumerate()
        .any(|(index, root)| index != backend && root.values().any(|value| *value != 0))
    {
        return Ok(false);
    }
    let root = &roots[backend];
    let common = root["channels"] == 1
        && root["terminal_task_records"] == 1
        && root["producers_running"] == 0
        && root["producers_exited"] == 1
        && root["ends_published"] == 1
        && root["ends_acknowledged"] == 0
        && root["sealed"] == u64::from(sealed);
    if !common {
        return Ok(false);
    }
    // The completed unary encode copies the body into its own backing. A held
    // Native send keeps that copy's grant, delivery and metadata alive, while
    // seal retires the two original Worker segments. These are separate facts.
    if sealed {
        if root["data_positions"] != 0 || root["payload_bytes"] != 0 || root["segments"] != 0 {
            return Ok(false);
        }
    } else if root["data_positions"] != 2 || root["payload_bytes"] != S + 8 || root["segments"] != 2
    {
        return Ok(false);
    }
    if positive_holder {
        Ok(root["deliveries"] > 0
            && root["retained_reservations"] > 0
            && root["metadata_holders"] > 0
            && root["metadata_bytes"] > 0)
    } else {
        Ok(root["deliveries"] == 0
            && root["retained_reservations"] == 0
            && root["metadata_holders"] == 0)
    }
}
fn same_target(
    observer: &IndependentRootObserver,
    context: &mut ScenarioContext,
    original: &IndependentRootTarget,
    deadline: Instant,
) -> Result<()> {
    let fresh = observer.observe_until(context, deadline)?;
    ensure!(
        fresh.root == original.root
            && fresh.root_backend_index == original.root_backend_index
            && fresh.backend_process_from_descriptor == original.backend_process_from_descriptor
            && fresh.frontend == original.frontend
            && fresh.participations == original.participations
            && fresh.fresh_tasks == original.fresh_tasks,
        "closed original root/context/task inventory changed"
    );
    Ok(())
}
fn sample(
    context: &mut ScenarioContext,
    runtime: &Runtime,
    facts: &mut Facts,
    deadline: Instant,
    label: &'static str,
) -> Result<Census> {
    check(deadline)?;
    ensure!(
        facts.samples.len() < SAMPLES,
        "original sample positions exhausted"
    );
    let value = runtime.block_on(census(context, deadline))?;
    let observed_us = Instant::now()
        .saturating_duration_since(
            facts
                .original_prelaunch
                .context("original census clock origin absent")?,
        )
        .as_micros();
    facts.samples.push(
        json!({"phase":label,"roots":value.roots,"tasks_created":value.created,
        "observed_from_original_prelaunch_us":observed_us}),
    );
    check(deadline)?;
    Ok(value)
}
fn qualify(
    context: &mut ScenarioContext,
    runtime: &Runtime,
    facts: &mut Facts,
    observer: &IndependentRootObserver,
    target: &IndependentRootTarget,
    deadline: Instant,
    sealed: bool,
    holder: bool,
    label: &'static str,
) -> Result<()> {
    loop {
        same_target(observer, context, target, deadline)?;
        let value = sample(context, runtime, facts, deadline, label)?;
        if roots_match(&value.roots, target.root_backend_index, sealed, holder)? {
            return Ok(());
        }
        runtime.block_on(sleep_until_sample(deadline))?; // original fixed 100 ms
    }
}
fn original_role_inventory(
    (frontend, backends): (&ProcessLaunchIdentity, &[ProcessLaunchIdentity]),
) -> Result<Value> {
    ensure!(
        frontend.role == "fe"
            && backends.len() == 3
            && backends
                .iter()
                .zip(["be-0", "be-1", "be-2"])
                .all(|(role, name)| role.role == name),
        "original held role inventory differs from the declared topology"
    );
    // The harness API returns (FE, BE slice). Serialize the same four original
    // launch identities in order, not that tuple's nested JSON representation.
    Ok(serde_json::to_value([
        frontend,
        &backends[0],
        &backends[1],
        &backends[2],
    ])?)
}

fn run_owned(
    context: &mut ScenarioContext,
    runtime: &Runtime,
    deadline: Instant,
    intent: &SqlIntentOwner,
) -> Result<()> {
    let original_roles = original_role_inventory(context.process_launch_identities())?;
    let control = context.handle().native_root_reply_fault(0)?;
    let mut actor: Option<HeldRootResponse> = None;
    let mut job: Option<PublicJob> = None;
    let mut http = None::<TrinoClient>;
    let live = Arc::new(Mutex::new(None::<String>));
    let mut statement = None;
    let mut original = None::<RootReplyOriginalIdentity>;
    let mut protocol_deadline = None;
    let mut delete_started = None;
    let mut replay_joined = None;
    let mut facts = Facts {
        original_prelaunch: Some(deadline - WHOLE),
        ..Facts::default()
    };
    let mut errors: [Option<anyhow::Error>; 12] = std::array::from_fn(|_| None);
    let mut expected_connection_exit = None;
    let mut expected_server_error = None;
    let origin = deadline - WHOLE;
    let primary = (|| -> Result<()> {
        runtime.block_on(await_idle(context, deadline, "held-before"))?;
        let frontend = context
            .handle()
            .original_root_observation_frontend_identity(deadline)?;
        let observer = IndependentRootObserver::capture_baseline_until(
            context,
            frontend,
            deadline,
            FrontendIdentitySource::RootObservation,
        )?;
        // Captured once before submission/discovery; neither another HTTP page
        // nor the independent Native actor renews this original five seconds.
        let phase = deadline.min(Instant::now() + PROTOCOL);
        protocol_deadline = Some(phase);
        facts.protocol_started_from_original_prelaunch_us = Some(origin.elapsed().as_micros());
        let client = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            deadline,
            &context.scenario_root().join("held-original-trino-http"),
        )?
        .with_session(&[("x-trino-time-zone", "Asia/Shanghai")]);
        http = Some(client.clone());
        let pending = client.submit(SQL)?;
        let page = pending.assert_success()?;
        ensure!(
            page["stats"]["state"] == "QUEUED" && page.get("data").is_none(),
            "held POST executed before exact original arm"
        );
        let query = page["id"]
            .as_str()
            .context("held POST omitted original query id")?
            .to_owned();
        let uri = pending
            .next_uri()?
            .context("held POST has no original continuation")?;
        *live
            .lock()
            .map_err(|_| anyhow::anyhow!("original continuation owner poisoned"))? =
            Some(uri.clone());
        statement = Some(intent.take(0)?.arm_client_query(context, &query, SQL)?);
        control.begin_capture(phase)?;
        let original_live = live.clone();
        job = Some(
            std::thread::Builder::new()
                .name("held-original-trino".into())
                .spawn(move || public_query(client, query, uri, original_live))?,
        );
        let installed = context
            .handle()
            .await_statement_root_installed(statement.as_ref().unwrap(), phase)?;
        let target = observer.observe_ready_until(context, phase)?;
        ensure!(
            target.root == installed.root && target.frontend == frontend,
            "held target differs from original FE Installed and independent role facts"
        );
        facts.root = Some(identity(target.root));
        facts.target_source = Some(observer.source_evidence(&target)?);
        loop {
            check(phase)?;
            ensure!(
                !job.as_ref().unwrap().is_finished(),
                "held original query exited before exact Data capture"
            );
            if let Some(candidate) = control.candidate() {
                ensure!(
                    candidate.read.root_task() == target.root
                        && candidate.caller == frontend
                        && candidate.backend_index == target.root_backend_index
                        && candidate.read.profile() == RootProfileId::V1
                        && candidate.read.kind() == RootOutputKind::ClientRows
                        && candidate.read.consumed() == 0
                        && candidate.read.wanted().map(NonZeroU64::get) == Some(1),
                    "held candidate differs from the exact installed original Root"
                );
                control.arm_exact_hold(target.root, frontend, target.root_backend_index)?;
                break;
            }
            runtime.block_on(sleep_until_sample(phase))?;
        }
        loop {
            check(phase)?;
            ensure!(
                !job.as_ref().unwrap().is_finished(),
                "original query exited before its real response became held"
            );
            if let Some(held) = control.observation().original_hold {
                ensure!(
                    held.identity.root == target.root
                        && held.identity.caller == frontend
                        && held.identity.backend_index == target.root_backend_index
                        && held.disposition == RootReplyOriginalDisposition::Held,
                    "original response hold changed identity or settled early"
                );
                original = Some(held.identity);
                break;
            }
            runtime.block_on(sleep_until_sample(phase))?;
        }
        qualify(
            context, runtime, &mut facts, &observer, &target, phase, false, false, "W2",
        )?;
        runtime.block_on(sleep_until_sample(phase))?;
        qualify(
            context,
            runtime,
            &mut facts,
            &observer,
            &target,
            phase,
            false,
            false,
            "W2_repeat",
        )?;
        let read = request(target.root, false)?;
        let wire = encode_read(&read);
        let (reply, _) = probe_owned(context, target.root_backend_index, &wire, 0, phase)?;
        let proof = ProvenReplayOne::from_validated_data1(&read, &reply)?;
        drop(reply); // no decoded data alias substitutes for the real held response
        qualify(
            context,
            runtime,
            &mut facts,
            &observer,
            &target,
            phase,
            false,
            false,
            "quiet_before_replay",
        )?;
        actor = Some(HeldRootResponse::prepare(
            context,
            target.root_backend_index,
            proof,
            &wire,
            phase,
        )?);
        facts.actor = Some(runtime.block_on(actor.as_mut().unwrap().start())?);
        qualify(
            context,
            runtime,
            &mut facts,
            &observer,
            &target,
            phase,
            false,
            true,
            "held_open",
        )?;
        facts.actor = Some(runtime.block_on(actor.as_mut().unwrap().observe_held())?);
        check(phase)?;
        let uri = live
            .lock()
            .map_err(|_| anyhow::anyhow!("original continuation owner poisoned"))?
            .clone()
            .context("original held query lost its active continuation before DELETE")?;
        control.expect_exact_original_query_cancellation(original.as_ref().unwrap())?;
        facts.delete_attempted = true; // retain unknown outcome; never retry this DELETE
        let started = Instant::now();
        delete_started = Some(started);
        facts.delete_started_from_original_prelaunch_us =
            Some(started.duration_since(origin).as_micros());
        let response = http.as_ref().unwrap().delete(&uri)?;
        ensure!(
            matches!(response.status, 200 | 204),
            "held same-query DELETE did not return success"
        );
        check(phase)?;
        facts.delete_returned = true;
        facts.delete_returned_from_original_prelaunch_us = Some(origin.elapsed().as_micros());
        qualify(
            context,
            runtime,
            &mut facts,
            &observer,
            &target,
            phase,
            true,
            true,
            "sealed_held",
        )?;
        for label in ["ACK1_first", "ACK1_repeat"] {
            let ack = request(target.root, true)?;
            let (reply, _) = probe_owned(
                context,
                target.root_backend_index,
                &encode_read(&ack),
                1,
                phase,
            )?;
            actor.as_ref().unwrap().require_closed_ack(&ack, &reply)?;
            ensure!(
                facts.closed_acks.len() < 2,
                "closed ACK observation positions exhausted"
            );
            facts.closed_acks.push(json!({"label":label,"root":identity(reply.root_task),"profile":reply.profile.get(),
                "kind":"ClientRows","wanted":ack.wanted().map(NonZeroU64::get),"consumed":ack.consumed(),
                "accepted_consumed":reply.accepted_consumed,"outcome":"AwaitTerminalControl",
                "observed_from_original_prelaunch_us":origin.elapsed().as_micros()}));
            qualify(
                context, runtime, &mut facts, &observer, &target, phase, true, true, label,
            )?;
            facts.actor = Some(runtime.block_on(actor.as_mut().unwrap().observe_held())?);
        }
        check(phase)?;
        facts.protocol_complete = true;
        Ok(())
    })();
    errors[0] = primary.err();
    // Failed capture/discovery still keeps and cancels the same submitted
    // query at most once. A successful terminal page leaves no live URI.
    if job.is_some() && !facts.delete_attempted {
        let cancelled = (|| -> Result<()> {
            let uri = live
                .lock()
                .map_err(|_| anyhow::anyhow!("original continuation owner poisoned"))?
                .clone();
            if let Some(uri) = uri {
                facts.delete_attempted = true;
                let response = http
                    .as_ref()
                    .context("original query lost its HTTP owner")?
                    .delete(&uri)?;
                ensure!(
                    matches!(response.status, 200 | 204),
                    "original query cleanup DELETE failed"
                );
                facts.delete_returned = true;
            }
            Ok(())
        })();
        errors[1] = cancelled.err();
    }
    // Settle the same real replay actor even after phase expiry. Its sticky
    // clock and actual original driver Join never become a timeout/drop.
    if let Some(owner) = actor.as_mut() {
        match runtime.block_on(owner.settle()) {
            Ok(JoinedOriginalConnection {
                observation,
                actual_expected_cancellation,
            }) => {
                replay_joined = Some(Instant::now());
                facts.actor_actual_join = true;
                facts.actor = Some(observation);
                expected_connection_exit = actual_expected_cancellation;
            }
            Err(error) => {
                facts.actor = Some(error.observation);
                retain_secondary(&mut errors[2], error.into());
            }
        }
    }
    facts.settled_from_original_prelaunch_us = Some(origin.elapsed().as_micros());
    // The FE response copy remains independent of the real replay actor. Only
    // now release that original response, or retain the actual failed release.
    let released = (|| -> Result<()> {
        let original = original
            .as_ref()
            .context("original FE response never became held")?;
        control.release_exact_original(original)?;
        let phase = protocol_deadline.context("original protocol clock was never captured")?;
        loop {
            check(phase)?;
            let observed = control.observation();
            ensure!(
                observed.failures.is_empty(),
                "original FE response actor failed before settle"
            );
            let held = observed
                .original_hold
                .context("original FE hold disappeared")?;
            ensure!(
                &held.identity == original,
                "original FE response changed during release"
            );
            if held.settled_at.is_some() {
                ensure!(
                    matches!(
                        held.disposition,
                        RootReplyOriginalDisposition::ForwardedOriginal
                            | RootReplyOriginalDisposition::ExactDownstreamCancelled
                            | RootReplyOriginalDisposition::JoinedOriginalConnectionExit
                    ),
                    "original FE response did not actually settle"
                );
                break;
            }
            runtime.block_on(sleep_until_sample(phase))?;
        }
        Ok(())
    })();
    errors[8] = released.err();
    if errors[0].is_some() || errors[8].is_some() {
        control.disarm();
    }
    if let Some(job) = job.take() {
        match join_public(job) {
            Ok(value) => {
                facts.query_actual_join = true;
                facts.trino_pages = value.pages;
                let validation = validate_public_terminal(&value.error);
                facts.trino_terminal = Some(value.error.clone());
                if let Err(error) = validation {
                    retain_secondary(&mut errors[3], error);
                } else {
                    expected_server_error = Some(value.error);
                }
            }
            Err(error) => retain_secondary(&mut errors[3], error),
        }
    }
    facts.expected_server_err_retained = expected_server_error.is_some();
    facts.expected_connection_cancel_retained = expected_connection_exit.is_some();
    if errors.iter().all(Option::is_none) {
        errors[4]=(||->Result<()> {
            let before=runtime.block_on(census(context,deadline))?.created;
            facts.health_before_tasks_created=Some(before);
            ensure!(
                facts.actor_actual_join && facts.query_actual_join,
                "original actor and Trino query must join before statement binding retirement"
            );
            context.handle().drain_query_lifecycle_statement_binding(
                statement.as_ref().context(
                    "original target statement observation disappeared before health"
                )?,
                deadline,
            )?;
            drop(statement.take());
            let result=SqlSession::new(context.sql_user(),context.trino_port(),deadline)?.execute(HEALTH_SQL)?;
            ensure!(result.is_result_set() && result.columns.len()==1 && result.columns[0].name=="total"
                && result.columns[0].type_name=="bigint" && result.columns[0].type_signature.raw_type=="bigint"
                && result.columns[0].type_signature.arguments.is_empty()
                && result.rows==[vec![TrinoValue::Integer(5050)]],"ordinary Trino health oracle differs");
            facts.health=Some(json!({"columns":result.columns,"rows":result.rows.len(),"actual_total":single_integer(&result)?}));
            let after=runtime.block_on(census(context,deadline))?.created;
            facts.health_after_tasks_created=Some(after);
            ensure!(after.iter().zip(before).any(|(a,b)|*a>b) && after.iter().zip(before).all(|(a,b)|*a>=b),
                "ordinary health query produced no real Native Tasks");
            check(deadline)
        })().err();
    }
    if let Err(error) = runtime.block_on(await_idle(context, deadline, "held-after")) {
        retain_secondary(&mut errors[5], error);
    }
    if let Some(statement) = statement.as_ref()
        && let Err(error) = context
            .handle()
            .drain_query_lifecycle_statement_binding(statement, deadline)
    {
        retain_secondary(&mut errors[9], error);
    }
    let proxy_before = serde_json::to_value(control.observation());
    if let Err(error) = context.shutdown() {
        retain_secondary(&mut errors[10], error);
    }
    let proxy = control.observation();
    if let Err(error) =
        validate_proxy_join(&proxy, original.as_ref(), delete_started, replay_joined)
    {
        retain_secondary(&mut errors[11], error);
    }
    let receipt = json!({"schema_version":2,"status":if errors.iter().all(Option::is_none){"COMPONENT_AND_SCENE_ASSERTIONS_PASSED_PENDING_RUNNER_ROLE_EVIDENCE"}else{"FAILED"},
        "native_acceptance":false,"original_roles":original_roles,"selected_root":facts.root,
        "independent_target_source":facts.target_source,"closed_acks":facts.closed_acks,
        "effective_launch_config_sha256":context.effective_launch_config_evidence().artifact_sha256(),
        "health_before_tasks_created":facts.health_before_tasks_created,"health_after_tasks_created":facts.health_after_tasks_created,
        "protocol_complete":facts.protocol_complete,"actor":facts.actor,"actor_actual_join":facts.actor_actual_join,
        "query_actual_join":facts.query_actual_join,"delete_attempted":facts.delete_attempted,"delete_returned":facts.delete_returned,
        "timings_origin":"original launch_config prelaunch instant; never renewed at scene entry",
        "protocol_started_us":facts.protocol_started_from_original_prelaunch_us,"delete_started_us":facts.delete_started_from_original_prelaunch_us,
        "delete_returned_us":facts.delete_returned_from_original_prelaunch_us,"settled_us":facts.settled_from_original_prelaunch_us,
        "expected_server_error_actual_source_retained":facts.expected_server_err_retained,"expected_connection_joinerror_retained":facts.expected_connection_cancel_retained,
        "root_samples":facts.samples,"original_trino_terminal":facts.trino_terminal,"original_trino_pages":facts.trino_pages,
        "ordinary_trino_health":facts.health,"proxy_before_shutdown":proxy_before.as_ref().ok(),"proxy_after_shutdown":proxy,
        "failed_source_slots":errors.each_ref().map(Option::is_some),
        "failure_facts":errors.each_ref().map(|error|error.as_ref().map(error_fact)),"raw_error_formatter_invoked":false,
        "scope":"Context-owned unique Root real encoded replay delivery/reservation holders; proxy copy is separate; excludes allocator deallocation, final Arc alias and full Closing pool"});
    if let Err(error) = proxy_before {
        retain_secondary(&mut errors[11], error.into());
    }
    match serde_json::to_vec_pretty(&receipt) {
        Ok(bytes) if bytes.len() <= RECEIPT_BYTES => {
            let write = (|| -> Result<()> {
                use std::io::Write;
                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(context.scenario_root().join("held-late-ack-receipt.json"))?;
                file.write_all(&bytes)?;
                file.sync_all()?;
                Ok(())
            })();
            errors[6] = write.err();
        }
        Ok(_) => errors[6] = Some(anyhow::anyhow!("held late ACK receipt exceeds fixed bound")),
        Err(error) => errors[6] = Some(error.into()),
    }
    if let Err(error) = check(deadline) {
        errors[7] = Some(error);
    }
    drop(expected_connection_exit);
    drop(expected_server_error);
    if errors.iter().any(Option::is_some) {
        return Err(OwnedFailure { errors }.into());
    }
    ensure!(
        facts.protocol_complete && facts.actor_actual_join && facts.query_actual_join,
        "original protocol/ownership phase did not complete"
    );
    Ok(())
}
fn validate_public_terminal(error: &TrinoError) -> Result<()> {
    ensure!(
        error.error_code == 3
            && error.error_name == "USER_CANCELED"
            && error.error_type == "USER_ERROR",
        "original held Trino query did not return its exact cancellation error"
    );
    Ok(())
}
fn validate_proxy_join(
    proxy: &RootReplyFaultObservation,
    identity: Option<&RootReplyOriginalIdentity>,
    delete_started: Option<Instant>,
    replay_joined: Option<Instant>,
) -> Result<()> {
    let held = proxy
        .original_hold
        .as_ref()
        .context("original held response observation absent at Join")?;
    let deleted = delete_started.context("original DELETE start was never observed")?;
    let replay = replay_joined.context("real encoded replay driver was never actually joined")?;
    ensure!(
        held.held_at <= deleted
            && held
                .expected_query_cancellation_at
                .is_some_and(|prepared| prepared <= deleted)
            && held
                .release_requested_at
                .is_some_and(|released| released >= replay)
            && held.settled_at.is_some_and(|settled| settled >= deleted),
        "original response release/settle did not preserve DELETE and real replay Join order"
    );
    if held.disposition == RootReplyOriginalDisposition::JoinedOriginalConnectionExit {
        let exit = held
            .connection_exit
            .as_ref()
            .context("original connection exit absent")?;
        ensure!(
            exit.permitted_peer_exit
                && exit.joined_at >= deleted
                && exit.all_children_joined
                && !exit.cleanup_failed
                && exit.remaining_connection_bytes == 0
                && !exit.owner_stop_observed
                && !exit.generation_change_observed
                && held.downstream_reset.is_none(),
            "original connection exit is not the exact declared peer exit/Join"
        );
    }
    if held.disposition == RootReplyOriginalDisposition::ExactDownstreamCancelled {
        ensure!(
            held.downstream_reset
                .as_ref()
                .is_some_and(|reset| reset.observed_at >= deleted),
            "original exact stream cancellation preceded DELETE"
        );
    }
    ensure!(
        identity == Some(&held.identity)
            && held.settled_at.is_some()
            && proxy.shutdown_joined
            && proxy.active_listeners == 0
            && proxy.joined_listeners == 3
            && proxy.active_connections == 0
            && proxy.active_streams == 0
            && proxy.connection_positions == 0
            && proxy.stream_positions == 0
            && proxy.target_slots == 0
            && proxy.owned_buffer_bytes == 0
            && proxy.claimed == 1
            && proxy.emitted_messages
                == u64::from(held.disposition == RootReplyOriginalDisposition::ForwardedOriginal)
            && proxy.failures.is_empty()
            && !proxy.failure_overflow
            && proxy.first_failure.is_none(),
        "original FE proxy/response/byte owners did not actually join"
    );
    Ok(())
}

#[cfg(test)]
#[path = "result_delivery_held_late_ack_tests.rs"]
mod tests;

// Read only known owned type/context fields. No arbitrary Error Display/Debug
// is called while retaining evidence, including on a failing native run.
fn error_fact(error: &anyhow::Error) -> Value {
    if let Some(text) = error.downcast_ref::<String>() {
        return json!({"class":"OwnedStringContext","context":text.chars().take(1024).collect::<String>(),"truncated":text.chars().count()>1024});
    }
    if let Some(text) = error.downcast_ref::<&'static str>() {
        return json!({"class":"StaticContext","context":text});
    }
    if let Some(json) = error.downcast_ref::<serde_json::Error>() {
        return json!({"class":"Json","category":format!("{:?}",json.classify()),"line":json.line(),"column":json.column()});
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        return json!({"class":"Io","kind":format!("{:?}",io.kind()),"raw_os_error":io.raw_os_error()});
    }
    if let Some(http) = error.downcast_ref::<reqwest::Error>() {
        return json!({"class":"HttpTransport","status":http.status().map(|status|status.as_u16()),"timeout":http.is_timeout(),"connect":http.is_connect(),"body":http.is_body()});
    }
    if let Some(held) =
        error.downcast_ref::<super::result_delivery_root_protocol::held_response::HeldFailure>()
    {
        return json!({"class":"OriginalHeldFailure","phase":held.class,"observation":held.observation});
    }
    json!({"class":"OtherActualSourceRetained","raw_formatter_invoked":false})
}
