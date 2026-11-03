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

//! Original S+8 query, one held replay and two sealed ACK-only reads.
//! This is a child of exact_mysql_native_driver only to reuse its bounded observations.
//! It neither creates nor weakens an exact MySQL gate/control owner.
use super::{
    AdmittedExactNativeRun, Census, HEALTH_ROW_SHA, HEALTH_SQL, OriginalPreparedConfig,
    RECEIPT_BYTES, S, await_idle, census, check, retain_secondary, sleep_until_sample,
};
use crate::actors::mysql_stream::{
    AsyncMysqlStream, OwnedTextResultObservation, TextResultObservation,
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use crate::scenarios::independent_root_target::{
    FrontendIdentitySource, IndependentRootObserver, IndependentRootTarget,
};
use crate::scenarios::result_delivery_root_protocol::{
    held_response::{HeldObservation, HeldRootResponse, JoinedOriginalConnection, ProvenReplayOne},
    probe_owned,
};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::process_resources::ProcessLaunchIdentity;
use novarocks_cluster_harness::{LaunchProfile, TestClusterOwner};
use novarocks_execution_contract::{TaskIdentity, root_result::RootResultRead};
use novarocks_proto_codec::FieldPath;
use novarocks_result_contract::{RootOutputKind, RootProfileId};
use novarocks_task_codec::root_result::{decode_read, encode_read};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    num::NonZeroU64,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{runtime::Runtime, sync::oneshot, task::JoinHandle};

const NAME: &str = "result-delivery/held-response-late-ack";
const SQL: &str = "SELECT REPEAT('x', 1048576) AS payload FROM generate_series(1, 1)";
const ROW_SHA: &str = "e5a05e54f4636fe6e87eb8094fceda8e002bff799f3de76413a2a77c52fd50b8";
const SAMPLES: usize = 51;
const WHOLE: Duration = Duration::from_secs(20);
const PROTOCOL: Duration = Duration::from_secs(5);
const TARGET_RUN_TOKEN: &str = "m07-held-late-ack-target";
const HEALTH_RUN_TOKEN: &str = "m07-held-late-ack-health";
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

/// Called after prelaunch admission verifies the immutable input, actual
/// neutral-marker feature and original build pins. Shape checking below is local.
pub(super) fn from_admitted(
    run: AdmittedExactNativeRun,
    live_fences: bool,
) -> Result<Box<dyn Scenario>> {
    run.validate_shape()?;
    Ok(Box::new(HeldLateAck {
        run: Arc::new(run),
        clock: Mutex::new(None),
        prepared: Mutex::new(None),
        source_fences: Mutex::new(if live_fences {
            Some(crate::held_live_source_fence::SourceFenceOwner::from_original_stdin()?)
        } else {
            None
        }),
    }))
}
struct HeldLateAck {
    run: Arc<AdmittedExactNativeRun>,
    clock: Mutex<Option<Instant>>,
    prepared: Mutex<Option<OriginalPreparedConfig>>,
    source_fences: Mutex<Option<crate::held_live_source_fence::SourceFenceOwner>>,
}
impl HeldLateAck {
    fn clock(&self) -> Result<Instant> {
        self.clock
            .lock()
            .map_err(|_| anyhow::anyhow!("original root clock poisoned"))?
            .context("original root clock was not captured before role launch")
    }
}
impl Scenario for HeldLateAck {
    fn name(&self) -> &'static str {
        NAME
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        ensure!(
            profile == LaunchProfile::FaultScenario,
            "held response requires the fault-scenario launch profile"
        );
        self.run.validate_shape()
    }
    fn launch_config(&self, _: &Path) -> Result<ScenarioLaunchConfig> {
        let mut clock = self
            .clock
            .lock()
            .map_err(|_| anyhow::anyhow!("original clock poisoned"))?;
        ensure!(clock.is_none(), "original prelaunch clock already captured");
        *clock = Some(Instant::now() + WHOLE);
        // Neutral source feature is build-admitted, not enabled by an environment knob.
        // No exact Hub, socket, nonce or controller is created for this unarmed scene.
        Ok(ScenarioLaunchConfig::default())
    }
    fn test_session_plan(
        &self,
        launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        self.run.validate_shape()?;
        ensure!(
            original_deadline <= self.clock()?,
            "held declaration renewed original clock"
        );
        ensure!(
            launch.child_environment == Default::default(),
            "held root observation cannot adopt exact fixture or ambient control inputs"
        );
        Ok(Some(super::variant_sessions::fixture_plan(
            binary,
            cluster_size,
            original_deadline,
            vec![super::variant_sessions::grant(
                "root-observation",
                "original-neutral-root",
                json!({"action":"observe"}),
            )],
            "mem-1-m07-root-observation",
            &["novarocks-server", "novarocks-frontend-application"],
            &[TARGET_RUN_TOKEN, HEALTH_RUN_TOKEN],
        )?))
    }
    fn root_observation_deadline(&self) -> Result<Option<Instant>> {
        Ok(Some(self.clock()?))
    }
    fn freeze_prepared_exact_config(
        &self,
        artifact: &novarocks_cluster_harness::EffectiveLaunchConfigEvidence,
        root: &Path,
        deadline: Instant,
    ) -> Result<()> {
        ensure!(
            deadline == self.clock()?,
            "prepared config renewed original clock"
        );
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| anyhow::anyhow!("prepared owner poisoned"))?;
        ensure!(prepared.is_none(), "prepared config already frozen");
        *prepared = Some(OriginalPreparedConfig::freeze(artifact, root, deadline)?);
        let mut fences = self
            .source_fences
            .lock()
            .map_err(|_| anyhow::anyhow!("original source fence owner poisoned"))?;
        if let Some(fences) = fences.as_mut() {
            fences.prepared(deadline)?;
        }
        Ok(())
    }
    fn observe_original_durable_log_owner(
        &self,
        role: &str,
        device: u64,
        inode: u64,
    ) -> Result<()> {
        let mut fences = self
            .source_fences
            .lock()
            .map_err(|_| anyhow::anyhow!("original source fence owner poisoned"))?;
        fences
            .as_mut()
            .context("original source fence owner is not enabled")?
            .log_owner(role, device, inode, self.clock()?)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let deadline = self.clock()?;
        check(deadline)?;
        let prepared = self
            .prepared
            .lock()
            .map_err(|_| anyhow::anyhow!("prepared owner poisoned"))?;
        prepared
            .as_ref()
            .context("prepared callback did not run before FE spawn")?
            .verify(context.effective_launch_config_evidence())?;
        // A running worker drives the original MySQL job while synchronous strict
        // probe uses its existing private runtime; no nested Runtime::block_on.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        context.retain_artifacts();
        run_owned(context, &runtime, deadline, &self.run, &self.source_fences)
    }
    fn teardown(&self) -> Result<()> {
        let prepared = self
            .prepared
            .lock()
            .map_err(|_| anyhow::anyhow!("prepared owner poisoned"))?;
        if let Some(prepared) = &*prepared {
            prepared.verify_original_file()?;
        }
        // This occurs after original runner cleanup; a late exit cannot become PASS.
        check(self.clock()?)
    }
}

#[derive(Default)]
struct Facts {
    samples: Vec<Value>, // admission on every push: at most the original 51 positions
    actor: Option<HeldObservation>,
    root: Option<Value>,
    target_source: Option<Value>,
    closed_acks: Vec<Value>, // checked before each push: at most two
    original_prelaunch: Option<Instant>,
    mysql_terminal_code: Option<u16>,
    health_before_tasks_created: Option<[u64; 3]>,
    health_after_tasks_created: Option<[u64; 3]>,
    protocol_complete: bool,
    actor_actual_join: bool,
    mysql_actual_join: bool,
    kill_attempted: bool,
    kill_returned: bool,
    protocol_started_from_original_prelaunch_us: Option<u128>,
    kill_started_from_original_prelaunch_us: Option<u128>,
    kill_returned_from_original_prelaunch_us: Option<u128>,
    settled_from_original_prelaunch_us: Option<u128>,
    mysql: Option<TextResultObservation>,
    health: Option<TextResultObservation>,
    expected_server_err_retained: bool,
    expected_connection_cancel_retained: bool,
}
struct OwnedFailure {
    errors: [Option<anyhow::Error>; 8],
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
type MysqlJob = JoinHandle<(AsyncMysqlStream, OwnedTextResultObservation)>;

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
fn validate_mysql(value: &OwnedTextResultObservation, health: bool) -> Result<()> {
    let row = &value.observation;
    ensure!(
        row.columns == 1 && row.schema.len() == 1 && row.rows == 1 && row.packets == 5,
        "original MySQL row/metadata/packet count differs"
    );
    if health {
        ensure!(
            value.actual_failure.is_none()
                && value.server_result_error_code.is_none()
                && row.error.is_none()
                && row.schema[0].name == "total"
                && row.schema[0].mysql_type == 8
                && row.row_payload_bytes == 5
                && row.row_sha256 == HEALTH_ROW_SHA,
            "same original socket health oracle differs"
        );
    } else {
        ensure!(
            value.server_result_error_code == Some(1317)
                && value.actual_failure.is_some()
                && row.error.is_some()
                && row.schema[0].name == "payload"
                && row.schema[0].mysql_type == 253
                && row.row_payload_bytes == S + 4
                && row.row_sha256 == ROW_SHA,
            "original complete row plus interrupted terminal differs"
        );
    }
    Ok(())
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
        "original held role inventory differs from the admitted topology"
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
    admitted: &AdmittedExactNativeRun,
    source_fences: &Mutex<Option<crate::held_live_source_fence::SourceFenceOwner>>,
) -> Result<()> {
    let original_roles = original_role_inventory(context.process_launch_identities())?;
    // Every original handle stays outside the primary operation and all fallible writes.
    let mut actor: Option<HeldRootResponse> = None;
    let mut job: Option<MysqlJob> = None;
    let mut resume: Option<oneshot::Sender<()>> = None;
    let mut stream: Option<AsyncMysqlStream> = None;
    let mut cid: Option<u32> = None;
    let mut protocol_deadline = None;
    let mut facts = Facts {
        original_prelaunch: Some(deadline - WHOLE),
        ..Facts::default()
    };
    let mut errors: [Option<anyhow::Error>; 8] = std::array::from_fn(|_| None);
    let mut expected_server_error: Option<anyhow::Error> = None;
    let mut expected_connection_exit: Option<tokio::task::JoinError> = None;
    let primary = (|| -> Result<()> {
        runtime.block_on(await_idle(context, deadline))?;
        // Only this selected neutral source supplies expected FE; no RPC/CID/PID fallback.
        let frontend = context
            .handle()
            .original_root_observation_frontend_identity(deadline)?;
        let observer = IndependentRootObserver::capture_baseline_until(
            context,
            frontend,
            deadline,
            FrontendIdentitySource::RootObservation,
        )?;
        {
            let mut fences = source_fences
                .lock()
                .map_err(|_| anyhow::anyhow!("original source fence owner poisoned"))?;
            if let Some(fences) = fences.as_mut() {
                fences.baseline(observer.original_baseline_log_bytes(), deadline)?;
            }
        }
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        check(deadline)?;
        let client = runtime.block_on(async {
            tokio::time::timeout_at(deadline.into(), async {
                AsyncMysqlStream::connect_with_receive_buffer(
                    &user,
                    port,
                    deadline.saturating_duration_since(Instant::now()),
                    67_108_864,
                    4096,
                )
                .await
            })
            .await
        })??;
        check(deadline)?;
        ensure!(
            client
                .receive_buffer_bytes()
                .is_some_and(|bytes| bytes > 0 && bytes <= 65536),
            "actual original socket receive bound unavailable"
        );
        cid = Some(client.connection_id()?);
        super::variant_sessions::arm_statement_until(
            context,
            TARGET_RUN_TOKEN,
            cid.unwrap(),
            SQL,
            deadline,
        )?;
        let (ready_tx, ready_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        resume = Some(resume_tx);
        let mut client = client;
        job = Some(runtime.spawn(async move {
            let result = client
                .observe_text_query_owned_until(
                    SQL,
                    Duration::ZERO,
                    Some((ready_tx, resume_rx)),
                    deadline,
                )
                .await;
            (client, result)
        }));
        runtime.block_on(async {
            tokio::time::timeout_at(deadline.min(Instant::now() + PROTOCOL).into(), ready_rx)
                .await??;
            Ok::<(), anyhow::Error>(())
        })?;
        ensure!(
            !job.as_ref().unwrap().is_finished(),
            "original MySQL job left metadata pause"
        );
        let phase = deadline.min(Instant::now() + PROTOCOL);
        protocol_deadline = Some(phase); // exactly once, before discovery or any Root request
        facts.protocol_started_from_original_prelaunch_us = Some(
            Instant::now()
                .saturating_duration_since(deadline - WHOLE)
                .as_micros(),
        );
        let target = observer.observe_ready_until(context, phase)?;
        {
            let mut fences = source_fences
                .lock()
                .map_err(|_| anyhow::anyhow!("original source fence owner poisoned"))?;
            if let Some(fences) = fences.as_mut() {
                fences.target(target.after_log_bytes, phase)?;
            }
        }
        facts.root = Some(identity(target.root));
        facts.target_source = Some(observer.source_evidence(&target)?);
        // Same closed whole-cluster inventory and two genuine retained Data positions.
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
        drop(reply); // no decoded body alias is retained by this scene
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
        facts.kill_attempted = true; // preserve unknown outcome; never retry this KILL
        facts.kill_started_from_original_prelaunch_us = Some(
            Instant::now()
                .saturating_duration_since(deadline - WHOLE)
                .as_micros(),
        );
        context.handle().kill_query_until(cid.unwrap(), phase)?;
        check(phase)?;
        facts.kill_returned = true;
        facts.kill_returned_from_original_prelaunch_us = Some(
            Instant::now()
                .saturating_duration_since(deadline - WHOLE)
                .as_micros(),
        );
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
            facts
                .closed_acks
                .push(json!({"label":label,"root":identity(reply.root_task),
                "profile":reply.profile.get(),"kind":"ClientRows", // actual typed proof checked above
                "wanted":ack.wanted().map(|n| n.get()),"consumed":ack.consumed(),
                "accepted_consumed":reply.accepted_consumed,"outcome":"AwaitTerminalControl",
                "observed_from_original_prelaunch_us":Instant::now()
                    .saturating_duration_since(deadline-WHOLE).as_micros()}));
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

    // On failed discovery/probe, cancel the submitted original statement at most once.
    if job.is_some() && !facts.kill_attempted {
        facts.kill_attempted = true;
        if let Some(cid) = cid {
            let limit = protocol_deadline.unwrap_or(deadline); // existing whole clock, no renewed phase
            match context.handle().kill_query_until(cid, limit) {
                Ok(()) => facts.kill_returned = true,
                Err(error) => retain_secondary(&mut errors[1], error),
            }
        }
    }
    // Even an expired phase invokes actual settle on the retained same handle.
    // Actual join may be late; the actor keeps Clock failure sticky. No timeout-drop.
    if let Some(owner) = actor.as_mut() {
        match runtime.block_on(owner.settle()) {
            Ok(JoinedOriginalConnection {
                observation,
                actual_expected_cancellation,
            }) => {
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
    facts.settled_from_original_prelaunch_us = Some(
        Instant::now()
            .saturating_duration_since(deadline - WHOLE)
            .as_micros(),
    );
    if let Some(resume) = resume.take() {
        let _ = resume.send(());
    }
    // Borrow the original MySQL handle until it really returns. The observer owns
    // its timeout and returns retained partial facts; no outer timeout drops it.
    if let Some(handle) = job.as_mut() {
        match runtime.block_on(handle) {
            Ok((client, mut value)) => {
                facts.mysql_actual_join = true;
                facts.mysql_terminal_code = value.server_result_error_code;
                if let Err(error) = validate_mysql(&value, false) {
                    retain_secondary(&mut errors[3], error);
                }
                if value.server_result_error_code == Some(1317) {
                    expected_server_error = value.actual_failure.take();
                } else if let Some(error) = value.actual_failure.take() {
                    retain_secondary(&mut errors[3], error);
                }
                facts.mysql = Some(value.observation);
                stream = Some(client);
            }
            Err(error) => retain_secondary(&mut errors[3], error.into()),
        }
        job.take(); // original handle already actually joined, not discarded pending
    }
    facts.expected_server_err_retained = expected_server_error.is_some();
    facts.expected_connection_cancel_retained = expected_connection_exit.is_some();
    if errors.iter().all(Option::is_none) {
        let recovery = (|| -> Result<()> {
            let before = runtime.block_on(census(context, deadline))?.created;
            facts.health_before_tasks_created = Some(before);
            let client = stream
                .as_mut()
                .context("original client disappeared before health")?;
            super::variant_sessions::arm_statement_until(
                context,
                HEALTH_RUN_TOKEN,
                client.connection_id()?,
                HEALTH_SQL,
                deadline,
            )?;
            let mut value = runtime.block_on(client.observe_text_query_owned_until(
                HEALTH_SQL,
                Duration::ZERO,
                None,
                deadline,
            ));
            let validation = validate_mysql(&value, true);
            facts.health = Some(value.observation);
            if let Some(error) = value.actual_failure.take() {
                return Err(error);
            }
            validation?;
            let after = runtime.block_on(census(context, deadline))?.created;
            facts.health_after_tasks_created = Some(after);
            ensure!(
                after
                    .iter()
                    .zip(before)
                    .any(|(after, before)| *after > before)
                    && after
                        .iter()
                        .zip(before)
                        .all(|(after, before)| *after >= before),
                "same socket health produced no real native tasks"
            );
            check(deadline)
        })();
        errors[4] = recovery.err();
    }
    drop(stream.take()); // original MySQL job is already joined; close this original TCP owner
    if let Err(error) = runtime.block_on(await_idle(context, deadline)) {
        retain_secondary(&mut errors[5], error);
    }
    // Save partial wire and actor facts after cleanup even when the primary failed.
    // Saving invokes no arbitrary error formatter and cannot replace original sources.
    let receipt = json!({"schema_version":1,"status":if errors.iter().all(Option::is_none) {"COMPONENT_AND_SCENE_ASSERTIONS_PASSED_PENDING_ROLE_EXIT"} else {"FAILED"},
        "native_acceptance":false,"source_pins":{
            "clean_revision":admitted.clean_revision,"source_tree_sha256":admitted.source_tree_sha256,
            "server_binary_sha256":admitted.server_binary_sha256,"runner_binary_sha256":admitted.runner_binary_sha256,
            "base_config_sha256":admitted.base_config_sha256,"execution_binding_sha256":admitted.frozen_execution_binding_sha256},
        "original_roles":original_roles,"selected_root":facts.root,
        "independent_target_source":facts.target_source,"closed_acks":facts.closed_acks,
        "mysql_terminal_code":facts.mysql_terminal_code,
        "health_before_tasks_created":facts.health_before_tasks_created,
        "health_after_tasks_created":facts.health_after_tasks_created,
        "protocol_complete":facts.protocol_complete,"actor":facts.actor,
        "actor_actual_join":facts.actor_actual_join,"mysql_actual_join":facts.mysql_actual_join,
        "kill_attempted":facts.kill_attempted,"kill_returned":facts.kill_returned,
        "timings_origin":"the original launch_config prelaunch instant, not scene entry",
        "protocol_started_us":facts.protocol_started_from_original_prelaunch_us,
        "kill_started_us":facts.kill_started_from_original_prelaunch_us,
        "kill_returned_us":facts.kill_returned_from_original_prelaunch_us,
        "settled_us":facts.settled_from_original_prelaunch_us,
        "expected_server_error_actual_source_retained":facts.expected_server_err_retained,
        "expected_connection_joinerror_retained":facts.expected_connection_cancel_retained,
        "root_samples":facts.samples,"original_wire":facts.mysql,"same_socket_health":facts.health,
        "failed_source_slots":errors.each_ref().map(Option::is_some),"raw_error_formatter_invoked":false,
        "scope":"Context-owned unique root positive physical delivery/reservation holders only; excludes released roots, allocator dealloc, last Arc alias and full Closing pool"});
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
    // Expected terminal sources remain alive through receipt settlement. They are
    // separate from unknown failures; successful actual joins are already explicit.
    drop(expected_connection_exit);
    drop(expected_server_error);
    if errors.iter().any(Option::is_some) {
        return Err(OwnedFailure { errors }.into());
    }
    ensure!(
        facts.protocol_complete && facts.actor_actual_join && facts.mysql_actual_join,
        "original ownership/protocol phase did not complete"
    );
    Ok(())
}

#[cfg(test)]
#[path = "result_delivery_held_late_ack_tests.rs"]
mod tests;
