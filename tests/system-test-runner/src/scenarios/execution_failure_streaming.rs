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

//! The same exact Task failure before schema and after one visible row.

use super::native_sql_controls::{SqlIntentOwner, StatementSpec};
use super::query_lifecycle;
use crate::{
    actors::{
        mysql as mysql_actor,
        mysql_stream::{
            AsyncMysqlStream, BoundedErrorPrefix, BoundedNegativeTextObservation,
            OwnedTextResultObservation,
        },
    },
    scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig},
};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{
    LaunchProfile, QueryLifecycleFailureSite, QueryLifecycleStatementObservation,
    QueryLifecycleStructuredSnapshot, TestClusterOwner, test_sessions::ClusterTestSessionPlan,
};
use novarocks_execution_contract::{
    TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact, FailureReason, domain},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    num::NonZeroU16,
    path::Path,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const ACTION: &str = "task-execution-failure-after-visible-prefix";
const BEFORE_SCHEMA_ACTION: &str = "root-task-execution-failure-before-data";
const PROPOSED: &str = novarocks_failpoint::observation_marker::TASK_EXECUTION_FAILURE_INJECTED;
const ACCEPTED: &str =
    novarocks_failpoint::observation_marker::TASK_EXECUTION_FAILURE_AFTER_VISIBLE_PREFIX_ACCEPTED;
const CREATED: &str = novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED;
const DETAIL: &str = "Unexpected failure (reason 13:20)";
// The first small row is flushed while the first long row is still being
// scanned in the renderer's Small/Count phase. Each repeated value respects
// the inclusive 1MiB SQL string cap; the four-row tail totals 4MiB. Even when
// wanted=2 carries consumed=1, this exceeds the two 1MiB Root Data positions,
// keeping the real producer live behind that read without a timing workload.
const TARGET: &str = "SELECT payload FROM (SELECT 0 AS ordinal, CAST('1' AS VARCHAR) AS payload UNION ALL SELECT 1 AS ordinal, REPEAT('x', 1048576) AS payload UNION ALL SELECT 2 AS ordinal, REPEAT('x', 1048576) AS payload UNION ALL SELECT 3 AS ordinal, REPEAT('x', 1048576) AS payload UNION ALL SELECT 4 AS ordinal, REPEAT('x', 1048576) AS payload) ordered_rows ORDER BY ordinal ASC";
const HEALTHY: &str =
    "SELECT CAST(SUM(generate_series) AS VARCHAR) AS payload FROM generate_series(1, 100)";

pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(ExecutionFailureScene::default()),
        Box::new(ExecutionFailureScene {
            intent: SqlIntentOwner::default(),
            stage: Stage::BeforeSchema,
        }),
    ]
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Stage {
    #[default]
    AfterVisibleRow,
    BeforeSchema,
}
impl Stage {
    fn name(self) -> &'static str {
        match self {
            Self::AfterVisibleRow => "execution-failure/after-visible-row",
            Self::BeforeSchema => "execution-failure/before-schema",
        }
    }
    fn action(self) -> &'static str {
        match self {
            Self::AfterVisibleRow => ACTION,
            Self::BeforeSchema => BEFORE_SCHEMA_ACTION,
        }
    }
    fn sql(self) -> &'static str {
        match self {
            Self::AfterVisibleRow => TARGET,
            Self::BeforeSchema => HEALTHY,
        }
    }
    fn prefix(self) -> BoundedErrorPrefix<'static> {
        match self {
            Self::AfterVisibleRow => BoundedErrorPrefix::AfterMetadata { rows: &[Some("1")] },
            Self::BeforeSchema => BoundedErrorPrefix::BeforeSchema,
        }
    }
    fn evidence_file(self) -> &'static str {
        match self {
            Self::AfterVisibleRow => "after-visible-row.json",
            Self::BeforeSchema => "before-schema.json",
        }
    }
}

#[derive(Default)]
struct ExecutionFailureScene {
    intent: SqlIntentOwner,
    stage: Stage,
}
impl Scenario for ExecutionFailureScene {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        super::declarations::for_module(
            "execution_failure_streaming",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }
    fn name(&self) -> &'static str {
        self.stage.name()
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        ensure!(
            profile == LaunchProfile::FaultScenario,
            "execution failure requires the finite FaultScenario profile"
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
    ) -> Result<Option<ClusterTestSessionPlan>> {
        let specs = vec![
            StatementSpec {
                sql: self.stage.sql().into(),
                faults: (0..3).map(|index| (index, self.stage.action())).collect(),
                observations: (0..cluster_size)
                    .flat_map(|index| {
                        [1, 2].map(move |attempt| (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), Some(attempt)))
                    })
                    .collect(),
            },
            StatementSpec {
                sql: HEALTHY.into(),
                faults: vec![],
                observations: vec![],
            },
            StatementSpec {
                sql: HEALTHY.into(),
                faults: vec![],
                observations: vec![],
            },
        ];
        match self.stage {
            Stage::BeforeSchema => {
                self.intent
                    .freeze_before_schema_failure(specs, &[], cluster_size, binary, deadline)
            }
            Stage::AfterVisibleRow => {
                self.intent
                    .freeze(specs, &[], cluster_size, binary, deadline)
            }
        }
        .map(Some)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "scenario requires real native 1FE+3BE"
        );
        let launches = context.recheck_live_process_launch_identities()?;
        let baseline = query_lifecycle::resource_snapshot(context)?;
        let deadline = context.deadline();
        let stage = self.stage;
        let target = stage.sql();
        let action = stage.action();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .context("create original raw client runtime")?;
        let mut stream = runtime.block_on(AsyncMysqlStream::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect raw client")?,
        ))?;
        let connection = stream.connection_id()?;
        let selected = self.intent.take(0)?;
        let mut observations = Vec::new();
        let prefix_rows: &[&str] = match stage {
            Stage::AfterVisibleRow => &["1"],
            Stage::BeforeSchema => &[],
        };
        let mut evidence = json!({"scenario":self.name(), "prefix_rows":prefix_rows,
            "fault_enabled_after_prefix":false, "actual_client_joined":false});
        let mut next_command = None;
        let mut client = None;
        let primary = (|| -> Result<()> {
            // The same exact statement arm registers the original cleanup.
            // Only AfterVisibleRow is dormant until the verified client prefix.
            observations.push(selected.arm(context, connection, target)?);
            let (ready_tx, ready_rx) = oneshot::channel();
            let (negative_tx, negative_rx) = oneshot::channel();
            let (next_tx, next_rx) = oneshot::channel::<bool>();
            next_command = Some(next_tx);
            client = Some(runtime.spawn(async move {
                let negative = stream
                    .observe_bounded_execution_error(target, stage.prefix(), ready_tx, deadline)
                    .await;
                // A failed controller still drops this owned socket in this task.
                let _ = negative_tx.send(negative);
                let run_next =
                    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), next_rx)
                        .await
                        .context("same-socket command original deadline")?
                        .context("same-socket command owner disappeared")?;
                let result = if run_next {
                    Some(
                        stream
                            .observe_text_query_owned_until(HEALTHY, Duration::ZERO, None, deadline)
                            .await,
                    )
                } else {
                    None
                };
                drop(stream);
                Ok::<_, anyhow::Error>(result)
            }));
            runtime.block_on(receive_until(ready_rx, deadline, "selected raw prefix"))?;
            match stage {
                Stage::AfterVisibleRow => {
                    context.action("raw client parsed one exact complete payload/VARCHAR row before fault activation");
                    selected.activate_deferred_faults(context)?;
                    evidence["fault_enabled_after_prefix"] = json!(true);
                }
                Stage::BeforeSchema => {
                    // BeforeSchema ready only acknowledges COM_QUERY sent. The
                    // first complete ERR below independently proves the stage.
                    context.action("raw client sent exact before-schema failure statement");
                }
            }
            let negative = runtime.block_on(receive_until(
                negative_rx,
                deadline,
                "complete execution ERR",
            ))?;
            evidence["negative"] = serde_json::to_value(&negative)?;
            assert_negative(stage, &negative)?;
            let snapshot = await_converged(context, &observations[0], false)?;
            evidence["selected_failure"] = snapshot_evidence(&snapshot);
            let task = assert_failed_snapshot(&snapshot)?;
            if stage == Stage::BeforeSchema {
                assert_handoff_terminal_marker(context, &selected.token, connection)?;
                evidence["original_failure_observed_before_protocol_handoff"] = json!(true);
            }
            assert_raw_canonical(&negative, &snapshot)?;
            evidence["selected_failure"] = snapshot_evidence(&snapshot);
            let marker = match stage {
                Stage::AfterVisibleRow => ACCEPTED,
                Stage::BeforeSchema => PROPOSED,
            };
            let fault_backend = query_lifecycle::await_token_scoped_marker(
                context,
                marker,
                std::slice::from_ref(&selected.token),
            )?;
            match stage {
                Stage::AfterVisibleRow => {
                    assert_accepted_marker(context, fault_backend, task, &selected.token)?
                }
                Stage::BeforeSchema => {
                    assert_proposal_marker(context, fault_backend, task, &selected.token)?
                }
            }
            let hits = (0..3)
                .map(|index| {
                    context
                        .handle()
                        .selected_query_lifecycle_fault_hit(index, action, &selected.token)
                        .map(|hit| (index, hit))
                })
                .collect::<Result<Vec<_>>>()?;
            ensure!(
                hits.iter().filter(|(_, hit)| *hit).count() == 1 && hits[fault_backend].1,
                "expected one exact root Task action hit, got {hits:?}"
            );
            assert_no_replacement(context, &snapshot)?;
            evidence[match stage {
                Stage::AfterVisibleRow => "accepted_backend",
                Stage::BeforeSchema => "proposal_backend",
            }] = json!(fault_backend);
            // Clear the same registered actions before checking the next statement.
            context.handle().clear_query_lifecycle_faults()?;
            // A connection has one exact observer. Retire its completed
            // original binding before arming the same socket's next statement.
            context
                .handle()
                .drain_query_lifecycle_statement_binding(&observations[0], deadline)?;
            observations.remove(0);
            let next = self.intent.take(1)?;
            let next_observation = next.arm(context, connection, HEALTHY)?;
            observations.push(next_observation.clone());
            next_command
                .take()
                .context("missing same-socket command")?
                .send(true)
                .map_err(|_| anyhow::anyhow!("original raw client exited before next statement"))?;
            let healthy = runtime
                .block_on(client.take().context("missing original client task")?)
                .context("join original raw client")??
                .context("same-socket statement was not run")?;
            evidence["actual_client_joined"] = json!(true);
            assert_healthy_wire(&healthy)?;
            evidence["next_statement"] = serde_json::to_value(&healthy.observation)?;
            let next_snapshot = await_converged(context, &next_observation, true)?;
            assert_independent_success(&snapshot, &next_snapshot)?;
            // A separate real socket and statement binding rule out connection-local recovery.
            let independent =
                mysql_actor::connect_until(context.mysql_user(), context.mysql_port(), deadline)?;
            let other = self.intent.take(2)?;
            let other_observation = other.arm(context, independent.connection_id(), HEALTHY)?;
            observations.push(other_observation.clone());
            let value: Option<String> =
                independent.run(|connection| Ok(connection.query_first(HEALTHY)?))?;
            ensure!(
                value.as_deref() == Some("5050"),
                "independent query returned {value:?}"
            );
            let other_snapshot = await_converged(context, &other_observation, true)?;
            assert_independent_success(&snapshot, &other_snapshot)?;
            ensure!(
                other_snapshot.execution_id != next_snapshot.execution_id,
                "independent and next statements share an execution identity"
            );
            evidence["next_snapshot"] = snapshot_evidence(&next_snapshot);
            evidence["independent_snapshot"] = snapshot_evidence(&other_snapshot);
            assert_no_replacement(context, &snapshot)?;
            ensure!(
                launches == context.recheck_live_process_launch_identities()?,
                "scenario changed native process launch identities"
            );
            Ok(())
        })();
        // Always release the finite command gate and actually join the client.
        // The original absolute deadline bounds its query and command receive.
        let mut cleanup = Vec::new();
        if let Err(error) = context.handle().clear_query_lifecycle_faults() {
            cleanup.push(format!("clear original fault actions: {error:#}"));
        }
        if let Some(command) = next_command.take() {
            let _ = command.send(false);
        }
        if let Some(job) = client.take() {
            match runtime.block_on(job) {
                Ok(Ok(_)) => evidence["actual_client_joined"] = json!(true),
                Ok(Err(error)) => cleanup.push(format!("original client exit: {error:#}")),
                Err(error) => cleanup.push(format!("original client join: {error}")),
            }
        }
        if let Err(error) = query_lifecycle::await_resource_convergence(context, &baseline) {
            cleanup.push(format!("original resource convergence: {error:#}"));
        }
        for observation in &observations {
            if let Err(error) = context
                .handle()
                .drain_query_lifecycle_statement_observation(observation)
            {
                cleanup.push(format!("drain original statement observation: {error:#}"));
            }
        }
        evidence["primary_succeeded"] = json!(primary.is_ok());
        evidence["cleanup"] = json!({"error_count":cleanup.len(), "complete":cleanup.is_empty()});
        // This retained evidence contains only exact public protocol/fact fields.
        if let Err(error) = std::fs::write(
            context.scenario_root().join(stage.evidence_file()),
            serde_json::to_vec_pretty(&evidence)?,
        ) {
            cleanup.push(format!("save scene evidence: {error}"));
        }
        match (primary, cleanup.is_empty()) {
            (Ok(()), true) => Ok(()),
            (Err(error), true) => Err(error),
            (Ok(()), false) => anyhow::bail!("scenario cleanup failed: {}", cleanup.join("; ")),
            (Err(error), false) => anyhow::bail!("{error:#}; cleanup: {}", cleanup.join("; ")),
        }
    }
}

async fn receive_until<T>(
    receiver: oneshot::Receiver<T>,
    deadline: Instant,
    phase: &str,
) -> Result<T> {
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), receiver)
        .await
        .with_context(|| format!("{phase}: original deadline"))?
        .with_context(|| format!("{phase}: original sender exited"))
}

fn assert_negative(stage: Stage, observed: &BoundedNegativeTextObservation) -> Result<()> {
    ensure!(
        observed.text.error.is_none() && observed.pending_packet.is_none(),
        "bounded raw observer did not finish its exact prefix and ERR: {observed:?}"
    );
    match stage {
        Stage::AfterVisibleRow => {
            ensure!(
                observed.completed_metadata,
                "visible-row metadata was not completed"
            );
            ensure!(
                observed.text.rows == 1
                    && observed.text.columns == 1
                    && observed.text.schema.len() == 1
                    && observed.text.schema[0].name == "payload",
                "bounded raw prefix differs from the exact one-row contract"
            );
            ensure!(
                observed.text.row_sha256 == format!("{:x}", Sha256::digest([1, b'1'])),
                "first visible row payload is not the independent text encoding of 1"
            );
        }
        Stage::BeforeSchema => {
            ensure!(
                !observed.completed_metadata
                    && observed.metadata_payload_hex.is_empty()
                    && observed.text.columns == 0
                    && observed.text.schema.is_empty()
                    && observed.text.rows == 0
                    && observed.text.row_payload_bytes == 0
                    && observed.text.first_row_micros.is_none()
                    && observed.text.packets == 1,
                "before-schema ERR was preceded by metadata or row data: {observed:?}"
            );
            ensure!(
                observed
                    .terminal
                    .as_ref()
                    .is_some_and(|terminal| terminal.sequence == 1),
                "before-schema ERR must be the first response packet"
            );
        }
    }
    let terminal = observed
        .terminal
        .as_ref()
        .context("missing complete ERR packet")?;
    ensure!(
        terminal.code == 1105 && terminal.sqlstate == "HY000",
        "execution ERR has errno={} SQLSTATE={}",
        terminal.code,
        terminal.sqlstate
    );
    Ok(())
}

// This marker proves only that the finite FE handoff action observed its
// original failure watch. The Task snapshot remains the acceptance authority.
fn assert_handoff_terminal_marker(
    context: &mut ScenarioContext,
    token: &str,
    connection: u32,
) -> Result<()> {
    const MARKER: &str = novarocks_failpoint::observation_marker::RESULT_HANDOFF_TERMINAL_OBSERVED;
    loop {
        let log = context.handle().fe_log_contents()?;
        let matches = log
            .lines()
            .filter(|line| {
                line.starts_with(MARKER)
                    && line
                        .split_whitespace()
                        .any(|field| field == format!("token={token}"))
            })
            .collect::<Vec<_>>();
        if !matches.is_empty() {
            ensure!(
                matches.len() == 1,
                "expected one exact FE terminal handoff marker, got {}",
                matches.len()
            );
            let statement = matches[0]
                .split_whitespace()
                .find_map(|field| field.strip_prefix("statement="))
                .context("FE handoff marker has no original statement")?;
            let parts = statement
                .split(':')
                .map(str::parse::<u64>)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            ensure!(
                parts.len() == 3
                    && parts[0] == u64::from(connection)
                    && parts[1] > 0
                    && parts[2] > 0,
                "FE handoff marker names another statement: {statement}"
            );
            return Ok(());
        }
        std::thread::sleep(
            context
                .remaining("observe exact FE handoff action")?
                .min(Duration::from_millis(20)),
        );
    }
}

fn expected_fact() -> FailureFact {
    FailureFact::new(
        FailureClass::Unexpected,
        FailureReason::new(domain::NATIVE, NonZeroU16::new(20).unwrap()),
    )
}

fn assert_failed_snapshot(snapshot: &QueryLifecycleStructuredSnapshot) -> Result<TaskIdentity> {
    ensure!(
        snapshot.attempt_id == 1 && snapshot.worker_participations_converged,
        "failure must retain attempt 1 and positive original Worker convergence"
    );
    ensure!(
        snapshot.failure_fact == Some(expected_fact()),
        "canonical failure differs from exact originating NATIVE20: {:?}",
        snapshot.failure_fact
    );
    ensure!(
        snapshot.failure_detail.as_deref() == Some(DETAIL),
        "canonical safe detail differs from the original Native producer"
    );
    let Some(QueryLifecycleFailureSite::Task { identity, phase }) = &snapshot.failure_site else {
        anyhow::bail!("canonical execution failure is missing its original Task site");
    };
    ensure!(
        *phase == TaskFailurePhase::Execution,
        "failure invented a preparation phase"
    );
    let execution = identity.query_execution_id();
    ensure!(
        execution.attempt_id().get() == 1
            && u64::from_ne_bytes(execution.query_id().high().to_ne_bytes())
                == snapshot.process_namespace
            && execution.query_id().low() as u64 == snapshot.local_sequence,
        "canonical Task site differs from the selected immutable query identity"
    );
    ensure!(
        snapshot.execution_id.as_deref() == Some(execution_text(*identity).as_str()),
        "canonical execution text differs from original Task identity"
    );
    Ok(*identity)
}

fn assert_raw_canonical(
    raw: &BoundedNegativeTextObservation,
    snapshot: &QueryLifecycleStructuredSnapshot,
) -> Result<()> {
    let terminal = raw.terminal.as_ref().context("missing complete raw ERR")?;
    let detail = snapshot
        .failure_detail
        .as_ref()
        .context("missing original safe detail")?;
    ensure!(
        terminal.message == format!("[query.execution.unexpected] {detail}"),
        "raw SQL descriptor/detail differ from the canonical execution input"
    );
    Ok(())
}

fn await_converged(
    context: &mut ScenarioContext,
    observation: &QueryLifecycleStatementObservation,
    success: bool,
) -> Result<QueryLifecycleStructuredSnapshot> {
    let deadline = context.deadline();
    let snapshot = context
        .handle()
        .await_query_lifecycle_statement_observation(observation, success, deadline)?;
    ensure!(
        snapshot.worker_participations_converged,
        "original attempt has no positive convergence proof"
    );
    Ok(snapshot)
}

fn assert_independent_success(
    failed: &QueryLifecycleStructuredSnapshot,
    healthy: &QueryLifecycleStructuredSnapshot,
) -> Result<()> {
    ensure!(
        healthy.worker_participations_converged
            && healthy.attempt_id == 1
            && healthy.failure_fact.is_none()
            && healthy.failure_detail.is_none()
            && healthy.failure_site.is_none(),
        "healthy statement inherited execution failure or lacks positive context convergence"
    );
    ensure!(
        healthy.execution_id.is_some()
            && healthy.execution_id != failed.execution_id
            && healthy.process_namespace == failed.process_namespace
            && healthy.local_sequence != failed.local_sequence,
        "healthy statement did not acquire its own execution in the original FE process"
    );
    Ok(())
}

fn assert_healthy_wire(healthy: &OwnedTextResultObservation) -> Result<()> {
    ensure!(
        healthy.actual_failure.is_none()
            && healthy.server_result_error_code.is_none()
            && healthy.observation.error.is_none()
            && healthy.observation.rows == 1
            && healthy.observation.columns == 1,
        "same socket failed to run the next exact healthy statement"
    );
    ensure!(
        healthy.observation.row_sha256 == healthy_row_digest(),
        "same socket next statement returned different row bytes"
    );
    Ok(())
}

fn healthy_row_digest() -> String {
    // The original owned positive observer commits each row with its u64
    // little-endian payload length after the complete row payload.
    let mut digest = Sha256::new();
    digest.update([4, b'5', b'0', b'5', b'0']);
    digest.update(5u64.to_le_bytes());
    format!("{:x}", digest.finalize())
}

fn execution_text(task: TaskIdentity) -> String {
    let execution = task.query_execution_id();
    format!(
        "{}:{}:{}",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get()
    )
}

// This original marker proves the root-scoped proposal/hit only. The
// independently applied statement snapshot above is the accepting authority.
fn assert_proposal_marker(
    context: &mut ScenarioContext,
    backend: usize,
    task: TaskIdentity,
    token: &str,
) -> Result<()> {
    ensure!(
        context.handle().backend_process_id(backend)? == task.backend_process_id(),
        "root proposal came from a different backend process"
    );
    let log = context.handle().be_log_contents(backend)?;
    let token_field = format!("token={token}");
    let lines = log
        .lines()
        .filter(|line| {
            line.split_whitespace().next() == Some(PROPOSED)
                && line.split_whitespace().any(|field| field == token_field)
        })
        .collect::<Vec<_>>();
    ensure!(
        lines.len() == 1,
        "expected exactly one original root Task failure proposal"
    );
    let fields: Vec<_> = lines[0].split_whitespace().collect();
    let expected = [
        PROPOSED.into(),
        format!("execution_id={}", execution_text(task)),
        format!("stage={}", task.stage_id().get()),
        format!("task={}", task.task_id().get()),
        format!("backend_index={backend}"),
        token_field,
    ];
    ensure!(
        fields
            .iter()
            .copied()
            .eq(expected.iter().map(String::as_str)),
        "proposal marker differs from the snapshot's accepted Task identity"
    );
    Ok(())
}

fn assert_accepted_marker(
    context: &mut ScenarioContext,
    backend: usize,
    task: TaskIdentity,
    token: &str,
) -> Result<()> {
    ensure!(
        context.handle().backend_process_id(backend)? == task.backend_process_id(),
        "accepted action came from a different backend process"
    );
    let log = context.handle().be_log_contents(backend)?;
    let token_field = format!("token={token}");
    let lines = log
        .lines()
        .filter(|line| {
            line.split_whitespace().next() == Some(ACCEPTED)
                && line.split_whitespace().any(|field| field == token_field)
        })
        .collect::<Vec<_>>();
    ensure!(
        lines.len() == 1,
        "accepted action must have one original exact-Task marker"
    );
    let fields: Vec<_> = lines[0].split_whitespace().collect();
    let expected = [
        ACCEPTED.into(),
        format!("execution_id={}", execution_text(task)),
        format!("stage={}", task.stage_id().get()),
        format!("task={}", task.task_id().get()),
        format!("backend={}", task.backend_process_id()),
        token_field,
    ];
    ensure!(
        fields.len() == 7
            && fields[..6]
                .iter()
                .copied()
                .eq(expected.iter().map(String::as_str)),
        "accepted marker differs from the original selected Task identity"
    );
    // The raw client prefix is the phase proof. FE may have prefetched this
    // second read with consumed=0, so only the original Backend watermark is
    // recorded; it must never be fabricated from client visibility.
    let consumed = fields[6]
        .strip_prefix("accepted_consumed=")
        .context("missing actual ACK frontier")?;
    let value: u64 = consumed.parse().context("invalid actual ACK frontier")?;
    ensure!(
        value.to_string() == consumed,
        "actual accepted Root watermark must have its canonical integer encoding"
    );
    Ok(())
}

fn assert_no_replacement(
    context: &mut ScenarioContext,
    selected: &QueryLifecycleStructuredSnapshot,
) -> Result<()> {
    let high = i64::from_ne_bytes(selected.process_namespace.to_ne_bytes());
    let query_prefix = format!("{high}:{}:", selected.local_sequence);
    let mut observed = 0;
    for backend in 0..context.handle().be_count() {
        let log = context.handle().be_log_contents(backend)?;
        for line in log
            .lines()
            .filter(|line| line.split_whitespace().next() == Some(CREATED))
        {
            let identity = line
                .split_whitespace()
                .find_map(|field| field.strip_prefix("execution_id="))
                .context("task create marker missing execution identity")?;
            if let Some(attempt) = identity.strip_prefix(&query_prefix) {
                ensure!(
                    attempt == "1",
                    "selected query created a replacement attempt: {identity}"
                );
                observed += 1;
            }
        }
    }
    ensure!(
        observed > 0,
        "selected statement never crossed native Task creation"
    );
    Ok(())
}

fn snapshot_evidence(snapshot: &QueryLifecycleStructuredSnapshot) -> Value {
    json!({"execution_id":snapshot.execution_id,"attempt_id":snapshot.attempt_id,
        "failure_fact":format!("{:?}",snapshot.failure_fact),
        "failure_detail":snapshot.failure_detail,"failure_site":format!("{:?}",snapshot.failure_site),
        "worker_participations_converged":snapshot.worker_participations_converged})
}
