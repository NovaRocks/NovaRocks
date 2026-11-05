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
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{
    LaunchProfile, QueryLifecycleFailureSite, QueryLifecycleStatementObservation,
    QueryLifecycleStructuredSnapshot, TestClusterOwner, test_sessions::ClusterTestSessionPlan,
};
use novarocks_execution_contract::{
    TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact, FailureReason, domain},
};
use serde_json::{Value, json};
use std::{
    num::NonZeroU16,
    path::Path,
    time::{Duration, Instant},
};

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
        run_failure_trino(context, &self.intent, self.stage)
    }
}

/// Both original finite failure stages retain the exact Task cause and owners.
/// The visible stage activates only after EOF and Join of its first complete
/// one-row public page; the original wanted=2 hold provides that boundary.
fn run_failure_trino(
    context: &mut ScenarioContext,
    intent: &SqlIntentOwner,
    stage: Stage,
) -> Result<()> {
    use super::execution_failure::{
        check_payload_columns, observe_zero_row_failure, run_original_pending,
    };
    use crate::actors::sql::{SqlSession, TrinoValue};
    use crate::actors::trino_protocol::TrinoClient;
    use novarocks_test_trino_client::TrinoError;
    ensure!(
        context.handle().be_count() == 3,
        "failure requires real native 1FE+3BE"
    );
    let launches = context.recheck_live_process_launch_identities()?;
    let baseline = query_lifecycle::resource_snapshot(context)?;
    let deadline = context.deadline();
    let selected = intent.take(0)?;
    let mut observations = Vec::new();
    let mut live = None;
    let mut evidence = json!({"scenario":stage.name(),
        "prefix_rows":if stage==Stage::AfterVisibleRow {vec!["1"]} else {vec![]},
        "fault_enabled_after_prefix":false,
        "reader_policy":"every original page reaches EOF/Drop/real reader Join"});
    let primary = (|| -> Result<()> {
        let mut session = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
        let pending = session.submit(stage.sql())?;
        let cancel = pending.cancel_handle();
        let (pending, observation) = selected.arm_pending(context, pending, stage.sql())?;
        observations.push(observation.clone());
        live = Some(cancel);
        let actor = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            deadline,
            &context.scenario_root().join("execution-failure-pages"),
        )?;
        let query_id = pending.query_id.clone();
        let mut uri = pending
            .initial_next_uri
            .clone()
            .context("POST omitted original continuation")?;
        if stage == Stage::AfterVisibleRow {
            let (prefix, next) = await_original_visible_page(&actor, &query_id, uri)?;
            // read_negative_page has already consumed the actual response EOF,
            // dropped that body and joined its one original reader thread.
            evidence["first_published_page"] = prefix;
            context.action("verified the complete first Trino page contains exactly one VARCHAR row 1 and joined its original reader");
            selected.activate_deferred_faults(context)?;
            evidence["fault_enabled_after_prefix"] = json!(true);
            uri = next;
        }
        let observed = observe_zero_row_failure(&actor, &query_id, uri)?;
        live = None;
        drop(pending);
        evidence["negative"] = observed.clone();
        if stage == Stage::BeforeSchema {
            ensure!(
                observed["columns"].is_null(),
                "failure published metadata before schema refusal"
            );
        }
        let error: TrinoError = serde_json::from_value(observed["error"].clone())?;
        ensure!(
            error.error_code == 65536
                && error.error_name == "GENERIC_INTERNAL_ERROR"
                && error.error_type == "INTERNAL_ERROR",
            "NATIVE20 Unexpected changed its independent Trino mapping: {error:?}"
        );
        let snapshot = await_converged(context, &observation, false)?;
        let task = assert_failed_snapshot(&snapshot)?;
        ensure!(
            error.message
                == format!(
                    "[query.execution.unexpected] {}",
                    snapshot
                        .failure_detail
                        .as_deref()
                        .context("canonical failure omitted original safe detail")?
                ),
            "public typed error differs from the canonical Task failure"
        );
        evidence["selected_failure"] = snapshot_evidence(&snapshot);
        if stage == Stage::BeforeSchema {
            let statement = context
                .handle()
                .query_lifecycle_statement_token(&observation, deadline)?;
            assert_trino_handoff_marker(context, &selected.token, statement)?;
            evidence["original_failure_observed_before_protocol_handoff"] = json!(true);
        }
        let marker = if stage == Stage::AfterVisibleRow {
            ACCEPTED
        } else {
            PROPOSED
        };
        let backend = query_lifecycle::await_token_scoped_marker(
            context,
            marker,
            std::slice::from_ref(&selected.token),
        )?;
        if stage == Stage::AfterVisibleRow {
            assert_accepted_marker(context, backend, task, &selected.token)?;
        } else {
            assert_proposal_marker(context, backend, task, &selected.token)?;
        }
        let hits = (0..3)
            .map(|index| {
                context
                    .handle()
                    .selected_query_lifecycle_fault_hit(index, stage.action(), &selected.token)
                    .map(|hit| (index, hit))
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            hits.iter().filter(|(_, hit)| *hit).count() == 1 && hits[backend].1,
            "failure stage lost its one exact root action hit"
        );
        assert_no_replacement(context, &snapshot)?;
        evidence[if stage == Stage::AfterVisibleRow {
            "accepted_backend"
        } else {
            "proposal_backend"
        }] = json!(backend);
        context.handle().clear_query_lifecycle_faults()?;
        context
            .handle()
            .drain_query_lifecycle_statement_binding(&observation, deadline)?;
        observations.clear();
        let next = intent.take(1)?;
        let pending = session.submit(HEALTHY)?;
        let (pending, next_observation) = next.arm_pending(context, pending, HEALTHY)?;
        observations.push(next_observation.clone());
        let result = run_original_pending(&mut session, pending, deadline)?;
        check_payload_columns(&result.columns)?;
        ensure!(
            result.rows == [vec![TrinoValue::Text("5050".into())]],
            "next explicit-session query changed its exact text/NULL/width result"
        );
        let next_snapshot = await_converged(context, &next_observation, true)?;
        assert_independent_success(&snapshot, &next_snapshot)?;
        let mut independent = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
        let other = intent.take(2)?;
        let pending = independent.submit(HEALTHY)?;
        let (pending, other_observation) = other.arm_pending(context, pending, HEALTHY)?;
        observations.push(other_observation.clone());
        let result = run_original_pending(&mut independent, pending, deadline)?;
        check_payload_columns(&result.columns)?;
        ensure!(
            result.rows == [vec![TrinoValue::Text("5050".into())]],
            "independent query changed its complete typed result"
        );
        let other_snapshot = await_converged(context, &other_observation, true)?;
        assert_independent_success(&snapshot, &other_snapshot)?;
        ensure!(
            other_snapshot.execution_id != next_snapshot.execution_id,
            "independent and next queries share a logical execution"
        );
        evidence["next_snapshot"] = snapshot_evidence(&next_snapshot);
        evidence["independent_snapshot"] = snapshot_evidence(&other_snapshot);
        assert_no_replacement(context, &snapshot)?;
        ensure!(
            launches == context.recheck_live_process_launch_identities()?,
            "execution failure changed Native process identities"
        );
        Ok(())
    })();
    let mut cleanup = Vec::new();
    if let Some(cancel) = live
        && let Err(error) = cancel.delete(deadline)
    {
        cleanup.push(format!("cancel original public query: {error}"));
    }
    if let Err(error) = context.handle().clear_query_lifecycle_faults() {
        cleanup.push(format!("clear original fault actions: {error:#}"));
    }
    if let Err(error) = query_lifecycle::await_resource_convergence(context, &baseline) {
        cleanup.push(format!("original resource convergence: {error:#}"));
    }
    for observation in &observations {
        if let Err(error) = context
            .handle()
            .drain_query_lifecycle_statement_observation(observation)
        {
            cleanup.push(format!("drain original binding: {error:#}"));
        }
    }
    evidence["primary_succeeded"] = json!(primary.is_ok());
    evidence["cleanup"] = json!({"complete":cleanup.is_empty(),"errors":cleanup});
    if let Err(error) = std::fs::write(
        context.scenario_root().join(stage.evidence_file()),
        serde_json::to_vec_pretty(&evidence)?,
    ) {
        cleanup.push(format!("save scene evidence: {error}"));
    }
    match (primary, cleanup.is_empty()) {
        (Ok(()), true) => Ok(()),
        (Err(primary), true) => Err(primary),
        (Ok(()), false) => {
            anyhow::bail!("execution-failure cleanup failed: {}", cleanup.join("; "))
        }
        (Err(primary), false) => {
            Err(primary.context(format!("execution-failure cleanup: {}", cleanup.join("; "))))
        }
    }
}

fn await_original_visible_page(
    actor: &crate::actors::trino_protocol::TrinoClient,
    query: &str,
    mut uri: String,
) -> Result<(Value, String)> {
    use super::execution_failure::{check_payload_columns, read_negative_page};
    use novarocks_test_trino_client::Column;
    let mut columns = None;
    loop {
        let raw = read_negative_page(actor, &uri)?;
        ensure!(raw.status == 200, "visible-prefix page was not HTTP200");
        let page = raw.json()?;
        ensure!(
            page["id"].as_str() == Some(query)
                && page.get("error").is_none_or(Value::is_null)
                && page.get("updateType").is_none_or(Value::is_null)
                && page.get("updateCount").is_none_or(Value::is_null),
            "original visible-prefix page changed identity, failed early or became completion"
        );
        if let Some(value) = page.get("columns").filter(|value| !value.is_null()) {
            let actual: Vec<Column> = serde_json::from_value(value.clone())?;
            check_payload_columns(&actual)?;
            ensure!(
                columns.as_ref().is_none_or(|previous| previous == &actual),
                "original visible-prefix VARCHAR declaration changed between pages"
            );
            columns = Some(actual);
        }
        let next = raw
            .next_uri()?
            .context("visible-prefix query reached success EOF before original fault activation")?;
        if let Some(data) = page.get("data").filter(|data| !data.is_null()) {
            let rows = data
                .as_array()
                .context("visible-prefix data was not an array")?;
            if !rows.is_empty() {
                ensure!(
                    columns.is_some(),
                    "visible row preceded its exact payload declaration"
                );
                ensure!(
                    rows.len() == 1
                        && rows[0]
                            .as_array()
                            .is_some_and(|row| row.len() == 1 && row[0].as_str() == Some("1")),
                    "original wanted=2 hold did not retain exactly the one complete public row 1"
                );
                ensure!(
                    page["stats"]["state"] == "RUNNING",
                    "visible-prefix page was not the original in-flight result"
                );
                return Ok((page, next));
            }
        }
        uri = next;
    }
}

fn assert_trino_handoff_marker(
    context: &mut ScenarioContext,
    token: &str,
    expected: (u64, u64),
) -> Result<()> {
    const MARKER: &str = novarocks_failpoint::observation_marker::RESULT_HANDOFF_TERMINAL_OBSERVED;
    let token = format!("token={token}");
    loop {
        let log = context.handle().fe_log_contents()?;
        let matches = log
            .lines()
            .filter(|line| {
                line.split_whitespace().next() == Some(MARKER)
                    && line.split_whitespace().any(|field| field == token)
            })
            .collect::<Vec<_>>();
        if matches.is_empty() {
            std::thread::sleep(
                context
                    .remaining("observe exact FE handoff action")?
                    .min(Duration::from_millis(20)),
            );
            continue;
        }
        ensure!(
            matches.len() == 1,
            "expected one original before-schema handoff hit"
        );
        let statement = matches[0]
            .split_whitespace()
            .find_map(|field| field.strip_prefix("statement="))
            .context("original handoff omitted actual statement token")?;
        ensure!(
            expected.0 > 0
                && expected.1 > 0
                && statement == format!("{}:{}", expected.0, expected.1),
            "handoff does not name the exact public-query observer's original statement"
        );
        return Ok(());
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
