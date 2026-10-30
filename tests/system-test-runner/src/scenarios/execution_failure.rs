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

//! Real hard-limit refusal, policy, client continuity, and actual owner exit.

use crate::actors::mysql_stream::{
    AsyncMysqlStream, BoundedErrorPrefix, BoundedNegativeTextObservation, TextResultObservation,
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use crate::scenarios::{
    native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec},
    query_lifecycle::{await_resource_convergence, resource_snapshot},
};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{
    QueryLifecycleStatementObservation, QueryLifecycleStructuredSnapshot, ServerHandle,
};
use novarocks_execution_contract::failure::{
    FailureClass, FailureEvidence, FailureFact, ResourceDimension, domain,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::Path,
    time::{Duration, Instant},
};

const BACKENDS: usize = 3;
// The finite UNION ALL uses real compiled Native preparation. The hint is
// statement-scoped; neither subsequent query inherits its one-byte hard limit.
const RESOURCE_SQL: &str = "SELECT /*+ SET_VAR(query_mem_limit=1) */ CAST(v AS VARCHAR) AS payload FROM (SELECT 17 AS v UNION ALL SELECT 19) t ORDER BY v";
pub(super) const NEXT_SQL: &str =
    "SELECT CAST(v AS VARCHAR) AS payload FROM (SELECT 17 AS v UNION ALL SELECT 19) t ORDER BY v";
pub(super) const INDEPENDENT_SQL: &str = "SELECT CAST(v AS VARCHAR) AS payload FROM (SELECT TRUE AS v UNION ALL SELECT TRUE) t ORDER BY v";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(ResourceRecoveryPolicy::default())]
}

#[derive(Default)]
struct ResourceRecoveryPolicy {
    intent: SqlIntentOwner,
}
impl Scenario for ResourceRecoveryPolicy {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        super::declarations::for_module(
            "execution_failure",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }
    fn name(&self) -> &'static str {
        "execution-failure/resource-recovery-policy"
    }
    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let resource = StatementSpec {
            sql: RESOURCE_SQL.into(),
            faults: vec![],
            observations: (0..BACKENDS)
                .flat_map(|index| {
                    [1, 2].map(move |attempt| (index, "grpc-fragment", Some(attempt)))
                })
                .collect(),
        };
        let healthy = |sql: &str| StatementSpec {
            sql: sql.into(),
            faults: vec![],
            observations: (0..BACKENDS)
                .map(|index| (index, "grpc-fragment", None))
                .collect(),
        };
        Ok(Some(self.intent.freeze(
            vec![resource, healthy(NEXT_SQL), healthy(INDEPENDENT_SQL)],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_rejection_policy(context, &self.intent, 1041, assert_resource_terminal)
    }
}

/// A finite real-SQL fixture shared by the two explicit rejection scenarios.
/// The supplied oracle reads the original canonical terminal; no cause is
/// injected, guessed from text, or reconstructed by this helper.
pub(super) fn run_rejection_policy(
    context: &mut ScenarioContext,
    intent: &SqlIntentOwner,
    expected_errno: u16,
    terminal_oracle: fn(&QueryLifecycleStructuredSnapshot) -> Result<()>,
) -> Result<()> {
    ensure!(
        context.handle().be_count() == BACKENDS,
        "execution rejection requires native 1FE+3BE"
    );
    let baseline = resource_snapshot(context)?;
    ensure!(
        baseline.fe_running && baseline.backends.iter().all(|b| b.process_running),
        "execution rejection baseline contains a stopped role"
    );
    let processes = context.recheck_live_process_launch_identities()?;
    let resource = intent.take(0)?;
    let next = intent.take(1)?;
    let independent = intent.take(2)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create the existing asynchronous MySQL client runtime")?;
    let mut observations = Vec::new();
    let result = run_rejection_body(
        &runtime,
        context,
        &resource,
        &next,
        &independent,
        &mut observations,
        expected_errno,
        terminal_oracle,
    );
    // All client futures have settled and their streams have been dropped
    // before draining these exact statement bindings and real owners.
    let mut cleanup_failures = Vec::new();
    for observation in &observations {
        let deadline = context.deadline();
        if let Err(error) = context
            .handle()
            .drain_query_lifecycle_statement_binding(observation, deadline)
        {
            cleanup_failures
                .push(error.context("drain exact rejection-scenario statement binding"));
        }
    }
    if let Err(error) = await_resource_convergence(context, &baseline) {
        cleanup_failures.push(error);
    }
    match context.recheck_live_process_launch_identities() {
        Ok(after) if after == processes => {}
        Ok(_) => cleanup_failures.push(anyhow::anyhow!(
            "execution rejection changed a live role process identity"
        )),
        Err(error) => cleanup_failures.push(error),
    }
    match resource_snapshot(context) {
        Ok(after) => {
            if let Err(error) = write_artifact(
                context,
                "rejection-cleanup.json",
                &json!({
                    "processes_before": processes,
                    "resources_before": resource_json(&baseline),
                    "resources_after": resource_json(&after),
                    "cleanup_complete": cleanup_failures.is_empty(),
                }),
            ) {
                cleanup_failures.push(error);
            }
        }
        Err(error) => cleanup_failures.push(error),
    }
    match (result, cleanup_failures.is_empty()) {
        (Ok(()), true) => Ok(()),
        (Err(primary), true) => Err(primary),
        (Ok(()), false) => Err(anyhow::anyhow!(
            "rejection-scenario cleanup failed: {cleanup_failures:#?}"
        )),
        (Err(primary), false) => Err(primary.context(format!(
            "rejection-scenario cleanup also failed: {cleanup_failures:#?}"
        ))),
    }
}

fn run_rejection_body(
    runtime: &tokio::runtime::Runtime,
    context: &mut ScenarioContext,
    resource: &SelectedStatement,
    next: &SelectedStatement,
    independent: &SelectedStatement,
    observations: &mut Vec<QueryLifecycleStatementObservation>,
    expected_errno: u16,
    terminal_oracle: fn(&QueryLifecycleStructuredSnapshot) -> Result<()>,
) -> Result<()> {
    let deadline = context.deadline();
    let mut stream = runtime.block_on(connect_until(context, deadline))?;
    let original_connection = stream.connection_id()?;
    let observation = resource.arm(context, original_connection, &resource.sql)?;
    observations.push(observation.clone());
    let (ready, ready_receiver) = tokio::sync::oneshot::channel();
    let error = runtime.block_on(stream.observe_bounded_execution_error(
        &resource.sql,
        BoundedErrorPrefix::BeforeRows,
        ready,
        deadline,
    ));
    drop(ready_receiver);
    // Retain the entire bounded original packet even when a later assertion
    // fails. Parser diagnostics are not a source of failure classification.
    write_artifact(
        context,
        "rejection-original-err.json",
        &serde_json::to_value(&error)?,
    )?;
    assert_rejection_packet(&error, expected_errno)?;
    let terminal = context
        .handle()
        .await_query_lifecycle_statement_observation(&observation, false, deadline)?;
    write_artifact(
        context,
        "rejection-terminal.json",
        &terminal_json(&terminal),
    )?;
    terminal_oracle(&terminal)?;
    let mut native_observations = Vec::new();
    let mut replacement_observed = false;
    for index in 0..BACKENDS {
        let first = context.handle().selected_native_observation_binding(
            index,
            "grpc-fragment",
            &resource.token,
            Some(1),
        )?;
        let replacement = context.handle().selected_native_observation_binding(
            index,
            "grpc-fragment",
            &resource.token,
            Some(2),
        )?;
        replacement_observed |= replacement.is_some();
        let binding_json =
            |binding: novarocks_failpoint::target_binding::StatementExecutionBinding| {
                json!({
                    "backend_index": binding.backend_index,
                    "backend_process_id": binding.backend_process_id,
                    "query_id_hi": binding.query_id_hi, "query_id_lo": binding.query_id_lo,
                    "attempt_id": binding.attempt_id,
                })
            };
        native_observations.push(json!({
            "backend_index": index,
            "original_attempt": first.map(binding_json),
            "replacement_attempt": replacement.map(binding_json),
        }));
    }
    write_artifact(
        context,
        "rejection-native-attempts.json",
        &json!(native_observations),
    )?;
    ensure!(
        !replacement_observed,
        "deterministic execution rejection created replacement-attempt work"
    );
    context.action("observed the complete expected ERR, authoritative original failure facts, attempt 1, and actual Worker context convergence");

    // Reuse this exact socket: the first ERR must not poison a healthy stream
    // or leak SET_VAR(query_mem_limit=1) into the next statement.
    // The original terminal snapshot has been retained above. A connection
    // has one observer, so drain that exact completed binding before reuse.
    context
        .handle()
        .drain_query_lifecycle_statement_binding(&observations[0], deadline)?;
    observations.remove(0);
    let next_observation = next.arm(context, original_connection, &next.sql)?;
    observations.push(next_observation.clone());
    let next_result = runtime.block_on(stream.observe_text_query_owned_until(
        &next.sql,
        Duration::ZERO,
        None,
        deadline,
    ));
    write_artifact(
        context,
        "rejection-same-connection-next.json",
        &serde_json::to_value(&next_result.observation)?,
    )?;
    if let Some(error) = next_result.actual_failure {
        return Err(error.context("same-connection next query failed"));
    }
    assert_text_rows(&next_result.observation, &["17", "19"])?;
    ensure!(
        stream.connection_id()? == original_connection,
        "next query replaced the original MySQL connection"
    );
    let next_terminal = context
        .handle()
        .await_query_lifecycle_statement_observation(&next_observation, true, deadline)?;
    write_artifact(
        context,
        "rejection-same-connection-terminal.json",
        &terminal_json(&next_terminal),
    )?;
    assert_healthy_terminal(&next_terminal)?;

    // The failed connection remains open while an independent connection
    // executes finite Native work against the same surviving three roles.
    let mut other = runtime.block_on(connect_until(context, deadline))?;
    let other_connection = other.connection_id()?;
    ensure!(
        other_connection != original_connection,
        "independent query reused the original live connection identity"
    );
    let other_observation = independent.arm(context, other_connection, &independent.sql)?;
    observations.push(other_observation.clone());
    let other_result = runtime.block_on(other.observe_text_query_owned_until(
        &independent.sql,
        Duration::ZERO,
        None,
        deadline,
    ));
    write_artifact(
        context,
        "rejection-independent-query.json",
        &serde_json::to_value(&other_result.observation)?,
    )?;
    if let Some(error) = other_result.actual_failure {
        return Err(error.context("independent query failed"));
    }
    // Explicit Boolean-to-VARCHAR casting returns text, unlike the native BOOL wire value.
    assert_text_rows(&other_result.observation, &["true", "true"])?;
    let other_terminal = context
        .handle()
        .await_query_lifecycle_statement_observation(&other_observation, true, deadline)?;
    write_artifact(
        context,
        "rejection-independent-terminal.json",
        &terminal_json(&other_terminal),
    )?;
    assert_healthy_terminal(&other_terminal)?;
    context.action("verified the original socket's next statement and an independent Native query returned exact rows after execution rejection");
    Ok(())
}

async fn connect_until(context: &ScenarioContext, deadline: Instant) -> Result<AsyncMysqlStream> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .context("original resource-scenario deadline expired before MySQL connect")?;
    tokio::time::timeout_at(
        deadline.into(),
        AsyncMysqlStream::connect(context.mysql_user(), context.mysql_port(), remaining),
    )
    .await
    .context("original resource-scenario deadline expired during MySQL connect")?
}

fn assert_rejection_packet(
    observed: &BoundedNegativeTextObservation,
    expected_errno: u16,
) -> Result<()> {
    ensure!(
        observed.text.error.is_none(),
        "did not receive a complete execution ERR: {:?}",
        observed.text.error
    );
    ensure!(
        observed.pending_packet.is_none() && observed.text.rows == 0,
        "rejected query returned a partial packet or visible row"
    );
    let error = observed
        .terminal
        .as_ref()
        .context("rejected query did not return ERR")?;
    ensure!(
        error.code == expected_errno && error.sqlstate == "HY000",
        "execution rejection changed errno/SQLSTATE: {}/{}",
        error.code,
        error.sqlstate
    );
    ensure!(
        error.payload_bytes >= 9
            && error.payload_bytes <= 4096
            && error.payload_hex.len() == 2 * error.payload_bytes
            && error.payload_hex.starts_with(&format!(
                "ff{:02x}{:02x}234859303030",
                expected_errno.to_le_bytes()[0],
                expected_errno.to_le_bytes()[1]
            ))
            && observed.terminal_payload_hex.as_deref() == Some(error.payload_hex.as_str()),
        "original rejection ERR packet was truncated or differs from its parsed fixed header"
    );
    ensure!(
        !error.message.is_empty(),
        "execution ERR lost its producer-safe detail"
    );
    if observed.completed_metadata {
        ensure!(
            observed.text.columns == 1
                && observed.text.schema.len() == 1
                && observed.text.schema[0].name == "payload"
                && observed.text.schema[0].mysql_type == 253,
            "rejection metadata differs from the exact nullable VARCHAR result"
        );
        ensure!(
            error.sequence == 4,
            "post-metadata zero-row ERR sequence changed"
        );
    } else {
        ensure!(
            observed.text.columns == 0
                && observed.metadata_payload_hex.is_empty()
                && error.sequence == 1,
            "pre-schema resource ERR contained an unexpected prefix"
        );
    }
    Ok(())
}

fn assert_resource_terminal(terminal: &QueryLifecycleStructuredSnapshot) -> Result<()> {
    ensure!(
        terminal.execution_id.is_some() && terminal.attempt_id == 1,
        "hard-limit rejection did not retain the original attempt"
    );
    let fact = terminal
        .failure_fact
        .context("selected terminal omitted the authoritative canonical failure fact")?;
    ensure!(
        fact.class() == FailureClass::Resource
            && fact.evidence() == FailureEvidence::Originating
            && fact.reason().domain() == domain::MEMORY.get()
            && fact.reason().code() == 8,
        "selected failure was not the original query tracker hard limit: {fact:?}"
    );
    ensure!(
        fact.row_position().is_none() && fact.representation().is_none(),
        "tracker limit acquired invented row/representation facts"
    );
    let resource = fact
        .resource()
        .context("original tracker limit omitted resource facts")?;
    ensure!(
        resource.dimension == ResourceDimension::Bytes
            && resource.limit == Some(1)
            && resource.requested.is_some_and(|bytes| bytes > 1)
            && resource.committed.is_some_and(|bytes| bytes > 1),
        "actual hard-limit measurements were not preserved: {resource:?}"
    );
    ensure!(
        resource.request_scope.is_none()
            && resource.constraint_scope.is_none()
            && resource.required_growth.is_none()
            && resource.live.is_none(),
        "unknown tracker measurements were invented: {resource:?}"
    );
    ensure!(
        terminal.worker_contexts_converged,
        "selected attempt did not obtain actual Worker context convergence"
    );
    Ok(())
}

fn assert_healthy_terminal(terminal: &QueryLifecycleStructuredSnapshot) -> Result<()> {
    ensure!(
        terminal.execution_id.is_some()
            && terminal.attempt_id == 1
            && terminal.failure_fact.is_none()
            && terminal.worker_contexts_converged,
        "healthy continuity query did not finish and actually converge: {terminal:?}"
    );
    Ok(())
}

fn assert_text_rows(observed: &TextResultObservation, expected: &[&str]) -> Result<()> {
    ensure!(
        observed.error.is_none()
            && observed.rows == expected.len() as u64
            && observed.columns == 1
            && observed.schema.len() == 1
            && observed.schema[0].name == "payload"
            && observed.schema[0].mysql_type == 253,
        "healthy query returned an unexpected result shape: {observed:?}"
    );
    let mut digest = Sha256::new();
    for value in expected {
        ensure!(
            value.len() < 251,
            "expected health row exceeds its short-cell fixture"
        );
        digest.update([value.len() as u8]);
        digest.update(value.as_bytes());
        digest.update((value.len() as u64 + 1).to_le_bytes());
    }
    ensure!(
        observed.row_sha256 == format!("{:x}", digest.finalize()),
        "healthy query returned incorrect complete rows"
    );
    Ok(())
}

fn terminal_json(terminal: &QueryLifecycleStructuredSnapshot) -> Value {
    json!({
        "execution_id": terminal.execution_id,
        "process_namespace": terminal.process_namespace,
        "local_sequence": terminal.local_sequence,
        "attempt_id": terminal.attempt_id,
        "worker_contexts_converged": terminal.worker_contexts_converged,
        "failure_fact": terminal.failure_fact.map(fact_json),
        "failure_detail": terminal.failure_detail,
        "failure_site": terminal.failure_site.as_ref().map(|site| format!("{site:?}")),
    })
}
fn fact_json(fact: FailureFact) -> Value {
    json!({
        "class": format!("{:?}", fact.class()),
        "evidence": format!("{:?}", fact.evidence()),
        "reason": {"domain": fact.reason().domain(), "code": fact.reason().code()},
        "row_position": fact.row_position().map(|row| json!({
            "selected_ordinal": row.selected_ordinal, "batch_row": row.batch_row,
        })),
        "representation": fact.representation().map(|r| json!({
            "boundary": format!("{:?}", r.boundary),
            "layout": {"domain": r.layout.domain.get(), "code": r.layout.code.get()},
            "required": r.required, "extent_limit": r.extent_limit,
            "representable_prefix": r.representable_prefix,
        })),
        "resource": fact.resource().map(|r| json!({
            "request_scope": r.request_scope.map(|v| format!("{v:?}")),
            "constraint_scope": r.constraint_scope.map(|v| format!("{v:?}")),
            "dimension": format!("{:?}", r.dimension),
            "requested": r.requested, "required_growth": r.required_growth,
            "limit": r.limit, "live": r.live, "committed": r.committed,
        })),
    })
}
fn resource_json(snapshot: &novarocks_cluster_harness::QueryExecutionResourceSnapshot) -> Value {
    json!({"fe_running": snapshot.fe_running, "backends": snapshot.backends.iter().map(|b| json!({
        "index": b.index, "process_running": b.process_running, "resources": b.resources,
    })).collect::<Vec<_>>()})
}
fn write_artifact(context: &ScenarioContext, name: &str, value: &Value) -> Result<()> {
    fs::write(
        context.scenario_root().join(name),
        serde_json::to_vec_pretty(value)?,
    )
    .with_context(|| format!("write original resource-scenario evidence {name}"))
}
