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

use crate::actors::sql::{SqlSession, StatementResult, TrinoValue};
use crate::actors::trino_protocol::TrinoClient;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use crate::scenarios::{
    native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec},
    query_lifecycle::{await_resource_convergence, resource_snapshot},
};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{
    QueryLifecycleStatementObservation, QueryLifecycleStructuredSnapshot, TestClusterOwner,
};
use novarocks_execution_contract::failure::{
    FailureClass, FailureEvidence, FailureFact, ResourceDimension, domain,
};
use novarocks_test_trino_client::{Column, TrinoError, TypeArgument};
use serde_json::{Value, json};
use std::{fs, path::Path, time::Instant};

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
                    [1, 2].map(move |attempt| (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), Some(attempt)))
                })
                .collect(),
        };
        let healthy = |sql: &str| StatementSpec {
            sql: sql.into(),
            faults: vec![],
            observations: (0..BACKENDS)
                .map(|index| {
                    (
                        index,
                        novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                            .as_str(),
                        None,
                    )
                })
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
        run_rejection_policy(
            context,
            &self.intent,
            FailureClass::Resource,
            assert_resource_terminal,
        )
    }
}

/// A finite real-SQL fixture shared by the two explicit rejection scenarios.
/// The supplied oracle reads the original canonical terminal; no cause is
/// injected, guessed from text, or reconstructed by this helper.
pub(super) fn run_rejection_policy(
    context: &mut ScenarioContext,
    intent: &SqlIntentOwner,
    expected_class: FailureClass,
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
    let mut observations = Vec::new();
    let result = run_rejection_body(
        context,
        &resource,
        &next,
        &independent,
        &mut observations,
        expected_class,
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
    context: &mut ScenarioContext,
    resource: &SelectedStatement,
    next: &SelectedStatement,
    independent: &SelectedStatement,
    observations: &mut Vec<QueryLifecycleStatementObservation>,
    expected_class: FailureClass,
    terminal_oracle: fn(&QueryLifecycleStructuredSnapshot) -> Result<()>,
) -> Result<()> {
    let deadline = context.deadline();
    let mut session = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
    let pending = session.submit(&resource.sql)?;
    let cancel = pending.cancel_handle();
    let (pending, observation) = resource.arm_pending(context, pending, &resource.sql)?;
    observations.push(observation.clone());
    let query_id = pending.query_id.clone();
    let first_uri = pending
        .initial_next_uri
        .clone()
        .context("rejection POST omitted its unstarted continuation")?;
    let actor = TrinoClient::new(
        context.trino_port(),
        context.sql_user(),
        deadline,
        &context.scenario_root().join("rejection-pages"),
    )?;
    let observed = observe_zero_row_failure(&actor, &query_id, first_uri);
    let observed = match observed {
        Ok(value) => value,
        Err(primary) => {
            return match cancel.delete(deadline) {
                Ok(()) => Err(primary),
                Err(cleanup) => {
                    Err(primary.context(format!("cancel exact rejection continuation: {cleanup}")))
                }
            };
        }
    };
    drop(pending);
    write_artifact(context, "rejection-original-error.json", &observed)?;
    let error: TrinoError = serde_json::from_value(observed["error"].clone())?;
    let sql_code = match expected_class {
        FailureClass::Resource => "query.execution.resource_exhausted",
        FailureClass::Representation => "query.execution.representation",
        _ => anyhow::bail!("unsupported original rejection class"),
    };
    ensure!(
        error.error_code == 131072
            && error.error_name == "GENERIC_INSUFFICIENT_RESOURCES"
            && error.error_type == "INSUFFICIENT_RESOURCES"
            && error.message.starts_with(&format!("[{sql_code}] ")),
        "original rejection changed typed Trino identity: {error:?}"
    );
    let terminal = context
        .handle()
        .await_query_lifecycle_statement_observation(&observation, false, deadline)?;
    write_artifact(
        context,
        "rejection-terminal.json",
        &terminal_json(&terminal),
    )?;
    terminal_oracle(&terminal)?;
    ensure!(
        terminal
            .failure_fact
            .is_some_and(|fact| fact.class() == expected_class),
        "public rejection class differs from the authoritative original failure"
    );
    let mut native_observations = Vec::new();
    let mut replacement_observed = false;
    for index in 0..BACKENDS {
        let first = context.handle().selected_native_observation_binding(
            index,
            novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(),
            &resource.token,
            Some(1),
        )?;
        let replacement = context.handle().selected_native_observation_binding(
            index,
            novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(),
            &resource.token,
            Some(2),
        )?;
        replacement_observed |= replacement.is_some();
        let binding_json =
            |binding: novarocks_failpoint::target_binding::StatementExecutionBinding| {
                json!({"backend_index":binding.backend_index,"backend_process_id":binding.backend_process_id,
                "query_id_hi":binding.query_id_hi,"query_id_lo":binding.query_id_lo,"attempt_id":binding.attempt_id})
            };
        native_observations.push(json!({"backend_index":index,
            "original_attempt":first.map(binding_json),"replacement_attempt":replacement.map(binding_json)}));
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
    context.action("observed zero public rows, exact typed rejection, authoritative original failure, attempt 1 and actual Worker convergence");
    context
        .handle()
        .drain_query_lifecycle_statement_binding(&observations[0], deadline)?;
    observations.remove(0);
    // The same explicit session is reused; socket identity is not a Trino contract.
    let pending = session.submit(&next.sql)?;
    let (pending, next_observation) = next.arm_pending(context, pending, &next.sql)?;
    observations.push(next_observation.clone());
    let next_result = run_original_pending(&mut session, pending, deadline)?;
    assert_text_rows(&next_result, &["17", "19"])?;
    write_artifact(
        context,
        "rejection-next-session-statement.json",
        &json!({"query_id":next_result.query_id,"columns":next_result.columns,
        "rows":next_result.rows.iter().map(|row| match row.as_slice() {
            [TrinoValue::Text(value)] => Some(value.as_str()), _ => None,
        }).collect::<Vec<_>>()}),
    )?;
    let next_terminal = context
        .handle()
        .await_query_lifecycle_statement_observation(&next_observation, true, deadline)?;
    assert_healthy_terminal(&next_terminal)?;
    write_artifact(
        context,
        "rejection-next-terminal.json",
        &terminal_json(&next_terminal),
    )?;
    let mut other = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
    let pending = other.submit(&independent.sql)?;
    let (pending, other_observation) =
        independent.arm_pending(context, pending, &independent.sql)?;
    observations.push(other_observation.clone());
    let other_result = run_original_pending(&mut other, pending, deadline)?;
    assert_text_rows(&other_result, &["true", "true"])?;
    let other_terminal = context
        .handle()
        .await_query_lifecycle_statement_observation(&other_observation, true, deadline)?;
    assert_healthy_terminal(&other_terminal)?;
    ensure!(
        other_terminal.execution_id != next_terminal.execution_id
            && next_terminal.execution_id != terminal.execution_id,
        "continuity statements reused the failed or peer logical execution"
    );
    write_artifact(
        context,
        "rejection-independent-terminal.json",
        &terminal_json(&other_terminal),
    )?;
    context.action("verified explicit-session next statement and independent Native query returned exact typed rows after rejection");
    Ok(())
}

pub(super) fn run_original_pending(
    session: &mut SqlSession,
    pending: crate::actors::sql::PendingStatement,
    deadline: Instant,
) -> Result<StatementResult> {
    let cancel = pending.cancel_handle();
    match session.run_pending(pending) {
        Ok(result) => Ok(result),
        Err(primary @ crate::actors::sql::StatementFailure::Trino(_)) => Err(primary.into()),
        Err(primary) => match cancel.delete(deadline) {
            Ok(()) => Err(primary.into()),
            Err(cleanup) => Err(anyhow::Error::new(primary)
                .context(format!("cancel original continuity statement: {cleanup}"))),
        },
    }
}

pub(super) fn check_payload_columns(columns: &[Column]) -> Result<()> {
    ensure!(
        columns.len() == 1
            && columns[0].name == "payload"
            && columns[0].type_name == "varchar"
            && columns[0].type_signature.raw_type == "varchar"
            && columns[0].type_signature.arguments == [TypeArgument::Long(2_147_483_647)],
        "original payload declaration differs from nullable VARCHAR: {columns:?}"
    );
    Ok(())
}

/// A finite failure page owns its actual response reader through EOF/Join.
/// It also carries the visible-prefix scenario's one complete small row.
pub(super) fn read_negative_page(
    actor: &TrinoClient,
    uri: &str,
) -> Result<crate::actors::trino_protocol::RawResponse> {
    let mut held = actor.open_get(uri)?;
    let status = held.status();
    let headers = held.headers().clone();
    let read = (|| -> Result<Vec<u8>> {
        let mut body = Vec::new();
        loop {
            let step = held.read_step(4096)?;
            body.extend_from_slice(&step.bytes);
            // These scenarios require zero rows or one small visible-prefix row;
            // this finite diagnostic bound exceeds their full metadata/error.
            ensure!(
                body.len() <= 128 * 1024,
                "finite failure page exceeded its bounded envelope"
            );
            if step.eof {
                return Ok(body);
            }
        }
    })();
    let joined = if read.is_ok() {
        held.settle()
    } else {
        held.interrupt()
    };
    let settled = joined.require_clean();
    match (read, settled) {
        (Ok(body), Ok(())) => Ok(crate::actors::trino_protocol::RawResponse {
            status,
            headers,
            body,
        }),
        (Err(primary), Ok(())) | (Ok(_), Err(primary)) => Err(primary),
        (Err(primary), Err(cleanup)) => {
            Err(primary.context(format!("original HTTP reader Join failed: {cleanup:#}")))
        }
    }
}

pub(super) fn observe_zero_row_failure(
    actor: &TrinoClient,
    query: &str,
    mut uri: String,
) -> Result<Value> {
    let mut columns = None;
    let mut pages = 0u64;
    loop {
        let raw = read_negative_page(actor, &uri)?;
        ensure!(
            raw.status == 200,
            "execution rejection was not a typed HTTP200 SQL error"
        );
        let page = raw.json()?;
        ensure!(
            page["id"].as_str() == Some(query)
                && page.get("updateType").is_none_or(Value::is_null)
                && page.get("updateCount").is_none_or(Value::is_null),
            "rejection page changed query identity or became a completion"
        );
        ensure!(
            page.get("data")
                .is_none_or(|data| data.is_null() || data.as_array().is_some_and(Vec::is_empty)),
            "rejected query published a visible row"
        );
        if let Some(value) = page.get("columns").filter(|v| !v.is_null()) {
            let actual: Vec<Column> = serde_json::from_value(value.clone())?;
            check_payload_columns(&actual)?;
            ensure!(
                columns.as_ref().is_none_or(|old| old == &actual),
                "rejection columns changed"
            );
            columns = Some(actual);
        }
        pages = pages
            .checked_add(1)
            .context("rejection page count overflow")?;
        if let Some(error) = page.get("error").filter(|v| !v.is_null()) {
            let error: TrinoError = serde_json::from_value(error.clone())?;
            ensure!(
                raw.next_uri()?.is_none() && page["stats"]["state"] == "FAILED",
                "execution rejection was not the original terminal error page"
            );
            return Ok(
                json!({"query_id":query,"pages":pages,"rows":0,"columns":columns,"error":error}),
            );
        }
        uri = raw
            .next_uri()?
            .context("execution rejection unexpectedly completed successfully")?;
    }
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
        terminal.worker_participations_converged,
        "selected attempt did not obtain actual Worker context convergence"
    );
    Ok(())
}

fn assert_healthy_terminal(terminal: &QueryLifecycleStructuredSnapshot) -> Result<()> {
    ensure!(
        terminal.execution_id.is_some()
            && terminal.attempt_id == 1
            && terminal.failure_fact.is_none()
            && terminal.worker_participations_converged,
        "healthy continuity query did not finish and actually converge: {terminal:?}"
    );
    Ok(())
}

fn assert_text_rows(observed: &StatementResult, expected: &[&str]) -> Result<()> {
    check_payload_columns(&observed.columns)?;
    ensure!(
        observed.is_result_set() && observed.rows.len() == expected.len(),
        "healthy query returned another complete result shape"
    );
    for (row, expected) in observed.rows.iter().zip(expected) {
        ensure!(
            matches!(row.as_slice(),[TrinoValue::Text(value)] if value==expected),
            "healthy query changed exact text value/NULL/type"
        );
    }
    Ok(())
}

fn terminal_json(terminal: &QueryLifecycleStructuredSnapshot) -> Value {
    json!({
        "execution_id": terminal.execution_id,
        "process_namespace": terminal.process_namespace,
        "local_sequence": terminal.local_sequence,
        "attempt_id": terminal.attempt_id,
        "worker_participations_converged": terminal.worker_participations_converged,
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
