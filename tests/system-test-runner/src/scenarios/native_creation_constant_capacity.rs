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

//! SQL witnesses for constant capacity on the managed Native cluster.

use std::{collections::BTreeSet, fs, path::Path, time::Instant};

use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::{
    LaunchProfile, QueryLifecycleFailureSite, QueryLifecycleStatementObservation, TestClusterOwner,
    test_sessions::ClusterTestSessionPlan,
};
use novarocks_failpoint::observation_marker::ObservationAction;
use novarocks_test_trino_client::{StatementFailure, StatementResult, TrinoValue};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    actors::sql::SqlSession,
    scenario::{Scenario, ScenarioContext, ScenarioDeclaration, ScenarioLaunchConfig},
    scenarios::{
        declarations,
        native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec},
        query_lifecycle, task_evidence,
    },
};

use super::{REQUIRED_BACKENDS, creation_merge, require_three_backends};

#[path = "native_creation_constant_markers.rs"]
mod task_markers;

pub(in crate::scenarios) const DEFAULT_STATIC: &str = "native-creation/default-static-values";
pub(in crate::scenarios) const DEFAULT_FOLD: &str = "native-creation/default-optional-fold";
pub(in crate::scenarios) const LARGE_STATIC: &str = "native-creation/large-static-values";
pub(in crate::scenarios) const OVERSIZE: &str = "native-creation/oversize-fragment-refusal";

// The independent comparison uses the exact original P00 artifact source.
const BASELINE_SOURCE: &str = "2cc1ed48f62e6e825f8305e5d68b01363b5c441d";
const MIB: usize = 1024 * 1024;
const DEFAULT_QUERY_LENGTH: usize = 1_000_000;
const DEFAULT_RESULT_BYTES: usize = 8 * MIB;
const LARGE_QUERY_LENGTH: usize = 32 * MIB;
const OVERSIZE_REQUEST_LIMIT: usize = 8 * MIB;
const DEFAULT_REQUEST_LIMIT: usize = 64 * MIB;
const TRANSLATE_STEP_FOUR: &str =
    "SELECT length(translated) AS codepoints, length(to_utf8(translated)) AS utf8_bytes,
       crc32(to_utf8(translated)) AS fingerprint
FROM (SELECT translate(rpad('', 1024 * 1024, 'a'), 'a', '膨') AS translated) AS t";
const SMOKE_LENGTH: usize = 494_958;
const GROUPED_ROWS: usize = 8_193;
const SINGLE_BYTES: usize = 9 * MIB;
const GRID_CELL_BYTES: usize = 3 * MIB;

pub(super) fn scenarios() -> impl Iterator<Item = Box<dyn Scenario>> {
    [
        Group::DefaultStatic,
        Group::DefaultFold,
        Group::LargeStatic,
        Group::Oversize,
    ]
    .into_iter()
    .map(|group| {
        Box::new(Witness {
            group,
            intent: SqlIntentOwner::default(),
        }) as Box<dyn Scenario>
    })
}

#[derive(Clone, Copy)]
enum Group {
    DefaultStatic,
    DefaultFold,
    LargeStatic,
    Oversize,
}

impl Group {
    fn cases(self) -> &'static [Case] {
        match self {
            Self::DefaultStatic => &[Case::Smoke, Case::Grouped],
            Self::DefaultFold => &[Case::Fold, Case::Translate],
            Self::Oversize => &[Case::Fold],
            Self::LargeStatic => &[Case::Single, Case::Grid],
        }
    }

    fn query_limit(self) -> usize {
        match self {
            Self::LargeStatic => LARGE_QUERY_LENGTH,
            _ => DEFAULT_QUERY_LENGTH,
        }
    }

    fn request_limit(self) -> usize {
        match self {
            Self::Oversize => OVERSIZE_REQUEST_LIMIT,
            _ => DEFAULT_REQUEST_LIMIT,
        }
    }
}

struct Witness {
    group: Group,
    intent: SqlIntentOwner,
}

impl Scenario for Witness {
    fn name(&self) -> &'static str {
        match self.group {
            Group::DefaultStatic => DEFAULT_STATIC,
            Group::DefaultFold => DEFAULT_FOLD,
            Group::LargeStatic => LARGE_STATIC,
            Group::Oversize => OVERSIZE,
        }
    }

    fn declaration(&self) -> ScenarioDeclaration {
        declarations::for_module(
            "native_creation",
            self.name(),
            self.is_explicit_stage(),
            false,
        )
    }

    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        ensure!(
            profile == LaunchProfile::FaultScenario,
            "constant capacity requires the authenticated managed Native observation profile"
        );
        Ok(())
    }

    fn is_explicit_stage(&self) -> bool {
        matches!(self.group, Group::LargeStatic)
    }

    fn launch_config(&self, _: &Path) -> Result<ScenarioLaunchConfig> {
        let mut launch = ScenarioLaunchConfig::default();
        match self.group {
            Group::LargeStatic => {
                launch.config_overlay.fe = Some(format!(
                    "[standalone_server]\ntrino_query_max_length = {LARGE_QUERY_LENGTH}\n"
                ));
            }
            Group::Oversize => {
                let overlay = format!(
                    "[runtime.native_ingress]\nordinary_request_max_bytes = {OVERSIZE_REQUEST_LIMIT}\n"
                );
                launch.config_overlay.fe = Some(overlay.clone());
                launch.config_overlay.be = Some(overlay);
            }
            Group::DefaultStatic | Group::DefaultFold => {}
        }
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        let statements = self
            .group
            .cases()
            .iter()
            .map(|case| StatementSpec {
                sql: case.sql(),
                faults: vec![],
                observations: (0..cluster_size)
                    .flat_map(|index| {
                        let attempts: &[_] = if matches!(self.group, Group::Oversize) {
                            &[Some(1), Some(2)]
                        } else {
                            &[None]
                        };
                        attempts.iter().map(move |&attempt| {
                            (index, ObservationAction::TaskProtocol.as_str(), attempt)
                        })
                    })
                    .collect(),
            })
            .collect();
        Ok(Some(self.intent.freeze(
            statements,
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let pids = context.process_ids();
        ensure!(
            std::iter::once(pids.frontend)
                .chain(pids.backends)
                .collect::<BTreeSet<_>>()
                .len()
                == REQUIRED_BACKENDS + 1,
            "constant capacity requires four independent role processes"
        );
        let baseline_build = is_baseline(context)?;
        ensure!(
            !baseline_build || matches!(self.group, Group::DefaultStatic | Group::DefaultFold),
            "this scenario requires the current capacity contract"
        );
        record_config(context, self.group, baseline_build)?;
        let idle = query_lifecycle::resource_snapshot(context)?;
        let mut session =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let primary = (|| -> Result<()> {
            for (ordinal, &case) in self.group.cases().iter().enumerate() {
                let selected = self.intent.take(ordinal)?;
                if baseline_build && matches!(case, Case::Translate) {
                    continue;
                }
                run_statement(
                    context,
                    &mut session,
                    &selected,
                    case,
                    baseline_build,
                    matches!(self.group, Group::Oversize),
                )?;
                if baseline_build {
                    capture_baseline_contract(context, &mut session, &selected.sql, case)?;
                }
            }
            Ok(())
        })();
        creation_merge(
            primary,
            query_lifecycle::await_resource_convergence(context, &idle),
        )
    }
}

#[derive(Clone, Copy)]
enum Case {
    Smoke,
    Grouped,
    Fold,
    Translate,
    Single,
    Grid,
}

impl Case {
    fn label(self) -> &'static str {
        match self {
            Self::Smoke => "smoke-990000",
            Self::Grouped => "alternating-8193",
            Self::Fold => "optional-fold-9mib",
            Self::Translate => "function-translate-step-four",
            Self::Single => "single-9mib",
            Self::Grid => "grid-2-by-3",
        }
    }

    fn sql(self) -> String {
        match self {
            Self::Smoke => values_sql(&["v"], &[&[('A', 'x', 'Z')], &[('B', 'y', 'Y')]], SMOKE_LENGTH),
            Self::Grouped => {
                let mut sql = String::from("SELECT v, count(*) FROM (VALUES ");
                for row in 0..GROUPED_ROWS {
                    if row != 0 {
                        sql.push(',');
                    }
                    sql.push_str(if row % 2 == 0 { "(0)" } else { "(1)" });
                }
                sql.push_str(") AS t(v) GROUP BY v");
                sql
            }
            Self::Fold => "SELECT length(v), substr(v, 1, 1), substr(v, -1, 1) FROM (VALUES (rpad('', BIGINT '9437184', 'x'))) AS t(v)".into(),
            Self::Translate => TRANSLATE_STEP_FOUR.into(),
            Self::Single => values_sql(&["v"], &[&[('A', 'x', 'Z')]], SINGLE_BYTES),
            Self::Grid => values_sql(
                &["a", "b", "c"],
                &[&[('A', 'u', 'A'), ('B', 'v', 'B'), ('C', 'w', 'C')],
                  &[('D', 'x', 'D'), ('E', 'y', 'E'), ('F', 'z', 'F')]],
                GRID_CELL_BYTES,
            ),
        }
    }

    fn expected(self) -> Vec<Vec<Cell>> {
        let projected = |bytes: usize, ends: &[(char, char)]| {
            ends.iter()
                .flat_map(|&(first, last)| {
                    [
                        Cell::Integer(bytes as i64),
                        Cell::Text(first.to_string()),
                        Cell::Text(last.to_string()),
                    ]
                })
                .collect()
        };
        match self {
            Self::Smoke => vec![
                projected(SMOKE_LENGTH, &[('A', 'Z')]),
                projected(SMOKE_LENGTH, &[('B', 'Y')]),
            ],
            Self::Grouped => vec![
                vec![Cell::Integer(0), Cell::Integer(4097)],
                vec![Cell::Integer(1), Cell::Integer(4096)],
            ],
            Self::Fold => vec![projected(SINGLE_BYTES, &[('x', 'x')])],
            Self::Translate => vec![vec![
                Cell::Integer(1_048_576),
                Cell::Integer(3_145_728),
                Cell::Integer(3_347_635_901),
            ]],
            Self::Single => vec![projected(SINGLE_BYTES, &[('A', 'Z')])],
            Self::Grid => vec![
                projected(GRID_CELL_BYTES, &[('A', 'A'), ('B', 'B'), ('C', 'C')]),
                projected(GRID_CELL_BYTES, &[('D', 'D'), ('E', 'E'), ('F', 'F')]),
            ],
        }
    }

    fn dimensions(self) -> (usize, usize) {
        match self {
            Self::Smoke => (2, 1),
            Self::Grouped => (GROUPED_ROWS, 1),
            Self::Fold | Self::Translate | Self::Single => (1, 1),
            Self::Grid => (2, 3),
        }
    }

    fn result_types(self) -> Vec<&'static str> {
        match self {
            Self::Grouped => vec!["integer", "bigint"],
            Self::Translate => vec!["bigint"; 3],
            _ => ["bigint", "varchar", "varchar"].repeat(self.dimensions().1),
        }
    }
}

fn values_sql(columns: &[&str], rows: &[&[(char, char, char)]], bytes: usize) -> String {
    let mut sql = String::with_capacity(bytes * rows.len() * columns.len() + 512);
    sql.push_str("SELECT ");
    for (index, name) in columns.iter().enumerate() {
        if index != 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!(
            "length({name}), substr({name}, 1, 1), substr({name}, -1, 1)"
        ));
    }
    sql.push_str(" FROM (VALUES ");
    for (row, cells) in rows.iter().enumerate() {
        if row != 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        for (column, &(first, fill, last)) in cells.iter().enumerate() {
            if column != 0 {
                sql.push_str(", ");
            }
            sql.push('\'');
            sql.push(first);
            sql.extend(std::iter::repeat_n(fill, bytes - 2));
            sql.push(last);
            sql.push('\'');
        }
        sql.push(')');
    }
    sql.push_str(") AS t(");
    sql.push_str(&columns.join(", "));
    sql.push(')');
    sql
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(untagged)]
enum Cell {
    Integer(i64),
    Text(String),
}

fn checked_rows(result: &StatementResult, case: Case) -> Result<Vec<Vec<Cell>>> {
    let actual_types = result
        .columns
        .iter()
        .map(|column| column.type_signature.raw_type.as_str())
        .collect::<Vec<_>>();
    ensure!(
        result.is_result_set()
            && result.update_type.is_none()
            && result.update_count.is_none()
            && actual_types == case.result_types(),
        "{} returned unexpected result metadata: {:?}",
        case.label(),
        result.columns
    );
    let mut rows = result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| match cell {
                    TrinoValue::Integer(value) => Ok(Cell::Integer(*value)),
                    TrinoValue::Text(value) if value.len() == 1 && value.is_ascii() => {
                        Ok(Cell::Text(value.clone()))
                    }
                    unexpected => bail!(
                        "{} returned unexpected or non-ASCII cell kind {:?}",
                        case.label(),
                        std::mem::discriminant(unexpected)
                    ),
                })
                .collect()
        })
        .collect::<Result<Vec<Vec<_>>>>()?;
    let mut expected = case.expected();
    rows.sort();
    expected.sort();
    ensure!(
        rows == expected,
        "{} returned {rows:?}, expected {expected:?}",
        case.label()
    );
    Ok(rows)
}

fn run_statement(
    context: &mut ScenarioContext,
    session: &mut SqlSession,
    selected: &SelectedStatement,
    case: Case,
    baseline_build: bool,
    expect_oversize: bool,
) -> Result<()> {
    let before_freeze = expect_oversize
        .then(|| super::completed_statement_freeze_bytes(context))
        .transpose()?;
    let root = context.scenario_root().to_owned();
    let client_record = root.join(format!("{}.client.json", case.label()));
    let digest = format!("{:x}", Sha256::digest(selected.sql.as_bytes()));
    let sql_units = selected.sql.encode_utf16().count();
    if matches!(case, Case::Smoke) {
        ensure!(sql_units == 990_000, "default smoke SQL length drifted");
    }
    let pending = match session.submit(&selected.sql) {
        Ok(pending) => pending,
        Err(error) => {
            let report = write_record(
                &client_record,
                &json!({"phase": "post", "sql_sha256": digest,
                "sql_utf16_units": sql_units, "failure": error}),
            );
            return creation_merge(Err(anyhow::Error::from(error)), report);
        }
    };
    let query_id = pending.query_id.clone();
    let initial_continuation = pending.initial_next_uri.is_some();
    let cancel = pending.cancel_handle();
    let (pending, observation) = match selected.arm_pending(context, pending, &selected.sql) {
        Ok(armed) => armed,
        Err(error) => {
            let recorded = write_record(
                &client_record,
                &json!({
                    "phase": "bind-observation", "query_id": query_id, "sql_sha256": digest,
                    "sql_utf16_units": sql_units, "failure": format!("{error:#}")
                }),
            );
            return creation_merge(Err(error), recorded);
        }
    };
    let result = session.run_pending(pending);
    let deadline = context.deadline();
    let client_evidence = match &result {
        Ok(success) => json!({
            "phase": "terminal", "query_id": query_id, "sql_sha256": digest,
            "sql_utf16_units": sql_units, "initial_continuation": initial_continuation,
            "columns": success.columns, "pages": success.pages, "terminal_page": true
        }),
        Err(error) => json!({
            "phase": "drain", "query_id": query_id, "sql_sha256": digest,
            "sql_utf16_units": sql_units, "initial_continuation": initial_continuation,
            "failure": error
        }),
    };
    let recorded = write_record(&client_record, &client_evidence);
    let cancel_needed =
        matches!(&result, Err(error) if !matches!(error, StatementFailure::Trino(_)));
    let primary = (|| -> Result<()> {
        if let Some(before_freeze) = before_freeze {
            ensure!(
                initial_continuation,
                "oversize rejection requires the original terminal GET"
            );
            return check_oversize_refusal(
                context,
                selected,
                &observation,
                &query_id,
                result,
                before_freeze,
            );
        }
        let output = result.with_context(|| format!("drain {} Trino statement", case.label()))?;
        ensure!(
            initial_continuation && output.query_id == query_id && output.pages >= 2,
            "a real terminal GET for the original {} Trino query is required",
            case.label()
        );
        let rows = checked_rows(&output, case)?;
        write_record(
            &root.join(format!("{}.oracle.json", case.label())),
            &json!({"rows": rows, "expected": case.expected()}),
        )?;
        let terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(&observation, true, deadline)?;
        let backends = task_evidence::assert_query_completed_across_boundary(
            context,
            &terminal,
            case.label(),
        )?;
        let execution = terminal
            .execution_id
            .as_deref()
            .context("missing exact terminal execution")?;
        let tasks = task_markers::await_exact_tasks(
            context,
            selected,
            execution,
            &backends,
            case,
            !baseline_build,
        )?;
        write_record(
            &root.join(format!("{}.task.json", case.label())),
            &json!({
                "client_query_id": query_id, "execution_id": execution, "attempt": terminal.attempt_id,
                "released_backends": backends, "tasks": tasks, "layout_observation": !baseline_build
            }),
        )?;
        context.action(format!(
            "{} reached the original Trino terminal page and Native Task exit",
            case.label()
        ));
        Ok(())
    })();
    let cancelled = if cancel_needed {
        cancel.delete(deadline).map_err(anyhow::Error::from)
    } else {
        Ok(())
    };
    let drained = context
        .handle()
        .drain_query_lifecycle_statement_binding(&observation, deadline);
    creation_merge(
        primary,
        creation_merge(recorded, creation_merge(cancelled, drained)),
    )
}

fn check_oversize_refusal(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
    observation: &QueryLifecycleStatementObservation,
    query_id: &str,
    result: Result<StatementResult, StatementFailure>,
    before_freeze: (i64, i64),
) -> Result<()> {
    use novarocks_execution_contract::failure::{FailureClass, ResourceDimension, ResourceScope};

    let after_freeze = super::completed_statement_freeze_bytes(context)?;
    let frozen_bytes = after_freeze.1 - before_freeze.1;
    write_record(
        &context.scenario_root().join("oversize-freeze.json"),
        &json!({
            "query_id": query_id, "before": before_freeze, "after": after_freeze,
            "frozen_bytes": frozen_bytes, "request_limit": OVERSIZE_REQUEST_LIMIT
        }),
    )?;
    let Err(StatementFailure::Trino(error)) = result else {
        bail!("oversize fragment did not return a terminal Trino error: {result:?}");
    };
    ensure!(
        error.error_code == 131072
            && error.error_name == "GENERIC_INSUFFICIENT_RESOURCES"
            && error.error_type == "INSUFFICIENT_RESOURCES"
            && error
                .message
                .starts_with("[query.execution.resource_exhausted] "),
        "oversize fragment changed typed Trino identity: {error:?}"
    );
    let deadline = context.deadline();
    let terminal = context
        .handle()
        .await_query_lifecycle_statement_observation(observation, false, deadline)?;
    write_record(
        &context.scenario_root().join("oversize-terminal.json"),
        &json!({"query_id": query_id, "terminal": format!("{terminal:#?}")}),
    )?;
    ensure!(
        after_freeze.0 == before_freeze.0 + 1 && frozen_bytes > OVERSIZE_REQUEST_LIMIT as i64,
        "the statement did not complete an oversized freeze: {before_freeze:?} -> {after_freeze:?}"
    );
    let failure = terminal
        .failure_fact
        .context("oversize attempt lacks its original failure")?;
    ensure!(
        failure.class() == FailureClass::Resource
            && matches!(
                terminal.failure_site,
                Some(QueryLifecycleFailureSite::Coordinator)
            )
            && failure.resource().is_some_and(|resource| {
                resource.dimension == ResourceDimension::Bytes
                    && resource.constraint_scope == Some(ResourceScope::Request)
                    && resource.limit == Some(OVERSIZE_REQUEST_LIMIT as u64)
                    && resource
                        .requested
                        .is_some_and(|bytes| bytes > OVERSIZE_REQUEST_LIMIT as u64)
            }),
        "oversize fragment was not refused at FE request-byte preflight: {terminal:?}"
    );
    let execution = terminal
        .execution_id
        .as_deref()
        .context("terminal attempt lacks identity")?;
    ensure!(
        terminal.attempt_id == 1
            && execution
                == format!(
                    "{}:{}:1",
                    terminal.process_namespace as i64, terminal.local_sequence
                )
            && terminal.worker_participations_converged,
        "oversize attempt was replaced or did not converge: {terminal:?}"
    );
    let tasks = task_markers::await_refused_attempt_tasks(context, selected, execution)?;
    write_record(
        &context.scenario_root().join("oversize-tasks.json"),
        &json!({"query_id": query_id, "execution_id": execution, "attempt": 1, "tasks": tasks}),
    )?;
    context.action(
        "oversized frozen fragment was refused before send and the original attempt exited",
    );
    Ok(())
}

fn write_record(path: &Path, body: &Value) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(body)?)
        .with_context(|| format!("write constant capacity evidence {}", path.display()))
}

fn is_baseline(context: &mut ScenarioContext) -> Result<bool> {
    let build = context
        .handle()
        .actual_build_information_evidence()?
        .context("constant capacity requires actual role build receipts")?;
    let roles = build
        .as_array()
        .context("role build receipt is not an array")?;
    ensure!(
        roles.len() == REQUIRED_BACKENDS + 1,
        "constant capacity requires one build receipt per role"
    );
    let old_roles = roles
        .iter()
        .filter(|role| role["build_info"]["source_commit"].as_str() == Some(BASELINE_SOURCE))
        .count();
    ensure!(
        old_roles == 0 || old_roles == roles.len(),
        "constant capacity may not mix baseline and target roles"
    );
    Ok(old_roles != 0)
}

fn record_config(context: &ScenarioContext, group: Group, baseline_build: bool) -> Result<()> {
    let config = context.effective_launch_config_evidence();
    let effective: Value = serde_json::from_slice(config.artifact_bytes())?;
    let roles = effective["semantics"]["roles"]
        .as_array()
        .context("effective Native role config is missing")?;
    let fe = roles
        .iter()
        .find(|entry| entry["role"] == "fe")
        .context("effective frontend config is missing")?;
    let effective = &fe["effective_config"]["value"];
    let query = &effective["standalone_server"]["value"]["trino_query_max_length"];
    let result = &effective["runtime"]["value"]["frontend_workload"]["value"]["client_result_all_objects_bytes"];
    let configured = |entry: &Value, field| -> Result<Option<usize>> {
        if entry.is_null() {
            Ok(None)
        } else {
            Ok(Some(usize::try_from(
                entry["value"]
                    .as_u64()
                    .with_context(|| format!("invalid {field}"))?,
            )?))
        }
    };
    let query = configured(query, "trino_query_max_length")?;
    let result = configured(result, "client_result_all_objects_bytes")?;
    let query_limit = query.unwrap_or(DEFAULT_QUERY_LENGTH);
    let result_limit = result.unwrap_or(DEFAULT_RESULT_BYTES);
    ensure!(
        query_limit == group.query_limit() && result_limit == DEFAULT_RESULT_BYTES,
        "unexpected effective query limit {query_limit} or Client window {result_limit}"
    );
    let request_limits =
        roles
            .iter()
            .map(|role| {
                let entry = &role["effective_config"]["value"]["runtime"]["value"]["native_ingress"]
                    ["value"]["ordinary_request_max_bytes"];
                let configured = configured(entry, "ordinary_request_max_bytes")?;
                let limit = configured.unwrap_or(DEFAULT_REQUEST_LIMIT);
                ensure!(
                    limit == group.request_limit(),
                    "unexpected Native request limit {limit} for {}",
                    role["role"]
                );
                Ok(json!({"role": role["role"], "configured": configured, "effective": limit}))
            })
            .collect::<Result<Vec<_>>>()?;
    ensure!(
        request_limits.len() == REQUIRED_BACKENDS + 1,
        "missing exact role configuration"
    );
    write_record(
        &context
            .scenario_root()
            .join("constant-capacity-config.json"),
        &json!({
            "comparison": if baseline_build { "p00-baseline" } else { "target" },
            "effective_launch_config_sha256": config.artifact_sha256(),
            "trino_query_max_length": {"configured": query, "effective": query_limit},
            "client_result_all_objects_bytes": {"configured": result, "effective": result_limit},
            "ordinary_request_max_bytes": request_limits
        }),
    )
}

fn capture_baseline_contract(
    context: &ScenarioContext,
    session: &mut SqlSession,
    sql: &str,
    case: Case,
) -> Result<()> {
    let path = context
        .scenario_root()
        .join(format!("{}.baseline-contract", case.label()));
    let output = match session.execute(format!(
        "EXPLAIN WITH SESSION explain_detail = 'CONTRACT' {sql}"
    )) {
        Ok(output) => output,
        Err(error) => {
            let recorded = write_record(
                &path.with_extension("baseline-contract-error.json"),
                &serde_json::to_value(&error)?,
            );
            return creation_merge(Err(error.into()), recorded);
        }
    };
    let rows = baseline_contract_rows(&output)?;
    write_record(
        &path.with_extension("baseline-contract-rows.json"),
        &json!({
            "query_id": output.query_id, "pages": output.pages,
            "columns": output.columns, "rows": rows
        }),
    )?;
    let plan = rows.join("\n");
    fs::write(&path, &plan)?;
    check_baseline_values(&plan, case)
}

fn baseline_contract_rows(output: &StatementResult) -> Result<Vec<&str>> {
    ensure!(
        output.is_result_set()
            && output.columns.len() == 1
            && matches!(
                output.columns[0].type_signature.raw_type.as_str(),
                "varchar" | "char"
            ),
        "baseline EXPLAIN CONTRACT must return one text column"
    );
    ensure!(
        !output.rows.is_empty(),
        "baseline EXPLAIN CONTRACT returned no plan rows"
    );
    output
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Text(line)] => Ok(line.as_str()),
            _ => bail!("baseline EXPLAIN CONTRACT returned a non-text plan row"),
        })
        .collect()
}

fn check_baseline_values(plan: &str, case: Case) -> Result<()> {
    ensure!(
        plan.starts_with("PHYSICAL PLAN version="),
        "baseline must return its original CONTRACT plan"
    );
    let (rows, columns) = case.dimensions();
    let mut lines = plan.lines();
    let header = lines
        .find(|line| {
            line.trim_start()
                .starts_with(&format!("VALUES stats={{rows={rows},"))
        })
        .context("baseline CONTRACT lost the selected Values cardinality")?;
    let indent = header.len() - header.trim_start().len();
    let values = lines
        .take_while(|line| line.len() - line.trim_start().len() > indent)
        .collect::<Vec<_>>();
    let layout = values
        .iter()
        .find_map(|line| line.trim_start().strip_prefix("rows=[["))
        .and_then(|line| line.strip_suffix("]]"))
        .context("baseline CONTRACT lost its original Values layout")?;
    ensure!(
        layout.split("], [").count() == rows
            && layout
                .split("], [")
                .all(|row| row.split(", ").count() == columns),
        "baseline CONTRACT did not preserve {rows} by {columns} Values"
    );
    match case {
        Case::Grouped => {
            let mut runs = Vec::<(u32, usize)>::new();
            for line in values {
                let Some((_, reference)) = line.split_once("constant-reference(pool=") else {
                    continue;
                };
                let (pool, ordinal) = reference
                    .split_once(", ordinal=")
                    .context("baseline constant pool reference lacks ordinal")?;
                let ordinal: usize = ordinal
                    .split_once(')')
                    .context("unterminated ordinal")?
                    .0
                    .parse()?;
                let pool: u32 = pool.parse()?;
                if let Some((last, len)) = runs.last_mut().filter(|(last, _)| *last == pool) {
                    ensure!(
                        *last == pool && *len == ordinal,
                        "baseline constant pool ordinals are not contiguous"
                    );
                    *len += 1;
                } else {
                    ensure!(
                        ordinal == 0 && !runs.iter().any(|(previous, _)| *previous == pool),
                        "baseline reused a constant pool outside its contiguous run"
                    );
                    runs.push((pool, 1));
                }
            }
            ensure!(
                runs.iter().map(|(_, len)| *len).eq([4096, 4096, 1]),
                "baseline static pools have unexpected layout: {runs:?}"
            );
        }
        Case::Fold => ensure!(
            values
                .iter()
                .any(|line| line.contains("builtin.scalar/rpad/")),
            "baseline CONTRACT did not retain rpad under Values"
        ),
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn baseline_contract_collects_original_rows_and_checks_values() {
        use novarocks_test_trino_client::{Column, TypeSignature};

        let lines = [
            "PHYSICAL PLAN version=baseline",
            "    VALUES stats={rows=1, cpu=0} [fragment=1, node=0]",
            "      rows=[[n0:r0]]",
            "      n0:e3 = builtin.scalar/rpad/v1#fixture",
        ];
        let mut result = StatementResult {
            columns: vec![Column {
                name: "Explain String".into(),
                type_name: "varchar".into(),
                type_signature: TypeSignature::simple("varchar"),
            }],
            rows: lines
                .iter()
                .map(|line| vec![TrinoValue::Text((*line).into())])
                .collect(),
            query_id: "baseline-explain".into(),
            pages: 2,
            update_type: None,
            update_count: None,
        };
        let observed = baseline_contract_rows(&result).unwrap();
        assert_eq!(observed, lines);
        check_baseline_values(&observed.join("\n"), Case::Fold).unwrap();
        result.rows[1][0] = TrinoValue::Null;
        assert!(baseline_contract_rows(&result).is_err());
    }

    #[test]
    fn generation_preserves_exact_query_sizes_and_default_overlay() {
        let smoke = Case::Smoke.sql();
        assert_eq!(smoke.encode_utf16().count(), 990_000);
        assert!(!smoke.contains(['\n', ';']));
        let grouped = Case::Grouped.sql();
        assert_eq!(grouped.matches("(0)").count(), 4097);
        assert_eq!(grouped.matches("(1)").count(), 4096);
        assert!(Case::Single.sql().len() > SINGLE_BYTES);
        let grid = Case::Grid.sql();
        assert!(grid.len() > 6 * GRID_CELL_BYTES && grid.len() < LARGE_QUERY_LENGTH);
        for scenario in scenarios() {
            let launch = scenario.launch_config(Path::new("unused")).unwrap();
            if scenario.name() == LARGE_STATIC {
                let config: toml::Value = launch.config_overlay.fe.unwrap().parse().unwrap();
                assert_eq!(
                    config["standalone_server"]["trino_query_max_length"].as_integer(),
                    Some(LARGE_QUERY_LENGTH as i64)
                );
                assert!(config.get("runtime").is_none());
                assert!(launch.config_overlay.be.is_none());
            } else if scenario.name() == OVERSIZE {
                let fe = launch.config_overlay.fe.unwrap();
                assert_eq!(launch.config_overlay.be.as_deref(), Some(fe.as_str()));
                let config: toml::Value = fe.parse().unwrap();
                assert_eq!(
                    config["runtime"]["native_ingress"]["ordinary_request_max_bytes"].as_integer(),
                    Some(OVERSIZE_REQUEST_LIMIT as i64)
                );
                assert!(config["runtime"].get("frontend_workload").is_none());
            } else {
                assert!(launch.config_overlay.fe.is_none());
                assert!(launch.config_overlay.be.is_none());
            }
        }
    }
}
