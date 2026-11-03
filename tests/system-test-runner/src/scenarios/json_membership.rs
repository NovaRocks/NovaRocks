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
//! Explicit native lifecycle coverage for JSON mark joins. The fixed integer
//! keys give an independent three-valued oracle; object key order differs on
//! the two inputs. The existing joined SQL actor owns cancellation and reentry.
use super::{
    connector::{create_catalog, create_warehouse},
    native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec},
    query_lifecycle::{await_resource_convergence, resource_snapshot},
    writer_statement_actor::{ReadSessionSettings, WriterActor, retain_write_result},
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const CATALOG: &str = "json_mark";
const DATABASE: &str = "membership";
const HOLD: &str = "join-build-before-publish-hold";
const PROBES: i64 = 3072;
const CAPACITY_BYTES: u64 = 128 * 1024 * 1024;
// The complete RHS has twice the target's declared budget, before metadata.
// The small mark control must succeed under exactly that same target budget.
const BIG_ROWS: i64 = 4 * 1024 * 1024;
const BIG_TEXT: i64 = 64;
const CAPACITY_SETTINGS: ReadSessionSettings = ReadSessionSettings { pipeline_dop: 1 };
#[derive(Clone, Copy)]
enum Case {
    FullEos,
    Cancel,
    Capacity,
}
struct JsonMembership {
    case: Case,
    intent: SqlIntentOwner,
}
pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    [Case::FullEos, Case::Cancel, Case::Capacity]
        .into_iter()
        .map(|case| {
            Box::new(JsonMembership {
                case,
                intent: SqlIntentOwner::default(),
            }) as Box<dyn Scenario>
        })
        .collect()
}
fn selected_sql(case: Case) -> String {
    let (hint, rhs) = match case {
        Case::Capacity => (
            format!(" /*+ SET_VAR(query_mem_limit={CAPACITY_BYTES}) */"),
            "big_rhs",
        ),
        _ => (String::new(), "rhs"),
    };
    format!(
        "SELECT{hint} p.id, parse_json(p.txt) IN (SELECT parse_json(txt) FROM {CATALOG}.{DATABASE}.{rhs}) AS present FROM {CATALOG}.{DATABASE}.probe p ORDER BY p.id"
    )
}
impl Scenario for JsonMembership {
    fn name(&self) -> &'static str {
        match self.case {
            Case::FullEos => "query-lifecycle/json-membership-full-eos",
            Case::Cancel => "query-lifecycle/json-membership-cancel",
            Case::Capacity => "query-lifecycle/json-membership-capacity",
        }
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        let mut declaration =
            super::declarations::for_module("query_lifecycle", self.name(), true, false);
        declaration.impact.paths.extend(
            [
                "novarocks/execution/**",
                "novarocks/sql/**",
                "novarocks/types/**",
                "novarocks/local-compiler/**",
                "novarocks/local-program/**",
                "novarocks/physical-plan/**",
                "novarocks/plan-codec/**",
                "novarocks/failpoint/**",
                "novarocks/functions/src/native_inlist*.rs",
                "novarocks/functions/src/json_canonical*.rs",
                "novarocks/functions/src/builtin/json_canonical*.rs",
                "tests/system-test-runner/src/scenarios/json_membership.rs",
                "tests/system-test-runner/src/scenarios/writer_statement_actor.rs",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        declaration.basis = Some(
            "Fresh native FE+3BE and private managed-runtime Hadoop Iceberg files. Fixed integer-key/object-order oracle; exact statement/Task build hold and scheduler WaitFact observations; original joined SQL actor, same-connection error reentry and resource convergence. Explicit relevant-change coverage; no external shared fixture.",
        );
        declaration
    }
    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let held = !matches!(self.case, Case::Capacity);
        let spec = StatementSpec {
            sql: selected_sql(self.case),
            faults: if held {
                (0..cluster_size).map(|index| (index, HOLD)).collect()
            } else {
                vec![]
            },
            observations: (0..cluster_size)
                .flat_map(|index| {
                    [
                        (index, "grpc-fragment", None),
                        (index, "exchange-snapshot", None),
                    ]
                })
                .collect(),
        };
        let plan = if held {
            self.intent
                .freeze_mark_join_build(vec![spec], 0, cluster_size, binary, deadline)?
        } else {
            self.intent
                .freeze(vec![spec], &[], cluster_size, binary, deadline)?
        };
        Ok(Some(plan))
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "JSON membership requires native 1FE+3BE"
        );
        let mut control = super::super::actors::mysql::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        let warehouse = create_warehouse(context, "json-mark")?;
        create_catalog(&mut control, CATALOG, &warehouse)?;
        control.query_drop(format!("CREATE DATABASE {CATALOG}.{DATABASE}"))?;
        control.query_drop(format!(
            "CREATE TABLE {CATALOG}.{DATABASE}.probe (id BIGINT, txt STRING)"
        ))?;
        control.query_drop(format!(
            "CREATE TABLE {CATALOG}.{DATABASE}.rhs (txt STRING)"
        ))?;
        for low in [1, 1025, 2049] {
            let high = low + 1023;
            control.query_drop(format!("INSERT INTO {CATALOG}.{DATABASE}.probe SELECT generate_series, CASE WHEN generate_series % 17=0 THEN NULL ELSE CONCAT('{{\"a\":',CAST(generate_series % 13 AS STRING),',\"b\":1}}') END FROM TABLE(generate_series({low},{high}))"))?;
        }
        for (low, high) in [(0, 4), (5, 8)] {
            control.query_drop(format!("INSERT INTO {CATALOG}.{DATABASE}.rhs SELECT CONCAT('{{\"b\":1,\"a\":',CAST(generate_series AS STRING),'}}') FROM TABLE(generate_series({low},{high}))"))?;
        }
        control.query_drop(format!(
            "INSERT INTO {CATALOG}.{DATABASE}.rhs VALUES(NULL),('{{\"b\":1,\"a\":0}}')"
        ))?;
        if matches!(self.case, Case::Capacity) {
            seed_capacity(context, &mut control)?;
        }
        let baseline = resource_snapshot(context)?;
        let selected = self.intent.take(0)?;
        let rows = Arc::new(Mutex::new(None));
        let actor = WriterActor::start_read(
            context,
            &selected.sql,
            selected.clone(),
            rows.clone(),
            matches!(self.case, Case::Capacity).then_some(CAPACITY_SETTINGS),
        )?;
        let mut ended = std::collections::BTreeSet::new();
        let primary = (|| -> Result<mysql::Result<()>> {
            let mut parked = None;
            if !matches!(self.case, Case::Capacity) {
                parked = Some(await_held_probe(context, &selected, &actor)?);
                if matches!(self.case, Case::Cancel) {
                    control.query_drop(format!("KILL QUERY {}", actor.connection_id))?;
                } else {
                    end_hold(context, &selected, &mut ended)?;
                }
            }
            let result = actor
                .done
                .recv_timeout(context.remaining("await JSON membership result")?)
                .context("JSON membership actor did not terminate")?;
            let deadline = context.deadline();
            let snapshot = context
                .handle()
                .await_query_lifecycle_statement_observation(
                    actor.observation(),
                    matches!(self.case, Case::FullEos),
                    deadline,
                )?;
            match self.case {
                Case::FullEos => {
                    ensure!(result.is_ok(), "JSON mark query failed: {result:?}");
                    let parked = parked
                        .as_ref()
                        .context("mark probe park was not retained")?;
                    ensure!(
                        parked.has_wake(&context.handle().be_log_contents(parked.backend)?),
                        "published build did not wake its original probe driver/block epoch"
                    );
                    let actual = rows
                        .lock()
                        .map_err(|_| anyhow::anyhow!("JSON rows owner poisoned"))?
                        .take()
                        .context("successful JSON query has no rows")?;
                    ensure!(
                        actual.len() == PROBES as usize,
                        "mark join changed outer row multiplicity"
                    );
                    for (ordinal, row) in actual.into_iter().enumerate() {
                        let (id, present): (i64, Option<i64>) = mysql::from_row_opt(row)?;
                        let expected = if id % 17 == 0 || id % 13 > 8 {
                            None
                        } else {
                            Some(1)
                        };
                        ensure!(
                            id == ordinal as i64 + 1 && present == expected,
                            "id={id} actual={present:?} expected={expected:?}"
                        );
                    }
                }
                Case::Cancel => ensure!(
                    matches!(&result,Err(mysql::Error::MySqlError(error)) if error.code==1317),
                    "KILL must interrupt original query: {result:?}"
                ),
                Case::Capacity => {
                    ensure!(
                        matches!(&result, Err(mysql::Error::MySqlError(error)) if error.code == 1041),
                        "capacity query must fail publicly: {result:?}"
                    );
                    let fact = snapshot
                        .failure_fact
                        .context("capacity query has no original failure fact")?;
                    context.action(format!(
                        "original capacity failure fact={fact:?} site={:?}",
                        snapshot.failure_site
                    ));
                    ensure!(
                        fact.class()
                            == novarocks_execution_contract::failure::FailureClass::Resource,
                        "capacity source has unexpected fact {fact:?}"
                    );
                    let Some(novarocks_cluster_harness::QueryLifecycleFailureSite::Task {
                        identity,
                        ..
                    }) = &snapshot.failure_site
                    else {
                        bail!("capacity refusal lacks its original Task");
                    };
                    ensure!(
                        fact.reason().domain()
                            == novarocks_execution_contract::failure::domain::MEMORY.get()
                            && fact.reason().code() == 8,
                        "capacity refusal must preserve the original tracker limit fact: {fact:?}"
                    );
                    let resource = fact
                        .resource()
                        .context("query tracker refusal lacks resource facts")?;
                    ensure!(
                        resource.limit == Some(CAPACITY_BYTES)
                            && resource
                                .committed
                                .is_some_and(|bytes| bytes > CAPACITY_BYTES)
                            && resource.requested.is_some_and(|bytes| bytes > 0),
                        "capacity refusal lacks the frozen query limit and original request: {resource:?}"
                    );
                    assert_retained_query_owner(context, *identity)?;
                    ensure!(
                        matches!(
                            snapshot.failure_site,
                            Some(novarocks_cluster_harness::QueryLifecycleFailureSite::Task {
                                phase: novarocks_execution_contract::TaskFailurePhase::Execution,
                                ..
                            })
                        ),
                        "refusal must originate in actual Task execution: {:?}",
                        snapshot.failure_site
                    );
                }
            }
            Ok(result)
        })();
        let cleanup = if matches!(self.case, Case::Capacity) {
            Ok(())
        } else {
            end_hold(context, &selected, &mut ended)
        };
        let outcome = actor.settle(context, retain_write_result(primary, cleanup))?;
        if matches!(self.case, Case::FullEos) {
            outcome?;
        }
        await_resource_convergence(context, &baseline)?;
        // Empty RHS still preserves every outer occurrence, including SQL NULL.
        let empty:Vec<i64>=control.query(format!("SELECT COUNT(*) FROM (SELECT id,parse_json(txt) IN (SELECT parse_json(txt) FROM {CATALOG}.{DATABASE}.rhs WHERE FALSE) AS m FROM {CATALOG}.{DATABASE}.probe) q WHERE m=FALSE"))?;
        ensure!(
            empty == vec![PROBES],
            "empty RHS must yield FALSE for every probe"
        );
        for table in ["probe", "rhs"] {
            control.query_drop(format!("DROP TABLE {CATALOG}.{DATABASE}.{table}"))?;
        }
        if matches!(self.case, Case::Capacity) {
            control.query_drop(format!("DROP TABLE {CATALOG}.{DATABASE}.big_rhs"))?;
        }
        control.query_drop(format!("DROP DATABASE {CATALOG}.{DATABASE}"))?;
        control.query_drop(format!("DROP CATALOG {CATALOG}"))?;
        control.finish()
    }
}
fn end_hold(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
    ended: &mut std::collections::BTreeSet<usize>,
) -> Result<()> {
    let mut outcome = Ok(());
    for &(index, kind) in &selected.faults {
        if ended.contains(&index) {
            continue;
        }
        let result =
            context
                .handle()
                .end_selected_query_lifecycle_fault(index, kind, &selected.token);
        if result.is_ok() {
            ended.insert(index);
        }
        outcome = super::writer_statement_actor::combine(outcome, result);
    }
    outcome
}
struct ProbePark {
    backend: usize,
    task: String,
    driver: u64,
    epoch: u64,
}
impl ProbePark {
    fn has_wake(&self, log: &str) -> bool {
        log.lines().any(|line| {
            line.contains("NOVAROCKS_WRITER_DIAGNOSTIC event=driver_wake")
                && line.contains(&self.task)
                && scalar(line, "driver_id").ok() == Some(self.driver)
                && scalar(line, "block_epoch").ok() == Some(self.epoch)
        })
    }
}
fn scalar(line: &str, name: &str) -> Result<u64> {
    field(line, name)?
        .parse()
        .with_context(|| format!("invalid original scheduler observation {name}"))
}
fn field<'a>(line: &'a str, name: &str) -> Result<&'a str> {
    line.split_once(&format!("{name}="))
        .and_then(|(_, value)| value.split_whitespace().next())
        .with_context(|| format!("original observation lacks {name}"))
}
fn assert_retained_query_owner(
    context: &mut ScenarioContext,
    failure: novarocks_execution_contract::TaskIdentity,
) -> Result<()> {
    // Different Tasks share this BE's one original query tracker. Its first
    // refusal may occur in a producer while another Task retains the build.
    let execution = format!("{}/", failure.query_execution_id().query_id());
    let process = format!("@{}", failure.backend_process_id());
    let mut live_build_observed = false;
    for index in 0..context.handle().be_count() {
        let log = context.handle().be_log_contents(index)?;
        for line in log.lines().filter(|line| {
            line.contains("NOVAROCKS_WRITER_DIAGNOSTIC event=join_build_keys_retained")
        }) {
            let task = field(line, "task")?;
            if !task.starts_with(&execution) || !task.ends_with(&process) {
                continue;
            }
            if scalar(line, "attempt")?
                != u64::from(failure.query_execution_id().attempt_id().get())
            {
                continue;
            }
            ensure!(
                field(line, "unique_query_limit")? == "true"
                    && scalar(line, "query_limit")? == CAPACITY_BYTES
                    && scalar(line, "input_slots")? == u64::from(CAPACITY_SETTINGS.pipeline_dop),
                "original runtime ancestry does not identify the query's unique constraint owner"
            );
            // This is an actual live key-page witness, not a byte estimate or
            // a share of the query budget. The failure keeps its original owner.
            if scalar(line, "retained_rows")? > 0 {
                live_build_observed = true;
                context.action(format!(
                    "original retained-key witness on BE[{index}]: {line}"
                ));
            }
        }
    }
    ensure!(
        live_build_observed,
        "failure BE lacks actual retained mark build keys under the original query owner"
    );
    Ok(())
}
fn await_held_probe(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
    actor: &WriterActor,
) -> Result<ProbePark> {
    loop {
        context.remaining("observe exact mark build and probe park")?;
        for &(index, kind) in &selected.faults {
            let Some(task) = context.handle().selected_query_lifecycle_fault_task(
                index,
                kind,
                &selected.token,
            )?
            else {
                continue;
            };
            let identity = novarocks_execution_contract::TaskIdentity::new(
                task.statement.execution()?,
                novarocks_types::StageId::new(task.stage_id)?,
                novarocks_types::TaskId::new(task.task_id)?,
                task.statement.process()?,
            );
            let task_field = format!("task={identity}");
            let log = context.handle().be_log_contents(index)?;
            for line in log.lines().filter(|line| {
                line.contains("NOVAROCKS_WRITER_DIAGNOSTIC event=driver_park")
                    && line.contains(&task_field)
                    && line.contains("mark_probe_dependency=true")
            }) {
                let parked = ProbePark {
                    backend: index,
                    task: task_field.clone(),
                    driver: scalar(line, "driver_id")?,
                    epoch: scalar(line, "block_epoch")?,
                };
                if parked.has_wake(&log) {
                    continue;
                }
                context.action(format!("observed original Task {identity}: build reached sole publication after complete build EOS and mark probe parked on actual Dependency WaitFact"));
                return Ok(parked);
            }
        }
        // An original SQL terminal wakes this wait immediately. A sampling
        // timeout only rechecks original facts; it never proves a phase.
        match actor.done.recv_timeout(
            context
                .remaining("observe mark probe dependency")?
                .min(Duration::from_millis(20)),
        ) {
            Ok(result) => {
                // Compilation can fail before an attempt exists. Its terminal
                // cannot authorize waiting for an attempt-only observation.
                bail!("held mark query became terminal before probe parked: {result:?}");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                bail!("held SQL actor exited before publishing its terminal")
            }
        }
    }
}
fn seed_capacity(context: &mut ScenarioContext, control: &mut mysql::Conn) -> Result<()> {
    control.query_drop(format!(
        "CREATE TABLE {CATALOG}.{DATABASE}.big_rhs (txt STRING)"
    ))?;
    // Keep each JSON small: PARSE_JSON's original worst-case RawValue resource
    // envelope grows quadratically in one value's length. Large individual
    // values exercise that producer's admission rather than retained RHS size.
    // Each independent INSERT has at most 2MiB of string body before metadata.
    let seed_rows = 2 * 1024 * 1024 / BIG_TEXT;
    for low in (1..=BIG_ROWS).step_by(seed_rows as usize) {
        let high = (low + seed_rows - 1).min(BIG_ROWS);
        control.query_drop(format!("INSERT INTO {CATALOG}.{DATABASE}.big_rhs SELECT CONCAT('{{\"key\":',CAST(generate_series AS STRING),',\"large\":\"',REPEAT('x',{BIG_TEXT}),'\"}}') FROM TABLE(generate_series({low},{high}))"))?;
    }
    context.action("bounded large RHS fixture written");
    CAPACITY_SETTINGS.apply(control)?;
    context.action("applied original pipeline_dop=1 session property; batch size remains default");
    // SUM consumes every original PARSE_JSON result; COUNT alone could prune
    // that producer. The private snapshot and frozen execution options match
    // the target. No build is retained in this independent producer control.
    let produced: Option<(i64, Option<i64>)> = control.query_first(format!(
        "SELECT /*+ SET_VAR(query_mem_limit={CAPACITY_BYTES}) */ COUNT(*), SUM(LENGTH(CAST(parse_json(txt) AS STRING))) FROM {CATALOG}.{DATABASE}.big_rhs"))?;
    ensure!(
        // Original renderer syntax has 22 bytes plus the decimal key digits.
        // Each source key is distinct, so the canonical set cannot collapse it.
        produced
            == Some((
                BIG_ROWS,
                Some(BIG_ROWS * (BIG_TEXT + 22) + decimal_digit_total(BIG_ROWS))
            )),
        "same-budget complete original JSON producer control failed: {produced:?}"
    );
    context.action(
        "same-budget complete JSON producer control passed count and parsed-content length",
    );
    context.action("begin same-budget small mark control");
    let small: Vec<(i64, Option<i64>)> = control.query(selected_sql(Case::FullEos).replacen(
        "SELECT ",
        &format!("SELECT /*+ SET_VAR(query_mem_limit={CAPACITY_BYTES}) */ "),
        1,
    ))?;
    ensure!(
        small.len() == PROBES as usize,
        "same-budget small RHS control failed"
    );
    for (ordinal, (id, present)) in small.into_iter().enumerate() {
        let expected = if id % 17 == 0 || id % 13 > 8 {
            None
        } else {
            Some(1)
        };
        ensure!(
            id == ordinal as i64 + 1 && present == expected,
            "same-budget small RHS id={id} actual={present:?} expected={expected:?}"
        );
    }
    context.action("complete large fixture read; small mark build passed the target budget");
    Ok(())
}

// Independent integer arithmetic for keys 1..=rows; does not parse query output.
fn decimal_digit_total(rows: i64) -> i64 {
    let mut total = 0;
    let mut low = 1;
    let mut digits = 1;
    while low <= rows {
        let high = (low * 10 - 1).min(rows);
        total += (high - low + 1) * digits;
        low *= 10;
        digits += 1;
    }
    total
}
