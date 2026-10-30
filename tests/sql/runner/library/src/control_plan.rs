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

//! Frozen runner intent, derived once from selected parsed metadata.
use crate::PreparedSqlInputs;
use crate::statement_controls::{self, MvDocumentDemand, PlannedSubmission};
use crate::types::*;
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::test_sessions::{
    ClusterTestSessionPlan, RoleLaunchReference, RoleSlot,
};
use novarocks_failpoint::session::DomainGrant;
use novarocks_parser::ast::{
    LiteralKind, MaterializedViewStatement, SessionStatement, SetTarget, SetValue, SetWord,
    Statement,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub(crate) struct StepControl {
    pub submissions: Vec<PlannedSubmission>,
    pub needs_nomination: bool,
    #[cfg(test)]
    pub native_faults: Vec<(usize, &'static str)>,
}
#[derive(Debug)]
pub(crate) struct SqlControlExecution {
    pub deadline: Instant,
    pub has_restarts: bool,
    pub steps: BTreeMap<(String, String, usize), StepControl>,
    pub step_role_launches: BTreeMap<(String, String, usize), Vec<(RoleSlot, RoleLaunchReference)>>,
    #[cfg(test)]
    planned_launches: Vec<PlannedLaunchInspection>,
}
#[cfg(test)]
#[derive(Debug)]
struct PlannedLaunchInspection {
    role: RoleSlot,
    generation: usize,
    reference: RoleLaunchReference,
    grants: Vec<DomainGrant>,
}
impl SqlControlExecution {
    pub fn step(&self, suite: &str, case: &str, query: usize) -> Option<&StepControl> {
        self.steps.get(&(suite.into(), case.into(), query))
    }
}
struct Launch {
    reference: RoleLaunchReference,
    grants: Vec<DomainGrant>,
}
fn add(launch: &mut Launch, domain: &str, parameters: Value) {
    launch.grants.push(DomainGrant {
        domain: domain.into(),
        grant_id: format!("{}-{}", domain, launch.grants.len()),
        parameters,
    });
}
fn millis(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis()).context("SQL session duration overflow")
}
fn sum(total: &mut u64, value: u64) -> Result<()> {
    *total = total
        .checked_add(value)
        .context("SQL session total deadline overflow")?;
    Ok(())
}
fn mul(value: u64, count: usize) -> Result<u64> {
    value
        .checked_mul(u64::try_from(count)?)
        .context("SQL session budget multiplication overflow")
}
fn step_restarts_fe(meta: &QueryMeta) -> bool {
    meta.restart_fe_after_step
        || meta.imv_accelerator_wipe_restart.is_some()
        || meta.kill_fe_after_mv_known_committed_before_projector_cas
        || meta.kill_fe_after_be_log_contains.is_some()
        || meta
            .publication_catalog_fault
            .is_some_and(|fault| fault.fault.requires_inflight_frontend_kill())
}
fn step_restarts_be(meta: &QueryMeta) -> Option<usize> {
    meta.restart_be_after_establish_context_index
}
/// Checked serial upper budget using the already finite query/retry/helper/startup
/// contracts. Each transport also consumes this same absolute deadline at runtime.
fn budget(
    inputs: &[PreparedSqlInputs],
    startup: Duration,
    roles: usize,
    session_sql: usize,
) -> Result<Duration> {
    ensure!(
        !startup.is_zero(),
        "SQL control startup timeout must be finite and nonzero"
    );
    let mut total = mul(
        millis(startup)?,
        roles.checked_add(1).context("role count overflow")?,
    )?;
    // Existing control supervisor loss/join window plus original role readiness/reap reserve.
    sum(&mut total, 30_000)?;
    for input in inputs {
        ensure!(
            input.query_timeout > 0,
            "controlled SQL cannot have an unbounded query timeout"
        );
        let query = input
            .query_timeout
            .checked_mul(1000)
            .context("SQL query timeout overflow")?;
        for hook in [&input.suite_init_hook, &input.suite_cleanup_hook]
            .into_iter()
            .flatten()
        {
            sum(
                &mut total,
                mul(
                    query,
                    crate::session::split_sql_statements(&hook.sql)?
                        .len()
                        .checked_mul(2)
                        .context("hook count overflow")?,
                )?,
            )?;
        }
        for case in &input.cases {
            // Base context + selected session statements + original create/drop database helpers.
            sum(
                &mut total,
                mul(
                    query,
                    session_sql
                        .checked_add(3)
                        .and_then(|v| v.checked_add(case.case_dbs.len().checked_mul(4)?))
                        .context("case setup count overflow")?,
                )?,
            )?;
            for step in &case.steps {
                let attempts = crate::results::step_retry_count(step);
                ensure!(attempts > 0, "SQL retry count must be finite and nonzero");
                let statements = if crate::shell::is_shell_step(&step.sql) {
                    1
                } else {
                    crate::session::split_sql_statements(&step.sql)?.len()
                };
                // USE + SET query_timeout precede each real submission attempt.
                let operations = statements
                    .checked_add(2)
                    .and_then(|v| v.checked_mul(2))
                    .and_then(|v| v.checked_mul(attempts))
                    .context("SQL operation count overflow")?;
                sum(&mut total, mul(query, operations)?)?;
                sum(&mut total, mul(300, attempts)?)?;
                sum(
                    &mut total,
                    mul(
                        millis(crate::results::step_retry_interval(step))?,
                        attempts.saturating_sub(1),
                    )?,
                )?;
                if let Some(delay) = step.meta.restart_be_delay_ms {
                    sum(&mut total, delay)?;
                }
                if let Some(delay) = step.meta.heartbeat_delay_ms {
                    sum(&mut total, delay)?;
                }
                let meta = &step.meta;
                for (selected, polls) in [
                    (meta.wait_alter_column.is_some(), 60),
                    (meta.wait_alter_rollup.is_some(), 120),
                    (meta.wait_alter_optimize.is_some(), 300),
                ] {
                    if selected {
                        sum(
                            &mut total,
                            mul(
                                query
                                    .checked_add(1000)
                                    .context("ALTER poll budget overflow")?,
                                polls,
                            )?,
                        )?;
                    }
                }
                let helpers = usize::from(meta.imv_stateless_rebuild.is_some()) * 3
                    + usize::from(meta.imv_equivalence_check.is_some()) * 3
                    + usize::from(
                        !meta.explain_contains.is_empty() || !meta.explain_not_contains.is_empty(),
                    );
                sum(&mut total, mul(query, helpers)?)?;
                if meta.imv_accelerator_wipe_restart.is_some() {
                    sum(&mut total, 60_000)?;
                }
                if meta.mv_resume_management.is_some() {
                    sum(&mut total, 120_000)?;
                }
                if crate::fault_injection::has_fault(meta) {
                    sum(&mut total, 30_000)?;
                }
                if step_restarts_fe(meta)
                    || step_restarts_be(meta).is_some()
                    || meta.restart_be_delay_ms.and(meta.kill_be_index).is_some()
                {
                    sum(&mut total, mul(millis(startup)?, 2)?)?;
                }
            }
        }
    }
    Ok(Duration::from_millis(total))
}
#[derive(Clone)]
struct SqlContext {
    catalog: Option<String>,
    namespace: Option<String>,
}
impl SqlContext {
    fn apply(&mut self, sql: &str) {
        let Ok(statements) = novarocks_parser::parse(sql) else {
            return;
        };
        for statement in statements {
            match statement {
                Statement::Session(SessionStatement::Use(value)) => {
                    if let Some(catalog) = value.catalog {
                        self.catalog = Some(catalog.value);
                    }
                    self.namespace = Some(value.database.value);
                }
                Statement::Session(SessionStatement::Set(value)) => {
                    for assignment in value.assignments {
                        if matches!(assignment.target, SetTarget::Catalog { .. }) {
                            self.catalog = match assignment.value {
                                SetValue::Words(words) if words.len() == 1 => match &words[0] {
                                    SetWord::Ident(value) => Some(value.value.clone()),
                                    SetWord::Literal(value) => match &value.kind {
                                        LiteralKind::String(value) => Some(value.clone()),
                                        _ => None,
                                    },
                                },
                                _ => None,
                            };
                        }
                    }
                }
                _ => {}
            }
        }
    }
    fn target(&self, name: &str, catalog: Option<&str>) -> Result<MvDocumentDemand> {
        let parts = name.split('.').collect::<Vec<_>>();
        let (catalog, namespace, mv) = match parts.as_slice() {
            [mv] => (
                catalog
                    .or(self.catalog.as_deref())
                    .context("controlled MV has no exact catalog")?,
                self.namespace
                    .as_deref()
                    .context("controlled MV has no exact namespace")?,
                *mv,
            ),
            [namespace, mv] => (
                catalog
                    .or(self.catalog.as_deref())
                    .context("controlled MV has no exact catalog")?,
                *namespace,
                *mv,
            ),
            [named_catalog, namespace, mv] => {
                ensure!(
                    catalog.is_none_or(|catalog| catalog == *named_catalog),
                    "MV catalog target is conflicting"
                );
                (*named_catalog, *namespace, *mv)
            }
            _ => bail!("controlled MV target is ambiguous"),
        };
        ensure!(
            [catalog, namespace, mv].iter().all(|part| !part.is_empty()),
            "controlled MV target is empty"
        );
        Ok(MvDocumentDemand {
            catalog: catalog.into(),
            namespace: namespace.into(),
            mv: mv.into(),
            level: String::new(),
        })
    }
    fn refresh_target(&self, sql: &str) -> Result<MvDocumentDemand> {
        let statements = novarocks_parser::parse(sql)
            .map_err(|error| anyhow::anyhow!("decode controlled REFRESH: {error}"))?;
        let mut target = None;
        for statement in statements {
            if let Statement::MaterializedView(MaterializedViewStatement::Refresh(refresh)) =
                statement
            {
                ensure!(
                    target.is_none(),
                    "controlled step contains more than one REFRESH target"
                );
                let name = refresh
                    .name
                    .parts
                    .iter()
                    .map(|part| part.value.as_str())
                    .collect::<Vec<_>>()
                    .join(".");
                target = Some(self.target(&name, None)?);
            }
        }
        target.context("known-committed control requires a literal REFRESH target")
    }
}
fn mv_parameters(action: &str, target: &MvDocumentDemand) -> Value {
    json!({"action":action,"catalog":target.catalog,"namespace":target.namespace,"mv":target.mv,"level":target.level})
}
fn marker_actions(meta: &QueryMeta) -> Vec<&'static str> {
    let mut actions = Vec::new();
    for marker in meta
        .be_log_contains
        .iter()
        .chain(&meta.be_log_not_contains)
        .chain(meta.be_log_count_at_least.iter().map(|(marker, _)| marker))
        .chain(
            meta.be_log_be_count_at_least
                .iter()
                .map(|(marker, _)| marker),
        )
        .chain(meta.kill_query_after_be_log_contains.iter())
        .chain(meta.kill_fe_after_be_log_contains.iter())
        .chain(
            meta.kill_be_after_be_log_contains
                .iter()
                .map(|fault| &fault.pattern),
        )
    {
        for (prefix, action) in [
            ("NOVAROCKS_CONNECTOR_UNIT_READER_", "reader-marker"),
            ("NOVAROCKS_CONNECTOR_PAGE_SOURCE_", "reader-marker"),
            ("NOVAROCKS_READER_", "reader-marker"),
            ("NOVAROCKS_CONNECTOR_WRITER_", "writer-marker"),
            ("NOVAROCKS_WRITER_", "writer-marker"),
            ("NOVAROCKS_GRPC_", "grpc-fragment"),
            ("NOVAROCKS_TASK_CONTEXT_", "grpc-fragment"),
            ("NOVAROCKS_TASK_CREATE_", "grpc-fragment"),
            ("NOVAROCKS_TASK_PREPARED_", "grpc-fragment"),
            ("NOVAROCKS_TASK_LEASE_RENEWED", "grpc-fragment"),
            ("NOVAROCKS_TASK_SPLIT_", "reader-marker"),
            ("NOVAROCKS_TASK_ABORT_", "grpc-fragment"),
            ("NOVAROCKS_TASK_TERMINATION_", "grpc-fragment"),
            (
                "NOVAROCKS_CATALOG_MATERIALIZATION",
                "catalog-materialization",
            ),
            ("NOVAROCKS_CATALOG_INSTALL_", "catalog-lifecycle"),
            ("NOVAROCKS_CATALOG_LOADING", "catalog-lifecycle"),
            ("NOVAROCKS_CATALOG_LIFECYCLE", "catalog-lifecycle"),
            ("NOVAROCKS_EXCHANGE_", "exchange-snapshot"),
        ] {
            if marker.starts_with(prefix) && !actions.contains(&action) {
                actions.push(action);
            }
        }
    }
    actions
}

fn observation_attempt_slots(meta: &QueryMeta) -> Result<Vec<Option<u64>>> {
    let Some(attempts) = &meta.native_observation_attempts else {
        return Ok(vec![None]);
    };
    ensure!(
        !attempts.is_empty(),
        "native_observation_attempts must not be empty"
    );
    let mut seen = BTreeSet::new();
    for &attempt in attempts {
        ensure!(
            attempt > 0 && seen.insert(attempt),
            "native_observation_attempts ordinals must be positive and unique"
        );
    }
    Ok(attempts.iter().copied().map(Some).collect())
}

fn native_observation_grants(
    action: &str,
    root: &Path,
    backend_index: usize,
    run_token: &str,
    target: &RoleLaunchReference,
    attempt: Option<u64>,
) -> (Value, Value) {
    let mut bind = json!({"action":format!("bind-{action}"),"root":root,"backend_index":backend_index,"statement_run_token":run_token,"target_session_id":target.session_id,"target_process_receipt":target.process_receipt});
    let mut observe = json!({"action":action,"root":root,"backend_index":backend_index,"statement_run_token":run_token,"target_session_id":target.session_id,"max_events":16384});
    if let Some(attempt) = attempt {
        bind["attempt_id"] = json!(attempt);
        observe["attempt_id"] = json!(attempt);
    }
    (bind, observe)
}

pub(crate) fn required(inputs: &[PreparedSqlInputs]) -> bool {
    inputs
        .iter()
        .flat_map(|input| &input.cases)
        .flat_map(|case| &case.steps)
        .any(|step| {
            step.meta
                .kill_fe_after_mv_known_committed_before_projector_cas
                || step.meta.restart_be_after_establish_context_index.is_some()
                || step.meta.query_lifecycle_fault.is_some()
                || !step.meta.query_lifecycle_faults.is_empty()
                || step.meta.query_lifecycle_structured_assertion.is_some()
                || step.meta.native_observation_attempts.is_some()
                || step.meta.task_update_replay_barrier == Some(true)
                || step.meta.imv_stateless_rebuild.is_some()
                || step.meta.imv_accelerator_wipe_restart.is_some()
                || step
                    .meta
                    .tags
                    .iter()
                    .any(|tag| tag == crate::parser::MV_DOCUMENT_OBSERVATION_TAG)
                || !marker_actions(&step.meta).is_empty()
        })
}

pub(crate) fn build(
    inputs: &[PreparedSqlInputs],
    cluster_size: usize,
    startup: Duration,
    binary: &Path,
    auto_case_db: &BTreeMap<String, bool>,
    session_sql: &[String],
    absolute_limit: Option<Instant>,
) -> Result<Option<(ClusterTestSessionPlan, SqlControlExecution)>> {
    let needs = required(inputs);
    if !needs {
        return Ok(None);
    }
    let deadline = Instant::now()
        .checked_add(budget(inputs, startup, cluster_size, session_sql.len())?)
        .context("SQL original absolute deadline exceeds the monotonic clock")?;
    let deadline = absolute_limit.map_or(deadline, |limit| limit.min(deadline));
    ensure!(
        Instant::now() < deadline,
        "original SQL fixture work deadline expired"
    );
    let mut plan = ClusterTestSessionPlan::new(deadline)?;
    let transport = plan.reserve_transport("query-lifecycle-faults")?;
    let mut refs = BTreeMap::<(RoleSlot, usize), Launch>::new();
    let mut generation = BTreeMap::<RoleSlot, usize>::new();
    for role in std::iter::once(RoleSlot::Frontend).chain((0..cluster_size).map(RoleSlot::Backend))
    {
        refs.insert(
            (role, 0),
            Launch {
                reference: plan.reserve_launch(role)?,
                grants: vec![],
            },
        );
        generation.insert(role, 0);
    }
    let mut execution = SqlControlExecution {
        deadline,
        has_restarts: inputs
            .iter()
            .flat_map(|input| &input.cases)
            .flat_map(|case| &case.steps)
            .any(|step| {
                step_restarts_fe(&step.meta)
                    || step_restarts_be(&step.meta).is_some()
                    || step
                        .meta
                        .restart_be_delay_ms
                        .and(step.meta.kill_be_index)
                        .is_some()
            }),
        steps: BTreeMap::new(),
        step_role_launches: BTreeMap::new(),
        #[cfg(test)]
        planned_launches: Vec::new(),
    };
    let mut sequence = 0usize;
    for input in inputs {
        // Match run_suite's existing order: parallel cases finish before the
        // serial restart group. No consumer is moved across a cluster boundary.
        for case in input
            .cases
            .iter()
            .filter(|case| !case.sequential)
            .chain(input.cases.iter().filter(|case| case.sequential))
        {
            let mut context = SqlContext {
                catalog: input.suite_catalog_override.clone(),
                namespace: case
                    .case_dbs
                    .first()
                    .cloned()
                    .or_else(|| {
                        auto_case_db
                            .get(&input.suite_name)
                            .copied()
                            .unwrap_or(false)
                            .then(|| case.auto_db.clone())
                    })
                    .or_else(|| {
                        (!input.target_db_default.is_empty())
                            .then(|| input.target_db_default.clone())
                    }),
            };
            for sql in session_sql {
                context.apply(sql);
            }
            for step in &case.steps {
                let meta = &step.meta;
                // This external restart occurs in apply_pre_query, before this
                // step's nomination. Its observation must target the successor.
                if let Some(index) = meta.restart_be_delay_ms.and(meta.kill_be_index) {
                    ensure!(
                        index < cluster_size,
                        "pre-query BE restart label outside actual topology"
                    );
                    let role = RoleSlot::Backend(index);
                    let next = generation.get_mut(&role).unwrap();
                    *next = next
                        .checked_add(1)
                        .context("pre-query role generation overflow")?;
                    let reference = plan.reserve_launch(role)?;
                    execution
                        .step_role_launches
                        .entry((
                            input.suite_name.clone(),
                            case.case_id.clone(),
                            step.query_number,
                        ))
                        .or_default()
                        .push((role, reference.clone()));
                    refs.insert(
                        (role, *next),
                        Launch {
                            reference,
                            grants: vec![],
                        },
                    );
                }
                let fe_key = (RoleSlot::Frontend, generation[&RoleSlot::Frontend]);
                let mut step_context = context.clone();
                if let Some(db) = &meta.db {
                    match db.split_once('.') {
                        Some((catalog, namespace)) => {
                            step_context.catalog = Some(catalog.into());
                            step_context.namespace = Some(namespace.into());
                        }
                        None => step_context.namespace = Some(db.clone()),
                    }
                }
                step_context.apply(&step.sql);
                let tagged = statement_controls::tagged_mv_document_demands(&meta.tags, &step.sql)?;
                let observation_attempts = crate::results::step_retry_count(step)
                    .checked_mul(2)
                    .context("MV observation retry allocation overflow")?;
                let mut documents = Vec::new();
                ensure!(
                    tagged
                        .len()
                        .checked_mul(observation_attempts)
                        .context("MV observation count overflow")?
                        <= 64,
                    "tagged MV observation retries exceed the finite role grant bound"
                );
                for _ in 0..observation_attempts {
                    documents.extend(tagged.iter().cloned());
                }
                if let Some(directive) = &meta.imv_stateless_rebuild {
                    let mut target = step_context.target(
                        &directive.mv,
                        Some(directive.catalog.as_deref().unwrap_or("ice")),
                    )?;
                    target.level = directive.level.as_sql().into();
                    documents.extend([target.clone(), target]);
                }
                for target in documents {
                    add(
                        refs.get_mut(&fe_key).unwrap(),
                        "mv",
                        mv_parameters("document-observation", &target),
                    );
                }
                if meta.imv_accelerator_wipe_restart.is_some() {
                    add(
                        refs.get_mut(&fe_key).unwrap(),
                        "mv",
                        json!({"action":"accelerator-wipe","family":"novarocks/frontend/mv/accelerator/v3"}),
                    );
                }
                if meta.kill_fe_after_mv_known_committed_before_projector_cas {
                    let target = step_context.refresh_target(&step.sql)?;
                    add(
                        refs.get_mut(&fe_key).unwrap(),
                        "mv",
                        json!({"action":"known-committed-hold","catalog":target.catalog,"namespace":target.namespace,"mv":target.mv,"directory":transport}),
                    );
                }
                let mut faults = crate::fault_injection::configured_query_lifecycle_faults(meta)
                    .into_iter()
                    .map(|fault| (fault.be_index, fault.kind.as_str()))
                    .collect::<Vec<_>>();
                if let Some(index) = meta.restart_be_after_establish_context_index {
                    faults.push((index, "restart-after-establish-context"));
                }
                ensure!(
                    meta.terminal_snapshot_conflict_be_index.is_none(),
                    "retired terminal snapshot fault has no current product owner"
                );
                ensure!(
                    meta.task_update_replay_barrier != Some(true)
                        || (faults.len() == 1 && faults[0].1 == "task-update-terminal-ack-drop"),
                    "task_update_replay_barrier requires exactly one task-update-terminal-ack-drop fault"
                );
                let observations = marker_actions(meta);
                let observation_attempts = observation_attempt_slots(meta)?;
                ensure!(
                    meta.native_observation_attempts.is_none() || !observations.is_empty(),
                    "native_observation_attempts requires a selected Native observation marker"
                );
                let needs_nomination = !faults.is_empty()
                    || !observations.is_empty()
                    || meta.query_lifecycle_structured_assertion.is_some();
                if needs_nomination {
                    ensure!(
                        !crate::shell::is_shell_step(&step.sql),
                        "internal controls cannot target a shell step"
                    );
                    let submissions = statement_controls::plan_submissions(
                        &format!("sql-{sequence}"),
                        &step.sql,
                        crate::results::step_retry_count(step),
                    )?;
                    ensure!(
                        faults.is_empty()
                            || crate::session::split_sql_statements(&step.sql)?.len() == 1,
                        "selected Native fault requires one exact submitted SQL statement"
                    );
                    let fe = refs.get_mut(&fe_key).unwrap();
                    if !fe
                        .grants
                        .iter()
                        .any(|grant| grant.domain == "statement-observation")
                    {
                        let secret = plan.fresh_domain_secret()?;
                        add(
                            fe,
                            "statement-observation",
                            json!({"action":"observe","bearer_secret":secret}),
                        );
                    }
                    for (index, kind) in &faults {
                        ensure!(
                            *index < cluster_size,
                            "selected native BE label exceeds the actual topology"
                        );
                        let parsed = novarocks_failpoint::QueryLifecycleFaultKind::parse(kind)
                            .context("unsupported selected Native fault")?;
                        ensure!(!matches!(parsed,novarocks_failpoint::QueryLifecycleFaultKind::TerminalAckDrop|novarocks_failpoint::QueryLifecycleFaultKind::TerminalSnapshotConflict),"retired Native fault has no current owner");
                        let be_key = (
                            RoleSlot::Backend(*index),
                            generation[&RoleSlot::Backend(*index)],
                        );
                        let target = refs[&be_key].reference.clone();
                        let token = &submissions[0].run_token;
                        let mut bind = json!({"action":format!("bind-{kind}"),"root":transport,"backend_index":index,"statement_run_token":token,"target_session_id":target.session_id,"target_process_receipt":target.process_receipt});
                        let mut apply = json!({"action":kind,"root":transport,"backend_index":index,"statement_run_token":token,"target_session_id":target.session_id});
                        if meta.task_update_replay_barrier == Some(true) {
                            bind["hold_split_consumption_until_replay"] = json!(true);
                            apply["hold_split_consumption_until_replay"] = json!(true);
                        }
                        add(refs.get_mut(&fe_key).unwrap(), "native", bind);
                        add(refs.get_mut(&be_key).unwrap(), "native", apply);
                    }
                    for submission in &submissions {
                        for action in &observations {
                            for index in 0..cluster_size {
                                let be_key = (
                                    RoleSlot::Backend(index),
                                    generation[&RoleSlot::Backend(index)],
                                );
                                let target = refs[&be_key].reference.clone();
                                for &attempt in &observation_attempts {
                                    let (bind, observe) = native_observation_grants(
                                        action,
                                        &transport,
                                        index,
                                        &submission.run_token,
                                        &target,
                                        attempt,
                                    );
                                    add(refs.get_mut(&fe_key).unwrap(), "native-observation", bind);
                                    add(
                                        refs.get_mut(&be_key).unwrap(),
                                        "native-observation",
                                        observe,
                                    );
                                }
                            }
                        }
                    }
                    execution.steps.insert(
                        (
                            input.suite_name.clone(),
                            case.case_id.clone(),
                            step.query_number,
                        ),
                        StepControl {
                            submissions,
                            needs_nomination,
                            #[cfg(test)]
                            native_faults: faults,
                        },
                    );
                }
                context = step_context;
                for role in std::iter::once(RoleSlot::Frontend)
                    .filter(|_| step_restarts_fe(meta))
                    .chain(step_restarts_be(meta).map(RoleSlot::Backend))
                {
                    ensure!(
                        !matches!(role,RoleSlot::Backend(index) if index>=cluster_size),
                        "restart BE label outside actual topology"
                    );
                    let next = generation.get_mut(&role).context("unknown restart role")?;
                    *next = next.checked_add(1).context("role generation overflow")?;
                    let reference = plan.reserve_launch(role)?;
                    execution
                        .step_role_launches
                        .entry((
                            input.suite_name.clone(),
                            case.case_id.clone(),
                            step.query_number,
                        ))
                        .or_default()
                        .push((role, reference.clone()));
                    refs.insert(
                        (role, *next),
                        Launch {
                            reference,
                            grants: vec![],
                        },
                    );
                }
                sequence = sequence
                    .checked_add(1)
                    .context("SQL target sequence overflow")?;
            }
        }
    }
    for ((role, generation), launch) in refs {
        ensure!(
            launch.grants.len() <= novarocks_failpoint::session::MAX_GRANTS,
            "selected {role:?} launch {generation} exceeds the finite role grant bound: {} > {}",
            launch.grants.len(),
            novarocks_failpoint::session::MAX_GRANTS
        );
        #[cfg(test)]
        execution.planned_launches.push(PlannedLaunchInspection {
            role,
            generation,
            reference: launch.reference.clone(),
            grants: launch.grants.clone(),
        });
        if launch.grants.is_empty() {
            plan.authorize_ordinary_launch(&launch.reference, binary.to_path_buf())?;
        } else {
            let mut owners = vec![("novarocks-server".into(), "test-controls".into())];
            owners.push((
                match role {
                    RoleSlot::Frontend => "novarocks-frontend-application",
                    RoleSlot::Backend(_) => "novarocks-native-adapter",
                }
                .into(),
                "test-controls".into(),
            ));
            plan.set_launch_binary(&launch.reference, binary.to_path_buf())?;
            plan.authorize_launch(&launch.reference, launch.grants, owners)?;
        }
    }
    plan.validate_selected_intents(cluster_size)?;
    Ok(Some((plan, execution)))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn inputs(steps: Vec<SqlStep>) -> Vec<PreparedSqlInputs> {
        vec![PreparedSqlInputs {
            suite_name: "selected".into(),
            sql_dir: PathBuf::from("/unused"),
            result_dir: None,
            sql_glob: String::new(),
            suite_catalog_override: Some("ice".into()),
            target_db_default: "db".into(),
            ref_db_default: "db".into(),
            verify_enabled: true,
            query_timeout: 30,
            cases: vec![SqlCase {
                expected_variables: Default::default(),
                namespace_rewrites: Default::default(),
                auto_db: String::new(),
                source_file: PathBuf::from("/selected.sql"),
                case_id: "one".into(),
                steps,
                case_dbs: vec![],
                sequential: true,
            }],
            load_failures: vec![],
            suite_init_hook: None,
            suite_cleanup_hook: None,
        }]
    }
    fn step(number: usize, sql: &str, meta: QueryMeta) -> SqlStep {
        SqlStep {
            query_number: number,
            sql: sql.into(),
            meta,
        }
    }
    fn plan(
        inputs: &[PreparedSqlInputs],
        file: &Path,
    ) -> Result<(ClusterTestSessionPlan, SqlControlExecution)> {
        build(
            inputs,
            3,
            Duration::from_secs(5),
            file,
            &BTreeMap::new(),
            &[],
            None,
        )?
        .context("expected selected controls")
    }
    use std::path::PathBuf;
    #[test]
    fn document_tag_never_grants_wipe_and_reserves_distinct_retry_observations() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let input = inputs(vec![step(
            1,
            "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'db.mv', level => 'package')",
            QueryMeta {
                tags: vec![crate::parser::MV_DOCUMENT_OBSERVATION_TAG.into()],
                retry_count: Some(2),
                ..Default::default()
            },
        )]);
        let (plan, execution) = plan(&input, file.path()).unwrap();
        let summaries = plan.planned_launch_summaries();
        let grants = summaries
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|launch| launch["grants"].as_array().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(grants.len(), 4);
        assert!(
            grants
                .iter()
                .all(|grant| grant["domain"] == "mv" && grant["action"] == "document-observation")
        );
        assert!(
            execution.steps.is_empty(),
            "MV document intent does not imply Native fault nomination"
        );
    }
    #[test]
    fn whole_family_wipe_has_one_old_fe_claim_and_a_normal_successor() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let input = inputs(vec![step(
            1,
            "SELECT 1",
            QueryMeta {
                imv_accelerator_wipe_restart: Some(ImvStatelessDirective {
                    mv: "db.mv".into(),
                    level: ImvStatelessLevel::Package,
                    catalog: None,
                }),
                ..Default::default()
            },
        )]);
        let (plan, execution) = plan(&input, file.path()).unwrap();
        assert!(execution.has_restarts);
        let summaries = plan.planned_launch_summaries();
        let fes = summaries
            .as_array()
            .unwrap()
            .iter()
            .filter(|launch| launch["role"] == "fe")
            .collect::<Vec<_>>();
        assert_eq!(fes.len(), 2);
        assert_eq!(fes[0]["ordinary"], false);
        assert_eq!(fes[1]["ordinary"], true);
        assert_eq!(fes[0]["grants"][0]["action"], "accelerator-wipe");
        assert_eq!(fes[1]["grants"].as_array().unwrap().len(), 0);
        assert_ne!(fes[0]["session_id"], fes[1]["session_id"]);
    }
    #[test]
    fn replay_barrier_refines_only_the_selected_ack_drop_on_both_real_roles() {
        let file = tempfile::NamedTempFile::new().unwrap();
        for enabled in [false, true] {
            let input =
                inputs(vec![step(1, "SELECT 45", QueryMeta {
                query_lifecycle_fault: Some(crate::types::QueryLifecycleFaultDirective {
                    kind: novarocks_failpoint::QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
                    be_index: 0,
                }),
                task_update_replay_barrier: enabled.then_some(true),
                ..Default::default()
            })]);
            let (plan, _) = plan(&input, file.path()).unwrap();
            let summaries = plan.planned_launch_summaries();
            let selected = summaries
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|launch| launch["grants"].as_array().unwrap())
                .filter(|grant| {
                    matches!(
                        grant["action"].as_str(),
                        Some(
                            "task-update-terminal-ack-drop" | "bind-task-update-terminal-ack-drop"
                        )
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(selected.len(), 2);
            for grant in selected {
                assert_eq!(
                    grant.get("hold_split_consumption_until_replay"),
                    enabled.then_some(&serde_json::Value::Bool(true))
                );
            }
        }
        let orphan = inputs(vec![step(
            1,
            "SELECT 45",
            QueryMeta {
                task_update_replay_barrier: Some(true),
                ..Default::default()
            },
        )]);
        assert!(
            plan(&orphan, file.path())
                .unwrap_err()
                .to_string()
                .contains("requires exactly one")
        );
    }

    #[test]
    fn split_assignment_metadata_authorizes_the_actual_reader_owner() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let input = inputs(vec![step(
            1,
            "SELECT 1",
            QueryMeta {
                be_log_contains: vec![
                    "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED".into(),
                    "NOVAROCKS_TASK_SPLIT_NO_MORE".into(),
                ],
                ..Default::default()
            },
        )]);
        let (plan, execution) = plan(&input, file.path()).unwrap();
        // MysqlSession permits the initial submission and one transient
        // reconnect. Both need separate observation permission, while these
        // two split markers still share one reader action per submission.
        let submissions = &execution.step("selected", "one", 1).unwrap().submissions;
        assert_eq!(submissions.len(), 2);
        assert_eq!(submissions[0].transient_attempt, 0);
        assert_eq!(submissions[1].transient_attempt, 1);
        assert_ne!(submissions[0].run_token, submissions[1].run_token);
        for submission in submissions {
            submission.validate_sql("SELECT 1").unwrap();
        }
        let summaries = plan.planned_launch_summaries();
        for launch in summaries.as_array().unwrap() {
            let grants = launch["grants"].as_array().unwrap();
            let native = grants
                .iter()
                .filter(|grant| grant["domain"] == "native-observation")
                .collect::<Vec<_>>();
            if launch["role"] == "fe" {
                assert_eq!(native.len(), 6);
                assert!(
                    native
                        .iter()
                        .all(|grant| grant["action"] == "bind-reader-marker")
                );
            } else {
                assert_eq!(native.len(), 2);
                assert!(
                    native
                        .iter()
                        .all(|grant| grant["action"] == "reader-marker")
                );
            }
        }
    }
    #[test]
    fn pre_query_be_restart_observations_target_the_fresh_successor_slot() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let input = inputs(vec![step(
            1,
            "SELECT 1",
            QueryMeta {
                kill_be_index: Some(0),
                restart_be_delay_ms: Some(0),
                be_log_not_contains: vec!["NOVAROCKS_CONNECTOR_WRITER_OPENED".into()],
                ..Default::default()
            },
        )]);
        let (plan, _) = plan(&input, file.path()).unwrap();
        let summaries = plan.planned_launch_summaries();
        let bes = summaries
            .as_array()
            .unwrap()
            .iter()
            .filter(|launch| launch["role"] == "be-0")
            .collect::<Vec<_>>();
        assert_eq!(bes.len(), 2);
        assert_eq!(bes[0]["ordinary"], true);
        assert_eq!(bes[1]["ordinary"], false);
        assert!(bes[0]["grants"].as_array().unwrap().is_empty());
        assert!(
            bes[1]["grants"]
                .as_array()
                .unwrap()
                .iter()
                .all(|grant| grant["action"] == "writer-marker")
        );
    }
    #[test]
    fn unrelated_sql_text_and_unknown_tags_cannot_implicitly_request_internal_actions() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let input = inputs(vec![step(
            1,
            "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'db.mv', level => 'wipe')",
            Default::default(),
        )]);
        assert!(!required(&input));
        assert!(
            build(
                &input,
                3,
                Duration::from_secs(5),
                file.path(),
                &BTreeMap::new(),
                &[],
                None,
            )
            .unwrap()
            .is_none()
        );
        let tagged = inputs(vec![step(
            1,
            "SELECT 1",
            QueryMeta {
                tags: vec!["test-capability:unknown".into()],
                ..Default::default()
            },
        )]);
        assert!(
            crate::parser::validate_test_capability_tags(&tagged[0].cases[0].steps[0].meta.tags)
                .is_err()
        );
    }
    #[test]
    fn original_budget_overflow_and_finite_role_grant_overflow_fail_before_spawn() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut input = inputs(vec![step(
            1,
            "SELECT 1",
            QueryMeta {
                query_lifecycle_structured_assertion: Some(QueryLifecycleStructuredAssertion {
                    runtime_filter_availability: None,
                    runtime_filter_details: vec![],
                    runtime_filter_totals_at_least: vec![],
                }),
                ..Default::default()
            },
        )]);
        input[0].query_timeout = u64::MAX;
        assert!(
            plan(&input, file.path())
                .unwrap_err()
                .to_string()
                .contains("overflow")
        );
        let input = inputs(
            // Each selected statement contributes two submission attempts on
            // three BEs. Exceed the finite bound without fixing it at 64.
            (0..(novarocks_failpoint::session::MAX_GRANTS / (2 * 3) + 1))
                .map(|number| {
                    step(
                        number,
                        "SELECT 1",
                        QueryMeta {
                            be_log_not_contains: vec!["NOVAROCKS_CONNECTOR_WRITER_OPENED".into()],
                            ..Default::default()
                        },
                    )
                })
                .collect(),
        );
        assert!(
            plan(&input, file.path())
                .unwrap_err()
                .to_string()
                .contains("finite role grant bound")
        );
    }
    #[test]
    fn distributed_resilience_preserves_real_launches_and_exact_submission_authority() {
        let sql_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../correctness/distributed-resilience/sql");
        let meta_re = regex::Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap();
        let marker_re = regex::Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").unwrap();
        let files = crate::config::list_sql_files(&sql_dir, "*.sql").unwrap();
        assert!(!files.is_empty());
        let cases = files
            .iter()
            .map(|path| {
                crate::parser::load_sql_case_from_file(
                    path,
                    &meta_re,
                    &marker_re,
                    &crate::fixture_context::test_variables(
                        &files
                            .iter()
                            .map(|p| p.file_stem().unwrap().to_str().unwrap())
                            .collect::<Vec<_>>(),
                    ),
                )
                .unwrap()
                .expect("registered recovery SQL has executable steps")
            })
            .collect::<Vec<_>>();
        let mut input = inputs(Vec::new());
        input[0].suite_name = "distributed-resilience".into();
        input[0].sql_dir = sql_dir;
        input[0].cases = cases;
        let file = tempfile::NamedTempFile::new().unwrap();
        let (plan, execution) = plan(&input, file.path()).unwrap();
        // This includes the independently bounded serialized declarations,
        // not just the number of grants visible in the public summaries.
        plan.validate_selected_intents(3).unwrap();
        let launches = &execution.planned_launches;
        let peak = launches
            .iter()
            .map(|launch| launch.grants.len())
            .max()
            .unwrap();
        assert!(
            peak > 64,
            "the whole suite exercises the former capacity limit"
        );
        assert!(peak <= novarocks_failpoint::session::MAX_GRANTS);

        let mut tokens = BTreeSet::new();
        for ((suite, case_id, query), control) in &execution.steps {
            assert_eq!(suite, "distributed-resilience");
            let source = input[0]
                .cases
                .iter()
                .find(|case| &case.case_id == case_id)
                .unwrap();
            let source = source
                .steps
                .iter()
                .find(|step| &step.query_number == query)
                .unwrap();
            let statements = crate::session::split_sql_statements(&source.sql).unwrap();
            let expected = (0..crate::results::step_retry_count(source))
                .flat_map(|outer| {
                    (0..2).flat_map({
                        let count = statements.len();
                        move |transient| {
                            (0..count).map(move |statement| (outer, transient, statement))
                        }
                    })
                })
                .collect::<BTreeSet<_>>();
            let actual = control
                .submissions
                .iter()
                .map(|target| {
                    assert!(
                        tokens.insert(target.run_token.clone()),
                        "submission token reused"
                    );
                    target
                        .validate_sql(&statements[target.statement_index])
                        .unwrap();
                    (
                        target.outer_attempt,
                        target.transient_attempt,
                        target.statement_index,
                    )
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(actual, expected);
            assert_eq!(control.submissions.len(), actual.len());
        }
        assert!(!tokens.is_empty());

        // Runtime executes complete parallel cases before the sequential group.
        // Only original restart directives can advance an exact BE launch slot.
        let mut backend_generations = [0usize; 3];
        let mut token_generations = BTreeMap::new();
        for source_case in input[0]
            .cases
            .iter()
            .filter(|case| !case.sequential)
            .chain(input[0].cases.iter().filter(|case| case.sequential))
        {
            for source in &source_case.steps {
                if let Some(index) = source
                    .meta
                    .restart_be_delay_ms
                    .and(source.meta.kill_be_index)
                {
                    backend_generations[index] += 1;
                }
                if let Some(control) = execution.step(
                    "distributed-resilience",
                    &source_case.case_id,
                    source.query_number,
                ) {
                    for submission in &control.submissions {
                        token_generations
                            .insert(submission.run_token.as_str(), backend_generations);
                    }
                }
                if let Some(index) = source.meta.restart_be_after_establish_context_index {
                    backend_generations[index] += 1;
                }
            }
        }

        let references = launches
            .iter()
            .map(|launch| (launch.reference.session_id.as_str(), launch))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(references.len(), launches.len());
        assert_eq!(
            launches
                .iter()
                .map(|launch| &launch.reference.process_receipt)
                .collect::<BTreeSet<_>>()
                .len(),
            launches.len()
        );
        for role in std::iter::once(RoleSlot::Frontend).chain((0..3).map(RoleSlot::Backend)) {
            let explicit_restarts = input[0]
                .cases
                .iter()
                .flat_map(|case| &case.steps)
                .map(|step| match role {
                    RoleSlot::Frontend => usize::from(step_restarts_fe(&step.meta)),
                    RoleSlot::Backend(index) => {
                        usize::from(step_restarts_be(&step.meta) == Some(index))
                            + usize::from(
                                step.meta.restart_be_delay_ms.and(step.meta.kill_be_index)
                                    == Some(index),
                            )
                    }
                })
                .sum::<usize>();
            let actual = launches
                .iter()
                .filter(|launch| launch.role == role)
                .map(|launch| launch.generation)
                .collect::<BTreeSet<_>>();
            assert_eq!(
                actual,
                (0..=explicit_restarts).collect::<BTreeSet<_>>(),
                "capacity must not inject a role restart"
            );
        }

        for launch in launches {
            for grant in &launch.grants {
                if !matches!(grant.domain.as_str(), "native" | "native-observation") {
                    continue;
                }
                let action = grant.parameters["action"].as_str().unwrap();
                let Some(applied_action) = action.strip_prefix("bind-") else {
                    continue;
                };
                assert_eq!(launch.role, RoleSlot::Frontend);
                let target_id = grant.parameters["target_session_id"].as_str().unwrap();
                let target = references[target_id];
                let index = grant.parameters["backend_index"].as_u64().unwrap() as usize;
                assert_eq!(target.role, RoleSlot::Backend(index));
                assert_eq!(
                    grant.parameters["target_process_receipt"],
                    json!(target.reference.process_receipt)
                );
                let token = grant.parameters["statement_run_token"].as_str().unwrap();
                assert!(tokens.contains(token));
                assert_eq!(
                    target.generation, token_generations[token][index],
                    "binding must retain the original statement's exact BE launch"
                );
                let matching = target
                    .grants
                    .iter()
                    .filter(|candidate| {
                        candidate.domain == grant.domain
                            && candidate.parameters["action"] == applied_action
                            && [
                                "root",
                                "backend_index",
                                "statement_run_token",
                                "target_session_id",
                                "attempt_id",
                            ]
                            .iter()
                            .all(|key| candidate.parameters.get(*key) == grant.parameters.get(*key))
                    })
                    .count();
                assert_eq!(
                    matching, 1,
                    "binding must select one exact declared BE action"
                );
            }
        }
    }

    #[test]
    fn nomination_slots_keep_each_actual_retry_and_the_whole_outer_set_text_distinct() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let sql = "SET @query = (SELECT 1)";
        let input = inputs(vec![step(
            7,
            sql,
            QueryMeta {
                be_log_contains: vec!["NOVAROCKS_GRPC_FETCH_TYPED".into()],
                retry_count: Some(2),
                ..Default::default()
            },
        )]);
        let (_, execution) = plan(&input, file.path()).unwrap();
        let targets = &execution.step("selected", "one", 7).unwrap().submissions;
        assert_eq!(targets.len(), 4);
        let unique = targets
            .iter()
            .map(|target| &target.run_token)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), 4);
        assert!(
            targets
                .iter()
                .all(|target| target.validate_sql(sql).is_ok())
        );
        assert!(
            targets
                .iter()
                .all(|target| target.validate_sql("SELECT 1").is_err())
        );
    }
    #[test]
    fn native_observation_grant_pairs_share_exact_target_and_attempt_paths() {
        let root = Path::new("/selected/native-observations");
        for index in 0..3 {
            let target = RoleLaunchReference {
                session_id: format!("be-{index}-original"),
                process_receipt: PathBuf::from(format!("/selected/be-{index}.process.json")),
            };
            for attempt in [None, Some(1), Some(2)] {
                let (bind, observe) = native_observation_grants(
                    "grpc-fragment",
                    root,
                    index,
                    "selected-submission",
                    &target,
                    attempt,
                );
                for key in [
                    "root",
                    "backend_index",
                    "statement_run_token",
                    "target_session_id",
                    "attempt_id",
                ] {
                    assert_eq!(bind.get(key), observe.get(key), "{key}");
                }
                assert_eq!(bind.get("attempt_id").and_then(Value::as_u64), attempt);
                assert_eq!(
                    bind["target_process_receipt"],
                    json!(target.process_receipt)
                );
                assert_eq!(bind["action"], "bind-grpc-fragment");
                assert_eq!(observe["action"], "grpc-fragment");
                assert_eq!(observe["max_events"], 16384);
            }
        }
    }

    #[test]
    fn recovering_kill_case_predeclares_only_its_finite_observation_slots() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let input = inputs(vec![step(
            1,
            "SELECT COUNT(*) FROM t WHERE sleep(delay_s)",
            QueryMeta {
                kill_be_after_be_log_contains: Some(KillBeAfterBeLogDirective {
                    be_index: 1,
                    pattern: "NOVAROCKS_TASK_CREATE_APPLIED".into(),
                }),
                native_observation_attempts: Some(vec![1, 2]),
                ..Default::default()
            },
        )]);
        let (plan, execution) = plan(&input, file.path()).unwrap();
        let step = execution.step("selected", "one", 1).unwrap();
        assert!(
            step.native_faults.is_empty(),
            "physical kill must not mint Native fault grants"
        );
        assert_eq!(
            step.submissions.len(),
            2,
            "client transient submissions stay independent"
        );
        for launch in plan.planned_launch_summaries().as_array().unwrap() {
            let grants = launch["grants"].as_array().unwrap();
            assert!(grants.iter().all(|grant| grant["domain"] != "native"));
            let observations = grants
                .iter()
                .filter(|grant| grant["domain"] == "native-observation")
                .count();
            assert_eq!(observations, if launch["role"] == "fe" { 12 } else { 4 });
        }
    }

    #[test]
    fn native_observation_slot_validation_preserves_default_and_rejects_invalid_intent() {
        assert_eq!(
            observation_attempt_slots(&QueryMeta::default()).unwrap(),
            vec![None]
        );
        for attempts in [vec![], vec![0], vec![1, 1]] {
            assert!(
                observation_attempt_slots(&QueryMeta {
                    native_observation_attempts: Some(attempts),
                    ..Default::default()
                })
                .is_err()
            );
        }
        let file = tempfile::NamedTempFile::new().unwrap();
        let input = inputs(vec![step(
            1,
            "SELECT 1",
            QueryMeta {
                native_observation_attempts: Some(vec![1, 2]),
                ..Default::default()
            },
        )]);
        assert!(
            plan(&input, file.path())
                .unwrap_err()
                .to_string()
                .contains("requires a selected Native observation marker")
        );
    }
    #[test]
    fn post_step_restart_references_keep_their_original_case_and_step() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut input = inputs(vec![step(
            4,
            "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'db.mv', level => 'package')",
            QueryMeta {
                tags: vec![crate::parser::MV_DOCUMENT_OBSERVATION_TAG.into()],
                restart_fe_after_step: true,
                ..Default::default()
            },
        )]);
        input[0].cases[0].case_id = "a".into();
        let mut b = input[0].cases[0].clone();
        b.case_id = "b".into();
        b.steps[0].query_number = 7;
        input[0].cases.push(b);
        let (_, execution) = plan(&input, file.path()).unwrap();
        let a = &execution.step_role_launches[&("selected".into(), "a".into(), 4)][0].1;
        let b = &execution.step_role_launches[&("selected".into(), "b".into(), 7)][0].1;
        assert!(
            execution
                .step_role_launches
                .values()
                .all(|launches| { launches.len() == 1 && launches[0].0 == RoleSlot::Frontend })
        );
        assert_ne!(a.session_id, b.session_id);
        assert_ne!(a.process_receipt, b.process_receipt);
        assert_eq!(execution.step_role_launches.len(), 2);
        for (generation, reference) in [(1, a), (2, b)] {
            let original = execution
                .planned_launches
                .iter()
                .find(|launch| launch.role == RoleSlot::Frontend && launch.generation == generation)
                .unwrap();
            assert_eq!(reference.session_id, original.reference.session_id);
            assert_eq!(
                reference.process_receipt,
                original.reference.process_receipt
            );
        }
    }
    #[test]
    fn same_step_post_role_references_preserve_backend_and_frontend_originals() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut input = inputs(vec![step(
            4,
            "SELECT 1",
            QueryMeta {
                kill_be_index: Some(1),
                restart_be_delay_ms: Some(0),
                restart_fe_after_step: true,
                restart_be_after_establish_context_index: Some(1),
                ..Default::default()
            },
        )]);
        input[0].cases[0].case_id = "a".into();
        let mut b = input[0].cases[0].clone();
        b.case_id = "b".into();
        b.steps[0].query_number = 7;
        input[0].cases.push(b);
        let (_, execution) = plan(&input, file.path()).unwrap();
        assert_eq!(execution.step_role_launches.len(), 2);
        let mut all_references = BTreeSet::new();
        for (case, query, fe_generation, be_first_generation) in [("a", 4, 1, 1), ("b", 7, 2, 3)] {
            let launches = &execution.step_role_launches[&("selected".into(), case.into(), query)];
            assert_eq!(launches.len(), 3);
            let expected = [
                (RoleSlot::Backend(1), be_first_generation),
                (RoleSlot::Frontend, fe_generation),
                (RoleSlot::Backend(1), be_first_generation + 1),
            ];
            for ((role, reference), (expected_role, generation)) in launches.iter().zip(expected) {
                assert_eq!(*role, expected_role);
                let original = execution
                    .planned_launches
                    .iter()
                    .find(|launch| launch.role == *role && launch.generation == generation)
                    .unwrap();
                assert_eq!(reference.session_id, original.reference.session_id);
                assert_eq!(
                    reference.process_receipt,
                    original.reference.process_receipt
                );
                assert!(all_references.insert(reference.session_id.as_str()));
                assert_ne!(generation, 0, "initial startup is not a step reservation");
            }
        }
        assert_eq!(all_references.len(), 6);
    }
}
