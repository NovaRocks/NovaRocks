// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0 (the
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

//! Finite connector statements and their original per-phase observation needs.
use super::{
    ConnectorRead, ConnectorSqlOutcome, connector_bigint_rows, connector_integer_rows,
    connector_sql_outcome, publish_connector_result, run_original_connector_pending,
};
use crate::actors::sql::{SqlSession, StatementFailure, StatementResult};
use crate::scenario::ScenarioContext;
use crate::scenarios::native_sql_controls::SelectedStatement;
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::{
    QueryLifecycleStatementObservation, TestClusterOwner,
    test_sessions::{ClusterTestSessionPlan, RoleLaunchReference, RoleSlot},
};
use novarocks_failpoint::session::DomainGrant;
use serde_json::json;
#[cfg(test)]
use std::time::Duration;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};

pub(super) const READER: &str =
    novarocks_failpoint::observation_marker::ObservationAction::ReaderMarker.as_str();
pub(super) const GRPC: &str =
    novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str();
pub(super) const MATERIAL: &str =
    novarocks_failpoint::observation_marker::ObservationAction::CatalogMaterialization.as_str();
pub(super) const LIFECYCLE: &str =
    novarocks_failpoint::observation_marker::ObservationAction::CatalogLifecycle.as_str();
pub(super) const HOLD: &str = "catalog-hold";
pub(super) const FAILURE: &str = "catalog-failure";

pub(super) fn launch_config() -> crate::scenario::ScenarioLaunchConfig {
    let mut launch = super::connector_launch_config();
    launch.child_environment = novarocks_cluster_harness::CrossProcessChildEnvironment::default();
    launch
}

pub(super) fn held_sql(catalog: &str, database: &str, table: &str) -> String {
    format!(
        "SELECT t.v FROM (SELECT v FROM {catalog}.{database}.{table} WHERE v % 4096 = 0 AND sleep(1) = 0) AS t CROSS JOIN TABLE(generate_series(1, 64)) AS gs(x)"
    )
}
pub(super) fn count_sql(catalog: &str, database: &str, table: &str) -> String {
    format!("SELECT count(*) FROM {catalog}.{database}.{table}")
}
pub(super) fn summary_sql(catalog: &str, database: &str, table: &str) -> String {
    format!("SELECT count(*), sum(v) FROM {catalog}.{database}.{table}")
}
pub(super) fn sum_sql(catalog: &str, database: &str, table: &str) -> String {
    format!("SELECT sum(v) FROM {catalog}.{database}.{table}")
}

pub(super) struct Spec {
    pub sql: String,
    pub observations: Vec<(usize, &'static str)>,
    pub faults: Vec<(usize, &'static str)>,
    pub fe_launch: usize,
    pub be_launch: [usize; 3],
}
impl Spec {
    pub fn observed(sql: String, actions: &[&'static str]) -> Self {
        Self {
            sql,
            observations: (0..3)
                .flat_map(|index| actions.iter().map(move |action| (index, *action)))
                .collect(),
            faults: vec![],
            fe_launch: 0,
            be_launch: [0; 3],
        }
    }
    pub fn on_backend(mut self, index: usize, action: &'static str) -> Self {
        self.observations.push((index, action));
        self
    }
    pub fn fault(mut self, index: usize, action: &'static str) -> Self {
        self.faults.push((index, action));
        self
    }
    pub fn after_fe_restart(mut self) -> Self {
        self.fe_launch = 1;
        self
    }
    pub fn after_be_restart(mut self, index: usize) -> Self {
        self.be_launch[index] = 1;
        self
    }
}
#[derive(Clone)]
pub(super) struct Selected {
    pub statement: SelectedStatement,
    root: PathBuf,
    observations: Vec<(usize, &'static str)>,
}
impl Selected {
    pub fn trigger(&self, action: &'static str, contents: &str) -> Result<()> {
        ensure!(
            matches!(action, HOLD | FAILURE),
            "unsupported connector injection trigger"
        );
        let targets = self
            .observations
            .iter()
            .filter(|(_, name)| *name == action)
            .collect::<Vec<_>>();
        ensure!(
            !targets.is_empty(),
            "connector injection was not declared for this statement"
        );
        for &&(index, _) in &targets {
            fs::write(self.trigger_path(index, action), contents)?;
        }
        Ok(())
    }
    fn trigger_path(&self, index: usize, action: &str) -> PathBuf {
        let name = if action == HOLD { "hold" } else { "failure" };
        self.root
            .join(format!("catalog-install-{name}-{index}.trigger"))
    }
    pub fn release(&self, action: &'static str) -> Result<()> {
        let mut errors = vec![];
        for &(index, name) in &self.observations {
            if name != action {
                continue;
            }
            let path = self.trigger_path(index, action);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => errors.push(format!("{}: {error}", path.display())),
            }
        }
        if !errors.is_empty() {
            bail!(errors.join("; "));
        }
        Ok(())
    }
}
struct Frozen {
    selected: Vec<Selected>,
    used: BTreeSet<usize>,
}
#[derive(Default)]
pub(super) struct Owner(Mutex<Option<Frozen>>);
impl Owner {
    pub fn freeze(
        &self,
        specs: Vec<Spec>,
        fe_restart: bool,
        be_restarts: &[usize],
        size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<ClusterTestSessionPlan> {
        ensure!(size == 3, "connector controls require native 1FE+3BE");
        ensure!(
            !specs.is_empty() && specs.len() <= 64,
            "connector statement set exceeds finite bound"
        );
        let controlled = binary;
        let mut owner = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("connector intent owner poisoned"))?;
        ensure!(owner.is_none(), "connector intent already frozen");
        let mut plan = ClusterTestSessionPlan::new(deadline)?;
        let root = plan.reserve_transport("connector-observations")?;
        let mut launches: BTreeMap<(RoleSlot, usize), (RoleLaunchReference, Vec<DomainGrant>)> =
            BTreeMap::new();
        for role in std::iter::once(RoleSlot::Frontend).chain((0..3).map(RoleSlot::Backend)) {
            launches.insert((role, 0), (plan.reserve_launch(role)?, vec![]));
        }
        if fe_restart {
            launches.insert(
                (RoleSlot::Frontend, 1),
                (plan.reserve_launch(RoleSlot::Frontend)?, vec![]),
            );
        }
        let mut replacements = BTreeSet::new();
        for &index in be_restarts {
            ensure!(
                index < 3 && replacements.insert(index),
                "invalid connector backend restart slots"
            );
            let role = RoleSlot::Backend(index);
            launches.insert((role, 1), (plan.reserve_launch(role)?, vec![]));
        }
        let mut selected = vec![];
        let mut observed_fe = BTreeSet::new();
        for spec in specs {
            ensure!(
                !spec.sql.trim().is_empty(),
                "connector statement SQL must be complete"
            );
            let fe = (RoleSlot::Frontend, spec.fe_launch);
            ensure!(
                launches.contains_key(&fe),
                "connector query lacks its reserved FE launch"
            );
            if observed_fe.insert(fe) {
                add(
                    &mut launches.get_mut(&fe).unwrap().1,
                    "statement-observation",
                    json!({"action":"observe","bearer_secret":plan.fresh_domain_secret()?}),
                );
            }
            let token = plan.fresh_domain_secret()?;
            let mut unique = BTreeSet::new();
            for &(index, action) in &spec.observations {
                ensure!(
                    index < 3 && unique.insert((index, action)),
                    "duplicate connector observation target"
                );
                ensure!(
                    matches!(
                        action,
                        READER | GRPC | MATERIAL | LIFECYCLE | HOLD | FAILURE
                    ),
                    "unsupported connector observation owner"
                );
                let be = (RoleSlot::Backend(index), spec.be_launch[index]);
                let target = launches
                    .get(&be)
                    .context("connector query lacks its reserved BE launch")?
                    .0
                    .clone();
                add(
                    &mut launches.get_mut(&fe).unwrap().1,
                    "native-observation",
                    json!({"action":format!("bind-{action}"),"root":root,"backend_index":index,"statement_run_token":token,"target_session_id":target.session_id,"target_process_receipt":target.process_receipt}),
                );
                let mut parameters = json!({"action":action,"root":root,"backend_index":index,"statement_run_token":token,"target_session_id":target.session_id});
                if !matches!(action, HOLD | FAILURE) {
                    parameters["max_events"] = json!(16384);
                }
                add(
                    &mut launches.get_mut(&be).unwrap().1,
                    "native-observation",
                    parameters,
                );
            }
            let mut unique = BTreeSet::new();
            for &(index, action) in &spec.faults {
                ensure!(
                    index < 3 && unique.insert((index, action)),
                    "duplicate connector Native fault target"
                );
                novarocks_failpoint::QueryLifecycleFaultKind::parse(action)
                    .context("unsupported connector Native fault")?;
                let be = (RoleSlot::Backend(index), spec.be_launch[index]);
                let target = launches
                    .get(&be)
                    .context("connector fault lacks its reserved BE launch")?
                    .0
                    .clone();
                add(
                    &mut launches.get_mut(&fe).unwrap().1,
                    "native",
                    json!({"action":format!("bind-{action}"),"root":root,"backend_index":index,"statement_run_token":token,"target_session_id":target.session_id,"target_process_receipt":target.process_receipt}),
                );
                add(
                    &mut launches.get_mut(&be).unwrap().1,
                    "native",
                    json!({"action":action,"root":root,"backend_index":index,"statement_run_token":token,"target_session_id":target.session_id}),
                );
            }
            selected.push(Selected {
                statement: SelectedStatement {
                    sql: spec.sql,
                    token,
                    faults: spec.faults,
                    deadline,
                },
                root: root.clone(),
                observations: spec.observations,
            });
        }
        for ((role, ordinal), (reference, grants)) in launches {
            ensure!(
                grants.len() <= novarocks_failpoint::session::MAX_GRANTS,
                "connector {:?} launch {ordinal} needs {} grants, exceeds {}",
                role,
                grants.len(),
                novarocks_failpoint::session::MAX_GRANTS
            );
            if grants.is_empty() {
                plan.authorize_ordinary_launch(&reference, binary.to_path_buf())?;
            } else {
                let owners = if role == RoleSlot::Frontend {
                    &[
                        "novarocks-server",
                        "novarocks-frontend-application",
                        "novarocks-native-adapter",
                    ][..]
                } else {
                    &[
                        "novarocks-server",
                        "novarocks-native-adapter",
                        "novarocks-worker",
                        "novarocks-execution",
                    ][..]
                };
                plan.authorize_launch(
                    &reference,
                    grants,
                    owners
                        .iter()
                        .map(|owner| ((*owner).into(), "test-controls".into()))
                        .collect(),
                )?;
                plan.set_launch_binary(&reference, controlled.to_path_buf())?;
            }
        }
        plan.validate_selected_intents(size)?;
        *owner = Some(Frozen {
            selected,
            used: BTreeSet::new(),
        });
        Ok(plan)
    }
    pub fn take(&self, index: usize) -> Result<Selected> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("connector intent owner poisoned"))?;
        let owner = owner
            .as_mut()
            .context("connector controls were not frozen before launch")?;
        let selected = owner
            .selected
            .get(index)
            .context("connector statement outside finite plan")?
            .clone();
        ensure!(
            owner.used.insert(index),
            "connector statement already selected; retry requires another token"
        );
        Ok(selected)
    }
}
fn add(grants: &mut Vec<DomainGrant>, domain: &str, parameters: serde_json::Value) {
    grants.push(DomainGrant {
        domain: domain.into(),
        grant_id: format!("connector-{domain}-{}", grants.len()),
        parameters,
    });
}

pub(super) fn specs(name: &str) -> Result<(Vec<Spec>, bool, Vec<usize>)> {
    let simple = |catalog, database, table| count_sql(catalog, database, table);
    let held = |catalog, database, table| held_sql(catalog, database, table);
    let all_reader = |sql| Spec::observed(sql, &[READER]);
    let mut fe_restart = false;
    let mut be_restarts = vec![];
    let statements = match name {
        "connector/uea5d-multi-split-dop" => (0..2)
            .map(|_| {
                Spec::observed(
                    summary_sql("uea5d_dop_catalog", "uea5d_dop_db", "uea5d_dop_data"),
                    &[READER, GRPC],
                )
            })
            .collect(),
        "connector/iceberg-typed-read-data" => vec![Spec::observed(
            simple("typed_read_catalog", "typed_read_db", "typed_read_data"),
            &[READER, GRPC],
        )],
        "connector/distributed-reader-cancel" => vec![Spec::observed(
            held(
                "connector_cancel_catalog",
                "connector_cancel_db",
                "connector_cancel_data",
            ),
            &[READER, GRPC],
        )],
        "connector/distributed-reader-kill-connection" => vec![Spec::observed(
            held(
                "connector_kill_connection_catalog",
                "connector_kill_connection_db",
                "connector_kill_connection_data",
            ),
            &[READER, GRPC],
        )],
        "connector/catalog-version-drain" => vec![
            all_reader(held(
                "connector_generation_catalog",
                "connector_generation_db",
                "connector_generation_data",
            )),
            all_reader(simple(
                "connector_generation_catalog",
                "connector_generation_db",
                "connector_generation_data",
            )),
        ],
        "connector/catalog-ready-lifecycle" => {
            let catalog = "catalog_ready_lifecycle";
            let database = "catalog_ready_db";
            let table = "catalog_ready_data";
            vec![
                Spec::observed(
                    held(catalog, database, table),
                    &[READER, GRPC, MATERIAL, LIFECYCLE, HOLD],
                ),
                Spec::observed(simple(catalog, database, table), &[])
                    .on_backend(1, READER)
                    .on_backend(1, LIFECYCLE)
                    .on_backend(1, FAILURE),
                Spec::observed(simple(catalog, database, table), &[]).on_backend(1, MATERIAL),
                Spec::observed(
                    held(catalog, database, table),
                    &[READER, GRPC, MATERIAL, LIFECYCLE, HOLD],
                ),
                Spec::observed(simple(catalog, database, table), &[MATERIAL, LIFECYCLE]),
                Spec::observed(simple(catalog, database, table), &[MATERIAL])
                    .fault(1, super::ESTABLISH_PARTICIPATION_ACK_DROP),
            ]
        }
        "connector/catalog-fe-restart-cache" => {
            fe_restart = true;
            let (catalog, database, table) = (
                "catalog_fe_restart_cache",
                "catalog_fe_restart_db",
                "catalog_fe_restart_data",
            );
            let mut statements = materialized_inserts(
                catalog,
                database,
                table,
                ["1, 100000", "100001, 200000", "200001, 300000"],
            );
            statements.extend([
                Spec::observed(simple(catalog, database, table), &[READER, MATERIAL]),
                all_reader(held(catalog, database, table)),
                Spec::observed(simple(catalog, database, table), &[READER, MATERIAL])
                    .after_fe_restart(),
            ]);
            statements
        }
        "connector/catalog-read-write-runtime" => {
            be_restarts.push(0);
            let (catalog, database, table) = (
                "catalog_read_write_runtime",
                "catalog_read_write_db",
                "catalog_read_write_data",
            );
            let mut statements = materialized_inserts(
                catalog,
                database,
                table,
                ["1, 1000", "1001, 2000", "2001, 3000"],
            );
            statements.extend([
                Spec::observed(summary_sql(catalog, database, table), &[READER, MATERIAL]),
                Spec::observed(
                    format!("INSERT INTO {catalog}.{database}.{table} VALUES (1001)"),
                    &[],
                )
                .on_backend(0, MATERIAL)
                .after_be_restart(0),
                Spec::observed(summary_sql(catalog, database, table), &[READER, MATERIAL])
                    .after_be_restart(0),
            ]);
            statements
        }
        "connector/vended-credential-read-write" => vec![all_reader(summary_sql(
            "vended_rest_catalog",
            "vended_rest_db",
            "vended_rest_data",
        ))],
        "connector/static-credential-generation" => vec![
            all_reader(sum_sql(
                "cca_static_blue",
                "static_blue_db",
                "static_blue_data",
            )),
            all_reader(sum_sql(
                "cca_static_green",
                "static_green_db",
                "static_green_data",
            )),
        ],
        "connector/access-domain-cache-isolation" => vec![
            all_reader(sum_sql(
                "cca_cache_domain_blue",
                "cache_domain_db",
                "cache_domain_data",
            )),
            all_reader(sum_sql(
                "cca_cache_domain_green",
                "cache_domain_db",
                "cache_domain_data",
            )),
        ],
        _ => bail!("connector scene has no accepted observation declaration: {name}"),
    };
    Ok((statements, fe_restart, be_restarts))
}
fn materialized_inserts(
    catalog: &str,
    database: &str,
    table: &str,
    ranges: [&str; 3],
) -> Vec<Spec> {
    ranges.into_iter().map(|range|Spec::observed(format!("INSERT INTO {catalog}.{database}.{table} SELECT generate_series FROM TABLE(generate_series({range}))"),&[MATERIAL])).collect()
}

pub(super) fn observed_fixture(
    context: &mut ScenarioContext,
    owner: &Owner,
    control: &mut SqlSession,
    catalog: &str,
    database: &str,
    table: &str,
    warehouse: &Path,
) -> Result<()> {
    super::create_catalog(control, catalog, warehouse)?;
    control.execute(format!("CREATE DATABASE {catalog}.{database}"))?;
    control.execute(format!(
        "CREATE TABLE {catalog}.{database}.{table} (v BIGINT)"
    ))?;
    for (index, range) in ["1, 100000", "100001, 200000", "200001, 300000"]
        .into_iter()
        .enumerate()
    {
        let selected = owner.take(index)?;
        execute(
            context,
            &selected,
            control,
            &format!(
                "INSERT INTO {catalog}.{database}.{table} SELECT generate_series FROM TABLE(generate_series({range}))"
            ),
        )?;
    }
    Ok(())
}
pub(super) fn with_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => {
            Err(primary.context(format!("cleanup failed: {cleanup:#}")))
        }
    }
}
pub(super) fn outcome(
    context: &mut ScenarioContext,
    selected: &Selected,
    conn: &mut SqlSession,
    sql: &str,
) -> Result<std::result::Result<StatementResult, StatementFailure>> {
    let pending = conn.submit(sql)?;
    let (pending, observation) = selected.statement.arm_pending(context, pending, sql)?;
    let result = run_original_connector_pending(conn, pending, selected.statement.deadline);
    let fence = match &result {
        Ok(result) => public_error_successor_fence(conn, result, selected.statement.deadline),
        Err(_) => Ok(()),
    };
    let observed = settle_statement_observation(
        context,
        &observation,
        matches!(&result, Ok(Ok(_))),
        selected.statement.deadline,
    );
    let cleanup = with_cleanup(fence, observed);
    match result {
        Ok(result) => match cleanup {
            Ok(()) => Ok(result),
            Err(cleanup) => match result {
                Ok(_) => Err(cleanup.context("connector SQL succeeded but cleanup failed")),
                Err(primary) => Err(anyhow::Error::new(primary)
                    .context(format!("connector cleanup failed: {cleanup:#}"))),
            },
        },
        Err(primary) => with_cleanup(Err(primary), cleanup),
    }
}
pub(super) fn query(
    context: &mut ScenarioContext,
    selected: &Selected,
    conn: &mut SqlSession,
    sql: &str,
) -> Result<StatementResult> {
    outcome(context, selected, conn, sql)?.map_err(anyhow::Error::from)
}
pub(super) fn execute(
    context: &mut ScenarioContext,
    selected: &Selected,
    conn: &mut SqlSession,
    sql: &str,
) -> Result<()> {
    query(context, selected, conn, sql).map(|_| ())
}
fn public_error_successor_fence<T>(
    conn: &mut SqlSession,
    result: &std::result::Result<T, StatementFailure>,
    deadline: Instant,
) -> Result<()> {
    if !matches!(result, Err(StatementFailure::Trino(_))) {
        return Ok(());
    }
    let pending = conn.submit("SELECT 1")?;
    let rows = connector_integer_rows(run_original_connector_pending(conn, pending, deadline)??)?;
    ensure!(
        rows == [1],
        "connector successor after public SQL error returned {rows:?}, expected [1]"
    );
    Ok(())
}

fn settle_statement_observation(
    context: &mut ScenarioContext,
    observation: &QueryLifecycleStatementObservation,
    sql_succeeded: bool,
    deadline: Instant,
) -> Result<()> {
    if sql_succeeded {
        let observed = context
            .handle()
            .await_query_lifecycle_statement_observation(observation, true, deadline)
            .map(|_| ());
        let drained = context
            .handle()
            .drain_query_lifecycle_statement_binding(observation, deadline);
        with_cleanup(observed, drained)
    } else {
        // A failed attempt may have no release-contribution snapshot. This only
        // validates and releases its journal; SQL/actor/resource owners prove exit.
        context
            .handle()
            .drain_query_lifecycle_statement_binding(observation, deadline)
    }
}

fn pending_query_error<T>(
    result: &mpsc::Receiver<std::result::Result<T, StatementFailure>>,
) -> Result<()> {
    match result.try_recv() {
        Ok(Err(error)) => Err(error.into()),
        Ok(Ok(_)) | Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => Ok(()),
    }
}

pub(super) struct Reader {
    pub read: ConnectorRead,
    pub client_query_id: String,
    pub cancel: novarocks_test_trino_client::CancelHandle,
    observation: QueryLifecycleStatementObservation,
    selected: Selected,
    query_outcome: Arc<Mutex<Option<ConnectorSqlOutcome>>>,
    skip_retired_socket_probe: Arc<AtomicBool>,
}
impl Reader {
    pub fn start(
        context: &mut ScenarioContext,
        selected: &Selected,
        user: &str,
        port: u16,
        dop: Option<NonZeroU32>,
    ) -> Result<Self> {
        let deadline = context.deadline();
        let mut conn = SqlSession::new(user, port, deadline)?;
        if let Some(dop) = dop {
            conn.execute(format!("SET pipeline_dop = {}", dop.get()))?;
        }
        let pending = conn.submit(&selected.statement.sql)?;
        let id = pending.query_id.clone();
        let cancel = pending.cancel_handle();
        let (pending, observation) =
            selected
                .statement
                .arm_pending(context, pending, &selected.statement.sql)?;
        let (ready_tx, ready) = mpsc::sync_channel(1);
        let (done_tx, done) = mpsc::sync_channel(1);
        let (probe, probe_rx) = mpsc::sync_channel(1);
        let (probe_result_tx, probe_result) = mpsc::sync_channel(1);
        let (release, release_rx) = mpsc::channel();
        let query_outcome = Arc::new(Mutex::new(None));
        let saved = query_outcome.clone();
        let skip_retired_socket_probe = Arc::new(AtomicBool::new(false));
        let worker_skip_probe = skip_retired_socket_probe.clone();
        let worker_id = id.clone();
        let worker = thread::Builder::new()
            .name("connector-selected-reader".into())
            .spawn(move || {
                ready_tx
                    .send(worker_id)
                    .context("publish selected connector actual client query")?;
                let result = conn.run_pending(pending);
                *saved
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(connector_sql_outcome(&result));
                publish_connector_result(&done_tx, result.and_then(connector_bigint_rows))
                    .context("publish selected connector reader result")?;
                probe_rx
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    .context("receive selected connector probe")?;
                // The original parent explicitly retires this carrier-only
                // socket probe after FE exit or when the scenario has retired
                // the socket assertion. Waking this stage supplies no query
                // completion or physical exit; the actor still releases/joins.
                if !worker_skip_probe.load(Ordering::Acquire) {
                    let probe_result = match conn.submit("SELECT 1") {
                        Ok(pending) => run_original_connector_pending(
                            &mut conn, pending, deadline,
                        )?
                        .and_then(|result| {
                            connector_integer_rows(result).and_then(|rows| match rows.as_slice() {
                                [value] => Ok(Some(*value)),
                                _ => Err(StatementFailure::Protocol(format!(
                                    "connector probe expected one integer row: {rows:?}"
                                ))),
                            })
                        }),
                        Err(error) => Err(error),
                    };
                    publish_connector_result(&probe_result_tx, probe_result)
                        .context("publish selected connector probe")?;
                }
                release_rx
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    .context("release selected connector reader")?;
                Ok(())
            });
        match worker {
            Ok(thread) => Ok(Self {
                read: ConnectorRead {
                    ready,
                    done,
                    probe,
                    probe_result,
                    release,
                    thread,
                },
                client_query_id: id,
                cancel,
                observation,
                selected: selected.clone(),
                query_outcome,
                skip_retired_socket_probe,
            }),
            Err(primary) => {
                let cancelled = cancel.delete(deadline).map_err(anyhow::Error::from);
                let drained = context
                    .handle()
                    .drain_query_lifecycle_statement_binding(&observation, deadline);
                with_cleanup(Err(primary.into()), with_cleanup(cancelled, drained))
            }
        }
    }
    pub fn start_with_hold(
        context: &mut ScenarioContext,
        selected: &Selected,
        user: &str,
        port: u16,
    ) -> Result<Self> {
        match Self::start(context, selected, user, port, None) {
            Ok(reader) => Ok(reader),
            Err(primary) => with_cleanup(Err(primary), selected.release(HOLD)),
        }
    }
    pub fn finish_after<T>(
        self,
        context: &mut ScenarioContext,
        _control: &mut SqlSession,
        phase: Result<T>,
    ) -> Result<T> {
        self.finish_inner(context, phase, false, false)
    }
    /// Call only after the original restart owner proved the old FE physically exited.
    pub fn finish_after_fe_exit<T>(
        self,
        context: &mut ScenarioContext,
        _control: &mut SqlSession,
        phase: Result<T>,
    ) -> Result<T> {
        self.finish_inner(context, phase, true, true)
    }
    /// The HTTP cancellation scene has retired the old socket assertion.
    /// Close that original actor stage explicitly before consuming its Join.
    pub fn finish_after_retiring_socket_probe<T>(
        self,
        context: &mut ScenarioContext,
        phase: Result<T>,
    ) -> Result<T> {
        self.finish_inner(context, phase, false, true)
    }
    fn finish_inner<T>(
        self,
        context: &mut ScenarioContext,
        phase: Result<T>,
        fe_exited: bool,
        retire_socket_probe: bool,
    ) -> Result<T> {
        let before = *self
            .query_outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut attempted_cancel = false;
        let mut interrupted = if !fe_exited
            && phase.is_err()
            && !matches!(
                before,
                Some(ConnectorSqlOutcome::Success | ConnectorSqlOutcome::PublicFailure)
            ) {
            attempted_cancel = true;
            self.cancel
                .delete(self.selected.statement.deadline)
                .map_err(anyhow::Error::from)
        } else {
            Ok(())
        };
        if interrupted.is_err() {
            interrupted = with_cleanup(interrupted, context.shutdown());
        }
        let released = if phase.is_err() {
            self.selected.release(HOLD)
        } else {
            Ok(())
        };
        if retire_socket_probe {
            self.skip_retired_socket_probe
                .store(true, Ordering::Release);
        }
        if phase.is_err() || retire_socket_probe {
            let _ = self.read.probe.try_send(());
            let _ = self.read.release.send(());
        }
        let joined = self
            .read
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("selected connector reader panicked"))
            .and_then(|result| result);
        let after = *self
            .query_outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !fe_exited
            && !attempted_cancel
            && !matches!(
                after,
                Some(ConnectorSqlOutcome::Success | ConnectorSqlOutcome::PublicFailure)
            )
        {
            let cancelled = self
                .cancel
                .delete(self.selected.statement.deadline)
                .map_err(anyhow::Error::from);
            interrupted = with_cleanup(
                interrupted,
                if cancelled.is_err() {
                    with_cleanup(cancelled, context.shutdown())
                } else {
                    cancelled
                },
            );
        }
        let pending = with_cleanup(
            pending_query_error(&self.read.done),
            pending_query_error(&self.read.probe_result),
        );
        let observed = if fe_exited {
            Ok(())
        } else {
            settle_statement_observation(
                context,
                &self.observation,
                after == Some(ConnectorSqlOutcome::Success),
                self.selected.statement.deadline,
            )
        };
        with_cleanup(
            with_cleanup(
                with_cleanup(
                    with_cleanup(with_cleanup(phase, interrupted), released),
                    joined,
                ),
                pending,
            ),
            observed,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCENES: &[&str] = &[
        "connector/uea5d-multi-split-dop",
        "connector/iceberg-typed-read-data",
        "connector/distributed-reader-cancel",
        "connector/distributed-reader-kill-connection",
        "connector/catalog-version-drain",
        "connector/catalog-ready-lifecycle",
        "connector/catalog-fe-restart-cache",
        "connector/catalog-read-write-runtime",
        "connector/vended-credential-read-write",
        "connector/static-credential-generation",
        "connector/access-domain-cache-isolation",
    ];
    fn binary() -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("binary");
        fs::write(&binary, []).unwrap();
        (directory, binary)
    }

    #[test]
    fn every_connector_scene_fits_original_role_and_declaration_bounds() {
        let (_directory, binary) = binary();
        for &name in SCENES {
            let owner = Owner::default();
            let (specs, fe, restarts) = specs(name).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let plan = owner
                .freeze(specs, fe, &restarts, 3, &binary, deadline)
                .unwrap();
            plan.validate_selected_intents(3).unwrap();
            assert_eq!(plan.deadline(), deadline);
            for role in plan.planned_launch_summaries().as_array().unwrap() {
                assert!(role["grants"].as_array().unwrap().len() <= 64, "{name}");
            }
        }
    }

    #[test]
    fn joined_reader_preserves_pending_query_and_probe_failures() {
        let (done_tx, done) =
            mpsc::sync_channel::<std::result::Result<Vec<i64>, StatementFailure>>(1);
        let (probe_tx, probe) =
            mpsc::sync_channel::<std::result::Result<Option<i64>, StatementFailure>>(1);
        let actor = thread::spawn(move || -> Result<()> {
            publish_connector_result(
                &done_tx,
                Err(StatementFailure::Transport(
                    "pending reader SQL failure".into(),
                )),
            )?;
            publish_connector_result(
                &probe_tx,
                Err(StatementFailure::Transport(
                    "pending reader probe failure".into(),
                )),
            )?;
            Err(anyhow::anyhow!("reader socket cleanup failure"))
        });
        let joined = actor.join().expect("reader actor joined");
        let pending = with_cleanup(pending_query_error(&done), pending_query_error(&probe));
        let primary: Result<()> = Err(anyhow::anyhow!("original reader phase failure"));
        let error = with_cleanup(with_cleanup(primary, joined), pending).unwrap_err();
        let text = format!("{error:#}");
        for expected in [
            "original reader phase failure",
            "reader socket cleanup failure",
            "pending reader SQL failure",
            "pending reader probe failure",
        ] {
            assert!(text.contains(expected), "{text}");
        }
        // A result already consumed by an oracle must not be counted again.
        assert!(pending_query_error(&done).is_ok());
        assert!(pending_query_error(&probe).is_ok());
    }

    #[test]
    fn ready_lifecycle_has_minimal_phase_permissions_and_independent_injections() {
        let (specs, fe, restarts) = specs("connector/catalog-ready-lifecycle").unwrap();
        assert_eq!(specs.len(), 6);
        assert!(!fe && restarts.is_empty());
        assert_eq!(
            specs[0]
                .observations
                .iter()
                .filter(|(_, action)| *action == HOLD)
                .count(),
            3
        );
        assert_eq!(
            specs[1].observations,
            vec![(1, READER), (1, LIFECYCLE), (1, FAILURE)]
        );
        assert_eq!(specs[2].observations, vec![(1, MATERIAL)]);
        assert!(
            specs[4]
                .observations
                .iter()
                .all(|(_, action)| matches!(*action, MATERIAL | LIFECYCLE))
        );
        assert_eq!(
            specs[5].faults,
            vec![(1, "establish-participation-ack-drop")]
        );
        assert!(
            specs[5]
                .observations
                .iter()
                .all(|(_, action)| *action == MATERIAL)
        );
        let (_directory, binary) = binary();
        let owner = Owner::default();
        let plan = owner
            .freeze(
                specs,
                fe,
                &restarts,
                3,
                &binary,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        let summary = plan.planned_launch_summaries();
        let frontend = summary
            .as_array()
            .unwrap()
            .iter()
            .find(|role| role["role"] == "fe")
            .unwrap();
        assert_eq!(frontend["grants"].as_array().unwrap().len(), 45);
    }

    #[test]
    fn declaration_overflow_fails_before_launch_without_expanding_grant_budget() {
        let (_directory, binary) = binary();
        let owner = Owner::default();
        let maximum = novarocks_failpoint::session::MAX_GRANTS;
        let observations_per_statement = Spec::observed("SELECT 1".into(), &[READER, GRPC])
            .observations
            .len();
        let statement_count = (maximum - 1) / observations_per_statement + 1;
        assert!(statement_count <= 64);
        let expected_grants = 1 + statement_count * observations_per_statement;
        assert!(expected_grants > maximum);
        let too_many = (0..statement_count)
            .map(|_| Spec::observed("SELECT 1".into(), &[READER, GRPC]))
            .collect();
        let error = owner
            .freeze(
                too_many,
                false,
                &[],
                3,
                &binary,
                Instant::now() + Duration::from_secs(5),
            )
            .err()
            .unwrap();
        assert_eq!(
            error.to_string(),
            format!(
                "connector Frontend launch 0 needs {expected_grants} grants, exceeds {maximum}"
            )
        );
        assert!(owner.take(0).is_err());
    }

    #[test]
    fn identical_sql_phases_receive_distinct_once_tokens() {
        let (_directory, binary) = binary();
        let owner = Owner::default();
        let (specs, fe, restarts) = specs("connector/uea5d-multi-split-dop").unwrap();
        let _plan = owner
            .freeze(
                specs,
                fe,
                &restarts,
                3,
                &binary,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        let first = owner.take(0).unwrap();
        let second = owner.take(1).unwrap();
        assert_eq!(first.statement.sql, second.statement.sql);
        assert_ne!(first.statement.token, second.statement.token);
        assert!(owner.take(0).is_err());
        assert!(owner.take(2).is_err());
    }

    #[test]
    fn replacement_queries_use_reserved_successor_roles() {
        let (fe_specs, fe, restarts) = specs("connector/catalog-fe-restart-cache").unwrap();
        assert!(fe && restarts.is_empty());
        assert_eq!(fe_specs[5].fe_launch, 1);
        assert!(fe_specs[..5].iter().all(|spec| spec.fe_launch == 0));
        let (be_specs, fe, restarts) = specs("connector/catalog-read-write-runtime").unwrap();
        assert!(!fe);
        assert_eq!(restarts, vec![0]);
        assert_eq!(be_specs[4].be_launch, [1, 0, 0]);
        assert_eq!(be_specs[5].be_launch, [1, 0, 0]);
        assert!(be_specs[..4].iter().all(|spec| spec.be_launch == [0, 0, 0]));
    }

    #[test]
    fn observe_only_statement_cannot_create_a_catalog_injection_trigger() {
        let (_directory, binary) = binary();
        let owner = Owner::default();
        let _plan = owner
            .freeze(
                vec![Spec::observed("SELECT 1".into(), &[MATERIAL])],
                false,
                &[],
                3,
                &binary,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        let selected = owner.take(0).unwrap();
        assert!(selected.trigger(HOLD, "hold\n").is_err());
        assert!(selected.trigger(FAILURE, "fail\n").is_err());
        assert!(fs::read_dir(&selected.root).unwrap().next().is_none());
    }

    #[test]
    fn catalog_trigger_targets_only_declared_backend_and_releases_exact_transport() {
        let (_directory, binary) = binary();
        let owner = Owner::default();
        let _plan = owner
            .freeze(
                vec![Spec::observed("SELECT 1".into(), &[]).on_backend(1, FAILURE)],
                false,
                &[],
                3,
                &binary,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        let selected = owner.take(0).unwrap();
        selected.trigger(FAILURE, "fail\n").unwrap();
        assert!(selected.trigger_path(1, FAILURE).exists());
        assert!(!selected.trigger_path(0, FAILURE).exists());
        assert!(!selected.trigger_path(2, FAILURE).exists());
        selected.release(FAILURE).unwrap();
        selected.release(FAILURE).unwrap();
        assert!(!selected.trigger_path(1, FAILURE).exists());
    }
}
