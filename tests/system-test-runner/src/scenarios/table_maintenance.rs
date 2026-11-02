use super::mv_uea7::ManagedMvRestFixture;
use super::native_sql_controls::SelectedStatement;
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use ::mysql::prelude::{FromRow, Queryable};
use ::mysql::{Conn, Row};
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::{
    ServerHandle,
    test_sessions::{ClusterTestSessionPlan, RoleLaunchReference, RoleSlot},
};
use novarocks_failpoint::{
    session::DomainGrant,
    target_binding::{RoleProcessReceipt, StatementWorkNomination},
};
use serde::Deserialize;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const OPTIMIZE_SQL: &str = "ALTER TABLE orders OPTIMIZE";
const CATALOG: &str = "system_stat2f_maintenance";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(OptimizeTargetReplacement::default())]
}

#[derive(Default)]
struct OptimizeTargetReplacement {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
    intent: Mutex<Option<MaintenanceIntent>>,
}

#[derive(Clone)]
struct MaintenanceIntent {
    root: PathBuf,
    statements: [SelectedStatement; 2],
    frontend: RoleLaunchReference,
}

impl Scenario for OptimizeTargetReplacement {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "table_maintenance",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "table-maintenance/target-replacement"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        // REST admits detached maintenance, allowing replacement while the
        // worker is paused. Hadoop requires the submitting statement to await
        // actual worker exit to preserve its single-writer contract.
        let (rest, launch) = ManagedMvRestFixture::start(scenario_root, CATALOG)
            .context("start private REST fixture for optimize target replacement")?;
        let mut fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance REST fixture lock poisoned"))?;
        if fixture.is_some() {
            bail!("maintenance REST fixture was initialized more than once");
        }
        *fixture = Some(rest);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        ensure!(
            cluster_size == 3,
            "maintenance race requires native 1FE+3BE"
        );
        let controlled = binary;
        let mut intent = self
            .intent
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance intent lock poisoned"))?;
        ensure!(intent.is_none(), "maintenance intent already frozen");
        let mut plan = ClusterTestSessionPlan::new(original_deadline)?;
        let root = plan.reserve_transport("maintenance-rendezvous")?;
        let frontend = plan.reserve_launch(RoleSlot::Frontend)?;
        let successor = plan.reserve_launch(RoleSlot::Frontend)?;
        plan.authorize_ordinary_launch(&successor, binary.to_path_buf())?;
        for index in 0..cluster_size {
            let backend = plan.reserve_launch(RoleSlot::Backend(index))?;
            plan.authorize_ordinary_launch(&backend, binary.to_path_buf())?;
        }
        let mut statements: [SelectedStatement; 2] = std::array::from_fn(|_| SelectedStatement {
            sql: OPTIMIZE_SQL.to_owned(),
            token: String::new(),
            faults: vec![],
            deadline: original_deadline,
        });
        let mut grants = vec![DomainGrant {
            domain: "statement-observation".into(),
            grant_id: "maintenance-statements".into(),
            parameters: json!({"action":"observe","bearer_secret":plan.fresh_domain_secret()?}),
        }];
        for (ordinal, selected) in statements.iter_mut().enumerate() {
            selected.token = plan.fresh_domain_secret()?;
            for action in ["hold-before-rebind", "dispatch-count"] {
                grants.push(DomainGrant {
                    domain: "table-maintenance".into(), grant_id: format!("maintenance-{ordinal}-{action}"),
                    parameters: json!({"action":action,"root":root,"statement_run_token":selected.token,
                        "catalog":CATALOG,"namespace":"ns","table":"orders"}),
                });
            }
        }
        plan.authorize_launch(
            &frontend,
            grants,
            vec![
                ("novarocks-server".into(), "test-controls".into()),
                (
                    "novarocks-frontend-application".into(),
                    "test-controls".into(),
                ),
            ],
        )?;
        plan.set_launch_binary(&frontend, controlled.to_path_buf())?;
        *intent = Some(MaintenanceIntent {
            root,
            statements,
            frontend,
        });
        Ok(Some(plan))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = CATALOG;
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance REST fixture lock poisoned"))?
            .as_ref()
            .context("maintenance REST fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let intent = self
            .intent
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance intent lock poisoned"))?
            .clone()
            .context("maintenance finite intent is missing")?;
        let conn = connect(context)?;
        let phase = conn.run(|conn| {
        setup_orders_fixture(context, conn, catalog, &create_catalog_sql)?;

        submit_optimize(context, conn, &intent.statements[0], "submit optimize for the original table incarnation")?;
        let first = wait_for_ready_marker(context, &intent, 0, None)?;
        context.action("observed process-local optimize claim before its first rebind");

        execute(
            context,
            conn,
            "drop original optimize target incarnation",
            "DROP TABLE orders FORCE",
        )?;
        execute(
            context,
            conn,
            "recreate optimize target under the same logical name",
            "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
        )?;
        execute(
            context,
            conn,
            "seed replacement optimize target table",
            "INSERT INTO orders VALUES (9, 90)",
        )?;
        write_resume(&first)?;
        context.action("released optimize rebind barrier after same-name replacement");

        wait_for_job_state(context, conn, catalog, first.job_id, "TARGET_REPLACED")?;
        assert_dispatch_count(context, &first, 0)?;
        context.action("verified replacement became TARGET_REPLACED with zero provider dispatch");

        submit_optimize(context, conn, &intent.statements[1], "submit a new optimize to prove the terminal job released its active fence")?;
        let second = wait_for_ready_marker(context, &intent, 1, Some(first.job_id))?;
        write_resume(&second)?;
        context.action("a second process-local optimize claim acquired the same table fence");
        wait_for_terminal_job(context, conn, catalog, second.job_id)?;

        Ok(first.job_id)
        });
        let first_job_id = match phase {
            Ok(job_id) => job_id,
            Err(primary) => {
                let released = release_ready_barriers(&intent);
                let stopped = context.shutdown();
                return with_cleanup(with_cleanup(Err(primary), released), stopped);
            }
        };
        restart_frontend(
            context,
            "restart FE after process-local TARGET_REPLACED transition",
        )?;
        let conn = connect(context)?;
        conn.run(|conn| {
            select_catalog_and_database(context, conn, catalog)?;
            assert_job_not_listed(context, conn, catalog, first_job_id)?;
            context.action("verified the restarted FE discards process-local optimize history in native 1FE+3BE");
            Ok(())
        })
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance REST fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let be_count = context.handle().be_count();
    if be_count != 3 {
        bail!(
            "{} requires native 1FE+3BE, but runner launched {} BE(s)",
            context.name(),
            be_count
        );
    }
    context.action("confirmed native 1FE+3BE topology");
    Ok(())
}

fn connect(context: &mut ScenarioContext) -> Result<mysql_actor::DeadlineConnection> {
    context.action("connect through public MySQL protocol");
    mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )
}

fn submit_optimize(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    selected: &SelectedStatement,
    action: &str,
) -> Result<()> {
    context.action(action);
    let observation = selected.arm(context, conn.connection_id(), OPTIMIZE_SQL)?;
    let result = conn.query_drop(OPTIMIZE_SQL).map_err(anyhow::Error::from);
    let drained = context
        .handle()
        .drain_query_lifecycle_statement_binding(&observation, selected.deadline);
    with_cleanup(result, drained)
}
fn with_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => {
            Err(anyhow::anyhow!("{primary:#}; cleanup failed: {cleanup:#}"))
        }
    }
}
fn release_ready_barriers(intent: &MaintenanceIntent) -> Result<()> {
    let mut result = Ok(());
    for selected in &intent.statements {
        if intent
            .root
            .join(format!("{}.before-rebind.ready", selected.token))
            .exists()
        {
            let resumed = fs::write(
                intent
                    .root
                    .join(format!("{}.before-rebind.resume", selected.token)),
                "resume\n",
            )
            .context("release owned maintenance barrier during error cleanup");
            result = with_cleanup(result, resumed);
        }
    }
    result
}

fn setup_orders_fixture(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    create_catalog_sql: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "create private REST Iceberg catalog",
        create_catalog_sql,
    )?;
    execute(
        context,
        conn,
        "create maintenance fixture namespace",
        &format!("CREATE DATABASE {catalog}.ns"),
    )?;
    select_catalog_and_database(context, conn, catalog)?;
    execute(
        context,
        conn,
        "create maintenance target table",
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )?;
    execute(
        context,
        conn,
        "seed maintenance target table",
        "INSERT INTO orders VALUES (1, 10), (2, 20)",
    )
}

fn select_catalog_and_database(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "select maintenance catalog",
        &format!("SET CATALOG {catalog}"),
    )?;
    execute(context, conn, "select maintenance namespace", "USE ns")
}

fn execute(context: &mut ScenarioContext, conn: &mut Conn, action: &str, sql: &str) -> Result<()> {
    context.remaining(action)?;
    context.action(action);
    conn.query_drop(sql)
        .with_context(|| format!("{action}: {sql}"))
}

fn query<T: FromRow>(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    sql: &str,
    action: &str,
) -> Result<Vec<T>> {
    context.remaining(action)?;
    context.action(action);
    conn.query(sql).with_context(|| format!("{action}: {sql}"))
}

#[derive(Debug)]
struct BarrierPaths {
    job_id: i64,
    resume: PathBuf,
    dispatch_count: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadyReceipt {
    schema_version: u32,
    job_id: i64,
    work_id: u64,
    phase: String,
    catalog: String,
    namespace: String,
    table: String,
    object_id: Vec<u8>,
    launch_id: String,
    #[serde(deserialize_with = "read_ready_nomination")]
    nomination: StatementWorkNomination,
}

fn read_ready_nomination<'de, D>(
    deserializer: D,
) -> std::result::Result<StatementWorkNomination, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct WireNomination {
        target_session_id: String,
        statement_run_token: String,
        frontend_process_id: String,
        connection_id: u32,
        session_key: u64,
        statement_generation: u64,
    }
    let wire = WireNomination::deserialize(deserializer)?;
    Ok(StatementWorkNomination {
        target_session_id: wire.target_session_id,
        statement_run_token: wire.statement_run_token,
        frontend_process_id: wire.frontend_process_id,
        connection_id: wire.connection_id,
        session_key: wire.session_key,
        statement_generation: wire.statement_generation,
    })
}

fn wait_for_ready_marker(
    context: &mut ScenarioContext,
    intent: &MaintenanceIntent,
    ordinal: usize,
    excluded_job_id: Option<i64>,
) -> Result<BarrierPaths> {
    context.action("wait for STAT-2F optimize before-rebind barrier");
    let selected = &intent.statements[ordinal];
    let deadline = context.deadline();
    let ready = intent
        .root
        .join(format!("{}.before-rebind.ready", selected.token));
    loop {
        if ready.exists() {
            let metadata = fs::symlink_metadata(&ready)?;
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() <= 8192,
                "maintenance ready receipt is not bounded regular data"
            );
            let receipt: ReadyReceipt = serde_json::from_slice(&fs::read(&ready)?)
                .context("decode exact maintenance receipt")?;
            receipt.nomination.validate()?;
            let role = RoleProcessReceipt::read_private(&intent.frontend.process_receipt)?;
            ensure!(
                receipt.schema_version == 1
                    && receipt.job_id > 0
                    && receipt.work_id > 0
                    && Some(receipt.job_id) != excluded_job_id
                    && receipt.phase == "after-claim-before-rebind"
                    && receipt.catalog == CATALOG
                    && receipt.namespace == "ns"
                    && receipt.table == "orders"
                    && !receipt.object_id.is_empty()
                    && receipt.nomination.statement_run_token == selected.token
                    && receipt.nomination.target_session_id == intent.frontend.session_id
                    && role.session_id == intent.frontend.session_id
                    && role.launch_id == receipt.launch_id
                    && role.role == novarocks_failpoint::session::SessionRole::Fe
                    && role.process_id == receipt.nomination.frontend_process()?.to_string(),
                "maintenance ready receipt differs from its exact finite job/role target"
            );
            return Ok(BarrierPaths {
                job_id: receipt.job_id,
                resume: intent
                    .root
                    .join(format!("{}.before-rebind.resume", selected.token)),
                dispatch_count: intent
                    .root
                    .join(format!("{}.dispatch-count", selected.token)),
            });
        }
        ensure!(
            Instant::now() < deadline,
            "timed out waiting for exact maintenance before-rebind receipt"
        );
        context.remaining("wait for STAT-2F optimize before-rebind barrier")?;
        thread::sleep(POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
    }
}

fn write_resume(paths: &BarrierPaths) -> Result<()> {
    fs::write(&paths.resume, "resume\n").with_context(|| {
        format!(
            "write maintenance resume trigger {}",
            paths.resume.display()
        )
    })
}

fn assert_dispatch_count(
    context: &mut ScenarioContext,
    paths: &BarrierPaths,
    expected: u64,
) -> Result<()> {
    let raw = fs::read_to_string(&paths.dispatch_count).with_context(|| {
        format!(
            "read provider dispatch counter {}",
            paths.dispatch_count.display()
        )
    })?;
    let actual = raw.trim().parse::<u64>().with_context(|| {
        format!(
            "parse provider dispatch counter {}",
            paths.dispatch_count.display()
        )
    })?;
    if actual != expected {
        bail!(
            "optimize job {} provider dispatch count was {actual}, expected {expected}; {}",
            paths.job_id,
            context.diagnostics()
        );
    }
    Ok(())
}

fn wait_for_job_state(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    job_id: i64,
    expected_state: &str,
) -> Result<()> {
    context.action(format!(
        "wait for optimize job {job_id} state {expected_state}"
    ));
    loop {
        let jobs = list_optimize_jobs(context, conn, catalog)?;
        if let Some(state) = jobs
            .iter()
            .find(|(observed_job_id, _)| *observed_job_id == job_id)
            .map(|(_, state)| state.as_str())
        {
            if state == expected_state {
                return Ok(());
            }
            if !matches!(state, "PENDING" | "RUNNING") {
                bail!(
                    "optimize job {job_id} reached state {state}, expected {expected_state}; {}",
                    context.diagnostics()
                );
            }
        }
        context.remaining("wait for durable optimize job state")?;
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_terminal_job(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    job_id: i64,
) -> Result<()> {
    context.action(format!(
        "wait for resubmitted optimize job {job_id} terminal state"
    ));
    loop {
        let jobs = list_optimize_jobs(context, conn, catalog)?;
        if let Some(state) = jobs
            .iter()
            .find(|(observed_job_id, _)| *observed_job_id == job_id)
            .map(|(_, state)| state.as_str())
        {
            if matches!(state, "FINISHED" | "FAILED" | "TARGET_REPLACED") {
                return Ok(());
            }
            if !matches!(state, "PENDING" | "RUNNING") {
                bail!(
                    "resubmitted optimize job {job_id} reached unexpected state {state}; {}",
                    context.diagnostics()
                );
            }
        }
        context.remaining("wait for resubmitted optimize job terminal state")?;
        thread::sleep(POLL_INTERVAL);
    }
}

fn assert_job_not_listed(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    job_id: i64,
) -> Result<()> {
    let jobs = list_optimize_jobs(context, conn, catalog)?;
    if jobs
        .iter()
        .any(|(observed_job_id, _)| *observed_job_id == job_id)
    {
        bail!(
            "optimize job {job_id} survived FE restart even though optimize job state is process-local; {}",
            context.diagnostics()
        );
    }
    Ok(())
}

fn list_optimize_jobs(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
) -> Result<Vec<(i64, String)>> {
    let rows: Vec<Row> = query(
        context,
        conn,
        &format!(
            "SHOW ALTER TABLE OPTIMIZE FROM {catalog}.ns WHERE TableName = 'orders' ORDER BY CreateTime DESC"
        ),
        "read current-process optimize job state",
    )?;
    rows.into_iter()
        .map(|row| {
            let job_id = row
                .get::<String, _>(0)
                .context("SHOW ALTER TABLE OPTIMIZE JobId column")?
                .parse::<i64>()
                .context("parse SHOW ALTER TABLE OPTIMIZE JobId")?;
            let state = row
                .get::<String, _>(2)
                .context("SHOW ALTER TABLE OPTIMIZE State column")?;
            Ok((job_id, state))
        })
        .collect()
}

fn restart_frontend(context: &mut ScenarioContext, action: &str) -> Result<()> {
    context.action(action);
    let deadline = context.deadline();
    let action = action.to_owned();
    context
        .handle()
        .restart_fe_until(deadline)
        .with_context(|| action)
}
