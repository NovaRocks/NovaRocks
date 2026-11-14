use super::mv_uea7::ManagedMvRestFixture;
use super::native_sql_controls::SelectedStatement;
use crate::actors::sql::{SqlSession, StatementFailure, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::{
    TestClusterOwner,
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
        // A private REST fixture admits the concurrent DROP/CREATE writer
        // while this statement is held before its original planning load.
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
        let mut conn = connect(context)?;
        let phase = (|| {
            setup_orders_fixture(context, &mut conn, catalog, &create_catalog_sql)?;
            let (first, paths) = execute_held_optimize(
                context,
                &mut conn,
                &intent,
                0,
                "execute optimize for the original table incarnation",
                |context, conn, _paths| {
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
                    )
                },
            )?;
            let error = match first {
                Err(StatementFailure::Trino(error)) => error,
                Err(other) => bail!("replaced optimize target returned a non-SQL failure: {other}"),
                Ok(result) => bail!("replaced optimize target unexpectedly succeeded: {result:?}"),
            };
            ensure!(
                error.message.contains("TARGET_REPLACED"),
                "replaced optimize target lost its public binding failure: {error:?}"
            );
            assert_dispatch_count(context, &paths, 0)?;
            context.action("verified the original statement failed TARGET_REPLACED with zero provider dispatch");
            let (second, paths) = execute_held_optimize(
                context,
                &mut conn,
                &intent,
                1,
                "execute optimize again after the original statement actually closed",
                |_, _, _| Ok(()),
            )?;
            let second =
                second.context("optimize replacement table after releasing the original fence")?;
            assert_zero_optimize_metrics(&second)?;
            assert_dispatch_count(context, &paths, 0)?;
            context.action("a second synchronous optimize acquired the released table fence and returned three zero metrics");
            Ok(())
        })();
        match phase {
            Ok(()) => Ok(()),
            Err(primary) => {
                let released = release_ready_barriers(&intent);
                let stopped = context.shutdown();
                with_cleanup(with_cleanup(Err(primary), released), stopped)
            }
        }
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

fn connect(context: &mut ScenarioContext) -> Result<SqlSession> {
    context.action("connect through public Trino protocol");
    Ok(SqlSession::new(
        context.sql_user(),
        context.trino_port(),
        context.deadline(),
    )?)
}

fn execute_held_optimize(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    intent: &MaintenanceIntent,
    ordinal: usize,
    action: &str,
    held: impl FnOnce(&mut ScenarioContext, &mut SqlSession, &BarrierPaths) -> Result<()>,
) -> Result<(
    std::result::Result<StatementResult, StatementFailure>,
    BarrierPaths,
)> {
    context.action(action);
    let selected = &intent.statements[ordinal];
    let pending = conn.submit(OPTIMIZE_SQL)?;
    let (pending, observation) = selected.arm_pending(context, pending, OPTIMIZE_SQL)?;
    let cancel = pending.cancel_handle();
    let mut reader_session = conn.clone();
    // This scope joins the real reader on every ordinary/error return. The
    // main actor keeps a different session for the concurrent target writer.
    thread::scope(|scope| {
        let reader = scope.spawn(move || reader_session.run_pending(pending));
        let phase: Result<BarrierPaths> = (|| {
            let paths = wait_for_ready_marker(context, intent, ordinal, || reader.is_finished())?;
            held(context, conn, &paths)?;
            write_resume(&paths)?;
            Ok(paths)
        })();
        let released = if phase.is_err() {
            release_ready_barriers(intent)
        } else {
            Ok(())
        };
        let cancelled = if phase.is_err() {
            cancel.delete(selected.deadline).map_err(Into::into)
        } else {
            Ok(())
        };
        let joined = reader
            .join()
            .map_err(|_| anyhow::anyhow!("original optimize SQL reader panicked"));
        let response_needs_cancel = match joined.as_ref() {
            Err(_) => true,
            Ok(result) => {
                matches!(result, Err(error) if !matches!(error, StatementFailure::Trino(_)))
            }
        };
        let cancel_after_read = if phase.is_ok() && response_needs_cancel {
            cancel.delete(selected.deadline).map_err(Into::into)
        } else {
            Ok(())
        };
        let drained = context
            .handle()
            .drain_query_lifecycle_statement_binding(&observation, selected.deadline);
        let cleanup = with_cleanup(
            with_cleanup(with_cleanup(released, cancelled), cancel_after_read),
            drained,
        );
        let phase = match (&joined, phase) {
            (Ok(Err(original)), Err(primary)) => {
                Err(primary.context(format!("original optimize SQL reader: {original}")))
            }
            (_, phase) => phase,
        };
        let paths = with_cleanup(phase, cleanup)?;
        Ok((joined?, paths))
    })
}
fn assert_zero_optimize_metrics(result: &StatementResult) -> Result<()> {
    ensure!(
        result.is_result_set() && result.columns.len() == 2,
        "OPTIMIZE omitted its metric result set: {result:?}"
    );
    for (column, name, ty) in [
        (&result.columns[0], "metric_name", "varchar"),
        (&result.columns[1], "metric_value", "bigint"),
    ] {
        ensure!(
            column.name == name && column.type_signature.raw_type == ty,
            "OPTIMIZE metric declaration differs: {column:?}"
        );
    }
    let expected = [
        "rewritten_data_files_count",
        "removed_delete_files_count",
        "added_data_files_count",
    ];
    ensure!(
        result.rows.len() == expected.len(),
        "OPTIMIZE omitted a metric row: {result:?}"
    );
    for (row, metric) in result.rows.iter().zip(expected) {
        ensure!(
            matches!(row.as_slice(), [TrinoValue::Text(name), TrinoValue::Integer(0)] if name == metric),
            "singleton OPTIMIZE changed independently expected zero metric {metric}: {row:?}"
        );
    }
    Ok(())
}
fn with_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => {
            Err(primary.context(format!("cleanup failed: {cleanup:#}")))
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
    conn: &mut SqlSession,
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
    conn: &mut SqlSession,
    catalog: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "select maintenance catalog and namespace",
        &format!("USE {catalog}.ns"),
    )
}

fn execute(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    action: &str,
    sql: &str,
) -> Result<()> {
    context.remaining(action)?;
    context.action(action);
    conn.execute(sql)
        .map(|_| ())
        .with_context(|| format!("{action}: {sql}"))
}

#[derive(Debug)]
struct BarrierPaths {
    work_id: u64,
    resume: PathBuf,
    dispatch_count: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadyReceipt {
    schema_version: u32,
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
        session_key: u64,
        statement_generation: u64,
    }
    let wire = WireNomination::deserialize(deserializer)?;
    Ok(StatementWorkNomination {
        target_session_id: wire.target_session_id,
        statement_run_token: wire.statement_run_token,
        frontend_process_id: wire.frontend_process_id,
        session_key: wire.session_key,
        statement_generation: wire.statement_generation,
    })
}

fn wait_for_ready_marker(
    context: &mut ScenarioContext,
    intent: &MaintenanceIntent,
    ordinal: usize,
    reader_finished: impl Fn() -> bool,
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
                receipt.schema_version == 2
                    && receipt.work_id > 0
                    && receipt.phase == "after-capture-before-planning"
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
                "maintenance ready receipt differs from its exact finite statement/role target"
            );
            return Ok(BarrierPaths {
                work_id: receipt.work_id,
                resume: intent
                    .root
                    .join(format!("{}.before-rebind.resume", selected.token)),
                dispatch_count: intent
                    .root
                    .join(format!("{}.dispatch-count", selected.token)),
            });
        }
        ensure!(
            !reader_finished(),
            "original OPTIMIZE exited before reaching its authorized planning barrier"
        );
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
            "optimize statement work {} provider dispatch count was {actual}, expected {expected}; {}",
            paths.work_id,
            context.diagnostics()
        );
    }
    Ok(())
}
