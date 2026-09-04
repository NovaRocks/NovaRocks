use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use ::mysql::prelude::{FromRow, Queryable};
use ::mysql::{Conn, Row};
use anyhow::{Context, Result, bail};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, ServerHandle,
};
use novarocks_connector_iceberg::access_binding::IcebergReadBinding;
use novarocks_connector_iceberg::catalog_config::parse_catalog_configuration;
use novarocks_connector_iceberg::catalog_runtime::build_hadoop_catalog;
use novarocks_connector_iceberg::iceberg::{Catalog, TableIdent};
use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

const POLL_INTERVAL: Duration = Duration::from_millis(100);

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(MvStateStoreRestart),
        Box::new(MvSchedulerRecovery),
        Box::new(MvStagedPublishedRecovery),
        Box::new(MvFirstRefreshStaging),
        Box::new(MvBaseIdentityReplacement),
        Box::new(MvLakePublicationRestartRebuild),
    ]
}

struct MvStateStoreRestart;

impl Scenario for MvStateStoreRestart {
    fn name(&self) -> &'static str {
        "mv/state-store-restart"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_restart";
        let warehouse = context.runtime_dir().join("warehouse");
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, &warehouse, true)?;

        execute(
            context,
            &mut conn,
            "create StateStore-backed materialized view",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read first MV publication before FE restart",
        )?;
        drop(conn);

        restart_frontend(context, "restart FE with the persisted StateStore")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read existing MV after FE restart",
        )?;
        refresh_after_owner_crash(context, &mut conn, "orders_mv")?;
        execute(
            context,
            &mut conn,
            "create a second MV after StateStore recovery",
            "CREATE MATERIALIZED VIEW orders_mv_2 DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
        )?;
        let views: Vec<Row> = query(
            context,
            &mut conn,
            "SHOW MATERIALIZED VIEWS FROM ns",
            "list MV definitions after FE restart",
        )?;
        let names = views
            .iter()
            .map(|row| {
                row.get::<String, _>(0)
                    .context("SHOW MATERIALIZED VIEWS name column")
            })
            .collect::<Result<Vec<_>>>()?;
        if names != ["orders_mv", "orders_mv_2"] {
            bail!(
                "MV definitions did not survive StateStore restart: names={names:?}; {}",
                context.diagnostics()
            );
        }
        context
            .action("StateStore-backed MV definitions and visible publication survived FE restart");
        Ok(())
    }
}

struct MvSchedulerRecovery;

impl Scenario for MvSchedulerRecovery {
    fn name(&self) -> &'static str {
        "mv/scheduler-recovery"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let barrier_dir = scenario_root.join("mv-scheduler-barrier");
        fs::create_dir_all(&barrier_dir).with_context(|| {
            format!(
                "create scheduler barrier directory {}",
                barrier_dir.display()
            )
        })?;
        let mut child_environment = CrossProcessChildEnvironment::default();
        child_environment.fe.insert(
            "NOVAROCKS_MVX4_SCHEDULER_TEST_DIR".to_string(),
            barrier_dir.to_string_lossy().into_owned(),
        );
        Ok(ScenarioLaunchConfig {
            child_environment,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(
                    r#"
[standalone_server]
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 100
mv_refresh_scheduler_max_concurrent = 1
mv_refresh_scheduler_failure_backoff_ms = 100
mv_refresh_scheduler_max_failure_backoff_ms = 1000
"#
                    .to_string(),
                ),
                be: None,
                ..Default::default()
            },
            native_trust_fixture: Default::default(),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let barrier_dir = context.scenario_root().join("mv-scheduler-barrier");
        let hold_trigger = barrier_dir.join("mvx4-scheduler-hold.trigger");
        let _hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        context.action("armed scheduler admission barrier");

        let catalog = "system_mv_scheduler";
        let warehouse = context.runtime_dir().join("warehouse");
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, &warehouse, false)?;
        execute(
            context,
            &mut conn,
            "seed asynchronous MV source rows",
            "INSERT INTO orders VALUES (1, 10), (2, 20)",
        )?;
        execute(
            context,
            &mut conn,
            "create first asynchronous scheduler MV",
            "CREATE MATERIALIZED VIEW orders_mv_a DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
        )?;
        execute(
            context,
            &mut conn,
            "create second asynchronous scheduler MV",
            "CREATE MATERIALIZED VIEW orders_mv_b DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
        )?;
        wait_for_marker_count(
            context,
            &barrier_dir,
            1,
            "observe first scheduler admission",
        )?;
        thread::sleep(POLL_INTERVAL * 3);
        let admitted = marker_count(&barrier_dir)?;
        if admitted != 1 {
            bail!(
                "scheduler max_concurrent_refreshes=1 admitted {admitted} refreshes while held; {}",
                context.diagnostics()
            );
        }
        context.action("verified scheduler permit admitted exactly one native refresh");

        _hold.remove()?;
        context.action("released scheduler admission barrier");
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_a ORDER BY k1",
            &[(1, 10), (2, 20)],
            "wait for first scheduler MV initial catch-up",
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
            &[(1, 10), (2, 20)],
            "wait for second scheduler MV initial catch-up",
        )?;
        execute(
            context,
            &mut conn,
            "mutate asynchronous MV source rows",
            "INSERT INTO orders VALUES (3, 30)",
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_a ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "wait for first scheduler MV incremental catch-up",
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "wait for second scheduler MV incremental catch-up",
        )?;

        clear_scheduler_markers(&barrier_dir)?;
        let recovery_hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        execute(
            context,
            &mut conn,
            "create a scheduler MV for FE recovery",
            "CREATE MATERIALIZED VIEW orders_mv_recovery DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND AS SELECT k1, v2 FROM orders",
        )?;
        wait_for_marker_count(
            context,
            &barrier_dir,
            1,
            "hold scheduler refresh before FE recovery",
        )?;
        execute(
            context,
            &mut conn,
            "add rows while scheduler refresh is held",
            "INSERT INTO orders VALUES (4, 40)",
        )?;
        let _transient_preparation_fault = FileTrigger::create(
            &barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.trigger"),
            "inject one typed connector unavailability after FE restart\n",
        )?;
        context.action("armed one transient scheduler preparation fault for FE recovery");
        drop(conn);
        context.action("terminate held scheduler FE attempt for recovery");
        context
            .handle()
            .kill_fe()
            .context("kill FE during held scheduler refresh")?;
        recovery_hold.remove()?;
        restart_frontend(context, "restart FE after interrupted scheduler refresh")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_recovery ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30), (4, 40)],
            "wait for scheduler recovery to catch up durable MV",
        )?;
        context.action("scheduler recovered the interrupted durable refresh after FE restart");
        Ok(())
    }
}

struct MvStagedPublishedRecovery;

impl Scenario for MvStagedPublishedRecovery {
    fn name(&self) -> &'static str {
        "mv/staged-published-recovery"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-recovery-faults");
        fs::create_dir_all(&fault_dir).with_context(|| {
            format!("create MV recovery fault directory {}", fault_dir.display())
        })?;
        let mut child_environment = CrossProcessChildEnvironment::default();
        child_environment.fe.insert(
            "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR".to_string(),
            fault_dir.to_string_lossy().into_owned(),
        );
        Ok(ScenarioLaunchConfig {
            child_environment,
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fault_dir = context.scenario_root().join("mv-recovery-faults");
        let catalog = "system_mv_recovery";
        let warehouse = context.runtime_dir().join("warehouse");
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, &warehouse, true)?;
        execute(
            context,
            &mut conn,
            "create MV for staged and published recovery",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
        )?;

        let staged = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-write-committed.trigger"),
            "token=staged-before-publication\n",
        )?;
        context.action("armed staged-before-publication crash barrier");
        let staged_refresh = spawn_refresh(
            context.mysql_user().to_string(),
            context.mysql_port(),
            catalog,
            "orders_mv",
            context.remaining("start staged recovery refresh")?,
        );
        wait_for_fe_marker(
            context,
            "NOVAROCKS_MV_RECOVERY_PHASE phase=write-committed token=staged-before-publication",
            "wait for staged recovery barrier",
        )?;
        context.action("kill FE at staged-only recovery window");
        context
            .handle()
            .kill_fe()
            .context("kill FE at staged recovery barrier")?;
        staged.remove()?;
        expect_refresh_failure(
            context,
            staged_refresh,
            "staged refresh client after FE termination",
        )?;
        restart_frontend(context, "restart FE after staged-only crash")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[],
            "verify staged-only attempt did not publish main MV",
        )?;
        refresh_after_owner_crash(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify recovered first publication",
        )?;
        execute(
            context,
            &mut conn,
            "add source row before publication-committed crash",
            "INSERT INTO orders VALUES (3, 30)",
        )?;

        let published = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-publication-committed.trigger"),
            "token=published-before-cleanup\n",
        )?;
        context.action("armed publication-committed crash barrier");
        let published_refresh = spawn_refresh(
            context.mysql_user().to_string(),
            context.mysql_port(),
            catalog,
            "orders_mv",
            context.remaining("start publication recovery refresh")?,
        );
        wait_for_fe_marker(
            context,
            "NOVAROCKS_MV_RECOVERY_PHASE phase=publication-committed token=published-before-cleanup",
            "wait for publication recovery barrier",
        )?;
        context.action("kill FE after MV main publication and before cleanup");
        context
            .handle()
            .kill_fe()
            .context("kill FE at publication recovery barrier")?;
        published.remove()?;
        expect_refresh_failure(
            context,
            published_refresh,
            "publication refresh client after FE termination",
        )?;
        restart_frontend(context, "restart FE after publication-committed crash")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "verify published snapshot remains visible after recovery",
        )?;
        refresh_after_owner_crash(context, &mut conn, "orders_mv")?;
        context.action("staged and published crash windows converged through public MV behavior");
        Ok(())
    }
}

struct MvFirstRefreshStaging;

impl Scenario for MvFirstRefreshStaging {
    fn name(&self) -> &'static str {
        "mv/first-refresh-staging"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_staging";
        let warehouse = context.runtime_dir().join("warehouse");
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, &warehouse, true)?;

        execute(
            context,
            &mut conn,
            "create first-refresh projection MV",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify staged projection first refresh publishes only completed main snapshot",
        )?;

        execute(
            context,
            &mut conn,
            "create first-refresh aggregate MV",
            "CREATE MATERIALIZED VIEW orders_agg_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, SUM(v2) AS total_v2 FROM orders GROUP BY k1",
        )?;
        refresh(context, &mut conn, "orders_agg_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total_v2 FROM orders_agg_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify staged aggregate first refresh",
        )?;

        execute(
            context,
            &mut conn,
            "create MV used to prove failed first refresh is not published",
            "CREATE MATERIALIZED VIEW orders_start_fault_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
        )?;
        context.handle().arm_start_ack_suppress(0)?;
        context.action("armed native StartAck suppression for MV first refresh");
        let refresh_result = conn.query_drop("REFRESH MATERIALIZED VIEW orders_start_fault_mv");
        let cleanup_result = context.handle().clear_query_lifecycle_faults();
        cleanup_result.context("clear native StartAck suppression")?;
        let error = refresh_result.expect_err("suppressed native start must fail MV first refresh");
        if error.to_string().is_empty() {
            bail!("suppressed native start returned an empty MV refresh error");
        }
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_start_fault_mv ORDER BY k1",
            &[],
            "verify failed first refresh never publishes a partial main snapshot",
        )?;
        context.action("validated native first-refresh staging publishes no partial main snapshot");
        Ok(())
    }
}

struct MvBaseIdentityReplacement;

impl Scenario for MvBaseIdentityReplacement {
    fn name(&self) -> &'static str {
        "mv/base-identity-replacement"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_base_identity";
        let warehouse = context.runtime_dir().join("warehouse");
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, &warehouse, true)?;
        execute(
            context,
            &mut conn,
            "create MV with a durable base-object binding",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read publication before replacing its base table",
        )?;

        drop(conn);
        externally_drop_hadoop_table(context, catalog, &warehouse, "ns", "orders")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        execute(
            context,
            &mut conn,
            "recreate base table under the same logical name",
            "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
        )?;
        execute(
            context,
            &mut conn,
            "seed replacement base table incarnation",
            "INSERT INTO orders VALUES (9, 90)",
        )?;
        drop(conn);

        restart_frontend(context, "restart FE after same-name base replacement")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_mv_not_recovered_after_base_replacement(context, &mut conn, "orders_mv")?;
        context.action(
            "verified FE restart fail-closed removes the MV rather than bind a same-name replacement base",
        );
        Ok(())
    }
}

struct MvLakePublicationRestartRebuild;

impl Scenario for MvLakePublicationRestartRebuild {
    fn name(&self) -> &'static str {
        "mv/lake-publication-restart-rebuild"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_lake_rebuild";
        let warehouse = context.runtime_dir().join("warehouse");
        let mut conn = connect(context)?;
        setup_orders_fixture(context, &mut conn, catalog, &warehouse, true)?;
        execute(
            context,
            &mut conn,
            "create MV with a lake-native descriptor",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read newly published lake-native MV before FE restart",
        )?;
        drop(conn);

        restart_frontend(context, "restart FE before lake-native MV cache rebuild")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        let rows: Vec<Row> = query(
            context,
            &mut conn,
            &format!(
                "CALL {catalog}.system.novarocks_imv_stateless_rebuild(table => 'ns.orders_mv', level => 'full')"
            ),
            "clear and rebuild the newly written MV cache from its lake package",
        )?;
        let report = rows
            .first()
            .context("lake-native rebuild procedure returned no report row")?;
        let level = report
            .get::<String, _>(0)
            .context("lake-native rebuild AvailableLevel column")?;
        let source = report
            .get::<String, _>(4)
            .context("lake-native rebuild RebuildSource column")?;
        if level != "full" || source != "lake" {
            bail!(
                "unexpected lake-native rebuild report level={level:?}, source={source:?}; {}",
                context.diagnostics()
            );
        }
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read MV restored from its new-format lake publication",
        )?;
        context.action(
            "verified a post-restart full rebuild restores the new-format descriptor and publication from lake",
        );
        Ok(())
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

fn connect(context: &mut ScenarioContext) -> Result<Conn> {
    let timeout = context.remaining("connect MySQL client")?;
    context.action("connect through public MySQL protocol");
    mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)
}

fn setup_orders_fixture(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    warehouse: &Path,
    seed_rows: bool,
) -> Result<()> {
    fs::create_dir_all(warehouse)
        .with_context(|| format!("create MV warehouse {}", warehouse.display()))?;
    execute(
        context,
        conn,
        "create Hadoop Iceberg catalog",
        &format!(
            "CREATE EXTERNAL CATALOG {catalog} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
            warehouse.display()
        ),
    )?;
    execute(
        context,
        conn,
        "create MV fixture namespace",
        &format!("CREATE DATABASE {catalog}.ns"),
    )?;
    select_catalog_and_database(context, conn, catalog)?;
    execute(
        context,
        conn,
        "create MV source table",
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )?;
    if seed_rows {
        execute(
            context,
            conn,
            "seed MV source table",
            "INSERT INTO orders VALUES (1, 10), (2, 20)",
        )?;
    }
    Ok(())
}

fn select_catalog_and_database(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "select MV catalog",
        &format!("SET CATALOG {catalog}"),
    )?;
    execute(context, conn, "select MV namespace", "USE ns")
}

fn externally_drop_hadoop_table(
    context: &mut ScenarioContext,
    catalog_name: &str,
    warehouse: &Path,
    namespace: &str,
    table: &str,
) -> Result<()> {
    context.remaining("drop base table through external Hadoop catalog client")?;
    context.action("drop original base table through external Hadoop catalog client");
    let configuration = parse_catalog_configuration(
        catalog_name,
        &[
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
            (
                "iceberg.catalog.warehouse".to_string(),
                warehouse.to_string_lossy().into_owned(),
            ),
        ],
    )
    .map_err(anyhow::Error::msg)
    .context("configure external Hadoop catalog client")?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("create external Hadoop catalog runtime")?;
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
        Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
    );
    let catalog = build_hadoop_catalog(&configuration, binding)
        .map_err(anyhow::Error::msg)
        .context("construct external Hadoop catalog client")?;
    let table = TableIdent::from_strs([namespace, table])
        .context("construct external Hadoop table identifier")?;
    runtime
        .block_on(catalog.drop_table(&table))
        .context("drop original table through external Hadoop catalog client")
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

fn refresh(context: &mut ScenarioContext, conn: &mut Conn, mv: &str) -> Result<()> {
    execute(
        context,
        conn,
        "refresh materialized view",
        &format!("REFRESH MATERIALIZED VIEW {mv}"),
    )
}

fn assert_mv_not_recovered_after_base_replacement(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    mv: &str,
) -> Result<()> {
    context.remaining("verify MV is not recovered after same-name base replacement")?;
    context.action("verify MV is not recovered after same-name base replacement");
    let views: Vec<Row> = conn
        .query("SHOW MATERIALIZED VIEWS FROM ns")
        .context("list MVs after same-name base replacement")?;
    let names = views
        .iter()
        .map(|row| {
            row.get::<String, _>(0)
                .context("SHOW MATERIALIZED VIEWS name column")
        })
        .collect::<Result<Vec<_>>>()?;
    if names.iter().any(|name| name == mv) {
        bail!(
            "a recreated base table recovered the prior MV definition unexpectedly: names={names:?}; {}",
            context.diagnostics()
        );
    }
    let error = match conn.query_drop(format!("REFRESH MATERIALIZED VIEW {mv}")) {
        Err(error) => error,
        Ok(()) => {
            bail!(
                "a quarantined MV accepted refresh unexpectedly; {}",
                context.diagnostics()
            )
        }
    };
    let message = error.to_string();
    if !message.contains("MV target is unavailable")
        || !message.contains("published MV base object identities no longer match the live catalog")
    {
        bail!(
            "refresh after same-name base replacement returned unexpected error {message:?}; {}",
            context.diagnostics()
        );
    }
    Ok(())
}

fn assert_rows(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    sql: &str,
    expected: &[(i32, i64)],
    action: &str,
) -> Result<()> {
    let actual: Vec<(i32, i64)> = query(context, conn, sql, action)?;
    if actual != expected {
        bail!(
            "{action} returned {actual:?}, expected {expected:?}; {}",
            context.diagnostics()
        );
    }
    Ok(())
}

fn wait_for_rows(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    sql: &str,
    expected: &[(i32, i64)],
    action: &str,
) -> Result<()> {
    context.action(action);
    loop {
        let actual = conn.query::<(i32, i64), _>(sql);
        if let Ok(rows) = actual
            && rows == expected
        {
            return Ok(());
        }
        if context.remaining(action).is_err() {
            let observed = conn.query::<(i32, i64), _>(sql).ok();
            bail!(
                "timed out waiting for {action}; expected={expected:?}; observed={observed:?}; {}",
                context.diagnostics()
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
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

fn wait_for_marker_count(
    context: &mut ScenarioContext,
    directory: &Path,
    expected: usize,
    action: &str,
) -> Result<()> {
    context.action(action);
    loop {
        if marker_count(directory)? >= expected {
            return Ok(());
        }
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
}

fn marker_count(directory: &Path) -> Result<usize> {
    let count = fs::read_dir(directory)
        .with_context(|| format!("read scheduler marker directory {}", directory.display()))?
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("mvx4-scheduler-admitted-")
        })
        .count();
    Ok(count)
}

fn clear_scheduler_markers(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read scheduler marker directory {}", directory.display()))?
    {
        let entry = entry
            .with_context(|| format!("read scheduler marker entry in {}", directory.display()))?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with("mvx4-scheduler-admitted-")
        {
            fs::remove_file(entry.path()).with_context(|| {
                format!("remove stale scheduler marker {}", entry.path().display())
            })?;
        }
    }
    Ok(())
}

fn wait_for_fe_marker(context: &mut ScenarioContext, marker: &str, action: &str) -> Result<()> {
    context.action(action);
    loop {
        if context.handle().fe_log_contents()?.contains(marker) {
            return Ok(());
        }
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
}

fn spawn_refresh(
    user: String,
    port: u16,
    catalog: &str,
    mv: &str,
    timeout: Duration,
) -> Receiver<std::result::Result<(), String>> {
    let catalog = catalog.to_string();
    let mv = mv.to_string();
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let result = (|| -> Result<()> {
            let mut conn = mysql_actor::connect(&user, port, timeout)?;
            conn.query_drop(format!("SET CATALOG {catalog}"))?;
            conn.query_drop("USE ns")?;
            conn.query_drop(format!("REFRESH MATERIALIZED VIEW {mv}"))?;
            Ok(())
        })()
        .map_err(|error| format!("{error:#}"));
        let _ = sender.send(result);
    });
    receiver
}

fn expect_refresh_failure(
    context: &mut ScenarioContext,
    receiver: Receiver<std::result::Result<(), String>>,
    action: &str,
) -> Result<()> {
    context.action(action);
    let timeout = context.remaining(action)?;
    match receiver.recv_timeout(timeout) {
        Ok(Err(error)) if !error.is_empty() => Ok(()),
        Ok(Err(_)) => bail!("{action} returned an empty error"),
        Ok(Ok(())) => bail!("{action} unexpectedly succeeded"),
        Err(error) => bail!("{action} did not finish before deadline: {error}"),
    }
}

fn refresh_after_owner_crash(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    mv: &str,
) -> Result<()> {
    context.action("wait for durable MV refresh ownership takeover");
    let sql = format!("REFRESH MATERIALIZED VIEW {mv}");
    loop {
        match conn.query_drop(&sql) {
            Ok(()) => return Ok(()),
            Err(error) => {
                let message = error.to_string();
                if !message.contains("another frontend currently owns") {
                    return Err(anyhow::Error::new(error).context(
                        "recovery refresh returned an error other than ownership refusal",
                    ));
                }
                context.remaining("wait for durable MV refresh ownership takeover")?;
                thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

struct FileTrigger {
    path: PathBuf,
    removed: bool,
}

impl FileTrigger {
    fn create(path: &Path, contents: &str) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create trigger directory {}", parent.display()))?;
        }
        fs::write(path, contents).with_context(|| format!("write trigger {}", path.display()))?;
        Ok(Self {
            path: path.to_owned(),
            removed: false,
        })
    }

    fn remove(mut self) -> Result<()> {
        remove_if_exists(&self.path)?;
        self.removed = true;
        Ok(())
    }
}

impl Drop for FileTrigger {
    fn drop(&mut self) {
        if !self.removed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove trigger {}", path.display())),
    }
}
