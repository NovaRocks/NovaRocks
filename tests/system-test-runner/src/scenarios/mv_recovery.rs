#[path = "mv_control_plan.rs"]
pub(super) mod control_plan;

use super::mv_uea7::{
    ManagedMvRestFixture, property, require_status_phase, status, wait_for_status_phase,
};
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use ::mysql::prelude::{FromRow, Queryable};
use ::mysql::{Conn, Row};
use anyhow::{Context, Result, bail, ensure};
use control_plan::FeLaunchIntent;
use novarocks_cluster_harness::ServerHandle;
use novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan;
use reqwest::blocking::Client;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The task protocol's only fault that fails a participant which was admitted,
/// published RUNNING, and then failed on its own. It replaces the retired
/// protocol's `start-ack-suppress` here because a suppressed start acknowledged
/// nothing, while a staged MV snapshot only exists once a task really ran.
const TASK_EXECUTION_FAILURE: &str = "task-execution-failure";

/// Stable evidence that the injection above actually fired.
const TASK_EXECUTION_FAILURE_MARKER: &str = "NOVAROCKS_TASK_EXECUTION_FAILURE_INJECTED";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(MvStateStoreRestart::default()),
        Box::new(MvSchedulerRecovery::default()),
        Box::new(MvRewriteBindingBarrier::default()),
        Box::new(MvCurrentDependencyRecheck::default()),
        Box::new(MvLegacyInterpretationRebuild::default()),
        Box::new(MvRefreshConfigurationInterleaving::default()),
        Box::new(MvStagedPublishedRecovery::default()),
        Box::new(MvFirstRefreshStaging::default()),
        Box::new(MvBaseIdentityReplacement::default()),
        Box::new(MvLakePublicationRestartRebuild::default()),
    ]
}

#[derive(Default)]
struct MvStateStoreRestart {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvStateStoreRestart {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/state-store-restart"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, launch) = ManagedMvRestFixture::start(scenario_root, "system_mv_restart")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![FeLaunchIntent::Ordinary, FeLaunchIntent::Ordinary],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_restart";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;

        execute(
            context,
            &mut conn,
            "create StateStore-backed materialized view",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read first MV publication before FE restart",
        )?;
        conn.finish()?;

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
        require_status_phase(
            context,
            &mut conn,
            catalog,
            "orders_mv",
            "AWAITING_EFFECT_SETTLEMENT",
            "confirm management is closed after FE restart",
        )?;
        let closed = status(context, &mut conn, catalog, "orders_mv")?;
        let challenge = property(&closed, "Challenge")?;
        let previous_incarnation = property(&closed, "UnsettledEffect1Incarnation")?;
        context.action("declare the old FE isolated and resume exact MV management");
        let resumed: Vec<(String, Option<String>)> = conn
            .query(format!(
                "CALL novarocks_mv_resume_management('{catalog}', 'ns', 'orders_mv', \
                 '{challenge}', '{previous_incarnation}', 'uea7-system-runner', \
                 'the system scenario replaced the declared frontend process before this statement')"
            ))
            .context("resume managed MV after StateStore restart")?;
        if property(&resumed, "SettledEffects")? != "1" {
            bail!("StateStore restart readmission did not settle the old incarnation");
        }
        refresh(context, &mut conn, "orders_mv")?;
        execute(
            context,
            &mut conn,
            "create a second MV after StateStore recovery",
            "CREATE MATERIALIZED VIEW orders_mv_2 DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
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

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvSchedulerRecovery {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvSchedulerRecovery {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

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
        clear_scheduler_markers(&barrier_dir)?;
        remove_if_exists(
            &barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.consumed"),
        )?;
        remove_if_exists(
            &barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.trigger"),
        )?;
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_scheduler")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        let mut fe_overlay = launch.config_overlay.fe.take().unwrap_or_default();
        fe_overlay.push_str(
            r#"
[standalone_server]
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 100
mv_refresh_scheduler_max_concurrent = 1
mv_refresh_scheduler_failure_backoff_ms = 100
mv_refresh_scheduler_max_failure_backoff_ms = 1000
"#,
        );
        launch.config_overlay.fe = Some(fe_overlay);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![
                FeLaunchIntent::MvActions(vec![
                    control_plan::scheduler_outcome(
                        "outcome-a",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_a",
                        &scenario_root.join("mv-scheduler-outcomes/first/orders_mv_a"),
                    ),
                    control_plan::scheduler_outcome(
                        "outcome-b",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_b",
                        &scenario_root.join("mv-scheduler-outcomes/first/orders_mv_b"),
                    ),
                    control_plan::scheduler_outcome(
                        "outcome-recovery-first",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_recovery",
                        &scenario_root.join("mv-scheduler-outcomes/first/orders_mv_recovery"),
                    ),
                    control_plan::scheduler(
                        "scheduler-a",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_a",
                        &scenario_root.join("mv-scheduler-barrier"),
                    ),
                    control_plan::scheduler(
                        "scheduler-b",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_b",
                        &scenario_root.join("mv-scheduler-barrier"),
                    ),
                    control_plan::scheduler(
                        "scheduler-recovery",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_recovery",
                        &scenario_root.join("mv-scheduler-barrier"),
                    ),
                ]),
                FeLaunchIntent::MvActions(vec![
                    control_plan::scheduler_outcome(
                        "outcome-recovery-second",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_recovery",
                        &scenario_root.join("mv-scheduler-outcomes/recovery/orders_mv_recovery"),
                    ),
                    control_plan::transient(
                        "recovery-transient",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_recovery",
                        &scenario_root.join("mv-scheduler-barrier"),
                    ),
                ]),
            ],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let barrier_dir = context.scenario_root().join("mv-scheduler-barrier");
        let outcome_root = context.scenario_root().join("mv-scheduler-outcomes");
        let hold_trigger = barrier_dir.join("mvx4-scheduler-hold.trigger");
        let _hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        context.action("armed scheduler admission barrier");

        let catalog = "system_mv_scheduler";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, false)?;
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
            "CREATE MATERIALIZED VIEW orders_mv_a DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        execute(
            context,
            &mut conn,
            "create second asynchronous scheduler MV",
            "CREATE MATERIALIZED VIEW orders_mv_b DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
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
            SchedulerOutcomeTarget {
                directory: outcome_root.join("first/orders_mv_a"),
                grant_id: "outcome-a",
                mv: "orders_mv_a",
            },
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
            &[(1, 10), (2, 20)],
            "wait for second scheduler MV initial catch-up",
            SchedulerOutcomeTarget {
                directory: outcome_root.join("first/orders_mv_b"),
                grant_id: "outcome-b",
                mv: "orders_mv_b",
            },
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
            SchedulerOutcomeTarget {
                directory: outcome_root.join("first/orders_mv_a"),
                grant_id: "outcome-a",
                mv: "orders_mv_a",
            },
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "wait for second scheduler MV incremental catch-up",
            SchedulerOutcomeTarget {
                directory: outcome_root.join("first/orders_mv_b"),
                grant_id: "outcome-b",
                mv: "orders_mv_b",
            },
        )?;
        execute(
            context,
            &mut conn,
            "pause first scheduler MV before recovery fault",
            "ALTER MATERIALIZED VIEW orders_mv_a PAUSE REFRESH",
        )?;
        execute(
            context,
            &mut conn,
            "pause second scheduler MV before recovery fault",
            "ALTER MATERIALIZED VIEW orders_mv_b PAUSE REFRESH",
        )?;

        clear_scheduler_markers(&barrier_dir)?;
        let recovery_hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        execute(
            context,
            &mut conn,
            "create a scheduler MV for FE recovery",
            "CREATE MATERIALIZED VIEW orders_mv_recovery DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH ASYNC EVERY INTERVAL 1 SECOND PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        wait_for_file(
            context,
            &barrier_dir.join("mvx4-scheduler-admitted-orders_mv_recovery.marker"),
            "hold recovery MV scheduler refresh before FE replacement",
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
        conn.finish()?;
        context.action("terminate held scheduler FE attempt for recovery");
        context
            .handle()
            .kill_fe()
            .context("kill FE during held scheduler refresh")?;
        recovery_hold.remove()?;
        restart_frontend(context, "restart FE after interrupted scheduler refresh")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        resume_management_after_fe_restart(context, &mut conn, catalog, "orders_mv_recovery")?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_recovery ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30), (4, 40)],
            "wait for scheduler recovery to catch up durable MV",
            SchedulerOutcomeTarget {
                directory: outcome_root.join("recovery/orders_mv_recovery"),
                grant_id: "outcome-recovery-second",
                mv: "orders_mv_recovery",
            },
        )?;
        let consumed_fault =
            barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.consumed");
        if !consumed_fault.exists() {
            bail!(
                "scheduler caught up without consuming the injected preparation fault; {}",
                context.diagnostics()
            );
        }
        context.action("verified the scheduler consumed its transient preparation fault");
        context.action("scheduler caught up after explicit FE readmission and one transient preparation failure");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

/// Proves that a distributed rewritten query consumes the M1 target snapshot
/// whose completed physical plan and read access were frozen, even if a normal
/// refresh publishes M2 before the query is dispatched to its backend tasks.
#[derive(Default)]
struct MvRewriteBindingBarrier {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvRewriteBindingBarrier {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/rewrite-final-target-binding"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let barrier_dir = scenario_root.join("mv-rewrite-barrier");
        fs::create_dir_all(&barrier_dir).with_context(|| {
            format!(
                "create MV rewrite barrier directory {}",
                barrier_dir.display()
            )
        })?;
        let (fixture, launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_rewrite_binding")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![FeLaunchIntent::ObservedMvActions(vec![
                control_plan::rewrite(
                    "final-target-rewrite",
                    "system_mv_rewrite_binding",
                    "ns",
                    control_plan::REWRITE_BINDING_TOKEN,
                    &scenario_root.join("mv-rewrite-barrier"),
                ),
            ])],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_rewrite_binding";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let barrier_dir = context.scenario_root().join("mv-rewrite-barrier");
        let hold_trigger = barrier_dir.join("mvx4-rewrite-hold.trigger");
        let frozen_marker = barrier_dir.join("mvx4-completed-mv-target-frozen.marker");
        if frozen_marker.exists() {
            fs::remove_file(&frozen_marker).context("remove stale completed MV target marker")?;
        }
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, false)?;
        execute(
            context,
            &mut conn,
            "seed MV rewrite cost fixture",
            "INSERT INTO orders SELECT number % 3, CAST(number % 10 AS BIGINT) FROM TABLE(generate_series(1, 1200)) t(number)",
        )?;
        execute(
            context,
            &mut conn,
            "create aggregate MV for strict target binding",
            "CREATE MATERIALIZED VIEW orders_agg_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, SUM(v2) AS total_v2 FROM orders GROUP BY k1",
        )?;
        refresh(context, &mut conn, "orders_agg_mv")?;

        let rewritten: Vec<(String,)> = query(
            context,
            &mut conn,
            "EXPLAIN SELECT k1, SUM(v2) FROM orders GROUP BY k1 ORDER BY k1",
            "confirm the query selects the fresh MV target",
        )?;
        if !rewritten
            .iter()
            .any(|(line,)| line.contains("rewritten with mv: orders_agg_mv"))
        {
            bail!(
                "MV rewrite was not selected before the binding barrier; {}",
                context.diagnostics()
            );
        }
        context.action("confirmed query selection uses the M1 MV publication");

        let hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        let query = match control_plan::ObservedQuery::start(
            context,
            catalog,
            "ns",
            control_plan::REWRITE_BINDING_TOKEN,
        ) {
            Ok(query) => query,
            Err(error) => return control_plan::with_cleanup(Err(error), hold.remove()),
        };
        let phase = (|| -> Result<()> {
            wait_for_file_or_query(
                context,
                &frozen_marker,
                &query,
                "wait for completed M1 plan and read access to freeze",
            )?;
            context.action("observed completed query plan and access frozen on M1");
            execute(
                context,
                &mut conn,
                "advance source to S102 while query is held after M1 target freeze",
                "INSERT INTO orders VALUES (3, 99)",
            )?;
            refresh(context, &mut conn, "orders_agg_mv")?;
            context.action("published the normal M2 MV target before releasing the query");
            Ok(())
        })();
        let released = hold.remove();
        context.action("release rewritten query after M2 publication or failed barrier phase");
        let rows = query.finish_after_release(
            context,
            phase,
            released,
            "wait for rewritten query frozen against M1",
        )?;
        if rows != [(0, 1800), (1, 1800), (2, 1800)] {
            bail!(
                "rewritten query did not consume its frozen M1 target: rows={rows:?}; {}",
                context.diagnostics()
            );
        }
        context.action(
            "verified native 1FE+3BE query consumed M1 after concurrent S102/M2 publication",
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvCurrentDependencyRecheck {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

#[derive(Default)]
struct MvRefreshConfigurationInterleaving {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvRefreshConfigurationInterleaving {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/refresh-configuration-interleaving"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-recovery-faults");
        fs::create_dir_all(&fault_dir)?;
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_config_interleaving")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        let mut fe_overlay = launch.config_overlay.fe.take().unwrap_or_default();
        fe_overlay.push_str("\n[runtime]\nquery_blocking_worker_threads = 2\n");
        launch.config_overlay.fe = Some(fe_overlay);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![FeLaunchIntent::MvActions(vec![control_plan::recovery(
                "configuration-data-prepared",
                "system_mv_config_interleaving",
                "ns",
                "orders_mv",
                "data-prepared",
                "before-configuration-write",
                &scenario_root.join("mv-recovery-faults"),
            )?])],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_config_interleaving";
        let (create_catalog_sql, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV for concurrent configuration and refresh",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        let initial_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "orders_mv")?;
        execute(
            context,
            &mut conn,
            "create another independently managed MV",
            "CREATE MATERIALIZED VIEW parallel_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        let initial_parallel_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "parallel_mv")?;
        execute(
            context,
            &mut conn,
            "advance source before the held refresh",
            "INSERT INTO orders VALUES (3, 30)",
        )?;

        let fault_dir = context.scenario_root().join("mv-recovery-faults");
        let prepared = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-data-prepared.trigger"),
            "token=before-configuration-write\n",
        )?;
        let held_refresh = spawn_refresh(
            context.mysql_user().to_string(),
            context.mysql_port(),
            catalog,
            "orders_mv",
            context.deadline(),
        );
        wait_for_fe_marker(
            context,
            &held_refresh,
            "NOVAROCKS_MV_RECOVERY_PHASE phase=data-prepared token=before-configuration-write",
            "wait for completed MV computation before configuration write",
        )?;
        execute(
            context,
            &mut conn,
            "pause another MV while orders_mv refresh remains held",
            "ALTER MATERIALIZED VIEW parallel_mv PAUSE REFRESH",
        )?;
        let final_parallel_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "parallel_mv")?;
        if final_parallel_configuration == initial_parallel_configuration {
            bail!("other MV configuration did not commit while orders_mv was held");
        }

        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        context.remaining("start concurrent MV configuration write")?;
        let deadline = context.deadline();
        let result_rx = spawn_sql_actor(move || {
            mysql_actor::connect_until(&user, port, deadline)?.run(|writer| {
                writer.query_drop(format!("SET CATALOG {catalog}"))?;
                writer.query_drop("USE ns")?;
                started_tx
                    .send(())
                    .context("signal configuration writer readiness")?;
                writer.query_drop("ALTER MATERIALIZED VIEW orders_mv PAUSE REFRESH")?;
                Ok(())
            })
        });
        started_rx
            .recv_timeout(context.remaining("wait for configuration writer readiness")?)
            .context("configuration writer did not reach its SQL request")?;
        context.action("configuration writer reached SQL while the refresh is held");
        prepared.remove()?;
        context.action("release the refresh and settle the waiting configuration write");
        match held_refresh.recv_timeout(context.remaining("receive held refresh")?) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error).context("held refresh failed"),
            Err(error) => bail!("held refresh did not finish: {error}"),
        }
        match result_rx.recv_timeout(context.remaining("receive configuration write")?) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error).context("concurrent configuration write failed"),
            Err(error) => bail!("concurrent configuration write did not finish: {error}"),
        }
        require_refresh_paused(context, &mut conn, "orders_mv", true)?;
        require_refresh_paused(context, &mut conn, "parallel_mv", true)?;
        require_rest_snapshot_count(context, &rest_uri, "ns", "orders_mv", 3)?;
        require_rest_snapshot_count(context, &rest_uri, "ns", "parallel_mv", 1)?;
        let metadata =
            super::mv_uea7_documents::load_metadata(context, &rest_uri, "ns", "orders_mv")?;
        let graph = super::mv_uea7_documents::wire::Graph::from_metadata(&metadata)?;
        let last = graph
            .snapshots
            .last()
            .context("configuration slot snapshot missing")?;
        let previous = graph
            .snapshots
            .iter()
            .rev()
            .nth(1)
            .context("publication before slot change missing")?;
        ensure!(
            last.1 == previous.1,
            "configuration slot change did not carry P bytes"
        );
        let final_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "orders_mv")?;
        if final_configuration == initial_configuration {
            bail!("concurrent configuration write did not change the lake C revision");
        }
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "read the completed refresh after the configuration write",
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

fn require_refresh_paused(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    view: &str,
    expected: bool,
) -> Result<()> {
    let rows: Vec<Row> = conn.query("SHOW MATERIALIZED VIEWS")?;
    let row = rows
        .iter()
        .find(|row| row.get::<String, _>("Name").as_deref() == Some(view))
        .with_context(|| format!("SHOW MATERIALIZED VIEWS omitted {view}"))?;
    let actual = row
        .get::<String, _>("RefreshPaused")
        .context("SHOW MATERIALIZED VIEWS omitted RefreshPaused")?;
    if actual != expected.to_string() {
        bail!(
            "{view} RefreshPaused is {actual}, expected {expected}; {}",
            context.diagnostics()
        );
    }
    Ok(())
}

fn require_rest_snapshot_count(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
    expected: usize,
) -> Result<()> {
    context.action("check the exact number of retained MV outputs through REST");
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read exact MV output count")?)
        .build()?
        .get(&url)
        .send()?
        .error_for_status()?
        .json()?;
    let snapshots = loaded["metadata"]["snapshots"]
        .as_array()
        .context("REST MV metadata lacks snapshots")?;
    if snapshots.len() != expected {
        bail!("MV has {} snapshots, expected {expected}", snapshots.len());
    }
    Ok(())
}

// Finite subcases belong to the existing Current recheck scenario. Its original
// orders/D-drift test below remains unchanged and uses a separate source table.
fn source_window_interleavings(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    rest: &str,
) -> Result<()> {
    for (label, mode, phase) in [
        ("first", "first", "source-pinned"),
        ("full", "full", "source-prepared"),
        ("delta", "delta", "source-prepared"),
    ] {
        let source = format!("window_{label}_source");
        let mv = format!("window_{label}_mv");
        create_window_fixture(context, conn, &source, &mv)?;
        let expected = if mode == "first" {
            vec![(1, 10), (2, 20)]
        } else {
            refresh(context, conn, &mv)?;
            execute(
                context,
                conn,
                "advance source before the held window",
                &format!("INSERT INTO {source} VALUES (3,30)"),
            )?;
            vec![(1, 10), (2, 20), (3, 30)]
        };
        let before = window_metadata(context, rest, &source, &format!("{label}-source-before"))?;
        let old_to = window_snapshot(&before)?;
        let next = if mode == "first" { 3 } else { 4 };
        let (trigger, held) = hold_source_refresh(context, catalog, &mv, phase, mode == "full")?;
        let phase_result = (|| {
            wait_for_fe_marker(
                context,
                &held,
                &format!("NOVAROCKS_MV_RECOVERY_PHASE phase={phase} token=source-window-{mv}"),
                "wait for exact original source window boundary",
            )?;
            execute(
                context,
                conn,
                "append after the original source boundary",
                &format!("INSERT INTO {source} VALUES ({next},{})", next * 10),
            )?;
            Ok(())
        })();
        finish_source_hold(context, trigger, held, phase_result)??;
        let after = window_metadata(context, rest, &source, &format!("{label}-source-after"))?;
        ensure!(
            before["table-uuid"] == after["table-uuid"] && old_to != window_snapshot(&after)?,
            "append did not advance the same original source object"
        );
        assert_rows(
            context,
            conn,
            &format!("SELECT k1,v2 FROM {mv} ORDER BY k1"),
            &expected,
            "held refresh must read exactly its original To rows",
        )?;
        require_window_watermark(
            context,
            rest,
            &mv,
            old_to,
            &format!("{label}-published-old-to"),
        )?;
        refresh(context, conn, &mv)?;
        let mut all = expected;
        all.push((next, next as i64 * 10));
        assert_rows(
            context,
            conn,
            &format!("SELECT k1,v2 FROM {mv} ORDER BY k1"),
            &all,
            "only the next refresh includes the late append",
        )?;
        require_window_watermark(
            context,
            rest,
            &mv,
            window_snapshot(&after)?,
            &format!("{label}-published-next-to"),
        )?;
    }
    for label in ["uuid", "endpoint"] {
        let source = format!("window_{label}_source");
        let mv = format!("window_{label}_mv");
        create_window_fixture(context, conn, &source, &mv)?;
        refresh(context, conn, &mv)?;
        let target_before = window_metadata(context, rest, &mv, &format!("{label}-target-before"))?;
        execute(
            context,
            conn,
            "advance source before negative prepared window",
            &format!("INSERT INTO {source} VALUES (3,30)"),
        )?;
        let selected =
            window_metadata(context, rest, &source, &format!("{label}-selected-source"))?;
        let selected_to = window_snapshot(&selected)?;
        let replacement_location = if label == "uuid" {
            execute(
                context,
                conn,
                "create independently owned replacement source",
                "CREATE TABLE window_uuid_replacement (k1 INT,v2 BIGINT) TBLPROPERTIES ('format-version'='3','write.row-lineage'='true')",
            )?;
            execute(
                context,
                conn,
                "seed replacement source",
                "INSERT INTO window_uuid_replacement VALUES (9,90)",
            )?;
            Some(window_metadata_location(
                context,
                rest,
                "window_uuid_replacement",
            )?)
        } else {
            None
        };
        let (trigger, held) = hold_source_refresh(context, catalog, &mv, "source-prepared", false)?;
        let phase_result = (|| {
            wait_for_fe_marker(
                context,
                &held,
                &format!(
                    "NOVAROCKS_MV_RECOVERY_PHASE phase=source-prepared token=source-window-{mv}"
                ),
                "wait for negative exact prepared-source boundary",
            )?;
            if let Some(location) = &replacement_location {
                externally_drop_rest_table(context, rest, "ns", &source)?;
                // The original fixture's formal REST register author avoids a
                // same-name FE CREATE/cache/protection dependency entirely.
                register_window_source(context, rest, &source, location)?;
                let replacement = window_metadata(context, rest, &source, "uuid-replaced-source")?;
                ensure!(
                    replacement["table-uuid"] != selected["table-uuid"],
                    "external replacement did not change the actual UUID"
                );
            } else {
                execute(
                    context,
                    conn,
                    "move main off the selected endpoint before expiry",
                    &format!("INSERT INTO {source} VALUES (4,40)"),
                )?;
                let head = window_metadata(context, rest, &source, "endpoint-new-head")?;
                ensure!(
                    window_snapshot(&head)? != selected_to,
                    "expiry target is still main"
                );
                let expired = super::mv_uea7_documents::commit_updates(
                    context,
                    rest,
                    "ns",
                    &source,
                    &head,
                    vec![
                        serde_json::json!({"action":"remove-snapshots","snapshot-ids":[selected_to]}),
                    ],
                    "expire-held-source-endpoint",
                )?;
                ensure!(
                    expired["table-uuid"] == selected["table-uuid"]
                        && !expired["snapshots"]
                            .as_array()
                            .context("expired source has no snapshots")?
                            .iter()
                            .any(|v| v["snapshot-id"].as_i64() == Some(selected_to)),
                    "selected To was not actually removed from the same object"
                );
            }
            Ok(())
        })();
        let outcome = finish_source_hold(context, trigger, held, phase_result)?;
        require_window_failure(outcome, if label == "uuid" { 20 } else { 2 })?;
        let target_after =
            window_metadata(context, rest, &mv, &format!("{label}-target-after-failure"))?;
        require_window_publication_unchanged(&target_before, &target_after)?;
        wait_window_target_cleanup(context, rest, &mv, &target_before, label)?;
        assert_rows(
            context,
            conn,
            &format!("SELECT k1,v2 FROM {mv} ORDER BY k1"),
            &[(1, 10), (2, 20)],
            "failed prepared Source must preserve the complete previous publication",
        )?;
    }
    Ok(())
}
fn create_window_fixture(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    source: &str,
    mv: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "create isolated source-window table",
        &format!(
            "CREATE TABLE {source} (k1 INT,v2 BIGINT) TBLPROPERTIES ('format-version'='3','write.row-lineage'='true')"
        ),
    )?;
    execute(
        context,
        conn,
        "seed isolated source-window table",
        &format!("INSERT INTO {source} VALUES (1,10),(2,20)"),
    )?;
    execute(
        context,
        conn,
        "create isolated source-window MV",
        &format!(
            "CREATE MATERIALIZED VIEW {mv} DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine'='iceberg') AS SELECT k1,v2 FROM {source}"
        ),
    )
}
fn hold_source_refresh(
    context: &ScenarioContext,
    catalog: &str,
    mv: &str,
    phase: &str,
    full: bool,
) -> Result<(FileTrigger, JoinedSqlResult)> {
    let trigger = FileTrigger::create(
        &context
            .scenario_root()
            .join("mv-recovery-faults")
            .join(format!("mv-refresh-at-{phase}.trigger")),
        &format!("token=source-window-{mv}\n"),
    )?;
    let user = context.mysql_user().to_string();
    let port = context.mysql_port();
    let deadline = context.deadline();
    let catalog = catalog.to_string();
    let sql = format!(
        "REFRESH MATERIALIZED VIEW {mv}{} WITH SYNC MODE",
        if full { " FULL" } else { "" }
    );
    let held = spawn_sql_actor(move || {
        mysql_actor::connect_until(&user, port, deadline)?.run(|conn| {
            conn.query_drop(format!("SET CATALOG {catalog}"))?;
            conn.query_drop("USE ns")?;
            conn.query_drop(sql)?;
            Ok(())
        })
    });
    Ok((trigger, held))
}
fn finish_source_hold(
    context: &ScenarioContext,
    trigger: FileTrigger,
    held: JoinedSqlResult,
    phase: Result<()>,
) -> Result<Result<()>> {
    // Release before join even when marker/mutation/receive already failed.
    // The same original actor retains its work deadline; joining is not a
    // fresh producer allowance and the post-join drain never waits.
    let released = trigger.remove();
    let (mut sql, mut received) = match context.remaining("receive original held source refresh") {
        Ok(remaining) => match held.result.recv_timeout(remaining) {
            Ok(outcome) => (Some(outcome), Ok(())),
            Err(error) => (None, Err(error).context("receive original MV SQL result")),
        },
        Err(error) => (None, Err(error)),
    };
    let joined = held.join();
    if sql.is_none() {
        match held.result.try_recv() {
            Ok(outcome) => sql = Some(outcome),
            Err(error) => {
                received = Err(match received {
                    Ok(()) => {
                        anyhow::anyhow!(error).context("original MV SQL result absent after join")
                    }
                    Err(primary) => primary.context(format!("post-join SQL result: {error}")),
                });
            }
        }
    }
    source_hold_outcomes(sql, phase, received, joined, released)
}

/// A SQL refusal is an expected nested outcome only when every original
/// marker/receive/join/release obligation succeeded. Keep its typed cause as
/// primary when infrastructure also failed, without declaring an expected pass.
fn source_hold_outcomes(
    sql: Option<Result<()>>,
    phase: Result<()>,
    received: Result<()>,
    joined: Result<()>,
    released: Result<()>,
) -> Result<Result<()>> {
    let infrastructure = [
        ("source-window phase", phase),
        ("original SQL receive/deadline", received),
        ("original SQL join", joined),
        ("original source-hold release", released),
    ];
    if infrastructure.iter().all(|(_, value)| value.is_ok()) {
        return sql.context("original MV SQL result missing after join");
    }
    // Flatten before combining cleanup: Some(Err(sql)) is not an outer success.
    let mut primary = sql.and_then(Result::err);
    for (label, outcome) in infrastructure {
        if let Err(error) = outcome {
            primary = Some(match primary {
                Some(original) => original.context(format!("{label}: {error:#}")),
                None => error.context(label),
            });
        }
    }
    Err(primary.expect("one failed original infrastructure obligation"))
}

fn window_metadata(
    context: &ScenarioContext,
    rest: &str,
    table: &str,
    label: &str,
) -> Result<serde_json::Value> {
    let value = super::mv_uea7_documents::load_metadata(context, rest, "ns", table)?;
    let dir = context.scenario_root().join("source-window-evidence");
    fs::create_dir_all(&dir)?;
    fs::write(
        dir.join(format!("{label}.json")),
        serde_json::to_vec_pretty(&value)?,
    )?;
    Ok(value)
}
fn window_snapshot(metadata: &serde_json::Value) -> Result<i64> {
    metadata["current-snapshot-id"]
        .as_i64()
        .filter(|id| *id >= 0)
        .context("source window has no actual snapshot")
}
fn window_record(metadata: &serde_json::Value) -> Result<super::mv_uea7_documents::wire::Record> {
    let current = window_snapshot(metadata)?;
    let snapshot = metadata["snapshots"]
        .as_array()
        .context("window target has no snapshots")?
        .iter()
        .find(|v| v["snapshot-id"].as_i64() == Some(current))
        .context("window target main is absent")?;
    let frame = snapshot["summary"][super::mv_uea7_documents::wire::RECORD]
        .as_str()
        .context("window target main has no publication")?;
    super::mv_uea7_documents::wire::record(frame)
}
fn require_window_watermark(
    context: &ScenarioContext,
    rest: &str,
    mv: &str,
    expected: i64,
    label: &str,
) -> Result<()> {
    let metadata = window_metadata(context, rest, mv, label)?;
    let graph = super::mv_uea7_documents::wire::Graph::from_metadata(&metadata)?;
    let record = window_record(&metadata)?;
    ensure!(
        graph.record_snapshot_watermarks(&record)? == [Some(expected)],
        "MV publication did not retain its exact original Source To watermark"
    );
    Ok(())
}
fn require_window_publication_unchanged(
    before: &serde_json::Value,
    after: &serde_json::Value,
) -> Result<()> {
    ensure!(
        before["current-snapshot-id"] == after["current-snapshot-id"]
            && window_record(before)? == window_record(after)?,
        "failed source refresh changed target main or exact P"
    );
    let before_graph = super::mv_uea7_documents::wire::Graph::from_metadata(before)?;
    let after_graph = super::mv_uea7_documents::wire::Graph::from_metadata(after)?;
    ensure!(
        before_graph.roots == after_graph.roots,
        "failed source refresh changed original target document roots"
    );
    Ok(())
}
fn wait_window_target_cleanup(
    context: &ScenarioContext,
    rest: &str,
    mv: &str,
    before: &serde_json::Value,
    label: &str,
) -> Result<()> {
    let expected = before["refs"]
        .as_object()
        .context("target has no original ref inventory")?;
    let mut observation = 0usize;
    loop {
        context.remaining("observe actual failed-write target ref cleanup")?;
        let after = window_metadata(context, rest, mv, &format!("{label}-cleanup-{observation}"))?;
        require_window_publication_unchanged(before, &after)?;
        let actual = after["refs"]
            .as_object()
            .context("target has no cleanup ref inventory")?;
        if actual.keys().all(|name| expected.contains_key(name)) {
            // Prior housekeeping refs may disappear independently. Only refs
            // introduced by this failed refresh must settle here; main/P are
            // checked separately and never inferred from this inventory.
            return Ok(());
        }
        observation = observation
            .checked_add(1)
            .context("target cleanup observation ordinal overflow")?;
        // Poll an actual catalog fact under the original deadline; this does
        // not declare Native/Source exit, which the original teardown owns.
        thread::sleep(POLL_INTERVAL);
    }
}

fn require_window_failure(outcome: Result<()>, reason: u16) -> Result<()> {
    let Err(failure) = outcome else {
        bail!("source identity/endpoint drift succeeded");
    };
    let original = failure
        .chain()
        .find_map(|value| value.downcast_ref::<mysql::Error>())
        .context("source drift did not retain an original MySQL server error")?;
    let mysql::Error::MySqlError(server) = original else {
        bail!("source drift returned transport/client failure: {failure:#}");
    };
    ensure!(
        server.code == 1105
            && server.state == "HY000"
            && server.message.starts_with("[query.execution.dependency]")
            && server.message.contains(&format!("(reason 4:{reason})")),
        "wrong original public Source failure: {server:?}; {failure:#}"
    );
    Ok(())
}
fn window_metadata_location(context: &ScenarioContext, rest: &str, table: &str) -> Result<String> {
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read original replacement metadata pointer")?)
        .build()?
        .get(format!(
            "{}/v1/namespaces/ns/tables/{table}",
            rest.trim_end_matches('/')
        ))
        .send()?
        .error_for_status()?
        .json()?;
    loaded["metadata-location"]
        .as_str()
        .map(str::to_owned)
        .context("replacement source has no actual metadata-location")
}
fn register_window_source(
    context: &ScenarioContext,
    rest: &str,
    table: &str,
    metadata_location: &str,
) -> Result<()> {
    // Same formal register request used by catalog_publication::register_source.
    let response = Client::builder()
        .no_proxy()
        .timeout(context.remaining("register exact replacement source")?)
        .build()?
        .post(format!(
            "{}/v1/namespaces/ns/register",
            rest.trim_end_matches('/')
        ))
        .json(&serde_json::json!({"name":table,"metadata-location":metadata_location}))
        .send()?;
    ensure!(
        response.status().is_success(),
        "external source registration failed: {}",
        response.status()
    );
    Ok(())
}

fn rest_configuration_revision(
    context: &ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
) -> Result<Vec<u8>> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read exact lake configuration revision")?)
        .build()?
        .get(&url)
        .send()?
        .error_for_status()?
        .json()?;
    let graph = super::mv_uea7_documents::wire::Graph::from_metadata(&loaded["metadata"])?;
    graph.slots[super::mv_uea7_documents::wire::CONFIG].content_revision()
}

impl Scenario for MvCurrentDependencyRecheck {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/current-dependency-recheck"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-recovery-faults");
        fs::create_dir_all(&fault_dir)?;
        let (fixture, launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_dependency_recheck")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        let mut grants = vec![control_plan::recovery(
            "dependency-data-prepared",
            "system_mv_dependency_recheck",
            "ns",
            "orders_mv",
            "data-prepared",
            "before-current-dependency-recheck",
            &scenario_root.join("mv-recovery-faults"),
        )?];
        for (mv, phase) in [
            ("window_first_mv", "source-pinned"),
            ("window_full_mv", "source-prepared"),
            ("window_delta_mv", "source-prepared"),
            ("window_uuid_mv", "source-prepared"),
            ("window_endpoint_mv", "source-prepared"),
        ] {
            grants.push(control_plan::recovery(
                &format!("source-window-{mv}"),
                "system_mv_dependency_recheck",
                "ns",
                mv,
                phase,
                &format!("source-window-{mv}"),
                &scenario_root.join("mv-recovery-faults"),
            )?);
        }
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![FeLaunchIntent::MvActions(grants)],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_dependency_recheck";
        let (create_catalog_sql, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        source_window_interleavings(context, &mut conn, catalog, &rest_uri)?;
        execute(
            context,
            &mut conn,
            "create MV for Current dependency recheck",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        execute(
            context,
            &mut conn,
            "advance the source before the held refresh",
            "INSERT INTO orders VALUES (3, 30)",
        )?;

        let fault_dir = context.scenario_root().join("mv-recovery-faults");
        let prepared = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-data-prepared.trigger"),
            "token=before-current-dependency-recheck\n",
        )?;
        let held_refresh = spawn_refresh(
            context.mysql_user().to_string(),
            context.mysql_port(),
            catalog,
            "orders_mv",
            context.deadline(),
        );
        wait_for_fe_marker(
            context,
            &held_refresh,
            "NOVAROCKS_MV_RECOVERY_PHASE phase=data-prepared token=before-current-dependency-recheck",
            "wait for BE computation before Current recheck",
        )?;
        let before_injection =
            super::mv_uea7_documents::load_metadata(context, &rest_uri, "ns", "orders_mv")?;
        let graph = super::mv_uea7_documents::wire::Graph::from_metadata(&before_injection)?;
        let missing_key = format!("novarocks.doc.v2.rev.{}", graph.definition);
        let snapshot_id =
            externally_remove_current_definition_document(context, &rest_uri, "ns", "orders_mv")?;
        prepared.remove()?;
        context.action("verify an externally changed D stops the old refresh before commit");
        match held_refresh.recv_timeout(context.remaining("receive stale MV refresh")?) {
            Ok(Err(error))
                if format!("{error:#}")
                    .to_ascii_lowercase()
                    .contains("corrupt")
                    && format!("{error:#}").contains(&missing_key) => {}
            Ok(Err(error)) => {
                return Err(error).context("stale MV refresh failed for another reason");
            }
            Ok(Ok(())) => bail!("stale MV refresh published after D changed"),
            Err(error) => bail!("stale MV refresh did not finish: {error}"),
        }
        assert_rest_snapshot_unchanged(context, &rest_uri, "ns", "orders_mv", snapshot_id)?;
        context.action("Current D drift rejected without a second MV snapshot");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvLegacyInterpretationRebuild {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvLegacyInterpretationRebuild {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/legacy-interpretation-rebuild"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_legacy_interpretation")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![FeLaunchIntent::Ordinary, FeLaunchIntent::Ordinary],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_legacy_interpretation";
        let (create_catalog_sql, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        let create = "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders";
        execute(context, &mut conn, "create an unpublished MV", create)?;
        externally_persist_old_endian_interpretation(context, &rest_uri, "ns", "orders_mv")?;
        conn.finish()?;

        restart_frontend(context, "restart FE over an old nonzero big-endian L")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        let closed = wait_for_status_phase(
            context,
            &mut conn,
            catalog,
            "orders_mv",
            "AWAITING_EFFECT_SETTLEMENT",
            "wait for recovered old-format MV management barrier",
        )?;
        let challenge = property(&closed, "Challenge")?;
        let previous_incarnation = property(&closed, "UnsettledEffect1Incarnation")?;
        context.action("settle the old FE before testing the old L binding");
        let resumed: Vec<(String, Option<String>)> = conn.query(format!(
            "CALL novarocks_mv_resume_management('{catalog}', 'ns', 'orders_mv', \
             '{challenge}', '{previous_incarnation}', 'uea7-system-runner', \
             'the system scenario replaced the old FE before rebuilding an old-format MV')"
        ))?;
        if property(&resumed, "SettledEffects")? != "1" {
            bail!("old-format MV did not settle the old FE incarnation");
        }
        let management_metadata = capture_empty_management_snapshot(
            context,
            &rest_uri,
            "ns",
            "orders_mv",
            "before-rejected-refresh",
        )?;
        let management_snapshot = management_metadata["current-snapshot-id"]
            .as_i64()
            .context("empty management snapshot has no Current ID")?;
        context.action("old L must reject a refresh without a data publication");
        let error = conn
            .query_drop("REFRESH MATERIALIZED VIEW orders_mv")
            .expect_err("old nonzero big-endian L must fail closed")
            .to_string();
        if !error
            .contains("MV runtime target schema version is not from the exact document generation")
        {
            bail!("old L refresh failed for another reason: {error}");
        }
        assert_rest_snapshot_unchanged(context, &rest_uri, "ns", "orders_mv", management_snapshot)?;
        let after_rejection = capture_empty_management_snapshot(
            context,
            &rest_uri,
            "ns",
            "orders_mv",
            "after-rejected-refresh",
        )?;
        ensure!(
            after_rejection["snapshots"] == management_metadata["snapshots"],
            "old-L rejection changed the management snapshot facts"
        );

        execute(
            context,
            &mut conn,
            "drop the old-format MV",
            "DROP MATERIALIZED VIEW orders_mv",
        )?;
        execute(
            context,
            &mut conn,
            "recreate the MV using current L format",
            create,
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rest_snapshot_count(context, &rest_uri, "ns", "orders_mv", 1)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read the rebuilt MV",
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvStagedPublishedRecovery {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvStagedPublishedRecovery {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/staged-published-recovery"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-recovery-faults");
        fs::create_dir_all(&fault_dir).with_context(|| {
            format!("create MV recovery fault directory {}", fault_dir.display())
        })?;
        let (fixture, launch) = ManagedMvRestFixture::start(scenario_root, "system_mv_recovery")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![
                FeLaunchIntent::MvActions(vec![control_plan::recovery(
                    "staged-write-committed",
                    "system_mv_recovery",
                    "ns",
                    "orders_mv",
                    "write-committed",
                    "staged-before-publication",
                    &scenario_root.join("mv-recovery-faults"),
                )?]),
                FeLaunchIntent::MvActions(vec![control_plan::recovery(
                    "published-before-cleanup",
                    "system_mv_recovery",
                    "ns",
                    "orders_mv",
                    "publication-committed",
                    "published-before-cleanup",
                    &scenario_root.join("mv-recovery-faults"),
                )?]),
                FeLaunchIntent::Ordinary,
            ],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fault_dir = context.scenario_root().join("mv-recovery-faults");
        let catalog = "system_mv_recovery";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV for staged and published recovery",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;

        // A canonical publication is one commit: the rows and the publication
        // document land together, so there is no staged-but-unpublished state
        // to crash into. What this window still proves is that a crash between
        // that commit and the frontend's own record of it leaves the committed
        // publication visible and re-refreshable.
        let staged = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-write-committed.trigger"),
            "token=staged-before-publication\n",
        )?;
        context.action("armed committed-before-record crash barrier");
        let staged_refresh = spawn_refresh(
            context.mysql_user().to_string(),
            context.mysql_port(),
            catalog,
            "orders_mv",
            context.deadline(),
        );
        wait_for_fe_marker(
            context,
            &staged_refresh,
            "NOVAROCKS_MV_RECOVERY_PHASE phase=write-committed token=staged-before-publication",
            "wait for committed-before-record barrier",
        )?;
        context.action("kill FE between the publication commit and its record");
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
        restart_frontend(context, "restart FE after committed-before-record crash")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify the committed publication survived the unrecorded crash",
        )?;
        resume_and_refresh_after_fe_restart(context, &mut conn, catalog, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify recovery re-refreshes onto the same published rows",
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
            context.deadline(),
        );
        wait_for_fe_marker(
            context,
            &published_refresh,
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
        resume_and_refresh_after_fe_restart(context, &mut conn, catalog, "orders_mv")?;
        context.action("staged and published crash windows converged through public MV behavior");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvFirstRefreshStaging {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvFirstRefreshStaging {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/first-refresh-staging"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, launch) = ManagedMvRestFixture::start(scenario_root, "system_mv_staging")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::first_refresh_plan(cluster_size, binary, original_deadline).map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_staging";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;

        execute(
            context,
            &mut conn,
            "create first-refresh projection MV",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
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
            "CREATE MATERIALIZED VIEW orders_agg_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, SUM(v2) AS total_v2 FROM orders GROUP BY k1",
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
            "CREATE MATERIALIZED VIEW orders_start_fault_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        // The refresh has to fail from inside a task that was really admitted
        // and really started, because that is what leaves a staged main
        // snapshot behind for the publication rule to discard. A refused
        // admission would prove nothing about staging. The fault fires on the
        // backend it is armed for; the MV's base table is a connector scan, so
        // its scan fragment is placed on every live backend and that backend
        // does get a task.
        let injected_baseline = total_be_marker_count(context, TASK_EXECUTION_FAILURE_MARKER)?;
        let deadline = context.deadline();
        // The actual connection is idle after catalog/database setup. Arm the
        // exact outer REFRESH bytes before its real statement admission.
        let observation = context.handle().arm_selected_statement_observation(
            control_plan::FIRST_REFRESH_TOKEN,
            conn.connection_id(),
            control_plan::FIRST_REFRESH_SQL,
            deadline,
        )?;
        let armed = context.handle().arm_selected_query_lifecycle_fault(
            0,
            TASK_EXECUTION_FAILURE,
            control_plan::FIRST_REFRESH_TOKEN,
        );
        if let Err(error) = armed {
            let drained = context
                .handle()
                .drain_query_lifecycle_statement_observation(&observation);
            return control_plan::with_cleanup(Err(error), drained);
        }
        context.action("armed an injected task execution failure for MV first refresh");
        let refresh_result = conn.query_drop(control_plan::FIRST_REFRESH_SQL);
        let cleanup_result = context
            .handle()
            .clear_query_lifecycle_faults()
            .context("clear injected task execution failure");
        // query_drop returned the actual response before its observation is drained.
        let drained = context
            .handle()
            .drain_query_lifecycle_statement_observation(&observation);
        control_plan::with_cleanup(cleanup_result, drained)
            .with_context(|| format!("first-refresh actual response: {refresh_result:?}"))?;
        let error = refresh_result
            .expect_err("an injected task execution failure must fail the MV first refresh");
        if error.to_string().is_empty() {
            bail!("injected task execution failure returned an empty MV refresh error");
        }
        // Without this the remaining assertions would also hold for a refresh
        // that failed for an unrelated reason, or for one that never ran a
        // distributed task at all.
        let injected = total_be_marker_count(context, TASK_EXECUTION_FAILURE_MARKER)?;
        if injected <= injected_baseline {
            bail!(
                "MV first refresh failed without the injected task execution failure firing; \
                 expected a new {TASK_EXECUTION_FAILURE_MARKER} across the BE logs, \
                 count stayed at {injected_baseline}"
            );
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

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvBaseIdentityReplacement {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvBaseIdentityReplacement {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/base-identity-replacement"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_base_identity")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![FeLaunchIntent::Ordinary, FeLaunchIntent::Ordinary],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_base_identity";
        let (create_catalog_sql, rest_uri) = {
            let fixture = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = fixture
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV with a durable base-object binding",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read publication before replacing its base table",
        )?;

        conn.finish()?;
        externally_drop_rest_table(context, &rest_uri, "ns", "orders")?;
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
        conn.finish()?;

        restart_frontend(context, "restart FE after same-name base replacement")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_mv_quarantined_after_base_replacement(context, &mut conn, catalog, "orders_mv")?;
        context.action(
            "verified FE restart quarantines the MV rather than bind a same-name replacement base",
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvLakePublicationRestartRebuild {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvLakePublicationRestartRebuild {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/lake-publication-restart-rebuild"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_lake_rebuild")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![
                FeLaunchIntent::MvActions(vec![
                    control_plan::observation(
                        "lake-provenance",
                        "system_mv_lake_rebuild",
                        "ns",
                        "orders_mv",
                        "provenance",
                    ),
                    control_plan::wipe("accelerator-family-wipe"),
                ]),
                FeLaunchIntent::Ordinary,
            ],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_lake_rebuild";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV with canonical lake documents",
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read newly published lake-native MV before FE restart",
        )?;
        let rows: Vec<Row> = query(
            context,
            &mut conn,
            &format!(
                "CALL {catalog}.system.novarocks_imv_stateless_rebuild(table => 'ns.orders_mv', level => 'provenance')"
            ),
            "confirm the published MV has exact lake documents",
        )?;
        let report = rows
            .first()
            .context("lake document observation returned no report row")?;
        let level = report
            .get::<String, _>(0)
            .context("lake document observation AvailableLevel column")?;
        let source = report
            .get::<String, _>(4)
            .context("lake document observation RebuildSource column")?;
        if level != "provenance" || source != "lake-documents" {
            bail!(
                "unexpected lake document report level={level:?}, source={source:?}; {}",
                context.diagnostics()
            );
        }
        let wiped: Vec<Row> = query(
            context,
            &mut conn,
            &format!(
                "CALL {catalog}.system.novarocks_imv_stateless_rebuild(table => 'ns.orders_mv', level => 'wipe')"
            ),
            "wipe only the MV Accelerator after proving lake documents",
        )?;
        let wipe_report = wiped
            .first()
            .context("MV Accelerator wipe returned no report row")?;
        if wipe_report.get::<String, _>(0).as_deref() != Some("wipe")
            || wipe_report.get::<String, _>(4).as_deref() != Some("accelerator-wiped")
        {
            bail!("unexpected MV Accelerator wipe report: {wipe_report:?}");
        }
        conn.finish()?;
        restart_frontend(context, "restart FE after MV Accelerator wipe")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read MV rediscovered from canonical lake publication",
        )?;
        resume_and_refresh_after_fe_restart(context, &mut conn, catalog, "orders_mv")?;
        context.action("verified MV wipe, restart, readmission and refresh from lake documents");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

/// Counts one marker across every backend's log.
///
/// A single injected failure happens on one backend, so the sum is the honest
/// quantity here: counting the number of distinct backends that saw it would
/// assert a fanout the injection never claims.
fn total_be_marker_count(context: &mut ScenarioContext, marker: &str) -> Result<usize> {
    let be_count = context.handle().be_count();
    let mut total = 0;
    for index in 0..be_count {
        total += context
            .handle()
            .be_log_count(index, marker)
            .with_context(|| format!("count {marker} in BE[{index}] log"))?;
    }
    Ok(total)
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
    context.remaining("connect MySQL client")?;
    context.action("connect through public MySQL protocol");
    mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )
}

fn setup_orders_fixture_rest(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    create_catalog_sql: &str,
    seed_rows: bool,
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

fn externally_drop_rest_table(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
) -> Result<()> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    context.action("drop original base table through the private Iceberg REST catalog");
    Client::builder()
        .no_proxy()
        .timeout(context.remaining("drop base table through external REST catalog")?)
        .build()
        .context("build external REST catalog client")?
        .delete(&url)
        .send()
        .with_context(|| format!("delete original base table at {url}"))?
        .error_for_status()
        .with_context(|| {
            format!("REST catalog rejected deletion of original base table at {url}")
        })?;
    Ok(())
}

/// An unsupported external metadata writer removes only D from the target's
/// table-level manifest. It leaves `main` untouched, so physical snapshot OCC
/// cannot stand in for the frontend's post-compute D/L/P reobservation.
fn externally_remove_current_definition_document(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
) -> Result<i64> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let client = Client::builder()
        .no_proxy()
        .timeout(context.remaining("mutate Current D through external REST")?)
        .build()?;
    let loaded: serde_json::Value = client.get(&url).send()?.error_for_status()?.json()?;
    let metadata = loaded
        .get("metadata")
        .context("REST table load has no metadata")?;
    let snapshot_id = metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .context("REST table load has no current MV snapshot")?;
    use super::mv_uea7_documents::{commit_updates, wire::Graph};
    let graph = Graph::from_metadata(metadata)?;
    let key = format!("novarocks.doc.v2.rev.{}", graph.definition);
    context.action(format!(
        "remove current definition version {key} without changing main"
    ));
    commit_updates(
        context,
        rest_uri,
        namespace,
        table,
        metadata,
        vec![serde_json::json!({"action":"remove-properties","removals":[key]})],
        "remove-current-definition",
    )?;
    assert_rest_snapshot_unchanged(context, rest_uri, namespace, table, snapshot_id)?;
    Ok(snapshot_id)
}

/// Reproduce the pre-fix CREATE encoding on an unpublished target. The test
/// writer first advances the physical Iceberg schema ID, then writes that
/// exact nonzero ID into L in the former big-endian order. No P exists yet,
/// so no historical publication reference is rewritten by this fixture.
fn externally_persist_old_endian_interpretation(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
) -> Result<()> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let client = Client::builder()
        .no_proxy()
        .timeout(context.remaining("construct old-format L in private REST")?)
        .build()?;
    let loaded: serde_json::Value = client.get(&url).send()?.error_for_status()?.json()?;
    let metadata = loaded
        .get("metadata")
        .context("REST table load has no metadata")?;
    if metadata["snapshots"]
        .as_array()
        .is_some_and(|snapshots| !snapshots.is_empty())
    {
        bail!("old-format L fixture requires an unpublished MV target");
    }
    let table_uuid = metadata["table-uuid"]
        .as_str()
        .context("REST MV metadata has no table UUID")?;
    let current_schema_id = metadata["current-schema-id"]
        .as_i64()
        .context("REST MV metadata has no current schema ID")?;
    let next_schema_id = current_schema_id
        .checked_add(1)
        .context("REST MV schema ID overflow")?;
    if next_schema_id <= 0 {
        bail!("old-format L fixture requires a positive physical schema ID");
    }
    let last_column_id = metadata["last-column-id"]
        .as_i64()
        .context("REST MV metadata has no last column ID")?;
    let mut new_schema = metadata["schemas"]
        .as_array()
        .context("REST MV metadata has no schemas")?
        .iter()
        .find(|schema| schema["schema-id"].as_i64() == Some(current_schema_id))
        .cloned()
        .context("REST MV metadata has no current schema")?;
    new_schema["schema-id"] = serde_json::json!(next_schema_id);
    let fields = new_schema["fields"]
        .as_array_mut()
        .context("REST MV schema has no fields")?;
    if fields.len() < 2 {
        bail!("old-format L fixture requires two target fields to reorder");
    }
    fields.reverse();
    use super::mv_uea7_documents::wire::{Graph, ROOT, Reference, Version};
    let graph = Graph::from_metadata(metadata)?;
    let interpretation = graph
        .versions
        .get(&graph.interpretation)
        .context("MV lacks current L")?;
    let mut content = interpretation.bytes()?;
    let target = protobuf_bytes_field(&content, 9)?;
    let schema = protobuf_bytes_field(&content[target.clone()], 2)?;
    if schema.len() != 4 {
        bail!("old-format L fixture expected a four-byte schema version");
    }
    let schema = target.start + schema.start..target.start + schema.end;
    let original_id = i32::from_le_bytes(content[schema.clone()].try_into()?);
    if i64::from(original_id) != current_schema_id || next_schema_id > i64::from(i32::MAX) {
        bail!("old-format L fixture schema version does not match the exact physical target");
    }
    content[schema].copy_from_slice(&(next_schema_id as i32).to_be_bytes());
    let new_l = Version::new(
        &interpretation.owner,
        &interpretation.name,
        interpretation.format.clone(),
        interpretation.refs.clone(),
        &content,
    )?;
    let binding = &graph.versions[&graph.binding];
    let new_b = Version::new(
        &binding.owner,
        &binding.name,
        binding.format.clone(),
        vec![
            Reference {
                rel: "definition".into(),
                id: graph.definition.clone(),
            },
            Reference {
                rel: "interpretation".into(),
                id: new_l.id.clone(),
            },
        ],
        &binding.bytes()?,
    )?;
    context
        .action("persist an exact nonzero physical schema with the former big-endian L encoding");
    let update = serde_json::json!({
        "requirements": [
            {"type": "assert-table-uuid", "uuid": table_uuid},
            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null},
            {"type": "assert-current-schema-id", "current-schema-id": current_schema_id},
            {"type": "assert-last-assigned-field-id", "last-assigned-field-id": last_column_id}
        ],
        "updates": [
            {"action": "add-schema", "schema": new_schema, "last-column-id": last_column_id},
            {"action": "set-current-schema", "schema-id": -1},
            {"action": "set-properties", "updates": {(new_l.key()):new_l.encoded()?,(new_b.key()):new_b.encoded()?,(ROOT):new_b.id}}
        ]
    });
    let response = client.post(&url).json(&update).send()?;
    if !response.status().is_success() {
        bail!(
            "old-format L REST mutation failed: {} {}",
            response.status(),
            response.text()?
        );
    }
    let observed: serde_json::Value = client.get(&url).send()?.error_for_status()?.json()?;
    if observed["metadata"]["current-schema-id"].as_i64() != Some(next_schema_id) {
        bail!("old-format L REST mutation did not advance the physical schema ID");
    }
    let persisted_graph = Graph::from_metadata(&observed["metadata"])?;
    let persisted_content = persisted_graph.versions[&persisted_graph.interpretation].bytes()?;
    let target = protobuf_bytes_field(&persisted_content, 9)?;
    let schema = protobuf_bytes_field(&persisted_content[target.clone()], 2)?;
    let persisted_schema = target.start + schema.start..target.start + schema.end;
    if persisted_content[persisted_schema] != (next_schema_id as i32).to_be_bytes() {
        bail!("old-format L REST mutation did not retain the big-endian schema version");
    }
    Ok(())
}

pub(super) fn protobuf_bytes_field(
    input: &[u8],
    expected_field: u64,
) -> Result<std::ops::Range<usize>> {
    let mut offset = 0;
    let mut selected = None;
    while offset < input.len() {
        let key = protobuf_varint(input, &mut offset)?;
        let field = key >> 3;
        match key & 7 {
            0 => {
                protobuf_varint(input, &mut offset)?;
            }
            2 => {
                let len = usize::try_from(protobuf_varint(input, &mut offset)?)?;
                let end = offset
                    .checked_add(len)
                    .context("protobuf field length overflow")?;
                if end > input.len() {
                    bail!("protobuf field exceeds L document");
                }
                if field == expected_field && selected.replace(offset..end).is_some() {
                    bail!("L document repeats protobuf field {expected_field}");
                }
                offset = end;
            }
            wire => bail!("unsupported L fixture protobuf wire type {wire}"),
        }
    }
    selected.context("L document lacks its target schema field")
}

fn protobuf_varint(input: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..=63).step_by(7) {
        let byte = *input.get(*offset).context("truncated L protobuf varint")?;
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("L protobuf varint exceeds 64 bits")
}

// Capture the real registration snapshot, never a guessed publication count.
fn capture_empty_management_snapshot(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
    phase: &str,
) -> Result<serde_json::Value> {
    use super::mv_uea7_documents::wire::Graph;

    let metadata = super::mv_uea7_documents::load_metadata(context, rest_uri, namespace, table)?;
    fs::write(
        context
            .scenario_root()
            .join(format!("old-l-{phase}-metadata.json")),
        serde_json::to_vec_pretty(&metadata)?,
    )?;
    let graph = Graph::from_metadata(&metadata)?;
    ensure!(
        graph.snapshots.len() == 1,
        "empty management target must have exactly its registration snapshot"
    );
    let (snapshot_id, publication) = &graph.snapshots[0];
    ensure!(
        graph.current == Some(*snapshot_id)
            && metadata["refs"]["main"]["snapshot-id"].as_i64() == Some(*snapshot_id)
            && publication.is_none(),
        "empty management snapshot is not exact Current/main without P"
    );
    let snapshot = metadata["snapshots"]
        .as_array()
        .and_then(|snapshots| snapshots.first())
        .context("empty management target has no raw snapshot")?;
    let summary = snapshot["summary"]
        .as_object()
        .context("empty management snapshot has no summary")?;
    ensure!(
        summary.get("operation").and_then(serde_json::Value::as_str) == Some("append"),
        "first management snapshot must be an empty append"
    );
    for key in ["total-data-files", "total-delete-files", "total-records"] {
        ensure!(
            summary.get(key).and_then(serde_json::Value::as_str) == Some("0"),
            "empty management snapshot {key} is not exactly zero"
        );
    }
    Ok(metadata)
}

fn assert_rest_snapshot_count(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
    expected: usize,
) -> Result<()> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read old-format MV snapshot count")?)
        .build()?
        .get(&url)
        .send()?
        .error_for_status()?
        .json()?;
    let count = loaded["metadata"]["snapshots"]
        .as_array()
        .context("REST MV metadata has no snapshots")?
        .len();
    if count != expected {
        bail!("old-format MV has {count} snapshots, expected {expected}");
    }
    Ok(())
}

fn assert_rest_snapshot_unchanged(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
    expected_snapshot_id: i64,
) -> Result<()> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read exact Current MV snapshots")?)
        .build()?
        .get(&url)
        .send()?
        .error_for_status()?
        .json()?;
    let metadata = loaded
        .get("metadata")
        .context("REST table load has no metadata")?;
    let current = metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .context("REST table load has no current snapshot")?;
    let count = metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .context("REST table load has no snapshot list")?
        .len();
    if current != expected_snapshot_id || count != 1 {
        bail!(
            "stale MV refresh changed main or added a snapshot: current={current}, expected={expected_snapshot_id}, snapshots={count}"
        );
    }
    Ok(())
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

fn assert_mv_quarantined_after_base_replacement(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    context.remaining("verify MV is quarantined after same-name base replacement")?;
    context.action("verify MV is quarantined after same-name base replacement");
    let views: Vec<Row> = conn
        .query("SHOW MATERIALIZED VIEWS FROM ns")
        .context("list MVs after same-name base replacement")?;
    let manageability = views
        .iter()
        .find(|row| row.get::<String, _>("Name").as_deref() == Some(mv))
        .and_then(|row| row.get::<String, _>("Manageability"))
        .context("quarantined MV is missing from SHOW MATERIALIZED VIEWS")?;
    if !manageability.starts_with("UNAVAILABLE:")
        || !manageability
            .contains("published MV base occurrence 0 no longer resolves to its frozen object")
    {
        bail!(
            "same-name replacement MV is listed as {manageability:?}, expected source identity quarantine; {}",
            context.diagnostics()
        );
    }
    let closed = status(context, conn, catalog, mv)?;
    let challenge = property(&closed, "Challenge")?;
    let previous_incarnation = property(&closed, "UnsettledEffect1Incarnation")?;
    context.remaining("reject readmission onto a same-name replacement base")?;
    context.action("reject readmission onto a same-name replacement base");
    let readmission_error = match conn.query_drop(format!(
        "CALL novarocks_mv_resume_management('{catalog}', 'ns', '{mv}', \
         '{challenge}', '{previous_incarnation}', 'uea7-system-runner', \
         'the system scenario replaced the declared frontend process before this statement')"
    )) {
        Err(error) => error,
        Ok(()) => bail!("a replacement base readmitted the old MV unexpectedly"),
    };
    if !readmission_error
        .to_string()
        .contains("same-name relation was rebuilt with a different object identity")
    {
        bail!("replacement-base readmission failed for another reason: {readmission_error}");
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
    if !message.contains("MV target requires a successful fresh Current observation") {
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

/// Retain the original domain fact in the error chain. Display is the producer's
/// safe canonical projection, explicitly not an observed client SQL response.
#[derive(Debug)]
struct SchedulerRequestBlocked {
    fact: novarocks_execution_contract::failure::FailureFact,
    public_message: String,
}
impl std::fmt::Display for SchedulerRequestBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MV scheduler request outcome: {}; original fact={:?}; recovery row oracle not reached",
            self.public_message, self.fact
        )
    }
}
impl std::error::Error for SchedulerRequestBlocked {}

fn check_scheduler_outcome(
    directory: &Path,
    grant_id: &str,
    mv: &str,
    expected: &novarocks_failpoint::target_binding::RoleProcessReceipt,
) -> Result<()> {
    use prost::Message;
    use std::io::Read;
    expected
        .validate()
        .context("invalid authenticated scheduler observation scope")?;
    ensure!(
        expected.role == novarocks_failpoint::session::SessionRole::Fe,
        "scheduler observation expected scope is not FE"
    );
    let error_path = directory.join("scheduler-outcome-error.json");
    ensure!(
        !error_path.exists(),
        "MV scheduler observation rejected its frozen binding; outcome UNKNOWN; {}",
        error_path.display()
    );
    let path = directory.join("scheduler-outcome.json");
    const MAX_BYTES: usize = 64 * 1024;
    let file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("open finite scheduler outcome {}", path.display()));
        }
    };
    let mut raw = Vec::new();
    file.take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut raw)
        .context("read bounded scheduler outcome; outcome UNKNOWN")?;
    ensure!(
        raw.len() <= MAX_BYTES,
        "finite scheduler outcome is oversized; outcome UNKNOWN"
    );
    let record: serde_json::Value =
        serde_json::from_slice(&raw).context("decode finite scheduler outcome; outcome UNKNOWN")?;
    ensure!(
        record["schema_version"] == 1
            && record["semantic_scope"] == "scheduler-request-outcome"
            && record["decision"] == "Blocked"
            && record["grant_id"] == grant_id
            && record["target"]["catalog"] == "system_mv_scheduler"
            && record["target"]["namespace"] == "ns"
            && record["target"]["mv"] == mv,
        "finite scheduler outcome has a foreign target/grant or invalid shape; outcome UNKNOWN"
    );
    ensure!(
        record["session_id"] == expected.session_id
            && record["launch_id"] == expected.launch_id
            && record["process"] == expected.process_id,
        "finite scheduler outcome differs from the current authenticated FE session/launch/process; outcome UNKNOWN"
    );
    ensure!(
        record["admission_observation_sequence"]
            .as_u64()
            .is_some_and(|n| n > 0),
        "scheduler outcome has no admitted request; outcome UNKNOWN"
    );
    let first = &record["first_admitted_source"];
    let blocked = &record["blocked_request_source"];
    for key in [
        "mv_id",
        "target",
        "target_object",
        "definition_revision",
        "interpretation_revision",
        "configuration_revision",
        "deployment_owner",
        "process_incarnation",
    ] {
        ensure!(
            !first[key].is_null() && first[key] == blocked[key],
            "scheduler outcome rebound frozen target field {key}; outcome UNKNOWN"
        );
    }
    for source in [first, blocked] {
        for key in [
            "target",
            "target_object",
            "metadata_version",
            "publication_output_version",
            "deployment_owner",
            "process_incarnation",
        ] {
            ensure!(
                source[key]
                    .as_str()
                    .is_some_and(|s| !s.is_empty() && s.len() <= 8192),
                "invalid scheduler source identity; outcome UNKNOWN"
            );
        }
        if !source["publication_revision"].is_null() {
            let bytes: Vec<u8> = serde_json::from_value(source["publication_revision"].clone())
                .context("invalid publication source revision; outcome UNKNOWN")?;
            ensure!(
                bytes.len() == 32,
                "invalid publication source revision width; outcome UNKNOWN"
            );
        }
        ensure!(
            source["mv_id"].as_i64().is_some(),
            "scheduler outcome omitted actual mv_id; outcome UNKNOWN"
        );
        for key in [
            "definition_revision",
            "interpretation_revision",
            "configuration_revision",
        ] {
            let bytes: Vec<u8> = serde_json::from_value(source[key].clone())
                .context("invalid source revision; outcome UNKNOWN")?;
            ensure!(
                bytes.len() == 32,
                "invalid source revision width; outcome UNKNOWN"
            );
        }
        ensure!(
            source.get("publication_revision").is_some()
                && source["metadata_version"].is_string()
                && source["publication_output_version"].is_string(),
            "scheduler outcome omitted request P/provider revision; outcome UNKNOWN"
        );
    }
    for key in [
        "actual_publication_attempt",
        "actual_task_attempt",
        "actual_base_snapshots",
    ] {
        ensure!(
            record[key] == "NOT_OBSERVED",
            "scheduler outcome claimed an unobserved pipeline witness; outcome UNKNOWN"
        );
    }
    ensure!(
        record["failure_identity"] == "Source",
        "Blocked scheduler outcome lacks original typed Source identity; outcome UNKNOWN"
    );
    let bytes: Vec<u8> = serde_json::from_value(record["failure_fact"].clone())
        .context("Blocked scheduler outcome has no typed Source fact; outcome UNKNOWN")?;
    let dto = novarocks_proto_models::novarocks::FailureFact::decode(bytes.as_slice())
        .context("invalid scheduler failure fact; outcome UNKNOWN")?;
    let fact = novarocks_task_codec::status::decode_failure_fact(
        &dto,
        novarocks_proto_codec::FieldPath::root("scheduler_request_outcome").field("failure_fact"),
    )?;
    ensure!(
        novarocks_task_codec::status::encode_failure_fact(fact).encode_to_vec() == bytes,
        "scheduler outcome has noncanonical failure bytes; outcome UNKNOWN"
    );
    use novarocks_query_application::execution_error::{
        ExecutionFailureInput, ExecutionFailureSite,
    };
    let public_message = ExecutionFailureInput::new(
        fact,
        novarocks_execution_contract::SafeDetail::truncating(
            "MV scheduler request blocked; source diagnostic retained in FE role logs",
        ),
        ExecutionFailureSite::Coordinator,
    )
    .user_error()
    .to_string();
    ensure!(
        record["public_message"] == public_message,
        "scheduler outcome message disagrees with its decoded domain fact; outcome UNKNOWN"
    );
    Err(anyhow::Error::new(SchedulerRequestBlocked {
        fact,
        public_message,
    })
    .context(format!(
        "actual Blocked scheduler request recorded at {}",
        path.display()
    )))
}

struct SchedulerOutcomeTarget {
    directory: PathBuf,
    grant_id: &'static str,
    mv: &'static str,
}

fn wait_for_rows(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    sql: &str,
    expected: &[(i32, i64)],
    action: &str,
    outcome: SchedulerOutcomeTarget,
) -> Result<()> {
    context.action(action);
    loop {
        let scope = context.handle().mv_scheduler_outcome_binding(
            outcome.grant_id,
            "system_mv_scheduler",
            "ns",
            outcome.mv,
            &outcome.directory,
        )?;
        check_scheduler_outcome(&outcome.directory, outcome.grant_id, outcome.mv, &scope)?;
        let actual = conn.query::<(i32, i64), _>(sql);
        if let Ok(rows) = actual
            && rows == expected
        {
            return Ok(());
        }
        if context.remaining(action).is_err() {
            let observed = conn.query::<(i32, i64), _>(sql).ok();
            // Keep the controlled fixture result in the structured evidence so
            // an asynchronous refresh timeout remains diagnosable after the
            // harness redacts its full process-log diagnostic.
            context.action(format!(
                "{action} timed out with observed rows {observed:?}"
            ));
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

fn wait_for_file(context: &mut ScenarioContext, path: &Path, action: &str) -> Result<()> {
    context.action(action);
    while !path.exists() {
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
    Ok(())
}

fn wait_for_file_or_query(
    context: &mut ScenarioContext,
    path: &Path,
    query: &control_plan::ObservedQuery,
    action: &str,
) -> Result<()> {
    context.action(action);
    while !path.exists() {
        if query.is_finished() {
            bail!(
                "rewritten query completed before its completed-plan barrier; {}",
                context.diagnostics()
            );
        }
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
    Ok(())
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

fn wait_for_fe_marker(
    context: &mut ScenarioContext,
    producer: &JoinedSqlResult,
    marker: &str,
    action: &str,
) -> Result<()> {
    context.action(action);
    loop {
        if producer
            .observe_required_marker(|| Ok(context.handle().fe_log_contents()?.contains(marker)))?
        {
            return Ok(());
        }
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
}

struct JoinedSqlResult {
    result: Receiver<Result<()>>,
    actor: Mutex<Option<thread::JoinHandle<Result<()>>>>,
}
impl JoinedSqlResult {
    // Every caller holds an explicit refresh trigger until this marker. A
    // completed SQL producer without the marker cannot still reach the barrier.
    fn observe_required_marker(
        &self,
        mut marker_present: impl FnMut() -> Result<bool>,
    ) -> Result<bool> {
        // Keep the result untouched once the barrier is satisfied: the caller
        // still owns its later post-mutation or post-crash result assertion.
        if marker_present()? {
            return Ok(true);
        }
        let finished = self
            .actor
            .lock()
            .map_err(|_| anyhow::anyhow!("MV SQL actor lock poisoned"))?
            .as_ref()
            .is_none_or(thread::JoinHandle::is_finished);
        if !finished {
            return Ok(false);
        }
        // Read again after producer exit, so a marker emitted between the first
        // read and SQL completion still belongs to the satisfied barrier.
        if marker_present()? {
            return Ok(true);
        }
        match self.result.try_recv() {
            Err(mpsc::TryRecvError::Empty) => {
                self.join()?;
                bail!("original MV SQL producer exited without publishing its result")
            }
            Ok(Err(primary)) => {
                let primary = match self.join() {
                    Ok(()) => primary,
                    Err(cleanup) => {
                        primary.context(format!("original MV actor cleanup: {cleanup:#}"))
                    }
                };
                Err(primary.context("original MV SQL failed before its required FE marker"))
            }
            Ok(Ok(())) => {
                self.join()?;
                bail!("original MV SQL succeeded without reaching its required FE marker")
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                self.join()?;
                bail!("original MV SQL producer closed without publishing its result")
            }
        }
    }
    fn recv_timeout(&self, timeout: Duration) -> Result<Result<()>> {
        let result = self
            .result
            .recv_timeout(timeout)
            .context("receive original MV SQL result");
        let cleanup = self.join();
        match (result, cleanup) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(primary), Err(cleanup)) | (Ok(Err(primary)), Err(cleanup)) => {
                Err(primary.context(format!("original MV actor cleanup: {cleanup:#}")))
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }
    fn join(&self) -> Result<()> {
        if let Some(actor) = self
            .actor
            .lock()
            .map_err(|_| anyhow::anyhow!("MV SQL actor lock poisoned"))?
            .take()
        {
            actor
                .join()
                .map_err(|_| anyhow::anyhow!("original MV SQL actor panicked"))??;
        }
        Ok(())
    }
}
impl Drop for JoinedSqlResult {
    fn drop(&mut self) {
        if let Err(error) = self.join() {
            eprintln!("Original MV SQL actor cleanup failed: {error:#}");
        }
    }
}

fn spawn_refresh(
    user: String,
    port: u16,
    catalog: &str,
    mv: &str,
    deadline: Instant,
) -> JoinedSqlResult {
    let catalog = catalog.to_string();
    let mv = mv.to_string();
    spawn_sql_actor(move || {
        mysql_actor::connect_until(&user, port, deadline)?.run(|conn| {
            conn.query_drop(format!("SET CATALOG {catalog}"))?;
            conn.query_drop("USE ns")?;
            conn.query_drop(format!("REFRESH MATERIALIZED VIEW {mv}"))?;
            Ok(())
        })
    })
}

fn spawn_sql_actor(operation: impl FnOnce() -> Result<()> + Send + 'static) -> JoinedSqlResult {
    let (sender, receiver) = mpsc::sync_channel(1);
    let actor = thread::spawn(move || -> Result<()> {
        match sender.send(operation()) {
            Ok(()) => Ok(()),
            Err(mpsc::SendError(Err(primary))) => {
                Err(primary.context("MV SQL result owner closed before delivery"))
            }
            Err(mpsc::SendError(Ok(()))) => {
                bail!("MV SQL result owner closed before delivery")
            }
        }
    });
    JoinedSqlResult {
        result: receiver,
        actor: Mutex::new(Some(actor)),
    }
}

fn expect_refresh_failure(
    context: &mut ScenarioContext,
    receiver: JoinedSqlResult,
    action: &str,
) -> Result<()> {
    context.action(action);
    let timeout = context.remaining(action)?;
    match receiver.recv_timeout(timeout) {
        Ok(Err(error)) if !error.to_string().is_empty() => Ok(()),
        Ok(Err(_)) => bail!("{action} returned an empty error"),
        Ok(Ok(())) => bail!("{action} unexpectedly succeeded"),
        Err(error) => bail!("{action} did not finish before deadline: {error}"),
    }
}

fn resume_and_refresh_after_fe_restart(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    resume_management_after_fe_restart(context, conn, catalog, mv)?;
    refresh(context, conn, mv)
}

fn resume_management_after_fe_restart(
    context: &mut ScenarioContext,
    conn: &mut Conn,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    let closed = wait_for_status_phase(
        context,
        conn,
        catalog,
        mv,
        "AWAITING_EFFECT_SETTLEMENT",
        "wait for post-crash MV management to require effect settlement",
    )?;
    let challenge = property(&closed, "Challenge")?;
    let previous_incarnation = property(&closed, "UnsettledEffect1Incarnation")?;
    context.action("declare the crashed FE isolated and resume exact MV management");
    let resumed: Vec<(String, Option<String>)> = conn
        .query(format!(
            "CALL novarocks_mv_resume_management('{catalog}', 'ns', '{mv}', \
             '{challenge}', '{previous_incarnation}', 'uea7-system-runner', \
             'the system scenario replaced the declared frontend process before this statement')"
        ))
        .context("resume managed MV after frontend replacement")?;
    if property(&resumed, "SettledEffects")? != "1" {
        bail!("frontend replacement did not settle the old FE effect");
    }
    Ok(())
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

#[cfg(test)]
mod control_plan_tests {
    use super::*;
    use serde_json::{Value, json};

    struct PlanInputs {
        root: tempfile::TempDir,
        binary: PathBuf,
    }
    impl PlanInputs {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let binary = root.path().join("binary-selection");
            // Selection-only fixtures; no executable is started or self-report trusted.
            fs::write(&binary, b"selection fixture").unwrap();
            Self { root, binary }
        }
        fn summaries(&self, scenario: &dyn Scenario) -> Value {
            let deadline = Instant::now() + Duration::from_secs(5);
            let plan = scenario
                .test_session_plan(
                    &ScenarioLaunchConfig::default(),
                    self.root.path(),
                    3,
                    &self.binary,
                    deadline,
                )
                .unwrap()
                .unwrap();
            assert_eq!(plan.deadline(), deadline);
            let summaries = plan.planned_launch_summaries();
            assert!(
                summaries
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|launch| launch["binary"] == json!(self.binary))
            );
            summaries
        }
    }
    fn frontend_launches(value: &Value) -> Vec<&Value> {
        value
            .as_array()
            .unwrap()
            .iter()
            .filter(|launch| launch["role"] == "fe")
            .collect()
    }
    fn actions(launch: &Value) -> Vec<&str> {
        launch["grants"]
            .as_array()
            .unwrap()
            .iter()
            .map(|grant| grant["action"].as_str().unwrap())
            .collect()
    }
    fn assert_backends_ordinary(value: &Value) {
        for launch in value
            .as_array()
            .unwrap()
            .iter()
            .filter(|launch| launch["role"].as_str().unwrap().starts_with("be-"))
        {
            assert_eq!(launch["ordinary"], true);
            assert!(launch["grants"].as_array().unwrap().is_empty());
        }
    }

    #[test]
    fn staged_recovery_grants_follow_the_two_actual_restart_boundaries() {
        let input = PlanInputs::new();
        let value = input.summaries(&MvStagedPublishedRecovery::default());
        let launches = frontend_launches(&value);
        assert_eq!(launches.len(), 3);
        assert_eq!(actions(launches[0]), ["recovery-hold"]);
        assert_eq!(actions(launches[1]), ["recovery-hold"]);
        assert_eq!(launches[0]["ordinary"], false);
        assert_eq!(launches[1]["ordinary"], false);
        assert_ne!(launches[0]["session_id"], launches[1]["session_id"]);
        assert_eq!(launches[2]["ordinary"], true);
        assert!(actions(launches[2]).is_empty());
        assert_backends_ordinary(&value);
    }

    fn scheduler_expected_receipt() -> novarocks_failpoint::target_binding::RoleProcessReceipt {
        novarocks_failpoint::target_binding::RoleProcessReceipt {
            schema_version: 1,
            session_id: "original-session".into(),
            launch_id: "original-launch".into(),
            role: novarocks_failpoint::session::SessionRole::Fe,
            process_id: "019be123-4567-7abc-8def-0123456789ab".into(),
        }
    }
    fn scheduler_outcome_record() -> serde_json::Value {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let fact = FailureFact::new(
            FailureClass::Unsupported,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(1).unwrap()),
        );
        scheduler_outcome_record_for_fact(fact)
    }
    fn scheduler_outcome_record_for_fact(
        fact: novarocks_execution_contract::failure::FailureFact,
    ) -> serde_json::Value {
        use prost::Message;
        let source = json!({"mv_id":7,"target":"actual target","target_object":"actual object digest",
            "definition_revision":vec![1u8;32],"interpretation_revision":vec![2u8;32],"configuration_revision":vec![3u8;32],
            "publication_revision":null,"metadata_version":"actual metadata revision","publication_output_version":"None",
            "deployment_owner":"owner","process_incarnation":"incarnation"});
        json!({"schema_version":1,"semantic_scope":"scheduler-request-outcome","decision":"Blocked",
            "grant_id":"outcome-a","session_id":"original-session","launch_id":"original-launch","process":scheduler_expected_receipt().process_id,
            "target":{"catalog":"system_mv_scheduler","namespace":"ns","mv":"orders_mv_a"},
            "first_admitted_source":source,"blocked_request_source":source,"admission_observation_sequence":2,
            "failure_identity":"Source", "failure_fact":novarocks_task_codec::status::encode_failure_fact(fact).encode_to_vec(),
            "public_message":novarocks_query_application::execution_error::ExecutionFailureInput::new(fact,
                novarocks_execution_contract::SafeDetail::truncating("MV scheduler request blocked; source diagnostic retained in FE role logs"),
                novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator).user_error().to_string(),
            "actual_publication_attempt":"NOT_OBSERVED","actual_task_attempt":"NOT_OBSERVED","actual_base_snapshots":"NOT_OBSERVED"})
    }
    #[test]
    fn scheduler_late_peek_preserves_typed_blocked_error_and_read_does_not_mutate() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            check_scheduler_outcome(
                dir.path(),
                "outcome-a",
                "orders_mv_a",
                &scheduler_expected_receipt()
            )
            .is_ok()
        );
        let bytes = serde_json::to_vec(&scheduler_outcome_record()).unwrap();
        let path = dir.path().join("scheduler-outcome.json");
        fs::write(&path, &bytes).unwrap();
        for _ in 0..2 {
            let error = check_scheduler_outcome(
                dir.path(),
                "outcome-a",
                "orders_mv_a",
                &scheduler_expected_receipt(),
            )
            .unwrap_err();
            let blocked = error.downcast_ref::<SchedulerRequestBlocked>().unwrap();
            assert_eq!(
                blocked.fact.class(),
                novarocks_execution_contract::failure::FailureClass::Unsupported
            );
        }
        assert_eq!(fs::read(path).unwrap(), bytes);
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let invariant = FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(9).unwrap()),
        );
        let second = tempfile::tempdir().unwrap();
        let record = scheduler_outcome_record_for_fact(invariant);
        fs::write(
            second.path().join("scheduler-outcome.json"),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        let error = check_scheduler_outcome(
            second.path(),
            "outcome-a",
            "orders_mv_a",
            &scheduler_expected_receipt(),
        )
        .unwrap_err();
        let blocked = error.downcast_ref::<SchedulerRequestBlocked>().unwrap();
        assert_eq!(blocked.fact, invariant);
        assert_eq!(
            blocked.public_message,
            record["public_message"].as_str().unwrap()
        );
        assert!(!blocked.public_message.contains("UnsupportedCompiledShape"));
    }
    #[test]
    fn scheduler_unknown_corruption_foreign_target_and_rebinding_are_not_typed_refusals() {
        for variant in 0..12 {
            let dir = tempfile::tempdir().unwrap();
            let mut record = scheduler_outcome_record();
            match variant {
                0 => record["failure_fact"] = serde_json::Value::Null,
                1 => record["target"]["mv"] = json!("other"),
                2 => {
                    record["blocked_request_source"]["configuration_revision"] =
                        json!(vec![9u8; 32])
                }
                3 => record["failure_fact"] = json!(vec![255u8]),
                4 => record["actual_task_attempt"] = json!("invented"),
                5 => record["session_id"] = json!("another-session"),
                6 => record["launch_id"] = json!("another-launch"),
                7 => record["process"] = json!("019be123-4567-7abc-8def-0123456789ac"),
                8 => {
                    record["public_message"] = json!("[UnsupportedCompiledShape] forged diagnostic")
                }
                9 => record["extra"] = json!("x".repeat(65537)),
                10 => {
                    use novarocks_execution_contract::failure::{
                        FailureClass, FailureFact, FailureReason, domain,
                    };
                    use prost::Message;
                    let invariant = FailureFact::new(
                        FailureClass::Invariant,
                        FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(9).unwrap()),
                    );
                    record["failure_fact"] = json!(
                        novarocks_task_codec::status::encode_failure_fact(invariant)
                            .encode_to_vec()
                    );
                    // Keep the old Unsupported display: its fact is now Invariant.
                }
                11 => {
                    record["public_message"] =
                        json!("[query.execution.invariant] forged diagnostic")
                }
                _ => unreachable!(),
            }
            fs::write(
                dir.path().join("scheduler-outcome.json"),
                serde_json::to_vec(&record).unwrap(),
            )
            .unwrap();
            let error = check_scheduler_outcome(
                dir.path(),
                "outcome-a",
                "orders_mv_a",
                &scheduler_expected_receipt(),
            )
            .unwrap_err();
            assert!(error.downcast_ref::<SchedulerRequestBlocked>().is_none());
        }
    }

    #[test]
    fn scheduler_successor_only_receives_its_remaining_transient_action() {
        let input = PlanInputs::new();
        let value = input.summaries(&MvSchedulerRecovery::default());
        let launches = frontend_launches(&value);
        assert_eq!(launches.len(), 2);
        assert_eq!(
            actions(launches[0]),
            [
                "scheduler-outcome",
                "scheduler-outcome",
                "scheduler-outcome",
                "scheduler-hold",
                "scheduler-hold",
                "scheduler-hold"
            ]
        );
        assert_eq!(
            actions(launches[1]),
            ["scheduler-outcome", "transient-preparation"]
        );
        assert_backends_ordinary(&value);
    }

    #[test]
    fn lake_wipe_is_not_inherited_by_the_ordinary_successor() {
        let input = PlanInputs::new();
        let value = input.summaries(&MvLakePublicationRestartRebuild::default());
        let launches = frontend_launches(&value);
        assert_eq!(launches.len(), 2);
        assert_eq!(
            actions(launches[0]),
            ["document-observation", "accelerator-wipe"]
        );
        assert_eq!(launches[0]["binary"], json!(input.binary));
        assert_eq!(launches[1]["ordinary"], true);
        assert_eq!(launches[1]["binary"], json!(input.binary));
        assert!(actions(launches[1]).is_empty());
        assert_backends_ordinary(&value);
    }

    #[test]
    fn retained_state_restart_does_not_request_any_control_capability() {
        let input = PlanInputs::new();
        let value = input.summaries(&MvStateStoreRestart::default());
        assert_eq!(frontend_launches(&value).len(), 2);
        for launch in value.as_array().unwrap() {
            assert_eq!(launch["ordinary"], true);
            assert!(actions(launch).is_empty());
            assert_eq!(launch["binary"], json!(input.binary));
        }
    }

    #[test]
    fn first_refresh_fault_targets_only_the_selected_backend_launch() {
        let input = PlanInputs::new();
        let value = input.summaries(&MvFirstRefreshStaging::default());
        let launches = value.as_array().unwrap();
        let frontend = launches
            .iter()
            .find(|launch| launch["role"] == "fe")
            .unwrap();
        assert_eq!(
            actions(frontend),
            ["bind-task-execution-failure", "observe"]
        );
        let backend = launches
            .iter()
            .find(|launch| launch["role"] == "be-0")
            .unwrap();
        assert_eq!(backend["ordinary"], false);
        assert_eq!(actions(backend), ["task-execution-failure"]);
        for launch in launches
            .iter()
            .filter(|launch| launch["role"] == "be-1" || launch["role"] == "be-2")
        {
            assert_eq!(launch["ordinary"], true);
            assert!(actions(launch).is_empty());
        }
        assert!(
            launches
                .iter()
                .flat_map(|launch| actions(launch))
                .all(|action| action != "accelerator-wipe")
        );
    }

    #[test]
    fn storage_rewrite_observer_does_not_survive_the_ordinary_restart_slot() {
        let input = PlanInputs::new();
        let value =
            input.summaries(&crate::scenarios::mv_uea7_storage::MvStorageContract::default());
        let launches = frontend_launches(&value);
        assert_eq!(launches.len(), 2);
        assert_eq!(actions(launches[0]), ["rewrite-hold", "observe"]);
        assert_eq!(launches[1]["ordinary"], true);
        assert!(actions(launches[1]).is_empty());
        assert_backends_ordinary(&value);
    }

    #[test]
    fn selected_mv_control_requires_an_existing_primary_binary() {
        let input = PlanInputs::new();
        let result = MvLakePublicationRestartRebuild::default().test_session_plan(
            &ScenarioLaunchConfig::default(),
            input.root.path(),
            3,
            &input.root.path().join("missing-primary"),
            Instant::now() + Duration::from_secs(5),
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("binary is missing")
        );
    }
}

#[cfg(test)]
mod refresh_marker_tests {
    use super::*;

    fn query_failure() -> anyhow::Error {
        anyhow::Error::from(::mysql::Error::MySqlError(::mysql::MySqlError {
            code: 1146,
            state: "42S02".into(),
            message: "original refresh SQL failed".into(),
        }))
        .context("execute original refresh")
    }

    fn assert_original_mysql_error(error: &anyhow::Error) {
        let Some(::mysql::Error::MySqlError(actual)) = error.downcast_ref::<::mysql::Error>()
        else {
            panic!("original typed MySQL error missing: {error:#}");
        };
        assert_eq!(actual.code, 1146);
        assert_eq!(actual.state, "42S02");
        assert_eq!(actual.message, "original refresh SQL failed");
    }

    #[test]
    fn source_hold_expected_refusal_keeps_the_original_typed_sql_cause() {
        let outcome =
            source_hold_outcomes(Some(Err(query_failure())), Ok(()), Ok(()), Ok(()), Ok(()))
                .unwrap();
        assert_original_mysql_error(&outcome.unwrap_err());
    }

    #[test]
    fn source_hold_release_failure_cannot_replace_an_original_sql_failure() {
        let error = source_hold_outcomes(
            Some(Err(query_failure())),
            Ok(()),
            Ok(()),
            Ok(()),
            Err(anyhow::anyhow!("original trigger release failed")),
        )
        .unwrap_err();
        assert_original_mysql_error(&error);
        assert!(format!("{error:#}").contains("original trigger release failed"));
    }

    #[test]
    fn source_hold_deadline_and_join_failure_remain_contexts_of_observed_sql_failure() {
        let error = source_hold_outcomes(
            Some(Err(query_failure())),
            Ok(()),
            Err(anyhow::anyhow!("original remaining deadline expired")),
            Err(anyhow::anyhow!("original actor join failed")),
            Ok(()),
        )
        .unwrap_err();
        assert_original_mysql_error(&error);
        let detail = format!("{error:#}");
        assert!(detail.contains("original remaining deadline expired"));
        assert!(detail.contains("original actor join failed"));
        // Successful SQL plus an infrastructure failure is never returned as
        // either an expected SQL refusal or a successful held refresh.
        assert!(
            source_hold_outcomes(
                Some(Ok(())),
                Ok(()),
                Err(anyhow::anyhow!("original receive failed")),
                Ok(()),
                Ok(()),
            )
            .is_err()
        );
    }
    #[test]
    fn pre_marker_failure_preserves_original_mysql_code_and_state() {
        let producer = spawn_sql_actor(|| Err(query_failure()));
        producer.join().unwrap();
        let error = producer.observe_required_marker(|| Ok(false)).unwrap_err();
        assert_original_mysql_error(&error);
    }

    #[test]
    fn pre_marker_connection_failure_is_published_before_any_sql() {
        let producer = spawn_sql_actor(|| {
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "original connect",
            ))
            .context("connect original refresh actor")
        });
        producer.join().unwrap();
        let error = producer.observe_required_marker(|| Ok(false)).unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::ConnectionRefused
        );
    }

    #[test]
    fn successful_producer_without_required_marker_fails_instead_of_waiting() {
        let producer = spawn_sql_actor(|| Ok(()));
        producer.join().unwrap();
        assert!(producer.observe_required_marker(|| Ok(false)).is_err());
    }

    #[test]
    fn satisfied_marker_does_not_consume_successful_producer_result() {
        let producer = spawn_sql_actor(|| Ok(()));
        producer.join().unwrap();
        assert!(producer.observe_required_marker(|| Ok(true)).unwrap());
        producer.recv_timeout(Duration::ZERO).unwrap().unwrap();
    }

    #[test]
    fn satisfied_marker_keeps_later_post_crash_failure_assertion_owned() {
        let producer = spawn_sql_actor(|| Err(query_failure()));
        producer.join().unwrap();
        assert!(producer.observe_required_marker(|| Ok(true)).unwrap());
        let error = producer.recv_timeout(Duration::ZERO).unwrap().unwrap_err();
        assert_original_mysql_error(&error);
    }

    #[test]
    fn in_flight_producer_waits_without_consuming_its_future_result() {
        let (release, held) = mpsc::sync_channel(1);
        let producer = spawn_sql_actor(move || {
            held.recv().context("release held original SQL actor")?;
            Ok(())
        });
        assert!(!producer.observe_required_marker(|| Ok(false)).unwrap());
        release.send(()).unwrap();
        producer.join().unwrap();
        assert!(producer.observe_required_marker(|| Ok(true)).unwrap());
        producer.recv_timeout(Duration::ZERO).unwrap().unwrap();
    }

    #[test]
    fn marker_published_during_producer_completion_is_not_missing() {
        let producer = spawn_sql_actor(|| Ok(()));
        producer.join().unwrap();
        let mut reads = 0;
        assert!(
            producer
                .observe_required_marker(|| {
                    reads += 1;
                    Ok(reads == 2)
                })
                .unwrap()
        );
        assert_eq!(reads, 2);
        producer.recv_timeout(Duration::ZERO).unwrap().unwrap();
    }

    #[test]
    fn closed_receiver_keeps_original_mysql_error_in_producer_join() {
        let (release, held) = mpsc::sync_channel(1);
        let mut producer = spawn_sql_actor(move || {
            held.recv().context("release original producer")?;
            Err(query_failure())
        });
        let (replacement_sender, replacement_receiver) = mpsc::sync_channel(1);
        drop(replacement_sender);
        drop(std::mem::replace(
            &mut producer.result,
            replacement_receiver,
        ));
        release.send(()).unwrap();
        let error = producer.join().unwrap_err();
        assert_original_mysql_error(&error);
    }

    #[test]
    fn finished_producer_with_empty_result_owner_does_not_wait_for_marker() {
        let (sender, receiver) = mpsc::sync_channel(1);
        // Keep a foreign sender alive so this is Empty, not Disconnected. The
        // owned producer has already exited without delivering its result.
        let actor = thread::spawn(|| Ok(()));
        while !actor.is_finished() {
            thread::yield_now();
        }
        let producer = JoinedSqlResult {
            result: receiver,
            actor: Mutex::new(Some(actor)),
        };
        let error = producer.observe_required_marker(|| Ok(false)).unwrap_err();
        assert!(format!("{error:#}").contains("exited without publishing its result"));
        assert!(producer.actor.lock().unwrap().is_none());
        drop(sender);
    }

    #[test]
    fn disconnected_producer_without_result_is_not_a_marker_wait() {
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(sender);
        let producer = JoinedSqlResult {
            result: receiver,
            actor: Mutex::new(None),
        };
        assert!(producer.observe_required_marker(|| Ok(false)).is_err());
    }

    #[test]
    fn panicked_producer_is_reported_before_marker_deadline() {
        let producer = spawn_sql_actor(|| panic!("original SQL actor panic"));
        // Wait for the real thread to exit without consuming its join result.
        while !producer
            .actor
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_finished()
        {
            thread::yield_now();
        }
        let error = producer.observe_required_marker(|| Ok(false)).unwrap_err();
        assert!(format!("{error:#}").contains("original MV SQL actor panicked"));
    }
}
