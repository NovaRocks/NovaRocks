use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{CrossProcessConfigOverlay, ServerHandle};
use std::fs;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const REQUIRED_BACKENDS: usize = 3;
const POLL_INTERVAL: Duration = Duration::from_millis(25);

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(StaticFileDisposableCarrier),
        Box::new(CatalogPartialReadiness),
        Box::new(GracefulDrain),
        Box::new(ForcedDrain),
        Box::new(BlueGreenSessionCutover),
    ]
}

struct StaticFileDisposableCarrier;

struct CatalogPartialReadiness;

impl Scenario for CatalogPartialReadiness {
    fn name(&self) -> &'static str {
        "frontend-lifecycle/catalog-partial-readiness"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let warehouse = scenario_root.join("partial-ready-warehouse");
        fs::create_dir_all(&warehouse).with_context(|| {
            format!("create partial readiness warehouse {}", warehouse.display())
        })?;
        let snapshot = scenario_root.join("catalogs.toml");
        fs::write(
            &snapshot,
            format!(
                "format_version = 3\n\
                 [[catalogs]]\n\
                 instance_id = \"lnp8_healthy\"\n\
                 provider_id = \"iceberg\"\n\
                 display_name = \"lnp8_healthy\"\n\
                 config_format_version = 3\n\
                 credential_bindings = []\n\
                 [catalogs.properties]\n\
                 type = \"iceberg\"\n\
                 \"iceberg.catalog.type\" = \"hadoop\"\n\
                 \"iceberg.catalog.warehouse\" = \"{}\"\n\
                 [[catalogs]]\n\
                 instance_id = \"lnp8_unavailable\"\n\
                 provider_id = \"iceberg\"\n\
                 display_name = \"lnp8_unavailable\"\n\
                 config_format_version = 3\n\
                 credential_bindings = []\n\
                 [catalogs.properties]\n\
                 type = \"iceberg\"\n\
                 \"iceberg.catalog.type\" = \"hadoop\"\n",
                warehouse.display()
            ),
        )
        .with_context(|| format!("write partial readiness snapshot {}", snapshot.display()))?;
        Ok(ScenarioLaunchConfig {
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(format!(
                    "[catalog_source]\nmode = \"static-file\"\nstatic_file_path = \"{}\"\n",
                    snapshot.display()
                )),
                be: None,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let ready = context
            .handle()
            .frontend_management_get("/readyz", Duration::from_secs(2))?;
        let state = context
            .handle()
            .frontend_management_get("/v1/frontend/state", Duration::from_secs(2))?;
        ensure!(
            ready.status == 200,
            "partial catalog failure must not block readiness"
        );
        ensure!(
            state.status == 200 && state.body.contains("\"desired\":2"),
            "partial catalog bootstrap omitted its complete desired snapshot: {}",
            state.body
        );
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect healthy partial catalog")?,
        )?;
        connection
            .query_drop("SET CATALOG lnp8_healthy")
            .context("select healthy catalog after partial bootstrap")?;
        let rows: Vec<(i64,)> = connection
            .query("SELECT 1")
            .context("query through healthy partial catalog session")?;
        ensure!(rows == vec![(1,)]);
        let mut unavailable = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect unavailable partial catalog")?,
        )?;
        let error = format!(
            "{:#}",
            unavailable
                .query_drop("SET CATALOG lnp8_unavailable")
                .expect_err("invalid catalog definition must remain unavailable")
        );
        ensure!(
            error.contains("iceberg.catalog.warehouse"),
            "unavailable catalog returned an unexpected error: {error}"
        );
        context.action("proved one unavailable StaticFile catalog leaves the healthy catalog and FE readiness usable");
        Ok(())
    }
}

impl Scenario for StaticFileDisposableCarrier {
    fn name(&self) -> &'static str {
        "frontend-lifecycle/static-file-disposable-carrier"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let warehouse = scenario_root.join("static-file-warehouse");
        fs::create_dir_all(&warehouse)
            .with_context(|| format!("create StaticFile warehouse {}", warehouse.display()))?;
        let snapshot = scenario_root.join("catalogs.toml");
        fs::write(
            &snapshot,
            format!(
                "format_version = 3\n\
                 [[catalogs]]\n\
                 instance_id = \"lnp8_static\"\n\
                 provider_id = \"iceberg\"\n\
                 display_name = \"lnp8_static\"\n\
                 config_format_version = 3\n\
                 credential_bindings = []\n\
                 [catalogs.properties]\n\
                 type = \"iceberg\"\n\
                 \"iceberg.catalog.type\" = \"hadoop\"\n\
                 \"iceberg.catalog.warehouse\" = \"{}\"\n",
                warehouse.display()
            ),
        )
        .with_context(|| format!("write StaticFile snapshot {}", snapshot.display()))?;
        Ok(ScenarioLaunchConfig {
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(format!(
                    "[catalog_source]\nmode = \"static-file\"\nstatic_file_path = \"{}\"\n",
                    snapshot.display()
                )),
                be: None,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect StaticFile client")?,
        )?;
        connection
            .query_drop("SET CATALOG lnp8_static")
            .context("select StaticFile catalog")?;
        let namespace = format!("lnp8ns_{}", context.mysql_port());
        connection
            .query_drop(format!("CREATE DATABASE {namespace}"))
            .context("create StaticFile catalog namespace")?;
        connection
            .query_drop(format!(
                "CREATE TABLE {namespace}.orders (id INT, amount INT)"
            ))
            .context("create StaticFile catalog table")?;
        connection
            .query_drop(format!(
                "INSERT INTO {namespace}.orders VALUES (1, 10), (2, 20)"
            ))
            .context("write StaticFile catalog rows")?;
        let rows: Vec<(i32, i32)> = connection
            .query(format!(
                "SELECT id, amount FROM {namespace}.orders ORDER BY id"
            ))
            .context("query StaticFile catalog before FE disposal")?;
        ensure!(rows == vec![(1, 10), (2, 20)]);
        let state_before = context
            .handle()
            .frontend_management_get("/v1/frontend/state", Duration::from_secs(2))?;
        let digest_before = static_snapshot_digest(&state_before.body)?;
        ensure!(
            state_before
                .body
                .contains("\"source_mode\":\"static-file\"")
                && state_before.body.contains("\"desired\":1"),
            "StaticFile bootstrap state is incomplete: {}",
            state_before.body
        );
        context.action("captured the bootstrapped StaticFile catalog snapshot identity");

        drop(connection);
        let deadline = context.deadline();
        context
            .handle()
            .wipe_fe_state_store_and_restart_until(deadline)
            .context("dispose FE SQLite state and restart from StaticFile")?;
        let state_after = context
            .handle()
            .frontend_management_get("/v1/frontend/state", Duration::from_secs(2))?;
        ensure!(
            static_snapshot_digest(&state_after.body)? == digest_before,
            "StaticFile snapshot digest changed after FE disposal"
        );
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("reconnect after StaticFile disposal")?,
        )?;
        connection
            .query_drop("SET CATALOG lnp8_static")
            .context("reselect StaticFile catalog")?;
        let rebuilt: Vec<(i32, i32)> = connection
            .query(format!(
                "SELECT id, amount FROM {namespace}.orders ORDER BY id"
            ))
            .context("query StaticFile catalog after FE disposal")?;
        ensure!(
            rebuilt == rows,
            "StaticFile rows changed after FE disposal: {rebuilt:?}"
        );
        context.action("proved an empty FE SQLite store rebuilt the same StaticFile catalog state");
        Ok(())
    }
}

struct GracefulDrain;

struct ForcedDrain;

struct BlueGreenSessionCutover;

impl Scenario for BlueGreenSessionCutover {
    fn name(&self) -> &'static str {
        "frontend-lifecycle/blue-green-session-cutover"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let snapshot = scenario_root.join("blue-catalogs.toml");
        write_rest_static_snapshot(&snapshot, "lnp8_blue")?;
        Ok(static_snapshot_launch_config(&snapshot))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let green_snapshot = context.scenario_root().join("green-catalogs.toml");
        write_rest_static_snapshot(&green_snapshot, "lnp8_green")?;
        let mut green = context
            .launch_peer_cluster("green", static_snapshot_launch_config(&green_snapshot))
            .context("launch green 1FE+3BE cluster")?;
        let result = run_blue_green_cutover(context, &mut green);
        let cleanup = ServerHandle::shutdown(&mut green).context("stop green cluster");
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(cleanup)) => Err(cleanup),
            (Err(error), Err(cleanup)) => {
                Err(error.context(format!("green cleanup also failed: {cleanup:#}")))
            }
        }
    }
}

impl Scenario for ForcedDrain {
    fn name(&self) -> &'static str {
        "frontend-lifecycle/forced-drain"
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(
                    "[server]\nfrontend_drain_timeout_ms = 500\nfrontend_cleanup_timeout_ms = 2000\n"
                        .to_string(),
                ),
                be: None,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        let (query_tx, query_rx) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let result = (|| -> Result<Vec<i64>> {
                let mut connection = mysql_actor::connect(&user, port, Duration::from_secs(30))?;
                connection
                    .query("SELECT sleep(10)")
                    .context("run forced-drain query")
            })();
            let _ = query_tx.send(result);
        });
        wait_for_active_statement(context)?;
        context.action("observed an admitted statement before the short drain deadline");

        context
            .handle()
            .begin_fe_drain()
            .context("begin forced FE drain through SIGTERM")?;
        let result = query_rx
            .recv_timeout(context.remaining("wait for forced-drain cancellation")?)
            .map_err(|error| anyhow::anyhow!("forced-drain query did not finish: {error}"))?;
        let error = format!(
            "{:#}",
            result.expect_err("forced drain must cancel the statement")
        );
        ensure!(
            error.contains("FRONTEND_DRAIN_DEADLINE_EXCEEDED"),
            "forced drain did not preserve the typed deadline cancellation: {error}"
        );
        context.action("observed the typed frontend drain deadline cancellation");

        let deadline = context.deadline();
        context
            .handle()
            .wait_fe_exit_until(deadline)
            .context("wait for FE exit after forced drain")?;
        context.action("FE exited successfully within the configured forced-drain budget");
        Ok(())
    }
}

impl Scenario for GracefulDrain {
    fn name(&self) -> &'static str {
        "frontend-lifecycle/graceful-drain"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "graceful drain requires native 1FE+3BE"
        );
        context.action("verified native 1FE+3BE topology");

        let mut idle_connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            Duration::from_secs(10),
        )
        .context("open pre-drain idle MySQL session")?;
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        let (query_tx, query_rx) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let result = (|| -> Result<Vec<i64>> {
                let mut connection = mysql_actor::connect(&user, port, Duration::from_secs(30))?;
                connection
                    .query("SELECT sleep(10)")
                    .context("run admitted graceful-drain query")
            })();
            let _ = query_tx.send(result);
        });
        wait_for_active_statement(context)?;
        context.action("observed an admitted public MySQL statement lease");

        context
            .handle()
            .begin_fe_drain()
            .context("begin graceful FE drain through SIGTERM")?;
        wait_for_drain_observation(context)?;
        context.action(
            "observed ready=503, live=200, and Draining from the real FE management listener",
        );

        assert_idle_session_statement_is_rejected(&mut idle_connection)?;
        context
            .action("proved a pre-drain idle MySQL session receives the typed draining rejection");

        let query = query_rx
            .recv_timeout(context.remaining("wait for pre-drain statement")?)
            .map_err(|error| anyhow::anyhow!("pre-drain statement did not finish: {error}"))??;
        ensure!(
            query.len() == 1,
            "graceful-drain query did not return one completed row: {query:?}"
        );
        context.action("pre-drain statement completed normally after drain began");

        let deadline = context.deadline();
        context
            .handle()
            .wait_fe_exit_until(deadline)
            .context("wait for FE exit after graceful drain")?;
        context.action("FE exited successfully only after its admitted work completed");
        Ok(())
    }
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let count = context.handle().be_count();
    ensure!(
        count == REQUIRED_BACKENDS,
        "{} requires native 1FE+3BE, received 1FE+{count}BE",
        context.name()
    );
    context.action("verified native 1FE+3BE topology");
    Ok(())
}

fn static_snapshot_digest(state: &str) -> Result<String> {
    let marker = "\"digest\":\"";
    let start = state
        .find(marker)
        .map(|offset| offset + marker.len())
        .context("StaticFile management state omitted catalog snapshot digest")?;
    let end = state[start..]
        .find('"')
        .map(|offset| start + offset)
        .context("StaticFile management state has an unterminated catalog digest")?;
    Ok(state[start..end].to_string())
}

/// Catalog materialization begins asynchronously after the topology barrier.
/// The scenario proves its settled partial state, not the transient state in
/// which every desired catalog has been admitted but none has finished loading.
fn wait_for_active_statement(context: &mut ScenarioContext) -> Result<()> {
    loop {
        let timeout = context
            .remaining("observe admitted statement")?
            .min(Duration::from_secs(1));
        let response = context
            .handle()
            .frontend_management_get("/v1/frontend/state", timeout)?;
        if response.status == 200 && response.body.contains("\"statement\":1") {
            return Ok(());
        }
        if context.remaining("observe admitted statement")?.is_zero() {
            bail!("timed out observing active statement: {response:?}");
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_drain_observation(context: &mut ScenarioContext) -> Result<()> {
    loop {
        let timeout = context
            .remaining("observe FE drain")?
            .min(Duration::from_secs(1));
        let ready = context
            .handle()
            .frontend_management_get("/readyz", timeout)?;
        let live = context
            .handle()
            .frontend_management_get("/livez", timeout)?;
        let state = context
            .handle()
            .frontend_management_get("/v1/frontend/state", timeout)?;
        if ready.status == 503
            && live.status == 200
            && state.status == 200
            && state.body.contains("\"draining\"")
        {
            return Ok(());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn assert_idle_session_statement_is_rejected(connection: &mut mysql::Conn) -> Result<()> {
    let outcome = connection
        .query_drop("SELECT 1")
        .context("execute statement from pre-drain idle MySQL session");
    let error = format!(
        "{:#}",
        outcome.expect_err("post-drain SQL must be rejected")
    );
    ensure!(
        error.contains("FRONTEND_DRAINING") || error.contains("frontend is draining"),
        "post-drain SQL returned an unexpected error: {error}"
    );
    Ok(())
}

fn static_snapshot_launch_config(snapshot: &Path) -> ScenarioLaunchConfig {
    let object_store_metadata_credential = r#"
[[connector.credentials]]
purpose = "object-store-metadata"
name = "iceberg-test-data"
generation = "v1"
kind = "s3"
access_key_id = "${ENV:AWS_S3_ACCESS_KEY_ID}"
access_key_secret = "${ENV:AWS_S3_SECRET_ACCESS_KEY}"
"#;
    let object_store_data_credential =
        object_store_metadata_credential.replace("object-store-metadata", "object-store-data");
    ScenarioLaunchConfig {
        config_overlay: CrossProcessConfigOverlay {
            fe: Some(format!(
                "[catalog_source]\nmode = \"static-file\"\nstatic_file_path = \"{}\"\n{}",
                snapshot.display(),
                object_store_metadata_credential,
            )),
            be: Some(object_store_data_credential),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn write_rest_static_snapshot(snapshot: &Path, instance_id: &str) -> Result<()> {
    let rest_uri = std::env::var("NOVAROCKS_ICEBERG_REST_URI").context(
        "blue/green scenario requires NOVAROCKS_ICEBERG_REST_URI; source docker/iceberg-rest/runtime/current/env.sh",
    )?;
    let warehouse = std::env::var("NOVAROCKS_ICEBERG_REST_WAREHOUSE").context(
        "blue/green scenario requires NOVAROCKS_ICEBERG_REST_WAREHOUSE; source docker/iceberg-rest/runtime/current/env.sh",
    )?;
    let s3_endpoint = std::env::var("AWS_S3_ENDPOINT").context(
        "blue/green scenario requires AWS_S3_ENDPOINT; source docker/iceberg-rest/runtime/current/env.sh",
    )?;
    fs::write(
        snapshot,
        format!(
            "format_version = 3\n\
             [[catalogs]]\n\
             instance_id = \"{instance_id}\"\n\
             provider_id = \"iceberg\"\n\
             display_name = \"{instance_id}\"\n\
             config_format_version = 3\n\
             [catalogs.properties]\n\
             type = \"iceberg\"\n\
             \"iceberg.catalog.type\" = \"rest\"\n\
             uri = \"{rest_uri}\"\n\
             warehouse = \"{warehouse}\"\n\
             \"aws.s3.endpoint\" = \"{s3_endpoint}\"\n\
             \"aws.s3.enable_path_style_access\" = \"true\"\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-metadata\"\n\
             consumer_role = \"frontend\"\n\
             mode = \"static\"\n\
             name = \"iceberg-test-data\"\n\
             generation = \"v1\"\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-data\"\n\
             consumer_role = \"backend\"\n\
             mode = \"static\"\n\
             name = \"iceberg-test-data\"\n\
             generation = \"v1\"\n"
        ),
    )
    .with_context(|| {
        format!(
            "write blue/green StaticFile snapshot {}",
            snapshot.display()
        )
    })
}

fn run_blue_green_cutover(
    context: &mut ScenarioContext,
    green: &mut novarocks_cluster_harness::CrossProcessServerHandle,
) -> Result<()> {
    ensure!(
        green.be_count() == REQUIRED_BACKENDS,
        "green cutover requires native 1FE+3BE, received 1FE+{}BE",
        green.be_count()
    );
    let green_ready = green.frontend_management_get("/readyz", Duration::from_secs(5))?;
    ensure!(
        green_ready.status == 200,
        "green frontend did not become ready: {green_ready:?}"
    );
    context.action("launched an independent green 1FE+3BE cluster against the shared REST catalog");

    let mut blue_idle = mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining("connect blue pre-cutover session")?,
    )?;
    blue_idle
        .query_drop("SET CATALOG lnp8_blue")
        .context("select blue StaticFile catalog")?;
    let blue_user = context.mysql_user().to_string();
    let blue_port = context.mysql_port();
    let (blue_query_tx, blue_query_rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let result = (|| -> Result<Vec<i64>> {
            let mut connection =
                mysql_actor::connect(&blue_user, blue_port, Duration::from_secs(30))?;
            connection
                .query("SELECT sleep(2)")
                .context("run pre-cutover blue query")
        })();
        let _ = blue_query_tx.send(result);
    });
    wait_for_active_statement(context)?;
    context.action("admitted one blue query before removing the blue route");

    context
        .handle()
        .begin_fe_drain()
        .context("SIGTERM blue frontend after green readiness")?;
    wait_for_drain_observation(context)?;
    assert_idle_session_statement_is_rejected(&mut blue_idle)?;
    context.action(
        "blue accepted no new session statement after route cutover and returned SQLSTATE 1053",
    );

    let mut green_connection = mysql_actor::connect(
        green.mysql_user(),
        green.runtime().fe_mysql_port,
        context.remaining("connect green post-cutover session")?,
    )?;
    green_connection
        .query_drop("SET CATALOG lnp8_green")
        .context("select green StaticFile catalog")?;
    let green_rows: Vec<(i64,)> = green_connection
        .query("SELECT 1")
        .context("run a new query through green")?;
    ensure!(green_rows == vec![(1,)]);
    context.action("green accepted a new query after blue route withdrawal");

    let blue_rows = blue_query_rx
        .recv_timeout(context.remaining("wait for pre-cutover blue query")?)
        .map_err(|error| anyhow::anyhow!("blue pre-cutover query did not finish: {error}"))??;
    ensure!(
        blue_rows.len() == 1,
        "blue query returned unexpected rows: {blue_rows:?}"
    );
    let deadline = context.deadline();
    context
        .handle()
        .wait_fe_exit_until(deadline)
        .context("wait for drained blue frontend exit")?;
    context.action(
        "blue pre-cutover attempt terminated on blue; no query attempt was migrated to green",
    );
    Ok(())
}
