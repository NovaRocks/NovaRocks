use crate::actors::sql::{self, SqlSession, StatementFailure, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::{CrossProcessConfigOverlay, TestClusterOwner};
use std::fs;
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant};

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
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "frontend_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

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
        let mut connection = {
            context.remaining("connect healthy partial catalog")?;
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?
        };
        connection
            .execute("SET CATALOG lnp8_healthy")
            .context("select healthy catalog after partial bootstrap")?;
        let rows: Vec<(i64,)> = sql::integer_column(
            &connection
                .execute("SELECT 1")
                .context("query through healthy partial catalog session")?,
        )?
        .into_iter()
        .map(|value| (value,))
        .collect();
        ensure!(rows == vec![(1,)]);
        let mut unavailable = {
            context.remaining("connect unavailable partial catalog")?;
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?
        };
        let failure = unavailable
            .execute("SET CATALOG lnp8_unavailable")
            .expect_err("invalid catalog definition must remain unavailable");
        let StatementFailure::Trino(server) = &failure else {
            return Err(failure.into());
        };
        let error = &server.message;
        ensure!(
            error.contains("iceberg.catalog.warehouse"),
            "unavailable catalog returned an unexpected error: {error}"
        );
        context.action("proved one unavailable StaticFile catalog leaves the healthy catalog and FE readiness usable");
        Ok(())
    }
}

impl Scenario for StaticFileDisposableCarrier {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "frontend_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

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
        let mut connection = {
            context.remaining("connect StaticFile client")?;
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?
        };
        connection
            .execute("SET CATALOG lnp8_static")
            .context("select StaticFile catalog")?;
        let namespace = format!("lnp8ns_{}", context.trino_port());
        connection
            .execute(format!("CREATE DATABASE {namespace}"))
            .context("create StaticFile catalog namespace")?;
        connection
            .execute(format!(
                "CREATE TABLE {namespace}.orders (id INT, amount INT)"
            ))
            .context("create StaticFile catalog table")?;
        connection
            .execute(format!(
                "INSERT INTO {namespace}.orders VALUES (1, 10), (2, 20)"
            ))
            .context("write StaticFile catalog rows")?;
        let rows = static_file_rows(
            &connection
                .execute(format!(
                    "SELECT id, amount FROM {namespace}.orders ORDER BY id"
                ))
                .context("query StaticFile catalog before FE disposal")?,
        )?;
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
        let mut connection = {
            context.remaining("reconnect after StaticFile disposal")?;
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?
        };
        connection
            .execute("SET CATALOG lnp8_static")
            .context("reselect StaticFile catalog")?;
        let rebuilt = static_file_rows(
            &connection
                .execute(format!(
                    "SELECT id, amount FROM {namespace}.orders ORDER BY id"
                ))
                .context("query StaticFile catalog after FE disposal")?,
        )?;
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
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "frontend_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

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
        let cleanup = TestClusterOwner::shutdown(&mut green).context("stop green cluster");
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
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "frontend_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

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
        let session =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let query = FrontendStatementActor::start(
            session,
            "SELECT sleep(10)",
            context.deadline(),
            "forced-drain",
        )?;
        let phase = (|| -> Result<()> {
            wait_for_active_statement(context)?;
            context.action("observed an admitted statement before the short drain deadline");
            context
                .handle()
                .begin_fe_drain()
                .context("begin forced FE drain through SIGTERM")?;
            let outcome = query
                .result
                .recv_timeout(context.remaining("wait for forced-drain cancellation")?)
                .map_err(|error| anyhow::anyhow!("forced-drain query did not finish: {error}"))?;
            let failure = outcome.expect_err("forced drain must cancel the statement");
            require_drain_failure(failure, "FRONTEND_DRAIN_DEADLINE_EXCEEDED", "forced drain")?;
            context.action("observed the typed frontend drain deadline cancellation");
            let deadline = context.deadline();
            context
                .handle()
                .wait_fe_exit_until(deadline)
                .context("wait for FE exit after forced drain")?;
            context.action("FE exited successfully within the configured forced-drain budget");
            Ok(())
        })();
        query.finish_after(context, phase)
    }
}

impl Scenario for GracefulDrain {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "frontend_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "frontend-lifecycle/graceful-drain"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "graceful drain requires native 1FE+3BE"
        );
        context.action("verified native 1FE+3BE topology");
        // This pre-drain client retains only its explicit HTTP session value.
        let mut idle_connection =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())
                .context("open pre-drain SQL session value")?;
        let session =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let query = FrontendStatementActor::start(
            session,
            "SELECT sleep(10)",
            context.deadline(),
            "graceful-drain",
        )?;
        let phase = (|| -> Result<()> {
            wait_for_active_statement(context)?;
            context.action("observed an admitted public SQL statement lease");
            context
                .handle()
                .begin_fe_drain()
                .context("begin graceful FE drain through SIGTERM")?;
            wait_for_drain_observation(context)?;
            context.action(
                "observed ready=503, live=200, and Draining from the real FE management listener",
            );
            assert_new_statement_is_rejected_during_drain(&mut idle_connection)?;
            context.action(
                "proved a new statement on the draining FE receives the typed draining rejection",
            );
            let result = query
                .result
                .recv_timeout(context.remaining("wait for pre-drain statement")?)
                .map_err(|error| {
                    anyhow::anyhow!("pre-drain statement did not finish: {error}")
                })??;
            require_completed_sleep(&result, "graceful-drain query")?;
            context.action("pre-drain statement completed normally after drain began");
            let deadline = context.deadline();
            context
                .handle()
                .wait_fe_exit_until(deadline)
                .context("wait for FE exit after graceful drain")?;
            context.action("FE exited successfully only after its admitted work completed");
            Ok(())
        })();
        query.finish_after(context, phase)
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

fn assert_new_statement_is_rejected_during_drain(connection: &mut SqlSession) -> Result<()> {
    let failure = connection
        .execute("SELECT 1")
        .context("execute statement from pre-drain SQL session")
        .expect_err("post-drain SQL must be rejected");
    let actual = failure
        .chain()
        .find_map(|cause| cause.downcast_ref::<StatementFailure>())
        .context("drain refusal lost its original SQL failure")?;
    let StatementFailure::Trino(server) = actual else {
        return Err(failure);
    };
    if server.error_code != 65545
        || server.error_name != "SERVER_SHUTTING_DOWN"
        || server.error_type != "INTERNAL_ERROR"
        || !(server.message.contains("FRONTEND_DRAINING")
            || server.message.contains("frontend is draining"))
    {
        return Err(failure.context("post-drain SQL returned an unexpected error"));
    }
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
    green: &mut novarocks_cluster_harness::CrossProcessCluster,
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
    context.remaining("connect blue pre-cutover session")?;
    let mut blue_idle =
        SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
    blue_idle
        .execute("SET CATALOG lnp8_blue")
        .context("select blue StaticFile catalog")?;
    let session = SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
    let blue_query = FrontendStatementActor::start(
        session,
        "SELECT sleep(2)",
        context.deadline(),
        "blue-cutover",
    )?;
    let phase = (|| -> Result<()> {
        wait_for_active_statement(context)?;
        context.action("admitted one blue query before removing the blue route");
        context
            .handle()
            .begin_fe_drain()
            .context("SIGTERM blue frontend after green readiness")?;
        wait_for_drain_observation(context)?;
        assert_new_statement_is_rejected_during_drain(&mut blue_idle)?;
        context.action("blue accepted no new statement after route cutover and returned typed SERVER_SHUTTING_DOWN");
        context.remaining("connect green post-cutover session")?;
        let mut green_connection = SqlSession::new(
            green.sql_user(),
            green.runtime().fe_trino_port,
            context.deadline(),
        )?;
        green_connection
            .execute("SET CATALOG lnp8_green")
            .context("select green StaticFile catalog")?;
        let green_rows: Vec<(i64,)> = sql::integer_column(
            &green_connection
                .execute("SELECT 1")
                .context("run a new query through green")?,
        )?
        .into_iter()
        .map(|value| (value,))
        .collect();
        ensure!(green_rows == vec![(1,)]);
        context.action("green accepted a new query after blue route withdrawal");
        let result = blue_query
            .result
            .recv_timeout(context.remaining("wait for pre-cutover blue query")?)
            .map_err(|error| anyhow::anyhow!("blue pre-cutover query did not finish: {error}"))??;
        require_completed_sleep(&result, "blue pre-cutover query")?;
        let deadline = context.deadline();
        context
            .handle()
            .wait_fe_exit_until(deadline)
            .context("wait for drained blue frontend exit")?;
        context.action(
            "blue pre-cutover attempt terminated on blue; no query attempt was migrated to green",
        );
        Ok(())
    })();
    blue_query.finish_after(context, phase)
}

fn static_file_rows(result: &StatementResult) -> Result<Vec<(i32, i32)>> {
    ensure!(
        result.is_result_set() && result.columns.len() == 2,
        "StaticFile requires two declared INT columns"
    );
    ensure!(
        result
            .columns
            .iter()
            .all(|column| column.type_signature.raw_type == "integer"
                && column.type_signature.arguments.is_empty()),
        "StaticFile result lost its declared INT types"
    );
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Integer(id), TrinoValue::Integer(amount)] => {
                Ok((i32::try_from(*id)?, i32::try_from(*amount)?))
            }
            row => bail!("StaticFile requires two non-null INT cells: {row:?}"),
        })
        .collect()
}
fn require_completed_sleep(result: &StatementResult, subject: &str) -> Result<()> {
    ensure!(
        result.is_result_set()
            && result.columns.len() == 1
            && result.columns[0].type_name == "boolean"
            && result.columns[0].type_signature.raw_type == "boolean"
            && result.columns[0].type_signature.arguments.is_empty()
            && result.rows == vec![vec![TrinoValue::Boolean(true)]],
        "{subject} did not return one completed BOOLEAN SLEEP row: {result:?}"
    );
    Ok(())
}

fn require_drain_failure(failure: StatementFailure, detail: &str, subject: &str) -> Result<()> {
    let accepted = matches!(&failure, StatementFailure::Trino(server)
        if server.error_code == 131075 && server.error_name == "EXCEEDED_TIME_LIMIT"
        && server.error_type == "INSUFFICIENT_RESOURCES" && server.message.contains(detail));
    if !accepted {
        return Err(anyhow::Error::new(failure)
            .context(format!("{subject} did not preserve its typed drain reason")));
    }
    Ok(())
}
fn frontend_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(FrontendActorCleanup { primary, cleanup }.into()),
    }
}
/// Owns one actual submitted statement and its original physical reader.
struct FrontendStatementActor {
    result: mpsc::Receiver<std::result::Result<StatementResult, StatementFailure>>,
    worker: Option<thread::JoinHandle<Result<()>>>,
    cancel: novarocks_test_trino_client::CancelHandle,
    deadline: Instant,
    terminal: Arc<AtomicBool>,
}
impl FrontendStatementActor {
    fn start(mut session: SqlSession, sql: &str, deadline: Instant, name: &str) -> Result<Self> {
        let pending = session.submit(sql)?;
        let cancel = pending.cancel_handle();
        let terminal = Arc::new(AtomicBool::new(false));
        let saved = terminal.clone();
        let (sender, result) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name(format!("frontend-{name}-sql"))
            .spawn(move || {
                let response = session.run_pending(pending);
                saved.store(
                    matches!(&response, Ok(_) | Err(StatementFailure::Trino(_))),
                    Ordering::Release,
                );
                match sender.send(response) {
                    Ok(()) => Ok(()),
                    Err(mpsc::SendError(Err(primary))) => Err(anyhow::Error::new(primary)
                        .context("original frontend SQL result receiver closed")),
                    Err(mpsc::SendError(Ok(_))) => {
                        bail!("original frontend SQL result receiver closed")
                    }
                }
            });
        match worker {
            Ok(worker) => Ok(Self {
                result,
                worker: Some(worker),
                cancel,
                deadline,
                terminal,
            }),
            Err(primary) => frontend_cleanup(
                Err(primary.into()),
                cancel.delete(deadline).map_err(Into::into),
            ),
        }
    }
    fn finish_after<T>(mut self, context: &mut ScenarioContext, primary: Result<T>) -> Result<T> {
        let attempted = primary.is_err() && !self.terminal.load(Ordering::Acquire);
        let mut cleanup = if attempted {
            self.cancel
                .delete(self.deadline)
                .map_err(anyhow::Error::from)
        } else {
            Ok(())
        };
        if cleanup.is_err() {
            cleanup = frontend_cleanup(cleanup, context.shutdown());
        }
        if let Some(worker) = self.worker.take() {
            cleanup = frontend_cleanup(
                cleanup,
                worker
                    .join()
                    .map_err(|payload| anyhow::Error::new(FrontendActorPanic(Mutex::new(payload))))
                    .and_then(|result| result),
            );
        }
        if !attempted && !self.terminal.load(Ordering::Acquire) {
            let cancelled = self
                .cancel
                .delete(self.deadline)
                .map_err(anyhow::Error::from);
            let shutdown = if cancelled.is_err() {
                context.shutdown()
            } else {
                Ok(())
            };
            cleanup = frontend_cleanup(cleanup, frontend_cleanup(cancelled, shutdown));
        }
        if let Ok(Err(error)) = self.result.try_recv() {
            cleanup = frontend_cleanup(cleanup, Err(error.into()));
        }
        frontend_cleanup(primary, cleanup)
    }
}
impl Drop for FrontendStatementActor {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            let cancelled = if !self.terminal.load(Ordering::Acquire) {
                self.cancel
                    .delete(self.deadline)
                    .map_err(anyhow::Error::from)
            } else {
                Ok(())
            };
            let joined = worker
                .join()
                .map_err(|payload| anyhow::Error::new(FrontendActorPanic(Mutex::new(payload))))
                .and_then(|result| result);
            let pending = match self.result.try_recv() {
                Ok(Err(primary)) => Err(anyhow::Error::new(primary)
                    .context("original SQL result failed during actor Drop")),
                _ => Ok(()),
            };
            if let Err(error) = frontend_cleanup(frontend_cleanup(cancelled, joined), pending) {
                eprintln!("Original frontend SQL actor cleanup failed: {error:#}");
            }
        }
    }
}

struct FrontendActorCleanup {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl std::fmt::Debug for FrontendActorCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for FrontendActorCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original SQL operation failed: {:#}; actual actor cleanup failed: {:#}",
            self.primary, self.cleanup
        )
    }
}
impl std::error::Error for FrontendActorCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
struct FrontendActorPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Debug for FrontendActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for FrontendActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.0;
        f.write_str("original SQL actor panicked; payload retained")
    }
}
impl std::error::Error for FrontendActorPanic {}
