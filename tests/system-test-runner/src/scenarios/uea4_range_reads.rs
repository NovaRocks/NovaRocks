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

//! Native multi-file and short-range reads through scenario-owned Iceberg and S3.

use super::connector::{await_resource_convergence, require_three_backends, resource_baseline};
use super::provider_mysql_actors::{
    ActorStatements, ProviderActor, ProviderCheck, finish_provider_operation,
    provider_failure_receipt, record_cancelled_proxy_read, run_checks, unsigned_pairs,
    unsigned_rows,
};
use crate::actors::sql::{SqlSession, StatementFailure};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::delayed_s3::{DelayedS3Config, DelayedS3Proxy};
use novarocks_cluster_harness::fixture_run::{RunBudget, canonical_s3};
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture;
use novarocks_cluster_harness::{CrossProcessChildEnvironment, CrossProcessConfigOverlay};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

// Actual range/cancellation probes require object reads rather than cached pages.
const FUNCTIONAL_CACHE_OVERLAY: &str = "[runtime.cache]\npage_cache_enable = false\nparquet_page_cache_enable = false\ndatacache_enable = false\n";
const ACCESS_KEY_ENV: &str = "NOVAROCKS_UEA4A2_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "NOVAROCKS_UEA4A2_S3_SECRET_ACCESS_KEY";
const CREDENTIAL_NAME: &str = "range-fixture";
const CREDENTIAL_GENERATION: &str = "v1";
const CATALOG: &str = "range_fixture";
const NAMESPACE: &str = "range_source";
const TABLE: &str = "sample_data";
const MULTI_ROWS: u64 = 768;
const MULTI_FILES: usize = 24;
const SHORT_ROWS: u64 = 4096;

struct LiveFixture {
    rest: IsolatedIcebergRestFixture,
    proxy: Option<DelayedS3Proxy>,
}

#[derive(Deserialize, Serialize)]
struct SeedFile {
    path: String,
    rows: u64,
    size: u64,
}

fn credential_overlay(purpose: &str) -> String {
    format!(
        "[[connector.credentials]]\npurpose = \"{purpose}\"\nname = \"{CREDENTIAL_NAME}\"\ngeneration = \"{CREDENTIAL_GENERATION}\"\nkind = \"s3\"\naccess_key_id = \"${{ENV:{ACCESS_KEY_ENV}}}\"\naccess_key_secret = \"${{ENV:{SECRET_KEY_ENV}}}\"\n{FUNCTIONAL_CACHE_OVERLAY}"
    )
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn catalog_sql(rest: &IsolatedIcebergRestFixture, endpoint: &str) -> String {
    let rest = rest.endpoints();
    format!(
        "CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{}\",\"iceberg.catalog.warehouse\"=\"{}\",\"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{CREDENTIAL_NAME}\",\"credential.object-store-metadata.generation\"=\"{CREDENTIAL_GENERATION}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"static\",\"credential.object-store-data.name\"=\"{CREDENTIAL_NAME}\",\"credential.object-store-data.generation\"=\"{CREDENTIAL_GENERATION}\")",
        sql_string(&rest.rest_uri),
        sql_string(&rest.rest_warehouse),
        sql_string(endpoint),
    )
}

fn source_sql(rows: u64, files: usize, value: &str) -> String {
    let table = format!("ice_rest.{NAMESPACE}.{TABLE}");
    format!(
        "CREATE NAMESPACE ice_rest.{NAMESPACE};
CREATE TABLE {table} (id BIGINT, value BIGINT) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet', 'write.distribution-mode'='none');
INSERT INTO {table} SELECT id, {value} FROM range(0, {rows}, 1, {files});
SELECT concat('RANGE_FILE|', to_json(named_struct('path', file_path, 'rows', record_count, 'size', file_size_in_bytes)))
FROM {table}.files WHERE content = 0 ORDER BY file_path;
SELECT concat('RANGE_SOURCE_OK|', CAST(count(DISTINCT id) AS STRING)) FROM {table}
WHERE id BETWEEN 0 AND {rows} - 1 AND value = {value};
"
    )
}

#[cfg(unix)]
fn seed_source(
    rest: &IsolatedIcebergRestFixture,
    root: &Path,
    budget: &RunBudget,
    short: bool,
) -> Result<Vec<SeedFile>> {
    use novarocks_test_support::{ManagedCommand, capture_command_until};
    use std::os::unix::fs::OpenOptionsExt;

    let (rows, file_count, value) = if short {
        (SHORT_ROWS, 1, "id * 3")
    } else {
        (MULTI_ROWS, MULTI_FILES, "id DIV 32")
    };
    let source = root.join("range-source.sql");
    fs::write(&source, source_sql(rows, file_count, value))?;
    let spark_defaults = root.join("range-source.spark.conf");
    // The Spark task partition count fixes the fixture files. One worker
    // processes those small tasks without competing with the native cluster.
    fs::write(&spark_defaults, "spark.master local[1]\n")?;
    let program =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docker/iceberg-rest/spark-sql.sh");
    let env_file = rest.runtime_env_file()?;
    let mut command = ManagedCommand::new(&program);
    command
        .arg(&source)
        .env("NOVA_ENV_REST_ENV_FILE", &env_file)
        .env("NOVAROCKS_WORKSPACE_ROOT", rest.workspace_root())
        .env("NOVAROCKS_SPARK_EXTRA_DEFAULTS", &spark_defaults)
        .terminate_at(budget.work_deadline(), budget.deadline());
    let open_log = |name: &str| {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root.join(name))
    };
    fs::write(
        root.join("range-source-command.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "program":program, "sql":source, "private_env_file":env_file,
            "spark_defaults":spark_defaults,
        }))?,
    )?;
    let outcome = capture_command_until(
        command,
        open_log("range-source.stdout")?,
        open_log("range-source.stderr")?,
        budget.work_deadline(),
    )?;
    fs::write(
        root.join("range-source-exit.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "exit_code":outcome.output.status.code(), "timed_out":outcome.timed_out,
            "local_group_exited":outcome.group_exit_observed(), "failures":outcome.failures,
        }))?,
    )?;
    ensure!(
        outcome.output.status.success()
            && !outcome.timed_out
            && outcome.failures.is_empty()
            && outcome.group_exit_observed(),
        "private Spark fixture did not finish under its original deadline; diagnostics={}",
        outcome.diagnostic_root().display(),
    );
    let stdout = std::str::from_utf8(&outcome.output.stdout)?;
    let files = parse_source(stdout, rows, file_count, &rest.endpoints().rest_warehouse)?;
    fs::write(
        root.join("range-source-files.json"),
        serde_json::to_vec_pretty(&files)?,
    )?;
    outcome.release_diagnostics()?;
    Ok(files)
}

#[cfg(not(unix))]
fn seed_source(
    _: &IsolatedIcebergRestFixture,
    _: &Path,
    _: &RunBudget,
    _: bool,
) -> Result<Vec<SeedFile>> {
    anyhow::bail!("private Spark fixture process ownership requires Unix")
}

fn parse_source(
    stdout: &str,
    rows: u64,
    file_count: usize,
    warehouse: &str,
) -> Result<Vec<SeedFile>> {
    let counts = stdout
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("RANGE_SOURCE_OK|"))
        .map(str::parse::<u64>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        counts == [rows],
        "Spark source row oracle failed: {counts:?}"
    );
    let files = stdout
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("RANGE_FILE|"))
        .map(serde_json::from_str::<SeedFile>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let scope = format!(
        "{}/{NAMESPACE}/{TABLE}/data/",
        warehouse.trim_end_matches('/')
    );
    let mut paths = BTreeSet::new();
    ensure!(
        files.len() == file_count
            && files.iter().map(|file| file.rows).sum::<u64>() == rows
            && files.iter().all(|file| file.rows > 0
                && file.size > 0
                && file.path.starts_with(&scope)
                && file.path.ends_with(".parquet")
                && canonical_s3(&file.path).is_ok_and(|path| path == file.path)
                && paths.insert(&file.path)),
        "Spark source did not publish the required physical Iceberg files",
    );
    Ok(files)
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::<RangeFunctional>::default(),
        Box::new(RangeFunctional {
            short: true,
            fixture: Mutex::new(None),
        }),
    ]
}

/// Native range reads and cancellation against scenario-owned Iceberg data.
#[derive(Default)]
struct RangeFunctional {
    fixture: Mutex<Option<LiveFixture>>,
    short: bool,
}

impl Scenario for RangeFunctional {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "uea4_range_reads",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        if self.short {
            "connector/iceberg-short-range-concurrent"
        } else {
            "uea4/iceberg-range-functional"
        }
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config_with_fixture_run(
        &self,
        scenario_root: &Path,
        budget: &RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("range fixture lock poisoned"))?;
        ensure!(slot.is_none(), "range fixture initialized twice");
        let (rest, _) =
            IsolatedIcebergRestFixture::start_private_with_budget(scenario_root, budget, false)
                .context("start private range fixture")?;
        // The runner can settle the private owner even if Spark or proxy setup fails.
        let fixture = slot.insert(LiveFixture { rest, proxy: None });
        let files = seed_source(&fixture.rest, scenario_root, budget, self.short)?;
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: fixture.rest.endpoints().minio_endpoint.clone(),
            delay: Duration::ZERO,
        })?;
        for (index, file) in files.iter().enumerate() {
            let path = file
                .path
                .strip_prefix("s3://")
                .context("source file is not S3")?;
            proxy.label_object(&format!("/{path}"), &format!("range_data_{index}"))?;
        }
        fixture.proxy = Some(proxy);
        let identity = fixture.rest.static_s3_identity();
        let mut child = CrossProcessChildEnvironment::default();
        for role in [&mut child.fe, &mut child.be] {
            role.insert(ACCESS_KEY_ENV.to_owned(), identity.access_key_id.clone());
            role.insert(
                SECRET_KEY_ENV.to_owned(),
                identity.secret_access_key.clone(),
            );
        }
        Ok(ScenarioLaunchConfig {
            child_environment: child,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(credential_overlay("object-store-metadata")),
                be: Some(credential_overlay("object-store-data")),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("functional fixture lock poisoned"))?;
        let fixture = fixture
            .as_ref()
            .context("functional fixture was not prepared")?;
        let proxy = fixture
            .proxy
            .as_ref()
            .context("functional range proxy was not prepared")?;
        context.action("read from the private Spark-authored Iceberg snapshot");
        let table = format!("{CATALOG}.{NAMESPACE}.{TABLE}");
        let mut connection =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        connection
            .execute(catalog_sql(&fixture.rest, proxy.endpoint()))
            .map(|_| ())
            .context("create functional Iceberg catalog")?;
        if self.short {
            drop(connection);
            run_range_clients(context, &table, true)?;
            context.action("four short-range clients independently verified (id, 3 * id) for id=1..10 and exited");
            return Ok(());
        }
        let scan = format!(
            "SELECT id FROM {table} WHERE id BETWEEN 0 AND {} ORDER BY id",
            MULTI_ROWS - 1
        );
        let initial_baseline = resource_baseline(context)?;
        let before = proxy.snapshot();
        verify_functional_rows(&mut connection, &scan)?;
        let after = proxy.snapshot();
        ensure!(
            after.gets > before.gets && after.upstream_errors == before.upstream_errors,
            "multi-file Iceberg read did not complete real S3 GETs without upstream errors"
        );
        context.action("verified every ordered ID across the private multi-file Iceberg table");
        let first_five: Vec<u64> = connection
            .execute(format!("{scan} LIMIT 5"))
            .and_then(unsigned_rows)
            .context("read limited multi-file Iceberg result")?;
        ensure!(
            first_five == [0, 1, 2, 3, 4],
            "limited multi-file Iceberg read returned {first_five:?}"
        );
        drop(connection);
        await_resource_convergence(context, &initial_baseline, "multi-file and LIMIT reads")?;
        run_range_clients(context, &table, false)?;
        context.action(
            "four multi-file clients each verified the independent ordered ID oracle and exited",
        );

        let baseline = resource_baseline(context)?;
        let hold = proxy.hold_next_read_with_suffix(".parquet")?;
        let held_scan = scan.clone();
        let mut actor =
            ProviderActor::spawn(context, "a4-held-read", move |connection, statements| {
                Ok(statements
                    .run(connection, held_scan)?
                    .and_then(unsigned_rows))
            })?;
        let mut kill_actor = None;
        let mut entered = false;
        let operation = (|| -> Result<()> {
            actor.wait_started(context.remaining("receive held read connection ID")?)?;
            hold.wait_until_entered(
                context
                    .remaining("observe held Parquet read")?
                    .min(Duration::from_secs(30)),
            )?;
            entered = true;
            let original_query = actor.current_query()?;
            kill_actor = Some(ProviderActor::spawn(
                context,
                "a4-held-kill",
                move |_control, _statements| {
                    original_query
                        .cancel()
                        .context("cancel held multi-file Iceberg read")
                },
            )?);
            kill_actor
                .as_mut()
                .expect("control actor installed")
                .receive(
                    context
                        .remaining("await held read KILL QUERY")?
                        .min(Duration::from_secs(10)),
                )?;
            hold.release();
            hold.wait_until_cancelled_read_exited(
                context
                    .remaining("drain held Parquet read")?
                    .min(Duration::from_secs(30)),
            )?;
            let result = actor.receive(context.remaining("await cancelled Iceberg read")?)?;
            let error = result
                .err()
                .context("held Iceberg read unexpectedly succeeded after cancellation")?;
            if !matches!(&error, StatementFailure::Trino(error) if error.error_code == 3 && error.error_name == "USER_CANCELED" && error.error_type == "USER_ERROR")
            {
                return Err(anyhow::Error::new(error)
                    .context("held Iceberg read did not report query cancellation"));
            }
            Ok(())
        })();
        let mut cleanup = Vec::new();
        if operation.is_err() {
            if let Err(error) = actor.cancel(context) {
                cleanup.push(error.context("cancel read"));
            }
            if let Some(actor) = &mut kill_actor
                && let Err(error) = actor.cancel(context)
            {
                cleanup.push(error.context("cancel control"));
            }
        }
        hold.release();
        if entered
            && let Err(error) = hold.wait_until_cancelled_read_exited(Duration::from_secs(30))
        {
            cleanup.push(error.context("held read terminal I/O"));
        }
        if let Err(error) = actor.finish_with(context, |result| {
            result.map(|_| ()).map_err(anyhow::Error::from)
        }) {
            cleanup.push(error.context("read actual join and original result"));
        }
        if let Some(actor) = &mut kill_actor
            && let Err(error) = actor.finish_with(context, |()| Ok(()))
        {
            cleanup.push(error.context("control actual join"));
        }
        if let Err(error) =
            await_resource_convergence(context, &baseline, "held multi-file Iceberg cancellation")
        {
            cleanup.push(error.context("resource exit"));
        }
        if let Err(error) = record_cancelled_proxy_read(context, "a4-held-read", proxy, &hold) {
            cleanup.push(error.context("proxy terminal evidence"));
        }
        let evidence = (|| -> Result<()> {
            fs::write(
                context.scenario_root().join("a4-held-read-exit.json"),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "operation_error":operation.as_ref().err().map(provider_failure_receipt),
                    "cleanup_errors":cleanup.iter().map(provider_failure_receipt).collect::<Vec<_>>(),
                    "actual_actor_joins_completed":true,
                    "causal_priority":"original operation first; post-cancel/Join results remain actual secondary objects",
                }))?,
            )?;
            Ok(())
        })();
        if let Err(error) = evidence {
            cleanup.push(error.context("persist original range exit evidence"));
        }
        finish_provider_operation(operation, cleanup)?;
        context.action("cancelled a real held Parquet read and observed native resource drain");

        let mut followup =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        verify_functional_rows(&mut followup, &scan)?;
        context.action("verified the full Iceberg oracle after cancellation");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("range fixture lock poisoned"))?
            .take();
        if let Some(LiveFixture { mut rest, proxy }) = fixture {
            drop(proxy);
            rest.shutdown().context("shutdown private range fixture")?;
        }
        Ok(())
    }
}

fn verify_functional_rows(connection: &mut SqlSession, sql: &str) -> Result<()> {
    let rows: Vec<u64> = connection
        .execute(sql)
        .and_then(unsigned_rows)
        .context("read multi-file Iceberg rows")?;
    verify_functional_result(rows)
}
fn verify_functional_result(rows: Vec<u64>) -> Result<()> {
    ensure!(
        rows.iter().copied().eq(0..MULTI_ROWS),
        "multi-file Iceberg rows differ from the independent ordered ID oracle"
    );
    Ok(())
}

/// Each finite actor checks an independent input oracle; client agreement alone
/// does not establish correctness. Joining all actors precedes error reporting.
fn run_range_clients(context: &mut ScenarioContext, table: &str, short: bool) -> Result<()> {
    let baseline = resource_baseline(context)?;
    let sql = if short {
        format!("SELECT id, value FROM {table} WHERE id BETWEEN 1 AND 10 ORDER BY id")
    } else {
        format!(
            "SELECT id FROM {table} WHERE id BETWEEN 0 AND {} ORDER BY id",
            MULTI_ROWS - 1
        )
    };
    let checks = (0..4)
        .map(|index| {
            let sql = sql.clone();
            Box::new(
                move |connection: &mut SqlSession, statements: &ActorStatements| -> Result<()> {
                    if short {
                        let actual = unsigned_pairs(statements.run(connection, &sql)??)?;
                        let expected = (1..=10).map(|id| (id, 3 * id)).collect::<Vec<_>>();
                        ensure!(
                            actual == expected,
                            "short-range client {index} rows differ: {actual:?}"
                        );
                    } else {
                        verify_functional_result(unsigned_rows(
                            statements.run(connection, &sql)??,
                        )?)
                        .with_context(|| format!("multi-file range client {index}"))?;
                    }
                    Ok(())
                },
            ) as ProviderCheck
        })
        .collect();
    let execution = run_checks(
        context,
        if short {
            "short-range"
        } else {
            "multi-file-range"
        },
        checks,
    );
    let exit = await_resource_convergence(context, &baseline, "concurrent range clients");
    let evidence = fs::write(
        context.scenario_root().join("range-concurrent-exit.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "short_fixture":short,"actors":4,"execution_error":execution.as_ref().err().map(ToString::to_string),
            "resource_exit_error":exit.as_ref().err().map(ToString::to_string),
        }))?,
    );
    execution?;
    exit?;
    evidence?;
    Ok(())
}
