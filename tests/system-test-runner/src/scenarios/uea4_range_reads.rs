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

//! Native UEA-4A-2 range-read scenarios. The G0 pilot records calibration
//! only; the functional stage checks multi-file rows and held-read cleanup.

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
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, LaunchProfile,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

// Actual range/cancellation probes require object reads rather than cached pages.
const FUNCTIONAL_CACHE_OVERLAY: &str = "[runtime.cache]\npage_cache_enable = false\nparquet_page_cache_enable = false\ndatacache_enable = false\n";
const FIXTURE_ENV: &str = "NOVAROCKS_UEA4A2_FIXTURE_MANIFEST";
const SHORT_FIXTURE_ENV: &str = "NOVAROCKS_ICEBERG_SHORT_FIXTURE_MANIFEST";
const ACCESS_KEY_ENV: &str = "NOVAROCKS_UEA4A2_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "NOVAROCKS_UEA4A2_S3_SECRET_ACCESS_KEY";

#[derive(Deserialize)]
struct PublishedIcebergFixture {
    schema_version: u32,
    fixture_kind: String,
    table_location: String,
    warehouse_uri: String,
    rest_uri: String,
    s3_endpoint: String,
    region: String,
    credential_name: String,
    credential_generation: String,
    database: String,
    table: String,
    data_file_count: u64,
    row_count: u64,
    objects_sha256: String,
    artifacts: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct FixtureOracle {
    row_count: u64,
    min_id: u64,
    max_id: u64,
    id_sum: u64,
    value_sum: u64,
}

#[derive(Deserialize)]
struct FixtureObject {
    key: String,
    etag: String,
    sha256: Option<String>,
    size: u64,
}

struct CheckedFixture {
    facts: PublishedIcebergFixture,
    objects: Vec<FixtureObject>,
    manifest_sha256: String,
}

impl CheckedFixture {
    fn load_source(path: &Path, allow_a4: bool) -> Result<Self> {
        let bytes =
            fs::read(path).with_context(|| format!("read UEA-4A-2 fixture {}", path.display()))?;
        let digest = sha256(&bytes);
        let root = path
            .parent()
            .context("fixture manifest has no parent directory")?;
        let ready = fs::read_to_string(root.join("READY")).context("read exact fixture READY")?;
        ensure!(
            ready.trim() == format!("sha256:{digest}"),
            "fixture READY does not bind the exact manifest"
        );
        let facts: PublishedIcebergFixture =
            serde_json::from_slice(&bytes).context("decode published Iceberg fixture")?;
        ensure!(
            facts.schema_version == 1,
            "published Iceberg fixture must be versioned"
        );
        let a4_fixture = allow_a4 && facts.fixture_kind == "uea4a4-iceberg-performance-v1";
        let short_fixture = facts.fixture_kind == "uea4a2-iceberg-short-v1";
        ensure!(
            a4_fixture || short_fixture,
            "expected a published short or explicitly selected A4 Iceberg fixture"
        );
        ensure!(
            if a4_fixture {
                facts.data_file_count >= 16 && facts.row_count == 768
            } else {
                facts.data_file_count >= 1 && facts.row_count == 4096
            },
            "published Iceberg fixture has the wrong source data shape"
        );
        ensure!(
            facts.rest_uri.starts_with("http://127.0.0.1:")
                || facts.rest_uri.starts_with("http://localhost:"),
            "fixture REST endpoint must be loopback"
        );
        ensure!(
            facts.s3_endpoint.starts_with("http://127.0.0.1:")
                || facts.s3_endpoint.starts_with("http://localhost:"),
            "fixture S3 endpoint must be loopback"
        );
        ensure!(
            facts.table_location.starts_with("s3://") && !facts.table_location.contains('?'),
            "fixture table location must be an exact S3 path"
        );
        for id in [
            &facts.database,
            &facts.table,
            &facts.credential_name,
            &facts.credential_generation,
        ] {
            ensure!(
                !id.is_empty()
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
                "invalid fixture identifier"
            );
        }
        for (name, hash) in &facts.artifacts {
            ensure!(
                !name.contains('/') && !name.contains("..") && is_sha256(hash),
                "invalid fixture artifact name or hash"
            );
            ensure!(
                sha256_file(&root.join(name))? == *hash,
                "fixture artifact hash mismatch: {name}"
            );
        }
        ensure!(
            facts.artifacts.get("objects.json") == Some(&facts.objects_sha256),
            "fixture object inventory hash is not published"
        );
        let objects_bytes =
            fs::read(root.join("objects.json")).context("read G0 pilot object inventory")?;
        ensure!(
            sha256(&objects_bytes) == facts.objects_sha256,
            "fixture object inventory changed after artifact verification"
        );
        let objects: Vec<FixtureObject> =
            serde_json::from_slice(&objects_bytes).context("decode G0 pilot object inventory")?;
        let mut distinct_keys = BTreeSet::new();
        let data_files = objects
            .iter()
            .filter(|object| object.key.starts_with("data/") && object.key.ends_with(".parquet"))
            .count();
        ensure!(
            u64::try_from(data_files).ok() == Some(facts.data_file_count)
                && objects.iter().all(|object| {
                    (object.key.starts_with("metadata/")
                        || (object.key.starts_with("data/") && object.key.ends_with(".parquet")))
                        && !object.key.contains("..")
                        && !object.etag.is_empty()
                        && object.size > 0
                        && (!short_fixture
                            || !object.key.starts_with("data/")
                            || object.sha256.as_deref().is_some_and(is_sha256))
                        && distinct_keys.insert(&object.key)
                }),
            "fixture source object inventory does not match declared data files"
        );
        if short_fixture && data_files == 1 {
            let data_object = objects
                .iter()
                .find(|object| object.key.starts_with("data/"))
                .context("short fixture has no source data object")?;
            ensure!(
                data_object.sha256.as_ref() == facts.artifacts.get("data.parquet"),
                "short fixture data artifact does not match its source object"
            );
        }
        let oracle_bytes =
            fs::read(root.join("oracle.json")).context("read G0 pilot row oracle")?;
        ensure!(
            facts.artifacts.get("oracle.json") == Some(&sha256(&oracle_bytes)),
            "fixture oracle hash mismatch"
        );
        let oracle: FixtureOracle =
            serde_json::from_slice(&oracle_bytes).context("decode G0 pilot oracle")?;
        let expected_rows = if a4_fixture { 768 } else { 4096 };
        let expected_id_sum = (expected_rows - 1) * expected_rows / 2;
        ensure!(
            oracle.row_count == expected_rows
                && oracle.row_count == facts.row_count
                && oracle.min_id == 0
                && oracle.max_id == expected_rows - 1
                && oracle.id_sum == expected_id_sum
                && if a4_fixture {
                    oracle.value_sum > 0
                } else {
                    oracle.value_sum == 3 * expected_id_sum
                },
            "published Iceberg row oracle is inconsistent"
        );
        Ok(Self {
            facts,
            objects,
            manifest_sha256: digest,
        })
    }

    fn table_name(&self) -> String {
        format!("uea4a2_pilot.{}.{}", self.facts.database, self.facts.table)
    }

    fn catalog_sql(&self, endpoint: &str) -> String {
        let f = &self.facts;
        format!(
            "CREATE EXTERNAL CATALOG uea4a2_pilot PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{}\",\"iceberg.catalog.warehouse\"=\"{}\",\"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"{}\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{}\",\"credential.object-store-metadata.generation\"=\"{}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"static\",\"credential.object-store-data.name\"=\"{}\",\"credential.object-store-data.generation\"=\"{}\")",
            sql_string(&f.rest_uri),
            sql_string(&f.warehouse_uri),
            sql_string(endpoint),
            sql_string(&f.region),
            f.credential_name,
            f.credential_generation,
            f.credential_name,
            f.credential_generation
        )
    }
}

struct LiveFixture {
    checked: CheckedFixture,
    proxy: DelayedS3Proxy,
}

fn credential_overlay(purpose: &str, fixture: &PublishedIcebergFixture) -> String {
    format!(
        "[[connector.credentials]]\npurpose = \"{purpose}\"\nname = \"{}\"\ngeneration = \"{}\"\nkind = \"s3\"\naccess_key_id = \"${{ENV:{ACCESS_KEY_ENV}}}\"\naccess_key_secret = \"${{ENV:{SECRET_KEY_ENV}}}\"\n",
        fixture.credential_name, fixture.credential_generation
    )
}

fn env_path(name: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(env::var(name).with_context(|| {
        format!("{name} must name a published manifest")
    })?))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut reader =
        BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = reader
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
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

/// Functional native read and cancellation checks over the published A4
/// multi-file table. This stage has no timing, throughput, or RSS assertion.
#[derive(Default)]
struct RangeFunctional {
    fixture: Mutex<Option<LiveFixture>>,
    short: bool,
}

impl RangeFunctional {
    fn checked_fixture(&self) -> Result<CheckedFixture> {
        let variable = if self.short {
            SHORT_FIXTURE_ENV
        } else {
            FIXTURE_ENV
        };
        let fixture = CheckedFixture::load_source(&env_path(variable)?, !self.short)?;
        let expected_kind = if self.short {
            "uea4a2-iceberg-short-v1"
        } else {
            "uea4a4-iceberg-performance-v1"
        };
        ensure!(
            fixture.facts.fixture_kind == expected_kind,
            "functional range input does not match the explicitly selected fixture"
        );
        Ok(fixture)
    }
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

    fn validate_runner_inputs(&self, _profile: LaunchProfile) -> Result<()> {
        self.checked_fixture()?;
        for name in ["AWS_S3_ACCESS_KEY_ID", "AWS_S3_SECRET_ACCESS_KEY"] {
            ensure!(!env::var(name)?.is_empty(), "{name} must be nonempty");
        }
        Ok(())
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fixture = self.checked_fixture()?;
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: fixture.facts.s3_endpoint.clone(),
            delay: Duration::ZERO,
        })?;
        for (index, object) in fixture.objects.iter().enumerate() {
            let uri = format!(
                "{}/{}",
                fixture.facts.table_location.trim_end_matches('/'),
                object.key
            );
            let path = uri
                .strip_prefix("s3://")
                .context("fixture object is not S3")?;
            proxy.label_object(&format!("/{path}"), &format!("object_{index}"))?;
        }
        let mut child = CrossProcessChildEnvironment::default();
        let access = env::var("AWS_S3_ACCESS_KEY_ID")?;
        let secret = env::var("AWS_S3_SECRET_ACCESS_KEY")?;
        for values in [&mut child.fe, &mut child.be] {
            values.insert(ACCESS_KEY_ENV.to_owned(), access.clone());
            values.insert(SECRET_KEY_ENV.to_owned(), secret.clone());
        }
        let metadata = format!(
            "{}\n{FUNCTIONAL_CACHE_OVERLAY}",
            credential_overlay("object-store-metadata", &fixture.facts)
        );
        let data = format!(
            "{}\n{FUNCTIONAL_CACHE_OVERLAY}",
            credential_overlay("object-store-data", &fixture.facts)
        );
        *self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("functional fixture lock poisoned"))? =
            Some(LiveFixture {
                checked: fixture,
                proxy,
            });
        Ok(ScenarioLaunchConfig {
            child_environment: child,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(metadata),
                be: Some(data),
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
        context.action(format!(
            "functional Iceberg input manifest_sha256={}",
            fixture.checked.manifest_sha256
        ));
        let table = fixture.checked.table_name();
        let mut connection =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        connection
            .execute(fixture.checked.catalog_sql(fixture.proxy.endpoint()))
            .map(|_| ())
            .context("create functional Iceberg catalog")?;
        if self.short {
            drop(connection);
            run_range_clients(context, &table, true)?;
            context.action("four short-range clients independently verified (id, 3 * id) for id=1..10 and exited");
            return Ok(());
        }
        let scan = format!("SELECT id FROM {table} WHERE id BETWEEN 0 AND 767 ORDER BY id");
        let initial_baseline = resource_baseline(context)?;
        let before = fixture.proxy.snapshot();
        verify_functional_rows(&mut connection, &scan)?;
        let after = fixture.proxy.snapshot();
        ensure!(
            after.gets > before.gets && after.upstream_errors == before.upstream_errors,
            "multi-file Iceberg read did not complete real S3 GETs without upstream errors"
        );
        context.action("verified every ordered ID across the published multi-file Iceberg table");
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
        let hold = fixture.proxy.hold_next_read_with_suffix(".parquet")?;
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
        if let Err(error) =
            record_cancelled_proxy_read(context, "a4-held-read", &fixture.proxy, &hold)
        {
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
        self.fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("functional fixture lock poisoned"))?
            .take();
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
        rows.len() == 768 && rows.iter().copied().eq(0..768),
        "multi-file Iceberg rows differ from the published ordered ID oracle"
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
        format!("SELECT id FROM {table} WHERE id BETWEEN 0 AND 767 ORDER BY id")
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
