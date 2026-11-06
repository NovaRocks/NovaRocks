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

//! Short real-provider planning regression check for UEA-4A-4.
//!
//! The same scenario and workload run against B0 and candidate binaries. This
//! coarse check only rejects large regressions; it is not a formal benchmark.

use super::connector::{await_resource_convergence, require_three_backends, resource_baseline};
use super::provider_mysql_actors::{
    ProviderActor, finish_provider_operation, provider_failure_receipt,
    record_cancelled_proxy_read, unsigned_first, unsigned_pairs, unsigned_rows,
};
use crate::actors::sql::{SqlSession, StatementFailure, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::delayed_s3::{DelayedS3Config, DelayedS3Proxy};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, LaunchProfile,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::env;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

const ICEBERG_FIXTURE_ENV: &str = "NOVAROCKS_UEA4A4_ICEBERG_FIXTURE_MANIFEST";
const PAIMON_FIXTURE_ENV: &str = "NOVAROCKS_UEA4A4_PAIMON_FIXTURE_MANIFEST";
const ACCESS_KEY_ENV: &str = "NOVAROCKS_UEA4A4_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "NOVAROCKS_UEA4A4_S3_SECRET_ACCESS_KEY";

#[derive(Clone, Debug, Deserialize)]
struct ProviderFixture {
    schema_version: u32,
    fixture_kind: String,
    warehouse_uri: String,
    s3_endpoint: String,
    region: String,
    credential_name: String,
    credential_generation: String,
    database: String,
    table: String,
    snapshot_id: serde_json::Value,
    schema_id: serde_json::Value,
    data_file_count: u64,
    row_count: u64,
    objects_sha256: String,
    #[serde(default)]
    rest_uri: Option<String>,
}

#[derive(Clone)]
struct CheckedFixture {
    provider: &'static str,
    facts: ProviderFixture,
    manifest_sha256: String,
}

impl CheckedFixture {
    fn load(variable: &str, provider: &'static str, minimum_data_files: u64) -> Result<Self> {
        let path = PathBuf::from(
            env::var(variable)
                .with_context(|| format!("{variable} must name a published fixture"))?,
        );
        let bytes = fs::read(&path)
            .with_context(|| format!("read {provider} fixture manifest {}", path.display()))?;
        let manifest_sha256 = sha256(&bytes);
        let root = path
            .parent()
            .context("fixture manifest has no parent directory")?;
        let ready = fs::read_to_string(root.join("READY"))
            .with_context(|| format!("read {provider} fixture READY"))?;
        ensure!(
            ready.trim() == format!("sha256:{manifest_sha256}"),
            "{provider} fixture READY does not bind the exact manifest"
        );
        let facts: ProviderFixture = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode {provider} fixture manifest"))?;
        ensure!(
            facts.schema_version == 1,
            "unsupported {provider} fixture version"
        );
        ensure!(
            facts.fixture_kind == format!("uea4a4-{provider}-performance-v1"),
            "unexpected {provider} fixture kind"
        );
        ensure!(
            facts.data_file_count >= minimum_data_files && facts.row_count > 0,
            "{provider} fixture is too small for short planning regression"
        );
        for identifier in [&facts.database, &facts.table] {
            ensure!(
                !identifier.is_empty()
                    && identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
                "{provider} fixture contains an invalid database or table identifier"
            );
        }
        for identifier in [&facts.credential_name, &facts.credential_generation] {
            ensure!(
                !identifier.is_empty()
                    && identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'),
                "{provider} fixture contains an invalid identifier"
            );
        }
        ensure!(
            !facts.snapshot_id.is_null()
                && !facts.schema_id.is_null()
                && facts.snapshot_id.as_str().is_none_or(|id| !id.is_empty())
                && facts.schema_id.as_str().is_none_or(|id| !id.is_empty()),
            "{provider} fixture omitted snapshot or schema identity"
        );
        ensure!(
            facts.s3_endpoint.starts_with("http://127.0.0.1:")
                || facts.s3_endpoint.starts_with("http://localhost:"),
            "{provider} fixture must use a loopback HTTP S3 endpoint"
        );
        ensure!(
            provider != "iceberg" || facts.rest_uri.as_deref().is_some_and(|uri| !uri.is_empty()),
            "Iceberg fixture omitted REST Catalog endpoint"
        );
        let objects = fs::read(root.join("objects.json"))
            .with_context(|| format!("read {provider} fixture object inventory"))?;
        ensure!(
            sha256(&objects) == facts.objects_sha256,
            "{provider} fixture object inventory hash mismatch"
        );
        Ok(Self {
            provider,
            facts,
            manifest_sha256,
        })
    }

    fn catalog_name(&self, slow: bool) -> String {
        format!(
            "uea4a4_{}_{}",
            self.provider,
            if slow { "slow" } else { "normal" }
        )
    }

    fn table_name(&self, slow: bool) -> String {
        format!(
            "{}.{}.{}",
            self.catalog_name(slow),
            self.facts.database,
            self.facts.table
        )
    }

    fn catalog_sql(&self, slow: bool, s3_endpoint: &str) -> String {
        let name = self.catalog_name(slow);
        let properties = if self.provider == "iceberg" {
            format!(
                "\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{}\",\"iceberg.catalog.warehouse\"=\"{}\"",
                sql_string(self.facts.rest_uri.as_deref().unwrap_or_default()),
                sql_string(&self.facts.warehouse_uri)
            )
        } else {
            format!(
                "\"paimon.catalog.type\"=\"filesystem\",\"warehouse\"=\"{}\"",
                sql_string(&self.facts.warehouse_uri)
            )
        };
        format!(
            "CREATE EXTERNAL CATALOG {name} PROPERTIES(\"type\"=\"{}\",{properties},\
             \"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"{}\",\
             \"aws.s3.enable_path_style_access\"=\"true\",\
             \"credential.object-store-metadata.consumer-role\"=\"frontend\",\
             \"credential.object-store-metadata.mode\"=\"static\",\
             \"credential.object-store-metadata.name\"=\"{}\",\
             \"credential.object-store-metadata.generation\"=\"{}\",\
             \"credential.object-store-data.consumer-role\"=\"backend\",\
             \"credential.object-store-data.mode\"=\"static\",\
             \"credential.object-store-data.name\"=\"{}\",\
             \"credential.object-store-data.generation\"=\"{}\")",
            self.provider,
            sql_string(s3_endpoint),
            sql_string(&self.facts.region),
            self.facts.credential_name,
            self.facts.credential_generation,
            self.facts.credential_name,
            self.facts.credential_generation,
        )
    }
}

struct LiveFixtures {
    iceberg: CheckedFixture,
    paimon: CheckedFixture,
    iceberg_slow: DelayedS3Proxy,
    paimon_slow: DelayedS3Proxy,
}

/// Shared functional setup owns published inputs, credentials and proxies.
/// It does not load or validate measurement windows, repetitions or warmup.
struct FunctionalPlanningSetup {
    fixtures: Mutex<Option<LiveFixtures>>,
    concurrency_limit: usize,
}

impl FunctionalPlanningSetup {
    fn new(concurrency_limit: usize) -> Self {
        Self {
            fixtures: Mutex::new(None),
            concurrency_limit,
        }
    }

    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        ensure!(
            profile == LaunchProfile::Performance,
            "provider functionality requires the clean --launch-profile performance environment"
        );
        CheckedFixture::load(ICEBERG_FIXTURE_ENV, "iceberg", 16)?;
        CheckedFixture::load(PAIMON_FIXTURE_ENV, "paimon", 16)?;
        Ok(())
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        launch_planning_config(&self.fixtures, self.concurrency_limit, 16, Duration::ZERO)
    }

    fn teardown(&self) -> Result<()> {
        self.fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("provider fixture lock poisoned"))?
            .take();
        Ok(())
    }
}

fn launch_planning_config(
    fixtures: &Mutex<Option<LiveFixtures>>,
    concurrency_limit: usize,
    minimum_data_files: u64,
    delay: Duration,
) -> Result<ScenarioLaunchConfig> {
    let iceberg = CheckedFixture::load(ICEBERG_FIXTURE_ENV, "iceberg", minimum_data_files)?;
    let paimon = CheckedFixture::load(PAIMON_FIXTURE_ENV, "paimon", minimum_data_files)?;
    let iceberg_slow = DelayedS3Proxy::start(DelayedS3Config {
        downstream: iceberg.facts.s3_endpoint.clone(),
        delay,
    })?;
    let paimon_slow = DelayedS3Proxy::start(DelayedS3Config {
        downstream: paimon.facts.s3_endpoint.clone(),
        delay,
    })?;
    let access_key = env::var("AWS_S3_ACCESS_KEY_ID")
        .context("provider fixture requires AWS_S3_ACCESS_KEY_ID")?;
    let secret_key = env::var("AWS_S3_SECRET_ACCESS_KEY")
        .context("provider fixture requires AWS_S3_SECRET_ACCESS_KEY")?;
    ensure!(
        !access_key.is_empty() && !secret_key.is_empty(),
        "UEA-4A-4 fixture credentials must be nonempty"
    );
    let mut child_environment = CrossProcessChildEnvironment::default();
    for values in [&mut child_environment.fe, &mut child_environment.be] {
        values.insert(ACCESS_KEY_ENV.to_owned(), access_key.clone());
        values.insert(SECRET_KEY_ENV.to_owned(), secret_key.clone());
    }
    let mut fe = format!(
        "[runtime.frontend_workload]\nconcurrency_limit = {}\nwaiting_limit = 512\ncapacity_wait_timeout_ms = 30000\n",
        concurrency_limit
    );
    let mut be = String::new();
    let mut credentials = std::collections::BTreeSet::new();
    for fixture in [&iceberg, &paimon] {
        credentials.insert((
            fixture.facts.credential_name.as_str(),
            fixture.facts.credential_generation.as_str(),
        ));
    }
    for (name, generation) in credentials {
        fe.push_str(&credential_overlay(
            "object-store-metadata",
            name,
            generation,
        ));
        be.push_str(&credential_overlay("object-store-data", name, generation));
    }
    *fixtures
        .lock()
        .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))? = Some(LiveFixtures {
        iceberg,
        paimon,
        iceberg_slow,
        paimon_slow,
    });
    Ok(ScenarioLaunchConfig {
        child_environment,
        config_overlay: CrossProcessConfigOverlay {
            fe: Some(fe),
            be: Some(be),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn credential_overlay(purpose: &str, name: &str, generation: &str) -> String {
    format!(
        "[[connector.credentials]]\npurpose = \"{purpose}\"\nname = \"{name}\"\ngeneration = \"{generation}\"\nkind = \"s3\"\naccess_key_id = \"${{ENV:{ACCESS_KEY_ENV}}}\"\naccess_key_secret = \"${{ENV:{SECRET_KEY_ENV}}}\"\n"
    )
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
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
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

/// Hold a real Iceberg object request while KILL QUERY remains responsive.
/// The SDK manifest and native file reader are exercised separately because
/// they have different cancellation wiring and may run on different roles.
struct IcebergHeldReadCancellation {
    setup: FunctionalPlanningSetup,
}

/// Exercises the FE planning and admitted BE read paths with both real
/// providers. The published small fixture cannot cross the former Paimon
/// item/byte limits; that separate boundary is reported as uncovered.
struct CatalogPlanningNoFeQuota {
    setup: FunctionalPlanningSetup,
}

/// Verifies that an FE metadata read remains owned after query cancellation:
/// the next provider query can use the released warehouse slot while the old
/// object request remains held, and the old query eventually exits cancelled.
struct CatalogPlanningResponsibility {
    setup: FunctionalPlanningSetup,
}

impl CatalogPlanningResponsibility {
    fn new() -> Self {
        Self {
            setup: FunctionalPlanningSetup::new(1),
        }
    }
}

impl Scenario for CatalogPlanningResponsibility {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "uea4_catalog_planning",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "uea4/catalog-planning-responsibility"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, launch_profile: LaunchProfile) -> Result<()> {
        self.setup.validate_runner_inputs(launch_profile)
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        self.setup.launch_config(scenario_root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixtures = self
            .setup
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))?;
        let fixtures = fixtures
            .as_ref()
            .context("UEA-4A-4 fixture was not prepared")?;
        let mut control =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        for fixture in [&fixtures.iceberg, &fixtures.paimon] {
            control
                .execute(fixture.catalog_sql(
                    true,
                    if fixture.provider == "iceberg" {
                        fixtures.iceberg_slow.endpoint()
                    } else {
                        fixtures.paimon_slow.endpoint()
                    },
                ))
                .map(|_| ())
                .with_context(|| format!("create {} held-read catalog", fixture.provider))?;
            control
                .execute(fixture.catalog_sql(false, &fixture.facts.s3_endpoint))
                .map(|_| ())
                .with_context(|| format!("create {} independent catalog", fixture.provider))?;
        }
        drop(control);

        let iceberg_sql = format!(
            "SELECT COUNT(*) FROM {} WHERE id BETWEEN 1 AND 1000",
            fixtures.iceberg.table_name(true)
        );
        let paimon_sql = format!(
            "SELECT COUNT(*) FROM {} WHERE id BETWEEN 1 AND 1000",
            fixtures.paimon.table_name(true)
        );
        let iceberg_followup = format!(
            "SELECT COUNT(*) FROM {}",
            fixtures.iceberg.table_name(false)
        );
        let paimon_followup = format!("SELECT COUNT(*) FROM {}", fixtures.paimon.table_name(false));
        let paimon_snapshot_path = format!(
            "/{}.db/{}/snapshot/LATEST",
            fixtures.paimon.facts.database, fixtures.paimon.facts.table
        );

        context.action("hold Iceberg SDK manifest, cancel Q1, admit independent Paimon Q2 before releasing Q1 read");
        run_held_provider_read(
            context,
            &fixtures.iceberg_slow,
            &iceberg_sql,
            &paimon_followup,
            fixtures.paimon.facts.row_count,
            "Iceberg",
            "sdk-manifest",
            Some(".avro"),
            true,
        )?;
        context.action("hold Paimon snapshot/LATEST metadata, cancel Q1, admit independent Iceberg Q2 before releasing Q1 read");
        run_held_provider_read(
            context,
            &fixtures.paimon_slow,
            &paimon_sql,
            &iceberg_followup,
            fixtures.iceberg.facts.row_count,
            "Paimon",
            "snapshot-metadata",
            Some(&paimon_snapshot_path),
            true,
        )?;
        context
            .action("hold Iceberg native data read and verify cancellation at its next checkpoint");
        run_held_provider_read(
            context,
            &fixtures.iceberg_slow,
            &iceberg_sql,
            &paimon_followup,
            fixtures.paimon.facts.row_count,
            "Iceberg",
            "native-data",
            Some(".parquet"),
            false,
        )?;
        context.action("hold Iceberg SDK manifest until its one-second statement deadline expires without KILL QUERY");
        run_iceberg_held_deadline(
            context,
            &fixtures.iceberg_slow,
            &iceberg_sql,
            &paimon_followup,
            fixtures.paimon.facts.row_count,
        )?;
        let mut control =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        for fixture in [&fixtures.iceberg, &fixtures.paimon] {
            let table = fixture.table_name(false);
            let rows = control
                .execute(format!("EXPLAIN SELECT COUNT(*) FROM {table}"))
                .with_context(|| {
                    format!(
                        "EXPLAIN {} through provider metadata planning",
                        fixture.provider
                    )
                })?;
            ensure!(
                rows.is_result_set()
                    && rows.columns.len() == 1
                    && rows.columns[0].type_signature.raw_type == "varchar",
                "EXPLAIN returned non-text metadata: {rows:?}"
            );
            ensure!(
                rows.rows
                    .iter()
                    .all(|row| matches!(row.as_slice(), [TrinoValue::Text(_)])),
                "EXPLAIN returned non-text plan cells: {rows:?}"
            );
            let row_count = rows.rows.len();
            ensure!(
                row_count > 0,
                "{} EXPLAIN returned no plan rows",
                fixture.provider
            );
            context.action(format!(
                "{} non-SELECT EXPLAIN returned {row_count} plan rows",
                fixture.provider
            ));
        }
        let report = serde_json::json!({
            "schema_version": 1,
            "acceptance_status": "partial",
            "binary_sha256": sha256_file(context.primary_binary())?,
            "iceberg_fixture_sha256": fixtures.iceberg.manifest_sha256,
            "paimon_fixture_sha256": fixtures.paimon.manifest_sha256,
            "native_topology": "1FE+3BE",
            "covered": ["Iceberg SDK and FS held reads", "Paimon snapshot/LATEST metadata read held", "KILL QUERY while read held", "Iceberg held SDK statement deadline without KILL", "independent query before old read is released", "held deadline read fully forwarded after release", "both providers EXPLAIN", "independent CREATE EXTERNAL CATALOG"],
            "uncovered": ["DML or maintenance owner in this scenario", "direct native observation of ReadAccessSink late deposit", "native file-layer DeadlineExceeded type (covered by separate FS and SDK unit tests)"],
        });
        fs::write(
            context
                .scenario_root()
                .join("uea4a4-responsibility-coverage.json"),
            serde_json::to_vec_pretty(&report)?,
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.setup.teardown()
    }
}

impl CatalogPlanningNoFeQuota {
    fn new() -> Self {
        Self {
            setup: FunctionalPlanningSetup::new(3),
        }
    }
}

impl Scenario for CatalogPlanningNoFeQuota {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "uea4_catalog_planning",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "uea4/catalog-planning-no-fe-quota"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, launch_profile: LaunchProfile) -> Result<()> {
        self.setup.validate_runner_inputs(launch_profile)
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        self.setup.launch_config(scenario_root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixtures = self
            .setup
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))?;
        let fixtures = fixtures
            .as_ref()
            .context("UEA-4A-4 fixture was not prepared")?;
        let mut connection =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        for fixture in [&fixtures.iceberg, &fixtures.paimon] {
            connection
                .execute(fixture.catalog_sql(false, &fixture.facts.s3_endpoint))
                .map(|_| ())
                .with_context(|| format!("create {} no-quota catalog", fixture.provider))?;
            let table = fixture.table_name(false);
            let count: Option<u64> = connection
                .execute(format!("SELECT COUNT(*) FROM {table}"))
                .and_then(unsigned_first)
                .with_context(|| format!("read {} through admitted execution", fixture.provider))?;
            ensure!(
                count == Some(fixture.facts.row_count),
                "{} native row count differs from its published fixture",
                fixture.provider
            );
            context.action(format!(
                "{} FE catalog/planning and BE admitted read returned {} rows from {} published files",
                fixture.provider, fixture.facts.row_count, fixture.facts.data_file_count
            ));
        }
        let baseline = resource_baseline(context)?;
        let paimon_table = fixtures.paimon.table_name(false);
        let budget_error = connection
            .execute(format!(
                "SELECT /*+ SET_VAR(query_mem_limit=1) */ * FROM {paimon_table}"
            ))
            .expect_err("one-byte query memory budget must reject the admitted Paimon read");
        if !(matches!(&budget_error, StatementFailure::Trino(_))
            && budget_error
                .to_string()
                .to_ascii_lowercase()
                .contains("memory"))
        {
            return Err(anyhow::Error::new(budget_error)
                .context("low-budget Paimon read failed for another reason"));
        }
        await_resource_convergence(context, &baseline, "Paimon BE capacity refusal")?;
        let recovered_rows: Option<u64> = connection
            .execute(format!("SELECT COUNT(*) FROM {paimon_table}"))
            .and_then(unsigned_first)
            .context("read Paimon after BE capacity refusal")?;
        ensure!(
            recovered_rows == Some(fixtures.paimon.facts.row_count),
            "Paimon read after BE capacity refusal differed from its published fixture"
        );
        await_resource_convergence(context, &baseline, "Paimon BE capacity recovery")?;
        context.action(format!(
            "Paimon BE rejected a one-byte query memory budget and a normal-budget read recovered with {} rows",
            fixtures.paimon.facts.row_count
        ));
        let report = serde_json::json!({
            "schema_version": 1,
            "acceptance_status": "partial",
            "binary_sha256": sha256_file(context.primary_binary())?,
            "iceberg_fixture_sha256": fixtures.iceberg.manifest_sha256,
            "paimon_fixture_sha256": fixtures.paimon.manifest_sha256,
            "native_topology": "1FE+3BE",
            "covered": ["real Iceberg FE planning and BE read", "real Paimon FE planning and BE read", "Paimon BE query memory capacity refusal and recovery"],
            "uncovered": ["Paimon 256 MiB FE ledger", "Paimon catalog entries above 65536", "Paimon planned splits above 1000000", "Paimon planned files above 4000000", "Paimon listing above 16 MiB", "Paimon frozen metadata above 32 MiB", "Paimon split metadata above 256 MiB"],
        });
        fs::write(
            context
                .scenario_root()
                .join("uea4a4-no-fe-quota-coverage.json"),
            serde_json::to_vec_pretty(&report)?,
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.setup.teardown()
    }
}

impl IcebergHeldReadCancellation {
    fn new() -> Self {
        Self {
            setup: FunctionalPlanningSetup::new(1),
        }
    }
}

impl Scenario for IcebergHeldReadCancellation {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "uea4_catalog_planning",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "uea4/iceberg-held-read-cancellation"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, launch_profile: LaunchProfile) -> Result<()> {
        self.setup.validate_runner_inputs(launch_profile)
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        self.setup.launch_config(scenario_root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixtures = self
            .setup
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-4A-4 fixture lock poisoned"))?;
        let fixtures = fixtures
            .as_ref()
            .context("UEA-4A-4 fixture was not prepared")?;
        let fixture = &fixtures.iceberg;
        let proxy = &fixtures.iceberg_slow;
        let mut control =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        control
            .execute(fixture.catalog_sql(true, proxy.endpoint()))
            .map(|_| ())
            .context("create Iceberg held-read catalog")?;
        control
            .execute(
                fixtures
                    .paimon
                    .catalog_sql(false, &fixtures.paimon.facts.s3_endpoint),
            )
            .map(|_| ())
            .context("create independent Paimon follow-up catalog")?;
        let table = fixture.table_name(true);
        let read_sql = format!("SELECT COUNT(*) FROM {table} WHERE id BETWEEN 1 AND 1000");
        let followup_sql = format!("SELECT COUNT(*) FROM {}", fixtures.paimon.table_name(false));
        let followup_count = fixtures.paimon.facts.row_count;
        drop(control);

        for (owner, suffix) in [("sdk-manifest", ".avro"), ("native-data", ".parquet")] {
            context.action(format!(
                "hold real Iceberg {owner} GET/HEAD, cancel its query, then release the read"
            ));
            run_held_provider_read(
                context,
                proxy,
                &read_sql,
                &followup_sql,
                followup_count,
                "Iceberg",
                owner,
                Some(suffix),
                owner == "sdk-manifest",
            )?;
        }
        let mut control =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let count: Option<u64> = control
            .execute(format!("SELECT COUNT(*) FROM {table}"))
            .and_then(unsigned_first)
            .context("read Iceberg table after held cancellations")?;
        ensure!(
            count == Some(fixture.facts.row_count),
            "Iceberg post-cancellation row count differs from fixture"
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.setup.teardown()
    }
}

fn frontend_governance_snapshot(context: &mut ScenarioContext) -> Result<String> {
    let state = frontend_workload_state(context)?;
    Ok(format!(
        "active={} governance={}",
        state
            .pointer("/workload/active/statement")
            .unwrap_or(&serde_json::Value::Null),
        state
            .pointer("/workload/governance")
            .unwrap_or(&serde_json::Value::Null)
    ))
}

fn frontend_workload_state(context: &mut ScenarioContext) -> Result<serde_json::Value> {
    let timeout = context
        .remaining("read FE workload state")?
        .min(Duration::from_secs(2));
    let response = context
        .handle()
        .frontend_management_get("/v1/frontend/state", timeout)?;
    ensure!(
        response.status == 200,
        "FE workload HTTP {}",
        response.status
    );
    serde_json::from_str(&response.body).context("decode FE workload state")
}

pub(super) fn await_frontend_local_exit(
    context: &mut ScenarioContext,
    operation: &str,
) -> Result<()> {
    let deadline = Instant::now() + context.remaining(operation)?.min(Duration::from_secs(45));
    loop {
        let state = frontend_workload_state(context)?;
        let counts = [
            "/workload/active/statement",
            "/workload/governance/root_responsibilities",
            "/workload/governance/admitted_queries",
            "/workload/governance/preparation",
            "/workload/governance/execution",
            "/workload/governance/obligations",
            "/workload/governance/control_inflight",
        ];
        if counts
            .iter()
            .all(|path| state.pointer(path).and_then(serde_json::Value::as_u64) == Some(0))
        {
            context.action(format!(
                "{operation}: FE query and remote observation owners exited"
            ));
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "{operation} did not converge after the held object returned: active={} governance={}",
            state
                .pointer("/workload/active/statement")
                .unwrap_or(&serde_json::Value::Null),
            state
                .pointer("/workload/governance")
                .unwrap_or(&serde_json::Value::Null)
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn run_iceberg_held_deadline(
    context: &mut ScenarioContext,
    proxy: &DelayedS3Proxy,
    sql: &str,
    followup_sql: &str,
    followup_count: u64,
) -> Result<()> {
    let baseline = resource_baseline(context)?;
    let hold = proxy.hold_next_read_with_suffix(".avro")?;
    let query = sql.to_owned();
    let mut query_actor = ProviderActor::spawn(
        context,
        "iceberg-deadline-q1",
        move |connection, statements| {
            statements
                .run(connection, "SET query_timeout = 1")?
                .context("set one-second statement deadline")?;
            Ok(statements.run(connection, query)?.and_then(unsigned_rows))
        },
    )?;
    let mut followup_actor = None;
    let mut entered = false;
    let operation = (|| -> Result<()> {
        query_actor.wait_started(context.remaining("await deadline query connection")?)?;
        hold.wait_until_entered(
            context
                .remaining("observe held Iceberg deadline manifest read")?
                .min(Duration::from_secs(30)),
        )?;
        entered = true;
        let before = proxy.snapshot();
        let result = query_actor.receive(
            context
                .remaining("await statement deadline while S3 is held")?
                .min(Duration::from_secs(10)),
        )?;
        let error = result.err().context(
            "held Iceberg query unexpectedly succeeded instead of its deadline terminal",
        )?;
        if !matches!(&error, StatementFailure::Trino(error) if error.error_code == 131075 && error.error_name == "EXCEEDED_TIME_LIMIT" && error.error_type == "INSUFFICIENT_RESOURCES")
        {
            return Err(anyhow::Error::new(error)
                .context("held Iceberg query did not return its original deadline terminal"));
        }
        let state = frontend_workload_state(context)?;
        ensure!(
            state
                .pointer("/workload/governance/admitted_queries")
                .and_then(serde_json::Value::as_u64)
                == Some(0),
            "Q1 deadline terminal retained the sole warehouse permit: {state}"
        );
        context.action("Q1 returned a statement-timeout terminal and released admission while its SDK manifest read remained held");
        let query = followup_sql.to_owned();
        followup_actor = Some(ProviderActor::spawn(
            context,
            "iceberg-deadline-q2",
            move |connection, statements| {
                statements
                    .run(connection, query)?
                    .and_then(unsigned_first)
                    .context("read Q2 after Iceberg deadline")
            },
        )?);
        let actor = followup_actor.as_mut().expect("follow-up actor installed");
        actor.wait_started(context.remaining("await deadline follow-up connection")?)?;
        let observed = actor
            .receive(
                context
                    .remaining("await Q2 while expired Iceberg read remains held")?
                    .min(Duration::from_secs(10)),
            )
            .with_context(|| {
                format!(
                    "Q2 failed before Q1 read release; FE workload: {}",
                    frontend_governance_snapshot(context)
                        .unwrap_or_else(|error| format!("unavailable: {error:#}"))
                )
            })?;
        ensure!(
            observed == Some(followup_count),
            "Q2 returned {observed:?} while expired Q1 was held"
        );
        context.action("independent Q2 completed before the expired Q1 object read was released");
        hold.release();
        hold.wait_until_cancelled_read_exited(
            context
                .remaining("observe expired Iceberg request exit")?
                .min(Duration::from_secs(30)),
        )?;
        let after = proxy.snapshot();
        ensure!(
            after.gets + after.heads >= before.gets + before.heads
                && after.upstream_errors == before.upstream_errors,
            "released Iceberg deadline manifest read did not complete cleanly"
        );
        Ok(())
    })();
    let mut cleanup = Vec::new();
    if operation.is_err() {
        collect_actor_cleanup(&mut cleanup, "cancel Q1", query_actor.cancel(context));
        if let Some(actor) = &mut followup_actor {
            collect_actor_cleanup(&mut cleanup, "cancel Q2", actor.cancel(context));
        }
    }
    hold.release();
    if entered {
        collect_actor_cleanup(
            &mut cleanup,
            "cancelled held request exit",
            hold.wait_until_cancelled_read_exited(Duration::from_secs(30))
                .map(|_| ()),
        );
    }
    collect_actor_cleanup(
        &mut cleanup,
        "join Q1",
        query_actor.finish_with(context, |result| {
            result.map(|_| ()).map_err(anyhow::Error::from)
        }),
    );
    if let Some(actor) = &mut followup_actor {
        collect_actor_cleanup(
            &mut cleanup,
            "join Q2",
            actor.finish_with(context, |observed| {
                ensure!(
                    observed == Some(followup_count),
                    "unconsumed original follow-up count differs: {observed:?}"
                );
                Ok(())
            }),
        );
    }
    collect_actor_cleanup(
        &mut cleanup,
        "FE exit",
        await_frontend_local_exit(context, "Iceberg held deadline"),
    );
    collect_actor_cleanup(
        &mut cleanup,
        "BE exit",
        await_resource_convergence(context, &baseline, "Iceberg held deadline"),
    );
    collect_actor_cleanup(
        &mut cleanup,
        "proxy terminal evidence",
        record_cancelled_proxy_read(context, "iceberg-held-deadline", proxy, &hold),
    );
    finish_held_evidence(context, "iceberg-held-deadline", operation, cleanup)
}

fn run_held_provider_read(
    context: &mut ScenarioContext,
    proxy: &DelayedS3Proxy,
    sql: &str,
    followup_sql: &str,
    followup_count: u64,
    provider: &str,
    owner: &str,
    suffix: Option<&str>,
    expect_independent_followup: bool,
) -> Result<()> {
    let baseline = resource_baseline(context)?;
    let hold = match suffix {
        Some(suffix) if suffix.starts_with('/') => proxy.hold_next_read_with_path_suffix(suffix)?,
        Some(suffix) => proxy.hold_next_read_with_suffix(suffix)?,
        None => proxy.hold_next_read()?,
    };
    let query = sql.to_owned();
    let label = format!("{provider}-{owner}");
    let mut query_actor = ProviderActor::spawn(
        context,
        &format!("{label}-q1"),
        move |connection, statements| {
            Ok(statements.run(connection, query)?.and_then(unsigned_rows))
        },
    )?;
    let mut kill_actor = None;
    let mut followup_actor = None;
    let mut entered = false;
    // This original early observation can outlive a later follow-up failure.
    // Keep the single moved result outside the fallible business closure.
    let mut early_result = None;
    let operation = (|| -> Result<()> {
        query_actor.wait_started(context.remaining("receive held provider connection ID")?)?;
        hold.wait_until_entered(
            context
                .remaining("observe real held provider object read")?
                .min(Duration::from_secs(30)),
        )?;
        entered = true;
        let held_snapshot = proxy.snapshot();
        let original_query = query_actor.current_query()?;
        kill_actor = Some(ProviderActor::spawn(
            context,
            &format!("{label}-kill"),
            move |_control, _statements| {
                original_query
                    .cancel()
                    .context("cancel held provider query")
            },
        )?);
        kill_actor
            .as_mut()
            .expect("control actor installed")
            .receive(
                context
                    .remaining("await held provider KILL QUERY")?
                    .min(Duration::from_secs(10)),
            )?;
        early_result = query_actor.try_result()?;
        if expect_independent_followup {
            let query = followup_sql.to_owned();
            followup_actor = Some(ProviderActor::spawn(
                context,
                &format!("{label}-q2"),
                move |connection, statements| {
                    statements
                        .run(connection, query)?
                        .and_then(unsigned_first)
                        .context("independent query while provider read held")
                },
            )?);
            let actor = followup_actor.as_mut().expect("follow-up actor installed");
            actor.wait_started(context.remaining("await held provider follow-up connection")?)?;
            let observed = actor.receive(
                context
                    .remaining("await independent provider query")?
                    .min(Duration::from_secs(10)),
            )?;
            ensure!(
                observed == Some(followup_count),
                "independent query returned {observed:?} while {owner} read was held"
            );
        }
        hold.release();
        hold.wait_until_cancelled_read_exited(
            context
                .remaining("observe cancelled provider request exit")?
                .min(Duration::from_secs(30)),
        )?;
        let result = match early_result.take() {
            Some(result) => result,
            None => query_actor.receive(context.remaining("await held provider query exit")?)?,
        };
        let error = result
            .err()
            .context("held provider query unexpectedly succeeded after cancellation")?;
        if !matches!(&error, StatementFailure::Trino(error) if error.error_code == 3 && error.error_name == "USER_CANCELED" && error.error_type == "USER_ERROR")
        {
            return Err(anyhow::Error::new(error).context(format!(
                "{provider} {owner} query did not report cancellation after S3 release"
            )));
        }
        let after = proxy.snapshot();
        ensure!(
            after.gets + after.heads >= held_snapshot.gets + held_snapshot.heads
                && after.upstream_errors == held_snapshot.upstream_errors,
            "{provider} {owner} S3 proxy did not preserve the held read"
        );
        Ok(())
    })();
    let mut cleanup = Vec::new();
    if let Some(Err(actual)) = early_result.take() {
        cleanup.push(anyhow::Error::new(actual).context(
            "original early Q1 result retained after a later held-read operation failure",
        ));
    }
    if operation.is_err() {
        collect_actor_cleanup(&mut cleanup, "cancel Q1", query_actor.cancel(context));
        if let Some(actor) = &mut kill_actor {
            collect_actor_cleanup(&mut cleanup, "cancel control", actor.cancel(context));
        }
        if let Some(actor) = &mut followup_actor {
            collect_actor_cleanup(&mut cleanup, "cancel Q2", actor.cancel(context));
        }
    }
    hold.release();
    if entered {
        collect_actor_cleanup(
            &mut cleanup,
            "cancelled held request exit",
            hold.wait_until_cancelled_read_exited(Duration::from_secs(30))
                .map(|_| ()),
        );
    }
    collect_actor_cleanup(
        &mut cleanup,
        "join Q1",
        query_actor.finish_with(context, |result| {
            result.map(|_| ()).map_err(anyhow::Error::from)
        }),
    );
    if let Some(actor) = &mut kill_actor {
        collect_actor_cleanup(
            &mut cleanup,
            "join control",
            actor.finish_with(context, |()| Ok(())),
        );
    }
    if let Some(actor) = &mut followup_actor {
        collect_actor_cleanup(
            &mut cleanup,
            "join Q2",
            actor.finish_with(context, |observed| {
                ensure!(
                    observed == Some(followup_count),
                    "unconsumed original follow-up count differs: {observed:?}"
                );
                Ok(())
            }),
        );
    }
    collect_actor_cleanup(
        &mut cleanup,
        "FE exit",
        await_frontend_local_exit(context, &label),
    );
    collect_actor_cleanup(
        &mut cleanup,
        "BE exit",
        await_resource_convergence(context, &baseline, &label),
    );
    collect_actor_cleanup(
        &mut cleanup,
        "proxy terminal evidence",
        record_cancelled_proxy_read(context, &label, proxy, &hold),
    );
    finish_held_evidence(context, &label, operation, cleanup)
}

fn collect_actor_cleanup(errors: &mut Vec<anyhow::Error>, phase: &str, result: Result<()>) {
    if let Err(error) = result {
        errors.push(error.context(phase.to_owned()));
    }
}

fn finish_held_evidence(
    context: &ScenarioContext,
    label: &str,
    operation: Result<()>,
    mut cleanup: Vec<anyhow::Error>,
) -> Result<()> {
    let evidence = (|| -> Result<()> {
        fs::write(
            context.scenario_root().join(format!("{label}-exit.json")),
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
        cleanup.push(error.context("persist original provider exit evidence"));
    }
    finish_provider_operation(operation, cleanup)
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(CatalogPlanningSmoke {
            setup: FunctionalPlanningSetup::new(3),
        }),
        Box::new(IcebergHeldReadCancellation::new()),
        Box::new(CatalogPlanningNoFeQuota::new()),
        Box::new(CatalogPlanningResponsibility::new()),
        Box::new(ProviderReadQueuedProgress {
            setup: FunctionalPlanningSetup::new(1),
        }),
    ]
}

struct CatalogPlanningSmoke {
    setup: FunctionalPlanningSetup,
}

impl Scenario for CatalogPlanningSmoke {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "uea4_catalog_planning",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "uea4/catalog-planning-smoke"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        self.setup.validate_runner_inputs(profile)
    }
    fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
        self.setup.launch_config(root)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let slot = self
            .setup
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("provider fixture lock poisoned"))?;
        let fixtures = slot
            .as_ref()
            .context("provider fixtures were not prepared")?;
        let baseline = resource_baseline(context)?;
        let mut connection =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        for (fixture, proxy) in [
            (&fixtures.iceberg, &fixtures.iceberg_slow),
            (&fixtures.paimon, &fixtures.paimon_slow),
        ] {
            for (slow, endpoint) in [
                (false, fixture.facts.s3_endpoint.as_str()),
                (true, proxy.endpoint()),
            ] {
                connection
                    .execute(fixture.catalog_sql(slow, endpoint))
                    .map(|_| ())?;
                verify_provider_rows(&mut connection, &fixture.table_name(slow), fixture)?;
            }
        }
        drop(connection);
        await_frontend_local_exit(context, "provider smoke")?;
        await_resource_convergence(context, &baseline, "provider smoke")?;
        context.action("verified independent complete Iceberg and Paimon fixture rows through normal and proxy catalogs");
        Ok(())
    }
    fn teardown(&self) -> Result<()> {
        self.setup.teardown()
    }
}

/// The producer inserts 32 consecutive IDs per commit and stores the commit
/// ordinal as value. This formula is independent of either native query result.
fn verify_provider_rows(
    connection: &mut SqlSession,
    table: &str,
    fixture: &CheckedFixture,
) -> Result<()> {
    verify_provider_result(
        connection.execute(format!("SELECT id, value FROM {table} ORDER BY id"))?,
        fixture,
    )
}
fn verify_provider_result(result: StatementResult, fixture: &CheckedFixture) -> Result<()> {
    ensure!(
        fixture.facts.row_count.is_multiple_of(32),
        "provider fixture does not match its 32-row input batches"
    );
    let actual = unsigned_pairs(result)?;
    let expected = (0..fixture.facts.row_count)
        .map(|id| (id, id / 32))
        .collect::<Vec<_>>();
    ensure!(
        actual == expected,
        "{} provider rows differ from their independent fixture formula: {actual:?}",
        fixture.provider
    );
    Ok(())
}

struct ProviderReadQueuedProgress {
    setup: FunctionalPlanningSetup,
}

impl Scenario for ProviderReadQueuedProgress {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "uea4_catalog_planning",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "catalog/provider-read-queued-progress"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        self.setup.validate_runner_inputs(profile)
    }
    fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
        self.setup.launch_config(root)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let slot = self
            .setup
            .fixtures
            .lock()
            .map_err(|_| anyhow::anyhow!("provider fixture lock poisoned"))?;
        let fixtures = slot
            .as_ref()
            .context("provider fixtures were not prepared")?;
        for (fixture, proxy) in [
            (&fixtures.iceberg, &fixtures.iceberg_slow),
            (&fixtures.paimon, &fixtures.paimon_slow),
        ] {
            run_queued_provider(context, fixture, proxy)?;
        }
        Ok(())
    }
    fn teardown(&self) -> Result<()> {
        self.setup.teardown()
    }
}

fn run_queued_provider(
    context: &mut ScenarioContext,
    fixture: &CheckedFixture,
    proxy: &DelayedS3Proxy,
) -> Result<()> {
    let baseline = resource_baseline(context)?;
    let mut connection =
        SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
    connection
        .execute(fixture.catalog_sql(true, proxy.endpoint()))
        .map(|_| ())?;
    connection
        .execute(fixture.catalog_sql(false, &fixture.facts.s3_endpoint))
        .map(|_| ())?;
    drop(connection);
    let hold = proxy.hold_next_read_with_suffix(".parquet")?;
    let mut actors = Vec::new();
    let mut completed = [false; 2];
    let mut entered = false;
    let operation = (|| -> Result<()> {
        for index in 0..2 {
            let facts = fixture.clone();
            let table = facts.table_name(index == 0);
            actors.push(ProviderActor::spawn(
                context,
                &format!("{}-queued-{index}", fixture.provider),
                move |connection, statements| {
                    verify_provider_result(
                        statements.run(
                            connection,
                            format!("SELECT id, value FROM {table} ORDER BY id"),
                        )??,
                        &facts,
                    )
                },
            )?);
            actors[index].wait_started(context.remaining("await queued provider connection")?)?;
            if index == 0 {
                hold.wait_until_entered(
                    context
                        .remaining("observe Q1 provider read hold")?
                        .min(Duration::from_secs(30)),
                )?;
                entered = true;
            }
        }
        let deadline = Instant::now()
            + context
                .remaining("observe provider admission queue")?
                .min(Duration::from_secs(30));
        loop {
            let state = frontend_workload_state(context)?;
            let admitted = state
                .pointer("/workload/governance/admitted_queries")
                .and_then(serde_json::Value::as_u64);
            let waiting = state
                .pointer("/workload/governance/waiting_records")
                .and_then(serde_json::Value::as_u64);
            if admitted == Some(1) && waiting.is_some_and(|n| n >= 1) {
                fs::write(
                    context
                        .scenario_root()
                        .join(format!("{}-queued-state.json", fixture.provider)),
                    serde_json::to_vec_pretty(&state)?,
                )?;
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "{} Q1 hold did not establish admitted=1 and queued Q2: {state}",
                fixture.provider
            );
            for (index, actor) in actors.iter_mut().enumerate() {
                match actor.try_result() {
                    Ok(Some(())) => {
                        completed[index] = true;
                        anyhow::bail!(
                            "provider actor {index} completed before Q1 hold and Q2 queue"
                        );
                    }
                    Ok(None) => {}
                    Err(error) => {
                        completed[index] = true;
                        return Err(error);
                    }
                }
            }
            thread::sleep(Duration::from_millis(25));
        }
        context.action(format!(
            "{} Q1 provider read held with admitted=1 and Q2 waiting",
            fixture.provider
        ));
        hold.release();
        hold.wait_until_forwarded(
            context
                .remaining("forward held provider read")?
                .min(Duration::from_secs(30)),
        )?;
        while completed.iter().any(|done| !done) {
            for (index, actor) in actors.iter_mut().enumerate() {
                if !completed[index] {
                    match actor.try_result() {
                        Ok(Some(())) => completed[index] = true,
                        Ok(None) => {}
                        Err(error) => {
                            completed[index] = true;
                            return Err(error);
                        }
                    }
                }
            }
            context.remaining("await queued provider results")?;
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    })();
    let mut cleanup = Vec::new();
    if operation.is_err() {
        for actor in &mut actors {
            collect_actor_cleanup(&mut cleanup, "cancel actor", actor.cancel(context));
        }
    }
    hold.release();
    if entered {
        collect_actor_cleanup(
            &mut cleanup,
            "forward held request",
            hold.wait_until_forwarded(Duration::from_secs(30)),
        );
    }
    for (index, actor) in actors.iter_mut().enumerate() {
        collect_actor_cleanup(
            &mut cleanup,
            "actual join and original result",
            actor.finish_with(context, |()| {
                completed[index] = true;
                Ok(())
            }),
        );
    }
    collect_actor_cleanup(
        &mut cleanup,
        "FE exit",
        await_frontend_local_exit(context, "queued provider reads"),
    );
    collect_actor_cleanup(
        &mut cleanup,
        "BE exit",
        await_resource_convergence(context, &baseline, "queued provider reads"),
    );
    finish_held_evidence(
        context,
        &format!("{}-queued", fixture.provider),
        operation,
        cleanup,
    )?;
    context.action(format!(
        "{} queued actors each verified their independent full rows and owners exited",
        fixture.provider
    ));
    Ok(())
}
