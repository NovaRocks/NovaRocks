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

//! Warm, concurrent reads of independently generated delete families and
//! snapshot-specific visibility across a private DELETE publication.

use super::connector::{await_resource_convergence, require_three_backends, resource_baseline};
use super::iceberg_delete_applicability::{
    CATALOG, create_catalog_sql_from_parts, credential_overlay_with_cache, table_name,
};
use super::provider_mysql_actors::{ActorStatements, ProviderCheck, run_checks};
use crate::actors::sql::{SqlSession, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::CrossProcessChildEnvironment;
use novarocks_cluster_harness::fixture_run::{RunBudget, RunStorageScope};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const CORPUS_ENV: &str = "NOVAROCKS_ICEBERG_DELETE_CORPUS_RECEIPT";
const ACCESS_ENV: &str = "NOVAROCKS_UEA4G_FIXTURE_ACCESS";
const SECRET_ENV: &str = "NOVAROCKS_UEA4G_FIXTURE_SECRET";
const REPRESENTATIVE_CASES: [&str; 6] = [
    "same_commit_dv_equality",
    "same_commit_position_equality",
    "multi_target_position",
    "multi_blob_dv",
    "equality_null",
    "equality_promoted_int",
];
type RowBag = Vec<(Option<i64>, Option<i64>, Option<String>)>;

#[derive(Deserialize)]
struct CorpusReceipt {
    mode: String,
    runtime: CorpusRuntime,
    cases: Vec<CorpusCase>,
}
#[derive(Deserialize)]
struct CorpusRuntime {
    iceberg: String,
    requested_warehouse: String,
}
#[derive(Clone, Deserialize)]
struct CorpusCase {
    case: String,
    table: String,
    snapshot: i64,
    independent_expected_rows: RowBag,
    independent_oracle_matched: bool,
    java_row_read: JavaRowRead,
}
#[derive(Clone, Deserialize)]
struct JavaRowRead {
    status: String,
}
struct WarmFixture {
    receipt_path: PathBuf,
    receipt_sha256: String,
    cases: Vec<CorpusCase>,
    rest_uri: String,
    warehouse: String,
    s3_endpoint: String,
    run_budget: Option<RunBudget>,
    machine: Option<MachineBinding>,
}

struct MachineBinding {
    root: String,
    daemon: String,
    version: String,
    generation: String,
    storage: RunStorageScope,
    owns_run: bool,
}

impl MachineBinding {
    fn from_environment(budget: &RunBudget) -> Result<Self> {
        let read = |name: &str| -> Result<String> {
            let value = env::var(name)
                .with_context(|| format!("warm scratch writes require original {name}"))?;
            ensure!(!value.is_empty(), "warm scratch binding omitted {name}");
            Ok(value)
        };
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(read("NOVA_ENV_MANIFEST")?)?)?;
        let machine = &manifest["runtime"]["machine"];
        let binding = Self {
            root: read("NOVA_FIXTURE_MACHINE_ROOT")?,
            daemon: read("NOVA_FIXTURE_MACHINE_DAEMON")?,
            version: read("NOVA_FIXTURE_MACHINE_VERSION")?,
            generation: read("NOVA_FIXTURE_MACHINE_GENERATION")?,
            storage: RunStorageScope::from_environment(budget.identity(), machine)?,
            owns_run: env::var(novarocks_cluster_harness::fixture_run::RUN_CONTEXT_ENV)
                .map_or(true, |value| value == "null"),
        };
        budget.validate_machine(machine)?;
        ensure!(
            machine["control_root"] == binding.root
                && machine["daemon_id"] == binding.daemon
                && machine["version"] == binding.version
                && machine["generation"] == binding.generation,
            "warm scratch binding differs from its immutable publication"
        );
        Ok(binding)
    }
}

impl WarmFixture {
    fn load() -> Result<Self> {
        let receipt_path = PathBuf::from(
            env::var(CORPUS_ENV)
                .context("warm delete reads require the independently prepared corpus receipt")?,
        );
        let bytes = fs::read(&receipt_path)?;
        let receipt: CorpusReceipt = serde_json::from_slice(&bytes)?;
        ensure!(
            receipt.mode == "corpus" && receipt.runtime.iceberg.contains("1.11.0"),
            "warm delete reads require the official Java Iceberg corpus producer"
        );
        let mut cases = Vec::new();
        for name in REPRESENTATIVE_CASES {
            let selected = receipt
                .cases
                .iter()
                .filter(|case| case.case == name)
                .collect::<Vec<_>>();
            ensure!(
                selected.len() == 1,
                "corpus must contain one independently verified {name} case"
            );
            let case = selected[0];
            ensure!(
                case.independent_oracle_matched && case.java_row_read.status == "success",
                "corpus case {name} has no independently verified Java row bag"
            );
            table_name(&case.table)?;
            cases.push(case.clone());
        }
        let rest_uri = env::var("NOVAROCKS_ICEBERG_REST_URI")?;
        let s3_endpoint = env::var("AWS_S3_ENDPOINT")?;
        for endpoint in [&rest_uri, &s3_endpoint] {
            ensure!(
                endpoint.starts_with("http://127.0.0.1:")
                    || endpoint.starts_with("http://localhost:"),
                "warm delete fixture endpoint must be loopback"
            );
        }
        ensure!(
            receipt.runtime.requested_warehouse == env::var("NOVAROCKS_ICEBERG_REST_WAREHOUSE")?,
            "corpus warehouse differs from the explicitly bound fixture publication"
        );
        Ok(Self {
            receipt_path,
            receipt_sha256: format!("{:x}", Sha256::digest(bytes)),
            cases,
            rest_uri,
            warehouse: receipt.runtime.requested_warehouse,
            s3_endpoint,
            run_budget: None,
            machine: None,
        })
    }
}

#[derive(Default)]
struct IcebergDeleteWarmConcurrent {
    fixture: Mutex<Option<WarmFixture>>,
}
impl Scenario for IcebergDeleteWarmConcurrent {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "iceberg_delete_warm",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/iceberg-delete-warm-concurrent"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn launch_config(&self, _root: &Path) -> Result<ScenarioLaunchConfig> {
        let fixture = WarmFixture::load()?;
        let access = env::var("AWS_S3_ACCESS_KEY_ID")?;
        let secret = env::var("AWS_S3_SECRET_ACCESS_KEY")?;
        ensure!(
            !access.is_empty() && !secret.is_empty(),
            "delete fixture credentials must be nonempty"
        );
        let mut child = CrossProcessChildEnvironment::default();
        for role in [&mut child.fe, &mut child.be] {
            role.insert(ACCESS_ENV.to_owned(), access.clone());
            role.insert(SECRET_ENV.to_owned(), secret.clone());
        }
        *self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("warm delete fixture lock poisoned"))? = Some(fixture);
        Ok(ScenarioLaunchConfig {
            child_environment: child,
            config_overlay: credential_overlay_with_cache(true),
            ..Default::default()
        })
    }
    fn launch_config_with_fixture_run(
        &self,
        root: &Path,
        budget: &RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        budget.check_work()?;
        let machine = MachineBinding::from_environment(budget)?;
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()?;
        let publication = PathBuf::from(env::var("NOVA_ENV_REST_ENV_FILE")?).canonicalize()?;
        novarocks_cluster_harness::fixture_run::startup_sweep(&repo, &publication, budget)?;
        let config = self.launch_config(root)?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("warm delete fixture lock poisoned"))?;
        let fixture = slot
            .as_mut()
            .context("warm delete fixture was not prepared")?;
        fixture.run_budget = Some(budget.clone());
        fixture.machine = Some(machine);
        Ok(config)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("warm delete fixture lock poisoned"))?;
        let fixture = slot
            .as_ref()
            .context("warm delete fixture was not prepared")?;
        let baseline = resource_baseline(context)?;
        let mut control =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        control
            .execute("SET enable_scan_datacache = true")
            .map(|_| ())?;
        control
            .execute(create_catalog_sql_from_parts(
                &fixture.rest_uri,
                &fixture.warehouse,
                &fixture.s3_endpoint,
            ))
            .map(|_| ())?;
        let mut observations = Vec::new();
        for case in &fixture.cases {
            let table = table_name(&case.table)?;
            let sql = format!(
                "SELECT id, p, value FROM {table} FOR VERSION AS OF {}",
                case.snapshot
            );
            verify_row_bag(&mut control, &sql, &case.independent_expected_rows)?;
            let actors = (0..4)
                .map(|_| (sql.clone(), case.independent_expected_rows.clone()))
                .collect::<Vec<_>>();
            concurrent_row_bags(context, actors, &format!("delete-warm-{}", case.case))?;
            observations.push(
                json!({"case":case.case,"snapshot":case.snapshot,"warm_verified":true,"actors":4}),
            );
        }
        let budget = fixture
            .run_budget
            .as_ref()
            .context("warm delete writes require the original fixture budget")?;
        let machine = fixture
            .machine
            .as_ref()
            .context("warm delete omitted original machine binding")?;
        budget.check_work()?;
        let namespace = budget.identity().resource_name("warm")?;
        let storage_projection = machine.storage.to_json()?;
        let table = format!("{CATALOG}.{namespace}.visible_rows");
        let mut registration = novarocks_cluster_harness::resource_registration::register(
            context.scenario_root(),
            "warm-delete-catalog",
            &namespace,
            &storage_projection,
        )?;
        control
            .execute(format!("CREATE DATABASE {CATALOG}.{namespace}"))
            .map(|_| ())?;
        let mut location_verified = false;
        let private_operation = (|| -> Result<()> {
            control.execute(format!("CREATE TABLE {table} (id INT, p INT, value STRING) TBLPROPERTIES ('format-version'='3', 'write.row-lineage'='true')")).map(|_| ())?;
            verify_scratch_table(
                &fixture.rest_uri,
                &namespace,
                "visible_rows",
                &machine.storage,
                budget,
            )?;
            location_verified = true;
            control
                .execute(format!(
                    "INSERT INTO {table} VALUES (1,1,'one'),(2,1,'two'),(3,1,'three')"
                ))
                .map(|_| ())?;
            control
                .execute(format!("ALTER TABLE {table} CREATE TAG before_delete"))
                .map(|_| ())?;
            let old = format!("SELECT id,p,value FROM {table} FOR VERSION AS OF 'before_delete'");
            let latest = format!("SELECT id,p,value FROM {table}");
            let seed: RowBag = [(1, "one"), (2, "two"), (3, "three")]
                .into_iter()
                .map(|(id, value)| (Some(id), Some(1), Some(value.to_owned())))
                .collect();
            let surviving: RowBag = [(1, "one"), (3, "three")]
                .into_iter()
                .map(|(id, value)| (Some(id), Some(1), Some(value.to_owned())))
                .collect();
            verify_row_bag(&mut control, &old, &seed)?;
            verify_row_bag(&mut control, &latest, &seed)?;
            control
                .execute(format!("DELETE FROM {table} WHERE id = 2"))
                .map(|_| ())?;
            concurrent_row_bags(
                context,
                vec![
                    (old.clone(), seed.clone()),
                    (latest.clone(), surviving.clone()),
                    (old, seed),
                    (latest, surviving),
                ],
                "delete-snapshot-visibility",
            )?;
            context.action("pinned and warmed old snapshot before DELETE; concurrent old/latest actors independently checked their snapshot-specific row bags");
            Ok(())
        })();
        drop(control);
        let exit = await_resource_convergence(
            context,
            &baseline,
            "warm delete and snapshot visibility readers",
        );
        let cleanup = (|| -> Result<()> {
            ensure_cleanup_ready(&private_operation, location_verified, &exit)?;
            // The run-wide owner removes both catalogs before any recursive
            // object deletion, including native JDBC warehouse directories.
            if machine.owns_run {
                cleanup_scratch_run(budget, machine, context.scenario_root())
            } else {
                cleanup_warm_namespace(&fixture.rest_uri, &namespace, budget)
            }
        })();
        let cleanup_registration = registration.settle(&cleanup);
        fs::write(
            context.scenario_root().join("delete-warm-concurrent.json"),
            serde_json::to_vec_pretty(&json!({
                "corpus_receipt":fixture.receipt_path,"corpus_sha256":fixture.receipt_sha256,
                "observations":observations,"private_operation":private_operation.as_ref().err().map(ToString::to_string),
                "private_cleanup":cleanup.as_ref().err().map(ToString::to_string),
                "objects_reclaimed":machine.owns_run && cleanup.is_ok(),
                "cleanup_owner":if machine.owns_run {"scenario-run"} else {"original-outer-run"},
                "resource_exit":exit.as_ref().err().map(ToString::to_string),
            }))?,
        )?;
        private_operation?;
        cleanup?;
        cleanup_registration?;
        exit?;
        context.action("warm concurrent equality, position and DV reads matched independent producer rows and native owners exited");
        Ok(())
    }
    fn teardown(&self) -> Result<()> {
        self.fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("warm delete fixture lock poisoned"))?
            .take();
        Ok(())
    }
}

fn rest_client(budget: &RunBudget) -> Result<reqwest::blocking::Client> {
    budget.check_work()?;
    reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(budget.work_remaining())
        .build()
        .context("build bounded scratch namespace client")
}

fn verify_scratch_table(
    uri: &str,
    namespace: &str,
    table: &str,
    scope: &RunStorageScope,
    budget: &RunBudget,
) -> Result<()> {
    let answer: serde_json::Value = rest_client(budget)?
        .get(format!(
            "{}/v1/namespaces/{namespace}/tables/{table}",
            uri.trim_end_matches('/')
        ))
        .send()?
        .error_for_status()?
        .json()?;
    let location = answer["metadata"]["location"]
        .as_str()
        .context("warm delete table omitted actual location")?;
    verify_table_location(location, scope)
}

fn verify_table_location(location: &str, scope: &RunStorageScope) -> Result<()> {
    ensure!(
        scope.belongs(location)?,
        "warm delete table escaped its original run storage ranges"
    );
    Ok(())
}

fn ensure_cleanup_ready(
    operation: &Result<()>,
    location_verified: bool,
    actor_exit: &Result<()>,
) -> Result<()> {
    ensure!(
        operation.is_ok(),
        "warm delete operation did not confirm synchronous writer completion; files are retained"
    );
    ensure!(
        location_verified,
        "warm delete cleanup retains an unverified table location"
    );
    ensure!(
        actor_exit.is_ok(),
        "warm delete actor/resource convergence was not confirmed; files are retained"
    );
    Ok(())
}

fn cleanup_warm_namespace(uri: &str, namespace: &str, budget: &RunBudget) -> Result<()> {
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(budget.remaining())
        .build()?;
    let base = format!("{}/v1/namespaces/{namespace}", uri.trim_end_matches('/'));
    // Nested execution only releases its catalog entries. Its original outer
    // owner retains all files until the complete run-wide catalog barrier.
    client
        .delete(format!("{base}/tables/visible_rows?purgeRequested=false"))
        .send()?
        .error_for_status()?;
    client.delete(base).send()?.error_for_status()?;
    Ok(())
}

fn cleanup_scratch_run(budget: &RunBudget, machine: &MachineBinding, root: &Path) -> Result<()> {
    ensure!(
        machine.storage.identity() == budget.identity(),
        "warm cleanup storage differs from the original run"
    );
    let request_path = root.join("warm-scratch-cleanup.json");
    let storage: serde_json::Value = serde_json::from_str(&machine.storage.to_json()?)?;
    fs::write(
        &request_path,
        serde_json::to_vec(&json!({
            "identity":budget.identity(), "storage":storage, "users":[]
        }))?,
    )?;
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let mut command = novarocks_test_support::ManagedCommand::new("python3");
    command
        .env_clear()
        .envs(env::vars_os().filter(|(key, _)| {
            matches!(
                key.to_str(),
                Some(
                    "PATH"
                        | "HOME"
                        | "TMPDIR"
                        | "DOCKER_CONTEXT"
                        | "DOCKER_HOST"
                        | "DOCKER_CONFIG"
                        | "DOCKER_TLS_VERIFY"
                        | "DOCKER_CERT_PATH"
                )
            )
        }))
        .arg(repo.join("docker/iceberg-rest/machine_mc.py"));
    for (argument, value) in [
        ("--root", &machine.root),
        ("--daemon", &machine.daemon),
        ("--expected-generation", &machine.generation),
    ] {
        command.arg(argument).arg(value);
    }
    command
        .args(["--cleanup", "--cleanup-run", "--script-file"])
        .arg(&request_path)
        .env(
            novarocks_cluster_harness::fixture_run::RUN_CONTEXT_ENV,
            budget.to_json()?,
        )
        .env(
            novarocks_cluster_harness::fixture_run::RUN_STORAGE_ENV,
            machine.storage.to_json()?,
        );
    let outcome = novarocks_test_support::capture_command_until(
        command,
        fs::File::create(root.join("warm-cleanup.stdout"))?,
        fs::File::create(root.join("warm-cleanup.stderr"))?,
        budget.deadline(),
    )?;
    ensure!(
        !outcome.timed_out && outcome.failures.is_empty() && outcome.output.status.success(),
        "warm scratch cleanup did not confirm actual mc exit; diagnostics={}",
        outcome.diagnostic_root().display()
    );
    outcome.release_diagnostics()?;
    Ok(())
}
fn verify_row_bag(connection: &mut SqlSession, sql: &str, expected: &RowBag) -> Result<()> {
    verify_row_bag_result(connection.execute(sql)?, expected)
}
fn verify_row_bag_result(result: StatementResult, expected: &RowBag) -> Result<()> {
    ensure!(
        result.is_result_set() && result.columns.len() == 3,
        "expected row-bag result set: {result:?}"
    );
    ensure!(
        result.columns[..2].iter().all(|column| matches!(
            column.type_signature.raw_type.as_str(),
            "tinyint" | "smallint" | "integer" | "bigint"
        ) && column.type_signature.arguments.is_empty())
            && result.columns[2].type_signature.raw_type == "varchar",
        "row-bag declared types differ: {result:?}"
    );
    let mut actual: RowBag = result
        .rows
        .iter()
        .map(|row| {
            let integer = |value: &TrinoValue| match value {
                TrinoValue::Null => Ok(None),
                TrinoValue::Integer(value) => Ok(Some(*value)),
                other => anyhow::bail!("expected nullable row-bag integer, got {other:?}"),
            };
            match row.as_slice() {
                [id, partition, value] => Ok((
                    integer(id)?,
                    integer(partition)?,
                    match value {
                        TrinoValue::Null => None,
                        TrinoValue::Text(value) => Some(value.clone()),
                        other => anyhow::bail!("expected nullable row-bag text, got {other:?}"),
                    },
                )),
                other => anyhow::bail!("expected exactly three row-bag cells, got {other:?}"),
            }
        })
        .collect::<Result<_>>()?;
    let mut expected = expected.clone();
    actual.sort();
    expected.sort();
    ensure!(
        actual == expected,
        "delete row bag differs from its independent input: actual={actual:?}, expected={expected:?}"
    );
    Ok(())
}
fn concurrent_row_bags(
    context: &mut ScenarioContext,
    queries: Vec<(String, RowBag)>,
    label: &str,
) -> Result<()> {
    let checks = queries
        .into_iter()
        .map(|(sql, expected)| {
            Box::new(
                move |connection: &mut SqlSession, statements: &ActorStatements| -> Result<()> {
                    statements.run(connection, "SET enable_scan_datacache = true")??;
                    verify_row_bag_result(statements.run(connection, &sql)??, &expected)
                },
            ) as ProviderCheck
        })
        .collect();
    run_checks(context, label, checks)
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::<IcebergDeleteWarmConcurrent>::default()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converged_counters_do_not_authorize_cleanup_after_an_unknown_write() {
        let completed = Ok(());
        let uncertain = Err(anyhow::anyhow!("MySQL connection lost during commit"));
        assert!(ensure_cleanup_ready(&uncertain, true, &completed).is_err());
        assert!(ensure_cleanup_ready(&completed, false, &completed).is_err());
        assert!(ensure_cleanup_ready(&completed, true, &uncertain).is_err());
        assert!(ensure_cleanup_ready(&completed, true, &completed).is_ok());
    }

    #[test]
    fn mutable_table_location_uses_the_same_fixed_run_ranges() {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../docker/iceberg-rest/tests/run_storage_scope_vectors.json"
        ))
        .unwrap();
        let identity = serde_json::from_value(vectors["identity"].clone()).unwrap();
        let scope = RunStorageScope::from_record(&identity, &vectors["record"]).unwrap();
        let native = format!(
            "{}{}/visible_rows",
            vectors["projection"]["ranges"]["rest"]["warehouse"]
                .as_str()
                .unwrap(),
            identity.resource_name("old").unwrap()
        );
        assert!(verify_table_location(&native, &scope).is_ok());
        assert!(
            verify_table_location(
                &format!("{}/warm/visible_rows", identity.scope_root),
                &scope
            )
            .is_ok()
        );
        for location in [
            format!("{}/../benchmark/table", identity.scope_root),
            format!("{}/%2e%2e/table", identity.scope_root),
            format!("{}//table", identity.scope_root),
            "s3://warehouse/benchmarks/table".to_owned(),
        ] {
            assert!(
                verify_table_location(&location, &scope).is_err(),
                "{location}"
            );
        }
    }
}
