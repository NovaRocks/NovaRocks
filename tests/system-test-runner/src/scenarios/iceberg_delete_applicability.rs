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

//! Native verification of independently generated Iceberg delete fixtures.

use super::connector::{
    await_resource_convergence, connector_reader_environment, require_three_backends,
    resource_baseline,
};
use crate::actors::sql::{SqlSession, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::CrossProcessConfigOverlay;
use novarocks_cluster_harness::delayed_s3::{DelayedS3Config, DelayedS3Proxy};
use novarocks_cluster_harness::fixture_run::{RUN_CONTEXT_ENV, RUN_STORAGE_ENV, RunBudget};
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

const ACCESS_ENV: &str = "NOVAROCKS_UEA4G_FIXTURE_ACCESS";
const SECRET_ENV: &str = "NOVAROCKS_UEA4G_FIXTURE_SECRET";
pub(super) const CATALOG: &str = "uea4g_native";
const CACHE: &str = "[runtime.cache]\npage_cache_enable = false\nparquet_page_cache_enable = false\ndatacache_enable = false\n";

pub(super) struct GeneratedDeleteReceipt {
    pub(super) path: PathBuf,
    pub(super) sha256: String,
    pub(super) value: Value,
}

struct Fixture {
    rest: IsolatedIcebergRestFixture,
    input: Option<PreparedInput>,
}

struct PreparedInput {
    corpus: GeneratedDeleteReceipt,
    anomaly: GeneratedDeleteReceipt,
    proxy: DelayedS3Proxy,
}

#[derive(Default)]
pub(super) struct IcebergDeleteApplicability {
    fixture: Mutex<Option<Fixture>>,
}

/// Invoke the existing Java producer against the exact private publication.
/// On failure, the caller settles the private project before returning.
pub(super) fn produce_delete_receipt(
    rest: &IsolatedIcebergRestFixture,
    root: &Path,
    budget: &RunBudget,
    mode: &str,
) -> Result<GeneratedDeleteReceipt> {
    use novarocks_test_support::{ManagedCommand, capture_command_until};
    budget.check_work()?;
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let program = repo.join("tests/sql/fixtures/iceberg-delete-applicability/run.sh");
    let output = root.join(format!("delete-{mode}-input"));
    let env_file = rest.runtime_env_file()?;
    let namespace = format!("uea4g_{mode}");
    let mut command = ManagedCommand::new("bash");
    command
        .arg(&program)
        .args(["--mode", mode, "--namespace", &namespace, "--output"])
        .arg(&output)
        .env("NOVA_ENV_REST_ENV_FILE", &env_file)
        .env("NOVAROCKS_WORKSPACE_ROOT", rest.workspace_root())
        .env_remove(RUN_CONTEXT_ENV)
        .env_remove(RUN_STORAGE_ENV)
        .env_remove("NOVA_FIXTURE_CLEANUP_MODE")
        .env_remove("NOVAROCKS_SPARK_EXTRA_DEFAULTS");
    fs::write(
        root.join(format!("delete-{mode}-producer-command.json")),
        serde_json::to_vec_pretty(&json!({
            "script": program, "mode": mode, "namespace": namespace, "output": output,
            "private_environment": env_file,
            "workspace": rest.workspace_root(),
        }))?,
    )?;
    let outcome = capture_command_until(
        command,
        fs::File::create(root.join(format!("delete-{mode}-producer.stdout")))?,
        fs::File::create(root.join(format!("delete-{mode}-producer.stderr")))?,
        budget.work_deadline(),
    )?;
    fs::write(
        root.join(format!("delete-{mode}-producer-exit.json")),
        serde_json::to_vec_pretty(&json!({
            "exit": outcome.output.status.code(),
            "timed_out": outcome.timed_out,
            "group_exit_observed": outcome.group_exit_observed(),
            "failures": outcome.failures,
        }))?,
    )?;
    ensure!(
        outcome.output.status.success()
            && !outcome.timed_out
            && outcome.group_exit_observed()
            && outcome.failures.is_empty(),
        "private delete {mode} producer failed; diagnostics={}",
        outcome.diagnostic_root().display(),
    );
    let path = output.join("receipt.json");
    let bytes = fs::read(&path)?;
    let value: Value = serde_json::from_slice(&bytes)?;
    ensure!(
        value["mode"] == mode
            && value
                .pointer("/runtime/requested_warehouse")
                .and_then(Value::as_str)
                == Some(rest.endpoints().rest_warehouse.as_str()),
        "delete receipt is from a different producer or warehouse"
    );
    outcome.release_diagnostics()?;
    Ok(GeneratedDeleteReceipt {
        path,
        sha256: format!("{:x}", Sha256::digest(bytes)),
        value,
    })
}

pub(super) fn delete_launch_config(
    rest: &IsolatedIcebergRestFixture,
    cache: bool,
) -> ScenarioLaunchConfig {
    let identity = rest.static_s3_identity();
    let mut child = connector_reader_environment();
    for role in [&mut child.fe, &mut child.be] {
        role.insert(ACCESS_ENV.to_owned(), identity.access_key_id.clone());
        role.insert(SECRET_ENV.to_owned(), identity.secret_access_key.clone());
    }
    ScenarioLaunchConfig {
        child_environment: child,
        config_overlay: credential_overlay_with_cache(cache),
        ..Default::default()
    }
}

fn escaped(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
pub(super) fn table_name(raw: &str) -> Result<String> {
    let parts: Vec<_> = raw.split('.').collect();
    ensure!(parts.len() == 3 && parts.iter().all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')), "fixture table must contain three exact identifiers");
    Ok(format!("{CATALOG}.{}.{}", parts[1], parts[2]))
}
fn credential_overlay_with_cache(enabled: bool) -> CrossProcessConfigOverlay {
    let cache = if enabled {
        "[runtime.cache]\npage_cache_enable = true\nparquet_page_cache_enable = true\ndatacache_enable = true\n"
    } else {
        CACHE
    };
    let role_config = |purpose| {
        format!(
            "[[connector.credentials]]\npurpose = \"{purpose}\"\nname = \"uea4g-fixture\"\ngeneration = \"v1\"\nkind = \"s3\"\naccess_key_id = \"${{ENV:{ACCESS_ENV}}}\"\naccess_key_secret = \"${{ENV:{SECRET_ENV}}}\"\n"
        ) + cache
    };
    CrossProcessConfigOverlay {
        fe: Some(role_config("object-store-metadata")),
        be: Some(role_config("object-store-data")),
        ..Default::default()
    }
}
pub(super) fn create_catalog_sql_from_parts(
    rest_uri: &str,
    warehouse: &str,
    s3_endpoint: &str,
) -> String {
    format!(
        r#"CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(
"type"="iceberg", "iceberg.catalog.type"="rest", "uri"="{}", "warehouse"="{}",
"aws.s3.endpoint"="{}", "aws.s3.region"="us-east-1", "aws.s3.enable_path_style_access"="true",
"credential.object-store-metadata.consumer-role"="frontend", "credential.object-store-metadata.mode"="static", "credential.object-store-metadata.name"="uea4g-fixture", "credential.object-store-metadata.generation"="v1",
"credential.object-store-data.consumer-role"="backend", "credential.object-store-data.mode"="static", "credential.object-store-data.name"="uea4g-fixture", "credential.object-store-data.generation"="v1")"#,
        escaped(rest_uri),
        escaped(warehouse),
        escaped(s3_endpoint)
    )
}

type ExpectedRow = (Option<i64>, Option<i64>, Option<String>);

fn row_bag(case: &Value) -> Result<Vec<ExpectedRow>> {
    Ok(serde_json::from_value(
        case["independent_expected_rows"].clone(),
    )?)
}

async fn immutable_artifacts(
    rest: &IsolatedIcebergRestFixture,
    input: &PreparedInput,
) -> Result<novarocks_connector_iceberg::iceberg::io::FileIO> {
    use novarocks_fs::{
        FileCancellation, FileIdentity, FileReadRange, FsAccessResolver, ObjectStoreAccessContext,
        ObjectStoreConfig, ObjectStoreProviderPool, ObjectStoreProviderPoolOptions, SecretValue,
    };
    use novarocks_spi::connector::{StaticCredentialReference, StorageAccessDomainId};
    let identity = rest.static_s3_identity();
    let endpoint = &rest.endpoints().minio_endpoint;
    let io = novarocks_connector_iceberg::iceberg::io::FileIO::new_with_memory();
    let provider_pool = ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default())?;
    let config = ObjectStoreConfig {
        endpoint: endpoint.clone(),
        access_key_id: SecretValue::new(identity.access_key_id),
        access_key_secret: SecretValue::new(identity.secret_access_key),
        session_token: None,
        enable_path_style_access: Some(true),
        region: Some("us-east-1".to_owned()),
        retry_max_times: None,
        retry_min_delay_ms: None,
        retry_max_delay_ms: None,
        timeout_ms: None,
        io_timeout_ms: None,
    };
    let credential_reference = StaticCredentialReference::try_new("uea4g-immutable-fixture", "v1")?;
    // Fixture access identity is independent of execution fault-domain admission.
    let domain = StorageAccessDomainId::from_bytes(Sha256::digest(endpoint.as_bytes()).into());
    let mut verified = std::collections::BTreeMap::new();
    for artifact in input.corpus.value["artifacts"]
        .as_array()
        .context("corpus inventory absent")?
        .iter()
        .chain(
            input.anomaly.value["artifacts"]
                .as_array()
                .context("anomaly inventory absent")?,
        )
    {
        let path = artifact["path"].as_str().context("artifact path absent")?;
        let size = artifact["size"].as_u64().context("artifact size absent")?;
        let hash = artifact["sha256"]
            .as_str()
            .context("artifact digest absent")?;
        if let Some(previous) = verified.insert(path.to_owned(), (size, hash.to_owned())) {
            ensure!(
                previous == (size, hash.to_owned()),
                "artifact inventory conflict for {path}"
            );
            continue;
        }
        let (_bucket, _key) = path
            .strip_prefix("s3://")
            .context("artifact must be S3")?
            .split_once('/')
            .context("artifact lacks bucket/key")?;
        let file = FsAccessResolver::new().resolve_location(
            domain,
            path,
            Some(ObjectStoreAccessContext::for_static(
                config.endpoint_config(),
                credential_reference.clone(),
                config.secret_material(),
                &provider_pool,
            )),
        )?;
        let bytes = file
            .bind(0, FileIdentity::new(path, size, None))?
            .read(FileReadRange::WholeFile, &FileCancellation::new())
            .await?;
        ensure!(
            bytes.len() as u64 == size && format!("{:x}", Sha256::digest(&bytes)) == hash,
            "immutable artifact bytes changed: {path}"
        );
        io.new_output(path)?.write(bytes).await?;
    }
    Ok(io)
}

impl Scenario for IcebergDeleteApplicability {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "iceberg_delete_applicability",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/iceberg-delete-applicability"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config_with_fixture_run(
        &self,
        root: &Path,
        budget: &RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("delete fixture lock poisoned"))?;
        ensure!(slot.is_none(), "delete fixture launched more than once");
        let (rest, _) = IsolatedIcebergRestFixture::start_private_with_budget(root, budget, false)?;
        let fixture = slot.insert(Fixture { rest, input: None });
        let rest = &fixture.rest;
        fs::write(
            root.join("delete-fixture-runtime.json"),
            serde_json::to_vec_pretty(&rest.runtime_identity()?)?,
        )?;
        let corpus = produce_delete_receipt(rest, root, budget, "corpus")?;
        let anomaly = produce_delete_receipt(rest, root, budget, "anomalies")?;
        let cases = corpus.value["cases"]
            .as_array()
            .context("missing corpus cases")?;
        ensure!(
            cases.len() == 23
                && cases
                    .iter()
                    .all(|case| case["independent_oracle_matched"] == true
                        && case["java_row_read"]["status"] == "success"),
            "corpus must publish all 23 independently verified cases"
        );
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: rest.endpoints().minio_endpoint.clone(),
            delay: Duration::ZERO,
        })?;
        for (index, artifact) in corpus.value["artifacts"]
            .as_array()
            .context("missing corpus artifacts")?
            .iter()
            .enumerate()
        {
            let path = artifact["path"].as_str().context("artifact path absent")?;
            let key = path
                .strip_prefix("s3://")
                .context("fixture object must be S3")?;
            proxy.label_object(&format!("/{key}"), &format!("corpus_{index}"))?;
        }
        let launch = delete_launch_config(rest, false);
        fixture.input = Some(PreparedInput {
            corpus,
            anomaly,
            proxy,
        });
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("fixture lock poisoned"))?;
        let fixture = slot.as_ref().context("missing delete fixture")?;
        let input = fixture
            .input
            .as_ref()
            .context("missing prepared delete input")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (closure_summary, anomaly_closure_summary) = runtime.block_on(async {
            let file_io = immutable_artifacts(&fixture.rest, input).await?;
            let corpus = super::iceberg_delete_oracle::verify_receipt(
                &input.corpus.path,
                file_io.clone(),
                &context.scenario_root().join("closure-corpus"),
            )
            .await?;
            let anomaly = super::iceberg_delete_oracle::verify_receipt(
                &input.anomaly.path,
                file_io,
                &context.scenario_root().join("closure-anomaly"),
            )
            .await?;
            Ok::<_, anyhow::Error>((corpus, anomaly))
        })?;
        context.remaining("connect delete oracle session")?;
        let mut connection =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let endpoints = fixture.rest.endpoints();
        connection.execute(create_catalog_sql_from_parts(
            &endpoints.rest_uri,
            &endpoints.rest_warehouse,
            input.proxy.endpoint(),
        ))?;
        connection.execute("SET enable_scan_datacache = false")?;
        let mut observations = Vec::new();
        for case in input.corpus.value["cases"]
            .as_array()
            .context("missing corpus cases")?
        {
            let table = table_name(case["table"].as_str().context("case table missing")?)?;
            let snapshot = case["snapshot"].as_i64().context("snapshot missing")?;
            let query = format!("SELECT id,p,value FROM {table} FOR VERSION AS OF {snapshot}");
            let mut actual = read_row_bag(&connection.execute(&query)?)?;
            let mut expected = row_bag(case)?;
            actual.sort();
            expected.sort();
            ensure!(
                actual == expected,
                "native row bag differs from independent oracle for {}: actual={actual:?}, expected={expected:?}",
                case["case"]
            );
            observations.push(json!({"case":case["case"],"snapshot":snapshot,"metadata":case["metadata"],"row_bag":actual}));
        }
        await_resource_convergence(context, &baseline, "native delete applicability reads")?;
        let events = input.proxy.take_event_log()?.into_iter().map(|e| json!({"kind":format!("{:?}",e.kind),"request_id":e.request_id,"connection_id":e.connection_id,"elapsed_ms":e.elapsed_millis,"method":e.method.as_str(),"object_id":e.object_id,"range":e.range,"bytes":e.bytes})).collect::<Vec<_>>();
        ensure!(
            input.proxy.snapshot().event_overflow == 0,
            "proxy object evidence overflowed"
        );
        fs::write(
            context.scenario_root().join("delete-oracle.json"),
            serde_json::to_vec_pretty(&json!({
                "corpus_receipt": input.corpus.path, "corpus_sha256": input.corpus.sha256,
                "anomaly_receipt": input.anomaly.path, "anomaly_sha256": input.anomaly.sha256,
                "closure_oracle": closure_summary, "anomaly_closure_oracle": anomaly_closure_summary,
                "observations": observations, "resource_converged": true, "object_reads": events,
            }))?,
        )?;
        connection.execute(format!("DROP CATALOG {CATALOG}"))?;
        context.action("verified 23 independently generated delete row bags with object-range and resource-exit evidence");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("delete fixture lock poisoned"))?
            .take();
        if let Some(Fixture { mut rest, input }) = fixture {
            drop(input);
            rest.shutdown()?;
        }
        Ok(())
    }
}

fn read_row_bag(result: &StatementResult) -> Result<Vec<ExpectedRow>> {
    ensure!(
        result.is_result_set() && result.columns.len() == 3,
        "delete row bag expected its three columns: {result:?}"
    );
    for column in &result.columns[..2] {
        ensure!(
            matches!(
                column.type_signature.raw_type.as_str(),
                "tinyint" | "smallint" | "integer" | "bigint"
            ) && column.type_signature.arguments.is_empty(),
            "delete row bag expected a declared integer column: {column:?}"
        );
    }
    ensure!(
        result.columns[2].type_signature.raw_type == "varchar",
        "delete row bag expected its text value column: {:?}",
        result.columns[2]
    );
    fn integer(value: &TrinoValue) -> Result<Option<i64>> {
        match value {
            TrinoValue::Null => Ok(None),
            TrinoValue::Integer(n) => Ok(Some(*n)),
            value => anyhow::bail!("delete row bag has a non-integer cell: {value:?}"),
        }
    }
    result
        .rows
        .iter()
        .map(|row| {
            let [id, partition, value] = row.as_slice() else {
                anyhow::bail!("delete row bag has the wrong width: {row:?}");
            };
            let value = match value {
                TrinoValue::Null => None,
                TrinoValue::Text(s) => Some(s.clone()),
                value => anyhow::bail!("delete row bag has a non-text value: {value:?}"),
            };
            Ok((integer(id)?, integer(partition)?, value))
        })
        .collect()
}
pub(super) fn with_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => {
            Err(primary.context(format!("delete oracle cleanup failed: {cleanup:#}")))
        }
    }
}
