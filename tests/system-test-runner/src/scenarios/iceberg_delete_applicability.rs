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

//! Native replay of independently published Iceberg delete fixtures.

use super::connector::{
    await_resource_convergence, connector_reader_environment, require_three_backends,
    resource_baseline,
};
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::delayed_s3::{DelayedS3Config, DelayedS3Proxy};
use novarocks_cluster_harness::{CrossProcessConfigOverlay, ServerHandle};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

const MANIFEST_ENV: &str = "NOVAROCKS_UEA4G_NATIVE_MANIFEST";
const ACCESS_ENV: &str = "NOVAROCKS_UEA4G_FIXTURE_ACCESS";
const SECRET_ENV: &str = "NOVAROCKS_UEA4G_FIXTURE_SECRET";
pub(super) const CATALOG: &str = "uea4g_native";
const CACHE: &str = "[runtime.cache]\npage_cache_enable = false\nparquet_page_cache_enable = false\ndatacache_enable = false\n";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    pub(super) version: u32,
    pub(super) corpus: PathBuf,
    pub(super) corpus_sha256: String,
    pub(super) anomaly: PathBuf,
    pub(super) anomaly_sha256: String,
    pub(super) scale: PathBuf,
    pub(super) scale_sha256: String,
    pub(super) scale_artifacts_sha256: String,
    pub(super) scale_java_plans_sha256: String,
    pub(super) scale_java_delete_members_sha256: String,
    pub(super) scale_cases: Vec<String>,
    pub(super) rest_uri: String,
    pub(super) warehouse: String,
    pub(super) s3_endpoint: String,
}

pub(super) struct Fixture {
    pub(super) manifest: Manifest,
    pub(super) manifest_sha256: String,
    pub(super) corpus: Value,
    pub(super) anomaly: Value,
    pub(super) scale: Value,
    pub(super) corpus_path: PathBuf,
    pub(super) anomaly_path: PathBuf,
    pub(super) scale_path: PathBuf,
    pub(super) scale_artifacts: Vec<Value>,
    pub(super) proxy: DelayedS3Proxy,
}

#[derive(Default)]
pub(super) struct IcebergDeleteApplicability {
    pub(super) fixture: Mutex<Option<Fixture>>,
}

fn checked_json(path: &Path, expected: &str) -> Result<Value> {
    let bytes =
        fs::read(path).with_context(|| format!("read immutable fixture {}", path.display()))?;
    ensure!(
        format!("{:x}", Sha256::digest(&bytes)) == expected,
        "immutable fixture checksum changed: {}",
        path.display()
    );
    Ok(serde_json::from_slice(&bytes)?)
}
fn escaped(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
pub(super) fn table_name(raw: &str) -> Result<String> {
    let parts: Vec<_> = raw.split('.').collect();
    ensure!(parts.len() == 3 && parts.iter().all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')), "fixture table must contain three exact identifiers");
    Ok(format!("{CATALOG}.{}.{}", parts[1], parts[2]))
}
pub(super) fn credential_overlay_with_cache(enabled: bool) -> CrossProcessConfigOverlay {
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
pub(super) fn create_catalog_sql(fixture: &Fixture) -> String {
    format!(
        r#"CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(
"type"="iceberg", "iceberg.catalog.type"="rest", "uri"="{}", "warehouse"="{}",
"aws.s3.endpoint"="{}", "aws.s3.region"="us-east-1", "aws.s3.enable_path_style_access"="true",
"credential.object-store-metadata.consumer-role"="frontend", "credential.object-store-metadata.mode"="static", "credential.object-store-metadata.name"="uea4g-fixture", "credential.object-store-metadata.generation"="v1",
"credential.object-store-data.consumer-role"="backend", "credential.object-store-data.mode"="static", "credential.object-store-data.name"="uea4g-fixture", "credential.object-store-data.generation"="v1")"#,
        escaped(&fixture.manifest.rest_uri),
        escaped(&fixture.manifest.warehouse),
        escaped(fixture.proxy.endpoint())
    )
}

fn row_bag(case: &Value) -> Result<Vec<(Option<i64>, Option<i64>, Option<String>)>> {
    Ok(serde_json::from_value(
        case["independent_expected_rows"].clone(),
    )?)
}

pub(super) async fn immutable_artifacts(
    fixture: &Fixture,
) -> Result<novarocks_connector_iceberg::iceberg::io::FileIO> {
    use novarocks_connector_iceberg::opendal::{Operator, services::S3};
    let access = env::var("AWS_S3_ACCESS_KEY_ID").context("missing fixture access credential")?;
    let secret =
        env::var("AWS_S3_SECRET_ACCESS_KEY").context("missing fixture secret credential")?;
    let io = novarocks_connector_iceberg::iceberg::io::FileIO::new_with_memory();
    let mut operators = std::collections::BTreeMap::new();
    let mut verified = std::collections::BTreeMap::new();
    for artifact in fixture.corpus["artifacts"]
        .as_array()
        .context("corpus inventory absent")?
        .iter()
        .chain(
            fixture.anomaly["artifacts"]
                .as_array()
                .context("anomaly inventory absent")?,
        )
        .chain(&fixture.scale_artifacts)
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
        let (bucket, key) = path
            .strip_prefix("s3://")
            .context("artifact must be S3")?
            .split_once('/')
            .context("artifact lacks bucket/key")?;
        if !operators.contains_key(bucket) {
            let service = S3::default()
                .bucket(bucket)
                .endpoint(&fixture.manifest.s3_endpoint)
                .region("us-east-1")
                .access_key_id(&access)
                .secret_access_key(&secret)
                .disable_config_load();
            operators.insert(bucket.to_owned(), Operator::new(service)?.finish());
        }
        let bytes = operators[bucket].read(key).await?.to_bytes();
        ensure!(
            bytes.len() as u64 == size && format!("{:x}", Sha256::digest(&bytes)) == hash,
            "immutable artifact bytes changed: {path}"
        );
        io.new_output(path)?.write(bytes).await?;
    }
    Ok(io)
}

impl Scenario for IcebergDeleteApplicability {
    fn name(&self) -> &'static str {
        "connector/iceberg-delete-applicability"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let path = PathBuf::from(
            env::var(MANIFEST_ENV)
                .context("delete scenario requires its immutable native manifest")?,
        );
        let bytes = fs::read(&path)?;
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        ensure!(
            manifest.version == 1,
            "unsupported native fixture manifest version"
        );
        ensure!(
            manifest.rest_uri.starts_with("http://127.0.0.1:")
                || manifest.rest_uri.starts_with("http://localhost:"),
            "fixture REST must be loopback"
        );
        ensure!(
            manifest.s3_endpoint.starts_with("http://127.0.0.1:")
                || manifest.s3_endpoint.starts_with("http://localhost:"),
            "fixture S3 must be loopback"
        );
        let base = path.parent().context("manifest has no parent")?;
        let corpus = checked_json(&base.join(&manifest.corpus), &manifest.corpus_sha256)?;
        let anomaly = checked_json(&base.join(&manifest.anomaly), &manifest.anomaly_sha256)?;
        let scale = checked_json(&base.join(&manifest.scale), &manifest.scale_sha256)?;
        let scale_dir = base.join(&manifest.scale);
        let scale_dir = scale_dir.parent().context("scale receipt has no parent")?;
        for (name, expected) in [
            ("java-plans.jsonl", &manifest.scale_java_plans_sha256),
            (
                "java-delete-members.jsonl",
                &manifest.scale_java_delete_members_sha256,
            ),
        ] {
            ensure!(
                format!("{:x}", Sha256::digest(fs::read(scale_dir.join(name))?)) == *expected,
                "immutable Java sidecar checksum changed: {name}"
            );
        }
        let corpus_cases = corpus["cases"].as_array().context("missing corpus cases")?;
        ensure!(
            corpus_cases.len() == 23
                && corpus_cases
                    .iter()
                    .all(|c| c["independent_oracle_matched"] == true
                        && c["java_row_read"]["status"] == "success"),
            "corpus must publish all 23 independently verified cases"
        );
        ensure!(
            !manifest.scale_cases.is_empty()
                && manifest.scale_cases.iter().collect::<BTreeSet<_>>().len()
                    == manifest.scale_cases.len(),
            "scale selectors must be nonempty and unique"
        );
        let scale_cases = scale["cases"].as_array().context("missing scale cases")?;
        for name in &manifest.scale_cases {
            let case = scale_cases
                .iter()
                .find(|c| c["case"] == name.as_str())
                .context("missing selected scale case")?;
            ensure!(
                case["exact_bag_checked"] == true
                    && case["java_oracle"] == case["independent_oracle"]
                    && case["plan_file_count"].as_u64().is_some_and(|n| n >= 100),
                "native distribution requires verified scale input with at least 100 real files"
            );
        }
        let proxy = DelayedS3Proxy::start(DelayedS3Config {
            downstream: manifest.s3_endpoint.clone(),
            delay: Duration::ZERO,
        })?;
        // Publish opaque labels, never credential-bearing request headers.
        for (i, artifact) in corpus["artifacts"]
            .as_array()
            .context("missing corpus artifacts")?
            .iter()
            .enumerate()
        {
            let raw = artifact["path"].as_str().context("artifact path absent")?;
            let key = raw
                .strip_prefix("s3://")
                .context("fixture object must be S3")?;
            proxy.label_object(&format!("/{key}"), &format!("corpus_{i}"))?;
        }
        let scale_inventory = fs::read(
            base.join(&manifest.scale)
                .parent()
                .unwrap()
                .join("artifacts.jsonl"),
        )?;
        ensure!(
            format!("{:x}", Sha256::digest(&scale_inventory)) == manifest.scale_artifacts_sha256,
            "scale artifact inventory checksum changed"
        );
        let scale_objects = std::str::from_utf8(&scale_inventory)?;
        let mut scale_artifacts = Vec::new();
        for (i, line) in scale_objects.lines().enumerate() {
            let artifact: Value = serde_json::from_str(line)?;
            let raw = artifact["path"]
                .as_str()
                .context("scale artifact path absent")?;
            proxy.label_object(
                &format!(
                    "/{}",
                    raw.strip_prefix("s3://")
                        .context("scale object must be S3")?
                ),
                &format!("scale_{i}"),
            )?;
            scale_artifacts.push(artifact);
        }
        let mut child = connector_reader_environment();
        let access =
            env::var("AWS_S3_ACCESS_KEY_ID").context("fixture requires S3 access identity")?;
        let secret =
            env::var("AWS_S3_SECRET_ACCESS_KEY").context("fixture requires S3 secret material")?;
        ensure!(
            !access.is_empty() && !secret.is_empty(),
            "fixture credentials must be present"
        );
        for role in [&mut child.fe, &mut child.be] {
            role.insert(ACCESS_ENV.to_owned(), access.clone());
            role.insert(SECRET_ENV.to_owned(), secret.clone());
        }
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("fixture lock poisoned"))?;
        ensure!(slot.is_none(), "fixture launched more than once");
        *slot = Some(Fixture {
            corpus_path: base.join(&manifest.corpus),
            anomaly_path: base.join(&manifest.anomaly),
            scale_path: base.join(&manifest.scale),
            manifest,
            manifest_sha256: format!("{:x}", Sha256::digest(bytes)),
            corpus,
            anomaly,
            scale,
            scale_artifacts,
            proxy,
        });
        Ok(ScenarioLaunchConfig {
            child_environment: child,
            config_overlay: credential_overlay_with_cache(false),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("fixture lock poisoned"))?;
        let fixture = slot.as_ref().context("missing prepared fixture")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (closure_summary, anomaly_closure_summary, scale_closure_summary) =
            runtime.block_on(async {
                let file_io = immutable_artifacts(fixture).await?;
                let corpus = super::iceberg_delete_oracle::verify_receipt(
                    &fixture.corpus_path,
                    file_io.clone(),
                    &context.scenario_root().join("closure-corpus"),
                )
                .await?;
                let anomaly = super::iceberg_delete_oracle::verify_receipt(
                    &fixture.anomaly_path,
                    file_io.clone(),
                    &context.scenario_root().join("closure-anomaly"),
                )
                .await?;
                let scale = super::iceberg_delete_oracle::verify_scale_receipt(
                    &fixture.scale_path,
                    file_io,
                    &context.scenario_root().join("closure-scale"),
                )
                .await?;
                Ok::<_, anyhow::Error>((corpus, anomaly, scale))
            })?;
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect delete oracle session")?,
        )?;
        connection.query_drop(create_catalog_sql(fixture))?;
        connection.query_drop("SET enable_scan_datacache = false")?;
        let mut observations = Vec::new();
        for case in fixture.corpus["cases"].as_array().unwrap() {
            let table = table_name(case["table"].as_str().context("case table missing")?)?;
            let snapshot = case["snapshot"].as_i64().context("snapshot missing")?;
            let query = format!("SELECT id,p,value FROM {table} FOR VERSION AS OF {snapshot}");
            let mut actual: Vec<(Option<i64>, Option<i64>, Option<String>)> =
                connection.query(&query)?;
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
        let before_logs = (0..3)
            .map(|i| context.handle().be_current_log_contents(i))
            .collect::<Result<Vec<_>>>()?;
        for name in &fixture.manifest.scale_cases {
            let case = fixture.scale["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["case"] == name.as_str())
                .unwrap();
            let table = table_name(case["table"].as_str().context("scale table missing")?)?;
            let snapshot = case["snapshot"]
                .as_i64()
                .context("scale snapshot missing")?;
            let query = format!(
                "SELECT row_id,eq_key,partition_key,payload FROM {table} FOR VERSION AS OF {snapshot} ORDER BY row_id"
            );
            let mut hash = Sha256::new();
            let mut count = 0_i64;
            let mut sums = [0_i64; 4];
            let mut last = None;
            for row in connection.query_iter(&query)? {
                let row: (i64, i64, i64, i64) =
                    mysql::from_row_opt(row?).context("scale row is not four INT64 values")?;
                ensure!(
                    last.is_none_or(|old| old < row.0),
                    "scale rows repeat or violate row_id order"
                );
                last = Some(row.0);
                for (i, value) in [row.0, row.1, row.2, row.3].into_iter().enumerate() {
                    hash.update(value.to_be_bytes());
                    sums[i] = sums[i].checked_add(value).context("scale sum overflow")?;
                }
                count += 1;
            }
            let observed = json!({"row_count":count,"sum_row_id":sums[0],"sum_eq_key":sums[1],"sum_partition_key":sums[2],"sum_payload":sums[3],"sorted_row_sha256":format!("{:x}",hash.finalize())});
            ensure!(
                observed == case["independent_oracle"],
                "native exact scale bag differs for {name}: {observed}"
            );
            observations.push(json!({"case":name,"snapshot":snapshot,"metadata":case["metadata"],"exact_bag":observed}));
        }
        await_resource_convergence(context, &baseline, "native delete applicability reads")?;
        let mut placements = Vec::new();
        for (index, before) in before_logs.iter().enumerate() {
            let log = context.handle().be_current_log_contents(index)?;
            let added = log
                .get(before.len()..)
                .context("BE log truncated during exact placement observation")?;
            let accepted = added
                .matches("NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED")
                .count();
            let opened = added
                .matches("NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN")
                .count();
            let closed = added
                .matches("NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE")
                .count();
            ensure!(
                accepted > 0
                    && opened > 0
                    && opened == closed
                    && added.contains("NOVAROCKS_TASK_SPLIT_NO_MORE"),
                "BE[{index}] has no complete distributed read evidence: accepted={accepted}, open={opened}, close={closed}"
            );
            placements
                .push(json!({"backend":index,"accepted":accepted,"opened":opened,"closed":closed}));
        }
        let events = fixture.proxy.take_event_log()?.into_iter().map(|e| json!({"kind":format!("{:?}",e.kind),"request_id":e.request_id,"connection_id":e.connection_id,"elapsed_ms":e.elapsed_millis,"method":e.method.as_str(),"object_id":e.object_id,"range":e.range,"bytes":e.bytes})).collect::<Vec<_>>();
        ensure!(
            fixture.proxy.snapshot().event_overflow == 0,
            "proxy object evidence overflowed"
        );
        fs::write(
            context.scenario_root().join("delete-oracle.json"),
            serde_json::to_vec_pretty(
                &json!({"manifest_sha256":fixture.manifest_sha256,"corpus_sha256":fixture.manifest.corpus_sha256,"scale_sha256":fixture.manifest.scale_sha256,"scale_artifacts_sha256":fixture.manifest.scale_artifacts_sha256,"closure_oracle":closure_summary,"anomaly_closure_oracle":anomaly_closure_summary,"scale_closure_oracle":scale_closure_summary,"observations":observations,"placements":placements,"resource_converged":true,"object_reads":events}),
            )?,
        )?;
        connection.query_drop(format!("DROP CATALOG {CATALOG}"))?;
        context.action("verified 23 immutable delete row bags and selected scale bags on all three BEs, with object-range and resource-exit evidence");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("fixture lock poisoned"))?
            .take();
        Ok(())
    }
}
