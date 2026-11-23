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

//! Create test views through the admitted Java REST fixture and observe the
//! stored version directly, independently of NovaRocks' metadata decoder.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use reqwest::{StatusCode, Url, blocking::Client};
use serde_json::Value;

use crate::namespace_guard::NamespaceGuard;
use crate::types::{
    IcebergViewFixtureAssertion, IcebergViewFixtureDirective, IcebergViewFixtureEndpoint,
    IcebergViewFixtureSample, IcebergViewFixtureTarget,
};

pub(crate) fn sample_path(repo: &Path, sample: IcebergViewFixtureSample) -> PathBuf {
    let filename = match sample {
        IcebergViewFixtureSample::TrinoMv => "trino-mv.create-view.json",
        IcebergViewFixtureSample::SparkMv => "spark-mv.create-view.json",
    };
    repo.join("novarocks/connector/iceberg/src/rest_protocol/tests/fixtures/views")
        .join(filename)
}

#[derive(Debug, Eq, PartialEq)]
struct ViewObservation {
    uuid: String,
    version_id: i64,
    storage_table: Value,
}

impl ViewObservation {
    fn read(response: Value) -> Result<Self> {
        let metadata = &response["metadata"];
        let uuid = metadata["view-uuid"]
            .as_str()
            .context("REST view fixture response omitted view-uuid")?;
        let version_id = metadata["current-version-id"]
            .as_i64()
            .context("REST view fixture response omitted current-version-id")?;
        let mut matching = metadata["versions"]
            .as_array()
            .context("REST view fixture response omitted versions")?
            .iter()
            .filter(|version| version["version-id"].as_i64() == Some(version_id));
        let version = matching
            .next()
            .context("REST view fixture response omitted the current version")?;
        ensure!(
            matching.next().is_none(),
            "REST view fixture response repeated its current version"
        );
        Ok(Self {
            uuid: uuid.into(),
            version_id,
            storage_table: version["storage-table"].clone(),
        })
    }
}

fn request_from_sample(
    sample: &str,
    target: &IcebergViewFixtureTarget,
    storage_table: &str,
) -> Result<Value> {
    let mut request: Value =
        serde_json::from_str(sample).context("decode REST view fixture sample")?;
    for (pointer, original, name) in [
        ("/name", "__VIEW__", target.view.as_str()),
        (
            "/view-version/default-namespace/0",
            "__NAMESPACE__",
            target.namespace.as_str(),
        ),
        (
            "/view-version/storage-table/namespace/0",
            "__NAMESPACE__",
            target.namespace.as_str(),
        ),
        (
            "/view-version/storage-table/name",
            "__STORAGE_TABLE__",
            storage_table,
        ),
    ] {
        let field = request
            .pointer_mut(pointer)
            .with_context(|| format!("REST view fixture sample omitted {pointer}"))?;
        ensure!(
            field.as_str() == Some(original),
            "REST view fixture sample {pointer} must contain {original}"
        );
        *field = Value::String(name.into());
    }
    Ok(request)
}

fn remaining(deadline: Instant) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    ensure!(
        !remaining.is_zero(),
        "original REST view fixture deadline expired"
    );
    Ok(remaining)
}

fn projected_endpoint(endpoint: IcebergViewFixtureEndpoint) -> Result<String> {
    let (env_key, publication_key) = match endpoint {
        IcebergViewFixtureEndpoint::Rest => ("NOVAROCKS_ICEBERG_REST_URI", "iceberg_rest"),
        IcebergViewFixtureEndpoint::RestMv => ("NOVAROCKS_ICEBERG_REST_MV_URI", "iceberg_rest_mv"),
    };
    let uri = std::env::var(env_key)
        .with_context(|| format!("{env_key} is required for @iceberg_view_fixture"))?;
    let env_file = std::env::var_os("NOVA_ENV_REST_ENV_FILE")
        .context("Iceberg view fixture requires the original REST publication")?;
    let manifest = PathBuf::from(env_file).with_file_name("manifest.json");
    let publication: Value = serde_json::from_slice(&std::fs::read(&manifest)?)
        .context("decode Iceberg view fixture endpoint publication")?;
    ensure!(
        publication[publication_key]["uri"]
            .as_str()
            .is_some_and(|published| published.trim_end_matches('/') == uri.trim_end_matches('/')),
        "Iceberg view fixture endpoint differs from its original publication"
    );
    Ok(uri)
}

struct PendingViewFixture {
    client: Client,
    url: Url,
    target: IcebergViewFixtureTarget,
    original: Option<ViewObservation>,
}

impl PendingViewFixture {
    fn qualified_name(&self) -> String {
        let catalog = self
            .target
            .catalog
            .as_deref()
            .expect("bound fixture catalog");
        format!("{catalog}.{}.{}", self.target.namespace, self.target.view)
    }

    fn assert_unchanged(&self, deadline: Instant) -> Result<()> {
        let original = self
            .original
            .as_ref()
            .context("Iceberg view fixture did not finish installation")?;
        let current = ViewObservation::read(
            self.client
                .get(self.url.clone())
                .timeout(remaining(deadline)?)
                .send()
                .context("GET Iceberg view fixture")?
                .error_for_status()?
                .json()?,
        )?;
        ensure!(
            current == *original,
            "Iceberg view fixture changed: expected {original:?}, observed {current:?}"
        );
        Ok(())
    }

    fn delete(&self, deadline: Instant) -> Result<()> {
        let status = self
            .client
            .delete(self.url.clone())
            .timeout(remaining(deadline)?)
            .send()
            .context("DELETE Iceberg view fixture")?
            .status();
        if status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        ensure!(
            status.is_success(),
            "Iceberg view fixture cleanup returned HTTP {status}"
        );
        let status = self
            .client
            .get(self.url.clone())
            .timeout(remaining(deadline)?)
            .send()
            .context("confirm Iceberg view fixture removal")?
            .status();
        ensure!(
            status == StatusCode::NOT_FOUND,
            "Iceberg view fixture absence check returned HTTP {status}"
        );
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct CaseViewFixtures {
    views: Vec<PendingViewFixture>,
}

impl CaseViewFixtures {
    pub(crate) fn install(
        &mut self,
        directive: &IcebergViewFixtureDirective,
        guard: &NamespaceGuard,
        owns_case_namespace: impl FnOnce(&str) -> bool,
        deadline: Instant,
    ) -> Result<String> {
        let endpoint = projected_endpoint(directive.endpoint)?;
        let url = guard.view_fixture_url(&directive.target, &endpoint, owns_case_namespace)?;
        let mut target = directive.target.clone();
        target.catalog = Some(guard.view_fixture_catalog(&target)?.to_owned());
        let mut collection = url.clone();
        collection
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid Iceberg view fixture collection URI"))?
            .pop();
        let path = sample_path(&crate::config::resolve_repo_root()?, directive.sample);
        let sample = std::fs::read_to_string(&path)
            .with_context(|| format!("read Iceberg view fixture sample {}", path.display()))?;
        let request = request_from_sample(
            &sample,
            &target,
            directive.storage_table.as_deref().unwrap_or("mv_storage"),
        )?;
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("build Iceberg view fixture client")?;
        let status = client
            .get(url.clone())
            .timeout(remaining(deadline)?)
            .send()
            .context("check original Iceberg view fixture target")?
            .status();
        ensure!(
            status == StatusCode::NOT_FOUND,
            "Iceberg view fixture requires an unused target, got HTTP {status}"
        );
        // Retain the exact cleanup target before dispatch. A transport or
        // response-decode failure can happen after the Java server created it.
        self.views.push(PendingViewFixture {
            client,
            url,
            target,
            original: None,
        });
        let fixture = self.views.last_mut().expect("just registered fixture");
        let response = fixture
            .client
            .post(collection)
            .json(&request)
            .timeout(remaining(deadline)?)
            .send()
            .context("POST Iceberg view fixture")?;
        if response.status() == StatusCode::CONFLICT {
            self.views.pop();
            bail!("Iceberg view fixture target became occupied before create");
        }
        ensure!(
            response.status().is_success(),
            "Iceberg view fixture creation returned HTTP {}",
            response.status()
        );
        let original = ViewObservation::read(response.json()?)?;
        let expected_storage_table = match directive.endpoint {
            IcebergViewFixtureEndpoint::Rest => &Value::Null,
            IcebergViewFixtureEndpoint::RestMv => &request["view-version"]["storage-table"],
        };
        ensure!(
            original.storage_table == *expected_storage_table,
            "REST view fixture storage-table contract: expected {expected_storage_table}, observed {}",
            original.storage_table
        );
        fixture.original = Some(original);
        Ok(fixture.qualified_name())
    }

    pub(crate) fn assert_unchanged(
        &self,
        assertion: &IcebergViewFixtureAssertion,
        installed_on_step: bool,
        guard: &NamespaceGuard,
        deadline: Instant,
    ) -> Result<String> {
        let fixture = match assertion {
            IcebergViewFixtureAssertion::Named(target) => {
                let catalog = guard.view_fixture_catalog(target)?;
                self.views.iter().find(|fixture| {
                    fixture.target.catalog.as_deref() == Some(catalog)
                        && fixture.target.namespace == target.namespace
                        && fixture.target.view == target.view
                })
            }
            IcebergViewFixtureAssertion::InstalledOnStepOrOnlyFixture
                if installed_on_step || self.views.len() == 1 =>
            {
                self.views.last()
            }
            IcebergViewFixtureAssertion::InstalledOnStepOrOnlyFixture => None,
        }
        .context("@iceberg_view_fixture_unchanged requires one named or unambiguous fixture")?;
        fixture.assert_unchanged(deadline)?;
        Ok(fixture.qualified_name())
    }

    pub(crate) fn cleanup(
        &mut self,
        deadline: Instant,
    ) -> impl Iterator<Item = (String, Result<()>)> + '_ {
        self.views
            .drain(..)
            .rev()
            .map(move |fixture| (fixture.qualified_name(), fixture.delete(deadline)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn loads_and_binds_a_shared_sample_by_its_declared_name() {
        let repo = tempfile::tempdir().unwrap();
        let path = sample_path(repo.path(), IcebergViewFixtureSample::TrinoMv);
        assert!(path.ends_with(
            "novarocks/connector/iceberg/src/rest_protocol/tests/fixtures/views/trino-mv.create-view.json"
        ));
        assert_ne!(
            path,
            sample_path(repo.path(), IcebergViewFixtureSample::SparkMv)
        );
        let raw = json!({
            "name": "__VIEW__",
            "view-version": {
                "default-namespace": ["__NAMESPACE__"],
                "representations": [{"type": "sql", "sql": "SELECT '__NAMESPACE__'"}],
                "storage-table": {"namespace": ["__NAMESPACE__"], "name": "__STORAGE_TABLE__"}
            }
        });
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, raw.to_string()).unwrap();
        let target = IcebergViewFixtureTarget {
            catalog: Some("fixture".into()),
            namespace: "case_namespace".into(),
            view: "external_view".into(),
        };
        let actual =
            request_from_sample(&std::fs::read_to_string(path).unwrap(), &target, "storage")
                .unwrap();
        assert_eq!(actual["name"], "external_view");
        assert_eq!(
            actual["view-version"]["default-namespace"],
            json!(["case_namespace"])
        );
        assert_eq!(
            actual["view-version"]["storage-table"],
            json!({"namespace": ["case_namespace"], "name": "storage"})
        );
        assert_eq!(
            actual["view-version"]["representations"],
            raw["view-version"]["representations"]
        );
        assert!(request_from_sample("{}", &target, "storage").is_err());
    }

    #[test]
    fn observation_uses_the_exact_current_version_and_preserves_its_storage_identity() {
        let mut response = json!({
            "metadata": {
                "view-uuid": "original",
                "current-version-id": 2,
                "versions": [
                    {"version-id": 2, "storage-table": {"namespace": ["case"], "name": "old"}},
                    {"version-id": 1}
                ]
            }
        });
        let original = ViewObservation::read(response.clone()).unwrap();
        assert_eq!(original.version_id, 2);
        assert_eq!(
            original.storage_table,
            json!({"namespace": ["case"], "name": "old"})
        );
        response["metadata"]["versions"][0]["storage-table"]["name"] = json!("new");
        assert_ne!(ViewObservation::read(response.clone()).unwrap(), original);
        response["metadata"]["current-version-id"] = json!(1);
        assert_eq!(
            ViewObservation::read(response.clone())
                .unwrap()
                .storage_table,
            Value::Null
        );
        response["metadata"]["current-version-id"] = json!(3);
        assert!(ViewObservation::read(response).is_err());
    }
}
