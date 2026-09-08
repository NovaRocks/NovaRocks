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

use crate::actors::mysql as mysql_actor;
use crate::performance::{MixedFixtureBinding, PerformanceScenario, Uea1WorkloadManifest};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture;
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, LaunchProfile,
};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Mutex;

const PREPARATION_DIAGNOSTIC_SECRET_ENV: &str = "NOVAROCKS_PREPARATION_DIAGNOSTIC_SECRET";

const MIXED_CATALOG: &str = "uea1_performance";
const MIXED_CREDENTIAL_NAME: &str = "uea1-performance-data";
const MIXED_CREDENTIAL_GENERATION: &str = "v1";
const ACCESS_KEY_ENV: &str = "NOVAROCKS_UEA1_PERF_S3_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "NOVAROCKS_UEA1_PERF_S3_SECRET_ACCESS_KEY";

struct MixedScenarioFixture {
    rest: IsolatedIcebergRestFixture,
    create_catalog_sql: String,
}

struct Uea1PerformanceScenario {
    scenario: PerformanceScenario,
    mixed_fixture: Mutex<Option<MixedScenarioFixture>>,
}

impl Uea1PerformanceScenario {
    fn new(scenario: PerformanceScenario) -> Self {
        Self {
            scenario,
            mixed_fixture: Mutex::new(None),
        }
    }

    fn is_mixed(&self) -> bool {
        matches!(self.scenario, PerformanceScenario::Mixed)
    }
}

impl Scenario for Uea1PerformanceScenario {
    fn name(&self) -> &'static str {
        self.scenario.name()
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(
        &self,
        launch_profile: LaunchProfile,
        uea1_workload_manifest: Option<&Path>,
    ) -> Result<()> {
        anyhow::ensure!(
            launch_profile == LaunchProfile::Performance,
            "{} requires --launch-profile performance",
            self.name()
        );
        let path = uea1_workload_manifest
            .context("UEA-1 performance scenarios require --uea1-workload-manifest")?;
        Uea1WorkloadManifest::load(path)?;
        Ok(())
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let mut child_environment = CrossProcessChildEnvironment::default();
        child_environment.fe.insert(
            PREPARATION_DIAGNOSTIC_SECRET_ENV.to_string(),
            diagnostic_secret()?,
        );
        if !self.is_mixed() {
            return Ok(ScenarioLaunchConfig {
                child_environment,
                ..ScenarioLaunchConfig::default()
            });
        }

        let rest = IsolatedIcebergRestFixture::start(scenario_root)
            .context("start private Iceberg REST fixture for UEA-1 mixed workload")?;
        let endpoints = rest.endpoints().clone();
        let identity = rest.static_s3_identity();
        let create_catalog_sql = format!(
            "CREATE EXTERNAL CATALOG {MIXED_CATALOG} PROPERTIES(\
             \"type\"=\"iceberg\",\
             \"iceberg.catalog.type\"=\"rest\",\
             \"uri\"=\"{}\",\
             \"warehouse\"=\"{}\",\
             \"credential.object-store-data.consumer-role\"=\"frontend-and-backend\",\
             \"credential.object-store-data.mode\"=\"static\",\
             \"credential.object-store-data.name\"=\"{MIXED_CREDENTIAL_NAME}\",\
             \"credential.object-store-data.generation\"=\"{MIXED_CREDENTIAL_GENERATION}\",\
             \"aws.s3.endpoint\"=\"{}\",\
             \"aws.s3.region\"=\"us-east-1\",\
             \"aws.s3.enable_path_style_access\"=\"true\")",
            endpoints.rest_uri, endpoints.rest_warehouse, endpoints.minio_endpoint,
        );
        let mut fixture = self
            .mixed_fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-1 mixed fixture lock poisoned"))?;
        if fixture.is_some() {
            bail!("UEA-1 mixed fixture was initialized more than once");
        }
        *fixture = Some(MixedScenarioFixture {
            rest,
            create_catalog_sql,
        });

        child_environment
            .fe
            .insert(ACCESS_KEY_ENV.to_string(), identity.access_key_id.clone());
        child_environment.fe.insert(
            SECRET_KEY_ENV.to_string(),
            identity.secret_access_key.clone(),
        );
        child_environment
            .be
            .insert(ACCESS_KEY_ENV.to_string(), identity.access_key_id);
        child_environment
            .be
            .insert(SECRET_KEY_ENV.to_string(), identity.secret_access_key);
        let credential_registry = format!(
            r#"
[[connector.credentials]]
purpose = "object-store-data"
name = "{MIXED_CREDENTIAL_NAME}"
generation = "{MIXED_CREDENTIAL_GENERATION}"
kind = "s3"
access_key_id = "${{ENV:{ACCESS_KEY_ENV}}}"
access_key_secret = "${{ENV:{SECRET_KEY_ENV}}}"
"#
        );
        Ok(ScenarioLaunchConfig {
            child_environment,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(credential_registry.clone()),
                be: Some(credential_registry),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let path = context
            .uea1_workload_manifest()
            .context("UEA-1 performance scenarios require --uea1-workload-manifest")?
            .to_path_buf();
        let manifest = Uea1WorkloadManifest::load(&path)?;
        let mixed_binding = if self.is_mixed() {
            let fixture = self
                .mixed_fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("UEA-1 mixed fixture lock poisoned"))?;
            let fixture = fixture
                .as_ref()
                .context("UEA-1 mixed fixture is missing after cluster launch")?;
            let create_catalog_sql = fixture.create_catalog_sql.clone();
            let provider_runtime = fixture.rest.runtime_identity()?;
            let mut connection = mysql_actor::connect(
                context.mysql_user(),
                context.mysql_port(),
                context.remaining("create private UEA-1 mixed catalog")?,
            )?;
            connection
                .query_drop(create_catalog_sql)
                .context("create private UEA-1 mixed catalog")?;
            Some(MixedFixtureBinding::with_provider_runtime(
                MIXED_CATALOG.to_string(),
                provider_runtime,
            )?)
        } else {
            None
        };
        crate::performance::run(self.scenario, context, &manifest, mixed_binding.as_ref())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .mixed_fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("UEA-1 mixed fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture
            .rest
            .shutdown()
            .context("shutdown private UEA-1 mixed Iceberg REST fixture")
    }
}

fn diagnostic_secret() -> Result<String> {
    let mut bytes = [0_u8; 32];
    File::open("/dev/urandom")
        .context("open operating-system random source for diagnostic secret")?
        .read_exact(&mut bytes)
        .context("read diagnostic secret from operating-system random source")?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(Uea1PerformanceScenario::new(
            PerformanceScenario::ShortConcurrent,
        )),
        Box::new(Uea1PerformanceScenario::new(PerformanceScenario::Mixed)),
        Box::new(Uea1PerformanceScenario::new(
            PerformanceScenario::SlowOutput,
        )),
    ]
}
