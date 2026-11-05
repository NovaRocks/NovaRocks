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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! CTAS completion counts use the original fenced REST publication fixture.
//! Literal rows and counts are independent of the returned completion carrier.

use crate::{
    actors::{sql, trino_protocol::TrinoClient},
    scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig},
};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, LaunchProfile, TestClusterOwner,
    publication::PublicationFixture,
};
use std::{path::Path, sync::Mutex};

const CATALOG: &str = "trc3_ctas_count";
const TABLE: &str = "trc3_ctas_count.ns.copied";
const RAW_TABLE: &str = "trc3_ctas_count.ns.raw_copied";
// The provider evidence policy is the original catalog-publication setup
// policy. Every SQL request retains the scenario's original 300s deadline.
const PUBLICATION_EVIDENCE_MS: u64 = 30_000;

#[derive(Default)]
pub(super) struct SharedCtasCount {
    fixture: Mutex<Option<PublicationFixture>>,
}

impl Scenario for SharedCtasCount {
    fn name(&self) -> &'static str {
        "trino-protocol/shared-client-ctas-count"
    }

    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        super::declarations::for_module("sql_client_ctas", self.name(), true, false)
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        ensure!(
            profile == LaunchProfile::FaultScenario,
            "shared-client CTAS count requires the fault-scenario launch profile"
        );
        Ok(())
    }

    fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("publication fixture mutex poisoned"))?;
        ensure!(
            slot.is_none(),
            "publication fixture was prepared more than once"
        );
        let fixture = PublicationFixture::start(root)?;
        let identity = fixture.rest().static_s3_identity();
        let mut environment = CrossProcessChildEnvironment::default();
        for role in [&mut environment.fe, &mut environment.be] {
            role.insert(
                "NOVA_PUBLICATION_ACCESS_KEY".into(),
                identity.access_key_id.clone(),
            );
            role.insert(
                "NOVA_PUBLICATION_SECRET_KEY".into(),
                identity.secret_access_key.clone(),
            );
        }
        let credential = r#"
[[connector.credentials]]
purpose = "object-store-metadata"
name = "publication-fixture"
generation = "v1"
kind = "s3"
access_key_id = "${ENV:NOVA_PUBLICATION_ACCESS_KEY}"
access_key_secret = "${ENV:NOVA_PUBLICATION_SECRET_KEY}"
"#
        .to_owned();
        *slot = Some(fixture);
        Ok(ScenarioLaunchConfig {
            child_environment: environment,
            config_overlay: CrossProcessConfigOverlay {
                fe: Some(credential.clone()),
                be: Some(credential.replace("object-store-metadata", "object-store-data")),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "shared-client CTAS count requires native 1FE+3BE"
        );
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("publication fixture mutex poisoned"))?;
        let fixture = fixture
            .as_ref()
            .context("publication fixture not prepared")?;
        let mut client =
            sql::SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let phase = (|| -> Result<()> {
            context.action("CTAS setup: create isolated fenced REST catalog");
            client
                .execute(catalog_sql(fixture))
                .context("create isolated fenced REST CTAS catalog")?;
            client
                .execute(format!("CREATE DATABASE {CATALOG}.ns"))
                .context("create isolated CTAS namespace")?;
            context.action("CTAS positive: submit literal source rows [1, 3]");
            let created = client
                .execute(format!(
                    "CREATE TABLE {TABLE} AS SELECT 1 AS id UNION ALL SELECT 3 AS id"
                ))
                .context("positive CTAS from literal rows [1, 3]")?;
            sql::expect_update_count(&created, 2)
                .context("positive CTAS must return Counted(2) and one rows bigint cell")?;
            ensure!(
                created.update_type.as_deref() == Some("CREATE TABLE"),
                "CTAS must retain its admitted CREATE TABLE update type: {created:?}"
            );
            context.action(format!(
                "CTAS positive: Counted(2) and rows bigint verified client_query_id={}",
                created.query_id
            ));
            let rows = client
                .execute(format!("SELECT id FROM {TABLE} ORDER BY id"))
                .context("read positive CTAS table independently")?;
            ensure!(
                sql::integer_column(&rows)? == vec![1, 3],
                "CTAS did not publish its two literal source rows"
            );
            context.action(format!(
                "CTAS positive: independent table rows [1, 3] verified client_query_id={}",
                rows.query_id
            ));
            let raw = TrinoClient::new(
                context.trino_port(),
                context.sql_user(),
                context.deadline(),
                &context.scenario_root().join("ctas-raw-http"),
            )?;
            let query_id = super::trino_protocol::assert_raw_completion(
                &raw,
                &format!("CREATE TABLE {RAW_TABLE} AS SELECT 1 AS id UNION ALL SELECT 3 AS id"),
                "CREATE TABLE",
                super::trino_protocol::RawCompletionExpectation::Counted(2),
            )
            .context("raw CTAS Counted(2) shape, page position and same-token replay")?;
            let raw_rows = client
                .execute(format!("SELECT id FROM {RAW_TABLE} ORDER BY id"))
                .context("read raw CTAS table independently")?;
            ensure!(
                sql::integer_column(&raw_rows)? == vec![1, 3],
                "raw CTAS did not publish its two literal source rows"
            );
            context.action(format!(
                "raw CTAS Counted(2), rows bigint, replay and independent rows [1, 3] verified client_query_id={query_id}"
            ));
            context.action("shared typed and raw clients preserved fenced REST CTAS Counted(2) and independent table rows [1, 3]");
            Ok(())
        })();
        // SQL cleanup precedes cluster shutdown. The runner calls teardown
        // only after the original cluster owner has observed actual role exit.
        context.action("CTAS SQL cleanup: drop isolated table, namespace and catalog");
        let cleanup = cleanup_sql(&mut client);
        if cleanup.is_ok() {
            context.action("CTAS SQL cleanup completed");
        }
        combine(phase, cleanup)
    }

    fn teardown(&self) -> Result<()> {
        if let Some(mut fixture) = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("publication fixture mutex poisoned"))?
            .take()
        {
            fixture.shutdown()?;
        }
        Ok(())
    }
}

fn catalog_sql(fixture: &PublicationFixture) -> String {
    let endpoints = fixture.rest().endpoints();
    format!(
        r#"CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(
        "type"="iceberg", "iceberg.catalog.type"="rest", "uri"="{}", "warehouse"="{}",
        "novarocks.publication.evidence-timeout-ms"="{PUBLICATION_EVIDENCE_MS}",
        "credential.object-store-metadata.consumer-role"="frontend", "credential.object-store-metadata.mode"="static",
        "credential.object-store-metadata.name"="publication-fixture", "credential.object-store-metadata.generation"="v1",
        "credential.object-store-data.consumer-role"="backend", "credential.object-store-data.mode"="static",
        "credential.object-store-data.name"="publication-fixture", "credential.object-store-data.generation"="v1",
        "aws.s3.endpoint"="{}", "aws.s3.region"="us-east-1", "aws.s3.enable_path_style_access"="true")"#,
        fixture.uri(),
        endpoints.rest_warehouse,
        endpoints.minio_endpoint
    )
}

fn cleanup_sql(client: &mut sql::SqlSession) -> Result<()> {
    let mut errors = Vec::new();
    for statement in [
        format!("DROP TABLE IF EXISTS {RAW_TABLE}"),
        format!("DROP TABLE IF EXISTS {TABLE}"),
        format!("DROP DATABASE IF EXISTS {CATALOG}.ns"),
        format!("DROP CATALOG IF EXISTS {CATALOG}"),
    ] {
        if let Err(error) = client.execute(&statement) {
            errors.push(format!("{statement}: {error}"));
        }
    }
    ensure!(
        errors.is_empty(),
        "CTAS SQL cleanup failed: {}",
        errors.join("; ")
    );
    Ok(())
}

fn combine(primary: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Err(cleanup)) => {
            anyhow::bail!("{primary:#}; CTAS SQL cleanup: {cleanup:#}")
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
    }
}
